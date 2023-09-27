//! Tooling to distribute a single payload stream to multiple output streams.
//!
//! # Requirements
//! This is mostly designed to demux the ingester response stream. The requirements are:
//!
//! - **Eager Polling:** The response stream should be polled eagerly even when the demuxed streams are NOT consumed yet. This
//!   is important to get the data of the wire as quickly as possible, to help the ingester with its data management, and
//!   to lower query latency.
//! - **Low Latency:** We must be able to proceed with query planning and start query execution BEFORE the entire
//!   payload is polled. This rules out any non-async solutions.
//! - **Consumer Restarts/Clones:** A single demuxed stream might be used multiple times. This is because DataFusion is
//!   allowed to clone the physical execution node for a single ingester partition[^df_clone_phys]
//! - **Panic Propagation:** The stream usually communicates its failure via proper errors, but panics can never be
//!   fully avoided since they may also occur in upstream libraries. In case of a panic, the panic message shall be
//!   forwarded to the receiver ends and the streams shall be terminated.
//! - **Unbounded Buffer:** We do NOT know ahead of time how much data the ingester is going to send. The ingester counts as a
//!   trusted component though and we want to emphasize _eager polling_, so the buffer should be unbounded.
//!
//! # Alternatives
//! The following alternatives were considered:
//!
//! ## tokio broadcast
//! Use [`tokio::sync::broadcast`]. This broadcasts a data from a single async source to multiple receivers. Could be
//! combined with [`ReceiverStream`]. However this approach has multiple drawbacks:
//!
//! - **Consumer Restarts:** Broadcasts are only sent to existing consumers. Receiving old data on new consumer ends is
//!   generally not possible, so an additional workaround would be required.
//! - **Bounded Buffer:** The broadcast buffer is bounded. Setting that limit to a very large size is generally not
//!   possible, since the current implementation pre-allocates the buffer.
//!
//!
//! [`ReceiverStream`]: https://docs.rs/tokio-stream/0.1.14/tokio_stream/wrappers/struct.ReceiverStream.html
//! [`tokio::sync::broadcast`]: https://docs.rs/tokio/1.32.0/tokio/sync/broadcast/index.html
//!
//! [^df_clone_phys]: Currently (2023-09-26) DataFusion does NOT perform such trickery, but it might do in the future
//!     and the system is designed in a robust way now to avoid any weird behavior later.
use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use futures::{FutureExt, Stream, StreamExt};
use parking_lot::Mutex;
use tokio::task::JoinSet;

/// Receiving is done.
#[derive(Debug)]
enum Done {
    /// Finished in a clean fashion.
    Clean,

    /// Panicked.
    Panic(String),
}

/// State for a single receiver end.
#[derive(Debug)]
struct Receiver<V, E>
where
    V: Clone + Debug + Send + Sync + 'static,
    E: Clone + Debug + Send + Sync + 'static,
{
    /// Data received for this particular read end.
    received: Vec<Result<V, E>>,

    /// Wakers to be notified if the state changes.
    wakers: Vec<Waker>,

    /// Marker noting that the input finished.
    done: Option<Done>,
}

impl<V, E> Receiver<V, E>
where
    V: Clone + Debug + Send + Sync + 'static,
    E: Clone + Debug + Send + Sync + 'static,
{
    /// Send new data to this receiver.
    ///
    /// This will also [wake all waiting tasks](Self::wake_all).
    fn send(&mut self, res: Result<V, E>) {
        self.received.push(res);
        self.wake_all();
    }

    /// Finished stream in a clean way.
    ///
    /// This will also [wake all waiting tasks](Self::wake_all).
    fn done_clean(&mut self) {
        self.done = Some(Done::Clean);
        self.wake_all();
    }

    /// Finished stream with a panic.
    ///
    /// This will also [wake all waiting tasks](Self::wake_all).
    fn done_panic(&mut self, e: String) {
        self.done = Some(Done::Panic(e));
        self.wake_all();
    }

    /// Wake all waiting tasks and consume registered wakers.
    fn wake_all(&mut self) {
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

impl<V, E> Default for Receiver<V, E>
where
    V: Clone + Debug + Send + Sync + 'static,
    E: Clone + Debug + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            received: Default::default(),
            wakers: Default::default(),
            done: None,
        }
    }
}

type SharedReceiver<V, E> = Arc<Mutex<Receiver<V, E>>>;

/// Distribute/demux a single payload stream into multiple streams.
///
/// See [module docs](super) for more details.
#[derive(Debug)]
pub struct PayloadDistributor<K, V, E>
where
    K: Eq + Debug + Hash + Send + Sync + 'static,
    V: Clone + Debug + Send + Sync + 'static,
    E: Clone + Debug + Send + Sync + 'static,
{
    /// Background task feeding the payload stream into the different receivers.
    task: Arc<JoinSet<()>>,

    /// Receivers
    receivers: Arc<HashMap<K, SharedReceiver<V, E>>>,
}

impl<K, V, E> PayloadDistributor<K, V, E>
where
    K: Eq + Debug + Hash + Send + Sync + 'static,
    V: Clone + Debug + Send + Sync + 'static,
    E: Clone + Debug + Send + Sync + 'static,
{
    /// Create new distributor given a single stream and a known set of keys.
    ///
    /// If the stream contains data for unregistered keys, this data is silently dropped.
    pub fn new<S, I>(payload: S, keys: I) -> Self
    where
        I: IntoIterator<Item = K>,
        S: Stream<Item = Result<(K, V), E>> + Send + 'static,
    {
        let receivers = Arc::new(
            keys.into_iter()
                .map(|k| (k, Arc::new(Mutex::new(Receiver::default()))))
                .collect::<HashMap<_, _>>(),
        );

        let mut join_set = JoinSet::new();
        let receivers_captured = Arc::clone(&receivers);
        join_set.spawn(async move {
            let fut = async {
                let mut payload = std::pin::pin!(payload);

                while let Some(result) = payload.next().await {
                    match result {
                        Ok((k, v)) => {
                            if let Some(recv) = receivers_captured.get(&k) {
                                recv.lock().send(Ok(v));
                            }
                        }
                        Err(e) => {
                            for recv in receivers_captured.values() {
                                recv.lock().send(Err(e.clone()))
                            }
                        }
                    }
                }
            };

            match AssertUnwindSafe(fut).catch_unwind().await {
                Ok(()) => {
                    for recv in receivers_captured.values() {
                        recv.lock().done_clean();
                    }
                }
                Err(e) => {
                    let e = if let Some(e) = e.downcast_ref::<&str>() {
                        e.to_string()
                    } else if let Some(e) = e.downcast_ref::<String>() {
                        e.clone()
                    } else {
                        String::from("unknown panic")
                    };

                    for recv in receivers_captured.values() {
                        recv.lock().done_panic(e.clone());
                    }
                }
            }
        });

        Self {
            task: Arc::new(join_set),
            receivers,
        }
    }

    /// Get receiver stream.
    ///
    /// This stream will yield the first received message for this key. You may create multiple streams for the same
    /// key. They will all read from the first received message (i.e. you can read the same content multiple times).
    ///
    /// # Panic
    /// Panics if the given receiver was never registered.
    pub fn stream(&self, k: &K) -> ReceiverStream<V, E> {
        let recv = self.receivers.get(k).expect("receiver known");
        ReceiverStream::new(Arc::clone(recv), Arc::clone(&self.task))
    }
}

/// Stream produced by [`PayloadDistributor::stream`].
pub struct ReceiverStream<V, E>
where
    V: Clone + Debug + Send + Sync + 'static,
    E: Clone + Debug + Send + Sync + 'static,
{
    /// Shared receiver state.
    receiver: SharedReceiver<V, E>,

    /// Next item to be read from [`Receiver::received`].
    next: usize,

    /// Background task feeding the payload stream into the different receivers.
    _task: Arc<JoinSet<()>>,
}

impl<V, E> ReceiverStream<V, E>
where
    V: Clone + Debug + Send + Sync + 'static,
    E: Clone + Debug + Send + Sync + 'static,
{
    /// Create new receiver that will start consuming the first received message (NOT the last currently known one).
    fn new(receiver: SharedReceiver<V, E>, task: Arc<JoinSet<()>>) -> Self {
        Self {
            receiver,
            next: 0,
            _task: task,
        }
    }
}

impl<V, E> Stream for ReceiverStream<V, E>
where
    V: Clone + Debug + Send + Sync + 'static,
    E: Clone + Debug + Send + Sync + 'static,
{
    type Item = Result<V, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        let mut recv = this.receiver.lock();

        if let Some(data) = recv.received.get(this.next) {
            this.next += 1;
            Poll::Ready(Some(data.clone()))
        } else {
            match &recv.done {
                Some(Done::Clean) => Poll::Ready(None),
                Some(Done::Panic(s)) => {
                    panic!("receiver tasks panicked: {s}");
                }
                None => {
                    recv.wakers.push(cx.waker().clone());
                    Poll::Pending
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, future::Future, time::Duration};

    use async_trait::async_trait;
    use tokio::sync::Barrier;

    use super::*;

    #[tokio::test]
    #[should_panic(expected = "receiver known")]
    async fn test_panic_unknown_receiver() {
        let dist = PayloadDistributor::new(TestStreamBuilder::default().build(), [1, 3]);
        dist.stream(&2);
    }

    #[tokio::test]
    async fn test_distribute_ok() {
        let dist = PayloadDistributor::new(
            TestStreamBuilder::default()
                .next(Ok((1, 1001)))
                .next(Ok((1, 1002)))
                .next(Ok((3, 3001)))
                .next(Ok((1, 1003)))
                .build(),
            [1, 3],
        );

        assert_eq!(
            dist.stream(&1).collect::<Vec<_>>().await,
            vec![Ok(1001), Ok(1002), Ok(1003)],
        );

        assert_eq!(dist.stream(&3).collect::<Vec<_>>().await, vec![Ok(3001)],);
    }

    #[tokio::test]
    async fn test_distribute_err() {
        let dist = PayloadDistributor::new(
            TestStreamBuilder::default()
                .next(Ok((1, 1001)))
                .next(Err("foo"))
                .next(Ok((3, 3001)))
                .next(Ok((1, 1002)))
                .build(),
            [1, 3],
        );

        assert_eq!(
            dist.stream(&1).collect::<Vec<_>>().await,
            vec![Ok(1001), Err("foo"), Ok(1002)],
        );

        assert_eq!(
            dist.stream(&3).collect::<Vec<_>>().await,
            vec![Err("foo"), Ok(3001)],
        );
    }

    #[tokio::test]
    async fn test_ignore_unknown_inputs() {
        let dist = PayloadDistributor::new(
            TestStreamBuilder::default()
                .next(Ok((1, 1001)))
                .next(Ok((2, 2001)))
                .next(Ok((1, 1002)))
                .build(),
            [1, 3],
        );

        assert_eq!(
            dist.stream(&1).collect::<Vec<_>>().await,
            vec![Ok(1001), Ok(1002)],
        );
    }

    #[tokio::test]
    async fn test_consume_twice() {
        let dist = PayloadDistributor::new(
            TestStreamBuilder::default()
                .next(Ok((1, 1001)))
                .next(Ok((1, 1002)))
                .build(),
            [1],
        );

        for _ in 0..2 {
            assert_eq!(
                dist.stream(&1).collect::<Vec<_>>().await,
                vec![Ok(1001), Ok(1002)],
            );
        }
    }

    #[tokio::test]
    async fn test_source_polled_without_active_stream() {
        let barrier = barrier();
        let dist = PayloadDistributor::new(
            TestStreamBuilder::default()
                .next(Ok((1, 1001)))
                .wait(&barrier)
                .next(Ok((1, 1002)))
                .next(Ok((1, 1003)))
                .build(),
            [1],
        );

        barrier.wait().await;

        assert_eq!(
            dist.stream(&1).collect::<Vec<_>>().await,
            vec![Ok(1001), Ok(1002), Ok(1003)],
        );
    }

    #[tokio::test]
    async fn test_stream_keeps_task_alive() {
        let barrier_1 = barrier();
        let barrier_2 = barrier();
        let dist = PayloadDistributor::new(
            TestStreamBuilder::default()
                .next(Ok((1, 1001)))
                .wait(&barrier_1)
                .next(Ok((1, 1002)))
                .wait(&barrier_2)
                .next(Ok((1, 1003)))
                .next(Ok((1, 1004)))
                .build(),
            [1],
        );

        let stream = dist.stream(&1);
        drop(dist);

        barrier_1.wait().await;
        barrier_2.wait().await;

        assert_eq!(
            stream.collect::<Vec<_>>().await,
            vec![Ok(1001), Ok(1002), Ok(1003), Ok(1004)],
        );
    }

    #[tokio::test]
    async fn test_stream_pending_middle() {
        let barrier = barrier();
        let dist = PayloadDistributor::new(
            TestStreamBuilder::default()
                .next(Ok((1, 1001)))
                .wait(&barrier)
                .next(Ok((1, 1002)))
                .next(Ok((1, 1003)))
                .build(),
            [1],
        );

        let mut stream = dist.stream(&1);

        assert_eq!(stream.next().await, Some(Ok(1001)));

        let mut fut = stream.next();
        fut.assert_pending().await;

        barrier.wait().await;

        assert_eq!(fut.await, Some(Ok(1002)));

        assert_eq!(stream.next().await, Some(Ok(1003)));
        assert_eq!(stream.next().await, None);

        // can still consume entire stream
        assert_eq!(
            dist.stream(&1).collect::<Vec<_>>().await,
            vec![Ok(1001), Ok(1002), Ok(1003)],
        );
    }

    #[tokio::test]
    async fn test_stream_pending_end() {
        let barrier = barrier();
        let dist = PayloadDistributor::new(
            TestStreamBuilder::default()
                .next(Ok((1, 1001)))
                .wait(&barrier)
                .build(),
            [1],
        );

        let mut stream = dist.stream(&1);

        assert_eq!(stream.next().await, Some(Ok(1001)));

        let mut fut = stream.next();
        fut.assert_pending().await;

        barrier.wait().await;

        assert_eq!(fut.await, None);

        // can still consume entire stream
        assert_eq!(dist.stream(&1).collect::<Vec<_>>().await, vec![Ok(1001)],);
    }

    #[tokio::test]
    #[should_panic(expected = "test panic")]
    async fn test_propagate_panic_static() {
        let dist = PayloadDistributor::new(
            TestStreamBuilder::default()
                .next(Ok((1, 1001)))
                .panic_static()
                .build(),
            [1],
        );

        let mut stream = dist.stream(&1);

        assert_eq!(stream.next().await, Some(Ok(1001)));

        stream.next().await;
    }

    #[tokio::test]
    #[should_panic(expected = "test panic")]
    async fn test_propagate_panic_dynamic() {
        let dist = PayloadDistributor::new(
            TestStreamBuilder::default()
                .next(Ok((1, 1001)))
                .panic_dynamic()
                .build(),
            [1],
        );

        let mut stream = dist.stream(&1);

        assert_eq!(stream.next().await, Some(Ok(1001)));

        stream.next().await;
    }

    #[tokio::test]
    #[should_panic(expected = "unknown panic")]
    async fn test_propagate_panic_weird() {
        let dist = PayloadDistributor::new(
            TestStreamBuilder::default()
                .next(Ok((1, 1001)))
                .panic_weird()
                .build(),
            [1],
        );

        let mut stream = dist.stream(&1);

        assert_eq!(stream.next().await, Some(Ok(1001)));

        stream.next().await;
    }

    type TestStreamData = Result<(u8, u64), &'static str>;

    #[derive(Debug)]
    enum TestAction {
        Wait(Arc<Barrier>),
        Data(TestStreamData),
        PanicStatic,
        PanicDynamic,
        PanicWeird,
    }

    #[derive(Debug, Default)]
    struct TestStreamBuilder {
        actions: VecDeque<TestAction>,
    }

    impl TestStreamBuilder {
        fn next(mut self, data: TestStreamData) -> Self {
            self.actions.push_back(TestAction::Data(data));
            self
        }

        fn wait(mut self, barrier: &Arc<Barrier>) -> Self {
            self.actions
                .push_back(TestAction::Wait(Arc::clone(barrier)));
            self
        }

        fn panic_static(mut self) -> Self {
            self.actions.push_back(TestAction::PanicStatic);
            self
        }

        fn panic_dynamic(mut self) -> Self {
            self.actions.push_back(TestAction::PanicDynamic);
            self
        }

        fn panic_weird(mut self) -> Self {
            self.actions.push_back(TestAction::PanicWeird);
            self
        }

        fn build(self) -> impl Stream<Item = TestStreamData> {
            let Self { actions } = self;

            futures::stream::unfold(actions, |mut actions| async move {
                loop {
                    match actions.pop_front() {
                        Some(TestAction::Data(data)) => {
                            return Some((data, actions));
                        }
                        Some(TestAction::Wait(barrier)) => {
                            barrier.wait().await;
                        }
                        Some(TestAction::PanicStatic) => {
                            panic!("test panic")
                        }
                        Some(TestAction::PanicDynamic) => {
                            let s = String::from("test panic");
                            panic!("{s}")
                        }
                        Some(TestAction::PanicWeird) => std::panic::panic_any(1u8),
                        None => {
                            return None;
                        }
                    }
                }
            })
        }
    }

    fn barrier() -> Arc<Barrier> {
        Arc::new(Barrier::new(2))
    }

    #[async_trait]
    trait AssertFutureExt {
        async fn assert_pending(&mut self);
    }

    #[async_trait]
    impl<F> AssertFutureExt for F
    where
        F: Future + Send + Unpin,
    {
        async fn assert_pending(&mut self) {
            tokio::select! {
                biased;
                _ = self => {
                    panic!("not pending")
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    }
}
