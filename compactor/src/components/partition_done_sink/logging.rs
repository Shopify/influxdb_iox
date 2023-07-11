use std::fmt::Display;

use async_trait::async_trait;
use compactor_scheduler::PartitionDoneSink;
use data_types::PartitionId;
use observability_deps::tracing::{error, info};

use crate::error::{DynError, ErrorKindExt};

#[derive(Debug)]
pub struct LoggingPartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink<PartitionId>,
{
    inner: T,
}

impl<T> LoggingPartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink<PartitionId>,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for LoggingPartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink<PartitionId>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging({})", self.inner)
    }
}

#[async_trait]
impl<T> PartitionDoneSink<PartitionId> for LoggingPartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink<PartitionId>,
{
    async fn record(&self, partition: PartitionId, res: Result<(), DynError>) {
        match &res {
            Ok(()) => {
                info!(partition_id = partition.get(), "Finished partition",);
            }
            Err(e) => {
                error!(
                    %e,
                    kind=e.classify().name(),
                    partition_id = partition.get(),
                    "Error while compacting partition",
                );
            }
        }
        self.inner.record(partition, res).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use compactor_scheduler::MockPartitionDoneSink;
    use object_store::Error as ObjectStoreError;
    use test_helpers::tracing::TracingCapture;

    use super::*;

    #[test]
    fn test_display() {
        let sink = LoggingPartitionDoneSinkWrapper::new(MockPartitionDoneSink::new());
        assert_eq!(sink.to_string(), "logging(mock)");
    }

    #[tokio::test]
    async fn test_record() {
        let inner = Arc::new(MockPartitionDoneSink::new());
        let sink = LoggingPartitionDoneSinkWrapper::new(Arc::clone(&inner));

        let capture = TracingCapture::new();

        sink.record(PartitionId::new(1), Err("msg 1".into())).await;
        sink.record(PartitionId::new(2), Err("msg 2".into())).await;
        sink.record(
            PartitionId::new(1),
            Err(Box::new(ObjectStoreError::NotImplemented)),
        )
        .await;
        sink.record(PartitionId::new(3), Ok(())).await;

        assert_eq!(
            capture.to_string(),
            "level = ERROR; message = Error while compacting partition; e = msg 1; kind = \"unknown\"; partition_id = 1; \n\
level = ERROR; message = Error while compacting partition; e = msg 2; kind = \"unknown\"; partition_id = 2; \n\
level = ERROR; message = Error while compacting partition; e = Operation not yet implemented.; kind = \"object_store\"; partition_id = 1; \n\
level = INFO; message = Finished partition; partition_id = 3; ",
        );

        assert_eq!(
            inner.results(),
            HashMap::from([
                (
                    PartitionId::new(1),
                    Err(String::from("Operation not yet implemented.")),
                ),
                (PartitionId::new(2), Err(String::from("msg 2"))),
                (PartitionId::new(3), Ok(())),
            ]),
        );
    }
}
