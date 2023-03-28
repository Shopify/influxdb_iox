//! HTTP service implementations for `router`.

pub mod cst;
mod delete_predicate;
pub mod mt;
mod write_dml;
#[cfg(test)]
mod write_test_helpers;
mod write_v1;
mod write_v2;

use authz::{Action, Authorizer, Permission, Resource};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use hashbrown::HashMap;
use hyper::{header::CONTENT_ENCODING, Body, Method, Request, Response, StatusCode};
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, U64Counter};
use mutable_batch::MutableBatch;
use mutable_batch_lp::LinesConverter;
use observability_deps::tracing::*;
use predicate::delete_predicate::parse_delete_predicate;
use server_util::authorization::AuthorizationHeaderExtension;
use std::{str::Utf8Error, sync::Arc, time::Instant};
use thiserror::Error;
use tokio::sync::{Semaphore, TryAcquireError};
use trace::ctx::SpanContext;
use write_summary::WriteSummary;

use self::{
    delete_predicate::parse_http_delete_request, write_dml::WriteInfo, write_v1::DatabaseRpError,
    write_v2::OrgBucketError,
};
use crate::{
    dml_handlers::{
        DmlError, DmlHandler, PartitionError, RetentionError, RpcWriteError, SchemaError,
    },
    namespace_resolver::NamespaceResolver,
};

const WRITE_TOKEN_HTTP_HEADER: &str = "X-IOx-Write-Token";

/// Errors returned by the `router` HTTP request handler.
#[derive(Debug, Error)]
pub enum Error {
    /// The requested path has no registered handler.
    #[error("not found")]
    NoHandler,

    /// An error with the org/bucket in the request.
    #[error(transparent)]
    InvalidOrgBucket(#[from] OrgBucketError),

    /// An error with the db/rp in the request.
    #[error(transparent)]
    InvalidDatabaseRp(#[from] DatabaseRpError),

    /// The request body content is not valid utf8.
    #[error("body content is not valid utf8: {0}")]
    NonUtf8Body(Utf8Error),

    /// The `Content-Encoding` header is invalid and cannot be read.
    #[error("invalid content-encoding header: {0}")]
    NonUtf8ContentHeader(hyper::header::ToStrError),

    /// The specified `Content-Encoding` is not acceptable.
    #[error("unacceptable content-encoding: {0}")]
    InvalidContentEncoding(String),

    /// The client disconnected.
    #[error("client disconnected")]
    ClientHangup(hyper::Error),

    /// The client sent a request body that exceeds the configured maximum.
    #[error("max request size ({0} bytes) exceeded")]
    RequestSizeExceeded(usize),

    /// Decoding a gzip-compressed stream of data failed.
    #[error("error decoding gzip stream: {0}")]
    InvalidGzip(std::io::Error),

    /// Failure to decode the provided line protocol.
    #[error("failed to parse line protocol: {0}")]
    ParseLineProtocol(mutable_batch_lp::Error),

    /// Failure to parse the request delete predicate.
    #[error("failed to parse delete predicate: {0}")]
    ParseDelete(#[from] predicate::delete_predicate::Error),

    /// Failure to parse the delete predicate in the http request
    #[error("failed to parse delete predicate from http request: {0}")]
    ParseHttpDelete(#[from] self::delete_predicate::Error),

    /// An error returned from the [`DmlHandler`].
    #[error("dml handler error: {0}")]
    DmlHandler(#[from] DmlError),

    /// An error that occurs when attempting to map the user-provided namespace
    /// name into a [`NamespaceId`].
    ///
    /// [`NamespaceId`]: data_types::NamespaceId
    #[error(transparent)]
    NamespaceResolver(#[from] crate::namespace_resolver::Error),

    /// The router is currently servicing the maximum permitted number of
    /// simultaneous requests.
    #[error("this service is overloaded, please try again later")]
    RequestLimit,

    /// The request has no authentication, but authorization is configured.
    #[error("authentication required")]
    Unauthenticated,

    /// The provided authorization is not sufficient to perform the request.
    #[error("access denied")]
    Forbidden,

    /// An error occurred verifying the authorization token.
    #[error(transparent)]
    Authorizer(authz::Error),
}

impl Error {
    /// Convert the error into an appropriate [`StatusCode`] to be returned to
    /// the end user.
    pub fn as_status_code(&self) -> StatusCode {
        match self {
            Error::NoHandler => StatusCode::NOT_FOUND,
            Error::InvalidOrgBucket(_) => StatusCode::BAD_REQUEST,
            Error::InvalidDatabaseRp(_) => StatusCode::BAD_REQUEST,
            Error::ClientHangup(_) => StatusCode::BAD_REQUEST,
            Error::InvalidGzip(_) => StatusCode::BAD_REQUEST,
            Error::NonUtf8ContentHeader(_) => StatusCode::BAD_REQUEST,
            Error::NonUtf8Body(_) => StatusCode::BAD_REQUEST,
            Error::ParseLineProtocol(_) => StatusCode::BAD_REQUEST,
            Error::ParseDelete(_) => StatusCode::BAD_REQUEST,
            Error::ParseHttpDelete(_) => StatusCode::BAD_REQUEST,
            Error::RequestSizeExceeded(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Error::InvalidContentEncoding(_) => {
                // https://www.rfc-editor.org/rfc/rfc7231#section-6.5.13
                StatusCode::UNSUPPORTED_MEDIA_TYPE
            }
            Error::DmlHandler(err) => StatusCode::from(err),
            // Error from the namespace resolver is 4xx if autocreation is disabled, 5xx otherwise
            Error::NamespaceResolver(crate::namespace_resolver::Error::Create(
                crate::namespace_resolver::ns_autocreation::NamespaceCreationError::Reject(_),
            )) => StatusCode::BAD_REQUEST,
            Error::NamespaceResolver(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::RequestLimit => StatusCode::SERVICE_UNAVAILABLE,
            Error::Unauthenticated => StatusCode::UNAUTHORIZED,
            Error::Forbidden => StatusCode::FORBIDDEN,
            Error::Authorizer(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<authz::Error> for Error {
    fn from(value: authz::Error) -> Self {
        match value {
            authz::Error::Forbidden => Self::Forbidden,
            authz::Error::NoToken => Self::Unauthenticated,
            e => Self::Authorizer(e),
        }
    }
}

impl From<&DmlError> for StatusCode {
    fn from(e: &DmlError) -> Self {
        match e {
            DmlError::NamespaceNotFound(_) => StatusCode::NOT_FOUND,

            // Schema validation error cases
            DmlError::Schema(SchemaError::NamespaceLookup(_)) => {
                // While the [`NamespaceAutocreation`] layer is in use, this is
                // an internal error as the namespace should always exist.
                StatusCode::INTERNAL_SERVER_ERROR
            }
            DmlError::Schema(SchemaError::ServiceLimit(_)) => {
                // https://docs.influxdata.com/influxdb/cloud/account-management/limits/#api-error-responses
                StatusCode::BAD_REQUEST
            }
            DmlError::Schema(SchemaError::Conflict(_)) => StatusCode::BAD_REQUEST,
            DmlError::Schema(SchemaError::UnexpectedCatalogError(_)) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }

            DmlError::Internal(_) | DmlError::WriteBuffer(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DmlError::Partition(PartitionError::BatchWrite(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            DmlError::Retention(RetentionError::NamespaceLookup(_)) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            DmlError::Retention(RetentionError::OutsideRetention(_)) => StatusCode::FORBIDDEN,
            DmlError::RpcWrite(RpcWriteError::Upstream(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            DmlError::RpcWrite(RpcWriteError::DeletesUnsupported) => StatusCode::NOT_IMPLEMENTED,
            DmlError::RpcWrite(RpcWriteError::Timeout(_)) => StatusCode::GATEWAY_TIMEOUT,
            DmlError::RpcWrite(
                RpcWriteError::NoUpstreams
                | RpcWriteError::NotEnoughReplicas
                | RpcWriteError::PartialWrite { .. }
                | RpcWriteError::UpstreamNotConnected(_),
            ) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// Extract dml info. Set different trait implementation per tenancy.
pub trait WriteInfoExtractor: std::fmt::Debug + Send + Sync {
    #[allow(missing_docs)]
    fn extract_v1_dml_info(&self, req: &Request<Body>) -> Result<WriteInfo, Error>;
    #[allow(missing_docs)]
    fn extract_v2_dml_info(&self, req: &Request<Body>) -> Result<WriteInfo, Error>;
}

/// This type is responsible for servicing requests to the `router` HTTP
/// endpoint.
///
/// Requests to some paths may be handled externally by the caller - the IOx
/// server runner framework takes care of implementing the heath endpoint,
/// metrics, pprof, etc.
#[derive(Debug)]
pub struct HttpDelegate<D, N, T = SystemProvider> {
    max_request_bytes: usize,
    time_provider: T,
    namespace_resolver: N,
    dml_handler: D,
    authz: Option<Arc<dyn Authorizer>>,
    dml_info_extractor: &'static dyn WriteInfoExtractor,

    // A request limiter to restrict the number of simultaneous requests this
    // router services.
    //
    // This allows the router to drop a portion of requests when experiencing an
    // unusual flood of requests (i.e. due to peer routers crashing and
    // depleting the available instances in the pool) in order to preserve
    // overall system availability, instead of OOMing or otherwise failing.
    request_sem: Semaphore,

    write_metric_lines: U64Counter,
    http_line_protocol_parse_duration: DurationHistogram,
    write_metric_fields: U64Counter,
    write_metric_tables: U64Counter,
    write_metric_body_size: U64Counter,
    delete_metric_body_size: U64Counter,
    request_limit_rejected: U64Counter,
}

impl<D, N> HttpDelegate<D, N, SystemProvider> {
    /// Initialise a new [`HttpDelegate`] passing valid requests to the
    /// specified `dml_handler`.
    ///
    /// HTTP request bodies are limited to `max_request_bytes` in size,
    /// returning an error if exceeded.
    pub fn new(
        max_request_bytes: usize,
        max_requests: usize,
        namespace_resolver: N,
        dml_handler: D,
        authz: Option<Arc<dyn Authorizer>>,
        metrics: &metric::Registry,
        dml_info_extractor: &'static dyn WriteInfoExtractor,
    ) -> Self {
        let write_metric_lines = metrics
            .register_metric::<U64Counter>(
                "http_write_lines",
                "cumulative number of line protocol lines successfully routed",
            )
            .recorder(&[]);
        let write_metric_fields = metrics
            .register_metric::<U64Counter>(
                "http_write_fields",
                "cumulative number of line protocol fields successfully routed",
            )
            .recorder(&[]);
        let write_metric_tables = metrics
            .register_metric::<U64Counter>(
                "http_write_tables",
                "cumulative number of tables in each write request",
            )
            .recorder(&[]);
        let write_metric_body_size = metrics
            .register_metric::<U64Counter>(
                "http_write_body_bytes",
                "cumulative byte size of successfully routed (decompressed) line protocol write requests",
            )
            .recorder(&[]);
        let delete_metric_body_size = metrics
            .register_metric::<U64Counter>(
                "http_delete_body_bytes",
                "cumulative byte size of successfully routed (decompressed) delete requests",
            )
            .recorder(&[]);
        let request_limit_rejected = metrics
            .register_metric::<U64Counter>(
                "http_request_limit_rejected",
                "number of HTTP requests rejected due to exceeding parallel request limit",
            )
            .recorder(&[]);
        let http_line_protocol_parse_duration = metrics
            .register_metric::<DurationHistogram>(
                "http_line_protocol_parse_duration",
                "write latency of line protocol parsing",
            )
            .recorder(&[]);

        Self {
            max_request_bytes,
            time_provider: SystemProvider::default(),
            namespace_resolver,
            dml_info_extractor,
            dml_handler,
            authz,
            request_sem: Semaphore::new(max_requests),
            write_metric_lines,
            http_line_protocol_parse_duration,
            write_metric_fields,
            write_metric_tables,
            write_metric_body_size,
            delete_metric_body_size,
            request_limit_rejected,
        }
    }
}

impl<D, N, T> HttpDelegate<D, N, T>
where
    D: DmlHandler<WriteInput = HashMap<String, MutableBatch>, WriteOutput = WriteSummary>,
    N: NamespaceResolver,
    T: TimeProvider,
{
    /// Routes `req` to the appropriate handler, if any, returning the handler
    /// response.
    pub async fn route(&self, req: Request<Body>) -> Result<Response<Body>, Error> {
        // Acquire and hold a permit for the duration of this request, or return
        // a 503 if the existing requests have already exhausted the allocation.
        //
        // By dropping requests at the routing stage, before the request buffer
        // is read/decompressed, this limit can efficiently shed load to avoid
        // unnecessary memory pressure (the resource this request limit usually
        // aims to protect.)
        let _permit = match self.request_sem.try_acquire() {
            Ok(p) => p,
            Err(TryAcquireError::NoPermits) => {
                error!("simultaneous request limit exceeded - dropping request");
                self.request_limit_rejected.inc(1);
                return Err(Error::RequestLimit);
            }
            Err(e) => panic!("request limiter error: {e}"),
        };

        // Route the request to a handler.
        match (req.method(), req.uri().path()) {
            (&Method::POST, "/write") => {
                let dml_info = self.dml_info_extractor.extract_v1_dml_info(&req)?;
                self.write_handler(req, dml_info).await
            }
            (&Method::POST, "/api/v2/write") => {
                let dml_info = self.dml_info_extractor.extract_v2_dml_info(&req)?;
                self.write_handler(req, dml_info).await
            }
            (&Method::POST, "/api/v2/delete") => self.delete_handler(req).await,
            _ => return Err(Error::NoHandler),
        }
        .map(|summary| {
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header(WRITE_TOKEN_HTTP_HEADER, summary.to_token())
                .body(Body::empty())
                .unwrap()
        })
    }

    async fn write_handler(
        &self,
        req: Request<Body>,
        write_info: WriteInfo,
    ) -> Result<WriteSummary, Error> {
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();

        let token = req
            .extensions()
            .get::<AuthorizationHeaderExtension>()
            .and_then(|v| v.as_ref())
            .and_then(|v| {
                let s = v.as_ref();
                if s.len() < b"Token ".len() {
                    None
                } else {
                    match s.split_at(b"Token ".len()) {
                        (b"Token ", token) => Some(token),
                        _ => None,
                    }
                }
            });
        let perms = [Permission::ResourceAction(
            Resource::Namespace(write_info.namespace.to_string()),
            Action::Write,
        )];
        self.authz.require_any_permission(token, &perms).await?;

        trace!(
            namespace=%write_info.namespace,
            "processing write request"
        );

        // Read the HTTP body and convert it to a str.
        let body = self.read_body(req).await?;
        let body = std::str::from_utf8(&body).map_err(Error::NonUtf8Body)?;

        // The time, in nanoseconds since the epoch, to assign to any points that don't
        // contain a timestamp
        let default_time = self.time_provider.now().timestamp_nanos();
        let start_instant = Instant::now();

        let mut converter = LinesConverter::new(default_time);
        converter.set_timestamp_base(write_info.precision.timestamp_base());
        let (batches, stats) = match converter.write_lp(body).and_then(|_| converter.finish()) {
            Ok(v) => v,
            Err(mutable_batch_lp::Error::EmptyPayload) => {
                debug!("nothing to write");
                return Ok(WriteSummary::default());
            }
            Err(e) => return Err(Error::ParseLineProtocol(e)),
        };

        let num_tables = batches.len();
        let duration = start_instant.elapsed();
        self.http_line_protocol_parse_duration.record(duration);
        debug!(
            num_lines=stats.num_lines,
            num_fields=stats.num_fields,
            num_tables,
            precision=?write_info.precision,
            body_size=body.len(),
            namespace=%write_info.namespace,
            duration=?duration,
            "routing write",
        );

        // Retrieve the namespace ID for this namespace.
        let namespace_id = self
            .namespace_resolver
            .get_namespace_id(&write_info.namespace)
            .await?;

        let summary = self
            .dml_handler
            .write(&write_info.namespace, namespace_id, batches, span_ctx)
            .await
            .map_err(Into::into)?;

        self.write_metric_lines.inc(stats.num_lines as _);
        self.write_metric_fields.inc(stats.num_fields as _);
        self.write_metric_tables.inc(num_tables as _);
        self.write_metric_body_size.inc(body.len() as _);

        Ok(summary)
    }

    async fn delete_handler(&self, req: Request<Body>) -> Result<WriteSummary, Error> {
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();
        let write_info = self.dml_info_extractor.extract_v2_dml_info(&req)?;

        trace!(namespace=%write_info.namespace, "processing delete request");

        // Read the HTTP body and convert it to a str.
        let body = self.read_body(req).await?;
        let body = std::str::from_utf8(&body).map_err(Error::NonUtf8Body)?;

        // Parse and extract table name (which can be empty), start, stop, and predicate
        let parsed_delete = parse_http_delete_request(body)?;
        let predicate = parse_delete_predicate(
            &parsed_delete.start_time,
            &parsed_delete.stop_time,
            &parsed_delete.predicate,
        )?;

        debug!(
            table_name=%parsed_delete.table_name,
            predicate = %parsed_delete.predicate,
            start=%parsed_delete.start_time,
            stop=%parsed_delete.stop_time,
            body_size=body.len(),
            namespace=%write_info.namespace,
            "routing delete"
        );

        let namespace_id = self
            .namespace_resolver
            .get_namespace_id(&write_info.namespace)
            .await?;

        self.dml_handler
            .delete(
                &write_info.namespace,
                namespace_id,
                parsed_delete.table_name.as_str(),
                &predicate,
                span_ctx,
            )
            .await
            .map_err(Into::into)?;

        self.delete_metric_body_size.inc(body.len() as _);

        // TODO pass back write summaries for deletes as well
        // https://github.com/influxdata/influxdb_iox/issues/4209
        Ok(WriteSummary::default())
    }

    /// Parse the request's body into raw bytes, applying the configured size
    /// limits and decoding any content encoding.
    async fn read_body(&self, req: hyper::Request<Body>) -> Result<Bytes, Error> {
        let encoding = req
            .headers()
            .get(&CONTENT_ENCODING)
            .map(|v| v.to_str().map_err(Error::NonUtf8ContentHeader))
            .transpose()?;
        let ungzip = match encoding {
            None => false,
            Some("gzip") => true,
            Some(v) => return Err(Error::InvalidContentEncoding(v.to_string())),
        };

        let mut payload = req.into_body();

        let mut body = BytesMut::new();
        while let Some(chunk) = payload.next().await {
            let chunk = chunk.map_err(Error::ClientHangup)?;
            // limit max size of in-memory payload
            if (body.len() + chunk.len()) > self.max_request_bytes {
                return Err(Error::RequestSizeExceeded(self.max_request_bytes));
            }
            body.extend_from_slice(&chunk);
        }
        let body = body.freeze();

        // If the body is not compressed, return early.
        if !ungzip {
            return Ok(body);
        }

        // Unzip the gzip-encoded content
        use std::io::Read;
        let decoder = flate2::read::GzDecoder::new(&body[..]);

        // Read at most max_request_bytes bytes to prevent a decompression bomb
        // based DoS.
        //
        // In order to detect if the entire stream ahs been read, or truncated,
        // read an extra byte beyond the limit and check the resulting data
        // length - see the max_request_size_truncation test.
        let mut decoder = decoder.take(self.max_request_bytes as u64 + 1);
        let mut decoded_data = Vec::new();
        decoder
            .read_to_end(&mut decoded_data)
            .map_err(Error::InvalidGzip)?;

        // If the length is max_size+1, the body is at least max_size+1 bytes in
        // length, and possibly longer, but truncated.
        if decoded_data.len() > self.max_request_bytes {
            return Err(Error::RequestSizeExceeded(self.max_request_bytes));
        }

        Ok(decoded_data.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dml_handlers::{
            mock::{MockDmlHandler, MockDmlHandlerCall},
            CachedServiceProtectionLimit,
        },
        namespace_resolver::{mock::MockNamespaceResolver, NamespaceCreationError},
        server::http::mt::MultiTenantRequestParser,
    };
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use data_types::{NamespaceId, NamespaceMappingError, NamespaceNameError, TableId};
    use hyper::header::HeaderValue;
    use metric::{Attributes, Metric, U64Counter};
    use serde::de::Error as _;
    use std::{sync::Arc, time::Duration};
    use test_helpers::timeout::FutureTimeout;
    use tokio_stream::wrappers::ReceiverStream;

    const MAX_BYTES: usize = 1024;

    fn summary() -> WriteSummary {
        WriteSummary::default()
    }

    fn assert_metric_hit(metrics: &metric::Registry, name: &'static str, value: Option<u64>) {
        let counter = metrics
            .get_instrument::<Metric<U64Counter>>(name)
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[]))
            .expect("failed to get observer")
            .fetch();

        if let Some(want) = value {
            assert_eq!(want, counter, "metric does not have expected value");
        } else {
            assert!(counter > 0, "metric {name} did not record any values");
        }
    }

    #[derive(Debug, Error)]
    enum MockError {
        #[error("bad stuff")]
        Terrible,
    }

    // This test ensures the request limiter drops requests once the configured
    // number of simultaneous requests are being serviced.
    #[tokio::test]
    async fn test_request_limit_enforced() {
        let mock_namespace_resolver =
            MockNamespaceResolver::default().with_mapping("bananas", NamespaceId::new(42));

        let dml_handler = Arc::new(MockDmlHandler::default());
        let metrics = Arc::new(metric::Registry::default());
        let dml_info_extractor = &MultiTenantRequestParser;
        let delegate = Arc::new(HttpDelegate::new(
            MAX_BYTES,
            1,
            mock_namespace_resolver,
            Arc::clone(&dml_handler),
            None,
            &metrics,
            dml_info_extractor,
        ));

        // Use a channel to hold open the request.
        //
        // This causes the request handler to block reading the request body
        // until tx is dropped and the body stream ends, completing the body and
        // unblocking the request handler.
        let (body_1_tx, rx) = tokio::sync::mpsc::channel(1);
        let request_1 = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .body(Body::wrap_stream(ReceiverStream::new(rx)))
            .unwrap();

        // Spawn the first request and push at least 2 body chunks through tx.
        //
        // Spawning and writing through tx will avoid any race between which
        // request handler task is scheduled first by ensuring this request is
        // being actively read from - the first send() could fill the channel
        // buffer of 1, and therefore successfully returning from the second
        // send() MUST indicate the stream is being read by the handler (and
        // therefore the task has spawned and the request is actively being
        // serviced).
        let req_1 = tokio::spawn({
            let delegate = Arc::clone(&delegate);
            async move { delegate.route(request_1).await }
        });
        body_1_tx
            .send(Ok("cpu "))
            .await
            .expect("req1 closed channel");
        body_1_tx
            .send(Ok("field=1i"))
            // Never hang if there is no handler reading this request
            .with_timeout_panic(Duration::from_secs(1))
            .await
            .expect("req1 closed channel");

        //
        // At this point we can be certain that request 1 is being actively
        // serviced, and the HTTP server is in a state that should cause the
        // immediate drop of any subsequent requests.
        //

        assert_metric_hit(&metrics, "http_request_limit_rejected", Some(0));

        // Retain this tx handle for the second request and use it to prove the
        // request dropped before anything was read from the body - the request
        // should error _before_ anything is sent over tx, and subsequently
        // attempting to send something over tx after the error should fail with
        // a "channel closed" error.
        let (body_2_tx, rx) = tokio::sync::mpsc::channel::<Result<&'static str, MockError>>(1);
        let request_2 = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .body(Body::wrap_stream(ReceiverStream::new(rx)))
            .unwrap();

        // Attempt to service request 2.
        //
        // This should immediately return without requiring any body chunks to
        // be sent through tx.
        let err = delegate
            .route(request_2)
            .with_timeout_panic(Duration::from_secs(1))
            .await
            .expect_err("second request should be rejected");
        assert_matches!(err, Error::RequestLimit);

        // Ensure the "rejected requests" metric was incremented
        assert_metric_hit(&metrics, "http_request_limit_rejected", Some(1));

        // Prove the dropped request body is not being read:
        body_2_tx
            .send(Ok("wat"))
            .await
            .expect_err("channel should be closed");

        // Cause the first request handler to bail, releasing request capacity
        // back to the router.
        body_1_tx
            .send(Err(MockError::Terrible))
            .await
            .expect("req1 closed channel");
        // Wait for the handler to return to avoid any races.
        let req_1 = req_1
            .with_timeout_panic(Duration::from_secs(1))
            .await
            .expect("request 1 handler should not panic")
            .expect_err("request should fail");
        assert_matches!(req_1, Error::ClientHangup(_));

        // And submit a third request that should be serviced now there's no
        // concurrent request being handled.
        let request_3 = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .body(Body::from(""))
            .unwrap();
        delegate
            .route(request_3)
            .with_timeout_panic(Duration::from_secs(1))
            .await
            .expect("empty write should succeed");

        // And the request rejected metric must remain unchanged
        assert_metric_hit(&metrics, "http_request_limit_rejected", Some(1));
    }

    #[derive(Debug)]
    struct MockAuthorizer {}

    #[async_trait]
    impl Authorizer for MockAuthorizer {
        async fn permissions(
            &self,
            token: Option<&[u8]>,
            perms: &[Permission],
        ) -> Result<Vec<Permission>, authz::Error> {
            match token {
                Some(b"GOOD") => Ok(perms.to_vec()),
                Some(b"UGLY") => Err(authz::Error::verification("test", "test error")),
                Some(_) => Ok(vec![]),
                None => Err(authz::Error::NoToken),
            }
        }
    }

    #[tokio::test]
    async fn test_authz() {
        let mock_namespace_resolver =
            MockNamespaceResolver::default().with_mapping("bananas_test", NamespaceId::new(42));

        let dml_handler = Arc::new(
            MockDmlHandler::default()
                .with_write_return([Ok(summary())])
                .with_delete_return([]),
        );
        let metrics = Arc::new(metric::Registry::default());
        let authz = Arc::new(MockAuthorizer {});
        let dml_info_extractor = &MultiTenantRequestParser;
        let delegate = HttpDelegate::new(
            MAX_BYTES,
            1,
            mock_namespace_resolver,
            Arc::clone(&dml_handler),
            Some(authz),
            &metrics,
            dml_info_extractor,
        );

        let request = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .extension(AuthorizationHeaderExtension::new(Some(
                HeaderValue::from_str("Token GOOD").expect("ok"),
            )))
            .body(Body::from("platanos,tag1=A,tag2=B val=42i 123456"))
            .unwrap();

        let got = delegate.route(request).await;
        assert_matches!(got, Ok(_));

        let request = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .extension(AuthorizationHeaderExtension::new(Some(
                HeaderValue::from_str("Token BAD").expect("ok"),
            )))
            .body(Body::from(""))
            .unwrap();

        let got = delegate.route(request).await;
        assert_matches!(got, Err(Error::Forbidden));

        let request = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .body(Body::from(""))
            .unwrap();

        let got = delegate.route(request).await;
        assert_matches!(got, Err(Error::Unauthenticated));

        let request = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .extension(AuthorizationHeaderExtension::new(Some(
                HeaderValue::from_str("Token UGLY").expect("ok"),
            )))
            .body(Body::from(""))
            .unwrap();

        let got = delegate.route(request).await;
        assert_matches!(got, Err(Error::Authorizer(_)));

        let calls = dml_handler.calls();
        assert_matches!(calls.as_slice(), [MockDmlHandlerCall::Write{namespace, ..}] => {
            assert_eq!(namespace, "bananas_test");
        })
    }

    // The display text of Error gets passed through `ioxd_router::IoxHttpErrorAdaptor` then
    // `ioxd_common::http::error::HttpApiError` as the JSON "message" value in error response
    // bodies. These are fixture tests to document error messages that users might see when
    // making requests to `/api/v2/write`.
    macro_rules! check_errors {
        (
            $((                   // This macro expects a list of tuples, each specifying:
                $variant:ident        // - One of the error enum variants
                $(($data:expr))?,     // - If needed, an expression to construct the variant's data
                $msg:expr $(,)?       // - The string expected for `Display`ing this variant
            )),*,
        ) => {
            // Generate code that contains all possible error variants, to ensure a compiler error
            // if any errors are not explicitly covered in this test.
            #[test]
            fn all_error_variants_are_checked() {
                #[allow(dead_code)]
                fn all_documented(ensure_all_error_variants_are_checked: Error) {
                    #[allow(unreachable_patterns)]
                    match ensure_all_error_variants_are_checked {
                        $(Error::$variant { .. } => {},)*
                        // If this test doesn't compile because of a non-exhaustive pattern,
                        // a variant needs to be added to the `check_errors!` call with the
                        // expected `to_string()` text.
                    }
                }
            }

            // A test that covers all errors given to this macro.
            #[tokio::test]
            async fn error_messages_match() {
                // Generate an assert for each error given to this macro.
                $(
                    let e = Error::$variant $(($data))?;
                    assert_eq!(e.to_string(), $msg);
                )*
            }

            #[test]
            fn print_out_error_text() {
                println!("{}", concat!($(stringify!($variant), "\t", $msg, "\n",)*),)
            }
        };
    }

    check_errors! {
        (
            NoHandler,
            "not found",
        ),

        (InvalidOrgBucket(OrgBucketError::NotSpecified), "no org/bucket destination provided"),

        (
            InvalidOrgBucket({
                let e = serde::de::value::Error::custom("[deserialization error]");
                OrgBucketError::DecodeFail(e)
            }),
            "failed to deserialize org/bucket/precision in request: [deserialization error]",
        ),

        (
            InvalidOrgBucket(OrgBucketError::MappingFail(NamespaceMappingError::NotSpecified)),
            "missing org/bucket value",
        ),

        (
            InvalidOrgBucket({
                let e = NamespaceNameError::LengthConstraint { name: "[too long name]".into() };
                let e = NamespaceMappingError::InvalidNamespaceName { source: e };
                OrgBucketError::MappingFail(e)
            }),
            "Invalid namespace name: \
             Namespace name [too long name] length must be between 1 and 64 characters",
        ),

        (
            InvalidDatabaseRp({
                let e = NamespaceNameError::LengthConstraint { name: "[too long name]".into() };
                let e = NamespaceMappingError::InvalidNamespaceName { source: e };
                DatabaseRpError::MappingFail(e)
            }),
            "Invalid namespace name: \
             Namespace name [too long name] length must be between 1 and 64 characters",
        ),

        (
            NonUtf8Body(std::str::from_utf8(&[0, 159]).unwrap_err()),
            "body content is not valid utf8: invalid utf-8 sequence of 1 bytes from index 1",
        ),

        (
            NonUtf8ContentHeader({
                hyper::header::HeaderValue::from_bytes(&[159]).unwrap().to_str().unwrap_err()
            }),
            "invalid content-encoding header: failed to convert header to a str",
        ),

        (
            InvalidContentEncoding("[invalid content encoding value]".into()),
            "unacceptable content-encoding: [invalid content encoding value]",
        ),

        (
            ClientHangup({
                let url = "wrong://999.999.999.999:999999".parse().unwrap();
                hyper::Client::new().get(url).await.unwrap_err()
            }),
            "client disconnected",
        ),

        (
            RequestSizeExceeded(1337),
            "max request size (1337 bytes) exceeded",
        ),

        (
            InvalidGzip(std::io::Error::new(std::io::ErrorKind::Other, "[io Error]")),
            "error decoding gzip stream: [io Error]",
        ),

        (
            ParseLineProtocol(mutable_batch_lp::Error::LineProtocol {
                source: influxdb_line_protocol::Error::FieldSetMissing,
                line: 42,
            }),
            "failed to parse line protocol: \
            error parsing line 42 (1-based): No fields were provided",
        ),

        (
            ParseLineProtocol(mutable_batch_lp::Error::Write {
                source: mutable_batch_lp::LineWriteError::DuplicateTag {
                    name: "host".into(),
                },
                line: 42,
            }),
            "failed to parse line protocol: \
            error writing line 42: \
            the tag 'host' is specified more than once with conflicting values",
        ),

        (
            ParseLineProtocol(mutable_batch_lp::Error::Write {
                source: mutable_batch_lp::LineWriteError::ConflictedFieldTypes {
                    name: "bananas".into(),
                },
                line: 42,
            }),
            "failed to parse line protocol: \
            error writing line 42: \
            the field 'bananas' is specified more than once with conflicting types",
        ),

        (
            ParseLineProtocol(mutable_batch_lp::Error::EmptyPayload),
            "failed to parse line protocol: empty write payload",
        ),

        (
            ParseLineProtocol(mutable_batch_lp::Error::TimestampOverflow),
            "failed to parse line protocol: timestamp overflows i64",
        ),

        (
            ParseDelete({
                predicate::delete_predicate::Error::InvalidSyntax { value: "[syntax]".into() }
            }),
            "failed to parse delete predicate: Invalid predicate syntax: ([syntax])",
        ),

        (
            ParseHttpDelete({
                delete_predicate::Error::TableInvalid { value: "[table name]".into() }
            }),
            "failed to parse delete predicate from http request: \
             Invalid table name in delete '[table name]'"
        ),

        (
            DmlHandler(DmlError::NamespaceNotFound("[namespace name]".into())),
            "dml handler error: namespace [namespace name] does not exist",
        ),

        (
            NamespaceResolver({
                let e = iox_catalog::interface::Error::NameExists { name: "[name]".into() };
                crate::namespace_resolver::Error::Lookup(e)
            }),
            "failed to resolve namespace ID: \
             name [name] already exists",
        ),

        (
            NamespaceResolver(
                crate::namespace_resolver::Error::Create(NamespaceCreationError::Reject("bananas".to_string()))
            ),
            "rejecting write due to non-existing namespace: bananas",
        ),

        (
            RequestLimit,
            "this service is overloaded, please try again later",
        ),

        (
            Unauthenticated,
            "authentication required",
        ),

        (
            Forbidden,
            "access denied",
        ),

        (
            Authorizer(
                authz::Error::verification("bananas", NamespaceCreationError::Reject("bananas".to_string()))
            ),
            "token verification not possible: bananas",
        ),

        (
            DmlHandler(DmlError::Schema(SchemaError::ServiceLimit(Box::new(CachedServiceProtectionLimit::Column {
                table_name: "bananas".to_string(),
                existing_column_count: 42,
                merged_column_count: 4242,
                max_columns_per_table: 24,
            })))),
            "dml handler error: service limit reached: couldn't create columns in table `bananas`; table contains 42 \
            existing columns, applying this write would result in 4242 columns, limit is 24",
        ),

        (
            DmlHandler(DmlError::Schema(SchemaError::ServiceLimit(Box::new(CachedServiceProtectionLimit::Table {
                existing_table_count: 42,
                merged_table_count: 4242,
                table_count_limit: 24,
            })))),
            "dml handler error: service limit reached: couldn't create new table; namespace contains 42 existing \
            tables, applying this write would result in 4242 tables, limit is 24",
        ),

        (
            DmlHandler(DmlError::Schema(SchemaError::ServiceLimit(Box::new(iox_catalog::interface::Error::ColumnCreateLimitError {
                column_name: "bananas".to_string(),
                table_id: TableId::new(42),
            })))),
            "dml handler error: service limit reached: couldn't create column bananas in table 42; limit reached on \
            namespace",
        ),

        (
            DmlHandler(DmlError::Schema(SchemaError::ServiceLimit(Box::new(iox_catalog::interface::Error::TableCreateLimitError {
                table_name: "bananas".to_string(),
                namespace_id: NamespaceId::new(42),
            })))),
            "dml handler error: service limit reached: couldn't create table bananas; limit reached on namespace 42",
        ),
    }
}
