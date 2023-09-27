use std::{any::Any, collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use backoff::BackoffConfig;
use data_types::{ChunkId, NamespaceId};
use datafusion::{
    error::DataFusionError, physical_plan::stream::RecordBatchStreamAdapter, prelude::Expr,
};
use futures::{StreamExt, TryStreamExt};
use ingester_query_client::{
    interface::{Request, ResponseMetadata, ResponsePartition, ResponsePayload},
    layer::{Layer, QueryResponse},
    layers::{
        backoff::BackoffLayer,
        circuit_breaker::CircuitBreakerLayer,
        deserialize::DeserializeLayer,
        error_to_option::ErrorToOptionLayer,
        logging::LoggingLayer,
        network::{NetworkLayer, Uri},
        reconnect_on_error::ReconnectOnErrorLayer,
        serialize::{HeaderName, SerializeLayer},
        testing::TestLayer,
        tracing::TracingLayer,
    },
};
use iox_time::TimeProvider;
use metric::Registry;
use schema::Schema;
use trace::span::{Span, SpanRecorder};

use crate::{cache::namespace::CachedTable, ingester::IngesterChunkData};

use self::payload_distributor::PayloadDistributor;

use super::{DynError, IngesterConnection, IngesterPartition};

mod payload_distributor;

/// Create a new set of connections given ingester configurations
pub fn create_ingester_connections(
    ingester_addresses: Vec<Arc<str>>,
    time_provider: Arc<dyn TimeProvider>,
    metric_registry: &Registry,
    open_circuit_after_n_errors: u64,
    trace_context_header_name: &str,
) -> Arc<dyn IngesterConnection> {
    Arc::new(IngesterConnectionImpl::new(
        ingester_addresses,
        time_provider,
        metric_registry,
        open_circuit_after_n_errors,
        trace_context_header_name,
        None,
    ))
}

/// High-level client.
type IngesterClient = Arc<
    dyn Layer<
        Request = (Request, Option<Span>),
        ResponseMetadata = Option<ResponseMetadata>,
        ResponsePayload = ResponsePayload,
    >,
>;

/// Test layer for this specific client.
type TestLayerImpl = Arc<TestLayer<(Request, Option<Span>), ResponseMetadata, ResponsePayload>>;

#[derive(Debug)]
struct IngesterConnectionImpl {
    connections: Vec<(Arc<str>, IngesterClient)>,
}

impl IngesterConnectionImpl {
    fn new(
        ingester_addresses: Vec<Arc<str>>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &Registry,
        open_circuit_after_n_errors: u64,
        trace_context_header_name: &str,
        test_layers: Option<HashMap<Arc<str>, TestLayerImpl>>,
    ) -> Self {
        Self {
            connections: ingester_addresses
                .into_iter()
                .map(|addr| {
                    let conn = Self::new_conn(
                        Arc::clone(&addr),
                        open_circuit_after_n_errors,
                        trace_context_header_name,
                        Arc::clone(&time_provider),
                        metric_registry,
                        test_layers.as_ref().map(|test_layers| {
                            Arc::clone(test_layers.get(&addr).expect("must cover all test layers"))
                        }),
                    );
                    (addr, conn)
                })
                .collect(),
        }
    }

    fn new_conn(
        addr: Arc<str>,
        open_circuit_after_n_errors: u64,
        trace_context_header_name: &str,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &Registry,
        test_layer: Option<TestLayerImpl>,
    ) -> IngesterClient {
        let uri: Uri = addr.as_ref().try_into().expect("valid URI");
        let trace_context_header_name: HeaderName =
            trace_context_header_name.try_into().expect("valid header");
        let l = ReconnectOnErrorLayer::new(move || {
            if let Some(test_layer) = &test_layer {
                Arc::clone(test_layer)
                    as Arc<
                        dyn Layer<
                            Request = (Request, Option<Span>),
                            ResponseMetadata = ResponseMetadata,
                            ResponsePayload = ResponsePayload,
                        >,
                    >
            } else {
                let l = NetworkLayer::new(uri.clone());
                let l = SerializeLayer::new(l, trace_context_header_name.clone());
                let l = DeserializeLayer::new(l);
                Arc::new(l) as _
            }
        });

        let l = LoggingLayer::new(l, Arc::clone(&addr));

        let l = CircuitBreakerLayer::new(
            l,
            Arc::clone(&addr),
            time_provider,
            metric_registry,
            open_circuit_after_n_errors,
            BackoffConfig {
                init_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(60),
                base: 3.0,
                deadline: None,
            },
        );

        let l = BackoffLayer::new(
            l,
            BackoffConfig {
                init_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(1),
                base: 3.0,
                deadline: Some(Duration::from_secs(10)),
            },
        );

        let l = TracingLayer::new(l, addr);

        let l = ErrorToOptionLayer::new(l);

        Arc::new(l)
    }

    fn convert_response(
        resp: QueryResponse<Option<ResponseMetadata>, ResponsePayload>,
    ) -> Result<Vec<IngesterPartition>, DynError> {
        let QueryResponse { metadata, payload } = resp;

        let Some(metadata) = metadata else {
            return Ok(vec![]);
        };

        let ResponseMetadata {
            ingester_uuid,
            persist_counter,
            table_schema: _,
            partitions,
        } = metadata;

        let distributor = Arc::new(PayloadDistributor::new(
            payload
                .map_ok(|p| (p.partition_id.clone(), p))
                .map_err(Arc::new),
            partitions.iter().map(|p| p.id.clone()),
        ));

        partitions
            .into_iter()
            .map(|partition| {
                let ResponsePartition {
                    id,
                    t_min_max,
                    schema,
                } = partition;

                let distributor = Arc::clone(&distributor);
                let id_captured = id.clone();
                let schema_captured = Arc::clone(&schema);
                let data = IngesterChunkData::Stream(Arc::new(move || {
                    let stream = distributor
                        .stream(&id_captured)
                        .map_ok(|p| p.batch)
                        .map_err(|e| DataFusionError::External(e.into()));
                    let stream =
                        RecordBatchStreamAdapter::new(Arc::clone(&schema_captured), stream);
                    Box::pin(stream)
                }));

                let chunk_id = ChunkId::new();
                let schema = Schema::try_from(schema)?;

                let partition = IngesterPartition::new(ingester_uuid, id, persist_counter as u64)
                    .push_chunk(chunk_id, schema, data, t_min_max);
                Ok(partition)
            })
            .collect()
    }
}

#[async_trait]
impl IngesterConnection for IngesterConnectionImpl {
    async fn partitions(
        &self,
        namespace_id: NamespaceId,
        cached_table: Arc<CachedTable>,
        columns: Vec<String>,
        filters: &[Expr],
        span: Option<Span>,
    ) -> Result<Vec<IngesterPartition>, DynError> {
        let span_recorder = SpanRecorder::new(span);

        let request = Request {
            namespace_id,
            table_id: cached_table.id,
            columns,
            filters: filters.to_vec(),
        };

        // need to collect the futures first before feeding them into a stream to avoid a rustc higher-ranked lifetime error
        let futures = self
            .connections
            .iter()
            .map(|(addr, conn)| {
                let addr = Arc::clone(addr);
                let conn = Arc::clone(conn);
                let request = request.clone();
                let span = span_recorder.child_span(format!("ingester: {addr}"));

                async move {
                    let resp = conn.query((request, span)).await?;
                    Self::convert_response(resp)
                }
            })
            .collect::<Vec<_>>();

        let responses = futures::stream::iter(futures)
            .buffered(self.connections.len())
            .try_collect::<Vec<_>>()
            .await?;

        let mut partitions = responses.into_iter().flatten().collect::<Vec<_>>();

        // normalized response
        partitions.sort_by(|p1, p2| {
            (&p1.partition_id, p1.ingester_uuid).cmp(&(&p2.partition_id, p2.ingester_uuid))
        });

        Ok(partitions)
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

#[cfg(test)]
mod tests {
    use arrow::record_batch::RecordBatch;
    use data_types::{
        PartitionHashId, PartitionKey, TableId, TimestampMinMax, TransitionPartitionId,
    };
    use ingester_query_client::layers::testing::TestResponse;
    use iox_time::SystemProvider;
    use schema::{InfluxFieldType, SchemaBuilder, TIME_COLUMN_NAME};
    use uuid::Uuid;

    use crate::ingester::IngesterChunk;

    use super::*;

    #[tokio::test]
    async fn test_smoke() {
        let time_provider = Arc::new(SystemProvider::new());
        let metric_registry = Registry::new();

        let p_id_1 = p_id(1);
        let p_id_2 = p_id(2);

        let ingester_1 = Arc::from("http://ingester-1");
        let ingester_2 = Arc::from("http://ingester-2");

        let ingester_uuid_1 = Uuid::from_u128(1);
        let ingester_uuid_2 = Uuid::from_u128(2);

        let test_layer_1 = Arc::new(TestLayer::default());
        test_layer_1.mock_response(
            TestResponse::ok(ResponseMetadata {
                ingester_uuid: ingester_uuid_1,
                persist_counter: 10,
                table_schema: schema().as_arrow(),
                partitions: vec![
                    ResponsePartition {
                        id: p_id_1.clone(),
                        t_min_max: TimestampMinMax::new(10, 20),
                        schema: schema().as_arrow(),
                    },
                    ResponsePartition {
                        id: p_id_2.clone(),
                        t_min_max: TimestampMinMax::new(10, 20),
                        schema: schema().as_arrow(),
                    },
                ],
            })
            .with_ok_payload(ResponsePayload {
                partition_id: p_id_1.clone(),
                batch: batch(),
            })
            .with_ok_payload(ResponsePayload {
                partition_id: p_id_2.clone(),
                batch: batch(),
            })
            .with_ok_payload(ResponsePayload {
                partition_id: p_id_1.clone(),
                batch: batch(),
            }),
        );
        let test_layer_2 = Arc::new(TestLayer::default());
        test_layer_2.mock_response(
            TestResponse::ok(ResponseMetadata {
                ingester_uuid: ingester_uuid_2,
                persist_counter: 2,
                table_schema: schema().as_arrow(),
                partitions: vec![ResponsePartition {
                    id: p_id_1.clone(),
                    t_min_max: TimestampMinMax::new(10, 20),
                    schema: schema().as_arrow(),
                }],
            })
            .with_ok_payload(ResponsePayload {
                partition_id: p_id_1.clone(),
                batch: batch(),
            }),
        );

        let conn_impl = IngesterConnectionImpl::new(
            vec![Arc::clone(&ingester_1), Arc::clone(&ingester_2)],
            time_provider,
            &metric_registry,
            1,
            "trace-id",
            Some(HashMap::from([
                (ingester_1, test_layer_1),
                (ingester_2, test_layer_2),
            ])),
        );

        let partitions = conn_impl
            .partitions(
                NamespaceId::new(1),
                cached_table(),
                vec![
                    String::from("bar"),
                    String::from("foo"),
                    String::from(TIME_COLUMN_NAME),
                ],
                &[],
                None,
            )
            .await
            .unwrap();
        assert_eq!(partitions.len(), 3);

        let p_1 = &partitions[0];
        assert_eq!(p_1.partition_id, p_id_1);
        assert_eq!(p_1.ingester_uuid, ingester_uuid_1);
        assert_eq!(p_1.chunks.len(), 1);

        let c_1 = &p_1.chunks[0];
        let batches_1 = read_batches(c_1).await;
        assert_eq!(batches_1.len(), 2);

        let p_2 = &partitions[1];
        assert_eq!(p_2.partition_id, p_id_1);
        assert_eq!(p_2.ingester_uuid, ingester_uuid_2);
        assert_eq!(p_2.chunks.len(), 1);

        let c_2 = &p_2.chunks[0];
        let batches_2 = read_batches(c_2).await;
        assert_eq!(batches_2.len(), 1);

        let p_3 = &partitions[2];
        assert_eq!(p_3.partition_id, p_id_2);
        assert_eq!(p_3.ingester_uuid, ingester_uuid_1);
        assert_eq!(p_3.chunks.len(), 1);

        let c_3 = &p_3.chunks[0];
        let batches_3 = read_batches(c_3).await;
        assert_eq!(batches_3.len(), 1);
    }

    fn schema() -> Schema {
        SchemaBuilder::new()
            .influx_field("bar", InfluxFieldType::Float)
            .influx_field("baz", InfluxFieldType::Float)
            .influx_field("foo", InfluxFieldType::Float)
            .timestamp()
            .build()
            .unwrap()
    }

    fn batch() -> RecordBatch {
        RecordBatch::new_empty(schema().as_arrow())
    }

    fn cached_table() -> Arc<CachedTable> {
        Arc::new(CachedTable {
            id: TableId::new(2),
            schema: schema(),
            column_id_map: Default::default(),
            column_id_map_rev: Default::default(),
            primary_key_column_ids: Default::default(),
            partition_template: Default::default(),
        })
    }

    fn p_id(i: i64) -> TransitionPartitionId {
        TransitionPartitionId::Deterministic(PartitionHashId::new(
            TableId::new(1),
            &PartitionKey::from(format!("{i}")),
        ))
    }

    async fn read_batches(chunk: &IngesterChunk) -> Vec<RecordBatch> {
        match &chunk.data {
            IngesterChunkData::Eager(_) => panic!("should not be eager"),
            IngesterChunkData::Stream(f) => f().try_collect().await.unwrap(),
        }
    }
}
