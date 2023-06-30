use std::{collections::BTreeMap, sync::Arc, time::Duration};

use data_types::{
    partition_template::TablePartitionTemplateOverride, NamespaceId, PartitionId, PartitionKey,
    SequenceNumber, TableId,
};
use iox_catalog::{interface::Catalog, test_helpers::arbitrary_namespace};
use lazy_static::lazy_static;
use mutable_batch_lp::lines_to_batches;
use schema::Projection;
use trace::ctx::SpanContext;

use crate::{
    buffer_tree::{
        namespace::{
            name_resolver::{mock::MockNamespaceNameProvider, NamespaceNameProvider},
            NamespaceName,
        },
        partition::{PartitionData, SortKeyState},
        table::{
            name_resolver::{mock::MockTableNameProvider, TableNameProvider},
            TableName,
        },
    },
    deferred_load::DeferredLoad,
    dml_payload::write::{PartitionedData, TableData, WriteOperation},
};

pub(crate) const ARBITRARY_PARTITION_ID: PartitionId = PartitionId::new(1);
pub(crate) const ARBITRARY_NAMESPACE_ID: NamespaceId = NamespaceId::new(3);
pub(crate) const ARBITRARY_TABLE_ID: TableId = TableId::new(4);
pub(crate) const ARBITRARY_PARTITION_KEY_STR: &str = "platanos";

pub(crate) fn defer_namespace_name_1_sec() -> Arc<DeferredLoad<NamespaceName>> {
    Arc::new(DeferredLoad::new(
        Duration::from_secs(1),
        async { ARBITRARY_NAMESPACE_NAME.clone() },
        &metric::Registry::default(),
    ))
}

pub(crate) fn defer_namespace_name_1_ms() -> Arc<DeferredLoad<NamespaceName>> {
    Arc::new(DeferredLoad::new(
        Duration::from_millis(1),
        async { ARBITRARY_NAMESPACE_NAME.clone() },
        &metric::Registry::default(),
    ))
}

pub(crate) fn defer_table_name_1_sec() -> Arc<DeferredLoad<TableName>> {
    Arc::new(DeferredLoad::new(
        Duration::from_secs(1),
        async { ARBITRARY_TABLE_NAME.clone() },
        &metric::Registry::default(),
    ))
}

pub(crate) fn defer_partition_template_1_sec() -> Arc<DeferredLoad<TablePartitionTemplateOverride>>
{
    Arc::new(DeferredLoad::new(
        Duration::from_secs(1),
        async { todo!() }, // TODO(savage): What is an arbitrary partition tempalte? Not sure if this makes sense.
        &metric::Registry::default(),
    ))
}

lazy_static! {
    pub(crate) static ref ARBITRARY_PARTITION_KEY: PartitionKey =
        PartitionKey::from(ARBITRARY_PARTITION_KEY_STR);
    pub(crate) static ref ARBITRARY_NAMESPACE_NAME: NamespaceName =
        NamespaceName::from("namespace-bananas");
    pub(crate) static ref ARBITRARY_NAMESPACE_NAME_PROVIDER: Arc<dyn NamespaceNameProvider> =
        Arc::new(MockNamespaceNameProvider::new(&**ARBITRARY_NAMESPACE_NAME));
    pub(crate) static ref ARBITRARY_TABLE_NAME: TableName = TableName::from("bananas");
    pub(crate) static ref ARBITRARY_TABLE_NAME_PROVIDER: Arc<dyn TableNameProvider> =
        Arc::new(MockTableNameProvider::new(&**ARBITRARY_TABLE_NAME));
}

/// Build a [`PartitionData`] with mostly arbitrary-yet-valid values for tests.
#[derive(Debug, Clone, Default)]
pub(crate) struct PartitionDataBuilder {
    partition_id: Option<PartitionId>,
    partition_key: Option<PartitionKey>,
    namespace_id: Option<NamespaceId>,
    table_id: Option<TableId>,
    table_name_loader: Option<Arc<DeferredLoad<TableName>>>,
    namespace_loader: Option<Arc<DeferredLoad<NamespaceName>>>,
    sort_key: Option<SortKeyState>,
}

impl PartitionDataBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_partition_id(mut self, partition_id: PartitionId) -> Self {
        self.partition_id = Some(partition_id);
        self
    }

    pub(crate) fn with_partition_key(mut self, partition_key: PartitionKey) -> Self {
        self.partition_key = Some(partition_key);
        self
    }

    pub(crate) fn with_namespace_id(mut self, namespace_id: NamespaceId) -> Self {
        self.namespace_id = Some(namespace_id);
        self
    }

    pub(crate) fn with_table_id(mut self, table_id: TableId) -> Self {
        self.table_id = Some(table_id);
        self
    }

    pub(crate) fn with_table_name_loader(
        mut self,
        table_name_loader: Arc<DeferredLoad<TableName>>,
    ) -> Self {
        self.table_name_loader = Some(table_name_loader);
        self
    }

    pub(crate) fn with_namespace_loader(
        mut self,
        namespace_loader: Arc<DeferredLoad<NamespaceName>>,
    ) -> Self {
        self.namespace_loader = Some(namespace_loader);
        self
    }

    pub(crate) fn with_sort_key_state(mut self, sort_key_state: SortKeyState) -> Self {
        self.sort_key = Some(sort_key_state);
        self
    }

    /// Generate a valid [`PartitionData`] for use in tests where the exact values (or at least
    /// some of them) don't particularly matter.
    pub(crate) fn build(self) -> PartitionData {
        PartitionData::new(
            self.partition_id.unwrap_or(ARBITRARY_PARTITION_ID),
            None,
            self.partition_key
                .unwrap_or_else(|| ARBITRARY_PARTITION_KEY.clone()),
            self.namespace_id.unwrap_or(ARBITRARY_NAMESPACE_ID),
            self.namespace_loader
                .unwrap_or_else(defer_namespace_name_1_sec),
            self.table_id.unwrap_or(ARBITRARY_TABLE_ID),
            self.table_name_loader
                .unwrap_or_else(defer_table_name_1_sec),
            self.sort_key.unwrap_or(SortKeyState::Provided(None)),
        )
    }
}

/// Generate a [`RecordBatch`] & [`Schema`] with the specified columns and
/// values:
///
/// ```
/// // Generate a two column batch ("a" and "b") with the given types & values:
/// let (batch, schema) = make_batch!(
///     Int64Array("a" => vec![1, 2, 3, 4]),
///     Float32Array("b" => vec![4.1, 4.2, 4.3, 4.4]),
/// );
/// ```
///
/// # Panics
///
/// Panics if the batch cannot be constructed from the provided inputs.
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
/// [`RecordBatch`]: arrow::datatypes::Schema
#[macro_export]
macro_rules! make_batch {(
        $(
            $ty:tt($name:literal => $v:expr),
        )+
    ) => {{
        use std::sync::Arc;
        use arrow::{array::Array, datatypes::{Field, Schema}, record_batch::RecordBatch};

        // Generate the data arrays
        let data = vec![
            $(Arc::new($ty::from($v)) as Arc<dyn Array>,)+
        ];

        // Generate the field types for the schema
        let schema = Arc::new(Schema::new(vec![
            $(Field::new($name, $ty::from($v).data_type().clone(), true),)+
        ]));

        (
            RecordBatch::try_new(Arc::clone(&schema), data)
                .expect("failed to make batch"),
            schema
        )
    }}
}

/// Construct a [`PartitionStream`] from the given partitions & batches.
///
/// This example constructs a [`PartitionStream`] yielding two partitions
/// (with IDs 1 & 2), the former containing two [`RecordBatch`] and the
/// latter containing one.
///
/// See [`make_batch`] for a handy way to construct the [`RecordBatch`].
///
/// ```
/// let stream = make_partition_stream!(
///     PartitionId::new(1) => [
///         make_batch!(
///             Int64Array("a" => vec![1, 2, 3, 4, 5]),
///             Float32Array("b" => vec![4.1, 4.2, 4.3, 4.4, 5.0]),
///         ),
///         make_batch!(
///             Int64Array("c" => vec![1, 2, 3, 4, 5]),
///         ),
///     ],
///     PartitionId::new(2) => [
///         make_batch!(
///             Float32Array("d" => vec![1.1, 2.2, 3.3, 4.4, 5.5]),
///         ),
///     ],
/// );
/// ```
#[macro_export]
macro_rules! make_partition_stream {
        (
            $(
                $id:expr => [$($batch:expr,)+],
            )+
        ) => {{
            use arrow::datatypes::Schema;
            use $crate::query::{response::PartitionStream, partition_response::PartitionResponse};
            use futures::stream;

            PartitionStream::new(stream::iter([
                $({
                    let mut batches = vec![];
                    let mut schema = Schema::empty();
                    $(
                        let (batch, this_schema) = $batch;
                        batches.push(batch);
                        schema = Schema::try_merge([
                            schema,
                            (*this_schema).clone()
                        ]).expect("incompatible batch schemas");
                    )+
                    drop(schema);

                    PartitionResponse::new(
                        batches,
                        // Using the $id as both the PartitionId and the TableId in the
                        // PartitionHashId is a temporary way to reduce duplication in tests where
                        // the important part is which batches are in the same partition and which
                        // batches are in a different partition, not what the actual identifier
                        // values are. This will go away when the ingester no longer sends
                        // PartitionIds.
                        data_types::PartitionId::new($id),
                        Some(
                            PartitionHashId::new(
                                TableId::new($id),
                                &*ARBITRARY_PARTITION_KEY
                            )
                        ),
                        42,
                    )
                },)+
            ]))
        }};
    }

/// Construct a [`WriteOperation`] with the specified parameters, for LP that contains
/// a single table identified by `table_id`.
///
/// # Panics
///
/// This method panics if `lines` contains data for more than one table.
#[track_caller]
pub(crate) fn make_write_op(
    partition_key: &PartitionKey,
    namespace_id: NamespaceId,
    table_name: &str,
    table_id: TableId,
    sequence_number: i64,
    lines: &str,
    span_ctx: Option<SpanContext>,
) -> WriteOperation {
    let mut tables_by_name = lines_to_batches(lines, 0).expect("invalid LP");
    assert_eq!(
        tables_by_name.len(),
        1,
        "make_write_op only supports 1 table in the LP"
    );

    let tables_by_id = [(
        table_id,
        tables_by_name
            .remove(table_name)
            .expect("table_name does not exist in LP"),
    )]
    .into_iter()
    .map(|(table_id, mutable_batch)| {
        (
            table_id,
            TableData::new(
                table_id,
                PartitionedData::new(SequenceNumber::new(sequence_number), mutable_batch),
            ),
        )
    })
    .collect();

    WriteOperation::new(namespace_id, tables_by_id, partition_key.clone(), span_ctx)
}

pub(crate) async fn populate_catalog(
    catalog: &dyn Catalog,
    namespace: &str,
    table: &str,
) -> (NamespaceId, TableId) {
    let mut c = catalog.repositories().await;
    let ns_id = arbitrary_namespace(&mut *c, namespace).await.id;
    let table_id = c
        .tables()
        .create(table, Default::default(), ns_id)
        .await
        .unwrap()
        .id;

    (ns_id, table_id)
}

/// Assert `a` and `b` have identical metadata, and that when converting
/// them to Arrow batches they produces identical output.
#[track_caller]
pub(crate) fn assert_dml_writes_eq(a: WriteOperation, b: WriteOperation) {
    assert_eq!(a.namespace(), b.namespace(), "namespace");
    assert_eq!(a.tables().count(), b.tables().count(), "table count");
    assert_eq!(a.partition_key(), b.partition_key(), "partition key");

    // Assert sequence numbers were reassigned
    for (a_table, b_table) in a.tables().zip(b.tables()) {
        assert_eq!(
            a_table.1.partitioned_data().sequence_number(),
            b_table.1.partitioned_data().sequence_number(),
            "sequence number"
        );
    }

    let a = a.into_tables().collect::<BTreeMap<_, _>>();
    let b = b.into_tables().collect::<BTreeMap<_, _>>();

    a.into_iter().zip(b.into_iter()).for_each(|(a, b)| {
        assert_eq!(a.0, b.0, "table IDs differ - a table is missing!");
        assert_eq!(
            a.1.partitioned_data()
                .data()
                .clone()
                .to_arrow(Projection::All)
                .expect("failed projection for a"),
            b.1.partitioned_data()
                .data()
                .clone()
                .to_arrow(Projection::All)
                .expect("failed projection for b"),
            "table data differs"
        );
    })
}
