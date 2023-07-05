//! Information of a partition for compaction

use std::sync::Arc;

use data_types::{
    NamespaceId, PartitionHashId, PartitionId, PartitionKey, Table, TableSchema,
    TransitionPartitionId,
};
use schema::sort::SortKey;

/// Information about the Partition being compacted
#[derive(Debug, PartialEq)]
pub struct PartitionInfo {
    /// the partition
    pub partition_id: PartitionId,

    /// partition hash id
    pub partition_hash_id: Option<PartitionHashId>,

    /// Namespace ID
    pub namespace_id: NamespaceId,

    /// Namespace name
    pub namespace_name: String,

    /// Table.
    pub table: Arc<Table>,

    /// Table schema
    pub table_schema: Arc<TableSchema>,

    /// Sort key of the partition
    pub sort_key: Option<SortKey>,

    /// partition_key
    pub partition_key: PartitionKey,
}

impl PartitionInfo {
    /// Returns number of columns in the table
    pub fn column_count(&self) -> usize {
        self.table_schema.column_count()
    }

    /// If this partition has a `PartitionHashId` stored in the catalog, use that. Otherwise, use
    /// the database-assigned `PartitionId`.
    pub fn transition_partition_id(&self) -> TransitionPartitionId {
        self.partition_hash_id
            .map(TransitionPartitionId::Deterministic)
            .unwrap_or_else(|| TransitionPartitionId::Deprecated(self.partition_id))
    }
}
