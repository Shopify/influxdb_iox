//! Caching of [`NamespaceSchema`].

mod memory;
use hashbrown::{HashMap, HashSet};
pub use memory::*;

mod sharded_cache;
pub use sharded_cache::*;

pub mod metrics;

mod read_through_cache;
pub use read_through_cache::*;

use std::{error::Error, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};

/// An abstract cache of [`NamespaceSchema`].
#[async_trait]
pub trait NamespaceCache: Debug + Send + Sync {
    /// The type of error a [`NamespaceCache`] implementation produces
    /// when unable to read the [`NamespaceSchema`] requested from the
    /// cache.
    type ReadError: Error + Send;

    /// Return the [`NamespaceSchema`] for `namespace`.
    async fn get_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, Self::ReadError>;

    /// Place `schema` in the cache, merging the set of tables and their columns
    /// with the existing entry for `namespace`, if any.
    ///
    /// All data except the set of tables/columns have "last writer wins"
    /// semantics. The resulting merged schema is returned, along with a set
    /// of change statistics.
    ///
    /// Concurrent calls to this method will race and may result in a schema
    /// change being lost.
    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: NamespaceSchema,
    ) -> (Arc<NamespaceSchema>, ChangeStats);
}

/// Change statistics describing how the cache entry was modified by the
/// associated [`NamespaceCache::put_schema()`] call.
#[derive(Debug, PartialEq, Eq)]
pub struct ChangeStats {
    /// The names of the new tables added to the cache.
    pub(crate) new_table_names: HashSet<String>,

    /// The set of (TableName, ColumnName) for all columns added to the
    /// namespace schema by this update.
    pub(crate) new_column_names_per_table: HashMap<String, Vec<String>>,

    /// The number of new columns added by the change.
    pub(crate) new_column_count: usize,

    /// Indicates whether the namespace schema produced a new entry
    /// in the cache.
    pub(crate) did_create: bool,
}
