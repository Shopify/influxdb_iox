use std::fmt::Display;

use async_trait::async_trait;
use data_types::PartitionId;

use crate::{IdOnlyPartitionFilter, PartitionsSource};

/// An implementation of [`PartitionsSource`]
/// that filters the results of another [`PartitionsSource`].
#[derive(Debug)]
pub(crate) struct FilterPartitionsSourceWrapper<T, F>
where
    T: PartitionsSource,
    F: IdOnlyPartitionFilter,
{
    filter: F,
    inner: T,
}

impl<T, F> FilterPartitionsSourceWrapper<T, F>
where
    T: PartitionsSource,
    F: IdOnlyPartitionFilter,
{
    /// Create a new [`FilterPartitionsSourceWrapper`].
    pub(crate) fn new(filter: F, inner: T) -> Self {
        Self { filter, inner }
    }
}

impl<T, F> Display for FilterPartitionsSourceWrapper<T, F>
where
    T: PartitionsSource,
    F: IdOnlyPartitionFilter,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "filter({}, {})", self.filter, self.inner)
    }
}

#[async_trait]
impl<T, F> PartitionsSource for FilterPartitionsSourceWrapper<T, F>
where
    T: PartitionsSource<Output = PartitionId>,
    F: IdOnlyPartitionFilter,
{
    type Output = PartitionId;

    async fn fetch(&self) -> Vec<Self::Output> {
        self.inner
            .fetch()
            .await
            .into_iter()
            .filter(|id| self.filter.apply(*id))
            .collect()
    }
}
