//! Internals used by [`LocalScheduler`].
pub(crate) mod catalog_commit;
pub(crate) mod combos;
pub(crate) mod id_only_partition_filter;
pub(crate) mod partition_done_sink;
pub(crate) mod partitions_source;
pub(crate) mod partitions_source_config;
pub(crate) mod shard_config;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use backoff::BackoffConfig;
use data_types::PartitionId;
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use observability_deps::tracing::{info, warn};

use crate::{
    commit::{logging::LoggingCommitWrapper, metrics::MetricsCommitWrapper},
    Commit, CommitUpdate, CommitWrapper, CompactionJob, CompactionJobStatus,
    CompactionJobStatusResult, CompactionJobStatusVariant, MockCommit, MockPartitionsSource,
    PartitionsSource, PartitionsSourceConfig, Scheduler, ShardConfig, SkipReason,
};

use self::{
    catalog_commit::CatalogCommit,
    combos::{throttle_partition::throttle_partition, unique_partitions::unique_partitions},
    id_only_partition_filter::{
        and::AndIdOnlyPartitionFilter, shard::ShardPartitionFilter, IdOnlyPartitionFilter,
    },
    partition_done_sink::{
        catalog::CatalogPartitionDoneSink, mock::MockPartitionDoneSink, PartitionDoneSink,
    },
    partitions_source::{
        catalog_all::CatalogAllPartitionsSource,
        catalog_to_compact::CatalogToCompactPartitionsSource,
        filter::FilterPartitionsSourceWrapper,
    },
};

/// Configuration specific to the local scheduler.
#[derive(Debug, Default, Clone)]
pub struct LocalSchedulerConfig {
    /// Optionally wrap the `Commit` instance
    ///
    /// This is mostly used for testing
    pub commit_wrapper: Option<Arc<dyn CommitWrapper>>,
    /// The partitions source config used by the local sceduler.
    pub partitions_source_config: PartitionsSourceConfig,
    /// The shard config used by the local sceduler.
    pub shard_config: Option<ShardConfig>,
}

/// Implementation of the scheduler for local (per compactor) scheduling.
#[derive(Debug)]
pub(crate) struct LocalScheduler {
    /// Commits changes (i.e. deletion and creation) to the catalog
    pub(crate) commit: Arc<dyn Commit>,
    /// The partitions source to use for scheduling.
    partitions_source: Arc<dyn PartitionsSource<Output = PartitionId>>,
    /// The actions to take when a partition is done.
    ///
    /// Includes partition (PartitionId) tracking of uniqueness and throttling.
    partition_done_sink: Arc<dyn PartitionDoneSink<PartitionId>>,
    /// The shard config used for generating the PartitionsSource.
    shard_config: Option<ShardConfig>,
}

impl LocalScheduler {
    /// Create a new [`LocalScheduler`].
    pub(crate) fn new(
        config: LocalSchedulerConfig,
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        time_provider: Arc<dyn TimeProvider>,
        metrics: Arc<metric::Registry>,
        shadow_mode: bool,
    ) -> Self {
        let commit = Self::build_commit(
            config.clone(),
            backoff_config.clone(),
            Arc::clone(&catalog),
            metrics,
            shadow_mode,
        );

        let partitions_source = Self::build_partitions_source(
            config.clone(),
            backoff_config.clone(),
            Arc::clone(&catalog),
            Arc::clone(&time_provider),
        );

        let (partitions_source, commit, partition_done_sink) = Self::build_partition_done_sink(
            partitions_source,
            commit,
            backoff_config,
            catalog,
            time_provider,
            shadow_mode,
        );

        Self {
            commit,
            partitions_source,
            partition_done_sink,
            shard_config: config.shard_config,
        }
    }

    fn build_partitions_source(
        config: LocalSchedulerConfig,
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Arc<dyn PartitionsSource<Output = PartitionId>> {
        let shard_config = config.shard_config;
        let partitions_source: Arc<dyn PartitionsSource<Output = PartitionId>> =
            match &config.partitions_source_config {
                PartitionsSourceConfig::CatalogRecentWrites { threshold } => {
                    Arc::new(CatalogToCompactPartitionsSource::new(
                        backoff_config,
                        Arc::clone(&catalog),
                        *threshold,
                        None, // Recent writes is `threshold` ago to now
                        time_provider,
                    ))
                }
                PartitionsSourceConfig::CatalogAll => Arc::new(CatalogAllPartitionsSource::new(
                    backoff_config,
                    Arc::clone(&catalog),
                )),
                PartitionsSourceConfig::Fixed(ids) => {
                    Arc::new(MockPartitionsSource::new(ids.iter().cloned().collect()))
                }
            };

        let mut id_only_partition_filters: Vec<Arc<dyn IdOnlyPartitionFilter>> = vec![];
        if let Some(shard_config) = &shard_config {
            // add shard filter before performing any catalog IO
            info!(
                "starting compactor {} of {}",
                shard_config.shard_id, shard_config.n_shards
            );
            id_only_partition_filters.push(Arc::new(ShardPartitionFilter::new(
                shard_config.n_shards,
                shard_config.shard_id,
            )));
        }
        Arc::new(FilterPartitionsSourceWrapper::new(
            AndIdOnlyPartitionFilter::new(id_only_partition_filters),
            partitions_source,
        ))
    }

    #[allow(clippy::type_complexity)]
    fn build_partition_done_sink(
        partitions_source: Arc<dyn PartitionsSource<Output = PartitionId>>,
        commit: Arc<dyn Commit>,
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        time_provider: Arc<dyn TimeProvider>,
        shadow_mode: bool,
    ) -> (
        Arc<dyn PartitionsSource<Output = PartitionId>>,
        Arc<dyn Commit>,
        Arc<dyn PartitionDoneSink<PartitionId>>,
    ) {
        let partition_done_sink: Arc<dyn PartitionDoneSink<PartitionId>> = if shadow_mode {
            Arc::new(MockPartitionDoneSink::new())
        } else {
            Arc::new(CatalogPartitionDoneSink::new(
                backoff_config,
                Arc::clone(&catalog),
            ))
        };

        let (partitions_source, partition_done_sink) =
            unique_partitions(partitions_source, partition_done_sink, 1);

        let (partitions_source, commit, partition_done_sink) = throttle_partition(
            partitions_source,
            commit,
            partition_done_sink,
            Arc::clone(&time_provider),
            Duration::from_secs(60),
            1,
        );

        (
            Arc::new(partitions_source),
            Arc::new(commit),
            Arc::new(partition_done_sink),
        )
    }

    fn build_commit(
        config: LocalSchedulerConfig,
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        metrics_registry: Arc<metric::Registry>,
        shadow_mode: bool,
    ) -> Arc<dyn Commit> {
        let commit: Arc<dyn Commit> = if shadow_mode {
            Arc::new(MockCommit::new())
        } else {
            Arc::new(CatalogCommit::new(backoff_config, Arc::clone(&catalog)))
        };

        let commit = if let Some(commit_wrapper) = &config.commit_wrapper {
            commit_wrapper.wrap(commit)
        } else {
            commit
        };

        Arc::new(LoggingCommitWrapper::new(MetricsCommitWrapper::new(
            commit,
            &metrics_registry,
        )))
    }
}

#[async_trait]
impl Scheduler for LocalScheduler {
    async fn get_jobs(&self) -> Vec<CompactionJob> {
        self.partitions_source
            .fetch()
            .await
            .into_iter()
            .map(CompactionJob::new)
            .collect()
    }

    async fn job_status(
        &self,
        job_status: CompactionJobStatus,
    ) -> Result<CompactionJobStatusResult, Box<dyn std::error::Error>> {
        match job_status.status {
            CompactionJobStatusVariant::Update(commit_update) => {
                let CommitUpdate {
                    partition_id,
                    delete,
                    upgrade,
                    target_level,
                    create,
                } = commit_update;

                let result = self
                    .commit
                    .commit(partition_id, &delete, &upgrade, &create, target_level)
                    .await;

                // verify create commit counts
                assert_eq!(result.len(), create.len());

                Ok(CompactionJobStatusResult::UpdatedParquetFiles(result))
            }
            CompactionJobStatusVariant::RequestToSkip(SkipReason::CompactionError(msg)) => {
                self.partition_done_sink
                    .record(job_status.job.partition_id, Err(msg.into()))
                    .await;

                Ok(CompactionJobStatusResult::Ack)
            }
            CompactionJobStatusVariant::Error(error_kind) => {
                warn!("Error processing job: {:?}: {}", job_status.job, error_kind);
                Ok(CompactionJobStatusResult::Ack)
            }
            CompactionJobStatusVariant::Complete => {
                // TODO: once uuid is handled properly, we can track the job completion
                Ok(CompactionJobStatusResult::Ack)
            }
        }
    }
}

impl std::fmt::Display for LocalScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.shard_config {
            None => write!(f, "local_compaction_scheduler"),
            Some(shard_config) => write!(f, "local_compaction_scheduler({shard_config})",),
        }
    }
}

#[cfg(test)]
mod tests {
    use iox_tests::TestCatalog;
    use iox_time::{MockProvider, Time};

    use super::*;

    #[test]
    fn test_display() {
        let scheduler = LocalScheduler::new(
            LocalSchedulerConfig::default(),
            BackoffConfig::default(),
            TestCatalog::new().catalog(),
            Arc::new(MockProvider::new(Time::MIN)),
            Arc::new(metric::Registry::default()),
            false,
        );

        assert_eq!(scheduler.to_string(), "local_compaction_scheduler",);
    }

    #[test]
    fn test_display_with_sharding() {
        let shard_config = Some(ShardConfig {
            n_shards: 2,
            shard_id: 1,
        });

        let config = LocalSchedulerConfig {
            commit_wrapper: None,
            partitions_source_config: PartitionsSourceConfig::default(),
            shard_config,
        };

        let scheduler = LocalScheduler::new(
            config,
            BackoffConfig::default(),
            TestCatalog::new().catalog(),
            Arc::new(MockProvider::new(Time::MIN)),
            Arc::new(metric::Registry::default()),
            false,
        );

        assert_eq!(
            scheduler.to_string(),
            "local_compaction_scheduler(shard_cfg(n_shards=2,shard_id=1))",
        );
    }
}
