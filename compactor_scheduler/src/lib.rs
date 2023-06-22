//! service for scheduling compactor tasks.

#![deny(
    rustdoc::broken_intra_doc_links,
    rust_2018_idioms,
    missing_debug_implementations,
    unreachable_pub
)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    clippy::clone_on_ref_ptr,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    unused_crate_dependencies
)]
#![allow(clippy::missing_docs_in_private_items)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod local_scheduler;
pub use local_scheduler::LocalScheduler;
mod partitions_source;
pub use partitions_source::*;
mod scheduler;
pub use scheduler::*;

// Temporary exports. Will eventually be encapsulated in local_scheduler.
pub use local_scheduler::id_only_partition_filter::IdOnlyPartitionFilter;
pub use local_scheduler::partitions_source_config::PartitionsSourceConfig;
pub use local_scheduler::shard_config::ShardConfig;
