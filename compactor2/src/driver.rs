use std::sync::Arc;

use futures::StreamExt;
use observability_deps::tracing::info;

use crate::{compact::compact_files, components::Components, config::Config};

pub async fn compact(config: &Config, components: &Arc<Components>) {
    let partition_ids = components.partitions_source.fetch().await;

    futures::stream::iter(partition_ids)
        .map(|partition_id| {
            let config = config.clone();
            let components = Arc::clone(components);

            async move {
                let files = components.partition_files_source.fetch(partition_id).await;
                let files = components.files_filter.apply(files);

                if !components.partition_filter.apply(&files) {
                    return;
                }

                if let Err(e) = compact_files(&files, &config.catalog).await {
                    components
                        .partition_error_sink
                        .record(partition_id, &e.to_string())
                        .await;
                    return;
                }
                info!(
                    input_size = files.iter().map(|f| f.file_size_bytes).sum::<i64>(),
                    input_files = files.len(),
                    partition_id = partition_id.get(),
                    "Compacted partition",
                );
            }
        })
        .buffer_unordered(config.partition_concurrency.get())
        .collect::<()>()
        .await;
}
