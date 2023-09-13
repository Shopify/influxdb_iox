use crate::namespace_cache::NamespaceCache;
use data_types::NamespaceName;
use generated_types::influxdata::iox::schema::v1::*;
use observability_deps::tracing::warn;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Implementation of the gRPC schema service that is allowed to modify schemas
#[derive(Debug)]
pub(crate) struct SchemaService<C> {
    /// Namespace schema cache.
    namespace_cache: Arc<C>,
}

impl<C> SchemaService<C> {
    pub(crate) fn new(namespace_cache: Arc<C>) -> Self {
        Self { namespace_cache }
    }
}

#[tonic::async_trait]
impl<C> schema_service_server::SchemaService for SchemaService<C>
where
    C: NamespaceCache<ReadError = iox_catalog::interface::Error> + 'static,
{
    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let req = request.into_inner();

        let namespace_name = NamespaceName::try_from(req.namespace.clone())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let schema = self
            .namespace_cache
            .get_schema(&namespace_name)
            .await
            .map_err(|e| {
                warn!(error=%e, %req.namespace, "failed to retrieve namespace schema");
                Status::not_found(e.to_string())
            })?;

        Ok(Response::new(GetSchemaResponse {
            schema: Some((&*schema).into()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace_cache::{MemoryNamespaceCache, ReadThroughCache};
    use data_types::ColumnType;
    use futures::{future::BoxFuture, FutureExt};
    use generated_types::influxdata::iox::schema::v1::schema_service_server::SchemaService;
    use iox_catalog::{
        interface::{Catalog, RepoCollection},
        mem::MemCatalog,
        test_helpers::{arbitrary_namespace, arbitrary_table},
    };
    use std::sync::Arc;
    use tonic::Code;

    // `SchemaService` has to be specified in this way because the `generated_types` trait is
    // also in scope. Make an alias for convenience and to have one place to explain.
    type Service = super::SchemaService<ReadThroughCache<MemoryNamespaceCache>>;

    // Given some `catalog_setup` closure that can use the catalog repos and returns a future,
    // set up the catalog, await the future, and return the gRPC service.
    async fn service_setup<S, T>(mut catalog_setup: S) -> Service
    where
        S: (FnMut(&mut dyn RepoCollection) -> BoxFuture<'_, T>) + Send,
    {
        let metrics = Default::default();
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;

        let setup = catalog_setup(repos.as_mut());
        setup.await;

        let ns_cache = Arc::new(setup_test_cache(catalog));

        Service::new(ns_cache)
    }

    fn setup_test_cache(catalog: Arc<dyn Catalog>) -> ReadThroughCache<MemoryNamespaceCache> {
        ReadThroughCache::new(MemoryNamespaceCache::default(), catalog)
    }

    async fn get_schema(grpc: &Service, namespace: &str, table: Option<&str>) -> NamespaceSchema {
        let request = GetSchemaRequest {
            namespace: namespace.to_string(),
            table: table.map(Into::into),
        };

        let response = grpc.get_schema(Request::new(request)).await.unwrap();
        let response = response.into_inner();
        response.schema.unwrap()
    }

    async fn get_schema_expecting_failure(
        grpc: &Service,
        namespace: &str,
        table: Option<&str>,
        expected_code: Code,
        expected_message: &str,
    ) {
        let request = GetSchemaRequest {
            namespace: namespace.to_string(),
            table: table.map(Into::into),
        };

        let status = grpc.get_schema(Request::new(request)).await.unwrap_err();
        assert_eq!(status.code(), expected_code);
        assert_eq!(status.message(), expected_message);
    }

    fn sorted_table_names(schema: &NamespaceSchema) -> Vec<String> {
        let mut table_names: Vec<_> = schema.tables.keys().cloned().collect();
        table_names.sort();
        table_names
    }

    fn sorted_column_names(schema: &NamespaceSchema, table: &str) -> Vec<String> {
        let mut column_names: Vec<_> = schema
            .tables
            .get(table)
            .unwrap()
            .columns
            .keys()
            .cloned()
            .collect();
        column_names.sort();
        column_names
    }

    #[tokio::test]
    async fn get_schema_works() {
        let namespace = "namespace_schema_test";
        let table = "schema_test_table";
        let column = "schema_test_column";
        let another_table = "another_schema_test_table";
        let another_column = "another_schema_test_column";

        let grpc = service_setup(|repos| {
            async {
                let namespace = arbitrary_namespace(&mut *repos, namespace).await;

                let table = arbitrary_table(&mut *repos, table, &namespace).await;
                repos
                    .columns()
                    .create_or_get(column, table.id, ColumnType::Tag)
                    .await
                    .unwrap();

                let another_table = arbitrary_table(&mut *repos, another_table, &namespace).await;
                repos
                    .columns()
                    .create_or_get(another_column, another_table.id, ColumnType::Tag)
                    .await
                    .unwrap();
            }
            .boxed()
        })
        .await;

        // request all tables for a namespace
        let schema = get_schema(&grpc, namespace, None).await;
        assert_eq!(sorted_table_names(&schema), [another_table, table]);
        assert_eq!(sorted_column_names(&schema, table), [column]);

        // request one table for a namespace
        let schema = get_schema(&grpc, namespace, Some(table)).await;
        assert_eq!(sorted_table_names(&schema), [table]);
        assert_eq!(sorted_column_names(&schema, table), [column]);

        // request a nonexistent table for a namespace, which fails
        get_schema_expecting_failure(
            &grpc,
            namespace,
            Some("does_not_exist"),
            Code::NotFound,
            "table does_not_exist not found",
        )
        .await;
    }
}
