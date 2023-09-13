//! gRPC service implementations for `router`.

use crate::{namespace_cache::NamespaceCache, schema_validator::SchemaValidator};
use generated_types::influxdata::iox::{
    catalog::v1::*, namespace::v1::*, object_store::v1::*, schema::v1::*, table::v1::*,
};
use iox_catalog::interface::Catalog;
use object_store::DynObjectStore;
use service_grpc_catalog::CatalogService;
use service_grpc_namespace::NamespaceService;
use service_grpc_object_store::ObjectStoreService;
use service_grpc_table::TableService;
use std::sync::Arc;

mod schema_service;

/// This type manages all gRPC services exposed by a `router` using the RPC write path.
#[derive(Debug)]
pub struct RpcWriteGrpcDelegate<C> {
    catalog: Arc<dyn Catalog>,
    object_store: Arc<DynObjectStore>,
    schema_validator: Arc<SchemaValidator<C>>,
}

impl<C> RpcWriteGrpcDelegate<C>
where
    C: NamespaceCache<ReadError = iox_catalog::interface::Error> + 'static,
{
    /// Create a new gRPC handler
    pub fn new(
        catalog: Arc<dyn Catalog>,
        object_store: Arc<DynObjectStore>,
        schema_validator: Arc<SchemaValidator<C>>,
    ) -> Self {
        Self {
            catalog,
            object_store,
            schema_validator,
        }
    }

    /// Acquire a [`SchemaService`] gRPC service implementation.
    ///
    /// [`SchemaService`]: generated_types::influxdata::iox::schema::v1::schema_service_server::SchemaService.
    pub fn schema_service(&self) -> impl schema_service_server::SchemaService {
        schema_service::SchemaService::new(Arc::clone(&self.schema_validator))
    }

    /// Acquire a [`CatalogService`] gRPC service implementation.
    ///
    /// [`CatalogService`]: generated_types::influxdata::iox::catalog::v1::catalog_service_server::CatalogService.
    pub fn catalog_service(&self) -> impl catalog_service_server::CatalogService {
        CatalogService::new(Arc::clone(&self.catalog))
    }

    /// Acquire a [`ObjectStoreService`] gRPC service implementation.
    ///
    /// [`ObjectStoreService`]: generated_types::influxdata::iox::object_store::v1::object_store_service_server::ObjectStoreService.
    pub fn object_store_service(&self) -> impl object_store_service_server::ObjectStoreService {
        ObjectStoreService::new(Arc::clone(&self.catalog), Arc::clone(&self.object_store))
    }

    /// Acquire a [`NamespaceService`] gRPC service implementation.
    ///
    /// [`NamespaceService`]: generated_types::influxdata::iox::namespace::v1::namespace_service_server::NamespaceService.
    pub fn namespace_service(&self) -> impl namespace_service_server::NamespaceService {
        NamespaceService::new(Arc::clone(&self.catalog))
    }

    /// Acquire a [`TableService`] gRPC service implementation.
    ///
    /// [`TableService`]: generated_types::influxdata::iox::table::v1::table_service_server::TableService
    pub fn table_service(&self) -> impl table_service_server::TableService {
        TableService::new(Arc::clone(&self.catalog))
    }
}
