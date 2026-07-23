//! Lifecycle-owned, transport-neutral local application services.

use std::{fmt, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_services::{
    RequestContext, ServiceCapabilities, ServiceCapabilityError, ServiceDomain, ServiceError,
    ToolServices, TypedToolRequest, TypedToolResult,
};
use serde_json::{Map, Value};
use thiserror::Error;

mod contracts;

pub use contracts::{APPLICATION_CONTRACT_VERSION, application_capabilities};

const REQUIRED_DOMAINS: [ServiceDomain; 11] = [
    ServiceDomain::Source,
    ServiceDomain::Market,
    ServiceDomain::Research,
    ServiceDomain::Fundamental,
    ServiceDomain::Macro,
    ServiceDomain::Portfolio,
    ServiceDomain::Analysis,
    ServiceDomain::Model,
    ServiceDomain::FairValue,
    ServiceDomain::Bot,
    ServiceDomain::Execution,
];

/// One application-owned product-domain implementation.
///
/// Domain services consume only descriptor-admitted requests. They retain their own typed
/// financial, persistence, and authority invariants and return a transport-neutral bounded result.
#[async_trait]
pub trait ApplicationDomainService: Send + Sync + 'static {
    /// Product domain exclusively owned by this service.
    fn domain(&self) -> ServiceDomain;

    /// Executes one request already admitted by the exact application descriptor.
    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError>;
}

/// Complete, immutable set of product-domain implementations.
pub struct ApplicationDomainServices {
    services: Box<[Arc<dyn ApplicationDomainService>]>,
}

impl ApplicationDomainServices {
    /// Admits exactly one implementation for every required product domain.
    ///
    /// # Errors
    ///
    /// Returns [`ApplicationCompositionError`] for a missing, duplicate, or unsupported domain.
    pub fn try_new(
        services: impl IntoIterator<Item = Arc<dyn ApplicationDomainService>>,
    ) -> Result<Self, ApplicationCompositionError> {
        let mut admitted = Vec::new();
        admitted
            .try_reserve_exact(REQUIRED_DOMAINS.len())
            .map_err(|_| ApplicationCompositionError::Allocation)?;
        for service in services {
            if admitted.len() == REQUIRED_DOMAINS.len() {
                return Err(ApplicationCompositionError::IncompleteDomains);
            }
            admitted.push(service);
        }
        let mut services = admitted;
        if services.len() != REQUIRED_DOMAINS.len() {
            return Err(ApplicationCompositionError::IncompleteDomains);
        }
        services.sort_unstable_by_key(|service| domain_index(service.domain()));
        for (expected, service) in REQUIRED_DOMAINS.iter().zip(&services) {
            if service.domain() != *expected {
                return Err(ApplicationCompositionError::IncompleteDomains);
            }
        }
        Ok(Self {
            services: services.into_boxed_slice(),
        })
    }

    fn service(&self, domain: ServiceDomain) -> Option<&Arc<dyn ApplicationDomainService>> {
        self.services
            .get(domain_index(domain))
            .filter(|service| service.domain() == domain)
    }
}

impl fmt::Debug for ApplicationDomainServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationDomainServices")
            .field("domain_count", &self.services.len())
            .finish()
    }
}

/// Sole lifecycle-owned business composition shared by local CLI and MCP transports.
pub struct Application {
    capabilities: ServiceCapabilities,
    domains: ApplicationDomainServices,
}

impl Application {
    /// Constructs the complete local service surface after every mandatory domain is ready.
    ///
    /// # Errors
    ///
    /// Returns [`ApplicationCompositionError`] if a code-owned descriptor is invalid.
    pub fn try_new(
        domains: ApplicationDomainServices,
    ) -> Result<Self, ApplicationCompositionError> {
        Ok(Self {
            capabilities: application_capabilities()?,
            domains,
        })
    }

    /// Atomically admits a transport-neutral request through its exact registered descriptor.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError::NotFound`] for an unknown operation and
    /// [`ServiceError::InvalidRequest`] for arguments outside its closed typed contract.
    pub fn admit(
        &self,
        operation: &str,
        arguments: Map<String, Value>,
    ) -> Result<TypedToolRequest, ServiceError> {
        self.capabilities
            .find(operation)
            .ok_or(ServiceError::NotFound)?
            .admit(arguments)
    }

    /// Admits and invokes one operation through the same path used by every local transport.
    pub async fn invoke(
        &self,
        operation: &str,
        arguments: Map<String, Value>,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let request = self.admit(operation, arguments)?;
        self.call(request, context).await
    }
}

impl fmt::Debug for Application {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Application")
            .field("capabilities", &self.capabilities)
            .field("domains", &self.domains)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ToolServices for Application {
    fn capabilities(&self) -> ServiceCapabilities {
        self.capabilities.clone()
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(&context)?;
        let descriptor = self
            .capabilities
            .find(request.name())
            .ok_or(ServiceError::NotFound)?;
        if descriptor.version() != request.version() || descriptor.contract() != request.contract()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let service = self
            .domains
            .service(request.contract().domain())
            .ok_or(ServiceError::Unavailable)?;
        let result = service.call(request, context.clone()).await?;
        ensure_request_live(&context)?;
        result
            .validate_against(context.limits())
            .map_err(ServiceError::from)?;
        result
            .validate_for(descriptor)
            .map_err(ServiceError::from)?;
        Ok(result)
    }
}

fn ensure_request_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if Instant::now() >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    Ok(())
}

const fn domain_index(domain: ServiceDomain) -> usize {
    match domain {
        ServiceDomain::Source => 0,
        ServiceDomain::Market => 1,
        ServiceDomain::Research => 2,
        ServiceDomain::Fundamental => 3,
        ServiceDomain::Macro => 4,
        ServiceDomain::Portfolio => 5,
        ServiceDomain::Analysis => 6,
        ServiceDomain::Model => 7,
        ServiceDomain::FairValue => 8,
        ServiceDomain::Bot => 9,
        ServiceDomain::Execution => 10,
    }
}

/// Invalid complete-application composition.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ApplicationCompositionError {
    /// The fixed domain-service registry could not reserve its bounded storage.
    #[error("application domain service allocation failed")]
    Allocation,
    /// One or more product domains is absent, duplicated, or unsupported.
    #[error("application domain services are incomplete")]
    IncompleteDomains,
    /// A code-owned descriptor violated the shared service contract.
    #[error("application capability contract is invalid: {0}")]
    Capability(#[from] ServiceCapabilityError),
}
