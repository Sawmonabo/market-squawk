//! Lifecycle-owned, transport-neutral local application services.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use market_squawk_services::{
    RequestContext, ServiceCapabilities, ServiceCapabilityError, ServiceDomain, ServiceError,
    ServiceLimits, ToolServices, TypedToolRequest, TypedToolResult,
};
use serde_json::{Map, Value};
use thiserror::Error;

pub mod analysis;
mod contracts;
mod domain_support;
pub mod model;
mod paper;

pub use contracts::{APPLICATION_CONTRACT_VERSION, application_capabilities};
pub use paper::PaperApplicationServices;

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

    /// Atomically rejects new work and triggers cancellation of owned background activity.
    ///
    /// This operation must be nonblocking, idempotent, and safe to call from a fail-safe Drop
    /// path. Durable reconciliation and task joining belong in [`Self::finish_shutdown`].
    fn begin_shutdown(&self);

    /// Completes bounded reconciliation and task joining after shutdown has begun.
    ///
    /// Implementations must be idempotent and may narrow, but never extend, the shared absolute
    /// deadline.
    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError>;
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
    accepting_requests: AtomicBool,
    shutdown: tokio::sync::Mutex<Option<ApplicationShutdownReport>>,
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
            accepting_requests: AtomicBool::new(true),
            shutdown: tokio::sync::Mutex::new(None),
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

    /// Synchronously closes request admission and cancels every domain in reverse dependency order.
    ///
    /// The operation is nonblocking and idempotent. Call [`Self::shutdown`] to complete bounded
    /// reconciliation and task joining.
    pub fn begin_shutdown(&self) {
        if self.accepting_requests.swap(false, Ordering::AcqRel) {
            for service in self.domains.services.iter().rev() {
                service.begin_shutdown();
            }
        }
    }

    /// Completes reverse-order bounded shutdown under one absolute deadline.
    ///
    /// The returned report records each domain independently so one failure never prevents the
    /// remaining services from receiving their shutdown barrier.
    pub async fn shutdown(&self, deadline: Instant) -> ApplicationShutdownReport {
        self.begin_shutdown();
        let deadline = tokio::time::Instant::from_std(deadline);
        let Ok(mut retained) = tokio::time::timeout_at(deadline, self.shutdown.lock()).await else {
            return ApplicationShutdownReport::all_failed(ServiceError::DeadlineExceeded);
        };
        if let Some(report) = *retained {
            return report;
        }

        let mut report = ApplicationShutdownReport::complete();
        for service in self.domains.services.iter().rev() {
            let outcome =
                tokio::time::timeout_at(deadline, service.finish_shutdown(deadline.into_std()))
                    .await
                    .unwrap_or(Err(ServiceError::DeadlineExceeded));
            report.failures[domain_index(service.domain())] = outcome.err();
        }
        *retained = Some(report);
        report
    }
}

impl fmt::Debug for Application {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Application")
            .field("capabilities", &self.capabilities)
            .field("domains", &self.domains)
            .field(
                "accepting_requests",
                &self.accepting_requests.load(Ordering::Acquire),
            )
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
        if !self.accepting_requests.load(Ordering::Acquire) {
            return Err(ServiceError::Unavailable);
        }
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

impl Drop for Application {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

/// Terminal shutdown result for every required application domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationShutdownReport {
    failures: [Option<ServiceError>; REQUIRED_DOMAINS.len()],
}

impl ApplicationShutdownReport {
    const fn complete() -> Self {
        Self {
            failures: [None; REQUIRED_DOMAINS.len()],
        }
    }

    const fn all_failed(error: ServiceError) -> Self {
        Self {
            failures: [Some(error); REQUIRED_DOMAINS.len()],
        }
    }

    /// True only when every required domain reached its terminal barrier.
    #[must_use]
    pub fn is_complete(self) -> bool {
        self.failures.iter().all(Option::is_none)
    }

    /// Returns the terminal failure for one domain, if any.
    #[must_use]
    pub const fn failure(self, domain: ServiceDomain) -> Option<ServiceError> {
        self.failures[domain_index(domain)]
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

fn effective_service_limits(
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<ServiceLimits, ServiceError> {
    let requested = request
        .arguments()
        .get("resultLimits")
        .and_then(Value::as_object)
        .ok_or(ServiceError::InvalidRequest)?;
    let maximum_items = requested
        .get("maximumItems")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(ServiceError::InvalidRequest)?
        .min(context.limits().maximum_result_items());
    let maximum_bytes = requested
        .get("maximumBytes")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(ServiceError::InvalidRequest)?
        .min(context.limits().maximum_result_bytes());
    ServiceLimits::try_new(
        context.limits().maximum_inline_bytes().min(maximum_bytes),
        context.limits().maximum_inline_items().min(maximum_items),
        maximum_bytes,
        maximum_items,
        context.limits().result_structure(),
    )
    .map_err(|_error| ServiceError::InvalidRequest)
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
