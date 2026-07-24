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
pub mod fair_value;
mod live_fair_value;
pub mod model;
mod paper;
mod research;
pub mod source;

pub use contracts::{APPLICATION_CONTRACT_VERSION, application_capabilities};
pub use fair_value::{
    AnalyticsFairValueInputPublisher, FairValueDomainService, FairValueInputAuthorityError,
    FairValueInputAuthorityLimitInput, FairValueInputAuthorityLimits,
    FairValueInputResolutionError, FairValueInputResolutionRequest, FairValueInputResolver,
    FairValueProducerKind, FairValueProducerSelection, FairValueProducerSelectionAuthority,
    FairValueProducerSelectionError, FairValueProducerSelectionRequest, FairValueReceiptReference,
    FairValueReceiptRegistration, LiveFairValueInputPublisher, PortfolioFairValueInputPublisher,
    ProductionFairValueInputAuthority, ProductionFairValueInputResolver,
    ResearchFairValueInputPublisher,
};
pub use live_fair_value::{LiveFairValueObservationBuffer, LiveFairValueObservationBufferError};
pub use paper::PaperApplicationServices;
pub use research::{
    ManagedResearchExtractionSource, ProductionResearchIngestCoordinator,
    ResearchApplicationServices, ResearchExtractionLimits, ResearchIngestCompositionError,
    ResearchIngestCoordinator, ResearchRevisionPlanError, ResearchRightsAuthority,
    ResearchSourceDiscovery, ResearchSourceDiscoveryCoordinator, ResearchSourceDiscoveryObject,
    ResearchSourceDiscoveryRights, ResearchSourceObjectListing,
};
pub use source::{
    SourceApplicationError, SourceDomainService, SourceRuntimeRequest, SourceRuntimeSnapshot,
    SourceRuntimeSnapshotBatch, SourceRuntimeSnapshotError, SourceRuntimeView,
    SourceRuntimeViewError,
};

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

    /// Revokes authority represented only by a domain result that was never published.
    ///
    /// The default deliberately leaves durable domain mutations unchanged. Implementations may
    /// override this only for pending, unpublished authority encoded in their own exact
    /// request/result pair. The hook must be synchronous, idempotent, and affect no unrelated
    /// authority.
    fn rollback_unpublished_result(
        &self,
        _request: &TypedToolRequest,
        _result: &TypedToolResult,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

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
    /// Composes the complete product from its shared domain authorities.
    ///
    /// Research expands into Research, Fundamental, and Macro services. Paper expands into
    /// Market, Bot, and Execution services. Every other authority owns exactly one domain. The
    /// resulting set still crosses [`ApplicationDomainServices::try_new`], so a misplaced,
    /// duplicated, or missing implementation fails before either CLI or MCP can receive it.
    pub fn try_from_product_services(
        source: Arc<dyn ApplicationDomainService>,
        research: &ResearchApplicationServices,
        portfolio: Arc<dyn ApplicationDomainService>,
        analysis: Arc<dyn ApplicationDomainService>,
        model: Arc<dyn ApplicationDomainService>,
        fair_value: Arc<dyn ApplicationDomainService>,
        paper: &PaperApplicationServices,
    ) -> Result<Self, ApplicationCompositionError> {
        let services = vec![
            source,
            paper.market(),
            research.research(),
            research.fundamental(),
            research.macroeconomics(),
            portfolio,
            analysis,
            model,
            fair_value,
            paper.bot(),
            paper.execution(),
        ];
        Self::try_new(ApplicationDomainServices::try_new(services)?)
    }

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
        let result = service.call(request.clone(), context.clone()).await?;
        let publication = ApplicationPublicationGuard::arm(service.as_ref(), &request, &result);
        let validation = (|| {
            ensure_request_live(&context)?;
            result
                .validate_against(context.limits())
                .map_err(ServiceError::from)?;
            result.validate_for(descriptor).map_err(ServiceError::from)
        })();
        match validation {
            Ok(()) => {
                publication.commit();
                Ok(result)
            }
            Err(error) => publication.rollback().and(Err(error)),
        }
    }
}

/// Owns rollback from domain-result creation through the final application publication checks.
struct ApplicationPublicationGuard<'a> {
    service: &'a dyn ApplicationDomainService,
    request: &'a TypedToolRequest,
    result: &'a TypedToolResult,
    armed: bool,
}

impl<'a> ApplicationPublicationGuard<'a> {
    fn arm(
        service: &'a dyn ApplicationDomainService,
        request: &'a TypedToolRequest,
        result: &'a TypedToolResult,
    ) -> Self {
        Self {
            service,
            request,
            result,
            armed: true,
        }
    }

    fn commit(mut self) {
        self.armed = false;
    }

    fn rollback(mut self) -> Result<(), ServiceError> {
        self.armed = false;
        self.service
            .rollback_unpublished_result(self.request, self.result)
    }
}

impl Drop for ApplicationPublicationGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _rollback = self
                .service
                .rollback_unpublished_result(self.request, self.result);
        }
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

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use async_trait::async_trait;
    use market_squawk_services::{
        JsonStructureLimits, RequestContext, RequestId, ServiceDomain, ServiceError, ServiceLimits,
        ToolResultMetadata, ToolServices, TypedToolRequest, TypedToolResult,
    };
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    use super::{
        Application, ApplicationDomainService, ApplicationDomainServices, REQUIRED_DOMAINS,
    };

    #[tokio::test]
    async fn application_rejection_drops_only_the_unpublished_source_result()
    -> Result<(), Box<dyn std::error::Error>> {
        let retained = Arc::new([AtomicUsize::new(0), AtomicUsize::new(1)]);
        let domains = REQUIRED_DOMAINS.into_iter().map(|domain| {
            Arc::new(PublicationProbe {
                domain,
                retained: Arc::clone(&retained),
            }) as Arc<dyn ApplicationDomainService>
        });
        let application = Application::try_new(ApplicationDomainServices::try_new(domains)?)?;
        let request = application.admit(
            "Source.Discover",
            json!({
                "provider": "test.provider",
                "dataset": "test-dataset",
                "confirm": true,
                "sourceCoverage": ["test.provider"],
                "resultLimits": {"maximumItems": 1, "maximumBytes": 4096},
            })
            .as_object()
            .cloned()
            .ok_or("source arguments must be an object")?,
        )?;

        assert!(matches!(
            ToolServices::call(&application, request, context()?).await,
            Err(ServiceError::InvalidResult)
        ));
        assert_eq!(retained[0].load(Ordering::Acquire), 0);
        assert_eq!(retained[1].load(Ordering::Acquire), 1);
        Ok(())
    }

    struct PublicationProbe {
        domain: ServiceDomain,
        retained: Arc<[AtomicUsize; 2]>,
    }

    #[async_trait]
    impl ApplicationDomainService for PublicationProbe {
        fn domain(&self) -> ServiceDomain {
            self.domain
        }

        async fn call(
            &self,
            _request: TypedToolRequest,
            context: RequestContext,
        ) -> Result<TypedToolResult, ServiceError> {
            if self.domain == ServiceDomain::Source {
                self.retained[0].fetch_add(1, Ordering::AcqRel);
            }
            TypedToolResult::try_new(
                serde_json::Value::Null,
                0,
                ToolResultMetadata::complete_not_applicable(),
                context.limits(),
            )
            .map_err(Into::into)
        }

        fn rollback_unpublished_result(
            &self,
            request: &TypedToolRequest,
            _result: &TypedToolResult,
        ) -> Result<(), ServiceError> {
            if self.domain == ServiceDomain::Source && request.name() == "Source.Discover" {
                self.retained[0].store(0, Ordering::Release);
            }
            Ok(())
        }

        fn begin_shutdown(&self) {}

        async fn finish_shutdown(&self, _deadline: Instant) -> Result<(), ServiceError> {
            Ok(())
        }
    }

    fn context() -> Result<RequestContext, Box<dyn std::error::Error>> {
        Ok(RequestContext::new(
            RequestId::Integer(1),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
            ServiceLimits::try_new(
                4096,
                8,
                4096,
                8,
                JsonStructureLimits::try_new(16, 4096, 64, 64)?,
            )?,
        ))
    }
}
