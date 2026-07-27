//! Provider registration, onboarding, source-state, and receipt-mediated discovery service.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::{NonZeroU16, NonZeroU64},
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use market_squawk_data::CatalogLimit;
use market_squawk_domain::{ExactPayloadEvidence, SourceIdentifier, Timestamp};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, ServiceLimits, ToolResultMetadata,
    TypedToolRequest, TypedToolResult,
};
use market_squawk_sources::{DiscoveryRequestId, MAX_DISCOVERY_OBJECTS, SourceMetadata};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    sync::Mutex,
    task::JoinHandle,
    time::{Instant as TokioInstant, timeout_at},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ApplicationDomainService, ResearchSourceDiscovery, ResearchSourceDiscoveryCoordinator,
    ResearchSourceObjectListing,
    domain_support::{DomainLifecycle, admitted_result_limits, ensure_request_live},
};
use crate::{
    ProviderOnboardingPortal, ProviderOnboardingService, ProviderPortalActivationAuthority,
    ProviderPortalActivationError, ProviderPortalConfig, ProviderPortalError,
};

mod results;
mod runtime;

pub use runtime::{
    SourceRuntimeRequest, SourceRuntimeSnapshot, SourceRuntimeSnapshotBatch,
    SourceRuntimeSnapshotError, SourceRuntimeView, SourceRuntimeViewError,
};

use results::{
    SourceReadKind, bounded_source_result, data_quality_name, ensure_exact_provider_scope,
    ensure_provider_scope, inactive_row, map_onboarding_error, map_portal_error, map_runtime_error,
    not_applicable_result, registration_value, requested_sources, required_identifier,
    required_profile_field, required_provider, runtime_row, to_json,
};

const SOURCE_REGISTER: &str = "Source.Register";
const SOURCE_GET_STATUS: &str = "Source.GetStatus";
const SOURCE_GET_COVERAGE: &str = "Source.GetCoverage";
const SOURCE_GET_HEALTH: &str = "Source.GetHealth";
const SOURCE_SETUP: &str = "Source.Setup";
const SOURCE_LIST_OBJECTS: &str = "Source.ListObjects";
const SOURCE_DISCOVER: &str = "Source.Discover";
const SOURCE_INSPECT: &str = "Source.Inspect";

const MAX_CURRENT_SESSIONS: usize = 32;
const MAXIMUM_INSPECTION_PAGE_INDEX: u16 = 63;
const MAXIMUM_INSPECTION_RECORDS: u16 = 1_024;
const PORTAL_LIFETIME: Duration = Duration::from_secs(15 * 60);
const PORTAL_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const PORTAL_MAX_REQUESTS: u64 = 512;
const PORTAL_MAX_CONNECTIONS: usize = 16;

/// Validated authority request for one non-persistent provider page inspection.
pub struct EphemeralSourceInspectionRequest {
    provider: SourceIdentifier,
    onboarding_session_id: Uuid,
    dataset_identifier: SourceIdentifier,
    page_index: u16,
    max_records: NonZeroU16,
    max_bytes: NonZeroU64,
    deadline: Instant,
    cancellation: CancellationToken,
}

impl EphemeralSourceInspectionRequest {
    #[allow(
        clippy::too_many_arguments,
        reason = "all identity, lifecycle, and resource bounds remain explicit"
    )]
    fn try_new(
        provider: SourceIdentifier,
        onboarding_session_id: Uuid,
        dataset_identifier: SourceIdentifier,
        page_index: u16,
        max_records: NonZeroU16,
        max_bytes: NonZeroU64,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Self, ServiceError> {
        if page_index > MAXIMUM_INSPECTION_PAGE_INDEX
            || max_records.get() > MAXIMUM_INSPECTION_RECORDS
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self {
            provider,
            onboarding_session_id,
            dataset_identifier,
            page_index,
            max_records,
            max_bytes,
            deadline,
            cancellation,
        })
    }

    /// Returns the exact code-owned provider profile.
    pub const fn provider(&self) -> &SourceIdentifier {
        &self.provider
    }

    /// Returns the onboarding session that owns credential authority.
    pub const fn onboarding_session_id(&self) -> Uuid {
        self.onboarding_session_id
    }

    /// Returns the exact provider dataset grammar admitted by the adapter.
    pub const fn dataset_identifier(&self) -> &SourceIdentifier {
        &self.dataset_identifier
    }

    /// Returns the zero-based provider page index.
    pub const fn page_index(&self) -> u16 {
        self.page_index
    }

    /// Returns the hard observation count ceiling.
    pub const fn max_records(&self) -> NonZeroU16 {
        self.max_records
    }

    /// Returns the hard inline deep-byte ceiling.
    pub const fn max_bytes(&self) -> NonZeroU64 {
        self.max_bytes
    }

    /// Returns the monotonic request deadline.
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Returns request-owned cancellation authority.
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

impl fmt::Debug for EphemeralSourceInspectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EphemeralSourceInspectionRequest")
            .field("provider", &self.provider)
            .field("onboarding_session_id", &self.onboarding_session_id)
            .field("dataset_identifier", &self.dataset_identifier)
            .field("page_index", &self.page_index)
            .field("max_records", &self.max_records)
            .field("max_bytes", &self.max_bytes)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// Exact provider result returned by the sole ephemeral inspection authority.
#[derive(Debug)]
pub struct EphemeralSourceInspectionResult {
    provider: SourceIdentifier,
    onboarding_session_id: Uuid,
    dataset_identifier: SourceIdentifier,
    object_id: SourceIdentifier,
    page_index: u16,
    page_evidence: ExactPayloadEvidence,
    received_at: Timestamp,
    observations: Vec<Value>,
}

impl EphemeralSourceInspectionResult {
    #[allow(
        clippy::too_many_arguments,
        reason = "all provider lineage fields remain explicit"
    )]
    /// Builds the exact result returned after provider-page revalidation.
    #[must_use]
    pub fn new(
        provider: SourceIdentifier,
        onboarding_session_id: Uuid,
        dataset_identifier: SourceIdentifier,
        object_id: SourceIdentifier,
        page_index: u16,
        page_evidence: ExactPayloadEvidence,
        received_at: Timestamp,
        observations: Vec<Value>,
    ) -> Self {
        Self {
            provider,
            onboarding_session_id,
            dataset_identifier,
            object_id,
            page_index,
            page_evidence,
            received_at,
            observations,
        }
    }
}

/// Sole application authority for bounded credentialed inspection without persistence.
#[async_trait]
pub trait EphemeralSourceInspectionAuthority: Send + Sync {
    /// Retrieves one exact bounded page without creating durable research publication state.
    ///
    /// # Errors
    ///
    /// Returns a bounded service error when onboarding authority, provider access, validation,
    /// request lifecycle, or result construction fails.
    async fn inspect(
        &self,
        request: EphemeralSourceInspectionRequest,
    ) -> Result<EphemeralSourceInspectionResult, ServiceError>;
}

/// Transport-neutral Source-domain service over onboarding and current runtime evidence.
pub struct SourceDomainService {
    controller: Arc<SourceController>,
}

impl SourceDomainService {
    /// Binds the sole onboarding authority, current-runtime view, and discovery authority.
    ///
    /// # Errors
    ///
    /// Returns [`SourceApplicationError::AsyncRuntimeUnavailable`] outside a Tokio runtime.
    pub fn try_new(
        onboarding: Arc<ProviderOnboardingService>,
        runtime: Arc<dyn SourceRuntimeView>,
        discovery: Arc<dyn ResearchSourceDiscoveryCoordinator>,
        portal_activation: Arc<dyn ProviderPortalActivationAuthority>,
        inspection: Arc<dyn EphemeralSourceInspectionAuthority>,
    ) -> Result<Self, SourceApplicationError> {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_error| SourceApplicationError::AsyncRuntimeUnavailable)?;
        let portal_state = Arc::new(Mutex::new(PortalState::Empty));
        let portal_cancellation = CancellationToken::new();
        let portal_task = handle.spawn(portal_shutdown_worker(
            Arc::clone(&portal_state),
            portal_cancellation.clone(),
        ));
        Ok(Self {
            controller: Arc::new(SourceController {
                onboarding,
                runtime,
                discovery,
                portal_activation,
                inspection,
                lifecycle: DomainLifecycle::new(),
                session_limit: CatalogLimit::new(MAX_CURRENT_SESSIONS)
                    .map_err(|_error| SourceApplicationError::InvalidCodeOwnedLimit)?,
                portal_state,
                portal_cancellation,
                portal_task: Mutex::new(PortalTaskState::Running(portal_task)),
            }),
        })
    }
}

impl fmt::Debug for SourceDomainService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceDomainService")
            .field("controller", &self.controller)
            .finish()
    }
}

#[async_trait]
impl ApplicationDomainService for SourceDomainService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Source
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let _call = DomainLifecycle::enter(&self.controller.lifecycle, &context)?;
        let limits = admitted_result_limits(&request, &context)?;
        match request.name() {
            SOURCE_REGISTER => self.controller.register(&request, &context, limits),
            SOURCE_SETUP => self.controller.setup(&request, &context, limits).await,
            SOURCE_LIST_OBJECTS => {
                self.controller
                    .list_objects(&request, &context, limits)
                    .await
            }
            SOURCE_DISCOVER => self.controller.discover(&request, &context, limits).await,
            SOURCE_INSPECT => self.controller.inspect(&request, &context, limits).await,
            SOURCE_GET_STATUS => {
                self.controller
                    .read(&request, &context, limits, SourceReadKind::Status)
                    .await
            }
            SOURCE_GET_COVERAGE => {
                self.controller
                    .read(&request, &context, limits, SourceReadKind::Coverage)
                    .await
            }
            SOURCE_GET_HEALTH => {
                self.controller
                    .read(&request, &context, limits, SourceReadKind::Health)
                    .await
            }
            _ => Err(ServiceError::NotFound),
        }
    }

    fn rollback_unpublished_result(
        &self,
        request: &TypedToolRequest,
        result: &TypedToolResult,
    ) -> Result<(), ServiceError> {
        self.controller
            .rollback_unpublished_discovery(request, result)
    }

    fn begin_shutdown(&self) {
        self.controller.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.controller.finish_shutdown(deadline).await
    }
}

impl Drop for SourceDomainService {
    fn drop(&mut self) {
        self.controller.begin_shutdown();
    }
}

struct SourceController {
    onboarding: Arc<ProviderOnboardingService>,
    runtime: Arc<dyn SourceRuntimeView>,
    discovery: Arc<dyn ResearchSourceDiscoveryCoordinator>,
    portal_activation: Arc<dyn ProviderPortalActivationAuthority>,
    inspection: Arc<dyn EphemeralSourceInspectionAuthority>,
    lifecycle: Arc<DomainLifecycle>,
    session_limit: CatalogLimit,
    portal_state: Arc<Mutex<PortalState>>,
    portal_cancellation: CancellationToken,
    portal_task: Mutex<PortalTaskState>,
}

impl SourceController {
    async fn inspect(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let provider = required_identifier(request, "provider")?;
        ensure_exact_provider_scope(request, &provider)?;
        let onboarding_session_id = request
            .arguments()
            .get("onboardingSessionId")
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidRequest)
            .and_then(|value| {
                Uuid::parse_str(value).map_err(|_error| ServiceError::InvalidRequest)
            })?;
        let dataset_identifier = required_identifier(request, "datasetIdentifier")?;
        let page_index = request
            .arguments()
            .get("pageIndex")
            .and_then(Value::as_u64)
            .and_then(|value| u16::try_from(value).ok())
            .ok_or(ServiceError::InvalidRequest)?;
        let max_records = request
            .arguments()
            .get("maxRecords")
            .and_then(Value::as_u64)
            .and_then(|value| u16::try_from(value).ok())
            .and_then(NonZeroU16::new)
            .ok_or(ServiceError::InvalidRequest)?;
        if usize::from(max_records.get()) > limits.maximum_inline_items()
            || usize::from(max_records.get()) > limits.maximum_result_items()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let maximum_bytes = limits
            .maximum_inline_bytes()
            .min(limits.maximum_result_bytes());
        let maximum_bytes = u64::try_from(maximum_bytes)
            .ok()
            .and_then(NonZeroU64::new)
            .ok_or(ServiceError::InvalidRequest)?;
        let inspection_request = EphemeralSourceInspectionRequest::try_new(
            provider.clone(),
            onboarding_session_id,
            dataset_identifier.clone(),
            page_index,
            max_records,
            maximum_bytes,
            context.deadline(),
            context.cancellation().clone(),
        )?;
        let deadline = TokioInstant::from_std(context.deadline());
        let inspected = tokio::select! {
            biased;
            () = context.cancellation().cancelled() => return Err(ServiceError::Cancelled),
            () = self.lifecycle.shutdown_token().cancelled() => {
                return Err(ServiceError::Unavailable);
            }
            () = tokio::time::sleep_until(deadline) => {
                return Err(ServiceError::DeadlineExceeded);
            }
            result = self.inspection.inspect(inspection_request) => result?,
        };
        ensure_request_live(context, &self.lifecycle)?;
        if inspected.provider != provider
            || inspected.onboarding_session_id != onboarding_session_id
            || inspected.dataset_identifier != dataset_identifier
            || inspected.page_index != page_index
            || inspected.observations.len() > usize::from(max_records.get())
        {
            return Err(ServiceError::InvalidResult);
        }
        let item_count = inspected.observations.len();
        let content = json!({
            "provider": inspected.provider.as_str(),
            "onboardingSessionId": inspected.onboarding_session_id.to_string(),
            "datasetIdentifier": inspected.dataset_identifier.as_str(),
            "objectId": inspected.object_id.as_str(),
            "pageIndex": inspected.page_index,
            "pageEvidence": to_json(&inspected.page_evidence)?,
            "receivedAt": DateTime::<Utc>::from_timestamp_nanos(
                inspected.received_at.unix_nanos()
            ).to_rfc3339_opts(SecondsFormat::Nanos, true),
            "observations": inspected.observations,
        });
        let metadata = ToolResultMetadata::try_complete(
            json!({
                "provider": provider.as_str(),
                "dataset": dataset_identifier.as_str(),
                "operation": "ephemeral_inspection",
                "persistence": "none",
            }),
            json!({
                "quality": "official_delayed",
                "executionEligible": false,
            }),
        )
        .map_err(|_error| ServiceError::InvalidResult)?;
        let result = TypedToolResult::try_new(content, item_count, metadata, limits)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        ensure_request_live(context, &self.lifecycle)?;
        Ok(result)
    }

    async fn list_objects(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let profile = required_identifier(request, "provider")?;
        let dataset = required_identifier(request, "dataset")?;
        ensure_exact_provider_scope(request, &profile)?;
        let (publication_limits, maximum_items) = self.discovery_limits(limits)?;
        let listed = self
            .discovery
            .list_registered_objects(&profile, &dataset, None, maximum_items, context)
            .await?;
        ensure_request_live(context, &self.lifecycle)?;
        validate_listing(&listed, &profile, &dataset, maximum_items)?;
        if listed.objects().is_empty() {
            return Err(ServiceError::NotFound);
        }

        let coverage = discovery_coverage(
            &profile,
            &dataset,
            listed.metadata(),
            listed.request().request_id(),
        );
        let quality = discovery_quality(listed.metadata());
        let metadata = ToolResultMetadata::try_complete(coverage, quality)
            .map_err(|_error| ServiceError::InvalidResult)?;
        let item_count = listed.objects().len();
        let result =
            TypedToolResult::try_new(to_json(&listed)?, item_count, metadata, publication_limits)
                .map_err(|_error| ServiceError::ResourceExhausted)?;
        ensure_request_live(context, &self.lifecycle)?;
        Ok(result)
    }

    async fn discover(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let profile = required_identifier(request, "provider")?;
        let dataset = required_identifier(request, "dataset")?;
        ensure_exact_provider_scope(request, &profile)?;
        let (publication_limits, maximum_items) = self.discovery_limits(limits)?;
        let publication = DiscoveryPublicationGuard::new(
            Arc::clone(&self.discovery),
            self.discovery
                .discover_registered_objects(&profile, &dataset, None, maximum_items, context)
                .await?,
        );
        let discovered = publication.discovery()?;
        let result = (|| {
            ensure_request_live(context, &self.lifecycle)?;
            validate_discovery(discovered, &profile, &dataset, maximum_items)?;
            if discovered.objects().is_empty() {
                return Err(ServiceError::NotFound);
            }

            let coverage = discovery_coverage(
                &profile,
                &dataset,
                discovered.metadata(),
                discovered.request().request_id(),
            );
            let quality = discovery_quality(discovered.metadata());
            let metadata = ToolResultMetadata::try_complete(coverage, quality)
                .map_err(|_error| ServiceError::InvalidResult)?;
            let item_count = discovered.objects().len();
            let result = TypedToolResult::try_new(
                to_json(&discovered)?,
                item_count,
                metadata,
                publication_limits,
            )
            .map_err(|_error| ServiceError::ResourceExhausted)?;
            ensure_request_live(context, &self.lifecycle)?;
            Ok(result)
        })();
        match result {
            Ok(result) => {
                publication.commit();
                Ok(result)
            }
            Err(error) => publication.rollback().and(Err(error)),
        }
    }

    fn discovery_limits(
        &self,
        limits: ServiceLimits,
    ) -> Result<(ServiceLimits, NonZeroU16), ServiceError> {
        let maximum_inline_items = limits
            .maximum_inline_items()
            .min(limits.maximum_result_items());
        let maximum_inline_bytes = limits
            .maximum_inline_bytes()
            .min(limits.maximum_result_bytes());
        let publication_limits = ServiceLimits::try_new(
            maximum_inline_bytes,
            maximum_inline_items,
            maximum_inline_bytes,
            maximum_inline_items,
            limits.result_structure(),
        )
        .map_err(|_error| ServiceError::Internal)?;
        let maximum_items = limits
            .maximum_result_items()
            .min(publication_limits.maximum_result_items())
            .min(MAX_DISCOVERY_OBJECTS)
            .min(usize::from(
                self.discovery.maximum_discovery_objects().get(),
            ));
        let maximum_items = u16::try_from(maximum_items)
            .ok()
            .and_then(NonZeroU16::new)
            .ok_or(ServiceError::InvalidRequest)?;
        Ok((publication_limits, maximum_items))
    }

    fn rollback_unpublished_discovery(
        &self,
        request: &TypedToolRequest,
        result: &TypedToolResult,
    ) -> Result<(), ServiceError> {
        if request.name() != SOURCE_DISCOVER {
            return Ok(());
        }
        let profile = required_identifier(request, "provider")?;
        let dataset = required_identifier(request, "dataset")?;
        ensure_exact_provider_scope(request, &profile)?;
        let discovered =
            ResearchSourceDiscovery::from_publication(result.structured_content().clone())?;
        let maximum_items = NonZeroU16::new(discovered.request().max_results())
            .ok_or(ServiceError::InvalidResult)?;
        let requested_items = request
            .arguments()
            .get("resultLimits")
            .and_then(Value::as_object)
            .and_then(|limits| limits.get("maximumItems"))
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(ServiceError::InvalidRequest)?;
        if usize::from(maximum_items.get())
            > requested_items.min(MAX_DISCOVERY_OBJECTS).min(usize::from(
                self.discovery.maximum_discovery_objects().get(),
            ))
        {
            return Err(ServiceError::InvalidResult);
        }
        validate_discovery(&discovered, &profile, &dataset, maximum_items)?;
        if result.item_count() != discovered.objects().len()
            || result.metadata().source_coverage()
                != &discovery_coverage(
                    &profile,
                    &dataset,
                    discovered.metadata(),
                    discovered.request().request_id(),
                )
            || result.metadata().data_quality() != &discovery_quality(discovered.metadata())
        {
            return Err(ServiceError::InvalidResult);
        }
        self.discovery.revoke_discovery_receipts(&discovered)
    }

    fn register(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let provider = required_provider(request)?;
        ensure_provider_scope(request, provider)?;
        let registered = self
            .onboarding
            .register_profile(provider)
            .map_err(map_onboarding_error)?;
        ensure_request_live(context, &self.lifecycle)?;
        not_applicable_result(registration_value(&registered)?, limits)
    }

    async fn setup(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let provider = required_provider(request)?;
        ensure_provider_scope(request, provider)?;
        let registered = self
            .onboarding
            .register_profile(provider)
            .map_err(map_onboarding_error)?;
        let current = self
            .onboarding
            .current_sessions(self.session_limit)
            .map_err(map_onboarding_error)?
            .into_iter()
            .find(|session| session.surface_id() == provider);
        let portal = self.ensure_portal(context).await?;
        ensure_request_live(context, &self.lifecycle)?;

        let profile = to_json(registered.profile())?;
        let handoff_url = required_profile_field(&profile, "official_handoff_url")?;
        let handoff_instruction = required_profile_field(&profile, "handoff_instruction")?;
        let current = current.as_ref().map(to_json).transpose()?;
        not_applicable_result(
            json!({
                "registration": registration_value(&registered)?,
                "officialHandoff": {
                    "url": handoff_url,
                    "instruction": handoff_instruction,
                },
                "portal": {
                    "url": portal.base_url,
                    "expiresInSeconds": portal.expires_in_seconds,
                    "secretInput": "local_portal_only",
                },
                "currentSession": current,
            }),
            limits,
        )
    }

    async fn read(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
        kind: SourceReadKind,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let filters = requested_sources(request)?;
        let profiles = self.onboarding.profiles();
        let sessions = self
            .onboarding
            .current_sessions(self.session_limit)
            .map_err(map_onboarding_error)?;
        let runtime_request = SourceRuntimeRequest::new(
            filters.clone().into_boxed_slice(),
            context.cancellation().clone(),
            context.deadline(),
        );
        let runtime = self.current_runtime(runtime_request, context).await?;
        ensure_request_live(context, &self.lifecycle)?;

        let sessions = sessions
            .into_iter()
            .map(|session| (session.surface_id().to_owned(), session))
            .collect::<BTreeMap<_, _>>();
        let mut runtime = runtime.into_records();
        runtime.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
        if runtime
            .windows(2)
            .any(|pair| pair[0].sort_key() == pair[1].sort_key())
            || runtime.iter().any(|record| {
                !profiles
                    .iter()
                    .any(|profile| profile.id() == record.surface_id().as_str())
            })
        {
            return Err(ServiceError::InvalidResult);
        }

        let mut selected_profile_count = 0_usize;
        let mut selected_runtime_count = 0_usize;
        let mut runtime_classes = BTreeSet::new();
        let mut rows = Vec::new();
        for profile in &profiles {
            let profile_explicit =
                filters.is_empty() || filters.iter().any(|filter| filter.as_str() == profile.id());
            let selected_runtime = runtime
                .iter()
                .filter(|record| {
                    record.surface_id().as_str() == profile.id()
                        && (profile_explicit
                            || filters
                                .iter()
                                .any(|filter| filter.as_str() == record.source_id().as_str()))
                })
                .collect::<Vec<_>>();
            if !profile_explicit && selected_runtime.is_empty() {
                continue;
            }
            selected_profile_count = selected_profile_count.saturating_add(1);
            selected_runtime_count = selected_runtime_count.saturating_add(selected_runtime.len());
            runtime_classes.extend(
                selected_runtime
                    .iter()
                    .map(|record| data_quality_name(record.quality)),
            );
            let profile_value = to_json(profile)?;
            let session_value = sessions.get(profile.id()).map(to_json).transpose()?;
            if selected_runtime.is_empty() {
                rows.push(inactive_row(kind, profile, &profile_value, session_value)?);
            } else {
                rows.try_reserve(selected_runtime.len())
                    .map_err(|_error| ServiceError::ResourceExhausted)?;
                for record in selected_runtime {
                    rows.push(runtime_row(
                        kind,
                        profile,
                        &profile_value,
                        session_value.clone(),
                        record,
                    )?);
                }
            }
        }

        let coverage = json!({
            "authority": "code_owned_profiles_and_current_runtime_evidence",
            "requestedSources": filters
                .iter()
                .map(|source| source.as_str())
                .collect::<Vec<_>>(),
            "profileCount": selected_profile_count,
            "runtimeRecordCount": selected_runtime_count,
            "runtimeAbsence": "not_established",
        });
        let quality = json!({
            "authority": "profile_ceiling_and_runtime_qualification",
            "runtimeClasses": runtime_classes.into_iter().collect::<Vec<_>>(),
            "runtimeAbsence": "not_active",
            "executionEligibilityUnchanged": true,
        });
        bounded_source_result(rows, coverage, quality, limits)
    }

    async fn current_runtime(
        &self,
        request: SourceRuntimeRequest,
        context: &RequestContext,
    ) -> Result<SourceRuntimeSnapshotBatch, ServiceError> {
        let deadline = TokioInstant::from_std(context.deadline());
        tokio::select! {
            biased;
            () = context.cancellation().cancelled() => Err(ServiceError::Cancelled),
            () = self.lifecycle.shutdown_token().cancelled() => Err(ServiceError::Unavailable),
            () = tokio::time::sleep_until(deadline) => Err(ServiceError::DeadlineExceeded),
            result = self.runtime.current(request) => result.map_err(map_runtime_error),
        }
    }

    async fn ensure_portal(
        &self,
        context: &RequestContext,
    ) -> Result<PortalLocation, ServiceError> {
        let deadline = TokioInstant::from_std(context.deadline());
        let mut state = tokio::select! {
            biased;
            () = context.cancellation().cancelled() => return Err(ServiceError::Cancelled),
            () = self.lifecycle.shutdown_token().cancelled() => {
                return Err(ServiceError::Unavailable);
            }
            () = tokio::time::sleep_until(deadline) => {
                return Err(ServiceError::DeadlineExceeded);
            }
            state = self.portal_state.lock() => state,
        };
        let config = ProviderPortalConfig::try_new(
            PORTAL_LIFETIME,
            PORTAL_REQUEST_TIMEOUT,
            PORTAL_MAX_REQUESTS,
            PORTAL_MAX_CONNECTIONS,
        )
        .map_err(map_portal_error)?;
        loop {
            ensure_request_live(context, &self.lifecycle)?;
            if let PortalState::Running(slot) = &*state {
                let now = Instant::now();
                if now < slot.expires_at {
                    return Ok(slot.location(now));
                }

                let expired = std::mem::replace(&mut *state, PortalState::Empty);
                let PortalState::Running(expired) = expired else {
                    return Err(ServiceError::Internal);
                };
                *state = PortalState::Retiring(tokio::spawn(expired.portal.shutdown()));
                continue;
            }

            match &mut *state {
                PortalState::Empty => {
                    let onboarding = Arc::clone(&self.onboarding);
                    let activation = Arc::clone(&self.portal_activation);
                    *state = PortalState::Starting(tokio::spawn(async move {
                        ProviderOnboardingPortal::start(onboarding, activation, config).await
                    }));
                }
                PortalState::Starting(task) => {
                    let joined = tokio::select! {
                        biased;
                        () = context.cancellation().cancelled() => {
                            return Err(ServiceError::Cancelled);
                        }
                        () = self.lifecycle.shutdown_token().cancelled() => {
                            return Err(ServiceError::Unavailable);
                        }
                        () = tokio::time::sleep_until(deadline) => {
                            return Err(ServiceError::DeadlineExceeded);
                        }
                        result = task => result,
                    };
                    let portal = match joined {
                        Ok(Ok(portal)) => portal,
                        Ok(Err(error)) => {
                            *state = PortalState::Empty;
                            return Err(map_portal_error(error));
                        }
                        Err(_error) => {
                            *state = PortalState::Empty;
                            return Err(ServiceError::Unavailable);
                        }
                    };
                    let Some(expires_at) = Instant::now().checked_add(PORTAL_LIFETIME) else {
                        *state = PortalState::Retiring(tokio::spawn(portal.shutdown()));
                        return Err(ServiceError::Internal);
                    };
                    *state = PortalState::Running(PortalSlot { portal, expires_at });
                }
                PortalState::Retiring(task) => {
                    let joined = tokio::select! {
                        biased;
                        () = context.cancellation().cancelled() => {
                            return Err(ServiceError::Cancelled);
                        }
                        () = self.lifecycle.shutdown_token().cancelled() => {
                            return Err(ServiceError::Unavailable);
                        }
                        () = tokio::time::sleep_until(deadline) => {
                            return Err(ServiceError::DeadlineExceeded);
                        }
                        result = task => result,
                    };
                    *state = PortalState::Empty;
                    match joined {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => return Err(map_portal_error(error)),
                        Err(_error) => return Err(ServiceError::Unavailable),
                    }
                }
                PortalState::Running(_) => {}
            }
        }
    }

    fn begin_shutdown(&self) {
        self.portal_activation.begin_shutdown();
        self.lifecycle.begin_shutdown();
        self.portal_cancellation.cancel();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.begin_shutdown();
        let activation = self
            .portal_activation
            .finish_shutdown(deadline)
            .await
            .map_err(map_portal_activation_shutdown_error);
        let lifecycle = self.lifecycle.finish_shutdown(deadline).await;
        let portal = self.finish_portal_shutdown(deadline).await;
        activation.and(lifecycle).and(portal)
    }

    async fn finish_portal_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        let deadline = TokioInstant::from_std(deadline);
        let mut state = timeout_at(deadline, self.portal_task.lock())
            .await
            .map_err(|_error| ServiceError::DeadlineExceeded)?;
        let joined = match &mut *state {
            PortalTaskState::Running(task) => timeout_at(deadline, task)
                .await
                .map_err(|_error| ServiceError::DeadlineExceeded)?,
            PortalTaskState::Complete(outcome) => return *outcome,
        };
        let outcome = joined
            .map_err(|_error| ServiceError::Unavailable)?
            .map_err(map_portal_error);
        *state = PortalTaskState::Complete(outcome);
        outcome
    }
}

fn map_portal_activation_shutdown_error(error: ProviderPortalActivationError) -> ServiceError {
    match error {
        ProviderPortalActivationError::Cancelled => ServiceError::Cancelled,
        ProviderPortalActivationError::InvalidRequest
        | ProviderPortalActivationError::Unavailable
        | ProviderPortalActivationError::StateUnavailable => ServiceError::Unavailable,
    }
}

impl fmt::Debug for SourceController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceController")
            .field("onboarding", &self.onboarding)
            .field("runtime", &"[AUTHORITY-FREE RUNTIME VIEW]")
            .field("portal_activation", &"[DURABLE ADAPTER AUTHORITY]")
            .field("inspection", &"[EPHEMERAL EXTRACTION AUTHORITY]")
            .field("lifecycle", &self.lifecycle)
            .field("session_limit", &self.session_limit)
            .finish_non_exhaustive()
    }
}

impl Drop for SourceController {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

struct PortalSlot {
    portal: ProviderOnboardingPortal,
    expires_at: Instant,
}

impl PortalSlot {
    fn location(&self, now: Instant) -> PortalLocation {
        let remaining = self.expires_at.saturating_duration_since(now);
        let expires_in_seconds = remaining
            .as_secs()
            .saturating_add(u64::from(remaining.subsec_nanos() != 0));
        PortalLocation {
            base_url: self.portal.base_url().to_owned(),
            expires_in_seconds,
        }
    }
}

fn validate_discovery(
    discovery: &ResearchSourceDiscovery,
    profile: &SourceIdentifier,
    dataset: &SourceIdentifier,
    maximum_items: NonZeroU16,
) -> Result<(), ServiceError> {
    if discovery.profile() != profile
        || discovery.request().dataset() != dataset
        || discovery.request().effective_at().is_some()
        || discovery.request().max_results() != maximum_items.get()
        || discovery.objects().len() > usize::from(maximum_items.get())
        || discovery.receipts_survive_restart()
        || !discovery.rights().persistence_operation_admitted()
        || discovery
            .objects()
            .iter()
            .enumerate()
            .any(|(index, object)| {
                discovery.objects()[index.saturating_add(1)..]
                    .iter()
                    .any(|candidate| candidate.discovery_receipt() == object.discovery_receipt())
            })
        || discovery.objects().iter().any(|object| {
            object.source_object().dataset() != dataset
                || object.source_object().source_id() != discovery.metadata().source_id()
                || object.source_object().metadata_revision() != discovery.metadata().revision()
                || object.discovery_receipt().is_empty()
        })
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}

fn validate_listing(
    listing: &ResearchSourceObjectListing,
    profile: &SourceIdentifier,
    dataset: &SourceIdentifier,
    maximum_items: NonZeroU16,
) -> Result<(), ServiceError> {
    if listing.profile() != profile
        || listing.request().dataset() != dataset
        || listing.request().effective_at().is_some()
        || listing.request().max_results() != maximum_items.get()
        || listing.objects().len() > usize::from(maximum_items.get())
        || listing.objects().iter().any(|object| {
            object.dataset() != dataset
                || object.source_id() != listing.metadata().source_id()
                || object.metadata_revision() != listing.metadata().revision()
        })
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}

fn discovery_coverage(
    profile: &SourceIdentifier,
    dataset: &SourceIdentifier,
    metadata: &SourceMetadata,
    request_id: DiscoveryRequestId,
) -> Value {
    json!({
        "provider": profile,
        "providerDataset": dataset,
        "sourceId": metadata.source_id(),
        "metadataRevision": metadata.revision(),
        "coverageDomain": metadata.coverage().domain(),
        "coverageEvidence": metadata.coverage().evidence(),
        "coverageEffective": metadata.coverage().effective_interval(),
        "discoveryRequestId": request_id,
    })
}

fn discovery_quality(metadata: &SourceMetadata) -> Value {
    json!({
        "qualityCeiling": metadata.quality_ceiling(),
        "exactSourceObjectEvidence": true,
        "executionEligible": false,
    })
}

/// Revokes an unpublished receipt batch on every scope-exit path.
///
/// The guard owns no coordinator lock and is armed only after the coordinator future returns, so it
/// can be dropped synchronously if this application future is cancelled before publication commits.
struct DiscoveryPublicationGuard {
    coordinator: Arc<dyn ResearchSourceDiscoveryCoordinator>,
    discovery: Option<ResearchSourceDiscovery>,
}

impl DiscoveryPublicationGuard {
    fn new(
        coordinator: Arc<dyn ResearchSourceDiscoveryCoordinator>,
        discovery: ResearchSourceDiscovery,
    ) -> Self {
        Self {
            coordinator,
            discovery: Some(discovery),
        }
    }

    fn discovery(&self) -> Result<&ResearchSourceDiscovery, ServiceError> {
        self.discovery.as_ref().ok_or(ServiceError::Internal)
    }

    fn commit(mut self) {
        self.discovery = None;
    }

    fn rollback(mut self) -> Result<(), ServiceError> {
        let result = self.discovery.as_ref().map_or(Ok(()), |discovery| {
            self.coordinator.revoke_discovery_receipts(discovery)
        });
        self.discovery = None;
        result
    }
}

impl Drop for DiscoveryPublicationGuard {
    fn drop(&mut self) {
        if let Some(discovery) = self.discovery.as_ref() {
            let _rollback = self.coordinator.revoke_discovery_receipts(discovery);
        }
    }
}

struct PortalLocation {
    base_url: String,
    expires_in_seconds: u64,
}

enum PortalState {
    Empty,
    Starting(JoinHandle<Result<ProviderOnboardingPortal, ProviderPortalError>>),
    Running(PortalSlot),
    Retiring(JoinHandle<Result<(), ProviderPortalError>>),
}

enum PortalTaskState {
    Running(JoinHandle<Result<(), ProviderPortalError>>),
    Complete(Result<(), ServiceError>),
}

async fn portal_shutdown_worker(
    state: Arc<Mutex<PortalState>>,
    cancellation: CancellationToken,
) -> Result<(), ProviderPortalError> {
    cancellation.cancelled().await;
    let owned = {
        let mut state = state.lock().await;
        std::mem::replace(&mut *state, PortalState::Empty)
    };
    match owned {
        PortalState::Empty => Ok(()),
        PortalState::Starting(task) => {
            let portal = task
                .await
                .map_err(|_error| ProviderPortalError::ServerTask)??;
            portal.shutdown().await
        }
        PortalState::Running(slot) => slot.portal.shutdown().await,
        PortalState::Retiring(task) => task
            .await
            .map_err(|_error| ProviderPortalError::ServerTask)?,
    }
}

/// Source application construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SourceApplicationError {
    /// Source lifecycle tasks require a current Tokio runtime.
    #[error("source application requires an asynchronous runtime")]
    AsyncRuntimeUnavailable,
    /// A code-owned internal result ceiling was invalid.
    #[error("source application code-owned limit is invalid")]
    InvalidCodeOwnedLimit,
}
