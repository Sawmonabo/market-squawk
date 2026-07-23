//! Provider registration, onboarding, and authority-free source-state application service.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use market_squawk_data::{CatalogError, CatalogLimit};
use market_squawk_domain::{
    AssessmentStatus, CaptureIntegrityState, ConnectionGeneration, CoverageScope, CoverageStatus,
    DataQuality, InstrumentId, QualificationAssessment, SourceId, SourceIdentifier,
    StreamIntegrityState, Timestamp,
};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, ServiceLimits, ToolResultMetadata,
    TypedToolRequest, TypedToolResult,
};
use market_squawk_sources::{
    ConnectionLiveness, MarketFreshness, SourceHealthSnapshot, SourceTimestampFreshness,
    TransportFreshness,
};
use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    sync::Mutex,
    task::JoinHandle,
    time::{Instant as TokioInstant, timeout_at},
};
use tokio_util::sync::CancellationToken;

use super::{
    ApplicationDomainService,
    domain_support::{DomainLifecycle, admitted_result_limits, ensure_request_live},
};
use crate::{
    ProviderOnboardingError, ProviderOnboardingPortal, ProviderOnboardingService,
    ProviderPortalConfig, ProviderPortalError, ProviderProfileRegistrationOutcome,
    ProviderProfileView,
};

const SOURCE_REGISTER: &str = "Source.Register";
const SOURCE_GET_STATUS: &str = "Source.GetStatus";
const SOURCE_GET_COVERAGE: &str = "Source.GetCoverage";
const SOURCE_GET_HEALTH: &str = "Source.GetHealth";
const SOURCE_SETUP: &str = "Source.Setup";

const MAX_CURRENT_SESSIONS: usize = 32;
const MAX_RUNTIME_SNAPSHOTS: usize = 4_096;
const PORTAL_LIFETIME: Duration = Duration::from_secs(15 * 60);
const PORTAL_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const PORTAL_MAX_REQUESTS: u64 = 512;
const PORTAL_MAX_CONNECTIONS: usize = 16;

/// Bounded read request supplied to an authority-free runtime view.
#[derive(Clone)]
pub struct SourceRuntimeRequest {
    source_filters: Box<[SourceIdentifier]>,
    maximum_items: usize,
    cancellation: CancellationToken,
    deadline: Instant,
}

impl SourceRuntimeRequest {
    fn new(
        source_filters: Box<[SourceIdentifier]>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Self {
        Self {
            source_filters,
            maximum_items: MAX_RUNTIME_SNAPSHOTS,
            cancellation,
            deadline,
        }
    }

    /// Requested profile-surface or live-source identities. An empty slice means all sources.
    pub fn source_filters(&self) -> &[SourceIdentifier] {
        &self.source_filters
    }

    /// Hard maximum number of complete runtime records the producer may return.
    pub const fn maximum_items(&self) -> usize {
        self.maximum_items
    }

    /// Caller cancellation propagated without granting application lifecycle authority.
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Absolute caller deadline that a runtime view may narrow but never extend.
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }
}

impl fmt::Debug for SourceRuntimeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceRuntimeRequest")
            .field("source_filter_count", &self.source_filters.len())
            .field("maximum_items", &self.maximum_items)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// One authority-free runtime record reconstructed from source-health and qualification evidence.
#[derive(Clone, Debug)]
pub struct SourceRuntimeSnapshot {
    surface_id: SourceIdentifier,
    source_id: SourceId,
    instrument_id: InstrumentId,
    connection_generation: ConnectionGeneration,
    connection: ConnectionLiveness,
    transport_freshness: TransportFreshness,
    market_freshness: MarketFreshness,
    source_freshness: SourceTimestampFreshness,
    stream_integrity: StreamIntegrityState,
    capture_integrity: CaptureIntegrityState,
    coverage_scope: CoverageScope,
    coverage_status: CoverageStatus,
    quality: DataQuality,
    observed_at: Timestamp,
    qualification_valid_until: Timestamp,
}

impl SourceRuntimeSnapshot {
    /// Strips authority from one relationally matching health/qualification evidence pair.
    ///
    /// # Errors
    ///
    /// Returns [`SourceRuntimeSnapshotError::EvidenceMismatch`] when source, session, metadata,
    /// generation, integrity, or executable-quality facts do not describe one exact runtime.
    pub fn try_from_evidence(
        surface_id: SourceIdentifier,
        health: &SourceHealthSnapshot,
        assessment: &QualificationAssessment,
    ) -> Result<Self, SourceRuntimeSnapshotError> {
        let binding = assessment.binding();
        let coverage = assessment.market().coverage().result();
        if health.source_id() != binding.source_id()
            || health.metadata_revision() != binding.metadata_revision()
            || health.session_id().as_source_identifier() != binding.session_id()
            || health.connection_generation() != binding.connection_generation()
            || health.stream_integrity() != *assessment.market().stream().result()
            || health.capture_integrity() != *assessment.market().capture().result()
            || coverage.scope().source_id() != health.source_id()
            || coverage.scope().metadata_revision() != health.metadata_revision()
            || (assessment.recorded_quality() == DataQuality::DirectVerified
                && (health.live_valid_until().is_none()
                    || assessment.assessment_status_at(health.observed_at())
                        != AssessmentStatus::Satisfied))
        {
            return Err(SourceRuntimeSnapshotError::EvidenceMismatch);
        }
        Ok(Self {
            surface_id,
            source_id: health.source_id().clone(),
            instrument_id: binding.instrument_id(),
            connection_generation: binding.connection_generation(),
            connection: health.connection(),
            transport_freshness: health.transport_freshness(),
            market_freshness: health.market_freshness(),
            source_freshness: health.source_freshness(),
            stream_integrity: health.stream_integrity(),
            capture_integrity: health.capture_integrity(),
            coverage_scope: coverage.scope().clone(),
            coverage_status: coverage.status_at(health.observed_at()),
            quality: assessment.recorded_quality(),
            observed_at: health.observed_at(),
            qualification_valid_until: assessment.valid_until(),
        })
    }

    /// Code-owned provider surface to which composition bound this runtime.
    pub const fn surface_id(&self) -> &SourceIdentifier {
        &self.surface_id
    }

    /// Exact live source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    fn sort_key(&self) -> (&str, &str, &str, InstrumentId, &str, &str) {
        (
            self.surface_id.as_str(),
            self.source_id.as_str(),
            self.coverage_scope.venue_id().as_str(),
            self.instrument_id,
            self.coverage_scope
                .provider_product()
                .as_source_identifier()
                .as_str(),
            self.coverage_scope
                .provider_channel()
                .as_source_identifier()
                .as_str(),
        )
    }
}

/// Invalid authority-free runtime snapshot construction.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SourceRuntimeSnapshotError {
    /// Health and qualification evidence do not bind the same exact source runtime.
    #[error("source runtime evidence does not share one exact binding")]
    EvidenceMismatch,
}

/// Complete bounded runtime view. It never carries execution, source-session, or secret authority.
#[derive(Clone, Debug)]
pub struct SourceRuntimeSnapshotBatch {
    records: Box<[SourceRuntimeSnapshot]>,
}

impl SourceRuntimeSnapshotBatch {
    /// Retains one complete bounded runtime view.
    ///
    /// # Errors
    ///
    /// Returns [`SourceRuntimeViewError::ResourceExhausted`] above the code-owned source ceiling.
    pub fn try_new(records: Vec<SourceRuntimeSnapshot>) -> Result<Self, SourceRuntimeViewError> {
        if records.len() > MAX_RUNTIME_SNAPSHOTS {
            return Err(SourceRuntimeViewError::ResourceExhausted);
        }
        Ok(Self {
            records: records.into_boxed_slice(),
        })
    }

    /// Complete records returned by the current runtime.
    pub fn records(&self) -> &[SourceRuntimeSnapshot] {
        &self.records
    }

    fn into_records(self) -> Vec<SourceRuntimeSnapshot> {
        self.records.into_vec()
    }
}

/// Stable runtime-view failure without provider payload or authority.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SourceRuntimeViewError {
    /// Caller cancellation won the runtime-view race.
    #[error("source runtime view was cancelled")]
    Cancelled,
    /// Caller deadline elapsed.
    #[error("source runtime view deadline elapsed")]
    DeadlineExceeded,
    /// The complete runtime view exceeded its hard bound.
    #[error("source runtime view exceeded its resource bound")]
    ResourceExhausted,
    /// The current runtime view is temporarily unavailable.
    #[error("source runtime view is unavailable")]
    Unavailable,
    /// The producer returned contradictory authority-free facts.
    #[error("source runtime view is invalid")]
    InvalidSnapshot,
}

/// Least-authority read seam implemented by the application-owned live runtime.
#[async_trait]
pub trait SourceRuntimeView: Send + Sync + 'static {
    /// Returns a complete bounded current view without transferring runtime-control authority.
    async fn current(
        &self,
        request: SourceRuntimeRequest,
    ) -> Result<SourceRuntimeSnapshotBatch, SourceRuntimeViewError>;
}

/// Transport-neutral Source-domain service over onboarding and current runtime evidence.
pub struct SourceDomainService {
    controller: Arc<SourceController>,
}

impl SourceDomainService {
    /// Binds the sole onboarding authority and an authority-free current-runtime view.
    ///
    /// # Errors
    ///
    /// Returns [`SourceApplicationError::AsyncRuntimeUnavailable`] outside a Tokio runtime.
    pub fn try_new(
        onboarding: Arc<ProviderOnboardingService>,
        runtime: Arc<dyn SourceRuntimeView>,
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
    lifecycle: Arc<DomainLifecycle>,
    session_limit: CatalogLimit,
    portal_state: Arc<Mutex<PortalState>>,
    portal_cancellation: CancellationToken,
    portal_task: Mutex<PortalTaskState>,
}

impl SourceController {
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
                .map(SourceIdentifier::as_str)
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
                    *state = PortalState::Starting(tokio::spawn(async move {
                        ProviderOnboardingPortal::start(onboarding, config).await
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
        self.lifecycle.begin_shutdown();
        self.portal_cancellation.cancel();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.begin_shutdown();
        let lifecycle = self.lifecycle.finish_shutdown(deadline).await;
        let portal = self.finish_portal_shutdown(deadline).await;
        lifecycle.and(portal)
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

impl fmt::Debug for SourceController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceController")
            .field("onboarding", &self.onboarding)
            .field("runtime", &"[AUTHORITY-FREE RUNTIME VIEW]")
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

#[derive(Clone, Copy)]
enum SourceReadKind {
    Status,
    Coverage,
    Health,
}

fn inactive_row(
    kind: SourceReadKind,
    profile: &ProviderProfileView,
    profile_value: &Value,
    session: Option<Value>,
) -> Result<Value, ServiceError> {
    Ok(match kind {
        SourceReadKind::Status => json!({
            "profile": profile_value,
            "currentSession": session,
            "runtime": {"state": "not_active"},
        }),
        SourceReadKind::Coverage => json!({
            "surfaceId": profile.id(),
            "releaseState": required_profile_field(profile_value, "release_state")?,
            "declaredCoverage": required_profile_field(profile_value, "coverage")?,
            "qualityCeiling": required_profile_field(profile_value, "quality_ceiling")?,
            "rights": required_profile_field(profile_value, "rights")?,
            "runtimeCoverage": {"state": "not_established"},
        }),
        SourceReadKind::Health => json!({
            "surfaceId": profile.id(),
            "onboardingState": session
                .as_ref()
                .and_then(|value| value.get("state"))
                .cloned(),
            "runtimeHealth": {"state": "not_active"},
        }),
    })
}

fn runtime_row(
    kind: SourceReadKind,
    profile: &ProviderProfileView,
    profile_value: &Value,
    session: Option<Value>,
    runtime: &SourceRuntimeSnapshot,
) -> Result<Value, ServiceError> {
    Ok(match kind {
        SourceReadKind::Status => json!({
            "profile": profile_value,
            "currentSession": session,
            "runtime": runtime_status_value(runtime)?,
        }),
        SourceReadKind::Coverage => json!({
            "surfaceId": profile.id(),
            "releaseState": required_profile_field(profile_value, "release_state")?,
            "declaredCoverage": required_profile_field(profile_value, "coverage")?,
            "qualityCeiling": required_profile_field(profile_value, "quality_ceiling")?,
            "rights": required_profile_field(profile_value, "rights")?,
            "runtimeCoverage": runtime_coverage_value(runtime)?,
        }),
        SourceReadKind::Health => json!({
            "surfaceId": profile.id(),
            "onboardingState": session
                .as_ref()
                .and_then(|value| value.get("state"))
                .cloned(),
            "runtimeHealth": runtime_health_value(runtime)?,
        }),
    })
}

fn runtime_status_value(runtime: &SourceRuntimeSnapshot) -> Result<Value, ServiceError> {
    Ok(json!({
        "state": "active",
        "sourceId": runtime.source_id.as_str(),
        "venueId": runtime.coverage_scope.venue_id().as_str(),
        "instrumentId": runtime.instrument_id.to_string(),
        "providerProduct": runtime
            .coverage_scope
            .provider_product()
            .as_source_identifier()
            .as_str(),
        "providerChannel": runtime
            .coverage_scope
            .provider_channel()
            .as_source_identifier()
            .as_str(),
        "connectionGeneration": runtime.connection_generation.get(),
        "connection": to_json(&runtime.connection)?,
        "integrity": to_json(&runtime.stream_integrity)?,
        "quality": to_json(&runtime.quality)?,
        "observedAtUnixNanos": runtime.observed_at.unix_nanos(),
        "qualificationValidUntilUnixNanos": runtime.qualification_valid_until.unix_nanos(),
    }))
}

fn runtime_coverage_value(runtime: &SourceRuntimeSnapshot) -> Result<Value, ServiceError> {
    let scope = &runtime.coverage_scope;
    Ok(json!({
        "state": "established",
        "sourceId": scope.source_id().as_str(),
        "venueId": scope.venue_id().as_str(),
        "instrumentId": runtime.instrument_id.to_string(),
        "providerProduct": scope.provider_product().as_source_identifier().as_str(),
        "providerChannel": scope.provider_channel().as_source_identifier().as_str(),
        "eventClass": to_json(&scope.event_class())?,
        "marketDepth": to_json(&scope.depth())?,
        "delay": to_json(&scope.delay())?,
        "consolidation": to_json(&scope.consolidation())?,
        "effectiveFromUnixNanos": scope.effective_from().unix_nanos(),
        "effectiveUntilUnixNanos": scope.effective_until().map(Timestamp::unix_nanos),
        "metadataRevision": scope.metadata_revision().as_source_identifier().as_str(),
        "status": to_json(&runtime.coverage_status)?,
    }))
}

fn runtime_health_value(runtime: &SourceRuntimeSnapshot) -> Result<Value, ServiceError> {
    Ok(json!({
        "state": "active",
        "sourceId": runtime.source_id.as_str(),
        "venueId": runtime.coverage_scope.venue_id().as_str(),
        "instrumentId": runtime.instrument_id.to_string(),
        "connectionGeneration": runtime.connection_generation.get(),
        "connection": to_json(&runtime.connection)?,
        "transportFreshness": to_json(&runtime.transport_freshness)?,
        "marketFreshness": to_json(&runtime.market_freshness)?,
        "sourceTimestampFreshness": to_json(&runtime.source_freshness)?,
        "streamIntegrity": to_json(&runtime.stream_integrity)?,
        "captureIntegrity": to_json(&runtime.capture_integrity)?,
        "coverageStatus": to_json(&runtime.coverage_status)?,
        "quality": to_json(&runtime.quality)?,
        "observedAtUnixNanos": runtime.observed_at.unix_nanos(),
        "qualificationValidUntilUnixNanos": runtime.qualification_valid_until.unix_nanos(),
    }))
}

fn registration_value(
    registered: &crate::ProviderProfileRegistration,
) -> Result<Value, ServiceError> {
    Ok(json!({
        "profile": to_json(registered.profile())?,
        "outcome": match registered.outcome() {
            ProviderProfileRegistrationOutcome::Inserted => "inserted",
            ProviderProfileRegistrationOutcome::Replay => "replay",
        },
    }))
}

fn bounded_source_result(
    rows: Vec<Value>,
    coverage: Value,
    quality: Value,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    let available = rows.len();
    let maximum = available.min(limits.maximum_result_items());
    let mut low = 0_usize;
    let mut high = maximum;
    let mut best = None;
    while low <= high {
        let count = low + ((high - low) / 2);
        let content = if count == 0 {
            Value::Null
        } else {
            Value::Array(rows[..count].to_vec())
        };
        let metadata = source_metadata(count, available, coverage.clone(), quality.clone())?;
        match TypedToolResult::try_new(content, count, metadata, limits) {
            Ok(result) => {
                best = Some(result);
                low = count.saturating_add(1);
            }
            Err(_) if count > 0 => high = count - 1,
            Err(_) => break,
        }
    }
    best.ok_or(ServiceError::ResourceExhausted)
}

fn source_metadata(
    returned: usize,
    available: usize,
    coverage: Value,
    quality: Value,
) -> Result<ToolResultMetadata, ServiceError> {
    if returned < available {
        ToolResultMetadata::try_truncated(available, coverage, quality)
            .map_err(|_error| ServiceError::InvalidResult)
    } else {
        ToolResultMetadata::try_complete(coverage, quality)
            .map_err(|_error| ServiceError::InvalidResult)
    }
}

fn not_applicable_result(
    content: Value,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    TypedToolResult::try_new(
        content,
        1,
        ToolResultMetadata::complete_not_applicable(),
        limits,
    )
    .map_err(|_error| ServiceError::ResourceExhausted)
}

fn required_provider(request: &TypedToolRequest) -> Result<&str, ServiceError> {
    request
        .arguments()
        .get("provider")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
}

fn ensure_provider_scope(request: &TypedToolRequest, provider: &str) -> Result<(), ServiceError> {
    let filters = requested_sources(request)?;
    if filters.iter().any(|filter| filter.as_str() != provider) {
        Err(ServiceError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn requested_sources(request: &TypedToolRequest) -> Result<Vec<SourceIdentifier>, ServiceError> {
    request
        .arguments()
        .get("sourceCoverage")
        .map(|value| {
            value
                .as_array()
                .ok_or(ServiceError::InvalidRequest)?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .ok_or(ServiceError::InvalidRequest)
                        .and_then(|value| {
                            SourceIdentifier::try_from(value)
                                .map_err(|_error| ServiceError::InvalidRequest)
                        })
                })
                .collect()
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn required_profile_field(profile: &Value, field: &str) -> Result<Value, ServiceError> {
    profile
        .get(field)
        .cloned()
        .ok_or(ServiceError::InvalidResult)
}

fn to_json<T: Serialize>(value: &T) -> Result<Value, ServiceError> {
    serde_json::to_value(value).map_err(|_error| ServiceError::InvalidResult)
}

const fn data_quality_name(quality: DataQuality) -> &'static str {
    match quality {
        DataQuality::DirectVerified => "direct_verified",
        DataQuality::DirectUnverified => "direct_unverified",
        DataQuality::OfficialDelayed => "official_delayed",
        DataQuality::Aggregated => "aggregated",
        DataQuality::Indicative => "indicative",
        DataQuality::Modeled => "modeled",
        DataQuality::Estimated => "estimated",
        DataQuality::Stale => "stale",
        DataQuality::Quarantined => "quarantined",
    }
}

fn map_runtime_error(error: SourceRuntimeViewError) -> ServiceError {
    match error {
        SourceRuntimeViewError::Cancelled => ServiceError::Cancelled,
        SourceRuntimeViewError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        SourceRuntimeViewError::ResourceExhausted => ServiceError::ResourceExhausted,
        SourceRuntimeViewError::Unavailable => ServiceError::Unavailable,
        SourceRuntimeViewError::InvalidSnapshot => ServiceError::InvalidResult,
    }
}

fn map_onboarding_error(error: ProviderOnboardingError) -> ServiceError {
    match error {
        ProviderOnboardingError::UnknownProfile => ServiceError::NotFound,
        ProviderOnboardingError::InvalidProfile
        | ProviderOnboardingError::InvalidRequest
        | ProviderOnboardingError::AdministrativeContactRequired
        | ProviderOnboardingError::SecretImportUnavailable
        | ProviderOnboardingError::InvalidSecretShape => ServiceError::InvalidRequest,
        ProviderOnboardingError::OperationCancelled => ServiceError::Cancelled,
        ProviderOnboardingError::Catalog(
            CatalogError::InvalidLimit
            | CatalogError::ResultByteLimitExceeded
            | CatalogError::ResultRowLimitExceeded,
        ) => ServiceError::ResourceExhausted,
        ProviderOnboardingError::Catalog(CatalogError::OnboardingSessionNotFound) => {
            ServiceError::NotFound
        }
        ProviderOnboardingError::Catalog(CatalogError::OnboardingDeadlineExceeded) => {
            ServiceError::DeadlineExceeded
        }
        ProviderOnboardingError::SecretVerificationFailed
        | ProviderOnboardingError::InvalidSessionState
        | ProviderOnboardingError::ClientConfiguration
        | ProviderOnboardingError::ProbeUnavailable
        | ProviderOnboardingError::Clock
        | ProviderOnboardingError::Profile(_)
        | ProviderOnboardingError::Catalog(_)
        | ProviderOnboardingError::SecretStore(_)
        | ProviderOnboardingError::Identity(_)
        | ProviderOnboardingError::Network(_)
        | ProviderOnboardingError::Tls(_) => ServiceError::Unavailable,
    }
}

fn map_portal_error(_error: ProviderPortalError) -> ServiceError {
    ServiceError::Unavailable
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
