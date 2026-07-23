//! Provider registration, onboarding, and authority-free source-state application service.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use market_squawk_data::CatalogLimit;
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, ServiceLimits, TypedToolRequest, TypedToolResult,
};
use serde_json::json;
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
    ProviderOnboardingPortal, ProviderOnboardingService, ProviderPortalActivationAuthority,
    ProviderPortalConfig, ProviderPortalError,
};

mod results;
mod runtime;

pub use runtime::{
    SourceRuntimeRequest, SourceRuntimeSnapshot, SourceRuntimeSnapshotBatch,
    SourceRuntimeSnapshotError, SourceRuntimeView, SourceRuntimeViewError,
};

use results::{
    SourceReadKind, bounded_source_result, data_quality_name, ensure_provider_scope, inactive_row,
    map_onboarding_error, map_portal_error, map_runtime_error, not_applicable_result,
    registration_value, requested_sources, required_profile_field, required_provider, runtime_row,
    to_json,
};

const SOURCE_REGISTER: &str = "Source.Register";
const SOURCE_GET_STATUS: &str = "Source.GetStatus";
const SOURCE_GET_COVERAGE: &str = "Source.GetCoverage";
const SOURCE_GET_HEALTH: &str = "Source.GetHealth";
const SOURCE_SETUP: &str = "Source.Setup";

const MAX_CURRENT_SESSIONS: usize = 32;
const PORTAL_LIFETIME: Duration = Duration::from_secs(15 * 60);
const PORTAL_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const PORTAL_MAX_REQUESTS: u64 = 512;
const PORTAL_MAX_CONNECTIONS: usize = 16;

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
        portal_activation: Arc<dyn ProviderPortalActivationAuthority>,
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
                portal_activation,
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
    portal_activation: Arc<dyn ProviderPortalActivationAuthority>,
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
            .field("portal_activation", &"[DURABLE ADAPTER AUTHORITY]")
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
