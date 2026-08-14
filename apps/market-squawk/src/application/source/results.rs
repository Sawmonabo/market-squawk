//! Bounded Source-domain result construction and request parsing.

use market_squawk_data::CatalogError;
use market_squawk_domain::{CoverageDelay, DataQuality, SourceIdentifier, Timestamp};
use market_squawk_services::{
    ServiceError, ServiceLimits, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use market_squawk_sources::{
    ConnectionLiveness, MarketFreshness, SourceTimestampFreshness, TransportFreshness,
};
use serde::Serialize;
use serde_json::{Value, json};

use super::{SourceRuntimeSnapshot, SourceRuntimeViewError};
use crate::{
    ProviderOnboardingError, ProviderPortalError, ProviderProfileRegistrationOutcome,
    ProviderProfileView, application::domain_support::encode_hex,
};

#[derive(Clone, Copy)]
pub(super) enum SourceReadKind {
    Status,
    Coverage,
    Health,
}

pub(super) fn inactive_row(
    kind: SourceReadKind,
    profile: &ProviderProfileView,
    profile_value: &Value,
    session: Option<Value>,
    provider_dataset_identifier: Option<&SourceIdentifier>,
) -> Result<Value, ServiceError> {
    Ok(match kind {
        SourceReadKind::Status => json!({
            "profile": profile_value,
            "currentSession": session,
            "providerDatasetIdentifier": provider_dataset_identifier
                .map(SourceIdentifier::as_str),
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

pub(super) fn runtime_row(
    kind: SourceReadKind,
    profile: &ProviderProfileView,
    profile_value: &Value,
    session: Option<Value>,
    provider_dataset_identifier: Option<&SourceIdentifier>,
    runtime: &SourceRuntimeSnapshot,
) -> Result<Value, ServiceError> {
    Ok(match kind {
        SourceReadKind::Status => json!({
            "profile": profile_value,
            "currentSession": session,
            "providerDatasetIdentifier": provider_dataset_identifier
                .map(SourceIdentifier::as_str),
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
        "connectionGeneration": runtime.connection_generation.get().to_string(),
        "sessionId": runtime.session_id.as_str(),
        "healthEpoch": runtime.health_epoch.to_string(),
        "stateRevision": runtime.state_revision.to_string(),
        "assessmentId": runtime.assessment_id.as_str(),
        "bindingDigest": encode_hex(runtime.binding_digest),
        "connection": connection_liveness_value(runtime.connection),
        "integrity": to_json(&runtime.stream_integrity)?,
        "quality": to_json(&runtime.quality)?,
        "observedAtUnixNanos": runtime.observed_at.unix_nanos().to_string(),
        "qualificationEvaluatedAtUnixNanos":
            runtime.qualification_evaluated_at.unix_nanos().to_string(),
        "qualificationValidUntilUnixNanos": runtime.qualification_valid_until.unix_nanos().to_string(),
    }))
}

fn connection_liveness_value(value: ConnectionLiveness) -> Value {
    match value {
        ConnectionLiveness::Connecting => json!("connecting"),
        ConnectionLiveness::Live { last_activity_at } => {
            json!({"live": {"last_activity_at": last_activity_at.unix_nanos().to_string()}})
        }
        ConnectionLiveness::Stale { last_activity_at } => {
            json!({"stale": {"last_activity_at": last_activity_at.unix_nanos().to_string()}})
        }
        ConnectionLiveness::Disconnected { disconnected_at } => {
            json!({"disconnected": {"disconnected_at": disconnected_at.unix_nanos().to_string()}})
        }
    }
}

fn transport_freshness_value(value: TransportFreshness) -> Value {
    match value {
        TransportFreshness::Uninitialized => json!("uninitialized"),
        TransportFreshness::Fresh { last_transport_at } => {
            json!({"fresh": {"last_transport_at": last_transport_at.unix_nanos().to_string()}})
        }
        TransportFreshness::Stale { last_transport_at } => {
            json!({"stale": {"last_transport_at": last_transport_at.unix_nanos().to_string()}})
        }
    }
}

fn market_freshness_value(value: MarketFreshness) -> Value {
    match value {
        MarketFreshness::Uninitialized => json!("uninitialized"),
        MarketFreshness::Fresh { last_market_at } => {
            json!({"fresh": {"last_market_at": last_market_at.unix_nanos().to_string()}})
        }
        MarketFreshness::Stale { last_market_at } => {
            json!({"stale": {"last_market_at": last_market_at.unix_nanos().to_string()}})
        }
    }
}

fn source_timestamp_freshness_value(value: SourceTimestampFreshness) -> Value {
    match value {
        SourceTimestampFreshness::Uninitialized => json!("uninitialized"),
        SourceTimestampFreshness::Fresh { last_source_at } => {
            json!({"fresh": {"last_source_at": last_source_at.unix_nanos().to_string()}})
        }
        SourceTimestampFreshness::Stale { last_source_at } => {
            json!({"stale": {"last_source_at": last_source_at.unix_nanos().to_string()}})
        }
    }
}

fn coverage_delay_value(value: CoverageDelay) -> Value {
    match value {
        CoverageDelay::RealTime => json!({"kind": "real_time"}),
        CoverageDelay::Delayed(nanos) => {
            json!({"kind": "delayed", "value": nanos.to_string()})
        }
    }
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
        "delay": coverage_delay_value(scope.delay()),
        "consolidation": to_json(&scope.consolidation())?,
        "effectiveFromUnixNanos": scope.effective_from().unix_nanos().to_string(),
        "effectiveUntilUnixNanos": scope
            .effective_until()
            .map(Timestamp::unix_nanos)
            .map(|nanos| nanos.to_string()),
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
        "connectionGeneration": runtime.connection_generation.get().to_string(),
        "sessionId": runtime.session_id.as_str(),
        "healthEpoch": runtime.health_epoch.to_string(),
        "stateRevision": runtime.state_revision.to_string(),
        "assessmentId": runtime.assessment_id.as_str(),
        "bindingDigest": encode_hex(runtime.binding_digest),
        "connection": connection_liveness_value(runtime.connection),
        "transportFreshness": transport_freshness_value(runtime.transport_freshness),
        "marketFreshness": market_freshness_value(runtime.market_freshness),
        "sourceTimestampFreshness": source_timestamp_freshness_value(runtime.source_freshness),
        "streamIntegrity": to_json(&runtime.stream_integrity)?,
        "captureIntegrity": to_json(&runtime.capture_integrity)?,
        "coverageStatus": to_json(&runtime.coverage_status)?,
        "quality": to_json(&runtime.quality)?,
        "observedAtUnixNanos": runtime.observed_at.unix_nanos().to_string(),
        "qualificationEvaluatedAtUnixNanos":
            runtime.qualification_evaluated_at.unix_nanos().to_string(),
        "qualificationValidUntilUnixNanos":
            runtime.qualification_valid_until.unix_nanos().to_string(),
    }))
}

pub(super) fn registration_value(
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

pub(super) fn bounded_source_result(
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

pub(super) fn not_applicable_result(
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

pub(super) fn required_provider(request: &TypedToolRequest) -> Result<&str, ServiceError> {
    request
        .arguments()
        .get("provider")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
}

pub(super) fn ensure_provider_scope(
    request: &TypedToolRequest,
    provider: &str,
) -> Result<(), ServiceError> {
    let filters = requested_sources(request)?;
    if filters.iter().any(|filter| filter.as_str() != provider) {
        Err(ServiceError::InvalidRequest)
    } else {
        Ok(())
    }
}

pub(super) fn ensure_exact_provider_scope(
    request: &TypedToolRequest,
    provider: &SourceIdentifier,
) -> Result<(), ServiceError> {
    let filters = requested_sources(request)?;
    if filters.len() == 1 && filters.first() == Some(provider) {
        Ok(())
    } else {
        Err(ServiceError::InvalidRequest)
    }
}

pub(super) fn required_identifier(
    request: &TypedToolRequest,
    name: &str,
) -> Result<SourceIdentifier, ServiceError> {
    request
        .arguments()
        .get(name)
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
        .and_then(|value| {
            SourceIdentifier::try_from(value).map_err(|_error| ServiceError::InvalidRequest)
        })
}

pub(super) fn requested_sources(
    request: &TypedToolRequest,
) -> Result<Vec<SourceIdentifier>, ServiceError> {
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

pub(super) fn required_profile_field(profile: &Value, field: &str) -> Result<Value, ServiceError> {
    profile
        .get(field)
        .cloned()
        .ok_or(ServiceError::InvalidResult)
}

pub(super) fn to_json<T: Serialize>(value: &T) -> Result<Value, ServiceError> {
    serde_json::to_value(value).map_err(|_error| ServiceError::InvalidResult)
}

pub(super) const fn data_quality_name(quality: DataQuality) -> &'static str {
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

pub(super) fn map_runtime_error(error: SourceRuntimeViewError) -> ServiceError {
    match error {
        SourceRuntimeViewError::Cancelled => ServiceError::Cancelled,
        SourceRuntimeViewError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        SourceRuntimeViewError::ResourceExhausted => ServiceError::ResourceExhausted,
        SourceRuntimeViewError::Unavailable => ServiceError::Unavailable,
        SourceRuntimeViewError::InvalidSnapshot => ServiceError::InvalidResult,
    }
}

pub(super) fn map_onboarding_error(error: ProviderOnboardingError) -> ServiceError {
    match error {
        ProviderOnboardingError::UnknownProfile => ServiceError::NotFound,
        ProviderOnboardingError::InvalidProfile
        | ProviderOnboardingError::InvalidRequest
        | ProviderOnboardingError::AdministrativeContactRequired
        | ProviderOnboardingError::SecretImportUnavailable
        | ProviderOnboardingError::RenewalUnavailable
        | ProviderOnboardingError::InvalidSecretShape => ServiceError::InvalidRequest,
        ProviderOnboardingError::OperationCancelled => ServiceError::Cancelled,
        ProviderOnboardingError::ProbeRateLimited => ServiceError::ResourceExhausted,
        ProviderOnboardingError::ProbeDeadlineExceeded => ServiceError::DeadlineExceeded,
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
        ProviderOnboardingError::RightsBlocked | ProviderOnboardingError::CredentialRejected => {
            ServiceError::Unauthorized
        }
        ProviderOnboardingError::SecretVerificationFailed
        | ProviderOnboardingError::SecretOperationUnavailable
        | ProviderOnboardingError::SecretCleanupUnavailable
        | ProviderOnboardingError::RemoteReconciliationRequired
        | ProviderOnboardingError::InvalidSessionState
        | ProviderOnboardingError::ClientConfiguration
        | ProviderOnboardingError::ProbeUnavailable
        | ProviderOnboardingError::OfficialDocumentUnavailable
        | ProviderOnboardingError::EvidenceRefreshRequired
        | ProviderOnboardingError::ActivationUnavailable
        | ProviderOnboardingError::ActivationExpired
        | ProviderOnboardingError::Clock
        | ProviderOnboardingError::Profile(_)
        | ProviderOnboardingError::Catalog(_)
        | ProviderOnboardingError::SecretStore(_)
        | ProviderOnboardingError::Identity(_)
        | ProviderOnboardingError::Network(_)
        | ProviderOnboardingError::Tls(_) => ServiceError::Unavailable,
    }
}

pub(super) fn map_portal_error(_error: ProviderPortalError) -> ServiceError {
    ServiceError::Unavailable
}
