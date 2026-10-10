//! Explicit nonmarket schedule metadata under an already authorized Alpaca account.

use market_squawk_domain::{
    ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence, EffectiveInterval,
    ExactPayloadEvidence, RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability,
    SourceId, SourceIdentifier,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationGrant, AuthorizationMode, CoverageDomain, EndpointPolicy,
    FreshnessPolicy, HistoricalCapability, HttpRequestBounds, NetworkAccessPolicy, PathScope,
    ProviderBudgetPolicy, QueryParameterRule, QuerySensitivity, SourceCapabilities, SourceClass,
    SourceCoverage, SourceMetadata, SourceMetadataInput, SourceProtocolProfile,
};

use crate::{AlpacaCalendarMarket, AlpacaError, AlpacaTradingApiEnvironment};

/// Constructs an extraction-only calendar source with explicit application freshness policy.
/// This source declares no quote latency or instrument coverage; actual market scope is retained
/// in each exact request and canonical Coverage observation.
///
/// # Errors
/// Rejects inconsistent source authorization, nonmarket coverage, budget, or endpoint policy.
#[allow(
    clippy::too_many_arguments,
    reason = "source, account, coverage and request policies remain explicit"
)]
pub fn try_alpaca_calendar_metadata(
    source_id: SourceId,
    revision_evidence: RevisionBoundPayloadEvidence,
    authorization: AuthorizationGrant,
    coverage_evidence: ExactPayloadEvidence,
    coverage_effective: EffectiveInterval,
    freshness: FreshnessPolicy,
    budget: ProviderBudgetPolicy,
    bounds: HttpRequestBounds,
    environment: AlpacaTradingApiEnvironment,
) -> Result<SourceMetadata, AlpacaError> {
    let metadata = SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        source_id,
        revision_evidence,
        SourceClass::Broker,
        SourceIdentifier::try_from(crate::config::ALPACA_PROVIDER)?,
        authorization,
        SourceCoverage::try_non_instrument(
            coverage_evidence,
            coverage_effective,
            CoverageDomain::MarketCalendar,
            CoverageDelay::NotApplicable,
            DeliveryEvidence::AuthorizedBroker,
        )?,
        DataQuality::Aggregated,
        NetworkAccessPolicy::Allowlisted(calendar_endpoint_policy(bounds, environment)?),
        freshness,
        Some(budget),
        calendar_capabilities(),
        SourceProtocolProfile::NotLive,
    ))?;
    validate_alpaca_calendar_metadata(&metadata, bounds, environment)?;
    Ok(metadata)
}

/// Checks the exact closed endpoint and nonmarket extraction contract before runtime admission.
///
/// # Errors
/// Rejects any source or policy field outside the exact calendar profile.
pub fn validate_alpaca_calendar_metadata(
    metadata: &SourceMetadata,
    bounds: HttpRequestBounds,
    environment: AlpacaTradingApiEnvironment,
) -> Result<(), AlpacaError> {
    if metadata.provider().as_str() != crate::config::ALPACA_PROVIDER
        || metadata.source_class() != SourceClass::Broker
        || metadata.authorization().mode() != AuthorizationMode::UserAuthorized
        || metadata.coverage().domain() != CoverageDomain::MarketCalendar
        || metadata.coverage().delay() != CoverageDelay::NotApplicable
        || metadata.coverage().delivery() != DeliveryEvidence::AuthorizedBroker
        || metadata.quality_ceiling() != DataQuality::Aggregated
        || metadata.capabilities() != calendar_capabilities()
        || metadata.protocol_profile() != &SourceProtocolProfile::NotLive
        || metadata.network_policy()
            != &NetworkAccessPolicy::Allowlisted(calendar_endpoint_policy(bounds, environment)?)
    {
        return Err(AlpacaError::InvalidCoverage);
    }
    Ok(())
}

const fn calendar_capabilities() -> SourceCapabilities {
    SourceCapabilities::new(
        false,
        true,
        SequenceCapability::Unsupported,
        ChecksumCapability::Unsupported,
        HistoricalCapability::Historical,
        false,
    )
}

fn calendar_endpoint_policy(
    bounds: HttpRequestBounds,
    environment: AlpacaTradingApiEnvironment,
) -> Result<EndpointPolicy, AlpacaError> {
    let mut endpoints = Vec::with_capacity(3);
    for market in [
        AlpacaCalendarMarket::Iex,
        AlpacaCalendarMarket::Nyse,
        AlpacaCalendarMarket::Nasdaq,
    ] {
        let date_rule = |name: &str| {
            QueryParameterRule::try_new(
                SourceIdentifier::try_from(name)?,
                10,
                false,
                QuerySensitivity::Public,
            )
            .map_err(AlpacaError::from)
        };
        endpoints.push(ApiEndpointRule::try_new(
            &format!(
                "{}/v3/calendar/{}",
                environment.origin(),
                market.request_code()
            ),
            PathScope::Exact,
            vec![
                date_rule("start")?,
                date_rule("end")?,
                QueryParameterRule::try_new_exact_public(
                    SourceIdentifier::try_from("timezone")?,
                    SourceIdentifier::try_from("UTC")?,
                )?,
            ],
            3,
            96,
        )?);
    }
    Ok(EndpointPolicy::try_from_api_rules(endpoints, bounds)?)
}
