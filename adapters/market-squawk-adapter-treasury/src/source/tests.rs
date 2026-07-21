use std::collections::VecDeque;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AuthorizationBasis, CalendarDate, ChecksumCapability, CoverageDelay, DataQuality,
    DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    MetadataRevision, ResearchObservation, RevisionBoundPayloadEvidence, SchemaVersion,
    SequenceCapability, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    BackoffPolicy, BudgetScope, CoverageDomain, DiscoveryRequest, EndpointPolicy,
    ExtractionRequest, ExtractionSource, FreshnessPolicy, HistoricalCapability,
    NetworkAccessPolicy, PathScope, ProviderBudgetPolicy, QueryParameterRule, QuerySensitivity,
    SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput,
    SourceProtocolProfile,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::client::{
    TreasuryHttpRequest, TreasuryHttpResponse, TreasuryTransport, system_timestamp,
};
use crate::{TreasuryFiscalQuery, TreasurySourceConfig};

use super::TreasurySource;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Debug)]
struct ScriptedTransport {
    responses: Mutex<VecDeque<TreasuryHttpResponse>>,
    requested_urls: Mutex<Vec<String>>,
}

impl TreasuryTransport for ScriptedTransport {
    fn execute(
        &self,
        request: TreasuryHttpRequest,
        _max_bytes: usize,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<TreasuryHttpResponse, super::TreasurySourceError>> {
        Box::pin(async move {
            if !request.accept.starts_with("application/") {
                return Err(super::TreasurySourceError::InvalidProtocol);
            }
            self.requested_urls
                .lock()
                .map_err(|_| super::TreasurySourceError::InvalidProtocol)?
                .push(request.url);
            self.responses
                .lock()
                .map_err(|_| super::TreasurySourceError::InvalidProtocol)?
                .pop_front()
                .ok_or(super::TreasurySourceError::InvalidProtocol)
        })
    }
}

#[tokio::test]
async fn authority_bound_sources_emit_canonical_fiscal_and_yield_macro_records() -> TestResult {
    let now = system_timestamp()?;
    let fiscal_query = TreasuryFiscalQuery::average_interest_rates_v2(
        CalendarDate::new(2026, 1, 1)?,
        CalendarDate::new(2026, 12, 31)?,
        NonZeroU16::new(1).ok_or("nonzero page size")?,
    )?;
    let fiscal_config = TreasurySourceConfig::average_interest_rates(fiscal_query);
    let fiscal = exercise_source(
        now,
        fiscal_config,
        DataQuality::OfficialDelayed,
        include_bytes!("../../fixtures/average_interest_rates.json"),
        b"application/json",
    )
    .await?;
    assert_eq!(fiscal.len(), 1);
    assert_macro_record(
        &fiscal[0],
        "treasury:average-interest-rate:v2:Marketable:Treasury%20Bills",
        "3.706",
        DataQuality::OfficialDelayed,
    )?;

    let yield_config = TreasurySourceConfig::daily_par_yield_curve(2026)?;
    let yield_records = exercise_source(
        now,
        yield_config,
        DataQuality::Indicative,
        include_bytes!("../../fixtures/daily_par_yield_curve.xml"),
        b"application/atom+xml",
    )
    .await?;
    assert!(yield_records.len() >= 2);
    let one_month = yield_records
        .iter()
        .find(|record| {
            serde_json::from_slice::<ResearchObservation>(record.payload())
                .is_ok_and(|observation| {
                    matches!(observation, ResearchObservation::Macro(value) if value.series().as_str() == "treasury:daily-par-yield-curve:1m")
                })
        })
        .ok_or("missing one-month canonical yield")?;
    assert_macro_record(
        one_month,
        "treasury:daily-par-yield-curve:1m",
        "3.72",
        DataQuality::Indicative,
    )?;
    Ok(())
}

async fn exercise_source(
    now: Timestamp,
    config: TreasurySourceConfig,
    quality: DataQuality,
    payload: &'static [u8],
    content_type: &'static [u8],
) -> TestResult<Vec<market_squawk_sources::ExtractionRecord>> {
    let metadata = metadata(now, &config, quality)?;
    let response = || TreasuryHttpResponse {
        status: 200,
        retry_after: None,
        content_encoding: None,
        content_type: Some(content_type.to_vec()),
        body: Bytes::from_static(payload),
        received_at: now,
    };
    let transport = Arc::new(ScriptedTransport {
        responses: Mutex::new(VecDeque::from([response(), response()])),
        requested_urls: Mutex::new(Vec::new()),
    });
    let source =
        TreasurySource::try_new_with_transport(metadata.clone(), config, transport.clone())?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(metadata, now)?;
    let authority = registry.extraction_authority(&registered, &source)?;
    let deadline = now.checked_add_nanos(60_000_000_000)?;
    let discovery = source
        .discover(
            authority.clone(),
            DiscoveryRequest::try_new(
                source.dataset()?,
                None,
                NonZeroU16::new(1).ok_or("nonzero result count")?,
                deadline,
            )?,
            CancellationToken::new(),
        )
        .await?;
    let object = discovery
        .objects()
        .first()
        .ok_or("missing discovered object")?
        .clone();
    let extraction = source
        .extract(
            authority,
            ExtractionRequest::try_new(
                object,
                NonZeroU32::new(10_000).ok_or("nonzero record count")?,
                NonZeroU64::new(16 * 1024 * 1024).ok_or("nonzero byte count")?,
                deadline,
            )?,
            CancellationToken::new(),
        )
        .await?;
    let urls = transport
        .requested_urls
        .lock()
        .map_err(|_| "request log poisoned")?;
    assert_eq!(urls.len(), 2);
    assert_eq!(urls[0], urls[1]);
    if quality == DataQuality::Indicative {
        assert!(!urls[0].contains("page="));
        assert!(!urls[0].contains("page%5B"));
    }
    Ok(extraction.records().to_vec())
}

fn assert_macro_record(
    record: &market_squawk_sources::ExtractionRecord,
    series: &str,
    value: &str,
    quality: DataQuality,
) -> TestResult {
    assert_eq!(record.schema().as_str(), "market-squawk-research-v3");
    let observation: ResearchObservation = serde_json::from_slice(record.payload())?;
    let ResearchObservation::Macro(observation) = observation else {
        return Err("expected macro observation".into());
    };
    assert_eq!(observation.series().as_str(), series);
    assert_eq!(observation.unit().as_str(), "percent");
    assert_eq!(
        observation
            .value()
            .observed_value()
            .map(|decimal| decimal.to_string())
            .as_deref(),
        Some(value)
    );
    assert_eq!(observation.context().provenance().quality(), quality);
    assert!(
        observation
            .context()
            .provenance()
            .source_timestamp()
            .is_none()
    );
    assert!(observation.context().time().published().is_none());
    Ok(())
}

fn metadata(
    now: Timestamp,
    config: &TreasurySourceConfig,
    quality: DataQuality,
) -> TestResult<SourceMetadata> {
    let effective = EffectiveInterval::new(now.checked_sub_nanos(1)?, None)?;
    let evidence = exact_evidence(b"treasury-test-metadata");
    let provider = SourceIdentifier::try_from("us-treasury")?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(SourceIdentifier::try_from("official-public-interface")?),
        evidence.clone(),
        effective,
    );
    let endpoint = match config {
        TreasurySourceConfig::AverageInterestRates(query) => ApiEndpointRule::try_new(
            query
                .page(1)?
                .url()
                .split('?')
                .next()
                .ok_or("missing path")?,
            PathScope::Exact,
            query_rules(&[
                ("fields", 1_024),
                ("filter", 512),
                ("sort", 128),
                ("page[number]", 20),
                ("page[size]", 5),
            ])?,
            5,
            4_096,
        )?,
        TreasurySourceConfig::DailyParYieldCurve { profile, year } => ApiEndpointRule::try_new(
            profile
                .page(*year, 0)?
                .url()
                .split('?')
                .next()
                .ok_or("missing path")?,
            PathScope::Exact,
            query_rules(&[("data", 64), ("field_tdr_date_value", 4)])?,
            2,
            256,
        )?,
    };
    let network = EndpointPolicy::try_from_api_rules(
        vec![endpoint],
        market_squawk_sources::HttpRequestBounds::default(),
    )?;
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::new(provider.clone()),
        NonZeroU32::new(100).ok_or("nonzero request budget")?,
        NonZeroU64::new(60_000_000_000).ok_or("nonzero request window")?,
        NonZeroU16::new(2).ok_or("nonzero concurrency")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000).ok_or("nonzero backoff")?,
            NonZeroU64::new(60_000_000_000).ok_or("nonzero max backoff")?,
            0,
        )?,
    )?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from(format!("treasury-{}", quality_token(quality)))?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from(format!(
                "treasury-{}-test-v1",
                quality_token(quality)
            ))?),
            evidence.clone(),
        ),
        SourceClass::OfficialAgency,
        provider,
        authorization,
        SourceCoverage::try_non_instrument(
            evidence,
            effective,
            CoverageDomain::Macroeconomic,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Unknown,
        )?,
        quality,
        NetworkAccessPolicy::Allowlisted(network),
        FreshnessPolicy::try_new(60, 60, 60, 60, 1)?,
        Some(budget),
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::Historical,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))?)
}

fn query_rules(values: &[(&str, u16)]) -> TestResult<Vec<QueryParameterRule>> {
    values
        .iter()
        .map(|(key, max)| {
            QueryParameterRule::try_new(
                SourceIdentifier::try_from(*key)?,
                *max,
                false,
                QuerySensitivity::Public,
            )
            .map_err(Into::into)
        })
        .collect()
}

fn exact_evidence(bytes: &[u8]) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(bytes).into(),
    ))
}

const fn quality_token(quality: DataQuality) -> &'static str {
    match quality {
        DataQuality::OfficialDelayed => "fiscal",
        DataQuality::Indicative => "yield",
        _ => "invalid",
    }
}
