use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use bytes::Bytes;
use market_squawk_domain::{
    BarTimeSemantics, BarTimestampBasis, Currency, DigestAlgorithm, EffectiveInterval,
    EvidenceDigest, ExactPayloadEvidence, InstrumentId, MarketBarSessionEvidence,
    MarketBarSessionKind, MetadataRevision, ProviderInstrumentId, SourceId, SourceIdentifier,
    Timestamp, VenueId,
};
use market_squawk_platform::LocalPaths;
use market_squawk_sources::{
    AvailabilityEvidence, DiscoveryRequest, ExtractionRequest, ProviderNativeLineageImplementation,
    SourceObject,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::http::ScriptedHttpResponse;
use crate::{
    AdapterBounds, AdmissionDecision, AdmissionPolicy, AttemptKind, ChartInterval, ChartWindow,
    CircuitSnapshot, ExplicitDemand, ExplicitDemandPurpose, ProviderField,
    YAHOO_MISSING_RETRY_AFTER_COOLDOWN_FLOOR_MS, YahooAdmission, YahooAssetClass,
    YahooAttemptTarget, YahooCanonicalInstrumentAuthority, YahooCanonicalPublicationRequest,
    YahooChartActionScope, YahooChartAdjustmentMode, YahooChartEventKind, YahooChartSessionScope,
    YahooDurableStateStore, YahooExecutionDisposition, YahooExecutionLimits, YahooHttpFailureKind,
    YahooHttpSession, YahooHttpSessionConfig, YahooLocale, YahooParsedResponse,
    YahooPublicationBinding, YahooPublicationBridgeError, YahooRequestPlanner, YahooSymbol,
    YahooTarget,
};

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn new(name: &str) -> Self {
        Self(std::env::temp_dir().join(format!("market-squawk-yahoo-{name}-{}", Uuid::new_v4())))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn bounds(maximum_symbols: usize) -> AdapterBounds {
    AdapterBounds {
        max_symbols_per_operation: maximum_symbols,
        max_response_bytes: 64 * 1024,
        max_records_per_response: 512,
        max_option_contracts: 512,
        max_option_expirations: 64,
        max_fund_holdings: 128,
        max_string_bytes: 512,
    }
}

fn planner(maximum_symbols: usize) -> Result<YahooRequestPlanner, Box<dyn Error>> {
    Ok(YahooRequestPlanner::new(
        bounds(maximum_symbols),
        YahooLocale::new("en-US", "US", 512)?,
    )?)
}

fn demand(id: &str) -> Result<ExplicitDemand, Box<dyn Error>> {
    Ok(ExplicitDemand::new(
        id,
        1_786_473_600_000,
        ExplicitDemandPurpose::TargetedHistory,
        512,
    )?)
}

fn target(symbol: String) -> Result<YahooTarget, Box<dyn Error>> {
    Ok(YahooTarget {
        symbol: YahooSymbol::parse(symbol, 512)?,
        asset_class: YahooAssetClass::Equity,
    })
}

fn chart_publication_request(
    raw: &crate::YahooRawReceipt,
    binding: &YahooPublicationBinding,
) -> Result<YahooCanonicalPublicationRequest, Box<dyn Error>> {
    let received_at = Timestamp::from_unix_nanos(
        raw.received_at_unix_ms
            .checked_mul(1_000_000)
            .ok_or("received timestamp")?,
    );
    let available_at = Timestamp::from_unix_nanos(
        raw.available_at_unix_ms
            .checked_mul(1_000_000)
            .ok_or("available timestamp")?,
    );
    let ingested_at = available_at.checked_add_nanos(1)?;
    let deadline = ingested_at.checked_add_nanos(60_000_000_000)?;
    let dataset = SourceIdentifier::try_from("yahoo-finance.experimental.chart-history")?;
    let discovery = DiscoveryRequest::try_new(dataset, None, NonZeroU16::MIN, deadline)?;
    let object = SourceObject::try_new_with_availability(
        binding.source_id().clone(),
        binding.metadata_revision().clone(),
        &discovery,
        SourceIdentifier::try_from("yahoo-chart-aapl-response")?,
        SourceIdentifier::try_from("application-json")?,
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&raw.response_bytes).into(),
        )),
        EffectiveInterval::new(received_at, None)?,
        None,
        AvailabilityEvidence::LocalFirstObserved {
            observed_at: available_at,
        },
        Some(u64::try_from(raw.response_bytes.len())?),
    )?;
    let extraction = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(32).ok_or("record bound")?,
        NonZeroU64::new(4 * 1024 * 1024).ok_or("byte bound")?,
        deadline,
    )?;
    let authority = YahooCanonicalInstrumentAuthority::try_new(
        YahooSymbol::parse("AAPL", 512)?,
        InstrumentId::try_from(Uuid::from_u128(10))?,
        ProviderInstrumentId::try_from("AAPL")?,
        Some(VenueId::try_from("yahoo-nasdaq-experimental")?),
        Some(Currency::try_from("USD")?),
        MetadataRevision::new(SourceIdentifier::try_from("instrument-map-v1")?),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [7; 32]),
    )?;
    let session = MarketBarSessionEvidence::try_new(
        MarketBarSessionKind::ProviderDefined,
        SourceIdentifier::try_from("yahoo-provider-daily-session")?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [8; 32]),
    )?;
    let semantics = [1_786_473_600_i64, 1_786_560_000_i64]
        .into_iter()
        .map(|seconds| {
            let start = Timestamp::from_unix_nanos(seconds * 1_000_000_000);
            Ok(BarTimeSemantics::try_new(
                start,
                start.checked_add_nanos(86_400_000_000_000)?,
                BarTimestampBasis::PeriodStart,
                session.clone(),
            )?)
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(YahooCanonicalPublicationRequest::try_new(
        extraction,
        vec![authority],
        semantics,
        ingested_at,
    )?)
}

#[tokio::test]
async fn explicit_demand_network_response_crosses_one_pending_publication_handoff()
-> Result<(), Box<dyn Error>> {
    let chart = Bytes::from_static(
        br#"{"chart":{"error":null,"result":[{"meta":{"symbol":"AAPL","instrumentType":"EQUITY","currency":"USD","exchangeName":"NMS","fullExchangeName":"NasdaqGS","market":"us_market","country":"US","exchangeTimezoneName":"America/New_York","exchangeDataDelayedBy":0,"regularMarketTime":1786560050,"dataGranularity":"1d","range":"5d"},"timestamp":[1786473600,1786560000],"indicators":{"quote":[{"open":[201.0,null],"high":[202.0,203.0],"low":[200.0],"close":[201.5,202.5],"volume":[100,null]}],"adjclose":[{"adjclose":[199.5,200.5]}]},"events":{"dividends":{"dividend-aapl-20260811":{"amount":0.25,"date":1786473600,"currency":"USD"}},"splits":{"1786560000":{"date":1786560000,"numerator":2.0,"denominator":1.0,"splitRatio":"2:1"}},"capitalGains":{"1786646400":{"amount":0.1,"date":1786646400,"currency":"USD"}}}}]}}"#,
    );
    let config = YahooHttpSessionConfig {
        adapter_bounds: bounds(4),
        connect_timeout: Duration::from_secs(1),
        read_timeout: Duration::from_secs(1),
        total_timeout: Duration::from_secs(3),
        max_session_response_bytes: 64 * 1_024,
        max_crumb_bytes: 512,
        max_cache_entries: 8,
        max_cache_bytes: 128 * 1_024,
        max_redirects: 3,
        max_attempt_receipts: 8,
        admission_policy: AdmissionPolicy::new(YAHOO_MISSING_RETRY_AFTER_COOLDOWN_FLOOR_MS, 3)?,
    };
    let state_root = std::env::temp_dir().join(format!(
        "market-squawk-yahoo-durable-test-{}",
        Uuid::new_v4()
    ));
    let session = YahooHttpSession::new_for_test_with_durable(
        config,
        Url::parse("http://yahoo.test/")?,
        vec![
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after_ms: None,
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after_ms: None,
                body: Bytes::from_static(b"local-crumb"),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "application/json",
                retry_after_ms: None,
                body: chart.clone(),
            },
            ScriptedHttpResponse {
                status: 429,
                content_type: "text/plain",
                retry_after_ms: Some(2_000),
                body: Bytes::from_static(b"Too Many Requests"),
            },
        ],
        Some(YahooDurableStateStore::try_open(&state_root)?),
    )?;
    let plan = planner(4)?.chart_history(
        demand("http-fan-out")?,
        vec![target("AAPL".to_owned())?, target("MSFT".to_owned())?],
        ChartWindow::FiveDays,
        ChartInterval::OneDay,
        true,
    )?;
    let limits = YahooExecutionLimits {
        deadline: Instant::now() + Duration::from_secs(5),
        maximum_cache_age: Duration::ZERO,
    };
    let cancellation = CancellationToken::new();
    let first = session
        .execute(plan.requests[0].clone(), limits, &cancellation)
        .await?;
    let YahooParsedResponse::Chart(first_chart) = first.parsed_response() else {
        return Err("first response must be parsed chart enrichment".into());
    };
    let network_request_identity = first.raw_receipt().request_identity_sha256_hex.clone();
    let network_response_identity = first.raw_receipt().response_sha256_hex.clone();
    let network_received_at = first.raw_receipt().received_at_unix_ms;
    let network_attempts = first.raw_receipt().attempts.clone();
    assert_eq!(
        first_chart.provenance.received_at_unix_ms,
        first.raw_receipt().received_at_unix_ms
    );
    assert_eq!(
        first_chart.provenance.available_at_unix_ms,
        first.raw_receipt().available_at_unix_ms
    );
    assert_eq!(first.raw_receipt().attempts.len(), 3);
    assert_eq!(
        first.raw_receipt().attempts[0].target,
        YahooAttemptTarget::CookieBootstrap
    );
    assert_eq!(
        first.raw_receipt().attempts[1].target,
        YahooAttemptTarget::BasicCrumb
    );
    assert_eq!(
        first.raw_receipt().attempts[2].target,
        YahooAttemptTarget::Data(crate::YahooRequestFamily::ChartHistory)
    );
    let publication_binding = YahooPublicationBinding::try_new(
        SourceId::try_from("yahoo-finance-experimental")?,
        MetadataRevision::new(SourceIdentifier::try_from("rev-3")?),
        Uuid::from_u128(1),
        Uuid::from_u128(2),
    )?;
    let canonical = chart_publication_request(first.raw_receipt(), &publication_binding)?;
    let seal_root = TemporaryDirectory::new("seal");
    let seal_paths = LocalPaths::prepare(seal_root.path())?;
    let store = seal_paths.sealed_research_journal_store()?;
    let pending = first.into_pending_publication(publication_binding.clone(), canonical)?;
    let (rejoin, seal_request) = pending.into_sealing_parts();
    let native_history = rejoin
        .pending_chart_history()
        .ok_or("chart response must retain one native history candidate")?;
    assert_eq!(
        native_history.source_id().as_str(),
        "yahoo-finance-experimental"
    );
    assert_eq!(
        native_history
            .metadata_revision()
            .as_source_identifier()
            .as_str(),
        "rev-3"
    );
    assert_eq!(native_history.event_id(), Uuid::from_u128(1));
    assert_eq!(native_history.connection_id(), Uuid::from_u128(2));
    assert_eq!(
        native_history.request_identity_sha256_hex(),
        network_request_identity
    );
    assert_eq!(
        native_history.raw_body_identity_sha256_hex(),
        network_response_identity
    );
    assert_eq!(
        native_history.request_evidence().interval(),
        ChartInterval::OneDay
    );
    assert_eq!(
        native_history.request_evidence().window(),
        ChartWindow::FiveDays
    );
    assert_eq!(
        native_history.request_evidence().session_scope(),
        YahooChartSessionScope::IncludePreAndPost
    );
    assert!(
        !native_history
            .request_evidence()
            .provider_classifies_each_bar_session()
    );
    assert_eq!(
        native_history.request_evidence().adjustment_mode(),
        YahooChartAdjustmentMode::RawOhlcvWithSeparateAdjustedClose
    );
    assert_eq!(
        native_history.request_evidence().action_scope(),
        YahooChartActionScope::DividendsSplitsAndCapitalGains
    );
    let native_chart = native_history
        .enrichment()
        .data
        .as_ref()
        .ok_or("native history must retain parsed chart evidence")?;
    assert_eq!(
        native_chart.timestamp_container_entries,
        ProviderField::Value(2)
    );
    let ProviderField::Value(indicator_containers) = &native_chart.indicators else {
        return Err("native history must retain the indicators-container state".into());
    };
    assert_eq!(
        indicator_containers.quote_container_entries,
        ProviderField::Value(1)
    );
    assert_eq!(
        indicator_containers.adjusted_close_container_entries,
        ProviderField::Value(1)
    );
    assert_eq!(native_chart.bars.len(), 2);
    assert_eq!(
        native_chart.bars[0].close,
        ProviderField::Value(Decimal::new(2_015, 1))
    );
    assert_eq!(
        native_chart.bars[0].adjusted_close,
        ProviderField::Value(Decimal::new(1_995, 1))
    );
    assert_eq!(native_chart.bars[1].open, ProviderField::Null);
    assert_eq!(native_chart.bars[1].low, ProviderField::Missing);
    assert_eq!(native_chart.bars[1].volume, ProviderField::Null);
    let ProviderField::Value(native_actions) = &native_chart.events else {
        return Err("native history must retain the provider action-container state".into());
    };
    let ProviderField::Value(dividends) = &native_actions.dividends else {
        return Err("native history must retain the dividends-container state".into());
    };
    let ProviderField::Value(splits) = &native_actions.splits else {
        return Err("native history must retain the splits-container state".into());
    };
    let ProviderField::Value(capital_gains) = &native_actions.capital_gains else {
        return Err("native history must retain the capital-gains-container state".into());
    };
    assert_eq!(dividends.len(), 1);
    assert_eq!(splits.len(), 1);
    assert_eq!(capital_gains.len(), 1);
    assert_eq!(dividends[0].kind, YahooChartEventKind::Dividend);
    assert_eq!(dividends[0].provider_identity, "dividend-aapl-20260811");
    assert_eq!(
        dividends[0].date_unix_seconds,
        ProviderField::Value(1_786_473_600)
    );
    assert_eq!(splits[0].kind, YahooChartEventKind::Split);
    assert_eq!(capital_gains[0].kind, YahooChartEventKind::CapitalGain);
    let sealed_publication = rejoin.try_rejoin(seal_request.seal(&store)?)?;
    assert_eq!(
        sealed_publication.authority(),
        crate::EvidenceAuthority::ExperimentalSupplementOnly
    );
    assert!(!sealed_publication.governed_override_permitted());
    assert!(sealed_publication.revision_plan().native_lineage_required());
    assert_eq!(
        sealed_publication.sealed_capture_binding().record_count(),
        1
    );
    assert_eq!(
        sealed_publication
            .sealed_capture_binding()
            .native_lineage()
            .schema()
            .implementation(),
        ProviderNativeLineageImplementation::YahooEnrichmentV1
    );
    assert!(
        sealed_publication
            .sealed_capture_binding()
            .native_lineage()
            .batch_sidecar()
            .is_some()
    );
    let lineage = sealed_publication.sealed_capture_binding().native_lineage();
    let sidecar: serde_json::Value = serde_json::from_slice(
        lineage
            .batch_sidecar()
            .ok_or("native sidecar")?
            .semantic_payload(),
    )?;
    let row: serde_json::Value = serde_json::from_slice(lineage.rows()[0].semantic_payload())?;
    let pointer = row
        .get("parsed_response_pointer")
        .and_then(serde_json::Value::as_str)
        .ok_or("parsed-response pointer")?;
    assert_eq!(
        sidecar
            .get("parsed_response")
            .and_then(|parsed| parsed.pointer(pointer)),
        row.get("native_value")
    );
    sealed_publication.sealed_capture_binding().validate()?;

    let second = session
        .execute(plan.requests[1].clone(), limits, &cancellation)
        .await
        .expect_err("second ticker must exercise the mock 429");
    assert!(matches!(
        second.kind,
        YahooHttpFailureKind::CircuitOpen { .. }
    ));
    assert_eq!(second.attempts.len(), 1);
    assert_eq!(second.attempts[0].status, Some(429));
    assert_eq!(
        session.admission().snapshot()?.circuit,
        CircuitSnapshot::Open {
            retry_at_unix_ms: second.attempts[0]
                .completed_at_unix_ms
                .saturating_add(2_000),
        }
    );
    let rejected = session
        .execute(plan.requests[0].clone(), limits, &cancellation)
        .await
        .expect_err("open provider circuit must reject without another attempt");
    assert!(matches!(
        rejected.kind,
        YahooHttpFailureKind::CircuitOpen { .. }
    ));
    assert!(rejected.attempts.is_empty());

    let cached = session
        .execute(
            plan.requests[0].clone(),
            YahooExecutionLimits {
                deadline: limits.deadline,
                maximum_cache_age: Duration::from_secs(60),
            },
            &cancellation,
        )
        .await?;
    assert_eq!(cached.disposition(), YahooExecutionDisposition::CacheHit);
    let cached_canonical = chart_publication_request(cached.raw_receipt(), &publication_binding)?;
    assert!(matches!(
        cached.into_pending_publication(publication_binding.clone(), cached_canonical),
        Err(YahooPublicationBridgeError::NonPublicationResult)
    ));

    let requests = session.scripted_observed_targets().await;
    assert_eq!(requests.len(), 4);
    assert!(requests[2].contains("crumb=local-crumb"));
    assert!(requests[3].contains("/v8/finance/chart/MSFT?"));

    let mut inconsistent_cache_request = plan.requests[0].clone();
    inconsistent_cache_request.requested_targets[0] = target("MSFT".to_owned())?;
    let inconsistent_cache = session
        .execute(
            inconsistent_cache_request,
            YahooExecutionLimits {
                deadline: limits.deadline,
                maximum_cache_age: Duration::from_secs(60),
            },
            &cancellation,
        )
        .await
        .expect_err("a target/URL mismatch must fail before cache lookup");
    assert_eq!(
        inconsistent_cache.kind,
        YahooHttpFailureKind::InvalidRequest
    );
    assert_eq!(session.scripted_observed_targets().await.len(), 4);

    let snapshot = session.admission().snapshot()?;
    assert_eq!(snapshot.logical_primary_operations_total, 2);
    assert_eq!(snapshot.actual_http_attempts_total, 4);
    assert_eq!(snapshot.requested_units_total, 2);
    assert_eq!(snapshot.returned_units_total, 1);
    assert_eq!(snapshot.missing_units_total, 1);
    assert_eq!(snapshot.http_429_total, 1);

    let attempt_bounded = YahooHttpSession::new_for_test_with_durable(
        YahooHttpSessionConfig {
            max_attempt_receipts: 2,
            ..config
        },
        Url::parse("http://yahoo-attempt-bound.test/")?,
        vec![
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after_ms: None,
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after_ms: None,
                body: Bytes::from_static(b"bounded-crumb"),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "application/json",
                retry_after_ms: None,
                body: chart.clone(),
            },
        ],
        None,
    )?;
    let attempt_bound_failure = attempt_bounded
        .execute(plan.requests[0].clone(), limits, &cancellation)
        .await
        .expect_err("the third upstream attempt must be refused before I/O");
    assert_eq!(
        attempt_bound_failure.kind,
        YahooHttpFailureKind::AttemptReceiptLimit
    );
    assert_eq!(attempt_bound_failure.attempts.len(), 2);
    assert_eq!(attempt_bounded.scripted_observed_targets().await.len(), 2);
    assert_eq!(
        attempt_bounded
            .admission()
            .snapshot()?
            .actual_http_attempts_total,
        2
    );

    let fallback_session = YahooHttpSession::new_for_test_with_durable(
        YahooHttpSessionConfig {
            admission_policy: AdmissionPolicy::new(
                YAHOO_MISSING_RETRY_AFTER_COOLDOWN_FLOOR_MS,
                3,
            )?,
            ..config
        },
        Url::parse("http://yahoo-fallback.test/")?,
        vec![
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after_ms: None,
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after_ms: None,
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/html",
                retry_after_ms: None,
                body: Bytes::from_static(
                    br#"<input name="csrfToken" value="csrf"><input name="sessionId" value="session">"#,
                ),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after_ms: None,
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after_ms: None,
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after_ms: None,
                body: Bytes::from_static(b"csrf-crumb"),
            },
            ScriptedHttpResponse {
                status: 500,
                content_type: "application/json",
                retry_after_ms: None,
                body: Bytes::from_static(b"{}"),
            },
        ],
        None,
    )?;
    let fallback_failure = fallback_session
        .execute(plan.requests[0].clone(), limits, &cancellation)
        .await
        .expect_err("one consumed cookie-strategy fallback must not cycle again");
    assert_eq!(
        fallback_failure.kind,
        YahooHttpFailureKind::ProviderStatus { status: 500 }
    );
    assert_eq!(fallback_failure.attempts.len(), 7);
    assert_eq!(fallback_session.scripted_observed_targets().await.len(), 7);
    let fallback_snapshot = fallback_session.admission().snapshot()?;
    assert_eq!(fallback_snapshot.actual_http_attempts_total, 7);
    assert_eq!(fallback_snapshot.schema_failures_total, 1);
    assert_eq!(fallback_snapshot.transport_failures_total, 1);
    assert_eq!(fallback_snapshot.consecutive_failures, 2);

    let same_url_different_semantics = planner(4)?.chart_history(
        demand("same-url-different-semantics")?,
        vec![YahooTarget {
            symbol: YahooSymbol::parse("AAPL", 512)?,
            asset_class: YahooAssetClass::Index,
        }],
        ChartWindow::FiveDays,
        ChartInterval::OneDay,
        true,
    )?;
    let first_identity = crate::http::request_identity(&plan.requests[0]);
    let second_identity = crate::http::request_identity(&same_url_different_semantics.requests[0]);
    assert_eq!(
        plan.requests[0].target(),
        same_url_different_semantics.requests[0].target()
    );
    assert_ne!(first_identity, second_identity);
    let identity_admission = YahooAdmission::new(AdmissionPolicy::new(
        YAHOO_MISSING_RETRY_AFTER_COOLDOWN_FLOOR_MS,
        3,
    )?);
    let first_permit = match identity_admission.admit(
        &plan.requests[0],
        &first_identity,
        AttemptKind::Primary,
        1_786_473_600_000,
    )? {
        AdmissionDecision::Execute(permit) => permit,
        _ => return Err("first semantic identity must own the provider lane".into()),
    };
    assert!(matches!(
        identity_admission.admit(
            &same_url_different_semantics.requests[0],
            &second_identity,
            AttemptKind::Primary,
            1_786_473_600_001,
        )?,
        AdmissionDecision::Busy { .. }
    ));
    assert_eq!(identity_admission.snapshot()?.coalesced_callers_total, 0);
    drop(first_permit);

    drop(session);
    let restarted = YahooHttpSession::new_for_test_with_durable(
        config,
        Url::parse("http://yahoo.test/")?,
        Vec::new(),
        Some(YahooDurableStateStore::try_open(&state_root)?),
    )?;
    let restart_limits = YahooExecutionLimits {
        deadline: Instant::now() + Duration::from_secs(5),
        maximum_cache_age: Duration::from_secs(60),
    };
    let restored_cache = restarted
        .execute(plan.requests[0].clone(), restart_limits, &cancellation)
        .await?;
    assert_eq!(
        restored_cache.disposition(),
        YahooExecutionDisposition::CacheHit
    );
    assert_eq!(
        restored_cache.raw_receipt().request_identity_sha256_hex,
        network_request_identity
    );
    assert_eq!(
        restored_cache.raw_receipt().response_sha256_hex,
        network_response_identity
    );
    assert_eq!(
        restored_cache.raw_receipt().received_at_unix_ms,
        network_received_at
    );
    assert_eq!(restored_cache.raw_receipt().attempts, network_attempts);
    let restored_canonical =
        chart_publication_request(restored_cache.raw_receipt(), &publication_binding)?;
    assert!(matches!(
        restored_cache.into_pending_publication(publication_binding, restored_canonical),
        Err(YahooPublicationBridgeError::NonPublicationResult)
    ));
    let restored_circuit = restarted
        .execute(plan.requests[1].clone(), restart_limits, &cancellation)
        .await
        .expect_err("restart must preserve the open provider circuit");
    assert!(matches!(
        restored_circuit.kind,
        YahooHttpFailureKind::CircuitOpen { .. }
    ));
    assert!(restarted.scripted_observed_targets().await.is_empty());
    drop(restarted);

    let body_rate_session = YahooHttpSession::new_for_test_with_durable(
        YahooHttpSessionConfig {
            admission_policy: AdmissionPolicy::new(1, 3)?,
            ..config
        },
        Url::parse("http://yahoo-body-rate-limit.test/")?,
        vec![
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after_ms: None,
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after_ms: None,
                body: Bytes::from_static(b"too MANY requests\r\n"),
            },
        ],
        None,
    )?;
    let body_rate_limits = YahooExecutionLimits {
        deadline: Instant::now() + Duration::from_secs(5),
        maximum_cache_age: Duration::ZERO,
    };
    let body_rate_failure = body_rate_session
        .execute(plan.requests[0].clone(), body_rate_limits, &cancellation)
        .await
        .expect_err("a crumb body rate limit must stop before any data request");
    assert!(matches!(
        body_rate_failure.kind,
        YahooHttpFailureKind::CircuitOpen { .. }
    ));
    assert_eq!(body_rate_failure.attempts.len(), 2);
    assert_eq!(body_rate_failure.attempts[1].status, Some(200));
    assert!(matches!(
        body_rate_failure.attempts[1].disposition,
        crate::AttemptDisposition::Http429 {
            retry_after_ms: None
        }
    ));
    assert_eq!(body_rate_session.scripted_observed_targets().await.len(), 2);
    let body_rate_snapshot = body_rate_session.admission().snapshot()?;
    assert_eq!(body_rate_snapshot.http_429_total, 1);
    assert_eq!(
        body_rate_snapshot.circuit,
        CircuitSnapshot::Open {
            retry_at_unix_ms: body_rate_failure.attempts[1]
                .completed_at_unix_ms
                .saturating_add(i64::try_from(YAHOO_MISSING_RETRY_AFTER_COOLDOWN_FLOOR_MS,)?),
        }
    );
    let body_rate_rejected = body_rate_session
        .execute(plan.requests[0].clone(), body_rate_limits, &cancellation)
        .await
        .expect_err("the body-form rate-limit circuit must reject without another request");
    assert!(body_rate_rejected.attempts.is_empty());
    assert_eq!(body_rate_session.scripted_observed_targets().await.len(), 2);

    std::fs::remove_dir_all(state_root)?;
    Ok(())
}
