use std::error::Error;
use std::time::{Duration, Instant};

use bytes::Bytes;
use market_squawk_domain::{MetadataRevision, SourceId, SourceIdentifier};
use market_squawk_platform::LocalPaths;
use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::http::ScriptedHttpResponse;
use crate::{
    AdapterBounds, AdmissionDecision, AdmissionPolicy, AttemptDisposition, AttemptKind,
    AttemptOutcome, ChartInterval, ChartWindow, CircuitSnapshot, ExplicitDemand,
    ExplicitDemandPurpose, ProviderField, YahooAdmission, YahooAssetClass, YahooAttemptTarget,
    YahooChartActionScope, YahooChartAdjustmentMode, YahooChartEventKind, YahooChartSessionScope,
    YahooClockObservation, YahooDurableStateStore, YahooExecutionDisposition, YahooExecutionLimits,
    YahooHttpFailureKind, YahooHttpSession, YahooHttpSessionConfig, YahooLocale,
    YahooParsedResponse, YahooProviderRecoveryDirective, YahooPublicationBinding,
    YahooPublicationBridgeError, YahooRequestPlanner, YahooRetryAfterDirective, YahooSymbol,
    YahooTarget,
};

fn clock(base: Instant, wall_unix_ms: i64, elapsed_ms: u64) -> YahooClockObservation {
    YahooClockObservation::new(wall_unix_ms, base + Duration::from_millis(elapsed_ms))
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
        max_attempt_receipts: 8,
        admission_policy: AdmissionPolicy::new(1_000, 250, 3)?,
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
                retry_after: &[],
                rate_limit_reset: &[],
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after: &[],
                rate_limit_reset: &[],
                body: Bytes::from_static(b"local-crumb"),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "application/json",
                retry_after: &[],
                rate_limit_reset: &[],
                body: chart.clone(),
            },
            ScriptedHttpResponse {
                status: 429,
                content_type: "text/plain",
                retry_after: &["0", "2", "bogus"],
                rate_limit_reset: &["+9", "5", "4", "1000000000000000"],
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
    let pending = first.into_pending_publication(publication_binding.clone())?;
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
    let seal_root = state_root.join("sealed-publication");
    let seal_paths = LocalPaths::prepare(&seal_root)?;
    let sealed =
        rejoin.try_rejoin(seal_request.seal(&seal_paths.sealed_research_journal_store()?)?)?;
    assert_eq!(
        sealed.family(),
        crate::YahooSealedPublicationFamily::HistoricalBars
    );
    let (_, token, sealed_raw, _, sealed_binding) = sealed.into_parts();
    let receipt = token.persisted_receipt();
    assert_eq!(
        sealed_raw.response_bytes.len(),
        usize::try_from(receipt.capture().total_body_bytes())?
    );
    assert_eq!(
        receipt.capture().dataset().as_str(),
        "yahoo-finance.experimental.chart-history"
    );
    assert_eq!(
        receipt.capture().pages()[0].body_bytes(),
        receipt.capture().total_body_bytes()
    );
    assert_eq!(sealed_binding, publication_binding);
    drop(token);

    session.fail_next_scripted_post_send_clock().await;
    let second = session
        .execute(plan.requests[1].clone(), limits, &cancellation)
        .await
        .expect_err("second ticker must exercise the mock 429");
    let YahooHttpFailureKind::CircuitOpen { retry_at_unix_ms } = &second.kind else {
        return Err(
            "429 with server recovery evidence must open the exact provider circuit".into(),
        );
    };
    assert_eq!(second.attempts.len(), 1);
    assert_eq!(second.attempts[0].status, Some(429));
    assert_eq!(second.attempts[0].response_bytes, 0);
    assert_eq!(
        second.attempts[0].disposition,
        AttemptDisposition::ProviderBackoff {
            status: 429,
            recovery: YahooProviderRecoveryDirective::try_new(
                Some(YahooRetryAfterDirective::DeltaSeconds { seconds: 2 }),
                Some(5),
            ),
        }
    );
    assert_eq!(
        *retry_at_unix_ms,
        second.attempts[0]
            .completed_at_unix_ms
            .checked_add(5_000)
            .ok_or("retry coordinate overflow")?
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
    assert!(matches!(
        cached.into_pending_publication(publication_binding.clone()),
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

    let mut repeated_headers = HeaderMap::new();
    for value in ["2", "bogus", "0", "7"] {
        repeated_headers.append(RETRY_AFTER, HeaderValue::from_static(value));
    }
    for value in ["+8", "5", "4", "1000000000000000"] {
        repeated_headers.append("ratelimit-reset", HeaderValue::from_static(value));
    }
    let repeated_recovery = crate::http::provider_recovery_from_headers(&repeated_headers)
        .ok_or("at least one repeated recovery field must be usable")?;
    assert_eq!(repeated_recovery.minimum_delay_ms(20_000), Some(7_000));
    let zero_or_malformed_recovery = crate::http::provider_recovery_from_values(
        ["0", "+3", "-1", "1.5"],
        ["0", "+4", "-2", "1.5", "1000000000000000"],
    )
    .ok_or("strictly valid zero values remain evidence but are not future deadlines")?;
    assert_eq!(zero_or_malformed_recovery.minimum_delay_ms(20_000), None);

    let clock_base = Instant::now();
    let absolute_policy = AdmissionPolicy::new(9_000, 1_000, 3)?;
    let absolute_retry = YahooAdmission::new(absolute_policy);
    let absolute_permit = match absolute_retry.admit(
        &plan.requests[0],
        "absolute-retry-after",
        AttemptKind::Primary,
        clock(clock_base, 10_000, 0),
    )? {
        AdmissionDecision::Execute(permit) => permit,
        _ => return Err("fresh absolute Retry-After admission must execute".into()),
    };
    absolute_permit.complete(
        AttemptOutcome {
            returned_units: 0,
            missing_units: 1,
            returned_records: 0,
            response_bytes: 0,
            latency_ms: 1,
            disposition: AttemptDisposition::ProviderBackoff {
                status: 429,
                recovery: YahooProviderRecoveryDirective::try_new(
                    Some(YahooRetryAfterDirective::HttpDate {
                        retry_at_unix_ms: 50_000,
                    }),
                    None,
                ),
            },
        },
        clock(clock_base, 20_000, 10),
    )?;
    assert_eq!(
        absolute_retry.snapshot()?.circuit,
        CircuitSnapshot::Open {
            recorded_at_unix_ms: 20_000,
            retry_at_unix_ms: 50_000
        }
    );
    let durable_absolute = absolute_retry.snapshot()?;
    assert!(matches!(
        absolute_retry.admit(
            &plan.requests[0],
            "wall-jump-must-not-short-circuit",
            AttemptKind::Primary,
            clock(clock_base, 500_000, 30_009),
        )?,
        AdmissionDecision::CircuitOpen { .. }
    ));
    let wall_rewind_probe = match absolute_retry.admit(
        &plan.requests[0],
        "wall-rewind-after-monotonic-deadline",
        AttemptKind::Primary,
        clock(clock_base, 1_000, 30_010),
    )? {
        AdmissionDecision::Execute(permit) => permit,
        _ => return Err("monotonic deadline must admit despite a backward wall step".into()),
    };
    drop(wall_rewind_probe);

    let restart_clock = Instant::now();
    let restored_absolute = YahooAdmission::try_restore(
        absolute_policy,
        durable_absolute,
        clock(restart_clock, 9_000_000, 0),
    )?;
    assert!(matches!(
        restored_absolute.admit(
            &plan.requests[0],
            "restart-discontinuity",
            AttemptKind::Primary,
            clock(restart_clock, 99_000_000, 29_999),
        )?,
        AdmissionDecision::CircuitOpen { .. }
    ));
    let restart_probe = match restored_absolute.admit(
        &plan.requests[0],
        "restart-discontinuity",
        AttemptKind::Primary,
        clock(restart_clock, 1, 30_000),
    )? {
        AdmissionDecision::Execute(permit) => permit,
        _ => return Err("restart must conservatively reapply exactly the durable interval".into()),
    };
    drop(restart_probe);

    let expired_clock = Instant::now();
    let expired_retry = YahooAdmission::new(AdmissionPolicy::new(9_000, 1_000, 3)?);
    let expired_permit = match expired_retry.admit(
        &plan.requests[0],
        "expired-retry-after",
        AttemptKind::Primary,
        clock(expired_clock, 10_000, 0),
    )? {
        AdmissionDecision::Execute(permit) => permit,
        _ => return Err("fresh expired Retry-After admission must execute".into()),
    };
    expired_permit.complete(
        AttemptOutcome {
            returned_units: 0,
            missing_units: 1,
            returned_records: 0,
            response_bytes: 0,
            latency_ms: 1,
            disposition: AttemptDisposition::ProviderBackoff {
                status: 429,
                recovery: Some(zero_or_malformed_recovery),
            },
        },
        clock(expired_clock, 20_000, 10),
    )?;
    let CircuitSnapshot::Open {
        recorded_at_unix_ms: 20_000,
        retry_at_unix_ms,
    } = expired_retry.snapshot()?.circuit
    else {
        return Err(
            "non-future provider instructions must use the bounded fallback circuit".into(),
        );
    };
    assert!((29_000..=30_000).contains(&retry_at_unix_ms));
    assert_eq!(expired_retry.snapshot()?.fallback_backoff_exponent, 1);
    let mut second_fallback_permit = match expired_retry.admit(
        &plan.requests[0],
        "expired-retry-after",
        AttemptKind::Primary,
        clock(expired_clock, retry_at_unix_ms, 20_000),
    )? {
        AdmissionDecision::Execute(permit) => permit,
        _ => return Err("expired fallback deadline must admit one half-open probe".into()),
    };
    second_fallback_permit.record_actual_attempt(
        AttemptKind::HalfOpenProbe,
        AttemptOutcome {
            returned_units: 0,
            missing_units: 1,
            returned_records: 0,
            response_bytes: 0,
            latency_ms: 1,
            disposition: AttemptDisposition::ProviderBackoff {
                status: 429,
                recovery: None,
            },
        },
        clock(expired_clock, retry_at_unix_ms + 1, 20_001),
    )?;
    second_fallback_permit.finish(false, clock(expired_clock, retry_at_unix_ms + 1, 20_001))?;
    let second_fallback = expired_retry.snapshot()?;
    let CircuitSnapshot::Open {
        recorded_at_unix_ms: second_recorded_at,
        retry_at_unix_ms: second_retry_at,
    } = second_fallback.circuit
    else {
        return Err("a repeated provider-silent backoff must keep the circuit open".into());
    };
    assert_eq!(second_recorded_at, retry_at_unix_ms + 1);
    assert!(((retry_at_unix_ms + 18_001)..=(retry_at_unix_ms + 19_001)).contains(&second_retry_at));
    assert_eq!(second_fallback.fallback_backoff_exponent, 2);

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
                retry_after: &[],
                rate_limit_reset: &[],
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after: &[],
                rate_limit_reset: &[],
                body: Bytes::from_static(b"bounded-crumb"),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "application/json",
                retry_after: &[],
                rate_limit_reset: &[],
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
            admission_policy: AdmissionPolicy::new(1_000, 250, 3)?,
            ..config
        },
        Url::parse("http://yahoo-fallback.test/")?,
        vec![
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after: &[],
                rate_limit_reset: &[],
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after: &[],
                rate_limit_reset: &[],
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/html",
                retry_after: &[],
                rate_limit_reset: &[],
                body: Bytes::from_static(
                    br#"<input name="csrfToken" value="csrf"><input name="sessionId" value="session">"#,
                ),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after: &[],
                rate_limit_reset: &[],
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after: &[],
                rate_limit_reset: &[],
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after: &[],
                rate_limit_reset: &[],
                body: Bytes::from_static(b"csrf-crumb"),
            },
            ScriptedHttpResponse {
                status: 500,
                content_type: "application/json",
                retry_after: &[],
                rate_limit_reset: &[],
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

    let redirect_session = YahooHttpSession::new_for_test_with_durable(
        config,
        Url::parse("http://yahoo-redirect.test/")?,
        vec![
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after: &[],
                rate_limit_reset: &[],
                body: Bytes::new(),
            },
            ScriptedHttpResponse {
                status: 200,
                content_type: "text/plain",
                retry_after: &[],
                rate_limit_reset: &[],
                body: Bytes::from_static(b"redirect-crumb"),
            },
            ScriptedHttpResponse {
                status: 302,
                content_type: "text/html",
                retry_after: &[],
                rate_limit_reset: &[],
                body: Bytes::from_static(b"redirect refused"),
            },
        ],
        None,
    )?;
    let redirect_failure = redirect_session
        .execute(plan.requests[0].clone(), limits, &cancellation)
        .await
        .expect_err("redirect response must fail closed without an implicit follow-up send");
    assert_eq!(
        redirect_failure.kind,
        YahooHttpFailureKind::ProviderStatus { status: 302 }
    );
    assert_eq!(redirect_failure.attempts.len(), 3);
    assert_eq!(redirect_session.scripted_observed_targets().await.len(), 3);
    assert_eq!(
        redirect_session
            .admission()
            .snapshot()?
            .actual_http_attempts_total,
        3
    );

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
    let identity_admission = YahooAdmission::new(AdmissionPolicy::new(1_000, 250, 3)?);
    let identity_clock = Instant::now();
    let first_permit = match identity_admission.admit(
        &plan.requests[0],
        &first_identity,
        AttemptKind::Primary,
        clock(identity_clock, 1_786_473_600_000, 0),
    )? {
        AdmissionDecision::Execute(permit) => permit,
        _ => return Err("first semantic identity must own the provider lane".into()),
    };
    assert!(matches!(
        identity_admission.admit(
            &same_url_different_semantics.requests[0],
            &second_identity,
            AttemptKind::Primary,
            clock(identity_clock, 1_786_473_600_001, 1),
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
    assert!(matches!(
        restored_cache.into_pending_publication(publication_binding),
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

    let cutover_root = state_root.join("cutover-authority");
    let cutover_owner = YahooHttpSession::new_for_test_with_durable(
        config,
        Url::parse("http://yahoo-cutover.test/")?,
        Vec::new(),
        Some(YahooDurableStateStore::try_open(&cutover_root)?),
    )?;
    let predecessor_generation = cutover_owner.clone();
    let candidate_generation = cutover_owner.clone();
    drop(cutover_owner);
    assert!(YahooDurableStateStore::try_open(&cutover_root).is_err());
    drop(predecessor_generation);
    assert!(YahooDurableStateStore::try_open(&cutover_root).is_err());
    drop(candidate_generation);
    drop(YahooDurableStateStore::try_open(&cutover_root)?);
    std::fs::remove_dir_all(state_root)?;
    Ok(())
}
