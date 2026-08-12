use std::error::Error;
use std::time::{Duration, Instant};

use bytes::Bytes;
use market_squawk_domain::{MetadataRevision, SourceId, SourceIdentifier};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::http::ScriptedHttpResponse;
use crate::{
    AdapterBounds, AdmissionDecision, AdmissionPolicy, AttemptDisposition, AttemptKind,
    AttemptOutcome, ChartInterval, ChartWindow, CircuitSnapshot, ExplicitDemand,
    ExplicitDemandPurpose, ParseContext, ProviderField, YAHOO_FINANCE_EXPERIMENTAL, YahooAdmission,
    YahooAssetClass, YahooAttemptTarget, YahooEnrichmentState, YahooExecutionDisposition,
    YahooExecutionLimits, YahooHttpFailureKind, YahooHttpSession, YahooHttpSessionConfig,
    YahooLocale, YahooPublicationBinding, YahooPublicationBridgeError, YahooRequestPlanner,
    YahooSymbol, YahooTarget, parse_quote_response,
};

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

#[test]
fn symbol_breadth_is_application_bounded_not_a_disproven_twenty_five_provider_limit()
-> Result<(), Box<dyn Error>> {
    let planner = planner(40)?;
    let targets = (0..26)
        .map(|index| target(format!("T{index}")))
        .collect::<Result<Vec<_>, _>>()?;

    let quote = planner.quote(demand("quote-26")?, targets.clone())?;
    assert_eq!(quote.requests.len(), 1);
    assert_eq!(quote.requests[0].requested_symbol_count(), 26);

    let history = planner.chart_history(
        demand("history-26")?,
        targets,
        ChartWindow::OneMonth,
        ChartInterval::OneDay,
        false,
    )?;
    assert!(history.history_fans_out_per_ticker);
    assert_eq!(history.actual_primary_attempt_units(), 26);
    assert!(
        history
            .requests
            .iter()
            .all(|request| request.requested_symbol_count() == 1)
    );
    Ok(())
}

#[test]
fn per_ticker_attempts_share_actual_telemetry_and_one_provider_wide_429_circuit()
-> Result<(), Box<dyn Error>> {
    let planner = planner(4)?;
    let plan = planner.chart_history(
        demand("two-ticker-history")?,
        vec![target("AAPL".to_owned())?, target("MSFT".to_owned())?],
        ChartWindow::FiveDays,
        ChartInterval::OneDay,
        false,
    )?;
    let admission = YahooAdmission::new(AdmissionPolicy::new(1_000, 2)?);
    let shared = admission.clone();

    let first = match admission.admit(&plan.requests[0], AttemptKind::Primary, 10_000)? {
        AdmissionDecision::Execute(permit) => permit,
        decision => return Err(format!("unexpected first admission: {decision:?}").into()),
    };
    first.complete(
        AttemptOutcome {
            returned_units: 1,
            missing_units: 0,
            returned_records: 5,
            response_bytes: 1_024,
            latency_ms: 80,
            disposition: AttemptDisposition::Success,
        },
        10_080,
    )?;

    let second = match shared.admit(&plan.requests[1], AttemptKind::Primary, 10_100)? {
        AdmissionDecision::Execute(permit) => permit,
        decision => return Err(format!("unexpected second admission: {decision:?}").into()),
    };
    second.complete(
        AttemptOutcome {
            returned_units: 0,
            missing_units: 1,
            returned_records: 0,
            response_bytes: 96,
            latency_ms: 45,
            disposition: AttemptDisposition::Http429 {
                retry_after_ms: Some(2_000),
            },
        },
        10_145,
    )?;

    assert!(matches!(
        admission.admit(&plan.requests[0], AttemptKind::Primary, 11_000)?,
        AdmissionDecision::CircuitOpen {
            retry_at_unix_ms: 12_145
        }
    ));
    let snapshot = admission.snapshot()?;
    assert_eq!(snapshot.actual_http_attempts_total, 2);
    assert_eq!(snapshot.requested_units_total, 2);
    assert_eq!(snapshot.returned_units_total, 1);
    assert_eq!(snapshot.missing_units_total, 1);
    assert_eq!(snapshot.http_429_total, 1);
    assert_eq!(snapshot.maximum_observed_response_bytes, 1_024);
    assert_eq!(snapshot.maximum_observed_latency_ms, 80);
    assert_eq!(
        snapshot.circuit,
        CircuitSnapshot::Open {
            retry_at_unix_ms: 12_145
        }
    );
    Ok(())
}

#[test]
fn quote_parser_retains_provider_market_delay_clocks_and_supplement_only_authority()
-> Result<(), Box<dyn Error>> {
    let planner = planner(4)?;
    let request = planner
        .quote(
            demand("quote-provenance")?,
            vec![target("AAPL".to_owned())?, target("MSFT".to_owned())?],
        )?
        .requests
        .into_iter()
        .next()
        .ok_or("quote planner returned no request")?;
    let fixture = serde_json::to_vec(&json!({
        "quoteResponse": {
            "error": null,
            "result": [{
                "symbol": "AAPL",
                "quoteType": "EQUITY",
                "currency": "USD",
                "exchange": "NMS",
                "fullExchangeName": "NasdaqGS",
                "market": "us_market",
                "region": "US",
                "exchangeTimezoneName": "America/New_York",
                "exchangeDataDelayedBy": 0,
                "marketState": "REGULAR",
                "regularMarketTime": 1_786_473_650,
                "regularMarketPrice": 201.25,
                "bid": 201.20,
                "bidSize": 8,
                "ask": 201.30,
                "askSize": 7,
                "shortName": "Apple Inc."
            }]
        }
    }))?;
    let parsed = parse_quote_response(
        &request,
        &ParseContext {
            received_at_unix_ms: 1_786_473_650_125,
            available_at_unix_ms: 1_786_473_650_140,
        },
        bounds(4),
        &fixture,
    )?;

    assert_eq!(parsed.provider_returned_symbols.len(), 1);
    assert_eq!(parsed.missing_symbols.len(), 1);
    assert_eq!(parsed.valid_observations, 1);
    let apple = parsed
        .observations
        .iter()
        .find(|observation| observation.data.is_some())
        .ok_or("valid quote observation missing")?;
    assert_eq!(apple.state, YahooEnrichmentState::Experimental);
    assert!(!apple.governed_override_permitted());
    assert_eq!(apple.provenance.provider, YAHOO_FINANCE_EXPERIMENTAL);
    assert_eq!(
        apple.provenance.exchange,
        ProviderField::Value("NMS".to_owned())
    );
    assert_eq!(
        apple.provenance.full_exchange_name,
        ProviderField::Value("NasdaqGS".to_owned())
    );
    assert_eq!(
        apple.provenance.market,
        ProviderField::Value("us_market".to_owned())
    );
    assert_eq!(
        apple.provenance.country,
        ProviderField::Value("US".to_owned())
    );
    assert_eq!(
        apple.provenance.exchange_delay_seconds,
        ProviderField::Value(0)
    );
    assert_eq!(
        apple.provenance.provider_event_time_unix_seconds,
        ProviderField::Value(1_786_473_650)
    );
    assert_eq!(apple.provenance.received_at_unix_ms, 1_786_473_650_125);
    assert_eq!(apple.provenance.available_at_unix_ms, 1_786_473_650_140);
    Ok(())
}

#[tokio::test]
async fn local_http_proves_cookie_crumb_fan_out_attempts_and_immediate_429_circuit()
-> Result<(), Box<dyn Error>> {
    let chart = Bytes::from_static(
        br#"{"chart":{"error":null,"result":[{"meta":{"symbol":"AAPL","instrumentType":"EQUITY","currency":"USD","exchangeName":"NMS","fullExchangeName":"NasdaqGS","market":"us_market","country":"US","exchangeTimezoneName":"America/New_York","exchangeDataDelayedBy":0,"regularMarketTime":1786473650,"dataGranularity":"1d","range":"5d"},"timestamp":[1786473600],"indicators":{"quote":[{"open":[201.0],"high":[202.0],"low":[200.0],"close":[201.5],"volume":[100]}],"adjclose":[{"adjclose":[201.5]}]}}]}}"#,
    );
    let session = YahooHttpSession::new_for_test(
        YahooHttpSessionConfig {
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
            admission_policy: AdmissionPolicy::new(1_000, 3)?,
        },
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
                body: chart,
            },
            ScriptedHttpResponse {
                status: 429,
                content_type: "text/plain",
                retry_after_ms: Some(2_000),
                body: Bytes::from_static(b"Too Many Requests"),
            },
        ],
    )?;
    let plan = planner(4)?.chart_history(
        demand("http-fan-out")?,
        vec![target("AAPL".to_owned())?, target("MSFT".to_owned())?],
        ChartWindow::FiveDays,
        ChartInterval::OneDay,
        false,
    )?;
    let limits = YahooExecutionLimits {
        deadline: Instant::now() + Duration::from_secs(5),
        maximum_cache_age: Duration::ZERO,
    };
    let cancellation = CancellationToken::new();
    let first = session
        .execute(plan.requests[0].clone(), limits, &cancellation)
        .await?;
    assert_eq!(first.raw.attempts.len(), 3);
    assert_eq!(
        first.raw.attempts[0].target,
        YahooAttemptTarget::CookieBootstrap
    );
    assert_eq!(first.raw.attempts[1].target, YahooAttemptTarget::BasicCrumb);
    assert_eq!(
        first.raw.attempts[2].target,
        YahooAttemptTarget::Data(crate::YahooRequestFamily::ChartHistory)
    );
    let publication_binding = YahooPublicationBinding::new(
        SourceId::try_from("yahoo-finance-experimental")?,
        MetadataRevision::new(SourceIdentifier::try_from("rev-3")?),
        Uuid::from_u128(1),
        Uuid::from_u128(2),
    );
    let material = first.publication_material(publication_binding.clone())?;
    assert_eq!(material.records().len(), 1);
    assert_eq!(material.records()[0].payload(), first.raw.response_bytes);
    assert_eq!(material.records()[0].source_sequence(), Some(0));
    assert_eq!(
        material.receipt().dataset().as_str(),
        "yahoo-finance.experimental.chart-history"
    );
    assert_eq!(
        material.receipt().pages()[0].body_bytes(),
        u64::try_from(first.raw.response_bytes.len())?
    );

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
    assert_eq!(cached.disposition, YahooExecutionDisposition::CacheHit);
    assert!(matches!(
        cached.publication_material(publication_binding),
        Err(YahooPublicationBridgeError::NonPublicationResult)
    ));

    let requests = session.scripted_observed_targets().await;
    assert_eq!(requests.len(), 4);
    assert!(requests[2].contains("crumb=local-crumb"));
    assert!(requests[3].contains("/v8/finance/chart/MSFT?"));
    let snapshot = session.admission().snapshot()?;
    assert_eq!(snapshot.logical_primary_operations_total, 2);
    assert_eq!(snapshot.actual_http_attempts_total, 4);
    assert_eq!(snapshot.requested_units_total, 2);
    assert_eq!(snapshot.returned_units_total, 1);
    assert_eq!(snapshot.missing_units_total, 1);
    assert_eq!(snapshot.http_429_total, 1);
    Ok(())
}
