use std::error::Error;
use std::time::{Duration, Instant};

use bytes::Bytes;
use market_squawk_domain::{MetadataRevision, SourceId, SourceIdentifier};
use market_squawk_platform::LocalPaths;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::http::ScriptedHttpResponse;
use crate::{
    AdapterBounds, AdmissionPolicy, ChartInterval, ChartWindow, ExplicitDemand,
    ExplicitDemandPurpose, YahooAssetClass, YahooAttemptTarget, YahooDurableStateStore,
    YahooExecutionDisposition, YahooExecutionLimits, YahooHttpFailureKind, YahooHttpSession,
    YahooHttpSessionConfig, YahooLocale, YahooParsedResponse, YahooPublicationBinding,
    YahooPublicationBridgeError, YahooRequestPlanner, YahooSymbol, YahooTarget,
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

#[tokio::test]
async fn explicit_demand_network_response_crosses_one_pending_publication_handoff()
-> Result<(), Box<dyn Error>> {
    let chart = Bytes::from_static(
        br#"{"chart":{"error":null,"result":[{"meta":{"symbol":"AAPL","instrumentType":"EQUITY","currency":"USD","exchangeName":"NMS","fullExchangeName":"NasdaqGS","market":"us_market","country":"US","exchangeTimezoneName":"America/New_York","exchangeDataDelayedBy":0,"regularMarketTime":1786473650,"dataGranularity":"1d","range":"5d"},"timestamp":[1786473600],"indicators":{"quote":[{"open":[201.0],"high":[202.0],"low":[200.0],"close":[201.5],"volume":[100]}],"adjclose":[{"adjclose":[201.5]}]}}]}}"#,
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
        admission_policy: AdmissionPolicy::new(1_000, 3)?,
    };
    let state_root = std::env::temp_dir().join(format!(
        "market-squawk-yahoo-durable-test-{}",
        Uuid::new_v4()
    ));
    let journal_root = std::env::temp_dir().join(format!(
        "market-squawk-yahoo-journal-test-{}",
        Uuid::new_v4()
    ));
    let journal_paths = LocalPaths::prepare(&journal_root)?;
    let journal_store = journal_paths.sealed_research_journal_store()?;
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
                body: chart,
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
    let (rejoin, material) = pending.into_sealing_parts();
    assert_eq!(material.records().len(), 1);
    assert_eq!(
        material.records()[0].payload().len(),
        usize::try_from(material.receipt().total_body_bytes())?
    );
    assert_eq!(material.records()[0].source_sequence(), Some(0));
    assert_eq!(
        material.receipt().dataset().as_str(),
        "yahoo-finance.experimental.chart-history"
    );
    assert_eq!(
        material.receipt().pages()[0].body_bytes(),
        material.receipt().total_body_bytes()
    );
    let sealed_capture = material.seal(&journal_store)?;
    assert_eq!(
        sealed_capture.capture().dataset().as_str(),
        "yahoo-finance.experimental.chart-history"
    );
    drop(rejoin);

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
    assert_eq!(cached.disposition(), YahooExecutionDisposition::CacheHit);
    assert!(matches!(
        cached.into_pending_publication(publication_binding.clone()),
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
    drop(sealed_capture);
    drop(journal_store);
    drop(journal_paths);
    std::fs::remove_dir_all(journal_root)?;
    std::fs::remove_dir_all(state_root)?;
    Ok(())
}
