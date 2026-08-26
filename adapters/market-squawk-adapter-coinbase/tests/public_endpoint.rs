mod common;

use std::time::Duration;

use common::{TestResult, config, identifier};
use market_squawk_adapter_coinbase::{
    COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT, CoinbaseExchangeDecoder, CoinbaseExchangeSource,
    CoinbaseMarketDecodeOutcome,
};
use market_squawk_domain::{ConnectionGeneration, LiveEventClass, Timestamp};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, LiveMarketSource, RawMarketFrame, RawMarketSink, SessionId,
    SinkError, SourceError,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct SnapshotSink {
    snapshot: Option<RawMarketFrame>,
}

impl RawMarketSink for SnapshotSink {
    fn try_publish(&mut self, frame: RawMarketFrame) -> Result<(), SinkError> {
        let is_level_two = serde_json::from_slice::<serde_json::Value>(frame.payload())
            .ok()
            .and_then(|value| {
                value
                    .get("channel")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .is_some_and(|channel| channel == "l2_data");
        if is_level_two {
            self.snapshot = Some(frame);
            Err(SinkError::Closed)
        } else {
            Ok(())
        }
    }
}

#[test]
fn public_configuration_has_no_custom_endpoint_authority() -> TestResult {
    let config = config()?;
    assert_eq!(
        config.endpoint(),
        COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT
    );
    assert!(
        config
            .metadata()
            .network_policy()
            .authorize("ws://localhost:9000")
            .is_err()
    );
    assert!(
        config
            .metadata()
            .network_policy()
            .authorize("wss://example.com")
            .is_err()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit authorized external-network opt-in"]
async fn production_endpoint_smoke_is_opt_in_and_bounded() -> TestResult {
    if std::env::var("MARKET_SQUAWK_NETWORK_TESTS").as_deref() != Ok("1") {
        return Ok(());
    }
    let source_config = config()?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(
        source_config.metadata().clone(),
        Timestamp::from_unix_nanos(1),
    )?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(identifier("coinbase-public-smoke")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let capture = registry.take_capture_generation_capabilities(&session)?;
    let (mut initialization, _admission, _degradation) = capture.into_parts();
    initialization.mark_healthy()?;
    let generation = registry.take_live_source_generation(&session)?;
    let mut source = CoinbaseExchangeSource::try_new(source_config, generation)?;
    let mut sink = SnapshotSink { snapshot: None };
    let cancellation = CancellationToken::new();
    let timed = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        timed.cancel();
    });
    let outcome = source.run(&mut sink, cancellation).await;
    assert_eq!(outcome, Err(SourceError::Sink(SinkError::Closed)));
    let snapshot = sink
        .snapshot
        .ok_or("Coinbase did not publish a bounded level2 frame")?;
    let validated = session.validate_live_frame(&snapshot)?;
    let decoder_config = config()?;
    let mut decoder = CoinbaseExchangeDecoder::try_new(&decoder_config)?;
    let decoded = decoder.decode_market_handoff(&validated)?;
    let CoinbaseMarketDecodeOutcome::Market(handoff) = decoded else {
        return Err("Coinbase's live level2 frame did not decode as a market handoff".into());
    };
    let (_evidence, _raw_lineage, batch) = handoff.into_parts();
    assert!(
        batch
            .observations()
            .iter()
            .any(|observation| observation.event_class() == LiveEventClass::BookSnapshot)
    );
    let retained = batch.retained_bytes()?;
    let runtime_ceiling = decoder_config.transport_limits().max_frame_bytes();
    if retained > runtime_ceiling {
        return Err(format!(
            "decoded live snapshot retains {retained} bytes, above the configured runtime message ceiling {runtime_ceiling}"
        )
        .into());
    }
    Ok(())
}
