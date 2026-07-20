mod common;

use std::time::Duration;

use common::{TestResult, config, identifier};
use market_squawk_adapter_coinbase::{COINBASE_EXCHANGE_ENDPOINT, CoinbaseExchangeSource};
use market_squawk_domain::{ConnectionGeneration, Timestamp};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, LiveMarketSource, RawMarketSink, SessionId, SinkError, SourceError,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct RejectingSink;

impl RawMarketSink for RejectingSink {
    fn try_publish(
        &mut self,
        _frame: market_squawk_sources::RawMarketFrame,
    ) -> Result<(), SinkError> {
        Err(SinkError::Closed)
    }
}

#[test]
fn public_configuration_has_no_custom_endpoint_authority() -> TestResult {
    let config = config()?;
    assert_eq!(config.endpoint(), COINBASE_EXCHANGE_ENDPOINT);
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
    let config = config()?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(config.metadata().clone(), Timestamp::from_unix_nanos(1))?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(identifier("coinbase-public-smoke")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let mut source = CoinbaseExchangeSource::try_new(config, &session)?;
    let cancellation = CancellationToken::new();
    let timed = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        timed.cancel();
    });
    let outcome = source
        .run(&mut frames, &mut RejectingSink, cancellation)
        .await;
    assert!(matches!(
        outcome,
        Err(SourceError::Cancelled
            | SourceError::ProviderUnavailable
            | SourceError::Network
            | SourceError::Sink(_))
    ));
    Ok(())
}
