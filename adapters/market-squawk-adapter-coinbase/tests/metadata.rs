mod common;

use std::collections::BTreeSet;

use common::{TestResult, config, config_with_channels};
use market_squawk_adapter_coinbase::{
    COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT, CoinbaseChannel,
};
use market_squawk_domain::{ChecksumCapability, CoverageDelay, DataQuality, SequenceCapability};
use market_squawk_sources::AuthorizationMode;

#[test]
fn metadata_is_single_venue_realtime_partial_and_never_execution_quality() -> TestResult {
    let config = config()?;
    let metadata = config.metadata();

    assert_eq!(
        config.endpoint(),
        COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT
    );
    assert_eq!(
        metadata.authorization().mode(),
        AuthorizationMode::PublicInterface
    );
    assert_eq!(metadata.quality_ceiling(), DataQuality::DirectUnverified);
    assert!(metadata.coverage().topology().is_single_venue());
    assert!(!metadata.coverage().topology().is_consolidated());
    assert_eq!(metadata.coverage().delay(), CoverageDelay::RealTime);
    assert_eq!(
        metadata.capabilities().sequence(),
        SequenceCapability::Unsupported
    );
    assert_eq!(
        metadata.capabilities().checksum(),
        ChecksumCapability::Unsupported
    );
    assert!(
        metadata
            .network_policy()
            .authorize(COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT)
            .is_ok()
    );
    assert!(
        metadata
            .network_policy()
            .authorize("ws://127.0.0.1:9000")
            .is_err()
    );
    assert!(
        metadata
            .network_policy()
            .authorize("wss://ws-feed.exchange.coinbase.com")
            .is_err()
    );

    let channels = config.channels().iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(
        channels,
        BTreeSet::from([
            CoinbaseChannel::Level2,
            CoinbaseChannel::MarketTrades,
            CoinbaseChannel::Heartbeats,
        ])
    );
    Ok(())
}

#[test]
fn configuration_rejects_duplicate_or_incomplete_subscriptions() -> TestResult {
    assert!(
        config_with_channels(vec![CoinbaseChannel::Level2, CoinbaseChannel::Heartbeats,]).is_err()
    );
    assert!(
        config_with_channels(vec![
            CoinbaseChannel::Level2,
            CoinbaseChannel::MarketTrades,
            CoinbaseChannel::Heartbeats,
            CoinbaseChannel::Heartbeats,
        ],)
        .is_err()
    );
    Ok(())
}
