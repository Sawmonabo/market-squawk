use std::num::{NonZeroU32, NonZeroUsize};
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::time::Duration;

use market_squawk_analytics::{ExactFeatureRatio, FeatureValidity};
use market_squawk_domain::{
    AssetClass, ConnectionGeneration, Currency, Denomination, InstrumentDefinition,
    InstrumentDefinitionInput, InstrumentId, LotSize, TickSize, Timestamp, TradingStatus, VenueId,
    VenueMapping, VenueSymbol,
};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

use super::{CrossVenueRuntimeError, CrossVenueRuntimeReader, create_cross_venue_plane};
use crate::runtime::LiveFeatureCapacity;
use crate::{DepthLimit, LiveRouteConfig, LiveRouteConfigInput, ShardKey};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn worker_fails_closed_on_regression_then_recovers_and_rechecks_staleness() -> TestResult {
    let instrument = InstrumentId::from_str("00000000-0000-0000-0000-000000000009")?;
    let coinbase = route(instrument, "coinbase")?;
    let kraken = route(instrument, "kraken")?;
    let cancellation = CancellationToken::new();
    let (plane, worker) = create_cross_venue_plane(
        &[coinbase.clone(), kraken.clone()],
        capacity(8)?,
        cancellation.child_token(),
    )?;
    let (coinbase_publisher, reader) = plane.route(coinbase.route()).ok_or("missing route")?;
    let (kraken_publisher, _) = plane.route(kraken.route()).ok_or("missing route")?;
    let task = tokio::spawn(worker.ok_or("missing cross-venue worker")?.run());

    coinbase_publisher.try_publish(generation(2)?, ratio(200)?, timestamp(20))?;
    kraken_publisher.try_publish(generation(2)?, ratio(204)?, timestamp(20))?;
    wait_for(&reader, instrument, timestamp(20), FeatureValidity::Ready).await?;

    coinbase_publisher.try_publish(generation(1)?, ratio(202)?, timestamp(21))?;
    kraken_publisher.try_publish(generation(2)?, ratio(206)?, timestamp(21))?;
    wait_for(
        &reader,
        instrument,
        timestamp(21),
        FeatureValidity::Unavailable,
    )
    .await?;

    coinbase_publisher.try_publish(generation(2)?, ratio(202)?, timestamp(22))?;
    kraken_publisher.try_publish(generation(2)?, ratio(206)?, timestamp(22))?;
    wait_for(&reader, instrument, timestamp(22), FeatureValidity::Ready).await?;
    assert_eq!(
        reader
            .load(instrument, timestamp(1_000_000_023))?
            .validity(),
        FeatureValidity::Stale
    );

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(1), task).await??;
    Ok(())
}

#[tokio::test]
async fn dropped_admission_cannot_be_cleared_by_older_queued_updates() -> TestResult {
    let instrument = InstrumentId::from_str("00000000-0000-0000-0000-000000000010")?;
    let coinbase = route(instrument, "coinbase")?;
    let kraken = route(instrument, "kraken")?;
    let cancellation = CancellationToken::new();
    let (plane, worker) = create_cross_venue_plane(
        &[coinbase.clone(), kraken.clone()],
        capacity(2)?,
        cancellation.child_token(),
    )?;
    let (coinbase_publisher, reader) = plane.route(coinbase.route()).ok_or("missing route")?;
    let (kraken_publisher, _) = plane.route(kraken.route()).ok_or("missing route")?;
    coinbase_publisher.try_publish(generation(1)?, ratio(200)?, timestamp(20))?;
    kraken_publisher.try_publish(generation(1)?, ratio(204)?, timestamp(20))?;
    assert_eq!(
        coinbase_publisher.try_publish(generation(1)?, ratio(202)?, timestamp(21)),
        Err(CrossVenueRuntimeError::CountCapacityFull)
    );

    let task = tokio::spawn(worker.ok_or("missing cross-venue worker")?.run());
    wait_until_current_epoch_published(&reader, instrument).await?;
    assert_eq!(
        reader.load(instrument, timestamp(20))?.validity(),
        FeatureValidity::Unavailable
    );

    coinbase_publisher.try_publish(generation(1)?, ratio(202)?, timestamp(21))?;
    kraken_publisher.try_publish(generation(1)?, ratio(206)?, timestamp(21))?;
    wait_for(&reader, instrument, timestamp(21), FeatureValidity::Ready).await?;
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(1), task).await??;
    Ok(())
}

async fn wait_for(
    reader: &CrossVenueRuntimeReader,
    instrument: InstrumentId,
    observed_at: Timestamp,
    expected: FeatureValidity,
) -> TestResult {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if reader.load(instrument, observed_at)?.validity() == expected {
                return TestResult::Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

async fn wait_until_current_epoch_published(
    reader: &CrossVenueRuntimeReader,
    instrument: InstrumentId,
) -> TestResult {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let published = reader
                .instruments
                .get(&instrument)
                .ok_or("missing instrument")?;
            if published.admission_epoch.load(Ordering::Acquire) > 1
                && published.published_epoch.load(Ordering::Acquire)
                    == published.admission_epoch.load(Ordering::Acquire)
            {
                return TestResult::Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

fn capacity(command_count: usize) -> TestResult<LiveFeatureCapacity> {
    Ok(LiveFeatureCapacity {
        maximum_feature_window_observations_per_route: nonzero(8)?,
        maximum_feature_window_bytes_per_route: nonzero(1_048_576)?,
        maximum_feature_sets_per_route: nonzero(4)?,
        cross_venue_command_count: nonzero(command_count)?,
        cross_venue_command_bytes: NonZeroU32::new(65_536).ok_or("zero bytes")?,
        maximum_cross_venue_instruments: nonzero(4)?,
        maximum_venues_per_cross_venue_instrument: nonzero(2)?,
        maximum_feature_snapshot_bytes: NonZeroU32::new(65_536).ok_or("zero bytes")?,
        maximum_action_hook_bytes_per_route: nonzero(65_536)?,
    })
}

fn route(instrument: InstrumentId, venue: &str) -> TestResult<LiveRouteConfig> {
    let venue = VenueId::try_from(venue)?;
    let definition = InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: instrument,
        definition_revision: market_squawk_domain::InstrumentDefinitionRevision::try_from(1_u64)?,
        asset_class: AssetClass::Crypto,
        primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
        quote_currency: Currency::try_from("USD")?,
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::new(1, 2))?,
        contract_multiplier: Decimal::ONE,
        venue_mappings: vec![VenueMapping::new(
            venue.clone(),
            VenueSymbol::try_from("BTC-USD")?,
        )],
        provider_identities: Vec::new(),
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })?;
    Ok(LiveRouteConfig::try_new(LiveRouteConfigInput {
        route: ShardKey::new(venue, instrument),
        definition,
        depth: DepthLimit::new(4)?,
        nonce_capacity: 4,
        nonce_reclaim_budget: 1,
        maximum_capability_lifetime: Duration::from_secs(1),
    })?)
}

fn nonzero(value: usize) -> TestResult<NonZeroUsize> {
    Ok(NonZeroUsize::new(value).ok_or("zero capacity")?)
}

fn generation(value: u64) -> TestResult<ConnectionGeneration> {
    Ok(ConnectionGeneration::new(value)?)
}

fn ratio(numerator: i128) -> TestResult<ExactFeatureRatio> {
    Ok(ExactFeatureRatio::try_new(numerator, 2)?)
}

const fn timestamp(value: i64) -> Timestamp {
    Timestamp::from_unix_nanos(value)
}
