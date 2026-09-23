use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    ChecksumCapability, ChecksumEvidence, ChecksumScope, ChecksumValue, ConnectionGeneration,
    DataQuality, InstrumentId, IntegrityRule, MarketDepth, PriceTicks, QuantityLots, RuleVersion,
    SequenceEvidence, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::MarketFreshness;

use super::{
    OrderLevelBatch, OrderLevelBatchInput, OrderLevelBatchPayload, OrderLevelBook,
    OrderLevelBookError, OrderLevelDeleteQuantity, OrderLevelEvent, OrderLevelLimits,
    OrderLevelOperation, OrderLevelPhase, OrderLevelQuarantineReason, OrderLevelRoute,
    OrderLevelVisibleOrder, UnknownOrderDisposition,
};
use crate::{BookSide, DepthLimit};

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn duplicate_price_orders_survive_and_failed_checksum_cannot_publish() -> TestResult {
    let generation = ConnectionGeneration::new(7)?;
    let route = OrderLevelRoute::new(
        SourceId::try_from("kraken-authenticated-level3")?,
        VenueId::try_from("kraken")?,
        InstrumentId::from_str("864ee2b4-e1c6-4de0-b37e-1da522505f6c")?,
        SourceIdentifier::try_from("BTC/USD")?,
        generation,
    );
    let limits = OrderLevelLimits::new(16, DepthLimit::new(8)?)?;
    let mut book = OrderLevelBook::try_new(route.clone(), limits)?;
    let snapshot_time = Timestamp::from_unix_nanos(1_000);
    let snapshot_available_at = Timestamp::from_unix_nanos(1_100);
    let snapshot = OrderLevelBatch::try_new(OrderLevelBatchInput::new(
        route.clone(),
        SourceIdentifier::try_from("kraken-level3-snapshot-1")?,
        snapshot_time,
        snapshot_time,
        snapshot_available_at,
        DataQuality::DirectUnverified,
        MarketFreshness::Fresh {
            last_market_at: snapshot_time,
        },
        None,
        SequenceEvidence::unsupported(generation),
        checksum(generation, 41, 41)?,
        Some(1),
        OrderLevelBatchPayload::Snapshot {
            snapshot_source_timestamp: snapshot_time,
            snapshot_received_at: snapshot_time,
            orders: visible_orders()?,
            replay: Vec::new(),
        },
    ))?;
    book.apply(snapshot)?;

    assert_eq!(book.orders().len(), 4);
    let first = book.project_price_levels()?;
    assert_eq!(first.bids().len(), 2);
    assert_eq!(first.bids()[0].price(), PriceTicks::new(100));
    assert_eq!(first.bids()[0].quantity(), QuantityLots::new(7)?);
    assert_eq!(first.bids()[0].order_count(), 2);
    assert_eq!(first.asks()[0].price(), PriceTicks::new(101));
    assert_eq!(first.available_at(), snapshot_available_at);
    assert_eq!(first, book.project_price_levels()?);

    let committed_orders = book.orders().to_vec();
    let update_time = Timestamp::from_unix_nanos(2_000);
    let update_available_at = Timestamp::from_unix_nanos(2_100);
    let delete = OrderLevelOperation::Done {
        order_id: SourceIdentifier::try_from("bid-a")?,
        side: Some(BookSide::Bid),
        price: Some(PriceTicks::new(100)),
        quantity: OrderLevelDeleteQuantity::ZeroMeansDelete,
        provider_order_timestamp: Some(update_time),
        unknown_order: UnknownOrderDisposition::Reject,
    };
    let update = OrderLevelBatch::try_new(OrderLevelBatchInput::new(
        route,
        SourceIdentifier::try_from("kraken-level3-update-2")?,
        update_time,
        update_time,
        update_available_at,
        DataQuality::DirectUnverified,
        MarketFreshness::Fresh {
            last_market_at: update_time,
        },
        None,
        SequenceEvidence::unsupported(generation),
        checksum(generation, 42, 99)?,
        Some(2),
        OrderLevelBatchPayload::Update {
            events: vec![OrderLevelEvent::try_new(
                None,
                Some(2),
                update_time,
                update_time,
                vec![delete],
            )?],
        },
    ))?;
    assert_eq!(
        book.apply(update),
        Err(OrderLevelBookError::ChecksumIntegrity)
    );
    assert_eq!(book.orders(), committed_orders);
    assert_eq!(
        book.phase(),
        OrderLevelPhase::Quarantined(OrderLevelQuarantineReason::Checksum)
    );
    let isolated = book.project_price_levels()?;
    assert_eq!(isolated.quality(), DataQuality::Quarantined);
    assert_eq!(isolated.available_at(), snapshot_available_at);
    assert_eq!(isolated.bids()[0].quantity(), QuantityLots::new(7)?);
    assert_eq!(isolated.bids()[0].order_count(), 2);
    Ok(())
}

fn visible_orders() -> Result<Vec<OrderLevelVisibleOrder>, Box<dyn Error>> {
    Ok(vec![
        visible("bid-a", BookSide::Bid, 100, 3)?,
        visible("bid-b", BookSide::Bid, 100, 4)?,
        visible("bid-c", BookSide::Bid, 99, 2)?,
        visible("ask-a", BookSide::Ask, 101, 5)?,
    ])
}

fn visible(
    order_id: &str,
    side: BookSide,
    price: i64,
    quantity: i64,
) -> Result<OrderLevelVisibleOrder, Box<dyn Error>> {
    Ok(OrderLevelVisibleOrder::new(
        SourceIdentifier::try_from(order_id)?,
        side,
        PriceTicks::new(price),
        QuantityLots::new(quantity)?,
        None,
        None,
    )?)
}

fn checksum(
    generation: ConnectionGeneration,
    expected: u64,
    computed: u64,
) -> Result<ChecksumEvidence, Box<dyn Error>> {
    Ok(ChecksumEvidence::validate_book(
        ChecksumCapability::Provided,
        Some(IntegrityRule::new(
            SourceIdentifier::try_from("kraken-level3-crc32-v1")?,
            RuleVersion::new(1)?,
        )),
        generation,
        Some(ChecksumScope::new(
            MarketDepth::OrderLevel,
            10,
            SourceIdentifier::try_from("kraken-level3-top-10-price-levels")?,
        )?),
        Some(ChecksumValue::new(expected)),
        Some(ChecksumValue::new(computed)),
    )?)
}
