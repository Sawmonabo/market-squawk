use chrono::Utc;
use market_squawk::{
    Engine,
    domain::{BookChange, MarketEvent, PriceLevel, Side},
    quality::QualityState,
};
use rust_decimal::Decimal;

#[test]
fn delta_before_snapshot_is_quarantined() {
    let mut engine = Engine::new(5_000, false);
    let now = Utc::now();
    engine.handle(MarketEvent::BookDelta {
        source: "test".to_owned(),
        product: "BTC-USD".to_owned(),
        changes: vec![BookChange {
            side: Side::Buy,
            price: Decimal::from(100_u32),
            size: Decimal::ONE,
        }],
        exchange_at: Some(now),
        received_at: now,
    });

    let snapshot = engine.snapshot();
    let product = snapshot.products.get("BTC-USD").expect("product state");
    assert_eq!(product.quality.state, QualityState::Quarantined);
    assert!(product.features.is_none());
}

#[test]
fn source_disconnect_requires_a_fresh_snapshot() {
    let mut engine = Engine::new(5_000, false);
    let now = Utc::now();
    engine.handle(MarketEvent::BookSnapshot {
        source: "test".to_owned(),
        product: "BTC-USD".to_owned(),
        bids: vec![PriceLevel {
            price: Decimal::from(100_u32),
            size: Decimal::ONE,
        }],
        asks: vec![PriceLevel {
            price: Decimal::from(101_u32),
            size: Decimal::ONE,
        }],
        received_at: now,
    });
    engine.handle(MarketEvent::SourceStatus {
        source: "test".to_owned(),
        status: "disconnected".to_owned(),
        detail: None,
        received_at: now,
    });

    let disconnected = engine.snapshot();
    let product = disconnected.products.get("BTC-USD").expect("product state");
    assert_eq!(product.quality.state, QualityState::Quarantined);
    assert!(product.features.is_none());

    engine.handle(MarketEvent::BookSnapshot {
        source: "test".to_owned(),
        product: "BTC-USD".to_owned(),
        bids: vec![PriceLevel {
            price: Decimal::from(100_u32),
            size: Decimal::ONE,
        }],
        asks: vec![PriceLevel {
            price: Decimal::from(101_u32),
            size: Decimal::ONE,
        }],
        received_at: now,
    });

    let recovered = engine.snapshot();
    assert_eq!(
        recovered.products["BTC-USD"].quality.state,
        QualityState::Valid
    );
}
