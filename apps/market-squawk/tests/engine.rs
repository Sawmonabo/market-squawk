use anyhow::{Context, Result};
use chrono::Utc;
use market_squawk::{
    DiagnosticBookChange, DiagnosticEngine, DiagnosticMarketEvent, DiagnosticPriceLevel,
    DiagnosticSide, quality::QualityState,
};
use rust_decimal::Decimal;

#[test]
fn diagnostic_delta_before_snapshot_is_quarantined() -> Result<()> {
    let mut engine = DiagnosticEngine::new(5_000, false);
    let now = Utc::now();
    engine.handle(DiagnosticMarketEvent::BookDelta {
        source: "test".to_owned(),
        product: "BTC-USD".to_owned(),
        changes: vec![DiagnosticBookChange {
            side: DiagnosticSide::Buy,
            price: Decimal::from(100_u32),
            size: Decimal::ONE,
        }],
        exchange_at: Some(now),
        received_at: now,
    });

    let snapshot = engine.snapshot();
    let product = snapshot.products.get("BTC-USD").context("product state")?;
    assert_eq!(product.quality.state, QualityState::Quarantined);
    assert!(product.features.is_none());
    assert_eq!(snapshot.processed_events, 1);
    Ok(())
}

#[test]
fn diagnostic_disconnect_requires_a_fresh_snapshot() -> Result<()> {
    let mut engine = DiagnosticEngine::new(5_000, false);
    let now = Utc::now();
    engine.handle(DiagnosticMarketEvent::BookSnapshot {
        source: "test".to_owned(),
        product: "BTC-USD".to_owned(),
        bids: vec![DiagnosticPriceLevel {
            price: Decimal::from(100_u32),
            size: Decimal::ONE,
        }],
        asks: vec![DiagnosticPriceLevel {
            price: Decimal::from(101_u32),
            size: Decimal::ONE,
        }],
        received_at: now,
    });
    engine.handle(DiagnosticMarketEvent::SourceStatus {
        source: "test".to_owned(),
        status: "disconnected".to_owned(),
        detail: None,
        received_at: now,
    });

    let disconnected = engine.snapshot();
    let product = disconnected
        .products
        .get("BTC-USD")
        .context("product state")?;
    assert_eq!(product.quality.state, QualityState::Quarantined);
    assert!(product.features.is_none());

    engine.handle(DiagnosticMarketEvent::BookSnapshot {
        source: "test".to_owned(),
        product: "BTC-USD".to_owned(),
        bids: vec![DiagnosticPriceLevel {
            price: Decimal::from(100_u32),
            size: Decimal::ONE,
        }],
        asks: vec![DiagnosticPriceLevel {
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
    assert_eq!(recovered.processed_events, 3);
    Ok(())
}
