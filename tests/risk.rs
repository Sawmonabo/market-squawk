use chrono::{Duration, Utc};
use market_squawk::{
    bot::OrderIntent,
    domain::Side,
    quality::FeedQuality,
    risk::{RiskDecision, RiskKernel, RiskLimits},
};
use rust_decimal::Decimal;
use uuid::Uuid;

fn intent() -> OrderIntent {
    OrderIntent {
        intent_id: Uuid::new_v4(),
        strategy: "test".to_owned(),
        product: "BTC-USD".to_owned(),
        side: Side::Buy,
        quantity: Decimal::new(1, 2),
        limit_price: Decimal::from(100_u32),
        created_at: Utc::now(),
        reason: "test signal".to_owned(),
    }
}

#[test]
fn risk_rejects_when_feed_is_not_valid() {
    let mut kernel = RiskKernel::new(RiskLimits::default());
    let quality = FeedQuality::default();
    let decision = kernel.evaluate(&intent(), &quality, Decimal::ZERO, Utc::now());
    assert!(matches!(decision, RiskDecision::Rejected { .. }));
}

#[test]
fn risk_rejects_stale_market_data() {
    let now = Utc::now();
    let mut quality = FeedQuality::default();
    quality.accept_snapshot(now - Duration::seconds(10));
    let mut kernel = RiskKernel::new(RiskLimits {
        max_data_age_ms: 1_000,
        ..RiskLimits::default()
    });

    let decision = kernel.evaluate(&intent(), &quality, Decimal::ZERO, now);
    assert_eq!(
        decision,
        RiskDecision::Rejected {
            reason: "market data is stale".to_owned()
        }
    );
}

#[test]
fn kill_switch_is_irreversible_for_current_process() {
    let now = Utc::now();
    let mut quality = FeedQuality::default();
    quality.accept_snapshot(now);
    let mut kernel = RiskKernel::new(RiskLimits::default());
    kernel.trigger_kill_switch();

    let decision = kernel.evaluate(&intent(), &quality, Decimal::ZERO, now);
    assert_eq!(
        decision,
        RiskDecision::Rejected {
            reason: "kill switch is active".to_owned()
        }
    );
}
