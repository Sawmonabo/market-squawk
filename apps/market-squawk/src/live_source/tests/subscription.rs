use std::time::{Duration, Instant};

use super::super::subscription_state::{
    GenerationIdentity, SubscriptionFailure, SubscriptionLimits, SubscriptionPhase,
    SubscriptionStateMachine,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn generation(value: u64) -> TestResult<GenerationIdentity> {
    Ok(GenerationIdentity::try_new(
        "coinbase-exchange-public",
        "coinbase-exchange-v1",
        &format!("session-{value}"),
        value,
    )?)
}

fn state(
    generation: GenerationIdentity,
    started_at: Instant,
    limits: SubscriptionLimits,
) -> TestResult<SubscriptionStateMachine> {
    Ok(SubscriptionStateMachine::try_new(
        generation,
        ["BTC-USD", "ETH-USD"],
        Duration::from_secs(5),
        started_at,
        limits,
    )?)
}

fn generous_limits() -> TestResult<SubscriptionLimits> {
    Ok(SubscriptionLimits::try_new(64, 64 * 1024, 0, 0)?)
}

#[test]
fn exact_acknowledgement_is_required_once_for_the_current_generation() -> TestResult {
    let started_at = Instant::now();
    let generation = generation(1)?;
    let mut machine = state(generation.clone(), started_at, generous_limits()?)?;
    let estimated_peak_bytes = machine.estimated_peak_bytes();
    assert!(estimated_peak_bytes.get() > std::mem::size_of::<SubscriptionStateMachine>());

    assert_eq!(
        machine.observe_heartbeat(&generation, started_at)?,
        SubscriptionPhase::AwaitingAcknowledgement
    );
    assert_eq!(machine.last_market_data_at(), None);
    assert_eq!(
        machine.observe_acknowledgement(
            &generation,
            &["ETH-USD", "BTC-USD"],
            &["heartbeats", "market_trades", "level2"],
            started_at,
        )?,
        SubscriptionPhase::Active
    );
    assert_eq!(
        machine.acknowledged_products().collect::<Vec<_>>(),
        ["BTC-USD", "ETH-USD"]
    );
    assert_eq!(
        machine.observe_data(&generation, started_at)?,
        SubscriptionPhase::Active
    );
    assert_eq!(machine.last_market_data_at(), Some(started_at));
    assert_eq!(machine.estimated_peak_bytes(), estimated_peak_bytes);
    assert!(matches!(
        machine.observe_acknowledgement(
            &generation,
            &["BTC-USD", "ETH-USD"],
            &["level2", "market_trades", "heartbeats"],
            started_at,
        ),
        Err(SubscriptionFailure::DuplicateAcknowledgement)
    ));
    assert_eq!(machine.phase(), SubscriptionPhase::Invalid);
    Ok(())
}

#[test]
fn mismatched_ack_or_pre_ack_data_permanently_invalidates_without_replay() -> TestResult {
    let started_at = Instant::now();
    let cases: [(&[&str], &[&str]); 3] = [
        (&["BTC-USD"], &["level2", "market_trades", "heartbeats"]),
        (
            &["BTC-USD", "ETH-USD", "SOL-USD"],
            &["level2", "market_trades", "heartbeats"],
        ),
        (
            &["BTC-USD", "ETH-USD"],
            &["level2", "market_trades", "market_trades"],
        ),
    ];
    for (products, channels) in cases {
        let generation = generation(1)?;
        let mut machine = state(generation.clone(), started_at, generous_limits()?)?;
        assert!(matches!(
            machine.observe_acknowledgement(&generation, products, channels, started_at),
            Err(SubscriptionFailure::AcknowledgementMismatch)
        ));
        assert_eq!(machine.phase(), SubscriptionPhase::Invalid);
    }

    let generation = generation(1)?;
    let mut machine = state(generation.clone(), started_at, generous_limits()?)?;
    assert!(matches!(
        machine.observe_data(&generation, started_at),
        Err(SubscriptionFailure::DataBeforeAcknowledgement)
    ));
    assert_eq!(machine.rejected_pre_acknowledgement_data(), 1);
    assert_eq!(machine.last_market_data_at(), None);
    assert!(matches!(
        machine.observe_acknowledgement(
            &generation,
            &["BTC-USD", "ETH-USD"],
            &["level2", "market_trades", "heartbeats"],
            started_at,
        ),
        Err(SubscriptionFailure::GenerationInvalid)
    ));
    assert_eq!(machine.last_market_data_at(), None);
    Ok(())
}

#[test]
fn deadline_generation_and_bounded_audit_lifetime_fail_closed_only_on_integrity() -> TestResult {
    let started_at = Instant::now();
    let expired_start = started_at
        .checked_sub(Duration::from_secs(6))
        .ok_or("test clock could not represent expired subscription")?;
    let first = generation(1)?;
    let second = generation(2)?;

    let mut expired = state(first.clone(), expired_start, generous_limits()?)?;
    assert!(matches!(
        expired.poll_deadline(started_at),
        Err(SubscriptionFailure::AcknowledgementDeadlineExceeded)
    ));

    let mut stale = state(second.clone(), started_at, generous_limits()?)?;
    assert!(matches!(
        stale.observe_heartbeat(&first, started_at),
        Err(SubscriptionFailure::StaleGeneration)
    ));
    assert_eq!(stale.phase(), SubscriptionPhase::Invalid);

    let minimum_audit_bytes = SubscriptionLimits::minimum_control_bytes();
    assert!(SubscriptionLimits::try_new(1, minimum_audit_bytes - 1, 0, 0).is_err());
    let minimum_limits = SubscriptionLimits::try_new(1, minimum_audit_bytes, 0, 0)?;
    let mut minimum_audit = state(first.clone(), started_at, minimum_limits)?;
    minimum_audit.observe_heartbeat(&first, started_at)?;
    assert_eq!(minimum_audit.audit_usage(), (1, minimum_audit_bytes));

    let mut bounded_audit = state(
        first.clone(),
        started_at,
        SubscriptionLimits::try_new(2, 64, 0, 0)?,
    )?;
    bounded_audit.observe_validated_acknowledgement(&first, started_at)?;
    for _ in 0..5 {
        assert_eq!(
            bounded_audit.observe_heartbeat(&first, started_at)?,
            SubscriptionPhase::Active
        );
    }
    let (records, bytes) = bounded_audit.audit_usage();
    assert_eq!(records, 2);
    assert!(bytes <= 64);
    assert_eq!(
        super::super::subscription_state::next_transition(u64::MAX),
        Err(SubscriptionFailure::TransitionSequenceExhausted)
    );
    Ok(())
}
