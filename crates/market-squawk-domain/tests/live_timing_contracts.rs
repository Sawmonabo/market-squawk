use crate::support;

use std::error::Error;

use market_squawk_domain::{
    AssessmentStatus, BoundAssessment, ClassificationError, FreshnessState, LiveTimingAssessment,
    LiveTimingPolicy, MarketEventTiming, QualificationAssessment, Timestamp, TimestampIntegrity,
};
use support::live::{BindingSpec, assessment_input, binding, valid_assessment_input};

#[test]
fn strictest_expiry_is_inclusive_then_queue_delay_rejects_at_plus_one_nanosecond()
-> Result<(), Box<dyn Error>> {
    let assessment = QualificationAssessment::try_from(valid_assessment_input()?)?;

    assert_eq!(assessment.valid_until(), Timestamp::from_unix_nanos(1_020));
    assert_eq!(
        assessment.assessment_status_at(Timestamp::from_unix_nanos(1_020)),
        AssessmentStatus::Satisfied
    );
    assert_eq!(
        assessment.assessment_status_at(Timestamp::from_unix_nanos(1_021)),
        AssessmentStatus::Rejected
    );
    Ok(())
}

#[test]
fn timing_bound_cannot_be_stretched_past_market_age_deadline() -> Result<(), Box<dyn Error>> {
    let evidence_binding = binding(&BindingSpec::default())?;
    let evaluated_at = Timestamp::from_unix_nanos(1_010);
    let timing = LiveTimingAssessment::assess(
        evidence_binding.connection_generation(),
        Some(MarketEventTiming::new(
            Some(Timestamp::from_unix_nanos(1_000)),
            Timestamp::from_unix_nanos(1_000),
        )),
        None,
        evaluated_at,
        LiveTimingPolicy::new(0, 0, 100, 20)?,
    )?;

    assert!(
        BoundAssessment::new(
            evidence_binding.clone(),
            evaluated_at,
            Timestamp::from_unix_nanos(1_020),
            timing,
        )
        .is_ok()
    );
    assert!(
        BoundAssessment::new(
            evidence_binding,
            evaluated_at,
            Timestamp::from_unix_nanos(1_021),
            timing,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn timing_bound_cannot_be_stretched_past_source_age_deadline() -> Result<(), Box<dyn Error>> {
    let evidence_binding = binding(&BindingSpec::default())?;
    let evaluated_at = Timestamp::from_unix_nanos(1_010);
    let timing = LiveTimingAssessment::assess(
        evidence_binding.connection_generation(),
        Some(MarketEventTiming::new(
            Some(Timestamp::from_unix_nanos(1_000)),
            Timestamp::from_unix_nanos(1_000),
        )),
        None,
        evaluated_at,
        LiveTimingPolicy::new(0, 0, 20, 100)?,
    )?;

    assert!(
        BoundAssessment::new(
            evidence_binding.clone(),
            evaluated_at,
            Timestamp::from_unix_nanos(1_020),
            timing,
        )
        .is_ok()
    );
    assert!(
        BoundAssessment::new(
            evidence_binding,
            evaluated_at,
            Timestamp::from_unix_nanos(1_021),
            timing,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn qualification_deadline_is_derived_even_when_caller_requests_a_longer_window()
-> Result<(), Box<dyn Error>> {
    let base = binding(&BindingSpec::default())?;
    let input = assessment_input(base.clone(), None, base, Timestamp::from_unix_nanos(1_100));

    assert!(input.is_err());
    Ok(())
}

#[test]
fn timing_deadline_handles_i64_edges_without_overflow() -> Result<(), Box<dyn Error>> {
    let evidence_binding = binding(&BindingSpec::default())?;
    let maximum = Timestamp::from_unix_nanos(i64::MAX);
    let high = LiveTimingAssessment::assess(
        evidence_binding.connection_generation(),
        Some(MarketEventTiming::new(
            Some(Timestamp::from_unix_nanos(i64::MAX - 1)),
            Timestamp::from_unix_nanos(i64::MAX - 1),
        )),
        None,
        maximum,
        LiveTimingPolicy::new(0, 0, i64::MAX as u64, i64::MAX as u64)?,
    )?;
    assert!(BoundAssessment::new(evidence_binding.clone(), maximum, maximum, high).is_ok());

    let minimum = Timestamp::from_unix_nanos(i64::MIN);
    let low = LiveTimingAssessment::assess(
        evidence_binding.connection_generation(),
        Some(MarketEventTiming::new(Some(minimum), minimum)),
        None,
        minimum,
        LiveTimingPolicy::new(0, 0, 0, 0)?,
    )?;
    assert!(BoundAssessment::new(evidence_binding, minimum, minimum, low).is_ok());
    Ok(())
}

fn timing(
    source_at: Option<i64>,
    received_at: i64,
    evaluated_at: i64,
    policy: LiveTimingPolicy,
) -> Result<LiveTimingAssessment, Box<dyn Error>> {
    Ok(LiveTimingAssessment::assess(
        market_squawk_domain::ConnectionGeneration::new(7)?,
        Some(MarketEventTiming::new(
            source_at.map(Timestamp::from_unix_nanos),
            Timestamp::from_unix_nanos(received_at),
        )),
        Some(Timestamp::from_unix_nanos(evaluated_at)),
        Timestamp::from_unix_nanos(evaluated_at),
        policy,
    )?)
}

#[test]
fn atomic_timing_rejects_stale_source_and_heartbeat_cannot_refresh_market()
-> Result<(), Box<dyn Error>> {
    let policy = LiveTimingPolicy::new(5, 25, 50, 20)?;
    let stale_source = timing(Some(900), 1_000, 1_010, policy)?;
    assert_eq!(
        stale_source.timestamp_integrity(),
        TimestampIntegrity::Invalid
    );
    assert_eq!(stale_source.freshness(), FreshnessState::Fresh);

    let heartbeat_only = LiveTimingAssessment::assess(
        market_squawk_domain::ConnectionGeneration::new(7)?,
        Some(MarketEventTiming::new(
            Some(Timestamp::from_unix_nanos(1_000)),
            Timestamp::from_unix_nanos(1_000),
        )),
        Some(Timestamp::from_unix_nanos(2_000)),
        Timestamp::from_unix_nanos(2_000),
        LiveTimingPolicy::new(0, 2_000, 2_000, 10)?,
    )?;
    assert_eq!(heartbeat_only.freshness(), FreshnessState::Stale);
    Ok(())
}

#[test]
fn timing_boundaries_and_i64_extremes_use_checked_wide_arithmetic() -> Result<(), Box<dyn Error>> {
    let policy = LiveTimingPolicy::new(5, 25, 50, 20)?;
    let exact_transport = timing(Some(975), 1_000, 1_020, policy)?;
    assert_eq!(
        exact_transport.timestamp_integrity(),
        TimestampIntegrity::Valid
    );
    assert_eq!(
        exact_transport.maximum_valid_instant(),
        Some(Timestamp::from_unix_nanos(1_020))
    );
    let excessive_transport = timing(Some(974), 1_000, 1_020, policy)?;
    assert_eq!(
        excessive_transport.timestamp_integrity(),
        TimestampIntegrity::Invalid
    );
    assert_eq!(excessive_transport.maximum_valid_instant(), None);

    let exact_future_skew = timing(Some(1_005), 1_000, 1_010, policy)?;
    assert_eq!(
        exact_future_skew.timestamp_integrity(),
        TimestampIntegrity::Valid
    );
    assert_eq!(
        exact_future_skew.maximum_valid_instant(),
        Some(Timestamp::from_unix_nanos(1_020))
    );
    let excessive_future_skew = timing(Some(1_006), 1_000, 1_010, policy)?;
    assert_eq!(
        excessive_future_skew.timestamp_integrity(),
        TimestampIntegrity::Invalid
    );
    assert_eq!(excessive_future_skew.maximum_valid_instant(), None);
    assert_eq!(
        timing(Some(995), 1_000, 1_021, policy)?.freshness(),
        FreshnessState::Stale
    );

    let edge = timing(
        Some(i64::MIN),
        i64::MIN,
        i64::MAX,
        LiveTimingPolicy::new(0, i64::MAX as u64, i64::MAX as u64, i64::MAX as u64)?,
    )?;
    assert_eq!(edge.timestamp_integrity(), TimestampIntegrity::Invalid);
    assert_eq!(edge.freshness(), FreshnessState::Stale);
    assert_eq!(
        LiveTimingAssessment::assess(
            market_squawk_domain::ConnectionGeneration::new(7)?,
            Some(MarketEventTiming::new(
                Some(Timestamp::from_unix_nanos(2)),
                Timestamp::from_unix_nanos(2),
            )),
            None,
            Timestamp::from_unix_nanos(1),
            policy,
        ),
        Err(ClassificationError::EvaluationBeforeReceive)
    );
    Ok(())
}
