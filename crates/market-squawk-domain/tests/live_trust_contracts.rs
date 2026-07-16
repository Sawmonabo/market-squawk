mod support;

use std::error::Error;

use market_squawk_domain::{
    AssessmentStatus, ClassificationError, FreshnessState, LiveEventClass, LiveTimingAssessment,
    LiveTimingPolicy, MarketEventTiming, QualificationAssessment, QualificationComponent,
    QualificationError, SnapshotEvidence, Timestamp, TimestampIntegrity,
};
use support::live::{BindingSpec, Component, assessment_input, binding, valid_assessment_input};

#[test]
fn archive_assessment_never_returns_execution_authority() -> Result<(), Box<dyn Error>> {
    let assessment = QualificationAssessment::try_from(valid_assessment_input()?)?;

    assert_eq!(
        assessment.recorded_quality(),
        market_squawk_domain::DataQuality::DirectVerified
    );
    assert_eq!(
        assessment.assessment_status_at(Timestamp::from_unix_nanos(1_020)),
        AssessmentStatus::Satisfied
    );
    Ok(())
}

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
fn every_assessment_component_rejects_a_transplanted_binding() -> Result<(), Box<dyn Error>> {
    let base = binding(&BindingSpec::default())?;
    let replacement = binding(&BindingSpec {
        channel: "ticker",
        ..BindingSpec::default()
    })?;
    let cases = [
        (
            Component::SourcePolicy,
            QualificationComponent::SourcePolicy,
        ),
        (Component::Sequence, QualificationComponent::Sequence),
        (Component::Snapshot, QualificationComponent::Snapshot),
        (Component::Checksum, QualificationComponent::Checksum),
        (Component::Timing, QualificationComponent::Timing),
        (
            Component::TradingStatus,
            QualificationComponent::TradingStatus,
        ),
        (Component::Precision, QualificationComponent::Precision),
        (Component::Coverage, QualificationComponent::Coverage),
        (Component::Book, QualificationComponent::Book),
        (Component::Stream, QualificationComponent::Stream),
        (Component::Capture, QualificationComponent::Capture),
    ];

    for (component, expected) in cases {
        let input = assessment_input(
            base.clone(),
            Some(component),
            replacement.clone(),
            Timestamp::from_unix_nanos(1_020),
        )?;
        assert_eq!(
            QualificationAssessment::try_from(input),
            Err(QualificationError::BindingMismatch {
                component: expected
            })
        );
    }
    Ok(())
}

#[test]
fn complete_key_rejects_transplant_across_every_identity_dimension() -> Result<(), Box<dyn Error>> {
    let base_spec = BindingSpec::default();
    let base = binding(&base_spec)?;
    let mutations = [
        BindingSpec {
            source: "kraken-direct",
            ..base_spec.clone()
        },
        BindingSpec {
            session: "session-8",
            ..base_spec.clone()
        },
        BindingSpec {
            metadata_revision: "coinbase-advanced-trade-v4",
            ..base_spec.clone()
        },
        BindingSpec {
            authorization_basis: "different-authorized-account",
            ..base_spec.clone()
        },
        BindingSpec {
            venue: "KRAKEN",
            ..base_spec.clone()
        },
        BindingSpec {
            instrument: "0187f5f1-6fc2-7fa2-bf05-2ce5354c55cc",
            ..base_spec.clone()
        },
        BindingSpec {
            generation: 8,
            ..base_spec.clone()
        },
        BindingSpec {
            product: "ETH-USD",
            ..base_spec.clone()
        },
        BindingSpec {
            channel: "ticker",
            ..base_spec.clone()
        },
        BindingSpec {
            event_class: LiveEventClass::BookSnapshot,
            ..base_spec.clone()
        },
        BindingSpec {
            source_identifier: "update-43",
            ..base_spec.clone()
        },
        BindingSpec {
            payload_digest: 9,
            ..base_spec.clone()
        },
        BindingSpec {
            state_digest: 9,
            ..base_spec.clone()
        },
        BindingSpec {
            book_state_id: "book-state-43",
            ..base_spec.clone()
        },
        BindingSpec {
            depth: market_squawk_domain::MarketDepth::OrderLevel,
            ..base_spec
        },
    ];

    for mutation in mutations {
        let replacement = binding(&mutation)?;
        let input = assessment_input(
            base.clone(),
            Some(Component::Sequence),
            replacement,
            Timestamp::from_unix_nanos(1_020),
        )?;
        assert_eq!(
            QualificationAssessment::try_from(input),
            Err(QualificationError::BindingMismatch {
                component: QualificationComponent::Sequence,
            })
        );
    }
    Ok(())
}

#[test]
fn generation_rollover_invalidates_prior_assessment_without_aliasing() -> Result<(), Box<dyn Error>>
{
    let base = binding(&BindingSpec::default())?;
    let next_generation = binding(&BindingSpec {
        generation: 8,
        ..BindingSpec::default()
    })?;
    let input = assessment_input(
        base,
        Some(Component::Timing),
        next_generation,
        Timestamp::from_unix_nanos(1_020),
    )?;

    assert_eq!(
        QualificationAssessment::try_from(input),
        Err(QualificationError::BindingMismatch {
            component: QualificationComponent::Timing
        })
    );
    Ok(())
}

#[test]
fn snapshot_initialization_is_explicit_even_without_provider_sequence() -> Result<(), Box<dyn Error>>
{
    let generation = market_squawk_domain::ConnectionGeneration::new(7)?;
    let initialized = market_squawk_domain::InitializedSnapshot::new(
        generation,
        market_squawk_domain::SourceIdentifier::try_from("snapshot-7")?,
        market_squawk_domain::EvidenceDigest::new([7; 32]),
        Timestamp::from_unix_nanos(900),
        None,
    );
    let evidence = SnapshotEvidence::assess_initialized(initialized, generation, None)?;

    assert!(evidence.is_initialized());
    assert_eq!(evidence.snapshot_sequence(), None);
    assert!(!SnapshotEvidence::uninitialized(generation).is_initialized());
    Ok(())
}

#[test]
fn non_book_events_require_explicit_metadata_backed_snapshot_non_applicability()
-> Result<(), Box<dyn Error>> {
    let spec = BindingSpec {
        event_class: LiveEventClass::Trade,
        ..BindingSpec::default()
    };
    let base = binding(&spec)?;
    let input = assessment_input(base.clone(), None, base, Timestamp::from_unix_nanos(1_020))?;
    let assessment = QualificationAssessment::try_from(input)?;

    assert_eq!(
        assessment.assessment_status_at(Timestamp::from_unix_nanos(1_020)),
        AssessmentStatus::Satisfied
    );
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
    assert_eq!(
        timing(Some(975), 1_000, 1_020, policy)?.timestamp_integrity(),
        TimestampIntegrity::Valid
    );
    assert_eq!(
        timing(Some(974), 1_000, 1_020, policy)?.timestamp_integrity(),
        TimestampIntegrity::Invalid
    );
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
