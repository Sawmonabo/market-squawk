use std::error::Error;
use std::str::FromStr;

use market_squawk_data::{
    ContractRollEvidence, DatasetId, DatasetManifestRef, DatasetSchemaRegistry, DerivativeBoundary,
    DerivativeCivilDate, DerivativeLifecycleEvidence, DerivativeSelectionDecision,
    DerivativeUniverseSnapshot, Sha256Digest, UniverseError, UniverseExclusionReason, UniverseId,
    UniverseLimits, UniverseMembership, UniverseSnapshot,
};
use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, ContractRollMapping, DigestAlgorithm, EffectiveInterval,
    EvidenceDigest, FuturesLifecycleDateFields, FuturesLifecycleDates, InstrumentId,
    OccOptionIdentity, SourceIdentifier, Timestamp,
};

type TestResult = Result<(), Box<dyn Error>>;

fn manifest(marker: u8) -> Result<DatasetManifestRef, Box<dyn Error>> {
    Ok(DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("historical-constituents")?,
        u64::from(marker),
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([marker; 32]),
    )?)
}

fn membership(
    instrument: &str,
    starts_at: i64,
    ends_at: Option<i64>,
    availability: AvailabilityEvidence,
    marker: u8,
) -> Result<UniverseMembership, Box<dyn Error>> {
    Ok(UniverseMembership::new(
        InstrumentId::from_str(instrument)?,
        EffectiveInterval::new(
            Timestamp::from_unix_nanos(starts_at),
            ends_at.map(Timestamp::from_unix_nanos),
        )?,
        availability,
        manifest(marker)?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [marker; 32]),
    ))
}

fn derivative_membership(
    instrument_id: InstrumentId,
    marker: u8,
) -> Result<UniverseMembership, Box<dyn Error>> {
    Ok(UniverseMembership::new(
        instrument_id,
        EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
        AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(1)),
        manifest(marker)?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [marker; 32]),
    ))
}

fn futures_lifecycle(
    instrument_id: InstrumentId,
    marker: u8,
    fields: FuturesLifecycleDateFields,
) -> Result<DerivativeLifecycleEvidence, Box<dyn Error>> {
    Ok(DerivativeLifecycleEvidence::future(
        instrument_id,
        FuturesLifecycleDates::try_new(fields)?,
        AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(1)),
        manifest(marker)?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [marker; 32]),
    ))
}

#[test]
fn historical_universe_is_time_correct_deterministic_and_bounded() -> TestResult {
    let historical_delisted = "0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1";
    let active = "0187f5f1-6fc2-7fa2-bf05-2ce5354c55c2";
    let future = "0187f5f1-6fc2-7fa2-bf05-2ce5354c55c3";
    let inferred = "0187f5f1-6fc2-7fa2-bf05-2ce5354c55c4";
    let unknown = "0187f5f1-6fc2-7fa2-bf05-2ce5354c55c5";
    let expired = "0187f5f1-6fc2-7fa2-bf05-2ce5354c55c6";
    let historical_id = InstrumentId::from_str(historical_delisted)?;
    let active_id = InstrumentId::from_str(active)?;
    let future_id = InstrumentId::from_str(future)?;
    let as_of = Timestamp::from_unix_nanos(100);
    let universe_id = UniverseId::try_from("us-equities.historical")?;
    let limits = UniverseLimits::try_new(16, 1024 * 1024)?;
    let candidates = vec![
        membership(
            active,
            20,
            None,
            AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(99)),
            2,
        )?,
        membership(unknown, 20, None, AvailabilityEvidence::unknown(), 5)?,
        membership(
            historical_delisted,
            10,
            Some(101),
            AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(50),
                SourceIdentifier::try_from("index-notice-1")?,
            ),
            1,
        )?,
        membership(
            expired,
            10,
            Some(100),
            AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(50),
                SourceIdentifier::try_from("index-notice-6")?,
            ),
            6,
        )?,
        membership(
            future,
            20,
            None,
            AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(101),
                SourceIdentifier::try_from("index-notice-3")?,
            ),
            3,
        )?,
        membership(
            inferred,
            20,
            None,
            AvailabilityEvidence::inferred(
                Timestamp::from_unix_nanos(40),
                SourceIdentifier::try_from("vendor-date-inference-v1")?,
            ),
            4,
        )?,
    ];

    let snapshot =
        UniverseSnapshot::try_build(universe_id.clone(), as_of, candidates.clone(), limits)?;
    let mut reversed = candidates.clone();
    reversed.reverse();
    let reordered = UniverseSnapshot::try_build(universe_id.clone(), as_of, reversed, limits)?;

    assert_eq!(snapshot.content_hash(), reordered.content_hash());
    assert_eq!(snapshot.audit_hash(), reordered.audit_hash());
    assert_eq!(snapshot.memberships(), reordered.memberships());
    assert_eq!(snapshot.exclusions(), reordered.exclusions());
    assert_eq!(snapshot.as_of(), as_of);
    assert_eq!(snapshot.universe_id(), &universe_id);
    assert_eq!(
        snapshot
            .memberships()
            .iter()
            .map(UniverseMembership::instrument_id)
            .collect::<Vec<_>>(),
        vec![historical_id, active_id]
    );
    assert!(snapshot.contains(historical_id));
    assert_eq!(
        snapshot.membership(historical_id).map(|membership| {
            (
                membership.effective_interval(),
                membership.source_manifest().manifest_version(),
                membership.evidence_digest(),
            )
        }),
        Some((
            EffectiveInterval::new(
                Timestamp::from_unix_nanos(10),
                Some(Timestamp::from_unix_nanos(101)),
            )?,
            1,
            EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]),
        ))
    );
    assert_eq!(snapshot.exclusion_counts().total(), 4);
    assert_eq!(snapshot.exclusion_counts().not_effective(), 1);
    assert_eq!(snapshot.exclusion_counts().future_availability(), 1);
    assert_eq!(snapshot.exclusion_counts().inferred_availability(), 1);
    assert_eq!(snapshot.exclusion_counts().unknown_availability(), 1);
    assert_eq!(snapshot.conflict_counts().overlap_pairs(), 0);
    assert!(snapshot.exclusions().iter().any(|exclusion| {
        exclusion.membership().instrument_id() == future_id
            && exclusion.reason() == UniverseExclusionReason::FutureAvailability
    }));

    let boundary = UniverseSnapshot::try_build(
        universe_id.clone(),
        Timestamp::from_unix_nanos(101),
        vec![membership(
            historical_delisted,
            10,
            Some(101),
            AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(50)),
            1,
        )?],
        limits,
    )?;
    assert!(!boundary.contains(historical_id));
    assert_eq!(
        boundary.exclusions()[0].reason(),
        UniverseExclusionReason::NotEffective
    );

    let overlap = UniverseSnapshot::try_build(
        universe_id.clone(),
        as_of,
        vec![
            membership(
                historical_delisted,
                10,
                Some(110),
                AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(50)),
                7,
            )?,
            membership(
                historical_delisted,
                90,
                Some(120),
                AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(95)),
                8,
            )?,
            membership(unknown, 1, None, AvailabilityEvidence::unknown(), 9)?,
        ],
        limits,
    );
    assert!(matches!(
        overlap,
        Err(UniverseError::OverlappingAdmittedMemberships {
            first_instrument,
            conflicts,
            conflict_evidence,
            exclusions,
        }) if first_instrument == historical_id
            && conflicts.conflicting_instruments() == 1
            && conflicts.conflicting_memberships() == 2
            && conflicts.overlap_pairs() == 1
            && conflict_evidence.memberships().len() == 2
            && conflict_evidence.memberships()[0].source_manifest().manifest_version() == 7
            && conflict_evidence.memberships()[1].source_manifest().manifest_version() == 8
            && conflict_evidence.retained_bytes() <= limits.max_retained_bytes()
            && exclusions.total() == 1
            && exclusions.unknown_availability() == 1
    ));

    assert!(matches!(
        UniverseSnapshot::try_build(
            universe_id.clone(),
            as_of,
            candidates.iter().take(2).cloned().collect(),
            UniverseLimits::try_new(1, 1024 * 1024)?,
        ),
        Err(UniverseError::CandidateLimitExceeded {
            limit: 1,
            observed: 2,
        })
    ));
    assert!(matches!(
        UniverseSnapshot::try_build(
            universe_id,
            as_of,
            vec![candidates[0].clone()],
            UniverseLimits::try_new(1, 1)?,
        ),
        Err(UniverseError::RetainedByteLimitExceeded { limit: 1, .. })
    ));
    assert!(matches!(
        UniverseLimits::try_new(0, 0),
        Err(UniverseError::InvalidLimits)
    ));
    assert!(matches!(
        UniverseId::try_from("x".repeat(UniverseId::MAX_LENGTH + 1)),
        Err(UniverseError::UniverseIdTooLong { .. })
    ));
    Ok(())
}

#[test]
fn derivative_lifecycle_is_civil_date_conservative_and_rolls_only_on_explicit_evidence()
-> TestResult {
    let option_id = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c5601")?;
    let expiring_future = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c5602")?;
    let roll_successor = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c5603")?;
    let no_roll_future = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c5604")?;
    let listed = [option_id, expiring_future, roll_successor, no_roll_future];
    let candidates = listed
        .into_iter()
        .enumerate()
        .map(|(index, instrument_id)| {
            derivative_membership(instrument_id, u8::try_from(index + 1)?)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let lifecycle = vec![
        DerivativeLifecycleEvidence::try_option(
            option_id,
            OccOptionIdentity::try_from("SPX   260320C05000000")?,
            CalendarDate::new(2026, 3, 20)?,
            AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(1)),
            manifest(11)?,
            EvidenceDigest::new(DigestAlgorithm::Sha256, [11; 32]),
        )?,
        futures_lifecycle(
            expiring_future,
            12,
            FuturesLifecycleDateFields {
                last_trade_date: Some(CalendarDate::new(2026, 3, 20)?),
                expiration_date: Some(CalendarDate::new(2026, 3, 21)?),
                ..FuturesLifecycleDateFields::default()
            },
        )?,
        futures_lifecycle(
            roll_successor,
            13,
            FuturesLifecycleDateFields {
                last_trade_date: Some(CalendarDate::new(2026, 6, 19)?),
                expiration_date: Some(CalendarDate::new(2026, 6, 20)?),
                ..FuturesLifecycleDateFields::default()
            },
        )?,
        futures_lifecycle(
            no_roll_future,
            14,
            FuturesLifecycleDateFields {
                expiration_date: Some(CalendarDate::new(2026, 3, 21)?),
                ..FuturesLifecycleDateFields::default()
            },
        )?,
    ];
    let calendar_rule = SourceIdentifier::try_from("cme-calendar-2026-v1")?;
    let dates = |date| {
        listed
            .into_iter()
            .map(|instrument_id| {
                DerivativeCivilDate::new(instrument_id, date, calendar_rule.clone())
            })
            .collect::<Vec<_>>()
    };
    let limits = UniverseLimits::try_new(16, 1024 * 1024)?;
    let before = DerivativeUniverseSnapshot::try_build(
        UniverseId::try_from("listed-derivatives.historical")?,
        Timestamp::from_unix_nanos(99),
        candidates.clone(),
        lifecycle.clone(),
        dates(CalendarDate::new(2026, 3, 19)?),
        Vec::new(),
        limits,
    )?;
    assert!(before.contains(option_id));
    assert!(before.contains(expiring_future));

    let boundary = DerivativeUniverseSnapshot::try_build(
        UniverseId::try_from("listed-derivatives.historical")?,
        Timestamp::from_unix_nanos(100),
        candidates.clone(),
        lifecycle.clone(),
        dates(CalendarDate::new(2026, 3, 20)?),
        Vec::new(),
        limits,
    )?;
    assert_eq!(
        boundary.decision(option_id),
        Some(DerivativeSelectionDecision::SameDateUnresolved {
            boundary: DerivativeBoundary::OptionExpiration,
            date: CalendarDate::new(2026, 3, 20)?,
        })
    );
    assert!(!boundary.contains(option_id));
    assert_eq!(
        boundary.decision(expiring_future),
        Some(DerivativeSelectionDecision::SameDateUnresolved {
            boundary: DerivativeBoundary::FuturesLastTrade,
            date: CalendarDate::new(2026, 3, 20)?,
        })
    );
    assert!(boundary.contains(no_roll_future));

    let roll = ContractRollEvidence::new(
        ContractRollMapping::new(
            expiring_future,
            roll_successor,
            Timestamp::from_unix_nanos(100),
        )?,
        AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(90)),
        manifest(20)?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [20; 32]),
    );
    let after = DerivativeUniverseSnapshot::try_build(
        UniverseId::try_from("listed-derivatives.historical")?,
        Timestamp::from_unix_nanos(101),
        candidates.clone(),
        lifecycle.clone(),
        dates(CalendarDate::new(2026, 3, 22)?),
        vec![roll.clone()],
        limits,
    )?;
    assert_eq!(
        after.decision(expiring_future),
        Some(DerivativeSelectionDecision::Rolled {
            to_instrument_id: roll_successor,
            effective_at: Timestamp::from_unix_nanos(100),
        })
    );
    assert_eq!(
        after.decision(no_roll_future),
        Some(DerivativeSelectionDecision::FutureExpiredWithoutRoll {
            boundary: DerivativeBoundary::FuturesExpiration,
            date: CalendarDate::new(2026, 3, 21)?,
        })
    );
    assert_eq!(
        after.decision(option_id),
        Some(DerivativeSelectionDecision::OptionExpired {
            boundary: DerivativeBoundary::OptionExpiration,
            date: CalendarDate::new(2026, 3, 20)?,
        })
    );
    assert!(!after.contains(expiring_future));
    assert!(after.contains(roll_successor));
    assert!(!after.contains(no_roll_future));
    assert_eq!(after.resolved_roll(expiring_future), Some(&roll));
    let mut reversed_candidates = candidates.clone();
    reversed_candidates.reverse();
    let mut reversed_lifecycle = lifecycle;
    reversed_lifecycle.reverse();
    let reordered = DerivativeUniverseSnapshot::try_build(
        UniverseId::try_from("listed-derivatives.historical")?,
        Timestamp::from_unix_nanos(101),
        reversed_candidates,
        reversed_lifecycle,
        dates(CalendarDate::new(2026, 3, 22)?),
        vec![roll],
        limits,
    )?;
    assert_eq!(after.content_hash(), reordered.content_hash());
    assert_eq!(after.audit_hash(), reordered.audit_hash());
    assert_eq!(candidates[0].effective_interval().ends_at(), None);
    Ok(())
}
