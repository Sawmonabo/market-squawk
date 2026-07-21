use std::error::Error;
use std::str::FromStr;

use market_squawk_data::{
    DatasetId, DatasetManifestRef, DatasetSchemaRegistry, Sha256Digest, UniverseError,
    UniverseExclusionReason, UniverseId, UniverseLimits, UniverseMembership, UniverseSnapshot,
};
use market_squawk_domain::{
    AvailabilityEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest, InstrumentId,
    SourceIdentifier, Timestamp,
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
