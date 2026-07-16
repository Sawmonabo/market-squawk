use market_squawk_domain::{
    AssetClass, Currency, Denomination, EffectiveInterval, EvidenceDigest, InstrumentDefinition,
    InstrumentDefinitionInput, InstrumentError, InstrumentId, LotSize, MetadataRevision,
    PayloadHashAlgorithm, ProviderIdentityConflict, ProviderIdentityConflictReason,
    ProviderIdentityEvidence, ProviderIdentityIngestOutcome, ProviderIdentityLocator,
    ProviderIdentityRecord, ProviderIdentityRecordInput, ProviderIdentityRegistry,
    ProviderIdentitySupersession, ProviderInstrumentId, SourceId, SourceIdentifier, TickSize,
    Timestamp, TradingStatus,
};
use rust_decimal::Decimal;
use uuid::Uuid;

fn instrument(value: u128) -> Result<InstrumentId, Box<dyn std::error::Error>> {
    Ok(InstrumentId::try_from(Uuid::from_u128(value))?)
}

fn revision(value: &str) -> Result<MetadataRevision, Box<dyn std::error::Error>> {
    Ok(MetadataRevision::new(SourceIdentifier::try_from(value)?))
}

fn evidence(digest: u8) -> ProviderIdentityEvidence {
    ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
        PayloadHashAlgorithm::Sha256,
        [digest; 32],
    ))
}

fn evidence_with_locator(
    digest: u8,
    reference: &str,
    version: &str,
) -> Result<ProviderIdentityEvidence, Box<dyn std::error::Error>> {
    Ok(ProviderIdentityEvidence::with_version_pinned_locator(
        EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [digest; 32]),
        ProviderIdentityLocator::new(
            SourceIdentifier::try_from(reference)?,
            SourceIdentifier::try_from(version)?,
        ),
    ))
}

fn record_with_evidence(
    mapped_instrument: InstrumentId,
    revision_name: &str,
    observed_at: i64,
    evidence: ProviderIdentityEvidence,
    validity: EffectiveInterval,
    supersedes: Option<ProviderIdentitySupersession>,
) -> Result<ProviderIdentityRecord, Box<dyn std::error::Error>> {
    Ok(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: mapped_instrument,
        source_id: SourceId::try_from("vendor-alpha")?,
        provider_instrument_id: ProviderInstrumentId::try_from("12345")?,
        evidence,
        source_timestamp: Some(Timestamp::from_unix_nanos(99)),
        observed_at: Timestamp::from_unix_nanos(observed_at),
        metadata_revision: revision(revision_name)?,
        validity,
        supersedes,
    }))
}

fn record(
    mapped_instrument: InstrumentId,
    revision_name: &str,
    observed_at: i64,
    digest: u8,
    validity: EffectiveInterval,
    supersedes: Option<ProviderIdentitySupersession>,
) -> Result<ProviderIdentityRecord, Box<dyn std::error::Error>> {
    record_with_evidence(
        mapped_instrument,
        revision_name,
        observed_at,
        evidence(digest),
        validity,
        supersedes,
    )
}

fn supersedes(
    predecessor: &str,
    digest: u8,
) -> Result<ProviderIdentitySupersession, Box<dyn std::error::Error>> {
    Ok(ProviderIdentitySupersession::new(
        revision(predecessor)?,
        evidence(digest),
    ))
}

fn conflicted_registry(
    owner: InstrumentId,
) -> Result<ProviderIdentityRegistry, Box<dyn std::error::Error>> {
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?;
    Ok(ProviderIdentityRegistry::try_from_records(vec![
        record(owner, "revision-1", 100, 1, validity, None)?,
        record(owner, "revision-1", 200, 2, validity, None)?,
    ])?)
}

#[test]
fn registry_wire_capacity_matches_aggregate_reconstruction_budget() {
    assert_eq!(
        ProviderIdentityRegistry::MAX_WIRE_RECORDS,
        ProviderIdentityRegistry::MAX_RECONSTRUCTION_RECORDS
    );

    let one_more_conflict_bound = ProviderIdentityRegistry::MAX_CONFLICTS
        .checked_add(1)
        .and_then(|conflicts| {
            conflicts.checked_mul(ProviderIdentityConflict::MAX_COMPETING_ASSERTIONS)
        })
        .and_then(|conflict_records| {
            ProviderIdentityRegistry::MAX_ACCEPTED_RECORDS.checked_add(conflict_records)
        });
    assert!(matches!(
        one_more_conflict_bound,
        Some(total) if total > ProviderIdentityRegistry::MAX_RECONSTRUCTION_RECORDS
    ));
}

fn definition(
    definition_instrument: InstrumentId,
    provider_identities: Vec<ProviderIdentityRecord>,
) -> Result<InstrumentDefinition, InstrumentError> {
    InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: definition_instrument,
        asset_class: AssetClass::Equity,
        primary_denomination: Denomination::Currency(
            Currency::try_from("USD").map_err(|_| InstrumentError::InvalidEffectiveInterval)?,
        ),
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))
            .map_err(|_| InstrumentError::InvalidEffectiveInterval)?,
        lot_size: LotSize::try_from_decimal(Decimal::ONE)
            .map_err(|_| InstrumentError::InvalidEffectiveInterval)?,
        venue_mappings: Vec::new(),
        provider_identities,
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })
}

#[test]
fn registry_ingest_reports_exhaustive_state_transitions_and_never_selects_a_winner()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let first_interval = EffectiveInterval::new(
        Timestamp::from_unix_nanos(10),
        Some(Timestamp::from_unix_nanos(20)),
    )?;
    let second_interval = EffectiveInterval::new(Timestamp::from_unix_nanos(20), None)?;
    let mut registry = ProviderIdentityRegistry::new();

    assert_eq!(
        registry.ingest(record(owner, "revision-1", 100, 1, first_interval, None)?)?,
        ProviderIdentityIngestOutcome::Inserted
    );
    assert_eq!(
        registry.ingest(record_with_evidence(
            owner,
            "revision-1",
            101,
            evidence_with_locator(1, "assertion:a", "version:1")?,
            first_interval,
            None,
        )?)?,
        ProviderIdentityIngestOutcome::ObservationCoalesced
    );
    assert_eq!(
        registry.ingest(record(
            owner,
            "revision-2",
            200,
            2,
            second_interval,
            Some(supersedes("revision-1", 42)?),
        )?)?,
        ProviderIdentityIngestOutcome::SupersedingRevisionAppended
    );
    assert_eq!(
        registry.ingest(record(
            owner,
            "revision-2",
            201,
            3,
            second_interval,
            Some(supersedes("revision-1", 42)?),
        )?)?,
        ProviderIdentityIngestOutcome::ConflictQuarantined
    );
    assert_eq!(registry.accepted().len(), 1);
    assert_eq!(registry.conflicts().len(), 1);
    assert!(
        registry
            .provider_identity_at(
                &SourceId::try_from("vendor-alpha")?,
                &ProviderInstrumentId::try_from("12345")?,
                Timestamp::from_unix_nanos(15),
            )
            .is_none()
    );
    Ok(())
}

#[test]
fn exact_duplicate_reports_coalescing_without_changing_state()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let assertion = record(
        owner,
        "revision-1",
        100,
        1,
        EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?,
        None,
    )?;
    let mut registry = ProviderIdentityRegistry::try_from_records(vec![assertion.clone()])?;
    let before = registry.clone();

    assert_eq!(
        registry.ingest(assertion)?,
        ProviderIdentityIngestOutcome::ObservationCoalesced
    );
    assert_eq!(registry, before);
    Ok(())
}

#[test]
fn rejected_registry_ingest_is_transactional() -> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let first_interval = EffectiveInterval::new(
        Timestamp::from_unix_nanos(10),
        Some(Timestamp::from_unix_nanos(20)),
    )?;
    let second_interval = EffectiveInterval::new(Timestamp::from_unix_nanos(20), None)?;
    let mut registry = ProviderIdentityRegistry::try_from_records(vec![record(
        owner,
        "revision-1",
        100,
        1,
        first_interval,
        None,
    )?])?;
    let before = registry.clone();

    assert!(matches!(
        registry.ingest(record(
            owner,
            "revision-2",
            200,
            2,
            second_interval,
            Some(supersedes("revision-missing", 42)?),
        )?),
        Err(InstrumentError::MissingProviderIdentityPredecessor { .. })
    ));
    assert_eq!(registry, before);
    Ok(())
}

#[test]
fn duplicate_of_any_quarantined_variant_is_classified_as_observation_coalescing()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?;
    let mut registry = ProviderIdentityRegistry::try_from_records(vec![
        record(owner, "revision-1", 100, 1, validity, None)?,
        record(owner, "revision-1", 200, 2, validity, None)?,
    ])?;

    assert_eq!(
        registry.ingest(record(owner, "revision-1", 300, 2, validity, None)?)?,
        ProviderIdentityIngestOutcome::ObservationCoalesced
    );
    assert_eq!(registry.conflicts()[0].competing_assertions().len(), 2);
    assert_eq!(
        registry.conflicts()[0].competing_assertions()[1].observation_timestamps(),
        &[
            Timestamp::from_unix_nanos(200),
            Timestamp::from_unix_nanos(300)
        ]
    );
    Ok(())
}

#[test]
fn new_revision_for_a_quarantined_key_is_rejected_transactionally()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let first_interval = EffectiveInterval::new(
        Timestamp::from_unix_nanos(10),
        Some(Timestamp::from_unix_nanos(20)),
    )?;
    let second_interval = EffectiveInterval::new(Timestamp::from_unix_nanos(20), None)?;
    let mut registry = ProviderIdentityRegistry::try_from_records(vec![
        record(owner, "revision-1", 100, 1, first_interval, None)?,
        record(owner, "revision-1", 200, 2, first_interval, None)?,
    ])?;
    let before = registry.clone();

    assert!(matches!(
        registry.ingest(record(
            owner,
            "revision-2",
            300,
            3,
            second_interval,
            Some(supersedes("revision-1", 42)?),
        )?),
        Err(InstrumentError::ProviderIdentityKeyQuarantined { .. })
    ));
    assert_eq!(registry, before);
    Ok(())
}

#[test]
fn same_revision_conflicts_are_quarantined_without_an_order_dependent_winner()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?;
    let candidates = vec![
        record(owner, "revision-7", 100, 7, validity, None)?,
        record(owner, "revision-7", 300, 8, validity, None)?,
        record(
            owner,
            "revision-7",
            400,
            7,
            EffectiveInterval::new(Timestamp::from_unix_nanos(11), None)?,
            None,
        )?,
    ];
    let forward = definition(owner, candidates.clone())?;
    let reverse = definition(owner, candidates.into_iter().rev().collect())?;

    assert_eq!(forward, reverse);
    assert!(forward.provider_identities().is_empty());
    assert_eq!(forward.provider_identity_conflicts().len(), 1);
    let conflict = &forward.provider_identity_conflicts()[0];
    assert_eq!(
        conflict.reason(),
        ProviderIdentityConflictReason::SameRevisionDivergence
    );
    assert_eq!(conflict.competing_assertions().len(), 3);
    assert_eq!(
        serde_json::from_value::<InstrumentDefinition>(serde_json::to_value(&forward)?)?,
        forward
    );
    Ok(())
}

#[test]
fn instrument_definition_rejects_mismatched_quarantined_competitors()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let other = instrument(2)?;
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?;

    assert!(matches!(
        definition(
            owner,
            vec![
                record(owner, "revision-1", 100, 1, validity, None)?,
                record(other, "revision-1", 101, 1, validity, None)?,
            ],
        ),
        Err(InstrumentError::ProviderIdentityInstrumentMismatch { .. })
    ));
    Ok(())
}

#[test]
fn a_conflict_on_any_revision_suppresses_resolution_for_the_whole_natural_key()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let other = instrument(2)?;
    let first = record(
        owner,
        "revision-1",
        100,
        1,
        EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?,
        None,
    )?;
    let mut records = vec![first];
    records.extend([
        record(
            owner,
            "revision-2",
            200,
            2,
            EffectiveInterval::new(Timestamp::from_unix_nanos(20), None)?,
            Some(supersedes("revision-1", 9)?),
        )?,
        record(
            other,
            "revision-2",
            201,
            2,
            EffectiveInterval::new(Timestamp::from_unix_nanos(20), None)?,
            Some(supersedes("revision-1", 9)?),
        )?,
    ]);
    let registry = ProviderIdentityRegistry::try_from_records(records)?;

    assert_eq!(registry.accepted().len(), 1);
    assert_eq!(registry.conflicts().len(), 1);
    assert!(
        registry
            .provider_identity_at(
                &SourceId::try_from("vendor-alpha")?,
                &ProviderInstrumentId::try_from("12345")?,
                Timestamp::from_unix_nanos(15),
            )
            .is_none()
    );
    Ok(())
}

#[test]
fn quarantining_a_predecessor_revision_does_not_leave_a_partial_winner()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let other = instrument(2)?;
    let first_interval = EffectiveInterval::new(
        Timestamp::from_unix_nanos(10),
        Some(Timestamp::from_unix_nanos(20)),
    )?;
    let second_interval = EffectiveInterval::new(Timestamp::from_unix_nanos(20), None)?;
    let registry = ProviderIdentityRegistry::try_from_records(vec![
        record(owner, "revision-1", 100, 1, first_interval, None)?,
        record(other, "revision-1", 101, 1, first_interval, None)?,
        record(
            owner,
            "revision-2",
            200,
            2,
            second_interval,
            Some(supersedes("revision-1", 42)?),
        )?,
    ])?;

    assert_eq!(registry.accepted().len(), 1);
    assert_eq!(registry.conflicts().len(), 1);
    assert!(
        registry
            .provider_identity_at(
                &SourceId::try_from("vendor-alpha")?,
                &ProviderInstrumentId::try_from("12345")?,
                Timestamp::from_unix_nanos(25),
            )
            .is_none()
    );
    Ok(())
}

#[test]
fn registry_wire_reconstructs_invariants_and_rejects_tampering()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let registry = ProviderIdentityRegistry::try_from_records(vec![record(
        owner,
        "revision-1",
        100,
        1,
        EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?,
        None,
    )?])?;
    let wire = serde_json::to_value(&registry)?;
    assert_eq!(
        serde_json::from_value::<ProviderIdentityRegistry>(wire.clone())?,
        registry
    );

    let mut unknown = wire.clone();
    unknown["adapter_selected_winner"] = serde_json::json!(true);
    assert!(serde_json::from_value::<ProviderIdentityRegistry>(unknown).is_err());

    let mut empty_observations = wire;
    empty_observations["accepted"][0]["observation_timestamps"] = serde_json::json!([]);
    assert!(serde_json::from_value::<ProviderIdentityRegistry>(empty_observations).is_err());
    Ok(())
}

#[test]
fn registry_wire_round_trip_preserves_quarantined_competitors()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = conflicted_registry(instrument(1)?)?;

    assert_eq!(
        serde_json::from_value::<ProviderIdentityRegistry>(serde_json::to_value(&registry)?)?,
        registry
    );
    Ok(())
}

#[test]
fn registry_wire_field_order_preserves_checked_reconstruction()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = conflicted_registry(instrument(1)?)?;
    let wire = serde_json::to_value(&registry)?;
    let conflicts = serde_json::to_string(&wire["conflicts"])?;
    let accepted = serde_json::to_string(&wire["accepted"])?;
    let conflicts_first = format!(r#"{{"conflicts":{conflicts},"accepted":{accepted}}}"#);

    assert_eq!(
        serde_json::from_str::<ProviderIdentityRegistry>(&conflicts_first)?,
        registry
    );
    Ok(())
}

#[test]
fn instrument_definition_exposes_the_checked_provider_registry()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let definition = definition(
        owner,
        vec![record(
            owner,
            "revision-1",
            100,
            1,
            EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?,
            None,
        )?],
    )?;

    assert_eq!(
        definition.provider_identity_registry().accepted(),
        definition.provider_identities()
    );
    assert_eq!(
        definition.provider_identity_registry().conflicts(),
        definition.provider_identity_conflicts()
    );
    Ok(())
}
