use market_squawk_domain::{
    AssetClass, Currency, Denomination, EffectiveInterval, InstrumentDefinition,
    InstrumentDefinitionInput, InstrumentError, InstrumentId, LotSize, MetadataRevision,
    PayloadHash, PayloadHashAlgorithm, PayloadReference, ProviderIdentityConflictReason,
    ProviderIdentityRecord, ProviderIdentityRecordInput, ProviderIdentitySupersession,
    ProviderInstrumentId, SourceId, SourceIdentifier, TickSize, Timestamp, TradingStatus,
};
use proptest::prelude::*;
use rust_decimal::Decimal;
use uuid::Uuid;

fn instrument(value: u128) -> Result<InstrumentId, Box<dyn std::error::Error>> {
    Ok(InstrumentId::try_from(Uuid::from_u128(value))?)
}

fn revision(value: &str) -> Result<MetadataRevision, Box<dyn std::error::Error>> {
    Ok(MetadataRevision::new(SourceIdentifier::try_from(value)?))
}

fn reference(digest: u8) -> PayloadReference {
    PayloadReference::ContentHash(PayloadHash::new(PayloadHashAlgorithm::Sha256, [digest; 32]))
}

fn record(
    mapped_instrument: InstrumentId,
    revision_name: &str,
    observed_at: i64,
    digest: u8,
    validity: EffectiveInterval,
    supersedes: Option<ProviderIdentitySupersession>,
) -> Result<ProviderIdentityRecord, Box<dyn std::error::Error>> {
    Ok(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: mapped_instrument,
        source_id: SourceId::try_from("vendor-alpha")?,
        provider_instrument_id: ProviderInstrumentId::try_from("12345")?,
        source_reference: reference(digest),
        source_timestamp: Some(Timestamp::from_unix_nanos(99)),
        observed_at: Timestamp::from_unix_nanos(observed_at),
        metadata_revision: revision(revision_name)?,
        validity,
        supersedes,
    }))
}

fn supersedes(
    predecessor: &str,
    digest: u8,
) -> Result<ProviderIdentitySupersession, Box<dyn std::error::Error>> {
    Ok(ProviderIdentitySupersession::new(
        revision(predecessor)?,
        reference(digest),
    ))
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
fn identical_assertions_coalesce_and_retain_unique_sorted_observations()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?;
    let first = record(owner, "revision-7", 300, 7, validity, None)?;
    let duplicate_observation = record(owner, "revision-7", 300, 7, validity, None)?;
    let later_observation = record(owner, "revision-7", 500, 7, validity, None)?;

    let definition = definition(owner, vec![later_observation, duplicate_observation, first])?;
    assert_eq!(definition.provider_identities().len(), 1);
    assert_eq!(
        definition.provider_identities()[0].observation_timestamps(),
        &[
            Timestamp::from_unix_nanos(300),
            Timestamp::from_unix_nanos(500)
        ]
    );
    assert!(definition.provider_identity_conflicts().is_empty());
    assert_eq!(
        definition
            .provider_identity_at(
                &SourceId::try_from("vendor-alpha")?,
                &ProviderInstrumentId::try_from("12345")?,
                Timestamp::from_unix_nanos(20),
            )
            .map(ProviderIdentityRecord::instrument_id),
        Some(owner)
    );
    Ok(())
}

#[test]
fn same_natural_key_and_revision_conflicts_are_quarantined_without_order_winner()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let other = instrument(2)?;
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?;
    let candidates = vec![
        record(owner, "revision-7", 100, 7, validity, None)?,
        record(other, "revision-7", 200, 7, validity, None)?,
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
    assert_eq!(conflict.competing_assertions().len(), 4);
    assert_eq!(
        serde_json::from_value::<InstrumentDefinition>(serde_json::to_value(&forward)?)?,
        forward
    );
    assert!(
        forward
            .provider_identity_at(
                &SourceId::try_from("vendor-alpha")?,
                &ProviderInstrumentId::try_from("12345")?,
                Timestamp::from_unix_nanos(20),
            )
            .is_none()
    );
    Ok(())
}

#[test]
fn revisions_require_evidenced_linear_supersession_and_nonoverlapping_transition()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let first_interval = EffectiveInterval::new(
        Timestamp::from_unix_nanos(10),
        Some(Timestamp::from_unix_nanos(20)),
    )?;
    let second_interval = EffectiveInterval::new(Timestamp::from_unix_nanos(20), None)?;
    let first = record(owner, "revision-1", 100, 1, first_interval, None)?;
    let second = record(
        owner,
        "revision-2",
        200,
        2,
        second_interval,
        Some(supersedes("revision-1", 42)?),
    )?;
    let accepted = definition(owner, vec![second.clone(), first.clone()])?;
    assert_eq!(accepted.provider_identities().len(), 2);
    assert_eq!(
        accepted.provider_identities()[1]
            .supersedes()
            .map(ProviderIdentitySupersession::predecessor),
        Some(&revision("revision-1")?)
    );

    assert!(matches!(
        definition(
            owner,
            vec![record(
                owner,
                "revision-2",
                200,
                2,
                second_interval,
                Some(supersedes("revision-1", 42)?),
            )?],
        ),
        Err(InstrumentError::MissingProviderIdentityPredecessor { .. })
    ));
    assert!(matches!(
        definition(
            owner,
            vec![
                first.clone(),
                record(owner, "revision-2", 200, 2, second_interval, None)?,
            ],
        ),
        Err(InstrumentError::MissingProviderIdentitySupersession { .. })
    ));
    assert!(matches!(
        definition(
            owner,
            vec![
                first.clone(),
                record(
                    owner,
                    "revision-2",
                    200,
                    2,
                    second_interval,
                    Some(supersedes("revision-0", 42)?),
                )?,
            ],
        ),
        Err(InstrumentError::MissingProviderIdentityPredecessor { .. })
    ));
    assert!(matches!(
        definition(
            owner,
            vec![
                record(
                    owner,
                    "revision-1",
                    100,
                    1,
                    first_interval,
                    Some(supersedes("revision-2", 41)?),
                )?,
                second,
            ],
        ),
        Err(InstrumentError::ProviderIdentitySupersessionCycle { .. })
    ));
    assert!(matches!(
        definition(
            owner,
            vec![
                first,
                record(
                    owner,
                    "revision-2",
                    200,
                    2,
                    EffectiveInterval::new(Timestamp::from_unix_nanos(19), None)?,
                    Some(supersedes("revision-1", 42)?),
                )?,
            ],
        ),
        Err(InstrumentError::InvalidProviderIdentityTransition { .. })
    ));
    Ok(())
}

#[test]
fn a_conflict_on_any_revision_suppresses_active_resolution_for_the_natural_key()
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
    let conflicting_revision = vec![
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
    ];
    let mut records = vec![first];
    records.extend(conflicting_revision);
    let definition = definition(owner, records)?;
    assert_eq!(definition.provider_identities().len(), 1);
    assert_eq!(definition.provider_identity_conflicts().len(), 1);
    assert!(
        definition
            .provider_identity_at(
                &SourceId::try_from("vendor-alpha")?,
                &ProviderInstrumentId::try_from("12345")?,
                Timestamp::from_unix_nanos(15),
            )
            .is_none()
    );
    Ok(())
}

proptest! {
    #[test]
    fn canonical_provider_ingestion_is_permutation_invariant(sort_keys in prop::array::uniform6(any::<u64>())) {
        let owner = instrument(1).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let first_interval = EffectiveInterval::new(
            Timestamp::from_unix_nanos(10),
            Some(Timestamp::from_unix_nanos(20)),
        ).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let second_interval = EffectiveInterval::new(Timestamp::from_unix_nanos(20), None)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let edge = supersedes("revision-1", 9)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let records = vec![
            record(owner, "revision-1", 100, 1, first_interval, None),
            record(owner, "revision-1", 101, 1, first_interval, None),
            record(owner, "revision-1", 100, 1, first_interval, None),
            record(owner, "revision-2", 200, 2, second_interval, Some(edge.clone())),
            record(owner, "revision-2", 201, 2, second_interval, Some(edge.clone())),
            record(owner, "revision-2", 201, 2, second_interval, Some(edge)),
        ]
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let baseline = definition(
            owner,
            records.clone(),
        ).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let mut order: Vec<_> = (0..records.len()).collect();
        order.sort_by_key(|index| (sort_keys[*index], *index));
        let permuted = order.into_iter().map(|index| records[index].clone()).collect();
        let candidate = definition(owner, permuted)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(candidate, baseline);
    }
}

#[test]
fn provider_identity_wire_is_strict_and_canonical() -> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let record = record(
        owner,
        "revision-2",
        200,
        2,
        EffectiveInterval::new(Timestamp::from_unix_nanos(20), None)?,
        Some(supersedes("revision-1", 9)?),
    )?;
    let wire = serde_json::to_value(&record)?;
    assert_eq!(
        serde_json::from_value::<ProviderIdentityRecord>(wire.clone())?,
        record
    );
    let mut unknown = wire;
    unknown["untrusted_note"] = serde_json::json!(true);
    assert!(serde_json::from_value::<ProviderIdentityRecord>(unknown).is_err());
    Ok(())
}
