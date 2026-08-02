use market_squawk_domain::{
    AssetClass, Currency, Denomination, EffectiveInterval, EvidenceDigest, InstrumentDefinition,
    InstrumentDefinitionInput, InstrumentError, InstrumentId, LotSize, MetadataRevision,
    PayloadHashAlgorithm, ProviderIdentityEvidence, ProviderIdentityLocator,
    ProviderIdentityRecord, ProviderIdentityRecordInput, ProviderIdentityRegistry,
    ProviderIdentitySupersession, ProviderInstrumentId, SourceId, SourceIdentifier, TickSize,
    Timestamp, TradingStatus,
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

fn reference(digest: u8) -> ProviderIdentityEvidence {
    ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
        PayloadHashAlgorithm::Sha256,
        [digest; 32],
    ))
}

fn locator(
    reference: &str,
    version: &str,
) -> Result<ProviderIdentityLocator, Box<dyn std::error::Error>> {
    Ok(ProviderIdentityLocator::new(
        SourceIdentifier::try_from(reference)?,
        SourceIdentifier::try_from(version)?,
    ))
}

fn evidence_with_locator(
    digest: u8,
    reference: &str,
    version: &str,
) -> Result<ProviderIdentityEvidence, Box<dyn std::error::Error>> {
    Ok(ProviderIdentityEvidence::with_version_pinned_locator(
        EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [digest; 32]),
        locator(reference, version)?,
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
        reference(digest),
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
        reference(digest),
    ))
}

fn definition(
    definition_instrument: InstrumentId,
    provider_identities: Vec<ProviderIdentityRecord>,
) -> Result<InstrumentDefinition, InstrumentError> {
    InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: definition_instrument,
        definition_revision: market_squawk_domain::InstrumentDefinitionRevision::try_from(1_u64)
            .map_err(|_| InstrumentError::InvalidEffectiveInterval)?,
        asset_class: AssetClass::Equity,
        primary_denomination: Denomination::Currency(
            Currency::try_from("USD").map_err(|_| InstrumentError::InvalidEffectiveInterval)?,
        ),
        quote_currency: Currency::try_from("USD")
            .map_err(|_| InstrumentError::InvalidEffectiveInterval)?,
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))
            .map_err(|_| InstrumentError::InvalidEffectiveInterval)?,
        lot_size: LotSize::try_from_decimal(Decimal::ONE)
            .map_err(|_| InstrumentError::InvalidEffectiveInterval)?,
        contract_multiplier: Decimal::ONE,
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
fn locator_metadata_coalesces_without_changing_assertion_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?;
    let without_locator = record(owner, "revision-7", 300, 7, validity, None)?;
    let later_locator = record_with_evidence(
        owner,
        "revision-7",
        500,
        evidence_with_locator(7, "provider-object:z", "version:2")?,
        validity,
        None,
    )?;
    let earlier_locator = record_with_evidence(
        owner,
        "revision-7",
        400,
        evidence_with_locator(7, "provider-object:a", "version:1")?,
        validity,
        None,
    )?;

    let registry = ProviderIdentityRegistry::try_from_records(vec![
        later_locator,
        without_locator,
        earlier_locator,
    ])?;
    assert_eq!(registry.accepted().len(), 1);
    assert!(registry.conflicts().is_empty());
    assert_eq!(
        registry.accepted()[0].observation_timestamps(),
        &[
            Timestamp::from_unix_nanos(300),
            Timestamp::from_unix_nanos(400),
            Timestamp::from_unix_nanos(500),
        ]
    );
    let locators = registry.accepted()[0].evidence().locators();
    assert_eq!(locators.len(), 2);
    assert_eq!(locators[0].reference().as_str(), "provider-object:a");
    assert_eq!(locators[1].reference().as_str(), "provider-object:z");
    Ok(())
}

#[test]
fn supersession_locator_metadata_coalesces_on_content_equivalent_edges()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let first_interval = EffectiveInterval::new(
        Timestamp::from_unix_nanos(10),
        Some(Timestamp::from_unix_nanos(20)),
    )?;
    let second_interval = EffectiveInterval::new(Timestamp::from_unix_nanos(20), None)?;
    let root = record(owner, "revision-1", 100, 1, first_interval, None)?;
    let second_without_locators = record(
        owner,
        "revision-2",
        200,
        2,
        second_interval,
        Some(supersedes("revision-1", 42)?),
    )?;
    let second_with_locators = record_with_evidence(
        owner,
        "revision-2",
        201,
        evidence_with_locator(2, "assertion:b", "version:2")?,
        second_interval,
        Some(ProviderIdentitySupersession::new(
            revision("revision-1")?,
            evidence_with_locator(42, "transition:a", "version:1")?,
        )),
    )?;

    let registry = ProviderIdentityRegistry::try_from_records(vec![
        second_with_locators,
        root,
        second_without_locators,
    ])?;
    assert!(registry.conflicts().is_empty());
    assert_eq!(registry.accepted().len(), 2);
    let successor = &registry.accepted()[1];
    assert_eq!(successor.evidence().locators().len(), 1);
    assert_eq!(successor.observation_timestamps().len(), 2);
    assert_eq!(
        successor
            .supersedes()
            .map(ProviderIdentitySupersession::evidence)
            .map(ProviderIdentityEvidence::locators)
            .map(<[_]>::len),
        Some(1)
    );
    Ok(())
}

#[test]
fn digest_algorithm_and_transition_content_remain_substantive_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let first_interval = EffectiveInterval::new(
        Timestamp::from_unix_nanos(10),
        Some(Timestamp::from_unix_nanos(20)),
    )?;
    let second_interval = EffectiveInterval::new(Timestamp::from_unix_nanos(20), None)?;
    let root = record(owner, "revision-1", 100, 1, first_interval, None)?;
    let sha = record(
        owner,
        "revision-2",
        200,
        2,
        second_interval,
        Some(supersedes("revision-1", 42)?),
    )?;
    let blake = record_with_evidence(
        owner,
        "revision-2",
        201,
        ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
            PayloadHashAlgorithm::Blake3,
            [2; 32],
        )),
        second_interval,
        Some(supersedes("revision-1", 42)?),
    )?;
    let transition_changed = record(
        owner,
        "revision-2",
        202,
        2,
        second_interval,
        Some(supersedes("revision-1", 43)?),
    )?;

    let registry =
        ProviderIdentityRegistry::try_from_records(vec![root, sha, blake, transition_changed])?;
    assert_eq!(registry.accepted().len(), 1);
    assert_eq!(registry.conflicts().len(), 1);
    assert_eq!(registry.conflicts()[0].competing_assertions().len(), 3);
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
        let first_locator = evidence_with_locator(1, "provider-object:z", "version:2")
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let second_locator = evidence_with_locator(2, "provider-object:a", "version:1")
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let records = vec![
            record(owner, "revision-1", 100, 1, first_interval, None),
            record_with_evidence(
                owner,
                "revision-1",
                101,
                first_locator,
                first_interval,
                None,
            ),
            record(owner, "revision-1", 100, 1, first_interval, None),
            record(owner, "revision-2", 200, 2, second_interval, Some(edge.clone())),
            record_with_evidence(
                owner,
                "revision-2",
                201,
                second_locator,
                second_interval,
                Some(edge.clone()),
            ),
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

#[test]
fn provider_evidence_requires_a_digest_and_retains_sorted_unique_locator_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let bytes = [19; 32];
    let later = locator("provider-object:z", "metadata-version:42")?;
    let earlier = locator("provider-object:a", "metadata-version:41")?;
    let sha = ProviderIdentityEvidence::try_with_locators(
        EvidenceDigest::new(PayloadHashAlgorithm::Sha256, bytes),
        [later.clone(), earlier.clone(), later],
    )?;
    let blake = ProviderIdentityEvidence::with_version_pinned_locator(
        EvidenceDigest::new(PayloadHashAlgorithm::Blake3, bytes),
        earlier,
    );

    assert_ne!(sha, blake);
    assert_eq!(
        sha.content_digest().algorithm(),
        PayloadHashAlgorithm::Sha256
    );
    assert_eq!(sha.content_digest().bytes(), bytes);
    assert_eq!(sha.locators().len(), 2);
    assert_eq!(sha.locators()[0].reference().as_str(), "provider-object:a");
    assert_eq!(sha.locators()[1].reference().as_str(), "provider-object:z");
    assert_eq!(
        serde_json::from_value::<ProviderIdentityEvidence>(serde_json::to_value(&sha)?)?,
        sha
    );
    Ok(())
}

#[test]
fn provider_evidence_wire_rejects_bare_locators_and_tampering()
-> Result<(), Box<dyn std::error::Error>> {
    let bare_locator = serde_json::json!({
        "locators": [{
            "reference": "https://provider.example/current/instrument/12345",
            "version": "metadata-version:42"
        }]
    });
    assert!(serde_json::from_value::<ProviderIdentityEvidence>(bare_locator).is_err());

    let locator_without_version = serde_json::json!({
        "content_digest": {
            "algorithm": "sha256",
            "bytes": vec![7; 32]
        },
        "locators": [{
            "reference": "provider-object:instrument/12345"
        }]
    });
    assert!(serde_json::from_value::<ProviderIdentityEvidence>(locator_without_version).is_err());

    let legacy_bare_reference = serde_json::json!({
        "kind": "source_reference",
        "value": "https://provider.example/current/instrument/12345"
    });
    assert!(serde_json::from_value::<ProviderIdentityEvidence>(legacy_bare_reference).is_err());

    let mut unknown = serde_json::to_value(ProviderIdentityEvidence::from_content_digest(
        EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [7; 32]),
    ))?;
    unknown["trusted_without_digest"] = serde_json::json!(true);
    assert!(serde_json::from_value::<ProviderIdentityEvidence>(unknown).is_err());

    let too_many_locators = serde_json::json!({
        "content_digest": {
            "algorithm": "sha256",
            "bytes": vec![7; 32]
        },
        "locators": (0..=ProviderIdentityEvidence::MAX_LOCATORS)
            .map(|index| serde_json::json!({
                "reference": format!("provider-object:{index}"),
                "version": format!("metadata-version:{index}")
            }))
            .collect::<Vec<_>>()
    });
    assert!(serde_json::from_value::<ProviderIdentityEvidence>(too_many_locators).is_err());
    Ok(())
}
