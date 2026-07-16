use super::*;
use crate::{
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, InstrumentId, MetadataRevision,
    ProviderIdentityEvidence, ProviderIdentityLocator, ProviderIdentityRecordInput,
    SourceIdentifier,
};
use uuid::Uuid;

fn instrument(value: u128) -> Result<InstrumentId, Box<dyn std::error::Error>> {
    Ok(InstrumentId::try_from(Uuid::from_u128(value))?)
}

fn evidence(byte: u8) -> ProviderIdentityEvidence {
    ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [byte; 32],
    ))
}

fn evidence_with_locator(
    byte: u8,
    reference: &str,
    version: &str,
) -> Result<ProviderIdentityEvidence, Box<dyn std::error::Error>> {
    Ok(ProviderIdentityEvidence::with_version_pinned_locator(
        EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32]),
        ProviderIdentityLocator::new(
            SourceIdentifier::try_from(reference)?,
            SourceIdentifier::try_from(version)?,
        ),
    ))
}

fn record(
    owner: InstrumentId,
    provider_instrument_id: &str,
    observed_at: i64,
    evidence: ProviderIdentityEvidence,
) -> Result<ProviderIdentityRecord, Box<dyn std::error::Error>> {
    record_with_source_timestamp(owner, provider_instrument_id, observed_at, 99, evidence)
}

fn record_with_source_timestamp(
    owner: InstrumentId,
    provider_instrument_id: &str,
    observed_at: i64,
    source_timestamp: i64,
    evidence: ProviderIdentityEvidence,
) -> Result<ProviderIdentityRecord, Box<dyn std::error::Error>> {
    Ok(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: owner,
        source_id: SourceId::try_from("vendor-alpha")?,
        provider_instrument_id: ProviderInstrumentId::try_from(provider_instrument_id)?,
        evidence,
        source_timestamp: Some(Timestamp::from_unix_nanos(source_timestamp)),
        observed_at: Timestamp::from_unix_nanos(observed_at),
        metadata_revision: MetadataRevision::new(SourceIdentifier::try_from("revision-1")?),
        validity: EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?,
        supersedes: None,
    }))
}

#[test]
fn accepted_exact_duplicate_coalesces_at_test_policy_ceiling()
-> Result<(), Box<dyn std::error::Error>> {
    let assertion = record(instrument(1)?, "12345", 100, evidence(1))?;
    let mut registry = ProviderIdentityRegistry::try_from_records(vec![assertion.clone()])?;
    let before = registry.clone();
    let accepted_allocation = registry.accepted().as_ptr();

    assert_eq!(
        registry.ingest_with_reconstruction_limit(assertion, 1)?,
        ProviderIdentityIngestOutcome::ObservationCoalesced
    );
    assert_eq!(registry, before);
    assert_eq!(registry.accepted().as_ptr(), accepted_allocation);
    Ok(())
}

#[test]
fn accepted_metadata_only_duplicate_merges_at_test_policy_ceiling()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let mut registry = ProviderIdentityRegistry::try_from_records(vec![record(
        owner,
        "12345",
        200,
        evidence(1),
    )?])?;
    let accepted_allocation = registry.accepted().as_ptr();

    assert_eq!(
        registry.ingest_with_reconstruction_limit(
            record(
                owner,
                "12345",
                100,
                evidence_with_locator(1, "provider-object:z", "version:2")?,
            )?,
            1,
        )?,
        ProviderIdentityIngestOutcome::ObservationCoalesced
    );
    assert_eq!(
        registry.accepted()[0].observation_timestamps(),
        &[
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(200)
        ]
    );
    assert_eq!(registry.accepted()[0].evidence().locators().len(), 1);
    assert_eq!(registry.accepted().as_ptr(), accepted_allocation);
    assert_eq!(
        serde_json::from_value::<ProviderIdentityRegistry>(serde_json::to_value(&registry)?)?,
        registry
    );
    Ok(())
}

#[test]
fn quarantined_exact_duplicate_coalesces_at_test_policy_ceiling()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let assertion = record(owner, "12345", 200, evidence(2))?;
    let mut registry = ProviderIdentityRegistry::try_from_records(vec![
        record(owner, "12345", 100, evidence(1))?,
        assertion.clone(),
    ])?;
    let before = registry.clone();
    let competitor_allocation = registry.conflicts()[0].competing_assertions().as_ptr();

    assert_eq!(
        registry.ingest_with_reconstruction_limit(assertion, 2)?,
        ProviderIdentityIngestOutcome::ObservationCoalesced
    );
    assert_eq!(registry, before);
    assert_eq!(
        registry.conflicts()[0].competing_assertions().as_ptr(),
        competitor_allocation
    );
    Ok(())
}

#[test]
fn quarantined_metadata_only_duplicate_merges_and_orders_at_test_policy_ceiling()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let mut registry = ProviderIdentityRegistry::try_from_records(vec![
        record_with_source_timestamp(owner, "12345", 200, 99, evidence(2))?,
        record_with_source_timestamp(owner, "12345", 100, 100, evidence(2))?,
    ])?;
    let competitor_allocation = registry.conflicts()[0].competing_assertions().as_ptr();

    assert_eq!(
        registry.ingest_with_reconstruction_limit(
            record_with_source_timestamp(
                owner,
                "12345",
                50,
                99,
                evidence_with_locator(2, "provider-object:z", "version:2")?,
            )?,
            2,
        )?,
        ProviderIdentityIngestOutcome::ObservationCoalesced
    );
    let competitors = registry.conflicts()[0].competing_assertions();
    assert_eq!(
        competitors[0].source_timestamp(),
        Some(Timestamp::from_unix_nanos(100))
    );
    assert_eq!(
        competitors[1].source_timestamp(),
        Some(Timestamp::from_unix_nanos(99))
    );
    assert_eq!(
        competitors[1].observation_timestamps(),
        &[
            Timestamp::from_unix_nanos(50),
            Timestamp::from_unix_nanos(200)
        ]
    );
    assert_eq!(competitors[1].evidence().locators().len(), 1);
    assert_eq!(competitors.as_ptr(), competitor_allocation);
    assert_eq!(
        serde_json::from_value::<ProviderIdentityRegistry>(serde_json::to_value(&registry)?)?,
        registry
    );
    Ok(())
}

#[test]
fn growth_is_rejected_transactionally_at_test_policy_ceiling()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let mut registry = ProviderIdentityRegistry::try_from_records(vec![record(
        owner,
        "12345",
        100,
        evidence(1),
    )?])?;
    let before = registry.clone();
    reset_registry_test_probes();

    assert!(matches!(
        registry.ingest_with_reconstruction_limit(record(owner, "67890", 200, evidence(2))?, 1,),
        Err(InstrumentError::ProviderIdentityCapacityExceeded {
            collection: ProviderIdentityCollection::ReconstructionRecords,
            max: 1,
        })
    ));
    assert_eq!(registry, before);
    assert_eq!(reconstruction_build_count(), 0);
    Ok(())
}

#[test]
fn accepted_retry_lookup_is_logarithmic_and_does_not_reconstruct()
-> Result<(), Box<dyn std::error::Error>> {
    const RECORD_COUNT: usize = 4_096;
    let owner = instrument(1)?;
    let records = (0..RECORD_COUNT)
        .map(|index| record(owner, &format!("provider-{index:04}"), 100, evidence(1)))
        .collect::<Result<Vec<_>, _>>()?;
    let assertion = records[RECORD_COUNT / 2].clone();
    let mut registry = ProviderIdentityRegistry::try_from_records(records)?;
    reset_registry_test_probes();

    assert_eq!(
        registry.ingest(assertion)?,
        ProviderIdentityIngestOutcome::ObservationCoalesced
    );
    assert!(revision_lookup_comparison_count() <= 16);
    assert_eq!(reconstruction_build_count(), 0);
    Ok(())
}

#[test]
fn conflict_retry_lookup_finds_first_middle_and_last_groups_without_reconstruction()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let provider_ids = ["provider-0000", "provider-2048", "provider-4095"];
    let mut records = Vec::new();
    for provider_id in provider_ids {
        records.push(record(owner, provider_id, 100, evidence(1))?);
        records.push(record(owner, provider_id, 100, evidence(2))?);
    }
    let mut registry = ProviderIdentityRegistry::try_from_records(records)?;

    for provider_id in provider_ids {
        reset_registry_test_probes();
        assert_eq!(
            registry.ingest(record(owner, provider_id, 100, evidence(1))?)?,
            ProviderIdentityIngestOutcome::ObservationCoalesced
        );
        assert!(revision_lookup_comparison_count() <= 4);
        assert_eq!(reconstruction_build_count(), 0);
    }
    Ok(())
}

#[test]
fn accepted_metadata_merge_preserves_canonical_position_without_reconstruction()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = instrument(1)?;
    let mut registry = ProviderIdentityRegistry::try_from_records(vec![
        record(owner, "provider-0000", 100, evidence(1))?,
        record(owner, "provider-2048", 200, evidence(1))?,
        record(owner, "provider-4095", 100, evidence(1))?,
    ])?;
    let allocation = registry.accepted().as_ptr();
    reset_registry_test_probes();

    assert_eq!(
        registry.ingest(record(
            owner,
            "provider-2048",
            100,
            evidence_with_locator(1, "provider-object:middle", "version:2")?,
        )?)?,
        ProviderIdentityIngestOutcome::ObservationCoalesced
    );
    let provider_ids = registry
        .accepted()
        .iter()
        .map(|record| record.provider_instrument_id().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        provider_ids,
        [
            "provider-0000".to_owned(),
            "provider-2048".to_owned(),
            "provider-4095".to_owned()
        ]
    );
    assert_eq!(registry.accepted().as_ptr(), allocation);
    assert_eq!(reconstruction_build_count(), 0);
    Ok(())
}

#[test]
fn checked_record_count_rejects_aggregate_and_arithmetic_overflow() {
    assert!(matches!(
        checked_record_count(ProviderIdentityRegistry::MAX_RECONSTRUCTION_RECORDS, [1]),
        Err(InstrumentError::ProviderIdentityCapacityExceeded {
            collection: ProviderIdentityCollection::ReconstructionRecords,
            max: ProviderIdentityRegistry::MAX_RECONSTRUCTION_RECORDS,
        })
    ));
    assert!(matches!(
        checked_record_count(usize::MAX, std::iter::empty()),
        Err(InstrumentError::ProviderIdentityCapacityExceeded {
            collection: ProviderIdentityCollection::ReconstructionRecords,
            max: ProviderIdentityRegistry::MAX_RECONSTRUCTION_RECORDS,
        })
    ));
}

#[test]
fn locator_exhaustion_during_coalescing_is_transactional() -> Result<(), Box<dyn std::error::Error>>
{
    let owner = instrument(1)?;
    let locators = (0..ProviderIdentityEvidence::MAX_LOCATORS)
        .map(|index| {
            Ok(ProviderIdentityLocator::new(
                SourceIdentifier::try_from(format!("provider-object:{index:02}"))?,
                SourceIdentifier::try_from(format!("version:{index:02}"))?,
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    let full_evidence = ProviderIdentityEvidence::try_with_locators(
        EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]),
        locators,
    )?;
    let mut registry = ProviderIdentityRegistry::try_from_records(vec![record(
        owner,
        "12345",
        100,
        full_evidence,
    )?])?;
    let before = registry.clone();

    assert!(matches!(
        registry.ingest_with_reconstruction_limit(
            record(
                owner,
                "12345",
                100,
                evidence_with_locator(1, "provider-object:overflow", "version:overflow")?,
            )?,
            1,
        ),
        Err(InstrumentError::ProviderIdentityCapacityExceeded {
            collection: ProviderIdentityCollection::Locators,
            max: ProviderIdentityEvidence::MAX_LOCATORS,
        })
    ));
    assert_eq!(registry, before);
    Ok(())
}

#[test]
fn observation_exhaustion_in_quarantine_is_transactional() -> Result<(), Box<dyn std::error::Error>>
{
    let owner = instrument(1)?;
    let mut records = (0..ProviderIdentityRecord::MAX_OBSERVATIONS)
        .map(|offset| record(owner, "12345", 100 + offset as i64, evidence(1)))
        .collect::<Result<Vec<_>, _>>()?;
    records.push(record(owner, "12345", 200, evidence(2))?);
    let mut registry = ProviderIdentityRegistry::try_from_records(records)?;
    let before = registry.clone();

    assert!(matches!(
        registry.ingest_with_reconstruction_limit(record(owner, "12345", 10_000, evidence(1))?, 2,),
        Err(InstrumentError::ProviderIdentityCapacityExceeded {
            collection: ProviderIdentityCollection::ObservationTimestamps,
            max: ProviderIdentityRecord::MAX_OBSERVATIONS,
        })
    ));
    assert_eq!(registry, before);
    Ok(())
}
