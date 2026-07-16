//! Transactional provider-identity aggregate and typed ingest outcomes.

use serde::{Deserialize, Deserializer, Serialize};

use super::{
    BoundedVec, ProviderIdentityCollection, ProviderIdentityConflict, ProviderIdentityRecord,
    compare_records, normalize_provider_identities, provider_identity_at, same_natural_key,
};
use crate::{InstrumentError, ProviderInstrumentId, SourceId, Timestamp};

/// Exhaustive successful state transition produced by [`ProviderIdentityRegistry::ingest`].
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderIdentityIngestOutcome {
    /// The assertion introduced the first revision for its natural provider key.
    Inserted,
    /// A content-equivalent assertion was coalesced, including an already-retained exact duplicate.
    ///
    /// New locator metadata and local observation times are retained when supplied. Reingesting an
    /// exact duplicate reports this outcome without changing canonical registry state.
    ObservationCoalesced,
    /// A checked, evidenced successor revision was appended to an existing key.
    SupersedingRevisionAppended,
    /// A substantive same-revision variant caused every competitor to be quarantined.
    ConflictQuarantined,
}

/// Checked aggregate owning accepted provider assertions and quarantined conflicts.
///
/// Callers submit raw assertions through [`Self::ingest`] or [`Self::try_from_records`]. The
/// registry alone performs content coalescing, graph validation, deterministic canonicalization,
/// and conflict quarantine, so an adapter cannot select a winning same-revision assertion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProviderIdentityRegistry {
    accepted: Vec<ProviderIdentityRecord>,
    conflicts: Vec<ProviderIdentityConflict>,
}

const fn checked_max_wire_records() -> Option<usize> {
    match ProviderIdentityRegistry::MAX_CONFLICTS
        .checked_mul(ProviderIdentityConflict::MAX_COMPETING_ASSERTIONS)
    {
        Some(conflict_records) => {
            ProviderIdentityRegistry::MAX_ACCEPTED_RECORDS.checked_add(conflict_records)
        }
        None => None,
    }
}

const fn wire_record_bound_is_valid() -> bool {
    match checked_max_wire_records() {
        Some(total) => total <= ProviderIdentityRegistry::MAX_RECONSTRUCTION_RECORDS,
        None => false,
    }
}

impl ProviderIdentityRegistry {
    /// Maximum accepted revisions retained by one registry.
    pub const MAX_ACCEPTED_RECORDS: usize = 65_536;
    /// Maximum raw assertions accepted by one batch reconstruction.
    pub const MAX_RECONSTRUCTION_RECORDS: usize = 262_144;
    /// Maximum conflict groups retained without exceeding the worst-case reconstruction budget.
    pub const MAX_CONFLICTS: usize = 768;
    /// Maximum provider records the nested registry wire can retain before reconstruction.
    pub const MAX_WIRE_RECORDS: usize = match checked_max_wire_records() {
        Some(total) => total,
        None => usize::MAX,
    };

    /// Constructs an empty checked registry.
    pub const fn new() -> Self {
        Self {
            accepted: Vec::new(),
            conflicts: Vec::new(),
        }
    }

    /// Deterministically reconstructs a registry from raw assertions.
    ///
    /// # Errors
    ///
    /// Returns a typed [`InstrumentError`] when input exceeds a capacity bound or a conflict-free
    /// natural key has a missing, cyclic, branching, or temporally invalid revision graph.
    pub fn try_from_records(records: Vec<ProviderIdentityRecord>) -> Result<Self, InstrumentError> {
        if records.len() > Self::MAX_RECONSTRUCTION_RECORDS {
            return Err(InstrumentError::ProviderIdentityCapacityExceeded {
                collection: ProviderIdentityCollection::ReconstructionRecords,
                max: Self::MAX_RECONSTRUCTION_RECORDS,
            });
        }
        let (accepted, conflicts) = normalize_provider_identities(records)?;
        if accepted.len() > Self::MAX_ACCEPTED_RECORDS {
            return Err(InstrumentError::ProviderIdentityCapacityExceeded {
                collection: ProviderIdentityCollection::AcceptedRecords,
                max: Self::MAX_ACCEPTED_RECORDS,
            });
        }
        if conflicts.len() > Self::MAX_CONFLICTS {
            return Err(InstrumentError::ProviderIdentityCapacityExceeded {
                collection: ProviderIdentityCollection::Conflicts,
                max: Self::MAX_CONFLICTS,
            });
        }
        Ok(Self {
            accepted,
            conflicts,
        })
    }

    /// Transactionally ingests one raw assertion and returns its exact successful transition.
    ///
    /// Content-equivalent input merges only bounded locator and observation metadata and does not
    /// consume a reconstruction slot. The reconstruction ceiling applies to transitions that grow
    /// the canonical record set. The registry is unchanged if metadata capacity or revision-graph
    /// validation fails.
    ///
    /// # Errors
    ///
    /// Returns a typed [`InstrumentError`] for capacity or revision-graph violations.
    pub fn ingest(
        &mut self,
        record: ProviderIdentityRecord,
    ) -> Result<ProviderIdentityIngestOutcome, InstrumentError> {
        self.ingest_with_reconstruction_limit(record, Self::MAX_RECONSTRUCTION_RECORDS)
    }

    fn ingest_with_reconstruction_limit(
        &mut self,
        record: ProviderIdentityRecord,
        reconstruction_limit: usize,
    ) -> Result<ProviderIdentityIngestOutcome, InstrumentError> {
        if self.coalesce_matching_assertion(&record)? {
            return Ok(ProviderIdentityIngestOutcome::ObservationCoalesced);
        }

        let key_has_conflict = self.conflicts.iter().any(|conflict| {
            conflict.key().source_id() == record.source_id()
                && conflict.key().provider_instrument_id() == record.provider_instrument_id()
        });
        let outcome = self.classify_growth(&record);
        if key_has_conflict && outcome == ProviderIdentityIngestOutcome::SupersedingRevisionAppended
        {
            return Err(InstrumentError::ProviderIdentityKeyQuarantined { key: record.key() });
        }

        let mut records = self.reconstruction_records()?;
        if records.len() >= reconstruction_limit {
            return Err(InstrumentError::ProviderIdentityCapacityExceeded {
                collection: ProviderIdentityCollection::ReconstructionRecords,
                max: reconstruction_limit,
            });
        }
        records.push(record);
        let replacement = Self::try_from_records(records)?;
        *self = replacement;
        Ok(outcome)
    }

    fn coalesce_matching_assertion(
        &mut self,
        incoming: &ProviderIdentityRecord,
    ) -> Result<bool, InstrumentError> {
        if let Some(existing) = self
            .accepted
            .iter_mut()
            .find(|existing| existing.same_assertion(incoming))
        {
            if existing == incoming {
                return Ok(true);
            }
            let mut replacement = existing.clone();
            replacement.merge_assertion_metadata(incoming)?;
            if replacement != *existing {
                *existing = replacement;
                self.accepted.sort_by(compare_records);
            }
            return Ok(true);
        }

        for conflict in &mut self.conflicts {
            if let Some(existing) = conflict
                .competing_assertions
                .iter_mut()
                .find(|existing| existing.same_assertion(incoming))
            {
                if existing == incoming {
                    return Ok(true);
                }
                let mut replacement = existing.clone();
                replacement.merge_assertion_metadata(incoming)?;
                if replacement != *existing {
                    *existing = replacement;
                    conflict.competing_assertions.sort_by(compare_records);
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Returns accepted assertions in deterministic natural-key/revision order.
    pub fn accepted(&self) -> &[ProviderIdentityRecord] {
        &self.accepted
    }

    /// Returns quarantined conflict groups in deterministic natural-key/revision order.
    pub fn conflicts(&self) -> &[ProviderIdentityConflict] {
        &self.conflicts
    }

    /// Resolves an accepted mapping at an effective instant.
    ///
    /// Any conflict for the natural key suppresses resolution for the complete key.
    pub fn provider_identity_at(
        &self,
        source_id: &SourceId,
        provider_instrument_id: &ProviderInstrumentId,
        at: Timestamp,
    ) -> Option<&ProviderIdentityRecord> {
        provider_identity_at(
            &self.accepted,
            &self.conflicts,
            source_id,
            provider_instrument_id,
            at,
        )
    }

    fn classify_growth(&self, incoming: &ProviderIdentityRecord) -> ProviderIdentityIngestOutcome {
        let mut same_key = false;
        let mut same_revision = false;
        for existing in self.assertions() {
            if !same_natural_key(existing, incoming) {
                continue;
            }
            same_key = true;
            if existing.metadata_revision() == incoming.metadata_revision() {
                same_revision = true;
            }
        }
        if same_revision {
            ProviderIdentityIngestOutcome::ConflictQuarantined
        } else if same_key {
            ProviderIdentityIngestOutcome::SupersedingRevisionAppended
        } else {
            ProviderIdentityIngestOutcome::Inserted
        }
    }

    fn assertions(&self) -> impl Iterator<Item = &ProviderIdentityRecord> {
        self.accepted.iter().chain(
            self.conflicts
                .iter()
                .flat_map(|conflict| conflict.competing_assertions.iter()),
        )
    }

    fn reconstruction_records(&self) -> Result<Vec<ProviderIdentityRecord>, InstrumentError> {
        let mut records =
            Vec::with_capacity(self.accepted.len().min(Self::MAX_RECONSTRUCTION_RECORDS));
        for record in self.assertions() {
            if records.len() == Self::MAX_RECONSTRUCTION_RECORDS {
                return Err(InstrumentError::ProviderIdentityCapacityExceeded {
                    collection: ProviderIdentityCollection::ReconstructionRecords,
                    max: Self::MAX_RECONSTRUCTION_RECORDS,
                });
            }
            records.push(record.clone());
        }
        Ok(records)
    }
}

const _: () = assert!(wire_record_bound_is_valid());

impl Default for ProviderIdentityRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderIdentityRegistryWire {
    #[serde(default)]
    accepted:
        BoundedVec<ProviderIdentityRecord, { ProviderIdentityRegistry::MAX_ACCEPTED_RECORDS }>,
    #[serde(default)]
    conflicts: BoundedVec<ProviderIdentityConflict, { ProviderIdentityRegistry::MAX_CONFLICTS }>,
}

impl<'de> Deserialize<'de> for ProviderIdentityRegistry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderIdentityRegistryWire::deserialize(deserializer)?;
        let mut records = wire.accepted.into_inner();
        for conflict in wire.conflicts.into_inner() {
            if records
                .len()
                .saturating_add(conflict.competing_assertions().len())
                > Self::MAX_RECONSTRUCTION_RECORDS
            {
                return Err(serde::de::Error::custom(
                    "provider identity registry reconstruction exceeds its record bound",
                ));
            }
            records.extend_from_slice(conflict.competing_assertions());
        }
        Self::try_from_records(records).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
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

        assert!(matches!(
            registry
                .ingest_with_reconstruction_limit(record(owner, "67890", 200, evidence(2))?, 1,),
            Err(InstrumentError::ProviderIdentityCapacityExceeded {
                collection: ProviderIdentityCollection::ReconstructionRecords,
                max: 1,
            })
        ));
        assert_eq!(registry, before);
        Ok(())
    }

    #[test]
    fn locator_exhaustion_during_coalescing_is_transactional()
    -> Result<(), Box<dyn std::error::Error>> {
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
    fn observation_exhaustion_in_quarantine_is_transactional()
    -> Result<(), Box<dyn std::error::Error>> {
        let owner = instrument(1)?;
        let mut records = (0..ProviderIdentityRecord::MAX_OBSERVATIONS)
            .map(|offset| record(owner, "12345", 100 + offset as i64, evidence(1)))
            .collect::<Result<Vec<_>, _>>()?;
        records.push(record(owner, "12345", 200, evidence(2))?);
        let mut registry = ProviderIdentityRegistry::try_from_records(records)?;
        let before = registry.clone();

        assert!(matches!(
            registry
                .ingest_with_reconstruction_limit(record(owner, "12345", 10_000, evidence(1))?, 2,),
            Err(InstrumentError::ProviderIdentityCapacityExceeded {
                collection: ProviderIdentityCollection::ObservationTimestamps,
                max: ProviderIdentityRecord::MAX_OBSERVATIONS,
            })
        ));
        assert_eq!(registry, before);
        Ok(())
    }
}
