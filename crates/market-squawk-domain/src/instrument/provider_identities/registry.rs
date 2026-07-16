//! Transactional provider-identity aggregate and typed ingest outcomes.

use serde::{Deserialize, Deserializer, Serialize};

use super::{
    BoundedVec, ProviderIdentityCollection, ProviderIdentityConflict, ProviderIdentityRecord,
    normalize_provider_identities, provider_identity_at, same_natural_key,
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
    /// The registry is unchanged if capacity or revision-graph validation fails.
    ///
    /// # Errors
    ///
    /// Returns a typed [`InstrumentError`] for capacity or revision-graph violations.
    pub fn ingest(
        &mut self,
        record: ProviderIdentityRecord,
    ) -> Result<ProviderIdentityIngestOutcome, InstrumentError> {
        let key_has_conflict = self.conflicts.iter().any(|conflict| {
            conflict.key().source_id() == record.source_id()
                && conflict.key().provider_instrument_id() == record.provider_instrument_id()
        });
        let revision_exists = self.assertions().any(|existing| {
            same_natural_key(existing, &record)
                && existing.metadata_revision() == record.metadata_revision()
        });
        if key_has_conflict && !revision_exists {
            return Err(InstrumentError::ProviderIdentityKeyQuarantined { key: record.key() });
        }
        let outcome = self.classify_ingest(&record);
        let mut records = self.reconstruction_records()?;
        if records.len() == Self::MAX_RECONSTRUCTION_RECORDS {
            return Err(InstrumentError::ProviderIdentityCapacityExceeded {
                collection: ProviderIdentityCollection::ReconstructionRecords,
                max: Self::MAX_RECONSTRUCTION_RECORDS,
            });
        }
        records.push(record);
        let replacement = Self::try_from_records(records)?;
        *self = replacement;
        Ok(outcome)
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

    fn classify_ingest(&self, incoming: &ProviderIdentityRecord) -> ProviderIdentityIngestOutcome {
        let mut same_key = false;
        let mut same_revision = false;
        for existing in self.assertions() {
            if !same_natural_key(existing, incoming) {
                continue;
            }
            same_key = true;
            if existing.metadata_revision() == incoming.metadata_revision() {
                same_revision = true;
                if existing.same_assertion(incoming) {
                    return ProviderIdentityIngestOutcome::ObservationCoalesced;
                }
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
