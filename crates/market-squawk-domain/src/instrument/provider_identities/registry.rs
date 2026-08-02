//! Transactional provider-identity aggregate and typed ingest outcomes.

use std::cmp::Ordering;

use serde::{Deserialize, Deserializer, Serialize};

use super::{
    BoundedVec, ProviderIdentityCollection, ProviderIdentityConflict, ProviderIdentityRecord,
    compare_records, normalize_provider_identities, provider_identity_at,
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

        let key_has_conflict = self.has_conflict_for_natural_key(&record);
        let outcome = self.classify_growth(&record);
        if key_has_conflict && outcome == ProviderIdentityIngestOutcome::SupersedingRevisionAppended
        {
            return Err(InstrumentError::ProviderIdentityKeyQuarantined { key: record.key() });
        }

        let current_count = self.canonical_record_count()?;
        let required_capacity = Self::checked_growth_capacity(current_count, reconstruction_limit)?;
        let mut records = self.reconstruction_records(required_capacity)?;
        records.push(record);
        let replacement = Self::try_from_records(records)?;
        *self = replacement;
        Ok(outcome)
    }

    fn coalesce_matching_assertion(
        &mut self,
        incoming: &ProviderIdentityRecord,
    ) -> Result<bool, InstrumentError> {
        if let Ok(index) = self
            .accepted
            .binary_search_by(|existing| compare_record_revision_prefix(existing, incoming))
        {
            let existing = &mut self.accepted[index];
            if !existing.same_assertion(incoming) {
                return Ok(false);
            }
            if existing == incoming {
                return Ok(true);
            }
            let mut replacement = existing.clone();
            replacement.merge_assertion_metadata(incoming)?;
            if replacement != *existing {
                *existing = replacement;
            }
            return Ok(true);
        }

        if let Ok(index) = self
            .conflicts
            .binary_search_by(|conflict| compare_conflict_revision_prefix(conflict, incoming))
        {
            let conflict = &mut self.conflicts[index];
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
        let same_revision = self
            .accepted
            .binary_search_by(|existing| compare_record_revision_prefix(existing, incoming))
            .is_ok()
            || self
                .conflicts
                .binary_search_by(|conflict| compare_conflict_revision_prefix(conflict, incoming))
                .is_ok();
        let same_key = self
            .accepted
            .binary_search_by(|existing| compare_record_natural_key(existing, incoming))
            .is_ok()
            || self
                .conflicts
                .binary_search_by(|conflict| compare_conflict_natural_key(conflict, incoming))
                .is_ok();
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

    fn has_conflict_for_natural_key(&self, incoming: &ProviderIdentityRecord) -> bool {
        self.conflicts
            .binary_search_by(|conflict| compare_conflict_natural_key(conflict, incoming))
            .is_ok()
    }

    fn canonical_record_count(&self) -> Result<usize, InstrumentError> {
        checked_record_count(
            self.accepted.len(),
            self.conflicts
                .iter()
                .map(|conflict| conflict.competing_assertions().len()),
        )
    }

    fn checked_growth_capacity(
        current_count: usize,
        requested_limit: usize,
    ) -> Result<usize, InstrumentError> {
        let limit = requested_limit.min(Self::MAX_RECONSTRUCTION_RECORDS);
        if current_count >= limit {
            return Err(reconstruction_capacity_error(limit));
        }
        current_count
            .checked_add(1)
            .ok_or_else(|| reconstruction_capacity_error(limit))
    }

    fn reconstruction_records(
        &self,
        required_capacity: usize,
    ) -> Result<Vec<ProviderIdentityRecord>, InstrumentError> {
        record_reconstruction_build();
        let mut records = Vec::with_capacity(required_capacity);
        for record in self.assertions() {
            if records.len() == Self::MAX_RECONSTRUCTION_RECORDS {
                return Err(reconstruction_capacity_error(
                    Self::MAX_RECONSTRUCTION_RECORDS,
                ));
            }
            records.push(record.clone());
        }
        Ok(records)
    }
}

fn checked_record_count(
    accepted_count: usize,
    conflict_counts: impl IntoIterator<Item = usize>,
) -> Result<usize, InstrumentError> {
    if accepted_count > ProviderIdentityRegistry::MAX_RECONSTRUCTION_RECORDS {
        return Err(reconstruction_capacity_error(
            ProviderIdentityRegistry::MAX_RECONSTRUCTION_RECORDS,
        ));
    }
    let mut count = accepted_count;
    for conflict_count in conflict_counts {
        count = count.checked_add(conflict_count).ok_or_else(|| {
            reconstruction_capacity_error(ProviderIdentityRegistry::MAX_RECONSTRUCTION_RECORDS)
        })?;
        if count > ProviderIdentityRegistry::MAX_RECONSTRUCTION_RECORDS {
            return Err(reconstruction_capacity_error(
                ProviderIdentityRegistry::MAX_RECONSTRUCTION_RECORDS,
            ));
        }
    }
    Ok(count)
}

fn reconstruction_capacity_error(max: usize) -> InstrumentError {
    InstrumentError::ProviderIdentityCapacityExceeded {
        collection: ProviderIdentityCollection::ReconstructionRecords,
        max,
    }
}

fn compare_record_revision_prefix(
    existing: &ProviderIdentityRecord,
    incoming: &ProviderIdentityRecord,
) -> Ordering {
    record_revision_lookup_comparison();
    compare_record_natural_key(existing, incoming).then_with(|| {
        existing
            .metadata_revision()
            .as_source_identifier()
            .cmp(incoming.metadata_revision().as_source_identifier())
    })
}

fn compare_conflict_revision_prefix(
    existing: &ProviderIdentityConflict,
    incoming: &ProviderIdentityRecord,
) -> Ordering {
    record_revision_lookup_comparison();
    compare_conflict_natural_key(existing, incoming).then_with(|| {
        existing
            .metadata_revision()
            .as_source_identifier()
            .cmp(incoming.metadata_revision().as_source_identifier())
    })
}

fn compare_record_natural_key(
    existing: &ProviderIdentityRecord,
    incoming: &ProviderIdentityRecord,
) -> Ordering {
    existing
        .source_id()
        .cmp(incoming.source_id())
        .then_with(|| {
            existing
                .provider_instrument_id()
                .cmp(incoming.provider_instrument_id())
        })
}

fn compare_conflict_natural_key(
    existing: &ProviderIdentityConflict,
    incoming: &ProviderIdentityRecord,
) -> Ordering {
    existing
        .key()
        .source_id()
        .cmp(incoming.source_id())
        .then_with(|| {
            existing
                .key()
                .provider_instrument_id()
                .cmp(incoming.provider_instrument_id())
        })
}

#[cfg(test)]
std::thread_local! {
    static REVISION_LOOKUP_COMPARISONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static RECONSTRUCTION_BUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn record_revision_lookup_comparison() {
    REVISION_LOOKUP_COMPARISONS.with(|count| count.set(count.get().saturating_add(1)));
}

#[cfg(not(test))]
const fn record_revision_lookup_comparison() {}

#[cfg(test)]
fn record_reconstruction_build() {
    RECONSTRUCTION_BUILDS.with(|count| count.set(count.get().saturating_add(1)));
}

#[cfg(not(test))]
const fn record_reconstruction_build() {}

#[cfg(test)]
fn reset_registry_test_probes() {
    REVISION_LOOKUP_COMPARISONS.with(|count| count.set(0));
    RECONSTRUCTION_BUILDS.with(|count| count.set(0));
}

#[cfg(test)]
fn revision_lookup_comparison_count() -> usize {
    REVISION_LOOKUP_COMPARISONS.with(std::cell::Cell::get)
}

#[cfg(test)]
fn reconstruction_build_count() -> usize {
    RECONSTRUCTION_BUILDS.with(std::cell::Cell::get)
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
#[path = "registry/tests.rs"]
mod tests;
