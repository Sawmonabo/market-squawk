//! Deterministic, evidence-preserving provider identity normalization.

use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use super::{EffectiveInterval, InstrumentError};
use crate::{
    InstrumentId, MetadataRevision, PayloadHashAlgorithm, PayloadReference, ProviderInstrumentId,
    SourceId, Timestamp,
};

/// Natural namespace key for a provider-native instrument identity.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderIdentityKey {
    source_id: SourceId,
    provider_instrument_id: ProviderInstrumentId,
}

impl ProviderIdentityKey {
    /// Constructs a source-qualified provider identity key.
    pub const fn new(source_id: SourceId, provider_instrument_id: ProviderInstrumentId) -> Self {
        Self {
            source_id,
            provider_instrument_id,
        }
    }

    /// Returns the provider namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the provider-native instrument identity.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }
}

/// Immutable evidence authorizing a provider metadata revision to replace its predecessor.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderIdentitySupersession {
    predecessor: MetadataRevision,
    evidence: PayloadReference,
}

impl ProviderIdentitySupersession {
    /// Constructs an evidence-backed predecessor edge.
    pub const fn new(predecessor: MetadataRevision, evidence: PayloadReference) -> Self {
        Self {
            predecessor,
            evidence,
        }
    }

    /// Returns the exact predecessor revision.
    pub const fn predecessor(&self) -> &MetadataRevision {
        &self.predecessor
    }

    /// Returns immutable evidence for the revision transition.
    pub const fn evidence(&self) -> &PayloadReference {
        &self.evidence
    }
}

/// A normalized provider-to-internal-instrument assertion with local observations kept separate.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct ProviderIdentityRecord {
    instrument_id: InstrumentId,
    source_id: SourceId,
    provider_instrument_id: ProviderInstrumentId,
    source_reference: PayloadReference,
    source_timestamp: Option<Timestamp>,
    observation_timestamps: Vec<Timestamp>,
    metadata_revision: MetadataRevision,
    validity: EffectiveInterval,
    supersedes: Option<ProviderIdentitySupersession>,
}

/// Complete immutable evidence and one local observation of a provider mapping assertion.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProviderIdentityRecordInput {
    /// Stable internal instrument identity asserted by the provider metadata.
    pub instrument_id: InstrumentId,
    /// Provider/source namespace in which the ID is meaningful.
    pub source_id: SourceId,
    /// Source-native instrument identity.
    pub provider_instrument_id: ProviderInstrumentId,
    /// Immutable reference to the exact source object containing the assertion.
    pub source_reference: PayloadReference,
    /// Source-authored timestamp when supplied.
    pub source_timestamp: Option<Timestamp>,
    /// Local time this exact assertion was observed.
    pub observed_at: Timestamp,
    /// Immutable source metadata revision.
    pub metadata_revision: MetadataRevision,
    /// Half-open interval claimed by this revision.
    pub validity: EffectiveInterval,
    /// Explicit predecessor evidence; absent only for the first revision of a natural key.
    #[serde(default)]
    pub supersedes: Option<ProviderIdentitySupersession>,
}

impl ProviderIdentityRecord {
    /// Constructs one assertion carrying a single local observation.
    pub fn new(input: ProviderIdentityRecordInput) -> Self {
        Self {
            instrument_id: input.instrument_id,
            source_id: input.source_id,
            provider_instrument_id: input.provider_instrument_id,
            source_reference: input.source_reference,
            source_timestamp: input.source_timestamp,
            observation_timestamps: vec![input.observed_at],
            metadata_revision: input.metadata_revision,
            validity: input.validity,
            supersedes: input.supersedes,
        }
    }

    /// Returns the stable internal instrument identity asserted by the source.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the provider/source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the source-native instrument identity.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the natural provider identity key.
    pub fn key(&self) -> ProviderIdentityKey {
        ProviderIdentityKey::new(self.source_id.clone(), self.provider_instrument_id.clone())
    }

    /// Returns the immutable source-object evidence.
    pub const fn source_reference(&self) -> &PayloadReference {
        &self.source_reference
    }

    /// Returns the source-authored timestamp when supplied.
    pub const fn source_timestamp(&self) -> Option<Timestamp> {
        self.source_timestamp
    }

    /// Returns the first local observation timestamp retained for compatibility.
    pub fn observed_at(&self) -> Timestamp {
        self.observation_timestamps[0]
    }

    /// Returns every unique local observation timestamp in ascending order.
    pub fn observation_timestamps(&self) -> &[Timestamp] {
        &self.observation_timestamps
    }

    /// Returns the immutable source metadata revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the half-open effective interval.
    pub const fn validity(&self) -> EffectiveInterval {
        self.validity
    }

    /// Returns the evidenced predecessor edge, if this is a revised assertion.
    pub const fn supersedes(&self) -> Option<&ProviderIdentitySupersession> {
        self.supersedes.as_ref()
    }

    fn same_assertion(&self, other: &Self) -> bool {
        self.instrument_id == other.instrument_id
            && self.source_id == other.source_id
            && self.provider_instrument_id == other.provider_instrument_id
            && self.source_reference == other.source_reference
            && self.source_timestamp == other.source_timestamp
            && self.metadata_revision == other.metadata_revision
            && self.validity == other.validity
            && self.supersedes == other.supersedes
    }

    fn merge_observations(&mut self, other: &Self) {
        self.observation_timestamps
            .extend_from_slice(&other.observation_timestamps);
        self.observation_timestamps.sort_unstable();
        self.observation_timestamps.dedup();
    }

    fn is_effective_at(&self, at: Timestamp) -> bool {
        self.validity.starts_at() <= at && self.validity.ends_at().is_none_or(|end| at < end)
    }
}

impl fmt::Display for ProviderIdentityRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{}",
            self.source_id, self.provider_instrument_id
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderIdentityRecordWire {
    instrument_id: InstrumentId,
    source_id: SourceId,
    provider_instrument_id: ProviderInstrumentId,
    source_reference: PayloadReference,
    source_timestamp: Option<Timestamp>,
    observation_timestamps: Vec<Timestamp>,
    metadata_revision: MetadataRevision,
    validity: EffectiveInterval,
    supersedes: Option<ProviderIdentitySupersession>,
}

impl<'de> Deserialize<'de> for ProviderIdentityRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderIdentityRecordWire::deserialize(deserializer)?;
        if wire.observation_timestamps.is_empty() {
            return Err(serde::de::Error::custom(
                "provider identity requires at least one observation timestamp",
            ));
        }
        let mut observations = wire.observation_timestamps;
        observations.sort_unstable();
        observations.dedup();
        Ok(Self {
            instrument_id: wire.instrument_id,
            source_id: wire.source_id,
            provider_instrument_id: wire.provider_instrument_id,
            source_reference: wire.source_reference,
            source_timestamp: wire.source_timestamp,
            observation_timestamps: observations,
            metadata_revision: wire.metadata_revision,
            validity: wire.validity,
            supersedes: wire.supersedes,
        })
    }
}

/// Why competing provider identity assertions were removed from active resolution.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderIdentityConflictReason {
    /// One natural key and metadata revision carried divergent immutable assertions.
    SameRevisionDivergence,
}

/// All competing assertions quarantined for one natural key and metadata revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderIdentityConflict {
    key: ProviderIdentityKey,
    metadata_revision: MetadataRevision,
    reason: ProviderIdentityConflictReason,
    competing_assertions: Vec<ProviderIdentityRecord>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderIdentityConflictWire {
    key: ProviderIdentityKey,
    metadata_revision: MetadataRevision,
    reason: ProviderIdentityConflictReason,
    competing_assertions: Vec<ProviderIdentityRecord>,
}

impl<'de> Deserialize<'de> for ProviderIdentityConflict {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderIdentityConflictWire::deserialize(deserializer)?;
        let (accepted, mut conflicts) = normalize_provider_identities(wire.competing_assertions)
            .map_err(serde::de::Error::custom)?;
        if !accepted.is_empty() || conflicts.len() != 1 {
            return Err(serde::de::Error::custom(
                "provider identity conflict must contain divergent same-revision assertions",
            ));
        }
        let conflict = conflicts.remove(0);
        if conflict.key != wire.key
            || conflict.metadata_revision != wire.metadata_revision
            || conflict.reason != wire.reason
        {
            return Err(serde::de::Error::custom(
                "provider identity conflict header does not match its evidence",
            ));
        }
        Ok(conflict)
    }
}

impl ProviderIdentityConflict {
    /// Returns the source-qualified natural key.
    pub const fn key(&self) -> &ProviderIdentityKey {
        &self.key
    }

    /// Returns the metadata revision whose evidence diverged.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the typed quarantine reason.
    pub const fn reason(&self) -> ProviderIdentityConflictReason {
        self.reason
    }

    /// Returns every normalized competing assertion in deterministic order.
    pub fn competing_assertions(&self) -> &[ProviderIdentityRecord] {
        &self.competing_assertions
    }
}

pub(super) fn normalize_provider_identities(
    mut records: Vec<ProviderIdentityRecord>,
) -> Result<(Vec<ProviderIdentityRecord>, Vec<ProviderIdentityConflict>), InstrumentError> {
    records.sort_by(compare_records);
    let mut accepted = Vec::new();
    let mut conflicts = Vec::new();
    let mut cursor = 0;
    while cursor < records.len() {
        let end = records[cursor..]
            .iter()
            .position(|candidate| !same_revision_group(&records[cursor], candidate))
            .map_or(records.len(), |offset| cursor + offset);
        let mut variants: Vec<ProviderIdentityRecord> = Vec::new();
        for candidate in &records[cursor..end] {
            if let Some(existing) = variants
                .iter_mut()
                .find(|existing| existing.same_assertion(candidate))
            {
                existing.merge_observations(candidate);
            } else {
                variants.push(candidate.clone());
            }
        }
        variants.sort_by(compare_records);
        if variants.len() == 1 {
            accepted.push(variants.remove(0));
        } else {
            conflicts.push(ProviderIdentityConflict {
                key: records[cursor].key(),
                metadata_revision: records[cursor].metadata_revision.clone(),
                reason: ProviderIdentityConflictReason::SameRevisionDivergence,
                competing_assertions: variants,
            });
        }
        cursor = end;
    }
    accepted.sort_by(compare_records);
    validate_revision_graphs(&accepted)?;
    Ok((accepted, conflicts))
}

pub(super) fn provider_identity_at<'a>(
    accepted: &'a [ProviderIdentityRecord],
    conflicts: &[ProviderIdentityConflict],
    source_id: &SourceId,
    provider_instrument_id: &ProviderInstrumentId,
    at: Timestamp,
) -> Option<&'a ProviderIdentityRecord> {
    if conflicts.iter().any(|conflict| {
        conflict.key().source_id() == source_id
            && conflict.key().provider_instrument_id() == provider_instrument_id
    }) {
        return None;
    }
    accepted.iter().find(|record| {
        record.source_id() == source_id
            && record.provider_instrument_id() == provider_instrument_id
            && record.is_effective_at(at)
    })
}

fn validate_revision_graphs(records: &[ProviderIdentityRecord]) -> Result<(), InstrumentError> {
    let mut cursor = 0;
    while cursor < records.len() {
        let end = records[cursor..]
            .iter()
            .position(|candidate| !same_natural_key(&records[cursor], candidate))
            .map_or(records.len(), |offset| cursor + offset);
        validate_revision_graph(&records[cursor..end])?;
        cursor = end;
    }
    Ok(())
}

fn validate_revision_graph(records: &[ProviderIdentityRecord]) -> Result<(), InstrumentError> {
    for record in records {
        if let Some(edge) = record.supersedes() {
            if edge.predecessor() == record.metadata_revision() {
                return Err(InstrumentError::ProviderIdentitySupersessionCycle {
                    revision: record.metadata_revision().clone(),
                });
            }
            if !records
                .iter()
                .any(|candidate| candidate.metadata_revision() == edge.predecessor())
            {
                return Err(InstrumentError::MissingProviderIdentityPredecessor {
                    revision: record.metadata_revision().clone(),
                    predecessor: edge.predecessor().clone(),
                });
            }
        }
    }
    for start in records {
        let mut current = start;
        for _ in 0..records.len() {
            let Some(edge) = current.supersedes() else {
                break;
            };
            let Some(predecessor) = records
                .iter()
                .find(|candidate| candidate.metadata_revision() == edge.predecessor())
            else {
                break;
            };
            current = predecessor;
        }
        if current.supersedes().is_some() {
            return Err(InstrumentError::ProviderIdentitySupersessionCycle {
                revision: start.metadata_revision().clone(),
            });
        }
    }
    let roots: Vec<_> = records
        .iter()
        .filter(|record| record.supersedes().is_none())
        .collect();
    if roots.len() != 1 {
        let revision = roots
            .get(1)
            .copied()
            .unwrap_or(&records[0])
            .metadata_revision()
            .clone();
        return Err(InstrumentError::MissingProviderIdentitySupersession { revision });
    }
    for predecessor in records {
        let successors: Vec<_> = records
            .iter()
            .filter(|candidate| {
                candidate
                    .supersedes()
                    .is_some_and(|edge| edge.predecessor() == predecessor.metadata_revision())
            })
            .collect();
        if successors.len() > 1 {
            return Err(InstrumentError::AmbiguousProviderIdentitySuccessor {
                revision: predecessor.metadata_revision().clone(),
            });
        }
        if let Some(successor) = successors.first().copied() {
            let valid_transition = predecessor
                .validity()
                .ends_at()
                .is_some_and(|end| end <= successor.validity().starts_at());
            if !valid_transition {
                return Err(InstrumentError::InvalidProviderIdentityTransition {
                    predecessor: predecessor.metadata_revision().clone(),
                    successor: successor.metadata_revision().clone(),
                });
            }
        }
    }
    Ok(())
}

fn same_natural_key(left: &ProviderIdentityRecord, right: &ProviderIdentityRecord) -> bool {
    left.source_id == right.source_id && left.provider_instrument_id == right.provider_instrument_id
}

fn same_revision_group(left: &ProviderIdentityRecord, right: &ProviderIdentityRecord) -> bool {
    same_natural_key(left, right) && left.metadata_revision == right.metadata_revision
}

fn compare_records(left: &ProviderIdentityRecord, right: &ProviderIdentityRecord) -> Ordering {
    left.source_id
        .cmp(&right.source_id)
        .then_with(|| {
            left.provider_instrument_id
                .cmp(&right.provider_instrument_id)
        })
        .then_with(|| {
            left.metadata_revision
                .as_source_identifier()
                .cmp(right.metadata_revision.as_source_identifier())
        })
        .then_with(|| left.instrument_id.cmp(&right.instrument_id))
        .then_with(|| compare_payload_reference(&left.source_reference, &right.source_reference))
        .then_with(|| left.source_timestamp.cmp(&right.source_timestamp))
        .then_with(|| left.validity.starts_at().cmp(&right.validity.starts_at()))
        .then_with(|| left.validity.ends_at().cmp(&right.validity.ends_at()))
        .then_with(|| compare_supersession(&left.supersedes, &right.supersedes))
        .then_with(|| {
            left.observation_timestamps
                .cmp(&right.observation_timestamps)
        })
}

fn compare_supersession(
    left: &Option<ProviderIdentitySupersession>,
    right: &Option<ProviderIdentitySupersession>,
) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => left
            .predecessor()
            .as_source_identifier()
            .cmp(right.predecessor().as_source_identifier())
            .then_with(|| compare_payload_reference(left.evidence(), right.evidence())),
    }
}

fn compare_payload_reference(left: &PayloadReference, right: &PayloadReference) -> Ordering {
    match (left, right) {
        (PayloadReference::ContentHash(left), PayloadReference::ContentHash(right)) => {
            payload_algorithm_rank(left.algorithm())
                .cmp(&payload_algorithm_rank(right.algorithm()))
                .then_with(|| left.digest().cmp(&right.digest()))
        }
        (PayloadReference::ContentHash(_), PayloadReference::SourceReference(_)) => Ordering::Less,
        (PayloadReference::SourceReference(_), PayloadReference::ContentHash(_)) => {
            Ordering::Greater
        }
        (PayloadReference::SourceReference(left), PayloadReference::SourceReference(right)) => {
            left.cmp(right)
        }
    }
}

const fn payload_algorithm_rank(algorithm: PayloadHashAlgorithm) -> u8 {
    match algorithm {
        PayloadHashAlgorithm::Sha256 => 0,
        PayloadHashAlgorithm::Blake3 => 1,
    }
}
