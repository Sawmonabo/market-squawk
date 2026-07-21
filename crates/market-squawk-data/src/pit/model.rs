//! Point-in-time policy, cutoffs, limits, candidates, and natural-family keys.

use std::fmt;
use std::num::NonZeroU32;

use market_squawk_domain::{
    InstrumentId, ResearchContext, ResearchObservation, ResearchTemporalCoordinate, RevisionNumber,
    SourceId, SourceIdentifier, Timestamp,
};

use super::PointInTimeError;
use crate::DatasetManifestRef;

/// Canonical identity schema used by this selector release.
pub const POINT_IN_TIME_IDENTITY_SCHEMA_VERSION: u16 = 1;
/// Fixed process ceiling for candidates examined by one selection.
pub const MAX_POINT_IN_TIME_CANDIDATES: usize = 1_000_000;
/// Fixed process ceiling for distinct natural-identity families.
pub const MAX_POINT_IN_TIME_FAMILIES: usize = 1_000_000;
/// Fixed process ceiling for divergent same-revision conflict groups.
pub const MAX_POINT_IN_TIME_CONFLICTS: usize = 100_000;
/// Fixed process ceiling for usable result rows.
pub const MAX_POINT_IN_TIME_RESULT_ROWS: usize = 1_000_000;
/// Fixed process ceiling for selector-owned peak retained bytes.
pub const MAX_POINT_IN_TIME_RETAINED_BYTES: usize = 512 * 1024 * 1024;

const CONSERVATIVE_POLICY_VERSION: u32 = 1;

/// Revision history exposed by one explicit point-in-time policy.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PointInTimeRevisionMode {
    /// Retain only the highest revision that is current and knowable at the cutoff.
    LatestKnown,
    /// Retain every knowable revision and label its supersession state explicitly.
    AllKnown,
}

/// Closed versioned point-in-time policy.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PointInTimePolicy {
    version: NonZeroU32,
    revision_mode: PointInTimeRevisionMode,
}

impl PointInTimePolicy {
    /// Constructs a supported policy version with explicit revision-history behavior.
    pub fn try_new(
        version: NonZeroU32,
        revision_mode: PointInTimeRevisionMode,
    ) -> Result<Self, PointInTimeError<'static>> {
        if version.get() != CONSERVATIVE_POLICY_VERSION {
            Err(PointInTimeError::UnsupportedPolicyVersion {
                found: version.get(),
            })
        } else {
            Ok(Self {
                version,
                revision_mode,
            })
        }
    }

    /// Returns the explicit semantic policy version.
    pub const fn version(self) -> NonZeroU32 {
        self.version
    }

    /// Returns the policy's revision-history treatment.
    pub const fn revision_mode(self) -> PointInTimeRevisionMode {
        self.revision_mode
    }
}

/// Explicit caller bounds for one point-in-time selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PointInTimeLimits {
    max_candidates: usize,
    max_families: usize,
    max_conflicts: usize,
    max_result_rows: usize,
    max_retained_bytes: usize,
}

impl PointInTimeLimits {
    /// Constructs nonzero caller bounds within fixed process ceilings.
    pub fn try_new(
        max_candidates: usize,
        max_families: usize,
        max_conflicts: usize,
        max_result_rows: usize,
        max_retained_bytes: usize,
    ) -> Result<Self, PointInTimeError<'static>> {
        if max_candidates == 0
            || max_candidates > MAX_POINT_IN_TIME_CANDIDATES
            || max_families == 0
            || max_families > MAX_POINT_IN_TIME_FAMILIES
            || max_conflicts == 0
            || max_conflicts > MAX_POINT_IN_TIME_CONFLICTS
            || max_result_rows == 0
            || max_result_rows > MAX_POINT_IN_TIME_RESULT_ROWS
            || max_retained_bytes == 0
            || max_retained_bytes > MAX_POINT_IN_TIME_RETAINED_BYTES
        {
            Err(PointInTimeError::InvalidLimits)
        } else {
            Ok(Self {
                max_candidates,
                max_families,
                max_conflicts,
                max_result_rows,
                max_retained_bytes,
            })
        }
    }

    /// Returns the maximum candidates examined.
    pub const fn max_candidates(self) -> usize {
        self.max_candidates
    }

    /// Returns the maximum distinct natural-identity families.
    pub const fn max_families(self) -> usize {
        self.max_families
    }

    /// Returns the maximum divergent same-revision conflict groups.
    pub const fn max_conflicts(self) -> usize {
        self.max_conflicts
    }

    /// Returns the maximum usable records retained.
    pub const fn max_result_rows(self) -> usize {
        self.max_result_rows
    }

    /// Returns the maximum selector-owned peak retained bytes.
    pub const fn max_retained_bytes(self) -> usize {
        self.max_retained_bytes
    }
}

/// Immutable point-in-time cutoffs and policy for one selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointInTimeRequest {
    policy: PointInTimePolicy,
    as_of: Timestamp,
    publication_cutoff: Option<ResearchTemporalCoordinate>,
    effective_cutoff: ResearchTemporalCoordinate,
    label_cutoff: Option<ResearchTemporalCoordinate>,
    limits: PointInTimeLimits,
}

impl PointInTimeRequest {
    /// Constructs a precision-preserving point-in-time request.
    ///
    /// Without `label_cutoff`, effective coordinates must be at or before `effective_cutoff`.
    /// With `label_cutoff`, they must be in `(effective_cutoff, label_cutoff]`.
    pub fn try_new(
        policy: PointInTimePolicy,
        as_of: Timestamp,
        publication_cutoff: Option<ResearchTemporalCoordinate>,
        effective_cutoff: ResearchTemporalCoordinate,
        label_cutoff: Option<ResearchTemporalCoordinate>,
        limits: PointInTimeLimits,
    ) -> Result<Self, PointInTimeError<'static>> {
        if label_cutoff.as_ref().is_some_and(|label| {
            !matches!(
                label.partial_cmp(&effective_cutoff),
                Some(std::cmp::Ordering::Greater)
            )
        }) {
            return Err(PointInTimeError::InvalidLabelWindow);
        }
        Ok(Self {
            policy,
            as_of,
            publication_cutoff,
            effective_cutoff,
            label_cutoff,
            limits,
        })
    }

    /// Returns the exact versioned policy.
    pub const fn policy(&self) -> PointInTimePolicy {
        self.policy
    }

    /// Returns the exact knowledge cutoff.
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns the optional same-precision publication predicate.
    pub const fn publication_cutoff(&self) -> Option<&ResearchTemporalCoordinate> {
        self.publication_cutoff.as_ref()
    }

    /// Returns the feature/effective boundary without fabricating precision.
    pub const fn effective_cutoff(&self) -> &ResearchTemporalCoordinate {
        &self.effective_cutoff
    }

    /// Returns the optional label-window upper boundary.
    pub const fn label_cutoff(&self) -> Option<&ResearchTemporalCoordinate> {
        self.label_cutoff.as_ref()
    }

    /// Returns all caller-selected bounds.
    pub const fn limits(&self) -> PointInTimeLimits {
        self.limits
    }
}

/// Immutable source observation bound to its exact analytical generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointInTimeCandidate {
    observation: ResearchObservation,
    source_manifest: DatasetManifestRef,
}

impl PointInTimeCandidate {
    /// Binds an immutable canonical observation to one exact full manifest reference.
    pub const fn new(
        observation: ResearchObservation,
        source_manifest: DatasetManifestRef,
    ) -> Self {
        Self {
            observation,
            source_manifest,
        }
    }

    /// Returns the immutable canonical source observation.
    pub const fn observation(&self) -> &ResearchObservation {
        &self.observation
    }

    /// Returns the exact source dataset generation, schema, and content identity.
    pub const fn source_manifest(&self) -> &DatasetManifestRef {
        &self.source_manifest
    }

    /// Returns the one-based source revision.
    pub const fn revision(&self) -> RevisionNumber {
        observation_context(&self.observation).time().revision()
    }

    /// Returns the variant-specific stable natural-identity family key.
    pub fn family_key(&self) -> Result<ObservationFamilyKey, PointInTimeError<'static>> {
        ObservationFamilyKey::from_candidate(self)
    }
}

/// Variant-specific stable observation identity excluding payload and revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservationFamilyKey {
    Filing {
        source_id: SourceId,
        instrument_id: InstrumentId,
        accession: SourceIdentifier,
    },
    Fundamental {
        source_id: SourceId,
        instrument_id: InstrumentId,
        source_record: SourceIdentifier,
        concept: SourceIdentifier,
        unit: SourceIdentifier,
        effective: ResearchTemporalCoordinate,
    },
    Macro {
        source_id: SourceId,
        series: SourceIdentifier,
        effective: ResearchTemporalCoordinate,
    },
    PortfolioPosition {
        source_id: SourceId,
        instrument_id: InstrumentId,
        account_id: SourceIdentifier,
        effective: ResearchTemporalCoordinate,
    },
    Transaction {
        source_id: SourceId,
        account_id: SourceIdentifier,
        source_record_id: SourceIdentifier,
    },
    CorporateAction {
        source_id: SourceId,
        instrument_id: InstrumentId,
        source_record: SourceIdentifier,
    },
    AlternativeData {
        source_id: SourceId,
        instrument_id: Option<InstrumentId>,
        source_record: SourceIdentifier,
        dataset: SourceIdentifier,
        field: SourceIdentifier,
        effective: ResearchTemporalCoordinate,
    },
}

impl ObservationFamilyKey {
    fn from_candidate(candidate: &PointInTimeCandidate) -> Result<Self, PointInTimeError<'static>> {
        let context = observation_context(candidate.observation());
        let provenance = context.provenance();
        let source_id = provenance.source_id().clone();
        let effective = context.time().effective().clone();
        let required_instrument = || {
            provenance
                .instrument_id()
                .ok_or(PointInTimeError::CanonicalEncoding)
        };
        match candidate.observation() {
            ResearchObservation::Filing(value) => Ok(Self::Filing {
                source_id,
                instrument_id: required_instrument()?,
                accession: value.accession().clone(),
            }),
            ResearchObservation::Fundamental(value) => Ok(Self::Fundamental {
                source_id,
                instrument_id: required_instrument()?,
                source_record: provenance.source_identifier().clone(),
                concept: value.concept().clone(),
                unit: value.unit().clone(),
                effective,
            }),
            ResearchObservation::Macro(value) => Ok(Self::Macro {
                source_id,
                series: value.series().clone(),
                effective,
            }),
            ResearchObservation::PortfolioPosition(value) => Ok(Self::PortfolioPosition {
                source_id,
                instrument_id: required_instrument()?,
                account_id: value.account_id().clone(),
                effective,
            }),
            ResearchObservation::Transaction(value) => Ok(Self::Transaction {
                source_id,
                account_id: value.account_id().clone(),
                source_record_id: value.source_record_id().clone(),
            }),
            ResearchObservation::CorporateAction(_) => Ok(Self::CorporateAction {
                source_id,
                instrument_id: required_instrument()?,
                source_record: provenance.source_identifier().clone(),
            }),
            ResearchObservation::AlternativeData(value) => Ok(Self::AlternativeData {
                source_id,
                instrument_id: provenance.instrument_id(),
                source_record: provenance.source_identifier().clone(),
                dataset: value.dataset().clone(),
                field: value.field().clone(),
                effective,
            }),
        }
    }
}

impl fmt::Display for ObservationFamilyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Filing { .. } => "filing",
            Self::Fundamental { .. } => "fundamental",
            Self::Macro { .. } => "macro",
            Self::PortfolioPosition { .. } => "portfolio_position",
            Self::Transaction { .. } => "transaction",
            Self::CorporateAction { .. } => "corporate_action",
            Self::AlternativeData { .. } => "alternative_data",
        })
    }
}

pub(super) const fn observation_context(observation: &ResearchObservation) -> &ResearchContext {
    match observation {
        ResearchObservation::Filing(value) => value.context(),
        ResearchObservation::Fundamental(value) => value.context(),
        ResearchObservation::Macro(value) => value.context(),
        ResearchObservation::PortfolioPosition(value) => value.context(),
        ResearchObservation::Transaction(value) => value.context(),
        ResearchObservation::CorporateAction(value) => value.context(),
        ResearchObservation::AlternativeData(value) => value.context(),
    }
}
