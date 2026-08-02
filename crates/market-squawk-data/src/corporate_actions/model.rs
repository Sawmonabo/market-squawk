//! Public corporate-action policy, evidence, result, and error contracts.

use std::num::{NonZeroU32, NonZeroUsize};

use market_squawk_domain::{
    CorporateActionObservation, EvidenceDigest, InstrumentId, MergerConsideration, Money,
    Timestamp, VenueId, VenueSymbol,
};
use thiserror::Error;

use crate::{DatasetManifestRef, Sha256Digest};

/// Fixed process ceiling for corporate-action candidates in one plan.
pub const MAX_CORPORATE_ACTIONS: usize = 1_000_000;
/// Fixed process ceiling for Rust-visible bytes retained by one plan.
pub const MAX_CORPORATE_ACTION_RETAINED_BYTES: usize = 512 * 1024 * 1024;

/// Closed economic treatment applied by one versioned adjustment policy.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CorporateActionAdjustment {
    /// Retain admitted source actions without producing economic adjustments.
    Raw,
    /// Produce exact price and quantity factors for splits only.
    SplitAdjusted,
    /// Produce split, exact distribution, non-scalar, and lifecycle steps.
    TotalReturn,
}

/// Explicit versioned corporate-action treatment.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CorporateActionPolicy {
    adjustment: CorporateActionAdjustment,
    version: NonZeroU32,
}

impl CorporateActionPolicy {
    /// Constructs a policy with an explicit nonzero semantic version.
    pub const fn new(adjustment: CorporateActionAdjustment, version: NonZeroU32) -> Self {
        Self {
            adjustment,
            version,
        }
    }

    /// Returns the economic treatment.
    pub const fn adjustment(self) -> CorporateActionAdjustment {
        self.adjustment
    }

    /// Returns the explicit policy version.
    pub const fn version(self) -> NonZeroU32 {
        self.version
    }
}

/// Exact positive ratio used by an adjustment step.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AdjustmentRatio {
    numerator: NonZeroU32,
    denominator: NonZeroU32,
}

impl AdjustmentRatio {
    /// Constructs an exact ratio from already validated nonzero terms.
    pub const fn new(numerator: NonZeroU32, denominator: NonZeroU32) -> Self {
        Self {
            numerator,
            denominator,
        }
    }

    /// Returns the exact numerator.
    pub const fn numerator(self) -> NonZeroU32 {
        self.numerator
    }

    /// Returns the exact denominator.
    pub const fn denominator(self) -> NonZeroU32 {
        self.denominator
    }
}

/// Immutable source observation with its exact dataset-generation and evidence lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorporateActionRecord {
    pub(super) observation: CorporateActionObservation,
    pub(super) source_manifest: DatasetManifestRef,
    pub(super) evidence_digest: EvidenceDigest,
}

impl CorporateActionRecord {
    /// Binds one validated source observation to immutable manifest and evidence identities.
    pub const fn new(
        observation: CorporateActionObservation,
        source_manifest: DatasetManifestRef,
        evidence_digest: EvidenceDigest,
    ) -> Self {
        Self {
            observation,
            source_manifest,
            evidence_digest,
        }
    }

    /// Returns the source observation without applying or mutating it.
    pub const fn observation(&self) -> &CorporateActionObservation {
        &self.observation
    }

    /// Returns the complete immutable source dataset generation.
    pub const fn source_manifest(&self) -> &DatasetManifestRef {
        &self.source_manifest
    }

    /// Returns the exact source-observation evidence digest.
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Fail-closed reason a source action was not admitted to the plan.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CorporateActionExclusionReason {
    /// Conservative source availability is later than the requested knowledge cutoff.
    FutureAvailability,
    /// Source availability was inferred and is not authoritative point-in-time evidence.
    InferredAvailability,
    /// Historical availability cannot be established.
    UnknownAvailability,
    /// Effective time is later than the requested valuation cutoff.
    FutureEffectiveTime,
    /// The source retained a date or period but no exact effective instant.
    AmbiguousEffectiveTime,
}

/// Complete excluded source record and its typed admission failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorporateActionExclusion {
    pub(super) record: CorporateActionRecord,
    pub(super) reason: CorporateActionExclusionReason,
}

impl CorporateActionExclusion {
    /// Returns the complete excluded record and immutable lineage.
    pub const fn record(&self) -> &CorporateActionRecord {
        &self.record
    }

    /// Returns the fail-closed exclusion reason.
    pub const fn reason(&self) -> CorporateActionExclusionReason {
        self.reason
    }
}

/// One deterministic adjustment or lifecycle operation referencing an admitted record by index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdjustmentStep {
    /// Exact reciprocal price and direct quantity factors for a split.
    Split {
        /// Index into [`CorporateActionPlan::admitted`].
        admitted_index: usize,
        /// Old-price multiplier, `old_units / new_units`.
        price_factor: AdjustmentRatio,
        /// Old-quantity multiplier, `new_units / old_units`.
        quantity_factor: AdjustmentRatio,
    },
    /// Exact dividend cash flow per entitled unit.
    CashDividend {
        /// Index into [`CorporateActionPlan::admitted`].
        admitted_index: usize,
        /// Exact distribution amount and currency.
        amount: Money,
    },
    /// Exact returned-capital cash flow per entitled unit.
    ReturnOfCapital {
        /// Index into [`CorporateActionPlan::admitted`].
        admitted_index: usize,
        /// Exact returned amount and currency.
        amount: Money,
    },
    /// Non-scalar distribution of a distinct instrument.
    Spinoff {
        /// Index into [`CorporateActionPlan::admitted`].
        admitted_index: usize,
        /// Stable identity of the distributed instrument.
        distributed_instrument: InstrumentId,
        /// Distributed units per subject unit.
        distribution_ratio: AdjustmentRatio,
    },
    /// Non-scalar successor transition with complete source-supplied terms.
    Merger {
        /// Index into [`CorporateActionPlan::admitted`].
        admitted_index: usize,
        /// Stable successor identity.
        successor: InstrumentId,
        /// Exact source-supplied stock, cash, or mixed consideration.
        consideration: MergerConsideration,
    },
    /// Terminal instrument lifecycle event; no price or proceeds are invented.
    Delisting {
        /// Index into [`CorporateActionPlan::admitted`].
        admitted_index: usize,
    },
    /// Same-instrument venue-symbol transition; no economic factor is invented.
    SymbolChange {
        /// Index into [`CorporateActionPlan::admitted`].
        admitted_index: usize,
        /// Venue namespace for both symbols.
        venue_id: VenueId,
        /// Prior venue symbol.
        previous: VenueSymbol,
        /// New venue symbol.
        current: VenueSymbol,
    },
}

/// Admitted action whose requested policy cannot resolve complete economics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdjustmentConflict {
    /// A merger was admitted, but its source record omitted consideration terms.
    IncompleteMergerTerms {
        /// Index into [`CorporateActionPlan::admitted`].
        admitted_index: usize,
        /// Stable successor retained from the source record.
        successor: InstrumentId,
    },
}

/// Explicit caller-selected work and retained-memory ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CorporateActionLimits {
    pub(super) max_actions: NonZeroUsize,
    pub(super) max_retained_bytes: NonZeroUsize,
}

impl CorporateActionLimits {
    /// Constructs limits within fixed process ceilings.
    ///
    /// # Errors
    ///
    /// Returns [`CorporateActionError::InvalidLimits`] for excessive limits.
    pub fn try_new(
        max_actions: NonZeroUsize,
        max_retained_bytes: NonZeroUsize,
    ) -> Result<Self, CorporateActionError> {
        if max_actions.get() > MAX_CORPORATE_ACTIONS
            || max_retained_bytes.get() > MAX_CORPORATE_ACTION_RETAINED_BYTES
        {
            Err(CorporateActionError::InvalidLimits)
        } else {
            Ok(Self {
                max_actions,
                max_retained_bytes,
            })
        }
    }

    /// Returns the maximum number of source actions examined.
    pub const fn max_actions(self) -> NonZeroUsize {
        self.max_actions
    }

    /// Returns the maximum bytes retained by the completed plan.
    pub const fn max_retained_bytes(self) -> NonZeroUsize {
        self.max_retained_bytes
    }
}

/// Deterministically ordered point-in-time adjustment plan and complete audit partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorporateActionPlan {
    pub(super) policy: CorporateActionPolicy,
    pub(super) knowledge_cutoff: Timestamp,
    pub(super) valuation_cutoff: Timestamp,
    pub(super) admitted: Vec<CorporateActionRecord>,
    pub(super) exclusions: Vec<CorporateActionExclusion>,
    pub(super) steps: Vec<AdjustmentStep>,
    pub(super) conflicts: Vec<AdjustmentConflict>,
    pub(super) content_hash: Sha256Digest,
    pub(super) audit_hash: Sha256Digest,
    pub(super) retained_bytes: usize,
}

impl CorporateActionPlan {
    /// Returns the exact policy governing this plan.
    pub const fn policy(&self) -> CorporateActionPolicy {
        self.policy
    }

    /// Returns the latest knowledge time admitted by this plan.
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the latest effective time admitted by this plan.
    pub const fn valuation_cutoff(&self) -> Timestamp {
        self.valuation_cutoff
    }

    /// Returns source observations admitted under both cutoffs in canonical order.
    pub fn admitted(&self) -> &[CorporateActionRecord] {
        &self.admitted
    }

    /// Returns all unavailable, future, or temporally ambiguous candidates.
    pub fn exclusions(&self) -> &[CorporateActionExclusion] {
        &self.exclusions
    }

    /// Returns typed economic and lifecycle operations in admitted-record order.
    pub fn steps(&self) -> &[AdjustmentStep] {
        &self.steps
    }

    /// Returns typed unresolved economics in admitted-record order.
    pub fn conflicts(&self) -> &[AdjustmentConflict] {
        &self.conflicts
    }

    /// Returns the canonical identity of usable admitted content and policy output.
    ///
    /// Excluded future or unavailable candidates do not affect this identity.
    pub const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    /// Returns the canonical identity of the complete admission and exclusion audit.
    pub const fn audit_hash(&self) -> Sha256Digest {
        self.audit_hash
    }

    /// Returns checked Rust-visible bytes retained by this plan.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// Corporate-action planning, canonicalization, allocation, or retained-size failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CorporateActionError {
    /// Caller-selected limits exceed fixed process ceilings.
    #[error("corporate-action limits exceed fixed process ceilings")]
    InvalidLimits,
    /// Candidate work exceeds the caller-selected bound.
    #[error("corporate-action input has {observed} actions; caller limit is {limit}")]
    ActionLimitExceeded {
        /// Caller-selected action limit.
        limit: usize,
        /// Submitted action count.
        observed: usize,
    },
    /// The retained result would exceed the caller-selected byte ceiling.
    #[error("corporate-action plan requires {required} retained bytes; caller limit is {limit}")]
    RetainedByteLimitExceeded {
        /// Caller-selected retained-byte limit.
        limit: usize,
        /// Checked bytes required by the result.
        required: usize,
    },
    /// Checked retained-size arithmetic exceeded the platform representation.
    #[error("corporate-action retained-size calculation overflow")]
    RetainedSizeOverflow,
    /// A bounded vector or canonical buffer allocation failed.
    #[error("corporate-action bounded allocation failed")]
    AllocationFailed,
    /// A platform-size value cannot be represented by canonical encoding.
    #[error("corporate-action canonical encoding overflow")]
    CanonicalEncodingOverflow,
    /// A validated corporate-action observation unexpectedly lacked an instrument identity.
    #[error("corporate-action observation lacks its required instrument identity")]
    MissingInstrument,
}
