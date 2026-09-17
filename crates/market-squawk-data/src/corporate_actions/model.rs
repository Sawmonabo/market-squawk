//! Public corporate-action policy, evidence, result, and error contracts.

use std::num::{NonZeroU32, NonZeroUsize};

use market_squawk_domain::{
    CorporateActionObservation, EvidenceDigest, InstrumentId, MergerConsideration, Money,
    Timestamp, VenueId, VenueSymbol,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{DatasetId, DatasetManifestRef, DatasetSchemaRegistry, Sha256Digest};

/// Fixed process ceiling for corporate-action candidates in one plan.
pub const MAX_CORPORATE_ACTIONS: usize = 1_000_000;
/// Fixed process ceiling for Rust-visible bytes retained by one plan.
pub const MAX_CORPORATE_ACTION_RETAINED_BYTES: usize = 512 * 1024 * 1024;
/// Schema version for durable, self-validating corporate-action plan recovery material.
pub(super) const CORPORATE_ACTION_PLAN_CODEC_VERSION: u16 = 1;

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

    /// Encodes the exact source records and policy required to reconstruct this plan after a
    /// process restart. Derived steps, exclusions, and hashes are rebuilt rather than trusted
    /// from storage; both persisted identities are retained only as integrity assertions.
    pub fn encode_recovery_material(&self) -> Result<Vec<u8>, CorporateActionError> {
        let records = self
            .admitted
            .iter()
            .chain(self.exclusions.iter().map(|value| &value.record))
            .map(RecoveryRecord::from_record)
            .collect::<Vec<_>>();
        serde_json::to_vec(&RecoveryPlan {
            schema_version: CORPORATE_ACTION_PLAN_CODEC_VERSION,
            adjustment: RecoveryAdjustment::from(self.policy.adjustment),
            policy_version: self.policy.version.get(),
            knowledge_cutoff_unix_nanos: self.knowledge_cutoff.unix_nanos().to_string(),
            valuation_cutoff_unix_nanos: self.valuation_cutoff.unix_nanos().to_string(),
            records,
            content_hash: self.content_hash.bytes(),
            audit_hash: self.audit_hash.bytes(),
        })
        .map_err(|_| CorporateActionError::RecoveryCodec)
    }

    /// Reconstructs a plan exclusively through the normal checked plan builder and verifies both
    /// source-manifest and plan identities. Unknown codec/schema versions, altered source
    /// evidence, or a policy output that no longer hashes identically fail closed.
    pub fn decode_recovery_material(
        bytes: &[u8],
        limits: CorporateActionLimits,
    ) -> Result<Self, CorporateActionError> {
        let wire: RecoveryPlan =
            serde_json::from_slice(bytes).map_err(|_| CorporateActionError::RecoveryCodec)?;
        if wire.schema_version != CORPORATE_ACTION_PLAN_CODEC_VERSION {
            return Err(CorporateActionError::RecoveryCodec);
        }
        let policy_version =
            NonZeroU32::new(wire.policy_version).ok_or(CorporateActionError::RecoveryCodec)?;
        let knowledge_cutoff = parse_recovery_timestamp(&wire.knowledge_cutoff_unix_nanos)?;
        let valuation_cutoff = parse_recovery_timestamp(&wire.valuation_cutoff_unix_nanos)?;
        let records = wire
            .records
            .into_iter()
            .map(RecoveryRecord::into_record)
            .collect::<Result<Vec<_>, _>>()?;
        let plan = Self::try_build(
            CorporateActionPolicy::new(wire.adjustment.into(), policy_version),
            knowledge_cutoff,
            valuation_cutoff,
            records,
            limits,
        )?;
        if plan.content_hash.bytes() != wire.content_hash
            || plan.audit_hash.bytes() != wire.audit_hash
        {
            return Err(CorporateActionError::RecoveryCodec);
        }
        Ok(plan)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryPlan {
    schema_version: u16,
    adjustment: RecoveryAdjustment,
    policy_version: u32,
    knowledge_cutoff_unix_nanos: String,
    valuation_cutoff_unix_nanos: String,
    records: Vec<RecoveryRecord>,
    content_hash: [u8; 32],
    audit_hash: [u8; 32],
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RecoveryAdjustment {
    Raw,
    SplitAdjusted,
    TotalReturn,
}

impl From<CorporateActionAdjustment> for RecoveryAdjustment {
    fn from(value: CorporateActionAdjustment) -> Self {
        match value {
            CorporateActionAdjustment::Raw => Self::Raw,
            CorporateActionAdjustment::SplitAdjusted => Self::SplitAdjusted,
            CorporateActionAdjustment::TotalReturn => Self::TotalReturn,
        }
    }
}

impl From<RecoveryAdjustment> for CorporateActionAdjustment {
    fn from(value: RecoveryAdjustment) -> Self {
        match value {
            RecoveryAdjustment::Raw => Self::Raw,
            RecoveryAdjustment::SplitAdjusted => Self::SplitAdjusted,
            RecoveryAdjustment::TotalReturn => Self::TotalReturn,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryRecord {
    observation: CorporateActionObservation,
    dataset_id: String,
    manifest_version: u64,
    schema_name: String,
    schema_version: u16,
    schema_fingerprint: [u8; 32],
    content_hash: [u8; 32],
    evidence_digest: EvidenceDigest,
}

impl RecoveryRecord {
    fn from_record(record: &CorporateActionRecord) -> Self {
        Self {
            observation: record.observation.clone(),
            dataset_id: record.source_manifest.dataset_id().as_str().to_owned(),
            manifest_version: record.source_manifest.manifest_version(),
            schema_name: record.source_manifest.schema().name().to_owned(),
            schema_version: record.source_manifest.schema_version().get(),
            schema_fingerprint: record.source_manifest.schema().fingerprint(),
            content_hash: record.source_manifest.content_hash().bytes(),
            evidence_digest: record.evidence_digest,
        }
    }

    fn into_record(self) -> Result<CorporateActionRecord, CorporateActionError> {
        let schema = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| CorporateActionError::RecoveryCodec)?;
        if schema.name() != self.schema_name
            || schema.version().get() != self.schema_version
            || schema.fingerprint() != self.schema_fingerprint
        {
            return Err(CorporateActionError::RecoveryCodec);
        }
        let dataset_id = DatasetId::try_from(self.dataset_id.as_str())
            .map_err(|_| CorporateActionError::RecoveryCodec)?;
        let manifest = DatasetManifestRef::try_new_with_schema(
            dataset_id,
            self.manifest_version,
            schema,
            Sha256Digest::new(self.content_hash),
        )
        .map_err(|_| CorporateActionError::RecoveryCodec)?;
        Ok(CorporateActionRecord::new(
            self.observation,
            manifest,
            self.evidence_digest,
        ))
    }
}

fn parse_recovery_timestamp(value: &str) -> Result<Timestamp, CorporateActionError> {
    value
        .parse::<i64>()
        .ok()
        .map(Timestamp::from_unix_nanos)
        .ok_or(CorporateActionError::RecoveryCodec)
}

/// Corporate-action planning, canonicalization, allocation, or retained-size failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CorporateActionError {
    /// Durable plan-recovery material is malformed, unsupported, or fails identity validation.
    #[error("corporate-action recovery material is invalid")]
    RecoveryCodec,
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
