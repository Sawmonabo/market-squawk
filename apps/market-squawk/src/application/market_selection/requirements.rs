use market_squawk_domain::{
    AssetClass, DataQuality, DigestAlgorithm, EvidenceDigest, MarketDepth, Timestamp,
};
use sha2::{Digest, Sha256};

use super::receipt::MarketSelectionError;

const MARKET_SELECTION_POLICY_REVISION: u32 = 3;
const MAXIMUM_POLICY_CANDIDATES: usize = 4_096;
const POLICY_CANONICAL_IDENTITY: &[u8] = b"market-squawk.market-selection.v3\0\
strict-before-downgrade\0fewer-downgrades-first\0\
quality=direct-verified,direct-unverified,official-delayed,aggregated,indicative,modeled,estimated,stale,quarantined\0\
depth=order-level,price-level,top-of-book,none\0\
timing=real-time,delayed,end-of-day,historical,stored\0\
coverage=consolidated,multi-venue-partial,single-venue,benchmark,reference,user-owned\0\
freshness=newest-first\0health=healthy,degraded\0budget=not-required,open,interactive-only\0\
tie=provider,product,feed,source,venue,instrument,observation,definition-revision-digest\0\
paper-and-live-execution=real-time-direct-verified-no-downgrade\0\
asset-operation-health-budget-rights-integrity-execution-never-downgrade\0\
fallback-never-inherits-source-state";

/// Product operation for which a market observation is being selected.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum MarketOperation {
    ReferenceLookup,
    SnapshotDisplay,
    StreamDisplay,
    HistoricalRead,
    ResearchAnalysis,
    PortfolioMark,
    ModelInput,
    Backtest,
    PaperDecision,
    AutomatedExecution,
}

impl MarketOperation {
    const fn bit(self) -> u16 {
        1_u16 << (self as u16)
    }

    pub(super) const fn requires_execution_quality(self) -> bool {
        matches!(self, Self::PaperDecision | Self::AutomatedExecution)
    }
}

/// Fixed-space set of product operations supported by a source or rights decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MarketOperationSet(u16);

impl MarketOperationSet {
    /// Constructs a nonempty operation set without heap allocation.
    pub(crate) fn try_new(operations: &[MarketOperation]) -> Result<Self, MarketSelectionError> {
        let mut bits = 0_u16;
        for operation in operations {
            bits |= operation.bit();
        }
        if bits == 0 {
            Err(MarketSelectionError::EmptyOperationSet)
        } else {
            Ok(Self(bits))
        }
    }

    pub(super) const fn empty() -> Self {
        Self(0)
    }

    pub(crate) const fn contains(self, operation: MarketOperation) -> bool {
        self.0 & operation.bit() != 0
    }
}

/// Delivery-time class of an observation, independent of its evidentiary quality.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ObservationTiming {
    RealTime,
    Delayed,
    EndOfDay,
    Historical,
    Stored,
}

impl ObservationTiming {
    const fn bit(self) -> u8 {
        1_u8 << (self as u8)
    }

    pub(super) const fn preference(self) -> u8 {
        match self {
            Self::RealTime => 5,
            Self::Delayed => 4,
            Self::EndOfDay => 3,
            Self::Historical => 2,
            Self::Stored => 1,
        }
    }
}

/// Exact coverage represented by a candidate observation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum MarketCoverage {
    Consolidated,
    MultiVenuePartial,
    SingleVenue,
    Benchmark,
    Reference,
    UserOwned,
}

impl MarketCoverage {
    const fn bit(self) -> u8 {
        1_u8 << (self as u8)
    }

    pub(super) const fn preference(self) -> u8 {
        match self {
            Self::Consolidated => 6,
            Self::MultiVenuePartial => 5,
            Self::SingleVenue => 4,
            Self::Benchmark => 3,
            Self::Reference => 2,
            Self::UserOwned => 1,
        }
    }
}

/// Scheduling priority represented in a pure request; it grants no provider-budget authority.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum RequestPriority {
    Interactive,
    Foreground,
    Background,
}

/// Timestamp used to measure whether an observation is fresh enough for one request.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum FreshnessBasis {
    Source,
    Effective,
    Received,
    Available,
    Ingested,
}

/// Explicit request cutoff and maximum observation age.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FreshnessRequirement {
    as_of: Timestamp,
    basis: FreshnessBasis,
    maximum_age_nanos: u64,
}

impl FreshnessRequirement {
    pub(crate) fn try_new(
        as_of: Timestamp,
        basis: FreshnessBasis,
        maximum_age_nanos: u64,
    ) -> Result<Self, MarketSelectionError> {
        if maximum_age_nanos > i64::MAX as u64 {
            return Err(MarketSelectionError::FreshnessOutOfRange);
        }
        Ok(Self {
            as_of,
            basis,
            maximum_age_nanos,
        })
    }

    pub(crate) const fn as_of(self) -> Timestamp {
        self.as_of
    }

    pub(crate) const fn basis(self) -> FreshnessBasis {
        self.basis
    }

    pub(crate) const fn maximum_age_nanos(self) -> u64 {
        self.maximum_age_nanos
    }
}

/// Request-specific, allowlisted relaxation of otherwise strict requirements.
///
/// Health, budget, rights, integrity, asset identity, operation support, and execution authority
/// are intentionally absent: those dimensions can never be relaxed by fallback selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DowngradePolicy {
    timing_bits: u8,
    depth_bits: u8,
    quality_bits: u16,
    coverage_bits: u8,
    maximum_age_nanos: Option<u64>,
}

impl DowngradePolicy {
    pub(crate) const fn deny() -> Self {
        Self {
            timing_bits: 0,
            depth_bits: 0,
            quality_bits: 0,
            coverage_bits: 0,
            maximum_age_nanos: None,
        }
    }

    /// Builds an explicit allowlist for every relaxable selection dimension.
    pub(crate) fn try_new(
        timings: &[ObservationTiming],
        depths: &[Option<MarketDepth>],
        qualities: &[DataQuality],
        coverages: &[MarketCoverage],
        maximum_age_nanos: Option<u64>,
    ) -> Result<Self, MarketSelectionError> {
        if maximum_age_nanos.is_some_and(|maximum| maximum > i64::MAX as u64) {
            return Err(MarketSelectionError::FreshnessOutOfRange);
        }
        if qualities.contains(&DataQuality::Quarantined) {
            return Err(MarketSelectionError::QuarantinedDowngrade);
        }

        let timing_bits = timings
            .iter()
            .fold(0_u8, |bits, timing| bits | timing.bit());
        let depth_bits = depths
            .iter()
            .fold(0_u8, |bits, depth| bits | depth_bit(*depth));
        let quality_bits = qualities
            .iter()
            .fold(0_u16, |bits, quality| bits | quality_bit(*quality));
        let coverage_bits = coverages
            .iter()
            .fold(0_u8, |bits, coverage| bits | coverage.bit());

        Ok(Self {
            timing_bits,
            depth_bits,
            quality_bits,
            coverage_bits,
            maximum_age_nanos,
        })
    }

    pub(crate) const fn is_denied(self) -> bool {
        self.timing_bits == 0
            && self.depth_bits == 0
            && self.quality_bits == 0
            && self.coverage_bits == 0
            && self.maximum_age_nanos.is_none()
    }

    pub(super) const fn allows_timing(self, timing: ObservationTiming) -> bool {
        self.timing_bits & timing.bit() != 0
    }

    pub(super) const fn allows_depth(self, depth: Option<MarketDepth>) -> bool {
        self.depth_bits & depth_bit(depth) != 0
    }

    pub(super) const fn allows_quality(self, quality: DataQuality) -> bool {
        self.quality_bits & quality_bit(quality) != 0
    }

    pub(super) const fn allows_coverage(self, coverage: MarketCoverage) -> bool {
        self.coverage_bits & coverage.bit() != 0
    }

    pub(super) const fn maximum_age_nanos(self) -> Option<u64> {
        self.maximum_age_nanos
    }
}

/// Complete immutable requirements for one market-source decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketSelectionRequest {
    asset_class: AssetClass,
    operation: MarketOperation,
    timing: ObservationTiming,
    minimum_depth: Option<MarketDepth>,
    minimum_quality: DataQuality,
    coverage: MarketCoverage,
    freshness: FreshnessRequirement,
    priority: RequestPriority,
    downgrade: DowngradePolicy,
    definition_revision_digest: Option<EvidenceDigest>,
}

impl MarketSelectionRequest {
    #[expect(
        clippy::too_many_arguments,
        reason = "selection requirements must be admitted as one complete immutable request"
    )]
    pub(crate) fn try_new(
        asset_class: AssetClass,
        operation: MarketOperation,
        timing: ObservationTiming,
        minimum_depth: Option<MarketDepth>,
        minimum_quality: DataQuality,
        coverage: MarketCoverage,
        freshness: FreshnessRequirement,
        priority: RequestPriority,
        downgrade: DowngradePolicy,
        definition_revision_digest: Option<EvidenceDigest>,
    ) -> Result<Self, MarketSelectionError> {
        if definition_revision_digest.is_some_and(|digest| {
            digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32]
        }) {
            return Err(MarketSelectionError::InvalidDefinitionRevisionDigest);
        }
        if minimum_quality == DataQuality::Quarantined {
            return Err(MarketSelectionError::InvalidMinimumQuality);
        }
        if asset_class == AssetClass::Index && minimum_depth.is_some() {
            return Err(MarketSelectionError::IndexBookDepth);
        }
        if matches!(
            coverage,
            MarketCoverage::Benchmark | MarketCoverage::Reference
        ) && minimum_depth.is_some()
        {
            return Err(MarketSelectionError::CoverageBookDepth);
        }
        if operation.requires_execution_quality()
            && (timing != ObservationTiming::RealTime
                || minimum_quality != DataQuality::DirectVerified
                || !downgrade.is_denied())
        {
            return Err(MarketSelectionError::UnsafeExecutionRequest);
        }

        Ok(Self {
            asset_class,
            operation,
            timing,
            minimum_depth,
            minimum_quality,
            coverage,
            freshness,
            priority,
            downgrade,
            definition_revision_digest,
        })
    }

    pub(crate) const fn asset_class(&self) -> AssetClass {
        self.asset_class
    }

    pub(crate) const fn operation(&self) -> MarketOperation {
        self.operation
    }

    pub(crate) const fn timing(&self) -> ObservationTiming {
        self.timing
    }

    pub(crate) const fn minimum_depth(&self) -> Option<MarketDepth> {
        self.minimum_depth
    }

    pub(crate) const fn minimum_quality(&self) -> DataQuality {
        self.minimum_quality
    }

    pub(crate) const fn coverage(&self) -> MarketCoverage {
        self.coverage
    }

    pub(crate) const fn freshness(&self) -> FreshnessRequirement {
        self.freshness
    }

    pub(crate) const fn priority(&self) -> RequestPriority {
        self.priority
    }

    pub(crate) const fn downgrade(&self) -> DowngradePolicy {
        self.downgrade
    }

    /// Returns the exact whole-definition revision required for every candidate in this request.
    pub(crate) const fn definition_revision_digest(&self) -> Option<EvidenceDigest> {
        self.definition_revision_digest
    }
}

/// Code-owned selection policy identity and candidate bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MarketSelectionPolicy {
    revision: u32,
    digest: EvidenceDigest,
    maximum_candidates: usize,
}

impl MarketSelectionPolicy {
    /// Constructs the current policy and binds its candidate ceiling into the SHA-256 identity.
    pub(crate) fn v1(maximum_candidates: usize) -> Result<Self, MarketSelectionError> {
        if maximum_candidates == 0 || maximum_candidates > MAXIMUM_POLICY_CANDIDATES {
            return Err(MarketSelectionError::InvalidCandidateLimit {
                maximum: MAXIMUM_POLICY_CANDIDATES,
            });
        }
        let encoded_limit = u64::try_from(maximum_candidates).map_err(|_| {
            MarketSelectionError::InvalidCandidateLimit {
                maximum: MAXIMUM_POLICY_CANDIDATES,
            }
        })?;
        let mut hasher = Sha256::new();
        hasher.update(POLICY_CANONICAL_IDENTITY);
        hasher.update(MARKET_SELECTION_POLICY_REVISION.to_be_bytes());
        hasher.update(encoded_limit.to_be_bytes());
        Ok(Self {
            revision: MARKET_SELECTION_POLICY_REVISION,
            digest: EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into()),
            maximum_candidates,
        })
    }

    pub(crate) const fn revision(self) -> u32 {
        self.revision
    }

    pub(crate) const fn digest(self) -> EvidenceDigest {
        self.digest
    }

    pub(crate) const fn maximum_candidates(self) -> usize {
        self.maximum_candidates
    }
}

pub(super) const fn depth_preference(depth: Option<MarketDepth>) -> u8 {
    match depth {
        Some(MarketDepth::OrderLevel) => 3,
        Some(MarketDepth::PriceLevel) => 2,
        Some(MarketDepth::TopOfBook) => 1,
        None => 0,
    }
}

pub(super) const fn quality_preference(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 9,
        DataQuality::DirectUnverified => 8,
        DataQuality::OfficialDelayed => 7,
        DataQuality::Aggregated => 6,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 4,
        DataQuality::Estimated => 3,
        DataQuality::Stale => 2,
        DataQuality::Quarantined => 1,
    }
}

const fn depth_bit(depth: Option<MarketDepth>) -> u8 {
    match depth {
        None => 1,
        Some(MarketDepth::TopOfBook) => 1 << 1,
        Some(MarketDepth::PriceLevel) => 1 << 2,
        Some(MarketDepth::OrderLevel) => 1 << 3,
    }
}

const fn quality_bit(quality: DataQuality) -> u16 {
    1_u16 << (quality as u16)
}
