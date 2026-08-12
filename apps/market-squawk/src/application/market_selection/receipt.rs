use std::fmt;

use market_squawk_domain::{
    AssetClass, DataQuality, EvidenceDigest, ExecutionEligibility, MarketDepth, Timestamp,
};

use super::{
    BudgetAvailability, HealthState, IntegrityState, MarketCoverage, MarketOperation,
    MarketSelectionRequest, ObservationTiming, RightsState, SourceCandidate,
};

/// Construction or bounded selection failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MarketSelectionError {
    EmptyOperationSet,
    FreshnessOutOfRange,
    QuarantinedDowngrade,
    InvalidMinimumQuality,
    IndexBookDepth,
    CoverageBookDepth,
    UnsafeExecutionRequest,
    InvalidCandidateLimit { maximum: usize },
    InvalidTimestampOrder,
    InvalidBudgetSnapshot,
    InvalidRightsInterval,
    InvalidRightsState,
    MissingVenue,
    UnverifiedDirectQuality,
    InvalidExecutionEligibility,
    TooManyCandidates { maximum: usize, actual: usize },
    DuplicateCandidateIdentity,
    ReceiptEncoding,
    Allocation,
}

impl fmt::Display for MarketSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyOperationSet => formatter.write_str("operation set must not be empty"),
            Self::FreshnessOutOfRange => {
                formatter.write_str("freshness duration exceeds signed timestamp range")
            }
            Self::QuarantinedDowngrade => {
                formatter.write_str("quarantined data cannot be admitted as a downgrade")
            }
            Self::InvalidMinimumQuality => {
                formatter.write_str("quarantined is not a valid minimum quality")
            }
            Self::IndexBookDepth => {
                formatter.write_str("a calculated index cannot require or supply market depth")
            }
            Self::CoverageBookDepth => {
                formatter.write_str("benchmark and reference coverage cannot carry market depth")
            }
            Self::UnsafeExecutionRequest => formatter.write_str(
                "paper and live execution require real-time DirectVerified data and forbid downgrades",
            ),
            Self::InvalidCandidateLimit { maximum } => write!(
                formatter,
                "candidate limit must be between one and {maximum}"
            ),
            Self::InvalidTimestampOrder => formatter
                .write_str("candidate timestamps must satisfy received <= available <= ingested"),
            Self::InvalidBudgetSnapshot => {
                formatter.write_str("budget availability conflicts with remaining units")
            }
            Self::InvalidRightsInterval => {
                formatter.write_str("rights validity ends before it begins")
            }
            Self::InvalidRightsState => {
                formatter.write_str("admitted rights require an explicit operation set")
            }
            Self::MissingVenue => {
                formatter.write_str("single-venue coverage requires an exact venue")
            }
            Self::UnverifiedDirectQuality => formatter.write_str(
                "DirectVerified quality requires verified integrity and a connection generation",
            ),
            Self::InvalidExecutionEligibility => formatter.write_str(
                "execution eligibility requires this candidate's own DirectVerified integrity",
            ),
            Self::TooManyCandidates { maximum, actual } => write!(
                formatter,
                "candidate count {actual} exceeds the policy maximum {maximum}"
            ),
            Self::DuplicateCandidateIdentity => {
                formatter.write_str("candidate identities must be unique")
            }
            Self::ReceiptEncoding => {
                formatter.write_str("selection receipt cannot be represented canonically")
            }
            Self::Allocation => formatter.write_str("bounded selection allocation failed"),
        }
    }
}

impl std::error::Error for MarketSelectionError {}

/// Deterministic reason a candidate could not serve the request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CandidateRejectionReason {
    AssetMismatch {
        required: AssetClass,
        actual: AssetClass,
    },
    OperationUnsupported {
        operation: MarketOperation,
    },
    TimingUnavailable {
        required: ObservationTiming,
        actual: ObservationTiming,
    },
    DepthUnavailable {
        minimum: MarketDepth,
        actual: Option<MarketDepth>,
    },
    QualityBelowMinimum {
        minimum: DataQuality,
        actual: DataQuality,
    },
    CoverageUnavailable {
        required: MarketCoverage,
        actual: MarketCoverage,
    },
    FreshnessAnchorMissing,
    FreshnessAnchorAfterSelection,
    FreshnessExceeded {
        maximum_age_nanos: u64,
        actual_age_nanos: u64,
    },
    EffectiveAfterSelection,
    AvailableAfterSelection,
    IngestedAfterSelection,
    HealthObservedAfterSelection,
    HealthUnavailable {
        state: HealthState,
    },
    ExecutionRequiresHealthySource,
    BudgetObservedAfterSelection,
    BudgetUnavailable {
        state: BudgetAvailability,
    },
    BudgetPriorityDenied,
    RightsUnavailable {
        state: RightsState,
    },
    RightsObservedAfterSelection,
    RightsOperationDenied,
    RightsNotEffective,
    RightsExpired,
    IntegrityObservedAfterSelection,
    IntegrityUnavailable {
        state: IntegrityState,
    },
    QuarantinedQuality,
    ExecutionRequiresDirectVerified {
        actual: DataQuality,
    },
    ExecutionIneligible {
        actual: ExecutionEligibility,
    },
}

/// One explicitly admitted difference from the strict request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DowngradeDimension {
    Timing {
        required: ObservationTiming,
        selected: ObservationTiming,
    },
    Depth {
        minimum: MarketDepth,
        selected: Option<MarketDepth>,
    },
    Quality {
        minimum: DataQuality,
        selected: DataQuality,
    },
    Coverage {
        required: MarketCoverage,
        selected: MarketCoverage,
    },
    Freshness {
        maximum_age_nanos: u64,
        selected_age_nanos: u64,
    },
}

/// Complete set of explicitly admitted downgrade dimensions for one candidate.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AdmittedDowngrade {
    dimensions: Vec<DowngradeDimension>,
}

impl AdmittedDowngrade {
    pub(super) const fn new(dimensions: Vec<DowngradeDimension>) -> Self {
        Self { dimensions }
    }

    pub(crate) fn dimensions(&self) -> &[DowngradeDimension] {
        &self.dimensions
    }
}

/// Why an eligible candidate was retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SelectionClass {
    ExactRequirements,
    AdmittedDowngrade,
}

/// Source-preserving eligible candidate plus its exact freshness and downgrade result.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct EligibleCandidate {
    candidate: SourceCandidate,
    freshness_age_nanos: u64,
    downgrade: Option<AdmittedDowngrade>,
}

impl EligibleCandidate {
    pub(super) const fn new(
        candidate: SourceCandidate,
        freshness_age_nanos: u64,
        downgrade: Option<AdmittedDowngrade>,
    ) -> Self {
        Self {
            candidate,
            freshness_age_nanos,
            downgrade,
        }
    }

    pub(crate) const fn candidate(&self) -> &SourceCandidate {
        &self.candidate
    }

    pub(crate) const fn freshness_age_nanos(&self) -> u64 {
        self.freshness_age_nanos
    }

    pub(crate) const fn class(&self) -> SelectionClass {
        if self.downgrade.is_some() {
            SelectionClass::AdmittedDowngrade
        } else {
            SelectionClass::ExactRequirements
        }
    }

    pub(crate) const fn downgrade(&self) -> Option<&AdmittedDowngrade> {
        self.downgrade.as_ref()
    }
}

/// Rejected candidate and every deterministic reason found by the policy.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RejectedCandidate {
    candidate: SourceCandidate,
    reasons: Vec<CandidateRejectionReason>,
}

impl RejectedCandidate {
    pub(super) const fn new(
        candidate: SourceCandidate,
        reasons: Vec<CandidateRejectionReason>,
    ) -> Self {
        Self { candidate, reasons }
    }

    pub(crate) const fn candidate(&self) -> &SourceCandidate {
        &self.candidate
    }

    pub(crate) fn reasons(&self) -> &[CandidateRejectionReason] {
        &self.reasons
    }
}

/// Borrowed selected result. All classifications come from the selected candidate itself.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SelectedMarketSource<'a> {
    eligible: &'a EligibleCandidate,
}

impl<'a> SelectedMarketSource<'a> {
    pub(super) const fn new(eligible: &'a EligibleCandidate) -> Self {
        Self { eligible }
    }

    pub(crate) const fn candidate(self) -> &'a SourceCandidate {
        self.eligible.candidate()
    }

    pub(crate) const fn freshness_age_nanos(self) -> u64 {
        self.eligible.freshness_age_nanos()
    }

    pub(crate) const fn class(self) -> SelectionClass {
        self.eligible.class()
    }

    pub(crate) const fn downgrade(self) -> Option<&'a AdmittedDowngrade> {
        self.eligible.downgrade()
    }
}

/// Complete bounded source-selection receipt.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MarketSelectionReceipt {
    policy_revision: u32,
    policy_digest: EvidenceDigest,
    policy_candidate_limit: usize,
    request: MarketSelectionRequest,
    eligible: Vec<EligibleCandidate>,
    rejected: Vec<RejectedCandidate>,
    selected_at: Timestamp,
    selection_digest: EvidenceDigest,
}

impl MarketSelectionReceipt {
    pub(super) const fn new(
        policy_revision: u32,
        policy_digest: EvidenceDigest,
        policy_candidate_limit: usize,
        request: MarketSelectionRequest,
        eligible: Vec<EligibleCandidate>,
        rejected: Vec<RejectedCandidate>,
        selected_at: Timestamp,
        selection_digest: EvidenceDigest,
    ) -> Self {
        Self {
            policy_revision,
            policy_digest,
            policy_candidate_limit,
            request,
            eligible,
            rejected,
            selected_at,
            selection_digest,
        }
    }

    pub(crate) const fn policy_revision(&self) -> u32 {
        self.policy_revision
    }

    pub(crate) const fn policy_digest(&self) -> EvidenceDigest {
        self.policy_digest
    }

    pub(crate) const fn policy_candidate_limit(&self) -> usize {
        self.policy_candidate_limit
    }

    pub(crate) const fn request(&self) -> &MarketSelectionRequest {
        &self.request
    }

    pub(crate) fn eligible(&self) -> &[EligibleCandidate] {
        &self.eligible
    }

    pub(crate) fn rejected(&self) -> &[RejectedCandidate] {
        &self.rejected
    }

    pub(crate) const fn selected_at(&self) -> Timestamp {
        self.selected_at
    }

    /// SHA-256 commitment to the complete ordered request, evidence, decision, and policy receipt.
    pub(crate) const fn selection_digest(&self) -> EvidenceDigest {
        self.selection_digest
    }

    pub(crate) fn selected(&self) -> Option<SelectedMarketSource<'_>> {
        self.eligible.first().map(SelectedMarketSource::new)
    }
}
