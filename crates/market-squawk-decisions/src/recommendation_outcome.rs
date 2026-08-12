//! Immutable publication bindings and realized recommendation-outcome records.
//!
//! Forecast calibration describes a model's historical interval behavior. The records in this
//! module instead describe what happened after one published recommendation. They are separate
//! authorities and never grant order, portfolio-mutation, or execution capability.

use std::num::NonZeroU32;

use market_squawk_domain::{AccountId, Money, RevisionNumber, SourceIdentifier, Timestamp};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};

use crate::{
    DecisionContentDigest, InvestmentAnalysisId, InvestmentProposalDecision, InvestmentProposalId,
    ProposalExecutionEligibility, ProposalUnavailableReason, RecommendationAction,
    RecommendationDerivationDigest, RecommendationEvidenceDigest, RecommendationPolicyDigest,
};

/// Current schema for immutable investment-analysis publication bindings.
pub const INVESTMENT_ANALYSIS_PUBLICATION_SCHEMA_VERSION: u16 = 1;
/// Current schema for realized recommendation-outcome status records.
pub const RECOMMENDATION_OUTCOME_STATUS_SCHEMA_VERSION: u16 = 1;
/// Code-owned minimum completed observations before a group performance mean is displayed.
pub const RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED: u32 = 30;
/// Code-owned minimum completed coverage among due outcomes before performance is displayed.
pub const RECOMMENDATION_TRACK_RECORD_MINIMUM_COVERAGE_PPM: u32 = 800_000;

macro_rules! digest_identity {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 32]);

        impl $name {
            /// Reconstructs a nonzero fixed-width identity from persisted bytes.
            pub fn try_from_bytes(bytes: [u8; 32]) -> Result<Self, RecommendationOutcomeError> {
                if bytes == [0; 32] {
                    Err(RecommendationOutcomeError::ReservedIdentity)
                } else {
                    Ok(Self(bytes))
                }
            }

            /// Returns the complete SHA-256 identity bytes.
            #[must_use]
            pub const fn bytes(self) -> [u8; 32] {
                self.0
            }
        }
    };
}

digest_identity!(
    /// Content address of one immutable publication/profile/workflow binding.
    InvestmentAnalysisPublicationId
);
digest_identity!(
    /// Stable series identity for revisions of one published recommendation outcome.
    RecommendationOutcomeSeriesId
);
digest_identity!(
    /// Content address of one exact recommendation-outcome status revision.
    RecommendationOutcomeStatusDigest
);

/// Invalid publication or realized-outcome evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationOutcomeError {
    /// An all-zero fixed-width identity was supplied.
    ReservedIdentity,
    /// A publication or outcome did not bind its authoritative proposal.
    BindingMismatch,
    /// Evidence times were not point-in-time ordered.
    InvalidTimeOrder,
    /// An outcome price was nonpositive or used a different currency.
    InvalidPrice,
    /// A status was incompatible with the generated, no-action, or unavailable analysis family.
    InvalidStatus,
    /// Checked exact-decimal or count arithmetic failed.
    Arithmetic,
}

impl std::fmt::Display for RecommendationOutcomeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::ReservedIdentity => "recommendation outcome identity is reserved",
            Self::BindingMismatch => "recommendation outcome does not bind its analysis",
            Self::InvalidTimeOrder => "recommendation outcome time ordering is invalid",
            Self::InvalidPrice => "recommendation outcome price is invalid",
            Self::InvalidStatus => "recommendation outcome status is invalid",
            Self::Arithmetic => "recommendation outcome arithmetic failed",
        })
    }
}

impl std::error::Error for RecommendationOutcomeError {}

/// Canonical immutable reference to one analytical profile revision.
///
/// This is analysis methodology, not brokerage-account setup. Account identity remains a
/// separate field copied from the proposal evidence on [`PublishedInvestmentAnalysis`].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AnalyticalProfileBindingReference {
    profile_id: SourceIdentifier,
    revision: NonZeroU32,
    content_digest: DecisionContentDigest,
}

impl AnalyticalProfileBindingReference {
    /// Binds one named analytical profile to its exact immutable revision content.
    #[must_use]
    pub const fn new(
        profile_id: SourceIdentifier,
        revision: NonZeroU32,
        content_digest: DecisionContentDigest,
    ) -> Self {
        Self {
            profile_id,
            revision,
            content_digest,
        }
    }

    /// Returns the bounded profile identity.
    #[must_use]
    pub const fn profile_id(&self) -> &SourceIdentifier {
        &self.profile_id
    }

    /// Returns the nonzero immutable profile revision.
    #[must_use]
    pub const fn revision(&self) -> NonZeroU32 {
        self.revision
    }

    /// Returns the exact profile content identity.
    #[must_use]
    pub const fn content_digest(&self) -> DecisionContentDigest {
        self.content_digest
    }
}

/// Immutable reference to the workflow that published an investment analysis.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct InvestmentAnalysisWorkflowReference {
    workflow_id: SourceIdentifier,
    revision: NonZeroU32,
    content_digest: DecisionContentDigest,
}

impl InvestmentAnalysisWorkflowReference {
    /// Binds one named workflow to its exact immutable revision content.
    #[must_use]
    pub const fn new(
        workflow_id: SourceIdentifier,
        revision: NonZeroU32,
        content_digest: DecisionContentDigest,
    ) -> Self {
        Self {
            workflow_id,
            revision,
            content_digest,
        }
    }

    /// Returns the bounded workflow identity.
    #[must_use]
    pub const fn workflow_id(&self) -> &SourceIdentifier {
        &self.workflow_id
    }

    /// Returns the nonzero immutable workflow revision.
    #[must_use]
    pub const fn revision(&self) -> NonZeroU32 {
        self.revision
    }

    /// Returns the exact workflow content identity.
    #[must_use]
    pub const fn content_digest(&self) -> DecisionContentDigest {
        self.content_digest
    }
}

/// Immutable publication of one pure proposal decision under one profile and workflow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedInvestmentAnalysis {
    publication_id: InvestmentAnalysisPublicationId,
    analysis_id: InvestmentAnalysisId,
    proposal_id: Option<InvestmentProposalId>,
    derivation_digest: Option<RecommendationDerivationDigest>,
    policy_digest: RecommendationPolicyDigest,
    evidence_digest: RecommendationEvidenceDigest,
    analytical_profile: AnalyticalProfileBindingReference,
    workflow: InvestmentAnalysisWorkflowReference,
    account_id: AccountId,
    as_of: Timestamp,
    horizon_at: Timestamp,
    published_at: Timestamp,
    execution_eligibility: ProposalExecutionEligibility,
}

impl PublishedInvestmentAnalysis {
    /// Publishes an already derived decision without mutating its pure authority.
    pub fn try_new(
        decision: &InvestmentProposalDecision,
        analytical_profile: AnalyticalProfileBindingReference,
        workflow: InvestmentAnalysisWorkflowReference,
        published_at: Timestamp,
    ) -> Result<Self, RecommendationOutcomeError> {
        if published_at < decision.evidence().as_of() {
            return Err(RecommendationOutcomeError::InvalidTimeOrder);
        }
        let mut value = Self {
            publication_id: InvestmentAnalysisPublicationId([0; 32]),
            analysis_id: decision.analysis_id(),
            proposal_id: decision.proposal_id(),
            derivation_digest: decision.derivation_digest(),
            policy_digest: decision.policy_digest(),
            evidence_digest: decision.evidence_digest(),
            analytical_profile,
            workflow,
            account_id: decision.evidence().account_id(),
            as_of: decision.evidence().as_of(),
            horizon_at: decision.horizon_at(),
            published_at,
            execution_eligibility: ProposalExecutionEligibility::ResearchOnlyExecutionIneligible,
        };
        value.publication_id = publication_digest(&value);
        Ok(value)
    }

    /// Returns the publication schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        INVESTMENT_ANALYSIS_PUBLICATION_SCHEMA_VERSION
    }

    /// Returns the publication content address.
    #[must_use]
    pub const fn publication_id(&self) -> InvestmentAnalysisPublicationId {
        self.publication_id
    }

    /// Returns the stable analysis identity.
    #[must_use]
    pub const fn analysis_id(&self) -> InvestmentAnalysisId {
        self.analysis_id
    }

    /// Returns the proposal identity for generated and no-action analyses.
    #[must_use]
    pub const fn proposal_id(&self) -> Option<InvestmentProposalId> {
        self.proposal_id
    }

    /// Returns the proposal derivation identity when one exists.
    #[must_use]
    pub const fn derivation_digest(&self) -> Option<RecommendationDerivationDigest> {
        self.derivation_digest
    }

    /// Returns the code-owned proposal-policy identity.
    #[must_use]
    pub const fn policy_digest(&self) -> RecommendationPolicyDigest {
        self.policy_digest
    }

    /// Returns the complete proposal-evidence commitment.
    #[must_use]
    pub const fn evidence_digest(&self) -> RecommendationEvidenceDigest {
        self.evidence_digest
    }

    /// Returns the canonical analytical profile binding.
    #[must_use]
    pub const fn analytical_profile(&self) -> &AnalyticalProfileBindingReference {
        &self.analytical_profile
    }

    /// Returns the exact publishing workflow binding.
    #[must_use]
    pub const fn workflow(&self) -> &InvestmentAnalysisWorkflowReference {
        &self.workflow
    }

    /// Returns the distinct proposal account setup identity.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the proposal evidence cutoff.
    #[must_use]
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns the exact recommendation horizon.
    #[must_use]
    pub const fn horizon_at(&self) -> Timestamp {
        self.horizon_at
    }

    /// Returns when this profile-bound analysis was published.
    #[must_use]
    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }

    /// Returns the closed research-only execution marker.
    #[must_use]
    pub const fn execution_eligibility(&self) -> ProposalExecutionEligibility {
        self.execution_eligibility
    }
}

/// Evaluation cohort retained separately for every action and for no-action controls.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RecommendationOutcomeCohort {
    /// Generated actionable recommendation.
    Generated(RecommendationAction),
    /// Counterfactual instrument movement after an explicit no-action recommendation.
    NoActionControl,
    /// The original analysis was unavailable and did not issue a recommendation.
    AnalysisUnavailable,
}

/// Why an outcome remains pending.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RecommendationOutcomePendingReason {
    /// The exact recommendation horizon has not yet elapsed.
    AwaitingHorizon,
    /// The horizon elapsed but a completed, unambiguous observation is not yet available.
    AwaitingOutcomeEvidence,
}

/// Why a realized outcome is terminally unavailable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationOutcomeUnavailableReason {
    /// The analysis itself declined to issue a proposal.
    AnalysisUnavailable(ProposalUnavailableReason),
    /// No eligible completed outcome observation was published within the bounded process.
    OutcomeObservationUnavailable,
    /// More than one equally authoritative endpoint observation remained.
    AmbiguousOutcomeObservation,
    /// The candidate endpoint observation was not a proven completed period.
    IncompleteOutcomeObservation,
    /// Exact corporate-action evidence needed to compare prices was unavailable.
    CorporateActionEvidenceUnavailable,
}

/// Exact receipt-bound terminal price observation used for one realized outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationOutcomeObservation {
    endpoint_price: Money,
    observed_at: Timestamp,
    available_at: Timestamp,
    selection_receipt_identity: DecisionContentDigest,
    selected_observation_identity: DecisionContentDigest,
    no_applicable_corporate_actions_identity: DecisionContentDigest,
}

impl RecommendationOutcomeObservation {
    /// Captures one already selected, completed horizon observation and its absence-of-actions
    /// evidence. It does not select a bar or infer session semantics.
    pub fn try_new(
        endpoint_price: Money,
        observed_at: Timestamp,
        available_at: Timestamp,
        selection_receipt_identity: DecisionContentDigest,
        selected_observation_identity: DecisionContentDigest,
        no_applicable_corporate_actions_identity: DecisionContentDigest,
    ) -> Result<Self, RecommendationOutcomeError> {
        if endpoint_price.amount() <= Decimal::ZERO {
            return Err(RecommendationOutcomeError::InvalidPrice);
        }
        if observed_at > available_at {
            return Err(RecommendationOutcomeError::InvalidTimeOrder);
        }
        Ok(Self {
            endpoint_price,
            observed_at,
            available_at,
            selection_receipt_identity,
            selected_observation_identity,
            no_applicable_corporate_actions_identity,
        })
    }

    /// Returns the exact selected terminal price.
    #[must_use]
    pub const fn endpoint_price(self) -> Money {
        self.endpoint_price
    }

    /// Returns the completed observation coordinate.
    #[must_use]
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    /// Returns when the selected observation became knowable.
    #[must_use]
    pub const fn available_at(self) -> Timestamp {
        self.available_at
    }

    /// Returns the deterministic endpoint-selection receipt identity.
    #[must_use]
    pub const fn selection_receipt_identity(self) -> DecisionContentDigest {
        self.selection_receipt_identity
    }

    /// Returns the exact selected observation identity.
    #[must_use]
    pub const fn selected_observation_identity(self) -> DecisionContentDigest {
        self.selected_observation_identity
    }

    /// Returns evidence that no price-comparison adjustment was applicable over the horizon.
    #[must_use]
    pub const fn no_applicable_corporate_actions_identity(self) -> DecisionContentDigest {
        self.no_applicable_corporate_actions_identity
    }
}

/// Net return remains unavailable until exact recommendation-specific realized costs exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationNetReturnAvailability {
    /// Exact realized forward costs were not supplied.
    UnavailableExactRealizedCostEvidenceNotSupplied,
}

/// Benchmark-relative return remains unavailable until an exact PIT benchmark outcome exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationBenchmarkReturnAvailability {
    /// Exact proposal-time and horizon benchmark observations were not supplied.
    UnavailableExactBenchmarkOutcomeEvidenceNotSupplied,
}

/// After-tax return remains unavailable until account/lot/jurisdiction evidence exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationAfterTaxReturnAvailability {
    /// Exact tax-lot and jurisdiction evidence was not supplied.
    UnavailableExactTaxEvidenceNotSupplied,
}

/// Settlement result remains unavailable without exact settlement lifecycle evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationSettlementAvailability {
    /// No execution or exact settlement evidence was supplied.
    UnavailableNoExecutionOrSettlementEvidence,
}

/// Recomputed gross instrument-price outcome for a generated recommendation or no-action control.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationRealizedOutcome {
    start_mark: Money,
    observation: RecommendationOutcomeObservation,
    gross_price_return: Decimal,
    net_return: RecommendationNetReturnAvailability,
    benchmark_return: RecommendationBenchmarkReturnAvailability,
    after_tax_return: RecommendationAfterTaxReturnAvailability,
    settlement: RecommendationSettlementAvailability,
}

impl RecommendationRealizedOutcome {
    /// Returns the proposal's exact starting market mark.
    #[must_use]
    pub const fn start_mark(self) -> Money {
        self.start_mark
    }

    /// Returns the exact receipt-bound endpoint observation.
    #[must_use]
    pub const fn observation(self) -> RecommendationOutcomeObservation {
        self.observation
    }

    /// Returns `(endpoint - proposal_mark) / proposal_mark` as a deterministic decimal.
    ///
    /// This is gross instrument-price movement, not portfolio profit, execution P/L, or a promise.
    #[must_use]
    pub const fn gross_price_return(self) -> Decimal {
        self.gross_price_return
    }

    /// Returns realized net-return availability.
    #[must_use]
    pub const fn net_return(self) -> RecommendationNetReturnAvailability {
        self.net_return
    }

    /// Returns benchmark-relative realized-return availability.
    #[must_use]
    pub const fn benchmark_return(self) -> RecommendationBenchmarkReturnAvailability {
        self.benchmark_return
    }

    /// Returns after-tax realized-return availability.
    #[must_use]
    pub const fn after_tax_return(self) -> RecommendationAfterTaxReturnAvailability {
        self.after_tax_return
    }

    /// Returns settlement-result availability.
    #[must_use]
    pub const fn settlement(self) -> RecommendationSettlementAvailability {
        self.settlement
    }
}

/// Current immutable status payload for one recommendation-outcome series revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationOutcomeStatus {
    /// Not yet complete.
    Pending(RecommendationOutcomePendingReason),
    /// Terminally unavailable with an exact reason.
    Unavailable(RecommendationOutcomeUnavailableReason),
    /// Completed gross price observation.
    Completed(RecommendationRealizedOutcome),
}

/// One immutable revision in a recommendation-outcome lifecycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendationOutcomeStatusRecord {
    series_id: RecommendationOutcomeSeriesId,
    status_digest: RecommendationOutcomeStatusDigest,
    publication_id: InvestmentAnalysisPublicationId,
    analysis_id: InvestmentAnalysisId,
    proposal_id: Option<InvestmentProposalId>,
    cohort: RecommendationOutcomeCohort,
    horizon_at: Timestamp,
    revision: RevisionNumber,
    previous_status_digest: Option<RecommendationOutcomeStatusDigest>,
    evaluated_at: Timestamp,
    status: RecommendationOutcomeStatus,
    execution_eligibility: ProposalExecutionEligibility,
}

impl RecommendationOutcomeStatusRecord {
    /// Creates a pending status revision.
    pub fn try_pending(
        decision: &InvestmentProposalDecision,
        publication: &PublishedInvestmentAnalysis,
        revision: RevisionNumber,
        previous_status_digest: Option<RecommendationOutcomeStatusDigest>,
        evaluated_at: Timestamp,
        reason: RecommendationOutcomePendingReason,
    ) -> Result<Self, RecommendationOutcomeError> {
        let valid_reason_time = match reason {
            RecommendationOutcomePendingReason::AwaitingHorizon => {
                evaluated_at < decision.horizon_at()
            }
            RecommendationOutcomePendingReason::AwaitingOutcomeEvidence => {
                evaluated_at >= decision.horizon_at()
            }
        };
        if matches!(decision, InvestmentProposalDecision::Unavailable(_)) || !valid_reason_time {
            return Err(RecommendationOutcomeError::InvalidStatus);
        }
        Self::try_build(
            decision,
            publication,
            revision,
            previous_status_digest,
            evaluated_at,
            RecommendationOutcomeStatus::Pending(reason),
        )
    }

    /// Creates a terminal unavailable status revision.
    pub fn try_unavailable(
        decision: &InvestmentProposalDecision,
        publication: &PublishedInvestmentAnalysis,
        revision: RevisionNumber,
        previous_status_digest: Option<RecommendationOutcomeStatusDigest>,
        evaluated_at: Timestamp,
        reason: RecommendationOutcomeUnavailableReason,
    ) -> Result<Self, RecommendationOutcomeError> {
        let compatible = match (decision, reason) {
            (
                InvestmentProposalDecision::Unavailable(value),
                RecommendationOutcomeUnavailableReason::AnalysisUnavailable(actual),
            ) => value.reason() == actual,
            (
                InvestmentProposalDecision::Generated(_) | InvestmentProposalDecision::NoAction(_),
                RecommendationOutcomeUnavailableReason::OutcomeObservationUnavailable
                | RecommendationOutcomeUnavailableReason::AmbiguousOutcomeObservation
                | RecommendationOutcomeUnavailableReason::IncompleteOutcomeObservation
                | RecommendationOutcomeUnavailableReason::CorporateActionEvidenceUnavailable,
            ) => evaluated_at >= decision.horizon_at(),
            _ => false,
        };
        if !compatible {
            return Err(RecommendationOutcomeError::InvalidStatus);
        }
        Self::try_build(
            decision,
            publication,
            revision,
            previous_status_digest,
            evaluated_at,
            RecommendationOutcomeStatus::Unavailable(reason),
        )
    }

    /// Creates a completed outcome by recomputing gross price movement from proposal evidence.
    pub fn try_completed(
        decision: &InvestmentProposalDecision,
        publication: &PublishedInvestmentAnalysis,
        revision: RevisionNumber,
        previous_status_digest: Option<RecommendationOutcomeStatusDigest>,
        evaluated_at: Timestamp,
        observation: RecommendationOutcomeObservation,
    ) -> Result<Self, RecommendationOutcomeError> {
        if matches!(decision, InvestmentProposalDecision::Unavailable(_))
            || observation.observed_at < decision.horizon_at()
            || observation.available_at > evaluated_at
        {
            return Err(RecommendationOutcomeError::InvalidTimeOrder);
        }
        let start_mark = decision
            .evidence()
            .market()
            .ok_or(RecommendationOutcomeError::BindingMismatch)?
            .price();
        if start_mark.amount() <= Decimal::ZERO
            || start_mark.currency() != observation.endpoint_price.currency()
        {
            return Err(RecommendationOutcomeError::InvalidPrice);
        }
        let gross_price_return = observation
            .endpoint_price
            .amount()
            .checked_sub(start_mark.amount())
            .and_then(|change| change.checked_div(start_mark.amount()))
            .ok_or(RecommendationOutcomeError::Arithmetic)?
            .normalize();
        let realized = RecommendationRealizedOutcome {
            start_mark,
            observation,
            gross_price_return,
            net_return:
                RecommendationNetReturnAvailability::UnavailableExactRealizedCostEvidenceNotSupplied,
            benchmark_return: RecommendationBenchmarkReturnAvailability::UnavailableExactBenchmarkOutcomeEvidenceNotSupplied,
            after_tax_return:
                RecommendationAfterTaxReturnAvailability::UnavailableExactTaxEvidenceNotSupplied,
            settlement: RecommendationSettlementAvailability::UnavailableNoExecutionOrSettlementEvidence,
        };
        Self::try_build(
            decision,
            publication,
            revision,
            previous_status_digest,
            evaluated_at,
            RecommendationOutcomeStatus::Completed(realized),
        )
    }

    fn try_build(
        decision: &InvestmentProposalDecision,
        publication: &PublishedInvestmentAnalysis,
        revision: RevisionNumber,
        previous_status_digest: Option<RecommendationOutcomeStatusDigest>,
        evaluated_at: Timestamp,
        status: RecommendationOutcomeStatus,
    ) -> Result<Self, RecommendationOutcomeError> {
        ensure_publication_binding(decision, publication)?;
        if evaluated_at < publication.published_at() {
            return Err(RecommendationOutcomeError::InvalidTimeOrder);
        }
        let cohort = match decision {
            InvestmentProposalDecision::Generated(value) => {
                RecommendationOutcomeCohort::Generated(value.action())
            }
            InvestmentProposalDecision::NoAction(_) => RecommendationOutcomeCohort::NoActionControl,
            InvestmentProposalDecision::Unavailable(_) => {
                RecommendationOutcomeCohort::AnalysisUnavailable
            }
        };
        let series_id = outcome_series_id(publication);
        let mut value = Self {
            series_id,
            status_digest: RecommendationOutcomeStatusDigest([0; 32]),
            publication_id: publication.publication_id(),
            analysis_id: decision.analysis_id(),
            proposal_id: decision.proposal_id(),
            cohort,
            horizon_at: decision.horizon_at(),
            revision,
            previous_status_digest,
            evaluated_at,
            status,
            execution_eligibility: ProposalExecutionEligibility::ResearchOnlyExecutionIneligible,
        };
        value.status_digest = outcome_status_digest(&value);
        Ok(value)
    }

    /// Returns the status schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        RECOMMENDATION_OUTCOME_STATUS_SCHEMA_VERSION
    }

    /// Returns the stable lifecycle series identity.
    #[must_use]
    pub const fn series_id(&self) -> RecommendationOutcomeSeriesId {
        self.series_id
    }

    /// Returns this immutable revision's content identity.
    #[must_use]
    pub const fn status_digest(&self) -> RecommendationOutcomeStatusDigest {
        self.status_digest
    }

    /// Returns the bound publication identity.
    #[must_use]
    pub const fn publication_id(&self) -> InvestmentAnalysisPublicationId {
        self.publication_id
    }

    /// Returns the bound analysis identity.
    #[must_use]
    pub const fn analysis_id(&self) -> InvestmentAnalysisId {
        self.analysis_id
    }

    /// Returns the generated/no-action proposal identity when one exists.
    #[must_use]
    pub const fn proposal_id(&self) -> Option<InvestmentProposalId> {
        self.proposal_id
    }

    /// Returns the action-specific or no-action-control cohort.
    #[must_use]
    pub const fn cohort(&self) -> RecommendationOutcomeCohort {
        self.cohort
    }

    /// Returns the exact evaluation horizon.
    #[must_use]
    pub const fn horizon_at(&self) -> Timestamp {
        self.horizon_at
    }

    /// Returns the contiguous status revision.
    #[must_use]
    pub const fn revision(&self) -> RevisionNumber {
        self.revision
    }

    /// Returns the exact superseded status identity.
    #[must_use]
    pub const fn previous_status_digest(&self) -> Option<RecommendationOutcomeStatusDigest> {
        self.previous_status_digest
    }

    /// Returns when the outcome status was evaluated.
    #[must_use]
    pub const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    /// Returns pending, unavailable, or completed state.
    #[must_use]
    pub const fn status(&self) -> RecommendationOutcomeStatus {
        self.status
    }

    /// Returns the closed research-only execution marker.
    #[must_use]
    pub const fn execution_eligibility(&self) -> ProposalExecutionEligibility {
        self.execution_eligibility
    }
}

/// Currentness locator derived from the latest contiguous status revision, never append ranking.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendationOutcomeCurrentIndexEntry {
    analytical_profile: AnalyticalProfileBindingReference,
    analysis_id: InvestmentAnalysisId,
    series_id: RecommendationOutcomeSeriesId,
    horizon_at: Timestamp,
    revision: RevisionNumber,
    status_digest: RecommendationOutcomeStatusDigest,
    evaluated_at: Timestamp,
    status: RecommendationOutcomeStatus,
}

impl RecommendationOutcomeCurrentIndexEntry {
    pub(crate) fn new(
        publication: &PublishedInvestmentAnalysis,
        status: &RecommendationOutcomeStatusRecord,
    ) -> Self {
        Self {
            analytical_profile: publication.analytical_profile().clone(),
            analysis_id: status.analysis_id(),
            series_id: status.series_id(),
            horizon_at: status.horizon_at(),
            revision: status.revision(),
            status_digest: status.status_digest(),
            evaluated_at: status.evaluated_at(),
            status: status.status(),
        }
    }

    /// Returns the exact analytical profile grouping authority.
    #[must_use]
    pub const fn analytical_profile(&self) -> &AnalyticalProfileBindingReference {
        &self.analytical_profile
    }

    /// Returns the exact analysis identity.
    #[must_use]
    pub const fn analysis_id(&self) -> InvestmentAnalysisId {
        self.analysis_id
    }

    /// Returns the lifecycle series identity.
    #[must_use]
    pub const fn series_id(&self) -> RecommendationOutcomeSeriesId {
        self.series_id
    }

    /// Returns the exact outcome horizon.
    #[must_use]
    pub const fn horizon_at(&self) -> Timestamp {
        self.horizon_at
    }

    /// Returns the current contiguous revision.
    #[must_use]
    pub const fn revision(&self) -> RevisionNumber {
        self.revision
    }

    /// Returns the current revision identity.
    #[must_use]
    pub const fn status_digest(&self) -> RecommendationOutcomeStatusDigest {
        self.status_digest
    }

    /// Returns when current status was evaluated.
    #[must_use]
    pub const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    /// Returns the current status.
    #[must_use]
    pub const fn status(&self) -> RecommendationOutcomeStatus {
        self.status
    }
}

/// Availability of honest group performance after coverage and sample checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationTrackRecordPerformance {
    /// No recommendation in this group has reached its horizon.
    UnavailableNoDueOutcomes,
    /// Fewer completed outcomes than the code-owned disclosure minimum.
    UnavailableInsufficientCompletedSamples { required: u32, actual: u32 },
    /// Too many due outcomes are pending or unavailable for an honest aggregate.
    UnavailableInsufficientCoverage { required_ppm: u32, actual_ppm: u32 },
    /// Gross price movement summary, kept action-specific and separate from no-action controls.
    Available {
        mean_gross_price_return: Decimal,
        positive_outcomes: u32,
        zero_outcomes: u32,
        negative_outcomes: u32,
    },
}

/// One action-specific or no-action-control group within a profile/horizon track record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendationTrackRecordGroup {
    cohort: RecommendationOutcomeCohort,
    publication_count: u32,
    due_count: u32,
    completed_count: u32,
    pending_count: u32,
    unavailable_count: u32,
    coverage_ppm: u32,
    gross_price_return_sum: Decimal,
    positive_outcomes: u32,
    zero_outcomes: u32,
    negative_outcomes: u32,
    performance: RecommendationTrackRecordPerformance,
}

impl RecommendationTrackRecordGroup {
    pub(crate) fn empty(cohort: RecommendationOutcomeCohort) -> Self {
        Self {
            cohort,
            publication_count: 0,
            due_count: 0,
            completed_count: 0,
            pending_count: 0,
            unavailable_count: 0,
            coverage_ppm: 0,
            gross_price_return_sum: Decimal::ZERO,
            positive_outcomes: 0,
            zero_outcomes: 0,
            negative_outcomes: 0,
            performance: RecommendationTrackRecordPerformance::UnavailableNoDueOutcomes,
        }
    }

    pub(crate) fn observe(
        &mut self,
        due: bool,
        status: Option<RecommendationOutcomeStatus>,
    ) -> Result<(), RecommendationOutcomeError> {
        self.publication_count = self
            .publication_count
            .checked_add(1)
            .ok_or(RecommendationOutcomeError::Arithmetic)?;
        if !due {
            self.pending_count = self
                .pending_count
                .checked_add(1)
                .ok_or(RecommendationOutcomeError::Arithmetic)?;
            return Ok(());
        }
        self.due_count = self
            .due_count
            .checked_add(1)
            .ok_or(RecommendationOutcomeError::Arithmetic)?;
        match status {
            Some(RecommendationOutcomeStatus::Completed(outcome)) => {
                self.completed_count = self
                    .completed_count
                    .checked_add(1)
                    .ok_or(RecommendationOutcomeError::Arithmetic)?;
                self.gross_price_return_sum = self
                    .gross_price_return_sum
                    .checked_add(outcome.gross_price_return())
                    .ok_or(RecommendationOutcomeError::Arithmetic)?;
                match outcome.gross_price_return().cmp(&Decimal::ZERO) {
                    std::cmp::Ordering::Greater => {
                        self.positive_outcomes = self
                            .positive_outcomes
                            .checked_add(1)
                            .ok_or(RecommendationOutcomeError::Arithmetic)?;
                    }
                    std::cmp::Ordering::Equal => {
                        self.zero_outcomes = self
                            .zero_outcomes
                            .checked_add(1)
                            .ok_or(RecommendationOutcomeError::Arithmetic)?;
                    }
                    std::cmp::Ordering::Less => {
                        self.negative_outcomes = self
                            .negative_outcomes
                            .checked_add(1)
                            .ok_or(RecommendationOutcomeError::Arithmetic)?;
                    }
                }
            }
            Some(RecommendationOutcomeStatus::Unavailable(_)) => {
                self.unavailable_count = self
                    .unavailable_count
                    .checked_add(1)
                    .ok_or(RecommendationOutcomeError::Arithmetic)?;
            }
            Some(RecommendationOutcomeStatus::Pending(_)) | None => {
                self.pending_count = self
                    .pending_count
                    .checked_add(1)
                    .ok_or(RecommendationOutcomeError::Arithmetic)?;
            }
        }
        Ok(())
    }

    pub(crate) fn finalize(&mut self) -> Result<(), RecommendationOutcomeError> {
        self.coverage_ppm = if self.due_count == 0 {
            0
        } else {
            u32::try_from(u64::from(self.completed_count) * 1_000_000 / u64::from(self.due_count))
                .unwrap_or(0)
        };
        if self.due_count == 0 {
            self.performance = RecommendationTrackRecordPerformance::UnavailableNoDueOutcomes;
        } else if self.completed_count < RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED {
            self.performance =
                RecommendationTrackRecordPerformance::UnavailableInsufficientCompletedSamples {
                    required: RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED,
                    actual: self.completed_count,
                };
        } else if self.coverage_ppm < RECOMMENDATION_TRACK_RECORD_MINIMUM_COVERAGE_PPM {
            self.performance =
                RecommendationTrackRecordPerformance::UnavailableInsufficientCoverage {
                    required_ppm: RECOMMENDATION_TRACK_RECORD_MINIMUM_COVERAGE_PPM,
                    actual_ppm: self.coverage_ppm,
                };
        } else {
            self.performance = RecommendationTrackRecordPerformance::Available {
                mean_gross_price_return: self
                    .gross_price_return_sum
                    .checked_div(Decimal::from(self.completed_count))
                    .ok_or(RecommendationOutcomeError::Arithmetic)?
                    .normalize(),
                positive_outcomes: self.positive_outcomes,
                zero_outcomes: self.zero_outcomes,
                negative_outcomes: self.negative_outcomes,
            };
        }
        Ok(())
    }

    /// Returns the exact action or no-action-control group.
    #[must_use]
    pub const fn cohort(&self) -> RecommendationOutcomeCohort {
        self.cohort
    }

    /// Returns every publication in the group, including not-yet-due records.
    #[must_use]
    pub const fn publication_count(&self) -> u32 {
        self.publication_count
    }

    /// Returns publications whose exact horizon elapsed by the query coordinate.
    #[must_use]
    pub const fn due_count(&self) -> u32 {
        self.due_count
    }

    /// Returns due publications with a completed receipt-bound outcome.
    #[must_use]
    pub const fn completed_count(&self) -> u32 {
        self.completed_count
    }

    /// Returns not-yet-due, missing, or explicitly pending publications.
    #[must_use]
    pub const fn pending_count(&self) -> u32 {
        self.pending_count
    }

    /// Returns due publications with a terminal unavailable outcome.
    #[must_use]
    pub const fn unavailable_count(&self) -> u32 {
        self.unavailable_count
    }

    /// Returns completed/due coverage in integer parts per million.
    #[must_use]
    pub const fn coverage_ppm(&self) -> u32 {
        self.coverage_ppm
    }

    /// Returns the sample- and coverage-governed gross price performance summary.
    #[must_use]
    pub const fn performance(&self) -> RecommendationTrackRecordPerformance {
        self.performance
    }
}

/// Honest current-status track record grouped by exact profile, action, and horizon.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendationTrackRecord {
    analytical_profile: AnalyticalProfileBindingReference,
    horizon_nanos: i64,
    evaluated_at: Timestamp,
    analysis_unavailable_count: u32,
    groups: Box<[RecommendationTrackRecordGroup]>,
}

impl RecommendationTrackRecord {
    pub(crate) fn new(
        analytical_profile: AnalyticalProfileBindingReference,
        horizon_nanos: i64,
        evaluated_at: Timestamp,
        analysis_unavailable_count: u32,
        groups: Vec<RecommendationTrackRecordGroup>,
    ) -> Self {
        Self {
            analytical_profile,
            horizon_nanos,
            evaluated_at,
            analysis_unavailable_count,
            groups: groups.into_boxed_slice(),
        }
    }

    /// Returns the exact profile binding used for grouping.
    #[must_use]
    pub const fn analytical_profile(&self) -> &AnalyticalProfileBindingReference {
        &self.analytical_profile
    }

    /// Returns the exact recommendation horizon duration.
    #[must_use]
    pub const fn horizon_nanos(&self) -> i64 {
        self.horizon_nanos
    }

    /// Returns the point-in-time currentness coordinate.
    #[must_use]
    pub const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    /// Returns analyses that issued no recommendation because required evidence was unavailable.
    #[must_use]
    pub const fn analysis_unavailable_count(&self) -> u32 {
        self.analysis_unavailable_count
    }

    /// Returns fixed-order action-specific groups followed by the no-action control.
    #[must_use]
    pub fn groups(&self) -> &[RecommendationTrackRecordGroup] {
        &self.groups
    }
}

pub(crate) fn ensure_publication_binding(
    decision: &InvestmentProposalDecision,
    publication: &PublishedInvestmentAnalysis,
) -> Result<(), RecommendationOutcomeError> {
    if publication.analysis_id != decision.analysis_id()
        || publication.proposal_id != decision.proposal_id()
        || publication.derivation_digest != decision.derivation_digest()
        || publication.policy_digest != decision.policy_digest()
        || publication.evidence_digest != decision.evidence_digest()
        || publication.account_id != decision.evidence().account_id()
        || publication.as_of != decision.evidence().as_of()
        || publication.horizon_at != decision.horizon_at()
        || publication.execution_eligibility
            != ProposalExecutionEligibility::ResearchOnlyExecutionIneligible
    {
        Err(RecommendationOutcomeError::BindingMismatch)
    } else {
        Ok(())
    }
}

fn publication_digest(value: &PublishedInvestmentAnalysis) -> InvestmentAnalysisPublicationId {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/investment-analysis-publication/v1\0");
    hash.update(value.analysis_id.bytes());
    option_digest(
        &mut hash,
        value.proposal_id.map(InvestmentProposalId::bytes),
    );
    option_digest(
        &mut hash,
        value
            .derivation_digest
            .map(RecommendationDerivationDigest::bytes),
    );
    hash.update(value.policy_digest.bytes());
    hash.update(value.evidence_digest.bytes());
    profile_hash(&mut hash, &value.analytical_profile);
    workflow_hash(&mut hash, &value.workflow);
    hash.update(value.account_id.as_uuid().as_bytes());
    hash.update(value.as_of.unix_nanos().to_be_bytes());
    hash.update(value.horizon_at.unix_nanos().to_be_bytes());
    hash.update(value.published_at.unix_nanos().to_be_bytes());
    InvestmentAnalysisPublicationId(hash.finalize().into())
}

fn outcome_series_id(publication: &PublishedInvestmentAnalysis) -> RecommendationOutcomeSeriesId {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/recommendation-outcome-series/v1\0");
    hash.update(publication.publication_id.bytes());
    hash.update(publication.horizon_at.unix_nanos().to_be_bytes());
    RecommendationOutcomeSeriesId(hash.finalize().into())
}

fn outcome_status_digest(
    value: &RecommendationOutcomeStatusRecord,
) -> RecommendationOutcomeStatusDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/recommendation-outcome-status/v1\0");
    hash.update(value.series_id.bytes());
    hash.update(value.publication_id.bytes());
    hash.update(value.analysis_id.bytes());
    option_digest(
        &mut hash,
        value.proposal_id.map(InvestmentProposalId::bytes),
    );
    cohort_hash(&mut hash, value.cohort);
    hash.update(value.horizon_at.unix_nanos().to_be_bytes());
    hash.update(value.revision.get().to_be_bytes());
    option_digest(
        &mut hash,
        value
            .previous_status_digest
            .map(RecommendationOutcomeStatusDigest::bytes),
    );
    hash.update(value.evaluated_at.unix_nanos().to_be_bytes());
    match value.status {
        RecommendationOutcomeStatus::Pending(reason) => {
            hash.update([0]);
            hash.update([pending_reason_tag(reason)]);
        }
        RecommendationOutcomeStatus::Unavailable(reason) => {
            hash.update([1]);
            unavailable_reason_hash(&mut hash, reason);
        }
        RecommendationOutcomeStatus::Completed(outcome) => {
            hash.update([2]);
            money_hash(&mut hash, outcome.start_mark);
            let observation = outcome.observation;
            money_hash(&mut hash, observation.endpoint_price);
            hash.update(observation.observed_at.unix_nanos().to_be_bytes());
            hash.update(observation.available_at.unix_nanos().to_be_bytes());
            digest_hash(&mut hash, observation.selection_receipt_identity);
            digest_hash(&mut hash, observation.selected_observation_identity);
            digest_hash(
                &mut hash,
                observation.no_applicable_corporate_actions_identity,
            );
            decimal_hash(&mut hash, outcome.gross_price_return);
        }
    }
    RecommendationOutcomeStatusDigest(hash.finalize().into())
}

fn profile_hash(hash: &mut Sha256, value: &AnalyticalProfileBindingReference) {
    text_hash(hash, value.profile_id.as_str());
    hash.update(value.revision.get().to_be_bytes());
    digest_hash(hash, value.content_digest);
}

fn workflow_hash(hash: &mut Sha256, value: &InvestmentAnalysisWorkflowReference) {
    text_hash(hash, value.workflow_id.as_str());
    hash.update(value.revision.get().to_be_bytes());
    digest_hash(hash, value.content_digest);
}

fn cohort_hash(hash: &mut Sha256, value: RecommendationOutcomeCohort) {
    match value {
        RecommendationOutcomeCohort::Generated(action) => {
            hash.update([0, action_tag(action)]);
        }
        RecommendationOutcomeCohort::NoActionControl => hash.update([1]),
        RecommendationOutcomeCohort::AnalysisUnavailable => hash.update([2]),
    }
}

fn unavailable_reason_hash(hash: &mut Sha256, value: RecommendationOutcomeUnavailableReason) {
    match value {
        RecommendationOutcomeUnavailableReason::AnalysisUnavailable(reason) => {
            hash.update([0]);
            // The proposal authority already commits the full typed reason into analysis_id.
            hash.update(proposal_unavailable_reason_tag(reason).to_be_bytes());
        }
        RecommendationOutcomeUnavailableReason::OutcomeObservationUnavailable => hash.update([1]),
        RecommendationOutcomeUnavailableReason::AmbiguousOutcomeObservation => hash.update([2]),
        RecommendationOutcomeUnavailableReason::IncompleteOutcomeObservation => hash.update([3]),
        RecommendationOutcomeUnavailableReason::CorporateActionEvidenceUnavailable => {
            hash.update([4]);
        }
    }
}

const fn pending_reason_tag(value: RecommendationOutcomePendingReason) -> u8 {
    match value {
        RecommendationOutcomePendingReason::AwaitingHorizon => 0,
        RecommendationOutcomePendingReason::AwaitingOutcomeEvidence => 1,
    }
}

const fn action_tag(value: RecommendationAction) -> u8 {
    match value {
        RecommendationAction::Buy => 0,
        RecommendationAction::Add => 1,
        RecommendationAction::Hold => 2,
        RecommendationAction::Trim => 3,
        RecommendationAction::Sell => 4,
    }
}

const fn proposal_unavailable_reason_tag(value: ProposalUnavailableReason) -> u16 {
    match value {
        ProposalUnavailableReason::MissingEvidence(_) => 0,
        ProposalUnavailableReason::InstrumentMismatch { .. } => 1,
        ProposalUnavailableReason::CurrencyMismatch { .. } => 2,
        ProposalUnavailableReason::AccountMismatch { .. } => 3,
        ProposalUnavailableReason::NotAvailableAtCutoff(_) => 4,
        ProposalUnavailableReason::ExpiredEvidence(_) => 5,
        ProposalUnavailableReason::StaleEvidence(_) => 6,
        ProposalUnavailableReason::RejectedQuality { .. } => 7,
        ProposalUnavailableReason::ForecastHorizonMismatch { .. } => 8,
        ProposalUnavailableReason::ValuationHorizonMismatch { .. } => 9,
        ProposalUnavailableReason::BacktestHorizonMismatch { .. } => 10,
        ProposalUnavailableReason::InsufficientForecastOutcomes { .. } => 11,
        ProposalUnavailableReason::UnsupportedForecastCoverage { .. } => 12,
        ProposalUnavailableReason::InsufficientBacktestObservations { .. } => 13,
        ProposalUnavailableReason::InsufficientBacktestTrials { .. } => 14,
        ProposalUnavailableReason::ReservedPortfolioRevision => 15,
    }
}

fn option_digest(hash: &mut Sha256, value: Option<[u8; 32]>) {
    if let Some(value) = value {
        hash.update([1]);
        hash.update(value);
    } else {
        hash.update([0]);
    }
}

fn digest_hash(hash: &mut Sha256, value: DecisionContentDigest) {
    let digest = value.evidence_digest();
    hash.update([match digest.algorithm() {
        market_squawk_domain::DigestAlgorithm::Sha256 => 0,
        market_squawk_domain::DigestAlgorithm::Blake3 => 1,
    }]);
    hash.update(digest.bytes());
}

fn money_hash(hash: &mut Sha256, value: Money) {
    decimal_hash(hash, value.amount());
    text_hash(hash, value.currency().as_str());
}

fn decimal_hash(hash: &mut Sha256, value: Decimal) {
    let value = value.normalize();
    hash.update(value.mantissa().to_be_bytes());
    hash.update(value.scale().to_be_bytes());
}

fn text_hash(hash: &mut Sha256, value: &str) {
    hash.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(value.as_bytes());
}
