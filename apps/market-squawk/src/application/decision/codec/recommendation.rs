use std::num::NonZeroU32;

use market_squawk_decisions::{
    AnalyticalProfileBindingReference, CandidatePortfolioSizingState, CandidateSizingConstraints,
    CapacityRange, ExactPositionScale, InvestmentAnalysisWorkflowReference,
    InvestmentOutcomeProjection, InvestmentProjectionDigest, InvestmentProposalDecision,
    InvestmentProposalId, InvestmentSizingInputs, InvestmentSizingProjection, LotRange,
    NonnegativeMoneyRange, PublishedInvestmentAnalysis, RecommendationOutcomeObservation,
    RecommendationOutcomePendingReason, RecommendationOutcomeStatus,
    RecommendationOutcomeStatusDigest, RecommendationOutcomeStatusRecord,
    RecommendationOutcomeUnavailableReason, SizingCapacityAvailability, SizingCapacityEvidence,
};
use market_squawk_domain::{
    AccountId, EvidenceDigest, InstrumentDefinitionRevision, InstrumentExecutionTerms,
    InstrumentId, Money, QuantityLots, RevisionNumber, SourceIdentifier, Timestamp,
};
use market_squawk_portfolio::PortfolioRevisionToken;
use serde::{Deserialize, Serialize};

use super::super::DecisionApplicationError;
use super::common::content_digest;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InvestmentAnalysisPublicationWire {
    publication_id: [u8; 32],
    analysis_id: [u8; 32],
    profile: AnalyticalProfileWire,
    workflow: WorkflowWire,
    published_at: Timestamp,
}

impl From<&PublishedInvestmentAnalysis> for InvestmentAnalysisPublicationWire {
    fn from(value: &PublishedInvestmentAnalysis) -> Self {
        Self {
            publication_id: value.publication_id().bytes(),
            analysis_id: value.analysis_id().bytes(),
            profile: value.analytical_profile().into(),
            workflow: value.workflow().into(),
            published_at: value.published_at(),
        }
    }
}

impl InvestmentAnalysisPublicationWire {
    pub(super) fn key(&self) -> String {
        format!("publication:{}", hex(self.analysis_id))
    }

    pub(super) fn analysis_id(&self) -> [u8; 32] {
        self.analysis_id
    }

    pub(super) fn decode(
        self,
        decision: &InvestmentProposalDecision,
    ) -> Result<PublishedInvestmentAnalysis, DecisionApplicationError> {
        let value = PublishedInvestmentAnalysis::try_new(
            decision,
            self.profile.decode()?,
            self.workflow.decode()?,
            self.published_at,
        )
        .map_err(invalid_state)?;
        if value.analysis_id().bytes() != self.analysis_id
            || value.publication_id().bytes() != self.publication_id
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AnalyticalProfileWire {
    profile_id: SourceIdentifier,
    revision: u32,
    content_digest: EvidenceDigest,
}

impl From<&AnalyticalProfileBindingReference> for AnalyticalProfileWire {
    fn from(value: &AnalyticalProfileBindingReference) -> Self {
        Self {
            profile_id: value.profile_id().clone(),
            revision: value.revision().get(),
            content_digest: value.content_digest().evidence_digest(),
        }
    }
}

impl AnalyticalProfileWire {
    fn decode(self) -> Result<AnalyticalProfileBindingReference, DecisionApplicationError> {
        Ok(AnalyticalProfileBindingReference::new(
            self.profile_id,
            NonZeroU32::new(self.revision)
                .ok_or(DecisionApplicationError::InvalidPersistentState)?,
            content_digest(self.content_digest)?,
        ))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkflowWire {
    workflow_id: SourceIdentifier,
    revision: u32,
    content_digest: EvidenceDigest,
}

impl From<&InvestmentAnalysisWorkflowReference> for WorkflowWire {
    fn from(value: &InvestmentAnalysisWorkflowReference) -> Self {
        Self {
            workflow_id: value.workflow_id().clone(),
            revision: value.revision().get(),
            content_digest: value.content_digest().evidence_digest(),
        }
    }
}

impl WorkflowWire {
    fn decode(self) -> Result<InvestmentAnalysisWorkflowReference, DecisionApplicationError> {
        Ok(InvestmentAnalysisWorkflowReference::new(
            self.workflow_id,
            NonZeroU32::new(self.revision)
                .ok_or(DecisionApplicationError::InvalidPersistentState)?,
            content_digest(self.content_digest)?,
        ))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InvestmentOutcomeProjectionWire {
    proposal_id: [u8; 32],
    derivation_digest: [u8; 32],
    result_digest: [u8; 32],
    position_scale: Option<PositionScaleWire>,
}

impl From<&InvestmentOutcomeProjection> for InvestmentOutcomeProjectionWire {
    fn from(value: &InvestmentOutcomeProjection) -> Self {
        Self {
            proposal_id: value.binding().proposal_id().bytes(),
            derivation_digest: value.binding().derivation_digest().bytes(),
            result_digest: value.result_digest().bytes(),
            position_scale: value.position_scale().map(Into::into),
        }
    }
}

impl InvestmentOutcomeProjectionWire {
    pub(super) fn key(&self) -> String {
        format!("outcome_projection:{}", hex(self.proposal_id))
    }

    pub(super) fn proposal_id(&self) -> [u8; 32] {
        self.proposal_id
    }

    pub(super) fn decode(
        self,
        proposal: &market_squawk_decisions::GeneratedInvestmentProposal,
    ) -> Result<InvestmentOutcomeProjection, DecisionApplicationError> {
        let value = InvestmentOutcomeProjection::try_from_proposal(
            proposal,
            self.position_scale
                .map(PositionScaleWire::decode)
                .transpose()?,
        )
        .map_err(invalid_state)?;
        ensure_projection_identity(
            proposal.proposal_id(),
            proposal.derivation_digest().bytes(),
            value.result_digest(),
            self.proposal_id,
            self.derivation_digest,
            self.result_digest,
        )?;
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PositionScaleWire {
    terms: InstrumentExecutionTerms,
    quantity_lots: i64,
}

impl From<ExactPositionScale> for PositionScaleWire {
    fn from(value: ExactPositionScale) -> Self {
        Self {
            terms: value.terms(),
            quantity_lots: value.quantity().get(),
        }
    }
}

impl PositionScaleWire {
    fn decode(self) -> Result<ExactPositionScale, DecisionApplicationError> {
        Ok(ExactPositionScale::new(
            self.terms,
            QuantityLots::new(self.quantity_lots).map_err(invalid_state)?,
        ))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InvestmentSizingProjectionWire {
    proposal_id: [u8; 32],
    derivation_digest: [u8; 32],
    result_digest: [u8; 32],
    inputs: InvestmentSizingInputsWire,
}

impl From<&InvestmentSizingProjection> for InvestmentSizingProjectionWire {
    fn from(value: &InvestmentSizingProjection) -> Self {
        Self {
            proposal_id: value.binding().proposal_id().bytes(),
            derivation_digest: value.binding().derivation_digest().bytes(),
            result_digest: value.result_digest().bytes(),
            inputs: value.inputs().into(),
        }
    }
}

impl InvestmentSizingProjectionWire {
    pub(super) fn key(&self) -> String {
        format!("sizing_projection:{}", hex(self.proposal_id))
    }

    pub(super) fn proposal_id(&self) -> [u8; 32] {
        self.proposal_id
    }

    pub(super) fn decode(
        self,
        proposal: &market_squawk_decisions::GeneratedInvestmentProposal,
    ) -> Result<InvestmentSizingProjection, DecisionApplicationError> {
        let value = InvestmentSizingProjection::try_from_proposal(proposal, self.inputs.decode()?)
            .map_err(invalid_state)?;
        ensure_projection_identity(
            proposal.proposal_id(),
            proposal.derivation_digest().bytes(),
            value.result_digest(),
            self.proposal_id,
            self.derivation_digest,
            self.result_digest,
        )?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct InvestmentSizingInputsWire {
    evaluated_at: Timestamp,
    execution_terms: InstrumentExecutionTerms,
    selected_mark: Money,
    portfolio: CandidatePortfolioSizingStateWire,
    constraints: CandidateSizingConstraintsWire,
    liquidity_capacity: SizingCapacityAvailabilityWire,
    risk_capacity: SizingCapacityAvailabilityWire,
    forward_cost_capacity: SizingCapacityAvailabilityWire,
}

impl From<&InvestmentSizingInputs> for InvestmentSizingInputsWire {
    fn from(value: &InvestmentSizingInputs) -> Self {
        Self {
            evaluated_at: value.evaluated_at(),
            execution_terms: value.execution_terms(),
            selected_mark: value.selected_mark(),
            portfolio: value.portfolio().into(),
            constraints: value.constraints().into(),
            liquidity_capacity: value.liquidity_capacity().into(),
            risk_capacity: value.risk_capacity().into(),
            forward_cost_capacity: value.forward_cost_capacity().into(),
        }
    }
}

impl InvestmentSizingInputsWire {
    fn decode(self) -> Result<InvestmentSizingInputs, DecisionApplicationError> {
        Ok(InvestmentSizingInputs::new(
            self.evaluated_at,
            self.execution_terms,
            self.selected_mark,
            self.portfolio.decode()?,
            self.constraints.decode()?,
            self.liquidity_capacity.decode()?,
            self.risk_capacity.decode()?,
            self.forward_cost_capacity.decode()?,
        ))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CandidatePortfolioSizingStateWire {
    account_id: AccountId,
    instrument_id: InstrumentId,
    portfolio_revision: [u8; 32],
    marked_equity_at_selected_mark: Money,
    settlement_available_cash: Money,
    current_lots: i64,
}

impl From<&CandidatePortfolioSizingState> for CandidatePortfolioSizingStateWire {
    fn from(value: &CandidatePortfolioSizingState) -> Self {
        Self {
            account_id: value.account_id(),
            instrument_id: value.instrument_id(),
            portfolio_revision: value.portfolio_revision().bytes(),
            marked_equity_at_selected_mark: value.marked_equity_at_selected_mark(),
            settlement_available_cash: value.settlement_available_cash(),
            current_lots: value.current_lots().get(),
        }
    }
}

impl CandidatePortfolioSizingStateWire {
    fn decode(self) -> Result<CandidatePortfolioSizingState, DecisionApplicationError> {
        CandidatePortfolioSizingState::try_new(
            self.account_id,
            self.instrument_id,
            PortfolioRevisionToken::from_bytes(self.portfolio_revision),
            self.marked_equity_at_selected_mark,
            self.settlement_available_cash,
            QuantityLots::new(self.current_lots).map_err(invalid_state)?,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CandidateSizingConstraintsWire {
    minimum_cash_reserve: Money,
    preferred_weight_lower_basis_points: u16,
    preferred_weight_upper_basis_points: u16,
    maximum_downside_loss_basis_points: u16,
}

impl From<CandidateSizingConstraints> for CandidateSizingConstraintsWire {
    fn from(value: CandidateSizingConstraints) -> Self {
        Self {
            minimum_cash_reserve: value.minimum_cash_reserve(),
            preferred_weight_lower_basis_points: value.preferred_weight_lower_basis_points(),
            preferred_weight_upper_basis_points: value.preferred_weight_upper_basis_points(),
            maximum_downside_loss_basis_points: value.maximum_downside_loss_basis_points(),
        }
    }
}

impl CandidateSizingConstraintsWire {
    fn decode(self) -> Result<CandidateSizingConstraints, DecisionApplicationError> {
        CandidateSizingConstraints::try_new(
            self.minimum_cash_reserve,
            self.preferred_weight_lower_basis_points,
            self.preferred_weight_upper_basis_points,
            self.maximum_downside_loss_basis_points,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum SizingCapacityAvailabilityWire {
    Available(SizingCapacityEvidenceWire),
    UnavailableNotSupplied,
}

impl From<&SizingCapacityAvailability> for SizingCapacityAvailabilityWire {
    fn from(value: &SizingCapacityAvailability) -> Self {
        match value {
            SizingCapacityAvailability::Available(evidence) => {
                Self::Available(evidence.as_ref().into())
            }
            SizingCapacityAvailability::UnavailableNotSupplied => Self::UnavailableNotSupplied,
        }
    }
}

impl SizingCapacityAvailabilityWire {
    fn decode(self) -> Result<SizingCapacityAvailability, DecisionApplicationError> {
        match self {
            Self::Available(value) => Ok(SizingCapacityAvailability::Available(Box::new(
                value.decode()?,
            ))),
            Self::UnavailableNotSupplied => Ok(SizingCapacityAvailability::UnavailableNotSupplied),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SizingCapacityEvidenceWire {
    instrument_id: InstrumentId,
    account_id: AccountId,
    portfolio_revision: [u8; 32],
    definition_revision: u64,
    reference_mark: Money,
    range: CapacityRangeWire,
    content_identity: EvidenceDigest,
    observed_at: Timestamp,
    available_at: Timestamp,
    expires_at: Timestamp,
}

impl From<&SizingCapacityEvidence> for SizingCapacityEvidenceWire {
    fn from(value: &SizingCapacityEvidence) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            account_id: value.account_id(),
            portfolio_revision: value.portfolio_revision().bytes(),
            definition_revision: value.definition_revision().get(),
            reference_mark: value.reference_mark(),
            range: value.range().into(),
            content_identity: value.content_identity().evidence_digest(),
            observed_at: value.observed_at(),
            available_at: value.available_at(),
            expires_at: value.expires_at(),
        }
    }
}

impl SizingCapacityEvidenceWire {
    fn decode(self) -> Result<SizingCapacityEvidence, DecisionApplicationError> {
        SizingCapacityEvidence::try_new(
            self.instrument_id,
            self.account_id,
            PortfolioRevisionToken::from_bytes(self.portfolio_revision),
            InstrumentDefinitionRevision::try_from(self.definition_revision)
                .map_err(invalid_state)?,
            self.reference_mark,
            self.range.decode()?,
            content_digest(self.content_identity)?,
            self.observed_at,
            self.available_at,
            self.expires_at,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum CapacityRangeWire {
    Lots { lower: i64, upper: i64 },
    Notional { lower: Money, upper: Money },
}

impl From<CapacityRange> for CapacityRangeWire {
    fn from(value: CapacityRange) -> Self {
        match value {
            CapacityRange::Lots(value) => Self::Lots {
                lower: value.lower().get(),
                upper: value.upper().get(),
            },
            CapacityRange::Notional(value) => Self::Notional {
                lower: value.lower(),
                upper: value.upper(),
            },
        }
    }
}

impl CapacityRangeWire {
    fn decode(self) -> Result<CapacityRange, DecisionApplicationError> {
        match self {
            Self::Lots { lower, upper } => Ok(CapacityRange::Lots(
                LotRange::try_new(
                    QuantityLots::new(lower).map_err(invalid_state)?,
                    QuantityLots::new(upper).map_err(invalid_state)?,
                )
                .map_err(invalid_state)?,
            )),
            Self::Notional { lower, upper } => Ok(CapacityRange::Notional(
                NonnegativeMoneyRange::try_new(lower, upper).map_err(invalid_state)?,
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecommendationOutcomeStatusWire {
    series_id: [u8; 32],
    status_digest: [u8; 32],
    publication_id: [u8; 32],
    analysis_id: [u8; 32],
    proposal_id: Option<[u8; 32]>,
    revision: u32,
    previous_status_digest: Option<[u8; 32]>,
    evaluated_at: Timestamp,
    status: RecommendationOutcomeStatusWireValue,
}

impl From<&RecommendationOutcomeStatusRecord> for RecommendationOutcomeStatusWire {
    fn from(value: &RecommendationOutcomeStatusRecord) -> Self {
        Self {
            series_id: value.series_id().bytes(),
            status_digest: value.status_digest().bytes(),
            publication_id: value.publication_id().bytes(),
            analysis_id: value.analysis_id().bytes(),
            proposal_id: value.proposal_id().map(InvestmentProposalId::bytes),
            revision: value.revision().get(),
            previous_status_digest: value
                .previous_status_digest()
                .map(RecommendationOutcomeStatusDigest::bytes),
            evaluated_at: value.evaluated_at(),
            status: value.status().into(),
        }
    }
}

impl RecommendationOutcomeStatusWire {
    pub(super) fn key(&self) -> String {
        format!("outcome_status:{}:{}", hex(self.series_id), self.revision)
    }

    pub(super) fn analysis_id(&self) -> [u8; 32] {
        self.analysis_id
    }

    pub(super) fn decode(
        self,
        decision: &InvestmentProposalDecision,
        publication: &PublishedInvestmentAnalysis,
    ) -> Result<RecommendationOutcomeStatusRecord, DecisionApplicationError> {
        let revision = RevisionNumber::new(self.revision).map_err(invalid_state)?;
        let previous = self
            .previous_status_digest
            .map(RecommendationOutcomeStatusDigest::try_from_bytes)
            .transpose()
            .map_err(invalid_state)?;
        let value = match self.status {
            RecommendationOutcomeStatusWireValue::Pending { reason } => {
                RecommendationOutcomeStatusRecord::try_pending(
                    decision,
                    publication,
                    revision,
                    previous,
                    self.evaluated_at,
                    reason.into(),
                )
            }
            RecommendationOutcomeStatusWireValue::Unavailable { reason } => {
                let reason = reason.decode(decision)?;
                RecommendationOutcomeStatusRecord::try_unavailable(
                    decision,
                    publication,
                    revision,
                    previous,
                    self.evaluated_at,
                    reason,
                )
            }
            RecommendationOutcomeStatusWireValue::Completed { observation } => {
                RecommendationOutcomeStatusRecord::try_completed(
                    decision,
                    publication,
                    revision,
                    previous,
                    self.evaluated_at,
                    observation.decode()?,
                )
            }
        }
        .map_err(invalid_state)?;
        if value.series_id().bytes() != self.series_id
            || value.status_digest().bytes() != self.status_digest
            || value.publication_id().bytes() != self.publication_id
            || value.analysis_id().bytes() != self.analysis_id
            || value.proposal_id().map(InvestmentProposalId::bytes) != self.proposal_id
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum RecommendationOutcomeStatusWireValue {
    Pending {
        reason: RecommendationOutcomePendingReasonWire,
    },
    Unavailable {
        reason: RecommendationOutcomeUnavailableReasonWire,
    },
    Completed {
        observation: RecommendationOutcomeObservationWire,
    },
}

impl From<RecommendationOutcomeStatus> for RecommendationOutcomeStatusWireValue {
    fn from(value: RecommendationOutcomeStatus) -> Self {
        match value {
            RecommendationOutcomeStatus::Pending(reason) => Self::Pending {
                reason: reason.into(),
            },
            RecommendationOutcomeStatus::Unavailable(reason) => Self::Unavailable {
                reason: reason.into(),
            },
            RecommendationOutcomeStatus::Completed(value) => Self::Completed {
                observation: value.observation().into(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RecommendationOutcomePendingReasonWire {
    AwaitingHorizon,
    AwaitingOutcomeEvidence,
}

impl From<RecommendationOutcomePendingReason> for RecommendationOutcomePendingReasonWire {
    fn from(value: RecommendationOutcomePendingReason) -> Self {
        match value {
            RecommendationOutcomePendingReason::AwaitingHorizon => Self::AwaitingHorizon,
            RecommendationOutcomePendingReason::AwaitingOutcomeEvidence => {
                Self::AwaitingOutcomeEvidence
            }
        }
    }
}

impl From<RecommendationOutcomePendingReasonWire> for RecommendationOutcomePendingReason {
    fn from(value: RecommendationOutcomePendingReasonWire) -> Self {
        match value {
            RecommendationOutcomePendingReasonWire::AwaitingHorizon => Self::AwaitingHorizon,
            RecommendationOutcomePendingReasonWire::AwaitingOutcomeEvidence => {
                Self::AwaitingOutcomeEvidence
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RecommendationOutcomeUnavailableReasonWire {
    AnalysisUnavailable,
    OutcomeObservationUnavailable,
    AmbiguousOutcomeObservation,
    IncompleteOutcomeObservation,
    CorporateActionEvidenceUnavailable,
}

impl From<RecommendationOutcomeUnavailableReason> for RecommendationOutcomeUnavailableReasonWire {
    fn from(value: RecommendationOutcomeUnavailableReason) -> Self {
        match value {
            RecommendationOutcomeUnavailableReason::AnalysisUnavailable(_) => {
                Self::AnalysisUnavailable
            }
            RecommendationOutcomeUnavailableReason::OutcomeObservationUnavailable => {
                Self::OutcomeObservationUnavailable
            }
            RecommendationOutcomeUnavailableReason::AmbiguousOutcomeObservation => {
                Self::AmbiguousOutcomeObservation
            }
            RecommendationOutcomeUnavailableReason::IncompleteOutcomeObservation => {
                Self::IncompleteOutcomeObservation
            }
            RecommendationOutcomeUnavailableReason::CorporateActionEvidenceUnavailable => {
                Self::CorporateActionEvidenceUnavailable
            }
        }
    }
}

impl RecommendationOutcomeUnavailableReasonWire {
    fn decode(
        self,
        decision: &InvestmentProposalDecision,
    ) -> Result<RecommendationOutcomeUnavailableReason, DecisionApplicationError> {
        Ok(match self {
            Self::AnalysisUnavailable => match decision {
                InvestmentProposalDecision::Unavailable(value) => {
                    RecommendationOutcomeUnavailableReason::AnalysisUnavailable(value.reason())
                }
                InvestmentProposalDecision::Generated(_)
                | InvestmentProposalDecision::NoAction(_) => {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
            },
            Self::OutcomeObservationUnavailable => {
                RecommendationOutcomeUnavailableReason::OutcomeObservationUnavailable
            }
            Self::AmbiguousOutcomeObservation => {
                RecommendationOutcomeUnavailableReason::AmbiguousOutcomeObservation
            }
            Self::IncompleteOutcomeObservation => {
                RecommendationOutcomeUnavailableReason::IncompleteOutcomeObservation
            }
            Self::CorporateActionEvidenceUnavailable => {
                RecommendationOutcomeUnavailableReason::CorporateActionEvidenceUnavailable
            }
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RecommendationOutcomeObservationWire {
    endpoint_price: Money,
    observed_at: Timestamp,
    available_at: Timestamp,
    selection_receipt_identity: EvidenceDigest,
    selected_observation_identity: EvidenceDigest,
    no_applicable_corporate_actions_identity: EvidenceDigest,
}

impl From<RecommendationOutcomeObservation> for RecommendationOutcomeObservationWire {
    fn from(value: RecommendationOutcomeObservation) -> Self {
        Self {
            endpoint_price: value.endpoint_price(),
            observed_at: value.observed_at(),
            available_at: value.available_at(),
            selection_receipt_identity: value.selection_receipt_identity().evidence_digest(),
            selected_observation_identity: value.selected_observation_identity().evidence_digest(),
            no_applicable_corporate_actions_identity: value
                .no_applicable_corporate_actions_identity()
                .evidence_digest(),
        }
    }
}

impl RecommendationOutcomeObservationWire {
    fn decode(self) -> Result<RecommendationOutcomeObservation, DecisionApplicationError> {
        RecommendationOutcomeObservation::try_new(
            self.endpoint_price,
            self.observed_at,
            self.available_at,
            content_digest(self.selection_receipt_identity)?,
            content_digest(self.selected_observation_identity)?,
            content_digest(self.no_applicable_corporate_actions_identity)?,
        )
        .map_err(invalid_state)
    }
}

fn ensure_projection_identity(
    actual_proposal_id: InvestmentProposalId,
    actual_derivation_digest: [u8; 32],
    actual_result_digest: InvestmentProjectionDigest,
    proposal_id: [u8; 32],
    derivation_digest: [u8; 32],
    result_digest: [u8; 32],
) -> Result<(), DecisionApplicationError> {
    if actual_proposal_id.bytes() != proposal_id
        || actual_derivation_digest != derivation_digest
        || actual_result_digest.bytes() != result_digest
    {
        Err(DecisionApplicationError::InvalidPersistentState)
    } else {
        Ok(())
    }
}

fn invalid_state<T>(_error: T) -> DecisionApplicationError {
    DecisionApplicationError::InvalidPersistentState
}

fn hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}
