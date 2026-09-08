//! Strict, versioned wire records for the append-only decision journal.

pub(super) mod candidate;
mod common;
mod dossier;
mod proposal;
mod published_analysis;
mod recommendation;
mod recovery;
pub(super) mod screen;
mod target;
mod wire;

use market_squawk_decisions::{
    DecisionDossier, GovernedTargetSet, InvestmentOutcomeProjection, InvestmentProposalDecision,
    InvestmentSizingProjection, PreparedPublishedInvestmentAnalysis, PublishedInvestmentAnalysis,
    RecommendationOutcomeStatusRecord, SavedScreen, ScreenExecution, TargetInvalidation,
    TargetReview,
};
use market_squawk_domain::Timestamp;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use self::candidate::ExecutionWire;
use self::common::revision_key;
use self::dossier::DossierWire;
use self::proposal::InvestmentProposalWire;
use self::published_analysis::PreparedPublishedInvestmentAnalysisWire;
use self::recommendation::{
    InvestmentAnalysisPublicationWire, InvestmentOutcomeProjectionWire,
    InvestmentSizingProjectionWire, RecommendationOutcomeStatusWire,
};
pub(super) use self::recovery::RecoveryContext;
use self::screen::ScreenWire;
use self::target::{InvalidationWire, ReviewWire, TargetWire};
use self::wire::{
    KIND_DOSSIER, KIND_EXECUTION, KIND_INVALIDATION, KIND_INVESTMENT_PROPOSAL, KIND_REVIEW,
    KIND_SCREEN, KIND_SCREEN_JOB_INPUT, KIND_TARGET, WIRE_VERSION, WireEnvelope, WireRecord,
};
use super::DecisionApplicationError;
use super::screen_workflow::{ScreenJobPlan, ScreenJobPlanWire};

#[derive(Debug)]
pub(super) struct EncodedRecord {
    pub(super) kind: i64,
    pub(super) key: String,
    pub(super) payload: Vec<u8>,
    pub(super) digest: [u8; 32],
}

impl EncodedRecord {
    fn try_new(
        kind: i64,
        key: String,
        record: WireRecord,
    ) -> Result<Self, DecisionApplicationError> {
        let payload = encode(&WireEnvelope {
            version: WIRE_VERSION,
            record,
        })?;
        Ok(Self {
            kind,
            key,
            digest: Sha256::digest(&payload).into(),
            payload,
        })
    }
}

pub(super) fn screen(screen: &SavedScreen) -> Result<EncodedRecord, DecisionApplicationError> {
    EncodedRecord::try_new(
        KIND_SCREEN,
        revision_key(
            screen.revision().id().as_str(),
            screen.revision().revision(),
        ),
        WireRecord::Screen(ScreenWire::from(screen)),
    )
}

pub(super) fn execution(
    execution: &ScreenExecution,
    selected_at: Timestamp,
) -> Result<EncodedRecord, DecisionApplicationError> {
    EncodedRecord::try_new(
        KIND_EXECUTION,
        execution.run().id().as_str().to_owned(),
        WireRecord::Execution(ExecutionWire::from_execution(execution, selected_at)?),
    )
}

pub(super) fn dossier(
    dossier: &DecisionDossier,
) -> Result<EncodedRecord, DecisionApplicationError> {
    EncodedRecord::try_new(
        KIND_DOSSIER,
        dossier.dossier().id().as_str().to_owned(),
        WireRecord::Dossier(DossierWire::from(dossier)),
    )
}

pub(super) fn screen_job_input(
    plan: &ScreenJobPlan,
) -> Result<EncodedRecord, DecisionApplicationError> {
    EncodedRecord::try_new(
        KIND_SCREEN_JOB_INPUT,
        plan.run().id().as_str().to_owned(),
        WireRecord::ScreenJobInput(Box::new(ScreenJobPlanWire::from_plan(plan))),
    )
}

pub(super) fn target(
    target: &GovernedTargetSet,
) -> Result<EncodedRecord, DecisionApplicationError> {
    EncodedRecord::try_new(
        KIND_TARGET,
        revision_key(target.target().id().as_str(), target.target().revision()),
        WireRecord::Target(Box::new(TargetWire::from(target))),
    )
}

pub(super) fn review(review: &TargetReview) -> Result<EncodedRecord, DecisionApplicationError> {
    EncodedRecord::try_new(
        KIND_REVIEW,
        review.id().as_str().to_owned(),
        WireRecord::Review(ReviewWire::from(review)),
    )
}

pub(super) fn invalidation(
    invalidation: &TargetInvalidation,
) -> Result<EncodedRecord, DecisionApplicationError> {
    EncodedRecord::try_new(
        KIND_INVALIDATION,
        invalidation.id().as_str().to_owned(),
        WireRecord::Invalidation(InvalidationWire::from(invalidation)),
    )
}

pub(super) fn investment_proposal(
    decision: &InvestmentProposalDecision,
) -> Result<EncodedRecord, DecisionApplicationError> {
    if decision.evidence().selected_candidate().is_some() {
        return Err(DecisionApplicationError::InvalidPersistentState);
    }
    let wire = InvestmentProposalWire::from(decision);
    EncodedRecord::try_new(
        KIND_INVESTMENT_PROPOSAL,
        wire.key()?,
        WireRecord::InvestmentProposal(Box::new(wire)),
    )
}

pub(super) fn investment_analysis_publication(
    publication: &PublishedInvestmentAnalysis,
) -> Result<EncodedRecord, DecisionApplicationError> {
    let wire = InvestmentAnalysisPublicationWire::from(publication);
    EncodedRecord::try_new(
        KIND_INVESTMENT_PROPOSAL,
        wire.key(),
        WireRecord::InvestmentAnalysisPublication(wire),
    )
}

pub(super) fn prepared_published_investment_analysis(
    bundle: &PreparedPublishedInvestmentAnalysis,
) -> Result<EncodedRecord, DecisionApplicationError> {
    let wire = PreparedPublishedInvestmentAnalysisWire::from(bundle);
    EncodedRecord::try_new(
        KIND_INVESTMENT_PROPOSAL,
        wire.key()?,
        WireRecord::PreparedPublishedInvestmentAnalysis(Box::new(wire)),
    )
}

pub(super) fn investment_outcome_projection(
    projection: &InvestmentOutcomeProjection,
) -> Result<EncodedRecord, DecisionApplicationError> {
    let wire = InvestmentOutcomeProjectionWire::from(projection);
    EncodedRecord::try_new(
        KIND_INVESTMENT_PROPOSAL,
        wire.key(),
        WireRecord::InvestmentOutcomeProjection(wire),
    )
}

pub(super) fn investment_sizing_projection(
    projection: &InvestmentSizingProjection,
) -> Result<EncodedRecord, DecisionApplicationError> {
    let wire = InvestmentSizingProjectionWire::from(projection);
    EncodedRecord::try_new(
        KIND_INVESTMENT_PROPOSAL,
        wire.key(),
        WireRecord::InvestmentSizingProjection(Box::new(wire)),
    )
}

pub(super) fn recommendation_outcome_status(
    status: &RecommendationOutcomeStatusRecord,
) -> Result<EncodedRecord, DecisionApplicationError> {
    let wire = RecommendationOutcomeStatusWire::from(status);
    EncodedRecord::try_new(
        KIND_INVESTMENT_PROPOSAL,
        wire.key(),
        WireRecord::RecommendationOutcomeStatus(wire),
    )
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>, DecisionApplicationError> {
    serde_json::to_vec(value).map_err(|_error| DecisionApplicationError::InvalidPersistentState)
}
