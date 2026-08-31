use market_squawk_decisions::{
    CandidateAssessment, CandidateId, PreparedPublishedInvestmentAnalysis, SavedScreen, ScreenRun,
    ScreenRunId, SelectedCandidateAnalysisEvidence,
};
use market_squawk_domain::EvidenceDigest;
use serde::{Deserialize, Serialize};

use super::super::DecisionApplicationError;
use super::proposal::InvestmentProposalWire;
use super::recommendation::InvestmentAnalysisPublicationWire;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedPublishedInvestmentAnalysisWire {
    decision: InvestmentProposalWire,
    selected_candidate: SelectedCandidateReferenceWire,
    explanation_digest: [u8; 32],
    publication: InvestmentAnalysisPublicationWire,
}

impl From<&PreparedPublishedInvestmentAnalysis> for PreparedPublishedInvestmentAnalysisWire {
    fn from(value: &PreparedPublishedInvestmentAnalysis) -> Self {
        Self {
            decision: value.decision().into(),
            selected_candidate: value.selected_candidate().into(),
            explanation_digest: value.explanation().explanation_digest().bytes(),
            publication: value.publication().into(),
        }
    }
}

impl PreparedPublishedInvestmentAnalysisWire {
    pub(super) fn key(&self) -> Result<String, DecisionApplicationError> {
        Ok(format!("prepared_{}", self.decision.key()?))
    }

    pub(super) fn candidate_id(&self) -> Result<CandidateId, DecisionApplicationError> {
        CandidateId::try_new(&self.selected_candidate.candidate_id).map_err(invalid_state)
    }

    pub(super) fn screen_run_id(&self) -> Result<ScreenRunId, DecisionApplicationError> {
        ScreenRunId::try_new(&self.selected_candidate.screen_run_id).map_err(invalid_state)
    }

    pub(super) fn decode(
        self,
        screen: &SavedScreen,
        run: &ScreenRun,
        candidate: &CandidateAssessment,
    ) -> Result<PreparedPublishedInvestmentAnalysis, DecisionApplicationError> {
        let selected_candidate = SelectedCandidateAnalysisEvidence::try_new(screen, run, candidate)
            .map_err(invalid_state)?;
        if self.selected_candidate != SelectedCandidateReferenceWire::from(&selected_candidate) {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        let decision = self
            .decision
            .decode_with_selected_candidate(selected_candidate.clone())?;
        let publication = self.publication.decode(&decision)?;
        let value = PreparedPublishedInvestmentAnalysis::try_new(
            decision,
            selected_candidate,
            publication.analytical_profile().clone(),
            publication.workflow().clone(),
            publication.published_at(),
        )
        .map_err(invalid_state)?;
        if value.explanation().explanation_digest().bytes() != self.explanation_digest
            || value.publication() != &publication
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SelectedCandidateReferenceWire {
    candidate_id: String,
    screen_run_id: String,
    evidence_digest: EvidenceDigest,
}

impl From<&SelectedCandidateAnalysisEvidence> for SelectedCandidateReferenceWire {
    fn from(value: &SelectedCandidateAnalysisEvidence) -> Self {
        Self {
            candidate_id: value.candidate_id().as_str().to_owned(),
            screen_run_id: value.screen_run_id().as_str().to_owned(),
            evidence_digest: value.evidence_digest().evidence_digest(),
        }
    }
}

fn invalid_state<T>(_error: T) -> DecisionApplicationError {
    DecisionApplicationError::InvalidPersistentState
}
