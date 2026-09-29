use market_squawk_decisions::{
    CandidateAssessment, CandidateId, DecisionContentDigest, InvestmentAnalysisRequestProvenance,
    PreparedPublishedInvestmentAnalysis, SavedScreen, ScreenRun, ScreenRunId,
    SelectedCandidateAnalysisEvidence,
};
use market_squawk_domain::EvidenceDigest;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::super::DecisionApplicationError;
use super::proposal::{InvestmentProposalWire, RequiredOption};
use super::recommendation::{InvestmentAnalysisPublicationWire, InvestmentSizingInputsWire};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedPublishedInvestmentAnalysisWire {
    decision: InvestmentProposalWire,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    selected_candidate: RequiredOption<SelectedCandidateReferenceWire>,
    explanation_digest: [u8; 32],
    publication: InvestmentAnalysisPublicationWire,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    request_provenance: RequiredOption<InvestmentAnalysisRequestProvenanceWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    sizing_inputs: RequiredOption<InvestmentSizingInputsWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    outcome_projection_digest: RequiredOption<[u8; 32]>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    sizing_projection_digest: RequiredOption<[u8; 32]>,
}

impl TryFrom<&PreparedPublishedInvestmentAnalysis> for PreparedPublishedInvestmentAnalysisWire {
    type Error = DecisionApplicationError;

    fn try_from(value: &PreparedPublishedInvestmentAnalysis) -> Result<Self, Self::Error> {
        Ok(Self {
            decision: InvestmentProposalWire::try_from(value.decision())?,
            selected_candidate: RequiredOption(
                value
                    .selected_candidate()
                    .map(SelectedCandidateReferenceWire::from),
            ),
            explanation_digest: value.explanation().explanation_digest().bytes(),
            publication: value.publication().into(),
            request_provenance: RequiredOption(value.request_provenance().map(Into::into)),
            sizing_inputs: RequiredOption(value.sizing_inputs().map(Into::into)),
            outcome_projection_digest: RequiredOption(
                value
                    .outcome_projection()
                    .map(|value| value.result_digest().bytes()),
            ),
            sizing_projection_digest: RequiredOption(
                value
                    .sizing_projection()
                    .map(|value| value.result_digest().bytes()),
            ),
        })
    }
}

impl PreparedPublishedInvestmentAnalysisWire {
    pub(super) fn key(&self) -> Result<String, DecisionApplicationError> {
        if let Some(provenance) = self.request_provenance.0.as_ref() {
            DecisionContentDigest::try_new(provenance.request_digest).map_err(invalid_state)?;
        }
        let mut identity = Sha256::new();
        identity.update(b"market-squawk/investment-analysis-workflow-key/v1\0");
        identity.update(self.publication.workflow_id().as_str().as_bytes());
        Ok(format!("prepared_workflow_{:x}", identity.finalize()))
    }

    pub(super) fn candidate_reference(
        &self,
    ) -> Result<Option<(CandidateId, ScreenRunId)>, DecisionApplicationError> {
        self.selected_candidate
            .0
            .as_ref()
            .map(|value| {
                Ok((
                    CandidateId::try_new(&value.candidate_id).map_err(invalid_state)?,
                    ScreenRunId::try_new(&value.screen_run_id).map_err(invalid_state)?,
                ))
            })
            .transpose()
    }

    pub(super) fn analysis_id(&self) -> Result<market_squawk_decisions::InvestmentAnalysisId, DecisionApplicationError> {
        self.decision.analysis_id()
    }

    pub(super) fn has_current_share_projection(&self) -> bool {
        self.decision.has_current_share_projection()
    }

    pub(super) async fn decode_with_replay(
        self,
        candidate: Option<(&SavedScreen, &ScreenRun, &CandidateAssessment)>,
        replay: &super::super::current_share::CurrentShareReplayCapability,
        context: &market_squawk_services::RequestContext,
    ) -> Result<PreparedPublishedInvestmentAnalysis, DecisionApplicationError> {
        let selected = candidate.map(|(screen, run, candidate)| {
            SelectedCandidateAnalysisEvidence::try_new(screen, run, candidate).map_err(invalid_state)
        }).transpose()?;
        let request = self.request_provenance.0.as_ref()
            .ok_or(DecisionApplicationError::InvalidPersistentState)?;
        let decision = self.decision.clone().decode_with_replay(selected, &request.canonical_request, replay, context).await?;
        self.decode_recovered(candidate, Some(decision))
    }

    pub(super) fn decode(
        self,
        candidate: Option<(&SavedScreen, &ScreenRun, &CandidateAssessment)>,
    ) -> Result<PreparedPublishedInvestmentAnalysis, DecisionApplicationError> {
        self.decode_recovered(candidate, None)
    }

    fn decode_recovered(
        self,
        candidate: Option<(&SavedScreen, &ScreenRun, &CandidateAssessment)>,
        recovered: Option<market_squawk_decisions::InvestmentProposalDecision>,
    ) -> Result<PreparedPublishedInvestmentAnalysis, DecisionApplicationError> {
        let selected_candidate = candidate
            .map(|(screen, run, candidate)| {
                SelectedCandidateAnalysisEvidence::try_new(screen, run, candidate)
                    .map_err(invalid_state)
            })
            .transpose()?;
        if self.selected_candidate.0
            != selected_candidate
                .as_ref()
                .map(SelectedCandidateReferenceWire::from)
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        let decision = match recovered {
            Some(decision) => {
                if InvestmentProposalWire::try_from(&decision)? != self.decision {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                decision
            }
            None => match selected_candidate.as_ref() {
                Some(value) => self.decision.decode_with_selected_candidate(value.clone())?,
                None => self.decision.decode()?,
            },
        };
        let publication = self.publication.decode(&decision)?;
        let value = match self.request_provenance.0 {
            Some(provenance) => PreparedPublishedInvestmentAnalysis::try_from_generated_request(
                decision,
                selected_candidate,
                publication.analytical_profile().clone(),
                publication.workflow().clone(),
                provenance.decode()?,
                publication.published_at(),
            ),
            None => PreparedPublishedInvestmentAnalysis::try_new(
                decision,
                selected_candidate,
                publication.analytical_profile().clone(),
                publication.workflow().clone(),
                publication.published_at(),
            ),
        }
        .map_err(invalid_state)?;
        let value = match self.sizing_inputs.0 {
            Some(inputs) => value
                .try_with_sizing_inputs(inputs.decode()?)
                .map_err(invalid_state)?,
            None => value,
        };
        if value
            .outcome_projection()
            .map(|value| value.result_digest().bytes())
            != self.outcome_projection_digest.0
            || value
                .sizing_projection()
                .map(|value| value.result_digest().bytes())
                != self.sizing_projection_digest.0
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        if let Some(provenance) = value.request_provenance() {
            super::super::investment_request::validate_request_publication(
                provenance.canonical_request(),
                &value,
            )
            .map_err(invalid_state)?;
        }
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
struct InvestmentAnalysisRequestProvenanceWire {
    workspace_id: [u8; 16],
    canonical_request: Box<[u8]>,
    request_digest: EvidenceDigest,
}

impl From<&InvestmentAnalysisRequestProvenance> for InvestmentAnalysisRequestProvenanceWire {
    fn from(value: &InvestmentAnalysisRequestProvenance) -> Self {
        Self {
            workspace_id: value.workspace_id(),
            canonical_request: value.canonical_request().into(),
            request_digest: value.request_digest().evidence_digest(),
        }
    }
}

impl InvestmentAnalysisRequestProvenanceWire {
    fn decode(self) -> Result<InvestmentAnalysisRequestProvenance, DecisionApplicationError> {
        let value =
            InvestmentAnalysisRequestProvenance::try_new(self.workspace_id, self.canonical_request)
                .map_err(invalid_state)?;
        if value.request_digest().evidence_digest() != self.request_digest {
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
