use std::collections::BTreeMap;

use market_squawk_analytics::FeatureRegistry;
use market_squawk_decisions::{
    CandidateAssessment, CandidateFlag, CandidateId, CandidateInput, ScreenExecution,
    ScreenFeatureBinding, ScreenFeatureObservation, ScreenRun,
};
use market_squawk_domain::{DataQuality, EvidenceDigest, InstrumentId, Timestamp};
use market_squawk_portfolio::PortfolioRevisionToken;
use serde::{Deserialize, Serialize};

use super::super::DecisionApplicationError;
use super::common::{FeatureBindingWire, content_digest, statistical};
use super::screen::RunWire;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CandidateFlagWire {
    MissingFeatureIncluded,
    ModelDependent,
    PortfolioImpactBound,
    NonDirectData,
}

impl From<CandidateFlag> for CandidateFlagWire {
    fn from(value: CandidateFlag) -> Self {
        match value {
            CandidateFlag::MissingFeatureIncluded => Self::MissingFeatureIncluded,
            CandidateFlag::ModelDependent => Self::ModelDependent,
            CandidateFlag::PortfolioImpactBound => Self::PortfolioImpactBound,
            CandidateFlag::NonDirectData => Self::NonDirectData,
        }
    }
}

impl From<CandidateFlagWire> for CandidateFlag {
    fn from(value: CandidateFlagWire) -> Self {
        match value {
            CandidateFlagWire::MissingFeatureIncluded => Self::MissingFeatureIncluded,
            CandidateFlagWire::ModelDependent => Self::ModelDependent,
            CandidateFlagWire::PortfolioImpactBound => Self::PortfolioImpactBound,
            CandidateFlagWire::NonDirectData => Self::NonDirectData,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ContributionWire {
    binding: FeatureBindingWire,
    observed_bits: Option<u64>,
    contribution_bits: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CandidateWire {
    id: String,
    instrument_id: InstrumentId,
    rank: u32,
    score_bits: u64,
    selected_at: Timestamp,
    contributions: Vec<ContributionWire>,
    coverage_bits: u64,
    liquidity_bits: u64,
    data_quality: DataQuality,
    portfolio_revision: Option<[u8; 32]>,
    flags: Vec<CandidateFlagWire>,
    evidence_identity: EvidenceDigest,
}

impl From<&CandidateAssessment> for CandidateWire {
    fn from(value: &CandidateAssessment) -> Self {
        Self {
            id: value.record().id().as_str().to_owned(),
            instrument_id: value.record().instrument_id(),
            rank: value.record().rank().get(),
            score_bits: value.record().score().get().to_bits(),
            selected_at: value.record().selected_at(),
            contributions: value
                .score_contributions()
                .iter()
                .map(|contribution| ContributionWire {
                    binding: contribution.binding().into(),
                    observed_bits: contribution.observed().map(|value| value.get().to_bits()),
                    contribution_bits: contribution.contribution().get().to_bits(),
                })
                .collect(),
            coverage_bits: value.coverage().get().to_bits(),
            liquidity_bits: value.liquidity().get().to_bits(),
            data_quality: value.data_quality(),
            portfolio_revision: value.portfolio_impact().map(PortfolioRevisionToken::bytes),
            flags: value.flags().iter().copied().map(Into::into).collect(),
            evidence_identity: value.evidence_identity().evidence_digest(),
        }
    }
}

impl CandidateWire {
    fn decode(
        &self,
        bindings: &[ScreenFeatureBinding],
        registry: &FeatureRegistry,
    ) -> Result<CandidateInput, DecisionApplicationError> {
        let mut observed = BTreeMap::new();
        for contribution in &self.contributions {
            let binding = contribution.binding.decode(registry)?;
            if observed
                .insert(
                    (
                        binding.key().name().to_owned(),
                        binding.key().version().get(),
                    ),
                    contribution.observed_bits,
                )
                .is_some()
            {
                return Err(DecisionApplicationError::InvalidPersistentState);
            }
            statistical(contribution.contribution_bits)?;
        }
        let observations = bindings
            .iter()
            .map(|binding| {
                let bits = observed
                    .get(&(
                        binding.key().name().to_owned(),
                        binding.key().version().get(),
                    ))
                    .ok_or(DecisionApplicationError::InvalidPersistentState)?;
                Ok(ScreenFeatureObservation::new(
                    binding.clone(),
                    bits.map(statistical).transpose()?,
                ))
            })
            .collect::<Result<Vec<_>, DecisionApplicationError>>()?;
        CandidateInput::try_new(
            CandidateId::try_new(&self.id)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            self.instrument_id,
            observations,
            statistical(self.coverage_bits)?,
            statistical(self.liquidity_bits)?,
            self.data_quality,
            self.portfolio_revision
                .map(PortfolioRevisionToken::from_bytes),
            self.flags.iter().copied().map(Into::into).collect(),
            content_digest(self.evidence_identity)?,
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExecutionWire {
    run: RunWire,
    selected_at: Timestamp,
    candidates: Vec<CandidateWire>,
}

impl ExecutionWire {
    pub(super) fn key(&self) -> &str {
        self.run.key()
    }

    pub(super) fn from_execution(
        execution: &ScreenExecution,
        selected_at: Timestamp,
    ) -> Result<Self, DecisionApplicationError> {
        if execution
            .candidates()
            .iter()
            .any(|candidate| candidate.record().selected_at() != selected_at)
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(Self {
            run: execution.run().into(),
            selected_at,
            candidates: execution.candidates().iter().map(Into::into).collect(),
        })
    }

    pub(super) fn decode(
        &self,
        registry: &FeatureRegistry,
    ) -> Result<(ScreenRun, Vec<CandidateInput>, Timestamp), DecisionApplicationError> {
        let run = self.run.decode(registry)?;
        let candidates = self
            .candidates
            .iter()
            .map(|candidate| candidate.decode(run.feature_bindings(), registry))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((run, candidates, self.selected_at))
    }
}
