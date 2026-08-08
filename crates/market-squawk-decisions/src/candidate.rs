//! Bounded candidate evaluation over closed saved-screen semantics.

use std::num::NonZeroU32;

use market_squawk_analytics::StatisticalF64;
use market_squawk_domain::{DataQuality, InstrumentId, Timestamp};
use market_squawk_portfolio::PortfolioRevisionToken;

use crate::{
    CandidateId, CandidateRecord, DecisionContentDigest, DecisionContractError,
    MAX_SCREEN_FEATURE_BINDINGS, NullPolicy, RankingDirection, SavedScreen, ScreenFeatureBinding,
    ScreenRun,
};

/// Maximum source rows admitted to one in-process screen evaluation.
pub const MAX_SCREEN_INPUT_ROWS: usize = 100_000;
/// Maximum closed flags retained by one candidate.
pub const MAX_CANDIDATE_FLAGS: usize = 16;

/// Closed warning and provenance flags emitted by decision evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateFlag {
    /// At least one screen predicate admitted an unavailable value under explicit policy.
    MissingFeatureIncluded,
    /// The candidate depends on model or forecast evidence.
    ModelDependent,
    /// Portfolio impact was evaluated against an immutable revision.
    PortfolioImpactBound,
    /// Data quality is admitted but below direct verified delivery.
    NonDirectData,
}

/// One exact feature value returned by an admitted point-in-time dataset read.
#[derive(Clone, Debug, PartialEq)]
pub struct ScreenFeatureObservation {
    binding: ScreenFeatureBinding,
    value: Option<StatisticalF64>,
}

impl ScreenFeatureObservation {
    /// Constructs one typed observation; unavailable values remain explicit.
    #[must_use]
    pub const fn new(binding: ScreenFeatureBinding, value: Option<StatisticalF64>) -> Self {
        Self { binding, value }
    }

    /// Exact feature semantic.
    #[must_use]
    pub const fn binding(&self) -> &ScreenFeatureBinding {
        &self.binding
    }

    /// Finite value or explicit unavailability.
    #[must_use]
    pub const fn value(&self) -> Option<StatisticalF64> {
        self.value
    }
}

/// Admitted point-in-time row used by the closed evaluator.
#[derive(Clone, Debug, PartialEq)]
pub struct CandidateInput {
    id: CandidateId,
    instrument_id: InstrumentId,
    observations: Box<[ScreenFeatureObservation]>,
    coverage: StatisticalF64,
    liquidity: StatisticalF64,
    data_quality: DataQuality,
    portfolio_impact: Option<PortfolioRevisionToken>,
    flags: Box<[CandidateFlag]>,
    evidence_identity: DecisionContentDigest,
}

impl CandidateInput {
    /// Constructs one bounded input row with exact upstream evidence references.
    #[allow(
        clippy::too_many_arguments,
        reason = "identity, features, constraints, portfolio, flags, and evidence remain independently admitted"
    )]
    pub fn try_new(
        id: CandidateId,
        instrument_id: InstrumentId,
        mut observations: Vec<ScreenFeatureObservation>,
        coverage: StatisticalF64,
        liquidity: StatisticalF64,
        data_quality: DataQuality,
        portfolio_impact: Option<PortfolioRevisionToken>,
        mut flags: Vec<CandidateFlag>,
        evidence_identity: DecisionContentDigest,
    ) -> Result<Self, DecisionContractError> {
        if portfolio_impact.is_some() && !flags.contains(&CandidateFlag::PortfolioImpactBound) {
            flags
                .try_reserve(1)
                .map_err(|_error| DecisionContractError::InvalidBound)?;
            flags.push(CandidateFlag::PortfolioImpactBound);
        }
        if data_quality != DataQuality::DirectVerified
            && !flags.contains(&CandidateFlag::NonDirectData)
        {
            flags
                .try_reserve(1)
                .map_err(|_error| DecisionContractError::InvalidBound)?;
            flags.push(CandidateFlag::NonDirectData);
        }
        if observations.is_empty()
            || observations.len() > MAX_SCREEN_FEATURE_BINDINGS
            || !(0.0..=1.0).contains(&coverage.get())
            || liquidity.get() < 0.0
            || flags.len() > MAX_CANDIDATE_FLAGS
            || flags
                .iter()
                .enumerate()
                .any(|(index, flag)| flags[index + 1..].contains(flag))
        {
            return Err(DecisionContractError::InvalidCandidate);
        }
        observations.sort_unstable_by(|left, right| left.binding.key().cmp(right.binding.key()));
        if observations
            .windows(2)
            .any(|pair| pair[0].binding.key() == pair[1].binding.key())
        {
            return Err(DecisionContractError::InvalidCandidate);
        }
        Ok(Self {
            id,
            instrument_id,
            observations: observations.into_boxed_slice(),
            coverage,
            liquidity,
            data_quality,
            portfolio_impact,
            flags: flags.into_boxed_slice(),
            evidence_identity,
        })
    }

    /// Stable candidate identity allocated by the decision workflow authority.
    #[must_use]
    pub const fn id(&self) -> &CandidateId {
        &self.id
    }

    /// Instrument represented by this exact point-in-time input row.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Complete sorted feature-semantic closure consumed by the saved screen.
    #[must_use]
    pub fn observations(&self) -> &[ScreenFeatureObservation] {
        &self.observations
    }

    /// Fraction of required feature values present in this input row.
    #[must_use]
    pub const fn coverage(&self) -> StatisticalF64 {
        self.coverage
    }

    /// Finite upstream liquidity statistic in the saved screen's declared unit.
    #[must_use]
    pub const fn liquidity(&self) -> StatisticalF64 {
        self.liquidity
    }

    /// Evidentiary quality assigned by the source-owning application workflow.
    #[must_use]
    pub const fn data_quality(&self) -> DataQuality {
        self.data_quality
    }

    /// Exact portfolio revision used for candidate-impact evidence, when available.
    #[must_use]
    pub const fn portfolio_impact(&self) -> Option<&PortfolioRevisionToken> {
        self.portfolio_impact.as_ref()
    }

    /// Closed provenance flags derived by the application workflow.
    #[must_use]
    pub fn flags(&self) -> &[CandidateFlag] {
        &self.flags
    }

    /// Commitment to the exact upstream rows used to construct this input.
    #[must_use]
    pub const fn evidence_identity(&self) -> DecisionContentDigest {
        self.evidence_identity
    }

    fn observation(&self, binding: &ScreenFeatureBinding) -> Option<Option<StatisticalF64>> {
        self.observations
            .binary_search_by(|candidate| candidate.binding.key().cmp(binding.key()))
            .ok()
            .and_then(|index| self.observations.get(index))
            .filter(|candidate| candidate.binding == *binding)
            .map(|candidate| candidate.value)
    }
}

/// One transparent contribution from a closed screen predicate.
#[derive(Clone, Debug, PartialEq)]
pub struct CandidateScoreContribution {
    binding: ScreenFeatureBinding,
    observed: Option<StatisticalF64>,
    contribution: StatisticalF64,
}

impl CandidateScoreContribution {
    /// Exact feature semantic contributing to screening evidence.
    #[must_use]
    pub const fn binding(&self) -> &ScreenFeatureBinding {
        &self.binding
    }

    /// Observed value, if available.
    #[must_use]
    pub const fn observed(&self) -> Option<StatisticalF64> {
        self.observed
    }

    /// Code-owned contribution; absent included values contribute exact zero.
    #[must_use]
    pub const fn contribution(&self) -> StatisticalF64 {
        self.contribution
    }
}

/// Complete immutable candidate record and its constraint/evidence context.
#[derive(Clone, Debug, PartialEq)]
pub struct CandidateAssessment {
    record: CandidateRecord,
    score_contributions: Box<[CandidateScoreContribution]>,
    coverage: StatisticalF64,
    liquidity: StatisticalF64,
    data_quality: DataQuality,
    portfolio_impact: Option<PortfolioRevisionToken>,
    flags: Box<[CandidateFlag]>,
    evidence_identity: DecisionContentDigest,
}

impl CandidateAssessment {
    /// Ranked candidate core.
    #[must_use]
    pub const fn record(&self) -> &CandidateRecord {
        &self.record
    }

    /// Predicate contributions in saved-screen order.
    #[must_use]
    pub fn score_contributions(&self) -> &[CandidateScoreContribution] {
        &self.score_contributions
    }

    /// Source coverage fraction.
    #[must_use]
    pub const fn coverage(&self) -> StatisticalF64 {
        self.coverage
    }

    /// Admitted liquidity statistic.
    #[must_use]
    pub const fn liquidity(&self) -> StatisticalF64 {
        self.liquidity
    }

    /// Source data quality.
    #[must_use]
    pub const fn data_quality(&self) -> DataQuality {
        self.data_quality
    }

    /// Immutable portfolio-impact precondition, when evaluated.
    #[must_use]
    pub const fn portfolio_impact(&self) -> Option<&PortfolioRevisionToken> {
        self.portfolio_impact.as_ref()
    }

    /// Closed candidate flags.
    #[must_use]
    pub fn flags(&self) -> &[CandidateFlag] {
        &self.flags
    }

    /// Commitment to exact upstream row and constraint evidence.
    #[must_use]
    pub const fn evidence_identity(&self) -> DecisionContentDigest {
        self.evidence_identity
    }
}

/// One immutable screen run and its bounded ranked result set.
#[derive(Clone, Debug, PartialEq)]
pub struct ScreenExecution {
    run: ScreenRun,
    candidates: Box<[CandidateAssessment]>,
}

impl ScreenExecution {
    /// Exact point-in-time run identity.
    #[must_use]
    pub const fn run(&self) -> &ScreenRun {
        &self.run
    }

    /// Ranked bounded candidate set.
    #[must_use]
    pub fn candidates(&self) -> &[CandidateAssessment] {
        &self.candidates
    }
}

pub(crate) fn execute(
    screen: &SavedScreen,
    run: ScreenRun,
    mut inputs: Vec<CandidateInput>,
    selected_at: Timestamp,
) -> Result<ScreenExecution, DecisionContractError> {
    inputs.sort_unstable_by(|left, right| left.id.cmp(&right.id));
    if run.screen() != screen.revision()
        || run.universe_identity() != screen.universe_identity()
        || run.feature_bindings() != screen.feature_bindings()
        || selected_at < run.as_of()
        || inputs.len() > MAX_SCREEN_INPUT_ROWS
        || inputs.windows(2).any(|pair| pair[0].id == pair[1].id)
    {
        return Err(DecisionContractError::InvalidCandidate);
    }

    struct Matched {
        input: CandidateInput,
        score: StatisticalF64,
        contributions: Vec<CandidateScoreContribution>,
        included_null: bool,
    }

    let mut matched = Vec::new();
    matched
        .try_reserve_exact(inputs.len())
        .map_err(|_error| DecisionContractError::InvalidBound)?;
    for input in inputs {
        if input.observations.len() != screen.feature_bindings().len()
            || !input
                .observations
                .iter()
                .zip(screen.feature_bindings())
                .all(|(observation, binding)| observation.binding == *binding)
            || input.coverage.get() < screen.constraints().minimum_coverage().get()
            || input.liquidity.get() < screen.constraints().minimum_liquidity().get()
            || !screen
                .constraints()
                .admitted_data_qualities()
                .contains(&input.data_quality)
        {
            continue;
        }
        let mut contributions = Vec::new();
        let contribution_capacity = screen
            .predicates()
            .len()
            .checked_add(1)
            .ok_or(DecisionContractError::InvalidBound)?;
        contributions
            .try_reserve_exact(contribution_capacity)
            .map_err(|_error| DecisionContractError::InvalidBound)?;
        let mut included_null = false;
        let mut passed = true;
        for predicate in screen.predicates() {
            let observed = input
                .observation(predicate.binding())
                .ok_or(DecisionContractError::InvalidCandidate)?;
            let contribution = match observed {
                Some(value) if predicate.operator().evaluate(value, predicate.threshold()) => value,
                Some(_) => {
                    passed = false;
                    break;
                }
                None if matches!(predicate.null_policy(), NullPolicy::Include) => {
                    included_null = true;
                    StatisticalF64::try_new(0.0)
                        .map_err(|_error| DecisionContractError::InvalidCandidate)?
                }
                None => {
                    passed = false;
                    break;
                }
            };
            contributions.push(CandidateScoreContribution {
                binding: predicate.binding().clone(),
                observed,
                contribution,
            });
        }
        if !passed {
            continue;
        }
        let Some(score) = input
            .observation(screen.ranking().binding())
            .ok_or(DecisionContractError::InvalidCandidate)?
        else {
            continue;
        };
        if !contributions
            .iter()
            .any(|contribution| contribution.binding == *screen.ranking().binding())
        {
            contributions.push(CandidateScoreContribution {
                binding: screen.ranking().binding().clone(),
                observed: Some(score),
                contribution: score,
            });
        }
        matched.push(Matched {
            input,
            score,
            contributions,
            included_null,
        });
    }

    matched.sort_unstable_by(|left, right| {
        let score_order = left.score.get().total_cmp(&right.score.get());
        let score_order = match screen.ranking().direction() {
            RankingDirection::Ascending => score_order,
            RankingDirection::Descending => score_order.reverse(),
        };
        score_order.then_with(|| left.input.instrument_id.cmp(&right.input.instrument_id))
    });
    matched.truncate(screen.maximum_results().get());
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(matched.len())
        .map_err(|_error| DecisionContractError::InvalidBound)?;
    for (index, mut selected) in matched.into_iter().enumerate() {
        let rank = u32::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
            .and_then(NonZeroU32::new)
            .ok_or(DecisionContractError::InvalidBound)?;
        if selected.included_null
            && !selected
                .input
                .flags_contains(CandidateFlag::MissingFeatureIncluded)
        {
            let mut flags = selected.input.flags.into_vec();
            if flags.len() >= MAX_CANDIDATE_FLAGS {
                return Err(DecisionContractError::InvalidCandidate);
            }
            flags
                .try_reserve(1)
                .map_err(|_error| DecisionContractError::InvalidBound)?;
            flags.push(CandidateFlag::MissingFeatureIncluded);
            selected.input.flags = flags.into_boxed_slice();
        }
        let record = CandidateRecord::try_new(
            selected.input.id,
            &run,
            selected.input.instrument_id,
            rank,
            selected.score,
            selected_at,
        )?;
        candidates.push(CandidateAssessment {
            record,
            score_contributions: selected.contributions.into_boxed_slice(),
            coverage: selected.input.coverage,
            liquidity: selected.input.liquidity,
            data_quality: selected.input.data_quality,
            portfolio_impact: selected.input.portfolio_impact,
            flags: selected.input.flags,
            evidence_identity: selected.input.evidence_identity,
        });
    }
    Ok(ScreenExecution {
        run,
        candidates: candidates.into_boxed_slice(),
    })
}

impl CandidateInput {
    fn flags_contains(&self, flag: CandidateFlag) -> bool {
        self.flags.contains(&flag)
    }
}
