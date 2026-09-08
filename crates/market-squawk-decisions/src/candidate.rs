//! Bounded candidate evaluation over closed saved-screen semantics.

use std::num::NonZeroU32;

use market_squawk_analytics::StatisticalF64;
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentId, RevisionNumber, Timestamp,
};
use market_squawk_portfolio::PortfolioRevisionToken;
use sha2::{Digest as _, Sha256};

use crate::{
    AsOfSemantics, CandidateId, CandidateRecord, ComparisonOperator, DecisionContentDigest,
    DecisionContractError, MAX_SCREEN_FEATURE_BINDINGS, NullPolicy, RankingDirection, SavedScreen,
    ScreenFeatureBinding, ScreenRun,
};

/// Maximum source rows admitted to one in-process screen evaluation.
pub const MAX_SCREEN_INPUT_ROWS: usize = 100_000;
/// Maximum closed flags retained by one candidate.
pub const MAX_CANDIDATE_FLAGS: usize = 16;
/// Current schema for selected-screen-candidate analysis evidence.
pub const SELECTED_CANDIDATE_ANALYSIS_EVIDENCE_SCHEMA_VERSION: u16 = 1;

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

/// Closed point-in-time binding from one selected screen candidate into an investment analysis.
///
/// The value owns no ranking or proposal authority. It copies the exact result already produced by
/// the saved-screen evaluator so the later proposal can prove which retained candidate it analyzed.
#[derive(Clone, Debug, PartialEq)]
pub struct SelectedCandidateAnalysisEvidence {
    candidate_id: CandidateId,
    screen_run_id: crate::ScreenRunId,
    screen_id: crate::ScreenId,
    screen_revision: RevisionNumber,
    as_of: Timestamp,
    dataset_identity: DecisionContentDigest,
    universe_identity: DecisionContentDigest,
    instrument_id: InstrumentId,
    rank: NonZeroU32,
    score: StatisticalF64,
    selected_at: Timestamp,
    score_contributions: Box<[CandidateScoreContribution]>,
    coverage: StatisticalF64,
    liquidity: StatisticalF64,
    data_quality: DataQuality,
    portfolio_impact: Option<PortfolioRevisionToken>,
    flags: Box<[CandidateFlag]>,
    candidate_evidence_identity: DecisionContentDigest,
    evidence_digest: DecisionContentDigest,
}

// Every StatisticalF64 admitted by the analytics boundary is finite, so equality is reflexive.
impl Eq for SelectedCandidateAnalysisEvidence {}

impl SelectedCandidateAnalysisEvidence {
    /// Copies one exact retained candidate and its authoritative point-in-time parent run.
    ///
    /// # Errors
    ///
    /// Rejects a detached candidate, an empty or oversized contribution set, allocation failure,
    /// or the cryptographic all-zero sentinel.
    pub fn try_new(
        screen: &SavedScreen,
        run: &ScreenRun,
        candidate: &CandidateAssessment,
    ) -> Result<Self, DecisionContractError> {
        let record = candidate.record();
        if run.screen() != screen.revision()
            || run.universe_identity() != screen.universe_identity()
            || run.feature_bindings() != screen.feature_bindings()
            || record.screen_run_id() != run.id()
            || record.screen() != run.screen()
            || record.selected_at() < run.as_of()
            || candidate.score_contributions().is_empty()
            || candidate.score_contributions().len() > MAX_SCREEN_FEATURE_BINDINGS
            || candidate.flags().len() > MAX_CANDIDATE_FLAGS
            || candidate
                .flags()
                .iter()
                .enumerate()
                .any(|(index, flag)| candidate.flags()[index + 1..].contains(flag))
            || candidate.score_contributions().iter().any(|contribution| {
                !run.feature_bindings()
                    .iter()
                    .any(|binding| binding == contribution.binding())
            })
        {
            return Err(DecisionContractError::InvalidCandidate);
        }
        let mut score_contributions = Vec::new();
        score_contributions
            .try_reserve_exact(candidate.score_contributions().len())
            .map_err(|_error| DecisionContractError::InvalidBound)?;
        score_contributions.extend_from_slice(candidate.score_contributions());
        let mut flags = Vec::new();
        flags
            .try_reserve_exact(candidate.flags().len())
            .map_err(|_error| DecisionContractError::InvalidBound)?;
        flags.extend_from_slice(candidate.flags());
        let mut value = Self {
            candidate_id: record.id().clone(),
            screen_run_id: run.id().clone(),
            screen_id: run.screen().id().clone(),
            screen_revision: run.screen().revision(),
            as_of: run.as_of(),
            dataset_identity: run.dataset_identity(),
            universe_identity: run.universe_identity(),
            instrument_id: record.instrument_id(),
            rank: record.rank(),
            score: record.score(),
            selected_at: record.selected_at(),
            score_contributions: score_contributions.into_boxed_slice(),
            coverage: candidate.coverage(),
            liquidity: candidate.liquidity(),
            data_quality: candidate.data_quality(),
            portfolio_impact: candidate.portfolio_impact().cloned(),
            flags: flags.into_boxed_slice(),
            candidate_evidence_identity: candidate.evidence_identity(),
            evidence_digest: candidate.evidence_identity(),
        };
        value.evidence_digest = DecisionContentDigest::try_new(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            selected_candidate_evidence_digest(&value, screen),
        ))?;
        Ok(value)
    }

    /// Returns the closed selected-candidate evidence schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        SELECTED_CANDIDATE_ANALYSIS_EVIDENCE_SCHEMA_VERSION
    }

    /// Returns the stable selected-candidate identity.
    #[must_use]
    pub const fn candidate_id(&self) -> &CandidateId {
        &self.candidate_id
    }

    /// Returns the exact parent screen-run identity.
    #[must_use]
    pub const fn screen_run_id(&self) -> &crate::ScreenRunId {
        &self.screen_run_id
    }

    /// Returns the stable saved-screen identity.
    #[must_use]
    pub const fn screen_id(&self) -> &crate::ScreenId {
        &self.screen_id
    }

    /// Returns the exact one-based saved-screen revision.
    #[must_use]
    pub const fn screen_revision(&self) -> RevisionNumber {
        self.screen_revision
    }

    /// Returns the screen run's point-in-time cutoff.
    #[must_use]
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns the exact point-in-time dataset identity.
    #[must_use]
    pub const fn dataset_identity(&self) -> DecisionContentDigest {
        self.dataset_identity
    }

    /// Returns the exact historical-universe identity.
    #[must_use]
    pub const fn universe_identity(&self) -> DecisionContentDigest {
        self.universe_identity
    }

    /// Returns the selected instrument.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the one-based candidate rank.
    #[must_use]
    pub const fn rank(&self) -> NonZeroU32 {
        self.rank
    }

    /// Returns the finite screen score; it is not an executable price.
    #[must_use]
    pub const fn score(&self) -> StatisticalF64 {
        self.score
    }

    /// Returns when the retained evaluator selected this candidate.
    #[must_use]
    pub const fn selected_at(&self) -> Timestamp {
        self.selected_at
    }

    /// Returns the complete ordered contribution set retained by the screen evaluator.
    #[must_use]
    pub fn score_contributions(&self) -> &[CandidateScoreContribution] {
        &self.score_contributions
    }

    /// Returns the exact admitted source-coverage fraction retained by the evaluator.
    #[must_use]
    pub const fn coverage(&self) -> StatisticalF64 {
        self.coverage
    }

    /// Returns the exact admitted screen-liquidity statistic.
    #[must_use]
    pub const fn liquidity(&self) -> StatisticalF64 {
        self.liquidity
    }

    /// Returns the source data-quality classification used for screen admission.
    #[must_use]
    pub const fn data_quality(&self) -> DataQuality {
        self.data_quality
    }

    /// Returns the exact portfolio revision used for candidate-impact context, when present.
    #[must_use]
    pub const fn portfolio_impact(&self) -> Option<&PortfolioRevisionToken> {
        self.portfolio_impact.as_ref()
    }

    /// Returns every closed evaluator provenance flag in retained order.
    #[must_use]
    pub fn flags(&self) -> &[CandidateFlag] {
        &self.flags
    }

    /// Returns the candidate input's exact upstream-row commitment.
    #[must_use]
    pub const fn candidate_evidence_identity(&self) -> DecisionContentDigest {
        self.candidate_evidence_identity
    }

    /// Returns the canonical commitment to this complete selected-candidate binding.
    #[must_use]
    pub const fn evidence_digest(&self) -> DecisionContentDigest {
        self.evidence_digest
    }
}

/// Repository append also requires the retained immutable `SavedScreen` and its `ScreenExecution`,
/// while this digest independently binds every selection-policy field rather than trusting only a
/// local screen coordinate.
fn selected_candidate_evidence_digest(
    value: &SelectedCandidateAnalysisEvidence,
    screen: &SavedScreen,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/selected-candidate-analysis-evidence/v1\0");
    hash_text(&mut hash, value.candidate_id.as_str());
    hash_text(&mut hash, value.screen_run_id.as_str());
    hash_text(&mut hash, value.screen_id.as_str());
    hash.update(value.screen_revision.get().to_be_bytes());
    hash_screen_policy(&mut hash, screen);
    hash.update(value.as_of.unix_nanos().to_be_bytes());
    hash_content(&mut hash, value.dataset_identity);
    hash_content(&mut hash, value.universe_identity);
    hash.update(value.instrument_id.as_uuid().as_bytes());
    hash.update(value.rank.get().to_be_bytes());
    hash.update(canonical_statistical_bits(value.score).to_be_bytes());
    hash.update(value.selected_at.unix_nanos().to_be_bytes());
    hash.update(
        u64::try_from(value.score_contributions.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for contribution in &value.score_contributions {
        hash_text(&mut hash, contribution.binding.key().name());
        hash.update(contribution.binding.key().version().get().to_be_bytes());
        hash.update(contribution.binding.semantic_digest().as_bytes());
        match contribution.observed {
            Some(observed) => {
                hash.update([1]);
                hash.update(canonical_statistical_bits(observed).to_be_bytes());
            }
            None => hash.update([0]),
        }
        hash.update(canonical_statistical_bits(contribution.contribution).to_be_bytes());
    }
    hash.update(canonical_statistical_bits(value.coverage).to_be_bytes());
    hash.update(canonical_statistical_bits(value.liquidity).to_be_bytes());
    hash.update([data_quality_tag(value.data_quality)]);
    match &value.portfolio_impact {
        Some(revision) => {
            hash.update([1]);
            hash.update(revision.bytes());
        }
        None => hash.update([0]),
    }
    hash.update(
        u64::try_from(value.flags.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for flag in &value.flags {
        hash.update([candidate_flag_tag(*flag)]);
    }
    hash_content(&mut hash, value.candidate_evidence_identity);
    hash.finalize().into()
}

fn hash_screen_policy(hash: &mut Sha256, screen: &SavedScreen) {
    hash_content(hash, screen.universe_identity());
    hash.update([as_of_semantics_tag(screen.as_of_semantics())]);
    hash.update(
        u64::try_from(screen.predicates().len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for predicate in screen.predicates() {
        hash_feature_binding(hash, predicate.binding());
        hash.update([comparison_operator_tag(predicate.operator())]);
        hash.update(canonical_statistical_bits(predicate.threshold()).to_be_bytes());
        hash.update([null_policy_tag(predicate.null_policy())]);
    }
    hash_feature_binding(hash, screen.ranking().binding());
    hash.update([ranking_direction_tag(screen.ranking().direction())]);
    hash.update(
        u64::try_from(screen.maximum_results().get())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    hash.update(canonical_statistical_bits(screen.constraints().minimum_coverage()).to_be_bytes());
    hash.update(canonical_statistical_bits(screen.constraints().minimum_liquidity()).to_be_bytes());
    hash.update(
        u64::try_from(screen.constraints().admitted_data_qualities().len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for quality in screen.constraints().admitted_data_qualities() {
        hash.update([data_quality_tag(*quality)]);
    }
    hash.update(
        u64::try_from(screen.feature_bindings().len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for binding in screen.feature_bindings() {
        hash_feature_binding(hash, binding);
    }
}

fn hash_feature_binding(hash: &mut Sha256, binding: &ScreenFeatureBinding) {
    hash_text(hash, binding.key().name());
    hash.update(binding.key().version().get().to_be_bytes());
    hash.update(binding.semantic_digest().as_bytes());
}

const fn as_of_semantics_tag(value: AsOfSemantics) -> u8 {
    match value {
        AsOfSemantics::AvailableAtOrBeforeCutoff => 0,
    }
}

const fn comparison_operator_tag(value: ComparisonOperator) -> u8 {
    match value {
        ComparisonOperator::LessThan => 0,
        ComparisonOperator::LessThanOrEqual => 1,
        ComparisonOperator::Equal => 2,
        ComparisonOperator::GreaterThanOrEqual => 3,
        ComparisonOperator::GreaterThan => 4,
    }
}

const fn null_policy_tag(value: NullPolicy) -> u8 {
    match value {
        NullPolicy::Exclude => 0,
        NullPolicy::Include => 1,
    }
}

const fn ranking_direction_tag(value: RankingDirection) -> u8 {
    match value {
        RankingDirection::Ascending => 0,
        RankingDirection::Descending => 1,
    }
}

const fn data_quality_tag(value: DataQuality) -> u8 {
    match value {
        DataQuality::DirectVerified => 0,
        DataQuality::DirectUnverified => 1,
        DataQuality::OfficialDelayed => 2,
        DataQuality::Aggregated => 3,
        DataQuality::Indicative => 4,
        DataQuality::Modeled => 5,
        DataQuality::Estimated => 6,
        DataQuality::Stale => 7,
        DataQuality::Quarantined => 8,
    }
}

const fn candidate_flag_tag(value: CandidateFlag) -> u8 {
    match value {
        CandidateFlag::MissingFeatureIncluded => 0,
        CandidateFlag::ModelDependent => 1,
        CandidateFlag::PortfolioImpactBound => 2,
        CandidateFlag::NonDirectData => 3,
    }
}

fn canonical_statistical_bits(value: StatisticalF64) -> u64 {
    if value.get() == 0.0 {
        0.0_f64.to_bits()
    } else {
        value.get().to_bits()
    }
}

fn hash_text(hash: &mut Sha256, value: &str) {
    hash.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(value.as_bytes());
}

fn hash_content(hash: &mut Sha256, value: DecisionContentDigest) {
    let digest = value.evidence_digest();
    hash.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hash.update(digest.bytes());
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
