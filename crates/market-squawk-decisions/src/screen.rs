//! Closed, code-owned saved-screen semantics.

use std::num::NonZeroUsize;

use market_squawk_analytics::{
    FeatureCompatibility, FeatureOutputType, FeatureRegistry, StatisticalF64,
};
use market_squawk_domain::DataQuality;

use crate::{
    DecisionContentDigest, DecisionContractError, MAX_SCREEN_FEATURE_BINDINGS,
    ScreenFeatureBinding, ScreenRevision,
};

/// Maximum ranked candidates retained by one execution.
pub const MAX_SCREEN_RESULTS: usize = 1_024;
/// Maximum admitted data-quality classes in one saved screen.
pub const MAX_SCREEN_DATA_QUALITIES: usize = 9;

/// The only point-in-time cutoff semantics accepted by saved screens.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsOfSemantics {
    /// Consume observations whose authoritative availability time is no later than the cutoff.
    AvailableAtOrBeforeCutoff,
}

/// Closed comparison language; no SQL, expression, or executable formula is retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonOperator {
    /// Observed value is strictly less than the threshold.
    LessThan,
    /// Observed value is less than or equal to the threshold.
    LessThanOrEqual,
    /// Observed value is exactly equal to the threshold.
    Equal,
    /// Observed value is greater than or equal to the threshold.
    GreaterThanOrEqual,
    /// Observed value is strictly greater than the threshold.
    GreaterThan,
}

impl ComparisonOperator {
    #[allow(
        clippy::float_cmp,
        reason = "equality is an explicit saved-screen operator over already admitted finite values"
    )]
    pub(crate) fn evaluate(self, observed: StatisticalF64, threshold: StatisticalF64) -> bool {
        match self {
            Self::LessThan => observed.get() < threshold.get(),
            Self::LessThanOrEqual => observed.get() <= threshold.get(),
            Self::Equal => observed.get() == threshold.get(),
            Self::GreaterThanOrEqual => observed.get() >= threshold.get(),
            Self::GreaterThan => observed.get() > threshold.get(),
        }
    }
}

/// Explicit handling for unavailable feature values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NullPolicy {
    /// Exclude the instrument when the selected feature is unavailable.
    Exclude,
    /// Treat the predicate as satisfied while retaining an unavailable-value candidate flag.
    Include,
}

/// One typed predicate over an exact code-owned feature semantic.
#[derive(Clone, Debug, PartialEq)]
pub struct ScreenPredicate {
    binding: ScreenFeatureBinding,
    operator: ComparisonOperator,
    threshold: StatisticalF64,
    null_policy: NullPolicy,
}

impl ScreenPredicate {
    /// Constructs a predicate from closed values only.
    #[must_use]
    pub const fn new(
        binding: ScreenFeatureBinding,
        operator: ComparisonOperator,
        threshold: StatisticalF64,
        null_policy: NullPolicy,
    ) -> Self {
        Self {
            binding,
            operator,
            threshold,
            null_policy,
        }
    }

    /// Exact feature semantic evaluated by this predicate.
    #[must_use]
    pub const fn binding(&self) -> &ScreenFeatureBinding {
        &self.binding
    }

    /// Closed comparison operator.
    #[must_use]
    pub const fn operator(&self) -> ComparisonOperator {
        self.operator
    }

    /// Finite statistical threshold.
    #[must_use]
    pub const fn threshold(&self) -> StatisticalF64 {
        self.threshold
    }

    /// Explicit unavailable-value handling.
    #[must_use]
    pub const fn null_policy(&self) -> NullPolicy {
        self.null_policy
    }
}

/// Deterministic result ordering.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RankingDirection {
    /// Lowest finite value ranks first.
    Ascending,
    /// Highest finite value ranks first.
    Descending,
}

/// Exact code-owned feature used for deterministic ranking.
#[derive(Clone, Debug, PartialEq)]
pub struct ScreenRanking {
    binding: ScreenFeatureBinding,
    direction: RankingDirection,
}

impl ScreenRanking {
    /// Constructs one closed ranking policy.
    #[must_use]
    pub const fn new(binding: ScreenFeatureBinding, direction: RankingDirection) -> Self {
        Self { binding, direction }
    }

    /// Ranking feature semantic.
    #[must_use]
    pub const fn binding(&self) -> &ScreenFeatureBinding {
        &self.binding
    }

    /// Deterministic ordering direction.
    #[must_use]
    pub const fn direction(&self) -> RankingDirection {
        self.direction
    }
}

/// Candidate coverage, liquidity, and source-quality admission policy.
#[derive(Clone, Debug, PartialEq)]
pub struct ScreenConstraints {
    minimum_coverage: StatisticalF64,
    minimum_liquidity: StatisticalF64,
    admitted_data_qualities: Box<[DataQuality]>,
}

impl ScreenConstraints {
    /// Constructs bounded constraints with coverage in `[0, 1]` and nonnegative liquidity.
    pub fn try_new(
        minimum_coverage: StatisticalF64,
        minimum_liquidity: StatisticalF64,
        admitted_data_qualities: Vec<DataQuality>,
    ) -> Result<Self, DecisionContractError> {
        if !(0.0..=1.0).contains(&minimum_coverage.get())
            || minimum_liquidity.get() < 0.0
            || admitted_data_qualities.is_empty()
            || admitted_data_qualities.len() > MAX_SCREEN_DATA_QUALITIES
            || admitted_data_qualities
                .iter()
                .enumerate()
                .any(|(index, quality)| admitted_data_qualities[index + 1..].contains(quality))
        {
            return Err(DecisionContractError::InvalidBound);
        }
        Ok(Self {
            minimum_coverage,
            minimum_liquidity,
            admitted_data_qualities: admitted_data_qualities.into_boxed_slice(),
        })
    }

    /// Minimum fraction of required data present.
    #[must_use]
    pub const fn minimum_coverage(&self) -> StatisticalF64 {
        self.minimum_coverage
    }

    /// Minimum finite liquidity statistic in the upstream declared unit.
    #[must_use]
    pub const fn minimum_liquidity(&self) -> StatisticalF64 {
        self.minimum_liquidity
    }

    /// Exact source-quality allowlist.
    #[must_use]
    pub fn admitted_data_qualities(&self) -> &[DataQuality] {
        &self.admitted_data_qualities
    }
}

/// Immutable saved-screen revision admitted against the code-owned feature registry.
#[derive(Clone, Debug, PartialEq)]
pub struct SavedScreen {
    revision: ScreenRevision,
    universe_identity: DecisionContentDigest,
    as_of_semantics: AsOfSemantics,
    predicates: Box<[ScreenPredicate]>,
    ranking: ScreenRanking,
    maximum_results: NonZeroUsize,
    constraints: ScreenConstraints,
    feature_bindings: Box<[ScreenFeatureBinding]>,
}

impl SavedScreen {
    /// Constructs a screen with only exact point-in-time features from the local code registry.
    #[allow(
        clippy::too_many_arguments,
        reason = "revision, universe, cutoff, predicates, ranking, result, and constraint authority remain explicit"
    )]
    pub fn try_new(
        revision: ScreenRevision,
        universe_identity: DecisionContentDigest,
        as_of_semantics: AsOfSemantics,
        predicates: Vec<ScreenPredicate>,
        ranking: ScreenRanking,
        maximum_results: NonZeroUsize,
        constraints: ScreenConstraints,
        registry: &FeatureRegistry,
    ) -> Result<Self, DecisionContractError> {
        if predicates.is_empty()
            || predicates.len() > MAX_SCREEN_FEATURE_BINDINGS
            || maximum_results.get() > MAX_SCREEN_RESULTS
        {
            return Err(DecisionContractError::InvalidScreen);
        }
        let binding_capacity = predicates
            .len()
            .checked_add(1)
            .ok_or(DecisionContractError::InvalidBound)?;
        let mut feature_bindings = Vec::new();
        feature_bindings
            .try_reserve_exact(binding_capacity)
            .map_err(|_error| DecisionContractError::InvalidBound)?;
        feature_bindings.extend(predicates.iter().map(|predicate| predicate.binding.clone()));
        feature_bindings.push(ranking.binding.clone());
        feature_bindings.sort_unstable_by(|left, right| left.key().cmp(right.key()));
        feature_bindings.dedup_by(|left, right| left.key() == right.key());
        if feature_bindings.len() > MAX_SCREEN_FEATURE_BINDINGS {
            return Err(DecisionContractError::InvalidScreen);
        }
        for binding in &feature_bindings {
            let metadata = registry
                .try_resolve(binding.key(), FeatureCompatibility::PointInTime)
                .map_err(|_error| DecisionContractError::UnknownScreenFeature)?;
            if metadata.semantic_digest() != binding.semantic_digest()
                || metadata.output_type() != FeatureOutputType::StatisticalF64
            {
                return Err(DecisionContractError::UnknownScreenFeature);
            }
        }
        Ok(Self {
            revision,
            universe_identity,
            as_of_semantics,
            predicates: predicates.into_boxed_slice(),
            ranking,
            maximum_results,
            constraints,
            feature_bindings: feature_bindings.into_boxed_slice(),
        })
    }

    /// Stable saved-screen revision.
    #[must_use]
    pub const fn revision(&self) -> &ScreenRevision {
        &self.revision
    }

    /// Exact authoritative historical-universe identity.
    #[must_use]
    pub const fn universe_identity(&self) -> DecisionContentDigest {
        self.universe_identity
    }

    /// Point-in-time cutoff policy.
    #[must_use]
    pub const fn as_of_semantics(&self) -> AsOfSemantics {
        self.as_of_semantics
    }

    /// Closed ordered predicate set.
    #[must_use]
    pub fn predicates(&self) -> &[ScreenPredicate] {
        &self.predicates
    }

    /// Ranking policy.
    #[must_use]
    pub const fn ranking(&self) -> &ScreenRanking {
        &self.ranking
    }

    /// Maximum retained result count.
    #[must_use]
    pub const fn maximum_results(&self) -> NonZeroUsize {
        self.maximum_results
    }

    /// Candidate constraints.
    #[must_use]
    pub const fn constraints(&self) -> &ScreenConstraints {
        &self.constraints
    }

    /// Sorted unique feature-semantic closure required by every run.
    #[must_use]
    pub fn feature_bindings(&self) -> &[ScreenFeatureBinding] {
        &self.feature_bindings
    }
}
