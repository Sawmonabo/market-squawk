use std::num::NonZeroUsize;

use market_squawk_analytics::FeatureRegistry;
use market_squawk_decisions::{
    AsOfSemantics, ComparisonOperator, NullPolicy, RankingDirection, SavedScreen,
    ScreenConstraints, ScreenId, ScreenPredicate, ScreenRanking, ScreenRevision, ScreenRun,
    ScreenRunId,
};
use market_squawk_domain::{DataQuality, EvidenceDigest, Timestamp};
use serde::{Deserialize, Serialize};

use super::super::DecisionApplicationError;
use super::common::{FeatureBindingWire, content_digest, revision, statistical};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ComparisonWire {
    LessThan,
    LessThanOrEqual,
    Equal,
    GreaterThanOrEqual,
    GreaterThan,
}

impl From<ComparisonOperator> for ComparisonWire {
    fn from(value: ComparisonOperator) -> Self {
        match value {
            ComparisonOperator::LessThan => Self::LessThan,
            ComparisonOperator::LessThanOrEqual => Self::LessThanOrEqual,
            ComparisonOperator::Equal => Self::Equal,
            ComparisonOperator::GreaterThanOrEqual => Self::GreaterThanOrEqual,
            ComparisonOperator::GreaterThan => Self::GreaterThan,
        }
    }
}

impl From<ComparisonWire> for ComparisonOperator {
    fn from(value: ComparisonWire) -> Self {
        match value {
            ComparisonWire::LessThan => Self::LessThan,
            ComparisonWire::LessThanOrEqual => Self::LessThanOrEqual,
            ComparisonWire::Equal => Self::Equal,
            ComparisonWire::GreaterThanOrEqual => Self::GreaterThanOrEqual,
            ComparisonWire::GreaterThan => Self::GreaterThan,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum NullWire {
    Exclude,
    Include,
}

impl From<NullPolicy> for NullWire {
    fn from(value: NullPolicy) -> Self {
        match value {
            NullPolicy::Exclude => Self::Exclude,
            NullPolicy::Include => Self::Include,
        }
    }
}

impl From<NullWire> for NullPolicy {
    fn from(value: NullWire) -> Self {
        match value {
            NullWire::Exclude => Self::Exclude,
            NullWire::Include => Self::Include,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RankingWire {
    Ascending,
    Descending,
}

impl From<RankingDirection> for RankingWire {
    fn from(value: RankingDirection) -> Self {
        match value {
            RankingDirection::Ascending => Self::Ascending,
            RankingDirection::Descending => Self::Descending,
        }
    }
}

impl From<RankingWire> for RankingDirection {
    fn from(value: RankingWire) -> Self {
        match value {
            RankingWire::Ascending => Self::Ascending,
            RankingWire::Descending => Self::Descending,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PredicateWire {
    binding: FeatureBindingWire,
    operator: ComparisonWire,
    threshold_bits: u64,
    null_policy: NullWire,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScreenWire {
    id: String,
    revision: u32,
    universe_identity: EvidenceDigest,
    predicates: Vec<PredicateWire>,
    ranking_binding: FeatureBindingWire,
    ranking_direction: RankingWire,
    maximum_results: usize,
    minimum_coverage_bits: u64,
    minimum_liquidity_bits: u64,
    admitted_data_qualities: Vec<DataQuality>,
}

impl From<&SavedScreen> for ScreenWire {
    fn from(value: &SavedScreen) -> Self {
        Self {
            id: value.revision().id().as_str().to_owned(),
            revision: value.revision().revision().get(),
            universe_identity: value.universe_identity().evidence_digest(),
            predicates: value
                .predicates()
                .iter()
                .map(|predicate| PredicateWire {
                    binding: predicate.binding().into(),
                    operator: predicate.operator().into(),
                    threshold_bits: predicate.threshold().get().to_bits(),
                    null_policy: predicate.null_policy().into(),
                })
                .collect(),
            ranking_binding: value.ranking().binding().into(),
            ranking_direction: value.ranking().direction().into(),
            maximum_results: value.maximum_results().get(),
            minimum_coverage_bits: value.constraints().minimum_coverage().get().to_bits(),
            minimum_liquidity_bits: value.constraints().minimum_liquidity().get().to_bits(),
            admitted_data_qualities: value.constraints().admitted_data_qualities().to_vec(),
        }
    }
}

impl ScreenWire {
    pub(super) fn key(&self) -> String {
        format!("{}:{}", self.id, self.revision)
    }

    pub(super) fn decode(
        self,
        registry: &FeatureRegistry,
    ) -> Result<SavedScreen, DecisionApplicationError> {
        let screen_id = ScreenId::try_new(&self.id)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        let revision = revision(self.revision)?;
        let predicates = self
            .predicates
            .into_iter()
            .map(|predicate| {
                Ok(ScreenPredicate::new(
                    predicate.binding.decode(registry)?,
                    predicate.operator.into(),
                    statistical(predicate.threshold_bits)?,
                    predicate.null_policy.into(),
                ))
            })
            .collect::<Result<Vec<_>, DecisionApplicationError>>()?;
        let ranking = ScreenRanking::new(
            self.ranking_binding.decode(registry)?,
            self.ranking_direction.into(),
        );
        let constraints = ScreenConstraints::try_new(
            statistical(self.minimum_coverage_bits)?,
            statistical(self.minimum_liquidity_bits)?,
            self.admitted_data_qualities,
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        SavedScreen::try_new(
            ScreenRevision::new(screen_id, revision),
            content_digest(self.universe_identity)?,
            AsOfSemantics::AvailableAtOrBeforeCutoff,
            predicates,
            ranking,
            NonZeroUsize::new(self.maximum_results)
                .ok_or(DecisionApplicationError::InvalidPersistentState)?,
            constraints,
            registry,
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::application::decision) struct RunWire {
    id: String,
    screen_id: String,
    screen_revision: u32,
    as_of: Timestamp,
    dataset_identity: EvidenceDigest,
    universe_identity: EvidenceDigest,
    feature_bindings: Vec<FeatureBindingWire>,
}

impl From<&ScreenRun> for RunWire {
    fn from(value: &ScreenRun) -> Self {
        Self {
            id: value.id().as_str().to_owned(),
            screen_id: value.screen().id().as_str().to_owned(),
            screen_revision: value.screen().revision().get(),
            as_of: value.as_of(),
            dataset_identity: value.dataset_identity().evidence_digest(),
            universe_identity: value.universe_identity().evidence_digest(),
            feature_bindings: value.feature_bindings().iter().map(Into::into).collect(),
        }
    }
}

impl RunWire {
    pub(in crate::application::decision) fn key(&self) -> &str {
        &self.id
    }

    pub(in crate::application::decision) fn decode(
        &self,
        registry: &FeatureRegistry,
    ) -> Result<ScreenRun, DecisionApplicationError> {
        let bindings = self
            .feature_bindings
            .iter()
            .map(|binding| binding.decode(registry))
            .collect::<Result<Vec<_>, _>>()?;
        ScreenRun::try_new(
            ScreenRunId::try_new(&self.id)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            ScreenRevision::new(
                ScreenId::try_new(&self.screen_id)
                    .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
                revision(self.screen_revision)?,
            ),
            self.as_of,
            content_digest(self.dataset_identity)?,
            content_digest(self.universe_identity)?,
            bindings,
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)
    }
}
