//! Closed transport adapter over the sole durable investment-decision authority.

mod target_preparation;

use std::{
    num::{NonZeroU32, NonZeroUsize},
    str::FromStr as _,
    sync::Arc,
};

use market_squawk_analytics::{FeatureCompatibility, FeatureKey, StatisticalF64};
use market_squawk_decisions::{
    AppendOutcome, AsOfSemantics, CandidateAssessment, CandidateFlag, CandidateId, CandidateInput,
    ComparisonOperator, DecisionActorId, DecisionContentDigest, DecisionDossier,
    DecisionRepositoryError, DecisionText, DossierId, DossierSection, GovernedTargetSet,
    InvestmentTargetSet, InvestmentTargetSetId, NullPolicy, RankingDirection, SavedScreen,
    ScreenConstraints, ScreenFeatureBinding, ScreenFeatureObservation, ScreenId, ScreenPredicate,
    ScreenRanking, ScreenRevision, ScreenRun, ScreenRunId, TargetMethod, TargetReview,
    TargetReviewDisposition, TargetReviewId, TargetState, TargetStatus,
};
use market_squawk_domain::{
    Currency, DataQuality, EvidenceDigest, InstrumentId, Money, RevisionNumber, Timestamp,
};
use market_squawk_modeling::ProductionFeatureRegistry;
use market_squawk_portfolio::PortfolioRevisionToken;
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::application::decision::{DecisionApplication, DecisionApplicationError};
use crate::portfolio_application::PortfolioFairValueReadCapability;

use self::target_preparation::TargetPreparationOperations;

const SAVE_SCREEN: &str = "Decision.SaveScreen";
const RUN_SCREEN: &str = "Decision.RunScreen";
const LIST_SCREENS: &str = "Decision.ListScreens";
const GET_CANDIDATES: &str = "Decision.GetCandidates";
const LIST_SCREEN_RUNS: &str = "Decision.ListScreenRuns";
const GET_DOSSIER: &str = "Decision.GetDossier";
const LIST_CANDIDATE_DOSSIERS: &str = "Decision.ListCandidateDossiers";
const GET_TARGET: &str = "Decision.GetTargetSet";
const LIST_TARGETS: &str = "Decision.ListTargetSets";
const LIST_TARGET_INDEX: &str = "Decision.ListTargetIndex";
const REVIEW_TARGET: &str = "Decision.ReviewTargetSet";
const TARGET_STATUS: &str = "Decision.GetTargetSetStatus";

/// Closed decision operation surface shared by installed native and MCP transports.
pub(super) struct InstalledDecisionOperations {
    decisions: Arc<DecisionApplication>,
    features: ProductionFeatureRegistry,
    target_preparation: TargetPreparationOperations,
}

impl InstalledDecisionOperations {
    pub(super) fn try_new(
        decisions: Arc<DecisionApplication>,
        portfolio: PortfolioFairValueReadCapability,
        runtime: market_squawk_runtime::RuntimeIdentity,
    ) -> Result<Self, ServiceError> {
        let features =
            ProductionFeatureRegistry::try_new().map_err(|_error| ServiceError::Unavailable)?;
        Ok(Self {
            target_preparation: TargetPreparationOperations::new(
                Arc::clone(&decisions),
                portfolio,
                runtime,
            ),
            decisions,
            features,
        })
    }

    pub(super) fn owns(operation: &str) -> bool {
        TargetPreparationOperations::owns(operation)
            || matches!(
                operation,
                SAVE_SCREEN
                    | RUN_SCREEN
                    | LIST_SCREENS
                    | GET_CANDIDATES
                    | LIST_SCREEN_RUNS
                    | GET_DOSSIER
                    | LIST_CANDIDATE_DOSSIERS
                    | GET_TARGET
                    | LIST_TARGETS
                    | LIST_TARGET_INDEX
                    | REVIEW_TARGET
                    | TARGET_STATUS
            )
    }

    pub(super) fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        if TargetPreparationOperations::owns(request.name()) {
            return self.target_preparation.call(request, context);
        }
        let arguments = mutation_arguments(request.arguments());
        let (content, item_count) = match request.name() {
            SAVE_SCREEN => {
                let input: SaveScreenRequest = decode(&arguments)?;
                let screen = input.screen.decode(self.features.feature_registry())?;
                let outcome = self
                    .decisions
                    .save_screen(input.expected_revision.map(revision).transpose()?, screen)
                    .map_err(map_application)?;
                (append_outcome_value(outcome), 1)
            }
            RUN_SCREEN => {
                let input: RunScreenRequest = decode(&arguments)?;
                let run = input.run.decode(self.features.feature_registry())?;
                let candidates = input
                    .candidates
                    .into_iter()
                    .map(|candidate| candidate.decode(self.features.feature_registry()))
                    .collect::<Result<Vec<_>, _>>()?;
                let execution = self
                    .decisions
                    .run_screen(run, candidates, input.selected_at)
                    .map_err(map_application)?;
                let count = execution.candidates().len().max(1);
                (execution_value(&execution), count)
            }
            LIST_SCREENS => {
                let input: ListRequest = decode(&arguments)?;
                let screens = self
                    .decisions
                    .list_screens(input.limit)
                    .map_err(map_application)?;
                let count = screens.len().max(1);
                (
                    json!({"screens": screens.iter().map(screen_value).collect::<Vec<_>>() }),
                    count,
                )
            }
            GET_CANDIDATES => {
                let input: RunIdentityRequest = decode(&arguments)?;
                let candidates = self
                    .decisions
                    .get_candidates(&ScreenRunId::try_new(input.run_id).map_err(invalid)?)
                    .map_err(map_application)?;
                let count = candidates.len().max(1);
                (
                    json!({"candidates": candidates.iter().map(candidate_value).collect::<Vec<_>>() }),
                    count,
                )
            }
            LIST_SCREEN_RUNS => {
                let input: ScreenRunListRequest = decode(&arguments)?;
                let after = input
                    .after_run_id
                    .map(ScreenRunId::try_new)
                    .transpose()
                    .map_err(invalid)?;
                let mut runs = self
                    .decisions
                    .list_screen_runs_after(after.as_ref(), page_fetch_limit(input.limit)?)
                    .map_err(map_application)?;
                let next_after = trim_next(&mut runs, input.limit, |entry| {
                    entry.run().id().as_str().to_owned()
                });
                let count = runs.len().max(1);
                (
                    json!({
                        "runs": runs.iter().map(screen_run_index_value).collect::<Vec<_>>(),
                        "nextAfter": next_after,
                    }),
                    count,
                )
            }
            GET_DOSSIER => {
                let input: DossierIdentityRequest = decode(&arguments)?;
                let dossier = self
                    .decisions
                    .get_dossier(&DossierId::try_new(input.dossier_id).map_err(invalid)?)
                    .map_err(map_application)?;
                (dossier_value(&dossier), 1)
            }
            LIST_CANDIDATE_DOSSIERS => {
                let input: CandidateDossierListRequest = decode(&arguments)?;
                let candidate_id = CandidateId::try_new(input.candidate_id).map_err(invalid)?;
                let after = input
                    .after_dossier_id
                    .map(DossierId::try_new)
                    .transpose()
                    .map_err(invalid)?;
                let mut dossiers = self
                    .decisions
                    .list_candidate_dossiers_after(
                        &candidate_id,
                        after.as_ref(),
                        page_fetch_limit(input.limit)?,
                    )
                    .map_err(map_application)?;
                let next_after = trim_next(&mut dossiers, input.limit, |dossier| {
                    dossier.dossier().id().as_str().to_owned()
                });
                let count = dossiers.len().max(1);
                (
                    json!({
                        "dossiers": dossiers.iter().map(dossier_value).collect::<Vec<_>>(),
                        "nextAfter": next_after,
                    }),
                    count,
                )
            }
            GET_TARGET => {
                let input: TargetIdentityRequest = decode(&arguments)?;
                let state = self
                    .decisions
                    .get_target(&target_id(&input.target_id)?, revision(input.revision)?)
                    .map_err(map_application)?;
                (target_state_value(&state), 1)
            }
            LIST_TARGETS => {
                let input: TargetListRequest = decode(&arguments)?;
                let targets = self
                    .decisions
                    .list_targets(&target_id(&input.target_id)?)
                    .map_err(map_application)?;
                let count = targets.len().max(1);
                (
                    json!({"targets": targets.iter().map(target_state_value).collect::<Vec<_>>() }),
                    count,
                )
            }
            LIST_TARGET_INDEX => {
                let input: TargetIndexListRequest = decode(&arguments)?;
                let after = input
                    .after_target_id
                    .map(|value| target_id(&value))
                    .transpose()?;
                let mut targets = self
                    .decisions
                    .list_target_index_after(after.as_ref(), page_fetch_limit(input.limit)?)
                    .map_err(map_application)?;
                let next_after = trim_next(&mut targets, input.limit, |entry| {
                    entry.id().as_str().to_owned()
                });
                let count = targets.len().max(1);
                (
                    json!({
                        "targets": targets.iter().map(target_index_value).collect::<Vec<_>>(),
                        "nextAfter": next_after,
                    }),
                    count,
                )
            }
            REVIEW_TARGET => {
                let input: ReviewTargetRequest = decode(&arguments)?;
                let target_id = target_id(&input.review.target_id)?;
                let target = self
                    .decisions
                    .get_target(&target_id, revision(input.review.target_revision)?)
                    .map_err(map_application)?;
                let review = input.review.decode(target.target().target())?;
                let outcome = self
                    .decisions
                    .review_target(review)
                    .map_err(map_application)?;
                (append_outcome_value(outcome), 1)
            }
            TARGET_STATUS => {
                let input: TargetIdentityRequest = decode(&arguments)?;
                let status = self
                    .decisions
                    .target_status(&target_id(&input.target_id)?, revision(input.revision)?)
                    .map_err(map_application)?;
                (json!({"status": target_status_name(status)}), 1)
            }
            _ => return Err(ServiceError::NotFound),
        };
        ensure_live(context)?;
        TypedToolResult::try_new(
            content,
            item_count,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }
}

impl std::fmt::Debug for InstalledDecisionOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledDecisionOperations")
            .field("decisions", &"[DURABLE DECISION AUTHORITY]")
            .field("features", &"[CODE-OWNED FEATURE REGISTRY]")
            .field("target_preparation", &self.target_preparation)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SaveScreenRequest {
    expected_revision: Option<u32>,
    screen: ScreenInput,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RunScreenRequest {
    run: ScreenRunInput,
    candidates: Vec<CandidateInputDto>,
    selected_at: Timestamp,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListRequest {
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RunIdentityRequest {
    run_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ScreenRunListRequest {
    after_run_id: Option<String>,
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DossierIdentityRequest {
    dossier_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CandidateDossierListRequest {
    candidate_id: String,
    after_dossier_id: Option<String>,
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TargetIdentityRequest {
    target_id: String,
    revision: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TargetListRequest {
    target_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TargetIndexListRequest {
    after_target_id: Option<String>,
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewTargetRequest {
    review: ReviewInput,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct FeatureBindingInput {
    name: String,
    version: u32,
    semantic_digest: [u8; 32],
}

impl FeatureBindingInput {
    fn decode(
        self,
        registry: &market_squawk_analytics::FeatureRegistry,
    ) -> Result<ScreenFeatureBinding, ServiceError> {
        let version = NonZeroU32::new(self.version).ok_or(ServiceError::InvalidRequest)?;
        let key = FeatureKey::try_new(&self.name, version).map_err(invalid)?;
        let metadata = registry
            .try_resolve(&key, FeatureCompatibility::PointInTime)
            .map_err(invalid)?;
        if metadata.semantic_digest().as_bytes() != self.semantic_digest {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(ScreenFeatureBinding::new(key, metadata.semantic_digest()))
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ComparisonInput {
    LessThan,
    LessThanOrEqual,
    Equal,
    GreaterThanOrEqual,
    GreaterThan,
}

impl From<ComparisonInput> for ComparisonOperator {
    fn from(value: ComparisonInput) -> Self {
        match value {
            ComparisonInput::LessThan => Self::LessThan,
            ComparisonInput::LessThanOrEqual => Self::LessThanOrEqual,
            ComparisonInput::Equal => Self::Equal,
            ComparisonInput::GreaterThanOrEqual => Self::GreaterThanOrEqual,
            ComparisonInput::GreaterThan => Self::GreaterThan,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NullInput {
    Exclude,
    Include,
}

impl From<NullInput> for NullPolicy {
    fn from(value: NullInput) -> Self {
        match value {
            NullInput::Exclude => Self::Exclude,
            NullInput::Include => Self::Include,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RankingInput {
    Ascending,
    Descending,
}

impl From<RankingInput> for RankingDirection {
    fn from(value: RankingInput) -> Self {
        match value {
            RankingInput::Ascending => Self::Ascending,
            RankingInput::Descending => Self::Descending,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PredicateInput {
    binding: FeatureBindingInput,
    operator: ComparisonInput,
    threshold: f64,
    null_policy: NullInput,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RankingPolicyInput {
    binding: FeatureBindingInput,
    direction: RankingInput,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ScreenConstraintsInput {
    minimum_coverage: f64,
    minimum_liquidity: f64,
    admitted_data_qualities: Vec<DataQuality>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ScreenInput {
    id: String,
    revision: u32,
    universe_identity: EvidenceDigest,
    predicates: Vec<PredicateInput>,
    ranking: RankingPolicyInput,
    maximum_results: usize,
    constraints: ScreenConstraintsInput,
}

impl ScreenInput {
    fn decode(
        self,
        registry: &market_squawk_analytics::FeatureRegistry,
    ) -> Result<SavedScreen, ServiceError> {
        let predicates = self
            .predicates
            .into_iter()
            .map(|predicate| {
                Ok(ScreenPredicate::new(
                    predicate.binding.decode(registry)?,
                    predicate.operator.into(),
                    statistical(predicate.threshold)?,
                    predicate.null_policy.into(),
                ))
            })
            .collect::<Result<Vec<_>, ServiceError>>()?;
        let ranking = ScreenRanking::new(
            self.ranking.binding.decode(registry)?,
            self.ranking.direction.into(),
        );
        let constraints = ScreenConstraints::try_new(
            statistical(self.constraints.minimum_coverage)?,
            statistical(self.constraints.minimum_liquidity)?,
            self.constraints.admitted_data_qualities,
        )
        .map_err(invalid)?;
        SavedScreen::try_new(
            ScreenRevision::new(
                ScreenId::try_new(self.id).map_err(invalid)?,
                revision(self.revision)?,
            ),
            content_digest(self.universe_identity)?,
            AsOfSemantics::AvailableAtOrBeforeCutoff,
            predicates,
            ranking,
            NonZeroUsize::new(self.maximum_results).ok_or(ServiceError::InvalidRequest)?,
            constraints,
            registry,
        )
        .map_err(invalid)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ScreenRunInput {
    id: String,
    screen_id: String,
    screen_revision: u32,
    as_of: Timestamp,
    dataset_identity: EvidenceDigest,
    universe_identity: EvidenceDigest,
    feature_bindings: Vec<FeatureBindingInput>,
}

impl ScreenRunInput {
    fn decode(
        self,
        registry: &market_squawk_analytics::FeatureRegistry,
    ) -> Result<ScreenRun, ServiceError> {
        ScreenRun::try_new(
            ScreenRunId::try_new(self.id).map_err(invalid)?,
            ScreenRevision::new(
                ScreenId::try_new(self.screen_id).map_err(invalid)?,
                revision(self.screen_revision)?,
            ),
            self.as_of,
            content_digest(self.dataset_identity)?,
            content_digest(self.universe_identity)?,
            self.feature_bindings
                .into_iter()
                .map(|binding| binding.decode(registry))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(invalid)
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CandidateFlagInput {
    MissingFeatureIncluded,
    ModelDependent,
    PortfolioImpactBound,
    NonDirectData,
}

impl From<CandidateFlagInput> for CandidateFlag {
    fn from(value: CandidateFlagInput) -> Self {
        match value {
            CandidateFlagInput::MissingFeatureIncluded => Self::MissingFeatureIncluded,
            CandidateFlagInput::ModelDependent => Self::ModelDependent,
            CandidateFlagInput::PortfolioImpactBound => Self::PortfolioImpactBound,
            CandidateFlagInput::NonDirectData => Self::NonDirectData,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationInput {
    binding: FeatureBindingInput,
    value: Option<f64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CandidateInputDto {
    id: String,
    instrument_id: InstrumentId,
    observations: Vec<ObservationInput>,
    coverage: f64,
    liquidity: f64,
    data_quality: DataQuality,
    portfolio_revision: Option<[u8; 32]>,
    flags: Vec<CandidateFlagInput>,
    evidence_identity: EvidenceDigest,
}

impl CandidateInputDto {
    fn decode(
        self,
        registry: &market_squawk_analytics::FeatureRegistry,
    ) -> Result<CandidateInput, ServiceError> {
        CandidateInput::try_new(
            CandidateId::try_new(self.id).map_err(invalid)?,
            self.instrument_id,
            self.observations
                .into_iter()
                .map(|observation| {
                    Ok(ScreenFeatureObservation::new(
                        observation.binding.decode(registry)?,
                        observation.value.map(statistical).transpose()?,
                    ))
                })
                .collect::<Result<Vec<_>, ServiceError>>()?,
            statistical(self.coverage)?,
            statistical(self.liquidity)?,
            self.data_quality,
            self.portfolio_revision
                .map(PortfolioRevisionToken::from_bytes),
            self.flags.into_iter().map(Into::into).collect(),
            content_digest(self.evidence_identity)?,
        )
        .map_err(invalid)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MoneyInput {
    amount: String,
    currency: String,
}

impl MoneyInput {
    fn decode(self) -> Result<Money, ServiceError> {
        let amount = Decimal::from_str(&self.amount).map_err(invalid)?;
        let currency = Currency::try_from(self.currency.as_str()).map_err(invalid)?;
        Ok(Money::new(amount, currency))
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TargetMethodInput {
    ComparableEvidence,
    DiscountedCashFlow,
    ResidualIncome,
    ForecastDistribution,
    FairValueMeasurement,
}

impl From<TargetMethodInput> for TargetMethod {
    fn from(value: TargetMethodInput) -> Self {
        match value {
            TargetMethodInput::ComparableEvidence => Self::ComparableEvidence,
            TargetMethodInput::DiscountedCashFlow => Self::DiscountedCashFlow,
            TargetMethodInput::ResidualIncome => Self::ResidualIncome,
            TargetMethodInput::ForecastDistribution => Self::ForecastDistribution,
            TargetMethodInput::FairValueMeasurement => Self::FairValueMeasurement,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReviewDispositionInput {
    Activate,
    Reject,
    NeedsChanges,
}

impl From<ReviewDispositionInput> for TargetReviewDisposition {
    fn from(value: ReviewDispositionInput) -> Self {
        match value {
            ReviewDispositionInput::Activate => Self::Activate,
            ReviewDispositionInput::Reject => Self::Reject,
            ReviewDispositionInput::NeedsChanges => Self::NeedsChanges,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ReviewInput {
    id: String,
    target_id: String,
    target_revision: u32,
    reviewer: String,
    reviewed_at: Timestamp,
    disposition: ReviewDispositionInput,
    content_identity: EvidenceDigest,
}

impl ReviewInput {
    fn decode(self, target: &InvestmentTargetSet) -> Result<TargetReview, ServiceError> {
        if target.id().as_str() != self.target_id || target.revision().get() != self.target_revision
        {
            return Err(ServiceError::InvalidRequest);
        }
        TargetReview::try_new(
            TargetReviewId::try_new(self.id).map_err(invalid)?,
            target,
            DecisionActorId::try_new(self.reviewer).map_err(invalid)?,
            self.reviewed_at,
            self.disposition.into(),
            content_digest(self.content_identity)?,
        )
        .map_err(invalid)
    }
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(arguments.clone())).map_err(invalid)
}

fn mutation_arguments(arguments: &Map<String, Value>) -> Map<String, Value> {
    let mut admitted = arguments.clone();
    admitted.remove("confirm");
    admitted
}

fn revision(value: u32) -> Result<RevisionNumber, ServiceError> {
    RevisionNumber::new(value).map_err(invalid)
}

fn target_id(value: &str) -> Result<InvestmentTargetSetId, ServiceError> {
    InvestmentTargetSetId::try_new(value).map_err(invalid)
}

fn content_digest(value: EvidenceDigest) -> Result<DecisionContentDigest, ServiceError> {
    DecisionContentDigest::try_new(value).map_err(invalid)
}

fn statistical(value: f64) -> Result<StatisticalF64, ServiceError> {
    StatisticalF64::try_new(value).map_err(invalid)
}

fn page_fetch_limit(limit: usize) -> Result<usize, ServiceError> {
    if limit == 0 || limit > 1_000 {
        return Err(ServiceError::InvalidRequest);
    }
    limit.checked_add(1).ok_or(ServiceError::InvalidRequest)
}

fn trim_next<T>(
    values: &mut Vec<T>,
    limit: usize,
    identity: impl FnOnce(&T) -> String,
) -> Option<String> {
    if values.len() <= limit {
        return None;
    }
    values.truncate(limit);
    values.last().map(identity)
}

fn invalid<T>(_error: T) -> ServiceError {
    ServiceError::InvalidRequest
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if std::time::Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_application(error: DecisionApplicationError) -> ServiceError {
    match error {
        DecisionApplicationError::Repository(DecisionRepositoryError::NotFound) => {
            ServiceError::NotFound
        }
        DecisionApplicationError::Repository(
            DecisionRepositoryError::InvalidLimits
            | DecisionRepositoryError::Conflict
            | DecisionRepositoryError::StaleRevision
            | DecisionRepositoryError::EvidenceMismatch,
        ) => ServiceError::InvalidRequest,
        DecisionApplicationError::Repository(
            DecisionRepositoryError::Capacity | DecisionRepositoryError::Allocation,
        )
        | DecisionApplicationError::Allocation
        | DecisionApplicationError::Capacity => ServiceError::ResourceExhausted,
        DecisionApplicationError::Unavailable | DecisionApplicationError::Persistence => {
            ServiceError::Unavailable
        }
        DecisionApplicationError::InvalidPersistentState => ServiceError::Internal,
    }
}

fn append_outcome_value(outcome: AppendOutcome) -> Value {
    json!({
        "outcome": match outcome {
            AppendOutcome::Appended => "appended",
            AppendOutcome::AlreadyPresent => "already_present",
        }
    })
}

fn binding_value(binding: &ScreenFeatureBinding) -> Value {
    json!({
        "name": binding.key().name(),
        "version": binding.key().version().get(),
        "semanticDigest": binding.semantic_digest().as_bytes(),
    })
}

fn screen_value(screen: &SavedScreen) -> Value {
    json!({
        "id": screen.revision().id().as_str(),
        "revision": screen.revision().revision().get(),
        "universeIdentity": screen.universe_identity().evidence_digest(),
        "asOfSemantics": "available_at_or_before_cutoff",
        "predicates": screen.predicates().iter().map(|predicate| json!({
            "binding": binding_value(predicate.binding()),
            "operator": comparison_name(predicate.operator()),
            "threshold": predicate.threshold().get(),
            "nullPolicy": null_policy_name(predicate.null_policy()),
        })).collect::<Vec<_>>(),
        "ranking": {
            "binding": binding_value(screen.ranking().binding()),
            "direction": ranking_name(screen.ranking().direction()),
        },
        "maximumResults": screen.maximum_results().get(),
        "constraints": {
            "minimumCoverage": screen.constraints().minimum_coverage().get(),
            "minimumLiquidity": screen.constraints().minimum_liquidity().get(),
            "admittedDataQualities": screen.constraints().admitted_data_qualities(),
        },
    })
}

fn screen_run_index_value(entry: &market_squawk_decisions::ScreenRunIndexEntry) -> Value {
    let run = entry.run();
    json!({
        "id": run.id().as_str(),
        "screenId": run.screen().id().as_str(),
        "screenRevision": run.screen().revision().get(),
        "asOf": run.as_of(),
        "datasetIdentity": run.dataset_identity().evidence_digest(),
        "universeIdentity": run.universe_identity().evidence_digest(),
        "candidateCount": entry.candidate_count(),
    })
}

fn execution_value(execution: &market_squawk_decisions::ScreenExecution) -> Value {
    let run = execution.run();
    json!({
        "run": {
            "id": run.id().as_str(),
            "screenId": run.screen().id().as_str(),
            "screenRevision": run.screen().revision().get(),
            "asOf": run.as_of(),
            "datasetIdentity": run.dataset_identity().evidence_digest(),
            "universeIdentity": run.universe_identity().evidence_digest(),
            "featureBindings": run.feature_bindings().iter().map(binding_value).collect::<Vec<_>>(),
        },
        "candidates": execution.candidates().iter().map(candidate_value).collect::<Vec<_>>(),
    })
}

fn candidate_value(candidate: &CandidateAssessment) -> Value {
    let record = candidate.record();
    json!({
        "id": record.id().as_str(),
        "screenRunId": record.screen_run_id().as_str(),
        "screenId": record.screen().id().as_str(),
        "screenRevision": record.screen().revision().get(),
        "instrumentId": record.instrument_id(),
        "rank": record.rank().get(),
        "score": record.score().get(),
        "selectedAt": record.selected_at(),
        "scoreContributions": candidate.score_contributions().iter().map(|contribution| json!({
            "binding": binding_value(contribution.binding()),
            "observed": contribution.observed().map(StatisticalF64::get),
            "contribution": contribution.contribution().get(),
        })).collect::<Vec<_>>(),
        "coverage": candidate.coverage().get(),
        "liquidity": candidate.liquidity().get(),
        "dataQuality": candidate.data_quality(),
        "portfolioRevision": candidate.portfolio_impact().map(PortfolioRevisionToken::bytes),
        "flags": candidate.flags().iter().copied().map(candidate_flag_name).collect::<Vec<_>>(),
        "evidenceIdentity": candidate.evidence_identity().evidence_digest(),
    })
}

fn dossier_value(dossier: &DecisionDossier) -> Value {
    let core = dossier.dossier();
    json!({
        "id": core.id().as_str(),
        "candidateId": core.candidate_id().as_str(),
        "instrumentId": core.instrument_id(),
        "assembledAt": core.assembled_at(),
        "evidence": {
            "modelBundle": core.evidence().model_bundle().map(|value| value.as_str()),
            "portfolioRevision": core.evidence().portfolio_revision().map(PortfolioRevisionToken::bytes),
            "fairValueDecision": core.evidence().fair_value_decision().map(|value| value.to_string()),
            "contentIdentity": core.evidence().content_identity().evidence_digest(),
        },
        "references": dossier.references().iter().map(|reference| json!({
            "section": dossier_section_name(reference.section()),
            "contentIdentity": reference.content_identity().evidence_digest(),
        })).collect::<Vec<_>>(),
    })
}

fn target_state_value(state: &TargetState) -> Value {
    json!({
        "target": target_value(state.target()),
        "status": target_status_name(state.status()),
        "latestReview": state.latest_review().map(review_value),
        "latestInvalidation": state.latest_invalidation().map(|invalidation| json!({
            "id": invalidation.id().as_str(),
            "targetId": invalidation.target_id().as_str(),
            "targetRevision": invalidation.target_revision().get(),
            "kind": invalidation_kind_name(invalidation.kind()),
            "actor": invalidation.actor().map(DecisionActorId::as_str),
            "observedAt": invalidation.observed_at(),
            "contentIdentity": invalidation.content_identity().evidence_digest(),
        })),
    })
}

fn target_index_value(entry: &market_squawk_decisions::TargetIndexEntry) -> Value {
    json!({
        "id": entry.id().as_str(),
        "revision": entry.revision().get(),
        "instrumentId": entry.instrument_id(),
        "status": target_status_name(entry.status()),
    })
}

fn target_value(value: &GovernedTargetSet) -> Value {
    let target = value.target();
    json!({
        "id": target.id().as_str(),
        "revision": target.revision().get(),
        "dossierId": target.dossier_id().as_str(),
        "instrumentId": target.instrument_id(),
        "referencePrice": money_value(target.reference_mark().price()),
        "referenceObservedAt": target.reference_mark().observed_at(),
        "referenceIdentity": target.reference_mark().content_identity().evidence_digest(),
        "downside": money_value(target.cases().downside()),
        "base": money_value(target.cases().base()),
        "upside": money_value(target.cases().upside()),
        "entryLower": money_value(target.entry_range().lower()),
        "entryUpper": money_value(target.entry_range().upper()),
        "trimLower": money_value(target.trim_range().lower()),
        "trimUpper": money_value(target.trim_range().upper()),
        "exitLower": money_value(target.exit_range().lower()),
        "exitUpper": money_value(target.exit_range().upper()),
        "createdAt": target.created_at(),
        "horizonAt": target.horizon_at(),
        "expiresAt": target.expires_at(),
        "targetIdentity": target.content_identity().evidence_digest(),
        "addCase": money_value(value.add_case()),
        "method": target_method_name(value.method()),
        "assumptions": value.assumptions().iter().map(|assumption| json!({
            "text": assumption.text().as_str(),
            "evidenceIdentity": assumption.evidence_identity().evidence_digest(),
        })).collect::<Vec<_>>(),
        "portfolioRevision": value.decision_context().portfolio_revision().map(PortfolioRevisionToken::bytes),
        "effectiveAt": value.effective_at(),
        "reviewDueAt": value.review_due_at(),
        "supersedes": value.supersedes().map(|(revision, at)| json!({
            "revision": revision.get(), "supersededAt": at,
        })),
        "thesis": value.thesis().as_str(),
        "risks": value.risks().iter().map(DecisionText::as_str).collect::<Vec<_>>(),
        "invalidationConditions": value.invalidation_conditions().iter().map(DecisionText::as_str).collect::<Vec<_>>(),
        "forecast": value.evidence().forecast().map(DecisionContentDigest::evidence_digest),
        "fairValue": value.evidence().fair_value().map(|id| id.to_string()),
        "markQuality": value.mark_quality(),
        "author": value.author().as_str(),
        "rulesetVersion": value.ruleset_version().get(),
    })
}

fn review_value(review: &TargetReview) -> Value {
    json!({
        "id": review.id().as_str(),
        "targetId": review.target_id().as_str(),
        "targetRevision": review.target_revision().get(),
        "reviewer": review.reviewer().as_str(),
        "reviewedAt": review.reviewed_at(),
        "disposition": review_disposition_name(review.disposition()),
        "contentIdentity": review.content_identity().evidence_digest(),
    })
}

fn money_value(money: Money) -> Value {
    json!({"amount": money.amount().to_string(), "currency": money.currency().as_str()})
}

const fn comparison_name(value: ComparisonOperator) -> &'static str {
    match value {
        ComparisonOperator::LessThan => "less_than",
        ComparisonOperator::LessThanOrEqual => "less_than_or_equal",
        ComparisonOperator::Equal => "equal",
        ComparisonOperator::GreaterThanOrEqual => "greater_than_or_equal",
        ComparisonOperator::GreaterThan => "greater_than",
    }
}

const fn null_policy_name(value: NullPolicy) -> &'static str {
    match value {
        NullPolicy::Exclude => "exclude",
        NullPolicy::Include => "include",
    }
}

const fn ranking_name(value: RankingDirection) -> &'static str {
    match value {
        RankingDirection::Ascending => "ascending",
        RankingDirection::Descending => "descending",
    }
}

const fn candidate_flag_name(value: CandidateFlag) -> &'static str {
    match value {
        CandidateFlag::MissingFeatureIncluded => "missing_feature_included",
        CandidateFlag::ModelDependent => "model_dependent",
        CandidateFlag::PortfolioImpactBound => "portfolio_impact_bound",
        CandidateFlag::NonDirectData => "non_direct_data",
    }
}

const fn dossier_section_name(value: DossierSection) -> &'static str {
    match value {
        DossierSection::Data => "data",
        DossierSection::CorporateActions => "corporate_actions",
        DossierSection::Fundamentals => "fundamentals",
        DossierSection::Forecast => "forecast",
        DossierSection::PortfolioImpact => "portfolio_impact",
        DossierSection::FairValue => "fair_value",
        DossierSection::DecisionContext => "decision_context",
    }
}

const fn target_method_name(value: TargetMethod) -> &'static str {
    match value {
        TargetMethod::ComparableEvidence => "comparable_evidence",
        TargetMethod::DiscountedCashFlow => "discounted_cash_flow",
        TargetMethod::ResidualIncome => "residual_income",
        TargetMethod::ForecastDistribution => "forecast_distribution",
        TargetMethod::FairValueMeasurement => "fair_value_measurement",
    }
}

const fn invalidation_kind_name(value: market_squawk_decisions::InvalidationKind) -> &'static str {
    match value {
        market_squawk_decisions::InvalidationKind::CorporateAction => "corporate_action",
        market_squawk_decisions::InvalidationKind::Model => "model",
        market_squawk_decisions::InvalidationKind::Data => "data",
        market_squawk_decisions::InvalidationKind::ReferenceMark => "reference_mark",
        market_squawk_decisions::InvalidationKind::Assumption => "assumption",
    }
}

const fn review_disposition_name(value: TargetReviewDisposition) -> &'static str {
    match value {
        TargetReviewDisposition::Activate => "activate",
        TargetReviewDisposition::Reject => "reject",
        TargetReviewDisposition::NeedsChanges => "needs_changes",
    }
}

const fn target_status_name(value: TargetStatus) -> &'static str {
    match value {
        TargetStatus::PendingReview => "pending_review",
        TargetStatus::Active => "active",
        TargetStatus::Rejected => "rejected",
        TargetStatus::NeedsChanges => "needs_changes",
        TargetStatus::NeedsReview => "needs_review",
        TargetStatus::Superseded => "superseded",
    }
}
