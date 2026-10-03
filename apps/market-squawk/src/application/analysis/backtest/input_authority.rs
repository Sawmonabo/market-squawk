//! Restart-safe registration and fresh resolution of governed-backtest inputs.

mod index;
mod issuer;
mod plan;
mod preparation;
mod probability;
mod recipe;
mod resolution;
mod study;

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use market_squawk_backtesting::{
    AllOriginRoundTripEvaluationV1, AllOriginRoundTripEvaluatorV1, AllOriginRoundTripPolicyV1,
    BacktestDataset, BacktestExecutionBasis, MaterializedRecommendationSignalPlanV1,
    RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1, RecommendationAggregateEvidenceV1,
    RecommendationBacktestKernelV1, RecommendationBacktestLimits, RecommendationBacktestPolicyV1,
    RecommendationBacktestPublicationV1, RecommendationBacktestStudyV1,
    RecommendationSignalInformationSetV1, RecommendationSignalIssuanceV1,
    RecommendationSignalIssuerIdentityV1, RecommendationSignalPlanCompletenessV1,
    RecommendationSignalPlanMaterializationErrorV1, RecommendationSignalPlanMaterializerV1,
};
use market_squawk_data::{CorporateActionPlan, DatasetManifestRef, Sha256Digest};
use market_squawk_domain::{InstrumentId, SourceIdentifier, Timestamp};
use market_squawk_platform::{
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths, PathError,
};
use market_squawk_services::ServiceError;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    GovernedBacktestCommand, GovernedBacktestInputResolver, ResolvedGovernedBacktestInput,
    repository::lifecycle::{
        LinkedOperation, RepositoryLifecycle, await_blocking, ensure_operation_live,
    },
};
use crate::ResearchService;

pub(crate) use issuer::{
    HistoricalRecommendationAlphaProducer, HistoricalRecommendationAlphaProducerReadCapability,
    HistoricalRecommendationAlphaProducerReference,
};
pub(crate) use plan::{
    HistoricalFoldTrainingAuthorityV1, HistoricalStudyDatasetPartV1,
    HistoricalStudyPlanReadCapabilityV1, HistoricalStudyPlanReferenceV1, HistoricalStudyPlanV1,
};
pub(crate) use study::{PreparedRecommendationStudyV1, RecommendationStudyPreparationInputV1};

use index::{
    InputIndex, InputIndexError, InputIndexLimits, InputInsertDisposition, StoredInputRecipe,
};
use recipe::{InputRecipe, RecipeError, RegistrationRecipe};
use resolution::{BacktestInputMaterializer, MaterializedInput, RecommendationInputFacts};

pub use preparation::{
    BacktestPreparationCatalog, BacktestPreparationDatasetInput, BacktestPreparationError,
    BacktestPreparationLimits, BacktestPreparationOptions, BacktestPreparationPreview,
    BacktestPreparationReceipt, BacktestPreparationSelection, GovernedBacktestPreparationAuthority,
};
pub use recipe::{
    GovernedBacktestCohortCandidateRegistrationInput,
    GovernedBacktestCohortMemberRegistrationInput, GovernedBacktestCohortRegistrationInput,
    GovernedBacktestCorporateActionsInput, GovernedBacktestInputRegistrationInput,
    GovernedBacktestInputRegistrationJsonError, GovernedBacktestPortfolioSeedInput,
    GovernedBacktestQueryLimitsInput, MAX_GOVERNED_BACKTEST_REGISTRATION_REQUEST_BYTES,
};

const INPUT_INDEX_DIRECTORY: &str = "analysis/governed-backtest-inputs";
const HARD_MAXIMUM_INPUTS: usize = 16_384;
const HARD_MAXIMUM_MANIFEST_NODES: usize = 4_096;
const STANDARD_MAXIMUM_INPUTS: usize = 4_096;
const STANDARD_MAXIMUM_INDEX_BYTES: usize = 7 * 1024 * 1024;
const STANDARD_MAXIMUM_MANIFEST_NODES: usize = 1_024;

/// Explicit durable-input and recursive manifest-graph ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GovernedBacktestInputAuthorityLimits {
    maximum_inputs: usize,
    maximum_index_bytes: usize,
    maximum_manifest_nodes: usize,
}

impl GovernedBacktestInputAuthorityLimits {
    pub(crate) const fn maximum_backup_index_bytes(self) -> usize {
        self.maximum_index_bytes
    }

    /// Constructs limits within the process and crash-safe persistence ceilings.
    pub fn try_new(
        maximum_inputs: usize,
        maximum_index_bytes: usize,
        maximum_manifest_nodes: usize,
    ) -> Result<Self, ProductionGovernedBacktestInputAuthorityError> {
        if maximum_inputs == 0
            || maximum_inputs > HARD_MAXIMUM_INPUTS
            || maximum_index_bytes == 0
            || maximum_index_bytes > LocalAuthorityStateStore::maximum_payload_bytes()
            || maximum_manifest_nodes == 0
            || maximum_manifest_nodes > HARD_MAXIMUM_MANIFEST_NODES
        {
            return Err(ProductionGovernedBacktestInputAuthorityError::InvalidLimits);
        }
        Ok(Self {
            maximum_inputs,
            maximum_index_bytes,
            maximum_manifest_nodes,
        })
    }

    /// Production defaults bounded below the authority-store payload ceiling.
    #[must_use]
    pub const fn standard() -> Self {
        Self {
            maximum_inputs: STANDARD_MAXIMUM_INPUTS,
            maximum_index_bytes: STANDARD_MAXIMUM_INDEX_BYTES,
            maximum_manifest_nodes: STANDARD_MAXIMUM_MANIFEST_NODES,
        }
    }

    const fn index(self) -> InputIndexLimits {
        InputIndexLimits {
            maximum_inputs: self.maximum_inputs,
            maximum_index_bytes: self.maximum_index_bytes,
        }
    }
}

/// Durable registration receipt carrying the exact command accepted by resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GovernedBacktestInputRegistrationReceipt {
    command: GovernedBacktestCommand,
}

/// Least-authority registration capability consumed by the Analysis application service.
#[async_trait]
pub trait GovernedBacktestInputRegistrar: Send + Sync + 'static {
    /// Materializes and durably registers one complete immutable input recipe.
    async fn register_input(
        &self,
        input: GovernedBacktestInputRegistrationInput,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedBacktestInputRegistrationReceipt, ServiceError>;
}

impl GovernedBacktestInputRegistrationReceipt {
    /// Returns the complete immutable command binding.
    #[must_use]
    pub const fn command(&self) -> &GovernedBacktestCommand {
        &self.command
    }

    /// Returns the content-derived immutable input identity.
    #[must_use]
    pub const fn input_id(&self) -> &SourceIdentifier {
        self.command.input_id()
    }

    /// Consumes the receipt and returns the complete command.
    #[must_use]
    pub fn into_command(self) -> GovernedBacktestCommand {
        self.command
    }
}

/// Exact identities returned only after the existing registrar has reopened and admitted all
/// registered sealed raw histories. These facts supply policy inputs, not a financial recommendation,
/// source-completeness approval, or signal-issuer capability.
#[derive(Debug)]
pub(crate) struct GovernedRecommendationDailyInputRegistrationReceiptV1 {
    registration: GovernedBacktestInputRegistrationReceipt,
    facts: RecommendationInputFacts,
}

impl GovernedRecommendationDailyInputRegistrationReceiptV1 {
    pub(crate) const fn study_qualification(
        &self,
    ) -> market_squawk_backtesting::BacktestStudyQualification {
        self.facts.study_qualification
    }

    pub(crate) fn command(&self) -> &GovernedBacktestCommand {
        self.registration.command()
    }

    pub(crate) const fn dataset_identity(&self) -> Sha256Digest {
        self.facts.dataset_identity
    }

    pub(crate) const fn raw_price_evidence_digest(&self) -> Sha256Digest {
        self.facts.raw_price_evidence_digest
    }

    pub(crate) const fn corporate_action_content_digest(&self) -> Sha256Digest {
        self.facts.corporate_action_content_digest
    }

    pub(crate) const fn corporate_action_audit_digest(&self) -> Sha256Digest {
        self.facts.corporate_action_audit_digest
    }

    pub(crate) const fn corporate_action_valuation_cutoff(&self) -> Timestamp {
        self.facts.corporate_action_valuation_cutoff
    }

    pub(crate) const fn admitted_at(&self) -> Timestamp {
        self.facts.admitted_at
    }

    pub(crate) fn into_command(self) -> GovernedBacktestCommand {
        self.registration.into_command()
    }
}

/// Confined immutable recommendation materialization over one freshly resolved governed input.
///
/// This bundle exposes the pinned dataset and exact materialization evidence required by the pure
/// recommendation kernel, but no query engine, registration mutation, repository, job, path,
/// portfolio, order, risk, or execution authority.
#[allow(
    dead_code,
    reason = "a generic analysis consumer uses this at the next composition seam"
)]
#[derive(Debug)]
pub(crate) struct GovernedRecommendationMaterializedInputV1 {
    dataset: BacktestDataset,
    corporate_actions: CorporateActionPlan,
    signal_plan: MaterializedRecommendationSignalPlanV1,
}

#[allow(
    dead_code,
    reason = "a generic analysis consumer uses this at the next composition seam"
)]
impl GovernedRecommendationMaterializedInputV1 {
    /// Exact freshly pinned research dataset.
    #[must_use]
    pub(crate) const fn dataset(&self) -> &BacktestDataset {
        &self.dataset
    }

    /// Dataset- and policy-bound strict signal-plan materialization.
    #[must_use]
    pub(crate) const fn signal_plan(&self) -> &MaterializedRecommendationSignalPlanV1 {
        &self.signal_plan
    }

    /// Exact immutable dataset manifest.
    #[must_use]
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        self.dataset.manifest()
    }

    /// Complete pinned dataset identity.
    #[must_use]
    pub(crate) const fn dataset_identity(&self) -> Sha256Digest {
        self.dataset.identity()
    }

    /// Complete materialization receipt identity.
    #[must_use]
    pub(crate) const fn materialization_digest(&self) -> Sha256Digest {
        self.signal_plan.digest()
    }

    /// Consumes the confined input into issuer- and materialization-bound product evidence.
    pub(crate) fn evaluate(
        self,
        policy: RecommendationBacktestPolicyV1,
        publication: RecommendationBacktestPublicationV1,
        cancellation: &CancellationToken,
    ) -> Result<GovernedRecommendationBacktestEvidenceV1, ServiceError> {
        let study = RecommendationBacktestKernelV1::run_materialized_study(
            &self.dataset,
            policy,
            &self.corporate_actions,
            &self.signal_plan,
            publication,
            self.signal_plan.limits(),
            cancellation,
        )
        .map_err(|_| {
            if cancellation.is_cancelled() {
                ServiceError::Cancelled
            } else {
                ServiceError::InvalidResult
            }
        })?;
        GovernedRecommendationBacktestEvidenceV1::try_new(study, &self.signal_plan)
    }
}

impl ProductionGovernedBacktestInputAuthority {
    pub(in crate::application::analysis::backtest) async fn restore_recommendation_input(
        &self,
        command: &GovernedBacktestCommand,
        policy: RecommendationBacktestPolicyV1,
        limits: RecommendationBacktestLimits,
        signal_plan_bytes: &[u8],
        maximum_bytes: usize,
        repository_permit: Arc<tokio::sync::OwnedSemaphorePermit>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedRecommendationMaterializedInputV1, ServiceError> {
        let materialized = self
            .resolve_materialized(
                command,
                Some(Arc::clone(&repository_permit)),
                cancellation.clone(),
                deadline,
            )
            .await?;
        let (dataset, corporate_actions, execution_assumptions) =
            materialized.into_recommendation_dataset()?;
        if execution_assumptions != policy.execution_assumptions()
            || dataset.execution_basis() != policy.execution_basis()
        {
            return Err(ServiceError::InvalidResult);
        }
        let signal_plan = MaterializedRecommendationSignalPlanV1::restore_persisted(
            signal_plan_bytes,
            maximum_bytes,
            &dataset,
            policy,
            limits,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        ensure_operation_live(&cancellation, &self.lifecycle, deadline)?;
        Ok(GovernedRecommendationMaterializedInputV1 {
            dataset,
            corporate_actions,
            signal_plan,
        })
    }
}

/// App-owned recommendation evidence admitted for proposal adaptation.
///
/// Generic backtesting APIs can produce only [`RecommendationBacktestStudyV1`]. This wrapper is
/// created only after the nonconstructible installed issuer has produced a complete sequential
/// materialization over one freshly pinned governed input. It grants no risk, order, dispatch, or
/// execution authority.
#[allow(
    dead_code,
    reason = "the installed recommendation recipe is composed at the next serialized seam"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GovernedRecommendationBacktestEvidenceV1 {
    study: Arc<RecommendationBacktestStudyV1>,
    materialized_signal_plan_digest: Sha256Digest,
    issuer_identity: RecommendationSignalIssuerIdentityV1,
    digest: Sha256Digest,
}

#[allow(
    dead_code,
    reason = "the installed recommendation recipe is composed at the next serialized seam"
)]
impl GovernedRecommendationBacktestEvidenceV1 {
    pub(crate) fn basis(&self) -> market_squawk_domain::HistoricalStudyBasis {
        self.study.basis()
    }
    pub(crate) fn limitations(&self) -> &[market_squawk_domain::HistoricalStudyLimitation] {
        self.study.limitations()
    }
    pub(crate) fn snapshot_as_of(&self) -> Timestamp {
        self.study.snapshot_as_of()
    }
    pub(crate) fn source_snapshot_digest(&self) -> Sha256Digest {
        self.study.source_snapshot_digest()
    }

    pub(in crate::application::analysis::backtest) fn with_publication(
        mut self,
        publication: RecommendationBacktestPublicationV1,
    ) -> Result<Self, ServiceError> {
        self.study = Arc::new(
            Arc::try_unwrap(self.study)
                .unwrap_or_else(|study| (*study).clone())
                .with_publication(publication)
                .map_err(|_| ServiceError::InvalidResult)?,
        );
        self.digest = governed_recommendation_evidence_digest(
            self.study.digest(),
            self.materialized_signal_plan_digest,
            self.issuer_identity.digest(),
        );
        Ok(self)
    }

    fn try_new(
        study: RecommendationBacktestStudyV1,
        materialized: &MaterializedRecommendationSignalPlanV1,
    ) -> Result<Self, ServiceError> {
        if study.dataset_identity() != materialized.dataset_identity()
            || study.dataset_manifest_content() != materialized.dataset_manifest_content()
            || study.object_graph_digest() != materialized.object_graph_digest()
            || study.point_in_time_content() != materialized.point_in_time_content()
            || study.point_in_time_audit() != materialized.point_in_time_audit()
            || study.policy_digest() != materialized.policy_digest()
            || study.signal_plan_digest() != materialized.signal_plan().digest()
            || study.preauthorized_signal_plan_digest()
                != materialized
                    .signal_plan()
                    .preauthorized_signal_plan_digest()
            || study.completeness() != RecommendationSignalPlanCompletenessV1::Complete
            || study.publication().simulation_cutoff() != materialized.evaluation_ends_at()
            || study.limits() != materialized.limits()
        {
            return Err(ServiceError::InvalidResult);
        }
        let issuer_identity = materialized.issuer_identity().clone();
        let materialized_signal_plan_digest = materialized.digest();
        let digest = governed_recommendation_evidence_digest(
            study.digest(),
            materialized_signal_plan_digest,
            issuer_identity.digest(),
        );
        Ok(Self {
            study: Arc::new(study),
            materialized_signal_plan_digest,
            issuer_identity,
            digest,
        })
    }

    /// Complete research study admitted through the installed issuer path.
    #[must_use]
    pub(crate) fn study(&self) -> &RecommendationBacktestStudyV1 {
        &self.study
    }

    /// Exact PIT dataset identity.
    #[must_use]
    pub(crate) fn dataset_identity(&self) -> Sha256Digest {
        self.study.dataset_identity()
    }

    /// Complete strict recommendation policy.
    #[must_use]
    pub(crate) fn policy(&self) -> RecommendationBacktestPolicyV1 {
        self.study.policy()
    }

    /// Exact canonical signal-plan identity.
    #[must_use]
    pub(crate) fn signal_plan_digest(&self) -> Sha256Digest {
        self.study.signal_plan_digest()
    }

    /// Exact content-derived sequential issuer-plan identity.
    #[must_use]
    pub(crate) fn preauthorized_signal_plan_digest(&self) -> Sha256Digest {
        self.study.preauthorized_signal_plan_digest()
    }

    /// Exact evaluation and publication timing.
    #[must_use]
    pub(crate) fn publication(&self) -> RecommendationBacktestPublicationV1 {
        self.study.publication()
    }

    /// Exact aggregates or a typed incomplete-evidence refusal.
    #[must_use]
    pub(crate) fn aggregate(&self) -> RecommendationAggregateEvidenceV1 {
        self.study.aggregate()
    }

    /// Exact sequential materialization identity.
    #[must_use]
    pub(crate) fn materialized_signal_plan_digest(&self) -> Sha256Digest {
        self.materialized_signal_plan_digest
    }

    /// Exact semantic identity of the installed code-owned issuer.
    #[must_use]
    pub(crate) fn issuer_identity(&self) -> &RecommendationSignalIssuerIdentityV1 {
        &self.issuer_identity
    }

    /// Complete app-owned governed-evidence identity.
    #[must_use]
    pub(crate) fn digest(&self) -> Sha256Digest {
        self.digest
    }
}

fn governed_recommendation_evidence_digest(
    study_digest: Sha256Digest,
    materialized_signal_plan_digest: Sha256Digest,
    issuer_identity_digest: Sha256Digest,
) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/governed-recommendation-backtest-evidence/v1\0");
    hash.update(study_digest.bytes());
    hash.update(materialized_signal_plan_digest.bytes());
    hash.update(issuer_identity_digest.bytes());
    Sha256Digest::new(hash.finalize().into())
}

/// Request-specific installed capability over the concrete source-qualified financial producer.
/// There is no public callback constructor or caller-declared producer identity.
#[derive(Clone, Debug)]
pub(crate) struct GovernedRecommendationSignalIssuerV1 {
    producer: Arc<HistoricalRecommendationAlphaProducer>,
    request_context: market_squawk_services::RequestContext,
}

impl GovernedRecommendationSignalIssuerV1 {
    pub(crate) fn from_producer(
        producer: HistoricalRecommendationAlphaProducer,
        context: &market_squawk_services::RequestContext,
    ) -> Result<Self, ServiceError> {
        context.origin().ok_or(ServiceError::Unauthorized)?;
        Ok(Self {
            producer: Arc::new(producer),
            request_context: context.clone(),
        })
    }

    async fn issue(
        &self,
        view: &RecommendationSignalInformationSetV1<'_>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<RecommendationSignalIssuanceV1, ServiceError> {
        let context = market_squawk_services::RequestContext::new(
            self.request_context.request_id().clone(),
            cancellation,
            deadline,
            self.request_context.limits(),
        )
        .with_origin(
            self.request_context
                .origin()
                .ok_or(ServiceError::Unauthorized)?,
        );
        self.producer.issue(view, &context).await
    }

    pub(crate) fn identity(&self) -> RecommendationSignalIssuerIdentityV1 {
        self.producer.identity().clone()
    }

    pub(crate) fn reference(&self) -> &HistoricalRecommendationAlphaProducerReference {
        self.producer.reference()
    }
}

/// Least-authority reader for one exact already-registered recommendation backtest input.
///
/// Implementations re-pin every catalog/query/instrument-definition receipt under the supplied
/// lifecycle authority before invoking the installed code-owned signal issuer against that exact
/// immutable dataset. They do not register inputs, publish terminals, choose economic
/// instructions, or run jobs.
#[async_trait]
#[allow(
    dead_code,
    reason = "a generic analysis consumer uses this at the next composition seam"
)]
pub(crate) trait GovernedRecommendationInputMaterializerV1: Send + Sync + 'static {
    /// Resolves one exact command into immutable dataset and strict signal-plan evidence.
    async fn materialize_recommendation_input(
        &self,
        command: &GovernedBacktestCommand,
        policy: RecommendationBacktestPolicyV1,
        evaluation_starts_at: Timestamp,
        issuer: &GovernedRecommendationSignalIssuerV1,
        limits: RecommendationBacktestLimits,
        repository_permit: Arc<tokio::sync::OwnedSemaphorePermit>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedRecommendationMaterializedInputV1, ServiceError>;
}

/// Fixed-namespace authority for immutable recipes and fresh point-in-time receipts.
pub struct ProductionGovernedBacktestInputAuthority {
    store: Arc<LocalAuthorityStateStore>,
    index: Arc<Mutex<InputIndex>>,
    materializer: BacktestInputMaterializer,
    limits: GovernedBacktestInputAuthorityLimits,
    lifecycle: Arc<RepositoryLifecycle>,
    recommendation_gate: Arc<tokio::sync::Semaphore>,
}

impl ProductionGovernedBacktestInputAuthority {
    /// Opens the fixed control namespace and strictly validates every retained recipe.
    pub fn try_new(
        paths: &LocalPaths,
        research: Arc<ResearchService>,
        limits: GovernedBacktestInputAuthorityLimits,
    ) -> Result<Self, ProductionGovernedBacktestInputAuthorityError> {
        GovernedBacktestInputAuthorityLimits::try_new(
            limits.maximum_inputs,
            limits.maximum_index_bytes,
            limits.maximum_manifest_nodes,
        )?;
        let control = paths.control_root()?;
        control.try_clone_directory()?;
        let store = Arc::new(LocalAuthorityStateStore::try_open(
            control.root().join(INPUT_INDEX_DIRECTORY),
        )?);
        control.try_clone_directory()?;
        let index = store.load()?.map_or_else(
            || Ok(InputIndex::empty()),
            |bytes| InputIndex::decode(&bytes, limits.index()),
        )?;
        for entry in index.entries() {
            InputRecipe::decode(entry.recipe_bytes())
                .map_err(|_| ProductionGovernedBacktestInputAuthorityError::CorruptIndex)?;
        }
        let materializer =
            BacktestInputMaterializer::try_new(research, limits.maximum_manifest_nodes)
                .map_err(|_| ProductionGovernedBacktestInputAuthorityError::InvalidLimits)?;
        Ok(Self {
            store,
            index: Arc::new(Mutex::new(index)),
            materializer,
            limits,
            lifecycle: RepositoryLifecycle::new(),
            recommendation_gate: Arc::new(tokio::sync::Semaphore::new(1)),
        })
    }

    /// Injects the existing source reader; it reopens original references and acquires no data.
    pub(crate) fn with_source_action_reader(
        mut self,
        reader: crate::application::research::corporate_actions::SourceAppliedCorporateActionReadCapability,
    ) -> Self {
        self.materializer.source_actions = Some(reader);
        self
    }

    /// Materializes, validates, and durably registers one complete immutable input recipe.
    pub async fn register(
        &self,
        input: GovernedBacktestInputRegistrationInput,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedBacktestInputRegistrationReceipt, ServiceError> {
        let registration =
            RegistrationRecipe::try_new(input).map_err(map_registration_recipe_error)?;
        self.register_recipe(registration, cancellation, deadline)
            .await
            .map(|(receipt, _)| receipt)
    }

    /// Binds only genuine completed raw histories to the existing immutable recipe store.
    /// The actual read cutoff must be the one used for every sealed selection. Reopening
    /// compares every original receipt and result before anything is registered.
    pub(crate) async fn register_recommendation_daily<T: recipe::DailyHistoryInput>(
        &self,
        input: GovernedBacktestInputRegistrationInput,
        histories: &[T],
        admitted_at: Timestamp,
        source_action_reference: crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedRecommendationDailyInputRegistrationReceiptV1, ServiceError> {
        let registration = RegistrationRecipe::try_new(input)
            .and_then(|recipe| {
                recipe.with_daily_history(histories, admitted_at, source_action_reference)
            })
            .map_err(map_registration_recipe_error)?;
        let (registration, facts) = self
            .register_recipe(registration, cancellation, deadline)
            .await?;
        Ok(GovernedRecommendationDailyInputRegistrationReceiptV1 {
            registration,
            facts: facts.ok_or(ServiceError::InvalidResult)?,
        })
    }

    /// Reopens the registered subject-only StudyInputs population and evaluates every origin.
    ///
    /// The sealed dataset declares the population before outcomes are examined. This path uses
    /// the original source-action replay and execution assumptions, without a strategy issuer,
    /// authored fills, benchmark membership or outcome-dependent origin selection.
    #[allow(
        clippy::too_many_arguments,
        reason = "the original input, financial policy, bounds and lifecycle capabilities stay explicit"
    )]
    pub(crate) async fn evaluate_all_origin_round_trips(
        &self,
        command: &GovernedBacktestCommand,
        subject: InstrumentId,
        policy: AllOriginRoundTripPolicyV1,
        limits: RecommendationBacktestLimits,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<AllOriginRoundTripEvaluationV1, ServiceError> {
        if command.scope().instruments() != [subject]
            || policy.target_policy().execution_basis
                != market_squawk_data::ProbabilityExecutionBasisV1::CompletedDailyBar
        {
            return Err(ServiceError::InvalidRequest);
        }
        let _call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let permit = tokio::select! {
            () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
            () = self.lifecycle.shutdown_token().cancelled() => return Err(ServiceError::Unavailable),
            result = tokio::time::timeout_at(deadline.into(),
                Arc::clone(&self.recommendation_gate).acquire_owned()) => {
                result.map_err(|_| ServiceError::DeadlineExceeded)?
                    .map_err(|_| ServiceError::Unavailable)?
            }
        };
        // The recipe is immutable and content-addressed. Check the original simulation seed,
        // which is deliberately separate from ResearchExecutionAssumptions, before reopening.
        {
            let stored = self
                .index
                .lock()
                .map_err(|_| ServiceError::Unavailable)?
                .get(command.input_id())
                .ok_or(ServiceError::NotFound)?;
            let recipe = InputRecipe::decode(stored.recipe_bytes())
                .map_err(|_| ServiceError::InvalidResult)?;
            if recipe.core().seed() != policy.target_policy().seed
                || recipe.core().daily_history().is_none()
            {
                return Err(ServiceError::InvalidRequest);
            }
        }
        let materialized = self
            .resolve_materialized(command, None, cancellation.clone(), deadline)
            .await?;
        if !materialized.has_daily_history() {
            return Err(ServiceError::InvalidRequest);
        }
        ensure_operation_live(&cancellation, &self.lifecycle, deadline)?;
        let call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let lifecycle = Arc::clone(&self.lifecycle);
        let operation = LinkedOperation::new(
            cancellation.clone(),
            self.lifecycle.shutdown_token().clone(),
            deadline,
        );
        let worker_cancellation = operation.token().clone();
        let request_cancellation = cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _operation = operation;
            let _call = call;
            let _permit = permit;
            ensure_operation_live(&request_cancellation, &lifecycle, deadline)?;
            let (dataset, corporate_actions, execution_assumptions) =
                materialized.into_recommendation_dataset()?;
            if execution_assumptions != policy.execution_assumptions()
                || dataset.execution_basis() != BacktestExecutionBasis::CompletedDailyBar
                || dataset.study_qualification().is_none()
            {
                return Err(ServiceError::InvalidRequest);
            }
            let evaluated = AllOriginRoundTripEvaluatorV1::evaluate(
                &dataset,
                &corporate_actions,
                subject,
                policy,
                limits,
                &worker_cancellation,
            );
            ensure_operation_live(&request_cancellation, &lifecycle, deadline)?;
            evaluated.map_err(|error| match error {
                market_squawk_backtesting::RecommendationBacktestError::Cancelled => {
                    ServiceError::Cancelled
                }
                market_squawk_backtesting::RecommendationBacktestError::LimitExceeded => {
                    ServiceError::ResourceExhausted
                }
                market_squawk_backtesting::RecommendationBacktestError::InvalidPolicy
                | market_squawk_backtesting::RecommendationBacktestError::InvalidLimits => {
                    ServiceError::InvalidRequest
                }
                _ => ServiceError::InvalidResult,
            })
        });
        let evaluated = await_blocking(
            worker,
            &cancellation,
            self.lifecycle.shutdown_token(),
            deadline,
        )
        .await;
        ensure_operation_live(&cancellation, &self.lifecycle, deadline)?;
        evaluated
    }

    async fn register_recipe(
        &self,
        registration: RegistrationRecipe,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<
        (
            GovernedBacktestInputRegistrationReceipt,
            Option<RecommendationInputFacts>,
        ),
        ServiceError,
    > {
        let call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let linked = LinkedOperation::new(
            cancellation.clone(),
            self.lifecycle.shutdown_token().clone(),
            deadline,
        );
        let materialized = self
            .materializer
            .materialize(registration.core(), None, linked.token().clone(), deadline)
            .await?;
        let evidence = materialized.evidence.clone();
        let facts = materialized.validate_registration()?;
        let recipe = registration
            .bind(evidence)
            .map_err(map_registration_recipe_error)?;
        let encoded = recipe.encode().map_err(map_registration_recipe_error)?;
        let stored = StoredInputRecipe::try_new(encoded, self.limits.index())
            .map_err(map_index_error_to_service)?;
        let command = recipe
            .core()
            .command(stored.input_id().clone())
            .map_err(map_registration_recipe_error)?;
        let store = Arc::clone(&self.store);
        let index = Arc::clone(&self.index);
        let lifecycle = Arc::clone(&self.lifecycle);
        let limits = self.limits.index();
        let worker = tokio::task::spawn_blocking(move || {
            let _call = call;
            persist_recipe(&store, &index, &lifecycle, stored, limits)
        });
        worker.await.map_err(|_| ServiceError::Internal)??;
        ensure_operation_live(&cancellation, &self.lifecycle, deadline)?;
        Ok((GovernedBacktestInputRegistrationReceipt { command }, facts))
    }
}

#[async_trait]
impl GovernedBacktestInputRegistrar for ProductionGovernedBacktestInputAuthority {
    async fn register_input(
        &self,
        input: GovernedBacktestInputRegistrationInput,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedBacktestInputRegistrationReceipt, ServiceError> {
        self.register(input, cancellation, deadline).await
    }
}

#[async_trait]
impl GovernedRecommendationInputMaterializerV1 for ProductionGovernedBacktestInputAuthority {
    async fn materialize_recommendation_input(
        &self,
        command: &GovernedBacktestCommand,
        policy: RecommendationBacktestPolicyV1,
        evaluation_starts_at: Timestamp,
        issuer: &GovernedRecommendationSignalIssuerV1,
        limits: RecommendationBacktestLimits,
        repository_permit: Arc<tokio::sync::OwnedSemaphorePermit>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedRecommendationMaterializedInputV1, ServiceError> {
        let evaluation_ends_at = evaluation_starts_at
            .checked_add_nanos(RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let mut expected_instruments = [
            policy.subject_instrument_id(),
            policy.benchmark().instrument_id(),
            policy.accompanying_benchmark().instrument_id(),
        ];
        expected_instruments.sort_unstable();
        let expected_time_ranges = [(evaluation_starts_at, evaluation_ends_at)];
        if command.scope().instruments() != expected_instruments.as_slice()
            || command.scope().time_ranges() != expected_time_ranges.as_slice()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let _call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let permit = tokio::select! {
            () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
            () = self.lifecycle.shutdown_token().cancelled() => return Err(ServiceError::Unavailable),
            result = tokio::time::timeout_at(deadline.into(),
                Arc::clone(&self.recommendation_gate).acquire_owned()) => {
                result.map_err(|_| ServiceError::DeadlineExceeded)?
                    .map_err(|_| ServiceError::Unavailable)?
            }
        };
        let materialized = self
            .resolve_materialized(
                command,
                Some(Arc::clone(&repository_permit)),
                cancellation.clone(),
                deadline,
            )
            .await?;
        let (dataset, corporate_actions, execution_assumptions) =
            materialized.into_recommendation_dataset()?;
        if execution_assumptions != policy.execution_assumptions()
            || dataset.execution_basis() != policy.execution_basis()
        {
            return Err(ServiceError::InvalidRequest);
        }
        ensure_operation_live(&cancellation, &self.lifecycle, deadline)?;
        let call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let lifecycle = Arc::clone(&self.lifecycle);
        let operation = LinkedOperation::new(
            cancellation.clone(),
            self.lifecycle.shutdown_token().clone(),
            deadline,
        );
        let worker_cancellation = operation.token().clone();
        let issuer = issuer.clone();
        let runtime_handle = tokio::runtime::Handle::current();
        let worker = tokio::task::spawn_blocking(move || {
            let _operation = operation;
            let _call = call;
            let _permit = permit;
            let _repository_permit = repository_permit;
            let signal_plan = RecommendationSignalPlanMaterializerV1::materialize_sequentially(
                &dataset,
                policy,
                evaluation_starts_at,
                issuer.identity(),
                limits,
                |information| {
                    ensure_operation_live(&worker_cancellation, &lifecycle, deadline).map_err(
                        |_| RecommendationSignalPlanMaterializationErrorV1::IssuerUnavailable,
                    )?;
                    runtime_handle
                        .block_on(issuer.issue(information, worker_cancellation.clone(), deadline))
                        .map_err(|_| {
                            RecommendationSignalPlanMaterializationErrorV1::IssuerUnavailable
                        })
                },
            );
            ensure_operation_live(&worker_cancellation, &lifecycle, deadline)?;
            let signal_plan = signal_plan.map_err(map_recommendation_materialization_error)?;
            Ok(GovernedRecommendationMaterializedInputV1 {
                dataset,
                corporate_actions,
                signal_plan,
            })
        });
        await_blocking(
            worker,
            &cancellation,
            self.lifecycle.shutdown_token(),
            deadline,
        )
        .await
    }
}

impl fmt::Debug for ProductionGovernedBacktestInputAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductionGovernedBacktestInputAuthority")
            .field("store", &self.store)
            .field("index", &"[BOUNDED IMMUTABLE INPUT INDEX]")
            .field("materializer", &self.materializer)
            .field("limits", &self.limits)
            .field("lifecycle", &self.lifecycle)
            .finish()
    }
}

impl ProductionGovernedBacktestInputAuthority {
    async fn resolve_materialized(
        &self,
        command: &GovernedBacktestCommand,
        repository_permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<MaterializedInput, ServiceError> {
        let _call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let linked = LinkedOperation::new(
            cancellation.clone(),
            self.lifecycle.shutdown_token().clone(),
            deadline,
        );
        let stored = self
            .index
            .lock()
            .map_err(|_| ServiceError::Unavailable)?
            .get(command.input_id())
            .ok_or(ServiceError::NotFound)?;
        let recipe =
            InputRecipe::decode(stored.recipe_bytes()).map_err(|_| ServiceError::InvalidResult)?;
        let registered_command = recipe
            .core()
            .command(stored.input_id().clone())
            .map_err(|_| ServiceError::InvalidResult)?;
        if &registered_command != command {
            return Err(ServiceError::InvalidRequest);
        }
        let expected = recipe.expected().map_err(|_| ServiceError::InvalidResult)?;
        let materialized = self
            .materializer
            .materialize(
                recipe.core(),
                repository_permit,
                linked.token().clone(),
                deadline,
            )
            .await?;
        if materialized.evidence != expected {
            return Err(ServiceError::InvalidResult);
        }
        ensure_operation_live(&cancellation, &self.lifecycle, deadline)?;
        Ok(materialized)
    }
}

#[async_trait]
impl GovernedBacktestInputResolver for ProductionGovernedBacktestInputAuthority {
    async fn resolve(
        &self,
        command: &GovernedBacktestCommand,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<ResolvedGovernedBacktestInput, ServiceError> {
        let materialized = self
            .resolve_materialized(command, None, cancellation, deadline)
            .await?;
        if materialized.has_daily_history() {
            // The generic quote engine cannot reinterpret completed outcome bars as quote depth.
            return Err(ServiceError::InvalidRequest);
        }
        Ok(ResolvedGovernedBacktestInput::new(
            command.strategy_id().clone(),
            command.input_id().clone(),
            command.scope().clone(),
            materialized.into_generic()?,
        ))
    }

    fn begin_shutdown(&self) {
        self.lifecycle.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.lifecycle.finish_shutdown(deadline).await
    }
}

impl Drop for ProductionGovernedBacktestInputAuthority {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

/// Construction or strict recovery failure before the input authority can accept work.
#[derive(Debug, Error)]
pub enum ProductionGovernedBacktestInputAuthorityError {
    /// Configured limits are zero or exceed fixed process/persistence ceilings.
    #[error("governed backtest input-authority limits are invalid")]
    InvalidLimits,
    /// The prepared local control capability is unavailable or changed identity.
    #[error("governed backtest input control path is unavailable: {0}")]
    Path(#[from] PathError),
    /// The two-copy authority store could not be opened or recovered.
    #[error("governed backtest input state is unavailable: {0}")]
    Authority(#[from] LocalAuthorityStateStoreError),
    /// Retained state is malformed, noncanonical, unsupported, or internally inconsistent.
    #[error("governed backtest input index is corrupt")]
    CorruptIndex,
    /// Retained state exceeds its bounded allocation or encoding contract.
    #[error("governed backtest input index exceeded its resource contract")]
    ResourceExhausted,
}

impl From<InputIndexError> for ProductionGovernedBacktestInputAuthorityError {
    fn from(value: InputIndexError) -> Self {
        match value {
            InputIndexError::ResourceExhausted => Self::ResourceExhausted,
            InputIndexError::Corrupt | InputIndexError::Conflict => Self::CorruptIndex,
        }
    }
}

fn persist_recipe(
    store: &LocalAuthorityStateStore,
    index: &Mutex<InputIndex>,
    lifecycle: &RepositoryLifecycle,
    stored: StoredInputRecipe,
    limits: InputIndexLimits,
) -> Result<(), ServiceError> {
    let mut current = index.lock().map_err(|_| ServiceError::Unavailable)?;
    let mut candidate = current.clone();
    match candidate
        .insert(stored, limits)
        .map_err(map_index_error_to_service)?
    {
        InputInsertDisposition::Replay => return Ok(()),
        InputInsertDisposition::Added => {}
    }
    let encoded = candidate
        .encode(limits)
        .map_err(map_index_error_to_service)?;
    if let Err(error) = store.store(&encoded) {
        lifecycle.begin_shutdown();
        return Err(map_authority_error_to_service(error));
    }
    *current = candidate;
    Ok(())
}

fn map_registration_recipe_error(error: RecipeError) -> ServiceError {
    match error {
        RecipeError::Invalid => ServiceError::InvalidRequest,
        RecipeError::ResourceExhausted => ServiceError::ResourceExhausted,
    }
}

fn map_recommendation_materialization_error(
    error: RecommendationSignalPlanMaterializationErrorV1,
) -> ServiceError {
    match error {
        RecommendationSignalPlanMaterializationErrorV1::LimitExceeded => {
            ServiceError::ResourceExhausted
        }
        RecommendationSignalPlanMaterializationErrorV1::IssuerUnavailable => {
            ServiceError::Unavailable
        }
        RecommendationSignalPlanMaterializationErrorV1::PolicyMismatch
        | RecommendationSignalPlanMaterializationErrorV1::InvalidEvaluationWindow
        | RecommendationSignalPlanMaterializationErrorV1::InvalidInstruction => {
            ServiceError::InvalidRequest
        }
        RecommendationSignalPlanMaterializationErrorV1::DatasetScopeMismatch
        | RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel
        | RecommendationSignalPlanMaterializationErrorV1::InstructionEvidenceMismatch
        | RecommendationSignalPlanMaterializationErrorV1::MaterializationDrift => {
            ServiceError::InvalidResult
        }
    }
}

fn map_index_error_to_service(error: InputIndexError) -> ServiceError {
    match error {
        InputIndexError::ResourceExhausted => ServiceError::ResourceExhausted,
        InputIndexError::Conflict | InputIndexError::Corrupt => ServiceError::InvalidResult,
    }
}

fn map_authority_error_to_service(error: LocalAuthorityStateStoreError) -> ServiceError {
    match error {
        LocalAuthorityStateStoreError::PayloadTooLarge { .. }
        | LocalAuthorityStateStoreError::EnvelopeTooLarge { .. }
        | LocalAuthorityStateStoreError::Allocation
        | LocalAuthorityStateStoreError::GenerationExhausted => ServiceError::ResourceExhausted,
        _ => ServiceError::Unavailable,
    }
}

impl ProductionGovernedBacktestInputAuthority {
    /// Captures one canonical index and its original sealed durable revision; no source authority is minted.
    pub(crate) async fn export_backup_index(
        &self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<(Vec<u8>, [u8; 32]), ServiceError> {
        let call = RepositoryLifecycle::enter(&self.lifecycle, cancellation, deadline)?;
        let index = Arc::clone(&self.index);
        let store = Arc::clone(&self.store);
        let lifecycle = Arc::clone(&self.lifecycle);
        let limits = self.limits;
        let worker_cancellation = cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _call = call;
            ensure_operation_live(&worker_cancellation, &lifecycle, deadline)?;
            let current = index.try_lock().map_err(|_| ServiceError::Unavailable)?;
            let bytes = current
                .encode(limits.index())
                .map_err(map_index_error_to_service)?;
            Self::validate_backup_index(&bytes, limits)?;
            let durable = store
                .load_snapshot()
                .map_err(map_authority_error_to_service)?;
            let mut revision = sha2::Sha256::new();
            revision.update(b"market-squawk/input_index_directory-backup/v1\0");
            match durable {
                Some(snapshot) => {
                    if snapshot.payload() != bytes.as_slice() {
                        return Err(ServiceError::InvalidResult);
                    }
                    revision.update([1]);
                    revision.update(snapshot.context().authentication_bytes());
                }
                None if current.entries().is_empty() => revision.update([0]),
                None => return Err(ServiceError::InvalidResult),
            }
            revision.update(&bytes);
            ensure_operation_live(&worker_cancellation, &lifecycle, deadline)?;
            Ok((bytes, revision.finalize().into()))
        });
        await_blocking(
            worker,
            cancellation,
            self.lifecycle.shutdown_token(),
            deadline,
        )
        .await
    }

    pub(crate) async fn revalidate_backup_index(
        &self,
        expected_revision: [u8; 32],
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), ServiceError> {
        let (_, actual) = self.export_backup_index(cancellation, deadline).await?;
        if actual != expected_revision {
            return Err(ServiceError::InvalidResult);
        }
        Ok(())
    }

    pub(crate) fn validate_backup_index(
        bytes: &[u8],
        limits: GovernedBacktestInputAuthorityLimits,
    ) -> Result<(), ServiceError> {
        let decoded =
            InputIndex::decode(bytes, limits.index()).map_err(map_index_error_to_service)?;
        for entry in decoded.entries() {
            InputRecipe::decode(entry.recipe_bytes()).map_err(map_registration_recipe_error)?;
        }
        drop(decoded);
        Ok(())
    }

    /// Writes only the existing fixed namespace in a fresh, unpublished restore target.
    pub(crate) fn restore_backup_index_fresh(
        paths: &LocalPaths,
        limits: GovernedBacktestInputAuthorityLimits,
        bytes: &[u8],
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), ServiceError> {
        Self::validate_backup_index(bytes, limits)?;
        let ensure_live = || {
            if cancellation.is_cancelled() {
                Err(ServiceError::Cancelled)
            } else if Instant::now() >= deadline {
                Err(ServiceError::DeadlineExceeded)
            } else {
                Ok(())
            }
        };
        ensure_live()?;
        let control = paths
            .control_root()
            .map_err(|_| ServiceError::Unavailable)?;
        control
            .try_clone_directory()
            .map_err(|_| ServiceError::Unavailable)?;
        let store = LocalAuthorityStateStore::try_open(control.root().join(INPUT_INDEX_DIRECTORY))
            .map_err(map_authority_error_to_service)?;
        control
            .try_clone_directory()
            .map_err(|_| ServiceError::Unavailable)?;
        if store
            .load_snapshot()
            .map_err(map_authority_error_to_service)?
            .is_some()
        {
            return Err(ServiceError::InvalidRequest);
        }
        ensure_live()?;
        store.store(bytes).map_err(map_authority_error_to_service)?;
        if store
            .load()
            .map_err(map_authority_error_to_service)?
            .as_deref()
            != Some(bytes)
        {
            return Err(ServiceError::InvalidResult);
        }
        ensure_live()
    }
}
