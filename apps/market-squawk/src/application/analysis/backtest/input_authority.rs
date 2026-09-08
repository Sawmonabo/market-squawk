//! Restart-safe registration and fresh resolution of governed-backtest inputs.

mod index;
mod preparation;
mod recipe;
mod resolution;

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use market_squawk_backtesting::{
    BacktestDataset, MaterializedRecommendationSignalPlanV1,
    RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1, RecommendationAggregateEvidenceV1,
    RecommendationBacktestKernelV1, RecommendationBacktestLimits, RecommendationBacktestPolicyV1,
    RecommendationBacktestPublicationV1, RecommendationBacktestStudyV1,
    RecommendationSignalInformationSetV1, RecommendationSignalIssuanceV1,
    RecommendationSignalIssuerIdentityV1, RecommendationSignalPlanCompletenessV1,
    RecommendationSignalPlanMaterializationErrorV1, RecommendationSignalPlanMaterializerV1,
};
use market_squawk_data::{DatasetManifestRef, Sha256Digest};
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_platform::{
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths, PathError,
};
use market_squawk_services::ServiceError;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    GovernedBacktestCommand, GovernedBacktestInputResolver, ResolvedGovernedBacktestInput,
    repository::lifecycle::{LinkedOperation, RepositoryLifecycle, ensure_operation_live},
};
use crate::ResearchService;

use index::{
    InputIndex, InputIndexError, InputIndexLimits, InputInsertDisposition, StoredInputRecipe,
};
use recipe::{InputRecipe, RecipeError, RegistrationRecipe};
use resolution::BacktestInputMaterializer;

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
    ) -> Result<GovernedRecommendationBacktestEvidenceV1, ServiceError> {
        let study = RecommendationBacktestKernelV1::run_materialized_study(
            &self.dataset,
            policy,
            &self.signal_plan,
            publication,
            self.signal_plan.limits(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        GovernedRecommendationBacktestEvidenceV1::try_new(study, &self.signal_plan)
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
    study: RecommendationBacktestStudyV1,
    materialized_signal_plan_digest: Sha256Digest,
    issuer_identity: RecommendationSignalIssuerIdentityV1,
    digest: Sha256Digest,
}

#[allow(
    dead_code,
    reason = "the installed recommendation recipe is composed at the next serialized seam"
)]
impl GovernedRecommendationBacktestEvidenceV1 {
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
            study,
            materialized_signal_plan_digest,
            issuer_identity,
            digest,
        })
    }

    /// Complete research study admitted through the installed issuer path.
    #[must_use]
    pub(crate) const fn study(&self) -> &RecommendationBacktestStudyV1 {
        &self.study
    }

    /// Exact PIT dataset identity.
    #[must_use]
    pub(crate) const fn dataset_identity(&self) -> Sha256Digest {
        self.study.dataset_identity()
    }

    /// Complete strict recommendation policy.
    #[must_use]
    pub(crate) const fn policy(&self) -> RecommendationBacktestPolicyV1 {
        self.study.policy()
    }

    /// Exact canonical signal-plan identity.
    #[must_use]
    pub(crate) const fn signal_plan_digest(&self) -> Sha256Digest {
        self.study.signal_plan_digest()
    }

    /// Exact content-derived sequential issuer-plan identity.
    #[must_use]
    pub(crate) const fn preauthorized_signal_plan_digest(&self) -> Sha256Digest {
        self.study.preauthorized_signal_plan_digest()
    }

    /// Exact evaluation and publication timing.
    #[must_use]
    pub(crate) const fn publication(&self) -> RecommendationBacktestPublicationV1 {
        self.study.publication()
    }

    /// Exact aggregates or a typed incomplete-evidence refusal.
    #[must_use]
    pub(crate) const fn aggregate(&self) -> RecommendationAggregateEvidenceV1 {
        self.study.aggregate()
    }

    /// Exact sequential materialization identity.
    #[must_use]
    pub(crate) const fn materialized_signal_plan_digest(&self) -> Sha256Digest {
        self.materialized_signal_plan_digest
    }

    /// Exact semantic identity of the installed code-owned issuer.
    #[must_use]
    pub(crate) const fn issuer_identity(&self) -> &RecommendationSignalIssuerIdentityV1 {
        &self.issuer_identity
    }

    /// Complete app-owned governed-evidence identity.
    #[must_use]
    pub(crate) const fn digest(&self) -> Sha256Digest {
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

type GovernedRecommendationSignalIssueFnV1 = for<'dataset> fn(
    &RecommendationSignalInformationSetV1<'dataset>,
) -> Result<
    RecommendationSignalIssuanceV1,
    RecommendationSignalPlanMaterializationErrorV1,
>;

/// Nonconstructible capability for the code-owned recommendation signal issuer.
///
/// The future recipe owner must be composed through this module before this capability can exist.
/// Ordinary application callers cannot implement a trait or self-declare producer identity. The
/// callback receives only the immutable lineage-confined view and returns economic instructions;
/// the backtesting materializer derives row lineage, availability, and the complete plan digest.
#[allow(
    dead_code,
    reason = "the code-owned issuer recipe is installed at the next serialized composition seam"
)]
#[derive(Debug)]
pub(crate) struct GovernedRecommendationSignalIssuerV1 {
    identity: RecommendationSignalIssuerIdentityV1,
    issue: GovernedRecommendationSignalIssueFnV1,
}

#[allow(
    dead_code,
    reason = "the code-owned issuer recipe is installed at the next serialized composition seam"
)]
impl GovernedRecommendationSignalIssuerV1 {
    fn issue(
        &self,
        view: &RecommendationSignalInformationSetV1<'_>,
    ) -> Result<RecommendationSignalIssuanceV1, RecommendationSignalPlanMaterializationErrorV1>
    {
        (self.issue)(view)
    }

    fn identity(&self) -> RecommendationSignalIssuerIdentityV1 {
        self.identity.clone()
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
        })
    }

    /// Materializes, validates, and durably registers one complete immutable input recipe.
    pub async fn register(
        &self,
        input: GovernedBacktestInputRegistrationInput,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedBacktestInputRegistrationReceipt, ServiceError> {
        let call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let linked = LinkedOperation::new(
            cancellation.clone(),
            self.lifecycle.shutdown_token().clone(),
            deadline,
        );
        let registration =
            RegistrationRecipe::try_new(input).map_err(map_registration_recipe_error)?;
        let materialized = self
            .materializer
            .materialize(registration.core(), linked.token().clone(), deadline)
            .await?;
        let evidence = materialized.evidence.clone();
        materialized.validate_registration()?;
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
        Ok(GovernedBacktestInputRegistrationReceipt { command })
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
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedRecommendationMaterializedInputV1, ServiceError> {
        let evaluation_ends_at = evaluation_starts_at
            .checked_add_nanos(RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let mut expected_instruments = [
            policy.subject_instrument_id(),
            policy.benchmark().instrument_id(),
        ];
        expected_instruments.sort_unstable();
        let expected_time_ranges = [(evaluation_starts_at, evaluation_ends_at)];
        if command.scope().instruments() != expected_instruments.as_slice()
            || command.scope().time_ranges() != expected_time_ranges.as_slice()
        {
            return Err(ServiceError::InvalidRequest);
        }
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
            .materialize(recipe.core(), linked.token().clone(), deadline)
            .await?;
        if materialized.evidence != expected || materialized.input.cohort.is_some() {
            return Err(ServiceError::InvalidResult);
        }
        let crate::PinnedBacktestInput {
            query,
            instrument_definitions,
            execution_assumptions,
            portfolio: _,
            corporate_actions,
            sources: _,
            seed: _,
            limits: dataset_limits,
            experiment: _,
            cohort: _,
        } = materialized.input;
        if execution_assumptions != policy.execution_assumptions() || corporate_actions.is_some() {
            return Err(ServiceError::InvalidRequest);
        }
        let dataset =
            BacktestDataset::try_from_pinned_query(query, instrument_definitions, dataset_limits)
                .map_err(|_| ServiceError::InvalidResult)?;
        ensure_operation_live(&cancellation, &self.lifecycle, deadline)?;
        let signal_plan = RecommendationSignalPlanMaterializerV1::materialize_sequentially(
            &dataset,
            policy,
            evaluation_starts_at,
            issuer.identity(),
            limits,
            |information| {
                ensure_operation_live(&cancellation, &self.lifecycle, deadline).map_err(|_| {
                    RecommendationSignalPlanMaterializationErrorV1::IssuerUnavailable
                })?;
                issuer.issue(information)
            },
        )
        .map_err(map_recommendation_materialization_error)?;
        ensure_operation_live(&cancellation, &self.lifecycle, deadline)?;
        Ok(GovernedRecommendationMaterializedInputV1 {
            dataset,
            signal_plan,
        })
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

#[async_trait]
impl GovernedBacktestInputResolver for ProductionGovernedBacktestInputAuthority {
    async fn resolve(
        &self,
        command: &GovernedBacktestCommand,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<ResolvedGovernedBacktestInput, ServiceError> {
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
            .materialize(recipe.core(), linked.token().clone(), deadline)
            .await?;
        if materialized.evidence != expected {
            return Err(ServiceError::InvalidResult);
        }
        ensure_operation_live(&cancellation, &self.lifecycle, deadline)?;
        Ok(ResolvedGovernedBacktestInput::new(
            command.strategy_id().clone(),
            command.input_id().clone(),
            command.scope().clone(),
            materialized.input,
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
        | RecommendationSignalPlanMaterializationErrorV1::MissingEntryInFold
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
