//! One installed tool surface shared by native and MCP transports.

use super::probability::{
    InstalledProbabilityPreparation, PREPARE_PROBABILITY_EVENT, START_PROBABILITY_DATASET,
};
mod current_find;
mod find_results;
pub(super) mod training_preparation;
use current_find::InstalledCurrentFind;
use training_preparation::{
    GET_DATASET_RESULT, GET_TRAINING_RESULT, InstalledProductTraining, START_INVESTMENT_DATASET,
    START_PREPARED_TRAINING,
};

use std::{future::Future, pin::Pin, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    JobAuthority, JobAuthorityError, JobGeneration, JobId, JobOrigin, JobRepository,
    JobRepositoryError, JobSnapshot, JobState, SqliteJobRepository,
};
use market_squawk_runtime::{ClientId, InputStager, InputTicketId, RuntimeIdentity};
use market_squawk_services::{
    RequestContext, RequestId, ServiceCapabilities, ServiceError, ToolResultMetadata, ToolServices,
    TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    LocalProduct,
    application::{
        Application, DatasetPreparationPreviewRequest, DatasetPreparationReceipt,
        DatasetPreparationSelection, MarketRuntimeRegistry,
        lifecycle::WorkspaceRuntimeIdentity,
        market_calendar::{
            CompletedMarketSessionReadCapability, context::MarketSessionContextReadCapability,
        },
        model::{LIST_PRODUCT_ACTIVITY, product_model_activity},
        operations::OperationsApplicationServices,
        recommendation::RecommendationSetupAuthority,
    },
    jobs::{InstalledJobAuthority, InstalledJobRunners},
    local_product::cli_dataset::admit_inline_phase_one_derived_generation_request,
};

use super::{
    analysis::InstalledAnalysisOperations,
    backtest_preparation::{InstalledBacktestPreparation, START_PREPARED_BACKTEST},
    decision::{
        GENERATE_INVESTMENT_ANALYSIS, InstalledDecisionOperations, RUN_SCREEN,
        investment_generation::InvestmentGenerationOperations,
    },
    forecast_preparation::{
        GET_FISCAL_PREPARATION_PLAN, InstalledForecastPreparation, START_FISCAL_DATASET_BUILD,
        START_FISCAL_FORECAST, START_PREPARED_FORECAST,
    },
    historical_study::{
        COMPLETE_HISTORICAL_STUDY_FISCAL_PAGE, GET_HISTORICAL_STUDY_PLAN, InstalledHistoricalStudy,
        START_HISTORICAL_STUDY_DATASET, START_HISTORICAL_STUDY_TRAINING,
        START_RECOMMENDATION_BACKTEST,
    },
    jobs::{InstalledJobOperations, JobStartAdmission},
    market_evidence::{InstalledMarketEvidence, InvestmentEvidenceJobRunner, PREPARE},
    operations::InstalledOperations,
    portfolio_analysis::InstalledPortfolioAnalysis,
    portfolio_import::InstalledPortfolioImportOperations,
    provider_credential_import::{
        IMPORT_PROVIDER_CREDENTIAL_BUNDLE, InstalledProviderCredentialImport,
    },
    recommendation_backtest::InstalledRecommendationBacktestReadOperations,
    recommendation_setup::InstalledRecommendationSetupOperations,
    research_dataset::InstalledResearchDatasetPreparation,
    research_file_import::{InstalledResearchFileImportOperations, PreparedResearchFileCommit},
};

const START_FINANCIALS: &str = "Research.StartInvestmentFinancialPreparation";
const START_HISTORY: &str = "Market.StartHistoryPreparation";
const START_INGEST: &str = "Research.StartIngestSource";
const START_EXPORT: &str = "Research.StartExport";
const START_DATASET: &str = "Research.StartDatasetBuild";
const START_FEATURE_DATASET: &str = "Analysis.StartFeatureDatasetBuild";
const GET_FEATURE_DATASET_PREPARATION: &str = "Analysis.GetFeatureDatasetPreparationOptions";
const PREVIEW_FEATURE_DATASET: &str = "Analysis.PreviewFeatureDatasetBuild";
const START_PREPARED_FEATURE_DATASET: &str = "Analysis.StartPreparedFeatureDatasetBuild";
const START_SCENARIO: &str = "Analysis.StartScenarioBatch";
const START_BACKTEST: &str = "Analysis.StartBacktest";
const START_TRAINING: &str = "Model.StartTraining";
const TRAINING_CONFIG_MEDIA_TYPE: &str = "market-squawk.training-config.v1";
const TRAINING_AUTHORITY_MEDIA_TYPE: &str = "market-squawk.model-authority.v1";
const RESEARCH_INGEST_JOB_KIND: &str = "research.ingest-source.v1";
const RESEARCH_INGEST_INPUT_AUTHORITY: &str = "research.ingest-request.v1";
const RESEARCH_INGEST_RESULT_AUTHORITY: &str = "research.dataset-publication.v1";

/// Sole transport-neutral installed-service composition.
pub(super) struct InstalledToolServices {
    application: Arc<Application>,
    product_capabilities: ServiceCapabilities,
    pub(super) analytical_workflow:
        Arc<crate::application::analytical_workflow::host::WorkflowHost>,
    jobs: InstalledJobOperations,
    runners: Arc<InstalledJobRunners>,
    inputs: Arc<InputStager>,
    runtime: RuntimeIdentity,
    dataset_preparation: InstalledResearchDatasetPreparation,
    historical_study: InstalledHistoricalStudy,
    probability_preparation: InstalledProbabilityPreparation,
    historical_reader: Option<
        Arc<crate::application::analysis::HistoricalRecommendationAlphaProducerReadCapability>,
    >,
    backtest_preparation: InstalledBacktestPreparation,
    forecast_preparation: Arc<InstalledForecastPreparation>,
    profile_benchmarks: crate::application::RecommendationBenchmarkSelectionReadCapability,
    profile_research: Arc<crate::ResearchService>,
    training_preparation: InstalledProductTraining,
    current_find: InstalledCurrentFind,
    market_evidence: Option<Arc<InstalledMarketEvidence>>,
    investment_evidence: Option<Arc<InvestmentEvidenceJobRunner>>,
    market_session_runtime: Arc<MarketRuntimeRegistry>,
    market_session_reader: MarketSessionContextReadCapability,
    recommendation_backtest: InstalledRecommendationBacktestReadOperations,
    analysis: InstalledAnalysisOperations,
    decisions: InstalledDecisionOperations,
    operations: InstalledOperations,
    portfolio_analysis: InstalledPortfolioAnalysis,
    portfolio_import: InstalledPortfolioImportOperations,
    recommendation_setup: InstalledRecommendationSetupOperations,
    provider_credential_import: InstalledProviderCredentialImport,
    research_file_import: InstalledResearchFileImportOperations,
    research_file_job_repository: Arc<SqliteJobRepository>,
    research_file_job_authority: Arc<JobAuthority<SqliteJobRepository>>,
}

/// Application authorities required to compose the installed tool surface.
pub(super) struct InstalledToolServiceAuthorities<'a> {
    application: Arc<Application>,
    operations: Arc<OperationsApplicationServices>,
    recommendation_setup: Arc<RecommendationSetupAuthority>,
    product: &'a LocalProduct,
    jobs: &'a InstalledJobAuthority,
}

impl<'a> InstalledToolServiceAuthorities<'a> {
    pub(super) fn new(
        application: Arc<Application>,
        operations: Arc<OperationsApplicationServices>,
        recommendation_setup: Arc<RecommendationSetupAuthority>,
        product: &'a LocalProduct,
        jobs: &'a InstalledJobAuthority,
    ) -> Self {
        Self {
            application,
            operations,
            recommendation_setup,
            product,
            jobs,
        }
    }
}

/// Runtime-owned resources required to compose the installed tool surface.
pub(super) struct InstalledToolServiceRuntime {
    runners: Arc<InstalledJobRunners>,
    forecast_preparation: Arc<InstalledForecastPreparation>,
    market_evidence: Option<Arc<InstalledMarketEvidence>>,
    investment_evidence: Option<Arc<InvestmentEvidenceJobRunner>>,
    inputs: Arc<InputStager>,
    runtime: RuntimeIdentity,
    portfolio_import: InstalledPortfolioImportOperations,
    provider_credential_import: InstalledProviderCredentialImport,
    research_file_import: InstalledResearchFileImportOperations,
}

impl InstalledToolServiceRuntime {
    pub(super) fn new(
        runners: Arc<InstalledJobRunners>,
        forecast_preparation: Arc<InstalledForecastPreparation>,
        market_evidence: Option<Arc<InstalledMarketEvidence>>,
        investment_evidence: Option<Arc<InvestmentEvidenceJobRunner>>,
        inputs: Arc<InputStager>,
        runtime: RuntimeIdentity,
        portfolio_import: InstalledPortfolioImportOperations,
        provider_credential_import: InstalledProviderCredentialImport,
        research_file_import: InstalledResearchFileImportOperations,
    ) -> Self {
        Self {
            runners,
            forecast_preparation,
            market_evidence,
            investment_evidence,
            inputs,
            runtime,
            portfolio_import,
            provider_credential_import,
            research_file_import,
        }
    }
}

impl InstalledToolServices {
    pub(super) fn provider_setup_session_gate(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.provider_credential_import.session_gate()
    }

    pub(super) fn try_new(
        authorities: InstalledToolServiceAuthorities<'_>,
        runtime_resources: InstalledToolServiceRuntime,
    ) -> Result<Self, ServiceError> {
        let InstalledToolServiceAuthorities {
            application,
            operations,
            recommendation_setup,
            product,
            jobs,
        } = authorities;
        let InstalledToolServiceRuntime {
            runners,
            forecast_preparation,
            market_evidence,
            investment_evidence,
            inputs,
            runtime,
            portfolio_import,
            provider_credential_import,
            research_file_import,
        } = runtime_resources;
        let policy = market_squawk_decisions::RecommendationPolicy::v1()
            .map_err(|_| ServiceError::Internal)?;
        let maximum_mark_age_nanos = u64::try_from(policy.parameters().market_max_age_nanos)
            .map_err(|_| ServiceError::Internal)?;
        let markets =
            crate::application::market_selection::MarketInvestmentReadCapability::try_new(
                product.research(),
                product.research().instrument_definitions(),
                product.research().market_data_instruments(),
                maximum_mark_age_nanos,
            )?;
        let dataset_preparation = InstalledResearchDatasetPreparation::new(
            product.research(),
            product.macro_context_read_capability(),
            CompletedMarketSessionReadCapability::new(product.research(), product.market_runtime()),
            product.artifacts(),
        );
        let historical_study =
            InstalledHistoricalStudy::new(product, jobs, dataset_preparation.authority());
        let probability_preparation =
            InstalledProbabilityPreparation::new(product, jobs, dataset_preparation.authority())
                .with_outcome_publication(
                    product.source_action_preparation(),
                    product.model_domain(),
                    Arc::clone(runners.analysis_phase_one_feature_derived_generation()),
                    jobs,
                );
        let historical_reader = match (
            product.model_runtime(), runners.forecast().preparation_authority(),
            historical_study.fiscal_reader(),
        ) {
            (Some(model_runtime), Some(preparation), Some(fiscal_reader)) => Some(Arc::new(
                crate::application::analysis::HistoricalRecommendationAlphaProducerReadCapability::new(
                    model_runtime, preparation,
                    WorkspaceRuntimeIdentity::try_from_runtime(runtime).map_err(|_| ServiceError::Internal)?,
                    crate::application::RecommendationBenchmarkSelectionReadCapability::new(
                        product.research().market_data_instruments(),
                    ),
                    product.research(), product.fair_value_service(), fiscal_reader,
                    product.macro_context_read_capability(),
                    CompletedMarketSessionReadCapability::new(product.research(), product.market_runtime()),
                ),
            )),
            _ => None,
        };
        let investment_generation = InvestmentGenerationOperations::new(
            product.decisions(),
            product.research(),
            markets.clone(),
            product.macro_context_read_capability(),
            CompletedMarketSessionReadCapability::new(product.research(), product.market_runtime()),
            product.market_history_read_capability(),
            product.model_domain(),
            product.fair_value_service(),
            crate::application::fair_value::ForecastValuationSourceFactory::new(
                product.model_domain(),
                product.research(),
            ),
            product.backtest_inputs(),
            product.backtest_repository(),
            historical_reader.clone(),
            std::num::NonZeroUsize::new(
                crate::application::model::forecast::MAXIMUM_FORECAST_ARTIFACT_BYTES,
            )
            .ok_or(ServiceError::Internal)?,
            crate::application::SourceAppliedCorporateActionReadCapability::new(
                product.research(),
                CompletedMarketSessionReadCapability::new(
                    product.research(),
                    product.market_runtime(),
                ),
            )
            .with_artifact_repository(product.artifacts()),
        );
        let installed_operations = InstalledOperations::new(
            operations,
            jobs,
            Arc::clone(runners.backup()),
            Arc::clone(runners.recovery()),
            Arc::clone(runners.update()),
        );
        let product_capabilities = application
            .product_capabilities()
            .map_err(|_error| ServiceError::Internal)?;
        let analytical_workflow =
            crate::application::analytical_workflow::host::WorkflowHost::open(
                product.paths(),
                runtime.workspace_id().as_uuid(),
                crate::application::market_selection::product::MarketProductSelectionReadCapability::new(
                    product.research(),
                    product.research().market_data_instruments(),
                ),
            )
            .map_err(|_| ServiceError::Unavailable)?;
        Ok(Self {
            analytical_workflow,
            application: Arc::clone(&application),
            product_capabilities,
            jobs: InstalledJobOperations::new(jobs)
                .with_investment_evidence(investment_evidence.clone()),
            runners,
            inputs: Arc::clone(&inputs),
            runtime,
            dataset_preparation,
            historical_study,
            probability_preparation,
            historical_reader: historical_reader.clone(),
            backtest_preparation: InstalledBacktestPreparation::try_new(
                product.research().analytical_reader(),
                product.backtests(),
                runtime,
            )?,
            forecast_preparation,
            profile_benchmarks:
                crate::application::RecommendationBenchmarkSelectionReadCapability::new(
                    product.research().market_data_instruments(),
                ),
            profile_research: product.research(),
            training_preparation: InstalledProductTraining::new(product, jobs),
            current_find: InstalledCurrentFind::new(product, runtime),
            market_evidence,
            investment_evidence,
            market_session_runtime: product.market_runtime(),
            market_session_reader: MarketSessionContextReadCapability::new(
                product.research(),
                product.market_runtime(),
            ),
            recommendation_backtest: InstalledRecommendationBacktestReadOperations::new(
                product.backtest_inputs(),
                product.backtest_repository(),
                product.decisions(),
                runtime,
                historical_reader,
            ),
            analysis: InstalledAnalysisOperations::new(product, jobs),
            decisions: InstalledDecisionOperations::try_new(
                Arc::clone(&application),
                product.decisions(),
                product.fair_value_service().dossier_read_capability(),
                product.research().analytical_reader(),
                product.research().market_data_instruments(),
                product.portfolio().fair_value_reader(),
                product.portfolio().account_catalog_reader(),
                runtime,
                CompletedMarketSessionReadCapability::new(
                    product.research(),
                    product.market_runtime(),
                ),
                product.research(),
                investment_generation,
            )?,
            operations: installed_operations,
            portfolio_analysis: InstalledPortfolioAnalysis::new(
                product.portfolio_candidate_resolution(),
                Arc::clone(&recommendation_setup),
                product.portfolio().account_catalog_reader(),
                product.portfolio().candidate_impact_reader(),
                product.research().analytical_reader(),
                product.research(),
                CompletedMarketSessionReadCapability::new(
                    product.research(),
                    product.market_runtime(),
                ),
            ),
            portfolio_import,
            recommendation_setup: InstalledRecommendationSetupOperations::try_new(
                recommendation_setup,
                product.portfolio().account_catalog_reader(),
                runtime,
            )?,
            provider_credential_import,
            research_file_import,
            research_file_job_repository: jobs.repository(),
            research_file_job_authority: jobs.authority(),
        })
    }

    pub(super) fn recover_promoting_portfolio_imports(
        &self,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        self.portfolio_import.recover_promoting(context)
    }

    /// Complete authority registry for native management and product presentations.
    pub(super) fn capabilities(&self) -> ServiceCapabilities {
        self.application.capabilities()
    }

    pub(super) async fn recover_promoting_research_file_imports(
        &self,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        let committed = self.research_file_import.committed_jobs()?;
        self.research_file_import.discard_pending_after_restart()?;
        for preview_id in self.research_file_import.recovery_ids()? {
            self.restart_research_file_import(&preview_id, context)
                .await?;
        }
        for (preview_id, receipt) in committed {
            let view = self
                .jobs
                .view(receipt.job_id(), receipt.generation())
                .await?;
            if view.generation().get() != receipt.generation()
                || view.kind().as_str() != RESEARCH_INGEST_JOB_KIND
            {
                return Err(ServiceError::InvalidResult);
            }
            if research_file_import_requires_restart(&view)?
                && self
                    .research_file_import
                    .reopen_committed_job(&preview_id, receipt.job_id())?
            {
                self.restart_research_file_import(&preview_id, context)
                    .await?;
            }
        }
        Ok(())
    }

    async fn restart_research_file_import(
        &self,
        preview_id: &str,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        loop {
            let mut prepared = self
                .research_file_import
                .prepare_recovery(preview_id, context)
                .await?;
            let job_id = prepared.job_start().job_id().to_owned();
            let (result, reconciled_existing) = self
                .start_research_file_import_job(&mut prepared, context)
                .await?;
            let generation = result
                .structured_content()
                .get("generation")
                .and_then(Value::as_u64)
                .filter(|generation| *generation > 0)
                .ok_or(ServiceError::InvalidResult)?;
            self.research_file_import
                .complete_commit(prepared.preview_id(), &result)?;
            drop(prepared);
            if !reconciled_existing {
                return Ok(());
            }
            let view = self.jobs.view(&job_id, generation).await?;
            if !research_file_import_requires_restart(&view)?
                || !self
                    .research_file_import
                    .reopen_committed_job(preview_id, &job_id)?
            {
                return Ok(());
            }
        }
    }

    async fn start_job(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Option<TypedToolResult>, ServiceError> {
        if !owns_job_start(request.name()) {
            return Ok(None);
        }
        ensure_live(context)?;
        let descriptor = self
            .application
            .capabilities()
            .find(request.name())
            .cloned()
            .ok_or(ServiceError::NotFound)?;
        if descriptor.version() != request.version() || descriptor.contract() != request.contract()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let metadata = job_receipt_metadata(request)?;
        let permit = match self.jobs.begin_start(request, context).await? {
            JobStartAdmission::Existing(result) => return Ok(Some(result)),
            JobStartAdmission::Execute(permit) => permit,
        };
        let (admission, owner) = match self.prepare_job(request, context).await {
            Ok(prepared) => prepared,
            Err(error) => {
                self.jobs.reject_start(&permit).await?;
                return Err(error);
            }
        };
        let retained = admission.clone();
        match self.jobs.start(admission, &permit, context, metadata).await {
            Ok(result) => Ok(Some(result)),
            Err(error) => {
                if self.jobs.reject_start(&permit).await? {
                    self.revoke(owner, &retained);
                }
                Err(error)
            }
        }
    }

    // The operation match retains the largest admission future. Keep that state behind one
    // heap boundary so start_job and its transport caller do not embed additional copies while
    // polling a selected branch on an ordinary runtime worker stack.
    fn prepare_job<'a>(
        &'a self,
        request: &'a TypedToolRequest,
        context: &'a RequestContext,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        (crate::application::job::JobAdmission, JobAdmissionOwner),
                        ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let captured_at =
                super::runtime::current_timestamp().map_err(|_error| ServiceError::Unavailable)?;
            let limits = context.limits();
            let (admission, revoke) = match request.name() {
                PREPARE => {
                    let admission = self
                        .investment_evidence
                        .as_ref()
                        .ok_or(ServiceError::Unavailable)?
                        .admit(request, context)
                        .await?;
                    (admission, JobAdmissionOwner::InvestmentEvidence)
                }
                START_FINANCIALS => {
                    let input: FinancialPreparationStart = decode(request.arguments())?;
                    let runner = self
                        .runners
                        .investment_financials()
                        .ok_or(ServiceError::Unavailable)?;
                    let admission = runner
                        .admit(
                            &input.selection_token,
                            limits,
                            captured_at,
                            context.deadline(),
                            context.cancellation(),
                        )
                        .await
                        .map_err(map_research_admission)?;
                    (admission, JobAdmissionOwner::InvestmentFinancials)
                }
                START_HISTORY => {
                    use crate::application::market_selection::product::{
                        MarketProductSelectionReadCapability, product_market_identities,
                        resolve_token,
                    };
                    let input: HistoryPreparationStart = decode(request.arguments())?;
                    let lookback =
                        market_squawk_adapter_alpaca::AlpacaHistoricalLookback::try_from_days(
                            input.lookback_days,
                        )
                        .map_err(|_| ServiceError::InvalidRequest)?;
                    let selection = MarketProductSelectionReadCapability::new(
                        Arc::clone(&self.profile_research),
                        self.profile_research.market_data_instruments(),
                    );
                    let records = selection
                        .population(captured_at, context.deadline(), context.cancellation())
                        .await?;
                    let identities = product_market_identities(&records, captured_at, None)?;
                    let instrument_id =
                        resolve_token(&identities, &input.history_token, |identity| {
                            identity.history_token()
                        })?;
                    let instrument = records
                        .into_iter()
                        .find(|record| record.definition().instrument_id() == instrument_id)
                        .ok_or(ServiceError::InvalidResult)?;
                    ensure_live(context)?;
                    let admission = self
                        .runners
                        .market_history()
                        .admit(
                            instrument,
                            input.history_token,
                            lookback,
                            limits,
                            captured_at,
                        )
                        .map_err(map_research_admission)?;
                    (admission, JobAdmissionOwner::MarketHistory)
                }
                START_INGEST => {
                    let terminal = self.terminal_request(request, "Research.IngestSource")?;
                    let admission = self
                        .runners
                        .ingest()
                        .admit(terminal, limits, captured_at)
                        .map_err(map_research_admission)?;
                    (admission, JobAdmissionOwner::Ingest)
                }
                START_EXPORT => {
                    let terminal = self.terminal_request(request, "Research.GetHistory")?;
                    let admission = self
                        .runners
                        .export()
                        .admit(terminal, limits, captured_at)
                        .map_err(map_research_admission)?;
                    (admission, JobAdmissionOwner::Export)
                }
                START_DATASET | START_FEATURE_DATASET => {
                    let registration = request
                        .arguments()
                        .get("registration")
                        .and_then(serde_json::Value::as_object)
                        .ok_or(ServiceError::InvalidRequest)?;
                    let build = admit_inline_phase_one_derived_generation_request(registration)
                        .map_err(map_phase_one_derived_generation_admission)?;
                    if request.name() == START_DATASET {
                        let admission = self
                            .runners
                            .research_phase_one_derived_generation()
                            .admit(build, captured_at)
                            .map_err(map_research_admission)?;
                        (admission, JobAdmissionOwner::ResearchPhaseOneGeneration)
                    } else {
                        let admission = self
                            .runners
                            .analysis_phase_one_feature_derived_generation()
                            .admit(build, captured_at)
                            .map_err(map_research_admission)?;
                        (
                            admission,
                            JobAdmissionOwner::AnalysisPhaseOneFeatureGeneration,
                        )
                    }
                }
                START_RECOMMENDATION_BACKTEST => {
                    let runner = self.runners.training().ok_or(ServiceError::Unavailable)?;
                    let (setup, catalog) =
                        self.recommendation_setup.resolve_for_analysis(context)?;
                    let prepared = self
                        .historical_study
                        .prepare_study(
                            runner,
                            &self.forecast_preparation,
                            request,
                            setup.selected_account().account_id(),
                            market_squawk_valuation::ActorId::try_from(
                                "installed-investment-analysis",
                            )
                            .map_err(|_| ServiceError::Internal)?,
                            context,
                        )
                        .await?;
                    self.recommendation_setup
                        .recheck_for_analysis(&setup, &catalog, context)?;
                    let (study, issuer) = prepared.into_parts();
                    let admission = self
                        .runners
                        .backtest()
                        .admit_recommendation(
                            study,
                            issuer,
                            captured_at,
                            context.limits(),
                            self.historical_study
                                .fiscal_reader()
                                .ok_or(ServiceError::Unavailable)?,
                        )
                        .map_err(map_backtest_admission)?;
                    (admission, JobAdmissionOwner::Backtest)
                }
                START_PROBABILITY_DATASET => {
                    let setup = match self.recommendation_setup.resolve_for_analysis(context) {
                        Ok(value) => Some(value),
                        Err(ServiceError::Unavailable | ServiceError::NotFound) => None,
                        Err(error) => return Err(error),
                    };
                    let prepared = self
                        .probability_preparation
                        .prepare_dataset(
                            &self.forecast_preparation,
                            request,
                            setup
                                .as_ref()
                                .map(|(setup, _)| setup.selected_account().account_id()),
                            context,
                        )
                        .await?;
                    if let Some((setup, catalog)) = &setup {
                        self.recommendation_setup
                            .recheck_for_analysis(setup, catalog, context)?;
                    }
                    let admission = self
                        .runners
                        .analysis_phase_one_feature_derived_generation()
                        .admit_prepared(prepared, captured_at)
                        .map_err(map_research_admission)?;
                    (
                        admission,
                        JobAdmissionOwner::AnalysisPhaseOneFeatureGeneration,
                    )
                }
                START_FISCAL_DATASET_BUILD => {
                    let prepared = self
                        .forecast_preparation
                        .prepare_fiscal_dataset(
                            self.dataset_preparation.authority().as_ref(),
                            request,
                            context,
                        )
                        .await?;
                    let admission = self
                        .runners
                        .analysis_phase_one_feature_derived_generation()
                        .admit_prepared(prepared, captured_at)
                        .map_err(map_research_admission)?;
                    (
                        admission,
                        JobAdmissionOwner::AnalysisPhaseOneFeatureGeneration,
                    )
                }
                START_HISTORICAL_STUDY_DATASET => {
                    let prepared = self
                        .historical_study
                        .prepare_dataset(&self.forecast_preparation, request, context)
                        .await?;
                    let admission = self
                        .runners
                        .analysis_phase_one_feature_derived_generation()
                        .admit_prepared(prepared, captured_at)
                        .map_err(map_research_admission)?;
                    (
                        admission,
                        JobAdmissionOwner::AnalysisPhaseOneFeatureGeneration,
                    )
                }
                START_HISTORICAL_STUDY_TRAINING => {
                    let runner = self.runners.training().ok_or(ServiceError::Unavailable)?;
                    let prepared = self
                        .historical_study
                        .prepare_training(runner, &self.forecast_preparation, request, context)
                        .await?;
                    let admission = runner
                        .admit_prepared(prepared, captured_at)
                        .map_err(map_training_admission)?;
                    (admission, JobAdmissionOwner::Training)
                }
                START_INVESTMENT_DATASET => {
                    let prepared = self
                        .training_preparation
                        .prepare_dataset(&self.dataset_preparation, self.runtime, request, context)
                        .await?;
                    let admission = self
                        .runners
                        .analysis_phase_one_feature_derived_generation()
                        .admit_prepared(prepared, captured_at)
                        .map_err(map_research_admission)?;
                    (
                        admission,
                        JobAdmissionOwner::AnalysisPhaseOneFeatureGeneration,
                    )
                }
                current_find::START_DATASET => {
                    let prepared = self
                        .current_find
                        .prepare_dataset(
                            &self.dataset_preparation,
                            request,
                            context,
                            &self.forecast_preparation,
                        )
                        .await?;
                    let admission = self
                        .runners
                        .analysis_phase_one_feature_derived_generation()
                        .admit_prepared(prepared, captured_at)
                        .map_err(map_research_admission)?;
                    (
                        admission,
                        JobAdmissionOwner::AnalysisPhaseOneFeatureGeneration,
                    )
                }
                current_find::START_SCREEN => {
                    let prepared = self
                        .current_find
                        .prepare_screen(
                            &self.dataset_preparation,
                            &self.training_preparation,
                            request,
                            context,
                            &self.forecast_preparation,
                            captured_at,
                        )
                        .await?;
                    let admission = self
                        .runners
                        .screen()
                        .admit(crate::jobs::ScreenJobCommand::new(prepared), captured_at)
                        .map_err(map_screen_admission)?;
                    (admission, JobAdmissionOwner::Screen)
                }
                START_PREPARED_FEATURE_DATASET => {
                    let input: PreparedFeatureDatasetStart = decode(request.arguments())?;
                    let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
                    let workspace = WorkspaceRuntimeIdentity::try_from_runtime(self.runtime)
                        .map_err(|_error| ServiceError::Unavailable)?;
                    let prepared = self
                        .dataset_preparation
                        .consume(
                            input.receipt,
                            origin,
                            workspace,
                            Instant::now(),
                            context.deadline(),
                            context.cancellation(),
                        )
                        .map_err(ServiceError::from)?;
                    let admission = self
                        .runners
                        .analysis_phase_one_feature_derived_generation()
                        .admit_prepared(prepared, captured_at)
                        .map_err(map_research_admission)?;
                    (
                        admission,
                        JobAdmissionOwner::AnalysisPhaseOneFeatureGeneration,
                    )
                }
                START_SCENARIO => {
                    let terminal = self.terminal_request(request, "Analysis.GetScenarios")?;
                    let admission = self
                        .runners
                        .scenario()
                        .admit(terminal, limits, captured_at)
                        .map_err(map_research_admission)?;
                    (admission, JobAdmissionOwner::Scenario)
                }
                START_BACKTEST => {
                    let registration = request
                        .arguments()
                        .get("registration")
                        .and_then(serde_json::Value::as_object)
                        .ok_or(ServiceError::InvalidRequest)?;
                    let admission = self
                        .runners
                        .backtest()
                        .admit_registration(
                            self.runners.backtest_registrar().as_ref(),
                            registration,
                            context.cancellation().clone(),
                            context.deadline(),
                            captured_at,
                        )
                        .await
                        .map_err(map_backtest_admission)?;
                    (admission, JobAdmissionOwner::Backtest)
                }
                START_PREPARED_BACKTEST => {
                    let input = self.backtest_preparation.consume(request, context).await?;
                    let admission = self
                        .runners
                        .backtest()
                        .admit_prepared(
                            self.runners.backtest_registrar().as_ref(),
                            input,
                            context.cancellation().clone(),
                            context.deadline(),
                            captured_at,
                        )
                        .await
                        .map_err(map_backtest_admission)?;
                    (admission, JobAdmissionOwner::Backtest)
                }
                START_PREPARED_TRAINING => {
                    let runner = self.runners.training().ok_or(ServiceError::Unavailable)?;
                    let prepared = self
                        .training_preparation
                        .prepare(runner, request, context)
                        .await?;
                    let admission = runner
                        .admit_prepared(prepared, captured_at)
                        .map_err(map_training_admission)?;
                    (admission, JobAdmissionOwner::Training)
                }
                START_TRAINING => {
                    let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
                    let client = ClientId::try_from_uuid(origin.client_id())
                        .map_err(|_error| ServiceError::Unauthorized)?;
                    let config = self.claim_input(
                        request,
                        "configTicketId",
                        client,
                        TRAINING_CONFIG_MEDIA_TYPE,
                        captured_at,
                    )?;
                    let authority = self.claim_input(
                        request,
                        "authorityTicketId",
                        client,
                        TRAINING_AUTHORITY_MEDIA_TYPE,
                        captured_at,
                    )?;
                    let runner = self.runners.training().ok_or(ServiceError::Unavailable)?;
                    let admission = runner
                        .admit_staged(config, authority, captured_at)
                        .map_err(map_training_admission)?;
                    (admission, JobAdmissionOwner::Training)
                }
                START_PREPARED_FORECAST | START_FISCAL_FORECAST => {
                    let terminal = if request.name() == START_FISCAL_FORECAST {
                        self.forecast_preparation
                            .prepare_fiscal_forecast(
                                self.dataset_preparation.authority().as_ref(),
                                &self.training_preparation,
                                self.runners.training().ok_or(ServiceError::Unavailable)?,
                                request,
                                context,
                            )
                            .await?
                    } else {
                        self.forecast_preparation.consume(request, context).await?
                    };
                    let admission = self
                        .runners
                        .forecast()
                        .admit(
                            terminal,
                            limits,
                            captured_at,
                            context.cancellation().clone(),
                            context.deadline(),
                        )
                        .await
                        .map_err(map_research_admission)?;
                    (admission, JobAdmissionOwner::Forecast)
                }
                RUN_SCREEN => {
                    let prepared = self
                        .decisions
                        .prepare_screen_job(request, context, captured_at)
                        .await?;
                    let admission = self
                        .runners
                        .screen()
                        .admit(crate::jobs::ScreenJobCommand::new(prepared), captured_at)
                        .map_err(map_screen_admission)?;
                    (admission, JobAdmissionOwner::Screen)
                }
                _ => return Err(ServiceError::InvalidRequest),
            };
            Ok((admission, revoke))
        })
    }

    async fn start_research_file_import_job(
        &self,
        prepared: &mut super::research_file_import::PreparedStart,
        context: &RequestContext,
    ) -> Result<(TypedToolResult, bool), ServiceError> {
        ensure_live(context)?;
        let start = prepared.job_start();
        let job_id =
            JobId::try_from_str(start.job_id()).map_err(|_error| ServiceError::Internal)?;
        let generation = JobGeneration::try_new(1).map_err(|_error| ServiceError::Internal)?;
        let admitted_at = Timestamp::from_unix_nanos(start.admitted_at_unix_nanos());
        let request_id = RequestId::try_string(start.request_id().to_owned())
            .map_err(|_error| ServiceError::Internal)?;
        let workspace = SourceIdentifier::try_from(prepared.workspace_id())
            .map_err(|_error| ServiceError::Internal)?;
        let client = SourceIdentifier::try_from(prepared.client_id())
            .map_err(|_error| ServiceError::Internal)?;
        let origin = JobOrigin::new(workspace, client);

        match self
            .research_file_job_repository
            .get(job_id, generation)
            .await
        {
            Ok(snapshot) => {
                prepared.mark_job_admission_may_exist();
                validate_research_file_job_binding(
                    &snapshot,
                    job_id,
                    &origin,
                    &request_id,
                    admitted_at,
                )?;
                ensure_live(context)?;
                return Ok((prepared.queued_result(), true));
            }
            Err(JobRepositoryError::NotFound) => {}
            Err(_error) => {
                prepared.mark_job_admission_may_exist();
                return Err(ServiceError::Unavailable);
            }
        }

        let terminal = self.terminal_request(prepared.request(), "Research.IngestSource")?;
        let admission = self
            .runners
            .ingest()
            .admit(terminal, context.limits(), admitted_at)
            .map_err(map_research_admission)?;
        let spec = admission
            .clone()
            .into_spec(job_id, origin.clone(), request_id.clone(), admitted_at)
            .map_err(|_error| ServiceError::Internal)?;
        prepared.mark_job_admission_may_exist();
        match self.research_file_job_authority.start(&spec).await {
            Ok(snapshot) => {
                if snapshot.spec() != &spec
                    || snapshot.state() != JobState::Queued
                    || snapshot.sequence().get() != 0
                {
                    return Err(ServiceError::InvalidResult);
                }
                ensure_live(context)?;
                Ok((prepared.queued_result(), false))
            }
            Err(error) => {
                match self
                    .research_file_job_repository
                    .get(job_id, generation)
                    .await
                {
                    Ok(snapshot) => {
                        if snapshot.spec() != &spec {
                            return Err(ServiceError::InvalidResult);
                        }
                        Ok((prepared.queued_result(), false))
                    }
                    Err(JobRepositoryError::NotFound) => {
                        self.revoke(JobAdmissionOwner::Ingest, &admission);
                        prepared.mark_job_not_admitted();
                        Err(map_research_file_job_authority(error))
                    }
                    Err(_error) => Err(ServiceError::Unavailable),
                }
            }
        }
    }

    fn claim_input(
        &self,
        request: &TypedToolRequest,
        argument: &str,
        client: ClientId,
        media_type: &str,
        now: market_squawk_domain::Timestamp,
    ) -> Result<market_squawk_runtime::ClaimedInput, ServiceError> {
        let id = request
            .arguments()
            .get(argument)
            .and_then(serde_json::Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            .and_then(|value| InputTicketId::try_from_uuid(value).ok())
            .ok_or(ServiceError::InvalidRequest)?;
        let media_type = market_squawk_domain::SourceIdentifier::try_from(media_type)
            .map_err(|_error| ServiceError::Unavailable)?;
        self.inputs
            .claim(id, client, &media_type, now)
            .map_err(|_error| ServiceError::Unauthorized)
    }

    fn terminal_request(
        &self,
        start: &TypedToolRequest,
        terminal_name: &str,
    ) -> Result<TypedToolRequest, ServiceError> {
        let terminal = self
            .application
            .capabilities()
            .find(terminal_name)
            .cloned()
            .ok_or(ServiceError::Unavailable)?;
        crate::application::job::terminal_request_for_start(
            start,
            start.name(),
            &terminal,
            terminal_name,
        )
        .map_err(|_error| ServiceError::InvalidRequest)
    }

    fn revoke(&self, owner: JobAdmissionOwner, admission: &crate::application::job::JobAdmission) {
        match owner {
            JobAdmissionOwner::InvestmentEvidence => {
                if let Some(runner) = &self.investment_evidence {
                    let _result = runner.revoke(admission);
                }
            }
            JobAdmissionOwner::InvestmentFinancials => {
                if let Some(runner) = self.runners.investment_financials() {
                    let _result = runner.revoke(admission);
                }
            }
            JobAdmissionOwner::MarketHistory => {
                let _result = self.runners.market_history().revoke(admission);
            }
            JobAdmissionOwner::Ingest => {
                let _result = self.runners.ingest().revoke(admission);
            }
            JobAdmissionOwner::Export => {
                let _result = self.runners.export().revoke(admission);
            }
            JobAdmissionOwner::ResearchPhaseOneGeneration => {
                let _result = self
                    .runners
                    .research_phase_one_derived_generation()
                    .revoke(admission);
            }
            JobAdmissionOwner::AnalysisPhaseOneFeatureGeneration => {
                let _result = self
                    .runners
                    .analysis_phase_one_feature_derived_generation()
                    .revoke(admission);
            }
            JobAdmissionOwner::Scenario => {
                let _result = self.runners.scenario().revoke(admission);
            }
            JobAdmissionOwner::Backtest => {
                let _result = self.runners.backtest().revoke(admission);
            }
            JobAdmissionOwner::Training => {
                if let Some(runner) = self.runners.training() {
                    let _result = runner.revoke(admission);
                }
            }
            JobAdmissionOwner::Forecast => {
                let _result = self.runners.forecast().revoke(admission);
            }
            JobAdmissionOwner::Screen => {
                let _result = self.runners.screen().revoke(admission);
            }
        }
    }
}

fn research_file_import_requires_restart(
    view: &crate::application::job::JobView,
) -> Result<bool, ServiceError> {
    match view.state() {
        JobState::Completed | JobState::Cancelled => Ok(false),
        JobState::Queued | JobState::Preparing | JobState::Running | JobState::Recovering => {
            Ok(true)
        }
        JobState::Interrupted => Ok(!view.cancellation_requested()),
        JobState::Failed => Ok(view.failure().is_some_and(|failure| {
            failure.class().as_str() == "recovery"
                && failure.diagnostic().as_str() == "runner-recovery-failed"
        })),
        JobState::AwaitingConfirmation | JobState::Cancelling => Err(ServiceError::InvalidResult),
    }
}

fn validate_research_file_job_binding(
    snapshot: &JobSnapshot,
    job_id: JobId,
    origin: &JobOrigin,
    request_id: &RequestId,
    admitted_at: Timestamp,
) -> Result<(), ServiceError> {
    let spec = snapshot.spec();
    if snapshot.id() != job_id
        || snapshot.generation().get() != 1
        || spec.id() != job_id
        || spec.generation().get() != 1
        || spec.kind().as_str() != RESEARCH_INGEST_JOB_KIND
        || spec.origin() != origin
        || spec.request_id() != request_id
        || spec.input().authority().as_str() != RESEARCH_INGEST_INPUT_AUTHORITY
        || spec.authority().authority().as_str() != RESEARCH_INGEST_RESULT_AUTHORITY
        || spec.authority().identity().as_str() != RESEARCH_INGEST_RESULT_AUTHORITY
        || spec.authority().captured_at() != admitted_at
        || spec.attempt_limit().get() != 1
        || spec.admitted_at() != admitted_at
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}

fn map_research_file_job_authority(error: JobAuthorityError) -> ServiceError {
    match error {
        JobAuthorityError::Capacity => ServiceError::ResourceExhausted,
        JobAuthorityError::UnknownKind
        | JobAuthorityError::Repository
        | JobAuthorityError::Contract
        | JobAuthorityError::ShutdownIncomplete => ServiceError::Unavailable,
    }
}

pub(super) fn job_receipt_metadata(
    request: &TypedToolRequest,
) -> Result<ToolResultMetadata, ServiceError> {
    if request.name() != START_INGEST {
        return Ok(ToolResultMetadata::complete_not_applicable());
    }
    let provider = required_argument(request, "provider")?;
    let dataset = required_argument(request, "dataset")?;
    let object = required_argument(request, "object")?;
    ToolResultMetadata::try_complete(
        serde_json::json!({
            "provider": provider,
            "dataset": dataset,
        }),
        serde_json::json!({
            "sourceObject": object,
            "discoveryReceiptBound": true,
            "executionEligible": false,
        }),
    )
    .map_err(Into::into)
}

fn required_argument<'a>(
    request: &'a TypedToolRequest,
    name: &str,
) -> Result<&'a str, ServiceError> {
    request
        .arguments()
        .get(name)
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
}

#[derive(Clone, Copy)]
enum JobAdmissionOwner {
    InvestmentEvidence,
    MarketHistory,
    InvestmentFinancials,
    Ingest,
    Export,
    ResearchPhaseOneGeneration,
    AnalysisPhaseOneFeatureGeneration,
    Scenario,
    Backtest,
    Training,
    Forecast,
    Screen,
}

impl std::fmt::Debug for InstalledToolServices {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledToolServices")
            .field("application", &"[APPLICATION AUTHORITY]")
            .field("jobs", &self.jobs)
            .field("runners", &self.runners)
            .field("inputs", &"[ONE-SHOT NATIVE INPUT STAGER]")
            .field("runtime", &self.runtime)
            .field(
                "dataset_preparation",
                &"[ONE-USE DATASET PREPARATION AUTHORITY]",
            )
            .field(
                "backtest_preparation",
                &"[ONE-USE BACKTEST PREPARATION AUTHORITY]",
            )
            .field(
                "forecast_preparation",
                &"[ONE-USE FORECAST PREPARATION AUTHORITY]",
            )
            .field("analysis", &self.analysis)
            .field("decisions", &"[DURABLE DECISION AUTHORITY]")
            .field("operations", &self.operations)
            .field("portfolio_analysis", &self.portfolio_analysis)
            .field("portfolio_import", &self.portfolio_import)
            .field("recommendation_setup", &self.recommendation_setup)
            .field(
                "provider_credential_import",
                &self.provider_credential_import,
            )
            .field("research_file_import", &self.research_file_import)
            .finish()
    }
}

#[async_trait]
impl ToolServices for InstalledToolServices {
    fn capabilities(&self) -> ServiceCapabilities {
        self.product_capabilities.clone()
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        self.select_call(request, context).await
    }
}

impl InstalledToolServices {
    // Construct only the selected heap-owned operation here. This synchronous frame returns
    // before its future is polled, so its large construction temporaries do not stay on the
    // worker stack throughout nested provider I/O or other application operations.
    #[inline(never)]
    fn select_call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Pin<Box<dyn Future<Output = Result<TypedToolResult, ServiceError>> + Send + '_>> {
        if super::analytical_workflow::owns(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result =
                    super::analytical_workflow::call(&self.analytical_workflow, &request, &context)
                        .await?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                Ok(result)
            });
        }
        if InstalledResearchFileImportOperations::owns_commit(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                return match self
                    .research_file_import
                    .prepare_commit(&request, &context)
                    .await?
                {
                    PreparedResearchFileCommit::Existing(result) => {
                        result
                            .validate_for(&descriptor)
                            .map_err(ServiceError::from)?;
                        Ok(result)
                    }
                    PreparedResearchFileCommit::Ready(mut prepared) => {
                        let (result, _reconciled_existing) = self
                            .start_research_file_import_job(&mut prepared, &context)
                            .await?;
                        result
                            .validate_for(&descriptor)
                            .map_err(ServiceError::from)?;
                        self.research_file_import
                            .complete_commit(prepared.preview_id(), &result)?;
                        Ok(result)
                    }
                };
            });
        }
        if owns_job_start(request.name()) {
            return Box::pin(async move {
                let result = self
                    .start_job(&request, &context)
                    .await?
                    .ok_or(ServiceError::Internal)?;
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                Ok(result)
            });
        }
        if InstalledJobOperations::owns(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = self.jobs.call(&request, &context, &self.runners).await?;
                result
                    .validate_against(context.limits())
                    .map_err(ServiceError::from)?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if matches!(
            request.name(),
            GET_HISTORICAL_STUDY_PLAN
                | COMPLETE_HISTORICAL_STUDY_FISCAL_PAGE
                | super::jobs::GET_RECOMMENDATION_BACKTEST_JOB_RESULT
        ) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                ensure_live(&context)?;
                let result = if request.name() == GET_HISTORICAL_STUDY_PLAN {
                    self.historical_study
                        .plan(&self.forecast_preparation, &request, &context)
                        .await?
                } else if request.name() == super::jobs::GET_RECOMMENDATION_BACKTEST_JOB_RESULT {
                    self.jobs
                        .read_recommendation_backtest_result(
                            self.runners.backtest(),
                            self.historical_reader
                                .as_deref()
                                .ok_or(ServiceError::Unavailable)?,
                            &request,
                            &context,
                        )
                        .await?
                } else {
                    let runner = self.runners.training().ok_or(ServiceError::Unavailable)?;
                    self.historical_study
                        .complete_fiscal_page(
                            runner,
                            &self.forecast_preparation,
                            &request,
                            &context,
                        )
                        .await?
                };
                ensure_live(&context)?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if request.name() == LIST_PRODUCT_ACTIVITY {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                ensure_live(&context)?;
                let (views, next_cursor) = self
                    .jobs
                    .product_activity_page(&request, &context, true)
                    .await?;
                let activities = views
                    .iter()
                    .map(|view| product_model_activity(view).ok_or(ServiceError::InvalidResult))
                    .collect::<Result<Vec<_>, _>>()?;
                let result = TypedToolResult::try_new(
                    serde_json::json!({"activities": activities,"nextCursor":next_cursor}),
                    activities.len(),
                    ToolResultMetadata::complete_not_applicable(),
                    context.limits(),
                )
                .map_err(ServiceError::from)?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if matches!(
            request.name(),
            GET_FEATURE_DATASET_PREPARATION | PREVIEW_FEATURE_DATASET
        ) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                ensure_live(&context)?;
                let (content, item_count) = match request.name() {
                    GET_FEATURE_DATASET_PREPARATION => {
                        let options = self
                            .dataset_preparation
                            .options(context.deadline(), context.cancellation().clone())
                            .await
                            .map_err(|error| {
                                tracing::warn!(
                                    operation = GET_FEATURE_DATASET_PREPARATION,
                                    error = ?error,
                                    "guided feature-dataset preparation failed"
                                );
                                ServiceError::from(error)
                            })?;
                        let item_count = options.datasets.len();
                        (encode(&options)?, item_count)
                    }
                    PREVIEW_FEATURE_DATASET => {
                        let selection: DatasetPreparationSelection = decode(request.arguments())?;
                        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
                        let workspace = WorkspaceRuntimeIdentity::try_from_runtime(self.runtime)
                            .map_err(|_error| ServiceError::Unavailable)?;
                        let observed_at = super::runtime::current_timestamp()
                            .map_err(|_error| ServiceError::Unavailable)?;
                        let preview = self
                            .dataset_preparation
                            .preview(DatasetPreparationPreviewRequest {
                                selection,
                                origin,
                                workspace,
                                now: Instant::now(),
                                observed_at,
                                deadline: context.deadline(),
                                cancellation: context.cancellation().clone(),
                            })
                            .await
                            .map_err(ServiceError::from)?;
                        (encode(&preview)?, 1)
                    }
                    _ => return Err(ServiceError::NotFound),
                };
                ensure_live(&context)?;
                let result = TypedToolResult::try_new(
                    content,
                    item_count,
                    ToolResultMetadata::complete_not_applicable(),
                    context.limits(),
                )
                .map_err(ServiceError::from)?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if super::analytical_profile::owns(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = super::analytical_profile::call(
                    &request,
                    &context,
                    &self.forecast_preparation,
                    &self.profile_benchmarks,
                    &self.profile_research,
                )
                .await?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if request.name() == PREPARE_PROBABILITY_EVENT {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let setup = match self.recommendation_setup.resolve_for_analysis(&context) {
                    Ok(value) => Some(value),
                    Err(ServiceError::Unavailable | ServiceError::NotFound) => None,
                    Err(error) => return Err(error),
                };
                let result = self
                    .probability_preparation
                    .prepare(
                        &self.forecast_preparation,
                        &request,
                        setup
                            .as_ref()
                            .map(|(setup, _)| setup.selected_account().account_id()),
                        &context,
                    )
                    .await?;
                if let Some((setup, catalog)) = &setup {
                    self.recommendation_setup
                        .recheck_for_analysis(setup, catalog, &context)?;
                }
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                Ok(result)
            });
        }
        if matches!(request.name(), GET_DATASET_RESULT | GET_TRAINING_RESULT) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = if request.name() == GET_DATASET_RESULT {
                    self.training_preparation
                        .read_dataset_result(&request, &context)
                        .await?
                } else {
                    let runner = self.runners.training().ok_or(ServiceError::Unavailable)?;
                    self.training_preparation
                        .read_result(runner, &request, &context)
                        .await?
                };
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if request.name() == "Model.GetForecastJobResult" {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = self
                    .jobs
                    .read_forecast_result(self.runners.forecast(), &request, &context)
                    .await?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if InstalledForecastPreparation::owns(request.name())
            || request.name() == GET_FISCAL_PREPARATION_PLAN
        {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = if request.name() == GET_FISCAL_PREPARATION_PLAN {
                    self.forecast_preparation
                        .fiscal_plan(
                            self.dataset_preparation.authority().as_ref(),
                            &request,
                            &context,
                        )
                        .await?
                } else {
                    self.forecast_preparation.call(&request, &context).await?
                };
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if InstalledRecommendationBacktestReadOperations::owns(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = self
                    .recommendation_backtest
                    .call(&request, &context)
                    .await?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if super::market_session_context::owns(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = super::market_session_context::call(
                    &request,
                    &context,
                    &self.market_session_runtime,
                    &self.market_session_reader,
                )
                .await?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if InstalledMarketEvidence::owns(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let models = self
                    .forecast_preparation
                    .financial_profile_catalog_for_request(&request, &context)
                    .await?;
                let result = self
                    .market_evidence
                    .as_ref()
                    .ok_or(ServiceError::Unavailable)?
                    .call(&request, &context, models.as_ref())
                    .await?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if InstalledBacktestPreparation::owns(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = self
                    .backtest_preparation
                    .call(&request, &context, &self.jobs)
                    .await?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if InstalledCurrentFind::owns(request.name())
            || matches!(request.name(), find_results::PUBLISH | find_results::GET)
        {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = if matches!(request.name(), find_results::PUBLISH | find_results::GET)
                {
                    self.current_find
                        .call_find_results(&request, &context, &self.training_preparation)
                        .await?
                } else {
                    self.current_find
                        .call(
                            &self.dataset_preparation,
                            &self.training_preparation,
                            &request,
                            &context,
                            &self.forecast_preparation,
                        )
                        .await?
                };
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if InstalledDecisionOperations::owns(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = if request.name() == GENERATE_INVESTMENT_ANALYSIS {
                    self.decisions
                        .generate_investment_analysis(
                            &request,
                            &context,
                            &self.jobs,
                            self.runners.forecast(),
                            &self.portfolio_analysis,
                            &self.forecast_preparation,
                        )
                        .await?
                } else {
                    self.decisions.call(&request, &context).await?
                };
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if InstalledPortfolioAnalysis::owns(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let models = self
                    .forecast_preparation
                    .financial_profile_catalog_for_request(&request, &context)
                    .await?;
                let result = self
                    .portfolio_analysis
                    .call(&request, &context, models.as_ref())
                    .await?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if InstalledPortfolioImportOperations::owns(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = self.portfolio_import.call(request, context).await?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if InstalledRecommendationSetupOperations::owns(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = self.recommendation_setup.call(&request, &context)?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if request.name() == IMPORT_PROVIDER_CREDENTIAL_BUNDLE {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = self
                    .provider_credential_import
                    .call(&request, &context)
                    .await?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if InstalledResearchFileImportOperations::owns_direct(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = self.research_file_import.call(request, context).await?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if InstalledAnalysisOperations::owns(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = self.analysis.call(&request, &context).await?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if InstalledOperations::owns(request.name()) {
            return Box::pin(async move {
                let descriptor = self
                    .application
                    .capabilities()
                    .find(request.name())
                    .cloned()
                    .ok_or(ServiceError::NotFound)?;
                if descriptor.version() != request.version()
                    || descriptor.contract() != request.contract()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let result = self.operations.call(&request, &context).await?;
                result
                    .validate_for(&descriptor)
                    .map_err(ServiceError::from)?;
                return Ok(result);
            });
        }
        if request.name() == "Model.PrepareForecastOutcome" {
            return Box::pin(async move {
                // The original model owner first validates the saved token, maturity, authority,
                // and existing canonical label. Only a genuine mature catalog miss can acquire.
                let original = self
                    .application
                    .call(request.clone(), context.clone())
                    .await?;
                let value = original.structured_content();
                if value.get("state").and_then(Value::as_str) != Some("unavailable")
                    || value.get("reason").and_then(Value::as_str) != Some("source_unavailable")
                {
                    return Ok(original);
                }
                let token = request
                    .arguments()
                    .get("forecastToken")
                    .and_then(Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok())
                    .ok_or(ServiceError::InvalidRequest)?;
                let setup = match self.recommendation_setup.resolve_for_analysis(&context) {
                    Ok(value) => Some(value),
                    Err(ServiceError::Unavailable | ServiceError::NotFound) => None,
                    Err(error) => return Err(error),
                };
                let published = match self
                    .probability_preparation
                    .ensure_matured_event_publication(
                        token,
                        setup
                            .as_ref()
                            .map(|(setup, _)| setup.selected_account().account_id()),
                        &context,
                    )
                    .await
                {
                    Ok(value) => value,
                    Err(ServiceError::Unavailable | ServiceError::NotFound) => false,
                    Err(error) => return Err(error),
                };
                if let Some((setup, catalog)) = &setup {
                    self.recommendation_setup
                        .recheck_for_analysis(setup, catalog, &context)?;
                }
                if published {
                    self.application.call(request, context).await
                } else {
                    Ok(original)
                }
            });
        }
        self.application.call(request, context)
    }
}

fn owns_job_start(name: &str) -> bool {
    matches!(
        name,
        PREPARE
            | START_HISTORY
            | START_FINANCIALS
            | START_INGEST
            | START_EXPORT
            | START_DATASET
            | START_FEATURE_DATASET
            | START_PREPARED_FEATURE_DATASET
            | START_SCENARIO
            | START_BACKTEST
            | START_PREPARED_BACKTEST
            | START_TRAINING
            | START_PREPARED_TRAINING
            | START_INVESTMENT_DATASET
            | START_HISTORICAL_STUDY_DATASET
            | START_FISCAL_DATASET_BUILD
            | START_FISCAL_FORECAST
            | START_PROBABILITY_DATASET
            | START_HISTORICAL_STUDY_TRAINING
            | START_RECOMMENDATION_BACKTEST
            | START_PREPARED_FORECAST
            | RUN_SCREEN
            | current_find::START_DATASET
            | current_find::START_SCREEN
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FinancialPreparationStart {
    selection_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HistoryPreparationStart {
    history_token: String,
    lookback_days: u16,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PreparedFeatureDatasetStart {
    receipt: DatasetPreparationReceipt,
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(super::business_arguments(arguments)))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Value, ServiceError> {
    serde_json::to_value(value).map_err(|_error| ServiceError::Internal)
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

pub(super) fn map_research_admission(error: crate::jobs::ResearchJobRunnerError) -> ServiceError {
    match error {
        crate::jobs::ResearchJobRunnerError::InvalidLimits
        | crate::jobs::ResearchJobRunnerError::InvalidRequest
        | crate::jobs::ResearchJobRunnerError::Conflict => ServiceError::InvalidRequest,
        crate::jobs::ResearchJobRunnerError::Capacity => ServiceError::ResourceExhausted,
        crate::jobs::ResearchJobRunnerError::Cancelled => ServiceError::Cancelled,
        crate::jobs::ResearchJobRunnerError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        crate::jobs::ResearchJobRunnerError::Unavailable => ServiceError::Unavailable,
    }
}

fn map_backtest_admission(error: crate::jobs::BacktestJobRunnerError) -> ServiceError {
    match error {
        crate::jobs::BacktestJobRunnerError::InvalidLimits
        | crate::jobs::BacktestJobRunnerError::InvalidCommand
        | crate::jobs::BacktestJobRunnerError::Conflict => ServiceError::InvalidRequest,
        crate::jobs::BacktestJobRunnerError::Capacity => ServiceError::ResourceExhausted,
        crate::jobs::BacktestJobRunnerError::Unavailable => ServiceError::Unavailable,
    }
}

fn map_phase_one_derived_generation_admission(
    error: crate::local_product::CliDatasetError,
) -> ServiceError {
    match error {
        crate::local_product::CliDatasetError::InvalidRequest
        | crate::local_product::CliDatasetError::RequestJson
        | crate::local_product::CliDatasetError::ConfirmationRequired => {
            ServiceError::InvalidRequest
        }
        crate::local_product::CliDatasetError::RequestFile => ServiceError::Unauthorized,
        crate::local_product::CliDatasetError::PhaseOneDerivedGeneration(_)
        | crate::local_product::CliDatasetError::PhaseOneDescriptor(_) => ServiceError::Unavailable,
    }
}

pub(super) fn map_training_admission(error: crate::jobs::TrainingJobRunnerError) -> ServiceError {
    match error {
        crate::jobs::TrainingJobRunnerError::Runtime(error) => {
            crate::application::model::runtime_service_error(error)
        }
        crate::jobs::TrainingJobRunnerError::InvalidLimits
        | crate::jobs::TrainingJobRunnerError::InvalidInput
        | crate::jobs::TrainingJobRunnerError::InputChanged
        | crate::jobs::TrainingJobRunnerError::Conflict
        | crate::jobs::TrainingJobRunnerError::StagedInput(_) => ServiceError::InvalidRequest,
        crate::jobs::TrainingJobRunnerError::Capacity => ServiceError::ResourceExhausted,
        crate::jobs::TrainingJobRunnerError::Unavailable
        | crate::jobs::TrainingJobRunnerError::WorkerUnavailable
        | crate::jobs::TrainingJobRunnerError::StagingConflict
        | crate::jobs::TrainingJobRunnerError::InvalidCandidate
        | crate::jobs::TrainingJobRunnerError::Cleanup
        | crate::jobs::TrainingJobRunnerError::Input(_)
        | crate::jobs::TrainingJobRunnerError::Path
        | crate::jobs::TrainingJobRunnerError::Artifact
        | crate::jobs::TrainingJobRunnerError::Program(_) => ServiceError::Unavailable,
    }
}

fn map_screen_admission(error: crate::jobs::ScreenJobRunnerError) -> ServiceError {
    match error {
        crate::jobs::ScreenJobRunnerError::Conflict => ServiceError::InvalidRequest,
        crate::jobs::ScreenJobRunnerError::InvalidConfiguration => ServiceError::Unavailable,
    }
}
