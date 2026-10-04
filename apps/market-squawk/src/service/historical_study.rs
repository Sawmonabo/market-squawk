//! Installed preparation adapters over existing source, dataset, training and backtest owners.
//! No request carries a model vector, return, fold boundary, action record, or performance value.

use super::{InstalledProductTraining, forecast_preparation::InstalledForecastPreparation};
use crate::{
    LocalProduct, ResearchService,
    application::{
        /* Source-owned public facade; root serializes these narrow reexports. */
        DatasetPreparationAuthority, FiscalProjectionTarget, HISTORICAL_FISCAL_PAGE_SIZE,
        HistoricalFiscalCompletedJobs, HistoricalFiscalJobReference,
        HistoricalFiscalPageDescriptor, HistoricalFiscalPageReference,
        HistoricalFiscalStudyBinding, HistoricalFiscalUnavailableReference,
        MacroContextReadCapability, PreparedFeatureDatasetBuild, PreparedHistoricalFiscalDatasets,
        RecommendationBenchmarkSelectionReadCapability,
        analysis::{
            HistoricalRecommendationAlphaProducer, HistoricalStudyDatasetPartV1,
            HistoricalStudyPlanReadCapabilityV1, HistoricalStudyPlanReferenceV1,
            HistoricalStudyPlanV1, PreparedRecommendationStudyV1,
            ProductionGovernedBacktestInputAuthority, RecommendationStudyPreparationInputV1,
        },
        analytical_profile::{AnalyticalProfileResolution, ValidatedAnalyticalProfile},
        fair_value::{FairValueDomainService, HistoricalStudyValuationReadCapability},
        fiscal_projection_targets,
        market_calendar::CompletedMarketSessionReadCapability,
        model::runtime::ProductionModelRuntime,
        prepare_fixed_current_population,
    },
    jobs::{InstalledJobAuthority, PreparedProductTraining, TrainingJobRunner},
};
use market_squawk_data::{
    AnalyticalFeatureDataset, FeatureDatasetInputEpoch, FeatureDatasetInputEpochCursor,
    FeatureDatasetProductContract, OwnedFeatureDatasetInputCoordinate, QueryLimits, Sha256Digest,
};
use market_squawk_domain::{AccountId, InstrumentId, Timestamp};
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use market_squawk_valuation::ActorId;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub(super) const GET_HISTORICAL_STUDY_PLAN: &str = "Analysis.GetHistoricalStudyPlan";
pub(super) const START_HISTORICAL_STUDY_DATASET: &str = "Analysis.StartHistoricalStudyDataset";
pub(super) const START_HISTORICAL_STUDY_TRAINING: &str = "Model.StartHistoricalStudyTraining";
pub(super) const COMPLETE_HISTORICAL_STUDY_FISCAL_PAGE: &str =
    "Analysis.CompleteHistoricalStudyFiscalPage";
pub(super) const START_RECOMMENDATION_BACKTEST: &str = "Analysis.StartRecommendationBacktest";

pub(super) struct InstalledHistoricalStudy {
    plans: HistoricalStudyPlanReadCapabilityV1,
    datasets: Arc<DatasetPreparationAuthority>,
    macro_reader: MacroContextReadCapability,
    calendars: CompletedMarketSessionReadCapability,
    fiscal_reader: Option<Arc<crate::application::HistoricalFiscalForecastReadCapability>>,
    identities: Option<crate::application::InstrumentContextReadCapability>,
    training: InstalledProductTraining,
    research: Arc<ResearchService>,
    runtime: Option<Arc<ProductionModelRuntime>>,
    valuation: Arc<FairValueDomainService>,
    inputs: Arc<ProductionGovernedBacktestInputAuthority>,
}
impl InstalledHistoricalStudy {
    pub(super) fn fiscal_reader(
        &self,
    ) -> Option<Arc<crate::application::HistoricalFiscalForecastReadCapability>> {
        self.fiscal_reader.as_ref().map(Arc::clone)
    }

    pub(super) fn new(
        product: &LocalProduct,
        jobs: &InstalledJobAuthority,
        datasets: Arc<DatasetPreparationAuthority>,
    ) -> Self {
        let research = product.research();
        let fiscal_reader = product
            .instrument_context_read_capability()
            .zip(product.model_runtime())
            .map(|(identities, runtime)| {
                Arc::new(
                    crate::application::HistoricalFiscalForecastReadCapability::new(
                        Arc::clone(&research),
                        Arc::clone(&datasets),
                        Arc::new(identities),
                        runtime,
                        product.artifacts(),
                    ),
                )
            });
        Self {
            identities: product.instrument_context_read_capability(),
            plans: HistoricalStudyPlanReadCapabilityV1::new(
                &research,
                RecommendationBenchmarkSelectionReadCapability::new(
                    research.market_data_instruments(),
                ),
                Arc::clone(&datasets),
                crate::application::SourceAppliedCorporateActionReadCapability::new(
                    Arc::clone(&research),
                    CompletedMarketSessionReadCapability::new(
                        Arc::clone(&research),
                        product.market_runtime(),
                    ),
                ),
            ),
            datasets,
            fiscal_reader,
            macro_reader: product.macro_context_read_capability(),
            calendars: CompletedMarketSessionReadCapability::new(
                Arc::clone(&research),
                product.market_runtime(),
            ),
            training: InstalledProductTraining::new(product, jobs),
            runtime: product.model_runtime(),
            valuation: product.fair_value_service(),
            inputs: product.backtest_inputs(),
            research,
        }
    }
    async fn reopen(
        &self,
        reference: &HistoricalStudyPlanReferenceV1,
        forecasts: &InstalledForecastPreparation,
        context: &RequestContext,
    ) -> Result<HistoricalStudyPlanV1, ServiceError> {
        context.origin().ok_or(ServiceError::Unauthorized)?;
        let profile = forecasts
            .revalidate_profile(reference.financial_profile(), context)
            .await?;
        let cutoff = reference.source_cutoff()?;
        let identities = self.identities.as_ref().ok_or(ServiceError::Unavailable)?;
        let identity = identities
            .read(
                crate::application::InstrumentContextRequest::try_new(
                    reference.subject_instrument_id(),
                    cutoff,
                    cutoff,
                )
                .map_err(|_| ServiceError::InvalidRequest)?,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(super::market_evidence::map_identity_error)?;
        let crate::application::InstrumentContextOutcome::Exact(identity) = identity.outcome()
        else {
            return Err(ServiceError::Unavailable);
        };
        if !profile.admits_investment(identity.asset_class(), identity.exchange_traded_fund()) {
            return Err(ServiceError::InvalidRequest);
        }
        self.plans
            .reopen(reference, profile, context)
            .await?
            .ok_or(ServiceError::Unavailable)
    }
    pub(super) async fn plan(
        &self,
        forecasts: &InstalledForecastPreparation,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        context.origin().ok_or(ServiceError::Unauthorized)?;
        let input: PlanInput = decode(request)?;
        if input.fiscal_page.is_some()
            && (input.study_input_job.is_none()
                || input.page_ordinal.is_none()
                || input.price_example_id.is_some())
        {
            return Err(ServiceError::InvalidRequest);
        }
        let cutoff = input
            .source_cutoff_unix_nanos
            .parse::<i64>()
            .map_err(|_| ServiceError::InvalidRequest)?;
        if cutoff.to_string() != input.source_cutoff_unix_nanos {
            return Err(ServiceError::InvalidRequest);
        }
        let profile = forecasts
            .revalidate_profile(&input.financial_profile, context)
            .await?;
        let identities = self.identities.as_ref().ok_or(ServiceError::Unavailable)?;
        let identity = identities
            .read(
                crate::application::InstrumentContextRequest::try_new(
                    input.subject_instrument_id,
                    Timestamp::from_unix_nanos(cutoff),
                    Timestamp::from_unix_nanos(cutoff),
                )
                .map_err(|_| ServiceError::InvalidRequest)?,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(super::market_evidence::map_identity_error)?;
        let crate::application::InstrumentContextOutcome::Exact(identity) = identity.outcome()
        else {
            return Err(ServiceError::Unavailable);
        };
        if !profile.admits_investment(identity.asset_class(), identity.exchange_traded_fund()) {
            return Err(ServiceError::InvalidRequest);
        }
        let plan = self
            .plans
            .read(
                input.subject_instrument_id,
                Timestamp::from_unix_nanos(cutoff),
                profile,
                &input.source_action_reference,
                context,
            )
            .await?;
        let value = match plan {
            Some(plan) => {
                let fiscal = match (&input.study_input_job, &input.price_example_id) {
                    (None, None) if input.page_ordinal.is_none() => serde_json::Value::Null,
                    (None, None) => return Err(ServiceError::InvalidRequest),
                    (None, Some(_)) => return Err(ServiceError::InvalidRequest),
                    (Some(job), example) => {
                        let (_, prices) = self.open_price_inputs(&plan, job, context).await?;
                        let binding =
                            HistoricalFiscalStudyBinding::from_source(&plan, &prices, job.clone())?;
                        check_page_request_capacity(&plan, job)?;
                        match example {
                            None => {
                                if let Some(reference) = &input.fiscal_page {
                                    let reader = self
                                        .fiscal_reader
                                        .as_ref()
                                        .ok_or(ServiceError::Unavailable)?;
                                    let page = reader
                                        .read_page_reference(
                                            &binding,
                                            &prices,
                                            input
                                                .page_ordinal
                                                .ok_or(ServiceError::InvalidRequest)?,
                                            reference,
                                            context,
                                        )
                                        .await?;
                                    json!({"page":page,"targets":fiscal_projection_targets(),"fiscalPage":reference})
                                } else {
                                    let page =
                                        binding.page(&prices, input.page_ordinal.unwrap_or(0))?;
                                    json!({"page":page,"targets":fiscal_projection_targets()})
                                }
                            }
                            Some(example) => {
                                if input.page_ordinal.is_some() {
                                    return Err(ServiceError::InvalidRequest);
                                }
                                binding.contains(&prices, example)?;
                                let price = price_coordinate(
                                    &prices,
                                    example,
                                    input.subject_instrument_id,
                                )?;
                                let population = self
                                    .fiscal_population(price.epoch(), plan.profile(), context)
                                    .await?;
                                let mut targets = Vec::with_capacity(9);
                                for target in fiscal_projection_targets() {
                                    let availability = match self
                                        .datasets
                                        .prepare_historical_financial_datasets(
                                            price.epoch(),
                                            &target,
                                            population.clone(),
                                            plan.profile(),
                                            context,
                                        )
                                        .await
                                    {
                                        Ok(_) => "ready",
                                        Err(ServiceError::Unavailable | ServiceError::NotFound) => {
                                            "unavailable"
                                        }
                                        Err(error) => return Err(error),
                                    };
                                    targets
                                        .push(json!({"target":target,"availability":availability}));
                                }
                                json!({"priceExampleId":example,"targets":targets})
                            }
                        }
                    }
                };
                json!({"status":"available","plan":plan.reference(),"fiscal":fiscal,
                    "folds":plan.folds().iter().enumerate().map(|(index,fold)|json!({"foldIndex":index,
                        "startsAtUnixNanos":fold.starts_at().unix_nanos().to_string(),
                        "endsAtUnixNanos":fold.ends_at().unix_nanos().to_string()})).collect::<Vec<_>>()})
            }
            None => {
                json!({"status":"unavailable","reasons":["insufficient_source_qualified_history"]})
            }
        };
        TypedToolResult::try_new(
            value,
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }
    pub(super) async fn prepare_dataset(
        &self,
        forecasts: &InstalledForecastPreparation,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<PreparedFeatureDatasetBuild, ServiceError> {
        let input: DatasetInput = decode(request)?;
        let plan = self.reopen(&input.plan, forecasts, context).await?;
        if let Some(fiscal) = &input.fiscal {
            if input.fold_index.is_some() {
                return Err(ServiceError::InvalidRequest);
            }
            let (_, prices) = self
                .open_price_inputs(&plan, &fiscal.study_input_job, context)
                .await?;
            HistoricalFiscalStudyBinding::from_source(
                &plan,
                &prices,
                fiscal.study_input_job.clone(),
            )?
            .contains(&prices, &fiscal.price_example_id)?;
            let coordinate = price_coordinate(
                &prices,
                &fiscal.price_example_id,
                plan.reference().subject_instrument_id(),
            )?;
            let target = fiscal_target(&fiscal.target_id)?;
            let prepared = self
                .prepare_fiscal(coordinate.epoch(), &target, plan.profile(), context)
                .await?;
            let (training, inputs, _, _) = prepared.into_parts();
            return match input.part.as_str() {
                "training" => Ok(training),
                "studyInputs" => Ok(inputs),
                _ => Err(ServiceError::InvalidRequest),
            };
        }
        let part = match (input.part.as_str(), input.fold_index) {
            ("training", Some(index)) if index < 3 => HistoricalStudyDatasetPartV1::Training(index),
            ("studyInputs", None) => HistoricalStudyDatasetPartV1::StudyInputs,
            _ => return Err(ServiceError::InvalidRequest),
        };
        self.plans.prepare_dataset(&plan, part, context).await
    }
    pub(super) async fn prepare_training(
        &self,
        runner: &TrainingJobRunner,
        forecasts: &InstalledForecastPreparation,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<PreparedProductTraining, ServiceError> {
        let input: TrainingInput = decode(request)?;
        let plan = self.reopen(&input.plan, forecasts, context).await?;
        let snapshot = self
            .training
            .snapshot(
                &input.dataset_job.job_id,
                input.dataset_job.generation,
                context,
            )
            .await?;
        let selection = self
            .training
            .reopen_training_dataset(&snapshot, context)
            .await?;
        if let Some(fiscal) = &input.fiscal {
            if input.fold_index.is_some() {
                return Err(ServiceError::InvalidRequest);
            }
            let (_, prices) = self
                .open_price_inputs(&plan, &fiscal.study_input_job, context)
                .await?;
            HistoricalFiscalStudyBinding::from_source(
                &plan,
                &prices,
                fiscal.study_input_job.clone(),
            )?
            .contains(&prices, &fiscal.price_example_id)?;
            let coordinate = price_coordinate(
                &prices,
                &fiscal.price_example_id,
                plan.reference().subject_instrument_id(),
            )?;
            let target = fiscal_target(&fiscal.target_id)?;
            let prepared = self
                .prepare_fiscal(coordinate.epoch(), &target, plan.profile(), context)
                .await?;
            let (_, _, _, expectation) = prepared.into_parts();
            let role = expectation.admit_training(&snapshot, &selection, plan.profile())?;
            return runner
                .prepare_historical_fiscal_product(
                    &selection,
                    plan.profile(),
                    &role,
                    context.deadline(),
                    context.cancellation(),
                )
                .map_err(super::tool_services::map_training_admission);
        }
        let role = self
            .plans
            .admit_training(
                &plan,
                input.fold_index.ok_or(ServiceError::InvalidRequest)?,
                &snapshot,
                &selection,
                context,
            )
            .await?;
        runner
            .prepare_historical_product(
                &selection,
                plan.profile(),
                &role,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(super::tool_services::map_training_admission)
    }
    pub(super) async fn complete_fiscal_page(
        &self,
        runner: &TrainingJobRunner,
        forecasts: &InstalledForecastPreparation,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let input: CompleteFiscalPageInput = decode(request)?;
        let plan = self.reopen(&input.plan, forecasts, context).await?;
        let (_, prices) = self
            .open_price_inputs(&plan, &input.study_input_job, context)
            .await?;
        let binding = HistoricalFiscalStudyBinding::from_source(
            &plan,
            &prices,
            input.study_input_job.clone(),
        )?;
        let page = binding.page(&prices, input.page_ordinal)?;
        check_page_request_capacity(&plan, &input.study_input_job)?;
        let reference = self
            .complete_fiscal_inputs(runner, &plan, &prices, &page, input.fiscal_jobs, context)
            .await?;
        TypedToolResult::try_new(
            json!({"status":"completed","page":page,"fiscalPage":reference}),
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }
    /// Reopens the original source action plan; root supplies the selected account and actor
    /// from its trusted local analysis context. The issuer goes directly to job admission.
    pub(super) async fn prepare_study(
        &self,
        runner: &TrainingJobRunner,
        forecasts: &InstalledForecastPreparation,
        request: &TypedToolRequest,
        account: AccountId,
        actor: ActorId,
        context: &RequestContext,
    ) -> Result<PreparedRecommendationStudyV1, ServiceError> {
        let input: StudyInput = decode(request)?;
        let plan = self.reopen(&input.plan, forecasts, context).await?;
        let source_actions = self.plans.reopen_accounting_actions(&plan, context).await?;
        let (dataset, prices) = self
            .open_price_inputs(&plan, &input.study_input_job, context)
            .await?;
        let mut admissions = Vec::with_capacity(3);
        for (index, job) in input.training_jobs.iter().enumerate() {
            let dataset_job = self
                .training
                .snapshot(&job.dataset_job.job_id, job.dataset_job.generation, context)
                .await?;
            let dataset = self
                .training
                .reopen_training_dataset(&dataset_job, context)
                .await?;
            let role = self
                .plans
                .admit_training(&plan, index, &dataset_job, &dataset, context)
                .await?;
            let expected = runner
                .prepare_historical_product(
                    &dataset,
                    plan.profile(),
                    &role,
                    context.deadline(),
                    context.cancellation(),
                )
                .map_err(super::tool_services::map_training_admission)?;
            let completed = self
                .training
                .snapshot(&job.job_id, job.generation, context)
                .await?;
            if completed.spec().input().digest() != expected.commitment() {
                return Err(ServiceError::InvalidResult);
            }
            let model = runner
                .resolve_completed(&completed)
                .map_err(super::tool_services::map_training_admission)?;
            if model.dataset_selection_sha256() != dataset.selection_sha256() {
                return Err(ServiceError::InvalidResult);
            }
            admissions.push(model);
        }
        let admissions: [_; 3] = admissions
            .try_into()
            .map_err(|_| ServiceError::InvalidRequest)?;
        let runtimes = self
            .runtime
            .as_ref()
            .ok_or(ServiceError::Unavailable)?
            .select_completed_historical_runtimes(&admissions)?;
        let binding = HistoricalFiscalStudyBinding::from_source(
            &plan,
            &prices,
            input.study_input_job.clone(),
        )?;
        let reader = Arc::clone(
            self.fiscal_reader
                .as_ref()
                .ok_or(ServiceError::Unavailable)?,
        );
        let source = reader
            .publish_selection(
                binding,
                &prices,
                input.fiscal_pages,
                plan.profile(),
                context,
            )
            .await?;
        let valuation_inputs = Arc::new(HistoricalStudyValuationReadCapability::new(
            self.macro_reader.clone(),
            self.calendars.clone(),
            reader,
            source,
            plan.profile().clone(),
        ));
        let producer = HistoricalRecommendationAlphaProducer::try_new(
            runtimes,
            plan.benchmarks().clone(),
            plan.profile().clone(),
            plan.reference().subject_instrument_id(),
            account,
            actor,
            Arc::clone(&self.research),
            Arc::clone(&self.valuation),
            valuation_inputs,
            self.calendars.clone(),
        )?;
        self.inputs
            .prepare_recommendation_study(
                RecommendationStudyPreparationInputV1 {
                    producer,
                    dataset,
                    source_action_reference: plan.reference().source_action_reference().clone(),
                    histories: plan.into_histories(),
                    corporate_actions: source_actions,
                },
                context,
            )
            .await
    }
    /// All fiscal calls reopen the completed original price publication through its exact job.
    async fn open_price_inputs(
        &self,
        plan: &HistoricalStudyPlanV1,
        job: &JobReference,
        context: &RequestContext,
    ) -> Result<(AnalyticalFeatureDataset, FeatureDatasetInputEpochCursor), ServiceError> {
        let study_job = self
            .training
            .snapshot(&job.job_id, job.generation, context)
            .await?;
        let selection = self
            .training
            .reopen_prepared_dataset(
                &study_job,
                Some(
                    FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1,
                ),
                context,
            )
            .await?;
        let expected = self
            .plans
            .prepare_dataset(plan, HistoricalStudyDatasetPartV1::StudyInputs, context)
            .await?;
        let (expected, _) = expected.into_parts();
        if selection.identity().build_spec_digest() != expected.build_spec_digest()
            || study_job.spec().input().digest().bytes()
                != expected.build_spec_digest().digest().bytes()
        {
            return Err(ServiceError::InvalidResult);
        }
        let dataset = self
            .research
            .analytical_reader()
            .feature_dataset_for_build(
                selection.product_contract(),
                selection.identity().manifest().dataset_id(),
                selection.identity().build_spec_digest(),
                context.deadline(),
                context.cancellation(),
            )
            .map_err(crate::application::map_source_analytical_error)?
            .ok_or(ServiceError::Unavailable)?;
        if dataset.generation().manifest() != selection.identity().manifest() {
            return Err(ServiceError::InvalidResult);
        }

        let prices = self.read_input_epochs(&dataset, context).await?;
        // The source owner supplies the original population bound and exact causal subset.
        HistoricalFiscalStudyBinding::from_source(plan, &prices, job.clone())?;
        Ok((dataset, prices))
    }
    async fn read_input_epochs(
        &self,
        dataset: &AnalyticalFeatureDataset,
        context: &RequestContext,
    ) -> Result<FeatureDatasetInputEpochCursor, ServiceError> {
        let limits = QueryLimits::try_new_with_inline_bytes(
            32768,
            32 * 1024 * 1024,
            64 * 1024 * 1024,
            64 * 1024 * 1024,
            1,
            128,
            128,
            Duration::from_secs(30),
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        self.research
            .analytical_reader()
            .feature_dataset_input_epoch_cursor(
                dataset.product_contract(),
                dataset.generation().manifest(),
                limits,
                context.deadline(),
                context.cancellation().child_token(),
            )
            .await
            .map_err(crate::application::map_source_analytical_error)
    }
    async fn prepare_fiscal(
        &self,
        epoch: &FeatureDatasetInputEpoch,
        target: &FiscalProjectionTarget,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<PreparedHistoricalFiscalDatasets, ServiceError> {
        let population = self.fiscal_population(epoch, profile, context).await?;
        self.datasets
            .prepare_historical_financial_datasets(epoch, target, population, profile, context)
            .await
    }
    async fn fiscal_population(
        &self,
        epoch: &FeatureDatasetInputEpoch,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<market_squawk_data::CurrentListedPopulation, ServiceError> {
        let identities = self.identities.as_ref().ok_or(ServiceError::Unavailable)?;
        let digest = super::jobs::parse_sha256(&profile.resolution().configuration_digest)?;
        let population = prepare_fixed_current_population(
            &self.research,
            Arc::new(identities.clone()),
            vec![epoch.instrument_id()],
            Sha256Digest::new(digest.bytes()),
            epoch.source_selection_as_of(),
            context.deadline(),
            context.cancellation(),
        )
        .await?;
        let [member] = population.members() else {
            return Err(ServiceError::InvalidResult);
        };
        if !profile.admits_investment(
            member.canonical_record().definition().asset_class(),
            member.listing_record().is_etf(),
        ) {
            return Err(ServiceError::Unavailable);
        }
        Ok(population)
    }
    /// Complete the closed nine-target recipe for every actual subject origin. Job references
    /// never supply values: each job must match the newly source-reconstructed native builds.
    async fn complete_fiscal_inputs(
        &self,
        runner: &TrainingJobRunner,
        plan: &HistoricalStudyPlanV1,
        prices: &FeatureDatasetInputEpochCursor,
        page: &HistoricalFiscalPageDescriptor,
        jobs: Vec<CompletedFiscalTarget>,
        context: &RequestContext,
    ) -> Result<HistoricalFiscalPageReference, ServiceError> {
        if jobs.len() > HISTORICAL_FISCAL_PAGE_SIZE * 9 {
            return Err(ServiceError::ResourceExhausted);
        }
        let mut selected = BTreeMap::new();
        for job in jobs {
            // Enforce the proven per-target wire bound, including actual canonical source IDs.
            if serde_json::to_vec(&job)
                .map_err(|_| ServiceError::InvalidRequest)?
                .len()
                > 393
            {
                return Err(ServiceError::ResourceExhausted);
            }
            let key = (job.price_example_id.clone(), job.target_id.clone());
            fiscal_target(&job.target_id)?;
            if selected.insert(key, job).is_some() {
                return Err(ServiceError::InvalidRequest);
            }
        }
        let mut completed = Vec::new();
        let mut unavailable = Vec::new();
        for origin in &page.origins {
            let price = price_coordinate(
                prices,
                &origin.price_example_id,
                plan.reference().subject_instrument_id(),
            )?;
            let epoch = price.epoch();
            // A missing population is not evidence of missing fiscal data. Admit it once
            // before recording any target-specific source failure for this origin.
            let population = self
                .fiscal_population(epoch, plan.profile(), context)
                .await?;
            for target in fiscal_projection_targets() {
                let supplied =
                    selected.remove(&(epoch.example_id().to_owned(), target.target_id.clone()));
                let prepared = match self
                    .datasets
                    .prepare_historical_financial_datasets(
                        epoch,
                        &target,
                        population.clone(),
                        plan.profile(),
                        context,
                    )
                    .await
                {
                    Ok(prepared) => prepared,
                    Err(error @ (ServiceError::Unavailable | ServiceError::NotFound)) => {
                        if supplied.is_some() {
                            return Err(ServiceError::InvalidResult);
                        }
                        unavailable.push(HistoricalFiscalUnavailableReference::from_source(
                            &target.target_id,
                            price.coordinate(),
                            error,
                        )?);
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let job = supplied.ok_or(ServiceError::InvalidRequest)?;
                let (_, expected_inputs, _, expectation) = prepared.into_parts();
                let training_job = self
                    .training
                    .snapshot(
                        &job.training_dataset_job.job_id,
                        job.training_dataset_job.generation,
                        context,
                    )
                    .await?;
                let training = self
                    .training
                    .reopen_training_dataset(&training_job, context)
                    .await?;
                let role = expectation.admit_training(&training_job, &training, plan.profile())?;
                let expected = runner
                    .prepare_historical_fiscal_product(
                        &training,
                        plan.profile(),
                        &role,
                        context.deadline(),
                        context.cancellation(),
                    )
                    .map_err(super::tool_services::map_training_admission)?;
                let model_job = self
                    .training
                    .snapshot(
                        &job.training_job.job_id,
                        job.training_job.generation,
                        context,
                    )
                    .await?;
                if model_job.spec().input().digest() != expected.commitment() {
                    return Err(ServiceError::InvalidResult);
                }
                let model = runner
                    .resolve_completed(&model_job)
                    .map_err(super::tool_services::map_training_admission)?;
                if model.dataset_selection_sha256() != training.selection_sha256() {
                    return Err(ServiceError::InvalidResult);
                }
                let input_job = self
                    .training
                    .snapshot(
                        &job.input_dataset_job.job_id,
                        job.input_dataset_job.generation,
                        context,
                    )
                    .await?;
                let inputs=self.training.reopen_prepared_dataset(&input_job,Some(FeatureDatasetProductContract::FinancialAmountFiscalPeriodsStudyInputsV1),context).await?;
                let (expected_inputs, _) = expected_inputs.into_parts();
                if inputs.identity().build_spec_digest() != expected_inputs.build_spec_digest()
                    || input_job.spec().input().digest().bytes()
                        != expected_inputs.build_spec_digest().digest().bytes()
                {
                    return Err(ServiceError::InvalidResult);
                }
                let dataset = self
                    .research
                    .analytical_reader()
                    .feature_dataset_for_build(
                        inputs.product_contract(),
                        inputs.identity().manifest().dataset_id(),
                        inputs.identity().build_spec_digest(),
                        context.deadline(),
                        context.cancellation(),
                    )
                    .map_err(crate::application::map_source_analytical_error)?
                    .ok_or(ServiceError::Unavailable)?;
                if dataset.generation().manifest() != inputs.identity().manifest() {
                    return Err(ServiceError::InvalidResult);
                }
                let output = self.read_input_epochs(&dataset, context).await?;
                // Select only this origin's target; never collect a population of runtimes.
                let [runtime] = self
                    .runtime
                    .as_ref()
                    .ok_or(ServiceError::Unavailable)?
                    .select_completed_historical_runtimes(&[model])?;
                let forecast = expectation.forecast(
                    &self.research,
                    &runtime,
                    &output,
                    price.coordinate(),
                    context,
                )?;
                completed.push((
                    forecast.reference().clone(),
                    HistoricalFiscalCompletedJobs {
                        training_dataset: job.training_dataset_job,
                        input_dataset: job.input_dataset_job,
                        training: job.training_job,
                    },
                ));
                // Release native paths/residual support now; the controlled recipe retains
                // only exact source/model coordinates for later original-origin inference.
                drop(forecast);
            }
        }
        if !selected.is_empty() {
            return Err(ServiceError::InvalidRequest);
        }
        let reader = Arc::clone(
            self.fiscal_reader
                .as_ref()
                .ok_or(ServiceError::Unavailable)?,
        );
        reader
            .publish_page(page, completed, unavailable, context)
            .await
    }
}
fn decode<T: serde::de::DeserializeOwned>(request: &TypedToolRequest) -> Result<T, ServiceError> {
    serde_json::from_value(serde_json::Value::Object(super::business_arguments(
        request.arguments(),
    )))
    .map_err(|_| ServiceError::InvalidRequest)
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PlanInput {
    source_action_reference: crate::application::SourceAppliedCorporateActionPlanReference,
    page_ordinal: Option<usize>,
    fiscal_page: Option<HistoricalFiscalPageReference>,
    subject_instrument_id: InstrumentId,
    source_cutoff_unix_nanos: String,
    financial_profile: AnalyticalProfileResolution,
    study_input_job: Option<JobReference>,
    price_example_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatasetInput {
    plan: HistoricalStudyPlanReferenceV1,
    part: String,
    fold_index: Option<usize>,
    fiscal: Option<FiscalOriginRequest>,
}
type JobReference = HistoricalFiscalJobReference;
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TrainingInput {
    plan: HistoricalStudyPlanReferenceV1,
    fold_index: Option<usize>,
    dataset_job: JobReference,
    fiscal: Option<FiscalOriginRequest>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CompletedFold {
    job_id: String,
    generation: u64,
    dataset_job: JobReference,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StudyInput {
    plan: HistoricalStudyPlanReferenceV1,
    study_input_job: JobReference,
    training_jobs: [CompletedFold; 3],
    fiscal_pages: Vec<HistoricalFiscalPageReference>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FiscalOriginRequest {
    study_input_job: JobReference,
    price_example_id: String,
    target_id: String,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CompletedFiscalTarget {
    price_example_id: String,
    target_id: String,
    training_dataset_job: JobReference,
    input_dataset_job: JobReference,
    training_job: JobReference,
}
fn fiscal_target(id: &str) -> Result<FiscalProjectionTarget, ServiceError> {
    fiscal_projection_targets()
        .into_iter()
        .find(|target| target.target_id == id)
        .ok_or(ServiceError::InvalidRequest)
}
fn price_coordinate(
    prices: &FeatureDatasetInputEpochCursor,
    example: &str,
    instrument: InstrumentId,
) -> Result<OwnedFeatureDatasetInputCoordinate, ServiceError> {
    let mut selected = None;
    for coordinate in prices.coordinates() {
        let coordinate = coordinate.map_err(crate::application::map_source_analytical_error)?;
        if coordinate.epoch().example_id() == example
            && coordinate.epoch().instrument_id() == instrument
        {
            if selected.replace(coordinate).is_some() {
                return Err(ServiceError::InvalidResult);
            }
        }
    }
    selected.ok_or(ServiceError::InvalidRequest)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CompleteFiscalPageInput {
    plan: HistoricalStudyPlanReferenceV1,
    study_input_job: JobReference,
    page_ordinal: usize,
    fiscal_jobs: Vec<CompletedFiscalTarget>,
}

/// Reserve the actual serialized original header before the native owner schedules a page.
/// 393 bytes per canonical completed target, plus delimiters, follows the original closed wire.
fn check_page_request_capacity(
    plan: &HistoricalStudyPlanV1,
    job: &JobReference,
) -> Result<(), ServiceError> {
    let header = serde_json::to_vec(&json!({"plan":plan.reference(),"studyInputJob":job,
        "pageOrdinal":usize::MAX,"fiscalJobs":[],"confirm":true}))
    .map_err(|_| ServiceError::InvalidResult)?;
    let entries = HISTORICAL_FISCAL_PAGE_SIZE
        .checked_mul(9)
        .and_then(|n| n.checked_mul(394))
        .ok_or(ServiceError::ResourceExhausted)?;
    if header
        .len()
        .checked_add(entries)
        .is_none_or(|n| n > 1024 * 1024)
    {
        return Err(ServiceError::ResourceExhausted);
    }
    Ok(())
}
