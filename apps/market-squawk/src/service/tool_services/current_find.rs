//! Current source preparation and exact dataset/screen children over the installed authorities.
//! Desktop chooses and checkpoints the sequence. This leaf performs one requested primitive.

mod source_actions;

use std::sync::Arc;

use market_squawk_data::Sha256Digest;
use market_squawk_decisions::SelectedCandidateAnalysisEvidence;
use market_squawk_domain::Timestamp;
use market_squawk_jobs::{JobSnapshot, JobState};
use market_squawk_runtime::RuntimeIdentity;
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{
    LocalProduct, ResearchService,
    application::{
        CurrentFindScreenPartition, InstrumentContextReadCapability, PreparedCurrentFindFeatures,
        PreparedFeatureDatasetBuild,
        analytical_profile::{AnalyticalProfileResolution, ValidatedAnalyticalProfile, revalidate},
        decision::{
            AdmittedScreenJob, DecisionApplication, ScreenWorkflowError,
            current_find::{
                CurrentFindCompletionRecord, CurrentFindCompletionReference, CurrentFindDatasetJob,
                CurrentFindPreparationRecord, RetainedCurrentFindPartition,
            },
        },
        market_calendar::{CompletedMarketSessionRead, CompletedMarketSessionReadCapability},
        model::forecast_preparation::ForecastPreparationCatalog,
        prepare_find_population, read_find_population,
    },
};

use super::super::research_dataset::InstalledResearchDatasetPreparation;

pub(super) const PREPARE: &str = "Decision.PrepareCurrentScreen";
pub(super) const READ: &str = "Decision.ReadCurrentScreenPreparation";
pub(super) const PREPARE_PARTITION: &str = "Analysis.PrepareCurrentScreenPartition";
pub(super) const COMPLETE_PARTITION: &str = "Analysis.CompleteCurrentScreenPartition";
pub(super) const READ_COMPLETION: &str = "Decision.ReadCurrentScreenPartitionCompletion";
pub(super) const START_DATASET: &str = "Analysis.StartCurrentScreenDataset";
pub(super) const START_SCREEN: &str = "Decision.StartCurrentScreen";
pub(super) const GET_RESULT: &str = "Decision.GetCurrentScreenJobResult";
pub(super) const READ_COVERAGE: &str = "Decision.ReadCurrentScreenCoverage";

pub(super) struct InstalledCurrentFind {
    research: Arc<ResearchService>,
    pub(super) decisions: Arc<DecisionApplication>,
    identities: Option<Arc<InstrumentContextReadCapability>>,
    calendars: CompletedMarketSessionReadCapability,
    runtime: RuntimeIdentity,
    activation: Arc<crate::provider_activation::ProviderAdapterActivation>,
    ingest: Arc<crate::application::ProductionResearchIngestCoordinator>,
    actions: crate::application::SourceActionPreparationCapability,
    // Only current-screen preparation is serialized here; unrelated product reads stay responsive.
    preparation: tokio::sync::Mutex<()>,
}

impl InstalledCurrentFind {
    pub(super) fn new(product: &LocalProduct, runtime: RuntimeIdentity) -> Self {
        Self {
            research: product.research(),
            decisions: product.decisions(),
            identities: product.instrument_context_read_capability().map(Arc::new),
            calendars: CompletedMarketSessionReadCapability::new(
                product.research(),
                product.market_runtime(),
            ),
            runtime,
            activation: product.provider_activation(),
            ingest: product.research_ingest(),
            actions: crate::application::SourceActionPreparationCapability::new(
                product.research(),
                product.market_runtime(),
                product.research_ingest(),
            ),
            preparation: tokio::sync::Mutex::new(()),
        }
    }

    pub(super) fn owns(name: &str) -> bool {
        matches!(
            name,
            PREPARE
                | READ
                | PREPARE_PARTITION
                | COMPLETE_PARTITION
                | READ_COMPLETION
                | GET_RESULT
                | READ_COVERAGE
        )
    }

    pub(super) fn authorize(&self, context: &RequestContext) -> Result<(), ServiceError> {
        super::ensure_live(context)?;
        if context
            .origin()
            .ok_or(ServiceError::Unauthorized)?
            .workspace_id()
            != self.runtime.workspace_id().as_uuid()
        {
            return Err(ServiceError::Unauthorized);
        }
        Ok(())
    }

    async fn lock(
        &self,
        context: &RequestContext,
    ) -> Result<tokio::sync::MutexGuard<'_, ()>, ServiceError> {
        tokio::select! { biased;
            _ = context.cancellation().cancelled() => Err(ServiceError::Cancelled),
            _ = tokio::time::sleep_until(context.deadline().into()) => Err(ServiceError::DeadlineExceeded),
            guard = self.preparation.lock() => Ok(guard),
        }
    }

    pub(super) fn parent(
        &self,
        reference: &PreparationReference,
        context: &RequestContext,
    ) -> Result<CurrentFindPreparationRecord, ServiceError> {
        self.authorize(context)?;
        let parent = self
            .decisions
            .current_find_preparation(reference.preparation_id)
            .map_err(map_journal)?
            .ok_or(ServiceError::NotFound)?;
        parent.authorize(context)?;
        if parent.digest().map_err(map_journal)? != parse_digest(&reference.preparation_sha256)? {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(parent)
    }

    /// Saved coordinates remain inert until the original calendar is physically reopened.
    async fn validate_forecast_cohort(
        &self,
        parent: &CurrentFindPreparationRecord,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        let Some(reference) = &parent.forecast_cohort else {
            return Ok(());
        };
        let calendar = self
            .calendars
            .read_reference(
                reference.calendar(),
                parent.analytical_cutoff,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_calendar)?
            .ok_or(ServiceError::Unavailable)?;
        calendar
            .reopen_forecast_session_cohort(
                reference,
                parent.analytical_cutoff,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(map_calendar)?
            .ok_or(ServiceError::Unavailable)?;
        Ok(())
    }

    async fn reopen(
        &self,
        authority: &InstalledResearchDatasetPreparation,
        parent: &CurrentFindPreparationRecord,
        models: Option<&ForecastPreparationCatalog>,
        context: &RequestContext,
    ) -> Result<
        (
            PreparedCurrentFindFeatures,
            ValidatedAnalyticalProfile,
            CompletedMarketSessionRead,
        ),
        ServiceError,
    > {
        self.authorize(context)?;
        parent.authorize(context)?;
        let profile = revalidate(&parent.profile, models).map_err(ServiceError::from)?;
        let population = read_find_population(
            &self.research,
            self.identities.clone().ok_or(ServiceError::Unavailable)?,
            profile.clone(),
            parent.population.clone(),
            context.deadline(),
            context.cancellation(),
        )
        .await?;
        let source_actions = self
            .decisions
            .current_find_sources(&parent.source_pages, parent.analytical_cutoff)
            .map_err(map_journal)?;
        let plan = authority
            .prepare_current_find_features(
                &population,
                &self.research.market_data_instruments(),
                &profile,
                parent.analytical_cutoff,
                &source_actions,
                Some(&parent.partition_ends),
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(ServiceError::from)?;
        if plan.partitions().len() != parent.partition_count
            || plan.partition_ends() != parent.partition_ends
            || plan.analytical_cutoff() != parent.analytical_cutoff
        {
            return Err(ServiceError::InvalidResult);
        }
        drop(population);
        let calendar = self
            .calendars
            .read_reference(
                parent.calendar.as_ref().ok_or(ServiceError::Unavailable)?,
                parent.analytical_cutoff,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_calendar)?
            .ok_or(ServiceError::Unavailable)?;
        if let Some(reference) = &parent.forecast_cohort {
            let cohort = calendar
                .reopen_forecast_session_cohort(
                    reference,
                    parent.analytical_cutoff,
                    context.deadline(),
                    context.cancellation(),
                )
                .map_err(map_calendar)?
                .ok_or(ServiceError::Unavailable)?;
            if cohort.reference().horizon_nanos().map_err(map_calendar)?
                != profile
                    .horizon()
                    .step_nanos()
                    .and_then(|step| i64::try_from(step.get()).ok())
                    .ok_or(ServiceError::InvalidResult)?
            {
                return Err(ServiceError::InvalidResult);
            }
        }
        self.authorize(context)?;
        Ok((plan, profile, calendar))
    }

    pub(super) async fn call(
        &self,
        authority: &InstalledResearchDatasetPreparation,
        training: &super::training_preparation::InstalledProductTraining,
        request: &TypedToolRequest,
        context: &RequestContext,
        models: Option<&ForecastPreparationCatalog>,
    ) -> Result<TypedToolResult, ServiceError> {
        self.authorize(context)?;
        let content = match request.name() {
            PREPARE => {
                let _guard = self.lock(context).await?;
                let input: PrepareInput = super::decode(request.arguments())?;
                if !matches!(input.maximum_candidates, 8 | 16 | 32) {
                    return Err(ServiceError::InvalidRequest);
                }
                let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
                let request_bytes = serde_json::to_vec(context.request_id())
                    .map_err(|_| ServiceError::InvalidRequest)?;
                let id = crate::application::opaque_product_token(
                    b"market-squawk/current-screen-preparation/v1\0",
                    &[
                        origin.workspace_id().as_bytes(),
                        origin.client_id().as_bytes(),
                        &request_bytes,
                    ],
                );
                let request_sha256: [u8; 32] = Sha256::digest(
                    serde_json::to_vec(&super::super::business_arguments(request.arguments()))
                        .map_err(|_| ServiceError::InvalidRequest)?,
                )
                .into();
                let parent = match self
                    .decisions
                    .current_find_preparation(id)
                    .map_err(map_journal)?
                {
                    Some(parent) => {
                        parent.authorize(context)?;
                        if parent.request_sha256 != request_sha256 {
                            return Err(ServiceError::InvalidRequest);
                        }
                        parent
                    }
                    None => {
                        let profile = revalidate(&input.financial_profile, models)
                            .map_err(ServiceError::from)?;
                        let provisional_cutoff = super::super::runtime::current_timestamp()
                            .map_err(|_| ServiceError::Internal)?;
                        let provisional = prepare_find_population(
                            &self.research,
                            self.identities.clone().ok_or(ServiceError::Unavailable)?,
                            profile.clone(),
                            provisional_cutoff,
                            input.maximum_candidates,
                            context.deadline(),
                            context.cancellation(),
                        )
                        .await?;
                        let (pending, original_cohort) = self
                            .publish_current_sources(&provisional, &profile, input.benchmark_instrument_id, context)
                            .await?;
                        // Every source publication precedes this ONE immutable analytical cutoff.
                        let cutoff = super::super::runtime::current_timestamp()
                            .map_err(|_| ServiceError::Internal)?;
                        let mut source_pages = Vec::with_capacity(pending.len());
                        for batch in pending {
                            self.authorize(context)?;
                            let (original, training) = self
                                .actions
                                .finish_current_price_actions(batch, cutoff, context)
                                .await?;
                            source_pages.push(
                                self.decisions
                                    .retain_current_find_source_page(original, training, context)
                                    .map_err(|error| map_journal_control(error, context))?,
                            );
                        }
                        let source_actions = self
                            .decisions
                            .current_find_sources(&source_pages, cutoff)
                            .map_err(map_journal)?;
                        let population = prepare_find_population(
                            &self.research,
                            self.identities.clone().ok_or(ServiceError::Unavailable)?,
                            profile.clone(),
                            cutoff,
                            input.maximum_candidates,
                            context.deadline(),
                            context.cancellation(),
                        )
                        .await?;
                        source_actions::require_same_source_population(&provisional, &population)?;
                        drop(provisional);
                        let plan = authority
                            .prepare_current_find_features(
                                &population,
                                &self.research.market_data_instruments(),
                                &profile,
                                cutoff,
                                &source_actions,
                                None,
                                context.deadline(),
                                context.cancellation().clone(),
                            )
                            .await
                            .map_err(ServiceError::from)?;
                        let calendar = if plan.partitions().is_empty() {
                            None
                        } else {
                            Some(
                                self.calendars
                                    .select(
                                        cutoff,
                                        context.deadline(),
                                        context.cancellation().clone(),
                                    )
                                    .await
                                    .map_err(map_calendar)?
                                    .ok_or(ServiceError::Unavailable)?,
                            )
                        };
                        let forecast_cohort = calendar
                            .as_ref()
                            .map(|calendar| {
                                let horizon = profile
                                    .horizon()
                                    .step_nanos()
                                    .and_then(|step| i64::try_from(step.get()).ok())
                                    .ok_or(ServiceError::InvalidResult)?;
                                calendar
                                    .latest_forecast_session_cohort(
                                        cutoff,
                                        horizon,
                                        cutoff,
                                        context.deadline(),
                                        context.cancellation(),
                                    )
                                    .map_err(map_calendar)
                            })
                            .transpose()?
                            .flatten();
                        if original_cohort
                            .as_ref()
                            .zip(forecast_cohort.as_ref())
                            .is_some_and(|(original, current)| {
                                !original.same_economic_session(current.reference())
                            })
                            || (original_cohort.is_some() && forecast_cohort.is_none())
                        {
                            return Err(ServiceError::Unavailable);
                        }
                        self.authorize(context)?;
                        let parent = CurrentFindPreparationRecord::from_source(
                            id,
                            request_sha256,
                            &population,
                            &plan,
                            &profile,
                            calendar.as_ref().map(|read| read.reference().clone()),
                            forecast_cohort.as_ref(),
                            &source_actions,
                            context,
                        )?;
                        self.decisions
                            .retain_current_find_preparation(parent, context)
                            .map_err(|error| map_journal_control(error, context))?
                    }
                };
                self.validate_forecast_cohort(&parent, context).await?;
                self.preparation_value(&parent)?
            }
            READ => {
                let reference: PreparationReference = super::decode(request.arguments())?;
                let parent = self.parent(&reference, context)?;
                self.validate_forecast_cohort(&parent, context).await?;
                self.preparation_value(&parent)?
            }
            PREPARE_PARTITION => {
                let _guard = self.lock(context).await?;
                let input: PreparePartitionInput = super::decode(request.arguments())?;
                let parent = self.parent(&input.reference(), context)?;
                if input.ordinal >= parent.partition_count {
                    return Err(ServiceError::InvalidRequest);
                }
                if let Some(retained) = self
                    .decisions
                    .current_find_partition(&parent, input.ordinal)
                    .map_err(map_journal)?
                {
                    self.partition_value(&parent, &retained)?
                } else {
                    let (plan, profile, calendar) =
                        self.reopen(authority, &parent, models, context).await?;
                    if input.ordinal > 0 {
                        self.decisions
                            .current_find_completion(&parent, input.ordinal - 1)
                            .map_err(map_journal)?
                            .ok_or(ServiceError::InvalidRequest)?;
                    }
                    let prepared = authority
                        .prepare_current_find_feature_partition(
                            &plan,
                            input.ordinal,
                            &calendar,
                            &profile,
                            context.deadline(),
                            context.cancellation().clone(),
                        )
                        .await
                        .map_err(ServiceError::from)?;
                    let (evidence, build) = prepared.into_job_parts();
                    // This operation retains only the original source commitment. Start reconstructs one
                    // recipe and must reproduce that commitment before the ordinary dataset runner admits it.
                    drop(build);
                    self.authorize(context)?;
                    let retained = self
                        .decisions
                        .retain_current_find_partition(&parent, &evidence, context)
                        .map_err(|error| map_journal_control(error, context))?;
                    self.partition_value(&parent, &retained)?
                }
            }
            COMPLETE_PARTITION => {
                let _guard = self.lock(context).await?;
                let input: CompletePartitionInput = super::decode(request.arguments())?;
                let parent = self.parent(&input.reference(), context)?;
                let retained = self
                    .decisions
                    .current_find_partition(&parent, input.ordinal)
                    .map_err(map_journal)?
                    .ok_or(ServiceError::NotFound)?;
                let dataset_digest = self
                    .validate_partition_completion(
                        authority,
                        &retained,
                        input.dataset_job.as_ref(),
                        training,
                        context,
                    )
                    .await?;
                let child = input.dataset_job.map(|child| CurrentFindDatasetJob {
                    ordinal: input.ordinal,
                    job_id: child.job_id,
                    generation: child.generation,
                });
                self.authorize(context)?;
                let completed = self
                    .decisions
                    .retain_current_find_completion(
                        &parent,
                        &retained,
                        child,
                        dataset_digest,
                        context,
                    )
                    .map_err(|error| map_journal_control(error, context))?;
                completion_value(&parent, &completed)?
            }
            READ_COMPLETION => {
                let input: CompletionReadInput = super::decode(request.arguments())?;
                let parent = self.parent(&input.reference(), context)?;
                let completed = self
                    .decisions
                    .current_find_completion(&parent, input.completion.ordinal)
                    .map_err(map_journal)?
                    .ok_or(ServiceError::NotFound)?;
                if completed.reference().map_err(map_journal)? != input.completion {
                    return Err(ServiceError::InvalidRequest);
                }
                let retained = self
                    .decisions
                    .current_find_partition(&parent, input.completion.ordinal)
                    .map_err(map_journal)?
                    .ok_or(ServiceError::NotFound)?;
                let child = completed.dataset_job.as_ref().map(|child| DatasetJobInput {
                    job_id: child.job_id,
                    generation: child.generation,
                });
                if self
                    .validate_partition_completion(
                        authority,
                        &retained,
                        child.as_ref(),
                        training,
                        context,
                    )
                    .await?
                    != completed.dataset_content_sha256
                {
                    return Err(ServiceError::InvalidResult);
                }
                completion_value(&parent, &completed)?
            }
            READ_COVERAGE => {
                let input: CoverageInput = super::decode(request.arguments())?;
                let parent = self.parent(&input.reference(), context)?;
                if input.limit == 0 || input.limit > 128 {
                    return Err(ServiceError::InvalidRequest);
                }
                match input.ordinal {
                    Some(ordinal) => {
                        let retained = self
                            .decisions
                            .current_find_partition(&parent, ordinal)
                            .map_err(map_journal)?
                            .ok_or(ServiceError::NotFound)?;
                        let all = retained.evidence_reference().unavailable();
                        let rows = all.iter().skip(input.offset).take(input.limit).map(|item| json!({"instrumentId":item.instrument_id(),"reason":item.reason()})).collect::<Vec<_>>();
                        json!({"preparationId":parent.preparation_id,"preparationSha256":hex(parent.digest().map_err(map_journal)?),"ordinal":ordinal,"offset":input.offset,"total":all.len(),"rows":rows})
                    }
                    None => {
                        let exclusions = &parent.exclusions;
                        let rows = exclusions.iter().skip(input.offset).take(input.limit).map(|item| json!({"instrumentId":item.instrument_id(),"reason":item.reason()})).collect::<Vec<_>>();
                        json!({"preparationId":parent.preparation_id,"preparationSha256":hex(parent.digest().map_err(map_journal)?),"ordinal":null,"offset":input.offset,"total":exclusions.len(),"rows":rows})
                    }
                }
            }
            GET_RESULT => {
                let input: ScreenResultInput = super::decode(request.arguments())?;
                let parent = self.parent(&input.reference(), context)?;
                self.validate_forecast_cohort(&parent, context).await?;
                let (snapshot, execution) = self
                    .validate_screen_job(&parent, input.job_id, input.generation, training, context)
                    .await?;
                let run_id = execution.run().id().as_str();
                // Source identity is reopened at its original cutoff; deep analysis has its own later cutoff.
                let profile = revalidate(&parent.profile, models).map_err(ServiceError::from)?;
                let population = read_find_population(
                    &self.research,
                    self.identities.clone().ok_or(ServiceError::Unavailable)?,
                    profile.clone(),
                    parent.population.clone(),
                    context.deadline(),
                    context.cancellation(),
                )
                .await?;
                let screen = self
                    .decisions
                    .get_screen(
                        execution.run().screen().id(),
                        execution.run().screen().revision(),
                    )
                    .map_err(map_journal)?;
                let source_actions = self
                    .decisions
                    .current_find_sources(&parent.source_pages, parent.analytical_cutoff)
                    .map_err(map_journal)?;
                let plan = authority
                    .prepare_current_find_features(
                        &population,
                        &self.research.market_data_instruments(),
                        &profile,
                        parent.analytical_cutoff,
                        &source_actions,
                        Some(&parent.partition_ends),
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                    .map_err(ServiceError::from)?;
                let inputs = self
                    .selected_current_inputs(
                        authority, &parent, &plan, &execution, training, context,
                    )
                    .await?;
                let candidates = execution.candidates().iter().map(|candidate| {
                    let record = candidate.record();
                    let source = population.candidates().iter().find(|source| source.instrument_id() == record.instrument_id()).ok_or(ServiceError::InvalidResult)?;
                    let selected = SelectedCandidateAnalysisEvidence::try_new(&screen, execution.run(), candidate).map_err(|_|ServiceError::InvalidResult)?;
                    Ok(json!({"instrumentId":record.instrument_id(),"candidateId":record.id().as_str(),"screenRunId":record.screen_run_id().as_str(),
                        "evidenceDigest":hex(selected.evidence_digest().evidence_digest().bytes()),"selectionToken":source.selection_token(),"rank":record.rank().get(),
                        "currentFeatureInput":inputs.get(&record.instrument_id()).ok_or(ServiceError::InvalidResult)?}))
                }).collect::<Result<Vec<_>,ServiceError>>()?;
                let ranking_sha256: [u8;32] = Sha256::digest(serde_json::to_vec(&json!({"screenRunId":run_id,"preparationSha256":hex(parent.digest().map_err(map_journal)?),"candidates":&candidates})).map_err(|_|ServiceError::InvalidResult)?).into();
                json!({"job":crate::application::job::JobReceipt::from_snapshot(&snapshot),"preparationId":parent.preparation_id,
                    "preparationSha256":hex(parent.digest().map_err(map_journal)?),"sourceCutoffUnixNanos":parent.analytical_cutoff.unix_nanos().to_string(),"forecastCohort":parent.forecast_cohort,
                    "screenRunId":run_id,"candidates":candidates,"coverage":self.coverage_value(&parent)?.0,
                    "coverageReference":reference_value(&parent)?,"rankingSha256":hex(ranking_sha256)})
            }
            _ => return Err(ServiceError::NotFound),
        };
        self.authorize(context)?;
        let item_count = match request.name() {
            GET_RESULT => content
                .get("candidates")
                .and_then(Value::as_array)
                .ok_or(ServiceError::InvalidResult)?
                .len(),
            READ_COVERAGE => content
                .get("rows")
                .and_then(Value::as_array)
                .ok_or(ServiceError::InvalidResult)?
                .len(),
            _ => 1,
        };
        TypedToolResult::try_new(
            content,
            item_count,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }

    /// Resolve a genuine completed screen child against the original durable preparation.
    /// Final-result publication shares this path with the ordinary saved-screen read.
    pub(super) async fn validate_screen_job(
        &self,
        parent: &CurrentFindPreparationRecord,
        job_id: Uuid,
        generation: u64,
        training: &super::training_preparation::InstalledProductTraining,
        context: &RequestContext,
    ) -> Result<(JobSnapshot, market_squawk_decisions::ScreenExecution), ServiceError> {
        self.authorize(context)?;
        parent.authorize(context)?;
        let retained = self
            .decisions
            .current_find_screen(parent)
            .map_err(map_journal)?
            .ok_or(ServiceError::NotFound)?;
        let snapshot = training
            .snapshot(&job_id.to_string(), generation, context)
            .await?;
        let spec = snapshot.spec();
        let result = snapshot
            .terminal_result()
            .ok_or(ServiceError::Unavailable)?;
        if snapshot.state() != JobState::Completed
            || spec.kind().as_str() != "decision.screen-run.v1"
            || spec.input().authority().as_str() != "decision.screen-input.v1"
            || spec.input().identity() != &retained.input_identity
            || spec.input().digest() != retained.input_digest
            || spec.authority().authority().as_str() != "decision.screen-result.v1"
            || spec.authority().identity().as_str() != "decision.screen-result.v1"
            || result.authority().as_str() != "decision.screen-result.v1"
            || result.identity().as_str() != retained.run_id
            || result.evidence_digest() != retained.input_digest
            || !result.artifacts().is_empty()
        {
            return Err(ServiceError::InvalidResult);
        }
        let execution = self
            .decisions
            .prepared_screen_result(&retained.input_identity, retained.input_digest)
            .map_err(map_screen)?
            .ok_or(ServiceError::InvalidResult)?;
        if execution.run().id().as_str() != retained.run_id
            || execution
                .run()
                .universe_identity()
                .evidence_digest()
                .bytes()
                != parent.population_content_digest
            || execution.candidates().len() > parent.population.maximum_deep_analyses()
        {
            return Err(ServiceError::InvalidResult);
        }
        self.authorize(context)?;
        Ok((snapshot, execution))
    }

    pub(super) async fn prepare_dataset(
        &self,
        authority: &InstalledResearchDatasetPreparation,
        request: &TypedToolRequest,
        context: &RequestContext,
        models: Option<&ForecastPreparationCatalog>,
    ) -> Result<PreparedFeatureDatasetBuild, ServiceError> {
        let _guard = self.lock(context).await?;
        let input: PartitionInput = super::decode(request.arguments())?;
        let parent = self.parent(&input.reference(), context)?;
        let retained = self
            .decisions
            .current_find_partition(&parent, input.ordinal)
            .map_err(map_journal)?
            .ok_or(ServiceError::NotFound)?;
        if retained
            .evidence_reference()
            .expected_build_spec()
            .is_none()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let (plan, profile, calendar) = self.reopen(authority, &parent, models, context).await?;
        let prepared = authority
            .prepare_current_find_feature_partition(
                &plan,
                input.ordinal,
                &calendar,
                &profile,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(ServiceError::from)?;
        let (evidence, build) = prepared.into_job_parts();
        if &evidence.reference() != retained.evidence_reference() {
            return Err(ServiceError::InvalidResult);
        }
        self.authorize(context)?;
        build.ok_or(ServiceError::InvalidResult)
    }

    pub(super) async fn prepare_screen(
        &self,
        authority: &InstalledResearchDatasetPreparation,
        training: &super::training_preparation::InstalledProductTraining,
        request: &TypedToolRequest,
        context: &RequestContext,
        models: Option<&ForecastPreparationCatalog>,
        captured_at: Timestamp,
    ) -> Result<AdmittedScreenJob, ServiceError> {
        let _guard = self.lock(context).await?;
        let input: ScreenInput = super::decode(request.arguments())?;
        let parent = self.parent(&input.reference(), context)?;
        if input.completion.ordinal.checked_add(1) != Some(parent.partition_count) {
            return Err(ServiceError::InvalidRequest);
        }
        let final_completion = self
            .decisions
            .current_find_completion(&parent, input.completion.ordinal)
            .map_err(map_journal)?
            .ok_or(ServiceError::InvalidRequest)?;
        if final_completion.reference().map_err(map_journal)? != input.completion {
            return Err(ServiceError::InvalidRequest);
        }
        if let Some(existing) = self
            .decisions
            .current_find_screen(&parent)
            .map_err(map_journal)?
        {
            return self
                .decisions
                .prepared_screen_admission(&existing.input_identity, existing.input_digest)
                .map_err(map_screen);
        }
        let (plan, profile, calendar) = self.reopen(authority, &parent, models, context).await?;
        let mut parts = Vec::new();
        parts
            .try_reserve_exact(parent.partition_count)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        let mut dataset_jobs = Vec::new();
        let retained_partitions = self
            .decisions
            .current_find_partitions(&parent)
            .map_err(map_journal)?;
        if retained_partitions.len() != parent.partition_count {
            return Err(ServiceError::InvalidRequest);
        }
        for retained in retained_partitions {
            self.authorize(context)?;
            let ordinal = retained.ordinal();
            let evidence = authority
                .rebind_current_find_partition(&plan, &retained)
                .map_err(ServiceError::from)?;
            let part = authority
                .read_current_find_partition(evidence, context.deadline(), context.cancellation())
                .map_err(ServiceError::from)?;
            let completed = self
                .decisions
                .current_find_completion(&parent, ordinal)
                .map_err(map_journal)?
                .ok_or(ServiceError::InvalidRequest)?;
            let child = completed.dataset_job.as_ref().map(|job| DatasetJobInput {
                job_id: job.job_id,
                generation: job.generation,
            });
            self.validate_child(&part, child.as_ref(), training, context)
                .await?;
            if part
                .dataset()
                .map(|dataset| dataset.generation().manifest().content_hash().bytes())
                != completed.dataset_content_sha256
            {
                return Err(ServiceError::InvalidResult);
            }
            if let Some(job) = completed.dataset_job {
                dataset_jobs.push(job);
            }
            parts.push(part);
        }
        if dataset_jobs.is_empty() {
            return Err(ServiceError::Unavailable);
        }
        let admitted = self
            .decisions
            .prepare_current_find_screen_job(
                parts.into_boxed_slice(),
                parent.population.maximum_deep_analyses(),
                calendar,
                profile,
                Arc::clone(&self.research),
                captured_at,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_screen)?;
        self.authorize(context)?;
        self.decisions
            .retain_current_find_screen(
                &parent,
                dataset_jobs.into_boxed_slice(),
                &admitted,
                context,
            )
            .map_err(|error| map_journal_control(error, context))?;
        Ok(admitted)
    }

    async fn validate_partition_completion(
        &self,
        authority: &InstalledResearchDatasetPreparation,
        retained: &RetainedCurrentFindPartition,
        child: Option<&DatasetJobInput>,
        training: &super::training_preparation::InstalledProductTraining,
        context: &RequestContext,
    ) -> Result<Option<[u8; 32]>, ServiceError> {
        let dataset = authority
            .read_current_find_partition_dataset(
                retained,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(ServiceError::from)?;
        match (dataset, child) {
            (None, None) => Ok(None),
            (Some(dataset), Some(child)) => {
                let snapshot = training
                    .snapshot(&child.job_id.to_string(), child.generation, context)
                    .await?;
                validate_dataset_snapshot(&snapshot, &dataset)?;
                Ok(Some(dataset.generation().manifest().content_hash().bytes()))
            }
            _ => Err(ServiceError::InvalidRequest),
        }
    }

    async fn validate_child(
        &self,
        part: &CurrentFindScreenPartition,
        child: Option<&DatasetJobInput>,
        training: &super::training_preparation::InstalledProductTraining,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        match (part.dataset(), child) {
            (None, None) => Ok(()),
            (Some(dataset), Some(child)) => {
                let snapshot = training
                    .snapshot(&child.job_id.to_string(), child.generation, context)
                    .await?;
                validate_dataset_snapshot(&snapshot, dataset)
            }
            _ => Err(ServiceError::InvalidRequest),
        }
    }

    fn partition_value(
        &self,
        parent: &CurrentFindPreparationRecord,
        retained: &RetainedCurrentFindPartition,
    ) -> Result<Value, ServiceError> {
        let evidence = retained.evidence_reference();
        Ok(
            json!({"preparationId":parent.preparation_id,"preparationSha256":hex(parent.digest().map_err(map_journal)?),
            "ordinal":retained.ordinal(),"buildRequired":evidence.expected_build_spec().is_some(),
            "memberCount":evidence.partition().member_ids().len(),"unavailableCount":evidence.unavailable().len()}),
        )
    }

    fn preparation_value(
        &self,
        parent: &CurrentFindPreparationRecord,
    ) -> Result<Value, ServiceError> {
        let (coverage, available) = self.coverage_value(parent)?;
        Ok(
            json!({"preparationId":parent.preparation_id,"preparationSha256":hex(parent.digest().map_err(map_journal)?),
            "sourceCutoffUnixNanos":parent.analytical_cutoff.unix_nanos().to_string(),"forecastCohort":parent.forecast_cohort,"partitionCount":parent.partition_count,
            "populationCount":parent.population_count,"maximumCandidates":parent.population.maximum_deep_analyses(),
            "coverage":coverage,"availablePartitionCount":available,"coverageReference":reference_value(parent)?}),
        )
    }

    pub(super) fn coverage_value(
        &self,
        parent: &CurrentFindPreparationRecord,
    ) -> Result<(Value, usize), ServiceError> {
        let mut prepared = 0usize;
        let mut available = 0usize;
        let mut unavailable = 0usize;
        for retained in self
            .decisions
            .current_find_partitions(parent)
            .map_err(map_journal)?
        {
            prepared += 1;
            available += usize::from(
                retained
                    .evidence_reference()
                    .expected_build_spec()
                    .is_some(),
            );
            unavailable = unavailable
                .checked_add(retained.evidence_reference().unavailable().len())
                .ok_or(ServiceError::InvalidResult)?;
        }
        Ok((
            json!({"scope":"admitted_canonical_catalog","complete":prepared == parent.partition_count,
            "canonicalPopulationCount":parent.population.canonical_population_count(),"populationCount":parent.population_count,
            "excludedCount":parent.population.canonical_population_count().ok_or(ServiceError::InvalidResult)?.checked_sub(parent.population_count).ok_or(ServiceError::InvalidResult)?,"unavailableCount":unavailable,"preparedPartitionCount":prepared,"partitionCount":parent.partition_count}),
            available,
        ))
    }
}

fn validate_dataset_snapshot(
    snapshot: &JobSnapshot,
    dataset: &market_squawk_data::AnalyticalFeatureDataset,
) -> Result<(), ServiceError> {
    let spec = snapshot.spec();
    let result = snapshot
        .terminal_result()
        .ok_or(ServiceError::Unavailable)?;
    let manifest = dataset.generation().manifest();
    let build = dataset
        .generation()
        .build_spec_digest()
        .ok_or(ServiceError::InvalidResult)?
        .digest();
    if snapshot.state() != JobState::Completed
        || spec.kind().as_str() != "analysis.phase-one-feature-derived-generation-job.v1"
        || spec.input().authority().as_str() != "research.phase-one-derived-generation-request.v1"
        || spec.input().digest().bytes() != build.bytes()
        || spec.input().identity().as_str()
            != format!(
                "phase-one-build-v1:{}:{}",
                dataset.product_contract().identity(),
                manifest.dataset_id().as_str()
            )
        || spec.authority().authority().as_str()
            != "analysis.phase-one-feature-derived-generation.v1"
        || spec.authority().identity().as_str()
            != "analysis.phase-one-feature-derived-generation.v1"
        || result.authority().as_str() != "analysis.phase-one-feature-derived-generation.v1"
        || result.evidence_digest().bytes() != manifest.content_hash().bytes()
        || result.identity().as_str()
            != format!(
                "phase-one-derived-generation-{}",
                hex(manifest.content_hash().bytes())
            )
        || result.artifacts().len() != 1
        || result.artifacts()[0].media_type() != "application/json"
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}

fn reference_value(parent: &CurrentFindPreparationRecord) -> Result<Value, ServiceError> {
    Ok(
        json!({"preparationId":parent.preparation_id,"preparationSha256":hex(parent.digest().map_err(map_journal)?)}),
    )
}
fn hex(bytes: [u8; 32]) -> String {
    crate::application::model::forecast_preparation::hex(Sha256Digest::new(bytes))
}
fn parse_digest(value: &str) -> Result<[u8; 32], ServiceError> {
    super::super::jobs::parse_sha256(value).map(|value| value.bytes())
}
fn map_journal(error: crate::application::decision::DecisionApplicationError) -> ServiceError {
    super::super::decision::map_application(error)
}
fn map_screen(error: ScreenWorkflowError) -> ServiceError {
    match error {
        ScreenWorkflowError::InvalidRequest | ScreenWorkflowError::Conflict => {
            ServiceError::InvalidRequest
        }
        ScreenWorkflowError::NotFound => ServiceError::NotFound,
        ScreenWorkflowError::DatasetUnavailable => ServiceError::Unavailable,
        ScreenWorkflowError::Capacity => ServiceError::ResourceExhausted,
        ScreenWorkflowError::Application(error) => map_journal(error),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PrepareInput {
    financial_profile: AnalyticalProfileResolution,
    maximum_candidates: usize,
    benchmark_instrument_id: Option<market_squawk_domain::InstrumentId>,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PreparationReference {
    pub(super) preparation_id: Uuid,
    pub(super) preparation_sha256: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PartitionInput {
    preparation_id: Uuid,
    preparation_sha256: String,
    ordinal: usize,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatasetJobInput {
    job_id: Uuid,
    generation: u64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PreparePartitionInput {
    preparation_id: Uuid,
    preparation_sha256: String,
    ordinal: usize,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CompletePartitionInput {
    preparation_id: Uuid,
    preparation_sha256: String,
    ordinal: usize,
    dataset_job: Option<DatasetJobInput>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CompletionReadInput {
    preparation_id: Uuid,
    preparation_sha256: String,
    completion: CurrentFindCompletionReference,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScreenInput {
    preparation_id: Uuid,
    preparation_sha256: String,
    completion: CurrentFindCompletionReference,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScreenResultInput {
    preparation_id: Uuid,
    preparation_sha256: String,
    job_id: Uuid,
    generation: u64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CoverageInput {
    preparation_id: Uuid,
    preparation_sha256: String,
    ordinal: Option<usize>,
    offset: usize,
    limit: usize,
}

macro_rules! preparation_reference {
    ($($kind:ty),+ $(,)?) => {$(
        impl $kind { fn reference(&self) -> PreparationReference {
            PreparationReference { preparation_id: self.preparation_id, preparation_sha256: self.preparation_sha256.clone() }
        }}
    )+};
}
preparation_reference!(
    PartitionInput,
    PreparePartitionInput,
    CompletePartitionInput,
    CompletionReadInput,
    ScreenInput,
    ScreenResultInput,
    CoverageInput
);

fn map_calendar(
    error: crate::application::market_calendar::CompletedMarketSessionError,
) -> ServiceError {
    use crate::application::market_calendar::CompletedMarketSessionError as Error;
    match error {
        Error::InvalidRequest => ServiceError::InvalidRequest,
        Error::InvalidEvidence => ServiceError::InvalidResult,
        Error::ResourceBoundExceeded => ServiceError::ResourceExhausted,
        Error::Unavailable => ServiceError::Unavailable,
        Error::Cancelled => ServiceError::Cancelled,
        Error::DeadlineExceeded => ServiceError::DeadlineExceeded,
    }
}

fn map_journal_control(
    error: crate::application::decision::DecisionApplicationError,
    context: &RequestContext,
) -> ServiceError {
    match super::ensure_live(context) {
        Err(control) => control,
        Ok(()) => map_journal(error),
    }
}

fn completion_value(
    parent: &CurrentFindPreparationRecord,
    completed: &CurrentFindCompletionRecord,
) -> Result<Value, ServiceError> {
    let child = completed
        .dataset_job
        .as_ref()
        .map(|job| json!({"jobId": job.job_id, "generation": job.generation}));
    Ok(json!({"preparationId": parent.preparation_id,
        "preparationSha256": hex(parent.digest().map_err(map_journal)?),
        "completion": completed.reference().map_err(map_journal)?,
        "datasetJob": child, "datasetContentSha256": completed.dataset_content_sha256.map(hex)}))
}
