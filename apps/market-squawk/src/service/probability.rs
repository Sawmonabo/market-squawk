//! Three independent event recipes over original source cohorts and the existing dataset jobs.
//! Persisted plans are reopening coordinates, never publication or financial authority.

use super::{InstalledProductTraining, forecast_preparation::InstalledForecastPreparation};
use crate::{
    LocalProduct, ResearchService,
    application::{
        DatasetPreparationAuthority, InstrumentContextOutcome, InstrumentContextReadCapability,
        InstrumentContextRequest, PreparedFeatureDatasetBuild, PreparedProbabilityDatasetPair,
        ProbabilityBenchmarkSource, ProbabilityCohortPreparationRequest,
        ProbabilitySubjectInputRequest, RecommendationBenchmarkSelectionReadCapability,
        SelectedRecommendationBenchmark, SourceAppliedCorporateActionPlanReference,
        SourceAppliedCorporateActionReadCapability,
        analysis::ProductionGovernedBacktestInputAuthority,
        analytical_profile::{AnalyticalProfileResolution, ValidatedAnalyticalProfile},
        decision::{DecisionApplication, current_find::member::FindMemberContext},
        market_calendar::CompletedMarketSessionReadCapability,
        model::forecast_preparation::ForecastCurrentFeatureInputSelection,
    },
    jobs::InstalledJobAuthority,
};
use market_squawk_data::{
    AnalyticalFeatureDataset, ChronologicalSplitPolicy, CompleteMarketBarHistoryCursor,
    DatasetBuildPurpose, DatasetStudyPolicy, DatasetTargetHorizon, FeatureDatasetInputEpochOutput,
    FeatureDatasetProductContract, ProbabilityEventTarget, QueryLimits,
};
use market_squawk_domain::{AccountId, HistoricalStudyBasis, InstrumentId, Timestamp};
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest as _, Sha256};
mod outcome;

use std::{sync::Arc, time::Duration};

pub(super) const PREPARE_PROBABILITY_EVENT: &str = "Analysis.PrepareProbabilityEvent";
pub(super) const START_PROBABILITY_DATASET: &str = "Analysis.StartProbabilityDataset";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum EventKind {
    PriceHigher,
    BenchmarkOutperformance,
    ProfitAfterCosts,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EventRequest {
    instrument_id: InstrumentId,
    source_cutoff_unix_nanos: String,
    financial_profile: AnalyticalProfileResolution,
    event_kind: EventKind,
    benchmark_instrument_id: Option<InstrumentId>,
    source_action_reference: SourceAppliedCorporateActionPlanReference,
    find_member: Option<FindMemberContext>,
    selection_token: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JobReference {
    job_id: String,
    generation: u64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PrepareInput {
    instrument_id: InstrumentId,
    source_cutoff_unix_nanos: String,
    financial_profile: AnalyticalProfileResolution,
    event_kind: EventKind,
    benchmark_instrument_id: Option<InstrumentId>,
    source_action_reference: SourceAppliedCorporateActionPlanReference,
    subject_dataset_job: Option<JobReference>,
    find_member: Option<FindMemberContext>,
    selection_token: Option<String>,
}
impl PrepareInput {
    fn request(&self) -> EventRequest {
        EventRequest {
            instrument_id: self.instrument_id,
            source_cutoff_unix_nanos: self.source_cutoff_unix_nanos.clone(),
            financial_profile: self.financial_profile.clone(),
            event_kind: self.event_kind,
            benchmark_instrument_id: self.benchmark_instrument_id,
            source_action_reference: self.source_action_reference.clone(),
            find_member: self.find_member.clone(),
            selection_token: self.selection_token.clone(),
        }
    }
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProbabilityPlan {
    version: u16,
    request: EventRequest,
    event: ProbabilityEventTarget,
    source_selections: Vec<[u8; 32]>,
    population_start: Timestamp,
    population_end: Timestamp,
    split_ends: [Timestamp; 3],
    digest: [u8; 32],
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum DatasetPart {
    SubjectInputs,
    Training,
    Analysis,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatasetInput {
    plan: ProbabilityPlan,
    part: DatasetPart,
    subject_dataset_job: Option<JobReference>,
}
struct OpenPlan {
    reference: ProbabilityPlan,
    profile: ValidatedAnalyticalProfile,
    histories: Vec<CompleteMarketBarHistoryCursor>,
    benchmark: Option<SelectedRecommendationBenchmark>,
    cutoff: Timestamp,
}

pub(super) struct InstalledProbabilityPreparation {
    outcome: Option<outcome::OutcomePublication>,
    datasets: Arc<DatasetPreparationAuthority>,
    research: Arc<ResearchService>,
    decisions: Arc<DecisionApplication>,
    identities: Option<InstrumentContextReadCapability>,
    benchmarks: RecommendationBenchmarkSelectionReadCapability,
    actions: SourceAppliedCorporateActionReadCapability,
    training: InstalledProductTraining,
    inputs: Arc<ProductionGovernedBacktestInputAuthority>,
}
impl InstalledProbabilityPreparation {
    pub(super) fn new(
        product: &LocalProduct,
        jobs: &InstalledJobAuthority,
        datasets: Arc<DatasetPreparationAuthority>,
    ) -> Self {
        let research = product.research();
        Self {
            outcome: None,
            datasets,
            decisions: product.decisions(),
            identities: product.instrument_context_read_capability(),
            benchmarks: RecommendationBenchmarkSelectionReadCapability::new(
                research.market_data_instruments(),
            ),
            actions: SourceAppliedCorporateActionReadCapability::new(
                Arc::clone(&research),
                CompletedMarketSessionReadCapability::new(
                    Arc::clone(&research),
                    product.market_runtime(),
                ),
            )
            .with_artifact_repository(product.artifacts()),
            training: InstalledProductTraining::new(product, jobs),
            inputs: product.backtest_inputs(),
            research,
        }
    }
    pub(super) async fn prepare(
        &self,
        forecasts: &InstalledForecastPreparation,
        request: &TypedToolRequest,
        account: Option<AccountId>,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        context.origin().ok_or(ServiceError::Unauthorized)?;
        let input: PrepareInput = decode(request)?;
        let parsed = input
            .source_cutoff_unix_nanos
            .parse::<i64>()
            .map_err(|_| ServiceError::InvalidRequest)?;
        if parsed.to_string() != input.source_cutoff_unix_nanos {
            return Err(ServiceError::InvalidRequest);
        }
        let (request, _) = match self
            .original_request(input.request(), Timestamp::from_unix_nanos(parsed), context)
            .await
        {
            Ok(value) => value,
            Err(ServiceError::Unavailable | ServiceError::NotFound) => {
                return result(
                    response(&input, None, None, Some("source_evidence_unavailable")),
                    context,
                );
            }
            Err(error) => return Err(error),
        };
        if request.source_action_reference.knowledge_cutoff() != Timestamp::from_unix_nanos(parsed)
        {
            return result(
                response(&input, None, None, Some("source_clock_mismatch")),
                context,
            );
        }
        let selected = self.select(request, forecasts, context).await;
        let value = match selected {
            Ok(plan) => {
                let evaluated = if let Some(job) = &input.subject_dataset_job {
                    self.open_subject(&plan, job, context).await
                } else {
                    return match self.prepare_subject(&plan, context).await {
                        Ok(_) => result(response(&input, Some(&plan), None, None), context),
                        Err(ServiceError::Unavailable | ServiceError::NotFound) => result(
                            response(
                                &input,
                                Some(&plan),
                                None,
                                Some("subject_inputs_unavailable"),
                            ),
                            context,
                        ),
                        Err(error) => Err(error),
                    };
                };
                match evaluated {
                    Ok((dataset, epochs)) => {
                        let current = match current_input(&plan, &dataset, &epochs) {
                            Ok(value) => value,
                            Err(ServiceError::Unavailable) => {
                                return result(
                                    response(
                                        &input,
                                        Some(&plan),
                                        None,
                                        Some("current_features_unavailable"),
                                    ),
                                    context,
                                );
                            }
                            Err(error) => return Err(error),
                        };
                        match self.prepare_pair(&plan, dataset, account, context).await {
                            Ok(pair) => {
                                let c = &pair.coverage;
                                let complete = c.complete_classes.map(|v| v[0] + v[1]);
                                let reason = if c.complete_classes[0].contains(&0)
                                    || c.complete_classes[1].contains(&0)
                                    || complete[0] <= c.feature_count
                                    || complete[1] < 2
                                    || complete[2] < 2
                                {
                                    Some("insufficient_calibration")
                                } else {
                                    None
                                };
                                let mut value =
                                    response(&input, Some(&plan), Some(current), reason);
                                value["coverage"] =
                                    serde_json::to_value(c).map_err(|_| ServiceError::Internal)?;
                                value
                            }
                            Err(ServiceError::Unavailable | ServiceError::NotFound) => response(
                                &input,
                                Some(&plan),
                                Some(current),
                                Some("insufficient_event_evidence"),
                            ),
                            Err(error) => return Err(error),
                        }
                    }
                    Err(ServiceError::Unavailable | ServiceError::NotFound) => response(
                        &input,
                        Some(&plan),
                        None,
                        Some("subject_inputs_unavailable"),
                    ),
                    Err(error) => return Err(error),
                }
            }
            Err(ServiceError::Unavailable | ServiceError::NotFound) => {
                response(&input, None, None, Some("source_evidence_unavailable"))
            }
            Err(error) => return Err(error),
        };
        result(value, context)
    }
    pub(super) async fn prepare_dataset(
        &self,
        forecasts: &InstalledForecastPreparation,
        request: &TypedToolRequest,
        account: Option<AccountId>,
        context: &RequestContext,
    ) -> Result<PreparedFeatureDatasetBuild, ServiceError> {
        context.origin().ok_or(ServiceError::Unauthorized)?;
        let input: DatasetInput = decode(request)?;
        if input.plan.version != 1 || input.plan.digest == [0; 32] {
            return Err(ServiceError::InvalidRequest);
        }
        let plan = self
            .select(input.plan.request.clone(), forecasts, context)
            .await?;
        if plan.reference != input.plan {
            return Err(ServiceError::InvalidRequest);
        }
        match input.part {
            DatasetPart::SubjectInputs if input.subject_dataset_job.is_none() => {
                self.prepare_subject(&plan, context).await
            }
            DatasetPart::SubjectInputs => Err(ServiceError::InvalidRequest),
            part => {
                let (dataset, _) = self
                    .open_subject(
                        &plan,
                        input
                            .subject_dataset_job
                            .as_ref()
                            .ok_or(ServiceError::InvalidRequest)?,
                        context,
                    )
                    .await?;
                let pair = self.prepare_pair(&plan, dataset, account, context).await?;
                let c = &pair.coverage;
                if c.complete_classes[0].contains(&0)
                    || c.complete_classes[1].contains(&0)
                    || c.complete_classes[0].iter().sum::<usize>() <= c.feature_count
                    || c.complete_classes[1].iter().sum::<usize>() < 2
                    || c.complete_classes[2].iter().sum::<usize>() < 2
                {
                    return Err(ServiceError::Unavailable);
                }
                match part {
                    DatasetPart::Training => Ok(pair.training),
                    DatasetPart::Analysis => Ok(pair.analysis),
                    DatasetPart::SubjectInputs => unreachable!(),
                }
            }
        }
    }
    async fn original_request(
        &self,
        mut request: EventRequest,
        cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<(EventRequest, Option<InstrumentId>), ServiceError> {
        match (&request.find_member, &request.selection_token) {
            (None, None) => Ok((request, None)),
            (Some(member), Some(token)) => {
                let admitted = self.decisions.admit_find_member(
                    member,
                    &request.financial_profile,
                    token,
                    context,
                )?;
                if admitted.instrument_id() != request.instrument_id
                    || admitted.source_cutoff() != cutoff
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let (source, selected_benchmark) = self
                    .decisions
                    .find_member_source_reference(
                        &admitted,
                        request.benchmark_instrument_id,
                        context,
                    )?
                    .ok_or(ServiceError::Unavailable)?;
                if source.knowledge_cutoff() != cutoff {
                    return Err(ServiceError::InvalidResult);
                }
                request.source_action_reference = source;
                Ok((request, selected_benchmark))
            }
            _ => Err(ServiceError::InvalidRequest),
        }
    }
    async fn select(
        &self,
        request: EventRequest,
        forecasts: &InstalledForecastPreparation,
        context: &RequestContext,
    ) -> Result<OpenPlan, ServiceError> {
        let nanos = request
            .source_cutoff_unix_nanos
            .parse::<i64>()
            .map_err(|_| ServiceError::InvalidRequest)?;
        if nanos.to_string() != request.source_cutoff_unix_nanos {
            return Err(ServiceError::InvalidRequest);
        }
        let cutoff = Timestamp::from_unix_nanos(nanos);
        let (request, original_benchmark) = self.original_request(request, cutoff, context).await?;
        if request.source_action_reference.knowledge_cutoff() != cutoff {
            return Err(ServiceError::InvalidRequest);
        }
        let profile = forecasts
            .revalidate_profile(&request.financial_profile, context)
            .await?;
        if !profile
            .recommendation_policy()
            .parameters()
            .allow_retrospective_studies
        {
            return Err(ServiceError::Unavailable);
        }
        let identities = self.identities.as_ref().ok_or(ServiceError::Unavailable)?;
        let identity = identities
            .read(
                InstrumentContextRequest::try_new(request.instrument_id, cutoff, cutoff)
                    .map_err(|_| ServiceError::InvalidRequest)?,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(super::market_evidence::map_identity_error)?;
        let InstrumentContextOutcome::Exact(identity) = identity.outcome() else {
            return Err(ServiceError::Unavailable);
        };
        if !profile.admits_investment(identity.asset_class(), identity.exchange_traded_fund()) {
            return Err(ServiceError::Unavailable);
        }
        let benchmark = if request.event_kind == EventKind::BenchmarkOutperformance {
            // Find has already selected and retained its comparison before source acquisition.
            // Reopen that exact canonical identity; original absence remains unavailable. Keep
            // the requested/default identity unchanged in the saved event request.
            let requested_benchmark = if request.find_member.is_some() {
                Some(original_benchmark.ok_or(ServiceError::Unavailable)?)
            } else {
                request.benchmark_instrument_id
            };
            let selected = self
                .benchmarks
                .select_comparison(
                    requested_benchmark,
                    cutoff,
                    cutoff,
                    context.deadline(),
                    context.cancellation(),
                )?
                .ok_or(ServiceError::Unavailable)?;
            if selected.instrument_id() == request.instrument_id {
                return Err(ServiceError::Unavailable);
            }
            Some(selected)
        } else {
            None
        };
        let mut histories = Vec::with_capacity(2);
        for instrument in std::iter::once(request.instrument_id)
            .chain(benchmark.as_ref().map(|v| v.instrument_id()))
        {
            let output = self
                .actions
                .read_history_reference(
                    &request.source_action_reference,
                    instrument,
                    context.deadline(),
                    context.cancellation().clone(),
                    None,
                )
                .await
                .map_err(|error| source_error(error, context))?
                .ok_or(ServiceError::Unavailable)?;
            if output.read_receipt().knowledge_cutoff() != cutoff
                || output.selection().receipt().instrument_id() != instrument
                || !output.selection().receipt().realized_outcome_eligible()
                || output.native_sessions().is_none()
            {
                return Err(ServiceError::InvalidResult);
            }
            histories.push(output);
        }
        self.actions
            .read_price_reference_for_histories(
                &request.source_action_reference,
                &histories.iter().collect::<Vec<_>>(),
                context.deadline(),
                context.cancellation().clone(),
                None,
            )
            .await
            .map_err(|error| source_error(error, context))?
            .ok_or(ServiceError::Unavailable)?;
        let sessions = histories[0]
            .native_sessions()
            .ok_or(ServiceError::Unavailable)?;
        let mut first = None;
        let mut last = None;
        for session in sessions.sessions().iter() {
            let close = session
                .map_err(crate::application::map_source_analytical_error)?
                .closes_at_exclusive();
            if close <= cutoff {
                if first.is_none() {
                    first = Some(close);
                } else {
                    last = Some(close);
                }
            }
        }
        let first = first.ok_or(ServiceError::Unavailable)?;
        let last = last.ok_or(ServiceError::Unavailable)?;
        let span = last
            .unix_nanos()
            .checked_sub(first.unix_nanos())
            .filter(|v| *v > 0)
            .ok_or(ServiceError::Unavailable)?;
        // Fixed chronological 60/20/20 windows, selected from original calendar coverage before labels.
        let train = first
            .checked_add_nanos(span / 5 * 3)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let validation = first
            .checked_add_nanos(span / 5 * 4)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let test = last
            .checked_sub_nanos(1)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let split = [train, validation, test];
        ChronologicalSplitPolicy::try_new(train, validation, test)
            .map_err(|_| ServiceError::Unavailable)?;
        let currency = histories[0]
            .bars()
            .next()
            .transpose()
            .map_err(crate::application::map_source_analytical_error)?
            .ok_or(ServiceError::Unavailable)?
            .currency();
        for history in &histories {
            for bar in history.bars() {
                if bar
                    .map_err(crate::application::map_source_analytical_error)?
                    .currency()
                    != currency
                {
                    return Err(ServiceError::Unavailable);
                }
            }
        }
        let event = match request.event_kind {
            EventKind::PriceHigher => ProbabilityEventTarget::PriceHigher,
            EventKind::BenchmarkOutperformance => {
                let b = benchmark.as_ref().ok_or(ServiceError::Internal)?;
                ProbabilityEventTarget::BenchmarkOutperformance {
                    benchmark_instrument_id: b.instrument_id(),
                    benchmark_definition: b.reference_revision_digest(),
                }
            }
            EventKind::ProfitAfterCosts => ProbabilityEventTarget::ProfitAfterCosts {
                policy: self
                    .inputs
                    .probability_cost_policy(currency, profile.execution_assumptions())?,
            },
        };
        let mut reference = ProbabilityPlan {
            version: 1,
            request,
            event,
            source_selections: histories
                .iter()
                .map(|v| v.selection().selection_digest().bytes())
                .collect(),
            population_start: first,
            population_end: last,
            split_ends: split,
            digest: [0; 32],
        };
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/original-probability-plan/v1\0");
        digest.update(serde_json::to_vec(&reference).map_err(|_| ServiceError::Internal)?);
        for history in &histories {
            digest.update(
                history
                    .native_sessions()
                    .ok_or(ServiceError::Unavailable)?
                    .mapping_digest()
                    .bytes(),
            );
        }
        reference.digest = digest.finalize().into();
        Ok(OpenPlan {
            reference,
            profile,
            histories,
            benchmark,
            cutoff,
        })
    }
    async fn prepare_subject(
        &self,
        plan: &OpenPlan,
        context: &RequestContext,
    ) -> Result<PreparedFeatureDatasetBuild, ServiceError> {
        self.datasets
            .prepare_probability_subject_inputs(
                ProbabilitySubjectInputRequest {
                    event: plan.reference.event,
                    subject_instrument: plan.reference.request.instrument_id,
                    subject_manifest: plan.histories[0].selection().pinned().manifest().clone(),
                    study: study(plan, DatasetBuildPurpose::StudyInputs)?,
                    population_starts_at: plan.reference.population_start,
                    population_ends_at: plan.reference.population_end,
                    split: ChronologicalSplitPolicy::try_new(
                        plan.reference.split_ends[0],
                        plan.reference.split_ends[1],
                        plan.reference.population_end,
                    )
                    .map_err(|_| ServiceError::InvalidRequest)?,
                    source_action_reference: Some(
                        plan.reference.request.source_action_reference.clone(),
                    ),
                },
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(ServiceError::from)
    }
    async fn open_subject(
        &self,
        plan: &OpenPlan,
        job: &JobReference,
        context: &RequestContext,
    ) -> Result<(AnalyticalFeatureDataset, FeatureDatasetInputEpochOutput), ServiceError> {
        let snapshot = self
            .training
            .snapshot(&job.job_id, job.generation, context)
            .await?;
        let selected = self
            .training
            .reopen_prepared_dataset(
                &snapshot,
                Some(
                    FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1,
                ),
                context,
            )
            .await?;
        let (expected, _) = self.prepare_subject(plan, context).await?.into_parts();
        if selected.identity().build_spec_digest() != expected.build_spec_digest()
            || snapshot.spec().input().digest().bytes()
                != expected.build_spec_digest().digest().bytes()
        {
            return Err(ServiceError::InvalidResult);
        }
        let reader = self.research.analytical_reader();
        let dataset = reader
            .feature_dataset_for_build(
                selected.product_contract(),
                selected.identity().manifest().dataset_id(),
                selected.identity().build_spec_digest(),
                context.deadline(),
                context.cancellation(),
            )
            .map_err(crate::application::map_source_analytical_error)?
            .ok_or(ServiceError::Unavailable)?;
        if dataset.generation().manifest() != selected.identity().manifest() {
            return Err(ServiceError::InvalidResult);
        }
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
        .map_err(|_| ServiceError::Internal)?;
        let epochs = reader
            .feature_dataset_input_epochs(
                dataset.product_contract(),
                dataset.generation().manifest(),
                limits,
                context.deadline(),
                context.cancellation().child_token(),
            )
            .await
            .map_err(crate::application::map_source_analytical_error)?;
        Ok((dataset, epochs))
    }
    async fn prepare_pair(
        &self,
        plan: &OpenPlan,
        dataset: AnalyticalFeatureDataset,
        account: Option<AccountId>,
        context: &RequestContext,
    ) -> Result<PreparedProbabilityDatasetPair, ServiceError> {
        let costs =
            if let ProbabilityEventTarget::ProfitAfterCosts { policy } = plan.reference.event {
                let source = self
                    .actions
                    .read_reference_for_histories(
                        &plan.reference.request.source_action_reference,
                        &[&plan.histories[0]],
                        context.deadline(),
                        context.cancellation().clone(),
                        None,
                    )
                    .await
                    .map_err(|error| source_error(error, context))?
                    .ok_or(ServiceError::Unavailable)?;
                let actions = source
                    .into_covered_accounting_plan()
                    .map_err(|error| source_error(error, context))?;
                Some(Arc::new(
                    self.inputs
                        .prepare_probability_evaluation(
                            dataset,
                            &plan.histories[0],
                            actions,
                            plan.reference.request.source_action_reference.clone(),
                            policy,
                            account.ok_or(ServiceError::Unavailable)?,
                            context,
                        )
                        .await?,
                ))
            } else {
                None
            };
        let benchmark = plan
            .benchmark
            .clone()
            .map(|selection| ProbabilityBenchmarkSource {
                selection,
                manifest: plan.histories[1].selection().pinned().manifest().clone(),
            });
        self.datasets
            .prepare_probability_cohort(
                ProbabilityCohortPreparationRequest {
                    subject_instrument: plan.reference.request.instrument_id,
                    subject_manifest: plan.histories[0].selection().pinned().manifest().clone(),
                    benchmark,
                    event: plan.reference.event,
                    costs,
                    study: study(plan, DatasetBuildPurpose::Training)?,
                    population_starts_at: plan.reference.population_start,
                    population_ends_at: plan.reference.population_end,
                    split: split(plan)?,
                    source_action_reference: Some(
                        plan.reference.request.source_action_reference.clone(),
                    ),
                },
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(ServiceError::from)
    }
}
fn study(
    plan: &OpenPlan,
    purpose: DatasetBuildPurpose,
) -> Result<DatasetStudyPolicy, ServiceError> {
    let horizon = u64::try_from(plan.profile.recommendation_policy().horizon_nanos())
        .map_err(|_| ServiceError::InvalidRequest)?;
    DatasetStudyPolicy::try_new(
        HistoricalStudyBasis::RetrospectiveFrozenSnapshot,
        purpose,
        plan.cutoff,
        Some(Duration::ZERO),
        DatasetTargetHorizon::ExactElapsed(Duration::from_nanos(horizon)),
    )
    .map_err(|_| ServiceError::InvalidRequest)
}
fn split(plan: &OpenPlan) -> Result<ChronologicalSplitPolicy, ServiceError> {
    let [a, b, c] = plan.reference.split_ends;
    ChronologicalSplitPolicy::try_new(a, b, c).map_err(|_| ServiceError::InvalidRequest)
}
fn current_input(
    plan: &OpenPlan,
    dataset: &AnalyticalFeatureDataset,
    epochs: &FeatureDatasetInputEpochOutput,
) -> Result<ForecastCurrentFeatureInputSelection, ServiceError> {
    let (index, current) = epochs
        .epochs()
        .iter()
        .enumerate()
        .filter(|(_, e)| {
            e.instrument_id() == plan.reference.request.instrument_id
                && e.source_selection_as_of() <= plan.cutoff
                && e.decision_at().is_some_and(|at| at <= plan.cutoff)
        })
        .max_by_key(|(_, e)| e.target_origin())
        .ok_or(ServiceError::Unavailable)?;
    let coordinate = epochs
        .coordinate(index)
        .ok_or(ServiceError::InvalidResult)?;
    if coordinate.rows().iter().any(|row| {
        matches!(
            row.value(),
            market_squawk_data::ForecastFeatureValue::Missing
        )
    }) {
        return Err(ServiceError::Unavailable);
    }
    if current
        .target_origin()
        .is_none_or(|at| at <= plan.reference.split_ends[2])
    {
        return Err(ServiceError::InvalidResult);
    }
    ForecastCurrentFeatureInputSelection::try_new(
        dataset.generation().manifest().clone(),
        current.example_id(),
    )
    .map_err(|_| ServiceError::InvalidResult)
}
fn response(
    input: &PrepareInput,
    plan: Option<&OpenPlan>,
    current: Option<ForecastCurrentFeatureInputSelection>,
    reason: Option<&str>,
) -> serde_json::Value {
    json!({"eventKind":input.event_kind,"availability":{"state":if reason.is_some(){"unavailable"}else{"ready"},"reason":reason},
        "plan":plan.map(|p|&p.reference),"event":plan.map(|p|p.reference.event),"currentFeatureInput":current,
        "instrumentId":input.instrument_id,"sourceCutoffUnixNanos":input.source_cutoff_unix_nanos,
        "financialProfileDigest":input.financial_profile.configuration_digest,"coverage":null})
}
fn result(
    value: serde_json::Value,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    TypedToolResult::try_new(
        value,
        1,
        ToolResultMetadata::complete_not_applicable(),
        context.limits(),
    )
    .map_err(Into::into)
}
fn decode<T: serde::de::DeserializeOwned>(request: &TypedToolRequest) -> Result<T, ServiceError> {
    serde_json::from_value(serde_json::Value::Object(super::business_arguments(
        request.arguments(),
    )))
    .map_err(|_| ServiceError::InvalidRequest)
}
fn source_error(
    error: crate::application::ApplicableActionPlanError,
    context: &RequestContext,
) -> ServiceError {
    if context.cancellation().is_cancelled() {
        return ServiceError::Cancelled;
    }
    if std::time::Instant::now() >= context.deadline() {
        return ServiceError::DeadlineExceeded;
    }
    use crate::application::ApplicableActionPlanError as E;
    match error {
        E::SourceRead(error) => error,
        E::InvalidEvidence => ServiceError::InvalidResult,
        E::Interrupted => ServiceError::Cancelled,
        E::IncompleteOrdinaryCoverage | E::UnresolvedApplicableActions => ServiceError::Unavailable,
    }
}
