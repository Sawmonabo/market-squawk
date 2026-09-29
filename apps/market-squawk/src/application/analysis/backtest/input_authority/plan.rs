//! Durable, source-derived plan for the fixed three-fold recommendation study.
//!
//! A deserialized reference is a reopening recipe. Only `read`/`reopen` return the opaque plan;
//! their complete-history and benchmark reads re-admit its exact original source evidence.

use crate::{
    ResearchService,
    application::{
        analytical_profile::{AnalyticalProfileResolution, ValidatedAnalyticalProfile},
        research::{
            DatasetPreparationAuthority, PreparedFeatureDatasetBuild,
            RecommendationBenchmarkSelection, RecommendationBenchmarkSelectionReadCapability,
            RecommendationBenchmarkSelectionReference, RecommendationCohortPreparationRequest,
        },
    },
};
use market_squawk_backtesting::{
    RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1, RECOMMENDATION_OOS_FOLD_HORIZON_NANOS_V1,
    RECOMMENDATION_TARGET_HORIZON_NANOS_V1, RecommendationOosFoldV1,
    RecommendationSignalPlanMaterializerV1,
};
use market_squawk_data::{
    AnalyticalReadCapability, ChronologicalSplitPolicy, CompleteMarketBarHistoryOutput,
    DatasetBuildPurpose, DatasetStudyPolicy, DatasetTargetHorizon, FeatureDatasetProductContract,
    PythonDatasetSelection, Sha256Digest,
};
use market_squawk_domain::{HistoricalStudyBasis, InstrumentId, Timestamp};
use crate::application::research::corporate_actions::{SourceAppliedCorporateActionPlanReference, SourceAppliedCorporateActionReadCapability};
use market_squawk_jobs::{JobSnapshot, JobState};
use market_squawk_services::{RequestContext, ServiceError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeSet, sync::Arc, time::Duration};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct HistoricalStudyPlanReferenceV1 {
    version: u16,
    subject_instrument_id: InstrumentId,
    source_cutoff_unix_nanos: String,
    financial_profile: AnalyticalProfileResolution,
    benchmarks: RecommendationBenchmarkSelectionReference,
    source_action_reference: SourceAppliedCorporateActionPlanReference,
    // Exact source selections and original capture/completeness receipts, in subject/SPY/VTI order.
    sources: [[u8; 32]; 3],
    plan_digest: [u8; 32],
}
impl HistoricalStudyPlanReferenceV1 {
    pub(crate) const fn source_action_reference(&self) -> &SourceAppliedCorporateActionPlanReference {
        &self.source_action_reference
    }
    pub(crate) const fn financial_profile(&self) -> &AnalyticalProfileResolution {
        &self.financial_profile
    }
    pub(crate) fn source_cutoff(&self) -> Result<Timestamp, ServiceError> {
        let nanos = self
            .source_cutoff_unix_nanos
            .parse::<i64>()
            .map_err(|_| ServiceError::InvalidRequest)?;
        if nanos.to_string() != self.source_cutoff_unix_nanos {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Timestamp::from_unix_nanos(nanos))
    }
    pub(crate) const fn subject_instrument_id(&self) -> InstrumentId {
        self.subject_instrument_id
    }
}

/// Owns real immutable source output and the fixed role assignment. No public economic constructor.
pub(crate) struct HistoricalStudyPlanV1 {
    reference: HistoricalStudyPlanReferenceV1,
    profile: ValidatedAnalyticalProfile,
    benchmarks: RecommendationBenchmarkSelection,
    histories: [CompleteMarketBarHistoryOutput; 3],
    folds: [RecommendationOosFoldV1; 3],
    population_starts_at: Timestamp,
    population_ends_at: Timestamp,
    source_cutoff: Timestamp,
}

#[derive(Clone)]
pub(crate) struct HistoricalStudyPlanReadCapabilityV1 {
    reader: AnalyticalReadCapability,
    benchmarks: RecommendationBenchmarkSelectionReadCapability,
    datasets: Arc<DatasetPreparationAuthority>,
    actions: SourceAppliedCorporateActionReadCapability,
}
impl HistoricalStudyPlanReadCapabilityV1 {
    pub(crate) fn new(
        research: &ResearchService,
        benchmarks: RecommendationBenchmarkSelectionReadCapability,
        datasets: Arc<DatasetPreparationAuthority>,
        actions: SourceAppliedCorporateActionReadCapability,
    ) -> Self {
        Self {
            reader: research.analytical_reader(),
            benchmarks,
            datasets,
            actions,
        }
    }
    /// Reopens full accounting authority separately from the narrower price-feature plan.
    pub(crate) async fn reopen_accounting_actions(
        &self, plan: &HistoricalStudyPlanV1, context: &RequestContext,
    ) -> Result<market_squawk_data::CorporateActionPlan, ServiceError> {
        let histories = plan.histories.iter().collect::<Vec<_>>();
        let source = self.actions.read_reference_for_histories(
            &plan.reference.source_action_reference, &histories, context.deadline(),
            context.cancellation().clone(), None,
        ).await.map_err(|error| map_action_source(error, context))?.ok_or(ServiceError::Unavailable)?;
        source.into_covered_accounting_plan().map_err(|error| map_action_source(error, context))
    }
    pub(crate) async fn read(
        &self,
        subject: InstrumentId,
        cutoff: Timestamp,
        profile: ValidatedAnalyticalProfile,
        source_action_reference: &SourceAppliedCorporateActionPlanReference,
        context: &RequestContext,
    ) -> Result<Option<HistoricalStudyPlanV1>, ServiceError> {
        let Some(benchmarks) =
            self.benchmarks
                .select(cutoff, cutoff, context.deadline(), context.cancellation())?
        else {
            return Ok(None);
        };
        self.select(subject, cutoff, profile, benchmarks, source_action_reference, context)
            .await
    }
    pub(crate) async fn reopen(
        &self,
        reference: &HistoricalStudyPlanReferenceV1,
        profile: ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<Option<HistoricalStudyPlanV1>, ServiceError> {
        let cutoff = reference
            .source_cutoff_unix_nanos
            .parse::<i64>()
            .map_err(|_| ServiceError::InvalidRequest)?;
        if reference.version != 1
            || cutoff.to_string() != reference.source_cutoff_unix_nanos
            || profile.resolution() != &reference.financial_profile
            || reference.plan_digest == [0; 32]
        {
            return Err(ServiceError::InvalidRequest);
        }
        let Some(benchmarks) = self.benchmarks.read_reference(
            &reference.benchmarks,
            context.deadline(),
            context.cancellation(),
        )?
        else {
            return Ok(None);
        };
        let selected = self
            .select(
                reference.subject_instrument_id,
                Timestamp::from_unix_nanos(cutoff),
                profile,
                benchmarks,
                &reference.source_action_reference,
                context,
            )
            .await?;
        match selected {
            Some(plan) if plan.reference == *reference => Ok(Some(plan)),
            Some(_) => Err(ServiceError::InvalidRequest),
            None => Ok(None),
        }
    }
    async fn select(
        &self,
        subject: InstrumentId,
        cutoff: Timestamp,
        profile: ValidatedAnalyticalProfile,
        benchmarks: RecommendationBenchmarkSelection,
        source_action_reference: &SourceAppliedCorporateActionPlanReference,
        context: &RequestContext,
    ) -> Result<Option<HistoricalStudyPlanV1>, ServiceError> {
        if !profile
            .recommendation_policy()
            .parameters()
            .allow_retrospective_studies
            || profile.recommendation_policy().horizon_nanos()
                != RECOMMENDATION_TARGET_HORIZON_NANOS_V1
            || subject == benchmarks.primary().instrument_id()
            || subject == benchmarks.accompanying().instrument_id()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let instruments = [
            subject,
            benchmarks.primary().instrument_id(),
            benchmarks.accompanying().instrument_id(),
        ];
        if source_action_reference.knowledge_cutoff() != cutoff {
            return Err(ServiceError::InvalidRequest);
        }
        let Some(source) = self.actions.read_reference(source_action_reference, context.deadline(), context.cancellation().clone())
            .await.map_err(|error| map_action_source(error, context))? else { return Ok(None); };
        source.covered_price_plan().map_err(|error| map_action_source(error, context))?;
        let ordinary = source.ordinary_coverage().ok_or(ServiceError::Unavailable)?;
        let mut outputs = Vec::with_capacity(3);
        for instrument in instruments {
            let (original, _) = ordinary.reads().iter().find(|(read, _)| read.history().selection().receipt().instrument_id() == instrument)
                .ok_or(ServiceError::InvalidRequest)?;
            let original = original.history();
            let receipt = original.selection().receipt();
            let dates = receipt.date_windows().ok_or(ServiceError::Unavailable)?.requested_dates();
            let request = market_squawk_data::CompleteMarketBarHistoryRequest::try_exact_nominal(instrument,
                dates.0, dates.1, receipt.provider_instrument_id().clone(), receipt.venue_id().clone(),
                receipt.feed().clone(), receipt.interval().clone(), receipt.adjustment(), receipt.session_ruleset().clone(),
                cutoff, original.selection().pinned().manifest().clone())
                .and_then(|request| request.try_with_surface_requirement(original.selection().surface_requirement()))
                .map_err(|_| ServiceError::InvalidRequest)?;
            let Some(output) = self.reader.read_complete_market_bar_history(request,
                context.deadline(), context.cancellation().clone()).await.map_err(|_| ServiceError::Unavailable)?
            else { return Ok(None); };
            if output.selection().receipt().receipt_digest() != receipt.receipt_digest()
                || output.read_receipt().history_content_digest() != original.read_receipt().history_content_digest()
                || output.bars() != original.bars() {
                return Err(ServiceError::InvalidResult);
            }
            let (output, _) = self.datasets.rejoin_nominal_history(output, context.deadline(), context.cancellation())
                .await.map_err(ServiceError::from)?;
            if !output.selection().receipt().realized_outcome_eligible() || output.native_sessions().is_none() {
                return Ok(None);
            }
            outputs.push(output);
        }
        drop(source);
        let histories: [CompleteMarketBarHistoryOutput; 3] =
            outputs.try_into().map_err(|_| ServiceError::Internal)?;
        // Calendar membership determines the study, never observed returns, entry success or target maturity.
        let subject_sessions = histories[0]
            .native_sessions()
            .ok_or(ServiceError::Unavailable)?;
        let benchmark_sessions = histories[1]
            .native_sessions()
            .ok_or(ServiceError::Unavailable)?;
        let primary_closes: BTreeSet<_> = benchmark_sessions
            .sessions()
            .iter()
            .map(|s| s.closes_at_exclusive())
            .collect();
        let common: Vec<_> = subject_sessions
            .sessions()
            .iter()
            .map(|s| s.closes_at_exclusive())
            .filter(|at| *at <= cutoff && primary_closes.contains(at))
            .collect();
        let (Some(&population_start), Some(&last_close)) = (common.first(), common.last()) else {
            return Ok(None);
        };
        let evaluation_start = last_close
            .checked_sub_nanos(RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let minimum_train_end = evaluation_start
            .checked_sub_nanos(RECOMMENDATION_OOS_FOLD_HORIZON_NANOS_V1)
            .and_then(|v| v.checked_sub_nanos(1))
            .map_err(|_| ServiceError::InvalidRequest)?;
        // At least one full 365-day label fits before the first calibration boundary. Actual build
        // admission additionally requires nonempty, genuinely available train/calibration/test sets.
        if population_start
            .checked_add_nanos(RECOMMENDATION_TARGET_HORIZON_NANOS_V1)
            .map_err(|_| ServiceError::InvalidRequest)?
            >= minimum_train_end
        {
            return Ok(None);
        }
        let folds: [RecommendationOosFoldV1; 3] =
            RecommendationSignalPlanMaterializerV1::oos_folds(evaluation_start)
                .map_err(|_| ServiceError::InvalidRequest)?
                .try_into()
                .map_err(|_| ServiceError::Internal)?;
        let sources = histories
            .each_ref()
            .map(|history| history.selection().selection_digest().bytes());
        let mut reference = HistoricalStudyPlanReferenceV1 {
            version: 1,
            subject_instrument_id: subject,
            source_cutoff_unix_nanos: cutoff.unix_nanos().to_string(),
            financial_profile: profile.resolution().clone(),
            benchmarks: benchmarks.reference().clone(),
            source_action_reference: source_action_reference.clone(),
            sources,
            plan_digest: [0; 32],
        };
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/source-derived-historical-study-plan/v1\0");
        digest.update(serde_json::to_vec(&reference).map_err(|_| ServiceError::Internal)?);
        digest.update(population_start.unix_nanos().to_be_bytes());
        digest.update(last_close.unix_nanos().to_be_bytes());
        for history in &histories {
            let sessions = history.native_sessions().ok_or(ServiceError::Unavailable)?;
            digest.update(sessions.mapping_digest().bytes());
        }
        reference.plan_digest = digest.finalize().into();
        Ok(Some(HistoricalStudyPlanV1 {
            reference,
            profile,
            benchmarks,
            histories,
            folds,
            population_starts_at: population_start,
            population_ends_at: last_close,
            source_cutoff: cutoff,
        }))
    }
    pub(crate) async fn prepare_dataset(
        &self,
        plan: &HistoricalStudyPlanV1,
        part: HistoricalStudyDatasetPartV1,
        context: &RequestContext,
    ) -> Result<PreparedFeatureDatasetBuild, ServiceError> {
        let request = plan.dataset_request(part)?;
        self.datasets
            .prepare_recommendation_cohort(
                request,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(Into::into)
    }
    /// Rebuild the exact source-derived request and compare its immutable build identity against
    /// the already authenticated completed job and production-reopened Python dataset.
    pub(crate) async fn admit_training(
        &self,
        plan: &HistoricalStudyPlanV1,
        fold_index: usize,
        job: &JobSnapshot,
        selection: &PythonDatasetSelection,
        context: &RequestContext,
    ) -> Result<HistoricalFoldTrainingAuthorityV1, ServiceError> {
        let part = HistoricalStudyDatasetPartV1::Training(fold_index);
        let prepared = self.prepare_dataset(plan, part, context).await?;
        let (request, _) = prepared.into_parts();
        let digest = request.build_spec_digest().digest();
        if job.state()!=JobState::Completed || job.spec().input().digest().bytes()!=digest.bytes()
            || selection.identity().build_spec_digest().digest()!=digest
            || selection.study_policy().copied()!=Some(plan.study_policy(DatasetBuildPurpose::Training)?)
            || selection.split_policy()!=plan.split(fold_index)?
            || selection.product_contract()!=FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1 {
            return Err(ServiceError::InvalidRequest)
        }
        Ok(HistoricalFoldTrainingAuthorityV1 {
            fold: plan.folds[fold_index].clone(),
            profile: plan.profile.resolution().clone(),
            dataset_selection: selection.selection_sha256(),
            build_spec: digest,
            plan_digest: Sha256Digest::new(plan.reference.plan_digest),
        })
    }
}
#[derive(Clone, Copy)]
pub(crate) enum HistoricalStudyDatasetPartV1 {
    Training(usize),
    StudyInputs,
}
impl HistoricalStudyPlanV1 {
    pub(crate) const fn reference(&self) -> &HistoricalStudyPlanReferenceV1 {
        &self.reference
    }
    pub(crate) const fn profile(&self) -> &ValidatedAnalyticalProfile {
        &self.profile
    }
    pub(crate) const fn benchmarks(&self) -> &RecommendationBenchmarkSelection {
        &self.benchmarks
    }
    pub(crate) const fn folds(&self) -> &[RecommendationOosFoldV1; 3] {
        &self.folds
    }
    pub(crate) fn into_histories(self) -> [CompleteMarketBarHistoryOutput; 3] {
        self.histories
    }
    fn study_policy(
        &self,
        purpose: DatasetBuildPurpose,
    ) -> Result<DatasetStudyPolicy, ServiceError> {
        DatasetStudyPolicy::try_new(
            HistoricalStudyBasis::RetrospectiveFrozenSnapshot,
            purpose,
            self.source_cutoff,
            Some(Duration::ZERO),
            DatasetTargetHorizon::ExactElapsed(Duration::from_nanos(
                RECOMMENDATION_TARGET_HORIZON_NANOS_V1 as u64,
            )),
        )
        .map_err(|_| ServiceError::InvalidRequest)
    }
    fn split(&self, index: usize) -> Result<ChronologicalSplitPolicy, ServiceError> {
        let fold = self.folds.get(index).ok_or(ServiceError::InvalidRequest)?;
        let train_end = fold
            .starts_at()
            .checked_sub_nanos(RECOMMENDATION_OOS_FOLD_HORIZON_NANOS_V1)
            .and_then(|at| at.checked_sub_nanos(1))
            .map_err(|_| ServiceError::InvalidRequest)?;
        ChronologicalSplitPolicy::try_new(
            train_end,
            fold.starts_at()
                .checked_sub_nanos(1)
                .map_err(|_| ServiceError::InvalidRequest)?,
            fold.ends_at()
                .checked_sub_nanos(1)
                .map_err(|_| ServiceError::InvalidRequest)?,
        )
        .map_err(|_| ServiceError::InvalidRequest)
    }
    fn dataset_request(
        &self,
        part: HistoricalStudyDatasetPartV1,
    ) -> Result<RecommendationCohortPreparationRequest, ServiceError> {
        let (purpose, split) = match part {
            HistoricalStudyDatasetPartV1::Training(index) => {
                (DatasetBuildPurpose::Training, self.split(index)?)
            }
            HistoricalStudyDatasetPartV1::StudyInputs => {
                let first = self
                    .split(0)?
                    .timestamp_boundaries()
                    .ok_or(ServiceError::Internal)?;
                // Label-free inputs retain the inclusive source population through its last
                // genuine close. Signal issuance separately uses the half-open OOS window;
                // training folds keep their existing exclusive-end conversion and label purge.
                let final_end = self.population_ends_at;
                (
                    DatasetBuildPurpose::StudyInputs,
                    ChronologicalSplitPolicy::try_new(first[0], first[1], final_end)
                        .map_err(|_| ServiceError::InvalidRequest)?,
                )
            }
        };
        Ok(RecommendationCohortPreparationRequest {
            subject_instrument: self.reference.subject_instrument_id,
            subject_manifest: self.histories[0].selection().pinned().manifest().clone(),
            primary_benchmark_instrument: self.benchmarks.primary().instrument_id(),
            primary_benchmark_manifest: self.histories[1].selection().pinned().manifest().clone(),
            study: self.study_policy(purpose)?,
            population_starts_at: self.population_starts_at,
            population_ends_at: self.population_ends_at,
            source_action_reference: Some(self.reference.source_action_reference.clone()),
            split,
        })
    }
}

/// Non-deserializable distinct training role; only causal source-plan and completed-job admission
/// above constructs this value. The existing Exact live-price pin remains in the full profile.
pub(crate) struct HistoricalFoldTrainingAuthorityV1 {
    fold: RecommendationOosFoldV1,
    profile: AnalyticalProfileResolution,
    dataset_selection: Sha256Digest,
    build_spec: Sha256Digest,
    plan_digest: Sha256Digest,
}
impl HistoricalFoldTrainingAuthorityV1 {
    pub(crate) fn admits(
        &self,
        selection: &PythonDatasetSelection,
        profile: &ValidatedAnalyticalProfile,
    ) -> bool {
        selection.selection_sha256() == self.dataset_selection
            && selection.identity().build_spec_digest().digest() == self.build_spec
            && profile.resolution() == &self.profile
            && self.plan_digest.bytes() != [0; 32]
    }
    pub(crate) const fn fold(&self) -> &RecommendationOosFoldV1 {
        &self.fold
    }
    pub(crate) const fn plan_digest(&self) -> Sha256Digest {
        self.plan_digest
    }
}

fn map_action_source(error: crate::application::research::corporate_actions::ApplicableActionPlanError, context: &RequestContext) -> ServiceError {
    if context.cancellation().is_cancelled() { return ServiceError::Cancelled; }
    if std::time::Instant::now() >= context.deadline() { return ServiceError::DeadlineExceeded; }
    use crate::application::research::corporate_actions::ApplicableActionPlanError as Error;
    match error {
        Error::SourceRead(error) => error,
        Error::InvalidEvidence => ServiceError::InvalidResult,
        Error::Interrupted => ServiceError::Cancelled,
        Error::IncompleteOrdinaryCoverage | Error::UnresolvedApplicableActions => ServiceError::Unavailable,
    }
}
