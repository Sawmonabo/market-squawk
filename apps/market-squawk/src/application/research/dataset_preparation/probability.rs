//! Original source cohorts for the three separately calibrated binary events.

use std::{sync::Arc, time::Instant};
use market_squawk_backtesting::{AllOriginRoundTripDispositionV1, AllOriginRoundTripEvaluationV1};
use market_squawk_data::{ChronologicalSplitPolicy, DatasetBuildPurpose, DatasetManifestRef,
    DatasetStudyPolicy, FeatureDatasetProductContract, ProbabilityCostOutcomeAttestation,
    ProbabilityEventTarget};
use market_squawk_domain::{InstrumentId, Timestamp};
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use super::{DatasetPreparationAuthority, DatasetPreparationError, DatasetPreparationUse,
    PreparedFeatureDatasetBuild, check_control, history::CohortPreparationRequest};
use crate::application::research::{benchmark_selection::SelectedRecommendationBenchmark,
    corporate_actions::SourceAppliedCorporateActionPlanReference};

/// Original canonical definition and the exact admitted history chosen before outcomes.
#[derive(Clone, Debug)]
pub(crate) struct ProbabilityBenchmarkSource {
    pub(crate) selection: SelectedRecommendationBenchmark,
    pub(crate) manifest: DatasetManifestRef,
}

/// One independently available event. Missing comparison evidence cannot disable the other two.
#[derive(Clone, Debug)]
pub(crate) struct ProbabilityCohortPreparationRequest {
    pub(crate) subject_instrument: InstrumentId,
    pub(crate) subject_manifest: DatasetManifestRef,
    pub(crate) benchmark: Option<ProbabilityBenchmarkSource>,
    pub(crate) event: ProbabilityEventTarget,
    pub(crate) costs: Option<Arc<AllOriginRoundTripEvaluationV1>>,
    pub(crate) study: DatasetStudyPolicy,
    pub(crate) population_starts_at: Timestamp,
    pub(crate) population_ends_at: Timestamp,
    pub(crate) split: ChronologicalSplitPolicy,
    pub(crate) source_action_reference: Option<SourceAppliedCorporateActionPlanReference>,
}

/// Label-free subject population retained before the simulator inspects any outcome.
#[derive(Clone, Debug)]
pub(crate) struct ProbabilitySubjectInputRequest {
    pub(crate) event: ProbabilityEventTarget,
    pub(crate) subject_instrument: InstrumentId,
    pub(crate) subject_manifest: DatasetManifestRef,
    pub(crate) study: DatasetStudyPolicy,
    pub(crate) population_starts_at: Timestamp,
    pub(crate) population_ends_at: Timestamp,
    pub(crate) split: ChronologicalSplitPolicy,
    pub(crate) source_action_reference: Option<SourceAppliedCorporateActionPlanReference>,
}

/// Counts cover the predeclared subject population, not only successful fills or labels.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProbabilityCohortCoverage {
    pub(crate) original_origins: usize,
    pub(crate) outside_partitions: usize,
    pub(crate) missing_subject_terminal: usize,
    pub(crate) missing_benchmark: usize,
    pub(crate) unavailable_cost_outcome: usize,
    pub(crate) boundary_censored: usize,
    pub(crate) retained_examples: usize,
    pub(crate) split_counts: [usize; 3],
    /// Original complete-case false/true counts in train/calibration/test order.
    pub(crate) complete_classes: [[usize; 2]; 3],
    pub(crate) missing_features: usize,
    pub(crate) feature_count: usize,
}

/// Same original examples and source policy, two independently authorized output uses.
pub(crate) struct PreparedProbabilityDatasetPair {
    pub(crate) training: PreparedFeatureDatasetBuild,
    pub(crate) analysis: PreparedFeatureDatasetBuild,
    pub(crate) coverage: ProbabilityCohortCoverage,
}

impl DatasetPreparationAuthority {
    pub(crate) async fn prepare_probability_subject_inputs(
        &self, request: ProbabilitySubjectInputRequest, deadline: Instant, cancellation: CancellationToken,
    ) -> Result<PreparedFeatureDatasetBuild, DatasetPreparationError> {
        if request.study.purpose() != DatasetBuildPurpose::StudyInputs {
            return Err(DatasetPreparationError::InvalidSelection);
        }
        let cohort = self.prepare_source_cohort(CohortPreparationRequest {
            subject_instrument: request.subject_instrument, subject_manifest: request.subject_manifest,
            benchmark: None, event: None, probability_subject: Some(request.event), costs: None, study: request.study,
            population_starts_at: request.population_starts_at, population_ends_at: request.population_ends_at,
            split: request.split, source_action_reference: request.source_action_reference,
        }, deadline, &cancellation).await?;
        self.finalize_source_cohort(&cohort, DatasetPreparationUse::LocalAnalysis,
            FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1, deadline, &cancellation)
    }

    pub(crate) async fn prepare_probability_cohort(
        &self, request: ProbabilityCohortPreparationRequest, deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedProbabilityDatasetPair, DatasetPreparationError> {
        check_control(deadline, &cancellation)?;
        request.event.validate().map_err(|_| DatasetPreparationError::InvalidSelection)?;
        if request.study.purpose() != DatasetBuildPurpose::Training {
            return Err(DatasetPreparationError::InvalidSelection);
        }
        match (&request.event, &request.benchmark, &request.costs) {
            (ProbabilityEventTarget::PriceHigher, None, None) => {}
            (ProbabilityEventTarget::BenchmarkOutperformance { benchmark_instrument_id, benchmark_definition }, Some(source), None)
                if source.selection.instrument_id() == *benchmark_instrument_id
                    && source.selection.reference_revision_digest() == *benchmark_definition
                    && *benchmark_instrument_id != request.subject_instrument => {}
            (ProbabilityEventTarget::ProfitAfterCosts { policy }, None, Some(evaluation))
                if evaluation.policy().target_policy() == *policy
                    && evaluation.study_qualification().basis() == request.study.basis()
                    && evaluation.study_qualification().snapshot_as_of() == request.study.snapshot_as_of()
                    && request.study.target_horizon().exact_elapsed().is_some_and(|horizon|
                        u128::try_from(evaluation.policy().horizon_nanos()).ok() == Some(horizon.as_nanos())) => {}
            _ => return Err(DatasetPreparationError::InvalidSelection),
        }
        let event = request.event;
        let cohort = self.prepare_source_cohort(CohortPreparationRequest {
            subject_instrument: request.subject_instrument, subject_manifest: request.subject_manifest,
            benchmark: request.benchmark.map(|value| (value.selection.instrument_id(), value.manifest)),
            study: request.study, population_starts_at: request.population_starts_at,
            population_ends_at: request.population_ends_at, split: request.split,
            source_action_reference: request.source_action_reference,
            event: Some(event), probability_subject: None, costs: request.costs,
        }, deadline, &cancellation).await?;
        let training = self.finalize_source_cohort(&cohort, DatasetPreparationUse::Train,
            event_contract(event, DatasetPreparationUse::Train), deadline, &cancellation)?;
        let analysis = self.finalize_source_cohort(&cohort, DatasetPreparationUse::LocalAnalysis,
            event_contract(event, DatasetPreparationUse::LocalAnalysis), deadline, &cancellation)?;
        training.request.retained_bytes().checked_add(analysis.request.retained_bytes())
            .filter(|bytes| *bytes <= super::MAXIMUM_RECEIPT_BYTES)
            .ok_or(DatasetPreparationError::Capacity)?;
        Ok(PreparedProbabilityDatasetPair { training, analysis, coverage: cohort.coverage })
    }
}

pub(super) const fn event_contract(event: ProbabilityEventTarget, use_case: DatasetPreparationUse) -> FeatureDatasetProductContract {
    use FeatureDatasetProductContract as Contract;
    match (event, use_case) {
        (ProbabilityEventTarget::PriceHigher, DatasetPreparationUse::Train) => Contract::PriceReturnMacroContextFixedHorizonPriceHigherTrainingV1,
        (ProbabilityEventTarget::PriceHigher, DatasetPreparationUse::LocalAnalysis) => Contract::PriceReturnMacroContextFixedHorizonPriceHigherAnalysisV1,
        (ProbabilityEventTarget::BenchmarkOutperformance { .. }, DatasetPreparationUse::Train) => Contract::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceTrainingV1,
        (ProbabilityEventTarget::BenchmarkOutperformance { .. }, DatasetPreparationUse::LocalAnalysis) => Contract::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceAnalysisV1,
        (ProbabilityEventTarget::ProfitAfterCosts { .. }, DatasetPreparationUse::Train) => Contract::PriceReturnMacroContextFixedHorizonProfitAfterCostsTrainingV1,
        (ProbabilityEventTarget::ProfitAfterCosts { .. }, DatasetPreparationUse::LocalAnalysis) => Contract::PriceReturnMacroContextFixedHorizonProfitAfterCostsAnalysisV1,
    }
}

/// This is the only application conversion to the data owner's inert cost DTO. Every fact is
/// copied from an opaque simulator result, not caller-authored labels or synthetic receipts.
pub(super) fn cost_attestation(
    evaluation: &AllOriginRoundTripEvaluationV1, instrument: InstrumentId,
    origin: Timestamp, target: Timestamp, decision: Timestamp, source_cutoff: Timestamp,
) -> Result<Option<ProbabilityCostOutcomeAttestation>, DatasetPreparationError> {
    let mut rows = evaluation.results().iter().filter(|value|
        value.instrument_id() == instrument && value.target_origin() == origin);
    let row = rows.next().ok_or(DatasetPreparationError::InvalidEvidence)?;
    if rows.next().is_some() || row.target_at() != target || row.decision_at() != decision
        || row.source_selection_as_of() != source_cutoff {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    let AllOriginRoundTripDispositionV1::Completed(outcome) = row.disposition() else { return Ok(None); };
    Ok(Some(ProbabilityCostOutcomeAttestation {
        instrument_id: instrument, origin, target_at: target, decision_at: decision,
        source_selection_as_of: source_cutoff, label_available_at: row.label_available_at(),
        source_manifest: row.source_manifest().clone(), cohort_manifest: evaluation.dataset_manifest().clone(),
        source_epoch_digest: row.source_epoch_digest(), source_lineage_digest: row.source_lineage_digest(),
        cohort_digest: evaluation.cohort_digest(), evaluation_digest: evaluation.digest(), outcome_digest: row.digest(),
        action_content_digest: evaluation.action_content_digest(), action_audit_digest: evaluation.action_audit_digest(),
        policy: evaluation.policy().target_policy(), net_total_return: outcome.cost_adjusted_total_return(),
    }))
}
