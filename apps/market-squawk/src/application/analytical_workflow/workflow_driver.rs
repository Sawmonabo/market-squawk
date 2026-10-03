//! The shared native bounded default analysis sequence. Every calculation remains a service operation.
//!
//! There is one active workflow and one admitted child job at a time. A submission is journalled
//! before transport, and an uncertain acknowledgement is reconciled before any further admission.
//! Retained result bodies are references/checkpoints only; consumers reopen their service owners.

use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use market_squawk_services::RequestId;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    AnalyticalControllerResponse, AnalyticalWorkflowController, PendingCapabilityInvocation,
    ServiceJobReference, ServiceResultReference, WorkflowCheckpoint, WorkflowCheckpointStage,
    WorkflowError, WorkflowKind, WorkflowProfileBinding, WorkflowRun, WorkflowRunState,
    financial_profiles, hex_digest, opaque_workflow_token, unix_nanos_now, valid_digest,
    valid_market_selection_token, workflow_control, workflow_presentation,
};
use crate::application::analytical_workflow::host::{
    InvocationAuthority, WorkflowGeneration, WorkflowState, invoke_analytical_operation,
};

mod find;
mod historical;
mod probability;

pub use find::CoverageCursor;
pub(super) use find::{CoverageReason, read_coverage};
use find::{
    FindMemberUnavailable, FindState, compact_completed_partition, completion_receipt,
    current_find_member, find_publication_arguments, validate_find_result,
};
use historical::{
    HistoricalFiscalProgress, apply_historical_origin, apply_historical_page,
    check_historical_page_budget, compact_historical_frontier, historical_completion_arguments,
    historical_plan_arguments, historical_study_receipt, historical_target_arguments,
    retain_historical_page, retain_historical_target, revalidate_historical_frontier,
};

const PREPARATION_RESULT: &str = "Market.GetInvestmentEvidencePreparationResult";

const MAXIMUM_RECEIPTS: usize = 64;
const MAXIMUM_RECEIPT_BYTES: usize = 128 * 1024;
const MAXIMUM_RETAINED_RECEIPT_BYTES: usize = 2 * 1024 * 1024;
const MAXIMUM_FISCAL_TARGETS: usize = 9;
const MAXIMUM_UNAVAILABLE_RECEIPTS: usize = 7 * 32;

fn maximum_receipt_bytes(operation: &str) -> usize {
    if matches!(
        operation,
        "Market.PrepareInvestmentEvidence" | PREPARATION_RESULT
    ) {
        market_squawk_decisions::MAX_INVESTMENT_ANALYSIS_REQUEST_BYTES
    } else {
        MAXIMUM_RECEIPT_BYTES
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Step {
    ResolveProfile,
    FindPopulation,
    FindPartition,
    FindDataset,
    FindCompletePartition,
    FindReadPreparation,
    FindScreen,
    PrepareSelection,
    SelectMarket,
    PriceDataset,
    PriceTraining,
    PriceInputs,
    PreparePriceForecast,
    PriceForecast,
    ProbabilityPlan,
    ProbabilitySubject,
    ProbabilityPrepared,
    ProbabilityTrainingDataset,
    ProbabilityAnalysisDataset,
    ProbabilityTraining,
    PrepareProbabilityForecast,
    ProbabilityForecast,
    FiscalPlan,
    FiscalTrainingDataset,
    FiscalTraining,
    FiscalInputDataset,
    FiscalForecast,
    HistoricalStudy,
    StudyTrainingDataset,
    StudyTraining,
    StudyInputs,
    StudyFiscalPage,
    StudyFiscalOrigin,
    StudyFiscalTrainingDataset,
    StudyFiscalTraining,
    StudyFiscalInputDataset,
    StudyFiscalCompletePage,
    StudyBacktest,
    FinalPrepare,
    FinalEvidence,
    FinalPortfolio,
    Publish,
    ReadPublished,
    FindPublish,
    FindReadPublished,
    Finished,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct Receipt {
    operation: String,
    arguments: Map<String, Value>,
    body: Value,
    sha256: String,
}

impl Receipt {
    fn preparation(&self) -> Result<&Value, WorkflowError> {
        if self.operation != PREPARATION_RESULT {
            return Err(WorkflowError::internal());
        }
        preparation_arguments(&self.body)?;
        let job = self.body.get("job").ok_or_else(WorkflowError::internal)?;
        let child = workflow_control::job_reference(job)?;
        if job.get("state").and_then(Value::as_str) != Some("completed")
            || self.arguments != job_arguments(&child)?
        {
            return Err(WorkflowError::internal());
        }
        self.body
            .get("preparation")
            .filter(|value| value.is_object())
            .ok_or_else(WorkflowError::internal)
    }

    fn valid(&self) -> bool {
        super::valid_identifier(&self.operation, 128)
            && valid_digest(&self.sha256)
            && serde_json::to_vec(&self.body).is_ok_and(|bytes| {
                bytes.len() <= maximum_receipt_bytes(&self.operation)
                    && hex_digest(Sha256::digest(bytes)) == self.sha256
            })
            && serde_json::to_vec(&self.arguments).is_ok_and(|bytes| {
                bytes.len()
                    <= if self.operation == "Decision.GenerateInvestmentAnalysis" {
                        market_squawk_decisions::MAX_INVESTMENT_ANALYSIS_REQUEST_BYTES
                    } else {
                        64 * 1024
                    }
            })
            && (self.operation != PREPARATION_RESULT || self.preparation().is_ok())
            && (self.operation != "Model.PrepareInvestmentForecast"
                || price_preparation(self).is_ok())
            && (self.operation != "Analysis.GetHistoricalStudyPlan"
                || historical_study_receipt(self).is_ok())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ActiveJob {
    step: Step,
    result_operation: String,
    reference: ServiceJobReference,
    observed_sequence: u64,
    result_arguments: Map<String, Value>,
    /// Original business arguments must match the completed preparation, not just its own hash.
    preparation_arguments: Option<Map<String, Value>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct FiscalTarget {
    target: Value,
    availability: FiscalAvailability,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum FiscalAvailability {
    Ready { reason: () },
    Unavailable { reason: FiscalUnavailableReason },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum FiscalUnavailableReason {
    RequiredFiscalHistoryUnavailable,
    CommonEquityCashFlowSourceUnavailable,
    SourcePopulationUnavailable,
    RetrospectiveStudiesDisabled,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum PriceForecastAvailability {
    Ready {
        reason: (),
    },
    Unavailable {
        reason: PriceForecastUnavailableReason,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum PriceForecastUnavailableReason {
    CompatibleForecastSelectionUnavailable,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PriceForecastPreparation {
    instrument_id: Uuid,
    availability: PriceForecastAvailability,
    forecast: Option<Value>,
    request_sha256: Option<String>,
    financial_profile_digest: String,
    source_cutoff_unix_nanos: String,
    forecast_cohort: Option<Value>,
    expected_observed_through_unix_nanos: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct DriverState {
    step: Step,
    selection_token: Option<String>,
    benchmark_instrument_id: Option<Uuid>,
    instrument_id: Option<Uuid>,
    source_cutoff: Option<String>,
    receipts: BTreeMap<String, Receipt>,
    fiscal_targets: Vec<FiscalTarget>,
    fiscal_index: usize,
    probability_index: usize,
    study_fold: usize,
    historical_fiscal: Option<HistoricalFiscalProgress>,
    find: Option<FindState>,
    completed_outcomes: Vec<String>,
    completed_analyses: Vec<Value>,
    completed_unavailable_receipts: Vec<Receipt>,
    active_job: Option<ActiveJob>,
    /// Exact immutable backend reads are replayed before a resumed sequence advances.
    revalidate_after: Option<String>,
    revalidating: bool,
}

impl DriverState {
    fn new(
        _kind: WorkflowKind,
        selection_token: Option<String>,
        benchmark_instrument_id: Option<Uuid>,
    ) -> Self {
        Self {
            step: Step::ResolveProfile,
            selection_token,
            benchmark_instrument_id,
            instrument_id: None,
            source_cutoff: None,
            receipts: BTreeMap::new(),
            fiscal_targets: Vec::new(),
            fiscal_index: 0,
            probability_index: 0,
            study_fold: 0,
            historical_fiscal: None,
            find: None,
            completed_outcomes: Vec::new(),
            completed_analyses: Vec::new(),
            completed_unavailable_receipts: Vec::new(),
            active_job: None,
            revalidate_after: None,
            revalidating: false,
        }
    }

    pub(super) fn valid(&self) -> bool {
        self.selection_token
            .as_deref()
            .is_none_or(valid_market_selection_token)
            && self.benchmark_instrument_id.is_none_or(|id| !id.is_nil())
            && self.instrument_id.is_none_or(|id| !id.is_nil())
            && self
                .source_cutoff
                .as_deref()
                .is_none_or(super::valid_timestamp)
            && self.receipts.len() <= MAXIMUM_RECEIPTS
            && serde_json::to_vec(&self.receipts)
                .is_ok_and(|bytes| bytes.len() <= MAXIMUM_RETAINED_RECEIPT_BYTES)
            && self
                .receipts
                .iter()
                .all(|(key, receipt)| key.len() <= 96 && !key.is_empty() && receipt.valid())
            && self
                .receipts
                .get(&self.key(Step::HistoricalStudy))
                .is_none_or(|study| {
                    self.initial_source_action_reference()
                        .is_ok_and(|reference| {
                            study.arguments.get("sourceActionReference") == Some(reference)
                        })
                })
            && self
                .receipts
                .get(&self.key(Step::PreparePriceForecast))
                .is_none_or(|price| {
                    self.price_forecast_cutoff().is_ok_and(|cutoff| {
                        price
                            .arguments
                            .get("sourceCutoffUnixNanos")
                            .and_then(Value::as_str)
                            == Some(cutoff)
                    }) && self.find.as_ref().is_none_or(|find| {
                        price.arguments.get("forecastCohort")
                            == find.preparation.get("forecastCohort")
                            && price.arguments.get("currentFeatureInput")
                                == find
                                    .candidates
                                    .get(find.candidate_index)
                                    .and_then(|candidate| candidate.get("currentFeatureInput"))
                    })
                })
            && self.probability_index <= 3
            && probability::valid(self)
            && self.fiscal_targets.len() <= MAXIMUM_FISCAL_TARGETS
            && self.fiscal_index <= self.fiscal_targets.len()
            && self.study_fold <= 3
            && self
                .historical_fiscal
                .as_ref()
                .is_none_or(|fiscal| fiscal.valid(self))
            && self.completed_outcomes.len() <= 32
            && self.completed_analyses.len() <= 32
            && self.completed_unavailable_receipts.len() <= MAXIMUM_UNAVAILABLE_RECEIPTS
            && self
                .completed_unavailable_receipts
                .iter()
                .all(Receipt::valid)
            && serde_json::to_vec(&self.completed_unavailable_receipts)
                .is_ok_and(|bytes| bytes.len() <= 1024 * 1024)
            && self
                .completed_outcomes
                .iter()
                .all(|kind| matches!(kind.as_str(), "action" | "abstain" | "unavailable"))
            && self.find.as_ref().is_none_or(|find| {
                find.partition_count <= 65_536
                    && find.partition_index <= find.partition_count
                    && find
                        .pending_dataset_job
                        .as_ref()
                        .is_none_or(super::valid_service_job_reference)
                    && find.completed_partition.as_ref().is_none_or(|receipt| {
                        receipt.valid()
                            && completion_receipt(
                                receipt,
                                &find.preparation,
                                find.partition_index.checked_sub(1),
                            )
                            .is_ok()
                    })
                    && (find.completed_partition.is_some() == (find.partition_index > 0))
                    && find.candidates.len() <= 32
                    && find.candidate_index <= find.candidates.len()
                    && find.population_count <= 65_536
                    && find.population_count <= find.canonical_population_count
                    && find.canonical_population_count <= 65_536
            })
            && self.active_job.as_ref().is_none_or(|job| {
                super::valid_service_job_reference(&job.reference)
                    && match &job.preparation_arguments {
                        Some(arguments) => {
                            job.result_operation == PREPARATION_RESULT
                                && matches!(job.step, Step::PrepareSelection | Step::FinalPrepare)
                                && !arguments.contains_key("confirm")
                                && !arguments.contains_key("resultLimits")
                                && serde_json::to_vec(arguments)
                                    .is_ok_and(|bytes| bytes.len() <= 64 * 1024)
                        }
                        None => job.result_operation != PREPARATION_RESULT,
                    }
            })
    }

    pub(super) fn result_ordering(&self) -> Option<&'static str> {
        match self
            .receipts
            .get("FindReadPublished")?
            .body
            .get("ordering")?
            .as_str()?
        {
            "estimated_gain_descending" => Some("estimated_gain_descending"),
            "unavailable_incomparable_horizons" => Some("unavailable_incomparable_horizons"),
            _ => None,
        }
    }

    fn key(&self, step: Step) -> String {
        if probability::is_step(step) {
            format!("{step:?}-{}", self.probability_index)
        } else if matches!(
            step,
            Step::FiscalTrainingDataset
                | Step::FiscalTraining
                | Step::FiscalInputDataset
                | Step::FiscalForecast
        ) {
            format!("{step:?}-{}", self.fiscal_index)
        } else if matches!(step, Step::StudyTrainingDataset | Step::StudyTraining) {
            format!("{step:?}-{}", self.study_fold)
        } else {
            format!("{step:?}")
        }
    }

    fn receipt(&self, step: Step) -> Result<&Receipt, WorkflowError> {
        self.receipts
            .get(&self.key(step))
            .ok_or_else(WorkflowError::internal)
    }

    fn price_forecast_cutoff(&self) -> Result<&str, WorkflowError> {
        match &self.find {
            Some(find) => find
                .preparation
                .get("sourceCutoffUnixNanos")
                .and_then(Value::as_str)
                .filter(|value| super::valid_timestamp(value))
                .ok_or_else(WorkflowError::internal),
            None => self
                .source_cutoff
                .as_deref()
                .ok_or_else(WorkflowError::internal),
        }
    }

    fn initial_source_action_reference(&self) -> Result<&Value, WorkflowError> {
        self.receipt(Step::PrepareSelection)?
            .preparation()?
            .get("sourceActionReference")
            .filter(|value| value.is_object())
            .ok_or_else(|| {
                WorkflowError::new(
                    "analysis_source_actions_unavailable",
                    "The original source-action evidence is unavailable. Start a fresh analysis.",
                )
            })
    }

    fn profile(run: &WorkflowRun) -> Result<Value, WorkflowError> {
        serde_json::to_value(
            run.profile
                .financial_resolution
                .as_ref()
                .ok_or_else(WorkflowError::internal)?,
        )
        .map_err(|_| WorkflowError::internal())
    }

    fn input(&self, run: &WorkflowRun) -> Result<Value, WorkflowError> {
        Ok(
            json!({"instrumentId": self.instrument_id.ok_or_else(WorkflowError::internal)?,
            "sourceCutoffUnixNanos": self.source_cutoff.as_ref().ok_or_else(WorkflowError::internal)?,
            "financialProfile": Self::profile(run)?}),
        )
    }

    fn find_preparation(&self) -> Result<Value, WorkflowError> {
        let find = self.find.as_ref().ok_or_else(WorkflowError::internal)?;
        Ok(
            json!({"preparationId": find.preparation.get("preparationId").ok_or_else(WorkflowError::internal)?,
        "preparationSha256": find.preparation.get("preparationSha256").ok_or_else(WorkflowError::internal)?}),
        )
    }
}

impl AnalyticalWorkflowController {
    fn begin_workflow(
        &self,
        kind: WorkflowKind,
        selection: Option<String>,
        benchmark_instrument_id: Option<Uuid>,
        origin: super::host::WorkflowOrigin,
    ) -> Result<AnalyticalControllerResponse, WorkflowError> {
        if benchmark_instrument_id.is_some_and(|id| id.is_nil())
            || (kind == WorkflowKind::AnalyzeInvestment) != selection.is_some()
            || selection
                .as_deref()
                .is_some_and(|token| !valid_market_selection_token(token))
        {
            return Err(WorkflowError::invalid_request(
                "Choose an investment before starting its analysis.",
            ));
        }
        self.mutate(|document, now| {
            if document.workflow_runs.iter().any(|run| {
                matches!(
                    run.state,
                    WorkflowRunState::Queued
                        | WorkflowRunState::Running
                        | WorkflowRunState::WaitingForServiceJob
                        | WorkflowRunState::Paused
                        | WorkflowRunState::Cancelling
                )
            }) {
                return Err(WorkflowError::new(
                    "analysis_busy",
                    "Finish or stop the current analysis before starting another.",
                ));
            }
            if document.workflow_runs.len() == super::MAXIMUM_WORKFLOW_RUNS {
                return Err(WorkflowError::new(
                    "analysis_capacity",
                    "Saved analysis activity has reached its limit.",
                ));
            }
            let profile = document.profile(document.active_profile.profile_id)?;
            let run = WorkflowRun {
                origin,
                run_id: Uuid::new_v4(),
                schema_version: super::CONTROLLER_FORMAT_VERSION,
                owner_workspace_id: self.owner_workspace_id,
                kind,
                state: WorkflowRunState::Queued,
                target_selection_token: selection.clone(),
                profile: WorkflowProfileBinding {
                    active: document.active_profile.clone(),
                    config: profile.config.clone(),
                    financial_resolution: None,
                },
                created_at: now.clone(),
                updated_at: now.clone(),
                deadline_at: None,
                checkpoint_journal: vec![WorkflowCheckpoint {
                    sequence: 1,
                    stage: WorkflowCheckpointStage::Created,
                    recorded_at: now,
                    child_job: None,
                    result: None,
                }],
                child_jobs: Vec::new(),
                pending_invocation: None,
                result_references: Vec::new(),
                completion_reference: None,
                coverage_receipt: None,
                exclusion_receipt: None,
                ranking_receipt: None,
                execution_eligibility: super::ExecutionEligibility::ExecutionIneligible,
                last_error: None,
                driver: Some(DriverState::new(kind, selection, benchmark_instrument_id)),
            };
            let response = AnalyticalControllerResponse::Workflow {
                workflow: workflow_presentation(&run)?,
            };
            document.workflow_runs.push(run);
            Ok(response)
        })
    }

    fn resume_workflow(&self, token: &str) -> Result<AnalyticalControllerResponse, WorkflowError> {
        self.mutate(|document, now| {
            let run = workflow_control::find_workflow_mut(document, token)?;
            if run.state != WorkflowRunState::Paused
                || run
                    .deadline_at
                    .as_deref()
                    .is_some_and(|deadline| now.parse::<u64>().ok() >= deadline.parse::<u64>().ok())
            {
                return Err(WorkflowError::invalid_request(
                    "This analysis can no longer resume. Start a new analysis.",
                ));
            }
            if run.last_error.as_deref() == Some("analysis_preparation_required") {
                let driver = run.driver.as_ref().ok_or_else(WorkflowError::internal)?;
                let receipt = driver.receipt(driver.step)?.clone();
                if run.pending_invocation.is_some()
                    || driver.active_job.is_some()
                    || !preparation_requires_retry(run, &receipt)?
                {
                    return Err(WorkflowError::internal());
                }
                let child = job_from_receipt(&receipt)?;
                if !run.child_jobs.iter().any(|retained| {
                    retained.job_id == child.job_id
                        && retained.generation == child.generation
                        && retained.terminal_sequence == child.terminal_sequence
                        && retained.result.as_ref().is_some_and(|result| {
                            result.operation == PREPARATION_RESULT
                                && result.content_sha256 == receipt.sha256
                        })
                }) {
                    return Err(WorkflowError::internal());
                }
                let driver = run.driver.as_mut().ok_or_else(WorkflowError::internal)?;
                if driver.completed_unavailable_receipts.len() == MAXIMUM_UNAVAILABLE_RECEIPTS {
                    return Err(WorkflowError::internal());
                }
                // A confirmed resume admits a new attempt; the immutable failed preparation and
                // its terminal child remain evidence. Earlier analytical cutoffs never change.
                driver.completed_unavailable_receipts.push(receipt);
                driver.receipts.remove(&driver.key(driver.step));
            }
            let driver = run.driver.as_mut().ok_or_else(WorkflowError::internal)?;
            let refresh_current = matches!(driver.step, Step::FinalEvidence | Step::FinalPortfolio);
            if refresh_current {
                // Refresh only the reads. Completed preparation and an uncertain job admission
                // retain their original custody and cutoff across resumption.
                for key in ["FinalEvidence", "FinalPortfolio"] {
                    driver.receipts.remove(key);
                }
                driver.step = Step::FinalEvidence;
                run.pending_invocation = None;
            }
            driver.revalidating = true;
            driver.revalidate_after = None;
            if let Some(fiscal) = &mut driver.historical_fiscal {
                fiscal.revalidate_index = 0;
            }
            run.state = WorkflowRunState::Queued;
            run.last_error = None;
            if refresh_current {
                append_checkpoint(
                    run,
                    &now,
                    WorkflowCheckpointStage::CapabilityCompleted,
                    None,
                    None,
                )?;
            }
            run.updated_at = now;
            Ok(AnalyticalControllerResponse::Workflow {
                workflow: workflow_presentation(run)?,
            })
        })
    }

    fn next_run(&self) -> Result<Option<WorkflowRun>, WorkflowError> {
        Ok(self
            .lock_document()?
            .workflow_runs
            .iter()
            .find(|run| {
                matches!(
                    run.state,
                    WorkflowRunState::Queued
                        | WorkflowRunState::Running
                        | WorkflowRunState::WaitingForServiceJob
                        | WorkflowRunState::Cancelling
                )
            })
            .cloned())
    }

    fn retain_pending(
        &self,
        token: &str,
        operation: &'static str,
        arguments: Map<String, Value>,
    ) -> Result<PendingCapabilityInvocation, WorkflowError> {
        self.mutate(|document, now| {
            let run = workflow_control::find_workflow_mut(document, token)?;
            if run.pending_invocation.is_some() || run.state == WorkflowRunState::Cancelling {
                return Err(WorkflowError::internal());
            }
            let bytes = serde_json::to_vec(&arguments).map_err(|_| WorkflowError::internal())?;
            let pending = PendingCapabilityInvocation {
                request_id: format!(
                    "desktop-analysis-{}-{}",
                    run.run_id.simple(),
                    Uuid::new_v4().simple()
                ),
                operation: operation.to_owned(),
                arguments,
                arguments_sha256: hex_digest(Sha256::digest(bytes)),
            };
            if !super::valid_pending_invocation(&pending) {
                return Err(WorkflowError::internal());
            }
            run.pending_invocation = Some(pending.clone());
            run.state = WorkflowRunState::Running;
            run.updated_at = now;
            Ok(pending)
        })
    }

    fn retain_response(
        &self,
        token: &str,
        expected: &PendingCapabilityInvocation,
        body: Value,
    ) -> Result<(), WorkflowError> {
        self.mutate(|document, now| {
            let run = workflow_control::find_workflow_mut(document, token)?;
            if run.pending_invocation.as_ref() != Some(expected) {
                return Err(WorkflowError::internal());
            }
            if expected.operation == "Analysis.CompleteHistoricalStudyFiscalPage" {
                retain_historical_page(run, expected, body)?;
                compact_historical_frontier(run, &now)?;
                append_checkpoint(
                    run,
                    &now,
                    WorkflowCheckpointStage::CapabilityCompleted,
                    None,
                    None,
                )?;
                run.pending_invocation = None;
                if run.state != WorkflowRunState::Cancelling {
                    run.state = WorkflowRunState::Running;
                }
                run.updated_at = now;
                return Ok(());
            }
            let driver = run.driver.as_mut().ok_or_else(WorkflowError::internal)?;
            if let Some(result_operation) = job_result_operation(&expected.operation) {
                let child = workflow_control::job_reference(&body)?;
                let sequence = workflow_control::validate_child(&body, &child)?;
                if run.child_jobs.len() == super::MAXIMUM_CHILD_REFERENCES_PER_RUN {
                    return Err(WorkflowError::internal());
                }
                run.child_jobs.push(child.clone());
                let mut result_arguments = job_arguments(&child)?;
                if expected.operation == "Decision.StartCurrentScreen" {
                    result_arguments.extend(object(driver.find_preparation()?)?);
                }
                driver.active_job = Some(ActiveJob {
                    step: driver.step,
                    result_operation: result_operation.to_owned(),
                    reference: child.clone(),
                    observed_sequence: sequence,
                    result_arguments,
                    preparation_arguments: (expected.operation
                        == "Market.PrepareInvestmentEvidence")
                        .then(|| preparation_business_arguments(&expected.arguments)),
                });
                append_checkpoint(
                    run,
                    &now,
                    WorkflowCheckpointStage::WaitingForServiceJob,
                    Some(child),
                    None,
                )?;
                if run.state != WorkflowRunState::Cancelling {
                    run.state = WorkflowRunState::WaitingForServiceJob;
                }
            } else {
                let body = checkpoint_body(&expected.operation, body)?;
                let bytes = serde_json::to_vec(&body).map_err(|_| WorkflowError::internal())?;
                if bytes.len() > maximum_receipt_bytes(&expected.operation) {
                    return Err(WorkflowError::internal());
                }
                let sha256 = hex_digest(Sha256::digest(bytes));
                let receipt = Receipt {
                    operation: expected.operation.clone(),
                    arguments: expected.arguments.clone(),
                    body,
                    sha256: sha256.clone(),
                };
                apply_receipt(run, receipt, &now)?;
                if expected.operation == "Analysis.CompleteCurrentScreenPartition" {
                    compact_completed_partition(run)?;
                }
                append_checkpoint(
                    run,
                    &now,
                    WorkflowCheckpointStage::CapabilityCompleted,
                    None,
                    None,
                )?;
                if run.state != WorkflowRunState::Cancelling {
                    run.state = WorkflowRunState::Running;
                }
            }
            run.pending_invocation = None;
            run.updated_at = now;
            Ok(())
        })
    }

    fn pause_workflow(&self, token: &str, error: &WorkflowError) -> Result<(), WorkflowError> {
        let encoded = serde_json::to_value(error).map_err(|_| WorkflowError::internal())?;
        let code = encoded
            .get("code")
            .and_then(Value::as_str)
            .filter(|value| super::valid_identifier(value, 128))
            .ok_or_else(WorkflowError::internal)?;
        self.mutate(|document, now| {
            let run = workflow_control::find_workflow_mut(document, token)?;
            if matches!(
                run.state,
                WorkflowRunState::Queued
                    | WorkflowRunState::Running
                    | WorkflowRunState::WaitingForServiceJob
            ) {
                run.state = WorkflowRunState::Paused;
                run.last_error = Some(code.to_owned());
                run.updated_at = now;
            }
            Ok(())
        })
    }
}

pub(super) fn capabilities_available(generation: &WorkflowGeneration) -> bool {
    // This is callable-contract availability only. Data readiness is established by the workflow.
    [
        "AnalyticalProfile.Resolve",
        "Portfolio.GetRecommendationSetup",
        "Market.PrepareInvestmentEvidence",
        PREPARATION_RESULT,
        "Market.SelectInvestmentEvidence",
        "Analysis.PrepareProbabilityEvent",
        "Analysis.StartProbabilityDataset",
        "Analysis.StartInvestmentDataset",
        "Analysis.GetPreparedDatasetJobResult",
        "Model.StartPreparedTraining",
        "Model.GetTrainingJobResult",
        "Model.PrepareInvestmentForecast",
        "Model.StartPreparedForecast",
        "Model.GetForecastJobResult",
        "Analysis.GetFiscalPreparationPlan",
        "Analysis.StartFiscalDatasetBuild",
        "Model.StartFiscalForecast",
        "Decision.PrepareCurrentScreen",
        "Decision.ReadCurrentScreenPreparation",
        "Decision.ReadCurrentScreenCoverage",
        "Analysis.PrepareCurrentScreenPartition",
        "Analysis.CompleteCurrentScreenPartition",
        "Decision.ReadCurrentScreenPartitionCompletion",
        "Analysis.StartCurrentScreenDataset",
        "Decision.StartCurrentScreen",
        "Decision.GetCurrentScreenJobResult",
        "Analysis.GetHistoricalStudyPlan",
        "Analysis.StartHistoricalStudyDataset",
        "Analysis.CompleteHistoricalStudyFiscalPage",
        "Model.StartHistoricalStudyTraining",
        "Analysis.StartRecommendationBacktest",
        "Analysis.GetRecommendationBacktestJobResult",
        "Portfolio.SelectAnalysisPrerequisites",
        "Decision.GenerateInvestmentAnalysis",
        "Decision.GetInvestmentAnalysis",
        "Decision.PublishFindResults",
        "Decision.GetFindResults",
        "Job.Get",
        "Job.Cancel",
        "Job.ReconcileStart",
        "Job.CancelStart",
    ]
    .into_iter()
    .all(|operation| generation.has_operation(operation))
}

pub(super) async fn start(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    kind: WorkflowKind,
    selection: Option<String>,
    benchmark_instrument_id: Option<Uuid>,
) -> Result<AnalyticalControllerResponse, WorkflowError> {
    if !capabilities_available(generation) {
        return Err(WorkflowError::new(
            "analysis_unavailable",
            "Investment analysis is unavailable right now. Check Connections and try again.",
        ));
    }
    admit_recommendation_setup(state, generation).await?;
    let _fence = generation.analytical_retirement_fence().await;
    state.admit_current(generation)?;
    generation.analytical_controller().begin_workflow(
        kind,
        selection,
        benchmark_instrument_id,
        generation.origin(),
    )
}

/// Setup readiness prevents wasted acquisition and training; it conveys no financial authority.
/// The final portfolio selection and publication still reopen and recheck their original owners.
async fn admit_recommendation_setup(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
) -> Result<(), WorkflowError> {
    state.admit_current(generation)?;
    let setup = call(
        generation,
        "Portfolio.GetRecommendationSetup",
        Map::new(),
        false,
        RequestId::try_string(format!(
            "desktop-analysis-setup-{}",
            Uuid::new_v4().simple()
        ))
        .map_err(|_| WorkflowError::internal())?,
    )
    .await?;
    state.admit_current(generation)?;
    if uuid_field(&setup, "workspaceId")? != generation.analytical_controller().owner_workspace_id {
        return Err(WorkflowError::internal());
    }
    match (
        setup.get("state").and_then(Value::as_str),
        setup.get("setupRequiredReason"),
    ) {
        (Some("ready"), Some(Value::Null))
            if setup.get("accountSelection").is_some_and(Value::is_object)
                && setup.get("allocationProfile").is_some_and(Value::is_object) =>
        {
            Ok(())
        }
        (Some("setup_required"), Some(Value::String(reason))) => {
            let message = match reason.as_str() {
                "no_default_account" | "ambiguous_accounts" => {
                    "Choose a portfolio and confirm your allocation preferences on the Portfolio page before starting analysis."
                }
                "portfolio_evidence_unavailable" => {
                    "Update the selected portfolio's evidence on the Portfolio page before starting analysis."
                }
                "profile_review_required" => {
                    "Review and confirm your allocation preferences on the Portfolio page before starting analysis."
                }
                _ => return Err(WorkflowError::internal()),
            };
            Err(WorkflowError::new("analysis_setup_required", message))
        }
        _ => Err(WorkflowError::internal()),
    }
}

pub(super) async fn resume(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    token: &str,
) -> Result<AnalyticalControllerResponse, WorkflowError> {
    let _fence = generation.analytical_retirement_fence().await;
    state.admit_current(generation)?;
    generation.analytical_controller().resume_workflow(token)
}

/// One lightweight task uses the installed runtime; its workspace owner retains and joins it.
pub(crate) fn launch(generation: Arc<WorkflowGeneration>) {
    // Persisting the command precedes this notification; idle inspection registers first.
    generation.work_available().notify_one();
    let task_owner = Arc::clone(&generation);
    let Ok(mut task) = task_owner.analytical_driver_task().try_lock() else {
        return;
    };
    if generation.cancellation().is_cancelled() {
        return;
    }
    let controller = generation.analytical_controller();
    if controller
        .driver_active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    struct Running(Arc<WorkflowGeneration>);
    impl Drop for Running {
        fn drop(&mut self) {
            self.0
                .analytical_controller()
                .driver_active
                .store(false, Ordering::Release);
        }
    }
    let running = Running(Arc::clone(&generation));
    let mut previous = task.take();
    *task = Some(tokio::spawn(async move {
        let _running = running;
        let outcome: Result<(), WorkflowError> = async {
        // A naturally completed run is joined before its successor can touch retained work.
        // The shared active guard admits only this successor while that join completes.
        WorkflowGeneration::join_analytical_driver(&mut previous).await?;
        let cancellation = generation.cancellation();
        if cancellation.is_cancelled() {
            return Ok(());
        }
        {
            let state = WorkflowState;
            let _fence = generation.analytical_retirement_fence().await;
            if cancellation.is_cancelled() || state.admit_current(&generation).is_err() {
                return Ok(());
            }
            if generation
                .analytical_controller()
                .mutate(|document, _| {
                    for run in &mut document.workflow_runs {
                        if matches!(
                            run.state,
                            WorkflowRunState::Running | WorkflowRunState::WaitingForServiceJob
                        ) && let Some(driver) = &mut run.driver
                        {
                            driver.revalidating = true;
                            driver.revalidate_after = None;
                            if let Some(fiscal) = &mut driver.historical_fiscal {
                                fiscal.revalidate_index = 0;
                            }
                        }
                    }
                    Ok(())
                })
                .is_err()
            {
                return Ok(());
            }
        }
        loop {
            let notification = generation.work_available().notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            let state = WorkflowState;
            if state.admit_current(&generation).is_err() || cancellation.is_cancelled() {
                break;
            }
            let recovery = super::prepared_starts::recover_prepared_starts(&state, &generation).await;
            generation.record_background_failure(recovery.err())?;
            let run = match generation.analytical_controller().next_run()? {
                Some(run) => run,
                None => {
                    if generation.analytical_controller().unresolved_prepared_delivery()?.is_some() {
                        // Only genuinely unsettled delivery work is retried on the existing pace.
                        tokio::select! { _ = cancellation.cancelled() => break, _ = tokio::time::sleep(Duration::from_millis(750)) => {} }
                    } else {
                        tokio::select! { _ = cancellation.cancelled() => break, _ = &mut notification => {} }
                    }
                    continue;
                }
            };
            let token = opaque_workflow_token(&run)?;
            let retained_generation = generation.for_origin(run.origin)?;
            let progress = match advance(&state, &retained_generation, &run, &token).await {
                Ok(progress) => progress,
                Err(error) => {
                    let _fence = generation.analytical_retirement_fence().await;
                    if !cancellation.is_cancelled() && state.admit_current(&generation).is_ok() {
                        generation.analytical_controller().pause_workflow(&token, &error)?;
                        // The paused run retains its failure; the supervisor remains available
                        // for an explicitly confirmed resume without an admission/exit race.
                        continue;
                    }
                    break;
                }
            };
            match progress {
                AdvanceProgress::Advanced => tokio::task::yield_now().await,
                AdvanceProgress::Waiting => {
                    tokio::select! { _ = cancellation.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(750)) => {} }
                }
            }
        }
        Ok(())
        }.await;
        if let Err(error) = &outcome {
            let _ = generation.record_background_failure(Some(error.clone()));
        }
        outcome
    }));
}

/// Immediate continuation requires an acknowledged transition; unresolved work stays paced.
enum AdvanceProgress {
    Advanced,
    Waiting,
}

async fn advance(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    run: &WorkflowRun,
    token: &str,
) -> Result<AdvanceProgress, WorkflowError> {
    if run.state == WorkflowRunState::Cancelling {
        workflow_control::settle_cancellation(state, generation, token).await?;
        let document = generation.analytical_controller().lock_document()?;
        let retained = workflow_control::find_workflow(&document, token)?;
        return Ok(if retained.state == WorkflowRunState::Cancelling {
            AdvanceProgress::Waiting
        } else {
            AdvanceProgress::Advanced
        });
    }
    if run.deadline_at.as_deref().is_some_and(|deadline| {
        unix_nanos_now()
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            >= deadline.parse::<u64>().ok()
    }) {
        generation
            .analytical_controller()
            .request_workflow_cancellation(token)?;
        return Ok(AdvanceProgress::Advanced);
    }
    let _gate = generation
        .analytical_controller()
        .cancellation_gate
        .lock()
        .await;
    if let Some(pending) = &run.pending_invocation {
        recover_pending(state, generation, token, pending).await?;
        let document = generation.analytical_controller().lock_document()?;
        let retained = workflow_control::find_workflow(&document, token)?;
        return Ok(if retained.pending_invocation.is_none() {
            AdvanceProgress::Advanced
        } else {
            AdvanceProgress::Waiting
        });
    }
    let driver = run.driver.as_ref().ok_or_else(WorkflowError::internal)?;
    if driver.revalidating {
        revalidate(state, generation, run, token).await?;
        return Ok(AdvanceProgress::Advanced);
    }
    if let Some(job) = &driver.active_job {
        poll_job(state, generation, run, token, job).await?;
        let document = generation.analytical_controller().lock_document()?;
        let retained = workflow_control::find_workflow(&document, token)?;
        let driver = retained
            .driver
            .as_ref()
            .ok_or_else(WorkflowError::internal)?;
        return Ok(if driver.active_job.is_none() {
            AdvanceProgress::Advanced
        } else {
            // A newer progress sequence is still an unresolved job, not a next step.
            AdvanceProgress::Waiting
        });
    }
    if driver.step == Step::Finished {
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        finish_or_continue(generation.analytical_controller(), token)?;
        return Ok(AdvanceProgress::Advanced);
    }
    let (operation, arguments, mutation) = next_invocation(run)?;
    check_historical_page_budget(generation, run)?;
    let arguments = crate::application::analytical_workflow::host::prepare_analytical_arguments(
        generation,
        operation,
        arguments,
        if mutation {
            InvocationAuthority::ExactConfirmed(operation)
        } else {
            InvocationAuthority::ReadOnly
        },
    )?;
    let pending = {
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        generation
            .analytical_controller()
            .retain_pending(token, operation, arguments)?
    };
    let body = call(
        generation,
        operation,
        pending.arguments.clone(),
        mutation,
        RequestId::try_string(pending.request_id.clone()).map_err(|_| WorkflowError::internal())?,
    )
    .await?;
    let _fence = generation.analytical_retirement_fence().await;
    state.admit_current(generation)?;
    generation
        .analytical_controller()
        .retain_response(token, &pending, body)?;
    Ok(AdvanceProgress::Advanced)
}

async fn call(
    generation: &Arc<WorkflowGeneration>,
    operation: &'static str,
    arguments: Map<String, Value>,
    mutation: bool,
    request_id: RequestId,
) -> Result<Value, WorkflowError> {
    let response = invoke_analytical_operation(
        generation,
        operation,
        arguments,
        if mutation {
            InvocationAuthority::ExactConfirmed(operation)
        } else {
            InvocationAuthority::ReadOnly
        },
        request_id,
        CancellationToken::new(),
    )
    .await?;
    response
        .get("data")
        .cloned()
        .ok_or_else(WorkflowError::internal)
}

fn job_result_operation(operation: &str) -> Option<&'static str> {
    match operation {
        "Market.PrepareInvestmentEvidence" => Some(PREPARATION_RESULT),
        "Analysis.StartProbabilityDataset"
        | "Analysis.StartInvestmentDataset"
        | "Analysis.StartFiscalDatasetBuild"
        | "Analysis.StartCurrentScreenDataset"
        | "Analysis.StartHistoricalStudyDataset" => Some("Analysis.GetPreparedDatasetJobResult"),
        "Model.StartPreparedTraining" | "Model.StartHistoricalStudyTraining" => {
            Some("Model.GetTrainingJobResult")
        }
        "Model.StartPreparedForecast" | "Model.StartFiscalForecast" => {
            Some("Model.GetForecastJobResult")
        }
        "Decision.StartCurrentScreen" => Some("Decision.GetCurrentScreenJobResult"),
        "Analysis.StartRecommendationBacktest" => {
            Some("Analysis.GetRecommendationBacktestJobResult")
        }
        _ => None,
    }
}

fn next_invocation(
    run: &WorkflowRun,
) -> Result<(&'static str, Map<String, Value>, bool), WorkflowError> {
    let work = run.driver.as_ref().ok_or_else(WorkflowError::internal)?;
    let (operation, arguments, mutation) = match work.step {
        Step::ResolveProfile => (
            "AnalyticalProfile.Resolve",
            json!({"configuration": run.profile.config.financial_configuration.value()}),
            false,
        ),
        Step::FindPopulation => {
            let mut arguments = json!({"financialProfile": DriverState::profile(run)?, "maximumCandidates": run.profile.config.discovery_breadth.maximum_analyses()});
            if let Some(benchmark) = work.benchmark_instrument_id {
                arguments["benchmarkInstrumentId"] = json!(benchmark);
            }
            ("Decision.PrepareCurrentScreen", arguments, true)
        }
        Step::FindPartition | Step::FindDataset => {
            let mut input = work.find_preparation()?;
            let find = work.find.as_ref().ok_or_else(WorkflowError::internal)?;
            input["ordinal"] = json!(find.partition_index);
            (
                if work.step == Step::FindPartition {
                    "Analysis.PrepareCurrentScreenPartition"
                } else {
                    "Analysis.StartCurrentScreenDataset"
                },
                input,
                true,
            )
        }
        Step::FindCompletePartition => {
            let find = work.find.as_ref().ok_or_else(WorkflowError::internal)?;
            let mut input = work.find_preparation()?;
            input["ordinal"] = json!(find.partition_index);
            if let Some(job) = &find.pending_dataset_job {
                input["datasetJob"] = Value::Object(job_arguments(job)?);
            }
            ("Analysis.CompleteCurrentScreenPartition", input, true)
        }
        Step::FindScreen => {
            let mut input = work.find_preparation()?;
            input["completion"] = work
                .find
                .as_ref()
                .and_then(|find| find.completed_partition.as_ref())
                .and_then(|receipt| receipt.body.get("completion"))
                .cloned()
                .ok_or_else(WorkflowError::internal)?;
            ("Decision.StartCurrentScreen", input, true)
        }
        Step::FindReadPreparation => (
            "Decision.ReadCurrentScreenPreparation",
            work.find_preparation()?,
            false,
        ),
        Step::PrepareSelection => {
            let mut arguments = json!({"selectionToken":work.selection_token,"financialProfile":DriverState::profile(run)?});
            if let Some(benchmark) = work.benchmark_instrument_id {
                arguments["benchmarkInstrumentId"] = json!(benchmark);
            }
            if work.find.is_some() {
                arguments["findMember"] = serde_json::to_value(current_find_member(work)?)
                    .map_err(|_| WorkflowError::internal())?;
            }
            ("Market.PrepareInvestmentEvidence", arguments, true)
        }
        Step::SelectMarket => (
            "Market.SelectInvestmentEvidence",
            json!({"selectionToken": work.selection_token, "sourceCutoffUnixNanos": work.source_cutoff, "financialProfile": DriverState::profile(run)?}),
            false,
        ),
        Step::PriceDataset | Step::PriceInputs => {
            let mut input = work.input(run)?;
            input["intendedUse"] = json!(if work.step == Step::PriceDataset {
                "train"
            } else {
                "local_analysis"
            });
            ("Analysis.StartInvestmentDataset", input, true)
        }
        Step::PriceTraining | Step::FiscalTraining => {
            let step = if work.step == Step::PriceTraining {
                Step::PriceDataset
            } else {
                Step::FiscalTrainingDataset
            };
            let job = job_from_receipt(work.receipt(step)?)?;
            (
                "Model.StartPreparedTraining",
                json!({"datasetJobId": job.job_id, "datasetJobGeneration": generation_number(&job)?, "financialProfile": DriverState::profile(run)?}),
                true,
            )
        }
        Step::PreparePriceForecast => {
            let mut input = work.input(run)?;
            if let Some(find) = &work.find {
                let cohort = find
                    .preparation
                    .get("forecastCohort")
                    .filter(|value| value.is_object())
                    .ok_or_else(|| {
                        WorkflowError::new(
                            "analysis_period_unavailable",
                            "A shared analysis period is unavailable. Start a fresh search.",
                        )
                    })?;
                input["forecastCohort"] = cohort.clone();
                input["currentFeatureInput"] = find
                    .candidates
                    .get(find.candidate_index)
                    .and_then(|candidate| candidate.get("currentFeatureInput"))
                    .filter(|value| value.is_object())
                    .cloned()
                    .ok_or_else(WorkflowError::internal)?;
                input["sourceCutoffUnixNanos"] = json!(work.price_forecast_cutoff()?);
            }
            ("Model.PrepareInvestmentForecast", input, false)
        }
        Step::PriceForecast => (
            "Model.StartPreparedForecast",
            json!({"confirmationToken": work.receipt(Step::PreparePriceForecast)?.body.pointer("/forecast/confirmationToken").and_then(Value::as_str).ok_or_else(WorkflowError::internal)?}),
            true,
        ),
        Step::ProbabilityPlan
        | Step::ProbabilitySubject
        | Step::ProbabilityPrepared
        | Step::ProbabilityTrainingDataset
        | Step::ProbabilityAnalysisDataset
        | Step::ProbabilityTraining
        | Step::PrepareProbabilityForecast
        | Step::ProbabilityForecast => probability::next_invocation(run)?,
        Step::FiscalPlan => ("Analysis.GetFiscalPreparationPlan", work.input(run)?, false),
        Step::FiscalTrainingDataset | Step::FiscalInputDataset => {
            let target = work
                .fiscal_targets
                .get(work.fiscal_index)
                .ok_or_else(WorkflowError::internal)?;
            let mut input = work.input(run)?;
            input["targetId"] = target
                .target
                .get("targetId")
                .cloned()
                .ok_or_else(WorkflowError::internal)?;
            input["purpose"] = json!(if work.step == Step::FiscalTrainingDataset {
                "training"
            } else {
                "studyInputs"
            });
            ("Analysis.StartFiscalDatasetBuild", input, true)
        }
        Step::FiscalForecast => {
            let training = job_from_receipt(work.receipt(Step::FiscalTraining)?)?;
            let inputs = job_from_receipt(work.receipt(Step::FiscalInputDataset)?)?;
            (
                "Model.StartFiscalForecast",
                json!({"trainingJobId": training.job_id, "trainingJobGeneration": generation_number(&training)?,
                "inputDatasetJobId": inputs.job_id, "inputDatasetJobGeneration": generation_number(&inputs)?, "financialProfile": DriverState::profile(run)?}),
                true,
            )
        }
        Step::HistoricalStudy => (
            "Analysis.GetHistoricalStudyPlan",
            json!({"subjectInstrumentId": work.instrument_id,
            "sourceCutoffUnixNanos": work.source_cutoff, "financialProfile": DriverState::profile(run)?,
            "sourceActionReference": work.initial_source_action_reference()?}),
            false,
        ),
        Step::StudyTrainingDataset | Step::StudyInputs => {
            let plan = work
                .receipt(Step::HistoricalStudy)?
                .body
                .get("plan")
                .ok_or_else(WorkflowError::internal)?;
            let mut input = json!({"plan": plan, "part": if work.step == Step::StudyInputs { "studyInputs" } else { "training" }});
            if work.step == Step::StudyTrainingDataset {
                input["foldIndex"] = json!(work.study_fold);
            }
            ("Analysis.StartHistoricalStudyDataset", input, true)
        }
        Step::StudyTraining => {
            let dataset = job_from_receipt(work.receipt(Step::StudyTrainingDataset)?)?;
            (
                "Model.StartHistoricalStudyTraining",
                json!({"plan": work.receipt(Step::HistoricalStudy)?.body.get("plan").ok_or_else(WorkflowError::internal)?,
                "foldIndex": work.study_fold, "datasetJob": {"jobId": dataset.job_id, "generation": generation_number(&dataset)?}}),
                true,
            )
        }
        Step::StudyFiscalPage | Step::StudyFiscalOrigin => {
            let mut input = historical_plan_arguments(work)?;
            let fiscal = work
                .historical_fiscal
                .as_ref()
                .ok_or_else(WorkflowError::internal)?;
            if work.step == Step::StudyFiscalPage {
                input.insert("pageOrdinal".into(), json!(fiscal.pages.len()));
            } else {
                input.insert(
                    "priceExampleId".into(),
                    json!(fiscal.current_origin()?.price_example_id),
                );
            }
            (
                "Analysis.GetHistoricalStudyPlan",
                Value::Object(input),
                false,
            )
        }
        Step::StudyFiscalTrainingDataset | Step::StudyFiscalInputDataset => {
            let input = json!({"plan": work.receipt(Step::HistoricalStudy)?.body.get("plan")
                .ok_or_else(WorkflowError::internal)?,
                "part": if work.step == Step::StudyFiscalTrainingDataset { "training" } else { "studyInputs" },
                "fiscal": historical_target_arguments(work)?});
            ("Analysis.StartHistoricalStudyDataset", input, true)
        }
        Step::StudyFiscalTraining => {
            let job = job_from_receipt(work.receipt(Step::StudyFiscalTrainingDataset)?)?;
            (
                "Model.StartHistoricalStudyTraining",
                json!({
                "plan":work.receipt(Step::HistoricalStudy)?.body.get("plan").ok_or_else(WorkflowError::internal)?,
                "datasetJob":job_arguments(&job)?, "fiscal":historical_target_arguments(work)?}),
                true,
            )
        }
        Step::StudyFiscalCompletePage => (
            "Analysis.CompleteHistoricalStudyFiscalPage",
            historical_completion_arguments(work)?,
            true,
        ),
        Step::StudyBacktest => {
            let mut training = Vec::with_capacity(3);
            for fold in 0..3 {
                let receipt = work
                    .receipts
                    .get(&format!("StudyTraining-{fold}"))
                    .ok_or_else(WorkflowError::internal)?;
                let mut reference = job_arguments(&job_from_receipt(receipt)?)?;
                let dataset = work
                    .receipts
                    .get(&format!("StudyTrainingDataset-{fold}"))
                    .ok_or_else(WorkflowError::internal)?;
                reference.insert(
                    "datasetJob".to_owned(),
                    Value::Object(job_arguments(&job_from_receipt(dataset)?)?),
                );
                training.push(reference);
            }
            let inputs = job_from_receipt(work.receipt(Step::StudyInputs)?)?;
            (
                "Analysis.StartRecommendationBacktest",
                json!({"plan": work.receipt(Step::HistoricalStudy)?.body.get("plan").ok_or_else(WorkflowError::internal)?,
                "studyInputJob": job_arguments(&inputs)?, "trainingJobs": training,
                "fiscalPages": work.historical_fiscal.as_ref().filter(|fiscal| fiscal.complete())
                    .ok_or_else(WorkflowError::internal)?.pages.iter().map(|page| &page.reference).collect::<Vec<_>>()}),
                true,
            )
        }
        Step::FinalPrepare => (
            "Market.PrepareInvestmentEvidence",
            {
                let mut arguments = json!({"selectionToken": work.selection_token, "financialProfile": DriverState::profile(run)?, "purpose": "current_market",
                    "originalKnowledgeAtUnixNanos": work.source_cutoff.as_ref().ok_or_else(WorkflowError::internal)?});
                if let Some(origin) = work
                    .receipts
                    .get("PriceForecast")
                    .and_then(|receipt| receipt.body.pointer("/forecast/observedThroughUnixNanos"))
                    .filter(|value| value.is_string())
                {
                    arguments["shareOriginUnixNanos"] = origin.clone();
                }
                arguments
            },
            true,
        ),
        Step::FinalEvidence => (
            "Market.SelectInvestmentEvidence",
            json!({"selectionToken": work.selection_token, "sourceCutoffUnixNanos": work.receipt(Step::FinalPrepare)?.preparation()?.get("preparedAtUnixNanos").ok_or_else(WorkflowError::internal)?, "financialProfile": DriverState::profile(run)?}),
            false,
        ),
        Step::FinalPortfolio => (
            "Portfolio.SelectAnalysisPrerequisites",
            json!({"instrumentId": work.instrument_id, "sourceCutoffUnixNanos": work.receipt(Step::FinalEvidence)?.body.get("sourceCutoffUnixNanos").ok_or_else(WorkflowError::internal)?, "financialProfile": DriverState::profile(run)?}),
            false,
        ),
        Step::Publish => (
            "Decision.GenerateInvestmentAnalysis",
            publication_arguments(run)?,
            true,
        ),
        Step::ReadPublished => (
            "Decision.GetInvestmentAnalysis",
            json!({"actionToken": work.receipt(Step::Publish)?.body.get("actionToken").ok_or_else(WorkflowError::internal)?}),
            false,
        ),
        Step::FindPublish => (
            "Decision.PublishFindResults",
            find_publication_arguments(run)?,
            true,
        ),
        Step::FindReadPublished => ("Decision.GetFindResults", work.find_preparation()?, false),
        Step::Finished => return Err(WorkflowError::internal()),
    };
    let mut arguments = object(arguments)?;
    if mutation {
        arguments.insert("confirm".to_owned(), Value::Bool(true));
    }
    Ok((operation, arguments, mutation))
}

fn apply_receipt(run: &mut WorkflowRun, receipt: Receipt, now: &str) -> Result<(), WorkflowError> {
    if preparation_requires_retry(run, &receipt)? {
        let work = run.driver.as_mut().ok_or_else(WorkflowError::internal)?;
        let key = work.key(work.step);
        if work.receipts.len() == MAXIMUM_RECEIPTS || work.receipts.insert(key, receipt).is_some() {
            return Err(WorkflowError::internal());
        }
        run.state = WorkflowRunState::Paused;
        run.last_error = Some("analysis_preparation_required".to_owned());
        return Ok(());
    }
    let work = run.driver.as_mut().ok_or_else(WorkflowError::internal)?;
    let step = work.step;
    match step {
        Step::ResolveProfile => {
            let resolution: financial_profiles::FinancialResolution =
                serde_json::from_value(receipt.body.clone())
                    .map_err(|_| WorkflowError::internal())?;
            if !resolution.admits_configuration(&run.profile.config.financial_configuration) {
                return Err(WorkflowError::internal());
            }
            run.profile.financial_resolution = Some(resolution);
            work.step = if run.kind == WorkflowKind::FindOpportunities {
                Step::FindPopulation
            } else {
                Step::PrepareSelection
            };
        }
        Step::FindPopulation => {
            uuid_field(&receipt.body, "preparationId")?;
            if receipt
                .body
                .get("forecastCohort")
                .is_none_or(|value| !value.is_null() && !value.is_object())
            {
                return Err(WorkflowError::internal());
            }
            if !receipt
                .body
                .get("preparationSha256")
                .and_then(Value::as_str)
                .is_some_and(valid_digest)
            {
                return Err(WorkflowError::internal());
            }
            let partition_count = receipt
                .body
                .get("partitionCount")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value <= 65_536)
                .ok_or_else(WorkflowError::internal)?;
            let population_count = receipt
                .body
                .get("populationCount")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value <= 65_536)
                .ok_or_else(WorkflowError::internal)?;
            let canonical_population_count = receipt
                .body
                .pointer("/coverage/canonicalPopulationCount")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value >= population_count && *value <= 65_536)
                .ok_or_else(WorkflowError::internal)?;
            work.find = Some(FindState {
                preparation: receipt.body.clone(),
                partition_count,
                partition_index: 0,
                pending_dataset_job: None,
                completed_partition: None,
                candidates: Vec::new(),
                candidate_index: 0,
                population_count,
                canonical_population_count,
                screen_receipt: None,
            });
            work.step = if partition_count == 0 {
                Step::FindReadPreparation
            } else {
                Step::FindPartition
            };
        }
        Step::FindPartition => {
            let find = work.find.as_mut().ok_or_else(WorkflowError::internal)?;
            if receipt.body.get("ordinal").and_then(Value::as_u64)
                != u64::try_from(find.partition_index).ok()
            {
                return Err(WorkflowError::internal());
            }
            if receipt
                .body
                .get("buildRequired")
                .and_then(Value::as_bool)
                .ok_or_else(WorkflowError::internal)?
            {
                work.step = Step::FindDataset;
            } else {
                find.pending_dataset_job = None;
                work.step = Step::FindCompletePartition;
            }
        }
        Step::FindDataset => {
            let job = job_from_receipt(&receipt)?;
            let find = work.find.as_mut().ok_or_else(WorkflowError::internal)?;
            find.pending_dataset_job = Some(job);
            work.step = Step::FindCompletePartition;
        }
        Step::FindCompletePartition => {
            let find = work.find.as_mut().ok_or_else(WorkflowError::internal)?;
            completion_receipt(&receipt, &find.preparation, Some(find.partition_index))?;
            let expected = find
                .pending_dataset_job
                .as_ref()
                .map(job_arguments)
                .transpose()?
                .map(Value::Object);
            if receipt
                .body
                .get("datasetJob")
                .filter(|value| !value.is_null())
                != expected.as_ref()
                || receipt.arguments.get("datasetJob") != expected.as_ref()
            {
                return Err(WorkflowError::internal());
            }
            find.completed_partition = Some(receipt.clone());
            find.pending_dataset_job = None;
            find.partition_index += 1;
            work.step = if find.partition_index == find.partition_count {
                Step::FindReadPreparation
            } else {
                Step::FindPartition
            };
        }
        Step::FindReadPreparation => {
            let find = work.find.as_mut().ok_or_else(WorkflowError::internal)?;
            if receipt.body.get("forecastCohort") != find.preparation.get("forecastCohort")
                || receipt.body.get("preparationId") != find.preparation.get("preparationId")
                || receipt.body.get("preparationSha256")
                    != find.preparation.get("preparationSha256")
                || receipt
                    .body
                    .pointer("/coverage/complete")
                    .and_then(Value::as_bool)
                    != Some(true)
                || receipt
                    .body
                    .pointer("/coverage/preparedPartitionCount")
                    .and_then(Value::as_u64)
                    != u64::try_from(find.partition_count).ok()
            {
                return Err(WorkflowError::internal());
            }
            let available = receipt
                .body
                .get("availablePartitionCount")
                .and_then(Value::as_u64)
                .ok_or_else(WorkflowError::internal)?;
            if available > find.partition_count as u64 {
                return Err(WorkflowError::internal());
            }
            if available == 0 {
                find.screen_receipt = Some(receipt.clone());
                work.step = Step::Finished;
            } else {
                work.step = Step::FindScreen;
            }
        }
        Step::FindScreen => {
            let find = work.find.as_ref().ok_or_else(WorkflowError::internal)?;
            if receipt.body.get("forecastCohort") != find.preparation.get("forecastCohort")
                || receipt.body.get("preparationId") != find.preparation.get("preparationId")
                || receipt.body.get("preparationSha256")
                    != find.preparation.get("preparationSha256")
                || receipt
                    .body
                    .pointer("/coverage/complete")
                    .and_then(Value::as_bool)
                    != Some(true)
                || !receipt
                    .body
                    .get("rankingSha256")
                    .and_then(Value::as_str)
                    .is_some_and(valid_digest)
            {
                return Err(WorkflowError::internal());
            }
            let candidates = receipt
                .body
                .get("candidates")
                .and_then(Value::as_array)
                .filter(|rows| {
                    rows.len() <= run.profile.config.discovery_breadth.maximum_analyses()
                })
                .ok_or_else(WorkflowError::internal)?;
            let mut ids = std::collections::HashSet::new();
            for (index, candidate) in candidates.iter().enumerate() {
                let id = candidate
                    .get("candidateId")
                    .and_then(Value::as_str)
                    .filter(|value| super::valid_identifier(value, 256))
                    .ok_or_else(WorkflowError::internal)?;
                if !candidate
                    .get("screenRunId")
                    .and_then(Value::as_str)
                    .is_some_and(|value| super::valid_identifier(value, 256))
                {
                    return Err(WorkflowError::internal());
                }
                if !ids.insert(id)
                    || candidate.get("rank").and_then(Value::as_u64)
                        != u64::try_from(index + 1).ok()
                    || !candidate
                        .get("evidenceDigest")
                        .and_then(Value::as_str)
                        .is_some_and(valid_digest)
                    || !candidate
                        .get("selectionToken")
                        .and_then(Value::as_str)
                        .is_some_and(valid_market_selection_token)
                {
                    return Err(WorkflowError::internal());
                }
            }
            let find = work.find.as_mut().ok_or_else(WorkflowError::internal)?;
            find.candidates = candidates.clone();
            find.screen_receipt = Some(receipt.clone());
            find.candidate_index = 0;
            work.selection_token = candidates
                .first()
                .and_then(|row| row.get("selectionToken"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            work.step = if candidates.is_empty() {
                Step::Finished
            } else {
                Step::PrepareSelection
            };
        }
        Step::PrepareSelection => {
            let preparation = receipt.preparation()?;
            let original_arguments = preparation_arguments(&receipt.body)?;
            if !matches!(
                preparation.get("status").and_then(Value::as_str),
                Some("prepared" | "unavailable")
            ) {
                return Err(WorkflowError::new(
                    "analysis_evidence_unavailable",
                    "This investment does not yet have enough current information for analysis.",
                ));
            }
            let resolution = serde_json::to_value(
                run.profile
                    .financial_resolution
                    .as_ref()
                    .ok_or_else(WorkflowError::internal)?,
            )
            .map_err(|_| WorkflowError::internal())?;
            if preparation.get("financialConfigurationDigest")
                != resolution.get("configurationDigest")
            {
                return Err(WorkflowError::internal());
            }
            if work.find.is_some()
                && preparation.get("status").and_then(Value::as_str) == Some("unavailable")
                && preparation.get("findMemberUnavailable").is_some()
            {
                let unavailable: FindMemberUnavailable = serde_json::from_value(
                    preparation
                        .get("findMemberUnavailable")
                        .cloned()
                        .ok_or_else(WorkflowError::internal)?,
                )
                .map_err(|_| WorkflowError::internal())?;
                if !unavailable.valid()
                    || unavailable.member != current_find_member(work)?
                    || original_arguments.get("findMember")
                        != Some(
                            &serde_json::to_value(&unavailable.member)
                                .map_err(|_| WorkflowError::internal())?,
                        )
                    || preparation.get("reason")
                        != Some(
                            &serde_json::to_value(&unavailable.reason)
                                .map_err(|_| WorkflowError::internal())?,
                        )
                {
                    return Err(WorkflowError::internal());
                }
                work.completed_analyses
                    .push(json!({"memberUnavailable":unavailable}));
                work.completed_outcomes.push("unavailable".to_owned());
                work.step = Step::Finished;
                let key = work.key(step);
                if work.receipts.len() == MAXIMUM_RECEIPTS
                    || work.receipts.insert(key, receipt).is_some()
                {
                    return Err(WorkflowError::internal());
                }
                return Ok(());
            }
            work.instrument_id = Some(uuid_field(preparation, "instrumentId")?);
            let prepared_at = preparation
                .get("preparedAtUnixNanos")
                .and_then(Value::as_str)
                .filter(|value| super::valid_timestamp(value))
                .ok_or_else(WorkflowError::internal)?;
            if prepared_at.parse::<u64>().ok() > now.parse::<u64>().ok() {
                return Err(WorkflowError::internal());
            }
            work.source_cutoff = Some(prepared_at.to_owned());
            work.step = Step::SelectMarket;
        }
        Step::SelectMarket => {
            if receipt
                .body
                .get("instrumentId")
                .and_then(Value::as_str)
                .and_then(|id| id.parse().ok())
                != work.instrument_id
            {
                return Err(WorkflowError::internal());
            }
            let resolution = run
                .profile
                .financial_resolution
                .as_ref()
                .ok_or_else(WorkflowError::internal)?;
            let profile =
                serde_json::to_value(resolution).map_err(|_| WorkflowError::internal())?;
            work.step = match profile
                .pointer("/configuration/modelBundlePolicy/kind")
                .and_then(Value::as_str)
            {
                Some("best_admitted_calibrated_mean_v1") => Step::PriceDataset,
                Some("exact") => Step::PriceInputs,
                _ => return Err(WorkflowError::internal()),
            };
        }
        Step::PriceDataset => work.step = Step::PriceTraining,
        Step::PriceTraining => work.step = Step::PriceInputs,
        Step::PriceInputs => work.step = Step::PreparePriceForecast,
        Step::PreparePriceForecast => {
            let prepared = price_preparation(&receipt)?;
            if Some(prepared.instrument_id) != work.instrument_id
                || prepared.source_cutoff_unix_nanos != work.price_forecast_cutoff()?
            {
                return Err(WorkflowError::internal());
            }
            if matches!(
                prepared.availability,
                PriceForecastAvailability::Unavailable { .. }
            ) {
                // A successful exact selection-absence receipt skips only this forecast.
                // Fiscal, historical and final decision services still assess their own evidence.
                work.step = Step::ProbabilityPlan;
            } else {
                // Model selection remains profile policy owned by the backend. An Exact selection
                // must remain pinned; Best may select a stronger admitted calibrated model than the
                // training job completed in this workflow.
                let profile = serde_json::to_value(
                    run.profile
                        .financial_resolution
                        .as_ref()
                        .ok_or_else(WorkflowError::internal)?,
                )
                .map_err(|_| WorkflowError::internal())?;
                if receipt.body.get("financialProfileDigest") != profile.get("configurationDigest")
                {
                    return Err(WorkflowError::internal());
                }
                if profile
                    .pointer("/configuration/modelBundlePolicy/kind")
                    .and_then(Value::as_str)
                    == Some("exact")
                    && receipt.body.pointer("/forecast/model/modelToken")
                        != profile.pointer("/configuration/modelBundlePolicy/modelToken")
                {
                    return Err(WorkflowError::internal());
                }
                let expiry = receipt
                    .body
                    .pointer("/forecast/expiresAtUnixNanos")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or_else(WorkflowError::internal)?;
                if now.parse::<u64>().map_err(|_| WorkflowError::internal())? >= expiry {
                    return Err(WorkflowError::new(
                        "analysis_preview_expired",
                        "The analysis preparation expired. Start a fresh analysis.",
                    ));
                }
                work.step = Step::PriceForecast;
            }
        }
        Step::PriceForecast | Step::FiscalForecast => {
            let expected = serde_json::to_value(
                run.profile
                    .financial_resolution
                    .as_ref()
                    .ok_or_else(WorkflowError::internal)?,
            )
            .map_err(|_| WorkflowError::internal())?;
            if receipt.body.get("financialProfileDigest") != expected.get("configurationDigest") {
                return Err(WorkflowError::internal());
            }
            uuid_field(
                receipt
                    .body
                    .get("forecast")
                    .ok_or_else(WorkflowError::internal)?,
                "forecastToken",
            )?;
            if !receipt
                .body
                .get("requestSha256")
                .and_then(Value::as_str)
                .is_some_and(valid_digest)
            {
                return Err(WorkflowError::internal());
            }
            if step == Step::PriceForecast {
                work.step = Step::ProbabilityPlan;
            }
        }
        Step::ProbabilityPlan
        | Step::ProbabilitySubject
        | Step::ProbabilityPrepared
        | Step::ProbabilityTrainingDataset
        | Step::ProbabilityAnalysisDataset
        | Step::ProbabilityTraining
        | Step::PrepareProbabilityForecast
        | Step::ProbabilityForecast => {
            probability::apply(work, &receipt, &run.profile.financial_resolution, now)?;
        }
        Step::FiscalPlan => {
            let profile = serde_json::to_value(
                run.profile
                    .financial_resolution
                    .as_ref()
                    .ok_or_else(WorkflowError::internal)?,
            )
            .map_err(|_| WorkflowError::internal())?;
            if uuid_field(&receipt.body, "instrumentId")?
                != work.instrument_id.ok_or_else(WorkflowError::internal)?
                || receipt
                    .body
                    .get("sourceCutoffUnixNanos")
                    .and_then(Value::as_str)
                    != work.source_cutoff.as_deref()
                || receipt.body.get("financialProfileDigest") != profile.get("configurationDigest")
            {
                return Err(WorkflowError::internal());
            }
            let targets = receipt
                .body
                .get("targets")
                .and_then(Value::as_array)
                .filter(|targets| targets.len() == MAXIMUM_FISCAL_TARGETS)
                .ok_or_else(WorkflowError::internal)?;
            let mut ids = std::collections::HashSet::new();
            for row in targets {
                let target = row.get("target").ok_or_else(WorkflowError::internal)?;
                let id = target
                    .get("targetId")
                    .and_then(Value::as_str)
                    .filter(|id| id.len() <= 96 && !id.is_empty())
                    .ok_or_else(WorkflowError::internal)?;
                if !ids.insert(id) {
                    return Err(WorkflowError::internal());
                }
                let availability: FiscalAvailability = serde_json::from_value(
                    row.get("availability")
                        .cloned()
                        .ok_or_else(WorkflowError::internal)?,
                )
                .map_err(|_| WorkflowError::internal())?;
                work.fiscal_targets.push(FiscalTarget {
                    target: target.clone(),
                    availability,
                });
            }
            work.fiscal_index = 0;
            next_fiscal(work);
        }
        Step::FiscalTrainingDataset => work.step = Step::FiscalTraining,
        Step::FiscalTraining => work.step = Step::FiscalInputDataset,
        Step::FiscalInputDataset => work.step = Step::FiscalForecast,
        Step::HistoricalStudy => {
            historical_study_receipt(&receipt)?;
            if receipt.arguments.get("sourceActionReference")
                != Some(work.initial_source_action_reference()?)
                || receipt
                    .arguments
                    .get("sourceCutoffUnixNanos")
                    .and_then(Value::as_str)
                    != work.source_cutoff.as_deref()
                || receipt
                    .arguments
                    .get("subjectInstrumentId")
                    .and_then(Value::as_str)
                    .and_then(|id| id.parse().ok())
                    != work.instrument_id
            {
                return Err(WorkflowError::internal());
            }
            if receipt.body.get("status").and_then(Value::as_str) == Some("unavailable") {
                work.step = Step::FinalPrepare;
            } else {
                work.study_fold = 0;
                work.step = Step::StudyTrainingDataset;
            }
        }
        Step::StudyTrainingDataset => work.step = Step::StudyTraining,
        Step::StudyTraining => {
            work.step = if work.study_fold == 2 {
                Step::StudyInputs
            } else {
                Step::StudyTrainingDataset
            }
        }
        Step::StudyInputs => {
            work.historical_fiscal = Some(HistoricalFiscalProgress::new());
            work.step = Step::StudyFiscalPage;
        }
        Step::StudyFiscalPage => {
            apply_historical_page(work, &receipt)?;
            return Ok(());
        }
        Step::StudyFiscalOrigin => {
            apply_historical_origin(work, &receipt)?;
            return Ok(());
        }
        Step::StudyFiscalTrainingDataset => work.step = Step::StudyFiscalTraining,
        Step::StudyFiscalTraining => work.step = Step::StudyFiscalInputDataset,
        Step::StudyFiscalInputDataset => {
            retain_historical_target(work, &receipt)?;
            return Ok(());
        }
        Step::StudyBacktest => {
            if ["requestDigest", "evidenceDigest"].iter().any(|key| {
                !receipt
                    .body
                    .get(key)
                    .and_then(Value::as_str)
                    .is_some_and(valid_digest)
            }) {
                return Err(WorkflowError::internal());
            }
            work.step = Step::FinalPrepare;
        }
        Step::FinalPrepare => {
            let preparation = receipt.preparation()?;
            if uuid_field(preparation, "instrumentId")?
                != work.instrument_id.ok_or_else(WorkflowError::internal)?
                || !preparation
                    .get("preparedAtUnixNanos")
                    .and_then(Value::as_str)
                    .is_some_and(super::valid_timestamp)
            {
                return Err(WorkflowError::internal());
            }
            work.step = Step::FinalEvidence;
        }
        Step::FinalEvidence => {
            if uuid_field(&receipt.body, "instrumentId")?
                != work.instrument_id.ok_or_else(WorkflowError::internal)?
            {
                return Err(WorkflowError::internal());
            }
            match receipt.body.get("status").and_then(Value::as_str) {
                Some("available")
                    if receipt.body.get("reference").is_some_and(Value::is_object) => {}
                Some("unavailable")
                    if receipt.body.get("reason").and_then(Value::as_str)
                        == Some("market_evidence_unavailable") => {}
                _ => {
                    return Err(WorkflowError::new(
                        "analysis_source_unavailable",
                        "The selected investment or its evidence changed. Start a fresh analysis.",
                    ));
                }
            }
            // Missing current prices do not discard completed analytical jobs. Publication
            // independently rechecks absence and persists the existing unavailable decision.
            work.step = Step::FinalPortfolio;
        }
        Step::FinalPortfolio => {
            if uuid_field(&receipt.body, "instrumentId")?
                != work.instrument_id.ok_or_else(WorkflowError::internal)?
            {
                return Err(WorkflowError::internal());
            }
            if receipt.body.get("reference").is_none_or(Value::is_null) {
                return Err(WorkflowError::new(
                    "analysis_setup_required",
                    "Choose an account and review your recommendation preferences on the Portfolio page.",
                ));
            }
            work.step = Step::Publish;
        }
        Step::Publish => {
            let action = uuid_field(&receipt.body, "actionToken")?;
            if !receipt
                .body
                .get("analysisId")
                .and_then(Value::as_str)
                .is_some_and(valid_digest)
            {
                return Err(WorkflowError::internal());
            }
            let valuation_identity = receipt.body.get("valuationMethodSetIdentity");
            match receipt.arguments.get("market") {
                Some(Value::Null) if valuation_identity == Some(&Value::Null) => {}
                Some(Value::Object(_))
                    if valuation_identity
                        .and_then(Value::as_str)
                        .is_some_and(valid_digest) => {}
                _ => return Err(WorkflowError::internal()),
            }
            let digest = receipt
                .body
                .get("explanationDigest")
                .and_then(Value::as_str)
                .filter(|value| valid_digest(value))
                .ok_or_else(WorkflowError::internal)?;
            let result = ServiceResultReference {
                operation: "Decision.GetInvestmentAnalysis".to_owned(),
                result_id: action.to_string(),
                content_sha256: digest.to_owned(),
            };
            if run.result_references.len() == super::MAXIMUM_RESULT_REFERENCES_PER_RUN
                || run
                    .result_references
                    .iter()
                    .any(|saved| saved.result_id == result.result_id)
            {
                return Err(WorkflowError::internal());
            }
            run.result_references.push(result);
            work.completed_analyses.push(receipt.body.clone());
            work.step = Step::ReadPublished;
        }
        Step::ReadPublished => {
            if receipt.body.get("actionToken")
                != work.receipt(Step::Publish)?.body.get("actionToken")
            {
                return Err(WorkflowError::internal());
            }
            if !matches!(
                receipt
                    .body
                    .pointer("/recommendation/kind")
                    .and_then(Value::as_str),
                Some("action" | "abstain" | "unavailable")
            ) {
                return Err(WorkflowError::internal());
            }
            if work.receipt(Step::Publish)?.arguments.get("market") == Some(&Value::Null)
                && receipt
                    .body
                    .pointer("/recommendation/kind")
                    .and_then(Value::as_str)
                    != Some("unavailable")
            {
                return Err(WorkflowError::internal());
            }
            work.completed_outcomes.push(
                receipt
                    .body
                    .pointer("/recommendation/kind")
                    .and_then(Value::as_str)
                    .ok_or_else(WorkflowError::internal)?
                    .to_owned(),
            );
            work.step = Step::Finished;
        }
        Step::FindPublish => {
            validate_find_result(&receipt.body, work, &run.result_references)?;
            work.step = Step::FindReadPublished;
        }
        Step::FindReadPublished => {
            if receipt.body != work.receipt(Step::FindPublish)?.body {
                return Err(WorkflowError::internal());
            }
            let result = validate_find_result(&receipt.body, work, &run.result_references)?;
            // These ranks come from the saved backend result. No client financial comparison.
            let mut ordered = Vec::with_capacity(result.results.len());
            for row in result.results {
                let Some(action) = row.action_token else {
                    continue;
                };
                let action = action.to_string();
                let reference = run
                    .result_references
                    .iter()
                    .find(|result| result.result_id == action)
                    .ok_or_else(WorkflowError::internal)?;
                ordered.push(reference.clone());
            }
            run.result_references = ordered;
            // The exact GET body has just matched the publication. Keep one bounded aggregate,
            // rather than retaining a second complete copy of every source-unavailable reason.
            work.receipts.remove("FindPublish");
            work.step = Step::Finished;
        }
        _ => return Err(WorkflowError::internal()),
    }
    let key = work.key(step);
    if matches!(
        step,
        Step::FindPartition | Step::FindDataset | Step::FindCompletePartition
    ) {
        // Partition custody and the exact completed dataset reference are retained by their
        // original service owner; native keeps only one acknowledged completion and pending child.
        return Ok(());
    }
    if work.receipts.len() == MAXIMUM_RECEIPTS || work.receipts.insert(key, receipt).is_some() {
        return Err(WorkflowError::internal());
    }
    probability::after_receipt(work, step)?;
    if step == Step::FiscalForecast {
        work.fiscal_index += 1;
        next_fiscal(work);
    }
    if step == Step::StudyTraining {
        work.study_fold += 1;
    }
    Ok(())
}

fn next_fiscal(work: &mut DriverState) {
    while work
        .fiscal_targets
        .get(work.fiscal_index)
        .is_some_and(|target| matches!(target.availability, FiscalAvailability::Unavailable { .. }))
    {
        work.fiscal_index += 1;
    }
    work.step = if work.fiscal_index < work.fiscal_targets.len() {
        Step::FiscalTrainingDataset
    } else {
        Step::HistoricalStudy
    };
}

fn finish_or_continue(
    controller: &AnalyticalWorkflowController,
    token: &str,
) -> Result<(), WorkflowError> {
    controller.mutate(|document, now| {
        let run = workflow_control::find_workflow_mut(document, token)?;
        if run.state == WorkflowRunState::Cancelling {
            return Ok(());
        }
        let work = run.driver.as_mut().ok_or_else(WorkflowError::internal)?;
        if work.step != Step::Finished
            || work.active_job.is_some()
            || run.pending_invocation.is_some()
        {
            return Err(WorkflowError::internal());
        }
        if let Some(find) = &mut work.find {
            if find.candidate_index + 1 < find.candidates.len() {
                find.candidate_index += 1;
                work.selection_token = find.candidates[find.candidate_index]
                    .get("selectionToken")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                work.instrument_id = None;
                work.source_cutoff = None;
                work.fiscal_targets.clear();
                work.fiscal_index = 0;
                work.probability_index = 0;
                work.study_fold = 0;
                work.historical_fiscal = None;
                // The original backend decision now owns all immutable producer references for
                // this completed investment. Native retains its saved result plus every exact
                // child generation; only resumable intermediate response bodies are released.
                archive_unavailable_receipts(work)?;
                work.receipts.retain(|key, _| {
                    key == "ResolveProfile" || key == "FindPopulation" || key == "FindScreen"
                });
                work.step = Step::PrepareSelection;
                run.state = WorkflowRunState::Running;
                run.updated_at = now;
                return Ok(());
            }
            if !work.receipts.contains_key("FindReadPublished") {
                work.step = Step::FindPublish;
                run.state = WorkflowRunState::Running;
                run.updated_at = now;
                return Ok(());
            }
        }
        let deeply_analyzed =
            u32::try_from(work.completed_outcomes.len()).map_err(|_| WorkflowError::internal())?;
        let source_unavailable = work
            .completed_analyses
            .iter()
            .filter(|row| row.get("memberUnavailable").is_some())
            .count();
        if usize::try_from(deeply_analyzed).ok()
            != run.result_references.len().checked_add(source_unavailable)
        {
            return Err(WorkflowError::internal());
        }
        let final_find = work
            .find
            .as_ref()
            .map(|_| {
                validate_find_result(
                    &work.receipt(Step::FindReadPublished)?.body,
                    work,
                    &run.result_references,
                )
            })
            .transpose()?;
        let searched = final_find
            .as_ref()
            .map_or(1, |result| result.coverage.canonical_population_count);
        let population = final_find
            .as_ref()
            .map_or(1, |result| result.coverage.population_count);
        let excluded = final_find
            .as_ref()
            .map_or(0, |result| result.coverage.excluded_count);
        let input_unavailable = final_find
            .as_ref()
            .map_or(0, |result| result.coverage.unavailable_count);
        let count = |kind: &str| {
            u32::try_from(
                work.completed_outcomes
                    .iter()
                    .filter(|value| value.as_str() == kind)
                    .count(),
            )
            .map_err(|_| WorkflowError::internal())
        };
        let generated = count("action")?;
        let no_action = count("abstain")?;
        let unavailable = count("unavailable")?;
        let evidence = if work.find.is_some() {
            let receipt = work.receipt(Step::FindReadPublished)?;
            let result_id = uuid_field(&receipt.body, "preparationId")?.to_string();
            let content_sha256 = receipt
                .body
                .get("resultSha256")
                .and_then(Value::as_str)
                .filter(|value| valid_digest(value))
                .ok_or_else(WorkflowError::internal)?
                .to_owned();
            Some(ServiceResultReference {
                operation: receipt.operation.clone(),
                result_id,
                content_sha256,
            })
        } else {
            None
        };
        let counts = super::CoverageCounts {
            searched,
            population,
            input_unavailable,
            excluded,
            deeply_analyzed,
            generated,
            no_action,
            unavailable,
        };
        run.completion_reference = evidence
            .clone()
            .or_else(|| run.result_references.first().cloned());
        let coverage_digest = hex_digest(Sha256::digest(
            serde_json::to_vec(
                &json!({"counts": counts, "source": evidence, "results": run.result_references}),
            )
            .map_err(|_| WorkflowError::internal())?,
        ));
        run.coverage_receipt = Some(super::CoverageReceipt {
            receipt_id: Uuid::new_v5(&run.run_id, b"coverage"),
            completeness: super::CoverageCompleteness::Complete,
            counts,
            content_sha256: coverage_digest,
        });
        if excluded > 0 {
            let reasons_result = evidence.clone().ok_or_else(WorkflowError::internal)?;
            run.exclusion_receipt = Some(super::ExclusionReceipt {
                receipt_id: Uuid::new_v5(&run.run_id, b"exclusions"),
                excluded_count: excluded,
                content_sha256: reasons_result.content_sha256.clone(),
                reasons_result,
            });
        }
        // The final saved Find owner supplies financial ordering. Native retains it unchanged.
        if let Some(policy_result) = evidence
            && !run.result_references.is_empty()
        {
            run.ranking_receipt = Some(super::RankingReceipt {
                receipt_id: Uuid::new_v5(&run.run_id, b"find-order"),
                ordered_result_ids: run
                    .result_references
                    .iter()
                    .map(|result| result.result_id.clone())
                    .collect(),
                content_sha256: policy_result.content_sha256.clone(),
                policy_result,
            });
        }
        append_checkpoint(
            run,
            &now,
            WorkflowCheckpointStage::CoverageClosed,
            None,
            None,
        )?;
        append_checkpoint(run, &now, WorkflowCheckpointStage::Terminal, None, None)?;
        run.state = WorkflowRunState::Completed;
        run.updated_at = now;
        compact_completed_run(run)?;
        Ok(())
    })
}

fn compact_completed_run(run: &mut WorkflowRun) -> Result<(), WorkflowError> {
    if run.state != WorkflowRunState::Completed || run.completion_reference.is_none() {
        return Err(WorkflowError::internal());
    }
    let work = run.driver.as_mut().ok_or_else(WorkflowError::internal)?;
    let verified = if run.kind == WorkflowKind::FindOpportunities {
        "FindReadPublished"
    } else {
        "ReadPublished"
    };
    if !work.receipts.contains_key(verified) {
        return Err(WorkflowError::internal());
    }
    // The immutable backend result now retains successful producer provenance. Keep unique
    // native source-unavailable checkpoints. Failed child jobs never reach completion.
    if run.kind == WorkflowKind::FindOpportunities {
        run.child_jobs.clear();
    } else {
        run.child_jobs.retain(|job| {
            job.result
                .as_ref()
                .is_some_and(|result| result.operation == "Job.Get")
        });
    }
    archive_unavailable_receipts(work)?;
    work.receipts
        .retain(|key, _| run.kind == WorkflowKind::FindOpportunities && key == "FindReadPublished");
    work.find = None;
    work.completed_analyses.clear();
    work.fiscal_targets.clear();
    work.fiscal_index = 0;
    work.probability_index = 0;
    work.study_fold = 0;
    work.historical_fiscal = None;
    work.revalidating = false;
    work.revalidate_after = None;
    let created = run
        .checkpoint_journal
        .first()
        .cloned()
        .ok_or_else(WorkflowError::internal)?;
    run.checkpoint_journal = vec![created];
    let now = run.updated_at.clone();
    let results = run.result_references.clone();
    for result in results {
        append_checkpoint(
            run,
            &now,
            WorkflowCheckpointStage::ResultsRetained,
            None,
            Some(result),
        )?;
    }
    append_checkpoint(
        run,
        &now,
        WorkflowCheckpointStage::CoverageClosed,
        None,
        None,
    )?;
    append_checkpoint(
        run,
        &now,
        WorkflowCheckpointStage::Terminal,
        None,
        run.completion_reference.clone(),
    )
}

fn archive_unavailable_receipts(work: &mut DriverState) -> Result<(), WorkflowError> {
    for receipt in work.receipts.values() {
        let preparation = if receipt.operation == PREPARATION_RESULT {
            receipt.preparation()?
        } else {
            &receipt.body
        };
        let unavailable = matches!(
            receipt.operation.as_str(),
            "Model.PrepareInvestmentForecast" | "Analysis.PrepareProbabilityEvent"
        ) && receipt
            .body
            .pointer("/availability/state")
            .and_then(Value::as_str)
            == Some("unavailable")
            || preparation.get("status").and_then(Value::as_str) == Some("unavailable")
            || receipt.operation == "Analysis.GetFiscalPreparationPlan"
                && receipt
                    .body
                    .get("targets")
                    .and_then(Value::as_array)
                    .is_some_and(|targets| {
                        targets.iter().any(|target| {
                            target
                                .pointer("/availability/state")
                                .and_then(Value::as_str)
                                == Some("unavailable")
                        })
                    });
        // A source-assessed Find member is already retained in the immutable parent journal;
        // its final aggregate retains the exact reference and visible reason after compaction.
        if unavailable
            && preparation.get("findMemberUnavailable").is_none()
            && !work.completed_unavailable_receipts.contains(receipt)
        {
            if work.completed_unavailable_receipts.len() == MAXIMUM_UNAVAILABLE_RECEIPTS {
                return Err(WorkflowError::internal());
            }
            work.completed_unavailable_receipts.push(receipt.clone());
        }
    }
    Ok(())
}

fn exact_forecast_reference(receipt: &Receipt) -> Result<Value, WorkflowError> {
    if receipt.operation != "Model.GetForecastJobResult" {
        return Ok(Value::Null);
    }
    let job = job_from_receipt(receipt)?;
    Ok(
        json!({"jobId": job.job_id, "generation": generation_number(&job)?,
        "forecastToken": uuid_field(receipt.body.get("forecast").ok_or_else(WorkflowError::internal)?, "forecastToken")?,
        "requestSha256": receipt.body.get("requestSha256").ok_or_else(WorkflowError::internal)?}),
    )
}

/// Mirrors the shared transport envelope; the start binding still hashes all sent arguments.
fn preparation_business_arguments(arguments: &Map<String, Value>) -> Map<String, Value> {
    let mut business = arguments.clone();
    business.remove("confirm");
    business.remove("resultLimits");
    business
}

/// A completed attempt can truthfully have no admitted analytical cutoff. It must be retained,
/// but only an explicit resume may reacquire its original selection and financial configuration.
fn preparation_requires_retry(run: &WorkflowRun, receipt: &Receipt) -> Result<bool, WorkflowError> {
    let work = run.driver.as_ref().ok_or_else(WorkflowError::internal)?;
    if !matches!(work.step, Step::PrepareSelection | Step::FinalPrepare) {
        return Ok(false);
    }
    let preparation = receipt.preparation()?;
    if preparation.get("status").and_then(Value::as_str) != Some("unavailable")
        || preparation.get("preparedAtUnixNanos") != Some(&Value::Null)
    {
        return Ok(false);
    }
    if !matches!(
        preparation.get("reason").and_then(Value::as_str),
        Some("selection_changed" | "identity_unavailable" | "evidence_changed")
    ) || preparation.get("findMemberUnavailable").is_some()
    {
        return Ok(false);
    }
    let (_, arguments, _) = next_invocation(run)?;
    if !receipt.valid()
        || preparation_arguments(&receipt.body)? != &preparation_business_arguments(&arguments)
        || preparation.get("financialConfigurationDigest")
            != DriverState::profile(run)?.get("configurationDigest")
        || preparation.get("scope").and_then(Value::as_str)
            != Some(if work.step == Step::PrepareSelection {
                "investment_analysis"
            } else {
                "current_market"
            })
        || [
            "reference",
            "sourceActionReference",
            "fundamentalShareSources",
        ]
        .iter()
        .any(|key| preparation.get(*key) != Some(&Value::Null))
        || preparation
            .get("sources")
            .and_then(Value::as_array)
            .is_none_or(|sources| !sources.is_empty())
    {
        return Err(WorkflowError::internal());
    }
    match preparation.get("instrumentId") {
        Some(Value::Null)
            if preparation.get("reason").and_then(Value::as_str) == Some("selection_changed") => {}
        Some(Value::String(_)) => {
            let instrument = uuid_field(preparation, "instrumentId")?;
            if work
                .instrument_id
                .is_some_and(|expected| expected != instrument)
            {
                return Err(WorkflowError::internal());
            }
        }
        _ => return Err(WorkflowError::internal()),
    }
    Ok(true)
}

fn validate_preparation_binding(
    body: &Value,
    expected: &Map<String, Value>,
) -> Result<(), WorkflowError> {
    if preparation_arguments(body)? != expected {
        return Err(WorkflowError::internal());
    }
    Ok(())
}

fn preparation_arguments(body: &Value) -> Result<&Map<String, Value>, WorkflowError> {
    let arguments = body
        .get("arguments")
        .and_then(Value::as_object)
        .ok_or_else(WorkflowError::internal)?;
    let digest = hex_digest(Sha256::digest(
        serde_json::to_vec(arguments).map_err(|_| WorkflowError::internal())?,
    ));
    if arguments.contains_key("confirm")
        || arguments.contains_key("resultLimits")
        || body.get("requestSha256").and_then(Value::as_str) != Some(digest.as_str())
    {
        return Err(WorkflowError::internal());
    }
    Ok(arguments)
}

fn checkpoint_body(operation: &str, body: Value) -> Result<Value, WorkflowError> {
    // Keep the exact returned reference fields needed for resumption. Full financial results stay
    // in their original authorities and are reopened there by the eventual financial consumer.
    let fields: &[&str] = match operation {
        "Analysis.GetRecommendationBacktestJobResult" => {
            &["job", "requestDigest", "evidenceDigest"]
        }
        "Decision.GenerateInvestmentAnalysis" => &[
            "actionToken",
            "analysisId",
            "explanationDigest",
            "publishedAtUnixNanos",
            "valuationMethodSetIdentity",
        ],
        "Decision.GetInvestmentAnalysis" => &["actionToken", "recommendation"],
        "Model.GetForecastJobResult" => {
            &["job", "forecast", "requestSha256", "financialProfileDigest"]
        }
        _ => return Ok(body),
    };
    let mut retained = Map::new();
    for field in fields {
        retained.insert(
            (*field).to_owned(),
            body.get(*field)
                .cloned()
                .ok_or_else(WorkflowError::internal)?,
        );
    }
    if operation == "Model.GetForecastJobResult" {
        retained.insert("forecast".to_owned(), json!({"forecastToken": body.pointer("/forecast/forecastToken").ok_or_else(WorkflowError::internal)?, "observedThroughUnixNanos": body.pointer("/forecast/observedThroughUnixNanos").cloned().unwrap_or(Value::Null)}));
    }
    Ok(Value::Object(retained))
}

fn publication_arguments(run: &WorkflowRun) -> Result<Value, WorkflowError> {
    let work = run.driver.as_ref().ok_or_else(WorkflowError::internal)?;
    let price = work
        .receipts
        .get("PriceForecast")
        .map(exact_forecast_reference)
        .transpose()?
        .unwrap_or(Value::Null);
    let mut fiscal = Vec::new();
    for (key, receipt) in &work.receipts {
        if key.starts_with("FiscalForecast-") && receipt.operation == "Model.GetForecastJobResult" {
            fiscal.push(exact_forecast_reference(receipt)?);
        }
    }
    let selected = work
        .find
        .as_ref()
        .map(|find| {
            find.candidates
                .get(find.candidate_index)
                .ok_or_else(WorkflowError::internal)
        })
        .transpose()?;
    let selected = selected.map(|row| json!({"candidateId": row.get("candidateId"), "screenRunId": row.get("screenRunId"), "evidenceDigest": row.get("evidenceDigest")}));
    let workflow_id = if let Some(find) = &work.find {
        let candidate = find
            .candidates
            .get(find.candidate_index)
            .ok_or_else(WorkflowError::internal)?;
        Uuid::new_v5(
            &run.run_id,
            candidate
                .get("candidateId")
                .and_then(Value::as_str)
                .ok_or_else(WorkflowError::internal)?
                .as_bytes(),
        )
    } else {
        run.run_id
    };
    let study = work.receipts.get("StudyBacktest").filter(|receipt| receipt.operation == "Analysis.GetRecommendationBacktestJobResult")
        .map(|receipt| json!({"requestDigest": receipt.body.get("requestDigest"), "evidenceDigest": receipt.body.get("evidenceDigest")}));
    let probabilities = probability::publication(work)?;
    let market = work
        .receipt(Step::FinalEvidence)?
        .body
        .get("reference")
        .cloned()
        .unwrap_or(Value::Null);
    let source_action_reference = if price.is_null() {
        Value::Null
    } else {
        work.initial_source_action_reference()?.clone()
    };
    let current_share_action_reference =
        if price.is_null() || source_action_reference.is_null() || market.is_null() {
            Value::Null
        } else {
            work.receipt(Step::FinalPrepare)?
                .preparation()?
                .get("sourceActionReference")
                .cloned()
                .unwrap_or(Value::Null)
        };
    let fundamental_share_sources = work
        .receipt(Step::FinalPrepare)?
        .preparation()?
        .get("fundamentalShareSources")
        .cloned()
        .ok_or_else(WorkflowError::internal)?;
    let binding = json!({"instrumentId": work.instrument_id, "sourceCutoffUnixNanos": work.source_cutoff,
        "financialProfile": DriverState::profile(run)?, "priceForecast": price, "sourceActionReference": source_action_reference, "currentShareActionReference": current_share_action_reference,
        "fundamentalShareSources": fundamental_share_sources, "probabilityForecasts": probabilities, "benchmarkInstrumentId":work.benchmark_instrument_id, "financialForecasts": fiscal,
        "historicalStudy": study, "selectedCandidate": selected});
    let digest = hex_digest(Sha256::digest(
        serde_json::to_vec(&binding).map_err(|_| WorkflowError::internal())?,
    ));
    let revision = u32::try_from(run.profile.active.profile_revision)
        .map_err(|_| WorkflowError::internal())?;
    Ok(json!({"financialProfile": DriverState::profile(run)?,
        "analyticalProfile": {"profileId": run.profile.active.profile_id, "revision": revision, "contentSha256": run.profile.active.config_digest},
        "workflow": {"workflowId": workflow_id, "revision": 1, "contentSha256": digest},
        "market": market,
        "portfolio": work.receipt(Step::FinalPortfolio)?.body.get("reference").ok_or_else(WorkflowError::internal)?,
        "sourceCutoffUnixNanos": work.source_cutoff, "priceForecast": price,
        "sourceActionReference": source_action_reference,
        "currentShareActionReference": current_share_action_reference,
        "fundamentalShareSources": fundamental_share_sources,
        "probabilityForecasts": probabilities, "benchmarkInstrumentId":work.benchmark_instrument_id,
        "financialForecasts": fiscal, "historicalStudy": study, "selectedCandidate": selected}))
}

pub(super) async fn recover_pending(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    token: &str,
    pending: &PendingCapabilityInvocation,
) -> Result<(), WorkflowError> {
    // The original limits and digest are checked, never normalized after an uncertain call.
    let canonical = crate::application::analytical_workflow::host::prepare_analytical_arguments(
        generation,
        &pending.operation,
        pending.arguments.clone(),
        InvocationAuthority::ReadOnly,
    )?;
    if canonical != pending.arguments || !super::valid_pending_invocation(pending) {
        return Err(WorkflowError::internal());
    }
    if pending.operation == "Analysis.CompleteHistoricalStudyFiscalPage" {
        // Replay exact original work. An unknown publication may have a newer real selection
        // clock on retry; an acknowledged reference is never regenerated by this branch.
        let body = call(
            generation,
            "Analysis.CompleteHistoricalStudyFiscalPage",
            pending.arguments.clone(),
            true,
            RequestId::try_string(pending.request_id.clone())
                .map_err(|_| WorkflowError::internal())?,
        )
        .await?;
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        return generation
            .analytical_controller()
            .retain_response(token, pending, body);
    }
    if matches!(
        pending.operation.as_str(),
        "Decision.PrepareCurrentScreen"
            | "Analysis.PrepareCurrentScreenPartition"
            | "Analysis.CompleteCurrentScreenPartition"
    ) {
        // Custody preparation persists original request identity/cutoff or partition identity.
        // It is replayed identically; a lost reply never authorizes a later source selection.
        let operation = match pending.operation.as_str() {
            "Decision.PrepareCurrentScreen" => "Decision.PrepareCurrentScreen",
            "Analysis.PrepareCurrentScreenPartition" => "Analysis.PrepareCurrentScreenPartition",
            "Analysis.CompleteCurrentScreenPartition" => "Analysis.CompleteCurrentScreenPartition",
            _ => return Err(WorkflowError::internal()),
        };
        let body = call(
            generation,
            operation,
            pending.arguments.clone(),
            true,
            RequestId::try_string(pending.request_id.clone())
                .map_err(|_| WorkflowError::internal())?,
        )
        .await?;
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        return generation
            .analytical_controller()
            .retain_response(token, pending, body);
    }
    if matches!(
        pending.operation.as_str(),
        "Decision.GenerateInvestmentAnalysis" | "Decision.PublishFindResults"
    ) {
        // The backend atomically binds this exact request to its per-analysis workflow identity.
        // An identical retry reads the already-published bundle before current-source admission;
        // expiry never permits a changed request or a fresh source cutoff under that identity.
        let operation = if pending.operation == "Decision.GenerateInvestmentAnalysis" {
            "Decision.GenerateInvestmentAnalysis"
        } else {
            "Decision.PublishFindResults"
        };
        let body = call(
            generation,
            operation,
            pending.arguments.clone(),
            true,
            RequestId::try_string(format!(
                "desktop-publication-recover-{}",
                Uuid::new_v4().simple()
            ))
            .map_err(|_| WorkflowError::internal())?,
        )
        .await?;
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        return generation
            .analytical_controller()
            .retain_response(token, pending, body);
    }
    if job_result_operation(&pending.operation).is_none() {
        // Reads/expiring preparations are discarded on interruption. They confer no durable
        // result or background admission; their original cutoffs are retained in driver state.
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        return generation.analytical_controller().mutate(|document, now| {
            let run = workflow_control::find_workflow_mut(document, token)?;
            if run.pending_invocation.as_ref() != Some(pending) {
                return Err(WorkflowError::internal());
            }
            run.pending_invocation = None;
            append_checkpoint(
                run,
                &now,
                WorkflowCheckpointStage::CapabilityCompleted,
                None,
                None,
            )?;
            run.updated_at = now;
            Ok(())
        });
    }
    // Transport digests include confirm exactly as sent; no token is ever replayed here.
    let body = call(
        generation,
        "Job.ReconcileStart",
        object(json!({"requestId": pending.request_id,
        "operation": pending.operation, "argumentsSha256": pending.arguments_sha256}))?,
        false,
        RequestId::try_string(format!("desktop-reconcile-{}", Uuid::new_v4().simple()))
            .map_err(|_| WorkflowError::internal())?,
    )
    .await?;
    match body.get("state").and_then(Value::as_str) {
        Some("admitted") => {
            let job = body
                .get("job")
                .cloned()
                .ok_or_else(WorkflowError::internal)?;
            let _fence = generation.analytical_retirement_fence().await;
            state.admit_current(generation)?;
            generation
                .analytical_controller()
                .retain_response(token, pending, job)
        }
        Some("pending") => Ok(()),
        Some("unknown") => {
            let cancelled = call(
                generation,
                "Job.CancelStart",
                object(json!({"requestId": pending.request_id,
                "operation": pending.operation, "argumentsSha256": pending.arguments_sha256}))?,
                true,
                RequestId::try_string(format!("desktop-settle-{}", Uuid::new_v4().simple()))
                    .map_err(|_| WorkflowError::internal())?,
            )
            .await?;
            match cancelled.get("state").and_then(Value::as_str) {
                Some("admitted") => {
                    let _fence = generation.analytical_retirement_fence().await;
                    state.admit_current(generation)?;
                    generation.analytical_controller().retain_response(
                        token,
                        pending,
                        cancelled
                            .get("job")
                            .cloned()
                            .ok_or_else(WorkflowError::internal)?,
                    )
                }
                Some("not_admitted") if cancelled.get("job") == Some(&Value::Null) => {
                    discard_unadmitted(state, generation, token, pending).await
                }
                Some("pending") => Ok(()),
                _ => Err(WorkflowError::internal()),
            }
        }
        Some("not_admitted") if body.get("job") == Some(&Value::Null) => {
            discard_unadmitted(state, generation, token, pending).await
        }
        _ => Err(WorkflowError::internal()),
    }
}

async fn discard_unadmitted(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    token: &str,
    pending: &PendingCapabilityInvocation,
) -> Result<(), WorkflowError> {
    let _fence = generation.analytical_retirement_fence().await;
    state.admit_current(generation)?;
    generation.analytical_controller().mutate(|document, now| {
        let run = workflow_control::find_workflow_mut(document, token)?;
        if run.pending_invocation.as_ref() != Some(pending) {
            return Err(WorkflowError::internal());
        }
        let work = run.driver.as_mut().ok_or_else(WorkflowError::internal)?;
        if work.step == Step::PriceForecast {
            let key = work.key(Step::PreparePriceForecast);
            work.receipts.remove(&key);
            work.step = Step::PreparePriceForecast;
        }
        run.pending_invocation = None;
        append_checkpoint(
            run,
            &now,
            WorkflowCheckpointStage::CapabilityCompleted,
            None,
            None,
        )?;
        run.updated_at = now;
        Ok(())
    })
}

async fn poll_job(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    _run: &WorkflowRun,
    token: &str,
    active: &ActiveJob,
) -> Result<(), WorkflowError> {
    let response = call(
        generation,
        "Job.Get",
        job_arguments(&active.reference)?,
        false,
        RequestId::try_string(format!("desktop-watch-{}", Uuid::new_v4().simple()))
            .map_err(|_| WorkflowError::internal())?,
    )
    .await?;
    let sequence = workflow_control::validate_child(&response, &active.reference)?;
    if sequence < active.observed_sequence {
        return Err(WorkflowError::internal());
    }
    let status = response
        .get("state")
        .and_then(Value::as_str)
        .ok_or_else(WorkflowError::internal)?;
    if !matches!(status, "completed" | "failed" | "cancelled" | "interrupted") {
        if sequence == active.observed_sequence {
            return Ok(());
        }
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        return generation.analytical_controller().mutate(|document, now| {
            let retained = workflow_control::find_workflow_mut(document, token)?;
            let observed = retained
                .driver
                .as_mut()
                .and_then(|driver| driver.active_job.as_mut())
                .ok_or_else(WorkflowError::internal)?;
            if observed != active {
                return Err(WorkflowError::internal());
            }
            observed.observed_sequence = sequence;
            retained.updated_at = now;
            Ok(())
        });
    }
    let operation = match active.result_operation.as_str() {
        PREPARATION_RESULT => PREPARATION_RESULT,
        "Analysis.GetPreparedDatasetJobResult" => "Analysis.GetPreparedDatasetJobResult",
        "Model.GetTrainingJobResult" => "Model.GetTrainingJobResult",
        "Model.GetForecastJobResult" => "Model.GetForecastJobResult",
        "Decision.GetCurrentScreenJobResult" => "Decision.GetCurrentScreenJobResult",
        "Analysis.GetRecommendationBacktestJobResult" => {
            "Analysis.GetRecommendationBacktestJobResult"
        }
        _ => return Err(WorkflowError::internal()),
    };
    if status != "completed" {
        let bytes = serde_json::to_vec(&response).map_err(|_| WorkflowError::internal())?;
        if bytes.len() > MAXIMUM_RECEIPT_BYTES {
            return Err(WorkflowError::internal());
        }
        let receipt = Receipt {
            operation: "Job.Get".to_owned(),
            arguments: job_arguments(&active.reference)?,
            sha256: hex_digest(Sha256::digest(bytes)),
            body: response,
        };
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        return generation.analytical_controller().mutate(|document, now| {
            let retained = workflow_control::find_workflow_mut(document, token)?;
            if retained
                .driver
                .as_ref()
                .and_then(|driver| driver.active_job.as_ref())
                != Some(active)
            {
                return Err(WorkflowError::internal());
            }
            let child = retained
                .child_jobs
                .iter_mut()
                .find(|child| {
                    child.job_id == active.reference.job_id
                        && child.generation == active.reference.generation
                })
                .ok_or_else(WorkflowError::internal)?;
            child.terminal_sequence = Some(sequence.to_string());
            child.result = Some(ServiceResultReference {
                operation: "Job.Get".to_owned(),
                result_id: format!("{}:{}", child.job_id, child.generation),
                content_sha256: receipt.sha256.clone(),
            });
            let completed = child.clone();
            let work = retained
                .driver
                .as_mut()
                .ok_or_else(WorkflowError::internal)?;
            let key = work.key(active.step);
            if work.receipts.len() == MAXIMUM_RECEIPTS
                || work.receipts.insert(key, receipt).is_some()
            {
                return Err(WorkflowError::internal());
            }
            work.active_job = None;
            if retained.state != WorkflowRunState::Cancelling {
                // A terminal job failure is not source-absence evidence. Cancellation,
                // interrupted execution and integrity/resource failures stop this workflow;
                // only an explicit successful typed absence result can skip dependent work.
                retained.state = WorkflowRunState::Failed;
                retained.last_error = Some("analysis_job_failed".to_owned());
            }
            append_checkpoint(
                retained,
                &now,
                WorkflowCheckpointStage::CapabilityCompleted,
                Some(completed),
                None,
            )?;
            retained.updated_at = now;
            Ok(())
        });
    }
    let body = call(
        generation,
        operation,
        active.result_arguments.clone(),
        false,
        RequestId::try_string(format!("desktop-result-{}", Uuid::new_v4().simple()))
            .map_err(|_| WorkflowError::internal())?,
    )
    .await?;
    if workflow_control::validate_child(
        body.get("job").ok_or_else(WorkflowError::internal)?,
        &active.reference,
    )? != sequence
    {
        return Err(WorkflowError::internal());
    }
    if operation == PREPARATION_RESULT {
        validate_preparation_binding(
            &body,
            active
                .preparation_arguments
                .as_ref()
                .ok_or_else(WorkflowError::internal)?,
        )?;
    }
    let body = checkpoint_body(operation, body)?;
    let bytes = serde_json::to_vec(&body).map_err(|_| WorkflowError::internal())?;
    if bytes.len() > maximum_receipt_bytes(operation) {
        return Err(WorkflowError::internal());
    }
    let receipt = Receipt {
        operation: operation.to_owned(),
        arguments: active.result_arguments.clone(),
        sha256: hex_digest(Sha256::digest(bytes)),
        body,
    };
    let _fence = generation.analytical_retirement_fence().await;
    state.admit_current(generation)?;
    generation.analytical_controller().mutate(|document, now| {
        let retained = workflow_control::find_workflow_mut(document, token)?;
        retain_completed_job(retained, active, receipt, sequence, &now)
    })
}

fn retain_completed_job(
    retained: &mut WorkflowRun,
    active: &ActiveJob,
    receipt: Receipt,
    sequence: u64,
    now: &str,
) -> Result<(), WorkflowError> {
    if retained
        .driver
        .as_ref()
        .and_then(|driver| driver.active_job.as_ref())
        != Some(active)
    {
        return Err(WorkflowError::internal());
    }
    let child = retained
        .child_jobs
        .iter_mut()
        .find(|child| {
            child.job_id == active.reference.job_id
                && child.generation == active.reference.generation
        })
        .ok_or_else(WorkflowError::internal)?;
    child.terminal_sequence = Some(sequence.to_string());
    child.result = Some(ServiceResultReference {
        operation: receipt.operation.clone(),
        result_id: format!("{}:{}", child.job_id, child.generation),
        content_sha256: receipt.sha256.clone(),
    });
    let completed = child.clone();
    if retained.state != WorkflowRunState::Cancelling {
        apply_receipt(retained, receipt, now)?;
        if retained.state != WorkflowRunState::Paused {
            retained.state = WorkflowRunState::Running;
        }
    }
    retained
        .driver
        .as_mut()
        .ok_or_else(WorkflowError::internal)?
        .active_job = None;
    append_checkpoint(
        retained,
        now,
        WorkflowCheckpointStage::CapabilityCompleted,
        Some(completed),
        None,
    )?;
    if active.step == Step::StudyFiscalInputDataset
        && retained.state != WorkflowRunState::Cancelling
    {
        compact_historical_frontier(retained, now)?;
        append_checkpoint(
            retained,
            now,
            WorkflowCheckpointStage::CapabilityCompleted,
            None,
            None,
        )?;
    }
    retained.updated_at = now.to_owned();
    Ok(())
}

async fn revalidate(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    run: &WorkflowRun,
    token: &str,
) -> Result<(), WorkflowError> {
    let work = run.driver.as_ref().ok_or_else(WorkflowError::internal)?;
    if revalidate_historical_frontier(state, generation, run, token).await? {
        return Ok(());
    }
    if work.revalidate_after.is_none()
        && let Some(find) = &work.find
        && let Some(saved) = &find.completed_partition
    {
        let mut arguments = object(work.find_preparation()?)?;
        arguments.insert(
            "completion".into(),
            saved
                .body
                .get("completion")
                .cloned()
                .ok_or_else(WorkflowError::internal)?,
        );
        let actual = call(
            generation,
            "Decision.ReadCurrentScreenPartitionCompletion",
            arguments,
            false,
            RequestId::try_string(format!("desktop-find-frontier-{}", Uuid::new_v4().simple()))
                .map_err(|_| WorkflowError::internal())?,
        )
        .await?;
        if actual != saved.body || !saved.valid() {
            return Err(WorkflowError::internal());
        }
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        return generation.analytical_controller().mutate(|document, now| {
            let retained = workflow_control::find_workflow_mut(document, token)?;
            retained
                .driver
                .as_mut()
                .ok_or_else(WorkflowError::internal)?
                .revalidate_after = Some(String::new());
            retained.updated_at = now;
            Ok(())
        });
    }
    if work.revalidate_after.is_none()
        && let Some(find) = &work.find
    {
        let actual = call(
            generation,
            "Decision.ReadCurrentScreenPreparation",
            object(work.find_preparation()?)?,
            false,
            RequestId::try_string(format!("desktop-find-reopen-{}", Uuid::new_v4().simple()))
                .map_err(|_| WorkflowError::internal())?,
        )
        .await?;
        for key in [
            "preparationId",
            "preparationSha256",
            "sourceCutoffUnixNanos",
            "partitionCount",
            "populationCount",
            "forecastCohort",
        ] {
            if actual.get(key) != find.preparation.get(key) {
                return Err(WorkflowError::internal());
            }
        }
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        return generation.analytical_controller().mutate(|document, now| {
            let retained = workflow_control::find_workflow_mut(document, token)?;
            retained
                .driver
                .as_mut()
                .ok_or_else(WorkflowError::internal)?
                .revalidate_after = Some(String::new());
            retained.updated_at = now;
            Ok(())
        });
    }
    let next = work
        .receipts
        .iter()
        .filter(|(key, _)| {
            work.revalidate_after
                .as_ref()
                .is_none_or(|after| *key > after)
        })
        .find(|(_, receipt)| {
            matches!(
                receipt.operation.as_str(),
                PREPARATION_RESULT
                    | "Analysis.PrepareProbabilityEvent"
                    | "Analysis.GetPreparedDatasetJobResult"
                    | "Model.GetTrainingJobResult"
                    | "Model.GetForecastJobResult"
                    | "Analysis.GetRecommendationBacktestJobResult"
                    | "Decision.GetCurrentScreenJobResult"
                    | "Job.Get"
            )
        });
    if let Some((key, receipt)) = next {
        let operation = match receipt.operation.as_str() {
            PREPARATION_RESULT => PREPARATION_RESULT,
            "Analysis.PrepareProbabilityEvent" => "Analysis.PrepareProbabilityEvent",
            "Analysis.GetPreparedDatasetJobResult" => "Analysis.GetPreparedDatasetJobResult",
            "Model.GetTrainingJobResult" => "Model.GetTrainingJobResult",
            "Model.GetForecastJobResult" => "Model.GetForecastJobResult",
            "Analysis.GetRecommendationBacktestJobResult" => {
                "Analysis.GetRecommendationBacktestJobResult"
            }
            "Decision.GetCurrentScreenJobResult" => "Decision.GetCurrentScreenJobResult",
            "Job.Get" => "Job.Get",
            _ => return Err(WorkflowError::internal()),
        };
        let actual = call(
            generation,
            operation,
            receipt.arguments.clone(),
            false,
            RequestId::try_string(format!("desktop-reopen-{}", Uuid::new_v4().simple()))
                .map_err(|_| WorkflowError::internal())?,
        )
        .await?;
        if checkpoint_body(operation, actual)? != receipt.body {
            return Err(WorkflowError::new(
                "analysis_evidence_changed",
                "The saved investment information has changed. Start a new analysis.",
            ));
        }
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        generation.analytical_controller().mutate(|document, now| {
            let retained = workflow_control::find_workflow_mut(document, token)?;
            retained
                .driver
                .as_mut()
                .ok_or_else(WorkflowError::internal)?
                .revalidate_after = Some(key.clone());
            retained.updated_at = now;
            Ok(())
        })
    } else {
        let resolved = financial_profiles::resolve_configuration(
            generation,
            &run.profile.config.financial_configuration,
            &run.profile.active.config_digest,
        )
        .await?;
        if run
            .profile
            .financial_resolution
            .as_ref()
            .is_some_and(|retained| retained != &resolved)
        {
            return Err(WorkflowError::internal());
        }
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        generation.analytical_controller().mutate(|document, now| {
            let retained = workflow_control::find_workflow_mut(document, token)?;
            let driver = retained
                .driver
                .as_mut()
                .ok_or_else(WorkflowError::internal)?;
            driver.revalidating = false;
            driver.revalidate_after = None;
            retained.updated_at = now;
            Ok(())
        })
    }
}

fn append_checkpoint(
    run: &mut WorkflowRun,
    now: &str,
    stage: WorkflowCheckpointStage,
    child_job: Option<ServiceJobReference>,
    result: Option<ServiceResultReference>,
) -> Result<(), WorkflowError> {
    if run.checkpoint_journal.len() == super::MAXIMUM_CHECKPOINTS_PER_RUN {
        return Err(WorkflowError::internal());
    }
    let sequence = run
        .checkpoint_journal
        .last()
        .and_then(|checkpoint| checkpoint.sequence.checked_add(1))
        .ok_or_else(WorkflowError::internal)?;
    run.checkpoint_journal.push(WorkflowCheckpoint {
        sequence,
        stage,
        recorded_at: now.to_owned(),
        child_job,
        result,
    });
    Ok(())
}
fn object(value: Value) -> Result<Map<String, Value>, WorkflowError> {
    value
        .as_object()
        .cloned()
        .ok_or_else(WorkflowError::internal)
}
fn uuid_field(value: &Value, key: &str) -> Result<Uuid, WorkflowError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .and_then(|id| id.parse::<Uuid>().ok())
        .filter(|id| !id.is_nil())
        .ok_or_else(WorkflowError::internal)
}
fn generation_number(job: &ServiceJobReference) -> Result<u64, WorkflowError> {
    job.generation
        .parse()
        .map_err(|_| WorkflowError::internal())
}
fn job_arguments(job: &ServiceJobReference) -> Result<Map<String, Value>, WorkflowError> {
    object(json!({"jobId": job.job_id, "generation": generation_number(job)?}))
}
fn job_from_receipt(receipt: &Receipt) -> Result<ServiceJobReference, WorkflowError> {
    workflow_control::job_reference(
        receipt
            .body
            .get("job")
            .ok_or_else(WorkflowError::internal)?,
    )
}

fn price_preparation(receipt: &Receipt) -> Result<PriceForecastPreparation, WorkflowError> {
    let prepared: PriceForecastPreparation =
        serde_json::from_value(receipt.body.clone()).map_err(|_| WorkflowError::internal())?;
    if receipt
        .body
        .as_object()
        .is_none_or(|value| value.len() != 8)
        || prepared.instrument_id.is_nil()
        || receipt
            .arguments
            .get("instrumentId")
            .and_then(Value::as_str)
            != Some(prepared.instrument_id.to_string().as_str())
        || receipt
            .arguments
            .get("sourceCutoffUnixNanos")
            .and_then(Value::as_str)
            != Some(prepared.source_cutoff_unix_nanos.as_str())
        || !super::valid_timestamp(&prepared.source_cutoff_unix_nanos)
        || prepared
            .forecast_cohort
            .as_ref()
            .is_some_and(|value| !value.is_object())
        || receipt.arguments.get("forecastCohort") != prepared.forecast_cohort.as_ref()
        || (prepared.forecast_cohort.is_some()
            && receipt
                .arguments
                .get("currentFeatureInput")
                .is_none_or(|value| !value.is_object()))
        || prepared
            .expected_observed_through_unix_nanos
            .as_deref()
            .is_some_and(|value| !super::valid_timestamp(value))
        || !valid_digest(&prepared.financial_profile_digest)
        || receipt
            .arguments
            .get("financialProfile")
            .and_then(|value| value.get("configurationDigest"))
            .and_then(Value::as_str)
            != Some(prepared.financial_profile_digest.as_str())
    {
        return Err(WorkflowError::internal());
    }
    match &prepared.availability {
        PriceForecastAvailability::Ready { .. } => {
            if prepared
                .forecast
                .as_ref()
                .is_none_or(|value| !value.is_object())
                || !prepared.request_sha256.as_deref().is_some_and(valid_digest)
                || receipt
                    .body
                    .pointer("/forecast/confirmationToken")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                || (prepared.forecast_cohort.is_some()
                    && prepared.expected_observed_through_unix_nanos.is_none())
                || prepared
                    .expected_observed_through_unix_nanos
                    .as_deref()
                    .is_some_and(|expected| {
                        receipt
                            .body
                            .pointer("/forecast/observedThroughUnixNanos")
                            .and_then(Value::as_str)
                            != Some(expected)
                    })
            {
                return Err(WorkflowError::internal());
            }
        }
        PriceForecastAvailability::Unavailable { .. } => {
            if prepared.forecast.is_some()
                || prepared.request_sha256.is_some()
                || prepared.expected_observed_through_unix_nanos.is_some()
            {
                return Err(WorkflowError::internal());
            }
        }
    }
    Ok(prepared)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::decision::investment_request::{
        GenerateRequest, validate_canonical_request,
    };
    use market_squawk_domain::{CalendarDate, DigestAlgorithm, EvidenceDigest, Timestamp};

    fn receipt(operation: &str, arguments: Value, body: Value) -> Receipt {
        Receipt {
            operation: operation.to_owned(),
            arguments: serde_json::from_value(arguments).expect("receipt arguments"),
            sha256: hex_digest(Sha256::digest(
                serde_json::to_vec(&body).expect("receipt bytes"),
            )),
            body,
        }
    }

    fn preparation_receipt(
        run: &WorkflowRun,
        preparation: Value,
    ) -> Result<Receipt, WorkflowError> {
        let (operation, arguments, mutation) = next_invocation(run)?;
        assert_eq!(operation, "Market.PrepareInvestmentEvidence");
        assert!(mutation);
        assert_eq!(job_result_operation(operation), Some(PREPARATION_RESULT));
        let original = preparation_business_arguments(&arguments);
        let job_id = run
            .driver
            .as_ref()
            .and_then(|driver| driver.active_job.as_ref())
            .map_or_else(Uuid::new_v4, |active| active.reference.job_id);
        let job = json!({"jobId": job_id, "generation": 1, "sequence": 3, "state": "completed"});
        let reference = workflow_control::job_reference(&job)?;
        let active = ActiveJob {
            step: run
                .driver
                .as_ref()
                .ok_or_else(WorkflowError::internal)?
                .step,
            result_operation: PREPARATION_RESULT.to_owned(),
            reference: reference.clone(),
            observed_sequence: 1,
            result_arguments: job_arguments(&reference)?,
            preparation_arguments: Some(original.clone()),
        };
        // Retained active-job serialization keeps the original request binding.
        let active: ActiveJob = serde_json::from_value(
            serde_json::to_value(active).map_err(|_| WorkflowError::internal())?,
        )
        .map_err(|_| WorkflowError::internal())?;
        let body = json!({
            "job": job,
            "preparation": preparation,
            "arguments": original,
            "requestSha256": hex_digest(Sha256::digest(
                serde_json::to_vec(&original).map_err(|_| WorkflowError::internal())?
            )),
        });
        let expected = active
            .preparation_arguments
            .as_ref()
            .ok_or_else(WorkflowError::internal)?;
        validate_preparation_binding(&body, expected)?;
        let mut changed = body.clone();
        changed["arguments"]["findMember"] = json!({"member": "another-retained-member"});
        changed["requestSha256"] = json!(hex_digest(Sha256::digest(
            serde_json::to_vec(&changed["arguments"]).map_err(|_| WorkflowError::internal())?
        )));
        // A self-consistent digest does not authorize substituting the original Find member.
        assert!(validate_preparation_binding(&changed, expected).is_err());
        changed = body.clone();
        changed["requestSha256"] = json!("0".repeat(64));
        assert!(validate_preparation_binding(&changed, expected).is_err());
        let retained = receipt(
            PREPARATION_RESULT,
            Value::Object(active.result_arguments),
            body,
        );
        assert!(retained.valid());
        let reopened: Receipt = serde_json::from_value(
            serde_json::to_value(&retained).map_err(|_| WorkflowError::internal())?,
        )
        .map_err(|_| WorkflowError::internal())?;
        assert_eq!(reopened, retained);
        assert_eq!(reopened.preparation()?, &preparation);
        Ok(retained)
    }

    #[test]
    fn publication_action_references_follow_forecast_and_market_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::application::application_capabilities()?;
        let directory = tempfile::tempdir()?;
        let paths = market_squawk_platform::LocalPaths::prepare(directory.path())?;
        let workspace = Uuid::new_v4();
        let controller = AnalyticalWorkflowController::try_open(&paths, workspace)?;
        let selection = crate::application::market_selection::product::token(
            "market_",
            b"workflow-publication-regression",
            &[workspace.as_bytes()],
        )?;
        controller.begin_workflow(
            WorkflowKind::AnalyzeInvestment,
            Some(selection.into_string()),
            None,
            super::super::host::WorkflowOrigin::new(
                market_squawk_services::RequestOrigin::try_new(workspace, Uuid::new_v4())?,
            ),
        )?;
        let mut run = controller.next_run()?.ok_or("missing workflow")?;
        let profile = serde_json::to_value(
            crate::application::analytical_profile::resolve(None, None)?.resolution(),
        )?;
        let cutoff = 1_800_000_000_000_000_000_i64;
        let current = cutoff + 1_000_000_000;
        let now = current.to_string();
        let instrument = Uuid::new_v4();
        let digest = "1".repeat(64);
        let evidence_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]);
        let date = CalendarDate::new(2026, 1, 2)?;
        // Inert reference fixtures exercise request admission; they confer no source authority.
        let original = json!({
            "version": 1, "source_origin_content": evidence_digest,
            "source_binding": evidence_digest, "source_snapshot": evidence_digest,
            "calendar": {"originContentDigest": digest, "captureBindingDigest": digest},
            "requested_instruments": [instrument], "interval": [date, date],
            "knowledge_cutoff": Timestamp::from_unix_nanos(cutoff),
            "valuation_cutoff": Timestamp::from_unix_nanos(cutoff),
            "evaluated_at": Timestamp::from_unix_nanos(cutoff),
            "adjustment": 1, "policy_version": 1, "payment_policy": "retain_receivable",
            "content_hash": evidence_digest, "audit_hash": evidence_digest, "ordinary": [],
            "current_ordinary": null, "current_ordinary_digest": null,
            "ordinary_coverage_digest": null, "anchor": null
        });
        let mut current_actions = original.clone();
        for field in ["knowledge_cutoff", "valuation_cutoff", "evaluated_at"] {
            current_actions[field] = json!(Timestamp::from_unix_nanos(current));
        }
        let market = json!({
            "instrumentId": instrument, "sourceCutoffUnixNanos": now,
            "maximumMarkAgeNanos": "1000000000", "evidenceDigest": digest,
            "sourceSelectionDigest": digest, "rightsInputDigest": digest,
            "publicationSelectionDigest": digest, "definitionSelectionDigest": digest,
            "priceAuthorityDigest": digest, "sourceScopeDigests": null
        });
        let portfolio = json!({"calendar": null, "prerequisites": {
            "candidateInstrumentId": instrument, "sourceCutoffUnixNanos": now,
            "accountId": Uuid::new_v4(), "portfolioRevision": digest,
            "setupAuthorityDigest": digest, "configurationDigest": digest,
            "profileDigest": digest, "catalogDigest": digest, "prerequisitePolicyDigest": digest,
            "minimumHistoricalReturnObservations": 60, "maximumHistoricalReturnObservations": 252,
            "evidenceDigest": digest, "portfolioSnapshotDigest": digest, "marketSetDigest": digest,
            "calculatedAtUnixNanos": now, "status": "unavailable"
        }});
        apply_receipt(
            &mut run,
            receipt("AnalyticalProfile.Resolve", json!({}), profile.clone()),
            &now,
        )?;
        let unavailable = json!({
            "status": "unavailable", "reason": "evidence_changed",
            "scope": "investment_analysis", "instrumentId": instrument,
            "financialConfigurationDigest": profile["configurationDigest"],
            "preparedAtUnixNanos": null, "reference": null, "sourceActionReference": null,
            "fundamentalShareSources": null, "sources": []
        });
        // The real completed-job transition must commit its unavailable result before pausing.
        // Reopening alone cannot create another preparation; an explicit resume keeps this run.
        let token = opaque_workflow_token(&run)?;
        controller.mutate(|document, _| {
            document.workflow_runs[0] = run.clone();
            Ok(())
        })?;
        let (operation, arguments, _) = next_invocation(&run)?;
        let first = controller.retain_pending(&token, operation, arguments)?;
        let first_job = Uuid::new_v4();
        controller.retain_response(
            &token,
            &first,
            json!({"jobId": first_job, "generation": 1, "sequence": 1, "state": "queued"}),
        )?;
        let waiting = controller
            .next_run()?
            .ok_or("missing waiting preparation")?;
        let stale = preparation_receipt(&waiting, unavailable.clone())?;
        let mut tampered = stale.clone();
        tampered.body["requestSha256"] = json!("0".repeat(64));
        assert!(apply_receipt(&mut waiting.clone(), tampered, &now).is_err());
        let mut mismatched = stale.clone();
        mismatched.body["arguments"]["selectionToken"] =
            json!("market_00000000000000000000000000000001");
        mismatched.body["requestSha256"] = json!(hex_digest(Sha256::digest(serde_json::to_vec(
            &mismatched.body["arguments"]
        )?)));
        mismatched.sha256 = hex_digest(Sha256::digest(serde_json::to_vec(&mismatched.body)?));
        assert!(apply_receipt(&mut waiting.clone(), mismatched, &now).is_err());
        let active = waiting
            .driver
            .as_ref()
            .and_then(|driver| driver.active_job.as_ref())
            .ok_or("missing preparation job")?
            .clone();
        controller.mutate(|document, recorded_at| {
            retain_completed_job(
                workflow_control::find_workflow_mut(document, &token)?,
                &active,
                stale.clone(),
                3,
                &recorded_at,
            )
        })?;
        drop(controller);
        let reopened = AnalyticalWorkflowController::try_open(&paths, workspace)?;
        assert!(reopened.next_run()?.is_none());
        {
            let document = reopened.lock_document()?;
            let paused = workflow_control::find_workflow(&document, &token)?;
            assert_eq!(paused.state, WorkflowRunState::Paused);
            assert_eq!(
                paused.last_error.as_deref(),
                Some("analysis_preparation_required")
            );
            assert_eq!(paused.child_jobs[0].terminal_sequence.as_deref(), Some("3"));
            let driver = paused.driver.as_ref().ok_or("missing paused driver")?;
            assert!(driver.active_job.is_none());
            assert!(driver.source_cutoff.is_none());
            assert_eq!(driver.receipt(Step::PrepareSelection)?, &stale);
        }
        reopened.resume_workflow(&token)?;
        assert!(reopened.resume_workflow(&token).is_err());
        let resumed = reopened.next_run()?.ok_or("missing resumed preparation")?;
        assert_eq!(resumed.run_id, run.run_id);
        let driver = resumed.driver.as_ref().ok_or("missing resumed driver")?;
        assert!(driver.revalidating);
        assert!(driver.receipt(Step::PrepareSelection).is_err());
        assert_eq!(driver.completed_unavailable_receipts, vec![stale]);
        let (operation, arguments, _) = next_invocation(&resumed)?;
        assert_eq!(arguments, first.arguments);
        let second = reopened.retain_pending(&token, operation, arguments)?;
        assert_ne!(second.request_id, first.request_id);
        assert!(
            reopened
                .retain_pending(&token, operation, second.arguments.clone())
                .is_err()
        );
        reopened.retain_response(
            &token,
            &second,
            json!({"jobId": Uuid::new_v4(), "generation": 1, "sequence": 1, "state": "queued"}),
        )?;
        assert_eq!(
            reopened
                .next_run()?
                .ok_or("missing admitted retry")?
                .child_jobs
                .len(),
            2
        );
        drop(reopened);

        // A genuine partial result retains its real cutoff and still advances normally.
        let mut partial_run = run.clone();
        let mut partial = unavailable.clone();
        partial["reason"] = json!("source_evidence_unavailable");
        partial["preparedAtUnixNanos"] = json!(cutoff.to_string());
        apply_receipt(&mut partial_run, preparation_receipt(&run, partial)?, &now)?;
        assert_eq!(
            partial_run
                .driver
                .as_ref()
                .ok_or("missing partial driver")?
                .step,
            Step::SelectMarket
        );
        assert_eq!(
            partial_run
                .driver
                .as_ref()
                .ok_or("missing partial driver")?
                .source_cutoff
                .as_deref(),
            Some(cutoff.to_string().as_str())
        );
        let prepared = preparation_receipt(
            &run,
            json!({
                "status": "prepared", "instrumentId": instrument,
                "preparedAtUnixNanos": cutoff.to_string(),
                "financialConfigurationDigest": profile["configurationDigest"],
                "sourceActionReference": original
            }),
        )?;
        apply_receipt(&mut run, prepared, &now)?;
        run.driver.as_mut().ok_or("missing driver")?.step = Step::PreparePriceForecast;
        let (operation, arguments, _) = next_invocation(&run)?;
        let absence = receipt(
            operation,
            Value::Object(arguments),
            json!({
                "instrumentId": instrument,
                "availability": {"state": "unavailable", "reason": "compatible_forecast_selection_unavailable"},
                "forecast": null, "requestSha256": null,
                "financialProfileDigest": profile["configurationDigest"],
                "sourceCutoffUnixNanos": cutoff.to_string(), "forecastCohort": null,
                "expectedObservedThroughUnixNanos": null
            }),
        );
        assert!(absence.valid());
        apply_receipt(&mut run, absence.clone(), &now)?;
        assert_eq!(
            run.driver.as_ref().ok_or("missing driver")?.step,
            Step::ProbabilityPlan
        );
        assert_eq!(next_invocation(&run)?.1["sourceActionReference"], original);
        run.driver.as_mut().ok_or("missing driver")?.step = Step::HistoricalStudy;
        assert_eq!(next_invocation(&run)?.1["sourceActionReference"], original);

        let forecast = receipt(
            "Model.GetForecastJobResult",
            json!({}),
            json!({
                "job": {"jobId": Uuid::new_v4(), "generation": 1, "sequence": 1, "state": "completed"},
                "forecast": {"forecastToken": Uuid::new_v4()}, "requestSha256": digest,
                "financialProfileDigest": profile["configurationDigest"]
            }),
        );
        let forecast_reference = exact_forecast_reference(&forecast)?;
        let study = json!({"requestDigest": digest, "evidenceDigest": "2".repeat(64)});
        let work = run.driver.as_mut().ok_or("missing driver")?;
        work.receipts
            .insert("ProbabilityForecast-0".to_owned(), forecast.clone());
        work.receipts
            .insert("FiscalForecast-0".to_owned(), forecast.clone());
        work.receipts.insert(
            "StudyBacktest".to_owned(),
            receipt(
                "Analysis.GetRecommendationBacktestJobResult",
                json!({}),
                study.clone(),
            ),
        );
        work.step = Step::FinalPrepare;
        let mut final_unavailable = unavailable;
        final_unavailable["scope"] = json!("current_market");
        let final_stale = preparation_receipt(&run, final_unavailable)?;
        let mut final_retry = run.clone();
        let mut final_child = job_from_receipt(&final_stale)?;
        final_child.terminal_sequence = None;
        let final_active = ActiveJob {
            step: Step::FinalPrepare,
            result_operation: PREPARATION_RESULT.to_owned(),
            reference: final_child.clone(),
            observed_sequence: 1,
            result_arguments: final_stale.arguments.clone(),
            preparation_arguments: Some(preparation_arguments(&final_stale.body)?.clone()),
        };
        final_retry.child_jobs.push(final_child);
        final_retry
            .driver
            .as_mut()
            .ok_or("missing final driver")?
            .active_job = Some(final_active.clone());
        final_retry.state = WorkflowRunState::WaitingForServiceJob;
        let mut wrong_instrument = final_stale.clone();
        wrong_instrument.body["preparation"]["instrumentId"] = json!(Uuid::new_v4());
        wrong_instrument.sha256 =
            hex_digest(Sha256::digest(serde_json::to_vec(&wrong_instrument.body)?));
        assert!(apply_receipt(&mut final_retry.clone(), wrong_instrument, &now).is_err());
        retain_completed_job(
            &mut final_retry,
            &final_active,
            final_stale.clone(),
            3,
            &unix_nanos_now()?,
        )?;
        let controller = AnalyticalWorkflowController::try_open(&paths, workspace)?;
        controller.mutate(|document, _| {
            document.workflow_runs[0] = final_retry;
            Ok(())
        })?;
        drop(controller);
        let controller = AnalyticalWorkflowController::try_open(&paths, workspace)?;
        assert!(controller.next_run()?.is_none());
        controller.resume_workflow(&token)?;
        let final_resumed = controller.next_run()?.ok_or("missing final retry")?;
        let final_driver = final_resumed
            .driver
            .as_ref()
            .ok_or("missing resumed final driver")?;
        assert_eq!(final_driver.step, Step::FinalPrepare);
        assert_eq!(
            final_driver.receipts,
            run.driver
                .as_ref()
                .ok_or("missing original driver")?
                .receipts
        );
        assert_eq!(
            final_driver.source_cutoff,
            run.driver
                .as_ref()
                .ok_or("missing original driver")?
                .source_cutoff
        );
        assert_eq!(
            final_driver.completed_unavailable_receipts,
            vec![final_stale]
        );
        assert_eq!(next_invocation(&final_resumed)?, next_invocation(&run)?);
        drop(controller);
        let prepared = preparation_receipt(
            &run,
            json!({
                "instrumentId": instrument, "preparedAtUnixNanos": now,
                "sourceActionReference": current_actions, "fundamentalShareSources": null
            }),
        )?;
        apply_receipt(&mut run, prepared, &now)?;
        apply_receipt(
            &mut run,
            receipt(
                "Market.SelectInvestmentEvidence",
                json!({}),
                json!({
                    "instrumentId": instrument, "sourceCutoffUnixNanos": now,
                    "status": "available", "reference": market
                }),
            ),
            &now,
        )?;
        apply_receipt(
            &mut run,
            receipt(
                "Portfolio.SelectAnalysisPrerequisites",
                json!({}),
                json!({
                    "instrumentId": instrument, "reference": portfolio
                }),
            ),
            &now,
        )?;

        for (has_forecast, has_market) in [(false, true), (true, true), (true, false)] {
            let mut candidate = run.clone();
            let work = candidate.driver.as_mut().ok_or("missing driver")?;
            if has_forecast {
                work.receipts
                    .insert("PriceForecast".to_owned(), forecast.clone());
            }
            if !has_market {
                work.receipts.insert("FinalEvidence".to_owned(), receipt(
                    "Market.SelectInvestmentEvidence", json!({}), json!({
                        "instrumentId": instrument, "sourceCutoffUnixNanos": now,
                        "status": "unavailable", "reason": "market_evidence_unavailable", "reference": null
                    }),
                ));
            }
            let retained = work.clone();
            let (operation, mut arguments, mutation) = next_invocation(&candidate)?;
            assert_eq!(operation, "Decision.GenerateInvestmentAnalysis");
            assert!(mutation);
            assert_eq!(arguments.remove("confirm"), Some(json!(true)));
            assert_eq!(
                arguments["priceForecast"],
                if has_forecast {
                    forecast_reference.clone()
                } else {
                    Value::Null
                }
            );
            assert_eq!(
                arguments["sourceActionReference"],
                if has_forecast {
                    original.clone()
                } else {
                    Value::Null
                }
            );
            assert_eq!(
                arguments["currentShareActionReference"],
                if has_forecast && has_market {
                    current_actions.clone()
                } else {
                    Value::Null
                }
            );
            assert_eq!(
                arguments["probabilityForecasts"]["priceHigher"],
                forecast_reference
            );
            assert_eq!(arguments["financialForecasts"], json!([forecast_reference]));
            assert_eq!(arguments["historicalStudy"], study);

            let mut binding = arguments.clone();
            for key in ["analyticalProfile", "workflow", "market", "portfolio"] {
                binding.remove(key);
            }
            binding.insert("instrumentId".to_owned(), json!(instrument));
            assert_eq!(
                arguments["workflow"]["contentSha256"],
                json!(hex_digest(Sha256::digest(serde_json::to_vec(&binding)?)))
            );
            // Match the service's typed serialization before its actual canonical admission.
            let input: GenerateRequest = serde_json::from_value(Value::Object(arguments))?;
            validate_canonical_request(&serde_json::to_vec(&input)?)?;
            let after = candidate.driver.as_ref().ok_or("missing driver")?;
            assert_eq!(after, &retained);
            assert_eq!(after.initial_source_action_reference()?, &original);
            assert_eq!(after.receipt(Step::PreparePriceForecast)?, &absence);
        }
        Ok(())
    }
}
