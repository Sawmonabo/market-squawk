//! Cancellation stays attached to exact retained child-job generations across reconnects.

use std::{sync::Arc, time::Duration};

use market_squawk_services::RequestId;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::application::analytical_workflow::host::{
    InvocationAuthority, WorkflowGeneration, WorkflowState, invoke_analytical_operation,
};

use super::{
    AnalyticalControllerResponse, AnalyticalWorkflowController, ControllerDocument,
    PendingCapabilityInvocation, ServiceJobReference, WorkflowCheckpoint, WorkflowCheckpointStage,
    WorkflowError, WorkflowRun, WorkflowRunState, opaque_workflow_token, workflow_presentation,
};

impl AnalyticalWorkflowController {
    fn workflow_status(&self, token: &str) -> Result<AnalyticalControllerResponse, WorkflowError> {
        let document = self.lock_document()?;
        Ok(AnalyticalControllerResponse::Workflow {
            workflow: workflow_presentation(find_workflow(&document, token)?)?,
        })
    }

    pub(super) fn request_workflow_cancellation(
        &self,
        token: &str,
    ) -> Result<AnalyticalControllerResponse, WorkflowError> {
        self.mutate(|document, now| {
            let run = find_workflow_mut(document, token)?;
            if matches!(
                run.state,
                WorkflowRunState::Queued
                    | WorkflowRunState::Running
                    | WorkflowRunState::Paused
                    | WorkflowRunState::WaitingForServiceJob
            ) {
                run.state = WorkflowRunState::Cancelling;
                run.updated_at = now;
            }
            Ok(AnalyticalControllerResponse::Workflow {
                workflow: workflow_presentation(run)?,
            })
        })
    }

    fn pending_cancellations(
        &self,
        token: &str,
    ) -> Result<Vec<ServiceJobReference>, WorkflowError> {
        let document = self.lock_document()?;
        let run = find_workflow(&document, token)?;
        Ok(if run.state == WorkflowRunState::Cancelling {
            run.child_jobs
                .iter()
                .filter(|job| job.terminal_sequence.is_none())
                .cloned()
                .collect()
        } else {
            Vec::new()
        })
    }

    fn pending_start_cancellation(
        &self,
        token: &str,
    ) -> Result<Option<PendingCapabilityInvocation>, WorkflowError> {
        let document = self.lock_document()?;
        let run = find_workflow(&document, token)?;
        Ok((run.state == WorkflowRunState::Cancelling)
            .then(|| run.pending_invocation.clone())
            .flatten())
    }

    fn retain_reconciled_start(
        &self,
        token: &str,
        pending: &PendingCapabilityInvocation,
        child: Option<ServiceJobReference>,
    ) -> Result<AnalyticalControllerResponse, WorkflowError> {
        self.mutate(|document, now| {
            let run = find_workflow_mut(document, token)?;
            if run.state != WorkflowRunState::Cancelling
                || run.pending_invocation.as_ref() != Some(pending)
            {
                return Err(WorkflowError::invalid_request(
                    "This analysis changed while its background work was checked.",
                ));
            }
            if let Some(child) = &child {
                if let Some(existing) = run.child_jobs.iter().find(|existing| {
                    existing.job_id == child.job_id && existing.generation == child.generation
                }) {
                    if existing != child {
                        return Err(WorkflowError::internal());
                    }
                } else {
                    if run.child_jobs.len() >= super::MAXIMUM_CHILD_REFERENCES_PER_RUN {
                        return Err(WorkflowError::internal());
                    }
                    run.child_jobs.push(child.clone());
                }
            }
            if run.checkpoint_journal.len() >= super::MAXIMUM_CHECKPOINTS_PER_RUN {
                return Err(WorkflowError::internal());
            }
            let sequence = run
                .checkpoint_journal
                .last()
                .and_then(|checkpoint| checkpoint.sequence.checked_add(1))
                .ok_or_else(WorkflowError::internal)?;
            run.checkpoint_journal.push(WorkflowCheckpoint {
                sequence,
                stage: if child.is_some() {
                    WorkflowCheckpointStage::WaitingForServiceJob
                } else {
                    WorkflowCheckpointStage::CapabilityCompleted
                },
                recorded_at: now.clone(),
                child_job: child,
                result: None,
            });
            run.pending_invocation = None;
            run.updated_at = now;
            Ok(AnalyticalControllerResponse::Workflow {
                workflow: workflow_presentation(run)?,
            })
        })
    }

    fn retain_cancelled_child(
        &self,
        token: &str,
        child: &ServiceJobReference,
        terminal_sequence: u64,
    ) -> Result<AnalyticalControllerResponse, WorkflowError> {
        self.mutate(|document, now| {
            let run = find_workflow_mut(document, token)?;
            if run.state != WorkflowRunState::Cancelling {
                return Err(WorkflowError::invalid_request(
                    "This analysis is no longer stopping.",
                ));
            }
            let retained = run
                .child_jobs
                .iter_mut()
                .find(|job| job.job_id == child.job_id && job.generation == child.generation)
                .ok_or_else(WorkflowError::internal)?;
            retained.terminal_sequence = Some(terminal_sequence.to_string());
            run.updated_at = now;
            Ok(AnalyticalControllerResponse::Workflow {
                workflow: workflow_presentation(run)?,
            })
        })
    }

    fn finish_cancellation(
        &self,
        token: &str,
    ) -> Result<AnalyticalControllerResponse, WorkflowError> {
        self.mutate(|document, now| {
            let run = find_workflow_mut(document, token)?;
            if run.state == WorkflowRunState::Cancelling
                && run.pending_invocation.is_none()
                && run
                    .child_jobs
                    .iter()
                    .all(|job| job.terminal_sequence.is_some())
            {
                if run.checkpoint_journal.len() >= super::MAXIMUM_CHECKPOINTS_PER_RUN {
                    return Err(WorkflowError::internal());
                }
                let sequence = run
                    .checkpoint_journal
                    .last()
                    .map_or(Some(1), |checkpoint| checkpoint.sequence.checked_add(1))
                    .ok_or_else(WorkflowError::internal)?;
                run.checkpoint_journal.push(WorkflowCheckpoint {
                    sequence,
                    stage: WorkflowCheckpointStage::Terminal,
                    recorded_at: now.clone(),
                    child_job: None,
                    result: None,
                });
                run.state = WorkflowRunState::Cancelled;
                run.updated_at = now;
            }
            Ok(AnalyticalControllerResponse::Workflow {
                workflow: workflow_presentation(run)?,
            })
        })
    }
}

/// A bounded cancellation attempt. Unsettled work stays `Cancelling` and retains its exact handles.
pub(super) async fn settle_cancellation(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    token: &str,
) -> Result<AnalyticalControllerResponse, WorkflowError> {
    let retained_generation = generation.for_workflow(token)?;
    let generation = &retained_generation;
    let controller = generation.analytical_controller();
    let Ok(_cleanup_guard) = controller.cancellation_gate.try_lock() else {
        return controller.workflow_status(token);
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    if let Some(pending) = controller.pending_start_cancellation(token)? {
        if matches!(
            pending.operation.as_str(),
            "Decision.GenerateInvestmentAnalysis"
                | "Decision.PublishFindResults"
                | "Analysis.CompleteHistoricalStudyFiscalPage"
                | "Analysis.CompleteCurrentScreenPartition"
        ) {
            // Settle the same atomically bound publication. This can recover an already-saved
            // analysis, but cannot create a second analysis or submit an execution intent.
            let _ = tokio::time::timeout_at(
                deadline,
                super::workflow_driver::recover_pending(state, generation, token, &pending),
            )
            .await;
            return controller.finish_cancellation(token);
        }
        // A missing acknowledgement is not evidence that submission failed. Recover the exact
        // original admission before cancelling; never replay a one-use preparation token.
        let reconciliation =
            tokio::time::timeout_at(deadline, cancel_pending_start(generation, &pending)).await;
        if let Ok(Ok(StartCancellation::Settled(child))) = reconciliation {
            let _fence = generation.analytical_retirement_fence().await;
            state.admit_current(generation)?;
            controller.retain_reconciled_start(token, &pending, child)?;
        }
    }
    let pending = controller.pending_cancellations(token)?;
    if !pending.is_empty()
        && (!generation.has_operation("Job.Get") || !generation.has_operation("Job.Cancel"))
    {
        return Err(WorkflowError::new(
            "analysis_cleanup_unavailable",
            "Analysis could not finish stopping. Refresh the activity and try again.",
        ));
    }
    for child in pending {
        let generation_number = child
            .generation
            .parse::<u64>()
            .map_err(|_error| WorkflowError::internal())?;
        let arguments = json!({ "jobId": child.job_id, "generation": generation_number });
        let response = tokio::time::timeout_at(
            deadline,
            invoke_job_operation(
                "Job.Get",
                object(arguments)?,
                generation,
                InvocationAuthority::ReadOnly,
            ),
        )
        .await;
        let Ok(Ok(response)) = response else { break };
        let status = response.get("data").ok_or_else(WorkflowError::internal)?;
        let sequence = validate_child(status, &child)?;
        if terminal_state(status) {
            let _fence = generation.analytical_retirement_fence().await;
            state.admit_current(generation)?;
            generation
                .analytical_controller()
                .retain_cancelled_child(token, &child, sequence)?;
            continue;
        }
        let arguments = json!({
            "jobId": child.job_id, "generation": generation_number, "expectedSequence": sequence,
        });
        let response = tokio::time::timeout_at(
            deadline,
            invoke_job_operation(
                "Job.Cancel",
                object(arguments)?,
                generation,
                InvocationAuthority::ExactConfirmed("Job.Cancel"),
            ),
        )
        .await;
        let Ok(Ok(response)) = response else { break };
        let status = response.get("data").ok_or_else(WorkflowError::internal)?;
        let sequence = validate_child(status, &child)?;
        if terminal_state(status) {
            let _fence = generation.analytical_retirement_fence().await;
            state.admit_current(generation)?;
            generation
                .analytical_controller()
                .retain_cancelled_child(token, &child, sequence)?;
        } else {
            loop {
                let poll = async {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    invoke_job_operation(
                        "Job.Get",
                        object(json!({ "jobId": child.job_id, "generation": generation_number }))?,
                        generation,
                        InvocationAuthority::ReadOnly,
                    )
                    .await
                };
                let Ok(Ok(response)) = tokio::time::timeout_at(deadline, poll).await else {
                    break;
                };
                let status = response.get("data").ok_or_else(WorkflowError::internal)?;
                let sequence = validate_child(status, &child)?;
                if terminal_state(status) {
                    let _fence = generation.analytical_retirement_fence().await;
                    state.admit_current(generation)?;
                    generation
                        .analytical_controller()
                        .retain_cancelled_child(token, &child, sequence)?;
                    break;
                }
            }
        }
    }
    let _fence = generation.analytical_retirement_fence().await;
    state.admit_current(generation)?;
    generation
        .analytical_controller()
        .finish_cancellation(token)
}

enum StartCancellation {
    Settled(Option<ServiceJobReference>),
    Pending,
}

async fn cancel_pending_start(
    generation: &Arc<WorkflowGeneration>,
    pending: &PendingCapabilityInvocation,
) -> Result<StartCancellation, WorkflowError> {
    let canonical = crate::application::analytical_workflow::host::prepare_analytical_arguments(
        generation,
        &pending.operation,
        pending.arguments.clone(),
        InvocationAuthority::ReadOnly,
    )?;
    if canonical != pending.arguments || !super::valid_pending_invocation(pending) {
        return Err(WorkflowError::internal());
    }
    match pending.operation.as_str() {
        // These exact capabilities read retained authorities or prepare an expiring local
        // preview. None can publish a financial result or admit a background job.
        "AnalyticalProfile.Resolve"
        | "Market.SelectInvestmentEvidence"
        | "Market.ReadInvestmentEvidence"
        | "Portfolio.SelectAnalysisPrerequisites"
        | "Portfolio.ReadAnalysisPrerequisites"
        | "Model.PrepareInvestmentForecast"
        | "Analysis.GetFiscalPreparationPlan"
        | "Analysis.GetHistoricalStudyPlan"
        | "Decision.PrepareCurrentScreen"
        | "Analysis.PrepareCurrentScreenPartition"
        | "Decision.ReadCurrentScreenPreparation"
        | "Decision.GetFindResults"
        | "Decision.GetInvestmentAnalysis" => return Ok(StartCancellation::Settled(None)),
        "Market.PrepareInvestmentEvidence"
        | "Model.StartPreparedForecast"
        | "Analysis.StartPreparedBacktest"
        | "Analysis.StartInvestmentDataset"
        | "Analysis.StartFiscalDatasetBuild"
        | "Model.StartPreparedTraining"
        | "Model.StartHistoricalStudyTraining"
        | "Analysis.StartHistoricalStudyDataset"
        | "Analysis.StartRecommendationBacktest"
        | "Analysis.StartCurrentScreenDataset"
        | "Decision.StartCurrentScreen"
        | "Model.StartFiscalForecast" => {}
        _ => {
            return Err(WorkflowError::new(
                "analysis_cleanup_unavailable",
                "The interrupted background work could not yet be checked.",
            ));
        }
    }
    let response = invoke_job_operation(
        "Job.CancelStart",
        object(json!({
            "requestId": pending.request_id,
            "operation": pending.operation,
            "argumentsSha256": pending.arguments_sha256,
        }))?,
        generation,
        InvocationAuthority::ExactConfirmed("Job.CancelStart"),
    )
    .await?;
    let data = response.get("data").ok_or_else(WorkflowError::internal)?;
    match data.get("state").and_then(Value::as_str) {
        Some("admitted") => {
            let job = data.get("job").ok_or_else(WorkflowError::internal)?;
            Ok(StartCancellation::Settled(Some(job_reference(job)?)))
        }
        // Only the service's durable tombstone closes an unacknowledged submission. It prevents
        // a delayed original handler from admitting work after cancellation is reported.
        Some("not_admitted") if data.get("job") == Some(&Value::Null) => {
            Ok(StartCancellation::Settled(None))
        }
        Some("pending" | "unknown") if data.get("job") == Some(&Value::Null) => {
            Ok(StartCancellation::Pending)
        }
        _ => Err(WorkflowError::internal()),
    }
}

pub(super) async fn invoke_job_operation(
    operation: &'static str,
    arguments: serde_json::Map<String, Value>,
    generation: &Arc<WorkflowGeneration>,
    authority: InvocationAuthority,
) -> Result<Value, WorkflowError> {
    // Control calls bind their target in the arguments. Give each attempt a fresh transport
    // identity so a cached transient error cannot prevent later reconciliation or cancellation.
    let request_id =
        RequestId::try_string(format!("desktop-job-{}", uuid::Uuid::new_v4().simple(),))
            .map_err(|_error| WorkflowError::internal())?;
    invoke_analytical_operation(
        generation,
        operation,
        arguments,
        authority,
        request_id,
        CancellationToken::new(),
    )
    .await
}

pub(super) fn job_reference(job: &Value) -> Result<ServiceJobReference, WorkflowError> {
    let job_id = job
        .get("jobId")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<uuid::Uuid>().ok())
        .filter(|value| !value.is_nil())
        .ok_or_else(WorkflowError::internal)?;
    let generation = job
        .get("generation")
        .and_then(unsigned_value)
        .filter(|value| *value > 0)
        .ok_or_else(WorkflowError::internal)?;
    let mut child = ServiceJobReference {
        job_id,
        generation: generation.to_string(),
        terminal_sequence: None,
        result: None,
    };
    let sequence = validate_child(job, &child)?;
    if terminal_state(job) {
        child.terminal_sequence = Some(sequence.to_string());
    }
    Ok(child)
}

pub(super) fn validate_child(
    value: &Value,
    child: &ServiceJobReference,
) -> Result<u64, WorkflowError> {
    let id = value
        .get("jobId")
        .and_then(Value::as_str)
        .and_then(|id| id.parse::<uuid::Uuid>().ok());
    let generation = value.get("generation").and_then(unsigned_value);
    let sequence = value
        .get("sequence")
        .and_then(unsigned_value)
        .ok_or_else(WorkflowError::internal)?;
    if id != Some(child.job_id)
        || generation != child.generation.parse::<u64>().ok()
        || !matches!(
            value.get("state").and_then(Value::as_str),
            Some(
                "queued"
                    | "preparing"
                    | "running"
                    | "awaiting_confirmation"
                    | "cancelling"
                    | "completed"
                    | "failed"
                    | "cancelled"
                    | "interrupted"
                    | "recovering"
            )
        )
    {
        return Err(WorkflowError::internal());
    }
    Ok(sequence)
}

fn terminal_state(value: &Value) -> bool {
    matches!(
        value.get("state").and_then(Value::as_str),
        Some("completed" | "failed" | "cancelled" | "interrupted")
    )
}

fn unsigned_value(value: &Value) -> Option<u64> {
    value.as_u64()
}

fn object(value: Value) -> Result<serde_json::Map<String, Value>, WorkflowError> {
    match value {
        Value::Object(value) => Ok(value),
        _ => Err(WorkflowError::internal()),
    }
}

pub(super) fn find_workflow<'a>(
    document: &'a ControllerDocument,
    token: &str,
) -> Result<&'a WorkflowRun, WorkflowError> {
    document
        .workflow_runs
        .iter()
        .find(|run| opaque_workflow_token(run).is_ok_and(|candidate| candidate == token))
        .ok_or_else(|| WorkflowError::invalid_request("This saved analysis could not be found."))
}

pub(super) fn find_workflow_mut<'a>(
    document: &'a mut ControllerDocument,
    token: &str,
) -> Result<&'a mut WorkflowRun, WorkflowError> {
    document
        .workflow_runs
        .iter_mut()
        .find(|run| opaque_workflow_token(run).is_ok_and(|candidate| candidate == token))
        .ok_or_else(|| WorkflowError::invalid_request("This saved analysis could not be found."))
}
