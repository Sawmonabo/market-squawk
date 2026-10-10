//! Bounded delivery records for manual prepared jobs. The service remains the sole job owner.

use std::{collections::HashSet, sync::Arc};

use market_squawk_services::RequestId;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::application::analytical_workflow::host::{
    InvocationAuthority, WorkflowGeneration, WorkflowState, desktop_result_limits,
    invoke_analytical_operation, prepare_analytical_arguments,
};

use super::{
    AnalyticalWorkflowController, PendingCapabilityInvocation, ServiceJobReference, WorkflowError,
    hex_digest, valid_pending_invocation, valid_service_job_reference, valid_timestamp,
    workflow_control,
};

const MAXIMUM_DELIVERIES: usize = 128;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct PreparedStartDelivery {
    origin: super::host::WorkflowOrigin,
    invocation: PendingCapabilityInvocation,
    child_job: Option<ServiceJobReference>,
    not_admitted: bool,
    #[serde(default)]
    observed_sequence: Option<String>,
    #[serde(default)]
    forecast: Option<CompletedForecastReference>,
    #[serde(default)]
    forecast_finished_without_result: bool,
    created_at: String,
    updated_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CompletedForecastReference {
    forecast_token: Uuid,
    request_sha256: String,
}

pub(super) fn validate_deliveries(
    deliveries: &[PreparedStartDelivery],
) -> Result<(), WorkflowError> {
    let mut identities = HashSet::new();
    if deliveries.len() > MAXIMUM_DELIVERIES
        || deliveries.iter().any(|delivery| {
            !valid_pending_invocation(&delivery.invocation)
                || !is_prepared_start(&delivery.invocation.operation)
                || delivery.invocation.arguments.len() != 3
                || delivery.invocation.arguments.get("resultLimits")
                    != Some(&desktop_result_limits())
                || delivery.invocation.arguments.get("confirm") != Some(&Value::Bool(true))
                || delivery
                    .invocation
                    .arguments
                    .get("confirmationToken")
                    .and_then(Value::as_str)
                    .is_none_or(|token| token.is_empty() || token.len() > 256)
                || !identities.insert(&delivery.invocation.request_id)
                || delivery
                    .child_job
                    .as_ref()
                    .is_some_and(|child| !valid_service_job_reference(child))
                || (delivery.not_admitted && delivery.child_job.is_some())
                || delivery
                    .observed_sequence
                    .as_deref()
                    .is_some_and(|sequence| !super::valid_unsigned_decimal(sequence))
                || (delivery.observed_sequence.is_some() && delivery.child_job.is_none())
                || delivery.forecast.as_ref().is_some_and(|forecast| {
                    delivery.invocation.operation != "Model.StartPreparedForecast"
                        || forecast.forecast_token.is_nil()
                        || !super::valid_digest(&forecast.request_sha256)
                        || delivery.forecast_finished_without_result
                        || delivery.child_job.as_ref().is_none_or(|child| {
                            child.terminal_sequence.is_none()
                                || child.terminal_sequence != delivery.observed_sequence
                        })
                })
                || delivery.forecast_finished_without_result
                    && (delivery.child_job.is_none()
                        || delivery.invocation.operation != "Model.StartPreparedForecast")
                || !valid_timestamp(&delivery.created_at)
                || !valid_timestamp(&delivery.updated_at)
        })
    {
        return Err(WorkflowError::internal());
    }
    Ok(())
}

fn is_prepared_start(operation: &str) -> bool {
    matches!(
        operation,
        "Model.StartPreparedForecast" | "Analysis.StartPreparedBacktest"
    )
}

impl AnalyticalWorkflowController {
    pub(super) fn unresolved_prepared_delivery(
        &self,
    ) -> Result<Option<PreparedStartDelivery>, WorkflowError> {
        let document = self.lock_document()?;
        Ok(document
            .prepared_starts
            .iter()
            .find(|delivery| delivery.child_job.is_none() && !delivery.not_admitted)
            .or_else(|| {
                document
                    .prepared_starts
                    .iter()
                    .filter(|delivery| {
                        delivery.invocation.operation == "Model.StartPreparedForecast"
                            && delivery.child_job.is_some()
                            && delivery.forecast.is_none()
                            && !delivery.forecast_finished_without_result
                    })
                    .min_by_key(|delivery| delivery.updated_at.parse::<u64>().ok())
            })
            .cloned())
    }

    fn retain_prepared_delivery(
        &self,
        generation: &WorkflowGeneration,
        operation: &'static str,
        arguments: Map<String, Value>,
    ) -> Result<(PreparedStartDelivery, bool), WorkflowError> {
        if !is_prepared_start(operation) || arguments.len() != 1 {
            return Err(WorkflowError::internal());
        }
        let arguments = prepare_analytical_arguments(
            generation,
            operation,
            arguments,
            InvocationAuthority::ExactConfirmed(operation),
        )?;
        let encoded = serde_json::to_vec(&arguments).map_err(|_| WorkflowError::internal())?;
        let arguments_sha256 = hex_digest(Sha256::digest(encoded));
        let identity = serde_json::to_vec(&(generation.origin(), operation, &arguments_sha256))
            .map_err(|_| WorkflowError::internal())?;
        let request_id = format!(
            "desktop-prepared-{}",
            Uuid::new_v5(&super::PRESENTATION_TOKEN_NAMESPACE, &identity).simple()
        );
        let invocation = PendingCapabilityInvocation {
            request_id,
            operation: operation.to_owned(),
            arguments,
            arguments_sha256,
        };
        self.mutate(|document, now| {
            if let Some(existing) = document.prepared_starts.iter().find(|delivery| {
                delivery.invocation.request_id == invocation.request_id
            }) {
                if existing.invocation != invocation {
                    return Err(WorkflowError::internal());
                }
                return Ok((existing.clone(), false));
            }
            if document.prepared_starts.len() == MAXIMUM_DELIVERIES {
                // A delivered record can be recovered from its deterministic original request
                // and the sole service job authority. Unresolved submissions are never evicted.
                let settled = document.prepared_starts.iter().position(|delivery| {
                    delivery.child_job.is_some() || delivery.not_admitted
                }).ok_or_else(|| WorkflowError::new(
                    "analysis_delivery_capacity", "Earlier analyses are still being reconciled. Refresh their progress before starting more.",
                ))?;
                document.prepared_starts.remove(settled);
            }
            let delivery = PreparedStartDelivery {
                origin: generation.origin(),
                invocation, child_job: None, not_admitted: false,
                observed_sequence: None, forecast: None, forecast_finished_without_result: false,
                created_at: now.clone(), updated_at: now,
            };
            document.prepared_starts.push(delivery.clone());
            Ok((delivery, true))
        })
    }

    fn settle_prepared_delivery(
        &self,
        expected: &PreparedStartDelivery,
        child: Option<ServiceJobReference>,
        observed_sequence: Option<u64>,
        forecast_finished_without_result: bool,
    ) -> Result<(), WorkflowError> {
        self.mutate(|document, now| {
            let delivery = document
                .prepared_starts
                .iter_mut()
                .find(|delivery| delivery.invocation == expected.invocation)
                .ok_or_else(WorkflowError::internal)?;
            if delivery.not_admitted {
                if child.is_some() {
                    return Err(WorkflowError::internal());
                }
                return Ok(());
            }
            if let Some(previous) = &delivery.child_job {
                let next = child.as_ref().ok_or_else(WorkflowError::internal)?;
                let previous_generation = previous
                    .generation
                    .parse::<u64>()
                    .map_err(|_| WorkflowError::internal())?;
                let next_generation = next
                    .generation
                    .parse::<u64>()
                    .map_err(|_| WorkflowError::internal())?;
                let sequence = observed_sequence.ok_or_else(WorkflowError::internal)?;
                if next.job_id != previous.job_id
                    || next_generation < previous_generation
                    || next_generation == previous_generation
                        && (delivery
                            .observed_sequence
                            .as_deref()
                            .is_some_and(|previous| {
                                previous
                                    .parse::<u64>()
                                    .is_ok_and(|previous| sequence < previous)
                            })
                            || previous
                                .terminal_sequence
                                .as_deref()
                                .is_some_and(|previous| {
                                    previous
                                        .parse::<u64>()
                                        .is_ok_and(|previous| sequence < previous)
                                })
                            || previous.terminal_sequence.is_some()
                                && next.terminal_sequence.is_none())
                    || delivery.forecast.is_some()
                        && (previous != next
                            || delivery.observed_sequence.as_deref()
                                != Some(sequence.to_string().as_str()))
                {
                    return Err(WorkflowError::internal());
                }
            }
            delivery.not_admitted = child.is_none();
            delivery.child_job = child;
            delivery.observed_sequence = observed_sequence.map(|sequence| sequence.to_string());
            delivery.forecast_finished_without_result = forecast_finished_without_result;
            delivery.updated_at = now;
            Ok(())
        })
    }

    fn retain_completed_forecast(
        &self,
        expected: &PreparedStartDelivery,
        child: &ServiceJobReference,
        forecast: CompletedForecastReference,
    ) -> Result<(), WorkflowError> {
        self.mutate(|document, now| {
            let delivery = document
                .prepared_starts
                .iter_mut()
                .find(|delivery| delivery.invocation == expected.invocation)
                .ok_or_else(WorkflowError::internal)?;
            if delivery.child_job.as_ref() != Some(child)
                || delivery
                    .forecast
                    .as_ref()
                    .is_some_and(|existing| existing != &forecast)
            {
                return Err(WorkflowError::internal());
            }
            delivery.forecast = Some(forecast);
            delivery.updated_at = now;
            Ok(())
        })
    }
}

/// Activity refresh also settles a submission whose preview was lost with its WebView. Work is
/// bounded to one delivery per refresh; the complete job history is still read from the service.
pub(crate) async fn recover_prepared_starts(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
) -> Result<(), WorkflowError> {
    let controller = generation.analytical_controller();
    let Ok(_guard) = controller.prepared_start_gate.try_lock() else {
        return Ok(());
    };
    let delivery = {
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        controller.unresolved_prepared_delivery()?
    };
    let Some(delivery) = delivery else {
        return Ok(());
    };
    let retained_generation = generation.for_origin(delivery.origin)?;
    let generation = &retained_generation;
    let recovery = async {
        let mut result = reconcile(generation, &delivery, false).await?;
        if delivery.child_job.is_none()
            && matches!(
                result.get("state").and_then(Value::as_str),
                Some("pending" | "unknown")
            )
            && result.get("job") == Some(&Value::Null)
        {
            result = reconcile(generation, &delivery, true).await?;
        }
        match result.get("state").and_then(Value::as_str) {
            Some("admitted") => {
                retain_job(state, generation, &delivery, &result).await?;
                Ok(())
            }
            Some("not_admitted") if result.get("job") == Some(&Value::Null) => {
                retain_not_admitted(state, generation, &delivery).await
            }
            Some("pending" | "unknown") if result.get("job") == Some(&Value::Null) => Ok(()),
            _ => Err(WorkflowError::internal()),
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(12), recovery)
        .await
        .map_err(|_| delivery_pending())?
}

/// Persist before sending. A repeated call with the same preview reconnects to that original
/// request; it never silently consumes the preview under a second request identity.
pub(crate) async fn start_prepared(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    operation: &'static str,
    arguments: Map<String, Value>,
    confirmed: bool,
) -> Result<Value, WorkflowError> {
    if !confirmed {
        return Err(WorkflowError::new(
            "confirmation_required",
            "Confirm this analysis before starting it.",
        ));
    }
    let controller = generation.analytical_controller();
    let _delivery_guard = controller.prepared_start_gate.try_lock().map_err(|_| {
        WorkflowError::new(
            "analysis_delivery_busy",
            "Another analysis is starting. Check its progress before trying again.",
        )
    })?;
    let (delivery, first_attempt) = {
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        controller.retain_prepared_delivery(generation, operation, arguments)?
    };
    // Wake custody immediately after persistence, before the caller can be cancelled during I/O.
    super::workflow_driver::launch(Arc::clone(generation));
    if delivery.not_admitted {
        return Err(prepare_again());
    }
    // Reconcile even a newly retained delivery: an older settled local delivery may have been
    // evicted while the service still owns the same deterministic original request.
    let reconciliation = reconcile(generation, &delivery, false).await?;
    match reconciliation.get("state").and_then(Value::as_str) {
        Some("admitted") => return retain_job(state, generation, &delivery, &reconciliation).await,
        Some("not_admitted") if reconciliation.get("job") == Some(&Value::Null) => {
            retain_not_admitted(state, generation, &delivery).await?;
            return Err(prepare_again());
        }
        Some("unknown") if first_attempt && reconciliation.get("job") == Some(&Value::Null) => {}
        Some("unknown" | "pending")
            if delivery.child_job.is_none() && reconciliation.get("job") == Some(&Value::Null) =>
        {
            let fenced = reconcile(generation, &delivery, true).await?;
            return match fenced.get("state").and_then(Value::as_str) {
                Some("admitted") => retain_job(state, generation, &delivery, &fenced).await,
                Some("not_admitted") if fenced.get("job") == Some(&Value::Null) => {
                    retain_not_admitted(state, generation, &delivery).await?;
                    Err(prepare_again())
                }
                _ => Err(delivery_pending()),
            };
        }
        _ => return Err(WorkflowError::internal()),
    }
    let request_id = RequestId::try_string(delivery.invocation.request_id.clone())
        .map_err(|_| WorkflowError::internal())?;
    let response = invoke_analytical_operation(
        generation,
        operation,
        delivery.invocation.arguments.clone(),
        InvocationAuthority::ExactConfirmed(operation),
        request_id,
        CancellationToken::new(),
    )
    .await?;
    let job = response.get("data").ok_or_else(WorkflowError::internal)?;
    retain_job_view(state, generation, &delivery, job).await
}

async fn reconcile(
    generation: &Arc<WorkflowGeneration>,
    delivery: &PreparedStartDelivery,
    cancel: bool,
) -> Result<Value, WorkflowError> {
    validate_deliveries(std::slice::from_ref(delivery))?;
    let operation = if cancel {
        "Job.CancelStart"
    } else {
        "Job.ReconcileStart"
    };
    let response = workflow_control::invoke_job_operation(
        operation,
        object(json!({"requestId": delivery.invocation.request_id, "operation": delivery.invocation.operation, "argumentsSha256": delivery.invocation.arguments_sha256}))?,
        generation,
        if cancel { InvocationAuthority::ExactConfirmed("Job.CancelStart") } else { InvocationAuthority::ReadOnly },
    ).await?;
    response
        .get("data")
        .cloned()
        .ok_or_else(WorkflowError::internal)
}

async fn retain_job(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    delivery: &PreparedStartDelivery,
    reconciliation: &Value,
) -> Result<Value, WorkflowError> {
    let job = reconciliation
        .get("job")
        .ok_or_else(WorkflowError::internal)?;
    retain_job_view(state, generation, delivery, job).await
}

async fn retain_job_view(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    delivery: &PreparedStartDelivery,
    job: &Value,
) -> Result<Value, WorkflowError> {
    let child = workflow_control::job_reference(job)?;
    let sequence = workflow_control::validate_child(job, &child)?;
    let forecast_job = delivery.invocation.operation == "Model.StartPreparedForecast";
    let finished_without_forecast = forecast_job
        && matches!(
            job.get("state").and_then(Value::as_str),
            Some("failed" | "cancelled" | "interrupted")
        );
    {
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        generation
            .analytical_controller()
            .settle_prepared_delivery(
                delivery,
                Some(child.clone()),
                Some(sequence),
                finished_without_forecast,
            )?;
    }
    if forecast_job
        && delivery.forecast.is_none()
        && job.get("state").and_then(Value::as_str) == Some("completed")
    {
        // Job admission is already acknowledged. An unavailable optional result read must not
        // turn that acknowledgement into a false start failure; activity can retry the exact read.
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            retain_forecast_result(state, generation, delivery, &child, sequence),
        )
        .await;
    }
    state.admit_current(generation)?;
    receipt_result(job)
}

async fn retain_forecast_result(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    delivery: &PreparedStartDelivery,
    child: &ServiceJobReference,
    sequence: u64,
) -> Result<(), WorkflowError> {
    let response = workflow_control::invoke_job_operation(
        "Model.GetForecastJobResult",
        object(json!({"jobId": child.job_id, "generation": child.generation.parse::<u64>().map_err(|_| WorkflowError::internal())?}))?,
        generation, InvocationAuthority::ReadOnly,
    ).await?;
    let data = response.get("data").ok_or_else(WorkflowError::internal)?;
    let job = data.get("job").ok_or_else(WorkflowError::internal)?;
    if workflow_control::validate_child(job, child)? != sequence
        || job.get("state").and_then(Value::as_str) != Some("completed")
        || data.get("financialProfileDigest") != Some(&Value::Null)
    {
        return Err(WorkflowError::internal());
    }
    let forecast_token = data
        .get("forecast")
        .and_then(|forecast| forecast.get("forecastToken"))
        .and_then(Value::as_str)
        .and_then(|token| token.parse::<Uuid>().ok())
        .filter(|token| !token.is_nil())
        .ok_or_else(WorkflowError::internal)?;
    let request_sha256 = data
        .get("requestSha256")
        .and_then(Value::as_str)
        .filter(|digest| super::valid_digest(digest))
        .ok_or_else(WorkflowError::internal)?
        .to_owned();
    let _fence = generation.analytical_retirement_fence().await;
    state.admit_current(generation)?;
    generation
        .analytical_controller()
        .retain_completed_forecast(
            delivery,
            child,
            CompletedForecastReference {
                forecast_token,
                request_sha256,
            },
        )
}

async fn retain_not_admitted(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    delivery: &PreparedStartDelivery,
) -> Result<(), WorkflowError> {
    let _fence = generation.analytical_retirement_fence().await;
    state.admit_current(generation)?;
    generation
        .analytical_controller()
        .settle_prepared_delivery(delivery, None, None, false)
}

fn receipt_result(job: &Value) -> Result<Value, WorkflowError> {
    let child = workflow_control::job_reference(job)?;
    let sequence = workflow_control::validate_child(job, &child)?;
    Ok(json!({"data": {
        "jobId": child.job_id, "generation": child.generation,
        "sequence": sequence.to_string(), "state": job.get("state").ok_or_else(WorkflowError::internal)?,
    }, "metadata": {"completeness": "complete", "returnedItems": 1, "availableItems": 1}}))
}

fn object(value: Value) -> Result<Map<String, Value>, WorkflowError> {
    value
        .as_object()
        .cloned()
        .ok_or_else(WorkflowError::internal)
}

fn prepare_again() -> WorkflowError {
    WorkflowError::new(
        "analysis_preparation_required",
        "This analysis was not started. Review a new preview before starting it.",
    )
}

fn delivery_pending() -> WorkflowError {
    WorkflowError::new(
        "analysis_delivery_pending",
        "This analysis is still being checked. Retry to reconnect to its original progress.",
    )
}

impl PreparedStartDelivery {
    pub(super) fn admit_origin(&self, workspace: Uuid) -> Result<(), WorkflowError> {
        self.origin.admitted(workspace).map(|_| ())
    }
}
