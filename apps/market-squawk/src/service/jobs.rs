//! Typed job control shared by every installed transport.

mod historical_study;
pub(super) use historical_study::GET_RECOMMENDATION_BACKTEST_JOB_RESULT;

use std::sync::Arc;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_jobs::{
    JobConfirmation, JobEventPageLimit, JobEventSequence, JobGeneration, JobId, JobListCursor,
    JobListPageLimit, JobOrigin, JobRepository, JobStartAdmission as RepositoryStartAdmission,
    JobStartBinding, JobStartPermit, JobStartState, SqliteJobRepository,
};
use market_squawk_services::{
    RequestContext, RequestId, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};

use crate::{
    application::job::{JobAdmission, JobApplication, JobApplicationError, JobReceipt, JobView},
    jobs::{InstalledJobAuthority, MarketHistoryJobRunner},
};

/// Closed typed job query and mutation authority.
pub(super) struct InstalledJobOperations {
    application: JobApplication<SqliteJobRepository>,
    repository: Arc<SqliteJobRepository>,
}

pub(super) enum JobStartAdmission {
    Execute(JobStartPermit),
    Existing(TypedToolResult),
}

impl InstalledJobOperations {
    pub(super) fn new(jobs: &InstalledJobAuthority) -> Self {
        Self {
            application: JobApplication::new(jobs.repository(), jobs.authority()),
            repository: jobs.repository(),
        }
    }

    /// Reopens the actual forecast published by an exact completed job generation.
    pub(super) async fn read_forecast_result(
        &self,
        runner: &crate::jobs::ForecastJobRunner,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        authenticated_origin(context)?;
        let input: GetRequest = decode(&super::business_arguments(request.arguments()))?;
        let (snapshot, output) = self
            .read_completed_forecast(runner, &input.job_id, input.generation, context)
            .await?;
        TypedToolResult::try_new(
            json!({
                "job": JobReceipt::from_snapshot(&snapshot),
                "forecast": output.result.structured_content(),
                "requestSha256": digest_text(output.request_sha256),
                "financialProfileDigest": output.financial_profile_digest.map(digest_text),
            }),
            1,
            output.result.metadata().clone(),
            context.limits(),
        )
        .map_err(Into::into)
    }

    /// Returns exact completed forecast evidence without accepting its public projection.
    pub(super) async fn read_forecast_reference(
        &self,
        runner: &crate::jobs::ForecastJobRunner,
        job_id: &str,
        generation: u64,
        context: &RequestContext,
    ) -> Result<crate::jobs::ForecastJobResult, ServiceError> {
        self.read_completed_forecast(runner, job_id, generation, context)
            .await
            .map(|(_, output)| output)
    }

    async fn read_completed_forecast(
        &self,
        runner: &crate::jobs::ForecastJobRunner,
        job_id: &str,
        generation: u64,
        context: &RequestContext,
    ) -> Result<
        (
            market_squawk_jobs::JobSnapshot,
            crate::jobs::ForecastJobResult,
        ),
        ServiceError,
    > {
        ensure_live(context)?;
        authenticated_origin(context)?;
        let id = parse_id(job_id)?;
        let generation = parse_generation(generation)?;
        let snapshot = tokio::select! {
            biased;
            _ = context.cancellation().cancelled() => return Err(ServiceError::Cancelled),
            _ = tokio::time::sleep_until(context.deadline().into()) => {
                return Err(ServiceError::DeadlineExceeded);
            }
            result = self.repository.get(id, generation) => {
                result.map_err(|error| match error {
                    market_squawk_jobs::JobRepositoryError::NotFound => ServiceError::NotFound,
                    _ => ServiceError::Unavailable,
                })?
            }
        };
        // The runner checks the original workspace/client, completed state, input and artifact.
        let output = runner.read_result(&snapshot, context).await?;
        ensure_live(context)?;
        Ok((snapshot, output))
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(
            operation,
            "Market.GetHistoryPreparation"
                | "Market.CancelHistoryPreparation"
                | "Job.List"
                | "Job.Get"
                | "Job.Watch"
                | "Job.Cancel"
                | "Job.Confirm"
                | "Job.Retry"
                | "Job.ReconcileStart"
                | "Job.CancelStart"
        )
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        history_runner: &MarketHistoryJobRunner,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let arguments = super::business_arguments(request.arguments());
        let content = match request.name() {
            "Market.GetHistoryPreparation" | "Market.CancelHistoryPreparation" => {
                let input: HistoryPreparationRequest = decode(&arguments)?;
                let snapshot = self
                    .history_snapshot(history_runner, &input, context)
                    .await?;
                let view = if request.name() == "Market.CancelHistoryPreparation" {
                    self.cancel_generation(
                        snapshot.id(),
                        snapshot.generation(),
                        JobEventSequence::new(
                            input
                                .expected_sequence
                                .ok_or(ServiceError::InvalidRequest)?,
                        ),
                        history_runner,
                    )
                    .await?
                } else {
                    JobView::from_snapshot(&snapshot).map_err(map_application)?
                };
                encode(view)?
            }
            "Job.ReconcileStart" | "Job.CancelStart" => {
                let input: StartReconciliationRequest = decode(&arguments)?;
                let binding = JobStartBinding::new(
                    authenticated_origin(context)?,
                    parse_request_id(&input.request_id)?,
                    input.operation,
                    parse_sha256(&input.arguments_sha256)?,
                );
                encode(
                    if request.name() == "Job.CancelStart" {
                        self.application.cancel_start(&binding).await
                    } else {
                        self.application.reconcile_start(&binding).await
                    }
                    .map_err(map_application)?,
                )?
            }
            "Job.List" => {
                let input: ListRequest = decode(&arguments)?;
                let cursor = input
                    .after_job_id
                    .map(|value| {
                        SourceIdentifier::try_from(value)
                            .map(JobListCursor::new)
                            .map_err(|_error| ServiceError::InvalidRequest)
                    })
                    .transpose()?;
                let limit = JobListPageLimit::try_new(input.limit)
                    .map_err(|_error| ServiceError::InvalidRequest)?;
                encode(
                    self.application
                        .list(cursor.as_ref(), limit)
                        .await
                        .map_err(map_application)?,
                )?
            }
            "Job.Get" => {
                let input: GetRequest = decode(&arguments)?;
                encode(
                    self.application
                        .get(
                            parse_id(&input.job_id)?,
                            parse_generation(input.generation)?,
                        )
                        .await
                        .map_err(map_application)?,
                )?
            }
            "Job.Watch" => {
                let input: WatchRequest = decode(&arguments)?;
                encode(
                    self.application
                        .watch(
                            parse_id(&input.job_id)?,
                            parse_generation(input.generation)?,
                            JobEventSequence::new(input.after_sequence),
                            JobEventPageLimit::try_new(input.limit)
                                .map_err(|_error| ServiceError::InvalidRequest)?,
                        )
                        .await
                        .map_err(map_application)?,
                )?
            }
            "Job.Cancel" => {
                let input: MutationRequest = decode(&arguments)?;
                encode(
                    self.cancel_generation(
                        parse_id(&input.job_id)?,
                        parse_generation(input.generation)?,
                        JobEventSequence::new(input.expected_sequence),
                        history_runner,
                    )
                    .await?,
                )?
            }
            "Job.Confirm" => {
                let input: ConfirmRequest = decode(&arguments)?;
                let confirmation = JobConfirmation::new(
                    parse_id(&input.job_id)?,
                    parse_generation(input.generation)?,
                    JobEventSequence::new(input.expected_sequence),
                    input.identity,
                    parse_sha256(&input.digest)?,
                );
                encode(
                    self.application
                        .confirm(
                            &confirmation,
                            super::runtime::current_timestamp()
                                .map_err(|_error| ServiceError::Unavailable)?,
                        )
                        .await
                        .map_err(map_application)?,
                )?
            }
            "Job.Retry" => {
                let input: MutationRequest = decode(&arguments)?;
                encode(
                    self.application
                        .retry(
                            parse_id(&input.job_id)?,
                            parse_generation(input.generation)?,
                            JobEventSequence::new(input.expected_sequence),
                            super::runtime::current_timestamp()
                                .map_err(|_error| ServiceError::Unavailable)?,
                        )
                        .await
                        .map_err(map_application)?,
                )?
            }
            _ => return Err(ServiceError::NotFound),
        };
        ensure_live(context)?;
        TypedToolResult::try_new(
            content,
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }

    /// Applies the same durable cancellation and queued-input release for scoped and generic calls.
    async fn cancel_generation(
        &self,
        id: JobId,
        generation: JobGeneration,
        expected: JobEventSequence,
        history_runner: &MarketHistoryJobRunner,
    ) -> Result<JobView, ServiceError> {
        let view = self
            .application
            .cancel(
                id,
                generation,
                expected,
                super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?,
            )
            .await
            .map_err(map_application)?;
        if view.state().is_terminal() {
            // After a durable cancellation, cleanup must not be skipped merely because its caller
            // disconnected. The repository owns bounded IO; only queued input can be released.
            let snapshot = self
                .repository
                .get(id, generation)
                .await
                .map_err(|_| ServiceError::Unavailable)?;
            if history_runner.input(&snapshot).is_some() {
                history_runner
                    .release_terminal(&snapshot)
                    .map_err(super::tool_services::map_research_admission)?;
            }
        }
        Ok(view)
    }

    async fn history_snapshot(
        &self,
        runner: &MarketHistoryJobRunner,
        input: &HistoryPreparationRequest,
        context: &RequestContext,
    ) -> Result<market_squawk_jobs::JobSnapshot, ServiceError> {
        let origin = authenticated_origin(context)?;
        let id = parse_id(&input.job_id)?;
        let generation = parse_generation(input.generation)?;
        let snapshot = tokio::select! {
            biased;
            _ = context.cancellation().cancelled() => return Err(ServiceError::Cancelled),
            _ = tokio::time::sleep_until(context.deadline().into()) => return Err(ServiceError::DeadlineExceeded),
            result = self.repository.get(id, generation) => result.map_err(|error| match error {
                market_squawk_jobs::JobRepositoryError::NotFound => ServiceError::NotFound,
                _ => ServiceError::Unavailable,
            })?,
        };
        if snapshot.spec().origin() != &origin
            || !runner.belongs_to(&snapshot, &input.history_token)
        {
            return Err(ServiceError::Unauthorized);
        }
        Ok(snapshot)
    }

    pub(super) async fn start(
        &self,
        admission: JobAdmission,
        permit: &JobStartPermit,
        context: &RequestContext,
        metadata: ToolResultMetadata,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        if permit.binding().origin() != &authenticated_origin(context)?
            || permit.binding().request_id() != context.request_id()
        {
            return Err(ServiceError::Unauthorized);
        }
        let receipt = self
            .application
            .start_reserved(
                admission,
                permit,
                super::runtime::current_timestamp().map_err(|_error| ServiceError::Unavailable)?,
            )
            .await
            .map_err(map_application)?;
        ensure_live(context)?;
        TypedToolResult::try_new(encode(receipt)?, 1, metadata, context.limits())
            .map_err(Into::into)
    }

    pub(super) async fn begin_start(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<JobStartAdmission, ServiceError> {
        ensure_live(context)?;
        let bytes =
            serde_json::to_vec(request.arguments()).map_err(|_| ServiceError::InvalidRequest)?;
        let binding = JobStartBinding::new(
            authenticated_origin(context)?,
            context.request_id().clone(),
            SourceIdentifier::try_from(request.name()).map_err(|_| ServiceError::InvalidRequest)?,
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into()),
        );
        let admission = self
            .application
            .begin_start(&binding)
            .await
            .map_err(map_application)?;
        if let Err(error) = ensure_live(context) {
            // Only this newly issued permit belongs to the expiring call. An existing request
            // may already own a running job and must never be cancelled by a replay's deadline.
            if let RepositoryStartAdmission::Execute(permit) = &admission {
                self.reject_start(permit).await?;
            }
            return Err(error);
        }
        match admission {
            RepositoryStartAdmission::Execute(permit) => Ok(JobStartAdmission::Execute(permit)),
            RepositoryStartAdmission::Existing(reconciled) => {
                let snapshot = reconciled.snapshot().ok_or(match reconciled.state() {
                    JobStartState::NotAdmitted => ServiceError::Cancelled,
                    _ => ServiceError::Unavailable,
                })?;
                let result = TypedToolResult::try_new(
                    encode(JobReceipt::from_snapshot(snapshot))?,
                    1,
                    super::tool_services::job_receipt_metadata(request)?,
                    context.limits(),
                )
                .map_err(ServiceError::from)?;
                Ok(JobStartAdmission::Existing(result))
            }
        }
    }

    /// Cleanup ignores the expired transport deadline so durable input is never revoked on a guess.
    pub(super) async fn reject_start(&self, permit: &JobStartPermit) -> Result<bool, ServiceError> {
        let result = self
            .application
            .cancel_start(permit.binding())
            .await
            .map_err(map_application)?;
        Ok(result.state() == JobStartState::NotAdmitted)
    }

    pub(super) async fn view(
        &self,
        job_id: &str,
        generation: u64,
    ) -> Result<JobView, ServiceError> {
        self.application
            .get(parse_id(job_id)?, parse_generation(generation)?)
            .await
            .map_err(map_application)
    }

    pub(super) async fn product_activity_page(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        model: bool,
    ) -> Result<(Vec<JobView>, Value), ServiceError> {
        ensure_live(context)?;
        let input: ProductActivityRequest =
            decode(&super::business_arguments(request.arguments()))?;
        let limit = input.limit.unwrap_or(25);
        if !(1..=100).contains(&limit)
            || input
                .cursor
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > 512)
        {
            return Err(ServiceError::InvalidRequest);
        }
        let cursor = input
            .cursor
            .map(|value| {
                SourceIdentifier::try_from(value)
                    .map(JobListCursor::new)
                    .map_err(|_| ServiceError::InvalidRequest)
            })
            .transpose()?;
        let limit = JobListPageLimit::try_new(limit.min(context.limits().maximum_result_items()))
            .map_err(|_| ServiceError::InvalidRequest)?;
        let page = if model {
            self.repository
                .list_model_activity(cursor.as_ref(), limit)
                .await
        } else {
            self.repository
                .list_backtest_activity(cursor.as_ref(), limit)
                .await
        }
        .map_err(|_| ServiceError::Unavailable)?;
        let views = page
            .snapshots()
            .iter()
            .map(JobView::from_snapshot)
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_application)?;
        let next = serde_json::to_value(page.next()).map_err(|_| ServiceError::InvalidResult)?;
        ensure_live(context)?;
        Ok((views, next))
    }

    pub(super) async fn product_backtest_view(
        &self,
        token: uuid::Uuid,
        context: &RequestContext,
    ) -> Result<JobView, ServiceError> {
        ensure_live(context)?;
        let snapshot = self
            .repository
            .get_product_backtest(token)
            .await
            .map_err(|error| match error {
                market_squawk_jobs::JobRepositoryError::NotFound => ServiceError::NotFound,
                _ => ServiceError::Unavailable,
            })?;
        let view = JobView::from_snapshot(&snapshot).map_err(map_application)?;
        ensure_live(context)?;
        Ok(view)
    }
}

impl std::fmt::Debug for InstalledJobOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledJobOperations")
            .field("application", &"[DURABLE JOB APPLICATION]")
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ProductActivityRequest {
    cursor: Option<String>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ListRequest {
    after_job_id: Option<String>,
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StartReconciliationRequest {
    request_id: Value,
    operation: SourceIdentifier,
    arguments_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct HistoryPreparationRequest {
    history_token: String,
    job_id: String,
    generation: u64,
    expected_sequence: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct GetRequest {
    job_id: String,
    generation: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct WatchRequest {
    job_id: String,
    generation: u64,
    after_sequence: u64,
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MutationRequest {
    job_id: String,
    generation: u64,
    expected_sequence: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ConfirmRequest {
    job_id: String,
    generation: u64,
    expected_sequence: u64,
    identity: SourceIdentifier,
    digest: String,
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(arguments.clone()))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn encode(value: impl serde::Serialize) -> Result<Value, ServiceError> {
    serde_json::to_value(value).map_err(|_error| ServiceError::Internal)
}

fn parse_id(value: &str) -> Result<JobId, ServiceError> {
    JobId::try_from_str(value).map_err(|_error| ServiceError::InvalidRequest)
}

fn authenticated_origin(context: &RequestContext) -> Result<JobOrigin, ServiceError> {
    let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
    Ok(JobOrigin::new(
        SourceIdentifier::try_from(origin.workspace_id().to_string())
            .map_err(|_| ServiceError::Unauthorized)?,
        SourceIdentifier::try_from(origin.client_id().to_string())
            .map_err(|_| ServiceError::Unauthorized)?,
    ))
}

fn parse_request_id(value: &Value) -> Result<RequestId, ServiceError> {
    if let Some(value) = value.as_i64() {
        return Ok(RequestId::Integer(value));
    }
    value
        .as_str()
        .ok_or(ServiceError::InvalidRequest)
        .and_then(|value| RequestId::try_string(value).map_err(|_| ServiceError::InvalidRequest))
}

fn parse_generation(value: u64) -> Result<JobGeneration, ServiceError> {
    JobGeneration::try_new(value).map_err(|_error| ServiceError::InvalidRequest)
}

pub(super) fn parse_sha256(value: &str) -> Result<EvidenceDigest, ServiceError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ServiceError::InvalidRequest);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair).map_err(|_error| ServiceError::InvalidRequest)?;
        bytes[index] =
            u8::from_str_radix(pair, 16).map_err(|_error| ServiceError::InvalidRequest)?;
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

fn digest_text(bytes: [u8; 32]) -> String {
    crate::application::model::forecast_preparation::hex(market_squawk_data::Sha256Digest::new(
        bytes,
    ))
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if std::time::Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_application(error: JobApplicationError) -> ServiceError {
    match error {
        JobApplicationError::NotFound => ServiceError::NotFound,
        JobApplicationError::Contract => ServiceError::InvalidRequest,
        JobApplicationError::Repository | JobApplicationError::Authority => {
            ServiceError::Unavailable
        }
    }
}
