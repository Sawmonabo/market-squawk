//! Typed job control shared by every installed transport.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_jobs::{
    JobConfirmation, JobEventPageLimit, JobEventSequence, JobGeneration, JobId, JobListCursor,
    JobListPageLimit, JobOrigin, SqliteJobRepository,
};
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::{
    application::job::{JobAdmission, JobApplication, JobApplicationError},
    jobs::InstalledJobAuthority,
};

/// Closed typed job query and mutation authority.
pub(super) struct InstalledJobOperations {
    application: JobApplication<SqliteJobRepository>,
}

impl InstalledJobOperations {
    pub(super) fn new(jobs: &InstalledJobAuthority) -> Self {
        Self {
            application: JobApplication::new(jobs.repository(), jobs.authority()),
        }
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(
            operation,
            "Job.List" | "Job.Get" | "Job.Watch" | "Job.Cancel" | "Job.Confirm" | "Job.Retry"
        )
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let arguments = mutation_arguments(request.arguments());
        let content = match request.name() {
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
                let input: IdentityRequest = decode(&arguments)?;
                encode(
                    self.application
                        .get(parse_id(&input.job_id)?)
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
                    self.application
                        .cancel(
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

    pub(super) async fn start(
        &self,
        admission: JobAdmission,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let workspace = SourceIdentifier::try_from(origin.workspace_id().to_string())
            .map_err(|_error| ServiceError::Unauthorized)?;
        let client = SourceIdentifier::try_from(origin.client_id().to_string())
            .map_err(|_error| ServiceError::Unauthorized)?;
        let receipt = self
            .application
            .start(
                admission,
                JobOrigin::new(workspace, client),
                context.request_id().clone(),
                super::runtime::current_timestamp().map_err(|_error| ServiceError::Unavailable)?,
            )
            .await
            .map_err(map_application)?;
        ensure_live(context)?;
        TypedToolResult::try_new(
            encode(receipt)?,
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
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
struct ListRequest {
    after_job_id: Option<String>,
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct IdentityRequest {
    job_id: String,
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

fn mutation_arguments(arguments: &Map<String, Value>) -> Map<String, Value> {
    let mut admitted = arguments.clone();
    admitted.remove("confirm");
    admitted
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

fn parse_generation(value: u64) -> Result<JobGeneration, ServiceError> {
    JobGeneration::try_new(value).map_err(|_error| ServiceError::InvalidRequest)
}

fn parse_sha256(value: &str) -> Result<EvidenceDigest, ServiceError> {
    if value.len() != 64 {
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
        JobApplicationError::WaitCancelled => ServiceError::Cancelled,
        JobApplicationError::WaitDeadlineExceeded => ServiceError::DeadlineExceeded,
        JobApplicationError::Contract => ServiceError::InvalidRequest,
        JobApplicationError::Repository | JobApplicationError::Authority => {
            ServiceError::Unavailable
        }
    }
}
