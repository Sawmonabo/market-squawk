//! Typed native job control over the installed service's sole durable authority.

use std::sync::Arc;

use market_squawk_domain::{EvidenceDigest, SourceIdentifier};
use market_squawk_jobs::{
    JobAuthority, JobConfirmation, JobEventPageLimit, JobEventSequence, JobGeneration, JobId,
    JobListCursor, JobListPageLimit, JobRepository as _, SqliteJobRepository,
};
use market_squawk_runtime::{DispatchError, OperationEffect};
use market_squawk_services::RequestContext;
use serde::Deserialize;
use serde_json::Value;

use crate::jobs::InstalledJobAuthority;

pub(super) const OPERATIONS: [&str; 6] = [
    "Job.List",
    "Job.Get",
    "Job.Watch",
    "Job.Cancel",
    "Job.Confirm",
    "Job.Retry",
];

/// Closed installed-service job query and mutation authority.
pub(super) struct InstalledJobOperations {
    repository: Arc<SqliteJobRepository>,
    authority: Arc<JobAuthority<SqliteJobRepository>>,
}

impl InstalledJobOperations {
    pub(super) fn new(jobs: &InstalledJobAuthority) -> Self {
        Self {
            repository: jobs.repository(),
            authority: jobs.authority(),
        }
    }

    pub(super) fn effect(operation: &str) -> Option<OperationEffect> {
        match operation {
            "Job.List" | "Job.Get" | "Job.Watch" => Some(OperationEffect::Read),
            "Job.Cancel" | "Job.Confirm" | "Job.Retry" => Some(OperationEffect::Mutation),
            _ => None,
        }
    }

    pub(super) async fn dispatch(
        &self,
        operation: &str,
        arguments: &Value,
        context: &RequestContext,
    ) -> Result<Value, DispatchError> {
        ensure_live(context)?;
        let value = match operation {
            "Job.List" => {
                let request: ListRequest = decode(arguments)?;
                let cursor = request
                    .after_job_id
                    .map(|value| {
                        SourceIdentifier::try_from(value)
                            .map(JobListCursor::new)
                            .map_err(|_error| DispatchError::Rejected)
                    })
                    .transpose()?;
                let limit = JobListPageLimit::try_new(request.limit)
                    .map_err(|_error| DispatchError::Rejected)?;
                serde_json::to_value(
                    self.repository
                        .list(cursor.as_ref(), limit)
                        .await
                        .map_err(map_repository)?,
                )
            }
            "Job.Get" => {
                let request: IdentityRequest = decode(arguments)?;
                serde_json::to_value(
                    self.repository
                        .get_latest(parse_id(&request.job_id)?)
                        .await
                        .map_err(map_repository)?,
                )
            }
            "Job.Watch" => {
                let request: WatchRequest = decode(arguments)?;
                serde_json::to_value(
                    self.repository
                        .events_after(
                            parse_id(&request.job_id)?,
                            parse_generation(request.generation)?,
                            JobEventSequence::new(request.after_sequence),
                            JobEventPageLimit::try_new(request.limit)
                                .map_err(|_error| DispatchError::Rejected)?,
                        )
                        .await
                        .map_err(map_repository)?,
                )
            }
            "Job.Cancel" => {
                let request: MutationRequest = decode(arguments)?;
                serde_json::to_value(
                    self.authority
                        .cancel(
                            parse_id(&request.job_id)?,
                            parse_generation(request.generation)?,
                            JobEventSequence::new(request.expected_sequence),
                            crate::service::runtime::current_timestamp()
                                .map_err(|_error| DispatchError::Unavailable)?,
                        )
                        .await
                        .map_err(map_authority)?,
                )
            }
            "Job.Confirm" => {
                let request: ConfirmRequest = decode(arguments)?;
                let confirmation = JobConfirmation::new(
                    parse_id(&request.job_id)?,
                    parse_generation(request.generation)?,
                    JobEventSequence::new(request.expected_sequence),
                    request.identity,
                    request.digest,
                );
                serde_json::to_value(
                    self.authority
                        .confirm(
                            &confirmation,
                            crate::service::runtime::current_timestamp()
                                .map_err(|_error| DispatchError::Unavailable)?,
                        )
                        .await
                        .map_err(map_authority)?,
                )
            }
            "Job.Retry" => {
                let request: MutationRequest = decode(arguments)?;
                serde_json::to_value(
                    self.authority
                        .retry(
                            parse_id(&request.job_id)?,
                            parse_generation(request.generation)?,
                            JobEventSequence::new(request.expected_sequence),
                            crate::service::runtime::current_timestamp()
                                .map_err(|_error| DispatchError::Unavailable)?,
                        )
                        .await
                        .map_err(map_authority)?,
                )
            }
            _ => return Err(DispatchError::Rejected),
        };
        value.map_err(|_error| DispatchError::Unavailable)
    }
}

impl std::fmt::Debug for InstalledJobOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledJobOperations")
            .field("repository", &"[DURABLE JOB READ AUTHORITY]")
            .field("authority", &"[DURABLE JOB MUTATION AUTHORITY]")
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
    digest: EvidenceDigest,
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Value) -> Result<T, DispatchError> {
    serde_json::from_value(arguments.clone()).map_err(|_error| DispatchError::Rejected)
}

fn parse_id(value: &str) -> Result<JobId, DispatchError> {
    JobId::try_from_str(value).map_err(|_error| DispatchError::Rejected)
}

fn parse_generation(value: u64) -> Result<JobGeneration, DispatchError> {
    JobGeneration::try_new(value).map_err(|_error| DispatchError::Rejected)
}

fn ensure_live(context: &RequestContext) -> Result<(), DispatchError> {
    if context.cancellation().is_cancelled() || std::time::Instant::now() >= context.deadline() {
        Err(DispatchError::Interrupted)
    } else {
        Ok(())
    }
}

fn map_repository(error: market_squawk_jobs::JobRepositoryError) -> DispatchError {
    match error {
        market_squawk_jobs::JobRepositoryError::NotFound
        | market_squawk_jobs::JobRepositoryError::Conflict
        | market_squawk_jobs::JobRepositoryError::InvalidTransition
        | market_squawk_jobs::JobRepositoryError::Terminal
        | market_squawk_jobs::JobRepositoryError::InvalidState => DispatchError::Rejected,
        market_squawk_jobs::JobRepositoryError::Unavailable => DispatchError::Unavailable,
    }
}

fn map_authority(error: market_squawk_jobs::JobAuthorityError) -> DispatchError {
    match error {
        market_squawk_jobs::JobAuthorityError::UnknownKind
        | market_squawk_jobs::JobAuthorityError::Capacity
        | market_squawk_jobs::JobAuthorityError::Contract => DispatchError::Rejected,
        market_squawk_jobs::JobAuthorityError::Repository
        | market_squawk_jobs::JobAuthorityError::ShutdownIncomplete => DispatchError::Unavailable,
    }
}
