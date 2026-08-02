use market_squawk_domain::{EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_services::{ArtifactReference, RequestId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AdmittedJobInput, AdmittedJobSpec, JobAttemptLimit, JobAuthoritySnapshot,
    JobConfirmationRequest, JobContractError, JobEvent, JobEventSequence, JobFailure,
    JobGeneration, JobId, JobOrigin, JobProgress, JobRepositoryError, JobResultReference,
    JobSnapshot, JobState,
};

pub(super) fn state_code(state: JobState) -> i64 {
    match state {
        JobState::Queued => 0,
        JobState::Preparing => 1,
        JobState::Running => 2,
        JobState::AwaitingConfirmation => 3,
        JobState::Cancelling => 4,
        JobState::Completed => 5,
        JobState::Failed => 6,
        JobState::Cancelled => 7,
        JobState::Interrupted => 8,
        JobState::Recovering => 9,
    }
}

fn state_from_code(value: i64) -> Result<JobState, JobRepositoryError> {
    match value {
        0 => Ok(JobState::Queued),
        1 => Ok(JobState::Preparing),
        2 => Ok(JobState::Running),
        3 => Ok(JobState::AwaitingConfirmation),
        4 => Ok(JobState::Cancelling),
        5 => Ok(JobState::Completed),
        6 => Ok(JobState::Failed),
        7 => Ok(JobState::Cancelled),
        8 => Ok(JobState::Interrupted),
        9 => Ok(JobState::Recovering),
        _ => Err(JobRepositoryError::InvalidState),
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredSnapshot {
    id: Uuid,
    generation: u64,
    kind: SourceIdentifier,
    workspace: SourceIdentifier,
    client: SourceIdentifier,
    request_id: StoredRequestId,
    input_authority: SourceIdentifier,
    input_identity: SourceIdentifier,
    input_digest: EvidenceDigest,
    authority_name: SourceIdentifier,
    authority_identity: SourceIdentifier,
    authority_digest: EvidenceDigest,
    authority_captured_at: Timestamp,
    attempt_limit: u64,
    admitted_at: Timestamp,
    sequence: u64,
    state: i64,
    progress: Option<StoredProgress>,
    confirmation: Option<StoredConfirmation>,
    result: Option<StoredResult>,
    failure: Option<StoredFailure>,
    updated_at: Timestamp,
    cancellation_requested: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
enum StoredRequestId {
    Integer(i64),
    String(String),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredProgress {
    phase: SourceIdentifier,
    completed: u64,
    total: Option<u64>,
    recorded_at: Timestamp,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredConfirmation {
    identity: SourceIdentifier,
    digest: EvidenceDigest,
    expires_at: Timestamp,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredResult {
    authority: SourceIdentifier,
    identity: SourceIdentifier,
    evidence_digest: EvidenceDigest,
    artifacts: Vec<StoredArtifact>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredArtifact {
    id: String,
    sha256: String,
    byte_count: usize,
    media_type: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredFailure {
    class: SourceIdentifier,
    diagnostic: SourceIdentifier,
    retryable: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredEvent {
    state: i64,
    occurred_at: Timestamp,
    progress: Option<StoredProgress>,
    confirmation: Option<StoredConfirmation>,
    result: Option<StoredResult>,
    failure: Option<StoredFailure>,
}

pub(super) fn encode_snapshot(snapshot: &JobSnapshot) -> Result<Vec<u8>, JobRepositoryError> {
    serde_json::to_vec(&StoredSnapshot::from(snapshot))
        .map_err(|_| JobRepositoryError::InvalidState)
}

pub(super) fn decode_snapshot(bytes: &[u8]) -> Result<JobSnapshot, JobRepositoryError> {
    let stored: StoredSnapshot =
        serde_json::from_slice(bytes).map_err(|_| JobRepositoryError::InvalidState)?;
    stored.try_into()
}

pub(super) fn encode_event(event: &JobEvent) -> Result<Vec<u8>, JobRepositoryError> {
    serde_json::to_vec(&StoredEvent::from(event)).map_err(|_| JobRepositoryError::InvalidState)
}

pub(super) fn decode_event(bytes: &[u8]) -> Result<JobEvent, JobRepositoryError> {
    let stored: StoredEvent =
        serde_json::from_slice(bytes).map_err(|_| JobRepositoryError::InvalidState)?;
    stored.try_into()
}

impl From<&JobSnapshot> for StoredSnapshot {
    fn from(value: &JobSnapshot) -> Self {
        let spec = value.spec();
        Self {
            id: value.id().as_uuid(),
            generation: value.generation().get(),
            kind: spec.kind().clone(),
            workspace: spec.origin().workspace().clone(),
            client: spec.origin().client().clone(),
            request_id: StoredRequestId::from(spec.request_id()),
            input_authority: spec.input().authority().clone(),
            input_identity: spec.input().identity().clone(),
            input_digest: spec.input().digest(),
            authority_name: spec.authority().authority().clone(),
            authority_identity: spec.authority().identity().clone(),
            authority_digest: spec.authority().digest(),
            authority_captured_at: spec.authority().captured_at(),
            attempt_limit: spec.attempt_limit().get(),
            admitted_at: spec.admitted_at(),
            sequence: value.sequence().get(),
            state: state_code(value.state()),
            progress: value.progress().map(StoredProgress::from),
            confirmation: value.confirmation().map(StoredConfirmation::from),
            result: value.result().map(StoredResult::from),
            failure: value.failure().map(StoredFailure::from),
            updated_at: value.updated_at(),
            cancellation_requested: value.cancellation_requested(),
        }
    }
}

impl TryFrom<StoredSnapshot> for JobSnapshot {
    type Error = JobRepositoryError;

    fn try_from(value: StoredSnapshot) -> Result<Self, Self::Error> {
        let spec = AdmittedJobSpec::try_new(
            JobId::try_from_uuid(value.id).map_err(contract_error)?,
            JobGeneration::try_new(value.generation).map_err(contract_error)?,
            value.kind,
            JobOrigin::new(value.workspace, value.client),
            value.request_id.into_request_id()?,
            AdmittedJobInput::new(
                value.input_authority,
                value.input_identity,
                value.input_digest,
            ),
            JobAuthoritySnapshot::new(
                value.authority_name,
                value.authority_identity,
                value.authority_digest,
                value.authority_captured_at,
            ),
            JobAttemptLimit::try_new(value.attempt_limit).map_err(contract_error)?,
            value.admitted_at,
        )
        .map_err(contract_error)?;
        JobSnapshot::try_new(
            spec,
            JobEventSequence::new(value.sequence),
            state_from_code(value.state)?,
            value.progress.map(TryInto::try_into).transpose()?,
            value.confirmation.map(Into::into),
            value.result.map(TryInto::try_into).transpose()?,
            value.failure.map(Into::into),
            value.updated_at,
            value.cancellation_requested,
        )
        .map_err(contract_error)
    }
}

impl From<&RequestId> for StoredRequestId {
    fn from(value: &RequestId) -> Self {
        match value {
            RequestId::Integer(value) => Self::Integer(*value),
            RequestId::String(value) => Self::String(value.to_string()),
        }
    }
}

impl StoredRequestId {
    fn into_request_id(self) -> Result<RequestId, JobRepositoryError> {
        match self {
            Self::Integer(value) => Ok(RequestId::Integer(value)),
            Self::String(value) => {
                RequestId::try_string(value).map_err(|_| JobRepositoryError::InvalidState)
            }
        }
    }
}

impl From<&JobProgress> for StoredProgress {
    fn from(value: &JobProgress) -> Self {
        Self {
            phase: value.phase().clone(),
            completed: value.completed(),
            total: value.total(),
            recorded_at: value.recorded_at(),
        }
    }
}

impl TryFrom<StoredProgress> for JobProgress {
    type Error = JobRepositoryError;

    fn try_from(value: StoredProgress) -> Result<Self, Self::Error> {
        JobProgress::try_new(value.phase, value.completed, value.total, value.recorded_at)
            .map_err(contract_error)
    }
}

impl From<&JobConfirmationRequest> for StoredConfirmation {
    fn from(value: &JobConfirmationRequest) -> Self {
        Self {
            identity: value.identity().clone(),
            digest: value.digest(),
            expires_at: value.expires_at(),
        }
    }
}

impl From<StoredConfirmation> for JobConfirmationRequest {
    fn from(value: StoredConfirmation) -> Self {
        Self::new(value.identity, value.digest, value.expires_at)
    }
}

impl From<&JobResultReference> for StoredResult {
    fn from(value: &JobResultReference) -> Self {
        Self {
            authority: value.authority().clone(),
            identity: value.identity().clone(),
            evidence_digest: value.evidence_digest(),
            artifacts: value.artifacts().iter().map(StoredArtifact::from).collect(),
        }
    }
}

impl TryFrom<StoredResult> for JobResultReference {
    type Error = JobRepositoryError;

    fn try_from(value: StoredResult) -> Result<Self, Self::Error> {
        let artifacts = value
            .artifacts
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        JobResultReference::try_new(
            value.authority,
            value.identity,
            value.evidence_digest,
            artifacts,
        )
        .map_err(contract_error)
    }
}

impl From<&ArtifactReference> for StoredArtifact {
    fn from(value: &ArtifactReference) -> Self {
        Self {
            id: value.id().to_owned(),
            sha256: value.sha256().to_owned(),
            byte_count: value.byte_count(),
            media_type: value.media_type().to_owned(),
        }
    }
}

impl TryFrom<StoredArtifact> for ArtifactReference {
    type Error = JobRepositoryError;

    fn try_from(value: StoredArtifact) -> Result<Self, Self::Error> {
        ArtifactReference::try_new(value.id, value.sha256, value.byte_count, value.media_type)
            .map_err(|_| JobRepositoryError::InvalidState)
    }
}

impl From<&JobFailure> for StoredFailure {
    fn from(value: &JobFailure) -> Self {
        Self {
            class: value.class().clone(),
            diagnostic: value.diagnostic().clone(),
            retryable: value.retryable(),
        }
    }
}

impl From<StoredFailure> for JobFailure {
    fn from(value: StoredFailure) -> Self {
        Self::new(value.class, value.diagnostic, value.retryable)
    }
}

impl From<&JobEvent> for StoredEvent {
    fn from(value: &JobEvent) -> Self {
        Self {
            state: state_code(value.state()),
            occurred_at: value.occurred_at(),
            progress: value.progress().map(StoredProgress::from),
            confirmation: value.confirmation().map(StoredConfirmation::from),
            result: value.result().map(StoredResult::from),
            failure: value.failure().map(StoredFailure::from),
        }
    }
}

impl TryFrom<StoredEvent> for JobEvent {
    type Error = JobRepositoryError;

    fn try_from(value: StoredEvent) -> Result<Self, Self::Error> {
        JobEvent::try_new_with_confirmation(
            state_from_code(value.state)?,
            value.occurred_at,
            value.progress.map(TryInto::try_into).transpose()?,
            value.confirmation.map(Into::into),
            value.result.map(TryInto::try_into).transpose()?,
            value.failure.map(Into::into),
        )
        .map_err(contract_error)
    }
}

fn contract_error(_error: JobContractError) -> JobRepositoryError {
    JobRepositoryError::InvalidState
}
