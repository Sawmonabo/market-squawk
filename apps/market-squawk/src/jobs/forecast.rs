//! Forecast jobs retain their exact prepared input in the existing controlled artifact authority.

use std::{
    fmt,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    AdmittedJobInput, JobAttemptLimit, JobAuthoritySnapshot, JobCompletion, JobFailure,
    JobProgress, JobRecoveryDisposition, JobResultReference, JobRunContext, JobRunError, JobRunner,
    JobRunnerEvent, JobSnapshot, JobState,
};
use market_squawk_services::{
    ArtifactAuthority, ArtifactError, ArtifactPublication, ArtifactPublicationContext,
    ArtifactReadContext, ArtifactReadRequest, ArtifactReference, ArtifactResolveRequest,
    RequestContext, RequestOrigin, ServiceError, ServiceLimits, TypedToolResult,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    JobTerminalCommitSlot,
    research::{ResearchJobRunnerError, map_service_error},
};
use crate::application::{
    job::JobAdmission,
    model::{
        forecast::{ForecastApplicationError, ForecastJobExecutor, ForecastPrecommitAuthority},
        forecast_preparation::{
            ForecastPreparationAuthority, ForecastPreparationError,
            MAXIMUM_FORECAST_JOB_INPUT_BYTES, PreparedForecastJobInput,
        },
    },
};

const KIND: &str = "model.forecast-generation.v1";
const INPUT_AUTHORITY: &str = "model.forecast-input.v1";
const RESULT_AUTHORITY: &str = "model.forecast-vintage.v1";
const MAXIMUM_ATTEMPTS: u64 = 3;

/// Forecast runner failure uses the shared closed application-operation contract.
pub type ForecastJobRunnerError = ResearchJobRunnerError;

/// Owns forecast execution over the sole model and controlled artifact authorities.
pub struct ForecastJobRunner {
    kind: SourceIdentifier,
    input_authority: SourceIdentifier,
    result_authority: SourceIdentifier,
    authority_digest: EvidenceDigest,
    model: Arc<dyn ForecastJobExecutor>,
    artifacts: Arc<dyn ArtifactAuthority>,
    preparation: Option<Arc<ForecastPreparationAuthority>>,
    run_timeout: Duration,
}

impl ForecastJobRunner {
    /// Binds durable replay to the same preparation, artifact and model owners used for admission.
    pub(crate) fn try_new(
        model: Arc<dyn ForecastJobExecutor>,
        artifacts: Arc<dyn ArtifactAuthority>,
        preparation: Option<Arc<ForecastPreparationAuthority>>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, ForecastJobRunnerError> {
        if maximum_pending == 0
            || maximum_pending > 4_096
            || run_timeout.is_zero()
            || run_timeout > Duration::from_secs(24 * 60 * 60)
        {
            return Err(ForecastJobRunnerError::InvalidLimits);
        }
        Ok(Self {
            kind: identifier(KIND)?,
            input_authority: identifier(INPUT_AUTHORITY)?,
            result_authority: identifier(RESULT_AUTHORITY)?,
            authority_digest: digest(RESULT_AUTHORITY.as_bytes()),
            model,
            artifacts,
            preparation,
            run_timeout,
        })
    }

    pub(crate) fn preparation_authority(&self) -> Option<Arc<ForecastPreparationAuthority>> {
        self.preparation.clone()
    }

    /// Publishes the exact private-minted request before the existing jobs writer admits execution.
    pub(crate) async fn admit(
        &self,
        input: PreparedForecastJobInput,
        limits: ServiceLimits,
        captured_at: Timestamp,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<JobAdmission, ForecastJobRunnerError> {
        let publication_context = ArtifactPublicationContext::new(cancellation, deadline);
        publication_context
            .ensure_live()
            .map_err(map_artifact_admission)?;
        if self.preparation.is_none() {
            return Err(ForecastJobRunnerError::Unavailable);
        }
        let encoded = input
            .into_bytes(limits)
            .map_err(|error| map_admission_service(map_preparation_service(error)))?;
        let input_digest = digest(&encoded);
        let artifact = self
            .artifacts
            .publish(
                ArtifactPublication::try_json(encoded)
                    .map_err(|_| ForecastJobRunnerError::InvalidRequest)?,
                publication_context,
            )
            .await
            .map_err(map_artifact_admission)?;
        Ok(JobAdmission::new(
            self.kind.clone(),
            AdmittedJobInput::new(
                self.input_authority.clone(),
                identifier(artifact.id())?,
                input_digest,
            ),
            JobAuthoritySnapshot::new(
                self.result_authority.clone(),
                self.result_authority.clone(),
                self.authority_digest,
                captured_at,
            ),
            JobAttemptLimit::try_new(MAXIMUM_ATTEMPTS)
                .map_err(|_| ForecastJobRunnerError::InvalidRequest)?,
        ))
    }

    /// Rejected admission leaves only an unreferenced immutable artifact, never a runnable registry entry.
    pub fn revoke(&self, admission: &JobAdmission) -> Result<(), ForecastJobRunnerError> {
        if admission.kind() != &self.kind || admission.input().authority() != &self.input_authority
        {
            return Err(ForecastJobRunnerError::InvalidRequest);
        }
        Ok(())
    }

    fn validate_snapshot(&self, snapshot: &JobSnapshot) -> Result<RequestOrigin, JobRunError> {
        let spec = snapshot.spec();
        if spec.kind() != &self.kind
            || spec.input().authority() != &self.input_authority
            || spec.authority().authority() != &self.result_authority
            || spec.authority().identity() != &self.result_authority
            || spec.authority().digest() != self.authority_digest
            || spec.attempt_limit().get() != MAXIMUM_ATTEMPTS
        {
            return Err(JobRunError::Recovery);
        }
        RequestOrigin::try_new(
            Uuid::parse_str(spec.origin().workspace().as_str())
                .map_err(|_| JobRunError::Recovery)?,
            Uuid::parse_str(spec.origin().client().as_str()).map_err(|_| JobRunError::Recovery)?,
        )
        .map_err(|_| JobRunError::Recovery)
    }

    async fn load_input(
        &self,
        snapshot: &JobSnapshot,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<(PreparedForecastJobInput, ServiceLimits), ServiceError> {
        let origin = self
            .validate_snapshot(snapshot)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let maximum = NonZeroUsize::new(MAXIMUM_FORECAST_JOB_INPUT_BYTES)
            .ok_or(ServiceError::InvalidRequest)?;
        let read_context = ArtifactReadContext::new(cancellation, deadline);
        read_context.ensure_live().map_err(map_artifact_service)?;
        let reference = self
            .artifacts
            .resolve(
                ArtifactResolveRequest::try_new(
                    snapshot.spec().input().identity().as_str(),
                    maximum,
                )
                .map_err(map_artifact_service)?,
                read_context.clone(),
            )
            .await
            .map_err(map_artifact_service)?;
        if reference.sha256() != encode_hex(snapshot.spec().input().digest().bytes()) {
            return Err(ServiceError::InvalidRequest);
        }
        let read = self
            .artifacts
            .read(
                ArtifactReadRequest::try_new(reference, maximum).map_err(map_artifact_service)?,
                read_context.clone(),
            )
            .await
            .map_err(map_artifact_service)?;
        if digest(read.content()) != snapshot.spec().input().digest() {
            return Err(ServiceError::InvalidRequest);
        }
        let restored = self
            .preparation
            .as_ref()
            .ok_or(ServiceError::Unavailable)?
            .restore_job_input(read.content(), origin)
            .map_err(map_preparation_service)?;
        read_context.ensure_live().map_err(map_artifact_service)?;
        Ok(restored)
    }

    async fn recovered_result(
        &self,
        snapshot: &JobSnapshot,
    ) -> Result<Option<JobResultReference>, JobRunError> {
        let deadline = Instant::now()
            .checked_add(self.run_timeout)
            .ok_or(JobRunError::Recovery)?;
        // A later cancellation cannot undo an earlier durable domain commit. This bounded read
        // does not execute a forecast and therefore does not inherit the orphan's cancellation bit.
        let cancellation = CancellationToken::new();
        let (input, limits) = self
            .load_input(snapshot, cancellation.clone(), deadline)
            .await
            .map_err(map_service_error)?;
        let request_context = RequestContext::new(
            snapshot.spec().request_id().clone(),
            cancellation,
            deadline,
            limits,
        )
        .with_origin(input.origin().map_err(map_preparation_error)?);
        self.model
            .recover_forecast_job_output(input.request(), &request_context)
            .await
            .map_err(map_service_error)?
            .map(|output| self.result_reference(output.artifact))
            .transpose()
    }

    fn result_reference(
        &self,
        artifact: ArtifactReference,
    ) -> Result<JobResultReference, JobRunError> {
        let result_digest = digest_from_hex(artifact.sha256())?;
        JobResultReference::try_new(
            self.result_authority.clone(),
            identifier(artifact.id()).map_err(|_| JobRunError::Recovery)?,
            result_digest,
            vec![artifact],
        )
        .map_err(|_| JobRunError::Recovery)
    }

    /// Projects the exact completed vintage after checking the authenticated job's retained input.
    /// The service passes its already-authorized snapshot and current installed request context.
    pub(crate) async fn read_result(
        &self,
        snapshot: &JobSnapshot,
        context: &RequestContext,
    ) -> Result<ForecastJobResult, ServiceError> {
        let origin = self
            .validate_snapshot(snapshot)
            .map_err(|_| ServiceError::InvalidRequest)?;
        if context.origin() != Some(origin) {
            return Err(ServiceError::Unauthorized);
        }
        if snapshot.state() != JobState::Completed {
            return Err(ServiceError::InvalidRequest);
        }
        let (input, _) = self
            .load_input(snapshot, context.cancellation().clone(), context.deadline())
            .await?;
        let reference = snapshot
            .terminal_result()
            .ok_or(ServiceError::InvalidResult)?;
        let [artifact] = reference.artifacts() else {
            return Err(ServiceError::InvalidResult);
        };
        if reference.authority() != &self.result_authority
            || reference.evidence_digest()
                != digest_from_hex(artifact.sha256()).map_err(|_| ServiceError::InvalidResult)?
        {
            return Err(ServiceError::InvalidResult);
        }
        let result = self
            .model
            .read_forecast_job_result(input.request(), artifact, context)
            .await?;
        Ok(ForecastJobResult {
            result,
            request_sha256: input.request_sha256(),
            financial_profile_digest: input.financial_profile_digest(),
        })
    }
}

/// Exact product result plus original preparation commitments for the native workflow.
pub(crate) struct ForecastJobResult {
    pub(crate) result: TypedToolResult,
    pub(crate) request_sha256: [u8; 32],
    pub(crate) financial_profile_digest: Option<[u8; 32]>,
}

struct JobForecastCommitAuthority {
    slot: JobTerminalCommitSlot,
}
impl ForecastPrecommitAuthority for JobForecastCommitAuthority {
    fn validate_precommit(&self) -> Result<(), ForecastApplicationError> {
        self.slot.claim().map_err(|error| match error {
            JobRunError::Cancelled => ForecastApplicationError::Artifact(ArtifactError::Cancelled),
            JobRunError::Failed(_) | JobRunError::Recovery => ForecastApplicationError::Unavailable,
        })
    }
    fn commit_succeeded(&self) {
        self.slot.seal_domain_commit();
    }
}

#[async_trait]
impl JobRunner for ForecastJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        let deadline = Instant::now()
            .checked_add(self.run_timeout)
            .ok_or(JobRunError::Recovery)?;
        let (input, limits) = self
            .load_input(context.snapshot(), context.cancellation().clone(), deadline)
            .await
            .map_err(map_service_error)?;
        let progress = JobProgress::try_new(
            identifier("validating-inputs").map_err(|_| JobRunError::Recovery)?,
            0,
            None,
            context.snapshot().updated_at_timestamp(),
        )
        .map_err(|_| JobRunError::Recovery)?;
        let progressed = context
            .events()
            .append(JobRunnerEvent::Progress(progress))
            .await
            .map_err(|_| failed("forecast-progress-unavailable", true))?;
        let request_context = RequestContext::new(
            context.snapshot().spec().request_id().clone(),
            context.cancellation().clone(),
            deadline,
            limits,
        )
        .with_origin(input.origin().map_err(map_preparation_error)?);
        let commit = JobForecastCommitAuthority {
            slot: JobTerminalCommitSlot::new(&context, progressed.sequence()),
        };
        let output = match self
            .model
            .replay_forecast_for_job(input.request(), &request_context, &commit)
            .await
            .map_err(map_service_error)?
        {
            Some(output) => output,
            None => {
                self.preparation
                    .as_ref()
                    .ok_or(JobRunError::Recovery)?
                    .revalidate_job_input(&input, deadline, context.cancellation().clone())
                    .await
                    .map_err(map_preparation_error)?;
                self.model
                    .generate_forecast_for_job(input.request(), &request_context, &commit)
                    .await
                    .map_err(map_service_error)?
            }
        };
        // The domain already durably published this very artifact under the terminal commit slot.
        // No cancellable I/O or second artifact publication follows that commit.
        let published = commit.slot.take_published()?;
        let result = self.result_reference(output.artifact)?;
        Ok(JobCompletion::Published(result, published))
    }

    async fn recover(&self, snapshot: &JobSnapshot) -> JobRecoveryDisposition {
        match self.recovered_result(snapshot).await {
            Ok(Some(result)) => JobRecoveryDisposition::CompleteAlreadyPublished(result),
            Ok(None) if snapshot.generation().get() < MAXIMUM_ATTEMPTS => {
                JobRecoveryDisposition::RetryFromImmutableInput
            }
            Ok(None) => JobRecoveryDisposition::MarkInterrupted,
            Err(JobRunError::Failed(failure)) => JobRecoveryDisposition::Fail(failure),
            Err(JobRunError::Cancelled | JobRunError::Recovery) => {
                match failed("forecast-recovery-authority-unavailable", true) {
                    JobRunError::Failed(failure) => JobRecoveryDisposition::Fail(failure),
                    _ => JobRecoveryDisposition::MarkInterrupted,
                }
            }
        }
    }
}

impl fmt::Debug for ForecastJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ForecastJobRunner")
            .field("operation", &"Model.GenerateForecast")
            .field("input", &"[DURABLE EXACT PREPARATION]")
            .field("artifacts", &"[CONTROLLED ARTIFACT AUTHORITY]")
            .finish()
    }
}

fn identifier(value: impl AsRef<str>) -> Result<SourceIdentifier, ForecastJobRunnerError> {
    SourceIdentifier::try_from(value.as_ref()).map_err(|_| ForecastJobRunnerError::InvalidRequest)
}
fn digest(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}
fn encode_hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn digest_from_hex(value: &str) -> Result<EvidenceDigest, JobRunError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(JobRunError::Recovery);
    }
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| JobRunError::Recovery)?;
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}
fn failed(code: &str, retryable: bool) -> JobRunError {
    match SourceIdentifier::try_from(code) {
        Ok(code) => JobRunError::Failed(JobFailure::new(code.clone(), code, retryable)),
        Err(_) => JobRunError::Recovery,
    }
}
fn map_artifact_service(error: ArtifactError) -> ServiceError {
    match error {
        ArtifactError::Cancelled => ServiceError::Cancelled,
        ArtifactError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ArtifactError::ReadLimitExceeded => ServiceError::ResourceExhausted,
        ArtifactError::InvalidReference => ServiceError::InvalidRequest,
        ArtifactError::InvalidPublication => ServiceError::InvalidResult,
        ArtifactError::NotFound => ServiceError::NotFound,
        ArtifactError::Unavailable => ServiceError::Unavailable,
    }
}
fn map_artifact_admission(error: ArtifactError) -> ForecastJobRunnerError {
    map_admission_service(map_artifact_service(error))
}
fn map_admission_service(error: ServiceError) -> ForecastJobRunnerError {
    match error {
        ServiceError::Cancelled => ForecastJobRunnerError::Cancelled,
        ServiceError::DeadlineExceeded => ForecastJobRunnerError::DeadlineExceeded,
        ServiceError::ResourceExhausted => ForecastJobRunnerError::Capacity,
        ServiceError::Unavailable | ServiceError::Internal => ForecastJobRunnerError::Unavailable,
        _ => ForecastJobRunnerError::InvalidRequest,
    }
}
fn map_preparation_service(error: ForecastPreparationError) -> ServiceError {
    match error {
        ForecastPreparationError::Cancelled => ServiceError::Cancelled,
        ForecastPreparationError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ForecastPreparationError::Capacity => ServiceError::ResourceExhausted,
        ForecastPreparationError::ModelUnavailable
        | ForecastPreparationError::Unavailable
        | ForecastPreparationError::TimeUnavailable => ServiceError::Unavailable,
        ForecastPreparationError::ReceiptUnavailable => ServiceError::NotFound,
        ForecastPreparationError::ReceiptMismatch => ServiceError::Unauthorized,
        _ => ServiceError::InvalidRequest,
    }
}
fn map_preparation_error(error: ForecastPreparationError) -> JobRunError {
    map_service_error(map_preparation_service(error))
}
