//! Durable-job authority for one bounded official SEC fund publication.
//!
//! The common job repository and scheduler remain the sole owners of queued, running, failed,
//! cancelled, interrupted, and completed state. This leaf owns only immutable SEC request
//! admission, one-use execution, the terminal-publication fence, and deterministic result
//! evidence. The executor retains the exact coordinator-created live SEC source, claims terminal
//! job authority only at the final provider-logical precommit, and recovers by exact catalog
//! coordinate without any latest-generation fallback.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use market_squawk_data::{
    IngestError, IngestPrecommitAuthority, SecFundJobCoordinate as DataSecFundJobCoordinate,
    SecFundJobFamily as DataSecFundJobFamily, SecFundJobRecovery,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    AdmittedJobInput, JobAttemptLimit, JobAuthoritySnapshot, JobCompletion, JobFailure,
    JobGeneration, JobId, JobProgress, JobRecoveryDisposition, JobResultReference, JobRunContext,
    JobRunError, JobRunner, JobRunnerEvent, JobSnapshot, JobTerminalPublicationPermit,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    SecAdmittedFundProductRequest, SecFundProductBoundaryError, SecFundProductRequest,
    SecFundProductRequestFactory, SecFundPublicationProjection, SecFundPublicationReceipt,
    SecLiveFundApplicationError, SecLiveFundRequest, SecLiveFundSource,
};
use crate::application::job::JobAdmission;

const SEC_FUND_JOB_KIND: &str = "research.sec-fund-publication.v1";
const SEC_FUND_JOB_INPUT_AUTHORITY: &str = "research.sec-fund-publication-request.v1";
const SEC_FUND_JOB_RESULT_AUTHORITY: &str = "research.sec-fund-publication-result.v1";
const SEC_FUND_JOB_RESULT_DIGEST_DOMAIN: &[u8] = b"market-squawk/sec-fund-job-result/v1";
const SEC_FUND_JOB_MAXIMUM_PENDING: usize = 64;

/// Exact durable generation coordinate that must join the SEC catalog commit atomically.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SecFundJobExecutionCoordinate {
    job_id: JobId,
    generation: JobGeneration,
    admitted_request_digest: EvidenceDigest,
}

impl SecFundJobExecutionCoordinate {
    pub(crate) const fn job_id(self) -> JobId {
        self.job_id
    }

    pub(crate) const fn generation(self) -> JobGeneration {
        self.generation
    }

    pub(crate) const fn admitted_request_digest(self) -> EvidenceDigest {
        self.admitted_request_digest
    }
}

/// One-shot common-job publication authority consumed at the SEC catalog commit boundary.
///
/// The executor must compose this authority only with the accepted SEC provider-generation
/// precommit and persist [`Self::execution_coordinate`] in the same existing analytical-catalog
/// transaction as the exact manifest and logical-publication binding. Calling
/// [`IngestPrecommitAuthority::validate_precommit`] before that irreversible boundary is invalid:
/// this authority intentionally grants the terminal fence once.
pub(crate) trait SecFundJobCommitAuthority: IngestPrecommitAuthority {
    fn execution_coordinate(&self) -> SecFundJobExecutionCoordinate;
}

/// Coordinator-owned durable executor retaining the exact activated SEC live source allocation.
struct SecFundAtomicJobExecutor {
    source: Arc<SecLiveFundSource>,
}

impl SecFundAtomicJobExecutor {
    fn new(source: Arc<SecLiveFundSource>) -> Self {
        Self { source }
    }

    async fn execute_atomic(
        &self,
        request: SecLiveFundRequest,
        cancellation: CancellationToken,
        commit: Arc<dyn SecFundJobCommitAuthority>,
    ) -> Result<SecFundPublicationReceipt, SecFundJobExecutionError> {
        self.source
            .acquire_and_publish_for_job(request, cancellation, commit)
            .await
            .map_err(map_live_execution_error)
    }

    fn recover_published(
        &self,
        coordinate: SecFundJobExecutionCoordinate,
    ) -> Result<Option<SecFundPublicationProjection>, SecFundJobExecutionError> {
        let durable_coordinate = DataSecFundJobCoordinate::try_new(
            coordinate.job_id.as_uuid(),
            coordinate.generation.get(),
            coordinate.admitted_request_digest,
        )
        .map_err(|_| SecFundJobExecutionError::InvalidPublication)?;
        let durable = match self
            .source
            .recover_job_publication(durable_coordinate)
            .map_err(map_live_execution_error)?
        {
            SecFundJobRecovery::NotDataCommitted => return Ok(None),
            SecFundJobRecovery::DataCommittedWithoutProjection => {
                return Err(SecFundJobExecutionError::InvalidPublication);
            }
            SecFundJobRecovery::Published(durable) => durable,
        };
        if durable.coordinate() != durable_coordinate {
            return Err(SecFundJobExecutionError::InvalidPublication);
        }
        let family = match durable.family() {
            DataSecFundJobFamily::Nport => super::SecFundProductFamily::Nport,
            DataSecFundJobFamily::Ncen => super::SecFundProductFamily::Ncen,
        };
        let product_coordinate = SecFundProductRequestFactory
            .recover_coordinate(
                family,
                durable.year(),
                durable.quarter(),
                durable.accession(),
                durable.fund_id(),
                coordinate.admitted_request_digest,
            )
            .map_err(|_| SecFundJobExecutionError::InvalidPublication)?;
        let projection = SecFundPublicationProjection::try_from_durable_evidence(
            product_coordinate,
            durable.pinned().manifest().clone(),
            durable.binding_digest(),
            durable.preparation_digest(),
            durable.fund_instrument_id(),
            durable.pinned(),
        )
        .map_err(|_| SecFundJobExecutionError::InvalidPublication)?;
        if projection.generation_row_count() != durable.row_count()
            || projection.generation_total_bytes() != durable.total_bytes()
            || projection.generation_object_count() != durable.object_count()
        {
            return Err(SecFundJobExecutionError::InvalidPublication);
        }
        Ok(Some(projection))
    }
}

/// Dedicated bounded runner over the common durable job state machine.
pub(crate) struct SecFundJobRunner {
    kind: SourceIdentifier,
    input_authority: SourceIdentifier,
    result_authority: SourceIdentifier,
    result_authority_digest: EvidenceDigest,
    factory: SecFundProductRequestFactory,
    executor: SecFundAtomicJobExecutor,
    pending: Mutex<BTreeMap<SourceIdentifier, Option<SecAdmittedFundProductRequest>>>,
}

impl SecFundJobRunner {
    pub(crate) fn try_new(source: Arc<SecLiveFundSource>) -> Result<Self, SecFundJobRunnerError> {
        let result_authority = identifier(SEC_FUND_JOB_RESULT_AUTHORITY)?;
        Ok(Self {
            kind: identifier(SEC_FUND_JOB_KIND)?,
            input_authority: identifier(SEC_FUND_JOB_INPUT_AUTHORITY)?,
            result_authority_digest: namespace_digest(SEC_FUND_JOB_RESULT_AUTHORITY),
            result_authority,
            factory: SecFundProductRequestFactory,
            executor: SecFundAtomicJobExecutor::new(source),
            pending: Mutex::new(BTreeMap::new()),
        })
    }

    pub(crate) const fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    /// Freezes one exact catalog/physical admission before durable job creation.
    pub(crate) fn admit(
        &self,
        request: SecFundProductRequest,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, SecFundJobRunnerError> {
        let admitted = self.factory.admit(request)?;
        let digest = admitted.admission_digest();
        let identity = identifier(format!(
            "sec-fund-publication-{}",
            encode_hex(digest.bytes())
        ))?;
        let attempt_limit = JobAttemptLimit::try_new(1)
            .map_err(|_error| SecFundJobRunnerError::InvalidConfiguration)?;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_error| SecFundJobRunnerError::Unavailable)?;
        if pending.contains_key(&identity) {
            return Err(SecFundJobRunnerError::Conflict);
        }
        if pending.len() >= SEC_FUND_JOB_MAXIMUM_PENDING {
            return Err(SecFundJobRunnerError::Capacity);
        }
        pending.insert(identity.clone(), Some(admitted));
        Ok(JobAdmission::new(
            self.kind.clone(),
            AdmittedJobInput::new(self.input_authority.clone(), identity, digest),
            JobAuthoritySnapshot::new(
                self.result_authority.clone(),
                self.result_authority.clone(),
                self.result_authority_digest,
                captured_at,
            ),
            attempt_limit,
        ))
    }

    /// Releases one still-pending request when common durable job creation fails.
    pub(crate) fn revoke(&self, admission: &JobAdmission) -> Result<(), SecFundJobRunnerError> {
        if admission.kind() != &self.kind || admission.input().authority() != &self.input_authority
        {
            return Err(SecFundJobRunnerError::InvalidRequest);
        }
        let mut pending = self
            .pending
            .lock()
            .map_err(|_error| SecFundJobRunnerError::Unavailable)?;
        match pending.get(admission.input().identity()) {
            Some(Some(admitted)) if admitted.admission_digest() == admission.input().digest() => {
                pending.remove(admission.input().identity());
                Ok(())
            }
            Some(Some(_)) => Err(SecFundJobRunnerError::InvalidRequest),
            Some(None) => Err(SecFundJobRunnerError::Conflict),
            None => Err(SecFundJobRunnerError::InvalidRequest),
        }
    }

    fn take_pending<'a>(
        &'a self,
        context: &JobRunContext,
    ) -> Result<SecFundPendingExecution<'a>, JobRunError> {
        validate_snapshot(
            context.snapshot(),
            &self.kind,
            &self.input_authority,
            &self.result_authority,
            self.result_authority_digest,
        )?;
        let input = context.snapshot().spec().input();
        let mut pending = self
            .pending
            .lock()
            .map_err(|_error| JobRunError::Recovery)?;
        let admitted = pending
            .get_mut(input.identity())
            .ok_or(JobRunError::Recovery)?
            .take()
            .ok_or(JobRunError::Recovery)?;
        if admitted.admission_digest() != input.digest() {
            pending.remove(input.identity());
            return Err(JobRunError::Recovery);
        }
        Ok(SecFundPendingExecution {
            pending: &self.pending,
            identity: input.identity().clone(),
            admitted: Some(admitted),
        })
    }

    fn result_reference(
        &self,
        execution: SecFundJobExecutionCoordinate,
        publication: &SecFundPublicationProjection,
    ) -> Result<JobResultReference, JobRunError> {
        validate_projection_admission(&self.factory, execution, publication)?;
        let digest = result_digest(execution, publication);
        let identity = identifier(format!(
            "sec-fund-result-{}-{}-{}",
            execution.job_id.as_uuid(),
            execution.generation.get(),
            encode_hex(digest.bytes())
        ))
        .map_err(|_error| JobRunError::Recovery)?;
        JobResultReference::try_new(self.result_authority.clone(), identity, digest, Vec::new())
            .map_err(|_error| JobRunError::Recovery)
    }
}

#[async_trait]
impl JobRunner for SecFundJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        let mut pending = self.take_pending(&context)?;
        if context.cancellation().is_cancelled() {
            return Ok(JobCompletion::Cancelled);
        }
        let recorded_at = system_timestamp()?;
        let progress = JobProgress::try_new(
            identifier("acquiring-official-sec-fund-filing")
                .map_err(|_error| JobRunError::Recovery)?,
            0,
            Some(1),
            recorded_at,
        )
        .map_err(|_error| JobRunError::Recovery)?;
        let progressed = context
            .events()
            .append(JobRunnerEvent::Progress(progress))
            .await
            .map_err(|_error| failed("sec-fund-job-progress-unavailable", true))?;
        let admitted = pending.take().ok_or(JobRunError::Recovery)?;
        let prepared = self
            .factory
            .prepare_for_run(admitted, system_timestamp()?)
            .map_err(map_boundary_error)?;
        let (coordinate, request) = prepared.into_parts();
        let execution = execution_coordinate(&context);
        let commit = Arc::new(SecFundTerminalCommitAuthority {
            execution,
            context: context.clone(),
            expected: progressed.sequence(),
            permit: Mutex::new(None),
        });
        let receipt = self
            .executor
            .execute_atomic(request, context.cancellation().clone(), commit.clone())
            .await
            .map_err(map_execution_error)?;
        let publication = match super::SecFundProductProjection::try_published(coordinate, receipt)
            .map_err(map_boundary_error)?
        {
            super::SecFundProductProjection::Published(publication) => publication,
            super::SecFundProductProjection::SetupRequired(_)
            | super::SecFundProductProjection::Unavailable(_)
            | super::SecFundProductProjection::Queued(_) => return Err(JobRunError::Recovery),
        };
        let result = self.result_reference(execution, &publication)?;
        let published = commit.seal()?;
        Ok(JobCompletion::Published(result, published))
    }

    fn recover(&self, snapshot: &JobSnapshot) -> JobRecoveryDisposition {
        if validate_snapshot(
            snapshot,
            &self.kind,
            &self.input_authority,
            &self.result_authority,
            self.result_authority_digest,
        )
        .is_err()
        {
            return recovery_failure("sec-fund-job-recovery-binding-invalid", false);
        }
        let execution = snapshot_execution_coordinate(snapshot);
        match self.executor.recover_published(execution) {
            Ok(Some(publication)) => match self.result_reference(execution, &publication) {
                Ok(result) => JobRecoveryDisposition::CompleteAlreadyPublished(result),
                Err(_error) => recovery_failure("sec-fund-job-recovery-result-invalid", false),
            },
            Ok(None) => JobRecoveryDisposition::MarkInterrupted,
            Err(error) => recovery_execution_failure(error),
        }
    }
}

impl fmt::Debug for SecFundJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecFundJobRunner")
            .field("kind", &self.kind)
            .field("executor", &"[ATOMIC SEC FUND JOB EXECUTOR]")
            .field("pending", &"[BOUNDED ONE-USE REQUESTS]")
            .field("maximum_pending", &SEC_FUND_JOB_MAXIMUM_PENDING)
            .finish()
    }
}

struct SecFundPendingExecution<'a> {
    pending: &'a Mutex<BTreeMap<SourceIdentifier, Option<SecAdmittedFundProductRequest>>>,
    identity: SourceIdentifier,
    admitted: Option<SecAdmittedFundProductRequest>,
}

impl SecFundPendingExecution<'_> {
    fn take(&mut self) -> Option<SecAdmittedFundProductRequest> {
        self.admitted.take()
    }
}

impl Drop for SecFundPendingExecution<'_> {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(&self.identity);
        }
    }
}

struct SecFundTerminalCommitAuthority {
    execution: SecFundJobExecutionCoordinate,
    context: JobRunContext,
    expected: market_squawk_jobs::JobEventSequence,
    permit: Mutex<Option<JobTerminalPublicationPermit>>,
}

impl SecFundTerminalCommitAuthority {
    fn seal(&self) -> Result<market_squawk_jobs::JobPublishedPermit, JobRunError> {
        self.permit
            .lock()
            .map_err(|_error| JobRunError::Recovery)?
            .take()
            .ok_or(JobRunError::Recovery)
            .map(JobTerminalPublicationPermit::seal)
    }
}

impl IngestPrecommitAuthority for SecFundTerminalCommitAuthority {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        let mut permit = self
            .permit
            .lock()
            .map_err(|_error| IngestError::PublicationAuthorityRevoked)?;
        if permit.is_some() {
            return Err(IngestError::PublicationAuthorityRevoked);
        }
        *permit = Some(
            self.context
                .claim_terminal_publication(self.expected)
                .map_err(|error| match error {
                    JobRunError::Cancelled => IngestError::Cancelled,
                    JobRunError::Failed(_) | JobRunError::Recovery => {
                        IngestError::PublicationAuthorityRevoked
                    }
                })?,
        );
        Ok(())
    }
}

impl SecFundJobCommitAuthority for SecFundTerminalCommitAuthority {
    fn execution_coordinate(&self) -> SecFundJobExecutionCoordinate {
        self.execution
    }
}

impl fmt::Debug for SecFundTerminalCommitAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecFundTerminalCommitAuthority")
            .field("execution", &self.execution)
            .field("permit", &"[ONE-SHOT TERMINAL PUBLICATION PERMIT]")
            .finish()
    }
}

/// Closed execution failures; provider payloads and credential material cannot escape the runner.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SecFundJobExecutionError {
    #[error("SEC fund source setup is required")]
    SetupRequired,
    #[error("activated SEC fund runtime is unavailable")]
    Unavailable,
    #[error("SEC fund job was cancelled")]
    Cancelled,
    #[error("SEC fund job deadline was exceeded")]
    DeadlineExceeded,
    #[error("SEC fund publication evidence is invalid")]
    InvalidPublication,
    #[error("SEC fund job authority is unavailable")]
    AuthorityUnavailable,
}

/// Bounded admission/configuration failure before common durable job creation.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SecFundJobRunnerError {
    #[error("SEC fund job request is invalid")]
    InvalidRequest,
    #[error("SEC fund job configuration is invalid")]
    InvalidConfiguration,
    #[error("SEC fund job request is already pending or running")]
    Conflict,
    #[error("SEC fund job pending capacity is exhausted")]
    Capacity,
    #[error("SEC fund job admission authority is unavailable")]
    Unavailable,
}

impl From<SecFundProductBoundaryError> for SecFundJobRunnerError {
    fn from(error: SecFundProductBoundaryError) -> Self {
        match error {
            SecFundProductBoundaryError::InvalidRequest => Self::InvalidRequest,
            SecFundProductBoundaryError::InvalidConfiguration
            | SecFundProductBoundaryError::DeadlineUnavailable
            | SecFundProductBoundaryError::PublicationMismatch => Self::InvalidConfiguration,
            SecFundProductBoundaryError::ResourceExhausted => Self::Capacity,
        }
    }
}

fn validate_snapshot(
    snapshot: &JobSnapshot,
    kind: &SourceIdentifier,
    input_authority: &SourceIdentifier,
    result_authority: &SourceIdentifier,
    result_authority_digest: EvidenceDigest,
) -> Result<(), JobRunError> {
    let spec = snapshot.spec();
    let expected_input_identity = SourceIdentifier::try_from(format!(
        "sec-fund-publication-{}",
        encode_hex(spec.input().digest().bytes())
    ))
    .map_err(|_error| JobRunError::Recovery)?;
    if spec.kind() != kind
        || spec.input().authority() != input_authority
        || spec.input().identity() != &expected_input_identity
        || spec.authority().authority() != result_authority
        || spec.authority().identity() != result_authority
        || spec.authority().digest() != result_authority_digest
        || spec.input().digest().algorithm() != DigestAlgorithm::Sha256
        || spec.input().digest().bytes().iter().all(|byte| *byte == 0)
    {
        Err(JobRunError::Recovery)
    } else {
        Ok(())
    }
}

fn execution_coordinate(context: &JobRunContext) -> SecFundJobExecutionCoordinate {
    snapshot_execution_coordinate(context.snapshot())
}

fn snapshot_execution_coordinate(snapshot: &JobSnapshot) -> SecFundJobExecutionCoordinate {
    SecFundJobExecutionCoordinate {
        job_id: snapshot.id(),
        generation: snapshot.generation(),
        admitted_request_digest: snapshot.spec().input().digest(),
    }
}

fn validate_projection_admission(
    factory: &SecFundProductRequestFactory,
    execution: SecFundJobExecutionCoordinate,
    publication: &SecFundPublicationProjection,
) -> Result<(), JobRunError> {
    let coordinate = publication.coordinate();
    let request = SecFundProductRequest::try_new(
        coordinate.family(),
        coordinate.year(),
        coordinate.quarter(),
        coordinate.accession().as_str().to_owned(),
        coordinate
            .fund_id()
            .map(|fund_id| fund_id.as_str().to_owned()),
    )
    .map_err(|_error| JobRunError::Recovery)?;
    let admitted = factory
        .admit(request)
        .map_err(|_error| JobRunError::Recovery)?;
    if admitted.admission_digest() != execution.admitted_request_digest {
        return Err(JobRunError::Recovery);
    }
    Ok(())
}

fn result_digest(
    execution: SecFundJobExecutionCoordinate,
    publication: &SecFundPublicationProjection,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(SEC_FUND_JOB_RESULT_DIGEST_DOMAIN);
    digest.update(execution.job_id.as_uuid().as_bytes());
    digest.update(execution.generation.get().to_be_bytes());
    digest.update(execution.admitted_request_digest.bytes());
    let coordinate = publication.coordinate();
    hash_text(&mut digest, coordinate.family().as_str());
    digest.update(coordinate.year().to_be_bytes());
    digest.update(coordinate.quarter().to_be_bytes());
    hash_text(&mut digest, coordinate.accession().as_str());
    hash_optional_text(
        &mut digest,
        coordinate.fund_id().map(SourceIdentifier::as_str),
    );
    let manifest = publication.manifest();
    hash_text(&mut digest, manifest.dataset_id().as_str());
    digest.update(manifest.manifest_version().to_be_bytes());
    hash_text(&mut digest, manifest.schema().name());
    digest.update(manifest.schema().version().get().to_be_bytes());
    digest.update(manifest.schema().fingerprint());
    digest.update(manifest.content_hash().bytes());
    digest.update(publication.binding_digest().bytes());
    digest.update(publication.preparation_digest().bytes());
    digest.update(publication.fund_instrument_id().as_uuid().as_bytes());
    digest.update(publication.generation_row_count().to_be_bytes());
    digest.update(publication.generation_total_bytes().to_be_bytes());
    digest.update((publication.generation_object_count() as u64).to_be_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn map_boundary_error(error: SecFundProductBoundaryError) -> JobRunError {
    match error {
        SecFundProductBoundaryError::InvalidRequest => failed("sec-fund-job-input-rejected", false),
        SecFundProductBoundaryError::InvalidConfiguration => {
            failed("sec-fund-job-configuration-invalid", false)
        }
        SecFundProductBoundaryError::DeadlineUnavailable => {
            failed("sec-fund-job-deadline-unavailable", true)
        }
        SecFundProductBoundaryError::PublicationMismatch => {
            failed("sec-fund-job-publication-invalid", false)
        }
        SecFundProductBoundaryError::ResourceExhausted => {
            failed("sec-fund-job-resource-exhausted", false)
        }
    }
}

fn map_execution_error(error: SecFundJobExecutionError) -> JobRunError {
    match error {
        SecFundJobExecutionError::SetupRequired => failed("sec-fund-setup-required", false),
        SecFundJobExecutionError::Unavailable => failed("sec-fund-runtime-unavailable", true),
        SecFundJobExecutionError::Cancelled => JobRunError::Cancelled,
        SecFundJobExecutionError::DeadlineExceeded => {
            failed("sec-fund-job-deadline-exceeded", true)
        }
        SecFundJobExecutionError::InvalidPublication => {
            failed("sec-fund-job-publication-invalid", false)
        }
        SecFundJobExecutionError::AuthorityUnavailable => {
            failed("sec-fund-job-authority-unavailable", true)
        }
    }
}

fn map_live_execution_error(error: SecLiveFundApplicationError) -> SecFundJobExecutionError {
    match error {
        SecLiveFundApplicationError::DeadlineExceeded
        | SecLiveFundApplicationError::Precommit(IngestError::DeadlineExceeded) => {
            SecFundJobExecutionError::DeadlineExceeded
        }
        SecLiveFundApplicationError::Client(
            market_squawk_adapter_sec::SecClientError::Cancelled,
        )
        | SecLiveFundApplicationError::Preparation(
            market_squawk_adapter_sec::SecBulkError::Cancelled
            | market_squawk_adapter_sec::SecBulkError::Client(
                market_squawk_adapter_sec::SecClientError::Cancelled,
            ),
        )
        | SecLiveFundApplicationError::Precommit(IngestError::Cancelled)
        | SecLiveFundApplicationError::Publication(
            super::ingest::SecFundApplicationError::Ingest(IngestError::Cancelled),
        ) => SecFundJobExecutionError::Cancelled,
        SecLiveFundApplicationError::RuntimeUnavailable
        | SecLiveFundApplicationError::AuthorityMismatch
        | SecLiveFundApplicationError::ScopedRightsUnavailable
        | SecLiveFundApplicationError::Runtime(_)
        | SecLiveFundApplicationError::ExtractionAuthority(_)
        | SecLiveFundApplicationError::Rights(_)
        | SecLiveFundApplicationError::TrustedTimeUnavailable
        | SecLiveFundApplicationError::JobCatalog(_) => SecFundJobExecutionError::Unavailable,
        SecLiveFundApplicationError::RequestMismatch
        | SecLiveFundApplicationError::CapturedGraphMismatch
        | SecLiveFundApplicationError::PreparedGraphMismatch
        | SecLiveFundApplicationError::PublicationReceiptMismatch
        | SecLiveFundApplicationError::Precommit(_)
        | SecLiveFundApplicationError::Client(_)
        | SecLiveFundApplicationError::Preparation(_)
        | SecLiveFundApplicationError::Publication(_) => {
            SecFundJobExecutionError::InvalidPublication
        }
    }
}

fn recovery_execution_failure(error: SecFundJobExecutionError) -> JobRecoveryDisposition {
    match map_execution_error(error) {
        JobRunError::Failed(failure) => JobRecoveryDisposition::Fail(failure),
        JobRunError::Cancelled => recovery_failure("sec-fund-job-recovery-cancelled", true),
        JobRunError::Recovery => recovery_failure("sec-fund-job-recovery-invalid", false),
    }
}

fn recovery_failure(diagnostic: &str, retryable: bool) -> JobRecoveryDisposition {
    match job_failure(diagnostic, retryable) {
        Some(failure) => JobRecoveryDisposition::Fail(failure),
        None => JobRecoveryDisposition::MarkInterrupted,
    }
}

fn failed(diagnostic: &str, retryable: bool) -> JobRunError {
    job_failure(diagnostic, retryable)
        .map(JobRunError::Failed)
        .unwrap_or(JobRunError::Recovery)
}

fn job_failure(diagnostic: &str, retryable: bool) -> Option<JobFailure> {
    Some(JobFailure::new(
        SourceIdentifier::try_from("sec-fund-publication").ok()?,
        SourceIdentifier::try_from(diagnostic).ok()?,
        retryable,
    ))
}

fn system_timestamp() -> Result<Timestamp, JobRunError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| failed("sec-fund-job-time-unavailable", true))?;
    let nanos = duration.as_nanos();
    let nanos =
        i64::try_from(nanos).map_err(|_error| failed("sec-fund-job-time-unavailable", true))?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn namespace_digest(namespace: &str) -> EvidenceDigest {
    EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(namespace.as_bytes()).into(),
    )
}

fn identifier(
    value: impl TryInto<SourceIdentifier>,
) -> Result<SourceIdentifier, SecFundJobRunnerError> {
    value
        .try_into()
        .map_err(|_error| SecFundJobRunnerError::InvalidConfiguration)
}

fn hash_optional_text(digest: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            digest.update([1]);
            hash_text(digest, value);
        }
        None => digest.update([0]),
    }
}

fn hash_text(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

fn encode_hex(bytes: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}
