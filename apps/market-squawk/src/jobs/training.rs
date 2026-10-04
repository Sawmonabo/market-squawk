//! Governed contained-worker adapter for Rust-owned model admission.

mod prepared;
pub(crate) use prepared::PreparedProductTraining;
use prepared::TrainingAdmission;

use std::{
    collections::BTreeMap,
    fmt,
    io::Read as _,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    AdmittedJobInput, AdmittedProcessProgram, ContainedProcessError, ContainedProcessLimits,
    ContainedProcessRequest, ContainedProcessSupervisor, ContainedStdoutFrameLimits,
    JobAttemptLimit, JobAuthoritySnapshot, JobCompletion, JobFailure, JobProgress,
    JobRecoveryDisposition, JobResultReference, JobRunContext, JobRunError, JobRunner,
    JobRunnerEvent, ProcessProgramError, contained_stdout_frame_channel,
};
use market_squawk_modeling::{
    MAX_BUNDLE_AUTHORITY_BYTES, MAX_TRAINING_WORKER_EVENT_BYTES, MAX_TRAINING_WORKER_STDERR_BYTES,
    MAX_TRAINING_WORKER_STREAM_BYTES, TrainingWorkerEvent, TrainingWorkerPhase,
    TrainingWorkerProtocolError, TrainingWorkerProtocolSession, TrainingWorkerStderrEvidence,
};
use market_squawk_platform::{BoundedInput, InputFileError, LocalPaths, UserAuthorizedInputRoot};
use market_squawk_runtime::{ClaimedInput, InputStagingError};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::application::{
    job::JobAdmission,
    model::runtime::{ProductionModelRuntime, ProductionModelRuntimeError},
};

const KIND: &str = "model.training.v1";
const INPUT_AUTHORITY: &str = "model.training-input.v1";
const RESULT_AUTHORITY: &str = "model.runtime-admission.v1";
const RESULT_AUTHORITY_IDENTITY: &str = "production-model-runtime-v1";
const WORKER_IDENTITY: &str = "market-squawk-training-worker-v1";
const MAXIMUM_CONFIG_BYTES: u64 = 256 * 1024;
const MAXIMUM_ADMISSION_REQUEST_BYTES: u64 = 8 * 1024 * 1024;
const MAXIMUM_PENDING: usize = 4_096;

/// Immutable, bounded local inputs for one governed training job.
pub struct GovernedTrainingInput {
    config: TrainingInput,
    authority: TrainingInput,
    config_sha256: EvidenceDigest,
    authority_sha256: EvidenceDigest,
    authority_bytes: Box<[u8]>,
}

impl GovernedTrainingInput {
    /// Admits exact user-authorized config and independent authority bytes.
    pub fn try_new(
        paths: &LocalPaths,
        config_path: impl AsRef<Path>,
        authority_path: impl AsRef<Path>,
    ) -> Result<Self, TrainingJobRunnerError> {
        let config_path = exact_absolute(config_path.as_ref())?;
        let authority_path = exact_absolute(authority_path.as_ref())?;
        let config = read_input(&config_path, MAXIMUM_CONFIG_BYTES, None)?;
        verify_config_root(config.as_bytes(), paths.root())?;
        let authority = read_input(
            &authority_path,
            u64::try_from(MAX_BUNDLE_AUTHORITY_BYTES)
                .map_err(|_| TrainingJobRunnerError::InvalidInput)?,
            Some(paths.root()),
        )?;
        Ok(Self {
            config: TrainingInput::UserAuthorized(Arc::from(config_path)),
            authority: TrainingInput::UserAuthorized(Arc::from(authority_path)),
            config_sha256: config.digest(),
            authority_sha256: authority.digest(),
            authority_bytes: authority.into_bytes(),
        })
    }

    /// Admits two exact, one-shot native-streamed inputs without accepting ambient paths.
    pub fn try_from_staged(
        paths: &LocalPaths,
        config: ClaimedInput,
        authority: ClaimedInput,
    ) -> Result<Self, TrainingJobRunnerError> {
        let config_bytes = config.read_verified(MAXIMUM_CONFIG_BYTES)?;
        verify_config_root(&config_bytes, paths.root())?;
        let authority_bytes = authority.read_verified(
            u64::try_from(MAX_BUNDLE_AUTHORITY_BYTES)
                .map_err(|_| TrainingJobRunnerError::InvalidInput)?,
        )?;
        Ok(Self {
            config_sha256: config.ticket().digest(),
            authority_sha256: authority.ticket().digest(),
            config: TrainingInput::Staged(Box::new(config)),
            authority: TrainingInput::Staged(Box::new(authority)),
            authority_bytes,
        })
    }

    fn evidence_digest(&self) -> EvidenceDigest {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/governed-training-input/v1\0");
        digest.update(self.config_sha256.bytes());
        digest.update(self.authority_sha256.bytes());
        EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
    }

    fn revalidate(&self, paths: &LocalPaths) -> Result<(), TrainingJobRunnerError> {
        let config = self.config.read(MAXIMUM_CONFIG_BYTES, None)?;
        verify_config_root(&config, paths.root())?;
        let authority = self.authority.read(
            u64::try_from(MAX_BUNDLE_AUTHORITY_BYTES)
                .map_err(|_| TrainingJobRunnerError::InvalidInput)?,
            Some(paths.root()),
        )?;
        if sha256_evidence(&config) != self.config_sha256
            || sha256_evidence(&authority) != self.authority_sha256
            || authority.as_ref() != self.authority_bytes.as_ref()
        {
            return Err(TrainingJobRunnerError::InputChanged);
        }
        Ok(())
    }
}

enum TrainingInput {
    UserAuthorized(Arc<Path>),
    Staged(Box<ClaimedInput>),
}

impl TrainingInput {
    fn path(&self) -> &Path {
        match self {
            Self::UserAuthorized(path) => path,
            Self::Staged(input) => input.display_path(),
        }
    }

    fn read(
        &self,
        maximum: u64,
        disjoint_from: Option<&Path>,
    ) -> Result<Box<[u8]>, TrainingJobRunnerError> {
        match self {
            Self::UserAuthorized(path) => {
                read_input(path, maximum, disjoint_from).map(BoundedInput::into_bytes)
            }
            Self::Staged(input) => input.read_verified(maximum).map_err(Into::into),
        }
    }
}

impl fmt::Debug for GovernedTrainingInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GovernedTrainingInput")
            .field("config", &"[VERIFIED BOUNDED INPUT]")
            .field("authority", &"[VERIFIED INDEPENDENT AUTHORITY]")
            .finish()
    }
}

/// One bounded runner whose only terminal publication is durable Rust model admission.
pub struct TrainingJobRunner {
    paths: LocalPaths,
    runtime: Arc<ProductionModelRuntime>,
    research: Arc<crate::ResearchService>,
    program: Mutex<Option<(AdmittedProcessProgram, bool)>>,
    kind: SourceIdentifier,
    input_authority: SourceIdentifier,
    result_authority: SourceIdentifier,
    result_authority_identity: SourceIdentifier,
    result_authority_digest: EvidenceDigest,
    pending: Mutex<BTreeMap<SourceIdentifier, TrainingAdmission>>,
    maximum_pending: usize,
    process_deadline: Duration,
}

impl fmt::Debug for TrainingJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrainingJobRunner")
            .field("paths", &"[PREPARED LOCAL PATHS]")
            .field("runtime", &"[PRODUCTION MODEL RUNTIME]")
            .field("program", &self.program)
            .field("kind", &self.kind)
            .field("pending", &"[BOUNDED IMMUTABLE TRAINING INPUTS]")
            .field("maximum_pending", &self.maximum_pending)
            .field("process_deadline", &self.process_deadline)
            .finish()
    }
}

impl TrainingJobRunner {
    /// Retains runtime and limits; signed launcher admission belongs to the owning training job.
    pub fn try_new(
        paths: &LocalPaths,
        runtime: Arc<ProductionModelRuntime>,
        research: Arc<crate::ResearchService>,
        maximum_pending: usize,
        process_deadline: Duration,
    ) -> Result<Self, TrainingJobRunnerError> {
        if maximum_pending == 0
            || maximum_pending > MAXIMUM_PENDING
            || process_deadline.is_zero()
            || process_deadline > Duration::from_secs(24 * 60 * 60)
        {
            return Err(TrainingJobRunnerError::InvalidLimits);
        }
        Ok(Self {
            paths: paths.clone(),
            runtime,
            research,
            program: Mutex::new(None),
            kind: identifier(KIND)?,
            input_authority: identifier(INPUT_AUTHORITY)?,
            result_authority: identifier(RESULT_AUTHORITY)?,
            result_authority_identity: identifier(RESULT_AUTHORITY_IDENTITY)?,
            result_authority_digest: namespace_digest(RESULT_AUTHORITY),
            pending: Mutex::new(BTreeMap::new()),
            maximum_pending,
            process_deadline,
        })
    }

    async fn admitted_program(
        &self,
        deadline: Instant,
        cancellation: &tokio_util::sync::CancellationToken,
        input: &TrainingAdmission,
    ) -> Result<(AdmittedProcessProgram, bool), TrainingJobRunnerError> {
        self.runtime.ensure_execution_live(deadline, cancellation)?;
        let cached = self
            .program
            .lock()
            .map_err(|_| TrainingJobRunnerError::Unavailable)?
            .clone();
        if let Some(program) = &cached {
            if matches!(input, TrainingAdmission::Governed(_)) {
                return Ok(program.clone());
            }
        }
        let runtime = Arc::clone(&self.runtime);
        let environment = self
            .research
            .run_owned_research_io_joined(deadline, cancellation, move |worker_cancellation| {
                runtime.training_environment(deadline, &worker_cancellation)
            })
            .await
            .map_err(|_| {
                crate::application::model::runtime::check_admission_control(deadline, cancellation)
                    .err()
                    .map(TrainingJobRunnerError::Runtime)
                    .unwrap_or(TrainingJobRunnerError::Unavailable)
            })??;
        input.require_environment(&environment)?;
        self.runtime.ensure_execution_live(deadline, cancellation)?;
        if let Some(program) = cached {
            return Ok(program);
        }
        let mut program = self
            .program
            .lock()
            .map_err(|_| TrainingJobRunnerError::Unavailable)?;
        if let Some(program) = program.as_ref() {
            return Ok(program.clone());
        }
        let admitted = match &environment {
            market_squawk_modeling::ConfiguredTrainingEnvironment::SourceDevelopment(source) => (
                AdmittedProcessProgram::try_admit_source(
                    identifier(WORKER_IDENTITY)?,
                    source.interpreter(),
                )?,
                true,
            ),
            market_squawk_modeling::ConfiguredTrainingEnvironment::InstalledRelease(installed) => {
                let worker = installed.training_worker();
                let metadata = worker
                    .path()
                    .symlink_metadata()
                    .map_err(|_| TrainingJobRunnerError::WorkerUnavailable)?;
                if metadata.len() != worker.size_bytes() {
                    return Err(TrainingJobRunnerError::WorkerUnavailable);
                }
                (
                    AdmittedProcessProgram::try_admit(
                        identifier(WORKER_IDENTITY)?,
                        worker.path(),
                        EvidenceDigest::new(DigestAlgorithm::Sha256, worker.sha256()),
                    )?,
                    false,
                )
            }
        };
        self.runtime.ensure_execution_live(deadline, cancellation)?;
        *program = Some(admitted.clone());
        Ok(admitted)
    }

    /// Registers one immutable training input before durable job creation.
    pub fn admit(
        &self,
        input: GovernedTrainingInput,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, TrainingJobRunnerError> {
        self.admit_input(TrainingAdmission::Governed(input), captured_at)
    }

    fn admit_input(
        &self,
        input: TrainingAdmission,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, TrainingJobRunnerError> {
        let digest = input.evidence_digest();
        let identity = identifier(format!("training-input-{}", encode_hex(digest.bytes())))?;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| TrainingJobRunnerError::Unavailable)?;
        if pending.contains_key(&identity) {
            return Err(TrainingJobRunnerError::Conflict);
        }
        if pending.len() >= self.maximum_pending {
            return Err(TrainingJobRunnerError::Capacity);
        }
        pending.insert(identity.clone(), input);
        Ok(JobAdmission::new(
            self.kind.clone(),
            AdmittedJobInput::new(self.input_authority.clone(), identity, digest),
            JobAuthoritySnapshot::new(
                self.result_authority.clone(),
                self.result_authority_identity.clone(),
                self.result_authority_digest,
                captured_at,
            ),
            JobAttemptLimit::try_new(1).map_err(|_| TrainingJobRunnerError::InvalidInput)?,
        ))
    }

    /// Registers exact native-streamed training inputs before durable job creation.
    pub fn admit_staged(
        &self,
        config: ClaimedInput,
        authority: ClaimedInput,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, TrainingJobRunnerError> {
        self.admit(
            GovernedTrainingInput::try_from_staged(&self.paths, config, authority)?,
            captured_at,
        )
    }

    /// Releases pending process-local input when durable job creation did not succeed.
    pub fn revoke(&self, admission: &JobAdmission) -> Result<(), TrainingJobRunnerError> {
        if admission.kind() != &self.kind || admission.input().authority() != &self.input_authority
        {
            return Err(TrainingJobRunnerError::InvalidInput);
        }
        self.pending
            .lock()
            .map_err(|_| TrainingJobRunnerError::Unavailable)?
            .remove(admission.input().identity());
        Ok(())
    }

    fn take_input(&self, context: &JobRunContext) -> Result<TrainingAdmission, JobRunError> {
        let spec = context.snapshot().spec();
        if spec.kind() != &self.kind
            || spec.input().authority() != &self.input_authority
            || spec.authority().authority() != &self.result_authority
            || spec.authority().identity() != &self.result_authority_identity
            || spec.authority().digest() != self.result_authority_digest
        {
            return Err(JobRunError::Recovery);
        }
        let input = self
            .pending
            .lock()
            .map_err(|_| JobRunError::Recovery)?
            .remove(spec.input().identity())
            .ok_or(JobRunError::Recovery)?;
        if input.evidence_digest() != spec.input().digest() {
            return Err(JobRunError::Recovery);
        }
        Ok(input)
    }

    async fn run_owned(
        &self,
        context: &JobRunContext,
        input: &TrainingAdmission,
        staging: &mut TrainingStaging,
    ) -> Result<JobCompletion, JobRunError> {
        let deadline = Instant::now()
            .checked_add(self.process_deadline)
            .ok_or(JobRunError::Recovery)?;
        let (program, source_development) = self
            .admitted_program(deadline, context.cancellation(), input)
            .await
            .map_err(map_admission_error)?;
        input
            .revalidate(&self.paths, context.cancellation())
            .map_err(|error| {
                if context.cancellation().is_cancelled() {
                    JobRunError::Cancelled
                } else {
                    map_admission_error(error)
                }
            })?;
        let mut arguments = Vec::new();
        if source_development {
            arguments
                .extend(["-I", "-B", "-m", "market_squawk.training_driver"].map(str::to_owned));
        }
        arguments.extend(worker_arguments(context, input, staging));
        let request = ContainedProcessRequest::try_new(
            program,
            arguments,
            input.stdin(),
            MAXIMUM_CONFIG_BYTES as usize,
        )
        .map_err(map_process_error)?;
        let limits = ContainedProcessLimits::try_new(
            deadline.saturating_duration_since(Instant::now()),
            MAX_TRAINING_WORKER_STREAM_BYTES,
            MAX_TRAINING_WORKER_STDERR_BYTES,
        )
        .map_err(map_process_error)?;
        let frame_limits = ContainedStdoutFrameLimits::try_new(MAX_TRAINING_WORKER_EVENT_BYTES)
            .map_err(map_process_error)?;
        let (sink, mut frames) = contained_stdout_frame_channel(frame_limits);
        let process = ContainedProcessSupervisor.run_with_stdout_frames(
            request,
            limits,
            context.cancellation().clone(),
            sink,
        );
        tokio::pin!(process);
        let mut protocol = TrainingWorkerProtocolSession::try_new(
            &context.snapshot().id().as_uuid().to_string(),
            context.snapshot().generation().get(),
        )
        .map_err(map_protocol_error)?;
        let mut expected = context.snapshot().sequence();
        let output = loop {
            tokio::select! {
                frame = frames.recv() => {
                    if let Some(frame) = frame {
                        expected = publish_frame(context, &mut protocol, &frame, expected).await?;
                    } else {
                        break process.await.map_err(map_process_error)?;
                    }
                }
                completed = &mut process => {
                    let completed = completed.map_err(map_process_error)?;
                    while let Some(frame) = frames.recv().await {
                        expected = publish_frame(context, &mut protocol, &frame, expected).await?;
                    }
                    break completed;
                }
            }
        };
        let stderr =
            TrainingWorkerStderrEvidence::capture(output.stderr()).map_err(map_protocol_error)?;
        let candidate = protocol
            .finish(output.success())
            .map_err(map_protocol_error)?;
        input
            .revalidate(&self.paths, context.cancellation())
            .map_err(|error| {
                if context.cancellation().is_cancelled() {
                    JobRunError::Cancelled
                } else {
                    map_admission_error(error)
                }
            })?;
        if candidate.candidate_directory() != staging.candidate_directory {
            return Err(failed("training-candidate-coordinate-mismatch", false));
        }
        let request_bytes = staging
            .read_request(&self.paths)
            .map_err(map_admission_error)?;
        let request_sha256: [u8; 32] = Sha256::digest(&request_bytes).into();
        if request_sha256 != candidate.admission_request_sha256() {
            return Err(failed("training-request-digest-mismatch", false));
        }
        let mut admission = input
            .authorize(
                &self.paths,
                staging,
                &request_bytes,
                &candidate,
                &self
                    .runtime
                    .training_environment(deadline, context.cancellation())
                    .map_err(map_runtime_error)?,
            )
            .map_err(map_admission_error)?;
        admission
            .bind_training_job(context, &stderr)
            .map_err(map_runtime_error)?;
        let reference =
            result_reference(&candidate, stderr, context, self.result_authority.clone())?;
        let permit = context.claim_terminal_publication(expected)?;
        if let Err(error) = self
            .runtime
            .admit_with_cancellation(admission, context.cancellation())
        {
            if matches!(error, ProductionModelRuntimeError::PublicationUnresolved) {
                staging.publication_unresolved = true;
                permit.retain_for_reconciliation();
                return Err(JobRunError::Recovery);
            }
            return Err(map_runtime_error(error));
        }
        Ok(JobCompletion::Published(reference, permit.seal()))
    }
}

#[async_trait]
impl JobRunner for TrainingJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        if context.cancellation().is_cancelled() {
            return Err(JobRunError::Cancelled);
        }
        let input = self.take_input(&context)?;
        let mut staging =
            TrainingStaging::try_new(&self.paths, &context).map_err(map_admission_error)?;
        let result = self.run_owned(&context, &input, &mut staging).await;
        match result {
            Ok(completion) => Ok(completion),
            Err(error) => {
                if !staging.publication_unresolved {
                    staging.destroy(&self.paths).map_err(map_cleanup_error)?;
                }
                Err(error)
            }
        }
    }

    async fn recover(&self, snapshot: &market_squawk_jobs::JobSnapshot) -> JobRecoveryDisposition {
        match self.published_receipt(snapshot) {
            Ok(Some((_receipt, digest))) => match durable_result_reference(
                digest.bytes(),
                snapshot,
                self.result_authority.clone(),
            ) {
                Ok(reference) => JobRecoveryDisposition::CompleteAlreadyPublished(reference),
                Err(_) => JobRecoveryDisposition::ReconciliationRequired,
            },
            // Pending product inputs are process-owned. A restart never refits them or
            // treats staged candidate bytes as a publication; fresh preparation is required.
            Ok(None) => JobRecoveryDisposition::MarkInterrupted,
            Err(_) => JobRecoveryDisposition::ReconciliationRequired,
        }
    }
}

struct TrainingStaging {
    candidate_parent: String,
    candidate_directory: String,
    request_relative: PathBuf,
    request_display: String,
    publication_unresolved: bool,
}

impl TrainingStaging {
    fn try_new(
        paths: &LocalPaths,
        context: &JobRunContext,
    ) -> Result<Self, TrainingJobRunnerError> {
        let candidate_parent = format!(
            "models/training-{}/generation-{}",
            context.snapshot().id().as_uuid(),
            context.snapshot().generation().get()
        );
        let candidate_directory = format!("{candidate_parent}/candidate");
        let request_relative = PathBuf::from(&candidate_directory).join("admission.json");
        let artifacts = paths.artifacts()?;
        let directory = artifacts.try_clone_directory()?;
        if directory.symlink_metadata(&candidate_parent).is_ok() {
            return Err(TrainingJobRunnerError::StagingConflict);
        }
        let request_display = artifacts.root().join(&request_relative);
        Ok(Self {
            candidate_parent,
            candidate_directory,
            request_relative,
            publication_unresolved: false,
            request_display: request_display
                .to_str()
                .ok_or(TrainingJobRunnerError::InvalidInput)?
                .to_owned(),
        })
    }

    fn read_request(&self, paths: &LocalPaths) -> Result<Box<[u8]>, TrainingJobRunnerError> {
        let resolved = paths.artifacts()?.resolve(&self.request_relative)?;
        let file = resolved.open_read()?;
        let length = file
            .metadata()
            .map_err(|_| TrainingJobRunnerError::InvalidCandidate)?
            .len();
        if length == 0 || length > MAXIMUM_ADMISSION_REQUEST_BYTES {
            return Err(TrainingJobRunnerError::InvalidCandidate);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(
                usize::try_from(length).map_err(|_| TrainingJobRunnerError::Capacity)?,
            )
            .map_err(|_| TrainingJobRunnerError::Capacity)?;
        file.take(MAXIMUM_ADMISSION_REQUEST_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| TrainingJobRunnerError::InvalidCandidate)?;
        if u64::try_from(bytes.len()).ok() != Some(length) {
            return Err(TrainingJobRunnerError::InvalidCandidate);
        }
        Ok(bytes.into_boxed_slice())
    }

    fn destroy(&self, paths: &LocalPaths) -> Result<(), TrainingJobRunnerError> {
        let directory = paths.artifacts()?.try_clone_directory()?;
        match directory.symlink_metadata(&self.candidate_parent) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => directory
                .remove_dir_all(&self.candidate_parent)
                .map_err(|_| TrainingJobRunnerError::Cleanup),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Ok(_) | Err(_) => Err(TrainingJobRunnerError::Cleanup),
        }
    }
}

async fn publish_frame(
    context: &JobRunContext,
    protocol: &mut TrainingWorkerProtocolSession,
    frame: &[u8],
    expected: market_squawk_jobs::JobEventSequence,
) -> Result<market_squawk_jobs::JobEventSequence, JobRunError> {
    match protocol.accept_line(frame).map_err(map_protocol_error)? {
        TrainingWorkerEvent::Progress(progress) => {
            let event = JobProgress::try_new(
                identifier(phase_name(progress.phase())).map_err(|_| JobRunError::Recovery)?,
                progress.completed_units(),
                Some(progress.total_units()),
                context.snapshot().updated_at_timestamp(),
            )
            .map_err(|_| JobRunError::Recovery)?;
            context
                .events()
                .append(JobRunnerEvent::Progress(event))
                .await
                .map(|snapshot| snapshot.sequence())
                .map_err(|_| failed("training-progress-unavailable", true))
        }
        TrainingWorkerEvent::CandidateStaged | TrainingWorkerEvent::ErrorStaged { .. } => {
            Ok(expected)
        }
    }
}

fn worker_arguments(
    context: &JobRunContext,
    input: &TrainingAdmission,
    staging: &TrainingStaging,
) -> Vec<String> {
    let mut arguments = vec![
        "worker".to_owned(),
        "--run-id".to_owned(),
        context.snapshot().id().as_uuid().to_string(),
        "--generation".to_owned(),
        context.snapshot().generation().get().to_string(),
        "--candidate-parent".to_owned(),
        staging.candidate_parent.clone(),
        "--request".to_owned(),
        staging.request_display.clone(),
    ];
    match input {
        TrainingAdmission::Governed(input) => arguments.extend([
            "--config".to_owned(),
            input.config.path().to_string_lossy().into_owned(),
            "--authority".to_owned(),
            input.authority.path().to_string_lossy().into_owned(),
        ]),
        TrainingAdmission::Product(_) => arguments.push("--product-config-stdin".to_owned()),
    }
    arguments
}

fn verify_config_root(bytes: &[u8], root: &Path) -> Result<(), TrainingJobRunnerError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| TrainingJobRunnerError::InvalidInput)?;
    let configured = value
        .get("dataset")
        .and_then(serde_json::Value::as_object)
        .and_then(|dataset| dataset.get("root"))
        .and_then(serde_json::Value::as_str)
        .ok_or(TrainingJobRunnerError::InvalidInput)?;
    let configured = Path::new(configured)
        .canonicalize()
        .map_err(|_| TrainingJobRunnerError::InvalidInput)?;
    let expected = root
        .canonicalize()
        .map_err(|_| TrainingJobRunnerError::InvalidInput)?;
    if configured != expected {
        return Err(TrainingJobRunnerError::InvalidInput);
    }
    Ok(())
}

fn exact_absolute(path: &Path) -> Result<PathBuf, TrainingJobRunnerError> {
    if !path.is_absolute() || path.components().any(|part| part == Component::ParentDir) {
        return Err(TrainingJobRunnerError::InvalidInput);
    }
    path.canonicalize()
        .map_err(|_| TrainingJobRunnerError::InvalidInput)
}

fn read_input(
    path: &Path,
    maximum: u64,
    disjoint_from: Option<&Path>,
) -> Result<BoundedInput, TrainingJobRunnerError> {
    let parent = path.parent().ok_or(TrainingJobRunnerError::InvalidInput)?;
    let name = path
        .file_name()
        .ok_or(TrainingJobRunnerError::InvalidInput)?;
    let root = UserAuthorizedInputRoot::open(parent)?;
    if let Some(disjoint_from) = disjoint_from {
        root.ensure_disjoint_root(disjoint_from)?;
    }
    root.resolve(PathBuf::from(name))?
        .open_bounded(maximum)?
        .read_bounded()
        .map_err(Into::into)
}

fn sha256_evidence(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn phase_name(phase: TrainingWorkerPhase) -> &'static str {
    match phase {
        TrainingWorkerPhase::Validation => "validating-inputs",
        TrainingWorkerPhase::Training => "training-model",
        TrainingWorkerPhase::Evaluation => "evaluating-candidate",
        TrainingWorkerPhase::Export => "exporting-candidate",
        TrainingWorkerPhase::Complete => "candidate-staged",
        TrainingWorkerPhase::Cancelled => "training-cancelled",
        TrainingWorkerPhase::Failed => "training-failed",
    }
}

fn result_reference(
    candidate: &market_squawk_modeling::TrainingWorkerCandidate,
    stderr: TrainingWorkerStderrEvidence,
    context: &JobRunContext,
    authority: SourceIdentifier,
) -> Result<JobResultReference, JobRunError> {
    let mut evidence = Sha256::new();
    evidence.update(b"market-squawk/model-training-result/v1\0");
    evidence.update(candidate.metadata_sha256());
    evidence.update(candidate.artifact_sha256());
    evidence.update(candidate.training_run_sha256());
    evidence.update(candidate.authority_sha256());
    evidence.update(candidate.dataset_export_sha256());
    evidence.update(candidate.dataset_selection_sha256());
    evidence.update(stderr.captured_bytes().to_be_bytes());
    evidence.update(stderr.sha256());
    let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, evidence.finalize().into());
    durable_result_reference(digest.bytes(), context.snapshot(), authority)
}

fn durable_result_reference(
    result_sha256: [u8; 32],
    snapshot: &market_squawk_jobs::JobSnapshot,
    authority: SourceIdentifier,
) -> Result<JobResultReference, JobRunError> {
    let identity = identifier(format!(
        "training-result-{}-{}",
        snapshot.id().as_uuid(),
        snapshot.generation().get()
    ))
    .map_err(|_| JobRunError::Recovery)?;
    JobResultReference::try_new(
        authority,
        identity,
        EvidenceDigest::new(DigestAlgorithm::Sha256, result_sha256),
        Vec::new(),
    )
    .map_err(|_| JobRunError::Recovery)
}

impl TrainingJobRunner {
    fn published_receipt(
        &self,
        snapshot: &market_squawk_jobs::JobSnapshot,
    ) -> Result<
        Option<(
            crate::application::model::runtime::ModelAdmissionReceipt,
            market_squawk_data::Sha256Digest,
        )>,
        TrainingJobRunnerError,
    > {
        let spec = snapshot.spec();
        if spec.kind() != &self.kind
            || spec.input().authority() != &self.input_authority
            || spec.authority().authority() != &self.result_authority
            || spec.authority().identity() != &self.result_authority_identity
            || spec.authority().digest() != self.result_authority_digest
        {
            return Err(TrainingJobRunnerError::InvalidInput);
        }
        self.runtime
            .training_admission(snapshot)
            .map_err(|_| TrainingJobRunnerError::Unavailable)
    }

    pub(crate) fn completed_model_token(
        &self,
        receipt: &crate::application::model::runtime::ModelAdmissionReceipt,
    ) -> Result<uuid::Uuid, TrainingJobRunnerError> {
        self.runtime
            .training_model_token(receipt)
            .map_err(|_| TrainingJobRunnerError::InvalidCandidate)
    }

    /// Reopens the exact model generation named by the actual completed job.
    /// Callers retain this receipt and request its exact model/bundle from the existing runtime.
    pub(crate) fn resolve_completed(
        &self,
        snapshot: &market_squawk_jobs::JobSnapshot,
    ) -> Result<crate::application::model::runtime::ModelAdmissionReceipt, TrainingJobRunnerError>
    {
        if snapshot.state() != market_squawk_jobs::JobState::Completed {
            return Err(TrainingJobRunnerError::InvalidInput);
        }
        let (receipt, digest) = self
            .published_receipt(snapshot)?
            .ok_or(TrainingJobRunnerError::InvalidCandidate)?;
        let expected =
            durable_result_reference(digest.bytes(), snapshot, self.result_authority.clone())
                .map_err(|_| TrainingJobRunnerError::InvalidCandidate)?;
        if snapshot.terminal_result() != Some(&expected) {
            return Err(TrainingJobRunnerError::InvalidCandidate);
        }
        Ok(receipt)
    }
}

fn identifier(
    value: impl TryInto<SourceIdentifier>,
) -> Result<SourceIdentifier, TrainingJobRunnerError> {
    value
        .try_into()
        .map_err(|_| TrainingJobRunnerError::InvalidInput)
}

fn namespace_digest(value: &str) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn failed(diagnostic: &str, retryable: bool) -> JobRunError {
    match (
        SourceIdentifier::try_from("model-training"),
        SourceIdentifier::try_from(diagnostic),
    ) {
        (Ok(class), Ok(diagnostic)) => {
            JobRunError::Failed(JobFailure::new(class, diagnostic, retryable))
        }
        _ => JobRunError::Recovery,
    }
}

fn map_process_error(error: ContainedProcessError) -> JobRunError {
    match error {
        ContainedProcessError::Cancelled => JobRunError::Cancelled,
        ContainedProcessError::Deadline => failed("training-deadline-exceeded", true),
        ContainedProcessError::CleanupPending => failed("training-cleanup-pending", true),
        ContainedProcessError::ReaperCapacity => failed("training-cleanup-capacity", true),
        ContainedProcessError::ProgramChanged => failed("training-worker-changed", false),
        ContainedProcessError::OutputTooLarge
        | ContainedProcessError::StdoutFrameTooLarge
        | ContainedProcessError::StdoutFrameIncomplete
        | ContainedProcessError::StdoutFrameDelivery => failed("training-protocol-rejected", false),
        ContainedProcessError::InvalidRequest | ContainedProcessError::Unavailable => {
            failed("training-worker-unavailable", true)
        }
    }
}

fn map_protocol_error(_error: TrainingWorkerProtocolError) -> JobRunError {
    failed("training-protocol-rejected", false)
}

fn map_runtime_error(error: ProductionModelRuntimeError) -> JobRunError {
    match error {
        ProductionModelRuntimeError::Admission(
            market_squawk_modeling::ModelAdmissionError::Dataset(
                market_squawk_data::PythonDatasetCatalogError::Cancelled,
            ),
        ) => JobRunError::Cancelled,
        ProductionModelRuntimeError::TrainingEnvironment(
            market_squawk_modeling::TrainingEnvironmentError::Cancelled,
        ) => JobRunError::Cancelled,
        ProductionModelRuntimeError::TrainingEnvironment(
            market_squawk_modeling::TrainingEnvironmentError::DeadlineExceeded,
        )
        | ProductionModelRuntimeError::ValidationDeadline => {
            failed("training-admission-deadline-exceeded", true)
        }
        ProductionModelRuntimeError::TrainingEnvironment(_) => {
            failed("training-environment-rejected", false)
        }
        _ => failed("training-candidate-rejected", false),
    }
}

fn map_admission_error(error: TrainingJobRunnerError) -> JobRunError {
    match error {
        TrainingJobRunnerError::Runtime(error) => map_runtime_error(error),
        TrainingJobRunnerError::InputChanged => failed("training-input-changed", false),
        TrainingJobRunnerError::Capacity => failed("training-resource-exhausted", true),
        _ => failed("training-input-rejected", false),
    }
}

fn map_cleanup_error(_error: TrainingJobRunnerError) -> JobRunError {
    failed("training-candidate-cleanup-failed", false)
}

/// Governed training admission, containment, candidate, or cleanup failure.
#[derive(Debug, Error)]
pub enum TrainingJobRunnerError {
    #[error(transparent)]
    Runtime(#[from] ProductionModelRuntimeError),
    #[error("training runner limits are invalid")]
    InvalidLimits,
    #[error("training input is invalid")]
    InvalidInput,
    #[error("training input changed after admission")]
    InputChanged,
    #[error("training pending capacity is exhausted")]
    Capacity,
    #[error("training input is already pending")]
    Conflict,
    #[error("training runner is unavailable")]
    Unavailable,
    #[error("verified training worker is unavailable")]
    WorkerUnavailable,
    #[error("training staging coordinate already exists")]
    StagingConflict,
    #[error("training candidate is invalid")]
    InvalidCandidate,
    #[error("training candidate cleanup failed")]
    Cleanup,
    #[error(transparent)]
    Input(#[from] InputFileError),
    #[error("native-staged training input was rejected")]
    StagedInput(#[from] InputStagingError),
    #[error("training path authority failed")]
    Path,
    #[error("training artifact authority failed")]
    Artifact,
    #[error(transparent)]
    Program(#[from] ProcessProgramError),
}

impl From<market_squawk_platform::PathError> for TrainingJobRunnerError {
    fn from(_error: market_squawk_platform::PathError) -> Self {
        Self::Path
    }
}

impl From<market_squawk_platform::ArtifactPathError> for TrainingJobRunnerError {
    fn from(_error: market_squawk_platform::ArtifactPathError) -> Self {
        Self::Artifact
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fs::{self, File},
        io::Read as _,
        sync::Arc,
        time::Duration,
    };

    use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
    use market_squawk_jobs::{
        AdmittedProcessProgram, JobActivityClass, JobOrigin, JobRunner, JobRunnerRegistration,
        JobState, ProcessProgramError, SqliteJobRepository,
    };
    use market_squawk_platform::LocalPaths;
    use market_squawk_services::RequestId;
    use sha2::{Digest as _, Sha256};

    use super::{
        GovernedTrainingInput, INPUT_AUTHORITY, KIND, MAXIMUM_PENDING, RESULT_AUTHORITY,
        RESULT_AUTHORITY_IDENTITY, TrainingJobRunner, WORKER_IDENTITY, identifier,
        namespace_digest,
    };
    use crate::application::model::runtime::ProductionModelRuntime;
    use crate::{
        application::{
            job::{JobApplication, JobReceipt, JobView},
            model::ModelDomainService,
        },
        jobs::InstalledJobAuthority,
    };

    type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

    async fn wait_for_exact_terminal(
        application: &JobApplication<SqliteJobRepository>,
        receipt: JobReceipt,
        timeout: Duration,
    ) -> TestResult<JobView> {
        let terminal = tokio::time::timeout(timeout, async {
            loop {
                let view = application
                    .get(receipt.job_id(), receipt.generation())
                    .await?;
                if view.state().is_terminal() {
                    return Ok::<JobView, Box<dyn Error + Send + Sync>>(view);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await??;
        Ok(terminal)
    }

    #[tokio::test]
    async fn failed_contained_training_process_publishes_neither_runtime_admission_nor_model_read_image()
    -> TestResult {
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path().join("market-squawk"))?;
        let research = Arc::new(crate::ResearchService::initialize(
            &paths,
            market_squawk_data::CatalogConfig::try_new(
                paths.catalog()?.clone(),
                Duration::from_millis(250),
                market_squawk_data::CatalogLimit::new(32)?,
                market_squawk_data::CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
            )?,
            8,
            market_squawk_data::ObjectStoreConfig::try_new(
                1024 * 1024,
                32,
                Duration::from_secs(60),
            )?,
        )?);
        let runtime = Arc::new(ProductionModelRuntime::test_fixture(&paths, None)?);
        let retained_runtime = runtime.snapshot()?;
        let model = ModelDomainService::try_from_runtime_snapshot(
            runtime.snapshot()?,
            std::num::NonZeroUsize::new(8).ok_or("nonzero evaluation capacity")?,
        )?;
        let program = admitted_failing_test_program()?;
        let runner = Arc::new(TrainingJobRunner {
            paths: paths.clone(),
            runtime,
            research,
            program: std::sync::Mutex::new(Some((program, false))),
            kind: identifier(KIND)?,
            input_authority: identifier(INPUT_AUTHORITY)?,
            result_authority: identifier(RESULT_AUTHORITY)?,
            result_authority_identity: identifier(RESULT_AUTHORITY_IDENTITY)?,
            result_authority_digest: namespace_digest(RESULT_AUTHORITY),
            pending: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            maximum_pending: MAXIMUM_PENDING,
            process_deadline: Duration::from_secs(5),
        });
        let input_root = temporary.path().join("training-inputs");
        fs::create_dir(&input_root)?;
        let config_path = input_root.join("training-proof-config.json");
        let authority_path = input_root.join("training-proof-authority.json");
        fs::write(
            &config_path,
            serde_json::to_vec(&serde_json::json!({
                "dataset": {"root": paths.root()}
            }))?,
        )?;
        fs::write(&authority_path, b"contained-failure-proof-authority")?;
        let input = GovernedTrainingInput::try_new(&paths, &config_path, &authority_path)?;
        let admission = runner.admit(input, Timestamp::from_unix_nanos(100))?;
        let runner_trait: Arc<dyn JobRunner> = runner;
        let jobs = InstalledJobAuthority::open(
            &paths,
            vec![JobRunnerRegistration::new(
                runner_trait,
                JobActivityClass::Mutation,
            )],
            Timestamp::from_unix_nanos(100),
        )
        .await?;
        let application = JobApplication::new(jobs.repository(), jobs.authority());
        let receipt = application
            .start(
                admission,
                JobOrigin::new(
                    SourceIdentifier::try_from("default-workspace")?,
                    SourceIdentifier::try_from("training-proof-client")?,
                ),
                RequestId::try_string("contained-training-failure")?,
                Timestamp::from_unix_nanos(100),
            )
            .await?;
        let terminal =
            wait_for_exact_terminal(&application, receipt, Duration::from_secs(10)).await?;

        assert_eq!(terminal.state(), JobState::Failed);
        assert!(terminal.result().is_none());
        assert_eq!(model.admitted_generation_count(), 0);
        assert!(
            !paths
                .artifacts()?
                .root()
                .join(format!(
                    "models/training-{}/generation-1",
                    receipt.job_id().as_uuid()
                ))
                .exists()
        );

        jobs.shutdown_authority(Timestamp::from_unix_nanos(200), Duration::from_secs(1))
            .await?;
        jobs.shutdown_repository().await?;
        drop(application);
        drop(jobs);
        assert!(retained_runtime.is_empty());
        Ok(())
    }

    fn admitted_failing_test_program() -> Result<AdmittedProcessProgram, ProcessProgramError> {
        let executable = failing_test_program()?;
        let mut file = File::open(&executable).map_err(|_| ProcessProgramError::Unavailable)?;
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|_| ProcessProgramError::Unavailable)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        AdmittedProcessProgram::try_admit(
            identifier(WORKER_IDENTITY).map_err(|_| ProcessProgramError::InvalidProgram)?,
            executable,
            EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
        )
    }

    #[cfg(unix)]
    fn failing_test_program() -> Result<std::path::PathBuf, ProcessProgramError> {
        for candidate in ["/usr/bin/false", "/bin/false"] {
            if let Ok(path) = std::path::Path::new(candidate).canonicalize() {
                return Ok(path);
            }
        }
        Err(ProcessProgramError::Unavailable)
    }

    #[cfg(windows)]
    fn failing_test_program() -> Result<std::path::PathBuf, ProcessProgramError> {
        let system_root = std::env::var_os("SystemRoot")
            .filter(|root| !root.is_empty())
            .ok_or(ProcessProgramError::Unavailable)?;
        std::path::PathBuf::from(system_root)
            .join("System32")
            .join("where.exe")
            .canonicalize()
            .map_err(|_| ProcessProgramError::Unavailable)
    }
}
