//! Durable runner adapters for application-owned research publications.

use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use market_squawk_data::{
    DatasetBuildError, DatasetBuildPrecommitAuthority, DatasetBuildRequest, IngestError,
    IngestPrecommitAuthority,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    AdmittedJobInput, JobAttemptLimit, JobAuthoritySnapshot, JobCompletion, JobFailure,
    JobProgress, JobRecoveryDisposition, JobResultReference, JobRunContext, JobRunError, JobRunner,
    JobRunnerEvent,
};
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactRepository,
    RequestContext, ServiceDomain, ServiceError, ServiceLimits, TypedToolRequest,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    ResearchService, ResearchServiceError,
    application::{
        ApplicationDomainService, ResearchIngestCommitAuthority, ResearchIngestCoordinator,
        job::JobAdmission,
    },
};

use super::JobTerminalCommitSlot;

const INGEST_OPERATION: &str = "Research.IngestSource";
const INGEST_KIND: &str = "research.ingest-source.v1";
const INGEST_AUTHORITY: &str = "research.dataset-publication.v1";
const RESEARCH_PHASE_ONE_KIND: &str = "research.phase-one-derived-generation-job.v1";
const ANALYSIS_PHASE_ONE_FEATURE_KIND: &str =
    "analysis.phase-one-feature-derived-generation-job.v1";
const PHASE_ONE_INPUT_AUTHORITY: &str = "research.phase-one-derived-generation-request.v1";
const RESEARCH_PHASE_ONE_RESULT_AUTHORITY: &str = "research.phase-one-derived-generation.v1";
const ANALYSIS_PHASE_ONE_FEATURE_RESULT_AUTHORITY: &str =
    "analysis.phase-one-feature-derived-generation.v1";
const EXPORT_OPERATION: &str = "Research.GetHistory";
const EXPORT_KIND: &str = "research.dataset-export.v1";
const EXPORT_AUTHORITY: &str = "research.controlled-export.v1";

#[derive(Clone, Debug)]
struct PendingOperation {
    request: TypedToolRequest,
    limits: ServiceLimits,
}

struct PreparedOperation {
    pending: PendingOperation,
    expected: market_squawk_jobs::JobEventSequence,
    deadline: std::time::Instant,
}

#[derive(Debug)]
struct JobIngestCommitAuthority {
    slot: Arc<JobTerminalCommitSlot>,
}

impl IngestPrecommitAuthority for JobIngestCommitAuthority {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.slot.claim().map_err(|error| match error {
            JobRunError::Cancelled => IngestError::Cancelled,
            JobRunError::Failed(_) | JobRunError::Recovery => {
                IngestError::PublicationAuthorityRevoked
            }
        })
    }
}

impl ResearchIngestCommitAuthority for JobIngestCommitAuthority {
    fn commit_succeeded(&self) {
        self.slot.seal_domain_commit();
    }
}

#[derive(Debug)]
struct JobPhaseOneDerivedGenerationCommitAuthority {
    slot: Arc<JobTerminalCommitSlot>,
}

impl DatasetBuildPrecommitAuthority for JobPhaseOneDerivedGenerationCommitAuthority {
    fn validate_precommit(&self) -> Result<(), DatasetBuildError> {
        self.slot.claim().map_err(|error| match error {
            JobRunError::Cancelled => DatasetBuildError::Cancelled,
            JobRunError::Failed(_) | JobRunError::Recovery => {
                DatasetBuildError::PublicationAuthorityRevoked
            }
        })
    }

    fn commit_succeeded(&self) {
        self.slot.seal_domain_commit();
    }
}

pub(super) struct ApplicationOperationJobRunner {
    kind: SourceIdentifier,
    operation: &'static str,
    input_authority: SourceIdentifier,
    result_authority: SourceIdentifier,
    authority_digest: EvidenceDigest,
    domain: Arc<dyn ApplicationDomainService>,
    pending: std::sync::Mutex<BTreeMap<SourceIdentifier, PendingOperation>>,
    maximum_pending: usize,
    run_timeout: Duration,
}

impl ApplicationOperationJobRunner {
    pub(super) const fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the closed runner keeps operation, domain, authority, and resource ceilings explicit"
    )]
    pub(super) fn try_new(
        kind: &'static str,
        operation: &'static str,
        input_authority: &'static str,
        result_authority: &'static str,
        expected_domain: ServiceDomain,
        domain: Arc<dyn ApplicationDomainService>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, ResearchJobRunnerError> {
        if domain.domain() != expected_domain
            || maximum_pending == 0
            || maximum_pending > 4_096
            || run_timeout.is_zero()
            || run_timeout > Duration::from_secs(24 * 60 * 60)
        {
            return Err(ResearchJobRunnerError::InvalidLimits);
        }
        Ok(Self {
            kind: identifier(kind)?,
            operation,
            input_authority: identifier(input_authority)?,
            result_authority: identifier(result_authority)?,
            authority_digest: namespace_digest(result_authority),
            domain,
            pending: std::sync::Mutex::new(BTreeMap::new()),
            maximum_pending,
            run_timeout,
        })
    }

    pub(super) fn admit(
        &self,
        request: TypedToolRequest,
        limits: ServiceLimits,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, ResearchJobRunnerError> {
        if request.name() != self.operation {
            return Err(ResearchJobRunnerError::InvalidRequest);
        }
        let encoded = serde_json::to_vec(&serde_json::json!({
            "operation": request.name(),
            "version": request.version(),
            "arguments": request.arguments(),
        }))
        .map_err(|_error| ResearchJobRunnerError::InvalidRequest)?;
        let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&encoded).into());
        let identity = identifier(format!(
            "{}-{}",
            self.kind.as_str(),
            encode_hex(digest.bytes())
        ))?;
        let pending_operation = PendingOperation { request, limits };
        let mut pending = self
            .pending
            .lock()
            .map_err(|_error| ResearchJobRunnerError::Unavailable)?;
        match pending.get(&identity) {
            Some(_) => return Err(ResearchJobRunnerError::Conflict),
            None if pending.len() >= self.maximum_pending => {
                return Err(ResearchJobRunnerError::Capacity);
            }
            None => {
                pending.insert(identity.clone(), pending_operation);
            }
        }
        Ok(JobAdmission::new(
            self.kind.clone(),
            AdmittedJobInput::new(self.input_authority.clone(), identity, digest),
            JobAuthoritySnapshot::new(
                self.result_authority.clone(),
                identifier(self.result_authority.as_str())?,
                self.authority_digest,
                captured_at,
            ),
            JobAttemptLimit::try_new(1).map_err(|_error| ResearchJobRunnerError::InvalidRequest)?,
        ))
    }

    pub(super) fn revoke(&self, admission: &JobAdmission) -> Result<(), ResearchJobRunnerError> {
        if admission.kind() != &self.kind || admission.input().authority() != &self.input_authority
        {
            return Err(ResearchJobRunnerError::InvalidRequest);
        }
        self.pending
            .lock()
            .map_err(|_error| ResearchJobRunnerError::Unavailable)?
            .remove(admission.input().identity());
        Ok(())
    }

    async fn prepare_run(&self, context: &JobRunContext) -> Result<PreparedOperation, JobRunError> {
        let spec = context.snapshot().spec();
        if spec.kind() != &self.kind
            || spec.input().authority() != &self.input_authority
            || spec.authority().authority() != &self.result_authority
            || spec.authority().digest() != self.authority_digest
        {
            return Err(JobRunError::Recovery);
        }
        let pending = self
            .pending
            .lock()
            .map_err(|_error| JobRunError::Recovery)?
            .remove(spec.input().identity())
            .ok_or(JobRunError::Recovery)?;
        let encoded = serde_json::to_vec(&serde_json::json!({
            "operation": pending.request.name(),
            "version": pending.request.version(),
            "arguments": pending.request.arguments(),
        }))
        .map_err(|_error| JobRunError::Recovery)?;
        let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(encoded).into());
        if digest != spec.input().digest() || context.cancellation().is_cancelled() {
            return Err(if context.cancellation().is_cancelled() {
                JobRunError::Cancelled
            } else {
                JobRunError::Recovery
            });
        }
        let progress = JobProgress::try_new(
            identifier("validating-inputs").map_err(|_error| JobRunError::Recovery)?,
            0,
            None,
            context.snapshot().updated_at_timestamp(),
        )
        .map_err(|_error| JobRunError::Recovery)?;
        let progressed = context
            .events()
            .append(JobRunnerEvent::Progress(progress))
            .await
            .map_err(|_error| failed("job-progress-unavailable", true))?;
        let deadline = std::time::Instant::now()
            .checked_add(self.run_timeout)
            .ok_or(JobRunError::Recovery)?;
        Ok(PreparedOperation {
            pending,
            expected: progressed.sequence(),
            deadline,
        })
    }

    pub(super) async fn run_read_operation(
        &self,
        context: JobRunContext,
        artifacts: &Arc<dyn ArtifactRepository>,
    ) -> Result<JobCompletion, JobRunError> {
        let prepared = self.prepare_run(&context).await?;
        let spec = context.snapshot().spec();
        let request_context = RequestContext::new(
            spec.request_id().clone(),
            context.cancellation().clone(),
            prepared.deadline,
            prepared.pending.limits,
        );
        let result = self
            .domain
            .call(prepared.pending.request, request_context)
            .await
            .map_err(map_service_error)?;
        if context.cancellation().is_cancelled() {
            return Err(JobRunError::Cancelled);
        }
        let encoded = serde_json::to_vec(&result.clone().into_envelope())
            .map_err(|_error| failed("result-encoding-invalid", false))?;
        let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&encoded).into());
        let permit = context.claim_terminal_publication(prepared.expected)?;
        let publication = ArtifactPublication::try_json(encoded).map_err(map_artifact_error)?;
        let artifact = artifacts
            .publish(
                publication,
                ArtifactPublicationContext::new(context.cancellation().clone(), prepared.deadline),
            )
            .await
            .map_err(map_artifact_error)?;
        let published = permit.seal();
        let result_identity = identifier(format!(
            "{}-result-{}",
            self.kind.as_str(),
            encode_hex(digest.bytes())
        ))
        .map_err(|_error| JobRunError::Recovery)?;
        let reference = JobResultReference::try_new(
            self.result_authority.clone(),
            result_identity,
            digest,
            vec![artifact],
        )
        .map_err(|_error| JobRunError::Recovery)?;
        Ok(JobCompletion::Published(reference, published))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PhaseOneDerivedGenerationKind {
    ResearchDataset,
    AnalysisFeature,
}

impl PhaseOneDerivedGenerationKind {
    const fn kind(self) -> &'static str {
        match self {
            Self::ResearchDataset => RESEARCH_PHASE_ONE_KIND,
            Self::AnalysisFeature => ANALYSIS_PHASE_ONE_FEATURE_KIND,
        }
    }

    const fn result_authority(self) -> &'static str {
        match self {
            Self::ResearchDataset => RESEARCH_PHASE_ONE_RESULT_AUTHORITY,
            Self::AnalysisFeature => ANALYSIS_PHASE_ONE_FEATURE_RESULT_AUTHORITY,
        }
    }
}

/// Durable phase-one derived-generation runner over the sole analytical catalog authority.
///
/// Successful jobs publish immutable analytical generations and controlled result artifacts. They
/// do not mint product receipts, issuer authority, model authority, or execution authority.
pub struct PhaseOneDerivedGenerationJobRunner {
    generation_kind: PhaseOneDerivedGenerationKind,
    kind: SourceIdentifier,
    input_authority: SourceIdentifier,
    result_authority: SourceIdentifier,
    authority_digest: EvidenceDigest,
    research: Arc<ResearchService>,
    artifacts: Arc<dyn ArtifactRepository>,
    pending: std::sync::Mutex<BTreeMap<SourceIdentifier, DatasetBuildRequest>>,
    maximum_pending: usize,
    run_timeout: Duration,
}

impl PhaseOneDerivedGenerationJobRunner {
    /// Creates the phase-one runner currently reached through `Research.StartDatasetBuild`.
    pub fn try_new_research_dataset(
        research: Arc<ResearchService>,
        artifacts: Arc<dyn ArtifactRepository>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, ResearchJobRunnerError> {
        Self::try_new(
            PhaseOneDerivedGenerationKind::ResearchDataset,
            research,
            artifacts,
            maximum_pending,
            run_timeout,
        )
    }

    /// Creates the phase-one runner currently reached through `Analysis.StartFeatureDatasetBuild`.
    pub fn try_new_analysis_feature(
        research: Arc<ResearchService>,
        artifacts: Arc<dyn ArtifactRepository>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, ResearchJobRunnerError> {
        Self::try_new(
            PhaseOneDerivedGenerationKind::AnalysisFeature,
            research,
            artifacts,
            maximum_pending,
            run_timeout,
        )
    }

    fn try_new(
        generation_kind: PhaseOneDerivedGenerationKind,
        research: Arc<ResearchService>,
        artifacts: Arc<dyn ArtifactRepository>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, ResearchJobRunnerError> {
        if maximum_pending == 0
            || maximum_pending > 4_096
            || run_timeout.is_zero()
            || run_timeout > Duration::from_secs(24 * 60 * 60)
        {
            return Err(ResearchJobRunnerError::InvalidLimits);
        }
        let result_authority = identifier(generation_kind.result_authority())?;
        Ok(Self {
            generation_kind,
            kind: identifier(generation_kind.kind())?,
            input_authority: identifier(PHASE_ONE_INPUT_AUTHORITY)?,
            authority_digest: namespace_digest(generation_kind.result_authority()),
            result_authority,
            research,
            artifacts,
            pending: std::sync::Mutex::new(BTreeMap::new()),
            maximum_pending,
            run_timeout,
        })
    }

    /// Registers one fully validated, path-free phase-one generation request.
    pub fn admit(
        &self,
        request: DatasetBuildRequest,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, ResearchJobRunnerError> {
        let digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            request.build_spec_digest().digest().bytes(),
        );
        let identity = identifier(format!(
            "phase-one-derived-generation-request-{}",
            encode_hex(digest.bytes())
        ))?;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_error| ResearchJobRunnerError::Unavailable)?;
        match pending.get(&identity) {
            Some(_) => return Err(ResearchJobRunnerError::Conflict),
            None if pending.len() >= self.maximum_pending => {
                return Err(ResearchJobRunnerError::Capacity);
            }
            None => {
                pending.insert(identity.clone(), request);
            }
        }
        Ok(JobAdmission::new(
            self.kind.clone(),
            AdmittedJobInput::new(self.input_authority.clone(), identity, digest),
            JobAuthoritySnapshot::new(
                self.result_authority.clone(),
                identifier(self.generation_kind.result_authority())?,
                self.authority_digest,
                captured_at,
            ),
            JobAttemptLimit::try_new(1).map_err(|_error| ResearchJobRunnerError::InvalidRequest)?,
        ))
    }

    /// Releases one pending phase-one request when durable job creation did not succeed.
    pub fn revoke(&self, admission: &JobAdmission) -> Result<(), ResearchJobRunnerError> {
        if admission.kind() != &self.kind || admission.input().authority() != &self.input_authority
        {
            return Err(ResearchJobRunnerError::InvalidRequest);
        }
        self.pending
            .lock()
            .map_err(|_error| ResearchJobRunnerError::Unavailable)?
            .remove(admission.input().identity());
        Ok(())
    }

    fn take_request(&self, context: &JobRunContext) -> Result<DatasetBuildRequest, JobRunError> {
        let spec = context.snapshot().spec();
        if spec.kind() != &self.kind
            || spec.input().authority() != &self.input_authority
            || spec.authority().authority() != &self.result_authority
            || spec.authority().digest() != self.authority_digest
        {
            return Err(JobRunError::Recovery);
        }
        let request = self
            .pending
            .lock()
            .map_err(|_error| JobRunError::Recovery)?
            .remove(spec.input().identity())
            .ok_or(JobRunError::Recovery)?;
        let digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            request.build_spec_digest().digest().bytes(),
        );
        if digest != spec.input().digest() {
            return Err(JobRunError::Recovery);
        }
        Ok(request)
    }
}

#[async_trait]
impl JobRunner for PhaseOneDerivedGenerationJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        if context.cancellation().is_cancelled() {
            return Err(JobRunError::Cancelled);
        }
        let request = self.take_request(&context)?;
        let progress = JobProgress::try_new(
            identifier("building-phase-one-derived-generation")
                .map_err(|_error| JobRunError::Recovery)?,
            0,
            None,
            context.snapshot().updated_at_timestamp(),
        )
        .map_err(|_error| JobRunError::Recovery)?;
        let progressed = context
            .events()
            .append(JobRunnerEvent::Progress(progress))
            .await
            .map_err(|_error| failed("job-progress-unavailable", true))?;
        let deadline = std::time::Instant::now()
            .checked_add(self.run_timeout)
            .ok_or(JobRunError::Recovery)?;
        let cancellation = context.cancellation().child_token();
        let slot = Arc::new(JobTerminalCommitSlot::new(&context, progressed.sequence()));
        let precommit: Arc<dyn DatasetBuildPrecommitAuthority> =
            Arc::new(JobPhaseOneDerivedGenerationCommitAuthority {
                slot: Arc::clone(&slot),
            });
        let dataset = match tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.research
                .build_phase_one_derived_generation_with_precommit_authority(
                    request,
                    cancellation.clone(),
                    precommit,
                ),
        )
        .await
        {
            Ok(result) => result.map_err(map_phase_one_derived_generation_error)?,
            Err(_elapsed) => {
                cancellation.cancel();
                return Err(failed(
                    "phase-one-derived-generation-deadline-exceeded",
                    true,
                ));
            }
        };
        let published = slot.take_published().or_else(|_error| {
            context
                .claim_terminal_publication(progressed.sequence())
                .map(market_squawk_jobs::JobTerminalPublicationPermit::seal)
        })?;
        let manifest = dataset.manifest();
        let identity = identifier(format!(
            "phase-one-derived-generation-{}",
            encode_hex(manifest.content_hash().bytes())
        ))
        .map_err(|_error| JobRunError::Recovery)?;
        let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, manifest.content_hash().bytes());
        let splits = dataset.split_counts();
        let phase_one_descriptor_sha256 = dataset
            .python_export()
            .map_err(|error| map_phase_one_derived_generation_error(error.into()))?
            .content_hash();
        // This immutable result records only that this phase-one operation did not issue product
        // admission before publishing the result. Any receipt-backed product admission is a
        // separate Analysis.GetFeatureDatasets authority.
        let result_bytes = serde_json::to_vec(&serde_json::json!({
            "publicationStage": "phase_one_derived_generation",
            "productAdmission": "not_admitted_by_phase_one_operation_at_completion",
            "manifest": {
                "dataset": manifest.dataset_id().as_str(),
                "version": manifest.manifest_version(),
                "schema": manifest.schema().name(),
                "schemaVersion": manifest.schema_version().get(),
                "schemaFingerprintSha256": encode_hex(manifest.schema().fingerprint()),
                "contentSha256": encode_hex(manifest.content_hash().bytes()),
            },
            "buildSpecSha256": encode_hex(dataset.build_spec_digest().digest().bytes()),
            "policySha256": encode_hex(dataset.policy_digest().bytes()),
            "universeSha256": encode_hex(dataset.universe_digest().bytes()),
            "phaseOneDescriptorSha256": encode_hex(phase_one_descriptor_sha256.bytes()),
            "splitExamples": {
                "train": splits.train_examples(),
                "validation": splits.validation_examples(),
                "test": splits.test_examples(),
            },
        }))
        .map_err(|_error| failed("phase-one-derived-generation-result-invalid", false))?;
        let artifact = self
            .artifacts
            .publish(
                ArtifactPublication::try_json(result_bytes).map_err(map_artifact_error)?,
                ArtifactPublicationContext::new(context.cancellation().clone(), deadline),
            )
            .await
            .map_err(map_artifact_error)?;
        let result = JobResultReference::try_new(
            self.result_authority.clone(),
            identity,
            digest,
            vec![artifact],
        )
        .map_err(|_error| JobRunError::Recovery)?;
        Ok(JobCompletion::Published(result, published))
    }

    fn recover(&self, _snapshot: &market_squawk_jobs::JobSnapshot) -> JobRecoveryDisposition {
        // Phase-one requests retain non-cloneable source/output authority. Restart never recreates
        // it from serialized fields; any committed generation remains queryable by exact manifest.
        JobRecoveryDisposition::MarkInterrupted
    }
}

impl fmt::Debug for PhaseOneDerivedGenerationJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PhaseOneDerivedGenerationJobRunner")
            .field("generation_kind", &self.generation_kind)
            .field("kind", &self.kind)
            .field("research", &"[PHASE-ONE GENERATION AUTHORITY]")
            .field("artifacts", &"[CONTROLLED ARTIFACT AUTHORITY]")
            .field("pending", &"[BOUNDED PHASE-ONE REQUESTS]")
            .field("maximum_pending", &self.maximum_pending)
            .field("run_timeout", &self.run_timeout)
            .finish()
    }
}

impl fmt::Debug for ApplicationOperationJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationOperationJobRunner")
            .field("kind", &self.kind)
            .field("operation", &self.operation)
            .field("domain", &"[APPLICATION DOMAIN AUTHORITY]")
            .field("pending", &"[BOUNDED IMMUTABLE REQUESTS]")
            .field("maximum_pending", &self.maximum_pending)
            .field("run_timeout", &self.run_timeout)
            .finish()
    }
}

/// Research ingestion runner over the existing Research and controlled-artifact authorities.
pub struct ResearchJobRunner {
    operation: ApplicationOperationJobRunner,
    ingest: Arc<dyn ResearchIngestCoordinator>,
    artifacts: Arc<dyn ArtifactRepository>,
}

impl ResearchJobRunner {
    /// Creates the exact job runner for `Research.IngestSource`.
    pub fn try_new_ingest(
        research: Arc<dyn ApplicationDomainService>,
        ingest: Arc<dyn ResearchIngestCoordinator>,
        artifacts: Arc<dyn ArtifactRepository>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, ResearchJobRunnerError> {
        Ok(Self {
            operation: ApplicationOperationJobRunner::try_new(
                INGEST_KIND,
                INGEST_OPERATION,
                "research.ingest-request.v1",
                INGEST_AUTHORITY,
                ServiceDomain::Research,
                research,
                maximum_pending,
                run_timeout,
            )?,
            ingest,
            artifacts,
        })
    }

    /// Registers one already descriptor-admitted immutable research request.
    pub fn admit(
        &self,
        request: TypedToolRequest,
        limits: ServiceLimits,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, ResearchJobRunnerError> {
        self.operation.admit(request, limits, captured_at)
    }

    /// Releases one pending ingest when durable job creation did not succeed.
    pub fn revoke(&self, admission: &JobAdmission) -> Result<(), ResearchJobRunnerError> {
        self.operation.revoke(admission)
    }
}

#[async_trait]
impl JobRunner for ResearchJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        self.operation.kind()
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        let prepared = self.operation.prepare_run(&context).await?;
        let request_context = RequestContext::new(
            context.snapshot().spec().request_id().clone(),
            context.cancellation().clone(),
            prepared.deadline,
            prepared.pending.limits,
        );
        let slot = Arc::new(JobTerminalCommitSlot::new(&context, prepared.expected));
        let commit: Arc<dyn ResearchIngestCommitAuthority> = Arc::new(JobIngestCommitAuthority {
            slot: Arc::clone(&slot),
        });
        let result = self
            .ingest
            .ingest_with_precommit(
                &prepared.pending.request,
                &request_context,
                prepared.pending.limits,
                commit,
            )
            .await
            .map_err(map_service_error)?;
        let published = slot.take_published().or_else(|_error| {
            context
                .claim_terminal_publication(prepared.expected)
                .map(market_squawk_jobs::JobTerminalPublicationPermit::seal)
        })?;
        let encoded = serde_json::to_vec(&result.into_envelope())
            .map_err(|_error| failed("result-encoding-invalid", false))?;
        let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&encoded).into());
        let artifact = self
            .artifacts
            .publish(
                ArtifactPublication::try_json(encoded).map_err(map_artifact_error)?,
                ArtifactPublicationContext::new(context.cancellation().clone(), prepared.deadline),
            )
            .await
            .map_err(map_artifact_error)?;
        let result_identity = identifier(format!(
            "{}-result-{}",
            self.operation.kind.as_str(),
            encode_hex(digest.bytes())
        ))
        .map_err(|_error| JobRunError::Recovery)?;
        let reference = JobResultReference::try_new(
            self.operation.result_authority.clone(),
            result_identity,
            digest,
            vec![artifact],
        )
        .map_err(|_error| JobRunError::Recovery)?;
        Ok(JobCompletion::Published(reference, published))
    }

    fn recover(&self, _snapshot: &market_squawk_jobs::JobSnapshot) -> JobRecoveryDisposition {
        // Discovery selections and provider leases are process-generation capabilities. The exact
        // immutable request identity remains durable, but V1 requires a fresh explicit start.
        JobRecoveryDisposition::MarkInterrupted
    }
}

impl fmt::Debug for ResearchJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResearchJobRunner")
            .field("operation", &self.operation)
            .field("ingest", &"[RESEARCH INGEST COORDINATOR]")
            .field("artifacts", &"[CONTROLLED ARTIFACT AUTHORITY]")
            .finish()
    }
}

/// Durable controlled research export through the existing pinned read and artifact authorities.
pub struct ResearchExportJobRunner {
    operation: ApplicationOperationJobRunner,
    artifacts: Arc<dyn ArtifactRepository>,
}

impl ResearchExportJobRunner {
    /// Creates the runner for `Research.StartExport`.
    pub fn try_new(
        research: Arc<dyn ApplicationDomainService>,
        artifacts: Arc<dyn ArtifactRepository>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, ResearchJobRunnerError> {
        Ok(Self {
            operation: ApplicationOperationJobRunner::try_new(
                EXPORT_KIND,
                EXPORT_OPERATION,
                "research.export-request.v1",
                EXPORT_AUTHORITY,
                ServiceDomain::Research,
                research,
                maximum_pending,
                run_timeout,
            )?,
            artifacts,
        })
    }

    /// Registers one descriptor-admitted, manifest-pinned research read for controlled export.
    pub fn admit(
        &self,
        request: TypedToolRequest,
        limits: ServiceLimits,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, ResearchJobRunnerError> {
        self.operation.admit(request, limits, captured_at)
    }

    /// Releases one pending export when durable job creation did not succeed.
    pub fn revoke(&self, admission: &JobAdmission) -> Result<(), ResearchJobRunnerError> {
        self.operation.revoke(admission)
    }
}

#[async_trait]
impl JobRunner for ResearchExportJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        self.operation.kind()
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        self.operation
            .run_read_operation(context, &self.artifacts)
            .await
    }

    fn recover(&self, _snapshot: &market_squawk_jobs::JobSnapshot) -> JobRecoveryDisposition {
        JobRecoveryDisposition::MarkInterrupted
    }
}

impl fmt::Debug for ResearchExportJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResearchExportJobRunner")
            .field("operation", &self.operation)
            .field("artifacts", &"[CONTROLLED ARTIFACT AUTHORITY]")
            .finish()
    }
}

/// Research job admission failure without provider payload or secret disclosure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ResearchJobRunnerError {
    /// Runner domain or finite resource ceilings are invalid.
    #[error("research job runner configuration is invalid")]
    InvalidLimits,
    /// The request does not match the code-owned operation or cannot be canonically encoded.
    #[error("research job request is invalid")]
    InvalidRequest,
    /// Pending request capacity is exhausted.
    #[error("research job runner capacity is exhausted")]
    Capacity,
    /// The same immutable identity resolved to different request content.
    #[error("research job request identity conflicts")]
    Conflict,
    /// Pending request authority is unavailable.
    #[error("research job runner is unavailable")]
    Unavailable,
}

pub(super) fn map_service_error(error: ServiceError) -> JobRunError {
    match error {
        ServiceError::Cancelled => JobRunError::Cancelled,
        ServiceError::DeadlineExceeded => failed("operation-deadline-exceeded", true),
        ServiceError::InvalidRequest | ServiceError::NotFound => {
            failed("operation-input-rejected", false)
        }
        ServiceError::Unauthorized => failed("operation-authority-rejected", false),
        ServiceError::ResourceExhausted => failed("operation-resource-exhausted", true),
        ServiceError::Unavailable => failed("operation-authority-unavailable", true),
        ServiceError::InvalidResult | ServiceError::Internal => {
            failed("operation-terminal-invalid", false)
        }
    }
}

fn map_artifact_error(error: ArtifactError) -> JobRunError {
    match error {
        ArtifactError::Cancelled => JobRunError::Cancelled,
        ArtifactError::DeadlineExceeded => failed("export-deadline-exceeded", true),
        ArtifactError::ReadLimitExceeded => failed("export-resource-exhausted", true),
        ArtifactError::InvalidPublication | ArtifactError::InvalidReference => {
            failed("export-result-invalid", false)
        }
        ArtifactError::NotFound | ArtifactError::Unavailable => {
            failed("export-authority-unavailable", true)
        }
    }
}

fn map_phase_one_derived_generation_error(error: ResearchServiceError) -> JobRunError {
    match error {
        ResearchServiceError::Dataset(DatasetBuildError::Cancelled) => JobRunError::Cancelled,
        ResearchServiceError::Dataset(DatasetBuildError::DeadlineExceeded) => {
            failed("phase-one-derived-generation-deadline-exceeded", true)
        }
        ResearchServiceError::Dataset(DatasetBuildError::PublicationAuthorityRevoked) => failed(
            "phase-one-derived-generation-publication-authority-revoked",
            false,
        ),
        ResearchServiceError::Dataset(
            DatasetBuildError::InvalidLimits
            | DatasetBuildError::LimitExceeded
            | DatasetBuildError::Arrow(_)
            | DatasetBuildError::Parquet(_),
        ) => failed("phase-one-derived-generation-resource-exhausted", true),
        ResearchServiceError::Dataset(
            DatasetBuildError::InvalidRequest
            | DatasetBuildError::InvalidInputGeneration
            | DatasetBuildError::ComponentEvidenceMismatch
            | DatasetBuildError::ComponentAdjustmentMismatch
            | DatasetBuildError::MissingValueRejected
            | DatasetBuildError::TemporalLeakage
            | DatasetBuildError::InstrumentOutsideUniverse
            | DatasetBuildError::UniverseEvidenceMismatch
            | DatasetBuildError::UnresolvedCorporateAction
            | DatasetBuildError::EmptyDataset
            | DatasetBuildError::ExportEncoding
            | DatasetBuildError::PointInTime
            | DatasetBuildError::Universe(_)
            | DatasetBuildError::CorporateAction(_)
            | DatasetBuildError::ManifestPlan(_)
            | DatasetBuildError::Rights(_)
            | DatasetBuildError::Schema(_),
        ) => failed("phase-one-derived-generation-input-rejected", false),
        ResearchServiceError::Dataset(
            DatasetBuildError::AuthorityLockPoisoned
            | DatasetBuildError::ManifestCatalog(_)
            | DatasetBuildError::Ingest(_)
            | DatasetBuildError::Catalog(_)
            | DatasetBuildError::ResearchUse(_)
            | DatasetBuildError::PythonDataset(_),
        )
        | ResearchServiceError::Path(_)
        | ResearchServiceError::Catalog(_)
        | ResearchServiceError::Manifest(_)
        | ResearchServiceError::ProviderCaptureStore(_)
        | ResearchServiceError::ProviderOnboarding(_)
        | ResearchServiceError::Ingest(_)
        | ResearchServiceError::IngestAuthorityMismatch
        | ResearchServiceError::Rights(_)
        | ResearchServiceError::IdentityOverflow => {
            failed("phase-one-derived-generation-authority-unavailable", true)
        }
    }
}

pub(super) fn failed(diagnostic: &str, retryable: bool) -> JobRunError {
    let Ok(class) = SourceIdentifier::try_from("application-operation") else {
        return JobRunError::Recovery;
    };
    let Ok(diagnostic) = SourceIdentifier::try_from(diagnostic) else {
        return JobRunError::Recovery;
    };
    JobRunError::Failed(JobFailure::new(class, diagnostic, retryable))
}

fn namespace_digest(namespace: &str) -> EvidenceDigest {
    EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(namespace.as_bytes()).into(),
    )
}

fn identifier(
    value: impl TryInto<SourceIdentifier>,
) -> Result<SourceIdentifier, ResearchJobRunnerError> {
    value
        .try_into()
        .map_err(|_error| ResearchJobRunnerError::InvalidRequest)
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
