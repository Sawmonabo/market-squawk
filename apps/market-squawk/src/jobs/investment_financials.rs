//! Independently owned selected financial preparation using the installed job authority.
//!
//! Genuine SEC family publications and individually corroborated company/security links are
//! retained intermediates. Cancellation may leave those valid records, but never a completed
//! preparation result. Only the immutable full result wins the terminal publication fence;
//! no terminal fence spans provider I/O or the multiple association transactions.

use super::research::{ResearchJobRunnerError, failed, map_service_error};
use crate::application::{
    InvestmentFinancialPreparation, InvestmentFinancialPreparationInput, job::JobAdmission,
};
use async_trait::async_trait;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, InstrumentId, SourceIdentifier, Timestamp,
};
use market_squawk_jobs::{
    AdmittedJobInput, JobAttemptLimit, JobAuthoritySnapshot, JobCompletion, JobProgress,
    JobRecoveryDisposition, JobResultReference, JobRunContext, JobRunError, JobRunner,
    JobRunnerEvent, JobSnapshot,
};
use market_squawk_services::{
    ArtifactPublication, ArtifactPublicationContext, ArtifactRepository, ServiceError,
    ServiceLimits, validate_json_contract,
};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

const KIND: &str = "research.prepare-investment-financials.v1";
const INPUT: &str = "research.financial-preparation-request.v1";
const RESULT: &str = "research.financial-preparation-result.v1";

struct Pending {
    selection: InvestmentFinancialPreparationInput,
    limits: ServiceLimits,
}
struct Admitted {
    identity: SourceIdentifier,
    pending: Option<Pending>,
}

pub(crate) struct InvestmentFinancialJobRunner {
    kind: SourceIdentifier,
    preparation: Arc<InvestmentFinancialPreparation>,
    artifacts: Arc<dyn ArtifactRepository>,
    pending: Mutex<BTreeMap<InstrumentId, Admitted>>,
    maximum_pending: usize,
    run_timeout: Duration,
}

/// Durable scope remains inspectable even when process-owned provider authority is gone.
#[derive(Debug)]
pub(crate) struct InvestmentFinancialJobInput {
    pub(crate) instrument: InstrumentId,
    pub(crate) selection_token: String,
    pub(crate) captured_at: Timestamp,
}

impl InvestmentFinancialJobRunner {
    pub(crate) fn try_new(
        preparation: Arc<InvestmentFinancialPreparation>,
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
        Ok(Self {
            kind: id(KIND)?,
            preparation,
            artifacts,
            pending: Mutex::new(BTreeMap::new()),
            maximum_pending,
            run_timeout,
        })
    }

    /// Resolve local exact selection/listing evidence only. Remote acquisition belongs to run.
    pub(crate) async fn admit(
        &self,
        selection_token: &str,
        limits: ServiceLimits,
        captured_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<JobAdmission, ResearchJobRunnerError> {
        let selection = self
            .preparation
            .admit_selection(selection_token, captured_at, deadline, cancellation)
            .await
            .map_err(map_admission_error)?;
        let instrument = selection.instrument();
        let input = Pending { selection, limits };
        let digest = input_digest(&input)?;
        let identity = id(format!(
            "financial:{}:{}:{}",
            instrument,
            selection_token,
            hex(digest.bytes())
        ))?;
        let admission = JobAdmission::new(
            self.kind.clone(),
            AdmittedJobInput::new(id(INPUT)?, identity.clone(), digest),
            JobAuthoritySnapshot::new(
                id(RESULT)?,
                id(RESULT)?,
                digest_bytes(RESULT.as_bytes()),
                captured_at,
            ),
            JobAttemptLimit::try_new(1).map_err(|_| ResearchJobRunnerError::InvalidRequest)?,
        );
        if cancellation.is_cancelled() {
            return Err(ResearchJobRunnerError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(ResearchJobRunnerError::DeadlineExceeded);
        }
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| ResearchJobRunnerError::Unavailable)?;
        if pending.contains_key(&instrument) {
            return Err(ResearchJobRunnerError::Conflict);
        }
        if pending.len() >= self.maximum_pending {
            return Err(ResearchJobRunnerError::Capacity);
        }
        pending.insert(
            instrument,
            Admitted {
                identity,
                pending: Some(input),
            },
        );
        Ok(admission)
    }

    pub(crate) fn revoke(&self, admission: &JobAdmission) -> Result<(), ResearchJobRunnerError> {
        if admission.kind() != &self.kind || admission.input().authority().as_str() != INPUT {
            return Err(ResearchJobRunnerError::InvalidRequest);
        }
        self.release_pending(admission.input().identity())
    }

    pub(crate) fn release_terminal(
        &self,
        snapshot: &JobSnapshot,
    ) -> Result<(), ResearchJobRunnerError> {
        if self.input(snapshot).is_none() || !snapshot.state().is_terminal() {
            return Err(ResearchJobRunnerError::InvalidRequest);
        }
        self.release_pending(snapshot.spec().input().identity())
    }

    fn release_pending(&self, identity: &SourceIdentifier) -> Result<(), ResearchJobRunnerError> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| ResearchJobRunnerError::Unavailable)?;
        pending.retain(|_, value| &value.identity != identity || value.pending.is_none());
        Ok(())
    }

    /// The shared service additionally checks authenticated origin, as for history jobs.
    pub(crate) fn belongs_to(&self, snapshot: &JobSnapshot, selection_token: &str) -> bool {
        self.input(snapshot)
            .is_some_and(|input| input.selection_token == selection_token)
    }

    pub(crate) fn input(&self, snapshot: &JobSnapshot) -> Option<InvestmentFinancialJobInput> {
        let spec = snapshot.spec();
        if spec.kind() != &self.kind
            || spec.input().authority().as_str() != INPUT
            || spec.authority().authority().as_str() != RESULT
            || spec.authority().digest() != digest_bytes(RESULT.as_bytes())
        {
            return None;
        }
        let parts: Vec<_> = spec.input().identity().as_str().split(':').collect();
        let ["financial", instrument, token, digest] = parts.as_slice() else {
            return None;
        };
        if *digest != hex(spec.input().digest().bytes()) {
            return None;
        }
        Some(InvestmentFinancialJobInput {
            instrument: instrument.parse().ok()?,
            selection_token: (*token).to_owned(),
            captured_at: spec.authority().captured_at(),
        })
    }

    async fn execute(
        &self,
        context: &JobRunContext,
        input: Pending,
    ) -> Result<JobCompletion, JobRunError> {
        let deadline = Instant::now()
            .checked_add(self.run_timeout)
            .ok_or(JobRunError::Recovery)?;
        let progress = JobProgress::try_new(
            id("preparing-investment-financials").map_err(|_| JobRunError::Recovery)?,
            0,
            None,
            context.snapshot().updated_at_timestamp(),
        )
        .map_err(|_| JobRunError::Recovery)?;
        let progressed = context
            .events()
            .append(JobRunnerEvent::Progress(progress))
            .await
            .map_err(|_| failed("job-progress-unavailable", true))?;
        let outcome = self
            .preparation
            .acquire(&input.selection, deadline, context.cancellation())
            .await
            .map_err(map_service_error)?;
        let value = json!({
            "schemaVersion": RESULT,
            "selection": input.selection.coordinates(),
            "outcome": outcome.value(),
        });
        validate_json_contract(
            &value,
            input.limits.result_structure(),
            input.limits.maximum_result_bytes(),
        )
        .map_err(|_| failed("financial-preparation-result-invalid", false))?;
        let bytes = serde_json::to_vec(&value).map_err(|_| JobRunError::Recovery)?;
        let digest = digest_bytes(&bytes);
        // This artifact remains staged until the completed JobResultReference is published.
        // An interrupted attempt never infers completion merely from retained intermediate data.
        let artifact = self
            .artifacts
            .publish(
                ArtifactPublication::try_json(bytes).map_err(|_| JobRunError::Recovery)?,
                ArtifactPublicationContext::new(context.cancellation().clone(), deadline),
            )
            .await
            .map_err(|_| {
                if context.cancellation().is_cancelled() {
                    JobRunError::Cancelled
                } else {
                    failed("financial-preparation-result-unavailable", true)
                }
            })?;
        let result = JobResultReference::try_new(
            id(RESULT).map_err(|_| JobRunError::Recovery)?,
            id(format!("financial-result-{}", hex(digest.bytes())))
                .map_err(|_| JobRunError::Recovery)?,
            digest,
            vec![artifact],
        )
        .map_err(|_| JobRunError::Recovery)?;
        self.preparation
            .validate_completion(&outcome, deadline, context.cancellation())
            .map_err(map_service_error)?;
        // All provider, association, artifact and result work is complete. This is the only
        // terminal claim, matching the derived-generation runner's staged-result finalization.
        let published = context
            .claim_terminal_publication(progressed.sequence())?
            .seal();
        Ok(JobCompletion::Published(result, published))
    }
}

#[async_trait]
impl JobRunner for InvestmentFinancialJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }
    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        let scope = self
            .input(context.snapshot())
            .ok_or(JobRunError::Recovery)?;
        let identity = context.snapshot().spec().input().identity();
        let input = {
            let mut pending = self.pending.lock().map_err(|_| JobRunError::Recovery)?;
            let entry = pending
                .get_mut(&scope.instrument)
                .ok_or(JobRunError::Recovery)?;
            if &entry.identity != identity {
                return Err(JobRunError::Recovery);
            }
            entry.pending.take().ok_or(JobRunError::Recovery)?
        };
        let _lease = RunningAdmission {
            runner: self,
            instrument: scope.instrument,
            identity: identity.clone(),
        };
        if input_digest(&input).map_err(|_| JobRunError::Recovery)?
            != context.snapshot().spec().input().digest()
            || input.selection.captured_at() != scope.captured_at
            || input.selection.selection_token() != scope.selection_token
        {
            return Err(JobRunError::Recovery);
        }
        if context.cancellation().is_cancelled() {
            return Err(JobRunError::Cancelled);
        }
        self.execute(&context, input).await
    }
    async fn recover(&self, _snapshot: &JobSnapshot) -> JobRecoveryDisposition {
        // A completed job is reopened by its durable result reference. Nonterminal work has no
        // resumable provider lease; retained intermediates are not a completed preparation.
        JobRecoveryDisposition::MarkInterrupted
    }
}

struct RunningAdmission<'a> {
    runner: &'a InvestmentFinancialJobRunner,
    instrument: InstrumentId,
    identity: SourceIdentifier,
}
impl Drop for RunningAdmission<'_> {
    fn drop(&mut self) {
        let mut pending = self
            .runner
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending
            .get(&self.instrument)
            .is_some_and(|entry| entry.identity == self.identity)
        {
            pending.remove(&self.instrument);
        }
    }
}

fn input_digest(input: &Pending) -> Result<EvidenceDigest, ResearchJobRunnerError> {
    let limits = input.limits;
    let structure = limits.result_structure();
    serde_json::to_vec(&json!({ "selection": input.selection.coordinates(),
        "limits": [limits.maximum_inline_bytes(), limits.maximum_inline_items(),
            limits.maximum_result_bytes(), limits.maximum_result_items(), structure.maximum_depth(),
            structure.maximum_string_bytes(), structure.maximum_array_items(), structure.maximum_map_entries()] }))
        .map(|bytes| digest_bytes(&bytes)).map_err(|_| ResearchJobRunnerError::InvalidRequest)
}
fn map_admission_error(error: ServiceError) -> ResearchJobRunnerError {
    match error {
        ServiceError::Cancelled => ResearchJobRunnerError::Cancelled,
        ServiceError::DeadlineExceeded => ResearchJobRunnerError::DeadlineExceeded,
        ServiceError::InvalidRequest | ServiceError::NotFound => {
            ResearchJobRunnerError::InvalidRequest
        }
        ServiceError::ResourceExhausted => ResearchJobRunnerError::Capacity,
        _ => ResearchJobRunnerError::Unavailable,
    }
}
fn id(value: impl TryInto<SourceIdentifier>) -> Result<SourceIdentifier, ResearchJobRunnerError> {
    value
        .try_into()
        .map_err(|_| ResearchJobRunnerError::InvalidRequest)
}
fn digest_bytes(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}
fn hex(bytes: [u8; 32]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut value, byte| {
            let _ = write!(value, "{byte:02x}");
            value
        })
}
