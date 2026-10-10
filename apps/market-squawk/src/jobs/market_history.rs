//! Selected adjusted history under the existing installed job and publication authorities.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use market_squawk_adapter_alpaca::AlpacaHistoricalLookback;
use market_squawk_data::{CompleteMarketBarHistoryOutput, MarketDataInstrumentRecord};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, InstrumentId, SourceIdentifier, Timestamp,
};
use market_squawk_jobs::{
    AdmittedJobInput, JobAttemptLimit, JobAuthoritySnapshot, JobCompletion, JobProgress,
    JobRecoveryDisposition, JobResultReference, JobRunContext, JobRunError, JobRunner,
    JobRunnerEvent, JobSnapshot,
};
use market_squawk_modeling::ForecastArtifactManifestRecord;
use market_squawk_services::{
    ArtifactPublication, ArtifactPublicationContext, ArtifactRepository, RequestContext,
    ServiceLimits, validate_json_contract,
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use super::{
    JobTerminalCommitSlot,
    research::{ResearchJobRunnerError, failed, ingest_commit_authority, map_service_error},
};
use crate::application::{
    SourceActionPreparationCapability, job::JobAdmission,
    market_selection::product::product_market_identities,
};

const KIND: &str = "market.prepare-history.v1";
const INPUT: &str = "market.history-preparation-request.v1";
const RESULT: &str = "market.adjusted-history-publication.v1";

struct Pending {
    instrument: MarketDataInstrumentRecord,
    history_token: String,
    lookback: AlpacaHistoricalLookback,
    captured_at: Timestamp,
    limits: ServiceLimits,
}
struct Admitted {
    identity: SourceIdentifier,
    pending: Option<Pending>,
}

/// Process-bound pending input; durable jobs keep inspectable selected coordinates on restart.
pub(crate) struct MarketHistoryJobRunner {
    kind: SourceIdentifier,
    preparation: SourceActionPreparationCapability,
    artifacts: Arc<dyn ArtifactRepository>,
    pending: Mutex<BTreeMap<InstrumentId, Admitted>>,
    maximum_pending: usize,
    run_timeout: Duration,
}

/// Sanitized immutable coordinates parsed from the authoritative durable input, not live tokens.
#[derive(Debug)]
pub(crate) struct MarketHistoryJobInput {
    pub(crate) instrument: InstrumentId,
    pub(crate) history_token: String,
    pub(crate) lookback: AlpacaHistoricalLookback,
    pub(crate) captured_at: Timestamp,
}

impl MarketHistoryJobRunner {
    pub(crate) fn try_new(
        preparation: SourceActionPreparationCapability,
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

    /// Only local identity admission occurs here; no runtime lease or provider request is opened.
    pub(crate) fn admit(
        &self,
        instrument: MarketDataInstrumentRecord,
        history_token: String,
        lookback: AlpacaHistoricalLookback,
        limits: ServiceLimits,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, ResearchJobRunnerError> {
        let identities =
            product_market_identities(std::slice::from_ref(&instrument), captured_at, None)
                .map_err(|_| ResearchJobRunnerError::InvalidRequest)?;
        if identities.len() != 1
            || identities[0].history_token() != history_token
            || instrument.published_at() > captured_at
        {
            return Err(ResearchJobRunnerError::InvalidRequest);
        }
        let instrument_id = instrument.definition().instrument_id();
        let input = Pending {
            instrument,
            history_token,
            lookback,
            captured_at,
            limits,
        };
        let digest = input_digest(&input)?;
        let identity = id(format!(
            "history:{}:{}:{}:{}",
            instrument_id,
            input.history_token,
            lookback.days(),
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
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| ResearchJobRunnerError::Unavailable)?;
        if pending.contains_key(&instrument_id) {
            return Err(ResearchJobRunnerError::Conflict);
        }
        if pending.len() >= self.maximum_pending {
            return Err(ResearchJobRunnerError::Capacity);
        }
        pending.insert(
            instrument_id,
            Admitted {
                identity,
                pending: Some(input),
            },
        );
        Ok(admission)
    }

    /// Used only after definitive durable admission rejection; running work cannot be released.
    pub(crate) fn revoke(&self, admission: &JobAdmission) -> Result<(), ResearchJobRunnerError> {
        if admission.kind() != &self.kind || admission.input().authority().as_str() != INPUT {
            return Err(ResearchJobRunnerError::InvalidRequest);
        }
        self.release_pending(admission.input().identity())
    }

    /// Releases a cancelled queued admission that never entered run. Active runs release on exit.
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

    /// Durable scope check also works after restart without the old selected record or lease.
    /// The service must additionally apply its existing origin/authorization check.
    pub(crate) fn belongs_to(&self, snapshot: &JobSnapshot, history_token: &str) -> bool {
        self.input(snapshot)
            .is_some_and(|input| input.history_token == history_token)
    }

    pub(crate) fn input(&self, snapshot: &JobSnapshot) -> Option<MarketHistoryJobInput> {
        let spec = snapshot.spec();
        if spec.kind() != &self.kind
            || spec.input().authority().as_str() != INPUT
            || spec.authority().authority().as_str() != RESULT
            || spec.authority().digest() != digest_bytes(RESULT.as_bytes())
        {
            return None;
        }
        let parts: Vec<_> = spec.input().identity().as_str().split(':').collect();
        let ["history", instrument, token, days, digest] = parts.as_slice() else {
            return None;
        };
        if *digest != hex(spec.input().digest().bytes()) {
            return None;
        }
        Some(MarketHistoryJobInput {
            instrument: instrument.parse().ok()?,
            history_token: (*token).to_owned(),
            lookback: AlpacaHistoricalLookback::try_from_days(days.parse().ok()?).ok()?,
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
            id("preparing-adjusted-history").map_err(|_| JobRunError::Recovery)?,
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
        let request = RequestContext::new(
            context.snapshot().spec().request_id().clone(),
            context.cancellation().clone(),
            deadline,
            input.limits,
        );
        let slot = Arc::new(JobTerminalCommitSlot::new(context, progressed.sequence()));
        let prepared = self
            .preparation
            .prepare_selected_market_chart_history(
                &input.instrument,
                input.lookback,
                input.captured_at,
                ingest_commit_authority(Arc::clone(&slot)),
                &request,
            )
            .await;
        let history = match prepared {
            Ok(history) => history,
            Err(error) => {
                // Never report a committed history write as cancelled/failed. Its sealed fence
                // and immutable history survive for the existing restart reconciliation path.
                return Err(if slot.take_published().is_ok() {
                    JobRunError::Recovery
                } else {
                    map_service_error(error)
                });
            }
        };
        let published = slot.take_published()?;
        // Every later failure retains the sealed fence and the genuine history publication.
        let value = result_value(&input, &history).map_err(|_| JobRunError::Recovery)?;
        validate_json_contract(
            &value,
            input.limits.result_structure(),
            input.limits.maximum_result_bytes(),
        )
        .map_err(|_| JobRunError::Recovery)?;
        let bytes = serde_json::to_vec(&value).map_err(|_| JobRunError::Recovery)?;
        let digest = digest_bytes(&bytes);
        let artifact = self
            .artifacts
            .publish(
                ArtifactPublication::try_json(bytes).map_err(|_| JobRunError::Recovery)?,
                ArtifactPublicationContext::new(context.cancellation().clone(), deadline),
            )
            .await
            .map_err(|_| JobRunError::Recovery)?;
        let reference = JobResultReference::try_new(
            id(RESULT).map_err(|_| JobRunError::Recovery)?,
            id(format!("history-result-{}", hex(digest.bytes())))
                .map_err(|_| JobRunError::Recovery)?,
            digest,
            vec![artifact],
        )
        .map_err(|_| JobRunError::Recovery)?;
        Ok(JobCompletion::Published(reference, published))
    }
}

#[async_trait]
impl JobRunner for MarketHistoryJobRunner {
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
            let admitted = pending
                .get_mut(&scope.instrument)
                .ok_or(JobRunError::Recovery)?;
            if &admitted.identity != identity {
                return Err(JobRunError::Recovery);
            }
            admitted.pending.take().ok_or(JobRunError::Recovery)?
        };
        // Keep the instrument reserved across the entire future, including cancellation/drop.
        let _lease = RunningAdmission {
            runner: self,
            instrument: scope.instrument,
            identity: identity.clone(),
        };
        if input_digest(&input).map_err(|_| JobRunError::Recovery)?
            != context.snapshot().spec().input().digest()
            || input.captured_at != scope.captured_at
        {
            return Err(JobRunError::Recovery);
        }
        if context.cancellation().is_cancelled() {
            return Err(JobRunError::Cancelled);
        }
        self.execute(&context, input).await
    }

    async fn recover(&self, _snapshot: &JobSnapshot) -> JobRecoveryDisposition {
        // The immutable selected coordinates remain inspectable; the old provider lease does not.
        JobRecoveryDisposition::MarkInterrupted
    }
}

struct RunningAdmission<'a> {
    runner: &'a MarketHistoryJobRunner,
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

fn result_value(
    input: &Pending,
    history: &CompleteMarketBarHistoryOutput,
) -> Result<Value, JobRunError> {
    let receipt = history.selection().receipt();
    let (start, end) = receipt.requested_range().ok_or(JobRunError::Recovery)?;
    let (first, last, last_complete) = receipt.coverage().ok_or(JobRunError::Recovery)?;
    Ok(json!({
        "schemaVersion": "market.history-preparation-result.v1",
        "historyToken": input.history_token,
        "instrumentId": receipt.instrument_id(),
        "adjustment": "fully_adjusted",
        "manifest": ForecastArtifactManifestRecord::from_manifest(history.read_receipt().origin_manifest()),
        "requestedCoverage": { "lookbackDays": input.lookback.days(),
            "capturedAtUnixNanos": input.captured_at.unix_nanos().to_string(),
            "startUnixNanos": start.unix_nanos().to_string(), "endUnixNanos": end.unix_nanos().to_string() },
        "actualCoverage": { "firstUnixNanos": first.unix_nanos().to_string(),
            "lastUnixNanos": last.unix_nanos().to_string(),
            "lastCompleteUnixNanos": last_complete.unix_nanos().to_string(), "barCount": receipt.bar_count() },
        "readCutoffUnixNanos": history.read_receipt().knowledge_cutoff().unix_nanos().to_string(),
        "publicationReceiptDigest": hex(receipt.receipt_digest().bytes()),
        "historyContentDigest": hex(history.read_receipt().history_content_digest().bytes()),
        "verifiedReadDigest": hex(history.read_receipt().result_digest().bytes()),
    }))
}

fn input_digest(input: &Pending) -> Result<EvidenceDigest, ResearchJobRunnerError> {
    let limits = input.limits;
    let structure = limits.result_structure();
    let value = json!({ "instrument": input.instrument.definition(),
        "revision": input.instrument.revision_digest(), "revisionSequence": input.instrument.revision_sequence(),
        "publishedAt": input.instrument.published_at(), "historyToken": input.history_token,
        "lookbackDays": input.lookback.days(), "capturedAt": input.captured_at,
        "limits": [limits.maximum_inline_bytes(), limits.maximum_inline_items(),
            limits.maximum_result_bytes(), limits.maximum_result_items(), structure.maximum_depth(),
            structure.maximum_string_bytes(), structure.maximum_array_items(), structure.maximum_map_entries()] });
    serde_json::to_vec(&value)
        .map(|bytes| digest_bytes(&bytes))
        .map_err(|_| ResearchJobRunnerError::InvalidRequest)
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
