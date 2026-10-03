//! Job custody for the existing selected-investment source preparation pipeline.
//!
//! Process-owned input is consumed once. Its durable commitment includes the full original
//! arguments and authenticated origin; completed results retain that exact input and body.
//! Restart marks unfinished acquisition interrupted and only reopens completed artifacts.

use super::{
    GET_PREPARATION_RESULT, InstalledMarketEvidence, InvestmentIdentity, PREPARE, PrepareRequest,
    encode_digest, ensure_live, map_identity_error,
};
use crate::application::{
    InstrumentContextOutcome, InstrumentContextRead,
    job::{JobAdmission, JobReceipt},
};
use crate::service::forecast_preparation::InstalledForecastPreparation;
use async_trait::async_trait;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, InstrumentId, SourceIdentifier, Timestamp,
};
use market_squawk_jobs::{
    AdmittedJobInput, AdmittedJobSpec, JobAttemptLimit, JobAuthoritySnapshot, JobCompletion,
    JobFailure, JobProgress, JobRecoveryDisposition, JobResultReference, JobRunContext,
    JobRunError, JobRunner, JobRunnerEvent, JobSnapshot, JobState,
};
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactReadContext,
    ArtifactReadRequest, ArtifactRepository, JsonStructureLimits, RequestContext, RequestOrigin,
    ServiceError, ServiceLimits, ToolDescriptor, ToolResultMetadata, TypedToolRequest,
    TypedToolResult, validate_json_contract,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const KIND: &str = "market.prepare-investment-evidence.v1";
const INPUT: &str = "market.investment-evidence-request.v1";
const RESULT: &str = "market.investment-evidence-result.v1";

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RecordedInput {
    schema_version: String,
    operation: String,
    arguments: Value,
    workspace_id: Uuid,
    client_id: Uuid,
    request_id: Value,
    limits: [usize; 8],
    captured_at: Timestamp,
    instrument_id: InstrumentId,
    identity: Option<Value>,
}

impl RecordedInput {
    fn origin(&self) -> Result<RequestOrigin, ServiceError> {
        RequestOrigin::try_new(self.workspace_id, self.client_id)
            .map_err(|_| ServiceError::InvalidResult)
    }

    fn service_limits(&self) -> Result<ServiceLimits, ServiceError> {
        let [
            inline_bytes,
            inline_items,
            result_bytes,
            result_items,
            depth,
            strings,
            arrays,
            maps,
        ] = self.limits;
        ServiceLimits::try_new(
            inline_bytes,
            inline_items,
            result_bytes,
            result_items,
            JsonStructureLimits::try_new(depth, strings, arrays, maps)
                .map_err(|_| ServiceError::InvalidResult)?,
        )
        .map_err(|_| ServiceError::InvalidResult)
    }

    fn request(&self) -> Result<PrepareRequest, ServiceError> {
        serde_json::from_value(self.arguments.clone()).map_err(|_| ServiceError::InvalidResult)
    }

    fn digest(&self) -> Result<EvidenceDigest, ServiceError> {
        serde_json::to_vec(self)
            .map(|bytes| digest(&bytes))
            .map_err(|_| ServiceError::InvalidResult)
    }

    fn request_sha256(&self) -> Result<String, ServiceError> {
        serde_json::to_vec(&self.arguments)
            .map(|bytes| encode_digest(digest(&bytes).bytes()))
            .map_err(|_| ServiceError::InvalidResult)
    }

    fn identity_key(&self) -> Result<SourceIdentifier, ServiceError> {
        identifier(&format!(
            "evidence:{}:{}",
            self.request()?.selection_token,
            encode_digest(self.digest()?.bytes()),
        ))
    }

    fn validate(&self, spec: &AdmittedJobSpec) -> Result<(), ServiceError> {
        let origin = snapshot_origin(spec)?;
        let identity = self
            .identity
            .as_ref()
            .map(|value| serde_json::from_value::<InvestmentIdentity>(value.clone()))
            .transpose()
            .map_err(|_| ServiceError::InvalidResult)?;
        if self.schema_version != INPUT
            || self.operation != PREPARE
            || self.origin()? != origin
            || self.request_id
                != serde_json::to_value(spec.request_id())
                    .map_err(|_| ServiceError::InvalidResult)?
            || self.captured_at != spec.authority().captured_at()
            || self.digest()? != spec.input().digest()
            || self.identity_key()? != *spec.input().identity()
            || identity.is_some_and(|value| value.instrument_id != self.instrument_id)
        {
            return Err(ServiceError::InvalidResult);
        }
        let limits = self.service_limits()?;
        validate_json_contract(
            &self.arguments,
            limits.result_structure(),
            limits.maximum_result_bytes(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        Ok(())
    }
}

struct Pending {
    recorded: RecordedInput,
    identity: Option<InstrumentContextRead>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PreparedResult {
    schema_version: String,
    job_id: Uuid,
    generation: u64,
    input: RecordedInput,
    preparation: Value,
    request_sha256: String,
}

impl PreparedResult {
    fn validate_binding(&self, spec: &AdmittedJobSpec) -> Result<(), ServiceError> {
        if self.schema_version != RESULT
            || self.job_id != spec.id().as_uuid()
            || self.generation != spec.generation().get()
            || self.request_sha256 != self.input.request_sha256()?
        {
            return Err(ServiceError::InvalidResult);
        }
        self.input.validate(spec)
    }
}

pub(in crate::service) struct InvestmentEvidenceJobRunner {
    kind: SourceIdentifier,
    preparation: Arc<InstalledMarketEvidence>,
    forecast: Arc<InstalledForecastPreparation>,
    artifacts: Arc<dyn ArtifactRepository>,
    result_descriptor: ToolDescriptor,
    pending: Mutex<BTreeMap<SourceIdentifier, Option<Pending>>>,
    maximum_pending: usize,
    run_timeout: Duration,
}

impl InvestmentEvidenceJobRunner {
    pub(in crate::service) fn try_new(
        preparation: Arc<InstalledMarketEvidence>,
        forecast: Arc<InstalledForecastPreparation>,
        artifacts: Arc<dyn ArtifactRepository>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, ServiceError> {
        if maximum_pending == 0 || run_timeout.is_zero() {
            return Err(ServiceError::InvalidRequest);
        }
        let result_descriptor = crate::application::application_capabilities()
            .map_err(|_| ServiceError::Internal)?
            .tools()
            .iter()
            .find(|tool| tool.name() == GET_PREPARATION_RESULT)
            .cloned()
            .ok_or(ServiceError::Internal)?;
        Ok(Self {
            kind: identifier(KIND)?,
            preparation,
            forecast,
            artifacts,
            result_descriptor,
            pending: Mutex::new(BTreeMap::new()),
            maximum_pending,
            run_timeout,
        })
    }

    /// Admission performs retained local reads only and never inherits transport lifetime in run.
    pub(in crate::service) async fn admit(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<JobAdmission, ServiceError> {
        ensure_live(context)?;
        if request.name() != PREPARE {
            return Err(ServiceError::InvalidRequest);
        }
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let arguments = Value::Object(crate::service::business_arguments(request.arguments()));
        let input: PrepareRequest =
            serde_json::from_value(arguments.clone()).map_err(|_| ServiceError::InvalidRequest)?;
        let models = self
            .forecast
            .financial_profile_catalog(&input.financial_profile.configuration, context)
            .await?;
        let admitted = self
            .preparation
            .admit_preparation(&input, context, models.as_ref())
            .await?;
        let identity_value = admitted
            .identity
            .as_ref()
            .map(|identity| {
                let InstrumentContextOutcome::Exact(value) = identity.outcome() else {
                    return Err(ServiceError::InvalidResult);
                };
                serde_json::to_value(InvestmentIdentity::from(value))
                    .map_err(|_| ServiceError::InvalidResult)
            })
            .transpose()?;
        let limits = context.limits();
        let structure = limits.result_structure();
        let recorded = RecordedInput {
            schema_version: INPUT.to_owned(),
            operation: PREPARE.to_owned(),
            arguments,
            workspace_id: origin.workspace_id(),
            client_id: origin.client_id(),
            request_id: serde_json::to_value(context.request_id())
                .map_err(|_| ServiceError::InvalidRequest)?,
            limits: [
                limits.maximum_inline_bytes(),
                limits.maximum_inline_items(),
                limits.maximum_result_bytes(),
                limits.maximum_result_items(),
                structure.maximum_depth(),
                structure.maximum_string_bytes(),
                structure.maximum_array_items(),
                structure.maximum_map_entries(),
            ],
            captured_at: super::clock()?,
            instrument_id: admitted.instrument_id,
            identity: identity_value,
        };
        let key = recorded.identity_key()?;
        let admission = JobAdmission::new(
            self.kind.clone(),
            AdmittedJobInput::new(identifier(INPUT)?, key.clone(), recorded.digest()?),
            JobAuthoritySnapshot::new(
                identifier(RESULT)?,
                identifier(RESULT)?,
                digest(RESULT.as_bytes()),
                recorded.captured_at,
            ),
            JobAttemptLimit::try_new(1).map_err(|_| ServiceError::InvalidRequest)?,
        );
        ensure_live(context)?;
        let mut pending = self.pending.lock().map_err(|_| ServiceError::Unavailable)?;
        if pending.contains_key(&key) {
            return Err(ServiceError::Unavailable);
        }
        if pending.len() >= self.maximum_pending {
            return Err(ServiceError::ResourceExhausted);
        }
        pending.insert(
            key,
            Some(Pending {
                recorded,
                identity: admitted.identity,
            }),
        );
        Ok(admission)
    }

    pub(in crate::service) fn revoke(&self, admission: &JobAdmission) -> Result<(), ServiceError> {
        if admission.kind() != &self.kind || admission.input().authority().as_str() != INPUT {
            return Err(ServiceError::InvalidRequest);
        }
        self.release_pending(admission.input().identity())
    }

    pub(in crate::service) fn release_terminal(
        &self,
        snapshot: &JobSnapshot,
    ) -> Result<(), ServiceError> {
        snapshot_origin(snapshot.spec())?;
        if !snapshot.state().is_terminal() {
            return Err(ServiceError::InvalidRequest);
        }
        self.release_pending(snapshot.spec().input().identity())
    }

    fn release_pending(&self, key: &SourceIdentifier) -> Result<(), ServiceError> {
        let mut pending = self.pending.lock().map_err(|_| ServiceError::Unavailable)?;
        // A running input remains owned until the original runner exits, including cancellation.
        if pending.get(key).is_some_and(Option::is_some) {
            pending.remove(key);
        }
        Ok(())
    }

    pub(in crate::service) fn belongs_to(
        &self,
        snapshot: &JobSnapshot,
        selection_token: &str,
    ) -> bool {
        snapshot_origin(snapshot.spec()).is_ok()
            && snapshot.spec().input().identity().as_str()
                == format!(
                    "evidence:{selection_token}:{}",
                    encode_digest(snapshot.spec().input().digest().bytes())
                )
    }

    /// Reopening uses the completed job's exact bounded artifact; it performs no source selection.
    pub(in crate::service) async fn read_result(
        &self,
        snapshot: &JobSnapshot,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        ensure_live(context)?;
        let origin = snapshot_origin(snapshot.spec())?;
        if context.origin() != Some(origin) {
            return Err(ServiceError::Unauthorized);
        }
        if snapshot.state() != JobState::Completed {
            return Err(ServiceError::InvalidRequest);
        }
        let reference = snapshot
            .terminal_result()
            .ok_or(ServiceError::InvalidResult)?;
        let [artifact] = reference.artifacts() else {
            return Err(ServiceError::InvalidResult);
        };
        if reference.authority().as_str() != RESULT
            || reference.identity().as_str() != format!("evidence-result-{}", artifact.sha256())
            || encode_digest(reference.evidence_digest().bytes()) != artifact.sha256()
            || reference.evidence_digest().algorithm() != DigestAlgorithm::Sha256
            || artifact.media_type() != "application/json"
        {
            return Err(ServiceError::InvalidResult);
        }
        let maximum = NonZeroUsize::new(context.limits().maximum_result_bytes())
            .ok_or(ServiceError::InvalidRequest)?;
        let read = self
            .artifacts
            .read(
                ArtifactReadRequest::try_new(artifact.clone(), maximum).map_err(map_artifact)?,
                ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
            )
            .await
            .map_err(map_artifact)?;
        if read.reference() != artifact || digest(read.content()) != reference.evidence_digest() {
            return Err(ServiceError::InvalidResult);
        }
        let value: Value =
            serde_json::from_slice(read.content()).map_err(|_| ServiceError::InvalidResult)?;
        validate_json_contract(&value, context.limits().result_structure(), maximum.get())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        let output: PreparedResult =
            serde_json::from_value(value).map_err(|_| ServiceError::InvalidResult)?;
        output.validate_binding(snapshot.spec())?;
        self.validate_result(snapshot, &output, output.input.service_limits()?)?;
        let result = self.validate_result(snapshot, &output, context.limits())?;
        ensure_live(context)?;
        Ok(result)
    }

    fn validate_result(
        &self,
        snapshot: &JobSnapshot,
        output: &PreparedResult,
        limits: ServiceLimits,
    ) -> Result<Value, ServiceError> {
        let request = output.input.request()?;
        let body = &output.preparation;
        let instrument = output.input.instrument_id.to_string();
        if body["scope"].as_str() != Some(request.purpose.scope())
            || body["financialConfigurationDigest"].as_str()
                != Some(request.financial_profile.configuration_digest.as_str())
            || (!body["instrumentId"].is_null()
                && body["instrumentId"].as_str() != Some(instrument.as_str()))
        {
            return Err(ServiceError::InvalidResult);
        }
        if let Some(member) = body.get("findMemberUnavailable") {
            let retained: crate::application::decision::current_find::member::FindMemberUnavailableValue =
                serde_json::from_value(member.clone()).map_err(|_| ServiceError::InvalidResult)?;
            retained
                .validate()
                .map_err(|_| ServiceError::InvalidResult)?;
            if request.find_member.as_ref() != Some(&retained.member) {
                return Err(ServiceError::InvalidResult);
            }
        }
        let public = json!({
            "job": JobReceipt::from_snapshot(snapshot),
            "preparation": body,
            "arguments": output.input.arguments,
            "requestSha256": output.request_sha256,
        });
        let result = TypedToolResult::try_new(
            public,
            1,
            ToolResultMetadata::complete_not_applicable(),
            limits,
        )?;
        result.validate_for(&self.result_descriptor)?;
        Ok(json!({
            "preparation": body,
            "arguments": output.input.arguments,
            "requestSha256": output.request_sha256,
        }))
    }

    fn execute<'a>(
        &'a self,
        context: &'a JobRunContext,
        pending: Pending,
    ) -> BoxFuture<'a, Result<JobCompletion, JobRunError>> {
        Box::pin(self.execute_impl(context, pending))
    }

    async fn execute_impl(
        &self,
        context: &JobRunContext,
        pending: Pending,
    ) -> Result<JobCompletion, JobRunError> {
        pending
            .recorded
            .validate(context.snapshot().spec())
            .map_err(map_service)?;
        let deadline = Instant::now()
            .checked_add(self.run_timeout)
            .ok_or(JobRunError::Recovery)?;
        let owned = owned_context(
            context.snapshot().spec(),
            &pending.recorded,
            context.cancellation().clone(),
            deadline,
        )
        .map_err(map_service)?;
        let input = pending.recorded.request().map_err(map_service)?;
        let models = self
            .forecast
            .financial_profile_catalog(&input.financial_profile.configuration, &owned)
            .await
            .map_err(map_service)?;
        let progress = JobProgress::try_new(
            identifier("preparing-investment-evidence").map_err(map_service)?,
            0,
            None,
            context.snapshot().updated_at_timestamp(),
        )
        .map_err(|_| JobRunError::Recovery)?;
        let progressed = context
            .events()
            .append(JobRunnerEvent::Progress(progress))
            .await
            .map_err(|_| failed("investment-evidence-progress-unavailable", true))?;
        let response = self.preparation.prepare(input, &owned, models.as_ref()).await.map_err(|error| {
            tracing::warn!(operation = PREPARE, error = ?error, job_id = %context.snapshot().id().as_uuid(), "investment evidence preparation failed");
            map_service(error)
        })?;
        let output = PreparedResult {
            schema_version: RESULT.to_owned(),
            job_id: context.snapshot().id().as_uuid(),
            generation: context.snapshot().generation().get(),
            request_sha256: pending.recorded.request_sha256().map_err(map_service)?,
            input: pending.recorded,
            preparation: response.structured_content().clone(),
        };
        self.validate_result(context.snapshot(), &output, owned.limits())
            .map_err(map_service)?;
        let value = serde_json::to_value(&output).map_err(|_| JobRunError::Recovery)?;
        validate_json_contract(
            &value,
            owned.limits().result_structure(),
            owned.limits().maximum_result_bytes(),
        )
        .map_err(|_| failed("investment-evidence-result-invalid", false))?;
        let bytes = serde_json::to_vec(&value).map_err(|_| JobRunError::Recovery)?;
        let publication = ArtifactPublication::try_json(bytes)
            .map_err(map_artifact)
            .map_err(map_service)?;
        let result_digest = digest(publication.content());
        // This complete artifact is still only staged evidence until this job wins completion.
        let artifact = self
            .artifacts
            .publish(
                publication,
                ArtifactPublicationContext::new(owned.cancellation().clone(), deadline),
            )
            .await
            .map_err(map_artifact)
            .map_err(map_service)?;
        if artifact.sha256() != encode_digest(result_digest.bytes())
            || artifact.media_type() != "application/json"
        {
            return Err(failed("investment-evidence-artifact-invalid", false));
        }
        let result = JobResultReference::try_new(
            identifier(RESULT).map_err(map_service)?,
            identifier(&format!("evidence-result-{}", artifact.sha256())).map_err(map_service)?,
            result_digest,
            vec![artifact],
        )
        .map_err(|_| JobRunError::Recovery)?;
        // Revalidate current exact profile/selection/identity after staging, without reacquisition.
        let input = output.input.request().map_err(map_service)?;
        let models = self
            .forecast
            .financial_profile_catalog(&input.financial_profile.configuration, &owned)
            .await
            .map_err(map_service)?;
        crate::application::analytical_profile::revalidate(
            &input.financial_profile,
            models.as_ref(),
        )
        .map_err(ServiceError::from)
        .map_err(map_service)?;
        if output.preparation["status"] == "prepared" {
            let current = self
                .preparation
                .admit_preparation(&input, &owned, models.as_ref())
                .await
                .map_err(map_service)?;
            if current.instrument_id != output.input.instrument_id {
                return Err(failed("investment-evidence-selection-changed", false));
            }
            if let Some(identity) = &pending.identity {
                self.preparation
                    .identities
                    .verify_restart(identity, deadline, owned.cancellation())
                    .map_err(map_identity_error)
                    .map_err(map_service)?;
            }
        }
        ensure_live(&owned).map_err(map_service)?;
        let published = context
            .claim_terminal_publication(progressed.sequence())?
            .seal();
        Ok(JobCompletion::Published(result, published))
    }
}

#[async_trait]
impl JobRunner for InvestmentEvidenceJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        snapshot_origin(context.snapshot().spec()).map_err(map_service)?;
        let key = context.snapshot().spec().input().identity().clone();
        let pending = self
            .pending
            .lock()
            .map_err(|_| JobRunError::Recovery)?
            .get_mut(&key)
            .and_then(Option::take)
            .ok_or(JobRunError::Recovery)?;
        let _lease = RunningAdmission { runner: self, key };
        if context.cancellation().is_cancelled() {
            return Err(JobRunError::Cancelled);
        }
        self.execute(&context, pending).await
    }

    async fn recover(&self, _snapshot: &JobSnapshot) -> JobRecoveryDisposition {
        JobRecoveryDisposition::MarkInterrupted
    }
}

struct RunningAdmission<'a> {
    runner: &'a InvestmentEvidenceJobRunner,
    key: SourceIdentifier,
}
impl Drop for RunningAdmission<'_> {
    fn drop(&mut self) {
        self.runner
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.key);
    }
}

fn snapshot_origin(spec: &AdmittedJobSpec) -> Result<RequestOrigin, ServiceError> {
    if spec.kind().as_str() != KIND
        || spec.input().authority().as_str() != INPUT
        || spec.authority().authority().as_str() != RESULT
        || spec.authority().identity().as_str() != RESULT
        || spec.authority().digest() != digest(RESULT.as_bytes())
        || spec.input().digest().algorithm() != DigestAlgorithm::Sha256
        || spec.attempt_limit().get() != 1
        || spec.generation().get() != 1
    {
        return Err(ServiceError::InvalidRequest);
    }
    let workspace = Uuid::parse_str(spec.origin().workspace().as_str())
        .map_err(|_| ServiceError::InvalidRequest)?;
    let client = Uuid::parse_str(spec.origin().client().as_str())
        .map_err(|_| ServiceError::InvalidRequest)?;
    if spec.origin().workspace().as_str() != workspace.to_string()
        || spec.origin().client().as_str() != client.to_string()
    {
        return Err(ServiceError::InvalidRequest);
    }
    RequestOrigin::try_new(workspace, client).map_err(|_| ServiceError::InvalidRequest)
}

fn owned_context(
    spec: &AdmittedJobSpec,
    input: &RecordedInput,
    cancellation: CancellationToken,
    deadline: Instant,
) -> Result<RequestContext, ServiceError> {
    input.validate(spec)?;
    Ok(RequestContext::new(
        spec.request_id().clone(),
        cancellation,
        deadline,
        input.service_limits()?,
    )
    .with_origin(input.origin()?))
}
fn identifier(value: &str) -> Result<SourceIdentifier, ServiceError> {
    SourceIdentifier::try_from(value).map_err(|_| ServiceError::InvalidRequest)
}
fn digest(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}
fn failed(code: &str, retryable: bool) -> JobRunError {
    match identifier(code) {
        Ok(code) => JobRunError::Failed(JobFailure::new(code.clone(), code, retryable)),
        Err(_) => JobRunError::Recovery,
    }
}
fn map_service(error: ServiceError) -> JobRunError {
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
fn map_artifact(error: ArtifactError) -> ServiceError {
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

#[cfg(test)]
mod tests {
    use super::*;
    use market_squawk_jobs::{JobId, JobOrigin};
    use market_squawk_services::RequestId;

    // The controller's receipt tests do not exercise this runner's retained origin or lifetime.
    #[test]
    fn owned_preparation_lifetime_and_retained_origin_generation_are_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = crate::application::analytical_profile::resolve(None, None)?;
        let recorded = RecordedInput {
            schema_version: INPUT.to_owned(),
            operation: PREPARE.to_owned(),
            arguments: json!({
                "selectionToken": "market_00000000000000000000000000000001",
                "financialProfile": profile.resolution(),
            }),
            workspace_id: Uuid::from_u128(2),
            client_id: Uuid::from_u128(3),
            request_id: json!("original-preparation-request"),
            limits: [128 * 1024, 16, 256 * 1024, 32, 32, 128 * 1024, 1024, 1024],
            captured_at: Timestamp::from_unix_nanos(1),
            instrument_id: InstrumentId::try_from(Uuid::from_u128(1))?,
            identity: None,
        };
        let origin = recorded.origin()?;
        let spec = JobAdmission::new(
            identifier(KIND)?,
            AdmittedJobInput::new(
                identifier(INPUT)?,
                recorded.identity_key()?,
                recorded.digest()?,
            ),
            JobAuthoritySnapshot::new(
                identifier(RESULT)?,
                identifier(RESULT)?,
                digest(RESULT.as_bytes()),
                recorded.captured_at,
            ),
            JobAttemptLimit::try_new(1)?,
        )
        .into_spec(
            JobId::try_from_uuid(Uuid::from_u128(4))?,
            JobOrigin::new(
                identifier(&origin.workspace_id().to_string())?,
                identifier(&origin.client_id().to_string())?,
            ),
            RequestId::try_string("original-preparation-request")?,
            recorded.captured_at,
        )?;
        let request_cancel = CancellationToken::new();
        request_cancel.cancel();
        let request = RequestContext::new(
            spec.request_id().clone(),
            request_cancel,
            Instant::now() - Duration::from_secs(1),
            recorded.service_limits()?,
        )
        .with_origin(origin);
        let job_cancel = CancellationToken::new();
        let owned = owned_context(
            &spec,
            &recorded,
            job_cancel.clone(),
            Instant::now() + Duration::from_secs(60),
        )?;
        assert!(matches!(
            ensure_live(&request),
            Err(ServiceError::Cancelled)
        ));
        ensure_live(&owned)?;
        assert_eq!(owned.origin(), request.origin());
        assert_eq!(owned.limits(), request.limits());
        assert_eq!(owned.request_id(), request.request_id());
        job_cancel.cancel();
        assert!(matches!(ensure_live(&owned), Err(ServiceError::Cancelled)));

        let result = PreparedResult {
            schema_version: RESULT.to_owned(),
            job_id: spec.id().as_uuid(),
            generation: 1,
            request_sha256: recorded.request_sha256()?,
            input: recorded,
            preparation: json!({}),
        };
        let mut reopened: PreparedResult = serde_json::from_slice(&serde_json::to_vec(&result)?)?;
        reopened.validate_binding(&spec)?;
        reopened.input.client_id = Uuid::from_u128(5);
        assert!(matches!(
            reopened.validate_binding(&spec),
            Err(ServiceError::InvalidResult)
        ));
        reopened.input.client_id = origin.client_id();
        reopened.generation = 2;
        assert!(matches!(
            reopened.validate_binding(&spec),
            Err(ServiceError::InvalidResult)
        ));
        Ok(())
    }
}
