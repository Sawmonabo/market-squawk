//! Retained historical fiscal pages, target jobs and exact source-owner replay.

use market_squawk_services::RequestId;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

use super::{
    DriverState, InvocationAuthority, MAXIMUM_FISCAL_TARGETS, PendingCapabilityInvocation, Receipt,
    Step, WorkflowCheckpointStage, WorkflowError, WorkflowGeneration, WorkflowRun, WorkflowState,
    append_checkpoint, call, checkpoint_body, generation_number, hex_digest, job_from_receipt,
    object, valid_digest, workflow_control,
};

// Keep the producer's closed source-action reference opaque and byte-for-byte in the plan.
// Backend plan admission and dataset builds reopen its original source evidence.
pub(super) fn historical_study_receipt(receipt: &Receipt) -> Result<(), WorkflowError> {
    if receipt
        .arguments
        .get("sourceActionReference")
        .is_none_or(|value| !value.is_object())
    {
        return Err(WorkflowError::internal());
    }
    match receipt.body.get("status").and_then(Value::as_str) {
        Some("available") => {
            let plan = receipt
                .body
                .get("plan")
                .ok_or_else(WorkflowError::internal)?;
            if [
                "sourceActionReference",
                "subjectInstrumentId",
                "sourceCutoffUnixNanos",
                "financialProfile",
            ]
            .iter()
            .any(|key| plan.get(*key) != receipt.arguments.get(*key))
                || plan
                    .get("planDigest")
                    .and_then(Value::as_array)
                    .is_none_or(|bytes| {
                        bytes.len() != 32
                            || bytes
                                .iter()
                                .any(|byte| byte.as_u64().is_none_or(|value| value > 255))
                    })
                || receipt
                    .body
                    .get("folds")
                    .and_then(Value::as_array)
                    .is_none_or(|folds| folds.len() != 3)
            {
                return Err(WorkflowError::internal());
            }
        }
        Some("unavailable") => {
            if receipt
                .body
                .get("reasons")
                .and_then(Value::as_array)
                .is_none_or(|reasons| {
                    reasons.is_empty()
                        || reasons
                            .iter()
                            .any(|reason| reason.as_str().is_none_or(str::is_empty))
                })
            {
                return Err(WorkflowError::internal());
            }
        }
        _ => return Err(WorkflowError::internal()),
    }
    Ok(())
}

// These are closed custody DTOs for the existing service's page wire. They grant no source,
// calendar, model or financial authority: completion and replay physically reopen their owners.
const HISTORICAL_PAGE_SIZE: usize = 128;
const MAXIMUM_HISTORICAL_PAGES: usize = 32;
const MAXIMUM_PAGE_ARGUMENT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct HistoricalJob {
    job_id: Uuid,
    generation: u64,
}
impl HistoricalJob {
    fn from_receipt(receipt: &Receipt) -> Result<Self, WorkflowError> {
        let job = job_from_receipt(receipt)?;
        let result = Self {
            job_id: job.job_id,
            generation: generation_number(&job)?,
        };
        if !result.valid() {
            return Err(WorkflowError::internal());
        }
        Ok(result)
    }
    fn valid(&self) -> bool {
        !self.job_id.is_nil() && self.generation > 0
    }
    fn arguments(&self) -> Result<Map<String, Value>, WorkflowError> {
        object(json!(self))
    }
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct HistoricalPriceInputs {
    dataset_id: String,
    build_spec: [u8; 32],
    manifest_version: u64,
    manifest_hash: [u8; 32],
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct HistoricalBinding {
    // Field order matches the existing artifact owner's canonical binding commitment.
    plan_identity: [u8; 32],
    profile_identity: [u8; 32],
    subject: Uuid,
    source_cutoff: i64,
    evaluation_start: i64,
    evaluation_end: i64,
    price_inputs: HistoricalPriceInputs,
    study_input_job: HistoricalJob,
    total_origins: usize,
    epoch_set_digest: [u8; 32],
}
impl HistoricalBinding {
    fn digest(&self) -> Result<[u8; 32], WorkflowError> {
        Ok(Sha256::digest(serde_json::to_vec(self).map_err(|_| WorkflowError::internal())?).into())
    }
    fn valid(&self, work: &DriverState) -> bool {
        self.plan_identity != [0; 32]
            && self.profile_identity != [0; 32]
            && self.epoch_set_digest != [0; 32]
            && Some(self.subject) == work.instrument_id
            && work
                .source_cutoff
                .as_deref()
                .and_then(|s| s.parse::<i64>().ok())
                == Some(self.source_cutoff)
            && self.evaluation_start < self.evaluation_end
            && self.evaluation_end <= self.source_cutoff
            && self.total_origins > 0
            && self.total_origins <= HISTORICAL_PAGE_SIZE * MAXIMUM_HISTORICAL_PAGES
            && self.study_input_job.valid()
            && work
                .receipt(Step::StudyInputs)
                .and_then(HistoricalJob::from_receipt)
                .is_ok_and(|job| job == self.study_input_job)
            && super::super::valid_identifier(&self.price_inputs.dataset_id, 256)
            && self.price_inputs.build_spec != [0; 32]
            && self.price_inputs.manifest_hash != [0; 32]
            && self.price_inputs.manifest_version > 0
    }
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct HistoricalOrigin {
    pub(super) price_example_id: String,
    epoch_identity: [u8; 32],
    economic_origin_unix_nanos: String,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct HistoricalPage {
    binding: HistoricalBinding,
    page_ordinal: usize,
    total_origins: usize,
    total_pages: usize,
    epoch_set_digest: [u8; 32],
    origins: Vec<HistoricalOrigin>,
}
impl HistoricalPage {
    fn valid(&self, work: &DriverState) -> bool {
        self.binding.valid(work)
            && self.total_origins == self.binding.total_origins
            && self.epoch_set_digest == self.binding.epoch_set_digest
            && self.total_pages == self.total_origins.div_ceil(HISTORICAL_PAGE_SIZE)
            && self.page_ordinal < self.total_pages
            && self.total_pages <= MAXIMUM_HISTORICAL_PAGES
            && self.origins.len()
                == (self.total_origins - self.page_ordinal * HISTORICAL_PAGE_SIZE)
                    .min(HISTORICAL_PAGE_SIZE)
            && self.origins.iter().all(|o| {
                super::super::valid_identifier(&o.price_example_id, 256)
                    && o.epoch_identity != [0; 32]
                    && super::super::valid_timestamp(&o.economic_origin_unix_nanos)
            })
            && self
                .origins
                .windows(2)
                .all(|pair| pair[0].epoch_identity < pair[1].epoch_identity)
            && self
                .origins
                .iter()
                .map(|o| &o.price_example_id)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == self.origins.len()
    }
    fn digest(&self) -> Result<String, WorkflowError> {
        Ok(hex_digest(Sha256::digest(
            serde_json::to_vec(self).map_err(|_| WorkflowError::internal())?,
        )))
    }
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct HistoricalArtifact {
    artifact_id: String,
    sha256: String,
    byte_count: usize,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct HistoricalPageReference {
    binding_digest: [u8; 32],
    page_ordinal: usize,
    origin_count: usize,
    artifact: HistoricalArtifact,
}
impl HistoricalPageReference {
    fn valid(&self, binding: &HistoricalBinding, ordinal: usize) -> bool {
        ordinal < binding.total_origins.div_ceil(HISTORICAL_PAGE_SIZE)
            && self.page_ordinal == ordinal
            && binding
                .digest()
                .is_ok_and(|digest| digest == self.binding_digest)
            && self.origin_count
                == (binding.total_origins - ordinal * HISTORICAL_PAGE_SIZE)
                    .min(HISTORICAL_PAGE_SIZE)
            && super::super::valid_identifier(&self.artifact.artifact_id, 256)
            && valid_digest(&self.artifact.sha256)
            && self.artifact.byte_count > 0
            && self.artifact.byte_count <= 4_854_016
            && serde_json::to_vec(self).is_ok_and(|bytes| bytes.len() <= 1024)
    }
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct AcknowledgedHistoricalPage {
    pub(super) reference: HistoricalPageReference,
    descriptor_sha256: String,
    last_epoch_identity: [u8; 32],
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct HistoricalTargetJobs {
    price_example_id: String,
    target_id: String,
    training_dataset_job: HistoricalJob,
    input_dataset_job: HistoricalJob,
    training_job: HistoricalJob,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CompletedHistoricalTarget {
    jobs: HistoricalTargetJobs,
    // Reopen the exact completed job generation and compare its original result before reuse.
    training_dataset_sha256: String,
    input_dataset_sha256: String,
    training_sha256: String,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct HistoricalCurrentPage {
    descriptor: HistoricalPage,
    origin_index: usize,
    target_index: usize,
    ready: Vec<bool>,
    completed: Vec<CompletedHistoricalTarget>,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct HistoricalFiscalProgress {
    binding: Option<HistoricalBinding>,
    targets: Vec<Value>,
    pub(super) pages: Vec<AcknowledgedHistoricalPage>,
    current: Option<HistoricalCurrentPage>,
    pub(super) revalidate_index: usize,
}
impl HistoricalFiscalProgress {
    pub(super) fn new() -> Self {
        Self {
            binding: None,
            targets: Vec::new(),
            pages: Vec::new(),
            current: None,
            revalidate_index: 0,
        }
    }
    pub(super) fn current_origin(&self) -> Result<&HistoricalOrigin, WorkflowError> {
        let page = self.current.as_ref().ok_or_else(WorkflowError::internal)?;
        page.descriptor
            .origins
            .get(page.origin_index)
            .ok_or_else(WorkflowError::internal)
    }
    pub(super) fn complete(&self) -> bool {
        self.current.is_none()
            && self.binding.as_ref().is_some_and(|binding| {
                self.pages.len() == binding.total_origins.div_ceil(HISTORICAL_PAGE_SIZE)
            })
    }
    pub(super) fn valid(&self, work: &DriverState) -> bool {
        let Some(binding) = &self.binding else {
            return self.targets.is_empty()
                && self.pages.is_empty()
                && self.current.is_none()
                && self.revalidate_index == 0;
        };
        if !binding.valid(work)
            || !valid_historical_targets(&self.targets)
            || self.pages.len() > MAXIMUM_HISTORICAL_PAGES
            || self.pages.len() > binding.total_origins.div_ceil(HISTORICAL_PAGE_SIZE)
            || self.pages.iter().enumerate().any(|(ordinal, page)| {
                !page.reference.valid(binding, ordinal)
                    || !valid_digest(&page.descriptor_sha256)
                    || page.last_epoch_identity == [0; 32]
            })
            || self
                .pages
                .windows(2)
                .any(|pair| pair[0].last_epoch_identity >= pair[1].last_epoch_identity)
            || self.revalidate_index
                > self.pages.len()
                    + self
                        .current
                        .as_ref()
                        .map_or(0, |p| p.completed.len() * 3 + 1)
        {
            return false;
        }
        let Some(page) = &self.current else {
            return true;
        };
        if !page.descriptor.valid(work)
            || page.descriptor.binding != *binding
            || page.descriptor.page_ordinal != self.pages.len()
            || page.origin_index > page.descriptor.origins.len()
            || page.target_index > MAXIMUM_FISCAL_TARGETS
            || !(page.ready.is_empty() || page.ready.len() == MAXIMUM_FISCAL_TARGETS)
            || page.completed.len() > HISTORICAL_PAGE_SIZE * MAXIMUM_FISCAL_TARGETS
            || self.pages.last().is_some_and(|prior| {
                page.descriptor
                    .origins
                    .first()
                    .is_none_or(|origin| origin.epoch_identity <= prior.last_epoch_identity)
            })
        {
            return false;
        }
        let origin_indices = page
            .descriptor
            .origins
            .iter()
            .enumerate()
            .map(|(index, origin)| (origin.price_example_id.as_str(), index))
            .collect::<BTreeMap<_, _>>();
        let target_indices = self
            .targets
            .iter()
            .enumerate()
            .filter_map(|(index, target)| {
                target
                    .get("targetId")
                    .and_then(Value::as_str)
                    .map(|id| (id, index))
            })
            .collect::<BTreeMap<_, _>>();
        let mut previous = None;
        let mut jobs = std::collections::BTreeSet::new();
        for completed in &page.completed {
            let Some(&origin) = origin_indices.get(completed.jobs.price_example_id.as_str()) else {
                return false;
            };
            let Some(&target) = target_indices.get(completed.jobs.target_id.as_str()) else {
                return false;
            };
            let coordinate = (origin, target);
            if coordinate >= (page.origin_index, page.target_index)
                || previous.is_some_and(|p| p >= coordinate)
                || ![
                    &completed.training_dataset_sha256,
                    &completed.input_dataset_sha256,
                    &completed.training_sha256,
                ]
                .into_iter()
                .all(|s| valid_digest(s))
            {
                return false;
            }
            for job in [
                &completed.jobs.training_dataset_job,
                &completed.jobs.input_dataset_job,
                &completed.jobs.training_job,
            ] {
                if !job.valid() || !jobs.insert((job.job_id, job.generation)) {
                    return false;
                }
            }
            previous = Some(coordinate);
        }
        true
    }
}
fn valid_historical_targets(targets: &[Value]) -> bool {
    let mut ids = std::collections::BTreeSet::new();
    targets.len() == MAXIMUM_FISCAL_TARGETS
        && targets.iter().all(|target| {
            target.as_object().is_some_and(|object| object.len() == 6)
                && [
                    "targetId",
                    "role",
                    "basis",
                    "shareConvention",
                    "cadence",
                    "periodsAhead",
                ]
                .iter()
                .all(|key| target.get(*key).is_some())
                && target
                    .get("targetId")
                    .and_then(Value::as_str)
                    .is_some_and(|id| super::super::valid_identifier(id, 96) && ids.insert(id))
                && serde_json::to_vec(target).is_ok_and(|bytes| bytes.len() <= 1024)
        })
}
pub(super) fn historical_plan_arguments(
    work: &DriverState,
) -> Result<Map<String, Value>, WorkflowError> {
    let mut arguments = work.receipt(Step::HistoricalStudy)?.arguments.clone();
    arguments.insert(
        "studyInputJob".into(),
        json!(HistoricalJob::from_receipt(
            work.receipt(Step::StudyInputs)?
        )?),
    );
    Ok(arguments)
}
pub(super) fn historical_target_arguments(work: &DriverState) -> Result<Value, WorkflowError> {
    let fiscal = work
        .historical_fiscal
        .as_ref()
        .ok_or_else(WorkflowError::internal)?;
    let page = fiscal
        .current
        .as_ref()
        .ok_or_else(WorkflowError::internal)?;
    if page.ready.get(page.target_index) != Some(&true) {
        return Err(WorkflowError::internal());
    }
    Ok(
        json!({"studyInputJob":HistoricalJob::from_receipt(work.receipt(Step::StudyInputs)?)?,
        "priceExampleId":fiscal.current_origin()?.price_example_id,
        "targetId":fiscal.targets.get(page.target_index).and_then(|t| t.get("targetId")).ok_or_else(WorkflowError::internal)?}),
    )
}
pub(super) fn historical_completion_arguments(work: &DriverState) -> Result<Value, WorkflowError> {
    let fiscal = work
        .historical_fiscal
        .as_ref()
        .ok_or_else(WorkflowError::internal)?;
    let page = fiscal
        .current
        .as_ref()
        .ok_or_else(WorkflowError::internal)?;
    if page.origin_index != page.descriptor.origins.len() || !page.ready.is_empty() {
        return Err(WorkflowError::internal());
    }
    Ok(
        json!({"plan":work.receipt(Step::HistoricalStudy)?.body.get("plan").ok_or_else(WorkflowError::internal)?,
        "studyInputJob":HistoricalJob::from_receipt(work.receipt(Step::StudyInputs)?)?,
        "pageOrdinal":page.descriptor.page_ordinal,
        "fiscalJobs":page.completed.iter().map(|target| &target.jobs).collect::<Vec<_>>()}),
    )
}
pub(super) fn apply_historical_page(
    work: &mut DriverState,
    receipt: &Receipt,
) -> Result<(), WorkflowError> {
    historical_study_receipt(receipt)?;
    if receipt.body.get("status").and_then(Value::as_str) != Some("available")
        || receipt.body.get("plan") != work.receipt(Step::HistoricalStudy)?.body.get("plan")
    {
        return Err(WorkflowError::internal());
    }
    let page: HistoricalPage = serde_json::from_value(
        receipt
            .body
            .pointer("/fiscal/page")
            .cloned()
            .ok_or_else(WorkflowError::internal)?,
    )
    .map_err(|_| WorkflowError::internal())?;
    let targets = receipt
        .body
        .pointer("/fiscal/targets")
        .and_then(Value::as_array)
        .ok_or_else(WorkflowError::internal)?;
    if !page.valid(work) || !valid_historical_targets(targets) {
        return Err(WorkflowError::internal());
    }
    let fiscal = work
        .historical_fiscal
        .as_mut()
        .ok_or_else(WorkflowError::internal)?;
    if fiscal.current.is_some()
        || page.page_ordinal != fiscal.pages.len()
        || fiscal
            .binding
            .as_ref()
            .is_some_and(|binding| *binding != page.binding)
        || (!fiscal.targets.is_empty() && fiscal.targets != *targets)
        || fiscal.pages.last().is_some_and(|prior| {
            page.origins
                .first()
                .is_none_or(|o| o.epoch_identity <= prior.last_epoch_identity)
        })
    {
        return Err(WorkflowError::internal());
    }
    fiscal.binding = Some(page.binding.clone());
    fiscal.targets = targets.clone();
    fiscal.current = Some(HistoricalCurrentPage {
        descriptor: page,
        origin_index: 0,
        target_index: 0,
        ready: Vec::new(),
        completed: Vec::new(),
    });
    fiscal.revalidate_index = 0;
    work.step = Step::StudyFiscalOrigin;
    Ok(())
}
pub(super) fn apply_historical_origin(
    work: &mut DriverState,
    receipt: &Receipt,
) -> Result<(), WorkflowError> {
    historical_study_receipt(receipt)?;
    if receipt.body.get("status").and_then(Value::as_str) != Some("available")
        || receipt.body.get("plan") != work.receipt(Step::HistoricalStudy)?.body.get("plan")
    {
        return Err(WorkflowError::internal());
    }
    let fiscal = work
        .historical_fiscal
        .as_mut()
        .ok_or_else(WorkflowError::internal)?;
    if receipt
        .body
        .pointer("/fiscal/priceExampleId")
        .and_then(Value::as_str)
        != Some(fiscal.current_origin()?.price_example_id.as_str())
    {
        return Err(WorkflowError::internal());
    }
    let targets = receipt
        .body
        .pointer("/fiscal/targets")
        .and_then(Value::as_array)
        .filter(|targets| targets.len() == MAXIMUM_FISCAL_TARGETS)
        .ok_or_else(WorkflowError::internal)?;
    let mut ready = Vec::with_capacity(MAXIMUM_FISCAL_TARGETS);
    for (actual, expected) in targets.iter().zip(&fiscal.targets) {
        if actual.as_object().is_none_or(|row| row.len() != 2)
            || actual.get("target") != Some(expected)
        {
            return Err(WorkflowError::internal());
        }
        ready.push(match actual.get("availability").and_then(Value::as_str) {
            Some("ready") => true,
            Some("unavailable") => false,
            _ => return Err(WorkflowError::internal()),
        });
    }
    let page = fiscal
        .current
        .as_mut()
        .ok_or_else(WorkflowError::internal)?;
    if !page.ready.is_empty() || page.target_index != 0 {
        return Err(WorkflowError::internal());
    }
    page.ready = ready;
    advance_historical_target(work)
}
fn advance_historical_target(work: &mut DriverState) -> Result<(), WorkflowError> {
    let page = work
        .historical_fiscal
        .as_mut()
        .and_then(|fiscal| fiscal.current.as_mut())
        .ok_or_else(WorkflowError::internal)?;
    while page.target_index < MAXIMUM_FISCAL_TARGETS
        && page.ready.get(page.target_index) == Some(&false)
    {
        page.target_index += 1;
    }
    if page.target_index < MAXIMUM_FISCAL_TARGETS {
        if page.ready.get(page.target_index) != Some(&true) {
            return Err(WorkflowError::internal());
        }
        work.step = Step::StudyFiscalTrainingDataset;
    } else {
        page.origin_index += 1;
        page.target_index = 0;
        page.ready.clear();
        work.step = if page.origin_index == page.descriptor.origins.len() {
            Step::StudyFiscalCompletePage
        } else {
            Step::StudyFiscalOrigin
        };
    }
    Ok(())
}
pub(super) fn retain_historical_target(
    work: &mut DriverState,
    inputs: &Receipt,
) -> Result<(), WorkflowError> {
    let training_dataset = work.receipt(Step::StudyFiscalTrainingDataset)?;
    let training = work.receipt(Step::StudyFiscalTraining)?;
    let fiscal = work
        .historical_fiscal
        .as_ref()
        .ok_or_else(WorkflowError::internal)?;
    let page = fiscal
        .current
        .as_ref()
        .ok_or_else(WorkflowError::internal)?;
    let completed = CompletedHistoricalTarget {
        jobs: HistoricalTargetJobs {
            price_example_id: fiscal.current_origin()?.price_example_id.clone(),
            target_id: fiscal
                .targets
                .get(page.target_index)
                .and_then(|t| t.get("targetId"))
                .and_then(Value::as_str)
                .ok_or_else(WorkflowError::internal)?
                .to_owned(),
            training_dataset_job: HistoricalJob::from_receipt(training_dataset)?,
            input_dataset_job: HistoricalJob::from_receipt(inputs)?,
            training_job: HistoricalJob::from_receipt(training)?,
        },
        training_dataset_sha256: training_dataset.sha256.clone(),
        input_dataset_sha256: inputs.sha256.clone(),
        training_sha256: training.sha256.clone(),
    };
    let page = work
        .historical_fiscal
        .as_mut()
        .and_then(|f| f.current.as_mut())
        .ok_or_else(WorkflowError::internal)?;
    if page.completed.len() >= HISTORICAL_PAGE_SIZE * MAXIMUM_FISCAL_TARGETS {
        return Err(historical_capacity());
    }
    page.completed.push(completed);
    page.target_index += 1;
    work.receipts.remove("StudyFiscalTrainingDataset");
    work.receipts.remove("StudyFiscalTraining");
    advance_historical_target(work)
}
pub(super) fn retain_historical_page(
    run: &mut WorkflowRun,
    pending: &PendingCapabilityInvocation,
    body: Value,
) -> Result<(), WorkflowError> {
    let work = run.driver.as_mut().ok_or_else(WorkflowError::internal)?;
    if work.step != Step::StudyFiscalCompletePage || work.active_job.is_some() {
        return Err(WorkflowError::internal());
    }
    let expected = object(historical_completion_arguments(work)?)?;
    if expected
        .iter()
        .any(|(key, value)| pending.arguments.get(key) != Some(value))
        || pending.arguments.get("confirm") != Some(&Value::Bool(true))
        || pending
            .arguments
            .keys()
            .any(|key| !expected.contains_key(key) && key != "confirm" && key != "resultLimits")
        || body.as_object().is_none_or(|object| object.len() != 3)
        || body.get("status").and_then(Value::as_str) != Some("completed")
    {
        return Err(WorkflowError::internal());
    }
    let descriptor: HistoricalPage = serde_json::from_value(
        body.get("page")
            .cloned()
            .ok_or_else(WorkflowError::internal)?,
    )
    .map_err(|_| WorkflowError::internal())?;
    let reference: HistoricalPageReference = serde_json::from_value(
        body.get("fiscalPage")
            .cloned()
            .ok_or_else(WorkflowError::internal)?,
    )
    .map_err(|_| WorkflowError::internal())?;
    let fiscal = work
        .historical_fiscal
        .as_mut()
        .ok_or_else(WorkflowError::internal)?;
    let page = fiscal
        .current
        .as_ref()
        .ok_or_else(WorkflowError::internal)?;
    if page.descriptor != descriptor
        || !reference.valid(&descriptor.binding, fiscal.pages.len())
        || fiscal.pages.len() >= MAXIMUM_HISTORICAL_PAGES
    {
        return Err(WorkflowError::internal());
    }
    fiscal.pages.push(AcknowledgedHistoricalPage {
        reference,
        descriptor_sha256: descriptor.digest()?,
        last_epoch_identity: descriptor
            .origins
            .last()
            .ok_or_else(WorkflowError::internal)?
            .epoch_identity,
    });
    // One transaction retains the receipt before releasing exactly its transient target jobs.
    let completed = page
        .completed
        .iter()
        .flat_map(|target| {
            [
                &target.jobs.training_dataset_job,
                &target.jobs.input_dataset_job,
                &target.jobs.training_job,
            ]
        })
        .map(|job| (job.job_id, job.generation))
        .collect::<std::collections::BTreeSet<_>>();
    run.child_jobs.retain(|job| {
        !job.generation
            .parse::<u64>()
            .ok()
            .is_some_and(|generation| completed.contains(&(job.job_id, generation)))
    });
    fiscal.current = None;
    fiscal.revalidate_index = 0;
    work.step = if fiscal.complete() {
        Step::StudyBacktest
    } else {
        Step::StudyFiscalPage
    };
    Ok(())
}
pub(super) fn compact_historical_frontier(
    run: &mut WorkflowRun,
    now: &str,
) -> Result<(), WorkflowError> {
    let work = run.driver.as_ref().ok_or_else(WorkflowError::internal)?;
    if work.active_job.is_some() {
        return Err(WorkflowError::internal());
    }
    if let Some(page) = work
        .historical_fiscal
        .as_ref()
        .and_then(|fiscal| fiscal.current.as_ref())
    {
        let completed = page
            .completed
            .iter()
            .flat_map(|target| {
                [
                    &target.jobs.training_dataset_job,
                    &target.jobs.input_dataset_job,
                    &target.jobs.training_job,
                ]
            })
            .map(|job| (job.job_id, job.generation))
            .collect::<std::collections::BTreeSet<_>>();
        run.child_jobs.retain(|job| {
            !job.generation
                .parse::<u64>()
                .ok()
                .is_some_and(|generation| completed.contains(&(job.job_id, generation)))
        });
    }
    let created = run
        .checkpoint_journal
        .first()
        .cloned()
        .ok_or_else(WorkflowError::internal)?;
    if created.stage != WorkflowCheckpointStage::Created
        || run
            .child_jobs
            .iter()
            .any(|job| job.terminal_sequence.is_none())
    {
        return Err(WorkflowError::internal());
    }
    run.checkpoint_journal = vec![created];
    for child in run.child_jobs.clone() {
        append_checkpoint(
            run,
            now,
            WorkflowCheckpointStage::CapabilityCompleted,
            Some(child),
            None,
        )?;
    }
    for reference in run.result_references.clone() {
        append_checkpoint(
            run,
            now,
            WorkflowCheckpointStage::ResultsRetained,
            None,
            Some(reference),
        )?;
    }
    Ok(())
}
fn historical_capacity() -> WorkflowError {
    WorkflowError::new(
        "analysis_storage_full",
        "There is not enough capacity to retain this analysis. Saved progress has been preserved.",
    )
}
pub(super) fn check_historical_page_budget(
    generation: &Arc<WorkflowGeneration>,
    run: &WorkflowRun,
) -> Result<(), WorkflowError> {
    let work = run.driver.as_ref().ok_or_else(WorkflowError::internal)?;
    if work.step != Step::StudyFiscalOrigin {
        return Ok(());
    }
    let Some(page) = work
        .historical_fiscal
        .as_ref()
        .and_then(|fiscal| fiscal.current.as_ref())
    else {
        return Err(WorkflowError::internal());
    };
    if page.origin_index != 0 {
        return Ok(());
    }
    let header = object(
        json!({"plan":work.receipt(Step::HistoricalStudy)?.body.get("plan").ok_or_else(WorkflowError::internal)?,
        "studyInputJob":HistoricalJob::from_receipt(work.receipt(Step::StudyInputs)?)?,
        "pageOrdinal":page.descriptor.page_ordinal,"fiscalJobs":[],"confirm":true}),
    )?;
    let header = crate::application::analytical_workflow::host::prepare_analytical_arguments(
        generation,
        "Analysis.CompleteHistoricalStudyFiscalPage",
        header,
        InvocationAuthority::ExactConfirmed("Analysis.CompleteHistoricalStudyFiscalPage"),
    )?;
    let header_bytes = serde_json::to_vec(&header)
        .map_err(|_| WorkflowError::internal())?
        .len();
    // Existing maximum canonical entry is393 bytes for31-byte source example IDs. Reserve
    // the actual source ID's worst JSON escaping too, without fabricating any job/reference.
    let targets = &work
        .historical_fiscal
        .as_ref()
        .ok_or_else(WorkflowError::internal)?
        .targets;
    let entries = page
        .descriptor
        .origins
        .iter()
        .try_fold(0usize, |total, origin| {
            let encoded = serde_json::to_vec(&origin.price_example_id)
                .map_err(|_| WorkflowError::internal())?
                .len();
            targets.iter().try_fold(total, |total, target| {
                let target = target.get("targetId").ok_or_else(WorkflowError::internal)?;
                let target_bytes = serde_json::to_vec(target)
                    .map_err(|_| WorkflowError::internal())?
                    .len();
                let entry = 394usize
                    .checked_add(encoded.saturating_sub(33))
                    .and_then(|n| n.checked_add(target_bytes.saturating_sub(25)))
                    .ok_or_else(historical_capacity)?;
                total.checked_add(entry).ok_or_else(historical_capacity)
            })
        })?;
    if header_bytes
        .checked_add(entries)
        .is_none_or(|bytes| bytes > MAXIMUM_PAGE_ARGUMENT_BYTES)
    {
        return Err(historical_capacity());
    }
    Ok(())
}

pub(super) async fn revalidate_historical_frontier(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    run: &WorkflowRun,
    token: &str,
) -> Result<bool, WorkflowError> {
    let work = run.driver.as_ref().ok_or_else(WorkflowError::internal)?;
    let Some(fiscal) = &work.historical_fiscal else {
        return Ok(false);
    };
    let index = fiscal.revalidate_index;
    if index < fiscal.pages.len() {
        let saved = &fiscal.pages[index];
        let mut arguments = historical_plan_arguments(work)?;
        arguments.insert("pageOrdinal".into(), json!(index));
        arguments.insert("fiscalPage".into(), json!(&saved.reference));
        let body = call(
            generation,
            "Analysis.GetHistoricalStudyPlan",
            arguments,
            false,
            RequestId::try_string(format!(
                "desktop-study-page-reopen-{}",
                Uuid::new_v4().simple()
            ))
            .map_err(|_| WorkflowError::internal())?,
        )
        .await?;
        let page: HistoricalPage = serde_json::from_value(
            body.pointer("/fiscal/page")
                .cloned()
                .ok_or_else(WorkflowError::internal)?,
        )
        .map_err(|_| WorkflowError::internal())?;
        if body.get("status").and_then(Value::as_str) != Some("available")
            || body.get("plan") != work.receipt(Step::HistoricalStudy)?.body.get("plan")
            || body.pointer("/fiscal/fiscalPage") != Some(&json!(saved.reference))
            || body.pointer("/fiscal/targets") != Some(&json!(fiscal.targets))
            || !page.valid(work)
            || page.page_ordinal != index
            || fiscal.binding.as_ref() != Some(&page.binding)
            || page.digest()? != saved.descriptor_sha256
        {
            return Err(WorkflowError::internal());
        }
    } else if let Some(current) = &fiscal.current {
        let offset = index - fiscal.pages.len();
        if offset == 0 {
            let mut arguments = historical_plan_arguments(work)?;
            arguments.insert("pageOrdinal".into(), json!(current.descriptor.page_ordinal));
            let body = call(
                generation,
                "Analysis.GetHistoricalStudyPlan",
                arguments,
                false,
                RequestId::try_string(format!(
                    "desktop-study-current-reopen-{}",
                    Uuid::new_v4().simple()
                ))
                .map_err(|_| WorkflowError::internal())?,
            )
            .await?;
            if body.get("status").and_then(Value::as_str) != Some("available")
                || body.get("plan") != work.receipt(Step::HistoricalStudy)?.body.get("plan")
                || body.pointer("/fiscal/page") != Some(&json!(current.descriptor))
                || body.pointer("/fiscal/targets") != Some(&json!(fiscal.targets))
            {
                return Err(WorkflowError::internal());
            }
        } else {
            let offset = offset - 1;
            let Some(target) = current.completed.get(offset / 3) else {
                return Ok(false);
            };
            let (job, digest, operation) = match offset % 3 {
                0 => (
                    &target.jobs.training_dataset_job,
                    &target.training_dataset_sha256,
                    "Analysis.GetPreparedDatasetJobResult",
                ),
                1 => (
                    &target.jobs.training_job,
                    &target.training_sha256,
                    "Model.GetTrainingJobResult",
                ),
                _ => (
                    &target.jobs.input_dataset_job,
                    &target.input_dataset_sha256,
                    "Analysis.GetPreparedDatasetJobResult",
                ),
            };
            let body = call(
                generation,
                operation,
                job.arguments()?,
                false,
                RequestId::try_string(format!(
                    "desktop-study-job-reopen-{}",
                    Uuid::new_v4().simple()
                ))
                .map_err(|_| WorkflowError::internal())?,
            )
            .await?;
            let body = checkpoint_body(operation, body)?;
            let reference = workflow_control::job_reference(
                body.get("job").ok_or_else(WorkflowError::internal)?,
            )?;
            if reference.job_id != job.job_id
                || generation_number(&reference)? != job.generation
                || hex_digest(Sha256::digest(
                    serde_json::to_vec(&body).map_err(|_| WorkflowError::internal())?,
                )) != *digest
            {
                return Err(WorkflowError::internal());
            }
            // Coordinate-to-build/model admission is rechecked by the original page owner at
            // completion; native never substitutes another generation or interprets this body.
        }
    } else {
        return Ok(false);
    }
    let _fence = generation.analytical_retirement_fence().await;
    state.admit_current(generation)?;
    generation.analytical_controller().mutate(|document, now| {
        let retained = workflow_control::find_workflow_mut(document, token)?;
        let saved = retained
            .driver
            .as_mut()
            .and_then(|work| work.historical_fiscal.as_mut())
            .ok_or_else(WorkflowError::internal)?;
        if saved != fiscal {
            return Err(WorkflowError::internal());
        }
        saved.revalidate_index = index.checked_add(1).ok_or_else(WorkflowError::internal)?;
        retained.updated_at = now;
        Ok(())
    })?;
    Ok(true)
}
