//! Saved Find results, source coverage and partition custody for the native workflow.

use market_squawk_services::RequestId;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

use super::{
    AnalyticalControllerResponse, DriverState, Receipt, ServiceJobReference,
    ServiceResultReference, Step, WorkflowCheckpointStage, WorkflowError, WorkflowGeneration,
    WorkflowKind, WorkflowRun, WorkflowRunState, WorkflowState, call, job_arguments,
    job_from_receipt, object, uuid_field, valid_digest, workflow_control,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct FindCoverage {
    scope: String,
    complete: bool,
    pub(super) canonical_population_count: u32,
    pub(super) population_count: u32,
    pub(super) excluded_count: u32,
    pub(super) unavailable_count: u32,
    prepared_partition_count: usize,
    partition_count: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct FindResultRow {
    candidate_id: String,
    pub(super) action_token: Option<Uuid>,
    analysis_id: Option<String>,
    member_unavailable: Option<FindMemberUnavailable>,
    analysis_state: FindAnalysisState,
    screen_rank: usize,
    rank: usize,
    expected_return: Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct FindMemberContext {
    preparation_id: Uuid,
    preparation_sha256: String,
    candidate_id: String,
    screen_run_id: String,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct FindSourceFailure {
    source: String,
    failure: String,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum FindSourceUnavailableReason {
    IdentityUnavailable,
    SourceEvidenceUnavailable,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct FindMemberUnavailable {
    pub(super) member: FindMemberContext,
    assessment_sha256: String,
    pub(super) reason: FindSourceUnavailableReason,
    source_failures: Vec<FindSourceFailure>,
}
impl FindMemberUnavailable {
    pub(super) fn valid(&self) -> bool {
        !self.member.preparation_id.is_nil()
            && valid_digest(&self.member.preparation_sha256)
            && valid_digest(&self.assessment_sha256)
            && super::super::valid_identifier(&self.member.candidate_id, 256)
            && super::super::valid_identifier(&self.member.screen_run_id, 256)
            && self.source_failures.len() <= 6
            && self.source_failures.iter().all(|f| {
                matches!(
                    f.source.as_str(),
                    "current_session"
                        | "government_history"
                        | "benchmark_history"
                        | "selected_history"
                        | "source_actions"
                        | "equity_premium"
                ) && !f.failure.is_empty()
                    && f.failure.len() <= 1024
            })
            && match self.reason {
                FindSourceUnavailableReason::IdentityUnavailable => self.source_failures.is_empty(),
                FindSourceUnavailableReason::SourceEvidenceUnavailable => {
                    !self.source_failures.is_empty()
                }
            }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum FindAnalysisState {
    Generated,
    NoAction,
    Unavailable,
}
impl FindAnalysisState {
    fn outcome(&self) -> &'static str {
        match self {
            Self::Generated => "action",
            Self::NoAction => "abstain",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum FindOrdering {
    EstimatedGainDescending,
    UnavailableIncomparableHorizons,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct FindResultReference {
    preparation_id: Uuid,
    preparation_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FindBenchmarks {
    primary: String,
    alongside: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct FindResult {
    preparation_id: Uuid,
    preparation_sha256: String,
    result_sha256: String,
    ordering: FindOrdering,
    screen_run_id: Option<String>,
    coverage_reference: FindResultReference,
    pub(super) coverage: FindCoverage,
    pub(super) results: Vec<FindResultRow>,
    benchmarks: FindBenchmarks,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct FindState {
    pub(super) preparation: Value,
    pub(super) partition_count: usize,
    pub(super) partition_index: usize,
    pub(super) pending_dataset_job: Option<ServiceJobReference>,
    pub(super) completed_partition: Option<Receipt>,
    pub(super) candidates: Vec<Value>,
    pub(super) candidate_index: usize,
    pub(super) population_count: u32,
    pub(super) canonical_population_count: u32,
    pub(super) screen_receipt: Option<Receipt>,
}

pub(super) fn find_publication_arguments(run: &WorkflowRun) -> Result<Value, WorkflowError> {
    let work = run.driver.as_ref().ok_or_else(WorkflowError::internal)?;
    let find = work.find.as_ref().ok_or_else(WorkflowError::internal)?;
    let screen_receipt = find
        .screen_receipt
        .as_ref()
        .ok_or_else(WorkflowError::internal)?;
    let screen_job = if screen_receipt.operation == "Decision.GetCurrentScreenJobResult" {
        Value::Object(job_arguments(&job_from_receipt(screen_receipt)?)?)
    } else if screen_receipt.operation == "Decision.ReadCurrentScreenPreparation"
        && find.candidates.is_empty()
        && screen_receipt
            .body
            .get("availablePartitionCount")
            .and_then(Value::as_u64)
            == Some(0)
        && screen_receipt
            .body
            .pointer("/coverage/complete")
            .and_then(Value::as_bool)
            == Some(true)
    {
        Value::Null
    } else {
        return Err(WorkflowError::internal());
    };
    if work.completed_analyses.len() != find.candidates.len()
        || work.completed_outcomes.len() != find.candidates.len()
        || run.child_jobs.iter().any(|child| {
            child
                .result
                .as_ref()
                .is_some_and(|result| result.operation == "Job.Get")
        })
    {
        return Err(WorkflowError::internal());
    }
    let analyses = find.candidates.iter().zip(&work.completed_analyses).map(|(candidate, saved)| {
        let (action,assessment)=if let Some(value)=saved.get("memberUnavailable") {
            let unavailable:FindMemberUnavailable=serde_json::from_value(value.clone()).map_err(|_|WorkflowError::internal())?;
            if !unavailable.valid() || candidate.get("candidateId").and_then(Value::as_str)!=Some(unavailable.member.candidate_id.as_str()) {return Err(WorkflowError::internal());}
            (None,Some(unavailable.assessment_sha256))
        } else {(Some(uuid_field(saved,"actionToken")?),None)};
        Ok(json!({"candidateId":candidate.get("candidateId").ok_or_else(WorkflowError::internal)?,"actionToken":action,"unavailableAssessmentSha256":assessment}))
    }).collect::<Result<Vec<_>, WorkflowError>>()?;
    let mut arguments = work.find_preparation()?;
    arguments["screenJob"] = screen_job;
    arguments["analyses"] = json!(analyses);
    Ok(arguments)
}

pub(super) fn validate_find_result(
    body: &Value,
    work: &DriverState,
    references: &[ServiceResultReference],
) -> Result<FindResult, WorkflowError> {
    let result: FindResult =
        serde_json::from_value(body.clone()).map_err(|_| WorkflowError::internal())?;
    let find = work.find.as_ref().ok_or_else(WorkflowError::internal)?;
    let preparation = work.find_preparation()?;
    let coverage = &result.coverage;
    let expected_screen = find
        .screen_receipt
        .as_ref()
        .and_then(|receipt| receipt.body.get("screenRunId"))
        .and_then(Value::as_str);
    if body.as_object().is_none_or(|value| value.len() != 9)
        || result.preparation_id != uuid_field(&preparation, "preparationId")?
        || preparation.get("preparationSha256").and_then(Value::as_str)
            != Some(result.preparation_sha256.as_str())
        || !valid_digest(&result.result_sha256)
        || result.coverage_reference.preparation_id != result.preparation_id
        || result.coverage_reference.preparation_sha256 != result.preparation_sha256
        || result.screen_run_id.as_deref() != expected_screen
        || result.benchmarks.primary != "SPY"
        || result.benchmarks.alongside != "VTI"
        || coverage.scope != "admitted_canonical_catalog"
        || !coverage.complete
        || coverage.canonical_population_count != find.canonical_population_count
        || coverage.population_count != find.population_count
        || coverage
            .excluded_count
            .checked_add(coverage.population_count)
            != Some(coverage.canonical_population_count)
        || coverage.unavailable_count > coverage.population_count
        || coverage.partition_count != find.partition_count
        || coverage.prepared_partition_count != find.partition_count
        || result.results.len() != find.candidates.len()
        || result
            .results
            .iter()
            .filter(|row| row.member_unavailable.is_none())
            .count()
            != references.len()
        || result.results.len() != work.completed_analyses.len()
        || result.results.len() != work.completed_outcomes.len()
        || result.results.len() > 32
        || u32::try_from(result.results.len())
            .ok()
            .is_none_or(|count| count > coverage.population_count - coverage.unavailable_count)
    {
        return Err(WorkflowError::internal());
    }
    // Both states are truthful backend outcomes; incomparable estimates retain backend order.
    match &result.ordering {
        FindOrdering::EstimatedGainDescending | FindOrdering::UnavailableIncomparableHorizons => {}
    }
    let mut seen = std::collections::HashSet::new();
    for (index, row) in result.results.iter().enumerate() {
        let original = row
            .screen_rank
            .checked_sub(1)
            .ok_or_else(WorkflowError::internal)?;
        let candidate = find
            .candidates
            .get(original)
            .ok_or_else(WorkflowError::internal)?;
        let saved = work
            .completed_analyses
            .get(original)
            .ok_or_else(WorkflowError::internal)?;
        if row.rank != index + 1
            || !seen.insert(original)
            || candidate.get("candidateId").and_then(Value::as_str)
                != Some(row.candidate_id.as_str())
            || !row.expected_return.is_object()
            || work.completed_outcomes.get(original).map(String::as_str)
                != Some(row.analysis_state.outcome())
        {
            return Err(WorkflowError::internal());
        }
        if let Some(unavailable) = &row.member_unavailable {
            if !unavailable.valid()
                || unavailable.member.preparation_id != result.preparation_id
                || unavailable.member.preparation_sha256 != result.preparation_sha256
                || Some(unavailable.member.screen_run_id.as_str())
                    != result.screen_run_id.as_deref()
                || unavailable.member.candidate_id != row.candidate_id
                || saved.get("memberUnavailable")
                    != Some(
                        &serde_json::to_value(unavailable)
                            .map_err(|_| WorkflowError::internal())?,
                    )
                || row.action_token.is_some()
                || row.analysis_id.is_some()
                || row.analysis_state.outcome() != "unavailable"
            {
                return Err(WorkflowError::internal());
            }
        } else {
            let action = row.action_token.ok_or_else(WorkflowError::internal)?;
            let id = row
                .analysis_id
                .as_deref()
                .ok_or_else(WorkflowError::internal)?;
            if action != uuid_field(saved, "actionToken")?
                || saved.get("analysisId").and_then(Value::as_str) != Some(id)
                || !valid_digest(id)
                || !references.iter().any(|reference| {
                    reference.result_id == action.to_string()
                        && saved.get("explanationDigest").and_then(Value::as_str)
                            == Some(reference.content_sha256.as_str())
                })
            {
                return Err(WorkflowError::internal());
            }
        }
    }
    Ok(result)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CoverageCursor {
    partition: Option<u16>,
    offset: u32,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(in crate::application::analytical_workflow) struct CoverageReason {
    instrument_id: Uuid,
    reason: CoverageReasonKind,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum CoverageReasonKind {
    NoEffectiveCanonicalDefinition,
    OutsideProfileAssetScope,
    MissingOfficialListing,
    AmbiguousOfficialListing,
    SourceHistoryUnavailable,
    RequiredFeatureUnavailable,
    SourceRightsUnavailable,
    FreshnessUnavailable,
    CalendarUnavailable,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CoveragePage {
    preparation_id: Uuid,
    preparation_sha256: String,
    ordinal: Option<u16>,
    offset: u32,
    total: u32,
    rows: Vec<CoverageReason>,
}

pub(in crate::application::analytical_workflow) async fn read_coverage(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    token: &str,
    after: Option<&CoverageCursor>,
) -> Result<AnalyticalControllerResponse, WorkflowError> {
    let retained_generation = generation.for_workflow(token)?;
    let generation = &retained_generation;
    let (saved, expected_id, expected_digest) = {
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        let document = generation.analytical_controller().lock_document()?;
        let run = workflow_control::find_workflow(&document, token)?;
        if run.kind != WorkflowKind::FindOpportunities || run.state != WorkflowRunState::Completed {
            return Err(WorkflowError::invalid_request(
                "Complete this search before opening its coverage.",
            ));
        }
        let receipt = run
            .driver
            .as_ref()
            .ok_or_else(WorkflowError::internal)?
            .receipt(Step::FindReadPublished)?
            .clone();
        let completion = run
            .completion_reference
            .as_ref()
            .ok_or_else(WorkflowError::internal)?;
        (
            receipt,
            completion.result_id.clone(),
            completion.content_sha256.clone(),
        )
    };
    let actual = call(
        generation,
        "Decision.GetFindResults",
        saved.arguments.clone(),
        false,
        RequestId::try_string(format!(
            "desktop-coverage-reopen-{}",
            Uuid::new_v4().simple()
        ))
        .map_err(|_| WorkflowError::internal())?,
    )
    .await?;
    if actual != saved.body || !saved.valid() {
        return Err(WorkflowError::internal());
    }
    let result: FindResult =
        serde_json::from_value(actual).map_err(|_| WorkflowError::internal())?;
    if result.preparation_id.to_string() != expected_id || result.result_sha256 != expected_digest {
        return Err(WorkflowError::internal());
    }
    let mut cursor = after.cloned().unwrap_or(CoverageCursor {
        partition: None,
        offset: 0,
    });
    if cursor.offset > 65_536
        || cursor
            .partition
            .is_some_and(|ordinal| usize::from(ordinal) >= result.coverage.partition_count)
    {
        return Err(WorkflowError::invalid_request(
            "Choose a valid saved coverage page.",
        ));
    }
    let mut rows = Vec::new();
    let mut complete = false;
    // One user-requested page reads at most16 original partitions and retains at most64 reasons.
    // Empty partitions advance the cursor without inventing rows or truncating coverage.
    for _ in 0..16 {
        let limit = 64 - rows.len();
        let mut arguments = saved.arguments.clone();
        if let Some(ordinal) = cursor.partition {
            arguments.insert("ordinal".into(), json!(ordinal));
        }
        arguments.insert("offset".into(), json!(cursor.offset));
        arguments.insert("limit".into(), json!(limit));
        let body = call(
            generation,
            "Decision.ReadCurrentScreenCoverage",
            arguments,
            false,
            RequestId::try_string(format!("desktop-coverage-page-{}", Uuid::new_v4().simple()))
                .map_err(|_| WorkflowError::internal())?,
        )
        .await?;
        let page: CoveragePage =
            serde_json::from_value(body).map_err(|_| WorkflowError::internal())?;
        if page.preparation_id != result.preparation_id
            || page.preparation_sha256 != result.preparation_sha256
            || page.ordinal != cursor.partition
            || page.offset != cursor.offset
            || page.total > 65_536
            || cursor.offset > page.total
            || page.rows.len() > limit
            || page.rows.len()
                != usize::try_from(page.total - cursor.offset)
                    .map_err(|_| WorkflowError::internal())?
                    .min(limit)
            || page.rows.iter().any(|row| row.instrument_id.is_nil())
        {
            return Err(WorkflowError::internal());
        }
        cursor.offset = cursor
            .offset
            .checked_add(u32::try_from(page.rows.len()).map_err(|_| WorkflowError::internal())?)
            .ok_or_else(WorkflowError::internal)?;
        rows.extend(page.rows);
        if cursor.offset == page.total {
            let next = cursor
                .partition
                .map_or(0, |ordinal| usize::from(ordinal) + 1);
            if next == result.coverage.partition_count {
                complete = true;
                break;
            }
            cursor = CoverageCursor {
                partition: Some(u16::try_from(next).map_err(|_| WorkflowError::internal())?),
                offset: 0,
            };
        }
        if rows.len() == 64 {
            break;
        }
    }
    state.admit_current(generation)?;
    Ok(AnalyticalControllerResponse::WorkflowCoverage {
        workflow_token: token.to_owned(),
        rows,
        next_after: if complete { None } else { Some(cursor) },
    })
}

/// The exact source acknowledgement owns this completed prefix. Called in its atomic save.
pub(super) fn compact_completed_partition(run: &mut WorkflowRun) -> Result<(), WorkflowError> {
    let work = run.driver.as_ref().ok_or_else(WorkflowError::internal)?;
    let find = work.find.as_ref().ok_or_else(WorkflowError::internal)?;
    if !matches!(work.step, Step::FindPartition | Step::FindReadPreparation)
        || work.active_job.is_some()
        || find.pending_dataset_job.is_some()
        || run
            .child_jobs
            .iter()
            .any(|job| job.terminal_sequence.is_none())
        || find.completed_partition.as_ref().is_none_or(|receipt| {
            completion_receipt(
                receipt,
                &find.preparation,
                find.partition_index.checked_sub(1),
            )
            .is_err()
        })
    {
        return Err(WorkflowError::internal());
    }
    let created = run
        .checkpoint_journal
        .first()
        .cloned()
        .ok_or_else(WorkflowError::internal)?;
    if created.stage != WorkflowCheckpointStage::Created {
        return Err(WorkflowError::internal());
    }
    run.child_jobs.clear();
    run.checkpoint_journal = vec![created];
    Ok(())
}

pub(super) fn completion_receipt(
    receipt: &Receipt,
    preparation: &Value,
    ordinal: Option<usize>,
) -> Result<(), WorkflowError> {
    let expected = ordinal
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(WorkflowError::internal)?;
    if !serde_json::to_vec(receipt).is_ok_and(|bytes| bytes.len() <= 2_048)
        || receipt.operation != "Analysis.CompleteCurrentScreenPartition"
        || receipt
            .body
            .as_object()
            .is_none_or(|object| object.len() != 5)
        || receipt.arguments.get("ordinal").and_then(Value::as_u64) != Some(expected)
        || receipt
            .body
            .pointer("/completion/ordinal")
            .and_then(Value::as_u64)
            != Some(expected)
        || receipt
            .body
            .get("completion")
            .and_then(Value::as_object)
            .is_none_or(|object| object.len() != 2)
        || receipt
            .body
            .pointer("/completion/completionSha256")
            .and_then(Value::as_str)
            .is_none_or(|value| !valid_digest(value))
        || ["preparationId", "preparationSha256"].iter().any(|key| {
            receipt.body.get(*key) != preparation.get(*key)
                || receipt.arguments.get(*key) != preparation.get(*key)
        })
        || receipt.body.get("datasetJob").is_none()
        || receipt.body.get("datasetContentSha256").is_none()
        || receipt.body.get("datasetJob").is_some_and(Value::is_null)
            != receipt
                .body
                .get("datasetContentSha256")
                .is_some_and(Value::is_null)
        || receipt
            .body
            .get("datasetContentSha256")
            .filter(|value| !value.is_null())
            .is_some_and(|value| value.as_str().is_none_or(|value| !valid_digest(value)))
    {
        return Err(WorkflowError::internal());
    }
    if let Some(child) = receipt
        .body
        .get("datasetJob")
        .filter(|value| !value.is_null())
    {
        if child.as_object().is_none_or(|object| object.len() != 2)
            || child
                .get("jobId")
                .and_then(Value::as_str)
                .and_then(|id| id.parse::<Uuid>().ok())
                .is_none_or(|id| id.is_nil())
            || child
                .get("generation")
                .and_then(Value::as_u64)
                .is_none_or(|value| value == 0)
            || receipt.arguments.get("datasetJob") != Some(child)
        {
            return Err(WorkflowError::internal());
        }
    } else if receipt.arguments.contains_key("datasetJob") {
        return Err(WorkflowError::internal());
    }
    Ok(())
}

pub(super) fn current_find_member(work: &DriverState) -> Result<FindMemberContext, WorkflowError> {
    let find = work.find.as_ref().ok_or_else(WorkflowError::internal)?;
    let candidate = find
        .candidates
        .get(find.candidate_index)
        .ok_or_else(WorkflowError::internal)?;
    Ok(FindMemberContext {
        preparation_id: uuid_field(&find.preparation, "preparationId")?,
        preparation_sha256: find
            .preparation
            .get("preparationSha256")
            .and_then(Value::as_str)
            .filter(|s| valid_digest(s))
            .ok_or_else(WorkflowError::internal)?
            .into(),
        candidate_id: candidate
            .get("candidateId")
            .and_then(Value::as_str)
            .ok_or_else(WorkflowError::internal)?
            .into(),
        screen_run_id: candidate
            .get("screenRunId")
            .and_then(Value::as_str)
            .ok_or_else(WorkflowError::internal)?
            .into(),
    })
}
impl DriverState {
    /// Counts only source-issued rows in the retained final aggregate, never raw errors.
    pub(in crate::application::analytical_workflow) fn retained_find_unavailable_count(
        &self,
    ) -> Option<usize> {
        let receipt = self.receipts.get("FindReadPublished")?;
        if !receipt.valid() {
            return None;
        }
        let result: FindResult = serde_json::from_value(receipt.body.clone()).ok()?;
        if result.results.len() > 32 {
            return None;
        }
        let mut count = 0;
        for row in &result.results {
            if let Some(value) = &row.member_unavailable {
                if !value.valid()
                    || value.member.preparation_id != result.preparation_id
                    || value.member.preparation_sha256 != result.preparation_sha256
                    || Some(value.member.screen_run_id.as_str()) != result.screen_run_id.as_deref()
                    || value.member.candidate_id != row.candidate_id
                    || row.action_token.is_some()
                    || row.analysis_id.is_some()
                    || row.analysis_state.outcome() != "unavailable"
                {
                    return None;
                }
                count += 1;
            } else if row.action_token.is_none()
                || row.analysis_id.as_deref().is_none_or(|s| !valid_digest(s))
            {
                return None;
            }
        }
        Some(count)
    }
}

impl DriverState {
    pub(in crate::application::analytical_workflow) fn unavailable_member_presentations(
        &self,
    ) -> Result<Vec<super::super::FindUnavailableMemberPresentation>, WorkflowError> {
        let values = if let Some(receipt) = self.receipts.get("FindReadPublished") {
            if self.retained_find_unavailable_count().is_none() {
                return Err(WorkflowError::internal());
            }
            let result: FindResult = serde_json::from_value(receipt.body.clone())
                .map_err(|_| WorkflowError::internal())?;
            result
                .results
                .into_iter()
                .filter_map(|row| row.member_unavailable)
                .collect::<Vec<_>>()
        } else {
            self.completed_analyses
                .iter()
                .filter_map(|row| row.get("memberUnavailable"))
                .map(|value| {
                    serde_json::from_value::<FindMemberUnavailable>(value.clone())
                        .map_err(|_| WorkflowError::internal())
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        values
            .into_iter()
            .map(|value| {
                if !value.valid() {
                    return Err(WorkflowError::internal());
                }
                Ok(super::super::FindUnavailableMemberPresentation {
                    candidate_id: value.member.candidate_id,
                    reason: match value.reason {
                        FindSourceUnavailableReason::IdentityUnavailable => "identity_unavailable",
                        FindSourceUnavailableReason::SourceEvidenceUnavailable => {
                            "source_evidence_unavailable"
                        }
                    },
                    missing_evidence: value
                        .source_failures
                        .into_iter()
                        .map(|failure| {
                            use super::super::MissingInvestmentEvidence;
                            // The original diagnostic text remains in the retained receipt only.
                            match failure.source.as_str() {
                                "current_session" => Ok(MissingInvestmentEvidence::CurrentMarket),
                                "government_history" => {
                                    Ok(MissingInvestmentEvidence::InterestRateHistory)
                                }
                                "benchmark_history" => {
                                    Ok(MissingInvestmentEvidence::BenchmarkHistory)
                                }
                                "selected_history" => {
                                    Ok(MissingInvestmentEvidence::InvestmentHistory)
                                }
                                "source_actions" => Ok(MissingInvestmentEvidence::CorporateActions),
                                "equity_premium" => Ok(MissingInvestmentEvidence::EquityPremium),
                                _ => Err(WorkflowError::internal()),
                            }
                        })
                        .collect::<Result<Vec<_>, WorkflowError>>()?,
                })
            })
            .collect()
    }
}
