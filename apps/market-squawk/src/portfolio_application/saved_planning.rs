//! Durable completed calculations and account-bound saved markers, without recalculation.

use std::{num::NonZeroUsize, sync::Arc};

use market_squawk_data::{
    PortfolioPlanningCatalogCapability, PortfolioPlanningCompletion, PortfolioPlanningError,
    PortfolioPlanningHead, PortfolioPlanningKind, PortfolioPlanningSavedEntry,
};
use market_squawk_domain::{AccountId, Timestamp};
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactReadContext,
    ArtifactReadRequest, ArtifactReference, ArtifactRepository, RequestContext,
    ResultEnvelopeProjection, ServiceLimits, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use super::model::PortfolioReadImage;
use super::read::{ReadScope, narrowed_limits};
use super::snapshot_page::{encode_cursor, read_cursor, selected_revision};
use super::{PortfolioApplicationLimits, PortfolioApplicationServiceError, Runtime, ensure_live};

const SCHEMA: &str = "market-squawk/portfolio-planning-completion/v1";
const PAGE_SIZE: usize = 25;

#[derive(Clone)]
pub(super) struct SavedPlanningStorage {
    pub(super) catalog: PortfolioPlanningCatalogCapability,
    pub(super) artifacts: Arc<dyn ArtifactRepository>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OriginalRequest {
    operation: String,
    arguments: Value,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CompletedCalculation {
    schema: String,
    calculation_token: Uuid,
    account_id: AccountId,
    account_token: String,
    kind: PortfolioPlanningKind,
    snapshot_token: Uuid,
    calculated_at_unix_nanos: String,
    portfolio_effective_at_unix_nanos: String,
    portfolio_available_at_unix_nanos: Option<String>,
    request: OriginalRequest,
    result: Value,
    metadata: Value,
    internal_evidence: Value,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SavedPageCursor {
    version: u8,
    account_id: AccountId,
    fence: PortfolioPlanningHead,
    after: u64,
}

pub(super) fn is_calculation(operation: &str) -> bool {
    matches!(
        operation,
        "Portfolio.EvaluateScenario"
            | "Portfolio.EvaluateScenarioBatch"
            | "Portfolio.ProposeRebalance"
            | "Portfolio.EvaluateCandidateImpact"
    )
}

fn kind(operation: &str) -> Result<PortfolioPlanningKind, PortfolioApplicationServiceError> {
    match operation {
        "Portfolio.EvaluateScenario" => Ok(PortfolioPlanningKind::Scenario),
        "Portfolio.EvaluateScenarioBatch" => Ok(PortfolioPlanningKind::ScenarioBatch),
        "Portfolio.ProposeRebalance" => Ok(PortfolioPlanningKind::Rebalance),
        "Portfolio.EvaluateCandidateImpact" => Ok(PortfolioPlanningKind::PositionComparison),
        _ => Err(PortfolioApplicationServiceError::InvalidRequest),
    }
}

/// Captures producer-owned input evidence from the same immutable image used by the worker.
pub(super) fn portfolio_evidence(
    image: &PortfolioReadImage,
    request: &TypedToolRequest,
    context: &RequestContext,
    limits: PortfolioApplicationLimits,
) -> Result<Value, PortfolioApplicationServiceError> {
    let scope = ReadScope::from_product_request(image, request, limits)?;
    let token = text(request.arguments().get("snapshotToken"))?;
    let revision = selected_revision(image, &scope, token, context)?;
    Ok(json!({
        "revisionToken": super::import::hex(&revision.token().bytes()),
        "account": revision.account,
        "holdings": revision.holdings,
        "sourceId": revision.source_id,
        "sourceCoverage": revision.source_coverage,
        "artifactSha256": super::import::hex(&revision.artifact_sha256),
    }))
}

/// A completion becomes addressable only after immutable bytes and its catalog row are durable.
pub(super) async fn complete(
    runtime: &Arc<Runtime>,
    request: &TypedToolRequest,
    result: TypedToolResult,
    internal_evidence: Value,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    ensure_live(runtime, context)?;
    let storage = storage(runtime)?;
    let account_token = text(request.arguments().get("accountToken"))?.to_owned();
    let account_id = text(result.structured_content().get("accountId"))?
        .parse::<AccountId>()
        .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
    if super::product::account_binding(account_id, 1)?.token() != account_token {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    let snapshot_token = token(result.structured_content().get("snapshotToken"))?;
    let candidate = request.name() == "Portfolio.EvaluateCandidateImpact";
    let effective_field = if candidate {
        "portfolioEffectiveAtUnixNanos"
    } else {
        "effectiveAtUnixNanos"
    };
    let available_field = if candidate {
        "portfolioAvailableAtUnixNanos"
    } else {
        "availableAtUnixNanos"
    };
    let effective_at = timestamp(result.structured_content().get(effective_field))?;
    let available_at = match result.structured_content().get(available_field) {
        Some(Value::Null) => None,
        value => Some(timestamp(value)?),
    };
    let calculated_at = current_timestamp()?;
    let calculation_token = Uuid::new_v4();
    let mut content = result.structured_content().clone();
    let output = content
        .as_object_mut()
        .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
    output.insert("calculationToken".to_owned(), json!(calculation_token));
    output.insert(
        "calculatedAtUnixNanos".to_owned(),
        json!(calculated_at.unix_nanos().to_string()),
    );
    let limits = if candidate {
        context.limits()
    } else {
        let (maximum_items, maximum_bytes) = result_limits(runtime, request, context)?;
        narrowed_limits(context, maximum_items, maximum_bytes)?
    };
    let completed = TypedToolResult::try_new(
        content.clone(),
        result.item_count(),
        result.metadata().clone(),
        limits,
    )
    .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    let envelope = CompletedCalculation {
        schema: SCHEMA.to_owned(),
        calculation_token,
        account_id,
        account_token,
        kind: kind(request.name())?,
        snapshot_token,
        calculated_at_unix_nanos: calculated_at.unix_nanos().to_string(),
        portfolio_effective_at_unix_nanos: effective_at.unix_nanos().to_string(),
        portfolio_available_at_unix_nanos: available_at.map(|time| time.unix_nanos().to_string()),
        request: OriginalRequest {
            operation: request.name().to_owned(),
            arguments: json!(request.arguments()),
        },
        result: content,
        metadata: result.metadata_value(ResultEnvelopeProjection::NativeEvidenceV1),
        internal_evidence,
    };
    let completed_kind = envelope.kind;
    let artifacts = Arc::clone(&storage.artifacts);
    let runtime_worker = Arc::clone(runtime);
    let worker_context = context.clone();
    let worker_guard = runtime.admit()?;
    let handle = tokio::runtime::Handle::current();
    let reference = tokio::task::spawn_blocking(move || {
        let _guard = worker_guard;
        ensure_live(&runtime_worker, &worker_context)?;
        let bytes = serde_json::to_vec(&envelope)
            .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        if bytes.len() > runtime_worker.limits.max_retained_bytes {
            return Err(PortfolioApplicationServiceError::ResourceExhausted);
        }
        let publication = ArtifactPublication::try_json(bytes).map_err(artifact_error)?;
        handle
            .block_on(artifacts.publish(
                publication,
                ArtifactPublicationContext::new(
                    worker_context.cancellation().clone(),
                    worker_context.deadline(),
                ),
            ))
            .map_err(artifact_error)
    })
    .await
    .map_err(|_| PortfolioApplicationServiceError::Authority)??;
    let completion = PortfolioPlanningCompletion {
        calculation_token,
        account_id,
        kind: completed_kind,
        snapshot_token,
        calculated_at,
        portfolio_effective_at: effective_at,
        portfolio_available_at: available_at,
        artifact_id: reference.id().to_owned(),
        artifact_sha256: digest(reference.sha256())?,
        artifact_byte_length: u64::try_from(reference.byte_count())?,
        artifact_media_type: reference.media_type().to_owned(),
    };
    let catalog = storage.catalog;
    catalog_work(runtime, context, move || {
        catalog.complete(&completion).map_err(catalog_error)
    })
    .await?;
    ensure_live(runtime, context)?;
    Ok(completed)
}

pub(super) async fn call(
    runtime: &Arc<Runtime>,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    ensure_live(runtime, context)?;
    let storage = storage(runtime)?;
    let account_token = text(request.arguments().get("accountToken"))?.to_owned();
    let image = runtime.image.load();
    let accounts = super::product::account_catalog(&image)?;
    let account_id = super::product::resolve_account_token(&accounts, &account_token)?;
    drop(image);
    match request.name() {
        "Portfolio.SavePlanningResult" => {
            let calculation_token = token(request.arguments().get("calculationToken"))?;
            let catalog = storage.catalog.clone();
            let entry = catalog_work(runtime, context, move || {
                catalog
                    .completion(account_id, calculation_token)
                    .map_err(catalog_error)?
                    .ok_or(PortfolioApplicationServiceError::NotFound)
            })
            .await?;
            // Verify the immutable bytes before making a previously unsaved result reachable.
            let _original = load(
                &storage,
                &entry.completion,
                runtime.limits.max_retained_bytes,
                context,
            )
            .await?;
            let saved_at = current_timestamp()?;
            let catalog = storage.catalog;
            let saved = catalog_work(runtime, context, move || {
                catalog
                    .save(account_id, calculation_token, saved_at)
                    .map_err(catalog_error)
            })
            .await?;
            // No post-commit cancellation check: a later read cancellation cannot undo Save.
            retained_result(
                json!({"summary": summary(&saved, &account_token)}),
                1,
                context.limits(),
            )
        }
        "Portfolio.ListPlanningResults" => {
            list(
                runtime,
                &storage,
                request,
                context,
                account_id,
                &account_token,
            )
            .await
        }
        "Portfolio.GetPlanningResult" => {
            let saved_token = token(request.arguments().get("savedResultToken"))?;
            let (maximum_items, maximum_bytes) = result_limits(runtime, request, context)?;
            let catalog = storage.catalog.clone();
            let saved = catalog_work(runtime, context, move || {
                catalog
                    .saved(account_id, saved_token)
                    .map_err(catalog_error)?
                    .ok_or(PortfolioApplicationServiceError::NotFound)
            })
            .await?;
            let original = load(
                &storage,
                &saved.completion.completion,
                runtime.limits.max_retained_bytes,
                context,
            )
            .await?;
            ensure_live(runtime, context)?;
            let metadata = ToolResultMetadata::try_complete(
                original
                    .metadata
                    .get("sourceCoverage")
                    .cloned()
                    .ok_or(PortfolioApplicationServiceError::CorruptPublication)?,
                original
                    .metadata
                    .get("dataQuality")
                    .cloned()
                    .ok_or(PortfolioApplicationServiceError::CorruptPublication)?,
            )
            .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
            TypedToolResult::try_new(
                json!({
                    "summary": summary(&saved, &account_token),
                    "request": original.request, "result": original.result,
                }),
                1,
                metadata,
                narrowed_limits(context, maximum_items, maximum_bytes)?,
            )
            .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)
        }
        _ => Err(PortfolioApplicationServiceError::InvalidRequest),
    }
}

async fn list(
    runtime: &Arc<Runtime>,
    storage: &SavedPlanningStorage,
    request: &TypedToolRequest,
    context: &RequestContext,
    account_id: AccountId,
    account_token: &str,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let (maximum_items, maximum_bytes) = result_limits(runtime, request, context)?;
    let requested_limit = request
        .arguments()
        .get("limit")
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| (1..=100).contains(value))
                .ok_or(PortfolioApplicationServiceError::InvalidRequest)
        })
        .transpose()?
        .unwrap_or(PAGE_SIZE);
    let limit = requested_limit.min(maximum_items).min(PAGE_SIZE);
    let cursor: Option<SavedPageCursor> = read_cursor(request, context)?;
    let catalog = storage.catalog.clone();
    let fence = if let Some(cursor) = &cursor {
        if cursor.version != 1
            || cursor.account_id != account_id
            || cursor.after > cursor.fence.saves.sequence
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        cursor.fence
    } else {
        catalog_work(runtime, context, move || {
            catalog.head().map_err(catalog_error)
        })
        .await?
    };
    let after = cursor.map_or(0, |cursor| cursor.after);
    let page_cursor = encode_cursor(&SavedPageCursor {
        version: 1,
        account_id,
        fence,
        after,
    })?;
    let catalog = storage.catalog.clone();
    let mut page = catalog_work(runtime, context, move || {
        catalog
            .saved_page(account_id, fence, after, limit + 1)
            .map_err(catalog_error)
    })
    .await?;
    let available = page.len();
    let mut has_more = page.len() > limit;
    page.truncate(limit);
    loop {
        ensure_live(runtime, context)?;
        let next_cursor = if has_more {
            let last = page
                .last()
                .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
            Some(encode_cursor(&SavedPageCursor {
                version: 1,
                account_id,
                fence,
                after: last.head.sequence,
            })?)
        } else {
            None
        };
        let output = json!({"results": page.iter().map(|entry| summary(entry, account_token)).collect::<Vec<_>>(),
            "pageCursor": page_cursor, "nextCursor": next_cursor});
        let metadata = if has_more {
            ToolResultMetadata::try_truncated_not_applicable(available)
        } else {
            Ok(ToolResultMetadata::complete_not_applicable())
        }
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        match TypedToolResult::try_new(
            output,
            page.len(),
            metadata,
            narrowed_limits(context, maximum_items, maximum_bytes)?,
        ) {
            Ok(result) => return Ok(result),
            Err(_) if page.len() > 1 => {
                page.pop();
                has_more = true;
            }
            Err(_) => return Err(PortfolioApplicationServiceError::ResourceExhausted),
        }
    }
}

fn summary(saved: &PortfolioPlanningSavedEntry, account_token: &str) -> Value {
    let completion = &saved.completion.completion;
    json!({
        "savedResultToken": completion.calculation_token,
        "calculationToken": completion.calculation_token,
        "accountToken": account_token,
        "kind": completion.kind,
        "snapshotToken": completion.snapshot_token,
        "portfolioEffectiveAtUnixNanos": completion.portfolio_effective_at.unix_nanos().to_string(),
        "portfolioAvailableAtUnixNanos": completion.portfolio_available_at.map(|time| time.unix_nanos().to_string()),
        "calculatedAtUnixNanos": completion.calculated_at.unix_nanos().to_string(),
        "savedAtUnixNanos": saved.saved_at.unix_nanos().to_string(),
    })
}

async fn load(
    storage: &SavedPlanningStorage,
    completion: &PortfolioPlanningCompletion,
    maximum_bytes: usize,
    context: &RequestContext,
) -> Result<CompletedCalculation, PortfolioApplicationServiceError> {
    let reference = artifact_reference(completion)?;
    let read = storage
        .artifacts
        .read(
            ArtifactReadRequest::try_new(
                reference,
                NonZeroUsize::new(maximum_bytes)
                    .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?,
            )
            .map_err(artifact_error)?,
            ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
        )
        .await
        .map_err(artifact_error)?;
    let original: CompletedCalculation = serde_json::from_slice(read.content())
        .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
    let account_token = super::product::account_binding(completion.account_id, 1)?;
    let candidate = original.kind == PortfolioPlanningKind::PositionComparison;
    let effective_field = if candidate {
        "portfolioEffectiveAtUnixNanos"
    } else {
        "effectiveAtUnixNanos"
    };
    let available_field = if candidate {
        "portfolioAvailableAtUnixNanos"
    } else {
        "availableAtUnixNanos"
    };
    if original.schema != SCHEMA
        || original.calculation_token != completion.calculation_token
        || original.account_id != completion.account_id
        || original.account_token != account_token.token()
        || original.kind != completion.kind
        || kind(&original.request.operation)? != completion.kind
        || original.snapshot_token != completion.snapshot_token
        || original.calculated_at_unix_nanos != completion.calculated_at.unix_nanos().to_string()
        || original.portfolio_effective_at_unix_nanos
            != completion.portfolio_effective_at.unix_nanos().to_string()
        || original.portfolio_available_at_unix_nanos
            != completion
                .portfolio_available_at
                .map(|time| time.unix_nanos().to_string())
        || original.request.arguments.get("accountToken") != Some(&json!(original.account_token))
        || original.result.get("accountId") != Some(&json!(completion.account_id.to_string()))
        || original.result.get("snapshotToken") != Some(&json!(completion.snapshot_token))
        || original.result.get("calculationToken") != Some(&json!(completion.calculation_token))
        || original.result.get("calculatedAtUnixNanos")
            != Some(&json!(original.calculated_at_unix_nanos))
        || original.result.get(effective_field)
            != Some(&json!(original.portfolio_effective_at_unix_nanos))
        || original.result.get(available_field)
            != Some(&json!(original.portfolio_available_at_unix_nanos))
        || !original.internal_evidence.is_object()
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    Ok(original)
}

pub(crate) fn artifact_reference(
    completion: &PortfolioPlanningCompletion,
) -> Result<ArtifactReference, PortfolioApplicationServiceError> {
    ArtifactReference::try_new(
        completion.artifact_id.clone(),
        super::import::hex(&completion.artifact_sha256),
        usize::try_from(completion.artifact_byte_length)?,
        completion.artifact_media_type.clone(),
    )
    .map_err(artifact_error)
}

async fn catalog_work<T: Send + 'static>(
    runtime: &Arc<Runtime>,
    context: &RequestContext,
    work: impl FnOnce() -> Result<T, PortfolioApplicationServiceError> + Send + 'static,
) -> Result<T, PortfolioApplicationServiceError> {
    let guard = runtime.admit()?;
    let runtime = Arc::clone(runtime);
    let context = context.clone();
    tokio::task::spawn_blocking(move || {
        let _guard = guard;
        ensure_live(&runtime, &context)?;
        work()
    })
    .await
    .map_err(|_| PortfolioApplicationServiceError::Authority)?
}

fn storage(runtime: &Runtime) -> Result<SavedPlanningStorage, PortfolioApplicationServiceError> {
    runtime
        .planning
        .get()
        .cloned()
        .ok_or(PortfolioApplicationServiceError::Authority)
}

fn result_limits(
    runtime: &Runtime,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<(usize, usize), PortfolioApplicationServiceError> {
    let values = request
        .arguments()
        .get("resultLimits")
        .and_then(Value::as_object)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    let positive = |name: &str| {
        values
            .get(name)
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| *value > 0)
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)
    };
    Ok((
        positive("maximumItems")?
            .min(runtime.limits.max_result_items)
            .min(context.limits().maximum_result_items()),
        positive("maximumBytes")?
            .min(runtime.limits.max_retained_bytes)
            .min(context.limits().maximum_result_bytes()),
    ))
}

fn retained_result(
    value: Value,
    count: usize,
    limits: ServiceLimits,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let metadata = ToolResultMetadata::complete_not_applicable();
    TypedToolResult::try_new(value, count, metadata, limits)
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)
}

fn text(value: Option<&Value>) -> Result<&str, PortfolioApplicationServiceError> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)
}

fn token(value: Option<&Value>) -> Result<Uuid, PortfolioApplicationServiceError> {
    text(value)?
        .parse::<Uuid>()
        .ok()
        .filter(|token| !token.is_nil())
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)
}

fn timestamp(value: Option<&Value>) -> Result<Timestamp, PortfolioApplicationServiceError> {
    text(value)?
        .parse::<i64>()
        .map(Timestamp::from_unix_nanos)
        .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)
}

fn current_timestamp() -> Result<Timestamp, PortfolioApplicationServiceError> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(PortfolioApplicationServiceError::Publication)
}

fn digest(value: &str) -> Result<[u8; 32], PortfolioApplicationServiceError> {
    let mut bytes = [0; 32];
    if value.len() != 64 || !value.is_ascii() {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
    }
    Ok(bytes)
}

fn artifact_error(error: ArtifactError) -> PortfolioApplicationServiceError {
    match error {
        ArtifactError::ReadLimitExceeded => PortfolioApplicationServiceError::ResourceExhausted,
        ArtifactError::Cancelled => PortfolioApplicationServiceError::Cancelled,
        ArtifactError::DeadlineExceeded => PortfolioApplicationServiceError::DeadlineExceeded,
        ArtifactError::NotFound
        | ArtifactError::InvalidReference
        | ArtifactError::InvalidPublication => PortfolioApplicationServiceError::CorruptPublication,
        ArtifactError::Unavailable => PortfolioApplicationServiceError::Authority,
    }
}

fn catalog_error(error: PortfolioPlanningError) -> PortfolioApplicationServiceError {
    match error {
        PortfolioPlanningError::NotFound => PortfolioApplicationServiceError::NotFound,
        PortfolioPlanningError::Capacity => PortfolioApplicationServiceError::ResourceExhausted,
        PortfolioPlanningError::InvalidRecord => PortfolioApplicationServiceError::InvalidRequest,
        PortfolioPlanningError::Conflict | PortfolioPlanningError::Corrupt => {
            PortfolioApplicationServiceError::CorruptPublication
        }
        PortfolioPlanningError::Unavailable | PortfolioPlanningError::Storage(_) => {
            PortfolioApplicationServiceError::Authority
        }
    }
}
