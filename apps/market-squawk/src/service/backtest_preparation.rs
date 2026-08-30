//! Installed-service adapter for guided governed-backtest preparation.

use std::{collections::BTreeSet, sync::Arc, time::Instant};

use chrono::{DateTime, SecondsFormat};
use market_squawk_data::{
    AnalyticalReadCapability, AnalyticalReadLimit, DatasetId, FeatureDatasetProductContract,
    ForecastDatasetReadLimits,
};
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_jobs::{JobListPageLimit, JobState};
use market_squawk_runtime::RuntimeIdentity;
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::application::{
    analysis::{
        BacktestPreparationCatalog, BacktestPreparationDatasetInput, BacktestPreparationLimits,
        BacktestPreparationReceipt, BacktestPreparationSelection, GovernedBacktestAuthority,
        GovernedBacktestInputRegistrationInput, GovernedBacktestPreparationAuthority,
    },
    job::{JobView, product_activity_state, product_progress_percent},
    lifecycle::WorkspaceRuntimeIdentity,
    opaque_product_token,
};

use super::jobs::InstalledJobOperations;

pub(super) const GET_BACKTEST_PREPARATION: &str = "Analysis.GetBacktestPreparation";
pub(super) const PREVIEW_BACKTEST: &str = "Analysis.PreviewBacktest";
pub(super) const START_PREPARED_BACKTEST: &str = "Analysis.StartPreparedBacktest";
pub(super) const LIST_PRODUCT_BACKTESTS: &str = "Analysis.ListProductBacktests";
pub(super) const GET_PRODUCT_BACKTEST: &str = "Analysis.GetProductBacktest";

const DATASET_PAGE: usize = 64;
const MAXIMUM_DATASETS: usize = 4_096;
const MAXIMUM_ROWS_PER_DATASET: usize = 100_000;
const MAXIMUM_BYTES_PER_DATASET: usize = 256 * 1024 * 1024;

/// One process-generation preparation authority over the current analytical catalog.
pub(super) struct InstalledBacktestPreparation {
    authority: Arc<GovernedBacktestPreparationAuthority>,
    analytical: AnalyticalReadCapability,
    backtests: Arc<dyn GovernedBacktestAuthority>,
    runtime: RuntimeIdentity,
}

impl InstalledBacktestPreparation {
    pub(super) fn try_new(
        analytical: AnalyticalReadCapability,
        backtests: Arc<dyn GovernedBacktestAuthority>,
        runtime: RuntimeIdentity,
    ) -> Result<Self, ServiceError> {
        Ok(Self {
            authority:
                Arc::new(
                    GovernedBacktestPreparationAuthority::try_new(
                        BacktestPreparationLimits::standard(),
                    )
                    .map_err(ServiceError::from)?,
                ),
            analytical,
            backtests,
            runtime,
        })
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(
            operation,
            GET_BACKTEST_PREPARATION
                | PREVIEW_BACKTEST
                | LIST_PRODUCT_BACKTESTS
                | GET_PRODUCT_BACKTEST
        )
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        jobs: &InstalledJobOperations,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let (content, item_count) = match request.name() {
            GET_BACKTEST_PREPARATION => {
                let catalog = self.catalog(context).await?;
                let options = self
                    .authority
                    .options(&catalog)
                    .map_err(ServiceError::from)?;
                let count = options.datasets.len();
                (encode(&options)?, count)
            }
            PREVIEW_BACKTEST => {
                let catalog = self.catalog(context).await?;
                let input: BacktestPreviewRequest =
                    decode(&super::business_arguments(request.arguments()))?;
                let preview = self
                    .authority
                    .preview(
                        &catalog,
                        input.selection,
                        context.origin().ok_or(ServiceError::Unauthorized)?,
                        self.workspace()?,
                        Instant::now(),
                        super::runtime::current_timestamp()
                            .map_err(|_error| ServiceError::Unavailable)?,
                    )
                    .map_err(ServiceError::from)?;
                (encode(&preview)?, 1)
            }
            LIST_PRODUCT_BACKTESTS => {
                let activities = self.product_activities(jobs, context).await?;
                let count = activities.len();
                (serde_json::json!({"activities": activities}), count)
            }
            GET_PRODUCT_BACKTEST => {
                let input: BacktestProductRequest =
                    decode(&super::business_arguments(request.arguments()))?;
                (
                    self.product_result(jobs, input.backtest_token, context)
                        .await?,
                    1,
                )
            }
            _ => return Err(ServiceError::NotFound),
        };
        ensure_live(context)?;
        TypedToolResult::try_new(
            content,
            item_count,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(ServiceError::from)
    }

    async fn product_activities(
        &self,
        jobs: &InstalledJobOperations,
        context: &RequestContext,
    ) -> Result<Vec<Value>, ServiceError> {
        let page = jobs.list_page(product_job_page_limit(context)?).await?;
        page.jobs()
            .iter()
            .filter(|view| is_backtest_job(view))
            .map(product_backtest_activity)
            .collect()
    }

    async fn product_result(
        &self,
        jobs: &InstalledJobOperations,
        token: Uuid,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let page = jobs.list_page(product_job_page_limit(context)?).await?;
        let view = page
            .jobs()
            .iter()
            .find(|view| is_backtest_job(view) && product_backtest_token(view) == token)
            .ok_or(ServiceError::NotFound)?;
        if view.state() != JobState::Completed {
            return Err(ServiceError::NotFound);
        }
        let run_id = view
            .result()
            .map(|result| result.identity().as_str())
            .ok_or(ServiceError::InvalidResult)?;
        let record = self
            .backtests
            .get(run_id, context.cancellation().clone(), context.deadline())
            .await?
            .ok_or(ServiceError::NotFound)?;
        record.product_value(token, view.updated_at())
    }

    pub(super) async fn consume(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<GovernedBacktestInputRegistrationInput, ServiceError> {
        ensure_live(context)?;
        let input: BacktestStartRequest = decode(&super::business_arguments(request.arguments()))?;
        let catalog = self.catalog(context).await?;
        self.authority
            .consume(
                &catalog,
                input.receipt,
                context.origin().ok_or(ServiceError::Unauthorized)?,
                self.workspace()?,
                Instant::now(),
            )
            .map_err(ServiceError::from)
    }

    async fn catalog(
        &self,
        context: &RequestContext,
    ) -> Result<BacktestPreparationCatalog, ServiceError> {
        let page_limit = AnalyticalReadLimit::try_new(DATASET_PAGE)
            .map_err(|_error| ServiceError::Unavailable)?;
        let evidence_limits =
            ForecastDatasetReadLimits::try_new(MAXIMUM_ROWS_PER_DATASET, MAXIMUM_BYTES_PER_DATASET)
                .map_err(|_error| ServiceError::Unavailable)?;
        let mut after: Option<DatasetId> = None;
        let mut datasets = Vec::new();
        loop {
            ensure_live(context)?;
            let page = self
                .analytical
                .feature_datasets(
                    FeatureDatasetProductContract::PriceReturnFixedHorizonForwardReturnAnalysisV1,
                    after.as_ref(),
                    page_limit,
                    context.deadline(),
                    context.cancellation(),
                )
                .map_err(map_analytical)?;
            if page.datasets().is_empty() {
                break;
            }
            for dataset in page.datasets() {
                if datasets.len() >= MAXIMUM_DATASETS {
                    return Err(ServiceError::ResourceExhausted);
                }
                ensure_live(context)?;
                let generation = dataset.generation();
                let evidence = self
                    .analytical
                    .forecast_dataset_evidence(
                        FeatureDatasetProductContract::PriceReturnFixedHorizonForwardReturnAnalysisV1,
                        generation.manifest(),
                        Timestamp::from_unix_nanos(i64::MAX),
                        evidence_limits,
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                    .map_err(map_analytical)?;
                let instruments = evidence
                    .rows()
                    .iter()
                    .map(|row| row.instrument_id())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                let starts_at = evidence
                    .rows()
                    .iter()
                    .map(|row| row.cutoff_at())
                    .min()
                    .ok_or(ServiceError::InvalidResult)?;
                let ends_at = evidence
                    .rows()
                    .iter()
                    .map(|row| row.cutoff_at())
                    .max()
                    .ok_or(ServiceError::InvalidResult)?
                    .checked_add_nanos(1)
                    .map_err(|_error| ServiceError::InvalidResult)?;
                let dataset_id = generation.manifest().dataset_id().as_str();
                datasets.push(BacktestPreparationDatasetInput::new(
                    SourceIdentifier::try_from(dataset_id)
                        .map_err(|_error| ServiceError::InvalidResult)?,
                    display_name(dataset_id),
                    generation.manifest().clone(),
                    instruments,
                    starts_at,
                    ends_at,
                    dataset.source_ids().to_vec(),
                ));
            }
            after = page
                .datasets()
                .last()
                .map(|dataset| dataset.generation().manifest().dataset_id().clone());
            if !page.has_more() {
                break;
            }
        }
        BacktestPreparationCatalog::try_new(datasets).map_err(ServiceError::from)
    }

    fn workspace(&self) -> Result<WorkspaceRuntimeIdentity, ServiceError> {
        WorkspaceRuntimeIdentity::try_from_runtime(self.runtime)
            .map_err(|_error| ServiceError::Unavailable)
    }
}

impl std::fmt::Debug for InstalledBacktestPreparation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledBacktestPreparation")
            .field("authority", &"[ONE-USE BACKTEST PREPARATION AUTHORITY]")
            .field("analytical", &self.analytical)
            .field("backtests", &"[GOVERNED BACKTEST AUTHORITY]")
            .field("runtime", &self.runtime)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BacktestPreviewRequest {
    selection: BacktestPreparationSelection,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BacktestStartRequest {
    receipt: BacktestPreparationReceipt,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BacktestProductRequest {
    backtest_token: Uuid,
}

fn product_job_page_limit(context: &RequestContext) -> Result<JobListPageLimit, ServiceError> {
    JobListPageLimit::try_new(context.limits().maximum_result_items().min(1_000))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn is_backtest_job(view: &JobView) -> bool {
    view.kind().as_str() == "analysis.backtest.v1"
}

fn product_backtest_token(view: &JobView) -> Uuid {
    let job_id = view.job_id().as_uuid();
    let generation = view.generation().get().to_be_bytes();
    opaque_product_token(
        b"market-squawk/product-backtest/v1\0",
        &[job_id.as_bytes(), &generation],
    )
}

fn product_backtest_activity(view: &JobView) -> Result<Value, ServiceError> {
    let (state, status_message) = product_activity_state(view);
    Ok(serde_json::json!({
        "backtestToken": product_backtest_token(view),
        "label": "Investment approach backtest",
        "startedAt": timestamp_text(view.started_at()),
        "updatedAt": timestamp_text(view.updated_at()),
        "state": state,
        "progressPercent": product_progress_percent(view),
        "statusMessage": status_message,
    }))
}

fn timestamp_text(timestamp: Timestamp) -> String {
    DateTime::from_timestamp_nanos(timestamp.unix_nanos())
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn display_name(dataset_id: &str) -> String {
    const MAXIMUM_DISPLAY_BYTES: usize = 160;
    if dataset_id.len() <= MAXIMUM_DISPLAY_BYTES {
        dataset_id.to_owned()
    } else {
        dataset_id[..MAXIMUM_DISPLAY_BYTES].to_owned()
    }
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(arguments.clone()))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Value, ServiceError> {
    serde_json::to_value(value).map_err(|_error| ServiceError::Internal)
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_analytical(error: market_squawk_data::AnalyticalReadError) -> ServiceError {
    match error {
        market_squawk_data::AnalyticalReadError::ForecastDatasetUnavailable => {
            ServiceError::NotFound
        }
        market_squawk_data::AnalyticalReadError::InvalidLimit => ServiceError::ResourceExhausted,
        _ => ServiceError::Unavailable,
    }
}
