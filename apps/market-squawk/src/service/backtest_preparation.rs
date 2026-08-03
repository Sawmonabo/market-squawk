//! Installed-service adapter for guided governed-backtest preparation.

use std::{collections::BTreeSet, sync::Arc, time::Instant};

use market_squawk_data::{
    AnalyticalReadCapability, AnalyticalReadLimit, DatasetId, ForecastDatasetReadLimits,
};
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_runtime::RuntimeIdentity;
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::application::{
    analysis::{
        BacktestPreparationCatalog, BacktestPreparationDatasetInput, BacktestPreparationLimits,
        BacktestPreparationReceipt, BacktestPreparationSelection,
        GovernedBacktestInputRegistrationInput, GovernedBacktestPreparationAuthority,
    },
    lifecycle::WorkspaceRuntimeIdentity,
};

pub(super) const GET_BACKTEST_PREPARATION: &str = "Analysis.GetBacktestPreparation";
pub(super) const PREVIEW_BACKTEST: &str = "Analysis.PreviewBacktest";
pub(super) const START_PREPARED_BACKTEST: &str = "Analysis.StartPreparedBacktest";

const DATASET_PAGE: usize = 64;
const MAXIMUM_DATASETS: usize = 4_096;
const MAXIMUM_ROWS_PER_DATASET: usize = 100_000;
const MAXIMUM_BYTES_PER_DATASET: usize = 256 * 1024 * 1024;

/// One process-generation preparation authority over the current analytical catalog.
pub(super) struct InstalledBacktestPreparation {
    authority: Arc<GovernedBacktestPreparationAuthority>,
    analytical: AnalyticalReadCapability,
    runtime: RuntimeIdentity,
}

impl InstalledBacktestPreparation {
    pub(super) fn try_new(
        analytical: AnalyticalReadCapability,
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
            runtime,
        })
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(operation, GET_BACKTEST_PREPARATION | PREVIEW_BACKTEST)
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let catalog = self.catalog(context).await?;
        let (content, item_count) = match request.name() {
            GET_BACKTEST_PREPARATION => {
                let options = self
                    .authority
                    .options(&catalog)
                    .map_err(ServiceError::from)?;
                let count = options.datasets.len().max(1);
                (encode(&options)?, count)
            }
            PREVIEW_BACKTEST => {
                let input: BacktestPreviewRequest = decode(request.arguments())?;
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

    pub(super) async fn consume(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<GovernedBacktestInputRegistrationInput, ServiceError> {
        ensure_live(context)?;
        let input: BacktestStartRequest = decode_without_confirmation(request.arguments())?;
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

fn decode_without_confirmation<T: for<'de> Deserialize<'de>>(
    arguments: &Map<String, Value>,
) -> Result<T, ServiceError> {
    let mut admitted = arguments.clone();
    admitted.remove("confirm");
    decode(&admitted)
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
