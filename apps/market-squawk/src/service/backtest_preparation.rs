//! Installed-service adapter for guided governed-backtest preparation.

use std::{collections::BTreeSet, sync::Arc, time::Instant};

use chrono::{DateTime, SecondsFormat};
use market_squawk_backtesting::ResearchExecutionAssumptionsInput;
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
        BacktestPreparationOptions, BacktestPreparationPreview, BacktestPreparationSelection,
        GovernedBacktestAuthority, GovernedBacktestInputRegistrationInput,
        GovernedBacktestPreparationAuthority,
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
                (preparation_options_value(&catalog, &options)?, count)
            }
            PREVIEW_BACKTEST => {
                let catalog = self.catalog(context).await?;
                let options = self
                    .authority
                    .options(&catalog)
                    .map_err(ServiceError::from)?;
                let input: BacktestPreviewRequest =
                    decode(&super::business_arguments(request.arguments()))?;
                let resolved = resolve_product_selection(&catalog, &options, input.selection)?;
                let preview = self
                    .authority
                    .preview(
                        &catalog,
                        resolved.selection.clone(),
                        context.origin().ok_or(ServiceError::Unauthorized)?,
                        self.workspace()?,
                        Instant::now(),
                        super::runtime::current_timestamp()
                            .map_err(|_error| ServiceError::Unavailable)?,
                    )
                    .map_err(ServiceError::from)?;
                (preparation_preview_value(&preview, &resolved)?, 1)
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
            .consume_token(
                &catalog,
                input.confirmation_token,
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
        let selection_cutoff =
            super::runtime::current_timestamp().map_err(|_error| ServiceError::Unavailable)?;
        let mut after: Option<DatasetId> = None;
        let mut datasets = Vec::new();
        loop {
            ensure_live(context)?;
            let page = self
                .analytical
                .feature_datasets(
                    FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1,
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
                        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1,
                        generation.manifest(),
                        selection_cutoff,
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
    selection: ProductBacktestSelection,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BacktestStartRequest {
    confirmation_token: Uuid,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProductBacktestSelection {
    history_token: Uuid,
    period_token: Uuid,
    method_token: Uuid,
    cost_token: Uuid,
    portfolio_token: Uuid,
    comparison_token: Uuid,
}

struct ResolvedProductBacktestSelection {
    selection: BacktestPreparationSelection,
    investment_universe: String,
    period: String,
    method: String,
    costs: ResearchExecutionAssumptionsInput,
    portfolio: String,
    comparison: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BacktestProductRequest {
    backtest_token: Uuid,
}

fn preparation_options_value(
    catalog: &BacktestPreparationCatalog,
    options: &BacktestPreparationOptions,
) -> Result<Value, ServiceError> {
    let histories = options
        .datasets
        .iter()
        .map(|history| {
            let history_token = history_token(catalog, &history.id);
            let periods = history
                .periods
                .iter()
                .map(|period| {
                    serde_json::json!({
                        "periodToken": period_token(
                            history_token,
                            &period.id,
                            &period.starts_at,
                            &period.ends_at,
                        ),
                        "label": product_period_label(&period.id),
                        "startsAt": period.starts_at,
                        "endsAt": period.ends_at,
                    })
                })
                .collect::<Vec<_>>();
            serde_json::json!({
                "historyToken": history_token,
                "label": format!(
                    "Point-in-time history for {} investments",
                    history.instrument_count
                ),
                "investmentCount": history.instrument_count,
                "periods": periods,
            })
        })
        .collect::<Vec<_>>();
    let methods = options
        .strategies
        .iter()
        .map(|method| {
            (method.id == "baseline-buy-once")
                .then(|| {
                    named_choice_value(
                        method_token(method.id),
                        "Buy-and-hold baseline",
                        "Buy each eligible investment once and hold it through the selected period.",
                    )
                })
                .ok_or(ServiceError::InvalidResult)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let cost_plans = options
        .cost_policies
        .iter()
        .map(|cost| {
            let label = match cost.id {
                "standard" => "Standard trading costs",
                "conservative" => "Conservative trading costs",
                _ => return Err(ServiceError::InvalidResult),
            };
            let assumptions = options
                .execution_assumptions(cost.id)
                .map_err(ServiceError::from)?;
            Ok(named_choice_value(
                cost_token(cost.id),
                label,
                &cost_choice_description(assumptions)?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let portfolios = options
        .portfolios
        .iter()
        .map(|portfolio| {
            let description = match portfolio.id {
                "research-usd-100k" => "Start the historical simulation with $100,000.",
                "research-usd-1m" => "Start the historical simulation with $1,000,000.",
                _ => return Err(ServiceError::InvalidResult),
            };
            Ok(named_choice_value(
                portfolio_token(portfolio.id),
                portfolio.label,
                description,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let comparisons = options
        .comparisons
        .iter()
        .map(|comparison| {
            let description = match comparison.id {
                "single-run" => "Evaluate the selected approach once over the chosen period.",
                "walk-forward-robustness" => {
                    "Compare predeclared variants across two later, independent test windows."
                }
                _ => return Err(ServiceError::InvalidResult),
            };
            Ok(named_choice_value(
                comparison_token(comparison.id),
                comparison.label,
                description,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(serde_json::json!({
        "histories": histories,
        "methods": methods,
        "costPlans": cost_plans,
        "portfolios": portfolios,
        "comparisons": comparisons,
        "guidance": "Choose a period, investment approach, realistic trading costs, portfolio size, and evaluation method. Results disclose incomplete coverage and weak out-of-sample evidence rather than treating either as success.",
    }))
}

fn resolve_product_selection(
    catalog: &BacktestPreparationCatalog,
    options: &BacktestPreparationOptions,
    selected: ProductBacktestSelection,
) -> Result<ResolvedProductBacktestSelection, ServiceError> {
    let history = unique_match(&options.datasets, selected.history_token, |history| {
        history_token(catalog, &history.id)
    })?;
    let retained_history_token = history_token(catalog, &history.id);
    let period = unique_match(&history.periods, selected.period_token, |period| {
        period_token(
            retained_history_token,
            &period.id,
            &period.starts_at,
            &period.ends_at,
        )
    })?;
    let method = unique_match(&options.strategies, selected.method_token, |method| {
        method_token(method.id)
    })?;
    let cost = unique_match(&options.cost_policies, selected.cost_token, |cost| {
        cost_token(cost.id)
    })?;
    let portfolio = unique_match(&options.portfolios, selected.portfolio_token, |portfolio| {
        portfolio_token(portfolio.id)
    })?;
    let comparison = unique_match(
        &options.comparisons,
        selected.comparison_token,
        |comparison| comparison_token(comparison.id),
    )?;
    let costs = options
        .execution_assumptions(cost.id)
        .map_err(ServiceError::from)?;
    Ok(ResolvedProductBacktestSelection {
        selection: BacktestPreparationSelection {
            dataset: history.id.clone(),
            period: period.id.clone(),
            strategy: method.id.to_owned(),
            cost_policy: cost.id.to_owned(),
            seed: "release-baseline".to_owned(),
            portfolio: portfolio.id.to_owned(),
            comparison: comparison.id.to_owned(),
        },
        investment_universe: format!(
            "{} investments with point-in-time history",
            history.instrument_count
        ),
        period: format!(
            "{} ({} through {})",
            product_period_label(&period.id),
            product_date(&period.starts_at)?,
            product_date(&period.ends_at)?,
        ),
        method: "Buy-and-hold baseline".to_owned(),
        costs,
        portfolio: portfolio.label.to_owned(),
        comparison: comparison.label.to_owned(),
    })
}

fn preparation_preview_value(
    preview: &BacktestPreparationPreview,
    resolved: &ResolvedProductBacktestSelection,
) -> Result<Value, ServiceError> {
    let selection = &resolved.selection;
    let out_of_sample = selection.comparison == "walk-forward-robustness";
    let limitations = if out_of_sample {
        vec![
            "Two test windows are useful evidence but remain a limited sample of possible market conditions.",
            "Historical performance cannot guarantee future profit or remove investment risk.",
            "Missing or incomplete history reduces coverage and must be reviewed in the completed result.",
        ]
    } else {
        vec![
            "This single run does not provide independent out-of-sample evidence.",
            "Historical performance cannot guarantee future profit or remove investment risk.",
            "Missing or incomplete history reduces coverage and must be reviewed in the completed result.",
        ]
    };
    Ok(serde_json::json!({
        "confirmationToken": preview.receipt.receipt_id(),
        "expiresAt": preview.expires_at,
        "investmentUniverse": resolved.investment_universe,
        "period": resolved.period,
        "method": resolved.method,
        "costs": cost_assumptions_value(resolved.costs)?,
        "portfolio": resolved.portfolio,
        "comparison": resolved.comparison,
        "pointInTimeEvidence": "verified",
        "outOfSamplePlan": if out_of_sample {
            "Two anchored evaluation folds compare predeclared variants on later, independent test windows."
        } else {
            "No independent test window is selected; the completed result will be marked as limited."
        },
        "evidence": [
            "Each simulated decision uses only information known by that decision time.",
            "Historical membership changes and delistings remain part of the evaluated investment universe.",
            "The reviewed history, period, costs, portfolio, and evaluation method are revalidated when the run starts."
        ],
        "assumptions": [
            "Trading fees, bid/ask spread, slippage, latency, participation limits, and partial fills are applied before performance is reported.",
            "The reproducibility control is fixed by the application and cannot be selected or changed from this screen.",
            "This is historical investment research, not a forecast or permission to trade."
        ],
        "limitations": limitations,
        "analysisOnly": true,
    }))
}

fn cost_choice_description(
    assumptions: ResearchExecutionAssumptionsInput,
) -> Result<String, ServiceError> {
    Ok(format!(
        "Use {} fees, {} slippage, up to {} additional market-impact variation, {}, and a {} participation limit.",
        basis_points_percent(assumptions.fee_basis_points.get()),
        basis_points_percent(assumptions.slippage_basis_points.get()),
        basis_points_percent(assumptions.maximum_random_slippage_basis_points.get()),
        latency_copy(assumptions.latency_nanos)?,
        basis_points_percent(assumptions.maximum_participation_basis_points.get()),
    ))
}

fn cost_assumptions_value(
    assumptions: ResearchExecutionAssumptionsInput,
) -> Result<Value, ServiceError> {
    Ok(serde_json::json!({
        "fees": format!("{} per fill", basis_points_percent(assumptions.fee_basis_points.get())),
        "spread": "The observed historical bid/ask spread when available",
        "slippage": format!(
            "{} plus up to {} additional market-impact variation",
            basis_points_percent(assumptions.slippage_basis_points.get()),
            basis_points_percent(assumptions.maximum_random_slippage_basis_points.get()),
        ),
        "latency": format!("{} before an order may fill", latency_copy(assumptions.latency_nanos)?),
        "participationLimit": format!(
            "At most {} of evidenced executable depth",
            basis_points_percent(assumptions.maximum_participation_basis_points.get()),
        ),
        "partialFills": if assumptions.allow_partial_fills {
            "Allowed when the full amount is not executable"
        } else {
            "Not allowed"
        },
    }))
}

fn basis_points_percent(basis_points: i32) -> String {
    let sign = if basis_points < 0 { "-" } else { "" };
    let absolute = basis_points.unsigned_abs();
    let whole = absolute / 100;
    let fraction = absolute % 100;
    if fraction == 0 {
        format!("{sign}{whole}%")
    } else if fraction.is_multiple_of(10) {
        format!("{sign}{whole}.{}%", fraction / 10)
    } else {
        format!("{sign}{whole}.{fraction:02}%")
    }
}

fn latency_copy(nanos: i64) -> Result<String, ServiceError> {
    const MICROSECOND: i64 = 1_000;
    const MILLISECOND: i64 = 1_000 * MICROSECOND;
    const SECOND: i64 = 1_000 * MILLISECOND;

    let (amount, unit) = if nanos > 0 && nanos % SECOND == 0 {
        (nanos / SECOND, "second")
    } else if nanos > 0 && nanos % MILLISECOND == 0 {
        (nanos / MILLISECOND, "millisecond")
    } else if nanos > 0 && nanos % MICROSECOND == 0 {
        (nanos / MICROSECOND, "microsecond")
    } else {
        return Err(ServiceError::InvalidResult);
    };
    Ok(format!(
        "{amount} {unit}{}",
        if amount == 1 { "" } else { "s" }
    ))
}

fn named_choice_value(token: Uuid, label: &str, description: &str) -> Value {
    serde_json::json!({
        "token": token,
        "label": label,
        "description": description,
    })
}

fn unique_match<T>(
    choices: &[T],
    selected: Uuid,
    mut token: impl FnMut(&T) -> Uuid,
) -> Result<&T, ServiceError> {
    let mut matches = choices.iter().filter(|choice| token(choice) == selected);
    let retained = matches.next().ok_or(ServiceError::InvalidRequest)?;
    if matches.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(retained)
}

fn history_token(catalog: &BacktestPreparationCatalog, history_id: &str) -> Uuid {
    let catalog_digest = catalog.digest();
    opaque_product_token(
        b"market-squawk/backtest-history-choice/v1\0",
        &[&catalog_digest, history_id.as_bytes()],
    )
}

fn period_token(history_token: Uuid, period_id: &str, starts_at: &str, ends_at: &str) -> Uuid {
    opaque_product_token(
        b"market-squawk/backtest-period-choice/v1\0",
        &[
            history_token.as_bytes(),
            period_id.as_bytes(),
            starts_at.as_bytes(),
            ends_at.as_bytes(),
        ],
    )
}

fn method_token(method_id: &str) -> Uuid {
    named_choice_token(b"market-squawk/backtest-method-choice/v1\0", method_id)
}

fn cost_token(cost_id: &str) -> Uuid {
    named_choice_token(b"market-squawk/backtest-cost-choice/v1\0", cost_id)
}

fn portfolio_token(portfolio_id: &str) -> Uuid {
    named_choice_token(
        b"market-squawk/backtest-portfolio-choice/v1\0",
        portfolio_id,
    )
}

fn comparison_token(comparison_id: &str) -> Uuid {
    named_choice_token(
        b"market-squawk/backtest-comparison-choice/v1\0",
        comparison_id,
    )
}

fn named_choice_token(domain: &'static [u8], identifier: &str) -> Uuid {
    opaque_product_token(domain, &[identifier.as_bytes()])
}

fn product_period_label(period_id: &str) -> &'static str {
    match period_id {
        "recent-half" => "Recent half of available history",
        _ => "Full available history",
    }
}

fn product_date(timestamp: &str) -> Result<&str, ServiceError> {
    timestamp.get(..10).ok_or(ServiceError::InvalidResult)
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
    let (state, _status_message) = product_activity_state(view);
    Ok(serde_json::json!({
        "backtestToken": product_backtest_token(view),
        "label": "Investment approach backtest",
        "startedAt": timestamp_text(view.started_at()),
        "updatedAt": timestamp_text(view.updated_at()),
        "state": state,
        "progressPercent": product_progress_percent(view),
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
