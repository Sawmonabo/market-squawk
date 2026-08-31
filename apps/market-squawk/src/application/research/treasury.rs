//! Transport-neutral, exact-generation U.S. Treasury Macro operations.

use std::{
    collections::BTreeSet,
    fmt,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use chrono::{DateTime, Datelike, Utc};
use market_squawk_adapter_treasury::{
    TreasuryDailyRateFamily, TreasuryDailyRateQuery, TreasurySurface,
};
use market_squawk_data::{
    AnalyticalMacroLatestKnownOutput, AnalyticalMacroSeriesAllowlist, AnalyticalReadError,
    DatasetManifestRef, QueryLimits,
};
use market_squawk_domain::{CalendarDate, SourceIdentifier, Timestamp};
use market_squawk_services::{
    RequestContext, ServiceError, ServiceLimits, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;

use super::fred::{
    FredGenerationSelector, generation_matches, generation_selector_value, parse_calendar_date,
    parse_generation_selector,
};
use super::ingest::{
    TREASURY_DAILY_RATES_LATEST_KNOWN_OPERATION, TREASURY_FISCAL_DATA_LATEST_KNOWN_OPERATION,
    TreasuryApplicationClosure, TreasuryApplicationError, TreasuryDailyRatesLatestKnownReceipt,
    TreasuryDailyRatesLatestKnownRequest, TreasuryFiscalDataLatestKnownReceipt,
    TreasuryFiscalDataLatestKnownRequest, TreasuryMacroPublicationReceipt,
    TreasuryMacroRestartSelector,
};
use super::{encode_hex, map_query_error, map_read_error, parse_timestamp, query_limits};

const SCHEMA: &str = "market-squawk-treasury-latest-known-operation/v1";
const MAX_SERIES: usize = 32;
const INTERNAL_QUERY_BYTES: u64 = 256 * 1024;
const INTERNAL_QUERY_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
const INTERNAL_QUERY_MAXIMUM_DURATION: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub(crate) struct TreasuryLatestKnownOperation {
    operation: &'static str,
    surface: TreasurySurface,
    state: Arc<RwLock<Arc<TreasuryState>>>,
}

enum TreasuryState {
    SetupRequired,
    ConfiguredUnavailable,
    Ready {
        closure: Arc<TreasuryApplicationClosure>,
        generations: Box<[TreasuryPublishedGeneration]>,
    },
}

struct TreasuryPublishedGeneration {
    selector: TreasuryMacroRestartSelector,
    fixed_series: AnalyticalMacroSeriesAllowlist,
}

/// One manifest-pinned Treasury Macro selection retained below transport serialization.
///
/// The variant is intentionally internal. Product composition consumes [`Self::output`] and
/// preserves the underlying receipt as opaque evidence; provider state and operation names never
/// enter an ordinary product DTO.
#[derive(Debug)]
pub(crate) enum TreasuryCurrentAnalyticalRead {
    Fiscal(TreasuryFiscalDataLatestKnownReceipt),
    DailyRates(TreasuryDailyRatesLatestKnownReceipt),
}

impl TreasuryCurrentAnalyticalRead {
    /// Returns the selected canonical observations and exact query/selection evidence.
    pub(crate) const fn output(&self) -> &AnalyticalMacroLatestKnownOutput {
        match self {
            Self::Fiscal(receipt) => receipt.output(),
            Self::DailyRates(receipt) => receipt.output(),
        }
    }

    /// Returns the internal source surface without exposing it through product serialization.
    pub(crate) const fn surface(&self) -> TreasurySurface {
        match self {
            Self::Fiscal(_) => TreasurySurface::FiscalData,
            Self::DailyRates(_) => TreasurySurface::DailyRatesXml,
        }
    }
}

impl TreasuryLatestKnownOperation {
    pub(crate) fn fiscal_setup_required() -> Self {
        Self::setup(
            TREASURY_FISCAL_DATA_LATEST_KNOWN_OPERATION,
            TreasurySurface::FiscalData,
        )
    }
    pub(crate) fn daily_setup_required() -> Self {
        Self::setup(
            TREASURY_DAILY_RATES_LATEST_KNOWN_OPERATION,
            TreasurySurface::DailyRatesXml,
        )
    }
    fn setup(operation: &'static str, surface: TreasurySurface) -> Self {
        Self {
            operation,
            surface,
            state: Arc::new(RwLock::new(Arc::new(TreasuryState::SetupRequired))),
        }
    }
    pub(crate) fn configured_unavailable(&self) -> Result<(), ServiceError> {
        *self.state.write().map_err(|_| ServiceError::Unavailable)? =
            Arc::new(TreasuryState::ConfiguredUnavailable);
        Ok(())
    }
    /// Installs only publication receipts minted after exact catalog restart verification.
    pub(crate) fn install_published(
        &self,
        closure: Arc<TreasuryApplicationClosure>,
        receipts: Vec<TreasuryMacroPublicationReceipt>,
    ) -> Result<(), ServiceError> {
        let expected_generation_count = match self.surface {
            TreasurySurface::FiscalData => 1,
            TreasurySurface::DailyRatesXml => TreasuryDailyRateFamily::ALL.len(),
        };
        if receipts.len() != expected_generation_count || receipts.len() > MAX_SERIES {
            return Err(ServiceError::InvalidResult);
        }
        let mut generations = Vec::with_capacity(receipts.len());
        let mut daily_families = BTreeSet::new();
        for receipt in receipts {
            closure
                .verify_publication_receipt(&receipt)
                .map_err(map_error)?;
            let selector = receipt.restart_selector().clone();
            if selector.surface() != self.surface || receipt.manifest() != selector.manifest() {
                return Err(ServiceError::InvalidResult);
            }
            if self.surface == TreasurySurface::DailyRatesXml
                && !daily_families.insert(daily_rate_family(selector.manifest())?)
            {
                return Err(ServiceError::InvalidResult);
            }
            let fixed_series = fixed_series_for_generation(self.surface, &selector)?;
            generations.push(TreasuryPublishedGeneration {
                selector,
                fixed_series,
            });
        }
        generations.sort_by(|left, right| {
            left.selector
                .manifest()
                .dataset_id()
                .as_str()
                .cmp(right.selector.manifest().dataset_id().as_str())
        });
        if generations
            .windows(2)
            .any(|pair| pair[0].selector.manifest() == pair[1].selector.manifest())
            || (self.surface == TreasurySurface::DailyRatesXml
                && (daily_families.len() != TreasuryDailyRateFamily::ALL.len()
                    || TreasuryDailyRateFamily::ALL
                        .into_iter()
                        .any(|family| !daily_families.contains(&family))))
        {
            return Err(ServiceError::InvalidResult);
        }
        *self.state.write().map_err(|_| ServiceError::Unavailable)? =
            Arc::new(TreasuryState::Ready {
                closure,
                generations: generations.into_boxed_slice(),
            });
        Ok(())
    }

    pub(crate) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        if request.name() != self.operation {
            return Err(ServiceError::NotFound);
        }
        if context.cancellation().is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        if Instant::now() >= context.deadline() {
            return Err(ServiceError::DeadlineExceeded);
        }
        let invocation = parse_invocation(request.arguments())?;
        let state = self
            .state
            .read()
            .map_err(|_| ServiceError::Unavailable)?
            .clone();
        match state.as_ref() {
            TreasuryState::SetupRequired => {
                self.status("setup_required", "desired_activation_absent", None, limits)
            }
            TreasuryState::ConfiguredUnavailable => {
                self.status("unavailable", "exact_manifest_absent", None, limits)
            }
            TreasuryState::Ready { generations, .. }
                if matches!(&invocation, Invocation::Status) =>
            {
                self.status("ready", "manifest_bound", Some(generations), limits)
            }
            TreasuryState::Ready {
                closure,
                generations,
            } => {
                self.read(closure, generations, invocation, context, limits)
                    .await
            }
        }
    }

    /// Returns every current manifest-pinned Treasury Macro selection for neutral composition.
    ///
    /// Setup-required and configured-unavailable states produce an empty bounded result. Ready
    /// state reads remain pinned to the restart-verified manifests and retain their complete
    /// raw/native evidence receipts. No provider DTO is serialized or reparsed.
    pub(crate) async fn read_current_analytical_latest_known(
        &self,
        knowledge_cutoff: Timestamp,
        effective_date_cutoff: CalendarDate,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Box<[TreasuryCurrentAnalyticalRead]>, ServiceError> {
        if cancellation.is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(ServiceError::DeadlineExceeded);
        }
        let evaluated_at = evaluated_at()?;
        if knowledge_cutoff > evaluated_at
            || effective_date_cutoff > timestamp_calendar_date(knowledge_cutoff)?
        {
            return Err(ServiceError::InvalidRequest);
        }
        let state = self
            .state
            .read()
            .map_err(|_| ServiceError::Unavailable)?
            .clone();
        let TreasuryState::Ready {
            closure,
            generations,
        } = state.as_ref()
        else {
            return Ok(Vec::new().into_boxed_slice());
        };

        let mut selected = Vec::new();
        selected
            .try_reserve_exact(generations.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for generation in generations {
            if cancellation.is_cancelled() {
                return Err(ServiceError::Cancelled);
            }
            if Instant::now() >= deadline {
                return Err(ServiceError::DeadlineExceeded);
            }
            let selector = generation.selector.clone();
            let fixed_series =
                fixed_series_for_cutoff(self.surface, generation, effective_date_cutoff)?;
            let read = match self.surface {
                TreasurySurface::FiscalData => {
                    let request = TreasuryFiscalDataLatestKnownRequest::try_new(
                        selector.clone(),
                        fixed_series.clone(),
                        knowledge_cutoff,
                        effective_date_cutoff,
                    )
                    .map_err(map_error)?;
                    let limits = internal_query_limits(request.required_query_rows(), deadline)?;
                    closure
                        .read_fiscal_data_latest_known(
                            request,
                            limits,
                            deadline,
                            cancellation.clone(),
                        )
                        .await
                        .map(TreasuryCurrentAnalyticalRead::Fiscal)
                        .map_err(map_error)?
                }
                TreasurySurface::DailyRatesXml => {
                    let request = TreasuryDailyRatesLatestKnownRequest::try_new(
                        selector.clone(),
                        fixed_series.clone(),
                        knowledge_cutoff,
                        effective_date_cutoff,
                    )
                    .map_err(map_error)?;
                    let limits = internal_query_limits(request.required_query_rows(), deadline)?;
                    closure
                        .read_daily_rates_latest_known(
                            request,
                            limits,
                            deadline,
                            cancellation.clone(),
                        )
                        .await
                        .map(TreasuryCurrentAnalyticalRead::DailyRates)
                        .map_err(map_error)?
                }
            };
            let output = read.output();
            if read.surface() != self.surface
                || output.output().manifest() != selector.manifest()
                || output.observations().iter().any(|observation| {
                    !fixed_series.series().contains(observation.series())
                        || observation.context().provenance().source_id() != output.source_id()
                })
            {
                return Err(ServiceError::InvalidResult);
            }
            selected.push(read);
        }
        Ok(selected.into_boxed_slice())
    }

    fn status(
        &self,
        state: &str,
        reason: &str,
        generations: Option<&[TreasuryPublishedGeneration]>,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        let generation_values = generations.map(|items| {
            items
                .iter()
                .map(|item| generation_selector_value(item.selector.manifest()))
                .collect::<Vec<_>>()
        });
        let manifest_pinned = generations.is_some();
        result(
            json!({"schemaIdentity":SCHEMA,"operation":self.operation,"state":state,"reason":reason,"generations":generation_values}),
            0,
            json!({
                "operation": self.operation,
                "state": state,
                "generationCount": generations.map_or(0, |items| items.len()),
            }),
            quality(
                if manifest_pinned {
                    "manifest_bound_not_read"
                } else {
                    "unavailable"
                },
                manifest_pinned,
            ),
            limits,
        )
    }

    async fn read(
        &self,
        closure: &TreasuryApplicationClosure,
        generations: &[TreasuryPublishedGeneration],
        invocation: Invocation,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        let Invocation::Read {
            generation,
            knowledge,
            effective,
            series_subset,
        } = invocation
        else {
            return Err(ServiceError::InvalidRequest);
        };
        let evaluated_at = evaluated_at()?;
        if knowledge > evaluated_at {
            return Err(ServiceError::InvalidRequest);
        }
        let generation = generations
            .iter()
            .find(|item| generation_matches(&generation, item.selector.manifest()))
            .ok_or(ServiceError::NotFound)?;
        let selector = generation.selector.clone();
        let fixed_series = fixed_series_for_cutoff(self.surface, generation, effective)?;
        let allowlist = series_subset.intersect(&fixed_series)?;
        let output = match self.surface {
            TreasurySurface::FiscalData => {
                let request = TreasuryFiscalDataLatestKnownRequest::try_new(
                    selector, allowlist, knowledge, effective,
                )
                .map_err(map_error)?;
                let receipt = closure
                    .read_fiscal_data_latest_known(
                        request,
                        query_limits(limits, context)?,
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                    .map_err(map_error)?;
                json!({"generation":generation_selector_value(receipt.restart_selector().manifest()),"selectionDigest":encode_hex(receipt.output().selection_digest().bytes()),"observations":receipt.output().observations()})
            }
            TreasurySurface::DailyRatesXml => {
                let request = TreasuryDailyRatesLatestKnownRequest::try_new(
                    selector, allowlist, knowledge, effective,
                )
                .map_err(map_error)?;
                let receipt = closure
                    .read_daily_rates_latest_known(
                        request,
                        query_limits(limits, context)?,
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                    .map_err(map_error)?;
                json!({"generation":generation_selector_value(receipt.restart_selector().manifest()),"selectionDigest":encode_hex(receipt.output().selection_digest().bytes()),"observations":receipt.output().observations()})
            }
        };
        let knowledge_utc_date = timestamp_calendar_date(knowledge)?;
        result(
            json!({"schemaIdentity":SCHEMA,"operation":self.operation,"state":"ready","result":output}),
            1,
            json!({
                "operation": self.operation,
                "state": "ready",
                "evaluation": {
                    "knowledgeCutoff": knowledge,
                    "knowledgeCutoffUtcDate": knowledge_utc_date.to_string(),
                    "effectiveDateCutoff": effective.to_string(),
                    "evaluatedAt": evaluated_at,
                    "causalCutoffsValidated": true,
                },
            }),
            quality("official_delayed_point_in_time", true),
            limits,
        )
    }
}

impl fmt::Debug for TreasuryLatestKnownOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TreasuryLatestKnownOperation")
            .field("operation", &self.operation)
            .finish_non_exhaustive()
    }
}

enum Invocation {
    Status,
    Read {
        generation: FredGenerationSelector,
        knowledge: Timestamp,
        effective: CalendarDate,
        series_subset: TreasuryRequestedSeriesSubset,
    },
}

struct TreasuryRequestedSeriesSubset {
    series: Box<[SourceIdentifier]>,
}

impl TreasuryRequestedSeriesSubset {
    fn try_from_transport(values: &[Value]) -> Result<Self, ServiceError> {
        if values.is_empty() || values.len() > MAX_SERIES {
            return Err(ServiceError::InvalidRequest);
        }
        let mut series = Vec::with_capacity(values.len());
        for value in values {
            series.push(
                SourceIdentifier::try_from(value.as_str().ok_or(ServiceError::InvalidRequest)?)
                    .map_err(|_| ServiceError::InvalidRequest)?,
            );
        }
        series.sort_unstable();
        if series.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self {
            series: series.into_boxed_slice(),
        })
    }

    fn intersect(
        &self,
        fixed: &AnalyticalMacroSeriesAllowlist,
    ) -> Result<AnalyticalMacroSeriesAllowlist, ServiceError> {
        if self
            .series
            .iter()
            .any(|requested| fixed.series().binary_search(requested).is_err())
        {
            return Err(ServiceError::InvalidRequest);
        }
        let mut selected = Vec::with_capacity(self.series.len());
        for code_owned in fixed.series() {
            if self.series.binary_search(code_owned).is_ok() {
                selected.push(code_owned.clone());
            }
        }
        if selected.len() != self.series.len() {
            return Err(ServiceError::InvalidResult);
        }
        AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(selected)
            .map_err(|_| ServiceError::InvalidResult)
    }
}

fn parse_invocation(arguments: &Map<String, Value>) -> Result<Invocation, ServiceError> {
    if arguments.keys().any(|key| {
        !matches!(
            key.as_str(),
            "generation" | "knowledgeCutoff" | "effectiveDateCutoff" | "seriesIds" | "resultLimits"
        )
    }) {
        return Err(ServiceError::InvalidRequest);
    }
    let fields = (
        arguments.get("generation"),
        arguments.get("knowledgeCutoff"),
        arguments.get("effectiveDateCutoff"),
        arguments.get("seriesIds"),
    );
    match fields {
        (None, None, None, None) => Ok(Invocation::Status),
        (Some(g), Some(k), Some(e), Some(s)) => {
            let values = s
                .as_array()
                .filter(|v| !v.is_empty() && v.len() <= MAX_SERIES)
                .ok_or(ServiceError::InvalidRequest)?;
            let knowledge = parse_timestamp(k.as_str().ok_or(ServiceError::InvalidRequest)?)?;
            let effective = parse_calendar_date(e.as_str().ok_or(ServiceError::InvalidRequest)?)?;
            if effective > timestamp_calendar_date(knowledge)? {
                return Err(ServiceError::InvalidRequest);
            }
            Ok(Invocation::Read {
                generation: parse_generation_selector(g)?,
                knowledge,
                effective,
                series_subset: TreasuryRequestedSeriesSubset::try_from_transport(values)?,
            })
        }
        _ => Err(ServiceError::InvalidRequest),
    }
}

fn result(
    content: Value,
    items: usize,
    coverage: Value,
    quality: Value,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    let metadata = ToolResultMetadata::try_complete(coverage, quality)
        .map_err(|_| ServiceError::InvalidResult)?;
    TypedToolResult::try_new(content, items, metadata, limits).map_err(Into::into)
}

fn internal_query_limits(
    required_rows: u64,
    deadline: Instant,
) -> Result<QueryLimits, ServiceError> {
    let now = Instant::now();
    if now >= deadline {
        return Err(ServiceError::DeadlineExceeded);
    }
    QueryLimits::try_new_with_inline_bytes(
        required_rows,
        INTERNAL_QUERY_BYTES,
        INTERNAL_QUERY_BYTES,
        INTERNAL_QUERY_MEMORY_BYTES,
        4,
        2_048,
        4_096,
        deadline
            .saturating_duration_since(now)
            .min(INTERNAL_QUERY_MAXIMUM_DURATION),
    )
    .map_err(map_query_error)
}

fn quality(classification: &'static str, manifest_pinned: bool) -> Value {
    json!({
        "classification": classification,
        "manifestPinned": manifest_pinned,
        "executionEligible": false,
        "executionEligibility": "research_only_execution_ineligible",
    })
}

fn map_error(error: TreasuryApplicationError) -> ServiceError {
    match error {
        TreasuryApplicationError::Service(error) => error,
        TreasuryApplicationError::AnalyticalRead(error) => map_treasury_read_error(error),
        TreasuryApplicationError::InvalidSelection
        | TreasuryApplicationError::AuthorityInvalid
        | TreasuryApplicationError::InvalidAcquisition
        | TreasuryApplicationError::SurfaceMismatch
        | TreasuryApplicationError::RestartInvalid
        | TreasuryApplicationError::Capture(_)
        | TreasuryApplicationError::AllHistory(_) => ServiceError::InvalidResult,
        TreasuryApplicationError::Composition(_)
        | TreasuryApplicationError::Research(_)
        | TreasuryApplicationError::Ingest(_) => ServiceError::Unavailable,
    }
}

fn map_treasury_read_error(error: AnalyticalReadError) -> ServiceError {
    match error {
        error @ (AnalyticalReadError::ForecastDatasetUnavailable
        | AnalyticalReadError::Manifest(_)
        | AnalyticalReadError::Query(_)
        | AnalyticalReadError::Parquet(_)
        | AnalyticalReadError::PythonDataset(_)) => map_read_error(error),
        AnalyticalReadError::InvalidLimit
        | AnalyticalReadError::InstrumentLimitExceeded
        | AnalyticalReadError::InvalidKnowledgeRange
        | AnalyticalReadError::UniverseMembershipReadMustBeExhaustive
        | AnalyticalReadError::InvalidMarketBarLimit
        | AnalyticalReadError::InvalidMarketBarEffectiveRange
        | AnalyticalReadError::InvalidFundNavLimit
        | AnalyticalReadError::InvalidFundNavDateRange
        | AnalyticalReadError::InvalidMacroSeriesAllowlist
        | AnalyticalReadError::MacroSnapshotSourceOwnerMismatch
        | AnalyticalReadError::MacroSnapshotResultRequiresInline
        | AnalyticalReadError::MacroSnapshotCandidateSetSaturated
        | AnalyticalReadError::MacroSnapshotRevisionConflict
        | AnalyticalReadError::MacroSnapshotIncomplete
        | AnalyticalReadError::InvalidMacroSnapshotResult
        | AnalyticalReadError::InvalidOutcomeMarketBarWindow
        | AnalyticalReadError::MarketBarResultRequiresInline
        | AnalyticalReadError::InvalidMarketBarResult
        | AnalyticalReadError::FundNavResultRequiresInline
        | AnalyticalReadError::InvalidFundNavResult
        | AnalyticalReadError::InvalidObservationSchema => ServiceError::InvalidResult,
    }
}

fn fixed_series_for_generation(
    surface: TreasurySurface,
    selector: &TreasuryMacroRestartSelector,
) -> Result<AnalyticalMacroSeriesAllowlist, ServiceError> {
    match surface {
        TreasurySurface::FiscalData => selector
            .single_published_series()
            .cloned()
            .ok_or(ServiceError::InvalidResult),
        TreasurySurface::DailyRatesXml => {
            if !selector.is_all_history() {
                return Err(ServiceError::InvalidResult);
            }
            fixed_daily_rate_series(selector.manifest(), None)
        }
    }
}

fn fixed_series_for_cutoff(
    surface: TreasurySurface,
    generation: &TreasuryPublishedGeneration,
    effective_date_cutoff: CalendarDate,
) -> Result<AnalyticalMacroSeriesAllowlist, ServiceError> {
    match surface {
        TreasurySurface::FiscalData => Ok(generation.fixed_series.clone()),
        TreasurySurface::DailyRatesXml => {
            let selected = fixed_daily_rate_series(
                generation.selector.manifest(),
                Some(effective_date_cutoff.year()),
            )?;
            if selected.series().iter().any(|series| {
                generation
                    .fixed_series
                    .series()
                    .binary_search(series)
                    .is_err()
            }) {
                return Err(ServiceError::InvalidResult);
            }
            Ok(selected)
        }
    }
}

fn fixed_daily_rate_series(
    manifest: &DatasetManifestRef,
    effective_year: Option<u16>,
) -> Result<AnalyticalMacroSeriesAllowlist, ServiceError> {
    let family = daily_rate_family(manifest)?;
    let query =
        TreasuryDailyRateQuery::all_history(family).map_err(|_| ServiceError::InvalidResult)?;
    let series = family
        .dashboard_metrics()
        .into_iter()
        .filter(|metric| effective_year.is_none_or(|year| metric.first_schema_year() <= year))
        .map(|metric| {
            SourceIdentifier::try_from(metric.canonical_series())
                .map_err(|_| ServiceError::InvalidResult)
        })
        .collect::<Result<Vec<_>, _>>()?;
    AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(series)
        .map_err(|_| ServiceError::InvalidResult)
}

fn daily_rate_family(
    manifest: &DatasetManifestRef,
) -> Result<TreasuryDailyRateFamily, ServiceError> {
    TreasuryDailyRateFamily::ALL
        .into_iter()
        .find(|family| {
            TreasuryDailyRateQuery::all_history(*family).is_ok_and(|query| {
                query.analytical_dataset().as_str() == manifest.dataset_id().as_str()
            })
        })
        .ok_or(ServiceError::InvalidResult)
}

fn evaluated_at() -> Result<Timestamp, ServiceError> {
    Utc::now()
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::Unavailable)
}

fn timestamp_calendar_date(timestamp: Timestamp) -> Result<CalendarDate, ServiceError> {
    let date = DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos()).date_naive();
    CalendarDate::new(
        u16::try_from(date.year()).map_err(|_| ServiceError::InvalidRequest)?,
        u8::try_from(date.month()).map_err(|_| ServiceError::InvalidRequest)?,
        u8::try_from(date.day()).map_err(|_| ServiceError::InvalidRequest)?,
    )
    .map_err(|_| ServiceError::InvalidRequest)
}
