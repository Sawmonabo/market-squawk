//! Provider-neutral Macro product context over exact canonical point-in-time reads.

use std::{fmt, sync::Arc, time::Duration};

use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use market_squawk_adapter_federal_reserve::{
    BOARD_DDP_SOURCE_ID, BoardDatasetFamily, BoardDatasetProfile, BoardFrequency, BoardRelease,
    h15_treasury_constant_maturities_canonical_unit_identifier,
    h15_treasury_constant_maturities_dashboard_series,
};
use market_squawk_adapter_treasury::{TreasuryDailyRateMetric, TreasuryMaturity, TreasurySurface};
use market_squawk_data::{
    AnalyticalMacroLatestKnownOutput, AnalyticalMacroLatestKnownRequest,
    AnalyticalMacroSeriesAllowlist, AnalyticalReadCapability, DatasetId, DatasetManifestRef,
    QueryLimits,
};
use market_squawk_domain::{
    CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest, MacroObservation, PayloadReference,
    ResearchTemporalCoordinate, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_services::{
    RequestContext, ServiceError, ServiceLimits, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::treasury::{TreasuryCurrentAnalyticalRead, TreasuryLatestKnownOperation};
use super::{FredLatestKnownOperation, map_query_error, map_read_error, parse_timestamp};

pub(crate) const MACRO_GET_CONTEXT: &str = "Macro.GetContext";

const KNOWLEDGE_CUTOFF_FIELD: &str = "knowledgeCutoff";
const EFFECTIVE_DATE_CUTOFF_FIELD: &str = "effectiveDateCutoff";
const RESULT_LIMITS_FIELD: &str = "resultLimits";
const FRED_SOURCE_ID: &str = "fred-fred-alfred.api-v1-v2";
const FRED_UNEMPLOYMENT_SERIES_ID: &str = "UNRATE";
const FRED_UNEMPLOYMENT_UNIT_ID: &str = "fred-unit:v1:Percent";
const TREASURY_PERCENT_UNIT_ID: &str = "percent";
const H15_INDICATOR_COUNT: usize = 11;
const MACRO_CONTEXT_INDICATOR_COUNT: usize = 12;
const MAXIMUM_MACRO_CONTEXT_INPUTS: usize = 4_096;
const MAXIMUM_TIMESTAMP_BYTES: usize = 64;
const MACRO_CONTEXT_QUERY_BYTES: u64 = 256 * 1024;
const MACRO_CONTEXT_QUERY_MEMORY_BYTES: u64 = 64 * 1024 * 1024;

const INTEREST_RATE_UNIT: MacroContextUnitDto = MacroContextUnitDto {
    code: "percent_per_year",
    label: "Percent per year",
    symbol: Some("%"),
};
const UNEMPLOYMENT_UNIT: MacroContextUnitDto = MacroContextUnitDto {
    code: "percent_of_labor_force",
    label: "Percent of labor force",
    symbol: Some("%"),
};

const H15_INDICATORS: [MacroContextIndicatorDefinition; H15_INDICATOR_COUNT] = [
    MacroContextIndicatorDefinition::interest_rate(
        "us-government-yield-1m",
        "1-month government bond yield",
        "1m",
    ),
    MacroContextIndicatorDefinition::interest_rate(
        "us-government-yield-3m",
        "3-month government bond yield",
        "3m",
    ),
    MacroContextIndicatorDefinition::interest_rate(
        "us-government-yield-6m",
        "6-month government bond yield",
        "6m",
    ),
    MacroContextIndicatorDefinition::interest_rate(
        "us-government-yield-1y",
        "1-year government bond yield",
        "1y",
    ),
    MacroContextIndicatorDefinition::interest_rate(
        "us-government-yield-2y",
        "2-year government bond yield",
        "2y",
    ),
    MacroContextIndicatorDefinition::interest_rate(
        "us-government-yield-3y",
        "3-year government bond yield",
        "3y",
    ),
    MacroContextIndicatorDefinition::interest_rate(
        "us-government-yield-5y",
        "5-year government bond yield",
        "5y",
    ),
    MacroContextIndicatorDefinition::interest_rate(
        "us-government-yield-7y",
        "7-year government bond yield",
        "7y",
    ),
    MacroContextIndicatorDefinition::interest_rate(
        "us-government-yield-10y",
        "10-year government bond yield",
        "10y",
    ),
    MacroContextIndicatorDefinition::interest_rate(
        "us-government-yield-20y",
        "20-year government bond yield",
        "20y",
    ),
    MacroContextIndicatorDefinition::interest_rate(
        "us-government-yield-30y",
        "30-year government bond yield",
        "30y",
    ),
];

const UNEMPLOYMENT_INDICATOR: MacroContextIndicatorDefinition = MacroContextIndicatorDefinition {
    indicator_id: "us-unemployment-rate",
    label: "U.S. unemployment rate",
    category: MacroContextCategory::LaborMarket,
    frequency: MacroContextFrequency::Monthly,
    seasonal_adjustment: MacroContextSeasonalAdjustment::SeasonallyAdjusted,
    unit: UNEMPLOYMENT_UNIT,
    source_slot: FRED_UNEMPLOYMENT_SERIES_ID,
};

/// Reusable provider-neutral point-in-time Macro selection below transport serialization.
///
/// Provider-qualified canonical evidence remains inside the opaque receipt and typed inputs.
/// Ordinary product DTOs receive only economic meaning, explicit missingness, and selection
/// confidence.
#[derive(Clone)]
pub(crate) struct MacroContextReadCapability {
    reader: AnalyticalReadCapability,
    fred: FredLatestKnownOperation,
    treasury_fiscal: Option<TreasuryLatestKnownOperation>,
    treasury_daily: Option<TreasuryLatestKnownOperation>,
}

impl MacroContextReadCapability {
    /// Binds the currently composed canonical Board/FRED read authorities.
    #[must_use]
    pub(crate) fn new(reader: AnalyticalReadCapability, fred: FredLatestKnownOperation) -> Self {
        Self {
            reader,
            fred,
            treasury_fiscal: None,
            treasury_daily: None,
        }
    }

    /// Adds the restart-safe Treasury read authorities without changing the public projection.
    #[must_use]
    pub(crate) fn with_treasury(
        reader: AnalyticalReadCapability,
        fred: FredLatestKnownOperation,
        treasury_fiscal: TreasuryLatestKnownOperation,
        treasury_daily: TreasuryLatestKnownOperation,
    ) -> Self {
        Self {
            reader,
            fred,
            treasury_fiscal: Some(treasury_fiscal),
            treasury_daily: Some(treasury_daily),
        }
    }

    /// Selects one bounded current or explicit point-in-time neutral Macro snapshot.
    pub(crate) async fn read_latest_known(
        &self,
        knowledge_cutoff: Timestamp,
        effective_date_cutoff: CalendarDate,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<MacroContextSnapshot, ServiceError> {
        let evaluated_at = current_timestamp()?;
        if knowledge_cutoff > evaluated_at
            || effective_date_cutoff > timestamp_calendar_date(knowledge_cutoff)?
        {
            return Err(ServiceError::InvalidRequest);
        }
        self.read_at(
            MacroContextCutoffs {
                knowledge_cutoff,
                effective_date_cutoff,
                evaluated_at,
            },
            deadline,
            cancellation,
        )
        .await
    }

    async fn read_at(
        &self,
        cutoffs: MacroContextCutoffs,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<MacroContextSnapshot, ServiceError> {
        if cancellation.is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        if std::time::Instant::now() >= deadline {
            return Err(ServiceError::DeadlineExceeded);
        }
        let board_cancellation = cancellation.child_token();
        let fred_cancellation = cancellation.child_token();
        let treasury_cancellation = cancellation.child_token();
        let board =
            Box::pin(async move { self.read_board(cutoffs, deadline, board_cancellation).await });
        let fred =
            Box::pin(async move { self.read_fred(cutoffs, deadline, fred_cancellation).await });
        let treasury = Box::pin(async move {
            self.read_treasury(cutoffs, deadline, treasury_cancellation)
                .await
        });
        let (board, fred, (treasury_fiscal, treasury_daily)) =
            tokio::try_join!(board, fred, treasury)?;
        product_snapshot(cutoffs, board, fred, treasury_fiscal, treasury_daily)
    }

    async fn read_board(
        &self,
        cutoffs: MacroContextCutoffs,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<AnalyticalMacroLatestKnownOutput>, ServiceError> {
        let profile = BoardDatasetProfile::h15_treasury_constant_maturities_rolling_dashboard()
            .map_err(|_| ServiceError::Unavailable)?;
        let contract = profile.contract();
        if contract.release() != BoardRelease::H15SelectedInterestRates
            || contract.family() != BoardDatasetFamily::H15TreasuryConstantMaturities
            || contract.frequency() != BoardFrequency::BusinessDaily
            || h15_treasury_constant_maturities_dashboard_series().len() != H15_INDICATOR_COUNT
        {
            return Err(ServiceError::Unavailable);
        }

        let dataset = DatasetId::try_from(profile.analytical_dataset().as_str())
            .map_err(|_| ServiceError::Unavailable)?;
        let Some(generation) = self
            .reader
            .latest(&dataset, deadline, &cancellation)
            .map_err(map_read_error)?
        else {
            return Ok(None);
        };
        let source_id =
            SourceId::try_from(BOARD_DDP_SOURCE_ID).map_err(|_| ServiceError::Unavailable)?;
        if generation.source_id() != &source_id || generation.manifest().dataset_id() != &dataset {
            return Err(ServiceError::InvalidResult);
        }

        let mut series = Vec::new();
        series
            .try_reserve_exact(H15_INDICATOR_COUNT)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for descriptor in h15_treasury_constant_maturities_dashboard_series() {
            series.push(
                descriptor
                    .canonical_macro_series_identifier()
                    .map_err(|_| ServiceError::Unavailable)?,
            );
        }
        let allowlist = AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(series)
            .map_err(map_read_error)?;
        let request = AnalyticalMacroLatestKnownRequest::try_new(
            generation.manifest().clone(),
            source_id,
            cutoffs.knowledge_cutoff,
            cutoffs.effective_date_cutoff,
            allowlist,
        )
        .map_err(map_read_error)?;
        let query_limits = macro_context_query_limits(&request, deadline)?;
        let output = self
            .reader
            .read_macro_latest_known_snapshot(request, query_limits, deadline, cancellation)
            .await
            .map_err(map_read_error)?;
        if output.output().manifest() != generation.manifest() {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Some(output))
    }

    async fn read_fred(
        &self,
        cutoffs: MacroContextCutoffs,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<AnalyticalMacroLatestKnownOutput>, ServiceError> {
        self.fred
            .read_current_analytical_latest_known(
                cutoffs.knowledge_cutoff,
                cutoffs.effective_date_cutoff,
                deadline,
                cancellation,
            )
            .await
    }

    async fn read_treasury(
        &self,
        cutoffs: MacroContextCutoffs,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<
        (
            Box<[TreasuryCurrentAnalyticalRead]>,
            Box<[TreasuryCurrentAnalyticalRead]>,
        ),
        ServiceError,
    > {
        let fiscal = async {
            let Some(operation) = self.treasury_fiscal.as_ref() else {
                return Ok(Vec::new().into_boxed_slice());
            };
            operation
                .read_current_analytical_latest_known(
                    cutoffs.knowledge_cutoff,
                    cutoffs.effective_date_cutoff,
                    deadline,
                    cancellation.child_token(),
                )
                .await
        };
        let daily = async {
            let Some(operation) = self.treasury_daily.as_ref() else {
                return Ok(Vec::new().into_boxed_slice());
            };
            operation
                .read_current_analytical_latest_known(
                    cutoffs.knowledge_cutoff,
                    cutoffs.effective_date_cutoff,
                    deadline,
                    cancellation.child_token(),
                )
                .await
        };
        tokio::try_join!(fiscal, daily)
    }
}

impl fmt::Debug for MacroContextReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MacroContextReadCapability")
            .field("analytical", &self.reader)
            .field("treasury_composed", &self.treasury_daily.is_some())
            .finish_non_exhaustive()
    }
}

/// One application-owned transport wrapper around [`MacroContextReadCapability`].
pub(crate) struct MacroContextOperation {
    read: MacroContextReadCapability,
}

impl MacroContextOperation {
    /// Preserves the current Board/FRED composition until the shared integration owner wires the
    /// already-created Treasury operations into [`Self::with_treasury`].
    #[must_use]
    pub(crate) fn new(reader: AnalyticalReadCapability, fred: FredLatestKnownOperation) -> Self {
        Self {
            read: MacroContextReadCapability::new(reader, fred),
        }
    }

    /// Binds all currently durable Board/FRED/Treasury inputs below one neutral product read.
    #[must_use]
    pub(crate) fn with_treasury(
        reader: AnalyticalReadCapability,
        fred: FredLatestKnownOperation,
        treasury_fiscal: TreasuryLatestKnownOperation,
        treasury_daily: TreasuryLatestKnownOperation,
    ) -> Self {
        Self {
            read: MacroContextReadCapability::with_treasury(
                reader,
                fred,
                treasury_fiscal,
                treasury_daily,
            ),
        }
    }

    /// Returns the reusable typed read capability without granting provider mutation authority.
    #[must_use]
    pub(crate) fn read_capability(&self) -> MacroContextReadCapability {
        self.read.clone()
    }

    /// Executes one bounded provider-neutral current or explicit point-in-time Macro read.
    pub(crate) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(request, context)?;
        if limits.maximum_result_items() < MACRO_CONTEXT_INDICATOR_COUNT {
            return Err(ServiceError::ResourceExhausted);
        }
        let cutoffs = MacroContextCutoffs::parse(request.arguments())?;
        self.read
            .read_at(
                cutoffs,
                context.deadline(),
                context.cancellation().child_token(),
            )
            .await?
            .into_tool_result(limits)
    }
}

impl fmt::Debug for MacroContextOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MacroContextOperation")
            .field("operation", &MACRO_GET_CONTEXT)
            .field("read", &self.read)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy)]
struct MacroContextCutoffs {
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: CalendarDate,
    evaluated_at: Timestamp,
}

impl MacroContextCutoffs {
    fn parse(arguments: &serde_json::Map<String, Value>) -> Result<Self, ServiceError> {
        if arguments.keys().any(|field| {
            !matches!(
                field.as_str(),
                KNOWLEDGE_CUTOFF_FIELD | EFFECTIVE_DATE_CUTOFF_FIELD | RESULT_LIMITS_FIELD
            )
        }) {
            return Err(ServiceError::InvalidRequest);
        }
        let evaluated_at = current_timestamp()?;
        match (
            arguments.get(KNOWLEDGE_CUTOFF_FIELD),
            arguments.get(EFFECTIVE_DATE_CUTOFF_FIELD),
        ) {
            (None, None) => Ok(Self {
                knowledge_cutoff: evaluated_at,
                effective_date_cutoff: timestamp_calendar_date(evaluated_at)?,
                evaluated_at,
            }),
            (Some(knowledge_cutoff), Some(effective_date_cutoff)) => {
                let knowledge_cutoff = knowledge_cutoff
                    .as_str()
                    .filter(|value| !value.is_empty() && value.len() <= MAXIMUM_TIMESTAMP_BYTES)
                    .ok_or(ServiceError::InvalidRequest)
                    .and_then(parse_timestamp)?;
                let effective_date_cutoff = effective_date_cutoff
                    .as_str()
                    .ok_or(ServiceError::InvalidRequest)
                    .and_then(parse_calendar_date)?;
                if knowledge_cutoff > evaluated_at
                    || effective_date_cutoff > timestamp_calendar_date(knowledge_cutoff)?
                {
                    return Err(ServiceError::InvalidRequest);
                }
                Ok(Self {
                    knowledge_cutoff,
                    effective_date_cutoff,
                    evaluated_at,
                })
            }
            (Some(_), None) | (None, Some(_)) => Err(ServiceError::InvalidRequest),
        }
    }
}

#[derive(Clone, Copy)]
struct MacroContextIndicatorDefinition {
    indicator_id: &'static str,
    label: &'static str,
    category: MacroContextCategory,
    frequency: MacroContextFrequency,
    seasonal_adjustment: MacroContextSeasonalAdjustment,
    unit: MacroContextUnitDto,
    source_slot: &'static str,
}

impl MacroContextIndicatorDefinition {
    const fn interest_rate(
        indicator_id: &'static str,
        label: &'static str,
        source_slot: &'static str,
    ) -> Self {
        Self {
            indicator_id,
            label,
            category: MacroContextCategory::InterestRates,
            frequency: MacroContextFrequency::BusinessDaily,
            seasonal_adjustment: MacroContextSeasonalAdjustment::NotApplicable,
            unit: INTEREST_RATE_UNIT,
            source_slot,
        }
    }

    fn unavailable(self) -> MacroContextObservationDto {
        MacroContextObservationDto {
            indicator_id: self.indicator_id,
            label: self.label,
            category: self.category,
            frequency: self.frequency,
            seasonal_adjustment: self.seasonal_adjustment,
            unit: self.unit,
            effective_date: None,
            recorded: MacroContextRecordedDateDto::NotSupplied,
            available_at: None,
            revision: None,
            superseded_after: None,
            value: MacroContextValueDto::Missing {
                reason: MacroContextMissingReason::Unavailable,
                explanation: "No observation is available at this cutoff.",
            },
            availability: MacroContextObservationAvailability::Unavailable,
            confidence: MacroContextConfidenceDto::unavailable(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MacroContextDto {
    availability: MacroContextAvailability,
    selection: MacroContextSelectionDto,
    confidence: MacroContextConfidenceDto,
    coverage: MacroContextCoverageDto,
    observations: Vec<MacroContextObservationDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MacroContextSelectionDto {
    knowledge_cutoff: String,
    effective_date_cutoff: String,
    evaluated_at: String,
    complete: bool,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MacroContextConfidenceDto {
    level: MacroContextConfidenceLevel,
    summary: &'static str,
}

impl MacroContextConfidenceDto {
    const fn moderate() -> Self {
        Self {
            level: MacroContextConfidenceLevel::Moderate,
            summary: "Official delayed observation selected at the requested cutoff.",
        }
    }

    const fn limited() -> Self {
        Self {
            level: MacroContextConfidenceLevel::Limited,
            summary: "The observation is explicitly missing at the requested cutoff.",
        }
    }

    const fn unavailable() -> Self {
        Self {
            level: MacroContextConfidenceLevel::Unavailable,
            summary: "No observation is available at the requested cutoff.",
        }
    }
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MacroContextConfidenceLevel {
    Moderate,
    Limited,
    Unavailable,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MacroContextCoverageDto {
    requested: usize,
    observed: usize,
    missing: usize,
    unavailable: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MacroContextObservationDto {
    indicator_id: &'static str,
    label: &'static str,
    category: MacroContextCategory,
    frequency: MacroContextFrequency,
    seasonal_adjustment: MacroContextSeasonalAdjustment,
    unit: MacroContextUnitDto,
    effective_date: Option<String>,
    recorded: MacroContextRecordedDateDto,
    available_at: Option<String>,
    revision: Option<u32>,
    superseded_after: Option<String>,
    value: MacroContextValueDto,
    availability: MacroContextObservationAvailability,
    confidence: MacroContextConfidenceDto,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MacroContextCategory {
    InterestRates,
    LaborMarket,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MacroContextFrequency {
    BusinessDaily,
    Monthly,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MacroContextSeasonalAdjustment {
    NotApplicable,
    SeasonallyAdjusted,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MacroContextUnitDto {
    code: &'static str,
    label: &'static str,
    symbol: Option<&'static str>,
}

#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum MacroContextRecordedDateDto {
    Known { date: String },
    NotSupplied,
}

#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum MacroContextValueDto {
    Observed {
        decimal: String,
    },
    Missing {
        reason: MacroContextMissingReason,
        explanation: &'static str,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MacroContextMissingReason {
    NotReported,
    Unavailable,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MacroContextObservationAvailability {
    Available,
    Missing,
    Unavailable,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MacroContextAvailability {
    Available,
    Partial,
    Unavailable,
}

/// Provider-neutral economic role for one retained canonical Macro input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MacroContextInputRole {
    GovernmentYieldCurve,
    ShortTermGovernmentFunding,
    InflationAdjustedGovernmentRates,
    GovernmentBorrowingCost,
    LaborMarket,
}

/// One typed canonical input retained by the neutral selection snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MacroContextInputObservation {
    role: MacroContextInputRole,
    observation: MacroObservation,
}

impl MacroContextInputObservation {
    /// Returns provider-neutral economic meaning for this canonical input.
    pub(crate) const fn role(&self) -> MacroContextInputRole {
        self.role
    }

    /// Returns exact canonical value, clocks, revisions, missingness, and internal provenance.
    pub(crate) const fn observation(&self) -> &MacroObservation {
        &self.observation
    }
}

/// One requested product indicator with an observed, explicitly missing, or unavailable value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MacroContextSelectedObservation {
    indicator_id: &'static str,
    observation: Option<MacroObservation>,
    authority: Option<MacroContextSelectionAuthority>,
    source_receipt: Option<Arc<MacroContextSourceReceipt>>,
}

impl MacroContextSelectedObservation {
    fn unavailable(definition: MacroContextIndicatorDefinition) -> Self {
        Self {
            indicator_id: definition.indicator_id,
            observation: None,
            authority: None,
            source_receipt: None,
        }
    }

    /// Returns the stable provider-neutral product indicator identity.
    pub(crate) const fn indicator_id(&self) -> &'static str {
        self.indicator_id
    }

    /// Returns `None` only when no canonical observation existed at the requested cutoff.
    ///
    /// A returned observation may still carry provider-authored explicit missingness.
    pub(crate) const fn observation(&self) -> Option<&MacroObservation> {
        self.observation.as_ref()
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum MacroContextSelectionAuthority {
    Treasury,
    Board,
    Fred,
}

/// Reusable typed neutral Macro selection plus opaque exact evidence.
pub(crate) struct MacroContextSnapshot {
    dto: MacroContextDto,
    inputs: Box<[MacroContextInputObservation]>,
    selected: Box<[MacroContextSelectedObservation]>,
    evidence: MacroContextEvidenceReceipt,
}

impl MacroContextSnapshot {
    /// Returns every bounded canonical input considered by the neutral selector.
    pub(crate) fn inputs(&self) -> &[MacroContextInputObservation] {
        &self.inputs
    }

    /// Returns every requested product indicator, including explicit unavailable states.
    pub(crate) fn selected(&self) -> &[MacroContextSelectedObservation] {
        &self.selected
    }

    /// Returns the opaque evidence receipt and exact parent generation set.
    pub(crate) const fn evidence(&self) -> &MacroContextEvidenceReceipt {
        &self.evidence
    }

    fn into_tool_result(self, limits: ServiceLimits) -> Result<TypedToolResult, ServiceError> {
        let availability = self.dto.availability;
        let selected_indicators = self
            .dto
            .coverage
            .observed
            .checked_add(self.dto.coverage.missing)
            .ok_or(ServiceError::ResourceExhausted)?;
        let complete = self.dto.selection.complete;
        let source_coverage = json!({
            "requestedIndicators": MACRO_CONTEXT_INDICATOR_COUNT,
            "selectedIndicators": selected_indicators,
            "complete": complete,
        });
        let data_quality = json!({
            "classification": match availability {
                MacroContextAvailability::Available => "moderate",
                MacroContextAvailability::Partial => "limited",
                MacroContextAvailability::Unavailable => "unavailable",
            },
            "pointInTime": true,
            "executionEligible": false,
        });
        let metadata = ToolResultMetadata::try_complete(source_coverage, data_quality)
            .map_err(|_| ServiceError::InvalidResult)?;
        let content = serde_json::to_value(self.dto).map_err(|_| ServiceError::InvalidResult)?;
        TypedToolResult::try_new(content, MACRO_CONTEXT_INDICATOR_COUNT, metadata, limits)
            .map_err(Into::into)
    }
}

impl fmt::Debug for MacroContextSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MacroContextSnapshot")
            .field("input_count", &self.inputs.len())
            .field("selected_count", &self.selected.len())
            .field("evidence", &self.evidence)
            .finish_non_exhaustive()
    }
}

fn product_snapshot(
    cutoffs: MacroContextCutoffs,
    board: Option<AnalyticalMacroLatestKnownOutput>,
    fred: Option<AnalyticalMacroLatestKnownOutput>,
    treasury_fiscal: Box<[TreasuryCurrentAnalyticalRead]>,
    treasury_daily: Box<[TreasuryCurrentAnalyticalRead]>,
) -> Result<MacroContextSnapshot, ServiceError> {
    let definitions = H15_INDICATORS
        .iter()
        .copied()
        .chain(std::iter::once(UNEMPLOYMENT_INDICATOR))
        .collect::<Vec<_>>();
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(MACRO_CONTEXT_INDICATOR_COUNT)
        .map_err(|_| ServiceError::ResourceExhausted)?;
    observations.extend(
        definitions
            .iter()
            .copied()
            .map(MacroContextIndicatorDefinition::unavailable),
    );
    let mut selected = Vec::new();
    selected
        .try_reserve_exact(MACRO_CONTEXT_INDICATOR_COUNT)
        .map_err(|_| ServiceError::ResourceExhausted)?;
    selected.extend(
        definitions
            .iter()
            .copied()
            .map(MacroContextSelectedObservation::unavailable),
    );

    let input_count = board
        .as_ref()
        .map_or(0, |output| output.observations().len())
        .checked_add(
            fred.as_ref()
                .map_or(0, |output| output.observations().len()),
        )
        .and_then(|count| {
            treasury_fiscal.iter().try_fold(count, |count, read| {
                count.checked_add(read.output().observations().len())
            })
        })
        .and_then(|count| {
            treasury_daily.iter().try_fold(count, |count, read| {
                count.checked_add(read.output().observations().len())
            })
        })
        .filter(|count| *count <= MAXIMUM_MACRO_CONTEXT_INPUTS)
        .ok_or(ServiceError::ResourceExhausted)?;
    let mut inputs = Vec::new();
    inputs
        .try_reserve_exact(input_count)
        .map_err(|_| ServiceError::ResourceExhausted)?;
    let mut receipts = Vec::new();
    receipts
        .try_reserve_exact(
            2_usize
                .checked_add(treasury_fiscal.len())
                .and_then(|count| count.checked_add(treasury_daily.len()))
                .ok_or(ServiceError::ResourceExhausted)?,
        )
        .map_err(|_| ServiceError::ResourceExhausted)?;

    if let Some(board) = board {
        let receipt = Arc::new(MacroContextSourceReceipt::try_from_output(
            MacroContextInternalSource::InterestRates,
            &board,
        )?);
        retain_inputs(
            &board,
            cutoffs,
            |_| Ok(MacroContextInputRole::GovernmentYieldCurve),
            &mut inputs,
        )?;
        project_board(
            &board,
            cutoffs,
            &mut observations[..H15_INDICATOR_COUNT],
            &mut selected[..H15_INDICATOR_COUNT],
            &receipt,
        )?;
        receipts.push(receipt);
    }
    if let Some(fred) = fred {
        let receipt = Arc::new(MacroContextSourceReceipt::try_from_output(
            MacroContextInternalSource::LaborMarket,
            &fred,
        )?);
        retain_inputs(
            &fred,
            cutoffs,
            |_| Ok(MacroContextInputRole::LaborMarket),
            &mut inputs,
        )?;
        project_fred(
            &fred,
            cutoffs,
            &mut observations[H15_INDICATOR_COUNT],
            &mut selected[H15_INDICATOR_COUNT],
            &receipt,
        )?;
        receipts.push(receipt);
    }
    for read in treasury_fiscal {
        if read.surface() != TreasurySurface::FiscalData {
            return Err(ServiceError::InvalidResult);
        }
        let output = read.output();
        retain_inputs(output, cutoffs, treasury_input_role, &mut inputs)?;
        receipts.push(Arc::new(MacroContextSourceReceipt::try_from_output(
            MacroContextInternalSource::FiscalConditions,
            output,
        )?));
    }
    for read in treasury_daily {
        if read.surface() != TreasurySurface::DailyRatesXml {
            return Err(ServiceError::InvalidResult);
        }
        let output = read.output();
        let receipt = Arc::new(MacroContextSourceReceipt::try_from_output(
            MacroContextInternalSource::InterestRates,
            output,
        )?);
        retain_inputs(output, cutoffs, treasury_input_role, &mut inputs)?;
        project_treasury_daily(
            output,
            cutoffs,
            &mut observations[..H15_INDICATOR_COUNT],
            &mut selected[..H15_INDICATOR_COUNT],
            &receipt,
        )?;
        receipts.push(receipt);
    }

    let coverage = observations.iter().try_fold(
        MacroContextCoverageDto {
            requested: MACRO_CONTEXT_INDICATOR_COUNT,
            observed: 0,
            missing: 0,
            unavailable: 0,
        },
        |mut coverage, observation| {
            let counter = match observation.availability {
                MacroContextObservationAvailability::Available => &mut coverage.observed,
                MacroContextObservationAvailability::Missing => &mut coverage.missing,
                MacroContextObservationAvailability::Unavailable => &mut coverage.unavailable,
            };
            *counter = counter
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?;
            Ok::<_, ServiceError>(coverage)
        },
    )?;
    if coverage
        .observed
        .checked_add(coverage.missing)
        .and_then(|value| value.checked_add(coverage.unavailable))
        != Some(MACRO_CONTEXT_INDICATOR_COUNT)
    {
        return Err(ServiceError::InvalidResult);
    }

    let availability = if coverage.observed == MACRO_CONTEXT_INDICATOR_COUNT {
        MacroContextAvailability::Available
    } else if coverage.observed > 0 {
        MacroContextAvailability::Partial
    } else {
        MacroContextAvailability::Unavailable
    };
    let confidence = match availability {
        MacroContextAvailability::Available => MacroContextConfidenceDto {
            level: MacroContextConfidenceLevel::Moderate,
            summary: "All requested indicators were selected from delayed official observations.",
        },
        MacroContextAvailability::Partial => MacroContextConfidenceDto {
            level: MacroContextConfidenceLevel::Limited,
            summary: "Some requested indicators are missing or unavailable at this cutoff.",
        },
        MacroContextAvailability::Unavailable => MacroContextConfidenceDto {
            level: MacroContextConfidenceLevel::Unavailable,
            summary: "No requested indicator is available at this cutoff.",
        },
    };
    let complete = coverage.unavailable == 0;
    let selection = MacroContextSelectionDto {
        knowledge_cutoff: timestamp_text(cutoffs.knowledge_cutoff)?,
        effective_date_cutoff: cutoffs.effective_date_cutoff.to_string(),
        evaluated_at: timestamp_text(cutoffs.evaluated_at)?,
        complete,
    };
    let dto = MacroContextDto {
        availability,
        selection,
        confidence,
        coverage,
        observations,
    };
    let evidence = MacroContextEvidenceReceipt::try_new(cutoffs, receipts, &selected)?;
    Ok(MacroContextSnapshot {
        dto,
        inputs: inputs.into_boxed_slice(),
        selected: selected.into_boxed_slice(),
        evidence,
    })
}

fn project_board(
    output: &AnalyticalMacroLatestKnownOutput,
    cutoffs: MacroContextCutoffs,
    target: &mut [MacroContextObservationDto],
    selected: &mut [MacroContextSelectedObservation],
    source_receipt: &Arc<MacroContextSourceReceipt>,
) -> Result<(), ServiceError> {
    if target.len() != H15_INDICATOR_COUNT
        || selected.len() != H15_INDICATOR_COUNT
        || output.observations().len() > H15_INDICATOR_COUNT
    {
        return Err(ServiceError::InvalidResult);
    }
    let expected_source =
        SourceId::try_from(BOARD_DDP_SOURCE_ID).map_err(|_| ServiceError::Unavailable)?;
    let expected_unit = h15_treasury_constant_maturities_canonical_unit_identifier()
        .map_err(|_| ServiceError::Unavailable)?;
    if output.source_id() != &expected_source {
        return Err(ServiceError::InvalidResult);
    }

    let mut matched = vec![false; output.observations().len()];
    for (target_index, (definition, descriptor)) in H15_INDICATORS
        .iter()
        .copied()
        .zip(h15_treasury_constant_maturities_dashboard_series())
        .enumerate()
    {
        if definition.source_slot != descriptor.slot() {
            return Err(ServiceError::InvalidResult);
        }
        let series = descriptor
            .canonical_macro_series_identifier()
            .map_err(|_| ServiceError::Unavailable)?;
        let mut candidates = output
            .observations()
            .iter()
            .enumerate()
            .filter(|(_, observation)| observation.series() == &series);
        if let Some((source_index, observation)) = candidates.next() {
            if candidates.next().is_some() || matched[source_index] {
                return Err(ServiceError::InvalidResult);
            }
            matched[source_index] = true;
            select_observation(
                &mut target[target_index],
                &mut selected[target_index],
                definition,
                observation,
                &expected_source,
                &series,
                &expected_unit,
                MacroContextSelectionAuthority::Board,
                source_receipt,
                cutoffs,
            )?;
        }
    }
    if matched.iter().any(|value| !value) {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}

fn project_fred(
    output: &AnalyticalMacroLatestKnownOutput,
    cutoffs: MacroContextCutoffs,
    target: &mut MacroContextObservationDto,
    selected: &mut MacroContextSelectedObservation,
    source_receipt: &Arc<MacroContextSourceReceipt>,
) -> Result<(), ServiceError> {
    let expected_source =
        SourceId::try_from(FRED_SOURCE_ID).map_err(|_| ServiceError::Unavailable)?;
    let expected_series = SourceIdentifier::try_from(FRED_UNEMPLOYMENT_SERIES_ID)
        .map_err(|_| ServiceError::Unavailable)?;
    let expected_unit = SourceIdentifier::try_from(FRED_UNEMPLOYMENT_UNIT_ID)
        .map_err(|_| ServiceError::Unavailable)?;
    if output.source_id() != &expected_source {
        return Err(ServiceError::InvalidResult);
    }
    match output.observations() {
        [] => {}
        [observation] => {
            select_observation(
                target,
                selected,
                UNEMPLOYMENT_INDICATOR,
                observation,
                &expected_source,
                &expected_series,
                &expected_unit,
                MacroContextSelectionAuthority::Fred,
                source_receipt,
                cutoffs,
            )?;
        }
        [_, _, ..] => return Err(ServiceError::InvalidResult),
    }
    Ok(())
}

fn project_treasury_daily(
    output: &AnalyticalMacroLatestKnownOutput,
    cutoffs: MacroContextCutoffs,
    target: &mut [MacroContextObservationDto],
    selected: &mut [MacroContextSelectedObservation],
    source_receipt: &Arc<MacroContextSourceReceipt>,
) -> Result<(), ServiceError> {
    if target.len() != H15_INDICATOR_COUNT || selected.len() != H15_INDICATOR_COUNT {
        return Err(ServiceError::InvalidResult);
    }
    let expected_unit = SourceIdentifier::try_from(TREASURY_PERCENT_UNIT_ID)
        .map_err(|_| ServiceError::Unavailable)?;
    for (target_index, definition) in H15_INDICATORS.iter().copied().enumerate() {
        let series = treasury_nominal_series(definition.source_slot)?;
        let mut candidates = output
            .observations()
            .iter()
            .filter(|observation| observation.series() == &series);
        if let Some(observation) = candidates.next() {
            if candidates.next().is_some() {
                return Err(ServiceError::InvalidResult);
            }
            select_observation(
                &mut target[target_index],
                &mut selected[target_index],
                definition,
                observation,
                output.source_id(),
                &series,
                &expected_unit,
                MacroContextSelectionAuthority::Treasury,
                source_receipt,
                cutoffs,
            )?;
        }
    }
    Ok(())
}

fn retain_inputs(
    output: &AnalyticalMacroLatestKnownOutput,
    cutoffs: MacroContextCutoffs,
    classify: impl Fn(&MacroObservation) -> Result<MacroContextInputRole, ServiceError>,
    inputs: &mut Vec<MacroContextInputObservation>,
) -> Result<(), ServiceError> {
    let remaining = MAXIMUM_MACRO_CONTEXT_INPUTS
        .checked_sub(inputs.len())
        .ok_or(ServiceError::ResourceExhausted)?;
    if output.observations().len() > remaining {
        return Err(ServiceError::ResourceExhausted);
    }
    for observation in output.observations() {
        validate_canonical_input(observation, output.source_id(), cutoffs)?;
        inputs.push(MacroContextInputObservation {
            role: classify(observation)?,
            observation: observation.clone(),
        });
    }
    Ok(())
}

fn treasury_input_role(
    observation: &MacroObservation,
) -> Result<MacroContextInputRole, ServiceError> {
    let series = observation.series().as_str();
    if series.starts_with("treasury:average-interest-rate:v2:") {
        Ok(MacroContextInputRole::GovernmentBorrowingCost)
    } else if series.starts_with("treasury:daily-par-yield-curve:")
        || series.starts_with("treasury:daily-long-term-rates:")
    {
        Ok(MacroContextInputRole::GovernmentYieldCurve)
    } else if series.starts_with("treasury:daily-bill-rates:") {
        Ok(MacroContextInputRole::ShortTermGovernmentFunding)
    } else if series.starts_with("treasury:daily-real-par-yield-curve:")
        || series.starts_with("treasury:daily-real-long-term-rates:")
    {
        Ok(MacroContextInputRole::InflationAdjustedGovernmentRates)
    } else {
        Err(ServiceError::InvalidResult)
    }
}

fn treasury_nominal_series(slot: &str) -> Result<SourceIdentifier, ServiceError> {
    let maturity = match slot {
        "1m" => TreasuryMaturity::OneMonth,
        "3m" => TreasuryMaturity::ThreeMonths,
        "6m" => TreasuryMaturity::SixMonths,
        "1y" => TreasuryMaturity::OneYear,
        "2y" => TreasuryMaturity::TwoYears,
        "3y" => TreasuryMaturity::ThreeYears,
        "5y" => TreasuryMaturity::FiveYears,
        "7y" => TreasuryMaturity::SevenYears,
        "10y" => TreasuryMaturity::TenYears,
        "20y" => TreasuryMaturity::TwentyYears,
        "30y" => TreasuryMaturity::ThirtyYears,
        _ => return Err(ServiceError::InvalidResult),
    };
    SourceIdentifier::try_from(
        TreasuryDailyRateMetric::NominalParYield(maturity).canonical_series(),
    )
    .map_err(|_| ServiceError::Unavailable)
}

#[allow(
    clippy::too_many_arguments,
    reason = "selection keeps every canonical identity and cutoff invariant explicit"
)]
fn select_observation(
    target: &mut MacroContextObservationDto,
    selected: &mut MacroContextSelectedObservation,
    definition: MacroContextIndicatorDefinition,
    observation: &MacroObservation,
    expected_source: &SourceId,
    expected_series: &SourceIdentifier,
    expected_unit: &SourceIdentifier,
    authority: MacroContextSelectionAuthority,
    source_receipt: &Arc<MacroContextSourceReceipt>,
    cutoffs: MacroContextCutoffs,
) -> Result<(), ServiceError> {
    if target.indicator_id != definition.indicator_id
        || selected.indicator_id != definition.indicator_id
    {
        return Err(ServiceError::InvalidResult);
    }
    let projected = project_observation(
        definition,
        observation,
        expected_source,
        expected_series,
        expected_unit,
        cutoffs,
    )?;
    if &source_receipt.source_id != expected_source {
        return Err(ServiceError::InvalidResult);
    }
    let candidate_rank = selection_rank(observation, authority, cutoffs)?;
    let replace = match (
        selected.observation.as_ref(),
        selected.authority,
        selected.source_receipt.as_ref(),
    ) {
        (None, None, None) => true,
        (Some(current), Some(current_authority), Some(_)) => {
            candidate_rank > selection_rank(current, current_authority, cutoffs)?
        }
        _ => return Err(ServiceError::InvalidResult),
    };
    if replace {
        *target = projected;
        selected.observation = Some(observation.clone());
        selected.authority = Some(authority);
        selected.source_receipt = Some(Arc::clone(source_receipt));
    }
    Ok(())
}

fn selection_rank(
    observation: &MacroObservation,
    authority: MacroContextSelectionAuthority,
    cutoffs: MacroContextCutoffs,
) -> Result<
    (
        CalendarDate,
        bool,
        Timestamp,
        MacroContextSelectionAuthority,
    ),
    ServiceError,
> {
    let observed = match (
        observation.value().observed_value(),
        observation.value().missing_value(),
    ) {
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (Some(_), Some(_)) | (None, None) => return Err(ServiceError::InvalidResult),
    };
    let effective = effective_calendar_date(observation, cutoffs)?;
    let available_at = observation
        .context()
        .provenance()
        .availability()
        .conservative_available_at()
        .filter(|available_at| *available_at <= cutoffs.knowledge_cutoff)
        .ok_or(ServiceError::InvalidResult)?;
    Ok((effective, observed, available_at, authority))
}

fn project_observation(
    definition: MacroContextIndicatorDefinition,
    observation: &MacroObservation,
    expected_source: &SourceId,
    expected_series: &SourceIdentifier,
    expected_unit: &SourceIdentifier,
    cutoffs: MacroContextCutoffs,
) -> Result<MacroContextObservationDto, ServiceError> {
    validate_canonical_input(observation, expected_source, cutoffs)?;
    let context = observation.context();
    let provenance = context.provenance();
    let time = context.time();
    let effective_date = effective_calendar_date(observation, cutoffs)?;
    let recorded = match time.published() {
        Some(published) => MacroContextRecordedDateDto::Known {
            date: coordinate_calendar_date_at_knowledge(published, cutoffs.knowledge_cutoff)?
                .to_string(),
        },
        None => MacroContextRecordedDateDto::NotSupplied,
    };
    let superseded_after = time
        .superseded()
        .map(coordinate_calendar_date)
        .transpose()?;
    let available_at = provenance
        .availability()
        .conservative_available_at()
        .filter(|available_at| *available_at <= cutoffs.knowledge_cutoff)
        .ok_or(ServiceError::InvalidResult)?;
    if observation.series() != expected_series
        || observation.unit() != expected_unit
        || provenance.source_id() != expected_source
    {
        return Err(ServiceError::InvalidResult);
    }

    let (value, availability, confidence) = match (
        observation.value().observed_value(),
        observation.value().missing_value(),
    ) {
        (Some(value), None) => (
            MacroContextValueDto::Observed {
                decimal: value.normalize().to_string(),
            },
            MacroContextObservationAvailability::Available,
            MacroContextConfidenceDto::moderate(),
        ),
        (None, Some(_missing)) => (
            MacroContextValueDto::Missing {
                reason: MacroContextMissingReason::NotReported,
                explanation: "No value was reported at this cutoff.",
            },
            MacroContextObservationAvailability::Missing,
            MacroContextConfidenceDto::limited(),
        ),
        (Some(_), Some(_)) | (None, None) => return Err(ServiceError::InvalidResult),
    };
    Ok(MacroContextObservationDto {
        indicator_id: definition.indicator_id,
        label: definition.label,
        category: definition.category,
        frequency: definition.frequency,
        seasonal_adjustment: definition.seasonal_adjustment,
        unit: definition.unit,
        effective_date: Some(effective_date.to_string()),
        recorded,
        available_at: Some(timestamp_text(available_at)?),
        revision: Some(time.revision().get()),
        superseded_after: superseded_after.map(|date| date.to_string()),
        value,
        availability,
        confidence,
    })
}

fn validate_canonical_input(
    observation: &MacroObservation,
    expected_source: &SourceId,
    cutoffs: MacroContextCutoffs,
) -> Result<(), ServiceError> {
    let context = observation.context();
    let provenance = context.provenance();
    let time = context.time();
    effective_calendar_date(observation, cutoffs)?;
    if let Some(published) = time.published() {
        coordinate_calendar_date_at_knowledge(published, cutoffs.knowledge_cutoff)?;
    }
    if let Some(superseded) = time.superseded() {
        coordinate_calendar_date(superseded)?;
    }
    provenance
        .availability()
        .conservative_available_at()
        .filter(|available_at| *available_at <= cutoffs.knowledge_cutoff)
        .ok_or(ServiceError::InvalidResult)?;
    if provenance.source_id() != expected_source
        || provenance.instrument_id().is_some()
        || provenance.venue_id().is_some()
        || provenance.quality() != DataQuality::OfficialDelayed
        || provenance.received_at() > cutoffs.knowledge_cutoff
        || provenance.ingested_at() > cutoffs.knowledge_cutoff
    {
        return Err(ServiceError::InvalidResult);
    }
    match provenance.payload_reference() {
        PayloadReference::ContentHash(hash)
            if hash.algorithm() == DigestAlgorithm::Sha256 && hash.digest() != [0; 32] => {}
        PayloadReference::ContentHash(_) | PayloadReference::SourceReference(_) => {
            return Err(ServiceError::InvalidResult);
        }
    }
    match (
        observation.value().observed_value(),
        observation.value().missing_value(),
    ) {
        (Some(_), None) | (None, Some(_)) => Ok(()),
        (Some(_), Some(_)) | (None, None) => Err(ServiceError::InvalidResult),
    }
}

fn effective_calendar_date(
    observation: &MacroObservation,
    cutoffs: MacroContextCutoffs,
) -> Result<CalendarDate, ServiceError> {
    let coordinate = observation.context().time().effective();
    let date = if let Some(date) = coordinate.calendar_date_value() {
        date
    } else if let Some(timestamp) = coordinate.exact_timestamp() {
        if timestamp > cutoffs.knowledge_cutoff {
            return Err(ServiceError::InvalidResult);
        }
        timestamp_calendar_date(timestamp)?
    } else {
        return Err(ServiceError::InvalidResult);
    };
    if date > cutoffs.effective_date_cutoff {
        Err(ServiceError::InvalidResult)
    } else {
        Ok(date)
    }
}

fn coordinate_calendar_date_at_knowledge(
    coordinate: &ResearchTemporalCoordinate,
    knowledge_cutoff: Timestamp,
) -> Result<CalendarDate, ServiceError> {
    if let Some(date) = coordinate.calendar_date_value() {
        if date <= timestamp_calendar_date(knowledge_cutoff)? {
            Ok(date)
        } else {
            Err(ServiceError::InvalidResult)
        }
    } else if let Some(timestamp) = coordinate.exact_timestamp() {
        if timestamp <= knowledge_cutoff {
            timestamp_calendar_date(timestamp)
        } else {
            Err(ServiceError::InvalidResult)
        }
    } else {
        Err(ServiceError::InvalidResult)
    }
}

fn coordinate_calendar_date(
    coordinate: &ResearchTemporalCoordinate,
) -> Result<CalendarDate, ServiceError> {
    if let Some(date) = coordinate.calendar_date_value() {
        Ok(date)
    } else if let Some(timestamp) = coordinate.exact_timestamp() {
        timestamp_calendar_date(timestamp)
    } else {
        Err(ServiceError::InvalidResult)
    }
}

/// Opaque exact evidence for one neutral Macro selection.
///
/// Provider-qualified inputs are intentionally retained below transport and product DTOs.
/// Consulted generations stay diagnostic-only; consumers receive only generations actually
/// selected by the neutral product projection and an exact digest of those selections.
pub(crate) struct MacroContextEvidenceReceipt {
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: CalendarDate,
    evaluated_at: Timestamp,
    consumed_parent_manifests: Box<[DatasetManifestRef]>,
    consumed_digest: EvidenceDigest,
    consulted_sources: Box<[Arc<MacroContextSourceReceipt>]>,
}

impl MacroContextEvidenceReceipt {
    fn try_new(
        cutoffs: MacroContextCutoffs,
        mut consulted_sources: Vec<Arc<MacroContextSourceReceipt>>,
        selected: &[MacroContextSelectedObservation],
    ) -> Result<Self, ServiceError> {
        consulted_sources.sort_by(|left, right| compare_source_receipts(left, right));
        if consulted_sources.windows(2).any(|pair| {
            pair[0].source == pair[1].source
                && pair[0].source_id == pair[1].source_id
                && pair[0].manifest == pair[1].manifest
        }) {
            return Err(ServiceError::InvalidResult);
        }

        let mut consumed_sources = Vec::new();
        consumed_sources
            .try_reserve_exact(selected.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for selection in selected {
            match (
                selection.observation.as_ref(),
                selection.authority,
                selection.source_receipt.as_ref(),
            ) {
                (None, None, None) => {}
                (Some(observation), Some(_), Some(receipt)) => {
                    if observation.context().provenance().source_id() != &receipt.source_id
                        || !consulted_sources
                            .iter()
                            .any(|consulted| Arc::ptr_eq(consulted, receipt))
                    {
                        return Err(ServiceError::InvalidResult);
                    }
                    consumed_sources.push(Arc::clone(receipt));
                }
                _ => return Err(ServiceError::InvalidResult),
            }
        }
        consumed_sources.sort_by(|left, right| compare_source_receipts(left, right));
        consumed_sources.dedup_by(|left, right| Arc::ptr_eq(left, right));

        let mut consumed_parent_manifests = consumed_sources
            .iter()
            .map(|source| source.manifest.clone())
            .collect::<Vec<_>>();
        consumed_parent_manifests.sort_by(compare_manifest_refs);
        for pair in consumed_parent_manifests.windows(2) {
            if pair[0].dataset_id() == pair[1].dataset_id()
                && pair[0].manifest_version() == pair[1].manifest_version()
                && pair[0] != pair[1]
            {
                return Err(ServiceError::InvalidResult);
            }
        }
        consumed_parent_manifests.dedup();

        let mut hasher = Sha256::new();
        hash_text(
            &mut hasher,
            "market-squawk/macro-context-consumed-evidence/v1",
        );
        hasher.update(cutoffs.knowledge_cutoff.unix_nanos().to_be_bytes());
        hash_text(&mut hasher, &cutoffs.effective_date_cutoff.to_string());
        hash_usize(&mut hasher, selected.len());
        for selection in selected {
            hash_text(&mut hasher, selection.indicator_id);
            match (
                selection.observation.as_ref(),
                selection.authority,
                selection.source_receipt.as_ref(),
            ) {
                (None, None, None) => hasher.update([0]),
                (Some(observation), Some(authority), Some(receipt)) => {
                    hasher.update([1, authority.digest_tag()]);
                    hash_source_receipt(&mut hasher, receipt);
                    let observation =
                        serde_json::to_vec(observation).map_err(|_| ServiceError::InvalidResult)?;
                    hash_bytes(&mut hasher, &observation);
                }
                _ => return Err(ServiceError::InvalidResult),
            }
        }
        let consumed_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into());
        require_sha256(consumed_digest)?;
        Ok(Self {
            knowledge_cutoff: cutoffs.knowledge_cutoff,
            effective_date_cutoff: cutoffs.effective_date_cutoff,
            evaluated_at: cutoffs.evaluated_at,
            consumed_parent_manifests: consumed_parent_manifests.into_boxed_slice(),
            consumed_digest,
            consulted_sources: consulted_sources.into_boxed_slice(),
        })
    }

    /// Returns the exact point-in-time knowledge cutoff used for selection.
    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the exact effective-date cutoff used for selection.
    pub(crate) const fn effective_date_cutoff(&self) -> CalendarDate {
        self.effective_date_cutoff
    }

    /// Returns when the application evaluated this snapshot.
    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    /// Returns only canonical generations consumed by the neutral selection.
    pub(crate) fn consumed_parent_manifests(&self) -> &[DatasetManifestRef] {
        &self.consumed_parent_manifests
    }

    /// Returns a nonzero SHA-256 identity over exact consumed selections and cutoffs.
    pub(crate) const fn consumed_digest(&self) -> EvidenceDigest {
        self.consumed_digest
    }
}

impl fmt::Debug for MacroContextEvidenceReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MacroContextEvidenceReceipt")
            .field(
                "consumed_parent_count",
                &self.consumed_parent_manifests.len(),
            )
            .field("consulted_source_count", &self.consulted_sources.len())
            .field("digest", &"[SHA-256]")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Eq, PartialEq)]
struct MacroContextSourceReceipt {
    source: MacroContextInternalSource,
    source_id: SourceId,
    manifest: DatasetManifestRef,
    object_graph_digest: EvidenceDigest,
    query_identity: EvidenceDigest,
    result_digest: EvidenceDigest,
    selection_digest: EvidenceDigest,
}

impl MacroContextSourceReceipt {
    fn try_from_output(
        source: MacroContextInternalSource,
        output: &AnalyticalMacroLatestKnownOutput,
    ) -> Result<Self, ServiceError> {
        let pinned = output.output();
        let object_graph_digest = require_sha256(pinned.object_graph_digest())?;
        let query_identity = require_sha256(pinned.query_identity())?;
        let result_digest = require_sha256(pinned.result_digest())?;
        let selection_digest = require_sha256(output.selection_digest())?;
        Ok(Self {
            source,
            source_id: output.source_id().clone(),
            manifest: pinned.manifest().clone(),
            object_graph_digest,
            query_identity,
            result_digest,
            selection_digest,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum MacroContextInternalSource {
    InterestRates,
    LaborMarket,
    FiscalConditions,
}

impl MacroContextInternalSource {
    const fn digest_tag(self) -> u8 {
        match self {
            Self::InterestRates => 1,
            Self::LaborMarket => 2,
            Self::FiscalConditions => 3,
        }
    }
}

impl MacroContextSelectionAuthority {
    const fn digest_tag(self) -> u8 {
        match self {
            Self::Treasury => 1,
            Self::Board => 2,
            Self::Fred => 3,
        }
    }
}

fn compare_source_receipts(
    left: &MacroContextSourceReceipt,
    right: &MacroContextSourceReceipt,
) -> std::cmp::Ordering {
    left.source
        .cmp(&right.source)
        .then_with(|| left.source_id.as_str().cmp(right.source_id.as_str()))
        .then_with(|| compare_manifest_refs(&left.manifest, &right.manifest))
        .then_with(|| {
            left.object_graph_digest
                .bytes()
                .cmp(&right.object_graph_digest.bytes())
        })
        .then_with(|| {
            left.query_identity
                .bytes()
                .cmp(&right.query_identity.bytes())
        })
        .then_with(|| left.result_digest.bytes().cmp(&right.result_digest.bytes()))
        .then_with(|| {
            left.selection_digest
                .bytes()
                .cmp(&right.selection_digest.bytes())
        })
}

fn compare_manifest_refs(
    left: &DatasetManifestRef,
    right: &DatasetManifestRef,
) -> std::cmp::Ordering {
    left.dataset_id()
        .as_str()
        .cmp(right.dataset_id().as_str())
        .then_with(|| left.manifest_version().cmp(&right.manifest_version()))
        .then_with(|| left.schema().name().cmp(right.schema().name()))
        .then_with(|| left.schema().version().cmp(&right.schema().version()))
        .then_with(|| {
            left.schema()
                .fingerprint()
                .cmp(&right.schema().fingerprint())
        })
        .then_with(|| {
            left.content_hash()
                .bytes()
                .cmp(&right.content_hash().bytes())
        })
}

fn hash_manifest(hasher: &mut Sha256, manifest: &DatasetManifestRef) {
    hash_text(hasher, manifest.dataset_id().as_str());
    hasher.update(manifest.manifest_version().to_be_bytes());
    hash_text(hasher, manifest.schema().name());
    hasher.update(manifest.schema().version().get().to_be_bytes());
    hasher.update(manifest.schema().fingerprint());
    hasher.update(manifest.content_hash().bytes());
}

fn hash_source_receipt(hasher: &mut Sha256, source: &MacroContextSourceReceipt) {
    hasher.update([source.source.digest_tag()]);
    hash_text(hasher, source.source_id.as_str());
    hash_manifest(hasher, &source.manifest);
    hash_digest(hasher, source.object_graph_digest);
    hash_digest(hasher, source.query_identity);
    hash_digest(hasher, source.result_digest);
    hash_digest(hasher, source.selection_digest);
}

fn hash_digest(hasher: &mut Sha256, digest: EvidenceDigest) {
    hasher.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hasher.update(digest.bytes());
}

fn hash_text(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u128).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u128).to_be_bytes());
    hasher.update(value);
}

fn hash_usize(hasher: &mut Sha256, value: usize) {
    hasher.update((value as u128).to_be_bytes());
}

fn ensure_request_live(
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<(), ServiceError> {
    if request.name() != MACRO_GET_CONTEXT {
        return Err(ServiceError::NotFound);
    }
    if context.cancellation().is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    Ok(())
}

fn macro_context_query_limits(
    request: &AnalyticalMacroLatestKnownRequest,
    deadline: std::time::Instant,
) -> Result<QueryLimits, ServiceError> {
    let now = std::time::Instant::now();
    if now >= deadline {
        return Err(ServiceError::DeadlineExceeded);
    }
    let maximum_duration = deadline
        .saturating_duration_since(now)
        .min(Duration::from_secs(60));
    QueryLimits::try_new_with_inline_bytes(
        request.required_query_rows(),
        MACRO_CONTEXT_QUERY_BYTES,
        MACRO_CONTEXT_QUERY_BYTES,
        MACRO_CONTEXT_QUERY_MEMORY_BYTES,
        4,
        2_048,
        4_096,
        maximum_duration,
    )
    .map_err(map_query_error)
}

fn current_timestamp() -> Result<Timestamp, ServiceError> {
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

fn parse_calendar_date(value: &str) -> Result<CalendarDate, ServiceError> {
    let bytes = value.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return Err(ServiceError::InvalidRequest);
    }
    let year = parse_date_component(&bytes[..4])?;
    let month = u8::try_from(parse_date_component(&bytes[5..7])?)
        .map_err(|_| ServiceError::InvalidRequest)?;
    let day = u8::try_from(parse_date_component(&bytes[8..])?)
        .map_err(|_| ServiceError::InvalidRequest)?;
    CalendarDate::new(year, month, day).map_err(|_| ServiceError::InvalidRequest)
}

fn parse_date_component(bytes: &[u8]) -> Result<u16, ServiceError> {
    bytes.iter().try_fold(0_u16, |value, byte| {
        let digit = byte
            .checked_sub(b'0')
            .filter(|digit| *digit <= 9)
            .ok_or(ServiceError::InvalidRequest)?;
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u16::from(digit)))
            .ok_or(ServiceError::InvalidRequest)
    })
}

fn timestamp_text(timestamp: Timestamp) -> Result<String, ServiceError> {
    let unix_nanos = timestamp.unix_nanos();
    let seconds = unix_nanos.div_euclid(1_000_000_000);
    let nanoseconds = u32::try_from(unix_nanos.rem_euclid(1_000_000_000))
        .map_err(|_| ServiceError::InvalidResult)?;
    DateTime::<Utc>::from_timestamp(seconds, nanoseconds)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Nanos, true))
        .ok_or(ServiceError::InvalidResult)
}

fn require_sha256(digest: EvidenceDigest) -> Result<EvidenceDigest, ServiceError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
        Err(ServiceError::InvalidResult)
    } else {
        Ok(digest)
    }
}
