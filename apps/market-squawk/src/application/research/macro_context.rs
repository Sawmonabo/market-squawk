//! Provider-neutral Macro product context over exact canonical point-in-time reads.

use std::{fmt, time::Duration};

use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use market_squawk_adapter_federal_reserve::{
    BOARD_DDP_SOURCE_ID, BoardDatasetFamily, BoardDatasetProfile, BoardFrequency, BoardRelease,
    h15_treasury_constant_maturities_canonical_unit_identifier,
    h15_treasury_constant_maturities_dashboard_series,
};
use market_squawk_data::{
    AnalyticalMacroLatestKnownOutput, AnalyticalMacroLatestKnownRequest,
    AnalyticalMacroSeriesAllowlist, AnalyticalReadCapability, DatasetId, DatasetManifestRef,
    QueryLimits,
};
use market_squawk_domain::{
    CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest, MacroObservation, PayloadReference,
    SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_services::{
    RequestContext, ServiceError, ServiceLimits, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use serde::Serialize;
use serde_json::{Value, json};

use super::{FredLatestKnownOperation, map_query_error, map_read_error, parse_timestamp};

pub(crate) const MACRO_GET_CONTEXT: &str = "Macro.GetContext";

const MACRO_CONTEXT_SCHEMA_IDENTITY: &str = "market-squawk-macro-context/v1";
const KNOWLEDGE_CUTOFF_FIELD: &str = "knowledgeCutoff";
const EFFECTIVE_DATE_CUTOFF_FIELD: &str = "effectiveDateCutoff";
const RESULT_LIMITS_FIELD: &str = "resultLimits";
const FRED_SOURCE_ID: &str = "fred-fred-alfred.api-v1-v2";
const FRED_UNEMPLOYMENT_SERIES_ID: &str = "UNRATE";
const FRED_UNEMPLOYMENT_UNIT_ID: &str = "fred-unit:v1:Percent";
const H15_INDICATOR_COUNT: usize = 11;
const MACRO_CONTEXT_INDICATOR_COUNT: usize = 12;
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

/// One application-owned provider-neutral Macro product operation.
///
/// The analytical reader has no provider client, credential, network, mutation, or raw-SQL
/// authority. FRED composition uses the typed canonical-read seam on the existing restart-safe
/// operation, which continues to own its exact ready manifest.
pub(crate) struct MacroContextOperation {
    reader: AnalyticalReadCapability,
    fred: FredLatestKnownOperation,
}

impl MacroContextOperation {
    /// Binds canonical analytical reads and the restart-safe FRED read owner.
    #[must_use]
    pub(crate) fn new(reader: AnalyticalReadCapability, fred: FredLatestKnownOperation) -> Self {
        Self { reader, fred }
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
        let (board, fred) = tokio::try_join!(
            self.read_board(cutoffs, context),
            self.read_fred(cutoffs, context),
        )?;
        product_result(cutoffs, board, fred, limits)
    }

    async fn read_board(
        &self,
        cutoffs: MacroContextCutoffs,
        context: &RequestContext,
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
            .latest(&dataset, context.deadline(), context.cancellation())
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
        let query_limits = macro_context_query_limits(&request, context)?;
        let output = self
            .reader
            .read_macro_latest_known_snapshot(
                request,
                query_limits,
                context.deadline(),
                context.cancellation().clone(),
            )
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
        context: &RequestContext,
    ) -> Result<Option<AnalyticalMacroLatestKnownOutput>, ServiceError> {
        self.fred
            .read_current_analytical_latest_known(
                cutoffs.knowledge_cutoff,
                cutoffs.effective_date_cutoff,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
    }
}

impl fmt::Debug for MacroContextOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MacroContextOperation")
            .field("operation", &MACRO_GET_CONTEXT)
            .field("fred_availability", &self.fred.availability())
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
    schema_identity: &'static str,
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

fn product_result(
    cutoffs: MacroContextCutoffs,
    board: Option<AnalyticalMacroLatestKnownOutput>,
    fred: Option<AnalyticalMacroLatestKnownOutput>,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(MACRO_CONTEXT_INDICATOR_COUNT)
        .map_err(|_| ServiceError::ResourceExhausted)?;
    observations.extend(
        H15_INDICATORS
            .iter()
            .copied()
            .chain(std::iter::once(UNEMPLOYMENT_INDICATOR))
            .map(MacroContextIndicatorDefinition::unavailable),
    );
    let mut receipts = Vec::new();
    receipts
        .try_reserve_exact(2)
        .map_err(|_| ServiceError::ResourceExhausted)?;

    if let Some(board) = board {
        receipts.push(project_board(
            board,
            cutoffs,
            &mut observations[..H15_INDICATOR_COUNT],
        )?);
    }
    if let Some(fred) = fred {
        receipts.push(project_fred(
            fred,
            cutoffs,
            &mut observations[H15_INDICATOR_COUNT],
        )?);
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
    let selected_indicators = coverage
        .observed
        .checked_add(coverage.missing)
        .ok_or(ServiceError::ResourceExhausted)?;
    let selection = MacroContextSelectionDto {
        knowledge_cutoff: timestamp_text(cutoffs.knowledge_cutoff)?,
        effective_date_cutoff: cutoffs.effective_date_cutoff.to_string(),
        evaluated_at: timestamp_text(cutoffs.evaluated_at)?,
        complete,
    };
    let dto = MacroContextDto {
        schema_identity: MACRO_CONTEXT_SCHEMA_IDENTITY,
        availability,
        selection,
        confidence,
        coverage,
        observations,
    };
    let content = serde_json::to_value(dto).map_err(|_| ServiceError::InvalidResult)?;
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
    let _internal_receipt = MacroContextInternalReceipt {
        _knowledge_cutoff: cutoffs.knowledge_cutoff,
        _effective_date_cutoff: cutoffs.effective_date_cutoff,
        _evaluated_at: cutoffs.evaluated_at,
        _sources: receipts.into_boxed_slice(),
    };
    TypedToolResult::try_new(content, MACRO_CONTEXT_INDICATOR_COUNT, metadata, limits)
        .map_err(Into::into)
}

fn project_board(
    output: AnalyticalMacroLatestKnownOutput,
    cutoffs: MacroContextCutoffs,
    target: &mut [MacroContextObservationDto],
) -> Result<MacroContextSourceReceipt, ServiceError> {
    if target.len() != H15_INDICATOR_COUNT || output.observations().len() != H15_INDICATOR_COUNT {
        return Err(ServiceError::InvalidResult);
    }
    let expected_source =
        SourceId::try_from(BOARD_DDP_SOURCE_ID).map_err(|_| ServiceError::Unavailable)?;
    let expected_unit = h15_treasury_constant_maturities_canonical_unit_identifier()
        .map_err(|_| ServiceError::Unavailable)?;
    if output.source_id() != &expected_source {
        return Err(ServiceError::InvalidResult);
    }

    let mut matched = [false; H15_INDICATOR_COUNT];
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
        let (source_index, observation) = candidates.next().ok_or(ServiceError::InvalidResult)?;
        if candidates.next().is_some() || matched[source_index] {
            return Err(ServiceError::InvalidResult);
        }
        matched[source_index] = true;
        target[target_index] = project_observation(
            definition,
            observation,
            &expected_source,
            &series,
            &expected_unit,
            cutoffs,
        )?;
    }
    if matched.iter().any(|value| !value) {
        return Err(ServiceError::InvalidResult);
    }
    MacroContextSourceReceipt::try_from_output(MacroContextInternalSource::InterestRates, &output)
}

fn project_fred(
    output: AnalyticalMacroLatestKnownOutput,
    cutoffs: MacroContextCutoffs,
    target: &mut MacroContextObservationDto,
) -> Result<MacroContextSourceReceipt, ServiceError> {
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
            *target = project_observation(
                UNEMPLOYMENT_INDICATOR,
                observation,
                &expected_source,
                &expected_series,
                &expected_unit,
                cutoffs,
            )?;
        }
        [_, _, ..] => return Err(ServiceError::InvalidResult),
    }
    MacroContextSourceReceipt::try_from_output(MacroContextInternalSource::LaborMarket, &output)
}

fn project_observation(
    definition: MacroContextIndicatorDefinition,
    observation: &MacroObservation,
    expected_source: &SourceId,
    expected_series: &SourceIdentifier,
    expected_unit: &SourceIdentifier,
    cutoffs: MacroContextCutoffs,
) -> Result<MacroContextObservationDto, ServiceError> {
    let context = observation.context();
    let provenance = context.provenance();
    let time = context.time();
    let effective_date = time
        .effective()
        .calendar_date_value()
        .filter(|date| *date <= cutoffs.effective_date_cutoff)
        .ok_or(ServiceError::InvalidResult)?;
    let knowledge_date = timestamp_calendar_date(cutoffs.knowledge_cutoff)?;
    let recorded = match time.published() {
        Some(published) => {
            let date = published
                .calendar_date_value()
                .filter(|date| *date <= knowledge_date)
                .ok_or(ServiceError::InvalidResult)?;
            MacroContextRecordedDateDto::Known {
                date: date.to_string(),
            }
        }
        None => MacroContextRecordedDateDto::NotSupplied,
    };
    let superseded_after = time
        .superseded()
        .map(|superseded| {
            superseded
                .calendar_date_value()
                .map(|date| date.to_string())
                .ok_or(ServiceError::InvalidResult)
        })
        .transpose()?;
    let available_at = provenance
        .availability()
        .conservative_available_at()
        .filter(|available_at| *available_at <= cutoffs.knowledge_cutoff)
        .ok_or(ServiceError::InvalidResult)?;
    if observation.series() != expected_series
        || observation.unit() != expected_unit
        || provenance.source_id() != expected_source
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
        superseded_after,
        value,
        availability,
        confidence,
    })
}

struct MacroContextInternalReceipt {
    _knowledge_cutoff: Timestamp,
    _effective_date_cutoff: CalendarDate,
    _evaluated_at: Timestamp,
    _sources: Box<[MacroContextSourceReceipt]>,
}

struct MacroContextSourceReceipt {
    _source: MacroContextInternalSource,
    _source_id: SourceId,
    _manifest: DatasetManifestRef,
    _object_graph_digest: EvidenceDigest,
    _query_identity: EvidenceDigest,
    _result_digest: EvidenceDigest,
    _selection_digest: EvidenceDigest,
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
            _source: source,
            _source_id: output.source_id().clone(),
            _manifest: pinned.manifest().clone(),
            _object_graph_digest: object_graph_digest,
            _query_identity: query_identity,
            _result_digest: result_digest,
            _selection_digest: selection_digest,
        })
    }
}

enum MacroContextInternalSource {
    InterestRates,
    LaborMarket,
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
    context: &RequestContext,
) -> Result<QueryLimits, ServiceError> {
    let now = std::time::Instant::now();
    if now >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    let deadline = context
        .deadline()
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
        deadline,
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
