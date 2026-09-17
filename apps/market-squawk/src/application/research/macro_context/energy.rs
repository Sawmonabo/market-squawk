//! Monthly residential electricity prices from exact retained canonical publication evidence.

use std::num::NonZeroU16;

use market_squawk_adapter_eia::{
    EiaNativePublishedSeriesCoordinate, decode_eia_native_published_series_coordinate,
};
use market_squawk_domain::ResearchPeriod;

use super::super::{EiaMacroEffectiveCutoff, EiaMacroRestartReceipt, EiaMacroRestartSelector};
use super::*;

const SOURCE_ID: &str = "eia-eia.api-v2";
const INDICATOR: MacroContextIndicatorDefinition = MacroContextIndicatorDefinition {
    indicator_id: "us-residential-electricity-price",
    label: "U.S. residential electricity price",
    category: MacroContextCategory::EnergyPrices,
    frequency: MacroContextFrequency::Monthly,
    seasonal_adjustment: MacroContextSeasonalAdjustment::NotSupplied,
    unit: &ENERGY_PRICE_UNIT,
    source_slot: "residential-electricity-price",
};

static ENERGY_PRICE_UNIT: MacroContextUnitDto = MacroContextUnitDto {
    code: Cow::Borrowed("native_energy_price"),
    label: Cow::Borrowed("Native price unit"),
    symbol: None,
};

pub(super) struct EnergyRead {
    pub(super) receipt: EiaMacroRestartReceipt,
    coordinate: EiaNativePublishedSeriesCoordinate,
    effective_cutoff: ResearchPeriod,
}

impl MacroContextReadCapability {
    pub(super) async fn read_energy(
        &self,
        cutoffs: MacroContextCutoffs,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<EnergyRead>, ServiceError> {
        let Some(research) = self.energy_store.as_ref() else {
            return Ok(None);
        };
        let Some((year, month, code)) = completed_month(cutoffs.effective_date_cutoff)? else {
            return Ok(None);
        };
        let dataset = DatasetId::try_from(RESIDENTIAL_ELECTRICITY_PRICE_DATASET)
            .map_err(|_| ServiceError::Unavailable)?;
        let reader = self.reader.clone();
        let (mut origins, _has_older_origins) = research
            .run_owned_research_io(deadline, &cancellation, move |worker_cancellation| {
                reader.provider_capture_origin_candidates(
                    &dataset,
                    cutoffs.knowledge_cutoff,
                    None,
                    market_squawk_data::AnalyticalReadLimit::try_new(1)?,
                    deadline,
                    &worker_cancellation,
                )
            })
            .await
            .map_err(map_energy_worker_error)?
            .map_err(map_read_error)?;
        let Some(manifest) = origins.pop() else {
            return Ok(None);
        };
        let source = SourceId::try_from(SOURCE_ID).map_err(|_| ServiceError::Unavailable)?;
        // The SQL selection applies the original publication cutoff before choosing one creating
        // generation. A newer publication cannot replace this pinned source outcome; corruption
        // or an exact-evidence mismatch fails instead of selecting a descendant or older fallback.
        let (selector, evidence) = EiaMacroRestartSelector::try_reopen(
            research,
            manifest,
            &source,
            deadline,
            &cancellation,
        )
        .await
        .map_err(map_energy_restart_error)?;
        if evidence.rows().is_empty() || evidence.rows().len() > 24 {
            return Err(ServiceError::InvalidResult);
        }
        let mut coordinate = None;
        for row in evidence.rows() {
            if cancellation.is_cancelled() {
                return Err(ServiceError::Cancelled);
            }
            if std::time::Instant::now() >= deadline {
                return Err(ServiceError::DeadlineExceeded);
            }
            let decoded =
                decode_eia_native_published_series_coordinate(row.native_semantic_payload())
                    .map_err(|_| ServiceError::InvalidResult)?;
            if !decoded.is_us_residential_monthly_electricity_price()
                || coordinate
                    .as_ref()
                    .is_some_and(|previous| previous != &decoded)
            {
                return Err(ServiceError::InvalidResult);
            }
            coordinate = Some(decoded);
        }
        let coordinate = coordinate.ok_or(ServiceError::InvalidResult)?;
        let scheme = selector
            .provider_period_scheme(coordinate.canonical_series())
            .ok_or(ServiceError::InvalidResult)?
            .clone();
        let effective_cutoff = ResearchPeriod::try_new(
            scheme,
            year,
            NonZeroU16::new(month).ok_or(ServiceError::InvalidResult)?,
            SourceIdentifier::try_from(code).map_err(|_| ServiceError::InvalidResult)?,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let series = AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(vec![
            coordinate.canonical_series().clone(),
        ])
        .map_err(map_read_error)?;
        let request = selector
            .try_point_in_time_request(
                series,
                cutoffs.knowledge_cutoff,
                EiaMacroEffectiveCutoff::ProviderPeriod(effective_cutoff.clone()),
            )
            .map_err(|_| ServiceError::InvalidResult)?;
        let now = std::time::Instant::now();
        if now >= deadline {
            return Err(ServiceError::DeadlineExceeded);
        }
        let limits = QueryLimits::try_new_with_inline_bytes(
            request.required_query_rows(),
            MACRO_CONTEXT_QUERY_BYTES,
            MACRO_CONTEXT_QUERY_BYTES,
            MACRO_CONTEXT_QUERY_MEMORY_BYTES,
            4,
            2_048,
            4_096,
            deadline
                .saturating_duration_since(now)
                .min(Duration::from_secs(60)),
        )
        .map_err(map_query_error)?;
        let receipt = selector
            .reopen_point_in_time(research, request, limits, deadline, cancellation.clone())
            .await
            .map_err(map_energy_restart_error)?;
        if cancellation.is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        if std::time::Instant::now() >= deadline {
            return Err(ServiceError::DeadlineExceeded);
        }
        Ok(Some(EnergyRead {
            receipt,
            coordinate,
            effective_cutoff,
        }))
    }
}

fn map_energy_worker_error(error: crate::ResearchServiceError) -> ServiceError {
    use market_squawk_data::IngestError;
    use market_squawk_platform::{ResearchObjectControlError, SealedResearchJournalStoreError};
    match error {
        crate::ResearchServiceError::Ingest(IngestError::Cancelled)
        | crate::ResearchServiceError::ProviderCaptureStore(
            SealedResearchJournalStoreError::ObjectControl(ResearchObjectControlError::Cancelled),
        ) => ServiceError::Cancelled,
        crate::ResearchServiceError::Ingest(IngestError::DeadlineExceeded)
        | crate::ResearchServiceError::ProviderCaptureStore(
            SealedResearchJournalStoreError::ObjectControl(
                ResearchObjectControlError::DeadlineExceeded,
            ),
        ) => ServiceError::DeadlineExceeded,
        crate::ResearchServiceError::IngestAuthorityMismatch => ServiceError::InvalidResult,
        _ => ServiceError::Unavailable,
    }
}

fn map_energy_restart_error(error: super::super::EiaMacroApplicationError) -> ServiceError {
    use super::super::EiaMacroApplicationError;
    match error {
        EiaMacroApplicationError::Cancelled => ServiceError::Cancelled,
        EiaMacroApplicationError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        EiaMacroApplicationError::AnalyticalRead(error) => map_read_error(error),
        EiaMacroApplicationError::Research(error) => map_energy_worker_error(error),
        EiaMacroApplicationError::RestartInvalid
        | EiaMacroApplicationError::SeriesNotPublished
        | EiaMacroApplicationError::TemporalPrecisionMismatch => ServiceError::InvalidResult,
        _ => ServiceError::Unavailable,
    }
}

/// Selects only months wholly within the requested civil-date boundary. This is a query boundary,
/// never a fabricated date/instant assigned to a provider's monthly observation.
pub(super) fn completed_month(
    date: CalendarDate,
) -> Result<Option<(u16, u16, String)>, ServiceError> {
    let date = chrono::NaiveDate::parse_from_str(&date.to_string(), "%Y-%m-%d")
        .map_err(|_| ServiceError::InvalidRequest)?;
    let month = if date
        .succ_opt()
        .is_some_and(|next| next.month() != date.month())
    {
        date
    } else {
        let Some(previous) = date.with_day(1).and_then(|first| first.pred_opt()) else {
            return Ok(None);
        };
        previous
    };
    if month.year() < 1 {
        return Ok(None);
    }
    Ok(Some((
        u16::try_from(month.year()).map_err(|_| ServiceError::InvalidRequest)?,
        u16::try_from(month.month()).map_err(|_| ServiceError::InvalidRequest)?,
        month.format("%Y-%m").to_string(),
    )))
}

pub(super) fn project_energy(
    read: Option<EnergyRead>,
    cutoffs: MacroContextCutoffs,
    observations: &mut Vec<MacroContextObservationDto>,
    inputs: &mut Vec<MacroContextInputObservation>,
    receipts: &mut Vec<Arc<MacroContextSourceReceipt>>,
) -> Result<MacroContextSelectedObservation, ServiceError> {
    let mut selected = MacroContextSelectedObservation::unavailable(INDICATOR);
    let mut projected = INDICATOR.unavailable();
    let Some(read) = read else {
        observations.push(projected);
        return Ok(selected);
    };
    let output = read.receipt.output();
    let receipt = Arc::new(MacroContextSourceReceipt {
        source: MacroContextInternalSource::EnergyPrices,
        source_id: read.receipt.source_id().clone(),
        manifest: output.manifest().clone(),
        object_graph_digest: require_sha256(output.object_graph_digest())?,
        query_identity: require_sha256(output.query_identity())?,
        result_digest: require_sha256(output.result_digest())?,
        selection_digest: require_sha256(read.receipt.selection_digest())?,
        native_binding_digest: Some(require_sha256(read.receipt.evidence().binding_digest())?),
    });
    projected.unit.label = Cow::Owned(read.coordinate.native_unit().to_owned());
    let rows = read.receipt.observations();
    if rows.len() > 1 {
        return Err(ServiceError::InvalidResult);
    }
    if let Some(observation) = rows.first() {
        let context = observation.context();
        let provenance = context.provenance();
        let time = context.time();
        let period = time
            .effective()
            .source_period_value()
            .ok_or(ServiceError::InvalidResult)?;
        if !matches!(
            period.partial_cmp(&read.effective_cutoff),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ) || observation.series() != read.coordinate.canonical_series()
            || observation.unit() != read.coordinate.canonical_unit()
            || provenance.source_id() != read.receipt.source_id()
            || provenance.instrument_id().is_some()
            || provenance.venue_id().is_some()
            || provenance.quality() != DataQuality::OfficialDelayed
            || period.ordinal().get() > 12
            || period.code().as_str() != format!("{:04}-{:02}", period.year(), period.ordinal())
        {
            return Err(ServiceError::InvalidResult);
        }
        match provenance.payload_reference() {
            PayloadReference::ContentHash(hash)
                if hash.algorithm() == DigestAlgorithm::Sha256 && hash.digest() != [0; 32] => {}
            _ => return Err(ServiceError::InvalidResult),
        }
        let available_at = provenance
            .availability()
            .conservative_available_at()
            .filter(|at| *at <= cutoffs.knowledge_cutoff)
            .ok_or(ServiceError::InvalidResult)?;
        projected.recorded = match time.published() {
            Some(published) => MacroContextRecordedDateDto::Known {
                date: coordinate_calendar_date_at_knowledge(published, cutoffs.knowledge_cutoff)?
                    .to_string(),
            },
            None => MacroContextRecordedDateDto::NotSupplied,
        };
        projected.superseded_after = time
            .superseded()
            .map(coordinate_calendar_date)
            .transpose()?
            .map(|date| date.to_string());
        projected.effective_period = Some(period.code().as_str().to_owned());
        projected.available_at = Some(timestamp_text(available_at)?);
        projected.revision = Some(time.revision().get());
        match (
            observation.value().observed_value(),
            observation.value().missing_value(),
        ) {
            (Some(value), None) => {
                projected.value = MacroContextValueDto::Observed {
                    decimal: value.normalize().to_string(),
                };
                projected.availability = MacroContextObservationAvailability::Available;
                projected.confidence = MacroContextConfidenceDto::moderate();
            }
            (None, Some(_)) => {
                projected.value = MacroContextValueDto::Missing {
                    reason: MacroContextMissingReason::NotReported,
                    explanation: "No value was reported at this cutoff.",
                };
                projected.availability = MacroContextObservationAvailability::Missing;
                projected.confidence = MacroContextConfidenceDto::limited();
            }
            _ => return Err(ServiceError::InvalidResult),
        }
        inputs.push(MacroContextInputObservation {
            role: MacroContextInputRole::ResidentialElectricityPrice,
            observation: observation.clone(),
        });
        selected.observation = Some(observation.clone());
        selected.authority = Some(MacroContextSelectionAuthority::OfficialEnergyStatistics);
        selected.source_receipt = Some(Arc::clone(&receipt));
    }
    receipts.push(receipt);
    observations.push(projected);
    Ok(selected)
}
