//! Regional employment from original Census publication generations at the requested cutoff.

use super::super::{
    CensusQuarterlyPointInTimeRequest, CensusQuarterlyRestartReceipt, CensusRestartSelector,
};
use super::*;
use market_squawk_adapter_census::{
    census_native_is_qwi_dataset, decode_census_qwi_employment_series,
};
use market_squawk_domain::ResearchPeriod;
use std::num::NonZeroU16;

const SOURCE_ID: &str = "census-census.data-api";
const INDICATOR: MacroContextIndicatorDefinition = MacroContextIndicatorDefinition {
    indicator_id: "california-beginning-quarter-employment",
    label: "California beginning-of-quarter employment",
    category: MacroContextCategory::LaborMarket,
    frequency: MacroContextFrequency::Quarterly,
    seasonal_adjustment: MacroContextSeasonalAdjustment::NotSupplied,
    unit: &EMPLOYMENT_UNIT,
    source_slot: "california-beginning-quarter-employment",
};
static EMPLOYMENT_UNIT: MacroContextUnitDto = MacroContextUnitDto {
    code: Cow::Borrowed("persons"),
    label: Cow::Borrowed("Persons"),
    symbol: None,
};
pub(super) struct CensusRead {
    receipt: CensusQuarterlyRestartReceipt,
    series: SourceIdentifier,
    effective_cutoff: ResearchPeriod,
    source_receipt: Arc<MacroContextSourceReceipt>,
    consulted: Vec<Arc<MacroContextSourceReceipt>>,
}
impl CensusRead {
    pub(super) fn observations(&self) -> &[MacroObservation] {
        self.receipt.output().observations()
    }
}

impl MacroContextReadCapability {
    pub(super) async fn read_census(
        &self,
        cutoffs: MacroContextCutoffs,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<CensusRead>, ServiceError> {
        let Some(research) = self.energy_store.as_ref() else {
            return Ok(None);
        };
        let Some(effective_cutoff) = completed_quarter(cutoffs.effective_date_cutoff)? else {
            return Ok(None);
        };
        let reader = self.reader.clone();
        let manifests = research
            .run_owned_research_io(
                deadline,
                &cancellation,
                move |cancel| -> Result<_, ServiceError> {
                    let mut cursor = None;
                    let mut manifests = Vec::new();
                    let mut seen_datasets = std::collections::BTreeSet::new();
                    let mut seen_manifests = std::collections::BTreeSet::new();
                    // The bound covers the complete scan, including unrelated datasets. Exhaustion never
                    // silently turns a partial scan into a latest-known answer.
                    for page_index in 0..16 {
                        let page = reader
                            .datasets(
                                cursor.as_ref(),
                                market_squawk_data::AnalyticalReadLimit::try_new(64)
                                    .map_err(map_read_error)?,
                                deadline,
                                &cancel,
                            )
                            .map_err(map_read_error)?;
                        for generation in page.generations() {
                            let dataset = generation.manifest().dataset_id();
                            if !seen_datasets.insert(dataset.as_str().to_owned()) {
                                return Err(ServiceError::InvalidResult);
                            }
                            if generation.source_id().as_str() == SOURCE_ID
                                && dataset.as_str().starts_with("census.data-v1.")
                            {
                                let (origins, _) = reader
                                    .provider_capture_origin_candidates(
                                        dataset,
                                        cutoffs.knowledge_cutoff,
                                        None,
                                        market_squawk_data::AnalyticalReadLimit::try_new(1)
                                            .map_err(map_read_error)?,
                                        deadline,
                                        &cancel,
                                    )
                                    .map_err(map_read_error)?;
                                for origin in origins {
                                    let identity = (
                                        origin.dataset_id().as_str().to_owned(),
                                        origin.manifest_version(),
                                        origin.content_hash().bytes().to_vec(),
                                    );
                                    if seen_manifests.insert(identity) {
                                        manifests.push(origin);
                                    }
                                }
                                if manifests.len() > 64 {
                                    return Err(ServiceError::ResourceExhausted);
                                }
                            }
                        }
                        if !page.has_more() {
                            return Ok(manifests);
                        }
                        if page_index == 15 {
                            return Err(ServiceError::ResourceExhausted);
                        }
                        cursor = Some(
                            page.generations()
                                .last()
                                .ok_or(ServiceError::InvalidResult)?
                                .manifest()
                                .dataset_id()
                                .clone(),
                        );
                    }
                    Err(ServiceError::ResourceExhausted)
                },
            )
            .await
            .map_err(map_census_worker_error)??;
        let source = SourceId::try_from(SOURCE_ID).map_err(|_| ServiceError::Unavailable)?;
        let mut selected: Option<CensusRead> = None;
        let mut consulted = Vec::new();
        let mut decoded_rows = 0_usize;
        for manifest in manifests {
            let (selector, evidence) = CensusRestartSelector::try_reopen(
                research,
                manifest,
                &source,
                deadline,
                &cancellation,
            )
            .await
            .map_err(map_census_restart_error)?;
            let first = evidence.rows().first().ok_or(ServiceError::InvalidResult)?;
            if !census_native_is_qwi_dataset(first.native_semantic_payload())
                .map_err(|_| ServiceError::InvalidResult)?
            {
                continue;
            }
            decoded_rows = decoded_rows
                .checked_add(evidence.rows().len())
                .ok_or(ServiceError::ResourceExhausted)?;
            if decoded_rows > MAXIMUM_MACRO_CONTEXT_INPUTS {
                return Err(ServiceError::ResourceExhausted);
            }
            let mut series = None;
            for row in evidence.rows() {
                match decode_census_qwi_employment_series(row.native_semantic_payload())
                    .map_err(|_| ServiceError::InvalidResult)?
                {
                    Some(candidate) => {
                        if series.as_ref().is_some_and(|prior| prior != &candidate) {
                            return Err(ServiceError::InvalidResult);
                        }
                        series = Some(candidate);
                    }
                    None => {}
                }
            }
            let Some(series) = series else {
                continue;
            };
            let allowlist = AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(vec![
                series.clone(),
            ])
            .map_err(map_read_error)?;
            let request = CensusQuarterlyPointInTimeRequest::try_new(
                &selector,
                allowlist,
                cutoffs.knowledge_cutoff,
                effective_cutoff.clone(),
            )
            .map_err(map_census_restart_error)?;
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
                2048,
                4096,
                deadline
                    .saturating_duration_since(now)
                    .min(Duration::from_secs(60)),
            )
            .map_err(map_query_error)?;
            let receipt = selector
                .reopen_quarterly(research, request, limits, deadline, cancellation.clone())
                .await
                .map_err(map_census_restart_error)?;
            let output = receipt.output();
            // The full generation may select an inherited row. Decode that row's original
            // native evidence, rather than attaching the creating run's newer binding.
            for (original, observation) in receipt.evidence().rows().iter().zip(output.observations()) {
                if decode_census_qwi_employment_series(original.row().native_semantic_payload())
                    .map_err(|_| ServiceError::InvalidResult)?
                    .as_ref() != Some(observation.series())
                {
                    return Err(ServiceError::InvalidResult);
                }
            }
            let source_receipt = Arc::new(MacroContextSourceReceipt {
                source: MacroContextInternalSource::RegionalEmployment,
                source_id: output.source_id().clone(),
                manifest: output.output().manifest().clone(),
                object_graph_digest: require_sha256(output.output().object_graph_digest())?,
                semantic_query_identity: require_sha256(output.output().semantic_query_identity())?,
                result_digest: require_sha256(output.output().result_digest())?,
                selection_digest: require_sha256(output.selection_digest())?,
                native_binding_digest: receipt.selected_binding_digest().map(require_sha256).transpose()?,
            });
            consulted.push(Arc::clone(&source_receipt));
            if output.observations().len() > 1 {
                return Err(ServiceError::InvalidResult);
            }
            let Some(observation) = output.observations().first() else {
                continue;
            };
            let key = selection_key(observation)?;
            let replace = match selected.as_ref().and_then(|old| old.observations().first()) {
                None => true,
                Some(old) => {
                    let old_key = selection_key(old)?;
                    if key == old_key
                        && serde_json::to_value(observation)
                            .map_err(|_| ServiceError::InvalidResult)?
                            != serde_json::to_value(old).map_err(|_| ServiceError::InvalidResult)?
                    {
                        return Err(ServiceError::InvalidResult);
                    }
                    key > old_key
                }
            };
            if replace {
                selected = Some(CensusRead {
                    receipt,
                    series,
                    effective_cutoff: effective_cutoff.clone(),
                    source_receipt,
                    consulted: Vec::new(),
                });
            }
        }
        if let Some(selected) = selected.as_mut() {
            selected.consulted = consulted;
        }
        Ok(selected)
    }
}
fn selection_key(observation: &MacroObservation) -> Result<(u16, u16, i64), ServiceError> {
    let context = observation.context();
    let period = context
        .time()
        .effective()
        .source_period_value()
        .ok_or(ServiceError::InvalidResult)?;
    let available = context
        .provenance()
        .availability()
        .conservative_available_at()
        .ok_or(ServiceError::InvalidResult)?;
    Ok((
        period.year(),
        period.ordinal().get(),
        available.unix_nanos(),
    ))
}
pub(super) fn completed_quarter(date: CalendarDate) -> Result<Option<ResearchPeriod>, ServiceError> {
    let Some((mut year, month, _)) = super::energy::completed_month(date)? else {
        return Ok(None);
    };
    let mut quarter = month / 3;
    if quarter == 0 {
        let Some(previous) = year.checked_sub(1) else {
            return Ok(None);
        };
        year = previous;
        quarter = 4;
    }
    if year == 0 {
        return Ok(None);
    }
    Ok(Some(
        ResearchPeriod::try_new(
            SourceIdentifier::try_from("census-quarter")
                .map_err(|_| ServiceError::InvalidResult)?,
            year,
            NonZeroU16::new(quarter).ok_or(ServiceError::InvalidResult)?,
            SourceIdentifier::try_from(format!("{year:04}-Q{quarter}"))
                .map_err(|_| ServiceError::InvalidResult)?,
        )
        .map_err(|_| ServiceError::InvalidResult)?,
    ))
}
pub(super) fn map_census_worker_error(error: crate::ResearchServiceError) -> ServiceError {
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

pub(super) fn map_census_restart_error(error: super::super::CensusMacroApplicationError) -> ServiceError {
    use super::super::CensusMacroApplicationError;
    match error {
        CensusMacroApplicationError::Research(error) => map_census_worker_error(error),
        CensusMacroApplicationError::Service(error) => error,
        CensusMacroApplicationError::AnalyticalRead(error) => map_read_error(error),
        CensusMacroApplicationError::RestartInvalid
        | CensusMacroApplicationError::QuarterlySelectionInvalid => ServiceError::InvalidResult,
        _ => ServiceError::Unavailable,
    }
}

pub(super) fn project_census(
    read: Option<CensusRead>,
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
    let selected_output = read.receipt.output();
    // Preserve the same receipt retained in consulted evidence; the product result
    // verifies original selection authority by Arc identity.
    let receipt = Arc::clone(&read.source_receipt);
    let rows = selected_output.observations();
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
        ) || observation.series() != &read.series
            || observation.unit().as_str() != EMPLOYMENT_UNIT.code.as_ref()
            || provenance.source_id() != selected_output.source_id()
            || provenance.instrument_id().is_some()
            || provenance.venue_id().is_some()
            || provenance.quality() != DataQuality::OfficialDelayed
            || period.ordinal().get() > 4
            || period.code().as_str() != format!("{:04}-Q{}", period.year(), period.ordinal())
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
            role: MacroContextInputRole::CaliforniaBeginningQuarterEmployment,
            observation: observation.clone(),
        });
        selected.observation = Some(observation.clone());
        selected.authority = Some(MacroContextSelectionAuthority::OfficialEmploymentStatistics);
        selected.source_receipt = Some(Arc::clone(&receipt));
    }
    receipts.extend(read.consulted);
    observations.push(projected);
    Ok(selected)
}
