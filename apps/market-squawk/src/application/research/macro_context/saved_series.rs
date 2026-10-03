//! Bounded neutral series discovery and exact saved macro reads over sealed original rows.

use super::super::CensusRestartSelector;
use super::*;
use arrow::array::{Array, BinaryArray};
use market_squawk_adapter_census::{
    CensusNativeMacroCoordinate, CensusPublicationPlan, decode_census_native_macro_coordinate,
};
use market_squawk_data::{
    AnalyticalMacroProviderPeriodLatestKnownRequest, AnalyticalMacroSeriesAllowlist,
    AnalyticalMacroSourceQualifiedSeries, AnalyticalObservationReadRequest,
    AnalyticalObservationTemplate, AnalyticalReadError, PersistedProviderCaptureBindingEvidence,
    QueryResult,
};
use market_squawk_domain::{ResearchObservation, ResearchPeriod};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;

pub(crate) const MACRO_GET_LATEST_SERIES_OBSERVATION: &str = "Macro.GetLatestSeriesObservation";
const SERIES_ID_FIELD: &str = "seriesId";
const AFTER_SERIES_ID_FIELD: &str = "afterSeriesId";
const MAX_SERIES: usize = 4_096;
const MAX_PROCESSED_ROWS: usize = 16_384;
const PAGE_SERIES: usize = 64;
pub(super) const QUERY_BYTES: u64 = 16 * 1024 * 1024;
const QUERY_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_INDEX_BYTES: usize = 16 * 1024 * 1024;
pub(super) const SOURCE_ID: &str = "census-census.data-api";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum EffectiveKind {
    CalendarDate,
    Period(SourceIdentifier),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SavedSeries {
    pub(super) id: SourceIdentifier,
    pub(super) label: String,
    pub(super) unit: SourceIdentifier,
    pub(super) geography: Value,
    pub(super) dimensions: Value,
    pub(super) effective_kind: EffectiveKind,
}

pub(super) struct SavedGeneration {
    pub(super) selector: CensusRestartSelector,
    pub(super) evidence: PersistedProviderCaptureBindingEvidence,
    pub(super) observations: Vec<MacroObservation>,
}

impl MacroContextOperation {
    pub(crate) async fn list_saved_series(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(request, context)?;
        let mut cutoff_arguments = request.arguments().clone();
        cutoff_arguments.remove(AFTER_SERIES_ID_FIELD);
        let cutoffs = MacroContextCutoffs::parse(&cutoff_arguments)?;
        let after = request
            .arguments()
            .get(AFTER_SERIES_ID_FIELD)
            .map(|value| {
                value
                    .as_str()
                    .ok_or(ServiceError::InvalidRequest)
                    .and_then(|id| {
                        SourceIdentifier::try_from(id).map_err(|_| ServiceError::InvalidRequest)
                    })
            })
            .transpose()?;
        let manifests = self
            .read
            .saved_manifest_candidates(
                cutoffs,
                context.deadline(),
                context.cancellation().child_token(),
            )
            .await?;
        let source = SourceId::try_from(SOURCE_ID).map_err(|_| ServiceError::Unavailable)?;
        let mut entries = BTreeMap::new();
        let mut decoded_rows = 0usize;
        let mut index_bytes = 0usize;
        for manifest in &manifests {
            let generation = self
                .read
                .load_saved_generation(
                    manifest.clone(),
                    &source,
                    context.deadline(),
                    context.cancellation().child_token(),
                )
                .await?;
            account_generation_rows(&mut decoded_rows, &generation)?;
            index_generation(&generation, cutoffs, &mut entries, &mut index_bytes)?;
        }
        let maximum = limits.maximum_result_items().min(PAGE_SERIES);
        if maximum == 0 {
            return Err(ServiceError::ResourceExhausted);
        }
        let mut remaining = entries
            .into_values()
            .filter(|entry| after.as_ref().is_none_or(|after| &entry.id > after));
        let items = remaining
            .by_ref()
            .take(maximum)
            .map(|entry| series_item(&entry))
            .collect::<Vec<_>>();
        let has_more = remaining.next().is_some();
        let next_after = if has_more {
            items
                .last()
                .and_then(|value| value.get("seriesId"))
                .cloned()
        } else {
            None
        };
        let returned = items.len();
        let content = json!({
            "items": items,
            "hasMore": has_more,
            "nextAfterSeriesId": next_after,
            "selection": {
                "knowledgeCutoff": timestamp_text(cutoffs.knowledge_cutoff)?,
                "effectiveDateCutoff": cutoffs.effective_date_cutoff.to_string(),
            },
        });
        let metadata = ToolResultMetadata::try_complete(
            json!({"originGenerationCount": manifests.len(), "rowCoordinateVerified": true,
                "responsePlanCoverage": if manifests.is_empty() { "not_applicable" } else { "verified" }}),
            json!({"classification": "official_delayed", "selection": "exact_saved_series"}),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        TypedToolResult::try_new(content, returned, metadata, limits).map_err(Into::into)
    }

    pub(crate) async fn get_latest_saved_series_observation(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(request, context)?;
        let series = request
            .arguments()
            .get(SERIES_ID_FIELD)
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidRequest)
            .and_then(|id| {
                SourceIdentifier::try_from(id).map_err(|_| ServiceError::InvalidRequest)
            })?;
        let namespace = series_namespace(&series)?;
        let mut cutoff_arguments = request.arguments().clone();
        cutoff_arguments.remove(SERIES_ID_FIELD);
        let cutoffs = MacroContextCutoffs::parse(&cutoff_arguments)?;
        let manifests = self
            .read
            .saved_manifest_candidates(
                cutoffs,
                context.deadline(),
                context.cancellation().child_token(),
            )
            .await?;
        let research = self
            .read
            .energy_store
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let source = SourceId::try_from(SOURCE_ID).map_err(|_| ServiceError::Unavailable)?;
        let mut selected: Option<(MacroObservation, Value, SavedSeries)> = None;
        let mut decoded_rows = 0usize;
        for manifest in manifests {
            let generation = self
                .read
                .load_saved_generation(
                    manifest,
                    &source,
                    context.deadline(),
                    context.cancellation().child_token(),
                )
                .await?;
            account_generation_rows(&mut decoded_rows, &generation)?;
            let mut entries = BTreeMap::new();
            let mut index_bytes = 0usize;
            index_generation(&generation, cutoffs, &mut entries, &mut index_bytes)?;
            let Some(entry) = entries.get(&series) else {
                continue;
            };
            if !generation
                .observations
                .iter()
                .any(|row| row.series() == &series)
            {
                continue;
            }
            let allowlist = AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(vec![
                series.clone(),
            ])
            .map_err(map_read_error)?;
            let selected_output = match &entry.effective_kind {
                EffectiveKind::CalendarDate => {
                    let request = AnalyticalMacroLatestKnownRequest::try_new(
                        generation.selector.manifest().clone(),
                        source.clone(),
                        cutoffs.knowledge_cutoff,
                        cutoffs.effective_date_cutoff,
                        allowlist,
                    )
                    .map_err(map_read_error)?;
                    let limits =
                        selected_query_limits(request.required_query_rows(), context.deadline())?;
                    let result = self
                        .read
                        .reader
                        .read_macro_latest_known_snapshot(
                            request,
                            limits,
                            context.deadline(),
                            context.cancellation().child_token(),
                        )
                        .await;
                    match result {
                        Ok(output) => SelectedOutput::Calendar(output),
                        Err(AnalyticalReadError::MacroSnapshotIncomplete) => continue,
                        Err(error) => return Err(map_read_error(error)),
                    }
                }
                EffectiveKind::Period(scheme) => {
                    let cutoff = completed_period(scheme, cutoffs.effective_date_cutoff)?
                        .ok_or(ServiceError::NotFound)?;
                    let request = AnalyticalMacroProviderPeriodLatestKnownRequest::try_new(
                        generation.selector.manifest().clone(),
                        AnalyticalMacroSourceQualifiedSeries::new(source.clone(), allowlist),
                        cutoffs.knowledge_cutoff,
                        cutoff,
                    )
                    .map_err(map_read_error)?;
                    let limits =
                        selected_query_limits(request.required_query_rows(), context.deadline())?;
                    let result = self
                        .read
                        .reader
                        .read_macro_provider_period_latest_known_snapshot(
                            request,
                            limits,
                            context.deadline(),
                            context.cancellation().child_token(),
                        )
                        .await;
                    match result {
                        Ok(output) => SelectedOutput::Period(output),
                        Err(AnalyticalReadError::MacroSnapshotIncomplete) => continue,
                        Err(error) => return Err(map_read_error(error)),
                    }
                }
            };
            if selected_output.manifest() != generation.selector.manifest()
                || selected_output.source_id() != &source
                || selected_output.observations().len() != 1
            {
                return Err(ServiceError::InvalidResult);
            }
            let original = research
                .read_selected_provider_capture_evidence(
                    selected_output
                        .selected_provider_rows()
                        .map_err(map_read_error)?,
                    QUERY_BYTES as usize,
                    context.deadline(),
                    context.cancellation(),
                )
                .await
                .map_err(census::map_census_worker_error)?;
            if original.manifest() != selected_output.manifest()
                || original.selection_digest() != selected_output.selection_digest()
                || original.rows().len() != 1
            {
                return Err(ServiceError::InvalidResult);
            }
            let original = &original.rows()[0];
            if original.binding().capture().source_id() != &source
                || original.binding().capture().dataset() != generation.selector.provider_dataset()
                || original.binding().native_lineage().implementation() != "census_tabular_v1"
            {
                return Err(ServiceError::InvalidResult);
            }
            let coordinate = decode_census_native_macro_coordinate(
                original.row().native_semantic_payload(),
                &namespace,
            )
            .map_err(|_| ServiceError::InvalidResult)?;
            let observation = &selected_output.observations()[0];
            let plan = super::super::ingest::open_selected_census_plan(
                original.binding(),
                selected_output.manifest(),
            )
            .map_err(census::map_census_restart_error)?;
            super::super::ingest::verify_selected_census_row(&plan, original, observation)
                .map_err(census::map_census_restart_error)?;
            if coordinate.series() != &series
                || observation.series() != &series
                || observation.unit() != &entry.unit
                || effective_kind(observation)? != entry.effective_kind
                || serde_json::to_value(coordinate.geography())
                    .map_err(|_| ServiceError::InvalidResult)?
                    != entry.geography
                || serde_json::to_value(coordinate.predicates())
                    .map_err(|_| ServiceError::InvalidResult)?
                    != entry.dimensions
            {
                return Err(ServiceError::InvalidResult);
            }
            validate_selected_observation(observation, &source, cutoffs)?;
            let key = selected_key(observation)?;
            let digest = selected_output.selection_digest();
            let coverage = json!({
                "manifestDigest": super::super::encode_hex(selected_output.manifest().content_hash().bytes()),
                "selectionDigest": super::super::encode_hex(digest.bytes()),
                "originalBindingDigest": super::super::encode_hex(original.binding().binding_digest().bytes()),
                "rowCoordinateVerified": true,
                "responsePlanCoverage": "verified",
            });
            let replace = match selected.as_ref() {
                None => true,
                Some((prior, _, previous_entry)) => {
                    if previous_entry != entry {
                        return Err(ServiceError::InvalidResult);
                    }
                    let prior_key = selected_key(prior)?;
                    if key == prior_key && prior != observation {
                        return Err(ServiceError::InvalidResult);
                    }
                    key > prior_key
                }
            };
            if replace {
                selected = Some((observation.clone(), coverage, entry.clone()));
            }
        }
        let Some((observation, coverage, entry)) = selected else {
            return Err(ServiceError::NotFound);
        };
        let content = selected_item(&entry, &observation)?;
        let quality = json!({"classification": "official_delayed", "executionEligible": false});
        let metadata = ToolResultMetadata::try_complete(coverage, quality)
            .map_err(|_| ServiceError::InvalidResult)?;
        TypedToolResult::try_new(content, 1, metadata, limits).map_err(Into::into)
    }
}

enum SelectedOutput {
    Calendar(market_squawk_data::AnalyticalMacroLatestKnownOutput),
    Period(market_squawk_data::AnalyticalMacroProviderPeriodLatestKnownOutput),
}
impl SelectedOutput {
    fn manifest(&self) -> &DatasetManifestRef {
        match self {
            Self::Calendar(o) => o.output().manifest(),
            Self::Period(o) => o.output().manifest(),
        }
    }
    fn source_id(&self) -> &SourceId {
        match self {
            Self::Calendar(o) => o.source_id(),
            Self::Period(o) => o.source_id(),
        }
    }
    fn observations(&self) -> &[MacroObservation] {
        match self {
            Self::Calendar(o) => o.observations(),
            Self::Period(o) => o.observations(),
        }
    }
    fn selection_digest(&self) -> EvidenceDigest {
        match self {
            Self::Calendar(o) => o.selection_digest(),
            Self::Period(o) => o.selection_digest(),
        }
    }
    fn selected_provider_rows(
        &self,
    ) -> Result<market_squawk_data::SelectedProviderCaptureRows, AnalyticalReadError> {
        match self {
            Self::Calendar(o) => o.selected_provider_rows(),
            Self::Period(o) => o.selected_provider_rows(),
        }
    }
}

impl MacroContextReadCapability {
    pub(super) async fn saved_manifest_candidates(
        &self,
        cutoffs: MacroContextCutoffs,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<Vec<DatasetManifestRef>, ServiceError> {
        let Some(research) = self.energy_store.as_ref() else {
            return Ok(Vec::new());
        };
        let reader = self.reader.clone();
        research
            .run_owned_research_io(
                deadline,
                &cancellation,
                move |cancel| -> Result<_, ServiceError> {
                    let mut cursor = None;
                    let mut manifests = Vec::new();
                    let mut seen_datasets = BTreeSet::new();
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
                            // Dataset discovery reports its latest generation; capture origins enumerate
                            // every bounded historical publication under that dataset.
                            if !seen_datasets.insert(dataset.as_str().to_owned()) {
                                continue;
                            }
                            if generation.source_id().as_str() == SOURCE_ID
                                && dataset.as_str().starts_with("census.data-v1.")
                            {
                                let (origins, more) = reader
                                    .provider_capture_origin_candidates(
                                        dataset,
                                        cutoffs.knowledge_cutoff,
                                        None,
                                        market_squawk_data::AnalyticalReadLimit::try_new(64)
                                            .map_err(map_read_error)?,
                                        deadline,
                                        &cancel,
                                    )
                                    .map_err(map_read_error)?;
                                if more
                                    || manifests
                                        .len()
                                        .checked_add(origins.len())
                                        .is_none_or(|count| count > 64)
                                {
                                    return Err(ServiceError::ResourceExhausted);
                                }
                                manifests.extend(origins);
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
            .map_err(census::map_census_worker_error)?
    }

    pub(super) async fn load_saved_generation(
        &self,
        manifest: DatasetManifestRef,
        source: &SourceId,
        deadline: std::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<SavedGeneration, ServiceError> {
        let research = self
            .energy_store
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let (selector, evidence) =
            CensusRestartSelector::try_reopen(research, manifest, source, deadline, &cancellation)
                .await
                .map_err(census::map_census_restart_error)?;
        let request = AnalyticalObservationReadRequest::try_new(
            selector.manifest().clone(),
            AnalyticalObservationTemplate::Macro,
            Vec::new(),
            None,
        )
        .map_err(map_read_error)?;
        let limits = selected_query_limits((MAX_SERIES + 1) as u64, deadline)?;
        let output = self
            .reader
            .read_observations(request, limits, deadline, cancellation.child_token())
            .await
            .map_err(map_read_error)?;
        if output.source_id() != source || output.output().manifest() != selector.manifest() {
            return Err(ServiceError::InvalidResult);
        }
        let QueryResult::Inline { batches, .. } = output.output().result() else {
            return Err(ServiceError::ResourceExhausted);
        };
        let mut observations = Vec::new();
        for batch in batches {
            let payloads = batch
                .column_by_name("payload_json")
                .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
                .ok_or(ServiceError::InvalidResult)?;
            if payloads.len() != batch.num_rows() {
                return Err(ServiceError::InvalidResult);
            }
            for index in 0..batch.num_rows() {
                if payloads.is_null(index) {
                    return Err(ServiceError::InvalidResult);
                }
                let ResearchObservation::Macro(observation) =
                    serde_json::from_slice(payloads.value(index))
                        .map_err(|_| ServiceError::InvalidResult)?
                else {
                    return Err(ServiceError::InvalidResult);
                };
                if observation.context().provenance().source_id() != source {
                    return Err(ServiceError::InvalidResult);
                }
                observations.push(observation);
            }
        }
        Ok(SavedGeneration {
            selector,
            evidence,
            observations,
        })
    }
}

pub(super) fn index_generation(
    generation: &SavedGeneration,
    cutoffs: MacroContextCutoffs,
    index: &mut BTreeMap<SourceIdentifier, SavedSeries>,
    index_bytes: &mut usize,
) -> Result<(), ServiceError> {
    // The creating generation was reopened through the physical capture verifier. Its sidecar
    // can therefore prove fixed time and response-wide mapping for these original rows.
    let payload = generation
        .evidence
        .native_lineage()
        .batch_sidecar_semantic_payload()
        .ok_or(ServiceError::InvalidResult)?;
    let plan = CensusPublicationPlan::try_from_retained_payload(
        payload,
        generation.evidence.capture(),
        generation.evidence.extraction_content_identity(),
    )
    .map_err(|_| ServiceError::InvalidResult)?;
    if plan.analytical_dataset().as_str() != generation.selector.manifest().dataset_id().as_str()
        || plan.observations().len() != generation.evidence.native_lineage().row_count()
    {
        return Err(ServiceError::InvalidResult);
    }
    for row in generation.evidence.rows() {
        plan.validate_native_row(
            usize::try_from(row.canonical_row_ordinal())
                .map_err(|_| ServiceError::InvalidResult)?,
            row.native_semantic_payload(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
    }
    let mut canonical = BTreeMap::new();
    let mut eligible_times: BTreeMap<SourceIdentifier, Vec<&MacroObservation>> = BTreeMap::new();
    let mut namespaces = BTreeSet::new();
    for observation in &generation.observations {
        let provenance = observation.context().provenance();
        if provenance
            .availability()
            .conservative_available_at()
            .is_none_or(|at| at > cutoffs.knowledge_cutoff)
            || provenance.received_at() > cutoffs.knowledge_cutoff
            || provenance.ingested_at() > cutoffs.knowledge_cutoff
        {
            continue;
        }
        let namespace = series_namespace(observation.series())?;
        namespaces.insert(namespace);
        let kind = effective_kind(observation)?;
        if !effective_at_cutoff(observation, cutoffs.effective_date_cutoff)? {
            continue;
        }
        let unit = observation.unit().clone();
        let key = observation.series().clone();
        eligible_times
            .entry(key.clone())
            .or_default()
            .push(observation);
        if let Some((prior_unit, prior_kind)) = canonical.insert(key, (unit.clone(), kind.clone()))
        {
            if prior_unit != unit || prior_kind != kind {
                return Err(ServiceError::InvalidResult);
            }
        }
    }
    for row in generation.evidence.rows() {
        let Some(initial_namespace) = namespaces.first() else {
            break;
        };
        let ordinal = usize::try_from(row.canonical_row_ordinal())
            .map_err(|_| ServiceError::InvalidResult)?;
        let coordinate =
            decode_census_native_macro_coordinate(row.native_semantic_payload(), initial_namespace)
                .map_err(|_| ServiceError::InvalidResult)?;
        for namespace in &namespaces {
            let id = coordinate
                .series_for_namespace(namespace)
                .map_err(|_| ServiceError::InvalidResult)?;
            if let Some((unit, kind)) = canonical.get(&id) {
                let binding = plan
                    .observations()
                    .get(ordinal)
                    .ok_or(ServiceError::InvalidResult)?;
                if binding.canonical_series() != &id || binding.canonical_unit() != unit {
                    return Err(ServiceError::InvalidResult);
                }
                if !eligible_times.get(&id).is_some_and(|times| {
                    times.iter().any(|observation| {
                        plan.validate_canonical_observation(ordinal, observation)
                            .is_ok()
                    })
                }) {
                    continue;
                }
                let entry = saved_entry(&coordinate, id, unit.clone(), kind.clone())?;
                match index.get(&entry.id) {
                    Some(previous) if previous != &entry => {
                        return Err(ServiceError::InvalidResult);
                    }
                    Some(_) => {}
                    None => {
                        *index_bytes = index_bytes
                            .checked_add(saved_entry_size(&entry)?)
                            .ok_or(ServiceError::ResourceExhausted)?;
                        if *index_bytes > MAX_INDEX_BYTES || index.len() >= MAX_SERIES {
                            return Err(ServiceError::ResourceExhausted);
                        }
                        index.insert(entry.id.clone(), entry);
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) fn account_generation_rows(
    total: &mut usize,
    generation: &SavedGeneration,
) -> Result<(), ServiceError> {
    *total = total
        .checked_add(generation.evidence.rows().len())
        .and_then(|count| count.checked_add(generation.observations.len()))
        .ok_or(ServiceError::ResourceExhausted)?;
    if *total > MAX_PROCESSED_ROWS {
        return Err(ServiceError::ResourceExhausted);
    }
    Ok(())
}

fn saved_entry_size(entry: &SavedSeries) -> Result<usize, ServiceError> {
    let geography =
        serde_json::to_vec(&entry.geography).map_err(|_| ServiceError::InvalidResult)?;
    let dimensions =
        serde_json::to_vec(&entry.dimensions).map_err(|_| ServiceError::InvalidResult)?;
    entry
        .id
        .as_str()
        .len()
        .checked_add(entry.label.len())
        .and_then(|bytes| bytes.checked_add(entry.unit.as_str().len()))
        .and_then(|bytes| bytes.checked_add(geography.len()))
        .and_then(|bytes| bytes.checked_add(dimensions.len()))
        .ok_or(ServiceError::ResourceExhausted)
}

fn saved_entry(
    coordinate: &CensusNativeMacroCoordinate,
    id: SourceIdentifier,
    unit: SourceIdentifier,
    effective_kind: EffectiveKind,
) -> Result<SavedSeries, ServiceError> {
    Ok(SavedSeries {
        id,
        label: coordinate.label().to_owned(),
        unit,
        geography: serde_json::to_value(coordinate.geography())
            .map_err(|_| ServiceError::InvalidResult)?,
        dimensions: serde_json::to_value(coordinate.predicates())
            .map_err(|_| ServiceError::InvalidResult)?,
        effective_kind,
    })
}
pub(super) fn series_namespace(
    series: &SourceIdentifier,
) -> Result<SourceIdentifier, ServiceError> {
    let (namespace, suffix) = series
        .as_str()
        .rsplit_once(":scope:")
        .ok_or(ServiceError::InvalidResult)?;
    if suffix.len() != 64
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(ServiceError::InvalidResult);
    }
    SourceIdentifier::try_from(namespace).map_err(|_| ServiceError::InvalidResult)
}
pub(super) fn effective_kind(
    observation: &MacroObservation,
) -> Result<EffectiveKind, ServiceError> {
    let effective = observation.context().time().effective();
    if effective.calendar_date_value().is_some() {
        return Ok(EffectiveKind::CalendarDate);
    }
    let period = effective
        .source_period_value()
        .ok_or(ServiceError::InvalidResult)?;
    match period.scheme().as_str() {
        "census-year" | "census-month" | "census-quarter" => {
            Ok(EffectiveKind::Period(period.scheme().clone()))
        }
        _ => Err(ServiceError::InvalidResult),
    }
}
fn effective_at_cutoff(
    observation: &MacroObservation,
    date: CalendarDate,
) -> Result<bool, ServiceError> {
    let effective = observation.context().time().effective();
    if let Some(value) = effective.calendar_date_value() {
        return Ok(value <= date);
    }
    let period = effective
        .source_period_value()
        .ok_or(ServiceError::InvalidResult)?;
    let Some(cutoff) = completed_period(period.scheme(), date)? else {
        return Ok(false);
    };
    Ok((period.year(), period.ordinal()) <= (cutoff.year(), cutoff.ordinal()))
}

pub(super) fn completed_period(
    scheme: &SourceIdentifier,
    date: CalendarDate,
) -> Result<Option<ResearchPeriod>, ServiceError> {
    let Some((month_year, month, _)) = energy::completed_month(date)? else {
        return Ok(None);
    };
    let (year, ordinal, code) = match scheme.as_str() {
        "census-year" => {
            let year = if date.month() == 12 && date.day() == 31 {
                date.year()
            } else {
                date.year().checked_sub(1).ok_or(ServiceError::NotFound)?
            };
            (year, 1, format!("{year:04}"))
        }
        "census-month" => (month_year, month, format!("{month_year:04}-{month:02}")),
        "census-quarter" => {
            let period = census::completed_quarter(date)?.ok_or(ServiceError::NotFound)?;
            (
                period.year(),
                period.ordinal().get(),
                period.code().as_str().to_owned(),
            )
        }
        _ => return Err(ServiceError::InvalidResult),
    };
    Ok(Some(
        ResearchPeriod::try_new(
            scheme.clone(),
            year,
            NonZeroU16::new(ordinal).ok_or(ServiceError::InvalidResult)?,
            SourceIdentifier::try_from(code).map_err(|_| ServiceError::InvalidResult)?,
        )
        .map_err(|_| ServiceError::InvalidResult)?,
    ))
}
pub(super) fn selected_query_limits(
    rows: u64,
    deadline: std::time::Instant,
) -> Result<QueryLimits, ServiceError> {
    let now = std::time::Instant::now();
    if now >= deadline {
        return Err(ServiceError::DeadlineExceeded);
    }
    QueryLimits::try_new_with_inline_bytes(
        rows,
        QUERY_BYTES,
        QUERY_BYTES,
        QUERY_MEMORY_BYTES,
        4,
        2048,
        4096,
        deadline
            .saturating_duration_since(now)
            .min(Duration::from_secs(60)),
    )
    .map_err(map_query_error)
}
pub(super) fn validate_selected_observation(
    observation: &MacroObservation,
    source: &SourceId,
    cutoffs: MacroContextCutoffs,
) -> Result<(), ServiceError> {
    let provenance = observation.context().provenance();
    if provenance.source_id() != source
        || provenance.instrument_id().is_some()
        || provenance.venue_id().is_some()
        || provenance.quality() != DataQuality::OfficialDelayed
        || provenance.received_at() > cutoffs.knowledge_cutoff
        || provenance.ingested_at() > cutoffs.knowledge_cutoff
        || provenance
            .availability()
            .conservative_available_at()
            .is_none_or(|at| at > cutoffs.knowledge_cutoff)
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}
fn selected_key(
    observation: &MacroObservation,
) -> Result<(u16, u16, String, u32, i64), ServiceError> {
    let time = observation.context().time();
    let (year, ordinal, code) = match time.effective().source_period_value() {
        Some(period) => (
            period.year(),
            period.ordinal().get(),
            period.code().as_str().to_owned(),
        ),
        None => {
            let date = time
                .effective()
                .calendar_date_value()
                .ok_or(ServiceError::InvalidResult)?;
            (date.year(), u16::from(date.month()), date.to_string())
        }
    };
    let at = observation
        .context()
        .provenance()
        .availability()
        .conservative_available_at()
        .ok_or(ServiceError::InvalidResult)?;
    Ok((year, ordinal, code, time.revision().get(), at.unix_nanos()))
}
pub(super) fn observation_item(observation: &MacroObservation) -> Result<Value, ServiceError> {
    let effective = observation.context().time().effective();
    let period = effective
        .source_period_value()
        .map(|value| value.code().as_str().to_owned())
        .or_else(|| effective.calendar_date_value().map(|date| date.to_string()))
        .ok_or(ServiceError::InvalidResult)?;
    let available = observation
        .context()
        .provenance()
        .availability()
        .conservative_available_at()
        .ok_or(ServiceError::InvalidResult)?;
    let value = match (
        observation.value().observed_value(),
        observation.value().missing_value(),
    ) {
        (Some(decimal), None) => {
            json!({"state":"observed", "decimal": decimal.normalize().to_string()})
        }
        (None, Some(_)) => json!({"state":"missing", "reason":"not_reported"}),
        _ => return Err(ServiceError::InvalidResult),
    };
    Ok(json!({
        "effectivePeriod": period,
        "availableAt": timestamp_text(available)?,
        "revision": observation.context().time().revision().get(),
        "value": value,
    }))
}

pub(super) fn selected_item(
    entry: &SavedSeries,
    observation: &MacroObservation,
) -> Result<Value, ServiceError> {
    let mut item = series_item(entry);
    let values = observation_item(observation)?;
    item.as_object_mut()
        .ok_or(ServiceError::InvalidResult)?
        .extend(
            values
                .as_object()
                .ok_or(ServiceError::InvalidResult)?
                .clone(),
        );
    Ok(item)
}
pub(super) fn series_item(entry: &SavedSeries) -> Value {
    json!({"seriesId": entry.id.as_str(), "label": entry.label, "unit": entry.unit.as_str(),
        "geography": entry.geography, "dimensions": entry.dimensions, "frequency": frequency(&entry.effective_kind)})
}
pub(super) fn frequency(kind: &EffectiveKind) -> &'static str {
    match kind {
        EffectiveKind::CalendarDate => "daily",
        EffectiveKind::Period(scheme) => match scheme.as_str() {
            "census-year" => "annual",
            "census-month" => "monthly",
            "census-quarter" => "quarterly",
            _ => "unknown",
        },
    }
}
