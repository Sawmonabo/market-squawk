//! Neutral selected history over bounded exact origin generations.

use super::saved_series::{
    EffectiveKind, QUERY_BYTES, SOURCE_ID, SavedSeries, account_generation_rows, completed_period,
    effective_kind, index_generation, observation_item, selected_query_limits, series_item,
    series_namespace, validate_selected_observation,
};
use super::*;
use market_squawk_adapter_census::{CensusPublicationPlan, decode_census_native_macro_coordinate};
use market_squawk_data::{
    AnalyticalMacroHistoryRange, AnalyticalMacroHistoryRequest, AnalyticalMacroSeriesAllowlist,
    AnalyticalMacroSourceQualifiedSeries, AnalyticalReadLimit,
};
use market_squawk_domain::ResearchPeriod;
use std::collections::BTreeMap;
use std::num::NonZeroU16;

pub(crate) const MACRO_GET_SERIES_HISTORY: &str = "Macro.GetSeriesHistory";
const START_FIELD: &str = "startEffectiveDate";
const AFTER_FIELD: &str = "afterEffectivePeriod";
const SERIES_FIELD: &str = "seriesId";
const PAGE_ITEMS: usize = 32;
const MAX_MERGED_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum HistoryKey {
    Calendar(CalendarDate),
    Period(u16, u16, String),
}

struct HistoryPoint {
    observation: MacroObservation,
    retained_bytes: usize,
}

impl MacroContextOperation {
    pub(crate) async fn get_saved_series_history(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(request, context)?;
        let arguments = request.arguments();
        let series = arguments
            .get(SERIES_FIELD)
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidRequest)
            .and_then(|value| {
                SourceIdentifier::try_from(value).map_err(|_| ServiceError::InvalidRequest)
            })?;
        let namespace = series_namespace(&series)?;
        let start = arguments
            .get(START_FIELD)
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidRequest)
            .and_then(parse_calendar_date)?;
        let mut cutoff_arguments = arguments.clone();
        cutoff_arguments.remove(SERIES_FIELD);
        cutoff_arguments.remove(START_FIELD);
        cutoff_arguments.remove(AFTER_FIELD);
        let cutoffs = MacroContextCutoffs::parse(&cutoff_arguments)?;
        if start > cutoffs.effective_date_cutoff {
            return Err(ServiceError::InvalidRequest);
        }
        let after = arguments
            .get(AFTER_FIELD)
            .map(|value| value.as_str().ok_or(ServiceError::InvalidRequest))
            .transpose()?;
        let maximum = limits.maximum_result_items().min(PAGE_ITEMS);
        if maximum == 0 {
            return Err(ServiceError::ResourceExhausted);
        }
        let page_limit = AnalyticalReadLimit::try_new(maximum).map_err(map_read_error)?;
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
        let mut identity: Option<SavedSeries> = None;
        let mut merged = BTreeMap::<HistoryKey, HistoryPoint>::new();
        let mut decoded_rows = 0usize;
        let mut retained_bytes = 0usize;
        let mut has_more = false;
        let mut verified_pages = 0usize;
        let mut original_plans = BTreeMap::<[u8; 32], CensusPublicationPlan>::new();
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
            let mut index = BTreeMap::new();
            let mut index_bytes = 0usize;
            index_generation(&generation, cutoffs, &mut index, &mut index_bytes)?;
            let Some(entry) = index.get(&series) else {
                continue;
            };
            if identity.as_ref().is_some_and(|prior| prior != entry) {
                return Err(ServiceError::InvalidResult);
            }
            identity.get_or_insert_with(|| entry.clone());
            let after_coordinate = after
                .map(|value| parse_after(&entry.effective_kind, value))
                .transpose()?;
            let Some(range) =
                history_range(&entry.effective_kind, start, cutoffs.effective_date_cutoff)?
            else {
                if after_coordinate.is_some() {
                    return Err(ServiceError::InvalidRequest);
                }
                continue;
            };
            let allowlist = AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(vec![
                series.clone(),
            ])
            .map_err(map_read_error)?;
            let source_series =
                AnalyticalMacroSourceQualifiedSeries::new(source.clone(), allowlist);
            let mut selected = AnalyticalMacroHistoryRequest::try_new(
                generation.selector.manifest().clone(),
                source_series,
                cutoffs.knowledge_cutoff,
                range.clone(),
                page_limit,
                None,
            )
            .map_err(map_read_error)?;
            if let Some(after) = after_coordinate {
                selected = selected
                    .after_coordinate(series.clone(), after)
                    .map_err(map_read_error)?;
            }
            let query_limits =
                selected_query_limits(selected.required_query_rows(), context.deadline())?;
            let page = self
                .read
                .reader
                .read_macro_selected_history(
                    selected,
                    query_limits,
                    context.deadline(),
                    context.cancellation().child_token(),
                )
                .await
                .map_err(map_read_error)?;
            if page.request().manifest() != generation.selector.manifest()
                || page.output().manifest() != generation.selector.manifest()
                || page.request().source_series().source_id() != &source
                || page.request().range() != &range
                || page.request().knowledge_cutoff() != cutoffs.knowledge_cutoff
                || page.observations().len() > maximum
            {
                return Err(ServiceError::InvalidResult);
            }
            if page.observations().is_empty() {
                if page.next_cursor().is_some() {
                    return Err(ServiceError::InvalidResult);
                }
                continue;
            }
            verified_pages += 1;
            has_more |= page.next_cursor().is_some();
            let original = research
                .read_selected_provider_capture_evidence(
                    page.selected_provider_rows(),
                    QUERY_BYTES as usize,
                    context.deadline(),
                    context.cancellation(),
                )
                .await
                .map_err(super::census::map_census_worker_error)?;
            if original.manifest() != page.output().manifest()
                || original.selection_digest() != page.selection_digest()
                || original.rows().len() != page.observations().len()
            {
                return Err(ServiceError::InvalidResult);
            }
            for (observation, native) in page.observations().iter().zip(original.rows()) {
                if native.binding().capture().source_id() != &source
                    || native.binding().capture().dataset()
                        != generation.selector.provider_dataset()
                    || native.binding().native_lineage().implementation() != "census_tabular_v1"
                {
                    return Err(ServiceError::InvalidResult);
                }
                let binding_key = native.binding().binding_digest().bytes();
                if !original_plans.contains_key(&binding_key) {
                    let plan = super::super::ingest::open_selected_census_plan(
                        native.binding(),
                        page.output().manifest(),
                    )
                    .map_err(super::census::map_census_restart_error)?;
                    original_plans.insert(binding_key, plan);
                }
                let plan = original_plans
                    .get(&binding_key)
                    .ok_or(ServiceError::InvalidResult)?;
                if plan.analytical_dataset().as_str()
                    != page.output().manifest().dataset_id().as_str()
                {
                    return Err(ServiceError::InvalidResult);
                }
                super::super::ingest::verify_selected_census_row(plan, native, observation)
                    .map_err(super::census::map_census_restart_error)?;
                let ordinal = usize::try_from(native.row().canonical_row_ordinal())
                    .map_err(|_| ServiceError::InvalidResult)?;
                let time_matches = plan.observations().get(ordinal).is_some_and(|row| {
                    row.effective_time() == observation.context().time().effective()
                });
                let coordinate = decode_census_native_macro_coordinate(
                    native.row().native_semantic_payload(),
                    &namespace,
                )
                .map_err(|_| ServiceError::InvalidResult)?;
                if coordinate.series() != &series
                    || observation.series() != &series
                    || !time_matches
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
                if !in_history_range(observation, &range) {
                    return Err(ServiceError::InvalidResult);
                }
                let key = history_key(observation)?;
                let point_bytes = serde_json::to_vec(observation)
                    .map_err(|_| ServiceError::InvalidResult)?
                    .len();
                match merged.get_mut(&key) {
                    Some(previous) => {
                        let old = history_rank(&previous.observation)?;
                        let new = history_rank(observation)?;
                        if new == old && previous.observation != *observation {
                            return Err(ServiceError::InvalidResult);
                        }
                        if new > old {
                            retained_bytes = retained_bytes
                                .checked_sub(previous.retained_bytes)
                                .and_then(|v| v.checked_add(point_bytes))
                                .ok_or(ServiceError::ResourceExhausted)?;
                            *previous = HistoryPoint {
                                observation: observation.clone(),
                                retained_bytes: point_bytes,
                            };
                        }
                    }
                    None => {
                        retained_bytes = retained_bytes
                            .checked_add(point_bytes)
                            .ok_or(ServiceError::ResourceExhausted)?;
                        merged.insert(
                            key,
                            HistoryPoint {
                                observation: observation.clone(),
                                retained_bytes: point_bytes,
                            },
                        );
                    }
                }
                if retained_bytes > MAX_MERGED_BYTES {
                    return Err(ServiceError::ResourceExhausted);
                }
            }
        }
        let entry = identity.ok_or(ServiceError::NotFound)?;
        let mut remaining = merged.into_iter();
        let mut observations = Vec::new();
        let mut last = None;
        for (key, point) in remaining.by_ref().take(maximum) {
            last = Some(key);
            observations.push(observation_item(&point.observation)?);
        }
        has_more |= remaining.next().is_some();
        let next_after = if has_more {
            Some(history_key_text(
                last.as_ref().ok_or(ServiceError::InvalidResult)?,
            ))
        } else {
            None
        };
        let mut content = series_item(&entry);
        let fields = content.as_object_mut().ok_or(ServiceError::InvalidResult)?;
        fields.insert(
            "selection".into(),
            json!({
                "knowledgeCutoff": timestamp_text(cutoffs.knowledge_cutoff)?,
                "startEffectiveDate": start.to_string(),
                "effectiveDateCutoff": cutoffs.effective_date_cutoff.to_string(),
            }),
        );
        fields.insert("observations".into(), Value::Array(observations));
        fields.insert("hasMore".into(), Value::Bool(has_more));
        fields.insert("nextAfterEffectivePeriod".into(), json!(next_after));
        let returned = fields
            .get("observations")
            .and_then(Value::as_array)
            .ok_or(ServiceError::InvalidResult)?
            .len();
        let metadata = ToolResultMetadata::try_complete(
            json!({"originGenerationCount": manifests.len(), "verifiedHistoryPages": verified_pages,
            "rowCoordinateVerified": true, "responsePlanCoverage": "verified"}),
            json!({"classification": "official_delayed", "executionEligible": false}),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        TypedToolResult::try_new(content, returned, metadata, limits).map_err(Into::into)
    }
}

fn history_range(
    kind: &EffectiveKind,
    start: CalendarDate,
    end: CalendarDate,
) -> Result<Option<AnalyticalMacroHistoryRange>, ServiceError> {
    match kind {
        EffectiveKind::CalendarDate => {
            Ok(Some(AnalyticalMacroHistoryRange::Calendar { start, end }))
        }
        EffectiveKind::Period(scheme) => {
            let first = period_containing(scheme, start)?;
            let Some(last) = completed_period(scheme, end)? else {
                return Ok(None);
            };
            if first.partial_cmp(&last).is_none_or(|order| order.is_gt()) {
                return Ok(None);
            }
            Ok(Some(AnalyticalMacroHistoryRange::ProviderPeriod {
                start: first,
                end: last,
            }))
        }
    }
}

fn period_containing(
    scheme: &SourceIdentifier,
    date: CalendarDate,
) -> Result<ResearchPeriod, ServiceError> {
    let year = date.year();
    let (ordinal, code) = match scheme.as_str() {
        "census-year" => (1, format!("{year:04}")),
        "census-month" => (
            u16::from(date.month()),
            format!("{year:04}-{:02}", date.month()),
        ),
        "census-quarter" => {
            let quarter = (u16::from(date.month()) - 1) / 3 + 1;
            (quarter, format!("{year:04}-Q{quarter}"))
        }
        _ => return Err(ServiceError::InvalidResult),
    };
    ResearchPeriod::try_new(
        scheme.clone(),
        year,
        NonZeroU16::new(ordinal).ok_or(ServiceError::InvalidResult)?,
        SourceIdentifier::try_from(code).map_err(|_| ServiceError::InvalidResult)?,
    )
    .map_err(|_| ServiceError::InvalidResult)
}

fn parse_after(
    kind: &EffectiveKind,
    text: &str,
) -> Result<ResearchTemporalCoordinate, ServiceError> {
    match kind {
        EffectiveKind::CalendarDate => {
            parse_calendar_date(text).map(ResearchTemporalCoordinate::calendar_date)
        }
        EffectiveKind::Period(scheme) => {
            let date = match scheme.as_str() {
                "census-year" if text.len() == 4 => format!("{text}-01-01"),
                "census-month" if text.len() == 7 => format!("{text}-01"),
                "census-quarter"
                    if text.len() == 7 && text.as_bytes().get(4..6) == Some(b"-Q".as_slice()) =>
                {
                    let year = std::str::from_utf8(&text.as_bytes()[..4])
                        .map_err(|_| ServiceError::InvalidRequest)?;
                    let quarter = std::str::from_utf8(&text.as_bytes()[6..])
                        .map_err(|_| ServiceError::InvalidRequest)?
                        .parse::<u8>()
                        .map_err(|_| ServiceError::InvalidRequest)?;
                    let month = quarter
                        .checked_sub(1)
                        .filter(|q| *q < 4)
                        .map(|q| q * 3 + 1)
                        .ok_or(ServiceError::InvalidRequest)?;
                    format!("{year}-{month:02}-01")
                }
                _ => return Err(ServiceError::InvalidRequest),
            };
            let date = parse_calendar_date(&date)?;
            let period = period_containing(scheme, date)?;
            if period.code().as_str() != text {
                return Err(ServiceError::InvalidRequest);
            }
            Ok(ResearchTemporalCoordinate::source_period(period))
        }
    }
}

fn history_key(observation: &MacroObservation) -> Result<HistoryKey, ServiceError> {
    let effective = observation.context().time().effective();
    if let Some(date) = effective.calendar_date_value() {
        return Ok(HistoryKey::Calendar(date));
    }
    let period = effective
        .source_period_value()
        .ok_or(ServiceError::InvalidResult)?;
    Ok(HistoryKey::Period(
        period.year(),
        period.ordinal().get(),
        period.code().as_str().to_owned(),
    ))
}

fn history_key_text(key: &HistoryKey) -> String {
    match key {
        HistoryKey::Calendar(date) => date.to_string(),
        HistoryKey::Period(_, _, code) => code.clone(),
    }
}

fn history_rank(observation: &MacroObservation) -> Result<(u32, i64), ServiceError> {
    let provenance = observation.context().provenance();
    let available = provenance
        .availability()
        .conservative_available_at()
        .ok_or(ServiceError::InvalidResult)?;
    Ok((
        observation.context().time().revision().get(),
        available.unix_nanos(),
    ))
}

fn in_history_range(observation: &MacroObservation, range: &AnalyticalMacroHistoryRange) -> bool {
    let effective = observation.context().time().effective();
    match range {
        AnalyticalMacroHistoryRange::Calendar { start, end } => effective
            .calendar_date_value()
            .is_some_and(|date| *start <= date && date <= *end),
        AnalyticalMacroHistoryRange::ProviderPeriod { start, end } => {
            effective.source_period_value().is_some_and(|period| {
                matches!(
                    period.partial_cmp(start),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ) && matches!(
                    period.partial_cmp(end),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            })
        }
    }
}
