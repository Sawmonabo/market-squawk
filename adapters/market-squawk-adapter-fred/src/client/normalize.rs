//! Canonical FRED/ALFRED research-observation normalization.

use bytes::Bytes;
use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailabilityEvidence, CalendarDate, DataQuality,
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, MacroMissingValue, MacroObservation,
    PayloadHash, PayloadReference, ResearchContext, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime, RevisionNumber,
    SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AvailabilityEvidence as ExtractionAvailabilityEvidence, ExtractionBatch,
    ProviderNativeLineageBatch, ProviderNativeLineageBatchBuilder,
    ProviderNativeLineageImplementation, SourceMetadata,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{
    FredDataset, FredNamespace, FredSeriesMetadata, FredSeriesMetadataDocument, FredSourceError,
    MAX_FRED_SERIES_METADATA_REVISIONS,
};

pub(super) struct CanonicalPageContext {
    pub(super) payload_digest: [u8; 32],
}

#[derive(Debug)]
pub(super) struct FredNativeLineagePlan {
    provider_dataset: SourceIdentifier,
    dataset: FredDataset,
    page: crate::FredObservationPage,
    series_revisions: Box<[FredSeriesMetadata]>,
}

struct FredSemanticIntersection<'a> {
    observation: &'a crate::FredObservation,
    metadata_revision_ordinal: usize,
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
}

impl FredNativeLineagePlan {
    pub(super) fn try_new(
        provider_dataset: SourceIdentifier,
        dataset: FredDataset,
        page: crate::FredObservationPage,
        series_revisions: Box<[FredSeriesMetadata]>,
    ) -> Result<Self, FredSourceError> {
        if FredDataset::parse(&provider_dataset).ok().as_ref() != Some(&dataset)
            || !metadata_revisions_are_unambiguous(&series_revisions, &dataset)
            || page.realtime_start() != dataset.realtime_start()
            || page.realtime_end() != dataset.realtime_end()
            || page.observations().is_empty()
        {
            return Err(FredSourceError::Protocol);
        }
        Ok(Self {
            provider_dataset,
            dataset,
            page,
            series_revisions,
        })
    }

    pub(super) fn try_encode(
        self,
        batch: &ExtractionBatch,
    ) -> Result<(ProviderNativeLineageBatch, Vec<u16>), FredSourceError> {
        if batch.request().object().dataset() != &self.provider_dataset {
            return Err(FredSourceError::Protocol);
        }
        let intersections = observation_metadata_intersections(
            &self.page,
            &self.series_revisions,
            batch.records().len(),
        )?;
        if intersections.len() != batch.records().len() {
            return Err(FredSourceError::Protocol);
        }
        let mut builder = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::FredAlfredSeriesObservationsV1,
            batch,
        )
        .map_err(|_| FredSourceError::Protocol)?;
        let mut native_series_revisions = Vec::new();
        native_series_revisions
            .try_reserve_exact(self.series_revisions.len())
            .map_err(|_| FredSourceError::Protocol)?;
        for series in &self.series_revisions {
            native_series_revisions.push(FredNativeSeriesV1 {
                id: series.series_id(),
                realtime_start: series.realtime_start(),
                realtime_end: series.realtime_end(),
                title: series.title(),
                observation_start: series.observation_start(),
                observation_end: series.observation_end(),
                frequency: series.frequency(),
                frequency_short: series.frequency_short(),
                units: series.units(),
                units_short: series.units_short(),
                seasonal_adjustment: series.seasonal_adjustment(),
                seasonal_adjustment_short: series.seasonal_adjustment_short(),
                last_updated: series.last_updated(),
                popularity: series.popularity(),
                notes: series.notes(),
            });
        }
        builder
            .try_set_batch_sidecar(&FredNativeLineageBatchV1 {
                version: 1,
                family: "fred_alfred_series_observations",
                namespace: match self.dataset.namespace {
                    FredNamespace::Fred => "fred",
                    FredNamespace::Alfred => "alfred",
                },
                provider_dataset: &self.provider_dataset,
                response_mode: FredNativeResponseModeV1 {
                    output_type: 1,
                    file_type: "json",
                    order_by: "observation_date",
                    sort_order: "asc",
                },
                series_revisions: native_series_revisions,
                semantic_rows: intersections.len(),
                page: FredNativePageV1 {
                    realtime_start: self.page.realtime_start(),
                    realtime_end: self.page.realtime_end(),
                    observation_start: self.page.observation_start(),
                    observation_end: self.page.observation_end(),
                    units: self.page.units(),
                    count: self.page.count(),
                    offset: self.page.offset(),
                    limit: self.page.limit(),
                    next_offset: self.page.next_offset(),
                    terminal: self.page.next_offset().is_none(),
                    returned: self.page.observations().len(),
                },
            })
            .map_err(|_| FredSourceError::Protocol)?;
        let mut row_capture_page_ordinals = Vec::new();
        row_capture_page_ordinals
            .try_reserve_exact(intersections.len())
            .map_err(|_| FredSourceError::Protocol)?;
        for (record, intersection) in batch.records().iter().zip(intersections) {
            let observation = intersection.observation;
            let expected_revision = source_revision_identifier(
                &self.dataset,
                observation.observation_date(),
                intersection.realtime_start,
            )?;
            if record.revision() != &expected_revision
                || record.effective_time().calendar_date_value()
                    != Some(observation.observation_date())
                || record
                    .published_time()
                    .and_then(ResearchTemporalCoordinate::calendar_date_value)
                    != Some(intersection.realtime_start)
                || record
                    .superseded_time()
                    .and_then(ResearchTemporalCoordinate::calendar_date_value)
                    != exclusive_superseded_at(intersection.realtime_end)?
                        .and_then(|coordinate| coordinate.calendar_date_value())
            {
                return Err(FredSourceError::Protocol);
            }
            builder
                .try_push(&FredNativeLineageRowV1 {
                    realtime_start: intersection.realtime_start,
                    realtime_end: intersection.realtime_end,
                    provider_realtime_start: observation.realtime_start(),
                    provider_realtime_end: observation.realtime_end(),
                    observation_date: observation.observation_date(),
                    raw_value: observation.raw_value(),
                    value: observation.value(),
                    missing_marker: observation.value().is_none().then_some("."),
                    metadata_revision_ordinal: u16::try_from(
                        intersection.metadata_revision_ordinal,
                    )
                    .map_err(|_| FredSourceError::Protocol)?,
                })
                .map_err(|_| FredSourceError::Protocol)?;
            row_capture_page_ordinals.push(1);
        }
        let lineage = builder.finish().map_err(|_| FredSourceError::Protocol)?;
        Ok((lineage, row_capture_page_ordinals))
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct FredNativeLineageBatchV1<'a> {
    version: u16,
    family: &'static str,
    namespace: &'static str,
    provider_dataset: &'a SourceIdentifier,
    response_mode: FredNativeResponseModeV1,
    series_revisions: Vec<FredNativeSeriesV1<'a>>,
    semantic_rows: usize,
    page: FredNativePageV1<'a>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct FredNativeResponseModeV1 {
    output_type: u8,
    file_type: &'static str,
    order_by: &'static str,
    sort_order: &'static str,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct FredNativeSeriesV1<'a> {
    id: &'a SourceIdentifier,
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
    title: &'a str,
    observation_start: CalendarDate,
    observation_end: CalendarDate,
    frequency: &'a str,
    frequency_short: &'a str,
    units: &'a str,
    units_short: &'a str,
    seasonal_adjustment: &'a str,
    seasonal_adjustment_short: &'a str,
    last_updated: &'a str,
    popularity: u32,
    notes: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct FredNativePageV1<'a> {
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
    observation_start: CalendarDate,
    observation_end: CalendarDate,
    units: &'a str,
    count: usize,
    offset: usize,
    limit: usize,
    next_offset: Option<usize>,
    terminal: bool,
    returned: usize,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct FredNativeLineageRowV1<'a> {
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
    provider_realtime_start: CalendarDate,
    provider_realtime_end: CalendarDate,
    observation_date: CalendarDate,
    raw_value: &'a str,
    value: Option<rust_decimal::Decimal>,
    missing_marker: Option<&'static str>,
    metadata_revision_ordinal: u16,
}

pub(super) fn canonical_observation_payloads(
    source: &SourceMetadata,
    dataset: &FredDataset,
    page: &crate::FredObservationPage,
    page_context: CanonicalPageContext,
    series_metadata: &FredSeriesMetadataDocument,
    received_at: Timestamp,
    ingested_at: Timestamp,
    max_records: usize,
) -> Result<Vec<CanonicalFredRecord>, FredSourceError> {
    let series =
        SourceIdentifier::try_from(dataset.series_id()).map_err(|_| FredSourceError::Protocol)?;
    let page_reference = PayloadReference::ContentHash(PayloadHash::new(
        DigestAlgorithm::Sha256,
        page_context.payload_digest,
    ));
    observation_metadata_intersections(page, series_metadata.series_revisions(), max_records)?
        .into_iter()
        .map(|intersection| {
            let observation = intersection.observation;
            let metadata = series_metadata
                .series_revisions()
                .get(intersection.metadata_revision_ordinal)
                .ok_or(FredSourceError::Protocol)?;
            let unit = fred_unit_identifier(metadata.units())?;
            let revision_number = revision_number_for_vintage(intersection.realtime_start)?;
            let source_revision = source_revision_identifier(
                dataset,
                observation.observation_date(),
                intersection.realtime_start,
            )?;
            let availability = ResearchAvailabilityEvidence::local_first_observed(received_at);
            let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
                source_id: source.source_id().clone(),
                instrument_id: None,
                venue_id: None,
                source_identifier: source_revision.clone(),
                source_timestamp: None,
                received_at,
                ingested_at,
                quality: DataQuality::OfficialDelayed,
                payload_reference: page_reference.clone(),
                availability,
            })
            .map_err(|_| FredSourceError::Protocol)?;
            let effective =
                ResearchTemporalCoordinate::calendar_date(observation.observation_date());
            let published = ResearchTemporalCoordinate::calendar_date(intersection.realtime_start);
            let superseded = exclusive_superseded_at(intersection.realtime_end)?;
            let time = ResearchTime::try_new_with_coordinates(
                effective.clone(),
                Some(published.clone()),
                revision_number,
                superseded.clone(),
            )
            .map_err(|_| FredSourceError::Protocol)?;
            let context =
                ResearchContext::new(provenance, time).map_err(|_| FredSourceError::Protocol)?;
            let macro_observation = match observation.value() {
                Some(value) => MacroObservation::new(context, series.clone(), value, unit.clone()),
                None => MacroObservation::missing(
                    context,
                    series.clone(),
                    MacroMissingValue::new(
                        SourceIdentifier::try_from(observation.raw_value())
                            .map_err(|_| FredSourceError::Protocol)?,
                        None,
                    ),
                    unit.clone(),
                ),
            };
            let payload = serde_json::to_vec(&ResearchObservation::Macro(macro_observation))
                .map(Bytes::from)
                .map_err(|_| FredSourceError::Protocol)?;
            let digest: [u8; 32] = Sha256::digest(&payload).into();
            Ok(CanonicalFredRecord {
                effective,
                published: Some(published),
                superseded,
                availability: ExtractionAvailabilityEvidence::LocalFirstObserved {
                    observed_at: received_at,
                },
                revision: source_revision,
                evidence: ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    digest,
                )),
                payload,
            })
        })
        .collect()
}

fn metadata_revisions_are_unambiguous(
    revisions: &[FredSeriesMetadata],
    dataset: &FredDataset,
) -> bool {
    if revisions.is_empty() || revisions.len() > MAX_FRED_SERIES_METADATA_REVISIONS {
        return false;
    }
    let mut previous_end = None;
    for revision in revisions {
        if revision.series_id().as_str() != dataset.series_id()
            || revision.realtime_start() > revision.realtime_end()
            || previous_end.is_some_and(|end: CalendarDate| revision.realtime_start() <= end)
        {
            return false;
        }
        previous_end = Some(revision.realtime_end());
    }
    true
}

fn applicable_metadata_revision(
    revisions: &[FredSeriesMetadata],
    published: CalendarDate,
) -> Result<(usize, &FredSeriesMetadata), FredSourceError> {
    let upper = revisions.partition_point(|revision| revision.realtime_start() <= published);
    let ordinal = upper.checked_sub(1).ok_or(FredSourceError::Protocol)?;
    let revision = revisions.get(ordinal).ok_or(FredSourceError::Protocol)?;
    if published > revision.realtime_end() {
        return Err(FredSourceError::Protocol);
    }
    Ok((ordinal, revision))
}

fn observation_metadata_intersections<'a>(
    page: &'a crate::FredObservationPage,
    revisions: &'a [FredSeriesMetadata],
    max_rows: usize,
) -> Result<Vec<FredSemanticIntersection<'a>>, FredSourceError> {
    if max_rows == 0 {
        return Err(FredSourceError::Protocol);
    }
    let mut intersections = Vec::new();
    intersections
        .try_reserve_exact(page.observations().len().min(max_rows))
        .map_err(|_| FredSourceError::Protocol)?;
    for observation in page.observations() {
        let mut realtime_start = observation.realtime_start().max(page.realtime_start());
        let observation_end = observation.realtime_end().min(page.realtime_end());
        if realtime_start > observation_end {
            return Err(FredSourceError::Protocol);
        }
        loop {
            let (metadata_revision_ordinal, metadata) =
                applicable_metadata_revision(revisions, realtime_start)?;
            let realtime_end = observation_end.min(metadata.realtime_end());
            if realtime_start < metadata.realtime_start()
                || realtime_end < realtime_start
                || intersections.len() == max_rows
            {
                return Err(FredSourceError::Protocol);
            }
            intersections.push(FredSemanticIntersection {
                observation,
                metadata_revision_ordinal,
                realtime_start,
                realtime_end,
            });
            if realtime_end == observation_end {
                break;
            }
            realtime_start = next_calendar_date(realtime_end)?.ok_or(FredSourceError::Protocol)?;
        }
    }
    Ok(intersections)
}

fn revision_number_for_vintage(
    realtime_start: CalendarDate,
) -> Result<RevisionNumber, FredSourceError> {
    const DAYS_FROM_YEAR_ONE_TO_UNIX_EPOCH: i32 = 719_163;

    let one_based_day = realtime_start
        .days_since_unix_epoch()
        .checked_add(DAYS_FROM_YEAR_ONE_TO_UNIX_EPOCH)
        .ok_or(FredSourceError::Protocol)?;
    let one_based_day = u32::try_from(one_based_day).map_err(|_| FredSourceError::Protocol)?;
    RevisionNumber::new(one_based_day).map_err(|_| FredSourceError::Protocol)
}

fn source_revision_identifier(
    dataset: &FredDataset,
    effective: CalendarDate,
    realtime_start: CalendarDate,
) -> Result<SourceIdentifier, FredSourceError> {
    SourceIdentifier::try_from(format!(
        "{}:{}:{effective}:{realtime_start}",
        match dataset.namespace {
            FredNamespace::Fred => "fred",
            FredNamespace::Alfred => "alfred",
        },
        dataset.series_id(),
    ))
    .map_err(|_| FredSourceError::Protocol)
}

fn fred_unit_identifier(value: &str) -> Result<SourceIdentifier, FredSourceError> {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut identifier = String::from("fred-unit:v1:");
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            identifier.push(char::from(byte));
        } else {
            identifier.push('%');
            identifier.push(char::from(HEX[usize::from(byte >> 4)]));
            identifier.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    SourceIdentifier::try_from(identifier).map_err(|_| FredSourceError::Protocol)
}

pub(super) struct CanonicalFredRecord {
    pub(super) effective: ResearchTemporalCoordinate,
    pub(super) published: Option<ResearchTemporalCoordinate>,
    pub(super) superseded: Option<ResearchTemporalCoordinate>,
    pub(super) availability: ExtractionAvailabilityEvidence,
    pub(super) revision: SourceIdentifier,
    pub(super) evidence: ExactPayloadEvidence,
    pub(super) payload: Bytes,
}

fn exclusive_superseded_at(
    inclusive_end: CalendarDate,
) -> Result<Option<ResearchTemporalCoordinate>, FredSourceError> {
    Ok(next_calendar_date(inclusive_end)?.map(ResearchTemporalCoordinate::calendar_date))
}

fn next_calendar_date(date: CalendarDate) -> Result<Option<CalendarDate>, FredSourceError> {
    if date == CalendarDate::new(9999, 12, 31).map_err(|_| FredSourceError::Protocol)? {
        return Ok(None);
    }
    let last_day = match date.month() {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if date.year().is_multiple_of(4)
            && (!date.year().is_multiple_of(100) || date.year().is_multiple_of(400)) =>
        {
            29
        }
        2 => 28,
        _ => return Err(FredSourceError::Protocol),
    };
    let next = if date.day() < last_day {
        CalendarDate::new(
            date.year(),
            date.month(),
            date.day().checked_add(1).ok_or(FredSourceError::Protocol)?,
        )
    } else if date.month() < 12 {
        CalendarDate::new(
            date.year(),
            date.month()
                .checked_add(1)
                .ok_or(FredSourceError::Protocol)?,
            1,
        )
    } else {
        CalendarDate::new(
            date.year()
                .checked_add(1)
                .ok_or(FredSourceError::Protocol)?,
            1,
            1,
        )
    }
    .map_err(|_| FredSourceError::Protocol)?;
    Ok(Some(next))
}

#[cfg(test)]
mod tests {
    use market_squawk_domain::{CalendarDate, SourceIdentifier};

    use crate::{FredObservationPage, FredParseLimits, FredSeriesMetadata};

    use super::{FredDataset, FredNamespace};
    use super::{
        applicable_metadata_revision, exclusive_superseded_at, observation_metadata_intersections,
        revision_number_for_vintage, source_revision_identifier,
    };

    #[test]
    fn provider_vintage_identity_is_window_invariant_ordered_and_exact()
    -> Result<(), Box<dyn std::error::Error>> {
        let page = |realtime_start: &str,
                    realtime_end: &str,
                    observations: serde_json::Value|
         -> Result<Vec<u8>, serde_json::Error> {
            serde_json::to_vec(&serde_json::json!({
                "realtime_start": realtime_start,
                "realtime_end": realtime_end,
                "observation_start": "2023-01-01",
                "observation_end": "2023-01-01",
                "units": "lin",
                "output_type": 1,
                "file_type": "json",
                "order_by": "observation_date",
                "sort_order": "asc",
                "count": observations.as_array().map_or(0, Vec::len),
                "offset": 0,
                "limit": 100,
                "observations": observations,
            }))
        };
        let vintage = serde_json::json!({
            "realtime_start": "2024-01-15",
            "realtime_end": "2024-01-31",
            "date": "2023-01-01",
            "value": "101.25",
        });
        let later_vintage = serde_json::json!({
            "realtime_start": "2024-02-01",
            "realtime_end": "9999-12-31",
            "date": "2023-01-01",
            "value": "102.5",
        });
        let limits = FredParseLimits::production_defaults();
        let narrow = FredObservationPage::parse(
            &page(
                "2024-01-01",
                "2024-01-31",
                serde_json::json!([vintage.clone()]),
            )?,
            limits,
        )?;
        let wide = FredObservationPage::parse(
            &page(
                "2023-01-01",
                "2024-12-31",
                serde_json::json!([vintage, later_vintage]),
            )?,
            limits,
        )?;
        let narrow_vintage = &narrow.observations()[0];
        let wide_vintage = &wide.observations()[0];
        let narrow_revision = revision_number_for_vintage(narrow_vintage.realtime_start())?;
        let wide_revision = revision_number_for_vintage(wide_vintage.realtime_start())?;
        assert_eq!(narrow_revision, wide_revision);
        assert_eq!(
            wide_revision,
            revision_number_for_vintage(wide_vintage.realtime_start())?
        );
        assert!(
            revision_number_for_vintage(wide.observations()[1].realtime_start())?.get()
                > wide_revision.get()
        );

        let narrow_dataset = FredDataset {
            namespace: FredNamespace::Alfred,
            series_id: "CPIAUCSL".to_owned(),
            realtime_start: narrow.realtime_start(),
            realtime_end: narrow.realtime_end(),
        };
        let wide_dataset = FredDataset {
            namespace: FredNamespace::Alfred,
            series_id: "CPIAUCSL".to_owned(),
            realtime_start: wide.realtime_start(),
            realtime_end: wide.realtime_end(),
        };
        let effective = narrow_vintage.observation_date();
        let expected_identifier =
            SourceIdentifier::try_from("alfred:CPIAUCSL:2023-01-01:2024-01-15")?;
        assert_eq!(
            source_revision_identifier(
                &narrow_dataset,
                effective,
                narrow_vintage.realtime_start()
            )?,
            expected_identifier
        );
        assert_eq!(
            source_revision_identifier(&wide_dataset, effective, wide_vintage.realtime_start())?,
            expected_identifier
        );

        let january = exclusive_superseded_at(narrow_vintage.realtime_end())?
            .and_then(|value| value.calendar_date_value())
            .ok_or("missing January boundary")?;
        assert_eq!(january, CalendarDate::new(2024, 2, 1)?);
        assert!(exclusive_superseded_at(wide.observations()[1].realtime_end())?.is_none());

        let series_id = SourceIdentifier::try_from("CPIAUCSL")?;
        let metadata_response = |realtime_start: &str,
                                 realtime_end: &str,
                                 units: &str|
         -> Result<Vec<u8>, serde_json::Error> {
            serde_json::to_vec(&serde_json::json!({
                "realtime_start": realtime_start,
                "realtime_end": realtime_end,
                "seriess": [{
                    "id": "CPIAUCSL",
                    "realtime_start": realtime_start,
                    "realtime_end": realtime_end,
                    "title": "Consumer price index",
                    "observation_start": "1947-01-01",
                    "observation_end": "2024-12-31",
                    "frequency": "Monthly",
                    "frequency_short": "M",
                    "units": units,
                    "units_short": units,
                    "seasonal_adjustment": "Seasonally Adjusted",
                    "seasonal_adjustment_short": "SA",
                    "last_updated": "2024-02-01 07:30:00-06",
                    "popularity": 90
                }]
            }))
        };
        let metadata_revisions = [
            FredSeriesMetadata::parse_probe_response(
                &metadata_response("2023-01-01", "2024-01-31", "Index")?,
                &series_id,
                limits,
            )?,
            FredSeriesMetadata::parse_probe_response(
                &metadata_response("2024-02-01", "2024-12-31", "Percent")?,
                &series_id,
                limits,
            )?,
        ];
        assert_eq!(
            applicable_metadata_revision(
                &metadata_revisions,
                wide.observations()[0].realtime_start()
            )?
            .1
            .units(),
            "Index"
        );
        assert_eq!(
            applicable_metadata_revision(
                &metadata_revisions,
                wide.observations()[1].realtime_start()
            )?
            .1
            .units(),
            "Percent"
        );

        let split_metadata = [
            FredSeriesMetadata::parse_probe_response(
                &metadata_response("2024-01-10", "2024-01-20", "Index")?,
                &series_id,
                limits,
            )?,
            FredSeriesMetadata::parse_probe_response(
                &metadata_response("2024-01-21", "2024-01-31", "Percent")?,
                &series_id,
                limits,
            )?,
        ];
        assert!(split_metadata[0].realtime_start() > narrow.realtime_start());
        let intersections = observation_metadata_intersections(&narrow, &split_metadata, 2)?;
        assert_eq!(intersections.len(), 2);
        assert_eq!(intersections[0].metadata_revision_ordinal, 0);
        assert_eq!(
            intersections[0].realtime_start,
            CalendarDate::new(2024, 1, 15)?
        );
        assert_eq!(
            intersections[0].realtime_end,
            CalendarDate::new(2024, 1, 20)?
        );
        assert_eq!(intersections[1].metadata_revision_ordinal, 1);
        assert_eq!(
            intersections[1].realtime_start,
            CalendarDate::new(2024, 1, 21)?
        );
        assert_eq!(
            intersections[1].realtime_end,
            CalendarDate::new(2024, 1, 31)?
        );

        let divergent_same_version = serde_json::json!([
            {
                "realtime_start": "2024-01-15",
                "realtime_end": "2024-01-31",
                "date": "2023-01-01",
                "value": "101.25",
            },
            {
                "realtime_start": "2024-01-15",
                "realtime_end": "2024-02-29",
                "date": "2023-01-01",
                "value": ".",
            }
        ]);
        assert!(
            FredObservationPage::parse(
                &page("2024-01-01", "2024-02-29", divergent_same_version)?,
                limits,
            )
            .is_err()
        );
        Ok(())
    }
}
