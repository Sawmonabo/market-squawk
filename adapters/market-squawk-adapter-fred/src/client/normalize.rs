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
    AvailabilityEvidence as ExtractionAvailabilityEvidence, SourceMetadata,
};
use sha2::{Digest, Sha256};

use super::{FredDataset, FredNamespace, FredSeriesMetadataDocument, FredSourceError};

pub(super) struct CanonicalPageContext {
    pub(super) payload_digest: [u8; 32],
}

pub(super) fn canonical_observation_payloads(
    source: &SourceMetadata,
    dataset: &FredDataset,
    page: &crate::FredObservationPage,
    page_context: CanonicalPageContext,
    series_metadata: &FredSeriesMetadataDocument,
    received_at: Timestamp,
    ingested_at: Timestamp,
) -> Result<Vec<CanonicalFredRecord>, FredSourceError> {
    let unit = fred_unit_identifier(series_metadata.series().units())?;
    let series =
        SourceIdentifier::try_from(dataset.series_id()).map_err(|_| FredSourceError::Protocol)?;
    let page_reference = PayloadReference::ContentHash(PayloadHash::new(
        DigestAlgorithm::Sha256,
        page_context.payload_digest,
    ));
    page.observations()
        .iter()
        .map(|observation| {
            let revision_number = revision_number_for_vintage(observation.realtime_start())?;
            let source_revision = source_revision_identifier(
                dataset,
                observation.observation_date(),
                observation.realtime_start(),
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
            let published = ResearchTemporalCoordinate::calendar_date(observation.realtime_start());
            let superseded = exclusive_superseded_at(observation.realtime_end())?;
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
    if inclusive_end == CalendarDate::new(9999, 12, 31).map_err(|_| FredSourceError::Protocol)? {
        return Ok(None);
    }
    let last_day = match inclusive_end.month() {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if inclusive_end.year().is_multiple_of(4)
            && (!inclusive_end.year().is_multiple_of(100)
                || inclusive_end.year().is_multiple_of(400)) =>
        {
            29
        }
        2 => 28,
        _ => return Err(FredSourceError::Protocol),
    };
    let next = if inclusive_end.day() < last_day {
        CalendarDate::new(
            inclusive_end.year(),
            inclusive_end.month(),
            inclusive_end
                .day()
                .checked_add(1)
                .ok_or(FredSourceError::Protocol)?,
        )
    } else if inclusive_end.month() < 12 {
        CalendarDate::new(
            inclusive_end.year(),
            inclusive_end
                .month()
                .checked_add(1)
                .ok_or(FredSourceError::Protocol)?,
            1,
        )
    } else {
        CalendarDate::new(
            inclusive_end
                .year()
                .checked_add(1)
                .ok_or(FredSourceError::Protocol)?,
            1,
            1,
        )
    }
    .map_err(|_| FredSourceError::Protocol)?;
    Ok(Some(ResearchTemporalCoordinate::calendar_date(next)))
}

#[cfg(test)]
mod tests {
    use market_squawk_domain::{CalendarDate, SourceIdentifier};

    use crate::{FredObservationPage, FredParseLimits};

    use super::{FredDataset, FredNamespace};
    use super::{exclusive_superseded_at, revision_number_for_vintage, source_revision_identifier};

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
