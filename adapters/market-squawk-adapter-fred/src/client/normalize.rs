//! Canonical FRED/ALFRED research-observation normalization.

use bytes::Bytes;
use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailabilityEvidence, DataQuality, DigestAlgorithm,
    EvidenceDigest, ExactPayloadEvidence, MacroMissingValue, MacroObservation, PayloadHash,
    PayloadReference, ResearchContext, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime, RevisionNumber,
    SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AvailabilityEvidence as ExtractionAvailabilityEvidence, SourceMetadata,
};
use sha2::{Digest, Sha256};

use super::{FredDataset, FredNamespace, FredSeriesMetadataDocument, FredSourceError};

pub(super) fn canonical_observation_payloads(
    source: &SourceMetadata,
    dataset: &FredDataset,
    page: &crate::FredObservationPage,
    page_digest: [u8; 32],
    series_metadata: &FredSeriesMetadataDocument,
    received_at: Timestamp,
    ingested_at: Timestamp,
) -> Result<Vec<CanonicalFredRecord>, FredSourceError> {
    let unit = fred_unit_identifier(series_metadata.series().units())?;
    let series =
        SourceIdentifier::try_from(dataset.series_id()).map_err(|_| FredSourceError::Protocol)?;
    let page_reference =
        PayloadReference::ContentHash(PayloadHash::new(DigestAlgorithm::Sha256, page_digest));
    page.observations()
        .iter()
        .map(|observation| {
            let revision = SourceIdentifier::try_from(format!(
                "{}:{}:{}:{}:{}",
                match dataset.namespace {
                    FredNamespace::Fred => "fred",
                    FredNamespace::Alfred => "alfred",
                },
                dataset.series_id(),
                observation.observation_date(),
                observation.realtime_start(),
                observation.realtime_end(),
            ))
            .map_err(|_| FredSourceError::Protocol)?;
            let availability = ResearchAvailabilityEvidence::local_first_observed(received_at);
            let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
                source_id: source.source_id().clone(),
                instrument_id: None,
                venue_id: None,
                source_identifier: revision.clone(),
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
            let time = ResearchTime::try_new_with_coordinates(
                effective.clone(),
                None,
                RevisionNumber::new(1).map_err(|_| FredSourceError::Protocol)?,
                None,
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
                availability: ExtractionAvailabilityEvidence::LocalFirstObserved {
                    observed_at: received_at,
                },
                revision,
                evidence: ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    digest,
                )),
                payload,
            })
        })
        .collect()
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
    pub(super) availability: ExtractionAvailabilityEvidence,
    pub(super) revision: SourceIdentifier,
    pub(super) evidence: ExactPayloadEvidence,
    pub(super) payload: Bytes,
}
