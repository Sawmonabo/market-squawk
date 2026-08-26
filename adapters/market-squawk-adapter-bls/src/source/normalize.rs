//! Canonical BLS macro-observation normalization.

use std::num::NonZeroU16;

use bytes::Bytes;
use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailabilityEvidence, DataQuality, DigestAlgorithm,
    EvidenceDigest, ExactPayloadEvidence, MacroMissingValue, MacroObservation, PayloadHash,
    PayloadReference, ResearchContext, ResearchObservation, ResearchPeriod, ResearchProvenance,
    ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime, RevisionNumber,
    SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AvailabilityEvidence as ExtractionAvailabilityEvidence, SourceMetadata,
};
use sha2::{Digest, Sha256};

use crate::observations::period_parts;
use crate::{BlsResponse, BlsSeriesMetadata, BlsSourceConfig, BlsSourceError};

#[derive(Clone, Debug)]
pub(super) struct CanonicalBlsRecord {
    pub(super) effective: ResearchTemporalCoordinate,
    pub(super) availability: ExtractionAvailabilityEvidence,
    pub(super) revision: SourceIdentifier,
    pub(super) evidence: ExactPayloadEvidence,
    pub(super) payload: Bytes,
}

pub(super) fn canonical_records(
    source: &SourceMetadata,
    config: &BlsSourceConfig,
    response: &BlsResponse,
    exact_source_payload: &[u8],
    first_observed_at: Timestamp,
    response_received_at: Timestamp,
    ingested_at: Timestamp,
) -> Result<Vec<CanonicalBlsRecord>, BlsSourceError> {
    if first_observed_at > response_received_at || response_received_at > ingested_at {
        return Err(BlsSourceError::Protocol);
    }
    let source_digest: [u8; 32] = Sha256::digest(exact_source_payload).into();
    let payload_reference =
        PayloadReference::ContentHash(PayloadHash::new(DigestAlgorithm::Sha256, source_digest));
    response
        .series()
        .iter()
        .flat_map(|series| {
            let payload_reference = &payload_reference;
            series.observations().iter().map(move |observation| {
                let metadata = config
                    .series_metadata(series.series_id())
                    .ok_or(BlsSourceError::InvalidSeriesMetadata)?;
                canonical_record(
                    source,
                    metadata,
                    series.series_id(),
                    observation,
                    payload_reference,
                    first_observed_at,
                    response_received_at,
                    ingested_at,
                    source_digest,
                )
            })
        })
        .collect()
}

#[allow(
    clippy::too_many_arguments,
    reason = "canonical provenance, source semantics, and local availability remain explicit"
)]
fn canonical_record(
    source: &SourceMetadata,
    metadata: &BlsSeriesMetadata,
    series_id: &str,
    observation: &crate::BlsObservation,
    payload_reference: &PayloadReference,
    first_observed_at: Timestamp,
    response_received_at: Timestamp,
    ingested_at: Timestamp,
    source_digest: [u8; 32],
) -> Result<CanonicalBlsRecord, BlsSourceError> {
    let (scheme, ordinal, frequency) =
        period_parts(observation.period()).ok_or(BlsSourceError::Protocol)?;
    if metadata.frequency().as_str() != frequency {
        return Err(BlsSourceError::InvalidSeriesMetadata);
    }
    let effective = ResearchTemporalCoordinate::source_period(
        ResearchPeriod::try_new(
            identifier(scheme)?,
            observation.year(),
            NonZeroU16::new(ordinal).ok_or(BlsSourceError::Protocol)?,
            identifier(observation.period())?,
        )
        .map_err(|_| BlsSourceError::Protocol)?,
    );
    let revision = identifier(format!(
        "bls:{series_id}:{}:{}:{}",
        observation.year(),
        observation.period(),
        lower_hex(source_digest),
    ))?;
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: source.source_id().clone(),
        instrument_id: None,
        venue_id: None,
        source_identifier: revision.clone(),
        source_timestamp: None,
        received_at: response_received_at,
        ingested_at,
        quality: DataQuality::OfficialDelayed,
        payload_reference: payload_reference.clone(),
        availability: ResearchAvailabilityEvidence::local_first_observed(first_observed_at),
    })
    .map_err(|_| BlsSourceError::Protocol)?;
    let time = ResearchTime::try_new_with_coordinates(
        effective.clone(),
        None,
        RevisionNumber::new(1).map_err(|_| BlsSourceError::Protocol)?,
        None,
    )
    .map_err(|_| BlsSourceError::Protocol)?;
    let context = ResearchContext::new(provenance, time).map_err(|_| BlsSourceError::Protocol)?;
    let series = identifier(series_id)?;
    let observation = match observation.value() {
        Some(value) => MacroObservation::new(context, series, value, metadata.unit().clone()),
        None => MacroObservation::missing(
            context,
            series,
            MacroMissingValue::new(identifier(observation.raw_value())?, None),
            metadata.unit().clone(),
        ),
    };
    let payload = serde_json::to_vec(&ResearchObservation::Macro(observation))
        .map(Bytes::from)
        .map_err(|_| BlsSourceError::Protocol)?;
    let canonical_digest: [u8; 32] = Sha256::digest(&payload).into();
    Ok(CanonicalBlsRecord {
        effective,
        availability: ExtractionAvailabilityEvidence::LocalFirstObserved {
            observed_at: first_observed_at,
        },
        revision,
        evidence: ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            canonical_digest,
        )),
        payload,
    })
}

fn identifier(value: impl AsRef<str>) -> Result<SourceIdentifier, BlsSourceError> {
    SourceIdentifier::try_from(value.as_ref()).map_err(|_| BlsSourceError::Protocol)
}

fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
