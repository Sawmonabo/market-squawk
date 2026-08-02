//! Canonical Treasury macro-observation normalization.

use bytes::Bytes;
use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailabilityEvidence, DataQuality, DigestAlgorithm,
    EvidenceDigest, ExactPayloadEvidence, MacroObservation, PayloadHash, PayloadReference,
    ResearchContext, ResearchObservation, ResearchProvenance, ResearchProvenanceInput,
    ResearchTemporalCoordinate, ResearchTime, RevisionNumber, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AvailabilityEvidence as ExtractionAvailabilityEvidence, SourceMetadata,
};
use sha2::{Digest, Sha256};

use crate::{
    AverageInterestRate, FiscalDataPage, TreasuryDailyRateMetric, TreasuryDailyRatePage,
    TreasuryRateProfile, TreasurySourceError,
};

pub(super) struct CanonicalTreasuryRecord {
    pub(super) effective: ResearchTemporalCoordinate,
    pub(super) published: Option<ResearchTemporalCoordinate>,
    pub(super) availability: ExtractionAvailabilityEvidence,
    pub(super) revision: SourceIdentifier,
    pub(super) evidence: ExactPayloadEvidence,
    pub(super) payload: Bytes,
}

pub(super) fn canonical_fiscal_records(
    source: &SourceMetadata,
    page: &FiscalDataPage,
    received_at: Timestamp,
    ingested_at: Timestamp,
) -> Result<Vec<CanonicalTreasuryRecord>, TreasurySourceError> {
    let profile = TreasuryRateProfile::average_interest_rates_v2();
    page.records()
        .iter()
        .map(|record| {
            let rate = AverageInterestRate::try_from_record(record, &profile)?;
            let series = identifier(format!(
                "treasury:average-interest-rate:v2:{}:{}",
                encode_component(rate.security_type_description()),
                encode_component(rate.security_description()),
            ))?;
            let revision = identifier(format!(
                "treasury-fiscal-rate:{}:{}:{}",
                rate.record_date(),
                rate.source_line_number(),
                lower_hex(record.row_identity()),
            ))?;
            canonical_record(
                source,
                DataQuality::OfficialDelayed,
                series,
                rate.rate_percent(),
                revision,
                rate.record_date(),
                page.response_payload_digest(),
                None,
                received_at,
                ingested_at,
            )
        })
        .collect()
}

pub(super) fn canonical_daily_rate_records(
    source: &SourceMetadata,
    page: &TreasuryDailyRatePage,
    received_at: Timestamp,
    ingested_at: Timestamp,
) -> Result<Vec<CanonicalTreasuryRecord>, TreasurySourceError> {
    page.observations()
        .iter()
        .flat_map(|observation| {
            observation
                .metric_points()
                .map(move |(metric, point)| (observation, metric, point))
        })
        .map(|(observation, metric, point)| {
            let series = identifier(metric_series(metric))?;
            let revision = identifier(format!(
                "treasury-daily-rate:{}:{}:{}:{}:{}",
                observation.family().dataset_family_token(),
                observation.record_date(),
                metric.as_series_token(),
                observation.source_published_at().unix_nanos(),
                lower_hex(observation.row_identity()),
            ))?;
            canonical_record(
                source,
                DataQuality::OfficialDelayed,
                series,
                point.rate_percent(),
                revision,
                observation.record_date(),
                page.response_payload_digest(),
                Some(observation.source_published_at()),
                received_at,
                ingested_at,
            )
        })
        .collect()
}

fn metric_series(metric: TreasuryDailyRateMetric) -> String {
    match metric {
        TreasuryDailyRateMetric::NominalParYield(maturity) => {
            format!("treasury:daily-par-yield-curve:{}", maturity.as_str())
        }
        TreasuryDailyRateMetric::Bill { maturity, measure } => format!(
            "treasury:daily-bill-rates:{}:{}",
            maturity.as_str(),
            measure.as_str()
        ),
        TreasuryDailyRateMetric::LongTerm(rate_type) => {
            format!("treasury:daily-long-term-rates:{}", rate_type.as_str())
        }
        TreasuryDailyRateMetric::RealParYield(maturity) => {
            format!("treasury:daily-real-par-yield-curve:{}", maturity.as_str())
        }
        TreasuryDailyRateMetric::RealLongTermAverage => {
            "treasury:daily-real-long-term-rates:average".to_owned()
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "canonical lineage and point-in-time dimensions remain explicit"
)]
fn canonical_record(
    source: &SourceMetadata,
    quality: DataQuality,
    series: SourceIdentifier,
    value: rust_decimal::Decimal,
    revision: SourceIdentifier,
    effective_at: market_squawk_domain::CalendarDate,
    page_digest: [u8; 32],
    published_at: Option<Timestamp>,
    received_at: Timestamp,
    ingested_at: Timestamp,
) -> Result<CanonicalTreasuryRecord, TreasurySourceError> {
    let availability = ResearchAvailabilityEvidence::local_first_observed(received_at);
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: source.source_id().clone(),
        instrument_id: None,
        venue_id: None,
        source_identifier: revision.clone(),
        source_timestamp: published_at,
        received_at,
        ingested_at,
        quality,
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            DigestAlgorithm::Sha256,
            page_digest,
        )),
        availability,
    })
    .map_err(|_| TreasurySourceError::InvalidProtocol)?;
    let effective = ResearchTemporalCoordinate::calendar_date(effective_at);
    let time = ResearchTime::try_new_with_coordinates(
        effective.clone(),
        published_at.map(ResearchTemporalCoordinate::exact),
        RevisionNumber::new(1).map_err(|_| TreasurySourceError::InvalidProtocol)?,
        None,
    )
    .map_err(|_| TreasurySourceError::InvalidProtocol)?;
    let context =
        ResearchContext::new(provenance, time).map_err(|_| TreasurySourceError::InvalidProtocol)?;
    let unit = identifier("percent")?;
    let payload = serde_json::to_vec(&ResearchObservation::Macro(MacroObservation::new(
        context, series, value, unit,
    )))
    .map(Bytes::from)
    .map_err(|_| TreasurySourceError::InvalidProtocol)?;
    let digest: [u8; 32] = Sha256::digest(&payload).into();
    Ok(CanonicalTreasuryRecord {
        effective,
        published: published_at.map(ResearchTemporalCoordinate::exact),
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
}

fn identifier(value: impl AsRef<str>) -> Result<SourceIdentifier, TreasurySourceError> {
    SourceIdentifier::try_from(value.as_ref()).map_err(|_| TreasurySourceError::InvalidProtocol)
}

fn encode_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    encoded
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
