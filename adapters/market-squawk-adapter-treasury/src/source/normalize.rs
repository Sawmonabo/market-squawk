//! Canonical Treasury macro-observation normalization.

use bytes::Bytes;
use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailabilityEvidence, DataQuality, DigestAlgorithm,
    EvidenceDigest, ExactPayloadEvidence, MacroMissingValue, MacroObservation, PayloadHash,
    PayloadReference, ResearchContext, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime, RevisionNumber,
    SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AvailabilityEvidence as ExtractionAvailabilityEvidence, MAX_EXTRACTION_RECORD_BYTES,
    MAX_EXTRACTION_RECORDS, MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, SourceMetadata,
};
use sha2::{Digest, Sha256};

use crate::{
    AverageInterestRate, FiscalDataPage, TreasuryDailyRatePage, TreasuryRateProfile,
    TreasurySourceError,
};

pub(crate) struct CanonicalTreasuryRecord {
    pub(super) effective: ResearchTemporalCoordinate,
    pub(super) published: Option<ResearchTemporalCoordinate>,
    pub(super) availability: ExtractionAvailabilityEvidence,
    pub(super) revision: SourceIdentifier,
    pub(super) evidence: ExactPayloadEvidence,
    pub(super) payload: Bytes,
    disposition: CanonicalValueDisposition,
}

pub(crate) struct CanonicalRecordAdmission {
    record_count: usize,
    observed_numeric_points: usize,
    explicit_missing_points: usize,
    retained_bytes: u64,
}

impl CanonicalRecordAdmission {
    pub(crate) const fn new() -> Self {
        Self {
            record_count: 0,
            observed_numeric_points: 0,
            explicit_missing_points: 0,
            retained_bytes: 0,
        }
    }

    pub(crate) fn admit(
        &mut self,
        record: CanonicalTreasuryRecord,
    ) -> Result<CanonicalTreasuryRecord, TreasurySourceError> {
        let payload_bytes = record.payload.len();
        if self.record_count == MAX_EXTRACTION_RECORDS
            || payload_bytes > MAX_EXTRACTION_RECORD_BYTES
        {
            return Err(TreasurySourceError::InvalidProtocol);
        }
        let record_bytes = u64::try_from(std::mem::size_of::<CanonicalTreasuryRecord>())
            .ok()
            .and_then(|fixed| {
                fixed.checked_add(u64::try_from(record.revision.as_str().len()).ok()?)
            })
            .and_then(|retained| retained.checked_add(u64::try_from(payload_bytes).ok()?))
            .ok_or(TreasurySourceError::InvalidProtocol)?;
        let retained_bytes = self
            .retained_bytes
            .checked_add(record_bytes)
            .filter(|value| *value <= MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES)
            .ok_or(TreasurySourceError::InvalidProtocol)?;
        let record_count = self
            .record_count
            .checked_add(1)
            .ok_or(TreasurySourceError::InvalidProtocol)?;
        let (observed_numeric_points, explicit_missing_points) = match record.disposition {
            CanonicalValueDisposition::ObservedNumeric => (
                self.observed_numeric_points
                    .checked_add(1)
                    .ok_or(TreasurySourceError::InvalidProtocol)?,
                self.explicit_missing_points,
            ),
            CanonicalValueDisposition::ExplicitMissing => (
                self.observed_numeric_points,
                self.explicit_missing_points
                    .checked_add(1)
                    .ok_or(TreasurySourceError::InvalidProtocol)?,
            ),
        };
        if observed_numeric_points.checked_add(explicit_missing_points) != Some(record_count) {
            return Err(TreasurySourceError::InvalidProtocol);
        }
        self.record_count = record_count;
        self.observed_numeric_points = observed_numeric_points;
        self.explicit_missing_points = explicit_missing_points;
        self.retained_bytes = retained_bytes;
        Ok(record)
    }

    pub(crate) const fn record_count(&self) -> usize {
        self.record_count
    }

    pub(crate) const fn observed_numeric_points(&self) -> usize {
        self.observed_numeric_points
    }

    pub(crate) const fn explicit_missing_points(&self) -> usize {
        self.explicit_missing_points
    }
}

pub(crate) fn canonical_fiscal_records<'a>(
    source: &'a SourceMetadata,
    page: &'a FiscalDataPage,
    received_at: Timestamp,
    ingested_at: Timestamp,
) -> impl Iterator<Item = Result<CanonicalTreasuryRecord, TreasurySourceError>> + 'a {
    let profile = TreasuryRateProfile::average_interest_rates_v2();
    page.records().iter().map(move |record| {
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
            CanonicalMacroValue::Observed(rate.rate_percent()),
            revision,
            rate.record_date(),
            page.response_payload_digest(),
            None,
            None,
            received_at,
            ingested_at,
        )
    })
}

pub(crate) fn canonical_daily_rate_records<'a>(
    source: &'a SourceMetadata,
    page: &'a TreasuryDailyRatePage,
    received_at: Timestamp,
    ingested_at: Timestamp,
) -> impl Iterator<Item = Result<CanonicalTreasuryRecord, TreasurySourceError>> + 'a {
    let chronology_error = (page.feed_published_at() > received_at || ingested_at < received_at)
        .then_some(Err(TreasurySourceError::InvalidProtocol));
    chronology_error.into_iter().chain(
        page.observations()
            .iter()
            .flat_map(|observation| {
                observation
                    .metric_points()
                    .map(move |(metric, point)| (observation, metric, point))
            })
            .map(move |(observation, metric, point)| {
                let series = identifier(metric.canonical_series())?;
                let revision = identifier(format!(
                    "treasury-daily-rate:{}:{}:{}:{}:{}",
                    observation.family().dataset_family_token(),
                    observation.record_date(),
                    metric.as_series_token(),
                    observation.source_published_at().unix_nanos(),
                    lower_hex(observation.row_identity()),
                ))?;
                let value = match point.rate_percent() {
                    Some(value) => CanonicalMacroValue::Observed(value),
                    None => CanonicalMacroValue::Missing(MacroMissingValue::new(
                        identifier(
                            point
                                .missing_marker()
                                .ok_or(TreasurySourceError::InvalidProtocol)?,
                        )?,
                        observation
                            .market_unavailability_reason()
                            .map(|reason| identifier(encode_component(reason)))
                            .transpose()?,
                    )),
                };
                canonical_record(
                    source,
                    DataQuality::OfficialDelayed,
                    series,
                    value,
                    revision,
                    observation.record_date(),
                    page.response_payload_digest(),
                    Some(observation.source_published_at()),
                    Some(page.feed_published_at()),
                    received_at,
                    ingested_at,
                )
            }),
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "canonical lineage and point-in-time dimensions remain explicit"
)]
fn canonical_record(
    source: &SourceMetadata,
    quality: DataQuality,
    series: SourceIdentifier,
    value: CanonicalMacroValue,
    revision: SourceIdentifier,
    effective_at: market_squawk_domain::CalendarDate,
    page_digest: [u8; 32],
    published_at: Option<Timestamp>,
    provider_publication_ceiling: Option<Timestamp>,
    received_at: Timestamp,
    ingested_at: Timestamp,
) -> Result<CanonicalTreasuryRecord, TreasurySourceError> {
    if ingested_at < received_at {
        return Err(TreasurySourceError::InvalidProtocol);
    }
    match (published_at, provider_publication_ceiling) {
        (Some(published_at), Some(ceiling))
            if published_at <= ceiling && ceiling <= received_at => {}
        (None, None) => {}
        _ => return Err(TreasurySourceError::InvalidProtocol),
    }
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
    let (observation, disposition) = match value {
        CanonicalMacroValue::Observed(value) => (
            MacroObservation::new(context, series, value, unit),
            CanonicalValueDisposition::ObservedNumeric,
        ),
        CanonicalMacroValue::Missing(missing) => (
            MacroObservation::missing(context, series, missing, unit),
            CanonicalValueDisposition::ExplicitMissing,
        ),
    };
    let payload = serde_json::to_vec(&ResearchObservation::Macro(observation))
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
        disposition,
    })
}

enum CanonicalMacroValue {
    Observed(rust_decimal::Decimal),
    Missing(MacroMissingValue),
}

#[derive(Clone, Copy)]
enum CanonicalValueDisposition {
    ObservedNumeric,
    ExplicitMissing,
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
