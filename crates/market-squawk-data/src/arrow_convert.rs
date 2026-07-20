//! Exact canonical-observation conversion to and from Arrow.

use std::sync::Arc;

use arrow::array::{
    Array as _, ArrayRef, BinaryArray, Decimal128Array, StringArray, TimestampNanosecondArray,
    UInt8Array, UInt16Array, UInt32Array,
};
use arrow::compute::concat_batches;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
    ResearchContext, ResearchObservation, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AvailabilityEvidence as SourceAvailabilityEvidence, DiscoveryRequestId, ExtractionBatch,
    ExtractionRequestId, payload_matches_exact_evidence,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Error as JsonError;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::schema::{
    DATASET_KEY, REQUEST_DIGEST_KEY, RESEARCH_RECORD_SCHEMA, SCHEMA_VERSION_KEY, decode_hex,
    research_schema,
};

/// A request- and dataset-bound canonical Arrow record batch.
#[derive(Clone, Debug)]
pub struct ResearchArrowBatch {
    batch: RecordBatch,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "binding", rename_all = "snake_case")]
enum RowLineage {
    Extraction(Box<ExtractionRowLineage>),
    CanonicalObservation {
        schema_version: u16,
        source_id: SourceId,
        dataset: SourceIdentifier,
        request_digest: EvidenceDigest,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ExtractionRowLineage {
    schema_version: u16,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    discovery_request_id: DiscoveryRequestId,
    extraction_request_id: ExtractionRequestId,
    request_digest: EvidenceDigest,
    object_id: SourceIdentifier,
    object_evidence: ExactPayloadEvidence,
    record_schema: SourceIdentifier,
    record_evidence: ExactPayloadEvidence,
    effective_at: Timestamp,
    published_at: Option<Timestamp>,
    availability: SourceAvailabilityEvidence,
    revision: SourceIdentifier,
    superseded_at: Option<Timestamp>,
}

impl ResearchArrowBatch {
    /// Converts only canonical observations retained by one exact extraction request.
    pub fn try_from_extraction_batch(
        extraction: &ExtractionBatch,
    ) -> Result<Self, ArrowConversionError> {
        if extraction.records().is_empty() {
            return Err(ArrowConversionError::EmptyBatch);
        }
        let request = serde_json::to_vec(extraction.request())?;
        let request_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(request).into());
        let mut observations = Vec::with_capacity(extraction.records().len());
        let mut lineages = Vec::with_capacity(extraction.records().len());
        for record in extraction.records() {
            let observation: ResearchObservation = serde_json::from_slice(record.payload())?;
            let lineage = RowLineage::Extraction(Box::new(ExtractionRowLineage {
                schema_version: 1,
                source_id: record.source_id().clone(),
                metadata_revision: record.metadata_revision().clone(),
                dataset: record.dataset().clone(),
                discovery_request_id: record.discovery_request_id(),
                extraction_request_id: record.extraction_request_id(),
                request_digest,
                object_id: record.object_id().clone(),
                object_evidence: record.object_evidence().clone(),
                record_schema: record.schema().clone(),
                record_evidence: record.evidence().clone(),
                effective_at: record.effective_at(),
                published_at: record.published_at(),
                availability: record.availability().clone(),
                revision: record.revision().clone(),
                superseded_at: record.superseded_at(),
            }));
            validate_row_lineage(
                &lineage,
                extraction.request().object().dataset(),
                request_digest.bytes(),
                &observation,
                record.payload(),
            )?;
            observations.push(observation);
            lineages.push(lineage);
        }
        let request_digests = vec![request_digest.bytes(); observations.len()];
        Self::try_from_observations_with_requests(
            extraction.request().object().dataset().clone(),
            request_digest,
            request_digests,
            lineages,
            observations,
        )
    }

    /// Converts validated canonical observations without floating-point or decimal rescaling.
    pub fn try_from_observations(
        dataset: SourceIdentifier,
        request_digest: EvidenceDigest,
        observations: Vec<ResearchObservation>,
    ) -> Result<Self, ArrowConversionError> {
        let request_digests = vec![request_digest.bytes(); observations.len()];
        let lineages = observations
            .iter()
            .map(|observation| RowLineage::CanonicalObservation {
                schema_version: 1,
                source_id: observation_context(observation)
                    .provenance()
                    .source_id()
                    .clone(),
                dataset: dataset.clone(),
                request_digest,
            })
            .collect();
        Self::try_from_observations_with_requests(
            dataset,
            request_digest,
            request_digests,
            lineages,
            observations,
        )
    }

    /// Rewrites already validated objects into one compaction batch while retaining each row's
    /// exact extraction-request identity.
    pub(crate) fn try_from_compaction_batches(
        dataset: SourceIdentifier,
        compaction_digest: EvidenceDigest,
        batches: Vec<RecordBatch>,
    ) -> Result<Self, ArrowConversionError> {
        if batches.is_empty() {
            return Err(ArrowConversionError::EmptyBatch);
        }
        let target_schema = research_schema(&dataset, compaction_digest);
        let mut normalized = Vec::with_capacity(batches.len());
        for batch in batches {
            let validated = Self::try_from_record_batch(batch)?;
            let retained_dataset = validated
                .batch
                .schema()
                .metadata()
                .get(DATASET_KEY)
                .cloned()
                .ok_or(ArrowConversionError::InvalidSchemaMetadata)?;
            if retained_dataset != dataset.as_str() {
                return Err(ArrowConversionError::ExtractionBindingMismatch);
            }
            normalized.push(RecordBatch::try_new(
                Arc::clone(&target_schema),
                validated.batch.columns().to_vec(),
            )?);
        }
        let compacted = concat_batches(&target_schema, &normalized)?;
        Self::try_from_record_batch(compacted)
    }

    fn try_from_observations_with_requests(
        dataset: SourceIdentifier,
        batch_digest: EvidenceDigest,
        request_digests: Vec<[u8; 32]>,
        row_lineages: Vec<RowLineage>,
        observations: Vec<ResearchObservation>,
    ) -> Result<Self, ArrowConversionError> {
        if !matches!(batch_digest.algorithm(), DigestAlgorithm::Sha256) {
            return Err(ArrowConversionError::RequestDigestNotSha256);
        }
        if request_digests.len() != observations.len() || row_lineages.len() != observations.len() {
            return Err(ArrowConversionError::InvalidSchema);
        }
        for ((lineage, observation), request_digest) in
            row_lineages.iter().zip(&observations).zip(&request_digests)
        {
            let payload = serde_json::to_vec(observation)?;
            validate_row_lineage(lineage, &dataset, *request_digest, observation, &payload)?;
        }
        let row_lineages = row_lineages
            .iter()
            .map(serde_json::to_vec)
            .collect::<Result<Vec<_>, _>>()?;
        let mut payloads = Vec::with_capacity(observations.len());
        let mut payload_digests = Vec::with_capacity(observations.len());
        let mut kinds = Vec::with_capacity(observations.len());
        let mut source_ids = Vec::with_capacity(observations.len());
        let mut instrument_ids = Vec::with_capacity(observations.len());
        let mut venue_ids = Vec::with_capacity(observations.len());
        let mut source_identifiers = Vec::with_capacity(observations.len());
        let mut source_timestamps = Vec::with_capacity(observations.len());
        let mut received_at = Vec::with_capacity(observations.len());
        let mut available_at = Vec::with_capacity(observations.len());
        let mut ingested_at = Vec::with_capacity(observations.len());
        let mut effective_at = Vec::with_capacity(observations.len());
        let mut published_at = Vec::with_capacity(observations.len());
        let mut revisions = Vec::with_capacity(observations.len());
        let mut superseded_at = Vec::with_capacity(observations.len());
        let mut qualities = Vec::with_capacity(observations.len());
        let mut mantissas = Vec::with_capacity(observations.len());
        let mut scales = Vec::with_capacity(observations.len());
        let mut units = Vec::with_capacity(observations.len());
        let mut currencies = Vec::with_capacity(observations.len());

        for observation in &observations {
            let payload = serde_json::to_vec(observation)?;
            payload_digests.push(Sha256::digest(&payload).to_vec());
            payloads.push(payload);
            let context = observation_context(observation);
            let provenance = context.provenance();
            let time = context.time();
            kinds.push(observation_kind(observation));
            source_ids.push(provenance.source_id().as_str().to_owned());
            instrument_ids.push(provenance.instrument_id().map(|id| id.to_string()));
            venue_ids.push(provenance.venue_id().map(|id| id.as_str().to_owned()));
            source_identifiers.push(provenance.source_identifier().as_str().to_owned());
            source_timestamps.push(
                provenance
                    .source_timestamp()
                    .map(|value| value.unix_nanos()),
            );
            received_at.push(provenance.received_at().unix_nanos());
            available_at.push(
                provenance
                    .availability()
                    .reported_at()
                    .map(|value| value.unix_nanos()),
            );
            ingested_at.push(provenance.ingested_at().unix_nanos());
            effective_at.push(time.effective_at().unix_nanos());
            published_at.push(time.published_at().map(|value| value.unix_nanos()));
            revisions.push(time.revision().get());
            superseded_at.push(time.superseded_at().map(|value| value.unix_nanos()));
            qualities.push(quality_name(provenance.quality()));
            let (decimal, unit) = analytical_value(observation);
            mantissas.push(decimal.map(|value| value.mantissa()));
            scales.push(
                decimal
                    .map(|value| u8::try_from(value.scale()))
                    .transpose()?,
            );
            units.push(unit.map(|value| value.as_str().to_owned()));
            currencies.push(
                unit.filter(|value| is_currency(value.as_str()))
                    .map(|value| value.as_str().to_owned()),
            );
        }

        let decimal = Decimal128Array::from(mantissas).with_precision_and_scale(38, 0)?;
        let utc =
            |values: Vec<Option<i64>>| TimestampNanosecondArray::from(values).with_timezone_utc();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from_value(1, observations.len())),
            Arc::new(BinaryArray::from_iter_values(request_digests)),
            Arc::new(BinaryArray::from_iter_values(row_lineages)),
            Arc::new(StringArray::from(kinds)),
            Arc::new(StringArray::from(source_ids)),
            Arc::new(StringArray::from(instrument_ids)),
            Arc::new(StringArray::from(venue_ids)),
            Arc::new(StringArray::from(source_identifiers)),
            Arc::new(utc(source_timestamps)),
            Arc::new(TimestampNanosecondArray::from(received_at).with_timezone_utc()),
            Arc::new(utc(available_at)),
            Arc::new(TimestampNanosecondArray::from(ingested_at).with_timezone_utc()),
            Arc::new(TimestampNanosecondArray::from(effective_at).with_timezone_utc()),
            Arc::new(utc(published_at)),
            Arc::new(UInt32Array::from(revisions)),
            Arc::new(utc(superseded_at)),
            Arc::new(StringArray::from(qualities)),
            Arc::new(decimal),
            Arc::new(UInt8Array::from(scales)),
            Arc::new(StringArray::from(units)),
            Arc::new(StringArray::from(currencies)),
            Arc::new(BinaryArray::from_iter_values(payload_digests)),
            Arc::new(BinaryArray::from_iter_values(payloads)),
        ];
        Ok(Self {
            batch: RecordBatch::try_new(research_schema(&dataset, batch_digest), arrays)?,
        })
    }

    /// Validates a persisted batch against the complete current schema and every projected value.
    pub fn try_from_record_batch(batch: RecordBatch) -> Result<Self, ArrowConversionError> {
        let metadata = batch.schema().metadata().clone();
        let version = metadata
            .get(SCHEMA_VERSION_KEY)
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or(ArrowConversionError::InvalidSchemaMetadata)?;
        if version != 1 {
            return Err(ArrowConversionError::UnsupportedSchemaVersion { found: version });
        }
        let dataset = metadata
            .get(DATASET_KEY)
            .ok_or(ArrowConversionError::InvalidSchemaMetadata)
            .and_then(|value| {
                SourceIdentifier::try_from(value.as_str())
                    .map_err(|_| ArrowConversionError::InvalidSchemaMetadata)
            })?;
        let request_digest = metadata
            .get(REQUEST_DIGEST_KEY)
            .and_then(|value| decode_hex(value))
            .ok_or(ArrowConversionError::InvalidSchemaMetadata)?;
        if batch.schema().fields()
            != research_schema(
                &dataset,
                EvidenceDigest::new(DigestAlgorithm::Sha256, request_digest),
            )
            .fields()
        {
            return Err(ArrowConversionError::InvalidSchema);
        }
        let candidate = Self { batch };
        let observations = candidate.decode_payloads()?;
        let request_digests = candidate.decode_request_digests()?;
        let row_lineages = candidate.decode_row_lineages()?;
        let rebuilt = Self::try_from_observations_with_requests(
            dataset,
            EvidenceDigest::new(DigestAlgorithm::Sha256, request_digest),
            request_digests,
            row_lineages,
            observations,
        )?;
        if rebuilt.batch != candidate.batch {
            return Err(ArrowConversionError::ProjectionMismatch);
        }
        Ok(candidate)
    }

    /// Returns the immutable Arrow batch.
    pub const fn record_batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Reconstructs canonical observations after validating every projected column.
    pub fn observations(&self) -> Result<Vec<ResearchObservation>, ArrowConversionError> {
        Self::try_from_record_batch(self.batch.clone())?.decode_payloads()
    }

    /// Hashes the ordered canonical row identities independently of Parquet layout.
    pub fn lineage_digest(&self) -> Result<EvidenceDigest, ArrowConversionError> {
        let request_digests = self
            .batch
            .column_by_name("request_sha256")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        let lineages = self
            .batch
            .column_by_name("extraction_lineage_json")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        let payload_digests = self
            .batch
            .column_by_name("payload_sha256")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        if request_digests.len() != lineages.len() || lineages.len() != payload_digests.len() {
            return Err(ArrowConversionError::InvalidSchema);
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/research-row-lineage/v2");
        for ((request_digest, lineage), payload_digest) in
            request_digests.iter().zip(lineages).zip(payload_digests)
        {
            let request_digest = request_digest.ok_or(ArrowConversionError::InvalidSchema)?;
            let lineage = lineage.ok_or(ArrowConversionError::InvalidSchema)?;
            let payload_digest = payload_digest.ok_or(ArrowConversionError::InvalidSchema)?;
            if request_digest.len() != 32 || payload_digest.len() != 32 {
                return Err(ArrowConversionError::InvalidSchema);
            }
            let lineage_bytes = u64::try_from(lineage.len())
                .map_err(|_| ArrowConversionError::InvalidSchema)?
                .to_be_bytes();
            hash.update(request_digest);
            hash.update(lineage_bytes);
            hash.update(lineage);
            hash.update(payload_digest);
        }
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            hash.finalize().into(),
        ))
    }

    fn decode_payloads(&self) -> Result<Vec<ResearchObservation>, ArrowConversionError> {
        let payloads = self
            .batch
            .column_by_name("payload_json")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        payloads
            .iter()
            .map(|payload| {
                let payload = payload.ok_or(ArrowConversionError::InvalidSchema)?;
                serde_json::from_slice(payload).map_err(ArrowConversionError::from)
            })
            .collect()
    }

    fn decode_request_digests(&self) -> Result<Vec<[u8; 32]>, ArrowConversionError> {
        let digests = self
            .batch
            .column_by_name("request_sha256")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        digests
            .iter()
            .map(|digest| {
                digest
                    .ok_or(ArrowConversionError::InvalidSchema)?
                    .try_into()
                    .map_err(|_| ArrowConversionError::InvalidSchema)
            })
            .collect()
    }

    fn decode_row_lineages(&self) -> Result<Vec<RowLineage>, ArrowConversionError> {
        let lineages = self
            .batch
            .column_by_name("extraction_lineage_json")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        lineages
            .iter()
            .map(|lineage| {
                let lineage = lineage.ok_or(ArrowConversionError::InvalidSchema)?;
                serde_json::from_slice(lineage).map_err(ArrowConversionError::from)
            })
            .collect()
    }
}

fn validate_row_lineage(
    lineage: &RowLineage,
    dataset: &SourceIdentifier,
    request_digest: [u8; 32],
    observation: &ResearchObservation,
    payload: &[u8],
) -> Result<(), ArrowConversionError> {
    let context = observation_context(observation);
    let provenance = context.provenance();
    let time = context.time();
    let matches = match lineage {
        RowLineage::Extraction(lineage) => {
            lineage.schema_version == 1
                && lineage.source_id == *provenance.source_id()
                && lineage.dataset == *dataset
                && lineage.request_digest.algorithm() == DigestAlgorithm::Sha256
                && lineage.request_digest.bytes() == request_digest
                && lineage.record_schema.as_str() == RESEARCH_RECORD_SCHEMA
                && lineage.effective_at == time.effective_at()
                && lineage.published_at == time.published_at()
                && Some(lineage.availability.available_at())
                    == provenance.availability().reported_at()
                && lineage.superseded_at == time.superseded_at()
                && payload_matches_exact_evidence(payload, &lineage.record_evidence)
        }
        RowLineage::CanonicalObservation {
            schema_version,
            source_id,
            dataset: lineage_dataset,
            request_digest: lineage_request_digest,
        } => {
            *schema_version == 1
                && source_id == provenance.source_id()
                && lineage_dataset == dataset
                && lineage_request_digest.algorithm() == DigestAlgorithm::Sha256
                && lineage_request_digest.bytes() == request_digest
        }
    };
    if matches {
        Ok(())
    } else {
        Err(ArrowConversionError::ExtractionBindingMismatch)
    }
}

/// Exact Arrow conversion failure.
#[derive(Debug, Error)]
pub enum ArrowConversionError {
    /// An ingest cannot publish an empty object.
    #[error("research Arrow batch must contain at least one observation")]
    EmptyBatch,
    /// Canonical provenance or time disagrees with exact extraction lineage.
    #[error("canonical observation does not match its extraction request and record lineage")]
    ExtractionBindingMismatch,
    /// The request binding was not SHA-256.
    #[error("Arrow request binding must use SHA-256")]
    RequestDigestNotSha256,
    /// Schema metadata is missing or malformed.
    #[error("Arrow schema metadata is invalid")]
    InvalidSchemaMetadata,
    /// This reader cannot interpret the retained schema version.
    #[error("unsupported Arrow schema version {found}")]
    UnsupportedSchemaVersion { found: u16 },
    /// Field names, types, nullability, or order do not match the current schema.
    #[error("Arrow fields do not match the current research schema")]
    InvalidSchema,
    /// Canonical payload and analytical projections disagree.
    #[error("Arrow analytical projection does not match its canonical payload")]
    ProjectionMismatch,
    /// A decimal scale cannot be represented in the explicit scale column.
    #[error("decimal scale is outside the supported exact range")]
    DecimalScale(#[from] std::num::TryFromIntError),
    /// Arrow rejected an array or record-batch invariant.
    #[error("Arrow conversion failed")]
    Arrow(#[from] ArrowError),
    /// Canonical JSON encoding or decoding failed.
    #[error("canonical observation serialization failed")]
    Json(#[from] JsonError),
}

fn observation_context(observation: &ResearchObservation) -> &ResearchContext {
    match observation {
        ResearchObservation::Filing(value) => value.context(),
        ResearchObservation::Fundamental(value) => value.context(),
        ResearchObservation::Macro(value) => value.context(),
        ResearchObservation::PortfolioPosition(value) => value.context(),
        ResearchObservation::Transaction(value) => value.context(),
        ResearchObservation::CorporateAction(value) => value.context(),
        ResearchObservation::AlternativeData(value) => value.context(),
    }
}

const fn observation_kind(observation: &ResearchObservation) -> &'static str {
    match observation {
        ResearchObservation::Filing(_) => "filing",
        ResearchObservation::Fundamental(_) => "fundamental",
        ResearchObservation::Macro(_) => "macro",
        ResearchObservation::PortfolioPosition(_) => "portfolio_position",
        ResearchObservation::Transaction(_) => "transaction",
        ResearchObservation::CorporateAction(_) => "corporate_action",
        ResearchObservation::AlternativeData(_) => "alternative_data",
    }
}

fn analytical_value(
    observation: &ResearchObservation,
) -> (Option<Decimal>, Option<&SourceIdentifier>) {
    match observation {
        ResearchObservation::Fundamental(value) => (Some(value.value()), Some(value.unit())),
        ResearchObservation::Macro(value) => (Some(value.value()), Some(value.unit())),
        ResearchObservation::AlternativeData(value) => (Some(value.value()), value.unit()),
        _ => (None, None),
    }
}

const fn quality_name(quality: DataQuality) -> &'static str {
    match quality {
        DataQuality::DirectVerified => "direct_verified",
        DataQuality::DirectUnverified => "direct_unverified",
        DataQuality::OfficialDelayed => "official_delayed",
        DataQuality::Aggregated => "aggregated",
        DataQuality::Indicative => "indicative",
        DataQuality::Modeled => "modeled",
        DataQuality::Estimated => "estimated",
        DataQuality::Stale => "stale",
        DataQuality::Quarantined => "quarantined",
    }
}

fn is_currency(unit: &str) -> bool {
    unit.len() == 3 && unit.bytes().all(|byte| byte.is_ascii_uppercase())
}
