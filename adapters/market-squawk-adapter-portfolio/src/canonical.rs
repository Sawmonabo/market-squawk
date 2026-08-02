//! Canonical research provenance and extraction output construction.

use bytes::Bytes;
use market_squawk_domain::{
    AlternativeDataObservation, DataQuality, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence,
    InstrumentId, PayloadHash, PayloadReference, ResearchContext, ResearchObservation,
    ResearchProvenance, ResearchProvenanceInput, ResearchTime, RevisionNumber, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AvailabilityEvidence as SourceAvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA,
    ExtractionBatch, ExtractionBatchAccumulator, ExtractionRecord,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};

use crate::archive::RawPortfolioRecord;
use crate::{PortfolioImportError, PortfolioImportLimits};

pub(crate) struct CanonicalObservation {
    pub(crate) input_index: usize,
    pub(crate) observation: ResearchObservation,
}

#[allow(
    clippy::too_many_arguments,
    reason = "research provenance must remain explicit"
)]
pub(crate) fn research_context(
    record: &ExtractionRecord,
    raw: &RawPortfolioRecord,
    source_identifier: &SourceIdentifier,
    source_id: SourceId,
    quality: DataQuality,
    instrument_id: Option<InstrumentId>,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    ingested_at: Timestamp,
    revision: RevisionNumber,
) -> Result<ResearchContext, PortfolioImportError> {
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id,
        instrument_id,
        venue_id: None,
        source_identifier: source_identifier.clone(),
        source_timestamp,
        received_at,
        ingested_at,
        quality,
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            raw.payload_hash().algorithm(),
            raw.payload_hash().bytes(),
        )),
        availability: domain_availability(record.availability()),
    })
    .map_err(|_| PortfolioImportError::ExtractionContract)?;
    let time = ResearchTime::try_new_with_coordinates(
        record.effective_time().clone(),
        record.published_time().cloned(),
        revision,
        record.superseded_time().cloned(),
    )
    .map_err(|_| PortfolioImportError::ExtractionContract)?;
    ResearchContext::new(provenance, time).map_err(|_| PortfolioImportError::ExtractionContract)
}

fn domain_availability(
    availability: &SourceAvailabilityEvidence,
) -> market_squawk_domain::AvailabilityEvidence {
    match availability {
        SourceAvailabilityEvidence::Observed {
            available_at,
            evidence,
        } => market_squawk_domain::AvailabilityEvidence::evidenced(*available_at, evidence.clone()),
        SourceAvailabilityEvidence::LocalFirstObserved { observed_at } => {
            market_squawk_domain::AvailabilityEvidence::local_first_observed(*observed_at)
        }
        SourceAvailabilityEvidence::Inferred {
            inferred_at,
            method,
        } => market_squawk_domain::AvailabilityEvidence::inferred(*inferred_at, method.clone()),
        SourceAvailabilityEvidence::Unknown => {
            market_squawk_domain::AvailabilityEvidence::unknown()
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "canonical scalar fields stay explicit"
)]
pub(crate) fn push_scalar(
    output: &mut Vec<CanonicalObservation>,
    input_index: usize,
    context: ResearchContext,
    dataset: &str,
    field: &str,
    value: Decimal,
    unit: Option<&str>,
) -> Result<(), PortfolioImportError> {
    let observation = AlternativeDataObservation::new(
        context,
        identifier(dataset)?,
        identifier(field)?,
        value,
        unit.map(identifier).transpose()?,
    );
    output.push(CanonicalObservation {
        input_index,
        observation: ResearchObservation::AlternativeData(observation),
    });
    Ok(())
}

pub(crate) fn build_canonical_batch(
    input: &ExtractionBatch,
    canonical: Vec<CanonicalObservation>,
    limits: PortfolioImportLimits,
) -> Result<ExtractionBatch, PortfolioImportError> {
    if canonical.is_empty() {
        return Err(PortfolioImportError::ExtractionContract);
    }
    if canonical.len() > limits.max_normalized_records {
        return Err(PortfolioImportError::NormalizedRecordLimitExceeded {
            max: limits.max_normalized_records,
        });
    }
    let request_max = usize::try_from(input.request().max_records())
        .map_err(|_| PortfolioImportError::ExtractionContract)?;
    if canonical.len() > request_max {
        return Err(PortfolioImportError::NormalizedRecordLimitExceeded { max: request_max });
    }
    let schema = identifier(CURRENT_RESEARCH_RECORD_SCHEMA)?;
    let mut accumulator = ExtractionBatchAccumulator::try_new(input.request())
        .map_err(|_| PortfolioImportError::ExtractionContract)?;
    for item in canonical {
        let source = input
            .records()
            .get(item.input_index)
            .ok_or(PortfolioImportError::ExtractionContract)?;
        let payload = serde_json::to_vec(&item.observation)
            .map_err(|_| PortfolioImportError::ExtractionContract)?;
        let evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&payload).into(),
        ));
        let output = ExtractionRecord::try_new_with_time(
            input.request(),
            schema.clone(),
            evidence,
            source.effective_time().clone(),
            source.published_time().cloned(),
            source.availability().clone(),
            source.revision().clone(),
            source.superseded_time().cloned(),
            Bytes::from(payload),
        )
        .map_err(|_| PortfolioImportError::ExtractionContract)?;
        accumulator
            .push(output)
            .map_err(|_| PortfolioImportError::ExtractionContract)?;
    }
    accumulator
        .finish()
        .map_err(|_| PortfolioImportError::ExtractionContract)
}

fn identifier(value: &str) -> Result<SourceIdentifier, PortfolioImportError> {
    SourceIdentifier::try_from(value).map_err(|_| PortfolioImportError::InvalidRecord)
}
