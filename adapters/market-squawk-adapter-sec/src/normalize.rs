//! Conservative point-in-time normalization of SEC Company Facts.

use std::collections::BTreeMap;

use market_squawk_domain::{
    DataQuality, FilingObservation, FundamentalObservation, PayloadHash, PayloadReference,
    ProviderIdentityRegistry, ProviderInstrumentId, ResearchContext, ResearchObservation,
    ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime,
    RevisionNumber, SourceId, SourceIdentifier, Timestamp,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{RetrievedCompanyFacts, RetrievedSubmissions, SecFiling};

/// Normalizes complete SEC submissions into canonical point-in-time filing observations.
pub fn normalize_filings(
    source_id: &SourceId,
    identities: &ProviderIdentityRegistry,
    retrieved: &RetrievedSubmissions,
    ingested_at: Timestamp,
) -> Result<Vec<ResearchObservation>, SecNormalizationError> {
    normalize_filings_with_cancellation(
        source_id,
        identities,
        retrieved,
        ingested_at,
        &CancellationToken::new(),
    )
}

/// Normalizes complete filings with cooperative observation cancellation.
pub fn normalize_filings_with_cancellation(
    source_id: &SourceId,
    identities: &ProviderIdentityRegistry,
    retrieved: &RetrievedSubmissions,
    ingested_at: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Vec<ResearchObservation>, SecNormalizationError> {
    check_cancelled(cancellation)?;
    let received_at = retrieved.raw().received_at();
    if ingested_at < received_at {
        return Err(SecNormalizationError::IngestedBeforeReceived);
    }
    let provider_id = ProviderInstrumentId::try_from(retrieved.document().cik().as_str())?;
    let instrument_id = identities
        .provider_identity_at(source_id, &provider_id, received_at)
        .ok_or(SecNormalizationError::InstrumentUnresolved)?
        .instrument_id();
    let mut ordered = Vec::new();
    ordered
        .try_reserve(retrieved.document().filings().len())
        .map_err(|_| SecNormalizationError::AllocationFailed)?;
    ordered.extend(retrieved.document().filings().iter());
    ordered.sort_by_key(|filing| {
        (
            filing.report_date().unwrap_or(filing.filed_on()),
            filing.filed_on(),
            filing.accession().as_str().to_owned(),
        )
    });
    let mut family_revisions = BTreeMap::<(String, String), u32>::new();
    let mut observations = Vec::new();
    observations
        .try_reserve(ordered.len())
        .map_err(|_| SecNormalizationError::AllocationFailed)?;
    for filing in ordered {
        check_cancelled(cancellation)?;
        if filing
            .accepted_at()
            .is_some_and(|published_at| published_at > ingested_at)
        {
            return Err(SecNormalizationError::PublicationAfterIngestion);
        }
        let family = filing_family(filing);
        let revision = family_revisions.entry(family.clone()).or_insert(0);
        *revision = revision
            .checked_add(1)
            .ok_or(SecNormalizationError::RevisionOverflow)?;
        let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: source_id.clone(),
            instrument_id: Some(instrument_id),
            venue_id: None,
            source_identifier: filing.accession().clone(),
            source_timestamp: filing.accepted_at(),
            received_at,
            ingested_at,
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                retrieved.raw().evidence().algorithm(),
                retrieved.raw().evidence().bytes(),
            )),
            availability: retrieved.raw().availability().clone(),
        })?;
        let effective_date = filing.report_date().unwrap_or(filing.filed_on());
        let time = ResearchTime::try_new_with_coordinates(
            ResearchTemporalCoordinate::calendar_date(effective_date),
            filing.accepted_at().map(ResearchTemporalCoordinate::exact),
            RevisionNumber::new(*revision)?,
            None,
        )?;
        observations.push(ResearchObservation::Filing(FilingObservation::new(
            ResearchContext::new(provenance, time)?,
            filing.form().clone(),
            filing.accession().clone(),
        )?));
    }
    Ok(observations)
}

fn filing_family(filing: &SecFiling) -> (String, String) {
    (
        filing
            .form()
            .as_str()
            .strip_suffix("/A")
            .unwrap_or(filing.form().as_str())
            .to_owned(),
        filing
            .report_date()
            .unwrap_or(filing.filed_on())
            .to_string(),
    )
}

/// Normalizes every numeric Company Facts occurrence with conservative availability semantics.
///
/// SEC acceptance and filing dates are retained by their source records but are not silently
/// promoted to first-public-availability evidence. The raw response's local first-observed time is
/// therefore the default point-in-time cutoff for online retrievals, while offline imports remain
/// explicitly unknown. Amendments and later occurrences for the same concept/unit/period receive
/// increasing revision numbers and are never overwritten.
pub fn normalize_company_facts(
    source_id: &SourceId,
    identities: &ProviderIdentityRegistry,
    retrieved: &RetrievedCompanyFacts,
    ingested_at: Timestamp,
) -> Result<Vec<ResearchObservation>, SecNormalizationError> {
    normalize_company_facts_with_cancellation(
        source_id,
        identities,
        retrieved,
        ingested_at,
        &CancellationToken::new(),
    )
}

/// Normalizes Company Facts with cooperative occurrence cancellation.
pub fn normalize_company_facts_with_cancellation(
    source_id: &SourceId,
    identities: &ProviderIdentityRegistry,
    retrieved: &RetrievedCompanyFacts,
    ingested_at: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Vec<ResearchObservation>, SecNormalizationError> {
    check_cancelled(cancellation)?;
    let received_at = retrieved.raw().received_at();
    if ingested_at < received_at {
        return Err(SecNormalizationError::IngestedBeforeReceived);
    }
    let provider_id = ProviderInstrumentId::try_from(retrieved.document().cik().as_str())?;
    let instrument_id = identities
        .provider_identity_at(source_id, &provider_id, received_at)
        .ok_or(SecNormalizationError::InstrumentUnresolved)?
        .instrument_id();
    let mut ordered = Vec::new();
    ordered
        .try_reserve(retrieved.document().occurrences().len())
        .map_err(|_| SecNormalizationError::AllocationFailed)?;
    ordered.extend(retrieved.document().occurrences().iter());
    ordered.sort_by_key(|occurrence| {
        (
            occurrence.filed_on(),
            occurrence.accession().as_str().to_owned(),
            occurrence.concept().as_str().to_owned(),
            occurrence.unit().as_str().to_owned(),
            occurrence.period().start(),
            occurrence.period().end(),
        )
    });
    let mut revisions: BTreeMap<(String, String, Option<String>, String), u32> = BTreeMap::new();
    let mut observations = Vec::new();
    observations
        .try_reserve(ordered.len())
        .map_err(|_| SecNormalizationError::AllocationFailed)?;
    for occurrence in ordered {
        check_cancelled(cancellation)?;
        let start = occurrence.period().start().map(|date| date.to_string());
        let end = occurrence.period().end().to_string();
        let key = (
            occurrence.concept().as_str().to_owned(),
            occurrence.unit().as_str().to_owned(),
            start.clone(),
            end.clone(),
        );
        let revision = revisions.entry(key).or_insert(0);
        *revision = revision
            .checked_add(1)
            .ok_or(SecNormalizationError::RevisionOverflow)?;
        let source_identifier = SourceIdentifier::try_from(format!(
            "{}:{}:{}:{}:{}",
            occurrence.accession(),
            occurrence.concept(),
            occurrence.unit(),
            start.as_deref().unwrap_or("instant"),
            end,
        ))?;
        let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: source_id.clone(),
            instrument_id: Some(instrument_id),
            venue_id: None,
            source_identifier,
            source_timestamp: None,
            received_at,
            ingested_at,
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                retrieved.raw().evidence().algorithm(),
                retrieved.raw().evidence().bytes(),
            )),
            availability: retrieved.raw().availability().clone(),
        })?;
        let research_time = ResearchTime::try_new_with_coordinates(
            ResearchTemporalCoordinate::calendar_date(occurrence.period().end()),
            None,
            RevisionNumber::new(*revision)?,
            None,
        )?;
        observations.push(ResearchObservation::Fundamental(
            FundamentalObservation::new(
                ResearchContext::new(provenance, research_time)?,
                occurrence.concept().clone(),
                occurrence.value(),
                occurrence.unit().clone(),
            )?,
        ));
    }
    Ok(observations)
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<(), SecNormalizationError> {
    if cancellation.is_cancelled() {
        Err(SecNormalizationError::Cancelled)
    } else {
        Ok(())
    }
}

/// SEC Company Facts normalization failure.
#[derive(Debug, Error)]
pub enum SecNormalizationError {
    #[error("SEC canonical normalization was cancelled")]
    Cancelled,
    #[error("Company Facts instrument identity is unresolved or quarantined")]
    InstrumentUnresolved,
    #[error("ingestion time precedes local receipt")]
    IngestedBeforeReceived,
    #[error("Company Facts revision counter overflow")]
    RevisionOverflow,
    #[error("SEC canonical normalization bounded allocation failed")]
    AllocationFailed,
    #[error("SEC publication time is later than local ingestion")]
    PublicationAfterIngestion,
    #[error(transparent)]
    Identity(#[from] market_squawk_domain::IdentityError),
    #[error(transparent)]
    Provenance(#[from] market_squawk_domain::ProvenanceError),
    #[error(transparent)]
    Research(#[from] market_squawk_domain::ResearchError),
}
