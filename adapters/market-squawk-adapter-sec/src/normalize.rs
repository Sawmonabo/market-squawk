//! Conservative point-in-time normalization of SEC Company Facts.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use market_squawk_domain::{
    DataQuality, FilingObservation, FundamentalAmendmentStatus, FundamentalCadence,
    FundamentalConsolidation, FundamentalDimensionContext, FundamentalFactContext,
    FundamentalFactContextInput, FundamentalObservation, FundamentalPeriod,
    FundamentalRestatementStatus, FundamentalRevisionOrder, PayloadHash, PayloadReference,
    ProviderIdentityRegistry, ProviderInstrumentId, ResearchContext, ResearchObservation,
    ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime,
    RevisionNumber, SchemaVersion, SourceId, SourceIdentifier, Timestamp,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{CompanyFactOccurrence, RetrievedCompanyFacts, RetrievedSubmissions, SecFiling};

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
        let published = filing
            .accepted_at()
            .map(ResearchTemporalCoordinate::exact)
            .unwrap_or_else(|| ResearchTemporalCoordinate::calendar_date(filing.filed_on()));
        let time = ResearchTime::try_new_with_coordinates(
            ResearchTemporalCoordinate::calendar_date(effective_date),
            Some(published),
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
/// promoted to first-public-availability evidence. The raw response's first local observation is
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
    ordered.sort_unstable_by(|left, right| compare_company_facts(left, right));
    let mut observations = Vec::new();
    observations
        .try_reserve(ordered.len())
        .map_err(|_| SecNormalizationError::AllocationFailed)?;
    let revision_ruleset = SourceIdentifier::try_from("sec-companyfacts-revision-order-v1")?;
    let mut previous_family: Option<&CompanyFactOccurrence> = None;
    let mut family_revision = 0_u32;
    for occurrence in ordered {
        check_cancelled(cancellation)?;
        if previous_family.is_some_and(|previous| same_company_fact_family(previous, occurrence)) {
            family_revision = family_revision
                .checked_add(1)
                .ok_or(SecNormalizationError::RevisionOverflow)?;
        } else {
            family_revision = 1;
        }
        previous_family = Some(occurrence);
        let start = occurrence.period().start().map(|date| date.to_string());
        let end = occurrence.period().end().to_string();
        let revision = RevisionNumber::new(family_revision)?;
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
            Some(ResearchTemporalCoordinate::calendar_date(
                occurrence.filed_on(),
            )),
            revision,
            None,
        )?;
        let period = match occurrence.period().start() {
            Some(start) => FundamentalPeriod::duration(start, occurrence.period().end())?,
            None => FundamentalPeriod::instant(occurrence.period().end()),
        };
        let fact_context = FundamentalFactContext::try_new(FundamentalFactContextInput {
            schema_version: SchemaVersion::CURRENT,
            period,
            unit: occurrence.unit().clone(),
            accession: occurrence.accession().clone(),
            filing_form: Some(occurrence.form().clone()),
            amendment_status: amendment_status(occurrence.form()),
            filed_on: Some(occurrence.filed_on()),
            frame: occurrence.frame().cloned(),
            fiscal_year: occurrence.fiscal_year(),
            fiscal_period: occurrence.fiscal_period().cloned(),
            cadence: company_facts_cadence(occurrence.fiscal_period()),
            xbrl_context_id: None,
            dimensions: FundamentalDimensionContext::unavailable(),
            consolidation: FundamentalConsolidation::Unavailable,
            revision_order: FundamentalRevisionOrder::new(revision, revision_ruleset.clone()),
            restatement_status: FundamentalRestatementStatus::Unavailable,
        })?;
        observations.push(ResearchObservation::Fundamental(
            FundamentalObservation::new(
                ResearchContext::new(provenance, research_time)?,
                occurrence.concept().clone(),
                occurrence.value(),
                fact_context,
            )?,
        ));
    }
    Ok(observations)
}

fn compare_company_facts(left: &CompanyFactOccurrence, right: &CompanyFactOccurrence) -> Ordering {
    left.concept()
        .cmp(right.concept())
        .then_with(|| left.unit().cmp(right.unit()))
        .then_with(|| left.period().start().cmp(&right.period().start()))
        .then_with(|| left.period().end().cmp(&right.period().end()))
        .then_with(|| left.filed_on().cmp(&right.filed_on()))
        .then_with(|| left.accession().cmp(right.accession()))
        .then_with(|| left.form().cmp(right.form()))
        .then_with(|| left.frame().cmp(&right.frame()))
        .then_with(|| left.fiscal_year().cmp(&right.fiscal_year()))
        .then_with(|| left.fiscal_period().cmp(&right.fiscal_period()))
        .then_with(|| left.value().cmp(&right.value()))
}

fn same_company_fact_family(left: &CompanyFactOccurrence, right: &CompanyFactOccurrence) -> bool {
    left.concept() == right.concept()
        && left.unit() == right.unit()
        && left.period() == right.period()
}

fn amendment_status(form: &SourceIdentifier) -> FundamentalAmendmentStatus {
    if form.as_str().ends_with("/A") {
        FundamentalAmendmentStatus::Amendment
    } else {
        FundamentalAmendmentStatus::Original
    }
}

fn company_facts_cadence(period: Option<&SourceIdentifier>) -> FundamentalCadence {
    match period.map(SourceIdentifier::as_str) {
        None => FundamentalCadence::Unavailable,
        Some("FY" | "CY") => FundamentalCadence::Annual,
        Some("Q1" | "Q2" | "Q3" | "Q4") => FundamentalCadence::Quarterly,
        Some(_) => FundamentalCadence::Other,
    }
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
    #[error(transparent)]
    FundamentalContext(#[from] market_squawk_domain::FundamentalContextError),
    #[error("SEC publication time is later than local ingestion")]
    PublicationAfterIngestion,
    #[error(transparent)]
    Identity(#[from] market_squawk_domain::IdentityError),
    #[error(transparent)]
    Provenance(#[from] market_squawk_domain::ProvenanceError),
    #[error(transparent)]
    Research(#[from] market_squawk_domain::ResearchError),
}
