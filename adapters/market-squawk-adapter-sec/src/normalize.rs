//! Conservative point-in-time normalization of SEC filing research.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::mem::size_of;

use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest,
    FilingObservation, FundamentalAmendmentStatus, FundamentalCadence, FundamentalConsolidation,
    FundamentalDimensionContext, FundamentalFactContext, FundamentalFactContextInput,
    FundamentalObservation, FundamentalPeriod, FundamentalRestatementStatus,
    FundamentalRevisionOrder, InstrumentId, PayloadHash, PayloadReference,
    ProviderIdentityRegistry, ProviderInstrumentId, ResearchContext, ResearchObservation,
    ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime,
    RevisionNumber, SchemaVersion, SourceId, SourceIdentifier, Timestamp, XbrlPeriod,
};
use market_squawk_sources::{
    ExtractionBatch, ProviderNativeLineageBatch, ProviderNativeLineageBatchBuilder,
    ProviderNativeLineageError, ProviderNativeLineageImplementation,
};
use serde::ser::SerializeSeq as _;
use serde::{Serialize, Serializer};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::product::SecFilingXbrlCoordinates;
use crate::xbrl::SecValidatedXbrlTaxonomySet;
use crate::{
    CompanyFactOccurrence, ParsedXbrlDocument, RetrievedCompanyFacts, RetrievedSubmissions,
    SecFiling, SecResearchDataset, SecResearchDatasetKind, XbrlNonnumericOccurrence,
};

const SEC_XBRL_OCCURRENCE_ORDER_RULESET: &str = "sec-inline-xbrl-occurrence-order-v1";
const SEC_XBRL_SOURCE_RECORD_PREFIX: &str = "sec.xbrl.fact.sha256.";

/// Mandatory typed provider-native handoff for nil and nonnumeric filing occurrences.
#[derive(Debug, Eq, PartialEq)]
pub struct SecXbrlNativeLineage {
    dataset: SourceIdentifier,
    filing: SecFilingXbrlCoordinates,
    taxonomy: SecValidatedXbrlTaxonomySet,
    availability: AvailabilityEvidence,
    received_at: Timestamp,
    ingested_at: Timestamp,
    numeric_fact_count: usize,
    numeric_occurrence_ids: Vec<SourceIdentifier>,
    nonnumeric_occurrences: Vec<XbrlNonnumericOccurrence>,
    total_retained_bytes: u64,
}

impl SecXbrlNativeLineage {
    /// Returns the exact accession/document/taxonomy-bound dataset identity.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns exact filing coordinates retained independently of canonical numeric facts.
    pub const fn filing(&self) -> &SecFilingXbrlCoordinates {
        &self.filing
    }

    /// Returns the same validated taxonomy-set capability used by the strict parser.
    pub const fn taxonomy(&self) -> &SecValidatedXbrlTaxonomySet {
        &self.taxonomy
    }

    /// Returns the conservative availability evidence used by canonical numeric facts.
    pub const fn availability(&self) -> &AvailabilityEvidence {
        &self.availability
    }

    /// Returns when the exact document first reached this process.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns when this typed lineage handoff was produced.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }

    /// Returns every bounded nil or nonnumeric occurrence in exact parser order.
    pub fn nonnumeric_occurrences(&self) -> &[XbrlNonnumericOccurrence] {
        &self.nonnumeric_occurrences
    }

    /// Converts this complete filing handoff into the shared bounded native-lineage contract.
    ///
    /// Each numeric row carries the exact canonical XBRL observation JSON because that canonical
    /// value already contains the complete provider occurrence evidence. The sidecar separately
    /// retains the filing, availability, taxonomy, nil, and nonnumeric occurrence semantics that
    /// cannot be represented as numeric facts.
    pub(crate) fn try_into_provider_native_lineage(
        self,
        batch: &ExtractionBatch,
    ) -> Result<ProviderNativeLineageBatch, ProviderNativeLineageError> {
        if batch.records().len() != self.numeric_fact_count
            || self.numeric_occurrence_ids.len() != self.numeric_fact_count
        {
            return Err(ProviderNativeLineageError::AlignmentMismatch);
        }
        let mut native_lineage = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::SecEdgarV1,
            batch,
        )?;
        native_lineage.try_set_batch_sidecar(&SecFilingXbrlNativeBatchV1 {
            version: 1,
            family: "filing_xbrl",
            dataset: &self.dataset,
            filing: SecFilingXbrlCoordinatesV1::from_filing(&self.filing),
            taxonomy: SecXbrlTaxonomyV1 {
                version: self.taxonomy.version(),
                artifact_set: self.taxonomy.artifact_set(),
                fingerprint: self.taxonomy.fingerprint(),
            },
            nonnumeric_occurrences: SecXbrlNonnumericOccurrencesV1(&self.nonnumeric_occurrences),
        })?;
        for (occurrence_id, record) in self.numeric_occurrence_ids.iter().zip(batch.records()) {
            if filing_fact_source_identifier(&self.dataset, occurrence_id)
                .ok()
                .as_ref()
                != Some(record.revision())
            {
                return Err(ProviderNativeLineageError::AlignmentMismatch);
            }
            native_lineage.try_push(&SecFilingXbrlNativeRowV1 {
                family: "numeric_fact",
                occurrence_id,
            })?;
        }
        native_lineage.finish()
    }

    /// Returns the checked conservative deep-retained size of this complete native handoff.
    pub const fn total_retained_bytes(&self) -> u64 {
        self.total_retained_bytes
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecFilingXbrlNativeBatchV1<'a> {
    version: u16,
    family: &'static str,
    dataset: &'a SourceIdentifier,
    filing: SecFilingXbrlCoordinatesV1<'a>,
    taxonomy: SecXbrlTaxonomyV1<'a>,
    nonnumeric_occurrences: SecXbrlNonnumericOccurrencesV1<'a>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecFilingXbrlCoordinatesV1<'a> {
    cik: &'a str,
    accession: &'a SourceIdentifier,
    document: &'a SourceIdentifier,
    filing_form: &'a SourceIdentifier,
    filed_on: CalendarDate,
    report_date: Option<CalendarDate>,
    filing_size_bytes: Option<u64>,
    is_inline_xbrl: bool,
    accepted_at: Option<Timestamp>,
    acceptance_evidence: Option<&'a SourceIdentifier>,
}

impl<'a> SecFilingXbrlCoordinatesV1<'a> {
    fn from_filing(filing: &'a SecFilingXbrlCoordinates) -> Self {
        Self {
            cik: filing.cik(),
            accession: filing.accession(),
            document: filing.document(),
            filing_form: filing.filing_form(),
            filed_on: filing.filed_on(),
            report_date: filing.report_date(),
            filing_size_bytes: filing.filing_size_bytes(),
            is_inline_xbrl: filing.is_inline_xbrl(),
            accepted_at: filing.acceptance().map(|value| value.accepted_at()),
            acceptance_evidence: filing.acceptance().map(|value| value.evidence()),
        }
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecXbrlTaxonomyV1<'a> {
    version: &'a SourceIdentifier,
    artifact_set: EvidenceDigest,
    fingerprint: EvidenceDigest,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecFilingXbrlNativeRowV1<'a> {
    family: &'static str,
    occurrence_id: &'a SourceIdentifier,
}

struct SecXbrlNonnumericOccurrencesV1<'a>(&'a [XbrlNonnumericOccurrence]);

impl Serialize for SecXbrlNonnumericOccurrencesV1<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for occurrence in self.0 {
            sequence.serialize_element(&SecXbrlNonnumericOccurrenceV1 {
                occurrence_id: occurrence.occurrence_id(),
                accession: occurrence.accession(),
                concept: occurrence.concept(),
                context_id: occurrence.context_id(),
                lexical_value: occurrence.lexical_value(),
                nil: occurrence.is_nil(),
                source_payload: occurrence.source_payload(),
                occurrence_relationships: occurrence.occurrence_relationships(),
            })?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecXbrlNonnumericOccurrenceV1<'a> {
    occurrence_id: &'a SourceIdentifier,
    accession: &'a SourceIdentifier,
    concept: &'a market_squawk_domain::XbrlQualifiedName,
    context_id: &'a SourceIdentifier,
    lexical_value: &'a market_squawk_domain::XbrlText,
    nil: bool,
    source_payload: &'a market_squawk_domain::ExactPayloadEvidence,
    occurrence_relationships: &'a market_squawk_domain::XbrlOccurrenceRelationships,
}

/// Numeric canonical observations paired indivisibly with native nonnumeric lineage.
#[derive(Debug)]
pub(crate) struct SecFilingXbrlNormalization {
    source_id: SourceId,
    instrument_id: InstrumentId,
    dataset: SourceIdentifier,
    filing: SecFilingXbrlCoordinates,
    taxonomy: SecValidatedXbrlTaxonomySet,
    availability: AvailabilityEvidence,
    source_timestamp: Option<Timestamp>,
    payload_digest: EvidenceDigest,
    received_at: Timestamp,
    ingested_at: Timestamp,
    occurrence_ruleset: SourceIdentifier,
    numeric_facts: std::vec::IntoIter<crate::XbrlNumericFact>,
    ordinals: std::vec::IntoIter<FamilyOrdinal>,
    numeric_fact_count: usize,
    numeric_occurrence_ids: Vec<SourceIdentifier>,
    nonnumeric_occurrences: Vec<XbrlNonnumericOccurrence>,
    native_lineage_retained_bytes: u64,
}

impl SecFilingXbrlNormalization {
    /// Produces one canonical numeric fact at a time so parsed and canonical families do not grow
    /// simultaneously as full vectors.
    pub(crate) fn try_next_observation(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<Option<ResearchObservation>, SecNormalizationError> {
        check_cancelled(cancellation)?;
        let Some(fact) = self.numeric_facts.next() else {
            if self.ordinals.next().is_some() {
                return Err(SecNormalizationError::XbrlDocumentBindingMismatch);
            }
            return Ok(None);
        };
        let ordinal = self
            .ordinals
            .next()
            .ok_or(SecNormalizationError::XbrlDocumentBindingMismatch)?;
        let numeric_index = self
            .numeric_fact_count
            .checked_sub(self.numeric_facts.len())
            .and_then(|processed| processed.checked_sub(1))
            .ok_or(SecNormalizationError::XbrlDocumentBindingMismatch)?;
        if self.numeric_occurrence_ids.get(numeric_index) != Some(fact.evidence().occurrence_id()) {
            return Err(SecNormalizationError::XbrlDocumentBindingMismatch);
        }
        let (concept, unit, value, evidence) = fact.into_parts();
        let period = fundamental_period(evidence.period())?;
        let source_identifier =
            filing_fact_source_identifier(&self.dataset, evidence.occurrence_id())?;
        let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: self.source_id.clone(),
            instrument_id: Some(self.instrument_id),
            venue_id: None,
            source_identifier,
            source_timestamp: self.source_timestamp,
            received_at: self.received_at,
            ingested_at: self.ingested_at,
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                self.payload_digest.algorithm(),
                self.payload_digest.bytes(),
            )),
            availability: self.availability.clone(),
        })?;
        let research_time = ResearchTime::try_new_with_coordinates(
            ResearchTemporalCoordinate::calendar_date(period.end()),
            Some(ResearchTemporalCoordinate::calendar_date(
                self.filing.filed_on(),
            )),
            RevisionNumber::new(1)?,
            None,
        )?;
        let fact_context = FundamentalFactContext::try_new(FundamentalFactContextInput {
            schema_version: SchemaVersion::CURRENT,
            period,
            unit,
            accession: self.filing.accession().clone(),
            filing_form: Some(self.filing.filing_form().clone()),
            amendment_status: amendment_status(self.filing.filing_form()),
            filed_on: Some(self.filing.filed_on()),
            frame: None,
            fiscal_year: None,
            fiscal_period: None,
            cadence: FundamentalCadence::Unavailable,
            xbrl_context_id: Some(evidence.context_id().clone()),
            dimensions: FundamentalDimensionContext::try_source_reported(evidence.dimensions())?,
            consolidation: FundamentalConsolidation::Unavailable,
            revision_order: FundamentalRevisionOrder::new(
                RevisionNumber::new(ordinal.ordinal)?,
                self.occurrence_ruleset.clone(),
            ),
            restatement_status: FundamentalRestatementStatus::Unavailable,
        })?;
        Ok(Some(ResearchObservation::Fundamental(
            FundamentalObservation::new_with_xbrl_evidence(
                ResearchContext::new(provenance, research_time)?,
                concept,
                value,
                fact_context,
                evidence,
            )?,
        )))
    }

    pub(crate) const fn native_lineage_retained_bytes(&self) -> u64 {
        self.native_lineage_retained_bytes
    }

    /// Consumes the mandatory native family after every numeric fact was streamed.
    pub(crate) fn into_native_lineage(
        mut self,
    ) -> Result<SecXbrlNativeLineage, SecNormalizationError> {
        if self.numeric_facts.next().is_some() || self.ordinals.next().is_some() {
            return Err(SecNormalizationError::XbrlDocumentBindingMismatch);
        }
        Ok(SecXbrlNativeLineage {
            dataset: self.dataset,
            filing: self.filing,
            taxonomy: self.taxonomy,
            availability: self.availability,
            received_at: self.received_at,
            ingested_at: self.ingested_at,
            numeric_fact_count: self.numeric_fact_count,
            numeric_occurrence_ids: self.numeric_occurrence_ids,
            nonnumeric_occurrences: self.nonnumeric_occurrences,
            total_retained_bytes: self.native_lineage_retained_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FamilyOrdinal {
    original_index: usize,
    ordinal: u32,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum FamilyPeriodKey {
    Instant(CalendarDate),
    Duration(CalendarDate, CalendarDate),
}

/// Maps one exact parsed filing into canonical numeric facts plus mandatory native text lineage.
pub(crate) fn normalize_filing_xbrl_with_cancellation(
    source_id: &SourceId,
    identities: &ProviderIdentityRegistry,
    dataset: SecResearchDataset,
    document: ParsedXbrlDocument,
    payload_digest: EvidenceDigest,
    received_at: Timestamp,
    ingested_at: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecFilingXbrlNormalization, SecNormalizationError> {
    check_cancelled(cancellation)?;
    if dataset.kind() != SecResearchDatasetKind::FilingXbrl {
        return Err(SecNormalizationError::InvalidXbrlDataset);
    }
    let filing = dataset
        .filing_xbrl_coordinates()
        .ok_or(SecNormalizationError::InvalidXbrlDataset)?;
    let taxonomy = dataset
        .xbrl_taxonomy()
        .ok_or(SecNormalizationError::InvalidXbrlDataset)?;
    if payload_digest.algorithm() != DigestAlgorithm::Sha256
        || payload_digest.bytes().iter().all(|byte| *byte == 0)
        || document.evaluated_at() != received_at
        || !document.matches_document_context(
            filing.accession(),
            &provider_cik(filing)?,
            taxonomy,
            payload_digest,
        )
    {
        return Err(SecNormalizationError::XbrlDocumentBindingMismatch);
    }
    if ingested_at < received_at {
        return Err(SecNormalizationError::IngestedBeforeReceived);
    }
    let (source_timestamp, availability) = match filing.acceptance() {
        Some(acceptance) => {
            if acceptance.accepted_at() > received_at {
                return Err(SecNormalizationError::PublicationAfterReceipt);
            }
            (
                Some(acceptance.accepted_at()),
                AvailabilityEvidence::evidenced(
                    acceptance.accepted_at(),
                    acceptance.evidence().clone(),
                ),
            )
        }
        None => (
            None,
            AvailabilityEvidence::local_first_observed(received_at),
        ),
    };
    let provider_id = ProviderInstrumentId::try_from(filing.cik())?;
    let instrument_id = identities
        .provider_identity_at(source_id, &provider_id, received_at)
        .ok_or(SecNormalizationError::InstrumentUnresolved)?
        .instrument_id();
    let mut ordinals = Vec::new();
    ordinals
        .try_reserve_exact(document.numeric_facts().len())
        .map_err(|_| SecNormalizationError::AllocationFailed)?;
    for original_index in 0..document.numeric_facts().len() {
        check_cancelled(cancellation)?;
        ordinals.push(FamilyOrdinal {
            original_index,
            ordinal: 0,
        });
    }
    check_cancelled(cancellation)?;
    ordinals.sort_unstable_by(|left, right| {
        compare_numeric_fact_family(
            &document.numeric_facts()[left.original_index],
            &document.numeric_facts()[right.original_index],
        )
        .then_with(|| left.original_index.cmp(&right.original_index))
    });
    check_cancelled(cancellation)?;
    let mut previous_index = None;
    let mut family_ordinal = 0_u32;
    for assignment in &mut ordinals {
        check_cancelled(cancellation)?;
        let same_family = previous_index.is_some_and(|previous| {
            compare_numeric_fact_family(
                &document.numeric_facts()[previous],
                &document.numeric_facts()[assignment.original_index],
            ) == Ordering::Equal
        });
        family_ordinal = if same_family {
            family_ordinal
                .checked_add(1)
                .ok_or(SecNormalizationError::RevisionOverflow)?
        } else {
            1
        };
        assignment.ordinal = family_ordinal;
        previous_index = Some(assignment.original_index);
    }
    check_cancelled(cancellation)?;
    ordinals.sort_unstable_by_key(|assignment| assignment.original_index);
    check_cancelled(cancellation)?;
    let mut numeric_occurrence_ids = Vec::new();
    numeric_occurrence_ids
        .try_reserve_exact(document.numeric_facts().len())
        .map_err(|_| SecNormalizationError::AllocationFailed)?;
    for fact in document.numeric_facts() {
        check_cancelled(cancellation)?;
        numeric_occurrence_ids.push(fact.evidence().occurrence_id().clone());
    }
    let numeric_occurrence_id_bytes = numeric_occurrence_ids
        .capacity()
        .checked_mul(size_of::<SourceIdentifier>())
        .and_then(|bytes| {
            numeric_occurrence_ids
                .iter()
                .try_fold(bytes, |total, id| total.checked_add(id.retained_bytes()))
        })
        .ok_or(SecNormalizationError::AllocationFailed)?;
    check_cancelled(cancellation)?;
    let (numeric_facts, nonnumeric_occurrences, retained_output_upper_bound) =
        document.into_families();
    let numeric_fact_count = numeric_facts.len();
    let nonnumeric_slot_bytes = nonnumeric_occurrences
        .capacity()
        .checked_mul(size_of::<XbrlNonnumericOccurrence>())
        .ok_or(SecNormalizationError::AllocationFailed)?;
    let (dataset, filing, taxonomy) = dataset
        .into_filing_xbrl_parts()
        .map_err(|_| SecNormalizationError::InvalidXbrlDataset)?;
    let native_lineage_retained_bytes = checked_native_lineage_retained_bytes(
        &dataset,
        &filing,
        &taxonomy,
        &availability,
        retained_output_upper_bound,
        numeric_occurrence_id_bytes,
        nonnumeric_slot_bytes,
    )?;
    Ok(SecFilingXbrlNormalization {
        source_id: source_id.clone(),
        instrument_id,
        dataset,
        filing,
        taxonomy,
        availability,
        source_timestamp,
        payload_digest,
        received_at,
        ingested_at,
        occurrence_ruleset: SourceIdentifier::try_from(SEC_XBRL_OCCURRENCE_ORDER_RULESET)?,
        numeric_facts: numeric_facts.into_iter(),
        ordinals: ordinals.into_iter(),
        numeric_fact_count,
        numeric_occurrence_ids,
        nonnumeric_occurrences,
        native_lineage_retained_bytes,
    })
}

fn checked_native_lineage_retained_bytes(
    dataset: &SourceIdentifier,
    filing: &SecFilingXbrlCoordinates,
    taxonomy: &SecValidatedXbrlTaxonomySet,
    availability: &AvailabilityEvidence,
    parser_retained_output_upper_bound: usize,
    numeric_occurrence_id_bytes: usize,
    nonnumeric_slot_bytes: usize,
) -> Result<u64, SecNormalizationError> {
    let availability_dynamic = match availability {
        AvailabilityEvidence::Evidenced { evidence, .. } => evidence.retained_bytes(),
        AvailabilityEvidence::Inferred { method, .. } => method.retained_bytes(),
        AvailabilityEvidence::LocalFirstObserved { .. } | AvailabilityEvidence::Unknown => 0,
    };
    let retained = size_of::<SecXbrlNativeLineage>()
        .checked_add(parser_retained_output_upper_bound)
        .and_then(|bytes| bytes.checked_add(numeric_occurrence_id_bytes))
        .and_then(|bytes| bytes.checked_add(nonnumeric_slot_bytes))
        .and_then(|bytes| bytes.checked_add(dataset.retained_bytes()))
        .and_then(|bytes| bytes.checked_add(filing.checked_dynamic_retained_bytes()?))
        .and_then(|bytes| bytes.checked_add(taxonomy.checked_dynamic_retained_bytes()?))
        .and_then(|bytes| bytes.checked_add(availability_dynamic))
        .ok_or(SecNormalizationError::AllocationFailed)?;
    u64::try_from(retained).map_err(|_| SecNormalizationError::AllocationFailed)
}

fn provider_cik(
    filing: &SecFilingXbrlCoordinates,
) -> Result<SourceIdentifier, SecNormalizationError> {
    SourceIdentifier::try_from(filing.cik()).map_err(Into::into)
}

fn fundamental_period(period: XbrlPeriod) -> Result<FundamentalPeriod, SecNormalizationError> {
    match period {
        XbrlPeriod::Instant { instant } => Ok(FundamentalPeriod::instant(instant)),
        XbrlPeriod::Duration { start, end } => {
            FundamentalPeriod::duration(start, end).map_err(Into::into)
        }
    }
}

fn compare_numeric_fact_family(
    left: &crate::XbrlNumericFact,
    right: &crate::XbrlNumericFact,
) -> Ordering {
    left.concept()
        .cmp(right.concept())
        .then_with(|| left.unit().cmp(right.unit()))
        .then_with(|| {
            family_period_key(left.evidence().period())
                .cmp(&family_period_key(right.evidence().period()))
        })
}

const fn family_period_key(period: XbrlPeriod) -> FamilyPeriodKey {
    match period {
        XbrlPeriod::Instant { instant } => FamilyPeriodKey::Instant(instant),
        XbrlPeriod::Duration { start, end } => FamilyPeriodKey::Duration(start, end),
    }
}

fn filing_fact_source_identifier(
    dataset: &SourceIdentifier,
    occurrence_id: &SourceIdentifier,
) -> Result<SourceIdentifier, SecNormalizationError> {
    let mut digest = Sha256::new();
    hash_identifier_field(&mut digest, dataset.as_str().as_bytes());
    hash_identifier_field(&mut digest, occurrence_id.as_str().as_bytes());
    SourceIdentifier::try_from(format!(
        "{SEC_XBRL_SOURCE_RECORD_PREFIX}{:x}",
        digest.finalize()
    ))
    .map_err(Into::into)
}

fn hash_identifier_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(
        u64::try_from(value.len())
            .map_or(u64::MAX, |length| length)
            .to_be_bytes(),
    );
    digest.update(value);
}

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
    ordered.sort_by(|left, right| compare_filings(left, right));
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

pub(crate) fn compare_filings(left: &SecFiling, right: &SecFiling) -> Ordering {
    left.report_date()
        .unwrap_or(left.filed_on())
        .cmp(&right.report_date().unwrap_or(right.filed_on()))
        .then_with(|| left.filed_on().cmp(&right.filed_on()))
        .then_with(|| left.accession().cmp(right.accession()))
}

pub(crate) fn compare_company_facts(
    left: &CompanyFactOccurrence,
    right: &CompanyFactOccurrence,
) -> Ordering {
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
    #[error("filing XBRL normalization requires exact filing-XBRL dataset coordinates")]
    InvalidXbrlDataset,
    #[error("parsed filing XBRL is not bound to the requested accession, taxonomy, or payload")]
    XbrlDocumentBindingMismatch,
    #[error("authoritative SEC acceptance time is later than local receipt")]
    PublicationAfterReceipt,
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
