//! Conservative point-in-time normalization of SEC filing research.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::mem::size_of;

use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, CompanyObservationSubject, DataQuality, DigestAlgorithm,
    EvidenceDigest, FilingObservation, FundamentalAmendmentStatus, FundamentalCadence,
    FundamentalConsolidation, FundamentalDimensionContext, FundamentalFactContext,
    FundamentalFactContextInput, FundamentalObservation, FundamentalPeriod,
    FundamentalRestatementStatus, FundamentalRevisionOrder, MetadataRevision, PayloadHash,
    PayloadReference, ResearchContext, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime, RevisionNumber,
    SchemaVersion, SourceId, SourceIdentifier, Timestamp, XbrlPeriod,
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
use crate::xbrl::IndexedXbrlDocument;
use crate::xbrl::{SecValidatedXbrlTaxonomySet, SecXbrlTaxonomyArtifact, SecXbrlTaxonomyReference};
use crate::{
    CompanyFactOccurrence, RetrievedCompanyFacts, RetrievedSubmissions, SecFiling,
    SecResearchDataset, SecResearchDatasetKind,
};

#[cfg(test)]
use crate::{XbrlFootnoteOccurrence, XbrlNonnumericOccurrence};

const SEC_XBRL_OCCURRENCE_ORDER_RULESET: &str = "sec-inline-xbrl-occurrence-order-v1";
const SEC_XBRL_SOURCE_RECORD_PREFIX: &str = "sec.xbrl.fact.sha256.";

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecFilingXbrlNativeBatchV1<'a> {
    version: u16,
    family: &'static str,
    dataset: &'a SourceIdentifier,
    filing: SecFilingXbrlCoordinatesV1<'a>,
    taxonomy: SecXbrlTaxonomyV1<'a>,
    availability: &'a AvailabilityEvidence,
    received_at: Timestamp,
    ingested_at: Timestamp,
    total_retained_bytes: u64,
    numeric_fact_count: usize,
    contexts: IndexedContexts<'a>,
    nonnumeric_occurrences: IndexedNonnumeric<'a>,
    footnotes: IndexedFootnotes<'a>,
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
    graph_evidence: EvidenceDigest,
    mapping_ruleset: &'a SourceIdentifier,
    catalog_release: &'a SourceIdentifier,
    physical_bytes: u64,
    scanned_bytes: u64,
    references: SecXbrlTaxonomyReferencesV1<'a>,
    artifacts: SecXbrlTaxonomyArtifactsV1<'a>,
}

struct SecXbrlTaxonomyReferencesV1<'a>(&'a [SecXbrlTaxonomyReference]);

impl Serialize for SecXbrlTaxonomyReferencesV1<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for reference in self.0 {
            sequence.serialize_element(&SecXbrlTaxonomyReferenceV1 {
                parent_logical_locator: reference.parent_logical_locator(),
                target_logical_locator: reference.target_logical_locator(),
                target_physical_locator: reference.target_physical_locator(),
                fragment: reference.fragment(),
                role: reference.role(),
                origin: reference.origin().as_str(),
            })?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecXbrlTaxonomyReferenceV1<'a> {
    parent_logical_locator: &'a SourceIdentifier,
    target_logical_locator: &'a SourceIdentifier,
    target_physical_locator: &'a SourceIdentifier,
    fragment: Option<&'a SourceIdentifier>,
    role: &'static str,
    origin: &'static str,
}

struct SecXbrlTaxonomyArtifactsV1<'a>(&'a [SecXbrlTaxonomyArtifact]);

impl Serialize for SecXbrlTaxonomyArtifactsV1<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for artifact in self.0 {
            sequence.serialize_element(&SecXbrlTaxonomyArtifactV1 {
                physical_locator: artifact.physical_locator(),
                logical_locators: artifact.logical_locators(),
                kind: artifact.kind().as_str(),
                pinned_release: artifact.pinned_release(),
                origin: artifact.origin().as_str(),
                source_id: artifact.source_id(),
                metadata_revision: artifact.metadata_revision(),
                target_namespace: artifact.target_namespace(),
                response_evidence: artifact.evidence(),
                response_bytes: artifact.size_bytes(),
                first_observed_at: artifact.first_observed_at(),
                retrieval_revision: artifact.retrieval_revision(),
            })?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecXbrlTaxonomyArtifactV1<'a> {
    physical_locator: &'a SourceIdentifier,
    logical_locators: &'a [SourceIdentifier],
    kind: &'static str,
    pinned_release: &'a SourceIdentifier,
    origin: &'static str,
    source_id: &'a SourceId,
    metadata_revision: &'a MetadataRevision,
    target_namespace: Option<&'a SourceIdentifier>,
    response_evidence: EvidenceDigest,
    response_bytes: u64,
    first_observed_at: Timestamp,
    retrieval_revision: u64,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecFilingXbrlNativeRowV1<'a> {
    family: &'static str,
    occurrence_id: &'a SourceIdentifier,
}

#[cfg(test)]
struct SecXbrlNonnumericOccurrencesV1<'a>(&'a [XbrlNonnumericOccurrence]);

#[cfg(test)]
struct SecXbrlOccurrenceContextsV1<'a>(&'a [&'a XbrlNonnumericOccurrence]);

#[cfg(test)]
impl Serialize for SecXbrlOccurrenceContextsV1<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for occurrence in self.0 {
            sequence.serialize_element(&SecXbrlOccurrenceContextV1 {
                context_id: occurrence.context_id(),
                entity: occurrence.entity(),
                period: occurrence.period(),
                dimensions: occurrence.dimensions(),
                context_graph: occurrence.context_graph(),
            })?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecXbrlOccurrenceContextV1<'a> {
    context_id: &'a SourceIdentifier,
    entity: &'a market_squawk_domain::XbrlEntity,
    period: XbrlPeriod,
    dimensions: &'a [market_squawk_domain::XbrlDimensionEvidence],
    context_graph: &'a market_squawk_domain::XbrlContextGraph,
}

#[cfg(test)]
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

#[cfg(test)]
struct SecXbrlFootnotesV1<'a>(&'a [XbrlFootnoteOccurrence]);

#[cfg(test)]
impl Serialize for SecXbrlFootnotesV1<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for footnote in self.0 {
            sequence.serialize_element(&SecXbrlFootnoteV1 {
                occurrence_id: footnote.occurrence_id(),
                accession: footnote.accession(),
                language: footnote.language(),
                role: footnote.role(),
                title: footnote.title(),
                lexical_value: footnote.lexical_value(),
                source_payload: footnote.source_payload(),
                occurrence_relationships: footnote.occurrence_relationships(),
            })?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecXbrlFootnoteV1<'a> {
    occurrence_id: &'a SourceIdentifier,
    accession: &'a SourceIdentifier,
    language: &'a market_squawk_domain::XbrlText,
    role: &'a SourceIdentifier,
    title: Option<&'a market_squawk_domain::XbrlText>,
    lexical_value: &'a market_squawk_domain::XbrlText,
    source_payload: &'a market_squawk_domain::ExactPayloadEvidence,
    occurrence_relationships: &'a market_squawk_domain::XbrlOccurrenceRelationships,
}

struct IndexedContexts<'a>(&'a IndexedXbrlDocument, &'a CancellationToken, Timestamp);
struct IndexedNonnumeric<'a>(&'a IndexedXbrlDocument, &'a CancellationToken, Timestamp);
struct IndexedFootnotes<'a>(&'a IndexedXbrlDocument, &'a CancellationToken, Timestamp);
impl Serialize for IndexedContexts<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let count = self.0.context_count().map_err(serde::ser::Error::custom)?;
        let mut sequence = serializer.serialize_seq(Some(count))?;
        for ordinal in 0..count {
            if self.1.is_cancelled()
                || crate::client::system_timestamp().map_err(serde::ser::Error::custom)? >= self.2
            {
                return Err(serde::ser::Error::custom(
                    "filing native serialization cancelled or expired",
                ));
            }
            let row = self
                .0
                .nonnumeric_context_at(ordinal)
                .map_err(serde::ser::Error::custom)?
                .ok_or_else(|| serde::ser::Error::custom("missing indexed context"))?;
            sequence.serialize_element(&SecXbrlOccurrenceContextV1 {
                context_id: row.context_id(),
                entity: row.entity(),
                period: row.period(),
                dimensions: row.dimensions(),
                context_graph: row.context_graph(),
            })?;
        }
        sequence.end()
    }
}
impl Serialize for IndexedNonnumeric<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.nonnumeric_count))?;
        for ordinal in 0..self.0.nonnumeric_count {
            if self.1.is_cancelled()
                || crate::client::system_timestamp().map_err(serde::ser::Error::custom)? >= self.2
            {
                return Err(serde::ser::Error::custom(
                    "filing native serialization cancelled or expired",
                ));
            }
            let row = self
                .0
                .nonnumeric_at(ordinal)
                .map_err(serde::ser::Error::custom)?
                .ok_or_else(|| serde::ser::Error::custom("missing indexed occurrence"))?;
            sequence.serialize_element(&SecXbrlNonnumericOccurrenceV1 {
                occurrence_id: row.occurrence_id(),
                accession: row.accession(),
                concept: row.concept(),
                context_id: row.context_id(),
                lexical_value: row.lexical_value(),
                nil: row.is_nil(),
                source_payload: row.source_payload(),
                occurrence_relationships: row.occurrence_relationships(),
            })?;
        }
        sequence.end()
    }
}
impl Serialize for IndexedFootnotes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.footnote_count))?;
        for ordinal in 0..self.0.footnote_count {
            if self.1.is_cancelled()
                || crate::client::system_timestamp().map_err(serde::ser::Error::custom)? >= self.2
            {
                return Err(serde::ser::Error::custom(
                    "filing native serialization cancelled or expired",
                ));
            }
            let row = self
                .0
                .footnote_at(ordinal)
                .map_err(serde::ser::Error::custom)?
                .ok_or_else(|| serde::ser::Error::custom("missing indexed footnote"))?;
            sequence.serialize_element(&SecXbrlFootnoteV1 {
                occurrence_id: row.occurrence_id(),
                accession: row.accession(),
                language: row.language(),
                role: row.role(),
                title: row.title(),
                lexical_value: row.lexical_value(),
                source_payload: row.source_payload(),
                occurrence_relationships: row.occurrence_relationships(),
            })?;
        }
        sequence.end()
    }
}

/// Numeric canonical observations paired indivisibly with native nonnumeric lineage.
#[derive(Debug)]
pub(crate) struct SecFilingXbrlNormalization {
    source_id: SourceId,
    dataset: SourceIdentifier,
    filing: SecFilingXbrlCoordinates,
    taxonomy: SecValidatedXbrlTaxonomySet,
    availability: AvailabilityEvidence,
    source_timestamp: Option<Timestamp>,
    payload_digest: EvidenceDigest,
    received_at: Timestamp,
    ingested_at: Timestamp,
    occurrence_ruleset: SourceIdentifier,
    next_numeric: usize,
    numeric_fact_count: usize,
    document: IndexedXbrlDocument,
    native_lineage_retained_bytes: u64,
    native_chunks: Option<market_squawk_sources::ProviderNativeSidecarChunks>,
    scratch_parent: std::path::PathBuf,
    working_set_retained_bytes: usize,
}

impl SecFilingXbrlNormalization {
    pub(crate) const fn working_set_retained_bytes(&self) -> usize {
        self.working_set_retained_bytes
    }

    /// Produces one canonical numeric fact at a time so parsed and canonical families do not grow
    /// simultaneously as full vectors.
    pub(crate) fn try_next_observation(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<Option<ResearchObservation>, SecNormalizationError> {
        check_cancelled(cancellation)?;
        let Some(fact) = self.document.numeric_at(self.next_numeric)? else {
            return Ok(None);
        };
        let ordinal = self.document.family_ordinal_at(self.next_numeric)?;
        self.next_numeric = self
            .next_numeric
            .checked_add(1)
            .ok_or(SecNormalizationError::RevisionOverflow)?;
        let (concept, unit, value, evidence) = fact.into_parts();
        let period = fundamental_period(evidence.period())?;
        let source_identifier =
            filing_fact_source_identifier(&self.dataset, evidence.occurrence_id())?;
        let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: self.source_id.clone(),
            instrument_id: None,
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
                RevisionNumber::new(ordinal)?,
                self.occurrence_ruleset.clone(),
            ),
            restatement_status: FundamentalRestatementStatus::Unavailable,
        })?;
        Ok(Some(ResearchObservation::Fundamental(
            FundamentalObservation::new_with_xbrl_evidence(
                ResearchContext::new(provenance, research_time)?,
                CompanyObservationSubject::Issuer(provider_cik(&self.filing)?),
                concept,
                value,
                fact_context,
                evidence,
            )?,
        )))
    }

    pub(crate) const fn numeric_fact_count(&self) -> usize {
        self.numeric_fact_count
    }

    /// Retains one shared immutable native stream while canonical rows leave in bounded ranges.
    pub(crate) fn native_for_batch(
        &mut self,
        batch: &ExtractionBatch,
        start: usize,
        maximum: usize,
        cancellation: &CancellationToken,
    ) -> Result<ProviderNativeLineageBatch, ProviderNativeLineageError> {
        if start
            .checked_add(batch.records().len())
            .is_none_or(|end| end > self.numeric_fact_count)
        {
            return Err(ProviderNativeLineageError::AlignmentMismatch);
        }
        if self.native_chunks.is_none() {
            self.native_chunks = Some(
                market_squawk_sources::ProviderNativeSidecarChunks::serialize_in(
                    &SecFilingXbrlNativeBatchV1 {
                        version: 1,
                        family: "filing_xbrl",
                        dataset: &self.dataset,
                        filing: SecFilingXbrlCoordinatesV1::from_filing(&self.filing),
                        taxonomy: SecXbrlTaxonomyV1 {
                            version: self.taxonomy.version(),
                            artifact_set: self.taxonomy.artifact_set(),
                            fingerprint: self.taxonomy.fingerprint(),
                            graph_evidence: self.taxonomy.graph_evidence(),
                            mapping_ruleset: self.taxonomy.mapping_ruleset(),
                            catalog_release: self.taxonomy.catalog_release(),
                            physical_bytes: self.taxonomy.physical_bytes(),
                            scanned_bytes: self.taxonomy.scanned_bytes(),
                            references: SecXbrlTaxonomyReferencesV1(self.taxonomy.references()),
                            artifacts: SecXbrlTaxonomyArtifactsV1(self.taxonomy.artifacts()),
                        },
                        availability: &self.availability,
                        received_at: self.received_at,
                        ingested_at: self.ingested_at,
                        total_retained_bytes: self.native_lineage_retained_bytes,
                        numeric_fact_count: self.numeric_fact_count,
                        contexts: IndexedContexts(
                            &self.document,
                            cancellation,
                            batch.request().deadline(),
                        ),
                        nonnumeric_occurrences: IndexedNonnumeric(
                            &self.document,
                            cancellation,
                            batch.request().deadline(),
                        ),
                        footnotes: IndexedFootnotes(
                            &self.document,
                            cancellation,
                            batch.request().deadline(),
                        ),
                    },
                    &self.scratch_parent,
                )?,
            );
        }
        let mut native = ProviderNativeLineageBatchBuilder::try_new_bounded(
            ProviderNativeLineageImplementation::SecEdgarV1,
            batch,
            maximum,
        )?;
        native.try_set_sidecar_chunks(
            self.native_chunks
                .as_ref()
                .ok_or(ProviderNativeLineageError::AlignmentMismatch)?
                .clone(),
        )?;
        for (ordinal, record) in batch.records().iter().enumerate() {
            let fact = self
                .document
                .numeric_at(start + ordinal)
                .map_err(|_| ProviderNativeLineageError::AlignmentMismatch)?
                .ok_or(ProviderNativeLineageError::AlignmentMismatch)?;
            let occurrence_id = fact.evidence().occurrence_id();
            if filing_fact_source_identifier(&self.dataset, occurrence_id)
                .ok()
                .as_ref()
                != Some(record.revision())
            {
                return Err(ProviderNativeLineageError::AlignmentMismatch);
            }
            native.try_push(&SecFilingXbrlNativeRowV1 {
                family: "numeric_fact",
                occurrence_id,
            })?;
        }
        native.finish()
    }
}

/// Maps one exact parsed filing into canonical numeric facts plus mandatory native text lineage.
pub(crate) fn normalize_filing_xbrl_with_cancellation(
    source_id: &SourceId,
    dataset: SecResearchDataset,
    document: IndexedXbrlDocument,
    payload_digest: EvidenceDigest,
    received_at: Timestamp,
    ingested_at: Timestamp,
    maximum_retained_bytes: usize,
    cancellation: &CancellationToken,
    scratch_parent: &std::path::Path,
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
    // Only one indexed record and its canonical serialization are live at a time.
    let working_set_retained_bytes = document
        .peak_retained_bytes
        .checked_add(
            dataset
                .checked_dynamic_retained_bytes()
                .ok_or(SecNormalizationError::AllocationFailed)?,
        )
        .and_then(|bytes| bytes.checked_add(size_of::<SecFilingXbrlNormalization>()))
        .ok_or(SecNormalizationError::AllocationFailed)?;
    if working_set_retained_bytes > maximum_retained_bytes {
        return Err(SecNormalizationError::AllocationFailed);
    }
    let numeric_fact_count = document.numeric_count;
    let (dataset, filing, taxonomy) = dataset
        .into_filing_xbrl_parts()
        .map_err(|_| SecNormalizationError::InvalidXbrlDataset)?;
    let native_lineage_retained_bytes = u64::try_from(working_set_retained_bytes)
        .map_err(|_| SecNormalizationError::AllocationFailed)?;
    Ok(SecFilingXbrlNormalization {
        source_id: source_id.clone(),
        dataset,
        filing,
        taxonomy,
        availability,
        source_timestamp,
        payload_digest,
        received_at,
        ingested_at,
        occurrence_ruleset: SourceIdentifier::try_from(SEC_XBRL_OCCURRENCE_ORDER_RULESET)?,
        next_numeric: 0,
        numeric_fact_count,
        document,
        native_lineage_retained_bytes,
        native_chunks: None,
        scratch_parent: scratch_parent.to_path_buf(),
        working_set_retained_bytes,
    })
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

/// Normalizes complete SEC submissions into issuer-owned point-in-time filing observations.
pub fn normalize_filings(
    source_id: &SourceId,
    retrieved: &RetrievedSubmissions,
    ingested_at: Timestamp,
) -> Result<Vec<ResearchObservation>, SecNormalizationError> {
    normalize_filings_with_cancellation(
        source_id,
        retrieved,
        ingested_at,
        &CancellationToken::new(),
    )
}

/// Normalizes complete filings with cooperative observation cancellation.
pub fn normalize_filings_with_cancellation(
    source_id: &SourceId,
    retrieved: &RetrievedSubmissions,
    ingested_at: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Vec<ResearchObservation>, SecNormalizationError> {
    check_cancelled(cancellation)?;
    let received_at = retrieved.raw().received_at();
    if ingested_at < received_at {
        return Err(SecNormalizationError::IngestedBeforeReceived);
    }
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
            instrument_id: None,
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
            CompanyObservationSubject::Issuer(retrieved.document().cik().clone()),
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

/// Normalizes every numeric issuer-owned Company Facts occurrence with conservative availability.
///
/// SEC acceptance and filing dates are retained by their source records but are not silently
/// promoted to first-public-availability evidence. The raw response's first local observation is
/// therefore the default point-in-time cutoff for online retrievals, while offline imports remain
/// explicitly unknown. Amendments and later occurrences for the same concept/unit/period receive
/// increasing revision numbers and are never overwritten.
pub fn normalize_company_facts(
    source_id: &SourceId,
    retrieved: &RetrievedCompanyFacts,
    ingested_at: Timestamp,
) -> Result<Vec<ResearchObservation>, SecNormalizationError> {
    normalize_company_facts_with_cancellation(
        source_id,
        retrieved,
        ingested_at,
        &CancellationToken::new(),
    )
}

/// Normalizes Company Facts with cooperative occurrence cancellation.
pub fn normalize_company_facts_with_cancellation(
    source_id: &SourceId,
    retrieved: &RetrievedCompanyFacts,
    ingested_at: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Vec<ResearchObservation>, SecNormalizationError> {
    check_cancelled(cancellation)?;
    let received_at = retrieved.raw().received_at();
    if ingested_at < received_at {
        return Err(SecNormalizationError::IngestedBeforeReceived);
    }
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
            "{}:{}:{}:{}:{}:{}",
            occurrence.accession(),
            occurrence.concept(),
            occurrence.unit(),
            start.as_deref().unwrap_or("instant"),
            end,
            occurrence.source_ordinal(),
        ))?;
        let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: source_id.clone(),
            instrument_id: None,
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
                CompanyObservationSubject::Issuer(retrieved.document().cik().clone()),
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
        .then_with(|| left.source_ordinal().cmp(&right.source_ordinal()))
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
    #[error(transparent)]
    Xbrl(#[from] crate::SecXbrlError),
    #[error("SEC canonical normalization was cancelled")]
    Cancelled,
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

#[cfg(test)]
mod tests {
    use super::*;
    use market_squawk_domain::{
        ExactPayloadEvidence, XbrlContextGraph, XbrlDimensionEvidence, XbrlEntity, XbrlTaxonomySet,
    };

    #[test]
    fn filing_text_and_nil_context_roundtrip_retains_numeric_occurrences()
    -> Result<(), Box<dyn std::error::Error>> {
        // Source contexts and listing text from Microsoft's 2025 10-K, accession
        // 0000950170-25-100235. The extra nil count is an adversarial occurrence.
        let filing = r#"<html xmlns="http://www.w3.org/1999/xhtml"
            xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
            xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:dei="http://xbrl.sec.gov/dei/2025"
            xmlns:us-gaap="http://fasb.org/us-gaap/2025">
            <xbrli:context id="C_ab42cc55-95ad-4b2e-8e74-169b6dee45a0">
              <xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0000789019</xbrli:identifier></xbrli:entity>
              <xbrli:period><xbrli:instant>2025-07-24</xbrli:instant></xbrli:period>
            </xbrli:context>
            <xbrli:context id="C_04ab15e3-43d1-4478-b37c-41786502888e">
              <xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0000789019</xbrli:identifier>
                <xbrli:segment><xbrldi:explicitMember dimension="us-gaap:StatementClassOfStockAxis">us-gaap:CommonStockMember</xbrldi:explicitMember></xbrli:segment>
              </xbrli:entity><xbrli:period><xbrli:startDate>2024-07-01</xbrli:startDate><xbrli:endDate>2025-06-30</xbrli:endDate></xbrli:period>
            </xbrli:context>
            <xbrli:unit id="U_shares"><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unit>
            <ix:nonFraction id="shares" contextRef="C_ab42cc55-95ad-4b2e-8e74-169b6dee45a0" name="dei:EntityCommonStockSharesOutstanding" unitRef="U_shares" decimals="0">7433166379</ix:nonFraction>
            <ix:nonNumeric id="symbol" contextRef="C_04ab15e3-43d1-4478-b37c-41786502888e" name="dei:TradingSymbol">MSFT</ix:nonNumeric>
            <ix:nonNumeric id="exchange" contextRef="C_04ab15e3-43d1-4478-b37c-41786502888e" name="dei:SecurityExchangeName">Nasdaq</ix:nonNumeric>
            <ix:nonFraction id="nil-shares" contextRef="C_04ab15e3-43d1-4478-b37c-41786502888e" name="dei:EntityCommonStockSharesOutstanding" unitRef="U_shares" xsi:nil="true"/>
          </html>"#;
        let parse = |bytes: &[u8]| {
            crate::XbrlDocumentParser::parse_with_cancellation(
                bytes,
                crate::SecParserLimits::production_defaults(),
                crate::XbrlDocumentContext::new(
                    SourceIdentifier::try_from("0000950170-25-100235").expect("static accession"),
                    XbrlTaxonomySet::declared(
                        EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]),
                        SourceIdentifier::try_from("fixture-taxonomy").expect("static taxonomy"),
                    ),
                    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        Sha256::digest(bytes).into(),
                    )),
                    Timestamp::from_unix_nanos(1),
                ),
                &CancellationToken::new(),
            )
        };
        let parsed = parse(filing.as_bytes())?;
        assert_eq!(parsed.numeric_facts().len(), 1);
        assert_eq!(
            parsed.numeric_facts()[0].value(),
            rust_decimal::Decimal::from(7_433_166_379_u64)
        );
        let occurrences = parsed.nonnumeric_occurrences();
        assert_eq!(occurrences.len(), 3);
        let contexts = [&occurrences[0]];
        let context_bytes = serde_json::to_vec(&SecXbrlOccurrenceContextsV1(&contexts))?;
        let occurrence_bytes = serde_json::to_vec(&SecXbrlNonnumericOccurrencesV1(occurrences))?;
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RetainedContext {
            context_id: SourceIdentifier,
            entity: XbrlEntity,
            period: XbrlPeriod,
            dimensions: Vec<XbrlDimensionEvidence>,
            context_graph: XbrlContextGraph,
        }
        let retained: Vec<RetainedContext> = serde_json::from_slice(&context_bytes)?;
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&occurrence_bytes)?;
        assert_eq!(retained.len(), 1);
        let context = &retained[0];
        assert_eq!(context.entity.value().as_str(), "0000789019");
        assert_eq!(context.period, occurrences[0].period());
        assert_eq!(context.dimensions, occurrences[0].dimensions());
        assert_eq!(context.context_graph, *occurrences[0].context_graph());
        assert!(!context.context_graph.events().is_empty());
        assert!(
            rows.iter()
                .all(|row| row["context_id"] == context.context_id.as_str())
        );
        assert_eq!(rows[0]["lexical_value"], "MSFT");
        assert_eq!(rows[2]["nil"], true);
        let malformed = filing.replace("<xbrli:startDate>2024-07-01</xbrli:startDate>", "");
        assert!(matches!(
            parse(malformed.as_bytes()),
            Err(crate::SecXbrlError::IncompleteContext)
        ));
        Ok(())
    }
}
