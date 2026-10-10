//! Complete verified filing occurrences. Economic class and share-model admission belongs to
//! the financial consumer; these types preserve source facts without minting a denominator.

use rusqlite::{Connection, OptionalExtension as _, params};
use serde::de::{DeserializeSeed, MapAccess, Visitor};
use sha2::{Digest as _, Sha256};
use std::io::Read;
use std::sync::Arc;

use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, EvidenceDigest, ExactPayloadEvidence, FilingForm,
    ResearchObservation, SourceIdentifier, Timestamp, XbrlContextGraph, XbrlDimensionEvidence,
    XbrlEntity, XbrlOccurrenceRelationships, XbrlPeriod, XbrlQualifiedName, XbrlText,
};
use serde::{Deserialize, Deserializer, Serialize};
use tokio_util::sync::CancellationToken;

use super::{SecResearchReadError, SecResearchRows, check_operation, observation_context};
use market_squawk_platform::ResearchObjectControl;

/// Complete nonnumeric/nil source family from one physically verified exact filing publication.
/// Numeric occurrences remain in the owning selection's complete `decoded_rows` collection.
/// This value cannot be constructed or deserialized by a caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecVerifiedFilingXbrl {
    source: FilingSidecar,
    sidecar_digest: EvidenceDigest,
}

impl SecVerifiedFilingXbrl {
    pub fn cik(&self) -> &str {
        &self.source.filing.cik
    }
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.source.filing.accession
    }
    pub const fn document(&self) -> &SourceIdentifier {
        &self.source.filing.document
    }
    pub const fn filing_form(&self) -> &FilingForm {
        &self.source.filing.filing_form
    }
    pub const fn filed_on(&self) -> CalendarDate {
        self.source.filing.filed_on
    }
    pub const fn report_date(&self) -> Option<CalendarDate> {
        self.source.filing.report_date
    }
    pub const fn availability(&self) -> &AvailabilityEvidence {
        &self.source.availability
    }
    pub const fn received_at(&self) -> Timestamp {
        self.source.received_at
    }
    pub const fn ingested_at(&self) -> Timestamp {
        self.source.ingested_at
    }
    pub const fn sidecar_digest(&self) -> EvidenceDigest {
        self.sidecar_digest
    }
    pub const fn numeric_fact_count(&self) -> usize {
        self.source.numeric_fact_count
    }
    pub fn contexts(&self) -> &SecResearchRows<SecFilingXbrlContext> {
        &self.source.contexts
    }
    pub fn nonnumeric_occurrences(&self) -> &SecResearchRows<SecFilingXbrlNonnumericOccurrence> {
        &self.source.nonnumeric_occurrences
    }
    /// Returns complete explanatory footnotes, including their original relationship edges.
    pub fn footnotes(&self) -> &SecResearchRows<SecFilingXbrlFootnote> {
        &self.source.footnotes
    }

    /// Looks up the shared context of a nil or nonnumeric source occurrence.
    pub fn context(
        &self,
        id: &SourceIdentifier,
    ) -> Result<Option<SecFilingXbrlContext>, SecResearchReadError> {
        self.source.contexts.by_key(id.as_str())
    }
}

/// One source context, retained once and referenced by original context identity.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SecFilingXbrlContext {
    context_id: SourceIdentifier,
    entity: XbrlEntity,
    period: XbrlPeriod,
    dimensions: Vec<XbrlDimensionEvidence>,
    context_graph: XbrlContextGraph,
}

impl SecFilingXbrlContext {
    pub const fn context_id(&self) -> &SourceIdentifier {
        &self.context_id
    }
    pub const fn entity(&self) -> &XbrlEntity {
        &self.entity
    }
    pub const fn period(&self) -> XbrlPeriod {
        self.period
    }
    pub fn dimensions(&self) -> &[XbrlDimensionEvidence] {
        &self.dimensions
    }
    pub const fn context_graph(&self) -> &XbrlContextGraph {
        &self.context_graph
    }
}

/// Original nonnumeric or nil occurrence. Its context is owned by the complete filing table.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SecFilingXbrlNonnumericOccurrence {
    occurrence_id: SourceIdentifier,
    accession: SourceIdentifier,
    concept: XbrlQualifiedName,
    context_id: SourceIdentifier,
    lexical_value: XbrlText,
    nil: bool,
    source_payload: ExactPayloadEvidence,
    occurrence_relationships: XbrlOccurrenceRelationships,
}

impl SecFilingXbrlNonnumericOccurrence {
    pub const fn occurrence_id(&self) -> &SourceIdentifier {
        &self.occurrence_id
    }
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }
    pub const fn concept(&self) -> &XbrlQualifiedName {
        &self.concept
    }
    pub const fn context_id(&self) -> &SourceIdentifier {
        &self.context_id
    }
    pub const fn lexical_value(&self) -> &XbrlText {
        &self.lexical_value
    }
    pub const fn is_nil(&self) -> bool {
        self.nil
    }
    pub const fn source_payload(&self) -> &ExactPayloadEvidence {
        &self.source_payload
    }
    pub const fn occurrence_relationships(&self) -> &XbrlOccurrenceRelationships {
        &self.occurrence_relationships
    }
}

/// Original explanatory footnote text and metadata from the verified filing payload.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SecFilingXbrlFootnote {
    occurrence_id: SourceIdentifier,
    accession: SourceIdentifier,
    language: XbrlText,
    role: SourceIdentifier,
    title: Option<XbrlText>,
    lexical_value: XbrlText,
    source_payload: ExactPayloadEvidence,
    occurrence_relationships: XbrlOccurrenceRelationships,
}

impl SecFilingXbrlFootnote {
    pub const fn occurrence_id(&self) -> &SourceIdentifier {
        &self.occurrence_id
    }
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }
    pub const fn language(&self) -> &XbrlText {
        &self.language
    }
    pub const fn role(&self) -> &SourceIdentifier {
        &self.role
    }
    pub const fn title(&self) -> Option<&XbrlText> {
        self.title.as_ref()
    }
    pub const fn lexical_value(&self) -> &XbrlText {
        &self.lexical_value
    }
    pub const fn source_payload(&self) -> &ExactPayloadEvidence {
        &self.source_payload
    }
    pub const fn occurrence_relationships(&self) -> &XbrlOccurrenceRelationships {
        &self.occurrence_relationships
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilingSidecar {
    version: u16,
    family: SourceIdentifier,
    dataset: SourceIdentifier,
    filing: FilingCoordinates,
    taxonomy: TaxonomyCoordinates,
    availability: AvailabilityEvidence,
    received_at: Timestamp,
    ingested_at: Timestamp,
    total_retained_bytes: u64,
    numeric_fact_count: usize,
    contexts: SecResearchRows<SecFilingXbrlContext>,
    nonnumeric_occurrences: SecResearchRows<SecFilingXbrlNonnumericOccurrence>,
    footnotes: SecResearchRows<SecFilingXbrlFootnote>,
}

struct FilingSidecarSeed(Arc<super::indexed::IndexScratch>);
impl<'de> DeserializeSeed<'de> for FilingSidecarSeed {
    type Value = FilingSidecar;
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<FilingSidecar, D::Error> {
        decoder.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for FilingSidecarSeed {
    type Value = FilingSidecar;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("complete filing sidecar")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<FilingSidecar, A::Error> {
        let mut version = None;
        let mut family = None;
        let mut dataset = None;
        let mut filing = None;
        let mut taxonomy = None;
        let mut availability = None;
        let mut received_at = None;
        let mut ingested_at = None;
        let mut total_retained_bytes = None;
        let mut numeric_fact_count = None;
        let mut contexts = None;
        let mut nonnumeric_occurrences = None;
        let mut footnotes = None;
        while let Some(field) = map.next_key::<String>()? {
            match field.as_str() {
                "version" => {
                    if version.is_some() {
                        return Err(serde::de::Error::duplicate_field("version"));
                    }
                    version = Some(map.next_value()?);
                }
                "family" => {
                    if family.is_some() {
                        return Err(serde::de::Error::duplicate_field("family"));
                    }
                    family = Some(map.next_value()?);
                }
                "dataset" => {
                    if dataset.is_some() {
                        return Err(serde::de::Error::duplicate_field("dataset"));
                    }
                    dataset = Some(map.next_value()?);
                }
                "filing" => {
                    if filing.is_some() {
                        return Err(serde::de::Error::duplicate_field("filing"));
                    }
                    filing = Some(map.next_value()?);
                }
                "taxonomy" => {
                    if taxonomy.is_some() {
                        return Err(serde::de::Error::duplicate_field("taxonomy"));
                    }
                    taxonomy = Some(map.next_value()?);
                }
                "availability" => {
                    if availability.is_some() {
                        return Err(serde::de::Error::duplicate_field("availability"));
                    }
                    availability = Some(map.next_value()?);
                }
                "received_at" => {
                    if received_at.is_some() {
                        return Err(serde::de::Error::duplicate_field("received_at"));
                    }
                    received_at = Some(map.next_value()?);
                }
                "ingested_at" => {
                    if ingested_at.is_some() {
                        return Err(serde::de::Error::duplicate_field("ingested_at"));
                    }
                    ingested_at = Some(map.next_value()?);
                }
                "total_retained_bytes" => {
                    if total_retained_bytes.is_some() {
                        return Err(serde::de::Error::duplicate_field("total_retained_bytes"));
                    }
                    total_retained_bytes = Some(map.next_value()?);
                }
                "numeric_fact_count" => {
                    if numeric_fact_count.is_some() {
                        return Err(serde::de::Error::duplicate_field("numeric_fact_count"));
                    }
                    numeric_fact_count = Some(map.next_value()?);
                }
                "contexts" => {
                    if contexts.is_some() {
                        return Err(serde::de::Error::duplicate_field("contexts"));
                    }
                    contexts = Some(map.next_value_seed(super::indexed::RowsSeed {
                        scratch: Arc::clone(&self.0),
                        row: std::marker::PhantomData,
                    })?);
                }
                "nonnumeric_occurrences" => {
                    if nonnumeric_occurrences.is_some() {
                        return Err(serde::de::Error::duplicate_field("nonnumeric_occurrences"));
                    }
                    nonnumeric_occurrences =
                        Some(map.next_value_seed(super::indexed::RowsSeed {
                            scratch: Arc::clone(&self.0),
                            row: std::marker::PhantomData,
                        })?);
                }
                "footnotes" => {
                    if footnotes.is_some() {
                        return Err(serde::de::Error::duplicate_field("footnotes"));
                    }
                    footnotes = Some(map.next_value_seed(super::indexed::RowsSeed {
                        scratch: Arc::clone(&self.0),
                        row: std::marker::PhantomData,
                    })?);
                }
                _ => return Err(serde::de::Error::custom("unknown filing sidecar field")),
            }
        }
        Ok(FilingSidecar {
            version: version.ok_or_else(|| serde::de::Error::missing_field("version"))?,
            family: family.ok_or_else(|| serde::de::Error::missing_field("family"))?,
            dataset: dataset.ok_or_else(|| serde::de::Error::missing_field("dataset"))?,
            filing: filing.ok_or_else(|| serde::de::Error::missing_field("filing"))?,
            taxonomy: taxonomy.ok_or_else(|| serde::de::Error::missing_field("taxonomy"))?,
            availability: availability
                .ok_or_else(|| serde::de::Error::missing_field("availability"))?,
            received_at: received_at
                .ok_or_else(|| serde::de::Error::missing_field("received_at"))?,
            ingested_at: ingested_at
                .ok_or_else(|| serde::de::Error::missing_field("ingested_at"))?,
            total_retained_bytes: total_retained_bytes
                .ok_or_else(|| serde::de::Error::missing_field("total_retained_bytes"))?,
            numeric_fact_count: numeric_fact_count
                .ok_or_else(|| serde::de::Error::missing_field("numeric_fact_count"))?,
            contexts: contexts.ok_or_else(|| serde::de::Error::missing_field("contexts"))?,
            nonnumeric_occurrences: nonnumeric_occurrences
                .ok_or_else(|| serde::de::Error::missing_field("nonnumeric_occurrences"))?,
            footnotes: footnotes.ok_or_else(|| serde::de::Error::missing_field("footnotes"))?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct FilingCoordinates {
    cik: String,
    accession: SourceIdentifier,
    document: SourceIdentifier,
    filing_form: FilingForm,
    filed_on: CalendarDate,
    report_date: Option<CalendarDate>,
    filing_size_bytes: Option<u64>,
    is_inline_xbrl: bool,
    accepted_at: Option<Timestamp>,
    acceptance_evidence: Option<SourceIdentifier>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct TaxonomyCoordinates {
    version: SourceIdentifier,
    artifact_set: EvidenceDigest,
    fingerprint: EvidenceDigest,
    graph_evidence: EvidenceDigest,
    mapping_ruleset: SourceIdentifier,
    catalog_release: SourceIdentifier,
    physical_bytes: u64,
    scanned_bytes: u64,
    // Existing common native/physical integrity owns these exact captured graph structures.
    // Their bytes are bound by sidecar_digest; this reader does not duplicate their ownership.
    references: CapturedGraph,
    artifacts: CapturedGraph,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
struct CapturedGraph;
impl<'de> Deserialize<'de> for CapturedGraph {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        serde::de::IgnoredAny::deserialize(deserializer).map(|_| Self)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NumericNativeRow {
    family: SourceIdentifier,
    occurrence_id: SourceIdentifier,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub(super) struct SecNumericNativeEvidence {
    pub(super) semantic_payload: Vec<u8>,
    pub(super) capture_page_ordinal: u16,
}

// Cancellation is checked during both JSON streaming and indexed graph validation.
struct ControlledReader<'a, R: Read> {
    reader: std::io::BufReader<R>,
    deadline: std::time::Instant,
    cancellation: &'a CancellationToken,
}
impl<R: Read> Read for ControlledReader<'_, R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        check_operation(self.deadline, self.cancellation).map_err(std::io::Error::other)?;
        self.reader.read(bytes)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "complete origin and operation controls are explicit"
)]
pub(super) fn read_verified_filing<R: Read>(
    capture: &market_squawk_sources::ProviderCaptureSetReceipt,
    reader: R,
    sidecar_digest: EvidenceDigest,
    native_rows: &SecResearchRows<SecNumericNativeEvidence>,
    observations: &SecResearchRows<ResearchObservation>,
    expected_cik: &str,
    maximum_bytes: usize,
    deadline: std::time::Instant,
    cancellation: &CancellationToken,
    _control: &dyn ResearchObjectControl,
    scratch: Arc<super::indexed::IndexScratch>,
) -> Result<(SecVerifiedFilingXbrl, usize), SecResearchReadError> {
    check_operation(deadline, cancellation)?;
    // Three source row caches plus graph validation and the streaming JSON buffer.
    let retained_bytes = 5 * 1024 * 1024;
    if retained_bytes > maximum_bytes {
        return Err(SecResearchReadError::ObjectBudgetExceeded);
    }
    let reader = ControlledReader {
        reader: std::io::BufReader::with_capacity(64 * 1024, reader),
        deadline,
        cancellation,
    };
    let mut decoder = serde_json::Deserializer::from_reader(reader);
    let decoded_source = FilingSidecarSeed(Arc::clone(&scratch)).deserialize(&mut decoder);
    check_operation(deadline, cancellation)?;
    scratch.ensure_budget()?;
    let source = decoded_source.map_err(|_| SecResearchReadError::ProviderBindingMismatch)?;
    decoder
        .end()
        .map_err(|_| SecResearchReadError::ProviderBindingMismatch)?;
    if source.version != 1
        || source.family.as_str() != "filing_xbrl"
        || &source.dataset != capture.dataset()
        || source.filing.cik != expected_cik
        || source.numeric_fact_count != observations.len()
        || source.numeric_fact_count != native_rows.len()
        || source.total_retained_bytes == 0
        || source.ingested_at < source.received_at
        || source
            .availability
            .conservative_available_at()
            .is_none_or(|available| available > source.received_at)
        || source.filing.accepted_at.is_some() != source.filing.acceptance_evidence.is_some()
        || source
            .filing
            .accepted_at
            .is_some_and(|accepted| accepted > source.received_at)
        || source.contexts.len() > source.nonnumeric_occurrences.len()
    {
        return Err(SecResearchReadError::ProviderBindingMismatch);
    }
    let complete = SecVerifiedFilingXbrl {
        source,
        sidecar_digest,
    };
    let document_locator = format!(
        "https://www.sec.gov/Archives/edgar/data/{}/{}/{}",
        expected_cik.trim_start_matches('0'),
        complete.accession().as_str().replace('-', ""),
        complete.document()
    );
    if capture.terminal()
        != market_squawk_sources::ProviderCaptureTerminalDisposition::CompleteRequestGraph
        || capture
            .request_graph_components()
            .get(1)
            .is_none_or(|component| component.dataset().as_str() != document_locator)
    {
        return Err(SecResearchReadError::ProviderBindingMismatch);
    }
    let directory = tempfile::Builder::new()
        .prefix("market-squawk-sec-validation-")
        .tempdir_in(scratch.path())
        .map_err(|_| SecResearchReadError::ObjectBudgetExceeded)?;
    let index = Connection::open(directory.path().join("graph.sqlite3"))?;
    index.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE; PRAGMA mmap_size=0; PRAGMA cache_size=-1024; CREATE TABLE occurrences(id TEXT PRIMARY KEY,footnote INTEGER NOT NULL); CREATE TABLE contexts(id TEXT PRIMARY KEY,digest BLOB NOT NULL,used INTEGER NOT NULL); CREATE TABLE numeric_contexts(id TEXT PRIMARY KEY,digest BLOB NOT NULL)")?;
    let mut disk = super::indexed::IndexAllocation::new(Arc::clone(&scratch));
    disk.update(&index)?;
    let mut previous = None;
    for context in complete.contexts().iter() {
        check_operation(deadline, cancellation)?;
        disk.update(&index)?;
        let context = context?;
        if context.entity.scheme().as_str() != "http://www.sec.gov/CIK"
            || context.entity.value().as_str() != expected_cik
            || context.dimensions.len() > market_squawk_domain::MAX_XBRL_DIMENSIONS
            || previous
                .as_ref()
                .is_some_and(|id| id >= &context.context_id)
        {
            return Err(SecResearchReadError::ProviderBindingMismatch);
        }
        index.execute(
            "INSERT INTO contexts VALUES (?1,?2,0)",
            params![context.context_id.as_str(), context_digest(&context)?],
        )?;
        previous = Some(context.context_id);
    }
    disk.update(&index)?;
    let mut payload = None;
    for (observation, retained_row) in observations.iter().zip(native_rows.iter()) {
        check_operation(deadline, cancellation)?;
        disk.update(&index)?;
        let observation = observation?;
        let retained_row = retained_row?;
        let ResearchObservation::Fundamental(fact) = &observation else {
            return Err(SecResearchReadError::ProviderBindingMismatch);
        };
        let evidence = fact
            .xbrl_evidence()
            .ok_or(SecResearchReadError::ProviderBindingMismatch)?;
        if retained_row.semantic_payload.as_slice().len() > 2 * SourceIdentifier::MAX_LENGTH + 128 {
            return Err(SecResearchReadError::ProviderBindingMismatch);
        }
        let native: NumericNativeRow =
            serde_json::from_slice(retained_row.semantic_payload.as_slice())
                .map_err(|_| SecResearchReadError::ProviderBindingMismatch)?;
        let provenance = observation_context(&observation).provenance();
        if native.family.as_str() != "numeric_fact"
            || &native.occurrence_id != evidence.occurrence_id()
            || evidence.accession() != complete.accession()
            || evidence.entity().scheme().as_str() != "http://www.sec.gov/CIK"
            || evidence.entity().value().as_str() != expected_cik
            || evidence.taxonomy_set().version() != &complete.source.taxonomy.version
            || evidence.taxonomy_set().digest() != complete.source.taxonomy.artifact_set
            || evidence.evaluated_at() != complete.received_at()
            || retained_row.capture_page_ordinal != 1
            || capture
                .pages()
                .get(1)
                .is_none_or(|page| page.body_digest() != evidence.source_payload().content_digest())
            || provenance.received_at() != complete.received_at()
            || provenance.ingested_at() != complete.ingested_at()
            || provenance.availability() != complete.availability()
            || payload.is_some_and(|value| value != evidence.source_payload().content_digest())
        {
            return Err(SecResearchReadError::ProviderBindingMismatch);
        }
        payload = Some(evidence.source_payload().content_digest());
        let context = SecFilingXbrlContext {
            context_id: evidence.context_id().clone(),
            entity: evidence.entity().clone(),
            period: evidence.period(),
            dimensions: evidence.dimensions().to_vec(),
            context_graph: evidence.context_graph().clone(),
        };
        let digest = context_digest(&context)?;
        let prior: Option<Vec<u8>> = index
            .query_row(
                "SELECT digest FROM numeric_contexts WHERE id=?1",
                [context.context_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        if prior.as_ref().is_some_and(|prior| prior != &digest) {
            return Err(SecResearchReadError::ProviderBindingMismatch);
        }
        let native_context: Option<Vec<u8>> = index
            .query_row(
                "SELECT digest FROM contexts WHERE id=?1",
                [context.context_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        if native_context
            .as_ref()
            .is_some_and(|prior| prior != &digest)
        {
            return Err(SecResearchReadError::ProviderBindingMismatch);
        }
        index.execute(
            "INSERT OR IGNORE INTO numeric_contexts VALUES (?1,?2)",
            params![context.context_id.as_str(), digest],
        )?;
        index.execute(
            "INSERT INTO occurrences VALUES (?1,0)",
            [evidence.occurrence_id().as_str()],
        )?;
    }
    for occurrence in complete.nonnumeric_occurrences().iter() {
        check_operation(deadline, cancellation)?;
        disk.update(&index)?;
        let occurrence = occurrence?;
        if occurrence.accession() != complete.accession()
            || payload != Some(occurrence.source_payload().content_digest())
            || occurrence.occurrence_relationships().parent_occurrence_id()
                == Some(occurrence.occurrence_id())
            || occurrence
                .occurrence_relationships()
                .child_occurrence_ids()
                .contains(occurrence.occurrence_id())
        {
            return Err(SecResearchReadError::ProviderBindingMismatch);
        }
        if index.execute(
            "UPDATE contexts SET used=1 WHERE id=?1",
            [occurrence.context_id().as_str()],
        )? != 1
        {
            return Err(SecResearchReadError::ProviderBindingMismatch);
        }
        index.execute(
            "INSERT INTO occurrences VALUES (?1,0)",
            [occurrence.occurrence_id().as_str()],
        )?;
    }
    for footnote in complete.footnotes().iter() {
        check_operation(deadline, cancellation)?;
        disk.update(&index)?;
        let footnote = footnote?;
        if footnote.accession() != complete.accession()
            || footnote.language().as_str().is_empty()
            || payload != Some(footnote.source_payload().content_digest())
            || footnote.occurrence_relationships().parent_occurrence_id()
                == Some(footnote.occurrence_id())
            || footnote
                .occurrence_relationships()
                .child_occurrence_ids()
                .contains(footnote.occurrence_id())
        {
            return Err(SecResearchReadError::ProviderBindingMismatch);
        }
        index.execute(
            "INSERT INTO occurrences VALUES (?1,1)",
            [footnote.occurrence_id().as_str()],
        )?;
    }
    if index.query_row(
        "SELECT EXISTS(SELECT 1 FROM contexts WHERE used=0)",
        [],
        |row| row.get::<_, bool>(0),
    )? {
        return Err(SecResearchReadError::ProviderBindingMismatch);
    }
    disk.update(&index)?;
    let validate = |graph: &XbrlOccurrenceRelationships| -> Result<(), SecResearchReadError> {
        for relationship in graph.relationships() {
            check_operation(deadline, cancellation)?;
            for source in relationship.from_refs() {
                let kind: Option<bool> = index
                    .query_row(
                        "SELECT footnote FROM occurrences WHERE id=?1",
                        [source.as_str()],
                        |row| row.get(0),
                    )
                    .optional()?;
                if kind != Some(false) {
                    return Err(SecResearchReadError::ProviderBindingMismatch);
                }
            }
            let mut target_kind = None;
            for target in relationship.to_refs() {
                let kind: Option<bool> = index
                    .query_row(
                        "SELECT footnote FROM occurrences WHERE id=?1",
                        [target.as_str()],
                        |row| row.get(0),
                    )
                    .optional()?;
                if kind.is_none()
                    || relationship.from_refs().contains(target)
                    || target_kind.is_some_and(|previous| Some(previous) != kind)
                {
                    return Err(SecResearchReadError::ProviderBindingMismatch);
                }
                target_kind = kind;
            }
        }
        Ok(())
    };
    for observation in observations.iter() {
        check_operation(deadline, cancellation)?;
        let observation = observation?;
        let ResearchObservation::Fundamental(fact) = &observation else {
            return Err(SecResearchReadError::ProviderBindingMismatch);
        };
        validate(
            fact.xbrl_evidence()
                .ok_or(SecResearchReadError::ProviderBindingMismatch)?
                .occurrence_relationships(),
        )?;
    }
    for occurrence in complete.nonnumeric_occurrences().iter() {
        validate(occurrence?.occurrence_relationships())?;
    }
    for footnote in complete.footnotes().iter() {
        validate(footnote?.occurrence_relationships())?;
    }
    check_operation(deadline, cancellation)?;
    disk.update(&index)?;
    Ok((complete, retained_bytes))
}
fn context_digest(context: &SecFilingXbrlContext) -> Result<Vec<u8>, SecResearchReadError> {
    let bytes =
        serde_json::to_vec(context).map_err(|_| SecResearchReadError::ProviderBindingMismatch)?;
    Ok(Sha256::digest(bytes).to_vec())
}

/// Metadata is derived only from a fully authenticated filing. The complete original graph
/// remains in the retained provider binding; row indexes preserve all exposed occurrences.
#[derive(Serialize, Deserialize)]
struct PreparedFiling {
    version: u16,
    family: SourceIdentifier,
    dataset: SourceIdentifier,
    filing: FilingCoordinates,
    taxonomy: TaxonomyCoordinates,
    availability: AvailabilityEvidence,
    received_at: Timestamp,
    ingested_at: Timestamp,
    total_retained_bytes: u64,
    numeric_fact_count: usize,
    sidecar_digest: EvidenceDigest,
}
impl SecVerifiedFilingXbrl {
    pub(super) fn persist_into(
        &self,
        connection: &Connection,
        deadline: std::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SecResearchReadError> {
        self.source
            .contexts
            .persist_into(connection, "filing_contexts", deadline, cancellation)?;
        self.source.nonnumeric_occurrences.persist_into(
            connection,
            "filing_nonnumeric",
            deadline,
            cancellation,
        )?;
        self.source.footnotes.persist_into(
            connection,
            "filing_footnotes",
            deadline,
            cancellation,
        )?;
        let value = PreparedFiling {
            version: self.source.version,
            family: self.source.family.clone(),
            dataset: self.source.dataset.clone(),
            filing: self.source.filing.clone(),
            taxonomy: self.source.taxonomy.clone(),
            availability: self.source.availability.clone(),
            received_at: self.source.received_at,
            ingested_at: self.source.ingested_at,
            total_retained_bytes: self.source.total_retained_bytes,
            numeric_fact_count: self.source.numeric_fact_count,
            sidecar_digest: self.sidecar_digest,
        };
        let bytes = serde_json::to_vec(&value).map_err(|_| SecResearchReadError::DigestEncoding)?;
        connection.execute(
            "INSERT INTO metadata(name,payload) VALUES('filing',?1)",
            [bytes],
        )?;
        Ok(())
    }
    pub(super) fn reopen(
        artifact: Arc<super::prepared::PreparedArtifact>,
        connection: Arc<std::sync::Mutex<Connection>>,
    ) -> Result<Option<Self>, SecResearchReadError> {
        let bytes: Option<Vec<u8>> = connection
            .lock()
            .map_err(|_| SecResearchReadError::AuthorityUnavailable)?
            .query_row(
                "SELECT payload FROM metadata WHERE name='filing'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let value: PreparedFiling =
            serde_json::from_slice(&bytes).map_err(|_| SecResearchReadError::PreparedIntegrity)?;
        let contexts = SecResearchRows::reopen(
            Arc::clone(&artifact),
            Arc::clone(&connection),
            "filing_contexts",
        )?;
        let nonnumeric_occurrences = SecResearchRows::reopen(
            Arc::clone(&artifact),
            Arc::clone(&connection),
            "filing_nonnumeric",
        )?;
        let footnotes = SecResearchRows::reopen(artifact, connection, "filing_footnotes")?;
        Ok(Some(Self {
            source: FilingSidecar {
                version: value.version,
                family: value.family,
                dataset: value.dataset,
                filing: value.filing,
                taxonomy: value.taxonomy,
                availability: value.availability,
                received_at: value.received_at,
                ingested_at: value.ingested_at,
                total_retained_bytes: value.total_retained_bytes,
                numeric_fact_count: value.numeric_fact_count,
                contexts,
                nonnumeric_occurrences,
                footnotes,
            },
            sidecar_digest: value.sidecar_digest,
        }))
    }
}
