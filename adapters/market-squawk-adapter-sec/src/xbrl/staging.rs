//! Filing-local indexed scratch. Only completed, validated output leaves this owner.

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Serialize, de::DeserializeOwned};

use super::*;

/// SQLite's page cache is bounded independently of the number of filing occurrences. The
/// directory and its journal are deleted on cancellation/error as well as normal completion.
#[derive(Debug)]
pub(crate) struct FilingIndex {
    connection: Connection,
    _directory: tempfile::TempDir,
    cancellation: CancellationToken,
    maximum_record_bytes: usize,
    largest_record_capacity: std::cell::Cell<usize>,
}

impl FilingIndex {
    pub(super) fn new(
        working_bytes: usize,
        cancellation: &CancellationToken,
        scratch_parent: Option<&std::path::Path>,
    ) -> Result<Self, SecXbrlError> {
        let maximum_record_bytes = working_bytes
            .checked_sub(4 * 1024 * 1024)
            .map(|bytes| bytes / 8)
            .filter(|bytes| *bytes > 0)
            .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?;
        let mut builder = tempfile::Builder::new();
        builder.prefix("market-squawk-xbrl-");
        let directory = match scratch_parent {
            Some(parent) => builder.tempdir_in(parent)?,
            None => builder.tempdir()?,
        };
        let connection = Connection::open(directory.path().join("filing.sqlite3"))?;
        let sql_cancellation = cancellation.clone();
        connection.progress_handler(4096, Some(move || sql_cancellation.is_cancelled()))?;
        connection.execute_batch(
            "PRAGMA journal_mode=DELETE; PRAGMA synchronous=OFF; PRAGMA cache_size=-2048;
             PRAGMA temp_store=FILE; PRAGMA mmap_size=0;
             CREATE TABLE objects(kind TEXT NOT NULL,id TEXT NOT NULL,ordinal INTEGER NOT NULL,
               payload BLOB NOT NULL,parent TEXT,context TEXT,unit TEXT,semantic BLOB,family BLOB,
               PRIMARY KEY(kind,id), UNIQUE(kind,ordinal));
             CREATE INDEX children ON objects(kind,parent,ordinal);
             CREATE INDEX semantic_groups ON objects(kind,semantic,ordinal);
             CREATE INDEX family_groups ON objects(kind,family,ordinal);
             CREATE TABLE nonnumeric_contexts(ordinal INTEGER PRIMARY KEY,representative INTEGER NOT NULL);
             CREATE TABLE family_ordinals(ordinal INTEGER PRIMARY KEY,value INTEGER NOT NULL);
             CREATE TABLE continuation_refs(id TEXT PRIMARY KEY);
             CREATE TABLE edges(relationship INTEGER NOT NULL,direction INTEGER NOT NULL,id TEXT NOT NULL,
               PRIMARY KEY(relationship,direction,id));
             CREATE INDEX incident ON edges(id,relationship);
             CREATE TABLE duplicate_groups(semantic BLOB PRIMARY KEY,classification BLOB NOT NULL,count INTEGER NOT NULL);
             CREATE TABLE accuracy_values(accuracy TEXT PRIMARY KEY,value TEXT NOT NULL);
             BEGIN IMMEDIATE;",
        )?;
        Ok(Self {
            connection,
            _directory: directory,
            cancellation: cancellation.clone(),
            maximum_record_bytes,
            largest_record_capacity: std::cell::Cell::new(0),
        })
    }

    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, SecXbrlError> {
        let mut writer = RecordWriter {
            bytes: Vec::new(),
            maximum: self.maximum_record_bytes,
        };
        serde_json::to_writer(&mut writer, value)?;
        self.largest_record_capacity.set(
            self.largest_record_capacity
                .get()
                .max(writer.bytes.capacity()),
        );
        Ok(writer.bytes)
    }
    /// Bounds the SQLite cache and one serialized/decoded/normalized record transition.
    /// This reservation is independent of the number of records retained on disk.
    pub(super) fn working_set_bytes(&self) -> Result<usize, SecXbrlError> {
        self.largest_record_capacity
            .get()
            .checked_mul(8)
            .and_then(|bytes| bytes.checked_add(4 * 1024 * 1024))
            .ok_or(SecXbrlError::RetainedOutputLimitExceeded)
    }
    pub(super) fn seal(&self, cancellation: &CancellationToken) -> Result<(), SecXbrlError> {
        // Final domain construction validates every numeric occurrence before the index becomes
        // a capability. A late invalid fact cannot produce a successful partial document.
        for ordinal in 0..self.count("numeric")? {
            check_xbrl_cancelled(cancellation)?;
            self.numeric_at(ordinal)?
                .ok_or(SecXbrlError::ParserInvariant)?;
        }
        self.connection
            .execute_batch("COMMIT; PRAGMA query_only=ON;")?;
        Ok(())
    }
    pub(super) fn insert<T: Serialize>(
        &self,
        kind: &str,
        id: &str,
        ordinal: usize,
        value: &T,
    ) -> Result<(), SecXbrlError> {
        if self.contains(kind, id)? {
            return Err(SecXbrlError::DuplicateIdentity);
        }
        self.connection.execute(
            "INSERT INTO objects(kind,id,ordinal,payload) VALUES(?1,?2,?3,?4)",
            params![
                kind,
                id,
                i64::try_from(ordinal).map_err(|_| SecXbrlError::RecordLimitExceeded)?,
                self.encode(value)?
            ],
        )?;
        Ok(())
    }

    pub(super) fn count(&self, kind: &str) -> Result<usize, SecXbrlError> {
        check_xbrl_cancelled(&self.cancellation)?;
        let count: i64 = self.connection.query_row(
            "SELECT COALESCE(max(ordinal)+1,0) FROM objects WHERE kind=?1",
            [kind],
            |row| row.get(0),
        )?;
        usize::try_from(count).map_err(|_| SecXbrlError::RecordLimitExceeded)
    }
    pub(super) fn contains(&self, kind: &str, id: &str) -> Result<bool, SecXbrlError> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM objects WHERE kind=?1 AND id=?2)",
            params![kind, id],
            |row| row.get(0),
        )?)
    }
    pub(super) fn get<T: DeserializeOwned>(
        &self,
        kind: &str,
        id: &str,
    ) -> Result<Option<T>, SecXbrlError> {
        check_xbrl_cancelled(&self.cancellation)?;
        let value = self
            .connection
            .query_row(
                "SELECT payload FROM objects WHERE kind=?1 AND id=?2",
                params![kind, id],
                |row| Ok(serde_json::from_slice::<T>(row.get_ref(0)?.as_blob()?)),
            )
            .optional()?;
        check_xbrl_cancelled(&self.cancellation)?;
        value.transpose().map_err(Into::into)
    }
    pub(crate) fn at<T: DeserializeOwned>(
        &self,
        kind: &str,
        ordinal: usize,
    ) -> Result<Option<T>, SecXbrlError> {
        check_xbrl_cancelled(&self.cancellation)?;
        let value = self
            .connection
            .query_row(
                "SELECT payload FROM objects WHERE kind=?1 AND ordinal=?2",
                params![
                    kind,
                    i64::try_from(ordinal).map_err(|_| SecXbrlError::RecordLimitExceeded)?
                ],
                |row| Ok(serde_json::from_slice::<T>(row.get_ref(0)?.as_blob()?)),
            )
            .optional()?;
        check_xbrl_cancelled(&self.cancellation)?;
        value.transpose().map_err(Into::into)
    }
    pub(super) fn reference(&self, id: &str) -> Result<(), SecXbrlError> {
        let inserted = self.connection.execute(
            "INSERT OR IGNORE INTO continuation_refs(id) VALUES(?1)",
            [id],
        )?;
        if inserted != 1 {
            return Err(SecXbrlError::ReusedContinuation);
        }
        Ok(())
    }
    pub(super) fn fact(&self, fact: &FactDraft) -> Result<(), SecXbrlError> {
        self.insert("fact", &fact.occurrence_id, self.count("fact")?, fact)?;
        self.connection.execute(
            "UPDATE objects SET parent=?1,context=?2,unit=?3 WHERE kind='fact' AND id=?4",
            params![
                fact.parent_occurrence_id,
                fact.context_id,
                fact.unit_id,
                fact.occurrence_id
            ],
        )?;
        Ok(())
    }
    pub(super) fn relationship(&self, relationship: RelationshipDraft) -> Result<(), SecXbrlError> {
        let ordinal = self.count("relationship")?;
        let evidence = relationship.into_evidence()?;
        self.insert("relationship", &ordinal.to_string(), ordinal, &evidence)?;
        for (direction, refs) in [(0, evidence.from_refs()), (1, evidence.to_refs())] {
            for id in refs {
                self.connection.execute(
                    "INSERT INTO edges(relationship,direction,id) VALUES(?1,?2,?3)",
                    params![
                        i64::try_from(ordinal).map_err(|_| SecXbrlError::RecordLimitExceeded)?,
                        direction,
                        id.as_str()
                    ],
                )?;
            }
        }
        Ok(())
    }
    pub(super) fn incident(&self, id: &str) -> Result<Vec<XbrlRelationshipEvidence>, SecXbrlError> {
        let mut statement=self.connection.prepare("SELECT payload FROM objects WHERE kind='relationship' AND ordinal IN (SELECT relationship FROM edges WHERE id=?1) ORDER BY ordinal")?;
        let mut rows = statement.query([id])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            if result.len() >= market_squawk_domain::MAX_XBRL_RELATIONSHIPS {
                return Err(market_squawk_domain::XbrlEvidenceError::TooManyRelationships.into());
            }
            result.push(serde_json::from_slice(
                row.get_ref(0)?.as_blob().map_err(rusqlite::Error::from)?,
            )?);
        }
        Ok(result)
    }
    pub(super) fn children(&self, id: &str) -> Result<Vec<SourceIdentifier>, SecXbrlError> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM objects WHERE kind='fact' AND parent=?1 ORDER BY ordinal")?;
        let mut rows = statement.query([id])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            result.push(SourceIdentifier::try_from(id)?);
        }
        Ok(result)
    }
    pub(super) fn numeric(&self, draft: &NormalizedDraft) -> Result<(), SecXbrlError> {
        let NormalizedDraft::Numeric { evidence_input, .. } = draft else {
            return Err(SecXbrlError::ParserInvariant);
        };
        let ordinal = self.count("numeric")?;
        self.insert(
            "numeric",
            evidence_input.occurrence_id.as_str(),
            ordinal,
            draft,
        )?;
        let NormalizedDraft::Numeric { concept, unit, .. } = draft else {
            return Err(SecXbrlError::ParserInvariant);
        };
        let family = serde_json::to_vec(&(concept, unit, evidence_input.period))?;
        self.connection.execute(
            "UPDATE objects SET semantic=?1,family=?2 WHERE kind='numeric' AND ordinal=?3",
            params![
                semantic_aspect_digest(evidence_input).as_slice(),
                family,
                i64::try_from(ordinal).map_err(|_| SecXbrlError::RecordLimitExceeded)?
            ],
        )?;
        Ok(())
    }
    pub(super) fn nonnumeric(&self, value: XbrlNonnumericOccurrence) -> Result<(), SecXbrlError> {
        let ordinal = self.count("nonnumeric")?;
        let record = StagedNonnumeric {
            occurrence_id: value.occurrence_id,
            accession: value.accession,
            concept: value.concept,
            context_id: value.context_id,
            lexical_value: value.lexical_value,
            nil: value.nil,
            source_payload: value.source_payload,
            occurrence_relationships: value.occurrence_relationships,
        };
        self.insert(
            "nonnumeric",
            record.occurrence_id.as_str(),
            ordinal,
            &record,
        )?;
        self.connection.execute(
            "UPDATE objects SET context=?1 WHERE kind='nonnumeric' AND ordinal=?2",
            params![
                record.context_id.as_str(),
                i64::try_from(ordinal).map_err(|_| SecXbrlError::RecordLimitExceeded)?
            ],
        )?;
        Ok(())
    }
    pub(crate) fn nonnumeric_at(
        &self,
        ordinal: usize,
    ) -> Result<Option<XbrlNonnumericOccurrence>, SecXbrlError> {
        let Some(record): Option<StagedNonnumeric> = self.at("nonnumeric", ordinal)? else {
            return Ok(None);
        };
        let context: ContextDraft = self
            .get("context", record.context_id.as_str())?
            .ok_or(SecXbrlError::UnknownContext)?;
        Ok(Some(XbrlNonnumericOccurrence {
            occurrence_id: record.occurrence_id,
            accession: record.accession,
            concept: record.concept,
            context_id: record.context_id,
            lexical_value: record.lexical_value,
            nil: record.nil,
            source_payload: record.source_payload,
            occurrence_relationships: record.occurrence_relationships,
            context: context.occurrence_context()?,
        }))
    }
    pub(crate) fn family_ordinal_at(&self, ordinal: usize) -> Result<u32, SecXbrlError> {
        check_xbrl_cancelled(&self.cancellation)?;
        let value: i64 = self.connection.query_row(
            "SELECT value FROM family_ordinals WHERE ordinal=?1",
            [i64::try_from(ordinal).map_err(|_| SecXbrlError::RecordLimitExceeded)?],
            |row| row.get(0),
        )?;
        u32::try_from(value).map_err(|_| SecXbrlError::ParserInvariant)
    }
    pub(crate) fn nonnumeric_context_at(
        &self,
        ordinal: usize,
    ) -> Result<Option<XbrlNonnumericOccurrence>, SecXbrlError> {
        check_xbrl_cancelled(&self.cancellation)?;
        let representative: Option<i64> = self
            .connection
            .query_row(
                "SELECT representative FROM nonnumeric_contexts WHERE ordinal=?1",
                [i64::try_from(ordinal).map_err(|_| SecXbrlError::RecordLimitExceeded)?],
                |row| row.get(0),
            )
            .optional()?;
        representative
            .map(|ordinal| {
                let ordinal =
                    usize::try_from(ordinal).map_err(|_| SecXbrlError::ParserInvariant)?;
                self.nonnumeric_at(ordinal)?
                    .ok_or(SecXbrlError::ParserInvariant)
            })
            .transpose()
    }
    pub(crate) fn context_count(&self) -> Result<usize, SecXbrlError> {
        check_xbrl_cancelled(&self.cancellation)?;
        let count: i64 =
            self.connection
                .query_row("SELECT count(*) FROM nonnumeric_contexts", [], |row| {
                    row.get(0)
                })?;
        usize::try_from(count).map_err(|_| SecXbrlError::ParserInvariant)
    }
    pub(super) fn classify(&self, cancellation: &CancellationToken) -> Result<(), SecXbrlError> {
        self.connection.execute_batch("INSERT INTO family_ordinals(ordinal,value) SELECT ordinal,row_number() OVER (PARTITION BY family ORDER BY ordinal) FROM objects WHERE kind='numeric';
            INSERT INTO nonnumeric_contexts(ordinal,representative) SELECT row_number() OVER (ORDER BY context)-1,min(ordinal) FROM objects WHERE kind='nonnumeric' GROUP BY context;")?;
        let mut groups=self.connection.prepare("SELECT semantic,count(*) FROM objects WHERE kind='numeric' GROUP BY semantic ORDER BY semantic")?;
        let mut groups = groups.query([])?;
        while let Some(group) = groups.next()? {
            check_xbrl_cancelled(cancellation)?;
            let key: Vec<u8> = group.get(0)?;
            let count: i64 = group.get(1)?;
            let mut classification = XbrlDuplicateClass::Unique;
            if count > 1 {
                self.connection.execute("DELETE FROM accuracy_values", [])?;
                let mut statement=self.connection.prepare("SELECT payload FROM objects WHERE kind='numeric' AND semantic=?1 ORDER BY ordinal")?;
                let mut rows = statement.query([&key])?;
                let mut interval: Option<(Decimal, Decimal)> = None;
                classification = XbrlDuplicateClass::ConsistentNumeric;
                while let Some(row) = rows.next()? {
                    check_xbrl_cancelled(cancellation)?;
                    let NormalizedDraft::Numeric {
                        value,
                        evidence_input,
                        ..
                    } = serde_json::from_slice(
                        row.get_ref(0)?.as_blob().map_err(rusqlite::Error::from)?,
                    )?
                    else {
                        return Err(SecXbrlError::ParserInvariant);
                    };
                    let accuracy =
                        format!("{:?}", effective_accuracy(value, evidence_input.accuracy));
                    let prior: Option<String> = self
                        .connection
                        .query_row(
                            "SELECT value FROM accuracy_values WHERE accuracy=?1",
                            [&accuracy],
                            |row| row.get(0),
                        )
                        .optional()?;
                    if prior.is_some_and(|prior| prior != value.normalize().to_string()) {
                        classification = XbrlDuplicateClass::Inconsistent;
                        break;
                    }
                    self.connection.execute(
                        "INSERT OR IGNORE INTO accuracy_values(accuracy,value) VALUES(?1,?2)",
                        params![accuracy, value.normalize().to_string()],
                    )?;
                    let Some((lower, upper)) = accuracy_interval(value, evidence_input.accuracy)
                    else {
                        classification = XbrlDuplicateClass::Unclassified;
                        break;
                    };
                    interval = Some(match interval {
                        None => (lower, upper),
                        Some((lo, hi)) => (lo.max(lower), hi.min(upper)),
                    });
                }
                if classification == XbrlDuplicateClass::ConsistentNumeric
                    && interval.is_none_or(|(lo, hi)| lo > hi)
                {
                    classification = XbrlDuplicateClass::Inconsistent;
                }
            }
            self.connection.execute(
                "INSERT INTO duplicate_groups(semantic,classification,count) VALUES(?1,?2,?3)",
                params![key, serde_json::to_vec(&classification)?, count],
            )?;
        }
        Ok(())
    }
    pub(crate) fn numeric_at(
        &self,
        ordinal: usize,
    ) -> Result<Option<XbrlNumericFact>, SecXbrlError> {
        let Some(draft) = self.at("numeric", ordinal)? else {
            return Ok(None);
        };
        let NormalizedDraft::Numeric {
            concept,
            unit,
            value,
            mut evidence_input,
        } = draft
        else {
            return Err(SecXbrlError::ParserInvariant);
        };
        let digest = semantic_aspect_digest(&evidence_input);
        let (classification, count): (Vec<u8>, i64) = self.connection.query_row(
            "SELECT classification,count FROM duplicate_groups WHERE semantic=?1",
            [digest.as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let group = (count > 1)
            .then(|| {
                SourceIdentifier::try_from(format!("xbrl-duplicate-{}", hex_prefix(&digest, 16)))
            })
            .transpose()?;
        evidence_input.duplicate = XbrlDuplicateEvidence::try_new(
            serde_json::from_slice(&classification)?,
            group,
            SourceIdentifier::try_from("sec-xbrl-duplicate-v2")?,
        )?;
        let evidence = XbrlFactEvidence::try_new(*evidence_input)?;
        evidence.validate_value(value)?;
        Ok(Some(XbrlNumericFact {
            concept,
            unit,
            value,
            evidence,
        }))
    }
}

/// Complete validated filing, with one-record reads instead of a corpus-sized owning vector.
#[derive(Debug)]
pub(crate) struct IndexedXbrlDocument {
    pub(crate) index: FilingIndex,
    pub(crate) document: XbrlDocumentContext,
    pub(crate) numeric_count: usize,
    pub(crate) nonnumeric_count: usize,
    pub(crate) footnote_count: usize,
    pub(crate) peak_retained_bytes: usize,
}

impl IndexedXbrlDocument {
    pub(crate) fn family_ordinal_at(&self, ordinal: usize) -> Result<u32, SecXbrlError> {
        self.index.family_ordinal_at(ordinal)
    }
    pub(crate) fn nonnumeric_context_at(
        &self,
        ordinal: usize,
    ) -> Result<Option<XbrlNonnumericOccurrence>, SecXbrlError> {
        self.index.nonnumeric_context_at(ordinal)
    }
    pub(crate) fn context_count(&self) -> Result<usize, SecXbrlError> {
        self.index.context_count()
    }
    pub(crate) const fn evaluated_at(&self) -> market_squawk_domain::Timestamp {
        self.document.evaluated_at
    }
    pub(crate) fn matches_document_context(
        &self,
        accession: &SourceIdentifier,
        expected_cik: &SourceIdentifier,
        taxonomy_set: &SecValidatedXbrlTaxonomySet,
        source_payload: market_squawk_domain::EvidenceDigest,
    ) -> bool {
        &self.document.accession == accession
            && self.document.expected_cik.as_ref() == Some(expected_cik)
            && self.document.taxonomy_set == taxonomy_set.domain_set()
            && self.document.source_payload.content_digest() == source_payload
    }
    pub(crate) fn numeric_at(
        &self,
        ordinal: usize,
    ) -> Result<Option<XbrlNumericFact>, SecXbrlError> {
        self.index.numeric_at(ordinal)
    }
    pub(crate) fn nonnumeric_at(
        &self,
        ordinal: usize,
    ) -> Result<Option<XbrlNonnumericOccurrence>, SecXbrlError> {
        self.index.nonnumeric_at(ordinal)
    }
    pub(crate) fn footnote_at(
        &self,
        ordinal: usize,
    ) -> Result<Option<XbrlFootnoteOccurrence>, SecXbrlError> {
        self.index.at("footnote_output", ordinal)
    }
    pub(super) fn materialize(
        self,
        cancellation: &CancellationToken,
    ) -> Result<ParsedXbrlDocument, SecXbrlError> {
        let mut numeric_facts = Vec::new();
        let mut nonnumeric_occurrences = Vec::new();
        let mut footnotes = Vec::new();
        let mut contexts =
            BTreeMap::<SourceIdentifier, std::sync::Arc<model::XbrlOccurrenceContext>>::new();
        for ordinal in 0..self.numeric_count {
            check_xbrl_cancelled(cancellation)?;
            numeric_facts.push(
                self.numeric_at(ordinal)?
                    .ok_or(SecXbrlError::ParserInvariant)?,
            );
        }
        for ordinal in 0..self.nonnumeric_count {
            check_xbrl_cancelled(cancellation)?;
            let mut occurrence = self
                .nonnumeric_at(ordinal)?
                .ok_or(SecXbrlError::ParserInvariant)?;
            if let Some(context) = contexts.get(occurrence.context_id()) {
                if context.as_ref() != occurrence.context.as_ref() {
                    return Err(SecXbrlError::ParserInvariant);
                }
                occurrence.context = std::sync::Arc::clone(context);
            } else {
                contexts.insert(
                    occurrence.context_id().clone(),
                    std::sync::Arc::clone(&occurrence.context),
                );
            }
            nonnumeric_occurrences.push(occurrence);
        }
        for ordinal in 0..self.footnote_count {
            check_xbrl_cancelled(cancellation)?;
            footnotes.push(
                self.footnote_at(ordinal)?
                    .ok_or(SecXbrlError::ParserInvariant)?,
            );
        }
        Ok(ParsedXbrlDocument {
            accession: self.document.accession,
            expected_cik: self.document.expected_cik,
            taxonomy_set: self.document.taxonomy_set,
            source_payload: self.document.source_payload,
            evaluated_at: self.document.evaluated_at,
            numeric_facts,
            nonnumeric_occurrences,
            footnotes,
        })
    }
}

struct RecordWriter {
    bytes: Vec<u8>,
    maximum: usize,
}
impl std::io::Write for RecordWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|next| *next <= self.maximum)
            .ok_or_else(|| std::io::Error::other("XBRL staging record exceeds working memory"))?;
        if next > self.bytes.capacity() {
            let capacity = next
                .max(self.bytes.capacity().saturating_mul(2))
                .min(self.maximum);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(std::io::Error::other)?;
            if self.bytes.capacity() > self.maximum {
                return Err(std::io::Error::other(
                    "XBRL staging record allocation exceeded admission",
                ));
            }
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StagedNonnumeric {
    occurrence_id: SourceIdentifier,
    accession: SourceIdentifier,
    concept: XbrlQualifiedName,
    context_id: SourceIdentifier,
    lexical_value: XbrlText,
    nil: bool,
    source_payload: market_squawk_domain::ExactPayloadEvidence,
    occurrence_relationships: XbrlOccurrenceRelationships,
}
