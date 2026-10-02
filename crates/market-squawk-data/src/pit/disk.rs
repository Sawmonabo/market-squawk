//! Ordered PIT staging and immutable prepared candidates. Canonical decisions cover the input;
//! only consumer-requested selected evidence is decoded into the working set.

use std::io::{self, Write};
use std::mem::size_of;
use std::time::Instant;

use market_squawk_domain::{ResearchObservation, RevisionNumber};
use rusqlite::{Connection, params};
use tokio_util::sync::CancellationToken;

use super::canonical::{self, CanonicalEncoder, map_error};
use super::retained::{OperationControl, RetainedBudget};
use super::select::TemporalAdmission;
use super::{
    PointInTimeCandidate, PointInTimeConflictCounts, PointInTimeError, PointInTimeExclusionCounts,
    PointInTimeExclusionReasons, PointInTimeRequest, PointInTimeRevisionCounts,
    PointInTimeRevisionMode, PointInTimeRevisionState,
};
use crate::{DatasetManifestRef, Sha256Digest};

type Result<T> = std::result::Result<T, PointInTimeError<'static>>;

/// Bump when persisted candidate metadata or its derivation semantics change.
pub(crate) const PREPARED_CANDIDATE_VERSION: u32 = 1;

const CANDIDATES_SCHEMA: &str = "CREATE TABLE candidates(id INTEGER PRIMARY KEY, family BLOB NOT NULL, revision INTEGER NOT NULL,
             family_identity BLOB NOT NULL, payload BLOB NOT NULL, provenance BLOB NOT NULL,
             evidence BLOB NOT NULL, source INTEGER NOT NULL, observation BLOB NOT NULL, admission BLOB NOT NULL);
             CREATE INDEX canonical_order ON candidates(family,revision,payload,evidence,id);
             CREATE INDEX native_fact ON candidates(source,payload);
";
const DECISIONS_SCHEMA: &str = "             CREATE TABLE states(id INTEGER PRIMARY KEY,reasons INTEGER NOT NULL,state INTEGER NOT NULL);
             CREATE TABLE groups(family BLOB NOT NULL,revision INTEGER NOT NULL,variants INTEGER NOT NULL,
                 eligible INTEGER NOT NULL,first_id INTEGER NOT NULL,PRIMARY KEY(family,revision)) WITHOUT ROWID;
             CREATE TABLE winners(family BLOB PRIMARY KEY,revision INTEGER NOT NULL) WITHOUT ROWID;
             CREATE TABLE decisions(id INTEGER PRIMARY KEY,reasons INTEGER NOT NULL,state INTEGER NOT NULL,decision INTEGER NOT NULL);";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecisionDisposition {
    Selected,
    Excluded(PointInTimeExclusionReasons),
    Conflict,
}

/// Exact identities already computed from admitted observations during append/selection.
/// Decision-only consumers do not need another decoded copy of the source payload.
pub(crate) struct DecisionRecord {
    pub(crate) family_identity: Sha256Digest,
    pub(crate) payload_identity: Sha256Digest,
    pub(crate) provenance_identity: Sha256Digest,
    pub(crate) evidence_identity: Sha256Digest,
    pub(crate) revision: RevisionNumber,
    pub(crate) revision_state: PointInTimeRevisionState,
}

/// Source payloads and canonical identities live on disk, staged once or prepared durably.
/// SQLite's B-tree supplies canonical byte ordering without a history-sized Rust sort.
pub(crate) struct CandidateStore {
    connection: Connection,
    // Drop the connection before the common crash-reclaimable operation owner.
    scratch: Option<crate::parquet_store::OperationScratchDirectory>,
    manifests: Vec<DatasetManifestRef>,
    count: usize,
    working_bytes: usize,
    cancellation: CancellationToken,
    deadline: Instant,
    usable: bool,
    has_selection: bool,
}

pub(crate) struct SelectedRecord {
    candidate: PointInTimeCandidate,
    payload: Sha256Digest,
    evidence: Sha256Digest,
}

impl SelectedRecord {
    pub(crate) const fn candidate(&self) -> &PointInTimeCandidate {
        &self.candidate
    }
    pub(crate) const fn evidence_identity(&self) -> Sha256Digest {
        self.evidence
    }
    pub(crate) const fn payload_identity(&self) -> Sha256Digest {
        self.payload
    }
}

/// Complete identities and counts, with only explicitly requested selected records in RAM.
/// Unretained records still participate in revision conflicts and both canonical hashes.
pub(crate) struct Selection {
    records: Vec<SelectedRecord>,
    content: Sha256Digest,
    audit: Sha256Digest,
    retained_bytes: usize,
    exclusions: PointInTimeExclusionCounts,
    revisions: PointInTimeRevisionCounts,
}

impl Selection {
    pub(crate) fn records(&self) -> &[SelectedRecord] {
        &self.records
    }
    pub(crate) const fn content_identity(&self) -> Sha256Digest {
        self.content
    }
    pub(crate) const fn audit_identity(&self) -> Sha256Digest {
        self.audit
    }
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    pub(crate) const fn exclusion_counts(&self) -> PointInTimeExclusionCounts {
        self.exclusions
    }
    pub(crate) const fn revision_counts(&self) -> PointInTimeRevisionCounts {
        self.revisions
    }
}

impl CandidateStore {
    pub(crate) fn new(
        scratch: crate::parquet_store::OperationScratchDirectory,
        working_bytes: usize,
        disk_bytes: u64,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<Self> {
        Self::open(scratch, working_bytes, disk_bytes, cancellation, deadline)
    }

    #[cfg(test)]
    pub(super) fn for_test(
        working_bytes: usize,
        disk_bytes: u64,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<Self> {
        let scratch = crate::parquet_store::OperationScratchDirectory::for_test()
            .map_err(|_| PointInTimeError::ScratchStorage)?;
        Self::open(scratch, working_bytes, disk_bytes, cancellation, deadline)
    }

    fn open(
        scratch: crate::parquet_store::OperationScratchDirectory,
        working_bytes: usize,
        disk_bytes: u64,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<Self> {
        OperationControl::new(cancellation, deadline)?;
        if working_bytes < 64 * 1024 || disk_bytes < 4096 {
            return Err(PointInTimeError::InvalidLimits);
        }
        let connection =
            Connection::open(scratch.path().join("candidates.sqlite3")).map_err(storage)?;
        connection
            .execute_batch(
                "PRAGMA page_size=4096; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;",
            )
            .map_err(storage)?;
        connection
            .pragma_update(
                None,
                "max_page_count",
                i64::try_from(disk_bytes / 4096)
                    .map_err(|_| PointInTimeError::AccountingOverflow)?,
            )
            .map_err(storage)?;
        connection
            .execute_batch(CANDIDATES_SCHEMA)
            .map_err(storage)?;
        Self::configure_selection(
            &connection,
            working_bytes,
            disk_bytes,
            cancellation,
            deadline,
            false,
        )?;
        Ok(Self {
            connection,
            scratch: Some(scratch),
            manifests: Vec::new(),
            count: 0,
            working_bytes,
            cancellation: cancellation.clone(),
            deadline,
            usable: true,
            has_selection: false,
        })
    }

    fn configure_selection(
        connection: &Connection,
        working_bytes: usize,
        disk_bytes: u64,
        cancellation: &CancellationToken,
        deadline: Instant,
        prepared: bool,
    ) -> Result<()> {
        OperationControl::new(cancellation, deadline)?;
        if working_bytes < 64 * 1024 || disk_bytes < 4096 {
            return Err(PointInTimeError::InvalidLimits);
        }
        let token = cancellation.clone();
        connection
            .progress_handler(
                1024,
                Some(move || token.is_cancelled() || Instant::now() >= deadline),
            )
            .map_err(storage)?;
        connection
            .execute_batch("PRAGMA temp_store=FILE; PRAGMA mmap_size=0;")
            .map_err(storage)?;
        connection
            .pragma_update(
                None,
                "cache_size",
                -(i64::try_from((working_bytes / 8).clamp(8192, 2 * 1024 * 1024) / 1024)
                    .map_err(|_| PointInTimeError::AccountingOverflow)?),
            )
            .map_err(storage)?;
        connection
            .set_limit(
                rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH,
                i32::try_from(working_bytes / 4).unwrap_or(i32::MAX),
            )
            .map_err(storage)?;
        if prepared {
            connection
                .pragma_update(Some("temp"), "page_size", 4096)
                .map_err(storage)?;
            connection
                .pragma_update(
                    Some("temp"),
                    "cache_size",
                    -(i64::try_from((working_bytes / 8).clamp(8192, 2 * 1024 * 1024) / 1024)
                        .map_err(|_| PointInTimeError::AccountingOverflow)?),
                )
                .map_err(storage)?;
            connection
                .pragma_update(
                    Some("temp"),
                    "max_page_count",
                    i64::try_from(disk_bytes / 4096)
                        .map_err(|_| PointInTimeError::AccountingOverflow)?,
                )
                .map_err(storage)?;
            connection
                .execute_batch(&DECISIONS_SCHEMA.replace("CREATE TABLE", "CREATE TEMP TABLE"))
                .map_err(storage)?;
        } else {
            connection
                .execute_batch(DECISIONS_SCHEMA)
                .map_err(storage)?;
        }
        Ok(())
    }

    /// Copies admitted candidates and their identities once during controlled publication.
    /// The destination remains unpublished until its owner validates and authenticates it.
    pub(crate) fn export_prepared(&self, destination: &Connection) -> Result<()> {
        if !self.usable {
            return Err(PointInTimeError::ScratchStorage);
        }
        let mut control = OperationControl::new(&self.cancellation, self.deadline)?;
        destination
            .execute_batch("SAVEPOINT pit_export")
            .map_err(storage)?;
        let result = (|| {
            destination
                .execute_batch(CANDIDATES_SCHEMA)
                .map_err(storage)?;
            destination.execute_batch(
                "CREATE TABLE pit_prepared(version INTEGER NOT NULL,candidate_count INTEGER NOT NULL);
                 CREATE TABLE pit_sources(source INTEGER PRIMARY KEY,manifest BLOB NOT NULL);"
            ).map_err(storage)?;
            destination
                .execute(
                    "INSERT INTO pit_prepared VALUES(?,?)",
                    params![PREPARED_CANDIDATE_VERSION, sql_integer(self.count)?],
                )
                .map_err(storage)?;
            for (source, manifest) in self.manifests.iter().enumerate() {
                control.observe()?;
                let bytes = encode_manifest(manifest, self.working_bytes / 4)?;
                destination
                    .execute(
                        "INSERT INTO pit_sources VALUES(?,?)",
                        params![sql_integer(source)?, bytes],
                    )
                    .map_err(storage)?;
            }
            let mut read = self.connection.prepare(
                "SELECT id,family,revision,family_identity,payload,provenance,evidence,source,observation,admission FROM candidates ORDER BY id"
            ).map_err(storage)?;
            let mut rows = read.query([]).map_err(storage)?;
            let mut insert = destination
                .prepare("INSERT INTO candidates VALUES(?,?,?,?,?,?,?,?,?,?)")
                .map_err(storage)?;
            while let Some(row) = rows.next().map_err(storage)? {
                control.observe()?;
                insert
                    .execute(params![
                        rusqlite::types::ToSqlOutput::Borrowed(row.get_ref(0).map_err(storage)?),
                        rusqlite::types::ToSqlOutput::Borrowed(row.get_ref(1).map_err(storage)?),
                        rusqlite::types::ToSqlOutput::Borrowed(row.get_ref(2).map_err(storage)?),
                        rusqlite::types::ToSqlOutput::Borrowed(row.get_ref(3).map_err(storage)?),
                        rusqlite::types::ToSqlOutput::Borrowed(row.get_ref(4).map_err(storage)?),
                        rusqlite::types::ToSqlOutput::Borrowed(row.get_ref(5).map_err(storage)?),
                        rusqlite::types::ToSqlOutput::Borrowed(row.get_ref(6).map_err(storage)?),
                        rusqlite::types::ToSqlOutput::Borrowed(row.get_ref(7).map_err(storage)?),
                        rusqlite::types::ToSqlOutput::Borrowed(row.get_ref(8).map_err(storage)?),
                        rusqlite::types::ToSqlOutput::Borrowed(row.get_ref(9).map_err(storage)?)
                    ])
                    .map_err(storage)?;
            }
            control.check_now()
        })();
        if result.is_ok() {
            destination
                .execute_batch("RELEASE pit_export")
                .map_err(storage)?;
        } else {
            destination
                .execute_batch("ROLLBACK TO pit_export; RELEASE pit_export")
                .map_err(storage)?;
        }
        result
    }

    /// Reuses a previously authenticated immutable candidate table. The caller must retain
    /// its controlled-artifact guard for this store's lifetime and verify origin/version.
    /// Main may be read-only; query_only must remain off so TEMP decisions can be written.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_prepared(
        connection: Connection,
        manifests: Vec<DatasetManifestRef>,
        count: usize,
        working_bytes: usize,
        disk_bytes: u64,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<Self> {
        Self::configure_selection(
            &connection,
            working_bytes,
            disk_bytes,
            cancellation,
            deadline,
            true,
        )?;
        let mut control = OperationControl::new(cancellation, deadline)?;
        {
            let mut metadata = connection
                .prepare("SELECT version,candidate_count FROM pit_prepared")
                .map_err(storage)?;
            let mut rows = metadata.query([]).map_err(storage)?;
            let row = rows
                .next()
                .map_err(storage)?
                .ok_or(PointInTimeError::CanonicalEncoding)?;
            if row.get::<_, u32>(0).map_err(storage)? != PREPARED_CANDIDATE_VERSION
                || read_usize(row, 1).map_err(storage)? != count
                || rows.next().map_err(storage)?.is_some()
            {
                return Err(PointInTimeError::CanonicalEncoding);
            }
            let mut sources = connection
                .prepare("SELECT source,manifest FROM pit_sources ORDER BY source")
                .map_err(storage)?;
            let mut rows = sources.query([]).map_err(storage)?;
            for (source, manifest) in manifests.iter().enumerate() {
                control.observe()?;
                let row = rows
                    .next()
                    .map_err(storage)?
                    .ok_or(PointInTimeError::CanonicalEncoding)?;
                let expected = encode_manifest(manifest, working_bytes / 4)?;
                if read_usize(row, 0).map_err(storage)? != source
                    || row
                        .get_ref(1)
                        .map_err(storage)?
                        .as_blob()
                        .map_err(|_| PointInTimeError::CanonicalEncoding)?
                        != expected
                {
                    return Err(PointInTimeError::CanonicalEncoding);
                }
            }
            if rows.next().map_err(storage)?.is_some() {
                return Err(PointInTimeError::CanonicalEncoding);
            }
        }
        control.check_now()?;
        Ok(Self {
            connection,
            scratch: None,
            manifests,
            count,
            working_bytes,
            cancellation: cancellation.clone(),
            deadline,
            usable: true,
            has_selection: false,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.count
    }

    /// Visits every decision in canonical audit order. The ordinal is the original zero-based
    /// append position. Exclusions and conflicts remain available until the next selection.
    pub(crate) fn visit_decisions(
        &self,
        mut visit: impl FnMut(usize, DecisionRecord, DecisionDisposition) -> Result<()>,
    ) -> Result<()> {
        if !self.usable || !self.has_selection {
            return Err(PointInTimeError::ScratchStorage);
        }
        let mut control = OperationControl::new(&self.cancellation, self.deadline)?;
        let mut statement = self.connection.prepare(
            "SELECT c.id,c.source,length(c.observation),c.family_identity,c.payload,c.provenance,c.evidence,
             d.state,d.decision,d.reasons,c.revision FROM candidates c INDEXED BY canonical_order
             JOIN decisions d ON d.id=c.id ORDER BY c.family,c.revision,c.payload,c.evidence,c.id"
        ).map_err(storage)?;
        let mut rows = statement.query([]).map_err(storage)?;
        while let Some(row) = rows.next().map_err(storage)? {
            control.observe()?;
            let source = read_usize(row, 1).map_err(storage)?;
            self.manifests
                .get(source)
                .ok_or(PointInTimeError::CanonicalEncoding)?;
            let bytes = read_usize(row, 2).map_err(storage)?;
            if bytes > self.working_bytes / 4 {
                return Err(PointInTimeError::RetainedBytesExceeded {
                    limit: self.working_bytes,
                    observed: bytes,
                });
            }
            let record = DecisionRecord {
                revision: RevisionNumber::new(row.get(10).map_err(storage)?)
                    .map_err(|_| PointInTimeError::CanonicalEncoding)?,
                family_identity: read_digest(row, 3)?,
                payload_identity: read_digest(row, 4)?,
                provenance_identity: read_digest(row, 5)?,
                evidence_identity: read_digest(row, 6)?,
                revision_state: read_state(row.get(7).map_err(storage)?)?,
            };
            let disposition = match row.get::<_, u8>(8).map_err(storage)? {
                1 => DecisionDisposition::Selected,
                2 => DecisionDisposition::Excluded(PointInTimeExclusionReasons::from_bits(
                    row.get(9).map_err(storage)?,
                )),
                3 => DecisionDisposition::Conflict,
                _ => return Err(PointInTimeError::CanonicalEncoding),
            };
            let ordinal = read_usize(row, 0)
                .map_err(storage)?
                .checked_sub(1)
                .filter(|ordinal| *ordinal < self.count)
                .ok_or(PointInTimeError::CanonicalEncoding)?;
            visit(ordinal, record, disposition)?;
        }
        control.check_now()
    }

    /// Each admitted batch is consumed before the next read; source manifests are shared once.
    pub(crate) fn append(
        &mut self,
        observations: Vec<ResearchObservation>,
        manifest: &DatasetManifestRef,
    ) -> Result<()> {
        if !self.usable || self.scratch.is_none() {
            return Err(PointInTimeError::ScratchStorage);
        }
        self.usable = false;
        self.has_selection = false;
        let source = if let Some(index) = self.manifests.iter().position(|value| value == manifest)
        {
            index
        } else {
            self.manifests
                .try_reserve(1)
                .map_err(|_| PointInTimeError::AllocationFailure)?;
            self.manifests.push(manifest.clone());
            self.manifests.len() - 1
        };
        let mut control = OperationControl::new(&self.cancellation, self.deadline)?;
        let transaction = self.connection.transaction().map_err(storage)?;
        let mut count = self.count;
        {
            let mut insert = transaction.prepare("INSERT INTO candidates(family,revision,family_identity,payload,provenance,evidence,source,observation,admission) VALUES(?,?,?,?,?,?,?,?,?)").map_err(storage)?;
            for observation in observations {
                control.observe()?;
                let candidate = PointInTimeCandidate::new(observation, manifest.clone());
                let mut budget = RetainedBudget::new(self.working_bytes);
                let family = canonical::family_encoding(&candidate, &mut control, &mut budget)?;
                let payload = canonical::payload_identity(&candidate, &mut control)?;
                let provenance = canonical::provenance_identity(&candidate, &mut control)?;
                let evidence = canonical::evidence_identity(
                    &candidate,
                    family.identity,
                    payload,
                    provenance,
                    &mut control,
                )?;
                let temporal = TemporalAdmission::from_observation(candidate.observation());
                let admission = encode_bounded(&temporal, self.working_bytes / 4)?;
                let bytes = encode_bounded(candidate.observation(), self.working_bytes / 4)?;
                if bytes.len() > self.working_bytes / 4 {
                    return Err(PointInTimeError::RetainedBytesExceeded {
                        limit: self.working_bytes,
                        observed: bytes.len(),
                    });
                }
                insert
                    .execute(params![
                        family.bytes,
                        candidate.revision().get(),
                        family.identity.bytes().as_slice(),
                        payload.bytes().as_slice(),
                        provenance.bytes().as_slice(),
                        evidence.bytes().as_slice(),
                        sql_integer(source)?,
                        bytes,
                        admission
                    ])
                    .map_err(storage)?;
                count = count
                    .checked_add(1)
                    .ok_or(PointInTimeError::AccountingOverflow)?;
            }
        }
        transaction.commit().map_err(storage)?;
        self.count = count;
        control.check_now()?;
        self.usable = true;
        Ok(())
    }

    /// Revalidate exact native source facts without reloading the whole source generation.
    pub(crate) fn count_observation(
        &self,
        observation: &ResearchObservation,
        manifest: &DatasetManifestRef,
    ) -> Result<usize> {
        if !self.usable {
            return Err(PointInTimeError::ScratchStorage);
        }
        let Some(source) = self.manifests.iter().position(|value| value == manifest) else {
            return Ok(0);
        };
        let bytes = encode_bounded(observation, self.working_bytes / 4)?;
        let mut control = OperationControl::new(&self.cancellation, self.deadline)?;
        let candidate = PointInTimeCandidate::new(observation.clone(), manifest.clone());
        let payload = canonical::payload_identity(&candidate, &mut control)?;
        self.connection
            .query_row(
                "SELECT count(*) FROM candidates WHERE source=? AND payload=? AND observation=?",
                params![sql_integer(source)?, payload.bytes().as_slice(), bytes],
                |row| read_usize(row, 0),
            )
            .map_err(storage)
    }

    pub(crate) fn select(
        &mut self,
        request: &PointInTimeRequest,
        mut retain: impl FnMut(&PointInTimeCandidate) -> bool,
    ) -> Result<Selection> {
        self.select_with_retention(request, Some(&mut retain), None)
    }

    /// Computes all decisions and identities without decoding selected observations.
    pub(crate) fn select_decisions(&mut self, request: &PointInTimeRequest) -> Result<Selection> {
        self.select_with_retention(request, None, None)
    }

    /// Source-level knowledge admission precedes PIT classification and hashes. The callback
    /// receives original zero-based append ordinals, including those it excludes.
    pub(crate) fn select_decisions_filtered(
        &mut self,
        request: &PointInTimeRequest,
        mut admit: impl FnMut(usize) -> Result<bool>,
    ) -> Result<Selection> {
        self.select_with_retention(request, None, Some(&mut admit))
    }

    fn select_with_retention(
        &mut self,
        request: &PointInTimeRequest,
        retain: Option<&mut dyn FnMut(&PointInTimeCandidate) -> bool>,
        admit: Option<&mut dyn FnMut(usize) -> Result<bool>>,
    ) -> Result<Selection> {
        if !self.usable {
            return Err(PointInTimeError::ScratchStorage);
        }
        let mut control = OperationControl::new(&self.cancellation, self.deadline)?;
        if admit.is_none() && self.count > request.limits().max_candidates() {
            return Err(PointInTimeError::CandidateLimitExceeded {
                limit: request.limits().max_candidates(),
                observed: self.count,
            });
        }
        self.usable = false;
        self.has_selection = false;
        self.connection.execute_batch("BEGIN; DELETE FROM decisions; DELETE FROM winners; DELETE FROM groups; DELETE FROM states;").map_err(storage)?;
        let result = self.select_inner(request, retain, admit, &mut control);
        if result.is_ok() || matches!(result, Err(PointInTimeError::DiskRevisionConflicts { .. })) {
            self.connection.execute_batch("COMMIT").map_err(storage)?;
            self.usable = true;
            self.has_selection = true;
        } else {
            // Any incomplete decision write poisons this per-read owner. Closing it discards
            // the operation scratch or TEMP state without modifying a prepared source.
        }
        control.check_now()?;
        result
    }

    fn select_inner(
        &self,
        request: &PointInTimeRequest,
        retain: Option<&mut dyn FnMut(&PointInTimeCandidate) -> bool>,
        mut admit: Option<&mut dyn FnMut(usize) -> Result<bool>>,
        control: &mut OperationControl,
    ) -> Result<Selection> {
        let mut admitted_count = 0_usize;
        // Source-level eligibility defines the request's input. Preserve every admitted
        // candidate in the PIT audit, including all temporal exclusions and conflicts.
        {
            let mut read = self
                .connection
                .prepare("SELECT id,admission FROM candidates ORDER BY id")
                .map_err(storage)?;
            let mut rows = read.query([]).map_err(storage)?;
            let mut insert = self
                .connection
                .prepare("INSERT INTO states VALUES(?,?,?)")
                .map_err(storage)?;
            while let Some(row) = rows.next().map_err(storage)? {
                control.observe()?;
                let ordinal = read_usize(row, 0)
                    .map_err(storage)?
                    .checked_sub(1)
                    .filter(|ordinal| *ordinal < self.count)
                    .ok_or(PointInTimeError::CanonicalEncoding)?;
                if let Some(admit) = &mut admit {
                    if !admit(ordinal)? {
                        continue;
                    }
                }
                admitted_count = admitted_count
                    .checked_add(1)
                    .ok_or(PointInTimeError::AccountingOverflow)?;
                if admitted_count > request.limits().max_candidates() {
                    return Err(PointInTimeError::CandidateLimitExceeded {
                        limit: request.limits().max_candidates(),
                        observed: admitted_count,
                    });
                }
                let raw = row.get_ref(1).map_err(storage)?;
                let bytes = raw
                    .as_blob()
                    .map_err(|_| PointInTimeError::CanonicalEncoding)?;
                if bytes.len() > self.working_bytes / 4 {
                    return Err(PointInTimeError::RetainedBytesExceeded {
                        limit: self.working_bytes,
                        observed: bytes.len(),
                    });
                }
                let temporal: TemporalAdmission = serde_json::from_slice(bytes)
                    .map_err(|_| PointInTimeError::CanonicalEncoding)?;
                let (reasons, state) = super::select::admission(request, &temporal);
                insert
                    .execute(params![
                        row.get::<_, i64>(0).map_err(storage)?,
                        reasons.bits(),
                        state_tag(state)
                    ])
                    .map_err(storage)?;
            }
        }
        // Only eligible payload variants conflict. Order is identical to the pure selector,
        // including the original ordinal as tie-breaker for identical canonical identities.
        {
            let mut read = self.connection.prepare("SELECT c.id,c.family,c.revision,c.payload FROM candidates c INDEXED BY canonical_order JOIN states s ON s.id=c.id WHERE s.reasons=0 ORDER BY c.family,c.revision,c.payload,c.evidence,c.id").map_err(storage)?;
            let mut rows = read.query([]).map_err(storage)?;
            let mut group: Option<Group> = None;
            while let Some(row) = rows.next().map_err(storage)? {
                control.observe()?;
                let family: Vec<u8> = row.get(1).map_err(storage)?;
                let revision: u32 = row.get(2).map_err(storage)?;
                let payload: Vec<u8> = row.get(3).map_err(storage)?;
                if group
                    .as_ref()
                    .is_some_and(|g| g.family != family || g.revision != revision)
                {
                    self.insert_group(group.take().ok_or(PointInTimeError::CanonicalEncoding)?)?;
                }
                if let Some(group) = &mut group {
                    group.eligible = group
                        .eligible
                        .checked_add(1)
                        .ok_or(PointInTimeError::AccountingOverflow)?;
                    if group.last_payload != payload {
                        group.variants = group
                            .variants
                            .checked_add(1)
                            .ok_or(PointInTimeError::AccountingOverflow)?;
                        group.last_payload = payload;
                    }
                } else {
                    group = Some(Group {
                        family,
                        revision,
                        last_payload: payload,
                        variants: 1,
                        eligible: 1,
                        first_id: row.get(0).map_err(storage)?,
                    });
                }
            }
            if let Some(group) = group {
                self.insert_group(group)?;
            }
        }
        let families: usize = self
            .connection
            .query_row(
                "SELECT count(*) FROM (SELECT c.family FROM candidates c JOIN states s ON s.id=c.id GROUP BY c.family)",
                [],
                |row| read_usize(row, 0),
            )
            .map_err(storage)?;
        if families > request.limits().max_families() {
            return Err(PointInTimeError::FamilyLimitExceeded {
                limit: request.limits().max_families(),
                observed: families,
            });
        }
        self.connection.execute("INSERT INTO winners SELECT family,max(revision) FROM groups WHERE variants=1 GROUP BY family", []).map_err(storage)?;
        let latest = request.policy().revision_mode() == PointInTimeRevisionMode::LatestKnown;
        self.connection.execute(
            "INSERT INTO decisions SELECT c.id,
             CASE WHEN s.reasons<>0 THEN s.reasons WHEN g.variants>1 THEN 0
                  WHEN ? AND c.revision<>w.revision THEN 4096 WHEN c.id<>g.first_id THEN 8192 ELSE 0 END,
             s.state, CASE WHEN s.reasons<>0 THEN 2 WHEN g.variants>1 THEN 3
                  WHEN ? AND c.revision<>w.revision THEN 2 WHEN c.id<>g.first_id THEN 2 ELSE 1 END
             FROM candidates c JOIN states s ON s.id=c.id LEFT JOIN groups g ON g.family=c.family AND g.revision=c.revision
             LEFT JOIN winners w ON w.family=c.family", params![latest, latest]).map_err(storage)?;
        let conflicts = self.connection.query_row("SELECT count(*),coalesce(sum(eligible),0),coalesce(sum(variants),0) FROM groups WHERE variants>1", [], |row| Ok(PointInTimeConflictCounts { conflicting_groups: read_usize(row,0)?, conflicting_candidates: read_usize(row,1)?, payload_variants: read_usize(row,2)? })).map_err(storage)?;
        if conflicts.conflicting_groups() > request.limits().max_conflicts() {
            return Err(PointInTimeError::ConflictLimitExceeded {
                limit: request.limits().max_conflicts(),
                observed: conflicts.conflicting_groups(),
            });
        }
        let selected_count: usize = self
            .connection
            .query_row(
                "SELECT count(*) FROM decisions WHERE decision=1",
                [],
                |row| read_usize(row, 0),
            )
            .map_err(storage)?;
        if selected_count > request.limits().max_result_rows() {
            return Err(PointInTimeError::ResultRowLimitExceeded {
                limit: request.limits().max_result_rows(),
                observed: selected_count,
            });
        }
        let mut exclusions = PointInTimeExclusionCounts::default();
        let mut revisions = PointInTimeRevisionCounts::default();
        let audit = {
            let mut encoder =
                CanonicalEncoder::new(canonical::AUDIT_DOMAIN, control).map_err(map_error)?;
            canonical::encode_request(&mut encoder, request).map_err(map_error)?;
            encoder
                .u64(
                    u64::try_from(admitted_count)
                        .map_err(|_| PointInTimeError::AccountingOverflow)?,
                )
                .map_err(map_error)?;
            let mut read = self.connection.prepare("SELECT c.evidence,d.decision,d.reasons,d.state FROM candidates c INDEXED BY canonical_order JOIN decisions d ON d.id=c.id ORDER BY c.family,c.revision,c.payload,c.evidence,c.id").map_err(storage)?;
            let mut rows = read.query([]).map_err(storage)?;
            while let Some(row) = rows.next().map_err(storage)? {
                encoder.digest(read_digest(row, 0)?).map_err(map_error)?;
                let decision: u8 = row.get(1).map_err(storage)?;
                encoder.u8(decision).map_err(map_error)?;
                match decision {
                    1 => {
                        let state = read_state(row.get(3).map_err(storage)?)?;
                        revisions.record(state);
                        canonical::encode_revision_state(&mut encoder, state).map_err(map_error)?;
                    }
                    2 => {
                        let reasons =
                            PointInTimeExclusionReasons::from_bits(row.get(2).map_err(storage)?);
                        exclusions.record(reasons);
                        encoder.u16(reasons.bits()).map_err(map_error)?;
                    }
                    3 => {}
                    _ => return Err(PointInTimeError::CanonicalEncoding),
                }
            }
            for count in exclusions
                .counts()
                .into_iter()
                .chain(revisions.values())
                .chain([
                    conflicts.conflicting_groups(),
                    conflicts.conflicting_candidates(),
                    conflicts.payload_variants(),
                ])
            {
                encoder
                    .u64(u64::try_from(count).map_err(|_| PointInTimeError::AccountingOverflow)?)
                    .map_err(map_error)?;
            }
            encoder.finish()
        };
        if conflicts.conflicting_groups() != 0 {
            return Err(PointInTimeError::DiskRevisionConflicts {
                counts: conflicts,
                audit_identity: audit,
            });
        }
        let content = {
            let mut encoder =
                CanonicalEncoder::new(canonical::CONTENT_DOMAIN, control).map_err(map_error)?;
            canonical::encode_request(&mut encoder, request).map_err(map_error)?;
            encoder
                .u64(
                    u64::try_from(selected_count)
                        .map_err(|_| PointInTimeError::AccountingOverflow)?,
                )
                .map_err(map_error)?;
            let mut read = self.connection.prepare("SELECT c.family_identity,c.revision,c.payload,d.state FROM candidates c INDEXED BY canonical_order JOIN decisions d ON d.id=c.id WHERE d.decision=1 ORDER BY c.family,c.revision,c.payload,c.evidence,c.id").map_err(storage)?;
            let mut rows = read.query([]).map_err(storage)?;
            while let Some(row) = rows.next().map_err(storage)? {
                encoder.digest(read_digest(row, 0)?).map_err(map_error)?;
                encoder
                    .u32(row.get(1).map_err(storage)?)
                    .map_err(map_error)?;
                encoder.digest(read_digest(row, 2)?).map_err(map_error)?;
                canonical::encode_revision_state(
                    &mut encoder,
                    read_state(row.get(3).map_err(storage)?)?,
                )
                .map_err(map_error)?;
            }
            encoder.finish()
        };
        let mut records = Vec::new();
        let mut retained_bytes = size_of::<Selection>();
        if let Some(retain) = retain {
            let mut read = self.connection.prepare("SELECT c.source,c.observation,c.family_identity,c.payload,c.provenance,c.evidence,d.state FROM candidates c INDEXED BY canonical_order JOIN decisions d ON d.id=c.id WHERE d.decision=1 ORDER BY c.family,c.revision,c.payload,c.evidence,c.id").map_err(storage)?;
            let mut rows = read.query([]).map_err(storage)?;
            while let Some(row) = rows.next().map_err(storage)? {
                control.observe()?;
                let candidate = self.decode(row, 0, 1)?;
                if !retain(&candidate) {
                    continue;
                }
                let encoded_bytes = row
                    .get_ref(1)
                    .map_err(storage)?
                    .as_blob()
                    .map_err(|_| PointInTimeError::CanonicalEncoding)?
                    .len();
                let charge = encoded_bytes
                    .checked_mul(4)
                    .and_then(|bytes| bytes.checked_add(size_of::<SelectedRecord>()))
                    .ok_or(PointInTimeError::AccountingOverflow)?;
                retained_bytes = retained_bytes
                    .checked_add(charge)
                    .ok_or(PointInTimeError::AccountingOverflow)?;
                if retained_bytes > request.limits().max_retained_bytes() {
                    return Err(PointInTimeError::RetainedBytesExceeded {
                        limit: request.limits().max_retained_bytes(),
                        observed: retained_bytes,
                    });
                }
                records
                    .try_reserve_exact(1)
                    .map_err(|_| PointInTimeError::AllocationFailure)?;
                records.push(SelectedRecord {
                    candidate,
                    payload: read_digest(row, 3)?,
                    evidence: read_digest(row, 5)?,
                });
            }
        }
        control.check_now()?;
        Ok(Selection {
            records,
            content,
            audit,
            retained_bytes,
            exclusions,
            revisions,
        })
    }

    fn insert_group(&self, group: Group) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO groups VALUES(?,?,?,?,?)",
                params![
                    group.family,
                    group.revision,
                    sql_integer(group.variants)?,
                    sql_integer(group.eligible)?,
                    group.first_id
                ],
            )
            .map_err(storage)?;
        Ok(())
    }

    fn decode(
        &self,
        row: &rusqlite::Row<'_>,
        source_column: usize,
        observation_column: usize,
    ) -> Result<PointInTimeCandidate> {
        let source = read_usize(row, source_column).map_err(storage)?;
        let manifest = self
            .manifests
            .get(source)
            .ok_or(PointInTimeError::CanonicalEncoding)?;
        let raw = row.get_ref(observation_column).map_err(storage)?;
        let bytes = raw
            .as_blob()
            .map_err(|_| PointInTimeError::CanonicalEncoding)?;
        if bytes.len() > self.working_bytes / 4 {
            return Err(PointInTimeError::RetainedBytesExceeded {
                limit: self.working_bytes,
                observed: bytes.len(),
            });
        }
        let observation =
            serde_json::from_slice(bytes).map_err(|_| PointInTimeError::CanonicalEncoding)?;
        Ok(PointInTimeCandidate::new(observation, manifest.clone()))
    }
}

struct Group {
    family: Vec<u8>,
    revision: u32,
    last_payload: Vec<u8>,
    variants: usize,
    eligible: usize,
    first_id: i64,
}
fn storage(error: rusqlite::Error) -> PointInTimeError<'static> {
    if matches!(error, rusqlite::Error::SqliteFailure(ref failure, _) if failure.code == rusqlite::ErrorCode::DiskFull)
    {
        PointInTimeError::ScratchDiskExhausted
    } else {
        PointInTimeError::ScratchStorage
    }
}
fn read_digest(row: &rusqlite::Row<'_>, column: usize) -> Result<Sha256Digest> {
    let raw = row.get_ref(column).map_err(storage)?;
    let bytes = raw
        .as_blob()
        .map_err(|_| PointInTimeError::CanonicalEncoding)?;
    Ok(Sha256Digest::new(
        bytes
            .try_into()
            .map_err(|_| PointInTimeError::CanonicalEncoding)?,
    ))
}
const fn state_tag(state: PointInTimeRevisionState) -> u8 {
    match state {
        PointInTimeRevisionState::Current => 0,
        PointInTimeRevisionState::Superseded => 1,
        PointInTimeRevisionState::SupersessionIncomparable => 2,
    }
}
fn read_state(value: u8) -> Result<PointInTimeRevisionState> {
    match value {
        0 => Ok(PointInTimeRevisionState::Current),
        1 => Ok(PointInTimeRevisionState::Superseded),
        2 => Ok(PointInTimeRevisionState::SupersessionIncomparable),
        _ => Err(PointInTimeError::CanonicalEncoding),
    }
}

fn encode_manifest(manifest: &DatasetManifestRef, maximum: usize) -> Result<Vec<u8>> {
    encode_bounded(
        &(
            manifest.dataset_id().as_str(),
            manifest.manifest_version(),
            manifest.schema().name(),
            manifest.schema().version().get(),
            manifest.schema().fingerprint(),
            manifest.content_hash().bytes(),
        ),
        maximum,
    )
}

fn encode_bounded(value: &impl serde::Serialize, maximum: usize) -> Result<Vec<u8>> {
    struct BoundedJson {
        bytes: Vec<u8>,
        maximum: usize,
    }
    impl Write for BoundedJson {
        fn write(&mut self, value: &[u8]) -> io::Result<usize> {
            let required = self
                .bytes
                .len()
                .checked_add(value.len())
                .filter(|required| *required <= self.maximum)
                .ok_or_else(|| io::Error::other("point-in-time row exceeds its working set"))?;
            self.bytes
                .try_reserve_exact(required - self.bytes.len())
                .map_err(|_| io::Error::other("point-in-time row allocation failed"))?;
            self.bytes.extend_from_slice(value);
            Ok(value.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = BoundedJson {
        bytes: Vec::new(),
        maximum,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| {
        PointInTimeError::RetainedBytesExceeded {
            limit: maximum,
            observed: maximum.saturating_add(1),
        }
    })?;
    Ok(writer.bytes)
}

fn sql_integer(value: usize) -> Result<i64> {
    i64::try_from(value).map_err(|_| PointInTimeError::AccountingOverflow)
}
fn read_usize(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<usize> {
    let value: i64 = row.get(column)?;
    usize::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}
