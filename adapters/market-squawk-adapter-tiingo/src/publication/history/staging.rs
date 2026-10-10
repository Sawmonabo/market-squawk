//! Disk-backed complete history projection. Only a single bounded native page is decoded at once.
use super::*;
use crate::{
    TiingoEodActionFieldDisposition, TiingoEodDailyActionDisposition,
    TiingoEodExpectedSessionRequest, TiingoEodNormalizedAction, TiingoHistoryPlan,
    TiingoVerifiedHistoryTerminal,
};
use market_squawk_domain::{
    EffectiveInterval, InstrumentId, MetadataRevision, ProviderInstrumentId, SourceId, VenueId,
};
use market_squawk_platform::SealedResearchJournalSegmentClaim;
use market_squawk_sources::{
    CanonicalObservationPayload, DiscoveryRequest, ProviderCapturePackAccumulator,
    ProviderNativeLineageBatch, SourceObject,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, de::DeserializeOwned};
use std::{
    io::Write,
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    path::Path,
};
use tokio_util::sync::CancellationToken;

/// Compact evidence; all unbounded row collections are independently framed indexed objects.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TiingoEodHistoryDescriptor {
    instrument_id: InstrumentId,
    instrument_revision_digest: EvidenceDigest,
    admitted_plan_digest: EvidenceDigest,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
    interval: SourceIdentifier,
    graph_purpose: SourceIdentifier,
    requested_dates: (CalendarDate, CalendarDate),
    source_id: SourceId,
    normalization: RetainedMarketHistoryNormalizationV1,
    completeness_evidence: EvidenceDigest,
    request_set_identity: EvidenceDigest,
    capture_pack_identity: EvidenceDigest,
    page_count: usize,
    session_count: usize,
    date_digest: EvidenceDigest,
    raw_count: usize,
    all_count: usize,
    raw_digest: Option<EvidenceDigest>,
    all_digest: Option<EvidenceDigest>,
    source_action_count: usize,
    normalized_action_count: usize,
    ordinary_fields_complete: bool,
    total_response_bytes: u64,
    total_canonical_rows: u64,
    checkpoint_receipt_identity: EvidenceDigest,
    first_received_at: Timestamp,
    max_received_at: Timestamp,
    max_ingested_at: Timestamp,
}
impl TiingoEodHistoryDescriptor {
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub const fn instrument_revision_digest(&self) -> EvidenceDigest {
        self.instrument_revision_digest
    }
    pub const fn admitted_plan_digest(&self) -> EvidenceDigest {
        self.admitted_plan_digest
    }
    pub fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }
    pub fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }
    pub fn interval(&self) -> &SourceIdentifier {
        &self.interval
    }
    pub fn graph_purpose(&self) -> &SourceIdentifier {
        &self.graph_purpose
    }
    pub const fn requested_dates(&self) -> (CalendarDate, CalendarDate) {
        self.requested_dates
    }
    pub fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    pub fn source_contract_revision(&self) -> &MetadataRevision {
        &self.normalization.source_contract_revision
    }
    pub fn normalization(&self) -> &RetainedMarketHistoryNormalizationV1 {
        &self.normalization
    }
    pub fn calendar(&self) -> &RetainedMarketHistoryCalendarV1 {
        &self.normalization.calendar
    }
    pub const fn completeness_evidence(&self) -> EvidenceDigest {
        self.completeness_evidence
    }
    pub const fn request_set_identity(&self) -> EvidenceDigest {
        self.request_set_identity
    }
    pub const fn capture_pack_identity(&self) -> EvidenceDigest {
        self.capture_pack_identity
    }
    pub const fn page_count(&self) -> usize {
        self.page_count
    }
    pub const fn session_count(&self) -> usize {
        self.session_count
    }
    pub const fn date_digest(&self) -> EvidenceDigest {
        self.date_digest
    }
    pub const fn raw_count(&self) -> usize {
        self.raw_count
    }
    pub const fn all_count(&self) -> usize {
        self.all_count
    }
    pub const fn raw_digest(&self) -> Option<EvidenceDigest> {
        self.raw_digest
    }
    pub const fn all_digest(&self) -> Option<EvidenceDigest> {
        self.all_digest
    }
    pub const fn source_action_count(&self) -> usize {
        self.source_action_count
    }
    pub const fn normalized_action_count(&self) -> usize {
        self.normalized_action_count
    }
    pub const fn ordinary_fields_complete(&self) -> bool {
        self.ordinary_fields_complete
    }
    pub fn total_canonical_rows(&self) -> u64 {
        self.total_canonical_rows
    }
    pub const fn total_response_bytes(&self) -> u64 {
        self.total_response_bytes
    }
    pub const fn first_received_at(&self) -> Timestamp {
        self.first_received_at
    }
    pub const fn max_received_at(&self) -> Timestamp {
        self.max_received_at
    }
    pub const fn max_available_at(&self) -> Timestamp {
        self.max_received_at
    }
    pub const fn max_ingested_at(&self) -> Timestamp {
        self.max_ingested_at
    }
}

/// Original page custody and native mapping coordinates. Ordinal zero denotes metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TiingoEodHistoryPageEvidence {
    pub ordinal: usize,
    pub capture: ProviderCaptureSetReceipt,
    pub original_segment_claim: SealedResearchJournalSegmentClaim,
    pub sealed_receipt_digest: EvidenceDigest,
    pub body_offset: u64,
    pub window: Option<CompleteMarketBarDateWindowV1>,
    pub page_identity: Option<EvidenceDigest>,
    pub handoff_identity: Option<EvidenceDigest>,
    #[serde(skip)]
    verified_receipt: Option<SealedProviderCaptureSetReceipt>,
}
impl TiingoEodHistoryPageEvidence {
    pub fn sealed_receipt(
        &self,
    ) -> Result<&SealedProviderCaptureSetReceipt, TiingoEodHistoryStageError> {
        self.verified_receipt
            .as_ref()
            .ok_or(TiingoEodHistoryStageError::Mismatch)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TiingoEodHistoryChunkCommitment {
    pub global_start: u64,
    pub row_count: u64,
    pub extraction_content_digest: EvidenceDigest,
    pub native_batch_digest: EvidenceDigest,
    pub original_page_ordinal: usize,
}
impl TiingoEodHistoryChunkCommitment {
    pub const fn global_start(&self) -> u64 {
        self.global_start
    }
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }
    pub const fn extraction_content_digest(&self) -> EvidenceDigest {
        self.extraction_content_digest
    }
    pub const fn native_batch_digest(&self) -> EvidenceDigest {
        self.native_batch_digest
    }
    pub const fn original_page_ordinal(&self) -> usize {
        self.original_page_ordinal
    }
}
#[derive(Debug)]
pub struct TiingoEodHistoryChunk {
    batch: ExtractionBatch,
    native: ProviderNativeLineageBatch,
    revisions: ExtractionRevisionPlan,
    original_page_ordinal: usize,
    global_start: u64,
}
impl TiingoEodHistoryChunk {
    pub fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }
    pub fn native_lineage(&self) -> &ProviderNativeLineageBatch {
        &self.native
    }
    pub const fn original_page_ordinal(&self) -> usize {
        self.original_page_ordinal
    }
    pub const fn global_start(&self) -> u64 {
        self.global_start
    }
    pub fn into_parts(
        self,
    ) -> (
        ExtractionBatch,
        ProviderNativeLineageBatch,
        ExtractionRevisionPlan,
        usize,
        u64,
    ) {
        (
            self.batch,
            self.native,
            self.revisions,
            self.original_page_ordinal,
            self.global_start,
        )
    }
}
#[derive(Debug, Error)]
pub enum TiingoEodHistoryStageError {
    #[error("Tiingo indexed history evidence does not match its complete original authority")]
    Mismatch,
    #[error("Tiingo indexed history operation was cancelled")]
    Cancelled,
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Eod(#[from] TiingoEodMapError),
    #[error(transparent)]
    Actions(#[from] TiingoEodActionError),
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
    #[error(transparent)]
    History(#[from] crate::TiingoHistoryEvidenceError),
    #[error(transparent)]
    Latest(#[from] TiingoLatestPublicationError),
    #[error(transparent)]
    Extraction(#[from] ExtractionError),
    #[error(transparent)]
    Native(#[from] ProviderNativeLineageError),
    #[error(transparent)]
    Revision(#[from] ObservedRevisionError),
    #[error(transparent)]
    Logical(#[from] market_squawk_sources::ProviderLogicalPublicationError),
}
fn check(cancel: &CancellationToken) -> Result<(), TiingoEodHistoryStageError> {
    if cancel.is_cancelled() {
        Err(TiingoEodHistoryStageError::Cancelled)
    } else {
        Ok(())
    }
}
fn index(value: usize) -> Result<i64, TiingoEodHistoryStageError> {
    i64::try_from(value).map_err(|_| TiingoEodHistoryStageError::Mismatch)
}
fn read_json<T: DeserializeOwned>(
    db: &Connection,
    table: &str,
    ordinal: usize,
) -> Result<Option<T>, TiingoEodHistoryStageError> {
    let bytes = read_bytes(db, table, ordinal)?;
    bytes
        .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
        .transpose()
}
fn read_bytes(
    db: &Connection,
    table: &str,
    ordinal: usize,
) -> Result<Option<Vec<u8>>, TiingoEodHistoryStageError> {
    let value: Option<(Vec<u8>, Vec<u8>)> = db
        .query_row(
            &format!("SELECT payload,digest FROM {table} WHERE ordinal=?1"),
            [index(ordinal)?],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    value
        .map(|(bytes, expected)| {
            if row_checksum(table, ordinal, &bytes).as_slice() != expected {
                return Err(TiingoEodHistoryStageError::Mismatch);
            }
            Ok(bytes)
        })
        .transpose()
}
fn row_checksum(table: &str, ordinal: usize, bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(table.as_bytes());
    hash.update((ordinal as u64).to_le_bytes());
    hash.update(bytes);
    hash.finalize().into()
}
fn projection_checksum(
    phase: u8,
    ordinal: usize,
    page: usize,
    surface: u8,
    bytes: &[u8],
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update([phase, surface]);
    hash.update((ordinal as u64).to_le_bytes());
    hash.update((page as u64).to_le_bytes());
    hash.update(bytes);
    hash.finalize().into()
}
struct ProjectionRow {
    page: usize,
    observation: Vec<u8>,
    native: Vec<u8>,
}
fn read_projection(
    db: &Connection,
    phase: u8,
    ordinal: usize,
) -> Result<Option<ProjectionRow>, TiingoEodHistoryStageError> {
    let value:Option<(i64,u8,Vec<u8>,Vec<u8>,Vec<u8>,Vec<u8>)>=db.query_row(
        "SELECT page,surface,observation,native,observation_digest,native_digest FROM projections WHERE phase=?1 AND ordinal=?2",params![phase,index(ordinal)?],
        |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?))).optional()?;
    value
        .map(
            |(page, surface, observation, native, observed_digest, native_digest)| {
                let page =
                    usize::try_from(page).map_err(|_| TiingoEodHistoryStageError::Mismatch)?;
                if projection_checksum(phase, ordinal, page, surface, &observation).as_slice()
                    != observed_digest
                    || projection_checksum(phase, ordinal, page, surface, &native).as_slice()
                        != native_digest
                {
                    return Err(TiingoEodHistoryStageError::Mismatch);
                }
                Ok(ProjectionRow {
                    page,
                    observation,
                    native,
                })
            },
        )
        .transpose()
}
fn put_json<T: Serialize>(
    db: &Connection,
    table: &str,
    ordinal: usize,
    value: &T,
) -> Result<(), TiingoEodHistoryStageError> {
    let bytes = serde_json::to_vec(value)?;
    let digest = row_checksum(table, ordinal, &bytes);
    db.execute(
        &format!("INSERT INTO {table}(ordinal,payload,digest) VALUES(?1,?2,?3)"),
        params![index(ordinal)?, bytes, digest.as_slice()],
    )?;
    Ok(())
}
fn put_projection(
    db: &Connection,
    phase: u8,
    ordinal: usize,
    page: usize,
    surface: u8,
    observation: &ResearchObservation,
    native: &TiingoNativeDailyRowV1,
) -> Result<(), TiingoEodHistoryStageError> {
    let observation = serde_json::to_vec(observation)?;
    let native = serde_json::to_vec(native)?;
    let observation_digest = projection_checksum(phase, ordinal, page, surface, &observation);
    let native_digest = projection_checksum(phase, ordinal, page, surface, &native);
    db.execute(
        "INSERT INTO projections VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            phase,
            index(ordinal)?,
            index(page)?,
            surface,
            observation,
            native,
            observation_digest.as_slice(),
            native_digest.as_slice()
        ],
    )?;
    Ok(())
}

/// Mutable stage owns scratch for projections and only compact verified per-request receipt headers.
#[derive(Debug)]
pub struct TiingoEodHistoryStage {
    db: std::sync::Mutex<Connection>,
    _scratch: tempfile::TempDir,
    plan: TiingoHistoryPlan,
    metadata: TiingoMetadataReceipt,
    receipts: Vec<SealedProviderCaptureSetReceipt>,
    instrument: TiingoEodInstrumentAuthority,
    contract: TiingoEodContractEvidence,
    cash_unit: Option<TiingoEodCashUnitEvidence>,
    admitted_plan_digest: EvidenceDigest,
    pack: ProviderCapturePackAccumulator,
    page_count: usize,
    session_count: usize,
    bars: usize,
    actions: usize,
    gaps: usize,
    raw_count: usize,
    all_count: usize,
    previous_date: Option<CalendarDate>,
    last_page_identity: Option<EvidenceDigest>,
    max_received_at: Timestamp,
    max_ingested_at: Timestamp,
    ordinary_fields_complete: bool,
    failed: bool,
}
/// Non-forgeable terminal authority; emissions are committed in its private index.
#[derive(Debug)]
pub struct ValidatedTiingoEodHistory {
    stage: TiingoEodHistoryStage,
    descriptor: TiingoEodHistoryDescriptor,
    phase: u8,
    cursor: usize,
    emitted: u64,
    chunks: usize,
    publication_authorized: bool,
}
impl TiingoEodHistoryStage {
    fn db(&self) -> Result<std::sync::MutexGuard<'_, Connection>, TiingoEodHistoryStageError> {
        self.db
            .lock()
            .map_err(|_| TiingoEodHistoryStageError::Mismatch)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        plan: TiingoHistoryPlan,
        metadata: TiingoMetadataReceipt,
        metadata_receipt: SealedProviderCaptureSetReceipt,
        instrument: TiingoEodInstrumentAuthority,
        contract: TiingoEodContractEvidence,
        cash_unit: Option<TiingoEodCashUnitEvidence>,
        admitted_plan_digest: EvidenceDigest,
        scratch_parent: &Path,
    ) -> Result<Self, TiingoEodHistoryStageError> {
        let capture = metadata_receipt.capture();
        let [page] = capture.pages() else {
            return Err(TiingoEodHistoryStageError::Mismatch);
        };
        let evidence = metadata.evidence();
        if capture.source_id() != contract.source_id()
            || capture.metadata_revision() != contract.source_contract_revision()
            || capture.dataset().as_str() != crate::canonical::TIINGO_METADATA_DATASET
            || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
            || page.request_identity() != evidence.request().request_identity()
            || capture.request_set_identity() != page.request_identity()
            || page.body_digest() != evidence.body_digest()
            || page.body_bytes() != evidence.response_bytes()
            || page.http_status() != evidence.status()
            || page.received_at() != evidence.received_at()
            || evidence.request().endpoint() != TiingoEndpointFamily::Metadata
            || admitted_plan_digest.bytes() == [0; 32]
            || plan.pages().is_empty()
        {
            return Err(TiingoEodHistoryStageError::Mismatch);
        }
        let scratch = tempfile::Builder::new()
            .prefix("tiingo-history-")
            .tempdir_in(scratch_parent)?;
        let db = Connection::open(scratch.path().join("projection.sqlite"))?;
        db.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE; PRAGMA cache_size=-2048; PRAGMA mmap_size=0;
          CREATE TABLE pages(ordinal INTEGER PRIMARY KEY,payload BLOB NOT NULL,digest BLOB NOT NULL);
          CREATE TABLE sessions(ordinal INTEGER PRIMARY KEY,payload BLOB NOT NULL,digest BLOB NOT NULL);
          CREATE TABLE dispositions(ordinal INTEGER PRIMARY KEY,payload BLOB NOT NULL,digest BLOB NOT NULL);
          CREATE TABLE actions(ordinal INTEGER PRIMARY KEY,payload BLOB NOT NULL,digest BLOB NOT NULL);
          CREATE TABLE chunks(ordinal INTEGER PRIMARY KEY,payload BLOB NOT NULL,digest BLOB NOT NULL);
          CREATE TABLE projections(phase INTEGER NOT NULL,ordinal INTEGER NOT NULL,page INTEGER NOT NULL,surface INTEGER NOT NULL,observation BLOB NOT NULL,native BLOB NOT NULL,observation_digest BLOB NOT NULL,native_digest BLOB NOT NULL,PRIMARY KEY(phase,ordinal));")?;
        put_json(
            &db,
            "pages",
            0,
            &TiingoEodHistoryPageEvidence {
                ordinal: 0,
                capture: capture.clone(),
                original_segment_claim: metadata_receipt.segment().claim().clone(),
                sealed_receipt_digest: metadata_receipt.receipt_digest(),
                body_offset: 0,
                window: None,
                page_identity: None,
                handoff_identity: None,
                verified_receipt: None,
            },
        )?;
        let mut pack = ProviderCapturePackAccumulator::new();
        pack.push(&metadata_receipt)?;
        let max_received_at = evidence.received_at();
        let max_ingested_at = evidence.decoded_at();
        Ok(Self {
            db: std::sync::Mutex::new(db),
            _scratch: scratch,
            plan,
            metadata,
            receipts: vec![metadata_receipt],
            instrument,
            contract,
            cash_unit,
            admitted_plan_digest,
            pack,
            page_count: 0,
            session_count: 0,
            bars: 0,
            actions: 0,
            gaps: 0,
            raw_count: 0,
            all_count: 0,
            previous_date: None,
            last_page_identity: None,
            max_received_at,
            max_ingested_at,
            ordinary_fields_complete: true,
            failed: false,
        })
    }
    pub fn push_page(
        &mut self,
        response: &TiingoEodReceipt,
        receipt: &SealedProviderCaptureSetReceipt,
        ingested_at: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<TiingoSealedHistoryPage, TiingoEodHistoryStageError> {
        check(cancel)?;
        if self.failed {
            return Err(TiingoEodHistoryStageError::Mismatch);
        }
        // A failed page poisons the unpublished stage. No partially mapped page can be resumed.
        self.failed = true;
        let expected = self
            .plan
            .pages()
            .get(self.page_count)
            .ok_or(TiingoEodHistoryStageError::Mismatch)?;
        let sealed = TiingoSealedHistoryPage::try_new(expected, response, receipt)?;
        let page = map_eod_page_candidate(TiingoEodMappingInput {
            response,
            metadata: &self.metadata,
            sealed_capture: receipt,
            sealed_metadata_capture: &self.receipts[0],
            instrument: &self.instrument,
            contract: &self.contract,
            ingested_at,
        })?;
        let TiingoRequestScope::History {
            start_date,
            end_date,
            ..
        } = response.evidence().request().scope()
        else {
            return Err(TiingoEodHistoryStageError::Mismatch);
        };
        let first_session = self.session_count;
        self.db()?.execute_batch("BEGIN")?;
        for row in response.rows() {
            check(cancel)?;
            if row.date() < *start_date
                || row.date() > *end_date
                || self.previous_date.is_some_and(|date| date >= row.date())
            {
                return Err(TiingoEodHistoryStageError::Mismatch);
            }
            put_json(
                &*self.db()?,
                "sessions",
                self.session_count,
                &CompleteMarketBarDateSessionV1 {
                    date: row.date(),
                    time: crate::eod::nominal_time(row)?,
                    row_digest: row.row_digest(),
                },
            )?;
            self.previous_date = Some(row.date());
            self.session_count += 1;
        }
        for bar in page.bars() {
            check(cancel)?;
            let observation = eod_observation(&page, bar)?;
            let row = response
                .rows()
                .get(bar.provider_row_index() as usize)
                .ok_or(TiingoEodHistoryStageError::Mismatch)?;
            let surface = match bar.surface() {
                TiingoEodSurface::Raw => {
                    self.raw_count += 1;
                    0
                }
                TiingoEodSurface::Adjusted => {
                    self.all_count += 1;
                    1
                }
            };
            put_projection(
                &*self.db()?,
                0,
                self.bars,
                self.page_count,
                surface,
                &observation,
                &TiingoNativeDailyRowV1::from_row(row, surface_name(bar.surface())),
            )?;
            self.bars += 1;
        }
        let actions = crate::actions::project_eod_page_actions(
            &page,
            self.page_count,
            self.cash_unit.as_ref(),
        )?;
        self.ordinary_fields_complete &= actions.ordinary_fields_complete();
        for (offset, row) in actions.rows().iter().enumerate() {
            check(cancel)?;
            let mut row = row.clone();
            for field in [&mut row.cash, &mut row.shares] {
                if let TiingoEodActionFieldDisposition::Normalized { observation_index } = field {
                    *observation_index = observation_index
                        .checked_add(self.actions)
                        .ok_or(TiingoEodHistoryStageError::Mismatch)?;
                }
            }
            put_json(&*self.db()?, "dispositions", first_session + offset, &row)?;
        }
        for action in actions.observations() {
            check(cancel)?;
            let row = response
                .rows()
                .get(action.provider_row_index as usize)
                .ok_or(TiingoEodHistoryStageError::Mismatch)?;
            let selected = match action.observation.action() {
                CorporateActionKind::Split { .. } => "split",
                CorporateActionKind::CashDividend { .. } => "dividend",
                _ => return Err(TiingoEodHistoryStageError::Mismatch),
            };
            let observation = ResearchObservation::CorporateAction(action.observation.clone());
            put_projection(
                &*self.db()?,
                1,
                self.actions,
                self.page_count,
                2,
                &observation,
                &TiingoNativeDailyRowV1::from_row(row, selected),
            )?;
            put_json(&*self.db()?, "actions", self.actions, action)?;
            self.actions += 1;
        }
        put_json(
            &*self.db()?,
            "pages",
            self.page_count + 1,
            &TiingoEodHistoryPageEvidence {
                ordinal: self.page_count + 1,
                capture: receipt.capture().clone(),
                original_segment_claim: receipt.segment().claim().clone(),
                sealed_receipt_digest: receipt.receipt_digest(),
                body_offset: self.pack.size_bytes(),
                window: Some(CompleteMarketBarDateWindowV1 {
                    component_ordinal: u16::try_from(self.page_count + 1)
                        .map_err(|_| TiingoEodHistoryStageError::Mismatch)?,
                    request_identity: response.evidence().request().request_identity(),
                    start_date: *start_date,
                    end_date: *end_date,
                    first_session_ordinal: u32::try_from(first_session)
                        .map_err(|_| TiingoEodHistoryStageError::Mismatch)?,
                    returned_session_count: u32::try_from(response.rows().len())
                        .map_err(|_| TiingoEodHistoryStageError::Mismatch)?,
                    decoded_at: page.decoded_at(),
                    ingested_at,
                }),
                page_identity: Some(sealed.page_identity()),
                handoff_identity: Some(page.handoff_identity()),
                verified_receipt: None,
            },
        )?;
        self.pack.push(receipt)?;
        self.receipts.push(receipt.clone());
        self.gaps += page.gaps().len();
        self.page_count += 1;
        self.last_page_identity = Some(sealed.page_identity());
        self.max_received_at = self.max_received_at.max(page.received_at());
        self.max_ingested_at = self.max_ingested_at.max(ingested_at);
        self.db()?.execute_batch("COMMIT")?;
        self.failed = false;
        Ok(sealed)
    }
    pub fn finish(
        self,
        terminal: TiingoVerifiedHistoryTerminal,
        authority: &dyn TiingoEodExpectedSessionAuthority,
        cancel: &CancellationToken,
    ) -> Result<ValidatedTiingoEodHistory, TiingoEodHistoryStageError> {
        check(cancel)?;
        if self.failed
            || self.page_count != self.plan.pages().len()
            || terminal.page_count != self.page_count
            || terminal.plan_identity != self.plan.request_set_identity()
            || terminal.last_page_identity != self.last_page_identity
        {
            return Err(TiingoEodHistoryStageError::Mismatch);
        }
        let request = TiingoEodExpectedSessionRequest::new(&self.plan, &self.instrument);
        let mut count = 0_usize;
        let mut dates = Sha256::new();
        crate::eod::append_field(&mut dates, &(self.session_count as u64).to_be_bytes());
        let expected = authority.resolve_expected_sessions(&request, &mut |date| {
            if cancel.is_cancelled() {
                return Err(TiingoEodMapError::InvalidExpectedSessionEvidence);
            }
            let session: CompleteMarketBarDateSessionV1 = read_json(
                &*self
                    .db()
                    .map_err(|_| TiingoEodMapError::InvalidExpectedSessionEvidence)?,
                "sessions",
                count,
            )
            .map_err(|_| TiingoEodMapError::InvalidExpectedSessionEvidence)?
            .ok_or(TiingoEodMapError::InvalidExpectedSessionEvidence)?;
            if date != session.date {
                return Err(TiingoEodMapError::InvalidExpectedSessionEvidence);
            }
            crate::eod::append_field(&mut dates, date.to_string().as_bytes());
            count += 1;
            Ok(())
        })?;
        crate::eod::validate_expected_session_evidence(&request, &expected)?;
        if count != self.session_count
            || expected.expected_session_count() != count
            || expected.expected_session_digest() != digest(dates)
        {
            return Err(TiingoEodHistoryStageError::Mismatch);
        }
        let retained_validation = authority.validate_current(&expected)?;
        crate::eod::validate_expected_session_validation(&expected, &retained_validation)?;
        let instrument = &self.instrument;
        let contract = &self.contract;
        let metadata = &self.metadata;
        let cash_unit = self.cash_unit.as_ref();
        let normalization = RetainedMarketHistoryNormalizationV1 {
            instrument_definition: instrument.instrument_definition().clone(),
            provider_mapping_evidence: instrument.provider_mapping_evidence().clone(),
            provider_exchange_code: instrument.provider_exchange_code().clone(),
            is_exchange_traded_fund: instrument.kind()
                == TiingoEodInstrumentKind::ExchangeTradedFund,
            resolved_at: instrument.resolved_at(),
            currency: instrument.currency(),
            source_contract_revision: contract.source_contract_revision().clone(),
            source_contract_evidence: contract.source_contract_evidence().clone(),
            native_schema_revision: contract.native_schema_revision().clone(),
            native_schema_evidence: contract.native_schema_evidence().clone(),
            entitlement_generation: contract.entitlement_generation_identity().clone(),
            entitlement_generation_number: contract.entitlement_generation(),
            entitlement_evidence: contract.entitlement_evidence(),
            adjusted_surface_evidence: contract.adjusted_surface_evidence().clone(),
            contract_identity: contract.mapping_identity(),
            metadata_decoded_at: metadata.evidence().decoded_at(),
            native_coverage: match metadata.metadata().coverage() {
                TiingoCoverage::Supported {
                    start_date,
                    end_date,
                } => RetainedMarketHistoryNativeCoverageV1::Supported {
                    start: start_date,
                    end: end_date,
                },
                TiingoCoverage::Unsupported => RetainedMarketHistoryNativeCoverageV1::Unsupported,
            },
            cash_unit: cash_unit.map(|unit| RetainedMarketHistoryCashUnitV1 {
                status: unit.status(),
                currency: unit.currency(),
                assertion: unit.assertion().clone(),
                available_at: unit.available_at(),
            }),
            calendar: RetainedMarketHistoryCalendarV1 {
                request_identity: expected.request_identity(),
                calendar_id: expected.calendar_id().clone(),
                origin_content_digest: expected.origin_content_digest(),
                capture_binding_digest: expected.capture_binding_digest(),
                relationship: expected.relationship().clone(),
                calendar_revision: expected.calendar_revision().clone(),
                authority_generation: expected.authority_generation().clone(),
                calendar_available_at: expected.calendar_available_at(),
                resolved_at: expected.resolved_at(),
                resolution_receipt: expected.resolution_receipt(),
                evidence_identity: expected.evidence_identity(),
                validated_at: retained_validation.validated_at(),
                authority_receipt: retained_validation.authority_receipt(),
                validation_identity: retained_validation.receipt_identity(),
            },
        };
        let descriptor =
            self.build_descriptor(normalization, terminal.checkpoint_identity, cancel)?;
        Ok(ValidatedTiingoEodHistory {
            stage: self,
            descriptor,
            phase: 0,
            cursor: 0,
            emitted: 0,
            chunks: 0,
            publication_authorized: true,
        })
    }
    /// Reconciliation against an already-authorized persisted descriptor, never live publication authority.
    pub fn finish_replay(
        self,
        expected: &TiingoEodHistoryDescriptor,
        cancel: &CancellationToken,
    ) -> Result<ValidatedTiingoEodHistory, TiingoEodHistoryStageError> {
        if self.failed || self.page_count != self.plan.pages().len() {
            return Err(TiingoEodHistoryStageError::Mismatch);
        }
        let descriptor = self.build_descriptor(
            expected.normalization.clone(),
            expected.checkpoint_receipt_identity,
            cancel,
        )?;
        if &descriptor != expected {
            return Err(TiingoEodHistoryStageError::Mismatch);
        }
        Ok(ValidatedTiingoEodHistory {
            stage: self,
            descriptor,
            phase: 0,
            cursor: 0,
            emitted: 0,
            chunks: 0,
            publication_authorized: false,
        })
    }
    fn build_descriptor(
        &self,
        normalization: RetainedMarketHistoryNormalizationV1,
        checkpoint: EvidenceDigest,
        cancel: &CancellationToken,
    ) -> Result<TiingoEodHistoryDescriptor, TiingoEodHistoryStageError> {
        use crate::eod::{append_evidence_digest, append_field};
        let mut raw = Sha256::new();
        append_field(
            &mut raw,
            b"market-squawk/tiingo/sealed-history-completion/v1",
        );
        append_field(&mut raw, &self.plan.request_set_identity().bytes());
        append_field(&mut raw, &self.plan.maximum_response_bytes().to_be_bytes());
        append_field(&mut raw, &(self.page_count as u64).to_be_bytes());
        for ordinal in 1..=self.page_count {
            check(cancel)?;
            let page: TiingoEodHistoryPageEvidence = read_json(&*self.db()?, "pages", ordinal)?
                .ok_or(TiingoEodHistoryStageError::Mismatch)?;
            append_field(
                &mut raw,
                &page
                    .page_identity
                    .ok_or(TiingoEodHistoryStageError::Mismatch)?
                    .bytes(),
            );
        }
        let response_bytes = self
            .pack
            .size_bytes()
            .checked_sub(self.receipts[0].capture().total_body_bytes())
            .ok_or(TiingoEodHistoryStageError::Mismatch)?;
        append_field(&mut raw, &response_bytes.to_be_bytes());
        append_field(&mut raw, &(self.session_count as u64).to_be_bytes());
        append_field(&mut raw, &checkpoint.bytes());
        append_field(
            &mut raw,
            b"application-date-windows-exhausted-without-provider-cursor",
        );
        let mut completion = Sha256::new();
        append_field(
            &mut completion,
            b"market-squawk/tiingo/eod-history-completion/v4",
        );
        append_evidence_digest(&mut completion, digest(raw));
        append_field(&mut completion, &(self.page_count as u64).to_be_bytes());
        for ordinal in 1..=self.page_count {
            check(cancel)?;
            let page: TiingoEodHistoryPageEvidence = read_json(&*self.db()?, "pages", ordinal)?
                .ok_or(TiingoEodHistoryStageError::Mismatch)?;
            append_evidence_digest(&mut completion, page.capture.request_set_identity());
            append_evidence_digest(
                &mut completion,
                page.handoff_identity
                    .ok_or(TiingoEodHistoryStageError::Mismatch)?,
            );
            append_evidence_digest(&mut completion, page.sealed_receipt_digest);
            append_field(
                &mut completion,
                &page.capture.total_body_bytes().to_be_bytes(),
            );
        }
        let calendar = &normalization.calendar;
        append_evidence_digest(&mut completion, calendar.evidence_identity);
        append_field(&mut completion, calendar.calendar_id.as_str().as_bytes());
        append_field(
            &mut completion,
            calendar
                .calendar_revision
                .metadata_revision()
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        );
        append_evidence_digest(
            &mut completion,
            calendar
                .calendar_revision
                .payload_evidence()
                .content_digest(),
        );
        append_field(
            &mut completion,
            calendar.authority_generation.as_str().as_bytes(),
        );
        append_field(
            &mut completion,
            &calendar.calendar_available_at.unix_nanos().to_be_bytes(),
        );
        append_field(
            &mut completion,
            &calendar.resolved_at.unix_nanos().to_be_bytes(),
        );
        append_evidence_digest(&mut completion, calendar.resolution_receipt);
        append_evidence_digest(&mut completion, calendar.validation_identity);
        append_field(
            &mut completion,
            &calendar.validated_at.unix_nanos().to_be_bytes(),
        );
        for _ in 0..2 {
            append_field(&mut completion, &(self.session_count as u64).to_be_bytes());
            for ordinal in 0..self.session_count {
                check(cancel)?;
                let session: CompleteMarketBarDateSessionV1 =
                    read_json(&*self.db()?, "sessions", ordinal)?
                        .ok_or(TiingoEodHistoryStageError::Mismatch)?;
                append_field(&mut completion, session.date.to_string().as_bytes());
            }
        }
        append_field(&mut completion, &0_u64.to_be_bytes());
        append_field(&mut completion, &[0]);
        for count in [self.bars, self.gaps, self.session_count] {
            append_field(&mut completion, &(count as u64).to_be_bytes());
        }
        let mut dates = Sha256::new();
        dates.update(b"market-squawk/market-bar-history-original-dates/v1");
        dates.update((self.session_count as u64).to_be_bytes());
        for ordinal in 0..self.session_count {
            check(cancel)?;
            let session: CompleteMarketBarDateSessionV1 =
                read_json(&*self.db()?, "sessions", ordinal)?
                    .ok_or(TiingoEodHistoryStageError::Mismatch)?;
            hash_date(&mut dates, session.date);
        }
        let raw_digest = self.surface_digest(0, self.raw_count, cancel)?;
        let all_digest = self.surface_digest(1, self.all_count, cancel)?;
        let total = self
            .bars
            .checked_add(self.actions)
            .and_then(|n| u64::try_from(n).ok())
            .ok_or(TiingoEodHistoryStageError::Mismatch)?;
        Ok(TiingoEodHistoryDescriptor {
            instrument_id: self.instrument.instrument_id(),
            instrument_revision_digest: self
                .instrument
                .instrument_definition()
                .payload_evidence()
                .content_digest(),
            admitted_plan_digest: self.admitted_plan_digest,
            provider_instrument_id: self.instrument.provider_instrument_id().clone(),
            venue_id: self.instrument.venue_id().clone(),
            interval: identifier("tiingo-calendar-day")?,
            graph_purpose: identifier(HISTORY_PURPOSE)?,
            requested_dates: self.plan.interval(),
            source_id: self.contract.source_id().clone(),
            normalization,
            completeness_evidence: digest(completion),
            request_set_identity: self.plan.request_set_identity(),
            capture_pack_identity: self.pack.clone().finish(),
            page_count: self.page_count,
            session_count: self.session_count,
            date_digest: digest(dates),
            raw_count: self.raw_count,
            all_count: self.all_count,
            raw_digest,
            all_digest,
            source_action_count: self.session_count,
            normalized_action_count: self.actions,
            ordinary_fields_complete: self.ordinary_fields_complete,
            total_response_bytes: self.pack.size_bytes(),
            total_canonical_rows: total,
            checkpoint_receipt_identity: checkpoint,
            first_received_at: self.metadata.evidence().received_at(),
            max_received_at: self.max_received_at,
            max_ingested_at: self.max_ingested_at,
        })
    }
    fn surface_digest(
        &self,
        surface: i64,
        count: usize,
        cancel: &CancellationToken,
    ) -> Result<Option<EvidenceDigest>, TiingoEodHistoryStageError> {
        if count != self.session_count {
            return Ok(None);
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/market-bar-history-nominal-bars/v1");
        hash.update((count as u64).to_be_bytes());
        let db = self.db()?;
        let mut stmt=db.prepare("SELECT observation,observation_digest,ordinal,page FROM projections WHERE phase=0 AND surface=?1 ORDER BY ordinal")?;
        let mut rows = stmt.query([surface])?;
        let mut actual = 0;
        while let Some(row) = rows.next()? {
            check(cancel)?;
            let bytes: Vec<u8> = row.get(0)?;
            let checksum: Vec<u8> = row.get(1)?;
            let ordinal = usize::try_from(row.get::<_, i64>(2)?)
                .map_err(|_| TiingoEodHistoryStageError::Mismatch)?;
            let page = usize::try_from(row.get::<_, i64>(3)?)
                .map_err(|_| TiingoEodHistoryStageError::Mismatch)?;
            let surface =
                u8::try_from(surface).map_err(|_| TiingoEodHistoryStageError::Mismatch)?;
            if projection_checksum(0, ordinal, page, surface, &bytes).as_slice() != checksum {
                return Err(TiingoEodHistoryStageError::Mismatch);
            }
            let observation: ResearchObservation = serde_json::from_slice(&bytes)?;
            let ResearchObservation::MarketBar(bar) = &observation else {
                return Err(TiingoEodHistoryStageError::Mismatch);
            };
            hash_date(
                &mut hash,
                bar.time_semantics()
                    .nominal_daily_date()
                    .ok_or(TiingoEodHistoryStageError::Mismatch)?
                    .date(),
            );
            let payload = CanonicalObservationPayload::try_from_observation(&observation)
                .map_err(|_| TiingoEodHistoryStageError::Mismatch)?;
            hash.update([match payload.identity().algorithm() {
                DigestAlgorithm::Sha256 => 1,
                DigestAlgorithm::Blake3 => 2,
            }]);
            hash.update(payload.identity().bytes());
            actual += 1;
        }
        if actual != count {
            return Err(TiingoEodHistoryStageError::Mismatch);
        }
        Ok(Some(digest(hash)))
    }
}
fn digest(hash: Sha256) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}
fn hash_date(hash: &mut Sha256, date: CalendarDate) {
    hash.update(date.year().to_be_bytes());
    hash.update([date.month(), date.day()]);
}
impl ValidatedTiingoEodHistory {
    pub fn descriptor(&self) -> &TiingoEodHistoryDescriptor {
        &self.descriptor
    }
    pub const fn publication_authorized(&self) -> bool {
        self.publication_authorized
    }
    pub fn page_count(&self) -> usize {
        self.descriptor.page_count
    }
    pub fn total_canonical_rows(&self) -> u64 {
        self.descriptor.total_canonical_rows()
    }
    pub fn capture_pack_identity(&self) -> EvidenceDigest {
        self.descriptor.capture_pack_identity
    }
    pub fn completion_identity(&self) -> EvidenceDigest {
        self.descriptor.completeness_evidence
    }
    pub fn request_set_identity(&self) -> EvidenceDigest {
        self.descriptor.request_set_identity
    }
    pub fn cash_unit(&self) -> Option<&TiingoEodCashUnitEvidence> {
        self.stage.cash_unit.as_ref()
    }
    pub fn ordinary_fields_complete(&self) -> bool {
        self.descriptor.ordinary_fields_complete
    }
    /// History ordinal zero is the first price response; metadata is page-index frame zero.
    pub fn page_at(
        &self,
        ordinal: usize,
    ) -> Result<Option<TiingoEodHistoryPageEvidence>, TiingoEodHistoryStageError> {
        if ordinal >= self.stage.page_count {
            return Ok(None);
        }
        let mut page: TiingoEodHistoryPageEvidence =
            read_json(&*self.stage.db()?, "pages", ordinal + 1)?
                .ok_or(TiingoEodHistoryStageError::Mismatch)?;
        page.verified_receipt = Some(self.stage.receipts[ordinal + 1].clone());
        Ok(Some(page))
    }
    pub fn session_at(
        &self,
        ordinal: usize,
    ) -> Result<Option<CompleteMarketBarDateSessionV1>, TiingoEodHistoryStageError> {
        read_json(&*self.stage.db()?, "sessions", ordinal)
    }
    pub fn action_disposition_at(
        &self,
        ordinal: usize,
    ) -> Result<Option<TiingoEodDailyActionDisposition>, TiingoEodHistoryStageError> {
        read_json(&*self.stage.db()?, "dispositions", ordinal)
    }
    pub fn normalized_action_at(
        &self,
        ordinal: usize,
    ) -> Result<Option<TiingoEodNormalizedAction>, TiingoEodHistoryStageError> {
        read_json(&*self.stage.db()?, "actions", ordinal)
    }
    pub fn action_dispositions(
        &self,
    ) -> impl Iterator<Item = Result<TiingoEodDailyActionDisposition, TiingoEodHistoryStageError>> + '_
    {
        (0..self.descriptor.source_action_count).map(|i| {
            self.action_disposition_at(i)?
                .ok_or(TiingoEodHistoryStageError::Mismatch)
        })
    }
    pub fn normalized_actions(
        &self,
    ) -> impl Iterator<Item = Result<TiingoEodNormalizedAction, TiingoEodHistoryStageError>> + '_
    {
        (0..self.descriptor.normalized_action_count).map(|i| {
            self.normalized_action_at(i)?
                .ok_or(TiingoEodHistoryStageError::Mismatch)
        })
    }
    pub fn canonical_chunk_at(
        &self,
        ordinal: usize,
    ) -> Result<Option<TiingoEodHistoryChunkCommitment>, TiingoEodHistoryStageError> {
        read_json(&*self.stage.db()?, "chunks", ordinal)
    }
    pub const fn chunk_count(&self) -> usize {
        self.chunks
    }
    pub fn is_exhausted(&self) -> bool {
        self.emitted == self.descriptor.total_canonical_rows()
    }
    fn position(&self) -> (u8, usize) {
        if self.phase == 0 && self.cursor == self.stage.bars {
            (1, 0)
        } else {
            (self.phase, self.cursor)
        }
    }
    pub fn next_page_ordinal(&self) -> Result<Option<usize>, TiingoEodHistoryStageError> {
        let (phase, cursor) = self.position();
        Ok(read_projection(&*self.stage.db()?, phase, cursor)?.map(|row| row.page))
    }
    pub fn extraction_request(
        &self,
        deadline: Timestamp,
        max_records: NonZeroU32,
        max_bytes: NonZeroU64,
    ) -> Result<Option<ExtractionRequest>, TiingoEodHistoryStageError> {
        let Some(ordinal) = self.next_page_ordinal()? else {
            return Ok(None);
        };
        let capture = self.stage.receipts[ordinal + 1].capture();
        let page = &capture.pages()[0];
        let discovery =
            DiscoveryRequest::try_new(capture.dataset().clone(), None, NonZeroU16::MIN, deadline)?;
        let object = SourceObject::try_new_with_capture_identity(
            capture.source_id().clone(),
            capture.metadata_revision().clone(),
            &discovery,
            identifier(&format!("tiingo-history-page-{ordinal}"))?,
            identifier(TIINGO_CANONICAL_MEDIA_TYPE)?,
            ExactPayloadEvidence::from_content_digest(capture.content_digest()),
            SourceObjectCaptureIdentity::try_from_capture(capture)?,
            EffectiveInterval::new(page.received_at(), None)
                .map_err(|_| TiingoEodHistoryStageError::Mismatch)?,
            None,
            AvailabilityEvidence::LocalFirstObserved {
                observed_at: page.received_at(),
            },
            Some(capture.total_body_bytes()),
        )?;
        Ok(Some(ExtractionRequest::try_new(
            object,
            max_records,
            max_bytes,
            deadline,
        )?))
    }
    pub fn next_chunk(
        &mut self,
        request: &ExtractionRequest,
        cancel: &CancellationToken,
    ) -> Result<Option<TiingoEodHistoryChunk>, TiingoEodHistoryStageError> {
        check(cancel)?;
        let Some(page) = self.next_page_ordinal()? else {
            return Ok(None);
        };
        let expected = self
            .extraction_request(
                request.deadline(),
                NonZeroU32::new(request.max_records())
                    .ok_or(TiingoEodHistoryStageError::Mismatch)?,
                NonZeroU64::new(request.max_bytes()).ok_or(TiingoEodHistoryStageError::Mismatch)?,
            )?
            .ok_or(TiingoEodHistoryStageError::Mismatch)?;
        if &expected != request || self.stage.max_ingested_at >= request.deadline() {
            return Err(TiingoEodHistoryStageError::Mismatch);
        }
        let (phase, start) = self.position();
        let mut cursor = start;
        let mut accumulator = ExtractionBatchAccumulator::try_new(request)?;
        // Bounded only by one original native page and the canonical chunk working budget.
        while cursor - start < request.max_records() as usize {
            check(cancel)?;
            let Some(row) = read_projection(&*self.stage.db()?, phase, cursor)? else {
                break;
            };
            if row.page != page {
                break;
            }
            let observation: ResearchObservation = serde_json::from_slice(&row.observation)?;
            let context = match &observation {
                ResearchObservation::MarketBar(value) => value.context(),
                ResearchObservation::CorporateAction(value) => value.context(),
                _ => return Err(TiingoEodHistoryStageError::Mismatch),
            };
            let record = extraction_record(
                request,
                &observation,
                context.time().effective().clone(),
                context.provenance().received_at(),
            )?;
            if accumulator.try_push_or_return(record)?.is_some() {
                break;
            }
            cursor += 1;
        }
        if cursor == start {
            return Err(TiingoEodHistoryStageError::Mismatch);
        }
        let batch = accumulator
            .finish()?
            .try_bind_provider_capture(self.stage.receipts[page + 1].capture())?;
        let mut native = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::TiingoEodMarketBarV1,
            &batch,
        )?;
        for ordinal in start..cursor {
            check(cancel)?;
            let stored = read_projection(&*self.stage.db()?, phase, ordinal)?
                .ok_or(TiingoEodHistoryStageError::Mismatch)?;
            if stored.page != page {
                return Err(TiingoEodHistoryStageError::Mismatch);
            }
            let row: TiingoNativeDailyRowV1 = serde_json::from_slice(&stored.native)?;
            native.try_push(&row)?;
        }
        let native = native.finish()?;
        let revisions =
            ExtractionRevisionPlan::locally_observed_with_native_lineage(batch.records().len())?;
        let commitment = TiingoEodHistoryChunkCommitment {
            global_start: self.emitted,
            row_count: batch.records().len() as u64,
            extraction_content_digest:
                market_squawk_sources::ExtractionContentIdentity::try_from_batch(&batch)?.digest(),
            native_batch_digest: native.batch_digest(),
            original_page_ordinal: page,
        };
        put_json(&*self.stage.db()?, "chunks", self.chunks, &commitment)?;
        self.phase = phase;
        self.cursor = cursor;
        self.chunks += 1;
        self.emitted = self
            .emitted
            .checked_add(commitment.row_count)
            .ok_or(TiingoEodHistoryStageError::Mismatch)?;
        Ok(Some(TiingoEodHistoryChunk {
            batch,
            native,
            revisions,
            original_page_ordinal: page,
            global_start: commitment.global_start,
        }))
    }
    pub fn write_page_index(
        &self,
        writer: &mut impl Write,
        cancel: &CancellationToken,
    ) -> Result<(), TiingoEodHistoryStageError> {
        self.write_index("pages", self.stage.page_count + 1, writer, cancel)
    }
    pub fn write_session_index(
        &self,
        writer: &mut impl Write,
        cancel: &CancellationToken,
    ) -> Result<(), TiingoEodHistoryStageError> {
        self.write_index("sessions", self.stage.session_count, writer, cancel)
    }
    pub fn write_action_index(
        &self,
        writer: &mut impl Write,
        cancel: &CancellationToken,
    ) -> Result<(), TiingoEodHistoryStageError> {
        self.write_index("dispositions", self.stage.session_count, writer, cancel)
    }
    fn write_index(
        &self,
        table: &str,
        count: usize,
        writer: &mut impl Write,
        cancel: &CancellationToken,
    ) -> Result<(), TiingoEodHistoryStageError> {
        for ordinal in 0..count {
            check(cancel)?;
            let bytes = read_bytes(&*self.stage.db()?, table, ordinal)?
                .ok_or(TiingoEodHistoryStageError::Mismatch)?;
            writer.write_all(&(ordinal as u64).to_le_bytes())?;
            writer.write_all(&(bytes.len() as u64).to_le_bytes())?;
            writer.write_all(&bytes)?;
        }
        Ok(())
    }
}
