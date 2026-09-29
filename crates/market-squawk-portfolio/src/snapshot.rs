//! Immutable transaction lineage with ordered, bounded SQLite replay.
//!
//! The spool is private operation state, never an alternative accounting authority. Every row is
//! decoded through the checked domain constructors, then replayed by the ordinary portfolio engine.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::mem::size_of;
use std::sync::Arc;

use market_squawk_data::OperationScratchDirectory;
use market_squawk_domain::{
    AccountId, EvidenceDigest, InstrumentId, Money, NormalizedPortfolioLotMethod,
    NormalizedPortfolioTransactionClass, NormalizedPortfolioTransactionEvidence,
    NormalizedPortfolioTransactionEvidenceInput, RevisionNumber, SourceId, SourceIdentifier,
    Timestamp,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::lots::LotSelection;
use crate::transaction::{
    CashFlow, CashFlowKind, LedgerEntry, LedgerEntryKind, Trade, TradeSide, TransactionRevision,
};
use crate::{PortfolioError, PortfolioLimits};

type Result<T> = std::result::Result<T, PortfolioError>;
type Active = BTreeMap<SourceIdentifier, LedgerEntry>;
type Seen = BTreeSet<(SourceIdentifier, u32)>;

#[derive(Clone, Debug)]
pub(crate) struct LedgerSnapshot {
    storage: Arc<Storage>,
    identity: [u8; 32],
}

#[derive(Debug)]
enum Storage {
    Memory { active: Active, seen: Seen },
    Disk(DiskSnapshot),
}

#[derive(Debug)]
struct DiskSnapshot {
    directory: tempfile::TempDir,
    scratch: Arc<OperationScratchDirectory>,
    spill_bytes: u64,
    row_bytes: usize,
    cache_bytes: usize,
}

impl PartialEq for LedgerSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}
impl Eq for LedgerSnapshot {}

impl LedgerSnapshot {
    pub(crate) fn empty() -> Self {
        Self {
            storage: Arc::new(Storage::Memory {
                active: Active::new(),
                seen: Seen::new(),
            }),
            identity: Sha256::digest(b"market-squawk-portfolio-snapshot-v1\0").into(),
        }
    }

    pub(crate) fn from_memory(active: Active, seen: Seen, limits: PortfolioLimits) -> Result<Self> {
        let mut snapshot = Self {
            storage: Arc::new(Storage::Memory { active, seen }),
            identity: [0; 32],
        };
        crate::admit_retained_bytes(snapshot.retained_bytes()?, limits)?;
        snapshot.identity =
            snapshot.fingerprint(&CancellationToken::new(), limits.max_retained_bytes)?;
        Ok(snapshot)
    }

    pub(crate) fn memory_candidate(&self) -> Result<(Active, Seen)> {
        match self.storage.as_ref() {
            Storage::Memory { active, seen } => Ok((active.clone(), seen.clone())),
            Storage::Disk(_) => Err(PortfolioError::Storage),
        }
    }

    pub(crate) fn disk_policy(&self) -> Option<(Arc<OperationScratchDirectory>, u64)> {
        match self.storage.as_ref() {
            Storage::Disk(disk) => Some((Arc::clone(&disk.scratch), disk.spill_bytes)),
            Storage::Memory { .. } => None,
        }
    }

    pub(crate) fn get(&self, key: &SourceIdentifier) -> Result<Option<LedgerEntry>> {
        self.verify(&CancellationToken::new())?;
        match self.storage.as_ref() {
            Storage::Memory { active, .. } => Ok(active.get(key).cloned()),
            Storage::Disk(disk) => {
                let connection = disk.open(&CancellationToken::new())?;
                let bytes: Option<Vec<u8>> = connection
                    .query_row(
                        "SELECT body FROM active WHERE logical=?1",
                        [key.as_str()],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(storage)?;
                bytes.map(|bytes| decode(&bytes)).transpose()
            }
        }
    }

    pub(crate) fn verify(&self, cancellation: &CancellationToken) -> Result<()> {
        if let Storage::Disk(disk) = self.storage.as_ref() {
            if self.fingerprint(cancellation, disk.row_bytes)? != self.identity {
                return Err(PortfolioError::EvidenceMismatch);
            }
        }
        Ok(())
    }

    pub(crate) fn retained_bytes(&self) -> Result<usize> {
        let mut bytes = size_of::<Self>() + size_of::<Storage>();
        if let Storage::Memory { active, seen } = self.storage.as_ref() {
            bytes = bytes
                .checked_add(
                    active
                        .len()
                        .checked_mul(size_of::<(SourceIdentifier, LedgerEntry)>())
                        .ok_or(PortfolioError::Arithmetic)?,
                )
                .ok_or(PortfolioError::Arithmetic)?;
            bytes = bytes
                .checked_add(
                    seen.len()
                        .checked_mul(size_of::<(SourceIdentifier, u32)>())
                        .ok_or(PortfolioError::Arithmetic)?,
                )
                .ok_or(PortfolioError::Arithmetic)?;
            for key in active.keys() {
                bytes = bytes
                    .checked_add(key.retained_bytes())
                    .ok_or(PortfolioError::Arithmetic)?;
            }
        }
        Ok(bytes)
    }

    pub(crate) fn visit_ordered(
        &self,
        cancellation: &CancellationToken,
        mut visit: impl FnMut(&LedgerEntry) -> Result<()>,
    ) -> Result<()> {
        check_cancelled(cancellation)?;
        match self.storage.as_ref() {
            Storage::Memory { active, .. } => {
                // The convenience API already owns this bounded input. Sorting references avoids
                // the old full LedgerEntry clone; the production stream uses the disk index.
                let mut ordered = active.values().collect::<Vec<_>>();
                ordered.sort_unstable_by(|left, right| compare_entries(left, right));
                for entry in ordered {
                    check_cancelled(cancellation)?;
                    visit(entry)?;
                }
            }
            Storage::Disk(disk) => {
                let connection = disk.open(cancellation)?;
                let mut statement = connection
                    .prepare("SELECT body,logical,account,occurred,source,revision FROM active ORDER BY account,occurred,source,logical")
                    .map_err(storage)?;
                let mut rows = statement.query([]).map_err(storage)?;
                while let Some(row) = rows.next().map_err(storage)? {
                    check_cancelled(cancellation)?;
                    let bytes: Vec<u8> = row.get(0).map_err(storage)?;
                    let entry = decode(&bytes)?;
                    let logical: String = row.get(1).map_err(storage)?;
                    let account: Vec<u8> = row.get(2).map_err(storage)?;
                    let occurred: i64 = row.get(3).map_err(storage)?;
                    let source: String = row.get(4).map_err(storage)?;
                    let revision: u32 = row.get(5).map_err(storage)?;
                    if logical != entry.transaction.transaction_id.as_str()
                        || account != entry.account_id.as_uuid().as_bytes().as_slice()
                        || occurred != entry.occurred_at.unix_nanos()
                        || source != entry.source.as_str()
                        || revision != entry.transaction.revision.get()
                    {
                        return Err(PortfolioError::EvidenceMismatch);
                    }

                    visit(&entry)?;
                }
            }
        }
        Ok(())
    }

    fn visit_seen(
        &self,
        cancellation: &CancellationToken,
        mut visit: impl FnMut(&str, u32) -> Result<()>,
    ) -> Result<()> {
        match self.storage.as_ref() {
            Storage::Memory { seen, .. } => {
                for (key, revision) in seen {
                    check_cancelled(cancellation)?;
                    visit(key.as_str(), *revision)?;
                }
            }
            Storage::Disk(disk) => {
                let connection = disk.open(cancellation)?;
                let mut statement = connection
                    .prepare("SELECT logical,revision FROM seen ORDER BY logical,revision")
                    .map_err(storage)?;
                let mut rows = statement.query([]).map_err(storage)?;
                while let Some(row) = rows.next().map_err(storage)? {
                    check_cancelled(cancellation)?;
                    let key: String = row.get(0).map_err(storage)?;
                    let revision: u32 = row.get(1).map_err(storage)?;
                    visit(&key, revision)?;
                }
            }
        }
        Ok(())
    }

    fn fingerprint(&self, cancellation: &CancellationToken, row_bytes: usize) -> Result<[u8; 32]> {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk-portfolio-snapshot-v1\0");
        self.visit_ordered(cancellation, |entry| {
            let bytes = encode(entry, row_bytes)?;
            digest.update([0]);
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes);
            Ok(())
        })?;
        self.visit_seen(cancellation, |key, revision| {
            digest.update([1]);
            digest.update((key.len() as u64).to_be_bytes());
            digest.update(key.as_bytes());
            digest.update(revision.to_be_bytes());
            Ok(())
        })?;
        Ok(digest.finalize().into())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_disk(
        &self,
        entries: impl IntoIterator<Item = Result<LedgerEntry>>,
        account: AccountId,
        as_of: Timestamp,
        limits: PortfolioLimits,
        scratch: Arc<OperationScratchDirectory>,
        spill_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        check_cancelled(cancellation)?;
        self.verify(cancellation)?;
        if spill_bytes < 4096
            || spill_bytes > i64::MAX as u64
            || limits.max_retained_bytes < 64 * 1024
        {
            return Err(PortfolioError::InvalidLimits);
        }
        let disk = DiskSnapshot {
            directory: tempfile::Builder::new()
                .prefix("portfolio-")
                .tempdir_in(scratch.path())
                .map_err(|_| PortfolioError::Storage)?,
            scratch,
            spill_bytes,
            row_bytes: (limits.max_retained_bytes / 8).min(8 * 1024 * 1024),
            cache_bytes: (limits.max_retained_bytes / 16).min(2 * 1024 * 1024),
        };
        let connection =
            Connection::open(disk.directory.path().join("ledger.sqlite3")).map_err(storage)?;
        disk.configure(&connection, cancellation)?;
        connection
            .execute_batch("PRAGMA page_size=4096;")
            .map_err(storage)?;
        connection
            .pragma_update(
                None,
                "max_page_count",
                i64::try_from(spill_bytes / 4096).map_err(|_| PortfolioError::Arithmetic)?,
            )
            .map_err(storage)?;
        connection.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
            CREATE TABLE active(logical TEXT PRIMARY KEY, account BLOB NOT NULL, occurred INTEGER NOT NULL,
                source TEXT NOT NULL, revision INTEGER NOT NULL, body BLOB NOT NULL) WITHOUT ROWID;
            CREATE INDEX economic_order ON active(account,occurred,source,logical);
            CREATE TABLE seen(logical TEXT NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(logical,revision)) WITHOUT ROWID;
            CREATE TABLE incoming(ordinal INTEGER PRIMARY KEY, account BLOB NOT NULL,occurred INTEGER NOT NULL,
                source TEXT NOT NULL,logical TEXT NOT NULL,revision INTEGER NOT NULL,body BLOB NOT NULL);
            CREATE INDEX admission_order ON incoming(account,occurred,source,logical,revision,ordinal);
            BEGIN;").map_err(storage)?;
        let mut active_count = 0usize;
        {
            let mut insert = connection
                .prepare("INSERT INTO active VALUES(?1,?2,?3,?4,?5,?6)")
                .map_err(storage)?;
            self.visit_ordered(cancellation, |entry| {
                let bytes = encode(entry, disk.row_bytes)?;
                insert
                    .execute(params![
                        entry.transaction.transaction_id.as_str(),
                        entry.account_id.as_uuid().as_bytes().as_slice(),
                        entry.occurred_at.unix_nanos(),
                        entry.source.as_str(),
                        entry.transaction.revision.get(),
                        bytes
                    ])
                    .map_err(storage)?;
                active_count = active_count
                    .checked_add(1)
                    .ok_or(PortfolioError::Arithmetic)?;
                Ok(())
            })?;
            let mut insert = connection
                .prepare("INSERT INTO seen VALUES(?1,?2)")
                .map_err(storage)?;
            self.visit_seen(cancellation, |key, revision| {
                insert.execute(params![key, revision]).map_err(storage)?;
                Ok(())
            })?;
        }
        {
            let mut insert=connection.prepare("INSERT INTO incoming(account,occurred,source,logical,revision,body) VALUES(?1,?2,?3,?4,?5,?6)").map_err(storage)?;
            for (index, entry) in entries.into_iter().enumerate() {
                check_cancelled(cancellation)?;
                if index >= limits.max_transactions {
                    return Err(PortfolioError::LimitExceeded {
                        resource: "transactions",
                        observed: index.saturating_add(1),
                        limit: limits.max_transactions,
                    });
                }
                let entry = entry?;
                if entry.account_id != account || entry.occurred_at > as_of {
                    return Err(PortfolioError::EvidenceMismatch);
                }
                let bytes = encode(&entry, disk.row_bytes)?;
                insert
                    .execute(params![
                        entry.account_id.as_uuid().as_bytes().as_slice(),
                        entry.occurred_at.unix_nanos(),
                        entry.source.as_str(),
                        entry.transaction.transaction_id.as_str(),
                        entry.transaction.revision.get(),
                        bytes
                    ])
                    .map_err(storage)?;
            }
        }
        {
            let mut incoming=connection.prepare("SELECT body FROM incoming ORDER BY account,occurred,source,logical,revision,ordinal").map_err(storage)?;
            let mut current = connection
                .prepare("SELECT revision FROM active WHERE logical=?1")
                .map_err(storage)?;
            let mut seen = connection
                .prepare("INSERT OR IGNORE INTO seen VALUES(?1,?2)")
                .map_err(storage)?;
            let mut update=connection.prepare("INSERT INTO active VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(logical) DO UPDATE SET account=excluded.account,occurred=excluded.occurred,source=excluded.source,revision=excluded.revision,body=excluded.body").map_err(storage)?;
            let mut rows = incoming.query([]).map_err(storage)?;
            while let Some(row) = rows.next().map_err(storage)? {
                check_cancelled(cancellation)?;
                let bytes: Vec<u8> = row.get(0).map_err(storage)?;
                let entry = decode(&bytes)?;
                let logical = entry.transaction.transaction_id.as_str();
                if seen
                    .execute(params![logical, entry.transaction.revision.get()])
                    .map_err(storage)?
                    == 0
                {
                    return Err(PortfolioError::DuplicateTransactionRevision);
                }
                let prior: Option<u32> = current
                    .query_row([logical], |row| row.get(0))
                    .optional()
                    .map_err(storage)?;
                let prior = prior
                    .map(RevisionNumber::new)
                    .transpose()
                    .map_err(|_| PortfolioError::Storage)?;
                super::validate_supersession(prior, &entry)?;
                if prior.is_none() {
                    active_count = active_count
                        .checked_add(1)
                        .ok_or(PortfolioError::Arithmetic)?;
                    if active_count > limits.max_transactions {
                        return Err(PortfolioError::LimitExceeded {
                            resource: "logical transactions",
                            observed: active_count,
                            limit: limits.max_transactions,
                        });
                    }
                }
                update
                    .execute(params![
                        logical,
                        entry.account_id.as_uuid().as_bytes().as_slice(),
                        entry.occurred_at.unix_nanos(),
                        entry.source.as_str(),
                        entry.transaction.revision.get(),
                        bytes
                    ])
                    .map_err(storage)?;
            }
        }
        connection
            .execute_batch("DROP TABLE incoming; COMMIT;")
            .map_err(storage)?;
        drop(connection);
        // No mutable database handle escapes: failed candidates are deleted, published snapshots
        // are immutable and a later correction always creates a separate complete disk image.
        let row_bytes = disk.row_bytes;
        let mut snapshot = Self {
            storage: Arc::new(Storage::Disk(disk)),
            identity: [0; 32],
        };
        snapshot.identity = snapshot.fingerprint(cancellation, row_bytes)?;
        check_cancelled(cancellation)?;
        Ok(snapshot)
    }
}

impl DiskSnapshot {
    fn configure(&self, connection: &Connection, cancellation: &CancellationToken) -> Result<()> {
        let token = cancellation.clone();
        connection
            .progress_handler(1024, Some(move || token.is_cancelled()))
            .map_err(storage)?;
        connection
            .execute_batch("PRAGMA temp_store=FILE; PRAGMA mmap_size=0;")
            .map_err(storage)?;
        connection
            .pragma_update(
                None,
                "cache_size",
                -i64::try_from((self.cache_bytes / 1024).max(8))
                    .map_err(|_| PortfolioError::Arithmetic)?,
            )
            .map_err(storage)?;
        connection
            .set_limit(
                rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH,
                i32::try_from(
                    self.row_bytes
                        .checked_mul(2)
                        .ok_or(PortfolioError::Arithmetic)?,
                )
                .unwrap_or(i32::MAX),
            )
            .map_err(storage)?;
        Ok(())
    }
    fn open(&self, cancellation: &CancellationToken) -> Result<Connection> {
        check_cancelled(cancellation)?;
        let connection = Connection::open_with_flags(
            self.directory.path().join("ledger.sqlite3"),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(storage)?;
        self.configure(&connection, cancellation)?;
        Ok(connection)
    }
}

pub(crate) fn check_cancelled(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(PortfolioError::Cancelled)
    } else {
        Ok(())
    }
}

fn storage(error: rusqlite::Error) -> PortfolioError {
    match error.sqlite_error_code() {
        Some(rusqlite::ErrorCode::DiskFull) => PortfolioError::StorageCapacity,
        Some(rusqlite::ErrorCode::OperationInterrupted) => PortfolioError::Cancelled,
        _ => PortfolioError::Storage,
    }
}

pub(crate) fn compare_entries(left: &LedgerEntry, right: &LedgerEntry) -> Ordering {
    left.account_id
        .cmp(&right.account_id)
        .then_with(|| left.occurred_at.cmp(&right.occurred_at))
        .then_with(|| left.source.cmp(&right.source))
        .then_with(|| {
            left.transaction
                .transaction_id
                .cmp(&right.transaction.transaction_id)
        })
        .then_with(|| {
            left.transaction
                .revision
                .get()
                .cmp(&right.transaction.revision.get())
        })
}

#[derive(Serialize, Deserialize)]
struct EntryWire {
    account: AccountId,
    logical: SourceIdentifier,
    revision: RevisionNumber,
    supersedes: Option<RevisionNumber>,
    occurred: Timestamp,
    source: SourceIdentifier,
    payload: PayloadWire,
    normalized: Option<NormalizedWire>,
}
#[derive(Serialize, Deserialize)]
enum PayloadWire {
    Trade {
        side: u8,
        instrument: InstrumentId,
        quantity: Decimal,
        price: Money,
        fee: Money,
        selection: SelectionWire,
        notional: Option<Money>,
    },
    Cash {
        kind: u8,
        amount: Money,
        instrument: Option<InstrumentId>,
    },
}
#[derive(Serialize, Deserialize)]
enum SelectionWire {
    Fifo,
    Lifo,
    AverageCost,
    Specific(Vec<SourceIdentifier>),
}
#[derive(Serialize, Deserialize)]
struct NormalizedWire {
    source_id: SourceId,
    logical_record_id: SourceIdentifier,
    source_revision: SourceIdentifier,
    supersedes_source_revision: Option<SourceIdentifier>,
    revision: RevisionNumber,
    raw_source_reference: SourceIdentifier,
    raw_payload_digest: EvidenceDigest,
    broker_transaction_id: SourceIdentifier,
    account_id: AccountId,
    instrument_id: Option<InstrumentId>,
    classification: NormalizedPortfolioTransactionClass,
    amount: Money,
    quantity: Option<Decimal>,
    occurred_at: Timestamp,
    lot_method: Option<NormalizedPortfolioLotMethod>,
}
impl From<&NormalizedPortfolioTransactionEvidence> for NormalizedWire {
    fn from(e: &NormalizedPortfolioTransactionEvidence) -> Self {
        Self {
            source_id: e.source_id().clone(),
            logical_record_id: e.logical_record_id().clone(),
            source_revision: e.source_revision().clone(),
            supersedes_source_revision: e.supersedes_source_revision().cloned(),
            revision: e.revision(),
            raw_source_reference: e.raw_source_reference().clone(),
            raw_payload_digest: e.raw_payload_digest(),
            broker_transaction_id: e.broker_transaction_id().clone(),
            account_id: e.account_id(),
            instrument_id: e.instrument_id(),
            classification: e.classification(),
            amount: e.amount(),
            quantity: e.quantity(),
            occurred_at: e.occurred_at(),
            lot_method: e.lot_method(),
        }
    }
}
impl NormalizedWire {
    fn checked(self) -> Result<NormalizedPortfolioTransactionEvidence> {
        NormalizedPortfolioTransactionEvidence::try_new(
            NormalizedPortfolioTransactionEvidenceInput {
                source_id: self.source_id,
                logical_record_id: self.logical_record_id,
                source_revision: self.source_revision,
                supersedes_source_revision: self.supersedes_source_revision,
                revision: self.revision,
                raw_source_reference: self.raw_source_reference,
                raw_payload_digest: self.raw_payload_digest,
                broker_transaction_id: self.broker_transaction_id,
                account_id: self.account_id,
                instrument_id: self.instrument_id,
                classification: self.classification,
                amount: self.amount,
                quantity: self.quantity,
                occurred_at: self.occurred_at,
                lot_method: self.lot_method,
            },
        )
        .map_err(|_| PortfolioError::Storage)
    }
}

fn encode(entry: &LedgerEntry, limit: usize) -> Result<Vec<u8>> {
    let payload = match &entry.kind {
        LedgerEntryKind::Trade(trade) => PayloadWire::Trade {
            side: trade.side as u8,
            instrument: trade.instrument_id,
            quantity: trade.quantity,
            price: trade.price,
            fee: trade.fee,
            selection: match &trade.lot_selection {
                LotSelection::Fifo => SelectionWire::Fifo,
                LotSelection::Lifo => SelectionWire::Lifo,
                LotSelection::AverageCost => SelectionWire::AverageCost,
                LotSelection::SpecificIdentification(ids) => {
                    let bytes = ids.iter().try_fold(0usize, |total, id| {
                        total
                            .checked_add(size_of::<SourceIdentifier>() + id.retained_bytes())
                            .ok_or(PortfolioError::Arithmetic)
                    })?;
                    if bytes > limit {
                        return Err(PortfolioError::RetainedBytesExceeded {
                            observed: bytes,
                            limit,
                        });
                    }
                    SelectionWire::Specific(ids.clone())
                }
            },
            notional: trade.executed_notional,
        },
        LedgerEntryKind::CashFlow(flow) => PayloadWire::Cash {
            kind: flow.kind as u8,
            amount: flow.amount,
            instrument: flow.instrument_id,
        },
    };
    let wire = EntryWire {
        account: entry.account_id,
        logical: entry.transaction.transaction_id.clone(),
        revision: entry.transaction.revision,
        supersedes: entry.transaction.supersedes,
        occurred: entry.occurred_at,
        source: entry.source.clone(),
        payload,
        normalized: entry.normalized_evidence.as_ref().map(NormalizedWire::from),
    };
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, &wire).map_err(|_| {
        PortfolioError::RetainedBytesExceeded {
            observed: limit.saturating_add(1),
            limit,
        }
    })?;
    Ok(writer.bytes)
}
fn decode(bytes: &[u8]) -> Result<LedgerEntry> {
    let wire: EntryWire = serde_json::from_slice(bytes).map_err(|_| PortfolioError::Storage)?;
    let transaction = TransactionRevision::try_new(wire.logical, wire.revision, wire.supersedes)?;
    let kind = match wire.payload {
        PayloadWire::Trade {
            side,
            instrument,
            quantity,
            price,
            fee,
            selection,
            notional,
        } => {
            let side = match side {
                0 => TradeSide::Buy,
                1 => TradeSide::Sell,
                2 => TradeSide::SellShort,
                3 => TradeSide::BuyToCover,
                _ => return Err(PortfolioError::Storage),
            };
            let selection = match selection {
                SelectionWire::Fifo => LotSelection::Fifo,
                SelectionWire::Lifo => LotSelection::Lifo,
                SelectionWire::AverageCost => LotSelection::AverageCost,
                SelectionWire::Specific(ids) => LotSelection::SpecificIdentification(ids),
            };
            LedgerEntryKind::Trade(match notional {
                Some(value) => Trade::try_from_execution(
                    side, instrument, quantity, price, fee, selection, value,
                )?,
                None => Trade::try_new(side, instrument, quantity, price, fee, selection)?,
            })
        }
        PayloadWire::Cash {
            kind,
            amount,
            instrument,
        } => {
            let kind = match kind {
                0 => CashFlowKind::Deposit,
                1 => CashFlowKind::Withdrawal,
                2 => CashFlowKind::Dividend,
                3 => CashFlowKind::Interest,
                4 => CashFlowKind::Withholding,
                5 => CashFlowKind::Fee,
                _ => return Err(PortfolioError::Storage),
            };
            LedgerEntryKind::CashFlow(CashFlow::try_new(kind, amount, instrument)?)
        }
    };
    let mut entry =
        LedgerEntry::try_new(wire.account, transaction, wire.occurred, wire.source, kind)?;
    entry.normalized_evidence = wire.normalized.map(NormalizedWire::checked).transpose()?;
    if entry
        .normalized_evidence
        .as_ref()
        .is_some_and(|normalized| {
            normalized.account_id() != entry.account_id
                || normalized.occurred_at() != entry.occurred_at
                || normalized.raw_source_reference() != &entry.source
        })
    {
        return Err(PortfolioError::EvidenceMismatch);
    }
    Ok(entry)
}
struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("portfolio row length overflow"))?;
        if next > self.limit {
            return Err(std::io::Error::other(
                "portfolio row working memory exhausted",
            ));
        }
        self.bytes
            .try_reserve_exact(bytes.len())
            .map_err(|_| std::io::Error::other("portfolio row allocation failed"))?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
