//! Private complete fill and equity history, retained on disk for repeatable reconciliation.
use super::BacktestError;
use crate::ResearchFill;
use rusqlite::{Connection, params};
use rust_decimal::Decimal;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct Storage {
    connection: Mutex<Connection>,
    _directory: tempfile::TempDir,
    _authority: Option<Arc<market_squawk_data::OperationScratchDirectory>>,
}
#[derive(Clone, Debug)]
pub(super) struct RunHistory {
    storage: Arc<Storage>,
    fills: usize,
    marks: usize,
}
impl RunHistory {
    pub(super) fn new(
        authority: Option<Arc<market_squawk_data::OperationScratchDirectory>>,
    ) -> Result<Self, BacktestError> {
        let mut builder = tempfile::Builder::new();
        builder.prefix("run-");
        let directory = match &authority {
            Some(owner) => builder.tempdir_in(owner.path()),
            None => builder.tempdir(),
        }
        .map_err(|_| BacktestError::LimitExceeded)?;
        let connection = Connection::open(directory.path().join("history.sqlite"))
            .map_err(|_| BacktestError::LimitExceeded)?;
        connection.execute_batch("PRAGMA cache_size=-64; PRAGMA temp_store=FILE; CREATE TABLE fills(ordinal INTEGER PRIMARY KEY,payload BLOB NOT NULL); CREATE TABLE marks(ordinal INTEGER PRIMARY KEY,payload BLOB NOT NULL); BEGIN IMMEDIATE;").map_err(|_|BacktestError::LimitExceeded)?;
        Ok(Self {
            storage: Arc::new(Storage {
                connection: Mutex::new(connection),
                _directory: directory,
                _authority: authority,
            }),
            fills: 0,
            marks: 0,
        })
    }
    pub(super) const fn len(&self) -> usize {
        self.fills
    }
    pub(super) const fn mark_count(&self) -> usize {
        self.marks
    }
    pub(super) fn push(&mut self, fill: ResearchFill) -> Result<(), BacktestError> {
        let bytes = fill.storage_bytes()?;
        self.storage
            .connection
            .lock()
            .map_err(|_| BacktestError::InvalidDataset)?
            .execute(
                "INSERT INTO fills VALUES(?1,?2)",
                params![self.fills as i64, bytes],
            )
            .map_err(|_| BacktestError::LimitExceeded)?;
        self.fills += 1;
        Ok(())
    }
    pub(super) fn mark(&mut self, mark: Decimal) -> Result<(), BacktestError> {
        self.storage
            .connection
            .lock()
            .map_err(|_| BacktestError::InvalidDataset)?
            .execute(
                "INSERT INTO marks VALUES(?1,?2)",
                params![self.marks as i64, mark.serialize().as_slice()],
            )
            .map_err(|_| BacktestError::LimitExceeded)?;
        self.marks += 1;
        Ok(())
    }
    pub(super) fn finish(&self) -> Result<(), BacktestError> {
        self.storage
            .connection
            .lock()
            .map_err(|_| BacktestError::InvalidDataset)?
            .execute_batch("COMMIT; PRAGMA query_only=ON;")
            .map_err(|_| BacktestError::LimitExceeded)
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = Result<ResearchFill, BacktestError>> + '_ {
        (0..self.fills).map(|index| {
            let bytes: Vec<u8> = self
                .storage
                .connection
                .lock()
                .map_err(|_| BacktestError::InvalidDataset)?
                .query_row(
                    "SELECT payload FROM fills WHERE ordinal=?1",
                    [index as i64],
                    |row| row.get(0),
                )
                .map_err(|_| BacktestError::InvalidDataset)?;
            ResearchFill::from_storage_bytes(&bytes)
        })
    }
    pub(super) fn marks(&self) -> impl Iterator<Item = Result<Decimal, BacktestError>> + '_ {
        (0..self.marks).map(|index| {
            let bytes: Vec<u8> = self
                .storage
                .connection
                .lock()
                .map_err(|_| BacktestError::InvalidDataset)?
                .query_row(
                    "SELECT payload FROM marks WHERE ordinal=?1",
                    [index as i64],
                    |row| row.get(0),
                )
                .map_err(|_| BacktestError::InvalidDataset)?;
            let bytes: [u8; 16] = bytes
                .try_into()
                .map_err(|_| BacktestError::InvalidDataset)?;
            Ok(Decimal::deserialize(bytes))
        })
    }
}
