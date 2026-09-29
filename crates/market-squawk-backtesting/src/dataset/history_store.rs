//! Private operation-owned outcome staging; only admitted bars can enter this store.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use market_squawk_domain::Timestamp;
use rusqlite::{Connection, params};

use super::BacktestDailyBar;
use crate::engine::BacktestError;

const PAGE_ROWS: usize = 128;

#[derive(Debug)]
struct StoredBars {
    connection: Mutex<Connection>,
    // Last owner closes SQLite before removing its private operation directory.
    _directory: tempfile::TempDir,
    _authority: Option<Arc<market_squawk_data::OperationScratchDirectory>>,
}

#[derive(Clone, Debug)]
pub(crate) struct DailyBarStore(Arc<StoredBars>);

impl DailyBarStore {
    pub(super) fn new(
        authority: Option<Arc<market_squawk_data::OperationScratchDirectory>>,
    ) -> Result<Self, BacktestError> {
        let mut builder = tempfile::Builder::new();
        builder.prefix("outcomes-");
        let directory = match &authority {
            Some(owner) => builder.tempdir_in(owner.path()),
            None => builder.tempdir(),
        }
        .map_err(|_| BacktestError::LimitExceeded)?;
        let connection = Connection::open(directory.path().join("outcomes.sqlite"))
            .map_err(|_| BacktestError::LimitExceeded)?;
        connection
            .execute_batch(
                "PRAGMA cache_size=-64; PRAGMA temp_store=FILE;
            CREATE TABLE bars (ends_at INTEGER NOT NULL, instrument BLOB NOT NULL,
            payload BLOB NOT NULL, PRIMARY KEY (ends_at,instrument)) WITHOUT ROWID;
            BEGIN IMMEDIATE;",
            )
            .map_err(|_| BacktestError::LimitExceeded)?;
        Ok(Self(Arc::new(StoredBars {
            connection: Mutex::new(connection),
            _directory: directory,
            _authority: authority,
        })))
    }

    pub(super) fn push(&mut self, bar: BacktestDailyBar) -> Result<(), BacktestError> {
        let payload = serde_json::to_vec(&bar).map_err(|_| BacktestError::InvalidDataset)?;
        self.0
            .connection
            .lock()
            .map_err(|_| BacktestError::InvalidDataset)?
            .execute(
                "INSERT INTO bars VALUES (?1,?2,?3)",
                params![
                    bar.ends_at.unix_nanos(),
                    bar.execution_terms
                        .instrument_id()
                        .as_uuid()
                        .as_bytes()
                        .as_slice(),
                    payload
                ],
            )
            .map_err(|_| BacktestError::InvalidDataset)?;
        Ok(())
    }

    pub(super) fn finish(self) -> Result<Self, BacktestError> {
        self.0
            .connection
            .lock()
            .map_err(|_| BacktestError::InvalidDataset)?
            .execute_batch("COMMIT; PRAGMA query_only=ON;")
            .map_err(|_| BacktestError::LimitExceeded)?;
        Ok(self)
    }

    pub(crate) fn from(&self, starts_at: Timestamp) -> DailyBarCursor {
        DailyBarCursor {
            store: self.clone(),
            starts_at,
            after: None,
            page: VecDeque::new(),
            done: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_bars(
        bars: impl IntoIterator<Item = BacktestDailyBar>,
    ) -> Result<Self, BacktestError> {
        let mut store = Self::new(None)?;
        for bar in bars {
            store.push(bar)?;
        }
        store.finish()
    }
}

#[derive(Debug)]
pub(crate) struct DailyBarCursor {
    store: DailyBarStore,
    starts_at: Timestamp,
    after: Option<(i64, Vec<u8>)>,
    page: VecDeque<BacktestDailyBar>,
    done: bool,
}

impl DailyBarCursor {
    fn refill(&mut self) -> Result<(), BacktestError> {
        let connection = self
            .store
            .0
            .connection
            .lock()
            .map_err(|_| BacktestError::InvalidDataset)?;
        let (sql, timestamp, instrument) = match &self.after {
            Some((timestamp, instrument)) => (
                "SELECT payload FROM bars WHERE (ends_at,instrument) > (?1,?2) ORDER BY ends_at,instrument LIMIT 128",
                *timestamp,
                instrument.as_slice(),
            ),
            None => (
                "SELECT payload FROM bars WHERE (ends_at,instrument) >= (?1,?2) ORDER BY ends_at,instrument LIMIT 128",
                self.starts_at.unix_nanos(),
                &[][..],
            ),
        };
        let mut statement = connection
            .prepare(sql)
            .map_err(|_| BacktestError::InvalidDataset)?;
        let mut rows = statement
            .query(params![timestamp, instrument])
            .map_err(|_| BacktestError::InvalidDataset)?;
        while let Some(row) = rows.next().map_err(|_| BacktestError::InvalidDataset)? {
            let payload: Vec<u8> = row.get(0).map_err(|_| BacktestError::InvalidDataset)?;
            let bar =
                serde_json::from_slice(&payload).map_err(|_| BacktestError::InvalidDataset)?;
            self.page.push_back(bar);
        }
        self.done = self.page.len() < PAGE_ROWS;
        Ok(())
    }
}

impl Iterator for DailyBarCursor {
    type Item = Result<BacktestDailyBar, BacktestError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.page.is_empty() && !self.done {
            if let Err(error) = self.refill() {
                self.done = true;
                return Some(Err(error));
            }
        }
        let bar = self.page.pop_front()?;
        self.after = Some((
            bar.ends_at.unix_nanos(),
            bar.execution_terms
                .instrument_id()
                .as_uuid()
                .as_bytes()
                .to_vec(),
        ));
        Some(Ok(bar))
    }
}

pub(super) mod digest_wire {
    use market_squawk_data::Sha256Digest;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub fn serialize<S: Serializer>(
        value: &Sha256Digest,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.bytes().serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Sha256Digest, D::Error> {
        <[u8; 32]>::deserialize(deserializer).map(Sha256Digest::new)
    }
}
