//! Disposable original comparison rows under the same source operation's scratch authority.
use super::{BenchmarkHistoryPoint, Error, check};
use market_squawk_data::OperationScratchDirectory;
use rusqlite::{Connection, params};
use serde::{
    Serialize,
    ser::{Error as _, SerializeSeq},
};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

pub(super) enum BenchmarkPoints {
    Empty,
    Stored {
        connection: Mutex<Connection>,
        _scratch: Arc<OperationScratchDirectory>,
        count: u64,
        deadline: Instant,
        cancellation: CancellationToken,
    },
}
impl BenchmarkPoints {
    pub(super) fn iter(&self) -> impl Iterator<Item = Result<BenchmarkHistoryPoint, Error>> + '_ {
        let mut ordinal = 0_u64;
        let mut done = false;
        std::iter::from_fn(move || {
            let Self::Stored {
                connection,
                count,
                deadline,
                cancellation,
                ..
            } = self
            else {
                return None;
            };
            if done || ordinal >= *count {
                return None;
            };
            let result = (|| {
                check(*deadline, cancellation)?;
                let connection = connection.lock().map_err(|_| Error::StorageUnavailable)?;
                let bytes: Vec<u8> = connection
                    .query_row(
                        "SELECT payload FROM comparison WHERE ordinal=?1",
                        [i64::try_from(ordinal).map_err(|_| Error::CapacityExceeded)?],
                        |row| row.get(0),
                    )
                    .map_err(|_| Error::StorageUnavailable)?;
                if bytes.len() > 16 * 1024 {
                    return Err(Error::IntegrityUnproven);
                };
                let point = serde_json::from_slice(&bytes).map_err(|_| Error::IntegrityUnproven)?;
                ordinal += 1;
                Ok(point)
            })();
            if result.is_err() {
                done = true
            };
            Some(result)
        })
    }
}
impl Serialize for BenchmarkPoints {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let count = match self {
            Self::Empty => 0,
            Self::Stored { count, .. } => *count,
        };
        let mut sequence =
            serializer.serialize_seq(Some(usize::try_from(count).map_err(S::Error::custom)?))?;
        for point in self.iter() {
            sequence.serialize_element(
                &point.map_err(|error| S::Error::custom(format!("{error:?}")))?,
            )?;
        }
        sequence.end()
    }
}

pub(super) struct BenchmarkPointWriter {
    connection: Connection,
    scratch: Arc<OperationScratchDirectory>,
    count: u64,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl BenchmarkPointWriter {
    pub(super) fn new(
        scratch: Arc<OperationScratchDirectory>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Self, Error> {
        check(deadline, &cancellation)?;
        let connection = Connection::open(
            scratch
                .path()
                .join(format!("benchmark-{}.sqlite", uuid::Uuid::new_v4())),
        )
        .map_err(|_| Error::StorageUnavailable)?;
        connection.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE; PRAGMA cache_size=-2048; CREATE TABLE comparison(ordinal INTEGER PRIMARY KEY,payload BLOB NOT NULL); BEGIN IMMEDIATE")
            .map_err(|_|Error::StorageUnavailable)?;
        Ok(Self {
            connection,
            scratch,
            count: 0,
            deadline,
            cancellation,
        })
    }
    pub(super) fn push(&mut self, point: &BenchmarkHistoryPoint) -> Result<(), Error> {
        check(self.deadline, &self.cancellation)?;
        let payload = serde_json::to_vec(point).map_err(|_| Error::IntegrityUnproven)?;
        if payload.len() > 16 * 1024 {
            return Err(Error::CapacityExceeded);
        };
        self.connection
            .execute(
                "INSERT INTO comparison(ordinal,payload) VALUES(?1,?2)",
                params![
                    i64::try_from(self.count).map_err(|_| Error::CapacityExceeded)?,
                    payload
                ],
            )
            .map_err(|_| Error::StorageUnavailable)?;
        self.count = self.count.checked_add(1).ok_or(Error::CapacityExceeded)?;
        Ok(())
    }
    pub(super) fn finish(self) -> Result<BenchmarkPoints, Error> {
        check(self.deadline, &self.cancellation)?;
        self.connection
            .execute_batch("COMMIT")
            .map_err(|_| Error::StorageUnavailable)?;
        Ok(BenchmarkPoints::Stored {
            connection: Mutex::new(self.connection),
            _scratch: self.scratch,
            count: self.count,
            deadline: self.deadline,
            cancellation: self.cancellation,
        })
    }
}
