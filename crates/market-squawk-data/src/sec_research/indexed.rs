//! Filing-scoped immutable row indexes. Only the requested row is decoded into memory.
use super::SecResearchReadError;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use rusqlite::{Connection, OptionalExtension as _, params};
use serde::{
    Serialize,
    de::{DeserializeOwned, DeserializeSeed, SeqAccess, Visitor},
};
use sha2::{Digest as _, Sha256};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// One quota shared by all operation-owned SEC row and relationship indexes.
#[derive(Debug)]
pub(super) struct IndexScratch {
    directory: crate::OperationScratchDirectory,
    maximum_bytes: u64,
    used_bytes: Mutex<u64>,
    exhausted: AtomicBool,
}
impl IndexScratch {
    pub(super) fn new(directory: crate::OperationScratchDirectory, maximum_bytes: u64) -> Self {
        Self {
            directory,
            maximum_bytes,
            used_bytes: Mutex::new(0),
            exhausted: AtomicBool::new(false),
        }
    }
    pub(super) fn path(&self) -> &std::path::Path {
        self.directory.path()
    }
    pub(super) fn ensure_budget(&self) -> Result<(), SecResearchReadError> {
        if self.exhausted.load(Ordering::Relaxed) {
            Err(SecResearchReadError::SpillBudgetExceeded)
        } else {
            Ok(())
        }
    }
    fn remaining(&self) -> Result<u64, SecResearchReadError> {
        self.ensure_budget()?;
        let used = self
            .used_bytes
            .lock()
            .map_err(|_| SecResearchReadError::AuthorityUnavailable)?;
        self.maximum_bytes
            .checked_sub(*used)
            .ok_or(SecResearchReadError::SpillBudgetExceeded)
    }
}
#[derive(Debug)]
pub(super) struct IndexAllocation {
    scratch: Arc<IndexScratch>,
    charged: u64,
}
impl IndexAllocation {
    pub(super) fn new(scratch: Arc<IndexScratch>) -> Self {
        Self {
            scratch,
            charged: 0,
        }
    }
    pub(super) fn update(&mut self, connection: &Connection) -> Result<(), SecResearchReadError> {
        self.scratch.ensure_budget()?;
        let pages = u64::try_from(
            connection
                .prepare_cached("PRAGMA page_count")?
                .query_row([], |row| row.get::<_, i64>(0))?,
        )
        .map_err(|_| SecResearchReadError::SpillBudgetExceeded)?;
        let page_size = u64::try_from(
            connection
                .prepare_cached("PRAGMA page_size")?
                .query_row([], |row| row.get::<_, i64>(0))?,
        )
        .map_err(|_| SecResearchReadError::SpillBudgetExceeded)?;
        let bytes = pages
            .checked_mul(page_size)
            .ok_or(SecResearchReadError::SpillBudgetExceeded)?;
        let mut used = self
            .scratch
            .used_bytes
            .lock()
            .map_err(|_| SecResearchReadError::AuthorityUnavailable)?;
        let next = used
            .checked_sub(self.charged)
            .and_then(|used| used.checked_add(bytes))
            .filter(|used| *used <= self.scratch.maximum_bytes)
            .ok_or_else(|| {
                self.scratch.exhausted.store(true, Ordering::Relaxed);
                SecResearchReadError::SpillBudgetExceeded
            })?;
        *used = next;
        self.charged = bytes;
        Ok(())
    }
}
impl Drop for IndexAllocation {
    fn drop(&mut self) {
        if let Ok(mut used) = self.scratch.used_bytes.lock() {
            *used = used.saturating_sub(self.charged);
        }
    }
}

#[derive(Debug)]
struct RowIndex {
    connection: Mutex<Connection>,
    _directory: tempfile::TempDir,
    allocation: IndexAllocation,
}
/// Complete immutable source rows, indexed on disk and shared across cloned read receipts.
#[derive(Clone, Debug)]
pub struct SecResearchRows<T> {
    index: Arc<RowIndex>,
    count: usize,
    digest: EvidenceDigest,
    _row: PhantomData<T>,
}
impl<T> PartialEq for SecResearchRows<T> {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count && self.digest == other.digest
    }
}
impl<T> Eq for SecResearchRows<T> {}
impl<T> SecResearchRows<T> {
    pub(super) fn remaining_spill_bytes(&self) -> Result<u64, SecResearchReadError> {
        self.index.allocation.scratch.remaining()
    }
    pub const fn len(&self) -> usize {
        self.count
    }
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }
}
impl<T: DeserializeOwned> SecResearchRows<T> {
    /// Loads one original source ordinal. Missing or corrupt evidence is an explicit error.
    pub fn get(&self, ordinal: usize) -> Result<Option<T>, SecResearchReadError> {
        let connection = self
            .index
            .connection
            .lock()
            .map_err(|_| SecResearchReadError::AuthorityUnavailable)?;
        let bytes: Option<Vec<u8>> =
            connection
                .prepare_cached("SELECT payload FROM rows WHERE ordinal=?1")?
                .query_row(
                    [i64::try_from(ordinal)
                        .map_err(|_| SecResearchReadError::ObjectBudgetExceeded)?],
                    |row| row.get(0),
                )
                .optional()?;
        bytes
            .map(|bytes| {
                serde_json::from_slice(&bytes)
                    .map_err(|_| SecResearchReadError::ProviderBindingMismatch)
            })
            .transpose()
    }
    pub fn iter(&self) -> impl Iterator<Item = Result<T, SecResearchReadError>> + '_ {
        (0..self.count).map(|ordinal| {
            self.get(ordinal)?
                .ok_or(SecResearchReadError::ProviderBindingMismatch)
        })
    }
    pub(super) fn by_key(&self, key: &str) -> Result<Option<T>, SecResearchReadError> {
        let connection = self
            .index
            .connection
            .lock()
            .map_err(|_| SecResearchReadError::AuthorityUnavailable)?;
        let bytes: Option<Vec<u8>> = connection
            .prepare_cached("SELECT payload FROM rows WHERE row_key=?1 ORDER BY ordinal LIMIT 1")?
            .query_row([key], |row| row.get(0))
            .optional()?;
        bytes
            .map(|bytes| {
                serde_json::from_slice(&bytes)
                    .map_err(|_| SecResearchReadError::ProviderBindingMismatch)
            })
            .transpose()
    }
}
pub(super) struct RowsBuilder<T> {
    connection: Connection,
    directory: tempfile::TempDir,
    allocation: IndexAllocation,
    count: usize,
    digest: Sha256,
    _row: PhantomData<T>,
}
impl<T: Serialize> RowsBuilder<T> {
    pub(super) fn new(scratch: Arc<IndexScratch>) -> Result<Self, SecResearchReadError> {
        let directory = tempfile::Builder::new()
            .prefix("market-squawk-sec-rows-")
            .tempdir_in(scratch.path())
            .map_err(|_| SecResearchReadError::ObjectBudgetExceeded)?;
        let connection = Connection::open(directory.path().join("rows.sqlite3"))?;
        connection.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE; PRAGMA mmap_size=0; PRAGMA cache_size=-1024; CREATE TABLE rows(ordinal INTEGER PRIMARY KEY,row_key TEXT,payload BLOB NOT NULL); CREATE INDEX row_keys ON rows(row_key); BEGIN")?;
        Ok(Self {
            connection,
            directory,
            allocation: IndexAllocation::new(scratch),
            count: 0,
            digest: Sha256::new(),
            _row: PhantomData,
        })
    }
    pub(super) fn push(&mut self, row: &T) -> Result<(), SecResearchReadError> {
        let bytes =
            serde_json::to_vec(row).map_err(|_| SecResearchReadError::ProviderBindingMismatch)?;
        // The JSON key is read by SQLite, avoiding a second decoded ownership tree.
        self.connection.prepare_cached("INSERT INTO rows(ordinal,row_key,payload) VALUES (?1,json_extract(CAST(?2 AS TEXT),'$.context_id'),?2)")?.execute(params![i64::try_from(self.count).map_err(|_| SecResearchReadError::ObjectBudgetExceeded)?, bytes]).map_err(|error| {
            let error = SecResearchReadError::from(error);
            if matches!(error, SecResearchReadError::SpillBudgetExceeded) { self.allocation.scratch.exhausted.store(true, Ordering::Relaxed); }
            error
        })?;
        self.allocation.update(&self.connection)?;
        self.digest.update((bytes.len() as u64).to_be_bytes());
        self.digest.update(&bytes);
        self.count = self
            .count
            .checked_add(1)
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
        Ok(())
    }
    pub(super) fn finish(mut self) -> Result<SecResearchRows<T>, SecResearchReadError> {
        self.connection
            .execute_batch("COMMIT; PRAGMA query_only=ON")?;
        self.allocation.update(&self.connection)?;
        Ok(SecResearchRows {
            index: Arc::new(RowIndex {
                connection: Mutex::new(self.connection),
                _directory: self.directory,
                allocation: self.allocation,
            }),
            count: self.count,
            digest: EvidenceDigest::new(DigestAlgorithm::Sha256, self.digest.finalize().into()),
            _row: PhantomData,
        })
    }
}
pub(super) struct RowsSeed<T> {
    pub(super) scratch: Arc<IndexScratch>,
    pub(super) row: PhantomData<T>,
}
impl<'de, T: DeserializeOwned + Serialize> DeserializeSeed<'de> for RowsSeed<T> {
    type Value = SecResearchRows<T>;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        struct RowsVisitor<T> {
            scratch: Arc<IndexScratch>,
            row: PhantomData<T>,
        }
        impl<'de, T: DeserializeOwned + Serialize> Visitor<'de> for RowsVisitor<T> {
            type Value = SecResearchRows<T>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("complete indexed filing rows")
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut rows = RowsBuilder::new(self.scratch).map_err(serde::de::Error::custom)?;
                while let Some(row) = sequence.next_element::<T>()? {
                    rows.push(&row).map_err(serde::de::Error::custom)?;
                }
                rows.finish().map_err(serde::de::Error::custom)
            }
        }
        deserializer.deserialize_seq(RowsVisitor {
            scratch: self.scratch,
            row: PhantomData,
        })
    }
}
