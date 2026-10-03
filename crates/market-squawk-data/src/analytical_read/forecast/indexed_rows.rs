//! Operation-owned forecast evidence: complete rows on disk, one decoded row per cursor step.
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use market_squawk_domain::InstrumentId;
use rusqlite::{Connection, OptionalExtension as _, params};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{AnalyticalReadError, DatasetSplit, ForecastFeatureRow, Sha256Digest};

type Result<T> = std::result::Result<T, AnalyticalReadError>;

#[derive(Debug)]
struct Quota {
    maximum: u64,
    used: Mutex<u64>,
}
#[derive(Debug)]
struct Allocation {
    quota: Arc<Quota>,
    bytes: u64,
}
impl Drop for Allocation {
    fn drop(&mut self) {
        if let Ok(mut used) = self.quota.used.lock() {
            *used = used.saturating_sub(self.bytes);
        }
    }
}
#[derive(Debug)]
struct RowIndex {
    connection: Mutex<Connection>,
    _directory: tempfile::TempDir,
    scratch: Arc<crate::OperationScratchDirectory>,
    allocation: Allocation,
    working_bytes: usize,
    deadline: Instant,
    cancellation: CancellationToken,
}
/// Complete verified selected rows. Iteration decodes one row and propagates storage failures.
#[derive(Clone, Debug)]
pub struct ForecastFeatureRows {
    index: Arc<RowIndex>,
    count: usize,
}
impl ForecastFeatureRows {
    pub const fn len(&self) -> usize {
        self.count
    }
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }
    /// Visits original immutable object order, preserving the selection-fence hash order.
    pub fn iter(&self) -> impl Iterator<Item = Result<ForecastFeatureRow>> + '_ {
        (0..self.count).map(|ordinal| {
            self.control()?;
            let connection = self.index.connection.lock().map_err(|_| corrupt())?;
            let value = connection
                .query_row(
                    "SELECT payload,digest FROM rows WHERE ordinal=?1",
                    [i64::try_from(ordinal).map_err(|_| corrupt())?],
                    |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
                )
                .map_err(storage)?;
            decode(value.0, value.1)
        })
    }
    /// Visits instrument, terminal target, then original ordinal order without a full-memory sort.
    pub fn iter_by_target(&self) -> impl Iterator<Item = Result<ForecastFeatureRow>> + '_ {
        TargetCursor::new(self, None)
    }
    /// Selects one component, optionally one instrument, in instrument/target/original-row order.
    pub fn iter_component(
        &self,
        instrument: Option<InstrumentId>,
        kind: u8,
        name: &str,
        version: u32,
    ) -> impl Iterator<Item = Result<ForecastFeatureRow>> + '_ {
        TargetCursor::new(
            self,
            Some(ComponentFilter {
                instrument,
                kind,
                name: name.to_owned(),
                version,
            }),
        )
    }
    fn control(&self) -> Result<()> {
        check_control(self.index.deadline, &self.index.cancellation)
    }

    /// Externally sorts every caller-derived row digest, retaining duplicates and exact count.
    /// Callback errors keep their original type; all rows still originate in this sealed authority.
    pub fn sorted_row_digests<E>(
        &self,
        mut digest: impl FnMut(&ForecastFeatureRow) -> std::result::Result<[u8; 32], E>,
        map_error: impl Fn(AnalyticalReadError) -> E,
    ) -> std::result::Result<ForecastSortedDigests, E> {
        self.control().map_err(&map_error)?;
        // Reserve against the same operation allowance while constructing the child sort index.
        let mut used = self
            .index
            .allocation
            .quota
            .used
            .lock()
            .map_err(|_| map_error(corrupt()))?;
        let remaining = self
            .index
            .allocation
            .quota
            .maximum
            .checked_sub(*used)
            .ok_or_else(|| map_error(spill()))?;
        let directory = tempfile::Builder::new()
            .prefix("forecast-digests-")
            .tempdir_in(self.index.scratch.path())
            .map_err(|_| map_error(spill()))?;
        let connection = Connection::open(directory.path().join("digests.sqlite3"))
            .map_err(|e| map_error(storage(e)))?;
        configure(
            &connection,
            self.index.working_bytes,
            remaining,
            self.index.deadline,
            &self.index.cancellation,
        )
        .map_err(&map_error)?;
        connection.execute_batch("CREATE TABLE digests(digest BLOB NOT NULL,ordinal INTEGER NOT NULL,PRIMARY KEY(digest,ordinal)) WITHOUT ROWID; BEGIN;")
            .map_err(|e|map_error(storage(e)))?;
        {
            let mut insert = connection
                .prepare("INSERT INTO digests VALUES(?1,?2)")
                .map_err(|e| map_error(storage(e)))?;
            for (ordinal, row) in self.iter().enumerate() {
                let row = row.map_err(&map_error)?;
                let digest = digest(&row)?;
                insert
                    .execute(params![
                        digest.as_slice(),
                        i64::try_from(ordinal).map_err(|_| map_error(corrupt()))?
                    ])
                    .map_err(|e| map_error(storage(e)))?;
            }
        }
        connection
            .execute_batch("COMMIT; PRAGMA query_only=ON")
            .map_err(|e| map_error(storage(e)))?;
        let bytes = database_bytes(&connection).map_err(&map_error)?;
        *used = used
            .checked_add(bytes)
            .filter(|total| *total <= self.index.allocation.quota.maximum)
            .ok_or_else(|| map_error(spill()))?;
        drop(used);
        Ok(ForecastSortedDigests {
            connection: Mutex::new(connection),
            _directory: directory,
            _allocation: Allocation {
                quota: Arc::clone(&self.index.allocation.quota),
                bytes,
            },
            _rows: self.clone(),
            count: self.count,
        })
    }
}

/// A bounded digest-order cursor owned by the same complete forecast evidence operation.
#[derive(Debug)]
pub struct ForecastSortedDigests {
    connection: Mutex<Connection>,
    _directory: tempfile::TempDir,
    _allocation: Allocation,
    _rows: ForecastFeatureRows,
    count: usize,
}
impl ForecastSortedDigests {
    pub const fn len(&self) -> usize {
        self.count
    }
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn iter(&self) -> impl Iterator<Item = Result<[u8; 32]>> + '_ {
        let mut previous: Option<([u8; 32], i64)> = None;
        let mut failed = false;
        (0..self.count).map(move |_| {
            if failed{return Err(corrupt());}
            let result=(||{
                self._rows.control()?;
                let connection=self.connection.lock().map_err(|_|corrupt())?;
                let (bytes,ordinal):(Vec<u8>,i64)=match previous {
                    Some((digest,ordinal))=>connection.query_row("SELECT digest,ordinal FROM digests WHERE (digest,ordinal)>(?1,?2) ORDER BY digest,ordinal LIMIT 1",params![digest.as_slice(),ordinal],|row|Ok((row.get(0)?,row.get(1)?))),
                    None=>connection.query_row("SELECT digest,ordinal FROM digests ORDER BY digest,ordinal LIMIT 1",[],|row|Ok((row.get(0)?,row.get(1)?))),
                }.map_err(storage)?;
                let digest=bytes.try_into().map_err(|_|corrupt())?;
                previous=Some((digest,ordinal));Ok(digest)
            })();
            failed=result.is_err();result
        })
    }
}
struct ComponentFilter {
    instrument: Option<InstrumentId>,
    kind: u8,
    name: String,
    version: u32,
}
struct TargetCursor<'a> {
    rows: &'a ForecastFeatureRows,
    filter: Option<ComponentFilter>,
    previous: Option<(Vec<u8>, i64, i64)>,
    done: bool,
}
impl<'a> TargetCursor<'a> {
    fn new(rows: &'a ForecastFeatureRows, filter: Option<ComponentFilter>) -> Self {
        Self {
            rows,
            filter,
            previous: None,
            done: false,
        }
    }
}
impl Iterator for TargetCursor<'_> {
    type Item = Result<ForecastFeatureRow>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let result = (|| {
            self.rows.control()?;
            let connection = self.rows.index.connection.lock().map_err(|_| corrupt())?;
            let (prior_instrument, prior_target, prior_ordinal) = self
                .previous
                .as_ref()
                .map(|(instrument, target, ordinal)| (instrument.as_slice(), *target, *ordinal))
                .unwrap_or((&[][..], i64::MIN, -1));
            let instrument = self
                .filter
                .as_ref()
                .and_then(|f| f.instrument)
                .map(|i| *i.as_uuid().as_bytes());
            let read = |row: &rusqlite::Row<'_>| -> rusqlite::Result<(Vec<u8>, Vec<u8>, Vec<u8>, i64, i64)> {
                Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))
            };
            let value = if let Some(filter) = &self.filter {
                if let Some(instrument) = instrument {
                    connection.query_row(
                        "SELECT payload,digest,instrument,target,ordinal FROM rows WHERE kind=?4 AND name=?5 AND version=?6 AND instrument=?7 AND (instrument,target,ordinal)>(?1,?2,?3) ORDER BY instrument,target,ordinal LIMIT 1",
                        params![prior_instrument,prior_target,prior_ordinal,filter.kind,filter.name,filter.version,instrument.as_slice()],read)
                } else {
                    connection.query_row(
                        "SELECT payload,digest,instrument,target,ordinal FROM rows WHERE kind=?4 AND name=?5 AND version=?6 AND (instrument,target,ordinal)>(?1,?2,?3) ORDER BY instrument,target,ordinal LIMIT 1",
                        params![prior_instrument,prior_target,prior_ordinal,filter.kind,filter.name,filter.version],read)
                }
            } else {
                connection.query_row(
                    "SELECT payload,digest,instrument,target,ordinal FROM rows WHERE (instrument,target,ordinal)>(?1,?2,?3) ORDER BY instrument,target,ordinal LIMIT 1",
                    params![prior_instrument,prior_target,prior_ordinal],read)
            }.optional().map_err(storage)?;
            let Some((bytes, digest, instrument, target, ordinal)) = value else {
                return Ok(None);
            };
            let row = decode(bytes, digest)?;
            if instrument != row.instrument_id.as_uuid().as_bytes().as_slice()
                || target != row.label_effective_at.map_or(i64::MIN, |t| t.unix_nanos())
                || self.filter.as_ref().is_some_and(|filter| {
                    filter
                        .instrument
                        .is_some_and(|instrument| instrument != row.instrument_id)
                        || filter.kind != row.component_kind
                        || filter.name != row.component_name.as_ref()
                        || filter.version != row.component_version
                })
            {
                return Err(corrupt());
            }
            self.previous = Some((instrument, target, ordinal));
            Ok(Some(row))
        })();
        match result {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}

pub(super) struct RowsBuilder {
    connection: Connection,
    directory: tempfile::TempDir,
    scratch: Arc<crate::OperationScratchDirectory>,
    working_bytes: usize,
    maximum: u64,
    count: usize,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl RowsBuilder {
    pub(super) fn new(
        scratch: Arc<crate::OperationScratchDirectory>,
        working_bytes: usize,
        maximum: u64,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        check_control(deadline, &cancellation)?;
        let directory = tempfile::Builder::new()
            .prefix("forecast-rows-")
            .tempdir_in(scratch.path())
            .map_err(|_| spill())?;
        let connection =
            Connection::open(directory.path().join("rows.sqlite3")).map_err(storage)?;
        configure(&connection, working_bytes, maximum, deadline, &cancellation)?;
        connection.execute_batch("CREATE TABLE rows(ordinal INTEGER PRIMARY KEY,instrument BLOB NOT NULL,target INTEGER NOT NULL,kind INTEGER NOT NULL,name TEXT NOT NULL,version INTEGER NOT NULL,payload BLOB NOT NULL,digest BLOB NOT NULL); CREATE INDEX target_order ON rows(instrument,target,ordinal); CREATE INDEX component_order ON rows(kind,name,version,instrument,target,ordinal); BEGIN;").map_err(storage)?;
        Ok(Self {
            connection,
            directory,
            scratch,
            working_bytes,
            maximum,
            count: 0,
            deadline,
            cancellation,
        })
    }
    pub(super) const fn len(&self) -> usize {
        self.count
    }
    pub(super) fn push(&mut self, row: &ForecastFeatureRow) -> Result<()> {
        check_control(self.deadline, &self.cancellation)?;
        let mut output = BoundedWriter {
            bytes: Vec::new(),
            limit: self.working_bytes / 4,
        };
        serde_json::to_writer(&mut output, &StoredRow { row: row.clone() }).map_err(|_| {
            AnalyticalReadError::Query(crate::QueryError::MemoryLimitExceeded {
                limit: self.working_bytes as u64,
            })
        })?;
        let digest: [u8; 32] = Sha256::digest(&output.bytes).into();
        self.connection
            .execute(
                "INSERT INTO rows VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    i64::try_from(self.count).map_err(|_| corrupt())?,
                    row.instrument_id.as_uuid().as_bytes().as_slice(),
                    row.label_effective_at.map_or(i64::MIN, |v| v.unix_nanos()),
                    row.component_kind,
                    row.component_name.as_ref(),
                    row.component_version,
                    output.bytes,
                    digest.as_slice(),
                ],
            )
            .map_err(storage)?;
        self.count = self.count.checked_add(1).ok_or_else(corrupt)?;
        Ok(())
    }
    pub(super) fn finish(self) -> Result<ForecastFeatureRows> {
        check_control(self.deadline, &self.cancellation)?;
        self.connection
            .execute_batch("COMMIT; PRAGMA query_only=ON")
            .map_err(storage)?;
        let bytes = database_bytes(&self.connection)?;
        if bytes > self.maximum {
            return Err(spill());
        }
        let quota = Arc::new(Quota {
            maximum: self.maximum,
            used: Mutex::new(bytes),
        });
        Ok(ForecastFeatureRows {
            index: Arc::new(RowIndex {
                connection: Mutex::new(self.connection),
                _directory: self.directory,
                scratch: self.scratch,
                allocation: Allocation { quota, bytes },
                working_bytes: self.working_bytes,
                deadline: self.deadline,
                cancellation: self.cancellation,
            }),
            count: self.count,
        })
    }
}
fn configure(
    connection: &Connection,
    working_bytes: usize,
    maximum: u64,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<()> {
    if working_bytes < 16 * 1024 || maximum < 4096 {
        return Err(AnalyticalReadError::InvalidLimit);
    }
    let token = cancellation.clone();
    connection
        .progress_handler(
            1024,
            Some(move || token.is_cancelled() || Instant::now() >= deadline),
        )
        .map_err(storage)?;
    connection.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE; PRAGMA mmap_size=0;").map_err(storage)?;
    connection
        .pragma_update(
            None,
            "cache_size",
            -i64::try_from((working_bytes / 8).clamp(8192, 2 * 1024 * 1024) / 1024)
                .map_err(|_| corrupt())?,
        )
        .map_err(storage)?;
    connection
        .pragma_update(
            None,
            "max_page_count",
            i64::try_from(maximum / 4096).map_err(|_| spill())?,
        )
        .map_err(storage)?;
    connection
        .set_limit(
            rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH,
            i32::try_from(working_bytes / 2).unwrap_or(i32::MAX),
        )
        .map_err(storage)?;
    Ok(())
}
fn database_bytes(connection: &Connection) -> Result<u64> {
    let pages: i64 = connection
        .pragma_query_value(None, "page_count", |row| row.get(0))
        .map_err(storage)?;
    let size: i64 = connection
        .pragma_query_value(None, "page_size", |row| row.get(0))
        .map_err(storage)?;
    u64::try_from(pages)
        .ok()
        .and_then(|pages| pages.checked_mul(u64::try_from(size).ok()?))
        .ok_or_else(spill)
}
fn decode(bytes: Vec<u8>, digest: Vec<u8>) -> Result<ForecastFeatureRow> {
    if digest.as_slice() != Sha256::digest(&bytes).as_slice() {
        return Err(corrupt());
    }
    serde_json::from_slice::<StoredRow>(&bytes)
        .map(|stored| stored.row)
        .map_err(|_| corrupt())
}
pub(super) fn check_control(deadline: Instant, cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(AnalyticalReadError::Query(crate::QueryError::Cancelled))
    } else if Instant::now() >= deadline {
        Err(AnalyticalReadError::Query(
            crate::QueryError::DeadlineExceeded,
        ))
    } else {
        Ok(())
    }
}
fn corrupt() -> AnalyticalReadError {
    crate::PythonDatasetCatalogError::CorruptAdmission.into()
}
fn spill() -> AnalyticalReadError {
    crate::QueryError::SpillStorageExhausted.into()
}
fn storage(error: rusqlite::Error) -> AnalyticalReadError {
    match error.sqlite_error_code() {
        Some(rusqlite::ErrorCode::DiskFull) => spill(),
        Some(rusqlite::ErrorCode::OperationInterrupted) => crate::QueryError::Cancelled.into(),
        _ => corrupt(),
    }
}
struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.limit)
        {
            return Err(std::io::Error::other("forecast row memory exhausted"));
        }
        self.bytes
            .try_reserve_exact(bytes.len())
            .map_err(|_| std::io::Error::other("forecast row allocation failed"))?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) mod digest_serde {
    use super::Sha256Digest;
    use serde::{Deserialize as _, Deserializer, Serialize as _, Serializer};
    pub fn serialize<S: Serializer>(
        value: &Sha256Digest,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        value.bytes().serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Sha256Digest, D::Error> {
        Ok(Sha256Digest::new(<[u8; 32]>::deserialize(deserializer)?))
    }
}
pub(super) mod optional_digest_serde {
    use super::Sha256Digest;
    use serde::{Deserialize as _, Deserializer, Serialize as _, Serializer};
    pub fn serialize<S: Serializer>(
        value: &Option<Sha256Digest>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        value.map(Sha256Digest::bytes).serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Option<Sha256Digest>, D::Error> {
        Ok(Option::<[u8; 32]>::deserialize(deserializer)?.map(Sha256Digest::new))
    }
}
pub(super) mod split_serde {
    use super::DatasetSplit;
    use serde::{Deserialize as _, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(
        value: &DatasetSplit,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_u8(match value {
            DatasetSplit::Train => 1,
            DatasetSplit::Validation => 2,
            DatasetSplit::Test => 3,
        })
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<DatasetSplit, D::Error> {
        match u8::deserialize(deserializer)? {
            1 => Ok(DatasetSplit::Train),
            2 => Ok(DatasetSplit::Validation),
            3 => Ok(DatasetSplit::Test),
            _ => Err(serde::de::Error::custom("invalid dataset split")),
        }
    }
}

// The wire form is private; callers cannot deserialize a verified ForecastFeatureRow directly.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(remote = "ForecastFeatureRow")]
struct ForecastRowWire {
    example_id: Box<str>,
    instrument_id: InstrumentId,
    source_selection_as_of: market_squawk_domain::Timestamp,
    label_selection_as_of: Option<market_squawk_domain::Timestamp>,
    decision_coordinate: market_squawk_domain::ResearchTemporalCoordinate,
    observed_effective_at: Option<market_squawk_domain::Timestamp>,
    label_effective_at: Option<market_squawk_domain::Timestamp>,
    target_coordinate_kind: u8,
    #[serde(with = "split_serde")]
    split: DatasetSplit,
    component_kind: u8,
    component_name: Box<str>,
    component_version: u32,
    #[serde(with = "value_serde")]
    value: super::ForecastFeatureValue,
    #[serde(with = "digest_serde")]
    lineage_sha256: Sha256Digest,
    origin_basis: Option<crate::FixedHorizonOriginBasis>,
    #[serde(with = "optional_digest_serde")]
    origin_series_sha256: Option<Sha256Digest>,
    #[serde(with = "optional_digest_serde")]
    origin_observation_sha256: Option<Sha256Digest>,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredRow {
    #[serde(with = "ForecastRowWire")]
    row: ForecastFeatureRow,
}

mod value_serde {
    use super::super::ForecastFeatureValue;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    #[derive(Serialize, Deserialize)]
    enum ValueWire {
        Float(u64),
        Decimal { mantissa: i128, scale: u8 },
        Missing,
    }
    pub fn serialize<S: Serializer>(
        value: &ForecastFeatureValue,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match value {
            ForecastFeatureValue::Float(value) => ValueWire::Float(value.to_bits()),
            ForecastFeatureValue::Decimal { mantissa, scale } => ValueWire::Decimal {
                mantissa: *mantissa,
                scale: *scale,
            },
            ForecastFeatureValue::Missing => ValueWire::Missing,
        }
        .serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<ForecastFeatureValue, D::Error> {
        Ok(match ValueWire::deserialize(deserializer)? {
            ValueWire::Float(bits) => ForecastFeatureValue::Float(f64::from_bits(bits)),
            ValueWire::Decimal { mantissa, scale } => {
                ForecastFeatureValue::Decimal { mantissa, scale }
            }
            ValueWire::Missing => ForecastFeatureValue::Missing,
        })
    }
}
