//! Ordered private signal staging with bounded coordinate reads.
use super::BacktestObservation;
use crate::engine::BacktestError;
use market_squawk_data::FeatureDatasetInputCoordinateHandle;
use market_squawk_domain::{InstrumentId, Timestamp};
use rusqlite::{Connection, params};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
struct Storage {
    connection: Mutex<Connection>,
    _directory: tempfile::TempDir,
    authority: Option<Arc<market_squawk_data::OperationScratchDirectory>>,
    coordinate_source: Option<FeatureDatasetInputCoordinateHandle>,
}
#[derive(Clone, Debug)]
pub(crate) struct ObservationStore {
    storage: Arc<Storage>,
    count: usize,
    first: Option<BacktestObservation>,
    last: Option<BacktestObservation>,
}
impl ObservationStore {
    pub(super) fn new() -> Result<Self, BacktestError> {
        Self::with_scratch(None)
    }
    pub(super) fn with_scratch(
        authority: Option<Arc<market_squawk_data::OperationScratchDirectory>>,
    ) -> Result<Self, BacktestError> {
        let mut builder = tempfile::Builder::new();
        builder.prefix("signals-");
        let directory = match &authority {
            Some(owner) => builder.tempdir_in(owner.path()),
            None => builder.tempdir(),
        }
        .map_err(|_| BacktestError::LimitExceeded)?;
        let connection = Connection::open(directory.path().join("signals.sqlite"))
            .map_err(|_| BacktestError::LimitExceeded)?;
        connection.execute_batch("PRAGMA cache_size=-64; PRAGMA temp_store=FILE; CREATE TABLE observations(decision INTEGER NOT NULL,instrument BLOB NOT NULL,payload BLOB NOT NULL,coordinate INTEGER, PRIMARY KEY(decision,instrument)) WITHOUT ROWID; BEGIN IMMEDIATE;")
            .map_err(|_| BacktestError::LimitExceeded)?;
        Ok(Self {
            storage: Arc::new(Storage {
                connection: Mutex::new(connection),
                _directory: directory,
                authority,
                coordinate_source: None,
            }),
            count: 0,
            first: None,
            last: None,
        })
    }
    pub(super) fn push(&mut self, observation: BacktestObservation) -> Result<(), BacktestError> {
        let storage = Arc::get_mut(&mut self.storage).ok_or(BacktestError::InvalidDataset)?;
        if storage.coordinate_source.is_none() {
            storage.coordinate_source = observation.input_coordinate.as_deref().cloned();
        }
        let payload =
            serde_json::to_vec(&observation).map_err(|_| BacktestError::InvalidDataset)?;
        storage
            .connection
            .get_mut()
            .map_err(|_| BacktestError::InvalidDataset)?
            .execute(
                "INSERT INTO observations VALUES(?1,?2,?3,?4)",
                params![
                    observation.decision_at.unix_nanos(),
                    observation.instrument_id().as_uuid().as_bytes().as_slice(),
                    payload,
                    observation
                        .input_coordinate
                        .as_ref()
                        .map(|v| v.ordinal() as i64)
                ],
            )
            .map_err(|_| BacktestError::InvalidDataset)?;
        if self.first.as_ref().is_none_or(|v| {
            (observation.decision_at, observation.instrument_id())
                < (v.decision_at, v.instrument_id())
        }) {
            self.first = Some(observation.clone());
        }
        if self.last.as_ref().is_none_or(|v| {
            (observation.decision_at, observation.instrument_id())
                > (v.decision_at, v.instrument_id())
        }) {
            self.last = Some(observation);
        }
        self.count = self
            .count
            .checked_add(1)
            .ok_or(BacktestError::LimitExceeded)?;
        Ok(())
    }
    pub(super) fn finish(self) -> Result<Self, BacktestError> {
        self.storage
            .connection
            .lock()
            .map_err(|_| BacktestError::InvalidDataset)?
            .execute_batch("COMMIT; PRAGMA query_only=ON;")
            .map_err(|_| BacktestError::LimitExceeded)?;
        Ok(self)
    }
    pub(crate) fn from_observations(
        values: impl IntoIterator<Item = BacktestObservation>,
    ) -> Result<Self, BacktestError> {
        let mut store = Self::new()?;
        for value in values {
            store.push(value)?;
        }
        store.finish()
    }
    pub(crate) fn operation_scratch(
        &self,
    ) -> Option<Arc<market_squawk_data::OperationScratchDirectory>> {
        self.storage.authority.clone()
    }
    pub(crate) const fn len(&self) -> usize {
        self.count
    }
    pub(crate) fn first(&self) -> Option<&BacktestObservation> {
        self.first.as_ref()
    }
    pub(crate) fn last(&self) -> Option<&BacktestObservation> {
        self.last.as_ref()
    }
    pub(crate) fn iter(&self) -> ObservationCursor {
        self.from(Timestamp::from_unix_nanos(i64::MIN))
    }
    pub(crate) fn from(&self, starts_at: Timestamp) -> ObservationCursor {
        ObservationCursor {
            store: self.clone(),
            starts_at,
            after: None,
            page: VecDeque::new(),
            done: false,
        }
    }
    pub(crate) fn find(
        &self,
        decision: Timestamp,
        instrument: InstrumentId,
    ) -> Result<BacktestObservation, BacktestError> {
        let connection = self
            .storage
            .connection
            .lock()
            .map_err(|_| BacktestError::InvalidDataset)?;
        let (payload, ordinal): (Vec<u8>, Option<i64>) = connection
            .query_row(
                "SELECT payload,coordinate FROM observations WHERE decision=?1 AND instrument=?2",
                params![
                    decision.unix_nanos(),
                    instrument.as_uuid().as_bytes().as_slice()
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| BacktestError::InvalidDataset)?;
        self.decode(&payload, ordinal)
    }
    pub(crate) fn first_for_instrument(
        &self,
        instrument: InstrumentId,
    ) -> Result<BacktestObservation, BacktestError> {
        let connection = self
            .storage
            .connection
            .lock()
            .map_err(|_| BacktestError::InvalidDataset)?;
        let (payload,ordinal):(Vec<u8>,Option<i64>)=connection.query_row("SELECT payload,coordinate FROM observations WHERE instrument=?1 ORDER BY decision LIMIT 1", [instrument.as_uuid().as_bytes().as_slice()],|row|Ok((row.get(0)?,row.get(1)?))).map_err(|_|BacktestError::InvalidDataset)?;
        self.decode(&payload, ordinal)
    }
    fn decode(
        &self,
        bytes: &[u8],
        ordinal: Option<i64>,
    ) -> Result<BacktestObservation, BacktestError> {
        let mut value: BacktestObservation =
            serde_json::from_slice(bytes).map_err(|_| BacktestError::InvalidDataset)?;
        value.input_coordinate = ordinal
            .map(|ordinal| {
                self.storage
                    .coordinate_source
                    .as_ref()
                    .and_then(|source| {
                        usize::try_from(ordinal)
                            .ok()
                            .and_then(|ordinal| source.at(ordinal))
                    })
                    .map(Box::new)
                    .ok_or(BacktestError::InvalidDataset)
            })
            .transpose()?;
        Ok(value)
    }
}
#[derive(Debug)]
pub(crate) struct ObservationCursor {
    store: ObservationStore,
    starts_at: Timestamp,
    after: Option<(i64, Vec<u8>)>,
    page: VecDeque<BacktestObservation>,
    done: bool,
}
impl ObservationCursor {
    fn refill(&mut self) -> Result<(), BacktestError> {
        let connection = self
            .store
            .storage
            .connection
            .lock()
            .map_err(|_| BacktestError::InvalidDataset)?;
        let (sql, timestamp, instrument) = match &self.after {
            Some((t, i)) => (
                "SELECT payload,coordinate FROM observations WHERE (decision,instrument)>(?1,?2) ORDER BY decision,instrument LIMIT 128",
                *t,
                i.as_slice(),
            ),
            None => (
                "SELECT payload,coordinate FROM observations WHERE (decision,instrument)>=(?1,?2) ORDER BY decision,instrument LIMIT 128",
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
            let ordinal: Option<i64> = row.get(1).map_err(|_| BacktestError::InvalidDataset)?;
            self.page.push_back(self.store.decode(&payload, ordinal)?);
        }
        self.done = self.page.len() < 128;
        Ok(())
    }
}
impl Iterator for ObservationCursor {
    type Item = Result<BacktestObservation, BacktestError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.page.is_empty() && !self.done {
            if let Err(error) = self.refill() {
                self.done = true;
                return Some(Err(error));
            }
        }
        let value = self.page.pop_front()?;
        self.after = Some((
            value.decision_at.unix_nanos(),
            value.instrument_id().as_uuid().as_bytes().to_vec(),
        ));
        Some(Ok(value))
    }
}

impl ObservationStore {
    pub(crate) fn panels(&self) -> ObservationPanels {
        ObservationPanels {
            cursor: self.iter(),
            pending: None,
        }
    }
}
#[derive(Debug)]
pub(crate) struct ObservationPanels {
    cursor: ObservationCursor,
    pending: Option<BacktestObservation>,
}
impl Iterator for ObservationPanels {
    type Item = Result<Vec<BacktestObservation>, BacktestError>;
    fn next(&mut self) -> Option<Self::Item> {
        let first = match self.pending.take().map(Ok).or_else(|| self.cursor.next())? {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };
        let at = first.decision_at;
        let mut values = vec![first];
        for value in self.cursor.by_ref() {
            match value {
                Ok(value) if value.decision_at == at => values.push(value),
                Ok(value) => {
                    self.pending = Some(value);
                    break;
                }
                Err(error) => return Some(Err(error)),
            }
        }
        Some(Ok(values))
    }
}
