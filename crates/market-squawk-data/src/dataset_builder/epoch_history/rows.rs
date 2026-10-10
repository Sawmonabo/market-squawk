//! Operation-owned repeatable rows; serialization retains the original canonical JSON array.
use super::*;
use serde::ser::{Error as _, SerializeSeq};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf,
    sync::Arc,
};

const MAX_ROW_BYTES: usize = 16 * 1024;

#[derive(Debug)]
pub(super) struct HistoryRows {
    _scratch: Arc<crate::OperationScratchDirectory>,
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    count: usize,
    last: Option<ForecastBasisHistoryRow>,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl HistoryRows {
    pub(super) fn new(
        scratch: Arc<crate::OperationScratchDirectory>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Self, DatasetBuildError> {
        check(deadline, &cancellation)?;
        let path = scratch
            .path()
            .join(format!("forecast-basis-{}.rows", uuid::Uuid::new_v4()));
        let writer = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|_| invalid())?;
        Ok(Self {
            _scratch: scratch,
            path,
            writer: Some(BufWriter::new(writer)),
            count: 0,
            last: None,
            deadline,
            cancellation,
        })
    }
    pub(super) fn push(&mut self, row: ForecastBasisHistoryRow) -> Result<(), DatasetBuildError> {
        check(self.deadline, &self.cancellation)?;
        let bytes = serde_json::to_vec(&row).map_err(|_| invalid())?;
        if bytes.len() > MAX_ROW_BYTES {
            return Err(DatasetBuildError::LimitExceeded);
        };
        let length = u32::try_from(bytes.len()).map_err(|_| invalid())?;
        let writer = self.writer.as_mut().ok_or_else(invalid)?;
        writer
            .write_all(&length.to_be_bytes())
            .and_then(|()| writer.write_all(&bytes))
            .map_err(|_| invalid())?;
        self.count = self
            .count
            .checked_add(1)
            .ok_or(DatasetBuildError::LimitExceeded)?;
        self.last = Some(row);
        Ok(())
    }
    pub(super) fn finish(&mut self) -> Result<(), DatasetBuildError> {
        self.writer
            .take()
            .ok_or_else(invalid)?
            .flush()
            .map_err(|_| invalid())
    }
    pub(super) fn len(&self) -> usize {
        self.count
    }
    pub(super) fn last(&self) -> Option<&ForecastBasisHistoryRow> {
        self.last.as_ref()
    }
    pub(super) fn iter(
        &self,
    ) -> impl Iterator<Item = Result<ForecastBasisHistoryRow, DatasetBuildError>> + '_ {
        let mut reader = File::open(&self.path)
            .map(BufReader::new)
            .map_err(|_| invalid());
        let mut index = 0;
        let mut done = false;
        std::iter::from_fn(move || {
            if done || index == self.count {
                return None;
            };
            let result = (|| {
                check(self.deadline, &self.cancellation)?;
                if self.writer.is_some() {
                    return Err(invalid());
                };
                let reader = reader.as_mut().map_err(|_| invalid())?;
                let mut length = [0; 4];
                reader.read_exact(&mut length).map_err(|_| invalid())?;
                let length = usize::try_from(u32::from_be_bytes(length)).map_err(|_| invalid())?;
                if length > MAX_ROW_BYTES {
                    return Err(invalid());
                };
                let mut bytes = vec![0; length];
                reader.read_exact(&mut bytes).map_err(|_| invalid())?;
                let row = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
                index += 1;
                if index == self.count {
                    let mut trailing = [0];
                    if reader.read(&mut trailing).map_err(|_| invalid())? != 0 {
                        return Err(invalid());
                    }
                }
                Ok(row)
            })();
            if result.is_err() {
                done = true
            };
            Some(result)
        })
    }
}
impl Serialize for HistoryRows {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.count))?;
        for row in self.iter() {
            sequence.serialize_element(&row.map_err(S::Error::custom)?)?;
        }
        sequence.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_rows_preserve_canonical_commitment_gaps_and_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let mut stored = HistoryRows::new(
            Arc::new(crate::OperationScratchDirectory::for_test()?),
            Instant::now() + std::time::Duration::from_secs(5),
            cancellation.clone(),
        )?;
        let row = ForecastBasisHistoryRow {
            native_date: CalendarDate::new(2026, 9, 1)?,
            session_open: Timestamp::from_unix_nanos(1),
            session_close: Timestamp::from_unix_nanos(2),
            nominal_date: None,
            provider_timestamp: None,
            completed_at: None,
            observed_at: Timestamp::from_unix_nanos(2),
            raw_available_at: None,
            available_at: None,
            quality: None,
            original_bar_identity: None,
            prices: None,
        };
        let mut observed = row.clone();
        observed.observed_at = Timestamp::from_unix_nanos(3);
        observed.available_at = Some(Timestamp::from_unix_nanos(4));
        observed.original_bar_identity = Some([5; 32]);
        let price = Money::new(
            Decimal::new(123450, 4),
            market_squawk_domain::Currency::try_from("USD")?,
        );
        observed.prices = Some(ForecastBasisOhlc {
            open: price,
            high: price,
            low: price,
            close: price,
        });
        let originals = vec![row, observed];
        for row in &originals {
            stored.push(row.clone())?;
        }
        stored.finish()?;
        assert_eq!(
            serde_json::to_vec(&originals)?,
            serde_json::to_vec(&stored)?
        );
        assert_eq!(hash(b"test", &originals)?, hash(b"test", &stored)?);
        assert_eq!(stored.iter().collect::<Result<Vec<_>, _>>()?.len(), 2);
        cancellation.cancel();
        assert!(matches!(
            stored.iter().next(),
            Some(Err(DatasetBuildError::Cancelled))
        ));
        Ok(())
    }
}
