//! Immutable financial display projections in the existing catalog authority.
//!
//! The financial owner supplies original values and evidence. This module only commits them
//! atomically and performs indexed, ordered reads; it never recalculates financial values.

use super::CatalogAuthority;
use rusqlite::{OptionalExtension as _, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// Exact source-bound identity, retained inside the original saved decision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChartProjectionReference {
    /// Commitment to original financial authority coordinates.
    pub source_sha256: [u8; 32],
    /// Complete ordered projection and metadata commitment.
    pub projection_sha256: [u8; 32],
    /// Commitment to compact original metadata independent of the requested row range.
    pub metadata_sha256: [u8; 32],
    /// Complete retained original observation count.
    pub row_count: u64,
    /// Number of parallel financial series.
    pub series_count: usize,
    /// Original first observation, absent only for an unavailable projection.
    pub first_time: Option<i64>,
    /// Original last observation.
    pub last_time: Option<i64>,
}

/// Exact display ordering over the complete forecast i128 domain and monetary decimal scales.
/// Arrow's maintained 256-bit integer avoids overflow and floating-point loss during comparison.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(try_from = "ChartProjectionValueWire")]
pub struct ChartProjectionValue {
    mantissa: i128,
    scale: u8,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChartProjectionValueWire {
    mantissa: i128,
    scale: u8,
}
impl TryFrom<ChartProjectionValueWire> for ChartProjectionValue {
    type Error = ChartProjectionError;
    fn try_from(value: ChartProjectionValueWire) -> Result<Self, Self::Error> {
        Self::try_new(value.mantissa, value.scale)
    }
}
impl ChartProjectionValue {
    /// Keeps the exact signed coefficient; no money, probability or return interpretation occurs.
    pub fn try_new(mantissa: i128, scale: u8) -> Result<Self, ChartProjectionError> {
        if scale > 28 {
            return Err(ChartProjectionError::Invalid);
        };
        Ok(Self { mantissa, scale })
    }
    fn comparable(self) -> arrow::datatypes::i256 {
        arrow::datatypes::i256::from_i128(self.mantissa).wrapping_mul(
            arrow::datatypes::i256::from_i128(10_i128.pow(u32::from(28 - self.scale))),
        )
    }
}
impl From<rust_decimal::Decimal> for ChartProjectionValue {
    fn from(value: rust_decimal::Decimal) -> Self {
        Self {
            mantissa: value.mantissa(),
            scale: value.scale() as u8,
        }
    }
}
impl PartialEq for ChartProjectionValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for ChartProjectionValue {}
impl PartialOrd for ChartProjectionValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ChartProjectionValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.comparable().cmp(&other.comparable())
    }
}

/// One exact original financial observation. Null values represent genuine gaps.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChartProjectionRow {
    /// Source-owned ordering coordinate: nanoseconds for timestamped series, YYYYMMDD for
    /// native-date series. The owner metadata fixes precision; dates never become instants.
    pub time_nanos: i64,
    /// Exact decimals used solely to select displayed extrema.
    pub values: Vec<Option<ChartProjectionValue>>,
    /// Canonical presentation point including original evidence coordinates.
    pub point: serde_json::Value,
}

/// The existing sole catalog session, with no separate database or writer.
#[derive(Clone)]
pub struct ChartProjectionCatalogCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}
impl std::fmt::Debug for ChartProjectionCatalogCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ChartProjectionCatalogCapability([CATALOG AUTHORITY])")
    }
}

impl ChartProjectionCatalogCapability {
    pub(crate) const fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }

    /// Consumes one row at a time; failure/cancellation rolls back the complete projection.
    pub fn publish(
        &self,
        source: [u8; 32],
        metadata: &[u8],
        series_count: usize,
        rows: impl IntoIterator<Item = Result<ChartProjectionRow, ChartProjectionError>>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ChartProjectionReference, ChartProjectionError> {
        check(deadline, cancellation)?;
        if source == [0; 32]
            || metadata.is_empty()
            || metadata.len() > 64 * 1024
            || !(1..=3).contains(&series_count)
        {
            return Err(ChartProjectionError::Invalid);
        }
        let authority = self
            .authority
            .lock()
            .map_err(|_| ChartProjectionError::Unavailable)?;
        check(deadline, cancellation)?;
        let tx = authority.catalog().connection.unchecked_transaction()?;
        let prior = header(&tx, &source)?;
        let mut count = 0_u64;
        let mut first = None;
        let mut last = None;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/chart-projection/v1\0");
        hash.update(source);
        hash.update((series_count as u64).to_be_bytes());
        hash.update((metadata.len() as u64).to_be_bytes());
        hash.update(metadata);
        {
            let mut insert = tx.prepare("INSERT INTO chart_projection_rows(source_sha256,ordinal,time_nanos,payload,payload_sha256) VALUES(?1,?2,?3,?4,?5)")?;
            for row in rows {
                check(deadline, cancellation)?;
                let row = row?;
                if row.values.len() != series_count
                    || last.is_some_and(|last| last >= row.time_nanos)
                {
                    return Err(ChartProjectionError::Invalid);
                }
                let bytes = serde_json::to_vec(&row).map_err(|_| ChartProjectionError::Invalid)?;
                if bytes.len() > 16 * 1024 {
                    return Err(ChartProjectionError::Invalid);
                }
                let digest: [u8; 32] = Sha256::digest(&bytes).into();
                hash.update(count.to_be_bytes());
                hash.update((bytes.len() as u64).to_be_bytes());
                hash.update(&bytes);
                if prior.is_none() {
                    insert.execute(params![
                        source.as_slice(),
                        i64::try_from(count).map_err(|_| ChartProjectionError::Invalid)?,
                        row.time_nanos,
                        bytes,
                        digest.as_slice()
                    ])?;
                }
                first.get_or_insert(row.time_nanos);
                last = Some(row.time_nanos);
                count = count.checked_add(1).ok_or(ChartProjectionError::Invalid)?;
            }
        }
        hash.update(count.to_be_bytes());
        let reference = ChartProjectionReference {
            source_sha256: source,
            projection_sha256: hash.finalize().into(),
            metadata_sha256: Sha256::digest(metadata).into(),
            row_count: count,
            series_count,
            first_time: first,
            last_time: last,
        };
        if let Some((saved, saved_metadata)) = prior {
            if saved != reference || saved_metadata != metadata {
                return Err(ChartProjectionError::Invalid);
            }
        } else {
            tx.execute("INSERT INTO chart_projection_headers(source_sha256,projection_sha256,row_count,series_count,first_time,last_time,metadata) VALUES(?1,?2,?3,?4,?5,?6,?7)", params![source.as_slice(), reference.projection_sha256.as_slice(), i64::try_from(count).map_err(|_| ChartProjectionError::Invalid)?, series_count as i64, first, last, metadata])?;
        }
        check(deadline, cancellation)?;
        tx.commit()?;
        Ok(reference)
    }

    /// Resolves one immutable source identity without selecting a newer projection.
    pub fn reference(
        &self,
        source: [u8; 32],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ChartProjectionReference>, ChartProjectionError> {
        check(deadline, cancellation)?;
        let authority = self
            .authority
            .lock()
            .map_err(|_| ChartProjectionError::Unavailable)?;
        let result =
            header(&authority.catalog().connection, &source)?.map(|(reference, _)| reference);
        check(deadline, cancellation)?;
        Ok(result)
    }

    /// Reads only compact projection metadata and checks the decision-bound complete identity.
    pub fn metadata(
        &self,
        reference: &ChartProjectionReference,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, ChartProjectionError> {
        check(deadline, cancellation)?;
        let authority = self
            .authority
            .lock()
            .map_err(|_| ChartProjectionError::Unavailable)?;
        let (actual, metadata) = header(&authority.catalog().connection, &reference.source_sha256)?
            .ok_or(ChartProjectionError::NotFound)?;
        if &actual != reference {
            return Err(ChartProjectionError::Invalid);
        }
        check(deadline, cancellation)?;
        Ok(metadata)
    }

    /// Verifies the complete retained projection with one decoded row in memory.
    pub fn verify(
        &self,
        reference: &ChartProjectionReference,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ChartProjectionError> {
        let metadata = self.metadata(reference, deadline, cancellation)?;
        if reference.source_sha256 == [0; 32]
            || metadata.is_empty()
            || metadata.len() > 64 * 1024
            || !(1..=3).contains(&reference.series_count)
        {
            return Err(ChartProjectionError::Invalid);
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/chart-projection/v1\0");
        hash.update(reference.source_sha256);
        hash.update((reference.series_count as u64).to_be_bytes());
        hash.update((metadata.len() as u64).to_be_bytes());
        hash.update(&metadata);
        let mut count = 0_u64;
        let mut first = None;
        let mut last = None;
        self.scan_rows(
            reference,
            i64::MIN,
            i64::MAX,
            deadline,
            cancellation,
            |ordinal, row, bytes| {
                if ordinal != count {
                    return Err(ChartProjectionError::Invalid);
                }
                hash.update(ordinal.to_be_bytes());
                hash.update((bytes.len() as u64).to_be_bytes());
                hash.update(bytes);
                first.get_or_insert(row.time_nanos);
                last = Some(row.time_nanos);
                count = count.checked_add(1).ok_or(ChartProjectionError::Invalid)?;
                Ok(())
            },
        )?;
        hash.update(count.to_be_bytes());
        let digest: [u8; 32] = hash.finalize().into();
        if count != reference.row_count
            || first != reference.first_time
            || last != reference.last_time
            || digest != reference.projection_sha256
        {
            return Err(ChartProjectionError::Invalid);
        }
        check(deadline, cancellation)
    }

    /// Visits an indexed inclusive range in original order, holding only one decoded row.
    /// The callback must not call another operation on this catalog session.
    pub fn scan(
        &self,
        reference: &ChartProjectionReference,
        start: i64,
        end: i64,
        deadline: Instant,
        cancellation: &CancellationToken,
        mut visit: impl FnMut(u64, ChartProjectionRow) -> Result<(), ChartProjectionError>,
    ) -> Result<u64, ChartProjectionError> {
        self.scan_rows(
            reference,
            start,
            end,
            deadline,
            cancellation,
            |ordinal, row, _| visit(ordinal, row),
        )
    }

    fn scan_rows(
        &self,
        reference: &ChartProjectionReference,
        start: i64,
        end: i64,
        deadline: Instant,
        cancellation: &CancellationToken,
        mut visit: impl FnMut(u64, ChartProjectionRow, &[u8]) -> Result<(), ChartProjectionError>,
    ) -> Result<u64, ChartProjectionError> {
        check(deadline, cancellation)?;
        if start > end {
            return Err(ChartProjectionError::Invalid);
        }
        let authority = self
            .authority
            .lock()
            .map_err(|_| ChartProjectionError::Unavailable)?;
        let connection = &authority.catalog().connection;
        let (actual, _) =
            header(connection, &reference.source_sha256)?.ok_or(ChartProjectionError::NotFound)?;
        if &actual != reference {
            return Err(ChartProjectionError::Invalid);
        }
        let mut statement = connection.prepare("SELECT ordinal,time_nanos,payload,payload_sha256 FROM chart_projection_rows WHERE source_sha256=?1 AND time_nanos>=?2 AND time_nanos<=?3 ORDER BY time_nanos,ordinal")?;
        let mut rows = statement.query(params![reference.source_sha256.as_slice(), start, end])?;
        let mut previous = None;
        let mut count = 0_u64;
        while let Some(row) = rows.next()? {
            check(deadline, cancellation)?;
            let ordinal =
                u64::try_from(row.get::<_, i64>(0)?).map_err(|_| ChartProjectionError::Invalid)?;
            let at: i64 = row.get(1)?;
            let bytes: Vec<u8> = row.get(2)?;
            let digest: Vec<u8> = row.get(3)?;
            if bytes.len() > 16 * 1024
                || Sha256::digest(&bytes).as_slice() != digest
                || ordinal >= reference.row_count
                || previous.is_some_and(|(prior_ordinal, prior_at)| {
                    prior_ordinal + 1 != ordinal || prior_at >= at
                })
            {
                return Err(ChartProjectionError::Invalid);
            }
            let point: ChartProjectionRow =
                serde_json::from_slice(&bytes).map_err(|_| ChartProjectionError::Invalid)?;
            if point.time_nanos != at || point.values.len() != reference.series_count {
                return Err(ChartProjectionError::Invalid);
            }
            visit(ordinal, point, &bytes)?;
            previous = Some((ordinal, at));
            count += 1;
        }
        check(deadline, cancellation)?;
        Ok(count)
    }
}

fn header(
    connection: &rusqlite::Connection,
    source: &[u8; 32],
) -> Result<Option<(ChartProjectionReference, Vec<u8>)>, ChartProjectionError> {
    Ok(connection.query_row("SELECT projection_sha256,row_count,series_count,first_time,last_time,metadata FROM chart_projection_headers WHERE source_sha256=?1", [source.as_slice()], |row| {
        let digest: [u8;32] = row.get(0)?;
        let metadata: Vec<u8> = row.get(5)?;
        Ok((ChartProjectionReference { source_sha256:*source, projection_sha256:digest, metadata_sha256:Sha256::digest(&metadata).into(), row_count:unsigned_column(row,1)?, series_count:usize::try_from(unsigned_column(row,2)?).map_err(|_|rusqlite::Error::IntegralValueOutOfRange(2,i64::MAX))?, first_time:row.get(3)?, last_time:row.get(4)? }, metadata))
    }).optional()?)
}
fn unsigned_column(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))
}
fn check(deadline: Instant, cancellation: &CancellationToken) -> Result<(), ChartProjectionError> {
    if cancellation.is_cancelled() {
        Err(ChartProjectionError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ChartProjectionError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
/// Storage and control failures never expose a partial projection.
#[derive(Debug, thiserror::Error)]
pub enum ChartProjectionError {
    /// Invalid or changed original projection.
    #[error("invalid chart projection")]
    Invalid,
    /// Original projection is absent.
    #[error("chart projection unavailable")]
    NotFound,
    /// Catalog session unavailable.
    #[error("chart catalog unavailable")]
    Unavailable,
    /// Read/publication was cancelled.
    #[error("chart operation cancelled")]
    Cancelled,
    /// Operation deadline elapsed.
    #[error("chart operation deadline exceeded")]
    DeadlineExceeded,
    /// Existing SQLite authority failed.
    #[error("chart storage failure")]
    Storage(#[from] rusqlite::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CatalogConfig, CatalogLimit, CatalogResultLimits};
    use market_squawk_platform::LocalPaths;
    use std::time::Duration;

    #[test]
    fn extrema_comparison_preserves_full_signed_forecast_precision()
    -> Result<(), ChartProjectionError> {
        let high = ChartProjectionValue::try_new(i128::MAX, 12)?;
        let adjacent = ChartProjectionValue::try_new(i128::MAX - 1, 12)?;
        assert!(high > adjacent);
        assert!(
            ChartProjectionValue::try_new(i128::MIN, 0)? < ChartProjectionValue::try_new(-1, 28)?
        );
        assert_eq!(
            ChartProjectionValue::try_new(100, 2)?,
            ChartProjectionValue::try_new(1, 0)?
        );
        assert!(ChartProjectionValue::try_new(1, 29).is_err());
        Ok(())
    }

    #[test]
    fn chart_publication_is_atomic_and_ranges_reopen_original_ordinals()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("chart"))?;
        let config = || -> Result<CatalogConfig, Box<dyn std::error::Error>> {
            Ok(CatalogConfig::try_new(
                paths.catalog()?.clone(),
                Duration::from_millis(750),
                CatalogLimit::new(32)?,
                CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
            )?)
        };
        let capability = ChartProjectionCatalogCapability::new(Arc::new(Mutex::new(
            CatalogAuthority::open(config()?)?,
        )));
        let deadline = Instant::now() + Duration::from_secs(10);
        let cancellation = CancellationToken::new();
        let rows = || {
            (0..12).map(|i| {
                Ok(ChartProjectionRow {
                    time_nanos: 100 + i,
                    values: vec![if i == 5 {
                        None
                    } else {
                        Some(rust_decimal::Decimal::from(i).into())
                    }],
                    point: serde_json::json!({"value":i.to_string()}),
                })
            })
        };
        let reference = capability.publish(
            [1; 32],
            b"{\"baseline\":100}",
            1,
            rows(),
            deadline,
            &cancellation,
        )?;
        assert_eq!(reference.row_count, 12);
        assert_eq!(
            capability.publish(
                [1; 32],
                b"{\"baseline\":100}",
                1,
                rows(),
                deadline,
                &cancellation
            )?,
            reference
        );
        let failed = capability.publish(
            [2; 32],
            b"{}",
            1,
            rows()
                .take(4)
                .chain(std::iter::once(Err(ChartProjectionError::Cancelled))),
            deadline,
            &cancellation,
        );
        assert!(matches!(failed, Err(ChartProjectionError::Cancelled)));
        drop(capability);
        let capability = ChartProjectionCatalogCapability::new(Arc::new(Mutex::new(
            CatalogAuthority::open(config()?)?,
        )));
        let mut selected = Vec::new();
        assert_eq!(
            capability.scan(
                &reference,
                104,
                106,
                deadline,
                &cancellation,
                |ordinal, row| {
                    selected.push((ordinal, row.time_nanos, row.values[0]));
                    Ok(())
                }
            )?,
            3
        );
        assert_eq!(
            selected
                .iter()
                .map(|(ordinal, _, _)| *ordinal)
                .collect::<Vec<_>>(),
            vec![4, 5, 6]
        );
        assert!(selected[1].2.is_none());
        capability.verify(&reference, deadline, &cancellation)?;
        let last_reference =
            capability.publish([3; 32], b"{}", 1, rows(), deadline, &cancellation)?;
        capability.verify(&last_reference, deadline, &cancellation)?;
        let authority = capability.authority.lock().map_err(|_| "catalog lock")?;
        let remaining: i64 = authority.catalog().connection.query_row(
            "SELECT count(*) FROM chart_projection_rows WHERE source_sha256=?1",
            [[2_u8; 32].as_slice()],
            |row| row.get(0),
        )?;
        assert_eq!(remaining, 0);
        // Model damaged persisted custody while preserving the original decision-bound headers.
        authority
            .catalog()
            .connection
            .execute_batch("DROP TRIGGER chart_projection_rows_immutable_delete")?;
        assert_eq!(
            authority.catalog().connection.execute(
                "DELETE FROM chart_projection_rows WHERE source_sha256=?1 AND ordinal=?2",
                params![reference.source_sha256.as_slice(), 0_i64]
            )?,
            1
        );
        assert_eq!(
            authority.catalog().connection.execute(
                "DELETE FROM chart_projection_rows WHERE source_sha256=?1 AND ordinal=?2",
                params![last_reference.source_sha256.as_slice(), 11_i64]
            )?,
            1
        );
        drop(authority);
        assert!(matches!(
            capability.verify(&reference, deadline, &cancellation),
            Err(ChartProjectionError::Invalid)
        ));
        assert!(matches!(
            capability.verify(&last_reference, deadline, &cancellation),
            Err(ChartProjectionError::Invalid)
        ));
        Ok(())
    }
}
