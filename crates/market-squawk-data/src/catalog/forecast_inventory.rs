//! Immutable indexed forecast publications and outcomes on the existing catalog authority.

use super::CatalogAuthority;
use rusqlite::{OptionalExtension as _, params};
use sha2::{Digest as _, Sha256};
use std::sync::{Arc, Mutex};

/// Immutable prefix coordinates for both append-only populations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ForecastInventoryHead {
    /// Last committed vintage sequence.
    pub vintages: u64,
    /// Last committed outcome sequence.
    pub outcomes: u64,
}

/// Independently validated model-owner publication with searchable immutable coordinates.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ForecastInventoryVintage {
    /// Exact forecast identity.
    pub vintage_id: String,
    /// Idempotent publication request identity.
    pub request_hash: String,
    /// Product-facing opaque token.
    pub product_token: String,
    /// Controlled payload artifact identity.
    pub artifact_id: String,
    /// Exact instrument identity.
    pub instrument_id: String,
    /// Publication time.
    pub created_at: i64,
    /// Earliest availability time.
    pub available_at: i64,
    /// Exclusive expiry time.
    pub expires_at: i64,
    /// Bounded canonical domain record; large histories never accumulate in the process.
    pub record: Box<[u8]>,
}

/// One immutable source-qualified outcome of one vintage target.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ForecastInventoryOutcome {
    /// Exact outcome identity.
    pub outcome_id: String,
    /// Admitted parent vintage.
    pub vintage_id: String,
    /// Original target timestamp.
    pub target_at: i64,
    /// Canonical bounded domain record.
    pub record: Box<[u8]>,
}

/// Closed indexed lookup coordinates.
#[derive(Clone, Copy, Debug)]
pub enum ForecastInventoryLookup<'a> {
    /// Exact vintage identity.
    Vintage(&'a str),
    /// Product token.
    Token(&'a str),
    /// Original publication request.
    Request(&'a str),
    /// Exact controlled artifact.
    Artifact(&'a str),
}

/// Narrow cloneable capability over the sole catalog session.
#[derive(Clone)]
pub struct ForecastInventoryCatalogCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}
impl std::fmt::Debug for ForecastInventoryCatalogCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ForecastInventoryCatalogCapability([CATALOG])")
    }
}
impl ForecastInventoryCatalogCapability {
    pub(crate) const fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }
    /// Reads a stable append-only fence without payload allocation.
    pub fn head(&self) -> Result<ForecastInventoryHead, ForecastInventoryError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_| ForecastInventoryError::Unavailable)?;
        read_head(&authority.catalog().connection)
    }
    /// Publishes one canonical vintage, or recognizes an exact replay.
    pub fn publish_vintage(
        &self,
        value: &ForecastInventoryVintage,
    ) -> Result<bool, ForecastInventoryError> {
        value.validate()?;
        let authority = self
            .authority
            .lock()
            .map_err(|_| ForecastInventoryError::Unavailable)?;
        let tx = authority.catalog().connection.unchecked_transaction()?;
        if let Some(bytes) = lookup(
            &tx,
            ForecastInventoryLookup::Request(&value.request_hash),
            i64::MAX as u64,
        )? {
            if bytes != value.record {
                return Err(ForecastInventoryError::Conflict);
            }
            return Ok(false);
        }
        let sequence = read_head(&tx)?
            .vintages
            .checked_add(1)
            .ok_or(ForecastInventoryError::Capacity)?;
        let digest: [u8; 32] = Sha256::digest(&value.record).into();
        tx.execute("INSERT INTO forecast_inventory_vintages(sequence,vintage_id,request_hash,product_token,artifact_id,instrument_id,created_at,available_at,expires_at,record,record_sha256) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)", params![sql(sequence)?,value.vintage_id,value.request_hash,value.product_token,value.artifact_id,value.instrument_id,value.created_at,value.available_at,value.expires_at,value.record.as_ref(),digest.as_slice()])?;
        tx.commit()?;
        Ok(true)
    }
    /// Publishes one outcome with database-enforced parent and target uniqueness.
    pub fn publish_outcome(
        &self,
        value: &ForecastInventoryOutcome,
    ) -> Result<bool, ForecastInventoryError> {
        if value.outcome_id.len() != 64
            || value.vintage_id.len() != 64
            || value.record.is_empty()
            || value.record.len() > 65536
        {
            return Err(ForecastInventoryError::InvalidRecord);
        }
        let authority = self
            .authority
            .lock()
            .map_err(|_| ForecastInventoryError::Unavailable)?;
        let tx = authority.catalog().connection.unchecked_transaction()?;
        let existing: Option<Vec<u8>> = tx.query_row("SELECT record FROM forecast_inventory_outcomes WHERE outcome_id=?1 OR (vintage_id=?2 AND target_at=?3)",params![value.outcome_id,value.vintage_id,value.target_at],|row|row.get(0)).optional()?;
        if let Some(existing) = existing {
            if existing.as_slice() != value.record.as_ref() {
                return Err(ForecastInventoryError::Conflict);
            }
            return Ok(false);
        }
        let sequence = read_head(&tx)?
            .outcomes
            .checked_add(1)
            .ok_or(ForecastInventoryError::Capacity)?;
        let digest: [u8; 32] = Sha256::digest(&value.record).into();
        tx.execute("INSERT INTO forecast_inventory_outcomes(sequence,outcome_id,vintage_id,target_at,record,record_sha256) VALUES(?1,?2,?3,?4,?5,?6)",params![sql(sequence)?,value.outcome_id,value.vintage_id,value.target_at,value.record.as_ref(),digest.as_slice()])?;
        tx.commit()?;
        Ok(true)
    }
    /// Resolves one exact immutable publication within the supplied fence.
    pub fn get(
        &self,
        fence: ForecastInventoryHead,
        key: ForecastInventoryLookup<'_>,
    ) -> Result<Option<Box<[u8]>>, ForecastInventoryError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_| ForecastInventoryError::Unavailable)?;
        verify_fence(&authority.catalog().connection, fence)?;
        lookup(&authority.catalog().connection, key, fence.vintages)
    }
    /// Reads one bounded page in publication order; reverse pages use `before` as an exclusive sequence.
    pub fn vintages(
        &self,
        fence: ForecastInventoryHead,
        after: u64,
        limit: usize,
        reverse: bool,
        instrument: Option<&str>,
    ) -> Result<Vec<(u64, Box<[u8]>)>, ForecastInventoryError> {
        if limit == 0 || limit > 32 {
            return Err(ForecastInventoryError::InvalidRecord);
        }
        let authority = self
            .authority
            .lock()
            .map_err(|_| ForecastInventoryError::Unavailable)?;
        let c = &authority.catalog().connection;
        verify_fence(c, fence)?;
        let relation = if reverse { "<" } else { ">" };
        let order = if reverse { "DESC" } else { "ASC" };
        let scope = if instrument.is_some() {
            "instrument_id=?3"
        } else {
            "?3 IS NULL"
        };
        let mut statement=c.prepare(&format!("SELECT sequence,record,record_sha256 FROM forecast_inventory_vintages WHERE sequence {relation} ?1 AND sequence<=?2 AND {scope} ORDER BY sequence {order} LIMIT ?4"))?;
        let mut rows = statement.query(params![
            sql(after)?,
            sql(fence.vintages)?,
            instrument,
            limit as i64
        ])?;
        let mut out = Vec::with_capacity(limit);
        while let Some(row) = rows.next()? {
            out.push((unsigned(row.get(0)?)?, read_payload(row, 1, 2, 4194304)?));
        }
        Ok(out)
    }
    /// Reads one bounded outcome page, optionally restricted to one exact parent vintage.
    pub fn outcomes(
        &self,
        fence: ForecastInventoryHead,
        after: u64,
        limit: usize,
        vintage: Option<&str>,
    ) -> Result<Vec<(u64, Box<[u8]>)>, ForecastInventoryError> {
        if limit == 0 || limit > 32 {
            return Err(ForecastInventoryError::InvalidRecord);
        }
        let authority = self
            .authority
            .lock()
            .map_err(|_| ForecastInventoryError::Unavailable)?;
        let c = &authority.catalog().connection;
        verify_fence(c, fence)?;
        let scope = if vintage.is_some() {
            "vintage_id=?3"
        } else {
            "?3 IS NULL"
        };
        let mut statement=c.prepare(&format!("SELECT sequence,record,record_sha256 FROM forecast_inventory_outcomes WHERE sequence>?1 AND sequence<=?2 AND {scope} ORDER BY sequence LIMIT ?4"))?;
        let mut rows = statement.query(params![
            sql(after)?,
            sql(fence.outcomes)?,
            vintage,
            limit as i64
        ])?;
        let mut out = Vec::with_capacity(limit);
        while let Some(row) = rows.next()? {
            out.push((unsigned(row.get(0)?)?, read_payload(row, 1, 2, 65536)?));
        }
        Ok(out)
    }
    /// Counts a vintage's outcome rows without reading evidence blobs.
    pub fn outcome_count(
        &self,
        fence: ForecastInventoryHead,
        vintage: &str,
    ) -> Result<u64, ForecastInventoryError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_| ForecastInventoryError::Unavailable)?;
        let c = &authority.catalog().connection;
        verify_fence(c, fence)?;
        unsigned(c.query_row(
            "SELECT count(*) FROM forecast_inventory_outcomes WHERE vintage_id=?1 AND sequence<=?2",
            params![vintage, sql(fence.outcomes)?],
            |row| row.get(0),
        )?)
    }
}
impl ForecastInventoryVintage {
    fn validate(&self) -> Result<(), ForecastInventoryError> {
        if self.vintage_id.len() != 64
            || self.request_hash.len() != 64
            || self.product_token.len() != 36
            || self.instrument_id.len() != 36
            || self.artifact_id.is_empty()
            || self.artifact_id.len() > 256
            || self.record.is_empty()
            || self.record.len() > 4194304
            || self.expires_at <= self.created_at
        {
            return Err(ForecastInventoryError::InvalidRecord);
        }
        Ok(())
    }
}
fn lookup(
    c: &rusqlite::Connection,
    key: ForecastInventoryLookup<'_>,
    fence: u64,
) -> Result<Option<Box<[u8]>>, ForecastInventoryError> {
    let (column, value) = match key {
        ForecastInventoryLookup::Vintage(v) => ("vintage_id", v),
        ForecastInventoryLookup::Token(v) => ("product_token", v),
        ForecastInventoryLookup::Request(v) => ("request_hash", v),
        ForecastInventoryLookup::Artifact(v) => ("artifact_id", v),
    };
    let mut statement=c.prepare(&format!("SELECT record,record_sha256 FROM forecast_inventory_vintages WHERE {column}=?1 AND sequence<=?2"))?;
    let mut rows = statement.query(params![value, sql(fence)?])?;
    rows.next()?
        .map(|row| read_payload(row, 0, 1, 4194304))
        .transpose()
}
fn read_payload(
    row: &rusqlite::Row<'_>,
    column: usize,
    digest: usize,
    maximum: usize,
) -> Result<Box<[u8]>, ForecastInventoryError> {
    let bytes = row
        .get_ref(column)?
        .as_blob()
        .map_err(|_| ForecastInventoryError::Corrupt)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(ForecastInventoryError::Corrupt);
    }
    let hash = row
        .get_ref(digest)?
        .as_blob()
        .map_err(|_| ForecastInventoryError::Corrupt)?;
    if hash != Sha256::digest(bytes).as_slice() {
        return Err(ForecastInventoryError::Corrupt);
    }
    Ok(bytes.into())
}
fn read_head(c: &rusqlite::Connection) -> Result<ForecastInventoryHead, ForecastInventoryError> {
    Ok(ForecastInventoryHead {
        vintages: unsigned(c.query_row(
            "SELECT coalesce(max(sequence),0) FROM forecast_inventory_vintages",
            [],
            |r| r.get(0),
        )?)?,
        outcomes: unsigned(c.query_row(
            "SELECT coalesce(max(sequence),0) FROM forecast_inventory_outcomes",
            [],
            |r| r.get(0),
        )?)?,
    })
}
fn verify_fence(
    c: &rusqlite::Connection,
    fence: ForecastInventoryHead,
) -> Result<(), ForecastInventoryError> {
    let current = read_head(c)?;
    if fence.vintages > current.vintages || fence.outcomes > current.outcomes {
        return Err(ForecastInventoryError::Corrupt);
    }
    Ok(())
}
fn sql(value: u64) -> Result<i64, ForecastInventoryError> {
    i64::try_from(value).map_err(|_| ForecastInventoryError::Capacity)
}
fn unsigned(value: i64) -> Result<u64, ForecastInventoryError> {
    u64::try_from(value).map_err(|_| ForecastInventoryError::Corrupt)
}

/// Publication, immutable identity, or catalog integrity failure.
#[derive(Debug, thiserror::Error)]
pub enum ForecastInventoryError {
    /// Invalid per-record bound or coordinate.
    #[error("forecast inventory record is invalid")]
    InvalidRecord,
    /// Immutable identity conflict.
    #[error("forecast inventory conflicts")]
    Conflict,
    /// Persisted data failed its bound or digest.
    #[error("forecast inventory is corrupt")]
    Corrupt,
    /// Sequence representation exhausted.
    #[error("forecast inventory capacity exceeded")]
    Capacity,
    /// Sole catalog session unavailable.
    #[error("forecast inventory unavailable")]
    Unavailable,
    /// SQLite failure.
    #[error(transparent)]
    Storage(#[from] rusqlite::Error),
}
