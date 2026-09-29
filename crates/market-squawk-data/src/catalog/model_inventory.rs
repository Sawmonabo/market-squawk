//! Immutable model admission inventory, independent of active inference capacity.

use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use market_squawk_domain::ModelId;
use rusqlite::{Connection, OptionalExtension as _, Row, params};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::{CatalogAuthority, CatalogError};

const MAX_RECORD_BYTES: usize = 1024 * 1024;
const PAGE_ROWS: usize = 32;
const COLUMNS: &str = "sequence, model_id, bundle_id, bundle_version, candidate_directory, record, record_sha256, chain_sha256, product_token";

/// Stable append-only inventory fence. Appending never changes records below this fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ModelInventoryHead {
    /// Number and sequence of committed admissions.
    pub sequence: u64,
    /// Chained identity of all admissions in publication order.
    pub sha256: [u8; 32],
}

impl ModelInventoryHead {
    /// Identity of an empty inventory.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            sequence: 0,
            sha256: Sha256::digest(b"market-squawk/model-inventory/v1").into(),
        }
    }
}

/// One bounded model-owner record; large artifacts remain in immutable artifact storage.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ModelInventoryRecord {
    /// Stable model series identity.
    pub model_id: ModelId,
    /// Stable opaque product token for exact demand-loaded details.
    pub model_token: uuid::Uuid,
    /// Stable bundle series identity.
    pub bundle_id: String,
    /// Exact immutable generation.
    pub bundle_version: NonZeroU64,
    /// Capability-relative candidate directory, unique across admissions.
    pub candidate_directory: String,
    /// Canonical model-owner admission bytes, revalidated by the model owner on reads.
    pub record: Box<[u8]>,
}

impl ModelInventoryRecord {
    fn validate(&self) -> Result<(), ModelInventoryError> {
        if self.bundle_id.is_empty()
            || self.bundle_id.len() > 128
            || self.candidate_directory.is_empty()
            || self.candidate_directory.len() > 512
            || self.record.is_empty()
            || self.record.len() > MAX_RECORD_BYTES
        {
            return Err(ModelInventoryError::InvalidRecord);
        }
        Ok(())
    }

    fn digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/model-inventory-record/v1");
        hash.update(self.model_id.as_uuid().as_bytes());
        hash.update(self.model_token.as_bytes());
        for value in [
            self.bundle_id.as_bytes(),
            self.candidate_directory.as_bytes(),
            &self.record,
        ] {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value);
        }
        hash.update(self.bundle_version.get().to_be_bytes());
        hash.finalize().into()
    }
}

/// One row bound to the exact inventory prefix ending with that admission.
#[derive(Debug)]
pub struct ModelInventoryEntry {
    /// Immutable admission.
    pub admission: ModelInventoryRecord,
    /// Inventory prefix identity through this row.
    pub head: ModelInventoryHead,
}

/// Cloneable access to the existing sole catalog authority; never opens another writer.
#[derive(Clone)]
pub struct ModelInventoryCatalogCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl std::fmt::Debug for ModelInventoryCatalogCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ModelInventoryCatalogCapability([CATALOG AUTHORITY])")
    }
}

impl ModelInventoryCatalogCapability {
    /// Binds the composition-owned catalog session.
    #[must_use]
    pub(crate) const fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }

    /// Reads the committed inventory fence without loading historical model artifacts.
    pub fn head(&self) -> Result<ModelInventoryHead, ModelInventoryError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_| ModelInventoryError::Unavailable)?;
        head(&authority.catalog().connection)
    }

    /// Publishes one independently admitted record, preserving exact retry semantics.
    pub fn publish(
        &self,
        record: &ModelInventoryRecord,
    ) -> Result<(ModelInventoryHead, bool), ModelInventoryError> {
        record.validate()?;
        let authority = self
            .authority
            .lock()
            .map_err(|_| ModelInventoryError::Unavailable)?;
        let transaction = authority.catalog().connection.unchecked_transaction()?;
        let existing = exact(
            &transaction,
            record.bundle_id.as_str(),
            record.bundle_version,
            u64::MAX,
        )?;
        if let Some(existing) = existing {
            if existing.admission != *record {
                return Err(ModelInventoryError::Conflict);
            }
            return Ok((head(&transaction)?, false));
        }
        let conflicting: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM model_inventory_series WHERE (model_id=?1 AND bundle_id<>?2) OR (bundle_id=?2 AND model_id<>?1)) OR EXISTS(SELECT 1 FROM model_inventory_records WHERE candidate_directory=?3)",
            params![record.model_id.to_string(), record.bundle_id, record.candidate_directory], |row| row.get(0))?;
        if conflicting {
            return Err(ModelInventoryError::Conflict);
        }
        let previous = head(&transaction)?;
        let sequence = previous
            .sequence
            .checked_add(1)
            .filter(|value| *value <= i64::MAX as u64)
            .ok_or(ModelInventoryError::Capacity)?;
        let digest = record.digest();
        let next = ModelInventoryHead {
            sequence,
            sha256: chain(previous, sequence, digest),
        };
        transaction.execute(
            "INSERT OR IGNORE INTO model_inventory_series(model_id,bundle_id) VALUES(?1,?2)",
            params![record.model_id.to_string(), record.bundle_id],
        )?;
        transaction.execute("INSERT INTO model_inventory_records(sequence,model_id,bundle_id,bundle_version,candidate_directory,record,record_sha256,chain_sha256,product_token) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![sequence as i64, record.model_id.to_string(), record.bundle_id, record.bundle_version.get().to_be_bytes().as_slice(), record.candidate_directory, record.record.as_ref(), digest.as_slice(), next.sha256.as_slice(), record.model_token.to_string()])?;
        transaction.commit()?;
        Ok((next, true))
    }

    /// Reads at most one fixed working page from an immutable prefix.
    pub fn page(
        &self,
        fence: ModelInventoryHead,
        after: u64,
    ) -> Result<Vec<ModelInventoryEntry>, ModelInventoryError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_| ModelInventoryError::Unavailable)?;
        let connection = &authority.catalog().connection;
        verify_fence(connection, fence)?;
        let mut statement = connection.prepare(&format!("SELECT {COLUMNS} FROM model_inventory_records WHERE sequence>?1 AND sequence<=?2 ORDER BY sequence LIMIT ?3"))?;
        let mut rows = statement.query(params![
            to_sql(after)?,
            to_sql(fence.sequence)?,
            PAGE_ROWS as i64
        ])?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next()? {
            entries.push(decode(row)?);
        }
        Ok(entries)
    }

    /// Reads an exact generation without selecting a replacement.
    pub fn get(
        &self,
        fence: ModelInventoryHead,
        bundle_id: &str,
        version: NonZeroU64,
    ) -> Result<Option<ModelInventoryEntry>, ModelInventoryError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_| ModelInventoryError::Unavailable)?;
        verify_fence(&authority.catalog().connection, fence)?;
        exact(
            &authority.catalog().connection,
            bundle_id,
            version,
            fence.sequence,
        )
    }

    /// Reads the newest generation of one model within the retained fence.
    pub fn latest(
        &self,
        fence: ModelInventoryHead,
        model_id: ModelId,
    ) -> Result<Option<ModelInventoryEntry>, ModelInventoryError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_| ModelInventoryError::Unavailable)?;
        let connection = &authority.catalog().connection;
        verify_fence(connection, fence)?;
        let mut statement = connection.prepare(&format!("SELECT {COLUMNS} FROM model_inventory_records WHERE model_id=?1 AND sequence<=?2 ORDER BY bundle_version DESC LIMIT 1"))?;
        let mut rows = statement.query(params![model_id.to_string(), to_sql(fence.sequence)?])?;
        rows.next()?.map(decode).transpose()
    }

    /// Resolves an opaque product token within a retained inventory prefix.
    pub fn by_token(
        &self,
        fence: ModelInventoryHead,
        token: uuid::Uuid,
    ) -> Result<Option<ModelInventoryEntry>, ModelInventoryError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_| ModelInventoryError::Unavailable)?;
        let connection = &authority.catalog().connection;
        verify_fence(connection, fence)?;
        let mut statement = connection.prepare(&format!(
            "SELECT {COLUMNS} FROM model_inventory_records WHERE product_token=?1 AND sequence<=?2"
        ))?;
        let mut rows = statement.query(params![token.to_string(), to_sql(fence.sequence)?])?;
        rows.next()?.map(decode).transpose()
    }

    /// Checks the complete admission chain with bounded pages; no model is compiled or retained.
    pub fn verify(&self, fence: ModelInventoryHead) -> Result<(), ModelInventoryError> {
        let mut previous = ModelInventoryHead::empty();
        loop {
            let page = self.page(fence, previous.sequence)?;
            if page.is_empty() {
                break;
            }
            for entry in page {
                if entry.head.sequence != previous.sequence + 1
                    || entry.head.sha256
                        != chain(previous, entry.head.sequence, entry.admission.digest())
                {
                    return Err(ModelInventoryError::Corrupt);
                }
                previous = entry.head;
            }
        }
        if previous != fence {
            return Err(ModelInventoryError::Corrupt);
        }
        Ok(())
    }
}

fn chain(previous: ModelInventoryHead, sequence: u64, record: [u8; 32]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(previous.sha256);
    hash.update(sequence.to_be_bytes());
    hash.update(record);
    hash.finalize().into()
}

fn head(connection: &Connection) -> Result<ModelInventoryHead, ModelInventoryError> {
    let value: Option<(i64, Vec<u8>)> = connection.query_row("SELECT sequence,chain_sha256 FROM model_inventory_records ORDER BY sequence DESC LIMIT 1", [], |row| Ok((row.get(0)?, row.get(1)?))).optional()?;
    value.map_or_else(
        || Ok(ModelInventoryHead::empty()),
        |(sequence, digest)| {
            Ok(ModelInventoryHead {
                sequence: u64::try_from(sequence).map_err(|_| ModelInventoryError::Corrupt)?,
                sha256: digest
                    .try_into()
                    .map_err(|_| ModelInventoryError::Corrupt)?,
            })
        },
    )
}

fn verify_fence(
    connection: &Connection,
    fence: ModelInventoryHead,
) -> Result<(), ModelInventoryError> {
    if fence.sequence == 0 {
        return if fence == ModelInventoryHead::empty() {
            Ok(())
        } else {
            Err(ModelInventoryError::Corrupt)
        };
    }
    let digest: Option<Vec<u8>> = connection
        .query_row(
            "SELECT chain_sha256 FROM model_inventory_records WHERE sequence=?1",
            [to_sql(fence.sequence)?],
            |row| row.get(0),
        )
        .optional()?;
    if digest.as_deref() != Some(fence.sha256.as_slice()) {
        return Err(ModelInventoryError::Corrupt);
    }
    Ok(())
}

fn exact(
    connection: &Connection,
    bundle: &str,
    version: NonZeroU64,
    fence: u64,
) -> Result<Option<ModelInventoryEntry>, ModelInventoryError> {
    let mut statement = connection.prepare(&format!("SELECT {COLUMNS} FROM model_inventory_records WHERE bundle_id=?1 AND bundle_version=?2 AND sequence<=?3"))?;
    let mut rows = statement.query(params![
        bundle,
        version.get().to_be_bytes().as_slice(),
        fence.min(i64::MAX as u64) as i64
    ])?;
    rows.next()?.map(decode).transpose()
}

fn decode(row: &Row<'_>) -> Result<ModelInventoryEntry, ModelInventoryError> {
    for (column, maximum) in [
        (1, 36),
        (2, 128),
        (3, 8),
        (4, 512),
        (5, MAX_RECORD_BYTES),
        (6, 32),
        (7, 32),
        (8, 36),
    ] {
        let bytes = match row.get_ref(column)? {
            rusqlite::types::ValueRef::Text(bytes) | rusqlite::types::ValueRef::Blob(bytes) => {
                bytes
            }
            _ => return Err(ModelInventoryError::Corrupt),
        };
        if bytes.is_empty() || bytes.len() > maximum {
            return Err(ModelInventoryError::Corrupt);
        }
    }
    let version: Vec<u8> = row.get(3)?;
    let admission = ModelInventoryRecord {
        model_id: row
            .get::<_, String>(1)?
            .parse()
            .map_err(|_| ModelInventoryError::Corrupt)?,
        model_token: row
            .get::<_, String>(8)?
            .parse()
            .map_err(|_| ModelInventoryError::Corrupt)?,
        bundle_id: row.get(2)?,
        bundle_version: NonZeroU64::new(u64::from_be_bytes(
            version
                .try_into()
                .map_err(|_| ModelInventoryError::Corrupt)?,
        ))
        .ok_or(ModelInventoryError::Corrupt)?,
        candidate_directory: row.get(4)?,
        record: row.get::<_, Vec<u8>>(5)?.into_boxed_slice(),
    };
    admission.validate()?;
    let digest: Vec<u8> = row.get(6)?;
    if digest.as_slice() != admission.digest() {
        return Err(ModelInventoryError::Corrupt);
    }
    Ok(ModelInventoryEntry {
        admission,
        head: ModelInventoryHead {
            sequence: u64::try_from(row.get::<_, i64>(0)?)
                .map_err(|_| ModelInventoryError::Corrupt)?,
            sha256: row
                .get::<_, Vec<u8>>(7)?
                .try_into()
                .map_err(|_| ModelInventoryError::Corrupt)?,
        },
    })
}

fn to_sql(value: u64) -> Result<i64, ModelInventoryError> {
    i64::try_from(value).map_err(|_| ModelInventoryError::Capacity)
}

/// Inventory publication or integrity failure. Capacity bounds apply to a row/page, not history.
#[derive(Debug, Error)]
pub enum ModelInventoryError {
    /// One admission record is invalid or exceeds the per-record bound.
    #[error("model inventory record is invalid")]
    InvalidRecord,
    /// An immutable identity was reused.
    #[error("model inventory admission conflicts")]
    Conflict,
    /// Catalog bytes or the immutable prefix differ.
    #[error("model inventory is corrupt")]
    Corrupt,
    /// A checked sequence or allocation cannot be represented.
    #[error("model inventory capacity exceeded")]
    Capacity,
    /// The sole catalog authority is unavailable.
    #[error("model inventory is unavailable")]
    Unavailable,
    /// SQLite failed without successful publication.
    #[error(transparent)]
    Storage(#[from] rusqlite::Error),
    /// Catalog authority failed.
    #[error(transparent)]
    Catalog(#[from] CatalogError),
}
