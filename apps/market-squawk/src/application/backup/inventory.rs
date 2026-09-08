//! Durable backup inventory and recoverable two-phase retention.

use std::{collections::BTreeSet, fmt, path::Path, sync::Mutex};

use async_trait::async_trait;
use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::Mutex as TokioMutex;

use super::{ProductBackupError, ProductBackupManifest};

const FORMAT_VERSION: u16 = 1;
const AUTHORITY_DIRECTORY: &str = "backup-inventory";
const MAXIMUM_BACKUPS: usize = 128;
const MAXIMUM_PAGE_SIZE: usize = 64;
const MAXIMUM_RETENTION_BATCH: usize = 32;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum InventoryState {
    Verified,
    PendingDeletion,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InventoryEntry {
    manifest: ProductBackupManifest,
    state: InventoryState,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InventoryDocument {
    format_version: u16,
    revision: u64,
    entries: Vec<InventoryEntry>,
}

impl InventoryDocument {
    fn empty() -> Self {
        Self {
            format_version: FORMAT_VERSION,
            revision: 1,
            entries: Vec::new(),
        }
    }

    fn validate(mut self) -> Result<Self, ProductBackupError> {
        if self.format_version != FORMAT_VERSION
            || self.revision == 0
            || self.entries.len() > MAXIMUM_BACKUPS
        {
            return Err(ProductBackupError::InventoryCorrupt);
        }
        let mut identities = BTreeSet::new();
        for entry in &self.entries {
            entry.manifest.verify()?;
            if !identities.insert(entry.manifest.backup_id()) {
                return Err(ProductBackupError::InventoryCorrupt);
            }
        }
        sort_entries(&mut self.entries);
        Ok(self)
    }

    fn advance_revision(&mut self) -> Result<(), ProductBackupError> {
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(ProductBackupError::InventoryCorrupt)?;
        Ok(())
    }
}

/// One bounded stable inventory page.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupInventoryPage {
    revision: u64,
    manifests: Vec<ProductBackupManifest>,
    next_after_backup_id: Option<[u8; 32]>,
    pending_deletions: usize,
}

/// Exact retention preview bound to the current inventory revision.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupRetentionPreview {
    revision: u64,
    keep_latest: usize,
    delete_backup_ids: Vec<[u8; 32]>,
    preview_sha256: [u8; 32],
}

impl BackupRetentionPreview {
    /// Consumes a nonempty exact preview into an approval token.
    pub fn try_approve(self) -> Result<BackupRetentionApproval, ProductBackupError> {
        if self.delete_backup_ids.is_empty() {
            return Err(ProductBackupError::RetentionEmpty);
        }
        Ok(BackupRetentionApproval {
            revision: self.revision,
            keep_latest: self.keep_latest,
            delete_backup_ids: self.delete_backup_ids,
            preview_sha256: self.preview_sha256,
        })
    }
}

/// Explicit retention approval bound to exact backup identities and inventory revision.
#[derive(Clone, Debug)]
pub struct BackupRetentionApproval {
    revision: u64,
    keep_latest: usize,
    delete_backup_ids: Vec<[u8; 32]>,
    preview_sha256: [u8; 32],
}

impl BackupRetentionApproval {
    /// Returns the canonical preview commitment covering the inventory revision, retention count,
    /// and ordered exact backup identities authorized for removal.
    #[must_use]
    pub const fn preview_sha256(&self) -> [u8; 32] {
        self.preview_sha256
    }
}

/// Controlled bundle removal authority. Removal must be idempotent for crash recovery.
#[async_trait]
pub trait BackupBundleRemover: fmt::Debug + Send + Sync {
    /// Removes one exact managed bundle and no unrelated backup or active workspace data.
    async fn remove_exact(&self, backup_id: [u8; 32]) -> Result<(), ProductBackupError>;
}

/// Exclusive durable owner of backup inventory and retention transitions.
pub struct ProductBackupInventory {
    store: LocalAuthorityStateStore,
    document: Mutex<InventoryDocument>,
    mutation: TokioMutex<()>,
}

impl ProductBackupInventory {
    /// Opens or initializes the bounded inventory below the prepared control root.
    pub fn try_open(control_root: &Path) -> Result<Self, ProductBackupError> {
        let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))?;
        let document = match store.load()? {
            Some(encoded) => serde_json::from_slice::<InventoryDocument>(&encoded)
                .map_err(|_| ProductBackupError::InventoryCorrupt)?
                .validate()?,
            None => {
                let document = InventoryDocument::empty();
                store.store(&encode(&document)?)?;
                document
            }
        };
        Ok(Self {
            store,
            document: Mutex::new(document),
            mutation: TokioMutex::new(()),
        })
    }

    /// Registers one reverified manifest idempotently.
    pub async fn register(
        &self,
        manifest: ProductBackupManifest,
    ) -> Result<(), ProductBackupError> {
        manifest.verify()?;
        let _mutation = self.mutation.lock().await;
        let mut document = self
            .document
            .lock()
            .map_err(|_| ProductBackupError::InventoryUnavailable)?;
        if document
            .entries
            .iter()
            .any(|entry| entry.manifest.backup_id() == manifest.backup_id())
        {
            return Ok(());
        }
        if document.entries.len() == MAXIMUM_BACKUPS {
            return Err(ProductBackupError::InventoryCapacity);
        }
        let mut candidate = document.clone();
        candidate.entries.push(InventoryEntry {
            manifest,
            state: InventoryState::Verified,
        });
        sort_entries(&mut candidate.entries);
        candidate.advance_revision()?;
        self.store.store(&encode(&candidate)?)?;
        *document = candidate;
        Ok(())
    }

    /// Lists verified backups in descending capture-time order.
    pub fn list(
        &self,
        after_backup_id: Option<[u8; 32]>,
        limit: usize,
    ) -> Result<BackupInventoryPage, ProductBackupError> {
        if limit == 0 || limit > MAXIMUM_PAGE_SIZE {
            return Err(ProductBackupError::InvalidInventoryLimit);
        }
        let document = self
            .document
            .lock()
            .map_err(|_| ProductBackupError::InventoryUnavailable)?;
        let start = after_backup_id
            .map(|cursor| {
                document
                    .entries
                    .iter()
                    .position(|entry| entry.manifest.backup_id() == cursor)
                    .map(|index| index.saturating_add(1))
                    .ok_or(ProductBackupError::InvalidInventoryCursor)
            })
            .transpose()?
            .unwrap_or(0);
        let mut manifests = document
            .entries
            .iter()
            .skip(start)
            .filter(|entry| entry.state == InventoryState::Verified)
            .take(limit.saturating_add(1))
            .map(|entry| entry.manifest.clone())
            .collect::<Vec<_>>();
        let has_more = manifests.len() > limit;
        manifests.truncate(limit);
        let next_after_backup_id = has_more
            .then(|| manifests.last().map(ProductBackupManifest::backup_id))
            .flatten();
        Ok(BackupInventoryPage {
            revision: document.revision,
            manifests,
            next_after_backup_id,
            pending_deletions: document
                .entries
                .iter()
                .filter(|entry| entry.state == InventoryState::PendingDeletion)
                .count(),
        })
    }

    /// Returns one exact verified backup manifest by its content-derived identity.
    pub fn get(&self, backup_id: [u8; 32]) -> Result<ProductBackupManifest, ProductBackupError> {
        self.document
            .lock()
            .map_err(|_| ProductBackupError::InventoryUnavailable)?
            .entries
            .iter()
            .find(|entry| {
                entry.state == InventoryState::Verified && entry.manifest.backup_id() == backup_id
            })
            .map(|entry| entry.manifest.clone())
            .ok_or(ProductBackupError::BackupNotFound)
    }

    /// Previews deleting at most one bounded batch while retaining at least one verified backup.
    pub fn preview_retention(
        &self,
        keep_latest: usize,
    ) -> Result<BackupRetentionPreview, ProductBackupError> {
        if keep_latest == 0 || keep_latest > MAXIMUM_BACKUPS {
            return Err(ProductBackupError::InvalidRetentionPolicy);
        }
        let document = self
            .document
            .lock()
            .map_err(|_| ProductBackupError::InventoryUnavailable)?;
        let delete_backup_ids = document
            .entries
            .iter()
            .filter(|entry| entry.state == InventoryState::Verified)
            .skip(keep_latest)
            .take(MAXIMUM_RETENTION_BATCH)
            .map(|entry| entry.manifest.backup_id())
            .collect::<Vec<_>>();
        let preview_sha256 = retention_digest(document.revision, keep_latest, &delete_backup_ids)?;
        Ok(BackupRetentionPreview {
            revision: document.revision,
            keep_latest,
            delete_backup_ids,
            preview_sha256,
        })
    }

    /// Applies two-phase retention, retaining pending markers across interrupted removal.
    pub async fn apply_retention(
        &self,
        approval: BackupRetentionApproval,
        remover: &dyn BackupBundleRemover,
    ) -> Result<usize, ProductBackupError> {
        let _mutation = self.mutation.lock().await;
        let expected_digest = retention_digest(
            approval.revision,
            approval.keep_latest,
            &approval.delete_backup_ids,
        )?;
        if expected_digest != approval.preview_sha256 {
            return Err(ProductBackupError::StaleRetentionApproval);
        }
        {
            let mut document = self
                .document
                .lock()
                .map_err(|_| ProductBackupError::InventoryUnavailable)?;
            if document.revision != approval.revision {
                return Err(ProductBackupError::StaleRetentionApproval);
            }
            let requested = approval
                .delete_backup_ids
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            let verified_count = document
                .entries
                .iter()
                .filter(|entry| entry.state == InventoryState::Verified)
                .count();
            if requested.len() != approval.delete_backup_ids.len()
                || approval.keep_latest == 0
                || verified_count
                    .checked_sub(requested.len())
                    .is_none_or(|remaining| remaining < approval.keep_latest)
                || document
                    .entries
                    .iter()
                    .filter(|entry| requested.contains(&entry.manifest.backup_id()))
                    .any(|entry| entry.state != InventoryState::Verified)
                || document
                    .entries
                    .iter()
                    .filter(|entry| requested.contains(&entry.manifest.backup_id()))
                    .count()
                    != requested.len()
            {
                return Err(ProductBackupError::StaleRetentionApproval);
            }
            let mut candidate = document.clone();
            for entry in &mut candidate.entries {
                if requested.contains(&entry.manifest.backup_id()) {
                    entry.state = InventoryState::PendingDeletion;
                }
            }
            candidate.advance_revision()?;
            self.store.store(&encode(&candidate)?)?;
            *document = candidate;
        }
        for backup_id in &approval.delete_backup_ids {
            remover.remove_exact(*backup_id).await?;
        }
        let mut document = self
            .document
            .lock()
            .map_err(|_| ProductBackupError::InventoryUnavailable)?;
        let requested = approval
            .delete_backup_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut candidate = document.clone();
        candidate.entries.retain(|entry| {
            !(entry.state == InventoryState::PendingDeletion
                && requested.contains(&entry.manifest.backup_id()))
        });
        candidate.advance_revision()?;
        self.store.store(&encode(&candidate)?)?;
        *document = candidate;
        Ok(requested.len())
    }

    /// Resumes every pending deletion after an interrupted retention operation.
    pub async fn recover_pending(
        &self,
        remover: &dyn BackupBundleRemover,
    ) -> Result<usize, ProductBackupError> {
        let _mutation = self.mutation.lock().await;
        let pending = self
            .document
            .lock()
            .map_err(|_| ProductBackupError::InventoryUnavailable)?
            .entries
            .iter()
            .filter(|entry| entry.state == InventoryState::PendingDeletion)
            .map(|entry| entry.manifest.backup_id())
            .collect::<Vec<_>>();
        for backup_id in &pending {
            remover.remove_exact(*backup_id).await?;
        }
        if pending.is_empty() {
            return Ok(0);
        }
        let pending_set = pending.iter().copied().collect::<BTreeSet<_>>();
        let mut document = self
            .document
            .lock()
            .map_err(|_| ProductBackupError::InventoryUnavailable)?;
        let mut candidate = document.clone();
        candidate.entries.retain(|entry| {
            !(entry.state == InventoryState::PendingDeletion
                && pending_set.contains(&entry.manifest.backup_id()))
        });
        candidate.advance_revision()?;
        self.store.store(&encode(&candidate)?)?;
        *document = candidate;
        Ok(pending.len())
    }
}

impl fmt::Debug for ProductBackupInventory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductBackupInventory([EXCLUSIVE RETENTION AUTHORITY])")
    }
}

fn retention_digest(
    revision: u64,
    keep_latest: usize,
    delete_backup_ids: &[[u8; 32]],
) -> Result<[u8; 32], ProductBackupError> {
    serde_json::to_vec(&(
        "market-squawk-backup-retention-preview-v1",
        revision,
        keep_latest,
        delete_backup_ids,
    ))
    .map(|encoded| Sha256::digest(encoded).into())
    .map_err(|_| ProductBackupError::Encoding)
}

fn sort_entries(entries: &mut [InventoryEntry]) {
    entries.sort_by(|left, right| {
        right
            .manifest
            .created_at()
            .cmp(&left.manifest.created_at())
            .then_with(|| left.manifest.backup_id().cmp(&right.manifest.backup_id()))
    });
}

fn encode(document: &InventoryDocument) -> Result<Vec<u8>, ProductBackupError> {
    let encoded = serde_json::to_vec(document).map_err(|_| ProductBackupError::InventoryCorrupt)?;
    if encoded.len() > LocalAuthorityStateStore::maximum_payload_bytes() {
        return Err(ProductBackupError::InventoryCapacity);
    }
    Ok(encoded)
}

impl From<LocalAuthorityStateStoreError> for ProductBackupError {
    fn from(_error: LocalAuthorityStateStoreError) -> Self {
        Self::InventoryPersistence
    }
}
