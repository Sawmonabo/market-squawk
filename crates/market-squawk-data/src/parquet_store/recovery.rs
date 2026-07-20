//! Grace-bounded orphan enumeration, quarantine, and deletion.

#[cfg(test)]
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use market_squawk_domain::Timestamp;

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// One bounded orphan reconciliation result.
pub struct OrphanRecoveryReport {
    quarantined: usize,
    deleted: usize,
}

impl OrphanRecoveryReport {
    /// Returns newly quarantined unreferenced objects.
    pub const fn quarantined(self) -> usize {
        self.quarantined
    }

    /// Returns quarantine entries deleted after the grace interval.
    pub const fn deleted(self) -> usize {
        self.deleted
    }
}

#[derive(Debug)]
pub(crate) struct OrphanRecoverySession<'a> {
    store: &'a ParquetObjectStore,
    _lease: PublicationLease,
    candidates: Vec<PublishedObject>,
    quarantined: usize,
    deleted: usize,
}

impl OrphanRecoverySession<'_> {
    pub(crate) fn candidates(&self) -> &[PublishedObject] {
        &self.candidates
    }

    pub(crate) fn quarantine(&mut self, object: &PublishedObject) -> Result<(), ParquetStoreError> {
        if !self.candidates.contains(object) {
            return Err(ParquetStoreError::ObjectMetadataMismatch);
        }
        let name = format!("{}.parquet", encode_hex(object.content_hash.bytes()));
        let destination = format!("{QUARANTINE}/{name}");
        match self
            .store
            .publish_no_replace(&object.relative_reference, &destination)
        {
            Ok(()) => {
                self.quarantined = self
                    .quarantined
                    .checked_add(1)
                    .ok_or(ParquetStoreError::RecoveryScanLimit)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                self.store
                    .directory
                    .remove_file(&object.relative_reference)?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<OrphanRecoveryReport, ParquetStoreError> {
        sync_directory(&self.store.directory, QUARANTINE)?;
        sync_directory(&self.store.directory, OBJECTS)?;
        Ok(OrphanRecoveryReport {
            quarantined: self.quarantined,
            deleted: self.deleted,
        })
    }
}

impl ParquetObjectStore {
    /// Returns a bounded snapshot of content-addressed final objects.
    #[cfg(test)]
    pub(crate) fn published_objects(&self) -> Result<Vec<PublishedObject>, ParquetStoreError> {
        scan_objects(&self.directory, OBJECTS)
    }

    #[cfg(test)]
    pub(crate) async fn collect_orphans_fault_fixture(
        &self,
        referenced: &[Sha256Digest],
        now: Timestamp,
    ) -> Result<OrphanRecoveryReport, ParquetStoreError> {
        let referenced: BTreeSet<_> = referenced.iter().copied().collect();
        let mut recovery = self.begin_recovery(now).await?;
        for object in recovery.candidates().to_vec() {
            if !referenced.contains(&object.content_hash) {
                recovery.quarantine(&object)?;
            }
        }
        recovery.finish()
    }

    pub(crate) async fn begin_recovery(
        &self,
        now: Timestamp,
    ) -> Result<OrphanRecoverySession<'_>, ParquetStoreError> {
        let lease = self.authority.publication.acquire_recovery().await;
        let deleted = delete_expired_quarantine(&self.directory, now, self.config.orphan_grace)?;
        let quarantined = self.quarantine_expired_staging(now)?;
        let cutoff = recovery_cutoff(now, self.config.orphan_grace)?;
        let candidates = scan_objects(&self.directory, OBJECTS)?
            .into_iter()
            .filter(|object| object.created_at.unix_nanos() <= cutoff)
            .collect();
        Ok(OrphanRecoverySession {
            store: self,
            _lease: lease,
            candidates,
            quarantined,
            deleted,
        })
    }

    fn quarantine_expired_staging(&self, now: Timestamp) -> Result<usize, ParquetStoreError> {
        let cutoff = recovery_cutoff(now, self.config.orphan_grace)?;
        let mut quarantined = 0_usize;
        let mut scanned = 0_usize;
        for entry in self.directory.read_dir(STAGING)? {
            scanned = scanned
                .checked_add(1)
                .ok_or(ParquetStoreError::RecoveryScanLimit)?;
            if scanned > MAX_SCAN_OBJECTS {
                return Err(ParquetStoreError::RecoveryScanLimit);
            }
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
            let identifier = name
                .strip_suffix(".tmp")
                .and_then(|value| Uuid::parse_str(value).ok())
                .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
            let modified = timestamp_from_system_time(entry.metadata()?.modified()?.into_std())?;
            if modified.unix_nanos() > cutoff {
                continue;
            }
            let source = format!("{STAGING}/{name}");
            let destination = format!("{QUARANTINE}/staged-{identifier}.tmp");
            match self.publish_no_replace(&source, &destination) {
                Ok(()) => {
                    quarantined = quarantined
                        .checked_add(1)
                        .ok_or(ParquetStoreError::RecoveryScanLimit)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    self.directory.remove_file(source)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(quarantined)
    }
}

fn scan_objects(directory: &Dir, root: &str) -> Result<Vec<PublishedObject>, ParquetStoreError> {
    let mut objects = Vec::new();
    let mut prefixes = 0_usize;
    for prefix in directory.read_dir(root)? {
        let prefix = prefix?;
        prefixes = prefixes
            .checked_add(1)
            .ok_or(ParquetStoreError::RecoveryScanLimit)?;
        if prefixes > 256 {
            return Err(ParquetStoreError::RecoveryScanLimit);
        }
        if !prefix.file_type()?.is_dir() {
            return Err(ParquetStoreError::ObjectMetadataMismatch);
        }
        let prefix_name = prefix.file_name();
        let prefix_name = prefix_name
            .to_str()
            .filter(|value| {
                value.len() == 2
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            })
            .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
        let prefix_path = format!("{root}/{prefix_name}");
        for entry in directory.read_dir(&prefix_path)? {
            if objects.len() >= MAX_SCAN_OBJECTS {
                return Err(ParquetStoreError::RecoveryScanLimit);
            }
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
            let encoded = name
                .strip_suffix(".parquet")
                .filter(|value| value.starts_with(prefix_name))
                .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
            let named_digest = decode_hex(encoded)
                .map(Sha256Digest::new)
                .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
            let reference = format!("{prefix_path}/{name}");
            let object = object_from_reference(directory, &reference, 0)?;
            if object.content_hash != named_digest {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            objects.push(object);
        }
    }
    Ok(objects)
}

fn delete_expired_quarantine(
    directory: &Dir,
    now: Timestamp,
    grace: Duration,
) -> Result<usize, ParquetStoreError> {
    let cutoff = recovery_cutoff(now, grace)?;
    let mut deleted = 0_usize;
    let mut scanned = 0_usize;
    for entry in directory.read_dir(QUARANTINE)? {
        scanned = scanned
            .checked_add(1)
            .ok_or(ParquetStoreError::RecoveryScanLimit)?;
        if scanned > MAX_SCAN_OBJECTS {
            return Err(ParquetStoreError::RecoveryScanLimit);
        }
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(ParquetStoreError::ObjectMetadataMismatch);
        }
        let modified = timestamp_from_system_time(entry.metadata()?.modified()?.into_std())?;
        if modified.unix_nanos() <= cutoff {
            let name = entry.file_name();
            directory.remove_file(Path::new(QUARANTINE).join(name))?;
            deleted = deleted
                .checked_add(1)
                .ok_or(ParquetStoreError::RecoveryScanLimit)?;
        }
    }
    Ok(deleted)
}

fn recovery_cutoff(now: Timestamp, grace: Duration) -> Result<i64, ParquetStoreError> {
    let grace_nanos =
        i64::try_from(grace.as_nanos()).map_err(|_| ParquetStoreError::SizeOverflow)?;
    now.unix_nanos()
        .checked_sub(grace_nanos)
        .ok_or(ParquetStoreError::SizeOverflow)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use market_squawk_platform::LocalPaths;
    use tokio_util::sync::CancellationToken;

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    #[tokio::test]
    async fn direct_orphan_collection_is_a_private_lease_serialized_fault_fixture() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
        let store = Arc::new(ParquetObjectStore::open(
            paths.artifacts()?.clone(),
            ObjectStoreConfig::try_new(1024 * 1024, 2, Duration::from_secs(60))?,
            paths.catalog()?.path(),
            [7; 32],
        )?);
        let batch = RecordBatch::try_new(
            Schema::new(vec![Field::new("value", DataType::Int64, false)]).into(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        )?;
        let cancellation = CancellationToken::new();
        let first = store.publish(&batch, &cancellation).await?;
        assert_eq!(store.publish(&batch, &cancellation).await?, first);
        assert_eq!(store.published_objects()?.len(), 1);

        let lease = store.begin_publication(&cancellation).await?;
        let recovery_store = Arc::clone(&store);
        let recovery_now = first.created_at().checked_add_nanos(61_000_000_000)?;
        let referenced = [first.content_hash()];
        let recovery = tokio::spawn(async move {
            recovery_store
                .collect_orphans_fault_fixture(&referenced, recovery_now)
                .await
        });
        tokio::task::yield_now().await;
        assert!(!recovery.is_finished());
        drop(lease);
        assert_eq!(recovery.await??.quarantined(), 0);
        assert!(store.verify(&first)?);

        let report = store
            .collect_orphans_fault_fixture(&[], recovery_now)
            .await?;
        assert_eq!(report.quarantined(), 1);
        assert_eq!(store.published_objects()?.len(), 0);
        assert_eq!(
            store
                .collect_orphans_fault_fixture(
                    &[],
                    first.created_at().checked_add_nanos(122_000_000_000)?,
                )
                .await?
                .deleted(),
            1
        );
        Ok(())
    }
}
