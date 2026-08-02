//! Grace-bounded orphan enumeration, quarantine, and deletion.

#[cfg(test)]
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;

use market_squawk_domain::Timestamp;
use tokio_util::sync::CancellationToken;

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
    cancellation: &'a CancellationToken,
    deadline: Instant,
    _lease: PublicationLease,
    candidates: Vec<PublishedObject>,
    quarantined: usize,
    deleted: usize,
}

impl OrphanRecoverySession<'_> {
    pub(crate) fn candidates(&self) -> &[PublishedObject] {
        &self.candidates
    }

    #[cfg(test)]
    pub(crate) fn quarantine(&mut self, object: &PublishedObject) -> Result<(), ParquetStoreError> {
        check_recovery_operation(self.cancellation, self.deadline)?;
        if !self.candidates.contains(object) {
            return Err(ParquetStoreError::ObjectMetadataMismatch);
        }
        self.quarantine_verified(object)
    }

    fn quarantine_verified(&mut self, object: &PublishedObject) -> Result<(), ParquetStoreError> {
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

    pub(crate) fn quarantine_candidate(&mut self, index: usize) -> Result<(), ParquetStoreError> {
        let object = self
            .candidates
            .get(index)
            .cloned()
            .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
        check_recovery_operation(self.cancellation, self.deadline)?;
        self.quarantine_verified(&object)
    }

    pub(crate) fn finish(self) -> Result<OrphanRecoveryReport, ParquetStoreError> {
        check_recovery_operation(self.cancellation, self.deadline)?;
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
        let cancellation = CancellationToken::new();
        scan_objects(
            &self.directory,
            OBJECTS,
            &cancellation,
            Instant::now() + Duration::from_secs(60),
        )
    }

    #[cfg(test)]
    pub(crate) async fn collect_orphans_fault_fixture(
        &self,
        referenced: &[Sha256Digest],
        now: Timestamp,
    ) -> Result<OrphanRecoveryReport, ParquetStoreError> {
        let referenced: BTreeSet<_> = referenced.iter().copied().collect();
        let cancellation = CancellationToken::new();
        let mut recovery = self
            .begin_recovery(now, &cancellation, Instant::now() + Duration::from_secs(60))
            .await?;
        for object in recovery.candidates().to_vec() {
            if !referenced.contains(&object.content_hash) {
                recovery.quarantine(&object)?;
            }
        }
        recovery.finish()
    }

    pub(crate) async fn begin_recovery<'a>(
        &'a self,
        now: Timestamp,
        cancellation: &'a CancellationToken,
        deadline: Instant,
    ) -> Result<OrphanRecoverySession<'a>, ParquetStoreError> {
        check_recovery_operation(cancellation, deadline)?;
        let lease = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ParquetStoreError::Cancelled),
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(ParquetStoreError::RecoveryDeadlineExceeded);
            }
            lease = self.authority.publication.acquire_recovery() => lease,
        };
        check_recovery_operation(cancellation, deadline)?;
        let deleted = delete_expired_quarantine(
            &self.directory,
            now,
            self.config.orphan_grace,
            cancellation,
            deadline,
        )?;
        let quarantined = self.quarantine_expired_staging(now, cancellation, deadline)?;
        let cutoff = recovery_cutoff(now, self.config.orphan_grace)?;
        let candidates = scan_objects(&self.directory, OBJECTS, cancellation, deadline)?
            .into_iter()
            .filter(|object| object.created_at.unix_nanos() <= cutoff)
            .collect();
        Ok(OrphanRecoverySession {
            store: self,
            cancellation,
            deadline,
            _lease: lease,
            candidates,
            quarantined,
            deleted,
        })
    }

    fn quarantine_expired_staging(
        &self,
        now: Timestamp,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<usize, ParquetStoreError> {
        let cutoff = recovery_cutoff(now, self.config.orphan_grace)?;
        let mut quarantined = 0_usize;
        let mut scanned = 0_usize;
        for entry in self.directory.read_dir(STAGING)? {
            check_recovery_operation(cancellation, deadline)?;
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

fn scan_objects(
    directory: &Dir,
    root: &str,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<Vec<PublishedObject>, ParquetStoreError> {
    let mut objects = Vec::new();
    let mut prefixes = 0_usize;
    for prefix in directory.read_dir(root)? {
        check_recovery_operation(cancellation, deadline)?;
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
            check_recovery_operation(cancellation, deadline)?;
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
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<usize, ParquetStoreError> {
    let cutoff = recovery_cutoff(now, grace)?;
    let mut deleted = 0_usize;
    let mut scanned = 0_usize;
    for entry in directory.read_dir(QUARANTINE)? {
        check_recovery_operation(cancellation, deadline)?;
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

fn check_recovery_operation(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), ParquetStoreError> {
    if cancellation.is_cancelled() {
        Err(ParquetStoreError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ParquetStoreError::RecoveryDeadlineExceeded)
    } else {
        Ok(())
    }
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
            [7; 32],
            None,
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
