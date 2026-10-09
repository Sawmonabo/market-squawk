//! Bounded service-owned reuse of verified immutable indexes, never selection or authority.
use super::{OpenedGeneration, SecResearchDisplayRows, SecResearchReadError};
use market_squawk_domain::EvidenceDigest;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

// Each owner keeps a bounded SQLite page cache and disk-backed row descriptors. Eviction
// releases only the cache's reference; existing selection handles retain their own owners.
const MAXIMUM_RESIDENT_INDEXES: usize = 16;

#[derive(Debug, Default)]
pub(crate) struct SecPreparedReadCache {
    entries: Mutex<VecDeque<(EvidenceDigest, CachedIndex)>>,
}

#[derive(Clone, Debug)]
enum CachedIndex {
    Generation(Arc<OpenedGeneration>),
    Display(SecResearchDisplayRows),
}
impl CachedIndex {
    fn record(&self) -> &crate::ArtifactRecord {
        match self {
            Self::Generation(value) => &value.artifact.record,
            Self::Display(value) => &value.artifact.record,
        }
    }
}
impl SecPreparedReadCache {
    fn get(
        &self,
        key: EvidenceDigest,
        record: &crate::ArtifactRecord,
    ) -> Result<Option<CachedIndex>, SecResearchReadError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| SecResearchReadError::AuthorityUnavailable)?;
        let Some(position) = entries
            .iter()
            .position(|(candidate, value)| *candidate == key && value.record() == record)
        else {
            return Ok(None);
        };
        let entry = entries
            .remove(position)
            .ok_or(SecResearchReadError::PreparedIntegrity)?;
        let value = entry.1.clone();
        entries.push_back(entry);
        Ok(Some(value))
    }
    fn insert(&self, key: EvidenceDigest, value: CachedIndex) -> Result<(), SecResearchReadError> {
        let (replaced, evicted) = {
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| SecResearchReadError::AuthorityUnavailable)?;
            let replaced = entries
                .iter()
                .position(|(candidate, _)| *candidate == key)
                .and_then(|position| entries.remove(position));
            entries.push_back((key, value));
            let evicted = (entries.len() > MAXIMUM_RESIDENT_INDEXES).then(|| entries.pop_front());
            (replaced, evicted)
        };
        // Closing SQLite/file owners can perform I/O, so drop them outside the cache lock.
        drop((replaced, evicted));
        Ok(())
    }
    pub(super) fn generation(
        &self,
        key: EvidenceDigest,
        record: &crate::ArtifactRecord,
    ) -> Result<Option<OpenedGeneration>, SecResearchReadError> {
        match self.get(key, record)? {
            Some(CachedIndex::Generation(value)) => Ok(Some((*value).clone())),
            None => Ok(None),
            _ => Err(SecResearchReadError::PreparedIntegrity),
        }
    }
    pub(super) fn display(
        &self,
        key: EvidenceDigest,
        record: &crate::ArtifactRecord,
    ) -> Result<Option<SecResearchDisplayRows>, SecResearchReadError> {
        match self.get(key, record)? {
            Some(CachedIndex::Display(value)) => Ok(Some(value)),
            None => Ok(None),
            _ => Err(SecResearchReadError::PreparedIntegrity),
        }
    }
    pub(super) fn insert_generation(
        &self,
        key: EvidenceDigest,
        value: OpenedGeneration,
    ) -> Result<(), SecResearchReadError> {
        self.insert(key, CachedIndex::Generation(Arc::new(value)))
    }
    pub(super) fn insert_display(
        &self,
        key: EvidenceDigest,
        value: SecResearchDisplayRows,
    ) -> Result<(), SecResearchReadError> {
        self.insert(key, CachedIndex::Display(value))
    }
}

/// Compare the held descriptor and its current named endpoint to the baseline captured
/// before hashing. Changes during verification also fail the post-verification check.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PreparedFileStamp {
    device: u64,
    inode: u64,
    length: u64,
    modified: cap_std::time::SystemTime,
    #[cfg(unix)]
    changed: (i64, i64),
}
impl PreparedFileStamp {
    pub(super) fn read(file: &std::fs::File) -> Result<Self, SecResearchReadError> {
        let metadata = cap_std::fs::File::from_std(
            file.try_clone()
                .map_err(|_| SecResearchReadError::PreparedIo)?,
        )
        .metadata()
        .map_err(|_| SecResearchReadError::PreparedIo)?;
        if !metadata.is_file() {
            return Err(SecResearchReadError::PreparedIntegrity);
        }
        #[cfg(unix)]
        use cap_std::fs::MetadataExt as _;
        Ok(Self {
            device: cap_fs_ext::MetadataExt::dev(&metadata),
            inode: cap_fs_ext::MetadataExt::ino(&metadata),
            length: metadata.len(),
            modified: metadata
                .modified()
                .map_err(|_| SecResearchReadError::PreparedIo)?,
            #[cfg(unix)]
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
}
