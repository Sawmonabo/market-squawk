//! Immutable analytical generation plans and identities.

use std::fmt;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

mod catalog;

pub use self::catalog::{
    AnalyticalManifestCatalog, GenerationKind, ManifestCatalogError, PinnedDataset,
    PinnedManifestObject,
};

/// Stable local analytical dataset identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DatasetId(String);

impl DatasetId {
    /// Returns the validated dataset identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for DatasetId {
    type Error = ManifestPlanError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.is_empty()
            || value.len() > 256
            || value
                .bytes()
                .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')))
        {
            Err(ManifestPlanError::InvalidDatasetId)
        } else {
            Ok(Self(value.to_owned()))
        }
    }
}

/// Exact SHA-256 identity with no algorithm ambiguity.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    /// Constructs an already computed SHA-256 value.
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the digest bytes.
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }

    pub(crate) const fn evidence(self) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, self.0)
    }
}

impl fmt::Debug for Sha256Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Sha256Digest([REDACTED CONTENT IDENTITY])")
    }
}

/// Immutable reader pin for one committed manifest generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetManifestRef {
    dataset_id: DatasetId,
    manifest_version: u64,
    content_hash: Sha256Digest,
}

impl DatasetManifestRef {
    /// Constructs a nonzero generation pin.
    pub fn try_new(
        dataset_id: DatasetId,
        manifest_version: u64,
        content_hash: Sha256Digest,
    ) -> Result<Self, ManifestPlanError> {
        if manifest_version == 0 {
            return Err(ManifestPlanError::InvalidManifestVersion);
        }
        Ok(Self {
            dataset_id,
            manifest_version,
            content_hash,
        })
    }

    /// Returns the dataset identity.
    pub const fn dataset_id(&self) -> &DatasetId {
        &self.dataset_id
    }

    /// Returns the immutable generation number.
    pub const fn manifest_version(&self) -> u64 {
        self.manifest_version
    }

    /// Returns the semantic manifest hash.
    pub const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }
}

/// One immutable Parquet object included in a manifest generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestObject {
    content_hash: Sha256Digest,
    row_count: u64,
    size_bytes: u64,
    lineage_digest: Sha256Digest,
}

impl ManifestObject {
    /// Constructs bounded, nonempty object metadata.
    pub fn try_new(
        content_hash: Sha256Digest,
        row_count: u64,
        size_bytes: u64,
        lineage_digest: Sha256Digest,
    ) -> Result<Self, ManifestPlanError> {
        if row_count == 0 || size_bytes == 0 {
            return Err(ManifestPlanError::EmptyObject);
        }
        Ok(Self {
            content_hash,
            row_count,
            size_bytes,
            lineage_digest,
        })
    }

    /// Returns exact physical content identity.
    pub const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    /// Returns object rows.
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns exact Parquet bytes.
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Returns the canonical row/lineage semantic identity.
    pub const fn lineage_digest(&self) -> Sha256Digest {
        self.lineage_digest
    }
}

/// Complete immutable object-set plan for one generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestPlan {
    dataset_id: DatasetId,
    objects: Vec<ManifestObject>,
    row_count: u64,
    total_bytes: u64,
    lineage_digest: Sha256Digest,
    content_hash: Sha256Digest,
}

impl ManifestPlan {
    /// Appends one immutable object while enforcing the configured small-file ceiling.
    pub fn append(
        dataset_id: DatasetId,
        previous: Option<&Self>,
        object: ManifestObject,
        max_objects: usize,
    ) -> Result<Self, ManifestPlanError> {
        if max_objects == 0 {
            return Err(ManifestPlanError::SmallFileCeiling { max_objects });
        }
        let mut objects = match previous {
            Some(previous) if previous.dataset_id == dataset_id => previous.objects.clone(),
            Some(_) => return Err(ManifestPlanError::DatasetMismatch),
            None => Vec::new(),
        };
        if objects.len() >= max_objects {
            return Err(ManifestPlanError::SmallFileCeiling { max_objects });
        }
        objects.push(object);
        Self::from_objects(dataset_id, objects)
    }

    /// Replaces a prior generation's object set with one semantically identical compacted object.
    pub fn compact(previous: &Self, compacted: ManifestObject) -> Result<Self, ManifestPlanError> {
        if compacted.row_count != previous.row_count
            || compacted.lineage_digest != previous.lineage_digest
        {
            return Err(ManifestPlanError::CompactionSemanticMismatch);
        }
        Self::from_objects(previous.dataset_id.clone(), vec![compacted])
    }

    /// Returns the dataset identity.
    pub const fn dataset_id(&self) -> &DatasetId {
        &self.dataset_id
    }

    /// Returns the immutable ordered object set.
    pub fn objects(&self) -> &[ManifestObject] {
        &self.objects
    }

    /// Returns total rows under checked arithmetic.
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns total physical bytes.
    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Returns canonical row and source-lineage identity.
    pub const fn lineage_digest(&self) -> Sha256Digest {
        self.lineage_digest
    }

    /// Returns the exact ordered manifest-plan identity.
    pub const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    fn from_objects(
        dataset_id: DatasetId,
        objects: Vec<ManifestObject>,
    ) -> Result<Self, ManifestPlanError> {
        let row_count = objects.iter().try_fold(0_u64, |total, object| {
            total
                .checked_add(object.row_count)
                .ok_or(ManifestPlanError::CountOverflow)
        })?;
        let total_bytes = objects.iter().try_fold(0_u64, |total, object| {
            total
                .checked_add(object.size_bytes)
                .ok_or(ManifestPlanError::CountOverflow)
        })?;
        let lineage_digest = if objects.len() == 1 {
            objects[0].lineage_digest
        } else {
            let mut hash = Sha256::new();
            hash.update(b"market-squawk/analytical-lineage/v1");
            for object in &objects {
                hash.update(object.lineage_digest.bytes());
            }
            Sha256Digest::new(hash.finalize().into())
        };
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/manifest-plan/v1");
        hash.update((dataset_id.as_str().len() as u64).to_be_bytes());
        hash.update(dataset_id.as_str().as_bytes());
        hash.update(row_count.to_be_bytes());
        hash.update(total_bytes.to_be_bytes());
        hash.update(lineage_digest.bytes());
        for object in &objects {
            hash.update(object.content_hash.bytes());
            hash.update(object.row_count.to_be_bytes());
            hash.update(object.size_bytes.to_be_bytes());
            hash.update(object.lineage_digest.bytes());
        }
        Ok(Self {
            dataset_id,
            objects,
            row_count,
            total_bytes,
            lineage_digest,
            content_hash: Sha256Digest::new(hash.finalize().into()),
        })
    }
}

/// Immutable manifest planning failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ManifestPlanError {
    /// Dataset identity is empty, oversized, or nonportable.
    #[error("dataset identity is invalid")]
    InvalidDatasetId,
    /// Generation zero is reserved.
    #[error("manifest version must be nonzero")]
    InvalidManifestVersion,
    /// A Parquet object cannot be empty.
    #[error("manifest object must contain rows and bytes")]
    EmptyObject,
    /// Previous and requested dataset identities disagree.
    #[error("manifest dataset identity mismatch")]
    DatasetMismatch,
    /// Another object would exceed the configured generation object ceiling.
    #[error("manifest requires compaction before exceeding {max_objects} objects")]
    SmallFileCeiling { max_objects: usize },
    /// Row or byte accumulation overflowed.
    #[error("manifest row or byte count overflow")]
    CountOverflow,
    /// Compaction changed row count or semantic lineage.
    #[error("compaction changed row or lineage semantics")]
    CompactionSemanticMismatch,
}
