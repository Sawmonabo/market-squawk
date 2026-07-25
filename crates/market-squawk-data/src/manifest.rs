//! Immutable analytical generation plans and identities.

use std::cmp::Ordering;
use std::fmt;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::schema::{DatasetSchemaRef, DatasetSchemaRegistry};

mod catalog;

#[cfg(feature = "release-evidence")]
pub use self::catalog::benchmark_support::{
    ReleaseEvidenceStorageError, ReleaseEvidenceStorageResult, run_release_evidence_storage,
};
pub use self::catalog::{
    AnalyticalManifestCatalog, GenerationKind, MAX_RETAINED_PYTHON_DATASET_ADMISSIONS,
    MAX_RETAINED_PYTHON_DATASET_DESCRIPTOR_BYTES, ManifestCatalogError, PinnedDataset,
    PinnedManifestObject,
};
pub(crate) use self::catalog::{
    CatalogFeatureDataset, CatalogFeatureDatasetPage, CatalogFeatureDatasetSelection,
    CatalogGenerationPage,
};

/// Fixed maximum number of exact input generations retained by one derived generation.
pub const MAX_DERIVED_GENERATION_PARENTS: usize = 256;

/// Crate-sealed authority for committing derived lineage after output publication.
///
/// No production issuer exists until the research-use authority migration binds this capability
/// to transitive source permits and independent output-persistence authority.
#[derive(Debug)]
#[allow(
    dead_code,
    reason = "the immediately following ResearchUse authority wiring supplies the sole issuer"
)]
pub(crate) struct DerivedGenerationCommitAuthority {
    _private: (),
}

#[cfg(test)]
impl DerivedGenerationCommitAuthority {
    const fn for_test() -> Self {
        Self { _private: () }
    }
}

/// Stable local analytical dataset identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DatasetId(Box<str>);

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
            Ok(Self(value.into()))
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
    schema: DatasetSchemaRef,
    content_hash: Sha256Digest,
}

/// Nonzero canonical SHA-256 identity supplied by the complete dataset-build specification.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DatasetBuildSpecDigest(Sha256Digest);

impl DatasetBuildSpecDigest {
    /// Constructs a non-reserved build-specification identity.
    pub fn try_new(bytes: [u8; 32]) -> Result<Self, ManifestPlanError> {
        if bytes == [0; 32] {
            Err(ManifestPlanError::InvalidBuildSpecDigest)
        } else {
            Ok(Self(Sha256Digest::new(bytes)))
        }
    }

    /// Returns the exact SHA-256 identity.
    pub const fn digest(self) -> Sha256Digest {
        self.0
    }
}

impl fmt::Debug for DatasetBuildSpecDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DatasetBuildSpecDigest([SHA-256])")
    }
}

/// Typed semantic relationship from one generation to an exact prior generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum GenerationParentRelation {
    /// An ingest appends one object to its immediately preceding generation.
    AppendPredecessor,
    /// A compaction replaces its immediately preceding generation without changing semantics.
    CompactionPredecessor,
    /// A derived output consumes one explicitly named immutable input generation.
    DerivedInput,
}

impl GenerationParentRelation {
    pub(super) const fn database_name(self) -> &'static str {
        match self {
            Self::AppendPredecessor => "append_predecessor",
            Self::CompactionPredecessor => "compaction_predecessor",
            Self::DerivedInput => "derived_input",
        }
    }

    pub(super) fn from_database_name(value: &str) -> Option<Self> {
        match value {
            "append_predecessor" => Some(Self::AppendPredecessor),
            "compaction_predecessor" => Some(Self::CompactionPredecessor),
            "derived_input" => Some(Self::DerivedInput),
            _ => None,
        }
    }
}

/// One immutable, exact generation-parent edge in canonical child order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationParent {
    relation: GenerationParentRelation,
    manifest: DatasetManifestRef,
}

impl GenerationParent {
    pub(super) const fn new(
        relation: GenerationParentRelation,
        manifest: DatasetManifestRef,
    ) -> Self {
        Self { relation, manifest }
    }

    /// Returns why this exact parent contributes to the child generation.
    pub const fn relation(&self) -> GenerationParentRelation {
        self.relation
    }

    /// Returns the complete immutable parent identity.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
}

/// Nonempty, bounded, canonical and duplicate-free inputs to one derived generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedGenerationParents(Box<[DatasetManifestRef]>);

impl DerivedGenerationParents {
    /// Canonicalizes exact parent identities and rejects duplicates or coordinate conflicts.
    pub fn try_new(mut parents: Vec<DatasetManifestRef>) -> Result<Self, ManifestPlanError> {
        if parents.is_empty() || parents.len() > MAX_DERIVED_GENERATION_PARENTS {
            return Err(ManifestPlanError::InvalidDerivedParentCount);
        }
        parents.sort_unstable_by(compare_manifest_refs);
        for pair in parents.windows(2) {
            if pair[0].dataset_id == pair[1].dataset_id
                && pair[0].manifest_version == pair[1].manifest_version
            {
                return if pair[0] == pair[1] {
                    Err(ManifestPlanError::DuplicateDerivedParent)
                } else {
                    Err(ManifestPlanError::ConflictingDerivedParent)
                };
            }
        }
        Ok(Self(parents.into_boxed_slice()))
    }

    /// Returns parents in stable canonical identity order.
    pub fn as_slice(&self) -> &[DatasetManifestRef] {
        &self.0
    }
}

pub(crate) fn compare_manifest_refs(
    left: &DatasetManifestRef,
    right: &DatasetManifestRef,
) -> Ordering {
    left.dataset_id
        .as_str()
        .cmp(right.dataset_id.as_str())
        .then_with(|| left.manifest_version.cmp(&right.manifest_version))
        .then_with(|| left.schema.cmp(&right.schema))
        .then_with(|| left.content_hash.cmp(&right.content_hash))
}

impl DatasetManifestRef {
    /// Constructs a nonzero generation pin.
    pub fn try_new(
        dataset_id: DatasetId,
        manifest_version: u64,
        schema_version: market_squawk_domain::SchemaVersion,
        content_hash: Sha256Digest,
    ) -> Result<Self, ManifestPlanError> {
        let schema = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| ManifestPlanError::InvalidDatasetSchema)?;
        if schema.version() != schema_version {
            return Err(ManifestPlanError::InvalidDatasetSchema);
        }
        Self::try_new_with_schema(dataset_id, manifest_version, schema, content_hash)
    }

    /// Constructs a nonzero generation pin carrying one complete retained schema identity.
    ///
    /// Construction preserves an untrusted retained identity exactly. Catalog readers and query
    /// registration resolve it through [`DatasetSchemaRegistry`] before use.
    pub fn try_new_with_schema(
        dataset_id: DatasetId,
        manifest_version: u64,
        schema: DatasetSchemaRef,
        content_hash: Sha256Digest,
    ) -> Result<Self, ManifestPlanError> {
        if manifest_version == 0 {
            return Err(ManifestPlanError::InvalidManifestVersion);
        }
        Ok(Self {
            dataset_id,
            manifest_version,
            schema,
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

    /// Returns the exact analytical row-schema version for this immutable generation.
    pub const fn schema_version(&self) -> market_squawk_domain::SchemaVersion {
        self.schema.version()
    }

    /// Returns the exact registered dataset-schema identity for this immutable generation.
    pub const fn schema(&self) -> &DatasetSchemaRef {
        &self.schema
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
    objects: Box<[ManifestObject]>,
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
        let previous_objects = match previous {
            Some(previous) if previous.dataset_id == dataset_id => previous.objects.as_ref(),
            Some(_) => return Err(ManifestPlanError::DatasetMismatch),
            None => &[],
        };
        if previous_objects.len() >= max_objects {
            return Err(ManifestPlanError::SmallFileCeiling { max_objects });
        }
        let object_count = previous_objects
            .len()
            .checked_add(1)
            .ok_or(ManifestPlanError::CountOverflow)?;
        let mut objects = Vec::new();
        objects
            .try_reserve_exact(object_count)
            .map_err(|_| ManifestPlanError::CountOverflow)?;
        if objects.capacity() != object_count {
            return Err(ManifestPlanError::AllocationContract);
        }
        objects.extend_from_slice(previous_objects);
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

    /// Creates one complete derived generation without inheriting a prior physical object set.
    ///
    /// Physical objects are retained in deterministic content-identity order. Partition meaning
    /// remains part of the separately supplied dataset-build specification digest.
    pub fn derive(
        dataset_id: DatasetId,
        mut derived: Vec<ManifestObject>,
        max_objects: usize,
    ) -> Result<Self, ManifestPlanError> {
        if max_objects == 0 || derived.is_empty() || derived.len() > max_objects {
            return Err(ManifestPlanError::SmallFileCeiling { max_objects });
        }
        derived.sort_unstable_by(|left, right| {
            left.content_hash
                .cmp(&right.content_hash)
                .then_with(|| left.row_count.cmp(&right.row_count))
                .then_with(|| left.size_bytes.cmp(&right.size_bytes))
                .then_with(|| left.lineage_digest.cmp(&right.lineage_digest))
        });
        if derived
            .windows(2)
            .any(|pair| pair[0].content_hash == pair[1].content_hash)
        {
            return Err(ManifestPlanError::DuplicateDerivedObject);
        }
        Self::from_objects(dataset_id, derived)
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
        let allocation = objects.as_ptr();
        let objects = objects.into_boxed_slice();
        if objects.as_ptr() != allocation {
            return Err(ManifestPlanError::AllocationContract);
        }
        Self::from_exact_objects(dataset_id, objects)
    }

    pub(super) fn from_exact_objects(
        dataset_id: DatasetId,
        objects: Box<[ManifestObject]>,
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
    /// The compatibility constructor was asked to manufacture a noncanonical research identity.
    #[error("manifest dataset schema identity is invalid")]
    InvalidDatasetSchema,
    /// The build specification used the reserved all-zero identity.
    #[error("dataset build-specification digest is invalid")]
    InvalidBuildSpecDigest,
    /// A derived generation must name between one and 256 exact parents.
    #[error("derived generation parent count is invalid")]
    InvalidDerivedParentCount,
    /// One exact derived parent was supplied more than once.
    #[error("derived generation repeats an exact parent")]
    DuplicateDerivedParent,
    /// One dataset/version coordinate carried conflicting retained identities.
    #[error("derived generation parent identity conflicts")]
    ConflictingDerivedParent,
    /// A derived generation repeated one content-addressed physical object.
    #[error("derived generation repeats a physical object")]
    DuplicateDerivedObject,
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
    /// An exact-capacity immutable construction changed allocation identity.
    #[error("manifest immutable allocation contract changed")]
    AllocationContract,
}
