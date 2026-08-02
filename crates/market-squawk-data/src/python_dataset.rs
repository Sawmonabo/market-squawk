//! Read-only catalog verification and immutable selection receipts for Python research.

#[path = "python_dataset/descriptor.rs"]
mod descriptor;
#[path = "python_dataset/verify.rs"]
mod verify;

use std::time::Instant;

use market_squawk_domain::{Currency, InstrumentId, SourceIdentifier, Timestamp};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    ArrowConversionError, CatalogEndpointIdentity, CatalogError, DatasetBuildSpecDigest,
    DatasetManifestRef, DatasetSplitCounts, Sha256Digest, UniverseId,
};

const MAX_PYTHON_DATASET_ROWS: usize = 100_000;
const MAX_PYTHON_DATASET_BYTES: usize = 256 * 1024 * 1024;

/// Durable Python-dataset registration or read-only verification failure.
#[derive(Debug, Error)]
pub enum PythonDatasetCatalogError {
    /// The requested export is absent from the selected catalog.
    #[error("Python dataset admission is unknown")]
    UnknownAdmission,
    /// Catalog, descriptor, generation, object, or selected-row identities disagree.
    #[error("Python dataset admission evidence is corrupt")]
    CorruptAdmission,
    /// A caller-selected count, byte, or elapsed-time bound was exceeded.
    #[error("Python dataset verification limit was exceeded")]
    LimitExceeded,
    /// The caller cancelled verification.
    #[error("Python dataset verification was cancelled")]
    Cancelled,
    /// The caller-selected monotonic deadline elapsed.
    #[error("Python dataset verification deadline elapsed")]
    DeadlineExceeded,
    /// Local path authority rejected the configured catalog or artifact root.
    #[error("Python dataset local path authority failed: {0}")]
    Path(#[from] market_squawk_platform::PathError),
    /// A controlled artifact reference or open failed.
    #[error("Python dataset artifact authority failed: {0}")]
    Artifact(#[from] market_squawk_platform::ArtifactPathError),
    /// The hardened catalog rejected the operation.
    #[error("Python dataset catalog operation failed: {0}")]
    Catalog(#[from] CatalogError),
    /// SQLite rejected the bounded transaction or query.
    #[error("Python dataset SQLite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// A controlled object read failed.
    #[error("Python dataset object read failed: {0}")]
    Io(#[from] std::io::Error),
    /// Parquet metadata or decoding failed.
    #[error("Python dataset Parquet decoding failed: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Registered Arrow validation failed.
    #[error("Python dataset Arrow validation failed: {0}")]
    Arrow(#[from] ArrowConversionError),
    /// Arrow decoding failed before registered-schema validation.
    #[error("Python dataset Arrow decoding failed: {0}")]
    ArrowDecode(#[from] arrow::error::ArrowError),
}

/// Explicit aggregate resource limits for one native dataset verification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PythonDatasetVerificationLimits {
    max_rows: usize,
    max_bytes: usize,
}

impl PythonDatasetVerificationLimits {
    /// Constructs bounded selected-row and aggregate-memory limits.
    pub fn try_new(max_rows: usize, max_bytes: usize) -> Result<Self, PythonDatasetCatalogError> {
        if max_rows == 0
            || max_rows > MAX_PYTHON_DATASET_ROWS
            || max_bytes == 0
            || max_bytes > MAX_PYTHON_DATASET_BYTES
        {
            return Err(PythonDatasetCatalogError::LimitExceeded);
        }
        Ok(Self {
            max_rows,
            max_bytes,
        })
    }

    pub(crate) const fn max_rows(self) -> usize {
        self.max_rows
    }

    pub(crate) const fn max_bytes(self) -> usize {
        self.max_bytes
    }
}

/// Exact value variant retained by one canonical feature/label row.
#[derive(Clone, Debug, PartialEq)]
pub enum PythonDatasetValue {
    /// Finite statistical floating-point value represented by exact IEEE bits.
    Float(f64),
    /// Exact decimal mantissa and nonnegative scale.
    Decimal { mantissa: i128, scale: u8 },
    /// Explicit bounded missing-value reason.
    Missing(Box<str>),
}

/// One canonical selected row accepted for opaque-receipt revalidation.
#[derive(Clone, Debug, PartialEq)]
pub struct PythonDatasetRow {
    example_id: Box<str>,
    instrument_id: [u8; 16],
    cutoff_at: Timestamp,
    split: u8,
    component_kind: u8,
    component_name: Box<str>,
    component_version: u32,
    value: PythonDatasetValue,
    unit: Option<Box<str>>,
    currency: Option<Box<str>>,
    lineage: [u8; 32],
}

impl PythonDatasetRow {
    /// Constructs one exact row after applying the same closed grammar as Task 11 publication.
    #[allow(
        clippy::too_many_arguments,
        reason = "each typed feature/label column remains an independently checked identity"
    )]
    pub fn try_new(
        example_id: &str,
        instrument_id: [u8; 16],
        cutoff_at: Timestamp,
        split: u8,
        component_kind: u8,
        component_name: &str,
        component_version: u32,
        value: PythonDatasetValue,
        unit: Option<&str>,
        currency: Option<&str>,
        lineage: [u8; 32],
    ) -> Result<Self, PythonDatasetCatalogError> {
        let instrument = Uuid::from_bytes(instrument_id);
        if !canonical_identifier(example_id, 256)
            || InstrumentId::try_from(instrument).is_err()
            || !matches!(split, 1..=3)
            || !matches!(component_kind, 1..=2)
            || !canonical_identifier(component_name, 256)
            || component_version == 0
            || lineage == [0; 32]
            || !unit.is_none_or(canonical_unit)
            || !currency.is_none_or(|value| {
                Currency::try_from(value).is_ok_and(|parsed| parsed.as_str() == value)
            })
            || !valid_value(&value)
            || (matches!(&value, PythonDatasetValue::Missing(_))
                && (unit.is_some() || currency.is_some()))
        {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        Ok(Self {
            example_id: example_id.into(),
            instrument_id,
            cutoff_at,
            split,
            component_kind,
            component_name: component_name.into(),
            component_version,
            value,
            unit: unit.map(Into::into),
            currency: currency.map(Into::into),
            lineage,
        })
    }
}

/// Native catalog/object proof for one exact point-in-time row selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PythonDatasetIdentity {
    manifest: DatasetManifestRef,
    build_spec_digest: DatasetBuildSpecDigest,
    universe_digest: Sha256Digest,
    policy_digest: Sha256Digest,
    universe_id: UniverseId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PythonFeatureDatasetSummary {
    pub(crate) identity: PythonDatasetIdentity,
    pub(crate) split_counts: DatasetSplitCounts,
}

pub(crate) fn feature_dataset_summary(
    descriptor_bytes: &[u8],
    export_sha256: Sha256Digest,
) -> Result<PythonFeatureDatasetSummary, PythonDatasetCatalogError> {
    if descriptor_bytes.is_empty()
        || descriptor_bytes.len() > crate::MAX_FEATURE_LABEL_EXPORT_BYTES
        || Sha256Digest::new(Sha256::digest(descriptor_bytes).into()) != export_sha256
    {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    let descriptor = descriptor::Descriptor::parse(descriptor_bytes)?;
    let identity = descriptor.identity()?;
    let split_counts = DatasetSplitCounts::from_parts(
        descriptor.split_counts.train,
        descriptor.split_counts.validation,
        descriptor.split_counts.test,
    );
    Ok(PythonFeatureDatasetSummary {
        identity,
        split_counts,
    })
}

impl PythonDatasetIdentity {
    /// Returns the exact registered feature/label generation.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the producer-owned complete build identity.
    pub const fn build_spec_digest(&self) -> DatasetBuildSpecDigest {
        self.build_spec_digest
    }

    /// Returns the historical-universe content identity.
    pub const fn universe_digest(&self) -> Sha256Digest {
        self.universe_digest
    }

    /// Returns the point-in-time and transformation-policy identity.
    pub const fn policy_digest(&self) -> Sha256Digest {
        self.policy_digest
    }

    /// Returns the human-stable historical-universe identity.
    pub const fn universe_id(&self) -> &UniverseId {
        &self.universe_id
    }
}

/// Native catalog/object proof for one exact point-in-time row selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PythonDatasetSelection {
    local_root: std::path::PathBuf,
    identity: PythonDatasetIdentity,
    catalog_identity: CatalogEndpointIdentity,
    export_sha256: Sha256Digest,
    descriptor: Box<[u8]>,
    selection_sha256: Sha256Digest,
    selected_rows: usize,
    as_of: Timestamp,
}

impl PythonDatasetSelection {
    /// Returns the canonical local root derived by retained platform path authority.
    pub fn local_root(&self) -> &std::path::Path {
        &self.local_root
    }

    /// Returns the exact producer-owned generation and build identities.
    pub const fn identity(&self) -> &PythonDatasetIdentity {
        &self.identity
    }

    /// Returns the exact catalog endpoint selected by operator configuration.
    pub const fn catalog_identity(&self) -> CatalogEndpointIdentity {
        self.catalog_identity
    }

    /// Returns the producer-registered descriptor identity.
    pub const fn export_sha256(&self) -> Sha256Digest {
        self.export_sha256
    }

    /// Returns the exact producer-registered descriptor bytes.
    pub fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }

    /// Returns the independently derived canonical selected-row identity.
    pub const fn selection_sha256(&self) -> Sha256Digest {
        self.selection_sha256
    }

    /// Returns the exact selected component-row count.
    pub const fn selected_rows(&self) -> usize {
        self.selected_rows
    }

    /// Returns the exact point-in-time cutoff bound into the selection digest.
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Starts a streaming rehash against this immutable receipt.
    pub fn revalidation(&self) -> PythonDatasetSelectionRevalidation {
        PythonDatasetSelectionRevalidation {
            hash: selection_hash_prefix(self.catalog_identity, self.export_sha256, self.as_of),
            expected: self.selection_sha256,
            expected_rows: self.selected_rows,
            rows: 0,
        }
    }
}

/// Streaming selected-row revalidation used immediately before training and export.
#[derive(Clone, Debug)]
pub struct PythonDatasetSelectionRevalidation {
    hash: Sha256,
    expected: Sha256Digest,
    expected_rows: usize,
    rows: usize,
}

impl PythonDatasetSelectionRevalidation {
    /// Adds one canonical row in retained object/batch/row order.
    pub fn update(&mut self, row: &PythonDatasetRow) -> Result<(), PythonDatasetCatalogError> {
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or(PythonDatasetCatalogError::LimitExceeded)?;
        if self.rows > self.expected_rows {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        self.hash.update(row_digest(row));
        Ok(())
    }

    /// Proves exact row count and canonical identity equality.
    pub fn finish(mut self) -> Result<(), PythonDatasetCatalogError> {
        if self.rows != self.expected_rows {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        self.hash.update(
            u64::try_from(self.rows)
                .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?
                .to_be_bytes(),
        );
        if Sha256Digest::new(self.hash.finalize().into()) != self.expected {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        Ok(())
    }
}

/// Resolves and verifies one registered Task 11 export from an operator-selected local root.
pub fn verify_python_dataset(
    local_root: impl AsRef<std::path::Path>,
    export_sha256: Sha256Digest,
    as_of: Timestamp,
    limits: PythonDatasetVerificationLimits,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<PythonDatasetSelection, PythonDatasetCatalogError> {
    verify::verify(
        local_root.as_ref(),
        export_sha256,
        as_of,
        limits,
        deadline,
        cancellation,
    )
}

pub(crate) fn check_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), PythonDatasetCatalogError> {
    if cancellation.is_cancelled() {
        Err(PythonDatasetCatalogError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(PythonDatasetCatalogError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

pub(crate) fn new_selection_hasher(
    catalog_identity: CatalogEndpointIdentity,
    export_sha256: Sha256Digest,
    as_of: Timestamp,
) -> Sha256 {
    selection_hash_prefix(catalog_identity, export_sha256, as_of)
}

pub(crate) fn finish_selection_hash(
    mut hash: Sha256,
    selected_rows: usize,
) -> Result<Sha256Digest, PythonDatasetCatalogError> {
    hash.update(
        u64::try_from(selected_rows)
            .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?
            .to_be_bytes(),
    );
    Ok(Sha256Digest::new(hash.finalize().into()))
}

pub(crate) fn update_selection_hash(hash: &mut Sha256, row: &PythonDatasetRow) {
    hash.update(row_digest(row));
}

fn selection_hash_prefix(
    catalog_identity: CatalogEndpointIdentity,
    export_sha256: Sha256Digest,
    as_of: Timestamp,
) -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/python-dataset-selection/v1");
    hash.update(catalog_identity.bytes());
    hash.update(export_sha256.bytes());
    hash.update(as_of.unix_nanos().to_be_bytes());
    hash
}

fn row_digest(row: &PythonDatasetRow) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/python-dataset-row/v1");
    update_bytes(&mut hash, row.example_id.as_bytes());
    hash.update(row.instrument_id);
    hash.update(row.cutoff_at.unix_nanos().to_be_bytes());
    hash.update([row.split, row.component_kind]);
    update_bytes(&mut hash, row.component_name.as_bytes());
    hash.update(row.component_version.to_be_bytes());
    match &row.value {
        PythonDatasetValue::Float(value) => {
            hash.update([1]);
            hash.update(value.to_bits().to_be_bytes());
        }
        PythonDatasetValue::Decimal { mantissa, scale } => {
            hash.update([2]);
            hash.update(mantissa.to_be_bytes());
            hash.update([*scale]);
        }
        PythonDatasetValue::Missing(reason) => {
            hash.update([3]);
            update_bytes(&mut hash, reason.as_bytes());
        }
    }
    update_optional(&mut hash, row.unit.as_deref());
    update_optional(&mut hash, row.currency.as_deref());
    hash.update(row.lineage);
    hash.finalize().into()
}

fn update_optional(hash: &mut Sha256, value: Option<&str>) {
    if let Some(value) = value {
        hash.update([1]);
        update_bytes(hash, value.as_bytes());
    } else {
        hash.update([0]);
    }
}

fn update_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(value);
}

fn valid_value(value: &PythonDatasetValue) -> bool {
    match value {
        PythonDatasetValue::Float(value) => value.is_finite(),
        PythonDatasetValue::Decimal { scale, .. } => *scale <= 28,
        PythonDatasetValue::Missing(reason) => canonical_identifier(reason, 256),
    }
}

fn canonical_identifier(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

fn canonical_unit(value: &str) -> bool {
    SourceIdentifier::try_from(value).is_ok()
        && value.len() <= 32
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/' | b'%')
        })
}
