//! Bounded catalog evidence snapshot DTOs and semantic validation.

use market_squawk_domain::{SourceIdentifier, Timestamp};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{CatalogContentEvidenceDigest, EvidenceError, MAX_PARQUET_METADATA_BYTES};
use crate::manifest::{DatasetBuildSpecDigest, GenerationParent, GenerationParentRelation};
use crate::{
    DatasetId, DatasetManifestRef, DatasetSchemaRef, DatasetSchemaRegistry, GenerationKind,
    Sha256Digest,
};

const MAX_EVIDENCE_TOTAL_BYTES: u64 = 16 * 1024 * 1024 * 1024 * 1024;
const MAX_EVIDENCE_OBJECT_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
const MAX_PROVIDER_RELATION_KEY_BYTES: usize = 128;

/// Closed durable provider relations whose exact rows participate in catalog authority evidence.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ProviderCatalogRelation {
    SealedRawObject,
    LogicalPublicationBinding,
    LogicalPublicationRequiredFamily,
    LogicalPublicationObject,
    LogicalPublicationPartition,
    LogicalPublicationCanonicalExpectation,
    OptionMarketBinding,
    OptionMarketNativeLineage,
    OptionMarketBindingRow,
    MarketEventSelectionIndex,
    DirectProviderCaptureBinding,
    DirectProviderPublicationBinding,
    LogicalOriginal,
    LogicalOriginalObject,
    CaptureOriginal,
    LogicalPartitionArtifact,
    MarketEventStorageHead,
    MarketEventCommit,
    MarketEventActiveRow,
    MarketEventArchiveObject,
    MarketEventArchiveMembership,
    MarketEventArchiveProgress,
    NativeReferenceCapture,
    ForecastInventoryVintage,
    ForecastInventoryOutcome,
    ModelInventorySeries,
    ChartProjectionHeader,
    ChartProjectionRow,
    ModelInventoryRecord,
}

impl ProviderCatalogRelation {
    pub(crate) const ALL: [Self; 29] = [
        Self::SealedRawObject,
        Self::LogicalPublicationBinding,
        Self::LogicalPublicationRequiredFamily,
        Self::LogicalPublicationObject,
        Self::LogicalPublicationPartition,
        Self::LogicalPublicationCanonicalExpectation,
        Self::OptionMarketBinding,
        Self::OptionMarketNativeLineage,
        Self::OptionMarketBindingRow,
        Self::MarketEventSelectionIndex,
        Self::DirectProviderCaptureBinding,
        Self::DirectProviderPublicationBinding,
        Self::LogicalOriginal,
        Self::LogicalOriginalObject,
        Self::CaptureOriginal,
        Self::LogicalPartitionArtifact,
        Self::MarketEventStorageHead,
        Self::MarketEventCommit,
        Self::MarketEventActiveRow,
        Self::MarketEventArchiveObject,
        Self::MarketEventArchiveMembership,
        Self::MarketEventArchiveProgress,
        Self::NativeReferenceCapture,
        Self::ForecastInventoryVintage,
        Self::ForecastInventoryOutcome,
        Self::ModelInventorySeries,
        Self::ChartProjectionHeader,
        Self::ChartProjectionRow,
        Self::ModelInventoryRecord,
    ];
    pub(crate) const fn canonical_tag(self) -> u8 {
        match self {
            Self::SealedRawObject => 1,
            Self::LogicalPublicationBinding => 2,
            Self::LogicalPublicationRequiredFamily => 3,
            Self::LogicalPublicationObject => 4,
            Self::LogicalPublicationPartition => 5,
            Self::LogicalPublicationCanonicalExpectation => 6,
            Self::OptionMarketBinding => 7,
            Self::OptionMarketNativeLineage => 8,
            Self::OptionMarketBindingRow => 9,
            Self::MarketEventSelectionIndex => 10,
            Self::DirectProviderCaptureBinding => 11,
            Self::DirectProviderPublicationBinding => 12,
            Self::LogicalOriginal => 13,
            Self::LogicalOriginalObject => 14,
            Self::CaptureOriginal => 15,
            Self::LogicalPartitionArtifact => 16,
            Self::MarketEventStorageHead => 17,
            Self::MarketEventCommit => 18,
            Self::MarketEventActiveRow => 19,
            Self::MarketEventArchiveObject => 20,
            Self::MarketEventArchiveMembership => 21,
            Self::MarketEventArchiveProgress => 22,
            Self::NativeReferenceCapture => 23,
            Self::ForecastInventoryVintage => 24,
            Self::ForecastInventoryOutcome => 25,
            Self::ModelInventorySeries => 26,
            Self::ChartProjectionHeader => 27,
            Self::ChartProjectionRow => 28,
            Self::ModelInventoryRecord => 29,
        }
    }

    pub(crate) const fn database_name(self) -> &'static str {
        match self {
            Self::SealedRawObject => "sealed_raw_objects",
            Self::LogicalPublicationBinding => "provider_logical_publication_bindings",
            Self::LogicalPublicationRequiredFamily => {
                "provider_logical_publication_required_families"
            }
            Self::LogicalPublicationObject => "provider_logical_publication_objects",
            Self::LogicalPublicationPartition => "provider_logical_publication_partitions",
            Self::LogicalPublicationCanonicalExpectation => {
                "provider_logical_publication_canonical_expectations"
            }
            Self::OptionMarketBinding => "provider_option_market_bindings",
            Self::OptionMarketNativeLineage => "provider_option_market_binding_native_lineage",
            Self::OptionMarketBindingRow => "provider_option_market_binding_rows",
            Self::MarketEventSelectionIndex => "provider_market_event_selection_index",
            Self::DirectProviderCaptureBinding => "ingest_run_provider_capture_bindings",
            Self::DirectProviderPublicationBinding => "ingest_run_provider_publication_bindings",
            Self::LogicalOriginal => "provider_logical_originals",
            Self::LogicalOriginalObject => "provider_logical_original_objects",
            Self::CaptureOriginal => "provider_capture_originals",
            Self::LogicalPartitionArtifact => "ingest_run_provider_logical_partition_artifacts",
            Self::MarketEventStorageHead => "market_event_storage_heads",
            Self::MarketEventCommit => "market_event_commits",
            Self::MarketEventActiveRow => "market_event_active_rows",
            Self::MarketEventArchiveObject => "market_event_archive_objects",
            Self::MarketEventArchiveMembership => "market_event_archive_memberships",
            Self::MarketEventArchiveProgress => "market_event_archive_progress",
            Self::NativeReferenceCapture => "market_data_native_reference_captures",
            Self::ForecastInventoryVintage => "forecast_inventory_vintages",
            Self::ForecastInventoryOutcome => "forecast_inventory_outcomes",
            Self::ModelInventorySeries => "model_inventory_series",
            Self::ChartProjectionHeader => "chart_projection_headers",
            Self::ChartProjectionRow => "chart_projection_rows",
            Self::ModelInventoryRecord => "model_inventory_records",
        }
    }

    pub(crate) fn from_database_name(value: &str) -> Option<Self> {
        Some(match value {
            "sealed_raw_objects" => Self::SealedRawObject,
            "provider_logical_publication_bindings" => Self::LogicalPublicationBinding,
            "provider_logical_publication_required_families" => {
                Self::LogicalPublicationRequiredFamily
            }
            "provider_logical_publication_objects" => Self::LogicalPublicationObject,
            "provider_logical_publication_partitions" => Self::LogicalPublicationPartition,
            "provider_logical_publication_canonical_expectations" => {
                Self::LogicalPublicationCanonicalExpectation
            }
            "provider_option_market_bindings" => Self::OptionMarketBinding,
            "provider_option_market_binding_native_lineage" => Self::OptionMarketNativeLineage,
            "provider_option_market_binding_rows" => Self::OptionMarketBindingRow,
            "provider_market_event_selection_index" => Self::MarketEventSelectionIndex,
            "ingest_run_provider_capture_bindings" => Self::DirectProviderCaptureBinding,
            "ingest_run_provider_publication_bindings" => Self::DirectProviderPublicationBinding,
            "provider_logical_originals" => Self::LogicalOriginal,
            "provider_logical_original_objects" => Self::LogicalOriginalObject,
            "provider_capture_originals" => Self::CaptureOriginal,
            "ingest_run_provider_logical_partition_artifacts" => Self::LogicalPartitionArtifact,
            "market_event_storage_heads" => Self::MarketEventStorageHead,
            "market_event_commits" => Self::MarketEventCommit,
            "market_event_active_rows" => Self::MarketEventActiveRow,
            "market_event_archive_objects" => Self::MarketEventArchiveObject,
            "market_event_archive_memberships" => Self::MarketEventArchiveMembership,
            "market_event_archive_progress" => Self::MarketEventArchiveProgress,
            "market_data_native_reference_captures" => Self::NativeReferenceCapture,
            "forecast_inventory_vintages" => Self::ForecastInventoryVintage,
            "forecast_inventory_outcomes" => Self::ForecastInventoryOutcome,
            "model_inventory_series" => Self::ModelInventorySeries,
            "chart_projection_headers" => Self::ChartProjectionHeader,
            "chart_projection_rows" => Self::ChartProjectionRow,
            "model_inventory_records" => Self::ModelInventoryRecord,
            _ => return None,
        })
    }
}

/// One exact provider-catalog row reduced under a relation-specific, field-complete SHA-256 domain.
///
/// Large JSON and native sidecar values are hashed while each SQLite row is owned and are not
/// retained in the snapshot. The canonical primary key remains explicit so duplicate rows and
/// non-deterministic relation ordering fail closed independently of the row-content digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderCatalogRelationEvidenceRow {
    relation: ProviderCatalogRelation,
    primary_key: Box<[u8]>,
    row_content_digest: Sha256Digest,
}

impl ProviderCatalogRelationEvidenceRow {
    pub(crate) fn try_new(
        relation: ProviderCatalogRelation,
        primary_key: impl Into<Box<[u8]>>,
        row_content_digest: Sha256Digest,
        accounted_object_bytes: u64,
    ) -> Result<Self, EvidenceError> {
        let primary_key = primary_key.into();
        // Event dataset keys mirror the schema's 256-byte dataset bound; a commit key
        // appends one separator and the 19 decimal digits of a positive SQLite integer.
        let maximum_key_bytes = match relation {
            ProviderCatalogRelation::MarketEventStorageHead
            | ProviderCatalogRelation::MarketEventArchiveProgress => 256,
            ProviderCatalogRelation::MarketEventCommit => 256 + 1 + 19,
            _ => MAX_PROVIDER_RELATION_KEY_BYTES,
        };
        if primary_key.is_empty()
            || primary_key.len() > maximum_key_bytes
            || (relation == ProviderCatalogRelation::SealedRawObject)
                != (accounted_object_bytes > 0)
            || accounted_object_bytes > MAX_EVIDENCE_OBJECT_BYTES
        {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
        Ok(Self {
            relation,
            primary_key,
            row_content_digest,
        })
    }

    pub(crate) const fn relation(&self) -> ProviderCatalogRelation {
        self.relation
    }

    pub(crate) fn primary_key(&self) -> &[u8] {
        &self.primary_key
    }

    pub(crate) const fn row_content_digest(&self) -> Sha256Digest {
        self.row_content_digest
    }
}

/// Explicit byte and per-object Parquet metadata budgets; row cursors retain one row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EvidenceLimits {
    max_total_bytes: u64,
    max_object_bytes: u64,
    max_parquet_metadata_bytes: u64,
}

impl EvidenceLimits {
    pub(crate) fn try_new(
        max_total_bytes: u64,
        max_object_bytes: u64,
        max_parquet_metadata_bytes: u64,
    ) -> Result<Self, EvidenceError> {
        if max_total_bytes == 0
            || max_total_bytes > MAX_EVIDENCE_TOTAL_BYTES
            || max_object_bytes == 0
            || max_object_bytes > MAX_EVIDENCE_OBJECT_BYTES
            || max_object_bytes > max_total_bytes
            || !(8..=MAX_PARQUET_METADATA_BYTES).contains(&max_parquet_metadata_bytes)
        {
            return Err(EvidenceError::InvalidLimits);
        }
        Ok(Self {
            max_total_bytes,
            max_object_bytes,
            max_parquet_metadata_bytes,
        })
    }

    pub(crate) const fn max_total_bytes(self) -> u64 {
        self.max_total_bytes
    }

    pub(crate) const fn max_object_bytes(self) -> u64 {
        self.max_object_bytes
    }

    pub(crate) const fn max_parquet_metadata_bytes(self) -> u64 {
        self.max_parquet_metadata_bytes
    }
}

/// One consistent catalog snapshot request and its stored expiry cutoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EvidenceSnapshotRequest {
    cutoff: Timestamp,
    limits: EvidenceLimits,
}

impl EvidenceSnapshotRequest {
    pub(crate) const fn new(cutoff: Timestamp, limits: EvidenceLimits) -> Self {
        Self { cutoff, limits }
    }

    pub(crate) const fn cutoff(self) -> Timestamp {
        self.cutoff
    }

    pub(crate) const fn limits(self) -> EvidenceLimits {
        self.limits
    }
}

/// Exact physical object retained by the primary artifact registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactEvidenceRow {
    artifact_id: Uuid,
    run_id: Uuid,
    publication_ordinal: u16,
    relative_reference: Box<str>,
    content_hash: Sha256Digest,
    size_bytes: u64,
}

impl ArtifactEvidenceRow {
    pub(crate) fn try_new(
        artifact_id: Uuid,
        run_id: Uuid,
        publication_ordinal: u16,
        relative_reference: impl Into<Box<str>>,
        content_hash: Sha256Digest,
        size_bytes: u64,
    ) -> Result<Self, EvidenceError> {
        let relative_reference = relative_reference.into();
        if artifact_id.is_nil()
            || run_id.is_nil()
            || publication_ordinal > 1023
            || size_bytes == 0
            || size_bytes > MAX_EVIDENCE_OBJECT_BYTES
            || !canonical_object_reference(&relative_reference, content_hash)
        {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
        Ok(Self {
            artifact_id,
            run_id,
            publication_ordinal,
            relative_reference,
            content_hash,
            size_bytes,
        })
    }

    pub(crate) const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    pub(crate) const fn run_id(&self) -> Uuid {
        self.run_id
    }

    pub(crate) const fn publication_ordinal(&self) -> u16 {
        self.publication_ordinal
    }

    pub(crate) fn relative_reference(&self) -> &str {
        &self.relative_reference
    }

    pub(crate) const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    pub(crate) const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

/// Dataset-manifest anchor needed to validate every analytical generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestEvidenceRow {
    manifest_id: Uuid,
    dataset_id: DatasetId,
    schema_version: u32,
    artifact_id: Uuid,
    content_hash: Sha256Digest,
}

impl ManifestEvidenceRow {
    pub(crate) fn try_new(
        manifest_id: Uuid,
        dataset_id: DatasetId,
        schema_version: u32,
        artifact_id: Uuid,
        content_hash: Sha256Digest,
    ) -> Result<Self, EvidenceError> {
        if manifest_id.is_nil() || artifact_id.is_nil() || schema_version == 0 {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
        Ok(Self {
            manifest_id,
            dataset_id,
            schema_version,
            artifact_id,
            content_hash,
        })
    }

    pub(crate) const fn manifest_id(&self) -> Uuid {
        self.manifest_id
    }

    pub(crate) const fn dataset_id(&self) -> &DatasetId {
        &self.dataset_id
    }

    pub(crate) const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub(crate) const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    pub(crate) const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }
}

/// One ordered object member of an immutable historical generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GenerationObjectEvidenceRow {
    artifact_id: Uuid,
    content_hash: Sha256Digest,
    row_count: u64,
    size_bytes: u64,
    lineage_hash: Sha256Digest,
}

impl GenerationObjectEvidenceRow {
    pub(crate) fn try_new(
        artifact_id: Uuid,
        content_hash: Sha256Digest,
        row_count: u64,
        size_bytes: u64,
        lineage_hash: Sha256Digest,
    ) -> Result<Self, EvidenceError> {
        if artifact_id.is_nil()
            || row_count == 0
            || size_bytes == 0
            || size_bytes > MAX_EVIDENCE_OBJECT_BYTES
        {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
        Ok(Self {
            artifact_id,
            content_hash,
            row_count,
            size_bytes,
            lineage_hash,
        })
    }

    pub(crate) const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    pub(crate) const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    pub(crate) const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(crate) const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub(crate) const fn lineage_hash(&self) -> Sha256Digest {
        self.lineage_hash
    }
}

/// One exact, ordered, relationship-bearing generation parent in backup evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GenerationParentEvidenceRow {
    generation_sequence: u64,
    parent: GenerationParent,
}

impl GenerationParentEvidenceRow {
    pub(crate) fn try_new(
        generation_sequence: u64,
        relation: GenerationParentRelation,
        manifest: DatasetManifestRef,
    ) -> Result<Self, EvidenceError> {
        if generation_sequence == 0 {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
        DatasetSchemaRegistry::local()
            .resolve(manifest.schema())
            .map_err(|_| EvidenceError::InvalidCatalogEvidence)?;
        Ok(Self {
            generation_sequence,
            parent: GenerationParent::new(relation, manifest),
        })
    }

    pub(crate) const fn generation_sequence(&self) -> u64 {
        self.generation_sequence
    }

    pub(crate) const fn parent(&self) -> &GenerationParent {
        &self.parent
    }
}

/// Published query result that is reachable at the stored snapshot cutoff.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QueryArtifactEvidenceRow {
    reservation_id: Uuid,
    owner: SourceIdentifier,
    request_hash: Sha256Digest,
    artifact_id: Uuid,
    relative_reference: Box<str>,
    content_hash: Sha256Digest,
    size_bytes: u64,
    expires_at: Timestamp,
}

impl QueryArtifactEvidenceRow {
    #[allow(
        clippy::too_many_arguments,
        reason = "the evidence row binds each persisted ownership and physical-object field"
    )]
    pub(crate) fn try_new(
        reservation_id: Uuid,
        owner: SourceIdentifier,
        request_hash: Sha256Digest,
        artifact_id: Uuid,
        relative_reference: impl Into<Box<str>>,
        content_hash: Sha256Digest,
        size_bytes: u64,
        expires_at: Timestamp,
    ) -> Result<Self, EvidenceError> {
        let relative_reference = relative_reference.into();
        if reservation_id.is_nil()
            || artifact_id.is_nil()
            || size_bytes == 0
            || size_bytes > MAX_EVIDENCE_OBJECT_BYTES
            || !canonical_object_reference(&relative_reference, content_hash)
        {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
        Ok(Self {
            reservation_id,
            owner,
            request_hash,
            artifact_id,
            relative_reference,
            content_hash,
            size_bytes,
            expires_at,
        })
    }

    pub(crate) const fn reservation_id(&self) -> Uuid {
        self.reservation_id
    }

    pub(crate) const fn owner(&self) -> &SourceIdentifier {
        &self.owner
    }

    pub(crate) const fn request_hash(&self) -> Sha256Digest {
        self.request_hash
    }

    pub(crate) const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    pub(crate) fn relative_reference(&self) -> &str {
        &self.relative_reference
    }

    pub(crate) const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    pub(crate) const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub(crate) const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// A retained market-event archive object, independent of ingest artifacts and manifests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketEventArchiveEvidenceRow {
    relative_reference: Box<str>,
    content_hash: Sha256Digest,
    schema: DatasetSchemaRef,
    size_bytes: u64,
    row_count: u64,
    created_at: Timestamp,
    published_at: Timestamp,
}

impl MarketEventArchiveEvidenceRow {
    #[allow(
        clippy::too_many_arguments,
        reason = "the row binds every durable archive object column"
    )]
    pub(crate) fn try_new(
        relative_reference: impl Into<Box<str>>,
        content_hash: Sha256Digest,
        schema: DatasetSchemaRef,
        size_bytes: u64,
        row_count: u64,
        created_at: Timestamp,
        published_at: Timestamp,
    ) -> Result<Self, EvidenceError> {
        let relative_reference = relative_reference.into();
        if size_bytes == 0
            || size_bytes > MAX_EVIDENCE_OBJECT_BYTES
            || row_count == 0
            || published_at < created_at
            || !canonical_object_reference(&relative_reference, content_hash)
            || DatasetSchemaRegistry::local()
                .canonical_market_events()
                .map_err(|_| EvidenceError::InvalidCatalogEvidence)?
                != schema
        {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
        Ok(Self {
            relative_reference,
            content_hash,
            schema,
            size_bytes,
            row_count,
            created_at,
            published_at,
        })
    }
    pub(crate) fn relative_reference(&self) -> &str {
        &self.relative_reference
    }
    pub(crate) const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }
    pub(crate) const fn schema(&self) -> &DatasetSchemaRef {
        &self.schema
    }
    pub(crate) const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
    pub(crate) const fn row_count(&self) -> u64 {
        self.row_count
    }
    pub(crate) const fn created_at(&self) -> Timestamp {
        self.created_at
    }
    pub(crate) const fn published_at(&self) -> Timestamp {
        self.published_at
    }
}

/// Exact compact summary of one fully validated SQLite read transaction.
#[derive(Clone, Debug)]
pub(crate) struct CatalogEvidenceSnapshot {
    request: EvidenceSnapshotRequest,
    physical_artifact_count: u64,
    physical_artifact_bytes: u64,
    reference_count: u64,
    total_bytes: u64,
    digest: CatalogContentEvidenceDigest,
}
impl CatalogEvidenceSnapshot {
    pub(crate) fn new(
        request: EvidenceSnapshotRequest,
        physical_artifact_count: u64,
        physical_artifact_bytes: u64,
        reference_count: u64,
        total_bytes: u64,
        digest: CatalogContentEvidenceDigest,
    ) -> Self {
        Self {
            request,
            physical_artifact_count,
            physical_artifact_bytes,
            reference_count,
            total_bytes,
            digest,
        }
    }
    pub(crate) const fn request(&self) -> EvidenceSnapshotRequest {
        self.request
    }
    pub(crate) const fn physical_artifact_count(&self) -> u64 {
        self.physical_artifact_count
    }
    pub(crate) const fn physical_artifact_bytes(&self) -> u64 {
        self.physical_artifact_bytes
    }
    pub(crate) const fn reference_count(&self) -> u64 {
        self.reference_count
    }
    pub(crate) const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
    pub(crate) fn evidence_digest(&self) -> Result<CatalogContentEvidenceDigest, EvidenceError> {
        Ok(self.digest)
    }
    pub(crate) fn check_cancellation(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), EvidenceError> {
        if cancellation.is_cancelled() {
            Err(EvidenceError::Cancelled)
        } else {
            Ok(())
        }
    }
}

fn canonical_object_reference(reference: &str, digest: Sha256Digest) -> bool {
    let Some(relative) = reference
        .strip_prefix("objects/sha256/")
        .and_then(|value| value.split_once('/'))
    else {
        return false;
    };
    let (shard, filename) = relative;
    let Some(encoded) = filename.strip_suffix(".parquet") else {
        return false;
    };
    if shard.len() != 2
        || encoded.len() != 64
        || encoded.contains(|character: char| {
            !character.is_ascii_hexdigit() || character.is_ascii_uppercase()
        })
    {
        return false;
    }
    let expected = digest.bytes();
    let encoded = encoded.as_bytes();
    let shard = shard.as_bytes();
    shard[0] == hex_digit(expected[0] >> 4)
        && shard[1] == hex_digit(expected[0] & 0x0f)
        && expected.iter().enumerate().all(|(index, byte)| {
            encoded[index * 2] == hex_digit(byte >> 4)
                && encoded[index * 2 + 1] == hex_digit(byte & 0x0f)
        })
}

const fn hex_digit(nibble: u8) -> u8 {
    if nibble < 10 {
        b'0' + nibble
    } else if nibble < 16 {
        b'a' + (nibble - 10)
    } else {
        u8::MAX
    }
}

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub(crate) struct GenerationEvidenceHeader {
    pub(crate) generation_sequence: u64,
    pub(crate) dataset_id: DatasetId,
    pub(crate) manifest_version: u64,
    pub(crate) content_hash: Sha256Digest,
    pub(crate) lineage_hash: Sha256Digest,
    pub(crate) row_count: u64,
    pub(crate) total_bytes: u64,
    pub(crate) schema: DatasetSchemaRef,
    pub(crate) anchor_manifest_id: Uuid,
    pub(crate) kind: GenerationKind,
    pub(crate) build_spec_digest: Option<DatasetBuildSpecDigest>,
}
impl GenerationEvidenceHeader {
    pub(crate) const fn generation_sequence(&self) -> u64 {
        self.generation_sequence
    }

    pub(crate) const fn dataset_id(&self) -> &DatasetId {
        &self.dataset_id
    }

    pub(crate) const fn manifest_version(&self) -> u64 {
        self.manifest_version
    }

    pub(crate) const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    pub(crate) const fn lineage_hash(&self) -> Sha256Digest {
        self.lineage_hash
    }

    pub(crate) const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(crate) const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub(crate) const fn schema(&self) -> &DatasetSchemaRef {
        &self.schema
    }

    pub(crate) const fn anchor_manifest_id(&self) -> Uuid {
        self.anchor_manifest_id
    }

    pub(crate) const fn kind(&self) -> GenerationKind {
        self.kind
    }

    pub(crate) const fn build_spec_digest(&self) -> Option<DatasetBuildSpecDigest> {
        self.build_spec_digest
    }
}
