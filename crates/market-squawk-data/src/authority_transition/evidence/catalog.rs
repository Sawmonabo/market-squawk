//! Bounded catalog evidence snapshot DTOs and semantic validation.

use std::collections::{BTreeMap, BTreeSet};

use market_squawk_domain::{SourceIdentifier, Timestamp};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{CatalogContentEvidenceDigest, EvidenceError, MAX_PARQUET_METADATA_BYTES};
use crate::manifest::{
    DatasetBuildSpecDigest, GenerationParent, GenerationParentRelation,
    MAX_DERIVED_GENERATION_PARENTS, compare_manifest_refs,
};
use crate::{
    DatasetId, DatasetManifestRef, DatasetSchemaRef, DatasetSchemaRegistry, GenerationKind,
    ManifestObject, ManifestPlan, Sha256Digest,
};

const MAX_EVIDENCE_ARTIFACTS: usize = 100_000;
const MAX_EVIDENCE_REFERENCES: usize = 400_000;
const MAX_EVIDENCE_TOTAL_BYTES: u64 = 16 * 1024 * 1024 * 1024 * 1024;
const MAX_EVIDENCE_OBJECT_BYTES: u64 = 1024 * 1024 * 1024 * 1024;

/// Caller-selected resource bounds, capped by fixed process ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EvidenceLimits {
    max_artifacts: usize,
    max_references: usize,
    max_total_bytes: u64,
    max_object_bytes: u64,
    max_parquet_metadata_bytes: u64,
}

impl EvidenceLimits {
    pub(crate) fn try_new(
        max_artifacts: usize,
        max_references: usize,
        max_total_bytes: u64,
        max_object_bytes: u64,
        max_parquet_metadata_bytes: u64,
    ) -> Result<Self, EvidenceError> {
        if max_artifacts == 0
            || max_artifacts > MAX_EVIDENCE_ARTIFACTS
            || max_references == 0
            || max_references > MAX_EVIDENCE_REFERENCES
            || max_references < max_artifacts
            || max_total_bytes == 0
            || max_total_bytes > MAX_EVIDENCE_TOTAL_BYTES
            || max_object_bytes == 0
            || max_object_bytes > MAX_EVIDENCE_OBJECT_BYTES
            || max_object_bytes > max_total_bytes
            || !(8..=MAX_PARQUET_METADATA_BYTES).contains(&max_parquet_metadata_bytes)
        {
            return Err(EvidenceError::InvalidLimits);
        }
        Ok(Self {
            max_artifacts,
            max_references,
            max_total_bytes,
            max_object_bytes,
            max_parquet_metadata_bytes,
        })
    }

    pub(crate) const fn max_artifacts(self) -> usize {
        self.max_artifacts
    }

    pub(crate) const fn max_references(self) -> usize {
        self.max_references
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
    relative_reference: Box<str>,
    content_hash: Sha256Digest,
    size_bytes: u64,
}

impl ArtifactEvidenceRow {
    pub(crate) fn try_new(
        artifact_id: Uuid,
        run_id: Uuid,
        relative_reference: impl Into<Box<str>>,
        content_hash: Sha256Digest,
        size_bytes: u64,
    ) -> Result<Self, EvidenceError> {
        let relative_reference = relative_reference.into();
        if artifact_id.is_nil()
            || run_id.is_nil()
            || size_bytes == 0
            || size_bytes > MAX_EVIDENCE_OBJECT_BYTES
            || !canonical_object_reference(&relative_reference, content_hash)
        {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
        Ok(Self {
            artifact_id,
            run_id,
            relative_reference,
            content_hash,
            size_bytes,
        })
    }

    pub(crate) const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    pub(super) const fn run_id(&self) -> Uuid {
        self.run_id
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

    pub(super) const fn manifest_id(&self) -> Uuid {
        self.manifest_id
    }

    pub(super) const fn dataset_id(&self) -> &DatasetId {
        &self.dataset_id
    }

    pub(super) const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub(super) const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    pub(super) const fn content_hash(&self) -> Sha256Digest {
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

    fn manifest_object(&self) -> Result<ManifestObject, EvidenceError> {
        ManifestObject::try_new(
            self.content_hash,
            self.row_count,
            self.size_bytes,
            self.lineage_hash,
        )
        .map_err(|_| EvidenceError::GenerationSemanticMismatch)
    }

    pub(super) const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    pub(super) const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    pub(super) const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(super) const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub(super) const fn lineage_hash(&self) -> Sha256Digest {
        self.lineage_hash
    }
}

/// Complete immutable historical generation in catalog order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GenerationEvidenceRow {
    generation_sequence: u64,
    dataset_id: DatasetId,
    manifest_version: u64,
    content_hash: Sha256Digest,
    lineage_hash: Sha256Digest,
    row_count: u64,
    total_bytes: u64,
    schema: DatasetSchemaRef,
    anchor_manifest_id: Uuid,
    kind: GenerationKind,
    build_spec_digest: Option<DatasetBuildSpecDigest>,
    parents: Vec<GenerationParentEvidenceRow>,
    objects: Vec<GenerationObjectEvidenceRow>,
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

    pub(super) const fn generation_sequence(&self) -> u64 {
        self.generation_sequence
    }

    pub(super) const fn parent(&self) -> &GenerationParent {
        &self.parent
    }
}

impl GenerationEvidenceRow {
    #[allow(
        clippy::too_many_arguments,
        reason = "the row mirrors independently durable analytical generation columns"
    )]
    pub(crate) fn try_new(
        generation_sequence: u64,
        dataset_id: DatasetId,
        manifest_version: u64,
        content_hash: Sha256Digest,
        lineage_hash: Sha256Digest,
        row_count: u64,
        total_bytes: u64,
        schema: DatasetSchemaRef,
        anchor_manifest_id: Uuid,
        kind: GenerationKind,
        build_spec_digest: Option<DatasetBuildSpecDigest>,
        parents: Vec<GenerationParentEvidenceRow>,
        objects: Vec<GenerationObjectEvidenceRow>,
    ) -> Result<Self, EvidenceError> {
        if generation_sequence == 0
            || manifest_version == 0
            || row_count == 0
            || total_bytes == 0
            || anchor_manifest_id.is_nil()
            || objects.is_empty()
            || parents.len() > MAX_DERIVED_GENERATION_PARENTS
            || (kind == GenerationKind::Derived) != build_spec_digest.is_some()
        {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_| EvidenceError::InvalidCatalogEvidence)?;
        Ok(Self {
            generation_sequence,
            dataset_id,
            manifest_version,
            content_hash,
            lineage_hash,
            row_count,
            total_bytes,
            schema,
            anchor_manifest_id,
            kind,
            build_spec_digest,
            parents,
            objects,
        })
    }

    pub(super) const fn generation_sequence(&self) -> u64 {
        self.generation_sequence
    }

    pub(super) const fn dataset_id(&self) -> &DatasetId {
        &self.dataset_id
    }

    pub(super) const fn manifest_version(&self) -> u64 {
        self.manifest_version
    }

    pub(super) const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    pub(super) const fn lineage_hash(&self) -> Sha256Digest {
        self.lineage_hash
    }

    pub(super) const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(super) const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub(super) const fn schema(&self) -> &DatasetSchemaRef {
        &self.schema
    }

    pub(super) const fn anchor_manifest_id(&self) -> Uuid {
        self.anchor_manifest_id
    }

    pub(super) const fn kind(&self) -> GenerationKind {
        self.kind
    }

    pub(super) const fn build_spec_digest(&self) -> Option<DatasetBuildSpecDigest> {
        self.build_spec_digest
    }

    pub(super) fn parents(&self) -> &[GenerationParentEvidenceRow] {
        &self.parents
    }

    pub(super) fn objects(&self) -> &[GenerationObjectEvidenceRow] {
        &self.objects
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

    pub(super) const fn reservation_id(&self) -> Uuid {
        self.reservation_id
    }

    pub(super) const fn owner(&self) -> &SourceIdentifier {
        &self.owner
    }

    pub(super) const fn request_hash(&self) -> Sha256Digest {
        self.request_hash
    }

    pub(super) const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    pub(super) fn relative_reference(&self) -> &str {
        &self.relative_reference
    }

    pub(super) const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    pub(super) const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub(super) const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Validated, bounded relational evidence captured from one SQLite read snapshot.
#[derive(Debug)]
pub(crate) struct CatalogEvidenceSnapshot {
    request: EvidenceSnapshotRequest,
    artifacts: Vec<ArtifactEvidenceRow>,
    manifests: Vec<ManifestEvidenceRow>,
    generations: Vec<GenerationEvidenceRow>,
    query_artifacts: Vec<QueryArtifactEvidenceRow>,
}

impl CatalogEvidenceSnapshot {
    pub(crate) fn try_new(
        request: EvidenceSnapshotRequest,
        artifacts: Vec<ArtifactEvidenceRow>,
        manifests: Vec<ManifestEvidenceRow>,
        generations: Vec<GenerationEvidenceRow>,
        query_artifacts: Vec<QueryArtifactEvidenceRow>,
    ) -> Result<Self, EvidenceError> {
        let limits = request.limits;
        let generation_objects = generations.iter().try_fold(0_usize, |count, generation| {
            count
                .checked_add(generation.objects.len())
                .ok_or(EvidenceError::ResourceLimitExceeded)
        })?;
        let generation_parents = generations.iter().try_fold(0_usize, |count, generation| {
            count
                .checked_add(generation.parents.len())
                .ok_or(EvidenceError::ResourceLimitExceeded)
        })?;
        let references = artifacts
            .len()
            .checked_add(manifests.len())
            .and_then(|count| count.checked_add(generation_objects))
            .and_then(|count| count.checked_add(generation_parents))
            .and_then(|count| count.checked_add(query_artifacts.len()))
            .ok_or(EvidenceError::ResourceLimitExceeded)?;
        let physical_artifacts = artifacts
            .len()
            .checked_add(query_artifacts.len())
            .ok_or(EvidenceError::ResourceLimitExceeded)?;
        if physical_artifacts > limits.max_artifacts || references > limits.max_references {
            return Err(EvidenceError::ResourceLimitExceeded);
        }
        validate_relational_evidence(
            request,
            &artifacts,
            &manifests,
            &generations,
            &query_artifacts,
        )?;
        Ok(Self {
            request,
            artifacts,
            manifests,
            generations,
            query_artifacts,
        })
    }

    pub(crate) const fn request(&self) -> EvidenceSnapshotRequest {
        self.request
    }

    pub(crate) fn artifacts(&self) -> &[ArtifactEvidenceRow] {
        &self.artifacts
    }

    pub(crate) fn manifests(&self) -> &[ManifestEvidenceRow] {
        &self.manifests
    }

    pub(crate) fn generations(&self) -> &[GenerationEvidenceRow] {
        &self.generations
    }

    pub(crate) fn query_artifacts(&self) -> &[QueryArtifactEvidenceRow] {
        &self.query_artifacts
    }

    pub(crate) fn physical_artifact_count(&self) -> usize {
        self.artifacts.len() + self.query_artifacts.len()
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

    pub(crate) fn evidence_digest(&self) -> Result<CatalogContentEvidenceDigest, EvidenceError> {
        super::canonical::evidence_digest(self)
    }
}

fn validate_relational_evidence(
    request: EvidenceSnapshotRequest,
    artifacts: &[ArtifactEvidenceRow],
    manifests: &[ManifestEvidenceRow],
    generations: &[GenerationEvidenceRow],
    query_artifacts: &[QueryArtifactEvidenceRow],
) -> Result<(), EvidenceError> {
    let limits = request.limits;
    let mut artifacts_by_id = BTreeMap::new();
    let mut physical_artifact_ids = BTreeSet::new();
    let mut runs = BTreeSet::new();
    let mut references = BTreeSet::new();
    let mut total_bytes = 0_u64;
    for artifact in artifacts {
        if artifact.size_bytes > limits.max_object_bytes
            || artifacts_by_id
                .insert(artifact.artifact_id, artifact)
                .is_some()
            || !physical_artifact_ids.insert(artifact.artifact_id)
            || !runs.insert(artifact.run_id)
            || !references.insert(artifact.relative_reference.as_ref())
        {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
        total_bytes = total_bytes
            .checked_add(artifact.size_bytes)
            .ok_or(EvidenceError::ResourceLimitExceeded)?;
        if total_bytes > limits.max_total_bytes {
            return Err(EvidenceError::ResourceLimitExceeded);
        }
    }

    let mut manifests_by_id = BTreeMap::new();
    for manifest in manifests {
        if !artifacts_by_id.contains_key(&manifest.artifact_id)
            || manifests_by_id
                .insert(manifest.manifest_id, manifest)
                .is_some()
        {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
    }

    let mut generations_by_dataset: BTreeMap<&str, BTreeMap<u64, &GenerationEvidenceRow>> =
        BTreeMap::new();
    let mut generation_sequences = BTreeSet::new();
    for generation in generations {
        let anchor = manifests_by_id
            .get(&generation.anchor_manifest_id)
            .ok_or(EvidenceError::GenerationSemanticMismatch)?;
        if anchor.dataset_id != generation.dataset_id
            || anchor.schema_version != u32::from(generation.schema.version().get())
            || anchor.content_hash != generation.content_hash
        {
            return Err(EvidenceError::GenerationSemanticMismatch);
        }
        for object in &generation.objects {
            let artifact = artifacts_by_id
                .get(&object.artifact_id)
                .ok_or(EvidenceError::GenerationSemanticMismatch)?;
            if artifact.content_hash != object.content_hash
                || artifact.size_bytes != object.size_bytes
            {
                return Err(EvidenceError::GenerationSemanticMismatch);
            }
        }
        if !generation_sequences.insert(generation.generation_sequence)
            || generations_by_dataset
                .entry(generation.dataset_id.as_str())
                .or_default()
                .insert(generation.manifest_version, generation)
                .is_some()
        {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
    }
    for generation in generations {
        validate_generation_parents(generation, &generations_by_dataset)?;
    }
    for versions in generations_by_dataset.values() {
        validate_dataset_history(versions)?;
    }

    let mut query_reservations = BTreeSet::new();
    for query in query_artifacts {
        if query.size_bytes > limits.max_object_bytes
            || !query_reservations.insert(query.reservation_id)
            || !physical_artifact_ids.insert(query.artifact_id)
            || !references.insert(query.relative_reference.as_ref())
            || query.expires_at <= request.cutoff
        {
            return Err(EvidenceError::InvalidCatalogEvidence);
        }
        total_bytes = total_bytes
            .checked_add(query.size_bytes)
            .ok_or(EvidenceError::ResourceLimitExceeded)?;
        if total_bytes > limits.max_total_bytes {
            return Err(EvidenceError::ResourceLimitExceeded);
        }
    }
    Ok(())
}

fn validate_generation_parents(
    child: &GenerationEvidenceRow,
    generations: &BTreeMap<&str, BTreeMap<u64, &GenerationEvidenceRow>>,
) -> Result<(), EvidenceError> {
    for (ordinal, edge) in child.parents.iter().enumerate() {
        let retained = generations
            .get(edge.parent.manifest().dataset_id().as_str())
            .and_then(|versions| versions.get(&edge.parent.manifest().manifest_version()))
            .ok_or(EvidenceError::GenerationSemanticMismatch)?;
        if retained.generation_sequence != edge.generation_sequence
            || retained.generation_sequence >= child.generation_sequence
            || retained.schema != *edge.parent.manifest().schema()
            || retained.content_hash != edge.parent.manifest().content_hash()
            || (ordinal > 0
                && compare_manifest_refs(
                    child.parents[ordinal - 1].parent.manifest(),
                    edge.parent.manifest(),
                )
                .is_ge())
        {
            return Err(EvidenceError::GenerationSemanticMismatch);
        }
    }
    let predecessor = |relation| match child.parents.as_slice() {
        [edge] => {
            edge.parent.relation() == relation
                && edge.parent.manifest().dataset_id() == &child.dataset_id
                && edge.parent.manifest().manifest_version().checked_add(1)
                    == Some(child.manifest_version)
                && edge.parent.manifest().schema() == &child.schema
        }
        _ => false,
    };
    let valid = match child.kind {
        GenerationKind::Ingest if child.manifest_version == 1 => child.parents.is_empty(),
        GenerationKind::Ingest => predecessor(GenerationParentRelation::AppendPredecessor),
        GenerationKind::Compaction => predecessor(GenerationParentRelation::CompactionPredecessor),
        GenerationKind::Derived => {
            !child.parents.is_empty()
                && child
                    .parents
                    .iter()
                    .all(|edge| edge.parent.relation() == GenerationParentRelation::DerivedInput)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(EvidenceError::GenerationSemanticMismatch)
    }
}

fn validate_dataset_history(
    versions: &BTreeMap<u64, &GenerationEvidenceRow>,
) -> Result<(), EvidenceError> {
    let mut previous_plan: Option<ManifestPlan> = None;
    let mut retained_schema: Option<&DatasetSchemaRef> = None;
    let mut expected_version = 1_u64;
    for generation in versions.values() {
        if generation.manifest_version != expected_version
            || retained_schema.is_some_and(|schema| schema != &generation.schema)
        {
            return Err(EvidenceError::GenerationSemanticMismatch);
        }
        let plan = match generation.kind {
            GenerationKind::Ingest => {
                let object = generation
                    .objects
                    .last()
                    .ok_or(EvidenceError::GenerationSemanticMismatch)?
                    .manifest_object()?;
                ManifestPlan::append(
                    generation.dataset_id.clone(),
                    previous_plan.as_ref(),
                    object,
                    generation.objects.len(),
                )
                .map_err(|_| EvidenceError::GenerationSemanticMismatch)?
            }
            GenerationKind::Compaction => {
                let previous = previous_plan
                    .as_ref()
                    .ok_or(EvidenceError::GenerationSemanticMismatch)?;
                if generation.objects.len() != 1 {
                    return Err(EvidenceError::GenerationSemanticMismatch);
                }
                ManifestPlan::compact(previous, generation.objects[0].manifest_object()?)
                    .map_err(|_| EvidenceError::GenerationSemanticMismatch)?
            }
            GenerationKind::Derived => ManifestPlan::derive(
                generation.dataset_id.clone(),
                generation
                    .objects
                    .iter()
                    .map(GenerationObjectEvidenceRow::manifest_object)
                    .collect::<Result<Vec<_>, _>>()?,
                generation.objects.len(),
            )
            .map_err(|_| EvidenceError::GenerationSemanticMismatch)?,
        };
        if plan.objects().len() != generation.objects.len()
            || plan.content_hash() != generation.content_hash
            || plan.lineage_digest() != generation.lineage_hash
            || plan.row_count() != generation.row_count
            || plan.total_bytes() != generation.total_bytes
            || plan
                .objects()
                .iter()
                .zip(&generation.objects)
                .any(|(planned, stored)| {
                    planned.content_hash() != stored.content_hash
                        || planned.row_count() != stored.row_count
                        || planned.size_bytes() != stored.size_bytes
                        || planned.lineage_digest() != stored.lineage_hash
                })
        {
            return Err(EvidenceError::GenerationSemanticMismatch);
        }
        previous_plan = Some(plan);
        retained_schema = Some(&generation.schema);
        expected_version = expected_version
            .checked_add(1)
            .ok_or(EvidenceError::ResourceLimitExceeded)?;
    }
    Ok(())
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
