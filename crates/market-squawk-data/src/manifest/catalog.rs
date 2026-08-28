//! SQLite-backed immutable analytical generation storage.

#[cfg(feature = "release-evidence")]
#[path = "../benchmark_support.rs"]
pub(super) mod benchmark_support;

use std::fmt;
use std::fmt::Write as _;
use std::mem::size_of;
use std::sync::Mutex;
use std::time::Instant;

use market_squawk_domain::{
    CompanyIdentityObservation, DigestAlgorithm, EvidenceDigest, SourceId, Timestamp,
};
use market_squawk_platform::{CatalogFileGuard, CatalogLocation};
use rusqlite::{
    Connection, ErrorCode, OpenFlags, OptionalExtension as _, Transaction, TransactionBehavior,
    params,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::market_history::{
    CanonicalMarketBarHistoryRequest, CompleteMarketBarHistoryRequest,
    CompleteMarketBarHistorySelection, generation_market_bar_history_candidate_matches,
    generation_market_bar_history_inputs_match_manifest,
    insert_generation_market_bar_history_inputs, select_canonical_market_bar_history,
    select_complete_market_bar_history,
};
use super::{
    DatasetBuildSpecDigest, DatasetId, DatasetManifestRef, DerivedGenerationCommitAuthority,
    DerivedGenerationParents, GenerationParent, GenerationParentRelation,
    MAX_DERIVED_GENERATION_PARENTS, ManifestObject, ManifestPlan, ManifestPlanError,
    MarketBarHistoryPublicationCandidate, Sha256Digest, compare_manifest_refs,
};
use crate::OptionMarketPointInTimeRequest;
use crate::catalog::exact_catalog_file_binding;
use crate::catalog::{
    PublicationSourceEvidence, complete_ingest_in_transaction,
    publish_artifact_manifest_in_transaction, trusted_catalog_now,
};
use crate::schema::{DatasetSchemaRef, DatasetSchemaRegistry};
use crate::{
    ArtifactRecord, CatalogEndpointIdentity, CatalogError, CatalogResultLimits, ContractCompletion,
    DatasetManifestRecord, FeatureDatasetProductContract, IngestReservation, IngestRunRecord,
    ResearchUse, ResearchUseDecisionDigest, ResearchUseGraphDigest, SourceOperation,
};

const REFERENCE_MEMBERSHIP_CHUNK: usize = 128;
const FEATURE_DATASET_MEMBERSHIP_CHUNK: usize = 128;
const MAX_FEATURE_DATASET_LEGACY_CANDIDATES: usize = 4_096;
const MAX_GENERATION_CAPTURE_INPUTS: usize = 4_096;
const SQLITE_PROGRESS_OPERATIONS: i32 = 1_000;

/// Maximum immutable feature-dataset production-admission rows retained by one local catalog.
pub const MAX_RETAINED_FEATURE_DATASET_PRODUCTION_ADMISSIONS: usize = 4_096;

/// Maximum descriptor-plus-receipt bytes retained across all production admissions.
pub const MAX_RETAINED_FEATURE_DATASET_PRODUCTION_PAYLOAD_BYTES: usize = 256 * 1024 * 1024;

/// One manifest-pinned object resolved from immutable catalog metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PinnedManifestObject {
    artifact_id: Uuid,
    relative_reference: Box<str>,
    object: ManifestObject,
}

impl PinnedManifestObject {
    /// Returns controlled artifact identity.
    pub const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    /// Returns the portable reference below the artifact root.
    pub fn relative_reference(&self) -> &str {
        &self.relative_reference
    }

    /// Returns immutable object metadata.
    pub const fn object(&self) -> &ManifestObject {
        &self.object
    }
}

/// Complete immutable generation resolved by exact manifest reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PinnedDataset {
    manifest: DatasetManifestRef,
    plan: ManifestPlan,
    generation_kind: GenerationKind,
    build_spec_digest: Option<DatasetBuildSpecDigest>,
    parents: Box<[GenerationParent]>,
    objects: Box<[PinnedManifestObject]>,
    retained_bytes: usize,
}

impl PinnedDataset {
    /// Returns the exact reader pin.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the immutable manifest plan.
    pub const fn plan(&self) -> &ManifestPlan {
        &self.plan
    }

    /// Returns how this generation relates to its exact retained parents.
    pub const fn generation_kind(&self) -> GenerationKind {
        self.generation_kind
    }

    /// Returns the caller-supplied build identity for a derived generation.
    pub const fn build_spec_digest(&self) -> Option<DatasetBuildSpecDigest> {
        self.build_spec_digest
    }

    /// Returns every exact parent in canonical durable ordinal order.
    pub fn parents(&self) -> &[GenerationParent] {
        &self.parents
    }

    /// Returns objects in stable row order.
    pub fn objects(&self) -> &[PinnedManifestObject] {
        &self.objects
    }

    /// Returns the checked requested bytes retained by this complete immutable pin graph.
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// SQLite-backed immutable analytical generation registry.
pub struct AnalyticalManifestCatalog {
    connection: Mutex<Connection>,
    max_objects_per_generation: usize,
    catalog_binding: [u8; 32],
    catalog_file: CatalogFileGuard,
}

#[derive(Debug)]
pub(crate) struct CatalogGenerationPage {
    pub(crate) generations: Vec<(PinnedDataset, SourceId, Option<Sha256Digest>)>,
    pub(crate) has_more: bool,
}

#[derive(Debug)]
pub(crate) struct CatalogFeatureDataset {
    pub(crate) pinned: PinnedDataset,
    pub(crate) source_id: SourceId,
    pub(crate) export_sha256: Sha256Digest,
    pub(crate) descriptor: Box<[u8]>,
    pub(crate) production_identity: Sha256Digest,
    pub(crate) receipt_sha256: Sha256Digest,
    pub(crate) receipt_json: Box<[u8]>,
    pub(crate) catalog_identity: CatalogEndpointIdentity,
    pub(crate) product_contract: FeatureDatasetProductContract,
    pub(crate) output_group_id: [u8; 32],
    pub(crate) final_output_rights_id: [u8; 32],
    pub(crate) research_decision: ResearchUseDecisionDigest,
    pub(crate) research_graph: ResearchUseGraphDigest,
    pub(crate) research_use: ResearchUse,
    pub(crate) research_use_expires_at: Timestamp,
    pub(crate) admitted_at: Timestamp,
    pub(crate) source_ids: Box<[SourceId]>,
}

#[derive(Debug)]
pub(crate) struct CatalogFeatureDatasetPage {
    pub(crate) datasets: Vec<CatalogFeatureDataset>,
    pub(crate) has_more: bool,
    pub(crate) available: usize,
    pub(crate) overlapping_legacy_dataset_ids: Vec<DatasetId>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum CatalogFeatureDatasetSelection<'a> {
    LatestByDataset(&'a DatasetId),
    ExactManifest(&'a DatasetManifestRef),
    Page { after: Option<&'a DatasetId> },
}

struct RetainedFeatureDatasetAdmission {
    dataset: String,
    version: i64,
    schema_name: String,
    schema_version: i64,
    schema_fingerprint: Vec<u8>,
    content_hash: Vec<u8>,
    export_sha256: Vec<u8>,
    descriptor: Vec<u8>,
    production_identity: Vec<u8>,
    receipt_schema: String,
    receipt_sha256: Vec<u8>,
    receipt_json: Vec<u8>,
    catalog_identity: Vec<u8>,
    product_contract: String,
    selection_digest_version: i64,
    output_group_id: Vec<u8>,
    final_output_rights_id: Vec<u8>,
    research_decision: Vec<u8>,
    research_graph: Vec<u8>,
    research_use: String,
    research_use_expires_at_ns: i64,
    admitted_at_ns: i64,
}

impl fmt::Debug for AnalyticalManifestCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnalyticalManifestCatalog")
            .field("connection", &"[SQLITE CONNECTION]")
            .field(
                "max_objects_per_generation",
                &self.max_objects_per_generation,
            )
            .finish()
    }
}

impl AnalyticalManifestCatalog {
    pub(crate) fn select_provider_option_market_publication(
        &self,
        request: &OptionMarketPointInTimeRequest,
    ) -> Result<Option<(DatasetManifestRef, EvidenceDigest, String)>, ManifestCatalogError> {
        let connection = self.lock()?;
        let exact_version = request
            .exact_manifest()
            .map(|manifest| to_i64(manifest.manifest_version()))
            .transpose()?;
        let publication_kind = match request.publication_kind() {
            market_squawk_sources::OptionMarketBatchKind::Snapshots => "option_snapshots",
            market_squawk_sources::OptionMarketBatchKind::Expirations => "option_expirations",
        };
        let mut statement = connection.prepare(
            "WITH candidates AS (
                 SELECT generation.dataset_id, generation.manifest_version,
                        generation.schema_name, generation.schema_version,
                        generation.schema_fingerprint, generation.content_hash,
                        publication.publication_digest, publication.publication_kind,
                        binding.available_at_ns, binding.received_at_ns, binding.ingested_at_ns,
                        ROW_NUMBER() OVER (
                            PARTITION BY publication.publication_digest
                            ORDER BY generation.manifest_version
                        ) AS origin_rank
                 FROM analytical_generation_provider_publication_bindings AS publication
                 JOIN analytical_generations AS generation
                   ON generation.generation_sequence=publication.generation_sequence
                 JOIN provider_option_market_bindings AS binding
                   ON binding.option_binding_digest=publication.publication_digest
                 WHERE generation.dataset_id=?1
                   AND generation.schema_name='market_squawk.option_market'
                   AND publication.publication_kind=?2
                   AND binding.underlying_instrument_id=?3
                   AND binding.filter_digest=?4
                   AND binding.available_at_ns<=?5
                   AND binding.ingested_at_ns<=?5
                   AND (?6 IS NULL OR generation.manifest_version=?6)
             )
             SELECT dataset_id, manifest_version, schema_name, schema_version,
                    schema_fingerprint, content_hash, publication_digest, publication_kind,
                    available_at_ns, received_at_ns, ingested_at_ns
             FROM candidates WHERE origin_rank=1 OR ?6 IS NOT NULL
             ORDER BY available_at_ns DESC, received_at_ns DESC, ingested_at_ns DESC,
                      publication_digest
             LIMIT 2",
        )?;
        let mut rows = statement.query(params![
            request.dataset().as_str(),
            publication_kind,
            request
                .underlying_instrument_id()
                .as_uuid()
                .as_bytes()
                .as_slice(),
            request.filter_digest().bytes().as_slice(),
            request.knowledge_cutoff().unix_nanos(),
            exact_version,
        ])?;
        type Candidate = (DatasetManifestRef, EvidenceDigest, String, (i64, i64, i64));
        let mut candidates: Vec<Candidate> = Vec::new();
        while let Some(row) = rows.next()? {
            let dataset_id = DatasetId::try_from(row.get::<_, String>(0)?.as_str())?;
            let version = u64::try_from(row.get::<_, i64>(1)?)
                .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
            let schema_name: String = row.get(2)?;
            let schema_version = market_squawk_domain::SchemaVersion::new(
                u16::try_from(row.get::<_, i64>(3)?)
                    .map_err(|_| ManifestCatalogError::CorruptCatalog)?,
            )
            .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
            let schema_fingerprint: Vec<u8> = row.get(4)?;
            let schema = DatasetSchemaRef::try_new(
                schema_name,
                schema_version,
                parse_digest(&schema_fingerprint)?.bytes(),
            )?;
            DatasetSchemaRegistry::local().resolve(&schema)?;
            let content_hash: Vec<u8> = row.get(5)?;
            let manifest = DatasetManifestRef::try_new_with_schema(
                dataset_id,
                version,
                schema,
                parse_digest(&content_hash)?,
            )?;
            candidates.push((
                manifest,
                EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    parse_digest(&row.get::<_, Vec<u8>>(6)?)?.bytes(),
                ),
                row.get(7)?,
                (row.get(8)?, row.get(9)?, row.get(10)?),
            ));
        }
        let Some(first) = candidates.first() else {
            return Ok(None);
        };
        if candidates
            .get(1)
            .is_some_and(|second| second.3 == first.3 && second.1 != first.1)
        {
            return Err(ManifestCatalogError::GenerationConflict);
        }
        if request
            .exact_manifest()
            .is_some_and(|exact| exact != &first.0)
        {
            return Err(ManifestCatalogError::GenerationConflict);
        }
        Ok(Some((first.0.clone(), first.1, first.2.clone())))
    }

    pub(crate) fn provider_publication_bindings(
        &self,
        manifest: &DatasetManifestRef,
    ) -> Result<Vec<(EvidenceDigest, String)>, ManifestCatalogError> {
        let connection = self.lock()?;
        let generation_sequence = connection
            .query_row(
                "SELECT generation_sequence FROM analytical_generations
                 WHERE dataset_id=?1 AND manifest_version=?2
                   AND schema_name=?3 AND schema_version=?4
                   AND schema_fingerprint=?5 AND content_hash=?6",
                params![
                    manifest.dataset_id().as_str(),
                    to_i64(manifest.manifest_version())?,
                    manifest.schema().name(),
                    i64::from(manifest.schema().version().get()),
                    manifest.schema().fingerprint().as_slice(),
                    manifest.content_hash().bytes(),
                ],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .ok_or(ManifestCatalogError::GenerationConflict)?;
        let mut statement = connection.prepare(
            "SELECT publication_digest, publication_kind
             FROM analytical_generation_provider_publication_bindings
             WHERE generation_sequence=?1 ORDER BY input_ordinal LIMIT ?2",
        )?;
        let mut rows = statement.query(params![
            generation_sequence,
            i64::try_from(MAX_GENERATION_CAPTURE_INPUTS + 1)
                .map_err(|_| ManifestCatalogError::CountOverflow)?,
        ])?;
        let mut publications = Vec::new();
        publications
            .try_reserve_exact(MAX_GENERATION_CAPTURE_INPUTS)
            .map_err(|_| ManifestCatalogError::CountOverflow)?;
        while let Some(row) = rows.next()? {
            if publications.len() == MAX_GENERATION_CAPTURE_INPUTS {
                return Err(ManifestCatalogError::CaptureInputLimitExceeded {
                    max: MAX_GENERATION_CAPTURE_INPUTS,
                });
            }
            let digest: Vec<u8> = row.get(0)?;
            publications.push((
                EvidenceDigest::new(DigestAlgorithm::Sha256, parse_digest(&digest)?.bytes()),
                row.get(1)?,
            ));
        }
        Ok(publications)
    }

    pub(crate) fn provider_capture_binding_digests(
        &self,
        manifest: &DatasetManifestRef,
    ) -> Result<Vec<EvidenceDigest>, ManifestCatalogError> {
        let connection = self.lock()?;
        let generation_sequence = connection
            .query_row(
                "SELECT generation_sequence
                 FROM analytical_generations
                 WHERE dataset_id=?1 AND manifest_version=?2
                   AND schema_name=?3 AND schema_version=?4
                   AND schema_fingerprint=?5 AND content_hash=?6",
                params![
                    manifest.dataset_id().as_str(),
                    to_i64(manifest.manifest_version())?,
                    manifest.schema().name(),
                    i64::from(manifest.schema().version().get()),
                    manifest.schema().fingerprint().as_slice(),
                    manifest.content_hash().bytes(),
                ],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .ok_or(ManifestCatalogError::GenerationConflict)?;
        let mut statement = connection.prepare(
            "SELECT input.binding_digest
             FROM analytical_generation_provider_capture_bindings AS input
             WHERE input.generation_sequence=?1
             ORDER BY input.input_ordinal
             LIMIT ?2",
        )?;
        let mut rows = statement.query(params![
            generation_sequence,
            i64::try_from(MAX_GENERATION_CAPTURE_INPUTS + 1)
                .map_err(|_| ManifestCatalogError::CountOverflow)?,
        ])?;
        let mut digests = Vec::new();
        digests
            .try_reserve_exact(MAX_GENERATION_CAPTURE_INPUTS)
            .map_err(|_| ManifestCatalogError::CountOverflow)?;
        while let Some(row) = rows.next()? {
            if digests.len() == MAX_GENERATION_CAPTURE_INPUTS {
                return Err(ManifestCatalogError::CaptureInputLimitExceeded {
                    max: MAX_GENERATION_CAPTURE_INPUTS,
                });
            }
            let digest: Vec<u8> = row.get(0)?;
            digests.push(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                parse_digest(&digest)?.bytes(),
            ));
        }
        Ok(digests)
    }

    /// Rejects an append that would mix analytical row schemas before object publication.
    pub(crate) fn validate_append_schema(
        &self,
        dataset_id: &DatasetId,
        schema: &DatasetSchemaRef,
    ) -> Result<(), ManifestCatalogError> {
        DatasetSchemaRegistry::local().resolve(schema)?;
        let connection = self.lock()?;
        ensure_append_schema(
            load_latest(&connection, dataset_id, self.max_objects_per_generation)?.as_ref(),
            schema,
        )
    }

    /// Opens the Task 3 catalog after analytical and query-artifact migrations are applied.
    pub fn open(
        location: &CatalogLocation,
        max_objects_per_generation: usize,
    ) -> Result<Self, ManifestCatalogError> {
        if max_objects_per_generation == 0 || max_objects_per_generation > 1024 {
            return Err(ManifestCatalogError::InvalidConfiguration);
        }
        location.validate_for_open()?;
        let catalog_file = location.prepare_catalog_file()?;
        let catalog_binding =
            exact_catalog_file_binding(&catalog_file.try_clone_file()?, location.path())?;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let connection = Connection::open_with_flags(location.path(), flags)?;
        catalog_file.validate_identity()?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "trusted_schema", "OFF")?;
        let migrated: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=14)",
            [],
            |row| row.get(0),
        )?;
        if !migrated {
            return Err(ManifestCatalogError::MigrationMissing);
        }
        let retention_is_consistent: bool = connection.query_row(
            "SELECT retained_rows = (
                        SELECT COUNT(*) FROM feature_dataset_production_admissions
                    )
                    AND retained_payload_bytes = (
                        SELECT COALESCE(
                            SUM(length(descriptor_json) + length(receipt_json)), 0
                        )
                        FROM feature_dataset_production_admissions
                    )
             FROM feature_dataset_production_admission_retention
             WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        if !retention_is_consistent {
            return Err(ManifestCatalogError::CorruptCatalog);
        }
        catalog_file.validate_identity()?;
        Ok(Self {
            connection: Mutex::new(connection),
            max_objects_per_generation,
            catalog_binding,
            catalog_file,
        })
    }

    pub(crate) const fn catalog_binding(&self) -> [u8; 32] {
        self.catalog_binding
    }

    /// Builds the exact next ingest plan while the process-owned catalog writer is serialized.
    pub(crate) fn preview_append(
        &self,
        dataset_id: DatasetId,
        schema: &DatasetSchemaRef,
        object: ManifestObject,
    ) -> Result<ManifestPlan, ManifestCatalogError> {
        DatasetSchemaRegistry::local().resolve(schema)?;
        let connection = self.lock()?;
        let previous = load_latest(&connection, &dataset_id, self.max_objects_per_generation)?;
        ensure_append_schema(previous.as_ref(), schema)?;
        ManifestPlan::append(
            dataset_id,
            previous.as_ref().map(PinnedDataset::plan),
            object,
            self.max_objects_per_generation,
        )
        .map_err(Into::into)
    }

    /// Builds a one-object compaction plan preserving the exact prior semantics.
    pub(crate) fn preview_compaction(
        &self,
        previous: &DatasetManifestRef,
        compacted: ManifestObject,
    ) -> Result<ManifestPlan, ManifestCatalogError> {
        let connection = self.lock()?;
        let previous = load_pinned(&connection, previous, self.max_objects_per_generation)?;
        ManifestPlan::compact(previous.plan(), compacted).map_err(Into::into)
    }

    /// Builds a complete multi-object derived plan in canonical content order.
    #[allow(
        dead_code,
        reason = "the immediately following ResearchUse dataset builder is the sole caller"
    )]
    pub(crate) fn preview_derived(
        &self,
        dataset_id: DatasetId,
        objects: Vec<ManifestObject>,
    ) -> Result<ManifestPlan, ManifestCatalogError> {
        ManifestPlan::derive(dataset_id, objects, self.max_objects_per_generation)
            .map_err(Into::into)
    }

    /// Commits one complete generation using `BEGIN IMMEDIATE` after the Task 3 anchor exists.
    pub(crate) fn commit_generation(
        &self,
        plan: &ManifestPlan,
        artifact: &ArtifactRecord,
        anchor: &DatasetManifestRecord,
        schema: &DatasetSchemaRef,
        kind: GenerationKind,
        source_input: Option<&IngestRunRecord>,
        market_bar_history: Option<&MarketBarHistoryPublicationCandidate>,
    ) -> Result<DatasetManifestRef, ManifestCatalogError> {
        if kind == GenerationKind::Derived
            || !matches!(
                (kind, source_input),
                (GenerationKind::Ingest, Some(_)) | (GenerationKind::Compaction, None)
            )
        {
            return Err(ManifestCatalogError::GenerationConflict);
        }
        DatasetSchemaRegistry::local().resolve(schema)?;
        if anchor.artifact_id() != artifact.artifact_id()
            || anchor.schema_version() != schema.version()
            || sha256_from_evidence(anchor.content_digest())? != plan.content_hash
            || sha256_from_evidence(artifact.content_digest())?
                != plan
                    .objects
                    .last()
                    .ok_or(ManifestCatalogError::CorruptCatalog)?
                    .content_hash
        {
            return Err(ManifestCatalogError::AnchorMismatch);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let manifest = commit_generation_in_transaction(
            &transaction,
            plan,
            artifact,
            anchor,
            schema,
            kind,
            source_input,
            market_bar_history,
            self.max_objects_per_generation,
        )?;
        transaction.commit()?;
        Ok(manifest)
    }

    /// Commits provider raw authority, artifact metadata, immutable generation, and successful
    /// run completion in one SQLite transaction. No process-local provider authority is required
    /// after this transition commits.
    #[allow(
        clippy::too_many_arguments,
        reason = "the atomic provider boundary keeps every independently verified input explicit"
    )]
    pub(crate) fn commit_provider_ingest_publication(
        &self,
        catalog_session_id: Uuid,
        result_limits: CatalogResultLimits,
        reservation: &IngestReservation,
        plan: &ManifestPlan,
        artifact: &ArtifactRecord,
        anchor: &DatasetManifestRecord,
        schema: &DatasetSchemaRef,
        source_input: &IngestRunRecord,
        source_evidence: PublicationSourceEvidence<'_>,
        company_identity: Option<&CompanyIdentityObservation>,
        market_bar_history: Option<&MarketBarHistoryPublicationCandidate>,
    ) -> Result<DatasetManifestRef, ManifestCatalogError> {
        if reservation.catalog_id() != catalog_session_id {
            return Err(ManifestCatalogError::CatalogAuthority(
                CatalogError::InvalidReservationCapability,
            ));
        }
        DatasetSchemaRegistry::local().resolve(schema)?;
        validate_generation_anchor(plan, artifact, anchor, schema)?;
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let catalog_now = trusted_catalog_now(&transaction)?;
        if artifact.created_at() > catalog_now
            || anchor.created_at() > catalog_now
            || catalog_now < reservation.requested_at()
        {
            return Err(ManifestCatalogError::CatalogAuthority(
                CatalogError::PublicationTimeConflict,
            ));
        }
        let artifact = ArtifactRecord::try_new(
            artifact.relative_reference(),
            artifact.content_digest(),
            artifact.size_bytes(),
            catalog_now,
        )?;
        let anchor = DatasetManifestRecord::try_new(
            anchor.dataset_name().clone(),
            anchor.schema_version(),
            artifact.artifact_id(),
            anchor.content_digest(),
            catalog_now,
        );
        validate_generation_anchor(plan, &artifact, &anchor, schema)?;
        let publication = publish_artifact_manifest_in_transaction(
            &transaction,
            result_limits,
            reservation,
            &artifact,
            &anchor,
            source_evidence,
            catalog_now,
        )?;
        let manifest = commit_generation_in_transaction(
            &transaction,
            plan,
            publication.artifact(),
            publication.manifest(),
            schema,
            GenerationKind::Ingest,
            Some(source_input),
            market_bar_history,
            self.max_objects_per_generation,
        )?;
        complete_ingest_in_transaction(
            &transaction,
            reservation,
            ContractCompletion::Succeeded,
            company_identity,
            catalog_now,
        )?;
        transaction.commit()?;
        Ok(manifest)
    }

    /// Commits one complete derived generation and every exact input edge atomically.
    #[allow(
        dead_code,
        clippy::too_many_arguments,
        reason = "the ResearchUse-gated atomic commit verifies seven independent retained identities"
    )]
    pub(crate) fn commit_derived_generation(
        &self,
        _authority: &DerivedGenerationCommitAuthority,
        plan: &ManifestPlan,
        artifacts: &[ArtifactRecord],
        anchor: &DatasetManifestRecord,
        schema: &DatasetSchemaRef,
        parents: &DerivedGenerationParents,
        build_spec_digest: DatasetBuildSpecDigest,
    ) -> Result<DatasetManifestRef, ManifestCatalogError> {
        DatasetSchemaRegistry::local().resolve(schema)?;
        if artifacts.len() != plan.objects.len() || artifacts.is_empty() {
            return Err(ManifestCatalogError::AnchorMismatch);
        }
        let mut canonical_artifacts: Vec<_> = artifacts.iter().collect();
        for artifact in &canonical_artifacts {
            if !matches!(
                artifact.content_digest().algorithm(),
                DigestAlgorithm::Sha256
            ) {
                return Err(ManifestCatalogError::AnchorMismatch);
            }
        }
        canonical_artifacts.sort_unstable_by_key(|artifact| artifact.content_digest().bytes());
        if anchor.artifact_id() != canonical_artifacts[0].artifact_id()
            || anchor.dataset_name().as_str() != plan.dataset_id.as_str()
            || anchor.schema_version() != schema.version()
            || sha256_from_evidence(anchor.content_digest())? != plan.content_hash
            || canonical_artifacts
                .iter()
                .zip(&plan.objects)
                .any(|(artifact, object)| {
                    sha256_from_evidence(artifact.content_digest()).ok()
                        != Some(object.content_hash)
                        || artifact.size_bytes() != object.size_bytes
                })
        {
            return Err(ManifestCatalogError::AnchorMismatch);
        }
        let expected = ManifestPlan::derive(
            plan.dataset_id.clone(),
            plan.objects.to_vec(),
            self.max_objects_per_generation,
        )?;
        if expected != *plan {
            return Err(ManifestCatalogError::GenerationConflict);
        }
        let requested_parents: Vec<_> = parents
            .as_slice()
            .iter()
            .cloned()
            .map(|manifest| GenerationParent::new(GenerationParentRelation::DerivedInput, manifest))
            .collect();
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for artifact in &canonical_artifacts {
            require_exact_artifact(&transaction, artifact)?;
        }
        require_exact_anchor(&transaction, anchor)?;
        if let Some(existing) = manifest_for_anchor(&transaction, anchor.manifest_id())? {
            let pinned = load_pinned(&transaction, &existing, self.max_objects_per_generation)?;
            if pinned.plan == *plan
                && pinned.manifest.schema == *schema
                && pinned.generation_kind == GenerationKind::Derived
                && pinned.build_spec_digest == Some(build_spec_digest)
                && pinned.parents.as_ref() == requested_parents
                && generation_capture_inputs_match_manifest(&transaction, &existing)?
                && generation_publication_inputs_match_manifest(&transaction, &existing)?
            {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(ManifestCatalogError::GenerationConflict);
        }
        let previous = load_latest(
            &transaction,
            &plan.dataset_id,
            self.max_objects_per_generation,
        )?;
        if previous
            .as_ref()
            .is_some_and(|value| value.manifest().schema() != schema)
        {
            return Err(ManifestCatalogError::SchemaMismatch);
        }
        for parent in parents.as_slice() {
            require_exact_generation(&transaction, parent)?;
        }
        let version = previous_version(&transaction, &plan.dataset_id)?
            .checked_add(1)
            .ok_or(ManifestCatalogError::CountOverflow)?;
        transaction.execute(
            "INSERT INTO analytical_generations
             (dataset_id, manifest_version, content_hash, lineage_hash, row_count, total_bytes,
              schema_name, schema_version, schema_fingerprint, anchor_manifest_id,
              generation_kind, parent_count, build_spec_digest, created_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'derived', ?11, ?12, ?13)",
            params![
                plan.dataset_id.as_str(),
                to_i64(version)?,
                plan.content_hash.bytes(),
                plan.lineage_digest.bytes(),
                to_i64(plan.row_count)?,
                to_i64(plan.total_bytes)?,
                schema.name(),
                i64::from(schema.version().get()),
                schema.fingerprint().as_slice(),
                anchor.manifest_id().to_string(),
                i64::try_from(requested_parents.len())
                    .map_err(|_| ManifestCatalogError::CountOverflow)?,
                build_spec_digest.digest().bytes(),
                anchor.created_at().unix_nanos(),
            ],
        )?;
        let generation_sequence = transaction.last_insert_rowid();
        for (ordinal, (artifact, object)) in
            canonical_artifacts.iter().zip(&plan.objects).enumerate()
        {
            insert_generation_object(
                &transaction,
                &plan.dataset_id,
                version,
                ordinal,
                artifact.artifact_id(),
                object,
            )?;
        }
        for (ordinal, parent) in requested_parents.iter().enumerate() {
            insert_generation_parent(&transaction, &plan.dataset_id, version, ordinal, parent)?;
        }
        propagate_generation_provider_capture_bindings(&transaction, generation_sequence)?;
        propagate_generation_provider_publication_bindings(&transaction, generation_sequence)?;
        let manifest = DatasetManifestRef::try_new_with_schema(
            plan.dataset_id.clone(),
            version,
            schema.clone(),
            plan.content_hash,
        )?;
        transaction.commit()?;
        Ok(manifest)
    }

    /// Resolves only the explicitly supplied immutable generation.
    pub fn pinned(
        &self,
        manifest: &DatasetManifestRef,
    ) -> Result<PinnedDataset, ManifestCatalogError> {
        DatasetSchemaRegistry::local()
            .resolve(manifest.schema())
            .map_err(|_| ManifestCatalogError::SchemaMismatch)?;
        let connection = self.lock()?;
        load_pinned(&connection, manifest, self.max_objects_per_generation)
    }

    /// Selects only a clock-safe complete market-bar window under one immutable generation.
    pub fn select_complete_market_bar_history(
        &self,
        request: &CompleteMarketBarHistoryRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<CompleteMarketBarHistorySelection>, ManifestCatalogError> {
        check_read_operation(deadline, cancellation)?;
        let mut connection = self.lock()?;
        let token = cancellation.clone();
        connection.progress_handler(
            SQLITE_PROGRESS_OPERATIONS,
            Some(move || token.is_cancelled() || Instant::now() >= deadline),
        )?;
        let result = (|| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
            match select_complete_market_bar_history(
                &transaction,
                self.max_objects_per_generation,
                request,
                deadline,
                cancellation,
            ) {
                Ok(selection) => {
                    transaction.commit()?;
                    Ok(selection)
                }
                Err(error) => Err(error),
            }
        })();
        connection.progress_handler::<fn() -> bool>(0, None)?;
        result.map_err(|error| classify_sqlite_interrupt(error, deadline, cancellation))
    }

    /// Resolves exactly one clock-safe durable series from canonical, provider-neutral inputs.
    pub fn select_canonical_market_bar_history(
        &self,
        request: &CanonicalMarketBarHistoryRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<CompleteMarketBarHistorySelection>, ManifestCatalogError> {
        check_read_operation(deadline, cancellation)?;
        let mut connection = self.lock()?;
        let token = cancellation.clone();
        connection.progress_handler(
            SQLITE_PROGRESS_OPERATIONS,
            Some(move || token.is_cancelled() || Instant::now() >= deadline),
        )?;
        let result = (|| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
            match select_canonical_market_bar_history(
                &transaction,
                self.max_objects_per_generation,
                request,
                deadline,
                cancellation,
            ) {
                Ok(selection) => {
                    transaction.commit()?;
                    Ok(selection)
                }
                Err(error) => Err(error),
            }
        })();
        connection.progress_handler::<fn() -> bool>(0, None)?;
        result.map_err(|error| classify_sqlite_interrupt(error, deadline, cancellation))
    }

    pub(crate) fn market_bar_history_candidate_matches(
        &self,
        manifest: &DatasetManifestRef,
        candidate: Option<&MarketBarHistoryPublicationCandidate>,
    ) -> Result<bool, ManifestCatalogError> {
        let connection = self.lock()?;
        generation_market_bar_history_candidate_matches(&connection, manifest, candidate)
    }

    /// Returns the current generation only as an explicit pin, never as a directory inference.
    pub fn latest(
        &self,
        dataset_id: &DatasetId,
    ) -> Result<Option<DatasetManifestRef>, ManifestCatalogError> {
        let connection = self.lock()?;
        Ok(
            load_latest(&connection, dataset_id, self.max_objects_per_generation)?
                .map(|value| value.manifest),
        )
    }

    /// Resolves one unique immutable derived generation by its complete build identity.
    pub(crate) fn matching_derived_build(
        &self,
        dataset_id: &DatasetId,
        build_spec_digest: DatasetBuildSpecDigest,
    ) -> Result<Option<PinnedDataset>, ManifestCatalogError> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT dataset_id, manifest_version, schema_name, schema_version,
                    schema_fingerprint, content_hash
             FROM analytical_generations
             WHERE dataset_id=?1 AND generation_kind='derived' AND build_spec_digest=?2
             ORDER BY manifest_version LIMIT 2",
        )?;
        let rows = statement.query_map(
            params![dataset_id.as_str(), build_spec_digest.digest().bytes()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                ))
            },
        )?;
        let mut matching = None;
        for row in rows {
            if matching.is_some() {
                return Err(ManifestCatalogError::CorruptCatalog);
            }
            let (dataset, version, schema_name, schema_version, fingerprint, content) = row?;
            matching = Some(DatasetManifestRef::try_new_with_schema(
                DatasetId::try_from(dataset.as_str())?,
                from_i64(version)?,
                parse_schema_identity(&schema_name, schema_version, &fingerprint)?,
                parse_digest(&content)?,
            )?);
        }
        matching
            .as_ref()
            .map(|manifest| load_pinned(&connection, manifest, self.max_objects_per_generation))
            .transpose()
    }

    /// Resolves the immutable generation anchored by one Task 3 ingest run, when present.
    pub fn for_run(&self, run_id: Uuid) -> Result<Option<PinnedDataset>, ManifestCatalogError> {
        let connection = self.lock()?;
        let reference = connection
            .query_row(
                "SELECT generations.dataset_id, generations.manifest_version,
                        generations.schema_name, generations.schema_version,
                        generations.schema_fingerprint, generations.content_hash
                 FROM analytical_generations AS generations
                 JOIN dataset_manifests AS manifests
                   ON manifests.manifest_id=generations.anchor_manifest_id
                 JOIN artifacts ON artifacts.artifact_id=manifests.artifact_id
                 WHERE artifacts.run_id=?1",
                [run_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                        row.get::<_, Vec<u8>>(5)?,
                    ))
                },
            )
            .optional()?
            .map(
                |(dataset, version, schema_name, schema_version, fingerprint, content)| {
                    DatasetManifestRef::try_new_with_schema(
                        DatasetId::try_from(dataset.as_str())?,
                        from_i64(version)?,
                        parse_schema_identity(&schema_name, schema_version, &fingerprint)?,
                        parse_digest(&content)?,
                    )
                    .map_err(ManifestCatalogError::from)
                },
            )
            .transpose()?;
        reference
            .as_ref()
            .map(|reference| load_pinned(&connection, reference, self.max_objects_per_generation))
            .transpose()
    }

    /// Returns the source-rights namespace that owns one immutable generation.
    pub fn source_id(
        &self,
        manifest: &DatasetManifestRef,
    ) -> Result<SourceId, ManifestCatalogError> {
        let connection = self.lock()?;
        generation_source(&connection, manifest)
    }

    pub(crate) fn read_exact(
        &self,
        manifest: &DatasetManifestRef,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(PinnedDataset, SourceId, Option<Sha256Digest>), ManifestCatalogError> {
        check_read_operation(deadline, cancellation)?;
        let connection = self.lock()?;
        let pinned = load_pinned(&connection, manifest, self.max_objects_per_generation)?;
        let source_id = generation_source(&connection, manifest)?;
        let python_export_sha256 = generation_python_export(&connection, manifest)?;
        check_read_operation(deadline, cancellation)?;
        Ok((pinned, source_id, python_export_sha256))
    }

    pub(crate) fn read_latest(
        &self,
        dataset_id: &DatasetId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<(PinnedDataset, SourceId, Option<Sha256Digest>)>, ManifestCatalogError> {
        check_read_operation(deadline, cancellation)?;
        let connection = self.lock()?;
        let Some(pinned) = load_latest(&connection, dataset_id, self.max_objects_per_generation)?
        else {
            check_read_operation(deadline, cancellation)?;
            return Ok(None);
        };
        let source_id = generation_source(&connection, pinned.manifest())?;
        let python_export_sha256 = generation_python_export(&connection, pinned.manifest())?;
        check_read_operation(deadline, cancellation)?;
        Ok(Some((pinned, source_id, python_export_sha256)))
    }

    pub(crate) fn read_latest_page(
        &self,
        after: Option<&DatasetId>,
        limit: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CatalogGenerationPage, ManifestCatalogError> {
        check_read_operation(deadline, cancellation)?;
        let retrieval_limit = limit
            .checked_add(1)
            .ok_or(ManifestCatalogError::CountOverflow)?;
        let retrieval_limit =
            i64::try_from(retrieval_limit).map_err(|_| ManifestCatalogError::CountOverflow)?;
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "WITH latest AS (
                 SELECT dataset_id, MAX(manifest_version) AS manifest_version
                 FROM analytical_generations
                 WHERE dataset_id>?1
                 GROUP BY dataset_id
                 ORDER BY dataset_id
                 LIMIT ?2
             )
             SELECT generations.dataset_id, generations.manifest_version,
                    generations.schema_name, generations.schema_version,
                    generations.schema_fingerprint, generations.content_hash
             FROM analytical_generations AS generations
             JOIN latest USING (dataset_id, manifest_version)
             ORDER BY generations.dataset_id",
        )?;
        let rows = statement.query_map(
            params![
                after.map(DatasetId::as_str).unwrap_or_default(),
                retrieval_limit
            ],
            manifest_reference_from_row,
        )?;
        let mut references = Vec::new();
        references
            .try_reserve_exact(
                usize::try_from(retrieval_limit)
                    .map_err(|_| ManifestCatalogError::CountOverflow)?,
            )
            .map_err(|_| ManifestCatalogError::CountOverflow)?;
        for row in rows {
            check_read_operation(deadline, cancellation)?;
            references.push(row??);
        }
        drop(statement);
        let has_more = references.len() > limit;
        references.truncate(limit);
        let mut generations = Vec::new();
        generations
            .try_reserve_exact(references.len())
            .map_err(|_| ManifestCatalogError::CountOverflow)?;
        for reference in references {
            check_read_operation(deadline, cancellation)?;
            let pinned = load_pinned(&connection, &reference, self.max_objects_per_generation)?;
            let source_id = generation_source(&connection, &reference)?;
            let python_export_sha256 = generation_python_export(&connection, &reference)?;
            generations.push((pinned, source_id, python_export_sha256));
        }
        check_read_operation(deadline, cancellation)?;
        Ok(CatalogGenerationPage {
            generations,
            has_more,
        })
    }

    pub(crate) fn read_history(
        &self,
        dataset_id: &DatasetId,
        before_version: Option<u64>,
        limit: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CatalogGenerationPage, ManifestCatalogError> {
        check_read_operation(deadline, cancellation)?;
        let retrieval_limit = limit
            .checked_add(1)
            .ok_or(ManifestCatalogError::CountOverflow)?;
        let retrieval_limit =
            i64::try_from(retrieval_limit).map_err(|_| ManifestCatalogError::CountOverflow)?;
        let before_version = before_version.map(to_i64).transpose()?;
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT dataset_id, manifest_version, schema_name, schema_version,
                    schema_fingerprint, content_hash
             FROM analytical_generations
             WHERE dataset_id=?1 AND (?2 IS NULL OR manifest_version<?2)
             ORDER BY manifest_version DESC
             LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![dataset_id.as_str(), before_version, retrieval_limit],
            manifest_reference_from_row,
        )?;
        let mut references = Vec::new();
        references
            .try_reserve_exact(
                usize::try_from(retrieval_limit)
                    .map_err(|_| ManifestCatalogError::CountOverflow)?,
            )
            .map_err(|_| ManifestCatalogError::CountOverflow)?;
        for row in rows {
            check_read_operation(deadline, cancellation)?;
            references.push(row??);
        }
        drop(statement);
        let has_more = references.len() > limit;
        references.truncate(limit);
        let mut generations = Vec::new();
        generations
            .try_reserve_exact(references.len())
            .map_err(|_| ManifestCatalogError::CountOverflow)?;
        for reference in references {
            check_read_operation(deadline, cancellation)?;
            let pinned = load_pinned(&connection, &reference, self.max_objects_per_generation)?;
            let source_id = generation_source(&connection, &reference)?;
            let python_export_sha256 = generation_python_export(&connection, &reference)?;
            generations.push((pinned, source_id, python_export_sha256));
        }
        check_read_operation(deadline, cancellation)?;
        Ok(CatalogGenerationPage {
            generations,
            has_more,
        })
    }

    pub(crate) fn read_feature_dataset_snapshot(
        &self,
        expected_contract: FeatureDatasetProductContract,
        selection: CatalogFeatureDatasetSelection<'_>,
        legacy_candidates: &[DatasetId],
        limit: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CatalogFeatureDatasetPage, ManifestCatalogError> {
        check_read_operation(deadline, cancellation)?;
        if legacy_candidates.len() > MAX_FEATURE_DATASET_LEGACY_CANDIDATES {
            return Err(ManifestCatalogError::FeatureDatasetCandidateLimitExceeded {
                max_candidates: MAX_FEATURE_DATASET_LEGACY_CANDIDATES,
            });
        }
        let live_catalog_identity = CatalogEndpointIdentity::try_from_bytes(self.catalog_binding)
            .ok_or(ManifestCatalogError::CorruptCatalog)?;
        let mut connection = self.lock()?;
        let token = cancellation.clone();
        connection.progress_handler(
            SQLITE_PROGRESS_OPERATIONS,
            Some(move || token.is_cancelled() || Instant::now() >= deadline),
        )?;
        let operation = (|| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
            let overlapping_legacy_dataset_ids = feature_dataset_overlaps(
                &transaction,
                expected_contract,
                legacy_candidates,
                deadline,
                cancellation,
            )?;
            let (admissions, has_more, available) = feature_dataset_admissions(
                &transaction,
                expected_contract,
                selection,
                limit,
                deadline,
                cancellation,
            )?;
            let mut datasets = Vec::new();
            datasets
                .try_reserve_exact(admissions.len())
                .map_err(|_| ManifestCatalogError::CountOverflow)?;
            for admission in admissions {
                check_read_operation(deadline, cancellation)?;
                let dataset = load_feature_dataset_admission(
                    &transaction,
                    admission,
                    expected_contract,
                    self.max_objects_per_generation,
                    deadline,
                    cancellation,
                )?;
                if dataset.catalog_identity != live_catalog_identity {
                    return Err(ManifestCatalogError::CorruptCatalog);
                }
                datasets.push(dataset);
            }
            check_read_operation(deadline, cancellation)?;
            transaction.commit()?;
            Ok(CatalogFeatureDatasetPage {
                datasets,
                has_more,
                available,
                overlapping_legacy_dataset_ids,
            })
        })();
        connection.progress_handler::<fn() -> bool>(0, None)?;
        operation.map_err(|error| classify_sqlite_interrupt(error, deadline, cancellation))
    }

    /// Resolves only candidate reachability in bounded chunks under one consistent read snapshot.
    pub(crate) fn referenced_candidates<I>(
        &self,
        candidates: I,
        now: Timestamp,
        max_candidates: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<bool>, ManifestCatalogError>
    where
        I: ExactSizeIterator<Item = Sha256Digest>,
    {
        check_read_operation(deadline, cancellation)?;
        let expected = candidates.len();
        if expected > max_candidates {
            return Err(ManifestCatalogError::ReferenceWorkLimitExceeded { max_candidates });
        }
        let mut membership = Vec::new();
        membership
            .try_reserve_exact(expected)
            .map_err(|_| ManifestCatalogError::CountOverflow)?;
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let mut candidates = candidates.peekable();
        while candidates.peek().is_some() {
            check_read_operation(deadline, cancellation)?;
            let mut chunk = Vec::new();
            chunk
                .try_reserve_exact(REFERENCE_MEMBERSHIP_CHUNK.min(expected))
                .map_err(|_| ManifestCatalogError::CountOverflow)?;
            while chunk.len() < REFERENCE_MEMBERSHIP_CHUNK {
                let Some(candidate) = candidates.next() else {
                    break;
                };
                chunk.push(candidate);
                let collected = membership
                    .len()
                    .checked_add(chunk.len())
                    .ok_or(ManifestCatalogError::CountOverflow)?;
                if collected > max_candidates
                    || (collected == max_candidates && candidates.peek().is_some())
                {
                    return Err(ManifestCatalogError::ReferenceWorkLimitExceeded {
                        max_candidates,
                    });
                }
            }
            append_reference_membership(
                &transaction,
                &chunk,
                now,
                deadline,
                cancellation,
                &mut membership,
            )?;
        }
        if membership.len() != expected {
            return Err(ManifestCatalogError::CorruptCatalog);
        }
        check_read_operation(deadline, cancellation)?;
        transaction.commit()?;
        Ok(membership)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, ManifestCatalogError> {
        self.catalog_file.validate_identity()?;
        self.connection
            .lock()
            .map_err(|_| ManifestCatalogError::LockPoisoned)
    }
}

fn feature_dataset_admissions(
    transaction: &Transaction<'_>,
    expected_contract: FeatureDatasetProductContract,
    selection: CatalogFeatureDatasetSelection<'_>,
    limit: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(Vec<RetainedFeatureDatasetAdmission>, bool, usize), ManifestCatalogError> {
    check_read_operation(deadline, cancellation)?;
    let expected_use = expected_contract.required_use();
    let after = match selection {
        CatalogFeatureDatasetSelection::LatestByDataset(dataset_id) => {
            let admission = transaction
                .query_row(
                    "SELECT generation.dataset_id, generation.manifest_version,
                            generation.schema_name, generation.schema_version,
                            generation.schema_fingerprint, generation.content_hash,
                            admission.export_sha256, admission.descriptor_json,
                            admission.production_identity_sha256, admission.receipt_schema,
                            admission.receipt_sha256, admission.receipt_json,
                            admission.catalog_identity, admission.product_contract,
                            admission.selection_digest_version,
                            admission.output_group_id, admission.final_output_rights_id,
                            admission.research_decision_id, admission.research_graph_digest,
                            admission.research_use, admission.research_use_expires_at_ns,
                            admission.admitted_at_ns
                     FROM feature_dataset_production_admissions AS admission
                     JOIN analytical_generations AS generation
                       USING (dataset_id, manifest_version)
                     WHERE generation.dataset_id=?1 AND admission.product_contract=?2
                       AND admission.research_use=?3
                     ORDER BY generation.manifest_version DESC
                     LIMIT 1",
                    params![
                        dataset_id.as_str(),
                        expected_contract.identity(),
                        expected_use.database_name()
                    ],
                    retained_feature_dataset_admission,
                )
                .optional()?;
            let available = usize::from(admission.is_some());
            let mut admissions = Vec::new();
            admissions
                .try_reserve_exact(available)
                .map_err(|_| ManifestCatalogError::CountOverflow)?;
            admissions.extend(admission);
            return Ok((admissions, false, available));
        }
        CatalogFeatureDatasetSelection::ExactManifest(manifest) => {
            let manifest_version = i64::try_from(manifest.manifest_version())
                .map_err(|_| ManifestCatalogError::CountOverflow)?;
            let schema_version = i64::from(manifest.schema().version().get());
            let admission = transaction
                .query_row(
                    "SELECT generation.dataset_id, generation.manifest_version,
                            generation.schema_name, generation.schema_version,
                            generation.schema_fingerprint, generation.content_hash,
                            admission.export_sha256, admission.descriptor_json,
                            admission.production_identity_sha256, admission.receipt_schema,
                            admission.receipt_sha256, admission.receipt_json,
                            admission.catalog_identity, admission.product_contract,
                            admission.selection_digest_version,
                            admission.output_group_id, admission.final_output_rights_id,
                            admission.research_decision_id, admission.research_graph_digest,
                            admission.research_use, admission.research_use_expires_at_ns,
                            admission.admitted_at_ns
                     FROM feature_dataset_production_admissions AS admission
                     JOIN analytical_generations AS generation
                       USING (dataset_id, manifest_version)
                     WHERE generation.dataset_id=?1 AND generation.manifest_version=?2
                       AND generation.schema_name=?3 AND generation.schema_version=?4
                       AND generation.schema_fingerprint=?5 AND generation.content_hash=?6
                       AND admission.product_contract=?7 AND admission.research_use=?8
                     LIMIT 1",
                    params![
                        manifest.dataset_id().as_str(),
                        manifest_version,
                        manifest.schema().name(),
                        schema_version,
                        manifest.schema().fingerprint().as_slice(),
                        manifest.content_hash().bytes(),
                        expected_contract.identity(),
                        expected_use.database_name()
                    ],
                    retained_feature_dataset_admission,
                )
                .optional()?;
            let available = usize::from(admission.is_some());
            let mut admissions = Vec::new();
            admissions
                .try_reserve_exact(available)
                .map_err(|_| ManifestCatalogError::CountOverflow)?;
            admissions.extend(admission);
            return Ok((admissions, false, available));
        }
        CatalogFeatureDatasetSelection::Page { after } => after,
    };
    let after = after.map(DatasetId::as_str).unwrap_or_default();
    let available_sql: i64 = transaction.query_row(
        "SELECT COUNT(DISTINCT dataset_id)
         FROM feature_dataset_production_admissions
         WHERE dataset_id>?1 AND product_contract=?2 AND research_use=?3",
        params![
            after,
            expected_contract.identity(),
            expected_use.database_name()
        ],
        |row| row.get(0),
    )?;
    let available =
        usize::try_from(available_sql).map_err(|_| ManifestCatalogError::CountOverflow)?;
    let retrieval_limit = limit
        .checked_add(1)
        .ok_or(ManifestCatalogError::CountOverflow)?;
    let retrieval_limit_sql =
        i64::try_from(retrieval_limit).map_err(|_| ManifestCatalogError::CountOverflow)?;
    let mut statement = transaction.prepare(
        "WITH latest AS (
             SELECT dataset_id, MAX(manifest_version) AS manifest_version
             FROM feature_dataset_production_admissions
             WHERE dataset_id>?1 AND product_contract=?2 AND research_use=?3
             GROUP BY dataset_id
             ORDER BY dataset_id
             LIMIT ?4
         )
         SELECT generation.dataset_id, generation.manifest_version,
                generation.schema_name, generation.schema_version,
                generation.schema_fingerprint, generation.content_hash,
                admission.export_sha256, admission.descriptor_json,
                admission.production_identity_sha256, admission.receipt_schema,
                admission.receipt_sha256, admission.receipt_json,
                admission.catalog_identity, admission.product_contract,
                admission.selection_digest_version,
                admission.output_group_id, admission.final_output_rights_id,
                admission.research_decision_id, admission.research_graph_digest,
                admission.research_use, admission.research_use_expires_at_ns,
                admission.admitted_at_ns
         FROM latest
         JOIN analytical_generations AS generation USING (dataset_id, manifest_version)
         JOIN feature_dataset_production_admissions AS admission
           USING (dataset_id, manifest_version)
         ORDER BY generation.dataset_id",
    )?;
    let rows = statement.query_map(
        params![
            after,
            expected_contract.identity(),
            expected_use.database_name(),
            retrieval_limit_sql
        ],
        retained_feature_dataset_admission,
    )?;
    let mut admissions = Vec::new();
    admissions
        .try_reserve_exact(retrieval_limit)
        .map_err(|_| ManifestCatalogError::CountOverflow)?;
    for row in rows {
        check_read_operation(deadline, cancellation)?;
        admissions.push(row?);
    }
    drop(statement);
    let has_more = admissions.len() > limit;
    admissions.truncate(limit);
    if admissions.len() > available || has_more != (admissions.len() < available) {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    Ok((admissions, has_more, available))
}

fn retained_feature_dataset_admission(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RetainedFeatureDatasetAdmission> {
    Ok(RetainedFeatureDatasetAdmission {
        dataset: row.get(0)?,
        version: row.get(1)?,
        schema_name: row.get(2)?,
        schema_version: row.get(3)?,
        schema_fingerprint: row.get(4)?,
        content_hash: row.get(5)?,
        export_sha256: row.get(6)?,
        descriptor: row.get(7)?,
        production_identity: row.get(8)?,
        receipt_schema: row.get(9)?,
        receipt_sha256: row.get(10)?,
        receipt_json: row.get(11)?,
        catalog_identity: row.get(12)?,
        product_contract: row.get(13)?,
        selection_digest_version: row.get(14)?,
        output_group_id: row.get(15)?,
        final_output_rights_id: row.get(16)?,
        research_decision: row.get(17)?,
        research_graph: row.get(18)?,
        research_use: row.get(19)?,
        research_use_expires_at_ns: row.get(20)?,
        admitted_at_ns: row.get(21)?,
    })
}

fn feature_dataset_overlaps(
    transaction: &Transaction<'_>,
    expected_contract: FeatureDatasetProductContract,
    legacy_candidates: &[DatasetId],
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<DatasetId>, ManifestCatalogError> {
    let mut overlaps = Vec::new();
    overlaps
        .try_reserve_exact(legacy_candidates.len())
        .map_err(|_| ManifestCatalogError::CountOverflow)?;
    for candidates in legacy_candidates.chunks(FEATURE_DATASET_MEMBERSHIP_CHUNK) {
        check_read_operation(deadline, cancellation)?;
        let query_capacity = candidates
            .len()
            .checked_mul(8)
            .and_then(|value| value.checked_add(256))
            .ok_or(ManifestCatalogError::CountOverflow)?;
        let mut query = String::new();
        query
            .try_reserve_exact(query_capacity)
            .map_err(|_| ManifestCatalogError::CountOverflow)?;
        query.push_str(
            "SELECT DISTINCT dataset_id FROM feature_dataset_production_admissions \
             WHERE product_contract=?1 AND research_use=?2 AND dataset_id IN (",
        );
        for index in 0..candidates.len() {
            if index > 0 {
                query.push(',');
            }
            write!(&mut query, "?{}", index + 3)
                .map_err(|_| ManifestCatalogError::AllocationContract)?;
        }
        query.push_str(") ORDER BY dataset_id");
        let mut statement = transaction.prepare(&query)?;
        statement.raw_bind_parameter(1, expected_contract.identity())?;
        statement.raw_bind_parameter(2, expected_contract.required_use().database_name())?;
        for (index, candidate) in candidates.iter().enumerate() {
            statement.raw_bind_parameter(index + 3, candidate.as_str())?;
        }
        let mut rows = statement.raw_query();
        while let Some(row) = rows.next()? {
            check_read_operation(deadline, cancellation)?;
            let dataset = row.get::<_, String>(0)?;
            overlaps.push(DatasetId::try_from(dataset.as_str())?);
        }
    }
    overlaps.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
    overlaps.dedup_by(|left, right| left == right);
    if overlaps.len() > legacy_candidates.len() {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    Ok(overlaps)
}

fn classify_sqlite_interrupt(
    error: ManifestCatalogError,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> ManifestCatalogError {
    match error {
        ManifestCatalogError::Sqlite(sqlite)
            if sqlite.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) =>
        {
            if cancellation.is_cancelled() {
                ManifestCatalogError::Cancelled
            } else if Instant::now() >= deadline {
                ManifestCatalogError::DeadlineExceeded
            } else {
                ManifestCatalogError::Sqlite(sqlite)
            }
        }
        error => error,
    }
}

fn load_feature_dataset_admission(
    connection: &Connection,
    admission: RetainedFeatureDatasetAdmission,
    expected_contract: FeatureDatasetProductContract,
    max_objects_per_generation: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CatalogFeatureDataset, ManifestCatalogError> {
    let expected_use = expected_contract.required_use();
    let manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from(admission.dataset.as_str())?,
        from_i64(admission.version)?,
        parse_schema_identity(
            &admission.schema_name,
            admission.schema_version,
            &admission.schema_fingerprint,
        )?,
        parse_digest(&admission.content_hash)?,
    )?;
    let pinned = load_pinned(connection, &manifest, max_objects_per_generation)?;
    let source_id = generation_source(connection, &manifest)?;
    let export_sha256 = parse_digest(&admission.export_sha256)?;
    let production_identity = parse_digest(&admission.production_identity)?;
    let receipt_sha256 = parse_digest(&admission.receipt_sha256)?;
    let catalog_identity =
        CatalogEndpointIdentity::try_from_bytes(parse_digest(&admission.catalog_identity)?.bytes())
            .ok_or(ManifestCatalogError::CorruptCatalog)?;
    let output_group_id = parse_digest(&admission.output_group_id)?.bytes();
    let final_output_rights_id = parse_digest(&admission.final_output_rights_id)?.bytes();
    let research_decision = ResearchUseDecisionDigest::try_from_bytes(
        parse_digest(&admission.research_decision)?.bytes(),
    )
    .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let research_graph =
        ResearchUseGraphDigest::try_from_bytes(parse_digest(&admission.research_graph)?.bytes())
            .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let research_use = match admission.research_use.as_str() {
        "local_analysis" => ResearchUse::LocalAnalysis,
        "train" => ResearchUse::Train,
        _ => return Err(ManifestCatalogError::CorruptCatalog),
    };
    let product_contract =
        FeatureDatasetProductContract::from_identity(&admission.product_contract)
            .ok_or(ManifestCatalogError::CorruptCatalog)?;
    if product_contract != expected_contract
        || research_use != expected_use
        || admission.receipt_schema != crate::FEATURE_DATASET_PRODUCTION_RECEIPT_SCHEMA
        || admission.selection_digest_version != 2
        || admission.admitted_at_ns >= admission.research_use_expires_at_ns
    {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let mut source_ids = Vec::new();
    source_ids
        .try_reserve_exact(pinned.parents().len())
        .map_err(|_| ManifestCatalogError::CountOverflow)?;
    for parent in pinned.parents() {
        check_read_operation(deadline, cancellation)?;
        source_ids.push(generation_source(connection, parent.manifest())?);
    }
    source_ids.sort_unstable();
    source_ids.dedup();
    if source_ids.is_empty() {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    Ok(CatalogFeatureDataset {
        pinned,
        source_id,
        export_sha256,
        descriptor: admission.descriptor.into_boxed_slice(),
        production_identity,
        receipt_sha256,
        receipt_json: admission.receipt_json.into_boxed_slice(),
        catalog_identity,
        product_contract,
        output_group_id,
        final_output_rights_id,
        research_decision,
        research_graph,
        research_use,
        research_use_expires_at: Timestamp::from_unix_nanos(admission.research_use_expires_at_ns),
        admitted_at: Timestamp::from_unix_nanos(admission.admitted_at_ns),
        source_ids: source_ids.into_boxed_slice(),
    })
}

fn ensure_append_schema(
    previous: Option<&PinnedDataset>,
    schema: &DatasetSchemaRef,
) -> Result<(), ManifestCatalogError> {
    if previous.is_some_and(|value| value.manifest().schema() != schema) {
        Err(ManifestCatalogError::SchemaMismatch)
    } else {
        Ok(())
    }
}

/// How one immutable generation changes its predecessor's object set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GenerationKind {
    /// Append one newly ingested object.
    Ingest,
    /// Replace all prior objects with one equivalent compacted object.
    Compaction,
    /// Publish a complete output computed from explicit exact input generations.
    Derived,
}

impl GenerationKind {
    pub(crate) const fn database_name(self) -> &'static str {
        match self {
            Self::Ingest => "ingest",
            Self::Compaction => "compaction",
            Self::Derived => "derived",
        }
    }

    pub(crate) fn from_database_name(value: &str) -> Option<Self> {
        match value {
            "ingest" => Some(Self::Ingest),
            "compaction" => Some(Self::Compaction),
            "derived" => Some(Self::Derived),
            _ => None,
        }
    }
}

/// Immutable generation catalog failure.
#[derive(Debug, Error)]
pub enum ManifestCatalogError {
    /// Object ceiling is zero or excessive.
    #[error("analytical manifest configuration is invalid")]
    InvalidConfiguration,
    /// Task 3 did not apply the digest-bound analytical migration.
    #[error("analytical catalog migration is missing")]
    MigrationMissing,
    /// Artifact, Task 3 manifest, and analytical plan identities disagree.
    #[error("analytical manifest anchor mismatch")]
    AnchorMismatch,
    /// The latest generation changed or an idempotency replay differs.
    #[error("analytical manifest generation conflicts with retained state")]
    GenerationConflict,
    /// A dataset cannot combine independently admitted source-rights namespaces implicitly.
    #[error("analytical dataset source identity conflicts with its prior generation")]
    SourceMismatch,
    /// Append cannot mix row schemas in one immutable generation; migration must be explicit.
    #[error("analytical dataset schema conflicts with its prior generation")]
    SchemaMismatch,
    /// A supplied dataset schema is unknown or its fingerprint is not canonical.
    #[error("analytical dataset schema identity is invalid")]
    SchemaIdentity(#[from] crate::schema::DatasetSchemaError),
    /// Stored generation metadata does not reconstruct exactly.
    #[error("analytical manifest catalog is corrupt")]
    CorruptCatalog,
    /// Stored generation membership exceeds the configured reader ceiling.
    #[error("analytical generation exceeds the {max_objects}-object reader ceiling")]
    ObjectLimitExceeded { max_objects: usize },
    /// Transitive provider-capture lineage exceeds the fixed generation ceiling.
    #[error("analytical generation exceeds the {max}-provider-capture input ceiling")]
    CaptureInputLimitExceeded { max: usize },
    /// Typed market-bar history lineage disagrees with rows, capture, or immutable generation.
    #[error("complete market-bar history publication evidence is invalid")]
    MarketBarHistoryMismatch,
    /// Transitive complete-history lineage exceeds the fixed generation ceiling.
    #[error("analytical generation exceeds the {max}-market-bar-history input ceiling")]
    MarketBarHistoryInputLimitExceeded { max: usize },
    /// Candidate reachability work exceeds the explicit operation ceiling.
    #[error("analytical reference lookup exceeds the {max_candidates}-candidate work ceiling")]
    ReferenceWorkLimitExceeded { max_candidates: usize },
    /// Legacy/durable overlap work exceeded the fixed candidate ceiling.
    #[error(
        "analytical feature-dataset overlap lookup exceeds the {max_candidates}-candidate ceiling"
    )]
    FeatureDatasetCandidateLimitExceeded { max_candidates: usize },
    /// Candidate reachability was cancelled before its read snapshot completed.
    #[error("analytical reference lookup was cancelled")]
    Cancelled,
    /// Candidate reachability exceeded its elapsed-time deadline.
    #[error("analytical reference lookup deadline exceeded")]
    DeadlineExceeded,
    /// Row, byte, version, or ordinal conversion overflowed.
    #[error("analytical manifest count overflow")]
    CountOverflow,
    /// An exact-capacity immutable construction changed allocation identity.
    #[error("analytical manifest immutable allocation contract changed")]
    AllocationContract,
    /// The connection lock was poisoned.
    #[error("analytical manifest catalog lock is unavailable")]
    LockPoisoned,
    /// Pure manifest invariant failed.
    #[error("analytical manifest plan is invalid")]
    Plan(#[from] ManifestPlanError),
    /// Prepared catalog path validation failed.
    #[error("analytical catalog path is invalid")]
    Path(#[from] market_squawk_platform::PathError),
    /// SQLite rejected a transaction or retained invariant.
    #[error("analytical catalog SQLite operation failed")]
    Sqlite(#[from] rusqlite::Error),
    /// Exact catalog-file identity could not be established for analytical composition.
    #[error("analytical catalog file authority is invalid")]
    CatalogAuthority(#[from] CatalogError),
}

fn validate_generation_anchor(
    plan: &ManifestPlan,
    artifact: &ArtifactRecord,
    anchor: &DatasetManifestRecord,
    schema: &DatasetSchemaRef,
) -> Result<(), ManifestCatalogError> {
    if anchor.artifact_id() != artifact.artifact_id()
        || anchor.schema_version() != schema.version()
        || sha256_from_evidence(anchor.content_digest())? != plan.content_hash
        || sha256_from_evidence(artifact.content_digest())?
            != plan
                .objects
                .last()
                .ok_or(ManifestCatalogError::CorruptCatalog)?
                .content_hash
    {
        return Err(ManifestCatalogError::AnchorMismatch);
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "one transaction binds generation identity, source, schema, and typed history evidence"
)]
fn commit_generation_in_transaction(
    transaction: &Transaction<'_>,
    plan: &ManifestPlan,
    artifact: &ArtifactRecord,
    anchor: &DatasetManifestRecord,
    schema: &DatasetSchemaRef,
    kind: GenerationKind,
    source_input: Option<&IngestRunRecord>,
    market_bar_history: Option<&MarketBarHistoryPublicationCandidate>,
    max_objects_per_generation: usize,
) -> Result<DatasetManifestRef, ManifestCatalogError> {
    if kind == GenerationKind::Derived
        || !matches!(
            (kind, source_input),
            (GenerationKind::Ingest, Some(_)) | (GenerationKind::Compaction, None)
        )
    {
        return Err(ManifestCatalogError::GenerationConflict);
    }
    DatasetSchemaRegistry::local().resolve(schema)?;
    validate_generation_anchor(plan, artifact, anchor, schema)?;
    if let Some(existing) = manifest_for_anchor(transaction, anchor.manifest_id())? {
        let pinned = load_pinned(transaction, &existing, max_objects_per_generation)?;
        if pinned.plan == *plan
            && pinned.manifest.schema == *schema
            && pinned.generation_kind == kind
            && pinned.build_spec_digest.is_none()
            && generation_source_input_matches(transaction, &existing, kind, source_input)?
            && generation_capture_inputs_match_manifest(transaction, &existing)?
            && generation_publication_inputs_match_manifest(transaction, &existing)?
            && generation_market_bar_history_candidate_matches(
                transaction,
                &existing,
                market_bar_history,
            )?
        {
            return Ok(existing);
        }
        return Err(ManifestCatalogError::GenerationConflict);
    }
    let previous = load_latest(transaction, &plan.dataset_id, max_objects_per_generation)?;
    let current_source = source_for_artifact(transaction, artifact.artifact_id())?;
    if let Some(previous) = previous.as_ref()
        && generation_source(transaction, previous.manifest())? != current_source
    {
        return Err(ManifestCatalogError::SourceMismatch);
    }
    if previous
        .as_ref()
        .is_some_and(|value| value.manifest().schema() != schema)
    {
        return Err(ManifestCatalogError::SchemaMismatch);
    }
    let expected = match kind {
        GenerationKind::Ingest => ManifestPlan::append(
            plan.dataset_id.clone(),
            previous.as_ref().map(PinnedDataset::plan),
            plan.objects
                .last()
                .cloned()
                .ok_or(ManifestCatalogError::CorruptCatalog)?,
            max_objects_per_generation,
        )?,
        GenerationKind::Compaction => {
            let previous = previous
                .as_ref()
                .ok_or(ManifestCatalogError::GenerationConflict)?;
            ManifestPlan::compact(
                previous.plan(),
                plan.objects
                    .last()
                    .cloned()
                    .ok_or(ManifestCatalogError::CorruptCatalog)?,
            )?
        }
        GenerationKind::Derived => return Err(ManifestCatalogError::GenerationConflict),
    };
    if expected != *plan {
        return Err(ManifestCatalogError::GenerationConflict);
    }
    let version = previous_version(transaction, &plan.dataset_id)?
        .checked_add(1)
        .ok_or(ManifestCatalogError::CountOverflow)?;
    let parent = previous.as_ref().map(|previous| {
        GenerationParent::new(
            match kind {
                GenerationKind::Ingest => GenerationParentRelation::AppendPredecessor,
                GenerationKind::Compaction => GenerationParentRelation::CompactionPredecessor,
                GenerationKind::Derived => GenerationParentRelation::DerivedInput,
            },
            previous.manifest().clone(),
        )
    });
    transaction.execute(
        "INSERT INTO analytical_generations
         (dataset_id, manifest_version, content_hash, lineage_hash, row_count, total_bytes,
          schema_name, schema_version, schema_fingerprint, anchor_manifest_id,
          generation_kind, parent_count, build_spec_digest, created_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, NULL, ?13)",
        params![
            plan.dataset_id.as_str(),
            to_i64(version)?,
            plan.content_hash.bytes(),
            plan.lineage_digest.bytes(),
            to_i64(plan.row_count)?,
            to_i64(plan.total_bytes)?,
            schema.name(),
            i64::from(schema.version().get()),
            schema.fingerprint().as_slice(),
            anchor.manifest_id().to_string(),
            kind.database_name(),
            i64::from(parent.is_some()),
            anchor.created_at().unix_nanos(),
        ],
    )?;
    let generation_sequence = transaction.last_insert_rowid();
    insert_generation_source_input(transaction, generation_sequence, kind, source_input)?;
    let prior_objects = previous
        .as_ref()
        .map(PinnedDataset::objects)
        .unwrap_or_default();
    match kind {
        GenerationKind::Ingest => {
            for (ordinal, prior) in prior_objects.iter().enumerate() {
                insert_generation_object(
                    transaction,
                    &plan.dataset_id,
                    version,
                    ordinal,
                    prior.artifact_id,
                    &prior.object,
                )?;
            }
            insert_generation_object(
                transaction,
                &plan.dataset_id,
                version,
                prior_objects.len(),
                artifact.artifact_id(),
                plan.objects
                    .last()
                    .ok_or(ManifestCatalogError::CorruptCatalog)?,
            )?;
        }
        GenerationKind::Compaction => insert_generation_object(
            transaction,
            &plan.dataset_id,
            version,
            0,
            artifact.artifact_id(),
            plan.objects
                .last()
                .ok_or(ManifestCatalogError::CorruptCatalog)?,
        )?,
        GenerationKind::Derived => return Err(ManifestCatalogError::GenerationConflict),
    }
    if let Some(parent) = parent.as_ref() {
        insert_generation_parent(transaction, &plan.dataset_id, version, 0, parent)?;
    }
    propagate_generation_provider_capture_bindings(transaction, generation_sequence)?;
    propagate_generation_provider_publication_bindings(transaction, generation_sequence)?;
    insert_generation_market_bar_history_inputs(
        transaction,
        generation_sequence,
        plan,
        artifact,
        anchor,
        schema,
        source_input,
        market_bar_history,
    )?;
    DatasetManifestRef::try_new_with_schema(
        plan.dataset_id.clone(),
        version,
        schema.clone(),
        plan.content_hash,
    )
    .map_err(Into::into)
}

fn append_reference_membership(
    connection: &Connection,
    candidates: &[Sha256Digest],
    now: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
    membership: &mut Vec<bool>,
) -> Result<(), ManifestCatalogError> {
    if candidates.is_empty() || candidates.len() > REFERENCE_MEMBERSHIP_CHUNK {
        return Err(ManifestCatalogError::ReferenceWorkLimitExceeded {
            max_candidates: REFERENCE_MEMBERSHIP_CHUNK,
        });
    }
    let query_capacity = candidates
        .len()
        .checked_mul(24)
        .and_then(|value| value.checked_add(768))
        .ok_or(ManifestCatalogError::CountOverflow)?;
    let mut query = String::new();
    query
        .try_reserve_exact(query_capacity)
        .map_err(|_| ManifestCatalogError::CountOverflow)?;
    query.push_str("WITH candidates(candidate_ordinal, content_hash) AS (VALUES ");
    for ordinal in 0..candidates.len() {
        if ordinal > 0 {
            query.push(',');
        }
        write!(&mut query, "({ordinal}, ?{})", ordinal + 1)
            .map_err(|_| ManifestCatalogError::AllocationContract)?;
    }
    let now_parameter = candidates
        .len()
        .checked_add(1)
        .ok_or(ManifestCatalogError::CountOverflow)?;
    write!(
        &mut query,
        ") SELECT candidate_ordinal,
            EXISTS(
                SELECT 1 FROM analytical_generation_objects AS objects
                WHERE objects.content_hash=candidates.content_hash
            ) OR EXISTS(
                SELECT 1
                FROM query_artifact_results AS results
                JOIN query_artifact_reservations AS reservations USING (reservation_id)
                WHERE results.content_algorithm=1
                  AND results.content_digest=candidates.content_hash
                  AND reservations.state='published'
                  AND reservations.expires_at_ns>?{now_parameter}
            )
         FROM candidates ORDER BY candidate_ordinal"
    )
    .map_err(|_| ManifestCatalogError::AllocationContract)?;
    let mut statement = connection.prepare(&query)?;
    for (index, candidate) in candidates.iter().enumerate() {
        let bytes = candidate.bytes();
        statement.raw_bind_parameter(index + 1, bytes.as_slice())?;
    }
    statement.raw_bind_parameter(now_parameter, now.unix_nanos())?;
    let mut rows = statement.raw_query();
    let membership_start = membership.len();
    while let Some(row) = rows.next()? {
        check_read_operation(deadline, cancellation)?;
        let expected_ordinal = membership
            .len()
            .checked_sub(membership_start)
            .ok_or(ManifestCatalogError::CountOverflow)?;
        let ordinal = usize::try_from(row.get::<_, i64>(0)?)
            .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
        if ordinal != expected_ordinal {
            return Err(ManifestCatalogError::CorruptCatalog);
        }
        membership.push(row.get::<_, bool>(1)?);
    }
    if membership.len().saturating_sub(membership_start) != candidates.len() {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    Ok(())
}

fn check_read_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ManifestCatalogError> {
    if cancellation.is_cancelled() {
        Err(ManifestCatalogError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ManifestCatalogError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn manifest_reference_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<DatasetManifestRef, ManifestCatalogError>> {
    let dataset = row.get::<_, String>(0)?;
    let version = row.get::<_, i64>(1)?;
    let schema_name = row.get::<_, String>(2)?;
    let schema_version = row.get::<_, i64>(3)?;
    let fingerprint = row.get::<_, Vec<u8>>(4)?;
    let content = row.get::<_, Vec<u8>>(5)?;
    Ok((|| {
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(dataset.as_str())?,
            from_i64(version)?,
            parse_schema_identity(&schema_name, schema_version, &fingerprint)?,
            parse_digest(&content)?,
        )
        .map_err(ManifestCatalogError::from)
    })())
}

fn load_latest(
    connection: &Connection,
    dataset_id: &DatasetId,
    max_objects: usize,
) -> Result<Option<PinnedDataset>, ManifestCatalogError> {
    let reference = connection
        .query_row(
            "SELECT manifest_version, schema_name, schema_version, schema_fingerprint, content_hash
             FROM analytical_generations
             WHERE dataset_id=?1 ORDER BY manifest_version DESC LIMIT 1",
            [dataset_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                ))
            },
        )
        .optional()?
        .map(
            |(version, schema_name, schema_version, fingerprint, digest)| {
                DatasetManifestRef::try_new_with_schema(
                    dataset_id.clone(),
                    from_i64(version)?,
                    parse_schema_identity(&schema_name, schema_version, &fingerprint)?,
                    parse_digest(&digest)?,
                )
                .map_err(ManifestCatalogError::from)
            },
        )
        .transpose()?;
    reference
        .as_ref()
        .map(|reference| load_pinned(connection, reference, max_objects))
        .transpose()
}

pub(super) fn load_pinned(
    connection: &Connection,
    reference: &DatasetManifestRef,
    max_objects: usize,
) -> Result<PinnedDataset, ManifestCatalogError> {
    DatasetSchemaRegistry::local()
        .resolve(reference.schema())
        .map_err(|_| ManifestCatalogError::SchemaMismatch)?;
    let header = connection
        .query_row(
            "SELECT generation_sequence, content_hash, lineage_hash, row_count, total_bytes,
                    schema_name, schema_version, schema_fingerprint, generation_kind,
                    parent_count, build_spec_digest
             FROM analytical_generations WHERE dataset_id=?1 AND manifest_version=?2",
            params![
                reference.dataset_id.as_str(),
                to_i64(reference.manifest_version)?
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Vec<u8>>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, Option<Vec<u8>>>(10)?,
                ))
            },
        )
        .optional()?
        .ok_or(ManifestCatalogError::GenerationConflict)?;
    if parse_digest(&header.1)? != reference.content_hash
        || parse_schema_identity(&header.5, header.6, &header.7)? != *reference.schema()
    {
        return Err(ManifestCatalogError::GenerationConflict);
    }
    let generation_sequence = from_i64(header.0)?;
    let generation_kind = GenerationKind::from_database_name(&header.8)
        .ok_or(ManifestCatalogError::CorruptCatalog)?;
    let parent_count = usize::try_from(header.9)
        .ok()
        .filter(|count| *count <= MAX_DERIVED_GENERATION_PARENTS)
        .ok_or(ManifestCatalogError::CorruptCatalog)?;
    let build_spec_digest = header
        .10
        .as_deref()
        .map(parse_build_spec_digest)
        .transpose()?;
    let parents = load_generation_parents(
        connection,
        reference,
        generation_sequence,
        generation_kind,
        parent_count,
    )?;
    if !generation_capture_inputs_match_manifest(connection, reference)? {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    if !generation_publication_inputs_match_manifest(connection, reference)? {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    if !generation_market_bar_history_inputs_match_manifest(connection, reference)? {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let retrieval_limit = max_objects
        .checked_add(1)
        .ok_or(ManifestCatalogError::CountOverflow)?;
    let mut statement = connection.prepare(
        "SELECT objects.artifact_id, artifacts.relative_reference, objects.content_hash,
                objects.row_count, objects.size_bytes, objects.lineage_hash
         FROM analytical_generation_objects AS objects
         LEFT JOIN artifacts USING (artifact_id)
         WHERE objects.dataset_id=?1 AND objects.manifest_version=?2
         ORDER BY objects.ordinal LIMIT ?3",
    )?;
    let rows = statement.query_map(
        params![
            reference.dataset_id.as_str(),
            to_i64(reference.manifest_version)?,
            i64::try_from(retrieval_limit).map_err(|_| ManifestCatalogError::CountOverflow)?
        ],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Vec<u8>>(5)?,
            ))
        },
    )?;
    let mut staged = Vec::new();
    staged
        .try_reserve_exact(retrieval_limit)
        .map_err(|_| ManifestCatalogError::CountOverflow)?;
    if staged.capacity() != retrieval_limit {
        return Err(ManifestCatalogError::AllocationContract);
    }
    for row in rows {
        staged.push(row?);
    }
    if staged.len() > max_objects {
        return Err(ManifestCatalogError::ObjectLimitExceeded { max_objects });
    }
    if staged.is_empty() {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let object_count = staged.len();
    let mut objects = Vec::new();
    objects
        .try_reserve_exact(object_count)
        .map_err(|_| ManifestCatalogError::CountOverflow)?;
    if objects.capacity() != object_count {
        return Err(ManifestCatalogError::AllocationContract);
    }
    for (artifact_id, relative_reference, content, rows, bytes, lineage) in staged {
        objects.push(PinnedManifestObject {
            artifact_id: Uuid::parse_str(&artifact_id)
                .map_err(|_| ManifestCatalogError::CorruptCatalog)?,
            relative_reference: relative_reference
                .ok_or(ManifestCatalogError::CorruptCatalog)?
                .into_boxed_str(),
            object: ManifestObject::try_new(
                parse_digest(&content)?,
                from_i64(rows)?,
                from_i64(bytes)?,
                parse_digest(&lineage)?,
            )?,
        });
    }
    let mut plan_objects = Vec::new();
    plan_objects
        .try_reserve_exact(object_count)
        .map_err(|_| ManifestCatalogError::CountOverflow)?;
    if plan_objects.capacity() != object_count {
        return Err(ManifestCatalogError::AllocationContract);
    }
    plan_objects.extend(objects.iter().map(|value| value.object.clone()));
    let plan_allocation = plan_objects.as_ptr();
    let plan_objects = plan_objects.into_boxed_slice();
    if plan_objects.as_ptr() != plan_allocation {
        return Err(ManifestCatalogError::AllocationContract);
    }
    let plan = ManifestPlan::from_exact_objects(reference.dataset_id.clone(), plan_objects)?;
    if plan.content_hash != reference.content_hash
        || plan.lineage_digest != parse_digest(&header.2)?
        || plan.row_count != from_i64(header.3)?
        || plan.total_bytes != from_i64(header.4)?
        || (generation_kind == GenerationKind::Derived
            && ManifestPlan::derive(
                reference.dataset_id.clone(),
                plan.objects.to_vec(),
                max_objects,
            )? != plan)
        || (generation_kind == GenerationKind::Derived) != build_spec_digest.is_some()
    {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let object_allocation = objects.as_ptr();
    let objects = objects.into_boxed_slice();
    if objects.as_ptr() != object_allocation {
        return Err(ManifestCatalogError::AllocationContract);
    }
    let retained_bytes = pinned_dataset_retained_bytes(reference, &plan, &parents, &objects)?;
    Ok(PinnedDataset {
        manifest: reference.clone(),
        plan,
        generation_kind,
        build_spec_digest,
        parents,
        objects,
        retained_bytes,
    })
}

fn load_generation_parents(
    connection: &Connection,
    child: &DatasetManifestRef,
    child_sequence: u64,
    kind: GenerationKind,
    expected_count: usize,
) -> Result<Box<[GenerationParent]>, ManifestCatalogError> {
    let retrieval_limit = expected_count
        .checked_add(1)
        .ok_or(ManifestCatalogError::CountOverflow)?;
    let mut statement = connection.prepare(
        "SELECT ordinal, relation, parent_generation_sequence, parent_dataset_id,
                parent_manifest_version, parent_schema_name, parent_schema_version,
                parent_schema_fingerprint, parent_content_hash
         FROM analytical_generation_parents
         WHERE child_dataset_id=?1 AND child_manifest_version=?2
         ORDER BY ordinal LIMIT ?3",
    )?;
    let rows = statement.query_map(
        params![
            child.dataset_id().as_str(),
            to_i64(child.manifest_version())?,
            i64::try_from(retrieval_limit).map_err(|_| ManifestCatalogError::CountOverflow)?,
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, Vec<u8>>(7)?,
                row.get::<_, Vec<u8>>(8)?,
            ))
        },
    )?;
    let mut parents = Vec::new();
    parents
        .try_reserve_exact(retrieval_limit)
        .map_err(|_| ManifestCatalogError::CountOverflow)?;
    for row in rows {
        let (
            ordinal,
            relation,
            parent_sequence,
            dataset,
            version,
            schema_name,
            schema_version,
            schema_fingerprint,
            content_hash,
        ) = row?;
        if usize::try_from(ordinal).ok() != Some(parents.len())
            || from_i64(parent_sequence)? >= child_sequence
        {
            return Err(ManifestCatalogError::CorruptCatalog);
        }
        let manifest = DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(dataset.as_str())?,
            from_i64(version)?,
            parse_schema_identity(&schema_name, schema_version, &schema_fingerprint)?,
            parse_digest(&content_hash)?,
        )?;
        parents.push(GenerationParent::new(
            GenerationParentRelation::from_database_name(&relation)
                .ok_or(ManifestCatalogError::CorruptCatalog)?,
            manifest,
        ));
    }
    if parents.len() != expected_count
        || !valid_parent_semantics(child, kind, &parents)
        || (kind == GenerationKind::Derived
            && parents
                .windows(2)
                .any(|pair| compare_manifest_refs(pair[0].manifest(), pair[1].manifest()).is_ge()))
    {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    Ok(parents.into_boxed_slice())
}

fn valid_parent_semantics(
    child: &DatasetManifestRef,
    kind: GenerationKind,
    parents: &[GenerationParent],
) -> bool {
    match kind {
        GenerationKind::Ingest if child.manifest_version() == 1 => parents.is_empty(),
        GenerationKind::Ingest => {
            predecessor_matches(child, parents, GenerationParentRelation::AppendPredecessor)
        }
        GenerationKind::Compaction => predecessor_matches(
            child,
            parents,
            GenerationParentRelation::CompactionPredecessor,
        ),
        GenerationKind::Derived => {
            !parents.is_empty()
                && parents
                    .iter()
                    .all(|parent| parent.relation() == GenerationParentRelation::DerivedInput)
        }
    }
}

fn predecessor_matches(
    child: &DatasetManifestRef,
    parents: &[GenerationParent],
    relation: GenerationParentRelation,
) -> bool {
    let Some(parent) = parents.first().filter(|_| parents.len() == 1) else {
        return false;
    };
    parent.relation() == relation
        && parent.manifest().dataset_id() == child.dataset_id()
        && parent.manifest().manifest_version().checked_add(1) == Some(child.manifest_version())
        && parent.manifest().schema() == child.schema()
}

fn pinned_dataset_retained_bytes(
    manifest: &DatasetManifestRef,
    plan: &ManifestPlan,
    parents: &[GenerationParent],
    objects: &[PinnedManifestObject],
) -> Result<usize, ManifestCatalogError> {
    let inline_objects = objects
        .len()
        .checked_mul(size_of::<PinnedManifestObject>())
        .ok_or(ManifestCatalogError::CountOverflow)?;
    let plan_objects = plan
        .objects()
        .len()
        .checked_mul(size_of::<ManifestObject>())
        .ok_or(ManifestCatalogError::CountOverflow)?;
    let inline_parents = parents
        .len()
        .checked_mul(size_of::<GenerationParent>())
        .ok_or(ManifestCatalogError::CountOverflow)?;
    objects.iter().try_fold(
        size_of::<PinnedDataset>()
            .checked_add(inline_objects)
            .and_then(|value| value.checked_add(plan_objects))
            .and_then(|value| value.checked_add(inline_parents))
            .and_then(|value| value.checked_add(manifest.dataset_id().as_str().len()))
            .and_then(|value| value.checked_add(manifest.schema().name().len()))
            .and_then(|value| value.checked_add(plan.dataset_id().as_str().len()))
            .and_then(|value| {
                parents.iter().try_fold(value, |retained, parent| {
                    retained
                        .checked_add(parent.manifest().dataset_id().as_str().len())
                        .and_then(|value| {
                            value.checked_add(parent.manifest().schema().name().len())
                        })
                })
            })
            .ok_or(ManifestCatalogError::CountOverflow)?,
        |total, object| {
            total
                .checked_add(object.relative_reference().len())
                .ok_or(ManifestCatalogError::CountOverflow)
        },
    )
}

fn manifest_for_anchor(
    connection: &Connection,
    anchor: Uuid,
) -> Result<Option<DatasetManifestRef>, ManifestCatalogError> {
    connection
        .query_row(
            "SELECT dataset_id, manifest_version, schema_name, schema_version,
                    schema_fingerprint, content_hash
             FROM analytical_generations
             WHERE anchor_manifest_id=?1",
            [anchor.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                ))
            },
        )
        .optional()?
        .map(
            |(dataset, version, schema_name, schema_version, fingerprint, digest)| {
                DatasetManifestRef::try_new_with_schema(
                    DatasetId::try_from(dataset.as_str())?,
                    from_i64(version)?,
                    parse_schema_identity(&schema_name, schema_version, &fingerprint)?,
                    parse_digest(&digest)?,
                )
                .map_err(ManifestCatalogError::from)
            },
        )
        .transpose()
}

fn insert_generation_source_input(
    transaction: &rusqlite::Transaction<'_>,
    generation_sequence: i64,
    kind: GenerationKind,
    source_input: Option<&IngestRunRecord>,
) -> Result<(), ManifestCatalogError> {
    match (kind, source_input) {
        (GenerationKind::Ingest, Some(source_input))
            if source_input.operation() == SourceOperation::Persist =>
        {
            if generation_sequence <= 0 {
                return Err(ManifestCatalogError::CorruptCatalog);
            }
            transaction.execute(
                "INSERT INTO analytical_generation_source_inputs
                 (generation_sequence, run_id, source_id, rights_id)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    generation_sequence,
                    source_input.run_id().to_string(),
                    source_input.source_id().as_str(),
                    source_input.rights_id(),
                ],
            )?;
            Ok(())
        }
        (GenerationKind::Compaction, None) => Ok(()),
        (GenerationKind::Ingest | GenerationKind::Compaction | GenerationKind::Derived, _) => {
            Err(ManifestCatalogError::GenerationConflict)
        }
    }
}

fn generation_source_input_matches(
    connection: &Connection,
    manifest: &DatasetManifestRef,
    kind: GenerationKind,
    source_input: Option<&IngestRunRecord>,
) -> Result<bool, ManifestCatalogError> {
    let exists = match (kind, source_input) {
        (GenerationKind::Ingest, Some(source_input))
            if source_input.operation() == SourceOperation::Persist =>
        {
            connection.query_row(
                "SELECT EXISTS(
                    SELECT 1
                    FROM analytical_generations AS generation
                    JOIN analytical_generation_source_inputs AS source_input
                      ON source_input.generation_sequence=generation.generation_sequence
                    WHERE generation.dataset_id=?1 AND generation.manifest_version=?2
                      AND source_input.run_id=?3 AND source_input.source_id=?4
                      AND source_input.rights_id=?5
                 )",
                params![
                    manifest.dataset_id().as_str(),
                    to_i64(manifest.manifest_version())?,
                    source_input.run_id().to_string(),
                    source_input.source_id().as_str(),
                    source_input.rights_id(),
                ],
                |row| row.get(0),
            )?
        }
        (GenerationKind::Compaction, None) => !connection.query_row(
            "SELECT EXISTS(
                SELECT 1
                FROM analytical_generations AS generation
                JOIN analytical_generation_source_inputs AS source_input
                  ON source_input.generation_sequence=generation.generation_sequence
                WHERE generation.dataset_id=?1 AND generation.manifest_version=?2
             )",
            params![
                manifest.dataset_id().as_str(),
                to_i64(manifest.manifest_version())?
            ],
            |row| row.get::<_, bool>(0),
        )?,
        (GenerationKind::Ingest | GenerationKind::Compaction | GenerationKind::Derived, _) => false,
    };
    Ok(exists)
}

pub(crate) fn propagate_generation_provider_capture_bindings(
    transaction: &Transaction<'_>,
    generation_sequence: i64,
) -> Result<(), ManifestCatalogError> {
    if generation_sequence <= 0 {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let kind: String = transaction.query_row(
        "SELECT generation_kind FROM analytical_generations WHERE generation_sequence=?1",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let expected =
        expected_generation_capture_input_count(transaction, generation_sequence, &kind)?;
    let expected = usize::try_from(expected)
        .ok()
        .filter(|count| *count <= MAX_GENERATION_CAPTURE_INPUTS)
        .ok_or(ManifestCatalogError::CaptureInputLimitExceeded {
            max: MAX_GENERATION_CAPTURE_INPUTS,
        })?;
    let inserted = match kind.as_str() {
        "ingest" | "compaction" | "derived" => transaction.execute(
            "INSERT INTO analytical_generation_provider_capture_bindings
             (generation_sequence, input_ordinal, binding_digest, run_id, source_id)
             WITH candidates AS (
                 SELECT capture_input.binding_digest,
                        capture_input.run_id,
                        capture_input.source_id
                 FROM analytical_generation_source_inputs AS source_input
                 JOIN ingest_run_provider_capture_bindings AS capture_input USING (run_id)
                 WHERE source_input.generation_sequence=?1
                 UNION
                 SELECT parent_input.binding_digest,
                        parent_input.run_id,
                        parent_input.source_id
                 FROM analytical_generation_parents AS edge
                 JOIN analytical_generation_provider_capture_bindings AS parent_input
                   ON parent_input.generation_sequence=edge.parent_generation_sequence
                 JOIN analytical_generations AS child
                   ON child.dataset_id=edge.child_dataset_id
                  AND child.manifest_version=edge.child_manifest_version
                 WHERE child.generation_sequence=?1
             )
             SELECT ?1,
                    ROW_NUMBER() OVER (
                        ORDER BY binding_digest, run_id, source_id
                    ) - 1,
                    binding_digest, run_id, source_id
             FROM candidates
             ORDER BY binding_digest, run_id, source_id
             LIMIT ?2",
            params![
                generation_sequence,
                i64::try_from(MAX_GENERATION_CAPTURE_INPUTS)
                    .map_err(|_| ManifestCatalogError::CountOverflow)?
            ],
        )?,
        _ => return Err(ManifestCatalogError::CorruptCatalog),
    };
    if expected != inserted {
        return Err(ManifestCatalogError::GenerationConflict);
    }
    Ok(())
}

pub(crate) fn propagate_generation_provider_publication_bindings(
    transaction: &Transaction<'_>,
    generation_sequence: i64,
) -> Result<(), ManifestCatalogError> {
    if generation_sequence <= 0 {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let kind: String = transaction.query_row(
        "SELECT generation_kind FROM analytical_generations WHERE generation_sequence=?1",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let expected =
        expected_generation_publication_input_count(transaction, generation_sequence, &kind)?;
    let expected = usize::try_from(expected)
        .ok()
        .filter(|count| *count <= MAX_GENERATION_CAPTURE_INPUTS)
        .ok_or(ManifestCatalogError::CaptureInputLimitExceeded {
            max: MAX_GENERATION_CAPTURE_INPUTS,
        })?;
    let inserted = match kind.as_str() {
        "ingest" | "compaction" | "derived" => transaction.execute(
            "INSERT INTO analytical_generation_provider_publication_bindings
             (generation_sequence, input_ordinal, publication_digest, publication_kind,
              run_id, source_id)
             WITH candidates AS (
                 SELECT publication.publication_digest,
                        publication.publication_kind,
                        publication.run_id,
                        publication.source_id
                 FROM analytical_generation_source_inputs AS source_input
                 JOIN ingest_run_provider_publication_bindings AS publication USING (run_id)
                 WHERE source_input.generation_sequence=?1
                 UNION
                 SELECT parent_input.publication_digest,
                        parent_input.publication_kind,
                        parent_input.run_id,
                        parent_input.source_id
                 FROM analytical_generation_parents AS edge
                 JOIN analytical_generation_provider_publication_bindings AS parent_input
                   ON parent_input.generation_sequence=edge.parent_generation_sequence
                 JOIN analytical_generations AS child
                   ON child.dataset_id=edge.child_dataset_id
                  AND child.manifest_version=edge.child_manifest_version
                 WHERE child.generation_sequence=?1
             )
             SELECT ?1,
                    ROW_NUMBER() OVER (
                        ORDER BY publication_digest, publication_kind, run_id, source_id
                    ) - 1,
                    publication_digest, publication_kind, run_id, source_id
             FROM candidates
             ORDER BY publication_digest, publication_kind, run_id, source_id
             LIMIT ?2",
            params![
                generation_sequence,
                i64::try_from(MAX_GENERATION_CAPTURE_INPUTS)
                    .map_err(|_| ManifestCatalogError::CountOverflow)?,
            ],
        )?,
        _ => return Err(ManifestCatalogError::CorruptCatalog),
    };
    if expected != inserted {
        return Err(ManifestCatalogError::GenerationConflict);
    }
    Ok(())
}

fn expected_generation_publication_input_count(
    transaction: &Connection,
    generation_sequence: i64,
    kind: &str,
) -> Result<i64, ManifestCatalogError> {
    match kind {
        "ingest" | "compaction" | "derived" => Ok(transaction.query_row(
            "WITH candidates AS (
                 SELECT publication.publication_digest
                 FROM analytical_generation_source_inputs AS source_input
                 JOIN ingest_run_provider_publication_bindings AS publication USING (run_id)
                 WHERE source_input.generation_sequence=?1
                 UNION
                 SELECT parent_input.publication_digest
                 FROM analytical_generation_parents AS edge
                 JOIN analytical_generation_provider_publication_bindings AS parent_input
                   ON parent_input.generation_sequence=edge.parent_generation_sequence
                 JOIN analytical_generations AS child
                   ON child.dataset_id=edge.child_dataset_id
                  AND child.manifest_version=edge.child_manifest_version
                 WHERE child.generation_sequence=?1
             )
             SELECT COUNT(*) FROM candidates",
            [generation_sequence],
            |row| row.get(0),
        )?),
        _ => Err(ManifestCatalogError::CorruptCatalog),
    }
}

fn expected_generation_capture_input_count(
    transaction: &Connection,
    generation_sequence: i64,
    kind: &str,
) -> Result<i64, ManifestCatalogError> {
    match kind {
        "ingest" | "compaction" | "derived" => Ok(transaction.query_row(
            "WITH candidates AS (
                 SELECT capture_input.binding_digest
                 FROM analytical_generation_source_inputs AS source_input
                 JOIN ingest_run_provider_capture_bindings AS capture_input USING (run_id)
                 WHERE source_input.generation_sequence=?1
                 UNION
                 SELECT parent_input.binding_digest
                 FROM analytical_generation_parents AS edge
                 JOIN analytical_generation_provider_capture_bindings AS parent_input
                   ON parent_input.generation_sequence=edge.parent_generation_sequence
                 JOIN analytical_generations AS child
                   ON child.dataset_id=edge.child_dataset_id
                  AND child.manifest_version=edge.child_manifest_version
                 WHERE child.generation_sequence=?1
             )
             SELECT COUNT(*) FROM candidates",
            [generation_sequence],
            |row| row.get(0),
        )?),
        _ => Err(ManifestCatalogError::CorruptCatalog),
    }
}

fn generation_capture_inputs_match_manifest(
    transaction: &Connection,
    manifest: &DatasetManifestRef,
) -> Result<bool, ManifestCatalogError> {
    let (sequence, kind): (i64, String) = transaction.query_row(
        "SELECT generation_sequence, generation_kind FROM analytical_generations
         WHERE dataset_id=?1 AND manifest_version=?2",
        params![
            manifest.dataset_id().as_str(),
            to_i64(manifest.manifest_version())?
        ],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let expected = expected_generation_capture_input_count(transaction, sequence, &kind)?;
    let ordinal_shape: bool = transaction.query_row(
        "SELECT COUNT(*)=0
             OR (
                 MIN(input_ordinal)=0
                 AND MAX(input_ordinal)=COUNT(*)-1
             )
         FROM analytical_generation_provider_capture_bindings
         WHERE generation_sequence=?1",
        [sequence],
        |row| row.get(0),
    )?;
    let exact: bool = transaction.query_row(
        "WITH candidates AS (
             SELECT capture_input.binding_digest,
                    capture_input.run_id,
                    capture_input.source_id
             FROM analytical_generation_source_inputs AS source_input
             JOIN ingest_run_provider_capture_bindings AS capture_input USING (run_id)
             WHERE source_input.generation_sequence=?1
             UNION
             SELECT parent_input.binding_digest,
                    parent_input.run_id,
                    parent_input.source_id
             FROM analytical_generation_parents AS edge
             JOIN analytical_generation_provider_capture_bindings AS parent_input
               ON parent_input.generation_sequence=edge.parent_generation_sequence
             JOIN analytical_generations AS child
               ON child.dataset_id=edge.child_dataset_id
              AND child.manifest_version=edge.child_manifest_version
             WHERE child.generation_sequence=?1
         ),
         expected AS (
             SELECT ROW_NUMBER() OVER (
                        ORDER BY binding_digest, run_id, source_id
                    ) - 1 AS input_ordinal,
                    binding_digest, run_id, source_id
             FROM candidates
         ),
         actual AS (
             SELECT input_ordinal, binding_digest, run_id, source_id
             FROM analytical_generation_provider_capture_bindings
             WHERE generation_sequence=?1
         )
         SELECT NOT EXISTS(SELECT * FROM expected EXCEPT SELECT * FROM actual)
            AND NOT EXISTS(SELECT * FROM actual EXCEPT SELECT * FROM expected)",
        [sequence],
        |row| row.get(0),
    )?;
    Ok(exact
        && ordinal_shape
        && expected >= 0
        && usize::try_from(expected).is_ok_and(|count| count <= MAX_GENERATION_CAPTURE_INPUTS))
}

fn generation_publication_inputs_match_manifest(
    transaction: &Connection,
    manifest: &DatasetManifestRef,
) -> Result<bool, ManifestCatalogError> {
    let (sequence, kind): (i64, String) = transaction.query_row(
        "SELECT generation_sequence, generation_kind FROM analytical_generations
         WHERE dataset_id=?1 AND manifest_version=?2",
        params![
            manifest.dataset_id().as_str(),
            to_i64(manifest.manifest_version())?,
        ],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let expected = expected_generation_publication_input_count(transaction, sequence, &kind)?;
    let ordinal_shape: bool = transaction.query_row(
        "SELECT COUNT(*)=0 OR (MIN(input_ordinal)=0 AND MAX(input_ordinal)=COUNT(*)-1)
         FROM analytical_generation_provider_publication_bindings
         WHERE generation_sequence=?1",
        [sequence],
        |row| row.get(0),
    )?;
    let exact: bool = transaction.query_row(
        "WITH candidates AS (
             SELECT publication.publication_digest,
                    publication.publication_kind,
                    publication.run_id,
                    publication.source_id
             FROM analytical_generation_source_inputs AS source_input
             JOIN ingest_run_provider_publication_bindings AS publication USING (run_id)
             WHERE source_input.generation_sequence=?1
             UNION
             SELECT parent_input.publication_digest,
                    parent_input.publication_kind,
                    parent_input.run_id,
                    parent_input.source_id
             FROM analytical_generation_parents AS edge
             JOIN analytical_generation_provider_publication_bindings AS parent_input
               ON parent_input.generation_sequence=edge.parent_generation_sequence
             JOIN analytical_generations AS child
               ON child.dataset_id=edge.child_dataset_id
              AND child.manifest_version=edge.child_manifest_version
             WHERE child.generation_sequence=?1
         ),
         expected AS (
             SELECT ROW_NUMBER() OVER (
                        ORDER BY publication_digest, publication_kind, run_id, source_id
                    ) - 1 AS input_ordinal,
                    publication_digest, publication_kind, run_id, source_id
             FROM candidates
         ),
         actual AS (
             SELECT input_ordinal, publication_digest, publication_kind, run_id, source_id
             FROM analytical_generation_provider_publication_bindings
             WHERE generation_sequence=?1
         )
         SELECT NOT EXISTS(SELECT * FROM expected EXCEPT SELECT * FROM actual)
            AND NOT EXISTS(SELECT * FROM actual EXCEPT SELECT * FROM expected)",
        [sequence],
        |row| row.get(0),
    )?;
    Ok(exact
        && ordinal_shape
        && expected >= 0
        && usize::try_from(expected).is_ok_and(|count| count <= MAX_GENERATION_CAPTURE_INPUTS))
}

fn generation_source(
    connection: &Connection,
    manifest: &DatasetManifestRef,
) -> Result<SourceId, ManifestCatalogError> {
    let source: String = connection
        .query_row(
            "SELECT runs.source_id
             FROM analytical_generations AS generations
             JOIN dataset_manifests AS manifests
               ON manifests.manifest_id=generations.anchor_manifest_id
             JOIN artifacts ON artifacts.artifact_id=manifests.artifact_id
             JOIN ingest_runs AS runs ON runs.run_id=artifacts.run_id
             WHERE generations.dataset_id=?1 AND generations.manifest_version=?2",
            params![
                manifest.dataset_id().as_str(),
                to_i64(manifest.manifest_version())?
            ],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(ManifestCatalogError::GenerationConflict)?;
    SourceId::try_from(source.as_str()).map_err(|_| ManifestCatalogError::CorruptCatalog)
}

fn generation_python_export(
    connection: &Connection,
    manifest: &DatasetManifestRef,
) -> Result<Option<Sha256Digest>, ManifestCatalogError> {
    connection
        .query_row(
            "SELECT export_sha256
             FROM feature_dataset_production_admissions
             WHERE dataset_id=?1 AND manifest_version=?2",
            params![
                manifest.dataset_id().as_str(),
                to_i64(manifest.manifest_version())?
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?
        .map(|digest| parse_digest(&digest))
        .transpose()
}

fn source_for_artifact(
    connection: &Connection,
    artifact_id: Uuid,
) -> Result<SourceId, ManifestCatalogError> {
    let source: String = connection
        .query_row(
            "SELECT runs.source_id FROM artifacts
             JOIN ingest_runs AS runs USING (run_id)
             WHERE artifacts.artifact_id=?1",
            [artifact_id.to_string()],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(ManifestCatalogError::AnchorMismatch)?;
    SourceId::try_from(source.as_str()).map_err(|_| ManifestCatalogError::CorruptCatalog)
}

fn previous_version(
    connection: &Connection,
    dataset_id: &DatasetId,
) -> Result<u64, ManifestCatalogError> {
    let value: Option<i64> = connection.query_row(
        "SELECT MAX(manifest_version) FROM analytical_generations WHERE dataset_id=?1",
        [dataset_id.as_str()],
        |row| row.get(0),
    )?;
    value
        .map(from_i64)
        .transpose()
        .map(|value| value.unwrap_or(0))
}

fn insert_generation_object(
    connection: &Connection,
    dataset_id: &DatasetId,
    version: u64,
    ordinal: usize,
    artifact_id: Uuid,
    object: &ManifestObject,
) -> Result<(), ManifestCatalogError> {
    connection.execute(
        "INSERT INTO analytical_generation_objects
         (dataset_id, manifest_version, ordinal, artifact_id, content_hash,
          row_count, size_bytes, lineage_hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            dataset_id.as_str(),
            to_i64(version)?,
            i64::try_from(ordinal).map_err(|_| ManifestCatalogError::CountOverflow)?,
            artifact_id.to_string(),
            object.content_hash.bytes(),
            to_i64(object.row_count)?,
            to_i64(object.size_bytes)?,
            object.lineage_digest.bytes(),
        ],
    )?;
    Ok(())
}

fn insert_generation_parent(
    connection: &Connection,
    child_dataset_id: &DatasetId,
    child_version: u64,
    ordinal: usize,
    parent: &GenerationParent,
) -> Result<(), ManifestCatalogError> {
    let parent_sequence: i64 = connection
        .query_row(
            "SELECT generation_sequence
             FROM analytical_generations
             WHERE dataset_id=?1 AND manifest_version=?2
               AND schema_name=?3 AND schema_version=?4
               AND schema_fingerprint=?5 AND content_hash=?6",
            params![
                parent.manifest().dataset_id().as_str(),
                to_i64(parent.manifest().manifest_version())?,
                parent.manifest().schema().name(),
                i64::from(parent.manifest().schema_version().get()),
                parent.manifest().schema().fingerprint().as_slice(),
                parent.manifest().content_hash().bytes(),
            ],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(ManifestCatalogError::GenerationConflict)?;
    connection.execute(
        "INSERT INTO analytical_generation_parents
         (child_dataset_id, child_manifest_version, ordinal, relation,
          parent_generation_sequence, parent_dataset_id, parent_manifest_version,
          parent_schema_name, parent_schema_version, parent_schema_fingerprint,
          parent_content_hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            child_dataset_id.as_str(),
            to_i64(child_version)?,
            i64::try_from(ordinal).map_err(|_| ManifestCatalogError::CountOverflow)?,
            parent.relation().database_name(),
            parent_sequence,
            parent.manifest().dataset_id().as_str(),
            to_i64(parent.manifest().manifest_version())?,
            parent.manifest().schema().name(),
            i64::from(parent.manifest().schema_version().get()),
            parent.manifest().schema().fingerprint().as_slice(),
            parent.manifest().content_hash().bytes(),
        ],
    )?;
    Ok(())
}

#[allow(
    dead_code,
    reason = "used only by the sealed derived commit pending its ResearchUse-authorized caller"
)]
fn require_exact_generation(
    connection: &Connection,
    manifest: &DatasetManifestRef,
) -> Result<(), ManifestCatalogError> {
    DatasetSchemaRegistry::local()
        .resolve(manifest.schema())
        .map_err(|_| ManifestCatalogError::SchemaMismatch)?;
    let exists: bool = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM analytical_generations
             WHERE dataset_id=?1 AND manifest_version=?2
               AND schema_name=?3 AND schema_version=?4
               AND schema_fingerprint=?5 AND content_hash=?6
         )",
        params![
            manifest.dataset_id().as_str(),
            to_i64(manifest.manifest_version())?,
            manifest.schema().name(),
            i64::from(manifest.schema_version().get()),
            manifest.schema().fingerprint().as_slice(),
            manifest.content_hash().bytes(),
        ],
        |row| row.get(0),
    )?;
    if exists {
        Ok(())
    } else {
        Err(ManifestCatalogError::GenerationConflict)
    }
}

#[allow(
    dead_code,
    reason = "used only by the sealed derived commit pending its ResearchUse-authorized caller"
)]
fn require_exact_artifact(
    connection: &Connection,
    artifact: &ArtifactRecord,
) -> Result<(), ManifestCatalogError> {
    let (algorithm, digest) = match artifact.content_digest().algorithm() {
        DigestAlgorithm::Sha256 => (1_i64, artifact.content_digest().bytes()),
        DigestAlgorithm::Blake3 => (2_i64, artifact.content_digest().bytes()),
    };
    let exists: bool = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM artifacts
             WHERE artifact_id=?1 AND relative_reference=?2
               AND content_algorithm=?3 AND content_digest=?4
               AND size_bytes=?5 AND created_at_ns=?6
         )",
        params![
            artifact.artifact_id().to_string(),
            artifact.relative_reference(),
            algorithm,
            digest,
            to_i64(artifact.size_bytes())?,
            artifact.created_at().unix_nanos(),
        ],
        |row| row.get(0),
    )?;
    if exists {
        Ok(())
    } else {
        Err(ManifestCatalogError::AnchorMismatch)
    }
}

#[allow(
    dead_code,
    reason = "used only by the sealed derived commit pending its ResearchUse-authorized caller"
)]
fn require_exact_anchor(
    connection: &Connection,
    anchor: &DatasetManifestRecord,
) -> Result<(), ManifestCatalogError> {
    let (algorithm, digest) = match anchor.content_digest().algorithm() {
        DigestAlgorithm::Sha256 => (1_i64, anchor.content_digest().bytes()),
        DigestAlgorithm::Blake3 => (2_i64, anchor.content_digest().bytes()),
    };
    let exists: bool = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM dataset_manifests
             WHERE manifest_id=?1 AND dataset_name=?2 AND schema_version=?3
               AND artifact_id=?4 AND content_algorithm=?5 AND content_digest=?6
               AND created_at_ns=?7
         )",
        params![
            anchor.manifest_id().to_string(),
            anchor.dataset_name().as_str(),
            i64::from(anchor.schema_version().get()),
            anchor.artifact_id().to_string(),
            algorithm,
            digest,
            anchor.created_at().unix_nanos(),
        ],
        |row| row.get(0),
    )?;
    if exists {
        Ok(())
    } else {
        Err(ManifestCatalogError::AnchorMismatch)
    }
}

fn sha256_from_evidence(value: EvidenceDigest) -> Result<Sha256Digest, ManifestCatalogError> {
    if !matches!(value.algorithm(), DigestAlgorithm::Sha256) {
        return Err(ManifestCatalogError::AnchorMismatch);
    }
    Ok(Sha256Digest::new(value.bytes()))
}

fn parse_digest(value: &[u8]) -> Result<Sha256Digest, ManifestCatalogError> {
    let bytes: [u8; 32] = value
        .try_into()
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    Ok(Sha256Digest::new(bytes))
}

fn parse_build_spec_digest(value: &[u8]) -> Result<DatasetBuildSpecDigest, ManifestCatalogError> {
    let bytes: [u8; 32] = value
        .try_into()
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    DatasetBuildSpecDigest::try_new(bytes).map_err(|_| ManifestCatalogError::CorruptCatalog)
}

fn parse_schema_identity(
    name: &str,
    version: i64,
    fingerprint: &[u8],
) -> Result<DatasetSchemaRef, ManifestCatalogError> {
    let version = parse_schema_version(version)?;
    let fingerprint: [u8; 32] = fingerprint
        .try_into()
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let schema = DatasetSchemaRef::try_new(name, version, fingerprint)
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    DatasetSchemaRegistry::local()
        .resolve(&schema)
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    Ok(schema)
}

fn parse_schema_version(
    value: i64,
) -> Result<market_squawk_domain::SchemaVersion, ManifestCatalogError> {
    let value = u16::try_from(value).map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    market_squawk_domain::SchemaVersion::new(value)
        .map_err(|_| ManifestCatalogError::CorruptCatalog)
}

fn to_i64(value: u64) -> Result<i64, ManifestCatalogError> {
    i64::try_from(value).map_err(|_| ManifestCatalogError::CountOverflow)
}

fn from_i64(value: i64) -> Result<u64, ManifestCatalogError> {
    u64::try_from(value).map_err(|_| ManifestCatalogError::CorruptCatalog)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fmt::Write as _;
    use std::time::{Duration, Instant};

    use market_squawk_domain::{
        DigestAlgorithm, EvidenceDigest, SourceId, SourceIdentifier, Timestamp,
    };
    use market_squawk_platform::LocalPaths;
    use rusqlite::{Connection, params};
    use tokio_util::sync::CancellationToken;

    use super::{
        AnalyticalManifestCatalog, DerivedGenerationCommitAuthority, ManifestCatalogError,
    };
    use crate::authority_transition::evidence::{EvidenceLimits, EvidenceSnapshotRequest};
    use crate::manifest::{DatasetBuildSpecDigest, DerivedGenerationParents};
    use crate::rights::SourceRightsDecision;
    use crate::{
        ArtifactRecord, CatalogAuthority, CatalogConfig, CatalogLimit, CatalogResultLimits,
        DatasetId, DatasetManifestRecord, DatasetManifestRef, DatasetSchemaRef,
        DatasetSchemaRegistry, GenerationKind, ManifestObject, ManifestPlan, RightsBasis,
        RightsDecisionInput, Sha256Digest, SourceOperation,
    };

    type TestResult = Result<(), Box<dyn Error>>;
    const FIXTURE_SOURCE_ID: &str = "derived-lineage-fixture";

    #[test]
    fn append_rejects_mixed_row_schema_before_planning() -> TestResult {
        let registry = DatasetSchemaRegistry::local();
        let research = registry.canonical_research_observations()?;
        let feature_labels = registry.canonical_feature_labels()?;
        let (_directory, location) = migrated_catalog()?;
        let dataset = DatasetId::try_from("schema-bound")?;
        let prior_object =
            ManifestObject::try_new(Sha256Digest::new([1; 32]), 1, 1, Sha256Digest::new([2; 32]))?;
        let prior_plan = ManifestPlan::append(dataset.clone(), None, prior_object.clone(), 8)?;
        let artifact_id = uuid::Uuid::new_v4();
        let connection = Connection::open(location.path())?;
        connection.pragma_update(None, "foreign_keys", false)?;
        connection.execute(
            "INSERT INTO artifacts
             (artifact_id, run_id, relative_reference, content_algorithm, content_digest,
              size_bytes, created_at_ns)
             VALUES (?1, ?2, 'objects/fixture.parquet', 1, ?3, 1, 1)",
            params![
                artifact_id.to_string(),
                uuid::Uuid::new_v4().to_string(),
                prior_object.content_hash().bytes().as_slice(),
            ],
        )?;
        connection.execute(
            "INSERT INTO analytical_generations
             (dataset_id, manifest_version, content_hash, lineage_hash, row_count, total_bytes,
              schema_name, schema_version, schema_fingerprint, anchor_manifest_id,
              generation_kind, parent_count, build_spec_digest, created_at_ns)
             VALUES (?1, 1, ?2, ?3, 1, 1, ?4, ?5, ?6, ?7, 'ingest', 0, NULL, 1)",
            params![
                dataset.as_str(),
                prior_plan.content_hash().bytes().as_slice(),
                prior_plan.lineage_digest().bytes().as_slice(),
                research.name(),
                i64::from(research.version().get()),
                research.fingerprint().as_slice(),
                uuid::Uuid::new_v4().to_string(),
            ],
        )?;
        connection.execute(
            "INSERT INTO analytical_generation_objects
             (dataset_id, manifest_version, ordinal, artifact_id, content_hash, row_count,
              size_bytes, lineage_hash)
             VALUES (?1, 1, 0, ?2, ?3, 1, 1, ?4)",
            params![
                dataset.as_str(),
                artifact_id.to_string(),
                prior_object.content_hash().bytes().as_slice(),
                prior_object.lineage_digest().bytes().as_slice(),
            ],
        )?;
        drop(connection);
        let catalog = AnalyticalManifestCatalog::open(&location, 8)?;

        assert!(matches!(
            catalog.validate_append_schema(&dataset, &feature_labels),
            Err(ManifestCatalogError::SchemaMismatch)
        ));
        Ok(())
    }

    #[test]
    fn pinned_read_rejects_max_plus_one_objects_before_reconstruction() -> TestResult {
        let (_directory, location) = migrated_catalog()?;
        let schema = DatasetSchemaRegistry::local().canonical_research_observations()?;
        let connection = Connection::open(location.path())?;
        connection.pragma_update(None, "foreign_keys", false)?;
        connection.execute(
            "INSERT INTO analytical_generations
             (dataset_id, manifest_version, content_hash, lineage_hash, row_count, total_bytes,
              schema_name, schema_version, schema_fingerprint, anchor_manifest_id,
              generation_kind, parent_count, build_spec_digest, created_at_ns)
             VALUES ('over-limit', 1, ?1, ?2, 2, 2, ?3, ?4, ?5, ?6, 'ingest', 0, NULL, 1)",
            params![
                [7_u8; 32].as_slice(),
                [8_u8; 32].as_slice(),
                schema.name(),
                i64::from(schema.version().get()),
                schema.fingerprint().as_slice(),
                uuid::Uuid::new_v4().to_string()
            ],
        )?;
        for ordinal in 0_i64..2 {
            connection.execute(
                "INSERT INTO analytical_generation_objects
                 (dataset_id, manifest_version, ordinal, artifact_id, content_hash, row_count,
                  size_bytes, lineage_hash)
                 VALUES ('over-limit', 1, ?1, ?2, ?3, 1, 1, ?4)",
                params![
                    ordinal,
                    uuid::Uuid::new_v4().to_string(),
                    [ordinal as u8; 32].as_slice(),
                    [8_u8; 32].as_slice()
                ],
            )?;
        }
        drop(connection);
        let catalog = AnalyticalManifestCatalog::open(&location, 1)?;
        let manifest = DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from("over-limit")?,
            1,
            schema,
            Sha256Digest::new([7; 32]),
        )?;

        assert!(matches!(
            catalog.pinned(&manifest),
            Err(ManifestCatalogError::ObjectLimitExceeded { max_objects: 1 })
        ));
        Ok(())
    }

    #[test]
    fn derived_commit_retains_canonical_multi_object_and_parent_evidence() -> TestResult {
        let (_directory, location) = migrated_catalog()?;
        let schema = DatasetSchemaRegistry::local().canonical_research_observations()?;
        let connection = Connection::open(location.path())?;
        connection.pragma_update(None, "foreign_keys", false)?;
        insert_control_fixture(&connection)?;

        let first_parent = insert_ingest_fixture(
            &connection,
            &schema,
            "inputs.second",
            ManifestObject::try_new(
                Sha256Digest::new([21; 32]),
                7,
                211,
                Sha256Digest::new([22; 32]),
            )?,
            11,
        )?;
        let second_parent = insert_ingest_fixture(
            &connection,
            &schema,
            "inputs.first",
            ManifestObject::try_new(
                Sha256Digest::new([11; 32]),
                5,
                111,
                Sha256Digest::new([12; 32]),
            )?,
            12,
        )?;
        let output_dataset = DatasetId::try_from("features.derived")?;
        let high = ManifestObject::try_new(
            Sha256Digest::new([41; 32]),
            4,
            410,
            Sha256Digest::new([42; 32]),
        )?;
        let low = ManifestObject::try_new(
            Sha256Digest::new([31; 32]),
            3,
            310,
            Sha256Digest::new([32; 32]),
        )?;
        let plan =
            ManifestPlan::derive(output_dataset.clone(), vec![high.clone(), low.clone()], 8)?;
        let low_artifact = fixture_artifact(&low, 21)?;
        let high_artifact = fixture_artifact(&high, 22)?;
        insert_artifact(&connection, &low_artifact)?;
        insert_artifact(&connection, &high_artifact)?;
        let anchor = DatasetManifestRecord::try_new(
            SourceIdentifier::try_from(output_dataset.as_str())?,
            schema.version(),
            low_artifact.artifact_id(),
            plan.content_hash().evidence(),
            Timestamp::from_unix_nanos(21),
        );
        insert_manifest(&connection, &anchor)?;
        drop(connection);

        let parents =
            DerivedGenerationParents::try_new(vec![first_parent.clone(), second_parent.clone()])?;
        let build = DatasetBuildSpecDigest::try_new([71; 32])?;
        let authority = DerivedGenerationCommitAuthority::for_test();
        let catalog = AnalyticalManifestCatalog::open(&location, 8)?;
        let mismatched_high = ArtifactRecord::try_new(
            high_artifact.relative_reference(),
            high_artifact.content_digest(),
            high_artifact.size_bytes(),
            Timestamp::from_unix_nanos(99),
        )?;
        assert!(matches!(
            catalog.commit_derived_generation(
                &authority,
                &plan,
                &[mismatched_high.clone(), low_artifact.clone()],
                &anchor,
                &schema,
                &parents,
                build,
            ),
            Err(ManifestCatalogError::AnchorMismatch)
        ));

        let committed = catalog.commit_derived_generation(
            &authority,
            &plan,
            &[high_artifact.clone(), low_artifact.clone()],
            &anchor,
            &schema,
            &parents,
            build,
        );
        assert!(committed.is_ok(), "derived commit failed: {committed:?}");
        let manifest = committed?;
        let resolved = catalog.pinned(&manifest);
        assert!(resolved.is_ok(), "derived pin failed: {resolved:?}");
        let pinned = resolved?;
        assert_eq!(pinned.generation_kind(), GenerationKind::Derived);
        assert_eq!(pinned.build_spec_digest(), Some(build));
        assert_eq!(pinned.parents().len(), 2);
        assert_eq!(pinned.parents()[0].manifest(), &second_parent);
        assert_eq!(pinned.parents()[1].manifest(), &first_parent);
        assert_eq!(pinned.plan().objects(), &[low, high]);
        assert!(matches!(
            catalog.commit_derived_generation(
                &authority,
                &plan,
                &[mismatched_high, low_artifact],
                &anchor,
                &schema,
                &parents,
                build,
            ),
            Err(ManifestCatalogError::AnchorMismatch)
        ));
        drop(catalog);

        let reopened = CatalogAuthority::open(test_catalog_config(location)?);
        assert!(reopened.is_ok(), "catalog reopen failed: {reopened:?}");
        let catalog_authority = reopened?;
        let limits = EvidenceLimits::try_new(16, 64, 1 << 20, 1 << 20, 64 << 10)?;
        let captured = catalog_authority.analytical_evidence_snapshot(
            EvidenceSnapshotRequest::new(Timestamp::from_unix_nanos(100), limits),
        );
        assert!(captured.is_ok(), "evidence capture failed: {captured:?}");
        let (_, evidence) = captured?;
        assert_ne!(evidence.evidence_digest()?.bytes(), [0; 32]);
        Ok(())
    }

    #[test]
    fn long_history_membership_is_candidate_bounded_and_cancelable() -> TestResult {
        let (_directory, location) = migrated_catalog()?;
        let mut connection = Connection::open(location.path())?;
        connection.pragma_update(None, "foreign_keys", false)?;
        let transaction = connection.transaction()?;
        for ordinal in 0_i64..4_096 {
            transaction.execute(
                "INSERT INTO analytical_generation_objects
                 (dataset_id, manifest_version, ordinal, artifact_id, content_hash, row_count,
                  size_bytes, lineage_hash)
                 VALUES ('history', 1, ?1, ?2, ?3, 1, 1, ?4)",
                params![
                    ordinal,
                    uuid::Uuid::new_v4().to_string(),
                    [ordinal.rem_euclid(251) as u8; 32].as_slice(),
                    [8_u8; 32].as_slice()
                ],
            )?;
        }
        transaction.commit()?;
        drop(connection);
        let catalog = AnalyticalManifestCatalog::open(&location, 8)?;
        let candidate = Sha256Digest::new([254; 32]);
        let deadline = Instant::now() + Duration::from_secs(1);

        assert_eq!(
            catalog.referenced_candidates(
                [candidate].into_iter(),
                Timestamp::from_unix_nanos(1),
                1,
                deadline,
                &CancellationToken::new(),
            )?,
            vec![false]
        );
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            catalog.referenced_candidates(
                [candidate].into_iter(),
                Timestamp::from_unix_nanos(1),
                1,
                deadline,
                &cancelled,
            ),
            Err(ManifestCatalogError::Cancelled)
        ));
        Ok(())
    }

    fn migrated_catalog()
    -> Result<(tempfile::TempDir, market_squawk_platform::CatalogLocation), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
        let location = paths.catalog()?.clone();
        drop(CatalogAuthority::open(CatalogConfig::try_new(
            location.clone(),
            Duration::from_millis(750),
            CatalogLimit::new(32)?,
            CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
        )?)?);
        Ok((directory, location))
    }

    fn test_catalog_config(
        location: market_squawk_platform::CatalogLocation,
    ) -> Result<CatalogConfig, Box<dyn Error>> {
        Ok(CatalogConfig::try_new(
            location,
            Duration::from_millis(750),
            CatalogLimit::new(32)?,
            CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
        )?)
    }

    fn insert_ingest_fixture(
        connection: &Connection,
        schema: &DatasetSchemaRef,
        dataset_name: &str,
        object: ManifestObject,
        created_at_ns: i64,
    ) -> Result<DatasetManifestRef, Box<dyn Error>> {
        let dataset = DatasetId::try_from(dataset_name)?;
        let plan = ManifestPlan::append(dataset.clone(), None, object.clone(), 8)?;
        let artifact = fixture_artifact(&object, created_at_ns)?;
        insert_artifact(connection, &artifact)?;
        let anchor = DatasetManifestRecord::try_new(
            SourceIdentifier::try_from(dataset_name)?,
            schema.version(),
            artifact.artifact_id(),
            plan.content_hash().evidence(),
            Timestamp::from_unix_nanos(created_at_ns),
        );
        insert_manifest(connection, &anchor)?;
        connection.execute(
            "INSERT INTO analytical_generations
             (dataset_id, manifest_version, content_hash, lineage_hash, row_count, total_bytes,
              schema_name, schema_version, schema_fingerprint, anchor_manifest_id,
              generation_kind, parent_count, build_spec_digest, created_at_ns)
             VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'ingest', 0, NULL, ?10)",
            params![
                dataset.as_str(),
                plan.content_hash().bytes(),
                plan.lineage_digest().bytes(),
                i64::try_from(plan.row_count())?,
                i64::try_from(plan.total_bytes())?,
                schema.name(),
                i64::from(schema.version().get()),
                schema.fingerprint().as_slice(),
                anchor.manifest_id().to_string(),
                created_at_ns,
            ],
        )?;
        connection.execute(
            "INSERT INTO analytical_generation_objects
             (dataset_id, manifest_version, ordinal, artifact_id, content_hash, row_count,
              size_bytes, lineage_hash)
             VALUES (?1, 1, 0, ?2, ?3, ?4, ?5, ?6)",
            params![
                dataset.as_str(),
                artifact.artifact_id().to_string(),
                object.content_hash().bytes(),
                i64::try_from(object.row_count())?,
                i64::try_from(object.size_bytes())?,
                object.lineage_digest().bytes(),
            ],
        )?;
        Ok(DatasetManifestRef::try_new_with_schema(
            dataset,
            1,
            schema.clone(),
            plan.content_hash(),
        )?)
    }

    fn fixture_artifact(
        object: &ManifestObject,
        created_at_ns: i64,
    ) -> Result<ArtifactRecord, Box<dyn Error>> {
        let mut encoded = String::with_capacity(64);
        for byte in object.content_hash().bytes() {
            write!(&mut encoded, "{byte:02x}")?;
        }
        Ok(ArtifactRecord::try_new(
            format!("objects/sha256/{}/{}.parquet", &encoded[..2], encoded),
            object.content_hash().evidence(),
            object.size_bytes(),
            Timestamp::from_unix_nanos(created_at_ns),
        )?)
    }

    fn insert_artifact(
        connection: &Connection,
        artifact: &ArtifactRecord,
    ) -> Result<(), Box<dyn Error>> {
        let algorithm = match artifact.content_digest().algorithm() {
            DigestAlgorithm::Sha256 => 1_i64,
            DigestAlgorithm::Blake3 => 2_i64,
        };
        let rights = SourceRightsDecision::try_new(RightsDecisionInput {
            source_id: SourceId::try_from(FIXTURE_SOURCE_ID)?,
            payload_digest: artifact.content_digest(),
            retrieved_at: Timestamp::from_unix_nanos(1),
            basis: RightsBasis::reviewed_terms(
                "https://fixture.invalid/terms",
                EvidenceDigest::new(DigestAlgorithm::Sha256, [93; 32]),
            )?,
            authorization_evidence: EvidenceDigest::new(DigestAlgorithm::Sha256, [94; 32]),
            authorization_expires_at: None,
            permitted_operations: vec![SourceOperation::Persist],
        })?;
        let (basis_algorithm, basis_digest) = match rights.basis().digest().algorithm() {
            DigestAlgorithm::Sha256 => (1_i64, rights.basis().digest().bytes()),
            DigestAlgorithm::Blake3 => (2_i64, rights.basis().digest().bytes()),
        };
        let (authorization_algorithm, authorization_digest) =
            match rights.authorization_evidence().algorithm() {
                DigestAlgorithm::Sha256 => (1_i64, rights.authorization_evidence().bytes()),
                DigestAlgorithm::Blake3 => (2_i64, rights.authorization_evidence().bytes()),
            };
        connection.execute(
            "INSERT INTO source_rights
             (rights_id, source_id, payload_algorithm, payload_digest, retrieved_at_ns,
              basis_reference, basis_algorithm, basis_digest, authorization_algorithm,
              authorization_digest, authorization_expires_at_ns, operation_mask, admitted_at_ns,
              basis_kind, basis_root_algorithm, basis_root_digest, fingerprint_version)
             VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, ?7, ?8, ?9, NULL, ?10, 1,
                     'reviewed_terms', NULL, NULL, 2)",
            params![
                rights.fingerprint().as_slice(),
                FIXTURE_SOURCE_ID,
                algorithm,
                artifact.content_digest().bytes(),
                rights.basis().reference(),
                basis_algorithm,
                basis_digest.as_slice(),
                authorization_algorithm,
                authorization_digest.as_slice(),
                i64::from(rights.operation_mask()),
            ],
        )?;
        let run_id = uuid::Uuid::new_v4();
        connection.execute(
            "INSERT INTO ingest_runs
             (run_id, idempotency_key, source_id, payload_algorithm, payload_digest, operation,
              rights_id, state, requested_at_ns, completed_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, 'persist', ?6, 'succeeded', 2, ?7)",
            params![
                run_id.to_string(),
                artifact.artifact_id().to_string(),
                FIXTURE_SOURCE_ID,
                algorithm,
                artifact.content_digest().bytes(),
                rights.fingerprint().as_slice(),
                artifact.created_at().unix_nanos(),
            ],
        )?;
        connection.execute(
            "INSERT INTO artifacts
             (artifact_id, run_id, relative_reference, content_algorithm, content_digest,
              size_bytes, created_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                artifact.artifact_id().to_string(),
                run_id.to_string(),
                artifact.relative_reference(),
                algorithm,
                artifact.content_digest().bytes(),
                i64::try_from(artifact.size_bytes())?,
                artifact.created_at().unix_nanos(),
            ],
        )?;
        Ok(())
    }

    fn insert_control_fixture(connection: &Connection) -> Result<(), Box<dyn Error>> {
        let revision = [90_u8; 32];
        connection.execute(
            "INSERT INTO sources
             (source_id, current_revision_digest, current_registered_at_ns,
              first_registered_at_ns)
             VALUES (?1, ?2, 1, 1)",
            params![FIXTURE_SOURCE_ID, revision.as_slice()],
        )?;
        connection.execute(
            "INSERT INTO source_revisions
             (source_id, revision_digest, metadata_json, registered_at_ns)
             VALUES (?1, ?2, '{\"fixture\":true}', 1)",
            params![FIXTURE_SOURCE_ID, revision.as_slice()],
        )?;
        Ok(())
    }

    fn insert_manifest(
        connection: &Connection,
        manifest: &DatasetManifestRecord,
    ) -> Result<(), Box<dyn Error>> {
        let algorithm = match manifest.content_digest().algorithm() {
            DigestAlgorithm::Sha256 => 1_i64,
            DigestAlgorithm::Blake3 => 2_i64,
        };
        connection.execute(
            "INSERT INTO dataset_manifests
             (manifest_id, dataset_name, schema_version, artifact_id, content_algorithm,
              content_digest, created_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                manifest.manifest_id().to_string(),
                manifest.dataset_name().as_str(),
                i64::from(manifest.schema_version().get()),
                manifest.artifact_id().to_string(),
                algorithm,
                manifest.content_digest().bytes(),
                manifest.created_at().unix_nanos(),
            ],
        )?;
        Ok(())
    }
}
