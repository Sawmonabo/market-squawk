//! Original-file custody and exact creating-generation locators in the logical catalog.

use crate::ingest::LogicalOriginalSourceRevisionKind;

use std::time::Instant;

use market_squawk_domain::SourceId;
use market_squawk_sources::SealedLogicalObjectInput;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    DatasetId, DatasetManifestRef, DatasetSchemaRegistry, RegisteredRightsGrant, Sha256Digest,
    SourceOperation,
};

pub(crate) const MAX_PROVIDER_LOGICAL_ORIGINAL_CHECKPOINT_BYTES: usize = 128 * 1024;
const MAX_ORIGINAL_OBJECTS: usize = 64;
const ORIGINAL_COORDINATE_DOMAIN: &[u8] = b"market-squawk/provider-logical-original/coordinate/v1";
const ORIGINAL_RECEIPT_DOMAIN: &[u8] = b"market-squawk/provider-logical-original/custody/v1";

/// Inert source checkpoint retained only after exact Persist admission and physical verification.
/// Serving evidence still requires reopening the original and authorizing its creating manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderLogicalOriginalReceipt {
    coordinate: EvidenceDigest,
    original_digest: EvidenceDigest,
    checkpoint: Box<[u8]>,
    publication_digest: Option<EvidenceDigest>,
}

impl ProviderLogicalOriginalReceipt {
    /// Returns the source-owned original-file receipt identity.
    pub const fn original_digest(&self) -> EvidenceDigest {
        self.original_digest
    }
    /// Returns the bounded inert source-owned reopening locator.
    pub fn checkpoint_bytes(&self) -> &[u8] {
        &self.checkpoint
    }
    /// Returns the exact logical binding only after its atomic publication.
    pub const fn publication_digest(&self) -> Option<EvidenceDigest> {
        self.publication_digest
    }
}

/// Exact creating generation and its retained logical evidence, never an inherited descendant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderLogicalPublicationOrigin {
    manifest: DatasetManifestRef,
    publication: PersistedProviderLogicalPublicationBinding,
}

impl ProviderLogicalPublicationOrigin {
    /// Returns the actual generation created by this publication's source run.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    /// Returns retained value evidence, which does not recreate live publication authority.
    pub const fn publication(&self) -> &PersistedProviderLogicalPublicationBinding {
        &self.publication
    }
}

struct Original {
    receipt: ProviderLogicalOriginalReceipt,
    dataset: DatasetId,
    source: SourceId,
    native_schema: EvidenceDigest,
    source_revision: EvidenceDigest,
    registered_source_revision: EvidenceDigest,
    source_revision_kind: LogicalOriginalSourceRevisionKind,
    received_at: Timestamp,
    retained_at: Timestamp,
    rights_id: [u8; 32],
    objects: Box<[PersistedProviderLogicalObjectClaim]>,
}

impl Catalog {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn retain_provider_logical_original(
        &self,
        dataset: &DatasetId,
        source: &SourceId,
        native_schema: EvidenceDigest,
        source_revision: EvidenceDigest,
        registered_source_revision: EvidenceDigest,
        source_revision_kind: LogicalOriginalSourceRevisionKind,
        original_digest: EvidenceDigest,
        received_at: Timestamp,
        checkpoint: &[u8],
        objects: &[SealedLogicalObjectInput],
        grant: &RegisteredRightsGrant,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ProviderLogicalOriginalReceipt, CatalogError> {
        check_control(deadline, cancellation)?;
        if grant.catalog_id != self.catalog_id || grant.payload_digest() != original_digest {
            return Err(CatalogError::InvalidRightsCapability);
        }
        if checkpoint.is_empty()
            || checkpoint.len() > MAX_PROVIDER_LOGICAL_ORIGINAL_CHECKPOINT_BYTES
            || checkpoint
                .len()
                .checked_add(4096)
                .is_none_or(|bytes| bytes > self.result_limits().max_record_bytes())
            || objects.is_empty()
            || objects.len() > MAX_ORIGINAL_OBJECTS
        {
            return Err(CatalogError::ProviderLogicalMismatch);
        }
        let coordinate = original_coordinate(
            dataset,
            source,
            native_schema,
            source_revision,
            original_digest,
        )?;
        let mut claims = Vec::new();
        claims
            .try_reserve_exact(objects.len())
            .map_err(|_| CatalogError::Allocation)?;
        let mut metadata_bytes = 0;
        for (ordinal, object) in objects.iter().enumerate() {
            check_control(deadline, cancellation)?;
            if object.ordinal()
                != u32::try_from(ordinal).map_err(|_| CatalogError::InvalidRecord)?
            {
                return Err(CatalogError::ProviderLogicalMismatch);
            }
            validate_sha256(object.semantic_identity())?;
            let claim = object.object().claim().clone();
            let json = logical_claim_json(&claim)?;
            metadata_bytes = charge_metadata(metadata_bytes, json.len())?;
            if claim.size_bytes() == 0
                || json
                    .len()
                    .checked_add(4096)
                    .is_none_or(|bytes| bytes > self.result_limits().max_record_bytes())
            {
                return Err(CatalogError::ProviderLogicalMismatch);
            }
            claims.push(PersistedProviderLogicalObjectClaim {
                role: object.role(),
                ordinal: object.ordinal(),
                semantic_identity: object.semantic_identity(),
                raw_claim_digest: raw_claim_digest(json.as_bytes()),
                claim,
            });
        }
        let transaction = self.connection.unchecked_transaction()?;
        let now = super::super::storage::trusted_catalog_now(&transaction)?;
        if received_at > now {
            return Err(CatalogError::PublicationTimeConflict);
        }
        validate_source_revision(
            &transaction,
            source,
            source_revision,
            registered_source_revision,
            source_revision_kind,
            now,
        )?;
        let admitted: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM source_rights AS rights
             JOIN source_revisions AS revision ON revision.source_id=rights.source_id
             WHERE rights.rights_id=?1 AND rights.source_id=?2
               AND rights.payload_algorithm=1 AND rights.payload_digest=?3
               AND (rights.operation_mask & ?4)<>0 AND rights.admitted_at_ns<=?5
               AND (rights.authorization_expires_at_ns IS NULL OR rights.authorization_expires_at_ns>?5)
               AND revision.revision_digest=?6 AND revision.registered_at_ns<=?5)",
            params![grant.rights_id(), source.as_str(), original_digest.bytes(),
                i64::from(SourceOperation::Persist.mask()), now.unix_nanos(), registered_source_revision.bytes()],
            |row| row.get(0),
        )?;
        if !admitted {
            return Err(CatalogError::InvalidRightsCapability);
        }
        if let Some(existing) = load_original(&transaction, coordinate, deadline, cancellation)? {
            if existing.dataset != *dataset
                || existing.source != *source
                || existing.native_schema != native_schema
                || existing.source_revision != source_revision
                || existing.registered_source_revision != registered_source_revision
                || existing.source_revision_kind != source_revision_kind
                || existing.received_at != received_at
                || existing.receipt.checkpoint_bytes() != checkpoint
                || existing.objects.as_ref() != claims.as_slice()
            {
                return Err(CatalogError::ProviderLogicalConflict);
            }
            check_control(deadline, cancellation)?;
            transaction.commit()?;
            return Ok(existing.receipt);
        }
        let value = Original {
            receipt: ProviderLogicalOriginalReceipt {
                coordinate,
                original_digest,
                checkpoint: checkpoint.into(),
                publication_digest: None,
            },
            dataset: dataset.clone(),
            source: source.clone(),
            native_schema,
            source_revision,
            registered_source_revision,
            source_revision_kind,
            received_at,
            retained_at: now,
            rights_id: grant.rights_id(),
            objects: claims.into_boxed_slice(),
        };
        let receipt_digest = custody_digest(&value);
        transaction.execute(
            "INSERT INTO provider_logical_originals
             (coordinate_digest, dataset_id, source_id, native_schema_digest, source_revision_digest,
              original_digest, received_at_ns, checkpoint_digest, checkpoint_bytes,
              object_count, object_set_digest, rights_id, custody_digest, retained_at_ns,
              publication_digest, published_at_ns, registered_source_revision_digest, source_revision_kind)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,NULL,NULL,?15,?16)",
            params![coordinate.bytes(), dataset.as_str(), source.as_str(), native_schema.bytes(),
                source_revision.bytes(), original_digest.bytes(), received_at.unix_nanos(),
                checkpoint_digest(checkpoint).bytes(), checkpoint, to_i64(value.objects.len())?,
                object_set_digest(&value.objects).bytes(), grant.rights_id(), receipt_digest.bytes(), now.unix_nanos(), registered_source_revision.bytes(), source_revision_kind.name()],
        )?;
        for object in &value.objects {
            check_control(deadline, cancellation)?;
            insert_logical_claim(&transaction, object.raw_claim_digest, &object.claim, now)?;
            transaction.execute(
                "INSERT INTO provider_logical_original_objects
                 (coordinate_digest, object_ordinal, object_role, semantic_identity,
                  raw_claim_digest, physical_receipt_digest) VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    coordinate.bytes(),
                    i64::from(object.ordinal),
                    object_role_name(object.role),
                    object.semantic_identity.bytes(),
                    object.raw_claim_digest.bytes(),
                    object.claim.physical_receipt_digest().bytes()
                ],
            )?;
        }
        let retained = load_original(&transaction, coordinate, deadline, cancellation)?
            .ok_or(CatalogError::ProviderLogicalConflict)?;
        if retained.receipt != value.receipt || retained.objects != value.objects {
            return Err(CatalogError::ProviderLogicalConflict);
        }
        append_audit(
            &transaction,
            "provider-logical-original.retained",
            source.as_str(),
            receipt_digest.bytes(),
            now,
        )?;
        check_control(deadline, cancellation)?;
        transaction.commit()?;
        Ok(retained.receipt)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn provider_logical_original(
        &self,
        dataset: &DatasetId,
        source: &SourceId,
        native_schema: EvidenceDigest,
        source_revision: EvidenceDigest,
        original: Option<EvidenceDigest>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ProviderLogicalOriginalReceipt>, CatalogError> {
        validate_sha256(native_schema)?;
        validate_sha256(source_revision)?;
        if let Some(original) = original {
            validate_sha256(original)?;
        }
        let coordinate: Option<Vec<u8>> = self.connection.query_row(
            "SELECT coordinate_digest FROM provider_logical_originals
             WHERE dataset_id=?1 AND source_id=?2 AND native_schema_digest=?3 AND source_revision_digest=?4
               AND ((?5 IS NULL AND publication_digest IS NULL) OR original_digest=?5)
             ORDER BY retained_at_ns DESC, coordinate_digest DESC LIMIT 1",
            params![dataset.as_str(), source.as_str(), native_schema.bytes(), source_revision.bytes(), original.map(EvidenceDigest::bytes)],
            |row| row.get(0),
        ).optional()?;
        coordinate
            .map(|bytes| {
                load_original(
                    &self.connection,
                    parse_digest(1, &bytes)?,
                    deadline,
                    cancellation,
                )?
                .map(|original| original.receipt)
                .ok_or(CatalogError::CorruptCatalog)
            })
            .transpose()
    }
}

impl crate::catalog::CatalogReadSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn provider_logical_origins(
        &self,
        dataset: &DatasetId,
        source: &SourceId,
        native_schema: EvidenceDigest,
        knowledge_cutoff: Timestamp,
        before_version: Option<u64>,
        limit: usize,
        exact: Option<(EvidenceDigest, Sha256Digest)>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<ProviderLogicalPublicationOrigin>, bool), CatalogError> {
        validate_sha256(native_schema)?;
        if !(1..=64).contains(&limit) {
            return Err(CatalogError::InvalidLimit);
        }
        if let Some((binding, content)) = exact {
            validate_sha256(binding)?;
            validate_sha256(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                content.bytes(),
            ))?;
        }
        let schema = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| CatalogError::InvalidRecord)?;
        let before_version = before_version.map(to_i64).transpose()?;
        let connection = self.connection();
        let mut statement = connection.prepare(
            "SELECT generation.manifest_version, generation.content_hash, input.publication_digest, original.coordinate_digest
             FROM analytical_available_generations AS generation
             JOIN dataset_manifests AS anchor ON anchor.manifest_id=generation.anchor_manifest_id
             JOIN ingest_runs AS run ON run.run_id=anchor.run_id AND run.state='succeeded' AND run.operation='persist'
             JOIN ingest_run_provider_publication_bindings AS input ON input.run_id=anchor.run_id
               AND input.publication_kind='provider_logical' AND input.logical_binding_digest=input.publication_digest
             JOIN analytical_generation_provider_publication_bindings AS generation_input
               ON generation_input.generation_sequence=generation.generation_sequence
              AND generation_input.run_id=input.run_id AND generation_input.source_id=input.source_id
              AND generation_input.publication_digest=input.publication_digest AND generation_input.publication_kind=input.publication_kind
             JOIN analytical_generation_source_inputs AS source_input
               ON source_input.generation_sequence=generation.generation_sequence
              AND source_input.run_id=input.run_id AND source_input.source_id=input.source_id
             JOIN provider_logical_originals AS original ON original.publication_digest=input.publication_digest
              AND original.dataset_id=generation.dataset_id AND original.source_id=input.source_id
             WHERE generation.dataset_id=?1 AND original.source_id=?2 AND original.native_schema_digest=?3
               AND generation.generation_kind='ingest' AND generation.available_at_ns<=?4
               AND anchor.created_at_ns<=?4 AND run.completed_at_ns<=?4 AND original.published_at_ns<=?4
               AND original.received_at_ns<=?4 AND original.retained_at_ns<=?4
               AND (?5 IS NULL OR generation.manifest_version<?5)
               AND generation.schema_name=?6 AND generation.schema_version=?7 AND generation.schema_fingerprint=?8
               AND (?9 IS NULL OR input.publication_digest=?9) AND (?10 IS NULL OR generation.content_hash=?10)
             ORDER BY generation.manifest_version DESC LIMIT ?11",
        )?;
        let mut rows = statement.query(params![
            dataset.as_str(),
            source.as_str(),
            native_schema.bytes(),
            knowledge_cutoff.unix_nanos(),
            before_version,
            schema.name(),
            i64::from(schema.version().get()),
            schema.fingerprint().as_slice(),
            exact.map(|value| value.0.bytes()),
            exact.map(|value| value.1.bytes()),
            to_i64(limit + 1)?
        ])?;
        let mut origins = Vec::new();
        origins
            .try_reserve_exact(limit)
            .map_err(|_| CatalogError::Allocation)?;
        let mut has_more = false;
        while let Some(row) = rows.next()? {
            check_control(deadline, cancellation)?;
            if origins.len() == limit {
                has_more = true;
                break;
            }
            let version =
                u64::try_from(row.get::<_, i64>(0)?).map_err(|_| CatalogError::CorruptCatalog)?;
            let content = parse_digest(1, &row.get::<_, Vec<u8>>(1)?)?;
            let binding = parse_digest(1, &row.get::<_, Vec<u8>>(2)?)?;
            let original = load_original(
                connection,
                parse_digest(1, &row.get::<_, Vec<u8>>(3)?)?,
                deadline,
                cancellation,
            )?
            .ok_or(CatalogError::CorruptCatalog)?;
            let publication = load_provider_logical_publication_binding(connection, binding)?
                .ok_or(CatalogError::CorruptCatalog)?;
            verify_original_binding(&original, dataset, &publication)?;
            if original.receipt.publication_digest() != Some(binding) {
                return Err(CatalogError::CorruptCatalog);
            }
            let manifest = DatasetManifestRef::try_new_with_schema(
                dataset.clone(),
                version,
                schema.clone(),
                Sha256Digest::new(content.bytes()),
            )
            .map_err(|_| CatalogError::CorruptCatalog)?;
            origins.push(ProviderLogicalPublicationOrigin {
                manifest,
                publication,
            });
        }
        check_control(deadline, cancellation)?;
        Ok((origins, has_more))
    }
}

/// Runs inside the existing artifact/manifest/generation transaction under its progress guard.
#[allow(clippy::too_many_arguments)]
pub(crate) fn publish_original(
    transaction: &Transaction<'_>,
    run_id: Uuid,
    dataset: &DatasetId,
    binding: &SealedProviderLogicalPublicationBinding,
    coordinates: &[ProviderArtifactInputCoordinate],
    now: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CatalogError> {
    check_control(deadline, cancellation)?;
    let evidence = PersistedProviderLogicalPublicationBinding::try_from_live(binding)?;
    let native_schema = native_schema(&evidence)?;
    let key = original_coordinate(
        dataset,
        evidence.terminal.source_id(),
        native_schema,
        evidence.terminal.source_revision_digest(),
        evidence.terminal.provider_terminal_evidence_digest(),
    )?;
    let original = load_original(transaction, key, deadline, cancellation)?
        .ok_or(CatalogError::ProviderLogicalMismatch)?;
    verify_original_binding(&original, dataset, &evidence)?;
    if original.receipt.publication_digest.is_some() {
        return Err(CatalogError::ProviderLogicalConflict);
    }
    // The original grant admitted raw custody; the final run separately admits the complete binding.
    let valid_run: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ingest_runs WHERE run_id=?1 AND state='reserved'
         AND operation='persist' AND source_id=?2 AND payload_algorithm=1 AND payload_digest=?3)",
        params![
            run_id.to_string(),
            original.source.as_str(),
            binding.binding_digest().bytes()
        ],
        |row| row.get(0),
    )?;
    if !valid_run {
        return Err(CatalogError::ProviderLogicalMismatch);
    }
    retain_sealed_provider_logical_publication_binding(
        transaction,
        run_id,
        binding,
        coordinates,
        now,
    )?;
    if transaction.execute(
        "UPDATE provider_logical_originals SET publication_digest=?1, published_at_ns=?2
         WHERE coordinate_digest=?3 AND publication_digest IS NULL AND published_at_ns IS NULL",
        params![
            binding.binding_digest().bytes(),
            now.unix_nanos(),
            key.bytes()
        ],
    )? != 1
    {
        return Err(CatalogError::ProviderLogicalConflict);
    }
    append_audit(
        transaction,
        "provider-logical-original.published",
        &run_id.to_string(),
        key.bytes(),
        now,
    )?;
    check_control(deadline, cancellation)
}

pub(crate) fn original_publication_matches(
    connection: &Connection,
    dataset: &DatasetId,
    binding: &SealedProviderLogicalPublicationBinding,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<bool, CatalogError> {
    let evidence = PersistedProviderLogicalPublicationBinding::try_from_live(binding)?;
    let key = original_coordinate(
        dataset,
        evidence.terminal.source_id(),
        native_schema(&evidence)?,
        evidence.terminal.source_revision_digest(),
        evidence.terminal.provider_terminal_evidence_digest(),
    )?;
    let Some(original) = load_original(connection, key, deadline, cancellation)? else {
        return Ok(false);
    };
    verify_original_binding(&original, dataset, &evidence)?;
    Ok(original.receipt.publication_digest == Some(binding.binding_digest()))
}

fn native_schema(
    publication: &PersistedProviderLogicalPublicationBinding,
) -> Result<EvidenceDigest, CatalogError> {
    let mut native = publication
        .partitions
        .iter()
        .filter(|part| part.family == LogicalPartitionFamily::ProviderNative);
    let schema = native
        .next()
        .ok_or(CatalogError::ProviderLogicalMismatch)?
        .schema_identity;
    if native.any(|part| part.schema_identity != schema) {
        return Err(CatalogError::ProviderLogicalMismatch);
    }
    Ok(schema)
}

fn verify_original_binding(
    original: &Original,
    dataset: &DatasetId,
    publication: &PersistedProviderLogicalPublicationBinding,
) -> Result<(), CatalogError> {
    if original.dataset != *dataset
        || original.source != *publication.terminal.source_id()
        || original.native_schema != native_schema(publication)?
        || original.source_revision != publication.terminal.source_revision_digest()
        || original.receipt.original_digest
            != publication.terminal.provider_terminal_evidence_digest()
        || original.objects != publication.objects
    {
        return Err(CatalogError::ProviderLogicalMismatch);
    }
    Ok(())
}

fn load_original(
    connection: &Connection,
    coordinate: EvidenceDigest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<Original>, CatalogError> {
    check_control(deadline, cancellation)?;
    type Header = (
        String,
        String,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        i64,
        Vec<u8>,
        Vec<u8>,
        i64,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        i64,
        Option<Vec<u8>>,
        Option<i64>,
        Vec<u8>,
        String,
    );
    let header: Option<Header> = connection.query_row(
        "SELECT dataset_id, source_id, native_schema_digest, source_revision_digest, original_digest,
         received_at_ns, checkpoint_digest, checkpoint_bytes, object_count, object_set_digest,
         rights_id, custody_digest, retained_at_ns, publication_digest, published_at_ns,
         registered_source_revision_digest, source_revision_kind
         FROM provider_logical_originals WHERE coordinate_digest=?1", [coordinate.bytes()],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?,row.get(11)?,row.get(12)?,row.get(13)?,row.get(14)?,row.get(15)?,row.get(16)?)),
    ).optional()?;
    let Some(header) = header else {
        return Ok(None);
    };
    if header.7.is_empty()
        || header.7.len() > MAX_PROVIDER_LOGICAL_ORIGINAL_CHECKPOINT_BYTES
        || checkpoint_digest(&header.7) != parse_digest(1, &header.6)?
        || header.5 > header.12
        || header.13.is_some() != header.14.is_some()
        || header.14.is_some_and(|time| time < header.12)
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let count = bounded_count(header.8, MAX_ORIGINAL_OBJECTS)?;
    if count == 0 {
        return Err(CatalogError::CorruptCatalog);
    }
    let mut statement = connection.prepare(
        "SELECT object.object_ordinal, object.object_role, object.semantic_identity,
         object.raw_claim_digest, object.physical_receipt_digest, claim.raw_claim_json
         FROM provider_logical_original_objects AS object JOIN sealed_raw_objects AS claim
           ON claim.raw_claim_digest=object.raw_claim_digest AND claim.physical_receipt_digest=object.physical_receipt_digest
          AND claim.raw_claim_kind='logical_object'
         WHERE object.coordinate_digest=?1 ORDER BY object.object_ordinal LIMIT 65",
    )?;
    let mut rows = statement.query([coordinate.bytes()])?;
    let mut objects = Vec::new();
    objects
        .try_reserve_exact(count)
        .map_err(|_| CatalogError::Allocation)?;
    let mut metadata_bytes = 0;
    while let Some(row) = rows.next()? {
        check_control(deadline, cancellation)?;
        let ordinal = row.get::<_, i64>(0)?;
        if objects.len() == count || ordinal != to_i64(objects.len())? {
            return Err(CatalogError::CorruptCatalog);
        }
        let json: String = row.get(5)?;
        metadata_bytes = charge_metadata(metadata_bytes, json.len())?;
        let claim = parse_logical_claim(&json)?;
        let digest = parse_digest(1, &row.get::<_, Vec<u8>>(3)?)?;
        let semantic_identity = parse_digest(1, &row.get::<_, Vec<u8>>(2)?)?;
        validate_sha256(semantic_identity)?;
        if claim.size_bytes() == 0
            || raw_claim_digest(json.as_bytes()) != digest
            || claim.physical_receipt_digest() != parse_digest(1, &row.get::<_, Vec<u8>>(4)?)?
        {
            return Err(CatalogError::CorruptCatalog);
        }
        objects.push(PersistedProviderLogicalObjectClaim {
            role: parse_object_role(&row.get::<_, String>(1)?)?,
            ordinal: u32::try_from(ordinal).map_err(|_| CatalogError::CorruptCatalog)?,
            semantic_identity,
            raw_claim_digest: digest,
            claim,
        });
    }
    if objects.len() != count {
        return Err(CatalogError::CorruptCatalog);
    }
    let original = Original {
        receipt: ProviderLogicalOriginalReceipt {
            coordinate,
            original_digest: parse_digest(1, &header.4)?,
            checkpoint: header.7.into_boxed_slice(),
            publication_digest: header
                .13
                .as_ref()
                .map(|bytes| parse_digest(1, bytes))
                .transpose()?,
        },
        dataset: DatasetId::try_from(header.0.as_str())
            .map_err(|_| CatalogError::CorruptCatalog)?,
        source: SourceId::try_from(header.1.as_str()).map_err(|_| CatalogError::CorruptCatalog)?,
        native_schema: parse_digest(1, &header.2)?,
        source_revision: parse_digest(1, &header.3)?,
        registered_source_revision: parse_digest(1, &header.15)?,
        source_revision_kind: match header.16.as_str() {
            "metadata" => LogicalOriginalSourceRevisionKind::Metadata,
            "contract_payload" => LogicalOriginalSourceRevisionKind::ContractPayload,
            _ => return Err(CatalogError::CorruptCatalog),
        },
        received_at: Timestamp::from_unix_nanos(header.5),
        retained_at: Timestamp::from_unix_nanos(header.12),
        rights_id: header
            .10
            .try_into()
            .map_err(|_| CatalogError::CorruptCatalog)?,
        objects: objects.into_boxed_slice(),
    };
    if original_coordinate(
        &original.dataset,
        &original.source,
        original.native_schema,
        original.source_revision,
        original.receipt.original_digest,
    )? != coordinate
        || object_set_digest(&original.objects) != parse_digest(1, &header.9)?
        || custody_digest(&original) != parse_digest(1, &header.11)?
    {
        return Err(CatalogError::CorruptCatalog);
    }
    validate_source_revision(
        connection,
        &original.source,
        original.source_revision,
        original.registered_source_revision,
        original.source_revision_kind,
        original.retained_at,
    )?;
    let rights_match: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM source_rights WHERE rights_id=?1 AND source_id=?2
         AND payload_algorithm=1 AND payload_digest=?3 AND (operation_mask & ?4)<>0
         AND admitted_at_ns<=?5 AND (authorization_expires_at_ns IS NULL OR authorization_expires_at_ns>?5))",
        params![original.rights_id, original.source.as_str(), original.receipt.original_digest.bytes(),
            i64::from(SourceOperation::Persist.mask()), original.retained_at.unix_nanos()], |row| row.get(0),
    )?;
    if !rights_match {
        return Err(CatalogError::CorruptCatalog);
    }
    if let Some(binding) = original.receipt.publication_digest {
        let publication = load_provider_logical_publication_binding(connection, binding)?
            .ok_or(CatalogError::CorruptCatalog)?;
        verify_original_binding(&original, &original.dataset, &publication)?;
    }
    check_control(deadline, cancellation)?;
    Ok(Some(original))
}

fn original_coordinate(
    dataset: &DatasetId,
    source: &SourceId,
    native_schema: EvidenceDigest,
    source_revision: EvidenceDigest,
    original: EvidenceDigest,
) -> Result<EvidenceDigest, CatalogError> {
    for value in [native_schema, source_revision, original] {
        validate_sha256(value)?;
    }
    let mut hash = Sha256::new();
    hash.update(ORIGINAL_COORDINATE_DOMAIN);
    hash_field(&mut hash, dataset.as_str().as_bytes());
    hash_field(&mut hash, source.as_str().as_bytes());
    for value in [native_schema, source_revision, original] {
        hash_digest(&mut hash, value);
    }
    Ok(sha256(hash))
}

fn custody_digest(original: &Original) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(ORIGINAL_RECEIPT_DOMAIN);
    hash_digest(&mut hash, original.receipt.coordinate);
    hash_digest(&mut hash, original.registered_source_revision);
    hash_field(&mut hash, original.source_revision_kind.name().as_bytes());
    hash_digest(
        &mut hash,
        checkpoint_digest(original.receipt.checkpoint_bytes()),
    );
    hash_digest(&mut hash, object_set_digest(&original.objects));
    hash.update(original.received_at.unix_nanos().to_be_bytes());
    hash.update(original.retained_at.unix_nanos().to_be_bytes());
    hash.update(original.rights_id);
    sha256(hash)
}

fn checkpoint_digest(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn check_control(deadline: Instant, cancellation: &CancellationToken) -> Result<(), CatalogError> {
    if cancellation.is_cancelled() {
        Err(CatalogError::MarketRecoveryReadCancelled)
    } else if Instant::now() >= deadline {
        Err(CatalogError::MarketRecoveryReadDeadlineExceeded)
    } else {
        Ok(())
    }
}

fn validate_source_revision(
    connection: &Connection,
    source: &SourceId,
    revision: EvidenceDigest,
    registered: EvidenceDigest,
    kind: LogicalOriginalSourceRevisionKind,
    retained_at: Timestamp,
) -> Result<(), CatalogError> {
    let json: String = connection.query_row(
        "SELECT metadata_json FROM source_revisions WHERE source_id=?1 AND revision_digest=?2 AND registered_at_ns<=?3",
        params![source.as_str(), registered.bytes(), retained_at.unix_nanos()], |row| row.get(0),
    ).optional()?.ok_or(CatalogError::CorruptCatalog)?;
    let metadata: market_squawk_sources::SourceMetadata = serde_json::from_str(&json)?;
    if json.len() > 1024 * 1024
        || serde_json::to_string(&metadata)? != json
        || metadata.source_id() != source
        || EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(json.as_bytes()).into(),
        ) != registered
        || kind.digest(&metadata, registered) != revision
    {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(())
}
