//! Durable provider-neutral evidence for streamed logical publications.

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_platform::{ResearchObjectClaim, SealedResearchRawClaim};
use market_squawk_sources::{
    CanonicalPartitionExpectation, LogicalItemRange, LogicalObjectRole, LogicalPartitionFamily,
    MAX_PROVIDER_CANONICAL_PARTITIONS, MAX_PROVIDER_LOGICAL_CATALOG_BYTES,
    MAX_PROVIDER_LOGICAL_OBJECTS, MAX_PROVIDER_LOGICAL_PARTITIONS, ProviderLogicalTerminalReceipt,
    SealedProviderLogicalPublicationBinding,
};
use rusqlite::{Connection, OptionalExtension as _, Transaction, params};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::provider_capture::raw_claim_digest;
use super::storage::{append_audit, parse_digest};
use super::{Catalog, CatalogError};

const BINDING_FORMAT_VERSION: i64 = 1;
const PUBLICATION_KIND: &str = "provider_logical";
const LOGICAL_OBJECT_SET_DOMAIN: &[u8] =
    b"market-squawk/provider-logical-publication/object-set/v1";
const LOGICAL_PARTITION_SEMANTIC_DOMAIN: &[u8] =
    b"market-squawk/provider-logical-publication/partition-semantic/v1";
const LOGICAL_PARTITION_SET_DOMAIN: &[u8] =
    b"market-squawk/provider-logical-publication/partition-set/v1";
const CANONICAL_PARTITION_SET_DOMAIN: &[u8] =
    b"market-squawk/provider-logical-publication/canonical-partition-set/v1";
const LOGICAL_TERMINAL_RECEIPT_DOMAIN: &[u8] =
    b"market-squawk/provider-logical-publication/terminal/v1";
const LOGICAL_PUBLICATION_BINDING_DOMAIN: &[u8] =
    b"market-squawk/provider-logical-publication/binding/v1";

/// Exact persisted logical raw object in its provider-authored order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderLogicalObjectClaim {
    role: LogicalObjectRole,
    ordinal: u32,
    semantic_identity: EvidenceDigest,
    raw_claim_digest: EvidenceDigest,
    claim: ResearchObjectClaim,
}

impl PersistedProviderLogicalObjectClaim {
    /// Returns the closed semantic role.
    pub const fn role(&self) -> LogicalObjectRole {
        self.role
    }

    /// Returns the zero-based position in the complete raw object graph.
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Returns the provider/application semantic identity for this exact object.
    pub const fn semantic_identity(&self) -> EvidenceDigest {
        self.semantic_identity
    }

    /// Returns the digest of the canonical serialized physical claim.
    pub const fn raw_claim_digest(&self) -> EvidenceDigest {
        self.raw_claim_digest
    }

    /// Returns the exact restart-verifiable physical object claim.
    pub const fn claim(&self) -> &ResearchObjectClaim {
        &self.claim
    }
}

/// Exact persisted evidence partition and its physical logical-object claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderLogicalPartitionClaim {
    family: LogicalPartitionFamily,
    partition_ordinal: u32,
    item_range: LogicalItemRange,
    schema_identity: EvidenceDigest,
    semantic_digest: EvidenceDigest,
    raw_claim_digest: EvidenceDigest,
    claim: ResearchObjectClaim,
}

impl PersistedProviderLogicalPartitionClaim {
    /// Returns the closed evidence family.
    pub const fn family(&self) -> LogicalPartitionFamily {
        self.family
    }

    /// Returns the zero-based partition ordinal within its family.
    pub const fn partition_ordinal(&self) -> u32 {
        self.partition_ordinal
    }

    /// Returns the exact represented global item range.
    pub const fn item_range(&self) -> LogicalItemRange {
        self.item_range
    }

    /// Returns the code/provider-owned schema identity.
    pub const fn schema_identity(&self) -> EvidenceDigest {
        self.schema_identity
    }

    /// Returns the digest binding family, range, schema, and physical object.
    pub const fn semantic_digest(&self) -> EvidenceDigest {
        self.semantic_digest
    }

    /// Returns the digest of the canonical serialized physical claim.
    pub const fn raw_claim_digest(&self) -> EvidenceDigest {
        self.raw_claim_digest
    }

    /// Returns the exact restart-verifiable physical object claim.
    pub const fn claim(&self) -> &ResearchObjectClaim {
        &self.claim
    }
}

/// Historical value-only evidence for one sealed provider logical publication.
///
/// This cannot recreate live publication authority. It is sufficient to reopen and verify every
/// physical object, partition, canonical expectation, terminal receipt, and common binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderLogicalPublicationBinding {
    binding_digest: EvidenceDigest,
    terminal: ProviderLogicalTerminalReceipt,
    required_partition_families: Box<[LogicalPartitionFamily]>,
    objects: Box<[PersistedProviderLogicalObjectClaim]>,
    partitions: Box<[PersistedProviderLogicalPartitionClaim]>,
    canonical_partitions: Box<[CanonicalPartitionExpectation]>,
}

impl PersistedProviderLogicalPublicationBinding {
    /// Returns the common-owned digest of the whole logical publication binding.
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    /// Returns the exact terminal receipt.
    pub const fn terminal(&self) -> &ProviderLogicalTerminalReceipt {
        &self.terminal
    }

    /// Returns the exact sorted closed set required at common publication closure.
    pub const fn required_partition_families(&self) -> &[LogicalPartitionFamily] {
        &self.required_partition_families
    }

    /// Returns ordered raw logical objects and their exact claims.
    pub const fn objects(&self) -> &[PersistedProviderLogicalObjectClaim] {
        &self.objects
    }

    /// Returns ordered provider-native, canonical-map, and other admitted evidence partitions.
    pub const fn partitions(&self) -> &[PersistedProviderLogicalPartitionClaim] {
        &self.partitions
    }

    /// Returns ordered immutable canonical partition expectations.
    pub const fn canonical_partitions(&self) -> &[CanonicalPartitionExpectation] {
        &self.canonical_partitions
    }

    fn try_from_live(
        binding: &SealedProviderLogicalPublicationBinding,
    ) -> Result<Self, CatalogError> {
        let required_partition_families = binding
            .partitions()
            .iter()
            .map(|partition| partition.family())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let objects = binding
            .objects()
            .iter()
            .map(|object| {
                let claim = object.object().claim().clone();
                let claim_json = logical_claim_json(&claim)?;
                Ok(PersistedProviderLogicalObjectClaim {
                    role: object.role(),
                    ordinal: object.ordinal(),
                    semantic_identity: object.semantic_identity(),
                    raw_claim_digest: raw_claim_digest(claim_json.as_bytes()),
                    claim,
                })
            })
            .collect::<Result<Vec<_>, CatalogError>>()?
            .into_boxed_slice();
        let partitions = binding
            .partitions()
            .iter()
            .map(|partition| {
                let claim = partition.object().claim().clone();
                let claim_json = logical_claim_json(&claim)?;
                Ok(PersistedProviderLogicalPartitionClaim {
                    family: partition.family(),
                    partition_ordinal: partition.partition_ordinal(),
                    item_range: partition.item_range(),
                    schema_identity: partition.schema_identity(),
                    semantic_digest: partition.semantic_digest(),
                    raw_claim_digest: raw_claim_digest(claim_json.as_bytes()),
                    claim,
                })
            })
            .collect::<Result<Vec<_>, CatalogError>>()?
            .into_boxed_slice();
        let evidence = Self {
            binding_digest: binding.binding_digest(),
            terminal: binding.terminal().clone(),
            required_partition_families,
            objects,
            partitions,
            canonical_partitions: binding.canonical_partitions().to_vec().into_boxed_slice(),
        };
        evidence.verify_integrity()?;
        Ok(evidence)
    }

    fn verify_integrity(&self) -> Result<(), CatalogError> {
        validate_sha256(self.binding_digest)?;
        validate_sha256(self.terminal.source_revision_digest())?;
        if let Some(attempt) = self.terminal.execution_attempt_digest() {
            validate_sha256(attempt)?;
        }
        validate_sha256(self.terminal.provider_terminal_evidence_digest())?;
        if self.objects.is_empty()
            || self.objects.len() > MAX_PROVIDER_LOGICAL_OBJECTS
            || self.partitions.len() > MAX_PROVIDER_LOGICAL_PARTITIONS
            || self.canonical_partitions.len() > MAX_PROVIDER_CANONICAL_PARTITIONS
            || self.required_partition_families.is_empty()
            || self
                .required_partition_families
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(CatalogError::ProviderLogicalMismatch);
        }

        let mut metadata_bytes = 0usize;
        let mut logical_bytes = 0u64;
        for (ordinal, object) in self.objects.iter().enumerate() {
            if object.ordinal
                != u32::try_from(ordinal).map_err(|_| CatalogError::ProviderLogicalMismatch)?
                || object.claim.size_bytes() == 0
            {
                return Err(CatalogError::ProviderLogicalMismatch);
            }
            validate_sha256(object.semantic_identity)?;
            let claim_json = logical_claim_json(&object.claim)?;
            if raw_claim_digest(claim_json.as_bytes()) != object.raw_claim_digest {
                return Err(CatalogError::ProviderLogicalMismatch);
            }
            metadata_bytes = charge_metadata(metadata_bytes, claim_catalog_bytes(&object.claim)?)?;
            logical_bytes = logical_bytes
                .checked_add(object.claim.size_bytes())
                .ok_or(CatalogError::ProviderLogicalMismatch)?;
        }

        let supplied_families = self
            .partitions
            .iter()
            .map(|partition| partition.family)
            .collect::<BTreeSet<_>>();
        if supplied_families
            .iter()
            .copied()
            .ne(self.required_partition_families.iter().copied())
        {
            return Err(CatalogError::ProviderLogicalMismatch);
        }
        validate_partition_order_and_claims(&self.partitions, &mut metadata_bytes)?;
        validate_canonical_alignment(&self.canonical_partitions, &self.partitions)?;

        let decoded_events =
            family_item_count(&self.partitions, LogicalPartitionFamily::DecodedEvent)?;
        let native_rows =
            family_item_count(&self.partitions, LogicalPartitionFamily::ProviderNative)?;
        let row_map_rows =
            family_item_count(&self.partitions, LogicalPartitionFamily::CanonicalRowMap)?;
        let canonical_rows =
            self.canonical_partitions
                .iter()
                .try_fold(0u64, |total, partition| {
                    total
                        .checked_add(u64::from(partition.row_range().item_count().get()))
                        .ok_or(CatalogError::ProviderLogicalMismatch)
                })?;
        if logical_bytes != self.terminal.total_logical_object_bytes()
            || decoded_events != self.terminal.total_decoded_events()
            || canonical_rows != self.terminal.total_canonical_rows()
            || native_rows != canonical_rows
            || row_map_rows != canonical_rows
        {
            return Err(CatalogError::ProviderLogicalMismatch);
        }

        let raw_set = object_set_digest(&self.objects);
        let partition_set = partition_set_digest(&self.partitions);
        let canonical_set = canonical_set_digest(&self.canonical_partitions);
        if raw_set != self.terminal.raw_object_set_digest()
            || partition_set != self.terminal.evidence_partition_set_digest()
            || canonical_set != self.terminal.canonical_partition_set_digest()
            || terminal_receipt_digest(&self.terminal) != self.terminal.receipt_digest()
            || publication_binding_digest(&self.terminal) != self.binding_digest
        {
            return Err(CatalogError::ProviderLogicalMismatch);
        }
        Ok(())
    }
}

/// Exact immutable relation from an ingest run and analytical generation to logical evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderLogicalGenerationBinding {
    ingest_run_id: Uuid,
    analytical_generation_sequence: u64,
    publication: PersistedProviderLogicalPublicationBinding,
}

impl PersistedProviderLogicalGenerationBinding {
    /// Returns the exact source ingest run retained by the generation.
    pub const fn ingest_run_id(&self) -> Uuid {
        self.ingest_run_id
    }

    /// Returns the exact immutable analytical generation sequence.
    pub const fn analytical_generation_sequence(&self) -> u64 {
        self.analytical_generation_sequence
    }

    /// Returns the complete provider-neutral logical publication evidence.
    pub const fn publication(&self) -> &PersistedProviderLogicalPublicationBinding {
        &self.publication
    }
}

impl Catalog {
    /// Reopens one exact logical provider publication after restart.
    pub fn provider_logical_publication_binding(
        &self,
        binding_digest: EvidenceDigest,
    ) -> Result<Option<PersistedProviderLogicalPublicationBinding>, CatalogError> {
        load_provider_logical_publication_binding(&self.connection, binding_digest)
    }

    /// Resolves one exact immutable generation-to-run logical publication relation.
    pub fn provider_logical_generation_binding(
        &self,
        analytical_generation_sequence: u64,
        binding_digest: EvidenceDigest,
    ) -> Result<Option<PersistedProviderLogicalGenerationBinding>, CatalogError> {
        let generation = i64::try_from(analytical_generation_sequence)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(CatalogError::InvalidRecord)?;
        let run_id = self
            .connection
            .query_row(
                "SELECT generation_input.run_id
                 FROM analytical_generation_provider_publication_bindings AS generation_input
                 JOIN ingest_run_provider_publication_bindings AS run_input
                   ON run_input.run_id=generation_input.run_id
                  AND run_input.publication_digest=generation_input.publication_digest
                 WHERE generation_input.generation_sequence=?1
                   AND generation_input.publication_digest=?2
                   AND generation_input.publication_kind=?3
                   AND run_input.publication_kind=?3
                   AND run_input.logical_binding_digest=?2",
                params![generation, digest_bytes(binding_digest), PUBLICATION_KIND],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(run_id) = run_id else {
            return Ok(None);
        };
        let ingest_run_id = Uuid::parse_str(&run_id).map_err(|_| CatalogError::CorruptCatalog)?;
        let publication =
            load_provider_logical_publication_binding(&self.connection, binding_digest)?
                .ok_or(CatalogError::CorruptCatalog)?;
        Ok(Some(PersistedProviderLogicalGenerationBinding {
            ingest_run_id,
            analytical_generation_sequence,
            publication,
        }))
    }
}

pub(crate) fn retain_sealed_provider_logical_publication_binding(
    transaction: &Transaction<'_>,
    run_id: Uuid,
    binding: &SealedProviderLogicalPublicationBinding,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    let evidence = PersistedProviderLogicalPublicationBinding::try_from_live(binding)?;
    let run_source: String = transaction.query_row(
        "SELECT source_id FROM ingest_runs
         WHERE run_id=?1 AND state='reserved' AND operation='persist'",
        [run_id.to_string()],
        |row| row.get(0),
    )?;
    if run_source != evidence.terminal.source_id().as_str() {
        return Err(CatalogError::ProviderLogicalMismatch);
    }
    let used: bool = transaction.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM ingest_run_provider_publication_bindings
             WHERE publication_digest=?1
         )",
        [digest_bytes(evidence.binding_digest)],
        |row| row.get(0),
    )?;
    if used {
        return Err(CatalogError::ProviderLogicalConflict);
    }

    insert_logical_binding(transaction, &evidence, recorded_at)?;
    let inserted = transaction.execute(
        "INSERT INTO ingest_run_provider_publication_bindings
         (run_id, input_ordinal, publication_digest, publication_kind, source_id,
          response_binding_digest, event_binding_digest, composite_binding_digest,
          option_binding_digest, logical_binding_digest)
         VALUES (?1, 0, ?2, ?3, ?4, NULL, NULL, NULL, NULL, ?2)",
        params![
            run_id.to_string(),
            digest_bytes(evidence.binding_digest),
            PUBLICATION_KIND,
            evidence.terminal.source_id().as_str(),
        ],
    )?;
    if inserted != 1 {
        return Err(CatalogError::ProviderLogicalConflict);
    }
    append_audit(
        transaction,
        "provider-logical-publication.retained",
        &run_id.to_string(),
        evidence.binding_digest.bytes(),
        recorded_at,
    )?;
    let retained = load_provider_logical_publication_binding(transaction, evidence.binding_digest)?
        .ok_or(CatalogError::ProviderLogicalConflict)?;
    if retained != evidence {
        return Err(CatalogError::ProviderLogicalConflict);
    }
    Ok(())
}

fn insert_logical_binding(
    connection: &Connection,
    evidence: &PersistedProviderLogicalPublicationBinding,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    let terminal_json = serde_json::to_vec(&evidence.terminal)?;
    connection.execute(
        "INSERT OR IGNORE INTO provider_logical_publication_bindings
         (binding_digest, binding_format_version, source_id, terminal_receipt_digest,
          terminal_json, required_family_count, object_count, partition_count,
          canonical_partition_count, recorded_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            digest_bytes(evidence.binding_digest),
            BINDING_FORMAT_VERSION,
            evidence.terminal.source_id().as_str(),
            digest_bytes(evidence.terminal.receipt_digest()),
            terminal_json,
            to_i64(evidence.required_partition_families.len())?,
            to_i64(evidence.objects.len())?,
            to_i64(evidence.partitions.len())?,
            to_i64(evidence.canonical_partitions.len())?,
            recorded_at.unix_nanos(),
        ],
    )?;

    for (ordinal, family) in evidence.required_partition_families.iter().enumerate() {
        connection.execute(
            "INSERT OR IGNORE INTO provider_logical_publication_required_families
             (binding_digest, family_ordinal, family)
             VALUES (?1, ?2, ?3)",
            params![
                digest_bytes(evidence.binding_digest),
                to_i64(ordinal)?,
                partition_family_name(*family),
            ],
        )?;
    }
    for object in &evidence.objects {
        insert_logical_claim(
            connection,
            object.raw_claim_digest,
            &object.claim,
            recorded_at,
        )?;
        connection.execute(
            "INSERT OR IGNORE INTO provider_logical_publication_objects
             (binding_digest, object_ordinal, object_role, semantic_identity,
              raw_claim_digest, physical_receipt_digest)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                digest_bytes(evidence.binding_digest),
                i64::from(object.ordinal),
                object_role_name(object.role),
                digest_bytes(object.semantic_identity),
                digest_bytes(object.raw_claim_digest),
                digest_bytes(object.claim.physical_receipt_digest()),
            ],
        )?;
    }
    for partition in &evidence.partitions {
        insert_logical_claim(
            connection,
            partition.raw_claim_digest,
            &partition.claim,
            recorded_at,
        )?;
        connection.execute(
            "INSERT OR IGNORE INTO provider_logical_publication_partitions
             (binding_digest, partition_family_ordinal, partition_family,
              partition_ordinal, first_item_ordinal, item_count, schema_identity,
              semantic_digest, raw_claim_digest,
              physical_receipt_digest)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                digest_bytes(evidence.binding_digest),
                to_i64(
                    evidence
                        .required_partition_families
                        .binary_search(&partition.family)
                        .map_err(|_| CatalogError::ProviderLogicalMismatch)?
                )?,
                partition_family_name(partition.family),
                i64::from(partition.partition_ordinal),
                to_i64(partition.item_range.first_ordinal())?,
                i64::from(partition.item_range.item_count().get()),
                digest_bytes(partition.schema_identity),
                digest_bytes(partition.semantic_digest),
                digest_bytes(partition.raw_claim_digest),
                digest_bytes(partition.claim.physical_receipt_digest()),
            ],
        )?;
    }
    for expected in &evidence.canonical_partitions {
        connection.execute(
            "INSERT OR IGNORE INTO provider_logical_publication_canonical_expectations
             (binding_digest, partition_ordinal, first_row_ordinal, row_count,
              schema_identity, semantic_digest, aligned_native_partition,
              aligned_row_map_partition)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                digest_bytes(evidence.binding_digest),
                i64::from(expected.partition_ordinal()),
                to_i64(expected.row_range().first_ordinal())?,
                i64::from(expected.row_range().item_count().get()),
                digest_bytes(expected.schema_identity()),
                digest_bytes(expected.semantic_digest()),
                i64::from(expected.aligned_native_partition()),
                i64::from(expected.aligned_row_map_partition()),
            ],
        )?;
    }
    Ok(())
}

fn insert_logical_claim(
    connection: &Connection,
    claim_digest: EvidenceDigest,
    claim: &ResearchObjectClaim,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    let claim_json = logical_claim_json(claim)?;
    if claim_json.is_empty()
        || claim_json.len() > MAX_PROVIDER_LOGICAL_CATALOG_BYTES
        || raw_claim_digest(claim_json.as_bytes()) != claim_digest
    {
        return Err(CatalogError::ProviderLogicalMismatch);
    }
    connection.execute(
        "INSERT OR IGNORE INTO sealed_raw_objects
         (raw_claim_digest, raw_claim_kind, physical_receipt_digest, relative_reference,
          content_digest, size_bytes, integrity_chunk_bytes, unit_count, raw_claim_json,
          recorded_at_ns)
         VALUES (?1, 'logical_object', ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            digest_bytes(claim_digest),
            digest_bytes(claim.physical_receipt_digest()),
            claim.relative_reference(),
            digest_bytes(claim.content_digest()),
            to_i64(claim.size_bytes())?,
            to_i64(claim.integrity_chunk_bytes())?,
            to_i64(claim.chunks().len())?,
            claim_json,
            recorded_at.unix_nanos(),
        ],
    )?;
    let exact: bool = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM sealed_raw_objects
             WHERE raw_claim_digest=?1 AND raw_claim_kind='logical_object'
               AND physical_receipt_digest=?2 AND relative_reference=?3
               AND content_digest=?4 AND size_bytes=?5
               AND integrity_chunk_bytes=?6 AND unit_count=?7 AND raw_claim_json=?8
         )",
        params![
            digest_bytes(claim_digest),
            digest_bytes(claim.physical_receipt_digest()),
            claim.relative_reference(),
            digest_bytes(claim.content_digest()),
            to_i64(claim.size_bytes())?,
            to_i64(claim.integrity_chunk_bytes())?,
            to_i64(claim.chunks().len())?,
            claim_json,
        ],
        |row| row.get(0),
    )?;
    if exact {
        Ok(())
    } else {
        Err(CatalogError::ProviderLogicalConflict)
    }
}

fn load_provider_logical_publication_binding(
    connection: &Connection,
    binding_digest: EvidenceDigest,
) -> Result<Option<PersistedProviderLogicalPublicationBinding>, CatalogError> {
    validate_sha256(binding_digest)?;
    type Header = (Vec<u8>, Vec<u8>, String, i64, i64, i64, i64);
    let header: Option<Header> = connection
        .query_row(
            "SELECT terminal_json, terminal_receipt_digest, source_id,
                    required_family_count, object_count, partition_count,
                    canonical_partition_count
             FROM provider_logical_publication_bindings
             WHERE binding_digest=?1 AND binding_format_version=?2",
            params![digest_bytes(binding_digest), BINDING_FORMAT_VERSION],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        terminal_json,
        terminal_receipt_digest,
        source_id,
        family_count,
        object_count,
        partition_count,
        canonical_count,
    )) = header
    else {
        return Ok(None);
    };
    let terminal: ProviderLogicalTerminalReceipt = serde_json::from_slice(&terminal_json)?;
    if terminal.source_id().as_str() != source_id
        || terminal.receipt_digest() != parse_digest(1, &terminal_receipt_digest)?
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let family_count = bounded_count(family_count, 6)?;
    let object_count = bounded_count(object_count, MAX_PROVIDER_LOGICAL_OBJECTS)?;
    let partition_count = bounded_count(partition_count, MAX_PROVIDER_LOGICAL_PARTITIONS)?;
    let canonical_count = bounded_count(canonical_count, MAX_PROVIDER_CANONICAL_PARTITIONS)?;

    let required_partition_families =
        load_required_families(connection, binding_digest, family_count)?;
    let objects = load_objects(connection, binding_digest, object_count)?;
    let partitions = load_partitions(
        connection,
        binding_digest,
        &required_partition_families,
        partition_count,
    )?;
    let canonical_partitions =
        load_canonical_expectations(connection, binding_digest, canonical_count)?;
    let evidence = PersistedProviderLogicalPublicationBinding {
        binding_digest,
        terminal,
        required_partition_families,
        objects,
        partitions,
        canonical_partitions,
    };
    evidence
        .verify_integrity()
        .map_err(|_| CatalogError::CorruptCatalog)?;
    Ok(Some(evidence))
}

fn load_required_families(
    connection: &Connection,
    binding_digest: EvidenceDigest,
    expected: usize,
) -> Result<Box<[LogicalPartitionFamily]>, CatalogError> {
    let mut statement = connection.prepare(
        "SELECT family_ordinal, family
         FROM provider_logical_publication_required_families
         WHERE binding_digest=?1 ORDER BY family_ordinal",
    )?;
    let rows = statement.query_map([digest_bytes(binding_digest)], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(expected)
        .map_err(|_| CatalogError::Allocation)?;
    for row in rows {
        let (ordinal, family) = row?;
        if ordinal != to_i64(values.len())? || values.len() == expected {
            return Err(CatalogError::CorruptCatalog);
        }
        values.push(parse_partition_family(&family)?);
    }
    if values.len() != expected {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(values.into_boxed_slice())
}

fn load_objects(
    connection: &Connection,
    binding_digest: EvidenceDigest,
    expected: usize,
) -> Result<Box<[PersistedProviderLogicalObjectClaim]>, CatalogError> {
    let mut statement = connection.prepare(
        "SELECT object.object_ordinal, object.object_role, object.semantic_identity,
                object.raw_claim_digest, object.physical_receipt_digest,
                claim.raw_claim_json
         FROM provider_logical_publication_objects AS object
         JOIN sealed_raw_objects AS claim
           ON claim.raw_claim_digest=object.raw_claim_digest
          AND claim.physical_receipt_digest=object.physical_receipt_digest
          AND claim.raw_claim_kind='logical_object'
         WHERE object.binding_digest=?1 ORDER BY object.object_ordinal",
    )?;
    let rows = statement.query_map([digest_bytes(binding_digest)], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Vec<u8>>(2)?,
            row.get::<_, Vec<u8>>(3)?,
            row.get::<_, Vec<u8>>(4)?,
            row.get::<_, String>(5)?,
        ))
    })?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(expected)
        .map_err(|_| CatalogError::Allocation)?;
    for row in rows {
        let (ordinal, role, semantic, claim_digest, physical_receipt_digest, claim_json) = row?;
        if ordinal != to_i64(values.len())? || values.len() == expected {
            return Err(CatalogError::CorruptCatalog);
        }
        let claim = parse_logical_claim(&claim_json)?;
        if claim.physical_receipt_digest() != parse_digest(1, &physical_receipt_digest)? {
            return Err(CatalogError::CorruptCatalog);
        }
        values.push(PersistedProviderLogicalObjectClaim {
            role: parse_object_role(&role)?,
            ordinal: u32::try_from(ordinal).map_err(|_| CatalogError::CorruptCatalog)?,
            semantic_identity: parse_digest(1, &semantic)?,
            raw_claim_digest: parse_digest(1, &claim_digest)?,
            claim,
        });
    }
    if values.len() != expected {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(values.into_boxed_slice())
}

fn load_partitions(
    connection: &Connection,
    binding_digest: EvidenceDigest,
    required_families: &[LogicalPartitionFamily],
    expected: usize,
) -> Result<Box<[PersistedProviderLogicalPartitionClaim]>, CatalogError> {
    let mut statement = connection.prepare(
        "SELECT partition.partition_family_ordinal, partition.partition_family,
                partition.partition_ordinal,
                partition.first_item_ordinal, partition.item_count,
                partition.schema_identity, partition.semantic_digest,
                partition.raw_claim_digest, partition.physical_receipt_digest,
                claim.raw_claim_json
         FROM provider_logical_publication_partitions AS partition
         JOIN sealed_raw_objects AS claim
           ON claim.raw_claim_digest=partition.raw_claim_digest
          AND claim.physical_receipt_digest=partition.physical_receipt_digest
          AND claim.raw_claim_kind='logical_object'
         WHERE partition.binding_digest=?1
         ORDER BY partition.partition_family_ordinal, partition.partition_ordinal",
    )?;
    let rows = statement.query_map([digest_bytes(binding_digest)], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, Vec<u8>>(5)?,
            row.get::<_, Vec<u8>>(6)?,
            row.get::<_, Vec<u8>>(7)?,
            row.get::<_, Vec<u8>>(8)?,
            row.get::<_, String>(9)?,
        ))
    })?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(expected)
        .map_err(|_| CatalogError::Allocation)?;
    for row in rows {
        if values.len() == expected {
            return Err(CatalogError::CorruptCatalog);
        }
        let (
            family_ordinal,
            family,
            ordinal,
            first,
            count,
            schema,
            semantic,
            claim_digest,
            physical_receipt_digest,
            claim_json,
        ) = row?;
        let family_ordinal =
            usize::try_from(family_ordinal).map_err(|_| CatalogError::CorruptCatalog)?;
        let family = parse_partition_family(&family)?;
        if required_families.get(family_ordinal) != Some(&family) {
            return Err(CatalogError::CorruptCatalog);
        }
        let count = u32::try_from(count)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(CatalogError::CorruptCatalog)?;
        let first = u64::try_from(first).map_err(|_| CatalogError::CorruptCatalog)?;
        let claim = parse_logical_claim(&claim_json)?;
        if claim.physical_receipt_digest() != parse_digest(1, &physical_receipt_digest)? {
            return Err(CatalogError::CorruptCatalog);
        }
        values.push(PersistedProviderLogicalPartitionClaim {
            family,
            partition_ordinal: u32::try_from(ordinal).map_err(|_| CatalogError::CorruptCatalog)?,
            item_range: LogicalItemRange::try_new(first, count)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            schema_identity: parse_digest(1, &schema)?,
            semantic_digest: parse_digest(1, &semantic)?,
            raw_claim_digest: parse_digest(1, &claim_digest)?,
            claim,
        });
    }
    if values.len() != expected {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(values.into_boxed_slice())
}

fn load_canonical_expectations(
    connection: &Connection,
    binding_digest: EvidenceDigest,
    expected: usize,
) -> Result<Box<[CanonicalPartitionExpectation]>, CatalogError> {
    let mut statement = connection.prepare(
        "SELECT partition_ordinal, first_row_ordinal, row_count, schema_identity,
                semantic_digest, aligned_native_partition, aligned_row_map_partition
         FROM provider_logical_publication_canonical_expectations
         WHERE binding_digest=?1 ORDER BY partition_ordinal",
    )?;
    let rows = statement.query_map([digest_bytes(binding_digest)], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Vec<u8>>(3)?,
            row.get::<_, Vec<u8>>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
        ))
    })?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(expected)
        .map_err(|_| CatalogError::Allocation)?;
    for row in rows {
        let (ordinal, first, count, schema, semantic, native, row_map) = row?;
        if ordinal != to_i64(values.len())? || values.len() == expected {
            return Err(CatalogError::CorruptCatalog);
        }
        let count = u32::try_from(count)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(CatalogError::CorruptCatalog)?;
        values.push(
            CanonicalPartitionExpectation::try_new(
                u32::try_from(ordinal).map_err(|_| CatalogError::CorruptCatalog)?,
                LogicalItemRange::try_new(
                    u64::try_from(first).map_err(|_| CatalogError::CorruptCatalog)?,
                    count,
                )
                .map_err(|_| CatalogError::CorruptCatalog)?,
                parse_digest(1, &schema)?,
                parse_digest(1, &semantic)?,
                u32::try_from(native).map_err(|_| CatalogError::CorruptCatalog)?,
                u32::try_from(row_map).map_err(|_| CatalogError::CorruptCatalog)?,
            )
            .map_err(|_| CatalogError::CorruptCatalog)?,
        );
    }
    if values.len() != expected {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(values.into_boxed_slice())
}

fn validate_partition_order_and_claims(
    partitions: &[PersistedProviderLogicalPartitionClaim],
    metadata_bytes: &mut usize,
) -> Result<(), CatalogError> {
    let mut prior_family = None;
    let mut expected_partition = 0u32;
    let mut expected_item = None;
    for partition in partitions {
        validate_sha256(partition.schema_identity)?;
        validate_sha256(partition.semantic_digest)?;
        let claim_json = logical_claim_json(&partition.claim)?;
        if raw_claim_digest(claim_json.as_bytes()) != partition.raw_claim_digest
            || partition.claim.size_bytes() == 0
            || partition_semantic_digest(partition) != partition.semantic_digest
        {
            return Err(CatalogError::ProviderLogicalMismatch);
        }
        *metadata_bytes = charge_metadata(*metadata_bytes, claim_catalog_bytes(&partition.claim)?)?;
        if prior_family != Some(partition.family) {
            if prior_family.is_some_and(|prior| prior >= partition.family) {
                return Err(CatalogError::ProviderLogicalMismatch);
            }
            prior_family = Some(partition.family);
            expected_partition = 0;
            expected_item = Some(partition.item_range.first_ordinal());
        }
        if partition.partition_ordinal != expected_partition
            || expected_item != Some(partition.item_range.first_ordinal())
        {
            return Err(CatalogError::ProviderLogicalMismatch);
        }
        expected_partition = expected_partition
            .checked_add(1)
            .ok_or(CatalogError::ProviderLogicalMismatch)?;
        expected_item = Some(
            partition
                .item_range
                .end_exclusive()
                .map_err(|_| CatalogError::ProviderLogicalMismatch)?,
        );
    }
    Ok(())
}

fn validate_canonical_alignment(
    canonical: &[CanonicalPartitionExpectation],
    partitions: &[PersistedProviderLogicalPartitionClaim],
) -> Result<(), CatalogError> {
    let native = partitions
        .iter()
        .filter(|partition| partition.family == LogicalPartitionFamily::ProviderNative)
        .collect::<Vec<_>>();
    let row_maps = partitions
        .iter()
        .filter(|partition| partition.family == LogicalPartitionFamily::CanonicalRowMap)
        .collect::<Vec<_>>();
    let mut expected_row = canonical
        .first()
        .map(|partition| partition.row_range().first_ordinal());
    for (ordinal, expected) in canonical.iter().enumerate() {
        if expected.partition_ordinal()
            != u32::try_from(ordinal).map_err(|_| CatalogError::ProviderLogicalMismatch)?
            || expected_row != Some(expected.row_range().first_ordinal())
        {
            return Err(CatalogError::ProviderLogicalMismatch);
        }
        let native = native
            .get(
                usize::try_from(expected.aligned_native_partition())
                    .map_err(|_| CatalogError::ProviderLogicalMismatch)?,
            )
            .ok_or(CatalogError::ProviderLogicalMismatch)?;
        let row_map = row_maps
            .get(
                usize::try_from(expected.aligned_row_map_partition())
                    .map_err(|_| CatalogError::ProviderLogicalMismatch)?,
            )
            .ok_or(CatalogError::ProviderLogicalMismatch)?;
        if native.partition_ordinal != expected.aligned_native_partition()
            || row_map.partition_ordinal != expected.aligned_row_map_partition()
            || native.item_range != expected.row_range()
            || row_map.item_range != expected.row_range()
        {
            return Err(CatalogError::ProviderLogicalMismatch);
        }
        expected_row = Some(
            expected
                .row_range()
                .end_exclusive()
                .map_err(|_| CatalogError::ProviderLogicalMismatch)?,
        );
    }
    if canonical.is_empty() != (native.is_empty() && row_maps.is_empty())
        || canonical.len() != native.len()
        || canonical.len() != row_maps.len()
    {
        return Err(CatalogError::ProviderLogicalMismatch);
    }
    Ok(())
}

fn family_item_count(
    partitions: &[PersistedProviderLogicalPartitionClaim],
    family: LogicalPartitionFamily,
) -> Result<u64, CatalogError> {
    partitions
        .iter()
        .filter(|partition| partition.family == family)
        .try_fold(0u64, |total, partition| {
            total
                .checked_add(u64::from(partition.item_range.item_count().get()))
                .ok_or(CatalogError::ProviderLogicalMismatch)
        })
}

fn object_set_digest(objects: &[PersistedProviderLogicalObjectClaim]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(LOGICAL_OBJECT_SET_DOMAIN);
    hash.update((objects.len() as u64).to_be_bytes());
    for object in objects {
        hash.update([object_role_tag(object.role)]);
        hash.update(object.ordinal.to_be_bytes());
        hash_digest(&mut hash, object.semantic_identity);
        hash_digest(&mut hash, object.claim.content_digest());
        hash.update(object.claim.size_bytes().to_be_bytes());
        hash_digest(&mut hash, object.claim.physical_receipt_digest());
    }
    sha256(hash)
}

fn partition_semantic_digest(partition: &PersistedProviderLogicalPartitionClaim) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(LOGICAL_PARTITION_SEMANTIC_DOMAIN);
    hash.update([partition_family_tag(partition.family)]);
    hash.update(partition.partition_ordinal.to_be_bytes());
    hash.update(partition.item_range.first_ordinal().to_be_bytes());
    hash.update(partition.item_range.item_count().get().to_be_bytes());
    hash_digest(&mut hash, partition.schema_identity);
    hash_digest(&mut hash, partition.claim.content_digest());
    hash.update(partition.claim.size_bytes().to_be_bytes());
    hash_digest(&mut hash, partition.claim.physical_receipt_digest());
    sha256(hash)
}

fn partition_set_digest(partitions: &[PersistedProviderLogicalPartitionClaim]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(LOGICAL_PARTITION_SET_DOMAIN);
    hash.update((partitions.len() as u64).to_be_bytes());
    for partition in partitions {
        hash.update([partition_family_tag(partition.family)]);
        hash.update(partition.partition_ordinal.to_be_bytes());
        hash.update(partition.item_range.first_ordinal().to_be_bytes());
        hash.update(partition.item_range.item_count().get().to_be_bytes());
        hash_digest(&mut hash, partition.schema_identity);
        hash_digest(&mut hash, partition.semantic_digest);
    }
    sha256(hash)
}

fn canonical_set_digest(partitions: &[CanonicalPartitionExpectation]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(CANONICAL_PARTITION_SET_DOMAIN);
    hash.update((partitions.len() as u64).to_be_bytes());
    for partition in partitions {
        hash.update(partition.partition_ordinal().to_be_bytes());
        hash.update(partition.row_range().first_ordinal().to_be_bytes());
        hash.update(partition.row_range().item_count().get().to_be_bytes());
        hash_digest(&mut hash, partition.schema_identity());
        hash_digest(&mut hash, partition.semantic_digest());
        hash.update(partition.aligned_native_partition().to_be_bytes());
        hash.update(partition.aligned_row_map_partition().to_be_bytes());
    }
    sha256(hash)
}

fn terminal_receipt_digest(receipt: &ProviderLogicalTerminalReceipt) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(LOGICAL_TERMINAL_RECEIPT_DOMAIN);
    hash_field(&mut hash, receipt.source_id().as_str().as_bytes());
    hash_digest(&mut hash, receipt.source_revision_digest());
    hash_optional_digest(&mut hash, receipt.execution_attempt_digest());
    hash_digest(&mut hash, receipt.provider_terminal_evidence_digest());
    hash_digest(&mut hash, receipt.raw_object_set_digest());
    hash_digest(&mut hash, receipt.evidence_partition_set_digest());
    hash_digest(&mut hash, receipt.canonical_partition_set_digest());
    hash.update(receipt.total_decoded_events().to_be_bytes());
    hash.update(receipt.total_canonical_rows().to_be_bytes());
    hash.update(receipt.total_logical_object_bytes().to_be_bytes());
    sha256(hash)
}

fn publication_binding_digest(receipt: &ProviderLogicalTerminalReceipt) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(LOGICAL_PUBLICATION_BINDING_DOMAIN);
    hash_digest(&mut hash, receipt.receipt_digest());
    hash_digest(&mut hash, receipt.raw_object_set_digest());
    hash_digest(&mut hash, receipt.evidence_partition_set_digest());
    hash_digest(&mut hash, receipt.canonical_partition_set_digest());
    sha256(hash)
}

fn charge_metadata(total: usize, bytes: usize) -> Result<usize, CatalogError> {
    total
        .checked_add(bytes)
        .filter(|total| *total <= MAX_PROVIDER_LOGICAL_CATALOG_BYTES)
        .ok_or(CatalogError::ProviderLogicalMismatch)
}

fn logical_claim_json(claim: &ResearchObjectClaim) -> Result<String, CatalogError> {
    serde_json::to_string(&SealedResearchRawClaim::LogicalObject(claim.clone())).map_err(Into::into)
}

fn claim_catalog_bytes(claim: &ResearchObjectClaim) -> Result<usize, CatalogError> {
    logical_claim_json(claim).map(|encoded| encoded.len())
}

fn parse_logical_claim(json: &str) -> Result<ResearchObjectClaim, CatalogError> {
    let claim = match serde_json::from_str(json)? {
        SealedResearchRawClaim::LogicalObject(claim) => claim,
        SealedResearchRawClaim::JournalSegment(_) => return Err(CatalogError::CorruptCatalog),
    };
    if logical_claim_json(&claim)? == json {
        Ok(claim)
    } else {
        Err(CatalogError::CorruptCatalog)
    }
}

fn validate_sha256(digest: EvidenceDigest) -> Result<(), CatalogError> {
    if digest.algorithm() == DigestAlgorithm::Sha256 && digest.bytes() != [0; 32] {
        Ok(())
    } else {
        Err(CatalogError::ProviderLogicalMismatch)
    }
}

fn bounded_count(value: i64, maximum: usize) -> Result<usize, CatalogError> {
    usize::try_from(value)
        .ok()
        .filter(|value| *value <= maximum)
        .ok_or(CatalogError::CorruptCatalog)
}

fn to_i64<T>(value: T) -> Result<i64, CatalogError>
where
    i64: TryFrom<T>,
{
    i64::try_from(value).map_err(|_| CatalogError::InvalidRecord)
}

const fn object_role_name(role: LogicalObjectRole) -> &'static str {
    match role {
        LogicalObjectRole::Catalog => "catalog",
        LogicalObjectRole::ProviderPayload => "provider_payload",
        LogicalObjectRole::ExpandedPayload => "expanded_payload",
        LogicalObjectRole::ProviderComponent => "provider_component",
    }
}

const fn object_role_tag(role: LogicalObjectRole) -> u8 {
    match role {
        LogicalObjectRole::Catalog => 1,
        LogicalObjectRole::ProviderPayload => 2,
        LogicalObjectRole::ExpandedPayload => 3,
        LogicalObjectRole::ProviderComponent => 4,
    }
}

fn parse_object_role(value: &str) -> Result<LogicalObjectRole, CatalogError> {
    match value {
        "catalog" => Ok(LogicalObjectRole::Catalog),
        "provider_payload" => Ok(LogicalObjectRole::ProviderPayload),
        "expanded_payload" => Ok(LogicalObjectRole::ExpandedPayload),
        "provider_component" => Ok(LogicalObjectRole::ProviderComponent),
        _ => Err(CatalogError::CorruptCatalog),
    }
}

const fn partition_family_name(family: LogicalPartitionFamily) -> &'static str {
    match family {
        LogicalPartitionFamily::DecodedEvent => "decoded_event",
        LogicalPartitionFamily::ProviderNative => "provider_native",
        LogicalPartitionFamily::CanonicalRowMap => "canonical_row_map",
        LogicalPartitionFamily::ResolverAssertion => "resolver_assertion",
        LogicalPartitionFamily::ResolverOutcome => "resolver_outcome",
        LogicalPartitionFamily::ResolverConflict => "resolver_conflict",
    }
}

const fn partition_family_tag(family: LogicalPartitionFamily) -> u8 {
    match family {
        LogicalPartitionFamily::DecodedEvent => 1,
        LogicalPartitionFamily::ProviderNative => 2,
        LogicalPartitionFamily::CanonicalRowMap => 3,
        LogicalPartitionFamily::ResolverAssertion => 4,
        LogicalPartitionFamily::ResolverOutcome => 5,
        LogicalPartitionFamily::ResolverConflict => 6,
    }
}

fn parse_partition_family(value: &str) -> Result<LogicalPartitionFamily, CatalogError> {
    match value {
        "decoded_event" => Ok(LogicalPartitionFamily::DecodedEvent),
        "provider_native" => Ok(LogicalPartitionFamily::ProviderNative),
        "canonical_row_map" => Ok(LogicalPartitionFamily::CanonicalRowMap),
        "resolver_assertion" => Ok(LogicalPartitionFamily::ResolverAssertion),
        "resolver_outcome" => Ok(LogicalPartitionFamily::ResolverOutcome),
        "resolver_conflict" => Ok(LogicalPartitionFamily::ResolverConflict),
        _ => Err(CatalogError::CorruptCatalog),
    }
}

fn hash_field(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn hash_digest(hash: &mut Sha256, digest: EvidenceDigest) {
    hash.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hash.update(digest.bytes());
}

fn hash_optional_digest(hash: &mut Sha256, digest: Option<EvidenceDigest>) {
    match digest {
        Some(digest) => {
            hash.update([1]);
            hash_digest(hash, digest);
        }
        None => hash.update([0]),
    }
}

fn sha256(hash: Sha256) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn digest_bytes(digest: EvidenceDigest) -> [u8; 32] {
    digest.bytes()
}
