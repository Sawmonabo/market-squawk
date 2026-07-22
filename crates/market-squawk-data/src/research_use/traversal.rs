//! Bounded iterative traversal of exact immutable analytical lineage.

use std::collections::BTreeMap;
use std::time::Instant;

use market_squawk_domain::{SchemaVersion, SourceId};
use rusqlite::{OptionalExtension as _, Transaction, params};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::catalog::{ResearchUseCatalogError, ResearchUseRequest};
use super::{
    ResearchUseGeneration, ResearchUseGraph, ResearchUseGraphEdge, ResearchUseSourceInput,
};
use crate::{
    DatasetBuildSpecDigest, DatasetId, DatasetManifestRef, DatasetSchemaRef, GenerationKind,
    GenerationParentRelation, Sha256Digest,
};

const GRAPH_FIXED_RETAINED_BYTES: usize = 12 * 8;

pub(super) fn load_graph(
    transaction: &Transaction<'_>,
    request: &ResearchUseRequest,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<ResearchUseGraph, ResearchUseCatalogError> {
    check_control(cancellation, deadline)?;

    let mut retained_bytes = GRAPH_FIXED_RETAINED_BYTES;
    let mut roots = Vec::new();
    roots
        .try_reserve_exact(request.roots.len())
        .map_err(|_| ResearchUseCatalogError::LimitExceeded)?;
    let mut pending = Vec::new();
    pending
        .try_reserve_exact(request.roots.len())
        .map_err(|_| ResearchUseCatalogError::LimitExceeded)?;
    for requested in &request.roots {
        check_control(cancellation, deadline)?;
        let stored = load_generation_by_coordinate(transaction, requested)?
            .ok_or(ResearchUseCatalogError::UnknownGeneration)?;
        let generation = stored.into_generation()?;
        if generation.manifest() != requested {
            return Err(ResearchUseCatalogError::UnknownGeneration);
        }
        charge(
            &mut retained_bytes,
            manifest_retained_bytes(requested)?,
            request.limits.max_retained_bytes(),
        )?;
        roots.push(requested.clone());
        pending.push(generation.sequence());
    }

    let mut nodes = BTreeMap::new();
    let mut edges = Vec::new();
    let mut sources = Vec::new();
    while let Some(sequence) = pending.pop() {
        check_control(cancellation, deadline)?;
        if nodes.contains_key(&sequence) {
            continue;
        }
        if nodes.len() >= request.limits.max_nodes() {
            return Err(ResearchUseCatalogError::LimitExceeded);
        }
        let stored = load_generation_by_sequence(transaction, sequence)?
            .ok_or(ResearchUseCatalogError::CorruptCatalog)?;
        let generation = stored.into_generation()?;
        charge(
            &mut retained_bytes,
            generation_retained_bytes(&generation)?,
            request.limits.max_retained_bytes(),
        )?;
        let parents = load_parents(transaction, &generation, cancellation, deadline)?;
        if parents.len() != generation.parent_count() {
            return Err(ResearchUseCatalogError::CorruptCatalog);
        }
        if edges
            .len()
            .checked_add(parents.len())
            .is_none_or(|count| count > request.limits.max_edges())
        {
            return Err(ResearchUseCatalogError::LimitExceeded);
        }
        for (parent_sequence, relation) in parents {
            charge(&mut retained_bytes, 17, request.limits.max_retained_bytes())?;
            let edge = ResearchUseGraphEdge::try_new(sequence, parent_sequence, relation)
                .map_err(|_| ResearchUseCatalogError::CorruptCatalog)?;
            edges.push(edge);
            pending.push(parent_sequence);
        }
        if generation.kind() == GenerationKind::Ingest {
            if sources.len() >= request.limits.max_sources() {
                return Err(ResearchUseCatalogError::LimitExceeded);
            }
            let source = load_source_input(transaction, sequence)?;
            charge(
                &mut retained_bytes,
                source_retained_bytes(&source)?,
                request.limits.max_retained_bytes(),
            )?;
            sources.push(source);
        } else if source_input_exists(transaction, sequence)? {
            return Err(ResearchUseCatalogError::CorruptCatalog);
        }
        nodes.insert(sequence, generation);
    }

    ResearchUseGraph::try_new(
        roots,
        nodes.into_values().collect(),
        edges,
        sources,
        request.limits,
    )
    .map_err(|error| match error {
        super::ResearchUseError::InvalidGraph
            if retained_bytes > request.limits.max_retained_bytes() =>
        {
            ResearchUseCatalogError::LimitExceeded
        }
        _ => ResearchUseCatalogError::CorruptCatalog,
    })
}

struct StoredGeneration {
    sequence: i64,
    dataset_id: String,
    manifest_version: i64,
    schema_name: String,
    schema_version: i64,
    schema_fingerprint: Vec<u8>,
    content_hash: Vec<u8>,
    kind: String,
    parent_count: i64,
    build_spec_digest: Option<Vec<u8>>,
}

impl StoredGeneration {
    fn into_generation(self) -> Result<ResearchUseGeneration, ResearchUseCatalogError> {
        let sequence = u64::try_from(self.sequence)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ResearchUseCatalogError::CorruptCatalog)?;
        let manifest_version = u64::try_from(self.manifest_version)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ResearchUseCatalogError::CorruptCatalog)?;
        let schema_version = u16::try_from(self.schema_version)
            .ok()
            .and_then(|value| SchemaVersion::new(value).ok())
            .ok_or(ResearchUseCatalogError::CorruptCatalog)?;
        let schema = DatasetSchemaRef::try_new(
            self.schema_name,
            schema_version,
            parse_digest(self.schema_fingerprint)?,
        )
        .map_err(|_| ResearchUseCatalogError::CorruptCatalog)?;
        let manifest = DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.dataset_id.as_str())
                .map_err(|_| ResearchUseCatalogError::CorruptCatalog)?,
            manifest_version,
            schema,
            Sha256Digest::new(parse_digest(self.content_hash)?),
        )
        .map_err(|_| ResearchUseCatalogError::CorruptCatalog)?;
        let kind = GenerationKind::from_database_name(&self.kind)
            .ok_or(ResearchUseCatalogError::CorruptCatalog)?;
        let build_spec_digest = self
            .build_spec_digest
            .map(parse_digest)
            .transpose()?
            .map(DatasetBuildSpecDigest::try_new)
            .transpose()
            .map_err(|_| ResearchUseCatalogError::CorruptCatalog)?;
        let parent_count = usize::try_from(self.parent_count)
            .map_err(|_| ResearchUseCatalogError::CorruptCatalog)?;
        ResearchUseGeneration::try_new(sequence, manifest, kind, build_spec_digest, parent_count)
            .map_err(|_| ResearchUseCatalogError::CorruptCatalog)
    }
}

fn load_generation_by_coordinate(
    transaction: &Transaction<'_>,
    manifest: &DatasetManifestRef,
) -> Result<Option<StoredGeneration>, ResearchUseCatalogError> {
    transaction
        .query_row(
            "SELECT generation_sequence, dataset_id, manifest_version, schema_name,
                    schema_version, schema_fingerprint, content_hash, generation_kind,
                    parent_count, build_spec_digest
             FROM analytical_generations
             WHERE dataset_id=?1 AND manifest_version=?2",
            params![
                manifest.dataset_id().as_str(),
                to_i64(manifest.manifest_version())?
            ],
            stored_generation,
        )
        .optional()
        .map_err(Into::into)
}

fn load_generation_by_sequence(
    transaction: &Transaction<'_>,
    sequence: u64,
) -> Result<Option<StoredGeneration>, ResearchUseCatalogError> {
    transaction
        .query_row(
            "SELECT generation_sequence, dataset_id, manifest_version, schema_name,
                    schema_version, schema_fingerprint, content_hash, generation_kind,
                    parent_count, build_spec_digest
             FROM analytical_generations WHERE generation_sequence=?1",
            [to_i64(sequence)?],
            stored_generation,
        )
        .optional()
        .map_err(Into::into)
}

fn stored_generation(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredGeneration> {
    Ok(StoredGeneration {
        sequence: row.get(0)?,
        dataset_id: row.get(1)?,
        manifest_version: row.get(2)?,
        schema_name: row.get(3)?,
        schema_version: row.get(4)?,
        schema_fingerprint: row.get(5)?,
        content_hash: row.get(6)?,
        kind: row.get(7)?,
        parent_count: row.get(8)?,
        build_spec_digest: row.get(9)?,
    })
}

fn load_parents(
    transaction: &Transaction<'_>,
    generation: &ResearchUseGeneration,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<Vec<(u64, GenerationParentRelation)>, ResearchUseCatalogError> {
    let mut statement = transaction.prepare(
        "SELECT ordinal, parent_generation_sequence, relation
         FROM analytical_generation_parents
         WHERE child_dataset_id=?1 AND child_manifest_version=?2
         ORDER BY ordinal",
    )?;
    let rows = statement.query_map(
        params![
            generation.manifest().dataset_id().as_str(),
            to_i64(generation.manifest().manifest_version())?
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        },
    )?;
    let mut parents = Vec::new();
    parents
        .try_reserve_exact(generation.parent_count())
        .map_err(|_| ResearchUseCatalogError::LimitExceeded)?;
    for row in rows {
        check_control(cancellation, deadline)?;
        let (ordinal, parent_sequence, relation) = row?;
        if usize::try_from(ordinal).ok() != Some(parents.len()) {
            return Err(ResearchUseCatalogError::CorruptCatalog);
        }
        let parent_sequence = u64::try_from(parent_sequence)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ResearchUseCatalogError::CorruptCatalog)?;
        let relation = GenerationParentRelation::from_database_name(&relation)
            .ok_or(ResearchUseCatalogError::CorruptCatalog)?;
        parents.push((parent_sequence, relation));
    }
    Ok(parents)
}

fn load_source_input(
    transaction: &Transaction<'_>,
    sequence: u64,
) -> Result<ResearchUseSourceInput, ResearchUseCatalogError> {
    let row = transaction
        .query_row(
            "SELECT run_id, source_id, rights_id
             FROM analytical_generation_source_inputs WHERE generation_sequence=?1",
            [to_i64(sequence)?],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()?
        .ok_or(ResearchUseCatalogError::CorruptCatalog)?;
    ResearchUseSourceInput::try_new(
        sequence,
        Uuid::parse_str(&row.0).map_err(|_| ResearchUseCatalogError::CorruptCatalog)?,
        SourceId::try_from(row.1).map_err(|_| ResearchUseCatalogError::CorruptCatalog)?,
        parse_digest(row.2)?,
    )
    .map_err(|_| ResearchUseCatalogError::CorruptCatalog)
}

fn source_input_exists(
    transaction: &Transaction<'_>,
    sequence: u64,
) -> Result<bool, ResearchUseCatalogError> {
    transaction
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM analytical_generation_source_inputs
                WHERE generation_sequence=?1
             )",
            [to_i64(sequence)?],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

pub(super) fn check_control(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), ResearchUseCatalogError> {
    if cancellation.is_cancelled() {
        Err(ResearchUseCatalogError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ResearchUseCatalogError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn generation_retained_bytes(
    generation: &ResearchUseGeneration,
) -> Result<usize, ResearchUseCatalogError> {
    let mut total = 8_usize;
    total = checked_add(total, manifest_retained_bytes(generation.manifest())?)?;
    total = checked_add(total, 1 + 1 + 8)?;
    if generation.build_spec_digest().is_some() {
        total = checked_add(total, 32)?;
    }
    Ok(total)
}

fn manifest_retained_bytes(
    manifest: &DatasetManifestRef,
) -> Result<usize, ResearchUseCatalogError> {
    let mut total = framed_bytes(manifest.dataset_id().as_str())?;
    total = checked_add(total, 8)?;
    total = checked_add(total, framed_bytes(manifest.schema().name())?)?;
    checked_add(total, 2 + 32 + 32)
}

fn source_retained_bytes(
    source: &ResearchUseSourceInput,
) -> Result<usize, ResearchUseCatalogError> {
    checked_add(8 + 16 + 32, framed_bytes(source.source_id().as_str())?)
}

fn framed_bytes(value: &str) -> Result<usize, ResearchUseCatalogError> {
    checked_add(8, value.len())
}

fn charge(
    retained: &mut usize,
    additional: usize,
    limit: usize,
) -> Result<(), ResearchUseCatalogError> {
    *retained = checked_add(*retained, additional)?;
    if *retained > limit {
        Err(ResearchUseCatalogError::LimitExceeded)
    } else {
        Ok(())
    }
}

fn checked_add(left: usize, right: usize) -> Result<usize, ResearchUseCatalogError> {
    left.checked_add(right)
        .ok_or(ResearchUseCatalogError::LimitExceeded)
}

fn parse_digest(value: Vec<u8>) -> Result<[u8; 32], ResearchUseCatalogError> {
    value
        .try_into()
        .map_err(|_| ResearchUseCatalogError::CorruptCatalog)
}

fn to_i64(value: u64) -> Result<i64, ResearchUseCatalogError> {
    i64::try_from(value).map_err(|_| ResearchUseCatalogError::CorruptCatalog)
}
