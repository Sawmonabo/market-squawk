//! Canonical transitive generation graph contracts.

use std::cmp::Ordering;

use market_squawk_domain::SourceId;
use uuid::Uuid;

use super::canonical;
use super::model::{ResearchUseError, ResearchUseGraphDigest, ResearchUseLimits};
use crate::{DatasetBuildSpecDigest, DatasetManifestRef, GenerationKind, GenerationParentRelation};

/// One complete immutable generation in a transitive research graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResearchUseGeneration {
    sequence: u64,
    manifest: DatasetManifestRef,
    kind: GenerationKind,
    build_spec_digest: Option<DatasetBuildSpecDigest>,
    parent_count: usize,
}

impl ResearchUseGeneration {
    /// Binds a nonzero catalog sequence to its complete retained generation identity.
    pub fn try_new(
        sequence: u64,
        manifest: DatasetManifestRef,
        kind: GenerationKind,
        build_spec_digest: Option<DatasetBuildSpecDigest>,
        parent_count: usize,
    ) -> Result<Self, ResearchUseError> {
        let valid_build = matches!(
            (kind, build_spec_digest),
            (GenerationKind::Derived, Some(_))
                | (GenerationKind::Ingest | GenerationKind::Compaction, None)
        );
        let valid_parent_count = match kind {
            GenerationKind::Ingest if manifest.manifest_version() == 1 => parent_count == 0,
            GenerationKind::Ingest | GenerationKind::Compaction => parent_count == 1,
            GenerationKind::Derived => {
                (1..=crate::MAX_DERIVED_GENERATION_PARENTS).contains(&parent_count)
            }
        };
        if sequence == 0 || !valid_build || !valid_parent_count {
            return Err(ResearchUseError::InvalidGeneration);
        }
        Ok(Self {
            sequence,
            manifest,
            kind,
            build_spec_digest,
            parent_count,
        })
    }

    /// Returns the nonzero catalog sequence.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the complete immutable manifest identity.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the generation's closed kind.
    pub const fn kind(&self) -> GenerationKind {
        self.kind
    }

    /// Returns the mandatory derived-build identity, when applicable.
    pub const fn build_spec_digest(&self) -> Option<DatasetBuildSpecDigest> {
        self.build_spec_digest
    }

    /// Returns the exact nonnegative parent count retained by the catalog row.
    pub const fn parent_count(&self) -> usize {
        self.parent_count
    }
}

/// One exact child-to-parent edge in a transitive research graph.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ResearchUseGraphEdge {
    child_sequence: u64,
    parent_sequence: u64,
    relation: GenerationParentRelation,
}

impl ResearchUseGraphEdge {
    /// Constructs a strictly backward edge; this ordering makes cycles unrepresentable.
    pub fn try_new(
        child_sequence: u64,
        parent_sequence: u64,
        relation: GenerationParentRelation,
    ) -> Result<Self, ResearchUseError> {
        if parent_sequence == 0 || child_sequence == 0 || parent_sequence >= child_sequence {
            return Err(ResearchUseError::InvalidGraph);
        }
        Ok(Self {
            child_sequence,
            parent_sequence,
            relation,
        })
    }

    /// Returns the exact child sequence.
    pub const fn child_sequence(self) -> u64 {
        self.child_sequence
    }

    /// Returns the exact parent sequence.
    pub const fn parent_sequence(self) -> u64 {
        self.parent_sequence
    }

    /// Returns the retained semantic relationship.
    pub const fn relation(self) -> GenerationParentRelation {
        self.relation
    }
}

/// One direct ingest generation's immutable source and source-rights anchor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResearchUseSourceInput {
    generation_sequence: u64,
    ingest_run_id: Uuid,
    source_id: SourceId,
    rights_id: [u8; 32],
}

impl ResearchUseSourceInput {
    /// Constructs a complete non-reserved direct source mapping.
    pub fn try_new(
        generation_sequence: u64,
        ingest_run_id: Uuid,
        source_id: SourceId,
        rights_id: [u8; 32],
    ) -> Result<Self, ResearchUseError> {
        if generation_sequence == 0 || ingest_run_id.is_nil() || rights_id == [0; 32] {
            return Err(ResearchUseError::InvalidSourceInput);
        }
        Ok(Self {
            generation_sequence,
            ingest_run_id,
            source_id,
            rights_id,
        })
    }

    /// Returns the exact ingest generation sequence.
    pub const fn generation_sequence(&self) -> u64 {
        self.generation_sequence
    }

    /// Returns the immutable ingest run identity.
    pub const fn ingest_run_id(&self) -> Uuid {
        self.ingest_run_id
    }

    /// Returns the direct source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact retained source-rights identity.
    pub const fn rights_id(&self) -> [u8; 32] {
        self.rights_id
    }
}

/// Validated, canonical, duplicate-free transitive research graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResearchUseGraph {
    roots: Box<[DatasetManifestRef]>,
    nodes: Box<[ResearchUseGeneration]>,
    edges: Box<[ResearchUseGraphEdge]>,
    sources: Box<[ResearchUseSourceInput]>,
    retained_bytes: usize,
    limits: ResearchUseLimits,
    digest: ResearchUseGraphDigest,
}

impl ResearchUseGraph {
    /// Validates, canonicalizes, and hashes a complete bounded transitive graph.
    pub fn try_new(
        mut roots: Vec<DatasetManifestRef>,
        mut nodes: Vec<ResearchUseGeneration>,
        mut edges: Vec<ResearchUseGraphEdge>,
        mut sources: Vec<ResearchUseSourceInput>,
        limits: ResearchUseLimits,
    ) -> Result<Self, ResearchUseError> {
        validate_counts(roots.len(), nodes.len(), edges.len(), sources.len(), limits)?;
        let retained_bytes = canonical_retained_bytes(&roots, &nodes, &edges, &sources)?;
        if retained_bytes > limits.max_retained_bytes() {
            return Err(ResearchUseError::InvalidGraph);
        }
        roots.sort_unstable_by(compare_manifests);
        reject_duplicate_manifests(&roots)?;
        nodes.sort_unstable_by(|left, right| {
            left.sequence
                .cmp(&right.sequence)
                .then_with(|| compare_manifests(&left.manifest, &right.manifest))
        });
        if nodes
            .windows(2)
            .any(|pair| pair[0].sequence == pair[1].sequence)
        {
            return Err(ResearchUseError::DuplicateGraphMember);
        }
        reject_duplicate_node_manifests(&nodes)?;
        edges.sort_unstable();
        if edges.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ResearchUseError::DuplicateGraphMember);
        }
        sources.sort_unstable_by(compare_sources);
        if sources
            .windows(2)
            .any(|pair| pair[0].generation_sequence == pair[1].generation_sequence)
        {
            return Err(ResearchUseError::DuplicateGraphMember);
        }
        validate_graph_members(&roots, &nodes, &edges, &sources)?;

        let mut graph = Self {
            roots: roots.into_boxed_slice(),
            nodes: nodes.into_boxed_slice(),
            edges: edges.into_boxed_slice(),
            sources: sources.into_boxed_slice(),
            retained_bytes,
            limits,
            digest: ResearchUseGraphDigest::from_canonical([0; 32]),
        };
        graph.digest = canonical::graph_digest(&graph)?;
        Ok(graph)
    }

    /// Returns the exact canonical graph identity.
    pub const fn digest(&self) -> ResearchUseGraphDigest {
        self.digest
    }

    /// Returns exact roots in canonical manifest order.
    pub fn roots(&self) -> &[DatasetManifestRef] {
        &self.roots
    }

    /// Returns every generation in canonical sequence order.
    pub fn nodes(&self) -> &[ResearchUseGeneration] {
        &self.nodes
    }

    /// Returns every edge in canonical child-parent-relation order.
    pub fn edges(&self) -> &[ResearchUseGraphEdge] {
        &self.edges
    }

    /// Returns direct source mappings in canonical generation order.
    pub fn sources(&self) -> &[ResearchUseSourceInput] {
        &self.sources
    }

    /// Returns traversal-owned retained bytes recorded in the identity.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Returns the exact resource limits enforced while constructing this graph.
    ///
    /// Limits are decision evidence, not topology: the graph digest intentionally excludes them,
    /// while every decision hashes these exact retained values.
    pub(crate) const fn limits(&self) -> ResearchUseLimits {
        self.limits
    }
}

pub(super) fn compare_manifests(left: &DatasetManifestRef, right: &DatasetManifestRef) -> Ordering {
    left.dataset_id()
        .as_str()
        .cmp(right.dataset_id().as_str())
        .then_with(|| left.manifest_version().cmp(&right.manifest_version()))
        .then_with(|| left.schema().cmp(right.schema()))
        .then_with(|| left.content_hash().cmp(&right.content_hash()))
}

pub(super) fn compare_sources(
    left: &ResearchUseSourceInput,
    right: &ResearchUseSourceInput,
) -> Ordering {
    left.generation_sequence
        .cmp(&right.generation_sequence)
        .then_with(|| left.source_id.cmp(&right.source_id))
        .then_with(|| {
            left.ingest_run_id
                .as_bytes()
                .cmp(right.ingest_run_id.as_bytes())
        })
        .then_with(|| left.rights_id.cmp(&right.rights_id))
}

fn validate_counts(
    roots: usize,
    nodes: usize,
    edges: usize,
    sources: usize,
    limits: ResearchUseLimits,
) -> Result<(), ResearchUseError> {
    if roots == 0
        || roots > limits.max_roots()
        || nodes == 0
        || nodes > limits.max_nodes()
        || edges > limits.max_edges()
        || sources == 0
        || sources > limits.max_sources()
    {
        Err(ResearchUseError::InvalidGraph)
    } else {
        Ok(())
    }
}

fn canonical_retained_bytes(
    roots: &[DatasetManifestRef],
    nodes: &[ResearchUseGeneration],
    edges: &[ResearchUseGraphEdge],
    sources: &[ResearchUseSourceInput],
) -> Result<usize, ResearchUseError> {
    // Graph v1 retains one canonical u64 charge, seven fixed-width limit values, and four
    // length-framed collections in addition to the exact members below. Values are charged by
    // their canonical widths rather than target-specific Rust layouts.
    let mut total = checked_product(12, 8)?;
    for root in roots {
        total = checked_sum(total, manifest_retained_bytes(root)?)?;
    }
    for node in nodes {
        total = checked_sum(total, 8)?;
        total = checked_sum(total, manifest_retained_bytes(&node.manifest)?)?;
        total = checked_sum(total, 1)?;
        total = checked_sum(total, 1)?;
        if node.build_spec_digest.is_some() {
            total = checked_sum(total, 32)?;
        }
        total = checked_sum(total, 8)?;
    }
    total = checked_sum(total, checked_product(edges.len(), 17)?)?;
    for source in sources {
        total = checked_sum(total, 8 + 16)?;
        total = checked_sum(total, framed_string_bytes(source.source_id.as_str())?)?;
        total = checked_sum(total, 32)?;
    }
    Ok(total)
}

fn manifest_retained_bytes(manifest: &DatasetManifestRef) -> Result<usize, ResearchUseError> {
    let mut total = framed_string_bytes(manifest.dataset_id().as_str())?;
    total = checked_sum(total, 8)?;
    total = checked_sum(total, framed_string_bytes(manifest.schema().name())?)?;
    total = checked_sum(total, 2 + 32 + 32)?;
    Ok(total)
}

fn framed_string_bytes(value: &str) -> Result<usize, ResearchUseError> {
    checked_sum(8, value.len())
}

fn checked_product(left: usize, right: usize) -> Result<usize, ResearchUseError> {
    left.checked_mul(right)
        .ok_or(ResearchUseError::CanonicalEncodingOverflow)
}

fn checked_sum(left: usize, right: usize) -> Result<usize, ResearchUseError> {
    left.checked_add(right)
        .ok_or(ResearchUseError::CanonicalEncodingOverflow)
}

fn reject_duplicate_manifests(manifests: &[DatasetManifestRef]) -> Result<(), ResearchUseError> {
    for pair in manifests.windows(2) {
        if same_manifest_coordinate(&pair[0], &pair[1]) {
            return if pair[0] == pair[1] {
                Err(ResearchUseError::DuplicateGraphMember)
            } else {
                Err(ResearchUseError::ConflictingGraphMember)
            };
        }
    }
    Ok(())
}

fn reject_duplicate_node_manifests(
    nodes: &[ResearchUseGeneration],
) -> Result<(), ResearchUseError> {
    let mut order = Vec::new();
    order
        .try_reserve_exact(nodes.len())
        .map_err(|_| ResearchUseError::AllocationFailed)?;
    order.extend(0..nodes.len());
    order.sort_unstable_by(|left, right| {
        compare_manifests(&nodes[*left].manifest, &nodes[*right].manifest)
    });
    for pair in order.windows(2) {
        let left = &nodes[pair[0]].manifest;
        let right = &nodes[pair[1]].manifest;
        if same_manifest_coordinate(left, right) {
            return if left == right {
                Err(ResearchUseError::DuplicateGraphMember)
            } else {
                Err(ResearchUseError::ConflictingGraphMember)
            };
        }
    }
    Ok(())
}

fn validate_graph_members(
    roots: &[DatasetManifestRef],
    nodes: &[ResearchUseGeneration],
    edges: &[ResearchUseGraphEdge],
    sources: &[ResearchUseSourceInput],
) -> Result<(), ResearchUseError> {
    let mut reachable = Vec::new();
    reachable
        .try_reserve_exact(nodes.len())
        .map_err(|_| ResearchUseError::AllocationFailed)?;
    reachable.resize(nodes.len(), false);
    for root in roots {
        let index = nodes
            .iter()
            .position(|node| node.manifest == *root)
            .ok_or(ResearchUseError::InvalidGraph)?;
        reachable[index] = true;
    }
    for edge in edges {
        let child = node_index(nodes, edge.child_sequence)?;
        let parent = node_index(nodes, edge.parent_sequence)?;
        let valid_relation = matches!(
            (nodes[child].kind, edge.relation),
            (
                GenerationKind::Ingest,
                GenerationParentRelation::AppendPredecessor
            ) | (
                GenerationKind::Compaction,
                GenerationParentRelation::CompactionPredecessor
            ) | (
                GenerationKind::Derived,
                GenerationParentRelation::DerivedInput
            )
        );
        if !valid_relation || parent >= child {
            return Err(ResearchUseError::InvalidGraph);
        }
    }
    if nodes
        .iter()
        .any(|node| !valid_parent_semantics(node, nodes, edges))
    {
        return Err(ResearchUseError::InvalidGraph);
    }
    for child in (0..nodes.len()).rev() {
        if reachable[child] {
            for edge in child_edges(edges, nodes[child].sequence) {
                reachable[node_index(nodes, edge.parent_sequence)?] = true;
            }
        }
    }
    if reachable.iter().any(|value| !value) {
        return Err(ResearchUseError::InvalidGraph);
    }
    let mut source_index = 0;
    for node in nodes {
        if sources
            .get(source_index)
            .is_some_and(|source| source.generation_sequence < node.sequence)
        {
            return Err(ResearchUseError::InvalidGraph);
        }
        let has_source = sources
            .get(source_index)
            .is_some_and(|source| source.generation_sequence == node.sequence);
        if (node.kind == GenerationKind::Ingest) != has_source {
            return Err(ResearchUseError::InvalidGraph);
        }
        source_index += usize::from(has_source);
    }
    if source_index != sources.len() {
        return Err(ResearchUseError::InvalidGraph);
    }
    Ok(())
}

fn valid_parent_semantics(
    child: &ResearchUseGeneration,
    nodes: &[ResearchUseGeneration],
    edges: &[ResearchUseGraphEdge],
) -> bool {
    let child_edges = child_edges(edges, child.sequence);
    if child_edges.len() != child.parent_count {
        return false;
    }
    match child.kind {
        GenerationKind::Ingest if child.manifest.manifest_version() == 1 => child_edges.is_empty(),
        GenerationKind::Ingest => predecessor_matches(
            child,
            nodes,
            child_edges,
            GenerationParentRelation::AppendPredecessor,
        ),
        GenerationKind::Compaction => predecessor_matches(
            child,
            nodes,
            child_edges,
            GenerationParentRelation::CompactionPredecessor,
        ),
        GenerationKind::Derived => {
            !child_edges.is_empty()
                && child_edges
                    .iter()
                    .all(|edge| edge.relation == GenerationParentRelation::DerivedInput)
        }
    }
}

fn child_edges(edges: &[ResearchUseGraphEdge], child_sequence: u64) -> &[ResearchUseGraphEdge] {
    let start = edges.partition_point(|edge| edge.child_sequence < child_sequence);
    let end = start + edges[start..].partition_point(|edge| edge.child_sequence == child_sequence);
    &edges[start..end]
}

fn predecessor_matches(
    child: &ResearchUseGeneration,
    nodes: &[ResearchUseGeneration],
    edges: &[ResearchUseGraphEdge],
    relation: GenerationParentRelation,
) -> bool {
    let Some(edge) = edges.first().filter(|_| edges.len() == 1) else {
        return false;
    };
    let Ok(parent_index) = node_index(nodes, edge.parent_sequence) else {
        return false;
    };
    let parent = &nodes[parent_index].manifest;
    edge.relation == relation
        && parent.dataset_id() == child.manifest.dataset_id()
        && parent.manifest_version().checked_add(1) == Some(child.manifest.manifest_version())
        && parent.schema() == child.manifest.schema()
}

fn node_index(nodes: &[ResearchUseGeneration], sequence: u64) -> Result<usize, ResearchUseError> {
    nodes
        .binary_search_by_key(&sequence, |node| node.sequence)
        .map_err(|_| ResearchUseError::InvalidGraph)
}

fn same_manifest_coordinate(left: &DatasetManifestRef, right: &DatasetManifestRef) -> bool {
    left.dataset_id() == right.dataset_id() && left.manifest_version() == right.manifest_version()
}
