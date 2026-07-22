//! Canonical derived-publication inputs bound to independent output authority.

use std::cmp::Ordering;
use std::fmt;

use uuid::Uuid;

use super::canonical;
use super::graph::ResearchUseGraph;
use super::model::{
    ResearchUse, ResearchUseDecisionDigest, ResearchUseError, ResearchUseGraphDigest,
};
use super::permit::ResearchUsePermit;
use crate::{
    DatasetBuildSpecDigest, DatasetManifestRef, DatasetSchemaRef, ManifestPlan, Sha256Digest,
};

/// Maximum independently reserved physical objects in one derived publication.
pub const MAX_DERIVED_PUBLICATION_OBJECTS: usize = 1_024;

/// Closed retention operation independently authorized for a derived output.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DerivedRetentionOperation {
    /// Persist the normalized analytical object.
    Persist,
    /// Retain the object as a reusable local analytical cache.
    Cache,
}

impl DerivedRetentionOperation {
    pub(super) const fn tag(self) -> u8 {
        match self {
            Self::Persist => 1,
            Self::Cache => 2,
        }
    }
}

/// Exact SHA-256 identity of one canonical derived publication.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DerivedPublicationDigest([u8; 32]);

impl DerivedPublicationDigest {
    /// Reconstructs a non-reserved retained publication identity.
    pub fn try_from_bytes(bytes: [u8; 32]) -> Result<Self, ResearchUseError> {
        if bytes == [0; 32] {
            Err(ResearchUseError::MalformedDigest)
        } else {
            Ok(Self(bytes))
        }
    }

    pub(super) const fn from_canonical(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns exact SHA-256 bytes.
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for DerivedPublicationDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DerivedPublicationDigest([SHA-256])")
    }
}

/// One physical output and its independently admitted reservation and rights anchor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedPublicationObject {
    pub(super) run_id: Uuid,
    pub(super) reservation_digest: [u8; 32],
    pub(super) operation: DerivedRetentionOperation,
    pub(super) rights_id: [u8; 32],
    pub(super) artifact_id: Uuid,
    pub(super) content_hash: Sha256Digest,
    pub(super) row_count: u64,
    pub(super) size_bytes: u64,
    pub(super) lineage_digest: Sha256Digest,
}

impl DerivedPublicationObject {
    /// Constructs complete non-reserved output authority and immutable object metadata.
    #[allow(
        clippy::too_many_arguments,
        reason = "all output authority bindings are mandatory"
    )]
    pub fn try_new(
        run_id: Uuid,
        reservation_digest: [u8; 32],
        operation: DerivedRetentionOperation,
        rights_id: [u8; 32],
        artifact_id: Uuid,
        content_hash: Sha256Digest,
        row_count: u64,
        size_bytes: u64,
        lineage_digest: Sha256Digest,
    ) -> Result<Self, ResearchUseError> {
        if run_id.is_nil()
            || reservation_digest == [0; 32]
            || rights_id == [0; 32]
            || artifact_id.is_nil()
            || row_count == 0
            || size_bytes == 0
        {
            return Err(ResearchUseError::InvalidPublication);
        }
        Ok(Self {
            run_id,
            reservation_digest,
            operation,
            rights_id,
            artifact_id,
            content_hash,
            row_count,
            size_bytes,
            lineage_digest,
        })
    }

    /// Returns the reservation's ingest-run identity.
    pub const fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Returns the exact output artifact identity.
    pub const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    pub(crate) const fn reservation_digest(&self) -> [u8; 32] {
        self.reservation_digest
    }

    pub(crate) const fn operation(&self) -> DerivedRetentionOperation {
        self.operation
    }

    pub(crate) const fn rights_id(&self) -> [u8; 32] {
        self.rights_id
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

    pub(crate) const fn lineage_digest(&self) -> Sha256Digest {
        self.lineage_digest
    }
}

/// Validated canonical inputs to one sealed derived-publication transaction.
#[derive(Debug)]
pub struct DerivedPublicationInput {
    permit: ResearchUsePermit,
    pub(super) parents: Box<[DatasetManifestRef]>,
    pub(super) build_spec_digest: DatasetBuildSpecDigest,
    pub(super) schema: DatasetSchemaRef,
    pub(super) plan: ManifestPlan,
    pub(super) objects: Box<[DerivedPublicationObject]>,
    pub(super) anchor_artifact_id: Uuid,
    digest: DerivedPublicationDigest,
}

impl DerivedPublicationInput {
    /// Validates, canonicalizes, and hashes exact input, plan, and output authority bindings.
    #[allow(
        clippy::too_many_arguments,
        reason = "publication identity has independent bindings"
    )]
    pub(crate) fn try_new(
        permit: ResearchUsePermit,
        graph: &ResearchUseGraph,
        build_spec_digest: DatasetBuildSpecDigest,
        schema: DatasetSchemaRef,
        plan: ManifestPlan,
        mut objects: Vec<DerivedPublicationObject>,
        anchor_artifact_id: Uuid,
    ) -> Result<Self, ResearchUseError> {
        if permit.research_use() == ResearchUse::Display
            || permit.graph_digest() != graph.digest()
            || objects.is_empty()
            || objects.len() > MAX_DERIVED_PUBLICATION_OBJECTS
            || anchor_artifact_id.is_nil()
        {
            return Err(ResearchUseError::InvalidPublication);
        }
        let mut parents = Vec::new();
        parents
            .try_reserve_exact(graph.roots().len())
            .map_err(|_| ResearchUseError::AllocationFailed)?;
        parents.extend_from_slice(graph.roots());
        objects.sort_unstable_by(compare_objects);
        validate_objects(&objects, &plan, anchor_artifact_id)?;
        let mut publication = Self {
            permit,
            parents: parents.into_boxed_slice(),
            build_spec_digest,
            schema,
            plan,
            objects: objects.into_boxed_slice(),
            anchor_artifact_id,
            digest: DerivedPublicationDigest::from_canonical([0; 32]),
        };
        publication.digest = canonical::publication_digest(&publication)?;
        Ok(publication)
    }

    /// Returns the exact canonical publication identity.
    pub const fn digest(&self) -> DerivedPublicationDigest {
        self.digest
    }

    /// Returns the exact allowed decision identity.
    pub(crate) const fn decision_digest(&self) -> ResearchUseDecisionDigest {
        self.permit.decision_digest()
    }

    /// Returns the exact authorized graph identity.
    pub(crate) const fn graph_digest(&self) -> ResearchUseGraphDigest {
        self.permit.graph_digest()
    }

    /// Returns output objects in canonical content and authority order.
    pub(crate) fn objects(&self) -> &[DerivedPublicationObject] {
        &self.objects
    }

    pub(crate) const fn requested_use(&self) -> ResearchUse {
        self.permit.research_use()
    }

    pub(crate) fn parents(&self) -> &[DatasetManifestRef] {
        &self.parents
    }

    pub(crate) const fn build_spec_digest(&self) -> DatasetBuildSpecDigest {
        self.build_spec_digest
    }

    pub(crate) const fn schema(&self) -> &DatasetSchemaRef {
        &self.schema
    }

    pub(crate) const fn plan(&self) -> &ManifestPlan {
        &self.plan
    }

    pub(crate) const fn anchor_artifact_id(&self) -> Uuid {
        self.anchor_artifact_id
    }

    pub(crate) const fn permit(&self) -> &ResearchUsePermit {
        &self.permit
    }
}

fn compare_objects(left: &DerivedPublicationObject, right: &DerivedPublicationObject) -> Ordering {
    left.content_hash
        .cmp(&right.content_hash)
        .then_with(|| {
            left.artifact_id
                .as_bytes()
                .cmp(right.artifact_id.as_bytes())
        })
        .then_with(|| left.run_id.as_bytes().cmp(right.run_id.as_bytes()))
        .then_with(|| left.reservation_digest.cmp(&right.reservation_digest))
}

fn validate_objects(
    objects: &[DerivedPublicationObject],
    plan: &ManifestPlan,
    anchor_artifact_id: Uuid,
) -> Result<(), ResearchUseError> {
    if !objects
        .iter()
        .any(|object| object.artifact_id == anchor_artifact_id)
        || objects
            .iter()
            .any(|object| object.operation != objects[0].operation)
    {
        return Err(ResearchUseError::InvalidPublication);
    }
    for left in 0..objects.len() {
        for right in (left + 1)..objects.len() {
            if objects[left].run_id == objects[right].run_id
                || objects[left].reservation_digest == objects[right].reservation_digest
                || objects[left].artifact_id == objects[right].artifact_id
                || objects[left].content_hash == objects[right].content_hash
            {
                return Err(ResearchUseError::DuplicatePublicationMember);
            }
        }
    }
    if plan.objects().len() != objects.len()
        || plan.objects().iter().zip(objects).any(|(planned, output)| {
            planned.content_hash() != output.content_hash
                || planned.row_count() != output.row_count
                || planned.size_bytes() != output.size_bytes
                || planned.lineage_digest() != output.lineage_digest
        })
    {
        return Err(ResearchUseError::InvalidPublication);
    }
    Ok(())
}
