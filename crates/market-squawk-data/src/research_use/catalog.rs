//! Catalog-backed ResearchUse authority and publication service.

use std::fmt;

use market_squawk_domain::{EvidenceDigest, Timestamp};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::persistence;
use super::{
    DerivedPublicationInput, DerivedPublicationObject, DerivedRetentionOperation, ResearchUse,
    ResearchUseDecisionDigest, ResearchUseDenialReason, ResearchUseError, ResearchUseGraph,
    ResearchUseLimits, ResearchUsePermit, ResearchUseSet,
};
use crate::{
    CatalogAuthority, CatalogError, DatasetBuildSpecDigest, DatasetManifestRef, DatasetSchemaRef,
    IngestReservation, ManifestPlan, Sha256Digest,
};

/// One bounded, exact request to evaluate transitive source authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResearchUseRequest {
    pub(super) roots: Box<[DatasetManifestRef]>,
    pub(super) requested_use: ResearchUse,
    pub(super) limits: ResearchUseLimits,
}

impl ResearchUseRequest {
    /// Constructs a request without resolving mutable "latest" dataset state.
    pub fn try_new(
        mut roots: Vec<DatasetManifestRef>,
        requested_use: ResearchUse,
        limits: ResearchUseLimits,
    ) -> Result<Self, ResearchUseCatalogError> {
        if roots.is_empty() || roots.len() > limits.max_roots() {
            return Err(ResearchUseCatalogError::LimitExceeded);
        }
        roots.sort_unstable_by(super::graph::compare_manifests);
        for pair in roots.windows(2) {
            if pair[0].dataset_id() == pair[1].dataset_id()
                && pair[0].manifest_version() == pair[1].manifest_version()
            {
                return Err(if pair[0] == pair[1] {
                    ResearchUseError::DuplicateGraphMember.into()
                } else {
                    ResearchUseError::ConflictingGraphMember.into()
                });
            }
        }
        Ok(Self {
            roots: roots.into_boxed_slice(),
            requested_use,
            limits,
        })
    }

    /// Returns the independently evaluated downstream use.
    pub const fn requested_use(&self) -> ResearchUse {
        self.requested_use
    }

    /// Returns the caller-selected bounded resource policy.
    pub const fn limits(&self) -> ResearchUseLimits {
        self.limits
    }

    /// Returns exact immutable roots; no mutable dataset lookup is implied.
    pub fn roots(&self) -> &[DatasetManifestRef] {
        &self.roots
    }
}

/// Complete downstream-use evidence to admit beneath one source-rights decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResearchUseGrantInput {
    pub(super) rights_id: [u8; 32],
    pub(super) permitted_uses: ResearchUseSet,
    pub(super) evidence: EvidenceDigest,
    pub(super) authorization_expires_at: Option<Timestamp>,
}

impl ResearchUseGrantInput {
    /// Constructs a nonempty, evidence-bound downstream-use grant.
    pub fn try_new(
        rights_id: [u8; 32],
        permitted_uses: ResearchUseSet,
        evidence: EvidenceDigest,
        authorization_expires_at: Option<Timestamp>,
    ) -> Result<Self, ResearchUseCatalogError> {
        if rights_id == [0; 32] || evidence.bytes() == [0; 32] || permitted_uses.is_empty() {
            return Err(ResearchUseCatalogError::InvalidGrant);
        }
        Ok(Self {
            rights_id,
            permitted_uses,
            evidence,
            authorization_expires_at,
        })
    }
}

/// Session-bound handle to one immutable durable research-use grant.
#[derive(Clone, Eq, PartialEq)]
pub struct RegisteredResearchUseGrant {
    pub(super) catalog_id: Uuid,
    pub(super) grant_id: [u8; 32],
}

impl RegisteredResearchUseGrant {
    /// Returns the durable grant identity for local audit and revocation requests.
    pub const fn grant_id(&self) -> [u8; 32] {
        self.grant_id
    }
}

impl fmt::Debug for RegisteredResearchUseGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RegisteredResearchUseGrant([SEALED GRANT])")
    }
}

/// Closed reason for withdrawing previously admitted downstream use.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResearchUseRevocationReason {
    /// The underlying authorization was withdrawn.
    AuthorizationWithdrawn,
    /// A newer authorization superseded this grant.
    AuthorizationSuperseded,
    /// Retained evidence was determined to be invalid.
    EvidenceInvalidated,
    /// The source was disabled for downstream research.
    SourceDisabled,
}

impl ResearchUseRevocationReason {
    pub(super) const fn database_name(self) -> &'static str {
        match self {
            Self::AuthorizationWithdrawn => "authorization_withdrawn",
            Self::AuthorizationSuperseded => "authorization_superseded",
            Self::EvidenceInvalidated => "evidence_invalidated",
            Self::SourceDisabled => "source_disabled",
        }
    }

    pub(super) const fn tag(self) -> u8 {
        match self {
            Self::AuthorizationWithdrawn => 1,
            Self::AuthorizationSuperseded => 2,
            Self::EvidenceInvalidated => 3,
            Self::SourceDisabled => 4,
        }
    }
}

/// Exact immutable revocation request scoped to a registered grant and use subset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResearchUseRevocationInput {
    pub(super) catalog_id: Uuid,
    pub(super) grant_id: [u8; 32],
    pub(super) revoked_uses: ResearchUseSet,
    pub(super) reason: ResearchUseRevocationReason,
    pub(super) evidence: EvidenceDigest,
    pub(super) effective_at: Timestamp,
}

impl ResearchUseRevocationInput {
    /// Binds an immutable revocation to this process's admitted grant capability.
    pub fn try_new(
        grant: &RegisteredResearchUseGrant,
        revoked_uses: ResearchUseSet,
        reason: ResearchUseRevocationReason,
        evidence: EvidenceDigest,
        effective_at: Timestamp,
    ) -> Result<Self, ResearchUseCatalogError> {
        if evidence.bytes() == [0; 32] || revoked_uses.is_empty() {
            return Err(ResearchUseCatalogError::InvalidRevocation);
        }
        Ok(Self {
            catalog_id: grant.catalog_id,
            grant_id: grant.grant_id,
            revoked_uses,
            reason,
            evidence,
            effective_at,
        })
    }
}

/// Durable identity and total-order position of one immutable revocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResearchUseRevocationReceipt {
    revocation_id: [u8; 32],
    sequence: u64,
    recorded_at: Timestamp,
}

impl ResearchUseRevocationReceipt {
    pub(super) const fn new(
        revocation_id: [u8; 32],
        sequence: u64,
        recorded_at: Timestamp,
    ) -> Self {
        Self {
            revocation_id,
            sequence,
            recorded_at,
        }
    }

    /// Returns the content-addressed revocation identity.
    pub const fn revocation_id(self) -> [u8; 32] {
        self.revocation_id
    }

    /// Returns the catalog-wide monotonic revocation frontier position.
    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    /// Returns the trusted durable record time.
    pub const fn recorded_at(self) -> Timestamp {
        self.recorded_at
    }
}

/// Opaque successful decision paired with its exact graph and single-use permit.
#[derive(Debug)]
pub struct AuthorizedResearchUse {
    graph: ResearchUseGraph,
    permit: ResearchUsePermit,
}

impl AuthorizedResearchUse {
    pub(super) const fn new(graph: ResearchUseGraph, permit: ResearchUsePermit) -> Self {
        Self { graph, permit }
    }

    /// Returns the exact transitive graph authorized by this capability.
    pub const fn graph(&self) -> &ResearchUseGraph {
        &self.graph
    }

    /// Returns the durable canonical identity of the decision behind this capability.
    pub const fn decision_digest(&self) -> ResearchUseDecisionDigest {
        self.permit.decision_digest()
    }

    /// Returns the independently authorized downstream use without exposing the permit.
    pub const fn research_use(&self) -> ResearchUse {
        self.permit.research_use()
    }

    /// Returns the exclusive expiry of the process-local permit without exposing it.
    pub const fn expires_at(&self) -> Timestamp {
        self.permit.expires_at()
    }

    /// Consumes the capability into one canonical, independently rights-bound publication.
    pub fn prepare_derived_publication(
        self,
        build_spec_digest: DatasetBuildSpecDigest,
        schema: DatasetSchemaRef,
        plan: ManifestPlan,
        objects: Vec<DerivedPublicationObject>,
        anchor_artifact_id: Uuid,
    ) -> Result<DerivedPublicationInput, ResearchUseCatalogError> {
        DerivedPublicationInput::try_new(
            self.permit,
            &self.graph,
            build_spec_digest,
            schema,
            plan,
            objects,
            anchor_artifact_id,
        )
        .map_err(Into::into)
    }
}

/// Exact immutable metadata used to bind one reserved derived-output object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DerivedOutputObjectInput {
    pub(super) artifact_id: Uuid,
    pub(super) content_hash: Sha256Digest,
    pub(super) row_count: u64,
    pub(super) size_bytes: u64,
    pub(super) lineage_digest: Sha256Digest,
}

impl DerivedOutputObjectInput {
    /// Constructs nonempty exact physical and semantic object metadata.
    pub fn try_new(
        artifact_id: Uuid,
        content_hash: Sha256Digest,
        row_count: u64,
        size_bytes: u64,
        lineage_digest: Sha256Digest,
    ) -> Result<Self, ResearchUseCatalogError> {
        if artifact_id.is_nil() || row_count == 0 || size_bytes == 0 {
            return Err(ResearchUseCatalogError::InvalidPublication);
        }
        Ok(Self {
            artifact_id,
            content_hash,
            row_count,
            size_bytes,
            lineage_digest,
        })
    }
}

/// Durable receipt for one atomically authorized derived generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedDerivedGeneration {
    generation_sequence: u64,
    manifest: DatasetManifestRef,
    output_group_id: [u8; 32],
}

impl PublishedDerivedGeneration {
    pub(super) const fn new(
        generation_sequence: u64,
        manifest: DatasetManifestRef,
        output_group_id: [u8; 32],
    ) -> Self {
        Self {
            generation_sequence,
            manifest,
            output_group_id,
        }
    }

    /// Returns the catalog-wide generation sequence.
    pub const fn generation_sequence(&self) -> u64 {
        self.generation_sequence
    }

    /// Returns the immutable derived dataset pin.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the canonical derived-publication identity.
    pub const fn output_group_id(&self) -> [u8; 32] {
        self.output_group_id
    }
}

/// Durable ResearchUse authority, traversal, or publication failure.
#[derive(Debug, Error)]
pub enum ResearchUseCatalogError {
    /// A grant did not bind valid non-reserved evidence or authority.
    #[error("research-use grant is invalid")]
    InvalidGrant,
    /// A revocation did not bind a valid admitted grant and use subset.
    #[error("research-use revocation is invalid")]
    InvalidRevocation,
    /// An exact root generation does not exist in the catalog.
    #[error("research-use root generation is unknown")]
    UnknownGeneration,
    /// Traversal or retained evidence exceeded an explicit bound.
    #[error("research-use resource limit was exceeded")]
    LimitExceeded,
    /// The caller cancelled bounded traversal before decision commit.
    #[error("research-use traversal was cancelled")]
    Cancelled,
    /// The monotonic traversal deadline elapsed before decision commit.
    #[error("research-use traversal deadline elapsed")]
    DeadlineExceeded,
    /// No complete, currently valid transitive authority admitted the requested use.
    #[error("research-use request was denied: {reason:?}")]
    Denied {
        /// Exact durable canonical decision identity.
        decision_digest: ResearchUseDecisionDigest,
        /// Closed fail-closed reason selected by the authority policy.
        reason: ResearchUseDenialReason,
    },
    /// A permit belongs to another catalog process session.
    #[error("research-use permit is not valid for this catalog session")]
    InvalidPermitSession,
    /// A permit or required source authority expired before publication commit.
    #[error("research-use authority expired before publication")]
    Expired,
    /// A selected source grant was revoked before publication commit.
    #[error("research-use authority was revoked before publication")]
    Revoked,
    /// Derived output metadata or its durable reservation does not match exactly.
    #[error("derived publication binding is invalid")]
    InvalidPublication,
    /// Durable authority or lineage rows did not reconstruct their canonical contracts.
    #[error("research-use catalog evidence is corrupt")]
    CorruptCatalog,
    /// Pure canonical contract construction failed.
    #[error("research-use contract is invalid: {0}")]
    Contract(#[from] ResearchUseError),
    /// The underlying hardened catalog rejected the operation.
    #[error("research-use catalog operation failed: {0}")]
    Catalog(#[from] CatalogError),
    /// SQLite rejected an atomic operation or schema invariant.
    #[error("research-use SQLite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

impl CatalogAuthority {
    /// Admits one immutable downstream-use grant under existing source-rights evidence.
    pub fn admit_research_use_grant(
        &self,
        input: ResearchUseGrantInput,
    ) -> Result<RegisteredResearchUseGrant, ResearchUseCatalogError> {
        self.with_research_use_transaction(move |transaction, session_id, now| {
            persistence::admit_grant(transaction, session_id, now, input)
        })
    }

    /// Appends one immutable, globally ordered downstream-use revocation.
    pub fn revoke_research_use(
        &self,
        input: ResearchUseRevocationInput,
    ) -> Result<ResearchUseRevocationReceipt, ResearchUseCatalogError> {
        self.with_research_use_transaction(move |transaction, session_id, now| {
            persistence::revoke_grant(transaction, session_id, now, input)
        })
    }

    /// Traverses exact lineage, commits its canonical decision, and issues a session permit.
    pub fn authorize_research_use(
        &self,
        request: ResearchUseRequest,
        cancellation: &CancellationToken,
    ) -> Result<AuthorizedResearchUse, ResearchUseCatalogError> {
        let outcome = self.with_research_use_transaction(move |transaction, session_id, now| {
            persistence::authorize(transaction, session_id, now, request, cancellation)
        })?;
        match outcome {
            AuthorizationTransactionOutcome::Allowed(authorization) => Ok(*authorization),
            AuthorizationTransactionOutcome::Denied {
                decision_digest,
                reason,
            } => Err(ResearchUseCatalogError::Denied {
                decision_digest,
                reason,
            }),
        }
    }

    /// Binds a reserved, already published artifact to exact derived-object metadata.
    pub fn bind_derived_output_object(
        &self,
        reservation: &IngestReservation,
        input: DerivedOutputObjectInput,
    ) -> Result<DerivedPublicationObject, ResearchUseCatalogError> {
        self.validate_ingest_reservation(reservation)?;
        self.with_research_use_transaction(|transaction, session_id, _now| {
            persistence::bind_derived_output(transaction, session_id, reservation, input)
        })
    }

    /// Atomically publishes outputs, exact parents, derived generation, and authority evidence.
    pub fn publish_derived_generation(
        &self,
        input: DerivedPublicationInput,
    ) -> Result<PublishedDerivedGeneration, ResearchUseCatalogError> {
        self.with_research_use_transaction(move |transaction, session_id, now| {
            persistence::publish_derived(transaction, session_id, now, input)
        })
    }
}

pub(super) enum AuthorizationTransactionOutcome {
    Allowed(Box<AuthorizedResearchUse>),
    Denied {
        decision_digest: ResearchUseDecisionDigest,
        reason: ResearchUseDenialReason,
    },
}

pub(super) const fn retention_operation_name(operation: DerivedRetentionOperation) -> &'static str {
    match operation {
        DerivedRetentionOperation::Persist => "persist",
        DerivedRetentionOperation::Cache => "cache",
    }
}
