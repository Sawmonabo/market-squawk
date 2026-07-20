//! Explicit, append-only analytical artifact-root authority transitions.

use std::fmt;
use std::num::NonZeroU64;

use market_squawk_domain::Timestamp;
use thiserror::Error;
use uuid::Uuid;

use crate::{BackupReceipt, CatalogConfig, CatalogError, ParquetStoreError};
use crate::{CatalogAuthority, ObjectStoreConfig, ParquetObjectStore};
use market_squawk_platform::ArtifactRoot;

use self::restore::{
    RestoreValidationError, VerifiedRestoreHandoff, target_authority_evidence_from_receipt,
};

pub(crate) mod evidence;
mod legacy;
pub(crate) mod restore;

use self::evidence::CatalogContentEvidenceDigest;

macro_rules! opaque_sha256_identity {
    ($visibility:vis $name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        $visibility struct $name([u8; 32]);

        impl $name {
            /// Reconstructs a non-reserved identity from exact SHA-256 bytes.
            $visibility fn try_from_bytes(bytes: [u8; 32]) -> Option<Self> {
                (bytes != [0; 32]).then_some(Self(bytes))
            }

            pub(crate) fn try_new(bytes: [u8; 32]) -> Option<Self> {
                Self::try_from_bytes(bytes)
            }

            /// Returns the exact SHA-256 bytes.
            $visibility const fn bytes(self) -> [u8; 32] {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "([SHA-256])"))
            }
        }
    };
}

opaque_sha256_identity!(
    pub
    CatalogEndpointIdentity,
    "Opaque identity of one retained exact SQLite catalog endpoint."
);
opaque_sha256_identity!(
    pub(crate)
    RootEndpointIdentity,
    "Opaque identity of one retained exact analytical-root directory endpoint."
);
opaque_sha256_identity!(
    pub(crate)
    RootInstanceId,
    "Opaque random identity persisted for one analytical-root authority transition."
);
opaque_sha256_identity!(
    pub
    AuthorityEvidenceDigest,
    "Opaque identity of the canonical evidence admitted for an authority transition."
);
opaque_sha256_identity!(
    pub
    AuthorityEventDigest,
    "Opaque identity of one append-only catalog authority event."
);
opaque_sha256_identity!(
    pub(crate)
    ControlRecordDigest,
    "Opaque identity of one immutable analytical-root control record."
);
opaque_sha256_identity!(
    pub
    StableArtifactRootIdentity,
    "Opaque stable identity of a retained analytical artifact root."
);
opaque_sha256_identity!(
    pub
    ArtifactInventoryDigest,
    "Opaque identity of a bounded canonical analytical-artifact inventory."
);

/// Unique identity of one explicit authority transition.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct TransitionId(Uuid);

impl TransitionId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub(crate) fn try_from_bytes(bytes: [u8; 16]) -> Option<Self> {
        let value = Uuid::from_bytes(bytes);
        (!value.is_nil()).then_some(Self(value))
    }

    pub(crate) const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for TransitionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TransitionId([UUID])")
    }
}

/// Monotonic generation of one catalog's append-only authority lineage.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AuthorityGeneration(NonZeroU64);

impl AuthorityGeneration {
    /// Constructs a nonzero authority generation.
    pub const fn try_new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the nonzero generation number.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Checked deterministic immutable binding-record generation for one authority generation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct RootBindingGeneration(NonZeroU64);

impl RootBindingGeneration {
    pub(crate) const fn try_new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub(crate) const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Authorized kind of explicit authority transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuthorityTransitionKind {
    /// Bind a fresh or provably empty analytical catalog and root.
    Initialize,
    /// Migrate an exact two-sided version-3/version-4 authority relationship.
    LegacyMigration,
    /// Restore a receipt-verified catalog and complete artifact bundle to fresh endpoints.
    BackupRestore,
}

/// Receipt identities retained only for a verified backup-restore transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RestoreReceiptFields {
    source_catalog_identity: CatalogEndpointIdentity,
    source_root_identity: StableArtifactRootIdentity,
    source_authority_generation: AuthorityGeneration,
    source_bound_event: AuthorityEventDigest,
    source_authority_evidence: AuthorityEvidenceDigest,
    source_catalog_content_evidence: CatalogContentEvidenceDigest,
    artifact_inventory: ArtifactInventoryDigest,
    catalog_backup: BackupReceipt,
    snapshot_at: Timestamp,
}

impl RestoreReceiptFields {
    #[allow(
        clippy::too_many_arguments,
        reason = "the append-only event schema stores each independently verified restore identity"
    )]
    pub(crate) fn new(
        source_catalog_identity: CatalogEndpointIdentity,
        source_root_identity: StableArtifactRootIdentity,
        source_authority_generation: AuthorityGeneration,
        source_bound_event: AuthorityEventDigest,
        source_authority_evidence: AuthorityEvidenceDigest,
        source_catalog_content_evidence: CatalogContentEvidenceDigest,
        artifact_inventory: ArtifactInventoryDigest,
        catalog_backup: BackupReceipt,
        snapshot_at: Timestamp,
    ) -> Self {
        Self {
            source_catalog_identity,
            source_root_identity,
            source_authority_generation,
            source_bound_event,
            source_authority_evidence,
            source_catalog_content_evidence,
            artifact_inventory,
            catalog_backup,
            snapshot_at,
        }
    }

    pub(crate) const fn source_catalog_identity(&self) -> CatalogEndpointIdentity {
        self.source_catalog_identity
    }

    pub(crate) const fn source_root_identity(&self) -> StableArtifactRootIdentity {
        self.source_root_identity
    }

    pub(crate) const fn source_bound_event(&self) -> AuthorityEventDigest {
        self.source_bound_event
    }

    pub(crate) const fn source_authority_generation(&self) -> AuthorityGeneration {
        self.source_authority_generation
    }

    pub(crate) const fn source_authority_evidence(&self) -> AuthorityEvidenceDigest {
        self.source_authority_evidence
    }

    pub(crate) const fn source_catalog_content_evidence(&self) -> CatalogContentEvidenceDigest {
        self.source_catalog_content_evidence
    }

    pub(crate) const fn artifact_inventory(&self) -> ArtifactInventoryDigest {
        self.artifact_inventory
    }

    pub(crate) const fn catalog_backup(&self) -> &BackupReceipt {
        &self.catalog_backup
    }

    pub(crate) const fn snapshot_at(&self) -> Timestamp {
        self.snapshot_at
    }
}

/// Exact durable intent recorded before any root control record is activated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedAuthorityTransition {
    transition_id: TransitionId,
    kind: AuthorityTransitionKind,
    authority_generation: AuthorityGeneration,
    target_catalog_identity: CatalogEndpointIdentity,
    target_root_endpoint_identity: RootEndpointIdentity,
    root_instance_id: RootInstanceId,
    evidence_digest: AuthorityEvidenceDigest,
    restore_receipt: Option<RestoreReceiptFields>,
    root_binding_generation: RootBindingGeneration,
}

impl PreparedAuthorityTransition {
    #[allow(
        clippy::too_many_arguments,
        reason = "the prepared event must bind every independently verified authority identity"
    )]
    pub(crate) fn try_new(
        transition_id: TransitionId,
        kind: AuthorityTransitionKind,
        authority_generation: AuthorityGeneration,
        target_catalog_identity: CatalogEndpointIdentity,
        target_root_endpoint_identity: RootEndpointIdentity,
        root_instance_id: RootInstanceId,
        evidence_digest: AuthorityEvidenceDigest,
        restore_receipt: Option<RestoreReceiptFields>,
    ) -> Option<Self> {
        let restore_shape_is_valid =
            matches!(kind, AuthorityTransitionKind::BackupRestore) == restore_receipt.is_some();
        if !restore_shape_is_valid {
            return None;
        }
        let root_binding_generation = RootBindingGeneration::try_new(authority_generation.get())?;
        Some(Self {
            transition_id,
            kind,
            authority_generation,
            target_catalog_identity,
            target_root_endpoint_identity,
            root_instance_id,
            evidence_digest,
            restore_receipt,
            root_binding_generation,
        })
    }

    pub(crate) const fn transition_id(&self) -> TransitionId {
        self.transition_id
    }

    pub(crate) const fn kind(&self) -> AuthorityTransitionKind {
        self.kind
    }

    pub(crate) const fn authority_generation(&self) -> AuthorityGeneration {
        self.authority_generation
    }

    pub(crate) const fn target_catalog_identity(&self) -> CatalogEndpointIdentity {
        self.target_catalog_identity
    }

    pub(crate) const fn target_root_endpoint_identity(&self) -> RootEndpointIdentity {
        self.target_root_endpoint_identity
    }

    pub(crate) const fn root_instance_id(&self) -> RootInstanceId {
        self.root_instance_id
    }

    pub(crate) const fn evidence_digest(&self) -> AuthorityEvidenceDigest {
        self.evidence_digest
    }

    pub(crate) const fn restore_receipt(&self) -> Option<&RestoreReceiptFields> {
        self.restore_receipt.as_ref()
    }

    pub(crate) const fn root_binding_generation(&self) -> RootBindingGeneration {
        self.root_binding_generation
    }
}

/// Exact immutable root-control evidence added by a completed authority transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundAuthorityTransition {
    prepared: PreparedAuthorityTransition,
    root_marker_record_digest: ControlRecordDigest,
    stable_root_identity: StableArtifactRootIdentity,
    root_binding_record_digest: ControlRecordDigest,
}

impl BoundAuthorityTransition {
    pub(crate) fn new(
        prepared: PreparedAuthorityTransition,
        root_marker_record_digest: ControlRecordDigest,
        stable_root_identity: StableArtifactRootIdentity,
        root_binding_record_digest: ControlRecordDigest,
    ) -> Self {
        Self {
            prepared,
            root_marker_record_digest,
            stable_root_identity,
            root_binding_record_digest,
        }
    }

    pub(crate) const fn prepared(&self) -> &PreparedAuthorityTransition {
        &self.prepared
    }

    pub(crate) const fn root_marker_record_digest(&self) -> ControlRecordDigest {
        self.root_marker_record_digest
    }

    pub(crate) const fn stable_root_identity(&self) -> StableArtifactRootIdentity {
        self.stable_root_identity
    }

    pub(crate) const fn root_binding_record_digest(&self) -> ControlRecordDigest {
        self.root_binding_record_digest
    }
}

/// Last validated event in the append-only catalog authority chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AuthorityHead {
    sequence: NonZeroU64,
    event_digest: AuthorityEventDigest,
}

/// Exact first event requiring a two-sided version-1 root migration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LegacyAuthorityRequirement {
    head: AuthorityHead,
    catalog_identity: CatalogEndpointIdentity,
    evidence_digest: AuthorityEvidenceDigest,
}

impl LegacyAuthorityRequirement {
    pub(crate) const fn new(
        head: AuthorityHead,
        catalog_identity: CatalogEndpointIdentity,
        evidence_digest: AuthorityEvidenceDigest,
    ) -> Self {
        Self {
            head,
            catalog_identity,
            evidence_digest,
        }
    }

    pub(crate) const fn head(self) -> AuthorityHead {
        self.head
    }

    pub(crate) const fn catalog_identity(self) -> CatalogEndpointIdentity {
        self.catalog_identity
    }

    pub(crate) const fn evidence_digest(self) -> AuthorityEvidenceDigest {
        self.evidence_digest
    }
}

impl AuthorityHead {
    pub(crate) fn new(sequence: NonZeroU64, event_digest: AuthorityEventDigest) -> Self {
        Self {
            sequence,
            event_digest,
        }
    }

    pub(crate) const fn sequence(self) -> NonZeroU64 {
        self.sequence
    }

    pub(crate) const fn event_digest(self) -> AuthorityEventDigest {
        self.event_digest
    }
}

/// Validated catalog-side state of analytical-root authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AuthorityState {
    /// No authority event exists; an explicit initialization operation is required.
    InitializationRequired,
    /// A version-3/version-4 catalog requires explicit two-sided legacy migration.
    LegacyRequired {
        /// Exact head of the append-only event chain.
        head: AuthorityHead,
        /// Identity of the two-sided legacy evidence expected by migration.
        evidence_digest: AuthorityEvidenceDigest,
    },
    /// Evidence is committed but root control publication has not been activated.
    Prepared {
        /// Exact head of the append-only event chain.
        head: AuthorityHead,
        /// Complete durable transition intent.
        transition: PreparedAuthorityTransition,
    },
    /// The exact catalog/root pair is active and may be opened ordinarily.
    Bound {
        /// Exact head of the append-only event chain.
        head: AuthorityHead,
        /// Complete activated authority relationship.
        transition: BoundAuthorityTransition,
    },
}

/// One fully validated read of the append-only authority event lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AuthoritySnapshot {
    state: AuthorityState,
    legacy_requirement: Option<LegacyAuthorityRequirement>,
}

impl AuthoritySnapshot {
    pub(crate) const fn new(
        state: AuthorityState,
        legacy_requirement: Option<LegacyAuthorityRequirement>,
    ) -> Self {
        Self {
            state,
            legacy_requirement,
        }
    }

    pub(crate) const fn state(&self) -> &AuthorityState {
        &self.state
    }

    pub(crate) const fn head(&self) -> Option<AuthorityHead> {
        self.state.head()
    }

    pub(crate) const fn bound(&self) -> Option<&BoundAuthorityTransition> {
        self.state.bound()
    }

    pub(crate) const fn legacy_requirement(&self) -> Option<LegacyAuthorityRequirement> {
        self.legacy_requirement
    }
}

/// Non-forgeable capability accepted by catalog authority append operations.
pub(crate) struct AuthorityMutationToken {
    _private: (),
}

impl AuthorityMutationToken {
    fn new() -> Self {
        Self { _private: () }
    }
}

/// Sole sealed coordinator for catalog/root authority mutation and ordered capability retention.
pub(crate) struct AuthorityTransitionService;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FirstBindCheckpoint {
    CatalogPrepared,
    MarkerPending,
    MarkerFinal,
    BindingPending,
    BindingFinal,
    CatalogBound,
}

impl AuthorityTransitionService {
    pub(crate) fn initialize(
        authority: CatalogAuthority,
        root: ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<(CatalogAuthority, ParquetObjectStore), AuthorityTransitionError> {
        Self::initialize_with_checkpoint(authority, root, object_config, &mut |_| Ok(()))
    }

    fn initialize_with_checkpoint(
        mut authority: CatalogAuthority,
        root: ArtifactRoot,
        object_config: ObjectStoreConfig,
        checkpoint: &mut impl FnMut(
            crate::parquet_store::RootBindingCheckpointInternal,
        ) -> Result<(), ParquetStoreError>,
    ) -> Result<(CatalogAuthority, ParquetObjectStore), AuthorityTransitionError> {
        authority.integrity_check()?;
        let snapshot = authority.authority_snapshot()?;
        let prepared_root = ParquetObjectStore::acquire_prepared_root_authority(root, true)?;
        let token = AuthorityMutationToken::new();
        let prepared = match snapshot.state() {
            AuthorityState::InitializationRequired => {
                prepared_root.require_fresh_initialization_root()?;
                let endpoint = prepared_root.endpoint();
                let evidence_digest = authority.initialization_evidence_digest(endpoint)?;
                let catalog_identity = authority.catalog_endpoint_identity()?;
                let prepared = PreparedAuthorityTransition::try_new(
                    TransitionId::new(),
                    AuthorityTransitionKind::Initialize,
                    AuthorityGeneration::try_new(1)
                        .ok_or(AuthorityTransitionError::InvalidIdentity)?,
                    catalog_identity,
                    endpoint,
                    new_root_instance_id()?,
                    evidence_digest,
                    None,
                )
                .ok_or(AuthorityTransitionError::InvalidIdentity)?;
                authority.append_prepared_authority(&token, prepared.clone())?;
                checkpoint(
                    crate::parquet_store::RootBindingCheckpointInternal::CatalogPreparedDurable,
                )?;
                prepared
            }
            AuthorityState::Prepared { transition, .. }
                if transition.kind() == AuthorityTransitionKind::Initialize =>
            {
                transition.clone()
            }
            AuthorityState::Bound { transition, .. }
                if transition.prepared().kind() == AuthorityTransitionKind::Initialize =>
            {
                let activated = prepared_root.activate_bound_v2(transition)?;
                let objects = ParquetObjectStore::from_activated_root(activated, object_config)?;
                return Ok((authority, objects));
            }
            AuthorityState::LegacyRequired { .. } => {
                return Err(CatalogError::ArtifactRootMigrationRequired.into());
            }
            AuthorityState::Prepared { .. } | AuthorityState::Bound { .. } => {
                return Err(AuthorityTransitionError::TransitionConflict);
            }
        };
        let evidence =
            prepared_root.publish_or_recover_v2_with_checkpoint(&prepared, checkpoint)?;
        let bound = evidence.bind(prepared);
        authority.append_bound_authority(&token, bound.clone())?;
        checkpoint(crate::parquet_store::RootBindingCheckpointInternal::CatalogBoundDurable)?;
        let activated = prepared_root.activate_bound_v2(&bound)?;
        let objects = ParquetObjectStore::from_activated_root(activated, object_config)?;
        Ok((authority, objects))
    }

    #[cfg(test)]
    pub(crate) fn initialize_fault_fixture(
        authority: CatalogAuthority,
        root: ArtifactRoot,
        object_config: ObjectStoreConfig,
        crash_at: FirstBindCheckpoint,
    ) -> Result<(), AuthorityTransitionError> {
        let result =
            Self::initialize_with_checkpoint(authority, root, object_config, &mut |checkpoint| {
                if first_bind_checkpoint(checkpoint) == crash_at {
                    Err(ParquetStoreError::FirstBindFaultInjected)
                } else {
                    Ok(())
                }
            });
        match result {
            Err(AuthorityTransitionError::Root(ParquetStoreError::FirstBindFaultInjected)) => {
                Ok(())
            }
            Ok(_) | Err(_) => Err(AuthorityTransitionError::TransitionConflict),
        }
    }

    pub(crate) fn migrate_legacy(
        mut authority: CatalogAuthority,
        root: ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<(CatalogAuthority, ParquetObjectStore), AuthorityTransitionError> {
        authority.integrity_check()?;
        let snapshot = authority.authority_snapshot()?;
        let requirement = snapshot
            .legacy_requirement()
            .ok_or(AuthorityTransitionError::LegacyEvidenceMismatch)?;
        let catalog_identity = authority.catalog_endpoint_identity()?;
        if requirement.catalog_identity() != catalog_identity {
            return Err(AuthorityTransitionError::LegacyEvidenceMismatch);
        }
        let catalog_binding = authority.artifact_root_binding();
        let prepared_root = ParquetObjectStore::acquire_prepared_root_authority(root, false)?;
        let legacy_root = prepared_root.verify_legacy_v1(catalog_binding)?;
        let root_endpoint = prepared_root.endpoint();
        let evidence_digest = legacy::migration_evidence_digest(
            requirement,
            catalog_identity,
            catalog_binding,
            root_endpoint,
            &legacy_root,
        )?;
        let token = AuthorityMutationToken::new();
        let (prepared, append_prepared) = match snapshot.state() {
            AuthorityState::LegacyRequired {
                head,
                evidence_digest: required_evidence,
            } if *head == requirement.head()
                && *required_evidence == requirement.evidence_digest() =>
            {
                let prepared = PreparedAuthorityTransition::try_new(
                    TransitionId::new(),
                    AuthorityTransitionKind::LegacyMigration,
                    AuthorityGeneration::try_new(1)
                        .ok_or(AuthorityTransitionError::InvalidIdentity)?,
                    catalog_identity,
                    root_endpoint,
                    new_root_instance_id()?,
                    evidence_digest,
                    None,
                )
                .ok_or(AuthorityTransitionError::InvalidIdentity)?;
                (prepared, true)
            }
            AuthorityState::Prepared { transition, .. }
                if exact_legacy_transition(
                    transition,
                    catalog_identity,
                    root_endpoint,
                    evidence_digest,
                ) =>
            {
                (transition.clone(), false)
            }
            AuthorityState::Bound { transition, .. }
                if exact_legacy_transition(
                    transition.prepared(),
                    catalog_identity,
                    root_endpoint,
                    evidence_digest,
                ) =>
            {
                legacy_root.revalidate(&prepared_root, catalog_binding)?;
                let activated = prepared_root.activate_bound_v2(transition)?;
                let objects = ParquetObjectStore::from_activated_root(activated, object_config)?;
                return Ok((authority, objects));
            }
            AuthorityState::InitializationRequired
            | AuthorityState::LegacyRequired { .. }
            | AuthorityState::Prepared { .. }
            | AuthorityState::Bound { .. } => {
                return Err(AuthorityTransitionError::LegacyEvidenceMismatch);
            }
        };
        legacy_root.revalidate(&prepared_root, catalog_binding)?;
        if append_prepared {
            authority.append_prepared_authority(&token, prepared.clone())?;
        }
        legacy_root.revalidate(&prepared_root, catalog_binding)?;
        let evidence = prepared_root.publish_or_recover_v2(&prepared)?;
        legacy_root.revalidate(&prepared_root, catalog_binding)?;
        let bound = evidence.bind(prepared);
        authority.append_bound_authority(&token, bound.clone())?;
        let activated = prepared_root.activate_bound_v2(&bound)?;
        let objects = ParquetObjectStore::from_activated_root(activated, object_config)?;
        Ok((authority, objects))
    }

    pub(crate) fn restore(
        handoff: VerifiedRestoreHandoff,
        catalog_config: CatalogConfig,
        object_config: ObjectStoreConfig,
    ) -> Result<(CatalogAuthority, ParquetObjectStore), AuthorityTransitionError> {
        let receipt = handoff.receipt();
        let (source_catalog, installed_catalog, materialized_root, _source_evidence) =
            handoff.into_retained_parts();
        source_catalog.revalidate()?;
        let mut authority = CatalogAuthority::open_installed(catalog_config, installed_catalog)?;
        authority.integrity_check()?;
        let snapshot = authority.authority_snapshot_without_endpoint()?;
        let (artifact_root, retained_directory) = materialized_root.into_retained_capabilities();
        let prepared_root =
            ParquetObjectStore::acquire_prepared_root_authority(artifact_root, true)?;
        drop(retained_directory);
        let target_catalog = authority.catalog_endpoint_identity()?;
        let target_root = prepared_root.endpoint();
        let evidence_digest =
            target_authority_evidence_from_receipt(receipt, target_catalog, target_root)?;
        let generation = receipt
            .source_authority_generation()
            .get()
            .checked_add(1)
            .and_then(AuthorityGeneration::try_new)
            .ok_or(AuthorityTransitionError::InvalidIdentity)?;
        let restore_receipt = restore_receipt_fields(receipt);
        let token = AuthorityMutationToken::new();

        let prepared = match snapshot.state() {
            AuthorityState::Bound { head, transition }
                if head.event_digest() == receipt.source_authority_event()
                    && transition.prepared().target_catalog_identity()
                        == receipt.source_catalog_identity()
                    && transition.prepared().authority_generation()
                        == receipt.source_authority_generation()
                    && transition.prepared().evidence_digest()
                        == receipt.source_authority_evidence()
                    && transition.stable_root_identity() == receipt.source_root_identity() =>
            {
                let prepared = PreparedAuthorityTransition::try_new(
                    TransitionId::new(),
                    AuthorityTransitionKind::BackupRestore,
                    generation,
                    target_catalog,
                    target_root,
                    new_root_instance_id()?,
                    evidence_digest,
                    Some(restore_receipt.clone()),
                )
                .ok_or(AuthorityTransitionError::InvalidIdentity)?;
                authority.append_prepared_authority(&token, prepared.clone())?;
                prepared
            }
            AuthorityState::Prepared { transition, .. }
                if transition.kind() == AuthorityTransitionKind::BackupRestore
                    && transition.authority_generation() == generation
                    && transition.target_catalog_identity() == target_catalog
                    && transition.target_root_endpoint_identity() == target_root
                    && transition.evidence_digest() == evidence_digest
                    && transition.restore_receipt() == Some(&restore_receipt) =>
            {
                transition.clone()
            }
            AuthorityState::Bound { transition, .. }
                if transition.prepared().kind() == AuthorityTransitionKind::BackupRestore
                    && transition.prepared().authority_generation() == generation
                    && transition.prepared().target_catalog_identity() == target_catalog
                    && transition.prepared().target_root_endpoint_identity() == target_root
                    && transition.prepared().evidence_digest() == evidence_digest
                    && transition.prepared().restore_receipt() == Some(&restore_receipt) =>
            {
                let activated = prepared_root.activate_bound_v2(transition)?;
                let objects = ParquetObjectStore::from_activated_root(activated, object_config)?;
                source_catalog.revalidate()?;
                return Ok((authority, objects));
            }
            AuthorityState::InitializationRequired
            | AuthorityState::LegacyRequired { .. }
            | AuthorityState::Prepared { .. }
            | AuthorityState::Bound { .. } => {
                return Err(AuthorityTransitionError::TransitionConflict);
            }
        };
        source_catalog.revalidate()?;
        let evidence = prepared_root.publish_or_recover_v2(&prepared)?;
        source_catalog.revalidate()?;
        let bound = evidence.bind(prepared);
        authority.append_bound_authority(&token, bound.clone())?;
        let activated = prepared_root.activate_bound_v2(&bound)?;
        let objects = ParquetObjectStore::from_activated_root(activated, object_config)?;
        source_catalog.revalidate()?;
        Ok((authority, objects))
    }

    pub(crate) fn open_bound(
        authority: CatalogAuthority,
        root: ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<(CatalogAuthority, ParquetObjectStore), AuthorityTransitionError> {
        authority.integrity_check()?;
        let snapshot = authority.authority_snapshot()?;
        let AuthorityState::Bound { transition, .. } = snapshot.state() else {
            return match snapshot.state() {
                AuthorityState::InitializationRequired => {
                    Err(CatalogError::ArtifactRootAuthorityInitializationRequired.into())
                }
                AuthorityState::LegacyRequired { .. } => {
                    Err(CatalogError::ArtifactRootMigrationRequired.into())
                }
                AuthorityState::Prepared { .. } => {
                    Err(CatalogError::ArtifactRootAuthorityNotBound.into())
                }
                AuthorityState::Bound { .. } => Err(AuthorityTransitionError::NotBound),
            };
        };
        let prepared_root = ParquetObjectStore::acquire_prepared_root_authority(root, false)?;
        let activated = prepared_root.activate_bound_v2(transition)?;
        let objects = ParquetObjectStore::from_activated_root(activated, object_config)?;
        Ok((authority, objects))
    }
}

fn exact_legacy_transition(
    transition: &PreparedAuthorityTransition,
    catalog_identity: CatalogEndpointIdentity,
    root_endpoint: RootEndpointIdentity,
    evidence_digest: AuthorityEvidenceDigest,
) -> bool {
    transition.kind() == AuthorityTransitionKind::LegacyMigration
        && transition.authority_generation().get() == 1
        && transition.target_catalog_identity() == catalog_identity
        && transition.target_root_endpoint_identity() == root_endpoint
        && transition.evidence_digest() == evidence_digest
        && transition.restore_receipt().is_none()
}

fn restore_receipt_fields(
    receipt: crate::analytical_backup::AnalyticalBackupBundleReceipt,
) -> RestoreReceiptFields {
    RestoreReceiptFields::new(
        receipt.source_catalog_identity(),
        receipt.source_root_identity(),
        receipt.source_authority_generation(),
        receipt.source_authority_event(),
        receipt.source_authority_evidence(),
        receipt.catalog_content_evidence(),
        receipt.artifact_inventory_sha256(),
        *receipt.catalog_backup(),
        receipt.cutoff(),
    )
}

fn new_root_instance_id() -> Result<RootInstanceId, AuthorityTransitionError> {
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    bytes[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    RootInstanceId::try_new(bytes).ok_or(AuthorityTransitionError::InvalidIdentity)
}

impl AuthorityState {
    pub(crate) const fn head(&self) -> Option<AuthorityHead> {
        match self {
            Self::InitializationRequired => None,
            Self::LegacyRequired { head, .. }
            | Self::Prepared { head, .. }
            | Self::Bound { head, .. } => Some(*head),
        }
    }

    pub(crate) const fn bound(&self) -> Option<&BoundAuthorityTransition> {
        match self {
            Self::Bound { transition, .. } => Some(transition),
            Self::InitializationRequired | Self::LegacyRequired { .. } | Self::Prepared { .. } => {
                None
            }
        }
    }
}

/// Typed failure of an explicit catalog/root authority transition.
#[derive(Debug, Error)]
pub(crate) enum AuthorityTransitionError {
    /// Durable catalog state or event-chain validation failed.
    #[error("catalog authority transition failed: {0}")]
    Catalog(#[from] CatalogError),
    /// Capability-relative root validation or durable control publication failed.
    #[error("artifact-root authority transition failed: {0}")]
    Root(#[from] ParquetStoreError),
    /// Receipt-bound restore evidence or retained capabilities failed validation.
    #[error("analytical restore transition evidence is invalid: {0}")]
    Restore(#[from] RestoreValidationError),
    /// A decoded identity, digest, generation, or event field was invalid.
    #[error("authority transition identity is invalid")]
    InvalidIdentity,
    /// Durable state names another exact transition or endpoint.
    #[error("authority transition conflicts with durable state")]
    TransitionConflict,
    /// Version-3/version-4 migration lacks exact catalog and root evidence.
    #[error("legacy authority transition evidence does not match")]
    LegacyEvidenceMismatch,
    /// Ordinary activation was requested before an exact bound event existed.
    #[error("analytical artifact-root authority is not bound")]
    NotBound,
}

#[cfg(test)]
fn first_bind_checkpoint(
    checkpoint: crate::parquet_store::RootBindingCheckpointInternal,
) -> FirstBindCheckpoint {
    use crate::parquet_store::RootBindingCheckpointInternal;

    match checkpoint {
        RootBindingCheckpointInternal::CatalogPreparedDurable => {
            FirstBindCheckpoint::CatalogPrepared
        }
        RootBindingCheckpointInternal::MarkerPreparedDurable => FirstBindCheckpoint::MarkerPending,
        RootBindingCheckpointInternal::MarkerDurable => FirstBindCheckpoint::MarkerFinal,
        RootBindingCheckpointInternal::RootBindingPreparedDurable => {
            FirstBindCheckpoint::BindingPending
        }
        RootBindingCheckpointInternal::RootBindingDurable => FirstBindCheckpoint::BindingFinal,
        RootBindingCheckpointInternal::CatalogBoundDurable => FirstBindCheckpoint::CatalogBound,
    }
}
