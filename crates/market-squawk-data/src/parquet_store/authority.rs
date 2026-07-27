//! Durable, exact retained-root ownership and first-bind recovery.

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use cap_fs_ext::{DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use fs2::FileExt as _;
use market_squawk_platform::ArtifactRoot;
use sha2::{Digest as _, Sha256};
#[cfg(test)]
use uuid::Uuid;

use super::{ParquetStoreError, sync_directory};
use crate::authority_transition::{
    BoundAuthorityTransition, ControlRecordDigest, PreparedAuthorityTransition,
    RootEndpointIdentity, StableArtifactRootIdentity,
};
use crate::publication_coordinator::PublicationCoordinator;

#[cfg(unix)]
#[path = "authority/unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "authority/windows.rs"]
mod platform;

#[cfg(any(unix, windows))]
use platform::{configure_private_root_control, private_root_control_metadata};
#[cfg(all(test, any(unix, windows)))]
use platform::{publish_prepared_root_record, recover_committed_root_record};

const ROOT_AUTHORITY_LOCK: &str = ".analytical-root-authority.lock";
const LEGACY_PAPER_REPOSITORY_LOCK: &str = ".market-squawk-paper-checkpoints.lock";
const ROOT_IDENTITY_MARKER: &str = ".analytical-root.identity";
#[cfg(any(test, windows))]
const ROOT_IDENTITY_PENDING: &str = ".analytical-root.identity.pending";
const ROOT_IDENTITY_MARKER_V2: &str = ".analytical-root.identity.v2";
const ROOT_IDENTITY_PENDING_V2: &str = ".analytical-root.identity.v2.pending";
const ROOT_CATALOG_BINDING: &str = ".analytical-root-catalog.binding";
#[cfg(any(test, windows))]
const ROOT_CATALOG_BINDING_PENDING: &str = ".analytical-root-catalog.binding.pending";
const ROOT_RECORD_VERSION: u16 = 1;
const ROOT_RECORD_BYTES: usize = 106;
const ROOT_MARKER_MAGIC: &[u8; 8] = b"MSQKROOT";
const ROOT_BINDING_MAGIC: &[u8; 8] = b"MSQKBIND";
const ROOT_MARKER_V2_MAGIC: &[u8; 8] = b"MSQKRTV2";
const ROOT_BINDING_V2_MAGIC: &[u8; 8] = b"MSQKBV2!";
const ROOT_RECORD_V2_VERSION: u16 = 2;

static OPEN_ARTIFACT_ROOTS: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactRootIdentity {
    pub(super) path: PathBuf,
    pub(super) stable_root: [u8; 32],
    pub(super) catalog_binding: [u8; 32],
}

#[allow(
    clippy::enum_variant_names,
    reason = "variant names are exact durable crash-boundary locators"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootBindingCheckpointInternal {
    CatalogPreparedDurable,
    MarkerPreparedDurable,
    MarkerDurable,
    RootBindingPreparedDurable,
    RootBindingDurable,
    CatalogBoundDurable,
}

struct RootRegistryGuard {
    path: PathBuf,
}

impl Drop for RootRegistryGuard {
    fn drop(&mut self) {
        if let Ok(mut roots) = OPEN_ARTIFACT_ROOTS
            .get_or_init(|| Mutex::new(BTreeSet::new()))
            .lock()
        {
            roots.remove(&self.path);
        }
    }
}

pub(super) struct RootAuthority {
    pub(super) identity: ArtifactRootIdentity,
    pub(super) publication: PublicationCoordinator,
    _lock: File,
    _registry: RootRegistryGuard,
}

/// Ordered process-root and cross-process-root ownership retained through a catalog transition.
pub(crate) struct PreparedRootAuthority {
    root: ArtifactRoot,
    directory: Dir,
    endpoint: RootEndpointIdentity,
    lock: File,
    registry: RootRegistryGuard,
}

pub(crate) struct VerifiedRestoreControlSubset {
    names: BTreeSet<String>,
}

impl VerifiedRestoreControlSubset {
    pub(crate) fn contains(&self, name: &str) -> bool {
        self.names.contains(name)
    }
}

impl std::fmt::Debug for PreparedRootAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedRootAuthority")
            .field("root", &"[RETAINED ARTIFACT ROOT]")
            .field("directory", &"[DIRECTORY CAPABILITY]")
            .field("endpoint", &self.endpoint)
            .field("lock", &"[EXCLUSIVE ROOT LOCK]")
            .finish()
    }
}

/// Exact durable root-control result admitted by the catalog's Bound event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RootActivationEvidence {
    marker_digest: ControlRecordDigest,
    stable_root: StableArtifactRootIdentity,
    binding_digest: ControlRecordDigest,
}

/// Retained exact version-1 root controls admitted for one explicit migration.
pub(crate) struct VerifiedLegacyRootAuthority {
    marker: File,
    marker_record: Vec<u8>,
    binding: File,
    binding_record: Vec<u8>,
    stable_root: StableArtifactRootIdentity,
}

impl VerifiedLegacyRootAuthority {
    pub(crate) const fn stable_root(&self) -> StableArtifactRootIdentity {
        self.stable_root
    }

    pub(crate) fn marker_record(&self) -> &[u8] {
        &self.marker_record
    }

    pub(crate) fn binding_record(&self) -> &[u8] {
        &self.binding_record
    }

    pub(crate) fn revalidate(
        &self,
        prepared_root: &PreparedRootAuthority,
        catalog_binding: [u8; 32],
    ) -> Result<(), ParquetStoreError> {
        if root_endpoint_identity(&prepared_root.directory, prepared_root.root.root())?
            != prepared_root.endpoint
        {
            return Err(ParquetStoreError::RootCatalogMismatch);
        }
        revalidate_legacy_record(
            &prepared_root.directory,
            ROOT_IDENTITY_MARKER,
            ROOT_MARKER_MAGIC,
            catalog_binding,
            None,
            &self.marker,
            &self.marker_record,
        )?;
        let stable_root = root_identity(
            &prepared_root.directory,
            prepared_root.root.root(),
            &self.marker,
            &self.marker_record,
        )?;
        if stable_root != self.stable_root.bytes() {
            return Err(ParquetStoreError::RootCatalogMismatch);
        }
        revalidate_legacy_record(
            &prepared_root.directory,
            ROOT_CATALOG_BINDING,
            ROOT_BINDING_MAGIC,
            catalog_binding,
            Some(stable_root),
            &self.binding,
            &self.binding_record,
        )
    }
}

impl std::fmt::Debug for VerifiedLegacyRootAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifiedLegacyRootAuthority")
            .field("marker", &"[RETAINED EXACT FILE]")
            .field("binding", &"[RETAINED EXACT FILE]")
            .field("stable_root", &self.stable_root)
            .finish()
    }
}

impl RootActivationEvidence {
    pub(crate) fn bind(self, prepared: PreparedAuthorityTransition) -> BoundAuthorityTransition {
        BoundAuthorityTransition::new(
            prepared,
            self.marker_digest,
            self.stable_root,
            self.binding_digest,
        )
    }
}

/// Fully verified root capabilities transferred into the active Parquet object store.
pub(crate) struct ActivatedRootAuthority {
    pub(super) root: ArtifactRoot,
    pub(super) directory: Dir,
    pub(super) authority: RootAuthority,
}

impl std::fmt::Debug for ActivatedRootAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActivatedRootAuthority")
            .field("root", &"[RETAINED ARTIFACT ROOT]")
            .field("directory", &"[DIRECTORY CAPABILITY]")
            .field("authority", &self.authority)
            .finish()
    }
}

pub(crate) fn acquire_prepared_root_authority(
    root: ArtifactRoot,
    create_lock: bool,
) -> Result<PreparedRootAuthority, ParquetStoreError> {
    require_supported_root_authority_platform()?;
    let directory = root
        .try_clone_directory()
        .map_err(crate::parquet_store::map_artifact_root_clone_error)?;
    let registry = acquire_process_registry(root.root())?;
    let lock = open_root_authority_lock(&directory, create_lock)?;
    match lock.try_lock_exclusive() {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            return Err(ParquetStoreError::RootAuthorityAlreadyOwned);
        }
        Err(error) => return Err(error.into()),
    }
    validate_root_control_file(&directory, ROOT_AUTHORITY_LOCK, &lock, 1)?;
    let endpoint = root_endpoint_identity(&directory, root.root())?;
    Ok(PreparedRootAuthority {
        root,
        directory,
        endpoint,
        lock,
        registry,
    })
}

pub(crate) fn restore_root_endpoint(
    root: &ArtifactRoot,
) -> Result<RootEndpointIdentity, ParquetStoreError> {
    require_supported_root_authority_platform()?;
    let directory = root
        .try_clone_directory()
        .map_err(crate::parquet_store::map_artifact_root_clone_error)?;
    root_endpoint_identity(&directory, root.root())
}

pub(crate) fn validate_restore_control_subset(
    directory: &Dir,
    prepared: &PreparedAuthorityTransition,
    catalog_bound: bool,
) -> Result<VerifiedRestoreControlSubset, ParquetStoreError> {
    require_only_expected_v2_control_files(directory, prepared)?;
    let lock = open_root_control_file(directory, ROOT_AUTHORITY_LOCK, 1)?
        .ok_or(ParquetStoreError::RootCatalogMismatch)?;
    if lock.metadata()?.len() != 0 {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    validate_root_control_file(directory, ROOT_AUTHORITY_LOCK, &lock, 1)?;
    let marker = validate_exact_record_subset(
        directory,
        ROOT_IDENTITY_MARKER_V2,
        ROOT_IDENTITY_PENDING_V2,
        &encode_root_marker_v2(prepared),
    )?;
    let binding_name = root_binding_generation_name(prepared);
    let binding_pending = format!("{binding_name}.pending");
    let marker_digest = control_record_digest(&encode_root_marker_v2(prepared))?;
    let stable_root = stable_root_identity_v2(prepared, marker_digest)?;
    let binding = validate_exact_record_subset(
        directory,
        &binding_name,
        &binding_pending,
        &encode_root_binding_v2(prepared, marker_digest, stable_root),
    )?;
    if !matches!(binding.state, RestoreRecordState::Absent)
        && !matches!(marker.state, RestoreRecordState::Committed)
    {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    if catalog_bound
        && (!matches!(marker.state, RestoreRecordState::Committed)
            || marker.pending_present
            || !matches!(binding.state, RestoreRecordState::Committed)
            || binding.pending_present)
    {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    let mut names = BTreeSet::new();
    names.insert(ROOT_AUTHORITY_LOCK.to_owned());
    names.extend(marker.names);
    names.extend(binding.names);
    if catalog_bound {
        let staging = root_control_exists(directory, "staging")?;
        let quarantine = root_control_exists(directory, "quarantine")?;
        match (staging, quarantine) {
            (false, false) => {}
            (true, true) => {
                validate_empty_store_namespace(directory, "staging", "parquet")?;
                validate_empty_store_namespace(directory, "quarantine", "parquet")?;
                names.insert("staging".to_owned());
                names.insert("quarantine".to_owned());
            }
            (false, true) | (true, false) => {
                return Err(ParquetStoreError::RootCatalogMismatch);
            }
        }
    }
    Ok(VerifiedRestoreControlSubset { names })
}

fn validate_empty_store_namespace(
    directory: &Dir,
    namespace: &str,
    leaf: &str,
) -> Result<(), ParquetStoreError> {
    let namespace = directory
        .open_dir_nofollow(namespace)
        .map_err(|_| ParquetStoreError::RootCatalogMismatch)?;
    let mut entries = namespace.entries()?;
    let entry = entries
        .next()
        .transpose()?
        .ok_or(ParquetStoreError::RootCatalogMismatch)?;
    if entry.file_name() != leaf || entries.next().transpose()?.is_some() {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    let leaf = namespace
        .open_dir_nofollow(leaf)
        .map_err(|_| ParquetStoreError::RootCatalogMismatch)?;
    if leaf.entries()?.next().transpose()?.is_some() {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum RestoreRecordState {
    Absent,
    Pending,
    Committed,
}

struct RestoreRecordSubset {
    state: RestoreRecordState,
    pending_present: bool,
    names: BTreeSet<String>,
}

fn validate_exact_record_subset(
    directory: &Dir,
    final_name: &str,
    pending_name: &str,
    expected: &[u8],
) -> Result<RestoreRecordSubset, ParquetStoreError> {
    let final_exists = root_control_exists(directory, final_name)?;
    let pending_exists = root_control_exists(directory, pending_name)?;
    let mut names = BTreeSet::new();
    match (final_exists, pending_exists) {
        (false, false) => Ok(RestoreRecordSubset {
            state: RestoreRecordState::Absent,
            pending_present: false,
            names,
        }),
        (false, true) => {
            open_exact_root_control_file(directory, pending_name, expected, 1)?
                .ok_or(ParquetStoreError::RootCatalogMismatch)?;
            names.insert(pending_name.to_owned());
            Ok(RestoreRecordSubset {
                state: RestoreRecordState::Pending,
                pending_present: true,
                names,
            })
        }
        (true, false) => {
            open_exact_root_control_file(directory, final_name, expected, 1)?
                .ok_or(ParquetStoreError::RootCatalogMismatch)?;
            names.insert(final_name.to_owned());
            Ok(RestoreRecordSubset {
                state: RestoreRecordState::Committed,
                pending_present: false,
                names,
            })
        }
        (true, true) => {
            validate_linked_record_subset(directory, final_name, pending_name, expected, names)
        }
    }
}

#[cfg(unix)]
fn validate_linked_record_subset(
    directory: &Dir,
    final_name: &str,
    pending_name: &str,
    expected: &[u8],
    mut names: BTreeSet<String>,
) -> Result<RestoreRecordSubset, ParquetStoreError> {
    let final_file = open_exact_root_control_file(directory, final_name, expected, 2)?
        .ok_or(ParquetStoreError::RootCatalogMismatch)?;
    let pending_file = open_exact_root_control_file(directory, pending_name, expected, 2)?
        .ok_or(ParquetStoreError::RootCatalogMismatch)?;
    if !same_exact_opened_file(&final_file, &pending_file)? {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    names.insert(final_name.to_owned());
    names.insert(pending_name.to_owned());
    Ok(RestoreRecordSubset {
        state: RestoreRecordState::Committed,
        pending_present: true,
        names,
    })
}

#[cfg(not(unix))]
fn validate_linked_record_subset(
    _directory: &Dir,
    _final_name: &str,
    _pending_name: &str,
    _expected: &[u8],
    _names: BTreeSet<String>,
) -> Result<RestoreRecordSubset, ParquetStoreError> {
    Err(ParquetStoreError::RootCatalogMismatch)
}

impl PreparedRootAuthority {
    pub(crate) const fn endpoint(&self) -> RootEndpointIdentity {
        self.endpoint
    }

    pub(crate) fn require_fresh_initialization_root(&self) -> Result<(), ParquetStoreError> {
        for entry in self.directory.entries()? {
            let entry = entry?;
            let name = entry.file_name();
            if name != ROOT_AUTHORITY_LOCK
                && !is_safe_legacy_paper_repository_lock(&self.directory, &name)?
            {
                return Err(ParquetStoreError::RootCatalogMismatch);
            }
        }
        Ok(())
    }

    pub(crate) fn verify_legacy_v1(
        &self,
        catalog_binding: [u8; 32],
    ) -> Result<VerifiedLegacyRootAuthority, ParquetStoreError> {
        require_only_legacy_control_files(&self.directory)?;
        let marker = open_committed_record(
            &self.directory,
            ROOT_IDENTITY_MARKER,
            ROOT_MARKER_MAGIC,
            catalog_binding,
            None,
            1,
        )?;
        if marker.record.payload == [0; 32] {
            return Err(ParquetStoreError::RootCatalogMismatch);
        }
        let stable_root = root_identity(
            &self.directory,
            self.root.root(),
            &marker.file,
            &marker.record.bytes,
        )?;
        let stable_root = StableArtifactRootIdentity::try_new(stable_root)
            .ok_or(ParquetStoreError::RootCatalogMismatch)?;
        let binding = open_committed_record(
            &self.directory,
            ROOT_CATALOG_BINDING,
            ROOT_BINDING_MAGIC,
            catalog_binding,
            Some(stable_root.bytes()),
            1,
        )?;
        Ok(VerifiedLegacyRootAuthority {
            marker: marker.file,
            marker_record: marker.record.bytes,
            binding: binding.file,
            binding_record: binding.record.bytes,
            stable_root,
        })
    }

    pub(crate) fn publish_or_recover_v2(
        &self,
        prepared: &PreparedAuthorityTransition,
    ) -> Result<RootActivationEvidence, ParquetStoreError> {
        self.publish_or_recover_v2_with_checkpoint(prepared, &mut |_| Ok(()))
    }

    pub(crate) fn publish_or_recover_v2_with_checkpoint(
        &self,
        prepared: &PreparedAuthorityTransition,
        checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
    ) -> Result<RootActivationEvidence, ParquetStoreError> {
        if prepared.target_root_endpoint_identity() != self.endpoint {
            return Err(ParquetStoreError::RootCatalogMismatch);
        }
        require_only_expected_v2_control_files(&self.directory, prepared)?;
        let marker_record = encode_root_marker_v2(prepared);
        publish_or_recover_exact_record(
            &self.directory,
            &self.root,
            ROOT_IDENTITY_MARKER_V2,
            ROOT_IDENTITY_PENDING_V2,
            &marker_record,
            RootBindingCheckpointInternal::MarkerPreparedDurable,
            RootBindingCheckpointInternal::MarkerDurable,
            checkpoint,
        )?;
        let marker_digest = control_record_digest(&marker_record)?;
        let stable_root = stable_root_identity_v2(prepared, marker_digest)?;
        let binding_record = encode_root_binding_v2(prepared, marker_digest, stable_root);
        let binding_name = root_binding_generation_name(prepared);
        let pending_name = format!("{binding_name}.pending");
        publish_or_recover_exact_record(
            &self.directory,
            &self.root,
            &binding_name,
            &pending_name,
            &binding_record,
            RootBindingCheckpointInternal::RootBindingPreparedDurable,
            RootBindingCheckpointInternal::RootBindingDurable,
            checkpoint,
        )?;
        Ok(RootActivationEvidence {
            marker_digest,
            stable_root,
            binding_digest: control_record_digest(&binding_record)?,
        })
    }

    pub(crate) fn activate_bound_v2(
        self,
        bound: &BoundAuthorityTransition,
    ) -> Result<ActivatedRootAuthority, ParquetStoreError> {
        let prepared = bound.prepared();
        if prepared.target_root_endpoint_identity() != self.endpoint {
            return Err(ParquetStoreError::RootCatalogMismatch);
        }
        require_only_committed_v2_control_files(&self.directory, prepared)?;
        let marker_record = encode_root_marker_v2(prepared);
        open_exact_root_control_file(&self.directory, ROOT_IDENTITY_MARKER_V2, &marker_record, 1)?;
        let marker_digest = control_record_digest(&marker_record)?;
        let stable_root = stable_root_identity_v2(prepared, marker_digest)?;
        let binding_record = encode_root_binding_v2(prepared, marker_digest, stable_root);
        open_exact_root_control_file(
            &self.directory,
            &root_binding_generation_name(prepared),
            &binding_record,
            1,
        )?;
        if marker_digest != bound.root_marker_record_digest()
            || stable_root != bound.stable_root_identity()
            || control_record_digest(&binding_record)? != bound.root_binding_record_digest()
        {
            return Err(ParquetStoreError::RootCatalogMismatch);
        }
        let identity = ArtifactRootIdentity {
            path: self.root.root().to_path_buf(),
            stable_root: stable_root.bytes(),
            catalog_binding: prepared.target_catalog_identity().bytes(),
        };
        Ok(ActivatedRootAuthority {
            root: self.root,
            directory: self.directory,
            authority: RootAuthority {
                identity,
                publication: PublicationCoordinator::default(),
                _lock: self.lock,
                _registry: self.registry,
            },
        })
    }
}

fn require_only_legacy_control_files(directory: &Dir) -> Result<(), ParquetStoreError> {
    for entry in directory.entries()? {
        let entry = entry?;
        let name = entry.file_name();
        if !matches!(
            name.to_str(),
            Some(
                ROOT_AUTHORITY_LOCK
                    | ROOT_IDENTITY_MARKER
                    | ROOT_CATALOG_BINDING
                    | "objects"
                    | "staging"
                    | "quarantine"
            )
        ) && !is_safe_shared_artifact_namespace(directory, &name)?
            && !is_safe_legacy_paper_repository_lock(directory, &name)?
        {
            return Err(ParquetStoreError::RootCatalogMismatch);
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "legacy revalidation binds both the retained file and its exact v1 record semantics"
)]
fn revalidate_legacy_record(
    directory: &Dir,
    name: &str,
    magic: &[u8; 8],
    catalog_binding: [u8; 32],
    expected_payload: Option<[u8; 32]>,
    retained: &File,
    expected_record: &[u8],
) -> Result<(), ParquetStoreError> {
    validate_root_control_file(directory, name, retained, 1)?;
    let mut reopened = retained.try_clone()?;
    let record =
        read_root_record(&mut reopened, magic)?.ok_or(ParquetStoreError::RootCatalogMismatch)?;
    verify_root_record(&record, catalog_binding, expected_payload)?;
    if record.bytes != expected_record {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    validate_root_control_file(directory, name, retained, 1)
}

fn open_root_authority_lock(directory: &Dir, create: bool) -> Result<File, ParquetStoreError> {
    if create {
        return open_or_create_root_authority_lock(directory);
    }
    open_root_control_file(directory, ROOT_AUTHORITY_LOCK, 1)?
        .ok_or(ParquetStoreError::RootCatalogMismatch)
}

fn root_endpoint_identity(
    directory: &Dir,
    root_path: &Path,
) -> Result<RootEndpointIdentity, ParquetStoreError> {
    use cap_fs_ext::MetadataExt as _;

    let metadata = directory.dir_metadata()?;
    let path = root_path.as_os_str().as_encoded_bytes();
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/analytical-root-endpoint/v2");
    digest.update(
        u64::try_from(path.len())
            .map_err(|_| ParquetStoreError::SizeOverflow)?
            .to_be_bytes(),
    );
    digest.update(path);
    digest.update(metadata.dev().to_be_bytes());
    digest.update(metadata.ino().to_be_bytes());
    RootEndpointIdentity::try_new(digest.finalize().into())
        .ok_or(ParquetStoreError::RootCatalogMismatch)
}

fn encode_root_marker_v2(prepared: &PreparedAuthorityTransition) -> Vec<u8> {
    let mut record = Vec::with_capacity(122);
    record.extend_from_slice(ROOT_MARKER_V2_MAGIC);
    record.extend_from_slice(&ROOT_RECORD_V2_VERSION.to_be_bytes());
    record.extend_from_slice(prepared.transition_id().as_uuid().as_bytes());
    record.extend_from_slice(&prepared.target_root_endpoint_identity().bytes());
    record.extend_from_slice(&prepared.root_instance_id().bytes());
    append_v2_record_checksum(b"market-squawk/analytical-root-marker/v2", &mut record);
    record
}

fn encode_root_binding_v2(
    prepared: &PreparedAuthorityTransition,
    marker_digest: ControlRecordDigest,
    stable_root: StableArtifactRootIdentity,
) -> Vec<u8> {
    let mut record = Vec::with_capacity(267);
    record.extend_from_slice(ROOT_BINDING_V2_MAGIC);
    record.extend_from_slice(&ROOT_RECORD_V2_VERSION.to_be_bytes());
    record.extend_from_slice(&prepared.root_binding_generation().get().to_be_bytes());
    record.extend_from_slice(prepared.transition_id().as_uuid().as_bytes());
    record.push(match prepared.kind() {
        crate::authority_transition::AuthorityTransitionKind::Initialize => 1,
        crate::authority_transition::AuthorityTransitionKind::LegacyMigration => 2,
        crate::authority_transition::AuthorityTransitionKind::BackupRestore => 3,
    });
    record.extend_from_slice(&prepared.authority_generation().get().to_be_bytes());
    record.extend_from_slice(&prepared.target_catalog_identity().bytes());
    record.extend_from_slice(&prepared.target_root_endpoint_identity().bytes());
    record.extend_from_slice(&prepared.root_instance_id().bytes());
    record.extend_from_slice(&prepared.evidence_digest().bytes());
    record.extend_from_slice(&marker_digest.bytes());
    record.extend_from_slice(&stable_root.bytes());
    append_v2_record_checksum(b"market-squawk/analytical-root-binding/v2", &mut record);
    record
}

fn append_v2_record_checksum(domain: &[u8], record: &mut Vec<u8>) {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(record.as_slice());
    record.extend_from_slice(&digest.finalize());
}

fn control_record_digest(record: &[u8]) -> Result<ControlRecordDigest, ParquetStoreError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/analytical-root-control-record/v2");
    digest.update(
        u64::try_from(record.len())
            .map_err(|_| ParquetStoreError::SizeOverflow)?
            .to_be_bytes(),
    );
    digest.update(record);
    ControlRecordDigest::try_new(digest.finalize().into())
        .ok_or(ParquetStoreError::RootCatalogMismatch)
}

fn stable_root_identity_v2(
    prepared: &PreparedAuthorityTransition,
    marker_digest: ControlRecordDigest,
) -> Result<StableArtifactRootIdentity, ParquetStoreError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/analytical-root-identity/v2");
    digest.update(prepared.target_root_endpoint_identity().bytes());
    digest.update(prepared.root_instance_id().bytes());
    digest.update(marker_digest.bytes());
    StableArtifactRootIdentity::try_new(digest.finalize().into())
        .ok_or(ParquetStoreError::RootCatalogMismatch)
}

fn root_binding_generation_name(prepared: &PreparedAuthorityTransition) -> String {
    format!(
        ".analytical-root-catalog.binding.{:016}",
        prepared.root_binding_generation().get()
    )
}

fn require_only_expected_v2_control_files(
    directory: &Dir,
    prepared: &PreparedAuthorityTransition,
) -> Result<(), ParquetStoreError> {
    let binding = root_binding_generation_name(prepared);
    let pending = format!("{binding}.pending");
    for entry in directory.entries()? {
        let entry = entry?;
        let name = entry.file_name();
        let data_namespace = matches!(name.to_str(), Some("objects" | "staging" | "quarantine"));
        let legacy_control = prepared.kind()
            == crate::authority_transition::AuthorityTransitionKind::LegacyMigration
            && matches!(
                name.to_str(),
                Some(ROOT_IDENTITY_MARKER | ROOT_CATALOG_BINDING)
            );
        let restored_data = prepared.kind()
            == crate::authority_transition::AuthorityTransitionKind::BackupRestore
            && data_namespace;
        if name != ROOT_AUTHORITY_LOCK
            && name != ROOT_IDENTITY_MARKER_V2
            && name != ROOT_IDENTITY_PENDING_V2
            && name != binding.as_str()
            && name != pending.as_str()
            && !legacy_control
            && !(prepared.kind()
                == crate::authority_transition::AuthorityTransitionKind::LegacyMigration
                && data_namespace)
            && !restored_data
            && !(prepared.kind()
                == crate::authority_transition::AuthorityTransitionKind::LegacyMigration
                && is_safe_shared_artifact_namespace(directory, &name)?)
            && !is_safe_legacy_paper_repository_lock(directory, &name)?
        {
            return Err(ParquetStoreError::RootCatalogMismatch);
        }
    }
    Ok(())
}

fn require_only_committed_v2_control_files(
    directory: &Dir,
    prepared: &PreparedAuthorityTransition,
) -> Result<(), ParquetStoreError> {
    let binding = root_binding_generation_name(prepared);
    for entry in directory.entries()? {
        let entry = entry?;
        let name = entry.file_name();
        let legacy_control = prepared.kind()
            == crate::authority_transition::AuthorityTransitionKind::LegacyMigration
            && matches!(
                name.to_str(),
                Some(ROOT_IDENTITY_MARKER | ROOT_CATALOG_BINDING)
            );
        if name != ROOT_AUTHORITY_LOCK
            && name != ROOT_IDENTITY_MARKER_V2
            && name != binding.as_str()
            && name != "objects"
            && name != "staging"
            && name != "quarantine"
            && !legacy_control
            && !is_safe_shared_artifact_namespace(directory, &name)?
            && !is_safe_legacy_paper_repository_lock(directory, &name)?
        {
            return Err(ParquetStoreError::RootCatalogMismatch);
        }
    }
    Ok(())
}

fn is_safe_shared_artifact_namespace(
    directory: &Dir,
    name: &OsStr,
) -> Result<bool, ParquetStoreError> {
    use cap_fs_ext::MetadataExt as _;

    let Some(name_text) = name.to_str() else {
        return Ok(false);
    };
    if name_text.is_empty()
        || name_text.len() > 255
        || name_text.starts_with('.')
        || name_text.chars().any(char::is_control)
    {
        return Ok(false);
    }
    let metadata = directory.symlink_metadata(name)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let opened = match directory.open_dir_nofollow(name) {
        Ok(opened) => opened,
        Err(_) => return Ok(false),
    };
    let opened_metadata = opened.dir_metadata()?;
    Ok(opened_metadata.is_dir()
        && (metadata.dev(), metadata.ino()) == (opened_metadata.dev(), opened_metadata.ino()))
}

fn is_safe_legacy_paper_repository_lock(
    directory: &Dir,
    name: &OsStr,
) -> Result<bool, ParquetStoreError> {
    if name != LEGACY_PAPER_REPOSITORY_LOCK {
        return Ok(false);
    }
    let lock = open_root_control_file(directory, LEGACY_PAPER_REPOSITORY_LOCK, 1)?
        .ok_or(ParquetStoreError::RootCatalogMismatch)?;
    if lock.metadata()?.len() != 0 {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    validate_root_control_file(directory, LEGACY_PAPER_REPOSITORY_LOCK, &lock, 1)?;
    Ok(true)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the exact pending/final protocol keeps both durability checkpoints explicit"
)]
fn publish_or_recover_exact_record(
    directory: &Dir,
    root: &ArtifactRoot,
    final_name: &str,
    pending_name: &str,
    expected: &[u8],
    prepared_checkpoint: RootBindingCheckpointInternal,
    durable_checkpoint: RootBindingCheckpointInternal,
    checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<File, ParquetStoreError> {
    if root_control_exists(directory, final_name)? {
        return reconcile_exact_committed_record(directory, final_name, pending_name, expected);
    }
    let pending = match open_exact_root_control_file(directory, pending_name, expected, 1)? {
        Some(file) => file,
        None => create_root_control_file(directory, pending_name, expected)?,
    };
    checkpoint(prepared_checkpoint)?;
    publish_exact_prepared_record(
        directory,
        root,
        final_name,
        pending_name,
        expected,
        pending,
        durable_checkpoint,
        checkpoint,
    )
}

fn open_exact_root_control_file(
    directory: &Dir,
    name: &str,
    expected: &[u8],
    links: u64,
) -> Result<Option<File>, ParquetStoreError> {
    let Some(mut file) = open_root_control_file(directory, name, links)? else {
        return Ok(None);
    };
    file.seek(SeekFrom::Start(0))?;
    let limit = u64::try_from(expected.len())
        .map_err(|_| ParquetStoreError::SizeOverflow)?
        .checked_add(1)
        .ok_or(ParquetStoreError::SizeOverflow)?;
    let mut observed = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(limit)
        .read_to_end(&mut observed)?;
    if observed != expected {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    validate_root_control_file(directory, name, &file, links)?;
    Ok(Some(file))
}

#[cfg(unix)]
fn reconcile_exact_committed_record(
    directory: &Dir,
    final_name: &str,
    pending_name: &str,
    expected: &[u8],
) -> Result<File, ParquetStoreError> {
    if !root_control_exists(directory, pending_name)? {
        return open_exact_root_control_file(directory, final_name, expected, 1)?
            .ok_or(ParquetStoreError::RootCatalogMismatch);
    }
    let pending = open_exact_root_control_file(directory, pending_name, expected, 2)?
        .ok_or(ParquetStoreError::RootCatalogMismatch)?;
    let committed = open_exact_root_control_file(directory, final_name, expected, 2)?
        .ok_or(ParquetStoreError::RootCatalogMismatch)?;
    if !same_exact_opened_file(&pending, &committed)? {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    drop(pending);
    drop(committed);
    directory.remove_file(pending_name)?;
    sync_directory(directory, ".")?;
    open_exact_root_control_file(directory, final_name, expected, 1)?
        .ok_or(ParquetStoreError::RootCatalogMismatch)
}

#[cfg(windows)]
fn reconcile_exact_committed_record(
    directory: &Dir,
    final_name: &str,
    pending_name: &str,
    expected: &[u8],
) -> Result<File, ParquetStoreError> {
    if root_control_exists(directory, pending_name)? {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    open_exact_root_control_file(directory, final_name, expected, 1)?
        .ok_or(ParquetStoreError::RootCatalogMismatch)
}

#[cfg(not(any(unix, windows)))]
fn reconcile_exact_committed_record(
    _directory: &Dir,
    _final_name: &str,
    _pending_name: &str,
    _expected: &[u8],
) -> Result<File, ParquetStoreError> {
    Err(unsupported_root_authority_durability().into())
}

#[cfg(unix)]
#[allow(
    clippy::too_many_arguments,
    reason = "the no-replace hard-link publication exposes one exact durability checkpoint"
)]
fn publish_exact_prepared_record(
    directory: &Dir,
    _root: &ArtifactRoot,
    final_name: &str,
    pending_name: &str,
    expected: &[u8],
    pending: File,
    durable_checkpoint: RootBindingCheckpointInternal,
    checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<File, ParquetStoreError> {
    directory.hard_link(pending_name, directory, final_name)?;
    sync_directory(directory, ".")?;
    validate_root_control_file(directory, pending_name, &pending, 2)?;
    let committed = open_exact_root_control_file(directory, final_name, expected, 2)?
        .ok_or(ParquetStoreError::RootCatalogMismatch)?;
    if !same_exact_opened_file(&pending, &committed)? {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    checkpoint(durable_checkpoint)?;
    drop(pending);
    drop(committed);
    directory.remove_file(pending_name)?;
    sync_directory(directory, ".")?;
    open_exact_root_control_file(directory, final_name, expected, 1)?
        .ok_or(ParquetStoreError::RootCatalogMismatch)
}

#[cfg(unix)]
fn same_exact_opened_file(first: &File, second: &File) -> Result<bool, ParquetStoreError> {
    use cap_fs_ext::MetadataExt as _;

    let first = first.metadata()?;
    let second = second.metadata()?;
    Ok((first.dev(), first.ino()) == (second.dev(), second.ino()))
}

#[cfg(windows)]
#[allow(
    clippy::too_many_arguments,
    reason = "the exact Windows move validates both retained endpoints around publication"
)]
fn publish_exact_prepared_record(
    directory: &Dir,
    root: &ArtifactRoot,
    final_name: &str,
    pending_name: &str,
    expected: &[u8],
    pending: File,
    durable_checkpoint: RootBindingCheckpointInternal,
    checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<File, ParquetStoreError> {
    validate_root_control_file(directory, pending_name, &pending, 1)?;
    drop(pending);
    if !valid_v2_root_control_name(final_name) || !valid_v2_root_control_name(pending_name) {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    let retained_before = root_endpoint_identity(directory, root.root())?;
    let source = root.root().join(pending_name);
    let destination = root.root().join(final_name);
    let publication = atomicwrites::move_atomic(&source, &destination);
    if root_endpoint_identity(directory, root.root())? != retained_before {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    match publication {
        Ok(()) => {
            let committed = open_exact_root_control_file(directory, final_name, expected, 1)?
                .ok_or(ParquetStoreError::RootCatalogMismatch)?;
            if root_control_exists(directory, pending_name)? {
                return Err(ParquetStoreError::RootCatalogMismatch);
            }
            checkpoint(durable_checkpoint)?;
            Ok(committed)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(windows)]
fn valid_v2_root_control_name(name: &str) -> bool {
    if matches!(name, ROOT_IDENTITY_MARKER_V2 | ROOT_IDENTITY_PENDING_V2) {
        return true;
    }
    let Some(generation) = name.strip_prefix(".analytical-root-catalog.binding.") else {
        return false;
    };
    let generation = generation.strip_suffix(".pending").unwrap_or(generation);
    generation.len() == 16 && generation.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(not(any(unix, windows)))]
#[allow(
    clippy::too_many_arguments,
    reason = "unsupported platforms retain the exact shared publication contract"
)]
fn publish_exact_prepared_record(
    _directory: &Dir,
    _root: &ArtifactRoot,
    _final_name: &str,
    _pending_name: &str,
    _expected: &[u8],
    _pending: File,
    _durable_checkpoint: RootBindingCheckpointInternal,
    _checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<File, ParquetStoreError> {
    Err(unsupported_root_authority_durability().into())
}

impl std::fmt::Debug for RootAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RootAuthority")
            .field("identity", &self.identity)
            .field("publication", &self.publication)
            .field("lock", &"[LOCKED ROOT CAPABILITY]")
            .finish()
    }
}

#[derive(Debug, Eq, PartialEq)]
struct RootRecord {
    bytes: Vec<u8>,
    catalog_binding: [u8; 32],
    payload: [u8; 32],
}

struct OpenedRootRecord {
    file: File,
    record: RootRecord,
}

#[allow(
    clippy::too_many_arguments,
    reason = "first-bind ordering keeps each independently durable record explicit"
)]
#[cfg(test)]
pub(super) fn acquire_root_authority(
    directory: &Dir,
    root: &ArtifactRoot,
    catalog_binding: [u8; 32],
    catalog_root_identity: Option<[u8; 32]>,
    checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<RootAuthority, ParquetStoreError> {
    require_supported_root_authority_platform()?;
    let root_path = root.root();
    let registry = acquire_process_registry(root_path)?;
    let lock = open_or_create_root_authority_lock(directory)?;
    match lock.try_lock_exclusive() {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            return Err(ParquetStoreError::RootAuthorityAlreadyOwned);
        }
        Err(error) => return Err(error.into()),
    }
    validate_root_control_file(directory, ROOT_AUTHORITY_LOCK, &lock, 1)?;

    let stable_root = load_or_create_root_marker(
        directory,
        root,
        catalog_binding,
        catalog_root_identity,
        checkpoint,
    )?;
    load_or_create_root_binding(
        directory,
        root,
        catalog_binding,
        stable_root,
        catalog_root_identity,
        checkpoint,
    )?;
    Ok(RootAuthority {
        identity: ArtifactRootIdentity {
            path: root_path.to_path_buf(),
            stable_root,
            catalog_binding,
        },
        publication: PublicationCoordinator::default(),
        _lock: lock,
        _registry: registry,
    })
}

fn open_or_create_root_authority_lock(directory: &Dir) -> Result<File, ParquetStoreError> {
    if let Some(existing) = root_control_metadata(directory, ROOT_AUTHORITY_LOCK)? {
        validate_named_root_control_metadata(&existing, 1)?;
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    options.follow(FollowSymlinks::No);
    configure_private_root_control(&mut options);
    let lock = match directory.open_with(ROOT_AUTHORITY_LOCK, &options) {
        Ok(lock) => lock.into_std(),
        Err(error) => {
            return Err(classify_root_control_open_error(
                directory,
                ROOT_AUTHORITY_LOCK,
                error,
            ));
        }
    };
    validate_root_control_file(directory, ROOT_AUTHORITY_LOCK, &lock, 1)?;
    Ok(lock)
}

fn classify_root_control_open_error(
    directory: &Dir,
    name: &str,
    open_error: std::io::Error,
) -> ParquetStoreError {
    match root_control_metadata(directory, name) {
        Ok(Some(metadata)) if validate_named_root_control_metadata(&metadata, 1).is_err() => {
            ParquetStoreError::RootCatalogMismatch
        }
        Ok(_) => open_error.into(),
        Err(error) => error,
    }
}

fn acquire_process_registry(root_path: &Path) -> Result<RootRegistryGuard, ParquetStoreError> {
    let mut roots = OPEN_ARTIFACT_ROOTS
        .get_or_init(|| Mutex::new(BTreeSet::new()))
        .lock()
        .map_err(|_| ParquetStoreError::RootAuthorityRegistryUnavailable)?;
    if !roots.insert(root_path.to_path_buf()) {
        return Err(ParquetStoreError::RootAuthorityAlreadyOwned);
    }
    Ok(RootRegistryGuard {
        path: root_path.to_path_buf(),
    })
}

#[cfg(test)]
fn load_or_create_root_marker(
    directory: &Dir,
    root: &ArtifactRoot,
    catalog_binding: [u8; 32],
    catalog_root_identity: Option<[u8; 32]>,
    checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<[u8; 32], ParquetStoreError> {
    let root_path = root.root();
    let later_state_exists = root_control_exists(directory, ROOT_CATALOG_BINDING)?
        || root_control_exists(directory, ROOT_CATALOG_BINDING_PENDING)?;
    let opened = recover_or_publish_root_record(
        directory,
        root,
        ROOT_IDENTITY_MARKER,
        ROOT_IDENTITY_PENDING,
        ROOT_MARKER_MAGIC,
        catalog_binding,
        None,
        || {
            let mut nonce = [0_u8; 32];
            nonce[..16].copy_from_slice(Uuid::new_v4().as_bytes());
            nonce[16..].copy_from_slice(Uuid::new_v4().as_bytes());
            nonce
        },
        catalog_root_identity.is_some(),
        later_state_exists,
        RootBindingCheckpointInternal::MarkerPreparedDurable,
        RootBindingCheckpointInternal::MarkerDurable,
        checkpoint,
    )?;
    let stable_root = root_identity(directory, root_path, &opened.file, &opened.record.bytes)?;
    validate_root_control_file(directory, ROOT_IDENTITY_MARKER, &opened.file, 1)?;
    if catalog_root_identity.is_some_and(|expected| expected != stable_root) {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    Ok(stable_root)
}

#[cfg(test)]
fn load_or_create_root_binding(
    directory: &Dir,
    root: &ArtifactRoot,
    catalog_binding: [u8; 32],
    stable_root: [u8; 32],
    catalog_root_identity: Option<[u8; 32]>,
    checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<(), ParquetStoreError> {
    recover_or_publish_root_record(
        directory,
        root,
        ROOT_CATALOG_BINDING,
        ROOT_CATALOG_BINDING_PENDING,
        ROOT_BINDING_MAGIC,
        catalog_binding,
        Some(stable_root),
        || stable_root,
        catalog_root_identity.is_some(),
        false,
        RootBindingCheckpointInternal::RootBindingPreparedDurable,
        RootBindingCheckpointInternal::RootBindingDurable,
        checkpoint,
    )?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the pending/final state machine keeps each crash boundary explicit"
)]
#[cfg(test)]
fn recover_or_publish_root_record<F>(
    directory: &Dir,
    root: &ArtifactRoot,
    final_name: &str,
    pending_name: &str,
    magic: &[u8; 8],
    catalog_binding: [u8; 32],
    expected_payload: Option<[u8; 32]>,
    create_payload: F,
    catalog_committed: bool,
    later_state_exists: bool,
    prepared_checkpoint: RootBindingCheckpointInternal,
    durable_checkpoint: RootBindingCheckpointInternal,
    checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<OpenedRootRecord, ParquetStoreError>
where
    F: FnOnce() -> [u8; 32],
{
    if root_control_exists(directory, final_name)? {
        return recover_committed_root_record(
            directory,
            root,
            final_name,
            pending_name,
            magic,
            catalog_binding,
            expected_payload,
            catalog_committed,
            durable_checkpoint,
            checkpoint,
        );
    }
    if catalog_committed {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }

    let pending = match open_root_control_file(directory, pending_name, 1)? {
        Some(mut pending) => match read_root_record(&mut pending, magic)? {
            Some(record) => {
                verify_root_record(&record, catalog_binding, expected_payload)?;
                OpenedRootRecord {
                    file: pending,
                    record,
                }
            }
            None if !later_state_exists => {
                drop(pending);
                directory.remove_file(pending_name)?;
                sync_directory(directory, ".")?;
                create_pending_record(
                    directory,
                    pending_name,
                    magic,
                    catalog_binding,
                    create_payload(),
                )?
            }
            None => return Err(ParquetStoreError::RootCatalogMismatch),
        },
        None => create_pending_record(
            directory,
            pending_name,
            magic,
            catalog_binding,
            create_payload(),
        )?,
    };
    checkpoint(prepared_checkpoint)?;
    publish_prepared_root_record(
        directory,
        root,
        final_name,
        pending_name,
        magic,
        catalog_binding,
        expected_payload,
        pending,
        durable_checkpoint,
        checkpoint,
    )
}

#[cfg(not(any(unix, windows)))]
#[allow(
    clippy::too_many_arguments,
    reason = "unsupported-target recovery retains the shared explicit interface"
)]
fn recover_committed_root_record(
    _directory: &Dir,
    _root: &ArtifactRoot,
    _final_name: &str,
    _pending_name: &str,
    _magic: &[u8; 8],
    _catalog_binding: [u8; 32],
    _expected_payload: Option<[u8; 32]>,
    _catalog_committed: bool,
    _durable_checkpoint: RootBindingCheckpointInternal,
    _checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<OpenedRootRecord, ParquetStoreError> {
    Err(unsupported_root_authority_durability().into())
}

#[cfg(not(any(unix, windows)))]
#[allow(
    clippy::too_many_arguments,
    reason = "unsupported-target publication retains the shared explicit interface"
)]
fn publish_prepared_root_record(
    _directory: &Dir,
    _root: &ArtifactRoot,
    _final_name: &str,
    _pending_name: &str,
    _magic: &[u8; 8],
    _catalog_binding: [u8; 32],
    _expected_payload: Option<[u8; 32]>,
    _pending: OpenedRootRecord,
    _durable_checkpoint: RootBindingCheckpointInternal,
    _checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<OpenedRootRecord, ParquetStoreError> {
    Err(unsupported_root_authority_durability().into())
}

#[cfg(test)]
fn create_pending_record(
    directory: &Dir,
    name: &str,
    magic: &[u8; 8],
    catalog_binding: [u8; 32],
    payload: [u8; 32],
) -> Result<OpenedRootRecord, ParquetStoreError> {
    let record = encode_root_record(magic, catalog_binding, payload);
    let mut file = create_root_control_file(directory, name, &record)?;
    let observed =
        read_root_record(&mut file, magic)?.ok_or(ParquetStoreError::RootCatalogMismatch)?;
    validate_root_control_file(directory, name, &file, 1)?;
    verify_root_record(&observed, catalog_binding, Some(payload))?;
    Ok(OpenedRootRecord {
        file,
        record: observed,
    })
}

fn open_committed_record(
    directory: &Dir,
    name: &str,
    magic: &[u8; 8],
    catalog_binding: [u8; 32],
    expected_payload: Option<[u8; 32]>,
    links: u64,
) -> Result<OpenedRootRecord, ParquetStoreError> {
    let mut file = open_root_control_file(directory, name, links)?
        .ok_or(ParquetStoreError::RootCatalogMismatch)?;
    let record =
        read_root_record(&mut file, magic)?.ok_or(ParquetStoreError::RootCatalogMismatch)?;
    validate_root_control_file(directory, name, &file, links)?;
    verify_root_record(&record, catalog_binding, expected_payload)?;
    Ok(OpenedRootRecord { file, record })
}

fn verify_root_record(
    record: &RootRecord,
    catalog_binding: [u8; 32],
    expected_payload: Option<[u8; 32]>,
) -> Result<(), ParquetStoreError> {
    if record.catalog_binding != catalog_binding
        || expected_payload.is_some_and(|expected| expected != record.payload)
    {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    Ok(())
}

fn root_control_exists(directory: &Dir, name: &str) -> Result<bool, ParquetStoreError> {
    Ok(root_control_metadata(directory, name)?.is_some())
}

fn root_control_metadata(
    directory: &Dir,
    name: &str,
) -> Result<Option<cap_std::fs::Metadata>, ParquetStoreError> {
    match directory.symlink_metadata(name) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn open_root_control_file(
    directory: &Dir,
    name: &str,
    links: u64,
) -> Result<Option<File>, ParquetStoreError> {
    let Some(named) = root_control_metadata(directory, name)? else {
        return Ok(None);
    };
    validate_named_root_control_metadata(&named, links)?;
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    configure_private_root_control(&mut options);
    let file = directory.open_with(name, &options)?.into_std();
    validate_root_control_file(directory, name, &file, links)?;
    Ok(Some(file))
}

fn create_root_control_file(
    directory: &Dir,
    name: &str,
    record: &[u8],
) -> Result<File, ParquetStoreError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    options.follow(FollowSymlinks::No);
    configure_private_root_control(&mut options);
    let mut file = directory.open_with(name, &options)?.into_std();
    validate_root_control_file(directory, name, &file, 1)?;
    file.write_all(record)?;
    file.sync_all()?;
    sync_directory(directory, ".")?;
    validate_root_control_file(directory, name, &file, 1)?;
    Ok(file)
}

fn validate_root_control_file(
    directory: &Dir,
    name: &str,
    file: &File,
    links: u64,
) -> Result<(), ParquetStoreError> {
    use cap_fs_ext::MetadataExt as _;

    let opened = cap_std::fs::File::from_std(file.try_clone()?).metadata()?;
    let named = directory.symlink_metadata(name)?;
    validate_named_root_control_metadata(&named, links)?;
    if !opened.is_file()
        || opened.nlink() != links
        || (opened.dev(), opened.ino()) != (named.dev(), named.ino())
    {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    Ok(())
}

fn validate_named_root_control_metadata(
    metadata: &cap_std::fs::Metadata,
    links: u64,
) -> Result<(), ParquetStoreError> {
    use cap_fs_ext::MetadataExt as _;

    if !metadata.is_file() || metadata.nlink() != links || !private_root_control_metadata(metadata)
    {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    Ok(())
}

#[cfg(test)]
fn encode_root_record(magic: &[u8; 8], catalog_binding: [u8; 32], payload: [u8; 32]) -> Vec<u8> {
    let mut record = Vec::with_capacity(ROOT_RECORD_BYTES);
    record.extend_from_slice(magic);
    record.extend_from_slice(&ROOT_RECORD_VERSION.to_be_bytes());
    record.extend_from_slice(&catalog_binding);
    record.extend_from_slice(&payload);
    let mut checksum = Sha256::new();
    checksum.update(b"market-squawk/analytical-root-record/v1");
    checksum.update(&record);
    record.extend_from_slice(&checksum.finalize());
    record
}

fn read_root_record(
    file: &mut File,
    magic: &[u8; 8],
) -> Result<Option<RootRecord>, ParquetStoreError> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::with_capacity(ROOT_RECORD_BYTES + 1);
    file.take(u64::try_from(ROOT_RECORD_BYTES + 1).map_err(|_| ParquetStoreError::SizeOverflow)?)
        .read_to_end(&mut bytes)?;
    if bytes.len() != ROOT_RECORD_BYTES {
        return Ok(None);
    }
    let checksum_start = ROOT_RECORD_BYTES - 32;
    let mut checksum = Sha256::new();
    checksum.update(b"market-squawk/analytical-root-record/v1");
    checksum.update(&bytes[..checksum_start]);
    if bytes[checksum_start..] != checksum.finalize()[..] {
        return Ok(None);
    }
    if bytes.get(..8) != Some(magic.as_slice())
        || bytes.get(8..10) != Some(ROOT_RECORD_VERSION.to_be_bytes().as_slice())
    {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    let catalog_binding = bytes[10..42]
        .try_into()
        .map_err(|_| ParquetStoreError::RootCatalogMismatch)?;
    let payload = bytes[42..74]
        .try_into()
        .map_err(|_| ParquetStoreError::RootCatalogMismatch)?;
    Ok(Some(RootRecord {
        bytes,
        catalog_binding,
        payload,
    }))
}

fn root_identity(
    directory: &Dir,
    root_path: &Path,
    marker: &File,
    marker_record: &[u8],
) -> Result<[u8; 32], ParquetStoreError> {
    use cap_fs_ext::MetadataExt as _;

    let root = directory.dir_metadata()?;
    let marker = cap_std::fs::File::from_std(marker.try_clone()?).metadata()?;
    let path = root_path.as_os_str().as_encoded_bytes();
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/analytical-root-identity/v1");
    digest.update(
        u64::try_from(path.len())
            .map_err(|_| ParquetStoreError::SizeOverflow)?
            .to_be_bytes(),
    );
    digest.update(path);
    digest.update(root.dev().to_be_bytes());
    digest.update(root.ino().to_be_bytes());
    digest.update(marker.dev().to_be_bytes());
    digest.update(marker.ino().to_be_bytes());
    digest.update(
        u64::try_from(marker_record.len())
            .map_err(|_| ParquetStoreError::SizeOverflow)?
            .to_be_bytes(),
    );
    digest.update(marker_record);
    Ok(digest.finalize().into())
}

#[cfg(any(unix, windows))]
const fn require_supported_root_authority_platform() -> Result<(), ParquetStoreError> {
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn require_supported_root_authority_platform() -> Result<(), ParquetStoreError> {
    Err(unsupported_root_authority_durability().into())
}

#[cfg(not(any(unix, windows)))]
fn unsupported_root_authority_durability() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "analytical root-authority durability is unsupported",
    )
}

#[cfg(not(any(unix, windows)))]
fn configure_private_root_control(_options: &mut OpenOptions) {}

#[cfg(not(any(unix, windows)))]
fn private_root_control_metadata(_metadata: &cap_std::fs::Metadata) -> bool {
    false
}
