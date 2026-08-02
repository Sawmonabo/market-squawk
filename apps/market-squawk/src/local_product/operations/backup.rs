//! Concrete installed-product backup materialization and lifecycle authority.

use std::{
    collections::BTreeMap,
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use cap_fs_ext::{DirExt as _, FollowSymlinks, MetadataExt as _, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, Metadata, OpenOptions};
use fs2::FileExt as _;
use market_squawk_data::{
    AnalyticalBackupLimits, AnalyticalBackupLocation, AnalyticalBackupService,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::{ControlRoot, LocalPaths};
use market_squawk_runtime::InstallationId;
use market_squawk_services::ServiceError;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tokio::sync::Mutex as TokioMutex;
use tokio_util::sync::CancellationToken;

use crate::{
    application::{
        backup::{
            BackupBundleRemover, BackupRetentionApproval, ProductBackupError,
            ProductBackupInventory, ProductBackupManifest, ProductBackupOwnership,
            ProductBackupService, ProductBackupSnapshotAuthority,
        },
        lifecycle::WorkspaceRuntimeIdentity,
        operations::{ManagedBackupOperations, PreparedOperation},
    },
    jobs::{
        BackupJobAction, BackupJobCommand, BackupJobRunner, LifecycleJobExecutionError,
        LifecycleJobPublication, LifecycleJobPublicationError,
    },
};

const REPOSITORY_DIRECTORY: &str = "product-backups";
const REPOSITORY_LOCK_FILE: &str = ".owner.lock";
const MANIFEST_FILE: &str = "manifest.json";
const STAGING_PREFIX: &str = "stage-";
const MAXIMUM_REPOSITORY_ENTRIES: usize = 256;
const MAXIMUM_MANIFEST_BYTES: usize = 1024 * 1024;
const MAXIMUM_PENDING_OPERATIONS: usize = 256;

/// Sealed composition binding for the authority that captures non-analytical product state.
///
/// The binding deliberately contains no path or pre-selected file. Its authority must issue all
/// required components for the request's exact snapshot and revalidate their producer leases after
/// materialization.
pub struct ManagedBackupComponentSource {
    authority: Arc<dyn ProductBackupSnapshotAuthority>,
}

impl ManagedBackupComponentSource {
    /// Retains the sole component-snapshot authority for installed backup composition.
    #[must_use]
    pub fn new(authority: Arc<dyn ProductBackupSnapshotAuthority>) -> Self {
        Self { authority }
    }
}

impl std::fmt::Debug for ManagedBackupComponentSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManagedBackupComponentSource")
            .field("authority", &"[SEALED PRODUCT SNAPSHOT AUTHORITY]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

#[cfg(unix)]
fn private_permissions(metadata: &Metadata) -> bool {
    cap_fs_ext::OsMetadataExt::mode(metadata) & 0o077 == 0
}

#[cfg(windows)]
fn private_permissions(metadata: &Metadata) -> bool {
    use cap_std::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0
}

#[cfg(not(any(unix, windows)))]
fn private_permissions(_metadata: &Metadata) -> bool {
    false
}

/// Exclusive capability-confined repository for complete staged and published backup bundles.
pub struct ManagedBackupRepository {
    root: Dir,
    display_root: PathBuf,
    _owner_lock: std::fs::File,
    mutation: TokioMutex<()>,
}

impl ManagedBackupRepository {
    /// Opens the sole installed repository below the already prepared control-root capability.
    pub fn try_open(control_root: &ControlRoot) -> Result<Self, ServiceError> {
        let control = control_root
            .try_clone_directory()
            .map_err(|_| ServiceError::Unavailable)?;
        control
            .create_dir_all(REPOSITORY_DIRECTORY)
            .map_err(|_| ServiceError::Unavailable)?;
        let root = control
            .open_dir_nofollow(REPOSITORY_DIRECTORY)
            .map_err(|_| ServiceError::Unavailable)?;
        let mut lock_options = OpenOptions::new();
        lock_options.read(true).write(true).create(true);
        lock_options.follow(FollowSymlinks::No);
        configure_private_creation(&mut lock_options);
        let owner_lock = root
            .open_with(REPOSITORY_LOCK_FILE, &lock_options)
            .map(cap_std::fs::File::into_std)
            .map_err(|_| ServiceError::Unavailable)?;
        owner_lock
            .try_lock_exclusive()
            .map_err(|_| ServiceError::Unavailable)?;
        synchronize_directory(&control, Path::new(REPOSITORY_DIRECTORY))
            .map_err(|_| ServiceError::Unavailable)?;
        Ok(Self {
            root,
            display_root: control_root.root().join(REPOSITORY_DIRECTORY),
            _owner_lock: owner_lock,
            mutation: TokioMutex::new(()),
        })
    }

    fn create_staging(&self) -> Result<StagedBundle, RepositoryError> {
        let mut nonce = [0_u8; 16];
        getrandom::fill(&mut nonce).map_err(|_| RepositoryError::Unavailable)?;
        let name = format!("{STAGING_PREFIX}{}", hex_bytes(&nonce));
        self.root
            .create_dir(&name)
            .map_err(|_| RepositoryError::Unavailable)?;
        synchronize_directory(&self.root, Path::new("."))?;
        let paths = LocalPaths::prepare(self.display_root.join(&name))
            .map_err(|_| RepositoryError::Unavailable)?;
        Ok(StagedBundle { name, paths })
    }

    fn location_for_name(&self, name: &str) -> Result<AnalyticalBackupLocation, RepositoryError> {
        let paths = LocalPaths::open_existing(self.display_root.join(name))
            .map_err(|_| RepositoryError::Unavailable)?;
        AnalyticalBackupLocation::try_new(
            paths
                .catalog()
                .map_err(|_| RepositoryError::Unavailable)?
                .clone(),
            paths
                .artifacts()
                .map_err(|_| RepositoryError::Unavailable)?
                .clone(),
        )
        .map_err(|_| RepositoryError::Unavailable)
    }

    fn location_for_id(
        &self,
        backup_id: [u8; 32],
    ) -> Result<AnalyticalBackupLocation, RepositoryError> {
        self.location_for_name(&hex_bytes(&backup_id))
    }

    fn load_manifest(&self, name: &str) -> Result<Option<ProductBackupManifest>, RepositoryError> {
        let bundle = self
            .root
            .open_dir_nofollow(name)
            .map_err(|_| RepositoryError::Unavailable)?;
        let encoded = match read_bounded_private_file(&bundle, Path::new(MANIFEST_FILE)) {
            Ok(encoded) => encoded,
            Err(RepositoryError::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        let manifest = serde_json::from_slice::<ProductBackupManifest>(&encoded)
            .map_err(|_| RepositoryError::Corrupt)?;
        manifest.verify().map_err(|_| RepositoryError::Corrupt)?;
        Ok(Some(manifest))
    }

    fn manifest_for_id(
        &self,
        backup_id: [u8; 32],
    ) -> Result<ProductBackupManifest, RepositoryError> {
        let name = hex_bytes(&backup_id);
        let manifest = self
            .load_manifest(&name)?
            .ok_or(RepositoryError::NotFound)?;
        if manifest.backup_id() != backup_id {
            return Err(RepositoryError::Corrupt);
        }
        Ok(manifest)
    }

    fn write_staged_manifest(
        &self,
        staging_name: &str,
        manifest: &ProductBackupManifest,
    ) -> Result<Vec<u8>, RepositoryError> {
        let encoded = serde_json::to_vec(manifest).map_err(|_| RepositoryError::Corrupt)?;
        if encoded.is_empty() || encoded.len() > MAXIMUM_MANIFEST_BYTES {
            return Err(RepositoryError::Corrupt);
        }
        let staging = self
            .root
            .open_dir_nofollow(staging_name)
            .map_err(|_| RepositoryError::Unavailable)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let mut file = staging
            .open_with(MANIFEST_FILE, &options)
            .map_err(|_| RepositoryError::Unavailable)?;
        file.write_all(&encoded)
            .map_err(|_| RepositoryError::Unavailable)?;
        file.sync_all().map_err(|_| RepositoryError::Unavailable)?;
        drop(file);
        synchronize_directory(&staging, Path::new("."))?;
        Ok(encoded)
    }

    fn publish_staged(
        &self,
        staging_name: &str,
        manifest: &ProductBackupManifest,
    ) -> Result<(), RepositoryError> {
        let final_name = hex_bytes(&manifest.backup_id());
        match self.root.symlink_metadata(&final_name) {
            Ok(metadata) if metadata.is_dir() => {
                let retained = self
                    .load_manifest(&final_name)?
                    .ok_or(RepositoryError::Corrupt)?;
                if retained != *manifest {
                    return Err(RepositoryError::Conflict);
                }
                self.remove_directory(staging_name)?;
                return Ok(());
            }
            Ok(_) => return Err(RepositoryError::Conflict),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(RepositoryError::Unavailable),
        }
        self.root
            .rename(staging_name, &self.root, &final_name)
            .map_err(|_| RepositoryError::Indeterminate)?;
        synchronize_directory(&self.root, Path::new("."))?;
        let retained = self
            .load_manifest(&final_name)?
            .ok_or(RepositoryError::Indeterminate)?;
        if retained != *manifest {
            return Err(RepositoryError::Indeterminate);
        }
        Ok(())
    }

    fn remove_directory(&self, name: &str) -> Result<(), RepositoryError> {
        match self.root.symlink_metadata(name) {
            Ok(metadata) if metadata.is_dir() => self
                .root
                .remove_dir_all(name)
                .map_err(|_| RepositoryError::Unavailable)?,
            Ok(_) => return Err(RepositoryError::Corrupt),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(RepositoryError::Unavailable),
        }
        synchronize_directory(&self.root, Path::new("."))
    }

    fn repository_entries(&self) -> Result<Vec<String>, RepositoryError> {
        let mut entries = Vec::new();
        for entry in self
            .root
            .entries()
            .map_err(|_| RepositoryError::Unavailable)?
        {
            let entry = entry.map_err(|_| RepositoryError::Unavailable)?;
            let name = entry.file_name();
            if name == REPOSITORY_LOCK_FILE {
                continue;
            }
            let name = name.into_string().map_err(|_| RepositoryError::Corrupt)?;
            entries
                .try_reserve(1)
                .map_err(|_| RepositoryError::Capacity)?;
            entries.push(name);
            if entries.len() > MAXIMUM_REPOSITORY_ENTRIES {
                return Err(RepositoryError::Capacity);
            }
        }
        entries.sort();
        Ok(entries)
    }
}

impl std::fmt::Debug for ManagedBackupRepository {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ManagedBackupRepository([EXCLUSIVE CONTROL-ROOT AUTHORITY])")
    }
}

#[async_trait]
impl BackupBundleRemover for ManagedBackupRepository {
    async fn remove_exact(&self, backup_id: [u8; 32]) -> Result<(), ProductBackupError> {
        let _mutation = self.mutation.lock().await;
        self.remove_directory(&hex_bytes(&backup_id))
            .map_err(|_| ProductBackupError::InventoryPersistence)
    }
}

struct StagedBundle {
    name: String,
    paths: LocalPaths,
}

impl StagedBundle {
    fn location(&self) -> Result<AnalyticalBackupLocation, RepositoryError> {
        AnalyticalBackupLocation::try_new(
            self.paths
                .catalog()
                .map_err(|_| RepositoryError::Unavailable)?
                .clone(),
            self.paths
                .artifacts()
                .map_err(|_| RepositoryError::Unavailable)?
                .clone(),
        )
        .map_err(|_| RepositoryError::Unavailable)
    }
}

#[derive(Clone, Copy, Debug)]
enum RepositoryError {
    NotFound,
    Corrupt,
    Conflict,
    Capacity,
    Unavailable,
    Indeterminate,
}

impl From<std::io::Error> for RepositoryError {
    fn from(_error: std::io::Error) -> Self {
        Self::Unavailable
    }
}

#[derive(Debug)]
enum RetainedBackupPlan {
    Create {
        active: WorkspaceRuntimeIdentity,
        cutoff: Timestamp,
    },
    Verify {
        manifest: ProductBackupManifest,
    },
    Retention {
        approval: BackupRetentionApproval,
    },
}

impl RetainedBackupPlan {
    const fn action(&self) -> BackupJobAction {
        match self {
            Self::Create { .. } => BackupJobAction::Create,
            Self::Verify { .. } => BackupJobAction::Verify,
            Self::Retention { .. } => BackupJobAction::EnforceRetention,
        }
    }
}

#[derive(Debug)]
struct RetainedOperation {
    evidence_digest: EvidenceDigest,
    plan: RetainedBackupPlan,
}

/// Sole installed implementation of backup prepare, revoke, execute, and recovery authority.
pub struct InstalledManagedBackupOperations {
    installation_id: InstallationId,
    service: Arc<ProductBackupService>,
    inventory: Arc<ProductBackupInventory>,
    repository: Arc<ManagedBackupRepository>,
    limits: AnalyticalBackupLimits,
    pending: Mutex<BTreeMap<SourceIdentifier, RetainedOperation>>,
    maximum_pending: usize,
    recovery: TokioMutex<()>,
}

impl InstalledManagedBackupOperations {
    /// Constructs the complete installed backup authority from existing analytical, inventory,
    /// control-root, installation, and coherent component-snapshot authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "every backup authority and resource ceiling remains explicit at composition"
    )]
    pub fn try_new(
        analytical: Arc<AnalyticalBackupService>,
        inventory: Arc<ProductBackupInventory>,
        control_root: &ControlRoot,
        installation_id: InstallationId,
        component_source: ManagedBackupComponentSource,
        limits: AnalyticalBackupLimits,
        maximum_pending: usize,
    ) -> Result<Self, ServiceError> {
        if maximum_pending == 0 || maximum_pending > MAXIMUM_PENDING_OPERATIONS {
            return Err(ServiceError::InvalidRequest);
        }
        let service = Arc::new(ProductBackupService::new(
            analytical,
            component_source.authority,
        ));
        let repository = Arc::new(ManagedBackupRepository::try_open(control_root)?);
        Ok(Self {
            installation_id,
            service,
            inventory,
            repository,
            limits,
            pending: Mutex::new(BTreeMap::new()),
            maximum_pending,
            recovery: TokioMutex::new(()),
        })
    }

    /// Reconciles interrupted retention, complete staging bundles, and published manifests.
    ///
    /// Composition must await this method before publishing Operations inventory reads. Every
    /// execution also invokes it, so a missed startup call still fails closed before mutation.
    pub async fn recover(
        &self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), ServiceError> {
        let _recovery = self.recovery.lock().await;
        ensure_service_live(cancellation, deadline)?;
        self.inventory
            .recover_pending(self.repository.as_ref())
            .await
            .map_err(map_backup_service_error)?;
        let _repository = self.repository.mutation.lock().await;
        let entries = self
            .repository
            .repository_entries()
            .map_err(map_repository_service_error)?;
        for name in entries {
            ensure_service_live(cancellation, deadline)?;
            if let Some(staging_suffix) = name.strip_prefix(STAGING_PREFIX) {
                if staging_suffix.len() != 32
                    || !staging_suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
                {
                    return Err(ServiceError::Unavailable);
                }
                let Some(manifest) = self
                    .repository
                    .load_manifest(&name)
                    .map_err(map_repository_service_error)?
                else {
                    self.repository
                        .remove_directory(&name)
                        .map_err(map_repository_service_error)?;
                    continue;
                };
                let location = self
                    .repository
                    .location_for_name(&name)
                    .map_err(map_repository_service_error)?;
                ProductBackupService::open_verified(
                    location,
                    manifest.clone(),
                    self.limits,
                    cancellation,
                )
                .map_err(map_backup_service_error)?;
                self.repository
                    .publish_staged(&name, &manifest)
                    .map_err(map_repository_service_error)?;
                self.inventory
                    .register(manifest)
                    .await
                    .map_err(map_backup_service_error)?;
                continue;
            }
            let Some(backup_id) = parse_backup_directory_name(&name) else {
                return Err(ServiceError::Unavailable);
            };
            let manifest = self
                .repository
                .manifest_for_id(backup_id)
                .map_err(map_repository_service_error)?;
            let location = self
                .repository
                .location_for_id(backup_id)
                .map_err(map_repository_service_error)?;
            ProductBackupService::open_verified(
                location,
                manifest.clone(),
                self.limits,
                cancellation,
            )
            .map_err(map_backup_service_error)?;
            self.inventory
                .register(manifest)
                .await
                .map_err(map_backup_service_error)?;
        }
        ensure_service_live(cancellation, deadline)
    }

    fn retain_plan(
        &self,
        plan: RetainedBackupPlan,
        digest_payload: impl Serialize,
    ) -> Result<PreparedOperation, ServiceError> {
        let mut nonce = [0_u8; 16];
        getrandom::fill(&mut nonce).map_err(|_| ServiceError::Unavailable)?;
        let identity = SourceIdentifier::try_from(format!(
            "product-backup-{}-{}",
            action_name(plan.action()),
            hex_bytes(&nonce)
        ))
        .map_err(|_| ServiceError::Internal)?;
        let encoded = serde_json::to_vec(&(
            "market-squawk-installed-backup-operation-v1",
            action_name(plan.action()),
            &identity,
            digest_payload,
        ))
        .map_err(|_| ServiceError::Internal)?;
        let evidence_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(encoded).into());
        let mut pending = self.pending.lock().map_err(|_| ServiceError::Unavailable)?;
        if pending.len() >= self.maximum_pending || pending.contains_key(&identity) {
            return Err(ServiceError::ResourceExhausted);
        }
        pending.insert(
            identity.clone(),
            RetainedOperation {
                evidence_digest,
                plan,
            },
        );
        PreparedOperation::try_new(identity, evidence_digest)
    }

    fn take_plan(&self, command: &BackupJobCommand) -> Result<RetainedBackupPlan, ServiceError> {
        let mut pending = self.pending.lock().map_err(|_| ServiceError::Unavailable)?;
        let retained = pending
            .get(command.identity())
            .ok_or(ServiceError::NotFound)?;
        if retained.evidence_digest != command.evidence_digest()
            || retained.plan.action() != command.action()
        {
            return Err(ServiceError::Unauthorized);
        }
        pending
            .remove(command.identity())
            .map(|retained| retained.plan)
            .ok_or(ServiceError::NotFound)
    }

    async fn execute_create(
        &self,
        active: WorkspaceRuntimeIdentity,
        cutoff: Timestamp,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        ensure_execution_live(&cancellation, deadline)?;
        let _repository = self.repository.mutation.lock().await;
        let staging = self
            .repository
            .create_staging()
            .map_err(|_| backup_failure("backup-staging-failed", true))?;
        let location = match staging.location() {
            Ok(location) => location,
            Err(_) => {
                let _ignored = self.repository.remove_directory(&staging.name);
                return Err(backup_failure("backup-staging-failed", true));
            }
        };
        let ownership = ProductBackupOwnership::new(self.installation_id, active.workspace_id());
        let operation_cancellation = cancellation.child_token();
        let created = run_create_until_deadline(
            self.service.as_ref(),
            location,
            cutoff,
            self.limits,
            ownership,
            &cancellation,
            &operation_cancellation,
            deadline,
        )
        .await;
        let verified = match created {
            Ok(verified) => verified,
            Err(error) => {
                let _ignored = self.repository.remove_directory(&staging.name);
                return Err(error);
            }
        };
        let manifest = verified.manifest().clone();
        let manifest_bytes = match self
            .repository
            .write_staged_manifest(&staging.name, &manifest)
        {
            Ok(encoded) => encoded,
            Err(_) => {
                let _ignored = self.repository.remove_directory(&staging.name);
                return Err(backup_failure("backup-manifest-publication-failed", true));
            }
        };
        if let Err(error) = ensure_execution_live(&cancellation, deadline) {
            let _ignored = self.repository.remove_directory(&staging.name);
            return Err(error);
        }
        let result = result_for_manifest(&manifest, &manifest_bytes)?;
        if let Err(error) = publication.prepare_and_claim(result) {
            let _ignored = self.repository.remove_directory(&staging.name);
            return Err(error.into());
        }
        self.repository
            .publish_staged(&staging.name, &manifest)
            .map_err(|_| backup_failure("backup-bundle-publication-indeterminate", false))?;
        self.inventory
            .register(manifest)
            .await
            .map_err(|_| backup_failure("backup-inventory-commit-failed", false))?;
        publication.commit_succeeded();
        Ok(())
    }

    async fn execute_verify(
        &self,
        manifest: ProductBackupManifest,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        ensure_execution_live(&cancellation, deadline)?;
        let _repository = self.repository.mutation.lock().await;
        let retained = self
            .repository
            .manifest_for_id(manifest.backup_id())
            .map_err(|_| backup_failure("backup-manifest-unavailable", false))?;
        if retained != manifest {
            return Err(backup_failure("backup-manifest-mismatch", false));
        }
        let location = self
            .repository
            .location_for_id(manifest.backup_id())
            .map_err(|_| backup_failure("backup-bundle-unavailable", false))?;
        ProductBackupService::open_verified(location, manifest.clone(), self.limits, &cancellation)
            .map_err(|error| match error {
                ProductBackupError::Cancelled => LifecycleJobExecutionError::Cancelled,
                _ => backup_failure("backup-verification-failed", false),
            })?;
        ensure_execution_live(&cancellation, deadline)?;
        let manifest_bytes = serde_json::to_vec(&manifest)
            .map_err(|_| backup_failure("backup-manifest-invalid", false))?;
        let result = result_for_manifest(&manifest, &manifest_bytes)?;
        publication.prepare_and_claim(result)?;
        self.inventory
            .register(manifest)
            .await
            .map_err(|_| backup_failure("backup-inventory-commit-failed", false))?;
        publication.commit_succeeded();
        Ok(())
    }

    async fn execute_retention(
        &self,
        approval: BackupRetentionApproval,
        cancellation: CancellationToken,
        deadline: Instant,
        command_identity: SourceIdentifier,
        command_digest: EvidenceDigest,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        ensure_execution_live(&cancellation, deadline)?;
        let result =
            BackupJobRunner::try_result_reference(command_identity, command_digest, Vec::new())
                .map_err(|_| backup_failure("backup-result-invalid", false))?;
        publication.prepare_and_claim(result)?;
        self.inventory
            .apply_retention(approval, self.repository.as_ref())
            .await
            .map_err(|_| backup_failure("backup-retention-commit-failed", false))?;
        publication.commit_succeeded();
        Ok(())
    }
}

impl std::fmt::Debug for InstalledManagedBackupOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledManagedBackupOperations")
            .field("service", &self.service)
            .field("inventory", &self.inventory)
            .field("repository", &self.repository)
            .field("pending", &"[BOUNDED IMMUTABLE PLANS]")
            .field("maximum_pending", &self.maximum_pending)
            .finish()
    }
}

#[async_trait]
impl ManagedBackupOperations for InstalledManagedBackupOperations {
    async fn prepare_create(
        &self,
        active: WorkspaceRuntimeIdentity,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<PreparedOperation, ServiceError> {
        self.recover(&cancellation, deadline).await?;
        ensure_service_live(&cancellation, deadline)?;
        let cutoff = current_timestamp()?;
        self.retain_plan(
            RetainedBackupPlan::Create { active, cutoff },
            (active, cutoff),
        )
    }

    async fn prepare_verify(
        &self,
        manifest: ProductBackupManifest,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<PreparedOperation, ServiceError> {
        self.recover(&cancellation, deadline).await?;
        ensure_service_live(&cancellation, deadline)?;
        let retained = self
            .repository
            .manifest_for_id(manifest.backup_id())
            .map_err(map_repository_service_error)?;
        if retained != manifest {
            return Err(ServiceError::InvalidRequest);
        }
        let location = self
            .repository
            .location_for_id(manifest.backup_id())
            .map_err(map_repository_service_error)?;
        ProductBackupService::open_verified(location, manifest.clone(), self.limits, &cancellation)
            .map_err(map_backup_service_error)?;
        ensure_service_live(&cancellation, deadline)?;
        self.retain_plan(
            RetainedBackupPlan::Verify {
                manifest: manifest.clone(),
            },
            manifest,
        )
    }

    fn prepare_retention(
        &self,
        approval: BackupRetentionApproval,
    ) -> Result<PreparedOperation, ServiceError> {
        let approval_evidence = (
            "market-squawk-installed-backup-retention-approval-v1",
            approval.preview_sha256(),
        );
        self.retain_plan(
            RetainedBackupPlan::Retention { approval },
            approval_evidence,
        )
    }

    fn revoke(&self, operation: &PreparedOperation) {
        let Ok(mut pending) = self.pending.lock() else {
            return;
        };
        if pending
            .get(operation.identity())
            .is_some_and(|retained| retained.evidence_digest == operation.evidence_digest())
        {
            pending.remove(operation.identity());
        }
    }

    async fn execute(
        &self,
        command: BackupJobCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        ensure_execution_live(&cancellation, deadline)?;
        self.recover(&cancellation, deadline)
            .await
            .map_err(map_service_execution_error)?;
        let command_identity = command.identity().clone();
        let command_digest = command.evidence_digest();
        let plan = self
            .take_plan(&command)
            .map_err(map_service_execution_error)?;
        match plan {
            RetainedBackupPlan::Create { active, cutoff } => {
                self.execute_create(active, cutoff, cancellation, deadline, publication)
                    .await
            }
            RetainedBackupPlan::Verify { manifest } => {
                self.execute_verify(manifest, cancellation, deadline, publication)
                    .await
            }
            RetainedBackupPlan::Retention { approval } => {
                self.execute_retention(
                    approval,
                    cancellation,
                    deadline,
                    command_identity,
                    command_digest,
                    publication,
                )
                .await
            }
        }
    }
}

async fn run_create_until_deadline(
    service: &ProductBackupService,
    location: AnalyticalBackupLocation,
    cutoff: Timestamp,
    limits: AnalyticalBackupLimits,
    ownership: ProductBackupOwnership,
    request_cancellation: &CancellationToken,
    operation_cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<crate::application::backup::VerifiedProductBackup, LifecycleJobExecutionError> {
    let operation = service.create(location, cutoff, limits, ownership, operation_cancellation);
    tokio::pin!(operation);
    let deadline_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(deadline_sleep);
    tokio::select! {
        result = &mut operation => result.map_err(|error| match error {
            ProductBackupError::Cancelled => LifecycleJobExecutionError::Cancelled,
            _ => backup_failure("backup-materialization-failed", false),
        }),
        () = request_cancellation.cancelled() => {
            operation_cancellation.cancel();
            let _ignored = operation.await;
            Err(LifecycleJobExecutionError::Cancelled)
        }
        () = &mut deadline_sleep => {
            operation_cancellation.cancel();
            let _ignored = operation.await;
            Err(backup_failure("backup-deadline-exceeded", true))
        }
    }
}

fn result_for_manifest(
    manifest: &ProductBackupManifest,
    encoded: &[u8],
) -> Result<market_squawk_jobs::JobResultReference, LifecycleJobExecutionError> {
    let identity = SourceIdentifier::try_from(format!(
        "product-backup-result-{}",
        hex_bytes(&manifest.backup_id())
    ))
    .map_err(|_| backup_failure("backup-result-invalid", false))?;
    let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(encoded).into());
    BackupJobRunner::try_result_reference(identity, digest, Vec::new())
        .map_err(|_| backup_failure("backup-result-invalid", false))
}

fn ensure_service_live(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn ensure_execution_live(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), LifecycleJobExecutionError> {
    if cancellation.is_cancelled() {
        Err(LifecycleJobExecutionError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(backup_failure("backup-deadline-exceeded", true))
    } else {
        Ok(())
    }
}

fn map_service_execution_error(error: ServiceError) -> LifecycleJobExecutionError {
    match error {
        ServiceError::Cancelled => LifecycleJobExecutionError::Cancelled,
        ServiceError::DeadlineExceeded => backup_failure("backup-deadline-exceeded", true),
        ServiceError::ResourceExhausted => backup_failure("backup-capacity-exhausted", true),
        ServiceError::NotFound | ServiceError::Unauthorized | ServiceError::InvalidRequest => {
            backup_failure("backup-operation-revoked", false)
        }
        ServiceError::Unavailable | ServiceError::InvalidResult | ServiceError::Internal => {
            backup_failure("backup-authority-unavailable", true)
        }
    }
}

fn backup_failure(diagnostic: &'static str, retryable: bool) -> LifecycleJobExecutionError {
    SourceIdentifier::try_from(diagnostic).map_or(
        LifecycleJobExecutionError::Publication(LifecycleJobPublicationError::Revoked),
        |diagnostic| LifecycleJobExecutionError::failed(diagnostic, retryable),
    )
}

fn map_backup_service_error(error: ProductBackupError) -> ServiceError {
    match error {
        ProductBackupError::Cancelled => ServiceError::Cancelled,
        ProductBackupError::BackupNotFound => ServiceError::NotFound,
        ProductBackupError::InventoryCapacity => ServiceError::ResourceExhausted,
        ProductBackupError::InvalidComponent
        | ProductBackupError::InvalidSnapshot
        | ProductBackupError::InvalidComponentSchema
        | ProductBackupError::IncompleteComponents
        | ProductBackupError::UnencryptedSecretPayload
        | ProductBackupError::InvalidEncryptionEvidence
        | ProductBackupError::UnsupportedVersion
        | ProductBackupError::DigestMismatch
        | ProductBackupError::InvalidInventoryLimit
        | ProductBackupError::InvalidInventoryCursor
        | ProductBackupError::InvalidRetentionPolicy
        | ProductBackupError::RetentionEmpty
        | ProductBackupError::StaleRetentionApproval => ServiceError::InvalidRequest,
        ProductBackupError::Encoding
        | ProductBackupError::SnapshotMismatch
        | ProductBackupError::ArtifactUnavailable
        | ProductBackupError::ArtifactMismatch
        | ProductBackupError::Analytical(_)
        | ProductBackupError::InvalidRestoreTarget
        | ProductBackupError::RestoreComponents
        | ProductBackupError::RestoreWorker
        | ProductBackupError::InventoryCorrupt
        | ProductBackupError::InventoryUnavailable
        | ProductBackupError::InventoryPersistence => ServiceError::Unavailable,
    }
}

fn map_repository_service_error(error: RepositoryError) -> ServiceError {
    match error {
        RepositoryError::NotFound => ServiceError::NotFound,
        RepositoryError::Capacity => ServiceError::ResourceExhausted,
        RepositoryError::Conflict | RepositoryError::Corrupt => ServiceError::InvalidResult,
        RepositoryError::Unavailable | RepositoryError::Indeterminate => ServiceError::Unavailable,
    }
}

fn current_timestamp() -> Result<Timestamp, ServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?;
    let nanos = i64::try_from(elapsed.as_nanos()).map_err(|_| ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn action_name(action: BackupJobAction) -> &'static str {
    match action {
        BackupJobAction::Create => "create",
        BackupJobAction::Verify => "verify",
        BackupJobAction::EnforceRetention => "retention",
    }
}

fn parse_backup_directory_name(name: &str) -> Option<[u8; 32]> {
    if name.len() != 64 || !name.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in name.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        bytes[index] = high.checked_mul(16)?.checked_add(low)?;
    }
    Some(bytes)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn read_bounded_private_file(directory: &Dir, path: &Path) -> Result<Vec<u8>, RepositoryError> {
    let metadata = match directory.symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(RepositoryError::NotFound);
        }
        Err(_) => return Err(RepositoryError::Unavailable),
    };
    if !metadata.is_file()
        || metadata.nlink() != 1
        || !private_permissions(&metadata)
        || metadata.len() == 0
        || metadata.len() > u64::try_from(MAXIMUM_MANIFEST_BYTES).unwrap_or(u64::MAX)
    {
        return Err(RepositoryError::Corrupt);
    }
    let size = usize::try_from(metadata.len()).map_err(|_| RepositoryError::Capacity)?;
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    configure_nonblocking_read(&mut options);
    let mut file = directory
        .open_with(path, &options)
        .map_err(|_| RepositoryError::Unavailable)?;
    let opened = file.metadata().map_err(|_| RepositoryError::Unavailable)?;
    if FileIdentity::from_metadata(&opened) != FileIdentity::from_metadata(&metadata)
        || opened.len() != metadata.len()
    {
        return Err(RepositoryError::Corrupt);
    }
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(size)
        .map_err(|_| RepositoryError::Capacity)?;
    encoded.resize(size, 0);
    file.read_exact(&mut encoded)
        .map_err(|_| RepositoryError::Unavailable)?;
    let mut trailing = [0_u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|_| RepositoryError::Unavailable)?
        != 0
    {
        return Err(RepositoryError::Corrupt);
    }
    Ok(encoded)
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn configure_nonblocking_read(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC);
}

#[cfg(not(unix))]
fn configure_nonblocking_read(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn synchronize_directory(directory: &Dir, path: &Path) -> Result<(), RepositoryError> {
    use cap_std::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    directory
        .open_with(path, &options)
        .map(cap_std::fs::File::into_std)
        .and_then(|opened| opened.sync_all())
        .map_err(|_| RepositoryError::Unavailable)
}

#[cfg(windows)]
fn synchronize_directory(_directory: &Dir, _path: &Path) -> Result<(), RepositoryError> {
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn synchronize_directory(_directory: &Dir, _path: &Path) -> Result<(), RepositoryError> {
    Err(RepositoryError::Unavailable)
}
