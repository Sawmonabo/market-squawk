//! Verified-bundle reading and fresh managed-workspace restore composition.
//!
//! Product components are copied into private retained staging files, verified against the
//! manifest, and consumed only after the analytical catalog and artifacts have been restored.
//! Every durable owner then restores through its typed fresh-target API; no live SQLite database
//! or journal is copied into place.

use std::{
    collections::BTreeMap,
    fmt,
    io::{Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use cap_fs_ext::{DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_data::{
    AnalyticalDataService, AnalyticalRestoreMode, AnalyticalRestoreTarget, ObjectStoreConfig,
};
use market_squawk_decisions::DecisionRepositoryLimits;
use market_squawk_jobs::{JobRepositoryConfig, SqliteJobRepository};
use market_squawk_platform::LocalPaths;
use market_squawk_runtime::WorkspaceId;
use market_squawk_services::ArtifactRepository;
use market_squawk_valuation::FairValueLimits;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    application::{
        backup::{
            ProductBackupComponent, ProductBackupComponentKind, ProductBackupError,
            ProductBackupManifest, ProductRestoreFinalizer, StagedProductRestoreTarget,
        },
        decision::DecisionApplication,
        fair_value::FairValueBackupAttestation,
        model::backup::ModelBackupAuthority,
        settings::SettingsSeed,
        workspace::WorkspaceDescriptor,
    },
    artifact_repository::controlled_artifact_repository,
    portfolio_application::{PortfolioApplicationLimits, PortfolioBackupAuthority},
};

use super::{
    backup::ManagedBackupRepository,
    configuration_backup::restore_configuration_component_absent,
    settings::SettingsLifecycleAuthority,
    source_data_backup::validate_fresh_restore,
    workspace_backup::{
        FreshWorkspaceRestoreAuthority, FreshWorkspaceRestoreSession,
        VerifiedWorkspaceComponentReader, WorkspaceBackupBundleSource,
    },
};
use crate::local_product::provider_activation_state::ProviderMetadataBackupAuthority;

const RESTORE_STAGING_DIRECTORY: &str = "product-restore-staging";
const RESTORE_FILE_PREFIX: &str = "restore-";
const COPY_BUFFER_BYTES: usize = 64 * 1024;
const REQUIRED_COMPONENTS: [ProductBackupComponentKind; 9] = [
    ProductBackupComponentKind::Configuration,
    ProductBackupComponentKind::ProviderMetadata,
    ProductBackupComponentKind::SourceData,
    ProductBackupComponentKind::Portfolios,
    ProductBackupComponentKind::Transactions,
    ProductBackupComponentKind::Models,
    ProductBackupComponentKind::DecisionTargets,
    ProductBackupComponentKind::JobsAndReceipts,
    ProductBackupComponentKind::FairValueEvidence,
];

/// Fresh inactive managed workspace returned by the installation-global selector authority.
pub(crate) struct PreparedFreshWorkspace {
    descriptor: WorkspaceDescriptor,
    paths: LocalPaths,
}

impl PreparedFreshWorkspace {
    /// Binds one prepared descriptor to its already capability-confined local root.
    pub(crate) fn try_new(
        descriptor: WorkspaceDescriptor,
        paths: LocalPaths,
    ) -> Result<Self, ProductBackupError> {
        if !descriptor.is_prepared() {
            return Err(ProductBackupError::InvalidRestoreTarget);
        }
        paths
            .control_root()
            .and_then(|control| control.try_clone_directory().map(|_directory| ()))
            .map_err(|_| ProductBackupError::InvalidRestoreTarget)?;
        Ok(Self { descriptor, paths })
    }
}

impl fmt::Debug for PreparedFreshWorkspace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedFreshWorkspace")
            .field("descriptor", &self.descriptor)
            .field("paths", &"[FRESH MANAGED CAPABILITIES]")
            .finish()
    }
}

/// Installation-global owner that creates and abandons inactive managed workspaces.
#[async_trait]
pub(crate) trait ManagedWorkspaceRestoreAuthority: fmt::Debug + Send + Sync {
    /// Creates a new registered-but-inactive workspace. It must never reopen an existing target.
    async fn prepare_fresh(
        &self,
        source_workspace: WorkspaceId,
        active_workspace: WorkspaceId,
        cancellation: &CancellationToken,
    ) -> Result<PreparedFreshWorkspace, ProductBackupError>;

    /// Removes or permanently marks unusable a failed inactive workspace, idempotently.
    async fn abandon(&self, workspace_id: WorkspaceId) -> Result<(), ProductBackupError>;
}

/// Complete validated restore policy shared by every fresh workspace session.
pub(crate) struct WorkspaceRestorePolicy {
    settings_seed: SettingsSeed,
    settings_lifecycle: SettingsLifecycleAuthority,
    portfolio_limits: PortfolioApplicationLimits,
    model: Arc<ModelBackupAuthority>,
    decision_limits: DecisionRepositoryLimits,
    jobs: JobRepositoryConfig,
    fair_value_limits: FairValueLimits,
    object_store: ObjectStoreConfig,
    maximum_objects_per_generation: usize,
    maximum_controlled_artifact_bytes: NonZeroUsize,
    maximum_buffered_component_bytes: NonZeroUsize,
}

impl WorkspaceRestorePolicy {
    /// Binds the same validated limits and restore capabilities used by normal composition.
    #[allow(
        clippy::too_many_arguments,
        reason = "each owner-specific restore capability and resource ceiling remains explicit"
    )]
    pub(crate) fn try_new(
        settings_seed: SettingsSeed,
        settings_lifecycle: SettingsLifecycleAuthority,
        portfolio_limits: PortfolioApplicationLimits,
        model: Arc<ModelBackupAuthority>,
        decision_limits: DecisionRepositoryLimits,
        jobs: JobRepositoryConfig,
        fair_value_limits: FairValueLimits,
        object_store: ObjectStoreConfig,
        maximum_objects_per_generation: usize,
        maximum_controlled_artifact_bytes: NonZeroUsize,
        maximum_buffered_component_bytes: NonZeroUsize,
    ) -> Result<Self, ProductBackupError> {
        if !(1..=1024).contains(&maximum_objects_per_generation) {
            return Err(ProductBackupError::InvalidRestoreTarget);
        }
        Ok(Self {
            settings_seed,
            settings_lifecycle,
            portfolio_limits,
            model,
            decision_limits,
            jobs,
            fair_value_limits,
            object_store,
            maximum_objects_per_generation,
            maximum_controlled_artifact_bytes,
            maximum_buffered_component_bytes,
        })
    }
}

impl fmt::Debug for WorkspaceRestorePolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WorkspaceRestorePolicy([VALIDATED OWNER CAPABILITIES AND LIMITS])")
    }
}

/// Least-authority reader over exact components in the managed backup repository.
pub(crate) struct InstalledWorkspaceBackupBundleSource {
    repository: Arc<ManagedBackupRepository>,
}

impl InstalledWorkspaceBackupBundleSource {
    /// Restricts restore reads to one already-opened managed repository.
    #[must_use]
    pub(crate) fn new(repository: Arc<ManagedBackupRepository>) -> Self {
        Self { repository }
    }
}

impl fmt::Debug for InstalledWorkspaceBackupBundleSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InstalledWorkspaceBackupBundleSource([MANAGED REPOSITORY])")
    }
}

#[async_trait]
impl WorkspaceBackupBundleSource for InstalledWorkspaceBackupBundleSource {
    async fn open_verified_component(
        &self,
        manifest: &ProductBackupManifest,
        component: &ProductBackupComponent,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn VerifiedWorkspaceComponentReader>, ProductBackupError> {
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        let file = self
            .repository
            .open_exact_component(manifest, component)
            .await?;
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        Ok(Box::new(file))
    }
}

/// Installed fresh-restore authority over one workspace factory and exact owner policy.
pub(crate) struct InstalledFreshWorkspaceRestoreAuthority {
    workspaces: Arc<dyn ManagedWorkspaceRestoreAuthority>,
    policy: Arc<WorkspaceRestorePolicy>,
}

impl InstalledFreshWorkspaceRestoreAuthority {
    /// Seals the installation-global workspace factory and all fresh-owner dependencies.
    #[must_use]
    pub(crate) fn new(
        workspaces: Arc<dyn ManagedWorkspaceRestoreAuthority>,
        policy: Arc<WorkspaceRestorePolicy>,
    ) -> Self {
        Self { workspaces, policy }
    }
}

impl fmt::Debug for InstalledFreshWorkspaceRestoreAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InstalledFreshWorkspaceRestoreAuthority([FRESH WORKSPACE OWNER])")
    }
}

#[async_trait]
impl FreshWorkspaceRestoreAuthority for InstalledFreshWorkspaceRestoreAuthority {
    async fn prepare(
        &self,
        manifest: &ProductBackupManifest,
        active_workspace: WorkspaceId,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn FreshWorkspaceRestoreSession>, ProductBackupError> {
        manifest.verify()?;
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        let source_workspace = manifest.ownership().workspace_id();
        let fresh = self
            .workspaces
            .prepare_fresh(source_workspace, active_workspace, cancellation)
            .await?;
        let workspace_id = fresh.descriptor.workspace_id();
        if workspace_id == source_workspace || workspace_id == active_workspace {
            self.workspaces.abandon(workspace_id).await?;
            return Err(ProductBackupError::InvalidRestoreTarget);
        }
        let staging = match RestoreStagingDirectory::create(&fresh.paths) {
            Ok(staging) => staging,
            Err(error) => {
                self.workspaces.abandon(workspace_id).await?;
                return Err(error);
            }
        };
        Ok(Box::new(InstalledFreshWorkspaceRestoreSession {
            fresh,
            source_workspace,
            active_workspace,
            snapshot: manifest.snapshot(),
            policy: Arc::clone(&self.policy),
            staging,
            components: BTreeMap::new(),
            next_component: 0,
        }))
    }

    async fn abandon(
        &self,
        workspace_id: WorkspaceId,
        _cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        self.workspaces.abandon(workspace_id).await
    }
}

struct InstalledFreshWorkspaceRestoreSession {
    fresh: PreparedFreshWorkspace,
    source_workspace: WorkspaceId,
    active_workspace: WorkspaceId,
    snapshot: crate::application::backup::ProductBackupSnapshot,
    policy: Arc<WorkspaceRestorePolicy>,
    staging: RestoreStagingDirectory,
    components: BTreeMap<ProductBackupComponentKind, StagedComponent>,
    next_component: usize,
}

impl fmt::Debug for InstalledFreshWorkspaceRestoreSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledFreshWorkspaceRestoreSession")
            .field("workspace", &self.fresh.descriptor)
            .field("staged_components", &self.components.len())
            .finish()
    }
}

#[async_trait]
impl FreshWorkspaceRestoreSession for InstalledFreshWorkspaceRestoreSession {
    fn workspace_id(&self) -> WorkspaceId {
        self.fresh.descriptor.workspace_id()
    }

    async fn stage_component(
        &mut self,
        component: &ProductBackupComponent,
        reader: &mut (dyn Read + Send),
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        let expected = REQUIRED_COMPONENTS
            .get(self.next_component)
            .copied()
            .ok_or(ProductBackupError::IncompleteComponents)?;
        if cancellation.is_cancelled()
            || component.kind() != expected
            || component.snapshot() != self.snapshot
            || component.byte_length() == 0
            || self.components.contains_key(&expected)
        {
            return if cancellation.is_cancelled() {
                Err(ProductBackupError::Cancelled)
            } else {
                Err(ProductBackupError::InvalidComponent)
            };
        }
        let mut file = self.staging.create_component(expected)?;
        copy_exact_component(reader, &mut file, component.byte_length(), cancellation)?;
        file.sync_all()
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        file.seek(SeekFrom::Start(0))
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        self.components.insert(
            expected,
            StagedComponent {
                evidence: component.clone(),
                file,
            },
        );
        self.next_component = self
            .next_component
            .checked_add(1)
            .ok_or(ProductBackupError::IncompleteComponents)?;
        Ok(())
    }

    async fn complete(
        self: Box<Self>,
        manifest: &ProductBackupManifest,
        cancellation: &CancellationToken,
    ) -> Result<StagedProductRestoreTarget, ProductBackupError> {
        manifest.verify()?;
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        if manifest.snapshot() != self.snapshot
            || manifest.ownership().workspace_id() != self.source_workspace
            || self.next_component != REQUIRED_COMPONENTS.len()
            || self.components.len() != REQUIRED_COMPONENTS.len()
            || manifest.components().iter().any(|component| {
                self.components
                    .get(&component.kind())
                    .is_none_or(|staged| staged.evidence != *component)
            })
        {
            return Err(ProductBackupError::IncompleteComponents);
        }
        let catalog = crate::local_product::local_catalog_config(&self.fresh.paths)
            .map_err(|_| ProductBackupError::InvalidRestoreTarget)?;
        let artifacts = self
            .fresh
            .paths
            .artifacts()
            .map_err(|_| ProductBackupError::InvalidRestoreTarget)?
            .clone();
        let analytical = AnalyticalRestoreTarget::try_new(
            catalog,
            artifacts,
            self.policy.maximum_objects_per_generation,
            self.policy.object_store,
            AnalyticalRestoreMode::Fresh,
        )?;
        let target_workspace = self.fresh.descriptor.workspace_id();
        let finalizer = InstalledWorkspaceRestoreFinalizer {
            paths: self.fresh.paths.clone(),
            target_workspace,
            snapshot: self.snapshot,
            policy: self.policy,
            staging: self.staging,
            components: self.components,
        };
        StagedProductRestoreTarget::try_new(
            self.fresh.descriptor,
            analytical,
            Box::new(finalizer),
            self.source_workspace,
            self.active_workspace,
        )
    }
}

struct InstalledWorkspaceRestoreFinalizer {
    paths: LocalPaths,
    target_workspace: WorkspaceId,
    snapshot: crate::application::backup::ProductBackupSnapshot,
    policy: Arc<WorkspaceRestorePolicy>,
    staging: RestoreStagingDirectory,
    components: BTreeMap<ProductBackupComponentKind, StagedComponent>,
}

impl fmt::Debug for InstalledWorkspaceRestoreFinalizer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledWorkspaceRestoreFinalizer")
            .field("components", &self.components.len())
            .field("staging", &"[PRIVATE RETAINED FILES]")
            .finish()
    }
}

#[async_trait]
impl ProductRestoreFinalizer for InstalledWorkspaceRestoreFinalizer {
    async fn finalize(
        mut self: Box<Self>,
        analytical: &AnalyticalDataService,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        ensure_live(cancellation)?;
        let configuration =
            self.read_component(ProductBackupComponentKind::Configuration, cancellation)?;
        restore_configuration_component_absent(
            self.paths
                .control_root()
                .map_err(|_| ProductBackupError::InvalidRestoreTarget)?
                .root(),
            self.target_workspace,
            self.policy.settings_seed.clone(),
            self.policy.settings_lifecycle.clone(),
            &configuration,
        )
        .map_err(|_| ProductBackupError::RestoreComponents)?;

        ensure_live(cancellation)?;
        let provider_metadata =
            self.read_component(ProductBackupComponentKind::ProviderMetadata, cancellation)?;
        let _requirements = ProviderMetadataBackupAuthority::restore_fresh_workspace(
            &self.paths,
            &provider_metadata,
        )
        .map_err(|_| ProductBackupError::RestoreComponents)?;

        ensure_live(cancellation)?;
        let source_data =
            self.read_component(ProductBackupComponentKind::SourceData, cancellation)?;
        validate_fresh_restore(self.snapshot, &source_data)?;

        ensure_live(cancellation)?;
        let portfolios =
            self.read_component(ProductBackupComponentKind::Portfolios, cancellation)?;
        let transactions =
            self.read_component(ProductBackupComponentKind::Transactions, cancellation)?;
        if portfolios
            .len()
            .checked_add(transactions.len())
            .is_none_or(|combined| combined > self.policy.maximum_buffered_component_bytes.get())
        {
            return Err(ProductBackupError::RestoreComponents);
        }
        let _portfolio = PortfolioBackupAuthority::restore_fresh(
            &self.paths,
            self.policy.portfolio_limits,
            &portfolios,
            &transactions,
        )
        .map_err(|_| ProductBackupError::RestoreComponents)?;

        ensure_live(cancellation)?;
        let controlled = controlled_artifact_repository(
            self.paths
                .artifacts()
                .map_err(|_| ProductBackupError::InvalidRestoreTarget)?
                .clone(),
            self.policy.maximum_controlled_artifact_bytes,
        )
        .map_err(|_| ProductBackupError::RestoreComponents)?;
        let artifacts: Arc<dyn ArtifactRepository> = controlled;
        let models = self
            .components
            .get_mut(&ProductBackupComponentKind::Models)
            .ok_or(ProductBackupError::IncompleteComponents)?;
        models.verify_and_rewind(cancellation)?;
        let _models = self
            .policy
            .model
            .restore_fresh_workspace(
                &mut models.file,
                self.paths.clone(),
                artifacts,
                cancellation,
            )
            .await
            .map_err(|_| ProductBackupError::RestoreComponents)?;

        ensure_live(cancellation)?;
        let decisions =
            self.read_component(ProductBackupComponentKind::DecisionTargets, cancellation)?;
        let _decisions = DecisionApplication::restore_backup_fresh(
            self.paths
                .control_root()
                .map_err(|_| ProductBackupError::InvalidRestoreTarget)?
                .decision_database_location(),
            self.policy.decision_limits,
            &decisions,
            component_digest(
                &self.components,
                ProductBackupComponentKind::DecisionTargets,
            )?,
        )
        .map_err(|_| ProductBackupError::RestoreComponents)?;

        ensure_live(cancellation)?;
        let jobs =
            self.read_component(ProductBackupComponentKind::JobsAndReceipts, cancellation)?;
        SqliteJobRepository::restore_fresh(
            self.paths
                .control_root()
                .map_err(|_| ProductBackupError::InvalidRestoreTarget)?
                .job_database_location(),
            self.policy.jobs,
            &jobs,
        )
        .await
        .map_err(|_| ProductBackupError::RestoreComponents)?;

        ensure_live(cancellation)?;
        let fair_value =
            self.read_component(ProductBackupComponentKind::FairValueEvidence, cancellation)?;
        let attestation = FairValueBackupAttestation::decode(&fair_value)
            .map_err(|_| ProductBackupError::RestoreComponents)?;
        let _fair_value = attestation
            .validate_restored_catalog(
                analytical.fair_value_catalog(),
                self.policy.fair_value_limits,
            )
            .map_err(|_| ProductBackupError::RestoreComponents)?;
        ensure_live(cancellation)?;
        self.components.clear();
        self.staging.remove()?;
        Ok(())
    }
}

impl InstalledWorkspaceRestoreFinalizer {
    fn read_component(
        &mut self,
        kind: ProductBackupComponentKind,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, ProductBackupError> {
        self.components
            .get_mut(&kind)
            .ok_or(ProductBackupError::IncompleteComponents)?
            .read_verified(
                self.policy.maximum_buffered_component_bytes.get(),
                cancellation,
            )
    }
}

struct StagedComponent {
    evidence: ProductBackupComponent,
    file: cap_std::fs::File,
}

impl StagedComponent {
    fn verify_and_rewind(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        let mut digest = Sha256::new();
        let mut observed = 0_u64;
        let mut buffer = [0_u8; COPY_BUFFER_BYTES];
        loop {
            ensure_live(cancellation)?;
            let read = self
                .file
                .read(&mut buffer)
                .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
            if read == 0 {
                break;
            }
            observed = observed
                .checked_add(u64::try_from(read).map_err(|_| ProductBackupError::ArtifactMismatch)?)
                .ok_or(ProductBackupError::ArtifactMismatch)?;
            if observed > self.evidence.byte_length() {
                return Err(ProductBackupError::ArtifactMismatch);
            }
            digest.update(&buffer[..read]);
        }
        if observed != self.evidence.byte_length()
            || <[u8; 32]>::from(digest.finalize()) != self.evidence.sha256()
        {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        Ok(())
    }

    fn read_verified(
        &mut self,
        maximum_bytes: usize,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, ProductBackupError> {
        if self.evidence.byte_length() > u64::try_from(maximum_bytes).unwrap_or(u64::MAX) {
            return Err(ProductBackupError::RestoreComponents);
        }
        self.verify_and_rewind(cancellation)?;
        let size = usize::try_from(self.evidence.byte_length())
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| ProductBackupError::RestoreComponents)?;
        let mut buffer = [0_u8; COPY_BUFFER_BYTES];
        loop {
            ensure_live(cancellation)?;
            let read = self
                .file
                .read(&mut buffer)
                .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
            if bytes.len() > size {
                return Err(ProductBackupError::ArtifactMismatch);
            }
        }
        if bytes.len() != size {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        Ok(bytes)
    }
}

fn component_digest(
    components: &BTreeMap<ProductBackupComponentKind, StagedComponent>,
    kind: ProductBackupComponentKind,
) -> Result<[u8; 32], ProductBackupError> {
    components
        .get(&kind)
        .map(|component| component.evidence.sha256())
        .ok_or(ProductBackupError::IncompleteComponents)
}

struct RestoreStagingDirectory {
    parent: Dir,
    name: String,
    directory: Option<Dir>,
    removed: bool,
}

impl RestoreStagingDirectory {
    fn create(paths: &LocalPaths) -> Result<Self, ProductBackupError> {
        let control = paths
            .control_root()
            .map_err(|_| ProductBackupError::InvalidRestoreTarget)?
            .try_clone_directory()
            .map_err(|_| ProductBackupError::InvalidRestoreTarget)?;
        ensure_private_staging_parent(&control)?;
        let parent = control
            .open_dir_nofollow(RESTORE_STAGING_DIRECTORY)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        let mut nonce = [0_u8; 16];
        getrandom::fill(&mut nonce).map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        let name = format!("{RESTORE_FILE_PREFIX}{}", hex(&nonce));
        parent
            .create_dir(&name)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        configure_private_directory(&parent, Path::new(&name))?;
        let directory = parent
            .open_dir_nofollow(&name)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        Ok(Self {
            parent,
            name,
            directory: Some(directory),
            removed: false,
        })
    }

    fn create_component(
        &self,
        kind: ProductBackupComponentKind,
    ) -> Result<cap_std::fs::File, ProductBackupError> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        configure_private_file(&mut options);
        self.directory
            .as_ref()
            .ok_or(ProductBackupError::ArtifactUnavailable)?
            .open_with(component_name(kind), &options)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)
    }

    fn remove(&mut self) -> Result<(), ProductBackupError> {
        if self.removed {
            return Ok(());
        }
        drop(self.directory.take());
        self.parent
            .remove_dir_all(&self.name)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        self.removed = true;
        Ok(())
    }
}

impl Drop for RestoreStagingDirectory {
    fn drop(&mut self) {
        if !self.removed {
            drop(self.directory.take());
            let _ignored = self.parent.remove_dir_all(&self.name);
        }
    }
}

fn copy_exact_component(
    reader: &mut (dyn Read + Send),
    writer: &mut cap_std::fs::File,
    expected_length: u64,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    let mut observed = 0_u64;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        ensure_live(cancellation)?;
        let read = reader
            .read(&mut buffer)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(u64::try_from(read).map_err(|_| ProductBackupError::ArtifactMismatch)?)
            .ok_or(ProductBackupError::ArtifactMismatch)?;
        if observed > expected_length {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        writer
            .write_all(&buffer[..read])
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
    }
    if observed != expected_length {
        return Err(ProductBackupError::ArtifactMismatch);
    }
    Ok(())
}

fn ensure_live(cancellation: &CancellationToken) -> Result<(), ProductBackupError> {
    if cancellation.is_cancelled() {
        Err(ProductBackupError::Cancelled)
    } else {
        Ok(())
    }
}

fn component_name(kind: ProductBackupComponentKind) -> &'static str {
    match kind {
        ProductBackupComponentKind::Configuration => "configuration.component",
        ProductBackupComponentKind::ProviderMetadata => "provider-metadata.component",
        ProductBackupComponentKind::SourceData => "source-data.component",
        ProductBackupComponentKind::Portfolios => "portfolios.component",
        ProductBackupComponentKind::Transactions => "transactions.component",
        ProductBackupComponentKind::Models => "models.component",
        ProductBackupComponentKind::DecisionTargets => "decision-targets.component",
        ProductBackupComponentKind::JobsAndReceipts => "jobs-and-receipts.component",
        ProductBackupComponentKind::FairValueEvidence => "fair-value-evidence.component",
    }
}

fn ensure_private_staging_parent(control: &Dir) -> Result<(), ProductBackupError> {
    match control.symlink_metadata(RESTORE_STAGING_DIRECTORY) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Err(ProductBackupError::ArtifactUnavailable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            control
                .create_dir(RESTORE_STAGING_DIRECTORY)
                .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
            configure_private_directory(control, Path::new(RESTORE_STAGING_DIRECTORY))?;
        }
        Err(_) => return Err(ProductBackupError::ArtifactUnavailable),
    }
    control
        .open_dir_nofollow(RESTORE_STAGING_DIRECTORY)
        .map(|_directory| ())
        .map_err(|_| ProductBackupError::ArtifactUnavailable)
}

#[cfg(unix)]
fn configure_private_directory(directory: &Dir, path: &Path) -> Result<(), ProductBackupError> {
    use cap_std::fs::PermissionsExt as _;

    directory
        .set_permissions(path, cap_std::fs::Permissions::from_mode(0o700))
        .map_err(|_| ProductBackupError::ArtifactUnavailable)
}

#[cfg(not(unix))]
fn configure_private_directory(_directory: &Dir, _path: &Path) -> Result<(), ProductBackupError> {
    Ok(())
}

#[cfg(unix)]
fn configure_private_file(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_file(_options: &mut OpenOptions) {}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
