//! Installed Operations authority construction from retained production capabilities.

use std::{
    fmt,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk_data::{AnalyticalBackupLimits, AnalyticalBackupService};
use market_squawk_platform::LocalAuthorityStateStore;
use market_squawk_runtime::{InstallationId, WorkspaceId};
use market_squawk_services::ServiceError;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    LocalProduct,
    application::{
        backup::{
            ProductBackupInventory, ProductBackupSnapshotAuthority,
            ProductRestoreComponentAuthority,
        },
        lifecycle::{
            TrustedUpdateAuthority, UpdateActivitySnapshot, UpdateError,
            WorkspaceLifecycleAuthority,
        },
        operations::{
            ManagedBackupOperations, ManagedRecoveryOperations,
            ManagedUpdateOperations as ManagedUpdateOperationsContract, UpdateAvailabilityEvidence,
        },
        workspace::DurableWorkspaceRegistry,
    },
    jobs::{BackupJobRunner, InstalledJobAuthority},
    local_product::operations::{
        backup::{
            InstalledManagedBackupOperations, ManagedBackupComponentSource, ManagedBackupRepository,
        },
        configuration_backup::ConfigurationWorkspaceBackupAuthority,
        decision_backup::DecisionWorkspaceBackupAuthority,
        fair_value_backup::FairValueWorkspaceBackupAuthority,
        jobs_backup::JobsAndReceiptsWorkspaceBackupAuthority,
        model_backup::ModelWorkspaceBackupAuthority,
        portfolio_backup::PortfolioWorkspaceBackupAuthority,
        provider_metadata_backup::ProviderMetadataWorkspaceBackupAuthority,
        recovery::{
            DurableRecoveryState, InstalledRecoveryOperations, RecoveryActivityAuthority,
            SupervisorRestartWorkspaceTransition,
        },
        settings::ProductionSettingsOperations,
        source_data_backup::SourceDataWorkspaceBackupAuthority,
        update::{
            ManagedUpdateLimits, ManagedUpdateOperations, TrustedUpdateRepository,
            UnavailableUpdateOperations,
        },
        workspace_backup::{
            InstalledWorkspaceBackupAuthority, WorkspaceComponentSnapshotAuthority,
        },
        workspace_restore::{
            InstalledFreshWorkspaceRestoreAuthority, InstalledWorkspaceBackupBundleSource,
        },
    },
};

/// Owned completion returned by the service's bounded update-drain authority.
pub(crate) type UpdateDrainFuture =
    Pin<Box<dyn Future<Output = Result<(), UpdateError>> + Send + 'static>>;
/// Exact runtime-activity reader used to admit one immutable update bundle.
pub(crate) type UpdateActivityReader =
    dyn Fn(u64) -> Result<UpdateActivitySnapshot, ServiceError> + Send + Sync + 'static;
/// Bounded service drain and reconciliation authority used before update activation.
pub(crate) type UpdateDrainAuthority =
    dyn Fn(CancellationToken, Instant) -> UpdateDrainFuture + Send + Sync + 'static;

/// Exact ordered capabilities consumed by the service's atomic Operations binding.
pub(crate) type InstalledOperationsAuthorityParts = (
    Arc<dyn ManagedBackupOperations>,
    Arc<dyn ManagedRecoveryOperations>,
    Arc<dyn ManagedUpdateOperationsContract>,
);

/// Composes the fixed nine-component aggregate from the exact live product owners.
pub(crate) fn try_compose_installed_workspace_backup(
    product: &LocalProduct,
    settings: Arc<ProductionSettingsOperations>,
    jobs: &InstalledJobAuthority,
    backup_runner: &BackupJobRunner,
    bundles: Arc<InstalledWorkspaceBackupBundleSource>,
    restore: Arc<InstalledFreshWorkspaceRestoreAuthority>,
    active_workspace: WorkspaceId,
) -> Result<Arc<InstalledWorkspaceBackupAuthority>, ServiceError> {
    let owners: Vec<Arc<dyn WorkspaceComponentSnapshotAuthority>> = vec![
        Arc::new(ConfigurationWorkspaceBackupAuthority::try_new(settings)?),
        Arc::new(ProviderMetadataWorkspaceBackupAuthority::try_new(
            product.provider_metadata_backup_authority(),
        )?),
        Arc::new(SourceDataWorkspaceBackupAuthority::try_new()?),
        Arc::new(PortfolioWorkspaceBackupAuthority::try_new(
            product.portfolio().backup_authority(),
        )?),
        Arc::new(ModelWorkspaceBackupAuthority::try_new(
            product
                .model_backup_authority()
                .map_err(|_error| ServiceError::Unavailable)?,
        )?),
        Arc::new(DecisionWorkspaceBackupAuthority::try_new(
            product.decisions(),
        )?),
        Arc::new(JobsAndReceiptsWorkspaceBackupAuthority::try_new(
            jobs,
            backup_runner,
        )?),
        Arc::new(FairValueWorkspaceBackupAuthority::try_new(
            product.fair_value_service(),
        )?),
    ];
    InstalledWorkspaceBackupAuthority::try_new(owners, bundles, restore, active_workspace)
        .map(Arc::new)
        .map_err(|_error| ServiceError::Unavailable)
}

/// Exact workspace snapshot and fresh-restore authorities retained by installed composition.
#[derive(Clone)]
pub(crate) struct WorkspaceBackupAuthorities {
    snapshot: Arc<dyn ProductBackupSnapshotAuthority>,
    restore: Arc<dyn ProductRestoreComponentAuthority>,
}

impl WorkspaceBackupAuthorities {
    /// Uses the fixed nine-owner workspace aggregate for both snapshot and restore.
    pub(crate) fn from_installed(authority: Arc<InstalledWorkspaceBackupAuthority>) -> Self {
        let snapshot: Arc<dyn ProductBackupSnapshotAuthority> = authority.clone();
        let restore: Arc<dyn ProductRestoreComponentAuthority> = authority;
        Self { snapshot, restore }
    }
}

impl fmt::Debug for WorkspaceBackupAuthorities {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WorkspaceBackupAuthorities([SNAPSHOT AND FRESH RESTORE AUTHORITIES])")
    }
}

/// Backup inputs that are independent of recovery and update composition.
pub(crate) struct InstalledBackupAuthorityInputs {
    pub(crate) analytical: Arc<AnalyticalBackupService>,
    pub(crate) inventory: Arc<ProductBackupInventory>,
    pub(crate) repository: Arc<ManagedBackupRepository>,
    pub(crate) installation_id: InstallationId,
    pub(crate) workspace: WorkspaceBackupAuthorities,
    pub(crate) limits: AnalyticalBackupLimits,
    pub(crate) maximum_pending: usize,
    pub(crate) startup_cancellation: CancellationToken,
    pub(crate) startup_deadline: Instant,
}

impl fmt::Debug for InstalledBackupAuthorityInputs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledBackupAuthorityInputs")
            .field("installation_id", &self.installation_id)
            .field("workspace", &self.workspace)
            .field("limits", &self.limits)
            .field("maximum_pending", &self.maximum_pending)
            .finish_non_exhaustive()
    }
}

/// Recovery inputs retained from the workspace, lifecycle, supervisor, and installer owners.
pub(crate) struct InstalledRecoveryAuthorityInputs {
    pub(crate) backup_repository_root: PathBuf,
    pub(crate) workspace_repository_root: PathBuf,
    pub(crate) backup_limits: AnalyticalBackupLimits,
    pub(crate) minimum_schema_version: u32,
    pub(crate) maximum_schema_version: u32,
    pub(crate) install_root: Option<PathBuf>,
    pub(crate) workspaces: Arc<DurableWorkspaceRegistry>,
    pub(crate) lifecycle: Arc<WorkspaceLifecycleAuthority>,
    pub(crate) transition: Arc<SupervisorRestartWorkspaceTransition>,
    pub(crate) activity: Arc<dyn RecoveryActivityAuthority>,
    pub(crate) durable: Arc<DurableRecoveryState>,
}

impl fmt::Debug for InstalledRecoveryAuthorityInputs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledRecoveryAuthorityInputs")
            .field("backup_repository_root", &"[BACKUP REPOSITORY]")
            .field("workspace_repository_root", &"[WORKSPACE REPOSITORY]")
            .field("backup_limits", &self.backup_limits)
            .field("minimum_schema_version", &self.minimum_schema_version)
            .field("maximum_schema_version", &self.maximum_schema_version)
            .field("install_root", &"[INSTALL ROOT]")
            .finish_non_exhaustive()
    }
}

/// Complete inputs for constructing the three concrete installed lifecycle-operation authorities.
pub(crate) struct InstalledOperationsAuthorityInputs {
    pub(crate) backup: InstalledBackupAuthorityInputs,
    pub(crate) recovery: InstalledRecoveryAuthorityInputs,
    pub(crate) update: Arc<dyn ManagedUpdateOperationsContract>,
}

impl fmt::Debug for InstalledOperationsAuthorityInputs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledOperationsAuthorityInputs")
            .field("backup", &self.backup)
            .field("recovery", &self.recovery)
            .field("update", &"[TRUSTED UPDATE AUTHORITY]")
            .finish()
    }
}

/// Fully initialized authorities ready for one atomic service-level bind.
pub(crate) struct InstalledOperationsAuthorityBundle {
    backup: Arc<dyn ManagedBackupOperations>,
    recovery: Arc<dyn ManagedRecoveryOperations>,
    update: Arc<dyn ManagedUpdateOperationsContract>,
}

/// Exact installed-operations composition stage and its closed service failure.
#[derive(Debug, Error)]
#[error("installed operations {stage} failed")]
pub(crate) struct InstalledOperationsCompositionError {
    stage: &'static str,
    #[source]
    source: ServiceError,
}

impl InstalledOperationsCompositionError {
    const fn new(stage: &'static str, source: ServiceError) -> Self {
        Self { stage, source }
    }

    pub(crate) fn into_parts(self) -> (&'static str, ServiceError) {
        (self.stage, self.source)
    }
}

impl InstalledOperationsAuthorityBundle {
    /// Consumes the bundle into the exact three trait capabilities used by service composition.
    pub(crate) fn into_parts(self) -> InstalledOperationsAuthorityParts {
        (self.backup, self.recovery, self.update)
    }
}

impl fmt::Debug for InstalledOperationsAuthorityBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledOperationsAuthorityBundle")
            .field("backup", &"[INSTALLED BACKUP AUTHORITY]")
            .field("recovery", &"[INSTALLED RECOVERY AUTHORITY]")
            .field("update", &"[TRUSTED UPDATE AUTHORITY]")
            .finish()
    }
}

/// Constructs, reconciles, and seals all installed lifecycle-operation authorities.
pub(crate) async fn try_compose_installed_operations(
    inputs: InstalledOperationsAuthorityInputs,
) -> Result<InstalledOperationsAuthorityBundle, InstalledOperationsCompositionError> {
    let backup = Arc::new(
        InstalledManagedBackupOperations::try_new_with_repository(
            inputs.backup.analytical,
            inputs.backup.inventory,
            inputs.backup.repository,
            inputs.backup.installation_id,
            ManagedBackupComponentSource::new(inputs.backup.workspace.snapshot),
            inputs.backup.limits,
            inputs.backup.maximum_pending,
        )
        .map_err(|source| {
            InstalledOperationsCompositionError::new("backup construction", source)
        })?,
    );
    backup
        .recover(
            &inputs.backup.startup_cancellation,
            inputs.backup.startup_deadline,
        )
        .await
        .map_err(|source| InstalledOperationsCompositionError::new("backup recovery", source))?;

    let recovery = Arc::new(
        InstalledRecoveryOperations::try_new(
            inputs.recovery.backup_repository_root,
            inputs.recovery.workspace_repository_root,
            inputs.recovery.backup_limits,
            inputs.recovery.minimum_schema_version,
            inputs.recovery.maximum_schema_version,
            inputs.recovery.install_root,
            inputs.recovery.workspaces,
            inputs.recovery.lifecycle,
            inputs.recovery.transition,
            inputs.backup.workspace.restore,
            inputs.recovery.activity,
            inputs.recovery.durable,
        )
        .map_err(|source| {
            InstalledOperationsCompositionError::new("recovery construction", source)
        })?,
    );

    Ok(InstalledOperationsAuthorityBundle {
        backup,
        recovery,
        update: inputs.update,
    })
}

/// Closed trusted-update repository inputs admitted by the immutable package loader.
pub(crate) struct AvailableUpdateAuthorityInputs {
    pub(crate) install_root: PathBuf,
    pub(crate) staging_root: PathBuf,
    pub(crate) state: LocalAuthorityStateStore,
    pub(crate) base_url: Url,
    pub(crate) pinned_root: Box<[u8]>,
    pub(crate) manifest_target_path: Box<str>,
    pub(crate) archive_target_path: Box<str>,
    pub(crate) lifecycle: Arc<TrustedUpdateAuthority>,
    pub(crate) maximum_bundle_bytes: u64,
    pub(crate) maximum_prepared_plans: usize,
    pub(crate) request_timeout: Duration,
    pub(crate) activation_timeout: Duration,
    pub(crate) activity: Arc<UpdateActivityReader>,
    pub(crate) drain: Arc<UpdateDrainAuthority>,
}

impl fmt::Debug for AvailableUpdateAuthorityInputs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AvailableUpdateAuthorityInputs")
            .field("install_root", &"[INSTALL ROOT]")
            .field("staging_root", &"[STAGING ROOT]")
            .field("base_url", &self.base_url)
            .field("pinned_root", &"[VERIFIED PUBLIC TRUST ROOT]")
            .field("manifest_target_path", &self.manifest_target_path)
            .field("archive_target_path", &self.archive_target_path)
            .field("maximum_bundle_bytes", &self.maximum_bundle_bytes)
            .field("maximum_prepared_plans", &self.maximum_prepared_plans)
            .field("request_timeout", &self.request_timeout)
            .field("activation_timeout", &self.activation_timeout)
            .finish_non_exhaustive()
    }
}

/// Constructs the network-capable update authority only from package-admitted trust material.
pub(crate) fn try_compose_available_update_authority(
    inputs: AvailableUpdateAuthorityInputs,
) -> Result<Arc<dyn ManagedUpdateOperationsContract>, ServiceError> {
    let repository = TrustedUpdateRepository::try_new(
        inputs.base_url,
        &inputs.pinned_root,
        inputs.manifest_target_path,
        inputs.archive_target_path,
    )?;
    let limits = ManagedUpdateLimits::try_new(
        inputs.maximum_bundle_bytes,
        inputs.maximum_prepared_plans,
        inputs.request_timeout,
        inputs.activation_timeout,
    )?;
    let activity = inputs.activity;
    let drain = inputs.drain;
    let operations = ManagedUpdateOperations::try_new(
        inputs.install_root,
        inputs.staging_root,
        inputs.state,
        repository,
        inputs.lifecycle,
        limits,
        move |bundle_bytes| activity(bundle_bytes),
        move |cancellation, deadline| drain(cancellation, deadline),
    )?;
    Ok(Arc::new(operations))
}

/// Constructs the truthful fail-closed update authority for a valid package without a channel.
pub(crate) fn try_compose_unavailable_update_authority(
    availability: UpdateAvailabilityEvidence,
    current_version: impl Into<Box<str>>,
) -> Result<Arc<dyn ManagedUpdateOperationsContract>, ServiceError> {
    UnavailableUpdateOperations::try_new(availability, current_version)
        .map(|operations| Arc::new(operations) as Arc<dyn ManagedUpdateOperationsContract>)
}
