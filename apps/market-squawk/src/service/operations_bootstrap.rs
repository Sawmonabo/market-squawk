//! Closed construction of the installed Operations authority graph.

use std::{
    collections::BTreeSet,
    fmt,
    num::NonZeroUsize,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use market_squawk_data::AnalyticalBackupLimits;
use market_squawk_jobs::JobRunner as _;
use market_squawk_platform::{LocalAuthorityStateStore, LocalPaths};
use market_squawk_runtime::{RuntimeIdentity, ServiceGeneration};
use market_squawk_services::ServiceError;
use tokio_util::sync::CancellationToken;

use super::{
    InstalledServiceError,
    lifecycle::InstalledServiceLifecycle,
    operations_activity::{RuntimeActivityCoordinator, RuntimeActivityLimits},
    operations_composition::{
        OperationsApplicationDependencies, PendingOperationsComposition, ReadyOperationsComposition,
    },
    update_package::{InstalledUpdatePackage, InstalledUpdateUnavailable},
    workspace_recovery::WorkspaceRecoveryBridge,
    workspace_selector::{WorkspaceSelector, WorkspaceStartupSelection},
};
use crate::{
    AppConfig, LocalProduct,
    application::{
        Application,
        backup::ProductBackupInventory,
        lifecycle::{
            LifecycleError, TrustedUpdateAuthority, UpdateError, WorkspaceLifecycleAuthority,
        },
        logs::DiagnosticArtifactPublisher,
        operations::{OperationsApplicationServices, UpdateAvailabilityEvidence},
        settings::{
            DurableSettingsStore, SettingKey, SettingValue, SettingsSeed, SettingsSnapshot,
        },
        setup::SetupPlanAuthority,
        workspace::{DurableWorkspaceRegistry, WorkspaceDescriptor, WorkspaceHealth},
    },
    jobs::{InstalledJobAuthority, InstalledJobRunners},
    local_product::operations::{
        AvailableUpdateAuthorityInputs, ControlledDiagnosticArtifactPublisher,
        DurableRecoveryState, DurableUpdateJournal, InstalledBackupAuthorityInputs,
        InstalledFreshWorkspaceRestoreAuthority, InstalledOperationsAuthorityInputs,
        InstalledRecoveryAuthorityInputs, InstalledServiceRecoveryHooks,
        InstalledWorkspaceBackupBundleSource, ManagedBackupRepository,
        ProductionSettingsOperations, SettingsApplicationProof, SettingsLifecycleAuthority,
        SettingsRestartHandoff, SettingsStartupReconciliation,
        SupervisorRestartWorkspaceTransition, WorkspaceBackupAuthorities,
        try_compose_available_update_authority, try_compose_installed_operations,
        try_compose_installed_workspace_backup, try_compose_unavailable_update_authority,
    },
};

const MAXIMUM_BACKUP_ARTIFACTS: usize = 100_000;
const MAXIMUM_BACKUP_REFERENCES: usize = 400_000;
const MAXIMUM_BACKUP_BYTES: u64 = 16 * 1024 * 1024 * 1024 * 1024;
const MAXIMUM_BACKUP_OBJECT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAXIMUM_PARQUET_METADATA_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_PENDING_OPERATIONS: usize = 256;
const WORKSPACE_SCHEMA_VERSION: u32 = 1;
const MAXIMUM_DIAGNOSTIC_BYTES: usize = 64 * 1024 * 1024;
const MAXIMUM_DIAGNOSTIC_RECORDS: usize = 100_000;
const UPDATE_STAGING_DIRECTORY: &str = "updates/staging";
const UPDATE_STATE_DIRECTORY: &str = "updates/state";

/// Authorities composed before lifecycle runners can be constructed.
pub(super) struct PreparedInstalledOperations {
    pending: PendingOperationsComposition,
    backups: Arc<ProductBackupInventory>,
    workspaces: Arc<DurableWorkspaceRegistry>,
    workspace_lifecycle: Arc<WorkspaceLifecycleAuthority>,
    update_lifecycle: Arc<TrustedUpdateAuthority>,
    settings_seed: SettingsSeed,
    settings_lifecycle: SettingsLifecycleAuthority,
    settings_operations: Arc<ProductionSettingsOperations>,
    activity: Arc<RuntimeActivityCoordinator>,
    recovery: Arc<DurableRecoveryState>,
    recovery_bridge: Arc<WorkspaceRecoveryBridge>,
    service_lifecycle: Arc<InstalledServiceLifecycle>,
    workspace_paths: LocalPaths,
    installation_paths: LocalPaths,
    selection: WorkspaceStartupSelection,
    backup_limits: AnalyticalBackupLimits,
}

/// Publishable Operations graph and the readers needed before transport publication.
pub(super) struct ReadyInstalledOperations {
    composition: ReadyOperationsComposition,
    activity: Arc<RuntimeActivityCoordinator>,
    workspaces: Arc<DurableWorkspaceRegistry>,
    recovery_bridge: Arc<WorkspaceRecoveryBridge>,
    settings_operations: Arc<ProductionSettingsOperations>,
}

impl ReadyInstalledOperations {
    pub(super) fn application(&self) -> Arc<OperationsApplicationServices> {
        self.composition.application()
    }

    pub(super) fn activity(&self) -> Arc<RuntimeActivityCoordinator> {
        Arc::clone(&self.activity)
    }

    pub(super) fn workspaces(&self) -> Arc<DurableWorkspaceRegistry> {
        Arc::clone(&self.workspaces)
    }

    pub(super) fn recovery_bridge(&self) -> Arc<WorkspaceRecoveryBridge> {
        Arc::clone(&self.recovery_bridge)
    }

    pub(super) fn settings_operations(&self) -> Arc<ProductionSettingsOperations> {
        Arc::clone(&self.settings_operations)
    }

    pub(super) fn reconcile_settings_startup(&self) -> Result<(), InstalledServiceError> {
        match self.settings_operations.reconcile_successor_startup() {
            Ok(SettingsStartupReconciliation::Ready) => Ok(()),
            Ok(SettingsStartupReconciliation::RollbackRestartRequired(handoff)) => {
                self.settings_operations
                    .signal_restart_handoff(handoff)
                    .map_err(|_error| InstalledServiceError::InvalidComposition)?;
                Err(InstalledServiceError::ReadinessFailed)
            }
            Err(_error) => Err(InstalledServiceError::InvalidComposition),
        }
    }
}

impl PreparedInstalledOperations {
    #[allow(
        clippy::too_many_arguments,
        reason = "the composition root must receive every installation-owned authority explicitly"
    )]
    pub(super) fn prepare(
        config: &AppConfig,
        installation_paths: &LocalPaths,
        workspace_paths: &LocalPaths,
        selection: WorkspaceStartupSelection,
        product: &LocalProduct,
        service_lifecycle: Arc<InstalledServiceLifecycle>,
        selector: Arc<WorkspaceSelector>,
        logs: Arc<crate::application::logs::StructuredLogStore>,
    ) -> Result<Self, InstalledServiceError> {
        let control_root = workspace_paths.control_root()?;
        let control_path = control_root.root();
        let settings_seed = SettingsSeed::from_config(config)
            .map_err(|_error| InstalledServiceError::CompositionStage("settings seed"))?;
        let settings = Arc::new(
            DurableSettingsStore::try_open(control_path, settings_seed.clone())
                .map_err(|_error| InstalledServiceError::CompositionStage("settings store"))?,
        );
        let consumers = Arc::new(InstalledSettingsConsumers::default());
        let reload_consumers = Arc::clone(&consumers);
        let startup_consumers = Arc::clone(&consumers);
        let lifecycle_for_settings = Arc::clone(&service_lifecycle);
        let current_runtime = selection
            .identity()
            .to_runtime(service_lifecycle.current().installation_id())
            .map_err(|_error| InstalledServiceError::CompositionStage("settings runtime"))?;
        let settings_lifecycle = SettingsLifecycleAuthority::try_new(
            SettingKey::all(),
            Arc::new(move |snapshot| reload_consumers.apply(snapshot)),
            Arc::new(move |handoff| {
                request_settings_restart(&lifecycle_for_settings, current_runtime, handoff)
            }),
            Arc::new(move |snapshot| startup_consumers.apply(snapshot)),
        )
        .map_err(|_error| InstalledServiceError::CompositionStage("settings lifecycle"))?;
        let settings_operations = Arc::new(
            ProductionSettingsOperations::try_new(
                control_path,
                Arc::clone(&settings),
                settings_lifecycle.clone(),
            )
            .map_err(|_error| InstalledServiceError::CompositionStage("settings operations"))?,
        );

        let backups = Arc::new(
            ProductBackupInventory::try_open(control_path)
                .map_err(|_error| InstalledServiceError::CompositionStage("backup inventory"))?,
        );
        let descriptor = WorkspaceDescriptor::try_new(
            selection.identity().workspace_id(),
            "Primary workspace",
            WORKSPACE_SCHEMA_VERSION,
            WorkspaceHealth::Healthy,
            0,
        )
        .map_err(|_error| InstalledServiceError::CompositionStage("workspace descriptor"))?;
        let workspaces = Arc::new(
            DurableWorkspaceRegistry::try_open(control_path, selection.identity(), descriptor)
                .map_err(|_error| InstalledServiceError::CompositionStage("workspace registry"))?,
        );
        let active_workspace = workspaces
            .active()
            .map_err(|_error| InstalledServiceError::CompositionStage("workspace registry"))?;
        if active_workspace != selection.identity() {
            if selection.handoff().is_some() {
                return Err(InstalledServiceError::InvalidComposition);
            }
            workspaces
                .reconcile_ordinary_startup(selection.identity())
                .map_err(|_error| {
                    InstalledServiceError::CompositionStage("workspace registry startup fence")
                })?;
        }
        let workspace_journal = workspaces.clone();
        let workspace_lifecycle = Arc::new(
            WorkspaceLifecycleAuthority::try_new(selection.identity(), workspace_journal)
                .map_err(|_error| InstalledServiceError::CompositionStage("workspace lifecycle"))?,
        );

        let update_journal = Arc::new(
            DurableUpdateJournal::try_open(control_path)
                .map_err(|_error| InstalledServiceError::CompositionStage("update journal"))?,
        );
        let program_generation = update_journal
            .current_generation()
            .map_err(|_error| InstalledServiceError::CompositionStage("update journal"))?;
        let update_lifecycle = Arc::new(TrustedUpdateAuthority::new(
            program_generation,
            update_journal,
        ));
        let recovery = Arc::new(
            DurableRecoveryState::try_open(control_path, program_generation)
                .map_err(|_error| InstalledServiceError::CompositionStage("recovery state"))?,
        );
        let recovery_bridge = Arc::new(WorkspaceRecoveryBridge::new(
            selector,
            Arc::clone(&recovery),
        ));

        let log_artifacts: Arc<dyn DiagnosticArtifactPublisher> =
            Arc::new(ControlledDiagnosticArtifactPublisher::new(
                product.controlled_artifacts(),
                NonZeroUsize::new(MAXIMUM_DIAGNOSTIC_BYTES)
                    .ok_or(InstalledServiceError::InvalidComposition)?,
                NonZeroUsize::new(MAXIMUM_DIAGNOSTIC_RECORDS)
                    .ok_or(InstalledServiceError::InvalidComposition)?,
            ));
        let setup = Arc::new(
            SetupPlanAuthority::try_open(control_path, selection.identity().workspace_id())
                .map_err(|_error| InstalledServiceError::CompositionStage("setup plan"))?,
        );
        let activity = Arc::new(RuntimeActivityCoordinator::new(
            RuntimeActivityLimits::try_new(
                4_096,
                4_096,
                16_384,
                u64::MAX,
                MAXIMUM_BACKUP_BYTES,
                u32::MAX,
            )
            .map_err(|_error| InstalledServiceError::CompositionStage("activity coordinator"))?,
        ));
        let pending = PendingOperationsComposition::new(OperationsApplicationDependencies {
            backups: Arc::clone(&backups),
            workspaces: Arc::clone(&workspaces),
            workspace_lifecycle: Arc::clone(&workspace_lifecycle),
            activity: Arc::clone(&activity),
            updates: Arc::clone(&update_lifecycle),
            logs,
            log_artifacts,
            settings,
            settings_operations: settings_operations.clone(),
            setup,
        });
        let backup_limits = AnalyticalBackupLimits::try_new(
            MAXIMUM_BACKUP_ARTIFACTS,
            MAXIMUM_BACKUP_REFERENCES,
            MAXIMUM_BACKUP_BYTES,
            MAXIMUM_BACKUP_OBJECT_BYTES,
            MAXIMUM_PARQUET_METADATA_BYTES,
        )
        .map_err(|_error| InstalledServiceError::CompositionStage("backup limits"))?;
        Ok(Self {
            pending,
            backups,
            workspaces,
            workspace_lifecycle,
            update_lifecycle,
            settings_seed,
            settings_lifecycle,
            settings_operations,
            activity,
            recovery,
            recovery_bridge,
            service_lifecycle,
            workspace_paths: workspace_paths.clone(),
            installation_paths: installation_paths.clone(),
            selection,
            backup_limits,
        })
    }

    pub(super) fn application_for_job_runners(&self) -> Arc<OperationsApplicationServices> {
        self.pending.application_for_job_runners()
    }

    pub(super) async fn bind(
        self,
        product: &LocalProduct,
        jobs: &InstalledJobAuthority,
        runners: &InstalledJobRunners,
    ) -> Result<ReadyInstalledOperations, InstalledServiceError> {
        let update_package = InstalledUpdatePackage::load().map_err(|_error| {
            InstalledServiceError::CompositionStage("installed update package")
        })?;
        let (minimum_schema_version, maximum_schema_version, update_install_root) =
            match &update_package {
                InstalledUpdatePackage::Available(package) => (
                    u32::try_from(package.minimum_workspace_schema_version()).map_err(
                        |_error| InstalledServiceError::CompositionStage("update workspace schema"),
                    )?,
                    u32::try_from(package.maximum_workspace_schema_version()).map_err(
                        |_error| InstalledServiceError::CompositionStage("update workspace schema"),
                    )?,
                    Some(package.install_root().to_path_buf()),
                ),
                InstalledUpdatePackage::Unavailable(_) => {
                    (WORKSPACE_SCHEMA_VERSION, WORKSPACE_SCHEMA_VERSION, None)
                }
            };
        let control_root = self.workspace_paths.control_root()?;
        let repository = Arc::new(ManagedBackupRepository::try_open(control_root).map_err(
            |_error| InstalledServiceError::CompositionStage("managed backup repository"),
        )?);
        let bundles = Arc::new(InstalledWorkspaceBackupBundleSource::new(Arc::clone(
            &repository,
        )));
        let restore_policy = product.workspace_restore_policy(
            self.settings_seed,
            self.settings_lifecycle,
            InstalledJobAuthority::repository_config()?,
        )?;
        let restore_owner: Arc<
            dyn crate::local_product::operations::ManagedWorkspaceRestoreAuthority,
        > = self.recovery_bridge.clone();
        let restore = Arc::new(InstalledFreshWorkspaceRestoreAuthority::new(
            restore_owner,
            restore_policy,
        ));
        let workspace_backup = try_compose_installed_workspace_backup(
            product,
            Arc::clone(&self.settings_operations),
            jobs,
            runners.backup(),
            bundles,
            restore,
            self.selection.identity().workspace_id(),
        )
        .map_err(|_error| {
            InstalledServiceError::CompositionStage("workspace backup authorities")
        })?;
        let backup_authorities = WorkspaceBackupAuthorities::from_installed(workspace_backup);

        let recovery_hooks = Arc::new(InstalledRecoveryRuntimeHooks::new(
            product.application(),
            jobs.authority(),
            Arc::clone(&self.service_lifecycle),
            runners.recovery().kind().clone(),
        ));
        let selection_authority: Arc<
            dyn crate::local_product::operations::RecoveryWorkspaceSelectionAuthority,
        > = self.recovery_bridge.clone();
        let recovery_hooks: Arc<dyn InstalledServiceRecoveryHooks> = recovery_hooks;
        let transition = Arc::new(SupervisorRestartWorkspaceTransition::new(
            selection_authority,
            recovery_hooks,
            self.service_lifecycle.current().installation_id(),
            self.selection.identity(),
        ));
        let activity_authority: Arc<
            dyn crate::local_product::operations::RecoveryActivityAuthority,
        > = self.activity.clone();
        let managed_update = compose_update_authority(
            update_package,
            &self.installation_paths,
            self.selection.identity().workspace_id(),
            Arc::clone(&self.update_lifecycle),
            Arc::clone(&self.activity),
            Arc::new(InstalledRecoveryRuntimeHooks::new(
                product.application(),
                jobs.authority(),
                Arc::clone(&self.service_lifecycle),
                runners.update().kind().clone(),
            )),
        )
        .map_err(|error| super::composition_stage(error, "managed update authority"))?;
        let analytical = Arc::new(product.research().analytical().backup_service());
        let installed = try_compose_installed_operations(InstalledOperationsAuthorityInputs {
            backup: InstalledBackupAuthorityInputs {
                analytical,
                inventory: Arc::clone(&self.backups),
                repository: Arc::clone(&repository),
                installation_id: self.service_lifecycle.current().installation_id(),
                workspace: backup_authorities,
                limits: self.backup_limits,
                maximum_pending: MAXIMUM_PENDING_OPERATIONS,
                startup_cancellation: CancellationToken::new(),
                startup_deadline: Instant::now()
                    .checked_add(Duration::from_secs(5 * 60))
                    .ok_or(InstalledServiceError::InvalidComposition)?,
            },
            recovery: InstalledRecoveryAuthorityInputs {
                backup_repository_root: repository.root().to_path_buf(),
                workspace_repository_root: self
                    .recovery_bridge
                    .workspace_repository_root()
                    .to_path_buf(),
                backup_limits: self.backup_limits,
                minimum_schema_version,
                maximum_schema_version,
                install_root: update_install_root,
                workspaces: Arc::clone(&self.workspaces),
                lifecycle: Arc::clone(&self.workspace_lifecycle),
                transition,
                activity: activity_authority,
                durable: Arc::clone(&self.recovery),
            },
            update: managed_update,
        })
        .await
        .map_err(|error| {
            let (stage, source) = error.into_parts();
            InstalledServiceError::OperationsAuthority { stage, source }
        })?;
        let (backup, recovery, update) = installed.into_parts();
        let composition = self
            .pending
            .bind(backup, recovery, update)
            .map_err(|_error| {
                InstalledServiceError::CompositionStage("operations application binding")
            })?;
        Ok(ReadyInstalledOperations {
            composition,
            activity: self.activity,
            workspaces: self.workspaces,
            recovery_bridge: self.recovery_bridge,
            settings_operations: self.settings_operations,
        })
    }
}

#[derive(Default)]
struct InstalledSettingsConsumers {
    applied: Mutex<Option<std::collections::BTreeMap<SettingKey, SettingValue>>>,
}

impl InstalledSettingsConsumers {
    fn apply(&self, snapshot: &SettingsSnapshot) -> Result<SettingsApplicationProof, ServiceError> {
        let values = snapshot
            .entries()
            .iter()
            .map(|entry| (entry.key(), entry.value().clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        let keys = values.keys().copied().collect::<BTreeSet<_>>();
        if values.len() != SettingKey::all().len()
            || !SettingKey::all().into_iter().all(|key| keys.contains(&key))
        {
            return Err(ServiceError::InvalidResult);
        }
        *self.applied.lock().map_err(|_| ServiceError::Unavailable)? = Some(values);
        Ok(SettingsApplicationProof::for_snapshot(snapshot))
    }
}

fn request_settings_restart(
    lifecycle: &InstalledServiceLifecycle,
    current: RuntimeIdentity,
    handoff: SettingsRestartHandoff,
) -> Result<(), ServiceError> {
    if handoff.revision() == 0 || handoff.digest() == [0; 32] {
        return Err(ServiceError::InvalidRequest);
    }
    let generation = current
        .service_generation()
        .get()
        .checked_add(1)
        .and_then(|value| ServiceGeneration::try_new(value).ok())
        .ok_or(ServiceError::ResourceExhausted)?;
    let expected = RuntimeIdentity::try_new(
        current.installation_id(),
        current.workspace_id(),
        generation,
    )
    .map_err(|_| ServiceError::Internal)?;
    lifecycle
        .request_restart(expected)
        .map_err(|_| ServiceError::Unavailable)
}

struct InstalledRecoveryRuntimeHooks {
    application: Weak<Application>,
    jobs: Weak<market_squawk_jobs::JobAuthority<market_squawk_jobs::SqliteJobRepository>>,
    lifecycle: Weak<InstalledServiceLifecycle>,
    active_job_kind: market_squawk_domain::SourceIdentifier,
}

impl InstalledRecoveryRuntimeHooks {
    fn new(
        application: Arc<Application>,
        jobs: Arc<market_squawk_jobs::JobAuthority<market_squawk_jobs::SqliteJobRepository>>,
        lifecycle: Arc<InstalledServiceLifecycle>,
        active_job_kind: market_squawk_domain::SourceIdentifier,
    ) -> Self {
        Self {
            application: Arc::downgrade(&application),
            jobs: Arc::downgrade(&jobs),
            lifecycle: Arc::downgrade(&lifecycle),
            active_job_kind,
        }
    }
}

#[async_trait]
impl InstalledServiceRecoveryHooks for InstalledRecoveryRuntimeHooks {
    async fn drain_and_reconcile(&self, deadline: Instant) -> Result<(), LifecycleError> {
        let application = self
            .application
            .upgrade()
            .ok_or(LifecycleError::AuthorityUnavailable)?;
        let jobs = self
            .jobs
            .upgrade()
            .ok_or(LifecycleError::AuthorityUnavailable)?;
        application.begin_shutdown();
        let _lifecycle_fence = jobs
            .retain_lifecycle_fence(&self.active_job_kind)
            .await
            .map_err(|_| LifecycleError::PreflightBlocked)?;
        if application.shutdown(deadline).await.is_complete() {
            Ok(())
        } else {
            Err(LifecycleError::AuthorityUnavailable)
        }
    }

    fn request_restart(&self, expected: RuntimeIdentity) -> Result<(), LifecycleError> {
        self.lifecycle
            .upgrade()
            .ok_or(LifecycleError::AuthorityUnavailable)?
            .request_restart(expected)
            .map_err(|_| LifecycleError::AuthorityUnavailable)
    }
}

impl fmt::Debug for InstalledRecoveryRuntimeHooks {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InstalledRecoveryRuntimeHooks([APPLICATION AND JOB DRAIN])")
    }
}

fn compose_update_authority(
    package: InstalledUpdatePackage,
    installation_paths: &LocalPaths,
    workspace_id: market_squawk_runtime::WorkspaceId,
    lifecycle: Arc<TrustedUpdateAuthority>,
    activity: Arc<RuntimeActivityCoordinator>,
    hooks: Arc<InstalledRecoveryRuntimeHooks>,
) -> Result<Arc<dyn crate::application::operations::ManagedUpdateOperations>, InstalledServiceError>
{
    match package {
        InstalledUpdatePackage::Unavailable(reason) => {
            let availability = match reason {
                InstalledUpdateUnavailable::SourceOrDevelopmentExecution => {
                    UpdateAvailabilityEvidence::SourceOrDevelopmentExecution
                }
                InstalledUpdateUnavailable::ProductionSigningMaterialUnavailable => {
                    UpdateAvailabilityEvidence::ProductionSigningMaterialUnavailable
                }
            };
            try_compose_unavailable_update_authority(availability, env!("CARGO_PKG_VERSION"))
                .map_err(|_error| InstalledServiceError::InvalidComposition)
        }
        InstalledUpdatePackage::Available(package) => {
            let repository = package.repository();
            let control = installation_paths.control_root()?;
            let state =
                LocalAuthorityStateStore::try_open(control.root().join(UPDATE_STATE_DIRECTORY))?;
            let staging_root = control.root().join(UPDATE_STAGING_DIRECTORY);
            let activity_reader = Arc::new(move |required_bytes| {
                activity.update_snapshot(workspace_id, required_bytes)
            });
            let drain = Arc::new(move |_cancellation: CancellationToken, deadline: Instant| {
                let hooks = Arc::clone(&hooks);
                Box::pin(async move {
                    hooks
                        .drain_and_reconcile(deadline)
                        .await
                        .map_err(|_| UpdateError::ActivationFailed)
                }) as crate::local_product::operations::UpdateDrainFuture
            });
            try_compose_available_update_authority(AvailableUpdateAuthorityInputs {
                install_root: package.install_root().to_path_buf(),
                staging_root,
                state,
                base_url: repository.base_url().clone(),
                pinned_root: repository.pinned_root().to_vec().into_boxed_slice(),
                manifest_target_path: repository.manifest_target_path().into(),
                archive_target_path: repository.archive_target_path().into(),
                lifecycle,
                maximum_bundle_bytes: 4 * 1024 * 1024 * 1024,
                maximum_prepared_plans: 64,
                request_timeout: Duration::from_secs(60),
                activation_timeout: Duration::from_secs(10 * 60),
                activity: activity_reader,
                drain,
            })
            .map_err(|_error| InstalledServiceError::InvalidComposition)
        }
    }
}
