//! Concrete installed-product Operations authorities.

mod backup;
mod composition;
mod configuration_backup;
mod decision_backup;
mod fair_value_backup;
mod jobs_backup;
mod log_artifact;
mod model_backup;
mod portfolio_backup;
mod provider_metadata_backup;
mod recovery;
mod settings;
mod source_data_backup;
mod update;
mod update_journal;
mod workspace_backup;
mod workspace_restore;

pub(crate) use backup::ManagedBackupRepository;
pub(crate) use composition::{
    AvailableUpdateAuthorityInputs, InstalledBackupAuthorityInputs,
    InstalledOperationsAuthorityInputs, InstalledRecoveryAuthorityInputs, UpdateDrainFuture,
    WorkspaceBackupAuthorities, try_compose_available_update_authority,
    try_compose_installed_operations, try_compose_installed_workspace_backup,
    try_compose_unavailable_update_authority,
};
pub(crate) use log_artifact::ControlledDiagnosticArtifactPublisher;
pub(crate) use recovery::{
    DurableRecoveryState, InstalledServiceRecoveryHooks, RecoveryActivityAuthority,
    RecoveryWorkspaceSelectionAuthority, SupervisorRestartWorkspaceTransition,
    WorkspaceRecoveryDisposition,
};
pub(crate) use settings::{
    ProductionSettingsOperations, SettingsApplicationProof, SettingsLifecycleAuthority,
    SettingsRestartHandoff, SettingsStartupReconciliation,
};
pub(crate) use update_journal::DurableUpdateJournal;
pub(crate) use workspace_restore::{
    InstalledFreshWorkspaceRestoreAuthority, InstalledWorkspaceBackupBundleSource,
    ManagedWorkspaceRestoreAuthority, PreparedFreshWorkspace, WorkspaceRestorePolicy,
};
