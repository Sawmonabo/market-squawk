//! Concrete installed-product Operations authorities.

mod backup;
mod log_artifact;
mod recovery;
mod settings;
mod update;

pub(super) use backup::{InstalledManagedBackupOperations, ManagedBackupComponentSource};
pub(super) use log_artifact::ControlledDiagnosticArtifactPublisher;
pub(super) use recovery::{
    DurableRecoveryState, InstalledRecoveryOperations, RecoveryRuntimeActivity,
    RecoveryWorkspaceHandoff, SupervisorRestartWorkspaceTransition,
};
pub(super) use settings::{ProductionSettingsOperations, SettingsLifecycleAuthority};
pub(super) use update::ManagedUpdateOperations;
