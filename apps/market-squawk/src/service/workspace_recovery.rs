//! Bridge from installed recovery operations to the sole workspace selector authority.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use market_squawk_runtime::WorkspaceId;
use tokio_util::sync::CancellationToken;

use crate::{
    application::{
        backup::ProductBackupError,
        lifecycle::{LifecycleError, WorkspaceRuntimeIdentity},
        workspace::{WorkspaceDescriptor, WorkspaceHealth},
    },
    local_product::operations::{
        DurableRecoveryState, ManagedWorkspaceRestoreAuthority, PreparedFreshWorkspace,
        RecoveryWorkspaceSelectionAuthority, WorkspaceRecoveryDisposition,
    },
};

use super::workspace_selector::{
    WorkspaceHandoffPhase, WorkspaceSelector, WorkspaceSelectorError, WorkspaceStartupSelection,
};

/// Sole bridge for selector handoffs, recovery receipts, and fresh inactive workspace roots.
pub(super) struct WorkspaceRecoveryBridge {
    selector: Arc<WorkspaceSelector>,
    recovery: Arc<DurableRecoveryState>,
}

impl WorkspaceRecoveryBridge {
    /// Binds selector authority to the operation journal without creating another selector.
    #[must_use]
    pub(super) fn new(
        selector: Arc<WorkspaceSelector>,
        recovery: Arc<DurableRecoveryState>,
    ) -> Self {
        Self { selector, recovery }
    }

    /// Returns the selector-validated managed workspace container.
    pub(super) fn workspace_repository_root(&self) -> &std::path::Path {
        self.selector.workspace_repository_root()
    }

    /// Finalizes a healthy selector startup and its exact recovery receipt before publication.
    ///
    /// Re-recording the selector handoff repairs a crash after selector staging but before the
    /// recovery journal recorded the correlation. Completing from retained recovery evidence also
    /// repairs a crash after selector finalization but before its operation receipt committed.
    pub(super) fn finalize_startup(
        &self,
        selection: &WorkspaceStartupSelection,
    ) -> Result<(), LifecycleError> {
        if let Some(handoff) = selection.handoff() {
            self.record_handoff(handoff)?;
            self.selector
                .finalize_startup(selection)
                .map_err(map_selector_lifecycle)?;
            return self
                .recovery
                .complete_workspace_handoff(handoff.handoff_id().as_uuid(), selection.identity())
                .map_err(|_error| LifecycleError::AuthorityUnavailable);
        }

        self.selector
            .finalize_startup(selection)
            .map_err(map_selector_lifecycle)?;
        let Some(pending) = self
            .recovery
            .pending_workspace_handoff()
            .map_err(|_error| LifecycleError::AuthorityUnavailable)?
        else {
            return Ok(());
        };
        if selection.identity().workspace_id() != pending.candidate().workspace_id()
            || selection.identity().generation().get() < pending.candidate().generation().get()
        {
            return Err(LifecycleError::InvalidRestartHandoff);
        }
        self.recovery
            .complete_workspace_handoff(pending.handoff_id(), selection.identity())
            .map_err(|_error| LifecycleError::AuthorityUnavailable)
    }

    fn record_handoff(
        &self,
        handoff: super::workspace_selector::WorkspaceSupervisorHandoff,
    ) -> Result<(), LifecycleError> {
        let disposition = match handoff.phase() {
            WorkspaceHandoffPhase::Activate => WorkspaceRecoveryDisposition::Activated,
            WorkspaceHandoffPhase::Rollback => WorkspaceRecoveryDisposition::RolledBack,
        };
        self.recovery
            .record_workspace_handoff(
                handoff.handoff_id().as_uuid(),
                handoff.previous(),
                handoff.attempted(),
                handoff.candidate(),
                disposition,
            )
            .map_err(|_error| LifecycleError::AuthorityUnavailable)
    }
}

impl RecoveryWorkspaceSelectionAuthority for WorkspaceRecoveryBridge {
    fn stage_activation(
        &self,
        expected_active: WorkspaceRuntimeIdentity,
        target: WorkspaceId,
    ) -> Result<WorkspaceRuntimeIdentity, LifecycleError> {
        let handoff = self
            .selector
            .stage_activation(expected_active, target)
            .map_err(map_selector_lifecycle)?;
        self.record_handoff(handoff)?;
        Ok(handoff.candidate())
    }

    fn has_pending_handoff(&self) -> Result<bool, LifecycleError> {
        self.selector
            .has_pending_handoff()
            .map_err(map_selector_lifecycle)
    }
}

#[async_trait]
impl ManagedWorkspaceRestoreAuthority for WorkspaceRecoveryBridge {
    async fn prepare_fresh(
        &self,
        source_workspace: WorkspaceId,
        active_workspace: WorkspaceId,
        cancellation: &CancellationToken,
    ) -> Result<PreparedFreshWorkspace, ProductBackupError> {
        if cancellation.is_cancelled() || source_workspace == active_workspace {
            return if cancellation.is_cancelled() {
                Err(ProductBackupError::Cancelled)
            } else {
                Err(ProductBackupError::InvalidRestoreTarget)
            };
        }
        let selected = self
            .selector
            .active_identity()
            .map_err(map_selector_restore)?;
        if selected.workspace_id() != active_workspace {
            return Err(ProductBackupError::InvalidRestoreTarget);
        }
        let (workspace_id, paths) = self
            .selector
            .prepare_fresh_managed_workspace()
            .map_err(map_selector_restore)?;
        if cancellation.is_cancelled()
            || workspace_id == source_workspace
            || workspace_id == active_workspace
        {
            drop(paths);
            self.selector
                .abandon_managed_workspace(workspace_id)
                .map_err(map_selector_restore)?;
            return if cancellation.is_cancelled() {
                Err(ProductBackupError::Cancelled)
            } else {
                Err(ProductBackupError::InvalidRestoreTarget)
            };
        }
        let display_name = format!(
            "Restored workspace {}",
            &workspace_id.as_uuid().to_string()[..8]
        );
        let descriptor = match WorkspaceDescriptor::try_new(
            workspace_id,
            display_name,
            1,
            WorkspaceHealth::Prepared,
            0,
        ) {
            Ok(descriptor) => descriptor,
            Err(_error) => {
                drop(paths);
                self.selector
                    .abandon_managed_workspace(workspace_id)
                    .map_err(map_selector_restore)?;
                return Err(ProductBackupError::InvalidRestoreTarget);
            }
        };
        PreparedFreshWorkspace::try_new(descriptor, paths)
    }

    async fn abandon(&self, workspace_id: WorkspaceId) -> Result<(), ProductBackupError> {
        self.selector
            .abandon_managed_workspace(workspace_id)
            .map_err(map_selector_restore)
    }
}

impl fmt::Debug for WorkspaceRecoveryBridge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WorkspaceRecoveryBridge([SOLE SELECTOR AND RECOVERY EVIDENCE])")
    }
}

fn map_selector_lifecycle(error: WorkspaceSelectorError) -> LifecycleError {
    match error {
        WorkspaceSelectorError::GenerationExhausted => LifecycleError::GenerationExhausted,
        WorkspaceSelectorError::SelectionConflict => LifecycleError::InvalidTarget,
        WorkspaceSelectorError::HandoffConflict => LifecycleError::InvalidRestartHandoff,
        _ => LifecycleError::AuthorityUnavailable,
    }
}

fn map_selector_restore(error: WorkspaceSelectorError) -> ProductBackupError {
    match error {
        WorkspaceSelectorError::WorkspaceConflict
        | WorkspaceSelectorError::ActiveWorkspaceRemoval
        | WorkspaceSelectorError::SelectionConflict => ProductBackupError::InvalidRestoreTarget,
        _ => ProductBackupError::RestoreComponents,
    }
}
