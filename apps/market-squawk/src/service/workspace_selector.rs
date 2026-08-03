//! Installation-global workspace selection and supervisor handoff persistence.

use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use cap_std::{ambient_authority, fs::Dir};
use market_squawk_platform::{
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths, PathError,
};
use market_squawk_runtime::WorkspaceId;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::application::lifecycle::{LifecycleError, WorkspaceRuntimeIdentity};

const FORMAT_VERSION: u16 = 1;
const AUTHORITY_DIRECTORY: &str = "installed-workspace-selector";
const WORKSPACE_DIRECTORY: &str = "workspaces";
const MAXIMUM_WORKSPACES: usize = 64;

/// One-time seed admitted only from the native installation bootstrap boundary.
#[derive(Debug)]
pub(super) enum WorkspaceBootstrapSeed {
    /// A new installation whose first workspace uses the code-owned workspace container.
    Managed { workspace_id: WorkspaceId },
    /// A pre-selector workspace retained in place until a separate migration succeeds.
    Legacy {
        workspace_id: WorkspaceId,
        paths: LocalPaths,
    },
}

impl WorkspaceBootstrapSeed {
    /// Creates the first managed workspace without accepting an ambient path.
    #[must_use]
    pub(super) const fn managed(workspace_id: WorkspaceId) -> Self {
        Self::Managed { workspace_id }
    }

    /// Binds a legacy root that was already prepared from trusted native configuration.
    ///
    /// This constructor does not move or copy the workspace. The selector records an explicit
    /// migration-required placement until a separately verified migration activates a managed
    /// workspace.
    pub(super) fn try_from_legacy_paths(
        workspace_id: WorkspaceId,
        paths: &LocalPaths,
    ) -> Result<Self, WorkspaceSelectorError> {
        validate_prepared_paths(paths)?;
        Ok(Self::Legacy {
            workspace_id,
            paths: paths.clone(),
        })
    }

    const fn workspace_id(&self) -> WorkspaceId {
        match self {
            Self::Managed { workspace_id } | Self::Legacy { workspace_id, .. } => *workspace_id,
        }
    }
}

/// Durable placement classification for one registered workspace root.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WorkspacePlacement {
    /// Root is derived as `<installation-data-root>/workspaces/<workspace-id>`.
    Managed,
    /// Root came from trusted legacy configuration and still requires explicit migration.
    LegacyMigrationRequired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct WorkspaceRootRecord {
    root: PathBuf,
    placement: WorkspacePlacement,
}

/// Opaque identity for one exact cross-process supervisor handoff.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub(super) struct WorkspaceHandoffId(Uuid);

impl WorkspaceHandoffId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }

    #[must_use]
    pub(super) const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// Phase of a pending cross-process workspace transition.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WorkspaceHandoffPhase {
    /// The replacement process must start the selected target workspace.
    Activate,
    /// The replacement process must restore the prior workspace under a newer fence.
    Rollback,
}

/// Exact durable supervisor handoff consumed by replacement-process startup.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct WorkspaceSupervisorHandoff {
    handoff_id: WorkspaceHandoffId,
    previous: WorkspaceRuntimeIdentity,
    attempted: WorkspaceRuntimeIdentity,
    candidate: WorkspaceRuntimeIdentity,
    phase: WorkspaceHandoffPhase,
}

impl WorkspaceSupervisorHandoff {
    /// Exact handoff identity required by finalize and rollback calls.
    #[must_use]
    pub(super) const fn handoff_id(self) -> WorkspaceHandoffId {
        self.handoff_id
    }

    /// Workspace identity that the replacement process must compose.
    #[must_use]
    pub(super) const fn candidate(self) -> WorkspaceRuntimeIdentity {
        self.candidate
    }

    /// Active selection from which this transition began.
    #[must_use]
    pub(super) const fn previous(self) -> WorkspaceRuntimeIdentity {
        self.previous
    }

    /// First target attempted by this transition, retained across rollback.
    #[must_use]
    pub(super) const fn attempted(self) -> WorkspaceRuntimeIdentity {
        self.attempted
    }

    /// Current activation or rollback phase.
    #[must_use]
    pub(super) const fn phase(self) -> WorkspaceHandoffPhase {
        self.phase
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct WorkspaceSelectorDocument {
    format_version: u16,
    active: WorkspaceRuntimeIdentity,
    rollback: Option<WorkspaceRuntimeIdentity>,
    pending: Option<WorkspaceSupervisorHandoff>,
    recovery_required: bool,
    roots: BTreeMap<WorkspaceId, WorkspaceRootRecord>,
}

impl WorkspaceSelectorDocument {
    fn initial(active: WorkspaceRuntimeIdentity, root: WorkspaceRootRecord) -> Self {
        let mut roots = BTreeMap::new();
        roots.insert(active.workspace_id(), root);
        Self {
            format_version: FORMAT_VERSION,
            active,
            rollback: None,
            pending: None,
            recovery_required: false,
            roots,
        }
    }

    fn validate_structure(self) -> Result<Self, WorkspaceSelectorError> {
        if self.format_version != FORMAT_VERSION
            || self.roots.is_empty()
            || self.roots.len() > MAXIMUM_WORKSPACES
            || !self.roots.contains_key(&self.active.workspace_id())
            || self
                .rollback
                .is_some_and(|rollback| !self.roots.contains_key(&rollback.workspace_id()))
            || self.pending.as_ref().is_some_and(|pending| {
                pending.handoff_id.0.is_nil()
                    || pending.previous != self.active
                    || !self.roots.contains_key(&pending.candidate.workspace_id())
                    || !self.roots.contains_key(&pending.attempted.workspace_id())
                    || !valid_pending_generation(pending)
                    || match pending.phase {
                        WorkspaceHandoffPhase::Activate => {
                            pending.candidate != pending.attempted
                                || pending.candidate.workspace_id()
                                    == pending.previous.workspace_id()
                                || self.rollback != Some(pending.previous)
                        }
                        WorkspaceHandoffPhase::Rollback => {
                            pending.candidate.workspace_id() != pending.previous.workspace_id()
                                || pending.attempted.workspace_id()
                                    == pending.previous.workspace_id()
                                || self.rollback != Some(pending.previous)
                        }
                    }
            })
            || self.recovery_required
                && self
                    .pending
                    .as_ref()
                    .is_none_or(|pending| pending.phase != WorkspaceHandoffPhase::Rollback)
        {
            return Err(WorkspaceSelectorError::CorruptState);
        }
        Ok(self)
    }
}

/// Capability-bearing selection returned before workspace runtime preparation.
#[derive(Clone, Debug)]
pub(super) struct WorkspaceStartupSelection {
    identity: WorkspaceRuntimeIdentity,
    placement: WorkspacePlacement,
    paths: LocalPaths,
    handoff: Option<WorkspaceSupervisorHandoff>,
}

impl WorkspaceStartupSelection {
    /// Exact workspace and nonzero generation selected for this process.
    #[must_use]
    pub(super) const fn identity(&self) -> WorkspaceRuntimeIdentity {
        self.identity
    }

    /// Prepared capability for the selected workspace root.
    #[must_use]
    pub(super) const fn paths(&self) -> &LocalPaths {
        &self.paths
    }

    /// Placement state shown by setup until a legacy workspace is explicitly migrated.
    #[must_use]
    pub(super) const fn placement(&self) -> WorkspacePlacement {
        self.placement
    }

    /// Exact pending handoff, if this process is a replacement generation.
    #[must_use]
    pub(super) const fn handoff(&self) -> Option<WorkspaceSupervisorHandoff> {
        self.handoff
    }
}

/// Installation-global owner of the active workspace and one exact pending handoff.
pub(super) struct WorkspaceSelector {
    installation_root: PathBuf,
    workspace_container: PathBuf,
    workspace_directory: Arc<Dir>,
    store: LocalAuthorityStateStore,
    document: Mutex<WorkspaceSelectorDocument>,
}

impl WorkspaceSelector {
    /// Opens durable selection, creating a managed workspace unless legacy data already exists.
    pub(super) fn try_open_or_bootstrap(
        installation_paths: &LocalPaths,
        legacy_paths: &LocalPaths,
    ) -> Result<Self, WorkspaceSelectorError> {
        match Self::try_open(installation_paths, None) {
            Ok(selector) => Ok(selector),
            Err(WorkspaceSelectorError::BootstrapRequired) => {
                let workspace_id = WorkspaceId::try_from_uuid(Uuid::new_v4())
                    .map_err(|_error| WorkspaceSelectorError::InvalidWorkspaceIdentity)?;
                let seed = if legacy_workspace_contains_state(legacy_paths)? {
                    WorkspaceBootstrapSeed::try_from_legacy_paths(workspace_id, legacy_paths)?
                } else {
                    WorkspaceBootstrapSeed::managed(workspace_id)
                };
                Self::try_open(installation_paths, Some(seed))
            }
            Err(error) => Err(error),
        }
    }

    /// Opens or initializes the selector beneath the stable per-user installation data root.
    ///
    /// `bootstrap` is required only when no durable selector exists. A supplied seed on later
    /// opens must match its already-registered record exactly and cannot replace the active
    /// selection; callers may pass `None` after bootstrap completes.
    pub(super) fn try_open(
        installation_paths: &LocalPaths,
        bootstrap: Option<WorkspaceBootstrapSeed>,
    ) -> Result<Self, WorkspaceSelectorError> {
        validate_prepared_paths(installation_paths)?;
        let installation_root = installation_paths.root().to_path_buf();
        let (workspace_container, workspace_directory) =
            prepare_workspace_container(&installation_root)?;
        let control_root = installation_paths.control_root()?;
        control_root.try_clone_directory()?;
        let store =
            LocalAuthorityStateStore::try_open(control_root.root().join(AUTHORITY_DIRECTORY))?;
        let document = match store.load()? {
            Some(encoded) => {
                let document = decode(&encoded)?.validate_structure()?;
                validate_document_roots(&document, &installation_root, &workspace_container)?;
                if let Some(bootstrap) = &bootstrap {
                    validate_bootstrap_match(&document, bootstrap, &installation_root)?;
                }
                document
            }
            None => {
                let bootstrap = bootstrap.ok_or(WorkspaceSelectorError::BootstrapRequired)?;
                let identity = WorkspaceRuntimeIdentity::try_new(bootstrap.workspace_id(), 1)?;
                let root =
                    prepare_bootstrap_root(&installation_root, &workspace_container, bootstrap)?;
                let document = WorkspaceSelectorDocument::initial(identity, root);
                store_document(&store, &document)?;
                document
            }
        };
        Ok(Self {
            installation_root,
            workspace_container,
            workspace_directory,
            store,
            document: Mutex::new(document),
        })
    }

    /// Returns the exact selector-owned active identity without advancing its generation.
    pub(super) fn active_identity(
        &self,
    ) -> Result<WorkspaceRuntimeIdentity, WorkspaceSelectorError> {
        self.lock_document().map(|document| document.active)
    }

    /// Returns the validated installation-owned workspace container for bounded inventory checks.
    pub(super) fn workspace_repository_root(&self) -> &Path {
        &self.workspace_container
    }

    /// Returns whether one exact activation or rollback handoff remains durable.
    pub(super) fn has_pending_handoff(&self) -> Result<bool, WorkspaceSelectorError> {
        self.lock_document()
            .map(|document| document.pending.is_some())
    }

    /// Allocates and durably registers a new inactive managed workspace.
    ///
    /// Existing roots are never reopened. Directory creation precedes selector publication so a
    /// crash can leave only an unreachable orphan, never a registered missing workspace.
    pub(super) fn prepare_fresh_managed_workspace(
        &self,
    ) -> Result<(WorkspaceId, LocalPaths), WorkspaceSelectorError> {
        const MAXIMUM_IDENTITY_ATTEMPTS: usize = 16;

        let mut document = self.lock_document()?;
        if document.recovery_required || document.roots.len() >= MAXIMUM_WORKSPACES {
            return Err(if document.recovery_required {
                WorkspaceSelectorError::RecoveryRequired
            } else {
                WorkspaceSelectorError::CapacityExhausted
            });
        }
        for _attempt in 0..MAXIMUM_IDENTITY_ATTEMPTS {
            let workspace_id = WorkspaceId::try_from_uuid(Uuid::new_v4())
                .map_err(|_error| WorkspaceSelectorError::InvalidWorkspaceIdentity)?;
            if document.roots.contains_key(&workspace_id) {
                continue;
            }
            let name = workspace_id.as_uuid().to_string();
            match self.workspace_directory.symlink_metadata(&name) {
                Ok(_metadata) => continue,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(WorkspaceSelectorError::workspace_io(error)),
            }
            let expected_root = self.workspace_container.join(&name);
            let paths = LocalPaths::prepare(&expected_root)?;
            validate_exact_root(&paths, &expected_root)?;
            let mut updated = document.clone();
            updated.roots.insert(
                workspace_id,
                WorkspaceRootRecord {
                    root: expected_root,
                    placement: WorkspacePlacement::Managed,
                },
            );
            if let Err(error) = sync_workspace_container(&self.workspace_directory) {
                drop(paths);
                let _cleanup = self.workspace_directory.remove_dir_all(&name);
                return Err(error);
            }
            if let Err(error) = self.persist(&updated) {
                drop(paths);
                let _cleanup = self.workspace_directory.remove_dir_all(&name);
                return Err(error);
            }
            *document = updated;
            return Ok((workspace_id, paths));
        }
        Err(WorkspaceSelectorError::IdentityAllocationExhausted)
    }

    /// Unregisters and removes one failed inactive managed workspace, idempotently.
    ///
    /// Selector removal commits before filesystem deletion. A crash can therefore leave only an
    /// unreachable orphan, and a repeated call safely finishes that exact managed child cleanup.
    pub(super) fn abandon_managed_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<(), WorkspaceSelectorError> {
        let name = workspace_id.as_uuid().to_string();
        let expected_root = self.workspace_container.join(&name);
        let mut document = self.lock_document()?;
        if document.active.workspace_id() == workspace_id
            || document
                .rollback
                .is_some_and(|identity| identity.workspace_id() == workspace_id)
            || document.pending.is_some_and(|handoff| {
                handoff.previous.workspace_id() == workspace_id
                    || handoff.attempted.workspace_id() == workspace_id
                    || handoff.candidate.workspace_id() == workspace_id
            })
        {
            return Err(WorkspaceSelectorError::ActiveWorkspaceRemoval);
        }
        if let Some(record) = document.roots.get(&workspace_id) {
            if record.placement != WorkspacePlacement::Managed || record.root != expected_root {
                return Err(WorkspaceSelectorError::WorkspaceConflict);
            }
            let mut updated = document.clone();
            updated.roots.remove(&workspace_id);
            self.persist(&updated)?;
            *document = updated;
        }
        match self.workspace_directory.remove_dir_all(&name) {
            Ok(()) => sync_workspace_container(&self.workspace_directory),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(WorkspaceSelectorError::workspace_io(error)),
        }
    }

    /// Resolves one registered workspace through the selector's retained root authority.
    pub(super) fn workspace_paths(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<LocalPaths, WorkspaceSelectorError> {
        let document = self.lock_document()?;
        let record = document
            .roots
            .get(&workspace_id)
            .ok_or(WorkspaceSelectorError::SelectionConflict)?;
        open_recorded_root(
            workspace_id,
            record,
            &self.installation_root,
            &self.workspace_container,
        )
    }

    /// Reserves and resolves the durable startup selection before any workspace authority opens.
    ///
    /// An ordinary process start advances the generation before returning. A pending activation or
    /// rollback already owns its exact generation and is returned unchanged so a failed unpublished
    /// startup can be retried without inventing another transition.
    pub(super) fn startup_selection(
        &self,
    ) -> Result<WorkspaceStartupSelection, WorkspaceSelectorError> {
        let mut document = self.lock_document()?;
        if document.recovery_required {
            return Err(WorkspaceSelectorError::RecoveryRequired);
        }
        let (identity, handoff) = match document.pending {
            Some(pending) => (pending.candidate, Some(pending)),
            None => {
                let generation = document
                    .active
                    .generation()
                    .get()
                    .checked_add(1)
                    .ok_or(WorkspaceSelectorError::GenerationExhausted)?;
                let identity =
                    WorkspaceRuntimeIdentity::try_new(document.active.workspace_id(), generation)?;
                let mut updated = document.clone();
                updated.active = identity;
                self.persist(&updated)?;
                *document = updated;
                (identity, None)
            }
        };
        let record = document
            .roots
            .get(&identity.workspace_id())
            .ok_or(WorkspaceSelectorError::CorruptState)?;
        let paths = open_recorded_root(
            identity.workspace_id(),
            record,
            &self.installation_root,
            &self.workspace_container,
        )?;
        Ok(WorkspaceStartupSelection {
            identity,
            placement: record.placement,
            paths,
            handoff,
        })
    }

    /// Persists one idempotent activation handoff for an already-registered workspace.
    pub(super) fn stage_activation(
        &self,
        expected_active: WorkspaceRuntimeIdentity,
        target: WorkspaceId,
    ) -> Result<WorkspaceSupervisorHandoff, WorkspaceSelectorError> {
        let mut document = self.lock_document()?;
        if document.recovery_required {
            return Err(WorkspaceSelectorError::RecoveryRequired);
        }
        if let Some(pending) = document.pending {
            if pending.phase == WorkspaceHandoffPhase::Activate
                && pending.previous == expected_active
                && pending.candidate.workspace_id() == target
            {
                return Ok(pending);
            }
            return Err(WorkspaceSelectorError::HandoffConflict);
        }
        if document.active != expected_active
            || target == expected_active.workspace_id()
            || !document.roots.contains_key(&target)
        {
            return Err(WorkspaceSelectorError::SelectionConflict);
        }
        let generation = expected_active
            .generation()
            .get()
            .checked_add(1)
            .ok_or(WorkspaceSelectorError::GenerationExhausted)?;
        let candidate = WorkspaceRuntimeIdentity::try_new(target, generation)?;
        let pending = WorkspaceSupervisorHandoff {
            handoff_id: WorkspaceHandoffId::new(),
            previous: expected_active,
            attempted: candidate,
            candidate,
            phase: WorkspaceHandoffPhase::Activate,
        };
        let mut updated = document.clone();
        updated.rollback = Some(expected_active);
        updated.pending = Some(pending);
        self.persist(&updated)?;
        *document = updated;
        Ok(pending)
    }

    /// Replaces a failed activation with the exact prior workspace under a newer generation.
    pub(super) fn stage_startup_rollback(
        &self,
        handoff_id: WorkspaceHandoffId,
        failed: WorkspaceRuntimeIdentity,
    ) -> Result<WorkspaceSupervisorHandoff, WorkspaceSelectorError> {
        let mut document = self.lock_document()?;
        let pending = document
            .pending
            .ok_or(WorkspaceSelectorError::HandoffConflict)?;
        if pending.handoff_id != handoff_id || pending.attempted != failed {
            return Err(WorkspaceSelectorError::HandoffConflict);
        }
        if pending.phase == WorkspaceHandoffPhase::Rollback {
            return Ok(pending);
        }
        let rollback = document
            .rollback
            .filter(|rollback| *rollback == pending.previous)
            .ok_or(WorkspaceSelectorError::CorruptState)?;
        let generation = failed
            .generation()
            .get()
            .checked_add(1)
            .ok_or(WorkspaceSelectorError::GenerationExhausted)?;
        let candidate = WorkspaceRuntimeIdentity::try_new(rollback.workspace_id(), generation)?;
        let mut updated = document.clone();
        let rollback_handoff = WorkspaceSupervisorHandoff {
            candidate,
            phase: WorkspaceHandoffPhase::Rollback,
            ..pending
        };
        updated.pending = Some(rollback_handoff);
        self.persist(&updated)?;
        *document = updated;
        Ok(rollback_handoff)
    }

    /// Marks a failed rollback and prevents automatic supervisor restart loops.
    pub(super) fn mark_rollback_failed(
        &self,
        handoff_id: WorkspaceHandoffId,
        failed: WorkspaceRuntimeIdentity,
    ) -> Result<(), WorkspaceSelectorError> {
        let mut document = self.lock_document()?;
        let pending = document
            .pending
            .ok_or(WorkspaceSelectorError::HandoffConflict)?;
        if pending.handoff_id != handoff_id
            || pending.phase != WorkspaceHandoffPhase::Rollback
            || pending.candidate != failed
        {
            return Err(WorkspaceSelectorError::HandoffConflict);
        }
        if document.recovery_required {
            return Ok(());
        }
        let mut updated = document.clone();
        updated.recovery_required = true;
        self.persist(&updated)?;
        *document = updated;
        Ok(())
    }

    /// Commits one healthy replacement startup or confirms the unchanged active startup.
    pub(super) fn finalize_startup(
        &self,
        observed: &WorkspaceStartupSelection,
    ) -> Result<(), WorkspaceSelectorError> {
        let mut document = self.lock_document()?;
        if document.recovery_required {
            return Err(WorkspaceSelectorError::RecoveryRequired);
        }
        let Some(pending) = document.pending else {
            if observed.handoff.is_none() && observed.identity == document.active {
                return Ok(());
            }
            return Err(WorkspaceSelectorError::HandoffConflict);
        };
        if observed.handoff != Some(pending) || observed.identity != pending.candidate {
            return Err(WorkspaceSelectorError::HandoffConflict);
        }
        validate_selection_root(observed, &self.installation_root, &self.workspace_container)?;
        let mut updated = document.clone();
        updated.active = pending.candidate;
        updated.pending = None;
        if pending.phase == WorkspaceHandoffPhase::Rollback {
            updated.rollback = None;
        }
        self.persist(&updated)?;
        *document = updated;
        Ok(())
    }

    fn lock_document(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, WorkspaceSelectorDocument>, WorkspaceSelectorError> {
        self.document
            .lock()
            .map_err(|_| WorkspaceSelectorError::Unavailable)
    }

    fn persist(&self, document: &WorkspaceSelectorDocument) -> Result<(), WorkspaceSelectorError> {
        store_document(&self.store, document)
    }
}

fn legacy_workspace_contains_state(paths: &LocalPaths) -> Result<bool, WorkspaceSelectorError> {
    const MAXIMUM_ENTRIES: usize = 4_096;
    let mut inspected = 0_usize;
    for entry in std::fs::read_dir(paths.root()).map_err(WorkspaceSelectorError::workspace_io)? {
        let entry = entry.map_err(WorkspaceSelectorError::workspace_io)?;
        inspected = inspected
            .checked_add(1)
            .ok_or(WorkspaceSelectorError::CapacityExhausted)?;
        if inspected > MAXIMUM_ENTRIES {
            return Err(WorkspaceSelectorError::CapacityExhausted);
        }
        let name = entry.file_name();
        if matches!(name.to_str(), Some("journal" | "artifacts" | "control")) {
            let mut children =
                std::fs::read_dir(entry.path()).map_err(WorkspaceSelectorError::workspace_io)?;
            if children
                .next()
                .transpose()
                .map_err(WorkspaceSelectorError::workspace_io)?
                .is_some()
            {
                return Ok(true);
            }
        } else {
            return Ok(true);
        }
    }
    Ok(false)
}

impl fmt::Debug for WorkspaceSelector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WorkspaceSelector([INSTALLATION-GLOBAL AUTHORITY])")
    }
}

fn valid_pending_generation(pending: &WorkspaceSupervisorHandoff) -> bool {
    if pending.previous.generation().get().checked_add(1)
        != Some(pending.attempted.generation().get())
    {
        return false;
    }
    let expected = match pending.phase {
        WorkspaceHandoffPhase::Activate => pending.previous.generation().get().checked_add(1),
        WorkspaceHandoffPhase::Rollback => pending.attempted.generation().get().checked_add(1),
    };
    expected == Some(pending.candidate.generation().get())
}

fn prepare_workspace_container(
    installation_root: &Path,
) -> Result<(PathBuf, Arc<Dir>), WorkspaceSelectorError> {
    let expected = installation_root.join(WORKSPACE_DIRECTORY);
    std::fs::create_dir_all(&expected).map_err(WorkspaceSelectorError::workspace_io)?;
    let metadata =
        std::fs::symlink_metadata(&expected).map_err(WorkspaceSelectorError::workspace_io)?;
    let canonical =
        std::fs::canonicalize(&expected).map_err(WorkspaceSelectorError::workspace_io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() || canonical != expected {
        return Err(WorkspaceSelectorError::UnsafeWorkspaceRoot);
    }
    let directory = Dir::open_ambient_dir(&canonical, ambient_authority())
        .map_err(WorkspaceSelectorError::workspace_io)?;
    Ok((canonical, Arc::new(directory)))
}

#[cfg(unix)]
fn sync_workspace_container(directory: &Dir) -> Result<(), WorkspaceSelectorError> {
    Dir::reopen_dir(directory)
        .map_err(WorkspaceSelectorError::workspace_io)?
        .into_std_file()
        .sync_all()
        .map_err(WorkspaceSelectorError::workspace_io)
}

#[cfg(windows)]
fn sync_workspace_container(directory: &Dir) -> Result<(), WorkspaceSelectorError> {
    let metadata = directory
        .dir_metadata()
        .map_err(WorkspaceSelectorError::workspace_io)?;
    metadata
        .is_dir()
        .then_some(())
        .ok_or(WorkspaceSelectorError::UnsafeWorkspaceRoot)
}

#[cfg(not(any(unix, windows)))]
fn sync_workspace_container(_directory: &Dir) -> Result<(), WorkspaceSelectorError> {
    Err(WorkspaceSelectorError::UnsafeWorkspaceRoot)
}

fn prepare_bootstrap_root(
    installation_root: &Path,
    workspace_container: &Path,
    bootstrap: WorkspaceBootstrapSeed,
) -> Result<WorkspaceRootRecord, WorkspaceSelectorError> {
    match bootstrap {
        WorkspaceBootstrapSeed::Managed { workspace_id } => {
            let expected = workspace_container.join(workspace_id.as_uuid().to_string());
            let paths = LocalPaths::prepare(&expected)?;
            validate_exact_root(&paths, &expected)?;
            Ok(WorkspaceRootRecord {
                root: expected,
                placement: WorkspacePlacement::Managed,
            })
        }
        WorkspaceBootstrapSeed::Legacy { paths, .. } => {
            validate_legacy_root(&paths, installation_root)?;
            Ok(WorkspaceRootRecord {
                root: paths.root().to_path_buf(),
                placement: WorkspacePlacement::LegacyMigrationRequired,
            })
        }
    }
}

fn validate_bootstrap_match(
    document: &WorkspaceSelectorDocument,
    bootstrap: &WorkspaceBootstrapSeed,
    installation_root: &Path,
) -> Result<(), WorkspaceSelectorError> {
    let record = document
        .roots
        .get(&bootstrap.workspace_id())
        .ok_or(WorkspaceSelectorError::BootstrapConflict)?;
    match bootstrap {
        WorkspaceBootstrapSeed::Managed { workspace_id } => {
            let expected = installation_root
                .join(WORKSPACE_DIRECTORY)
                .join(workspace_id.as_uuid().to_string());
            (record.placement == WorkspacePlacement::Managed && record.root == expected)
                .then_some(())
                .ok_or(WorkspaceSelectorError::BootstrapConflict)
        }
        WorkspaceBootstrapSeed::Legacy { paths, .. } => {
            validate_legacy_root(paths, installation_root)?;
            (record.placement == WorkspacePlacement::LegacyMigrationRequired
                && record.root == paths.root())
            .then_some(())
            .ok_or(WorkspaceSelectorError::BootstrapConflict)
        }
    }
}

fn validate_document_roots(
    document: &WorkspaceSelectorDocument,
    installation_root: &Path,
    workspace_container: &Path,
) -> Result<(), WorkspaceSelectorError> {
    for (workspace_id, record) in &document.roots {
        open_recorded_root(
            *workspace_id,
            record,
            installation_root,
            workspace_container,
        )?;
    }
    Ok(())
}

fn open_recorded_root(
    workspace_id: WorkspaceId,
    record: &WorkspaceRootRecord,
    installation_root: &Path,
    workspace_container: &Path,
) -> Result<LocalPaths, WorkspaceSelectorError> {
    let paths = LocalPaths::open_existing(&record.root)?;
    match record.placement {
        WorkspacePlacement::Managed => {
            let expected = workspace_container.join(workspace_id.as_uuid().to_string());
            if record.root != expected {
                return Err(WorkspaceSelectorError::CorruptState);
            }
            validate_exact_root(&paths, &expected)?;
        }
        WorkspacePlacement::LegacyMigrationRequired => {
            validate_legacy_root(&paths, installation_root)?;
        }
    }
    Ok(paths)
}

fn validate_selection_root(
    selection: &WorkspaceStartupSelection,
    installation_root: &Path,
    workspace_container: &Path,
) -> Result<(), WorkspaceSelectorError> {
    match selection.placement {
        WorkspacePlacement::Managed => validate_exact_root(
            &selection.paths,
            &workspace_container.join(selection.identity.workspace_id().as_uuid().to_string()),
        ),
        WorkspacePlacement::LegacyMigrationRequired => {
            validate_legacy_root(&selection.paths, installation_root)
        }
    }
}

fn validate_prepared_paths(paths: &LocalPaths) -> Result<(), WorkspaceSelectorError> {
    let root = paths.root();
    if !root.is_absolute()
        || root.to_str().is_none()
        || std::fs::canonicalize(root).map_err(WorkspaceSelectorError::workspace_io)? != root
    {
        return Err(WorkspaceSelectorError::UnsafeWorkspaceRoot);
    }
    paths.control_root()?.try_clone_directory()?;
    Ok(())
}

fn validate_exact_root(paths: &LocalPaths, expected: &Path) -> Result<(), WorkspaceSelectorError> {
    validate_prepared_paths(paths)?;
    if paths.root() != expected {
        return Err(WorkspaceSelectorError::UnsafeWorkspaceRoot);
    }
    Ok(())
}

fn validate_legacy_root(
    paths: &LocalPaths,
    installation_root: &Path,
) -> Result<(), WorkspaceSelectorError> {
    validate_prepared_paths(paths)?;
    let legacy = paths.root();
    if legacy.starts_with(installation_root) || installation_root.starts_with(legacy) {
        return Err(WorkspaceSelectorError::UnsafeWorkspaceRoot);
    }
    Ok(())
}

fn decode(encoded: &[u8]) -> Result<WorkspaceSelectorDocument, WorkspaceSelectorError> {
    serde_json::from_slice(encoded).map_err(|_| WorkspaceSelectorError::CorruptState)
}

fn store_document(
    store: &LocalAuthorityStateStore,
    document: &WorkspaceSelectorDocument,
) -> Result<(), WorkspaceSelectorError> {
    let encoded = serde_json::to_vec(document).map_err(|_| WorkspaceSelectorError::CorruptState)?;
    if encoded.len() > LocalAuthorityStateStore::maximum_payload_bytes() {
        return Err(WorkspaceSelectorError::CapacityExhausted);
    }
    store.store(&encoded)?;
    Ok(())
}

/// Fail-closed selector, root, persistence, and generation errors.
#[derive(Debug, Error)]
pub(super) enum WorkspaceSelectorError {
    /// The durable selector is malformed or internally inconsistent.
    #[error("workspace selector state is corrupt")]
    CorruptState,
    /// The configured installation or workspace root is unsafe or changed identity.
    #[error("workspace root is unsafe or changed identity")]
    UnsafeWorkspaceRoot,
    /// A one-time bootstrap seed conflicts with durable selector state.
    #[error("workspace bootstrap conflicts with durable selector state")]
    BootstrapConflict,
    /// The selector has no durable state and requires a trusted one-time seed.
    #[error("workspace selector requires trusted bootstrap state")]
    BootstrapRequired,
    /// A workspace identity already maps to a different placement or root.
    #[error("workspace registration conflicts with durable state")]
    WorkspaceConflict,
    /// The expected active selection is stale or the target is not registered.
    #[error("workspace selection conflicts with current authority")]
    SelectionConflict,
    /// Another exact supervisor handoff is already pending.
    #[error("workspace supervisor handoff conflicts with durable state")]
    HandoffConflict,
    /// Automatic recovery stopped after the rollback startup also failed.
    #[error("workspace recovery requires explicit repair")]
    RecoveryRequired,
    /// The workspace inventory or encoded state reached its hard bound.
    #[error("workspace selector capacity is exhausted")]
    CapacityExhausted,
    /// A generation cannot advance without overflow.
    #[error("workspace generation is exhausted")]
    GenerationExhausted,
    /// In-process selector serialization is unavailable.
    #[error("workspace selector is unavailable")]
    Unavailable,
    /// The stable authority-state store could not be opened or committed.
    #[error("workspace selector persistence failed")]
    Persistence(#[from] LocalAuthorityStateStoreError),
    /// A prepared local path capability could not be opened or revalidated.
    #[error("workspace path capability is unavailable")]
    Path(#[from] PathError),
    /// Workspace generation or identity construction failed.
    #[error("workspace runtime identity is invalid")]
    Lifecycle(#[from] LifecycleError),
    /// A newly allocated workspace identity was invalid.
    #[error("workspace identity is invalid")]
    InvalidWorkspaceIdentity,
    /// A fresh workspace identity could not be allocated without colliding with retained state.
    #[error("workspace identity allocation is exhausted")]
    IdentityAllocationExhausted,
    /// An active, rollback, or pending workspace cannot be removed.
    #[error("active or transition-bound workspace cannot be removed")]
    ActiveWorkspaceRemoval,
    /// The code-owned workspace container could not be prepared.
    #[error("workspace container operation failed")]
    WorkspaceIo(#[source] std::io::Error),
}

impl WorkspaceSelectorError {
    fn workspace_io(source: std::io::Error) -> Self {
        Self::WorkspaceIo(source)
    }
}
