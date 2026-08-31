//! Durable, path-free workspace inventory and transition journal.

use std::{collections::BTreeMap, fmt, path::Path, sync::Mutex};

use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};
use market_squawk_runtime::WorkspaceId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::lifecycle::{
    LifecycleError, WorkspaceRuntimeIdentity, WorkspaceTransitionDisposition,
    WorkspaceTransitionJournal, WorkspaceTransitionRecord,
};

const FORMAT_VERSION: u16 = 1;
const AUTHORITY_DIRECTORY: &str = "workspace-authority";
const MAXIMUM_WORKSPACES: usize = 64;
const MAXIMUM_TRANSITION_RECORDS: usize = 256;
const MAXIMUM_PAGE_SIZE: usize = 64;
const MAXIMUM_DISPLAY_NAME_BYTES: usize = 128;

/// Health state established by workspace preparation or post-switch inspection.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceHealth {
    Prepared,
    Healthy,
    RecoveryRequired,
}

/// Secret-free inventory record for one capability-confined workspace.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct WorkspaceDescriptor {
    workspace_id: WorkspaceId,
    display_name: String,
    schema_version: u32,
    health: WorkspaceHealth,
    estimated_bytes: u64,
}

impl WorkspaceDescriptor {
    /// Creates one bounded inventory record. Filesystem authority is retained separately.
    pub fn try_new(
        workspace_id: WorkspaceId,
        display_name: impl Into<String>,
        schema_version: u32,
        health: WorkspaceHealth,
        estimated_bytes: u64,
    ) -> Result<Self, WorkspaceRegistryError> {
        let display_name = display_name.into();
        if display_name.trim() != display_name
            || display_name.is_empty()
            || display_name.len() > MAXIMUM_DISPLAY_NAME_BYTES
            || display_name.chars().any(char::is_control)
            || schema_version == 0
        {
            return Err(WorkspaceRegistryError::InvalidDescriptor);
        }
        Ok(Self {
            workspace_id,
            display_name,
            schema_version,
            health,
            estimated_bytes,
        })
    }

    /// Returns the workspace identity.
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    /// Returns the schema version proved by preparation.
    #[must_use]
    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Returns whether the workspace has been staged but not yet activated.
    #[must_use]
    pub const fn is_prepared(&self) -> bool {
        matches!(self.health, WorkspaceHealth::Prepared)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct WorkspaceRegistryDocument {
    format_version: u16,
    active: WorkspaceRuntimeIdentity,
    workspaces: BTreeMap<WorkspaceId, WorkspaceDescriptor>,
    transitions: Vec<WorkspaceTransitionRecord>,
}

impl WorkspaceRegistryDocument {
    fn initial(
        active: WorkspaceRuntimeIdentity,
        descriptor: WorkspaceDescriptor,
    ) -> Result<Self, WorkspaceRegistryError> {
        if active.workspace_id() != descriptor.workspace_id()
            || descriptor.health != WorkspaceHealth::Healthy
        {
            return Err(WorkspaceRegistryError::InvalidDescriptor);
        }
        let mut workspaces = BTreeMap::new();
        workspaces.insert(descriptor.workspace_id(), descriptor);
        Ok(Self {
            format_version: FORMAT_VERSION,
            active,
            workspaces,
            transitions: Vec::new(),
        })
    }

    fn validate(self) -> Result<Self, WorkspaceRegistryError> {
        if self.format_version != FORMAT_VERSION
            || self.workspaces.is_empty()
            || self.workspaces.len() > MAXIMUM_WORKSPACES
            || self.transitions.len() > MAXIMUM_TRANSITION_RECORDS
            || !self.workspaces.contains_key(&self.active.workspace_id())
            || self
                .workspaces
                .get(&self.active.workspace_id())
                .is_none_or(|descriptor| descriptor.health != WorkspaceHealth::Healthy)
            || self
                .workspaces
                .iter()
                .any(|(identity, descriptor)| identity != &descriptor.workspace_id())
        {
            return Err(WorkspaceRegistryError::CorruptState);
        }
        Ok(self)
    }
}

/// Bounded workspace inventory page.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspacePage {
    active: WorkspaceRuntimeIdentity,
    workspaces: Vec<WorkspaceDescriptor>,
    next_after_workspace_id: Option<WorkspaceId>,
}

/// Exclusive durable owner of workspace inventory and transition history.
pub struct DurableWorkspaceRegistry {
    store: LocalAuthorityStateStore,
    document: Mutex<WorkspaceRegistryDocument>,
}

impl DurableWorkspaceRegistry {
    /// Opens or initializes the installed workspace authority under the prepared control root.
    pub fn try_open(
        control_root: &Path,
        initial_active: WorkspaceRuntimeIdentity,
        initial_descriptor: WorkspaceDescriptor,
    ) -> Result<Self, WorkspaceRegistryError> {
        let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))?;
        let document = match store.load()? {
            Some(encoded) => serde_json::from_slice::<WorkspaceRegistryDocument>(&encoded)
                .map_err(|_| WorkspaceRegistryError::CorruptState)?
                .validate()?,
            None => {
                let document =
                    WorkspaceRegistryDocument::initial(initial_active, initial_descriptor)?;
                store.store(&encode(&document)?)?;
                document
            }
        };
        Ok(Self {
            store,
            document: Mutex::new(document),
        })
    }

    /// Returns the current durable identity used to initialize lifecycle fencing.
    pub fn active(&self) -> Result<WorkspaceRuntimeIdentity, WorkspaceRegistryError> {
        self.document
            .lock()
            .map(|document| document.active)
            .map_err(|_| WorkspaceRegistryError::Unavailable)
    }

    /// Advances the active fence to a selector-reserved same-workspace process generation.
    ///
    /// Gaps represent failed unpublished startup attempts whose generations must never be reused.
    /// Workspace changes remain owned by the explicit transition journal.
    pub(crate) fn reconcile_ordinary_startup(
        &self,
        selected: WorkspaceRuntimeIdentity,
    ) -> Result<(), WorkspaceRegistryError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| WorkspaceRegistryError::Unavailable)?;
        if document.active == selected {
            return Ok(());
        }
        if document.active.workspace_id() != selected.workspace_id()
            || selected.generation().get() <= document.active.generation().get()
            || !document.workspaces.contains_key(&selected.workspace_id())
        {
            return Err(WorkspaceRegistryError::CapacityOrConflict);
        }
        let mut candidate = document.clone();
        candidate.active = selected;
        self.store.store(&encode(&candidate)?)?;
        *document = candidate;
        Ok(())
    }

    /// Returns the exact durable descriptor for one registered workspace.
    pub fn descriptor(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<WorkspaceDescriptor>, WorkspaceRegistryError> {
        Ok(self
            .document
            .lock()
            .map_err(|_| WorkspaceRegistryError::Unavailable)?
            .workspaces
            .get(&workspace_id)
            .cloned())
    }

    /// Registers one fully prepared workspace without changing the active workspace.
    pub fn register_prepared(
        &self,
        descriptor: WorkspaceDescriptor,
    ) -> Result<(), WorkspaceRegistryError> {
        if descriptor.health != WorkspaceHealth::Prepared {
            return Err(WorkspaceRegistryError::InvalidDescriptor);
        }
        let mut document = self
            .document
            .lock()
            .map_err(|_| WorkspaceRegistryError::Unavailable)?;
        if document.workspaces.len() >= MAXIMUM_WORKSPACES
            || document.workspaces.contains_key(&descriptor.workspace_id())
        {
            return Err(WorkspaceRegistryError::CapacityOrConflict);
        }
        let mut candidate = document.clone();
        candidate
            .workspaces
            .insert(descriptor.workspace_id(), descriptor);
        self.store.store(&encode(&candidate)?)?;
        *document = candidate;
        Ok(())
    }

    /// Lists workspaces in stable identity order under a fixed page ceiling.
    pub fn list(
        &self,
        after: Option<WorkspaceId>,
        limit: usize,
    ) -> Result<WorkspacePage, WorkspaceRegistryError> {
        if limit == 0 || limit > MAXIMUM_PAGE_SIZE {
            return Err(WorkspaceRegistryError::InvalidLimit);
        }
        let document = self
            .document
            .lock()
            .map_err(|_| WorkspaceRegistryError::Unavailable)?;
        let mut workspaces = document
            .workspaces
            .values()
            .filter(|descriptor| after.is_none_or(|cursor| descriptor.workspace_id() > cursor))
            .take(limit.saturating_add(1))
            .cloned()
            .collect::<Vec<_>>();
        let has_more = workspaces.len() > limit;
        workspaces.truncate(limit);
        let next_after_workspace_id = has_more
            .then(|| workspaces.last().map(WorkspaceDescriptor::workspace_id))
            .flatten();
        Ok(WorkspacePage {
            active: document.active,
            workspaces,
            next_after_workspace_id,
        })
    }
}

impl WorkspaceTransitionJournal for DurableWorkspaceRegistry {
    fn append(&self, record: &WorkspaceTransitionRecord) -> Result<(), LifecycleError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| LifecycleError::AuthorityUnavailable)?;
        if !document
            .workspaces
            .contains_key(&record.active().workspace_id())
        {
            return Err(LifecycleError::AuthorityUnavailable);
        }
        let mut candidate = document.clone();
        candidate.active = record.active();
        candidate
            .workspaces
            .get_mut(&record.active().workspace_id())
            .ok_or(LifecycleError::AuthorityUnavailable)?
            .health = WorkspaceHealth::Healthy;
        if record.disposition() == WorkspaceTransitionDisposition::RolledBack {
            candidate
                .workspaces
                .get_mut(&record.attempted().workspace_id())
                .ok_or(LifecycleError::AuthorityUnavailable)?
                .health = WorkspaceHealth::RecoveryRequired;
        }
        if candidate.transitions.len() == MAXIMUM_TRANSITION_RECORDS {
            candidate.transitions.remove(0);
        }
        candidate.transitions.push(record.clone());
        let encoded = encode(&candidate).map_err(|_| LifecycleError::AuthorityUnavailable)?;
        self.store
            .store(&encoded)
            .map_err(|_| LifecycleError::AuthorityUnavailable)?;
        *document = candidate;
        Ok(())
    }
}

impl fmt::Debug for DurableWorkspaceRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DurableWorkspaceRegistry([EXCLUSIVE LOCAL AUTHORITY])")
    }
}

fn encode(document: &WorkspaceRegistryDocument) -> Result<Vec<u8>, WorkspaceRegistryError> {
    let encoded = serde_json::to_vec(document).map_err(|_| WorkspaceRegistryError::CorruptState)?;
    if encoded.len() > LocalAuthorityStateStore::maximum_payload_bytes() {
        return Err(WorkspaceRegistryError::CapacityOrConflict);
    }
    Ok(encoded)
}

/// Typed workspace registry failure without local paths.
#[derive(Debug, Error)]
pub enum WorkspaceRegistryError {
    #[error("workspace descriptor is invalid")]
    InvalidDescriptor,
    #[error("workspace registry state is corrupt")]
    CorruptState,
    #[error("workspace registry is unavailable")]
    Unavailable,
    #[error("workspace registry capacity is exhausted or identity conflicts")]
    CapacityOrConflict,
    #[error("workspace page limit is invalid")]
    InvalidLimit,
    #[error("workspace state persistence failed")]
    Persistence(#[from] LocalAuthorityStateStoreError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn workspace(value: u128) -> Result<WorkspaceId, Box<dyn std::error::Error>> {
        Ok(WorkspaceId::try_from_uuid(Uuid::from_u128(value))?)
    }

    #[test]
    fn registry_reopens_the_exact_durable_active_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let active_id = workspace(1)?;
        let active = WorkspaceRuntimeIdentity::try_new(active_id, 3)?;
        let descriptor =
            WorkspaceDescriptor::try_new(active_id, "Primary", 1, WorkspaceHealth::Healthy, 1024)?;
        {
            let registry =
                DurableWorkspaceRegistry::try_open(root.path(), active, descriptor.clone())?;
            assert_eq!(registry.active()?, active);
        }
        let reopened = DurableWorkspaceRegistry::try_open(root.path(), active, descriptor)?;
        assert_eq!(reopened.active()?, active);
        Ok(())
    }

    #[test]
    fn registry_reconciles_a_later_same_workspace_startup_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let active_id = workspace(1)?;
        let active = WorkspaceRuntimeIdentity::try_new(active_id, 3)?;
        let selected = WorkspaceRuntimeIdentity::try_new(active_id, 6)?;
        let descriptor =
            WorkspaceDescriptor::try_new(active_id, "Primary", 1, WorkspaceHealth::Healthy, 1024)?;
        {
            let registry =
                DurableWorkspaceRegistry::try_open(root.path(), active, descriptor.clone())?;
            registry.reconcile_ordinary_startup(selected)?;
            assert_eq!(registry.active()?, selected);
            assert!(registry.reconcile_ordinary_startup(active).is_err());
        }
        let reopened = DurableWorkspaceRegistry::try_open(root.path(), selected, descriptor)?;
        assert_eq!(reopened.active()?, selected);
        Ok(())
    }
}
