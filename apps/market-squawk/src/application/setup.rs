//! Workspace-owned durable setup-plan preview and acceptance authority.

mod persistence;
mod plan;

use std::{
    collections::BTreeMap,
    fmt,
    path::Path,
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};
use market_squawk_runtime::WorkspaceId;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use uuid::Uuid;

use self::persistence::SetupPlanDocument;
pub use self::plan::{
    SetupCapability, SetupDiskEstimate, SetupDiskImpact, SetupExternalContact, SetupFirstResult,
    SetupGoal, SetupImportFormat, SetupOutcome, SetupPlan, SetupPlanCatalog, SetupPlanSelection,
    SetupPlanStep, SetupProviderOutcome, SetupRequiredInput, SetupReversibleLocalChange,
    SetupSafeSkip, SetupStarterPlan, SetupStepChoice, SetupStepDisposition, SetupStepId,
    SetupTimeEstimate, SetupTimePolicy,
};
use self::plan::{
    disk_estimate, external_contacts, included_capabilities, plan_digest, reversible_changes,
    safe_skip_steps, time_estimate,
};

/// Durable and client DTO format revision for the V1 setup-plan authority.
pub const SETUP_PLAN_FORMAT_VERSION: u16 = 1;
const AUTHORITY_DIRECTORY: &str = "setup-plan-authority";
const MAXIMUM_PREVIEWS: usize = 64;
const MAXIMUM_PREVIEW_BYTES: usize = 64 * 1024;
const PREVIEW_LIFETIME: Duration = Duration::from_secs(15 * 60);
const PREVIEW_ID_ATTEMPTS: usize = 16;

/// Opaque one-process identity for one immutable setup-plan preview.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SetupPreviewId(Uuid);

impl SetupPreviewId {
    /// Admits a non-nil preview identity decoded by a transport boundary.
    pub fn try_from_uuid(value: Uuid) -> Result<Self, SetupPlanError> {
        if value.is_nil() {
            Err(SetupPlanError::InvalidConfirmation)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the UUID representation for closed transport projection.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl<'de> Deserialize<'de> for SetupPreviewId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Uuid::deserialize(deserializer)?;
        Self::try_from_uuid(value).map_err(serde::de::Error::custom)
    }
}

/// Immutable preview returned before any durable setup authority changes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupPlanPreview {
    format_version: u16,
    preview_id: SetupPreviewId,
    owner_workspace: WorkspaceId,
    current_revision: u64,
    plan_digest: [u8; 32],
    plan: SetupPlan,
    included_capabilities: Box<[SetupCapability]>,
    external_contacts: Box<[SetupExternalContact]>,
    reversible_local_changes: Box<[SetupReversibleLocalChange]>,
    expected_time: SetupTimeEstimate,
    expected_disk: SetupDiskEstimate,
    safe_skip_steps: Box<[SetupStepId]>,
    issued_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
    preview_sha256: [u8; 32],
}

impl SetupPlanPreview {
    /// Returns the closed preview DTO format revision.
    #[must_use]
    pub const fn format_version(&self) -> u16 {
        self.format_version
    }

    /// Returns the opaque one-process preview identity.
    #[must_use]
    pub const fn preview_id(&self) -> SetupPreviewId {
        self.preview_id
    }

    /// Returns the workspace identity that exclusively owns this preview.
    #[must_use]
    pub const fn owner_workspace(&self) -> WorkspaceId {
        self.owner_workspace
    }

    /// Returns the durable revision against which the preview was built.
    #[must_use]
    pub const fn current_revision(&self) -> u64 {
        self.current_revision
    }

    /// Returns the durable plan revision that acceptance would install.
    #[must_use]
    pub const fn plan_revision(&self) -> u64 {
        self.plan.revision()
    }

    /// Returns the canonical SHA-256 identity of the workspace-bound plan.
    #[must_use]
    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }

    /// Returns the complete closed plan for display and later owner-fact composition.
    #[must_use]
    pub const fn plan(&self) -> &SetupPlan {
        &self.plan
    }

    /// Returns the canonical SHA-256 identity of every exact preview field.
    #[must_use]
    pub const fn preview_sha256(&self) -> [u8; 32] {
        self.preview_sha256
    }

    /// Returns the wall-clock expiry used for client display; monotonic process time enforces it.
    #[must_use]
    pub const fn expires_at_unix_seconds(&self) -> u64 {
        self.expires_at_unix_seconds
    }

    /// Returns when the immutable preview was issued, as Unix seconds.
    #[must_use]
    pub const fn issued_at_unix_seconds(&self) -> u64 {
        self.issued_at_unix_seconds
    }

    /// Returns every included capability without asserting live readiness.
    #[must_use]
    pub fn included_capabilities(&self) -> &[SetupCapability] {
        &self.included_capabilities
    }

    /// Returns the official external systems the included steps may contact.
    #[must_use]
    pub fn external_contacts(&self) -> &[SetupExternalContact] {
        &self.external_contacts
    }

    /// Returns all disclosed reversible local changes.
    #[must_use]
    pub fn reversible_local_changes(&self) -> &[SetupReversibleLocalChange] {
        &self.reversible_local_changes
    }

    /// Returns the checked active-time and first-value forecast.
    #[must_use]
    pub const fn expected_time(&self) -> SetupTimeEstimate {
        self.expected_time
    }

    /// Returns the bounded disk-impact forecast.
    #[must_use]
    pub const fn expected_disk(&self) -> &SetupDiskEstimate {
        &self.expected_disk
    }

    /// Returns the steps that remain installed and available after an explicit safe skip.
    #[must_use]
    pub fn safe_skip_steps(&self) -> &[SetupStepId] {
        &self.safe_skip_steps
    }
}

/// Explicit client confirmation bound to one exact one-use preview.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SetupPlanConfirmation {
    preview_id: SetupPreviewId,
    preview_sha256: [u8; 32],
}

impl SetupPlanConfirmation {
    /// Constructs confirmation authority from an already validated preview ID and exact digest.
    /// The act of submitting this type is confirmation; no caller-supplied completion flag exists.
    #[must_use]
    pub const fn new(preview_id: SetupPreviewId, preview_sha256: [u8; 32]) -> Self {
        Self {
            preview_id,
            preview_sha256,
        }
    }
}

/// Exact accepted plan retained in workspace control state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AcceptedSetupPlan {
    revision: u64,
    digest: [u8; 32],
    accepted_at_unix_seconds: u64,
    plan: SetupPlan,
}

impl AcceptedSetupPlan {
    fn try_new(
        workspace: WorkspaceId,
        plan: SetupPlan,
        accepted_at_unix_seconds: u64,
    ) -> Result<Self, SetupPlanError> {
        if accepted_at_unix_seconds == 0 {
            return Err(SetupPlanError::TimeUnavailable);
        }
        let revision = plan.revision();
        let digest = plan_digest(workspace, &plan)?;
        Ok(Self {
            revision,
            digest,
            accepted_at_unix_seconds,
            plan,
        })
    }

    pub(super) fn validate(&self, workspace: WorkspaceId) -> Result<(), SetupPlanError> {
        self.plan.validate()?;
        if self.revision == 0
            || self.revision != self.plan.revision()
            || self.accepted_at_unix_seconds == 0
            || self.digest != plan_digest(workspace, &self.plan)?
        {
            return Err(SetupPlanError::CorruptState);
        }
        Ok(())
    }

    /// Returns the monotonic accepted plan revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the canonical workspace-bound plan SHA-256.
    #[must_use]
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Returns when persistence completed, as Unix seconds.
    #[must_use]
    pub const fn accepted_at_unix_seconds(&self) -> u64 {
        self.accepted_at_unix_seconds
    }

    /// Returns the exact immutable accepted plan.
    #[must_use]
    pub const fn plan(&self) -> &SetupPlan {
        &self.plan
    }
}

/// Closed redacted status DTO for `Setup.GetStatus` composition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupPlanStatus {
    format_version: u16,
    catalog: SetupPlanCatalog,
    current_revision: u64,
    accepted_plan: Option<AcceptedSetupPlan>,
}

impl SetupPlanStatus {
    /// Returns the closed setup status DTO format revision.
    #[must_use]
    pub const fn format_version(&self) -> u16 {
        self.format_version
    }

    /// Returns the versioned goal and starter-plan catalog.
    #[must_use]
    pub const fn catalog(&self) -> SetupPlanCatalog {
        self.catalog
    }

    /// Returns zero before the first acceptance, otherwise the exact durable revision.
    #[must_use]
    pub const fn current_revision(&self) -> u64 {
        self.current_revision
    }

    /// Returns the exact accepted plan, if one exists. Live capability completion is deliberately
    /// absent and must be re-evaluated from owning application services after every restart.
    #[must_use]
    pub const fn accepted_plan(&self) -> Option<&AcceptedSetupPlan> {
        self.accepted_plan.as_ref()
    }
}

/// Durable acknowledgement returned only after both state-store copies retain the accepted plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupPlanReceipt {
    revision: u64,
    digest: [u8; 32],
    accepted_at_unix_seconds: u64,
}

impl SetupPlanReceipt {
    /// Returns the newly active durable plan revision.
    #[must_use]
    pub const fn revision(self) -> u64 {
        self.revision
    }

    /// Returns the canonical workspace-bound accepted plan SHA-256.
    #[must_use]
    pub const fn digest(self) -> [u8; 32] {
        self.digest
    }

    /// Returns when durable persistence completed, as Unix seconds.
    #[must_use]
    pub const fn accepted_at_unix_seconds(self) -> u64 {
        self.accepted_at_unix_seconds
    }
}

#[derive(Clone, Debug)]
struct StoredPreview {
    preview: SetupPlanPreview,
    expires_at: Instant,
}

struct SetupAuthorityState {
    document: Option<SetupPlanDocument>,
    recovery_required: bool,
}

/// Exclusive workspace owner of setup preview, compare-and-apply, and durable accepted-plan state.
pub struct SetupPlanAuthority {
    owner_workspace: WorkspaceId,
    store: LocalAuthorityStateStore,
    state: Mutex<SetupAuthorityState>,
    previews: Mutex<BTreeMap<SetupPreviewId, StoredPreview>>,
}

impl SetupPlanAuthority {
    /// Opens the workspace's crash-safe setup authority beneath an already prepared control root.
    /// Corrupt, cross-workspace, unsupported, or ambiguous recovery state fails closed.
    pub fn try_open(control_root: &Path, workspace: WorkspaceId) -> Result<Self, SetupPlanError> {
        let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))?;
        let document = store
            .load()?
            .map(|encoded| SetupPlanDocument::decode(&encoded, workspace))
            .transpose()?;
        Ok(Self {
            owner_workspace: workspace,
            store,
            state: Mutex::new(SetupAuthorityState {
                document,
                recovery_required: false,
            }),
            previews: Mutex::new(BTreeMap::new()),
        })
    }

    /// Returns the complete closed V1 goal and starter-plan catalog.
    #[must_use]
    pub const fn catalog() -> SetupPlanCatalog {
        SetupPlanCatalog::current()
    }

    /// Returns the durable accepted plan only; live setup facts are composed separately by owners.
    pub fn status(&self) -> Result<SetupPlanStatus, SetupPlanError> {
        let state = self.state.lock().map_err(|_| SetupPlanError::Unavailable)?;
        if state.recovery_required {
            return Err(SetupPlanError::RecoveryRequired);
        }
        Ok(SetupPlanStatus {
            format_version: SETUP_PLAN_FORMAT_VERSION,
            catalog: Self::catalog(),
            current_revision: current_revision(state.document.as_ref()),
            accepted_plan: state
                .document
                .as_ref()
                .map(|document| document.accepted_plan().clone()),
        })
    }

    /// Builds and retains one immutable, bounded, workspace-owned preview without changing durable
    /// product authority. A restart intentionally invalidates all outstanding previews.
    pub fn preview_plan(
        &self,
        workspace: WorkspaceId,
        expected_revision: u64,
        selection: SetupPlanSelection,
    ) -> Result<SetupPlanPreview, SetupPlanError> {
        self.ensure_owner(workspace)?;
        selection.validate()?;
        let state = self.state.lock().map_err(|_| SetupPlanError::Unavailable)?;
        if state.recovery_required {
            return Err(SetupPlanError::RecoveryRequired);
        }
        let current = current_revision(state.document.as_ref());
        if expected_revision != current {
            return Err(SetupPlanError::StaleRevision);
        }
        let revision = current
            .checked_add(1)
            .ok_or(SetupPlanError::RevisionExhausted)?;
        let plan = SetupPlan::try_build(revision, selection)?;
        let plan_digest = plan_digest(workspace, &plan)?;
        let now = Instant::now();
        let issued_at_unix_seconds = current_unix_seconds()?;
        let expires_at = now
            .checked_add(PREVIEW_LIFETIME)
            .ok_or(SetupPlanError::TimeUnavailable)?;
        let expires_at_unix_seconds = issued_at_unix_seconds
            .checked_add(PREVIEW_LIFETIME.as_secs())
            .ok_or(SetupPlanError::TimeUnavailable)?;
        let mut previews = self
            .previews
            .lock()
            .map_err(|_| SetupPlanError::Unavailable)?;
        prune_expired(&mut previews, now);
        if previews.len() >= MAXIMUM_PREVIEWS {
            return Err(SetupPlanError::CapacityExceeded);
        }
        let preview_id = next_preview_id(&previews)?;
        let included_capabilities = included_capabilities(&plan)?.into_boxed_slice();
        let external_contacts = external_contacts(&plan)?.into_boxed_slice();
        let reversible_local_changes = reversible_changes(&plan)?.into_boxed_slice();
        let expected_time = time_estimate(&plan)?;
        let expected_disk = disk_estimate(&plan)?;
        let safe_skip_steps = safe_skip_steps(&plan)?.into_boxed_slice();
        let preview_sha256 = preview_digest(
            preview_id,
            workspace,
            current,
            plan_digest,
            &plan,
            &included_capabilities,
            &external_contacts,
            &reversible_local_changes,
            expected_time,
            &expected_disk,
            &safe_skip_steps,
            issued_at_unix_seconds,
            expires_at_unix_seconds,
        )?;
        let preview = SetupPlanPreview {
            format_version: SETUP_PLAN_FORMAT_VERSION,
            preview_id,
            owner_workspace: workspace,
            current_revision: current,
            plan_digest,
            plan,
            included_capabilities,
            external_contacts,
            reversible_local_changes,
            expected_time,
            expected_disk,
            safe_skip_steps,
            issued_at_unix_seconds,
            expires_at_unix_seconds,
            preview_sha256,
        };
        let encoded = serde_json::to_vec(&preview).map_err(|_| SetupPlanError::Encoding)?;
        if encoded.len() > MAXIMUM_PREVIEW_BYTES {
            return Err(SetupPlanError::CapacityExceeded);
        }
        previews.insert(
            preview_id,
            StoredPreview {
                preview: preview.clone(),
                expires_at,
            },
        );
        Ok(preview)
    }

    /// Consumes one exact preview and persists its accepted plan before returning acknowledgement.
    /// Stale, expired, foreign, mismatched, and replayed confirmations cannot mutate authority.
    pub fn apply_plan(
        &self,
        workspace: WorkspaceId,
        confirmation: SetupPlanConfirmation,
    ) -> Result<SetupPlanReceipt, SetupPlanError> {
        self.ensure_owner(workspace)?;
        let mut state = self.state.lock().map_err(|_| SetupPlanError::Unavailable)?;
        if state.recovery_required {
            return Err(SetupPlanError::RecoveryRequired);
        }
        let mut previews = self
            .previews
            .lock()
            .map_err(|_| SetupPlanError::Unavailable)?;
        let now = Instant::now();
        let stored = previews
            .get(&confirmation.preview_id)
            .ok_or(SetupPlanError::PreviewUnavailable)?;
        if stored.preview.owner_workspace != workspace {
            return Err(SetupPlanError::CrossWorkspacePreview);
        }
        if stored.expires_at <= now {
            previews.remove(&confirmation.preview_id);
            return Err(SetupPlanError::PreviewExpired);
        }
        if stored.preview.preview_sha256 != confirmation.preview_sha256 {
            return Err(SetupPlanError::InvalidConfirmation);
        }
        let stored = previews
            .remove(&confirmation.preview_id)
            .ok_or(SetupPlanError::PreviewUnavailable)?;
        prune_expired(&mut previews, now);
        let current = current_revision(state.document.as_ref());
        if stored.preview.current_revision != current
            || stored.preview.plan.revision()
                != current
                    .checked_add(1)
                    .ok_or(SetupPlanError::RevisionExhausted)?
            || stored.preview.plan_digest != plan_digest(workspace, &stored.preview.plan)?
        {
            return Err(SetupPlanError::StaleRevision);
        }
        let accepted_at_unix_seconds = current_unix_seconds()?;
        let accepted =
            AcceptedSetupPlan::try_new(workspace, stored.preview.plan, accepted_at_unix_seconds)?;
        let candidate = SetupPlanDocument::try_new(workspace, accepted.clone())?;
        if let Err(error) = self.store.store(&candidate.encode()?) {
            state.recovery_required = true;
            return Err(SetupPlanError::Persistence(error));
        }
        let receipt = SetupPlanReceipt {
            revision: accepted.revision,
            digest: accepted.digest,
            accepted_at_unix_seconds: accepted.accepted_at_unix_seconds,
        };
        state.document = Some(candidate);
        Ok(receipt)
    }

    fn ensure_owner(&self, workspace: WorkspaceId) -> Result<(), SetupPlanError> {
        if workspace == self.owner_workspace {
            Ok(())
        } else {
            Err(SetupPlanError::CrossWorkspacePreview)
        }
    }
}

impl fmt::Debug for SetupPlanAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SetupPlanAuthority([WORKSPACE-OWNED DURABLE AUTHORITY])")
    }
}

fn current_revision(document: Option<&SetupPlanDocument>) -> u64 {
    document.map_or(0, SetupPlanDocument::revision)
}

fn next_preview_id(
    previews: &BTreeMap<SetupPreviewId, StoredPreview>,
) -> Result<SetupPreviewId, SetupPlanError> {
    for _ in 0..PREVIEW_ID_ATTEMPTS {
        let candidate = SetupPreviewId(Uuid::new_v4());
        if !previews.contains_key(&candidate) {
            return Ok(candidate);
        }
    }
    Err(SetupPlanError::CapacityExceeded)
}

fn prune_expired(previews: &mut BTreeMap<SetupPreviewId, StoredPreview>, now: Instant) {
    previews.retain(|_, stored| stored.expires_at > now);
}

#[allow(
    clippy::too_many_arguments,
    reason = "the digest binds every independently displayed preview fact"
)]
fn preview_digest(
    preview_id: SetupPreviewId,
    workspace: WorkspaceId,
    current_revision: u64,
    plan_digest: [u8; 32],
    plan: &SetupPlan,
    capabilities: &[SetupCapability],
    contacts: &[SetupExternalContact],
    changes: &[SetupReversibleLocalChange],
    expected_time: SetupTimeEstimate,
    expected_disk: &SetupDiskEstimate,
    safe_skip_steps: &[SetupStepId],
    issued_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
) -> Result<[u8; 32], SetupPlanError> {
    serde_json::to_vec(&(
        "market-squawk-setup-preview-v1",
        preview_id,
        workspace,
        current_revision,
        plan_digest,
        plan,
        capabilities,
        contacts,
        changes,
        expected_time,
        expected_disk,
        safe_skip_steps,
        issued_at_unix_seconds,
        expires_at_unix_seconds,
    ))
    .map(|encoded| Sha256::digest(encoded).into())
    .map_err(|_| SetupPlanError::Encoding)
}

fn current_unix_seconds() -> Result<u64, SetupPlanError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .map_err(|_| SetupPlanError::TimeUnavailable)
}

/// Fail-closed setup-plan validation, preview, persistence, and recovery errors.
#[derive(Debug, Error)]
pub enum SetupPlanError {
    /// The selected goals are empty, duplicated, incompatible, or exceed the closed goal set.
    #[error("setup-plan selection is invalid")]
    InvalidSelection,
    /// A plan or recovered document uses an invalid zero revision.
    #[error("setup-plan revision is invalid")]
    InvalidRevision,
    /// The caller's expected durable revision is not current.
    #[error("setup-plan revision is stale")]
    StaleRevision,
    /// The preview is unknown, already consumed, or was invalidated by process restart.
    #[error("setup-plan preview is unavailable")]
    PreviewUnavailable,
    /// The preview exceeded its bounded process lifetime.
    #[error("setup-plan preview expired")]
    PreviewExpired,
    /// The submitted preview digest does not match the retained immutable preview.
    #[error("setup-plan confirmation is invalid")]
    InvalidConfirmation,
    /// A preview or request does not belong to this exact workspace owner.
    #[error("setup-plan preview belongs to a different workspace")]
    CrossWorkspacePreview,
    /// A monotonic accepted-plan revision cannot advance.
    #[error("setup-plan revision space is exhausted")]
    RevisionExhausted,
    /// A checked vector, registry, preview, or durable payload exceeded its hard bound.
    #[error("setup-plan capacity is exhausted")]
    CapacityExceeded,
    /// Durable state is malformed, unsupported, owner-mismatched, or semantically inconsistent.
    #[error("setup-plan durable state is corrupt")]
    CorruptState,
    /// In-process authority serialization is unavailable after abnormal unwind.
    #[error("setup-plan authority is unavailable")]
    Unavailable,
    /// An interrupted durable publication requires process restart and state-store recovery.
    #[error("setup-plan persistence recovery is required")]
    RecoveryRequired,
    /// Wall or monotonic time could not represent the required preview lifetime.
    #[error("setup-plan time is unavailable")]
    TimeUnavailable,
    /// Canonical closed DTO encoding failed.
    #[error("setup-plan encoding failed")]
    Encoding,
    /// The crash-safe local authority-state store rejected open, recovery, or publication.
    #[error("setup-plan persistence failed")]
    Persistence(#[from] LocalAuthorityStateStoreError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_squawk_runtime::WorkspaceId;
    use uuid::Uuid;

    #[test]
    fn stale_and_replayed_previews_cannot_change_the_accepted_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let workspace = WorkspaceId::try_from_uuid(Uuid::from_u128(1))?;
        let authority = SetupPlanAuthority::try_open(directory.path(), workspace)?;
        let selection = SetupPlanSelection::recommended(vec![SetupGoal::EverythingRecommended])?;
        let first = authority.preview_plan(workspace, 0, selection.clone())?;
        let stale = authority.preview_plan(workspace, 0, selection)?;

        let first_confirmation =
            SetupPlanConfirmation::new(first.preview_id(), first.preview_sha256());
        let accepted = authority.apply_plan(workspace, first_confirmation)?;
        assert_eq!(accepted.revision(), 1);
        assert!(matches!(
            authority.apply_plan(
                workspace,
                SetupPlanConfirmation::new(stale.preview_id(), stale.preview_sha256())
            ),
            Err(SetupPlanError::StaleRevision)
        ));
        assert!(matches!(
            authority.apply_plan(workspace, first_confirmation),
            Err(SetupPlanError::PreviewUnavailable)
        ));
        assert_eq!(
            authority
                .status()?
                .accepted_plan()
                .map(AcceptedSetupPlan::digest),
            Some(accepted.digest())
        );
        Ok(())
    }

    #[test]
    fn reopen_returns_the_exact_durable_accepted_plan_revision_and_digest()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let workspace = WorkspaceId::try_from_uuid(Uuid::from_u128(2))?;
        let expected_plan;
        let expected_receipt;
        {
            let authority = SetupPlanAuthority::try_open(directory.path(), workspace)?;
            let preview = authority.preview_plan(
                workspace,
                0,
                SetupPlanSelection::recommended(vec![SetupGoal::ManagePortfolio])?,
            )?;
            expected_plan = preview.plan().clone();
            expected_receipt = authority.apply_plan(
                workspace,
                SetupPlanConfirmation::new(preview.preview_id(), preview.preview_sha256()),
            )?;
        }

        let reopened = SetupPlanAuthority::try_open(directory.path(), workspace)?;
        let status = reopened.status()?;
        let accepted = status.accepted_plan().ok_or(SetupPlanError::CorruptState)?;
        assert_eq!(accepted.revision(), expected_receipt.revision());
        assert_eq!(accepted.digest(), expected_receipt.digest());
        assert_eq!(accepted.plan(), &expected_plan);
        Ok(())
    }
}
