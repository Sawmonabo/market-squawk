//! Concrete installed-product settings persistence and lifecycle coordination.

use std::{
    collections::BTreeSet,
    fmt,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

use market_squawk_platform::LocalAuthorityStateStore;
use market_squawk_services::ServiceError;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::application::{
    operations::{
        ManagedSettingsOperations, ManagedSettingsRollbackApproval, ManagedSettingsRollbackPreview,
    },
    settings::{
        DurableSettingsStore, RestartImpact, SettingKey, SettingsChangeApproval, SettingsError,
        SettingsReceipt, SettingsSeed, SettingsSnapshot, WorkspaceSettingsBackup,
    },
};

const JOURNAL_DIRECTORY: &str = "settings-operations";
const JOURNAL_SCHEMA_VERSION: u16 = 1;
const MAXIMUM_RETAINED_RECEIPTS: usize = 64;
const WORKSPACE_CONFIGURATION_FORMAT_VERSION: u16 = 1;

pub(crate) type SettingsApplicationAction = dyn Fn(&SettingsSnapshot) -> Result<SettingsApplicationProof, ServiceError>
    + Send
    + Sync
    + 'static;
pub(crate) type SettingsRestartAction =
    dyn Fn(SettingsRestartHandoff) -> Result<(), ServiceError> + Send + Sync + 'static;

/// Service-owned proof that one exact durable snapshot is active in every bound consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SettingsApplicationProof {
    revision: u64,
    digest: [u8; 32],
}

impl SettingsApplicationProof {
    /// Binds a successful consumer application or startup health result to one exact snapshot.
    #[must_use]
    pub(crate) const fn for_snapshot(snapshot: &SettingsSnapshot) -> Self {
        Self {
            revision: snapshot.revision(),
            digest: snapshot.digest(),
        }
    }

    fn matches(self, snapshot: &SettingsSnapshot) -> bool {
        self.revision == snapshot.revision() && self.digest == snapshot.digest()
    }
}

/// Why a durable settings transition requires a supervisor-owned process generation change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SettingsRestartReason {
    /// A newly persisted restart-impacting snapshot awaits first startup.
    Apply,
    /// Successor health failed and the durable settings authority rolled back.
    RollbackRecovery,
}

/// Digest-bound restart handoff passed only to the installed-service lifecycle owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SettingsRestartHandoff {
    revision: u64,
    digest: [u8; 32],
    reason: SettingsRestartReason,
}

impl SettingsRestartHandoff {
    #[must_use]
    const fn for_snapshot(snapshot: &SettingsSnapshot, reason: SettingsRestartReason) -> Self {
        Self {
            revision: snapshot.revision(),
            digest: snapshot.digest(),
            reason,
        }
    }

    /// Returns the exact durable revision the successor must load.
    #[must_use]
    pub(crate) const fn revision(self) -> u64 {
        self.revision
    }

    /// Returns the digest of the exact durable snapshot the successor must load.
    #[must_use]
    pub(crate) const fn digest(self) -> [u8; 32] {
        self.digest
    }
}

/// Successor-startup decision made before its runtime endpoint may be published.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SettingsStartupReconciliation {
    /// The exact durable settings snapshot is active and health-proven.
    Ready,
    /// The attempted snapshot failed health and was rolled back durably.
    ///
    /// Startup must remain unpublished and the supervisor must start another process generation
    /// using this exact rollback snapshot.
    RollbackRestartRequired(SettingsRestartHandoff),
}

/// Closed callbacks supplied by the installed-service composition owner.
///
/// Callbacks receive only a validated typed snapshot. They never receive a configuration path,
/// raw configuration document, environment map, or secret material.
#[derive(Clone)]
pub(crate) struct SettingsLifecycleAuthority {
    reload: Arc<SettingsApplicationAction>,
    signal_restart: Arc<SettingsRestartAction>,
    startup_health: Arc<SettingsApplicationAction>,
}

impl SettingsLifecycleAuthority {
    /// Binds every closed setting key to reload, restart, and startup-health ownership.
    ///
    /// The constructor fails closed unless the installed-service composition declares every
    /// setting key exactly once. A callback returning [`SettingsApplicationProof`] asserts that
    /// its bound consumers actually applied and observed that exact snapshot; it must not be a
    /// no-op acknowledgement.
    pub(crate) fn try_new(
        supported_keys: impl IntoIterator<Item = SettingKey>,
        reload: Arc<SettingsApplicationAction>,
        signal_restart: Arc<SettingsRestartAction>,
        startup_health: Arc<SettingsApplicationAction>,
    ) -> Result<Self, ServiceError> {
        let supported_keys = supported_keys.into_iter().collect::<Vec<_>>();
        let supported = supported_keys.iter().copied().collect::<BTreeSet<_>>();
        let required = SettingKey::all().into_iter().collect::<BTreeSet<_>>();
        if supported_keys.len() != required.len() || supported != required {
            return Err(ServiceError::Unavailable);
        }
        Ok(Self {
            reload,
            signal_restart,
            startup_health,
        })
    }

    fn prove(
        action: &SettingsApplicationAction,
        snapshot: &SettingsSnapshot,
    ) -> Result<(), ServiceError> {
        if action(snapshot)?.matches(snapshot) {
            Ok(())
        } else {
            Err(ServiceError::InvalidResult)
        }
    }

    fn apply_reload(&self, snapshot: &SettingsSnapshot) -> Result<(), ServiceError> {
        Self::prove(self.reload.as_ref(), snapshot)
    }

    fn prove_startup(&self, snapshot: &SettingsSnapshot) -> Result<(), ServiceError> {
        Self::prove(self.startup_health.as_ref(), snapshot)
    }

    fn signal_restart(&self, handoff: SettingsRestartHandoff) -> Result<(), ServiceError> {
        (self.signal_restart)(handoff)
    }
}

impl fmt::Debug for SettingsLifecycleAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SettingsLifecycleAuthority([REDACTED SERVICE ACTIONS])")
    }
}

/// Sole installed adapter for typed settings persistence and service lifecycle application.
pub(crate) struct ProductionSettingsOperations {
    settings: Arc<DurableSettingsStore>,
    lifecycle: SettingsLifecycleAuthority,
    journal: SettingsOperationJournal,
    transaction: Mutex<()>,
}

impl ProductionSettingsOperations {
    /// Opens the durable receipt journal without consuming successor-startup authority.
    ///
    /// `control_root` is the already-prepared private product control root. `settings` must be the
    /// same exclusive store published through `OperationsApplicationServices`. `lifecycle` must be
    /// owned by the installed-service composition and act on that same product instance.
    pub(crate) fn try_new(
        control_root: &Path,
        settings: Arc<DurableSettingsStore>,
        lifecycle: SettingsLifecycleAuthority,
    ) -> Result<Self, ServiceError> {
        Ok(Self {
            settings,
            lifecycle,
            journal: SettingsOperationJournal::try_open(control_root)?,
            transaction: Mutex::new(()),
        })
    }

    fn acquire(&self) -> Result<MutexGuard<'_, ()>, ServiceError> {
        self.transaction
            .lock()
            .map_err(|_| ServiceError::Unavailable)
    }

    /// Retains one complete, no-pending configuration export under the settings transaction.
    ///
    /// The returned bytes contain only validated local settings state, non-secret startup
    /// evidence, and the completed receipt chain. The export neither reads nor serializes a
    /// data directory, training release path, source locator, secret, or ambient input.
    pub(crate) fn retain_workspace_configuration(
        &self,
    ) -> Result<RetainedWorkspaceConfiguration, ServiceError> {
        let _transaction = self.acquire()?;
        let settings = self
            .settings
            .export_workspace_backup()
            .map_err(map_settings_error)?;
        let journal = self.journal.completed_snapshot()?;
        let backup = WorkspaceConfigurationBackup::try_new(settings, journal)?;
        RetainedWorkspaceConfiguration::try_new(backup)
    }

    /// Rechecks that the current fenced configuration has the exact retained semantic identity.
    pub(crate) fn revalidate_workspace_configuration(
        &self,
        retained: &RetainedWorkspaceConfiguration,
    ) -> Result<(), ServiceError> {
        let current = self.retain_workspace_configuration()?;
        if current.authority_revision_sha256 == retained.authority_revision_sha256 {
            Ok(())
        } else {
            Err(ServiceError::InvalidResult)
        }
    }

    /// Restores a configuration component only into absent settings and journal stores, then
    /// performs the normal checked settings reopen before returning the lifecycle authority.
    pub(crate) fn restore_workspace_configuration_absent(
        control_root: &Path,
        seed: SettingsSeed,
        lifecycle: SettingsLifecycleAuthority,
        canonical_bytes: &[u8],
    ) -> Result<Self, ServiceError> {
        if canonical_bytes.is_empty()
            || canonical_bytes.len() > LocalAuthorityStateStore::maximum_payload_bytes()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let backup = serde_json::from_slice::<WorkspaceConfigurationBackup>(canonical_bytes)
            .map_err(|_| ServiceError::InvalidRequest)?
            .validate()?;
        DurableSettingsStore::ensure_workspace_backup_target_absent(control_root)
            .map_err(map_settings_error)?;
        SettingsOperationJournal::ensure_absent(control_root)?;
        let settings = Arc::new(
            DurableSettingsStore::restore_workspace_backup_absent(
                control_root,
                seed,
                backup.settings,
            )
            .map_err(map_settings_error)?,
        );
        SettingsOperationJournal::restore_absent(control_root, backup.journal)?;
        Self::try_new(control_root, settings, lifecycle)
    }

    fn apply_lifecycle(
        &self,
        receipt: SettingsReceipt,
        snapshot: &SettingsSnapshot,
    ) -> Result<(), ServiceError> {
        if receipt.active_revision() != snapshot.revision()
            || receipt.active_digest() != snapshot.digest()
        {
            return Err(ServiceError::InvalidResult);
        }
        match receipt.restart_impact() {
            RestartImpact::None | RestartImpact::ServiceReload => {
                self.lifecycle.apply_reload(snapshot)
            }
            RestartImpact::ServiceRestart => Err(ServiceError::InvalidResult),
        }
    }

    fn recover_after_failure(
        &self,
        previous_revision: u64,
        attempted: SettingsReceipt,
        original: ServiceError,
    ) -> Result<SettingsReceipt, ServiceError> {
        let _recovery_intent = self.journal.begin_recovery(
            attempted.active_revision(),
            attempted.active_digest(),
            attempted.restart_impact(),
        );
        let recovery = self
            .settings
            .rollback(attempted.active_revision(), previous_revision)
            .map_err(map_recovery_error)?;
        let recovered_snapshot = self.settings.snapshot().map_err(map_recovery_error)?;
        self.apply_lifecycle(recovery, &recovered_snapshot)
            .map_err(|_| ServiceError::Internal)?;
        self.journal
            .complete(
                OperationDisposition::Recovered,
                attempted,
                recovery,
                &recovered_snapshot,
            )
            .map_err(|_| ServiceError::Internal)?;
        Err(original)
    }

    /// Signals one pending restart only after the response carrying its receipt committed.
    ///
    /// The installed response owner calls this after a successful response write. A failed signal
    /// leaves the durable handoff pending so a later response-drain poll can retry it. The bound
    /// restart callback must therefore be idempotent for an identical handoff.
    pub(crate) fn signal_pending_restart_after_response(&self) -> Result<bool, ServiceError> {
        let _transaction = self.acquire()?;
        let Some(pending) = self.journal.pending()? else {
            return Ok(false);
        };
        let (attempted_digest, attempted_impact) = match pending.phase {
            PendingPhase::Applied {
                attempted_digest,
                attempted_impact: RestartImpact::ServiceRestart,
            } => (attempted_digest, RestartImpact::ServiceRestart),
            _ => return Ok(false),
        };
        let current = self.settings.snapshot().map_err(map_settings_error)?;
        if attempted_impact != RestartImpact::ServiceRestart
            || current.revision() != pending.attempted_revision
            || current.digest() != attempted_digest
        {
            return Err(ServiceError::Unavailable);
        }
        self.lifecycle
            .signal_restart(SettingsRestartHandoff::for_snapshot(
                &current,
                SettingsRestartReason::Apply,
            ))?;
        Ok(true)
    }

    /// Reconciles settings after all consumers are composed but before startup publication.
    ///
    /// The service must stop startup on any error. A rollback outcome requires one unpublished
    /// supervisor restart; the following successor calls this method again to health-prove and
    /// finalize the recovered snapshot.
    pub(crate) fn reconcile_successor_startup(
        &self,
    ) -> Result<SettingsStartupReconciliation, ServiceError> {
        let _transaction = self.acquire()?;
        let current = self.settings.snapshot().map_err(map_settings_error)?;
        let Some(pending) = self.journal.pending()? else {
            self.lifecycle.prove_startup(&current)?;
            return Ok(SettingsStartupReconciliation::Ready);
        };

        if matches!(pending.phase, PendingPhase::Persisting)
            && current.revision() == pending.previous_revision
            && current.digest() == pending.previous_digest
        {
            self.journal.abandon()?;
            self.lifecycle.prove_startup(&current)?;
            return Ok(SettingsStartupReconciliation::Ready);
        }
        match pending.phase {
            PendingPhase::Recovering {
                attempted_digest,
                attempted_impact,
            } => {
                if current.revision() <= pending.attempted_revision
                    || current.digest() == attempted_digest
                {
                    return Err(ServiceError::Unavailable);
                }
                self.lifecycle.prove_startup(&current)?;
                self.journal.complete_values(
                    OperationDisposition::Recovered,
                    RecoveredAttempt {
                        active_revision: pending.attempted_revision,
                        active_digest: attempted_digest,
                        restart_impact: attempted_impact,
                    },
                    current.revision(),
                    current.digest(),
                    &current,
                )?;
                Ok(SettingsStartupReconciliation::Ready)
            }
            phase => {
                let attempted_impact = match phase.attempted_impact() {
                    Some(impact) => impact,
                    None => self
                        .settings
                        .preview_rollback(current.revision(), pending.previous_revision)
                        .map_err(map_recovery_error)?
                        .restart_impact(),
                };
                let attempted_digest = phase.attempted_digest().unwrap_or(current.digest());
                if current.revision() != pending.attempted_revision
                    || current.digest() != attempted_digest
                {
                    return Err(ServiceError::Unavailable);
                }
                if self.lifecycle.prove_startup(&current).is_ok() {
                    self.journal.complete_values(
                        OperationDisposition::Applied,
                        RecoveredAttempt {
                            active_revision: current.revision(),
                            active_digest: current.digest(),
                            restart_impact: attempted_impact,
                        },
                        current.revision(),
                        current.digest(),
                        &current,
                    )?;
                    return Ok(SettingsStartupReconciliation::Ready);
                }
                self.journal.begin_recovery(
                    current.revision(),
                    current.digest(),
                    attempted_impact,
                )?;
                self.settings
                    .rollback(current.revision(), pending.previous_revision)
                    .map_err(map_recovery_error)?;
                let recovered = self.settings.snapshot().map_err(map_recovery_error)?;
                Ok(SettingsStartupReconciliation::RollbackRestartRequired(
                    SettingsRestartHandoff::for_snapshot(
                        &recovered,
                        SettingsRestartReason::RollbackRecovery,
                    ),
                ))
            }
        }
    }

    /// Transfers one already-durable successor handoff to the service lifecycle owner.
    pub(crate) fn signal_restart_handoff(
        &self,
        handoff: SettingsRestartHandoff,
    ) -> Result<(), ServiceError> {
        self.lifecycle.signal_restart(handoff)
    }

    fn apply_persisted(
        &self,
        before: &SettingsSnapshot,
        receipt: SettingsReceipt,
    ) -> Result<SettingsReceipt, ServiceError> {
        let after = match self.settings.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return self.recover_after_failure(
                    before.revision(),
                    receipt,
                    map_settings_error(error),
                );
            }
        };
        if receipt.active_revision() != before.revision().checked_add(1).unwrap_or(0)
            || receipt.active_revision() != after.revision()
            || receipt.active_digest() != after.digest()
        {
            return self.recover_after_failure(
                before.revision(),
                receipt,
                ServiceError::InvalidResult,
            );
        }
        if let Err(error) = self.journal.mark_applied(receipt, &after) {
            return self.recover_after_failure(before.revision(), receipt, error);
        }
        if receipt.restart_impact() == RestartImpact::ServiceRestart {
            return Ok(receipt);
        }
        if let Err(error) = self.apply_lifecycle(receipt, &after) {
            return self.recover_after_failure(before.revision(), receipt, error);
        }
        if let Err(error) =
            self.journal
                .complete(OperationDisposition::Applied, receipt, receipt, &after)
        {
            return self.recover_after_failure(before.revision(), receipt, error);
        }
        Ok(receipt)
    }
}

impl ManagedSettingsOperations for ProductionSettingsOperations {
    fn apply_change(
        &self,
        approval: SettingsChangeApproval,
    ) -> Result<SettingsReceipt, ServiceError> {
        let _transaction = self.acquire()?;
        let before = self.settings.snapshot().map_err(map_settings_error)?;
        self.journal.begin(OperationKind::Change, &before)?;
        let receipt = match self.settings.apply(approval) {
            Ok(receipt) => receipt,
            Err(error) => {
                self.journal.abandon()?;
                return Err(map_settings_error(error));
            }
        };
        self.apply_persisted(&before, receipt)
    }

    fn preview_rollback(
        &self,
        expected_revision: u64,
        target_revision: u64,
    ) -> Result<ManagedSettingsRollbackPreview, ServiceError> {
        let _transaction = self.acquire()?;
        let preview = self
            .settings
            .preview_rollback(expected_revision, target_revision)
            .map_err(map_settings_error)?;
        ManagedSettingsRollbackPreview::try_new(
            expected_revision,
            target_revision,
            matches!(preview.restart_impact(), RestartImpact::ServiceRestart),
            preview.preview_sha256(),
        )
    }

    fn apply_rollback(
        &self,
        approval: ManagedSettingsRollbackApproval,
    ) -> Result<SettingsReceipt, ServiceError> {
        let _transaction = self.acquire()?;
        let before = self.settings.snapshot().map_err(map_settings_error)?;
        if before.revision() != approval.current_revision() {
            return Err(ServiceError::InvalidRequest);
        }
        let preview = self
            .settings
            .preview_rollback(approval.current_revision(), approval.target_revision())
            .map_err(map_settings_error)?;
        if preview.preview_sha256() != approval.digest() {
            return Err(ServiceError::InvalidRequest);
        }
        self.journal.begin(
            OperationKind::Rollback {
                target_revision: approval.target_revision(),
                preview_sha256: approval.digest(),
            },
            &before,
        )?;
        let receipt = match self
            .settings
            .rollback(approval.current_revision(), approval.target_revision())
        {
            Ok(receipt) => receipt,
            Err(error) => {
                self.journal.abandon()?;
                return Err(map_settings_error(error));
            }
        };
        if receipt.active_digest() != preview.resulting_digest() {
            return self.recover_after_failure(
                before.revision(),
                receipt,
                ServiceError::InvalidResult,
            );
        }
        self.apply_persisted(&before, receipt)
    }
}

impl fmt::Debug for ProductionSettingsOperations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionSettingsOperations([DURABLE SETTINGS LIFECYCLE])")
    }
}

/// Canonical configuration component emitted by the settings lifecycle owner.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkspaceConfigurationBackup {
    format_version: u16,
    settings: WorkspaceSettingsBackup,
    journal: JournalDocument,
    semantic_authority_sha256: [u8; 32],
}

impl WorkspaceConfigurationBackup {
    fn try_new(
        settings: WorkspaceSettingsBackup,
        journal: JournalDocument,
    ) -> Result<Self, ServiceError> {
        let settings = settings.validate().map_err(map_settings_error)?;
        let (revision, digest) = settings.startup_binding();
        let journal = journal.validate_completed_for(revision, digest)?;
        let semantic_authority_sha256 =
            configuration_semantic_authority_digest(&settings, &journal)?;
        Ok(Self {
            format_version: WORKSPACE_CONFIGURATION_FORMAT_VERSION,
            settings,
            journal,
            semantic_authority_sha256,
        })
    }

    fn validate(self) -> Result<Self, ServiceError> {
        if self.format_version != WORKSPACE_CONFIGURATION_FORMAT_VERSION {
            return Err(ServiceError::InvalidRequest);
        }
        let expected = self.semantic_authority_sha256;
        let validated = Self::try_new(self.settings, self.journal)?;
        if expected != validated.semantic_authority_sha256 {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(validated)
    }

    fn canonical_bytes(&self) -> Result<Vec<u8>, ServiceError> {
        let bytes = serde_json::to_vec(self).map_err(|_| ServiceError::Internal)?;
        if bytes.is_empty() || bytes.len() > LocalAuthorityStateStore::maximum_payload_bytes() {
            return Err(ServiceError::ResourceExhausted);
        }
        Ok(bytes)
    }
}

/// Exact retained configuration bytes and their independent semantic authority identity.
#[derive(Clone, Debug)]
pub(crate) struct RetainedWorkspaceConfiguration {
    canonical_bytes: Vec<u8>,
    authority_revision_sha256: [u8; 32],
}

impl RetainedWorkspaceConfiguration {
    fn try_new(backup: WorkspaceConfigurationBackup) -> Result<Self, ServiceError> {
        let canonical_bytes = backup.canonical_bytes()?;
        Ok(Self {
            canonical_bytes,
            authority_revision_sha256: backup.semantic_authority_sha256,
        })
    }

    /// Returns the bounded canonical configuration-component bytes.
    #[must_use]
    pub(crate) fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Returns the semantic revision identity certified by the settings transaction.
    #[must_use]
    pub(crate) const fn authority_revision_sha256(&self) -> [u8; 32] {
        self.authority_revision_sha256
    }
}

fn configuration_semantic_authority_digest(
    settings: &WorkspaceSettingsBackup,
    journal: &JournalDocument,
) -> Result<[u8; 32], ServiceError> {
    serde_json::to_vec(&(
        "market-squawk-workspace-configuration-v1",
        settings,
        journal,
    ))
    .map(|bytes| Sha256::digest(bytes).into())
    .map_err(|_| ServiceError::Internal)
}

#[derive(Clone, Copy, Debug)]
struct RecoveredAttempt {
    active_revision: u64,
    active_digest: [u8; 32],
    restart_impact: RestartImpact,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum OperationKind {
    Change,
    Rollback {
        target_revision: u64,
        preview_sha256: [u8; 32],
    },
}

impl OperationKind {
    fn is_valid_for(&self, previous_revision: u64) -> bool {
        match self {
            Self::Change => true,
            Self::Rollback {
                target_revision,
                preview_sha256,
            } => {
                *target_revision > 0
                    && *target_revision < previous_revision
                    && *preview_sha256 != [0; 32]
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "phase")]
enum PendingPhase {
    Persisting,
    Applied {
        attempted_digest: [u8; 32],
        attempted_impact: RestartImpact,
    },
    Recovering {
        attempted_digest: [u8; 32],
        attempted_impact: RestartImpact,
    },
}

impl PendingPhase {
    const fn attempted_impact(self) -> Option<RestartImpact> {
        match self {
            Self::Persisting => None,
            Self::Applied {
                attempted_impact, ..
            }
            | Self::Recovering {
                attempted_impact, ..
            } => Some(attempted_impact),
        }
    }

    const fn attempted_digest(self) -> Option<[u8; 32]> {
        match self {
            Self::Persisting => None,
            Self::Applied {
                attempted_digest, ..
            }
            | Self::Recovering {
                attempted_digest, ..
            } => Some(attempted_digest),
        }
    }

    fn is_valid(self) -> bool {
        match self {
            Self::Persisting => true,
            Self::Applied {
                attempted_digest, ..
            }
            | Self::Recovering {
                attempted_digest, ..
            } => attempted_digest != [0; 32],
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PendingOperation {
    sequence: u64,
    operation: OperationKind,
    previous_revision: u64,
    previous_digest: [u8; 32],
    attempted_revision: u64,
    phase: PendingPhase,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum OperationDisposition {
    Applied,
    Recovered,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OperationReceiptRecord {
    sequence: u64,
    operation: OperationKind,
    previous_revision: u64,
    attempted_revision: u64,
    active_revision: u64,
    attempted_digest: [u8; 32],
    active_digest: [u8; 32],
    restart_impact: RestartImpact,
    disposition: OperationDisposition,
    prior_receipt_sha256: [u8; 32],
    receipt_sha256: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JournalDocument {
    schema_version: u16,
    next_sequence: u64,
    anchor_sha256: [u8; 32],
    pending: Option<PendingOperation>,
    receipts: Vec<OperationReceiptRecord>,
    document_sha256: [u8; 32],
}

impl JournalDocument {
    fn initial() -> Result<Self, ServiceError> {
        let mut document = Self {
            schema_version: JOURNAL_SCHEMA_VERSION,
            next_sequence: 1,
            anchor_sha256: [0; 32],
            pending: None,
            receipts: Vec::new(),
            document_sha256: [0; 32],
        };
        document.refresh_digest()?;
        Ok(document)
    }

    fn validate(mut self) -> Result<Self, ServiceError> {
        let expected = self.document_sha256;
        self.refresh_digest()?;
        if self.schema_version != JOURNAL_SCHEMA_VERSION
            || self.receipts.len() > MAXIMUM_RETAINED_RECEIPTS
            || self.next_sequence == 0
            || expected != self.document_sha256
        {
            return Err(ServiceError::Unavailable);
        }
        let mut prior = self.anchor_sha256;
        let mut sequence = 0;
        for receipt in &self.receipts {
            if receipt.sequence <= sequence
                || receipt.prior_receipt_sha256 != prior
                || receipt.receipt_sha256 != receipt_digest(receipt)?
                || !receipt.operation.is_valid_for(receipt.previous_revision)
                || receipt.active_revision < receipt.attempted_revision
                || receipt.previous_revision >= receipt.attempted_revision
                || receipt.attempted_digest == [0; 32]
                || receipt.active_digest == [0; 32]
            {
                return Err(ServiceError::Unavailable);
            }
            sequence = receipt.sequence;
            prior = receipt.receipt_sha256;
        }
        if self.next_sequence != sequence.checked_add(1).unwrap_or(0)
            || self.pending.as_ref().is_some_and(|pending| {
                pending.sequence != self.next_sequence
                    || pending.previous_revision == 0
                    || !pending.operation.is_valid_for(pending.previous_revision)
                    || pending.attempted_revision
                        != pending.previous_revision.checked_add(1).unwrap_or(0)
                    || pending.previous_digest == [0; 32]
                    || !pending.phase.is_valid()
            })
        {
            return Err(ServiceError::Unavailable);
        }
        Ok(self)
    }

    fn refresh_digest(&mut self) -> Result<(), ServiceError> {
        self.document_sha256 = journal_digest(self)?;
        Ok(())
    }

    fn validate_completed_for(
        self,
        active_revision: u64,
        active_digest: [u8; 32],
    ) -> Result<Self, ServiceError> {
        let document = self.validate()?;
        if document.pending.is_some()
            || document.receipts.last().is_some_and(|receipt| {
                receipt.active_revision != active_revision || receipt.active_digest != active_digest
            })
            || (document.receipts.is_empty() && active_revision != 1)
        {
            return Err(ServiceError::Unavailable);
        }
        Ok(document)
    }
}

struct SettingsOperationJournal {
    store: LocalAuthorityStateStore,
    document: Mutex<JournalDocument>,
}

impl SettingsOperationJournal {
    fn try_open(control_root: &Path) -> Result<Self, ServiceError> {
        let store = LocalAuthorityStateStore::try_open(control_root.join(JOURNAL_DIRECTORY))
            .map_err(|_| ServiceError::Unavailable)?;
        let document = match store.load().map_err(|_| ServiceError::Unavailable)? {
            Some(bytes) => serde_json::from_slice::<JournalDocument>(&bytes)
                .map_err(|_| ServiceError::Unavailable)?
                .validate()?,
            None => {
                let document = JournalDocument::initial()?;
                store_document(&store, &document)?;
                document
            }
        };
        Ok(Self {
            store,
            document: Mutex::new(document),
        })
    }

    fn pending(&self) -> Result<Option<PendingOperation>, ServiceError> {
        self.document
            .lock()
            .map(|document| document.pending.clone())
            .map_err(|_| ServiceError::Unavailable)
    }

    fn completed_snapshot(&self) -> Result<JournalDocument, ServiceError> {
        self.document
            .lock()
            .map(|document| document.clone())
            .map_err(|_| ServiceError::Unavailable)
    }

    fn ensure_absent(control_root: &Path) -> Result<(), ServiceError> {
        let store = LocalAuthorityStateStore::try_open(control_root.join(JOURNAL_DIRECTORY))
            .map_err(|_| ServiceError::Unavailable)?;
        if store
            .load()
            .map_err(|_| ServiceError::Unavailable)?
            .is_some()
        {
            Err(ServiceError::InvalidResult)
        } else {
            Ok(())
        }
    }

    fn restore_absent(control_root: &Path, document: JournalDocument) -> Result<(), ServiceError> {
        let document = document.validate()?;
        let store = LocalAuthorityStateStore::try_open(control_root.join(JOURNAL_DIRECTORY))
            .map_err(|_| ServiceError::Unavailable)?;
        if store
            .load()
            .map_err(|_| ServiceError::Unavailable)?
            .is_some()
        {
            return Err(ServiceError::InvalidResult);
        }
        store_document(&store, &document)
    }

    fn begin(
        &self,
        operation: OperationKind,
        before: &SettingsSnapshot,
    ) -> Result<(), ServiceError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        if document.pending.is_some() {
            return Err(ServiceError::Unavailable);
        }
        let attempted_revision = before
            .revision()
            .checked_add(1)
            .ok_or(ServiceError::Internal)?;
        let mut candidate = document.clone();
        candidate.pending = Some(PendingOperation {
            sequence: candidate.next_sequence,
            operation,
            previous_revision: before.revision(),
            previous_digest: before.digest(),
            attempted_revision,
            phase: PendingPhase::Persisting,
        });
        commit_document(&self.store, &mut document, candidate)
    }

    fn mark_applied(
        &self,
        receipt: SettingsReceipt,
        snapshot: &SettingsSnapshot,
    ) -> Result<(), ServiceError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        let pending = document.pending.as_ref().ok_or(ServiceError::Unavailable)?;
        if pending.attempted_revision != receipt.active_revision()
            || snapshot.revision() != receipt.active_revision()
            || snapshot.digest() != receipt.active_digest()
        {
            return Err(ServiceError::InvalidResult);
        }
        let mut candidate = document.clone();
        candidate
            .pending
            .as_mut()
            .ok_or(ServiceError::Unavailable)?
            .phase = PendingPhase::Applied {
            attempted_digest: snapshot.digest(),
            attempted_impact: receipt.restart_impact(),
        };
        commit_document(&self.store, &mut document, candidate)
    }

    fn begin_recovery(
        &self,
        attempted_revision: u64,
        attempted_digest: [u8; 32],
        impact: RestartImpact,
    ) -> Result<(), ServiceError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        let pending = document.pending.as_ref().ok_or(ServiceError::Unavailable)?;
        if attempted_revision < pending.attempted_revision || attempted_digest == [0; 32] {
            return Err(ServiceError::InvalidResult);
        }
        let attempted_digest = pending.phase.attempted_digest().unwrap_or(attempted_digest);
        let attempted_impact = pending.phase.attempted_impact().unwrap_or(impact);
        let mut candidate = document.clone();
        candidate
            .pending
            .as_mut()
            .ok_or(ServiceError::Unavailable)?
            .phase = PendingPhase::Recovering {
            attempted_digest,
            attempted_impact,
        };
        commit_document(&self.store, &mut document, candidate)
    }

    fn abandon(&self) -> Result<(), ServiceError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        if !matches!(
            document.pending.as_ref().map(|pending| pending.phase),
            Some(PendingPhase::Persisting)
        ) {
            return Err(ServiceError::Unavailable);
        }
        let mut candidate = document.clone();
        candidate.pending = None;
        commit_document(&self.store, &mut document, candidate)
    }

    fn complete(
        &self,
        disposition: OperationDisposition,
        attempted: SettingsReceipt,
        active: SettingsReceipt,
        active_snapshot: &SettingsSnapshot,
    ) -> Result<(), ServiceError> {
        self.complete_values(
            disposition,
            RecoveredAttempt {
                active_revision: attempted.active_revision(),
                active_digest: attempted.active_digest(),
                restart_impact: attempted.restart_impact(),
            },
            active.active_revision(),
            active.active_digest(),
            active_snapshot,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "attempted and active evidence are independently validated"
    )]
    fn complete_values(
        &self,
        disposition: OperationDisposition,
        attempted: RecoveredAttempt,
        active_revision: u64,
        active_digest: [u8; 32],
        active_snapshot: &SettingsSnapshot,
    ) -> Result<(), ServiceError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        let pending = document
            .pending
            .as_ref()
            .cloned()
            .ok_or(ServiceError::Unavailable)?;
        if attempted.active_revision < pending.attempted_revision
            || attempted.active_digest == [0; 32]
            || active_revision != active_snapshot.revision()
            || active_digest != active_snapshot.digest()
            || (matches!(disposition, OperationDisposition::Applied)
                && active_revision != pending.attempted_revision)
            || (matches!(disposition, OperationDisposition::Recovered)
                && active_revision <= pending.attempted_revision)
        {
            return Err(ServiceError::InvalidResult);
        }
        let prior_receipt_sha256 = document
            .receipts
            .last()
            .map_or(document.anchor_sha256, |receipt| receipt.receipt_sha256);
        let mut receipt = OperationReceiptRecord {
            sequence: pending.sequence,
            operation: pending.operation,
            previous_revision: pending.previous_revision,
            attempted_revision: pending.attempted_revision,
            active_revision,
            attempted_digest: attempted.active_digest,
            active_digest,
            restart_impact: attempted.restart_impact,
            disposition,
            prior_receipt_sha256,
            receipt_sha256: [0; 32],
        };
        receipt.receipt_sha256 = receipt_digest(&receipt)?;
        let mut candidate = document.clone();
        candidate.pending = None;
        candidate.next_sequence = candidate
            .next_sequence
            .checked_add(1)
            .ok_or(ServiceError::Internal)?;
        candidate.receipts.push(receipt);
        if candidate.receipts.len() > MAXIMUM_RETAINED_RECEIPTS {
            let removed = candidate.receipts.remove(0);
            candidate.anchor_sha256 = removed.receipt_sha256;
        }
        commit_document(&self.store, &mut document, candidate)
    }
}

impl fmt::Debug for SettingsOperationJournal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SettingsOperationJournal([DURABLE REDACTED RECEIPTS])")
    }
}

fn commit_document(
    store: &LocalAuthorityStateStore,
    current: &mut JournalDocument,
    mut candidate: JournalDocument,
) -> Result<(), ServiceError> {
    candidate.refresh_digest()?;
    store_document(store, &candidate)?;
    *current = candidate;
    Ok(())
}

fn store_document(
    store: &LocalAuthorityStateStore,
    document: &JournalDocument,
) -> Result<(), ServiceError> {
    let encoded = serde_json::to_vec(document).map_err(|_| ServiceError::Internal)?;
    if encoded.len() > LocalAuthorityStateStore::maximum_payload_bytes() {
        return Err(ServiceError::ResourceExhausted);
    }
    store.store(&encoded).map_err(|_| ServiceError::Unavailable)
}

fn journal_digest(document: &JournalDocument) -> Result<[u8; 32], ServiceError> {
    serde_json::to_vec(&(
        "market-squawk-settings-operation-journal-v1",
        document.schema_version,
        document.next_sequence,
        document.anchor_sha256,
        &document.pending,
        &document.receipts,
    ))
    .map(|bytes| Sha256::digest(bytes).into())
    .map_err(|_| ServiceError::Internal)
}

fn receipt_digest(receipt: &OperationReceiptRecord) -> Result<[u8; 32], ServiceError> {
    serde_json::to_vec(&(
        "market-squawk-settings-operation-receipt-v1",
        receipt.sequence,
        &receipt.operation,
        receipt.previous_revision,
        receipt.attempted_revision,
        receipt.active_revision,
        receipt.attempted_digest,
        receipt.active_digest,
        receipt.restart_impact,
        receipt.disposition,
        receipt.prior_receipt_sha256,
    ))
    .map(|bytes| Sha256::digest(bytes).into())
    .map_err(|_| ServiceError::Internal)
}

fn map_settings_error(error: SettingsError) -> ServiceError {
    match error {
        SettingsError::InvalidValue { .. }
        | SettingsError::InvalidChangeSet
        | SettingsError::ImmutableOrDuplicateSetting { .. }
        | SettingsError::StaleRevision
        | SettingsError::StaleOrInvalidApproval
        | SettingsError::RestoreTargetExists => ServiceError::InvalidRequest,
        SettingsError::UnknownRollbackRevision => ServiceError::NotFound,
        SettingsError::CapacityExceeded => ServiceError::ResourceExhausted,
        SettingsError::Unavailable | SettingsError::Persistence(_) => ServiceError::Unavailable,
        SettingsError::IncompleteSeed
        | SettingsError::RevisionExhausted
        | SettingsError::CorruptState
        | SettingsError::Encoding => ServiceError::Internal,
    }
}

fn map_recovery_error(_error: SettingsError) -> ServiceError {
    ServiceError::Internal
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::settings::{SettingKey, SettingValue, SettingsSeed, UpdateChannel};

    #[test]
    fn restart_handoff_is_deferred_until_successor_health() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let settings = Arc::new(DurableSettingsStore::try_open(
            directory.path(),
            SettingsSeed::recommended_defaults()?,
        )?);
        let signals = Arc::new(Mutex::new(Vec::new()));
        let recorded_signals = Arc::clone(&signals);
        let lifecycle = SettingsLifecycleAuthority::try_new(
            SettingKey::all(),
            Arc::new(|snapshot| Ok(SettingsApplicationProof::for_snapshot(snapshot))),
            Arc::new(move |handoff| {
                recorded_signals
                    .lock()
                    .map_err(|_| ServiceError::Unavailable)?
                    .push(handoff);
                Ok(())
            }),
            Arc::new(|snapshot| Ok(SettingsApplicationProof::for_snapshot(snapshot))),
        )?;
        let operations = ProductionSettingsOperations::try_new(
            directory.path(),
            Arc::clone(&settings),
            lifecycle,
        )?;
        let before = settings.snapshot()?;
        let approval = settings
            .preview(
                before.revision(),
                vec![SettingValue::UpdateChannel(UpdateChannel::Preview)],
            )?
            .approve();

        let receipt = operations.apply_change(approval)?;

        assert_eq!(receipt.restart_impact(), RestartImpact::ServiceRestart);
        assert!(
            signals
                .lock()
                .map_err(|_| ServiceError::Unavailable)?
                .is_empty()
        );
        assert!(operations.signal_pending_restart_after_response()?);
        assert_eq!(
            signals
                .lock()
                .map_err(|_| ServiceError::Unavailable)?
                .as_slice(),
            &[SettingsRestartHandoff::for_snapshot(
                &settings.snapshot()?,
                SettingsRestartReason::Apply,
            )]
        );
        assert_eq!(
            operations.reconcile_successor_startup()?,
            SettingsStartupReconciliation::Ready
        );
        assert!(!operations.signal_pending_restart_after_response()?);
        Ok(())
    }
}
