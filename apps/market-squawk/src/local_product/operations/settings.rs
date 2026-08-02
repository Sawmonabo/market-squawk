//! Concrete installed-product settings persistence and lifecycle coordination.

use std::{
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
        DurableSettingsStore, RestartImpact, SettingsChangeApproval, SettingsError,
        SettingsReceipt, SettingsSnapshot,
    },
};

const JOURNAL_DIRECTORY: &str = "settings-operations";
const JOURNAL_SCHEMA_VERSION: u16 = 1;
const MAXIMUM_RETAINED_RECEIPTS: usize = 64;

pub(crate) type LifecycleAction =
    dyn Fn(&SettingsSnapshot) -> Result<(), ServiceError> + Send + Sync + 'static;

/// Closed callbacks supplied by the installed-service composition owner.
///
/// Callbacks receive only a validated typed snapshot. They never receive a configuration path,
/// raw configuration document, environment map, or secret material.
pub(crate) struct SettingsLifecycleAuthority {
    reload: Arc<LifecycleAction>,
    restart: Arc<LifecycleAction>,
    health_check: Arc<LifecycleAction>,
}

impl SettingsLifecycleAuthority {
    /// Binds reload, restart, and post-transition health operations from the service owner.
    pub(crate) fn new(
        reload: Arc<LifecycleAction>,
        restart: Arc<LifecycleAction>,
        health_check: Arc<LifecycleAction>,
    ) -> Self {
        Self {
            reload,
            restart,
            health_check,
        }
    }

    fn apply(
        &self,
        impact: RestartImpact,
        snapshot: &SettingsSnapshot,
    ) -> Result<(), ServiceError> {
        match impact {
            RestartImpact::None => {}
            RestartImpact::ServiceReload => (self.reload)(snapshot)?,
            RestartImpact::ServiceRestart => (self.restart)(snapshot)?,
        }
        (self.health_check)(snapshot)
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
    /// Opens the durable receipt journal and resolves any interrupted settings transition.
    ///
    /// `control_root` is the already-prepared private product control root. `settings` must be the
    /// same exclusive store published through `OperationsApplicationServices`. `lifecycle` must be
    /// owned by the installed-service composition and act on that same product instance.
    pub(crate) fn try_new(
        control_root: &Path,
        settings: Arc<DurableSettingsStore>,
        lifecycle: SettingsLifecycleAuthority,
    ) -> Result<Self, ServiceError> {
        let authority = Self {
            settings,
            lifecycle,
            journal: SettingsOperationJournal::try_open(control_root)?,
            transaction: Mutex::new(()),
        };
        authority.recover_interrupted()?;
        Ok(authority)
    }

    fn acquire(&self) -> Result<MutexGuard<'_, ()>, ServiceError> {
        self.transaction
            .lock()
            .map_err(|_| ServiceError::Unavailable)
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
        self.lifecycle.apply(receipt.restart_impact(), snapshot)
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

    fn recover_interrupted(&self) -> Result<(), ServiceError> {
        let _transaction = self.acquire()?;
        let Some(pending) = self.journal.pending()? else {
            return Ok(());
        };
        let current = self.settings.snapshot().map_err(map_settings_error)?;
        if current.revision() == pending.previous_revision {
            if matches!(pending.phase, PendingPhase::Persisting)
                && current.digest() == pending.previous_digest
            {
                return self.journal.abandon();
            }
            return Err(ServiceError::Unavailable);
        }
        if current.revision() < pending.attempted_revision {
            return Err(ServiceError::Unavailable);
        }

        let rollback = self
            .settings
            .preview_rollback(current.revision(), pending.previous_revision)
            .map_err(map_recovery_error)?;
        self.journal.begin_recovery(
            current.revision(),
            current.digest(),
            rollback.restart_impact(),
        )?;
        let recovered = self
            .settings
            .rollback(current.revision(), pending.previous_revision)
            .map_err(map_recovery_error)?;
        let recovered_snapshot = self.settings.snapshot().map_err(map_recovery_error)?;
        self.apply_lifecycle(recovered, &recovered_snapshot)
            .map_err(|_| ServiceError::Unavailable)?;

        let attempted_impact = pending
            .phase
            .attempted_impact()
            .unwrap_or(rollback.restart_impact());
        let attempted_receipt = RecoveredAttempt {
            active_revision: current.revision(),
            active_digest: current.digest(),
            restart_impact: attempted_impact,
        };
        self.journal
            .complete_interrupted(attempted_receipt, recovered, &recovered_snapshot)
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

    fn complete_interrupted(
        &self,
        attempted: RecoveredAttempt,
        active: SettingsReceipt,
        active_snapshot: &SettingsSnapshot,
    ) -> Result<(), ServiceError> {
        self.complete_values(
            OperationDisposition::Recovered,
            attempted,
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
        | SettingsError::StaleOrInvalidApproval => ServiceError::InvalidRequest,
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
