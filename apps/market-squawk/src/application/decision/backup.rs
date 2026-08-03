//! Owner-issued backup and fresh-restore capability for the complete decision journal.

use std::{fmt, sync::Arc};

use market_squawk_decisions::{DecisionAuthority, DecisionRepository, DecisionRepositoryLimits};
use market_squawk_platform::DecisionDatabaseLocation;
use sha2::{Digest as _, Sha256};

use super::persistence::{DecisionJournal, DecisionJournalBackup};
use super::{DecisionApplication, DecisionApplicationError, DecisionState, RecoveryContext};

/// Non-cloneable exact decision-journal image retained under the application's mutation fence.
pub(crate) struct RetainedDecisionBackupSnapshot {
    application: Arc<DecisionApplication>,
    journal: DecisionJournalBackup,
}

impl DecisionApplication {
    /// Fences every decision mutation and creates a complete SQLite online-backup image.
    ///
    /// Reads remain available while the lease exists. Mutations fail closed until the lease is
    /// dropped, which lets the product backup coordinator emit and revalidate this exact semantic
    /// authority revision after its common cutoff has been allocated.
    pub(crate) fn retain_backup(
        self: &Arc<Self>,
    ) -> Result<RetainedDecisionBackupSnapshot, DecisionApplicationError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_error| DecisionApplicationError::Unavailable)?;
        if state.poisoned || state.backup_retained {
            return Err(DecisionApplicationError::Unavailable);
        }
        state.backup_retained = true;
        let journal = validated_backup(&state);
        match journal {
            Ok(journal) => Ok(RetainedDecisionBackupSnapshot {
                application: Arc::clone(self),
                journal,
            }),
            Err(error) => {
                state.backup_retained = false;
                Err(error)
            }
        }
    }

    /// Restores the complete typed journal only into a fresh database and reopens it through the
    /// normal application recovery boundary before returning any decision authority.
    pub(crate) fn restore_backup_fresh(
        location: DecisionDatabaseLocation,
        limits: DecisionRepositoryLimits,
        bytes: &[u8],
        expected_content_sha256: [u8; 32],
    ) -> Result<Self, DecisionApplicationError> {
        if expected_content_sha256 == [0; 32]
            || <[u8; 32]>::from(Sha256::digest(bytes)) != expected_content_sha256
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        let restored_semantic = DecisionJournal::restore_fresh(&location, limits, bytes)?;
        let application = Self::open(location, limits)?;
        let state = application
            .state
            .lock()
            .map_err(|_error| DecisionApplicationError::Unavailable)?;
        let reopened_semantic = semantic_revision(&state)?;
        drop(state);
        if reopened_semantic != restored_semantic {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(application)
    }
}

impl RetainedDecisionBackupSnapshot {
    /// Returns the complete immutable SQLite image, including all six typed journal record kinds.
    pub(crate) fn bytes(&self) -> &[u8] {
        self.journal.bytes()
    }

    /// Returns the semantic identity of the exact ordered, typed journal revision.
    pub(crate) const fn authority_revision_sha256(&self) -> [u8; 32] {
        self.journal.semantic_sha256()
    }

    /// Returns the digest of the exact SQLite bytes emitted to the managed backup repository.
    pub(crate) const fn content_sha256(&self) -> [u8; 32] {
        self.journal.content_sha256()
    }

    /// Revalidates an adapter's exact output receipt and the still-fenced live authority.
    pub(crate) fn revalidate_emitted(
        &self,
        authority_revision_sha256: [u8; 32],
        byte_length: u64,
        content_sha256: [u8; 32],
    ) -> Result<(), DecisionApplicationError> {
        if authority_revision_sha256 != self.authority_revision_sha256()
            || usize::try_from(byte_length).ok() != Some(self.bytes().len())
            || content_sha256 != self.content_sha256()
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        let state = self
            .application
            .state
            .lock()
            .map_err(|_error| DecisionApplicationError::Unavailable)?;
        if state.poisoned
            || !state.backup_retained
            || semantic_revision(&state)? != self.authority_revision_sha256()
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(())
    }
}

impl fmt::Debug for RetainedDecisionBackupSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedDecisionBackupSnapshot")
            .field("byte_length", &self.bytes().len())
            .field("authority_revision_sha256", &"[SHA-256]")
            .field("content_sha256", &"[SHA-256]")
            .finish()
    }
}

impl Drop for RetainedDecisionBackupSnapshot {
    fn drop(&mut self) {
        if let Ok(mut state) = self.application.state.lock() {
            state.backup_retained = false;
        }
    }
}

fn validated_backup(
    state: &DecisionState,
) -> Result<DecisionJournalBackup, DecisionApplicationError> {
    let semantic_sha256 = semantic_revision(state)?;
    let backup = state.journal.online_backup()?;
    if backup.semantic_sha256() != semantic_sha256 {
        return Err(DecisionApplicationError::InvalidPersistentState);
    }
    Ok(backup)
}

fn semantic_revision(state: &DecisionState) -> Result<[u8; 32], DecisionApplicationError> {
    let repository = DecisionRepository::try_new(state.limits)?;
    let mut authority = DecisionAuthority::new(repository);
    let mut recovery = RecoveryContext::try_new()?;
    state.journal.recover(&mut authority, &mut recovery)
}
