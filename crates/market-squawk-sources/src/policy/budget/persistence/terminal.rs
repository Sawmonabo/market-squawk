//! Irreversible, session-global durable-budget terminalization.

use super::*;

impl AuthorityDurabilitySession {
    /// Lets exactly one caller persist the already-latched terminal state and detach the store.
    pub(crate) fn persist_terminal_and_detach(&self) -> Result<(), AuthorityPersistenceError> {
        match self.claim_terminal_writer() {
            lifecycle::TerminalWriterClaim::Persisted => return Ok(()),
            lifecycle::TerminalWriterClaim::Failed => {
                return Err(AuthorityPersistenceError::SessionUnavailable);
            }
            lifecycle::TerminalWriterClaim::Unavailable => {
                return Err(AuthorityPersistenceError::SessionUnavailable);
            }
            lifecycle::TerminalWriterClaim::Owner => {}
        }

        let result = self
            .persist_global_terminal_state_and_detach()
            .map_err(|_error| AuthorityPersistenceError::SessionUnavailable);
        self.finish_terminal_write(result.is_ok());
        result
    }

    fn persist_global_terminal_state_and_detach(&self) -> Result<(), AuthorityPersistenceError> {
        let (mut current, envelope_usable) = match self.envelope.lock() {
            Ok(current) => (current, true),
            Err(poisoned) => (poisoned.into_inner(), false),
        };
        let (mut store, store_usable) = match self.store.lock() {
            Ok(store) => (store, true),
            Err(poisoned) => (poisoned.into_inner(), false),
        };
        if !envelope_usable || !store_usable {
            *store = None;
            return Err(AuthorityPersistenceError::SessionUnavailable);
        }
        let already_terminal = current.run_state == DurableRunState::InUse
            && current.budgets.as_slice().iter().all(|group| {
                let checkpoint = group.checkpoint();
                checkpoint.terminal && checkpoint.poisoned && checkpoint.disabled
            });
        let result = if already_terminal {
            Ok(())
        } else {
            (|| {
                let mut groups = current.budgets.as_slice().to_vec();
                for group in &mut groups {
                    group.checkpoint.terminalize_unclean();
                }
                let mut candidate = current.clone();
                candidate.budgets = BoundedVec::try_new(groups)
                    .map_err(|_| AuthorityPersistenceError::StateTooLarge)?;
                candidate.run_state = DurableRunState::InUse;
                candidate.saved_at_wall = candidate.wall_high_water;
                let payload = serialize_canonical_envelope(&candidate)?;
                store
                    .as_ref()
                    .ok_or(AuthorityPersistenceError::SessionUnavailable)?
                    .store(&payload)
                    .map_err(|_| AuthorityPersistenceError::Store)?;
                *current = candidate;
                Ok(())
            })()
        };
        *store = None;
        result
    }

    pub(super) fn detach_store(&self) {
        let mut store = match self.store.lock() {
            Ok(store) => store,
            Err(poisoned) => poisoned.into_inner(),
        };
        *store = None;
    }
}
