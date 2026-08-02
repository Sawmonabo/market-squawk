//! Idempotent allocation and durability-session terminal faults.

use super::*;

impl SharedProviderBudget {
    pub(in crate::policy) fn terminal_fault(
        &self,
        reason: BudgetUnavailableReason,
        admission: &RuntimeOperationAdmission,
    ) -> BudgetUnavailableReason {
        let durable_binding = self.allocation.durability.as_ref();
        let durable_admission = match self.validated_durable_admission(admission) {
            Ok(admission) => admission,
            Err(reason) => return reason,
        };
        if let Some(admission) = durable_admission {
            admission.latch_terminal();
        }
        let first_fault = !self.allocation.terminal.swap(true, Ordering::AcqRel);
        if first_fault {
            let _previous_generation = self.allocation.availability_generation.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |generation| generation.checked_add(1),
            );
        }
        let durable_terminal =
            durable_binding.map(|binding| binding.session.persist_terminal_and_detach());
        match durable_terminal {
            None | Some(Ok(())) => reason,
            Some(Err(_)) => BudgetUnavailableReason::PersistenceUnavailable,
        }
    }

    pub(in crate::policy) fn terminal_fail<T>(
        &self,
        reason: BudgetUnavailableReason,
        admission: &RuntimeOperationAdmission,
    ) -> Result<T, BudgetUnavailableReason> {
        Err(self.terminal_fault(reason, admission))
    }

    pub(in crate::policy) fn terminal_unavailable(
        &self,
        reason: BudgetUnavailableReason,
        admission: &RuntimeOperationAdmission,
    ) -> BudgetDecision {
        BudgetDecision::Unavailable(self.terminal_fault(reason, admission))
    }
}
