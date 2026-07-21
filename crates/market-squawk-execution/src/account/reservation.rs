//! Non-cloneable account reservation authority and fail-safe lifecycle guards.

use std::fmt;
use std::sync::Arc;

use market_squawk_domain::AccountId;
use thiserror::Error;

use crate::account::AccountRiskReconciliationFence;
use crate::clock::{AccountReservationLease, system_now};
use crate::{AccountRiskViolation, OrderIntentDigest};

/// Stable, nonempty account reservation rejection.
#[derive(Debug, Eq, PartialEq)]
pub struct AccountReservationError {
    reasons: Box<[AccountRiskViolation]>,
}

impl AccountReservationError {
    /// Returns every applicable reason in stable enum order.
    pub const fn reasons(&self) -> &[AccountRiskViolation] {
        &self.reasons
    }

    pub(super) fn from_reason(reason: AccountRiskViolation) -> Self {
        Self {
            reasons: Box::new([reason]),
        }
    }

    pub(super) fn from_reasons(mut reasons: Vec<AccountRiskViolation>) -> Self {
        reasons.sort_unstable();
        reasons.dedup();
        debug_assert!(!reasons.is_empty());
        Self {
            reasons: reasons.into_boxed_slice(),
        }
    }
}

impl fmt::Display for AccountReservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("account risk rejected order:")?;
        for reason in &self.reasons {
            write!(formatter, " {reason}")?;
        }
        Ok(())
    }
}

impl std::error::Error for AccountReservationError {}

/// Private-field, non-cloneable account reservation.
///
/// This value conveys no broker or adapter authority. Dropping an active reservation releases its
/// exposure atomically without acquiring the account partition lock.
#[derive(Debug)]
pub struct AccountRiskReservation {
    pub(super) account_id: AccountId,
    pub(super) intent_digest: OrderIntentDigest,
    pub(super) lease: Arc<AccountReservationLease>,
    pub(super) reconciliation: AccountRiskReconciliationFence,
}

impl AccountRiskReservation {
    /// Returns the reserved account.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the exact order-intent digest bound to this reservation.
    pub const fn intent_digest(&self) -> OrderIntentDigest {
        self.intent_digest
    }

    pub(crate) fn expected_account_revision(&self) -> u64 {
        self.lease.expected_account_revision()
    }

    /// Revalidates state revision, reservation state, and both deadlines.
    ///
    /// # Errors
    ///
    /// Fails after release, commit, reconciliation transition, account replacement, clock failure,
    /// or inclusive expiration.
    pub fn validate_current(&self) -> Result<(), AccountReservationStateError> {
        let now = system_now().map_err(|_| AccountReservationStateError::ClockFailure)?;
        self.lease.validate(now)
    }

    pub(crate) fn validate_at(
        &self,
        now: crate::clock::ClockReading,
    ) -> Result<(), AccountReservationStateError> {
        self.lease.validate(now)
    }

    pub(crate) fn valid_until(&self) -> market_squawk_domain::Timestamp {
        self.lease.wall_expiry()
    }

    pub(crate) fn begin_submission(
        &self,
        approval_valid_until: market_squawk_domain::Timestamp,
        approval_monotonic_deadline: std::time::Instant,
    ) -> Result<AccountSubmissionFailSafe, AccountReservationStateError> {
        let _publication = self
            .reconciliation
            .try_begin_reservation_publication()
            .map_err(|_| AccountReservationStateError::ReconciliationRequired)?;
        if !self.reconciliation.is_current() {
            return Err(AccountReservationStateError::ReconciliationRequired);
        }
        let now = system_now().map_err(|_| AccountReservationStateError::ClockFailure)?;
        if crate::clock::deadline_expired(now, approval_valid_until, approval_monotonic_deadline) {
            return Err(AccountReservationStateError::Expired);
        }
        self.lease.begin_submission(now)?;
        Ok(AccountSubmissionFailSafe {
            lease: Arc::clone(&self.lease),
            armed: true,
        })
    }

    pub(crate) fn mark_accepted(&self) -> Result<(), AccountReservationStateError> {
        self.lease.mark_accepted()
    }

    pub(crate) fn mark_known_not_accepted(&self) {
        self.lease.mark_known_not_accepted();
    }

    pub(crate) fn mark_reconciliation_required(&self) {
        self.lease.mark_reconciliation_required();
    }

    pub(crate) fn mark_terminal_unfilled(&self) {
        self.lease.mark_terminal_unfilled();
    }

    pub(crate) fn outcome_fail_safe(&self) -> AccountOutcomeFailSafe {
        AccountOutcomeFailSafe {
            lease: Arc::clone(&self.lease),
            armed: true,
        }
    }
}

/// Drop guard that converts an interrupted in-flight adapter call into account reconciliation.
#[derive(Debug)]
pub(crate) struct AccountSubmissionFailSafe {
    lease: Arc<AccountReservationLease>,
    armed: bool,
}

impl AccountSubmissionFailSafe {
    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for AccountSubmissionFailSafe {
    fn drop(&mut self) {
        if self.armed {
            self.lease.mark_reconciliation_required();
        }
    }
}

/// Drop guard for accepted-order cancellation or reconciliation calls.
#[derive(Debug)]
pub(crate) struct AccountOutcomeFailSafe {
    lease: Arc<AccountReservationLease>,
    armed: bool,
}

impl AccountOutcomeFailSafe {
    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for AccountOutcomeFailSafe {
    fn drop(&mut self) {
        if self.armed {
            self.lease.mark_reconciliation_required();
        }
    }
}

impl Drop for AccountRiskReservation {
    fn drop(&mut self) {
        self.lease.fail_safe_drop();
    }
}

#[cfg(test)]
pub(crate) fn accepted_reservation_for_test()
-> Result<(AccountRiskReservation, Arc<std::sync::atomic::AtomicBool>), Box<dyn std::error::Error>>
{
    use std::sync::atomic::{AtomicBool, AtomicU64};

    use market_squawk_domain::Timestamp;

    use crate::clock::monotonic_deadline;

    let account_id = "50000000-0000-0000-0000-000000000001".parse::<AccountId>()?;
    let account_revision = Arc::new(AtomicU64::new(1));
    let reconciliation_required = Arc::new(AtomicBool::new(false));
    let now = system_now()?;
    let lease = Arc::new(AccountReservationLease::new(
        account_revision,
        Arc::clone(&reconciliation_required),
        1,
        Timestamp::from_unix_nanos(i64::MAX),
        monotonic_deadline(now, 1_000_000_000)?,
    ));
    lease.begin_submission(now)?;
    lease.mark_accepted()?;
    Ok((
        AccountRiskReservation {
            account_id,
            intent_digest: OrderIntentDigest::from_bytes([1; 32]),
            lease,
            reconciliation: AccountRiskReconciliationFence::new(0),
        },
        reconciliation_required,
    ))
}

/// Current reservation validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AccountReservationStateError {
    /// Reservation is released, committed, or awaiting reconciliation.
    #[error("account reservation is not active")]
    NotActive,
    /// Reservation did not enter the submitted state before result finalization.
    #[error("account reservation is not submitted")]
    NotSubmitted,
    /// Authoritative account state changed after reservation.
    #[error("account state revision changed")]
    AccountStateChanged,
    /// Authoritative account state requires reconciliation before submission.
    #[error("account reconciliation is required")]
    ReconciliationRequired,
    /// Either wall or monotonic expiry was reached.
    #[error("account reservation expired")]
    Expired,
    /// Trusted clock failure.
    #[error("trusted account-reservation clock failed")]
    ClockFailure,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::time::{Duration, Instant};

    use market_squawk_domain::{AccountId, Timestamp};

    use super::{AccountReservationStateError, AccountRiskReservation};
    use crate::OrderIntentDigest;
    use crate::account::AccountRiskReconciliationFence;
    use crate::clock::AccountReservationLease;

    #[test]
    fn financial_fence_invalidates_an_active_approval_before_submission()
    -> Result<(), Box<dyn std::error::Error>> {
        let fence = AccountRiskReconciliationFence::new(0);
        let reservation = AccountRiskReservation {
            account_id: "50000000-0000-0000-0000-000000000001".parse::<AccountId>()?,
            intent_digest: OrderIntentDigest::from_bytes([1; 32]),
            lease: Arc::new(AccountReservationLease::new(
                Arc::new(AtomicU64::new(1)),
                Arc::new(AtomicBool::new(false)),
                1,
                Timestamp::from_unix_nanos(i64::MAX),
                Instant::now() + Duration::from_secs(60),
            )),
            reconciliation: fence.clone(),
        };
        fence.require(std::num::NonZeroU64::MIN)?;

        assert_eq!(
            reservation
                .begin_submission(
                    Timestamp::from_unix_nanos(i64::MAX),
                    Instant::now() + Duration::from_secs(60),
                )
                .err(),
            Some(AccountReservationStateError::ReconciliationRequired)
        );
        Ok(())
    }

    #[test]
    fn submission_rejects_expired_approval_when_reservation_is_still_current()
    -> Result<(), Box<dyn std::error::Error>> {
        let reservation = AccountRiskReservation {
            account_id: "50000000-0000-0000-0000-000000000001".parse::<AccountId>()?,
            intent_digest: OrderIntentDigest::from_bytes([1; 32]),
            lease: Arc::new(AccountReservationLease::new(
                Arc::new(AtomicU64::new(1)),
                Arc::new(AtomicBool::new(false)),
                1,
                Timestamp::from_unix_nanos(i64::MAX),
                Instant::now() + Duration::from_secs(60),
            )),
            reconciliation: AccountRiskReconciliationFence::new(0),
        };

        assert_eq!(
            reservation
                .begin_submission(
                    Timestamp::from_unix_nanos(i64::MIN),
                    Instant::now() + Duration::from_secs(60),
                )
                .err(),
            Some(AccountReservationStateError::Expired)
        );
        Ok(())
    }
}
