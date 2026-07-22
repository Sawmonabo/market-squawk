//! Checked allocation accounting and bounded cancellation/deadline observation.

use std::mem::size_of;
use std::time::Instant;

use tokio_util::sync::CancellationToken;

use super::PointInTimeError;

const CONTROL_CHECK_INTERVAL: usize = 64;

pub(super) struct OperationControl {
    cancellation: CancellationToken,
    deadline: Instant,
    operations_until_check: usize,
}

impl OperationControl {
    pub(super) fn new<'error>(
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<Self, PointInTimeError<'error>> {
        let value = Self {
            cancellation: cancellation.clone(),
            deadline,
            operations_until_check: CONTROL_CHECK_INTERVAL,
        };
        value.check_now()?;
        Ok(value)
    }

    pub(super) fn observe<'error>(&mut self) -> Result<(), PointInTimeError<'error>> {
        self.operations_until_check -= 1;
        if self.operations_until_check == 0 {
            self.check_now()?;
            self.operations_until_check = CONTROL_CHECK_INTERVAL;
        }
        Ok(())
    }

    pub(super) fn check_now<'error>(&self) -> Result<(), PointInTimeError<'error>> {
        if self.cancellation.is_cancelled() {
            Err(PointInTimeError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(PointInTimeError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct RetainedBudget {
    limit: usize,
    current: usize,
    peak: usize,
}

impl RetainedBudget {
    pub(super) const fn new(limit: usize) -> Self {
        Self {
            limit,
            current: 0,
            peak: 0,
        }
    }

    pub(super) fn charge<'a>(&mut self, bytes: usize) -> Result<(), PointInTimeError<'a>> {
        let observed = self
            .current
            .checked_add(bytes)
            .ok_or(PointInTimeError::AccountingOverflow)?;
        if observed > self.limit {
            return Err(PointInTimeError::RetainedBytesExceeded {
                limit: self.limit,
                observed,
            });
        }
        self.current = observed;
        self.peak = self.peak.max(observed);
        Ok(())
    }

    pub(super) const fn peak(self) -> usize {
        self.peak
    }
}

pub(super) fn reserve_exact<'a, T>(
    values: &mut Vec<T>,
    additional: usize,
    budget: &mut RetainedBudget,
) -> Result<(), PointInTimeError<'a>> {
    let requested = additional
        .checked_mul(size_of::<T>())
        .ok_or(PointInTimeError::AccountingOverflow)?;
    budget.charge(requested)?;
    values
        .try_reserve_exact(additional)
        .map_err(|_| PointInTimeError::AllocationFailure)?;
    let actual = values
        .capacity()
        .checked_mul(size_of::<T>())
        .ok_or(PointInTimeError::AccountingOverflow)?;
    if actual > requested {
        budget.charge(actual - requested)?;
    }
    Ok(())
}

pub(super) fn checked_add<'a>(left: usize, right: usize) -> Result<usize, PointInTimeError<'a>> {
    left.checked_add(right)
        .ok_or(PointInTimeError::AccountingOverflow)
}
