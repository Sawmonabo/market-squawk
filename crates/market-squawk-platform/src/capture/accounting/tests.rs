use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use super::{
    AccountingComponent, CaptureAccountingError, CaptureAccountingSnapshotError,
    CaptureMemoryAccounting,
};

#[test]
fn fixed_resident_and_record_totals_reconcile_across_raii_release()
-> Result<(), Box<dyn std::error::Error>> {
    let accounting = Arc::new(CaptureMemoryAccounting::try_new(
        100,
        NonZeroUsize::new(1_000).ok_or("ceiling")?,
    )?);
    let resident = accounting.try_reserve(AccountingComponent::ResidentGeneration, 200)?;
    let record = accounting.try_reserve(AccountingComponent::Record, 300)?;
    let snapshot = accounting.try_snapshot(NonZeroUsize::new(4).ok_or("attempts")?)?;
    assert_eq!(snapshot.fixed_capture_bytes(), 100);
    assert_eq!(snapshot.resident_generation_bytes(), 200);
    assert_eq!(snapshot.record_reservation_bytes(), 300);
    assert_eq!(snapshot.total_accounted_bytes(), 600);
    drop(record);
    drop(resident);
    let final_snapshot = accounting.try_snapshot(NonZeroUsize::MIN)?;
    assert_eq!(final_snapshot.total_accounted_bytes(), 100);
    assert_eq!(final_snapshot.record_reservation_bytes(), 0);
    assert_eq!(final_snapshot.resident_generation_bytes(), 0);
    Ok(())
}

#[test]
fn one_over_ceiling_is_rejected_without_counter_mutation() -> Result<(), Box<dyn std::error::Error>>
{
    let accounting = Arc::new(CaptureMemoryAccounting::try_new(
        100,
        NonZeroUsize::new(101).ok_or("ceiling")?,
    )?);
    assert!(matches!(
        accounting.try_reserve(AccountingComponent::Record, 2),
        Err(CaptureAccountingError::BudgetExceeded {
            required: 102,
            ceiling: 101
        })
    ));
    let snapshot = accounting.try_snapshot(NonZeroUsize::MIN)?;
    assert_eq!(snapshot.total_accounted_bytes(), 100);
    assert_eq!(snapshot.completed_epoch(), 0);
    Ok(())
}

#[test]
fn bounded_snapshot_reports_contention_without_fabricating_a_sample()
-> Result<(), Box<dyn std::error::Error>> {
    let accounting = CaptureMemoryAccounting::try_new(1, NonZeroUsize::new(10).ok_or("ceiling")?)?;
    accounting.with_held_transition_for_test(|| {
        assert_eq!(
            accounting.try_snapshot(NonZeroUsize::new(2).ok_or("attempts")?),
            Err(CaptureAccountingSnapshotError::Contended {
                attempts: NonZeroUsize::new(2).ok_or("attempts")?,
            })
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    assert!(accounting.try_snapshot(NonZeroUsize::MIN).is_ok());
    Ok(())
}

#[test]
fn abandoned_transition_is_durably_poisoned_and_rejects_later_reservation()
-> Result<(), Box<dyn std::error::Error>> {
    let accounting = Arc::new(CaptureMemoryAccounting::try_new(
        1,
        NonZeroUsize::new(10).ok_or("ceiling")?,
    )?);
    accounting.abandon_transition_for_test()?;
    assert_eq!(
        accounting.try_snapshot(NonZeroUsize::MIN),
        Err(CaptureAccountingSnapshotError::InvariantViolated)
    );
    assert!(matches!(
        accounting.try_reserve(AccountingComponent::Record, 1),
        Err(CaptureAccountingError::InvariantViolated)
    ));
    Ok(())
}

#[test]
fn impossible_initial_fixed_total_and_epoch_overflow_are_typed()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        CaptureMemoryAccounting::try_new(2, NonZeroUsize::MIN),
        Err(CaptureAccountingError::BudgetExceeded {
            required: 2,
            ceiling: 1
        })
    ));
    let accounting = CaptureMemoryAccounting::for_test_with_epoch(
        1,
        NonZeroUsize::new(10).unwrap_or(NonZeroUsize::MIN),
        NonZeroU64::new(u64::MAX).unwrap_or(NonZeroU64::MIN),
    )?;
    assert!(matches!(
        accounting.try_reserve(AccountingComponent::Record, 1),
        Err(CaptureAccountingError::EpochOverflow)
    ));
    assert_eq!(
        accounting.try_snapshot(NonZeroUsize::MIN),
        Err(CaptureAccountingSnapshotError::EpochOverflow)
    );
    Ok(())
}
