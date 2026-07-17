//! Exhaustive fatal-branch classification and transient-control regressions.

use super::*;

#[path = "branch_matrix/permit.rs"]
mod permit;

struct GlobalFaultFixture {
    store: Arc<MemoryStore>,
    session: Arc<AuthorityDurabilitySession>,
    clock: Arc<SwitchableClock>,
    budget: SharedProviderBudget,
    alias: SharedProviderBudget,
    peer: SharedProviderBudget,
    prior: BudgetAvailabilityLease,
    slot: usize,
}

fn global_fault_fixture() -> TestResult<GlobalFaultFixture> {
    let DurableBudgetFixture {
        store,
        session,
        clock,
        budget,
        slot,
    } = durable_budget(1)?;
    let alias = budget.clone();
    let prior = budget
        .availability_lease()
        .map_err(|reason| format!("availability setup failed: {reason:?}"))?;
    let peer_declaration = declaration(2)?;
    let observation = clock
        .observation()
        .map_err(|reason| format!("clock setup failed: {reason:?}"))?;
    let peer_checkpoint = checkpoint_from_runtime(
        peer_declaration.policy(),
        &BudgetState {
            window_started_at: observation.monotonic,
            restored_window_ends_at: None,
            requests_used: 0,
            in_flight: 0,
            unavailable_until: None,
            disabled: false,
            consecutive_refusals: 0,
        },
        observation,
        1,
        false,
    )?;
    let peer_slot = session.register_budget_group(
        crate::RegistryAuthorityState::empty(),
        peer_declaration.clone(),
        peer_checkpoint,
        observation.wall_clock,
    )?;
    let peer = SharedProviderBudget::new_durable(
        peer_declaration.policy().clone(),
        observation.monotonic,
        clock.clone(),
        BudgetDurabilityBinding {
            session: session.clone(),
            slot: peer_slot,
        },
    );
    Ok(GlobalFaultFixture {
        store,
        session,
        clock,
        alias,
        budget,
        peer,
        prior,
        slot,
    })
}

fn assert_global_terminal(
    fixture: GlobalFaultFixture,
    terminal_checkpoint_was_stored: bool,
) -> TestResult {
    fixture.clock.recover();
    assert!(!fixture.session.is_available());
    assert!(!fixture.prior.is_available());
    assert!(fixture.budget.allocation.terminal.load(Ordering::Acquire));
    let terminal_generation = fixture
        .budget
        .allocation
        .availability_generation
        .load(Ordering::Acquire);
    assert!(matches!(
        fixture.alias.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
            | BudgetDecision::Unavailable(BudgetUnavailableReason::AvailabilityGenerationExhausted)
    ));
    assert_eq!(
        fixture
            .budget
            .allocation
            .availability_generation
            .load(Ordering::Acquire),
        terminal_generation,
        "a terminal allocation minted a later availability generation"
    );
    assert!(matches!(
        fixture.peer.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert!(matches!(
        fixture.session.register_budget_group(
            crate::RegistryAuthorityState::empty(),
            declaration(3)?,
            checkpoint(3),
            Timestamp::from_unix_nanos(100),
        ),
        Err(AuthorityPersistenceError::SessionUnavailable)
    ));
    assert!(matches!(
        fixture.session.close_clean(
            crate::RegistryAuthorityState::empty(),
            Timestamp::from_unix_nanos(100),
        ),
        Err(AuthorityPersistenceError::SessionUnavailable)
    ));

    let envelope = deserialize_canonical_envelope(&fixture.store.payload()?)?;
    assert_eq!(envelope.run_state, DurableRunState::InUse);
    if terminal_checkpoint_was_stored {
        assert!(
            envelope
                .budgets
                .as_slice()
                .iter()
                .all(|group| {
                    let checkpoint = group.checkpoint();
                    checkpoint.terminal && checkpoint.poisoned && checkpoint.disabled
                }),
            "global terminal persistence left a pre-existing group recoverable"
        );
    } else {
        let _affected = envelope
            .budgets
            .as_slice()
            .get(fixture.slot)
            .ok_or("affected durable budget group missing")?;
    }

    fixture.store.reject_stores.store(false, Ordering::Release);
    let restarted =
        AuthorityDurabilitySession::open(fixture.store, Timestamp::from_unix_nanos(100))?;
    assert!(restarted.recovered_unclean());
    assert!(!restarted.is_available());
    assert!(
        restarted
            .budget_groups()?
            .iter()
            .all(|group| group.checkpoint().terminal && group.checkpoint().poisoned)
    );
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum AvailabilityFatalCase {
    ClockUnavailable,
    StatePoisoned,
    ClockRegression,
    DeadlineOverflow,
    PersistenceFailure,
    GenerationOverflow,
}

#[test]
fn availability_fatal_branch_matrix_invalidates_global_durability() -> TestResult {
    for case in [
        AvailabilityFatalCase::ClockUnavailable,
        AvailabilityFatalCase::StatePoisoned,
        AvailabilityFatalCase::ClockRegression,
        AvailabilityFatalCase::DeadlineOverflow,
        AvailabilityFatalCase::PersistenceFailure,
        AvailabilityFatalCase::GenerationOverflow,
    ] {
        let fixture = global_fault_fixture()?;
        match case {
            AvailabilityFatalCase::ClockUnavailable => fixture.clock.fail(),
            AvailabilityFatalCase::StatePoisoned => poison_budget_state(&fixture.budget),
            AvailabilityFatalCase::ClockRegression => {
                fixture
                    .budget
                    .allocation
                    .state
                    .lock()
                    .map_err(|_| "budget state lock poisoned")?
                    .window_started_at = MonotonicInstant::from_nanos(1);
            }
            AvailabilityFatalCase::DeadlineOverflow => {
                fixture.clock.set(i64::MAX, u64::MAX)?;
                fixture
                    .budget
                    .allocation
                    .state
                    .lock()
                    .map_err(|_| "budget state lock poisoned")?
                    .window_started_at = MonotonicInstant::from_nanos(u64::MAX);
            }
            AvailabilityFatalCase::PersistenceFailure => {
                fixture.store.reject_stores.store(true, Ordering::Release);
            }
            AvailabilityFatalCase::GenerationOverflow => {
                fixture
                    .budget
                    .allocation
                    .availability_generation
                    .store(u64::MAX, Ordering::Release);
                fixture
                    .budget
                    .allocation
                    .state
                    .lock()
                    .map_err(|_| "budget state lock poisoned")?
                    .disabled = true;
            }
        }
        let expected = match case {
            AvailabilityFatalCase::ClockUnavailable => BudgetUnavailableReason::ClockUnavailable,
            AvailabilityFatalCase::StatePoisoned => BudgetUnavailableReason::StatePoisoned,
            AvailabilityFatalCase::ClockRegression => BudgetUnavailableReason::ClockRegression,
            AvailabilityFatalCase::DeadlineOverflow => BudgetUnavailableReason::DeadlineOverflow,
            AvailabilityFatalCase::PersistenceFailure => {
                BudgetUnavailableReason::PersistenceUnavailable
            }
            AvailabilityFatalCase::GenerationOverflow => {
                BudgetUnavailableReason::AvailabilityGenerationExhausted
            }
        };
        assert!(matches!(fixture.budget.availability_lease(), Err(reason) if reason == expected));
        assert_global_terminal(
            fixture,
            !matches!(case, AvailabilityFatalCase::PersistenceFailure),
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum AcquireFatalCase {
    ClockUnavailable,
    StatePoisoned,
    ClockRegression,
    WindowDeadlineOverflow,
    RequestsCounterCorrupt,
    InFlightCounterCorrupt,
    PersistenceFailure,
}

#[test]
fn try_acquire_fatal_branch_matrix_invalidates_global_durability() -> TestResult {
    for case in [
        AcquireFatalCase::ClockUnavailable,
        AcquireFatalCase::StatePoisoned,
        AcquireFatalCase::ClockRegression,
        AcquireFatalCase::WindowDeadlineOverflow,
        AcquireFatalCase::RequestsCounterCorrupt,
        AcquireFatalCase::InFlightCounterCorrupt,
        AcquireFatalCase::PersistenceFailure,
    ] {
        let fixture = global_fault_fixture()?;
        match case {
            AcquireFatalCase::ClockUnavailable => fixture.clock.fail(),
            AcquireFatalCase::StatePoisoned => poison_budget_state(&fixture.budget),
            AcquireFatalCase::ClockRegression => {
                fixture
                    .budget
                    .allocation
                    .state
                    .lock()
                    .map_err(|_| "budget state lock poisoned")?
                    .window_started_at = MonotonicInstant::from_nanos(1);
            }
            AcquireFatalCase::WindowDeadlineOverflow => {
                fixture.clock.set(i64::MAX, u64::MAX)?;
                fixture
                    .budget
                    .allocation
                    .state
                    .lock()
                    .map_err(|_| "budget state lock poisoned")?
                    .window_started_at = MonotonicInstant::from_nanos(u64::MAX);
            }
            AcquireFatalCase::RequestsCounterCorrupt => {
                fixture
                    .budget
                    .allocation
                    .state
                    .lock()
                    .map_err(|_| "budget state lock poisoned")?
                    .requests_used = u32::MAX;
            }
            AcquireFatalCase::InFlightCounterCorrupt => {
                fixture
                    .budget
                    .allocation
                    .state
                    .lock()
                    .map_err(|_| "budget state lock poisoned")?
                    .in_flight = u16::MAX;
            }
            AcquireFatalCase::PersistenceFailure => {
                fixture.store.reject_stores.store(true, Ordering::Release);
            }
        }
        let expected = match case {
            AcquireFatalCase::ClockUnavailable => BudgetUnavailableReason::ClockUnavailable,
            AcquireFatalCase::StatePoisoned => BudgetUnavailableReason::StatePoisoned,
            AcquireFatalCase::ClockRegression => BudgetUnavailableReason::ClockRegression,
            AcquireFatalCase::WindowDeadlineOverflow => BudgetUnavailableReason::DeadlineOverflow,
            AcquireFatalCase::RequestsCounterCorrupt | AcquireFatalCase::InFlightCounterCorrupt => {
                BudgetUnavailableReason::StateCorrupt
            }
            AcquireFatalCase::PersistenceFailure => BudgetUnavailableReason::PersistenceUnavailable,
        };
        assert!(matches!(
            fixture.budget.try_acquire(),
            BudgetDecision::Unavailable(reason) if reason == expected
        ));
        assert_global_terminal(
            fixture,
            !matches!(case, AcquireFatalCase::PersistenceFailure),
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum RetryAfterFatalCase {
    ClockUnavailable,
    StatePoisoned,
    RelativeMonotonicOverflow,
    AbsoluteWallSubtractionOverflow,
    AbsoluteMonotonicOverflow,
    PersistenceFailure,
}

#[test]
fn retry_after_fatal_branch_matrix_invalidates_global_durability() -> TestResult {
    for case in [
        RetryAfterFatalCase::ClockUnavailable,
        RetryAfterFatalCase::StatePoisoned,
        RetryAfterFatalCase::RelativeMonotonicOverflow,
        RetryAfterFatalCase::AbsoluteWallSubtractionOverflow,
        RetryAfterFatalCase::AbsoluteMonotonicOverflow,
        RetryAfterFatalCase::PersistenceFailure,
    ] {
        let fixture = global_fault_fixture()?;
        let retry_after = match case {
            RetryAfterFatalCase::ClockUnavailable => {
                fixture.clock.fail();
                RetryAfter::Delay(NonZeroU64::new(1).ok_or("retry delay must be nonzero")?)
            }
            RetryAfterFatalCase::StatePoisoned => {
                poison_budget_state(&fixture.budget);
                RetryAfter::Delay(NonZeroU64::new(1).ok_or("retry delay must be nonzero")?)
            }
            RetryAfterFatalCase::RelativeMonotonicOverflow => {
                fixture.clock.set(i64::MAX, u64::MAX)?;
                RetryAfter::Delay(NonZeroU64::new(1).ok_or("retry delay must be nonzero")?)
            }
            RetryAfterFatalCase::AbsoluteWallSubtractionOverflow => {
                fixture.clock.set(i64::MIN, 0)?;
                RetryAfter::AtWallClock(Timestamp::from_unix_nanos(i64::MAX))
            }
            RetryAfterFatalCase::AbsoluteMonotonicOverflow => {
                fixture.clock.set(0, u64::MAX)?;
                RetryAfter::AtWallClock(Timestamp::from_unix_nanos(1))
            }
            RetryAfterFatalCase::PersistenceFailure => {
                fixture.store.reject_stores.store(true, Ordering::Release);
                RetryAfter::Delay(NonZeroU64::new(1).ok_or("retry delay must be nonzero")?)
            }
        };
        let expected = match case {
            RetryAfterFatalCase::ClockUnavailable => BudgetUnavailableReason::ClockUnavailable,
            RetryAfterFatalCase::StatePoisoned => BudgetUnavailableReason::StatePoisoned,
            RetryAfterFatalCase::PersistenceFailure => {
                BudgetUnavailableReason::PersistenceUnavailable
            }
            RetryAfterFatalCase::RelativeMonotonicOverflow
            | RetryAfterFatalCase::AbsoluteWallSubtractionOverflow
            | RetryAfterFatalCase::AbsoluteMonotonicOverflow => {
                BudgetUnavailableReason::DeadlineOverflow
            }
        };
        assert!(matches!(
            fixture.budget.apply_retry_after(retry_after),
            BudgetDecision::Unavailable(reason) if reason == expected
        ));
        assert_global_terminal(
            fixture,
            !matches!(case, RetryAfterFatalCase::PersistenceFailure),
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum RefusalFatalCase {
    ClockUnavailable,
    StatePoisoned,
    CounterOverflow,
    DeadlineOverflow,
    PersistenceFailure,
}

#[test]
fn refusal_fatal_branch_matrix_invalidates_global_durability() -> TestResult {
    for case in [
        RefusalFatalCase::ClockUnavailable,
        RefusalFatalCase::StatePoisoned,
        RefusalFatalCase::CounterOverflow,
        RefusalFatalCase::DeadlineOverflow,
        RefusalFatalCase::PersistenceFailure,
    ] {
        let fixture = global_fault_fixture()?;
        match case {
            RefusalFatalCase::ClockUnavailable => fixture.clock.fail(),
            RefusalFatalCase::StatePoisoned => poison_budget_state(&fixture.budget),
            RefusalFatalCase::CounterOverflow => {
                fixture
                    .budget
                    .allocation
                    .state
                    .lock()
                    .map_err(|_| "budget state lock poisoned")?
                    .consecutive_refusals = u32::MAX;
            }
            RefusalFatalCase::DeadlineOverflow => {
                fixture.clock.set(i64::MAX, u64::MAX)?;
            }
            RefusalFatalCase::PersistenceFailure => {
                fixture.store.reject_stores.store(true, Ordering::Release);
            }
        }
        let expected = match case {
            RefusalFatalCase::ClockUnavailable => BudgetUnavailableReason::ClockUnavailable,
            RefusalFatalCase::StatePoisoned => BudgetUnavailableReason::StatePoisoned,
            RefusalFatalCase::CounterOverflow => BudgetUnavailableReason::StateCorrupt,
            RefusalFatalCase::DeadlineOverflow => BudgetUnavailableReason::DeadlineOverflow,
            RefusalFatalCase::PersistenceFailure => BudgetUnavailableReason::PersistenceUnavailable,
        };
        assert!(matches!(
            fixture.budget.apply_refusal(0),
            BudgetDecision::Unavailable(reason) if reason == expected
        ));
        assert_global_terminal(
            fixture,
            !matches!(case, RefusalFatalCase::PersistenceFailure),
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum AdministrativeFatalCase {
    RecordSuccessClockUnavailable,
    RecordSuccessPoisoned,
    RecordSuccessPersistenceFailure,
    DisableClockUnavailable,
    DisablePoisoned,
    DisablePersistenceFailure,
}

#[test]
fn administrative_fatal_branch_matrix_invalidates_global_durability() -> TestResult {
    for case in [
        AdministrativeFatalCase::RecordSuccessClockUnavailable,
        AdministrativeFatalCase::RecordSuccessPoisoned,
        AdministrativeFatalCase::RecordSuccessPersistenceFailure,
        AdministrativeFatalCase::DisableClockUnavailable,
        AdministrativeFatalCase::DisablePoisoned,
        AdministrativeFatalCase::DisablePersistenceFailure,
    ] {
        let fixture = global_fault_fixture()?;
        match case {
            AdministrativeFatalCase::RecordSuccessClockUnavailable
            | AdministrativeFatalCase::DisableClockUnavailable => fixture.clock.fail(),
            AdministrativeFatalCase::RecordSuccessPoisoned
            | AdministrativeFatalCase::DisablePoisoned => {
                poison_budget_state(&fixture.budget);
            }
            AdministrativeFatalCase::RecordSuccessPersistenceFailure
            | AdministrativeFatalCase::DisablePersistenceFailure => {
                fixture.store.reject_stores.store(true, Ordering::Release);
            }
        }
        let observed = match case {
            AdministrativeFatalCase::RecordSuccessClockUnavailable
            | AdministrativeFatalCase::RecordSuccessPoisoned
            | AdministrativeFatalCase::RecordSuccessPersistenceFailure => {
                fixture.budget.record_success()
            }
            AdministrativeFatalCase::DisableClockUnavailable
            | AdministrativeFatalCase::DisablePoisoned
            | AdministrativeFatalCase::DisablePersistenceFailure => {
                match fixture.budget.disable() {
                    BudgetDecision::Unavailable(reason) => Err(reason),
                    other => return Err(format!("unexpected disable result: {other:?}").into()),
                }
            }
        };
        let expected = match case {
            AdministrativeFatalCase::RecordSuccessClockUnavailable
            | AdministrativeFatalCase::DisableClockUnavailable => {
                BudgetUnavailableReason::ClockUnavailable
            }
            AdministrativeFatalCase::RecordSuccessPoisoned
            | AdministrativeFatalCase::DisablePoisoned => BudgetUnavailableReason::StatePoisoned,
            AdministrativeFatalCase::RecordSuccessPersistenceFailure
            | AdministrativeFatalCase::DisablePersistenceFailure => {
                BudgetUnavailableReason::PersistenceUnavailable
            }
        };
        assert_eq!(observed, Err(expected));
        assert_global_terminal(
            fixture,
            !matches!(
                case,
                AdministrativeFatalCase::RecordSuccessPersistenceFailure
                    | AdministrativeFatalCase::DisablePersistenceFailure
            ),
        )?;
    }
    Ok(())
}

#[test]
fn over_policy_retry_after_is_durably_restrictive_without_invalidating_peers() -> TestResult {
    let fixture = global_fault_fixture()?;
    let excessive = fixture
        .budget
        .policy()
        .backoff()
        .maximum_nanos()
        .checked_add(1)
        .and_then(NonZeroU64::new)
        .ok_or("excessive retry delay overflow")?;
    assert!(matches!(
        fixture
            .budget
            .apply_retry_after(RetryAfter::Delay(excessive)),
        BudgetDecision::Unavailable(BudgetUnavailableReason::RetryAfterExceedsPolicy)
    ));
    assert!(fixture.session.is_available());
    assert!(!fixture.prior.is_available());
    assert!(matches!(
        fixture.alias.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled)
    ));
    assert!(fixture.peer.availability_lease().is_ok());
    let envelope = deserialize_canonical_envelope(&fixture.store.payload()?)?;
    let restricted = envelope
        .budgets
        .as_slice()
        .get(fixture.slot)
        .ok_or("restricted budget group missing")?
        .checkpoint();
    assert!(restricted.disabled);
    assert!(!restricted.terminal && !restricted.poisoned);
    Ok(())
}

#[test]
fn cooldown_quota_concurrency_and_post_mint_generation_changes_remain_transient() -> TestResult {
    let retry = global_fault_fixture()?;
    assert!(matches!(
        retry.budget.apply_retry_after(RetryAfter::Delay(
            NonZeroU64::new(1).ok_or("retry delay must be nonzero")?
        )),
        BudgetDecision::WaitUntil(_)
    ));
    assert!(retry.session.is_available());
    retry.clock.set(101, 1)?;
    assert!(matches!(
        retry.alias.try_acquire(),
        BudgetDecision::Ready(_)
    ));

    let quota = global_fault_fixture()?;
    quota
        .budget
        .allocation
        .state
        .lock()
        .map_err(|_| "budget state lock poisoned")?
        .requests_used = quota.budget.policy().requests_per_window();
    assert!(matches!(
        quota.budget.availability_lease(),
        Err(BudgetUnavailableReason::RequestWindowExhausted)
    ));
    assert!(quota.session.is_available());
    let next_window = quota.budget.policy().window_nanos();
    quota.clock.set(
        i64::try_from(next_window).map_err(|_| "window does not fit wall clock")? + 100,
        next_window,
    )?;
    assert!(quota.alias.availability_lease().is_ok());

    let concurrency = global_fault_fixture()?;
    let permit = match concurrency.budget.try_acquire() {
        BudgetDecision::Ready(permit) => permit,
        other => return Err(format!("concurrency setup failed: {other:?}").into()),
    };
    assert!(
        !concurrency.prior.is_available(),
        "a legitimately consumed final slot must revoke the older lease"
    );
    assert!(matches!(
        concurrency.alias.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::ConcurrencyExhausted)
    ));
    assert!(concurrency.session.is_available());
    permit.release();
    assert!(concurrency.alias.availability_lease().is_ok());
    Ok(())
}
