//! Adversarial lifecycle admission and clean-close linearization coverage.

use std::sync::Condvar;

use super::*;

#[derive(Debug)]
struct BlockingFailClock {
    observation: ClockObservation,
    armed: AtomicBool,
    entered: (Mutex<bool>, Condvar),
    released: (Mutex<bool>, Condvar),
}

#[derive(Debug)]
struct BlockedClockRelease<'a> {
    clock: &'a BlockingFailClock,
    released: bool,
}

impl BlockedClockRelease<'_> {
    fn release(mut self) -> TestResult {
        self.clock.signal_release()?;
        self.released = true;
        Ok(())
    }
}

impl Drop for BlockedClockRelease<'_> {
    fn drop(&mut self) {
        if !self.released {
            let _release_result = self.clock.signal_release();
        }
    }
}

impl BlockingFailClock {
    fn new(observation: ClockObservation) -> Self {
        Self {
            observation,
            armed: AtomicBool::new(false),
            entered: (Mutex::new(false), Condvar::new()),
            released: (Mutex::new(false), Condvar::new()),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    fn wait_until_entered(&self) -> TestResult<BlockedClockRelease<'_>> {
        self.wait_until_entered_with_timeout(TEST_WATCHDOG_TIMEOUT)
    }

    fn wait_until_entered_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> TestResult<BlockedClockRelease<'_>> {
        let release = BlockedClockRelease {
            clock: self,
            released: false,
        };
        let (entered, signal) = &self.entered;
        let entered = entered.lock().map_err(|_| "clock entered lock poisoned")?;
        let (entered, wait) = signal
            .wait_timeout_while(entered, timeout, |entered| !*entered)
            .map_err(|_| "clock entered wait poisoned")?;
        if !*entered {
            let message = if wait.timed_out() {
                "timed out waiting for clock entry"
            } else {
                "clock-entry wait woke without entry"
            };
            return Err(message.into());
        }
        Ok(release)
    }

    fn signal_release(&self) -> TestResult {
        let (released, signal) = &self.released;
        *released.lock().map_err(|_| "clock release lock poisoned")? = true;
        signal.notify_all();
        Ok(())
    }
}

impl BudgetClock for BlockingFailClock {
    fn observation(&self) -> Result<ClockObservation, BudgetUnavailableReason> {
        if !self.armed.load(Ordering::Acquire) {
            return Ok(self.observation);
        }
        let (entered, entered_signal) = &self.entered;
        *entered
            .lock()
            .map_err(|_| BudgetUnavailableReason::ClockUnavailable)? = true;
        entered_signal.notify_all();

        let (released, release_signal) = &self.released;
        let mut released = released
            .lock()
            .map_err(|_| BudgetUnavailableReason::ClockUnavailable)?;
        while !*released {
            released = release_signal
                .wait(released)
                .map_err(|_| BudgetUnavailableReason::ClockUnavailable)?;
        }
        Err(BudgetUnavailableReason::ClockUnavailable)
    }

    fn shared_allocation_charge(&self) -> usize {
        std::mem::size_of::<Self>() + crate::conservative_arc_control_block_charge::<Self>()
    }
}

#[test]
fn observer_timeout_releases_a_clock_call_that_enters_late() -> TestResult {
    let observation = ClockObservation::new(
        Timestamp::from_unix_nanos(100),
        MonotonicInstant::from_nanos(0),
    );
    let clock = Arc::new(BlockingFailClock::new(observation));
    clock.arm();
    assert!(
        clock
            .wait_until_entered_with_timeout(std::time::Duration::ZERO)
            .is_err()
    );

    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let late_clock = clock.clone();
    let observer = std::thread::spawn(move || result_tx.send(late_clock.observation()).is_ok());
    assert_eq!(
        result_rx.recv_timeout(TEST_WATCHDOG_TIMEOUT)?,
        Err(BudgetUnavailableReason::ClockUnavailable)
    );
    assert!(
        observer
            .join()
            .map_err(|_| "late clock observer panicked")?
    );
    Ok(())
}

struct BlockingBudgetFixture {
    store: Arc<BlockingStore>,
    session: Arc<AuthorityDurabilitySession>,
    budget: SharedProviderBudget,
    slot: usize,
}

fn blocking_budget() -> TestResult<BlockingBudgetFixture> {
    let store = Arc::new(BlockingStore::default());
    let session = AuthorityDurabilitySession::open(store.clone(), Timestamp::from_unix_nanos(100))?;
    let declaration = declaration(1)?;
    let clock = Arc::new(SwitchableClock::new(100, 0));
    let observation = clock
        .observation()
        .map_err(|reason| format!("clock setup failed: {reason:?}"))?;
    let checkpoint = checkpoint_from_runtime(
        declaration.policy(),
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
    let slot = session.register_budget_group(
        crate::RegistryAuthorityState::empty(),
        declaration.clone(),
        checkpoint,
        observation.wall_clock,
    )?;
    let budget = SharedProviderBudget::new_durable(
        declaration.policy().clone(),
        observation.monotonic,
        clock.clone(),
        BudgetDurabilityBinding {
            session: session.clone(),
            slot,
        },
    );
    Ok(BlockingBudgetFixture {
        store,
        session,
        budget,
        slot,
    })
}

fn wait_until_session_unavailable(session: &AuthorityDurabilitySession) -> TestResult {
    let started = std::time::Instant::now();
    while session.is_available() {
        if started.elapsed() >= TEST_WATCHDOG_TIMEOUT {
            return Err("timed out waiting for terminal lifecycle latch".into());
        }
        std::thread::yield_now();
    }
    Ok(())
}

#[test]
fn admitted_fatal_operation_prevents_clean_write_even_when_terminal_store_fails() -> TestResult {
    let store = Arc::new(MemoryStore::default());
    let session = AuthorityDurabilitySession::open(store.clone(), Timestamp::from_unix_nanos(100))?;
    let declaration = declaration(1)?;
    let observation = ClockObservation::new(
        Timestamp::from_unix_nanos(100),
        MonotonicInstant::from_nanos(0),
    );
    let clock = Arc::new(BlockingFailClock::new(observation));
    let checkpoint = checkpoint_from_runtime(
        declaration.policy(),
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
    let slot = session.register_budget_group(
        crate::RegistryAuthorityState::empty(),
        declaration.clone(),
        checkpoint,
        observation.wall_clock,
    )?;
    let budget = SharedProviderBudget::new_durable(
        declaration.policy().clone(),
        observation.monotonic,
        clock.clone(),
        BudgetDurabilityBinding {
            session: session.clone(),
            slot,
        },
    );

    clock.arm();
    let request = std::thread::spawn(move || budget.try_acquire());
    let blocked_clock = clock.wait_until_entered()?;
    let calls_before_close = store.store_calls.load(Ordering::Acquire);
    assert_eq!(
        session.close_clean_for_test(
            crate::RegistryAuthorityState::empty(),
            Timestamp::from_unix_nanos(100),
        ),
        Err(AuthorityPersistenceError::SessionUnavailable),
        "close must fail fast while a terminal-capable operation is admitted"
    );
    assert_eq!(
        store.store_calls.load(Ordering::Acquire),
        calls_before_close,
        "close must not publish Clean before admitted operations have resolved"
    );

    store.reject_stores.store(true, Ordering::Release);
    blocked_clock.release()?;
    assert!(matches!(
        request.join().map_err(|_| "request thread panicked")?,
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert!(!session.is_available());
    assert_eq!(
        store.store_calls.load(Ordering::Acquire),
        calls_before_close + 1,
        "one terminal writer must own the only failed terminal-store attempt"
    );

    store.reject_stores.store(false, Ordering::Release);
    let restarted = AuthorityDurabilitySession::open(store, Timestamp::from_unix_nanos(100))?;
    assert!(restarted.recovered_unclean());
    assert!(!restarted.is_available());
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum OrdinaryTransaction {
    UpdateBudget,
    PersistRegistry,
}

#[test]
fn ordinary_transaction_admission_prevents_clean_close_for_both_write_paths() -> TestResult {
    for transaction in [
        OrdinaryTransaction::UpdateBudget,
        OrdinaryTransaction::PersistRegistry,
    ] {
        let BlockingBudgetFixture {
            store,
            session,
            budget: _,
            slot,
        } = blocking_budget()?;
        let checkpoint = session
            .budget_groups()?
            .get(slot)
            .ok_or("budget checkpoint missing")?
            .checkpoint()
            .clone();
        store.block_next_store();
        let transaction_session = session.clone();
        let write = std::thread::spawn(move || match transaction {
            OrdinaryTransaction::UpdateBudget => {
                transaction_session.update_budget(slot, checkpoint, Timestamp::from_unix_nanos(200))
            }
            OrdinaryTransaction::PersistRegistry => transaction_session.persist_registry(
                crate::RegistryAuthorityState::empty(),
                Timestamp::from_unix_nanos(200),
            ),
        });
        let blocked_store = store.wait_until_blocked()?;
        let calls_while_blocked = store.store_calls.load(Ordering::Acquire);
        assert_eq!(
            session.close_clean_for_test(
                crate::RegistryAuthorityState::empty(),
                Timestamp::from_unix_nanos(200),
            ),
            Err(AuthorityPersistenceError::SessionUnavailable),
            "close crossed an admitted {transaction:?}"
        );
        assert_eq!(
            store.store_calls.load(Ordering::Acquire),
            calls_while_blocked
        );
        blocked_store.release()?;
        assert_eq!(
            write.join().map_err(|_| "ordinary write thread panicked")?,
            Ok(())
        );
        assert_eq!(
            session.close_clean_for_test(
                crate::RegistryAuthorityState::empty(),
                Timestamp::from_unix_nanos(200),
            ),
            Ok(())
        );
    }
    Ok(())
}

#[test]
fn terminal_writer_owns_failed_overwrite_after_blocked_normal_store() -> TestResult {
    let BlockingBudgetFixture {
        store,
        session,
        budget,
        slot: _,
    } = blocking_budget()?;
    let calls_before = store.store_calls.load(Ordering::Acquire);
    store.block_next_store();
    let normal_session = session.clone();
    let normal = std::thread::spawn(move || {
        normal_session.persist_registry(
            crate::RegistryAuthorityState::empty(),
            Timestamp::from_unix_nanos(200),
        )
    });
    let blocked_store = store.wait_until_blocked()?;
    store.reject_store_call(calls_before + 2);
    let terminal_operation = budget
        .admit_runtime_operation()
        .map_err(|reason| format!("terminal admission failed: {reason:?}"))?;
    let terminal = std::thread::spawn(move || {
        budget.terminal_fault(BudgetUnavailableReason::StateCorrupt, &terminal_operation)
    });
    wait_until_session_unavailable(&session)?;
    blocked_store.release()?;
    assert_eq!(
        normal.join().map_err(|_| "normal write thread panicked")?,
        Err(AuthorityPersistenceError::SessionUnavailable)
    );
    assert_eq!(
        terminal.join().map_err(|_| "terminal thread panicked")?,
        BudgetUnavailableReason::PersistenceUnavailable
    );
    assert_eq!(
        store.store_calls.load(Ordering::Acquire),
        calls_before + 2,
        "normal write plus exactly one terminal overwrite were expected"
    );

    let restarted = AuthorityDurabilitySession::open(store, Timestamp::from_unix_nanos(200))?;
    assert!(restarted.recovered_unclean());
    assert!(!restarted.is_available());
    Ok(())
}

#[test]
fn permit_retains_admission_until_explicit_release_or_drop_finishes() -> TestResult {
    for explicit_release in [true, false] {
        let DurableBudgetFixture {
            store,
            session,
            clock: _,
            budget,
            slot: _,
        } = durable_budget(1)?;
        let permit = match budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("permit setup failed: {other:?}").into()),
        };
        let calls_before_close = store.store_calls.load(Ordering::Acquire);
        assert_eq!(
            session.close_clean_for_test(
                crate::RegistryAuthorityState::empty(),
                Timestamp::from_unix_nanos(100),
            ),
            Err(AuthorityPersistenceError::SessionUnavailable)
        );
        assert_eq!(
            store.store_calls.load(Ordering::Acquire),
            calls_before_close
        );
        if explicit_release {
            permit.release();
        } else {
            drop(permit);
        }
        assert!(session.is_available());
        assert_eq!(
            session.close_clean_for_test(
                crate::RegistryAuthorityState::empty(),
                Timestamp::from_unix_nanos(100),
            ),
            Ok(())
        );
    }
    Ok(())
}

#[test]
fn terminal_checkpoint_uses_trusted_durable_wall_high_water() -> TestResult {
    let DurableBudgetFixture {
        store,
        session,
        clock,
        budget,
        slot: _,
    } = durable_budget(1)?;
    session.persist_registry(
        crate::RegistryAuthorityState::empty(),
        Timestamp::from_unix_nanos(500),
    )?;
    clock.fail();
    assert!(matches!(
        budget.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::ClockUnavailable)
    ));
    let envelope = deserialize_canonical_envelope(&store.payload()?)?;
    assert_eq!(envelope.wall_high_water, Timestamp::from_unix_nanos(500));
    assert_eq!(envelope.saved_at_wall, Timestamp::from_unix_nanos(500));
    Ok(())
}

#[test]
fn packed_admission_overflow_terminalizes_once_and_preserves_in_use_restart() -> TestResult {
    let DurableBudgetFixture {
        store,
        session,
        clock: _,
        budget,
        slot: _,
    } = durable_budget(1)?;
    session.lifecycle.store_raw(!7_u64);
    let calls_before = store.store_calls.load(Ordering::Acquire);
    assert!(matches!(
        budget.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before + 1);
    assert!(!session.is_available());
    assert!(matches!(
        budget.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before + 1);

    let restarted = AuthorityDurabilitySession::open(store, Timestamp::from_unix_nanos(100))?;
    assert!(restarted.recovered_unclean());
    assert!(!restarted.is_available());
    Ok(())
}

#[test]
fn runtime_admission_composition_mismatch_fails_every_involved_session_closed() -> TestResult {
    let first = durable_budget(1)?;
    let second = durable_budget(2)?;
    let foreign_token = first.session.admit_operation()?;
    let foreign = RuntimeOperationAdmission::durable_for_test(foreign_token);
    assert!(matches!(
        second.budget.validated_durable_admission(&foreign),
        Err(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert!(!first.session.is_available());
    assert!(!second.session.is_available());

    let third = durable_budget(3)?;
    let ephemeral = RuntimeOperationAdmission::ephemeral_for_test();
    assert!(matches!(
        third.budget.validated_durable_admission(&ephemeral),
        Err(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert!(!third.session.is_available());
    Ok(())
}

#[test]
fn packed_lifecycle_count_tracks_owned_admissions_exactly() -> TestResult {
    let DurableBudgetFixture {
        store: _,
        session,
        clock: _,
        budget: _,
        slot: _,
    } = durable_budget(1)?;
    assert_eq!(session.lifecycle.load_raw(), 0);
    let first = session.admit_operation()?;
    assert_eq!(session.lifecycle.load_raw(), 8);
    let second = session.admit_operation()?;
    assert_eq!(session.lifecycle.load_raw(), 16);
    drop(first);
    assert_eq!(session.lifecycle.load_raw(), 8);
    drop(second);
    assert_eq!(session.lifecycle.load_raw(), 0);
    Ok(())
}

#[test]
#[allow(clippy::panic)]
fn admission_drop_during_unwind_latches_without_io_before_one_control_write() -> TestResult {
    let DurableBudgetFixture {
        store,
        session,
        clock: _,
        budget: _,
        slot: _,
    } = durable_budget(1)?;
    let admission = session.admit_operation()?;
    let calls_before = store.store_calls.load(Ordering::Acquire);
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _admission = admission;
        panic!("test-only admitted operation unwind");
    }));
    assert!(unwind.is_err());
    assert_eq!(session.lifecycle.load_raw(), 2);
    assert!(!session.is_available());
    assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before);
    assert_eq!(
        session.close_clean_for_test(
            crate::RegistryAuthorityState::empty(),
            Timestamp::from_unix_nanos(100),
        ),
        Err(AuthorityPersistenceError::SessionUnavailable)
    );
    assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before);
    assert_eq!(session.persist_terminal_and_detach(), Ok(()));
    assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before + 1);
    Ok(())
}

#[test]
fn impossible_drop_underflow_and_finish_transitions_fail_active_closed() -> TestResult {
    let first = durable_budget(1)?;
    let admission = first.session.admit_operation()?;
    first.session.lifecycle.store_raw(0);
    drop(admission);
    assert_eq!(first.session.lifecycle.load_raw(), 5);
    assert!(!first.session.is_available());

    let second = durable_budget(2)?;
    second.session.finish_clean_close(true);
    assert_eq!(second.session.lifecycle.load_raw(), 5);
    assert!(!second.session.is_available());

    let third = durable_budget(3)?;
    third.session.finish_terminal_write(true);
    assert_eq!(third.session.lifecycle.load_raw(), 5);
    assert!(!third.session.is_available());
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum SessionLockToPoison {
    Envelope,
    Store,
}

#[test]
#[allow(clippy::panic)]
fn poisoned_session_locks_detach_store_without_publishing_clean() -> TestResult {
    for lock in [SessionLockToPoison::Envelope, SessionLockToPoison::Store] {
        let DurableBudgetFixture {
            store,
            session,
            clock: _,
            budget,
            slot: _,
        } = durable_budget(1)?;
        let calls_before = store.store_calls.load(Ordering::Acquire);
        let poisoning = session.clone();
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || match lock {
            SessionLockToPoison::Envelope => {
                if let Ok(_envelope) = poisoning.envelope.lock() {
                    panic!("test-only envelope poison");
                }
            }
            SessionLockToPoison::Store => {
                if let Ok(_store) = poisoning.store.lock() {
                    panic!("test-only store poison");
                }
            }
        }));
        assert!(unwind.is_err());
        assert!(!session.is_available());
        assert!(matches!(
            budget.try_acquire(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
        ));
        assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before);
        let detached = match session.store.lock() {
            Ok(store) => store.is_none(),
            Err(poisoned) => poisoned.into_inner().is_none(),
        };
        assert!(detached, "poisoned {lock:?} retained the store capability");

        let restarted = AuthorityDurabilitySession::open(store, Timestamp::from_unix_nanos(100))?;
        assert!(restarted.recovered_unclean());
        assert!(!restarted.is_available());
    }
    Ok(())
}
