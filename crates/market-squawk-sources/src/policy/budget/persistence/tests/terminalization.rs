//! Fatal durable-budget terminalization and session blast-radius tests.

use super::*;

#[path = "terminalization/availability_changed.rs"]
mod availability_changed;
#[path = "terminalization/branch_matrix.rs"]
mod branch_matrix;
#[path = "terminalization/concurrency.rs"]
mod concurrency;
#[path = "terminalization/lifecycle.rs"]
mod lifecycle;

#[derive(Debug)]
struct SwitchableClock {
    observation: Mutex<ClockObservation>,
    available: AtomicBool,
}

impl SwitchableClock {
    fn new(wall: i64, monotonic: u64) -> Self {
        Self {
            observation: Mutex::new(ClockObservation::new(
                Timestamp::from_unix_nanos(wall),
                MonotonicInstant::from_nanos(monotonic),
            )),
            available: AtomicBool::new(true),
        }
    }

    fn set(&self, wall: i64, monotonic: u64) -> TestResult {
        *self
            .observation
            .lock()
            .map_err(|_| "clock observation lock poisoned")? = ClockObservation::new(
            Timestamp::from_unix_nanos(wall),
            MonotonicInstant::from_nanos(monotonic),
        );
        Ok(())
    }

    fn fail(&self) {
        self.available.store(false, Ordering::Release);
    }

    fn recover(&self) {
        self.available.store(true, Ordering::Release);
    }
}

impl BudgetClock for SwitchableClock {
    fn observation(&self) -> Result<ClockObservation, BudgetUnavailableReason> {
        if !self.available.load(Ordering::Acquire) {
            return Err(BudgetUnavailableReason::ClockUnavailable);
        }
        self.observation
            .lock()
            .map(|observation| *observation)
            .map_err(|_| BudgetUnavailableReason::ClockUnavailable)
    }

    fn shared_allocation_charge(&self) -> usize {
        std::mem::size_of::<Self>() + crate::conservative_arc_control_block_charge::<Self>()
    }
}

struct DurableBudgetFixture {
    store: Arc<MemoryStore>,
    session: Arc<AuthorityDurabilitySession>,
    clock: Arc<SwitchableClock>,
    budget: SharedProviderBudget,
    slot: usize,
}

fn durable_budget(index: u8) -> TestResult<DurableBudgetFixture> {
    let store = Arc::new(MemoryStore::default());
    let session = AuthorityDurabilitySession::open(store.clone(), Timestamp::from_unix_nanos(100))?;
    let declaration = declaration(index)?;
    let clock = Arc::new(SwitchableClock::new(100, 0));
    let state = BudgetState {
        window_started_at: MonotonicInstant::from_nanos(0),
        restored_window_ends_at: None,
        requests_used: 0,
        in_flight: 0,
        unavailable_until: None,
        disabled: false,
        consecutive_refusals: 0,
    };
    let observation = clock
        .observation()
        .map_err(|reason| format!("clock setup failed: {reason:?}"))?;
    let checkpoint = checkpoint_from_runtime(declaration.policy(), &state, observation, 1, false)?;
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
    Ok(DurableBudgetFixture {
        store,
        session,
        clock,
        budget,
        slot,
    })
}

fn assert_persisted_terminal(
    store: &MemoryStore,
    slot: usize,
) -> TestResult<DurableAuthorityEnvelope> {
    let envelope = deserialize_canonical_envelope(&store.payload()?)?;
    let persisted = envelope
        .budgets
        .as_slice()
        .get(slot)
        .ok_or("terminal checkpoint slot missing")?
        .checkpoint();
    assert!(persisted.terminal);
    assert!(persisted.poisoned);
    assert!(persisted.disabled);
    assert_eq!(envelope.run_state, DurableRunState::InUse);
    Ok(envelope)
}

#[derive(Clone, Copy, Debug)]
enum FatalEntryPoint {
    AvailabilityLease,
    TryAcquire,
    RetryAfter,
    Refusal,
    RecordSuccess,
    Disable,
    PermitRelease,
}

impl FatalEntryPoint {
    const ALL: [Self; 7] = [
        Self::AvailabilityLease,
        Self::TryAcquire,
        Self::RetryAfter,
        Self::Refusal,
        Self::RecordSuccess,
        Self::Disable,
        Self::PermitRelease,
    ];
}

#[test]
fn every_fatal_entry_point_terminalizes_and_invalidates_the_durable_session() -> TestResult {
    for entry_point in FatalEntryPoint::ALL {
        let DurableBudgetFixture {
            store,
            session,
            clock,
            budget,
            slot,
        } = durable_budget(1)?;
        let permit = if matches!(entry_point, FatalEntryPoint::PermitRelease) {
            match budget.try_acquire() {
                BudgetDecision::Ready(permit) => Some(permit),
                other => return Err(format!("permit setup failed: {other:?}").into()),
            }
        } else {
            None
        };
        clock.fail();
        match entry_point {
            FatalEntryPoint::AvailabilityLease => {
                let _result = budget.availability_lease();
            }
            FatalEntryPoint::TryAcquire => {
                let _decision = budget.try_acquire();
            }
            FatalEntryPoint::RetryAfter => {
                let _decision = budget.apply_retry_after(RetryAfter::Delay(
                    NonZeroU64::new(1).ok_or("retry delay must be nonzero")?,
                ));
            }
            FatalEntryPoint::Refusal => {
                let _decision = budget.apply_refusal(0);
            }
            FatalEntryPoint::RecordSuccess => {
                let _result = budget.record_success();
            }
            FatalEntryPoint::Disable => {
                let _decision = budget.disable();
            }
            FatalEntryPoint::PermitRelease => {
                permit.ok_or("permit missing")?.release();
            }
        }
        assert!(
            !session.is_available(),
            "fatal entry point recovered: {entry_point:?}"
        );
        assert!(budget.allocation.terminal.load(Ordering::Acquire));
        assert_persisted_terminal(&store, slot)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum FatalIntegrityFault {
    StatePoison,
    ClockRegression,
    WindowDeadlineOverflow,
    RetryDeadlineOverflow,
    RefusalCounterOverflow,
}

impl FatalIntegrityFault {
    const ALL: [Self; 5] = [
        Self::StatePoison,
        Self::ClockRegression,
        Self::WindowDeadlineOverflow,
        Self::RetryDeadlineOverflow,
        Self::RefusalCounterOverflow,
    ];
}

#[allow(clippy::panic)]
fn poison_budget_state(budget: &SharedProviderBudget) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let Ok(_state) = budget.allocation.state.lock() else {
            return;
        };
        panic!("test-only durable budget state poison");
    }));
    assert!(result.is_err());
}

#[test]
fn fatal_integrity_faults_cannot_recover_in_the_same_session() -> TestResult {
    for fault in FatalIntegrityFault::ALL {
        let DurableBudgetFixture {
            store,
            session,
            clock,
            budget,
            slot,
        } = durable_budget(1)?;
        match fault {
            FatalIntegrityFault::StatePoison => poison_budget_state(&budget),
            FatalIntegrityFault::ClockRegression => {
                budget
                    .allocation
                    .state
                    .lock()
                    .map_err(|_| "budget state lock poisoned")?
                    .window_started_at = MonotonicInstant::from_nanos(1);
            }
            FatalIntegrityFault::WindowDeadlineOverflow => {
                clock.set(i64::MAX, u64::MAX)?;
                budget
                    .allocation
                    .state
                    .lock()
                    .map_err(|_| "budget state lock poisoned")?
                    .window_started_at = MonotonicInstant::from_nanos(u64::MAX);
            }
            FatalIntegrityFault::RetryDeadlineOverflow => {
                clock.set(i64::MAX, u64::MAX)?;
            }
            FatalIntegrityFault::RefusalCounterOverflow => {
                budget
                    .allocation
                    .state
                    .lock()
                    .map_err(|_| "budget state lock poisoned")?
                    .consecutive_refusals = u32::MAX;
            }
        }
        match fault {
            FatalIntegrityFault::RetryDeadlineOverflow => {
                let _decision = budget.apply_retry_after(RetryAfter::Delay(
                    NonZeroU64::new(1).ok_or("retry delay must be nonzero")?,
                ));
            }
            FatalIntegrityFault::RefusalCounterOverflow => {
                let _decision = budget.apply_refusal(0);
            }
            FatalIntegrityFault::StatePoison
            | FatalIntegrityFault::ClockRegression
            | FatalIntegrityFault::WindowDeadlineOverflow => {
                let _decision = budget.try_acquire();
            }
        }
        assert!(!session.is_available(), "fatal fault recovered: {fault:?}");
        assert_persisted_terminal(&store, slot)?;
    }
    Ok(())
}

#[test]
fn one_fatal_scope_revokes_alias_peer_future_registration_shutdown_and_restart() -> TestResult {
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
    let peer_checkpoint = checkpoint_from_runtime(
        peer_declaration.policy(),
        &BudgetState {
            window_started_at: MonotonicInstant::from_nanos(0),
            restored_window_ends_at: None,
            requests_used: 0,
            in_flight: 0,
            unavailable_until: None,
            disabled: false,
            consecutive_refusals: 0,
        },
        clock
            .observation()
            .map_err(|reason| format!("clock setup failed: {reason:?}"))?,
        1,
        false,
    )?;
    let peer_slot = session.register_budget_group(
        crate::RegistryAuthorityState::empty(),
        peer_declaration.clone(),
        peer_checkpoint,
        Timestamp::from_unix_nanos(100),
    )?;
    let peer = SharedProviderBudget::new_durable(
        peer_declaration.policy().clone(),
        MonotonicInstant::from_nanos(0),
        clock.clone(),
        BudgetDurabilityBinding {
            session: session.clone(),
            slot: peer_slot,
        },
    );

    clock.fail();
    assert!(matches!(
        budget.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::ClockUnavailable)
    ));
    clock.recover();
    assert!(!prior.is_available());
    assert!(matches!(
        alias.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert!(matches!(
        peer.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert!(matches!(
        session.register_budget_group(
            crate::RegistryAuthorityState::empty(),
            declaration(3)?,
            checkpoint(3),
            Timestamp::from_unix_nanos(100),
        ),
        Err(AuthorityPersistenceError::SessionUnavailable)
    ));
    assert!(matches!(
        session.close_clean_for_test(
            crate::RegistryAuthorityState::empty(),
            Timestamp::from_unix_nanos(100),
        ),
        Err(AuthorityPersistenceError::SessionUnavailable)
    ));
    assert_persisted_terminal(&store, slot)?;

    let restarted = AuthorityDurabilitySession::open(store, Timestamp::from_unix_nanos(100))?;
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

#[test]
fn failed_terminal_store_leaves_in_use_state_for_unclean_restart_rejection() -> TestResult {
    let DurableBudgetFixture {
        store,
        session,
        clock,
        budget,
        slot: _,
    } = durable_budget(1)?;
    store.reject_stores.store(true, Ordering::Release);
    let calls_before_terminal = store.store_calls.load(Ordering::Acquire);
    clock.fail();
    let _decision = budget.try_acquire();
    assert!(!session.is_available());
    assert_eq!(
        store.store_calls.load(Ordering::Acquire),
        calls_before_terminal + 1,
        "the sole terminal writer must attempt exactly one durable publication"
    );
    assert!(matches!(
        budget.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert_eq!(
        store.store_calls.load(Ordering::Acquire),
        calls_before_terminal + 1,
        "a failed terminal outcome must never be retried by a later caller"
    );
    store.reject_stores.store(false, Ordering::Release);

    let restarted = AuthorityDurabilitySession::open(store, Timestamp::from_unix_nanos(100))?;
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

#[test]
fn repeated_terminal_persistence_succeeds_only_after_a_proven_terminal_write() -> TestResult {
    let DurableBudgetFixture {
        store,
        session,
        clock: _,
        budget,
        slot: _,
    } = durable_budget(1)?;

    let calls_before_terminal = store.store_calls.load(Ordering::Acquire);
    let operation = budget
        .admit_runtime_operation()
        .map_err(|reason| format!("operation admission failed: {reason:?}"))?;
    assert_eq!(
        budget.terminal_fault(BudgetUnavailableReason::StateCorrupt, &operation,),
        BudgetUnavailableReason::StateCorrupt
    );
    assert_eq!(
        store.store_calls.load(Ordering::Acquire),
        calls_before_terminal + 1
    );
    assert!(matches!(
        budget.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert_eq!(
        store.store_calls.load(Ordering::Acquire),
        calls_before_terminal + 1,
        "a proven terminal outcome must never be republished by a later caller"
    );
    assert!(!session.is_available());
    Ok(())
}

#[test]
fn clean_close_winner_rejects_stale_runtime_entry_without_terminal_io() -> TestResult {
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
        clock,
        BudgetDurabilityBinding {
            session: session.clone(),
            slot,
        },
    );
    store.block_next_store();

    let closing_session = session.clone();
    let close = std::thread::spawn(move || {
        closing_session.close_clean_for_test(
            crate::RegistryAuthorityState::empty(),
            Timestamp::from_unix_nanos(100),
        )
    });
    let blocked_store = store.wait_until_blocked()?;
    let (decision_tx, decision_rx) = std::sync::mpsc::sync_channel(1);
    let stale = std::thread::spawn(move || decision_tx.send(budget.try_acquire()).is_ok());
    assert!(matches!(
        decision_rx.recv_timeout(TEST_WATCHDOG_TIMEOUT)?,
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    blocked_store.release()?;

    assert_eq!(close.join().map_err(|_| "close thread panicked")?, Ok(()));
    assert!(stale.join().map_err(|_| "stale thread panicked")?);
    let payload = store
        .load()
        .map_err(|_| "blocking store load failed")?
        .ok_or("blocking store payload missing")?;
    let envelope = deserialize_canonical_envelope(&payload)?;
    assert_eq!(envelope.run_state, DurableRunState::Clean);
    let restarted = AuthorityDurabilitySession::open(store, Timestamp::from_unix_nanos(100))?;
    assert!(!restarted.recovered_unclean());
    assert!(restarted.is_available());
    Ok(())
}

#[test]
fn terminal_fault_publishes_the_global_latch_before_the_terminal_store_finishes() -> TestResult {
    use std::sync::mpsc;

    let store = Arc::new(BlockingStore::default());
    let session = AuthorityDurabilitySession::open(store.clone(), Timestamp::from_unix_nanos(100))?;
    let clock = Arc::new(SwitchableClock::new(100, 0));
    let observation = clock
        .observation()
        .map_err(|reason| format!("clock setup failed: {reason:?}"))?;
    let make_budget = |index: u8| -> TestResult<SharedProviderBudget> {
        let declaration = declaration(index)?;
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
        Ok(SharedProviderBudget::new_durable(
            declaration.policy().clone(),
            observation.monotonic,
            clock.clone(),
            BudgetDurabilityBinding {
                session: session.clone(),
                slot,
            },
        ))
    };
    let failing = make_budget(1)?;
    let peer = make_budget(2)?;
    let peer_lease = peer
        .availability_lease()
        .map_err(|reason| format!("peer availability setup failed: {reason:?}"))?;
    store.block_next_store();

    let terminal_operation = failing
        .admit_runtime_operation()
        .map_err(|reason| format!("terminal operation admission failed: {reason:?}"))?;
    let terminal = std::thread::spawn(move || {
        failing.terminal_fault(BudgetUnavailableReason::StateCorrupt, &terminal_operation)
    });
    let blocked_store = store.wait_until_blocked()?;
    assert!(!session.is_available());
    assert!(!peer_lease.is_available());

    let (decision_tx, decision_rx) = mpsc::sync_channel(1);
    let peer_request = std::thread::spawn(move || decision_tx.send(peer.try_acquire()).is_ok());
    assert!(matches!(
        decision_rx.recv_timeout(TEST_WATCHDOG_TIMEOUT)?,
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    blocked_store.release()?;

    assert_eq!(
        terminal.join().map_err(|_| "terminal thread panicked")?,
        BudgetUnavailableReason::StateCorrupt
    );
    assert!(peer_request.join().map_err(|_| "peer thread panicked")?);
    Ok(())
}
