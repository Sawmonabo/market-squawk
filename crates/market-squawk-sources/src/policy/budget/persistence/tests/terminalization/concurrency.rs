//! Deterministic single-flight, transaction, permit, and phase-race coverage.

use std::sync::mpsc;

use super::*;

struct BlockingBudgetFixture {
    store: Arc<BlockingStore>,
    session: Arc<AuthorityDurabilitySession>,
    clock: Arc<SwitchableClock>,
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
            primary_sliding_releases: VecDeque::new(),
            additional_windows: Vec::new(),
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
        clock,
        budget,
        slot,
    })
}

#[test]
fn already_admitted_faults_have_one_terminal_writer_for_success_and_failure() -> TestResult {
    for terminal_store_fails in [false, true] {
        let BlockingBudgetFixture {
            store,
            session,
            clock: _,
            budget,
            slot: _,
        } = blocking_budget()?;
        let mut operations = Vec::new();
        for _ in 0..8 {
            operations.push(
                budget
                    .admit_runtime_operation()
                    .map_err(|reason| format!("peer admission failed: {reason:?}"))?,
            );
        }
        let owner_operation = operations.remove(0);
        let calls_before = store.store_calls.load(Ordering::Acquire);
        if terminal_store_fails {
            store.reject_store_call(calls_before + 1);
        }
        store.block_next_store();
        let owner_budget = budget.clone();
        let owner = std::thread::spawn(move || {
            owner_budget.terminal_fault(BudgetUnavailableReason::StateCorrupt, &owner_operation)
        });
        let blocked_store = store.wait_until_blocked()?;
        assert!(!session.is_available());

        let (result_tx, result_rx) = mpsc::sync_channel(operations.len());
        let peers: Vec<_> = operations
            .into_iter()
            .map(|operation| {
                let peer_budget = budget.clone();
                let result_tx = result_tx.clone();
                std::thread::spawn(move || {
                    result_tx
                        .send(
                            peer_budget
                                .terminal_fault(BudgetUnavailableReason::StateCorrupt, &operation),
                        )
                        .is_ok()
                })
            })
            .collect();
        drop(result_tx);
        for _ in &peers {
            assert_eq!(
                result_rx.recv_timeout(TEST_WATCHDOG_TIMEOUT)?,
                BudgetUnavailableReason::PersistenceUnavailable,
                "a terminal peer waited or retried persistence"
            );
        }
        assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before + 1);
        blocked_store.release()?;
        let expected_owner = if terminal_store_fails {
            BudgetUnavailableReason::PersistenceUnavailable
        } else {
            BudgetUnavailableReason::StateCorrupt
        };
        assert_eq!(
            owner.join().map_err(|_| "terminal owner panicked")?,
            expected_owner
        );
        for peer in peers {
            assert!(peer.join().map_err(|_| "terminal peer panicked")?);
        }
        assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before + 1);
        assert!(matches!(
            budget.try_acquire(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
        ));
        assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before + 1);
    }
    Ok(())
}

#[test]
fn close_winner_rejects_update_and_registry_writes_without_extra_io() -> TestResult {
    let BlockingBudgetFixture {
        store,
        session,
        clock: _,
        budget: _,
        slot,
    } = blocking_budget()?;
    let checkpoint = session
        .budget_groups()?
        .get(slot)
        .ok_or("checkpoint missing")?
        .checkpoint()
        .clone();
    store.block_next_store();
    let closing = session.clone();
    let close = std::thread::spawn(move || {
        closing.close_clean_for_test(
            crate::RegistryAuthorityState::empty(),
            Timestamp::from_unix_nanos(200),
        )
    });
    let blocked_store = store.wait_until_blocked()?;
    let calls_with_close_blocked = store.store_calls.load(Ordering::Acquire);
    let (result_tx, result_rx) = mpsc::sync_channel(2);
    let updating = session.clone();
    let update_tx = result_tx.clone();
    let update = std::thread::spawn(move || {
        update_tx
            .send(updating.update_budget(slot, checkpoint, Timestamp::from_unix_nanos(200)))
            .is_ok()
    });
    let persisting = session.clone();
    let persist = std::thread::spawn(move || {
        result_tx
            .send(persisting.persist_registry(
                crate::RegistryAuthorityState::empty(),
                Timestamp::from_unix_nanos(200),
            ))
            .is_ok()
    });
    for _ in 0..2 {
        assert_eq!(
            result_rx.recv_timeout(TEST_WATCHDOG_TIMEOUT)?,
            Err(AuthorityPersistenceError::SessionUnavailable)
        );
    }
    assert_eq!(
        store.store_calls.load(Ordering::Acquire),
        calls_with_close_blocked
    );
    blocked_store.release()?;
    assert_eq!(close.join().map_err(|_| "close thread panicked")?, Ok(()));
    assert!(update.join().map_err(|_| "update thread panicked")?);
    assert!(persist.join().map_err(|_| "persist thread panicked")?);
    assert_eq!(
        store.store_calls.load(Ordering::Acquire),
        calls_with_close_blocked
    );
    Ok(())
}

#[test]
fn normal_store_failure_retains_store_for_one_terminal_attempt() -> TestResult {
    let DurableBudgetFixture {
        store,
        session,
        clock: _,
        budget,
        slot: _,
    } = durable_budget(1)?;
    let calls_before = store.store_calls.load(Ordering::Acquire);
    store.reject_stores.store(true, Ordering::Release);
    assert_eq!(
        session.persist_registry(
            crate::RegistryAuthorityState::empty(),
            Timestamp::from_unix_nanos(200),
        ),
        Err(AuthorityPersistenceError::Store)
    );
    assert_eq!(
        store.store_calls.load(Ordering::Acquire),
        calls_before + 2,
        "normal failure must retain the store for one terminal attempt"
    );
    assert!(matches!(
        budget.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
    ));
    assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before + 2);
    store.reject_stores.store(false, Ordering::Release);
    let restarted = AuthorityDurabilitySession::open(store, Timestamp::from_unix_nanos(200))?;
    assert!(restarted.recovered_unclean());
    Ok(())
}

#[test]
fn explicit_and_drop_release_retain_admission_during_blocked_terminal_write() -> TestResult {
    for explicit_release in [true, false] {
        let BlockingBudgetFixture {
            store,
            session,
            clock,
            budget,
            slot: _,
        } = blocking_budget()?;
        let permit = match budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("permit setup failed: {other:?}").into()),
        };
        clock.fail();
        let calls_before = store.store_calls.load(Ordering::Acquire);
        store.block_next_store();
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let releasing = std::thread::spawn(move || {
            if explicit_release {
                permit.release();
            } else {
                drop(permit);
            }
            release_tx.send(()).is_ok()
        });
        let blocked_store = store.wait_until_blocked()?;
        assert!(!session.is_available());
        assert_eq!(
            session.close_clean_for_test(
                crate::RegistryAuthorityState::empty(),
                Timestamp::from_unix_nanos(100),
            ),
            Err(AuthorityPersistenceError::SessionUnavailable)
        );
        assert!(matches!(
            release_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before + 1);
        blocked_store.release()?;
        release_rx.recv_timeout(TEST_WATCHDOG_TIMEOUT)?;
        assert!(releasing.join().map_err(|_| "release thread panicked")?);
        assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before + 1);
    }
    Ok(())
}

#[test]
fn generation_exhaustion_has_one_terminal_write_and_no_later_normal_write() -> TestResult {
    let DurableBudgetFixture {
        store,
        session: _,
        clock: _,
        budget,
        slot: _,
    } = durable_budget(1)?;
    budget
        .allocation
        .availability_generation
        .store(u64::MAX, Ordering::Release);
    let calls_before = store.store_calls.load(Ordering::Acquire);
    assert!(matches!(
        budget.try_acquire(),
        BudgetDecision::Unavailable(BudgetUnavailableReason::AvailabilityGenerationExhausted)
    ));
    assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before + 1);
    assert_eq!(
        budget.record_success(),
        Err(BudgetUnavailableReason::PersistenceUnavailable)
    );
    assert_eq!(store.store_calls.load(Ordering::Acquire), calls_before + 1);
    Ok(())
}
