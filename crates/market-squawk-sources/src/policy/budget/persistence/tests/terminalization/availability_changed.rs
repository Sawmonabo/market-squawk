//! Deterministic post-mint availability-generation race coverage.

use std::sync::Weak;

use super::*;

#[derive(Debug, Default)]
struct GenerationAdvancingStore {
    payload: Mutex<Option<Vec<u8>>>,
    allocation: Mutex<Option<Weak<BudgetAllocation>>>,
    advance_next_store: AtomicBool,
}

impl GenerationAdvancingStore {
    fn arm(&self, allocation: &Arc<BudgetAllocation>) -> TestResult {
        *self
            .allocation
            .lock()
            .map_err(|_| "generation callback lock poisoned")? = Some(Arc::downgrade(allocation));
        self.advance_next_store.store(true, Ordering::Release);
        Ok(())
    }
}

impl AuthorityStateStore for GenerationAdvancingStore {
    fn load(&self) -> Result<Option<Vec<u8>>, AuthorityStateStoreError> {
        self.payload
            .lock()
            .map(|payload| payload.clone())
            .map_err(|_| AuthorityStateStoreError::Unavailable)
    }

    fn store(&self, payload: &[u8]) -> Result<(), AuthorityStateStoreError> {
        self.payload
            .lock()
            .map_err(|_| AuthorityStateStoreError::Unavailable)?
            .replace(payload.to_vec());
        if self.advance_next_store.swap(false, Ordering::AcqRel) {
            let allocation = self
                .allocation
                .lock()
                .map_err(|_| AuthorityStateStoreError::Unavailable)?
                .as_ref()
                .and_then(Weak::upgrade)
                .ok_or(AuthorityStateStoreError::Unavailable)?;
            allocation
                .availability_generation
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                    generation.checked_add(1)
                })
                .map_err(|_| AuthorityStateStoreError::Unavailable)?;
        }
        Ok(())
    }
}

#[test]
fn legitimate_post_mint_generation_change_returns_precise_nonterminal_reason() -> TestResult {
    let store = Arc::new(GenerationAdvancingStore::default());
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
    store.arm(&budget.allocation)?;

    assert!(matches!(
        budget.availability_lease(),
        Err(BudgetUnavailableReason::AvailabilityChanged)
    ));
    assert!(session.is_available());
    assert!(!budget.allocation.terminal.load(Ordering::Acquire));
    assert!(budget.availability_lease().is_ok());
    Ok(())
}
