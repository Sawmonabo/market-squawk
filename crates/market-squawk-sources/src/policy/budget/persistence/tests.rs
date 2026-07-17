#[cfg(test)]
mod tests {
    use std::sync::Condvar;

    use market_squawk_domain::{
        AuthorizationBasis, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
        ExactPayloadEvidence,
    };

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    #[path = "terminalization.rs"]
    mod terminalization;

    #[derive(Debug, Default)]
    struct MemoryStore {
        payload: Mutex<Option<Vec<u8>>>,
        reject_stores: AtomicBool,
    }

    impl MemoryStore {
        fn payload(&self) -> TestResult<Vec<u8>> {
            self.payload
                .lock()
                .map_err(|_| "memory store lock poisoned".into())
                .and_then(|payload| payload.clone().ok_or_else(|| "payload missing".into()))
        }

        fn replace(&self, payload: Vec<u8>) -> TestResult {
            *self
                .payload
                .lock()
                .map_err(|_| "memory store lock poisoned")? = Some(payload);
            Ok(())
        }
    }

    impl AuthorityStateStore for MemoryStore {
        fn load(&self) -> Result<Option<Vec<u8>>, AuthorityStateStoreError> {
            self.payload
                .lock()
                .map(|payload| payload.clone())
                .map_err(|_| AuthorityStateStoreError::Unavailable)
        }

        fn store(&self, payload: &[u8]) -> Result<(), AuthorityStateStoreError> {
            if self.reject_stores.load(Ordering::Acquire) {
                return Err(AuthorityStateStoreError::Unavailable);
            }
            self.payload
                .lock()
                .map_err(|_| AuthorityStateStoreError::Unavailable)?
                .replace(payload.to_vec());
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct BlockingStore {
        payload: Mutex<Option<Vec<u8>>>,
        block_next: AtomicBool,
        entered: (Mutex<bool>, Condvar),
        released: (Mutex<bool>, Condvar),
    }

    impl BlockingStore {
        fn block_next_store(&self) {
            self.block_next.store(true, Ordering::Release);
        }

        fn wait_until_blocked(&self) -> TestResult {
            let (entered, signal) = &self.entered;
            let mut entered = entered.lock().map_err(|_| "entered lock poisoned")?;
            while !*entered {
                entered = signal
                    .wait(entered)
                    .map_err(|_| "entered wait poisoned")?;
            }
            Ok(())
        }

        fn release_store(&self) -> TestResult {
            let (released, signal) = &self.released;
            *released.lock().map_err(|_| "release lock poisoned")? = true;
            signal.notify_all();
            Ok(())
        }
    }

    impl AuthorityStateStore for BlockingStore {
        fn load(&self) -> Result<Option<Vec<u8>>, AuthorityStateStoreError> {
            self.payload
                .lock()
                .map(|payload| payload.clone())
                .map_err(|_| AuthorityStateStoreError::Unavailable)
        }

        fn store(&self, payload: &[u8]) -> Result<(), AuthorityStateStoreError> {
            if self.block_next.swap(false, Ordering::AcqRel) {
                let (entered, entered_signal) = &self.entered;
                *entered
                    .lock()
                    .map_err(|_| AuthorityStateStoreError::Unavailable)? = true;
                entered_signal.notify_all();

                let (released, release_signal) = &self.released;
                let mut released = released
                    .lock()
                    .map_err(|_| AuthorityStateStoreError::Unavailable)?;
                while !*released {
                    released = release_signal
                        .wait(released)
                        .map_err(|_| AuthorityStateStoreError::Unavailable)?;
                }
            }
            self.payload
                .lock()
                .map_err(|_| AuthorityStateStoreError::Unavailable)?
                .replace(payload.to_vec());
            Ok(())
        }
    }

    fn declaration(index: u8) -> TestResult<PersistedProviderBudgetPolicy> {
        let provider = SourceIdentifier::try_from(format!("provider-{index}"))?;
        let policy = ProviderBudgetPolicy::try_new(
            BudgetScope::new(provider),
            NonZeroU32::new(10).ok_or("request limit must be nonzero")?,
            NonZeroU64::new(60_000_000_000).ok_or("window must be nonzero")?,
            NonZeroU16::new(1).ok_or("concurrency must be nonzero")?,
            BackoffPolicy::try_new(
                NonZeroU64::new(1_000_000).ok_or("backoff must be nonzero")?,
                NonZeroU64::new(60_000_000_000).ok_or("backoff cap must be nonzero")?,
                0,
            )?,
        )?;
        let authorization = crate::AuthorizationGrant::new(
            crate::AuthorizationMode::PublicInterface,
            AuthorizationBasis::new(SourceIdentifier::try_from("public-terms")?),
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                [index; 32],
            )),
            EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        );
        Ok(PersistedProviderBudgetPolicy::try_new(
            policy,
            EndpointPolicy::try_new([format!("https://provider-{index}.example.test")])?,
            authorization,
            None,
        )?)
    }

    fn checkpoint(index: u8) -> BudgetCheckpointState {
        BudgetCheckpointState {
            window_started_wall: Timestamp::from_unix_nanos(i64::from(index)),
            window_ends_wall: Timestamp::from_unix_nanos(
                60_000_000_000 + i64::from(index),
            ),
            requests_used: u32::from(index),
            in_flight: 0,
            unavailable_until_wall: None,
            disabled: false,
            consecutive_refusals: 0,
            availability_generation: 1,
            terminal: false,
            poisoned: false,
        }
    }

    #[test]
    fn canonical_serialization_is_identical_across_one_hundred_group_permutations() -> TestResult {
        let mut groups = Vec::new();
        for index in 1_u8..=6 {
            groups.push(DurableBudgetGroup::try_new(
                declaration(index)?,
                checkpoint(index),
            )?);
        }
        let base = DurableAuthorityEnvelope {
            format_version: DURABLE_AUTHORITY_FORMAT_VERSION,
            run_generation: 7,
            run_state: DurableRunState::InUse,
            saved_at_wall: Timestamp::from_unix_nanos(100),
            wall_high_water: Timestamp::from_unix_nanos(100),
            registry: crate::RegistryAuthorityState::empty(),
            budgets: BoundedVec::try_new(groups.clone())?,
        };
        let expected = serialize_canonical_envelope(&base)?;
        let mut seed = 0x6a09_e667_f3bc_c909_u64;
        for _ in 0..100 {
            let mut permuted = groups.clone();
            for index in (1..permuted.len()).rev() {
                seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let bound = u64::try_from(index.checked_add(1).ok_or("shuffle overflow")?)?;
                let selected = usize::try_from(seed % bound)?;
                permuted.swap(index, selected);
            }
            let candidate = DurableAuthorityEnvelope {
                budgets: BoundedVec::try_new(permuted)?,
                ..base.clone()
            };
            assert_eq!(serialize_canonical_envelope(&candidate)?, expected);
        }
        Ok(())
    }

    #[test]
    fn load_rejects_truncation_noncanonical_bytes_and_temporal_ambiguity() -> TestResult {
        let store = Arc::new(MemoryStore::default());
        store.replace(b"{\"format_version\":".to_vec())?;
        assert!(matches!(
            AuthorityDurabilitySession::open(store.clone(), Timestamp::from_unix_nanos(100)),
            Err(AuthorityPersistenceError::InvalidState)
        ));

        let envelope = DurableAuthorityEnvelope {
            run_generation: 1,
            ..DurableAuthorityEnvelope::empty(Timestamp::from_unix_nanos(100))
        };
        store.replace(serde_json::to_vec_pretty(&envelope)?)?;
        assert!(matches!(
            AuthorityDurabilitySession::open(store.clone(), Timestamp::from_unix_nanos(100)),
            Err(AuthorityPersistenceError::InvalidState)
        ));

        store.replace(serialize_canonical_envelope(&envelope)?)?;
        assert!(matches!(
            AuthorityDurabilitySession::open(store.clone(), Timestamp::from_unix_nanos(99)),
            Err(AuthorityPersistenceError::WallRollback)
        ));

        let future_checkpoint = BudgetCheckpointState {
            window_started_wall: Timestamp::from_unix_nanos(151),
            window_ends_wall: Timestamp::from_unix_nanos(60_000_000_151),
            ..checkpoint(1)
        };
        let future = DurableAuthorityEnvelope {
            budgets: BoundedVec::singleton(DurableBudgetGroup::try_new(
                declaration(1)?,
                future_checkpoint,
            )?),
            ..envelope.clone()
        };
        store.replace(serialize_canonical_envelope(&future)?)?;
        assert!(matches!(
            AuthorityDurabilitySession::open(store, Timestamp::from_unix_nanos(150)),
            Err(AuthorityPersistenceError::FutureState)
        ));
        Ok(())
    }

    #[test]
    fn run_generation_exhaustion_and_store_failure_fail_before_session_publication() -> TestResult {
        let exhausted = DurableAuthorityEnvelope {
            run_generation: u64::MAX,
            ..DurableAuthorityEnvelope::empty(Timestamp::from_unix_nanos(100))
        };
        let store = Arc::new(MemoryStore::default());
        store.replace(serialize_canonical_envelope(&exhausted)?)?;
        assert!(matches!(
            AuthorityDurabilitySession::open(store, Timestamp::from_unix_nanos(100)),
            Err(AuthorityPersistenceError::GenerationExhausted)
        ));

        let failing = Arc::new(MemoryStore::default());
        failing.reject_stores.store(true, Ordering::Release);
        assert!(matches!(
            AuthorityDurabilitySession::open(failing.clone(), Timestamp::from_unix_nanos(100)),
            Err(AuthorityPersistenceError::Store)
        ));
        assert!(failing.load()?.is_none());
        Ok(())
    }

    #[test]
    fn clean_close_is_rejected_after_an_unclean_predecessor() -> TestResult {
        let mut envelope = DurableAuthorityEnvelope::empty(Timestamp::from_unix_nanos(100));
        envelope.run_generation = 1;
        envelope.run_state = DurableRunState::InUse;
        let store = Arc::new(MemoryStore::default());
        store.replace(serialize_canonical_envelope(&envelope)?)?;
        let session = AuthorityDurabilitySession::open(
            store,
            Timestamp::from_unix_nanos(100),
        )?;
        assert!(session.recovered_unclean());
        assert!(matches!(
            session.close_clean(
                crate::RegistryAuthorityState::empty(),
                Timestamp::from_unix_nanos(100)
            ),
            Err(AuthorityPersistenceError::SessionUnavailable)
        ));
        Ok(())
    }

    #[test]
    fn stale_concurrent_observation_preserves_deadline_distance_from_wall_high_water()
    -> TestResult {
        let store = Arc::new(MemoryStore::default());
        let session = AuthorityDurabilitySession::open(
            store,
            Timestamp::from_unix_nanos(100),
        )?;
        let slot = session.register_budget_group(
            crate::RegistryAuthorityState::empty(),
            declaration(1)?,
            checkpoint(1),
            Timestamp::from_unix_nanos(100),
        )?;
        let stale = BudgetCheckpointState {
            window_started_wall: Timestamp::from_unix_nanos(90),
            window_ends_wall: Timestamp::from_unix_nanos(60_000_000_090),
            requests_used: 2,
            in_flight: 0,
            unavailable_until_wall: Some(Timestamp::from_unix_nanos(190)),
            disabled: false,
            consecutive_refusals: 1,
            availability_generation: 2,
            terminal: false,
            poisoned: false,
        };
        session.update_budget(slot, stale, Timestamp::from_unix_nanos(90))?;
        let restored = session.budget_groups()?;
        let anchored = restored
            .get(slot)
            .ok_or("anchored budget group missing")?
            .checkpoint();
        assert_eq!(anchored.window_started_wall, Timestamp::from_unix_nanos(100));
        assert_eq!(
            anchored.window_ends_wall,
            Timestamp::from_unix_nanos(60_000_000_100)
        );
        assert_eq!(
            anchored.unavailable_until_wall,
            Some(Timestamp::from_unix_nanos(200))
        );
        assert!(session.is_available());
        Ok(())
    }

    #[test]
    fn clean_close_serializes_against_and_rejects_a_waiting_budget_update() -> TestResult {
        let store = Arc::new(BlockingStore::default());
        let session = AuthorityDurabilitySession::open(
            store.clone(),
            Timestamp::from_unix_nanos(100),
        )?;
        let slot = session.register_budget_group(
            crate::RegistryAuthorityState::empty(),
            declaration(1)?,
            checkpoint(1),
            Timestamp::from_unix_nanos(100),
        )?;
        store.block_next_store();

        let closing = session.clone();
        let close = std::thread::spawn(move || {
            closing.close_clean(
                crate::RegistryAuthorityState::empty(),
                Timestamp::from_unix_nanos(100),
            )
        });
        store.wait_until_blocked()?;

        let updating = session.clone();
        let update = std::thread::spawn(move || {
            updating.update_budget(
                slot,
                checkpoint(2),
                Timestamp::from_unix_nanos(100),
            )
        });
        store.release_store()?;

        assert_eq!(close.join().map_err(|_| "close thread panicked")?, Ok(()));
        assert_eq!(
            update.join().map_err(|_| "update thread panicked")?,
            Err(AuthorityPersistenceError::SessionUnavailable)
        );
        assert!(!session.is_available());
        assert!(session
            .store
            .lock()
            .map_err(|_| "session store lock poisoned")?
            .is_none());
        Ok(())
    }

    #[test]
    fn memory_store_test_fixture_exposes_last_payload() -> TestResult {
        let store = Arc::new(MemoryStore::default());
        let _session = AuthorityDurabilitySession::open(
            store.clone(),
            Timestamp::from_unix_nanos(100),
        )?;
        assert!(!store.payload()?.is_empty());
        Ok(())
    }
}
