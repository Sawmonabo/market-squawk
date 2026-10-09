#[cfg(test)]
mod coordinator_tests {
    use std::error::Error;
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
    use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
    use std::time::Duration;

    use market_squawk_domain::{
        AuthorizationBasis, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
        ExactPayloadEvidence,
    };

    use super::*;
    use crate::policy::persistence::AuthorityStateStoreError;

    type TestResult<T = ()> = Result<T, Box<dyn Error>>;

    #[derive(Debug, Default)]
    struct NoopAuthorityStore {
        writes: AtomicUsize,
    }

    impl AuthorityStateStore for NoopAuthorityStore {
        fn load(&self) -> Result<Option<Vec<u8>>, AuthorityStateStoreError> {
            Ok(None)
        }

        fn store(&self, _payload: &[u8]) -> Result<(), AuthorityStateStoreError> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct FailFirstRegistrationStore {
        writes: AtomicUsize,
    }

    impl AuthorityStateStore for FailFirstRegistrationStore {
        fn load(&self) -> Result<Option<Vec<u8>>, AuthorityStateStoreError> {
            Ok(None)
        }

        fn store(&self, _payload: &[u8]) -> Result<(), AuthorityStateStoreError> {
            let write = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
            if write == 2 {
                Err(AuthorityStateStoreError::Unavailable)
            } else {
                Ok(())
            }
        }
    }

    #[derive(Debug, Default)]
    struct TrackingProviderRateStore {
        commits: Arc<AtomicUsize>,
        rollbacks: Arc<AtomicUsize>,
        admission_enabled: bool,
        reserve_calls: AtomicUsize,
        active_requests: AtomicUsize,
        maximum_concurrent: usize,
        dispatch_calls: AtomicUsize,
        availability_calls: AtomicUsize,
        retained_missing: AtomicBool,
        disabled: AtomicBool,
        dispatch_wait: AtomicBool,
    }

    #[derive(Debug)]
    struct TrackingPreparedRegistrationBatch {
        registrations: Box<[ProviderRateRegistration]>,
        commits: Arc<AtomicUsize>,
        rollbacks: Arc<AtomicUsize>,
        finalized: bool,
    }

    impl PreparedProviderRateRegistrationBatch for TrackingPreparedRegistrationBatch {
        fn registrations(&self) -> &[ProviderRateRegistration] {
            &self.registrations
        }

        fn commit(mut self: Box<Self>) -> Result<(), ProviderRateStoreError> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            self.finalized = true;
            Ok(())
        }
    }

    impl Drop for TrackingPreparedRegistrationBatch {
        fn drop(&mut self) {
            if !self.finalized {
                self.rollbacks.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    impl ProviderRateStore for TrackingProviderRateStore {
        fn start_run(&self, _now: Timestamp) -> Result<ProviderRateRunId, ProviderRateStoreError> {
            Ok(ProviderRateRunId::from_bytes([1; 16]))
        }

        fn prepare_registration_batch(
            &self,
            _run_id: ProviderRateRunId,
            declarations: &[ProviderRateDeclaration],
            _now: Timestamp,
        ) -> Result<Box<dyn PreparedProviderRateRegistrationBatch>, ProviderRateStoreError>
        {
            let registrations = declarations
                .iter()
                .map(|declaration| {
                    ProviderRateRegistration::new(
                        ProviderRateGroupId::from_bytes([2; 16]),
                        declaration.policy_digest(),
                        declaration.declaration_digest(),
                    )
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            Ok(Box::new(TrackingPreparedRegistrationBatch {
                registrations,
                commits: Arc::clone(&self.commits),
                rollbacks: Arc::clone(&self.rollbacks),
                finalized: false,
            }))
        }

        fn try_reserve(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _now: Timestamp,
        ) -> Result<ProviderRateReservationDecision, ProviderRateStoreError> {
            self.reserve_calls.fetch_add(1, Ordering::SeqCst);
            if !self.admission_enabled {
                return Err(ProviderRateStoreError::Unavailable);
            }
            if self.disabled.load(Ordering::SeqCst) {
                return Ok(ProviderRateReservationDecision::Unavailable(
                    BudgetUnavailableReason::Disabled,
                ));
            }
            if self.active_requests.load(Ordering::SeqCst) >= self.maximum_concurrent.max(1) {
                return Ok(ProviderRateReservationDecision::Unavailable(
                    BudgetUnavailableReason::ConcurrencyExhausted,
                ));
            }
            let previous = self.active_requests.fetch_add(1, Ordering::SeqCst);
            Ok(ProviderRateReservationDecision::Ready(
                ProviderRateReservationId::from_bytes(
                    [u8::try_from(previous + 3).map_err(|_| ProviderRateStoreError::Corrupt)?; 16],
                ),
            ))
        }

        fn inspect_availability(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _now: Timestamp,
        ) -> Result<ProviderRateAvailability, ProviderRateStoreError> {
            self.availability_calls.fetch_add(1, Ordering::SeqCst);
            Ok(if self.disabled.load(Ordering::SeqCst) {
                ProviderRateAvailability::Unavailable(BudgetUnavailableReason::Disabled)
            } else if self.active_requests.load(Ordering::SeqCst) >= self.maximum_concurrent.max(1)
            {
                ProviderRateAvailability::Unavailable(BudgetUnavailableReason::ConcurrencyExhausted)
            } else {
                ProviderRateAvailability::Available
            })
        }

        fn validate_retained_budget(
            &self,
            _run_id: ProviderRateRunId,
            _declaration: &ProviderRateDeclaration,
            _now: Timestamp,
        ) -> Result<(), ProviderRateStoreError> {
            if self.retained_missing.load(Ordering::SeqCst) {
                Err(ProviderRateStoreError::Corrupt)
            } else {
                Ok(())
            }
        }

        fn commit_dispatch(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _reservation_id: ProviderRateReservationId,
            now: Timestamp,
        ) -> Result<ProviderRateDispatchDecision, ProviderRateStoreError> {
            self.dispatch_calls.fetch_add(1, Ordering::SeqCst);
            if !self.admission_enabled {
                return Err(ProviderRateStoreError::Unavailable);
            }
            if self.dispatch_wait.load(Ordering::SeqCst) {
                self.active_requests.fetch_sub(1, Ordering::SeqCst);
                return Ok(ProviderRateDispatchDecision::WaitUntil(
                    now.checked_add_nanos(1_000_000_000)
                        .map_err(|_| ProviderRateStoreError::Clock)?,
                ));
            }
            Ok(ProviderRateDispatchDecision::Ready(
                ProviderRatePermitId::from_bytes([4; 16]),
            ))
        }

        fn cancel_reservation(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _reservation_id: ProviderRateReservationId,
        ) -> Result<(), ProviderRateStoreError> {
            self.active_requests.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }

        fn release(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _permit_id: ProviderRatePermitId,
        ) -> Result<(), ProviderRateStoreError> {
            self.active_requests.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }

        fn apply_retry_after(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _now: Timestamp,
            _retry_after: RetryAfter,
        ) -> Result<ProviderRateReservationDecision, ProviderRateStoreError> {
            if !self.admission_enabled {
                return Err(ProviderRateStoreError::Unavailable);
            }
            self.disabled.store(true, Ordering::SeqCst);
            Ok(ProviderRateReservationDecision::Unavailable(
                BudgetUnavailableReason::Disabled,
            ))
        }

        fn apply_refusal(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _now: Timestamp,
            _jitter_sample_basis_points: u16,
        ) -> Result<ProviderRateReservationDecision, ProviderRateStoreError> {
            Err(ProviderRateStoreError::Unavailable)
        }

        fn record_success(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _now: Timestamp,
        ) -> Result<(), ProviderRateStoreError> {
            Ok(())
        }

        fn bind_authorization_subject(
            &self,
            _run_id: ProviderRateRunId,
            _mode: crate::AuthorizationMode,
            _evidence: EvidenceDigest,
            _subject: &SourceIdentifier,
            _now: Timestamp,
        ) -> Result<(), ProviderRateStoreError> {
            Err(ProviderRateStoreError::Unavailable)
        }

        fn resolve_authorization_subject(
            &self,
            _mode: crate::AuthorizationMode,
            _evidence: EvidenceDigest,
        ) -> Result<Option<SourceIdentifier>, ProviderRateStoreError> {
            Ok(None)
        }
    }

    fn test_policy(scope: &str, requests_per_window: u32) -> TestResult<ProviderBudgetPolicy> {
        Ok(ProviderBudgetPolicy::try_new(
            BudgetScope::new(SourceIdentifier::try_from(scope)?),
            NonZeroU32::new(requests_per_window).ok_or("request limit must be nonzero")?,
            NonZeroU64::new(60_000_000_000).ok_or("window must be nonzero")?,
            NonZeroU16::new(1).ok_or("concurrency must be nonzero")?,
            BackoffPolicy::try_new(
                NonZeroU64::new(1_000_000).ok_or("backoff must be nonzero")?,
                NonZeroU64::new(60_000_000_000).ok_or("backoff cap must be nonzero")?,
                0,
            )?,
        )?)
    }

    #[derive(Debug)]
    struct NoAccountSubjects;

    impl crate::AuthorizationSubjectResolver for NoAccountSubjects {
        fn resolve_subject_record(
            &self,
            _mode: crate::AuthorizationMode,
            _evidence: EvidenceDigest,
        ) -> Result<SourceIdentifier, crate::AuthorizationSubjectResolutionError> {
            Err(crate::AuthorizationSubjectResolutionError::UnsupportedMode)
        }
    }

    fn resolved_policy(
        scope: &str,
        requests_per_window: u32,
    ) -> TestResult<ResolvedProviderBudgetPolicy> {
        resolved_policy_with_hosts(scope, requests_per_window, &[scope])
    }

    fn resolved_policy_with_hosts(
        scope: &str,
        requests_per_window: u32,
        hosts: &[&str],
    ) -> TestResult<ResolvedProviderBudgetPolicy> {
        let policy = test_policy(scope, requests_per_window)?;
        let authorization = crate::AuthorizationGrant::new(
            crate::AuthorizationMode::PublicInterface,
            AuthorizationBasis::new(SourceIdentifier::try_from("public-interface-terms")?),
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                [1; 32],
            )),
            EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        );
        Ok(ResolvedProviderBudgetPolicy::try_new(
            policy,
            EndpointPolicy::try_new(
                hosts
                    .iter()
                    .map(|host| format!("https://{host}.example.test/path")),
            )?,
            authorization,
            &NoAccountSubjects,
        )?)
    }

    fn register_fresh(policy: ResolvedProviderBudgetPolicy) -> TestResult<SharedProviderBudget> {
        let mut pool = ProviderBudgetPool::new()?;
        Ok(pool.register(policy)?)
    }

    #[tokio::test]
    async fn shared_request_admission_waits_without_polling_and_releases_cancelled_turns()
    -> TestResult {
        struct Adapter(crate::SourceMetadata);
        impl crate::SourceMetadataProvider for Adapter {
            fn metadata(&self) -> &crate::SourceMetadata {
                &self.0
            }
        }

        let store = Arc::new(TrackingProviderRateStore {
            admission_enabled: true,
            maximum_concurrent: 2,
            ..TrackingProviderRateStore::default()
        });
        let rate = ProviderRateAuthority::try_new(store.clone())?;
        let publisher = crate::FASB_XBRL_TAXONOMY_AUTHORITY;
        let mut policy_wire = serde_json::to_value(test_policy(publisher.rate_scope(), 2)?)?;
        policy_wire["max_concurrent"] = serde_json::json!(2);
        let policy: ProviderBudgetPolicy = serde_json::from_value(policy_wire)?;
        let mut metadata = serde_json::to_value(publisher.dependency_source_metadata()?)?;
        metadata["budget"] = serde_json::to_value(&policy)?;
        let adapter = Adapter(serde_json::from_value(metadata)?);
        let mut registry =
            crate::AuthoritativeSourceRegistry::try_new_in_memory_for_bounded_extraction(
                Arc::new(NoAccountSubjects),
                rate.clone(),
            )?;
        let at = SystemBudgetClock::new()
            .observation()
            .map_err(|reason| format!("clock: {reason:?}"))?
            .wall_clock;
        let registered = registry.register(adapter.0.clone(), at)?;
        let authority = registry.extraction_authority(&registered, &adapter)?;
        let target = "https://xbrl.fasb.org/test.xsd";

        // Separate handles for the same durable group share waiting state even when their local
        // allocations differ. This is the source-registry/onboarding composition boundary.
        let declaration =
            ProviderRateDeclaration::try_for_endpoint(policy, &publisher.endpoint_policy()?)?;
        let first_budget = rate.register_budget(declaration.clone())?;
        let second_budget = rate.register_budget(declaration)?;
        assert!(!first_budget.shares_allocation_with(&second_budget));
        assert!(Arc::ptr_eq(
            &first_budget.request_admission(),
            &second_budget.request_admission(),
        ));

        let mut transport = match second_budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("transport dispatch: {other:?}").into()),
        };
        transport
            .complete_transport_handshake()
            .map_err(|reason| format!("transport: {reason:?}"))?;
        let transport_lease = transport.active_lease();
        let dispatches_before = store.dispatch_calls.load(Ordering::SeqCst);
        let prior_lease = second_budget
            .availability_lease()
            .map_err(|reason| format!("availability: {reason:?}"))?;
        let independent = match first_budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("independent dispatch: {other:?}").into()),
        };
        let held = authority.acquire_network_request(target).await?;
        assert_eq!(store.active_requests.load(Ordering::SeqCst), 2);
        assert_eq!(
            store.dispatch_calls.load(Ordering::SeqCst),
            dispatches_before + 2
        );
        assert!(!prior_lease.is_available());
        let inspected = store.availability_calls.load(Ordering::SeqCst);
        assert!(!prior_lease.is_available());
        assert_eq!(store.availability_calls.load(Ordering::SeqCst), inspected);
        let mut first = Box::pin(authority.acquire_network_request(target));
        let mut second = Box::pin(authority.acquire_network_request(target));
        assert!(futures_util::poll!(&mut first).is_pending());
        let after_first = store.reserve_calls.load(Ordering::SeqCst);
        assert!(futures_util::poll!(&mut second).is_pending());
        // Longer than the removed SEC 25 ms recheck: neither waiter may call the store again.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(futures_util::poll!(&mut first).is_pending());
        assert!(futures_util::poll!(&mut second).is_pending());
        assert_eq!(store.reserve_calls.load(Ordering::SeqCst), after_first);

        // Cancelling the head transfers admission ownership without freeing the active request.
        drop(first);
        assert!(futures_util::poll!(&mut second).is_pending());
        assert_eq!(store.reserve_calls.load(Ordering::SeqCst), after_first + 1);
        held.release();
        let second = tokio::time::timeout(Duration::from_secs(1), second).await??;

        // New work cannot overtake an already queued caller, even if it is polled first on wake.
        let mut earlier = Box::pin(authority.acquire_network_request(target));
        let mut later = Box::pin(authority.acquire_network_request(target));
        assert!(futures_util::poll!(&mut earlier).is_pending());
        assert!(futures_util::poll!(&mut later).is_pending());
        second.release();
        assert!(futures_util::poll!(&mut later).is_pending());
        let earlier = tokio::time::timeout(Duration::from_secs(1), earlier).await??;
        assert!(futures_util::poll!(&mut later).is_pending());
        earlier.release();
        tokio::time::timeout(Duration::from_secs(1), later)
            .await??
            .release();

        // A release after arming but before polling the notification is not lost. Reservation
        // cancellation must signal too, although it has never charged a provider request.
        let admission = first_budget.request_admission();
        let reserved = authority.try_network_request(target)?;
        let changed = admission.changed();
        tokio::pin!(changed);
        changed.as_mut().enable();
        reserved.release();
        tokio::time::timeout(Duration::from_secs(1), changed).await?;

        // Durable reservation cleanup also wakes another binding when local admission failed
        // before an outer BudgetReservation could own it.
        let binding = first_budget
            .allocation
            .provider_rate
            .clone()
            .ok_or("missing binding")?;
        let (_, decision) = binding
            .try_reserve_decision()
            .map_err(|reason| format!("reservation: {reason:?}"))?;
        let ProviderRateReservationDecision::Ready(reservation_id) = decision else {
            return Err("expected durable reservation".into());
        };
        let reservation =
            crate::policy::provider_rate::ProviderRateReservation::new(binding, reservation_id);
        let changed = admission.changed();
        tokio::pin!(changed);
        changed.as_mut().enable();
        drop(reservation);
        tokio::time::timeout(Duration::from_secs(1), changed).await?;

        // Weighted/claim-dependent windows may reject dispatch even when reservation was ready.
        // Its own reservation release cannot repeatedly wake the unchanged rate deadline.
        store.dispatch_wait.store(true, Ordering::SeqCst);
        let mut delayed = Box::pin(authority.acquire_network_request(target));
        assert!(futures_util::poll!(&mut delayed).is_pending());
        let after_dispatch_wait = store.reserve_calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(futures_util::poll!(&mut delayed).is_pending());
        assert_eq!(
            store.reserve_calls.load(Ordering::SeqCst),
            after_dispatch_wait
        );
        store.dispatch_wait.store(false, Ordering::SeqCst);
        // A response/cooldown change after arming must still wake immediately, before the old
        // one-second deadline. This is the same signal fired after those durable transitions.
        drop(admission.notify_on_drop());
        tokio::time::timeout(Duration::from_millis(100), delayed)
            .await??
            .release();

        // Revocation wakes both the capacity waiter and its FIFO successor while capacity is
        // still held; neither needs a release or timer recheck to discover lost authority.
        let held = authority.acquire_network_request(target).await?;
        let mut head = Box::pin(authority.acquire_network_request(target));
        let mut queued = Box::pin(authority.acquire_network_request(target));
        assert!(futures_util::poll!(&mut head).is_pending());
        assert!(futures_util::poll!(&mut queued).is_pending());
        registry.revoke(&registered, at)?;
        for waiting in [head, queued] {
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(1), waiting).await?,
                Err(crate::ExtractionAuthorityError::NotCurrent)
            ));
        }
        held.release();

        // Revocation is permanent within a registry; use a new owner for the terminal case.
        let mut registry =
            crate::AuthoritativeSourceRegistry::try_new_in_memory_for_bounded_extraction(
                Arc::new(NoAccountSubjects),
                rate.clone(),
            )?;
        let registered = registry.register(adapter.0.clone(), at)?;
        let authority = registry.extraction_authority(&registered, &adapter)?;

        // An independent handle can make the durable group terminal while a request still owns
        // capacity; the blocked caller must wake and fail without waiting for that request's Drop.
        let held = authority.acquire_network_request(target).await?;
        let mut waiting = Box::pin(authority.acquire_network_request(target));
        assert!(futures_util::poll!(&mut waiting).is_pending());
        assert!(transport_lease.is_current());
        assert!(matches!(
            second_budget.apply_retry_after(RetryAfter::Delay(NonZeroU64::MIN)),
            BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled)
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), waiting).await?,
            Err(crate::ExtractionAuthorityError::BudgetUnavailable {
                reason: BudgetUnavailableReason::Disabled,
            })
        ));
        held.release();
        assert!(!transport_lease.is_current());
        drop(transport);
        independent.release();
        assert_eq!(store.active_requests.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn system_clock_pairs_wall_before_monotonic_for_conservative_deadlines() -> TestResult {
        let origin = Instant::now();
        let order = AtomicU8::new(0);
        let observation = observe_system_clocks(
            origin,
            || {
                assert_eq!(order.swap(1, Ordering::SeqCst), 0);
                UNIX_EPOCH + Duration::from_nanos(1_000)
            },
            || {
                assert_eq!(order.swap(2, Ordering::SeqCst), 1);
                origin + Duration::from_nanos(250)
            },
        )
        .map_err(|reason| format!("clock observation failed: {reason:?}"))?;

        assert_eq!(order.load(Ordering::SeqCst), 2);
        assert_eq!(observation.wall_clock.unix_nanos(), 1_000);
        assert_eq!(observation.monotonic, MonotonicInstant::from_nanos(250));
        Ok(())
    }

    #[test]
    fn local_registration_failure_rolls_back_the_prepared_provider_batch() -> TestResult {
        let clock = SystemBudgetClock::new();
        let observation = clock
            .observation()
            .map_err(|reason| format!("test clock unavailable: {reason:?}"))?;
        let local_store = Arc::new(FailFirstRegistrationStore::default());
        let session = AuthorityDurabilitySession::open(local_store, observation.wall_clock)?;
        let provider_store = Arc::new(TrackingProviderRateStore::default());
        let commits = Arc::clone(&provider_store.commits);
        let rollbacks = Arc::clone(&provider_store.rollbacks);
        let provider_rate = ProviderRateAuthority::try_new(provider_store)?;
        let policies = [
            resolved_policy("atomic-local-first", 2)?,
            resolved_policy("atomic-local-second", 2)?,
        ];
        let mut coordinator = ProcessBudgetCoordinator::new(4);

        assert!(matches!(
            coordinator.coordinate_with_provider_rate(
                &policies,
                Some(DurableRegistration {
                    session: &session,
                    registry: &crate::RegistryAuthorityState::empty(),
                }),
                Some(&provider_rate),
            ),
            Err(BudgetPoolError::Persistence)
        ));
        assert!(coordinator.allocations.is_empty());
        assert_eq!(commits.load(Ordering::SeqCst), 0);
        assert_eq!(rollbacks.load(Ordering::SeqCst), 1);
        assert!(!session.is_available());
        Ok(())
    }

    fn durable_pool(
        prefix: &str,
        count: u8,
    ) -> TestResult<(
        Arc<AuthorityDurabilitySession>,
        ProviderBudgetPool,
        ClockObservation,
    )> {
        let clock = SystemBudgetClock::new();
        let observation = clock
            .observation()
            .map_err(|reason| format!("test clock unavailable: {reason:?}"))?;
        let store: Arc<dyn AuthorityStateStore> = Arc::new(NoopAuthorityStore::default());
        let session = AuthorityDurabilitySession::open(store, observation.wall_clock)?;
        let mut pool = ProviderBudgetPool::new_durable(session.clone());
        for index in 0..count {
            pool.register_durable(
                resolved_policy(&format!("{prefix}-clean-proof-{index}"), 2)?,
                &crate::RegistryAuthorityState::empty(),
            )?;
        }
        Ok((session, pool, observation))
    }

    #[test]
    fn account_qualified_policy_has_an_exact_shared_allocation_charge() -> TestResult {
        fn capacity_identifier(character: char) -> TestResult<SourceIdentifier> {
            let mut value = String::with_capacity(SourceIdentifier::MAX_LENGTH);
            value.push(character);
            Ok(SourceIdentifier::try_from(value)?)
        }

        let provider = capacity_identifier('p')?;
        let account = capacity_identifier('a')?;
        let expected_dynamic = provider
            .retained_bytes()
            .checked_add(account.retained_bytes())
            .ok_or("budget policy dynamic charge overflow")?;
        let policy = ProviderBudgetPolicy::try_new(
            BudgetScope::with_authorization_account(provider, account),
            NonZeroU32::new(1).ok_or("request limit must be nonzero")?,
            NonZeroU64::new(60_000_000_000).ok_or("window must be nonzero")?,
            NonZeroU16::new(1).ok_or("concurrency must be nonzero")?,
            BackoffPolicy::try_new(
                NonZeroU64::new(1_000_000).ok_or("backoff must be nonzero")?,
                NonZeroU64::new(60_000_000_000).ok_or("backoff cap must be nonzero")?,
                0,
            )?,
        )?;
        let clock = Arc::new(SystemBudgetClock::new());
        let starts_at = clock
            .observation()
            .map_err(|reason| std::io::Error::other(format!("clock unavailable: {reason:?}")))?
            .monotonic;
        let budget = SharedProviderBudget::new(policy, starts_at, clock.clone());
        let expected_window_storage = {
            let state = budget
                .allocation
                .state
                .lock()
                .map_err(|_| "budget state lock poisoned")?;
            assert_eq!(state.windows.len(), 1);
            let window = state
                .windows
                .first()
                .ok_or("numeric request window missing")?;
            assert_eq!(window.sliding_releases.capacity(), 0);
            state
                .windows
                .capacity()
                .checked_mul(std::mem::size_of_val(window))
                .ok_or("budget window storage charge overflow")?
        };
        let lease = budget.availability_lease().map_err(|reason| {
            std::io::Error::other(format!("budget lease unavailable: {reason:?}"))
        })?;
        let expected = std::mem::size_of::<BudgetAllocation>()
            .checked_add(crate::conservative_arc_control_block_charge::<
                BudgetAllocation,
            >())
            .and_then(|bytes| bytes.checked_add(expected_dynamic))
            .and_then(|bytes| bytes.checked_add(expected_window_storage))
            .and_then(|bytes| {
                bytes.checked_add(
                    std::mem::size_of::<crate::policy::provider_rate::admission::RequestAdmission>(
                    )
                    .checked_add(
                        crate::conservative_arc_control_block_charge::<
                            crate::policy::provider_rate::admission::RequestAdmission,
                        >(),
                    )?,
                )
            })
            .and_then(|bytes| bytes.checked_add(clock.shared_allocation_charge()))
            .ok_or("shared budget allocation charge overflow")?;

        assert_eq!(lease.shared_allocation_charge(), Some(expected));
        Ok(())
    }

    #[test]
    fn dropping_every_external_handle_cannot_reset_request_state() -> TestResult {
        let policy = resolved_policy("drop-reset-request-state", 1)?;
        let budget = register_fresh(policy.clone())?;
        let permit = match budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("unexpected first acquire: {other:?}").into()),
        };
        permit.release();
        drop(budget);

        let restored = register_fresh(policy)?;
        assert!(matches!(
            restored.try_acquire(),
            BudgetDecision::WaitUntil(_)
        ));
        Ok(())
    }

    #[test]
    fn dropping_every_external_handle_preserves_refusal_disabled_and_terminal_state() -> TestResult
    {
        let refusal_policy = resolved_policy("drop-reset-refusal-state", 4)?;
        let refusal = register_fresh(refusal_policy.clone())?;
        let mut transport = match refusal.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("unexpected upgrade dispatch: {other:?}").into()),
        };
        let request_lease = transport.active_lease();
        assert!(request_lease.is_current());
        transport
            .complete_transport_handshake()
            .map_err(|reason| format!("upgrade completion failed: {reason:?}"))?;
        assert!(!request_lease.is_current());
        let transport_lease = transport.active_lease();
        let concurrent = match refusal.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("upgrade retained request slot: {other:?}").into()),
        };
        assert!(transport_lease.is_current());
        assert!(matches!(
            refusal.try_acquire(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::ConcurrencyExhausted)
        ));
        assert!(transport_lease.is_current());
        drop(concurrent);
        drop(transport);
        assert!(!transport_lease.is_current());
        assert!(!request_lease.is_current());

        let mut refused_transport = match refusal.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("unexpected upgrade dispatch: {other:?}").into()),
        };
        refused_transport
            .complete_transport_handshake()
            .map_err(|reason| format!("upgrade completion failed: {reason:?}"))?;
        let refused_lease = refused_transport.active_lease();
        assert!(refused_lease.is_current());
        let deadline = match refusal.apply_refusal(0) {
            BudgetDecision::WaitUntil(deadline) => deadline,
            other => return Err(format!("unexpected refusal decision: {other:?}").into()),
        };
        assert!(!refused_lease.is_current());
        drop(refused_transport);
        drop(refused_lease);
        drop(transport_lease);
        drop(request_lease);
        let refusal_allocation = Arc::downgrade(&refusal.allocation);
        drop(refusal);
        let refusal_restored = register_fresh(refusal_policy)?;
        assert!(refusal_allocation.ptr_eq(&Arc::downgrade(&refusal_restored.allocation)));
        let refusal_state = refusal_restored
            .allocation
            .state
            .lock()
            .map_err(|_| "refusal budget state poisoned")?;
        assert_eq!(refusal_state.unavailable_until, Some(deadline));
        assert_eq!(refusal_state.consecutive_refusals, 1);
        drop(refusal_state);

        let disabled_policy = resolved_policy("drop-reset-disabled-state", 2)?;
        let disabled = register_fresh(disabled_policy.clone())?;
        let mut disabled_transport = match disabled.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("unexpected upgrade dispatch: {other:?}").into()),
        };
        disabled_transport
            .complete_transport_handshake()
            .map_err(|reason| format!("upgrade completion failed: {reason:?}"))?;
        let disabled_lease = disabled_transport.active_lease();
        assert!(disabled_lease.is_current());
        assert!(matches!(
            disabled.disable(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled)
        ));
        assert!(!disabled_lease.is_current());
        drop(disabled_transport);
        drop(disabled_lease);
        drop(disabled);
        let disabled_restored = register_fresh(disabled_policy)?;
        assert!(matches!(
            disabled_restored.try_acquire(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled)
        ));

        let terminal_policy = resolved_policy("drop-reset-terminal-state", 2)?;
        let terminal = register_fresh(terminal_policy.clone())?;
        let mut terminal_transport = match terminal.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("unexpected upgrade dispatch: {other:?}").into()),
        };
        terminal_transport
            .complete_transport_handshake()
            .map_err(|reason| format!("upgrade completion failed: {reason:?}"))?;
        let terminal_lease = terminal_transport.active_lease();
        assert!(terminal_lease.is_current());
        terminal
            .allocation
            .availability_generation
            .store(u64::MAX, Ordering::Release);
        assert!(matches!(
            terminal.disable(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::AvailabilityGenerationExhausted)
        ));
        assert!(!terminal_lease.is_current());
        drop(terminal_transport);
        drop(terminal_lease);
        drop(terminal);
        let terminal_restored = register_fresh(terminal_policy)?;
        assert!(matches!(
            terminal_restored.try_acquire(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::AvailabilityGenerationExhausted)
        ));
        Ok(())
    }

    #[test]
    fn coordinator_capacity_and_conflict_fail_without_mutating_authoritative_state() -> TestResult {
        let first_policy = resolved_policy("bounded-coordinator-first", 1)?;
        let second_policy = resolved_policy("bounded-coordinator-second", 1)?;
        let mut coordinator = ProcessBudgetCoordinator::new(1);
        let first = coordinator.coordinate(std::slice::from_ref(&first_policy), None)?;
        let first_budget = first.first().ok_or("first coordinated budget missing")?;
        let permit = match first_budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("unexpected bounded acquire: {other:?}").into()),
        };
        permit.release();
        drop(first);
        let retained = Arc::clone(
            &coordinator
                .allocations
                .first()
                .ok_or("retained allocation missing")?
                .allocation,
        );

        assert!(matches!(
            coordinator.coordinate(std::slice::from_ref(&second_policy), None),
            Err(BudgetPoolError::CoordinatorCapacity)
        ));
        assert_eq!(coordinator.allocations.len(), 1);
        assert!(Arc::ptr_eq(
            &coordinator
                .allocations
                .first()
                .ok_or("first allocation removed after capacity failure")?
                .allocation,
            &retained,
        ));

        let conflicting = resolved_policy("bounded-coordinator-first", 2)?;
        assert!(matches!(
            coordinator.coordinate(std::slice::from_ref(&conflicting), None),
            Err(BudgetPoolError::ConflictingPolicy)
        ));
        assert_eq!(coordinator.allocations.len(), 1);
        let restored = coordinator.coordinate(std::slice::from_ref(&first_policy), None)?;
        assert!(matches!(
            restored
                .first()
                .ok_or("restored retained allocation missing")?
                .try_acquire(),
            BudgetDecision::WaitUntil(_)
        ));
        Ok(())
    }

    #[test]
    fn colliding_scopes_share_only_an_exact_window_conjunction() -> TestResult {
        fn resolved_with_windows(
            scope: &str,
            windows: &[ProviderBudgetWindow],
        ) -> TestResult<ResolvedProviderBudgetPolicy> {
            let policy = ProviderBudgetPolicy::try_new_conjunctive(
                BudgetScope::new(SourceIdentifier::try_from(scope)?),
                windows,
                NonZeroU16::new(1).ok_or("concurrency must be nonzero")?,
                BackoffPolicy::try_new(
                    NonZeroU64::new(1_000_000).ok_or("backoff must be nonzero")?,
                    NonZeroU64::new(60_000_000_000).ok_or("backoff cap must be nonzero")?,
                    0,
                )?,
            )?;
            let authorization = crate::AuthorizationGrant::new(
                crate::AuthorizationMode::PublicInterface,
                AuthorizationBasis::new(SourceIdentifier::try_from("public-interface-terms")?),
                ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    [1; 32],
                )),
                EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
            );
            Ok(ResolvedProviderBudgetPolicy::try_new(
                policy,
                EndpointPolicy::try_new(["https://conjunction.example.test/path"])?,
                authorization,
                &NoAccountSubjects,
            )?)
        }

        let base = [
            ProviderBudgetWindow::try_new(
                NonZeroU32::new(2).ok_or("request limit must be nonzero")?,
                NonZeroU64::new(1_000).ok_or("window must be nonzero")?,
                BudgetWindowSemantics::Sliding,
            )?,
            ProviderBudgetWindow::try_new(
                NonZeroU32::new(10).ok_or("request limit must be nonzero")?,
                NonZeroU64::new(10_000).ok_or("window must be nonzero")?,
                BudgetWindowSemantics::Tumbling,
            )?,
        ];
        let exact = resolved_with_windows("conjunction-exact", &base)?;
        let conflicting_windows = [
            base[0],
            ProviderBudgetWindow::try_new(
                NonZeroU32::new(9).ok_or("request limit must be nonzero")?,
                NonZeroU64::new(10_000).ok_or("window must be nonzero")?,
                BudgetWindowSemantics::Tumbling,
            )?,
        ];
        let conflicting = resolved_with_windows("conjunction-conflict", &conflicting_windows)?;
        let mut coordinator = ProcessBudgetCoordinator::new(2);
        let first = coordinator.coordinate(std::slice::from_ref(&exact), None)?;
        let repeated = coordinator.coordinate(std::slice::from_ref(&exact), None)?;
        assert!(Arc::ptr_eq(
            &first.first().ok_or("first allocation missing")?.allocation,
            &repeated
                .first()
                .ok_or("repeated allocation missing")?
                .allocation,
        ));
        assert!(matches!(
            coordinator.coordinate(std::slice::from_ref(&conflicting), None),
            Err(BudgetPoolError::ConflictingPolicy)
        ));
        Ok(())
    }

    #[test]
    fn canonical_authority_union_accepts_exact_bound_and_rejects_one_over_atomically() -> TestResult
    {
        fn authority(host: &str) -> TestResult<CanonicalNetworkAuthority> {
            Ok(CanonicalNetworkAuthority {
                host: SourceIdentifier::try_from(host)?,
                port: 443,
            })
        }

        let mut exact = BudgetCollisionKey::Public(vec![authority("bound-a.example.test")?]);
        let additional = BudgetCollisionKey::Public(vec![authority("bound-b.example.test")?]);
        exact.merge_public_authorities_with_limit(&additional, 2)?;
        assert_eq!(
            exact,
            BudgetCollisionKey::Public(vec![
                authority("bound-a.example.test")?,
                authority("bound-b.example.test")?,
            ])
        );

        let before = exact.clone();
        let one_over = BudgetCollisionKey::Public(vec![authority("bound-c.example.test")?]);
        assert_eq!(
            exact.merge_public_authorities_with_limit(&one_over, 2),
            Err(BudgetCollisionMergeError::Capacity)
        );
        assert_eq!(exact, before);
        Ok(())
    }

    #[test]
    fn durable_public_group_requires_and_accepts_transitive_connectivity() -> TestResult {
        let first = resolved_policy_with_hosts("restore-transitive-a", 2, &["a", "b"])?;
        let second = resolved_policy_with_hosts("restore-transitive-b", 2, &["b", "c"])?;
        let third = resolved_policy_with_hosts("restore-transitive-c", 2, &["c", "d"])?;

        let combined = combine_durable_group(&[first, second, third])?;
        let BudgetCollisionKey::Public(authorities) = combined.collision_key() else {
            return Err("combined public group became an account group".into());
        };
        let hosts = authorities
            .iter()
            .map(|authority| authority.host.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            hosts,
            [
                "a.example.test",
                "b.example.test",
                "c.example.test",
                "d.example.test"
            ]
        );
        Ok(())
    }

    #[test]
    fn durable_public_group_rejects_disconnected_declarations() -> TestResult {
        let first = resolved_policy_with_hosts("restore-disconnected-a", 2, &["a"])?;
        let second = resolved_policy_with_hosts("restore-disconnected-b", 2, &["b"])?;

        assert!(matches!(
            combine_durable_group(&[first, second]),
            Err(BudgetPoolError::CoordinatorCorrupt)
        ));
        Ok(())
    }

    #[test]
    fn restored_group_batch_failure_publishes_no_partial_process_allocation() -> TestResult {
        let first = resolved_policy("restore-atomic-first", 2)?;
        let second = resolved_policy("restore-atomic-second", 2)?;
        let clock = SystemBudgetClock::new();
        let observation = clock
            .observation()
            .map_err(|reason| format!("test clock unavailable: {reason:?}"))?;
        let state = BudgetState::new(first.policy(), observation.monotonic);
        let first_checkpoint =
            checkpoint_from_runtime(first.policy(), &state, observation, 1, false)?;
        let mut invalid_checkpoint =
            checkpoint_from_runtime(second.policy(), &state, observation, 1, false)?;
        let (_started, _ends, requests_used) = invalid_checkpoint
            .windows
            .as_mut_slice()
            .first_mut()
            .and_then(BudgetWindowCheckpointState::tumbling_mut)
            .ok_or("checkpoint window was not tumbling")?;
        *requests_used = second
            .policy()
            .requests_per_window()
            .ok_or("numeric request window missing")?
            + 1;
        let store: Arc<dyn AuthorityStateStore> = Arc::new(NoopAuthorityStore::default());
        let session = AuthorityDurabilitySession::open(store, observation.wall_clock)?;
        let mut coordinator = ProcessBudgetCoordinator::new(4);

        assert!(matches!(
            coordinator.coordinate_restored(
                &[(first, first_checkpoint), (second, invalid_checkpoint)],
                &session
            ),
            Err(BudgetPoolError::Persistence)
        ));
        assert!(coordinator.allocations.is_empty());
        Ok(())
    }

    #[test]
    fn clean_shutdown_proof_requires_allocation_slot_group_bijection() -> TestResult {
        let (orphan_session, orphan_pool, observation) = durable_pool("orphan", 1)?;
        let orphan_policy = resolved_policy("orphan-clean-group", 2)?;
        let orphan_checkpoint = checkpoint_from_runtime(
            orphan_policy.policy(),
            &BudgetState::new(orphan_policy.policy(), observation.monotonic),
            observation,
            1,
            false,
        )?;
        orphan_session.register_budget_group(
            crate::RegistryAuthorityState::empty(),
            orphan_policy.persisted().clone(),
            orphan_checkpoint,
            observation.wall_clock,
        )?;
        assert!(matches!(
            orphan_pool.validate_clean_shutdown(&orphan_session),
            Err(CleanShutdownValidationError::OrphanedGroup)
        ));

        let (collision_session, mut collision_pool, collision_observation) =
            durable_pool("collision", 2)?;
        let second_policy = collision_pool
            .budgets
            .get(1)
            .ok_or("second registered budget missing")?
            .budget
            .policy()
            .clone();
        collision_pool
            .budgets
            .get_mut(1)
            .ok_or("second registered budget missing")?
            .budget = SharedProviderBudget::new_durable(
            second_policy,
            collision_observation.monotonic,
            Arc::new(SystemBudgetClock::new()),
            BudgetDurabilityBinding {
                session: collision_session.clone(),
                slot: Some(0),
            },
        );
        assert!(matches!(
            collision_pool.validate_clean_shutdown(&collision_session),
            Err(CleanShutdownValidationError::SlotCollision)
        ));

        let (declaration_session, mut declaration_pool, _observation) =
            durable_pool("declaration", 1)?;
        declaration_pool
            .budgets
            .get_mut(0)
            .ok_or("registered budget missing")?
            .persisted = resolved_policy("mismatched-clean-declaration", 2)?
            .persisted()
            .clone();
        assert!(matches!(
            declaration_pool.validate_clean_shutdown(&declaration_session),
            Err(CleanShutdownValidationError::DeclarationMismatch)
        ));
        let clock = SystemBudgetClock::new();
        let observed = clock
            .observation()
            .map_err(|reason| format!("clock: {reason:?}"))?;
        let provider_store = Arc::new(TrackingProviderRateStore {
            admission_enabled: true,
            ..TrackingProviderRateStore::default()
        });
        let provider_rate = ProviderRateAuthority::try_new(provider_store.clone())?;
        let source_store = Arc::new(NoopAuthorityStore::default());
        let session = AuthorityDurabilitySession::open(source_store.clone(), observed.wall_clock)?;
        let mut pool = ProviderBudgetPool::new_durable_with_provider_rate(
            session.clone(),
            provider_rate.clone(),
        );
        let resolved = resolved_policy("provider-clean-proof", 1)?;
        let budget =
            pool.register_durable(resolved.clone(), &crate::RegistryAuthorityState::empty())?;
        assert!(session.budget_groups()?.is_empty());
        assert!(
            budget
                .allocation
                .durability
                .as_ref()
                .ok_or("missing session")?
                .slot
                .is_none()
        );
        let source_writes = source_store.writes.load(Ordering::SeqCst);
        let mut transport = match budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("provider dispatch: {other:?}").into()),
        };
        assert!(matches!(
            pool.validate_clean_shutdown(&session),
            Err(CleanShutdownValidationError::ActiveRequest)
        ));
        transport
            .complete_transport_handshake()
            .map_err(|reason| format!("handshake: {reason:?}"))?;
        // The network slot has gone, but the retained transport still owns a lifecycle admission.
        assert!(
            !pool
                .has_active_requests()
                .map_err(|reason| format!("owned count: {reason:?}"))?
        );
        assert!(matches!(
            pool.validate_clean_shutdown(&session),
            Err(CleanShutdownValidationError::ActiveRequest)
        ));
        drop(transport);
        assert_eq!(source_store.writes.load(Ordering::SeqCst), source_writes);
        let proof = pool
            .validate_clean_shutdown(&session)
            .map_err(|reason| format!("clean provider: {reason:?}"))?;
        session.close_clean(
            proof,
            crate::RegistryAuthorityState::empty(),
            clock
                .observation()
                .map_err(|reason| format!("clock: {reason:?}"))?
                .wall_clock,
        )?;
        assert!(matches!(
            budget.try_acquire(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
        ));
        assert!(budget.provider_rate_availability().is_err());

        // A source checkpoint is redundant only after its aggregate collision association exists.
        let archived = AuthorityDurabilitySession::open(
            Arc::new(NoopAuthorityStore::default()),
            observed.wall_clock,
        )?;
        let checkpoint = checkpoint_from_runtime(
            resolved.policy(),
            &BudgetState::new(resolved.policy(), observed.monotonic),
            observed,
            1,
            false,
        )?;
        archived.register_budget_group(
            crate::RegistryAuthorityState::empty(),
            resolved.persisted().clone(),
            checkpoint.clone(),
            observed.wall_clock,
        )?;
        let mut restored =
            ProviderBudgetPool::new_durable_with_provider_rate(archived.clone(), provider_rate);
        provider_store
            .retained_missing
            .store(true, Ordering::SeqCst);
        let commits = provider_store.commits.load(Ordering::SeqCst);
        assert!(
            restored
                .restore_provider_associations(
                    vec![resolved.clone()],
                    vec![(vec![resolved.clone()], checkpoint.clone())]
                )
                .is_err()
        );
        assert_eq!(archived.budget_groups()?.len(), 1);
        provider_store
            .retained_missing
            .store(false, Ordering::SeqCst);
        restored.restore_provider_associations(
            vec![resolved.clone()],
            vec![(vec![resolved.clone()], checkpoint)],
        )?;
        assert!(archived.budget_groups()?.is_empty());
        assert_eq!(provider_store.commits.load(Ordering::SeqCst), commits);
        assert_eq!(restored.policies(), vec![resolved.persisted().clone()]);
        assert!(restored.budgets.is_empty());
        Ok(())
    }
}
