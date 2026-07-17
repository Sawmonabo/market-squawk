#[cfg(test)]
mod coordinator_tests {
    use std::error::Error;
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
    use std::sync::atomic::Ordering;

    use market_squawk_domain::{
        AuthorizationBasis, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
        ExactPayloadEvidence,
    };

    use super::*;
    use crate::policy::persistence::AuthorityStateStoreError;

    type TestResult<T = ()> = Result<T, Box<dyn Error>>;

    #[derive(Debug)]
    struct NoopAuthorityStore;

    impl AuthorityStateStore for NoopAuthorityStore {
        fn load(&self) -> Result<Option<Vec<u8>>, AuthorityStateStoreError> {
            Ok(None)
        }

        fn store(&self, _payload: &[u8]) -> Result<(), AuthorityStateStoreError> {
            Ok(())
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
        let lease = budget.availability_lease().map_err(|reason| {
            std::io::Error::other(format!("budget lease unavailable: {reason:?}"))
        })?;
        let expected = std::mem::size_of::<BudgetAllocation>()
            .checked_add(crate::conservative_arc_control_block_charge::<
                BudgetAllocation,
            >())
            .and_then(|bytes| bytes.checked_add(expected_dynamic))
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
        assert!(matches!(restored.try_acquire(), BudgetDecision::WaitUntil(_)));
        Ok(())
    }

    #[test]
    fn dropping_every_external_handle_preserves_refusal_disabled_and_terminal_state()
    -> TestResult {
        let refusal_policy = resolved_policy("drop-reset-refusal-state", 2)?;
        let refusal = register_fresh(refusal_policy.clone())?;
        let deadline = match refusal.apply_refusal(0) {
            BudgetDecision::WaitUntil(deadline) => deadline,
            other => return Err(format!("unexpected refusal decision: {other:?}").into()),
        };
        drop(refusal);
        let refusal_restored = register_fresh(refusal_policy)?;
        assert!(matches!(
            refusal_restored.try_acquire(),
            BudgetDecision::WaitUntil(observed) if observed == deadline
        ));

        let disabled_policy = resolved_policy("drop-reset-disabled-state", 2)?;
        let disabled = register_fresh(disabled_policy.clone())?;
        assert!(matches!(
            disabled.disable(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled)
        ));
        drop(disabled);
        let disabled_restored = register_fresh(disabled_policy)?;
        assert!(matches!(
            disabled_restored.try_acquire(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled)
        ));

        let terminal_policy = resolved_policy("drop-reset-terminal-state", 2)?;
        let terminal = register_fresh(terminal_policy.clone())?;
        terminal
            .allocation
            .availability_generation
            .store(u64::MAX, Ordering::Release);
        assert!(matches!(
            terminal.disable(),
            BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted
            )
        ));
        drop(terminal);
        let terminal_restored = register_fresh(terminal_policy)?;
        assert!(matches!(
            terminal_restored.try_acquire(),
            BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted
            )
        ));
        Ok(())
    }

    #[test]
    fn coordinator_capacity_and_conflict_fail_without_mutating_authoritative_state()
    -> TestResult {
        let first_policy = resolved_policy("bounded-coordinator-first", 1)?;
        let second_policy = resolved_policy("bounded-coordinator-second", 1)?;
        let mut coordinator = ProcessBudgetCoordinator::new(1);
        let first = coordinator.coordinate(std::slice::from_ref(&first_policy), None)?;
        let first_budget = first
            .first()
            .ok_or("first coordinated budget missing")?;
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
    fn canonical_authority_union_accepts_exact_bound_and_rejects_one_over_atomically()
    -> TestResult {
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
        assert_eq!(hosts, ["a.example.test", "b.example.test", "c.example.test", "d.example.test"]);
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
        let state = BudgetState {
            window_started_at: observation.monotonic,
            restored_window_ends_at: None,
            requests_used: 0,
            in_flight: 0,
            unavailable_until: None,
            disabled: false,
            consecutive_refusals: 0,
        };
        let first_checkpoint = checkpoint_from_runtime(
            first.policy(),
            &state,
            observation,
            1,
            false,
        )?;
        let mut invalid_checkpoint = checkpoint_from_runtime(
            second.policy(),
            &state,
            observation,
            1,
            false,
        )?;
        invalid_checkpoint.requests_used = second.policy().requests_per_window() + 1;
        let store: Arc<dyn AuthorityStateStore> = Arc::new(NoopAuthorityStore);
        let session = AuthorityDurabilitySession::open(store, observation.wall_clock)?;
        let mut coordinator = ProcessBudgetCoordinator::new(4);

        assert!(matches!(
            coordinator.coordinate_restored(
                &[
                    (first, first_checkpoint),
                    (second, invalid_checkpoint)
                ],
                &session
            ),
            Err(BudgetPoolError::Persistence)
        ));
        assert!(coordinator.allocations.is_empty());
        Ok(())
    }
}
