#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct ManualClock {
        observation: Mutex<ClockObservation>,
    }

    impl ManualClock {
        fn new(wall: i64, monotonic: u64) -> Self {
            Self {
                observation: Mutex::new(ClockObservation::new(
                    Timestamp::from_unix_nanos(wall),
                    MonotonicInstant::from_nanos(monotonic),
                )),
            }
        }

        fn set(&self, wall: i64, monotonic: u64) -> bool {
            let Ok(mut observation) = self.observation.lock() else {
                return false;
            };
            *observation = ClockObservation::new(
                Timestamp::from_unix_nanos(wall),
                MonotonicInstant::from_nanos(monotonic),
            );
            true
        }
    }

    impl BudgetClock for ManualClock {
        fn observation(&self) -> Result<ClockObservation, BudgetUnavailableReason> {
            self.observation
                .lock()
                .map(|observation| *observation)
                .map_err(|_| BudgetUnavailableReason::StatePoisoned)
        }


        fn shared_allocation_charge(&self) -> usize {
            std::mem::size_of::<Self>() + crate::conservative_arc_control_block_charge::<Self>()
        }
    }

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

        fn set(&self, wall: i64, monotonic: u64) -> bool {
            let Ok(mut observation) = self.observation.lock() else {
                return false;
            };
            *observation = ClockObservation::new(
                Timestamp::from_unix_nanos(wall),
                MonotonicInstant::from_nanos(monotonic),
            );
            true
        }

        fn fail(&self) {
            self.available.store(false, Ordering::Release);
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

    #[derive(Clone, Copy, Debug)]
    enum AvailabilityFailureCase {
        ClockUnavailable,
        StatePoisoned,
        Disabled,
        ClockRegression,
        CoolingDown,
        DeadlineOverflow,
        RequestWindowExhausted,
        ConcurrencyExhausted,
    }

    impl AvailabilityFailureCase {
        const ALL: [Self; 8] = [
            Self::ClockUnavailable,
            Self::StatePoisoned,
            Self::Disabled,
            Self::ClockRegression,
            Self::CoolingDown,
            Self::DeadlineOverflow,
            Self::RequestWindowExhausted,
            Self::ConcurrencyExhausted,
        ];

        const fn expected(self) -> BudgetUnavailableReason {
            match self {
                Self::ClockUnavailable => BudgetUnavailableReason::ClockUnavailable,
                Self::StatePoisoned => BudgetUnavailableReason::StatePoisoned,
                Self::Disabled => BudgetUnavailableReason::Disabled,
                Self::ClockRegression => BudgetUnavailableReason::ClockRegression,
                Self::CoolingDown => BudgetUnavailableReason::CoolingDown,
                Self::DeadlineOverflow => BudgetUnavailableReason::DeadlineOverflow,
                Self::RequestWindowExhausted => {
                    BudgetUnavailableReason::RequestWindowExhausted
                }
                Self::ConcurrencyExhausted => BudgetUnavailableReason::ConcurrencyExhausted,
            }
        }
    }

    #[allow(clippy::panic)]
    fn poison_budget_state(budget: &SharedProviderBudget) {
        // This panic is intentionally caught and exists solely to exercise fail-closed poison
        // handling. Production paths remain panic-free.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let Ok(_state) = budget.allocation.state.lock() else {
                return;
            };
            panic!("test-only budget state poison");
        }));
        assert!(result.is_err());
    }

    fn policy() -> Result<ProviderBudgetPolicy, NetworkPolicyError> {
        ProviderBudgetPolicy::try_new(
            BudgetScope::new(
                SourceIdentifier::try_from("provider")
                    .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?,
            ),
            NonZeroU32::new(2).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
            NonZeroU64::new(100).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
            NonZeroU16::new(1).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
            BackoffPolicy::try_new(
                NonZeroU64::new(10).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
                NonZeroU64::new(100).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
                1_000,
            )?,
        )
    }

    fn authorization(
        mode: crate::AuthorizationMode,
        basis: &str,
    ) -> Result<crate::AuthorizationGrant, NetworkPolicyError> {
        use market_squawk_domain::{
            AuthorizationBasis, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
            ExactPayloadEvidence,
        };

        Ok(crate::AuthorizationGrant::new(
            mode,
            AuthorizationBasis::new(
                SourceIdentifier::try_from(basis)
                    .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?,
            ),
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                [7; 32],
            )),
            EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)
                .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?,
        ))
    }

    #[test]
    fn budget_scope_is_exhaustively_derived_from_authorization_mode_and_basis()
    -> Result<(), NetworkPolicyError> {
        let provider = SourceIdentifier::try_from("provider")
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        let public = authorization(crate::AuthorizationMode::PublicInterface, "public-terms")?;
        let user = authorization(crate::AuthorizationMode::UserAuthorized, "account-a")?;
        let licensed = authorization(crate::AuthorizationMode::Licensed, "license-a")?;
        let local = authorization(crate::AuthorizationMode::UserOwnedLocal, "local-file")?;

        assert_eq!(
            BudgetScope::for_authorization(provider.clone(), &public)?,
            BudgetScope::new(provider.clone())
        );
        assert_eq!(
            BudgetScope::for_authorization(provider.clone(), &user)?,
            BudgetScope::with_authorization_account(
                provider.clone(),
                SourceIdentifier::try_from("account-a")
                    .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?,
            )
        );
        assert_eq!(
            BudgetScope::for_authorization(provider.clone(), &licensed)?,
            BudgetScope::with_authorization_account(
                provider.clone(),
                SourceIdentifier::try_from("license-a")
                    .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?,
            )
        );
        assert_eq!(
            BudgetScope::for_authorization(provider, &local),
            Err(NetworkPolicyError::InvalidBudgetScope)
        );
        Ok(())
    }

    #[test]
    fn monotonic_window_ignores_forward_and_backward_wall_clock_changes()
    -> Result<(), NetworkPolicyError> {
        let clock = Arc::new(ManualClock::new(0, 0));
        let budget =
            SharedProviderBudget::new(policy()?, MonotonicInstant::from_nanos(0), clock.clone());
        for _ in 0..2 {
            let BudgetDecision::Ready(permit) = budget.try_acquire() else {
                return Err(NetworkPolicyError::InvalidBudgetPolicy);
            };
            permit.release();
        }
        assert!(
            matches!(budget.try_acquire(), BudgetDecision::WaitUntil(deadline) if deadline.as_nanos() == 100)
        );
        assert!(clock.set(1_000_000, 50));
        assert!(
            matches!(budget.try_acquire(), BudgetDecision::WaitUntil(deadline) if deadline.as_nanos() == 100)
        );
        assert!(clock.set(-1_000_000, 50));
        assert!(
            matches!(budget.try_acquire(), BudgetDecision::WaitUntil(deadline) if deadline.as_nanos() == 100)
        );
        assert!(clock.set(0, 100));
        assert!(matches!(budget.try_acquire(), BudgetDecision::Ready(_)));
        Ok(())
    }

    #[test]
    fn refusal_escalation_and_retry_after_only_extend_shared_cooldown()
    -> Result<(), NetworkPolicyError> {
        let clock = Arc::new(ManualClock::new(100, 1_000));
        let budget = SharedProviderBudget::new(
            policy()?,
            MonotonicInstant::from_nanos(1_000),
            clock.clone(),
        );
        assert!(
            matches!(budget.apply_refusal(0), BudgetDecision::WaitUntil(deadline) if deadline.as_nanos() == 1_010)
        );
        assert!(
            matches!(budget.apply_refusal(0), BudgetDecision::WaitUntil(deadline) if deadline.as_nanos() == 1_020)
        );
        assert!(
            matches!(budget.apply_retry_after(RetryAfter::Delay(NonZeroU64::new(5).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?)), BudgetDecision::WaitUntil(deadline) if deadline.as_nanos() == 1_020)
        );
        assert!(
            matches!(budget.apply_retry_after(RetryAfter::AtWallClock(Timestamp::from_unix_nanos(150))), BudgetDecision::WaitUntil(deadline) if deadline.as_nanos() == 1_050)
        );
        assert!(matches!(
            budget.apply_retry_after(RetryAfter::Delay(
                NonZeroU64::new(101).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?
            )),
            BudgetDecision::Unavailable(BudgetUnavailableReason::RetryAfterExceedsPolicy)
        ));
        assert!(matches!(
            budget.try_acquire(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled)
        ));
        assert!(clock.set(1_000_000, 2_000));
        Ok(())
    }

    #[test]
    fn every_availability_failure_revokes_prior_lease_before_returning()
    -> Result<(), NetworkPolicyError> {
        for case in AvailabilityFailureCase::ALL {
            let clock = Arc::new(SwitchableClock::new(0, 0));
            let budget = SharedProviderBudget::new(
                policy()?,
                MonotonicInstant::from_nanos(0),
                clock.clone(),
            );
            let prior = match budget.availability_lease() {
                Ok(lease) => lease,
                Err(_) => return Err(NetworkPolicyError::InvalidBudgetPolicy),
            };
            match case {
                AvailabilityFailureCase::ClockUnavailable => clock.fail(),
                AvailabilityFailureCase::StatePoisoned => poison_budget_state(&budget),
                AvailabilityFailureCase::Disabled => {
                    let Ok(mut state) = budget.allocation.state.lock() else {
                        return Err(NetworkPolicyError::InvalidBudgetPolicy);
                    };
                    state.disabled = true;
                }
                AvailabilityFailureCase::ClockRegression => {
                    let Ok(mut state) = budget.allocation.state.lock() else {
                        return Err(NetworkPolicyError::InvalidBudgetPolicy);
                    };
                    state.window_started_at = MonotonicInstant::from_nanos(1);
                }
                AvailabilityFailureCase::CoolingDown => {
                    let Ok(mut state) = budget.allocation.state.lock() else {
                        return Err(NetworkPolicyError::InvalidBudgetPolicy);
                    };
                    state.unavailable_until = Some(MonotonicInstant::from_nanos(1));
                }
                AvailabilityFailureCase::DeadlineOverflow => {
                    assert!(clock.set(i64::MAX, u64::MAX));
                    let Ok(mut state) = budget.allocation.state.lock() else {
                        return Err(NetworkPolicyError::InvalidBudgetPolicy);
                    };
                    state.window_started_at = MonotonicInstant::from_nanos(u64::MAX);
                }
                AvailabilityFailureCase::RequestWindowExhausted => {
                    let Ok(mut state) = budget.allocation.state.lock() else {
                        return Err(NetworkPolicyError::InvalidBudgetPolicy);
                    };
                    state.requests_used = budget.policy().requests_per_window();
                }
                AvailabilityFailureCase::ConcurrencyExhausted => {
                    let Ok(mut state) = budget.allocation.state.lock() else {
                        return Err(NetworkPolicyError::InvalidBudgetPolicy);
                    };
                    state.in_flight = budget.policy().max_concurrent();
                }
            }
            let result = budget.availability_lease();
            assert!(
                matches!(result, Err(reason) if reason == case.expected()),
                "unexpected availability result for {case:?}: {result:?}"
            );
            assert!(
                !prior.is_available(),
                "prior lease survived availability failure {case:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn consuming_final_available_slot_revokes_prior_availability_lease()
    -> Result<(), NetworkPolicyError> {
        let clock = Arc::new(ManualClock::new(0, 0));
        let budget =
            SharedProviderBudget::new(policy()?, MonotonicInstant::from_nanos(0), clock);
        let prior = budget
            .availability_lease()
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;

        let permit = match budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            _ => return Err(NetworkPolicyError::InvalidBudgetPolicy),
        };
        assert!(!prior.is_available());
        permit.release();
        assert!(budget.availability_lease().is_ok());
        Ok(())
    }

    #[test]
    fn availability_generation_overflow_terminalizes_all_future_operations()
    -> Result<(), NetworkPolicyError> {
        let clock = Arc::new(ManualClock::new(0, 0));
        let budget =
            SharedProviderBudget::new(policy()?, MonotonicInstant::from_nanos(0), clock);
        budget
            .allocation
            .availability_generation
            .store(u64::MAX, Ordering::Release);
        let final_generation_lease = budget
            .availability_lease()
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        {
            let Ok(mut state) = budget.allocation.state.lock() else {
                return Err(NetworkPolicyError::InvalidBudgetPolicy);
            };
            state.disabled = true;
        }

        assert!(matches!(
            budget.availability_lease(),
            Err(BudgetUnavailableReason::AvailabilityGenerationExhausted)
        ));
        assert!(!final_generation_lease.is_available());
        assert!(budget.allocation.terminal.load(Ordering::Acquire));

        if let Ok(mut state) = budget.allocation.state.lock() {
            state.disabled = false;
        }
        assert!(matches!(
            budget.availability_lease(),
            Err(BudgetUnavailableReason::AvailabilityGenerationExhausted)
        ));
        assert!(matches!(
            budget.try_acquire(),
            BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted
            )
        ));
        assert_eq!(
            budget.record_success(),
            Err(BudgetUnavailableReason::AvailabilityGenerationExhausted)
        );
        Ok(())
    }

    #[test]
    fn unavailable_state_is_not_observable_before_generation_revocation()
    -> Result<(), NetworkPolicyError> {
        use std::sync::{Barrier, mpsc};

        let clock = Arc::new(ManualClock::new(0, 0));
        let budget = Arc::new(SharedProviderBudget::new(
            policy()?,
            MonotonicInstant::from_nanos(0),
            clock,
        ));
        let prior = budget
            .availability_lease()
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        let release = Arc::new(Barrier::new(2));
        let worker_budget = Arc::clone(&budget);
        let worker_release = Arc::clone(&release);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let decision = worker_budget.try_acquire();
            let sent = ready_tx.send(()).is_ok();
            worker_release.wait();
            (decision, sent)
        });

        ready_rx
            .recv()
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        {
            let state = budget
                .allocation
                .state
                .lock()
                .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
            assert_eq!(state.in_flight, budget.policy().max_concurrent());
            assert!(!prior.is_available());
        }
        release.wait();
        let (decision, sent) = worker
            .join()
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        assert!(sent);
        assert!(matches!(decision, BudgetDecision::Ready(_)));
        Ok(())
    }

    #[test]
    fn durable_checkpoint_rejects_corrupt_counters_generations_and_deadlines()
    -> Result<(), NetworkPolicyError> {
        let policy = policy()?;
        let observation = ClockObservation::new(
            Timestamp::from_unix_nanos(1_000),
            MonotonicInstant::from_nanos(1_000),
        );
        let state = BudgetState {
            window_started_at: observation.monotonic,
            restored_window_ends_at: None,
            requests_used: 1,
            in_flight: 0,
            unavailable_until: None,
            disabled: false,
            consecutive_refusals: 0,
        };
        let valid = checkpoint_from_runtime(&policy, &state, observation, 1, false)
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        assert!(validate_checkpoint(&policy, &valid, observation).is_ok());

        let mut excessive_requests = valid.clone();
        excessive_requests.requests_used = policy.requests_per_window() + 1;
        assert_eq!(
            validate_checkpoint(&policy, &excessive_requests, observation),
            Err(AuthorityPersistenceError::InvalidState)
        );

        let mut excessive_in_flight = valid.clone();
        excessive_in_flight.in_flight = policy.max_concurrent() + 1;
        assert_eq!(
            validate_checkpoint(&policy, &excessive_in_flight, observation),
            Err(AuthorityPersistenceError::InvalidState)
        );

        let mut zero_generation = valid.clone();
        zero_generation.availability_generation = 0;
        assert_eq!(
            validate_checkpoint(&policy, &zero_generation, observation),
            Err(AuthorityPersistenceError::InvalidState)
        );

        let mut poisoned_without_terminal = valid.clone();
        poisoned_without_terminal.poisoned = true;
        assert_eq!(
            validate_checkpoint(&policy, &poisoned_without_terminal, observation),
            Err(AuthorityPersistenceError::InvalidState)
        );

        let mut wrong_window = valid.clone();
        wrong_window.window_ends_wall = wrong_window
            .window_ends_wall
            .checked_add_nanos(1)
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        assert_eq!(
            validate_checkpoint(&policy, &wrong_window, observation),
            Err(AuthorityPersistenceError::InvalidState)
        );

        let mut future_window = valid.clone();
        future_window.window_started_wall = Timestamp::from_unix_nanos(1_001);
        future_window.window_ends_wall = Timestamp::from_unix_nanos(1_101);
        assert_eq!(
            validate_checkpoint(&policy, &future_window, observation),
            Err(AuthorityPersistenceError::FutureState)
        );

        let mut excessive_cooldown = valid;
        excessive_cooldown.unavailable_until_wall = Some(Timestamp::from_unix_nanos(1_101));
        assert_eq!(
            validate_checkpoint(&policy, &excessive_cooldown, observation),
            Err(AuthorityPersistenceError::InvalidState)
        );
        Ok(())
    }
}
