#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::SourceError;

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
                Self::RequestWindowExhausted => BudgetUnavailableReason::RequestWindowExhausted,
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

    #[test]
    fn conjunctive_budget_policy_preserves_legacy_wire_and_enforces_invariants()
    -> Result<(), Box<dyn std::error::Error>> {
        let legacy = policy()?;
        let legacy_json = serde_json::to_string(&legacy)?;
        assert_eq!(
            legacy_json,
            r#"{"scope":{"provider":"provider","authorization_account":null},"requests_per_window":2,"window_nanos":100,"max_concurrent":1,"backoff":{"initial_nanos":10,"maximum_nanos":100,"jitter_basis_points":1000}}"#
        );
        assert_eq!(serde_json::from_str::<ProviderBudgetPolicy>(&legacy_json)?, legacy);

        let windows = [
            ProviderBudgetWindow::try_new(
                NonZeroU32::new(1).ok_or("window limit must be nonzero")?,
                NonZeroU64::new(500).ok_or("window duration must be nonzero")?,
                BudgetWindowSemantics::Sliding,
            )?,
            ProviderBudgetWindow::try_new(
                NonZeroU32::new(2).ok_or("window limit must be nonzero")?,
                NonZeroU64::new(1_000).ok_or("window duration must be nonzero")?,
                BudgetWindowSemantics::Sliding,
            )?,
        ];
        let conjunctive = ProviderBudgetPolicy::try_new_conjunctive(
            legacy.scope().clone(),
            &windows,
            NonZeroU16::new(1).ok_or("concurrency must be nonzero")?,
            legacy.backoff(),
        )?;
        assert_eq!(conjunctive.window_count(), 2);
        assert_eq!(conjunctive.window(0), Some(windows[0]));
        assert_eq!(conjunctive.window(1), Some(windows[1]));
        assert!(serde_json::to_string(&conjunctive)?.contains("additional_windows"));

        let duplicate_duration = [windows[0], ProviderBudgetWindow::try_new(
            NonZeroU32::new(2).ok_or("window limit must be nonzero")?,
            NonZeroU64::new(500).ok_or("window duration must be nonzero")?,
            BudgetWindowSemantics::Tumbling,
        )?];
        assert_eq!(
            ProviderBudgetPolicy::try_new_conjunctive(
                legacy.scope().clone(),
                &duplicate_duration,
                NonZeroU16::new(1).ok_or("concurrency must be nonzero")?,
                legacy.backoff(),
            ),
            Err(NetworkPolicyError::InvalidBudgetPolicy)
        );
        assert_eq!(
            ProviderBudgetPolicy::try_new_conjunctive(
                legacy.scope().clone(),
                &windows,
                NonZeroU16::new(2).ok_or("concurrency must be nonzero")?,
                legacy.backoff(),
            ),
            Err(NetworkPolicyError::InvalidBudgetPolicy)
        );
        let oversized_sliding = [ProviderBudgetWindow::try_new(
            NonZeroU32::new(4_097).ok_or("window limit must be nonzero")?,
            NonZeroU64::new(1).ok_or("window duration must be nonzero")?,
            BudgetWindowSemantics::Sliding,
        )?];
        assert_eq!(
            ProviderBudgetPolicy::try_new_conjunctive(
                legacy.scope().clone(),
                &oversized_sliding,
                NonZeroU16::new(1).ok_or("concurrency must be nonzero")?,
                legacy.backoff(),
            ),
            Err(NetworkPolicyError::InvalidBudgetPolicy)
        );
        assert_eq!(
            ProviderBudgetWindow::try_new(
                NonZeroU32::new(1).ok_or("window limit must be nonzero")?,
                NonZeroU64::new((i64::MAX as u64) + 1)
                    .ok_or("window duration must be nonzero")?,
                BudgetWindowSemantics::Tumbling,
            ),
            Err(NetworkPolicyError::InvalidBudgetPolicy)
        );
        Ok(())
    }

    #[test]
    fn conjunctive_sliding_admission_is_all_or_nothing() -> Result<(), NetworkPolicyError> {
        let clock = Arc::new(ManualClock::new(0, 0));
        let windows = [
            ProviderBudgetWindow::try_new(
                NonZeroU32::new(1).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
                NonZeroU64::new(500).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
                BudgetWindowSemantics::Sliding,
            )?,
            ProviderBudgetWindow::try_new(
                NonZeroU32::new(1).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
                NonZeroU64::new(700).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
                BudgetWindowSemantics::Sliding,
            )?,
            ProviderBudgetWindow::try_new(
                NonZeroU32::new(2).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
                NonZeroU64::new(1_000).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
                BudgetWindowSemantics::Sliding,
            )?,
        ];
        let budget = SharedProviderBudget::new(
            ProviderBudgetPolicy::try_new_conjunctive(
                BudgetScope::new(
                    SourceIdentifier::try_from("conjunctive-provider")
                        .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?,
                ),
                &windows,
                NonZeroU16::new(1).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
                policy()?.backoff(),
            )?,
            MonotonicInstant::from_nanos(0),
            clock.clone(),
        );
        let BudgetDecision::Ready(first) = budget.try_acquire() else {
            return Err(NetworkPolicyError::InvalidBudgetPolicy);
        };
        first.release();

        assert!(clock.set(100, 100));
        assert!(
            matches!(budget.try_acquire(), BudgetDecision::WaitUntil(deadline) if deadline.as_nanos() == 700)
        );
        assert!(clock.set(700, 700));
        let BudgetDecision::Ready(second) = budget.try_acquire() else {
            return Err(NetworkPolicyError::InvalidBudgetPolicy);
        };
        second.release();
        assert!(
            matches!(budget.try_acquire(), BudgetDecision::WaitUntil(deadline) if deadline.as_nanos() == 1_400)
        );
        Ok(())
    }

    fn retry_after_policy() -> Result<ProviderBudgetPolicy, NetworkPolicyError> {
        ProviderBudgetPolicy::try_new(
            BudgetScope::new(
                SourceIdentifier::try_from("retry-after-provider")
                    .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?,
            ),
            NonZeroU32::new(2).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
            NonZeroU64::new(30_000_000_000)
                .ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
            NonZeroU16::new(1).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
            BackoffPolicy::try_new(
                NonZeroU64::new(10).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
                NonZeroU64::new(10_000_000_000)
                    .ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
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
    fn http_retry_after_preserves_valid_deadlines_and_bounds_fallbacks()
    -> Result<(), NetworkPolicyError> {
        let decision_for = |field: Option<&[u8]>| {
            let budget = SharedProviderBudget::new(
                retry_after_policy()?,
                MonotonicInstant::from_nanos(100),
                Arc::new(ManualClock::new(0, 100)),
            );
            Ok::<_, NetworkPolicyError>(apply_http_retry_after(&budget, field, 0))
        };

        assert!(matches!(
            decision_for(Some(b"2"))?,
            BudgetDecision::WaitUntil(deadline) if deadline.as_nanos() == 2_000_000_100
        ));
        assert!(matches!(
            decision_for(Some(b"Thu, 01 Jan 1970 00:00:02 GMT"))?,
            BudgetDecision::WaitUntil(deadline) if deadline.as_nanos() == 2_000_000_100
        ));

        let oversized = [b'9'; 129];
        let non_ascii = [0xff];
        for field in [
            None,
            Some(b"0".as_slice()),
            Some(b"invalid".as_slice()),
            Some(b"18446744073709551615".as_slice()),
            Some(non_ascii.as_slice()),
            Some(oversized.as_slice()),
        ] {
            assert!(matches!(
                decision_for(field)?,
                BudgetDecision::WaitUntil(deadline) if deadline.as_nanos() == 110
            ));
        }
        let over_policy = SharedProviderBudget::new(
            retry_after_policy()?,
            MonotonicInstant::from_nanos(100),
            Arc::new(ManualClock::new(0, 100)),
        );
        let over_policy_error = SourceError::from_applied_budget_refusal(apply_http_retry_after(
            &over_policy,
            Some(b"11"),
            0,
        ));
        assert_eq!(
            over_policy_error,
            SourceError::BudgetUnavailable {
                reason: BudgetUnavailableReason::RetryAfterExceedsPolicy,
            }
        );
        assert!(matches!(
            over_policy.try_acquire(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled)
        ));
        let mapped = SourceError::from_applied_budget_refusal(apply_http_retry_after(
            &SharedProviderBudget::new(
                retry_after_policy()?,
                MonotonicInstant::from_nanos(100),
                Arc::new(ManualClock::new(0, 100)),
            ),
            Some(b"2"),
            0,
        ));
        assert!(matches!(
            mapped,
            SourceError::BudgetWaitUntil { deadline } if deadline.as_nanos() == 2_000_000_100
        ));
        Ok(())
    }

    #[test]
    fn refusal_cooldown_blocks_before_and_expires_at_the_exact_monotonic_deadline()
    -> Result<(), NetworkPolicyError> {
        let clock = Arc::new(ManualClock::new(100, 1_000));
        let budget = SharedProviderBudget::new(
            policy()?,
            MonotonicInstant::from_nanos(1_000),
            clock.clone(),
        );
        let deadline = match budget.apply_refusal(0) {
            BudgetDecision::WaitUntil(deadline) => deadline,
            _ => return Err(NetworkPolicyError::InvalidBudgetPolicy),
        };
        assert_eq!(deadline.as_nanos(), 1_010);

        assert!(clock.set(109, 1_009));
        assert_eq!(
            budget.remaining_wait(deadline),
            Ok(Duration::from_nanos(1))
        );
        assert!(matches!(
            budget.try_acquire(),
            BudgetDecision::WaitUntil(observed) if observed == deadline
        ));

        assert!(clock.set(110, 1_010));
        assert_eq!(budget.remaining_wait(deadline), Ok(Duration::ZERO));
        let BudgetDecision::Ready(permit) = budget.try_acquire() else {
            return Err(NetworkPolicyError::InvalidBudgetPolicy);
        };
        permit.release();
        Ok(())
    }

    #[test]
    fn remaining_wait_terminally_rejects_clock_failure_and_regression()
    -> Result<(), NetworkPolicyError> {
        let regression_clock = Arc::new(ManualClock::new(100, 1_000));
        let regression_budget = SharedProviderBudget::new(
            policy()?,
            MonotonicInstant::from_nanos(1_000),
            regression_clock.clone(),
        );
        let deadline = match regression_budget.apply_refusal(0) {
            BudgetDecision::WaitUntil(deadline) => deadline,
            _ => return Err(NetworkPolicyError::InvalidBudgetPolicy),
        };
        assert!(regression_clock.set(99, 999));
        assert_eq!(
            regression_budget.remaining_wait(deadline),
            Err(BudgetUnavailableReason::ClockRegression)
        );
        assert!(matches!(
            regression_budget.try_acquire(),
            BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted
            )
        ));

        let unavailable_clock = Arc::new(SwitchableClock::new(100, 1_000));
        let unavailable_budget = SharedProviderBudget::new(
            policy()?,
            MonotonicInstant::from_nanos(1_000),
            unavailable_clock.clone(),
        );
        let deadline = match unavailable_budget.apply_refusal(0) {
            BudgetDecision::WaitUntil(deadline) => deadline,
            _ => return Err(NetworkPolicyError::InvalidBudgetPolicy),
        };
        unavailable_clock.fail();
        assert_eq!(
            unavailable_budget.remaining_wait(deadline),
            Err(BudgetUnavailableReason::ClockUnavailable)
        );
        assert!(matches!(
            unavailable_budget.try_acquire(),
            BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted
            )
        ));
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
        let budget = SharedProviderBudget::new(policy()?, MonotonicInstant::from_nanos(0), clock);
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
        let budget = SharedProviderBudget::new(policy()?, MonotonicInstant::from_nanos(0), clock);
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
            BudgetDecision::Unavailable(BudgetUnavailableReason::AvailabilityGenerationExhausted)
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
            primary_sliding_releases: VecDeque::new(),
            additional_windows: Vec::new(),
            in_flight: 0,
            unavailable_until: None,
            disabled: false,
            consecutive_refusals: 0,
        };
        let valid = checkpoint_from_runtime(&policy, &state, observation, 1, false)
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        assert!(validate_checkpoint(&policy, &valid, observation).is_ok());

        let mut excessive_requests = valid.clone();
        let (_started, _ends, requests_used) = excessive_requests
            .windows
            .as_mut_slice()
            .first_mut()
            .and_then(BudgetWindowCheckpointState::tumbling_mut)
            .ok_or(NetworkPolicyError::InvalidBudgetPolicy)?;
        *requests_used = policy.requests_per_window() + 1;
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
        let (_started, window_ends_wall, _requests) = wrong_window
            .windows
            .as_mut_slice()
            .first_mut()
            .and_then(BudgetWindowCheckpointState::tumbling_mut)
            .ok_or(NetworkPolicyError::InvalidBudgetPolicy)?;
        *window_ends_wall = window_ends_wall
            .checked_add_nanos(1)
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        assert_eq!(
            validate_checkpoint(&policy, &wrong_window, observation),
            Err(AuthorityPersistenceError::InvalidState)
        );

        let mut future_window = valid.clone();
        let (window_started_wall, window_ends_wall, _requests) = future_window
            .windows
            .as_mut_slice()
            .first_mut()
            .and_then(BudgetWindowCheckpointState::tumbling_mut)
            .ok_or(NetworkPolicyError::InvalidBudgetPolicy)?;
        *window_started_wall = Timestamp::from_unix_nanos(1_001);
        *window_ends_wall = Timestamp::from_unix_nanos(1_101);
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

    #[test]
    fn v2_sliding_checkpoint_restores_exact_deadlines_and_rejects_corruption()
    -> Result<(), Box<dyn std::error::Error>> {
        let windows = [
            ProviderBudgetWindow::try_new(
                NonZeroU32::new(2).ok_or("window limit must be nonzero")?,
                NonZeroU64::new(100).ok_or("window duration must be nonzero")?,
                BudgetWindowSemantics::Sliding,
            )?,
            ProviderBudgetWindow::try_new(
                NonZeroU32::new(2).ok_or("window limit must be nonzero")?,
                NonZeroU64::new(200).ok_or("window duration must be nonzero")?,
                BudgetWindowSemantics::Sliding,
            )?,
        ];
        let policy = ProviderBudgetPolicy::try_new_conjunctive(
            BudgetScope::new(SourceIdentifier::try_from("restart-provider")?),
            &windows,
            NonZeroU16::new(1).ok_or("concurrency must be nonzero")?,
            policy()?.backoff(),
        )?;
        let saved = ClockObservation::new(
            Timestamp::from_unix_nanos(1_000),
            MonotonicInstant::from_nanos(100),
        );
        let mut state = BudgetState::new(&policy, saved.monotonic);
        state.primary_sliding_releases.extend([
            MonotonicInstant::from_nanos(150),
            MonotonicInstant::from_nanos(190),
        ]);
        state.requests_used = 2;
        let additional = state
            .additional_windows
            .first_mut()
            .ok_or("additional window missing")?;
        additional
            .sliding_releases
            .push_back(MonotonicInstant::from_nanos(250));
        additional.requests_used = 1;

        let checkpoint = checkpoint_from_runtime(&policy, &state, saved, 1, false)?;
        let restored = runtime_state_from_checkpoint(
            &policy,
            &checkpoint,
            ClockObservation::new(
                Timestamp::from_unix_nanos(1_050),
                MonotonicInstant::from_nanos(500),
            ),
        )?;
        assert_eq!(
            restored
                .primary_sliding_releases
                .iter()
                .map(|deadline| deadline.as_nanos())
                .collect::<Vec<_>>(),
            [540]
        );
        assert_eq!(
            restored
                .additional_windows
                .first()
                .ok_or("restored additional window missing")?
                .sliding_releases
                .front()
                .map(|deadline| deadline.as_nanos()),
            Some(600)
        );

        let mut corrupt = checkpoint;
        corrupt.windows = BoundedVec::try_new(vec![
            BudgetWindowCheckpointState::Sliding {
                release_deadlines_wall: BoundedVec::try_new(vec![
                    Timestamp::from_unix_nanos(1_090),
                    Timestamp::from_unix_nanos(1_050),
                ])?,
            },
            BudgetWindowCheckpointState::Sliding {
                release_deadlines_wall: BoundedVec::singleton(
                    Timestamp::from_unix_nanos(1_150),
                ),
            },
        ])?;
        assert_eq!(
            validate_checkpoint(&policy, &corrupt, saved),
            Err(AuthorityPersistenceError::InvalidState)
        );
        Ok(())
    }
}
