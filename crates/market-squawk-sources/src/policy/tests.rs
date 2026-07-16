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
}
