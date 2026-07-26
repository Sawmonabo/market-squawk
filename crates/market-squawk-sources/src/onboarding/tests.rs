use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::{SecretGeneration, SecretRef};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn available_persistence_is_bound_to_exact_current_evidence() -> TestResult {
    for profile in built_in_provider_profiles()?.iter() {
        let persists = profile.rights().0.iter().any(|right| {
            right.operation() == DataUseOperation::Persist
                && right.admission() == OperationAdmission::Admitted
        });
        if profile.release_state() == ProfileReleaseState::Available && persists {
            let evidence = profile
                .persistence_evidence()
                .ok_or("available persistence has no selected evidence")?;
            assert!(evidence.content_digest().is_some());
            assert!(!evidence.refresh_required());
            if profile.id() == "treasury.fiscal-data" {
                assert_eq!(evidence.source_id(), "DOC-031");
            }
        }
    }
    Ok(())
}

#[test]
fn provider_onboarding_authority_lifecycle_requires_exact_generation_and_renewal() -> TestResult {
    let capability_v1 = capability(1)?;
    let mut registry = ProviderCapabilityRegistry::new();
    assert_eq!(
        registry.register(capability_v1.clone())?,
        CapabilityRegistrationOutcome::Inserted
    );
    assert_eq!(
        registry.register(capability_v1.clone())?,
        CapabilityRegistrationOutcome::Replay
    );
    assert!(matches!(
        registry.register(capability(3)?),
        Err(ProviderCapabilityError::RevisionGap)
    ));

    let narrowed = registry.narrow_current(
        capability_v1.surface_id(),
        RuntimeCapabilityObservation::try_new(
            SetupMode::ManualApiKeyImport,
            authority_set(&["account.read"])?,
            true,
        )?,
    )?;
    assert_eq!(
        narrowed.maximum_authority(),
        &authority_set(&["account.read"])?
    );
    assert!(matches!(
        registry.narrow_current(
            capability_v1.surface_id(),
            RuntimeCapabilityObservation::try_new(
                SetupMode::OAuthDevice,
                authority_set(&["account.read", "account.write"])?,
                true,
            )?,
        ),
        Err(ProviderCapabilityError::RuntimeBroadening)
    ));

    let requested = authority_set(&["account.read"])?;
    let mut lifecycle = OnboardingLifecycle::reserve(&capability_v1, requested.clone())?;
    let observed_at = Timestamp::from_unix_nanos(100);
    assert_eq!(lifecycle.state(), OnboardingState::UserActionRequired);
    assert_eq!(
        lifecycle.candidate_generation(),
        Some(SecretGeneration::new(1)?)
    );
    assert!(matches!(
        lifecycle.apply(
            &capability_v1,
            OnboardingEvent::Activate {
                generation: Some(SecretGeneration::new(1)?),
            },
            observed_at,
        ),
        Err(OnboardingStateError::InvalidTransition)
    ));

    let generation_one = SecretGeneration::new(1)?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::CredentialStored {
            reference: secret_ref(1, 'a')?,
        },
        observed_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::AuthorityVerified {
            verification: Box::new(verified_authority(&capability_v1, requested.clone(), 10)?),
        },
        observed_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::RightsAdmitted {
            generation: Some(generation_one),
            decision_digest: digest(41),
        },
        observed_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::RatePolicyAdmitted {
            generation: Some(generation_one),
            policy_digest: capability_v1.rate_policy().evidence_digest(),
        },
        observed_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::RuntimeVerified {
            generation: Some(generation_one),
            evidence_digest: digest(42),
        },
        observed_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::Activate {
            generation: Some(generation_one),
        },
        observed_at,
    )?;
    assert_eq!(lifecycle.state(), OnboardingState::ActiveScoped);
    assert_eq!(lifecycle.active_generation(), Some(generation_one));

    let renewal_at = lifecycle
        .generation_verification(generation_one)
        .and_then(AuthorityVerification::expires_at)
        .ok_or("generation one verification omitted its renewal boundary")?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::RenewalRequired {
            generation: generation_one,
            expires_at: renewal_at,
            evidence_digest: digest(40),
        },
        renewal_at,
    )?;
    assert_eq!(lifecycle.state(), OnboardingState::RenewalRequired);

    let generation_two = SecretGeneration::new(2)?;
    let rotation_at = Timestamp::from_unix_nanos(
        renewal_at
            .unix_nanos()
            .checked_add(1)
            .ok_or("rotation timestamp overflow")?,
    );
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::BeginRotation {
            candidate_generation: generation_two,
            operation_owner: Some(SourceIdentifier::try_from("test-rotation-generation-2")?),
            deadline_at: Some(Timestamp::from_unix_nanos(2_000)),
            retry_budget: 1,
        },
        renewal_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::CredentialStored {
            reference: secret_ref(2, 'b')?,
        },
        rotation_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::AuthorityVerified {
            verification: Box::new(verified_authority(
                &capability_v1,
                requested,
                rotation_at.unix_nanos(),
            )?),
        },
        rotation_at,
    )?;
    for event in [
        OnboardingEvent::RightsAdmitted {
            generation: Some(generation_two),
            decision_digest: digest(43),
        },
        OnboardingEvent::RatePolicyAdmitted {
            generation: Some(generation_two),
            policy_digest: capability_v1.rate_policy().evidence_digest(),
        },
        OnboardingEvent::RuntimeVerified {
            generation: Some(generation_two),
            evidence_digest: digest(44),
        },
    ] {
        lifecycle.apply(&capability_v1, event, rotation_at)?;
    }
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::Cutover {
            prior_generation: generation_one,
            candidate_generation: generation_two,
        },
        rotation_at,
    )?;
    assert_eq!(lifecycle.active_generation(), Some(generation_two));
    assert_eq!(
        lifecycle.generation_state(generation_one),
        Some(CredentialGenerationState::SupersededRetained)
    );

    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::RemoteRevocation {
            generation: generation_one,
            outcome: RemoteRevocationOutcome::Unsupported,
            evidence_digest: digest(45),
        },
        rotation_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::LocalDeletion {
            generation: generation_one,
            outcome: LocalDeletionOutcome::Deleted,
        },
        rotation_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::Retire {
            generation: generation_one,
        },
        rotation_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::Tombstone {
            generation: generation_one,
        },
        rotation_at,
    )?;
    assert_eq!(
        lifecycle.generation_state(generation_one),
        Some(CredentialGenerationState::Tombstoned)
    );
    assert_eq!(lifecycle.state(), OnboardingState::ActiveScoped);
    assert_eq!(
        lifecycle.generation_remote_revocation(generation_one),
        Some(RemoteRevocationOutcome::Unsupported)
    );
    assert_eq!(
        lifecycle.generation_local_deletion(generation_one),
        Some(LocalDeletionOutcome::Deleted)
    );
    let active_reference = secret_ref(2, 'b')?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::ActivationQuarantined {
            evidence_digest: digest(46),
        },
        rotation_at,
    )?;
    assert_eq!(lifecycle.state(), OnboardingState::CleanupRequired);
    assert_eq!(lifecycle.active_generation(), None);
    assert_eq!(lifecycle.candidate_generation(), None);
    assert_eq!(
        lifecycle.generation_state(generation_two),
        Some(CredentialGenerationState::CleanupRequired)
    );
    assert_eq!(
        lifecycle.generation_reference(generation_two),
        Some(&active_reference)
    );
    Ok(())
}

#[test]
fn provider_onboarding_authority_rate_policies_are_explicit_and_fail_closed() -> TestResult {
    let profiles = built_in_provider_profiles()?;
    for profile in profiles.iter() {
        let descriptor = profile.capability().rate_policy();
        let policy = descriptor
            .enforcement_policy()
            .ok_or("current profile omitted rate enforcement")?;
        assert!(!policy.scope().as_source_identifier().as_str().is_empty());
        assert!(policy.window_count() >= 1);
        assert!(policy.max_concurrent() >= 1);
        assert!(descriptor.enforcement_revision().is_some());
        assert!(descriptor.endpoint_class().is_some());
        assert!(descriptor.scope_evidence_digest().is_some());
        assert!(descriptor.unknown_is_conservative());
    }

    assert!(capability(1)?.rate_policy().enforcement_policy().is_none());

    let bls = profiles
        .get("bls.v2-registered")
        .ok_or("missing BLS v2 profile")?
        .capability()
        .rate_policy()
        .enforcement_policy()
        .ok_or("BLS v2 omitted rate enforcement")?;
    assert_eq!(bls.window_count(), 2);
    assert_eq!(
        bls.window(0)
            .map(|window| (window.requests_per_window(), window.window_nanos())),
        Some((50, 10_000_000_000))
    );
    assert_eq!(
        bls.window(1)
            .map(|window| (window.requests_per_window(), window.window_nanos())),
        Some((500, 86_400_000_000_000))
    );
    Ok(())
}

fn capability(revision: u64) -> TestResult<ProviderCapability> {
    Ok(ProviderCapability::try_new(ProviderCapabilityInput {
        surface_id: SourceIdentifier::try_from("provider.private-account")?,
        revision: ProviderCapabilityRevision::new(revision)?,
        setup_mode: SetupMode::ManualApiKeyImport,
        official_entry_uri: "https://provider.example.test/settings/api".to_owned(),
        human_boundary: HumanBoundary::ProviderControlled,
        credential_kind: CredentialKind::ApiKey,
        minimum_authority: authority_set(&["account.read"])?,
        maximum_authority: authority_set(&["account.read", "portfolio.read"])?,
        verifier_revision: SourceIdentifier::try_from("provider-key-info-v1")?,
        rate_policy: RatePolicyDescriptor::try_new(
            SourceIdentifier::try_from("provider/private/rest/key-info/v1")?,
            digest(21),
            true,
        )?,
        rights_state: RightsAdmissionState::Pending,
        lifecycle_support: LifecycleSupport::new(true, false, true),
        evidence: vec![EvidenceBinding::new(
            SourceIdentifier::try_from("DOC-TEST-001")?,
            digest(31),
        )],
        refresh_trigger: SourceIdentifier::try_from("provider-private")?,
    })?)
}

fn verified_authority(
    capability: &ProviderCapability,
    requested: AuthoritySet,
    verified_at: i64,
) -> TestResult<AuthorityVerification> {
    Ok(AuthorityVerification::try_new(
        capability,
        AuthorityVerificationInput {
            requested: requested.clone(),
            observed: requested,
            restrictions_digest: digest(51),
            bindings: AuthorityBindings::new(
                Some(digest(52)),
                Some(digest(53)),
                Some(digest(54)),
                Some(digest(55)),
            ),
            verified_at: Timestamp::from_unix_nanos(verified_at),
            expires_at: Some(Timestamp::from_unix_nanos(
                verified_at
                    .checked_add(1_000)
                    .ok_or("verification expiry overflow")?,
            )),
            verifier_revision: SourceIdentifier::try_from("provider-key-info-v1")?,
            assurance_limitation: SourceIdentifier::try_from("provider-reported-authority")?,
            evidence_digest: digest(56),
        },
    )?)
}

fn authority_set(values: &[&str]) -> TestResult<AuthoritySet> {
    let values = values
        .iter()
        .map(|value| SourceIdentifier::try_from(*value))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AuthoritySet::try_new(values)?)
}

fn secret_ref(generation: u64, fill: char) -> TestResult<SecretRef> {
    Ok(serde_json::from_value(serde_json::json!({
        "version": 1,
        "backend": "encrypted_file",
        "locator": fill.to_string().repeat(64),
        "generation": generation,
    }))?)
}

const fn digest(byte: u8) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
}
