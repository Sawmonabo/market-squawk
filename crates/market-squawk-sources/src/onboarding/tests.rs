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
fn capabilities_only_narrow_and_lifecycle_requires_exact_generation_authority() -> TestResult {
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

    let generation_two = SecretGeneration::new(2)?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::BeginRotation {
            candidate_generation: generation_two,
        },
        observed_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::CredentialStored {
            reference: secret_ref(2, 'b')?,
        },
        observed_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::AuthorityVerified {
            verification: Box::new(verified_authority(&capability_v1, requested, 20)?),
        },
        observed_at,
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
        lifecycle.apply(&capability_v1, event, observed_at)?;
    }
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::Cutover {
            prior_generation: generation_one,
            candidate_generation: generation_two,
        },
        observed_at,
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
        observed_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::LocalDeletion {
            generation: generation_one,
            outcome: LocalDeletionOutcome::Deleted,
        },
        observed_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::Retire {
            generation: generation_one,
        },
        observed_at,
    )?;
    lifecycle.apply(
        &capability_v1,
        OnboardingEvent::Tombstone {
            generation: generation_one,
        },
        observed_at,
    )?;
    assert_eq!(
        lifecycle.generation_state(generation_one),
        Some(CredentialGenerationState::Tombstoned)
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
            expires_at: Some(Timestamp::from_unix_nanos(1_000)),
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
