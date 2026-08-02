use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::{SecretGeneration, SecretRef};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn available_persistence_is_bound_to_exact_current_evidence() -> TestResult {
    let profiles = built_in_provider_profiles()?;
    for profile in profiles.iter() {
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
    let fred = profiles
        .get("fred-alfred.api-v1-v2")
        .ok_or("missing FRED/ALFRED profile")?;
    assert_eq!(fred.zero_fee(), ZeroFeeStatus::Confirmed);
    assert_eq!(fred.release_state(), ProfileReleaseState::RightsLimited);
    assert_eq!(
        fred.capability().rights_state(),
        RightsAdmissionState::Pending
    );
    assert_eq!(fred.capability().revision().get(), 5);
    assert_eq!(
        fred.capability_history()
            .map(|capability| capability.revision().get())
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
    assert!(
        fred.capability_history()
            .filter(|capability| capability.revision().get() < 4)
            .all(|capability| capability.rights_state() == RightsAdmissionState::Blocked)
    );
    assert!(fred.persistence_evidence().is_none());
    let fred_probe_policy = fred
        .probe()
        .endpoint_policy()
        .ok_or("FRED credential probe omitted its endpoint policy")?;
    assert!(
        fred_probe_policy
            .authorize_request(
                "https://api.stlouisfed.org/fred/series?series_id=UNRATE&file_type=json&api_key=0123456789abcdef0123456789abcdef"
            )
            .is_ok()
    );
    assert!(
        fred_probe_policy
            .authorize_request(
                "https://api.stlouisfed.org/fred/series?series_id=GDP&file_type=json&api_key=0123456789abcdef0123456789abcdef"
            )
            .is_err()
    );
    for source in [
        "MSQ-FRED-ALFRED-SELF-HOSTED-AUTHORITY-2026-07-26",
        "MSQ-FRED-RIGHTS-MANIFEST-2026-07-26",
    ] {
        assert!(
            fred.capability()
                .evidence()
                .iter()
                .any(|binding| binding.source_id().as_str() == source)
        );
    }
    let revision_four = fred
        .capability_history()
        .find(|capability| capability.revision().get() == 4)
        .ok_or("missing immutable FRED/ALFRED revision 4")?;
    let revision_four_source = concat!("MSQ-FRED-ALFRED-LOCAL-", "FIRST-AUTHORITY-2026-07-26");
    assert!(
        revision_four
            .evidence()
            .iter()
            .any(|binding| { binding.source_id().as_str() == revision_four_source })
    );
    assert_eq!(
        revision_four.content_digest().bytes(),
        [
            0x63, 0xc7, 0x72, 0x79, 0x5a, 0x8e, 0x54, 0xd7, 0x21, 0x5f, 0xd0, 0x39, 0x5b, 0x41,
            0xc9, 0x75, 0x67, 0xa8, 0x1d, 0x95, 0x39, 0x5b, 0x8f, 0xb9, 0x36, 0xd3, 0x2b, 0x00,
            0x90, 0x28, 0x4d, 0xd9,
        ]
    );
    for (profile_id, evidence_source, evidence_digest) in [
        (
            "sec.edgar-public",
            "MSQ-SEC-EDGAR-PUBLIC-API-AUTHORITY-2026-07-26",
            EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                [
                    0xf4, 0x25, 0x65, 0x04, 0x19, 0x56, 0xc1, 0x33, 0x45, 0xae, 0xc3, 0xa3, 0xb5,
                    0x5e, 0x52, 0x83, 0x4d, 0xc1, 0x5a, 0x78, 0x97, 0xe9, 0x26, 0xf6, 0x90, 0xc7,
                    0x56, 0xd1, 0x0d, 0x2f, 0x4a, 0x80,
                ],
            ),
        ),
        (
            "bls.v1-unregistered",
            "MSQ-BLS-PUBLIC-V1-AUTHORITY-2026-07-26",
            EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                [
                    0x8c, 0xd0, 0x23, 0x7b, 0x36, 0x23, 0x23, 0x79, 0x58, 0x65, 0x10, 0xcc, 0x5c,
                    0x3b, 0xd3, 0x7d, 0x6c, 0x64, 0xf9, 0x7b, 0x89, 0x79, 0xab, 0x4a, 0x23, 0xa7,
                    0x80, 0x28, 0x1a, 0x08, 0xf1, 0x82,
                ],
            ),
        ),
    ] {
        let profile = profiles
            .get(profile_id)
            .ok_or("missing public provider profile")?;
        assert_eq!(profile.zero_fee(), ZeroFeeStatus::Confirmed);
        assert_eq!(profile.release_state(), ProfileReleaseState::Available);
        assert_eq!(profile.capability().revision().get(), 4);
        assert_eq!(
            profile
                .capability_history()
                .map(|capability| capability.revision().get())
                .collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
        let persistence_evidence = profile
            .persistence_evidence()
            .ok_or("public profile omitted persistence evidence")?;
        assert_eq!(persistence_evidence.source_id(), evidence_source);
        assert_eq!(persistence_evidence.content_digest(), Some(evidence_digest));
        assert!(!persistence_evidence.refresh_required());
        assert!(profile.capability().evidence().iter().any(|binding| {
            binding.source_id().as_str() == evidence_source && binding.digest() == evidence_digest
        }));
    }
    Ok(())
}

#[test]
fn treasury_daily_rates_profile_binds_complete_public_release_authority() -> TestResult {
    let profiles = built_in_provider_profiles()?;
    let profile = profiles
        .get("treasury.daily-rates-xml")
        .ok_or("missing Treasury daily-rates profile")?;
    let decision_source = "MSQ-TREASURY-DAILY-RATES-RELEASE-AUTHORITY-2026-07-26";
    let decision_digest = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [
            0x43, 0x17, 0x26, 0xc4, 0x84, 0x3c, 0x75, 0x7b, 0xe6, 0x5e, 0x79, 0x8c, 0x64, 0x9c,
            0x50, 0xfb, 0x14, 0x88, 0x1c, 0xc5, 0xfa, 0x05, 0xe3, 0x7c, 0xce, 0xd7, 0x68, 0x8e,
            0x1d, 0x5e, 0x7f, 0x92,
        ],
    );

    assert_eq!(profile.zero_fee(), ZeroFeeStatus::Confirmed);
    assert_eq!(profile.release_state(), ProfileReleaseState::Available);
    assert_eq!(
        profile.activation_mode(),
        ProfileActivationMode::NoCredential
    );
    assert_eq!(
        profile.requirements(),
        (
            Requirement::NotRequired,
            Requirement::NotRequired,
            Requirement::NotRequired,
        )
    );
    let (coverage, quality) = profile.coverage();
    assert_eq!(quality, market_squawk_domain::DataQuality::OfficialDelayed);
    for family in [
        "daily_treasury_yield_curve",
        "daily_treasury_bill_rates",
        "daily_treasury_long_term_rate",
        "daily_treasury_real_yield_curve",
        "daily_treasury_real_long_term",
    ] {
        assert!(coverage.contains(family));
    }
    let (rights, duties) = profile.rights();
    assert_eq!(rights.len(), 6);
    assert!(
        rights
            .iter()
            .all(|right| right.admission() == OperationAdmission::Admitted)
    );
    assert_eq!(
        duties,
        [
            "apply CC0 1.0 only to the five identified Treasury daily-rate datasets",
            "retain the dataset family, official URL, retrieval time, payload digest, provider \
             record identity, and publication and effective-time provenance",
            "exclude Treasury seals, trademarks, unrelated website media, and third-party \
             materials from this dataset-level admission",
        ]
    );

    let persistence_evidence = profile
        .persistence_evidence()
        .ok_or("Treasury daily rates omitted persistence evidence")?;
    assert_eq!(persistence_evidence.source_id(), decision_source);
    assert_eq!(persistence_evidence.content_digest(), Some(decision_digest));
    assert!(!persistence_evidence.refresh_required());

    assert_eq!(profile.capability().revision().get(), 4);
    assert_eq!(
        profile
            .capability_history()
            .map(|capability| capability.revision().get())
            .collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    let prior = profile
        .capability_history()
        .find(|capability| capability.revision().get() == 3)
        .ok_or("Treasury daily rates omitted historical revision 3")?;
    assert_eq!(prior.refresh_trigger().as_str(), "TREASURY-XML");
    assert!(
        prior
            .evidence()
            .iter()
            .all(|binding| binding.source_id().as_str() != decision_source)
    );
    assert_eq!(
        profile.capability().refresh_trigger().as_str(),
        "TREASURY-XML-AUTHORITY-2026-07-26"
    );
    assert_eq!(
        profile
            .capability()
            .evidence()
            .iter()
            .find(|binding| binding.source_id().as_str() == decision_source)
            .map(EvidenceBinding::digest),
        Some(decision_digest)
    );
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

    let fred = profiles
        .get("fred-alfred.api-v1-v2")
        .ok_or("missing FRED/ALFRED profile")?;
    let fred_budget = fred
        .capability()
        .rate_policy()
        .enforcement_policy()
        .ok_or("FRED/ALFRED omitted rate enforcement")?;
    assert_eq!(fred_budget.window_count(), 2);
    assert_eq!(
        fred_budget
            .window(0)
            .map(|window| (window.requests_per_window(), window.window_nanos())),
        Some((2, 1_000_000_000))
    );
    assert_eq!(
        fred_budget
            .window(1)
            .map(|window| (window.requests_per_window(), window.window_nanos())),
        Some((120, 60_000_000_000))
    );

    let sec = profiles
        .get("sec.edgar-public")
        .ok_or("missing SEC profile")?;
    let bls_public = profiles
        .get("bls.v1-unregistered")
        .ok_or("missing BLS v1 profile")?;
    let treasury_xml = profiles
        .get("treasury.daily-rates-xml")
        .ok_or("missing Treasury XML profile")?;
    let treasury_fiscal = profiles
        .get("treasury.fiscal-data")
        .ok_or("missing Treasury Fiscal Data profile")?;
    for profile in [sec, bls_public] {
        assert_eq!(profile.capability().revision().get(), 4);
        assert_eq!(
            profile
                .capability_history()
                .map(|capability| capability.revision().get())
                .collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
        assert!(profile.capability().evidence().iter().any(|binding| {
            binding.source_id().as_str() == "MSQ-PROVIDER-RELEASE-EVIDENCE-2026-07-25"
        }));
    }
    assert_eq!(fred.capability().revision().get(), 5);
    assert_eq!(
        fred.capability_history()
            .map(|capability| capability.revision().get())
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
    assert_eq!(treasury_fiscal.capability().revision().get(), 4);
    assert_eq!(
        treasury_fiscal
            .capability_history()
            .map(|capability| capability.revision().get())
            .collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    assert_eq!(
        treasury_fiscal.probe().endpoint(),
        Some(
            "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates?page%5Bsize%5D=1"
        )
    );
    assert_eq!(
        treasury_fiscal.capability().verifier_revision().as_str(),
        "treasury.fiscal-data.probe.v2"
    );
    assert_eq!(
        treasury_fiscal
            .capability_history()
            .find(|capability| capability.revision().get() == 3)
            .ok_or("Treasury Fiscal profile omitted revision 3")?
            .verifier_revision()
            .as_str(),
        "treasury.fiscal-data.probe.v1"
    );
    assert_eq!(
        bls_public.capability().setup_mode(),
        SetupMode::NoCredential
    );
    assert!(
        sec.evidence()
            .iter()
            .any(|evidence| evidence.source_id() == "SEC-FAIR-ACCESS")
    );
    assert!(
        bls_public
            .evidence()
            .iter()
            .any(|evidence| evidence.source_id() == "BLS-CONTENT-ORIGIN")
    );
    assert_eq!(
        treasury_xml.probe().endpoint(),
        Some(
            "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value=2025"
        )
    );
    assert_eq!(
        treasury_xml.capability().verifier_revision().as_str(),
        "treasury.daily-rates-xml.probe.v2"
    );

    let public_coinbase = profiles
        .get("coinbase.public-market-data")
        .ok_or("missing public Coinbase profile")?;
    let direct_coinbase = profiles
        .get("coinbase.exchange-direct-market-data")
        .ok_or("missing Coinbase Exchange Direct profile")?;
    assert_eq!(
        public_coinbase.coverage().1,
        market_squawk_domain::DataQuality::DirectUnverified
    );
    assert_eq!(
        direct_coinbase.coverage().1,
        market_squawk_domain::DataQuality::DirectVerified
    );
    assert_eq!(
        direct_coinbase.capability().maximum_authority().as_slice(),
        &[SourceIdentifier::try_from(
            "coinbase.exchange.market-data.read"
        )?]
    );
    assert_eq!(
        direct_coinbase.capability().credential_kind(),
        CredentialKind::ApiKeySecretPassphrase
    );
    let direct_capability_json: serde_json::Value =
        serde_json::from_slice(&direct_coinbase.capability().canonical_json()?)?;
    assert_eq!(
        direct_capability_json
            .get("credential_kind")
            .and_then(serde_json::Value::as_str),
        Some("api_key_secret_passphrase")
    );
    assert_eq!(direct_coinbase.capability().revision().get(), 3);
    let direct_history = direct_coinbase
        .capability_history()
        .map(|capability| (capability.revision().get(), capability.credential_kind()))
        .collect::<Vec<_>>();
    assert_eq!(
        direct_history,
        [
            (1, CredentialKind::ApiKey),
            (2, CredentialKind::ApiKey),
            (3, CredentialKind::ApiKeySecretPassphrase),
        ]
    );
    let composition_digest = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [
            0xdf, 0x90, 0xf3, 0xc1, 0x53, 0x0e, 0x9a, 0xc6, 0xd4, 0x4f, 0x89, 0x48, 0x00, 0x10,
            0x1f, 0xdf, 0x61, 0xf9, 0x3a, 0x8b, 0x93, 0x33, 0xd3, 0x25, 0x6c, 0xb5, 0x77, 0xd7,
            0x2f, 0x8e, 0x75, 0x90,
        ],
    );
    assert_eq!(
        direct_coinbase
            .capability()
            .evidence()
            .iter()
            .find(|binding| {
                binding.source_id().as_str() == "MSQ-COINBASE-DIRECT-COMPOSITION-AUDIT-2026-07-25"
            })
            .map(EvidenceBinding::digest),
        Some(composition_digest)
    );
    assert_eq!(
        direct_coinbase
            .capability()
            .rate_policy()
            .enforcement_revision()
            .map(ProviderCapabilityRevision::get),
        Some(2)
    );
    for source_id in ["CB-EXCHANGE-REST-RATE-LIMITS", "CB-EXCHANGE-WS-RATE-LIMITS"] {
        assert!(
            direct_coinbase
                .evidence()
                .iter()
                .any(|evidence| evidence.source_id() == source_id)
        );
    }
    assert_eq!(
        direct_coinbase.capability().rate_policy().evidence_digest(),
        composition_digest
    );
    assert_eq!(
        direct_coinbase
            .capability()
            .rate_policy()
            .scope_evidence_digest(),
        Some(composition_digest)
    );
    let direct_budget = direct_coinbase
        .capability()
        .rate_policy()
        .enforcement_policy()
        .ok_or("Coinbase Direct omitted rate enforcement")?;
    assert_eq!(
        direct_budget.scope().as_source_identifier().as_str(),
        "coinbase-exchange"
    );
    assert_eq!(
        direct_budget
            .scope()
            .authorization_account()
            .map(SourceIdentifier::as_str),
        Some("coinbase.exchange-direct.account-template")
    );
    assert_eq!(direct_budget.max_concurrent(), 2);
    assert_eq!(
        direct_budget
            .window(0)
            .map(|window| (window.requests_per_window(), window.window_nanos())),
        Some((8, 1_000_000_000))
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
