use market_squawk_domain::{
    CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{SecretGeneration, SecretRef};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};

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

    let alpaca = profiles
        .get("alpaca.basic-market-data")
        .ok_or("missing Alpaca Paper Only profile")?;
    assert_eq!(alpaca.release_state(), ProfileReleaseState::Available);
    assert_eq!(alpaca.capability().revision().get(), 4);
    assert_eq!(
        alpaca.capability().maximum_authority().as_slice(),
        &[SourceIdentifier::try_from("alpaca.market-data.read")?]
    );
    assert_eq!(
        alpaca
            .capability_history()
            .map(|capability| capability.revision().get())
            .collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    assert_eq!(
        alpaca.requirements(),
        (
            Requirement::RequiredProviderControlled,
            Requirement::RequiredProviderControlled,
            Requirement::NotRequired,
        )
    );
    assert!(alpaca.handoff().1.contains("set exactly to paper"));
    assert_eq!(
        alpaca
            .rights()
            .0
            .iter()
            .map(|right| right.admission())
            .collect::<Vec<_>>(),
        [
            OperationAdmission::Admitted,
            OperationAdmission::Admitted,
            OperationAdmission::Admitted,
            OperationAdmission::Admitted,
            OperationAdmission::Blocked,
            OperationAdmission::Blocked,
        ]
    );
    assert_eq!(
        alpaca
            .persistence_evidence()
            .map(ProfileEvidence::source_id),
        Some("MSQ-SELECTED-MARKET-DATA-ARCHITECTURE-2026-08-11")
    );

    for profile_id in [
        "kraken.spot-public-market-data",
        "kraken.spot-authenticated-level3-market-data",
    ] {
        let profile = profiles
            .get(profile_id)
            .ok_or("missing Kraken market-data profile")?;
        assert_eq!(profile.release_state(), ProfileReleaseState::Available);
        assert_eq!(
            profile.capability().rights_state(),
            RightsAdmissionState::AdmittedScoped
        );
        assert_eq!(
            profile
                .rights()
                .0
                .iter()
                .map(|right| right.admission())
                .collect::<Vec<_>>(),
            [
                OperationAdmission::Admitted,
                OperationAdmission::Admitted,
                OperationAdmission::Admitted,
                OperationAdmission::Admitted,
                OperationAdmission::Blocked,
                OperationAdmission::Blocked,
            ]
        );
        assert_eq!(
            profile
                .persistence_evidence()
                .map(ProfileEvidence::source_id),
            Some("MSQ-SELECTED-MARKET-DATA-ARCHITECTURE-2026-08-11")
        );
    }
    assert!(
        profiles
            .get("kraken.spot-public-market-data")
            .ok_or("missing public Kraken profile")?
            .coverage()
            .0
            .contains("books, and trades")
    );

    let nasdaq = profiles
        .get("nasdaq-trader-symbol-directory-reference")
        .ok_or("missing Nasdaq reference profile")?;
    assert_eq!(
        nasdaq.activation_mode(),
        ProfileActivationMode::NoCredential
    );
    assert_eq!(nasdaq.release_state(), ProfileReleaseState::RightsLimited);
    assert_eq!(nasdaq.capability().revision().get(), 3);
    assert!(nasdaq.persistence_evidence().is_none());
    assert!(nasdaq.coverage().0.contains("process-local reference only"));
    assert_eq!(
        nasdaq.probe().endpoint(),
        Some("https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt")
    );

    let board = profiles
        .get("federal-reserve-board.data-download-program")
        .ok_or("missing Federal Reserve Board profile")?;
    assert_eq!(board.release_state(), ProfileReleaseState::Available);
    assert_eq!(board.capability().revision().get(), 4);
    assert_eq!(
        board
            .capability_history()
            .map(|capability| capability.revision().get())
            .collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    assert_eq!(board.probe().transport(), ProbeTransport::HttpGet);
    assert_eq!(
        board.probe().endpoint(),
        Some(super::built_in_profiles::FEDERAL_RESERVE_BOARD_H15_PROBE_URL)
    );
    let board_policy = board
        .probe()
        .endpoint_policy()
        .ok_or("Board doctor omitted its exact endpoint policy")?;
    assert!(
        board_policy
            .authorize_request(super::built_in_profiles::FEDERAL_RESERVE_BOARD_H15_PROBE_URL)
            .is_ok()
    );
    assert!(
        board_policy
            .authorize_request(
                "https://www.federalreserve.gov/datadownload/Output.aspx?filetype=csv&label=include&lastobs=11&layout=seriescolumn&rel=H15&series=bf17364827e38702b42a58cf8eaa3f78&type=package"
            )
            .is_err()
    );
    assert_eq!(
        board.capability().verifier_revision().as_str(),
        "federal-reserve-board.data-download-program.probe.v2"
    );
    let board_revision_three = board
        .capability_history()
        .find(|capability| capability.revision().get() == 3)
        .ok_or("Board profile omitted immutable revision 3")?;
    assert_eq!(
        board_revision_three.verifier_revision().as_str(),
        "federal-reserve-board.data-download-program.probe.v1"
    );
    assert_eq!(
        board_revision_three.rate_policy().policy_id().as_str(),
        "federal-reserve-board.data-download-program.pending-rate-policy.v1"
    );
    assert_eq!(
        board.capability().rate_policy().policy_id().as_str(),
        "federal-reserve-board.data-download-program.rate-policy.v1"
    );
    assert!(board.coverage().0.contains("exact 11-series H.15"));
    assert_eq!(
        board.persistence_evidence().map(ProfileEvidence::source_id),
        Some("MSQ-SELECTED-MARKET-DATA-ARCHITECTURE-2026-08-11")
    );
    let board_budget = board
        .capability()
        .rate_policy()
        .enforcement_policy()
        .ok_or("Board profile omitted its application budget")?;
    assert_eq!(board_budget.max_concurrent(), 1);
    assert_eq!(
        board_budget
            .window(0)
            .map(|window| (window.requests_per_window(), window.window_nanos())),
        Some((1, 60_000_000_000))
    );

    for (profile_id, activation, release, setup, credential, coverage_marker, windows) in [
        (
            "schwab.trader-api-market-data",
            ProfileActivationMode::ManualSecretImport,
            ProfileReleaseState::RefreshRequired,
            SetupMode::ManualApiKeyImport,
            CredentialKind::ApiKeyPair,
            "provider-native read-only REST",
            &[(1, 60_000_000_000)][..],
        ),
        (
            "yahoo-finance.experimental-enrichment",
            ProfileActivationMode::NoCredential,
            ProfileReleaseState::Available,
            SetupMode::NoCredential,
            CredentialKind::None,
            "cookie/crumb HTTP",
            &[(1, 60_000_000_000)][..],
        ),
        (
            "iex.hist-feed-files",
            ProfileActivationMode::NoCredential,
            ProfileReleaseState::RefreshRequired,
            SetupMode::NoCredential,
            CredentialKind::None,
            "bounded cold-job",
            &[(1, 60_000_000_000)][..],
        ),
        (
            "occ.options-reference",
            ProfileActivationMode::NoCredential,
            ProfileReleaseState::RefreshRequired,
            SetupMode::NoCredential,
            CredentialKind::None,
            "selected/daily DLP",
            &[(1, 60_000_000_000)][..],
        ),
        (
            "cboe.options-reference",
            ProfileActivationMode::NoCredential,
            ProfileReleaseState::RefreshRequired,
            SetupMode::NoCredential,
            CredentialKind::None,
            "four-file request plans",
            &[(1, 60_000_000_000)][..],
        ),
        (
            "bea.api-data",
            ProfileActivationMode::ManualSecretImport,
            ProfileReleaseState::RefreshRequired,
            SetupMode::ManualApiKeyImport,
            CredentialKind::ApiKey,
            "100 requests, 100 MB, and 30 errors per minute",
            &[(60, 60_000_000_000)][..],
        ),
        (
            "census.data-api",
            ProfileActivationMode::ManualSecretImport,
            ProfileReleaseState::RefreshRequired,
            SetupMode::ManualApiKeyImport,
            CredentialKind::ApiKey,
            "400 requests per day",
            &[(1, 1_000_000_000), (400, 86_400_000_000_000)][..],
        ),
        (
            "eia.api-v2",
            ProfileActivationMode::ManualSecretImport,
            ProfileReleaseState::RefreshRequired,
            SetupMode::ManualApiKeyImport,
            CredentialKind::ApiKey,
            "5,000-row maximum",
            &[(1, 1_000_000_000)][..],
        ),
        (
            "tiingo.starter-eod-nav",
            ProfileActivationMode::ManualSecretImport,
            ProfileReleaseState::Available,
            SetupMode::ManualApiKeyImport,
            CredentialKind::ApiKey,
            "500 unique symbols/month",
            &[(40, 3_600_000_000_000), (800, 86_400_000_000_000)][..],
        ),
    ] {
        let profile = profiles
            .get(profile_id)
            .ok_or("missing selected pending provider profile")?;
        assert_eq!(profile.activation_mode(), activation);
        assert_eq!(profile.release_state(), release);
        assert_eq!(profile.capability().revision().get(), 3);
        assert_eq!(profile.capability().setup_mode(), setup);
        assert_eq!(profile.capability().credential_kind(), credential);
        assert_eq!(
            profile.capability().rights_state(),
            RightsAdmissionState::AdmittedScoped
        );
        assert_eq!(profile.probe().transport(), ProbeTransport::Local);
        assert!(profile.probe().endpoint().is_none());
        assert_eq!(
            profile
                .persistence_evidence()
                .map(ProfileEvidence::source_id),
            Some("MSQ-SELECTED-MARKET-DATA-ARCHITECTURE-2026-08-11")
        );
        assert!(profile.coverage().0.contains(coverage_marker));
        assert!(profile.capability().evidence().iter().any(|binding| {
            binding.source_id().as_str() == "MSQ-SELECTED-MARKET-DATA-ARCHITECTURE-2026-08-11"
        }));
        assert_eq!(
            profile
                .rights()
                .0
                .iter()
                .map(|right| right.admission())
                .collect::<Vec<_>>(),
            [
                OperationAdmission::Admitted,
                OperationAdmission::Admitted,
                OperationAdmission::Admitted,
                OperationAdmission::Admitted,
                OperationAdmission::Blocked,
                OperationAdmission::Blocked,
            ]
        );
        let budget = profile
            .capability()
            .rate_policy()
            .enforcement_policy()
            .ok_or("selected pending profile omitted its structural budget")?;
        assert_eq!(budget.window_count(), windows.len());
        for (index, expected) in windows.iter().enumerate() {
            assert_eq!(
                budget
                    .window(index)
                    .map(|window| (window.requests_per_window(), window.window_nanos())),
                Some(*expected)
            );
        }
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
            evidence: RuntimeVerificationEvidence::digest_v1(digest(42))?,
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
            evidence: RuntimeVerificationEvidence::digest_v1(digest(44))?,
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
fn alpaca_doctor_receipt_closes_contract_graph_and_same_generation_renewal() -> TestResult {
    let profiles = built_in_provider_profiles()?;
    let profile = profiles
        .get(ALPACA_BASIC_MARKET_DATA_SURFACE_ID)
        .ok_or("missing Alpaca Paper/IEX profile")?;
    let capability = profile.capability();
    let requested_authority = capability.maximum_authority().clone();
    assert_eq!(
        requested_authority.as_slice(),
        &[SourceIdentifier::try_from("alpaca.market-data.read")?]
    );

    let session_identifier = SourceIdentifier::try_from("018f76a0-3d3b-7d62-a60b-0242ac120002")?;
    let public_configuration_digest = digest(70);
    let principal_digest = digest(71);
    let generation = SecretGeneration::new(1)?;
    let mut lifecycle = OnboardingLifecycle::reserve_with_runtime_verification_context(
        capability,
        requested_authority.clone(),
        RuntimeVerificationContext::try_new(
            session_identifier.clone(),
            public_configuration_digest,
        )?,
    )?;
    assert_eq!(lifecycle.requested_authority(), &requested_authority);
    lifecycle.apply(
        capability,
        OnboardingEvent::CredentialStored {
            reference: secret_ref(1, 'c')?,
        },
        Timestamp::from_unix_nanos(100),
    )?;
    lifecycle.apply(
        capability,
        OnboardingEvent::AuthorityVerified {
            verification: Box::new(AuthorityVerification::try_new(
                capability,
                AuthorityVerificationInput {
                    requested: requested_authority.clone(),
                    observed: requested_authority,
                    restrictions_digest: digest(72),
                    bindings: AuthorityBindings::new(None, None, None, Some(principal_digest)),
                    verified_at: Timestamp::from_unix_nanos(100),
                    expires_at: None,
                    verifier_revision: capability.verifier_revision().clone(),
                    assurance_limitation: SourceIdentifier::try_from(
                        "alpaca-paper-iex-market-data-only",
                    )?,
                    evidence_digest: digest(73),
                },
            )?),
        },
        Timestamp::from_unix_nanos(100),
    )?;
    for event in [
        OnboardingEvent::RightsAdmitted {
            generation: Some(generation),
            decision_digest: profile.rights_decision_digest(),
        },
        OnboardingEvent::RatePolicyAdmitted {
            generation: Some(generation),
            policy_digest: capability.rate_policy().evidence_digest(),
        },
    ] {
        lifecycle.apply(capability, event, Timestamp::from_unix_nanos(100))?;
    }

    let initial_input = alpaca_doctor_input(
        profile,
        &session_identifier,
        public_configuration_digest,
        generation,
        principal_digest,
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(2_000),
        None,
    )?;
    let initial_receipt = AlpacaPaperIexDoctorReceiptV1::try_new(initial_input.clone())?;
    let initial_expires_at = initial_receipt.exclusive_expires_at();
    assert!(initial_receipt.admits_source_start());
    assert_eq!(
        initial_receipt.provider_observation_sha256(),
        initial_input.provider_observation_sha256
    );

    let mut after_hours_input = initial_input.clone();
    after_hours_input.quote.disposition = RuntimeCapabilityDisposition::Degraded;
    let after_hours_quote = after_hours_input
        .quote
        .observation
        .as_mut()
        .ok_or("after-hours quote observation omitted")?;
    after_hours_quote.bid_price = Some(Decimal::ZERO);
    after_hours_quote.bid_size = Some(0);
    after_hours_input.batch.disposition = RuntimeCapabilityDisposition::Degraded;
    after_hours_input
        .batch
        .observation
        .as_mut()
        .ok_or("after-hours batch observation omitted")?
        .effective_cardinality = 49;
    super::runtime_verification::seal_test_alpaca_provider_observation(&mut after_hours_input)?;
    let after_hours_receipt = AlpacaPaperIexDoctorReceiptV1::try_new(after_hours_input.clone())?;
    assert_eq!(
        after_hours_receipt.input().quote.disposition,
        RuntimeCapabilityDisposition::Degraded
    );
    assert_eq!(
        after_hours_receipt.input().batch.disposition,
        RuntimeCapabilityDisposition::Degraded
    );
    assert!(after_hours_receipt.admits_source_start());

    let mut mismatched_quote = after_hours_input.clone();
    mismatched_quote
        .quote
        .observation
        .as_mut()
        .ok_or("mismatched quote observation omitted")?
        .bid_size = Some(1);
    super::runtime_verification::seal_test_alpaca_provider_observation(&mut mismatched_quote)?;
    assert!(matches!(
        AlpacaPaperIexDoctorReceiptV1::try_new(mismatched_quote),
        Err(RuntimeVerificationEvidenceError::InvalidEvidence)
    ));

    let mut crossed_quote = initial_input.clone();
    crossed_quote.quote.disposition = RuntimeCapabilityDisposition::Degraded;
    crossed_quote
        .quote
        .observation
        .as_mut()
        .ok_or("crossed quote observation omitted")?
        .bid_price = Some(Decimal::new(10_002, 2));
    super::runtime_verification::seal_test_alpaca_provider_observation(&mut crossed_quote)?;
    assert!(matches!(
        AlpacaPaperIexDoctorReceiptV1::try_new(crossed_quote),
        Err(RuntimeVerificationEvidenceError::InvalidEvidence)
    ));

    let mut invalid_batch = after_hours_input.clone();
    invalid_batch
        .batch
        .observation
        .as_mut()
        .ok_or("invalid batch observation omitted")?
        .invalid_count = 1;
    super::runtime_verification::seal_test_alpaca_provider_observation(&mut invalid_batch)?;
    assert!(!AlpacaPaperIexDoctorReceiptV1::try_new(invalid_batch)?.admits_source_start());

    let mut missing_batch = after_hours_input.clone();
    let missing_batch_observation = missing_batch
        .batch
        .observation
        .as_mut()
        .ok_or("missing batch observation omitted")?;
    missing_batch_observation.returned_count = 49;
    missing_batch_observation.missing_count = 1;
    super::runtime_verification::seal_test_alpaca_provider_observation(&mut missing_batch)?;
    assert!(!AlpacaPaperIexDoctorReceiptV1::try_new(missing_batch)?.admits_source_start());

    let mut degraded_stream = after_hours_input.clone();
    degraded_stream.stream.disposition = RuntimeCapabilityDisposition::Degraded;
    super::runtime_verification::seal_test_alpaca_provider_observation(&mut degraded_stream)?;
    assert!(!AlpacaPaperIexDoctorReceiptV1::try_new(degraded_stream)?.admits_source_start());

    let mut forged_provider_observation = initial_input.clone();
    forged_provider_observation.provider_observation_sha256 = digest(79);
    assert!(matches!(
        AlpacaPaperIexDoctorReceiptV1::try_new(forged_provider_observation),
        Err(RuntimeVerificationEvidenceError::InvalidEvidence)
    ));
    let mut caller_selected_expiry = initial_input.clone();
    caller_selected_expiry.exclusive_expires_at = caller_selected_expiry
        .exclusive_expires_at
        .checked_add_nanos(1)?;
    assert!(matches!(
        AlpacaPaperIexDoctorReceiptV1::try_new(caller_selected_expiry),
        Err(RuntimeVerificationEvidenceError::InvalidEvidence)
    ));
    assert_eq!(
        initial_receipt.doctor_revision().as_str(),
        "market-squawk.alpaca-paper-iex-doctor-implementation.v3"
    );
    let mut expected_contract = Sha256::new();
    expected_contract.update(b"market-squawk/alpaca-paper-iex-doctor-contract/v1\0");
    expected_contract.update(capability.revision().get().to_be_bytes());
    expected_contract.update(capability.content_digest().bytes());
    expected_contract.update(initial_receipt.doctor_revision().as_str().as_bytes());
    assert_eq!(
        initial_receipt.doctor_contract_digest(),
        EvidenceDigest::new(DigestAlgorithm::Sha256, expected_contract.finalize().into())
    );
    let mut serialized_receipt = serde_json::to_value(&initial_receipt)?;
    assert!(serialized_receipt["input"].get("doctor_revision").is_none());
    assert!(
        serialized_receipt["input"]
            .get("doctor_contract_digest")
            .is_none()
    );
    serialized_receipt["doctor_contract_digest"] = serde_json::to_value(digest(74))?;
    assert!(serde_json::from_value::<AlpacaPaperIexDoctorReceiptV1>(serialized_receipt).is_err());

    let mut disconnected_history = initial_input.clone();
    disconnected_history
        .historical
        .observation
        .as_mut()
        .ok_or("history observation omitted")?
        .pages[1]
        .request_page_token_digest = None;
    assert!(matches!(
        AlpacaPaperIexDoctorReceiptV1::try_new(disconnected_history),
        Err(RuntimeVerificationEvidenceError::InvalidEvidence)
    ));

    let mut mismatched_range = initial_input.clone();
    mismatched_range
        .calendar
        .observation
        .as_mut()
        .ok_or("calendar observation omitted")?
        .start_date = CalendarDate::new(2026, 8, 12)?;
    assert!(matches!(
        AlpacaPaperIexDoctorReceiptV1::try_new(mismatched_range),
        Err(RuntimeVerificationEvidenceError::InvalidEvidence)
    ));

    let mut mismatched_count = initial_input.clone();
    let calendar = mismatched_count
        .calendar
        .observation
        .as_mut()
        .ok_or("calendar observation omitted")?;
    calendar.session_count = 1;
    calendar.history_date_count = 1;
    calendar.matched_count = 1;
    assert!(matches!(
        AlpacaPaperIexDoctorReceiptV1::try_new(mismatched_count),
        Err(RuntimeVerificationEvidenceError::InvalidEvidence)
    ));

    let mut mismatched_dates = initial_input.clone();
    let calendar = mismatched_dates
        .calendar
        .observation
        .as_mut()
        .ok_or("calendar observation omitted")?;
    calendar.session_dates_digest = digest(75);
    calendar.history_dates_digest = digest(75);
    assert!(matches!(
        AlpacaPaperIexDoctorReceiptV1::try_new(mismatched_dates),
        Err(RuntimeVerificationEvidenceError::InvalidEvidence)
    ));

    let initial_digest = initial_receipt.receipt_sha256();
    lifecycle.apply(
        capability,
        OnboardingEvent::RuntimeVerified {
            generation: Some(generation),
            evidence: alpaca_runtime_evidence(initial_receipt),
        },
        Timestamp::from_unix_nanos(1_100),
    )?;
    let pending_successor = AlpacaPaperIexDoctorReceiptV1::try_new(alpaca_doctor_input(
        profile,
        &session_identifier,
        public_configuration_digest,
        generation,
        principal_digest,
        Timestamp::from_unix_nanos(1_200),
        Timestamp::from_unix_nanos(2_200),
        Some(initial_digest),
    )?)?;
    assert!(matches!(
        lifecycle.apply(
            capability,
            OnboardingEvent::RuntimeVerified {
                generation: Some(generation),
                evidence: alpaca_runtime_evidence(pending_successor),
            },
            Timestamp::from_unix_nanos(1_300),
        ),
        Err(OnboardingStateError::InvalidEvidence)
    ));
    assert_eq!(
        lifecycle.generation_runtime_digest(generation),
        Some(initial_digest)
    );
    lifecycle.apply(
        capability,
        OnboardingEvent::Activate {
            generation: Some(generation),
        },
        Timestamp::from_unix_nanos(1_300),
    )?;
    assert_eq!(lifecycle.state(), OnboardingState::ActiveScoped);

    let successor = AlpacaPaperIexDoctorReceiptV1::try_new(alpaca_doctor_input(
        profile,
        &session_identifier,
        public_configuration_digest,
        generation,
        principal_digest,
        Timestamp::from_unix_nanos(1_500),
        Timestamp::from_unix_nanos(2_500),
        Some(initial_digest),
    )?)?;
    assert!(matches!(
        lifecycle.apply(
            capability,
            OnboardingEvent::RuntimeVerified {
                generation: Some(generation),
                evidence: alpaca_runtime_evidence(successor.clone()),
            },
            Timestamp::from_unix_nanos(1_600),
        ),
        Err(OnboardingStateError::InvalidTransition)
    ));
    assert_eq!(lifecycle.state(), OnboardingState::ActiveScoped);

    lifecycle.apply(
        capability,
        OnboardingEvent::RenewalRequired {
            generation,
            expires_at: initial_expires_at,
            evidence_digest: digest(76),
        },
        initial_expires_at,
    )?;
    assert_eq!(lifecycle.state(), OnboardingState::RenewalRequired);
    let renewal_observed_at = initial_expires_at.checked_add_nanos(100)?;

    for rejected_input in [
        alpaca_doctor_input(
            profile,
            &session_identifier,
            public_configuration_digest,
            generation,
            principal_digest,
            Timestamp::from_unix_nanos(1_500),
            Timestamp::from_unix_nanos(2_500),
            Some(digest(77)),
        )?,
        alpaca_doctor_input(
            profile,
            &session_identifier,
            public_configuration_digest,
            generation,
            principal_digest,
            Timestamp::from_unix_nanos(1_000),
            Timestamp::from_unix_nanos(2_500),
            Some(initial_digest),
        )?,
        alpaca_doctor_input(
            profile,
            &session_identifier,
            public_configuration_digest,
            SecretGeneration::new(2)?,
            principal_digest,
            Timestamp::from_unix_nanos(1_500),
            Timestamp::from_unix_nanos(2_500),
            Some(initial_digest),
        )?,
        alpaca_doctor_input(
            profile,
            &session_identifier,
            public_configuration_digest,
            generation,
            digest(78),
            Timestamp::from_unix_nanos(1_500),
            Timestamp::from_unix_nanos(2_500),
            Some(initial_digest),
        )?,
    ] {
        let rejected = AlpacaPaperIexDoctorReceiptV1::try_new(rejected_input)?;
        assert!(
            lifecycle
                .apply(
                    capability,
                    OnboardingEvent::RuntimeVerified {
                        generation: Some(generation),
                        evidence: alpaca_runtime_evidence(rejected),
                    },
                    renewal_observed_at,
                )
                .is_err()
        );
        assert_eq!(lifecycle.state(), OnboardingState::RenewalRequired);
        assert_eq!(
            lifecycle.generation_runtime_digest(generation),
            Some(initial_digest)
        );
    }

    let successor_digest = successor.receipt_sha256();
    assert_eq!(
        lifecycle.apply(
            capability,
            OnboardingEvent::RuntimeVerified {
                generation: Some(generation),
                evidence: alpaca_runtime_evidence(successor),
            },
            renewal_observed_at,
        )?,
        OnboardingState::ActiveScoped
    );
    assert_eq!(lifecycle.active_generation(), Some(generation));
    assert_eq!(
        lifecycle.generation_runtime_digest(generation),
        Some(successor_digest)
    );
    assert!(lifecycle.active_generation_is_fully_admitted(capability, renewal_observed_at)?);
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

fn alpaca_doctor_input(
    profile: &ProviderOnboardingProfile,
    session_identifier: &SourceIdentifier,
    public_configuration_digest: EvidenceDigest,
    generation: SecretGeneration,
    principal_digest: EvidenceDigest,
    verified_at: Timestamp,
    exclusive_expires_at: Timestamp,
    predecessor_digest: Option<EvidenceDigest>,
) -> TestResult<AlpacaPaperIexDoctorReceiptInput> {
    let capability = profile.capability();
    let received_at = |offset: i64| {
        Timestamp::from_unix_nanos(
            verified_at
                .unix_nanos()
                .checked_sub(offset)
                .expect("test doctor timestamp underflow"),
        )
    };
    let history_dates_digest = digest(140);
    let continuation_digest = digest(141);
    let start_date = CalendarDate::new(2026, 8, 11)?;
    let end_date = CalendarDate::new(2026, 8, 12)?;
    let additional_capabilities = [
        (
            AlpacaDoctorAdditionalCapability::OptionsRest,
            RuntimeCapabilityDisposition::NotProbed,
        ),
        (
            AlpacaDoctorAdditionalCapability::OptionsStream,
            RuntimeCapabilityDisposition::NotProbed,
        ),
        (
            AlpacaDoctorAdditionalCapability::FixedIncome,
            RuntimeCapabilityDisposition::NotProbed,
        ),
        (
            AlpacaDoctorAdditionalCapability::CorporateActions,
            RuntimeCapabilityDisposition::NotProbed,
        ),
        (
            AlpacaDoctorAdditionalCapability::Sip,
            RuntimeCapabilityDisposition::Unavailable,
        ),
        (
            AlpacaDoctorAdditionalCapability::Nbbo,
            RuntimeCapabilityDisposition::Unavailable,
        ),
        (
            AlpacaDoctorAdditionalCapability::Opra,
            RuntimeCapabilityDisposition::Unavailable,
        ),
        (
            AlpacaDoctorAdditionalCapability::PriceLevelDepth,
            RuntimeCapabilityDisposition::Unavailable,
        ),
        (
            AlpacaDoctorAdditionalCapability::OrderLevelDepth,
            RuntimeCapabilityDisposition::Unavailable,
        ),
        (
            AlpacaDoctorAdditionalCapability::BrokerageAccount,
            RuntimeCapabilityDisposition::Unavailable,
        ),
        (
            AlpacaDoctorAdditionalCapability::Positions,
            RuntimeCapabilityDisposition::Unavailable,
        ),
        (
            AlpacaDoctorAdditionalCapability::Orders,
            RuntimeCapabilityDisposition::Unavailable,
        ),
        (
            AlpacaDoctorAdditionalCapability::Trading,
            RuntimeCapabilityDisposition::Unavailable,
        ),
    ]
    .into_iter()
    .enumerate()
    .map(
        |(index, (capability, disposition))| AlpacaDoctorCapabilityEvidence {
            capability,
            disposition,
            disposition_evidence_digest: digest(
                u8::try_from(180 + index).expect("test capability digest index overflow"),
            ),
        },
    )
    .collect::<Vec<_>>()
    .into_boxed_slice();

    let mut input = AlpacaPaperIexDoctorReceiptInput {
        provider_observation_origin: AlpacaPaperIexDoctorReceiptV1::provider_observed_origin()?,
        provider_observation_sha256: digest(79),
        surface_id: capability.surface_id().clone(),
        session_identifier: session_identifier.clone(),
        generation,
        realm: AlpacaDoctorCredentialRealm::Paper,
        market_data_principal_sha256: principal_digest,
        capability_revision: capability.revision(),
        capability_digest: capability.content_digest(),
        public_configuration_digest,
        rights_decision_digest: profile.rights_decision_digest(),
        rate_policy_digest: capability.rate_policy().evidence_digest(),
        data_quality: DataQuality::DirectUnverified,
        quote: available_probe(
            AlpacaDoctorQuoteObservation {
                http: alpaca_http_evidence(80, received_at(50)),
                semantic_result_digest: digest(83),
                quote_timestamp: Some(received_at(51)),
                bid_price: Some(Decimal::new(10_000, 2)),
                ask_price: Some(Decimal::new(10_001, 2)),
                bid_size: Some(10),
                ask_size: Some(11),
            },
            84,
        ),
        batch: available_probe(
            AlpacaDoctorBatchObservation {
                http: alpaca_http_evidence(85, received_at(40)),
                semantic_result_digest: digest(88),
                requested_count: 50,
                returned_count: 50,
                missing_count: 0,
                unexpected_count: 0,
                duplicate_count: 0,
                invalid_count: 0,
                effective_cardinality: 50,
                requested_set_digest: digest(89),
                returned_set_digest: digest(90),
                missing_set_digest: digest(91),
                unexpected_set_digest: digest(92),
            },
            93,
        ),
        stream: available_probe(
            AlpacaDoctorStreamObservation {
                endpoint_contract_digest: digest(94),
                request_digest: digest(95),
                connected_frame_digest: digest(96),
                authenticated_frame_digest: digest(97),
                subscription_frame_digest: digest(98),
                semantic_result_digest: digest(99),
                handshake_status: 101,
                handshake_rate: alpaca_rate_evidence(),
                subscribed_trade_count: 1,
                subscribed_quote_count: 1,
                frames_observed: 3,
                bytes_observed: 512,
                authenticated_at: received_at(35),
                subscribed_at: received_at(34),
                close_sent: true,
                clean_close_observed: true,
                completed_at: received_at(30),
            },
            100,
        ),
        historical: available_probe(
            AlpacaDoctorHistoricalObservation {
                endpoint_contract_digest: digest(101),
                request_digest: digest(102),
                semantic_result_digest: digest(103),
                start_date,
                end_date,
                page_count: 2,
                returned_bar_count: 2,
                distinct_date_count: 2,
                first_bar_timestamp: Some(received_at(100)),
                last_bar_timestamp: Some(received_at(90)),
                returned_dates_digest: history_dates_digest,
                pagination_graph_digest: digest(104),
                terminal_page_observed: true,
                pages: vec![
                    AlpacaDoctorHistoricalPageEvidence {
                        http: alpaca_http_evidence(105, received_at(25)),
                        request_page_token_digest: None,
                        response_page_token_digest: Some(continuation_digest),
                    },
                    AlpacaDoctorHistoricalPageEvidence {
                        http: alpaca_http_evidence(108, received_at(20)),
                        request_page_token_digest: Some(continuation_digest),
                        response_page_token_digest: None,
                    },
                ]
                .into_boxed_slice(),
            },
            111,
        ),
        calendar: available_probe(
            AlpacaDoctorCalendarObservation {
                http: alpaca_http_evidence(112, received_at(10)),
                semantic_result_digest: digest(115),
                start_date,
                end_date,
                session_count: 2,
                history_date_count: 2,
                matched_count: 2,
                missing_history_count: 0,
                unexpected_history_count: 0,
                session_dates_digest: history_dates_digest,
                history_dates_digest,
                exact_date_reconciliation: true,
            },
            117,
        ),
        additional_capabilities,
        verified_at,
        exclusive_expires_at,
        predecessor_digest,
    };
    super::runtime_verification::seal_test_alpaca_provider_observation(&mut input)?;
    Ok(input)
}

fn alpaca_runtime_evidence(receipt: AlpacaPaperIexDoctorReceiptV1) -> RuntimeVerificationEvidence {
    RuntimeVerificationEvidence::AlpacaPaperIexDoctorReceiptV1(Box::new(receipt))
}

fn available_probe<T>(observation: T, digest_byte: u8) -> AlpacaDoctorProbeEvidence<T> {
    AlpacaDoctorProbeEvidence {
        disposition: RuntimeCapabilityDisposition::Available,
        disposition_evidence_digest: digest(digest_byte),
        observation: Some(observation),
    }
}

fn alpaca_http_evidence(digest_byte: u8, received_at: Timestamp) -> AlpacaDoctorHttpEvidence {
    AlpacaDoctorHttpEvidence {
        endpoint_contract_digest: digest(digest_byte),
        request_digest: digest(digest_byte + 1),
        status_code: 200,
        body_digest: digest(digest_byte + 2),
        response_bytes: 512,
        received_at,
        latency_nanos: 1,
        rate: alpaca_rate_evidence(),
    }
}

const fn alpaca_rate_evidence() -> AlpacaDoctorRateEvidence {
    AlpacaDoctorRateEvidence {
        limit: AlpacaRateLimitField::Observed(200),
        remaining: AlpacaRateLimitField::Observed(199),
        reset_unix_seconds: AlpacaRateLimitField::Observed(1_800_000_000),
        retry_after: AlpacaRateLimitField::Missing,
    }
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
