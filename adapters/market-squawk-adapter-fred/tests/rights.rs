use market_squawk_adapter_fred::{
    CURRENT_FRED_RIGHTS_ARTIFACT_BYTE_LENGTH, CURRENT_FRED_RIGHTS_ARTIFACT_SHA256,
    CURRENT_UNRATE_RIGHTS_ARTIFACT_BYTE_LENGTH, CURRENT_UNRATE_RIGHTS_ARTIFACT_SHA256,
    FredOperation, FredOwnerAuthorizationEvidence, FredRightsArtifact, FredRightsDisposition,
    FredRightsPolicy, FredSeriesRightsBasis, FredSeriesRightsEvidence, FredSeriesRightsGrant,
    FredServicePermissionChannel, FredServicePermissionEvidence, FredServicePermissionReview,
    FredTermsDocumentBytes, FredTermsDocumentRole, MAX_FRED_TERMS_DOCUMENT_BYTES, Sha256Digest,
};
use market_squawk_domain::{SourceIdentifier, Timestamp};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const TEST_PRIVACY_GUID: &str = "{679A23BE-3C34-4F6E-A98E-9A9246CFF1B5}";
const TEST_PRIVACY_RAW: &[u8] = concat!(
    r#"<div class="component search-box col-12" id="a" data-properties='"#,
    r#"{"endpoint":"//sxa/search/results/","#,
    r#""suggestionEndpoint":"//sxa/search/suggestions/","suggestionsMode":"","#,
    r#""resultPage":"/search","targetSignature":"siteResults","#,
    r#""v":"{E22FB38C-3672-49E1-B145-563EEAEC4951}","#,
    r#""s":"{A10D94E2-3F41-4100-A3BA-24E58460A483}","#,
    r#""p":0,"l":"","languageSource":"AllLanguages","#,
    r#""searchResultsSignature":"","itemid":"{679A23BE-3C34-4F6E-A98E-9A9246CFF1B5}"#,
    r#"","minSuggestionsTriggerCharacterCount":2}'>"#
)
.as_bytes();

#[test]
fn manifest_only_terms_cannot_admit_non_ephemeral_use() -> TestResult {
    use sha2::Digest as _;

    let manifest = include_bytes!("../../../docs/verification/fred-rights-decision.json");
    assert_eq!(manifest.len(), CURRENT_FRED_RIGHTS_ARTIFACT_BYTE_LENGTH);
    assert_eq!(
        Sha256Digest::from_bytes(sha2::Sha256::digest(manifest).into()),
        CURRENT_FRED_RIGHTS_ARTIFACT_SHA256
    );
    let artifact = FredRightsArtifact::parse_current_reviewed_manifest(manifest)?;
    assert!(artifact.operations().contains(&FredOperation::Display));

    let unrate_review =
        include_bytes!("../../../docs/verification/fred-unrate-public-domain-rights.json");
    assert_eq!(
        unrate_review.len(),
        CURRENT_UNRATE_RIGHTS_ARTIFACT_BYTE_LENGTH
    );
    assert_eq!(
        Sha256Digest::from_bytes(sha2::Sha256::digest(unrate_review).into()),
        CURRENT_UNRATE_RIGHTS_ARTIFACT_SHA256
    );
    let evidence = FredSeriesRightsEvidence::parse_reviewed_unrate_public_domain(unrate_review)?;
    assert_eq!(evidence.basis(), FredSeriesRightsBasis::PublicDomain);
    assert!(
        FredSeriesRightsGrant::try_new_with_evidence(
            SourceIdentifier::try_from("CPIAUCSL")?,
            SourceIdentifier::try_from("us-bureau-of-labor-statistics")?,
            evidence.clone(),
            artifact.terms_evidence().bundle_digest(),
            vec![FredOperation::Persist],
            Timestamp::from_unix_nanos(1_785_024_000_000_000_000),
            artifact.review_required_by(),
        )
        .is_err()
    );
    let grant = FredSeriesRightsGrant::try_new_with_evidence(
        SourceIdentifier::try_from("UNRATE")?,
        SourceIdentifier::try_from("us-bureau-of-labor-statistics")?,
        evidence,
        artifact.terms_evidence().bundle_digest(),
        vec![FredOperation::Display, FredOperation::Persist],
        Timestamp::from_unix_nanos(1_785_024_000_000_000_000),
        artifact.review_required_by(),
    )?;
    assert_eq!(
        grant.citation_url(),
        "https://fred.stlouisfed.org/series/UNRATE"
    );
    let without_bank_permission =
        FredRightsPolicy::try_new(artifact.clone(), None, vec![grant.clone()])?;
    assert_eq!(
        without_bank_permission
            .assess(
                &SourceIdentifier::try_from("UNRATE")?,
                &[FredOperation::Persist],
                Timestamp::from_unix_nanos(1_785_024_000_000_000_000),
            )?
            .disposition(),
        FredRightsDisposition::BlockedUnknownRights
    );
    let permission = service_permission(
        &artifact,
        "UNRATE",
        vec![FredOperation::Persist],
        Timestamp::from_unix_nanos(1_785_024_000_000_000_000),
        artifact.review_required_by(),
    )?;
    let policy = FredRightsPolicy::try_new(artifact, Some(permission), vec![grant])?;
    let unrate = SourceIdentifier::try_from("UNRATE")?;
    let at = Timestamp::from_unix_nanos(1_785_024_000_000_000_000);
    assert!(policy.durable_authority(at).is_err());
    assert_eq!(
        policy
            .assess(&unrate, &[FredOperation::RetrieveEphemeral], at)?
            .disposition(),
        FredRightsDisposition::Permitted
    );
    assert_eq!(
        policy
            .assess(
                &unrate,
                &[FredOperation::RetrieveEphemeral, FredOperation::Persist],
                at,
            )?
            .disposition(),
        FredRightsDisposition::BlockedUnknownRights
    );
    for operation in [
        FredOperation::Display,
        FredOperation::Persist,
        FredOperation::Cache,
        FredOperation::Archive,
        FredOperation::Redistribute,
        FredOperation::Train,
    ] {
        assert_eq!(
            policy.assess(&unrate, &[operation], at)?.disposition(),
            FredRightsDisposition::BlockedUnknownRights
        );
    }
    Ok(())
}

#[test]
#[ignore = "requires the controlled local exact FRED terms evidence directory"]
fn controlled_release_artifact_verifies_exact_terms_and_requires_scoped_grants() -> TestResult {
    let root = std::env::var_os("MARKET_SQUAWK_FRED_RIGHTS_EVIDENCE_DIR")
        .ok_or("MARKET_SQUAWK_FRED_RIGHTS_EVIDENCE_DIR is required")?;
    let root = std::path::PathBuf::from(root);
    let artifact_bytes = std::fs::read(root.join("fred-rights-decision.json"))?;
    let api_terms = std::fs::read(root.join("api-terms.html"))?;
    let legal_terms = std::fs::read(root.join("fred-legal.html"))?;
    let privacy_policy = std::fs::read(root.join("privacy.html"))?;
    let terms_bytes = [
        FredTermsDocumentBytes::try_new(FredTermsDocumentRole::ApiTerms, &api_terms)?,
        FredTermsDocumentBytes::try_new(
            FredTermsDocumentRole::FredServicesLegalTerms,
            &legal_terms,
        )?,
        FredTermsDocumentBytes::try_new(FredTermsDocumentRole::PrivacyPolicy, &privacy_policy)?,
    ];
    let artifact = FredRightsArtifact::parse_current_reviewed(&artifact_bytes, &terms_bytes)?;
    assert_eq!(
        artifact.disposition(),
        FredRightsDisposition::ServicePermissionRequired
    );
    assert_eq!(artifact.terms_evidence().documents().count(), 3);
    Ok(())
}

#[test]
fn release_artifact_requires_grants_and_runtime_fails_closed_without_one() -> TestResult {
    let artifact_bytes = test_artifact();
    let terms_bytes = test_terms_bytes()?;
    let artifact = FredRightsArtifact::parse(&artifact_bytes, &terms_bytes)?;
    let policy = FredRightsPolicy::try_new(artifact.clone(), None, Vec::new())?;
    let series = SourceIdentifier::try_from("CPIAUCSL")?;
    let at = artifact.assessed_at();

    let ephemeral = policy.assess(&series, &[FredOperation::RetrieveEphemeral], at)?;
    assert_eq!(ephemeral.disposition(), FredRightsDisposition::Permitted);

    let durable = policy.assess(
        &series,
        &[
            FredOperation::Persist,
            FredOperation::Cache,
            FredOperation::Archive,
            FredOperation::Train,
        ],
        at,
    )?;
    assert_eq!(
        durable.disposition(),
        FredRightsDisposition::BlockedUnknownRights
    );
    assert_eq!(
        artifact.disposition(),
        FredRightsDisposition::ServicePermissionRequired
    );
    assert_eq!(
        durable.terms_bundle_digest(),
        artifact.terms_evidence().bundle_digest()
    );
    assert_eq!(artifact.terms_evidence().documents().count(), 3);

    let after_review_deadline = artifact.review_required_by().checked_add_nanos(1)?;
    assert_eq!(
        policy
            .assess(
                &series,
                &[FredOperation::RetrieveEphemeral],
                after_review_deadline,
            )?
            .disposition(),
        FredRightsDisposition::BlockedStaleTerms
    );
    Ok(())
}

#[test]
fn an_exact_owner_grant_cannot_widen_to_another_series_or_operation() -> TestResult {
    let artifact_bytes = test_artifact();
    let terms_bytes = test_terms_bytes()?;
    let artifact = FredRightsArtifact::parse(&artifact_bytes, &terms_bytes)?;
    let series = SourceIdentifier::try_from("CPIAUCSL")?;
    let authorization = FredOwnerAuthorizationEvidence::try_new(
        "https://www.bls.gov/".to_owned(),
        Sha256Digest::from_lower_hex(
            "4f74f39c9be10fb0bd783b68087014430860fc5d0abc4080dff3c07263194681",
        )?,
        31,
        b"exact owner authorization bytes",
    )?;
    let grant = FredSeriesRightsGrant::try_new(
        series.clone(),
        SourceIdentifier::try_from("us-bls")?,
        authorization,
        artifact.terms_evidence().bundle_digest(),
        vec![FredOperation::Persist],
        artifact.assessed_at(),
        Timestamp::from_unix_nanos(artifact.assessed_at().unix_nanos() + 1_000_000),
    )?;
    let permission = service_permission(
        &artifact,
        series.as_str(),
        vec![FredOperation::Persist],
        artifact.assessed_at(),
        Timestamp::from_unix_nanos(artifact.assessed_at().unix_nanos() + 1_000_000),
    )?;
    let policy = FredRightsPolicy::try_new(artifact.clone(), Some(permission), vec![grant])?;
    let authority = policy.durable_authority(artifact.assessed_at())?;
    assert_eq!(authority.series().collect::<Vec<_>>(), [&series]);
    assert!(authority.admits(&series, FredOperation::Persist));
    assert_ne!(authority.authorization_digest().bytes(), [0; 32]);

    assert_eq!(
        policy
            .assess(&series, &[FredOperation::Persist], artifact.assessed_at())?
            .disposition(),
        FredRightsDisposition::Permitted
    );
    assert_eq!(
        policy
            .assess(&series, &[FredOperation::Train], artifact.assessed_at())?
            .disposition(),
        FredRightsDisposition::BlockedOperationScope
    );
    assert_eq!(
        policy
            .assess(
                &series,
                &[FredOperation::Redistribute],
                artifact.assessed_at(),
            )?
            .disposition(),
        FredRightsDisposition::BlockedOperationScope
    );
    assert_eq!(
        policy
            .assess(
                &SourceIdentifier::try_from("UNRATE")?,
                &[FredOperation::Persist],
                artifact.assessed_at(),
            )?
            .disposition(),
        FredRightsDisposition::BlockedUnknownRights
    );
    Ok(())
}

#[test]
fn rights_authority_rejects_unverified_terms_and_owner_bytes() -> TestResult {
    let artifact_bytes = test_artifact();
    let terms_bytes = test_terms_bytes()?;
    assert!(FredRightsArtifact::parse(&artifact_bytes, &terms_bytes[..2]).is_err());
    let duplicate_role = [
        FredTermsDocumentBytes::try_new(FredTermsDocumentRole::ApiTerms, b"exact FRED API terms")?,
        FredTermsDocumentBytes::try_new(
            FredTermsDocumentRole::ApiTerms,
            b"exact FRED services terms",
        )?,
        FredTermsDocumentBytes::try_new(
            FredTermsDocumentRole::PrivacyPolicy,
            b"exact St. Louis Fed online privacy notice",
        )?,
    ];
    assert!(FredRightsArtifact::parse(&artifact_bytes, &duplicate_role).is_err());
    let substituted = [
        FredTermsDocumentBytes::try_new(FredTermsDocumentRole::ApiTerms, b"exact FRED API terms")?,
        FredTermsDocumentBytes::try_new(
            FredTermsDocumentRole::FredServicesLegalTerms,
            b"substituted FRED services terms",
        )?,
        FredTermsDocumentBytes::try_new(
            FredTermsDocumentRole::PrivacyPolicy,
            b"exact St. Louis Fed online privacy notice",
        )?,
    ];
    assert!(FredRightsArtifact::parse(&artifact_bytes, &substituted).is_err());

    let noncanonical = String::from_utf8(artifact_bytes.clone())?.replace(
        "https://www.stlouisfed.org/about-us/privacy-policy/online-notice",
        "https://www.stlouisfed.org/about-us/privacy-policy/online-notice/",
    );
    assert!(FredRightsArtifact::parse(noncanonical.as_bytes(), &terms_bytes).is_err());
    let oversized = vec![0_u8; MAX_FRED_TERMS_DOCUMENT_BYTES + 1];
    assert!(FredTermsDocumentBytes::try_new(FredTermsDocumentRole::ApiTerms, &oversized).is_err());

    assert!(
        FredOwnerAuthorizationEvidence::try_new(
            "https://www.bls.gov/".to_owned(),
            Sha256Digest::from_lower_hex(
                "4f74f39c9be10fb0bd783b68087014430860fc5d0abc4080dff3c07263194681",
            )?,
            31,
            b"substituted owner authorization",
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn privacy_review_identity_accepts_only_one_context_bound_uppercase_guid() -> TestResult {
    use sha2::Digest as _;

    let artifact_bytes = test_artifact();
    let alternate = std::str::from_utf8(TEST_PRIVACY_RAW)?
        .replace(TEST_PRIVACY_GUID, "{A0012057-C229-4E2A-B9CE-66DA0AC7437C}");
    let alternate_terms = test_terms_bytes_with_privacy(alternate.as_bytes())?;
    let artifact = FredRightsArtifact::parse(&artifact_bytes, &alternate_terms)?;
    let privacy = artifact
        .terms_evidence()
        .document(FredTermsDocumentRole::PrivacyPolicy)
        .ok_or("privacy evidence is required")?;
    assert_eq!(
        privacy.raw_digest(),
        Some(Sha256Digest::from_bytes(
            sha2::Sha256::digest(alternate.as_bytes()).into()
        ))
    );
    assert_eq!(privacy.raw_byte_length(), Some(alternate.len()));
    assert_ne!(privacy.raw_digest(), Some(privacy.digest()));

    let lowercase = std::str::from_utf8(TEST_PRIVACY_RAW)?
        .replace(TEST_PRIVACY_GUID, "{679a23BE-3C34-4F6E-A98E-9A9246CFF1B5}");
    let malformed = std::str::from_utf8(TEST_PRIVACY_RAW)?
        .replace(TEST_PRIVACY_GUID, "{679A23BE-3C34-4F6E-A98E-9A9246CFF1B5");
    let multiple = [TEST_PRIVACY_RAW, TEST_PRIVACY_RAW].concat();
    let out_of_context =
        br#"<div data-properties='{"itemid":"{679A23BE-3C34-4F6E-A98E-9A9246CFF1B5}"}'>"#;
    for rejected in [
        b"privacy body without the required component".as_slice(),
        lowercase.as_bytes(),
        malformed.as_bytes(),
        multiple.as_slice(),
        out_of_context.as_slice(),
    ] {
        let terms = test_terms_bytes_with_privacy(rejected)?;
        assert!(FredRightsArtifact::parse(&artifact_bytes, &terms).is_err());
    }
    Ok(())
}

fn test_artifact() -> Vec<u8> {
    br#"{
        "schema_version": 5,
        "series_scope": "exact_service_and_series_grants",
        "terms_bundle_digest": "b3c33fd45878caee3c51ea1fafed95a1bed829432b49cd2a5c420e76df7aae3f",
        "terms_documents": [
            {
                "role": "privacy_policy",
                "representation": "privacy_sxa_search_item_canonical_v1",
                "url": "https://www.stlouisfed.org/about-us/privacy-policy/online-notice",
                "sha256": "6cd3afb454b4a8b7e6cfd026d43cede4ea7936cf081cd5a83bf803f846af7743",
                "byte_length": 473
            },
            {
                "role": "api_terms",
                "representation": "exact_raw",
                "url": "https://fred.stlouisfed.org/docs/api/terms_of_use.html",
                "sha256": "27d66951a524848e3777300299a69ef16f868ab2dbc9ca04a00ddea0b4db13bd",
                "byte_length": 20
            },
            {
                "role": "fred_services_legal_terms",
                "representation": "exact_raw",
                "url": "https://fred.stlouisfed.org/legal/",
                "sha256": "97da0ed4fc87909604e691990b7344467c66e7b1bc9424a2bfbcf41dcf25b9e5",
                "byte_length": 25
            }
        ],
        "assessed_at_unix_nanos": 100,
        "review_required_by_unix_nanos": 1000000100,
        "operations": ["persist", "cache", "archive", "train"],
        "disposition": "service_permission_required",
        "confirmed_facts": ["test evidence"],
        "engineering_inferences": ["test inference"],
        "sources": [{
            "url": "https://fred.stlouisfed.org/docs/api/terms_of_use.html",
            "accessed_on": "2026-07-21",
            "sha256": "27d66951a524848e3777300299a69ef16f868ab2dbc9ca04a00ddea0b4db13bd",
            "byte_length": 20,
            "evidence_class": "confirmed"
        }, {
            "url": "https://fred.stlouisfed.org/legal/",
            "accessed_on": "2026-07-21",
            "sha256": "97da0ed4fc87909604e691990b7344467c66e7b1bc9424a2bfbcf41dcf25b9e5",
            "byte_length": 25,
            "evidence_class": "confirmed"
        }, {
            "url": "https://www.stlouisfed.org/about-us/privacy-policy/online-notice",
            "accessed_on": "2026-07-21",
            "sha256": "8209e409687a552bea39fc7d01e7fe10e59b25ca03d0e8ffad59bafd12744749",
            "byte_length": 481,
            "evidence_class": "confirmed"
        }]
    }"#
    .to_vec()
}

fn test_terms_bytes() -> TestResult<[FredTermsDocumentBytes<'static>; 3]> {
    test_terms_bytes_with_privacy(TEST_PRIVACY_RAW)
}

fn test_terms_bytes_with_privacy(privacy: &[u8]) -> TestResult<[FredTermsDocumentBytes<'_>; 3]> {
    Ok([
        FredTermsDocumentBytes::try_new(
            FredTermsDocumentRole::FredServicesLegalTerms,
            b"exact FRED services terms",
        )?,
        FredTermsDocumentBytes::try_new(FredTermsDocumentRole::PrivacyPolicy, privacy)?,
        FredTermsDocumentBytes::try_new(FredTermsDocumentRole::ApiTerms, b"exact FRED API terms")?,
    ])
}

fn service_permission(
    artifact: &FredRightsArtifact,
    series: &str,
    operations: Vec<FredOperation>,
    effective_at: Timestamp,
    revalidate_by: Timestamp,
) -> TestResult<FredServicePermissionEvidence> {
    use sha2::Digest as _;

    let exact_response = b"exact written St. Louis Fed service permission";
    let channel = FredServicePermissionChannel::try_official_https(
        "https://fred.stlouisfed.org/contactus/permission-test".to_owned(),
        "https://fred.stlouisfed.org/contactus/".to_owned(),
    )?;
    let review = FredServicePermissionReview::try_new(
        SourceIdentifier::try_from("test-rights-reviewer")?,
        effective_at,
        SourceIdentifier::try_from("federal-reserve-bank-of-st-louis")?,
        SourceIdentifier::try_from("market-squawk")?,
        SourceIdentifier::try_from("fred-api")?,
        vec![SourceIdentifier::try_from(series)?],
        operations,
        Vec::new(),
        effective_at,
        None,
        revalidate_by,
    )?;
    Ok(FredServicePermissionEvidence::try_new(
        channel,
        review,
        artifact.terms_evidence().bundle_digest(),
        Sha256Digest::from_bytes(sha2::Sha256::digest(exact_response).into()),
        exact_response.len(),
        exact_response,
    )?)
}
