use market_squawk_adapter_fred::{
    FredOperation, FredOwnerAuthorizationEvidence, FredRightsArtifact, FredRightsDisposition,
    FredRightsPolicy, FredSeriesRightsGrant, FredTermsDocumentBytes, FredTermsDocumentRole,
    MAX_FRED_TERMS_DOCUMENT_BYTES, Sha256Digest,
};
use market_squawk_domain::{SourceIdentifier, Timestamp};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn release_artifact_and_runtime_policy_fail_closed_for_durable_use() -> TestResult {
    let artifact_bytes = test_artifact();
    let terms_bytes = test_terms_bytes()?;
    let artifact = FredRightsArtifact::parse(&artifact_bytes, &terms_bytes)?;
    let policy = FredRightsPolicy::try_new(artifact.terms_evidence().clone(), Vec::new())?;
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
    assert_eq!(artifact.disposition(), durable.disposition());
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
    let terms = artifact.terms_evidence().clone();
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
        terms.bundle_digest(),
        vec![FredOperation::Persist],
        artifact.assessed_at(),
        Timestamp::from_unix_nanos(artifact.assessed_at().unix_nanos() + 1_000_000),
    )?;
    let policy = FredRightsPolicy::try_new(terms, vec![grant])?;

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

fn test_artifact() -> Vec<u8> {
    br#"{
        "schema_version": 2,
        "series_scope": "unresolved",
        "terms_bundle_digest": "06323e093a0db740245f21c0cc89682998bfbfa0d1c02bd02691d72780659ce7",
        "terms_documents": [
            {
                "role": "privacy_policy",
                "url": "https://www.stlouisfed.org/about-us/privacy-policy/online-notice",
                "sha256": "2b4d39194871cb7e47314173f79cf2491ac46edb364c4bf17e7ab98749ebe722",
                "byte_length": 41
            },
            {
                "role": "api_terms",
                "url": "https://fred.stlouisfed.org/docs/api/terms_of_use.html",
                "sha256": "27d66951a524848e3777300299a69ef16f868ab2dbc9ca04a00ddea0b4db13bd",
                "byte_length": 20
            },
            {
                "role": "fred_services_legal_terms",
                "url": "https://fred.stlouisfed.org/legal/",
                "sha256": "97da0ed4fc87909604e691990b7344467c66e7b1bc9424a2bfbcf41dcf25b9e5",
                "byte_length": 25
            }
        ],
        "assessed_at_unix_nanos": 100,
        "review_required_by_unix_nanos": 1000000100,
        "operations": ["persist", "cache", "archive", "train"],
        "disposition": "blocked_unknown_rights",
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
            "sha256": "2b4d39194871cb7e47314173f79cf2491ac46edb364c4bf17e7ab98749ebe722",
            "byte_length": 41,
            "evidence_class": "confirmed"
        }]
    }"#
    .to_vec()
}

fn test_terms_bytes() -> TestResult<[FredTermsDocumentBytes<'static>; 3]> {
    Ok([
        FredTermsDocumentBytes::try_new(
            FredTermsDocumentRole::FredServicesLegalTerms,
            b"exact FRED services terms",
        )?,
        FredTermsDocumentBytes::try_new(
            FredTermsDocumentRole::PrivacyPolicy,
            b"exact St. Louis Fed online privacy notice",
        )?,
        FredTermsDocumentBytes::try_new(FredTermsDocumentRole::ApiTerms, b"exact FRED API terms")?,
    ])
}
