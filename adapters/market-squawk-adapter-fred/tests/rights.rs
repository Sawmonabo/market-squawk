use market_squawk_adapter_fred::{
    FredOperation, FredOwnerAuthorizationEvidence, FredRightsArtifact, FredRightsDisposition,
    FredRightsPolicy, FredSeriesRightsGrant, Sha256Digest,
};
use market_squawk_domain::{SourceIdentifier, Timestamp};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn release_artifact_and_runtime_policy_fail_closed_for_durable_use() -> TestResult {
    let terms_bytes = b"exact terms bytes";
    let artifact_bytes = test_artifact();
    let artifact = FredRightsArtifact::parse(&artifact_bytes, terms_bytes)?;
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
    let artifact = FredRightsArtifact::parse(&artifact_bytes, b"exact terms bytes")?;
    let terms = artifact.terms_evidence().clone();
    let series = SourceIdentifier::try_from("CPIAUCSL")?;
    let authorization = FredOwnerAuthorizationEvidence::try_new(
        "https://example.invalid/owner-authorization".to_owned(),
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
        terms.digest(),
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
    assert!(FredRightsArtifact::parse(&artifact_bytes, b"changed terms bytes").is_err());

    assert!(
        FredOwnerAuthorizationEvidence::try_new(
            "https://example.invalid/owner-authorization".to_owned(),
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
        "schema_version": 1,
        "series_scope": "unresolved",
        "terms_url": "https://fred.example.test/terms",
        "terms_digest": "e84779cc4eed635dff13aea470f60c17889fa934750e178856edaa0796df830f",
        "terms_bytes": 17,
        "assessed_at_unix_nanos": 100,
        "review_required_by_unix_nanos": 1000000100,
        "operations": ["persist", "cache", "archive", "train"],
        "disposition": "blocked_unknown_rights",
        "confirmed_facts": ["test evidence"],
        "engineering_inferences": ["test inference"],
        "sources": [{
            "url": "https://fred.example.test/terms",
            "accessed_on": "2026-07-21",
            "sha256": "e84779cc4eed635dff13aea470f60c17889fa934750e178856edaa0796df830f",
            "byte_length": 17,
            "evidence_class": "confirmed"
        }]
    }"#
    .to_vec()
}
