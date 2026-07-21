use market_squawk_adapter_fred::{
    FredOperation, FredRightsArtifact, FredRightsDisposition, FredRightsPolicy,
    FredSeriesRightsGrant, Sha256Digest,
};
use market_squawk_domain::{SourceIdentifier, Timestamp};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn release_artifact_and_runtime_policy_fail_closed_for_durable_use() -> TestResult {
    let artifact = FredRightsArtifact::parse(include_bytes!(
        "../../../docs/verification/fred-rights-decision.json"
    ))?;
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
    let artifact = FredRightsArtifact::parse(include_bytes!(
        "../../../docs/verification/fred-rights-decision.json"
    ))?;
    let terms = artifact.terms_evidence().clone();
    let series = SourceIdentifier::try_from("CPIAUCSL")?;
    let grant = FredSeriesRightsGrant::try_new(
        series.clone(),
        SourceIdentifier::try_from("us-bls")?,
        "https://example.invalid/owner-authorization".to_owned(),
        Sha256Digest::from_bytes([7; 32]),
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
