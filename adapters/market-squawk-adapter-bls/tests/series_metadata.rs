use bytes::Bytes;
use market_squawk_adapter_bls::{
    BlsAuthorization, BlsSeriesMetadata, BlsSourceConfig, BlsSourceError, BlsUsagePolicy,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, SourceIdentifier,
};
use sha2::{Digest, Sha256};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn exact_evidence(payload: &[u8]) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(payload).into(),
    ))
}

fn metadata(payload: &'static [u8]) -> Result<BlsSeriesMetadata, BlsSourceError> {
    BlsSeriesMetadata::parse_exact(
        Bytes::from_static(payload),
        exact_evidence(payload),
        SourceIdentifier::try_from("user-approved:bls-series-metadata:2026-07-21")
            .map_err(|_| BlsSourceError::InvalidSeriesMetadata)?,
    )
}

fn usage_policy() -> Result<BlsUsagePolicy, BlsSourceError> {
    BlsUsagePolicy::try_owner_authorized(EvidenceDigest::new(DigestAlgorithm::Sha256, [42; 32]))
}

#[test]
fn exact_user_authorized_metadata_is_required_and_bound_to_the_dataset() -> TestResult {
    const PERCENT: &[u8] = br#"{
        "schema_version":1,
        "series_id":"LNS14000000",
        "title":"Unemployment Rate",
        "unit":"percent",
        "frequency":"monthly",
        "seasonal_adjustment":"seasonally-adjusted",
        "measure":"rate"
    }"#;
    const INDEX: &[u8] = br#"{
        "schema_version":1,
        "series_id":"LNS14000000",
        "title":"Unemployment Rate",
        "unit":"index-1982-84-equals-100",
        "frequency":"monthly",
        "seasonal_adjustment":"seasonally-adjusted",
        "measure":"index"
    }"#;

    let percent = metadata(PERCENT)?;
    assert_eq!(percent.series_id(), "LNS14000000");
    assert_eq!(percent.unit().as_str(), "percent");
    assert_eq!(percent.exact_payload(), PERCENT);

    let percent_config = BlsSourceConfig::try_new(
        BlsAuthorization::PublicV1,
        usage_policy()?,
        vec![percent.clone()],
        2020,
        2026,
    )?;
    let index_config = BlsSourceConfig::try_new(
        BlsAuthorization::PublicV1,
        usage_policy()?,
        vec![metadata(INDEX)?],
        2020,
        2026,
    )?;
    assert_ne!(percent_config.dataset(), index_config.dataset());
    assert_eq!(
        percent_config
            .series_metadata("LNS14000000")
            .map(BlsSeriesMetadata::unit),
        Some(percent.unit())
    );

    let wrong_evidence = exact_evidence(INDEX);
    assert!(matches!(
        BlsSeriesMetadata::parse_exact(
            Bytes::from_static(PERCENT),
            wrong_evidence,
            SourceIdentifier::try_from("user-approved:bls-series-metadata:2026-07-21")?,
        ),
        Err(BlsSourceError::InvalidSeriesMetadata)
    ));
    assert!(
        BlsSourceConfig::try_new(
            BlsAuthorization::PublicV1,
            usage_policy()?,
            vec![percent.clone(), percent],
            2020,
            2026,
        )
        .is_err()
    );
    Ok(())
}
