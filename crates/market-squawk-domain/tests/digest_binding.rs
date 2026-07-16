mod support;

use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    AuthorizationBasis, BookStateBinding, CanonicalStateDigest, CanonicalizationRule,
    ConnectionGeneration, CoverageStatus, DataQuality, DecodedLiveProvenanceInput, EvidenceDigest,
    InstrumentId, LiveEventClass, LiveEvidenceBinding, LiveProvenance, MarketDepth,
    MetadataRevision, PayloadHash, PayloadHashAlgorithm, PayloadReference, ProvenanceError,
    ProviderChannel, ProviderProduct, RuleVersion, SourceId, SourceIdentifier, Timestamp, VenueId,
};

fn rule(name: &str, version: u32) -> Result<CanonicalizationRule, Box<dyn Error>> {
    Ok(CanonicalizationRule::new(
        SourceIdentifier::try_from(name)?,
        RuleVersion::new(version)?,
    ))
}

fn book_binding(
    book_digest: CanonicalStateDigest,
    canonical_digest: CanonicalStateDigest,
) -> Result<LiveEvidenceBinding, Box<dyn Error>> {
    Ok(LiveEvidenceBinding::new(
        SourceId::try_from("kraken-direct")?,
        SourceIdentifier::try_from("session-1")?,
        MetadataRevision::new(SourceIdentifier::try_from("kraken-book-v2")?),
        AuthorizationBasis::new(SourceIdentifier::try_from("public-direct-feed")?),
        VenueId::try_from("KRAKEN")?,
        InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb")?,
        ConnectionGeneration::new(1)?,
        ProviderProduct::new(SourceIdentifier::try_from("BTC/USD")?),
        ProviderChannel::new(SourceIdentifier::try_from("book")?),
        LiveEventClass::BookDelta,
        SourceIdentifier::try_from("update-1")?,
        EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [1; 32]),
        canonical_digest,
        Some(BookStateBinding::new(
            MarketDepth::PriceLevel,
            SourceIdentifier::try_from("state-1")?,
            book_digest,
        )),
    )?)
}

#[test]
fn payload_hash_binding_compares_algorithm_and_bytes() -> Result<(), Box<dyn Error>> {
    let binding = support::live::binding(&support::live::BindingSpec {
        event_class: LiveEventClass::Trade,
        ..support::live::BindingSpec::default()
    })?;
    let result = LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        binding,
        None,
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_001),
        Timestamp::from_unix_nanos(1_002),
        DataQuality::DirectUnverified,
        CoverageStatus::Sufficient,
        PayloadReference::ContentHash(PayloadHash::new(PayloadHashAlgorithm::Blake3, [1; 32])),
    ));
    assert_eq!(result, Err(ProvenanceError::PayloadDigestMismatch));

    let matching = support::live::binding(&support::live::BindingSpec {
        event_class: LiveEventClass::Trade,
        ..support::live::BindingSpec::default()
    })?;
    assert!(
        LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
            matching,
            None,
            Timestamp::from_unix_nanos(1_000),
            Timestamp::from_unix_nanos(1_001),
            Timestamp::from_unix_nanos(1_002),
            DataQuality::DirectUnverified,
            CoverageStatus::Sufficient,
            PayloadReference::ContentHash(PayloadHash::new(PayloadHashAlgorithm::Sha256, [1; 32],)),
        ))
        .is_ok()
    );
    Ok(())
}

#[test]
fn canonical_state_rejects_algorithm_transplant_with_identical_bytes() -> Result<(), Box<dyn Error>>
{
    let sha = CanonicalStateDigest::new(
        EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [2; 32]),
        rule("market-squawk.book.levels", 1)?,
    );
    let blake = CanonicalStateDigest::new(
        EvidenceDigest::new(PayloadHashAlgorithm::Blake3, [2; 32]),
        rule("market-squawk.book.levels", 1)?,
    );
    assert!(book_binding(sha, blake).is_err());
    Ok(())
}

#[test]
fn canonical_state_rejects_rule_or_version_transplant() -> Result<(), Box<dyn Error>> {
    let book = CanonicalStateDigest::new(
        EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [2; 32]),
        rule("market-squawk.book.levels", 1)?,
    );
    let wrong_rule = CanonicalStateDigest::new(
        EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [2; 32]),
        rule("provider.book.text", 1)?,
    );
    let wrong_version = CanonicalStateDigest::new(
        EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [2; 32]),
        rule("market-squawk.book.levels", 2)?,
    );
    assert!(book_binding(book.clone(), wrong_rule).is_err());
    assert!(book_binding(book, wrong_version).is_err());
    Ok(())
}

#[test]
fn digest_wire_rejects_unknown_fields_and_invalid_rule_versions() -> Result<(), Box<dyn Error>> {
    let state = CanonicalStateDigest::new(
        EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [2; 32]),
        rule("market-squawk.book.levels", 1)?,
    );
    let restored: CanonicalStateDigest = serde_json::from_str(&serde_json::to_string(&state)?)?;
    assert_eq!(restored, state);

    let mut unknown = serde_json::to_value(&state)?;
    unknown["unexpected"] = serde_json::json!(true);
    assert!(serde_json::from_value::<CanonicalStateDigest>(unknown).is_err());

    let mut zero = serde_json::to_value(&state)?;
    zero["canonicalization_rule"]["version"] = serde_json::json!(0);
    assert!(serde_json::from_value::<CanonicalStateDigest>(zero).is_err());
    Ok(())
}
