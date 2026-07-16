use market_squawk_domain::{
    AssignmentVerification, CalendarDate, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, ExternalIdentifier, ExternalIdentifierRecord,
    ExternalIdentifierRecordInput, FuturesContractIdentity, FuturesContractIdentityInput,
    FuturesLifecycleDateFields, FuturesLifecycleDates, FuturesSecurityType, IdentifierEntitlement,
    IdentifierRightsPolicyReference, Isin, MetadataRevision, ProviderInstrumentId,
    RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, Timestamp, VenueId, VenueSymbol,
    VersionPinnedSourceLocator,
};

fn digest(algorithm: DigestAlgorithm, byte: u8) -> EvidenceDigest {
    EvidenceDigest::new(algorithm, [byte; 32])
}

#[test]
fn exact_payload_evidence_requires_a_digest_and_preserves_algorithm_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let sha = ExactPayloadEvidence::from_content_digest(digest(DigestAlgorithm::Sha256, 7));
    let blake = ExactPayloadEvidence::from_content_digest(digest(DigestAlgorithm::Blake3, 7));

    assert_ne!(sha, blake);
    assert_eq!(sha.content_digest().algorithm(), DigestAlgorithm::Sha256);
    assert_eq!(sha.content_digest().bytes(), [7; 32]);
    assert_eq!(
        serde_json::from_value::<ExactPayloadEvidence>(serde_json::to_value(&sha)?)?,
        sha
    );

    let moving_url_alone = serde_json::json!({
        "kind": "source_reference",
        "value": "https://fiximate.fixtrading.org/en/FIX.Latest/tag200.html"
    });
    assert!(serde_json::from_value::<ExactPayloadEvidence>(moving_url_alone).is_err());
    assert!(serde_json::from_value::<ExactPayloadEvidence>(serde_json::json!({})).is_err());
    Ok(())
}

#[test]
fn changing_the_explicit_algorithm_deserializes_as_distinct_valid_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let original = ExactPayloadEvidence::from_content_digest(digest(DigestAlgorithm::Sha256, 8));
    let mut changed_wire = serde_json::to_value(&original)?;
    changed_wire["content_digest"]["algorithm"] = serde_json::json!("blake3");

    let changed = serde_json::from_value::<ExactPayloadEvidence>(changed_wire)?;

    assert_eq!(
        changed.content_digest().algorithm(),
        DigestAlgorithm::Blake3
    );
    assert_eq!(
        changed.content_digest().bytes(),
        original.content_digest().bytes()
    );
    assert_ne!(changed, original);
    Ok(())
}

#[test]
fn exact_payload_locator_is_optional_but_requires_a_separate_version()
-> Result<(), Box<dyn std::error::Error>> {
    let locator = VersionPinnedSourceLocator::new(
        SourceIdentifier::try_from("https://fiximate.fixtrading.org/en/FIX.Latest/tag200.html")?,
        SourceIdentifier::try_from("FIX.Latest_EP307:sha256:7721110c")?,
    );
    let evidence = ExactPayloadEvidence::with_version_pinned_locator(
        digest(DigestAlgorithm::Sha256, 9),
        locator,
    );

    assert_eq!(
        evidence
            .version_pinned_locator()
            .map(VersionPinnedSourceLocator::version)
            .map(SourceIdentifier::as_str),
        Some("FIX.Latest_EP307:sha256:7721110c")
    );
    assert!(
        serde_json::from_value::<ExactPayloadEvidence>(serde_json::json!({
            "content_digest": {
                "algorithm": "sha256",
                "bytes": vec![9; 32]
            },
            "version_pinned_locator": {
                "reference": "https://fiximate.fixtrading.org/en/FIX.Latest/tag200.html"
            }
        }))
        .is_err()
    );
    Ok(())
}

#[test]
fn revision_binding_is_atomic_strict_and_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    let revision = MetadataRevision::new(SourceIdentifier::try_from("FIX.Latest_EP307")?);
    let binding = RevisionBoundPayloadEvidence::new(
        revision,
        ExactPayloadEvidence::from_content_digest(digest(DigestAlgorithm::Sha256, 11)),
    );

    assert_eq!(
        binding.metadata_revision().as_source_identifier().as_str(),
        "FIX.Latest_EP307"
    );
    assert_eq!(
        binding.payload_evidence().content_digest().bytes(),
        [11; 32]
    );
    let encoded = serde_json::to_value(&binding)?;
    assert_eq!(
        serde_json::from_value::<RevisionBoundPayloadEvidence>(encoded.clone())?,
        binding
    );
    let mut unknown = encoded;
    unknown["unbound_revision_alias"] = serde_json::json!("latest");
    assert!(serde_json::from_value::<RevisionBoundPayloadEvidence>(unknown).is_err());
    Ok(())
}

fn exact_evidence(byte: u8) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(digest(DigestAlgorithm::Sha256, byte))
}

#[test]
fn authoritative_external_identifier_assignment_requires_exact_payload_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let record = ExternalIdentifierRecord::new(ExternalIdentifierRecordInput {
        identifier: ExternalIdentifier::Isin(Isin::try_from("US0378331005")?),
        assignment_verification: AssignmentVerification::VerifiedAssigned,
        source_id: SourceId::try_from("anna-reference")?,
        source_evidence: exact_evidence(13),
        source_timestamp: Some(Timestamp::from_unix_nanos(90)),
        observed_at: Timestamp::from_unix_nanos(100),
        validity: EffectiveInterval::new(Timestamp::from_unix_nanos(80), None)?,
        rights_policy: IdentifierRightsPolicyReference::new(
            SourceIdentifier::try_from("policy:identifier-restricted-v1")?,
            IdentifierEntitlement::UnknownOrRestricted,
            SourceIdentifier::try_from("https://www.iso.org/standard/78502.html")?,
        ),
    });

    assert_eq!(record.source_evidence().content_digest().bytes(), [13; 32]);
    let encoded = serde_json::to_value(&record)?;
    assert!(encoded.get("source_reference").is_none());
    assert_eq!(
        serde_json::from_value::<ExternalIdentifierRecord>(encoded.clone())?,
        record
    );
    let mut legacy = encoded;
    legacy
        .as_object_mut()
        .ok_or("record must serialize as an object")?
        .remove("source_evidence");
    legacy["source_reference"] = serde_json::json!({
        "kind": "source_reference",
        "value": "https://provider.example/current/identifier/US0378331005"
    });
    assert!(serde_json::from_value::<ExternalIdentifierRecord>(legacy).is_err());
    Ok(())
}

#[test]
fn futures_identity_binds_metadata_revision_to_exact_payload_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let source_evidence = RevisionBoundPayloadEvidence::new(
        MetadataRevision::new(SourceIdentifier::try_from("FIX.Latest_EP307")?),
        exact_evidence(17),
    );
    let identity = FuturesContractIdentity::try_new(FuturesContractIdentityInput {
        source_id: SourceId::try_from("cme-reference")?,
        source_evidence,
        source_timestamp: Some(Timestamp::from_unix_nanos(900)),
        observed_at: Timestamp::from_unix_nanos(1_000),
        venue_id: VenueId::try_from("XCME")?,
        security_id: ProviderInstrumentId::try_from("ESM6")?,
        security_id_source: SourceIdentifier::try_from("8")?,
        product_code: ProviderInstrumentId::try_from("ES")?,
        native_symbol: VenueSymbol::try_from("ESM6")?,
        security_type: FuturesSecurityType::Future,
        maturity_month_year: None,
        lifecycle: FuturesLifecycleDates::try_new(FuturesLifecycleDateFields {
            maturity_date: Some(CalendarDate::new(2026, 6, 19)?),
            ..FuturesLifecycleDateFields::default()
        })?,
        legs: Vec::new(),
    })?;

    assert_eq!(
        identity.metadata_revision().as_source_identifier().as_str(),
        "FIX.Latest_EP307"
    );
    assert_eq!(
        identity
            .source_evidence()
            .payload_evidence()
            .content_digest()
            .bytes(),
        [17; 32]
    );
    let encoded = serde_json::to_value(&identity)?;
    assert!(encoded.get("source_reference").is_none());
    assert!(encoded.get("metadata_revision").is_none());
    assert_eq!(
        serde_json::from_value::<FuturesContractIdentity>(encoded.clone())?,
        identity
    );
    let mut legacy = encoded;
    legacy
        .as_object_mut()
        .ok_or("identity must serialize as an object")?
        .remove("source_evidence");
    legacy["source_reference"] = serde_json::json!({
        "kind": "source_reference",
        "value": "https://fiximate.fixtrading.org/en/FIX.Latest/tag200.html"
    });
    legacy["metadata_revision"] = serde_json::json!("latest");
    assert!(serde_json::from_value::<FuturesContractIdentity>(legacy).is_err());
    Ok(())
}
