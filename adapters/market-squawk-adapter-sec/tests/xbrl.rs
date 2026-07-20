use std::error::Error;

use market_squawk_adapter_sec::{SecParserLimits, XbrlDocumentContext, XbrlDocumentParser};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, SourceIdentifier, Timestamp,
    XbrlDimensionLocation, XbrlTaxonomySet,
};

#[test]
fn inline_xbrl_preserves_context_transform_dimensions_and_nonnumeric_occurrences()
-> Result<(), Box<dyn Error>> {
    let bytes = include_bytes!("../fixtures/inline-xbrl.html");
    let document = XbrlDocumentParser::parse(
        bytes,
        SecParserLimits::production_defaults(),
        XbrlDocumentContext::new(
            SourceIdentifier::try_from("0000320193-25-000079")?,
            XbrlTaxonomySet::new(
                EvidenceDigest::new(DigestAlgorithm::Sha256, [3; 32]),
                SourceIdentifier::try_from("us-gaap-2025")?,
            ),
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                [7; 32],
            )),
            Timestamp::from_unix_nanos(42),
        ),
    )?;

    assert_eq!(document.numeric_facts().len(), 1);
    let fact = &document.numeric_facts()[0];
    assert_eq!(fact.concept().as_str(), "us-gaap:NetIncomeLoss");
    assert_eq!(fact.value().to_string(), "-23434000000");
    assert_eq!(fact.evidence().accession().as_str(), "0000320193-25-000079");
    assert_eq!(document.nonnumeric_occurrences().len(), 1);
    assert_eq!(
        document.nonnumeric_occurrences()[0]
            .lexical_value()
            .as_str(),
        "APPLE INC"
    );

    let wire = serde_json::to_value(fact.evidence())?;
    assert_eq!(
        wire["dimensions"][0]["location"],
        serde_json::json!("segment")
    );
    let _typed_location = XbrlDimensionLocation::Segment;
    Ok(())
}

#[test]
fn xbrl_rejects_doctype_and_depth_exhaustion() -> Result<(), Box<dyn Error>> {
    let context = XbrlDocumentContext::new(
        SourceIdentifier::try_from("0000320193-25-000079")?,
        XbrlTaxonomySet::new(
            EvidenceDigest::new(DigestAlgorithm::Sha256, [3; 32]),
            SourceIdentifier::try_from("us-gaap-2025")?,
        ),
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [7; 32],
        )),
        Timestamp::from_unix_nanos(42),
    );
    assert!(
        XbrlDocumentParser::parse(
            b"<!DOCTYPE x [<!ENTITY ex SYSTEM 'file:///etc/passwd'>]><x>&ex;</x>",
            SecParserLimits::production_defaults(),
            context.clone(),
        )
        .is_err()
    );
    let shallow = SecParserLimits::try_new(1024, 10, 2, 128, 512)?;
    assert!(XbrlDocumentParser::parse(b"<a><b><c/></b></a>", shallow, context).is_err());
    Ok(())
}
