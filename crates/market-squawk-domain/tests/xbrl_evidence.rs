use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    AvailabilityEvidence, DataQuality, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence,
    FundamentalObservation, InstrumentId, PayloadReference, ResearchContext, ResearchProvenance,
    ResearchProvenanceInput, ResearchTime, RevisionNumber, SourceId, SourceIdentifier, Timestamp,
    XbrlAccuracy, XbrlAccuracyValue, XbrlDuplicateClass, XbrlDuplicateEvidence, XbrlEntity,
    XbrlFactEvidence, XbrlFactEvidenceInput, XbrlPeriod, XbrlTaxonomySet,
};
use rust_decimal::Decimal;

fn research_context() -> Result<ResearchContext, Box<dyn Error>> {
    Ok(ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("sec-edgar")?,
            instrument_id: Some(InstrumentId::from_str(
                "0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb",
            )?),
            venue_id: None,
            source_identifier: SourceIdentifier::try_from("0000320193-25-000079")?,
            source_timestamp: None,
            received_at: Timestamp::from_unix_nanos(20),
            ingested_at: Timestamp::from_unix_nanos(30),
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::SourceReference(SourceIdentifier::try_from(
                "sec-fixture",
            )?),
            availability: AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(10),
                SourceIdentifier::try_from("sec-retrieval-evidence")?,
            ),
        })?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(5),
            Some(Timestamp::from_unix_nanos(10)),
            RevisionNumber::new(1)?,
            None,
        )?,
    )?)
}

#[test]
fn xbrl_evidence_round_trips_and_binds_the_exact_normalized_value() -> Result<(), Box<dyn Error>> {
    let context = research_context()?;
    let evidence = XbrlFactEvidence::try_new(XbrlFactEvidenceInput {
        occurrence_id: SourceIdentifier::try_from("fact-7")?,
        accession: SourceIdentifier::try_from("0000320193-25-000079")?,
        context_id: SourceIdentifier::try_from("D2025Q2")?,
        unit_id: SourceIdentifier::try_from("USD")?,
        entity: XbrlEntity::try_new("http://www.sec.gov/CIK", "0000320193")?,
        period: XbrlPeriod::duration(
            market_squawk_domain::CalendarDate::new(2025, 3, 30)?,
            market_squawk_domain::CalendarDate::new(2025, 6, 28)?,
        )?,
        accuracy: XbrlAccuracy::Decimals(XbrlAccuracyValue::Finite(-6)),
        lexical_value: "23434000000".try_into()?,
        transformed_lexeme: None,
        inline_scale: Some(0),
        inline_sign: Some(market_squawk_domain::XbrlSign::Negative),
        dimensions: Vec::new(),
        segment_evidence: None,
        language: Some(SourceIdentifier::try_from("en-US")?),
        duplicate: XbrlDuplicateEvidence::try_new(
            XbrlDuplicateClass::Unique,
            None,
            SourceIdentifier::try_from("sec-xbrl-duplicate-v1")?,
        )?,
        taxonomy_set: XbrlTaxonomySet::new(
            EvidenceDigest::new(DigestAlgorithm::Sha256, [3; 32]),
            SourceIdentifier::try_from("us-gaap-2025")?,
        ),
        source_payload: ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [7; 32],
        )),
        parser_ruleset: SourceIdentifier::try_from("sec-xbrl-parser-v1")?,
        rounding_ruleset: SourceIdentifier::try_from("sec-xbrl-rounding-v1")?,
        evaluated_at: Timestamp::from_unix_nanos(42),
    })?;
    let observation = FundamentalObservation::new_with_xbrl_evidence(
        context,
        SourceIdentifier::try_from("us-gaap:NetIncomeLoss")?,
        Decimal::from(-23_434_000_000_i64),
        SourceIdentifier::try_from("USD")?,
        evidence,
    )?;

    let wire = serde_json::to_string(&observation)?;
    let restored: FundamentalObservation = serde_json::from_str(&wire)?;
    assert_eq!(restored, observation);
    assert_eq!(
        restored
            .xbrl_evidence()
            .ok_or("missing evidence")?
            .accession()
            .as_str(),
        "0000320193-25-000079"
    );

    let mut mismatched: serde_json::Value = serde_json::from_str(&wire)?;
    mismatched["value"] = serde_json::Value::String("1".to_owned());
    assert!(serde_json::from_value::<FundamentalObservation>(mismatched).is_err());
    Ok(())
}

#[test]
fn xbrl_evidence_rejects_inverted_periods_and_unbounded_typed_members() -> Result<(), Box<dyn Error>>
{
    assert!(
        XbrlPeriod::duration(
            market_squawk_domain::CalendarDate::new(2025, 6, 28)?,
            market_squawk_domain::CalendarDate::new(2025, 3, 30)?,
        )
        .is_err()
    );
    assert!(
        market_squawk_domain::XbrlText::try_from(
            "x".repeat(market_squawk_domain::XbrlText::MAX_LENGTH + 1)
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<XbrlEntity>(serde_json::json!({
            "scheme": "",
            "value": "0000320193"
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<XbrlPeriod>(serde_json::json!({
            "kind": "duration",
            "start": { "year": 2025, "month": 6, "day": 28 },
            "end": { "year": 2025, "month": 3, "day": 30 }
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<XbrlDuplicateEvidence>(serde_json::json!({
            "classification": "inconsistent",
            "ruleset": "sec-xbrl-duplicate-v1"
        }))
        .is_err()
    );
    Ok(())
}
