use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest,
    ExactPayloadEvidence, FundamentalAmendmentStatus, FundamentalCadence, FundamentalConsolidation,
    FundamentalDimensionContext, FundamentalFactContext, FundamentalFactContextInput,
    FundamentalObservation, FundamentalPeriod, FundamentalRestatementStatus,
    FundamentalRevisionOrder, InstrumentId, PayloadReference, ResearchContext, ResearchProvenance,
    ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime, RevisionNumber,
    SchemaVersion, SourceId, SourceIdentifier, Timestamp, XbrlAccuracy, XbrlAccuracyValue,
    XbrlContextGraph, XbrlDimensionEvidence, XbrlDimensionLocation, XbrlDimensionMember,
    XbrlDuplicateClass, XbrlDuplicateEvidence, XbrlEntity, XbrlFactEvidence, XbrlFactEvidenceInput,
    XbrlOccurrenceRelationships, XbrlPeriod, XbrlQualifiedName, XbrlRelationshipEvidence,
    XbrlTaxonomySet, XbrlTypedMemberValidation, XbrlUnitExpression, XbrlXmlEvent,
};
use rust_decimal::Decimal;

fn research_context() -> Result<ResearchContext, Box<dyn Error>> {
    let effective = CalendarDate::new(2025, 6, 28)?;
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
        ResearchTime::try_new_with_coordinates(
            ResearchTemporalCoordinate::calendar_date(effective),
            None,
            RevisionNumber::new(1)?,
            None,
        )?,
    )?)
}

#[test]
fn xbrl_evidence_round_trips_and_binds_the_exact_normalized_value() -> Result<(), Box<dyn Error>> {
    let context = research_context()?;
    let concept =
        XbrlQualifiedName::try_new("us-gaap:NetIncomeLoss", "http://fasb.org/us-gaap/2025")?;
    let unit = XbrlUnitExpression::measure(XbrlQualifiedName::try_new(
        "iso4217:USD",
        "http://www.xbrl.org/2003/iso4217",
    )?);
    let channel_name =
        XbrlQualifiedName::try_new("custom:channel", "https://example.test/taxonomy")?;
    let typed_source_graph = XbrlContextGraph::try_new(vec![
        XbrlXmlEvent::Start {
            name: channel_name.clone(),
        },
        XbrlXmlEvent::Attribute {
            name: XbrlQualifiedName::unqualified("code")?,
            value: "D".try_into()?,
        },
        XbrlXmlEvent::Text {
            value: "Direct".try_into()?,
        },
        XbrlXmlEvent::End { name: channel_name },
    ])?;
    let context_graph = XbrlContextGraph::try_new(vec![
        XbrlXmlEvent::Start {
            name: XbrlQualifiedName::try_new(
                "xbrli:scenario",
                "http://www.xbrl.org/2003/instance",
            )?,
        },
        XbrlXmlEvent::Start {
            name: XbrlQualifiedName::try_new("xbrldi:typedMember", "http://xbrl.org/2006/xbrldi")?,
        },
        XbrlXmlEvent::End {
            name: XbrlQualifiedName::try_new("xbrldi:typedMember", "http://xbrl.org/2006/xbrldi")?,
        },
        XbrlXmlEvent::End {
            name: XbrlQualifiedName::try_new(
                "xbrli:scenario",
                "http://www.xbrl.org/2003/instance",
            )?,
        },
    ])?;
    let relationships = XbrlOccurrenceRelationships::try_new(
        None,
        vec![SourceIdentifier::try_from("fact-7-child")?],
        vec![SourceIdentifier::try_from("fact-7-continuation")?],
        vec![XbrlRelationshipEvidence::try_new(
            SourceIdentifier::try_from("http://www.xbrl.org/2003/arcrole/fact-explanatoryFact")?,
            vec![SourceIdentifier::try_from("fact-7")?],
            vec![SourceIdentifier::try_from("fact-note")?],
            None,
            None,
        )?],
    )?;
    let evidence = XbrlFactEvidence::try_new(XbrlFactEvidenceInput {
        occurrence_id: SourceIdentifier::try_from("fact-7")?,
        accession: SourceIdentifier::try_from("0000320193-25-000079")?,
        context_id: SourceIdentifier::try_from("D2025Q2")?,
        unit_id: SourceIdentifier::try_from("USD")?,
        concept,
        unit,
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
        dimensions: vec![XbrlDimensionEvidence::new(
            XbrlQualifiedName::try_new("custom:ChannelAxis", "https://example.test/taxonomy")?,
            XbrlDimensionMember::Typed {
                source_graph: typed_source_graph,
                validation: XbrlTypedMemberValidation::SourceOnly,
            },
            XbrlDimensionLocation::Scenario,
        )],
        context_graph,
        occurrence_relationships: relationships,
        language: Some(SourceIdentifier::try_from("en-US")?),
        duplicate: XbrlDuplicateEvidence::try_new(
            XbrlDuplicateClass::Unique,
            None,
            SourceIdentifier::try_from("sec-xbrl-duplicate-v1")?,
        )?,
        taxonomy_set: XbrlTaxonomySet::declared(
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
    let fact_context = FundamentalFactContext::try_new(FundamentalFactContextInput {
        schema_version: SchemaVersion::CURRENT,
        period: FundamentalPeriod::duration(
            CalendarDate::new(2025, 3, 30)?,
            CalendarDate::new(2025, 6, 28)?,
        )?,
        unit: SourceIdentifier::try_from("iso4217:USD")?,
        accession: SourceIdentifier::try_from("0000320193-25-000079")?,
        filing_form: None,
        amendment_status: FundamentalAmendmentStatus::Unavailable,
        filed_on: None,
        frame: None,
        fiscal_year: None,
        fiscal_period: None,
        cadence: FundamentalCadence::Unavailable,
        xbrl_context_id: Some(evidence.context_id().clone()),
        dimensions: FundamentalDimensionContext::try_source_reported(evidence.dimensions())?,
        consolidation: FundamentalConsolidation::Unavailable,
        revision_order: FundamentalRevisionOrder::new(
            RevisionNumber::new(1)?,
            SourceIdentifier::try_from("sec-inline-xbrl-order-v1")?,
        ),
        restatement_status: FundamentalRestatementStatus::Unavailable,
    })?;
    let observation = FundamentalObservation::new_with_xbrl_evidence(
        context,
        SourceIdentifier::try_from("us-gaap:NetIncomeLoss")?,
        Decimal::from(-23_434_000_000_i64),
        fact_context,
        evidence,
    )?;

    let wire = serde_json::to_string(&observation)?;
    let restored: FundamentalObservation = serde_json::from_str(&wire)?;
    assert_eq!(restored, observation);
    let evidence_wire = serde_json::to_value(
        restored
            .xbrl_evidence()
            .ok_or("missing restored evidence")?,
    )?;
    assert_eq!(evidence_wire["schema_version"], 2);
    assert_eq!(
        evidence_wire["taxonomy_set"]["status"],
        "caller_declared_unresolved"
    );
    assert_eq!(
        evidence_wire["dimensions"][0]["member"]["validation"],
        "source_only"
    );
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
fn xbrl_evidence_rejects_invalid_graph_units_and_overstated_authority() -> Result<(), Box<dyn Error>>
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
    let name = XbrlQualifiedName::try_new("custom:value", "https://example.test/taxonomy")?;
    assert!(XbrlContextGraph::try_new(vec![XbrlXmlEvent::Start { name: name.clone() }]).is_err());
    assert!(
        XbrlUnitExpression::divide(Vec::new(), vec![name.clone()]).is_err(),
        "divide units require a numerator"
    );
    assert!(
        XbrlUnitExpression::divide(vec![name.clone()], vec![name]).is_err(),
        "the same expanded measure cannot appear on both sides"
    );
    let declared_taxonomy = XbrlTaxonomySet::declared(
        EvidenceDigest::new(DigestAlgorithm::Sha256, [3; 32]),
        SourceIdentifier::try_from("us-gaap-2025")?,
    );
    let mut overstated_taxonomy = serde_json::to_value(declared_taxonomy)?;
    overstated_taxonomy["status"] = serde_json::json!("resolved_and_validated");
    assert!(
        serde_json::from_value::<XbrlTaxonomySet>(overstated_taxonomy).is_err(),
        "unresolved caller input cannot deserialize as validated taxonomy evidence"
    );
    Ok(())
}
