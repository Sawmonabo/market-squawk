use std::error::Error;

use market_squawk_adapter_sec::{
    SecParserLimits, SecXbrlError, XbrlDocumentContext, XbrlDocumentParser,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, SourceIdentifier, Timestamp,
    XbrlDimensionLocation, XbrlTaxonomySet,
};

fn document_context() -> Result<XbrlDocumentContext, Box<dyn Error>> {
    Ok(XbrlDocumentContext::new(
        SourceIdentifier::try_from("0000320193-25-000079")?,
        XbrlTaxonomySet::declared(
            EvidenceDigest::new(DigestAlgorithm::Sha256, [3; 32]),
            SourceIdentifier::try_from("us-gaap-2025")?,
        ),
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [7; 32],
        )),
        Timestamp::from_unix_nanos(42),
    ))
}

#[test]
fn inline_xbrl_preserves_authoritative_names_graph_units_and_duplicate_accuracy()
-> Result<(), Box<dyn Error>> {
    let bytes = br#"<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:instance="http://www.xbrl.org/2003/instance"
      xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
      xmlns:gaap="http://fasb.org/us-gaap/2025"
      xmlns:alt="http://fasb.org/us-gaap/2025"
      xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
      xmlns:cur="http://www.xbrl.org/2003/iso4217"
      xmlns:custom="https://example.test/taxonomy"
      xmlns:ixt="http://www.xbrl.org/inlineXBRL/transformation/2020-02-12">
      <body><ix:resources>
        <xbrli:context id="C1"><xbrli:entity>
          <xbrli:identifier scheme="http://www.sec.gov/CIK">0000320193</xbrli:identifier>
          <xbrli:segment><custom:explanation role="audit"><custom:line>North</custom:line></custom:explanation>
            <xbrldi:explicitMember dimension="gaap:StatementBusinessSegmentsAxis">gaap:AmericasSegmentMember</xbrldi:explicitMember>
          </xbrli:segment></xbrli:entity>
          <xbrli:period><xbrli:instant>2025-06-28</xbrli:instant></xbrli:period>
          <xbrli:scenario><xbrldi:typedMember dimension="custom:ChannelAxis"><custom:channel code="D">Direct</custom:channel></xbrldi:typedMember></xbrli:scenario>
        </xbrli:context>
        <xbrli:context id="C2"><xbrli:entity>
          <xbrli:identifier scheme="http://www.sec.gov/CIK">0000320193</xbrli:identifier>
          <xbrli:segment><custom:explanation role="audit"><custom:line>North</custom:line></custom:explanation>
            <xbrldi:explicitMember dimension="alt:StatementBusinessSegmentsAxis">alt:AmericasSegmentMember</xbrldi:explicitMember>
          </xbrli:segment></xbrli:entity>
          <xbrli:period><xbrli:instant>2025-06-28</xbrli:instant></xbrli:period>
          <xbrli:scenario><xbrldi:typedMember dimension="custom:ChannelAxis"><custom:channel code="D">Direct</custom:channel></xbrldi:typedMember></xbrli:scenario>
        </xbrli:context>
        <xbrli:unit id="U1"><xbrli:divide><xbrli:unitNumerator>
          <xbrli:measure>iso4217:USD</xbrli:measure>
        </xbrli:unitNumerator><xbrli:unitDenominator>
          <xbrli:measure>xbrli:shares</xbrli:measure>
        </xbrli:unitDenominator></xbrli:divide></xbrli:unit>
        <xbrli:unit id="U2"><xbrli:divide><xbrli:unitNumerator>
          <xbrli:measure>cur:USD</xbrli:measure>
        </xbrli:unitNumerator><xbrli:unitDenominator>
          <xbrli:measure>instance:shares</xbrli:measure>
        </xbrli:unitDenominator></xbrli:divide></xbrli:unit>
      </ix:resources>
      <ix:nonFraction id="amount-1" name="gaap:Amount" contextRef="C1" unitRef="U1"
        decimals="0" format="ixt:num-dot-decimal" continuedAt="amount-cont">100</ix:nonFraction>
      <ix:continuation id="amount-cont">.0</ix:continuation>
      <ix:nonFraction id="amount-2" name="alt:Amount" contextRef="C2" unitRef="U2"
        decimals="1" format="ixt:num-dot-decimal">100.4</ix:nonFraction>
      <ix:nonFraction id="other-1" name="gaap:Other" contextRef="C1" unitRef="U1"
        decimals="0">100</ix:nonFraction>
      <ix:nonFraction id="other-2" name="alt:Other" contextRef="C2" unitRef="U2"
        decimals="0">100.4</ix:nonFraction>
      <ix:nonFraction id="precision-1" name="gaap:Precise" contextRef="C1" unitRef="U1"
        precision="3">100</ix:nonFraction>
      <ix:nonFraction id="precision-2" name="alt:Precise" contextRef="C2" unitRef="U2"
        decimals="1">100.4</ix:nonFraction>
      <ix:nonFraction id="effective-1" name="gaap:EffectiveAccuracy" contextRef="C1" unitRef="U1"
        precision="3">100</ix:nonFraction>
      <ix:nonFraction id="effective-2" name="alt:EffectiveAccuracy" contextRef="C2" unitRef="U2"
        decimals="0">100.4</ix:nonFraction>
      <ix:nonFraction id="exact-1" name="gaap:Exact" contextRef="C1" unitRef="U1"
        precision="INF">5</ix:nonFraction>
      <ix:nonFraction id="exact-2" name="alt:Exact" contextRef="C2" unitRef="U2"
        decimals="INF">5</ix:nonFraction>
      <ix:nonNumeric id="note" name="gaap:Disclosure" contextRef="C1">Parent
        <ix:nonNumeric id="note-child" name="gaap:DisclosureDetail" contextRef="C1">child</ix:nonNumeric>
      </ix:nonNumeric>
      <ix:relationship arcrole="http://www.xbrl.org/2003/arcrole/fact-explanatoryFact"
        fromRefs="amount-1" toRefs="note"/>
    </body></html>"#;
    let document = XbrlDocumentParser::parse(
        bytes,
        SecParserLimits::production_defaults(),
        document_context()?,
    )?;

    assert_eq!(document.numeric_facts().len(), 10);
    let fact = document
        .numeric_facts()
        .iter()
        .find(|fact| fact.concept().as_str() == "gaap:Amount")
        .ok_or("missing amount fact")?;
    assert_eq!(fact.unit().as_str(), "divide(iso4217:USD/xbrli:shares)");
    let wire = serde_json::to_value(fact.evidence())?;
    assert_eq!(wire["concept"]["source_qname"], "gaap:Amount");
    assert_eq!(
        wire["concept"]["namespace_uri"],
        "http://fasb.org/us-gaap/2025"
    );
    assert_eq!(wire["unit"]["kind"], "divide");
    assert_eq!(wire["unit"]["numerator"][0]["source_qname"], "iso4217:USD");
    assert_eq!(wire["unit"]["denominator"][0]["local_name"], "shares");
    assert_eq!(wire["duplicate"]["classification"], "consistent_numeric");
    assert_eq!(wire["taxonomy_set"]["status"], "caller_declared_unresolved");
    assert_eq!(wire["dimensions"][1]["member"]["validation"], "source_only");
    assert!(wire["dimensions"][1]["member"]["source_graph"]["events"].is_array());
    assert!(
        wire["context_graph"]["events"]
            .as_array()
            .is_some_and(|events| {
                events.iter().any(|event| {
                    event["kind"] == "start" && event["name"]["local_name"] == "explanation"
                }) && events
                    .iter()
                    .any(|event| event["kind"] == "start" && event["name"]["local_name"] == "line")
            })
    );
    assert_eq!(
        wire["occurrence_relationships"]["continuation_chain"][0],
        "amount-cont"
    );
    assert_eq!(
        wire["occurrence_relationships"]["relationships"][0]["to_refs"][0],
        "note"
    );

    let classifications: Vec<_> = document
        .numeric_facts()
        .iter()
        .map(|fact| {
            let wire = serde_json::to_value(fact.evidence())?;
            Ok::<_, serde_json::Error>((fact.concept().as_str().to_owned(), wire))
        })
        .collect::<Result<_, _>>()?;
    assert!(classifications.iter().any(|(concept, wire)| {
        concept.ends_with(":Other") && wire["duplicate"]["classification"] == "inconsistent"
    }));
    assert!(classifications.iter().any(|(concept, wire)| {
        concept.ends_with(":Precise") && wire["duplicate"]["classification"] == "consistent_numeric"
    }));
    assert!(classifications.iter().any(|(concept, wire)| {
        concept.ends_with(":EffectiveAccuracy")
            && wire["duplicate"]["classification"] == "inconsistent"
    }));
    assert!(classifications.iter().any(|(concept, wire)| {
        concept.ends_with(":Exact") && wire["duplicate"]["classification"] == "consistent_numeric"
    }));

    let note = document
        .nonnumeric_occurrences()
        .iter()
        .find(|occurrence| occurrence.occurrence_id().as_str() == "note")
        .ok_or("missing parent note")?;
    let note_wire = serde_json::to_value(note.occurrence_relationships())?;
    assert_eq!(note_wire["child_occurrence_ids"][0], "note-child");
    Ok(())
}

#[test]
fn non_dimensional_context_content_prevents_false_duplicate_grouping() -> Result<(), Box<dyn Error>>
{
    let bytes = br#"<root xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:gaap="http://fasb.org/us-gaap/2025"
      xmlns:custom="https://example.test/taxonomy">
      <xbrli:context id="C1"><xbrli:entity>
        <xbrli:identifier scheme="sec">0000320193</xbrli:identifier>
        <xbrli:segment><custom:region>North</custom:region></xbrli:segment>
      </xbrli:entity><xbrli:period><xbrli:instant>2025-06-28</xbrli:instant></xbrli:period>
      </xbrli:context>
      <xbrli:context id="C2"><xbrli:entity>
        <xbrli:identifier scheme="sec">0000320193</xbrli:identifier>
        <xbrli:segment><custom:region>South</custom:region></xbrli:segment>
      </xbrli:entity><xbrli:period><xbrli:instant>2025-06-28</xbrli:instant></xbrli:period>
      </xbrli:context>
      <xbrli:unit id="U"><xbrli:measure>xbrli:pure</xbrli:measure></xbrli:unit>
      <gaap:Amount id="north" contextRef="C1" unitRef="U" decimals="0">10</gaap:Amount>
      <gaap:Amount id="south" contextRef="C2" unitRef="U" decimals="0">10</gaap:Amount>
    </root>"#;

    let document = XbrlDocumentParser::parse(
        bytes,
        SecParserLimits::production_defaults(),
        document_context()?,
    )?;
    assert_eq!(document.numeric_facts().len(), 2);
    for fact in document.numeric_facts() {
        let wire = serde_json::to_value(fact.evidence())?;
        assert_eq!(wire["duplicate"]["classification"], "unique");
    }
    Ok(())
}

#[test]
fn xbrl_rejects_namespace_spoofing_attribute_ambiguity_and_invalid_divide_units()
-> Result<(), Box<dyn Error>> {
    let spoofed_context = br#"<root xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:fake="https://attacker.test/xbrli" xmlns:gaap="https://example.test/gaap">
      <fake:context id="C"><xbrli:entity><xbrli:identifier scheme="s">e</xbrli:identifier>
      </xbrli:entity><xbrli:period><xbrli:instant>2025-01-01</xbrli:instant></xbrli:period>
      </fake:context><gaap:Amount contextRef="C" unitRef="U">1</gaap:Amount></root>"#;
    assert!(matches!(
        XbrlDocumentParser::parse(
            spoofed_context,
            SecParserLimits::production_defaults(),
            document_context()?,
        ),
        Err(SecXbrlError::UnknownContext)
    ));

    let ambiguous_attribute = br#"<root xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:evil="https://attacker.test"><xbrli:context id="C" evil:id="shadow"/></root>"#;
    assert!(matches!(
        XbrlDocumentParser::parse(
            ambiguous_attribute,
            SecParserLimits::production_defaults(),
            document_context()?,
        ),
        Err(SecXbrlError::AmbiguousSemanticAttribute)
    ));

    let spoofed_transform = br#"<root xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:gaap="http://fasb.org/us-gaap/2025" xmlns:evil="https://attacker.test">
      <xbrli:context id="C"><xbrli:entity><xbrli:identifier scheme="sec">1</xbrli:identifier>
      </xbrli:entity><xbrli:period><xbrli:instant>2025-01-01</xbrli:instant></xbrli:period>
      </xbrli:context><xbrli:unit id="U"><xbrli:measure>xbrli:pure</xbrli:measure></xbrli:unit>
      <ix:nonFraction name="gaap:Amount" contextRef="C" unitRef="U" decimals="0"
        format="evil:num-dot-decimal">1,000</ix:nonFraction></root>"#;
    assert!(matches!(
        XbrlDocumentParser::parse(
            spoofed_transform,
            SecParserLimits::production_defaults(),
            document_context()?,
        ),
        Err(SecXbrlError::UnsupportedTransform)
    ));

    let invalid_divide = br#"<root xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:iso4217="http://www.xbrl.org/2003/iso4217"><xbrli:unit id="U">
      <xbrli:divide><xbrli:unitNumerator><xbrli:measure>iso4217:USD</xbrli:measure>
      </xbrli:unitNumerator><xbrli:unitDenominator><xbrli:measure>iso4217:USD</xbrli:measure>
      </xbrli:unitDenominator></xbrli:divide></xbrli:unit></root>"#;
    assert!(matches!(
        XbrlDocumentParser::parse(
            invalid_divide,
            SecParserLimits::production_defaults(),
            document_context()?,
        ),
        Err(SecXbrlError::InvalidUnitExpression)
    ));
    Ok(())
}

#[test]
fn xbrl_rejects_doctype_and_depth_exhaustion() -> Result<(), Box<dyn Error>> {
    assert!(
        XbrlDocumentParser::parse(
            b"<!DOCTYPE x [<!ENTITY ex SYSTEM 'file:///etc/passwd'>]><x>&ex;</x>",
            SecParserLimits::production_defaults(),
            document_context()?,
        )
        .is_err()
    );
    let shallow = SecParserLimits::try_new(1024, 10, 2, 128, 512, 4096)?;
    assert!(
        XbrlDocumentParser::parse(b"<a><b><c/></b></a>", shallow, document_context()?).is_err()
    );
    let _typed_location = XbrlDimensionLocation::Segment;
    Ok(())
}

#[test]
fn xbrl_rejects_aggregate_retained_output_amplification() -> Result<(), Box<dyn Error>> {
    let facts = (0..24)
        .map(|index| {
            format!(
                r#"<ix:nonFraction id="f{index}" name="gaap:Amount" contextRef="C" unitRef="U" decimals="2" continuedAt="tail">1</ix:nonFraction>"#
            )
        })
        .collect::<String>();
    let references = (0..24)
        .map(|index| format!("f{index}"))
        .collect::<Vec<_>>()
        .join(" ");
    let document = format!(
        r#"<html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
          xmlns:xbrli="http://www.xbrl.org/2003/instance"
          xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
          xmlns:gaap="http://fasb.org/us-gaap/2025"
          xmlns:custom="https://example.test/taxonomy">
          <ix:resources>
            <xbrli:context id="C"><xbrli:entity><xbrli:identifier scheme="sec">1</xbrli:identifier>
              <xbrli:segment><custom:graph label="retained"><custom:node>evidence</custom:node></custom:graph>
                <xbrldi:typedMember dimension="custom:Axis"><custom:member code="A">shared</custom:member></xbrldi:typedMember>
              </xbrli:segment></xbrli:entity>
              <xbrli:period><xbrli:instant>2025-01-01</xbrli:instant></xbrli:period>
            </xbrli:context>
            <xbrli:unit id="U"><xbrli:measure>xbrli:pure</xbrli:measure></xbrli:unit>
          </ix:resources>
          {facts}<ix:continuation id="tail">.23456789</ix:continuation>
          <ix:nonNumeric id="note" name="gaap:Disclosure" contextRef="C">note</ix:nonNumeric>
          <ix:relationship arcrole="http://www.xbrl.org/2003/arcrole/fact-explanatoryFact"
            fromRefs="{references}" toRefs="note"/>
        </html>"#
    );
    let retained_limit = document.len() + 512;
    let limits = SecParserLimits::try_new(
        document.len() + 1,
        64,
        32,
        1024,
        document.len(),
        retained_limit,
    )?;

    assert!(matches!(
        XbrlDocumentParser::parse(document.as_bytes(), limits, document_context()?),
        Err(SecXbrlError::RetainedOutputLimitExceeded)
    ));
    Ok(())
}
