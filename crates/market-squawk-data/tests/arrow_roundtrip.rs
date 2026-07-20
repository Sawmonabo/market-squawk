use std::error::Error;

use arrow::array::{Decimal128Array, UInt8Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use market_squawk_data::{ArrowConversionError, ResearchArrowBatch};
use market_squawk_domain::{
    AvailabilityEvidence, DataQuality, DigestAlgorithm, EvidenceDigest, MacroObservation,
    PayloadReference, ResearchContext, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTime, RevisionNumber, SourceId, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn arrow_roundtrip_preserves_exact_decimal_and_point_in_time_metadata() -> TestResult {
    let observation = macro_observation(Decimal::from_parts(123_456_789, 0, 0, false, 7))?;
    let expected = vec![observation];
    let converted = ResearchArrowBatch::try_from_observations(
        SourceIdentifier::try_from("fred-gdp")?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [9; 32]),
        expected.clone(),
    )?;
    let batch = converted.record_batch();
    assert_eq!(
        batch
            .schema()
            .field_with_name("value_mantissa")?
            .data_type(),
        &DataType::Decimal128(38, 0)
    );
    let mantissas = batch
        .column_by_name("value_mantissa")
        .and_then(|array| array.as_any().downcast_ref::<Decimal128Array>())
        .ok_or("missing decimal mantissa")?;
    let scales = batch
        .column_by_name("value_scale")
        .and_then(|array| array.as_any().downcast_ref::<UInt8Array>())
        .ok_or("missing decimal scale")?;
    assert_eq!(mantissas.value(0), 123_456_789);
    assert_eq!(scales.value(0), 7);
    assert_eq!(converted.observations()?, expected);
    let differently_bound = ResearchArrowBatch::try_from_observations(
        SourceIdentifier::try_from("fred-gdp")?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [10; 32]),
        converted.observations()?,
    )?;
    assert_ne!(
        converted.lineage_digest()?,
        differently_bound.lineage_digest()?
    );
    Ok(())
}

#[test]
fn arrow_reader_rejects_an_unsupported_schema_version() -> TestResult {
    let converted = ResearchArrowBatch::try_from_observations(
        SourceIdentifier::try_from("fred-gdp")?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [9; 32]),
        vec![macro_observation(Decimal::new(1, 0))?],
    )?;
    let mut metadata = converted.record_batch().schema().metadata().clone();
    metadata.insert("market_squawk.schema_version".to_owned(), "2".to_owned());
    let schema = converted
        .record_batch()
        .schema()
        .as_ref()
        .clone()
        .with_metadata(metadata);
    let hostile = RecordBatch::try_new(schema.into(), converted.record_batch().columns().to_vec())?;
    assert!(matches!(
        ResearchArrowBatch::try_from_record_batch(hostile),
        Err(ArrowConversionError::UnsupportedSchemaVersion { found: 2 })
    ));
    Ok(())
}

fn macro_observation(value: Decimal) -> Result<ResearchObservation, Box<dyn Error>> {
    let context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("fred-local-fixture")?,
            instrument_id: None,
            venue_id: None,
            source_identifier: SourceIdentifier::try_from("GDP:2026Q1:v1")?,
            source_timestamp: None,
            received_at: Timestamp::from_unix_nanos(110),
            ingested_at: Timestamp::from_unix_nanos(120),
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::SourceReference(SourceIdentifier::try_from(
                "fred:gdp:2026q1",
            )?),
            availability: AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(100),
                SourceIdentifier::try_from("fred-release")?,
            ),
        })?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(90),
            Some(Timestamp::from_unix_nanos(100)),
            RevisionNumber::new(1)?,
            Some(Timestamp::from_unix_nanos(200)),
        )?,
    )?;
    Ok(ResearchObservation::Macro(MacroObservation::new(
        context,
        SourceIdentifier::try_from("GDP")?,
        value,
        SourceIdentifier::try_from("USD")?,
    )))
}
