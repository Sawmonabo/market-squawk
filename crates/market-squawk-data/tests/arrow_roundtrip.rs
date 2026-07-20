use std::error::Error;

use std::sync::Arc;

use arrow::array::{
    Array as _, ArrayRef, Decimal128Array, StringArray, TimestampNanosecondArray, UInt8Array,
};
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

#[test]
fn availability_projection_is_conservative_typed_and_tamper_evident() -> TestResult {
    let observations = vec![
        macro_observation_with_availability(
            Decimal::new(1, 0),
            AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(100),
                SourceIdentifier::try_from("fred-release")?,
            ),
        )?,
        macro_observation_with_availability(
            Decimal::new(2, 0),
            AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(101)),
        )?,
        macro_observation_with_availability(
            Decimal::new(3, 0),
            AvailabilityEvidence::inferred(
                Timestamp::from_unix_nanos(102),
                SourceIdentifier::try_from("release-calendar-v2")?,
            ),
        )?,
        macro_observation_with_availability(Decimal::new(4, 0), AvailabilityEvidence::unknown())?,
    ];
    let converted = ResearchArrowBatch::try_from_observations(
        SourceIdentifier::try_from("fred-gdp")?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [11; 32]),
        observations,
    )?;
    let batch = converted.record_batch();
    let available = timestamp_column(batch, "available_at")?;
    let reported = timestamp_column(batch, "availability_reported_or_inferred_at")?;
    let kinds = string_column(batch, "availability_kind")?;
    let evidence = string_column(batch, "availability_evidence")?;
    let methods = string_column(batch, "availability_method")?;

    assert_eq!(
        available.iter().collect::<Vec<_>>(),
        vec![Some(100), Some(101), None, None]
    );
    assert_eq!(
        reported.iter().collect::<Vec<_>>(),
        vec![Some(100), Some(101), Some(102), None]
    );
    assert_eq!(
        kinds.iter().collect::<Vec<_>>(),
        vec![
            Some("evidenced"),
            Some("local_first_observed"),
            Some("inferred"),
            Some("unknown"),
        ]
    );
    assert_eq!(
        evidence.iter().collect::<Vec<_>>(),
        vec![Some("fred-release"), None, None, None]
    );
    assert_eq!(
        methods.iter().collect::<Vec<_>>(),
        vec![None, None, Some("release-calendar-v2"), None]
    );

    let kind_index = batch.schema().index_of("availability_kind")?;
    let mut columns = batch.columns().to_vec();
    columns[kind_index] = Arc::new(StringArray::from(vec![
        Some("inferred"),
        Some("local_first_observed"),
        Some("inferred"),
        Some("unknown"),
    ])) as ArrayRef;
    let hostile = RecordBatch::try_new(batch.schema(), columns)?;
    assert!(matches!(
        ResearchArrowBatch::try_from_record_batch(hostile),
        Err(ArrowConversionError::ProjectionMismatch)
    ));
    Ok(())
}

fn timestamp_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a TimestampNanosecondArray, Box<dyn Error>> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<TimestampNanosecondArray>())
        .ok_or_else(|| format!("missing timestamp column {name}").into())
}

fn string_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a StringArray, Box<dyn Error>> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| format!("missing string column {name}").into())
}

fn macro_observation(value: Decimal) -> Result<ResearchObservation, Box<dyn Error>> {
    macro_observation_with_availability(
        value,
        AvailabilityEvidence::evidenced(
            Timestamp::from_unix_nanos(100),
            SourceIdentifier::try_from("fred-release")?,
        ),
    )
}

fn macro_observation_with_availability(
    value: Decimal,
    availability: AvailabilityEvidence,
) -> Result<ResearchObservation, Box<dyn Error>> {
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
            availability,
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
