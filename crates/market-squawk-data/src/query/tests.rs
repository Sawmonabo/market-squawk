use std::error::Error;
use std::time::Duration;

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, FixedSizeBinaryArray, Float64Array, Int64Array,
    TimestampNanosecondArray, UInt8Array, UInt32Array, builder::FixedSizeBinaryBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use market_squawk_domain::{
    AvailabilityEvidence, DataQuality, DigestAlgorithm, EvidenceDigest, MacroObservation,
    PayloadReference, ResearchContext, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTime, RevisionNumber, SourceId, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    PinnedFeatureMonetaryValue, PinnedQueryOutput, QueryError, QueryLimits, QueryRequest,
    QueryResult, ResearchQueryEngine, feature_monetary_sql,
};
use crate::{
    DatasetId, DatasetManifestRef, DatasetSchemaRegistry, ResearchArrowBatch, Sha256Digest,
};

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn query_artifact_identity_binds_exact_row_schema() -> TestResult {
    let registry = DatasetSchemaRegistry::local();
    let first = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("fred-gdp")?,
        7,
        registry.canonical_research_observations()?,
        Sha256Digest::new([7; 32]),
    )?;
    let second = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("fred-gdp")?,
        7,
        registry.canonical_feature_labels()?,
        Sha256Digest::new([7; 32]),
    )?;
    let limits = QueryLimits::try_new(
        2,
        4096,
        8 * 1024 * 1024,
        1,
        128,
        128,
        Duration::from_secs(1),
    )?;

    assert_ne!(
        QueryRequest::try_new(first, "SELECT 1")?.artifact_identity(&limits),
        QueryRequest::try_new(second, "SELECT 1")?.artifact_identity(&limits)
    );
    Ok(())
}

#[tokio::test]
async fn fixed_width_feature_query_issues_exact_monetary_receipt() -> TestResult {
    let registry = DatasetSchemaRegistry::local();
    let schema_ref = registry.canonical_feature_labels()?;
    let manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("training-features")?,
        1,
        schema_ref.clone(),
        Sha256Digest::new([7; 32]),
    )?;
    let instrument = Uuid::parse_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1")?;
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(fixed(Some(b"example-1"), 256)?),
        Arc::new(fixed(Some(instrument.as_bytes()), 16)?),
        Arc::new(TimestampNanosecondArray::from(vec![100]).with_timezone("+00:00")),
        Arc::new(UInt8Array::from(vec![1])),
        Arc::new(UInt8Array::from(vec![1])),
        Arc::new(fixed(Some(b"market-value"), 256)?),
        Arc::new(UInt32Array::from(vec![1])),
        Arc::new(Float64Array::from(vec![None])),
        Arc::new(Decimal128Array::from(vec![Some(123_456)]).with_precision_and_scale(38, 0)?),
        Arc::new(UInt8Array::from(vec![Some(2)])),
        Arc::new(fixed(Some(b"currency"), 32)?),
        Arc::new(fixed(Some(b"USD"), 3)?),
        Arc::new(fixed(None, 256)?),
        Arc::new(fixed(Some(&[9; 32]), 32)?),
    ];
    let batch = RecordBatch::try_new(registry.resolve(&schema_ref)?, arrays)?;
    let engine =
        ResearchQueryEngine::from_pinned_batches(manifest.clone(), "features", vec![batch])?;
    let result = engine
        .query(
            QueryRequest::try_new(manifest.clone(), feature_monetary_sql("features", 0))?,
            QueryLimits::try_new(
                1,
                64 * 1024,
                8 * 1024 * 1024,
                1,
                128,
                128,
                Duration::from_secs(1),
            )?,
            CancellationToken::new(),
        )
        .await?;
    let output = PinnedQueryOutput::new(
        manifest,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [2; 32]),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [3; 32]),
        result,
    );
    let value = PinnedFeatureMonetaryValue::try_from_output(&output, 0)?;

    assert_eq!(value.component_name(), "market-value");
    assert_eq!(value.currency().as_str(), "USD");
    assert_eq!(value.decimal()?, Decimal::new(123_456, 2));
    Ok(())
}

fn fixed(value: Option<&[u8]>, width: i32) -> Result<FixedSizeBinaryArray, Box<dyn Error>> {
    let mut builder = FixedSizeBinaryBuilder::new(width);
    if let Some(value) = value {
        let mut padded = vec![0; usize::try_from(width)?];
        padded
            .get_mut(..value.len())
            .ok_or("fixed-width fixture value exceeds its column")?
            .copy_from_slice(value);
        builder.append_value(&padded)?;
    } else {
        builder.append_null();
    }
    Ok(builder.finish())
}

#[tokio::test]
async fn query_service_allows_only_bounded_read_only_sql() -> TestResult {
    let manifest = manifest()?;
    let batch = RecordBatch::try_new(
        Schema::new(vec![Field::new("value", DataType::Int64, false)]).into(),
        vec![std::sync::Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )?;
    let engine =
        ResearchQueryEngine::from_pinned_batches(manifest.clone(), "observations", vec![batch])?;
    let limits = QueryLimits::try_new(
        2,
        4096,
        8 * 1024 * 1024,
        1,
        128,
        128,
        Duration::from_secs(1),
    )?;
    let result = engine
        .query(
            QueryRequest::try_new(
                manifest.clone(),
                "WITH bounded AS (SELECT value FROM observations) \
                 SELECT value FROM bounded ORDER BY value",
            )?,
            limits,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        result,
        Err(QueryError::RowLimitExceeded { limit: 2 })
    ));

    for forbidden in [
        "DELETE FROM observations",
        "CREATE EXTERNAL TABLE x STORED AS PARQUET LOCATION '/tmp/x'",
        "COPY observations TO '/tmp/x'",
        "SELECT * FROM read_parquet('/tmp/x')",
        "INSTALL extension",
    ] {
        assert!(matches!(
            QueryRequest::try_new(manifest.clone(), forbidden),
            Err(QueryError::ForbiddenStatement)
                | Err(QueryError::ForbiddenTableFunction)
                | Err(QueryError::Parse(_))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn available_at_cutoff_excludes_inferred_and_unknown_rows() -> TestResult {
    let manifest = manifest()?;
    let observations = [
        AvailabilityEvidence::evidenced(
            Timestamp::from_unix_nanos(100),
            SourceIdentifier::try_from("release-record")?,
        ),
        AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(101)),
        AvailabilityEvidence::inferred(
            Timestamp::from_unix_nanos(102),
            SourceIdentifier::try_from("calendar-v1")?,
        ),
        AvailabilityEvidence::unknown(),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, availability)| observation(index, availability))
    .collect::<Result<Vec<_>, _>>()?;
    let batch = ResearchArrowBatch::try_from_observations(
        SourceIdentifier::try_from("fred-gdp")?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [12; 32]),
        observations,
    )?;
    let engine = ResearchQueryEngine::from_pinned_batches(
        manifest.clone(),
        "observations",
        vec![batch.record_batch().clone()],
    )?;
    let result = engine
        .query(
            QueryRequest::try_new(
                manifest,
                "SELECT available_at FROM observations WHERE available_at IS NOT NULL",
            )?,
            QueryLimits::try_new(
                4,
                64 * 1024,
                8 * 1024 * 1024,
                1,
                128,
                128,
                Duration::from_secs(1),
            )?,
            CancellationToken::new(),
        )
        .await?;
    let QueryResult::Inline { batches, .. } = result else {
        return Err("expected inline result".into());
    };
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    Ok(())
}

fn observation(
    index: usize,
    availability: AvailabilityEvidence,
) -> Result<ResearchObservation, Box<dyn Error>> {
    let context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("fred-local-fixture")?,
            instrument_id: None,
            venue_id: None,
            source_identifier: SourceIdentifier::try_from(format!("GDP:2026Q1:{index}"))?,
            source_timestamp: None,
            received_at: Timestamp::from_unix_nanos(110),
            ingested_at: Timestamp::from_unix_nanos(120),
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::SourceReference(SourceIdentifier::try_from(
                format!("fred:gdp:2026q1:{index}"),
            )?),
            availability,
        })?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(90),
            Some(Timestamp::from_unix_nanos(100)),
            RevisionNumber::new(1)?,
            None,
        )?,
    )?;
    Ok(ResearchObservation::Macro(MacroObservation::new(
        context,
        SourceIdentifier::try_from("GDP")?,
        Decimal::new(i64::try_from(index)? + 1, 0),
        SourceIdentifier::try_from("USD")?,
    )))
}

fn manifest() -> Result<DatasetManifestRef, Box<dyn Error>> {
    Ok(DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("fred-gdp")?,
        7,
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([7; 32]),
    )?)
}
