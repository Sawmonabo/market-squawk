use std::error::Error;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use market_squawk_data::{
    DatasetId, DatasetManifestRef, QueryError, QueryLimits, QueryRequest, QueryResult,
    ResearchQueryEngine, Sha256Digest,
};
use tokio_util::sync::CancellationToken;

type TestResult = Result<(), Box<dyn Error>>;

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
                "WITH bounded AS (SELECT value FROM observations) SELECT value FROM bounded ORDER BY value",
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
async fn query_service_honors_cancellation_and_manifest_pinning() -> TestResult {
    let manifest = manifest()?;
    let engine = ResearchQueryEngine::from_pinned_batches(
        manifest.clone(),
        "observations",
        vec![RecordBatch::new_empty(Schema::empty().into())],
    )?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = engine
        .query(
            QueryRequest::try_new(manifest, "SELECT 1")?,
            QueryLimits::try_new(1, 1024, 1024 * 1024, 1, 32, 32, Duration::from_secs(1))?,
            cancellation,
        )
        .await;
    assert!(matches!(result, Err(QueryError::Cancelled)));
    assert!(!matches!(result, Ok(QueryResult::Inline { .. })));
    Ok(())
}

fn manifest() -> Result<DatasetManifestRef, Box<dyn Error>> {
    Ok(DatasetManifestRef::try_new(
        DatasetId::try_from("fred-gdp")?,
        7,
        Sha256Digest::new([7; 32]),
    )?)
}
