//! Immutable-generation selection and bounded read-only DataFusion execution.

use std::io;
use std::time::{Duration, Instant};

use arrow::json::ArrayWriter;
use market_squawk_data::{
    DatasetId, QueryError, QueryLimits, QueryRequest, QueryResult, ResearchQueryEngine,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{CLI_DEFAULT_MAXIMUM_BYTES, CliProductError, CliProductResult, LocalProduct, hex};

pub(super) async fn query_sql(
    product: &LocalProduct,
    dataset: &str,
    statement: String,
    maximum_rows: usize,
) -> Result<CliProductResult, CliProductError> {
    let cancellation = CancellationToken::new();
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(60))
        .ok_or(CliProductError::Limits)?;
    let dataset_id = DatasetId::try_from(dataset).map_err(|_| CliProductError::RequestShape)?;
    let research = product.research();
    let reader = research.analytical_reader();
    let generation = reader.latest(&dataset_id, deadline, &cancellation)?.ok_or(
        CliProductError::Application(market_squawk_services::ServiceError::NotFound),
    )?;
    let manifest = generation.manifest().clone();
    let pinned = research
        .analytical()
        .pinned(&manifest)
        .map_err(|_| CliProductError::RequestShape)?;
    let engine = ResearchQueryEngine::from_pinned_dataset(
        pinned,
        "dataset",
        research.analytical().object_store(),
        cancellation.clone(),
    )
    .await?;
    let rows = u64::try_from(maximum_rows).map_err(|_| CliProductError::Limits)?;
    let limits = QueryLimits::try_new(
        rows,
        256 * 1024,
        64 * 1024 * 1024,
        4,
        2_048,
        4_096,
        Duration::from_secs(60),
    )?;
    let request = QueryRequest::try_new(manifest.clone(), statement)?;
    let QueryResult::Inline {
        batches,
        byte_count,
    } = engine.query(request, limits, cancellation).await?
    else {
        return Err(CliProductError::Query(
            QueryError::ArtifactAuthorityRequired,
        ));
    };
    let returned_rows = batches.iter().try_fold(0_usize, |total, batch| {
        total
            .checked_add(batch.num_rows())
            .ok_or(CliProductError::Limits)
    })?;
    let mut writer = ArrayWriter::new(BoundedJsonWriter::new(CLI_DEFAULT_MAXIMUM_BYTES));
    let references = batches.iter().collect::<Vec<_>>();
    writer
        .write_batches(&references)
        .and_then(|()| writer.finish())
        .map_err(|_| CliProductError::Limits)?;
    let rows: Value = serde_json::from_slice(writer.into_inner().as_slice())
        .map_err(|_| CliProductError::RequestShape)?;
    Ok(CliProductResult {
        summary: "bounded SQL query completed",
        value: json!({
            "data": {
                "relation": "dataset",
                "manifest": {
                    "dataset": manifest.dataset_id().as_str(),
                    "version": manifest.manifest_version(),
                    "schema": manifest.schema().name(),
                    "schemaVersion": manifest.schema_version().get(),
                    "contentSha256": hex(&manifest.content_hash().bytes()),
                },
                "arrowIpcBytes": byte_count,
                "rows": rows,
            },
            "metadata": {
                "completeness": "complete",
                "returnedItems": returned_rows,
                "availableItems": returned_rows,
                "sourceCoverage": {
                    "sourceId": generation.source_id().as_str(),
                    "manifestPinned": true,
                },
                "dataQuality": {
                    "classification": "record_level_provenance",
                    "executionEligible": false,
                },
            },
        }),
    })
}

struct BoundedJsonWriter {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedJsonWriter {
    const fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

impl io::Write for BoundedJsonWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let required = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .filter(|required| *required <= self.maximum)
            .ok_or_else(|| io::Error::other("CLI JSON result exceeded its byte ceiling"))?;
        self.bytes
            .try_reserve(required.saturating_sub(self.bytes.len()))
            .map_err(|_| io::Error::other("CLI JSON result allocation failed"))?;
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
