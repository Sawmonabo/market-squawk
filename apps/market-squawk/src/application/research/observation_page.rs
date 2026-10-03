//! Demand reads over exact immutable physical row occurrences.

use super::*;
use arrow::{
    array::{Array, StringArray, TimestampNanosecondArray},
    ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

const PAGE_ROWS: usize = 100;
const BATCH_ROWS: usize = 128;
const BATCH_BYTES: usize = 64 * 1024 * 1024;
const CURSOR_TTL_NANOS: i64 = 3_600_000_000_000;
const SUMMARY_COLUMNS: &[&str] = &[
    "observation_kind",
    "instrument_id",
    "revision",
    "quality",
    "effective_at",
    "effective_date",
    "published_at",
    "published_date",
    "available_at",
    "superseded_at",
    "superseded_date",
];
const SUMMARY_OUTPUT_COLUMNS: &[&str] = &[
    "revision",
    "quality",
    "effective_at",
    "effective_date",
    "published_at",
    "published_date",
    "available_at",
    "superseded_at",
    "superseded_date",
];

/// Decimal strings preserve physical offsets and generation numbers across JavaScript.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PageCursor {
    version: String,
    content_hash: [u8; 32],
    query_digest: [u8; 32],
    object: String,
    row: String,
    expires_at: String,
}

impl ResearchController {
    pub(super) async fn observation_page(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
        template: AnalyticalObservationTemplate,
    ) -> Result<TypedToolResult, ServiceError> {
        live(context)?;
        let dataset = required_dataset(request)?;
        let projection = request
            .arguments()
            .get("projection")
            .filter(|value| !value.is_null())
            .map(|value| value.as_str().ok_or(ServiceError::InvalidRequest))
            .transpose()?
            .unwrap_or("complete");
        if !matches!(projection, "summary" | "complete") {
            return Err(ServiceError::InvalidRequest);
        }
        let summary = projection == "summary";

        let limit = request
            .arguments()
            .get("limit")
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok())
                    .filter(|value| (1..=PAGE_ROWS).contains(value))
                    .ok_or(ServiceError::InvalidRequest)
            })
            .transpose()?
            .unwrap_or(25)
            .min(limits.maximum_result_items());
        if limit == 0 {
            return Err(ServiceError::InvalidRequest);
        }
        let mut instruments = requested_instruments(request)?;
        instruments.sort_unstable();
        instruments.dedup();
        let range = requested_knowledge_range(request)?;
        let query_digest: [u8; 32] = Sha256::digest(serde_json::to_vec(&json!({
            "operation": request.name(), "dataset": dataset.as_str(), "projection":projection,
            "instruments": instruments,
            "range": range.map(|range| [range.start().unix_nanos().to_string(), range.end().unix_nanos().to_string()]),
            "order": "manifest_object_and_row_v1",
        })).map_err(|_| ServiceError::InvalidRequest)?).into();
        let now = clock()?;
        let incoming = request
            .arguments()
            .get("cursor")
            .filter(|value| !value.is_null())
            .map(|value| {
                let text = value
                    .as_str()
                    .filter(|value| value.len() <= 2048)
                    .ok_or(ServiceError::InvalidRequest)?;
                serde_json::from_str::<PageCursor>(text).map_err(|_| ServiceError::InvalidRequest)
            })
            .transpose()?;
        let (generation, object, row, expires_at) = if let Some(cursor) = incoming {
            if cursor.query_digest != query_digest {
                return Err(ServiceError::InvalidRequest);
            }
            let version = decimal_u64(&cursor.version)?;
            let expiry = decimal_i64(&cursor.expires_at)?;
            if expiry <= now {
                return Err(ServiceError::Unavailable);
            }
            if expiry
                > now
                    .checked_add(CURSOR_TTL_NANOS)
                    .ok_or(ServiceError::Internal)?
            {
                return Err(ServiceError::InvalidRequest);
            }
            let schema = market_squawk_data::DatasetSchemaRegistry::local()
                .canonical_research_observations()
                .map_err(|_| ServiceError::Unavailable)?;
            let manifest = DatasetManifestRef::try_new_with_schema(
                dataset.clone(),
                version,
                schema,
                market_squawk_data::Sha256Digest::new(cursor.content_hash),
            )
            .map_err(|_| ServiceError::InvalidRequest)?;
            let generation = self
                .reader
                .exact(&manifest, context.deadline(), context.cancellation())
                .map_err(map_read_error)?;
            let object = usize::try_from(decimal_u64(&cursor.object)?)
                .map_err(|_| ServiceError::InvalidRequest)?;
            (generation, object, decimal_u64(&cursor.row)?, expiry)
        } else {
            (
                self.reader
                    .latest(&dataset, context.deadline(), context.cancellation())
                    .map_err(map_read_error)?
                    .ok_or(ServiceError::NotFound)?,
                0,
                0,
                now.checked_add(CURSOR_TTL_NANOS)
                    .ok_or(ServiceError::Internal)?,
            )
        };
        let read = AnalyticalObservationReadRequest::try_new(
            generation.manifest().clone(),
            template,
            instruments,
            range,
        )
        .map_err(map_read_error)?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(limit)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        let mut next = None;
        let mut ipc = None;
        let mut json_bytes = 0_usize;
        // The envelope includes the manifest, provenance and the backend cursor. Reserve its
        // exact empty encoding before admitting row JSON, rather than an unrelated corpus cap.
        let reserve_cursor = cursor_for(
            &generation,
            query_digest,
            generation.object_count(),
            generation.row_count(),
            expires_at,
        )?;
        let empty = page_result(
            &generation,
            Vec::new(),
            Some(reserve_cursor),
            u64::MAX,
            limits,
        )?;
        let row_budget = limits
            .maximum_result_bytes()
            .checked_sub(empty.encoded_bytes())
            .ok_or(ServiceError::ResourceExhausted)?;
        if generation.row_count() != 0 {
            let mut cursor = self
                .reader
                .observation_batch_cursor(
                    generation.manifest(),
                    object,
                    row,
                    summary.then_some(SUMMARY_COLUMNS),
                    BATCH_ROWS,
                    BATCH_BYTES,
                    context.deadline(),
                    context.cancellation(),
                )
                .map_err(map_read_error)?;
            'read: loop {
                live(context)?;
                let before = cursor.position();
                let batch = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(context.deadline()),
                    cursor.next_batch(),
                )
                .await
                .map_err(|_| ServiceError::DeadlineExceeded)?
                .map_err(source_errors::map_parquet_error)?;
                let Some(batch) = batch else {
                    break;
                };
                for ordinal in 0..batch.num_rows() {
                    live(context)?;
                    if !matches_row(&batch, ordinal, &read)? {
                        continue;
                    }
                    let position = (
                        before.0,
                        before
                            .1
                            .checked_add(
                                u64::try_from(ordinal)
                                    .map_err(|_| ServiceError::ResourceExhausted)?,
                            )
                            .ok_or(ServiceError::ResourceExhausted)?,
                    );
                    if rows.len() == limit {
                        next = Some(cursor_for(
                            &generation,
                            query_digest,
                            position.0,
                            position.1,
                            expires_at,
                        )?);
                        break 'read;
                    }
                    let slice = batch.slice(ordinal, 1);
                    let slice = if summary {
                        let indices = SUMMARY_OUTPUT_COLUMNS
                            .iter()
                            .map(|name| {
                                slice
                                    .schema()
                                    .index_of(name)
                                    .map_err(|_| ServiceError::InvalidResult)
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        slice
                            .project(&indices)
                            .map_err(|_| ServiceError::InvalidResult)?
                    } else {
                        slice
                    };
                    let value = match arrow_rows(std::slice::from_ref(&slice), row_budget) {
                        Ok(value) => value,
                        Err(ServiceError::ResourceExhausted) if !rows.is_empty() => {
                            next = Some(cursor_for(
                                &generation,
                                query_digest,
                                position.0,
                                position.1,
                                expires_at,
                            )?);
                            break 'read;
                        }
                        Err(error) => return Err(error),
                    };
                    let [value] = value
                        .as_array()
                        .ok_or(ServiceError::InvalidResult)?
                        .as_slice()
                    else {
                        return Err(ServiceError::InvalidResult);
                    };
                    let value = if summary {
                        summary_row(value)?
                    } else {
                        value.clone()
                    };
                    let bytes = serde_json::to_vec(&value)
                        .map_err(|_| ServiceError::InvalidResult)?
                        .len()
                        .checked_add(1)
                        .ok_or(ServiceError::ResourceExhausted)?;
                    if json_bytes
                        .checked_add(bytes)
                        .is_none_or(|bytes| bytes > row_budget)
                    {
                        if rows.is_empty() {
                            return Err(ServiceError::ResourceExhausted);
                        }
                        next = Some(cursor_for(
                            &generation,
                            query_digest,
                            position.0,
                            position.1,
                            expires_at,
                        )?);
                        break 'read;
                    }
                    json_bytes += bytes;
                    if ipc.is_none() {
                        ipc = Some(
                            StreamWriter::try_new(CountBytes::default(), &slice.schema())
                                .map_err(|_| ServiceError::InvalidResult)?,
                        );
                    }
                    ipc.as_mut()
                        .ok_or(ServiceError::Internal)?
                        .write(&slice)
                        .map_err(|_| ServiceError::InvalidResult)?;
                    rows.push(value);
                }
            }
        }
        let ipc_bytes = if let Some(mut writer) = ipc {
            writer.finish().map_err(|_| ServiceError::InvalidResult)?;
            writer.get_ref().0
        } else {
            0
        };
        live(context)?;
        page_result(&generation, rows, next, ipc_bytes, limits)
    }
}

fn summary_row(value: &Value) -> Result<Value, ServiceError> {
    let source = value.as_object().ok_or(ServiceError::InvalidResult)?;
    let scalar = |names: &[&str]| {
        names
            .iter()
            .filter_map(|name| source.get(*name))
            .find(|value| !value.is_null())
            .cloned()
            .unwrap_or(Value::Null)
    };
    Ok(
        json!({"revision":scalar(&["revision"]), "quality":scalar(&["quality"]),
        "effectiveAt":scalar(&["effective_at","effective_date"]),
        "publishedAt":scalar(&["published_at","published_date"]),
        "availableAt":scalar(&["available_at"]), "supersededAt":scalar(&["superseded_at","superseded_date"])}),
    )
}

fn matches_row(
    batch: &RecordBatch,
    row: usize,
    request: &AnalyticalObservationReadRequest,
) -> Result<bool, ServiceError> {
    let strings = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or(ServiceError::InvalidResult)
    };
    if request.template() == AnalyticalObservationTemplate::AlternativeData
        && strings("observation_kind")?.value(row) != "alternative_data"
    {
        return Ok(false);
    }
    if !request.instrument_ids().is_empty() {
        let values = strings("instrument_id")?;
        if values.is_null(row) {
            return Ok(false);
        }
        let instrument =
            InstrumentId::from_str(values.value(row)).map_err(|_| ServiceError::InvalidResult)?;
        if request.instrument_ids().binary_search(&instrument).is_err() {
            return Ok(false);
        }
    }
    if let Some(range) = request.knowledge_range() {
        let values = batch
            .column_by_name("available_at")
            .and_then(|column| column.as_any().downcast_ref::<TimestampNanosecondArray>())
            .ok_or(ServiceError::InvalidResult)?;
        if values.is_null(row)
            || values.value(row) < range.start().unix_nanos()
            || values.value(row) > range.end().unix_nanos()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn cursor_for(
    generation: &AnalyticalGeneration,
    query_digest: [u8; 32],
    object: usize,
    row: u64,
    expiry: i64,
) -> Result<String, ServiceError> {
    serde_json::to_string(&PageCursor {
        version: generation.manifest().manifest_version().to_string(),
        content_hash: generation.manifest().content_hash().bytes(),
        query_digest,
        object: object.to_string(),
        row: row.to_string(),
        expires_at: expiry.to_string(),
    })
    .map_err(|_| ServiceError::Internal)
}

fn page_result(
    generation: &AnalyticalGeneration,
    rows: Vec<Value>,
    next: Option<String>,
    ipc_bytes: u64,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    let count = rows.len();
    let metadata = ToolResultMetadata::try_complete(json!({"sourceId": generation.source_id(),
        "manifest": manifest_value(generation.manifest()), "order":"manifest_object_and_row_v1"}),
        json!({"classification":"record_level_provenance", "qualityRetainedPerRow":true, "executionEligible":false}))
        .map_err(|_| ServiceError::InvalidResult)?;
    TypedToolResult::try_new(
        json!({"manifest":manifest_value(generation.manifest()), "arrowIpcBytes":ipc_bytes,
        "rows":rows, "hasMore":next.is_some(), "nextCursor":next}),
        count,
        metadata,
        limits,
    )
    .map_err(Into::into)
}

fn decimal_u64(value: &str) -> Result<u64, ServiceError> {
    value
        .parse::<u64>()
        .ok()
        .filter(|number| number.to_string() == value)
        .ok_or(ServiceError::InvalidRequest)
}
fn decimal_i64(value: &str) -> Result<i64, ServiceError> {
    value
        .parse::<i64>()
        .ok()
        .filter(|number| number.to_string() == value && *number > 0)
        .ok_or(ServiceError::InvalidRequest)
}
fn clock() -> Result<i64, ServiceError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|time| i64::try_from(time.as_nanos()).ok())
        .ok_or(ServiceError::Internal)
}
fn live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
#[derive(Default)]
struct CountBytes(u64);
impl Write for CountBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(
                u64::try_from(bytes.len()).map_err(|_| io::Error::other("IPC size overflow"))?,
            )
            .ok_or_else(|| io::Error::other("IPC size overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
