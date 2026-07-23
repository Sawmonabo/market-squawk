//! Manifest-pinned research, fundamental, and macro application services.

use std::{
    fmt,
    io::{self, Write},
    num::NonZeroU16,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::json::ArrayWriter;
use async_trait::async_trait;
use chrono::DateTime;
use market_squawk_data::{
    AnalyticalGeneration, AnalyticalObservationOutput, AnalyticalObservationReadRequest,
    AnalyticalObservationTemplate, AnalyticalReadCapability, AnalyticalReadError,
    AnalyticalReadLimit, DatasetId, DatasetManifestRef, GenerationKind, GenerationParentRelation,
    ManifestCatalogError, QueryError, QueryLimits, QueryResult,
};
use market_squawk_domain::{InstrumentId, SourceIdentifier, Timestamp};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, ServiceLimits, ToolResultMetadata,
    TypedToolRequest, TypedToolResult,
};
use serde_json::{Value, json};

use super::{ApplicationDomainService, domain_support::DomainLifecycle, effective_service_limits};
use crate::ResearchService;

mod ingest;

pub use ingest::{
    ManagedResearchExtractionSource, ProductionResearchIngestCoordinator, ResearchExtractionLimits,
    ResearchIngestCompositionError, ResearchRevisionPlanError, ResearchRightsAuthority,
    ResearchSourceDiscovery, ResearchSourceDiscoveryObject, ResearchSourceDiscoveryRights,
};

const RESEARCH_LIST_DATASETS: &str = "Research.ListDatasets";
const RESEARCH_GET_MANIFEST: &str = "Research.GetManifest";
const RESEARCH_GET_HISTORY: &str = "Research.GetHistory";
const RESEARCH_GET_ALTERNATIVE_DATA: &str = "Research.GetAlternativeData";
const RESEARCH_INGEST_SOURCE: &str = "Research.IngestSource";

const FUNDAMENTAL_GET_FILINGS: &str = "Fundamental.GetFilings";
const FUNDAMENTAL_GET_FACTS: &str = "Fundamental.GetFacts";
const FUNDAMENTAL_GET_STATEMENTS: &str = "Fundamental.GetStatements";
const FUNDAMENTAL_GET_RATIOS: &str = "Fundamental.GetRatios";

const MACRO_LIST_SERIES: &str = "Macro.ListSeries";
const MACRO_GET_OBSERVATIONS: &str = "Macro.GetObservations";
const MACRO_GET_VINTAGES: &str = "Macro.GetVintages";
const MACRO_GET_REVISIONS: &str = "Macro.GetRevisions";

const MAX_ANALYTICAL_PAGE: usize = 64;
const MAX_INLINE_QUERY_BYTES: usize = 256 * 1024;
const RESULT_ENVELOPE_RESERVE_BYTES: usize = 4 * 1024;

/// Provider extraction and rights-admitted ingestion used by the Research domain.
///
/// Implementations own the concrete extraction adapters and must be cancellation-aware. Shutdown
/// operations are idempotent because Source and Research may share the same coordinator.
#[async_trait]
pub trait ResearchIngestCoordinator: Send + Sync + 'static {
    /// Executes one descriptor-admitted provider extraction and durable ingest.
    async fn ingest(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError>;

    /// Rejects new extraction work and cancels owned background activity without blocking.
    fn begin_shutdown(&self);

    /// Completes bounded adapter and persistence shutdown.
    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError>;
}

/// Read-only discovery authority shared with the Source domain.
///
/// This narrow trait exposes registered adapter discovery without granting source registration,
/// raw adapter access, credential access, extraction, or analytical publication authority.
#[async_trait]
pub trait ResearchSourceDiscoveryCoordinator: Send + Sync + 'static {
    /// Discovers bounded exact objects for one active registered provider profile.
    async fn discover_registered_objects(
        &self,
        profile: &SourceIdentifier,
        dataset: &SourceIdentifier,
        effective_at: Option<Timestamp>,
        max_results: NonZeroU16,
        context: &RequestContext,
    ) -> Result<ResearchSourceDiscovery, ServiceError>;
}

/// Shared analytical authority exposed as separate Research, Fundamental, and Macro services.
pub struct ResearchApplicationServices {
    controller: Arc<ResearchController>,
}

impl ResearchApplicationServices {
    /// Binds one application-owned analytical service and one concrete extraction coordinator.
    #[must_use]
    pub fn new(service: Arc<ResearchService>, ingest: Arc<dyn ResearchIngestCoordinator>) -> Self {
        let reader = service.analytical_reader();
        Self {
            controller: Arc::new(ResearchController {
                _authority: service,
                reader,
                ingest,
                lifecycle: DomainLifecycle::new(),
            }),
        }
    }

    /// Returns the Research-domain implementation.
    pub fn research(&self) -> Arc<dyn ApplicationDomainService> {
        Arc::new(ResearchDomainService {
            controller: Arc::clone(&self.controller),
        })
    }

    /// Returns the Fundamental-domain implementation.
    pub fn fundamental(&self) -> Arc<dyn ApplicationDomainService> {
        Arc::new(FundamentalDomainService {
            controller: Arc::clone(&self.controller),
        })
    }

    /// Returns the Macro-domain implementation.
    pub fn macroeconomics(&self) -> Arc<dyn ApplicationDomainService> {
        Arc::new(MacroDomainService {
            controller: Arc::clone(&self.controller),
        })
    }
}

impl fmt::Debug for ResearchApplicationServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResearchApplicationServices")
            .field("controller", &self.controller)
            .finish()
    }
}

struct ResearchDomainService {
    controller: Arc<ResearchController>,
}

struct FundamentalDomainService {
    controller: Arc<ResearchController>,
}

struct MacroDomainService {
    controller: Arc<ResearchController>,
}

#[async_trait]
impl ApplicationDomainService for ResearchDomainService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Research
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let _call = DomainLifecycle::enter(&self.controller.lifecycle, &context)?;
        let limits = effective_service_limits(&request, &context)?;
        match request.name() {
            RESEARCH_LIST_DATASETS => self.controller.datasets(&request, &context, limits),
            RESEARCH_GET_MANIFEST => self.controller.manifest(&request, &context, limits),
            RESEARCH_GET_HISTORY => {
                self.controller
                    .observations(
                        &request,
                        &context,
                        limits,
                        AnalyticalObservationTemplate::All,
                    )
                    .await
            }
            RESEARCH_GET_ALTERNATIVE_DATA => {
                self.controller
                    .observations(
                        &request,
                        &context,
                        limits,
                        AnalyticalObservationTemplate::AlternativeData,
                    )
                    .await
            }
            RESEARCH_INGEST_SOURCE => {
                self.controller
                    .ingest
                    .ingest(&request, &context, limits)
                    .await
            }
            _ => Err(ServiceError::NotFound),
        }
    }

    fn begin_shutdown(&self) {
        self.controller.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.controller.finish_shutdown(deadline).await
    }
}

#[async_trait]
impl ApplicationDomainService for FundamentalDomainService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Fundamental
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let _call = DomainLifecycle::enter(&self.controller.lifecycle, &context)?;
        match request.name() {
            FUNDAMENTAL_GET_FILINGS
            | FUNDAMENTAL_GET_FACTS
            | FUNDAMENTAL_GET_STATEMENTS
            | FUNDAMENTAL_GET_RATIOS => {
                let limits = effective_service_limits(&request, &context)?;
                self.controller
                    .observations(
                        &request,
                        &context,
                        limits,
                        AnalyticalObservationTemplate::Fundamental,
                    )
                    .await
            }
            _ => Err(ServiceError::NotFound),
        }
    }

    fn begin_shutdown(&self) {
        self.controller.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.controller.finish_shutdown(deadline).await
    }
}

#[async_trait]
impl ApplicationDomainService for MacroDomainService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Macro
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let _call = DomainLifecycle::enter(&self.controller.lifecycle, &context)?;
        match request.name() {
            MACRO_LIST_SERIES
            | MACRO_GET_OBSERVATIONS
            | MACRO_GET_VINTAGES
            | MACRO_GET_REVISIONS => {
                let limits = effective_service_limits(&request, &context)?;
                self.controller
                    .observations(
                        &request,
                        &context,
                        limits,
                        AnalyticalObservationTemplate::Macro,
                    )
                    .await
            }
            _ => Err(ServiceError::NotFound),
        }
    }

    fn begin_shutdown(&self) {
        self.controller.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.controller.finish_shutdown(deadline).await
    }
}

struct ResearchController {
    _authority: Arc<ResearchService>,
    reader: AnalyticalReadCapability,
    ingest: Arc<dyn ResearchIngestCoordinator>,
    lifecycle: Arc<DomainLifecycle>,
}

impl ResearchController {
    fn datasets(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        let after = optional_dataset(request, "afterDataset")?;
        let page_limit = limits.maximum_result_items().min(MAX_ANALYTICAL_PAGE);
        let page = self
            .reader
            .datasets(
                after.as_ref(),
                AnalyticalReadLimit::try_new(page_limit).map_err(map_read_error)?,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(map_read_error)?;
        let returned = page.generations().len();
        if returned == 0 {
            return TypedToolResult::try_new(
                Value::Null,
                0,
                ToolResultMetadata::complete_not_applicable(),
                limits,
            )
            .map_err(Into::into);
        }
        let next_after = page
            .generations()
            .last()
            .filter(|_| page.has_more())
            .map(|generation| generation.manifest().dataset_id().as_str());
        let content = json!({
            "items": page
                .generations()
                .iter()
                .map(generation_value)
                .collect::<Vec<_>>(),
            "hasMore": page.has_more(),
            "nextAfterDataset": next_after,
        });
        TypedToolResult::try_new(
            content,
            returned,
            ToolResultMetadata::complete_not_applicable(),
            limits,
        )
        .map_err(Into::into)
    }

    fn manifest(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        let dataset = required_dataset(request)?;
        let generation = self
            .reader
            .latest(&dataset, context.deadline(), context.cancellation())
            .map_err(map_read_error)?
            .ok_or(ServiceError::NotFound)?;
        TypedToolResult::try_new(
            generation_value(&generation),
            1,
            ToolResultMetadata::complete_not_applicable(),
            limits,
        )
        .map_err(Into::into)
    }

    async fn observations(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
        template: AnalyticalObservationTemplate,
    ) -> Result<TypedToolResult, ServiceError> {
        let dataset = required_dataset(request)?;
        let generation = self
            .reader
            .latest(&dataset, context.deadline(), context.cancellation())
            .map_err(map_read_error)?
            .ok_or(ServiceError::NotFound)?;
        let instruments = requested_instruments(request)?;
        let knowledge_range = requested_knowledge_range(request)?;
        let read = AnalyticalObservationReadRequest::try_new(
            generation.manifest().clone(),
            template,
            instruments,
            knowledge_range,
        )
        .map_err(map_read_error)?;
        let query_limits = query_limits(limits, context)?;
        let output = self
            .reader
            .read_observations(
                read,
                query_limits,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_read_error)?;
        observation_result(output, limits)
    }

    fn begin_shutdown(&self) {
        self.lifecycle.begin_shutdown();
        self.ingest.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        let drained = self.lifecycle.finish_shutdown(deadline).await;
        let ingest = self.ingest.finish_shutdown(deadline).await;
        drained.and(ingest)
    }
}

impl fmt::Debug for ResearchController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResearchController")
            .field("authority", &"[ANALYTICAL AUTHORITY]")
            .field("reader", &self.reader)
            .field("ingest", &"[EXTRACTION COORDINATOR]")
            .field("lifecycle", &self.lifecycle)
            .finish()
    }
}

fn observation_result(
    output: AnalyticalObservationOutput,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    let pinned = output.output();
    let manifest = pinned.manifest();
    let coverage = json!({
        "sourceId": output.source_id(),
        "manifest": manifest_value(manifest),
        "objectGraphDigest": encode_hex(pinned.object_graph_digest().bytes()),
        "queryIdentity": encode_hex(pinned.query_identity().bytes()),
        "resultDigest": encode_hex(pinned.result_digest().bytes()),
    });
    let quality = json!({
        "classification": "record_level_provenance",
        "qualityRetainedPerRow": true,
        "executionEligible": false,
    });
    let metadata = ToolResultMetadata::try_complete(coverage, quality)
        .map_err(|_error| ServiceError::InvalidResult)?;
    let maximum_json_bytes = limits
        .maximum_result_bytes()
        .saturating_sub(RESULT_ENVELOPE_RESERVE_BYTES);
    match pinned.result() {
        QueryResult::Inline {
            batches,
            byte_count,
        } => {
            let returned = batches.iter().try_fold(0_usize, |total, batch| {
                total
                    .checked_add(batch.num_rows())
                    .ok_or(ServiceError::ResourceExhausted)
            })?;
            if returned == 0 {
                return TypedToolResult::try_new(Value::Null, 0, metadata, limits)
                    .map_err(Into::into);
            }
            let rows = arrow_rows(batches, maximum_json_bytes)?;
            let content = json!({
                "manifest": manifest_value(manifest),
                "arrowIpcBytes": byte_count,
                "rows": rows,
            });
            TypedToolResult::try_new(content, returned, metadata, limits).map_err(Into::into)
        }
        QueryResult::Artifact {
            object,
            artifact,
            ownership,
        } => {
            let returned = usize::try_from(object.row_count())
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            let content = json!({
                "manifest": manifest_value(manifest),
                "artifact": {
                    "artifactId": artifact.artifact_id(),
                    "contentDigest": encode_hex(artifact.content_digest().bytes()),
                    "sizeBytes": artifact.size_bytes(),
                    "rowCount": object.row_count(),
                    "owner": ownership.owner(),
                    "expiresAt": ownership.expires_at(),
                },
            });
            TypedToolResult::try_new(content, returned.max(1), metadata, limits).map_err(Into::into)
        }
    }
}

fn arrow_rows(
    batches: &[arrow::record_batch::RecordBatch],
    maximum_bytes: usize,
) -> Result<Value, ServiceError> {
    if maximum_bytes == 0 {
        return Err(ServiceError::ResourceExhausted);
    }
    let buffer = BoundedJsonBuffer::new(maximum_bytes);
    let mut writer = ArrayWriter::new(buffer);
    let references = batches.iter().collect::<Vec<_>>();
    writer
        .write_batches(&references)
        .and_then(|()| writer.finish())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    let bytes = writer.into_inner().bytes;
    serde_json::from_slice(&bytes).map_err(|_error| ServiceError::InvalidResult)
}

struct BoundedJsonBuffer {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedJsonBuffer {
    const fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }
}

impl Write for BoundedJsonBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let required = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .filter(|required| *required <= self.maximum)
            .ok_or_else(|| io::Error::other("bounded analytical JSON result exceeded"))?;
        self.bytes
            .try_reserve(required.saturating_sub(self.bytes.len()))
            .map_err(|_error| io::Error::other("bounded analytical JSON allocation failed"))?;
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn query_limits(
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<QueryLimits, ServiceError> {
    let now = Instant::now();
    if now >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    let deadline = context
        .deadline()
        .saturating_duration_since(now)
        .min(Duration::from_secs(60));
    let rows = u64::try_from(limits.maximum_result_items())
        .map_err(|_error| ServiceError::InvalidRequest)?;
    let bytes = limits.maximum_inline_bytes().min(MAX_INLINE_QUERY_BYTES);
    let bytes = u64::try_from(bytes).map_err(|_error| ServiceError::InvalidRequest)?;
    let memory = bytes
        .saturating_mul(4)
        .clamp(1024 * 1024, 1024 * 1024 * 1024);
    QueryLimits::try_new(rows, bytes, memory, 4, 2_048, 4_096, deadline).map_err(map_query_error)
}

fn required_dataset(request: &TypedToolRequest) -> Result<DatasetId, ServiceError> {
    optional_dataset(request, "dataset")?.ok_or(ServiceError::InvalidRequest)
}

fn optional_dataset(
    request: &TypedToolRequest,
    field: &str,
) -> Result<Option<DatasetId>, ServiceError> {
    request
        .arguments()
        .get(field)
        .map(|value| {
            value
                .as_str()
                .ok_or(ServiceError::InvalidRequest)
                .and_then(|value| {
                    DatasetId::try_from(value).map_err(|_error| ServiceError::InvalidRequest)
                })
        })
        .transpose()
}

fn requested_instruments(request: &TypedToolRequest) -> Result<Vec<InstrumentId>, ServiceError> {
    request
        .arguments()
        .get("instrumentIds")
        .map(|value| {
            value
                .as_array()
                .ok_or(ServiceError::InvalidRequest)?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .ok_or(ServiceError::InvalidRequest)
                        .and_then(|value| {
                            InstrumentId::from_str(value)
                                .map_err(|_error| ServiceError::InvalidRequest)
                        })
                })
                .collect()
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn requested_knowledge_range(
    request: &TypedToolRequest,
) -> Result<Option<market_squawk_data::ObservationKnowledgeRange>, ServiceError> {
    let Some(range) = request.arguments().get("timeRange") else {
        return Ok(None);
    };
    let range = range.as_object().ok_or(ServiceError::InvalidRequest)?;
    let start = range
        .get("start")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
        .and_then(parse_timestamp)?;
    let end = range
        .get("end")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
        .and_then(parse_timestamp)?;
    market_squawk_data::ObservationKnowledgeRange::try_new(start, end)
        .map(Some)
        .map_err(map_read_error)
}

fn parse_timestamp(value: &str) -> Result<Timestamp, ServiceError> {
    let parsed =
        DateTime::parse_from_rfc3339(value).map_err(|_error| ServiceError::InvalidRequest)?;
    parsed
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::InvalidRequest)
}

fn generation_value(generation: &AnalyticalGeneration) -> Value {
    json!({
        "manifest": manifest_value(generation.manifest()),
        "sourceId": generation.source_id(),
        "generationKind": generation_kind(generation.generation_kind()),
        "buildSpecDigest": generation
            .build_spec_digest()
            .map(|digest| encode_hex(digest.digest().bytes())),
        "parents": generation
            .parents()
            .iter()
            .map(|parent| json!({
                "relation": parent_relation(parent.relation()),
                "manifest": manifest_value(parent.manifest()),
            }))
            .collect::<Vec<_>>(),
        "rowCount": generation.row_count(),
        "totalBytes": generation.total_bytes(),
        "lineageDigest": encode_hex(generation.lineage_digest().bytes()),
        "objectCount": generation.object_count(),
    })
}

fn manifest_value(manifest: &DatasetManifestRef) -> Value {
    json!({
        "datasetId": manifest.dataset_id().as_str(),
        "manifestVersion": manifest.manifest_version(),
        "schema": {
            "name": manifest.schema().name(),
            "version": manifest.schema_version().get(),
            "fingerprint": encode_hex(manifest.schema().fingerprint()),
        },
        "contentHash": encode_hex(manifest.content_hash().bytes()),
    })
}

const fn generation_kind(kind: GenerationKind) -> &'static str {
    match kind {
        GenerationKind::Ingest => "ingest",
        GenerationKind::Compaction => "compaction",
        GenerationKind::Derived => "derived",
    }
}

const fn parent_relation(relation: GenerationParentRelation) -> &'static str {
    match relation {
        GenerationParentRelation::AppendPredecessor => "append_predecessor",
        GenerationParentRelation::CompactionPredecessor => "compaction_predecessor",
        GenerationParentRelation::DerivedInput => "derived_input",
    }
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn map_read_error(error: AnalyticalReadError) -> ServiceError {
    match error {
        AnalyticalReadError::InvalidLimit
        | AnalyticalReadError::InstrumentLimitExceeded
        | AnalyticalReadError::InvalidKnowledgeRange
        | AnalyticalReadError::InvalidObservationSchema => ServiceError::InvalidRequest,
        AnalyticalReadError::Manifest(error) => map_manifest_error(error),
        AnalyticalReadError::Query(error) => map_query_error(error),
    }
}

fn map_manifest_error(error: ManifestCatalogError) -> ServiceError {
    match error {
        ManifestCatalogError::Cancelled => ServiceError::Cancelled,
        ManifestCatalogError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ManifestCatalogError::ObjectLimitExceeded { .. }
        | ManifestCatalogError::ReferenceWorkLimitExceeded { .. }
        | ManifestCatalogError::CountOverflow
        | ManifestCatalogError::AllocationContract => ServiceError::ResourceExhausted,
        ManifestCatalogError::InvalidConfiguration
        | ManifestCatalogError::MigrationMissing
        | ManifestCatalogError::AnchorMismatch
        | ManifestCatalogError::GenerationConflict
        | ManifestCatalogError::SourceMismatch
        | ManifestCatalogError::SchemaMismatch
        | ManifestCatalogError::SchemaIdentity(_)
        | ManifestCatalogError::CorruptCatalog
        | ManifestCatalogError::LockPoisoned
        | ManifestCatalogError::Plan(_)
        | ManifestCatalogError::Path(_)
        | ManifestCatalogError::Sqlite(_)
        | ManifestCatalogError::CatalogAuthority(_) => ServiceError::Unavailable,
    }
}

fn map_query_error(error: QueryError) -> ServiceError {
    match error {
        QueryError::Cancelled => ServiceError::Cancelled,
        QueryError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        QueryError::InvalidLimits
        | QueryError::RowLimitExceeded { .. }
        | QueryError::ByteLimitExceeded { .. }
        | QueryError::MemoryLimitExceeded { .. }
        | QueryError::SizeOverflow
        | QueryError::DependencyAllocationContract
        | QueryError::BlockingTaskLimitExceeded
        | QueryError::ReaderMemoryBoundExceeded
        | QueryError::ArtifactStoreRequired
        | QueryError::ArtifactAuthorityRequired => ServiceError::ResourceExhausted,
        QueryError::InvalidSql
        | QueryError::Parse(_)
        | QueryError::ForbiddenStatement
        | QueryError::ForbiddenTableFunction
        | QueryError::ForbiddenFunction
        | QueryError::ForbiddenRelation
        | QueryError::InvalidSource
        | QueryError::ManifestPinMismatch
        | QueryError::PinnedQuerySourceRequired
        | QueryError::MonetaryValueRequiresInlineResult
        | QueryError::MonetaryCellOutOfBounds
        | QueryError::InvalidMonetaryCell
        | QueryError::UnsupportedMonetaryScale
        | QueryError::AstLimitExceeded
        | QueryError::PlanLimitExceeded
        | QueryError::PartitionLimitExceeded
        | QueryError::UnsupportedSourceSchema
        | QueryError::ArtifactReservationMismatch
        | QueryError::ArtifactRootMismatch
        | QueryError::DataFusion(_)
        | QueryError::Arrow(_)
        | QueryError::Artifact(_)
        | QueryError::ArrowConversion(_)
        | QueryError::Catalog(_)
        | QueryError::Io(_)
        | QueryError::ObjectStore(_) => ServiceError::Unavailable,
    }
}
