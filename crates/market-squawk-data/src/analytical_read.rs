//! Least-authority immutable analytical catalog and fixed-template observation reads.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use market_squawk_domain::{InstrumentId, SourceId, Timestamp};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::manifest::{
    CatalogFeatureDataset, CatalogFeatureDatasetPage, CatalogFeatureDatasetSelection,
    CatalogGenerationPage,
};
use crate::{
    AnalyticalManifestCatalog, DatasetBuildSpecDigest, DatasetId, DatasetManifestRef,
    DatasetSchemaRegistry, DatasetSplitCounts, GenerationKind, GenerationParent,
    ManifestCatalogError, ParquetObjectStore, PinnedDataset, PinnedFeatureMonetaryValue,
    PinnedMonetaryValue, PinnedQueryOutput, QueryError, QueryLimits, QueryRequest,
    ResearchQueryEngine, Sha256Digest, UniverseId,
};

const MAX_READ_ITEMS: usize = 64;
const MAX_FILTER_INSTRUMENTS: usize = 256;
const OBSERVATION_TABLE: &str = "observations";

/// Nonzero caller-selected page size under the analytical service ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticalReadLimit(NonZeroUsize);

impl AnalyticalReadLimit {
    /// Constructs a page size no greater than 64 fully validated generation records.
    pub fn try_new(value: usize) -> Result<Self, AnalyticalReadError> {
        NonZeroUsize::new(value)
            .filter(|limit| limit.get() <= MAX_READ_ITEMS)
            .map(Self)
            .ok_or(AnalyticalReadError::InvalidLimit)
    }

    const fn get(self) -> usize {
        self.0.get()
    }
}

/// Immutable generation metadata without physical object paths or mutation authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticalGeneration {
    manifest: DatasetManifestRef,
    source_id: SourceId,
    generation_kind: GenerationKind,
    build_spec_digest: Option<DatasetBuildSpecDigest>,
    parents: Box<[GenerationParent]>,
    row_count: u64,
    total_bytes: u64,
    lineage_digest: Sha256Digest,
    object_count: usize,
    python_export_sha256: Option<Sha256Digest>,
}

impl AnalyticalGeneration {
    fn from_pinned(
        pinned: PinnedDataset,
        source_id: SourceId,
        python_export_sha256: Option<Sha256Digest>,
    ) -> Self {
        Self {
            manifest: pinned.manifest().clone(),
            source_id,
            generation_kind: pinned.generation_kind(),
            build_spec_digest: pinned.build_spec_digest(),
            parents: pinned.parents().to_vec().into_boxed_slice(),
            row_count: pinned.plan().row_count(),
            total_bytes: pinned.plan().total_bytes(),
            lineage_digest: pinned.plan().lineage_digest(),
            object_count: pinned.objects().len(),
            python_export_sha256,
        }
    }

    /// Returns the complete immutable generation identity.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the retained source-rights namespace that owns this generation.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns how the generation was produced.
    pub const fn generation_kind(&self) -> GenerationKind {
        self.generation_kind
    }

    /// Returns the exact derived-build identity when this is a derived generation.
    pub const fn build_spec_digest(&self) -> Option<DatasetBuildSpecDigest> {
        self.build_spec_digest
    }

    /// Returns exact immutable parent edges in durable ordinal order.
    pub fn parents(&self) -> &[GenerationParent] {
        &self.parents
    }

    /// Returns the semantic row count retained by the generation.
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns the sum of immutable Parquet object bytes.
    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Returns the canonical semantic lineage identity.
    pub const fn lineage_digest(&self) -> Sha256Digest {
        self.lineage_digest
    }

    /// Returns the number of immutable objects in the exact generation.
    pub const fn object_count(&self) -> usize {
        self.object_count
    }

    /// Returns the exact admitted canonical Python descriptor digest for a feature dataset.
    pub const fn python_export_sha256(&self) -> Option<Sha256Digest> {
        self.python_export_sha256
    }
}

/// One stable bounded page of immutable generations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticalGenerationPage {
    generations: Box<[AnalyticalGeneration]>,
    has_more: bool,
}

impl AnalyticalGenerationPage {
    fn from_catalog(page: CatalogGenerationPage) -> Self {
        let generations = page
            .generations
            .into_iter()
            .map(|(pinned, source_id, export)| {
                AnalyticalGeneration::from_pinned(pinned, source_id, export)
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            generations,
            has_more: page.has_more,
        }
    }

    /// Returns generations in the operation's documented stable order.
    pub fn generations(&self) -> &[AnalyticalGeneration] {
        &self.generations
    }

    /// Returns whether another cursor-bounded page exists.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

/// One durable Python-admitted feature/label generation in the public analytical registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticalFeatureDataset {
    generation: AnalyticalGeneration,
    python_export_sha256: Sha256Digest,
    policy_digest: Sha256Digest,
    universe_digest: Sha256Digest,
    universe_id: UniverseId,
    split_counts: DatasetSplitCounts,
    source_ids: Box<[SourceId]>,
}

impl AnalyticalFeatureDataset {
    fn from_catalog(dataset: CatalogFeatureDataset) -> Result<Self, AnalyticalReadError> {
        let summary = crate::python_dataset::feature_dataset_summary(
            &dataset.descriptor,
            dataset.export_sha256,
        )
        .map_err(|_| AnalyticalReadError::Manifest(ManifestCatalogError::CorruptCatalog))?;
        let generation = AnalyticalGeneration::from_pinned(
            dataset.pinned,
            dataset.source_id,
            Some(dataset.export_sha256),
        );
        if summary.identity.manifest() != generation.manifest()
            || summary.identity.build_spec_digest()
                != generation
                    .build_spec_digest()
                    .ok_or(ManifestCatalogError::CorruptCatalog)?
        {
            return Err(ManifestCatalogError::CorruptCatalog.into());
        }
        Ok(Self {
            generation,
            python_export_sha256: dataset.export_sha256,
            policy_digest: summary.identity.policy_digest(),
            universe_digest: summary.identity.universe_digest(),
            universe_id: summary.identity.universe_id().clone(),
            split_counts: summary.split_counts,
            source_ids: dataset.source_ids,
        })
    }

    /// Returns the immutable generation and retained source owner.
    pub const fn generation(&self) -> &AnalyticalGeneration {
        &self.generation
    }

    /// Returns the exact canonical descriptor digest admitted for native Python verification.
    pub const fn python_export_sha256(&self) -> Sha256Digest {
        self.python_export_sha256
    }

    /// Returns the exact point-in-time and transformation-policy identity.
    pub const fn policy_digest(&self) -> Sha256Digest {
        self.policy_digest
    }

    /// Returns the exact historical-universe contract identity.
    pub const fn universe_digest(&self) -> Sha256Digest {
        self.universe_digest
    }

    /// Returns the human-stable historical-universe identity.
    pub const fn universe_id(&self) -> &UniverseId {
        &self.universe_id
    }

    /// Returns admitted example counts by chronological split.
    pub const fn split_counts(&self) -> DatasetSplitCounts {
        self.split_counts
    }

    /// Returns the canonical source-rights owners of all exact input generations.
    pub fn source_ids(&self) -> &[SourceId] {
        &self.source_ids
    }
}

/// Closed exact-or-page selector for one coherent feature-dataset catalog snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalyticalFeatureDatasetSelection<'a> {
    /// Resolve only the latest durable generation for one exact identity.
    Exact(&'a DatasetId),
    /// Resolve the durable identity suffix strictly after an optional cursor.
    Page { after: Option<&'a DatasetId> },
}

/// One stable bounded page of durable Python-admitted feature datasets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticalFeatureDatasetPage {
    datasets: Box<[AnalyticalFeatureDataset]>,
    has_more: bool,
    available: usize,
    overlapping_legacy_dataset_ids: Box<[DatasetId]>,
}

impl AnalyticalFeatureDatasetPage {
    fn from_catalog(page: CatalogFeatureDatasetPage) -> Result<Self, AnalyticalReadError> {
        let datasets = page
            .datasets
            .into_iter()
            .map(AnalyticalFeatureDataset::from_catalog)
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();
        Ok(Self {
            datasets,
            has_more: page.has_more,
            available: page.available,
            overlapping_legacy_dataset_ids: page.overlapping_legacy_dataset_ids.into_boxed_slice(),
        })
    }

    /// Returns feature datasets in stable dataset-id order.
    pub fn datasets(&self) -> &[AnalyticalFeatureDataset] {
        &self.datasets
    }

    /// Returns whether another cursor-bounded page exists.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the exact number of admitted durable identities in the selected cursor suffix.
    pub const fn available(&self) -> usize {
        self.available
    }

    /// Returns bounded legacy identities also present in the same durable catalog snapshot.
    pub fn overlapping_legacy_dataset_ids(&self) -> &[DatasetId] {
        &self.overlapping_legacy_dataset_ids
    }
}

/// Closed canonical observation family selectable through the read capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalyticalObservationTemplate {
    /// Every canonical research observation, including all retained revisions.
    All,
    /// Reported fundamental and XBRL fact observations.
    Fundamental,
    /// Macroeconomic series observations and revisions.
    Macro,
    /// User-owned or licensed alternative-data observations.
    AlternativeData,
}

impl AnalyticalObservationTemplate {
    const fn storage_name(self) -> Option<&'static str> {
        match self {
            Self::All => None,
            Self::Fundamental => Some("fundamental"),
            Self::Macro => Some("macro"),
            Self::AlternativeData => Some("alternative_data"),
        }
    }
}

/// Inclusive conservative availability-time filter for point-in-time observation reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservationKnowledgeRange {
    start: Timestamp,
    end: Timestamp,
}

impl ObservationKnowledgeRange {
    /// Constructs an inclusive range over retained `available_at` evidence.
    pub fn try_new(start: Timestamp, end: Timestamp) -> Result<Self, AnalyticalReadError> {
        if start > end {
            Err(AnalyticalReadError::InvalidKnowledgeRange)
        } else {
            Ok(Self { start, end })
        }
    }

    /// Returns the inclusive lower availability bound.
    pub const fn start(self) -> Timestamp {
        self.start
    }

    /// Returns the inclusive upper availability bound.
    pub const fn end(self) -> Timestamp {
        self.end
    }
}

/// Exact immutable input for one engine-owned observation query template.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticalObservationReadRequest {
    manifest: DatasetManifestRef,
    template: AnalyticalObservationTemplate,
    instrument_ids: Box<[InstrumentId]>,
    knowledge_range: Option<ObservationKnowledgeRange>,
}

impl AnalyticalObservationReadRequest {
    /// Validates the canonical research schema and bounded optional scope.
    pub fn try_new(
        manifest: DatasetManifestRef,
        template: AnalyticalObservationTemplate,
        mut instrument_ids: Vec<InstrumentId>,
        knowledge_range: Option<ObservationKnowledgeRange>,
    ) -> Result<Self, AnalyticalReadError> {
        let canonical = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| AnalyticalReadError::InvalidObservationSchema)?;
        if manifest.schema() != &canonical || instrument_ids.len() > MAX_FILTER_INSTRUMENTS {
            return Err(if manifest.schema() != &canonical {
                AnalyticalReadError::InvalidObservationSchema
            } else {
                AnalyticalReadError::InstrumentLimitExceeded
            });
        }
        instrument_ids.sort_unstable();
        instrument_ids.dedup();
        Ok(Self {
            manifest,
            template,
            instrument_ids: instrument_ids.into_boxed_slice(),
            knowledge_range,
        })
    }

    /// Returns the exact immutable input generation.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the closed query template.
    pub const fn template(&self) -> AnalyticalObservationTemplate {
        self.template
    }

    /// Returns the optional stable instrument scope in canonical order.
    pub fn instrument_ids(&self) -> &[InstrumentId] {
        &self.instrument_ids
    }

    /// Returns the optional conservative availability-time scope.
    pub const fn knowledge_range(&self) -> Option<ObservationKnowledgeRange> {
        self.knowledge_range
    }

    fn sql(&self) -> String {
        let mut filters = self
            .template
            .storage_name()
            .map(|name| vec![format!("observation_kind = '{name}'")])
            .unwrap_or_default();
        if !self.instrument_ids.is_empty() {
            let instruments = self
                .instrument_ids
                .iter()
                .map(|instrument_id| format!("'{instrument_id}'"))
                .collect::<Vec<_>>()
                .join(",");
            filters.push(format!("instrument_id IN ({instruments})"));
        }
        if let Some(range) = self.knowledge_range {
            filters.push(format!(
                "available_at IS NOT NULL \
                 AND CAST(available_at AS BIGINT) >= {} \
                 AND CAST(available_at AS BIGINT) <= {}",
                range.start.unix_nanos(),
                range.end.unix_nanos()
            ));
        }
        let predicate = if filters.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", filters.join(" AND "))
        };
        format!(
            "SELECT * FROM {OBSERVATION_TABLE}{predicate} \
             ORDER BY source_id, source_identifier, revision, payload_sha256"
        )
    }
}

/// Manifest-pinned fixed-template output and its generation-level source owner.
#[derive(Debug)]
pub struct AnalyticalObservationOutput {
    source_id: SourceId,
    output: PinnedQueryOutput,
}

impl AnalyticalObservationOutput {
    /// Returns the source-rights namespace that owns the queried generation.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the non-forgeable manifest/object/query/result evidence.
    pub const fn output(&self) -> &PinnedQueryOutput {
        &self.output
    }
}

/// Cloneable immutable analytical read authority with no catalog-writer or raw-SQL surface.
#[derive(Clone)]
pub struct AnalyticalReadCapability {
    manifests: Arc<AnalyticalManifestCatalog>,
    objects: Arc<ParquetObjectStore>,
}

impl fmt::Debug for AnalyticalReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnalyticalReadCapability")
            .field("manifests", &"[IMMUTABLE GENERATION CATALOG]")
            .field("objects", &"[PINNED READ-ONLY OBJECT ROOT]")
            .finish()
    }
}

impl AnalyticalReadCapability {
    pub(crate) fn new(
        manifests: Arc<AnalyticalManifestCatalog>,
        objects: Arc<ParquetObjectStore>,
    ) -> Self {
        Self { manifests, objects }
    }

    /// Lists one stable dataset-id page, returning each dataset's latest immutable generation.
    pub fn datasets(
        &self,
        after: Option<&DatasetId>,
        limit: AnalyticalReadLimit,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticalGenerationPage, AnalyticalReadError> {
        self.manifests
            .read_latest_page(after, limit.get(), deadline, cancellation)
            .map(AnalyticalGenerationPage::from_catalog)
            .map_err(Into::into)
    }

    /// Lists one stable dataset-id page of durable Python-admitted feature generations.
    pub fn feature_datasets(
        &self,
        after: Option<&DatasetId>,
        limit: AnalyticalReadLimit,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticalFeatureDatasetPage, AnalyticalReadError> {
        self.feature_dataset_snapshot(
            AnalyticalFeatureDatasetSelection::Page { after },
            &[],
            limit,
            deadline,
            cancellation,
        )
    }

    /// Reads one exact or cursor-relative durable page and bounded legacy overlap set atomically.
    pub fn feature_dataset_snapshot(
        &self,
        selection: AnalyticalFeatureDatasetSelection<'_>,
        legacy_candidates: &[DatasetId],
        limit: AnalyticalReadLimit,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticalFeatureDatasetPage, AnalyticalReadError> {
        let selection = match selection {
            AnalyticalFeatureDatasetSelection::Exact(dataset_id) => {
                CatalogFeatureDatasetSelection::Exact(dataset_id)
            }
            AnalyticalFeatureDatasetSelection::Page { after } => {
                CatalogFeatureDatasetSelection::Page { after }
            }
        };
        self.manifests
            .read_feature_dataset_snapshot(
                selection,
                legacy_candidates,
                limit.get(),
                deadline,
                cancellation,
            )
            .map_err(AnalyticalReadError::from)
            .and_then(AnalyticalFeatureDatasetPage::from_catalog)
    }

    /// Resolves the latest durable Python admission for one feature-dataset identity.
    pub fn feature_dataset(
        &self,
        dataset_id: &DatasetId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<AnalyticalFeatureDataset>, AnalyticalReadError> {
        Ok(self
            .feature_dataset_snapshot(
                AnalyticalFeatureDatasetSelection::Exact(dataset_id),
                &[],
                AnalyticalReadLimit::try_new(1)?,
                deadline,
                cancellation,
            )?
            .datasets
            .into_vec()
            .into_iter()
            .next())
    }

    /// Resolves the latest immutable generation for one dataset.
    pub fn latest(
        &self,
        dataset_id: &DatasetId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<AnalyticalGeneration>, AnalyticalReadError> {
        Ok(self
            .manifests
            .read_latest(dataset_id, deadline, cancellation)?
            .map(|(pinned, source_id, export)| {
                AnalyticalGeneration::from_pinned(pinned, source_id, export)
            }))
    }

    /// Resolves only the exact supplied immutable generation identity.
    pub fn exact(
        &self,
        manifest: &DatasetManifestRef,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticalGeneration, AnalyticalReadError> {
        let (pinned, source_id, export) =
            self.manifests
                .read_exact(manifest, deadline, cancellation)?;
        Ok(AnalyticalGeneration::from_pinned(pinned, source_id, export))
    }

    /// Returns a newest-first generation-history page below an optional exclusive version cursor.
    pub fn history(
        &self,
        dataset_id: &DatasetId,
        before_version: Option<u64>,
        limit: AnalyticalReadLimit,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticalGenerationPage, AnalyticalReadError> {
        self.manifests
            .read_history(
                dataset_id,
                before_version,
                limit.get(),
                deadline,
                cancellation,
            )
            .map(AnalyticalGenerationPage::from_catalog)
            .map_err(Into::into)
    }

    /// Resolves and verifies the source-rights namespace for one exact generation.
    pub fn source_owner(
        &self,
        manifest: &DatasetManifestRef,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SourceId, AnalyticalReadError> {
        self.manifests
            .read_exact(manifest, deadline, cancellation)
            .map(|(_, source_id, _)| source_id)
            .map_err(Into::into)
    }

    /// Executes one closed observation template over an exact pinned generation.
    pub async fn read_observations(
        &self,
        request: AnalyticalObservationReadRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<AnalyticalObservationOutput, AnalyticalReadError> {
        let (pinned, source_id, _) =
            self.manifests
                .read_exact(request.manifest(), deadline, &cancellation)?;
        let query = QueryRequest::try_new(pinned.manifest().clone(), request.sql())?;
        let engine = ResearchQueryEngine::from_pinned_dataset(
            pinned,
            OBSERVATION_TABLE,
            Arc::clone(&self.objects),
            cancellation.clone(),
        )
        .await?;
        let operation_cancellation = cancellation.child_token();
        let execution_cancellation = operation_cancellation.clone();
        let execution = engine.query_pinned(query, limits, execution_cancellation);
        tokio::pin!(execution);
        let deadline_at = tokio::time::Instant::from_std(deadline);
        let deadline_wait = tokio::time::sleep_until(deadline_at);
        tokio::pin!(deadline_wait);
        let output = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                operation_cancellation.cancel();
                let _ignored = execution.as_mut().await;
                return Err(AnalyticalReadError::Query(QueryError::Cancelled));
            }
            _ = deadline_wait.as_mut() => {
                operation_cancellation.cancel();
                let _ignored = execution.as_mut().await;
                return Err(AnalyticalReadError::Query(QueryError::DeadlineExceeded));
            }
            result = execution.as_mut() => result?,
        };
        Ok(AnalyticalObservationOutput { source_id, output })
    }

    /// Reads one producer-issued monetary observation from an exact immutable generation.
    ///
    /// The only selector is a bounded canonical row offset. Caller SQL, physical paths, catalog
    /// mutation, and caller-supplied monetary values are absent from this capability.
    pub async fn research_monetary_value(
        &self,
        manifest: &DatasetManifestRef,
        row: usize,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PinnedMonetaryValue, AnalyticalReadError> {
        let (pinned, _, _) = self
            .manifests
            .read_exact(manifest, deadline, &cancellation)?;
        let operation_cancellation = cancellation.child_token();
        let execution_cancellation = operation_cancellation.clone();
        let objects = Arc::clone(&self.objects);
        let execution = async move {
            let engine = ResearchQueryEngine::from_pinned_dataset(
                pinned,
                OBSERVATION_TABLE,
                objects,
                execution_cancellation.clone(),
            )
            .await?;
            engine
                .canonical_research_monetary_value(row, limits, execution_cancellation)
                .await
        };
        tokio::pin!(execution);
        let deadline_wait = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
        tokio::pin!(deadline_wait);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                operation_cancellation.cancel();
                let _ignored = execution.as_mut().await;
                Err(AnalyticalReadError::Query(QueryError::Cancelled))
            }
            _ = deadline_wait.as_mut() => {
                operation_cancellation.cancel();
                let _ignored = execution.as_mut().await;
                Err(AnalyticalReadError::Query(QueryError::DeadlineExceeded))
            }
            result = execution.as_mut() => result.map_err(Into::into),
        }
    }

    /// Reads one producer-issued monetary feature from an exact immutable generation.
    ///
    /// The feature identity, point-in-time coordinate, lineage, value, and currency all come from
    /// the registered canonical feature row.
    pub async fn feature_monetary_value(
        &self,
        manifest: &DatasetManifestRef,
        row: usize,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PinnedFeatureMonetaryValue, AnalyticalReadError> {
        let (pinned, _, _) = self
            .manifests
            .read_exact(manifest, deadline, &cancellation)?;
        let operation_cancellation = cancellation.child_token();
        let execution_cancellation = operation_cancellation.clone();
        let objects = Arc::clone(&self.objects);
        let execution = async move {
            let engine = ResearchQueryEngine::from_pinned_dataset(
                pinned,
                OBSERVATION_TABLE,
                objects,
                execution_cancellation.clone(),
            )
            .await?;
            engine
                .canonical_feature_monetary_value(row, limits, execution_cancellation)
                .await
        };
        tokio::pin!(execution);
        let deadline_wait = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
        tokio::pin!(deadline_wait);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                operation_cancellation.cancel();
                let _ignored = execution.as_mut().await;
                Err(AnalyticalReadError::Query(QueryError::Cancelled))
            }
            _ = deadline_wait.as_mut() => {
                operation_cancellation.cancel();
                let _ignored = execution.as_mut().await;
                Err(AnalyticalReadError::Query(QueryError::DeadlineExceeded))
            }
            result = execution.as_mut() => result.map_err(Into::into),
        }
    }
}

/// Immutable analytical request validation or execution failure.
#[derive(Debug, Error)]
pub enum AnalyticalReadError {
    /// A page limit was zero or exceeded the hard service ceiling.
    #[error("analytical read limit is invalid")]
    InvalidLimit,
    /// An observation request exceeded the stable-instrument filter ceiling.
    #[error("analytical observation instrument limit was exceeded")]
    InstrumentLimitExceeded,
    /// An availability-time range was reversed.
    #[error("analytical observation knowledge range is invalid")]
    InvalidKnowledgeRange,
    /// Fixed observation templates require the canonical research-observation schema.
    #[error("analytical observation schema is invalid")]
    InvalidObservationSchema,
    /// Immutable generation lookup failed.
    #[error("analytical manifest read failed: {0}")]
    Manifest(#[from] ManifestCatalogError),
    /// Fixed-template query construction or execution failed.
    #[error("analytical fixed-template query failed: {0}")]
    Query(#[from] QueryError),
}
