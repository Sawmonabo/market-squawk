//! Least-authority immutable analytical catalog and fixed-template observation reads.

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array as _, BinaryArray, Date32Array, Int64Array, StringArray, UInt32Array};
use market_squawk_domain::{
    BarTimestampBasis, CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest,
    FundNavObservation, InstrumentId, MacroObservation, MarketBarAdjustment, MarketBarObservation,
    MarketBarSessionEvidence, ProviderInstrumentId, ResearchObservation,
    ResearchTemporalCoordinate, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::CanonicalObservationFamily;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[path = "analytical_read/forecast.rs"]
mod forecast;

pub use forecast::{
    ForecastDatasetEvidence, ForecastDatasetEvidenceFence, ForecastDatasetReadLimits,
    ForecastFeatureRow, ForecastFeatureValue,
};

use crate::manifest::{
    CatalogFeatureDataset, CatalogFeatureDatasetPage, CatalogFeatureDatasetSelection,
    CatalogGenerationPage,
};
use crate::{
    AnalyticalManifestCatalog, DatasetBuildSpecDigest, DatasetId, DatasetManifestRef,
    DatasetSchemaRegistry, DatasetSplitCounts, FeatureDatasetProductContract, GenerationKind,
    GenerationParent, ManifestCatalogError, ParquetObjectStore, PinnedDataset,
    PinnedFeatureMonetaryValue, PinnedMonetaryValue, PinnedQueryOutput, QueryError, QueryLimits,
    QueryRequest, ResearchQueryEngine, Sha256Digest, UniverseId,
};
use crate::{
    PointInTimeCandidate, PointInTimeLimits, PointInTimePolicy, PointInTimeRequest,
    PointInTimeRevisionMode, PointInTimeService,
};

const MAX_READ_ITEMS: usize = 64;
const MAX_FILTER_INSTRUMENTS: usize = 256;
const MAX_MARKET_BAR_ROWS: u32 = 50_000;
const MAX_MARKET_BAR_REVISION_CANDIDATES: usize = 100_000;
const MAX_FUND_NAV_ROWS: u32 = 10_000;
const MAX_FUND_NAV_REVISION_CANDIDATES: usize = 100_000;
const MAX_MACRO_SNAPSHOT_SERIES: usize = 32;
const MAX_MACRO_SNAPSHOT_TIED_CANDIDATES_PER_SERIES: usize = 8;
const MAX_OUTCOME_MARKET_BAR_CANDIDATES: usize = 4_096;
const OUTCOME_MARKET_BAR_QUERY_BYTES: u64 = 64 * 1024 * 1024;
const OUTCOME_MARKET_BAR_QUERY_MEMORY_BYTES: u64 = 128 * 1024 * 1024;
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

/// One durable receipt-admitted feature/label generation in the public analytical registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticalFeatureDataset {
    generation: AnalyticalGeneration,
    python_export_sha256: Sha256Digest,
    production_receipt: crate::FeatureDatasetProductionReceiptV1,
    product_contract: FeatureDatasetProductContract,
    policy_digest: Sha256Digest,
    universe_digest: Sha256Digest,
    universe_id: UniverseId,
    split_counts: DatasetSplitCounts,
    source_ids: Box<[SourceId]>,
}

impl AnalyticalFeatureDataset {
    fn from_catalog(
        dataset: CatalogFeatureDataset,
        expected_contract: FeatureDatasetProductContract,
    ) -> Result<Self, AnalyticalReadError> {
        if dataset.product_contract != expected_contract
            || dataset.research_use != expected_contract.required_use()
        {
            return Err(ManifestCatalogError::CorruptCatalog.into());
        }
        let summary = crate::python_dataset::feature_dataset_summary(
            &dataset.descriptor,
            dataset.export_sha256,
        )
        .map_err(|_| AnalyticalReadError::Manifest(ManifestCatalogError::CorruptCatalog))?;
        let production_receipt = crate::FeatureDatasetProductionReceiptV1::decode_and_validate(
            &dataset.receipt_json,
            &crate::dataset_builder::FeatureDatasetProductionReceiptExpectation {
                production_identity: dataset.production_identity,
                receipt_sha256: dataset.receipt_sha256,
                catalog_identity: dataset.catalog_identity,
                product_contract: dataset.product_contract,
                manifest: dataset.pinned.manifest(),
                build_spec_digest: summary.identity.build_spec_digest(),
                policy_digest: summary.identity.policy_digest(),
                universe_digest: summary.identity.universe_digest(),
                universe_id: summary.identity.universe_id().as_str(),
                output_group_id: dataset.output_group_id,
                final_output_rights_id: dataset.final_output_rights_id,
                export_sha256: dataset.export_sha256,
                research_decision: dataset.research_decision,
                research_graph: dataset.research_graph,
                research_use: dataset.research_use,
                research_use_expires_at: dataset.research_use_expires_at,
                admitted_at: dataset.admitted_at,
            },
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
            production_receipt,
            product_contract: dataset.product_contract,
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

    /// Returns the required immutable producer receipt admitted with the Python descriptor.
    pub const fn production_receipt(&self) -> &crate::FeatureDatasetProductionReceiptV1 {
        &self.production_receipt
    }

    /// Returns the exact closed recipe and independently authorized consumer use.
    pub const fn product_contract(&self) -> FeatureDatasetProductContract {
        self.product_contract
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

/// One stable bounded page of durable receipt-admitted feature datasets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticalFeatureDatasetPage {
    datasets: Box<[AnalyticalFeatureDataset]>,
    has_more: bool,
    available: usize,
    overlapping_legacy_dataset_ids: Box<[DatasetId]>,
}

impl AnalyticalFeatureDatasetPage {
    fn from_catalog(
        page: CatalogFeatureDatasetPage,
        expected_contract: FeatureDatasetProductContract,
    ) -> Result<Self, AnalyticalReadError> {
        let datasets = page
            .datasets
            .into_iter()
            .map(|dataset| AnalyticalFeatureDataset::from_catalog(dataset, expected_contract))
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
    /// Regulatory and issuer filing observations.
    Filing,
    /// Reported fundamental and XBRL fact observations.
    Fundamental,
    /// Macroeconomic series observations and revisions.
    Macro,
    /// Exact historical market-bar observations.
    MarketBar,
    /// Exact daily fund/share-class NAV observations.
    FundNav,
    /// Exact historical universe-membership observations.
    UniverseMembership,
    /// User-owned or licensed alternative-data observations.
    AlternativeData,
}

impl AnalyticalObservationTemplate {
    const fn storage_name(self) -> Option<&'static str> {
        match self {
            Self::All => None,
            Self::Filing => Some("filing"),
            Self::Fundamental => Some("fundamental"),
            Self::Macro => Some("macro"),
            Self::MarketBar => Some("market_bar"),
            Self::FundNav => Some("fund_nav"),
            Self::UniverseMembership => Some("universe_membership"),
            Self::AlternativeData => Some("alternative_data"),
        }
    }
}

/// Canonically ordered, nonempty Macro series set compiled into the application.
///
/// Construction accepts only a `'static` slice so request input cannot widen the series authority
/// at runtime. Duplicate identities are rejected rather than silently changing the declared set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticalMacroSeriesAllowlist {
    series: Box<[SourceIdentifier]>,
}

impl AnalyticalMacroSeriesAllowlist {
    /// Parses at most 32 code-owned series identifiers into canonical order.
    pub fn try_from_code_owned(
        values: &'static [&'static str],
    ) -> Result<Self, AnalyticalReadError> {
        if values.is_empty() || values.len() > MAX_MACRO_SNAPSHOT_SERIES {
            return Err(AnalyticalReadError::InvalidMacroSeriesAllowlist);
        }
        let mut series = Vec::new();
        series
            .try_reserve_exact(values.len())
            .map_err(|_| AnalyticalReadError::InvalidMacroSeriesAllowlist)?;
        for value in values {
            series.push(
                SourceIdentifier::try_from(*value)
                    .map_err(|_| AnalyticalReadError::InvalidMacroSeriesAllowlist)?,
            );
        }
        series.sort_unstable();
        if series.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AnalyticalReadError::InvalidMacroSeriesAllowlist);
        }
        Ok(Self {
            series: series.into_boxed_slice(),
        })
    }

    /// Canonicalizes adapter-produced identities selected by code-owned application policy.
    pub fn try_from_code_owned_identifiers(
        mut values: Vec<SourceIdentifier>,
    ) -> Result<Self, AnalyticalReadError> {
        if values.is_empty() || values.len() > MAX_MACRO_SNAPSHOT_SERIES {
            return Err(AnalyticalReadError::InvalidMacroSeriesAllowlist);
        }
        values.sort_unstable();
        if values.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AnalyticalReadError::InvalidMacroSeriesAllowlist);
        }
        Ok(Self {
            series: values.into_boxed_slice(),
        })
    }

    /// Returns the exact canonical series set used by filtering and selection identity.
    pub fn series(&self) -> &[SourceIdentifier] {
        &self.series
    }

    fn contains(&self, series: &SourceIdentifier) -> bool {
        self.series.binary_search(series).is_ok()
    }
}

/// Exact immutable request for a bounded latest-known Macro snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticalMacroLatestKnownRequest {
    manifest: DatasetManifestRef,
    source_id: SourceId,
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: CalendarDate,
    series_allowlist: AnalyticalMacroSeriesAllowlist,
}

impl AnalyticalMacroLatestKnownRequest {
    /// Validates the canonical schema and retains the exact source, cutoff, and code-owned set.
    pub fn try_new(
        manifest: DatasetManifestRef,
        source_id: SourceId,
        knowledge_cutoff: Timestamp,
        effective_date_cutoff: CalendarDate,
        series_allowlist: AnalyticalMacroSeriesAllowlist,
    ) -> Result<Self, AnalyticalReadError> {
        let canonical = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| AnalyticalReadError::InvalidObservationSchema)?;
        if manifest.schema() != &canonical {
            return Err(AnalyticalReadError::InvalidObservationSchema);
        }
        Ok(Self {
            manifest,
            source_id,
            knowledge_cutoff,
            effective_date_cutoff,
            series_allowlist,
        })
    }

    /// Returns the exact immutable input generation.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the sole source-rights namespace admitted by this request.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the inclusive conservative local-knowledge cutoff.
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the inclusive calendar-precision effective-date cutoff.
    pub const fn effective_date_cutoff(&self) -> CalendarDate {
        self.effective_date_cutoff
    }

    /// Returns the canonical nonempty code-owned series set.
    pub const fn series_allowlist(&self) -> &AnalyticalMacroSeriesAllowlist {
        &self.series_allowlist
    }

    /// Returns the minimum query row envelope needed to retain ties plus a saturation sentinel.
    pub fn required_query_rows(&self) -> u64 {
        u64::try_from(self.candidate_limit_with_sentinel()).unwrap_or(u64::MAX)
    }

    fn sql(&self) -> String {
        let source_id = sql_string_literal(self.source_id.as_str());
        let cutoff = self.knowledge_cutoff.unix_nanos();
        let effective_cutoff = self.effective_date_cutoff;
        let series = self
            .series_allowlist
            .series()
            .iter()
            .map(|series| sql_string_literal(series.as_str()))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "WITH eligible AS ( \
                 SELECT macro_series, effective_date, revision, payload_sha256, payload_json, \
                        source_identifier \
                 FROM {OBSERVATION_TABLE} \
                 WHERE observation_kind = 'macro' \
                   AND source_id = {source_id} \
                   AND macro_series IN ({series}) \
                   AND available_at IS NOT NULL \
                   AND CAST(available_at AS BIGINT) <= {cutoff} \
                   AND CAST(received_at AS BIGINT) <= {cutoff} \
                   AND CAST(ingested_at AS BIGINT) <= {cutoff} \
                   AND (published_precision IS NULL \
                        OR published_precision <> 'exact_timestamp' \
                        OR (published_at IS NOT NULL \
                            AND CAST(published_at AS BIGINT) <= {cutoff})) \
                   AND effective_precision = 'calendar_date' \
                   AND effective_date IS NOT NULL \
                   AND effective_date <= DATE '{effective_cutoff}' \
             ), latest_date AS ( \
                 SELECT macro_series, MAX(effective_date) AS effective_date \
                 FROM eligible GROUP BY macro_series \
            ), latest_revision AS ( \
                 SELECT eligible.macro_series, eligible.effective_date, \
                        MAX(eligible.revision) AS revision \
                 FROM eligible \
                 JOIN latest_date \
                   ON eligible.macro_series = latest_date.macro_series \
                  AND eligible.effective_date = latest_date.effective_date \
                 GROUP BY eligible.macro_series, eligible.effective_date \
             ), selected AS ( \
                 SELECT eligible.macro_series, eligible.effective_date, eligible.revision, \
                        eligible.payload_sha256, eligible.payload_json, \
                        eligible.source_identifier \
                 FROM eligible \
                 JOIN latest_revision \
                   ON eligible.macro_series = latest_revision.macro_series \
                  AND eligible.effective_date = latest_revision.effective_date \
                  AND eligible.revision = latest_revision.revision \
             ), tie_counts AS ( \
                 SELECT macro_series, effective_date, revision, COUNT(*) AS tie_count \
                 FROM selected GROUP BY macro_series, effective_date, revision \
             ) \
             SELECT selected.macro_series, selected.effective_date, selected.revision, \
                    selected.payload_sha256, selected.payload_json, tie_counts.tie_count \
             FROM selected \
             JOIN tie_counts \
               ON selected.macro_series = tie_counts.macro_series \
              AND selected.effective_date = tie_counts.effective_date \
              AND selected.revision = tie_counts.revision \
             ORDER BY selected.macro_series, selected.payload_sha256, \
                      selected.source_identifier \
             LIMIT {}",
            self.candidate_limit_with_sentinel()
        )
    }

    fn candidate_limit(&self) -> usize {
        self.series_allowlist
            .series()
            .len()
            .saturating_mul(MAX_MACRO_SNAPSHOT_TIED_CANDIDATES_PER_SERIES)
    }

    fn candidate_limit_with_sentinel(&self) -> usize {
        self.candidate_limit().saturating_add(1)
    }
}

/// Nonzero Fund NAV result count under the fixed typed-read ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticalFundNavReadLimit(NonZeroU32);

impl AnalyticalFundNavReadLimit {
    /// Constructs a typed NAV limit no greater than 10,000 rows.
    pub fn try_new(value: u32) -> Result<Self, AnalyticalReadError> {
        NonZeroU32::new(value)
            .filter(|limit| limit.get() <= MAX_FUND_NAV_ROWS)
            .map(Self)
            .ok_or(AnalyticalReadError::InvalidFundNavLimit)
    }

    const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Inclusive calendar-precision date range for daily NAV history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FundNavDateRange {
    start: CalendarDate,
    end: CalendarDate,
}

impl FundNavDateRange {
    /// Constructs one non-reversed NAV-date range without inventing time-of-day precision.
    pub fn try_new(start: CalendarDate, end: CalendarDate) -> Result<Self, AnalyticalReadError> {
        if start > end {
            Err(AnalyticalReadError::InvalidFundNavDateRange)
        } else {
            Ok(Self { start, end })
        }
    }

    pub const fn start(self) -> CalendarDate {
        self.start
    }

    pub const fn end(self) -> CalendarDate {
        self.end
    }
}

/// Exact immutable input for a typed single-fund/share-class NAV history read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticalFundNavReadRequest {
    manifest: DatasetManifestRef,
    instrument_id: InstrumentId,
    knowledge_cutoff: Timestamp,
    date_range: Option<FundNavDateRange>,
    revision_mode: PointInTimeRevisionMode,
    limit: AnalyticalFundNavReadLimit,
}

impl AnalyticalFundNavReadRequest {
    /// Validates the canonical schema and retains all fixed PIT/query bounds.
    pub fn try_new(
        manifest: DatasetManifestRef,
        instrument_id: InstrumentId,
        knowledge_cutoff: Timestamp,
        date_range: Option<FundNavDateRange>,
        revision_mode: PointInTimeRevisionMode,
        limit: AnalyticalFundNavReadLimit,
    ) -> Result<Self, AnalyticalReadError> {
        let canonical = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| AnalyticalReadError::InvalidObservationSchema)?;
        if manifest.schema() != &canonical {
            return Err(AnalyticalReadError::InvalidObservationSchema);
        }
        Ok(Self {
            manifest,
            instrument_id,
            knowledge_cutoff,
            date_range,
            revision_mode,
            limit,
        })
    }

    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    pub const fn date_range(&self) -> Option<FundNavDateRange> {
        self.date_range
    }

    pub const fn revision_mode(&self) -> PointInTimeRevisionMode {
        self.revision_mode
    }

    fn sql(&self) -> String {
        let mut filters = vec![
            "observation_kind = 'fund_nav'".to_owned(),
            format!("instrument_id = '{}'", self.instrument_id),
            "available_at IS NOT NULL".to_owned(),
            format!(
                "CAST(available_at AS BIGINT) <= {}",
                self.knowledge_cutoff.unix_nanos()
            ),
            "effective_date IS NOT NULL".to_owned(),
        ];
        if let Some(range) = self.date_range {
            filters.push(format!(
                "effective_date >= DATE '{}' AND effective_date <= DATE '{}'",
                range.start, range.end
            ));
        }
        format!(
            "SELECT payload_json FROM {OBSERVATION_TABLE} WHERE {} \
             ORDER BY effective_date, source_id, revision, payload_sha256, source_identifier \
             LIMIT {}",
            filters.join(" AND "),
            MAX_FUND_NAV_REVISION_CANDIDATES + 1
        )
    }
}

/// Nonzero market-bar result count under the fixed typed-read ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticalMarketBarReadLimit(NonZeroU32);

impl AnalyticalMarketBarReadLimit {
    /// Constructs a typed bar limit no greater than 50,000 rows.
    pub fn try_new(value: u32) -> Result<Self, AnalyticalReadError> {
        NonZeroU32::new(value)
            .filter(|limit| limit.get() <= MAX_MARKET_BAR_ROWS)
            .map(Self)
            .ok_or(AnalyticalReadError::InvalidMarketBarLimit)
    }

    const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Inclusive exact effective-time range for typed market-bar reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketBarEffectiveRange {
    start: Timestamp,
    end: Timestamp,
}

impl MarketBarEffectiveRange {
    /// Constructs an inclusive range over exact bar timestamps.
    pub fn try_new(start: Timestamp, end: Timestamp) -> Result<Self, AnalyticalReadError> {
        if start > end {
            Err(AnalyticalReadError::InvalidMarketBarEffectiveRange)
        } else {
            Ok(Self { start, end })
        }
    }

    /// Returns the inclusive lower effective-time bound.
    pub const fn start(self) -> Timestamp {
        self.start
    }

    /// Returns the inclusive upper effective-time bound.
    pub const fn end(self) -> Timestamp {
        self.end
    }
}

/// Exact immutable input for a typed, single-instrument point-in-time market-bar read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticalMarketBarReadRequest {
    manifest: DatasetManifestRef,
    instrument_id: InstrumentId,
    knowledge_cutoff: Timestamp,
    effective_range: Option<MarketBarEffectiveRange>,
    limit: AnalyticalMarketBarReadLimit,
}

impl AnalyticalMarketBarReadRequest {
    /// Validates the canonical research schema and retains all fixed query bounds.
    pub fn try_new(
        manifest: DatasetManifestRef,
        instrument_id: InstrumentId,
        knowledge_cutoff: Timestamp,
        effective_range: Option<MarketBarEffectiveRange>,
        limit: AnalyticalMarketBarReadLimit,
    ) -> Result<Self, AnalyticalReadError> {
        let canonical = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| AnalyticalReadError::InvalidObservationSchema)?;
        if manifest.schema() != &canonical {
            return Err(AnalyticalReadError::InvalidObservationSchema);
        }
        Ok(Self {
            manifest,
            instrument_id,
            knowledge_cutoff,
            effective_range,
            limit,
        })
    }

    /// Returns the exact immutable input generation.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the sole stable instrument admitted by this request.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the conservative local-knowledge cutoff.
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the optional exact bar-time range.
    pub const fn effective_range(&self) -> Option<MarketBarEffectiveRange> {
        self.effective_range
    }

    fn sql(&self) -> String {
        let mut filters = vec![
            "observation_kind = 'market_bar'".to_owned(),
            format!("instrument_id = '{}'", self.instrument_id),
            "available_at IS NOT NULL".to_owned(),
            format!(
                "CAST(available_at AS BIGINT) <= {}",
                self.knowledge_cutoff.unix_nanos()
            ),
            "effective_at IS NOT NULL".to_owned(),
        ];
        if let Some(range) = self.effective_range {
            filters.push(format!(
                "CAST(effective_at AS BIGINT) >= {} AND CAST(effective_at AS BIGINT) <= {}",
                range.start.unix_nanos(),
                range.end.unix_nanos()
            ));
        }
        let predicate = filters.join(" AND ");
        format!(
            "SELECT payload_json \
             FROM {OBSERVATION_TABLE} \
             WHERE {predicate} \
             ORDER BY effective_at, source_id, venue_id, revision DESC, payload_sha256, \
                      source_identifier \
             LIMIT {}",
            MAX_MARKET_BAR_REVISION_CANDIDATES + 1
        )
    }
}

/// Exact stable market-bar series whose realized outcome may be selected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutcomeMarketBarSeries {
    instrument_id: InstrumentId,
    source_id: SourceId,
    venue_id: VenueId,
    provider_instrument_id: ProviderInstrumentId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    adjustment: MarketBarAdjustment,
    timestamp_basis: BarTimestampBasis,
    session: MarketBarSessionEvidence,
}

impl OutcomeMarketBarSeries {
    /// Constructs a fully qualified series without accepting a caller-supplied value or row bound.
    #[allow(
        clippy::too_many_arguments,
        reason = "every provider, venue, adjustment, timestamp, and session dimension stays explicit"
    )]
    pub const fn new(
        instrument_id: InstrumentId,
        source_id: SourceId,
        venue_id: VenueId,
        provider_instrument_id: ProviderInstrumentId,
        feed: SourceIdentifier,
        interval: SourceIdentifier,
        adjustment: MarketBarAdjustment,
        timestamp_basis: BarTimestampBasis,
        session: MarketBarSessionEvidence,
    ) -> Self {
        Self {
            instrument_id,
            source_id,
            venue_id,
            provider_instrument_id,
            feed,
            interval,
            adjustment,
            timestamp_basis,
            session,
        }
    }

    /// Returns the stable internal instrument.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact source-rights namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact venue.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the exact provider instrument identity.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the exact provider feed.
    pub const fn feed(&self) -> &SourceIdentifier {
        &self.feed
    }

    /// Returns the exact provider interval.
    pub const fn interval(&self) -> &SourceIdentifier {
        &self.interval
    }

    /// Returns the exact corporate-action adjustment.
    pub const fn adjustment(&self) -> MarketBarAdjustment {
        self.adjustment
    }

    /// Returns the exact provider timestamp boundary convention.
    pub const fn timestamp_basis(&self) -> BarTimestampBasis {
        self.timestamp_basis
    }

    /// Returns exact session kind, ruleset, and ruleset evidence.
    pub const fn session(&self) -> &MarketBarSessionEvidence {
        &self.session
    }
}

/// Exact-manifest request for the first uniquely completed bar at or after a forecast horizon.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutcomeMarketBarRequest {
    manifest: DatasetManifestRef,
    series: OutcomeMarketBarSeries,
    knowledge_cutoff: Timestamp,
    horizon: Timestamp,
    latest_eligible_completion: Timestamp,
}

impl OutcomeMarketBarRequest {
    /// Validates the sole current research schema and the closed completion window.
    pub fn try_new(
        manifest: DatasetManifestRef,
        series: OutcomeMarketBarSeries,
        knowledge_cutoff: Timestamp,
        horizon: Timestamp,
        latest_eligible_completion: Timestamp,
    ) -> Result<Self, AnalyticalReadError> {
        let canonical = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| AnalyticalReadError::InvalidObservationSchema)?;
        if manifest.schema() != &canonical {
            return Err(AnalyticalReadError::InvalidObservationSchema);
        }
        if horizon > latest_eligible_completion {
            return Err(AnalyticalReadError::InvalidOutcomeMarketBarWindow);
        }
        Ok(Self {
            manifest,
            series,
            knowledge_cutoff,
            horizon,
            latest_eligible_completion,
        })
    }

    /// Returns the exact immutable input generation.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the fully qualified outcome series.
    pub const fn series(&self) -> &OutcomeMarketBarSeries {
        &self.series
    }

    /// Returns the conservative local-knowledge cutoff.
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the inclusive forecast-horizon completion bound.
    pub const fn horizon(&self) -> Timestamp {
        self.horizon
    }

    /// Returns the inclusive latest eligible completion bound.
    pub const fn latest_eligible_completion(&self) -> Timestamp {
        self.latest_eligible_completion
    }

    fn sql(&self) -> String {
        let source_id = sql_string_literal(self.series.source_id.as_str());
        let venue_id = sql_string_literal(self.series.venue_id.as_str());
        let instrument_id = self.series.instrument_id;
        let cutoff = self.knowledge_cutoff.unix_nanos();
        let latest = self.latest_eligible_completion.unix_nanos();
        let completion_lower_bound = match self.series.timestamp_basis {
            BarTimestampBasis::PeriodStart => String::new(),
            BarTimestampBasis::PeriodEnd => format!(
                " AND CAST(effective_at AS BIGINT) >= {}",
                self.horizon.unix_nanos()
            ),
        };
        format!(
            "SELECT payload_json \
             FROM {OBSERVATION_TABLE} \
             WHERE observation_kind = 'market_bar' \
               AND source_id = {source_id} \
               AND instrument_id = '{instrument_id}' \
               AND venue_id = {venue_id} \
               AND available_at IS NOT NULL \
               AND CAST(available_at AS BIGINT) <= {cutoff} \
               AND effective_at IS NOT NULL \
               AND CAST(effective_at AS BIGINT) <= {latest}{completion_lower_bound} \
             ORDER BY effective_at, source_id, venue_id, revision DESC, payload_sha256, \
                      source_identifier \
             LIMIT {}",
            MAX_OUTCOME_MARKET_BAR_CANDIDATES + 1
        )
    }
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Closed reason that an exact outcome bar could not be admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeMarketBarUnavailableReason {
    /// The exact source-rights owner does not match the requested series.
    SourceOwnerMismatch,
    /// The fixed candidate ceiling was saturated, so uniqueness cannot be proven.
    CandidateSetSaturated,
    /// At least one returned candidate was incomplete or contradicted canonical projections.
    IncompleteCandidate,
    /// No completed exact-series bar was available inside the requested horizon window.
    NoEligibleBar,
    /// More than one exact-series bar shared the earliest eligible completion boundary.
    AmbiguousCompletion,
}

/// Non-forgeable receipt for one producer-authored realized market-bar outcome.
#[derive(Debug)]
pub struct OutcomeMarketBarSelectedReceipt {
    request: OutcomeMarketBarRequest,
    request_digest: EvidenceDigest,
    output: PinnedQueryOutput,
    ordinal: u32,
    payload_digest: EvidenceDigest,
    receipt_digest: EvidenceDigest,
    bar: MarketBarObservation,
}

impl OutcomeMarketBarSelectedReceipt {
    /// Returns the complete exact-manifest selection request.
    pub const fn request(&self) -> &OutcomeMarketBarRequest {
        &self.request
    }

    /// Returns the digest of the complete request and series identity.
    pub const fn request_digest(&self) -> EvidenceDigest {
        self.request_digest
    }

    /// Returns exact manifest, object-graph, query, and result evidence.
    pub const fn output(&self) -> &PinnedQueryOutput {
        &self.output
    }

    /// Returns the selected row's zero-based ordinal in the exact query result.
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Returns the digest of the exact canonical payload bytes selected.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns the receipt digest binding request, object/query/result graph, ordinal, and bar.
    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }

    /// Returns the selected typed completed market bar.
    pub const fn bar(&self) -> &MarketBarObservation {
        &self.bar
    }
}

/// Exact outcome selection or one closed fail-safe unavailable reason.
#[derive(Debug)]
pub enum OutcomeMarketBarSelection {
    /// A uniquely earliest eligible completed bar and non-forgeable evidence receipt.
    Selected(OutcomeMarketBarSelectedReceipt),
    /// Selection failed closed without choosing a favorable price.
    Unavailable(OutcomeMarketBarUnavailableReason),
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
        if template == AnalyticalObservationTemplate::UniverseMembership
            && (!instrument_ids.is_empty() || knowledge_range.is_some())
        {
            return Err(AnalyticalReadError::UniverseMembershipReadMustBeExhaustive);
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

    /// Constructs the sole exhaustive exact-manifest universe-membership request.
    ///
    /// Instrument and availability filters are absent by construction. A successful query output
    /// therefore covers every membership row in the exact manifest; a too-small caller
    /// [`QueryLimits`] envelope fails with [`QueryError::RowLimitExceeded`] and returns no partial
    /// output, preserving an explicit saturation proof.
    pub fn try_universe_membership(
        manifest: DatasetManifestRef,
    ) -> Result<Self, AnalyticalReadError> {
        Self::try_new(
            manifest,
            AnalyticalObservationTemplate::UniverseMembership,
            Vec::new(),
            None,
        )
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

    /// Builds the engine-owned query for this closed observation template.
    ///
    /// Callers receive a parsed read-only request rather than unrestricted SQL. The request stays
    /// bound to the exact manifest, canonical observation relation, and code-owned predicates.
    pub fn query_request(&self) -> Result<QueryRequest, QueryError> {
        QueryRequest::try_new(self.manifest.clone(), self.sql())
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
    request: AnalyticalObservationReadRequest,
    output: PinnedQueryOutput,
}

/// Typed latest-known Macro observations plus exact query and final-selection evidence.
#[derive(Debug)]
pub struct AnalyticalMacroLatestKnownOutput {
    source_id: SourceId,
    output: PinnedQueryOutput,
    observations: Box<[MacroObservation]>,
    selection_digest: EvidenceDigest,
}

impl AnalyticalMacroLatestKnownOutput {
    /// Returns the exact source-rights namespace requested and verified for the generation.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns exact manifest, object graph, candidate query, and candidate result evidence.
    pub const fn output(&self) -> &PinnedQueryOutput {
        &self.output
    }

    /// Returns latest-known observations in exact canonical Macro-family order.
    pub fn observations(&self) -> &[MacroObservation] {
        &self.observations
    }

    /// Returns the code-owned SHA-256 identity of the final typed selection.
    pub const fn selection_digest(&self) -> EvidenceDigest {
        self.selection_digest
    }
}

/// Typed bars plus the non-forgeable evidence for their exact manifest-pinned query.
#[derive(Debug)]
pub struct AnalyticalMarketBarOutput {
    source_id: SourceId,
    output: PinnedQueryOutput,
    bars: Box<[MarketBarObservation]>,
}

/// Typed NAV history plus non-forgeable evidence for its exact manifest-pinned query.
#[derive(Debug)]
pub struct AnalyticalFundNavOutput {
    source_id: SourceId,
    output: PinnedQueryOutput,
    observations: Box<[FundNavObservation]>,
}

impl AnalyticalFundNavOutput {
    /// Returns the source-rights namespace that owns the queried generation.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns exact manifest, object graph, query, and result evidence.
    pub const fn output(&self) -> &PinnedQueryOutput {
        &self.output
    }

    /// Returns NAV observations in canonical family/revision order under the requested PIT mode.
    pub fn observations(&self) -> &[FundNavObservation] {
        &self.observations
    }
}

impl AnalyticalMarketBarOutput {
    /// Returns the source-rights namespace that owns the queried generation.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact manifest, object graph, query, and result evidence.
    pub const fn output(&self) -> &PinnedQueryOutput {
        &self.output
    }

    /// Returns admitted bars in ascending effective-time and canonical-family order.
    pub fn bars(&self) -> &[MarketBarObservation] {
        &self.bars
    }
}

impl AnalyticalObservationOutput {
    /// Returns the source-rights namespace that owns the queried generation.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact closed request whose complete execution produced this output.
    pub const fn request(&self) -> &AnalyticalObservationReadRequest {
        &self.request
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

    /// Lists one stable dataset-id page of durable receipt-admitted feature generations.
    pub fn feature_datasets(
        &self,
        expected_contract: FeatureDatasetProductContract,
        after: Option<&DatasetId>,
        limit: AnalyticalReadLimit,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticalFeatureDatasetPage, AnalyticalReadError> {
        self.feature_dataset_snapshot(
            expected_contract,
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
        expected_contract: FeatureDatasetProductContract,
        selection: AnalyticalFeatureDatasetSelection<'_>,
        legacy_candidates: &[DatasetId],
        limit: AnalyticalReadLimit,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticalFeatureDatasetPage, AnalyticalReadError> {
        let selection = match selection {
            AnalyticalFeatureDatasetSelection::Exact(dataset_id) => {
                CatalogFeatureDatasetSelection::LatestByDataset(dataset_id)
            }
            AnalyticalFeatureDatasetSelection::Page { after } => {
                CatalogFeatureDatasetSelection::Page { after }
            }
        };
        self.manifests
            .read_feature_dataset_snapshot(
                expected_contract,
                selection,
                legacy_candidates,
                limit.get(),
                deadline,
                cancellation,
            )
            .map_err(AnalyticalReadError::from)
            .and_then(|page| AnalyticalFeatureDatasetPage::from_catalog(page, expected_contract))
    }

    /// Resolves the latest durable receipt admission for one feature-dataset identity.
    pub fn feature_dataset(
        &self,
        expected_contract: FeatureDatasetProductContract,
        dataset_id: &DatasetId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<AnalyticalFeatureDataset>, AnalyticalReadError> {
        Ok(self
            .feature_dataset_snapshot(
                expected_contract,
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
        Ok(AnalyticalObservationOutput {
            source_id,
            request,
            output,
        })
    }

    /// Reads a bounded latest-known PIT snapshot for an exact source and code-owned Macro set.
    ///
    /// The fixed query admits only conservative availability, receipt, and ingestion clocks at or
    /// before the knowledge cutoff. Typed decoding then revalidates every returned row, filters the
    /// canonical `MacroObservation::series` identity, and uses the shared PIT selector per
    /// comparable temporal-coordinate class. Same-family/same-revision semantic divergence fails
    /// closed before any observation crosses this boundary.
    pub async fn read_macro_latest_known_snapshot(
        &self,
        request: AnalyticalMacroLatestKnownRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<AnalyticalMacroLatestKnownOutput, AnalyticalReadError> {
        let (pinned, source_id, _) =
            self.manifests
                .read_exact(request.manifest(), deadline, &cancellation)?;
        if source_id != request.source_id {
            return Err(AnalyticalReadError::MacroSnapshotSourceOwnerMismatch);
        }
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
        let deadline_wait = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
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
        let selected =
            decode_macro_latest_known_snapshot(&output, &request, deadline, &cancellation).await?;
        let selection_digest = macro_latest_known_selection_digest(&request, &output, &selected);
        let observations = selected
            .into_iter()
            .map(|selected| selected.observation)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(AnalyticalMacroLatestKnownOutput {
            source_id,
            output,
            observations,
            selection_digest,
        })
    }

    /// Reads a bounded point-in-time bar series for exactly one stable instrument.
    ///
    /// The fixed query rejects unavailable future knowledge, then typed decoding selects the
    /// latest admitted revision of each exact canonical bar family. Decoding revalidates instrument
    /// identity, conservative availability, exact effective time, requested range, and row count
    /// before any bar crosses this capability boundary.
    pub async fn read_market_bars(
        &self,
        request: AnalyticalMarketBarReadRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<AnalyticalMarketBarOutput, AnalyticalReadError> {
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
        let deadline_wait = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
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
        let bars = decode_market_bars(&output, &request)?;
        Ok(AnalyticalMarketBarOutput {
            source_id,
            output,
            bars,
        })
    }

    /// Reads bounded exact daily NAV history for one resolved fund/share class.
    ///
    /// The query rejects future availability and preserves calendar-date precision. The shared
    /// PIT selector then applies the requested latest-known or all-known revision policy before
    /// any typed NAV crosses this capability boundary.
    pub async fn read_fund_nav_history(
        &self,
        request: AnalyticalFundNavReadRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<AnalyticalFundNavOutput, AnalyticalReadError> {
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
        let deadline_wait = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
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
        let observations =
            decode_fund_nav_history(&output, &request, &source_id, deadline, &cancellation).await?;
        Ok(AnalyticalFundNavOutput {
            source_id,
            output,
            observations,
        })
    }

    /// Selects the uniquely earliest exact-series completed bar at or after a forecast horizon.
    ///
    /// Candidate and execution bounds are code-owned. The caller cannot supply a row limit, price,
    /// or tie-break, and an incomplete, saturated, unavailable, or ambiguous result fails closed.
    pub async fn select_outcome_market_bar(
        &self,
        request: OutcomeMarketBarRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<OutcomeMarketBarSelection, AnalyticalReadError> {
        let (pinned, source_id, _) =
            self.manifests
                .read_exact(request.manifest(), deadline, &cancellation)?;
        if &source_id != request.series().source_id() {
            return Ok(OutcomeMarketBarSelection::Unavailable(
                OutcomeMarketBarUnavailableReason::SourceOwnerMismatch,
            ));
        }
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
        let limits = outcome_market_bar_query_limits()?;
        let execution = engine.query_pinned(query, limits, execution_cancellation);
        tokio::pin!(execution);
        let deadline_wait = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
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
        select_outcome_from_output(request, output)
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
    /// Universe membership must be read without instrument or availability filters.
    #[error("analytical universe-membership read must be exhaustive")]
    UniverseMembershipReadMustBeExhaustive,
    /// A typed market-bar request used zero or exceeded its fixed row ceiling.
    #[error("analytical market-bar limit is invalid")]
    InvalidMarketBarLimit,
    /// A typed market-bar effective-time range was reversed.
    #[error("analytical market-bar effective-time range is invalid")]
    InvalidMarketBarEffectiveRange,
    /// A typed Fund NAV request used zero or exceeded its fixed row ceiling.
    #[error("analytical Fund NAV limit is invalid")]
    InvalidFundNavLimit,
    /// A typed Fund NAV calendar-date range was reversed.
    #[error("analytical Fund NAV date range is invalid")]
    InvalidFundNavDateRange,
    /// A Macro snapshot series set was empty, duplicated, invalid, or above its fixed ceiling.
    #[error("analytical Macro series allowlist is invalid")]
    InvalidMacroSeriesAllowlist,
    /// The exact generation owner did not match the request's sole source namespace.
    #[error("analytical Macro snapshot source owner does not match the request")]
    MacroSnapshotSourceOwnerMismatch,
    /// Typed Macro snapshot decoding requires a bounded inline query result.
    #[error("analytical Macro snapshot result must be inline")]
    MacroSnapshotResultRequiresInline,
    /// The fixed Macro candidate ceiling was saturated, so a complete selection is unknowable.
    #[error("analytical Macro snapshot candidate set is saturated")]
    MacroSnapshotCandidateSetSaturated,
    /// A same-family/same-revision Macro group carried divergent semantic payloads.
    #[error("analytical Macro snapshot contains a revision conflict")]
    MacroSnapshotRevisionConflict,
    /// One or more requested Macro series had no unique latest-known observation.
    #[error("analytical Macro snapshot is incomplete")]
    MacroSnapshotIncomplete,
    /// A returned row violated the typed Macro request or canonical PIT contract.
    #[error("analytical Macro snapshot result is invalid")]
    InvalidMacroSnapshotResult,
    /// An outcome request reversed its inclusive horizon/completion window.
    #[error("analytical outcome market-bar completion window is invalid")]
    InvalidOutcomeMarketBarWindow,
    /// Typed market-bar decoding requires a bounded inline query result.
    #[error("analytical market-bar result must be inline")]
    MarketBarResultRequiresInline,
    /// A returned row violated the typed market-bar request or canonical payload contract.
    #[error("analytical market-bar result is invalid")]
    InvalidMarketBarResult,
    /// Typed Fund NAV decoding requires a bounded inline query result.
    #[error("analytical Fund NAV result must be inline")]
    FundNavResultRequiresInline,
    /// A returned row violated the typed Fund NAV request or canonical PIT contract.
    #[error("analytical Fund NAV result is invalid")]
    InvalidFundNavResult,
    /// Fixed observation templates require the canonical research-observation schema.
    #[error("analytical observation schema is invalid")]
    InvalidObservationSchema,
    /// An exact receipt-admitted forecast dataset was not found.
    #[error("analytical forecast dataset is unavailable")]
    ForecastDatasetUnavailable,
    /// Immutable generation lookup failed.
    #[error("analytical manifest read failed: {0}")]
    Manifest(#[from] ManifestCatalogError),
    /// Fixed-template query construction or execution failed.
    #[error("analytical fixed-template query failed: {0}")]
    Query(#[from] QueryError),
    /// Bounded immutable-object reading failed.
    #[error("analytical forecast object read failed: {0}")]
    Parquet(#[from] crate::ParquetStoreError),
    /// Canonical feature-row verification failed.
    #[error("analytical forecast row verification failed: {0}")]
    PythonDataset(#[from] crate::PythonDatasetCatalogError),
}

struct SelectedMacroObservation {
    observation: MacroObservation,
    effective_date: CalendarDate,
    revision: u32,
    stored_payload_sha256: EvidenceDigest,
    payload_identity: EvidenceDigest,
    provenance_identity: EvidenceDigest,
    evidence_identity: EvidenceDigest,
}

async fn decode_macro_latest_known_snapshot(
    output: &PinnedQueryOutput,
    request: &AnalyticalMacroLatestKnownRequest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<SelectedMacroObservation>, AnalyticalReadError> {
    if output.manifest() != request.manifest() {
        return Err(AnalyticalReadError::InvalidMacroSnapshotResult);
    }
    let crate::QueryResult::Inline { batches, .. } = output.result() else {
        return Err(AnalyticalReadError::MacroSnapshotResultRequiresInline);
    };
    let row_count = batches.iter().try_fold(0_usize, |count, batch| {
        count
            .checked_add(batch.num_rows())
            .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)
    })?;
    if row_count > request.candidate_limit() {
        return Err(AnalyticalReadError::MacroSnapshotCandidateSetSaturated);
    }
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(row_count)
        .map_err(|_| AnalyticalReadError::InvalidMacroSnapshotResult)?;
    let mut stored_payload_sha256 = Vec::new();
    stored_payload_sha256
        .try_reserve_exact(row_count)
        .map_err(|_| AnalyticalReadError::InvalidMacroSnapshotResult)?;
    let mut observed_ties = BTreeMap::<(String, i32, u32), (usize, usize)>::new();
    for batch in batches {
        let series = batch
            .column_by_name("macro_series")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)?;
        let effective_dates = batch
            .column_by_name("effective_date")
            .and_then(|column| column.as_any().downcast_ref::<Date32Array>())
            .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)?;
        let revisions = batch
            .column_by_name("revision")
            .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
            .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)?;
        let payload_digests = batch
            .column_by_name("payload_sha256")
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)?;
        let payloads = batch
            .column_by_name("payload_json")
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)?;
        let tie_counts = batch
            .column_by_name("tie_count")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)?;
        if [
            series.len(),
            effective_dates.len(),
            revisions.len(),
            payload_digests.len(),
            payloads.len(),
            tie_counts.len(),
        ]
        .into_iter()
        .any(|len| len != batch.num_rows())
        {
            return Err(AnalyticalReadError::InvalidMacroSnapshotResult);
        }
        for row in 0..batch.num_rows() {
            if series.is_null(row)
                || effective_dates.is_null(row)
                || revisions.is_null(row)
                || payload_digests.is_null(row)
                || payloads.is_null(row)
                || tie_counts.is_null(row)
                || revisions.value(row) == 0
            {
                return Err(AnalyticalReadError::InvalidMacroSnapshotResult);
            }
            let tie_count = usize::try_from(tie_counts.value(row))
                .map_err(|_| AnalyticalReadError::InvalidMacroSnapshotResult)?;
            if tie_count == 0 || tie_count > MAX_MACRO_SNAPSHOT_TIED_CANDIDATES_PER_SERIES {
                return Err(AnalyticalReadError::MacroSnapshotCandidateSetSaturated);
            }
            let payload = payloads.value(row);
            let payload_digest: [u8; 32] = payload_digests
                .value(row)
                .try_into()
                .map_err(|_| AnalyticalReadError::InvalidMacroSnapshotResult)?;
            let computed_payload_digest: [u8; 32] = Sha256::digest(payload).into();
            if payload_digest != computed_payload_digest {
                return Err(AnalyticalReadError::InvalidMacroSnapshotResult);
            }
            let tie_key = (
                series.value(row).to_owned(),
                effective_dates.value(row),
                revisions.value(row),
            );
            let entry = observed_ties.entry(tie_key).or_insert((tie_count, 0));
            if entry.0 != tie_count {
                return Err(AnalyticalReadError::InvalidMacroSnapshotResult);
            }
            entry.1 = entry
                .1
                .checked_add(1)
                .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)?;
            let observation = serde_json::from_slice::<ResearchObservation>(payload)
                .map_err(|_| AnalyticalReadError::InvalidMacroSnapshotResult)?;
            let ResearchObservation::Macro(macro_observation) = &observation else {
                return Err(AnalyticalReadError::InvalidMacroSnapshotResult);
            };
            let context = macro_observation.context();
            let provenance = context.provenance();
            let effective_date = context
                .time()
                .effective()
                .calendar_date_value()
                .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)?;
            let available_at = provenance
                .availability()
                .conservative_available_at()
                .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)?;
            if provenance.source_id() != &request.source_id
                || series.value(row) != macro_observation.series().as_str()
                || !request
                    .series_allowlist
                    .contains(macro_observation.series())
                || effective_dates.value(row) != effective_date.days_since_unix_epoch()
                || effective_date > request.effective_date_cutoff
                || revisions.value(row) != context.time().revision().get()
                || available_at > request.knowledge_cutoff
                || provenance.received_at() > request.knowledge_cutoff
                || provenance.ingested_at() > request.knowledge_cutoff
                || context.time().published().is_some_and(|published| {
                    published
                        .exact_timestamp()
                        .is_some_and(|published| published > request.knowledge_cutoff)
                })
            {
                return Err(AnalyticalReadError::InvalidMacroSnapshotResult);
            }
            candidates.push(PointInTimeCandidate::new(
                observation,
                request.manifest.clone(),
            ));
            stored_payload_sha256
                .push(EvidenceDigest::new(DigestAlgorithm::Sha256, payload_digest));
        }
    }
    if observed_ties
        .iter()
        .any(|(_, (declared, observed))| declared != observed)
    {
        return Err(AnalyticalReadError::InvalidMacroSnapshotResult);
    }

    let policy = PointInTimePolicy::try_new(NonZeroU32::MIN, PointInTimeRevisionMode::LatestKnown)
        .map_err(|_| AnalyticalReadError::InvalidMacroSnapshotResult)?;
    let pit_limits = PointInTimeLimits::try_new(
        request.candidate_limit(),
        request.series_allowlist.series().len(),
        request.candidate_limit(),
        request.series_allowlist.series().len(),
        32 * 1024 * 1024,
    )
    .map_err(|_| AnalyticalReadError::InvalidMacroSnapshotResult)?;
    let pit_request = PointInTimeRequest::try_new(
        policy,
        request.knowledge_cutoff,
        None,
        ResearchTemporalCoordinate::calendar_date(request.effective_date_cutoff),
        None,
        pit_limits,
    )
    .map_err(|_| AnalyticalReadError::InvalidMacroSnapshotResult)?;
    let selection = match PointInTimeService::new()
        .select(&pit_request, &candidates, cancellation, deadline)
        .await
    {
        Ok(selection) => selection,
        Err(crate::PointInTimeError::RevisionConflicts { .. }) => {
            return Err(AnalyticalReadError::MacroSnapshotRevisionConflict);
        }
        Err(crate::PointInTimeError::Cancelled) => {
            return Err(AnalyticalReadError::Query(QueryError::Cancelled));
        }
        Err(crate::PointInTimeError::DeadlineExceeded) => {
            return Err(AnalyticalReadError::Query(QueryError::DeadlineExceeded));
        }
        Err(_) => return Err(AnalyticalReadError::InvalidMacroSnapshotResult),
    };
    if selection.records().len() != request.series_allowlist.series().len() {
        return Err(AnalyticalReadError::MacroSnapshotIncomplete);
    }
    let mut selected = Vec::new();
    selected
        .try_reserve_exact(selection.records().len())
        .map_err(|_| AnalyticalReadError::InvalidMacroSnapshotResult)?;
    for record in selection.records() {
        let candidate_index = candidates
            .iter()
            .position(|candidate| std::ptr::eq(candidate, record.candidate()))
            .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)?;
        let ResearchObservation::Macro(macro_observation) = record.candidate().observation() else {
            return Err(AnalyticalReadError::InvalidMacroSnapshotResult);
        };
        let effective_date = macro_observation
            .context()
            .time()
            .effective()
            .calendar_date_value()
            .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)?;
        selected.push(SelectedMacroObservation {
            observation: macro_observation.clone(),
            effective_date,
            revision: macro_observation.context().time().revision().get(),
            stored_payload_sha256: *stored_payload_sha256
                .get(candidate_index)
                .ok_or(AnalyticalReadError::InvalidMacroSnapshotResult)?,
            payload_identity: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                record.payload_identity().bytes(),
            ),
            provenance_identity: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                record.provenance_identity().bytes(),
            ),
            evidence_identity: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                record.evidence_identity().bytes(),
            ),
        });
    }
    selected
        .sort_unstable_by(|left, right| left.observation.series().cmp(right.observation.series()));
    if !selected
        .iter()
        .map(|selected| selected.observation.series())
        .eq(request.series_allowlist.series().iter())
    {
        return Err(AnalyticalReadError::MacroSnapshotIncomplete);
    }
    Ok(selected)
}

fn macro_latest_known_selection_digest(
    request: &AnalyticalMacroLatestKnownRequest,
    output: &PinnedQueryOutput,
    selected: &[SelectedMacroObservation],
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/analytical-macro-latest-known-selection/v1");
    hash_manifest(&mut hash, request.manifest());
    hash_evidence(&mut hash, output.object_graph_digest());
    hash_evidence(&mut hash, output.query_identity());
    hash_evidence(&mut hash, output.result_digest());
    hash_str(&mut hash, request.source_id.as_str());
    hash_timestamp(&mut hash, request.knowledge_cutoff);
    hash.update(request.effective_date_cutoff.year().to_be_bytes());
    hash.update([request.effective_date_cutoff.month()]);
    hash.update([request.effective_date_cutoff.day()]);
    hash.update(b"point-in-time-policy/v1:latest-known;calendar-latest-effective/v1");
    hash.update(
        u64::try_from(request.series_allowlist.series().len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for series in request.series_allowlist.series() {
        hash_str(&mut hash, series.as_str());
    }
    hash.update(
        u64::try_from(selected.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for selected in selected {
        hash_str(&mut hash, selected.observation.series().as_str());
        hash.update(selected.effective_date.year().to_be_bytes());
        hash.update([selected.effective_date.month()]);
        hash.update([selected.effective_date.day()]);
        hash.update(selected.revision.to_be_bytes());
        hash_evidence(&mut hash, selected.stored_payload_sha256);
        hash_evidence(&mut hash, selected.payload_identity);
        hash_evidence(&mut hash, selected.provenance_identity);
        hash_evidence(&mut hash, selected.evidence_identity);
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

async fn decode_fund_nav_history(
    output: &PinnedQueryOutput,
    request: &AnalyticalFundNavReadRequest,
    source_id: &SourceId,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Box<[FundNavObservation]>, AnalyticalReadError> {
    let crate::QueryResult::Inline { batches, .. } = output.result() else {
        return Err(AnalyticalReadError::FundNavResultRequiresInline);
    };
    let row_count = batches.iter().try_fold(0_usize, |count, batch| {
        count
            .checked_add(batch.num_rows())
            .ok_or(AnalyticalReadError::InvalidFundNavResult)
    })?;
    if row_count > MAX_FUND_NAV_REVISION_CANDIDATES {
        return Err(AnalyticalReadError::InvalidFundNavResult);
    }
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(row_count)
        .map_err(|_| AnalyticalReadError::InvalidFundNavResult)?;
    for batch in batches {
        let payloads = batch
            .column_by_name("payload_json")
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .ok_or(AnalyticalReadError::InvalidFundNavResult)?;
        if payloads.len() != batch.num_rows() {
            return Err(AnalyticalReadError::InvalidFundNavResult);
        }
        for row in 0..payloads.len() {
            if payloads.is_null(row) {
                return Err(AnalyticalReadError::InvalidFundNavResult);
            }
            let observation = serde_json::from_slice::<ResearchObservation>(payloads.value(row))
                .map_err(|_| AnalyticalReadError::InvalidFundNavResult)?;
            let ResearchObservation::FundNav(nav) = &observation else {
                return Err(AnalyticalReadError::InvalidFundNavResult);
            };
            let provenance = nav.context().provenance();
            let available_at = provenance
                .availability()
                .conservative_available_at()
                .ok_or(AnalyticalReadError::InvalidFundNavResult)?;
            if provenance.instrument_id() != Some(request.instrument_id)
                || provenance.source_id() != source_id
                || available_at > request.knowledge_cutoff
                || request
                    .date_range
                    .is_some_and(|range| nav.nav_date() < range.start || nav.nav_date() > range.end)
            {
                return Err(AnalyticalReadError::InvalidFundNavResult);
            }
            // Local PIT history is conservative over every locally observed/published clock, not
            // only the provider's source-availability coordinate.
            if provenance.received_at() > request.knowledge_cutoff
                || provenance.ingested_at() > request.knowledge_cutoff
                || nav.canonical_published_at() > request.knowledge_cutoff
            {
                continue;
            }
            candidates.push(PointInTimeCandidate::new(
                observation,
                request.manifest.clone(),
            ));
        }
    }
    let effective_end = match request.date_range {
        Some(range) => range.end,
        None => CalendarDate::new(9999, 12, 31)
            .map_err(|_| AnalyticalReadError::InvalidFundNavResult)?,
    };
    let policy = PointInTimePolicy::try_new(NonZeroU32::MIN, request.revision_mode)
        .map_err(|_| AnalyticalReadError::InvalidFundNavResult)?;
    let pit_limits = PointInTimeLimits::try_new(
        MAX_FUND_NAV_REVISION_CANDIDATES,
        MAX_FUND_NAV_REVISION_CANDIDATES,
        MAX_FUND_NAV_REVISION_CANDIDATES,
        MAX_FUND_NAV_REVISION_CANDIDATES,
        256 * 1024 * 1024,
    )
    .map_err(|_| AnalyticalReadError::InvalidFundNavResult)?;
    let pit_request = PointInTimeRequest::try_new(
        policy,
        request.knowledge_cutoff,
        None,
        ResearchTemporalCoordinate::calendar_date(effective_end),
        None,
        pit_limits,
    )
    .map_err(|_| AnalyticalReadError::InvalidFundNavResult)?;
    let selection = PointInTimeService::new()
        .select(&pit_request, &candidates, cancellation, deadline)
        .await
        .map_err(|_| AnalyticalReadError::InvalidFundNavResult)?;
    let result_limit = usize::try_from(request.limit.get())
        .map_err(|_| AnalyticalReadError::InvalidFundNavResult)?;
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(selection.records().len().min(result_limit))
        .map_err(|_| AnalyticalReadError::InvalidFundNavResult)?;
    for record in selection.records().iter().take(result_limit) {
        let ResearchObservation::FundNav(nav) = record.candidate().observation() else {
            return Err(AnalyticalReadError::InvalidFundNavResult);
        };
        observations.push(nav.clone());
    }
    Ok(observations.into_boxed_slice())
}

fn decode_market_bars(
    output: &PinnedQueryOutput,
    request: &AnalyticalMarketBarReadRequest,
) -> Result<Box<[MarketBarObservation]>, AnalyticalReadError> {
    if inline_market_bar_row_count(output)? > MAX_MARKET_BAR_REVISION_CANDIDATES {
        return Err(AnalyticalReadError::InvalidMarketBarResult);
    }
    let mut candidates = decode_market_bar_candidates(output)?;
    for candidate in &candidates {
        let provenance = candidate.bar.context().provenance();
        if provenance.instrument_id() != Some(request.instrument_id)
            || candidate.available_at > request.knowledge_cutoff
            || request.effective_range.is_some_and(|range| {
                candidate.effective_at < range.start || candidate.effective_at > range.end
            })
        {
            return Err(AnalyticalReadError::InvalidMarketBarResult);
        }
    }
    candidates = latest_market_bar_revisions(candidates)?;
    candidates.sort_unstable_by(|left, right| {
        left.effective_at
            .cmp(&right.effective_at)
            .then_with(|| left.family.exact_bytes().cmp(right.family.exact_bytes()))
            .then_with(|| left.ordinal.cmp(&right.ordinal))
    });
    let result_limit = usize::try_from(request.limit.get())
        .map_err(|_| AnalyticalReadError::InvalidMarketBarResult)?;
    candidates.truncate(result_limit);
    let mut bars = Vec::new();
    bars.try_reserve_exact(candidates.len())
        .map_err(|_| AnalyticalReadError::InvalidMarketBarResult)?;
    bars.extend(candidates.into_iter().map(|candidate| candidate.bar));
    Ok(bars.into_boxed_slice())
}

struct DecodedMarketBarCandidate {
    ordinal: u32,
    payload_digest: EvidenceDigest,
    family: CanonicalObservationFamily,
    effective_at: Timestamp,
    available_at: Timestamp,
    bar: MarketBarObservation,
}

struct DecodedOutcomeMarketBar {
    ordinal: u32,
    payload_digest: EvidenceDigest,
    bar: MarketBarObservation,
}

fn inline_market_bar_row_count(output: &PinnedQueryOutput) -> Result<usize, AnalyticalReadError> {
    let crate::QueryResult::Inline { batches, .. } = output.result() else {
        return Err(AnalyticalReadError::MarketBarResultRequiresInline);
    };
    batches.iter().try_fold(0_usize, |count, batch| {
        count
            .checked_add(batch.num_rows())
            .ok_or(AnalyticalReadError::InvalidMarketBarResult)
    })
}

fn decode_market_bar_candidates(
    output: &PinnedQueryOutput,
) -> Result<Vec<DecodedMarketBarCandidate>, AnalyticalReadError> {
    let crate::QueryResult::Inline { batches, .. } = output.result() else {
        return Err(AnalyticalReadError::MarketBarResultRequiresInline);
    };
    let row_count = inline_market_bar_row_count(output)?;
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(row_count)
        .map_err(|_| AnalyticalReadError::InvalidMarketBarResult)?;
    let mut ordinal = 0_usize;
    for batch in batches {
        let payloads = batch
            .column_by_name("payload_json")
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .ok_or(AnalyticalReadError::InvalidMarketBarResult)?;
        if payloads.len() != batch.num_rows() {
            return Err(AnalyticalReadError::InvalidMarketBarResult);
        }
        for row in 0..payloads.len() {
            if payloads.is_null(row) {
                return Err(AnalyticalReadError::InvalidMarketBarResult);
            }
            let payload = payloads.value(row);
            let observation = serde_json::from_slice::<ResearchObservation>(payload)
                .map_err(|_| AnalyticalReadError::InvalidMarketBarResult)?;
            let family = CanonicalObservationFamily::try_from_observation(&observation)
                .map_err(|_| AnalyticalReadError::InvalidMarketBarResult)?;
            let ResearchObservation::MarketBar(bar) = observation else {
                return Err(AnalyticalReadError::InvalidMarketBarResult);
            };
            let context = bar.context();
            let effective_at = context
                .time()
                .effective()
                .exact_timestamp()
                .ok_or(AnalyticalReadError::InvalidMarketBarResult)?;
            let available_at = context
                .provenance()
                .availability()
                .conservative_available_at()
                .ok_or(AnalyticalReadError::InvalidMarketBarResult)?;
            candidates.push(DecodedMarketBarCandidate {
                ordinal: u32::try_from(ordinal)
                    .map_err(|_| AnalyticalReadError::InvalidMarketBarResult)?,
                payload_digest: EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    Sha256::digest(payload).into(),
                ),
                family,
                effective_at,
                available_at,
                bar,
            });
            ordinal = ordinal
                .checked_add(1)
                .ok_or(AnalyticalReadError::InvalidMarketBarResult)?;
        }
    }
    Ok(candidates)
}

fn latest_market_bar_revisions(
    mut candidates: Vec<DecodedMarketBarCandidate>,
) -> Result<Vec<DecodedMarketBarCandidate>, AnalyticalReadError> {
    candidates.sort_unstable_by(|left, right| {
        left.family
            .exact_bytes()
            .cmp(right.family.exact_bytes())
            .then_with(|| {
                right
                    .bar
                    .context()
                    .time()
                    .revision()
                    .get()
                    .cmp(&left.bar.context().time().revision().get())
            })
            .then_with(|| {
                left.payload_digest
                    .bytes()
                    .cmp(&right.payload_digest.bytes())
            })
            .then_with(|| left.ordinal.cmp(&right.ordinal))
    });
    let mut latest: Vec<DecodedMarketBarCandidate> = Vec::new();
    latest
        .try_reserve_exact(candidates.len())
        .map_err(|_| AnalyticalReadError::InvalidMarketBarResult)?;
    let mut current_family_start: Option<usize> = None;
    for candidate in candidates {
        let same_family = current_family_start
            .and_then(|index| latest.get(index))
            .is_some_and(|current| current.family.exact_bytes() == candidate.family.exact_bytes());
        if !same_family {
            current_family_start = Some(latest.len());
            latest.push(candidate);
            continue;
        }
        let family_start = current_family_start
            .and_then(|index| latest.get(index))
            .ok_or(AnalyticalReadError::InvalidMarketBarResult)?;
        if family_start.bar.context().time().revision() == candidate.bar.context().time().revision()
        {
            latest.push(candidate);
        }
    }
    Ok(latest)
}

fn outcome_market_bar_query_limits() -> Result<QueryLimits, AnalyticalReadError> {
    QueryLimits::try_new_with_inline_bytes(
        u64::try_from(MAX_OUTCOME_MARKET_BAR_CANDIDATES + 1)
            .map_err(|_| AnalyticalReadError::InvalidMarketBarResult)?,
        OUTCOME_MARKET_BAR_QUERY_BYTES,
        OUTCOME_MARKET_BAR_QUERY_BYTES,
        OUTCOME_MARKET_BAR_QUERY_MEMORY_BYTES,
        1,
        256,
        256,
        Duration::from_secs(30),
    )
    .map_err(Into::into)
}

fn select_outcome_from_output(
    request: OutcomeMarketBarRequest,
    output: PinnedQueryOutput,
) -> Result<OutcomeMarketBarSelection, AnalyticalReadError> {
    if inline_market_bar_row_count(&output)? > MAX_OUTCOME_MARKET_BAR_CANDIDATES {
        return Ok(OutcomeMarketBarSelection::Unavailable(
            OutcomeMarketBarUnavailableReason::CandidateSetSaturated,
        ));
    }
    let candidates = match decode_market_bar_candidates(&output) {
        Ok(candidates) => candidates,
        Err(AnalyticalReadError::InvalidMarketBarResult) => {
            return Ok(OutcomeMarketBarSelection::Unavailable(
                OutcomeMarketBarUnavailableReason::IncompleteCandidate,
            ));
        }
        Err(error) => return Err(error),
    };
    for candidate in &candidates {
        let provenance = candidate.bar.context().provenance();
        let effective_before_horizon = request.series.timestamp_basis
            == BarTimestampBasis::PeriodEnd
            && candidate.effective_at < request.horizon;
        if provenance.instrument_id() != Some(request.series.instrument_id)
            || provenance.source_id() != &request.series.source_id
            || provenance.venue_id() != Some(&request.series.venue_id)
            || candidate.available_at > request.knowledge_cutoff
            || candidate.effective_at > request.latest_eligible_completion
            || effective_before_horizon
        {
            return Ok(OutcomeMarketBarSelection::Unavailable(
                OutcomeMarketBarUnavailableReason::IncompleteCandidate,
            ));
        }
    }
    let candidates = match latest_market_bar_revisions(candidates) {
        Ok(candidates) => candidates,
        Err(AnalyticalReadError::InvalidMarketBarResult) => {
            return Ok(OutcomeMarketBarSelection::Unavailable(
                OutcomeMarketBarUnavailableReason::IncompleteCandidate,
            ));
        }
        Err(error) => return Err(error),
    };

    let mut selected: Option<DecodedOutcomeMarketBar> = None;
    let mut ambiguous = false;
    for candidate in candidates {
        if !outcome_series_matches(&candidate.bar, &request.series) {
            continue;
        }
        let completed_at = candidate.bar.completed_at();
        if completed_at < request.horizon || completed_at > request.latest_eligible_completion {
            continue;
        }
        let candidate = DecodedOutcomeMarketBar {
            ordinal: candidate.ordinal,
            payload_digest: candidate.payload_digest,
            bar: candidate.bar,
        };
        match selected.as_ref() {
            None => {
                selected = Some(candidate);
                ambiguous = false;
            }
            Some(current) if candidate.bar.completed_at() < current.bar.completed_at() => {
                selected = Some(candidate);
                ambiguous = false;
            }
            Some(current) if candidate.bar.completed_at() == current.bar.completed_at() => {
                ambiguous = true;
            }
            Some(_) => {}
        }
    }
    if ambiguous {
        return Ok(OutcomeMarketBarSelection::Unavailable(
            OutcomeMarketBarUnavailableReason::AmbiguousCompletion,
        ));
    }
    let Some(selected) = selected else {
        return Ok(OutcomeMarketBarSelection::Unavailable(
            OutcomeMarketBarUnavailableReason::NoEligibleBar,
        ));
    };
    let request_digest = outcome_market_bar_request_digest(&request);
    let receipt_digest = outcome_market_bar_receipt_digest(
        request_digest,
        &output,
        selected.ordinal,
        selected.payload_digest,
        &selected.bar,
    )?;
    Ok(OutcomeMarketBarSelection::Selected(
        OutcomeMarketBarSelectedReceipt {
            request,
            request_digest,
            output,
            ordinal: selected.ordinal,
            payload_digest: selected.payload_digest,
            receipt_digest,
            bar: selected.bar,
        },
    ))
}

fn outcome_series_matches(bar: &MarketBarObservation, series: &OutcomeMarketBarSeries) -> bool {
    bar.provider_instrument_id() == &series.provider_instrument_id
        && bar.feed() == &series.feed
        && bar.interval() == &series.interval
        && bar.adjustment() == series.adjustment
        && bar.time_semantics().timestamp_basis() == series.timestamp_basis
        && bar.time_semantics().session() == &series.session
}

fn outcome_market_bar_request_digest(request: &OutcomeMarketBarRequest) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/outcome-market-bar-request/v1");
    hash_manifest(&mut hash, request.manifest());
    let series = request.series();
    hash.update(series.instrument_id().as_uuid().as_bytes());
    hash_str(&mut hash, series.source_id().as_str());
    hash_str(&mut hash, series.venue_id().as_str());
    hash_str(&mut hash, series.provider_instrument_id().as_str());
    hash_str(&mut hash, series.feed().as_str());
    hash_str(&mut hash, series.interval().as_str());
    hash.update([market_bar_adjustment_digest_tag(series.adjustment())]);
    hash.update([bar_timestamp_basis_digest_tag(series.timestamp_basis())]);
    hash_market_bar_session(&mut hash, series.session());
    hash_timestamp(&mut hash, request.knowledge_cutoff());
    hash_timestamp(&mut hash, request.horizon());
    hash_timestamp(&mut hash, request.latest_eligible_completion());
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn outcome_market_bar_receipt_digest(
    request_digest: EvidenceDigest,
    output: &PinnedQueryOutput,
    ordinal: u32,
    payload_digest: EvidenceDigest,
    bar: &MarketBarObservation,
) -> Result<EvidenceDigest, AnalyticalReadError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/outcome-market-bar-selected/v1");
    hash_evidence(&mut hash, request_digest);
    hash_manifest(&mut hash, output.manifest());
    hash_evidence(&mut hash, output.object_graph_digest());
    hash_evidence(&mut hash, output.query_identity());
    hash_evidence(&mut hash, output.result_digest());
    hash.update(ordinal.to_be_bytes());
    hash_evidence(&mut hash, payload_digest);
    let context = bar.context();
    let provenance = context.provenance();
    hash_str(&mut hash, provenance.source_id().as_str());
    hash_str(&mut hash, provenance.source_identifier().as_str());
    hash_timestamp(&mut hash, bar.time_semantics().period_start());
    hash_timestamp(&mut hash, bar.completed_at());
    hash_timestamp(&mut hash, bar.time_semantics().provider_timestamp());
    let available_at = provenance
        .availability()
        .conservative_available_at()
        .ok_or(AnalyticalReadError::InvalidMarketBarResult)?;
    hash_timestamp(&mut hash, available_at);
    hash.update([bar_timestamp_basis_digest_tag(
        bar.time_semantics().timestamp_basis(),
    )]);
    hash_market_bar_session(&mut hash, bar.time_semantics().session());
    let close = bar.close().amount().normalize();
    hash.update(close.mantissa().to_be_bytes());
    hash.update(close.scale().to_be_bytes());
    hash_str(&mut hash, bar.currency().as_str());
    hash.update([data_quality_digest_tag(provenance.quality())]);
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn hash_market_bar_session(hash: &mut Sha256, session: &MarketBarSessionEvidence) {
    hash.update([match session.kind() {
        market_squawk_domain::MarketBarSessionKind::Regular => 1,
        market_squawk_domain::MarketBarSessionKind::Extended => 2,
        market_squawk_domain::MarketBarSessionKind::Continuous => 3,
        market_squawk_domain::MarketBarSessionKind::ProviderDefined => 4,
    }]);
    hash_str(hash, session.ruleset().as_str());
    hash_evidence(hash, session.evidence());
}

fn hash_evidence(hash: &mut Sha256, evidence: EvidenceDigest) {
    hash.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hash.update(evidence.bytes());
}

fn hash_manifest(hash: &mut Sha256, manifest: &DatasetManifestRef) {
    hash_str(hash, manifest.dataset_id().as_str());
    hash.update(manifest.manifest_version().to_be_bytes());
    hash_str(hash, manifest.schema().name());
    hash.update(manifest.schema_version().get().to_be_bytes());
    hash.update(manifest.schema().fingerprint());
    hash.update(manifest.content_hash().bytes());
}

fn hash_str(hash: &mut Sha256, value: &str) {
    hash.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(value.as_bytes());
}

fn hash_timestamp(hash: &mut Sha256, timestamp: Timestamp) {
    hash.update(timestamp.unix_nanos().to_be_bytes());
}

const fn market_bar_adjustment_digest_tag(adjustment: MarketBarAdjustment) -> u8 {
    match adjustment {
        MarketBarAdjustment::Raw => 1,
        MarketBarAdjustment::Split => 2,
        MarketBarAdjustment::Dividend => 3,
        MarketBarAdjustment::SpinOff => 4,
        MarketBarAdjustment::All => 5,
    }
}

const fn bar_timestamp_basis_digest_tag(basis: BarTimestampBasis) -> u8 {
    match basis {
        BarTimestampBasis::PeriodStart => 1,
        BarTimestampBasis::PeriodEnd => 2,
    }
}

const fn data_quality_digest_tag(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}
