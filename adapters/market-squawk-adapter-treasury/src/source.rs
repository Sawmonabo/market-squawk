use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_platform::{RawCaptureRecord, SealedResearchJournalStore};
use market_squawk_sources::{
    AuthorizationMode, CoverageDomain, DiscoveryBatch, DiscoveryRequest, ExtractionAuthority,
    ExtractionBatch, ExtractionBatchAccumulator, ExtractionRecord, ExtractionRequest,
    ExtractionRevisionPlan, ExtractionSource, ExtractionSourceError, HistoricalCapability,
    ProviderCaptureMaterial, ProviderCaptureMaterialSealError, ProviderCapturePageReceipt,
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition, ProviderNativeLineageBatch,
    SourceClass, SourceError, SourceMetadata, SourceMetadataProvider, SourceProtocolProfile,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::client::{JSON_MEDIA_TYPE, TreasuryHttpClient, XML_MEDIA_TYPE, system_timestamp};
use crate::{
    FiscalDataPage, FiscalDataParseLimits, TreasuryDailyRateFamily, TreasuryDailyRatePage,
    TreasuryDailyRatePageRequest, TreasuryDailyRateQuery, TreasuryFiscalQuery, TreasuryPageRequest,
    TreasuryProtocolError,
};

mod backfill;
pub(crate) mod lineage;
mod native_lineage;
pub(crate) mod normalize;

pub use backfill::{
    TreasuryAllHistoryAcquisitionCompletion, TreasuryAllHistoryBackfill,
    TreasuryAllHistoryCanonicalPage, TreasuryAllHistoryCheckpoint, TreasuryAllHistoryFetchedPage,
    TreasuryAllHistoryPageAdmission,
};

use crate::vertical::{
    TreasuryDiscoveryAccounting, TreasuryDiscoveryAccountingInput, TreasuryDiscoveryOutput,
    TreasuryExtractionAccounting, TreasuryExtractionAccountingInput,
};
use lineage::{
    FiscalChainFraming, ObjectKind, ParsedObjectId, fiscal_chain_source_object, invalid_protocol,
    lower_hex, source_object, verify_refetched_fiscal_chain, verify_refetched_object,
};
use native_lineage::TreasuryNativeLineagePlan;
use normalize::{
    CanonicalRecordAdmission, CanonicalTreasuryRecord, canonical_daily_rate_records,
    canonical_fiscal_records,
};

const MAX_DAILY_RATE_QUERIES: usize = 1_024;
const MAX_DAILY_RATE_PAGES: usize = 1_024;
const MAX_ALL_HISTORY_RESTORE_WORKERS: usize = 1;

/// A bounded, immutable set of official Treasury daily-rate datasets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreasuryDailyRatesConfig {
    queries: Vec<TreasuryDailyRateQuery>,
}

impl TreasuryDailyRatesConfig {
    /// Binds one source generation to an exact, non-empty set of daily-rate queries.
    ///
    /// # Errors
    ///
    /// Rejects an empty set, duplicate dataset identities, or more than 1,024 queries.
    pub fn try_new(
        queries: impl IntoIterator<Item = TreasuryDailyRateQuery>,
    ) -> Result<Self, TreasuryProtocolError> {
        let mut accepted = Vec::new();
        for query in queries {
            if accepted.len() == MAX_DAILY_RATE_QUERIES
                || accepted
                    .iter()
                    .any(|existing: &TreasuryDailyRateQuery| existing.dataset() == query.dataset())
            {
                return Err(TreasuryProtocolError::InvalidQuery);
            }
            accepted
                .try_reserve(1)
                .map_err(|_| TreasuryProtocolError::InvalidQuery)?;
            accepted.push(query);
        }
        if accepted.is_empty() {
            return Err(TreasuryProtocolError::InvalidQuery);
        }
        Ok(Self { queries: accepted })
    }

    /// Builds complete yearly coverage for all five official daily-rate families.
    ///
    /// Each family starts at the later of the requested year and Treasury's documented first
    /// available year. The end year must include at least one year from every required family.
    ///
    /// # Errors
    ///
    /// Rejects reversed, unsupported, incomplete, or excessively large ranges.
    pub fn all_families(start_year: u16, end_year: u16) -> Result<Self, TreasuryProtocolError> {
        let latest_family_start = TreasuryDailyRateFamily::ALL
            .into_iter()
            .map(TreasuryDailyRateFamily::start_year)
            .max()
            .ok_or(TreasuryProtocolError::InvalidQuery)?;
        if start_year > end_year || end_year < latest_family_start {
            return Err(TreasuryProtocolError::InvalidQuery);
        }
        let query_count = TreasuryDailyRateFamily::ALL
            .into_iter()
            .map(|family| {
                let first_year = start_year.max(family.start_year());
                usize::from(end_year - first_year) + 1
            })
            .sum::<usize>();
        if query_count > MAX_DAILY_RATE_QUERIES {
            return Err(TreasuryProtocolError::InvalidQuery);
        }
        let mut queries = Vec::new();
        queries
            .try_reserve_exact(query_count)
            .map_err(|_| TreasuryProtocolError::InvalidQuery)?;
        for family in TreasuryDailyRateFamily::ALL {
            let first_year = start_year.max(family.start_year());
            for year in first_year..=end_year {
                queries.push(TreasuryDailyRateQuery::year(family, year)?);
            }
        }
        Self::try_new(queries)
    }

    /// Builds one explicit resumable all-history query for every official daily-rate family.
    ///
    /// # Errors
    ///
    /// Fails closed if any official family request cannot be represented by the exact query
    /// grammar or the duplicate-free configuration bound.
    pub fn all_history_all_families() -> Result<Self, TreasuryProtocolError> {
        let queries = TreasuryDailyRateFamily::ALL
            .into_iter()
            .map(TreasuryDailyRateQuery::all_history)
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new(queries)
    }

    /// Returns all exact configured queries in stable family/range order.
    pub fn queries(&self) -> &[TreasuryDailyRateQuery] {
        &self.queries
    }

    fn query(&self, dataset: &SourceIdentifier) -> Option<&TreasuryDailyRateQuery> {
        self.queries.iter().find(|query| query.dataset() == dataset)
    }
}

/// One exact provider profile authorized for a Treasury source instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TreasurySourceConfig {
    /// One exact Fiscal Data average-interest-rates query family.
    AverageInterestRates(TreasuryFiscalQuery),
    /// One bounded set of exact official daily-rate query families.
    DailyRates(TreasuryDailyRatesConfig),
}

impl TreasurySourceConfig {
    /// Creates a source for one exact Fiscal Data query family.
    pub const fn average_interest_rates(query: TreasuryFiscalQuery) -> Self {
        Self::AverageInterestRates(query)
    }

    /// Creates a source for one official daily par-yield-curve year.
    ///
    /// # Errors
    ///
    /// Rejects a year outside the provider's supported nominal-curve range.
    pub fn daily_par_yield_curve(year: u16) -> Result<Self, TreasuryProtocolError> {
        let query =
            TreasuryDailyRateQuery::year(TreasuryDailyRateFamily::NominalParYieldCurve, year)?;
        Ok(Self::DailyRates(TreasuryDailyRatesConfig::try_new([
            query,
        ])?))
    }

    /// Creates a source for an exact set of official daily-rate queries.
    pub const fn daily_rates(config: TreasuryDailyRatesConfig) -> Self {
        Self::DailyRates(config)
    }

    /// Creates complete yearly coverage for all five official daily-rate families.
    ///
    /// # Errors
    ///
    /// Rejects a range that cannot include every family or exceeds bounded configuration limits.
    pub fn daily_rates_all_families(
        start_year: u16,
        end_year: u16,
    ) -> Result<Self, TreasuryProtocolError> {
        TreasuryDailyRatesConfig::all_families(start_year, end_year).map(Self::DailyRates)
    }

    /// Creates explicit resumable all-history acquisition for all five daily-rate families.
    pub fn daily_rates_all_history() -> Result<Self, TreasuryProtocolError> {
        TreasuryDailyRatesConfig::all_history_all_families().map(Self::DailyRates)
    }

    /// Returns every exact provider and analytical dataset carried by this configuration.
    ///
    /// The catalog is the activation intent used by provider-local doctor, acquisition, and
    /// dashboard-read gates. A multi-year daily-rate source therefore exposes every year/family
    /// selector instead of pretending it has one representative dataset.
    pub fn dataset_catalog(
        &self,
    ) -> Result<crate::TreasuryDatasetCatalog, crate::TreasuryVerticalError> {
        crate::TreasuryDatasetCatalog::try_from_config(self)
    }

    /// Binds the exact configured datasets into one provider-local activation identity.
    pub fn activation_intent(
        &self,
    ) -> Result<crate::TreasuryActivationIntent, crate::TreasuryVerticalError> {
        crate::TreasuryActivationIntent::try_new(self)
    }

    /// Builds one bounded representative doctor request per configured Treasury family.
    pub fn doctor_plan(&self) -> Result<crate::TreasuryDoctorPlan, crate::TreasuryVerticalError> {
        self.activation_intent()?.doctor_plan(self)
    }

    /// Returns the exact quality ceiling required by this profile.
    pub const fn quality(&self) -> DataQuality {
        match self {
            Self::AverageInterestRates(_) => DataQuality::OfficialDelayed,
            Self::DailyRates(_) => DataQuality::OfficialDelayed,
        }
    }

    fn authorization_probe_urls(&self) -> Result<Vec<String>, TreasuryProtocolError> {
        match self {
            Self::AverageInterestRates(query) => Ok(vec![query.page(1)?.url().to_owned()]),
            Self::DailyRates(config) => config
                .queries()
                .iter()
                .map(|query| query.page(0).map(|page| page.url().to_owned()))
                .collect(),
        }
    }

    fn query(&self, dataset: &SourceIdentifier) -> Option<&TreasuryDailyRateQuery> {
        match self {
            Self::AverageInterestRates(_) => None,
            Self::DailyRates(config) => config.query(dataset),
        }
    }

    fn accepts_dataset(&self, dataset: &SourceIdentifier) -> Result<bool, TreasurySourceError> {
        match self {
            Self::AverageInterestRates(query) => Ok(dataset == &fiscal_provider_dataset(query)?),
            Self::DailyRates(config) => Ok(config.query(dataset).is_some()),
        }
    }

    fn analytical_dataset(
        &self,
        provider_dataset: &SourceIdentifier,
    ) -> Result<SourceIdentifier, TreasurySourceError> {
        match self {
            Self::AverageInterestRates(query) => {
                if provider_dataset != &fiscal_provider_dataset(query)? {
                    return Err(TreasurySourceError::QueryBindingMismatch);
                }
                fiscal_analytical_dataset(query)
            }
            Self::DailyRates(config) => config
                .query(provider_dataset)
                .map(|query| query.analytical_dataset().clone())
                .ok_or(TreasurySourceError::QueryBindingMismatch),
        }
    }
}

pub(crate) fn fiscal_provider_dataset(
    query: &TreasuryFiscalQuery,
) -> Result<SourceIdentifier, TreasurySourceError> {
    SourceIdentifier::try_from(format!(
        "treasury:fiscal-data:average-interest-rates-v2:{}",
        lower_hex(query.query_digest())
    ))
    .map_err(|_| TreasurySourceError::InvalidProtocol)
}

pub(crate) fn fiscal_analytical_dataset(
    query: &TreasuryFiscalQuery,
) -> Result<SourceIdentifier, TreasurySourceError> {
    SourceIdentifier::try_from(format!(
        "treasury.fiscal-data.average-interest-rates-v2.{}",
        lower_hex(query.query_digest())
    ))
    .map_err(|_| TreasurySourceError::InvalidProtocol)
}

/// Stable local health state for the bounded research producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TreasurySourceHealth {
    last_attempt_at: Option<Timestamp>,
    last_success_at: Option<Timestamp>,
    last_payload_digest: Option<[u8; 32]>,
    consecutive_failures: u32,
}

impl TreasurySourceHealth {
    const fn new() -> Self {
        Self {
            last_attempt_at: None,
            last_success_at: None,
            last_payload_digest: None,
            consecutive_failures: 0,
        }
    }

    /// Returns the most recent local request attempt.
    pub const fn last_attempt_at(self) -> Option<Timestamp> {
        self.last_attempt_at
    }

    /// Returns the most recent successful local observation time.
    pub const fn last_success_at(self) -> Option<Timestamp> {
        self.last_success_at
    }

    /// Returns the most recent exact provider payload digest.
    pub const fn last_payload_digest(self) -> Option<[u8; 32]> {
        self.last_payload_digest
    }

    /// Returns saturating consecutive failures since the last success.
    pub const fn consecutive_failures(self) -> u32 {
        self.consecutive_failures
    }
}

/// A fetched Fiscal Data page retaining exact bytes and evidence of its first local observation.
#[derive(Debug)]
pub struct RetrievedFiscalDataPage {
    received_at: Timestamp,
    bytes: Bytes,
    page: FiscalDataPage,
    capture: ProviderCaptureMaterial,
}

impl RetrievedFiscalDataPage {
    /// Returns when this installation first observed the exact response.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns exact provider bytes for persistence and shared lineage construction.
    pub const fn exact_payload(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns the validated page.
    pub const fn page(&self) -> &FiscalDataPage {
        &self.page
    }

    /// Returns the exact Fiscal Data response ready for source-neutral raw sealing.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture
    }

    /// Consumes the parsed page and its exact raw response material together.
    pub fn into_parts(self) -> (Timestamp, Bytes, FiscalDataPage, ProviderCaptureMaterial) {
        (self.received_at, self.bytes, self.page, self.capture)
    }
}

/// A fetched daily-rate page retaining exact bytes and evidence of its first local observation.
#[derive(Debug)]
pub struct RetrievedDailyRatePage {
    received_at: Timestamp,
    bytes: Bytes,
    page: TreasuryDailyRatePage,
    capture: ProviderCaptureMaterial,
}

impl RetrievedDailyRatePage {
    /// Returns when this installation first observed the exact response.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns exact provider bytes for persistence and shared lineage construction.
    pub const fn exact_payload(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns the validated page.
    pub const fn page(&self) -> &TreasuryDailyRatePage {
        &self.page
    }

    /// Returns the exact daily-rate response ready for source-neutral raw sealing.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture
    }

    /// Consumes the parsed page and its exact raw response material together.
    pub fn into_parts(
        self,
    ) -> (
        Timestamp,
        Bytes,
        TreasuryDailyRatePage,
        ProviderCaptureMaterial,
    ) {
        (self.received_at, self.bytes, self.page, self.capture)
    }
}

/// Canonical Treasury rows paired with every exact response that produced them.
///
/// Fiscal output owns the complete validated page chain; bounded daily output owns its standalone
/// response. The common capture receipt retains ordered request, payload, byte, and receive-clock
/// evidence before the one-shot publication handoff can bind the canonical batch.
#[derive(Debug)]
pub struct TreasuryExtractionOutput {
    batch: ExtractionBatch,
    capture: ProviderCaptureMaterial,
    accounting: TreasuryExtractionAccounting,
    native_lineage_plan: TreasuryNativeLineagePlan,
}

/// Network-authorized Treasury doctor result paired with every exact raw probe response.
///
/// The receipt can only be minted by [`TreasurySource::run_doctor`], which traverses the normal
/// extraction authority, network allowlist, shared provider budget/cooldown, strict parser, and
/// canonical normalizer. The application must seal every returned capture before persisting an
/// activation decision.
#[derive(Debug)]
pub struct TreasuryDoctorRun {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    receipt: crate::TreasuryDoctorReceipt,
    captures: Box<[ProviderCaptureMaterial]>,
}

impl TreasuryDoctorRun {
    /// Returns the number of exact raw probe responses awaiting durable sealing.
    pub fn probe_count(&self) -> usize {
        self.captures.len()
    }

    /// Seals every exact probe response before exposing an activation receipt.
    pub fn seal(
        self,
        store: &SealedResearchJournalStore,
    ) -> Result<crate::TreasurySealedDoctorReceipt, TreasuryDoctorSealError> {
        let mut sealed = Vec::new();
        sealed.try_reserve_exact(self.captures.len()).map_err(|_| {
            TreasuryDoctorSealError::Contract(crate::TreasuryVerticalError::AccountingOverflow)
        })?;
        for capture in self.captures.into_vec() {
            let (expectation, seal_request) = capture.into_whole_seal_parts();
            let capture_token = expectation
                .try_rejoin(seal_request.seal(store)?)
                .and_then(market_squawk_sources::RejoinedProviderCapture::try_into_whole)
                .map_err(|_| crate::TreasuryVerticalError::DoctorRejected)?;
            sealed.push(capture_token.persisted_receipt().clone());
        }
        crate::TreasurySealedDoctorReceipt::try_new(
            self.source_id,
            self.metadata_revision,
            self.receipt,
            sealed,
        )
        .map_err(Into::into)
    }
}

/// Failure while sealing or identity-binding a completed Treasury doctor run.
#[derive(Debug, Error)]
pub enum TreasuryDoctorSealError {
    /// The shared sealed research journal rejected an exact provider response.
    #[error(transparent)]
    Capture(#[from] ProviderCaptureMaterialSealError),
    /// The sealed response set no longer matches parsed doctor evidence.
    #[error(transparent)]
    Contract(#[from] crate::TreasuryVerticalError),
}

impl TreasuryExtractionOutput {
    /// Returns the canonical shared extraction batch.
    pub const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }

    /// Returns exact provider response material for this complete extraction unit.
    ///
    /// Daily extraction contains one response; Fiscal extraction contains the complete ordered
    /// response set. The common publication path must seal all returned material as one unit.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture
    }

    /// Returns aggregate daily-response or complete Fiscal-chain accounting.
    pub const fn accounting(&self) -> &TreasuryExtractionAccounting {
        &self.accounting
    }

    /// Consumes this one-shot output for the source-neutral publication path.
    ///
    /// The returned batch is bound to the exact retained capture receipt only after the
    /// provider-local aggregate rows, points, raw bytes, request set, payload, terminal-page count,
    /// and clocks match both values.
    /// The returned provider-native batch is minted against that rebound canonical identity. Its
    /// aligned page-ordinal vector maps Fiscal rows to their exact chain page and daily-rate rows
    /// to the standalone response at page zero.
    /// The application remains responsible for sealing and reopening the original capture through
    /// its shared research journal before committing an analytical generation.
    pub fn try_into_common_publication(
        self,
    ) -> Result<
        (
            ExtractionBatch,
            ProviderCaptureMaterial,
            ProviderNativeLineageBatch,
            Vec<u16>,
        ),
        crate::TreasuryVerticalError,
    > {
        let Self {
            batch,
            capture,
            accounting,
            native_lineage_plan,
        } = self;
        let batch = batch
            .try_bind_provider_capture(capture.receipt())
            .map_err(|_| crate::TreasuryVerticalError::InvalidExtractionHandoff)?;
        accounting.validate_common_publication(&batch, &capture)?;
        let (native_lineage, row_capture_page_ordinals) = native_lineage_plan
            .try_encode(&batch)
            .map_err(|_| crate::TreasuryVerticalError::InvalidExtractionHandoff)?;
        if row_capture_page_ordinals.len() != batch.records().len()
            || row_capture_page_ordinals
                .iter()
                .any(|ordinal| usize::from(*ordinal) >= capture.receipt().pages().len())
        {
            return Err(crate::TreasuryVerticalError::InvalidExtractionHandoff);
        }
        Ok((batch, capture, native_lineage, row_capture_page_ordinals))
    }
}

/// Allowlisted Treasury research producer requiring registry authority per request.
pub struct TreasurySource {
    metadata: SourceMetadata,
    config: TreasurySourceConfig,
    activation: crate::TreasuryActivationIntent,
    client: TreasuryHttpClient,
    all_history_restore_admission: Arc<Semaphore>,
    health: Mutex<TreasurySourceHealth>,
}

impl std::fmt::Debug for TreasurySource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TreasurySource")
            .field("source_id", self.metadata.source_id())
            .field("revision", self.metadata.revision())
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl TreasurySource {
    /// Builds the most authoritative truthful revision plan supported by this Treasury profile.
    ///
    /// Neither Treasury surface exposes an immutable provider revision identifier. Revisions are
    /// therefore bound to exact locally observed canonical content instead of a fabricated order.
    /// Publication additionally requires the aligned Treasury-native lineage emitted by the
    /// extraction handoff.
    ///
    /// # Errors
    ///
    /// Returns [`TreasurySourceError::InvalidMetadata`] when the batch belongs to another source
    /// registration or [`TreasurySourceError::RevisionAuthority`] when bounded exact evidence
    /// construction fails.
    pub fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<ExtractionRevisionPlan, TreasurySourceError> {
        if batch.request().object().source_id() != self.metadata.source_id()
            || batch.request().object().metadata_revision() != self.metadata.revision()
        {
            return Err(TreasurySourceError::InvalidMetadata);
        }
        ExtractionRevisionPlan::locally_observed_with_native_lineage(batch.records().len())
            .map_err(Into::into)
    }

    /// Binds immutable metadata to one official Treasury profile.
    ///
    /// # Errors
    ///
    /// Fails closed unless the metadata authorizes this exact official-agency profile, coverage,
    /// quality ceiling, network target, and typed public-interface budget. Application composition
    /// separately owns the provider lease and research-rights authority.
    pub fn try_new(
        metadata: SourceMetadata,
        config: TreasurySourceConfig,
    ) -> Result<Self, TreasurySourceError> {
        Self::validate_metadata(&metadata, &config)?;
        let activation = config
            .activation_intent()
            .map_err(|_| TreasurySourceError::InvalidProtocol)?;
        let client = TreasuryHttpClient::try_new(&metadata)?;
        Ok(Self {
            metadata,
            config,
            activation,
            client,
            all_history_restore_admission: Arc::new(Semaphore::new(
                MAX_ALL_HISTORY_RESTORE_WORKERS,
            )),
            health: Mutex::new(TreasurySourceHealth::new()),
        })
    }

    #[cfg(test)]
    fn try_new_with_transport(
        metadata: SourceMetadata,
        config: TreasurySourceConfig,
        transport: Arc<dyn crate::client::TreasuryTransport>,
    ) -> Result<Self, TreasurySourceError> {
        Self::validate_metadata(&metadata, &config)?;
        let activation = config
            .activation_intent()
            .map_err(|_| TreasurySourceError::InvalidProtocol)?;
        let client = TreasuryHttpClient::try_new_with_transport(&metadata, transport)?;
        Ok(Self {
            metadata,
            config,
            activation,
            client,
            all_history_restore_admission: Arc::new(Semaphore::new(
                MAX_ALL_HISTORY_RESTORE_WORKERS,
            )),
            health: Mutex::new(TreasurySourceHealth::new()),
        })
    }

    fn validate_metadata(
        metadata: &SourceMetadata,
        config: &TreasurySourceConfig,
    ) -> Result<(), TreasurySourceError> {
        let budget = metadata
            .budget_policy()
            .ok_or(TreasurySourceError::InvalidMetadata)?;
        if metadata.source_class() != SourceClass::OfficialAgency
            || metadata.provider().as_str() != "us-treasury"
            || metadata.authorization().mode() != AuthorizationMode::PublicInterface
            || metadata.coverage().domain() != CoverageDomain::Macroeconomic
            || metadata.quality_ceiling() != config.quality()
            || crate::vertical::treasury_rate_policy_digest(budget).is_err()
            || metadata.capabilities().live()
            || !metadata.capabilities().extraction()
            || metadata.capabilities().historical() != HistoricalCapability::Historical
            || !matches!(metadata.protocol_profile(), SourceProtocolProfile::NotLive)
        {
            return Err(TreasurySourceError::InvalidMetadata);
        }
        for probe in config.authorization_probe_urls()? {
            metadata
                .network_policy()
                .authorize(&probe)
                .map_err(|_| TreasurySourceError::InvalidMetadata)?;
        }
        Ok(())
    }

    /// Returns the exact configured profile.
    pub const fn config(&self) -> &TreasurySourceConfig {
        &self.config
    }

    /// Returns the exact multi-dataset activation intent for this source generation.
    pub fn dataset_catalog(
        &self,
    ) -> Result<crate::TreasuryDatasetCatalog, crate::TreasuryVerticalError> {
        self.config.dataset_catalog()
    }

    /// Returns the immutable provider-local activation bound at source construction.
    pub const fn activation_intent(&self) -> &crate::TreasuryActivationIntent {
        &self.activation
    }

    /// Returns the bounded exact-query doctor plan for this source generation.
    pub fn doctor_plan(&self) -> Result<crate::TreasuryDoctorPlan, crate::TreasuryVerticalError> {
        self.activation.doctor_plan(&self.config)
    }

    /// Executes every bounded doctor probe through production retrieval and normalization.
    pub async fn run_doctor(
        &self,
        authority: ExtractionAuthority,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<TreasuryDoctorRun, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        let plan = self.doctor_plan().map_err(|_| invalid_protocol())?;
        let mut observations = Vec::new();
        let mut captures = Vec::new();
        observations
            .try_reserve_exact(plan.probes().len())
            .map_err(|_| invalid_protocol())?;
        captures
            .try_reserve_exact(plan.probes().len())
            .map_err(|_| invalid_protocol())?;
        for probe in plan.probes() {
            let started = Instant::now();
            let (received_at, body, capture) = if let Some(request) = probe.fiscal_request() {
                let retrieved = self
                    .fetch_fiscal_page_with_budget_wait(
                        &authority,
                        request,
                        FiscalDataParseLimits::production_defaults(),
                        deadline,
                        &cancellation,
                    )
                    .await?;
                let (received_at, body, _page, capture) = retrieved.into_parts();
                (received_at, body, capture)
            } else if let Some(request) = probe.daily_request() {
                let retrieved = self
                    .fetch_daily_rate_page_with_budget_wait(
                        &authority,
                        request,
                        FiscalDataParseLimits::production_defaults(),
                        deadline,
                        &cancellation,
                    )
                    .await?;
                let (received_at, body, _page, capture) = retrieved.into_parts();
                (received_at, body, capture)
            } else {
                return Err(invalid_protocol());
            };
            let normalized_at = system_timestamp().map_err(map_adapter_error)?;
            let observation = probe
                .inspect_response(
                    &self.metadata,
                    200,
                    &body,
                    received_at,
                    normalized_at,
                    started.elapsed(),
                )
                .map_err(|_| invalid_protocol())?;
            observations.push(observation);
            captures.push(capture);
        }
        let receipt = plan
            .close(observations, &self.metadata)
            .map_err(|_| invalid_protocol())?;
        Ok(TreasuryDoctorRun {
            source_id: self.metadata.source_id().clone(),
            metadata_revision: self.metadata.revision().clone(),
            receipt,
            captures: captures.into_boxed_slice(),
        })
    }

    /// Derives the storage-safe analytical identity for one exact configured provider dataset.
    ///
    /// Provider selectors, source-object identities, and record provenance remain unchanged. This
    /// method only supplies the separate local dataset identity used by analytical publication.
    ///
    /// # Errors
    ///
    /// Returns [`TreasurySourceError::QueryBindingMismatch`] when the provider dataset is not part
    /// of this source's exact configured query set.
    pub fn analytical_dataset_identifier(
        &self,
        provider_dataset: &SourceIdentifier,
    ) -> Result<SourceIdentifier, TreasurySourceError> {
        self.config.analytical_dataset(provider_dataset)
    }

    /// Returns a bounded copy of local producer health.
    ///
    /// # Errors
    ///
    /// Fails closed if health synchronization was poisoned.
    pub fn health(&self) -> Result<TreasurySourceHealth, TreasurySourceError> {
        self.health
            .lock()
            .map(|health| *health)
            .map_err(|_| TreasurySourceError::HealthUnavailable)
    }

    /// Fetches and validates one page from the exact configured Fiscal Data query family.
    pub async fn fetch_fiscal_page(
        &self,
        authority: &ExtractionAuthority,
        request: &TreasuryPageRequest,
        limits: FiscalDataParseLimits,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedFiscalDataPage, ExtractionSourceError> {
        let TreasurySourceConfig::AverageInterestRates(query) = &self.config else {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        };
        if query.query_digest() != request.query_digest() {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        self.validate_authority(authority)?;
        self.record_attempt().map_err(map_adapter_error)?;
        let result = self
            .client
            .fetch(
                &self.metadata,
                authority,
                request.url(),
                JSON_MEDIA_TYPE,
                limits.max_bytes(),
                deadline,
                cancellation,
            )
            .await
            .and_then(|response| {
                let page =
                    FiscalDataPage::parse(&response.bytes, request, limits).map_err(|_| {
                        ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                    })?;
                let (bytes, received_at) = response.record_success()?;
                Ok(RetrievedFiscalDataPage {
                    received_at,
                    capture: capture_material(
                        &self.metadata,
                        fiscal_provider_dataset(query).map_err(map_adapter_error)?,
                        request.request_digest(),
                        received_at,
                        bytes.clone(),
                    )?,
                    bytes,
                    page,
                })
            });
        self.record_extraction_result(&result, |page| page.page.response_payload_digest())?;
        result
    }

    pub(super) async fn fetch_fiscal_page_with_budget_wait(
        &self,
        authority: &ExtractionAuthority,
        request: &TreasuryPageRequest,
        limits: FiscalDataParseLimits,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedFiscalDataPage, ExtractionSourceError> {
        loop {
            let result = self
                .fetch_fiscal_page(authority, request, limits, deadline, cancellation)
                .await;
            match result {
                Ok(retrieved) => return Ok(retrieved),
                Err(error) => {
                    Self::wait_for_shared_budget(authority, error, deadline, cancellation).await?;
                }
            }
        }
    }

    /// Fetches and validates one page from an exact configured daily-rate query family.
    pub async fn fetch_daily_rate_page(
        &self,
        authority: &ExtractionAuthority,
        request: &TreasuryDailyRatePageRequest,
        limits: FiscalDataParseLimits,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedDailyRatePage, ExtractionSourceError> {
        let query = self
            .config
            .query(request.dataset())
            .ok_or_else(invalid_protocol)?;
        let expected = query
            .page(request.page_number())
            .map_err(|_| invalid_protocol())?;
        if expected.request_digest() != request.request_digest() {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        self.validate_authority(authority)?;
        self.record_attempt().map_err(map_adapter_error)?;
        let result = self
            .client
            .fetch(
                &self.metadata,
                authority,
                request.url(),
                XML_MEDIA_TYPE,
                limits.max_bytes(),
                deadline,
                cancellation,
            )
            .await
            .and_then(|response| {
                let page = TreasuryDailyRatePage::parse(&response.bytes, request, limits).map_err(
                    |_| ExtractionSourceError::Source(SourceError::InvalidProtocolState),
                )?;
                let (bytes, received_at) = response.record_success()?;
                Ok(RetrievedDailyRatePage {
                    received_at,
                    capture: capture_material(
                        &self.metadata,
                        request.dataset().clone(),
                        request.request_digest(),
                        received_at,
                        bytes.clone(),
                    )?,
                    bytes,
                    page,
                })
            });
        self.record_extraction_result(&result, |page| page.page.response_payload_digest())?;
        result
    }

    pub(super) async fn fetch_daily_rate_page_with_budget_wait(
        &self,
        authority: &ExtractionAuthority,
        request: &TreasuryDailyRatePageRequest,
        limits: FiscalDataParseLimits,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedDailyRatePage, ExtractionSourceError> {
        loop {
            let result = self
                .fetch_daily_rate_page(authority, request, limits, deadline, cancellation)
                .await;
            match result {
                Ok(retrieved) => return Ok(retrieved),
                Err(error) => {
                    Self::wait_for_shared_budget(authority, error, deadline, cancellation).await?;
                }
            }
        }
    }

    async fn wait_for_shared_budget(
        authority: &ExtractionAuthority,
        error: ExtractionSourceError,
        operation_deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<(), ExtractionSourceError> {
        let budget_deadline = match &error {
            ExtractionSourceError::Authority(
                market_squawk_sources::ExtractionAuthorityError::BudgetWaitUntil { deadline },
            )
            | ExtractionSourceError::Source(SourceError::BudgetWaitUntil { deadline }) => *deadline,
            _ => return Err(error),
        };
        let remaining = authority.remaining_budget_wait(budget_deadline)?;
        let sampled_at = system_timestamp().map_err(map_adapter_error)?;
        let remaining_nanos = i64::try_from(remaining.as_nanos())
            .map_err(|_| ExtractionSourceError::DeadlineExceeded)?;
        if sampled_at
            .checked_add_nanos(remaining_nanos)
            .map_err(|_| ExtractionSourceError::DeadlineExceeded)?
            > operation_deadline
        {
            return Err(ExtractionSourceError::DeadlineExceeded);
        }
        tokio::select! {
            () = cancellation.cancelled() => Err(ExtractionSourceError::Cancelled),
            () = tokio::time::sleep(remaining) => Ok(()),
        }
    }

    /// Traverses one ordinary exact query to its provider-defined terminal condition.
    ///
    /// Fiscal discovery emits one aggregate object only after every ordered response in the
    /// complete page chain has been validated and framed. A bounded daily query emits its one
    /// standalone response. All-history remains a separately checkpointed backfill mode.
    pub async fn discover_with_accounting(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<TreasuryDiscoveryOutput, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.effective_at().is_some()
            || !self
                .config
                .accepts_dataset(request.dataset())
                .map_err(map_adapter_error)?
        {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        let limits = FiscalDataParseLimits::production_defaults();
        let mut objects = Vec::new();
        let descriptor = self
            .config
            .dataset_catalog()
            .map_err(|_| invalid_protocol())?
            .dataset(request.dataset())
            .cloned()
            .ok_or_else(invalid_protocol)?;
        if descriptor.publication_mode() == crate::TreasuryPublicationMode::ResumableBackfill {
            return Err(invalid_protocol());
        }
        let mut request_count = 0_usize;
        let mut response_count = 0_usize;
        let mut returned_source_rows = 0_usize;
        let mut canonical_admission = CanonicalRecordAdmission::new();
        let mut raw_body_bytes = 0_u64;
        let mut reported_total_rows = None;
        let mut reported_total_pages = None;
        let mut first_received_at = None;
        let mut last_received_at = None;
        let mut source_payload_digests = Vec::new();
        match &self.config {
            TreasurySourceConfig::AverageInterestRates(query) => {
                let mut framing = FiscalChainFraming::try_new()?;
                let mut tracker = crate::TreasuryPaginationTracker::try_new(
                    query,
                    market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES,
                    market_squawk_sources::MAX_EXTRACTION_RECORDS,
                )
                .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
                let mut page_number = 1_usize;
                loop {
                    if page_number > market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES {
                        return Err(invalid_protocol());
                    }
                    let page_request = query.page(page_number).map_err(|_| {
                        ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                    })?;
                    let page_limits = fiscal_chain_page_limits(raw_body_bytes)?;
                    request_count = request_count.checked_add(1).ok_or_else(invalid_protocol)?;
                    let retrieved = self
                        .fetch_fiscal_page_with_budget_wait(
                            &authority,
                            &page_request,
                            page_limits,
                            request.deadline(),
                            &cancellation,
                        )
                        .await?;
                    response_count = response_count.checked_add(1).ok_or_else(invalid_protocol)?;
                    let payload_bytes = u64::try_from(retrieved.exact_payload().len())
                        .map_err(|_| invalid_protocol())?;
                    let next_raw_body_bytes = raw_body_bytes
                        .checked_add(payload_bytes)
                        .ok_or_else(invalid_protocol)?;
                    let next_returned_source_rows = returned_source_rows
                        .checked_add(retrieved.page().records().len())
                        .ok_or_else(invalid_protocol)?;
                    validate_fiscal_chain_work(
                        next_raw_body_bytes,
                        next_returned_source_rows,
                        page_number,
                    )?;
                    raw_body_bytes = next_raw_body_bytes;
                    returned_source_rows = next_returned_source_rows;
                    let ingested_at = system_timestamp().map_err(map_adapter_error)?;
                    for record in canonical_fiscal_records(
                        &self.metadata,
                        retrieved.page(),
                        retrieved.received_at(),
                        ingested_at,
                    ) {
                        canonical_admission
                            .admit(record.map_err(map_adapter_error)?)
                            .map_err(map_adapter_error)?;
                    }
                    reported_total_rows = Some(retrieved.page().total_count());
                    reported_total_pages = Some(retrieved.page().total_pages());
                    if last_received_at.is_some_and(|previous| retrieved.received_at() < previous) {
                        return Err(invalid_protocol());
                    }
                    first_received_at.get_or_insert(retrieved.received_at());
                    last_received_at = Some(retrieved.received_at());
                    let terminal = tracker.accept(retrieved.page()).map_err(|_| {
                        ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                    })?;
                    framing.push(retrieved.exact_payload())?;
                    if terminal {
                        break;
                    }
                    page_number = page_number.checked_add(1).ok_or({
                        ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                    })?;
                }
                let framing = framing.finish()?;
                let first_page = query.page(1).map_err(|_| invalid_protocol())?;
                let object = fiscal_chain_source_object(
                    &self.metadata,
                    &request,
                    &first_page,
                    response_count,
                    &framing,
                    last_received_at.ok_or_else(invalid_protocol)?,
                )?;
                source_payload_digests.push(object.evidence().content_digest());
                objects.push(object);
            }
            TreasurySourceConfig::DailyRates(config) => {
                let query = config
                    .query(request.dataset())
                    .ok_or_else(invalid_protocol)?;
                let mut tracker = query
                    .is_all_history()
                    .then(|| {
                        crate::TreasuryDailyRatePaginationTracker::try_new(
                            query,
                            MAX_DAILY_RATE_PAGES,
                            market_squawk_sources::MAX_EXTRACTION_RECORDS,
                        )
                    })
                    .transpose()
                    .map_err(|_| invalid_protocol())?;
                let mut page_number = 0_usize;
                loop {
                    if objects.len() == usize::from(request.max_results()) {
                        return Err(invalid_protocol());
                    }
                    let page_request = query.page(page_number).map_err(|_| {
                        ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                    })?;
                    let retrieved = self
                        .fetch_daily_rate_page_with_budget_wait(
                            &authority,
                            &page_request,
                            limits,
                            request.deadline(),
                            &cancellation,
                        )
                        .await?;
                    request_count = request_count.checked_add(1).ok_or_else(invalid_protocol)?;
                    response_count = response_count.checked_add(1).ok_or_else(invalid_protocol)?;
                    let payload_bytes = u64::try_from(retrieved.exact_payload().len())
                        .map_err(|_| invalid_protocol())?;
                    raw_body_bytes = raw_body_bytes
                        .checked_add(payload_bytes)
                        .ok_or_else(invalid_protocol)?;
                    returned_source_rows = returned_source_rows
                        .checked_add(retrieved.page().observations().len())
                        .ok_or_else(invalid_protocol)?;
                    let ingested_at = system_timestamp().map_err(map_adapter_error)?;
                    for record in canonical_daily_rate_records(
                        &self.metadata,
                        retrieved.page(),
                        retrieved.received_at(),
                        ingested_at,
                    ) {
                        canonical_admission
                            .admit(record.map_err(map_adapter_error)?)
                            .map_err(map_adapter_error)?;
                    }
                    if last_received_at.is_some_and(|previous| retrieved.received_at() < previous) {
                        return Err(invalid_protocol());
                    }
                    first_received_at.get_or_insert(retrieved.received_at());
                    last_received_at = Some(retrieved.received_at());
                    let terminal = match tracker.as_mut() {
                        Some(tracker) => tracker
                            .accept(retrieved.page())
                            .map_err(|_| invalid_protocol())?,
                        None => retrieved.page().is_terminal(),
                    };
                    let object = source_object(
                        &self.metadata,
                        &request,
                        &page_request,
                        retrieved.exact_payload(),
                        retrieved.received_at(),
                        "application/atom+xml",
                        ObjectKind::DailyRate,
                    )?;
                    source_payload_digests.push(object.evidence().content_digest());
                    objects.push(object);
                    if terminal {
                        break;
                    }
                    if !query.is_all_history() {
                        break;
                    }
                    page_number = page_number.checked_add(1).ok_or_else(invalid_protocol)?;
                }
            }
        }
        let first_received_at = first_received_at.ok_or_else(invalid_protocol)?;
        let last_received_at = last_received_at.ok_or_else(invalid_protocol)?;
        let canonical_points = canonical_admission.record_count();
        let observed_numeric_points = canonical_admission.observed_numeric_points();
        let explicit_missing_points = canonical_admission.explicit_missing_points();
        let source_object_count = objects.len();
        let batch = DiscoveryBatch::try_new(&request, objects)?;
        let accounting = TreasuryDiscoveryAccounting::try_new(TreasuryDiscoveryAccountingInput {
            descriptor,
            request_count,
            response_count,
            source_object_count,
            returned_source_rows,
            canonical_points,
            observed_numeric_points,
            explicit_missing_points,
            raw_body_bytes,
            terminal_response_observed: true,
            terminal_response_represented_by_source_object: true,
            reported_total_rows,
            reported_total_pages,
            first_received_at,
            last_received_at,
            source_payload_digests,
        })
        .map_err(|_| invalid_protocol())?;
        TreasuryDiscoveryOutput::try_new(batch, accounting).map_err(|_| invalid_protocol())
    }

    /// Refetches one discovered daily response or one complete Fiscal page chain.
    ///
    /// Returns all canonical rows together with the exact ordered raw response material required
    /// by the common raw-capture publication path.
    pub async fn extract_with_capture(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<TreasuryExtractionOutput, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.object().source_id() != self.metadata.source_id()
            || request.object().metadata_revision() != self.metadata.revision()
            || !self
                .config
                .accepts_dataset(request.object().dataset())
                .map_err(map_adapter_error)?
        {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        let descriptor = self
            .config
            .dataset_catalog()
            .map_err(|_| invalid_protocol())?
            .dataset(request.object().dataset())
            .cloned()
            .ok_or_else(invalid_protocol)?;
        let parsed = ParsedObjectId::parse(request.object().object_id())?;
        let limits = FiscalDataParseLimits::production_defaults();
        let schema =
            SourceIdentifier::try_from(market_squawk_sources::CURRENT_RESEARCH_RECORD_SCHEMA)
                .map_err(|_| invalid_protocol())?;
        let (batch, capture, accounting, native_lineage_plan) = match (&self.config, parsed.kind) {
            (TreasurySourceConfig::AverageInterestRates(query), ObjectKind::FiscalChain) => {
                let first_request = query.page(1).map_err(|_| invalid_protocol())?;
                parsed.verify_request(first_request.request_digest())?;
                if parsed.page_number > market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES {
                    return Err(invalid_protocol());
                }
                let mut tracker = crate::TreasuryPaginationTracker::try_new(
                    query,
                    market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES,
                    market_squawk_sources::MAX_EXTRACTION_RECORDS,
                )
                .map_err(|_| invalid_protocol())?;
                let mut captured = Vec::new();
                captured
                    .try_reserve_exact(parsed.page_number)
                    .map_err(|_| invalid_protocol())?;
                let mut framing = FiscalChainFraming::try_new()?;
                let mut batch = ExtractionBatchAccumulator::try_new(&request)?;
                let mut canonical_admission = CanonicalRecordAdmission::new();
                let mut native_lineage_plan =
                    TreasuryNativeLineagePlan::fiscal(request.object().dataset().clone(), query);
                let mut source_rows = 0_usize;
                let mut raw_body_bytes = 0_u64;
                let mut received_at = None;
                for page_number in 1..=parsed.page_number {
                    let page_request = query.page(page_number).map_err(|_| invalid_protocol())?;
                    let page_limits = fiscal_chain_page_limits(raw_body_bytes)?;
                    let retrieved = self
                        .fetch_fiscal_page_with_budget_wait(
                            &authority,
                            &page_request,
                            page_limits,
                            request.deadline(),
                            &cancellation,
                        )
                        .await?;
                    let terminal = tracker
                        .accept(retrieved.page())
                        .map_err(|_| invalid_protocol())?;
                    if terminal != (page_number == parsed.page_number) {
                        return Err(invalid_protocol());
                    }
                    let payload_bytes = u64::try_from(retrieved.exact_payload().len())
                        .map_err(|_| invalid_protocol())?;
                    let next_raw_body_bytes = raw_body_bytes
                        .checked_add(payload_bytes)
                        .ok_or_else(invalid_protocol)?;
                    let next_source_rows = source_rows
                        .checked_add(retrieved.page().records().len())
                        .ok_or_else(invalid_protocol)?;
                    validate_fiscal_chain_work(next_raw_body_bytes, next_source_rows, page_number)?;
                    if received_at.is_some_and(|previous| retrieved.received_at() < previous) {
                        return Err(invalid_protocol());
                    }
                    let ingested_at = system_timestamp().map_err(map_adapter_error)?;
                    for record in canonical_fiscal_records(
                        &self.metadata,
                        retrieved.page(),
                        retrieved.received_at(),
                        ingested_at,
                    ) {
                        let record = canonical_admission
                            .admit(record.map_err(map_adapter_error)?)
                            .map_err(map_adapter_error)?;
                        batch.push(canonical_extraction_record(&request, &schema, record)?)?;
                    }
                    framing.push(retrieved.exact_payload())?;
                    let request_digest = retrieved.page().request_digest();
                    let request_page_token = page_request.page_token();
                    let response_next_page_token =
                        retrieved.page().next_page_token().map(str::to_owned);
                    let received = retrieved.received_at();
                    let (_, body, decoded, _standalone_capture) = retrieved.into_parts();
                    native_lineage_plan
                        .try_push_fiscal_page(decoded)
                        .map_err(map_adapter_error)?;
                    captured.push(FiscalCapturedPage {
                        request_digest,
                        request_page_token,
                        response_next_page_token,
                        received_at: received,
                        body,
                    });
                    raw_body_bytes = next_raw_body_bytes;
                    source_rows = next_source_rows;
                    received_at = Some(received);
                }
                let framing = framing.finish()?;
                verify_refetched_fiscal_chain(
                    &request,
                    parsed.payload_digest,
                    parsed.page_number,
                    &framing,
                )?;
                let request_digest = fiscal_request_set_digest(&captured)?;
                let payload_digest = Sha256::digest(&framing).into();
                let canonical_points = canonical_admission.record_count();
                let observed_numeric_points = canonical_admission.observed_numeric_points();
                let explicit_missing_points = canonical_admission.explicit_missing_points();
                let capture = fiscal_chain_capture_material(
                    &self.metadata,
                    request.object().dataset().clone(),
                    request_digest,
                    &captured,
                )?;
                let accounting =
                    TreasuryExtractionAccounting::try_new(TreasuryExtractionAccountingInput {
                        descriptor,
                        terminal_page_count: parsed.page_number,
                        aggregate_source_rows: source_rows,
                        aggregate_canonical_points: canonical_points,
                        aggregate_observed_numeric_points: observed_numeric_points,
                        aggregate_explicit_missing_points: explicit_missing_points,
                        aggregate_raw_body_bytes: usize::try_from(raw_body_bytes)
                            .map_err(|_| invalid_protocol())?,
                        source_object_payload_bytes: framing.len(),
                        query_digest: query.query_digest(),
                        request_set_digest: request_digest,
                        source_object_payload_digest: payload_digest,
                        terminal_received_at: received_at.ok_or_else(invalid_protocol)?,
                        provider_published_at: None,
                        terminal_for_query: true,
                    })
                    .map_err(|_| invalid_protocol())?;
                (batch.finish()?, capture, accounting, native_lineage_plan)
            }
            (TreasurySourceConfig::DailyRates(config), ObjectKind::DailyRate) => {
                let query = config
                    .query(request.object().dataset())
                    .ok_or_else(invalid_protocol)?;
                let page_request = query.page(parsed.page_number).map_err(|_| {
                    ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                })?;
                parsed.verify_request(page_request.request_digest())?;
                let retrieved = self
                    .fetch_daily_rate_page_with_budget_wait(
                        &authority,
                        &page_request,
                        limits,
                        request.deadline(),
                        &cancellation,
                    )
                    .await?;
                verify_refetched_object(
                    &request,
                    parsed.payload_digest,
                    retrieved.exact_payload(),
                )?;
                let source_rows = retrieved.page().observations().len();
                let terminal_for_query = !query.is_all_history() || retrieved.page().is_terminal();
                let raw_body_bytes = retrieved.exact_payload().len();
                let received_at = retrieved.received_at();
                let provider_published_at = Some(retrieved.page().feed_published_at());
                let query_digest = retrieved.page().query_digest();
                let request_digest = retrieved.page().request_digest();
                let payload_digest = retrieved.page().response_payload_digest();
                let mut batch = ExtractionBatchAccumulator::try_new(&request)?;
                let mut canonical_admission = CanonicalRecordAdmission::new();
                let ingested_at = system_timestamp().map_err(map_adapter_error)?;
                for record in canonical_daily_rate_records(
                    &self.metadata,
                    retrieved.page(),
                    retrieved.received_at(),
                    ingested_at,
                ) {
                    let record = canonical_admission
                        .admit(record.map_err(map_adapter_error)?)
                        .map_err(map_adapter_error)?;
                    batch.push(canonical_extraction_record(&request, &schema, record)?)?;
                }
                let canonical_points = canonical_admission.record_count();
                let observed_numeric_points = canonical_admission.observed_numeric_points();
                let explicit_missing_points = canonical_admission.explicit_missing_points();
                let accounting =
                    TreasuryExtractionAccounting::try_new(TreasuryExtractionAccountingInput {
                        descriptor,
                        terminal_page_count: 1,
                        aggregate_source_rows: source_rows,
                        aggregate_canonical_points: canonical_points,
                        aggregate_observed_numeric_points: observed_numeric_points,
                        aggregate_explicit_missing_points: explicit_missing_points,
                        aggregate_raw_body_bytes: raw_body_bytes,
                        source_object_payload_bytes: raw_body_bytes,
                        query_digest,
                        request_set_digest: request_digest,
                        source_object_payload_digest: payload_digest,
                        terminal_received_at: received_at,
                        provider_published_at,
                        terminal_for_query,
                    })
                    .map_err(|_| invalid_protocol())?;
                let (_, _, decoded, capture) = retrieved.into_parts();
                let native_lineage_plan = TreasuryNativeLineagePlan::try_daily(
                    request.object().dataset().clone(),
                    decoded,
                )
                .map_err(map_adapter_error)?;
                (batch.finish()?, capture, accounting, native_lineage_plan)
            }
            _ => {
                return Err(ExtractionSourceError::Source(
                    SourceError::InvalidProtocolState,
                ));
            }
        };
        Ok(TreasuryExtractionOutput {
            batch,
            capture,
            accounting,
            native_lineage_plan,
        })
    }

    fn validate_authority(
        &self,
        authority: &ExtractionAuthority,
    ) -> Result<(), ExtractionSourceError> {
        authority.validate_current()?;
        if authority.metadata() != &self.metadata {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        Ok(())
    }

    fn record_attempt(&self) -> Result<(), TreasurySourceError> {
        let now = system_timestamp()?;
        let mut health = self
            .health
            .lock()
            .map_err(|_| TreasurySourceError::HealthUnavailable)?;
        health.last_attempt_at = Some(now);
        Ok(())
    }

    fn record_extraction_result<T>(
        &self,
        result: &Result<T, ExtractionSourceError>,
        digest: impl FnOnce(&T) -> [u8; 32],
    ) -> Result<(), ExtractionSourceError> {
        let mut health = self
            .health
            .lock()
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        match result {
            Ok(value) => {
                health.last_success_at = Some(system_timestamp().map_err(map_adapter_error)?);
                health.last_payload_digest = Some(digest(value));
                health.consecutive_failures = 0;
            }
            Err(_) => {
                health.consecutive_failures = health.consecutive_failures.saturating_add(1);
            }
        }
        Ok(())
    }
}

impl SourceMetadataProvider for TreasurySource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl ExtractionSource for TreasurySource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        Box::pin(async move {
            let output = self
                .discover_with_accounting(authority, request, cancellation)
                .await?;
            if !output.accounting().extraction_ready() {
                return Err(invalid_protocol());
            }
            Ok(output.into_batch())
        })
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        let _ = (authority, request, cancellation);
        Box::pin(async { Err(SourceError::InvalidProtocolState.into()) })
    }
}

fn capture_material(
    metadata: &SourceMetadata,
    dataset: SourceIdentifier,
    request_digest: [u8; 32],
    received_at: Timestamp,
    body: Bytes,
) -> Result<ProviderCaptureMaterial, ExtractionSourceError> {
    let request_identity = EvidenceDigest::new(DigestAlgorithm::Sha256, request_digest);
    let body_bytes = u64::try_from(body.len()).map_err(|_| invalid_protocol())?;
    let body_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&body).into());
    let page = ProviderCapturePageReceipt::try_new(
        0,
        request_identity,
        None,
        None,
        200,
        body_bytes,
        body_digest,
        received_at,
    )
    .map_err(|_| invalid_protocol())?;
    let receipt = ProviderCaptureSetReceipt::try_new(
        metadata.source_id().clone(),
        metadata.revision().clone(),
        dataset,
        request_identity,
        ProviderCaptureTerminalDisposition::StandaloneResponse,
        vec![page],
    )
    .map_err(|_| invalid_protocol())?;
    let record = RawCaptureRecord::try_new_live(
        deterministic_capture_uuid(b"event", &receipt),
        Arc::from(metadata.source_id().as_str()),
        deterministic_capture_uuid(b"connection", &receipt),
        Some(0),
        None,
        DateTime::<Utc>::from_timestamp_nanos(received_at.unix_nanos()),
        body,
    )
    .map_err(|_| invalid_protocol())?;
    ProviderCaptureMaterial::try_new(receipt, vec![record]).map_err(|_| invalid_protocol())
}

fn fiscal_request_set_digest(
    captured: &[FiscalCapturedPage],
) -> Result<[u8; 32], ExtractionSourceError> {
    if captured.is_empty() {
        return Err(invalid_protocol());
    }
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/treasury-fiscal-request-set/v1\0");
    digest.update(
        u64::try_from(captured.len())
            .map_err(|_| invalid_protocol())?
            .to_be_bytes(),
    );
    for page in captured {
        digest.update(page.request_digest);
    }
    Ok(digest.finalize().into())
}

fn canonical_extraction_record(
    request: &ExtractionRequest,
    schema: &SourceIdentifier,
    record: CanonicalTreasuryRecord,
) -> Result<ExtractionRecord, ExtractionSourceError> {
    ExtractionRecord::try_new_with_time(
        request,
        schema.clone(),
        record.evidence,
        record.effective,
        record.published,
        record.availability,
        record.revision,
        None,
        record.payload,
    )
    .map_err(Into::into)
}

struct FiscalCapturedPage {
    request_digest: [u8; 32],
    request_page_token: String,
    response_next_page_token: Option<String>,
    received_at: Timestamp,
    body: Bytes,
}

fn fiscal_chain_capture_material(
    metadata: &SourceMetadata,
    dataset: SourceIdentifier,
    request_set_digest: [u8; 32],
    captured: &[FiscalCapturedPage],
) -> Result<ProviderCaptureMaterial, ExtractionSourceError> {
    if captured.is_empty() {
        return Err(invalid_protocol());
    }
    let request_set_identity = EvidenceDigest::new(DigestAlgorithm::Sha256, request_set_digest);
    let mut pages = Vec::new();
    pages
        .try_reserve_exact(captured.len())
        .map_err(|_| invalid_protocol())?;
    for (index, captured_page) in captured.iter().enumerate() {
        let request_identity =
            EvidenceDigest::new(DigestAlgorithm::Sha256, captured_page.request_digest);
        let request_page_token_digest = (index > 0).then(|| {
            EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(captured_page.request_page_token.as_bytes()).into(),
            )
        });
        let response_next_page_token_digest =
            captured_page
                .response_next_page_token
                .as_ref()
                .map(|token| {
                    EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        Sha256::digest(token.as_bytes()).into(),
                    )
                });
        pages.push(
            ProviderCapturePageReceipt::try_new(
                u16::try_from(index).map_err(|_| invalid_protocol())?,
                request_identity,
                request_page_token_digest,
                response_next_page_token_digest,
                200,
                u64::try_from(captured_page.body.len()).map_err(|_| invalid_protocol())?,
                EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    Sha256::digest(&captured_page.body).into(),
                ),
                captured_page.received_at,
            )
            .map_err(|_| invalid_protocol())?,
        );
    }
    let receipt = ProviderCaptureSetReceipt::try_new(
        metadata.source_id().clone(),
        metadata.revision().clone(),
        dataset,
        request_set_identity,
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
        pages,
    )
    .map_err(|_| invalid_protocol())?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(captured.len())
        .map_err(|_| invalid_protocol())?;
    for (index, captured_page) in captured.iter().enumerate() {
        let ordinal = u64::try_from(index).map_err(|_| invalid_protocol())?;
        let mut event_tag = b"event".to_vec();
        event_tag.extend_from_slice(&ordinal.to_be_bytes());
        let mut connection_tag = b"connection".to_vec();
        connection_tag.extend_from_slice(&ordinal.to_be_bytes());
        records.push(
            RawCaptureRecord::try_new_live(
                deterministic_capture_uuid(&event_tag, &receipt),
                Arc::from(metadata.source_id().as_str()),
                deterministic_capture_uuid(&connection_tag, &receipt),
                Some(ordinal),
                None,
                DateTime::<Utc>::from_timestamp_nanos(captured_page.received_at.unix_nanos()),
                captured_page.body.clone(),
            )
            .map_err(|_| invalid_protocol())?,
        );
    }
    ProviderCaptureMaterial::try_new(receipt, records).map_err(|_| invalid_protocol())
}

fn validate_fiscal_chain_work(
    raw_body_bytes: u64,
    source_rows: usize,
    page_count: usize,
) -> Result<(), ExtractionSourceError> {
    if page_count == 0
        || page_count > market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES
        || raw_body_bytes > market_squawk_sources::MAX_PROVIDER_CAPTURE_BYTES
        || source_rows > market_squawk_sources::MAX_EXTRACTION_RECORDS
    {
        return Err(invalid_protocol());
    }
    Ok(())
}

fn fiscal_chain_page_limits(
    retained_raw_body_bytes: u64,
) -> Result<FiscalDataParseLimits, ExtractionSourceError> {
    let remaining = market_squawk_sources::MAX_PROVIDER_CAPTURE_BYTES
        .checked_sub(retained_raw_body_bytes)
        .filter(|value| *value > 0)
        .ok_or_else(invalid_protocol)?;
    let defaults = FiscalDataParseLimits::production_defaults();
    let max_bytes = usize::try_from(
        remaining.min(u64::try_from(defaults.max_bytes()).map_err(|_| invalid_protocol())?),
    )
    .map_err(|_| invalid_protocol())?;
    FiscalDataParseLimits::try_new(
        max_bytes,
        defaults.max_records(),
        defaults.max_fields(),
        market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES,
    )
    .map_err(|_| invalid_protocol())
}

fn deterministic_capture_uuid(tag: &[u8], receipt: &ProviderCaptureSetReceipt) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/treasury-raw-capture-id/v1");
    hash.update((tag.len() as u64).to_be_bytes());
    hash.update(tag);
    hash.update(receipt.request_set_identity().bytes());
    hash.update(receipt.observation_digest().bytes());
    let finalized = hash.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&finalized[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn map_adapter_error(error: TreasurySourceError) -> ExtractionSourceError {
    match error {
        TreasurySourceError::Cancelled => ExtractionSourceError::Cancelled,
        TreasurySourceError::DeadlineExceeded => ExtractionSourceError::DeadlineExceeded,
        TreasurySourceError::Source(error) => ExtractionSourceError::Source(error),
        TreasurySourceError::InvalidMetadata
        | TreasurySourceError::InvalidBackfillCheckpoint
        | TreasurySourceError::BackfillIncomplete
        | TreasurySourceError::QueryBindingMismatch
        | TreasurySourceError::InvalidProtocol
        | TreasurySourceError::Protocol(_)
        | TreasurySourceError::Rate(_)
        | TreasurySourceError::HealthUnavailable
        | TreasurySourceError::RestoreWorkerUnavailable
        | TreasurySourceError::RevisionAuthority(_) => invalid_protocol(),
        TreasurySourceError::BodyTooLarge => ExtractionSourceError::Source(SourceError::Network),
    }
}

#[cfg(test)]
#[path = "source/tests.rs"]
mod tests;

/// A Treasury source configuration, transport, protocol, or deadline failure.
#[derive(Debug, Error)]
pub enum TreasurySourceError {
    /// Metadata or registry authority does not authorize this source profile.
    #[error("Treasury source metadata is incompatible with the configured profile")]
    InvalidMetadata,
    /// A persisted all-history checkpoint or one of its retained page seals failed validation.
    #[error("Treasury all-history checkpoint is invalid")]
    InvalidBackfillCheckpoint,
    /// The provider-defined empty terminal response has not yet been durably sealed.
    #[error("Treasury all-history backfill is incomplete")]
    BackfillIncomplete,
    /// The page request does not belong to this source's exact query family.
    #[error("Treasury request does not match the configured query")]
    QueryBindingMismatch,
    /// The provider response violated its typed protocol profile.
    #[error("Treasury provider response is invalid")]
    InvalidProtocol,
    /// The response body exceeded its effective parser or metadata bound.
    #[error("Treasury response exceeded its byte limit")]
    BodyTooLarge,
    /// Cancellation was requested.
    #[error("Treasury request was cancelled")]
    Cancelled,
    /// The exact request deadline elapsed.
    #[error("Treasury request deadline elapsed")]
    DeadlineExceeded,
    /// Local source-health synchronization is unavailable.
    #[error("Treasury source health is unavailable")]
    HealthUnavailable,
    /// The bounded retained-page replay worker could not be admitted or joined.
    #[error("Treasury all-history restore worker is unavailable")]
    RestoreWorkerUnavailable,
    /// Shared source transport or provider-budget failure.
    #[error("Treasury source failed: {0}")]
    Source(#[from] SourceError),
    /// Provider schema or parsing failure.
    #[error("Treasury protocol failed: {0}")]
    Protocol(#[from] TreasuryProtocolError),
    /// Average-interest-rate conversion failure.
    #[error("Treasury rate conversion failed: {0}")]
    Rate(#[from] crate::TreasuryRateError),
    /// Exact provider or locally observed revision evidence violated bounded invariants.
    #[error(transparent)]
    RevisionAuthority(#[from] market_squawk_sources::ObservedRevisionError),
}
