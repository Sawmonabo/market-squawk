use std::sync::{Arc, Mutex};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
};
use market_squawk_platform::RawCaptureRecord;
use market_squawk_sources::{
    AuthorizationMode, CoverageDomain, DiscoveryBatch, DiscoveryRequest, ExtractionAuthority,
    ExtractionBatch, ExtractionRequest, ExtractionRevisionEvidence, ExtractionRevisionPlan,
    ExtractionSource, ExtractionSourceError, HistoricalCapability, ObservedProviderOrder,
    ProviderCaptureMaterial, ProviderCapturePageReceipt, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, SourceClass, SourceError, SourceMetadata,
    SourceMetadataProvider, SourceProtocolProfile,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::client::{JSON_MEDIA_TYPE, TreasuryHttpClient, XML_MEDIA_TYPE, system_timestamp};
use crate::{
    FiscalDataPage, FiscalDataParseLimits, TreasuryDailyRateFamily, TreasuryDailyRatePage,
    TreasuryDailyRatePageRequest, TreasuryDailyRateQuery, TreasuryFiscalQuery, TreasuryPageRequest,
    TreasuryProtocolError, TreasuryYieldCurvePageRequest,
};

mod lineage;
mod normalize;

use lineage::{
    ObjectKind, ParsedObjectId, invalid_protocol, lower_hex, source_object, verify_refetched_object,
};
use normalize::{canonical_daily_rate_records, canonical_fiscal_records};

const MAX_DAILY_RATE_QUERIES: usize = 1_024;
const MAX_DAILY_RATE_PAGES: usize = 1_024;

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

    fn single_dataset(&self) -> Result<SourceIdentifier, TreasurySourceError> {
        match self {
            Self::AverageInterestRates(query) => fiscal_provider_dataset(query),
            Self::DailyRates(config) if config.queries().len() == 1 => {
                Ok(config.queries()[0].dataset().clone())
            }
            Self::DailyRates(_) => Err(TreasurySourceError::InvalidProtocol),
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

/// Backward-compatible name for one retrieved Treasury daily-rate page.
pub type RetrievedYieldCurvePage = RetrievedDailyRatePage;

/// Canonical Treasury rows paired with the exact single response that produced them.
///
/// Each discovered source object identifies one provider page, so its extraction capture is a
/// standalone one-response set. Fiscal total pages and daily-rate terminal state remain validated
/// by the provider-native page before this output is constructed, while the object identity binds
/// the exact page number, request digest, and response digest.
#[derive(Debug)]
pub struct TreasuryExtractionOutput {
    batch: ExtractionBatch,
    capture: ProviderCaptureMaterial,
}

impl TreasuryExtractionOutput {
    /// Returns the canonical shared extraction batch.
    pub const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }

    /// Returns the exact provider response that must be sealed before publishing the batch.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture
    }

    /// Consumes the application handoff into canonical and exact raw components.
    pub fn into_parts(self) -> (ExtractionBatch, ProviderCaptureMaterial) {
        (self.batch, self.capture)
    }
}

/// Allowlisted Treasury research producer requiring registry authority per request.
pub struct TreasurySource {
    metadata: SourceMetadata,
    config: TreasurySourceConfig,
    client: TreasuryHttpClient,
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
    /// Daily-rate observations use the provider publication timestamp and exact record token.
    /// Fiscal Data average-rate rows publish no version chronology, so their revisions are bound to
    /// exact locally observed canonical content instead of a fabricated provider order.
    ///
    /// # Errors
    ///
    /// Returns [`TreasurySourceError::InvalidMetadata`] when the batch belongs to another source
    /// registration, [`TreasurySourceError::InvalidProtocol`] when a daily-rate record lacks its
    /// required publication timestamp, and [`TreasurySourceError::RevisionAuthority`] when bounded
    /// exact evidence construction fails.
    pub fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<ExtractionRevisionPlan, TreasurySourceError> {
        if batch.request().object().source_id() != self.metadata.source_id()
            || batch.request().object().metadata_revision() != self.metadata.revision()
        {
            return Err(TreasurySourceError::InvalidMetadata);
        }
        match &self.config {
            TreasurySourceConfig::AverageInterestRates(_) => {
                ExtractionRevisionPlan::locally_observed(batch.records().len()).map_err(Into::into)
            }
            TreasurySourceConfig::DailyRates(_) => {
                let mut evidence = Vec::new();
                evidence
                    .try_reserve_exact(batch.records().len())
                    .map_err(|_| {
                        TreasurySourceError::RevisionAuthority(
                            market_squawk_sources::ObservedRevisionError::AllocationFailure,
                        )
                    })?;
                for record in batch.records() {
                    let version = record.revision().as_str().as_bytes();
                    let published = record
                        .published_time()
                        .cloned()
                        .ok_or(TreasurySourceError::InvalidProtocol)?;
                    let order = ObservedProviderOrder::try_new(published, version)?;
                    evidence.push(ExtractionRevisionEvidence::provider_supplied(
                        version, order,
                    )?);
                }
                ExtractionRevisionPlan::try_new(evidence).map_err(Into::into)
            }
        }
    }

    /// Binds immutable metadata to one official Treasury profile.
    ///
    /// # Errors
    ///
    /// Fails closed unless the metadata authorizes this exact official-agency profile, coverage,
    /// quality ceiling, network target and public-interface budget.
    pub fn try_new(
        metadata: SourceMetadata,
        config: TreasurySourceConfig,
    ) -> Result<Self, TreasurySourceError> {
        Self::validate_metadata(&metadata, &config)?;
        let client = TreasuryHttpClient::try_new(&metadata)?;
        Ok(Self {
            metadata,
            config,
            client,
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
        let client = TreasuryHttpClient::try_new_with_transport(&metadata, transport)?;
        Ok(Self {
            metadata,
            config,
            client,
            health: Mutex::new(TreasurySourceHealth::new()),
        })
    }

    fn validate_metadata(
        metadata: &SourceMetadata,
        config: &TreasurySourceConfig,
    ) -> Result<(), TreasurySourceError> {
        if metadata.source_class() != SourceClass::OfficialAgency
            || metadata.provider().as_str() != "us-treasury"
            || metadata.authorization().mode() != AuthorizationMode::PublicInterface
            || metadata.coverage().domain() != CoverageDomain::Macroeconomic
            || metadata.quality_ceiling() != config.quality()
            || metadata.budget_policy().is_none()
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

    /// Returns the exact dataset identity accepted by discovery for this configured source.
    ///
    /// This compatibility accessor is available only for a single-dataset source. Multi-dataset
    /// daily-rate sources are addressed by the dataset supplied to each discovery request.
    ///
    /// # Errors
    ///
    /// Returns [`TreasurySourceError::InvalidProtocol`] for a multi-dataset configuration.
    pub fn dataset(&self) -> Result<SourceIdentifier, TreasurySourceError> {
        self.config.single_dataset()
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
                Ok(RetrievedFiscalDataPage {
                    received_at: response.received_at,
                    capture: capture_material(
                        &self.metadata,
                        fiscal_provider_dataset(query).map_err(map_adapter_error)?,
                        request.request_digest(),
                        response.received_at,
                        response.bytes.clone(),
                    )?,
                    bytes: response.bytes,
                    page,
                })
            });
        self.record_extraction_result(&result, |page| page.page.response_payload_digest())?;
        result
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
                Ok(RetrievedDailyRatePage {
                    received_at: response.received_at,
                    capture: capture_material(
                        &self.metadata,
                        request.dataset().clone(),
                        request.request_digest(),
                        response.received_at,
                        response.bytes.clone(),
                    )?,
                    bytes: response.bytes,
                    page,
                })
            });
        self.record_extraction_result(&result, |page| page.page.response_payload_digest())?;
        result
    }

    /// Backward-compatible nominal-yield fetch entry point.
    pub async fn fetch_yield_curve_page(
        &self,
        authority: &ExtractionAuthority,
        request: &TreasuryYieldCurvePageRequest,
        limits: FiscalDataParseLimits,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedYieldCurvePage, ExtractionSourceError> {
        self.fetch_daily_rate_page(
            authority,
            request.as_daily_request(),
            limits,
            deadline,
            cancellation,
        )
        .await
    }

    async fn discover_impl(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryBatch, ExtractionSourceError> {
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
        match &self.config {
            TreasurySourceConfig::AverageInterestRates(query) => {
                let mut tracker = crate::TreasuryPaginationTracker::try_new(
                    query,
                    100_000,
                    market_squawk_sources::MAX_EXTRACTION_RECORDS,
                )
                .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
                let mut page_number = 1_usize;
                while objects.len() < usize::from(request.max_results()) {
                    let page_request = query.page(page_number).map_err(|_| {
                        ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                    })?;
                    let retrieved = self
                        .fetch_fiscal_page(
                            &authority,
                            &page_request,
                            limits,
                            request.deadline(),
                            &cancellation,
                        )
                        .await?;
                    let terminal = tracker.accept(retrieved.page()).map_err(|_| {
                        ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                    })?;
                    objects.push(source_object(
                        &self.metadata,
                        &request,
                        &page_request,
                        retrieved.exact_payload(),
                        retrieved.received_at(),
                        "application/json",
                        ObjectKind::Fiscal,
                    )?);
                    if terminal {
                        break;
                    }
                    page_number = page_number.checked_add(1).ok_or({
                        ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                    })?;
                }
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
                    let page_request = query.page(page_number).map_err(|_| {
                        ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                    })?;
                    let retrieved = self
                        .fetch_daily_rate_page(
                            &authority,
                            &page_request,
                            limits,
                            request.deadline(),
                            &cancellation,
                        )
                        .await?;
                    let terminal = match tracker.as_mut() {
                        Some(tracker) => tracker
                            .accept(retrieved.page())
                            .map_err(|_| invalid_protocol())?,
                        None => retrieved.page().is_terminal(),
                    };
                    if terminal {
                        break;
                    }
                    if objects.len() == usize::from(request.max_results()) {
                        return Err(invalid_protocol());
                    }
                    objects.push(source_object(
                        &self.metadata,
                        &request,
                        &page_request,
                        retrieved.exact_payload(),
                        retrieved.received_at(),
                        "application/atom+xml",
                        ObjectKind::DailyRate,
                    )?);
                    if !query.is_all_history() {
                        break;
                    }
                    page_number = page_number.checked_add(1).ok_or_else(invalid_protocol)?;
                }
            }
        }
        DiscoveryBatch::try_new(&request, objects).map_err(ExtractionSourceError::from)
    }

    /// Refetches one discovered Treasury page and returns its canonical rows with the exact raw
    /// response material required before durable publication.
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
        let parsed = ParsedObjectId::parse(request.object().object_id())?;
        let limits = FiscalDataParseLimits::production_defaults();
        let (records, capture) = match (&self.config, parsed.kind) {
            (TreasurySourceConfig::AverageInterestRates(query), ObjectKind::Fiscal) => {
                let page_request = query.page(parsed.page_number).map_err(|_| {
                    ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                })?;
                parsed.verify_request(page_request.request_digest())?;
                let retrieved = self
                    .fetch_fiscal_page(
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
                let ingested_at = system_timestamp().map_err(map_adapter_error)?;
                let records = canonical_fiscal_records(
                    &self.metadata,
                    retrieved.page(),
                    retrieved.received_at(),
                    ingested_at,
                )
                .map_err(map_adapter_error)?;
                (records, retrieved.capture)
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
                    .fetch_daily_rate_page(
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
                let ingested_at = system_timestamp().map_err(map_adapter_error)?;
                let records = canonical_daily_rate_records(
                    &self.metadata,
                    retrieved.page(),
                    retrieved.received_at(),
                    ingested_at,
                )
                .map_err(map_adapter_error)?;
                (records, retrieved.capture)
            }
            _ => {
                return Err(ExtractionSourceError::Source(
                    SourceError::InvalidProtocolState,
                ));
            }
        };
        if records.len() > request.max_records() as usize {
            return Err(ExtractionSourceError::Contract(
                market_squawk_sources::ExtractionError::RecordLimitExceeded {
                    requested: request.max_records(),
                },
            ));
        }
        let schema = SourceIdentifier::try_from("market-squawk-research-v3")
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        let records = records
            .into_iter()
            .map(|record| {
                market_squawk_sources::ExtractionRecord::try_new_with_time(
                    &request,
                    schema.clone(),
                    record.evidence,
                    record.effective,
                    record.published,
                    record.availability,
                    record.revision,
                    None,
                    record.payload,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batch = ExtractionBatch::try_new(&request, records)?;
        Ok(TreasuryExtractionOutput { batch, capture })
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
        Box::pin(self.discover_impl(authority, request, cancellation))
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

fn deterministic_capture_uuid(tag: &[u8], receipt: &ProviderCaptureSetReceipt) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/treasury-raw-capture-id/v1");
    hash.update((tag.len() as u64).to_be_bytes());
    hash.update(tag);
    hash.update(receipt.request_set_identity().bytes());
    hash.update(receipt.observation_digest().bytes());
    let mut bytes: [u8; 16] = hash.finalize()[..16]
        .try_into()
        .expect("SHA-256 prefix has a fixed length");
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
        | TreasurySourceError::QueryBindingMismatch
        | TreasurySourceError::InvalidProtocol
        | TreasurySourceError::Protocol(_)
        | TreasurySourceError::Rate(_)
        | TreasurySourceError::HealthUnavailable
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
