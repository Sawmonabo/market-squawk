#[cfg(test)]
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{DataQuality, SourceIdentifier, Timestamp};
use market_squawk_sources::{
    AuthorizationMode, CoverageDomain, DiscoveryBatch, DiscoveryRequest, ExtractionAuthority,
    ExtractionBatch, ExtractionRequest, ExtractionSource, ExtractionSourceError,
    HistoricalCapability, SourceClass, SourceError, SourceMetadata, SourceMetadataProvider,
    SourceProtocolProfile,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::client::{JSON_MEDIA_TYPE, TreasuryHttpClient, XML_MEDIA_TYPE, system_timestamp};
use crate::{
    DailyParYieldCurvePage, FiscalDataPage, FiscalDataParseLimits, TreasuryFiscalQuery,
    TreasuryPageRequest, TreasuryProtocolError, TreasuryYieldCurvePageRequest,
    TreasuryYieldCurveProfile,
};

mod lineage;
mod normalize;

use lineage::{
    ObjectKind, ParsedObjectId, invalid_protocol, lower_hex, source_object, verify_refetched_object,
};
use normalize::{canonical_fiscal_records, canonical_yield_records};

/// One exact provider profile authorized for a Treasury source instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TreasurySourceConfig {
    /// One exact Fiscal Data average-interest-rates query family.
    AverageInterestRates(TreasuryFiscalQuery),
    /// One exact daily par-yield-curve year query family.
    DailyParYieldCurve {
        /// Official profile and methodology evidence.
        profile: TreasuryYieldCurveProfile,
        /// Exact provider year filter.
        year: u16,
    },
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
        let profile = TreasuryYieldCurveProfile::daily_par_yield_curve();
        profile.page(year, 0)?;
        Ok(Self::DailyParYieldCurve { profile, year })
    }

    /// Returns the exact quality ceiling required by this profile.
    pub const fn quality(&self) -> DataQuality {
        match self {
            Self::AverageInterestRates(_) => DataQuality::OfficialDelayed,
            Self::DailyParYieldCurve { profile, .. } => profile.quality(),
        }
    }

    fn authorization_probe_url(&self) -> Result<String, TreasuryProtocolError> {
        match self {
            Self::AverageInterestRates(query) => Ok(query.page(1)?.url().to_owned()),
            Self::DailyParYieldCurve { profile, year } => {
                Ok(profile.page(*year, 0)?.url().to_owned())
            }
        }
    }

    fn dataset(&self) -> Result<SourceIdentifier, TreasurySourceError> {
        let value = match self {
            Self::AverageInterestRates(query) => format!(
                "treasury:fiscal-data:average-interest-rates-v2:{}",
                lower_hex(query.query_digest())
            ),
            Self::DailyParYieldCurve { year, .. } => {
                format!("treasury:daily-par-yield-curve:{year}")
            }
        };
        SourceIdentifier::try_from(value).map_err(|_| TreasurySourceError::InvalidProtocol)
    }
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

/// A fetched Fiscal Data page retaining exact bytes and local-first-observation evidence.
#[derive(Clone, Debug)]
pub struct RetrievedFiscalDataPage {
    received_at: Timestamp,
    bytes: Bytes,
    page: FiscalDataPage,
}

impl RetrievedFiscalDataPage {
    /// Returns the local first-observation time for this exact response.
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
}

/// A fetched yield-curve page retaining exact bytes and local-first-observation evidence.
#[derive(Clone, Debug)]
pub struct RetrievedYieldCurvePage {
    received_at: Timestamp,
    bytes: Bytes,
    page: DailyParYieldCurvePage,
}

impl RetrievedYieldCurvePage {
    /// Returns the local first-observation time for this exact response.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns exact provider bytes for persistence and shared lineage construction.
    pub const fn exact_payload(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns the validated page.
    pub const fn page(&self) -> &DailyParYieldCurvePage {
        &self.page
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
        let probe = config.authorization_probe_url()?;
        metadata
            .network_policy()
            .authorize(&probe)
            .map_err(|_| TreasurySourceError::InvalidMetadata)?;
        Ok(())
    }

    /// Returns the exact configured profile.
    pub const fn config(&self) -> &TreasurySourceConfig {
        &self.config
    }

    /// Returns the exact dataset identity accepted by discovery for this configured source.
    pub fn dataset(&self) -> Result<SourceIdentifier, TreasurySourceError> {
        self.config.dataset()
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
                    bytes: response.bytes,
                    page,
                })
            });
        self.record_extraction_result(&result, |page| page.page.response_payload_digest())?;
        result
    }

    /// Fetches and validates one page from the exact configured daily par-yield query family.
    pub async fn fetch_yield_curve_page(
        &self,
        authority: &ExtractionAuthority,
        request: &TreasuryYieldCurvePageRequest,
        limits: FiscalDataParseLimits,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedYieldCurvePage, ExtractionSourceError> {
        let TreasurySourceConfig::DailyParYieldCurve { profile, year } = &self.config else {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        };
        let expected = profile
            .page(*year, request.page_number())
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
                let page = DailyParYieldCurvePage::parse(&response.bytes, request, limits)
                    .map_err(|_| {
                        ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                    })?;
                Ok(RetrievedYieldCurvePage {
                    received_at: response.received_at,
                    bytes: response.bytes,
                    page,
                })
            });
        self.record_extraction_result(&result, |page| page.page.response_payload_digest())?;
        result
    }

    async fn discover_impl(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryBatch, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.effective_at().is_some()
            || request.dataset() != &self.config.dataset().map_err(map_adapter_error)?
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
            TreasurySourceConfig::DailyParYieldCurve { profile, year } => {
                if request.max_results() > 0 {
                    let page_request = profile.page(*year, 0).map_err(|_| {
                        ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                    })?;
                    let retrieved = self
                        .fetch_yield_curve_page(
                            &authority,
                            &page_request,
                            limits,
                            request.deadline(),
                            &cancellation,
                        )
                        .await?;
                    objects.push(source_object(
                        &self.metadata,
                        &request,
                        &page_request,
                        retrieved.exact_payload(),
                        retrieved.received_at(),
                        "application/atom+xml",
                        ObjectKind::Yield,
                    )?);
                }
            }
        }
        DiscoveryBatch::try_new(&request, objects).map_err(ExtractionSourceError::from)
    }

    async fn extract_impl(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<ExtractionBatch, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.object().source_id() != self.metadata.source_id()
            || request.object().metadata_revision() != self.metadata.revision()
            || request.object().dataset() != &self.config.dataset().map_err(map_adapter_error)?
        {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        let parsed = ParsedObjectId::parse(request.object().object_id())?;
        let limits = FiscalDataParseLimits::production_defaults();
        let ingested_at;
        let records = match (&self.config, parsed.kind) {
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
                ingested_at = system_timestamp().map_err(map_adapter_error)?;
                canonical_fiscal_records(
                    &self.metadata,
                    retrieved.page(),
                    retrieved.received_at(),
                    ingested_at,
                )
                .map_err(map_adapter_error)?
            }
            (TreasurySourceConfig::DailyParYieldCurve { profile, year }, ObjectKind::Yield) => {
                let page_request = profile.page(*year, parsed.page_number).map_err(|_| {
                    ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                })?;
                parsed.verify_request(page_request.request_digest())?;
                let retrieved = self
                    .fetch_yield_curve_page(
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
                ingested_at = system_timestamp().map_err(map_adapter_error)?;
                canonical_yield_records(
                    &self.metadata,
                    retrieved.page(),
                    retrieved.received_at(),
                    ingested_at,
                )
                .map_err(map_adapter_error)?
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
        ExtractionBatch::try_new(&request, records).map_err(ExtractionSourceError::from)
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
        Box::pin(self.extract_impl(authority, request, cancellation))
    }
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
        | TreasurySourceError::HealthUnavailable => invalid_protocol(),
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
}
