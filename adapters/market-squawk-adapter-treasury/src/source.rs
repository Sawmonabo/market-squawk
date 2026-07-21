use std::sync::Mutex;

use bytes::Bytes;
use market_squawk_domain::{DataQuality, Timestamp};
use market_squawk_sources::{
    AuthorizationMode, CoverageDomain, HistoricalCapability, MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
    RegisteredSource, SharedProviderBudget, SourceClass, SourceError, SourceMetadata,
    SourceMetadataProvider, SourceProtocolProfile,
};
use serde::Serialize;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::client::{JSON_MEDIA_TYPE, TreasuryHttpClient, XML_MEDIA_TYPE, system_timestamp};
use crate::{
    AverageInterestRate, DailyParYieldCurvePage, FiscalDataPage, FiscalDataParseLimits,
    TreasuryFiscalQuery, TreasuryPageRequest, TreasuryProtocolError, TreasuryRateProfile,
    TreasuryYieldCurvePageRequest, TreasuryYieldCurveProfile,
};

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

    /// Produces bounded, deterministic normalized JSON payloads for later canonical records.
    ///
    /// # Errors
    ///
    /// Rejects provider schema drift or aggregate normalized payloads above the in-memory ceiling.
    pub fn normalized_payloads(&self) -> Result<Vec<Bytes>, TreasurySourceError> {
        let profile = TreasuryRateProfile::average_interest_rates_v2();
        let mut total_bytes = 0_u64;
        self.page
            .records()
            .iter()
            .map(|record| {
                let value = AverageInterestRate::try_from_record(record, &profile)?;
                let normalized = FiscalNormalizedRecord {
                    schema: "treasury-average-interest-rate-v1",
                    source_identity: profile.source_url(),
                    profile: profile.endpoint(),
                    api_version: profile.api_version(),
                    quality: profile.quality(),
                    local_first_observed_at: self.received_at,
                    query_digest: self.page.query_digest(),
                    request_digest: self.page.request_digest(),
                    source_payload_digest: self.page.response_payload_digest(),
                    value,
                };
                let bytes = Bytes::from(
                    serde_json::to_vec(&normalized)
                        .map_err(|_| TreasurySourceError::InvalidProtocol)?,
                );
                add_normalized_bytes(&mut total_bytes, bytes.len())?;
                Ok(bytes)
            })
            .collect()
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

    /// Produces bounded, deterministic normalized JSON payloads for later canonical records.
    ///
    /// # Errors
    ///
    /// Rejects aggregate normalized payloads above the in-memory ceiling.
    pub fn normalized_payloads(&self) -> Result<Vec<Bytes>, TreasurySourceError> {
        let profile = TreasuryYieldCurveProfile::daily_par_yield_curve();
        let mut total_bytes = 0_u64;
        self.page
            .observations()
            .iter()
            .map(|value| {
                let normalized = YieldNormalizedRecord {
                    schema: "treasury-daily-par-yield-curve-v1",
                    source_identity: profile.source_identity(),
                    profile: "daily_treasury_yield_curve",
                    methodology_url: profile.methodology_url(),
                    methodology_revision: profile.methodology_revision(),
                    quality: profile.quality(),
                    local_first_observed_at: self.received_at,
                    query_digest: self.page.query_digest(),
                    request_digest: self.page.request_digest(),
                    source_payload_digest: self.page.response_payload_digest(),
                    value,
                };
                let bytes = Bytes::from(
                    serde_json::to_vec(&normalized)
                        .map_err(|_| TreasurySourceError::InvalidProtocol)?,
                );
                add_normalized_bytes(&mut total_bytes, bytes.len())?;
                Ok(bytes)
            })
            .collect()
    }
}

#[derive(Serialize)]
struct FiscalNormalizedRecord {
    schema: &'static str,
    source_identity: &'static str,
    profile: &'static str,
    api_version: &'static str,
    quality: DataQuality,
    local_first_observed_at: Timestamp,
    query_digest: [u8; 32],
    request_digest: [u8; 32],
    source_payload_digest: [u8; 32],
    value: AverageInterestRate,
}

#[derive(Serialize)]
struct YieldNormalizedRecord<'a> {
    schema: &'static str,
    source_identity: &'static str,
    profile: &'static str,
    methodology_url: &'static str,
    methodology_revision: &'static str,
    quality: DataQuality,
    local_first_observed_at: Timestamp,
    query_digest: [u8; 32],
    request_digest: [u8; 32],
    source_payload_digest: [u8; 32],
    value: &'a crate::DailyParYieldCurveObservation,
}

/// Registered, allowlisted and registry-budget-coordinated Treasury research producer.
pub struct TreasurySource {
    metadata: SourceMetadata,
    budget: SharedProviderBudget,
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
    /// Binds immutable metadata and registry-issued budget authority to one profile.
    ///
    /// # Errors
    ///
    /// Fails closed unless the metadata authorizes this exact official-agency profile, coverage,
    /// quality ceiling, network target and public-interface budget.
    pub fn try_new(
        metadata: SourceMetadata,
        registered: &RegisteredSource,
        config: TreasurySourceConfig,
    ) -> Result<Self, TreasurySourceError> {
        if registered.source_id() != metadata.source_id()
            || registered.revision() != metadata.revision()
            || metadata.source_class() != SourceClass::OfficialAgency
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
        let budget = registered
            .budget()
            .cloned()
            .ok_or(TreasurySourceError::InvalidMetadata)?;
        let probe = config.authorization_probe_url()?;
        metadata
            .network_policy()
            .authorize(&probe)
            .map_err(|_| TreasurySourceError::InvalidMetadata)?;
        let client = TreasuryHttpClient::try_new(&metadata)?;
        Ok(Self {
            metadata,
            budget,
            config,
            client,
            health: Mutex::new(TreasurySourceHealth::new()),
        })
    }

    /// Returns the exact configured profile.
    pub const fn config(&self) -> &TreasurySourceConfig {
        &self.config
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
        request: &TreasuryPageRequest,
        limits: FiscalDataParseLimits,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedFiscalDataPage, TreasurySourceError> {
        let TreasurySourceConfig::AverageInterestRates(query) = &self.config else {
            return Err(TreasurySourceError::QueryBindingMismatch);
        };
        if query.query_digest() != request.query_digest() {
            return Err(TreasurySourceError::QueryBindingMismatch);
        }
        self.record_attempt()?;
        let result = self
            .client
            .fetch(
                &self.metadata,
                &self.budget,
                request.url(),
                JSON_MEDIA_TYPE,
                limits.max_bytes(),
                deadline,
                cancellation,
            )
            .await
            .and_then(|response| {
                let page = FiscalDataPage::parse(&response.bytes, request, limits)?;
                Ok(RetrievedFiscalDataPage {
                    received_at: response.received_at,
                    bytes: response.bytes,
                    page,
                })
            });
        self.record_result(&result, |page| page.page.response_payload_digest())?;
        result
    }

    /// Fetches and validates one page from the exact configured daily par-yield query family.
    pub async fn fetch_yield_curve_page(
        &self,
        request: &TreasuryYieldCurvePageRequest,
        limits: FiscalDataParseLimits,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedYieldCurvePage, TreasurySourceError> {
        let TreasurySourceConfig::DailyParYieldCurve { profile, year } = &self.config else {
            return Err(TreasurySourceError::QueryBindingMismatch);
        };
        let expected = profile.page(*year, request.page_number())?;
        if expected.request_digest() != request.request_digest() {
            return Err(TreasurySourceError::QueryBindingMismatch);
        }
        self.record_attempt()?;
        let result = self
            .client
            .fetch(
                &self.metadata,
                &self.budget,
                request.url(),
                XML_MEDIA_TYPE,
                limits.max_bytes(),
                deadline,
                cancellation,
            )
            .await
            .and_then(|response| {
                let page = DailyParYieldCurvePage::parse(&response.bytes, request, limits)?;
                Ok(RetrievedYieldCurvePage {
                    received_at: response.received_at,
                    bytes: response.bytes,
                    page,
                })
            });
        self.record_result(&result, |page| page.page.response_payload_digest())?;
        result
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

    fn record_result<T>(
        &self,
        result: &Result<T, TreasurySourceError>,
        digest: impl FnOnce(&T) -> [u8; 32],
    ) -> Result<(), TreasurySourceError> {
        let mut health = self
            .health
            .lock()
            .map_err(|_| TreasurySourceError::HealthUnavailable)?;
        match result {
            Ok(value) => {
                health.last_success_at = Some(system_timestamp()?);
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

fn add_normalized_bytes(total: &mut u64, bytes: usize) -> Result<(), TreasurySourceError> {
    let bytes = u64::try_from(bytes).map_err(|_| TreasurySourceError::BodyTooLarge)?;
    *total = total
        .checked_add(bytes)
        .ok_or(TreasurySourceError::BodyTooLarge)?;
    if *total > MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES {
        return Err(TreasurySourceError::BodyTooLarge);
    }
    Ok(())
}
