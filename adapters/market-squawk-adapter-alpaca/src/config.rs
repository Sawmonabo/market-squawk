use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::time::Duration;

use market_squawk_domain::{
    AssetClass, BarTimestampBasis, ChecksumCapability, CoverageDelay, DataQuality,
    DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    InstrumentId, IntegrityRule, LiveEventClass, MarketBarSessionEvidence, ProviderChannel,
    ProviderInstrumentId, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion,
    SchemaVersion, SequenceCapability, SnapshotApplicability, SourceId, SourceIdentifier,
    Timestamp, VenueId,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationGrant, AuthorizationMode, ChecksumValidationProfile,
    CoverageDomain, CoverageTopology, FreshnessPolicy, HistoricalCapability, HttpRequestBounds,
    InstrumentCoverage, InstrumentCoverageMembership, LiveCoverageDeclaration, LiveCoverageRule,
    LiveProtocolProfile, NetworkAccessPolicy, PathScope, ProviderBudgetPolicy,
    ProviderNumericPolicy, QueryParameterRule, QuerySensitivity, SemanticInterpretationProfile,
    SequenceValidationProfile, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataInput, SourceProtocolProfile,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::AlpacaError;
use crate::boot_snapshot::AlpacaIexBootSnapshotContract;

/// Alpaca Basic real-time equity WebSocket symbol ceiling.
pub const ALPACA_BASIC_EQUITY_SYMBOL_LIMIT: usize = 30;
/// Alpaca Basic indicative-option quote WebSocket symbol ceiling.
pub const ALPACA_BASIC_OPTION_SYMBOL_LIMIT: usize = 200;
/// Alpaca Basic historical request ceiling.
pub const ALPACA_BASIC_HISTORICAL_REQUESTS_PER_MINUTE: u32 = 200;
/// Market Squawk hard application ceiling for recurring Alpaca REST work.
///
/// This is an application policy, not a provider-published limit. Runtime-observed headers may
/// reduce admission but never raise this ceiling.
pub const ALPACA_APPLICATION_MAX_REQUESTS_PER_MINUTE: u32 = 150;
/// Market Squawk recurring Alpaca REST scheduling target.
///
/// The remaining application capacity is reserved for interactive requests, retries, pagination,
/// gap repair, and provider-health work.
pub const ALPACA_RECURRING_TARGET_REQUESTS_PER_MINUTE: u32 = 120;
/// Alpaca Basic historical exclusion window, in nanoseconds.
pub const ALPACA_HISTORICAL_EXCLUSION_NANOS: u64 = 900_000_000_000;
/// Minimum contiguous lookback admitted for one historical analysis plan.
pub const ALPACA_HISTORICAL_MIN_LOOKBACK_DAYS: u16 = 30;
/// Maximum contiguous lookback admitted for one historical analysis plan.
pub const ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS: u16 = 3_650;

pub(crate) const ALPACA_PROVIDER: &str = "alpaca-market-data";
pub(crate) const ALPACA_IEX_ENDPOINT: &str = "wss://stream.data.alpaca.markets/v2/iex";
pub(crate) const ALPACA_OPTIONS_ENDPOINT: &str =
    "wss://stream.data.alpaca.markets/v1beta1/indicative";
pub(crate) const ALPACA_STOCKS_BASE_ENDPOINT: &str = "https://data.alpaca.markets/v2/stocks";
pub(crate) const ALPACA_STOCKS_SNAPSHOTS_ENDPOINT: &str =
    "https://data.alpaca.markets/v2/stocks/snapshots";
pub(crate) const IEX_VENUE: &str = "iex";
pub(crate) const INDICATIVE_OPTIONS_VENUE: &str = "alpaca-indicative-options";

const MAX_SYMBOL_BYTES: usize = 32;
const MAX_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_SUBSCRIPTION_BYTES: usize = 64 * 1024;
const HISTORICAL_FLOOR_UNIX_NANOS: i64 = 1_451_606_400_000_000_000;
const HISTORICAL_PAGE_LIMIT: u16 = 10_000;
const NANOS_PER_DAY: u64 = 86_400_000_000_000;
const NANOS_PER_MINUTE: u64 = 60_000_000_000;
const PROVIDER_DATASET_PREFIX: &str = "alpaca:historical-equity:v1:";
const ANALYTICAL_DATASET_PREFIX: &str = "alpaca.historical-equity.v1.";

/// Stable provider symbol to internal instrument mapping for an equity or ETF.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaInstrumentMapping {
    symbol: Box<str>,
    instrument: InstrumentId,
    asset_class: AssetClass,
}

impl AlpacaInstrumentMapping {
    /// Constructs an Alpaca equity or ETF mapping.
    ///
    /// # Errors
    ///
    /// Rejects symbols outside the bounded US-listed grammar and non-equity/fund asset classes.
    pub fn try_new(
        symbol: String,
        instrument: InstrumentId,
        asset_class: AssetClass,
    ) -> Result<Self, AlpacaError> {
        validate_equity_symbol(&symbol)?;
        if !matches!(asset_class, AssetClass::Equity | AssetClass::Fund) {
            return Err(AlpacaError::InvalidCoverage);
        }
        Ok(Self {
            symbol: symbol.into_boxed_str(),
            instrument,
            asset_class,
        })
    }

    /// Returns the exact provider symbol.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Returns the stable internal instrument.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    /// Returns whether the mapping is an equity or fund/ETF.
    pub const fn asset_class(&self) -> AssetClass {
        self.asset_class
    }
}

/// Stable compact OCC-style option symbol to internal instrument mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaOptionMapping {
    symbol: Box<str>,
    instrument: InstrumentId,
}

impl AlpacaOptionMapping {
    /// Constructs a bounded option mapping.
    ///
    /// # Errors
    ///
    /// Rejects wildcard or non-compact OCC option symbols.
    pub fn try_new(symbol: String, instrument: InstrumentId) -> Result<Self, AlpacaError> {
        validate_option_symbol(&symbol)?;
        Ok(Self {
            symbol: symbol.into_boxed_str(),
            instrument,
        })
    }

    /// Returns the exact provider option symbol.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Returns the stable internal option instrument.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }
}

/// Count and deadline limits for one Alpaca WebSocket generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlpacaTransportLimits {
    max_frame_bytes: usize,
    connect_timeout: Duration,
    io_timeout: Duration,
}

impl AlpacaTransportLimits {
    /// Constructs nonzero bounded transport limits.
    pub fn try_new(
        max_frame_bytes: usize,
        connect_timeout: Duration,
        io_timeout: Duration,
    ) -> Result<Self, AlpacaError> {
        if max_frame_bytes == 0
            || max_frame_bytes > market_squawk_sources::MAX_RAW_FRAME_BYTES
            || connect_timeout.is_zero()
            || io_timeout.is_zero()
            || connect_timeout > MAX_OPERATION_TIMEOUT
            || io_timeout > MAX_OPERATION_TIMEOUT
        {
            return Err(AlpacaError::InvalidTransportLimits);
        }
        Ok(Self {
            max_frame_bytes,
            connect_timeout,
            io_timeout,
        })
    }

    /// Returns the maximum accepted decompressed WebSocket message size in bytes.
    pub const fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }

    /// Returns the exact WebSocket connection-handshake deadline.
    pub const fn connect_timeout(self) -> Duration {
        self.connect_timeout
    }

    /// Returns the exact deadline for each WebSocket write or read.
    ///
    /// This is also the subscription-acknowledgement wait bound: the transport sends the bounded
    /// subscription and each subsequent provider read, including the acknowledgement, must finish
    /// within this duration. Alpaca does not define a separate acknowledgement deadline.
    pub const fn io_timeout(self) -> Duration {
        self.io_timeout
    }
}

/// Secret-free resource projection for the mandatory IEX boot-snapshot request.
///
/// This value carries no endpoint, symbol, credential, provider-budget, or retry authority. It is
/// safe for application composition to bind into source revision evidence and pre-acknowledgement
/// resource limits before constructing [`AlpacaIexLiveConfig`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlpacaIexBootSnapshotPolicy {
    maximum_body_bytes: usize,
    total_timeout: Duration,
}

impl AlpacaIexBootSnapshotPolicy {
    /// Returns the current closed bootstrap protocol revision.
    pub const fn revision(self) -> &'static str {
        "alpaca-iex-live-boot-snapshot/v1"
    }

    /// Derives the only boot resource policy from an already validated live transport policy.
    pub fn from_transport_limits(limits: AlpacaTransportLimits) -> Self {
        Self {
            maximum_body_bytes: limits.max_frame_bytes(),
            total_timeout: limits.connect_timeout().max(limits.io_timeout()),
        }
    }

    /// Returns the exact raw-response and raw-frame byte ceiling.
    pub const fn maximum_body_bytes(self) -> usize {
        self.maximum_body_bytes
    }

    /// Returns the complete HTTP operation timeout before WebSocket connection begins.
    pub const fn total_timeout(self) -> Duration {
        self.total_timeout
    }
}

/// Immutable authenticated IEX live profile for Alpaca Basic.
#[derive(Clone, Debug)]
pub struct AlpacaIexLiveConfig {
    metadata: SourceMetadata,
    mappings: Box<[AlpacaInstrumentMapping]>,
    limits: AlpacaTransportLimits,
    boot_snapshot: AlpacaIexBootSnapshotContract,
    subscription: Box<str>,
}

impl AlpacaIexLiveConfig {
    /// Constructs real-time IEX-only trades, quotes, and statuses coverage.
    #[allow(
        clippy::too_many_arguments,
        reason = "source evidence and runtime bounds stay explicit"
    )]
    pub fn try_new(
        source_id: SourceId,
        revision_evidence: RevisionBoundPayloadEvidence,
        authorization: AuthorizationGrant,
        coverage_evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        mappings: Vec<AlpacaInstrumentMapping>,
        freshness: FreshnessPolicy,
        budget: ProviderBudgetPolicy,
        limits: AlpacaTransportLimits,
    ) -> Result<Self, AlpacaError> {
        validate_authorization_and_budget(&authorization, &budget)?;
        validate_equity_mappings(&mappings, ALPACA_BASIC_EQUITY_SYMBOL_LIMIT)?;
        let boot_snapshot = AlpacaIexBootSnapshotContract::try_new(&mappings, limits)?;
        let metadata = live_metadata(
            source_id,
            revision_evidence,
            authorization,
            coverage_evidence,
            effective,
            &mappings,
            Vec::new(),
            freshness,
            budget,
            LiveSurface::Iex,
            Some(&boot_snapshot),
        )?;
        let subscription = json_subscription(&mappings)?;
        Ok(Self {
            metadata,
            mappings: mappings.into_boxed_slice(),
            limits,
            boot_snapshot,
            subscription,
        })
    }

    /// Returns immutable source metadata with an IEX-only, `DirectUnverified` ceiling.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Returns the official IEX WebSocket endpoint.
    pub const fn endpoint(&self) -> &'static str {
        ALPACA_IEX_ENDPOINT
    }

    /// Returns the admitted mappings. Their count never exceeds 30.
    pub fn mappings(&self) -> &[AlpacaInstrumentMapping] {
        &self.mappings
    }

    /// Returns the immutable frame and deadline policy used by this source.
    pub const fn transport_limits(&self) -> AlpacaTransportLimits {
        self.limits
    }

    /// Returns the secret-free resource projection for the mandatory REST bootstrap.
    pub fn boot_snapshot_policy(&self) -> AlpacaIexBootSnapshotPolicy {
        AlpacaIexBootSnapshotPolicy::from_transport_limits(self.limits)
    }

    pub(crate) const fn boot_snapshot_contract(&self) -> &AlpacaIexBootSnapshotContract {
        &self.boot_snapshot
    }

    pub(crate) fn subscription(&self) -> &str {
        &self.subscription
    }
}

/// Immutable MessagePack indicative-options live profile for Alpaca Basic.
#[derive(Clone, Debug)]
pub struct AlpacaOptionsLiveConfig {
    metadata: SourceMetadata,
    mappings: Box<[AlpacaOptionMapping]>,
    limits: AlpacaTransportLimits,
    subscription: Box<[u8]>,
}

impl AlpacaOptionsLiveConfig {
    /// Constructs indicative option trades and modified quotes coverage.
    ///
    /// The entire surface carries the conservative 15-minute delay declaration because Alpaca's
    /// indicative trades are delayed while its quotes are modified rather than OPRA observations.
    #[allow(
        clippy::too_many_arguments,
        reason = "source evidence and runtime bounds stay explicit"
    )]
    pub fn try_new(
        source_id: SourceId,
        revision_evidence: RevisionBoundPayloadEvidence,
        authorization: AuthorizationGrant,
        coverage_evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        mappings: Vec<AlpacaOptionMapping>,
        freshness: FreshnessPolicy,
        budget: ProviderBudgetPolicy,
        limits: AlpacaTransportLimits,
    ) -> Result<Self, AlpacaError> {
        validate_authorization_and_budget(&authorization, &budget)?;
        validate_option_mappings(&mappings)?;
        let metadata = live_metadata(
            source_id,
            revision_evidence,
            authorization,
            coverage_evidence,
            effective,
            &[],
            mappings.clone(),
            freshness,
            budget,
            LiveSurface::IndicativeOptions,
            None,
        )?;
        let subscription = messagepack_subscription(&mappings)?;
        Ok(Self {
            metadata,
            mappings: mappings.into_boxed_slice(),
            limits,
            subscription,
        })
    }

    /// Returns immutable source metadata with an `Indicative` ceiling.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Returns the official indicative-options WebSocket endpoint.
    pub const fn endpoint(&self) -> &'static str {
        ALPACA_OPTIONS_ENDPOINT
    }

    /// Returns admitted option mappings. Wildcards are structurally impossible.
    pub fn mappings(&self) -> &[AlpacaOptionMapping] {
        &self.mappings
    }

    /// Returns the immutable frame and deadline policy used by this source.
    pub const fn transport_limits(&self) -> AlpacaTransportLimits {
        self.limits
    }

    pub(crate) fn subscription(&self) -> &[u8] {
        &self.subscription
    }
}

/// Exact aggregation timeframe accepted by Alpaca historical bars.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlpacaTimeframe {
    unit: TimeframeUnit,
    multiple: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimeframeUnit {
    Minute,
    Hour,
    Day,
    Week,
    Month,
}

impl AlpacaTimeframe {
    /// Constructs a 1-59 minute aggregation.
    pub fn minutes(multiple: u8) -> Result<Self, AlpacaError> {
        Self::bounded(TimeframeUnit::Minute, multiple, 1..=59)
    }

    /// Constructs a 1-23 hour aggregation.
    pub fn hours(multiple: u8) -> Result<Self, AlpacaError> {
        Self::bounded(TimeframeUnit::Hour, multiple, 1..=23)
    }

    /// Constructs a daily aggregation.
    pub const fn day() -> Self {
        Self {
            unit: TimeframeUnit::Day,
            multiple: 1,
        }
    }

    /// Constructs a weekly aggregation.
    pub const fn week() -> Self {
        Self {
            unit: TimeframeUnit::Week,
            multiple: 1,
        }
    }

    /// Constructs a supported 1, 2, 3, 4, 6, or 12 month aggregation.
    pub fn months(multiple: u8) -> Result<Self, AlpacaError> {
        if !matches!(multiple, 1 | 2 | 3 | 4 | 6 | 12) {
            return Err(AlpacaError::InvalidHistoricalPlan);
        }
        Ok(Self {
            unit: TimeframeUnit::Month,
            multiple,
        })
    }

    fn bounded(
        unit: TimeframeUnit,
        multiple: u8,
        range: std::ops::RangeInclusive<u8>,
    ) -> Result<Self, AlpacaError> {
        if !range.contains(&multiple) {
            return Err(AlpacaError::InvalidHistoricalPlan);
        }
        Ok(Self { unit, multiple })
    }

    pub(crate) fn provider_value(self) -> String {
        match self.unit {
            TimeframeUnit::Minute => format!("{}Min", self.multiple),
            TimeframeUnit::Hour => format!("{}Hour", self.multiple),
            TimeframeUnit::Day => "1Day".to_owned(),
            TimeframeUnit::Week => "1Week".to_owned(),
            TimeframeUnit::Month => format!("{}Month", self.multiple),
        }
    }

    /// Returns the exact bounded provider timeframe identifier.
    pub fn provider_identifier(self) -> Result<SourceIdentifier, AlpacaError> {
        SourceIdentifier::try_from(self.provider_value()).map_err(Into::into)
    }
}

/// Alpaca historical corporate-action adjustment policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaAdjustment {
    /// Preserve raw provider bars.
    Raw,
    /// Apply split adjustments.
    Split,
    /// Apply cash-dividend adjustments.
    Dividend,
    /// Apply spin-off adjustments.
    SpinOff,
    /// Apply all provider-supported adjustments.
    All,
}

impl AlpacaAdjustment {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Split => "split",
            Self::Dividend => "dividend",
            Self::SpinOff => "spin-off",
            Self::All => "all",
        }
    }
}

/// Code-owned contiguous lookback admitted for one historical analysis request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlpacaHistoricalLookback {
    days: NonZeroU16,
}

impl AlpacaHistoricalLookback {
    /// Constructs a bounded lookback without accepting arbitrary start and end coordinates.
    pub fn try_from_days(days: u16) -> Result<Self, AlpacaError> {
        NonZeroU16::new(days)
            .filter(|days| {
                (ALPACA_HISTORICAL_MIN_LOOKBACK_DAYS..=ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS)
                    .contains(&days.get())
            })
            .map(|days| Self { days })
            .ok_or(AlpacaError::InvalidHistoricalPlan)
    }

    /// Returns the admitted whole-day lookback.
    pub const fn days(self) -> u16 {
        self.days.get()
    }
}

/// Stable provider timestamp and session-ruleset identity required before a plan is admitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaHistoricalSeriesSemantics {
    timestamp_basis: BarTimestampBasis,
    session: MarketBarSessionEvidence,
}

impl AlpacaHistoricalSeriesSemantics {
    /// Binds one provider timestamp convention to exact versioned session evidence.
    pub const fn new(
        timestamp_basis: BarTimestampBasis,
        session: MarketBarSessionEvidence,
    ) -> Self {
        Self {
            timestamp_basis,
            session,
        }
    }

    /// Returns which aggregation-period boundary the provider timestamp identifies.
    pub const fn timestamp_basis(&self) -> BarTimestampBasis {
        self.timestamp_basis
    }

    /// Returns the exact session rules required from the per-bar time authority.
    pub const fn session(&self) -> &MarketBarSessionEvidence {
        &self.session
    }
}

/// Bounded historical request coordinates admitted before any session identity is minted.
///
/// This value intentionally cannot be registered as a provider dataset. The authenticated
/// preflight must first retain the exact terminal pagination graph and returned provider
/// timestamps, after which the runtime can obtain exact calendar authority and bind final series
/// semantics without a placeholder identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaHistoricalEquityPreflightPlan {
    mapping: AlpacaInstrumentMapping,
    timeframe: AlpacaTimeframe,
    start: Timestamp,
    end: Timestamp,
    adjustment: AlpacaAdjustment,
    page_limit: NonZeroU16,
}

impl AlpacaHistoricalEquityPreflightPlan {
    /// Anchors a contiguous bounded lookback at the delayed-data boundary for `analysis_at`.
    ///
    /// Callers cannot provide independent start/end coordinates. Runtime extraction separately
    /// verifies the derived end against the current trusted historical-exclusion boundary.
    pub fn try_new(
        mapping: AlpacaInstrumentMapping,
        timeframe: AlpacaTimeframe,
        analysis_at: Timestamp,
        lookback: AlpacaHistoricalLookback,
        adjustment: AlpacaAdjustment,
    ) -> Result<Self, AlpacaError> {
        let exclusion = i64::try_from(ALPACA_HISTORICAL_EXCLUSION_NANOS)
            .map_err(|_| AlpacaError::InvalidHistoricalPlan)?;
        let end = analysis_at
            .checked_sub_nanos(exclusion)
            .map_err(|_| AlpacaError::InvalidHistoricalPlan)?;
        let lookback_nanos = u64::from(lookback.days())
            .checked_mul(NANOS_PER_DAY)
            .and_then(|nanos| i64::try_from(nanos).ok())
            .ok_or(AlpacaError::InvalidHistoricalPlan)?;
        let start = end
            .checked_sub_nanos(lookback_nanos)
            .map_err(|_| AlpacaError::InvalidHistoricalPlan)?;
        if start.unix_nanos() < HISTORICAL_FLOOR_UNIX_NANOS || end <= start {
            return Err(AlpacaError::InvalidHistoricalPlan);
        }
        Ok(Self {
            mapping,
            timeframe,
            start,
            end,
            adjustment,
            page_limit: NonZeroU16::new(HISTORICAL_PAGE_LIMIT).ok_or(AlpacaError::Protocol)?,
        })
    }

    /// Returns the exact provider/internal instrument mapping requested by this preflight.
    pub const fn mapping(&self) -> &AlpacaInstrumentMapping {
        &self.mapping
    }

    /// Returns the exact provider timeframe bound into this plan.
    pub const fn timeframe(&self) -> AlpacaTimeframe {
        self.timeframe
    }

    /// Returns the inclusive historical request start coordinate.
    pub const fn start(&self) -> Timestamp {
        self.start
    }

    /// Returns the inclusive historical request end coordinate.
    pub const fn end(&self) -> Timestamp {
        self.end
    }

    /// Returns the exact provider adjustment policy.
    pub const fn adjustment(&self) -> AlpacaAdjustment {
        self.adjustment
    }

    /// Returns the code-owned provider page ceiling.
    pub const fn page_limit(&self) -> u16 {
        self.page_limit.get()
    }
}

/// Final unregistered historical plan whose identities include exact composite calendar evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaHistoricalEquityDatasetPlan {
    mapping: AlpacaInstrumentMapping,
    timeframe: AlpacaTimeframe,
    start: Timestamp,
    end: Timestamp,
    adjustment: AlpacaAdjustment,
    series_semantics: AlpacaHistoricalSeriesSemantics,
    page_limit: NonZeroU16,
}

impl AlpacaHistoricalEquityDatasetPlan {
    /// Finalizes only an exact preflight plan with runtime-produced series semantics.
    pub fn bind_preflight(
        preflight: AlpacaHistoricalEquityPreflightPlan,
        series_semantics: AlpacaHistoricalSeriesSemantics,
    ) -> Self {
        Self {
            mapping: preflight.mapping,
            timeframe: preflight.timeframe,
            start: preflight.start,
            end: preflight.end,
            adjustment: preflight.adjustment,
            series_semantics,
            page_limit: preflight.page_limit,
        }
    }

    /// Returns the exact provider timeframe bound into this final plan.
    pub const fn timeframe(&self) -> AlpacaTimeframe {
        self.timeframe
    }

    /// Returns the inclusive historical request start coordinate.
    pub const fn start(&self) -> Timestamp {
        self.start
    }

    /// Returns the inclusive historical request end coordinate.
    pub const fn end(&self) -> Timestamp {
        self.end
    }

    /// Returns the stable provider timestamp and session-ruleset contract for this series.
    pub const fn series_semantics(&self) -> &AlpacaHistoricalSeriesSemantics {
        &self.series_semantics
    }
}

/// One source-generation-bound historical IEX request dataset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaHistoricalEquityDataset {
    dataset: SourceIdentifier,
    mapping: AlpacaInstrumentMapping,
    timeframe: AlpacaTimeframe,
    start: Timestamp,
    end: Timestamp,
    adjustment: AlpacaAdjustment,
    series_semantics: AlpacaHistoricalSeriesSemantics,
    page_limit: NonZeroU16,
}

impl AlpacaHistoricalEquityDataset {
    fn bind(
        metadata: &SourceMetadata,
        plan: AlpacaHistoricalEquityDatasetPlan,
    ) -> Result<Self, AlpacaError> {
        let dataset = provider_dataset_identifier(metadata, &plan)?;
        Ok(Self {
            dataset,
            mapping: plan.mapping,
            timeframe: plan.timeframe,
            start: plan.start,
            end: plan.end,
            adjustment: plan.adjustment,
            series_semantics: plan.series_semantics,
            page_limit: plan.page_limit,
        })
    }

    /// Returns the exact dataset namespace used by discovery.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    pub(crate) const fn mapping(&self) -> &AlpacaInstrumentMapping {
        &self.mapping
    }

    pub(crate) const fn timeframe(&self) -> AlpacaTimeframe {
        self.timeframe
    }

    pub(crate) const fn start(&self) -> Timestamp {
        self.start
    }

    pub(crate) const fn end(&self) -> Timestamp {
        self.end
    }

    pub(crate) const fn adjustment(&self) -> AlpacaAdjustment {
        self.adjustment
    }

    pub(crate) const fn series_semantics(&self) -> &AlpacaHistoricalSeriesSemantics {
        &self.series_semantics
    }

    pub(crate) const fn page_limit(&self) -> u16 {
        self.page_limit.get()
    }

    pub(crate) fn matches_preflight(
        &self,
        preflight: &AlpacaHistoricalEquityPreflightPlan,
    ) -> bool {
        self.mapping == preflight.mapping
            && self.timeframe == preflight.timeframe
            && self.start == preflight.start
            && self.end == preflight.end
            && self.adjustment == preflight.adjustment
            && self.page_limit == preflight.page_limit
    }

    pub(crate) fn verify_provider_identity(
        &self,
        metadata: &SourceMetadata,
    ) -> Result<(), AlpacaError> {
        if !has_strict_provider_dataset_grammar(&self.dataset)
            || provider_dataset_identifier_for_bound(metadata, self)? != self.dataset
        {
            return Err(AlpacaError::InvalidHistoricalPlan);
        }
        Ok(())
    }

    pub(crate) fn analytical_dataset_identifier(
        &self,
        metadata: &SourceMetadata,
        provider_instrument_id: &ProviderInstrumentId,
        currency: market_squawk_domain::Currency,
    ) -> Result<SourceIdentifier, AlpacaError> {
        self.verify_provider_identity(metadata)?;
        if provider_instrument_id.as_str() != self.mapping.symbol() {
            return Err(AlpacaError::InvalidCoverage);
        }
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/alpaca-historical-analytical-series/v1\0");
        hash_source_generation(&mut digest, metadata);
        digest.update(self.mapping.instrument().as_uuid().as_bytes());
        hash_str(&mut digest, provider_instrument_id.as_str());
        hash_str(&mut digest, IEX_VENUE);
        hash_str(&mut digest, "iex");
        hash_str(&mut digest, &self.timeframe.provider_value());
        hash_str(&mut digest, self.adjustment.as_str());
        digest.update([asset_class_tag(self.mapping.asset_class())]);
        hash_str(&mut digest, currency.as_str());
        digest.update([bar_timestamp_basis_tag(
            self.series_semantics.timestamp_basis(),
        )]);
        hash_session_coordinates(&mut digest, self.series_semantics.session());
        SourceIdentifier::try_from(format!(
            "{ANALYTICAL_DATASET_PREFIX}{}",
            encode_lower_hex(digest.finalize().into())
        ))
        .map_err(Into::into)
    }
}

/// Immutable extraction-only configuration for delayed IEX historical bars.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaHistoricalEquityConfig {
    metadata: SourceMetadata,
    datasets: BTreeMap<String, AlpacaHistoricalEquityDataset>,
    request_bounds: HttpRequestBounds,
}

impl AlpacaHistoricalEquityConfig {
    /// Constructs generation-wide delayed-IEX historical metadata without inventing a click
    /// window or historical knowledge time.
    ///
    /// The supplied mappings declare only the bounded instruments this generation may later
    /// admit. Each click still needs terminal provider pages, exact range-calendar authority, and
    /// first-observed availability before [`Self::try_bind_one_plan`] can construct a source.
    #[allow(
        clippy::too_many_arguments,
        reason = "source evidence and request bounds stay explicit"
    )]
    pub fn try_parent_metadata(
        source_id: SourceId,
        revision_evidence: RevisionBoundPayloadEvidence,
        authorization: AuthorizationGrant,
        coverage_evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        mappings: Vec<AlpacaInstrumentMapping>,
        freshness: FreshnessPolicy,
        budget: ProviderBudgetPolicy,
        request_bounds: HttpRequestBounds,
    ) -> Result<SourceMetadata, AlpacaError> {
        validate_authorization_and_budget(&authorization, &budget)?;
        validate_equity_mappings(&mappings, 4_096)?;
        historical_parent_metadata(
            source_id,
            revision_evidence,
            authorization,
            coverage_evidence,
            effective,
            &mappings,
            freshness,
            budget,
            request_bounds,
        )
    }

    /// Constructs an extraction-only IEX bars profile.
    #[allow(
        clippy::too_many_arguments,
        reason = "source evidence and request bounds stay explicit"
    )]
    pub fn try_new(
        source_id: SourceId,
        revision_evidence: RevisionBoundPayloadEvidence,
        authorization: AuthorizationGrant,
        coverage_evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        datasets: Vec<AlpacaHistoricalEquityDatasetPlan>,
        freshness: FreshnessPolicy,
        budget: ProviderBudgetPolicy,
        request_bounds: HttpRequestBounds,
    ) -> Result<Self, AlpacaError> {
        validate_authorization_and_budget(&authorization, &budget)?;
        if datasets.is_empty() || datasets.len() > 4_096 {
            return Err(AlpacaError::InvalidCoverage);
        }
        let mut mappings = Vec::new();
        mappings
            .try_reserve_exact(datasets.len())
            .map_err(|_| AlpacaError::Allocation)?;
        for dataset in &datasets {
            if !mappings
                .iter()
                .any(|mapping: &AlpacaInstrumentMapping| mapping == &dataset.mapping)
            {
                mappings.push(dataset.mapping.clone());
            }
        }
        let metadata = Self::try_parent_metadata(
            source_id,
            revision_evidence,
            authorization,
            coverage_evidence,
            effective,
            mappings,
            freshness,
            budget,
            request_bounds,
        )?;
        let mut by_id = BTreeMap::new();
        for plan in datasets {
            let dataset = AlpacaHistoricalEquityDataset::bind(&metadata, plan)?;
            if by_id
                .insert(dataset.dataset().as_str().to_owned(), dataset)
                .is_some()
            {
                return Err(AlpacaError::InvalidCoverage);
            }
        }
        Ok(Self {
            metadata,
            datasets: by_id,
            request_bounds,
        })
    }

    /// Binds exactly one click-specific plan beneath immutable generation-wide source metadata.
    ///
    /// The parent metadata is retained byte-for-byte. It must already declare the exact delayed
    /// IEX historical surface, request bounds, shared account budget, and affirmative membership
    /// for the plan's internal instrument. The click window therefore changes only the provider
    /// dataset identity and never manufactures a new source generation or profile.
    pub fn try_bind_one_plan(
        parent_metadata: SourceMetadata,
        plan: AlpacaHistoricalEquityDatasetPlan,
        request_bounds: HttpRequestBounds,
    ) -> Result<Self, AlpacaError> {
        validate_historical_parent_metadata(&parent_metadata, &plan, request_bounds)?;
        let dataset = AlpacaHistoricalEquityDataset::bind(&parent_metadata, plan)?;
        let mut datasets = BTreeMap::new();
        datasets.insert(dataset.dataset().as_str().to_owned(), dataset);
        Ok(Self {
            metadata: parent_metadata,
            datasets,
            request_bounds,
        })
    }

    /// Validates immutable generation-wide metadata before the long-lived source is claimed.
    ///
    /// Instrument membership and plan-window effectiveness are checked separately by
    /// [`Self::try_bind_one_plan`] because they are exact per-click coordinates.
    pub fn validate_parent_metadata(
        metadata: &SourceMetadata,
        request_bounds: HttpRequestBounds,
    ) -> Result<(), AlpacaError> {
        validate_historical_parent_surface(metadata, request_bounds)
    }

    /// Returns immutable delayed, extraction-only metadata.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Returns every exact source-generation- and window-bound provider dataset identity.
    pub fn provider_dataset_identifiers(&self) -> impl ExactSizeIterator<Item = &SourceIdentifier> {
        self.datasets
            .values()
            .map(AlpacaHistoricalEquityDataset::dataset)
    }

    pub(crate) fn dataset(
        &self,
        identifier: &SourceIdentifier,
    ) -> Option<&AlpacaHistoricalEquityDataset> {
        self.datasets.get(identifier.as_str())
    }

    pub(crate) fn datasets(&self) -> impl ExactSizeIterator<Item = &AlpacaHistoricalEquityDataset> {
        self.datasets.values()
    }

    pub(crate) const fn request_bounds(&self) -> HttpRequestBounds {
        self.request_bounds
    }
}

fn validate_historical_parent_metadata(
    metadata: &SourceMetadata,
    plan: &AlpacaHistoricalEquityDatasetPlan,
    request_bounds: HttpRequestBounds,
) -> Result<(), AlpacaError> {
    validate_historical_parent_surface(metadata, request_bounds)?;
    let coverage = metadata.coverage();
    let membership = coverage.instruments().membership(plan.mapping.instrument());
    if !coverage
        .asset_classes()
        .contains(&plan.mapping.asset_class())
        || !matches!(
            membership,
            InstrumentCoverageMembership::Enumerated
                | InstrumentCoverageMembership::EvidenceBackedUniverse
        )
        || !coverage.is_effective_at(plan.start)
        || !coverage.is_effective_at(plan.end)
    {
        return Err(AlpacaError::InvalidCoverage);
    }
    Ok(())
}

fn validate_historical_parent_surface(
    metadata: &SourceMetadata,
    request_bounds: HttpRequestBounds,
) -> Result<(), AlpacaError> {
    let iex = VenueId::try_from(IEX_VENUE)?;
    let coverage = metadata.coverage();
    let expected_network =
        NetworkAccessPolicy::Allowlisted(historical_endpoint_policy(request_bounds)?);
    let expected_capabilities = SourceCapabilities::new(
        false,
        true,
        SequenceCapability::Unsupported,
        ChecksumCapability::Unsupported,
        HistoricalCapability::Historical,
        false,
    );
    if metadata.provider().as_str() != ALPACA_PROVIDER
        || metadata.source_class() != SourceClass::Broker
        || metadata.authorization().mode() != AuthorizationMode::UserAuthorized
        || metadata.quality_ceiling() != DataQuality::Aggregated
        || coverage.domain() != CoverageDomain::Instruments
        || coverage
            .asset_classes()
            .iter()
            .any(|asset_class| !matches!(asset_class, AssetClass::Equity | AssetClass::Fund))
        || !coverage.topology().is_partial()
        || coverage.topology().venues() != [iex]
        || coverage.live().is_some()
        || coverage.delay() != CoverageDelay::Delayed(ALPACA_HISTORICAL_EXCLUSION_NANOS)
        || coverage.delivery() != DeliveryEvidence::AuthorizedBroker
        || metadata.network_policy() != &expected_network
        || metadata.capabilities() != expected_capabilities
        || metadata.protocol_profile() != &SourceProtocolProfile::NotLive
    {
        return Err(AlpacaError::InvalidCoverage);
    }
    let budget = metadata.budget_policy().ok_or(AlpacaError::InvalidBudget)?;
    validate_authorization_and_budget(metadata.authorization(), budget)
}

#[allow(
    clippy::too_many_arguments,
    reason = "source evidence and request bounds stay explicit"
)]
fn historical_parent_metadata(
    source_id: SourceId,
    revision_evidence: RevisionBoundPayloadEvidence,
    authorization: AuthorizationGrant,
    coverage_evidence: ExactPayloadEvidence,
    effective: EffectiveInterval,
    mappings: &[AlpacaInstrumentMapping],
    freshness: FreshnessPolicy,
    budget: ProviderBudgetPolicy,
    request_bounds: HttpRequestBounds,
) -> Result<SourceMetadata, AlpacaError> {
    let instruments = mappings
        .iter()
        .map(AlpacaInstrumentMapping::instrument)
        .collect::<Vec<_>>();
    let mut asset_classes = Vec::new();
    for mapping in mappings {
        let asset_class = mapping.asset_class();
        if !asset_classes.contains(&asset_class) {
            asset_classes.push(asset_class);
        }
    }
    let provider = SourceIdentifier::try_from(ALPACA_PROVIDER)?;
    SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        source_id,
        revision_evidence,
        SourceClass::Broker,
        provider,
        authorization,
        SourceCoverage::try_instrument(
            coverage_evidence,
            effective,
            asset_classes,
            CoverageTopology::partial_venues(vec![VenueId::try_from(IEX_VENUE)?])?,
            InstrumentCoverage::enumerated(instruments)?,
            None,
            CoverageDelay::Delayed(ALPACA_HISTORICAL_EXCLUSION_NANOS),
            DeliveryEvidence::AuthorizedBroker,
        )?,
        DataQuality::Aggregated,
        NetworkAccessPolicy::Allowlisted(historical_endpoint_policy(request_bounds)?),
        freshness,
        Some(budget),
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::Historical,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))
    .map_err(Into::into)
}

fn provider_dataset_identifier(
    metadata: &SourceMetadata,
    plan: &AlpacaHistoricalEquityDatasetPlan,
) -> Result<SourceIdentifier, AlpacaError> {
    provider_dataset_identifier_from_parts(
        metadata,
        &plan.mapping,
        plan.timeframe,
        plan.start,
        plan.end,
        plan.adjustment,
        &plan.series_semantics,
    )
}

fn provider_dataset_identifier_for_bound(
    metadata: &SourceMetadata,
    dataset: &AlpacaHistoricalEquityDataset,
) -> Result<SourceIdentifier, AlpacaError> {
    provider_dataset_identifier_from_parts(
        metadata,
        &dataset.mapping,
        dataset.timeframe,
        dataset.start,
        dataset.end,
        dataset.adjustment,
        &dataset.series_semantics,
    )
}

fn provider_dataset_identifier_from_parts(
    metadata: &SourceMetadata,
    mapping: &AlpacaInstrumentMapping,
    timeframe: AlpacaTimeframe,
    start: Timestamp,
    end: Timestamp,
    adjustment: AlpacaAdjustment,
    series_semantics: &AlpacaHistoricalSeriesSemantics,
) -> Result<SourceIdentifier, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-historical-provider-dataset/v1\0");
    hash_source_generation(&mut digest, metadata);
    digest.update(mapping.instrument().as_uuid().as_bytes());
    hash_str(&mut digest, mapping.symbol());
    hash_str(&mut digest, IEX_VENUE);
    hash_str(&mut digest, "iex");
    hash_str(&mut digest, &timeframe.provider_value());
    hash_str(&mut digest, adjustment.as_str());
    hash_timestamp(&mut digest, start);
    hash_timestamp(&mut digest, end);
    digest.update([bar_timestamp_basis_tag(series_semantics.timestamp_basis())]);
    hash_session_coordinates(&mut digest, series_semantics.session());
    SourceIdentifier::try_from(format!(
        "{PROVIDER_DATASET_PREFIX}{}",
        encode_lower_hex(digest.finalize().into())
    ))
    .map_err(Into::into)
}

fn has_strict_provider_dataset_grammar(dataset: &SourceIdentifier) -> bool {
    dataset
        .as_str()
        .strip_prefix(PROVIDER_DATASET_PREFIX)
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}

fn hash_source_generation(hash: &mut Sha256, metadata: &SourceMetadata) {
    hash.update(metadata.schema_version().get().to_be_bytes());
    hash_str(hash, metadata.source_id().as_str());
    hash_str(hash, metadata.revision().as_source_identifier().as_str());
    hash_exact_evidence(hash, metadata.revision_evidence().payload_evidence());
    hash_str(hash, metadata.provider().as_str());
    hash_exact_evidence(hash, metadata.coverage().evidence());
    hash_effective_interval(hash, metadata.coverage().effective_interval());
    let authorization = metadata.authorization();
    hash.update([authorization_mode_tag(authorization.mode())]);
    hash_str(hash, authorization.basis().as_source_identifier().as_str());
    hash_exact_evidence(hash, authorization.evidence());
    hash_effective_interval(hash, authorization.effective_interval());
}

fn hash_effective_interval(hash: &mut Sha256, effective: EffectiveInterval) {
    hash_timestamp(hash, effective.starts_at());
    match effective.ends_at() {
        Some(end) => {
            hash.update([1]);
            hash_timestamp(hash, end);
        }
        None => hash.update([0]),
    }
}

fn hash_exact_evidence(hash: &mut Sha256, evidence: &ExactPayloadEvidence) {
    hash_evidence(hash, evidence.content_digest());
}

fn hash_evidence(hash: &mut Sha256, evidence: EvidenceDigest) {
    hash.update([digest_algorithm_tag(evidence.algorithm())]);
    hash.update(evidence.bytes());
}

fn hash_session_coordinates(hash: &mut Sha256, session: &MarketBarSessionEvidence) {
    hash.update([match session.kind() {
        market_squawk_domain::MarketBarSessionKind::Regular => 1,
        market_squawk_domain::MarketBarSessionKind::Extended => 2,
        market_squawk_domain::MarketBarSessionKind::Continuous => 3,
        market_squawk_domain::MarketBarSessionKind::ProviderDefined => 4,
    }]);
    hash_str(hash, session.ruleset().as_str());
    hash_evidence(hash, session.evidence());
}

fn hash_str(hash: &mut Sha256, value: &str) {
    let length = match u64::try_from(value.len()) {
        Ok(length) => length,
        Err(_) => u64::MAX,
    };
    hash.update(length.to_be_bytes());
    hash.update(value.as_bytes());
}

fn hash_timestamp(hash: &mut Sha256, value: Timestamp) {
    hash.update(value.unix_nanos().to_be_bytes());
}

fn encode_lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

const fn authorization_mode_tag(mode: AuthorizationMode) -> u8 {
    match mode {
        AuthorizationMode::PublicInterface => 1,
        AuthorizationMode::UserAuthorized => 2,
        AuthorizationMode::Licensed => 3,
        AuthorizationMode::UserOwnedLocal => 4,
    }
}

const fn digest_algorithm_tag(algorithm: DigestAlgorithm) -> u8 {
    match algorithm {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }
}

const fn asset_class_tag(asset_class: AssetClass) -> u8 {
    match asset_class {
        AssetClass::Equity => 1,
        AssetClass::FixedIncome => 2,
        AssetClass::Option => 3,
        AssetClass::Future => 4,
        AssetClass::ForeignExchange => 5,
        AssetClass::Crypto => 6,
        AssetClass::Commodity => 7,
        AssetClass::Fund => 8,
        AssetClass::Index => 9,
        AssetClass::Cash => 10,
    }
}

const fn bar_timestamp_basis_tag(basis: BarTimestampBasis) -> u8 {
    match basis {
        BarTimestampBasis::PeriodStart => 1,
        BarTimestampBasis::PeriodEnd => 2,
    }
}

#[derive(Clone, Copy)]
enum LiveSurface {
    Iex,
    IndicativeOptions,
}

#[allow(
    clippy::too_many_arguments,
    reason = "metadata evidence dimensions stay explicit"
)]
fn live_metadata(
    source_id: SourceId,
    revision_evidence: RevisionBoundPayloadEvidence,
    authorization: AuthorizationGrant,
    coverage_evidence: ExactPayloadEvidence,
    effective: EffectiveInterval,
    equities: &[AlpacaInstrumentMapping],
    options: Vec<AlpacaOptionMapping>,
    freshness: FreshnessPolicy,
    budget: ProviderBudgetPolicy,
    surface: LiveSurface,
    boot_snapshot: Option<&AlpacaIexBootSnapshotContract>,
) -> Result<SourceMetadata, AlpacaError> {
    let (endpoint, venue, assets, instruments, product, channel, quality, delay, delivery, events) =
        match surface {
            LiveSurface::Iex => (
                ALPACA_IEX_ENDPOINT,
                IEX_VENUE,
                distinct_assets(equities),
                equities
                    .iter()
                    .map(AlpacaInstrumentMapping::instrument)
                    .collect(),
                "alpaca-basic-iex-configured-symbols-v1",
                "trades+quotes+statuses",
                DataQuality::DirectUnverified,
                CoverageDelay::RealTime,
                DeliveryEvidence::AuthorizedBroker,
                vec![
                    LiveEventClass::Trade,
                    LiveEventClass::Quote,
                    LiveEventClass::TradingHalt,
                ],
            ),
            LiveSurface::IndicativeOptions => (
                ALPACA_OPTIONS_ENDPOINT,
                INDICATIVE_OPTIONS_VENUE,
                vec![AssetClass::Option],
                options
                    .iter()
                    .map(AlpacaOptionMapping::instrument)
                    .collect(),
                "alpaca-basic-indicative-options-configured-symbols-v1",
                "trades+quotes-msgpack",
                DataQuality::Indicative,
                CoverageDelay::Delayed(ALPACA_HISTORICAL_EXCLUSION_NANOS),
                DeliveryEvidence::Indirect,
                vec![LiveEventClass::Trade, LiveEventClass::Quote],
            ),
        };
    let no_snapshot = rule("alpaca-non-book-snapshot-not-applicable-v1")?;
    let rules = events
        .into_iter()
        .map(|event| {
            LiveCoverageRule::try_new(
                event,
                None,
                SnapshotApplicability::NotApplicable {
                    metadata_rule: no_snapshot.clone(),
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let live = LiveCoverageDeclaration::try_new(
        ProviderProduct::new(SourceIdentifier::try_from(product)?),
        ProviderChannel::new(SourceIdentifier::try_from(channel)?),
        rules,
    )?;
    let provider = SourceIdentifier::try_from(ALPACA_PROVIDER)?;
    let topology = match surface {
        LiveSurface::Iex => CoverageTopology::partial_venues(vec![VenueId::try_from(venue)?])?,
        LiveSurface::IndicativeOptions => CoverageTopology::single_venue(VenueId::try_from(venue)?),
    };
    let endpoint_policy = match (surface, boot_snapshot) {
        (LiveSurface::Iex, Some(boot_snapshot)) => {
            market_squawk_sources::EndpointPolicy::try_new_combined(
                [endpoint],
                vec![boot_snapshot.endpoint_rule().clone()],
                boot_snapshot.request_bounds(),
            )?
        }
        (LiveSurface::IndicativeOptions, None) => {
            market_squawk_sources::EndpointPolicy::try_new([endpoint])?
        }
        _ => return Err(AlpacaError::Protocol),
    };
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        source_id,
        revision_evidence,
        SourceClass::Broker,
        provider,
        authorization,
        SourceCoverage::try_instrument(
            coverage_evidence,
            effective,
            assets,
            topology,
            InstrumentCoverage::enumerated(instruments)?,
            Some(live),
            delay,
            delivery,
        )?,
        quality,
        NetworkAccessPolicy::Allowlisted(endpoint_policy),
        freshness,
        Some(budget),
        SourceCapabilities::new(
            true,
            false,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::None,
            true,
        ),
        SourceProtocolProfile::Live(Box::new(LiveProtocolProfile::new(
            rule(match surface {
                LiveSurface::Iex => "alpaca-iex-json-v3-boot-snapshot-decoder",
                LiveSurface::IndicativeOptions => "alpaca-indicative-options-msgpack-v1-decoder",
            })?,
            SemanticInterpretationProfile::new(
                rule("alpaca-trade-aggressor-unavailable")?,
                rule("alpaca-auction-unused")?,
                rule("alpaca-trading-status-codes-v1")?,
                rule("alpaca-corporate-action-unused")?,
            ),
            rule("alpaca-rfc3339-nanosecond-timestamp")?,
            SequenceValidationProfile::Unsupported {
                rule: rule("alpaca-sequence-unsupported")?,
            },
            ChecksumValidationProfile::Unsupported {
                rule: rule("alpaca-checksum-unsupported")?,
            },
            true,
            ProviderNumericPolicy::ExactDecimalLexeme,
        ))),
    ))?)
}

fn distinct_assets(mappings: &[AlpacaInstrumentMapping]) -> Vec<AssetClass> {
    let mut assets = Vec::new();
    for mapping in mappings {
        if !assets.contains(&mapping.asset_class()) {
            assets.push(mapping.asset_class());
        }
    }
    assets
}

fn validate_authorization_and_budget(
    authorization: &AuthorizationGrant,
    budget: &ProviderBudgetPolicy,
) -> Result<(), AlpacaError> {
    if authorization.mode() != AuthorizationMode::UserAuthorized {
        return Err(AlpacaError::InvalidAuthorization);
    }
    let retains_application_window = (0..budget.window_count()).any(|index| {
        budget.window(index).is_some_and(|window| {
            window.requests_per_window() == ALPACA_APPLICATION_MAX_REQUESTS_PER_MINUTE
                && window.window_nanos() == NANOS_PER_MINUTE
        })
    });
    if !retains_application_window {
        return Err(AlpacaError::InvalidBudget);
    }
    Ok(())
}

fn validate_equity_mappings(
    mappings: &[AlpacaInstrumentMapping],
    maximum: usize,
) -> Result<(), AlpacaError> {
    if mappings.is_empty() || mappings.len() > maximum {
        return Err(AlpacaError::SubscriptionLimit);
    }
    let mut symbols = BTreeSet::new();
    let mut instruments = BTreeSet::new();
    for mapping in mappings {
        if !symbols.insert(mapping.symbol()) || !instruments.insert(mapping.instrument()) {
            return Err(AlpacaError::InvalidCoverage);
        }
    }
    Ok(())
}

fn validate_option_mappings(mappings: &[AlpacaOptionMapping]) -> Result<(), AlpacaError> {
    if mappings.is_empty() || mappings.len() > ALPACA_BASIC_OPTION_SYMBOL_LIMIT {
        return Err(AlpacaError::SubscriptionLimit);
    }
    let mut symbols = BTreeSet::new();
    let mut instruments = BTreeSet::new();
    for mapping in mappings {
        if !symbols.insert(mapping.symbol()) || !instruments.insert(mapping.instrument()) {
            return Err(AlpacaError::InvalidCoverage);
        }
    }
    Ok(())
}

fn validate_equity_symbol(symbol: &str) -> Result<(), AlpacaError> {
    if symbol.is_empty()
        || symbol.len() > MAX_SYMBOL_BYTES
        || symbol == "*"
        || !symbol.bytes().all(|byte| {
            byte.is_ascii_uppercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
    {
        return Err(AlpacaError::InvalidCoverage);
    }
    Ok(())
}

fn validate_option_symbol(symbol: &str) -> Result<(), AlpacaError> {
    if symbol.len() < 16 || symbol.len() > 21 || symbol == "*" {
        return Err(AlpacaError::InvalidCoverage);
    }
    let split = symbol.len().saturating_sub(15);
    let (root, contract) = symbol.split_at(split);
    let bytes = contract.as_bytes();
    if root.is_empty()
        || root.len() > 6
        || !root
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        || !bytes[..6].iter().all(u8::is_ascii_digit)
        || !matches!(bytes[6], b'C' | b'P')
        || !bytes[7..].iter().all(u8::is_ascii_digit)
    {
        return Err(AlpacaError::InvalidCoverage);
    }
    Ok(())
}

#[derive(Serialize)]
struct Subscription<'a> {
    action: &'static str,
    trades: Vec<&'a str>,
    quotes: Vec<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    statuses: Option<Vec<&'a str>>,
}

fn json_subscription(mappings: &[AlpacaInstrumentMapping]) -> Result<Box<str>, AlpacaError> {
    let symbols = mappings
        .iter()
        .map(AlpacaInstrumentMapping::symbol)
        .collect::<Vec<_>>();
    let payload = serde_json::to_string(&Subscription {
        action: "subscribe",
        trades: symbols.clone(),
        quotes: symbols.clone(),
        statuses: Some(symbols),
    })
    .map_err(|_| AlpacaError::Serialization)?;
    if payload.len() > MAX_SUBSCRIPTION_BYTES {
        return Err(AlpacaError::SubscriptionLimit);
    }
    Ok(payload.into_boxed_str())
}

fn messagepack_subscription(mappings: &[AlpacaOptionMapping]) -> Result<Box<[u8]>, AlpacaError> {
    let symbols = mappings
        .iter()
        .map(AlpacaOptionMapping::symbol)
        .collect::<Vec<_>>();
    let payload = rmp_serde::to_vec_named(&Subscription {
        action: "subscribe",
        trades: symbols.clone(),
        quotes: symbols,
        statuses: None,
    })
    .map_err(|_| AlpacaError::Serialization)?;
    if payload.len() > MAX_SUBSCRIPTION_BYTES {
        return Err(AlpacaError::SubscriptionLimit);
    }
    Ok(payload.into_boxed_slice())
}

fn historical_endpoint_policy(
    bounds: HttpRequestBounds,
) -> Result<market_squawk_sources::EndpointPolicy, AlpacaError> {
    let public = |key: &str, max| {
        QueryParameterRule::try_new(
            SourceIdentifier::try_from(key)?,
            max,
            false,
            QuerySensitivity::Public,
        )
        .map_err(AlpacaError::from)
    };
    let rules = vec![
        public("timeframe", 16)?,
        public("start", 40)?,
        public("end", 40)?,
        public("limit", 5)?,
        public("adjustment", 16)?,
        QueryParameterRule::try_new_exact_public(
            SourceIdentifier::try_from("feed")?,
            SourceIdentifier::try_from("iex")?,
        )?,
        QueryParameterRule::try_new_exact_public(
            SourceIdentifier::try_from("sort")?,
            SourceIdentifier::try_from("asc")?,
        )?,
        public("page_token", 256)?,
    ];
    let endpoint = ApiEndpointRule::try_new(
        ALPACA_STOCKS_BASE_ENDPOINT,
        PathScope::Descendants,
        rules,
        8,
        1_024,
    )?;
    Ok(market_squawk_sources::EndpointPolicy::try_from_api_rules(
        vec![endpoint],
        bounds,
    )?)
}

fn rule(value: &str) -> Result<IntegrityRule, AlpacaError> {
    let version = RuleVersion::new(1).map_err(|_| AlpacaError::InvalidCoverage)?;
    Ok(IntegrityRule::new(
        SourceIdentifier::try_from(value)?,
        version,
    ))
}
