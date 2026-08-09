use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::time::Duration;

use market_squawk_domain::{
    AssetClass, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    EffectiveInterval, ExactPayloadEvidence, InstrumentId, IntegrityRule, LiveEventClass,
    ProviderChannel, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion, SchemaVersion,
    SequenceCapability, SnapshotApplicability, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationGrant, AuthorizationMode, ChecksumValidationProfile,
    CoverageTopology, FreshnessPolicy, HistoricalCapability, HttpRequestBounds, InstrumentCoverage,
    LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy, PathScope,
    ProviderBudgetPolicy, ProviderNumericPolicy, QueryParameterRule, QuerySensitivity,
    SemanticInterpretationProfile, SequenceValidationProfile, SourceCapabilities, SourceClass,
    SourceCoverage, SourceMetadata, SourceMetadataInput, SourceProtocolProfile,
};
use serde::Serialize;

use crate::AlpacaError;

/// Alpaca Basic real-time equity WebSocket symbol ceiling.
pub const ALPACA_BASIC_EQUITY_SYMBOL_LIMIT: usize = 30;
/// Alpaca Basic indicative-option quote WebSocket symbol ceiling.
pub const ALPACA_BASIC_OPTION_SYMBOL_LIMIT: usize = 200;
/// Alpaca Basic historical request ceiling.
pub const ALPACA_BASIC_HISTORICAL_REQUESTS_PER_MINUTE: u32 = 200;
/// Alpaca Basic historical exclusion window, in nanoseconds.
pub const ALPACA_HISTORICAL_EXCLUSION_NANOS: u64 = 900_000_000_000;

pub(crate) const ALPACA_PROVIDER: &str = "alpaca-market-data";
pub(crate) const ALPACA_IEX_ENDPOINT: &str = "wss://stream.data.alpaca.markets/v2/iex";
pub(crate) const ALPACA_OPTIONS_ENDPOINT: &str =
    "wss://stream.data.alpaca.markets/v1beta1/indicative";
pub(crate) const ALPACA_STOCKS_BASE_ENDPOINT: &str = "https://data.alpaca.markets/v2/stocks";
pub(crate) const IEX_VENUE: &str = "iex";
pub(crate) const INDICATIVE_OPTIONS_VENUE: &str = "alpaca-indicative-options";

const MAX_SYMBOL_BYTES: usize = 32;
const MAX_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_SUBSCRIPTION_BYTES: usize = 64 * 1024;
const HISTORICAL_FLOOR_UNIX_NANOS: i64 = 1_451_606_400_000_000_000;
const HISTORICAL_PAGE_LIMIT: u16 = 10_000;
const NANOS_PER_MINUTE: u64 = 60_000_000_000;

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

/// Immutable authenticated IEX live profile for Alpaca Basic.
#[derive(Clone, Debug)]
pub struct AlpacaIexLiveConfig {
    metadata: SourceMetadata,
    mappings: Box<[AlpacaInstrumentMapping]>,
    limits: AlpacaTransportLimits,
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
        )?;
        let subscription = json_subscription(&mappings)?;
        Ok(Self {
            metadata,
            mappings: mappings.into_boxed_slice(),
            limits,
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

/// One bounded historical IEX bar dataset registered with the extraction source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaHistoricalEquityDataset {
    dataset: SourceIdentifier,
    mapping: AlpacaInstrumentMapping,
    timeframe: AlpacaTimeframe,
    start: Timestamp,
    end: Timestamp,
    adjustment: AlpacaAdjustment,
    page_limit: NonZeroU16,
}

impl AlpacaHistoricalEquityDataset {
    /// Constructs a deterministic bounded query plan.
    #[allow(
        clippy::too_many_arguments,
        reason = "historical query identity stays explicit"
    )]
    pub fn try_new(
        dataset: SourceIdentifier,
        mapping: AlpacaInstrumentMapping,
        timeframe: AlpacaTimeframe,
        start: Timestamp,
        end: Timestamp,
        adjustment: AlpacaAdjustment,
        page_limit: NonZeroU16,
    ) -> Result<Self, AlpacaError> {
        if start.unix_nanos() < HISTORICAL_FLOOR_UNIX_NANOS
            || end <= start
            || page_limit.get() > HISTORICAL_PAGE_LIMIT
        {
            return Err(AlpacaError::InvalidHistoricalPlan);
        }
        Ok(Self {
            dataset,
            mapping,
            timeframe,
            start,
            end,
            adjustment,
            page_limit,
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

    pub(crate) const fn page_limit(&self) -> u16 {
        self.page_limit.get()
    }
}

/// Immutable extraction-only configuration for delayed IEX historical bars.
#[derive(Clone, Debug)]
pub struct AlpacaHistoricalEquityConfig {
    metadata: SourceMetadata,
    datasets: BTreeMap<String, AlpacaHistoricalEquityDataset>,
    request_bounds: HttpRequestBounds,
}

impl AlpacaHistoricalEquityConfig {
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
        datasets: Vec<AlpacaHistoricalEquityDataset>,
        freshness: FreshnessPolicy,
        budget: ProviderBudgetPolicy,
        request_bounds: HttpRequestBounds,
    ) -> Result<Self, AlpacaError> {
        validate_authorization_and_budget(&authorization, &budget)?;
        if datasets.is_empty() || datasets.len() > 4_096 {
            return Err(AlpacaError::InvalidCoverage);
        }
        let mut by_id = BTreeMap::new();
        for dataset in datasets {
            if by_id
                .insert(dataset.dataset().as_str().to_owned(), dataset)
                .is_some()
            {
                return Err(AlpacaError::InvalidCoverage);
            }
        }
        let instruments = by_id
            .values()
            .map(|dataset| dataset.mapping().instrument())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut asset_classes = Vec::new();
        for dataset in by_id.values() {
            let asset_class = dataset.mapping().asset_class();
            if !asset_classes.contains(&asset_class) {
                asset_classes.push(asset_class);
            }
        }
        let provider = SourceIdentifier::try_from(ALPACA_PROVIDER)?;
        let metadata = SourceMetadata::try_new(SourceMetadataInput::new(
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
        ))?;
        Ok(Self {
            metadata,
            datasets: by_id,
            request_bounds,
        })
    }

    /// Returns immutable delayed, extraction-only metadata.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    pub(crate) fn dataset(
        &self,
        identifier: &SourceIdentifier,
    ) -> Option<&AlpacaHistoricalEquityDataset> {
        self.datasets.get(identifier.as_str())
    }

    pub(crate) const fn request_bounds(&self) -> HttpRequestBounds {
        self.request_bounds
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
        NetworkAccessPolicy::Allowlisted(market_squawk_sources::EndpointPolicy::try_new([
            endpoint,
        ])?),
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
                LiveSurface::Iex => "alpaca-iex-json-v2-decoder",
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
    let retains_basic_window = (0..budget.window_count()).any(|index| {
        budget.window(index).is_some_and(|window| {
            window.requests_per_window() == ALPACA_BASIC_HISTORICAL_REQUESTS_PER_MINUTE
                && window.window_nanos() == NANOS_PER_MINUTE
        })
    });
    if !retains_basic_window {
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
