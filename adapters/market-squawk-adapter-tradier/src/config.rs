use std::collections::BTreeSet;
use std::time::Duration;

use market_squawk_domain::{
    AssetClass, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    EffectiveInterval, ExactPayloadEvidence, InstrumentId, IntegrityRule, LiveEventClass,
    MarketDepth, ProviderChannel, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion,
    SchemaVersion, SequenceCapability, SnapshotApplicability, SourceId, SourceIdentifier, VenueId,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationGrant, AuthorizationMode, ChecksumValidationProfile,
    CoverageTopology, FreshnessPolicy, HistoricalCapability, HttpRequestBounds, InstrumentCoverage,
    LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy, PathScope,
    ProviderBudgetPolicy, ProviderNumericPolicy, QueryParameterRule, QuerySensitivity,
    SemanticInterpretationProfile, SequenceValidationProfile, SourceCapabilities, SourceClass,
    SourceCoverage, SourceMetadata, SourceMetadataError, SourceMetadataInput,
    SourceProtocolProfile,
};
use thiserror::Error;

/// Production Brokerage REST endpoint used to create one short-lived market-stream session.
pub const TRADIER_MARKET_SESSION_ENDPOINT: &str =
    "https://api.tradier.com/v1/markets/events/session";
/// Production WebSocket endpoint for consolidated equity, ETF, and option events.
pub const TRADIER_WEBSOCKET_ENDPOINT: &str = "wss://ws.tradier.com/v1/markets/events";
/// Production bounded multi-symbol quote endpoint.
pub const TRADIER_QUOTES_ENDPOINT: &str = "https://api.tradier.com/v1/markets/quotes";
/// Production bounded option-chain endpoint.
pub const TRADIER_OPTIONS_CHAIN_ENDPOINT: &str =
    "https://api.tradier.com/v1/markets/options/chains";

pub(crate) const TRADIER_PROVIDER: &str = "tradier-brokerage";
pub(crate) const TRADIER_CONSOLIDATED_VENUE: &str = "tradier-consolidated-us";
pub(crate) const TRADIER_DERIVED_INDEX_VENUE: &str = "tradier-derived-index";
pub(crate) const MAX_STREAM_SYMBOLS: usize = 256;
pub(crate) const MAX_QUOTE_SYMBOLS: usize = 100;
pub(crate) const MAX_SYMBOL_BYTES: usize = 32;
const MAX_DERIVED_INDEX_SYMBOLS: usize = 3;
const MAX_IO_TIMEOUT: Duration = Duration::from_secs(20 * 60);

/// Logical data surface retained independently from the one authenticated account transport.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TradierLogicalProfile {
    /// Consolidated US equity, ETF, and option quotes/trades.
    ConsolidatedSecurities,
    /// Provider-derived NDX, RUT, and COMP benchmark values.
    DerivedIndexes,
}

/// Network access surface with its own exact provider-channel provenance.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TradierAccessSurface {
    /// One authenticated production WebSocket for quote and `tradex` events.
    Streaming,
    /// Bounded production REST quotes, derived indexes, and option chains.
    RestSnapshots,
}

impl TradierLogicalProfile {
    /// Returns the maximum quality this logical surface may publish.
    pub const fn quality_ceiling(self) -> DataQuality {
        match self {
            Self::ConsolidatedSecurities => DataQuality::Aggregated,
            Self::DerivedIndexes => DataQuality::Modeled,
        }
    }

    /// Returns the best market-depth description supported by this surface.
    pub const fn maximum_depth(self) -> Option<MarketDepth> {
        match self {
            Self::ConsolidatedSecurities => Some(MarketDepth::TopOfBook),
            Self::DerivedIndexes => None,
        }
    }

    /// Returns whether the official production WebSocket admits this logical surface.
    pub const fn supports_streaming(self) -> bool {
        matches!(self, Self::ConsolidatedSecurities)
    }
}

/// Provider-specific instrument family with exact quote-size semantics.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TradierInstrumentKind {
    /// US-listed equity; quote sizes are hundreds of shares.
    Equity,
    /// US-listed exchange-traded fund; quote sizes are hundreds of shares.
    Etf,
    /// OCC option contract; quote sizes are contracts.
    Option,
    /// Provider-derived benchmark with no native market book.
    DerivedIndex,
}

impl TradierInstrumentKind {
    pub(crate) const fn asset_class(self) -> AssetClass {
        match self {
            Self::Equity => AssetClass::Equity,
            Self::Etf => AssetClass::Fund,
            Self::Option => AssetClass::Option,
            Self::DerivedIndex => AssetClass::Index,
        }
    }

    pub(crate) const fn quote_quantity_multiplier(self) -> u32 {
        match self {
            Self::Equity | Self::Etf => 100,
            Self::Option | Self::DerivedIndex => 1,
        }
    }
}

/// Exact Tradier symbol-to-internal-instrument mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TradierSymbolMapping {
    symbol: SourceIdentifier,
    instrument: InstrumentId,
    kind: TradierInstrumentKind,
}

impl TradierSymbolMapping {
    /// Constructs a bounded production-symbol mapping.
    ///
    /// # Errors
    ///
    /// Rejects symbols outside Tradier's documented equity/OCC/index grammar.
    pub fn try_new(
        symbol: SourceIdentifier,
        instrument: InstrumentId,
        kind: TradierInstrumentKind,
    ) -> Result<Self, TradierConfigError> {
        validate_symbol(symbol.as_str())?;
        if kind == TradierInstrumentKind::DerivedIndex
            && !matches!(symbol.as_str(), "NDX" | "RUT" | "COMP")
        {
            return Err(TradierConfigError::UnsupportedDerivedIndex);
        }
        Ok(Self {
            symbol,
            instrument,
            kind,
        })
    }

    /// Returns the exact provider symbol.
    pub const fn symbol(&self) -> &SourceIdentifier {
        &self.symbol
    }

    /// Returns the stable internal instrument identity.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    /// Returns the exact provider-specific family and quantity semantics.
    pub const fn kind(&self) -> TradierInstrumentKind {
        self.kind
    }
}

/// Transport and response limits shared by one account owner and all logical surfaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TradierTransportLimits {
    max_frame_bytes: usize,
    io_timeout: Duration,
    http: HttpRequestBounds,
}

impl TradierTransportLimits {
    /// Constructs bounded WebSocket and REST limits.
    ///
    /// # Errors
    ///
    /// Rejects a zero/oversized frame, zero/excessive stream deadline, or a REST response bound
    /// above the global live-capture ceiling.
    pub fn try_new(
        max_frame_bytes: usize,
        io_timeout: Duration,
        http: HttpRequestBounds,
    ) -> Result<Self, TradierConfigError> {
        let maximum = market_squawk_sources::MAX_RAW_FRAME_BYTES;
        if max_frame_bytes == 0
            || max_frame_bytes > maximum
            || io_timeout.is_zero()
            || io_timeout > MAX_IO_TIMEOUT
            || usize::try_from(http.max_response_bytes()).map_or(true, |bytes| bytes > maximum)
        {
            return Err(TradierConfigError::InvalidTransportLimits);
        }
        Ok(Self {
            max_frame_bytes,
            io_timeout,
            http,
        })
    }

    /// Returns the exact incoming WebSocket frame ceiling.
    pub const fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }

    /// Returns the stream I/O and inactivity deadline.
    pub const fn io_timeout(self) -> Duration {
        self.io_timeout
    }

    /// Returns the hardened REST request bounds.
    pub const fn http(self) -> HttpRequestBounds {
        self.http
    }
}

/// Immutable logical-source metadata and provider-symbol mapping.
#[derive(Clone, Debug)]
pub struct TradierSourceConfig {
    metadata: SourceMetadata,
    profile: TradierLogicalProfile,
    access_surface: TradierAccessSurface,
    mappings: Box<[TradierSymbolMapping]>,
    limits: TradierTransportLimits,
}

impl TradierSourceConfig {
    /// Builds one truthful logical Tradier market-data surface.
    ///
    /// # Errors
    ///
    /// Rejects non-user authorization, mixed logical profiles, duplicate mappings, invalid
    /// budget authority, or any metadata declaration that could overstate provider coverage.
    #[allow(
        clippy::too_many_arguments,
        reason = "source metadata evidence and runtime bounds remain explicit"
    )]
    pub fn try_new(
        source_id: SourceId,
        revision_evidence: RevisionBoundPayloadEvidence,
        authorization: AuthorizationGrant,
        coverage_evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        profile: TradierLogicalProfile,
        access_surface: TradierAccessSurface,
        mappings: Vec<TradierSymbolMapping>,
        freshness: FreshnessPolicy,
        budget: ProviderBudgetPolicy,
        limits: TradierTransportLimits,
    ) -> Result<Self, TradierConfigError> {
        if authorization.mode() != AuthorizationMode::UserAuthorized {
            return Err(TradierConfigError::InvalidAuthorization);
        }
        if profile == TradierLogicalProfile::DerivedIndexes
            && access_surface == TradierAccessSurface::Streaming
        {
            return Err(TradierConfigError::InvalidAccessSurface);
        }
        validate_mappings(profile, &mappings)?;

        let venue = VenueId::try_from(match profile {
            TradierLogicalProfile::ConsolidatedSecurities => TRADIER_CONSOLIDATED_VENUE,
            TradierLogicalProfile::DerivedIndexes => TRADIER_DERIVED_INDEX_VENUE,
        })?;
        let no_snapshot_rule = rule("tradier-nonbook-snapshot-not-applicable")?;
        let mut coverage_rules = vec![LiveCoverageRule::try_new(
            LiveEventClass::Quote,
            None,
            SnapshotApplicability::NotApplicable {
                metadata_rule: no_snapshot_rule.clone(),
            },
        )?];
        if profile == TradierLogicalProfile::ConsolidatedSecurities
            && access_surface == TradierAccessSurface::Streaming
        {
            coverage_rules.push(LiveCoverageRule::try_new(
                LiveEventClass::Trade,
                None,
                SnapshotApplicability::NotApplicable {
                    metadata_rule: no_snapshot_rule,
                },
            )?);
        }
        let live = LiveCoverageDeclaration::try_new(
            ProviderProduct::new(SourceIdentifier::try_from(match profile {
                TradierLogicalProfile::ConsolidatedSecurities => {
                    "tradier-production-consolidated-market-data"
                }
                TradierLogicalProfile::DerivedIndexes => "tradier-production-derived-index-data",
            })?),
            ProviderChannel::new(SourceIdentifier::try_from(
                match (profile, access_surface) {
                    (
                        TradierLogicalProfile::ConsolidatedSecurities,
                        TradierAccessSurface::Streaming,
                    ) => "websocket-quote+tradex",
                    (
                        TradierLogicalProfile::ConsolidatedSecurities,
                        TradierAccessSurface::RestSnapshots,
                    ) => "rest-quotes+option-chains",
                    (
                        TradierLogicalProfile::DerivedIndexes,
                        TradierAccessSurface::RestSnapshots,
                    ) => "rest-derived-index-quotes",
                    (TradierLogicalProfile::DerivedIndexes, TradierAccessSurface::Streaming) => {
                        return Err(TradierConfigError::InvalidAccessSurface);
                    }
                },
            )?),
            coverage_rules,
        )?;
        let instruments = mappings
            .iter()
            .map(TradierSymbolMapping::instrument)
            .collect::<Vec<_>>();
        let mut asset_classes = mappings
            .iter()
            .map(|mapping| mapping.kind.asset_class())
            .collect::<Vec<_>>();
        asset_classes.sort_unstable_by_key(|class| asset_class_order(*class));
        asset_classes.dedup();
        let topology = match profile {
            TradierLogicalProfile::ConsolidatedSecurities => {
                CoverageTopology::consolidated(vec![venue])?
            }
            TradierLogicalProfile::DerivedIndexes => CoverageTopology::single_venue(venue),
        };
        let coverage = SourceCoverage::try_instrument(
            coverage_evidence,
            effective,
            asset_classes,
            topology,
            InstrumentCoverage::enumerated(instruments)?,
            Some(live),
            CoverageDelay::RealTime,
            match profile {
                TradierLogicalProfile::ConsolidatedSecurities => DeliveryEvidence::AuthorizedBroker,
                TradierLogicalProfile::DerivedIndexes => DeliveryEvidence::Indirect,
            },
        )?;
        let provider = SourceIdentifier::try_from(TRADIER_PROVIDER)?;
        let network = endpoint_policy(profile, access_surface, limits)?;
        let metadata = SourceMetadata::try_new(SourceMetadataInput::new(
            SchemaVersion::CURRENT,
            source_id,
            revision_evidence,
            SourceClass::Broker,
            provider,
            authorization,
            coverage,
            profile.quality_ceiling(),
            NetworkAccessPolicy::Allowlisted(network),
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
                rule("tradier-market-data-json-v1")?,
                SemanticInterpretationProfile::new(
                    rule("tradier-aggressor-unavailable")?,
                    rule("tradier-auction-unavailable")?,
                    rule("tradier-status-unavailable")?,
                    rule("tradier-corporate-action-unavailable")?,
                ),
                rule("tradier-epoch-millisecond-timestamp")?,
                SequenceValidationProfile::Unsupported {
                    rule: rule("tradier-mixed-event-sequence-unsupported")?,
                },
                ChecksumValidationProfile::Unsupported {
                    rule: rule("tradier-checksum-unsupported")?,
                },
                true,
                ProviderNumericPolicy::ExactDecimalLexeme,
            ))),
        ))?;
        Ok(Self {
            metadata,
            profile,
            access_surface,
            mappings: mappings.into_boxed_slice(),
            limits,
        })
    }

    /// Returns immutable source metadata for this logical surface.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Returns the exact logical coverage/quality profile.
    pub const fn profile(&self) -> TradierLogicalProfile {
        self.profile
    }

    /// Returns the exact provider transport/channel represented by this source registration.
    pub const fn access_surface(&self) -> TradierAccessSurface {
        self.access_surface
    }

    /// Returns every admitted symbol mapping.
    pub fn mappings(&self) -> &[TradierSymbolMapping] {
        &self.mappings
    }

    /// Returns shared transport limits.
    pub const fn transport_limits(&self) -> TradierTransportLimits {
        self.limits
    }

    pub(crate) fn mapping(&self, symbol: &str) -> Option<&TradierSymbolMapping> {
        self.mappings
            .iter()
            .find(|mapping| mapping.symbol.as_str() == symbol)
    }
}

fn validate_mappings(
    profile: TradierLogicalProfile,
    mappings: &[TradierSymbolMapping],
) -> Result<(), TradierConfigError> {
    let maximum = match profile {
        TradierLogicalProfile::ConsolidatedSecurities => MAX_STREAM_SYMBOLS,
        TradierLogicalProfile::DerivedIndexes => MAX_DERIVED_INDEX_SYMBOLS,
    };
    if mappings.is_empty() || mappings.len() > maximum {
        return Err(TradierConfigError::InvalidMappingCount { max: maximum });
    }
    let mut symbols = BTreeSet::new();
    let mut instruments = BTreeSet::new();
    for mapping in mappings {
        let kind_matches = match profile {
            TradierLogicalProfile::ConsolidatedSecurities => {
                mapping.kind != TradierInstrumentKind::DerivedIndex
            }
            TradierLogicalProfile::DerivedIndexes => {
                mapping.kind == TradierInstrumentKind::DerivedIndex
            }
        };
        if !kind_matches {
            return Err(TradierConfigError::MixedLogicalProfile);
        }
        if !symbols.insert(mapping.symbol.as_str()) {
            return Err(TradierConfigError::DuplicateSymbol);
        }
        if !instruments.insert(mapping.instrument) {
            return Err(TradierConfigError::DuplicateInstrument);
        }
    }
    Ok(())
}

pub(crate) fn validate_symbol(value: &str) -> Result<(), TradierConfigError> {
    if value.is_empty()
        || value.len() > MAX_SYMBOL_BYTES
        || !value.bytes().all(|byte| {
            byte.is_ascii_uppercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'/' | b'.' | b'-' | b'$')
        })
    {
        return Err(TradierConfigError::InvalidSymbol);
    }
    Ok(())
}

fn endpoint_policy(
    profile: TradierLogicalProfile,
    access_surface: TradierAccessSurface,
    limits: TradierTransportLimits,
) -> Result<market_squawk_sources::EndpointPolicy, TradierConfigError> {
    let public = QuerySensitivity::Public;
    let quotes = ApiEndpointRule::try_new(
        TRADIER_QUOTES_ENDPOINT,
        PathScope::Exact,
        vec![
            QueryParameterRule::try_new(
                SourceIdentifier::try_from("symbols")?,
                8_192,
                false,
                public,
            )?,
            QueryParameterRule::try_new(SourceIdentifier::try_from("greeks")?, 5, false, public)?,
        ],
        2,
        8_256,
    )?;
    let chains = ApiEndpointRule::try_new(
        TRADIER_OPTIONS_CHAIN_ENDPOINT,
        PathScope::Exact,
        vec![
            QueryParameterRule::try_new(
                SourceIdentifier::try_from("symbol")?,
                u16::try_from(MAX_SYMBOL_BYTES)
                    .map_err(|_| TradierConfigError::InvalidTransportLimits)?,
                false,
                public,
            )?,
            QueryParameterRule::try_new(
                SourceIdentifier::try_from("expiration")?,
                10,
                false,
                public,
            )?,
            QueryParameterRule::try_new(SourceIdentifier::try_from("greeks")?, 5, false, public)?,
        ],
        3,
        128,
    )?;
    let (endpoints, rules) = match (profile, access_surface) {
        (TradierLogicalProfile::ConsolidatedSecurities, TradierAccessSurface::Streaming) => (
            vec![TRADIER_MARKET_SESSION_ENDPOINT, TRADIER_WEBSOCKET_ENDPOINT],
            Vec::new(),
        ),
        (TradierLogicalProfile::ConsolidatedSecurities, TradierAccessSurface::RestSnapshots) => {
            (Vec::new(), vec![quotes, chains])
        }
        (TradierLogicalProfile::DerivedIndexes, TradierAccessSurface::RestSnapshots) => {
            (Vec::new(), vec![quotes])
        }
        (TradierLogicalProfile::DerivedIndexes, TradierAccessSurface::Streaming) => {
            return Err(TradierConfigError::InvalidAccessSurface);
        }
    };
    Ok(market_squawk_sources::EndpointPolicy::try_new_combined(
        endpoints,
        rules,
        limits.http(),
    )?)
}

fn rule(value: &str) -> Result<IntegrityRule, TradierConfigError> {
    Ok(IntegrityRule::new(
        SourceIdentifier::try_from(value)?,
        RuleVersion::new(1).map_err(|_| TradierConfigError::InvalidRule)?,
    ))
}

fn asset_class_order(class: AssetClass) -> u8 {
    match class {
        AssetClass::Equity => 0,
        AssetClass::Fund => 1,
        AssetClass::Option => 2,
        AssetClass::Index => 3,
        AssetClass::FixedIncome
        | AssetClass::Future
        | AssetClass::ForeignExchange
        | AssetClass::Crypto
        | AssetClass::Commodity
        | AssetClass::Cash => 4,
    }
}

/// Tradier logical-source configuration failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TradierConfigError {
    /// A bounded domain identity was invalid.
    #[error("Tradier configuration contains an invalid bounded identity")]
    Identity(#[from] market_squawk_domain::IdentityError),
    /// Source metadata fields contradicted one another.
    #[error("Tradier source metadata is invalid: {0}")]
    Metadata(#[from] SourceMetadataError),
    /// Endpoint or budget policy was invalid.
    #[error("Tradier source network policy is invalid: {0}")]
    Network(#[from] market_squawk_sources::NetworkPolicyError),
    /// Only a user-authorized Brokerage account may access this production adapter.
    #[error("Tradier production market data requires user-authorized account evidence")]
    InvalidAuthorization,
    /// Symbol syntax is outside the admitted Tradier grammar.
    #[error("invalid Tradier market symbol")]
    InvalidSymbol,
    /// A provider-derived symbol is outside the documented NDX/RUT/COMP set.
    #[error("unsupported Tradier derived index")]
    UnsupportedDerivedIndex,
    /// The logical source contained no mappings or exceeded its exact ceiling.
    #[error("Tradier mapping count is outside the supported bound {max}")]
    InvalidMappingCount {
        /// Maximum mappings for the selected logical profile.
        max: usize,
    },
    /// A provider symbol appeared more than once.
    #[error("duplicate Tradier provider symbol")]
    DuplicateSymbol,
    /// One internal instrument was mapped more than once.
    #[error("duplicate Tradier internal instrument")]
    DuplicateInstrument,
    /// Securities and derived benchmarks were mixed under one quality ceiling.
    #[error("Tradier logical source mixes incompatible quality profiles")]
    MixedLogicalProfile,
    /// The logical profile cannot use the requested provider channel.
    #[error("Tradier logical profile uses an invalid access surface")]
    InvalidAccessSurface,
    /// WebSocket or REST bounds were invalid.
    #[error("invalid Tradier transport limits")]
    InvalidTransportLimits,
    /// A static validation-rule revision could not be represented.
    #[error("invalid Tradier protocol rule")]
    InvalidRule,
}
