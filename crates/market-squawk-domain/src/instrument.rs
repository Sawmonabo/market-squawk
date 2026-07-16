use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    ChainAddress, CryptoPair, Currency, Cusip, Figi, FuturesContractIdentity, InstrumentId, Isin,
    LotSize, OccOptionIdentity, ProviderInstrumentId, Sedol, SourceId, TickSize, Ticker, Timestamp,
    VenueId, VenueSymbol,
};

/// A broad instrument asset family, separate from Task 4 evidence classifications.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetClass {
    /// Equity security.
    Equity,
    /// Fixed-income security.
    FixedIncome,
    /// Listed or OTC option.
    Option,
    /// Futures contract or venue-defined futures combination.
    Future,
    /// Foreign-exchange instrument.
    ForeignExchange,
    /// Cryptoasset spot or derivative product.
    Crypto,
    /// Commodity instrument not otherwise represented as a future.
    Commodity,
    /// Fund or exchange-traded product.
    Fund,
    /// Index or benchmark.
    Index,
    /// Cash balance or cash-equivalent instrument.
    Cash,
}

/// Reference-master trading status. Live integrity and eligibility remain separate Task 4 types.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingStatus {
    /// Instrument is active according to its reference source.
    Active,
    /// Instrument is temporarily halted.
    Halted,
    /// Instrument is inactive but retained historically.
    Inactive,
    /// Instrument is delisted and retained historically.
    Delisted,
}

/// A syntactically validated external identifier.
///
/// Every variant remains syntax/checksum-only. Registry assignment, existence, lifecycle, source,
/// and licensed-data rights must be established separately.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ExternalIdentifier {
    /// Ticker alias.
    Ticker(Ticker),
    /// CUSIP syntax/checksum value.
    Cusip(Cusip),
    /// ISIN syntax/checksum value.
    Isin(Isin),
    /// SEDOL syntax/checksum value.
    Sedol(Sedol),
    /// FIGI syntax/checksum value.
    Figi(Figi),
    /// OCC fixed-width option identity.
    OccOption(OccOptionIdentity),
    /// Structured venue futures identity.
    Futures(FuturesContractIdentity),
    /// Structured venue crypto pair.
    CryptoPair(CryptoPair),
    /// Chain-qualified protocol-specific address.
    ChainAddress(ChainAddress),
}

/// An instrument's symbol mapping in one venue namespace.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VenueMapping {
    venue_id: VenueId,
    venue_symbol: VenueSymbol,
    provider_instrument_id: Option<ProviderInstrumentId>,
}

impl VenueMapping {
    /// Constructs a venue symbol mapping with an optional source-native instrument ID.
    pub fn new(
        venue_id: VenueId,
        venue_symbol: VenueSymbol,
        provider_instrument_id: Option<ProviderInstrumentId>,
    ) -> Self {
        Self {
            venue_id,
            venue_symbol,
            provider_instrument_id,
        }
    }

    /// Returns the venue namespace.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the venue-native symbol.
    pub const fn venue_symbol(&self) -> &VenueSymbol {
        &self.venue_symbol
    }
}

/// Instrument-definition or effective-identity invariant failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InstrumentError {
    /// An effective interval ended at or before its start.
    InvalidEffectiveInterval,
    /// A lifecycle transition or roll mapped an instrument to itself.
    SelfTransition,
    /// An instrument definition contained multiple current mappings for one venue.
    DuplicateVenueMapping {
        /// Duplicated venue.
        venue: VenueId,
    },
}

impl fmt::Display for InstrumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEffectiveInterval => {
                formatter.write_str("effective interval end must be later than its start")
            }
            Self::SelfTransition => {
                formatter.write_str("identity transition must change instrument")
            }
            Self::DuplicateVenueMapping { venue } => {
                write!(formatter, "duplicate current venue mapping for {venue}")
            }
        }
    }
}

impl std::error::Error for InstrumentError {}

/// A half-open effective-time interval `[starts_at, ends_at)`.
///
/// `None` is a first-class open end; constructors never invent an end timestamp.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct EffectiveInterval {
    starts_at: Timestamp,
    ends_at: Option<Timestamp>,
}

impl EffectiveInterval {
    /// Constructs an ordered, optionally open-ended effective interval.
    ///
    /// # Errors
    ///
    /// Rejects an end at or before the start.
    pub fn new(starts_at: Timestamp, ends_at: Option<Timestamp>) -> Result<Self, InstrumentError> {
        match ends_at {
            Some(end) if end <= starts_at => Err(InstrumentError::InvalidEffectiveInterval),
            _ => Ok(Self { starts_at, ends_at }),
        }
    }

    /// Returns the inclusive interval start.
    pub const fn starts_at(self) -> Timestamp {
        self.starts_at
    }

    /// Returns the exclusive interval end, or `None` when still effective.
    pub const fn ends_at(self) -> Option<Timestamp> {
        self.ends_at
    }
}

#[derive(Deserialize)]
struct EffectiveIntervalWire {
    starts_at: Timestamp,
    ends_at: Option<Timestamp>,
}

impl<'de> Deserialize<'de> for EffectiveInterval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = EffectiveIntervalWire::deserialize(deserializer)?;
        Self::new(wire.starts_at, wire.ends_at).map_err(serde::de::Error::custom)
    }
}

/// A venue-symbol validity record retaining stable internal identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SymbolIdentityRecord {
    instrument_id: InstrumentId,
    venue_id: VenueId,
    venue_symbol: VenueSymbol,
    validity: EffectiveInterval,
}

impl SymbolIdentityRecord {
    /// Constructs a symbol-history record without inventing an end time.
    pub fn new(
        instrument_id: InstrumentId,
        venue_id: VenueId,
        venue_symbol: VenueSymbol,
        validity: EffectiveInterval,
    ) -> Self {
        Self {
            instrument_id,
            venue_id,
            venue_symbol,
            validity,
        }
    }

    /// Returns the effective interval.
    pub const fn validity(&self) -> EffectiveInterval {
        self.validity
    }
}

/// A provider-instrument-ID validity record retaining stable internal identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderIdentityRecord {
    instrument_id: InstrumentId,
    source_id: SourceId,
    provider_instrument_id: ProviderInstrumentId,
    validity: EffectiveInterval,
}

impl ProviderIdentityRecord {
    /// Constructs a provider-ID history record without inventing an end time.
    pub fn new(
        instrument_id: InstrumentId,
        source_id: SourceId,
        provider_instrument_id: ProviderInstrumentId,
        validity: EffectiveInterval,
    ) -> Self {
        Self {
            instrument_id,
            source_id,
            provider_instrument_id,
            validity,
        }
    }

    /// Returns the stable internal instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
}

/// The identity-level lifecycle transition persisted before canonical Task 4 event payloads exist.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LifecycleTransitionKind {
    /// Instrument merged into a distinct stable internal identity.
    Merger {
        /// Successor instrument.
        successor: InstrumentId,
    },
    /// Instrument was delisted with no invented successor.
    Delisting,
}

/// An effective identity lifecycle transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct LifecycleTransition {
    instrument_id: InstrumentId,
    effective_at: Timestamp,
    kind: LifecycleTransitionKind,
}

impl LifecycleTransition {
    /// Constructs an identity lifecycle transition.
    ///
    /// # Errors
    ///
    /// Rejects a merger whose successor is the same instrument.
    pub fn new(
        instrument_id: InstrumentId,
        effective_at: Timestamp,
        kind: LifecycleTransitionKind,
    ) -> Result<Self, InstrumentError> {
        if matches!(kind, LifecycleTransitionKind::Merger { successor } if successor == instrument_id)
        {
            Err(InstrumentError::SelfTransition)
        } else {
            Ok(Self {
                instrument_id,
                effective_at,
                kind,
            })
        }
    }

    /// Returns when the transition became effective.
    pub const fn effective_at(self) -> Timestamp {
        self.effective_at
    }
}

#[derive(Deserialize)]
struct LifecycleTransitionWire {
    instrument_id: InstrumentId,
    effective_at: Timestamp,
    kind: LifecycleTransitionKind,
}

impl<'de> Deserialize<'de> for LifecycleTransition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LifecycleTransitionWire::deserialize(deserializer)?;
        Self::new(wire.instrument_id, wire.effective_at, wire.kind)
            .map_err(serde::de::Error::custom)
    }
}

/// An effective mapping from an expiring contract identity to its roll successor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ContractRollMapping {
    from_instrument_id: InstrumentId,
    to_instrument_id: InstrumentId,
    effective_at: Timestamp,
}

impl ContractRollMapping {
    /// Constructs a contract-roll mapping between distinct instruments.
    ///
    /// # Errors
    ///
    /// Rejects a self mapping.
    pub fn new(
        from_instrument_id: InstrumentId,
        to_instrument_id: InstrumentId,
        effective_at: Timestamp,
    ) -> Result<Self, InstrumentError> {
        if from_instrument_id == to_instrument_id {
            Err(InstrumentError::SelfTransition)
        } else {
            Ok(Self {
                from_instrument_id,
                to_instrument_id,
                effective_at,
            })
        }
    }

    /// Returns the roll target instrument.
    pub const fn to_instrument_id(self) -> InstrumentId {
        self.to_instrument_id
    }
}

#[derive(Deserialize)]
struct ContractRollMappingWire {
    from_instrument_id: InstrumentId,
    to_instrument_id: InstrumentId,
    effective_at: Timestamp,
}

impl<'de> Deserialize<'de> for ContractRollMapping {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ContractRollMappingWire::deserialize(deserializer)?;
        Self::new(
            wire.from_instrument_id,
            wire.to_instrument_id,
            wire.effective_at,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Current instrument reference definition with invariant-preserving private fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InstrumentDefinition {
    instrument_id: InstrumentId,
    asset_class: AssetClass,
    primary_currency: Currency,
    tick_size: TickSize,
    lot_size: LotSize,
    venue_mappings: Vec<VenueMapping>,
    identifiers: Vec<ExternalIdentifier>,
    trading_status: TradingStatus,
}

impl InstrumentDefinition {
    /// Constructs a current instrument definition.
    ///
    /// # Errors
    ///
    /// Rejects duplicate current mappings for one venue. Historical mappings belong in
    /// [`SymbolIdentityRecord`] intervals.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        instrument_id: InstrumentId,
        asset_class: AssetClass,
        primary_currency: Currency,
        tick_size: TickSize,
        lot_size: LotSize,
        venue_mappings: Vec<VenueMapping>,
        identifiers: Vec<ExternalIdentifier>,
        trading_status: TradingStatus,
    ) -> Result<Self, InstrumentError> {
        for (index, mapping) in venue_mappings.iter().enumerate() {
            if venue_mappings
                .iter()
                .skip(index + 1)
                .any(|candidate| candidate.venue_id == mapping.venue_id)
            {
                return Err(InstrumentError::DuplicateVenueMapping {
                    venue: mapping.venue_id.clone(),
                });
            }
        }
        Ok(Self {
            instrument_id,
            asset_class,
            primary_currency,
            tick_size,
            lot_size,
            venue_mappings,
            identifiers,
            trading_status,
        })
    }

    /// Returns the stable internal identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the broad asset family.
    pub const fn asset_class(&self) -> AssetClass {
        self.asset_class
    }

    /// Returns the primary denomination currency.
    pub const fn primary_currency(&self) -> Currency {
        self.primary_currency
    }

    /// Returns the exact price increment.
    pub const fn tick_size(&self) -> TickSize {
        self.tick_size
    }

    /// Returns the exact quantity increment.
    pub const fn lot_size(&self) -> LotSize {
        self.lot_size
    }

    /// Returns current venue mappings.
    pub fn venue_mappings(&self) -> &[VenueMapping] {
        &self.venue_mappings
    }

    /// Returns syntactically validated external identifiers.
    pub fn identifiers(&self) -> &[ExternalIdentifier] {
        &self.identifiers
    }

    /// Returns current reference-master trading status.
    pub const fn trading_status(&self) -> TradingStatus {
        self.trading_status
    }
}

#[derive(Deserialize)]
struct InstrumentDefinitionWire {
    instrument_id: InstrumentId,
    asset_class: AssetClass,
    primary_currency: Currency,
    tick_size: TickSize,
    lot_size: LotSize,
    venue_mappings: Vec<VenueMapping>,
    identifiers: Vec<ExternalIdentifier>,
    trading_status: TradingStatus,
}

impl<'de> Deserialize<'de> for InstrumentDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = InstrumentDefinitionWire::deserialize(deserializer)?;
        Self::try_new(
            wire.instrument_id,
            wire.asset_class,
            wire.primary_currency,
            wire.tick_size,
            wire.lot_size,
            wire.venue_mappings,
            wire.identifiers,
            wire.trading_status,
        )
        .map_err(serde::de::Error::custom)
    }
}
