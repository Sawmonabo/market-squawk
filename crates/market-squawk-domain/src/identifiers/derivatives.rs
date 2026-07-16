use std::fmt;
use std::num::{NonZeroU16, NonZeroU32};

use serde::{Deserialize, Deserializer, Serialize};

use super::{IdentifierError, VenueSymbol};
use crate::{
    CalendarDate, PayloadReference, ProviderInstrumentId, SourceId, SourceIdentifier, Timestamp,
    VenueId,
};

#[path = "derivatives/options.rs"]
mod options;

pub use options::{OccOptionIdentity, OptionKind};

/// A validated futures contract month kept separate from venue-native symbols.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct ContractMonth {
    year: u16,
    month: u8,
}

impl fmt::Display for ContractMonth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:04}-{:02}", self.year, self.month)
    }
}

#[derive(Deserialize)]
struct ContractMonthWire {
    year: u16,
    month: u8,
}

impl<'de> Deserialize<'de> for ContractMonth {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ContractMonthWire::deserialize(deserializer)?;
        Self::new(wire.year, wire.month).map_err(serde::de::Error::custom)
    }
}

impl ContractMonth {
    /// Validates a year and month without parsing venue month codes.
    ///
    /// # Errors
    ///
    /// Rejects year zero and months outside 1 through 12.
    pub const fn new(year: u16, month: u8) -> Result<Self, IdentifierError> {
        if year == 0 || month == 0 || month > 12 {
            Err(IdentifierError::InvalidDate)
        } else {
            Ok(Self { year, month })
        }
    }

    /// Returns the full contract year.
    pub const fn year(self) -> u16 {
        self.year
    }

    /// Returns the contract month from 1 through 12.
    pub const fn month(self) -> u8 {
        self.month
    }
}

/// Venue reference-data security type for a futures identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FuturesSecurityType {
    /// Outright future.
    Future,
    /// Venue-defined spread or multileg instrument.
    SpreadOrMultileg,
    /// Venue-defined daily contract.
    Daily,
}

/// Source-evidenced contract lifecycle dates, independent of the contract month.
///
/// Exchange reference data can supply only some dates. This contract never manufactures missing
/// values, but it requires at least one observed lifecycle date and enforces relational ranges.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct FuturesLifecycleDates {
    source_id: SourceId,
    source_reference: PayloadReference,
    observed_at: Timestamp,
    maturity_date: Option<CalendarDate>,
    expiration_date: Option<CalendarDate>,
    last_trade_date: Option<CalendarDate>,
    first_notice_date: Option<CalendarDate>,
    last_notice_date: Option<CalendarDate>,
    first_delivery_date: Option<CalendarDate>,
    last_delivery_date: Option<CalendarDate>,
}

/// Optional date fields supplied by an authoritative futures reference-data record.
///
/// Absence is retained exactly. In particular, a full maturity date does not imply that a source
/// supplied FIX `MaturityMonthYear (200)`.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct FuturesLifecycleDateFields {
    /// Source maturity date, when present.
    pub maturity_date: Option<CalendarDate>,
    /// Source expiration date, when present.
    pub expiration_date: Option<CalendarDate>,
    /// Last date on which the contract may trade, when present.
    pub last_trade_date: Option<CalendarDate>,
    /// First notice date, when present.
    pub first_notice_date: Option<CalendarDate>,
    /// Last notice date, when present.
    pub last_notice_date: Option<CalendarDate>,
    /// First delivery date, when present.
    pub first_delivery_date: Option<CalendarDate>,
    /// Last delivery date, when present.
    pub last_delivery_date: Option<CalendarDate>,
}

/// Complete evidence input for constructing [`FuturesLifecycleDates`].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FuturesLifecycleDatesInput {
    /// Reference-data source namespace.
    pub source_id: SourceId,
    /// Immutable source payload evidence.
    pub source_reference: PayloadReference,
    /// Time Market Squawk first observed the record.
    pub observed_at: Timestamp,
    /// Lifecycle fields exactly as supplied by the source.
    pub dates: FuturesLifecycleDateFields,
}

impl FuturesLifecycleDates {
    /// Constructs source-evidenced lifecycle dates without inventing absent fields.
    ///
    /// # Errors
    ///
    /// Rejects an empty date set, last trade after expiration, a reversed notice range, or a
    /// reversed delivery range.
    pub fn try_new(input: FuturesLifecycleDatesInput) -> Result<Self, IdentifierError> {
        let FuturesLifecycleDatesInput {
            source_id,
            source_reference,
            observed_at,
            dates:
                FuturesLifecycleDateFields {
                    maturity_date,
                    expiration_date,
                    last_trade_date,
                    first_notice_date,
                    last_notice_date,
                    first_delivery_date,
                    last_delivery_date,
                },
        } = input;
        if [
            maturity_date,
            expiration_date,
            last_trade_date,
            first_notice_date,
            last_notice_date,
            first_delivery_date,
            last_delivery_date,
        ]
        .iter()
        .all(Option::is_none)
        {
            return Err(IdentifierError::MissingLifecycleDate);
        }
        if last_trade_date
            .zip(expiration_date)
            .is_some_and(|(last, expiration)| last > expiration)
            || first_notice_date
                .zip(last_notice_date)
                .is_some_and(|(first, last)| first > last)
            || first_delivery_date
                .zip(last_delivery_date)
                .is_some_and(|(first, last)| first > last)
        {
            return Err(IdentifierError::InvalidLifecycleOrdering);
        }
        Ok(Self {
            source_id,
            source_reference,
            observed_at,
            maturity_date,
            expiration_date,
            last_trade_date,
            first_notice_date,
            last_notice_date,
            first_delivery_date,
            last_delivery_date,
        })
    }

    /// Returns the reference-data source.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns immutable source payload evidence.
    pub const fn source_reference(&self) -> &PayloadReference {
        &self.source_reference
    }

    /// Returns when Market Squawk first observed this lifecycle record.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns the source maturity date.
    pub const fn maturity_date(&self) -> Option<CalendarDate> {
        self.maturity_date
    }

    /// Returns the source expiration date.
    pub const fn expiration_date(&self) -> Option<CalendarDate> {
        self.expiration_date
    }

    /// Returns the last trading date.
    pub const fn last_trade_date(&self) -> Option<CalendarDate> {
        self.last_trade_date
    }

    /// Returns the first notice date.
    pub const fn first_notice_date(&self) -> Option<CalendarDate> {
        self.first_notice_date
    }

    /// Returns the last notice date.
    pub const fn last_notice_date(&self) -> Option<CalendarDate> {
        self.last_notice_date
    }

    /// Returns the first delivery date.
    pub const fn first_delivery_date(&self) -> Option<CalendarDate> {
        self.first_delivery_date
    }

    /// Returns the last delivery date.
    pub const fn last_delivery_date(&self) -> Option<CalendarDate> {
        self.last_delivery_date
    }
}

#[derive(Deserialize)]
struct FuturesLifecycleDatesWire {
    source_id: SourceId,
    source_reference: PayloadReference,
    observed_at: Timestamp,
    maturity_date: Option<CalendarDate>,
    expiration_date: Option<CalendarDate>,
    last_trade_date: Option<CalendarDate>,
    first_notice_date: Option<CalendarDate>,
    last_notice_date: Option<CalendarDate>,
    first_delivery_date: Option<CalendarDate>,
    last_delivery_date: Option<CalendarDate>,
}

impl<'de> Deserialize<'de> for FuturesLifecycleDates {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FuturesLifecycleDatesWire::deserialize(deserializer)?;
        Self::try_new(FuturesLifecycleDatesInput {
            source_id: wire.source_id,
            source_reference: wire.source_reference,
            observed_at: wire.observed_at,
            dates: FuturesLifecycleDateFields {
                maturity_date: wire.maturity_date,
                expiration_date: wire.expiration_date,
                last_trade_date: wire.last_trade_date,
                first_notice_date: wire.first_notice_date,
                last_notice_date: wire.last_notice_date,
                first_delivery_date: wire.first_delivery_date,
                last_delivery_date: wire.last_delivery_date,
            },
        })
        .map_err(serde::de::Error::custom)
    }
}

/// Economic direction of a component in an ordered futures multileg identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FuturesLegSide {
    /// Buy the leg.
    Buy,
    /// Sell the leg.
    Sell,
}

/// One source-qualified component of a venue-defined futures multileg security.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct FuturesLeg {
    position: NonZeroU16,
    security_id: ProviderInstrumentId,
    security_id_source: SourceIdentifier,
    contract_month: Option<ContractMonth>,
    side: FuturesLegSide,
    ratio: NonZeroU32,
}

impl FuturesLeg {
    /// Constructs a leg with an explicit one-based position and nonzero ratio.
    ///
    /// # Errors
    ///
    /// Rejects position zero or ratio zero.
    pub fn try_new(
        position: u16,
        security_id: ProviderInstrumentId,
        security_id_source: SourceIdentifier,
        contract_month: Option<ContractMonth>,
        side: FuturesLegSide,
        ratio: u32,
    ) -> Result<Self, IdentifierError> {
        Ok(Self {
            position: NonZeroU16::new(position).ok_or(IdentifierError::ZeroLegPosition)?,
            security_id,
            security_id_source,
            contract_month,
            side,
            ratio: NonZeroU32::new(ratio).ok_or(IdentifierError::ZeroLegRatio)?,
        })
    }

    /// Returns the one-based ordered position.
    pub const fn position(&self) -> u16 {
        self.position.get()
    }

    /// Returns the venue/source security identity for the leg.
    pub const fn security_id(&self) -> &ProviderInstrumentId {
        &self.security_id
    }

    /// Returns the source scheme for the leg security identity.
    pub const fn security_id_source(&self) -> &SourceIdentifier {
        &self.security_id_source
    }

    /// Returns the separately supplied leg contract month when applicable.
    pub const fn contract_month(&self) -> Option<ContractMonth> {
        self.contract_month
    }

    /// Returns the economic leg side.
    pub const fn side(&self) -> FuturesLegSide {
        self.side
    }

    /// Returns the nonzero integer leg ratio.
    pub const fn ratio(&self) -> u32 {
        self.ratio.get()
    }
}

#[derive(Deserialize)]
struct FuturesLegWire {
    position: u16,
    security_id: ProviderInstrumentId,
    security_id_source: SourceIdentifier,
    contract_month: Option<ContractMonth>,
    side: FuturesLegSide,
    ratio: u32,
}

impl<'de> Deserialize<'de> for FuturesLeg {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FuturesLegWire::deserialize(deserializer)?;
        Self::try_new(
            wire.position,
            wire.security_id,
            wire.security_id_source,
            wire.contract_month,
            wire.side,
            wire.ratio,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// A structured futures identity; deliberately not a universal root/month-symbol parser.
///
/// CFTC large-trader layouts and CME security definitions keep exchange security identifiers,
/// identifier sources, product codes, native symbols, and expiry fields separate. Existence and
/// contract economics require licensed venue reference data.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct FuturesContractIdentity {
    venue_id: VenueId,
    security_id: ProviderInstrumentId,
    security_id_source: SourceIdentifier,
    product_code: ProviderInstrumentId,
    native_symbol: VenueSymbol,
    security_type: FuturesSecurityType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    contract_month: Option<ContractMonth>,
    lifecycle: FuturesLifecycleDates,
    legs: Vec<FuturesLeg>,
}

/// Complete source-field input for constructing [`FuturesContractIdentity`].
///
/// `contract_month` is optional because full maturity dates and leg-level maturity fields are
/// independent source fields. See FIX [`MaturityMonthYear (200)`], [`MaturityDate (541)`], and
/// [`LegMaturityMonthYear (610)`].
///
/// [`MaturityMonthYear (200)`]: https://fiximate.fixtrading.org/en/FIX.Latest/tag200.html
/// [`MaturityDate (541)`]: https://fiximate.fixtrading.org/en/FIX.Latest/tag541.html
/// [`LegMaturityMonthYear (610)`]: https://fiximate.fixtrading.org/en/FIX.Latest/tag610.html
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FuturesContractIdentityInput {
    /// Venue namespace in which the security identity is valid.
    pub venue_id: VenueId,
    /// Source-native security identifier.
    pub security_id: ProviderInstrumentId,
    /// FIX or source-specific security identifier scheme.
    pub security_id_source: SourceIdentifier,
    /// Source product or commodity code.
    pub product_code: ProviderInstrumentId,
    /// Unmodified venue-native symbol.
    pub native_symbol: VenueSymbol,
    /// Source security type.
    pub security_type: FuturesSecurityType,
    /// Source-supplied contract month, if explicit; never derived from a full date or a leg.
    #[serde(default)]
    pub contract_month: Option<ContractMonth>,
    /// Source-evidenced lifecycle dates.
    pub lifecycle: FuturesLifecycleDates,
    /// Ordered legs for a multileg security.
    pub legs: Vec<FuturesLeg>,
}

impl FuturesContractIdentity {
    /// Constructs a futures identity from separately sourced venue metadata fields.
    ///
    /// # Errors
    ///
    /// Multileg securities require at least two distinct legs in consecutive one-based order;
    /// outright and daily securities reject legs.
    pub fn try_new(input: FuturesContractIdentityInput) -> Result<Self, IdentifierError> {
        let FuturesContractIdentityInput {
            venue_id,
            security_id,
            security_id_source,
            product_code,
            native_symbol,
            security_type,
            contract_month,
            lifecycle,
            legs,
        } = input;
        match security_type {
            FuturesSecurityType::SpreadOrMultileg if legs.len() < 2 => {
                return Err(IdentifierError::InvalidLegStructure);
            }
            FuturesSecurityType::Future | FuturesSecurityType::Daily if !legs.is_empty() => {
                return Err(IdentifierError::InvalidLegStructure);
            }
            FuturesSecurityType::SpreadOrMultileg
            | FuturesSecurityType::Future
            | FuturesSecurityType::Daily => {}
        }
        for (index, leg) in legs.iter().enumerate() {
            let expected =
                u16::try_from(index + 1).map_err(|_| IdentifierError::InvalidLegOrdering)?;
            if leg.position() != expected {
                return Err(IdentifierError::InvalidLegOrdering);
            }
            if legs.iter().skip(index + 1).any(|candidate| {
                candidate.security_id == leg.security_id
                    && candidate.security_id_source == leg.security_id_source
            }) {
                return Err(IdentifierError::DuplicateLeg);
            }
        }
        Ok(Self {
            venue_id,
            security_id,
            security_id_source,
            product_code,
            native_symbol,
            security_type,
            contract_month,
            lifecycle,
            legs,
        })
    }

    /// Returns the venue namespace.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the venue/source security identity.
    pub const fn security_id(&self) -> &ProviderInstrumentId {
        &self.security_id
    }

    /// Returns the scheme identifying `security_id`.
    pub const fn security_id_source(&self) -> &SourceIdentifier {
        &self.security_id_source
    }

    /// Returns the source product or commodity code.
    pub const fn product_code(&self) -> &ProviderInstrumentId {
        &self.product_code
    }

    /// Returns the unmodified venue-native symbol.
    pub const fn native_symbol(&self) -> &VenueSymbol {
        &self.native_symbol
    }

    /// Returns the separately supplied contract month.
    pub const fn contract_month(&self) -> Option<ContractMonth> {
        self.contract_month
    }

    /// Returns the venue reference-data security type.
    pub const fn security_type(&self) -> FuturesSecurityType {
        self.security_type
    }

    /// Returns source-evidenced lifecycle dates.
    pub const fn lifecycle(&self) -> &FuturesLifecycleDates {
        &self.lifecycle
    }

    /// Returns ordered multileg components, empty for non-multileg securities.
    pub fn legs(&self) -> &[FuturesLeg] {
        &self.legs
    }
}

impl fmt::Display for FuturesContractIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.venue_id, self.security_id)
    }
}

impl<'de> Deserialize<'de> for FuturesContractIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = FuturesContractIdentityInput::deserialize(deserializer)?;
        Self::try_new(input).map_err(serde::de::Error::custom)
    }
}
