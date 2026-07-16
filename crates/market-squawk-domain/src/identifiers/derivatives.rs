use std::fmt;
use std::num::{NonZeroU16, NonZeroU32};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{IdentifierError, VenueSymbol};
use crate::{
    CalendarDate, PayloadReference, ProviderInstrumentId, SourceId, SourceIdentifier, Timestamp,
    VenueId,
};

/// OCC option type encoded in the fixed-width OSI identifier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionKind {
    /// Call option (`C`).
    Call,
    /// Put option (`P`).
    Put,
}

/// A syntactically validated OCC/OSI 21-character clearing identifier.
///
/// Fixed offsets and the example are specified by the [CAT Industry Member Technical
/// Specification](https://www.catnmsplan.com/sites/default/files/2026-03/03.06.26_CAT_Reporting_Technical_Specifications_for_Industry_Members_v4.1.0r15_CLEAN.pdf).
/// Syntax does not establish series existence, deliverables, economic underlying, or data rights.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OccOptionIdentity {
    raw: String,
    root_length: usize,
    expiration_yy: u8,
    expiration_month: u8,
    expiration_day: u8,
    kind: OptionKind,
    strike_thousandths: u64,
}

impl OccOptionIdentity {
    /// Returns the source-preserved 21-character identity including root padding.
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Returns the root field with trailing fixed-width padding removed.
    pub fn root(&self) -> &str {
        self.raw.get(..self.root_length).unwrap_or_default()
    }

    /// Returns the unresolved two-digit expiration year.
    pub const fn expiration_yy(&self) -> u8 {
        self.expiration_yy
    }

    /// Returns the expiration month.
    pub const fn expiration_month(&self) -> u8 {
        self.expiration_month
    }

    /// Returns the expiration day.
    pub const fn expiration_day(&self) -> u8 {
        self.expiration_day
    }

    /// Returns call/put identity.
    pub const fn kind(&self) -> OptionKind {
        self.kind
    }

    /// Returns the eight-digit strike field as integer thousandths.
    pub const fn strike_thousandths(&self) -> u64 {
        self.strike_thousandths
    }
}

impl TryFrom<&str> for OccOptionIdentity {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() != 21 || !value.is_ascii() {
            return Err(IdentifierError::InvalidLength);
        }
        let bytes = value.as_bytes();
        let root = bytes.get(..6).ok_or(IdentifierError::InvalidLength)?;
        let root_length = root
            .iter()
            .position(|byte| *byte == b' ')
            .unwrap_or(root.len());
        if root_length == 0
            || !root.iter().take(root_length).all(u8::is_ascii_graphic)
            || !root.iter().skip(root_length).all(|byte| *byte == b' ')
        {
            return Err(IdentifierError::InvalidCharacter);
        }
        let yy = parse_two_digits(bytes, 6)?;
        let month = parse_two_digits(bytes, 8)?;
        let day = parse_two_digits(bytes, 10)?;
        if !valid_two_digit_date(yy, month, day) {
            return Err(IdentifierError::InvalidDate);
        }
        let kind = match bytes.get(12) {
            Some(b'C') => OptionKind::Call,
            Some(b'P') => OptionKind::Put,
            _ => return Err(IdentifierError::InvalidOptionKind),
        };
        let strike = bytes.get(13..21).ok_or(IdentifierError::InvalidLength)?;
        if !strike.iter().all(u8::is_ascii_digit) {
            return Err(IdentifierError::InvalidCharacter);
        }
        let strike_thousandths = strike
            .iter()
            .fold(0_u64, |amount, byte| amount * 10 + u64::from(*byte - b'0'));
        Ok(Self {
            raw: value.to_owned(),
            root_length,
            expiration_yy: yy,
            expiration_month: month,
            expiration_day: day,
            kind,
            strike_thousandths,
        })
    }
}

impl TryFrom<String> for OccOptionIdentity {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl fmt::Display for OccOptionIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.raw.fmt(formatter)
    }
}

impl Serialize for OccOptionIdentity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.raw)
    }
}

impl<'de> Deserialize<'de> for OccOptionIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

fn parse_two_digits(bytes: &[u8], offset: usize) -> Result<u8, IdentifierError> {
    let Some(tens) = bytes.get(offset).copied() else {
        return Err(IdentifierError::InvalidLength);
    };
    let Some(ones) = bytes.get(offset + 1).copied() else {
        return Err(IdentifierError::InvalidLength);
    };
    if !tens.is_ascii_digit() || !ones.is_ascii_digit() {
        return Err(IdentifierError::InvalidCharacter);
    }
    Ok((tens - b'0') * 10 + (ones - b'0'))
}

fn valid_two_digit_date(year: u8, month: u8, day: u8) -> bool {
    let leap = year.is_multiple_of(4);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    day > 0 && day <= days
}

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

impl FuturesLifecycleDates {
    /// Constructs source-evidenced lifecycle dates without inventing absent fields.
    ///
    /// # Errors
    ///
    /// Rejects an empty date set, last trade after expiration, a reversed notice range, or a
    /// reversed delivery range.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
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
    ) -> Result<Self, IdentifierError> {
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
        Self::try_new(
            wire.source_id,
            wire.source_reference,
            wire.observed_at,
            wire.maturity_date,
            wire.expiration_date,
            wire.last_trade_date,
            wire.first_notice_date,
            wire.last_notice_date,
            wire.first_delivery_date,
            wire.last_delivery_date,
        )
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
    contract_month: ContractMonth,
    lifecycle: FuturesLifecycleDates,
    legs: Vec<FuturesLeg>,
}

impl FuturesContractIdentity {
    /// Constructs a futures identity from separately sourced venue metadata fields.
    ///
    /// # Errors
    ///
    /// Multileg securities require at least two distinct legs in consecutive one-based order;
    /// outright and daily securities reject legs.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        venue_id: VenueId,
        security_id: ProviderInstrumentId,
        security_id_source: SourceIdentifier,
        product_code: ProviderInstrumentId,
        native_symbol: VenueSymbol,
        security_type: FuturesSecurityType,
        contract_month: ContractMonth,
        lifecycle: FuturesLifecycleDates,
        legs: Vec<FuturesLeg>,
    ) -> Result<Self, IdentifierError> {
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
    pub const fn contract_month(&self) -> ContractMonth {
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

#[derive(Deserialize)]
struct FuturesContractIdentityWire {
    venue_id: VenueId,
    security_id: ProviderInstrumentId,
    security_id_source: SourceIdentifier,
    product_code: ProviderInstrumentId,
    native_symbol: VenueSymbol,
    security_type: FuturesSecurityType,
    contract_month: ContractMonth,
    lifecycle: FuturesLifecycleDates,
    legs: Vec<FuturesLeg>,
}

impl<'de> Deserialize<'de> for FuturesContractIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FuturesContractIdentityWire::deserialize(deserializer)?;
        Self::try_new(
            wire.venue_id,
            wire.security_id,
            wire.security_id_source,
            wire.product_code,
            wire.native_symbol,
            wire.security_type,
            wire.contract_month,
            wire.lifecycle,
            wire.legs,
        )
        .map_err(serde::de::Error::custom)
    }
}
