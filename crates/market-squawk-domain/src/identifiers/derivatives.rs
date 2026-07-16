use std::fmt;
use std::num::{NonZeroU16, NonZeroU32};

use serde::{Deserialize, Deserializer, Serialize};

use super::{IdentifierError, VenueSymbol};
use crate::{
    CalendarDate, PayloadReference, ProviderInstrumentId, SourceId, SourceIdentifier, Timestamp,
    VenueId,
};

#[path = "derivatives/maturity_month_year.rs"]
mod maturity_month_year;
#[path = "derivatives/options.rs"]
mod options;

pub use maturity_month_year::MaturityMonthYear;
pub use options::{OccOptionIdentity, OptionKind};

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

/// Optional contract lifecycle dates, independent of FIX `MaturityMonthYear(200)`.
///
/// An empty value means the source record supplied no lifecycle dates. Source payload and revision
/// evidence lives on [`FuturesContractIdentity`] so a tag-200-only record remains evidenced.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, Serialize)]
pub struct FuturesLifecycleDates {
    first_trade_date: Option<CalendarDate>,
    maturity_date: Option<CalendarDate>,
    expiration_date: Option<CalendarDate>,
    last_trade_date: Option<CalendarDate>,
    settlement_date: Option<CalendarDate>,
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
#[serde(deny_unknown_fields)]
pub struct FuturesLifecycleDateFields {
    /// First date on which the contract may trade, when supplied.
    pub first_trade_date: Option<CalendarDate>,
    /// Source maturity date, when present.
    pub maturity_date: Option<CalendarDate>,
    /// Source expiration date, when present.
    pub expiration_date: Option<CalendarDate>,
    /// Last date on which the contract may trade, when present.
    pub last_trade_date: Option<CalendarDate>,
    /// Final settlement date, when separately supplied by the source.
    pub settlement_date: Option<CalendarDate>,
    /// First notice date, when present.
    pub first_notice_date: Option<CalendarDate>,
    /// Last notice date, when present.
    pub last_notice_date: Option<CalendarDate>,
    /// First delivery date, when present.
    pub first_delivery_date: Option<CalendarDate>,
    /// Last delivery date, when present.
    pub last_delivery_date: Option<CalendarDate>,
}

impl FuturesLifecycleDates {
    /// Constructs lifecycle dates without inventing absent fields.
    ///
    /// # Errors
    ///
    /// Rejects last trade after expiration, a reversed notice range, or a reversed delivery range.
    /// An empty set is valid and faithfully represents a source record with no lifecycle fields.
    pub fn try_new(fields: FuturesLifecycleDateFields) -> Result<Self, IdentifierError> {
        let FuturesLifecycleDateFields {
            first_trade_date,
            maturity_date,
            expiration_date,
            last_trade_date,
            settlement_date,
            first_notice_date,
            last_notice_date,
            first_delivery_date,
            last_delivery_date,
        } = fields;
        if first_trade_date
            .zip(last_trade_date)
            .is_some_and(|(first, last)| first > last)
        {
            return Err(IdentifierError::FirstTradeAfterLastTrade);
        }
        if first_trade_date
            .zip(expiration_date)
            .is_some_and(|(first, expiration)| first > expiration)
        {
            return Err(IdentifierError::FirstTradeAfterExpiration);
        }
        if first_trade_date
            .zip(settlement_date)
            .is_some_and(|(first, settlement)| first > settlement)
        {
            return Err(IdentifierError::FirstTradeAfterSettlement);
        }
        if last_trade_date
            .zip(expiration_date)
            .is_some_and(|(last, expiration)| last > expiration)
        {
            return Err(IdentifierError::LastTradeAfterExpiration);
        }
        if last_trade_date
            .zip(settlement_date)
            .is_some_and(|(last, settlement)| last > settlement)
        {
            return Err(IdentifierError::LastTradeAfterSettlement);
        }
        if expiration_date
            .zip(settlement_date)
            .is_some_and(|(expiration, settlement)| expiration > settlement)
        {
            return Err(IdentifierError::ExpirationAfterSettlement);
        }
        if maturity_date
            .zip(settlement_date)
            .is_some_and(|(maturity, settlement)| maturity > settlement)
        {
            return Err(IdentifierError::MaturityAfterSettlement);
        }
        if first_notice_date
            .zip(last_notice_date)
            .is_some_and(|(first, last)| first > last)
        {
            return Err(IdentifierError::FirstNoticeAfterLastNotice);
        }
        if first_delivery_date
            .zip(last_delivery_date)
            .is_some_and(|(first, last)| first > last)
        {
            return Err(IdentifierError::FirstDeliveryAfterLastDelivery);
        }
        Ok(Self {
            first_trade_date,
            maturity_date,
            expiration_date,
            last_trade_date,
            settlement_date,
            first_notice_date,
            last_notice_date,
            first_delivery_date,
            last_delivery_date,
        })
    }

    /// Returns whether the source supplied no lifecycle date fields.
    pub const fn is_empty(&self) -> bool {
        self.first_trade_date.is_none()
            && self.maturity_date.is_none()
            && self.expiration_date.is_none()
            && self.last_trade_date.is_none()
            && self.settlement_date.is_none()
            && self.first_notice_date.is_none()
            && self.last_notice_date.is_none()
            && self.first_delivery_date.is_none()
            && self.last_delivery_date.is_none()
    }

    /// Returns the source-supplied first trading date.
    pub const fn first_trade_date(&self) -> Option<CalendarDate> {
        self.first_trade_date
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

    /// Returns the source-supplied final settlement date.
    pub const fn settlement_date(&self) -> Option<CalendarDate> {
        self.settlement_date
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

impl<'de> Deserialize<'de> for FuturesLifecycleDates {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields = FuturesLifecycleDateFields::deserialize(deserializer)?;
        Self::try_new(fields).map_err(serde::de::Error::custom)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    maturity_month_year: Option<MaturityMonthYear>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    maturity_date: Option<CalendarDate>,
    side: FuturesLegSide,
    ratio: NonZeroU32,
}

/// Complete FIX/source fields for one [`FuturesLeg`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FuturesLegInput {
    /// One-based source order in the multileg definition.
    pub position: u16,
    /// Source-native security identifier for the leg.
    pub security_id: ProviderInstrumentId,
    /// Scheme qualifying the source-native security identifier.
    pub security_id_source: SourceIdentifier,
    /// FIX `LegMaturityMonthYear(610)`, when supplied.
    #[serde(default)]
    pub maturity_month_year: Option<MaturityMonthYear>,
    /// FIX `LegMaturityDate(611)`, retained separately from tag 610.
    #[serde(default)]
    pub maturity_date: Option<CalendarDate>,
    /// Economic side of this component.
    pub side: FuturesLegSide,
    /// Nonzero integer leg ratio.
    pub ratio: u32,
}

impl FuturesLeg {
    /// Constructs a leg with an explicit one-based position and nonzero ratio.
    ///
    /// # Errors
    ///
    /// Rejects position zero or ratio zero.
    pub fn try_new(input: FuturesLegInput) -> Result<Self, IdentifierError> {
        let FuturesLegInput {
            position,
            security_id,
            security_id_source,
            maturity_month_year,
            maturity_date,
            side,
            ratio,
        } = input;
        Ok(Self {
            position: NonZeroU16::new(position).ok_or(IdentifierError::ZeroLegPosition)?,
            security_id,
            security_id_source,
            maturity_month_year,
            maturity_date,
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

    /// Returns FIX `LegMaturityMonthYear(610)` without collapsing its month/day/week form.
    pub const fn maturity_month_year(&self) -> Option<MaturityMonthYear> {
        self.maturity_month_year
    }

    /// Returns FIX `LegMaturityDate(611)`, separately from tag 610.
    pub const fn maturity_date(&self) -> Option<CalendarDate> {
        self.maturity_date
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

impl<'de> Deserialize<'de> for FuturesLeg {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = FuturesLegInput::deserialize(deserializer)?;
        Self::try_new(input).map_err(serde::de::Error::custom)
    }
}

/// A structured futures identity; deliberately not a universal root/month-symbol parser.
///
/// CFTC large-trader layouts and CME security definitions keep exchange security identifiers,
/// identifier sources, product codes, native symbols, and expiry fields separate. Existence and
/// contract economics require licensed venue reference data.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct FuturesContractIdentity {
    source_id: SourceId,
    source_reference: PayloadReference,
    source_timestamp: Option<Timestamp>,
    observed_at: Timestamp,
    metadata_revision: SourceIdentifier,
    venue_id: VenueId,
    security_id: ProviderInstrumentId,
    security_id_source: SourceIdentifier,
    product_code: ProviderInstrumentId,
    native_symbol: VenueSymbol,
    security_type: FuturesSecurityType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    maturity_month_year: Option<MaturityMonthYear>,
    #[serde(default, skip_serializing_if = "FuturesLifecycleDates::is_empty")]
    lifecycle: FuturesLifecycleDates,
    legs: Vec<FuturesLeg>,
}

/// Complete source-field input for constructing [`FuturesContractIdentity`].
///
/// `maturity_month_year` is optional because full maturity dates and leg-level maturity fields are
/// independent source fields. See FIX [`MaturityMonthYear (200)`], [`MaturityDate (541)`],
/// [`LegMaturityMonthYear (610)`], and [`LegMaturityDate (611)`].
///
/// [`MaturityMonthYear (200)`]: https://fiximate.fixtrading.org/en/FIX.Latest/tag200.html
/// [`MaturityDate (541)`]: https://fiximate.fixtrading.org/en/FIX.Latest/tag541.html
/// [`LegMaturityMonthYear (610)`]: https://fiximate.fixtrading.org/en/FIX.Latest/tag610.html
/// [`LegMaturityDate (611)`]: https://fiximate.fixtrading.org/en/FIX.Latest/tag611.html
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FuturesContractIdentityInput {
    /// Reference-data source namespace.
    pub source_id: SourceId,
    /// Immutable reference to the exact security-definition payload.
    pub source_reference: PayloadReference,
    /// Source-authored timestamp when the definition carries one.
    #[serde(default)]
    pub source_timestamp: Option<Timestamp>,
    /// Local first-observation timestamp for this payload.
    pub observed_at: Timestamp,
    /// Immutable provider publication, version, or mapping revision identifier.
    pub metadata_revision: SourceIdentifier,
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
    /// FIX `MaturityMonthYear(200)`, preserving month/day/week form exactly.
    #[serde(default)]
    pub maturity_month_year: Option<MaturityMonthYear>,
    /// Optional lifecycle dates, including FIX `MaturityDate(541)` separately from tag 200.
    #[serde(default)]
    pub lifecycle: FuturesLifecycleDates,
    /// Ordered legs for a multileg security.
    #[serde(default)]
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
            source_id,
            source_reference,
            source_timestamp,
            observed_at,
            metadata_revision,
            venue_id,
            security_id,
            security_id_source,
            product_code,
            native_symbol,
            security_type,
            maturity_month_year,
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
                    && candidate.maturity_month_year == leg.maturity_month_year
                    && candidate.maturity_date == leg.maturity_date
            }) {
                return Err(IdentifierError::DuplicateLeg);
            }
        }
        Ok(Self {
            source_id,
            source_reference,
            source_timestamp,
            observed_at,
            metadata_revision,
            venue_id,
            security_id,
            security_id_source,
            product_code,
            native_symbol,
            security_type,
            maturity_month_year,
            lifecycle,
            legs,
        })
    }

    /// Returns the reference-data source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the immutable source payload evidence.
    pub const fn source_reference(&self) -> &PayloadReference {
        &self.source_reference
    }

    /// Returns the source-authored timestamp when supplied.
    pub const fn source_timestamp(&self) -> Option<Timestamp> {
        self.source_timestamp
    }

    /// Returns when Market Squawk first observed this security definition.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns the immutable source metadata revision.
    pub const fn metadata_revision(&self) -> &SourceIdentifier {
        &self.metadata_revision
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

    /// Returns FIX `MaturityMonthYear(200)` without collapsing its month/day/week form.
    pub const fn maturity_month_year(&self) -> Option<MaturityMonthYear> {
        self.maturity_month_year
    }

    /// Returns the venue reference-data security type.
    pub const fn security_type(&self) -> FuturesSecurityType {
        self.security_type
    }

    /// Returns lifecycle dates exactly as supplied, including an empty set.
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
