//! Invariant-preserving order vocabulary and immutable instrument execution terms.

use std::fmt;
use std::num::NonZeroU64;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Currency, Denomination, InstrumentId, LotSize, TickSize};

/// Monotonic nonzero revision of an instrument definition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InstrumentDefinitionRevision(NonZeroU64);

impl InstrumentDefinitionRevision {
    /// Returns the nonzero revision.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for InstrumentDefinitionRevision {
    type Error = OrderContractError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(OrderContractError::ZeroDefinitionRevision)
    }
}

impl Serialize for InstrumentDefinitionRevision {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.get())
    }
}

impl<'de> Deserialize<'de> for InstrumentDefinitionRevision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::try_from(u64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Immutable, revision-bound financial terms required for order validation and accounting.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct InstrumentExecutionTerms {
    instrument_id: InstrumentId,
    definition_revision: InstrumentDefinitionRevision,
    price_tick: TickSize,
    lot_size: LotSize,
    quote_currency: Currency,
    settlement_denomination: Denomination,
    contract_multiplier: Decimal,
}

impl InstrumentExecutionTerms {
    /// Constructs exact execution terms.
    ///
    /// # Errors
    ///
    /// Returns [`OrderContractError::NonPositiveContractMultiplier`] unless the normalized exact
    /// multiplier is positive.
    #[allow(
        clippy::too_many_arguments,
        reason = "each execution term is an independently validated financial invariant"
    )]
    pub fn try_new(
        instrument_id: InstrumentId,
        definition_revision: InstrumentDefinitionRevision,
        price_tick: TickSize,
        lot_size: LotSize,
        quote_currency: Currency,
        settlement_denomination: Denomination,
        contract_multiplier: Decimal,
    ) -> Result<Self, OrderContractError> {
        if contract_multiplier <= Decimal::ZERO {
            return Err(OrderContractError::NonPositiveContractMultiplier);
        }
        Ok(Self {
            instrument_id,
            definition_revision,
            price_tick,
            lot_size,
            quote_currency,
            settlement_denomination,
            contract_multiplier: contract_multiplier.normalize(),
        })
    }

    /// Returns the bound stable instrument identity.
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the bound instrument-definition revision.
    pub const fn definition_revision(self) -> InstrumentDefinitionRevision {
        self.definition_revision
    }

    /// Returns the exact price increment.
    pub const fn price_tick(self) -> TickSize {
        self.price_tick
    }

    /// Returns the exact quantity increment.
    pub const fn lot_size(self) -> LotSize {
        self.lot_size
    }

    /// Returns the quote currency used to interpret price ticks.
    pub const fn quote_currency(self) -> Currency {
        self.quote_currency
    }

    /// Returns the typed settlement denomination.
    pub const fn settlement_denomination(self) -> Denomination {
        self.settlement_denomination
    }

    /// Returns the settlement currency, or `None` for explicitly non-currency settlement.
    pub const fn settlement_currency(self) -> Option<Currency> {
        match self.settlement_denomination {
            Denomination::Currency(currency) => Some(currency),
            Denomination::Asset(_) => None,
        }
    }

    /// Returns the exact positive contract multiplier.
    pub const fn contract_multiplier(self) -> Decimal {
        self.contract_multiplier
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstrumentExecutionTermsWire {
    instrument_id: InstrumentId,
    definition_revision: InstrumentDefinitionRevision,
    price_tick: TickSize,
    lot_size: LotSize,
    quote_currency: Currency,
    settlement_denomination: Denomination,
    contract_multiplier: Decimal,
}

impl<'de> Deserialize<'de> for InstrumentExecutionTerms {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = InstrumentExecutionTermsWire::deserialize(deserializer)?;
        Self::try_new(
            wire.instrument_id,
            wire.definition_revision,
            wire.price_tick,
            wire.lot_size,
            wire.quote_currency,
            wire.settlement_denomination,
            wire.contract_multiplier,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Buy or sell direction.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    /// Acquire quantity.
    Buy,
    /// Dispose of quantity.
    Sell,
}

/// Closed supported order type.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    /// Execute against available market liquidity.
    Market,
    /// Execute only at the limit price or better.
    Limit,
    /// Activate a market order at the stop price.
    Stop,
    /// Activate a limit order at the stop price.
    StopLimit,
}

/// Closed supported time-in-force policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    /// Expire at the configured venue-session boundary.
    Day,
    /// Remain active until canceled.
    GoodTilCancelled,
    /// Fill immediately available quantity and cancel the remainder.
    ImmediateOrCancel,
    /// Fill the complete quantity immediately or cancel without a fill.
    FillOrKill,
}

/// Bounded stable reason code retained with an order intent.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OrderReasonCode(String);

impl OrderReasonCode {
    /// Maximum encoded reason-code length.
    pub const MAX_LENGTH: usize = 96;

    /// Returns the validated code.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns bytes retained by the owned allocation.
    pub fn retained_bytes(&self) -> usize {
        self.0.capacity()
    }
}

impl TryFrom<&str> for OrderReasonCode {
    type Error = OrderContractError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_reason_code(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for OrderReasonCode {
    type Error = OrderContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_reason_code(&value)?;
        Ok(Self(value))
    }
}

impl fmt::Display for OrderReasonCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for OrderReasonCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for OrderReasonCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::try_from(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

fn validate_reason_code(value: &str) -> Result<(), OrderContractError> {
    if value.is_empty() {
        return Err(OrderContractError::EmptyReasonCode);
    }
    if value.len() > OrderReasonCode::MAX_LENGTH {
        return Err(OrderContractError::ReasonCodeTooLong {
            max: OrderReasonCode::MAX_LENGTH,
        });
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
    {
        return Err(OrderContractError::InvalidReasonCode);
    }
    Ok(())
}

/// Order-domain invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderContractError {
    /// Instrument revisions start at one.
    ZeroDefinitionRevision,
    /// A contract multiplier was zero or negative.
    NonPositiveContractMultiplier,
    /// A reason code was empty.
    EmptyReasonCode,
    /// A reason code exceeded its byte ceiling.
    ReasonCodeTooLong {
        /// Maximum accepted UTF-8 byte length.
        max: usize,
    },
    /// A reason code contained unsupported characters.
    InvalidReasonCode,
}

impl fmt::Display for OrderContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroDefinitionRevision => {
                formatter.write_str("instrument definition revision must be nonzero")
            }
            Self::NonPositiveContractMultiplier => {
                formatter.write_str("contract multiplier must be positive")
            }
            Self::EmptyReasonCode => formatter.write_str("order reason code must not be empty"),
            Self::ReasonCodeTooLong { max } => {
                write!(formatter, "order reason code exceeds {max} UTF-8 bytes")
            }
            Self::InvalidReasonCode => {
                formatter.write_str("order reason code contains unsupported characters")
            }
        }
    }
}

impl std::error::Error for OrderContractError {}
