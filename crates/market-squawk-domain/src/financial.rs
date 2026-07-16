use std::fmt;

use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A general financial-value invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinancialError {
    /// A tick or lot increment was zero or negative.
    NonPositiveIncrement,
    /// A currency code was not exactly three ASCII letters.
    InvalidCurrency,
    /// Arithmetic combined amounts denominated in different currencies.
    CurrencyMismatch {
        /// Currency of the left operand.
        left: Currency,
        /// Currency of the right operand.
        right: Currency,
    },
    /// Checked decimal arithmetic exceeded the supported representation.
    Overflow,
    /// A requested decimal scale exceeds the exact decimal representation.
    UnsupportedScale {
        /// Requested decimal scale.
        scale: u32,
        /// Maximum exact decimal scale.
        max: u32,
    },
    /// Price conversion failed.
    Price(PriceError),
    /// Quantity conversion failed.
    Quantity(QuantityError),
}

impl fmt::Display for FinancialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonPositiveIncrement => {
                formatter.write_str("tick and lot increments must be positive")
            }
            Self::InvalidCurrency => {
                formatter.write_str("currency must contain exactly three ASCII letters")
            }
            Self::CurrencyMismatch { left, right } => {
                write!(formatter, "currency mismatch: {left} and {right}")
            }
            Self::Overflow => formatter.write_str("financial arithmetic overflow"),
            Self::UnsupportedScale { scale, max } => {
                write!(
                    formatter,
                    "decimal scale {scale} exceeds maximum scale {max}"
                )
            }
            Self::Price(error) => error.fmt(formatter),
            Self::Quantity(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for FinancialError {}

impl From<PriceError> for FinancialError {
    fn from(value: PriceError) -> Self {
        Self::Price(value)
    }
}

impl From<QuantityError> for FinancialError {
    fn from(value: QuantityError) -> Self {
        Self::Quantity(value)
    }
}

/// A price conversion or scaled-integer arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PriceError {
    /// The decimal value is not an integral multiple of the tick size.
    InexactTick,
    /// The result cannot be represented in the target numeric type.
    Overflow,
}

impl fmt::Display for PriceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InexactTick => formatter.write_str("price is not an exact multiple of tick size"),
            Self::Overflow => formatter.write_str("price arithmetic overflow"),
        }
    }
}

impl std::error::Error for PriceError {}

/// A quantity conversion or scaled-integer arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuantityError {
    /// Quantities in this domain type cannot be negative.
    NegativeQuantity,
    /// The decimal value is not an integral multiple of the lot size.
    InexactLot,
    /// The result cannot be represented in the target numeric type.
    Overflow,
}

impl fmt::Display for QuantityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeQuantity => formatter.write_str("quantity must not be negative"),
            Self::InexactLot => {
                formatter.write_str("quantity is not an exact multiple of lot size")
            }
            Self::Overflow => formatter.write_str("quantity arithmetic overflow"),
        }
    }
}

impl std::error::Error for QuantityError {}

/// An explicit policy for conversions that are authorized to round.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoundingPolicy {
    /// Round midpoint ties to the nearest even integer.
    NearestEven,
    /// Round any non-integral value away from zero.
    AwayFromZero,
    /// Truncate toward zero.
    TowardZero,
    /// Round toward negative infinity.
    Floor,
    /// Round toward positive infinity.
    Ceiling,
}

impl RoundingPolicy {
    const fn strategy(self) -> RoundingStrategy {
        match self {
            Self::NearestEven => RoundingStrategy::MidpointNearestEven,
            Self::AwayFromZero => RoundingStrategy::AwayFromZero,
            Self::TowardZero => RoundingStrategy::ToZero,
            Self::Floor => RoundingStrategy::ToNegativeInfinity,
            Self::Ceiling => RoundingStrategy::ToPositiveInfinity,
        }
    }
}

/// A strictly positive exact price increment.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TickSize(Decimal);

impl TickSize {
    /// Validates and normalizes an exact decimal tick size.
    ///
    /// # Errors
    ///
    /// Returns [`FinancialError::NonPositiveIncrement`] for zero or negative input.
    pub fn try_from_decimal(value: Decimal) -> Result<Self, FinancialError> {
        positive_normalized(value).map(Self)
    }

    /// Constructs the exact tick size 10 raised to `-scale`.
    ///
    /// # Errors
    ///
    /// Returns [`FinancialError::UnsupportedScale`] when `scale` exceeds the exact decimal
    /// representation instead of invoking a panicking decimal constructor.
    pub fn power_of_ten(scale: u32) -> Result<Self, FinancialError> {
        Decimal::try_new(1, scale)
            .map(Self)
            .map_err(|_| FinancialError::UnsupportedScale {
                scale,
                max: Decimal::MAX_SCALE,
            })
    }

    /// Returns the exact normalized decimal increment.
    pub const fn as_decimal(self) -> Decimal {
        self.0
    }
}

impl Serialize for TickSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        <Decimal as Serialize>::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for TickSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = <Decimal as Deserialize>::deserialize(deserializer)?;
        Self::try_from_decimal(value).map_err(serde::de::Error::custom)
    }
}

/// A strictly positive exact quantity increment.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LotSize(Decimal);

impl LotSize {
    /// Validates and normalizes an exact decimal lot size.
    ///
    /// # Errors
    ///
    /// Returns [`FinancialError::NonPositiveIncrement`] for zero or negative input.
    pub fn try_from_decimal(value: Decimal) -> Result<Self, FinancialError> {
        positive_normalized(value).map(Self)
    }

    /// Returns the exact normalized decimal increment.
    pub const fn as_decimal(self) -> Decimal {
        self.0
    }
}

impl Serialize for LotSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        <Decimal as Serialize>::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for LotSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = <Decimal as Deserialize>::deserialize(deserializer)?;
        Self::try_from_decimal(value).map_err(serde::de::Error::custom)
    }
}

fn positive_normalized(value: Decimal) -> Result<Decimal, FinancialError> {
    if value <= Decimal::ZERO {
        Err(FinancialError::NonPositiveIncrement)
    } else {
        Ok(value.normalize())
    }
}

/// A price represented as an integer number of instrument ticks.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PriceTicks(i64);

impl PriceTicks {
    /// Constructs a tick count. Negative prices remain representable for markets that permit them.
    pub const fn new(ticks: i64) -> Self {
        Self(ticks)
    }

    /// Returns the integer tick count.
    pub const fn get(self) -> i64 {
        self.0
    }

    /// Converts an exact provider decimal to ticks without rounding.
    ///
    /// # Errors
    ///
    /// Returns [`PriceError::InexactTick`] when `value` is not an integral tick count, or
    /// [`PriceError::Overflow`] when that count does not fit in `i64`.
    pub fn try_from_decimal(value: Decimal, tick: TickSize) -> Result<Self, PriceError> {
        let quotient = value.checked_div(tick.0).ok_or(PriceError::Overflow)?;
        if !quotient.fract().is_zero() {
            return Err(PriceError::InexactTick);
        }
        decimal_to_i64(quotient)
            .map(Self)
            .ok_or(PriceError::Overflow)
    }

    /// Converts a provider decimal using the caller-selected rounding policy.
    ///
    /// # Errors
    ///
    /// Returns [`PriceError::Overflow`] when the rounded tick count does not fit in `i64`.
    pub fn from_decimal_rounded(
        value: Decimal,
        tick: TickSize,
        policy: RoundingPolicy,
    ) -> Result<Self, PriceError> {
        let quotient = value.checked_div(tick.0).ok_or(PriceError::Overflow)?;
        let rounded = quotient.round_dp_with_strategy(0, policy.strategy());
        decimal_to_i64(rounded)
            .map(Self)
            .ok_or(PriceError::Overflow)
    }

    /// Converts ticks back to an exact provider decimal using checked arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`PriceError::Overflow`] when the decimal result is unrepresentable.
    pub fn checked_to_decimal(self, tick: TickSize) -> Result<Decimal, PriceError> {
        Decimal::from(self.0)
            .checked_mul(tick.0)
            .ok_or(PriceError::Overflow)
    }

    /// Adds tick counts using checked integer arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`PriceError::Overflow`] on integer overflow.
    pub fn checked_add(self, other: Self) -> Result<Self, PriceError> {
        self.0
            .checked_add(other.0)
            .map(Self)
            .ok_or(PriceError::Overflow)
    }

    /// Subtracts tick counts using checked integer arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`PriceError::Overflow`] on integer overflow.
    pub fn checked_sub(self, other: Self) -> Result<Self, PriceError> {
        self.0
            .checked_sub(other.0)
            .map(Self)
            .ok_or(PriceError::Overflow)
    }

    /// Multiplies a scaled price and quantity into currency-aware money.
    ///
    /// # Errors
    ///
    /// Returns a typed conversion or overflow error when either scaled value cannot be converted
    /// exactly or the decimal multiplication is unrepresentable.
    pub fn checked_mul_quantity(
        self,
        quantity: QuantityLots,
        tick: TickSize,
        lot: LotSize,
        currency: Currency,
    ) -> Result<Money, FinancialError> {
        let price = self.checked_to_decimal(tick)?.normalize();
        let units = quantity.checked_to_decimal(lot)?.normalize();
        let product_scale = price
            .scale()
            .checked_add(units.scale())
            .filter(|scale| *scale <= Decimal::MAX_SCALE)
            .ok_or(FinancialError::Overflow)?;
        let amount = price
            .checked_mul(units)
            .filter(|product| product.scale() == product_scale)
            .ok_or(FinancialError::Overflow)?;
        Ok(Money::new(amount, currency))
    }
}

/// A nonnegative quantity represented as an integer number of instrument lots.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct QuantityLots(i64);

impl QuantityLots {
    /// Constructs a nonnegative lot count.
    ///
    /// # Errors
    ///
    /// Returns [`QuantityError::NegativeQuantity`] for negative input.
    pub const fn new(lots: i64) -> Result<Self, QuantityError> {
        if lots < 0 {
            Err(QuantityError::NegativeQuantity)
        } else {
            Ok(Self(lots))
        }
    }

    /// Returns the integer lot count.
    pub const fn get(self) -> i64 {
        self.0
    }

    /// Converts an exact nonnegative provider decimal to lots without rounding.
    ///
    /// # Errors
    ///
    /// Rejects negative values, fractional lots, and values that exceed `i64`.
    pub fn try_from_decimal(value: Decimal, lot: LotSize) -> Result<Self, QuantityError> {
        if value.is_sign_negative() {
            return Err(QuantityError::NegativeQuantity);
        }
        let quotient = value.checked_div(lot.0).ok_or(QuantityError::Overflow)?;
        if !quotient.fract().is_zero() {
            return Err(QuantityError::InexactLot);
        }
        decimal_to_i64(quotient)
            .ok_or(QuantityError::Overflow)
            .and_then(Self::new)
    }

    /// Converts a provider decimal using the caller-selected rounding policy.
    ///
    /// # Errors
    ///
    /// Rejects negative provider values before rounding and values whose rounded lot count does
    /// not fit in `i64`.
    pub fn from_decimal_rounded(
        value: Decimal,
        lot: LotSize,
        policy: RoundingPolicy,
    ) -> Result<Self, QuantityError> {
        if value.is_sign_negative() {
            return Err(QuantityError::NegativeQuantity);
        }
        let quotient = value.checked_div(lot.0).ok_or(QuantityError::Overflow)?;
        let rounded = quotient.round_dp_with_strategy(0, policy.strategy());
        decimal_to_i64(rounded)
            .ok_or(QuantityError::Overflow)
            .and_then(Self::new)
    }

    /// Converts lots back to an exact provider decimal using checked arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`QuantityError::Overflow`] when the decimal result is unrepresentable.
    pub fn checked_to_decimal(self, lot: LotSize) -> Result<Decimal, QuantityError> {
        Decimal::from(self.0)
            .checked_mul(lot.0)
            .ok_or(QuantityError::Overflow)
    }

    /// Adds nonnegative lot counts using checked integer arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`QuantityError::Overflow`] on integer overflow.
    pub fn checked_add(self, other: Self) -> Result<Self, QuantityError> {
        self.0
            .checked_add(other.0)
            .ok_or(QuantityError::Overflow)
            .and_then(Self::new)
    }

    /// Subtracts nonnegative lot counts without permitting a negative result.
    ///
    /// # Errors
    ///
    /// Returns [`QuantityError::NegativeQuantity`] when `other` is larger than `self`.
    pub fn checked_sub(self, other: Self) -> Result<Self, QuantityError> {
        self.0
            .checked_sub(other.0)
            .ok_or(QuantityError::Overflow)
            .and_then(Self::new)
    }
}

impl Serialize for QuantityLots {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(self.0)
    }
}

impl<'de> Deserialize<'de> for QuantityLots {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = i64::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

fn decimal_to_i64(value: Decimal) -> Option<i64> {
    i64::try_from(value).ok()
}

/// A normalized three-letter currency code.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Currency([u8; 3]);

impl Currency {
    /// Returns the normalized uppercase currency code.
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap_or_default()
    }
}

impl TryFrom<&str> for Currency {
    type Error = FinancialError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bytes = value.as_bytes();
        if bytes.len() != 3 || !bytes.iter().all(u8::is_ascii_alphabetic) {
            return Err(FinancialError::InvalidCurrency);
        }
        Ok(Self([
            bytes[0].to_ascii_uppercase(),
            bytes[1].to_ascii_uppercase(),
            bytes[2].to_ascii_uppercase(),
        ]))
    }
}

impl fmt::Display for Currency {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for Currency {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Currency {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value.as_str()).map_err(serde::de::Error::custom)
    }
}

/// A signed interest-rate or spread quantity measured in basis points.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct BasisPoints(i32);

impl BasisPoints {
    /// Constructs a signed basis-point value.
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    /// Returns the primitive basis-point count.
    pub const fn get(self) -> i32 {
        self.0
    }

    /// Converts basis points to an exact decimal rate, where 100 basis points is 0.01.
    pub fn as_decimal_rate(self) -> Decimal {
        Decimal::new(i64::from(self.0), 4)
    }

    /// Adds basis points using checked integer arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`FinancialError::Overflow`] on integer overflow.
    pub fn checked_add(self, other: Self) -> Result<Self, FinancialError> {
        self.0
            .checked_add(other.0)
            .map(Self)
            .ok_or(FinancialError::Overflow)
    }

    /// Subtracts basis points using checked integer arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`FinancialError::Overflow`] on integer overflow.
    pub fn checked_sub(self, other: Self) -> Result<Self, FinancialError> {
        self.0
            .checked_sub(other.0)
            .map(Self)
            .ok_or(FinancialError::Overflow)
    }
}

/// An exact decimal amount paired with its currency.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Money {
    amount: Decimal,
    currency: Currency,
}

impl Money {
    /// Constructs currency-aware exact money.
    pub fn new(amount: Decimal, currency: Currency) -> Self {
        Self {
            amount: amount.normalize(),
            currency,
        }
    }

    /// Returns the exact decimal amount.
    pub const fn amount(self) -> Decimal {
        self.amount
    }

    /// Returns the amount's currency.
    pub const fn currency(self) -> Currency {
        self.currency
    }

    /// Adds amounts when their currencies match.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies and decimal overflow.
    pub fn checked_add(self, other: Self) -> Result<Self, FinancialError> {
        self.ensure_same_currency(other)?;
        let exact_scale = self.amount.scale().max(other.amount.scale());
        self.amount
            .checked_add(other.amount)
            .filter(|amount| amount.scale() == exact_scale)
            .map(|amount| Self::new(amount, self.currency))
            .ok_or(FinancialError::Overflow)
    }

    /// Subtracts amounts when their currencies match.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies and decimal overflow.
    pub fn checked_sub(self, other: Self) -> Result<Self, FinancialError> {
        self.ensure_same_currency(other)?;
        let exact_scale = self.amount.scale().max(other.amount.scale());
        self.amount
            .checked_sub(other.amount)
            .filter(|amount| amount.scale() == exact_scale)
            .map(|amount| Self::new(amount, self.currency))
            .ok_or(FinancialError::Overflow)
    }

    fn ensure_same_currency(self, other: Self) -> Result<(), FinancialError> {
        if self.currency == other.currency {
            Ok(())
        } else {
            Err(FinancialError::CurrencyMismatch {
                left: self.currency,
                right: other.currency,
            })
        }
    }
}
