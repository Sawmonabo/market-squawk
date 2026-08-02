//! Exact decimal, rate, measurement-unit, and monetary-basis boundaries.

use market_squawk_domain::Money;
use rust_decimal::Decimal;

use crate::batch::validate_identifier;
use crate::{AnalyticsError, DecimalPolicy};

/// Semantic source scale normalized at an exact-decimal boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExactDecimalScale {
    /// Source is already expressed in whole units.
    Unit,
    /// Source uses percent notation, where `100` means one whole unit.
    Percent,
    /// Source uses basis points, where `10_000` means one whole unit.
    BasisPoints,
}

impl ExactDecimalScale {
    fn normalize(self, value: Decimal) -> Result<Decimal, AnalyticsError> {
        let divisor = match self {
            Self::Unit => Decimal::ONE,
            Self::Percent => Decimal::from(100_u32),
            Self::BasisPoints => Decimal::from(10_000_u32),
        };
        value
            .checked_div(divisor)
            .map(|normalized| normalized.normalize())
            .ok_or(AnalyticsError::DecimalArithmetic)
    }
}

/// Exact dimensionless rate normalized to whole-unit representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactRate {
    value: Decimal,
    source_scale: ExactDecimalScale,
}

impl ExactRate {
    /// Normalizes one exact rate while retaining its declared source scale.
    ///
    /// # Errors
    ///
    /// Returns [`AnalyticsError::DecimalArithmetic`] if scale normalization is not representable.
    pub fn try_new(
        source_value: Decimal,
        source_scale: ExactDecimalScale,
    ) -> Result<Self, AnalyticsError> {
        Ok(Self {
            value: source_scale.normalize(source_value)?,
            source_scale,
        })
    }

    /// Returns the whole-unit exact rate.
    #[must_use]
    pub const fn value(self) -> Decimal {
        self.value
    }

    /// Returns the scale declared at the source boundary.
    #[must_use]
    pub const fn source_scale(self) -> ExactDecimalScale {
        self.source_scale
    }
}

/// Measurement basis carried by a monetary financial fact.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MonetaryBasis {
    /// Aggregate amount for the reporting entity or position.
    Total,
    /// Amount per common-equivalent share.
    PerShare,
}

/// Exact money paired with its measurement basis.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MonetaryValue {
    money: Money,
    basis: MonetaryBasis,
}

impl MonetaryValue {
    /// Constructs an exact monetary measurement.
    #[must_use]
    pub const fn new(money: Money, basis: MonetaryBasis) -> Self {
        Self { money, basis }
    }

    /// Returns exact money and currency.
    #[must_use]
    pub const fn money(self) -> Money {
        self.money
    }

    /// Returns the declared monetary measurement basis.
    #[must_use]
    pub const fn basis(self) -> MonetaryBasis {
        self.basis
    }
}

/// Canonical identity of a non-monetary macro or alternative-data measurement.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MeasurementUnit(String);

impl MeasurementUnit {
    /// Constructs a bounded canonical unit identity such as `cpi.index` or `payroll.count`.
    ///
    /// # Errors
    ///
    /// Returns [`AnalyticsError::InvalidIdentifier`] for a non-canonical identity.
    pub fn try_new(value: &str) -> Result<Self, AnalyticsError> {
        validate_identifier(value)?;
        Ok(Self(value.to_owned()))
    }

    /// Returns the stable unit identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Exact decimal measurement normalized at a named-unit and source-scale boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecimalMeasurement {
    value: Decimal,
    unit: MeasurementUnit,
    source_scale: ExactDecimalScale,
}

impl DecimalMeasurement {
    /// Constructs a normalized exact measurement.
    ///
    /// # Errors
    ///
    /// Returns a checked decimal error when scale normalization is not representable.
    pub fn try_new(
        source_value: Decimal,
        unit: MeasurementUnit,
        source_scale: ExactDecimalScale,
    ) -> Result<Self, AnalyticsError> {
        Ok(Self {
            value: source_scale.normalize(source_value)?,
            unit,
            source_scale,
        })
    }

    /// Returns the normalized exact value.
    #[must_use]
    pub const fn value(&self) -> Decimal {
        self.value
    }

    /// Returns its named measurement unit.
    #[must_use]
    pub const fn unit(&self) -> &MeasurementUnit {
        &self.unit
    }

    /// Returns the source scale admitted at the boundary.
    #[must_use]
    pub const fn source_scale(&self) -> ExactDecimalScale {
        self.source_scale
    }
}

/// Semantic unit of one exact decimal analytical result.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExactDecimalUnit {
    /// Dimensionless ratio or multiple.
    Ratio,
    /// Whole-unit exact rate.
    Rate,
    /// Unitless standardized surprise.
    Standardized,
}

/// Exact decimal result carrying its output unit and rounding policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactDecimalResult {
    value: Decimal,
    unit: ExactDecimalUnit,
    policy: DecimalPolicy,
}

impl ExactDecimalResult {
    pub(crate) const fn new(value: Decimal, unit: ExactDecimalUnit, policy: DecimalPolicy) -> Self {
        Self {
            value,
            unit,
            policy,
        }
    }

    /// Returns the exact rounded value.
    #[must_use]
    pub const fn value(self) -> Decimal {
        self.value
    }

    /// Returns the semantic output unit.
    #[must_use]
    pub const fn unit(self) -> ExactDecimalUnit {
        self.unit
    }

    /// Returns the exact rounding policy used to produce the result.
    #[must_use]
    pub const fn policy(self) -> DecimalPolicy {
        self.policy
    }
}
