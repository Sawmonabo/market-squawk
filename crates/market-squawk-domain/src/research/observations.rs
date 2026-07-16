//! Validated payloads carried by [`super::ResearchObservation`].

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};

use super::{
    CorporateActionKind, PositionSide, QuantityLots, ResearchContext, ResearchError,
    SourceIdentifier, require_instrument, validate_corporate_action,
};

/// Regulatory or issuer filing identity and point-in-time context.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FilingObservation {
    context: ResearchContext,
    form_type: SourceIdentifier,
    accession: SourceIdentifier,
}

impl FilingObservation {
    /// Constructs an instrument-scoped filing observation.
    pub fn new(
        context: ResearchContext,
        form_type: SourceIdentifier,
        accession: SourceIdentifier,
    ) -> Result<Self, ResearchError> {
        require_instrument(&context)?;
        Ok(Self {
            context,
            form_type,
            accession,
        })
    }

    /// Returns point-in-time context and provenance.
    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }

    /// Returns the source-native filing form type.
    pub const fn form_type(&self) -> &SourceIdentifier {
        &self.form_type
    }

    /// Returns the source-native filing accession or object identity.
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }
}

#[derive(Deserialize)]
struct FilingObservationWire {
    context: ResearchContext,
    form_type: SourceIdentifier,
    accession: SourceIdentifier,
}

impl<'de> Deserialize<'de> for FilingObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FilingObservationWire::deserialize(deserializer)?;
        Self::new(wire.context, wire.form_type, wire.accession).map_err(serde::de::Error::custom)
    }
}

/// Exact decimal company fundamental fact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FundamentalObservation {
    context: ResearchContext,
    concept: SourceIdentifier,
    value: Decimal,
    unit: SourceIdentifier,
}

impl FundamentalObservation {
    /// Constructs an instrument-scoped exact fundamental observation.
    pub fn new(
        context: ResearchContext,
        concept: SourceIdentifier,
        value: Decimal,
        unit: SourceIdentifier,
    ) -> Result<Self, ResearchError> {
        require_instrument(&context)?;
        Ok(Self {
            context,
            concept,
            value: value.normalize(),
            unit,
        })
    }

    /// Returns the exact decimal value.
    pub const fn value(&self) -> Decimal {
        self.value
    }

    /// Returns point-in-time context and provenance.
    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }

    /// Returns the source-native concept identity.
    pub const fn concept(&self) -> &SourceIdentifier {
        &self.concept
    }

    /// Returns the source-native unit identity.
    pub const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }
}

#[derive(Deserialize)]
struct FundamentalObservationWire {
    context: ResearchContext,
    concept: SourceIdentifier,
    value: Decimal,
    unit: SourceIdentifier,
}

impl<'de> Deserialize<'de> for FundamentalObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundamentalObservationWire::deserialize(deserializer)?;
        Self::new(wire.context, wire.concept, wire.value, wire.unit)
            .map_err(serde::de::Error::custom)
    }
}

/// Exact decimal macroeconomic series value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MacroObservation {
    context: ResearchContext,
    series: SourceIdentifier,
    value: Decimal,
    unit: SourceIdentifier,
}

impl MacroObservation {
    /// Constructs a macroeconomic observation with explicit unit.
    pub fn new(
        context: ResearchContext,
        series: SourceIdentifier,
        value: Decimal,
        unit: SourceIdentifier,
    ) -> Self {
        Self {
            context,
            series,
            value: value.normalize(),
            unit,
        }
    }

    /// Returns point-in-time context and provenance.
    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }

    /// Returns the macro series identity.
    pub const fn series(&self) -> &SourceIdentifier {
        &self.series
    }

    /// Returns the exact decimal series value.
    pub const fn value(&self) -> Decimal {
        self.value
    }

    /// Returns the source-native unit identity.
    pub const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }
}

#[derive(Deserialize)]
struct MacroObservationWire {
    context: ResearchContext,
    series: SourceIdentifier,
    value: Decimal,
    unit: SourceIdentifier,
}

impl<'de> Deserialize<'de> for MacroObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = MacroObservationWire::deserialize(deserializer)?;
        Ok(Self::new(wire.context, wire.series, wire.value, wire.unit))
    }
}

/// Nonzero portfolio position with explicit long/short direction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PositionObservation {
    context: ResearchContext,
    account_id: SourceIdentifier,
    side: PositionSide,
    absolute_quantity: QuantityLots,
}

impl PositionObservation {
    /// Constructs an instrument-scoped nonzero position.
    pub fn new(
        context: ResearchContext,
        account_id: SourceIdentifier,
        side: PositionSide,
        absolute_quantity: QuantityLots,
    ) -> Result<Self, ResearchError> {
        require_instrument(&context)?;
        if absolute_quantity.get() == 0 {
            return Err(ResearchError::ZeroPosition);
        }
        Ok(Self {
            context,
            account_id,
            side,
            absolute_quantity,
        })
    }

    /// Returns the absolute lot quantity; direction is available through [`Self::side`].
    pub const fn absolute_quantity(&self) -> QuantityLots {
        self.absolute_quantity
    }

    /// Returns long or short direction.
    pub const fn side(&self) -> PositionSide {
        self.side
    }

    /// Returns point-in-time context and provenance.
    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }

    /// Returns the source-native account identity.
    pub const fn account_id(&self) -> &SourceIdentifier {
        &self.account_id
    }
}

#[derive(Deserialize)]
struct PositionObservationWire {
    context: ResearchContext,
    account_id: SourceIdentifier,
    side: PositionSide,
    absolute_quantity: QuantityLots,
}

impl<'de> Deserialize<'de> for PositionObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PositionObservationWire::deserialize(deserializer)?;
        Self::new(
            wire.context,
            wire.account_id,
            wire.side,
            wire.absolute_quantity,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Preserved source transaction identity and classification.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionObservation {
    context: ResearchContext,
    account_id: SourceIdentifier,
    transaction_type: SourceIdentifier,
    source_record_id: SourceIdentifier,
}

impl TransactionObservation {
    /// Constructs a source transaction without forcing heterogeneous transaction fields into one
    /// lossy amount representation.
    pub fn new(
        context: ResearchContext,
        account_id: SourceIdentifier,
        transaction_type: SourceIdentifier,
        source_record_id: SourceIdentifier,
    ) -> Self {
        Self {
            context,
            account_id,
            transaction_type,
            source_record_id,
        }
    }

    /// Returns point-in-time context and provenance.
    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }

    /// Returns the source-native account identity.
    pub const fn account_id(&self) -> &SourceIdentifier {
        &self.account_id
    }

    /// Returns the preserved source transaction classification.
    pub const fn transaction_type(&self) -> &SourceIdentifier {
        &self.transaction_type
    }

    /// Returns the immutable source transaction record identity.
    pub const fn source_record_id(&self) -> &SourceIdentifier {
        &self.source_record_id
    }
}

/// Corporate action obtained through point-in-time research ingestion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CorporateActionObservation {
    context: ResearchContext,
    action: CorporateActionKind,
}

impl CorporateActionObservation {
    /// Constructs an instrument-scoped action and validates relational variants.
    pub fn new(
        context: ResearchContext,
        action: CorporateActionKind,
    ) -> Result<Self, ResearchError> {
        validate_corporate_action(&context, &action)?;
        Ok(Self { context, action })
    }

    /// Returns point-in-time context and provenance.
    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }

    /// Returns the typed corporate action.
    pub const fn action(&self) -> &CorporateActionKind {
        &self.action
    }
}

#[derive(Deserialize)]
struct CorporateActionObservationWire {
    context: ResearchContext,
    action: CorporateActionKind,
}

impl<'de> Deserialize<'de> for CorporateActionObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = CorporateActionObservationWire::deserialize(deserializer)?;
        Self::new(wire.context, wire.action).map_err(serde::de::Error::custom)
    }
}

/// Exact scalar observation from an alternative dataset.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AlternativeDataObservation {
    context: ResearchContext,
    dataset: SourceIdentifier,
    field: SourceIdentifier,
    value: Decimal,
    unit: Option<SourceIdentifier>,
}

impl AlternativeDataObservation {
    /// Constructs a point-in-time alternative-data scalar without inventing a unit.
    pub fn new(
        context: ResearchContext,
        dataset: SourceIdentifier,
        field: SourceIdentifier,
        value: Decimal,
        unit: Option<SourceIdentifier>,
    ) -> Self {
        Self {
            context,
            dataset,
            field,
            value: value.normalize(),
            unit,
        }
    }

    /// Returns point-in-time context and provenance.
    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }

    /// Returns the dataset identity.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the observed field identity.
    pub const fn field(&self) -> &SourceIdentifier {
        &self.field
    }

    /// Returns the exact decimal value.
    pub const fn value(&self) -> Decimal {
        self.value
    }

    /// Returns the source-native unit when supplied.
    pub const fn unit(&self) -> Option<&SourceIdentifier> {
        self.unit.as_ref()
    }
}

#[derive(Deserialize)]
struct AlternativeDataObservationWire {
    context: ResearchContext,
    dataset: SourceIdentifier,
    field: SourceIdentifier,
    value: Decimal,
    unit: Option<SourceIdentifier>,
}

impl<'de> Deserialize<'de> for AlternativeDataObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = AlternativeDataObservationWire::deserialize(deserializer)?;
        Ok(Self::new(
            wire.context,
            wire.dataset,
            wire.field,
            wire.value,
            wire.unit,
        ))
    }
}
