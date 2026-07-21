//! Validated payloads carried by [`super::ResearchObservation`].

use rust_decimal::Decimal;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize};

use super::{
    CorporateActionKind, PositionSide, QuantityLots, ResearchContext, ResearchError,
    SourceIdentifier, XbrlFactEvidence, require_instrument, validate_corporate_action,
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
#[serde(deny_unknown_fields)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    xbrl_evidence: Option<Box<XbrlFactEvidence>>,
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
            xbrl_evidence: None,
        })
    }

    /// Constructs a numeric fundamental and binds rich XBRL occurrence evidence to its exact value.
    ///
    /// # Errors
    ///
    /// Rejects missing instrument identity or evidence that does not produce `value` after its
    /// retained Inline-XBRL scale and sign transforms.
    pub fn new_with_xbrl_evidence(
        context: ResearchContext,
        concept: SourceIdentifier,
        value: Decimal,
        unit: SourceIdentifier,
        xbrl_evidence: XbrlFactEvidence,
    ) -> Result<Self, ResearchError> {
        require_instrument(&context)?;
        xbrl_evidence.validate_observation(&concept, &unit, value)?;
        Ok(Self {
            context,
            concept,
            value: value.normalize(),
            unit,
            xbrl_evidence: Some(Box::new(xbrl_evidence)),
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

    /// Returns optional occurrence-level XBRL audit evidence.
    pub fn xbrl_evidence(&self) -> Option<&XbrlFactEvidence> {
        self.xbrl_evidence.as_deref()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundamentalObservationWire {
    context: ResearchContext,
    concept: SourceIdentifier,
    value: Decimal,
    unit: SourceIdentifier,
    #[serde(default)]
    xbrl_evidence: Option<XbrlFactEvidence>,
}

impl<'de> Deserialize<'de> for FundamentalObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundamentalObservationWire::deserialize(deserializer)?;
        match wire.xbrl_evidence {
            Some(evidence) => Self::new_with_xbrl_evidence(
                wire.context,
                wire.concept,
                wire.value,
                wire.unit,
                evidence,
            ),
            None => Self::new(wire.context, wire.concept, wire.value, wire.unit),
        }
        .map_err(serde::de::Error::custom)
    }
}

/// Provider-native evidence explaining why a macro series has no observed decimal value.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MacroMissingValue {
    marker: SourceIdentifier,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<SourceIdentifier>,
}

impl MacroMissingValue {
    /// Retains an exact bounded provider marker and optional provider reason code.
    pub const fn new(marker: SourceIdentifier, reason: Option<SourceIdentifier>) -> Self {
        Self { marker, reason }
    }

    /// Returns the exact provider lexical marker, such as `.` or `-`.
    pub const fn marker(&self) -> &SourceIdentifier {
        &self.marker
    }

    /// Returns the provider-native missing-value reason when supplied.
    pub const fn reason(&self) -> Option<&SourceIdentifier> {
        self.reason.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MacroValueKind {
    Observed(Decimal),
    Missing(MacroMissingValue),
}

/// Exact observed decimal or explicit provider-reported missing-value evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MacroValue {
    kind: MacroValueKind,
}

impl MacroValue {
    fn observed(value: Decimal) -> Self {
        Self {
            kind: MacroValueKind::Observed(value.normalize()),
        }
    }

    fn missing(value: MacroMissingValue) -> Self {
        Self {
            kind: MacroValueKind::Missing(value),
        }
    }

    /// Returns the exact normalized decimal when the provider reported an observation.
    pub const fn observed_value(&self) -> Option<Decimal> {
        match &self.kind {
            MacroValueKind::Observed(value) => Some(*value),
            MacroValueKind::Missing(_) => None,
        }
    }

    /// Returns provider-native missing-value evidence when no decimal was observed.
    pub const fn missing_value(&self) -> Option<&MacroMissingValue> {
        match &self.kind {
            MacroValueKind::Observed(_) => None,
            MacroValueKind::Missing(value) => Some(value),
        }
    }
}

/// Exact decimal or explicitly missing macroeconomic series value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MacroObservation {
    context: ResearchContext,
    series: SourceIdentifier,
    value: MacroValue,
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
            value: MacroValue::observed(value),
            unit,
        }
    }

    /// Constructs a macroeconomic observation with provider-native missing-value evidence.
    pub fn missing(
        context: ResearchContext,
        series: SourceIdentifier,
        missing: MacroMissingValue,
        unit: SourceIdentifier,
    ) -> Self {
        Self {
            context,
            series,
            value: MacroValue::missing(missing),
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

    /// Returns the exact observed-or-missing series value.
    pub const fn value(&self) -> &MacroValue {
        &self.value
    }

    /// Returns the source-native unit identity.
    pub const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }
}

impl Serialize for MacroObservation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("MacroObservation", 4)?;
        state.serialize_field("context", &self.context)?;
        state.serialize_field("series", &self.series)?;
        match &self.value.kind {
            MacroValueKind::Observed(value) => state.serialize_field("value", value)?,
            MacroValueKind::Missing(missing) => state.serialize_field("missing", missing)?,
        }
        state.serialize_field("unit", &self.unit)?;
        state.end()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MacroObservationWire {
    context: ResearchContext,
    series: SourceIdentifier,
    #[serde(default, deserialize_with = "deserialize_present_field")]
    value: PresentField<Decimal>,
    #[serde(default, deserialize_with = "deserialize_present_field")]
    missing: PresentField<MacroMissingValue>,
    unit: SourceIdentifier,
}

#[derive(Default)]
enum PresentField<T> {
    #[default]
    Absent,
    Present(T),
}

fn deserialize_present_field<'de, D, T>(deserializer: D) -> Result<PresentField<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(PresentField::Present)
}

impl<'de> Deserialize<'de> for MacroObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = MacroObservationWire::deserialize(deserializer)?;
        match (wire.value, wire.missing) {
            (PresentField::Present(value), PresentField::Absent) => {
                Ok(Self::new(wire.context, wire.series, value, wire.unit))
            }
            (PresentField::Absent, PresentField::Present(missing)) => {
                Ok(Self::missing(wire.context, wire.series, missing, wire.unit))
            }
            (PresentField::Present(_), PresentField::Present(_))
            | (PresentField::Absent, PresentField::Absent) => Err(serde::de::Error::custom(
                ResearchError::InvalidMacroValueState,
            )),
        }
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
