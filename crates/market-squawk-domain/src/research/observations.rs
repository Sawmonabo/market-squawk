//! Validated payloads carried by [`super::ResearchObservation`].

use rust_decimal::Decimal;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{Currency, EffectiveInterval, EvidenceDigest, Money, ProviderInstrumentId, Timestamp};

use super::{
    CorporateActionKind, FundamentalFactContext, PositionSide, QuantityLots, ResearchContext,
    ResearchError, SourceIdentifier, XbrlFactEvidence, require_instrument,
    validate_corporate_action,
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
    fact_context: FundamentalFactContext,
    xbrl_evidence: Option<Box<XbrlFactEvidence>>,
}

impl FundamentalObservation {
    /// Constructs an instrument-scoped exact fundamental observation.
    pub fn new(
        context: ResearchContext,
        concept: SourceIdentifier,
        value: Decimal,
        fact_context: FundamentalFactContext,
    ) -> Result<Self, ResearchError> {
        require_instrument(&context)?;
        fact_context.validate_research_context(&context)?;
        Ok(Self {
            context,
            concept,
            value: value.normalize(),
            fact_context,
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
        fact_context: FundamentalFactContext,
        xbrl_evidence: XbrlFactEvidence,
    ) -> Result<Self, ResearchError> {
        require_instrument(&context)?;
        fact_context.validate_research_context(&context)?;
        fact_context.validate_xbrl_evidence(&xbrl_evidence)?;
        xbrl_evidence.validate_observation(&concept, fact_context.unit(), value)?;
        Ok(Self {
            context,
            concept,
            value: value.normalize(),
            fact_context,
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

    /// Returns strict source-reported period, filing, fiscal, and revision context.
    pub const fn fact_context(&self) -> &FundamentalFactContext {
        &self.fact_context
    }

    /// Returns the source-native unit identity.
    pub const fn unit(&self) -> &SourceIdentifier {
        self.fact_context.unit()
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
    fact_context: FundamentalFactContext,
    xbrl_evidence: RequiredOption<XbrlFactEvidence>,
}

#[derive(Deserialize)]
#[serde(transparent)]
struct RequiredOption<T>(Option<T>);

impl<'de> Deserialize<'de> for FundamentalObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundamentalObservationWire::deserialize(deserializer)?;
        match wire.xbrl_evidence.0 {
            Some(evidence) => Self::new_with_xbrl_evidence(
                wire.context,
                wire.concept,
                wire.value,
                wire.fact_context,
                evidence,
            ),
            None => Self::new(wire.context, wire.concept, wire.value, wire.fact_context),
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

/// Corporate-action adjustment applied by a historical-bar provider.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketBarAdjustment {
    /// Preserve provider-reported raw prices.
    Raw,
    /// Apply split adjustments.
    Split,
    /// Apply cash-dividend adjustments.
    Dividend,
    /// Apply spin-off adjustments.
    SpinOff,
    /// Apply every provider-supported adjustment.
    All,
}

/// Provider timestamp boundary retained by one completed market bar.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BarTimestampBasis {
    /// The provider timestamp identifies the inclusive start of the aggregation period.
    PeriodStart,
    /// The provider timestamp identifies the exclusive end of the aggregation period.
    PeriodEnd,
}

/// Source-neutral trading-session class used to interpret one market-bar period.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketBarSessionKind {
    /// The venue's regular trading session.
    Regular,
    /// A session including trading outside the venue's regular session.
    Extended,
    /// A continuously traded market with no venue open/close boundary.
    Continuous,
    /// A provider-defined session whose exact rules are retained by evidence identity.
    ProviderDefined,
}

/// Exact nonzero identity of the session rules used to close one market bar.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct MarketBarSessionEvidence {
    kind: MarketBarSessionKind,
    ruleset: SourceIdentifier,
    evidence: EvidenceDigest,
}

impl MarketBarSessionEvidence {
    /// Constructs explicit session evidence.
    ///
    /// # Errors
    ///
    /// Rejects an all-zero digest, which cannot identify an admitted ruleset payload.
    pub fn try_new(
        kind: MarketBarSessionKind,
        ruleset: SourceIdentifier,
        evidence: EvidenceDigest,
    ) -> Result<Self, ResearchError> {
        if evidence.bytes() == [0; 32] {
            return Err(ResearchError::InvalidMarketBarSessionEvidence);
        }
        Ok(Self {
            kind,
            ruleset,
            evidence,
        })
    }

    /// Returns the source-neutral session class.
    pub const fn kind(&self) -> MarketBarSessionKind {
        self.kind
    }

    /// Returns the exact session-ruleset identity.
    pub const fn ruleset(&self) -> &SourceIdentifier {
        &self.ruleset
    }

    /// Returns the exact evidence digest for the ruleset payload.
    pub const fn evidence(&self) -> EvidenceDigest {
        self.evidence
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketBarSessionEvidenceWire {
    kind: MarketBarSessionKind,
    ruleset: SourceIdentifier,
    evidence: EvidenceDigest,
}

impl<'de> Deserialize<'de> for MarketBarSessionEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = MarketBarSessionEvidenceWire::deserialize(deserializer)?;
        Self::try_new(wire.kind, wire.ruleset, wire.evidence).map_err(serde::de::Error::custom)
    }
}

/// Complete aggregation-period and session semantics for one completed market bar.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct BarTimeSemantics {
    period_start: Timestamp,
    period_end_exclusive: Timestamp,
    timestamp_basis: BarTimestampBasis,
    session: MarketBarSessionEvidence,
}

impl BarTimeSemantics {
    /// Constructs one nonempty aggregation period without altering the provider timestamp anchor.
    ///
    /// # Errors
    ///
    /// Rejects an empty or reversed period.
    pub fn try_new(
        period_start: Timestamp,
        period_end_exclusive: Timestamp,
        timestamp_basis: BarTimestampBasis,
        session: MarketBarSessionEvidence,
    ) -> Result<Self, ResearchError> {
        if period_start >= period_end_exclusive {
            return Err(ResearchError::InvalidMarketBarTimeRange);
        }
        Ok(Self {
            period_start,
            period_end_exclusive,
            timestamp_basis,
            session,
        })
    }

    /// Returns the inclusive aggregation-period start.
    pub const fn period_start(&self) -> Timestamp {
        self.period_start
    }

    /// Returns the exclusive boundary at which the aggregation period is complete.
    pub const fn period_end_exclusive(&self) -> Timestamp {
        self.period_end_exclusive
    }

    /// Returns which exact period boundary the provider timestamp identifies.
    pub const fn timestamp_basis(&self) -> BarTimestampBasis {
        self.timestamp_basis
    }

    /// Returns exact evidence for the session rules used to determine the period.
    pub const fn session(&self) -> &MarketBarSessionEvidence {
        &self.session
    }

    /// Returns the exact provider timestamp boundary without rewriting it to bar completion.
    pub const fn provider_timestamp(&self) -> Timestamp {
        match self.timestamp_basis {
            BarTimestampBasis::PeriodStart => self.period_start,
            BarTimestampBasis::PeriodEnd => self.period_end_exclusive,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BarTimeSemanticsWire {
    period_start: Timestamp,
    period_end_exclusive: Timestamp,
    timestamp_basis: BarTimestampBasis,
    session: MarketBarSessionEvidence,
}

impl<'de> Deserialize<'de> for BarTimeSemantics {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BarTimeSemanticsWire::deserialize(deserializer)?;
        Self::try_new(
            wire.period_start,
            wire.period_end_exclusive,
            wire.timestamp_basis,
            wire.session,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Exact OHLCV market bar tied to canonical instrument, venue, provider, feed, and PIT evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MarketBarObservation {
    context: ResearchContext,
    provider_instrument_id: ProviderInstrumentId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    time_semantics: BarTimeSemantics,
    adjustment: MarketBarAdjustment,
    open: Money,
    high: Money,
    low: Money,
    close: Money,
    volume: Decimal,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trade_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    vwap: Option<Money>,
}

impl MarketBarObservation {
    /// Constructs one exact instrument- and venue-scoped bar.
    ///
    /// # Errors
    ///
    /// Rejects missing canonical identity, non-exact effective time, mixed currencies,
    /// nonpositive prices, negative volume, or prices outside the retained low/high envelope.
    #[allow(
        clippy::too_many_arguments,
        reason = "bar identity, adjustment, OHLCV, and optional provider fields stay explicit"
    )]
    pub fn new(
        context: ResearchContext,
        provider_instrument_id: ProviderInstrumentId,
        feed: SourceIdentifier,
        interval: SourceIdentifier,
        time_semantics: BarTimeSemantics,
        adjustment: MarketBarAdjustment,
        open: Money,
        high: Money,
        low: Money,
        close: Money,
        volume: Decimal,
        trade_count: Option<u64>,
        vwap: Option<Money>,
    ) -> Result<Self, ResearchError> {
        require_instrument(&context)?;
        if context.provenance().venue_id().is_none() {
            return Err(ResearchError::MissingVenue);
        }
        let Some(effective_at) = context.time().effective().exact_timestamp() else {
            return Err(ResearchError::MarketBarRequiresExactEffectiveTime);
        };
        let provider_timestamp = time_semantics.provider_timestamp();
        if effective_at != provider_timestamp
            || context.provenance().source_timestamp() != Some(provider_timestamp)
        {
            return Err(ResearchError::MarketBarProviderTimestampMismatch);
        }
        if context
            .provenance()
            .availability()
            .conservative_available_at()
            .is_none_or(|available_at| available_at < time_semantics.period_end_exclusive())
        {
            return Err(ResearchError::MarketBarUnavailableBeforeCompletion);
        }
        let currency = open.currency();
        if [high, low, close]
            .into_iter()
            .any(|price| price.currency() != currency)
            || vwap.is_some_and(|price| price.currency() != currency)
        {
            return Err(ResearchError::MarketBarCurrencyMismatch);
        }
        if [open, high, low, close]
            .into_iter()
            .any(|price| price.amount() <= Decimal::ZERO)
            || vwap.is_some_and(|price| price.amount() <= Decimal::ZERO)
        {
            return Err(ResearchError::NonPositiveMarketBarPrice);
        }
        if low.amount() > high.amount()
            || open.amount() < low.amount()
            || open.amount() > high.amount()
            || close.amount() < low.amount()
            || close.amount() > high.amount()
        {
            return Err(ResearchError::InvalidMarketBarRange);
        }
        if volume.is_sign_negative() {
            return Err(ResearchError::NegativeMarketBarVolume);
        }
        Ok(Self {
            context,
            provider_instrument_id,
            feed,
            interval,
            time_semantics,
            adjustment,
            open,
            high,
            low,
            close,
            volume: volume.normalize(),
            trade_count,
            vwap,
        })
    }

    /// Returns point-in-time context and provenance.
    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }

    /// Returns the exact source-native instrument identity validated against the canonical map.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the exact provider feed identity.
    pub const fn feed(&self) -> &SourceIdentifier {
        &self.feed
    }

    /// Returns the exact provider bar-interval identity.
    pub const fn interval(&self) -> &SourceIdentifier {
        &self.interval
    }

    /// Returns exact completed-period, provider-anchor, and session semantics.
    pub const fn time_semantics(&self) -> &BarTimeSemantics {
        &self.time_semantics
    }

    /// Returns the exclusive period end at which this bar became complete.
    pub const fn completed_at(&self) -> Timestamp {
        self.time_semantics.period_end_exclusive()
    }

    /// Returns the retained corporate-action adjustment policy.
    pub const fn adjustment(&self) -> MarketBarAdjustment {
        self.adjustment
    }

    /// Returns the opening price.
    pub const fn open(&self) -> Money {
        self.open
    }

    /// Returns the high price.
    pub const fn high(&self) -> Money {
        self.high
    }

    /// Returns the low price.
    pub const fn low(&self) -> Money {
        self.low
    }

    /// Returns the closing price.
    pub const fn close(&self) -> Money {
        self.close
    }

    /// Returns exact provider-reported volume.
    pub const fn volume(&self) -> Decimal {
        self.volume
    }

    /// Returns provider-reported trade count when supplied.
    pub const fn trade_count(&self) -> Option<u64> {
        self.trade_count
    }

    /// Returns provider-reported VWAP when supplied.
    pub const fn vwap(&self) -> Option<Money> {
        self.vwap
    }

    /// Returns the single price currency proven by the constructor.
    pub const fn currency(&self) -> Currency {
        self.open.currency()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketBarObservationWire {
    context: ResearchContext,
    provider_instrument_id: ProviderInstrumentId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    time_semantics: BarTimeSemantics,
    adjustment: MarketBarAdjustment,
    open: Money,
    high: Money,
    low: Money,
    close: Money,
    volume: Decimal,
    #[serde(default)]
    trade_count: Option<u64>,
    #[serde(default)]
    vwap: Option<Money>,
}

impl<'de> Deserialize<'de> for MarketBarObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = MarketBarObservationWire::deserialize(deserializer)?;
        Self::new(
            wire.context,
            wire.provider_instrument_id,
            wire.feed,
            wire.interval,
            wire.time_semantics,
            wire.adjustment,
            wire.open,
            wire.high,
            wire.low,
            wire.close,
            wire.volume,
            wire.trade_count,
            wire.vwap,
        )
        .map_err(serde::de::Error::custom)
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

/// Source-authored membership in one named instrument universe over a half-open interval.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct UniverseMembershipObservation {
    context: ResearchContext,
    universe: SourceIdentifier,
    effective_interval: EffectiveInterval,
}

impl UniverseMembershipObservation {
    /// Constructs instrument-scoped membership whose interval start equals its effective time.
    pub fn new(
        context: ResearchContext,
        universe: SourceIdentifier,
        effective_interval: EffectiveInterval,
    ) -> Result<Self, ResearchError> {
        require_instrument(&context)?;
        if context.time().effective().exact_timestamp() != Some(effective_interval.starts_at()) {
            return Err(ResearchError::UniverseIntervalStartMismatch);
        }
        Ok(Self {
            context,
            universe,
            effective_interval,
        })
    }

    /// Returns point-in-time context and provenance.
    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }

    /// Returns the source-authored universe identity.
    pub const fn universe(&self) -> &SourceIdentifier {
        &self.universe
    }

    /// Returns the source-authored half-open membership interval.
    pub const fn effective_interval(&self) -> EffectiveInterval {
        self.effective_interval
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UniverseMembershipObservationWire {
    context: ResearchContext,
    universe: SourceIdentifier,
    effective_interval: EffectiveInterval,
}

impl<'de> Deserialize<'de> for UniverseMembershipObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = UniverseMembershipObservationWire::deserialize(deserializer)?;
        Self::new(wire.context, wire.universe, wire.effective_interval)
            .map_err(serde::de::Error::custom)
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
