//! Provider-neutral option-market observations with explicit component availability.
//!
//! Option chains are neither live events nor historical bars. This module retains resolved
//! option and underlying identities, exact contract terms, independently timed observation
//! components, and explicit unavailable states without admitting provider-shaped payloads.

use std::fmt;

use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    CalendarDate, EvidenceDigest, InstrumentId, Money, OccOptionIdentity, OptionKind,
    ProviderInstrumentId, QuantityLots, SourceIdentifier, Timestamp,
};

/// Maximum source-authored trade conditions retained by one option snapshot.
pub const MAX_OPTION_TRADE_CONDITIONS: usize = 32;

/// Why one independently meaningful option component has no admitted value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionComponentState {
    /// The provider omitted the field from the response shape.
    ProviderAbsent,
    /// The provider supplied an explicit null.
    ProviderNull,
    /// The provider contract says the component does not apply.
    NotApplicable,
    /// The value was intentionally omitted or redacted by the source.
    Omitted,
    /// A supplied value could not satisfy the canonical component contract.
    Invalid,
    /// Required interpretation or identity evidence is not yet resolved.
    Unresolved,
}

/// One option-market component with its own value state and optional source time.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum OptionComponent<T> {
    /// An admitted exact component value.
    Observed {
        /// Exact normalized value.
        value: T,
        /// Provider component time when supplied; outer response time is never substituted.
        source_at: Option<Timestamp>,
    },
    /// A retained non-value state.
    Unavailable {
        /// Exact reason no value was admitted.
        reason: OptionComponentState,
        /// Provider component time when a missing-state record itself carries one.
        source_at: Option<Timestamp>,
    },
}

impl<T> OptionComponent<T> {
    /// Constructs an observed component without inventing a source timestamp.
    pub const fn observed(value: T, source_at: Option<Timestamp>) -> Self {
        Self::Observed { value, source_at }
    }

    /// Constructs an explicit unavailable component.
    pub const fn unavailable(reason: OptionComponentState, source_at: Option<Timestamp>) -> Self {
        Self::Unavailable { reason, source_at }
    }

    /// Returns the admitted value, when present.
    pub const fn value(&self) -> Option<&T> {
        match self {
            Self::Observed { value, .. } => Some(value),
            Self::Unavailable { .. } => None,
        }
    }

    /// Returns the unavailable reason, when no value was admitted.
    pub const fn unavailable_reason(&self) -> Option<OptionComponentState> {
        match self {
            Self::Observed { .. } => None,
            Self::Unavailable { reason, .. } => Some(*reason),
        }
    }

    /// Returns the provider component time without falling back to an outer response time.
    pub const fn source_at(&self) -> Option<Timestamp> {
        match self {
            Self::Observed { source_at, .. } | Self::Unavailable { source_at, .. } => *source_at,
        }
    }
}

/// Source-neutral option exercise style.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", content = "source_value", rename_all = "snake_case")]
pub enum OptionExerciseStyle {
    /// Exercise is permitted through expiration under American-style terms.
    American,
    /// Exercise occurs only at expiration under European-style terms.
    European,
    /// Exercise is permitted on a finite evidenced schedule.
    Bermudan,
    /// A source-defined style whose exact admitted label remains evidence-bound.
    Other(SourceIdentifier),
}

/// Source-neutral option settlement kind.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", content = "source_value", rename_all = "snake_case")]
pub enum OptionSettlementKind {
    /// Exercise or assignment settles by delivery of the underlying.
    Physical,
    /// Exercise or assignment settles in cash.
    Cash,
    /// A source-defined settlement contract whose exact label remains evidence-bound.
    Other(SourceIdentifier),
}

/// Source-neutral expiration classification retained only when evidenced.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", content = "source_value", rename_all = "snake_case")]
pub enum OptionExpirationClass {
    /// Standard listed expiration under the applicable contract rules.
    Standard,
    /// Weekly expiration.
    Weekly,
    /// Monthly expiration.
    Monthly,
    /// Quarterly expiration.
    Quarterly,
    /// End-of-month expiration.
    EndOfMonth,
    /// A source-defined classification whose exact label remains evidence-bound.
    Other(SourceIdentifier),
}

/// Complete input for checked option-contract terms.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionContractTermsInput {
    /// Stable canonical option identity.
    pub option_instrument_id: InstrumentId,
    /// Stable canonical economic-underlying identity.
    pub underlying_instrument_id: InstrumentId,
    /// Exact option definition revision used to interpret the observation.
    pub option_definition_revision: EvidenceDigest,
    /// Exact underlying definition revision used to interpret the observation.
    pub underlying_definition_revision: EvidenceDigest,
    /// Exact source-qualified option contract identity.
    pub provider_instrument_id: ProviderInstrumentId,
    /// OCC/OSI identity when independently evidenced and resolved.
    pub occ_identity: Option<OccOptionIdentity>,
    /// Civil expiration date without an invented time zone or time of day.
    pub expiration: CalendarDate,
    /// Exact strike amount and currency.
    pub strike: Money,
    /// Call or put contract kind.
    pub kind: OptionKind,
    /// Exact positive contract multiplier.
    pub multiplier: Decimal,
    /// Exercise-style component and its own source time/state.
    pub exercise_style: OptionComponent<OptionExerciseStyle>,
    /// Settlement component and its own source time/state.
    pub settlement: OptionComponent<OptionSettlementKind>,
}

/// Resolved, exact option-contract terms shared by chain and contract reads.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OptionContractTerms {
    option_instrument_id: InstrumentId,
    underlying_instrument_id: InstrumentId,
    option_definition_revision: EvidenceDigest,
    underlying_definition_revision: EvidenceDigest,
    provider_instrument_id: ProviderInstrumentId,
    occ_identity: Option<OccOptionIdentity>,
    expiration: CalendarDate,
    strike: Money,
    kind: OptionKind,
    multiplier: Decimal,
    exercise_style: OptionComponent<OptionExerciseStyle>,
    settlement: OptionComponent<OptionSettlementKind>,
}

impl OptionContractTerms {
    /// Validates resolved identities and exact economic terms.
    pub fn try_new(input: OptionContractTermsInput) -> Result<Self, OptionMarketError> {
        if input.option_instrument_id == input.underlying_instrument_id {
            return Err(OptionMarketError::SelfUnderlying);
        }
        require_evidence(input.option_definition_revision)?;
        require_evidence(input.underlying_definition_revision)?;
        if input.strike.amount().is_sign_negative() {
            return Err(OptionMarketError::NegativeStrike);
        }
        if input.multiplier <= Decimal::ZERO {
            return Err(OptionMarketError::NonPositiveMultiplier);
        }
        if let Some(identity) = input.occ_identity.as_ref() {
            if identity.kind() != input.kind {
                return Err(OptionMarketError::OccKindMismatch);
            }
            let strike_thousandths = i64::try_from(identity.strike_thousandths())
                .map_err(|_| OptionMarketError::OccStrikeMismatch)?;
            let occ_strike = Decimal::new(strike_thousandths, 3).normalize();
            if input.strike.amount() != occ_strike {
                return Err(OptionMarketError::OccStrikeMismatch);
            }
            if identity.expiration_month() != input.expiration.month()
                || identity.expiration_day() != input.expiration.day()
                || u16::from(identity.expiration_yy()) != input.expiration.year() % 100
            {
                return Err(OptionMarketError::OccExpirationMismatch);
            }
        }
        Ok(Self {
            option_instrument_id: input.option_instrument_id,
            underlying_instrument_id: input.underlying_instrument_id,
            option_definition_revision: input.option_definition_revision,
            underlying_definition_revision: input.underlying_definition_revision,
            provider_instrument_id: input.provider_instrument_id,
            occ_identity: input.occ_identity,
            expiration: input.expiration,
            strike: input.strike,
            kind: input.kind,
            multiplier: input.multiplier.normalize(),
            exercise_style: input.exercise_style,
            settlement: input.settlement,
        })
    }

    /// Returns the stable canonical option identity.
    pub const fn option_instrument_id(&self) -> InstrumentId {
        self.option_instrument_id
    }

    /// Returns the stable canonical underlying identity.
    pub const fn underlying_instrument_id(&self) -> InstrumentId {
        self.underlying_instrument_id
    }

    /// Returns the exact option definition revision.
    pub const fn option_definition_revision(&self) -> EvidenceDigest {
        self.option_definition_revision
    }

    /// Returns the exact underlying definition revision.
    pub const fn underlying_definition_revision(&self) -> EvidenceDigest {
        self.underlying_definition_revision
    }

    /// Returns the exact source-qualified option identity.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns independently evidenced OCC identity when available.
    pub const fn occ_identity(&self) -> Option<&OccOptionIdentity> {
        self.occ_identity.as_ref()
    }

    /// Returns the exact civil expiration date.
    pub const fn expiration(&self) -> CalendarDate {
        self.expiration
    }

    /// Returns the exact strike amount and currency.
    pub const fn strike(&self) -> Money {
        self.strike
    }

    /// Returns call or put kind.
    pub const fn kind(&self) -> OptionKind {
        self.kind
    }

    /// Returns the exact positive contract multiplier.
    pub const fn multiplier(&self) -> Decimal {
        self.multiplier
    }

    /// Returns independently stated exercise-style evidence.
    pub const fn exercise_style(&self) -> &OptionComponent<OptionExerciseStyle> {
        &self.exercise_style
    }

    /// Returns independently stated settlement evidence.
    pub const fn settlement(&self) -> &OptionComponent<OptionSettlementKind> {
        &self.settlement
    }
}

/// Underlying mark retained with the exact evidence that supplied or withheld it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OptionUnderlyingObservation {
    price: OptionComponent<Money>,
    evidence: EvidenceDigest,
}

impl OptionUnderlyingObservation {
    /// Constructs an evidence-bound underlying-price component.
    pub fn try_new(
        price: OptionComponent<Money>,
        evidence: EvidenceDigest,
    ) -> Result<Self, OptionMarketError> {
        require_evidence(evidence)?;
        if price
            .value()
            .is_some_and(|value| value.amount() <= Decimal::ZERO)
        {
            return Err(OptionMarketError::NonPositiveUnderlyingPrice);
        }
        Ok(Self { price, evidence })
    }

    /// Returns the underlying price or its explicit unavailable state.
    pub const fn price(&self) -> &OptionComponent<Money> {
        &self.price
    }

    /// Returns exact evidence for the underlying component, including a missing state.
    pub const fn evidence(&self) -> EvidenceDigest {
        self.evidence
    }
}

/// Complete input for one checked option snapshot row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionSnapshotObservationInput {
    /// Resolved contract terms and exact definition revisions.
    pub terms: OptionContractTerms,
    /// Bid price component.
    pub bid_price: OptionComponent<Money>,
    /// Bid size component.
    pub bid_size: OptionComponent<QuantityLots>,
    /// Ask price component.
    pub ask_price: OptionComponent<Money>,
    /// Ask size component.
    pub ask_size: OptionComponent<QuantityLots>,
    /// Last-trade price component.
    pub last_price: OptionComponent<Money>,
    /// Last-trade size component.
    pub last_size: OptionComponent<QuantityLots>,
    /// Provider mark component, kept distinct from executable quote sides.
    pub mark_price: OptionComponent<Money>,
    /// Bounded source-authored trade conditions.
    pub trade_conditions: OptionComponent<Box<[SourceIdentifier]>>,
    /// Provider volume and its own as-of time/state.
    pub volume: OptionComponent<u64>,
    /// Provider open interest and its own as-of time/state.
    pub open_interest: OptionComponent<u64>,
    /// Implied volatility component.
    pub implied_volatility: OptionComponent<Decimal>,
    /// Delta component.
    pub delta: OptionComponent<Decimal>,
    /// Gamma component.
    pub gamma: OptionComponent<Decimal>,
    /// Theta component.
    pub theta: OptionComponent<Decimal>,
    /// Vega component.
    pub vega: OptionComponent<Decimal>,
    /// Rho component.
    pub rho: OptionComponent<Decimal>,
    /// Exact underlying-price evidence retained separately from the option mark.
    pub underlying: OptionUnderlyingObservation,
}

/// One resolved option snapshot with independently timed and nullable components.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OptionSnapshotObservation {
    terms: OptionContractTerms,
    bid_price: OptionComponent<Money>,
    bid_size: OptionComponent<QuantityLots>,
    ask_price: OptionComponent<Money>,
    ask_size: OptionComponent<QuantityLots>,
    last_price: OptionComponent<Money>,
    last_size: OptionComponent<QuantityLots>,
    mark_price: OptionComponent<Money>,
    trade_conditions: OptionComponent<Box<[SourceIdentifier]>>,
    volume: OptionComponent<u64>,
    open_interest: OptionComponent<u64>,
    implied_volatility: OptionComponent<Decimal>,
    delta: OptionComponent<Decimal>,
    gamma: OptionComponent<Decimal>,
    theta: OptionComponent<Decimal>,
    vega: OptionComponent<Decimal>,
    rho: OptionComponent<Decimal>,
    underlying: OptionUnderlyingObservation,
}

impl OptionSnapshotObservation {
    /// Validates one complete canonical option-snapshot row.
    pub fn try_new(input: OptionSnapshotObservationInput) -> Result<Self, OptionMarketError> {
        let currency = input.terms.strike().currency();
        for component in [
            &input.bid_price,
            &input.ask_price,
            &input.last_price,
            &input.mark_price,
        ] {
            validate_option_price(component, currency)?;
        }
        if let (Some(bid), Some(ask)) = (input.bid_price.value(), input.ask_price.value()) {
            if bid.amount() > ask.amount() {
                return Err(OptionMarketError::CrossedQuote);
            }
        }
        if input
            .underlying
            .price()
            .value()
            .is_some_and(|price| price.currency() != currency)
        {
            return Err(OptionMarketError::CurrencyMismatch);
        }
        if input
            .implied_volatility
            .value()
            .is_some_and(Decimal::is_sign_negative)
        {
            return Err(OptionMarketError::NegativeImpliedVolatility);
        }
        validate_trade_conditions(&input.trade_conditions)?;
        Ok(Self {
            terms: input.terms,
            bid_price: input.bid_price,
            bid_size: input.bid_size,
            ask_price: input.ask_price,
            ask_size: input.ask_size,
            last_price: input.last_price,
            last_size: input.last_size,
            mark_price: input.mark_price,
            trade_conditions: input.trade_conditions,
            volume: input.volume,
            open_interest: input.open_interest,
            implied_volatility: normalize_decimal_component(input.implied_volatility),
            delta: normalize_decimal_component(input.delta),
            gamma: normalize_decimal_component(input.gamma),
            theta: normalize_decimal_component(input.theta),
            vega: normalize_decimal_component(input.vega),
            rho: normalize_decimal_component(input.rho),
            underlying: input.underlying,
        })
    }

    /// Returns resolved contract terms and identity revisions.
    pub const fn terms(&self) -> &OptionContractTerms {
        &self.terms
    }

    /// Returns the bid-price component.
    pub const fn bid_price(&self) -> &OptionComponent<Money> {
        &self.bid_price
    }

    /// Returns the bid-size component.
    pub const fn bid_size(&self) -> &OptionComponent<QuantityLots> {
        &self.bid_size
    }

    /// Returns the ask-price component.
    pub const fn ask_price(&self) -> &OptionComponent<Money> {
        &self.ask_price
    }

    /// Returns the ask-size component.
    pub const fn ask_size(&self) -> &OptionComponent<QuantityLots> {
        &self.ask_size
    }

    /// Returns the last-trade-price component.
    pub const fn last_price(&self) -> &OptionComponent<Money> {
        &self.last_price
    }

    /// Returns the last-trade-size component.
    pub const fn last_size(&self) -> &OptionComponent<QuantityLots> {
        &self.last_size
    }

    /// Returns the provider mark, distinct from executable quote sides.
    pub const fn mark_price(&self) -> &OptionComponent<Money> {
        &self.mark_price
    }

    /// Returns bounded source-authored trade conditions.
    pub const fn trade_conditions(&self) -> &OptionComponent<Box<[SourceIdentifier]>> {
        &self.trade_conditions
    }

    /// Returns provider volume and its own as-of state/time.
    pub const fn volume(&self) -> &OptionComponent<u64> {
        &self.volume
    }

    /// Returns provider open interest and its own as-of state/time.
    pub const fn open_interest(&self) -> &OptionComponent<u64> {
        &self.open_interest
    }

    /// Returns implied volatility independently of every Greek.
    pub const fn implied_volatility(&self) -> &OptionComponent<Decimal> {
        &self.implied_volatility
    }

    /// Returns delta independently of every other Greek.
    pub const fn delta(&self) -> &OptionComponent<Decimal> {
        &self.delta
    }

    /// Returns gamma independently of every other Greek.
    pub const fn gamma(&self) -> &OptionComponent<Decimal> {
        &self.gamma
    }

    /// Returns theta independently of every other Greek.
    pub const fn theta(&self) -> &OptionComponent<Decimal> {
        &self.theta
    }

    /// Returns vega independently of every other Greek.
    pub const fn vega(&self) -> &OptionComponent<Decimal> {
        &self.vega
    }

    /// Returns rho independently of every other Greek.
    pub const fn rho(&self) -> &OptionComponent<Decimal> {
        &self.rho
    }

    /// Returns exact underlying-price evidence.
    pub const fn underlying(&self) -> &OptionUnderlyingObservation {
        &self.underlying
    }
}

/// Complete input for one provider-neutral expiration observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionExpirationObservationInput {
    /// Stable canonical underlying identity.
    pub underlying_instrument_id: InstrumentId,
    /// Exact underlying definition revision.
    pub underlying_definition_revision: EvidenceDigest,
    /// Exact source-qualified underlying identity.
    pub provider_instrument_id: ProviderInstrumentId,
    /// Civil expiration date.
    pub expiration: CalendarDate,
    /// Optional evidenced expiration classification.
    pub class: OptionComponent<OptionExpirationClass>,
    /// Provider standard/nonstandard state when explicitly supplied.
    pub standard: OptionComponent<bool>,
}

/// One provider-neutral expiration made usable only after underlying resolution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OptionExpirationObservation {
    underlying_instrument_id: InstrumentId,
    underlying_definition_revision: EvidenceDigest,
    provider_instrument_id: ProviderInstrumentId,
    expiration: CalendarDate,
    class: OptionComponent<OptionExpirationClass>,
    standard: OptionComponent<bool>,
}

impl OptionExpirationObservation {
    /// Constructs an exact expiration observation without retaining derived days-to-expiration.
    pub fn try_new(input: OptionExpirationObservationInput) -> Result<Self, OptionMarketError> {
        require_evidence(input.underlying_definition_revision)?;
        Ok(Self {
            underlying_instrument_id: input.underlying_instrument_id,
            underlying_definition_revision: input.underlying_definition_revision,
            provider_instrument_id: input.provider_instrument_id,
            expiration: input.expiration,
            class: input.class,
            standard: input.standard,
        })
    }

    /// Returns the resolved underlying identity.
    pub const fn underlying_instrument_id(&self) -> InstrumentId {
        self.underlying_instrument_id
    }

    /// Returns the exact underlying definition revision.
    pub const fn underlying_definition_revision(&self) -> EvidenceDigest {
        self.underlying_definition_revision
    }

    /// Returns the exact source-qualified underlying identity.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the exact civil expiration date.
    pub const fn expiration(&self) -> CalendarDate {
        self.expiration
    }

    /// Returns source-evidenced expiration classification.
    pub const fn class(&self) -> &OptionComponent<OptionExpirationClass> {
        &self.class
    }

    /// Returns source-evidenced standard/nonstandard state.
    pub const fn standard(&self) -> &OptionComponent<bool> {
        &self.standard
    }
}

fn normalize_decimal_component(component: OptionComponent<Decimal>) -> OptionComponent<Decimal> {
    match component {
        OptionComponent::Observed { value, source_at } => {
            OptionComponent::observed(value.normalize(), source_at)
        }
        OptionComponent::Unavailable { reason, source_at } => {
            OptionComponent::unavailable(reason, source_at)
        }
    }
}

fn validate_option_price(
    component: &OptionComponent<Money>,
    currency: crate::Currency,
) -> Result<(), OptionMarketError> {
    if let Some(price) = component.value() {
        if price.currency() != currency {
            return Err(OptionMarketError::CurrencyMismatch);
        }
        if price.amount().is_sign_negative() {
            return Err(OptionMarketError::NegativeOptionPrice);
        }
    }
    Ok(())
}

fn validate_trade_conditions(
    conditions: &OptionComponent<Box<[SourceIdentifier]>>,
) -> Result<(), OptionMarketError> {
    let Some(conditions) = conditions.value() else {
        return Ok(());
    };
    if conditions.len() > MAX_OPTION_TRADE_CONDITIONS {
        return Err(OptionMarketError::TradeConditionLimitExceeded);
    }
    for (index, condition) in conditions.iter().enumerate() {
        if conditions
            .iter()
            .skip(index + 1)
            .any(|other| other == condition)
        {
            return Err(OptionMarketError::DuplicateTradeCondition);
        }
    }
    Ok(())
}

fn require_evidence(evidence: EvidenceDigest) -> Result<(), OptionMarketError> {
    if evidence.bytes().iter().all(|byte| *byte == 0) {
        Err(OptionMarketError::EmptyEvidence)
    } else {
        Ok(())
    }
}

/// Option-market identity, term, or observation invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptionMarketError {
    /// Required exact evidence was the all-zero digest.
    EmptyEvidence,
    /// An option was incorrectly supplied as its own economic underlying.
    SelfUnderlying,
    /// A strike cannot be negative.
    NegativeStrike,
    /// A contract multiplier must be positive.
    NonPositiveMultiplier,
    /// OCC call/put identity disagreed with resolved terms.
    OccKindMismatch,
    /// OCC strike identity disagreed with resolved terms.
    OccStrikeMismatch,
    /// OCC expiration identity disagreed with resolved terms.
    OccExpirationMismatch,
    /// One option snapshot mixed currencies.
    CurrencyMismatch,
    /// A canonical option price cannot be negative.
    NegativeOptionPrice,
    /// An observed underlying price must be positive.
    NonPositiveUnderlyingPrice,
    /// A simultaneously observed bid exceeded the ask.
    CrossedQuote,
    /// Observed implied volatility cannot be negative.
    NegativeImpliedVolatility,
    /// A snapshot exceeded the hard trade-condition bound.
    TradeConditionLimitExceeded,
    /// A snapshot repeated the same source-authored trade condition.
    DuplicateTradeCondition,
}

impl fmt::Display for OptionMarketError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyEvidence => "option-market evidence must be nonzero",
            Self::SelfUnderlying => "an option cannot be its own underlying",
            Self::NegativeStrike => "option strike cannot be negative",
            Self::NonPositiveMultiplier => "option contract multiplier must be positive",
            Self::OccKindMismatch => "OCC option kind disagrees with resolved contract terms",
            Self::OccStrikeMismatch => "OCC strike disagrees with resolved contract terms",
            Self::OccExpirationMismatch => "OCC expiration disagrees with resolved contract terms",
            Self::CurrencyMismatch => "option snapshot currencies must match",
            Self::NegativeOptionPrice => "option price cannot be negative",
            Self::NonPositiveUnderlyingPrice => "observed underlying price must be positive",
            Self::CrossedQuote => "option bid cannot exceed ask",
            Self::NegativeImpliedVolatility => "implied volatility cannot be negative",
            Self::TradeConditionLimitExceeded => "option trade-condition bound exceeded",
            Self::DuplicateTradeCondition => "option trade conditions must be unique",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for OptionMarketError {}
