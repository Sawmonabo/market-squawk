//! Validated, immutable strategy order intents.

use std::num::NonZeroU64;

use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, DataQuality, Denomination, InstrumentExecutionTerms,
    ModelId, OrderId, OrderReasonCode, OrderSide, OrderType, PriceTicks, QuantityLots, StrategyId,
    TimeInForce, Timestamp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Maximum number of distinct strategy reason codes retained with one intent.
pub const MAX_ORDER_REASON_CODES: usize = 8;

/// Maximum intent-selected slippage, equal to one hundred percent.
pub const MAX_INTENT_SLIPPAGE_BASIS_POINTS: i32 = 10_000;

/// Maximum UTF-8 bytes retained by an order target identity.
pub const MAX_ORDER_TARGET_ID_BYTES: usize = 128;

/// Bounded reference to exact target or decision content that produced an order intent.
///
/// This value is provenance only. It cannot approve risk, dispatch an order, or call an adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OrderTargetReference {
    target_id: Box<str>,
    revision: NonZeroU64,
    content_sha256: [u8; 32],
}

impl OrderTargetReference {
    /// Constructs a canonical bounded identity for nonzero immutable target content.
    ///
    /// Target identities begin with a lowercase ASCII letter and otherwise contain lowercase
    /// ASCII letters, digits, `.`, `_`, or `-`.
    pub fn try_new(
        target_id: impl AsRef<str>,
        revision: NonZeroU64,
        content_sha256: [u8; 32],
    ) -> Result<Self, OrderTargetReferenceError> {
        let target_id = target_id.as_ref();
        validate_target_id(target_id)?;
        if content_sha256 == [0; 32] {
            return Err(OrderTargetReferenceError::ZeroContentDigest);
        }
        Ok(Self {
            target_id: target_id.into(),
            revision,
            content_sha256,
        })
    }

    /// Revalidates a deserialized or recovered reference without allocation.
    pub fn validate(&self) -> Result<(), OrderTargetReferenceError> {
        validate_target_id(&self.target_id)?;
        if self.content_sha256 == [0; 32] {
            return Err(OrderTargetReferenceError::ZeroContentDigest);
        }
        Ok(())
    }

    /// Returns the stable bounded target series identity.
    pub fn target_id(&self) -> &str {
        &self.target_id
    }

    /// Returns the nonzero immutable target revision.
    pub const fn revision(&self) -> NonZeroU64 {
        self.revision
    }

    /// Returns the exact nonzero SHA-256 of target content.
    pub const fn content_sha256(&self) -> [u8; 32] {
        self.content_sha256
    }

    /// Returns the fixed-size canonical reference identity used by hot-path audit evidence.
    pub fn audit_digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/order-target-reference/v1\0");
        update_text(&mut digest, &self.target_id);
        digest.update(self.revision.get().to_be_bytes());
        digest.update(self.content_sha256);
        digest.finalize().into()
    }
}

/// Invalid target provenance supplied to an order intent.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OrderTargetReferenceError {
    /// The identity was empty, oversized, or not canonical lowercase ASCII.
    #[error("order target identity is invalid")]
    InvalidTargetId,
    /// The all-zero SHA-256 sentinel cannot identify target content.
    #[error("order target content digest must be nonzero")]
    ZeroContentDigest,
}

/// Untrusted input for constructing one validated [`OrderIntent`].
///
/// Public fields make CLI and strategy assembly straightforward; they convey no execution
/// authority. The validated intent has private fields and cannot reach an adapter without the
/// later risk-approval and one-use dispatch boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderIntentInput {
    /// Stable internal order identity.
    pub order_id: OrderId,
    /// Caller-selected idempotency identity.
    pub client_order_id: ClientOrderId,
    /// Strategy that produced this order.
    pub strategy_id: StrategyId,
    /// Optional model that contributed to the decision.
    pub model_id: Option<ModelId>,
    /// Account against which risk must reserve.
    pub account_id: AccountId,
    /// Immutable instrument revision and exact financial terms.
    pub execution_terms: InstrumentExecutionTerms,
    /// Buy or sell direction.
    pub side: OrderSide,
    /// Closed order kind.
    pub order_type: OrderType,
    /// Strictly positive quantity in instrument lots.
    pub quantity: QuantityLots,
    /// Limit price when required by the order kind.
    pub limit_price: Option<PriceTicks>,
    /// Stop trigger when required by the order kind.
    pub stop_price: Option<PriceTicks>,
    /// Requested time-in-force policy.
    pub time_in_force: TimeInForce,
    /// Trusted event-derived signal time.
    pub signal_at: Timestamp,
    /// Inclusive intent expiration time.
    pub expires_at: Timestamp,
    /// Nonempty bounded machine-readable rationale.
    pub reason_codes: Vec<OrderReasonCode>,
    /// Maximum adverse deviation from the current reference price.
    pub maximum_slippage: BasisPoints,
    /// Minimum quality requested by the strategy.
    pub required_quality: DataQuality,
}

/// Versioned SHA-256 digest over every canonical order-intent field.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OrderIntentDigest([u8; 32]);

impl OrderIntentDigest {
    /// Canonical digest format version.
    pub const VERSION: u8 = 2;

    /// Returns the fixed SHA-256 bytes.
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Restores exact canonical digest bytes from an authoritative persisted tombstone.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// Immutable, validated strategy request that still carries no execution authority.
#[derive(Debug, Eq, PartialEq)]
pub struct OrderIntent {
    order_id: OrderId,
    client_order_id: ClientOrderId,
    strategy_id: StrategyId,
    model_id: Option<ModelId>,
    target_reference: Option<OrderTargetReference>,
    account_id: AccountId,
    execution_terms: InstrumentExecutionTerms,
    side: OrderSide,
    order_type: OrderType,
    quantity: QuantityLots,
    limit_price: Option<PriceTicks>,
    stop_price: Option<PriceTicks>,
    time_in_force: TimeInForce,
    signal_at: Timestamp,
    expires_at: Timestamp,
    reason_codes: Box<[OrderReasonCode]>,
    maximum_slippage: BasisPoints,
    required_quality: DataQuality,
    digest: OrderIntentDigest,
}

impl OrderIntent {
    /// Validates all cross-field invariants and computes the stable input digest.
    ///
    /// # Errors
    ///
    /// Rejects inconsistent price fields, unsupported time-in-force combinations, zero quantity,
    /// invalid chronology, unbounded rationale, negative or excessive slippage, and any requested
    /// execution quality other than [`DataQuality::DirectVerified`].
    pub fn try_new(input: OrderIntentInput) -> Result<Self, OrderIntentError> {
        Self::try_new_inner(input, None)
    }

    /// Validates one target-bound intent while retaining the ordinary risk/dispatch path.
    pub fn try_new_with_target_reference(
        input: OrderIntentInput,
        target_reference: OrderTargetReference,
    ) -> Result<Self, OrderIntentError> {
        target_reference
            .validate()
            .map_err(|_| OrderIntentError::InvalidTargetReference)?;
        Self::try_new_inner(input, Some(target_reference))
    }

    fn try_new_inner(
        input: OrderIntentInput,
        target_reference: Option<OrderTargetReference>,
    ) -> Result<Self, OrderIntentError> {
        validate_order_prices(input.order_type, input.limit_price, input.stop_price)?;
        validate_time_in_force(input.order_type, input.time_in_force)?;
        if input.quantity.get() == 0 {
            return Err(OrderIntentError::ZeroQuantity);
        }
        if input.expires_at <= input.signal_at {
            return Err(OrderIntentError::InvalidChronology);
        }
        if input.reason_codes.is_empty() {
            return Err(OrderIntentError::MissingReasonCode);
        }
        if input.reason_codes.len() > MAX_ORDER_REASON_CODES {
            return Err(OrderIntentError::TooManyReasonCodes {
                max: MAX_ORDER_REASON_CODES,
            });
        }
        if input.maximum_slippage.get() < 0 {
            return Err(OrderIntentError::NegativeMaximumSlippage);
        }
        if input.maximum_slippage.get() > MAX_INTENT_SLIPPAGE_BASIS_POINTS {
            return Err(OrderIntentError::ExcessiveMaximumSlippage {
                max: MAX_INTENT_SLIPPAGE_BASIS_POINTS,
            });
        }
        if input.required_quality != DataQuality::DirectVerified {
            return Err(OrderIntentError::IneligibleRequiredQuality);
        }

        let digest = digest_intent(&input, &input.reason_codes, target_reference.as_ref());
        let reason_codes = input.reason_codes.into_boxed_slice();
        Ok(Self {
            order_id: input.order_id,
            client_order_id: input.client_order_id,
            strategy_id: input.strategy_id,
            model_id: input.model_id,
            target_reference,
            account_id: input.account_id,
            execution_terms: input.execution_terms,
            side: input.side,
            order_type: input.order_type,
            quantity: input.quantity,
            limit_price: input.limit_price,
            stop_price: input.stop_price,
            time_in_force: input.time_in_force,
            signal_at: input.signal_at,
            expires_at: input.expires_at,
            reason_codes,
            maximum_slippage: input.maximum_slippage,
            required_quality: input.required_quality,
            digest,
        })
    }

    /// Returns the stable internal order identity.
    pub const fn order_id(&self) -> OrderId {
        self.order_id
    }

    /// Returns the caller-selected idempotency identity.
    pub const fn client_order_id(&self) -> &ClientOrderId {
        &self.client_order_id
    }

    /// Returns the producing strategy identity.
    pub const fn strategy_id(&self) -> StrategyId {
        self.strategy_id
    }

    /// Returns the contributing model identity, if supplied.
    pub const fn model_id(&self) -> Option<ModelId> {
        self.model_id
    }

    /// Returns exact target/decision provenance when the order was target-derived.
    pub const fn target_reference(&self) -> Option<&OrderTargetReference> {
        self.target_reference.as_ref()
    }

    /// Returns the authoritative account target.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns immutable revision-bound execution terms.
    pub const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.execution_terms
    }

    /// Returns the order side.
    pub const fn side(&self) -> OrderSide {
        self.side
    }

    /// Returns the closed order type.
    pub const fn order_type(&self) -> OrderType {
        self.order_type
    }

    /// Returns the positive order quantity in instrument lots.
    pub const fn quantity(&self) -> QuantityLots {
        self.quantity
    }

    /// Returns the optional limit price in instrument ticks.
    pub const fn limit_price(&self) -> Option<PriceTicks> {
        self.limit_price
    }

    /// Returns the optional stop trigger in instrument ticks.
    pub const fn stop_price(&self) -> Option<PriceTicks> {
        self.stop_price
    }

    /// Returns the requested time-in-force policy.
    pub const fn time_in_force(&self) -> TimeInForce {
        self.time_in_force
    }

    /// Returns the signal timestamp.
    pub const fn signal_at(&self) -> Timestamp {
        self.signal_at
    }

    /// Returns the inclusive intent expiration timestamp.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the bounded strategy rationale.
    pub const fn reason_codes(&self) -> &[OrderReasonCode] {
        &self.reason_codes
    }

    /// Returns the maximum adverse slippage.
    pub const fn maximum_slippage(&self) -> BasisPoints {
        self.maximum_slippage
    }

    /// Returns the required quality, always direct and verified for a valid automated intent.
    pub const fn required_quality(&self) -> DataQuality {
        self.required_quality
    }

    /// Returns the versioned canonical digest.
    pub const fn digest(&self) -> OrderIntentDigest {
        self.digest
    }
}

/// Order-intent invariant failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OrderIntentError {
    /// A limit-bearing order omitted its limit.
    #[error("order type requires a limit price")]
    MissingLimitPrice,
    /// A non-limit order supplied a limit.
    #[error("order type does not permit a limit price")]
    UnexpectedLimitPrice,
    /// A stop-bearing order omitted its trigger.
    #[error("order type requires a stop price")]
    MissingStopPrice,
    /// A non-stop order supplied a trigger.
    #[error("order type does not permit a stop price")]
    UnexpectedStopPrice,
    /// The selected order type and time in force are not a portable supported combination.
    #[error("order type does not support the selected time in force")]
    UnsupportedTimeInForce,
    /// Orders must contain at least one lot.
    #[error("order quantity must be positive")]
    ZeroQuantity,
    /// Expiration must be strictly later than signal time.
    #[error("order expiration must be later than signal time")]
    InvalidChronology,
    /// At least one reason code is mandatory.
    #[error("order intent requires at least one reason code")]
    MissingReasonCode,
    /// The reason-code collection exceeded its fixed bound.
    #[error("order intent exceeds the maximum of {max} reason codes")]
    TooManyReasonCodes {
        /// Maximum accepted count.
        max: usize,
    },
    /// Slippage cannot be negative.
    #[error("maximum slippage must not be negative")]
    NegativeMaximumSlippage,
    /// Slippage exceeded the closed global intent ceiling.
    #[error("maximum slippage exceeds {max} basis points")]
    ExcessiveMaximumSlippage {
        /// Maximum accepted basis points.
        max: i32,
    },
    /// Immediate automated intents must require direct verified data.
    #[error("automated order intent must require DirectVerified data")]
    IneligibleRequiredQuality,
    /// Target provenance did not satisfy its bounded identity and digest invariants.
    #[error("order intent target reference is invalid")]
    InvalidTargetReference,
}

fn validate_order_prices(
    order_type: OrderType,
    limit_price: Option<PriceTicks>,
    stop_price: Option<PriceTicks>,
) -> Result<(), OrderIntentError> {
    let requires_limit = matches!(order_type, OrderType::Limit | OrderType::StopLimit);
    let requires_stop = matches!(order_type, OrderType::Stop | OrderType::StopLimit);
    match (requires_limit, limit_price.is_some()) {
        (true, false) => return Err(OrderIntentError::MissingLimitPrice),
        (false, true) => return Err(OrderIntentError::UnexpectedLimitPrice),
        _ => {}
    }
    match (requires_stop, stop_price.is_some()) {
        (true, false) => Err(OrderIntentError::MissingStopPrice),
        (false, true) => Err(OrderIntentError::UnexpectedStopPrice),
        _ => Ok(()),
    }
}

fn validate_time_in_force(
    order_type: OrderType,
    time_in_force: TimeInForce,
) -> Result<(), OrderIntentError> {
    let supported = match order_type {
        OrderType::Market => !matches!(time_in_force, TimeInForce::GoodTilCancelled),
        OrderType::Limit => true,
        OrderType::Stop | OrderType::StopLimit => {
            matches!(
                time_in_force,
                TimeInForce::Day | TimeInForce::GoodTilCancelled
            )
        }
    };
    if supported {
        Ok(())
    } else {
        Err(OrderIntentError::UnsupportedTimeInForce)
    }
}

fn digest_intent(
    input: &OrderIntentInput,
    reason_codes: &[OrderReasonCode],
    target_reference: Option<&OrderTargetReference>,
) -> OrderIntentDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/order-intent\0");
    digest.update([OrderIntentDigest::VERSION]);
    digest.update(input.order_id.as_uuid().as_bytes());
    update_text(&mut digest, input.client_order_id.as_str());
    digest.update(input.strategy_id.as_uuid().as_bytes());
    match input.model_id {
        Some(model_id) => {
            digest.update([1]);
            digest.update(model_id.as_uuid().as_bytes());
        }
        None => digest.update([0]),
    }
    match target_reference {
        Some(target_reference) => {
            digest.update([1]);
            update_text(&mut digest, target_reference.target_id());
            digest.update(target_reference.revision().get().to_be_bytes());
            digest.update(target_reference.content_sha256());
        }
        None => digest.update([0]),
    }
    digest.update(input.account_id.as_uuid().as_bytes());
    update_execution_terms(&mut digest, input.execution_terms);
    digest.update([order_side_tag(input.side), order_type_tag(input.order_type)]);
    digest.update(input.quantity.get().to_be_bytes());
    update_optional_price(&mut digest, input.limit_price);
    update_optional_price(&mut digest, input.stop_price);
    digest.update([time_in_force_tag(input.time_in_force)]);
    digest.update(input.signal_at.unix_nanos().to_be_bytes());
    digest.update(input.expires_at.unix_nanos().to_be_bytes());
    digest.update((reason_codes.len() as u32).to_be_bytes());
    for reason in reason_codes {
        update_text(&mut digest, reason.as_str());
    }
    digest.update(input.maximum_slippage.get().to_be_bytes());
    digest.update([data_quality_tag(input.required_quality)]);
    OrderIntentDigest(digest.finalize().into())
}

fn update_execution_terms(digest: &mut Sha256, terms: InstrumentExecutionTerms) {
    digest.update(terms.instrument_id().as_uuid().as_bytes());
    digest.update(terms.definition_revision().get().to_be_bytes());
    update_decimal(digest, terms.price_tick().as_decimal());
    update_decimal(digest, terms.lot_size().as_decimal());
    digest.update(terms.quote_currency().as_str().as_bytes());
    match terms.settlement_denomination() {
        Denomination::Currency(currency) => {
            digest.update([0]);
            digest.update(currency.as_str().as_bytes());
        }
        Denomination::Asset(instrument_id) => {
            digest.update([1]);
            digest.update(instrument_id.as_uuid().as_bytes());
        }
    }
    update_decimal(digest, terms.contract_multiplier());
}

fn update_decimal(digest: &mut Sha256, value: rust_decimal::Decimal) {
    let normalized = value.normalize();
    digest.update(normalized.mantissa().to_be_bytes());
    digest.update(normalized.scale().to_be_bytes());
}

fn update_optional_price(digest: &mut Sha256, price: Option<PriceTicks>) {
    match price {
        Some(price) => {
            digest.update([1]);
            digest.update(price.get().to_be_bytes());
        }
        None => digest.update([0]),
    }
}

fn update_text(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u32).to_be_bytes());
    digest.update(value.as_bytes());
}

fn validate_target_id(value: &str) -> Result<(), OrderTargetReferenceError> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || bytes.len() > MAX_ORDER_TARGET_ID_BYTES
        || !bytes[0].is_ascii_lowercase()
        || !bytes.iter().skip(1).all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        Err(OrderTargetReferenceError::InvalidTargetId)
    } else {
        Ok(())
    }
}

const fn order_side_tag(side: OrderSide) -> u8 {
    match side {
        OrderSide::Buy => 0,
        OrderSide::Sell => 1,
    }
}

const fn order_type_tag(order_type: OrderType) -> u8 {
    match order_type {
        OrderType::Market => 0,
        OrderType::Limit => 1,
        OrderType::Stop => 2,
        OrderType::StopLimit => 3,
    }
}

const fn time_in_force_tag(time_in_force: TimeInForce) -> u8 {
    match time_in_force {
        TimeInForce::Day => 0,
        TimeInForce::GoodTilCancelled => 1,
        TimeInForce::ImmediateOrCancel => 2,
        TimeInForce::FillOrKill => 3,
    }
}

const fn data_quality_tag(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 0,
        DataQuality::DirectUnverified => 1,
        DataQuality::OfficialDelayed => 2,
        DataQuality::Aggregated => 3,
        DataQuality::Indicative => 4,
        DataQuality::Modeled => 5,
        DataQuality::Estimated => 6,
        DataQuality::Stale => 7,
        DataQuality::Quarantined => 8,
    }
}
