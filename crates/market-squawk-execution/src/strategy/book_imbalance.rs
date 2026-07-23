use std::{cmp::Ordering, mem::size_of, num::NonZeroU32};

use market_squawk_analytics::{
    ExactFeatureRatio, FeatureKey, FeatureScalar, LiveFeatureView, RequiredLiveFeature,
};
use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, DataQuality, InstrumentExecutionTerms, MarketEvent,
    OrderId, OrderReasonCode, OrderSide, OrderType, PriceTicks, QuantityLots, StrategyId,
    TimeInForce, Timestamp,
};
use market_squawk_live::ShardKey;
use thiserror::Error;

use crate::{OrderIntent, OrderIntentInput};

use super::{BoundedOrderIntents, Strategy, StrategyContext, StrategyError};

/// The built-in paper signal has 250 milliseconds to clear central risk and dispatch.
pub const PAPER_BOOK_IMBALANCE_INTENT_LIFETIME_NANOS: i64 = 250_000_000;

/// The built-in paper signal accepts at most one percent adverse slippage.
pub const PAPER_BOOK_IMBALANCE_MAXIMUM_SLIPPAGE_BASIS_POINTS: i32 = 100;

/// The built-in paper signal emits exactly one instrument lot.
pub const PAPER_BOOK_IMBALANCE_ORDER_QUANTITY_LOTS: i64 = 1;

/// Untrusted identities and exact guards for one route-owned built-in paper strategy.
#[derive(Debug)]
pub struct BookImbalancePaperStrategyConfigInput {
    /// Exact live route that owns this strategy instance.
    pub route: ShardKey,
    /// Paper account evaluated later by central portfolio and account risk.
    pub account_id: AccountId,
    /// Stable identity for the single order this run may produce.
    pub order_id: OrderId,
    /// Idempotency identity for the single order this run may produce.
    pub client_order_id: ClientOrderId,
    /// Stable identity of this transparent built-in strategy.
    pub strategy_id: StrategyId,
    /// Stable machine-readable rationale retained with the order.
    pub reason_code: OrderReasonCode,
    /// Largest ready top-of-book spread permitted for a signal.
    pub maximum_spread: PriceTicks,
    /// Positive exact bid-side top-of-book imbalance required for a signal.
    pub minimum_book_imbalance: ExactFeatureRatio,
}

/// Validated immutable configuration for one transparent route-owned paper strategy.
#[derive(Debug)]
pub struct BookImbalancePaperStrategyConfig {
    route: ShardKey,
    account_id: AccountId,
    order_id: OrderId,
    client_order_id: ClientOrderId,
    strategy_id: StrategyId,
    reason_code: OrderReasonCode,
    maximum_spread: PriceTicks,
    minimum_book_imbalance: ExactFeatureRatio,
    spread_key: FeatureKey,
    imbalance_key: FeatureKey,
    dynamic_retained_bytes: usize,
}

impl BookImbalancePaperStrategyConfig {
    /// Validates positive bounded signal guards and freezes the built-in feature identities.
    ///
    /// # Errors
    ///
    /// Rejects a nonpositive spread guard, an imbalance threshold outside `(0, 1]`, an impossible
    /// code-owned feature identity, or unrepresentable retained-byte accounting.
    pub fn try_new(
        input: BookImbalancePaperStrategyConfigInput,
    ) -> Result<Self, BookImbalancePaperStrategyConfigError> {
        if input.maximum_spread.get() <= 0 {
            return Err(BookImbalancePaperStrategyConfigError::NonPositiveMaximumSpread);
        }
        let minimum_numerator = u128::try_from(input.minimum_book_imbalance.numerator())
            .map_err(|_| BookImbalancePaperStrategyConfigError::NonPositiveBookImbalance)?;
        if minimum_numerator == 0 {
            return Err(BookImbalancePaperStrategyConfigError::NonPositiveBookImbalance);
        }
        if minimum_numerator > input.minimum_book_imbalance.denominator().get() {
            return Err(BookImbalancePaperStrategyConfigError::BookImbalanceAboveOne);
        }
        let spread_key =
            FeatureKey::try_new(RequiredLiveFeature::Spread.name(), NonZeroU32::MIN)
                .map_err(|_| BookImbalancePaperStrategyConfigError::BuiltInFeatureIdentity)?;
        let imbalance_key =
            FeatureKey::try_new(RequiredLiveFeature::BookImbalance.name(), NonZeroU32::MIN)
                .map_err(|_| BookImbalancePaperStrategyConfigError::BuiltInFeatureIdentity)?;
        let dynamic_retained_bytes = input
            .route
            .venue()
            .retained_bytes()
            .checked_add(input.client_order_id.retained_bytes())
            .and_then(|value| value.checked_add(input.reason_code.retained_bytes()))
            .and_then(|value| value.checked_add(spread_key.name().len()))
            .and_then(|value| value.checked_add(imbalance_key.name().len()))
            .ok_or(BookImbalancePaperStrategyConfigError::RetainedSize)?;
        Ok(Self {
            route: input.route,
            account_id: input.account_id,
            order_id: input.order_id,
            client_order_id: input.client_order_id,
            strategy_id: input.strategy_id,
            reason_code: input.reason_code,
            maximum_spread: input.maximum_spread,
            minimum_book_imbalance: input.minimum_book_imbalance,
            spread_key,
            imbalance_key,
            dynamic_retained_bytes,
        })
    }

    /// Returns the exact route that must own this strategy.
    pub const fn route(&self) -> &ShardKey {
        &self.route
    }

    /// Returns the paper account evaluated later by central risk.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the identity of the sole order this strategy may produce.
    pub const fn order_id(&self) -> OrderId {
        self.order_id
    }

    /// Returns the stable built-in strategy identity.
    pub const fn strategy_id(&self) -> StrategyId {
        self.strategy_id
    }

    /// Returns the exact complete retained footprint of this immutable configuration.
    pub fn retained_bytes(&self) -> Result<usize, BookImbalancePaperStrategyConfigError> {
        size_of::<Self>()
            .checked_add(self.dynamic_retained_bytes)
            .ok_or(BookImbalancePaperStrategyConfigError::RetainedSize)
    }
}

/// Configuration failure for the transparent built-in paper strategy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BookImbalancePaperStrategyConfigError {
    /// The maximum accepted spread must be a positive tick count.
    #[error("paper strategy maximum spread must be positive")]
    NonPositiveMaximumSpread,
    /// The required buy-side imbalance must be strictly positive.
    #[error("paper strategy book-imbalance threshold must be positive")]
    NonPositiveBookImbalance,
    /// Top-of-book imbalance cannot exceed its exact mathematical maximum.
    #[error("paper strategy book-imbalance threshold must not exceed one")]
    BookImbalanceAboveOne,
    /// A code-owned required feature name or version became invalid.
    #[error("paper strategy built-in feature identity is invalid")]
    BuiltInFeatureIdentity,
    /// Exact retained-byte accounting overflowed.
    #[error("paper strategy configuration retained-size accounting failed")]
    RetainedSize,
}

/// Transparent bounded paper strategy driven only by ready spread and book-imbalance features.
///
/// This type owns no broker or risk authority. A qualifying event can create one typed intent;
/// current live authority, portfolio state, central risk, dispatch, and the paper adapter remain
/// mandatory downstream boundaries.
#[derive(Debug)]
pub struct BookImbalancePaperStrategy {
    config: BookImbalancePaperStrategyConfig,
    terminal: bool,
    retained_bytes: usize,
}

impl BookImbalancePaperStrategy {
    /// Creates a one-order strategy from already validated immutable route configuration.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::RetainedSize`] if the exact retained footprint cannot be
    /// represented.
    pub fn try_new(config: BookImbalancePaperStrategyConfig) -> Result<Self, StrategyError> {
        let retained_bytes = size_of::<Self>()
            .checked_add(config.dynamic_retained_bytes)
            .ok_or(StrategyError::RetainedSize)?;
        Ok(Self {
            config,
            terminal: false,
            retained_bytes,
        })
    }

    fn signal_ready(&self, features: &dyn LiveFeatureView) -> Result<bool, StrategyError> {
        let Some(spread) = features
            .feature(&self.config.spread_key)
            .and_then(|value| value.ready_value())
        else {
            return Ok(false);
        };
        let FeatureScalar::PriceTicks(spread) = spread else {
            return Err(StrategyError::Evaluation);
        };
        if spread.get() < 0 {
            return Err(StrategyError::Evaluation);
        }
        if spread > self.config.maximum_spread {
            return Ok(false);
        }

        let Some(imbalance) = features
            .feature(&self.config.imbalance_key)
            .and_then(|value| value.ready_value())
        else {
            return Ok(false);
        };
        let FeatureScalar::ExactRatio(imbalance) = imbalance else {
            return Err(StrategyError::Evaluation);
        };
        Ok(nonnegative_ratio_cmp(imbalance, self.config.minimum_book_imbalance) != Ordering::Less)
    }

    fn try_emit(
        &mut self,
        execution_terms: InstrumentExecutionTerms,
        signal_at: Timestamp,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        if self.terminal {
            return Ok(BoundedOrderIntents::new());
        }
        if execution_terms.instrument_id() != self.config.route.instrument() {
            return Err(StrategyError::Evaluation);
        }
        // A qualifying signal is terminal even if an unexpected downstream representation error
        // prevents construction. Repeated market events can never turn one route into a retry loop.
        self.terminal = true;
        let expires_at = signal_at
            .checked_add_nanos(PAPER_BOOK_IMBALANCE_INTENT_LIFETIME_NANOS)
            .map_err(|_| StrategyError::Evaluation)?;
        let intent = OrderIntent::try_new(OrderIntentInput {
            order_id: self.config.order_id,
            client_order_id: self.config.client_order_id.clone(),
            strategy_id: self.config.strategy_id,
            model_id: None,
            account_id: self.config.account_id,
            execution_terms,
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            quantity: QuantityLots::new(PAPER_BOOK_IMBALANCE_ORDER_QUANTITY_LOTS)
                .map_err(|_| StrategyError::Evaluation)?,
            limit_price: None,
            stop_price: None,
            time_in_force: TimeInForce::ImmediateOrCancel,
            signal_at,
            expires_at,
            reason_codes: vec![self.config.reason_code.clone()],
            maximum_slippage: BasisPoints::new(PAPER_BOOK_IMBALANCE_MAXIMUM_SLIPPAGE_BASIS_POINTS),
            required_quality: DataQuality::DirectVerified,
        })
        .map_err(|_| StrategyError::Evaluation)?;
        let mut output = BoundedOrderIntents::new();
        output.try_push(intent)?;
        Ok(output)
    }
}

impl Strategy for BookImbalancePaperStrategy {
    fn on_market_event(
        &mut self,
        context: &StrategyContext<'_>,
        event: &MarketEvent,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        if self.terminal {
            return Ok(BoundedOrderIntents::new());
        }
        if context.route() != self.config.route() {
            return Err(StrategyError::Evaluation);
        }
        if !matches!(
            event,
            MarketEvent::BookSnapshot(_) | MarketEvent::BookDelta(_)
        ) || !self.signal_ready(context.features())?
        {
            return Ok(BoundedOrderIntents::new());
        }
        self.try_emit(
            context.market().execution_terms(),
            context.market().observed_at(),
        )
    }

    fn retained_bytes(&self) -> Result<usize, StrategyError> {
        Ok(self.retained_bytes)
    }
}

fn nonnegative_ratio_cmp(left: ExactFeatureRatio, right: ExactFeatureRatio) -> Ordering {
    let Ok(mut left_numerator) = u128::try_from(left.numerator()) else {
        return Ordering::Less;
    };
    let Ok(mut right_numerator) = u128::try_from(right.numerator()) else {
        return Ordering::Greater;
    };
    let mut left_denominator = left.denominator().get();
    let mut right_denominator = right.denominator().get();
    let mut reversed = false;

    loop {
        let whole_ordering =
            (left_numerator / left_denominator).cmp(&(right_numerator / right_denominator));
        if whole_ordering != Ordering::Equal {
            return maybe_reverse(whole_ordering, reversed);
        }

        let left_remainder = left_numerator % left_denominator;
        let right_remainder = right_numerator % right_denominator;
        match (left_remainder == 0, right_remainder == 0) {
            (true, true) => return Ordering::Equal,
            (true, false) => return maybe_reverse(Ordering::Less, reversed),
            (false, true) => return maybe_reverse(Ordering::Greater, reversed),
            (false, false) => {
                left_numerator = left_denominator;
                left_denominator = left_remainder;
                right_numerator = right_denominator;
                right_denominator = right_remainder;
                reversed = !reversed;
            }
        }
    }
}

const fn maybe_reverse(ordering: Ordering, reversed: bool) -> Ordering {
    if reversed {
        ordering.reverse()
    } else {
        ordering
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, str::FromStr};

    use market_squawk_analytics::{FeatureError, FeatureValue};
    use market_squawk_domain::{
        AccountId, AggressorSide, AuthorizationBasis, BookLevel, BookSnapshotEvent,
        BookStateBinding, CanonicalStateDigest, CanonicalizationRule, ClientOrderId,
        ConnectionGeneration, CoverageStatus, Currency, DataQuality, Denomination, EvidenceDigest,
        InstrumentExecutionTerms, LiveEventClass, LiveEvidenceBinding, LiveProvenance, LotSize,
        MarketDepth, MarketEvent, MetadataRevision, OrderId, OrderReasonCode, OrderSide, OrderType,
        PayloadHashAlgorithm, PayloadReference, PriceTicks, ProviderChannel, ProviderProduct,
        QualificationAssessmentId, QuantityLots, RecordedLiveProvenanceInput, RuleVersion,
        SequenceNumber, SourceId, SourceIdentifier, StrategyId, TickSize, TimeInForce, Timestamp,
        TradeEvent, VenueId,
    };
    use rust_decimal::Decimal;

    use crate::ExecutionMarketReference;

    use super::super::{Strategy, StrategyContext, StrategyError};
    use super::{
        BookImbalancePaperStrategy, BookImbalancePaperStrategyConfig,
        BookImbalancePaperStrategyConfigError, BookImbalancePaperStrategyConfigInput,
        ExactFeatureRatio, FeatureKey, FeatureScalar, LiveFeatureView, RequiredLiveFeature,
    };

    #[derive(Debug)]
    struct PaperSignalFeatureView {
        spread_key: FeatureKey,
        spread: FeatureValue<FeatureScalar>,
        imbalance_key: FeatureKey,
        imbalance: FeatureValue<FeatureScalar>,
    }

    impl PaperSignalFeatureView {
        fn try_ready(
            spread: i64,
            imbalance_numerator: i128,
            imbalance_denominator: u128,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let observed_at = Timestamp::from_unix_nanos(7);
            Ok(Self {
                spread_key: FeatureKey::try_new(
                    RequiredLiveFeature::Spread.name(),
                    NonZeroU32::MIN,
                )?,
                spread: FeatureValue::ready(
                    FeatureScalar::PriceTicks(PriceTicks::new(spread)),
                    observed_at,
                ),
                imbalance_key: FeatureKey::try_new(
                    RequiredLiveFeature::BookImbalance.name(),
                    NonZeroU32::MIN,
                )?,
                imbalance: FeatureValue::ready(
                    FeatureScalar::ExactRatio(ExactFeatureRatio::try_new(
                        imbalance_numerator,
                        imbalance_denominator,
                    )?),
                    observed_at,
                ),
            })
        }
    }

    impl LiveFeatureView for PaperSignalFeatureView {
        fn feature(&self, key: &FeatureKey) -> Option<&FeatureValue<FeatureScalar>> {
            if key == &self.spread_key {
                Some(&self.spread)
            } else if key == &self.imbalance_key {
                Some(&self.imbalance)
            } else {
                None
            }
        }

        fn retained_bytes(&self) -> Result<usize, FeatureError> {
            Ok(std::mem::size_of::<Self>()
                + self.spread_key.name().len()
                + self.imbalance_key.name().len())
        }
    }

    fn paper_strategy_config(
        maximum_spread_ticks: i64,
        imbalance_numerator: i128,
        imbalance_denominator: u128,
    ) -> Result<
        Result<BookImbalancePaperStrategyConfig, BookImbalancePaperStrategyConfigError>,
        Box<dyn std::error::Error>,
    > {
        Ok(BookImbalancePaperStrategyConfig::try_new(
            BookImbalancePaperStrategyConfigInput {
                route: market_squawk_live::ShardKey::new(
                    VenueId::try_from("paper-test")?,
                    "018f0000-0000-7000-8000-000000000001".parse()?,
                ),
                account_id: AccountId::from_str("50000000-0000-0000-0000-000000000001")?,
                order_id: OrderId::from_str("20000000-0000-0000-0000-000000000001")?,
                client_order_id: ClientOrderId::try_from("paper-book-imbalance-1")?,
                strategy_id: StrategyId::from_str("30000000-0000-0000-0000-000000000001")?,
                reason_code: OrderReasonCode::try_from("paper.book-imbalance.buy")?,
                maximum_spread: PriceTicks::new(maximum_spread_ticks),
                minimum_book_imbalance: ExactFeatureRatio::try_new(
                    imbalance_numerator,
                    imbalance_denominator,
                )?,
            },
        ))
    }

    fn paper_execution_terms() -> Result<InstrumentExecutionTerms, Box<dyn std::error::Error>> {
        let currency = Currency::try_from("USD")?;
        Ok(InstrumentExecutionTerms::try_new(
            "018f0000-0000-7000-8000-000000000001".parse()?,
            1_u64.try_into()?,
            TickSize::try_from_decimal(Decimal::new(1, 2))?,
            LotSize::try_from_decimal(Decimal::new(1, 2))?,
            currency,
            Denomination::Currency(currency),
            Decimal::ONE,
        )?)
    }

    fn paper_market_event(
        event_class: LiveEventClass,
    ) -> Result<MarketEvent, Box<dyn std::error::Error>> {
        let provenance = LiveProvenance::recorded(RecordedLiveProvenanceInput::new(
            paper_live_binding(event_class)?,
            Some(Timestamp::from_unix_nanos(900)),
            Timestamp::from_unix_nanos(1_000),
            Timestamp::from_unix_nanos(1_000),
            Timestamp::from_unix_nanos(1_000),
            DataQuality::DirectVerified,
            CoverageStatus::Sufficient,
            PayloadReference::SourceReference(SourceIdentifier::try_from("paper-event")?),
            SourceIdentifier::try_from("paper-assessment")?,
        ))?;
        Ok(match event_class {
            LiveEventClass::BookSnapshot => MarketEvent::BookSnapshot(BookSnapshotEvent::new(
                provenance,
                MarketDepth::PriceLevel,
                vec![BookLevel::new(PriceTicks::new(100), QuantityLots::new(3)?)?],
                vec![BookLevel::new(PriceTicks::new(101), QuantityLots::new(1)?)?],
                Some(SequenceNumber::new(1)),
            )?),
            LiveEventClass::Trade => MarketEvent::Trade(TradeEvent::new(
                provenance,
                PriceTicks::new(100),
                QuantityLots::new(1)?,
                AggressorSide::Buy,
            )?),
            _ => return Err("unsupported paper strategy event fixture".into()),
        })
    }

    fn paper_live_binding(
        event_class: LiveEventClass,
    ) -> Result<LiveEvidenceBinding, Box<dyn std::error::Error>> {
        let canonicalization = || -> Result<CanonicalizationRule, Box<dyn std::error::Error>> {
            Ok(CanonicalizationRule::new(
                SourceIdentifier::try_from("paper-book-state-v1")?,
                RuleVersion::new(1)?,
            ))
        };
        let state_digest = CanonicalStateDigest::new(
            EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [2; 32]),
            canonicalization()?,
        );
        let book_state = if event_class.requires_book_state() {
            Some(BookStateBinding::new_with_snapshot_origin(
                MarketDepth::PriceLevel,
                SourceIdentifier::try_from("paper-book-state")?,
                state_digest.clone(),
                SourceIdentifier::try_from("paper-snapshot-state")?,
                CanonicalStateDigest::new(
                    EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [3; 32]),
                    canonicalization()?,
                ),
            ))
        } else {
            None
        };
        Ok(LiveEvidenceBinding::new(
            SourceId::try_from("paper-direct")?,
            SourceIdentifier::try_from("paper-session")?,
            MetadataRevision::new(SourceIdentifier::try_from("paper-metadata-v1")?),
            AuthorizationBasis::new(SourceIdentifier::try_from("paper-authorization")?),
            VenueId::try_from("paper-test")?,
            "018f0000-0000-7000-8000-000000000001".parse()?,
            ConnectionGeneration::new(1)?,
            ProviderProduct::new(SourceIdentifier::try_from("paper-product")?),
            ProviderChannel::new(SourceIdentifier::try_from("paper-channel")?),
            event_class,
            SourceIdentifier::try_from("paper-event")?,
            EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [1; 32]),
            state_digest,
            book_state,
        )?)
    }

    #[test]
    fn paper_strategy_configuration_requires_positive_bounded_guards()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            paper_strategy_config(0, 1, 5)?,
            Err(BookImbalancePaperStrategyConfigError::NonPositiveMaximumSpread)
        ));
        assert!(matches!(
            paper_strategy_config(5, 0, 1)?,
            Err(BookImbalancePaperStrategyConfigError::NonPositiveBookImbalance)
        ));
        assert!(matches!(
            paper_strategy_config(5, 6, 5)?,
            Err(BookImbalancePaperStrategyConfigError::BookImbalanceAboveOne)
        ));
        Ok(())
    }

    #[test]
    fn paper_strategy_boundary_enforces_route_book_features_and_one_shot_intent_bounds()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = paper_strategy_config(5, 1, 5)??;
        let route = config.route().clone();
        let account_id = config.account_id();
        let order_id = config.order_id();
        let strategy_id = config.strategy_id();
        let mut strategy = BookImbalancePaperStrategy::try_new(config)?;
        let retained = strategy.retained_bytes()?;

        let signal_at = Timestamp::from_unix_nanos(1_000_000_000);
        let terms = paper_execution_terms()?;
        let bids = [BookLevel::new(PriceTicks::new(100), QuantityLots::new(3)?)?];
        let asks = [BookLevel::new(PriceTicks::new(101), QuantityLots::new(1)?)?];
        let market = ExecutionMarketReference::for_strategy_test(terms, signal_at, &bids, &asks);
        let assessment = QualificationAssessmentId::new(SourceIdentifier::try_from(
            "paper-strategy-assessment",
        )?);
        let ready_features = PaperSignalFeatureView::try_ready(5, 1, 3)?;
        let book = paper_market_event(LiveEventClass::BookSnapshot)?;

        let wrong_route = market_squawk_live::ShardKey::new(
            VenueId::try_from("wrong-paper-route")?,
            route.instrument(),
        );
        let wrong_context =
            StrategyContext::from_committed(&wrong_route, &assessment, market, &ready_features);
        assert!(matches!(
            strategy.on_market_event(&wrong_context, &book),
            Err(StrategyError::Evaluation)
        ));

        let context = StrategyContext::from_committed(&route, &assessment, market, &ready_features);
        let trade = paper_market_event(LiveEventClass::Trade)?;
        assert!(strategy.on_market_event(&context, &trade)?.is_empty());

        let below_threshold = PaperSignalFeatureView::try_ready(5, 1, 6)?;
        let below_context =
            StrategyContext::from_committed(&route, &assessment, market, &below_threshold);
        assert!(strategy.on_market_event(&below_context, &book)?.is_empty());

        let output = strategy.on_market_event(&context, &book)?;
        assert_eq!(output.len(), 1);
        let intent = output
            .into_iter()
            .next()
            .ok_or("ready paper signal did not produce its one bounded intent")?;
        assert_eq!(intent.account_id(), account_id);
        assert_eq!(intent.order_id(), order_id);
        assert_eq!(intent.strategy_id(), strategy_id);
        assert_eq!(intent.side(), OrderSide::Buy);
        assert_eq!(intent.order_type(), OrderType::Market);
        assert_eq!(intent.quantity().get(), 1);
        assert_eq!(intent.time_in_force(), TimeInForce::ImmediateOrCancel);
        assert_eq!(intent.signal_at(), signal_at);
        assert_eq!(
            intent.expires_at(),
            signal_at.checked_add_nanos(250_000_000)?
        );
        assert_eq!(intent.maximum_slippage().get(), 100);
        assert_eq!(intent.reason_codes().len(), 1);
        assert_eq!(
            intent.reason_codes()[0].as_str(),
            "paper.book-imbalance.buy"
        );

        assert!(strategy.on_market_event(&context, &book)?.is_empty());
        assert_eq!(strategy.retained_bytes()?, retained);
        Ok(())
    }
}
