//! Conditional current-snapshot trading-cost allowance under the saved research policy.
//! It sizes a change in holdings, not a future round trip, broker fee quote or fill commitment.

use market_squawk_backtesting::ResearchExecutionAssumptions;
use market_squawk_decisions::{
    CapacityRange, DecisionContentDigest, LotRange, SizingCapacityAvailability,
    SizingCapacityEvidence,
};
use market_squawk_domain::{
    Denomination, DigestAlgorithm, EvidenceDigest, InstrumentExecutionTerms, Money, OrderSide,
    PriceTicks, QuantityLots, Timestamp,
};
use market_squawk_services::{RequestContext, ServiceError};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};

use crate::application::analytical_profile::ValidatedAnalyticalProfile;
use crate::portfolio_application::{
    PortfolioAnalysisDepthAvailability, PortfolioAnalysisDepthSideEvidence,
    PortfolioAnalysisLiquidityEvidence, PortfolioCandidateMarketEvidence,
    PortfolioRecommendationEvidence,
};

struct CostLevel {
    lots: i64,
    price: PriceTicks,
    per_lot: Money,
}
struct SideCurve {
    levels: Vec<CostLevel>,
    capacity: i64,
}

#[allow(
    clippy::too_many_arguments,
    reason = "original source, portfolio, policy and clocks retain distinct authorities"
)]
pub(super) fn capacity(
    value: &PortfolioRecommendationEvidence,
    market: &PortfolioCandidateMarketEvidence,
    liquidity: &PortfolioAnalysisLiquidityEvidence,
    current: QuantityLots,
    profile: &ValidatedAnalyticalProfile,
    evaluated_at: Timestamp,
    expires_at: Timestamp,
    context: &RequestContext,
) -> Result<SizingCapacityAvailability, ServiceError> {
    live(context)?;
    let portfolio = value.portfolio();
    let Some(cash) = portfolio.settlement_available_cash() else {
        return Ok(SizingCapacityAvailability::UnavailableNotSupplied);
    };
    let (
        PortfolioAnalysisDepthAvailability::Available(bid),
        PortfolioAnalysisDepthAvailability::Available(ask),
    ) = (liquidity.bid(), liquidity.ask())
    else {
        return Ok(SizingCapacityAvailability::UnavailableNotSupplied);
    };
    let reserve = portfolio.setup().setup().profile().minimum_cash_reserve();
    let terms = market.execution_terms();
    if cash.currency() != reserve.currency() || cash.currency() != terms.quote_currency() {
        return Err(ServiceError::InvalidResult);
    }
    if terms.settlement_denomination() != Denomination::Currency(cash.currency()) {
        return Ok(SizingCapacityAvailability::UnavailableNotSupplied);
    }
    let available = liquidity
        .available_at()
        .ok_or(ServiceError::InvalidResult)?
        .max(value.calculated_at());
    let expiry = expires_at.min(liquidity.fresh_until().ok_or(ServiceError::InvalidResult)?);
    if available > evaluated_at || evaluated_at >= expiry || available >= expiry {
        return Err(ServiceError::Unavailable);
    }
    let policy = profile.execution_assumptions();
    let bid = curve(bid, terms, OrderSide::Sell, policy, context)?;
    let ask = curve(ask, terms, OrderSide::Buy, policy, context)?;
    let Some(range) =
        cash_feasible_range(current, cash, reserve, &bid, &ask, terms, policy, context)?
    else {
        return Ok(SizingCapacityAvailability::UnavailableNotSupplied);
    };
    let mark = market.observation().unit_mark();
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/current-modeled-trading-cost-capacity/v1\0");
    hash.update(value.evidence_digest().bytes());
    hash.update(liquidity.evidence_digest().bytes());
    hash.update(policy.digest().bytes());
    hash.update(profile.resolution().configuration_digest.as_bytes());
    hash.update(market.selection().receipt_digest().bytes());
    hash.update(portfolio.account_id().as_uuid().as_bytes());
    hash.update(portfolio.revision().bytes());
    hash.update(terms.instrument_id().as_uuid().as_bytes());
    hash.update(terms.definition_revision().get().to_be_bytes());
    decimal(&mut hash, terms.price_tick().as_decimal());
    decimal(&mut hash, terms.lot_size().as_decimal());
    decimal(&mut hash, terms.contract_multiplier());
    decimal(&mut hash, mark.amount());
    hash.update(mark.currency().as_str().as_bytes());
    decimal(&mut hash, cash.amount());
    decimal(&mut hash, reserve.amount());
    hash.update(current.get().to_be_bytes());
    hash.update(available.unix_nanos().to_be_bytes());
    hash.update(expiry.unix_nanos().to_be_bytes());
    hash.update(b"conditional_current_snapshot;side_prices_include_spread;max_declared_jitter;single_change;no_latency_fill_promise;no_future_exit;not_broker_fees;no_execution_authority");
    match range {
        CapacityRange::NoFeasibleLots => hash.update([0]),
        CapacityRange::Lots(v) => {
            hash.update([1]);
            hash.update(v.lower().get().to_be_bytes());
            hash.update(v.upper().get().to_be_bytes());
        }
        CapacityRange::Notional(_) => return Err(ServiceError::InvalidResult),
    }
    let identity = DecisionContentDigest::try_new(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
    .map_err(invalid)?;
    live(context)?;
    Ok(SizingCapacityAvailability::Available(Box::new(
        SizingCapacityEvidence::try_new(
            terms.instrument_id(),
            portfolio.account_id(),
            portfolio.revision().clone(),
            terms.definition_revision(),
            mark,
            range,
            identity,
            liquidity.observed_at().ok_or(ServiceError::InvalidResult)?,
            available,
            expiry,
        )
        .map_err(invalid)?,
    )))
}

#[allow(
    clippy::too_many_arguments,
    reason = "cash, reserve and source curves retain distinct numerical inputs"
)]
fn cash_feasible_range(
    current: QuantityLots,
    cash: Money,
    reserve: Money,
    bid: &SideCurve,
    ask: &SideCurve,
    terms: InstrumentExecutionTerms,
    policy: ResearchExecutionAssumptions,
    context: &RequestContext,
) -> Result<Option<CapacityRange>, ServiceError> {
    // Nearest-even order-level fees can otherwise make microscopic sale increments nonmonotone.
    // A whole rounding quantum bounds the difference between two fee-rounding errors. Prove
    // each actual gross lot increment covers that quantum after the declared proportional fee.
    let fee_factor = Decimal::from(10_000 - policy.fee_basis_points().get())
        .checked_div(Decimal::from(10_000))
        .ok_or(ServiceError::InvalidResult)?;
    let quantum = Decimal::new(1, policy.fee_decimal_scale());
    for level in &bid.levels {
        let net = level
            .per_lot
            .amount()
            .checked_mul(fee_factor)
            .ok_or(ServiceError::InvalidResult)?;
        if net < quantum {
            return Ok(None);
        }
    }
    let low = current.get().saturating_sub(bid.capacity).max(0);
    let high = current
        .get()
        .checked_add(ask.capacity)
        .ok_or(ServiceError::ResourceExhausted)?;
    let cash_at =
        |target| cash_after_target(target, current, cash, bid, ask, terms, policy, context);
    // Cash is nonincreasing in the target on this proven source/fee domain. Both buy and sell
    // directions, including a cash deficit requiring a reduction, use the same exact predicate.
    let range = if cash_at(low)?.amount() < reserve.amount() {
        CapacityRange::NoFeasibleLots
    } else {
        let mut left = low;
        let mut right = high;
        while left < right {
            let mid = left + (right - left) / 2 + (right - left) % 2;
            if cash_at(mid)?.amount() >= reserve.amount() {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        CapacityRange::Lots(
            LotRange::try_new(
                QuantityLots::new(low).map_err(invalid)?,
                QuantityLots::new(left).map_err(invalid)?,
            )
            .map_err(invalid)?,
        )
    };
    Ok(Some(range))
}

#[allow(
    clippy::too_many_arguments,
    reason = "target and original cash are evaluated against both source sides"
)]
fn cash_after_target(
    target: i64,
    current: QuantityLots,
    cash: Money,
    bid: &SideCurve,
    ask: &SideCurve,
    terms: InstrumentExecutionTerms,
    policy: ResearchExecutionAssumptions,
    context: &RequestContext,
) -> Result<Money, ServiceError> {
    live(context)?;
    if target == current.get() {
        return Ok(cash);
    } // No trade means no modeled fee or slippage.
    let (side, quantity) = if target < current.get() {
        (bid, current.get() - target)
    } else {
        (ask, target - current.get())
    };
    let gross = notional(side, quantity, cash, terms, policy, context)?;
    let fee = policy.modeled_fee(gross).map_err(invalid)?;
    if target < current.get() {
        cash.checked_add(gross)
            .and_then(|v| v.checked_sub(fee))
            .map_err(invalid)
    } else {
        cash.checked_sub(gross)
            .and_then(|v| v.checked_sub(fee))
            .map_err(invalid)
    }
}

fn curve(
    depth: &PortfolioAnalysisDepthSideEvidence,
    terms: InstrumentExecutionTerms,
    side: OrderSide,
    policy: ResearchExecutionAssumptions,
    context: &RequestContext,
) -> Result<SideCurve, ServiceError> {
    let mut levels = Vec::new();
    levels
        .try_reserve_exact(depth.levels().len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    let one = QuantityLots::new(1).map_err(invalid)?;
    let mut count = 0_i64;
    for level in depth.levels() {
        live(context)?;
        let lots =
            QuantityLots::try_from_decimal(level.quantity(), terms.lot_size()).map_err(invalid)?;
        let price = policy
            .modeled_adverse_side_price(terms, side, level.unit_price())
            .map_err(invalid)?;
        let per_lot = policy
            .modeled_notional(terms, price, one)
            .map_err(invalid)?;
        if lots.get() <= 0 || per_lot.amount() <= Decimal::ZERO {
            return Err(ServiceError::InvalidResult);
        }
        count = count
            .checked_add(lots.get())
            .ok_or(ServiceError::ResourceExhausted)?;
        levels.push(CostLevel {
            lots: lots.get(),
            price,
            per_lot,
        });
    }
    let total = QuantityLots::try_from_decimal(depth.total_quantity(), terms.lot_size())
        .map_err(invalid)?;
    if levels.is_empty() || total.get() != count {
        return Err(ServiceError::InvalidResult);
    }
    Ok(SideCurve {
        levels,
        capacity: policy
            .modeled_participation_capacity(total)
            .map_err(invalid)?
            .get(),
    })
}

fn notional(
    curve: &SideCurve,
    quantity: i64,
    cash: Money,
    terms: InstrumentExecutionTerms,
    policy: ResearchExecutionAssumptions,
    context: &RequestContext,
) -> Result<Money, ServiceError> {
    if quantity < 0 || quantity > curve.capacity {
        return Err(ServiceError::InvalidResult);
    }
    let mut remaining = quantity;
    let mut value = Money::new(Decimal::ZERO, cash.currency());
    for level in &curve.levels {
        live(context)?;
        let take = remaining.min(level.lots);
        let gross = policy
            .modeled_notional(
                terms,
                level.price,
                QuantityLots::new(take).map_err(invalid)?,
            )
            .map_err(invalid)?;
        value = value.checked_add(gross).map_err(invalid)?;
        remaining -= take;
        if remaining == 0 {
            return Ok(value);
        }
    }
    Err(ServiceError::InvalidResult)
}

fn decimal(hash: &mut Sha256, value: Decimal) {
    let text = value.normalize().to_string();
    hash.update((text.len() as u64).to_be_bytes());
    hash.update(text.as_bytes());
}
fn invalid<T>(_: T) -> ServiceError {
    ServiceError::InvalidResult
}
fn live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if std::time::Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_squawk_backtesting::recommendation_conservative_execution_assumptions_v1;
    use market_squawk_domain::{Currency, InstrumentDefinitionRevision, LotSize, TickSize};
    use market_squawk_services::{JsonStructureLimits, RequestId, ServiceLimits};
    use std::time::{Duration, Instant};
    use tokio_util::sync::CancellationToken;

    #[test]
    fn modeled_cost_capacity_preserves_hold_buy_boundary_and_reserve_restoring_sale()
    -> Result<(), Box<dyn std::error::Error>> {
        let usd = Currency::try_from("USD")?;
        let money = |amount| Money::new(amount, usd);
        let terms = InstrumentExecutionTerms::try_new(
            "00000000-0000-0000-0000-000000000020".parse()?,
            InstrumentDefinitionRevision::try_from(1_u64)?,
            TickSize::try_from_decimal(Decimal::new(1, 2))?,
            LotSize::try_from_decimal(Decimal::ONE)?,
            usd,
            Denomination::Currency(usd),
            Decimal::ONE,
        )?;
        let policy = recommendation_conservative_execution_assumptions_v1()?;
        let context = RequestContext::new(
            RequestId::Integer(1),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
            ServiceLimits::try_new(
                1024,
                16,
                1024,
                16,
                JsonStructureLimits::try_new(4, 128, 16, 16)?,
            )?,
        );
        // Numerical fixtures only: no account, source receipt or executable authority is minted.
        // Real policy applies 20bp extra adverse movement to supplied $99 bid / $101 ask,
        // yielding floor($98.802)=$98.80 and ceil($101.202)=$101.21 at cent ticks.
        let side = |direction, price| -> Result<SideCurve, Box<dyn std::error::Error>> {
            let price = policy.modeled_adverse_side_price(terms, direction, money(price))?;
            Ok(SideCurve {
                levels: vec![CostLevel {
                    lots: 200,
                    price,
                    per_lot: policy.modeled_notional(terms, price, QuantityLots::new(1)?)?,
                }],
                capacity: policy
                    .modeled_participation_capacity(QuantityLots::new(200)?)?
                    .get(),
            })
        };
        let bid = side(OrderSide::Sell, Decimal::from(99))?;
        let ask = side(OrderSide::Buy, Decimal::from(101))?;
        assert_eq!(bid.levels[0].price.get(), 9880);
        assert_eq!(ask.levels[0].price.get(), 10121);
        assert_eq!((bid.capacity, ask.capacity), (10, 10));
        let current = QuantityLots::new(2)?;
        let cash = money(Decimal::from(1000));
        assert_eq!(
            cash_after_target(2, current, cash, &bid, &ask, terms, policy, &context)?,
            cash
        );

        // Two added lots: $202.42 gross + $0.20242 fee = $202.62242.
        // Three: $303.63 + $0.30363 = $303.93363. The exact reserve admits target4 only.
        let reserve = money(Decimal::new(79737758, 5));
        assert_eq!(
            cash_after_target(4, current, cash, &bid, &ask, terms, policy, &context)?,
            reserve
        );
        assert_eq!(
            cash_after_target(5, current, cash, &bid, &ask, terms, policy, &context)?,
            money(Decimal::new(69606637, 5))
        );
        assert_eq!(
            cash_feasible_range(current, cash, reserve, &bid, &ask, terms, policy, &context)?,
            Some(CapacityRange::Lots(LotRange::try_new(
                QuantityLots::new(0)?,
                QuantityLots::new(4)?
            )?))
        );

        // With no settled cash, selling one nets $98.80 - $0.0988 = $98.7012.
        // Holding two fails that reserve; reducing to one is an inclusive feasible endpoint.
        let cash = money(Decimal::ZERO);
        let reserve = money(Decimal::new(987012, 4));
        assert_eq!(
            cash_after_target(1, current, cash, &bid, &ask, terms, policy, &context)?,
            reserve
        );
        assert_eq!(
            cash_after_target(2, current, cash, &bid, &ask, terms, policy, &context)?,
            cash
        );
        assert_eq!(
            cash_feasible_range(current, cash, reserve, &bid, &ask, terms, policy, &context)?,
            Some(CapacityRange::Lots(LotRange::try_new(
                QuantityLots::new(0)?,
                QuantityLots::new(1)?
            )?))
        );

        // Liquidating both nets $197.60 - $0.1976 = $197.4024. Even the best target
        // cannot fund a reserve one fee quantum higher; this is evaluated infeasibility.
        assert_eq!(
            cash_after_target(0, current, cash, &bid, &ask, terms, policy, &context)?,
            money(Decimal::new(1974024, 4))
        );
        assert_eq!(
            cash_feasible_range(
                current,
                cash,
                money(Decimal::new(19740240001, 8)),
                &bid,
                &ask,
                terms,
                policy,
                &context
            )?,
            Some(CapacityRange::NoFeasibleLots)
        );
        Ok(())
    }
}
