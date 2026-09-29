//! Adapts current source-owned portfolio observations to the existing pure sizing kernel.
//! Only explicit source settlement cash and original aligned scenario capacity grant those facts.

mod forward_cost;

use market_squawk_decisions::{
    CandidatePortfolioSizingState, CandidateSizingConstraints, CapacityRange,
    DecisionContentDigest, InvestmentSizingInputs, LotRange, SizingCapacityAvailability,
    SizingCapacityEvidence,
};
use market_squawk_domain::{Money, QuantityError, QuantityLots, Timestamp};
use market_squawk_services::{RequestContext, ServiceError};

use crate::{
    application::{market_selection::MarketInvestmentReadReceipt, analytical_profile::ValidatedAnalyticalProfile},
    portfolio_application::{
        PortfolioAnalysisCurrentPosition, PortfolioAnalysisDepthAvailability,
        PortfolioAnalysisMarketAvailability, PortfolioAnalysisPrerequisiteResolution,
    },
};

/// Retains only exact lots. Genuine fractional or short imported holdings remain unsized.
/// The caller rechecks the source read immediately before this synchronous adaptation.
pub(super) fn current_sizing_inputs(
    resolution: &PortfolioAnalysisPrerequisiteResolution,
    market: &MarketInvestmentReadReceipt,
    evaluated_at: Timestamp,
    financial_profile: &ValidatedAnalyticalProfile,
    context: &RequestContext,
) -> Result<Option<InvestmentSizingInputs>, ServiceError> {
    let PortfolioAnalysisPrerequisiteResolution::Evaluated(value) = resolution else {
        return Ok(None);
    };
    let instrument = market.reference().instrument_id();
    let Some(terms) = market.execution_terms() else {
        // A genuine analytical mark does not establish lot, tick or contract sizing terms.
        return Ok(None);
    };
    let observed = market
        .observation()
        .map_err(|_| ServiceError::Unavailable)?;
    let mark = Money::new(observed.mark().value(), observed.mark().currency());
    let entry = value
        .markets()
        .entry(instrument)
        .ok_or(ServiceError::InvalidResult)?;
    let PortfolioAnalysisMarketAvailability::Available {
        market: selected,
        liquidity,
    } = entry.availability()
    else {
        return Err(ServiceError::InvalidResult);
    };
    let portfolio = value.portfolio();
    let marked = value.marked_portfolio();
    let profile = portfolio.setup().setup().profile();
    if selected.execution_terms() != terms
        || selected.selection().receipt_digest() != observed.selection_digest()
        || selected.observation().unit_mark() != mark
        || selected.observation().observed_at() != observed.timestamps().effective_at()
        || selected.observation().available_at() != observed.timestamps().available_at()
        || marked.candidate_instrument_id() != instrument
        || marked.portfolio_revision() != portfolio.revision()
        || marked.market_set_digest() != value.markets().digest()
        || portfolio.account_id() != profile.account_id()
        || portfolio.reporting_currency() != mark.currency()
        || value.calculated_at() > evaluated_at
    {
        return Err(ServiceError::InvalidResult);
    }
    let expires_at = marked
        .holdings()
        .iter()
        .map(|holding| holding.fresh_until())
        .fold(selected.observation().fresh_until(), Timestamp::min)
        .min(market.authorization_expires_at())
        .min(profile.review_due_at());
    if evaluated_at >= expires_at {
        return Err(ServiceError::Unavailable);
    }
    let current_lots = match marked.current_position() {
        PortfolioAnalysisCurrentPosition::NoPosition => QuantityLots::new(0).map_err(invalid)?,
        PortfolioAnalysisCurrentPosition::Position { quantity, .. } => {
            match QuantityLots::try_from_decimal(quantity, terms.lot_size()) {
                Ok(value) => value,
                Err(QuantityError::NegativeQuantity | QuantityError::InexactLot) => {
                    return Ok(None);
                }
                Err(QuantityError::Overflow) => return Err(ServiceError::ResourceExhausted),
            }
        }
    };
    let state = match portfolio.settlement_available_cash() {
        Some(cash) => CandidatePortfolioSizingState::try_new(
            portfolio.account_id(), instrument, portfolio.revision().clone(),
            marked.marked_equity(), cash, current_lots,
        ),
        None => CandidatePortfolioSizingState::try_without_settlement_cash(
            portfolio.account_id(), instrument, portfolio.revision().clone(),
            marked.marked_equity(), current_lots,
        ),
    }.map_err(invalid)?;
    let constraints = CandidateSizingConstraints::try_new(
        profile.minimum_cash_reserve(),
        profile.preferred_position_weight_lower_bps(),
        profile.preferred_position_weight_upper_bps(),
        profile.maximum_downside_loss_bps_of_marked_equity(),
    )
    .map_err(invalid)?;
    // Convert complete source depth quantity directly to exact lots. Depth notionals and
    // policy-relative ppm are never treated as quantity. Both sides are required for a complete
    // target-position interval, accounting for the existing holding before any hypothetical move.
    let liquidity_capacity = match (liquidity.bid(), liquidity.ask()) {
        (
            PortfolioAnalysisDepthAvailability::Available(bid),
            PortfolioAnalysisDepthAvailability::Available(ask),
        ) => {
            let bid_lots = QuantityLots::try_from_decimal(bid.total_quantity(), terms.lot_size())
                .map_err(invalid)?;
            let ask_lots = QuantityLots::try_from_decimal(ask.total_quantity(), terms.lot_size())
                .map_err(invalid)?;
            let lower = QuantityLots::new(current_lots.get().saturating_sub(bid_lots.get()).max(0))
                .map_err(invalid)?;
            let upper = current_lots.checked_add(ask_lots).map_err(invalid)?;
            let range = LotRange::try_new(lower, upper).map_err(invalid)?;
            let available_at = liquidity
                .available_at()
                .ok_or(ServiceError::InvalidResult)?
                .max(value.calculated_at());
            let expires = liquidity
                .fresh_until()
                .ok_or(ServiceError::InvalidResult)?
                .min(expires_at);
            if available_at >= expires || evaluated_at >= expires {
                return Err(ServiceError::Unavailable);
            }
            SizingCapacityAvailability::Available(Box::new(
                SizingCapacityEvidence::try_new(
                    instrument,
                    portfolio.account_id(),
                    portfolio.revision().clone(),
                    terms.definition_revision(),
                    mark,
                    CapacityRange::Lots(range),
                    DecisionContentDigest::try_new(liquidity.evidence_digest()).map_err(invalid)?,
                    liquidity.observed_at().ok_or(ServiceError::InvalidResult)?,
                    available_at,
                    expires,
                )
                .map_err(invalid)?,
            ))
        }
        _ => SizingCapacityAvailability::UnavailableNotSupplied,
    };
    let risk_capacity = match (value.risk_sizing_range(), value.risk_sizing_digest()) {
        (Some(range), Some(digest)) => SizingCapacityAvailability::Available(Box::new(
            SizingCapacityEvidence::try_new(
                instrument, portfolio.account_id(), portfolio.revision().clone(),
                terms.definition_revision(), mark, range,
                DecisionContentDigest::try_new(digest).map_err(invalid)?,
                selected.observation().observed_at(), value.calculated_at(), expires_at,
            ).map_err(invalid)?,
        )),
        (None, None) => SizingCapacityAvailability::UnavailableNotSupplied,
        _ => return Err(ServiceError::InvalidResult),
    };
    let forward_cost_capacity = forward_cost::capacity(
        value, selected, liquidity, current_lots, financial_profile,
        evaluated_at, expires_at, context,
    )?;
    Ok(Some(InvestmentSizingInputs::new(
        evaluated_at,
        terms,
        mark,
        state,
        constraints,
        liquidity_capacity,
        risk_capacity,
        forward_cost_capacity,
    )))
}

fn invalid<T>(_: T) -> ServiceError {
    ServiceError::InvalidResult
}
