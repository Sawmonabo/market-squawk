//! Source-bound target lots under the existing current-weight, fractional-tail 95% ES policy.
//!
//! This is hypothetical sizing evidence, never a reservation or execution approval. Source cash
//! bounds the search only; the existing reserve, liquidity and cost constraints still intersect it.

use super::history::ensure_live;
use market_squawk_decisions::{CapacityRange, LotRange};
use market_squawk_domain::{MarketBarAdjustment, QuantityError, QuantityLots};

use super::*;
use crate::ResearchService;
use crate::application::market_calendar::HistoryCurrentSessionQualification;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PortfolioRiskSizingEvidence {
    range: CapacityRange,
    digest: EvidenceDigest,
    qualification: HistoryCurrentSessionQualification,
}

impl PortfolioRiskSizingEvidence {
    pub(super) const fn range(&self) -> CapacityRange {
        self.range
    }
    pub(super) const fn digest(&self) -> EvidenceDigest {
        self.digest
    }

    pub(super) async fn recheck(
        &self,
        authority: Option<&CompletedMarketSessionAuthority>,
        calendars: &CompletedMarketSessionReadCapability,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), PortfolioApplicationServiceError> {
        ensure_live(deadline, cancellation)?;
        if let Some(reference) = self.qualification.nominal_calendar_reference() {
            let calendar = calendars
                .read_reference(reference, as_of, deadline, cancellation.clone())
                .await
                .map_err(history::calendar_error)?
                .ok_or(PortfolioApplicationServiceError::StateChanged)?;
            self.qualification
                .recheck_nominal(&calendar, as_of)
                .map_err(history::calendar_error)?;
        } else {
            self.qualification
                .recheck(
                    authority.ok_or(PortfolioApplicationServiceError::StateChanged)?,
                    as_of,
                )
                .map_err(history::calendar_error)?;
        }
        ensure_live(deadline, cancellation)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "source authorities, original financial context and lifecycle are independently bound"
)]
pub(super) async fn calculate(
    reader: &AnalyticalReadCapability,
    research: &ResearchService,
    calendars: &CompletedMarketSessionReadCapability,
    completed_sessions: Option<&CompletedMarketSessionAuthority>,
    portfolio: &PortfolioAnalysisPortfolioSnapshot,
    marked: &PortfolioAnalysisMarkedPortfolioEvidence,
    risk: &PortfolioAnalysisRiskEvidence,
    market: &PortfolioCandidateMarketEvidence,
    as_of: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<PortfolioRiskSizingEvidence>, PortfolioApplicationServiceError> {
    ensure_live(deadline, cancellation)?;
    let Some(cash) = portfolio.settlement_available_cash() else {
        return Ok(None);
    };
    let Some(scenario) = risk.historical_scenario() else {
        return Ok(None);
    };
    if !scenario.current_session_covered() {
        return Ok(None);
    }
    let terms = market.execution_terms();
    let current = match marked.current_position() {
        PortfolioAnalysisCurrentPosition::NoPosition => 0,
        PortfolioAnalysisCurrentPosition::Position { quantity, .. } => {
            match QuantityLots::try_from_decimal(quantity, terms.lot_size()) {
                Ok(lots) => lots.get(),
                Err(QuantityError::NegativeQuantity | QuantityError::InexactLot) => {
                    return Ok(None);
                }
                Err(QuantityError::Overflow) => {
                    return Err(PortfolioApplicationServiceError::ResourceExhausted);
                }
            }
        }
    };
    let unit_quantity = QuantityLots::new(1)
        .and_then(|lots| lots.checked_to_decimal(terms.lot_size()))
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let per_lot = marked_value(
        market.observation().unit_mark(),
        unit_quantity,
        terms.contract_multiplier(),
    )?;
    if per_lot.amount() <= Decimal::ZERO
        || marked.marked_equity().amount() <= Decimal::ZERO
        || cash.currency() != portfolio.reporting_currency()
        || per_lot.currency() != cash.currency()
        || terms.instrument_id() != marked.candidate_instrument_id()
        || market.portfolio_revision() != portfolio.revision()
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    // Reserve is nonnegative, so every cash-feasible target is inside this loose search bound.
    // A signed cash deficit never increases the bound. It remains for the reserve owner to admit.
    let additional = exact_floor_ratio(cash.amount().max(Decimal::ZERO), per_lot.amount())?;
    let upper = i64::try_from(additional)
        .ok()
        .and_then(|n| current.checked_add(n))
        .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
    let source = match history::read_source_history(
        reader,
        research,
        calendars,
        completed_sessions,
        marked.candidate_instrument_id(),
        portfolio.reporting_currency(),
        as_of,
        deadline,
        cancellation,
    )
    .await?
    {
        Ok(source) => source,
        Err(_) => return Ok(None),
    };
    let Some(qualification) = source.qualification else {
        return Ok(None);
    };
    if !qualification.current_session_covered() {
        return Ok(None);
    }
    if current != 0
        && scenario.source_history(marked.candidate_instrument_id()) != Some(source.digest)
    {
        return Err(PortfolioApplicationServiceError::StateChanged);
    }
    let maximum = portfolio
        .policy()
        .maximum_historical_return_observations()
        .get();
    let bars = source.output.bars();
    let bars = &bars[bars.len().saturating_sub(maximum.saturating_add(1))..];
    let mut coefficients = Vec::new();
    coefficients
        .try_reserve_exact(bars.len().saturating_sub(1))
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-target-lot-historical-risk/v1\0");
    canonical_evidence(&mut digest, portfolio.evidence_digest());
    canonical_evidence(&mut digest, marked.evidence_digest());
    canonical_evidence(&mut digest, risk.evidence_digest());
    canonical_evidence(&mut digest, source.digest);
    canonical_money(&mut digest, per_lot);
    canonical_money(&mut digest, cash);
    digest.update(current.to_be_bytes());
    digest.update(upper.to_be_bytes());
    let mut next = 0;
    for pair in bars.windows(2) {
        ensure_live(deadline, cancellation)?;
        let opening = &pair[0];
        let closing = &pair[1];
        let opening_at = history::source_session_close(&source.output, opening)?;
        let closing_at = history::source_session_close(&source.output, closing)?;
        if opening_at >= closing_at
            || closing_at > as_of
            || opening.currency() != portfolio.reporting_currency()
            || closing.currency() != portfolio.reporting_currency()
            || opening.adjustment() != MarketBarAdjustment::All
            || closing.adjustment() != MarketBarAdjustment::All
            || opening.close().amount() <= Decimal::ZERO
            || closing.close().amount() <= Decimal::ZERO
        {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        let key = (opening_at, closing_at);
        let base = if scenario.is_cash_only() {
            Decimal::ZERO // The admitted reporting-currency cash scenario has no price shock.
        } else {
            while next < scenario.periods().len()
                && (
                    scenario.periods()[next].opening_at,
                    scenario.periods()[next].closing_at,
                ) < key
            {
                next += 1;
            }
            let Some(period) = scenario
                .periods()
                .get(next)
                .filter(|period| (period.opening_at, period.closing_at) == key)
            else {
                continue;
            };
            period.weighted_return
        };
        // Same checked Decimal source-return convention as current portfolio scenarios. The
        // affine shock is anchored at the admitted current portfolio, not an inferred zero risk.
        let slope = closing
            .close()
            .amount()
            .checked_sub(opening.close().amount())
            .and_then(|change| change.checked_mul(per_lot.amount()))
            .and_then(|change| change.checked_div(opening.close().amount()))
            .and_then(|impact| impact.checked_div(marked.marked_equity().amount()))
            .map(|value| value.normalize())
            .ok_or(PortfolioApplicationServiceError::Analytics)?;
        digest.update(opening_at.unix_nanos().to_be_bytes());
        digest.update(closing_at.unix_nanos().to_be_bytes());
        canonical_decimal(&mut digest, base);
        canonical_decimal(&mut digest, slope);
        coefficients.push((base, slope));
    }
    if coefficients.len()
        < portfolio
            .policy()
            .minimum_historical_return_observations()
            .get()
    {
        return Ok(None);
    }
    let budget_bps = portfolio
        .setup()
        .setup()
        .profile()
        .maximum_downside_loss_bps_of_marked_equity();
    digest.update(budget_bps.to_be_bytes());
    digest.update((coefficients.len() as u64).to_be_bytes());
    drop(source.output);
    let range = feasible_target_lots(
        coefficients,
        current,
        upper,
        budget_bps,
        deadline,
        cancellation,
    )?;
    match range {
        CapacityRange::Lots(value) => {
            digest.update([1]);
            digest.update(value.lower().get().to_be_bytes());
            digest.update(value.upper().get().to_be_bytes());
        }
        CapacityRange::NoFeasibleLots => digest.update([0]),
        CapacityRange::Notional(_) => {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
    }
    ensure_live(deadline, cancellation)?;
    Ok(Some(PortfolioRiskSizingEvidence {
        range,
        qualification,
        digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
    }))
}

fn feasible_target_lots(
    coefficients: Vec<(Decimal, Decimal)>,
    current: i64,
    upper: i64,
    budget_bps: u16,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CapacityRange, PortfolioApplicationServiceError> {
    if current < 0 || upper < current || coefficients.is_empty() {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    let budget = Decimal::from(budget_bps)
        .checked_div(Decimal::from(10_000))
        .ok_or(PortfolioApplicationServiceError::Analytics)?;
    // Freeze admitted decimal coefficients on one exact integer scale. All search comparisons
    // are exact; no rounded ES, display basis points or advisory risk ppm decides feasibility.
    let scale = coefficients
        .iter()
        .flat_map(|(base, slope)| [base.scale(), slope.scale()])
        .fold(budget.normalize().scale(), u32::max);
    let mut exact = Vec::new();
    exact
        .try_reserve_exact(coefficients.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    for (base, slope) in coefficients {
        exact.push((scaled(base, scale)?, scaled(slope, scale)?));
    }
    let budget_total = scaled(budget, scale)?
        .checked_mul(i128::try_from(exact.len())?)
        .ok_or(PortfolioApplicationServiceError::Analytics)?;
    let mut losses = Vec::new();
    losses
        .try_reserve_exact(exact.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    losses.resize(exact.len(), 0_i128);
    let mut score = |lots: i64| -> Result<i128, PortfolioApplicationServiceError> {
        ensure_live(deadline, cancellation)?;
        let delta = i128::from(lots) - i128::from(current);
        for ((base, slope), loss) in exact.iter().zip(&mut losses) {
            let value = slope
                .checked_mul(delta)
                .and_then(|change| base.checked_add(change))
                .ok_or(PortfolioApplicationServiceError::Analytics)?;
            *loss = value
                .checked_neg()
                .ok_or(PortfolioApplicationServiceError::Analytics)?
                .max(0);
        }
        let (complete, remainder) = fractional_tail_95(&mut losses)?;
        let total = losses[..complete]
            .iter()
            .try_fold(0_i128, |sum, loss| sum.checked_add(*loss))
            .and_then(|sum| sum.checked_mul(20))
            .ok_or(PortfolioApplicationServiceError::Analytics)?;
        let boundary = if remainder == 0 {
            0
        } else {
            losses[complete]
                .checked_mul(i128::try_from(remainder)?)
                .ok_or(PortfolioApplicationServiceError::Analytics)?
        };
        total
            .checked_add(boundary)
            .ok_or(PortfolioApplicationServiceError::Analytics)
    };
    // The nonnegative loss of each affine scenario is convex. The weighted largest-tail sum is
    // convex too. Locate its discrete minimum first; a hedge may require strictly positive lots.
    let mut left = 0;
    let mut right = upper;
    while left < right {
        let mid = left + (right - left) / 2;
        if score(mid)? <= score(mid + 1)? {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    let minimum = left;
    let range = if score(minimum)? > budget_total {
        CapacityRange::NoFeasibleLots
    } else {
        left = 0;
        right = minimum;
        while left < right {
            let mid = left + (right - left) / 2;
            if score(mid)? <= budget_total {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        let lower = left;
        left = minimum;
        right = upper;
        while left < right {
            let mid = left + (right - left) / 2 + (right - left) % 2;
            if score(mid)? <= budget_total {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        CapacityRange::Lots(
            LotRange::try_new(
                QuantityLots::new(lower)
                    .map_err(|_| PortfolioApplicationServiceError::Analytics)?,
                QuantityLots::new(left).map_err(|_| PortfolioApplicationServiceError::Analytics)?,
            )
            .map_err(|_| PortfolioApplicationServiceError::Analytics)?,
        )
    };
    Ok(range)
}

fn scaled(value: Decimal, scale: u32) -> Result<i128, PortfolioApplicationServiceError> {
    let value = value.normalize();
    let multiplier = 10_i128
        .checked_pow(
            scale
                .checked_sub(value.scale())
                .ok_or(PortfolioApplicationServiceError::Analytics)?,
        )
        .ok_or(PortfolioApplicationServiceError::Analytics)?;
    value
        .mantissa()
        .checked_mul(multiplier)
        .ok_or(PortfolioApplicationServiceError::Analytics)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn historical_risk_capacity_retains_required_hedge_and_exact_boundaries()
    -> Result<(), Box<dyn std::error::Error>> {
        // Sixty simultaneous sessions: half have -6% current portfolio return and a +2%
        // per-lot hedge shock, half have -2% and -1%. The current position is two lots.
        // At zero lots the two returns are -10%, 0%; at three: -4%, -3%; at four:
        // -2%, -4%; at five: 0%, -5%. Thus the unchanged 4% ES policy permits [3, 4].
        let first = (Decimal::new(-6, 2), Decimal::new(2, 2));
        let second = (Decimal::new(-2, 2), Decimal::new(-1, 2));
        let coefficients = [vec![first; 30], vec![second; 30]].concat();
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        let range = feasible_target_lots(coefficients.clone(), 2, 8, 400, deadline, &cancellation)?;
        assert_eq!(
            range,
            CapacityRange::Lots(LotRange::try_new(
                QuantityLots::new(3)?,
                QuantityLots::new(4)?,
            )?)
        );
        // Independently stated scenario losses exercise the existing production ES kernel.
        let zero_losses = [vec![Decimal::new(10, 2); 30], vec![Decimal::ZERO; 30]].concat();
        let upper_losses = [vec![Decimal::ZERO; 30], vec![Decimal::new(5, 2); 30]].concat();
        assert_eq!(
            exact_discrete_expected_shortfall_95(&zero_losses)?,
            Decimal::new(10, 2)
        );
        assert_eq!(
            exact_discrete_expected_shortfall_95(&upper_losses)?,
            Decimal::new(5, 2)
        );
        assert!(exact_discrete_expected_shortfall_95(&zero_losses)? > Decimal::new(4, 2));
        assert!(exact_discrete_expected_shortfall_95(&upper_losses)? > Decimal::new(4, 2));
        assert_eq!(
            feasible_target_lots(coefficients, 2, 8, 300, deadline, &cancellation)?,
            CapacityRange::NoFeasibleLots,
        );
        Ok(())
    }
}
