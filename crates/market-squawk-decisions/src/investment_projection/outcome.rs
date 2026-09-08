//! Exact mark-relative outcome ranges without probability, cost, tax, or benchmark invention.

use market_squawk_domain::{InstrumentExecutionTerms, Money, PriceError, PriceTicks, QuantityLots};
use market_squawk_modeling::ForecastCentralStatistic;

use crate::{GeneratedInvestmentProposal, PriceForecastEvidence, TargetPriceRange};

use super::digest::outcome_projection_digest;
use super::{
    INVESTMENT_OUTCOME_PROJECTION_SCHEMA_VERSION, InvestmentProjectionAuthority,
    InvestmentProjectionBinding, InvestmentProjectionDigest, InvestmentProjectionError,
};

/// Exact signed money interval, inclusive at both ends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SignedMoneyRange {
    lower: Money,
    upper: Money,
}

impl SignedMoneyRange {
    /// Constructs an ordered, same-currency signed interval.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies or `lower > upper`.
    pub fn try_new(lower: Money, upper: Money) -> Result<Self, InvestmentProjectionError> {
        if lower.currency() != upper.currency() {
            return Err(InvestmentProjectionError::CurrencyMismatch);
        }
        if lower.amount() > upper.amount() {
            return Err(InvestmentProjectionError::InvalidFinancialValue);
        }
        Ok(Self { lower, upper })
    }

    /// Returns the inclusive lower amount.
    #[must_use]
    pub const fn lower(self) -> Money {
        self.lower
    }

    /// Returns the inclusive upper amount.
    #[must_use]
    pub const fn upper(self) -> Money {
        self.upper
    }
}

/// Exact signed financial ratio retained as a numerator and positive denominator.
///
/// No decimal division or display rounding is performed. For mark-relative returns, the numerator
/// is the exact target-price change and the denominator is the exact proposal mark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactFinancialRatio {
    numerator: Money,
    denominator: Money,
}

impl ExactFinancialRatio {
    /// Constructs an exact same-currency ratio with a strictly positive denominator.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies or a nonpositive denominator.
    pub fn try_new(
        numerator: Money,
        denominator: Money,
    ) -> Result<Self, InvestmentProjectionError> {
        if numerator.currency() != denominator.currency() {
            return Err(InvestmentProjectionError::CurrencyMismatch);
        }
        if denominator.amount() <= rust_decimal::Decimal::ZERO {
            return Err(InvestmentProjectionError::InvalidFinancialValue);
        }
        Ok(Self {
            numerator,
            denominator,
        })
    }

    /// Returns the exact signed numerator.
    #[must_use]
    pub const fn numerator(self) -> Money {
        self.numerator
    }

    /// Returns the exact positive denominator.
    #[must_use]
    pub const fn denominator(self) -> Money {
        self.denominator
    }
}

/// Inclusive exact mark-relative return interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactFinancialRatioRange {
    lower: ExactFinancialRatio,
    upper: ExactFinancialRatio,
}

impl ExactFinancialRatioRange {
    /// Constructs an ordered interval whose endpoints use the same denominator.
    ///
    /// # Errors
    ///
    /// Rejects different denominators or a descending numerator interval.
    pub fn try_new(
        lower: ExactFinancialRatio,
        upper: ExactFinancialRatio,
    ) -> Result<Self, InvestmentProjectionError> {
        if lower.denominator != upper.denominator {
            return Err(InvestmentProjectionError::InvalidFinancialValue);
        }
        if lower.numerator.currency() != upper.numerator.currency() {
            return Err(InvestmentProjectionError::CurrencyMismatch);
        }
        if lower.numerator.amount() > upper.numerator.amount() {
            return Err(InvestmentProjectionError::InvalidFinancialValue);
        }
        Ok(Self { lower, upper })
    }

    /// Returns the inclusive lower ratio.
    #[must_use]
    pub const fn lower(self) -> ExactFinancialRatio {
        self.lower
    }

    /// Returns the inclusive upper ratio.
    #[must_use]
    pub const fn upper(self) -> ExactFinancialRatio {
        self.upper
    }
}

/// Checked scale needed to turn a per-unit price change into exact gross position P/L.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactPositionScale {
    terms: InstrumentExecutionTerms,
    quantity: QuantityLots,
}

impl ExactPositionScale {
    /// Captures already validated instrument terms and a nonnegative integer lot count.
    #[must_use]
    pub const fn new(terms: InstrumentExecutionTerms, quantity: QuantityLots) -> Self {
        Self { terms, quantity }
    }

    /// Returns the immutable execution terms used only for exact scaling.
    #[must_use]
    pub const fn terms(self) -> InstrumentExecutionTerms {
        self.terms
    }

    /// Returns the exact nonnegative number of instrument lots.
    #[must_use]
    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }
}

/// Availability of exact-quantity gross price P/L.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GrossPricePnlAvailability {
    /// Gross P/L before costs and tax for the exact supplied quantity.
    Available(SignedMoneyRange),
    /// No checked exact quantity and execution-term scale was supplied.
    UnavailableExactQuantityNotSupplied,
}

/// Expected return availability under admitted conditional-mean forecast evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpectedReturnAvailability {
    /// Exact gross mark-relative expected price return at the proposal horizon.
    ///
    /// This value is derived only from an admitted conditional-mean terminal price. It is not a
    /// probability of profit and is not inferred from calibration coverage intervals.
    Available(ExactFinancialRatio),
    /// No separately admitted conditional mean terminal value was supplied.
    ///
    /// Calibration bands alone do not establish this statistic.
    UnavailableAdmittedExpectedTerminalValueNotSupplied,
}

/// Expected gross price P/L availability for one exact supplied position scale.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpectedGrossPricePnlAvailability {
    /// Exact quantity-scaled expected price P/L before costs and tax.
    ///
    /// This is gross expected price P/L, never net profit.
    Available(Money),
    /// No separately admitted conditional mean terminal value was supplied.
    UnavailableAdmittedExpectedTerminalValueNotSupplied,
    /// No checked exact quantity and execution-term scale was supplied.
    UnavailableExactQuantityNotSupplied,
}

/// Net P/L availability for this exact projected position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetPnlAvailability {
    /// No exact forward transaction-cost evidence was supplied for this quantity and horizon.
    UnavailableExactForwardCostEvidenceNotSupplied,
}

/// Benchmark-relative return availability at proposal time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchmarkReturnAvailability {
    /// No exact proposal-time benchmark level and horizon outcome evidence was supplied.
    UnavailableExactProposalTimeBenchmarkEvidenceNotSupplied,
}

/// After-tax P/L availability for this account and quantity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AfterTaxPnlAvailability {
    /// No account-, lot-, jurisdiction-, and horizon-specific tax evidence was supplied.
    UnavailableExactTaxEvidenceNotSupplied,
}

/// One downside, base, or upside horizon range relative to the exact proposal mark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GrossMarkRelativeRange {
    pub(super) price_range: TargetPriceRange,
    pub(super) absolute_change: SignedMoneyRange,
    pub(super) gross_return_from_mark: ExactFinancialRatioRange,
    pub(super) gross_price_pnl: GrossPricePnlAvailability,
}

impl GrossMarkRelativeRange {
    /// Returns the original exact horizon price range.
    #[must_use]
    pub const fn price_range(self) -> TargetPriceRange {
        self.price_range
    }

    /// Returns the signed target-price change range from the exact proposal mark.
    #[must_use]
    pub const fn absolute_change(self) -> SignedMoneyRange {
        self.absolute_change
    }

    /// Returns exact gross mark-relative return ratios, without decimal division.
    #[must_use]
    pub const fn gross_return_from_mark(self) -> ExactFinancialRatioRange {
        self.gross_return_from_mark
    }

    /// Returns exact-quantity gross price P/L when an exact position scale was supplied.
    #[must_use]
    pub const fn gross_price_pnl(self) -> GrossPricePnlAvailability {
        self.gross_price_pnl
    }
}

/// Exact signed distance from the proposal mark to one generated action/reference zone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarkToZoneDistance {
    pub(super) zone: TargetPriceRange,
    pub(super) absolute_distance: SignedMoneyRange,
    pub(super) relative_distance_from_mark: ExactFinancialRatioRange,
}

impl MarkToZoneDistance {
    /// Returns the exact generated zone.
    #[must_use]
    pub const fn zone(self) -> TargetPriceRange {
        self.zone
    }

    /// Returns the signed zone-boundary distances from the proposal mark.
    #[must_use]
    pub const fn absolute_distance(self) -> SignedMoneyRange {
        self.absolute_distance
    }

    /// Returns exact zone-boundary distance ratios relative to the mark.
    #[must_use]
    pub const fn relative_distance_from_mark(self) -> ExactFinancialRatioRange {
        self.relative_distance_from_mark
    }
}

/// Immutable exact outcome projection bound to one generated proposal derivation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvestmentOutcomeProjection {
    pub(super) binding: InvestmentProjectionBinding,
    pub(super) authority: InvestmentProjectionAuthority,
    pub(super) instrument_id: market_squawk_domain::InstrumentId,
    pub(super) mark: Money,
    pub(super) horizon_at: market_squawk_domain::Timestamp,
    pub(super) proposal_expires_at: market_squawk_domain::Timestamp,
    pub(super) position_scale: Option<ExactPositionScale>,
    pub(super) downside: GrossMarkRelativeRange,
    pub(super) base: GrossMarkRelativeRange,
    pub(super) upside: GrossMarkRelativeRange,
    pub(super) entry_distance: MarkToZoneDistance,
    pub(super) add_distance: MarkToZoneDistance,
    pub(super) trim_distance: MarkToZoneDistance,
    pub(super) exit_distance: MarkToZoneDistance,
    pub(super) expected_return: ExpectedReturnAvailability,
    pub(super) expected_gross_price_pnl: ExpectedGrossPricePnlAvailability,
    pub(super) net_pnl: NetPnlAvailability,
    pub(super) benchmark_return: BenchmarkReturnAvailability,
    pub(super) after_tax_pnl: AfterTaxPnlAvailability,
    pub(super) result_digest: InvestmentProjectionDigest,
}

impl InvestmentOutcomeProjection {
    /// Derives exact gross mark-relative ranges and optional exact-quantity gross price P/L.
    ///
    /// # Errors
    ///
    /// Requires the proposal's exact positive market evidence. When `position_scale` is supplied,
    /// its terms must match the proposal instrument, quote currency, settlement currency, and every
    /// projected price must be an exact execution-tick multiple.
    pub fn try_from_proposal(
        proposal: &GeneratedInvestmentProposal,
        position_scale: Option<ExactPositionScale>,
    ) -> Result<Self, InvestmentProjectionError> {
        let evidence = proposal.evidence();
        let market = evidence
            .market()
            .ok_or(InvestmentProjectionError::MissingProposalEvidence)?;
        let forecast = *evidence
            .price_forecast()
            .ok_or(InvestmentProjectionError::MissingProposalEvidence)?;
        let mark = market.price();
        if mark.amount() <= rust_decimal::Decimal::ZERO {
            return Err(InvestmentProjectionError::InvalidFinancialValue);
        }
        if market.instrument_id() != evidence.instrument_id()
            || forecast.instrument_id() != evidence.instrument_id()
        {
            return Err(InvestmentProjectionError::InstrumentMismatch);
        }
        if mark.currency() != evidence.currency() {
            return Err(InvestmentProjectionError::CurrencyMismatch);
        }

        let ladder = proposal.price_ladder();
        let ranges = [
            ladder.downside_range(),
            ladder.base_range(),
            ladder.upside_range(),
            ladder.entry_range(),
            ladder.add_range(),
            ladder.trim_range(),
            ladder.exit_range(),
        ];
        ensure_ranges_match_mark(ranges, mark)?;
        if let Some(scale) = position_scale {
            ensure_execution_terms(
                scale.terms,
                evidence.instrument_id(),
                evidence.currency(),
                mark,
                ranges,
            )?;
        }
        let (expected_return, expected_gross_price_pnl) =
            project_expected_outcome(forecast, mark, proposal.horizon_at(), position_scale)?;

        let binding =
            InvestmentProjectionBinding::new(proposal.proposal_id(), proposal.derivation_digest());
        let mut projection = Self {
            binding,
            authority: InvestmentProjectionAuthority::AnalysisOnlyNoMutationNoExecution,
            instrument_id: evidence.instrument_id(),
            mark,
            horizon_at: proposal.horizon_at(),
            proposal_expires_at: proposal.expires_at(),
            position_scale,
            downside: project_horizon_range(ladder.downside_range(), mark, position_scale)?,
            base: project_horizon_range(ladder.base_range(), mark, position_scale)?,
            upside: project_horizon_range(ladder.upside_range(), mark, position_scale)?,
            entry_distance: project_zone_distance(ladder.entry_range(), mark)?,
            add_distance: project_zone_distance(ladder.add_range(), mark)?,
            trim_distance: project_zone_distance(ladder.trim_range(), mark)?,
            exit_distance: project_zone_distance(ladder.exit_range(), mark)?,
            expected_return,
            expected_gross_price_pnl,
            net_pnl: NetPnlAvailability::UnavailableExactForwardCostEvidenceNotSupplied,
            benchmark_return:
                BenchmarkReturnAvailability::UnavailableExactProposalTimeBenchmarkEvidenceNotSupplied,
            after_tax_pnl: AfterTaxPnlAvailability::UnavailableExactTaxEvidenceNotSupplied,
            result_digest: InvestmentProjectionDigest::from_sha256([0; 32]),
        };
        projection.result_digest = outcome_projection_digest(&projection);
        Ok(projection)
    }

    /// Returns the canonical projection schema version committed by the result digest.
    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        INVESTMENT_OUTCOME_PROJECTION_SCHEMA_VERSION
    }

    /// Returns the exact generated-proposal binding.
    #[must_use]
    pub const fn binding(&self) -> InvestmentProjectionBinding {
        self.binding
    }

    /// Returns the analysis-only, no-mutation, no-execution marker.
    #[must_use]
    pub const fn authority(&self) -> InvestmentProjectionAuthority {
        self.authority
    }

    /// Returns the stable instrument whose mark and ranges are projected.
    #[must_use]
    pub const fn instrument_id(&self) -> market_squawk_domain::InstrumentId {
        self.instrument_id
    }

    /// Returns the exact proposal reference mark.
    #[must_use]
    pub const fn mark(&self) -> Money {
        self.mark
    }

    /// Returns the exact proposal outcome horizon.
    #[must_use]
    pub const fn horizon_at(&self) -> market_squawk_domain::Timestamp {
        self.horizon_at
    }

    /// Returns the exclusive proposal expiry retained by this sidecar.
    #[must_use]
    pub const fn proposal_expires_at(&self) -> market_squawk_domain::Timestamp {
        self.proposal_expires_at
    }

    /// Returns the exact position scale used for gross P/L, when supplied.
    #[must_use]
    pub const fn position_scale(&self) -> Option<ExactPositionScale> {
        self.position_scale
    }

    /// Returns the calibrated downside horizon range relative to the mark.
    #[must_use]
    pub const fn downside(&self) -> GrossMarkRelativeRange {
        self.downside
    }

    /// Returns the generated base horizon range relative to the mark.
    ///
    /// This is a named scenario range, not an expected return.
    #[must_use]
    pub const fn base(&self) -> GrossMarkRelativeRange {
        self.base
    }

    /// Returns the calibrated upside horizon range relative to the mark.
    #[must_use]
    pub const fn upside(&self) -> GrossMarkRelativeRange {
        self.upside
    }

    /// Returns the exact mark-to-entry-zone distance.
    #[must_use]
    pub const fn entry_distance(&self) -> MarkToZoneDistance {
        self.entry_distance
    }

    /// Returns the exact mark-to-add-zone distance.
    #[must_use]
    pub const fn add_distance(&self) -> MarkToZoneDistance {
        self.add_distance
    }

    /// Returns the exact mark-to-trim-zone distance.
    #[must_use]
    pub const fn trim_distance(&self) -> MarkToZoneDistance {
        self.trim_distance
    }

    /// Returns the exact mark-to-downside-invalidation/exit-zone distance.
    #[must_use]
    pub const fn exit_distance(&self) -> MarkToZoneDistance {
        self.exit_distance
    }

    /// Returns exact expected return only when an admitted conditional mean is present.
    #[must_use]
    pub const fn expected_return(&self) -> ExpectedReturnAvailability {
        self.expected_return
    }

    /// Returns exact quantity-scaled expected gross price P/L when its authorities are present.
    #[must_use]
    pub const fn expected_gross_price_pnl(&self) -> ExpectedGrossPricePnlAvailability {
        self.expected_gross_price_pnl
    }

    /// Returns the closed net-P/L unavailability state.
    #[must_use]
    pub const fn net_pnl(&self) -> NetPnlAvailability {
        self.net_pnl
    }

    /// Returns the closed benchmark-relative-return unavailability state.
    #[must_use]
    pub const fn benchmark_return(&self) -> BenchmarkReturnAvailability {
        self.benchmark_return
    }

    /// Returns the closed after-tax-P/L unavailability state.
    #[must_use]
    pub const fn after_tax_pnl(&self) -> AfterTaxPnlAvailability {
        self.after_tax_pnl
    }

    /// Returns the versioned SHA-256 identity of all inputs and exact outputs.
    #[must_use]
    pub const fn result_digest(&self) -> InvestmentProjectionDigest {
        self.result_digest
    }
}

pub(super) fn ensure_execution_terms(
    terms: InstrumentExecutionTerms,
    instrument_id: market_squawk_domain::InstrumentId,
    currency: market_squawk_domain::Currency,
    mark: Money,
    ranges: [TargetPriceRange; 7],
) -> Result<(), InvestmentProjectionError> {
    if terms.instrument_id() != instrument_id {
        return Err(InvestmentProjectionError::InstrumentMismatch);
    }
    if terms.quote_currency() != currency || mark.currency() != currency {
        return Err(InvestmentProjectionError::CurrencyMismatch);
    }
    if terms.settlement_currency() != Some(currency) {
        return Err(InvestmentProjectionError::SettlementCurrencyMismatch);
    }
    ensure_price_on_tick(mark, terms)?;
    for range in ranges {
        ensure_price_on_tick(range.lower(), terms)?;
        ensure_price_on_tick(range.upper(), terms)?;
    }
    Ok(())
}

fn ensure_ranges_match_mark(
    ranges: [TargetPriceRange; 7],
    mark: Money,
) -> Result<(), InvestmentProjectionError> {
    for range in ranges {
        if range.lower().currency() != mark.currency()
            || range.upper().currency() != mark.currency()
        {
            return Err(InvestmentProjectionError::CurrencyMismatch);
        }
        if range.lower().amount() < rust_decimal::Decimal::ZERO
            || range.lower().amount() > range.upper().amount()
        {
            return Err(InvestmentProjectionError::InvalidFinancialValue);
        }
    }
    Ok(())
}

fn ensure_price_on_tick(
    price: Money,
    terms: InstrumentExecutionTerms,
) -> Result<(), InvestmentProjectionError> {
    match PriceTicks::try_from_decimal(price.amount(), terms.price_tick()) {
        Ok(_) => Ok(()),
        Err(PriceError::InexactTick) => Err(InvestmentProjectionError::PriceNotOnExecutionTick),
        Err(PriceError::Overflow) => Err(InvestmentProjectionError::ArithmeticOverflow),
    }
}

fn project_expected_outcome(
    forecast: PriceForecastEvidence,
    mark: Money,
    proposal_horizon_at: market_squawk_domain::Timestamp,
    position_scale: Option<ExactPositionScale>,
) -> Result<
    (
        ExpectedReturnAvailability,
        ExpectedGrossPricePnlAvailability,
    ),
    InvestmentProjectionError,
> {
    if forecast.horizon_at() != proposal_horizon_at {
        return Err(InvestmentProjectionError::InvalidTimeOrder);
    }

    match (
        forecast.expected_terminal_statistic(),
        forecast.expected_terminal_price(),
        forecast.expected_terminal_horizon_at(),
        forecast.expected_terminal_statistic_identity(),
    ) {
        (None, None, None, None) => Ok((
            ExpectedReturnAvailability::UnavailableAdmittedExpectedTerminalValueNotSupplied,
            ExpectedGrossPricePnlAvailability::UnavailableAdmittedExpectedTerminalValueNotSupplied,
        )),
        (
            Some(ForecastCentralStatistic::ModelEstimatedConditionalMean),
            Some(expected_terminal_price),
            Some(expected_terminal_horizon_at),
            Some(expected_terminal_statistic_identity),
        ) => {
            if expected_terminal_horizon_at != proposal_horizon_at {
                return Err(InvestmentProjectionError::InvalidTimeOrder);
            }
            if expected_terminal_price.currency() != mark.currency() {
                return Err(InvestmentProjectionError::CurrencyMismatch);
            }
            if expected_terminal_price.amount() <= rust_decimal::Decimal::ZERO
                || expected_terminal_price != forecast.cases().base()
                || expected_terminal_statistic_identity != forecast.output_binding_identity()
            {
                return Err(InvestmentProjectionError::InvalidFinancialValue);
            }

            let expected_price_change = expected_terminal_price
                .checked_sub(mark)
                .map_err(map_financial_error)?;
            let expected_return = ExpectedReturnAvailability::Available(
                ExactFinancialRatio::try_new(expected_price_change, mark)?,
            );
            let expected_gross_price_pnl = match position_scale {
                Some(scale) => {
                    ensure_price_on_tick(expected_terminal_price, scale.terms)?;
                    ExpectedGrossPricePnlAvailability::Available(scale_money(
                        expected_price_change,
                        scale,
                    )?)
                }
                None => ExpectedGrossPricePnlAvailability::UnavailableExactQuantityNotSupplied,
            };
            Ok((expected_return, expected_gross_price_pnl))
        }
        (
            Some(
                ForecastCentralStatistic::ModelEstimatedConditionalMean
                | ForecastCentralStatistic::Unavailable,
            ),
            _,
            _,
            _,
        )
        | (None, _, _, _) => Err(InvestmentProjectionError::InvalidFinancialValue),
    }
}

fn project_horizon_range(
    price_range: TargetPriceRange,
    mark: Money,
    position_scale: Option<ExactPositionScale>,
) -> Result<GrossMarkRelativeRange, InvestmentProjectionError> {
    let (absolute_change, gross_return_from_mark) = project_relative_range(price_range, mark)?;
    let gross_price_pnl = match position_scale {
        Some(scale) => {
            GrossPricePnlAvailability::Available(scale_money_range(absolute_change, scale)?)
        }
        None => GrossPricePnlAvailability::UnavailableExactQuantityNotSupplied,
    };
    Ok(GrossMarkRelativeRange {
        price_range,
        absolute_change,
        gross_return_from_mark,
        gross_price_pnl,
    })
}

fn project_zone_distance(
    zone: TargetPriceRange,
    mark: Money,
) -> Result<MarkToZoneDistance, InvestmentProjectionError> {
    let (absolute_distance, relative_distance_from_mark) = project_relative_range(zone, mark)?;
    Ok(MarkToZoneDistance {
        zone,
        absolute_distance,
        relative_distance_from_mark,
    })
}

fn project_relative_range(
    price_range: TargetPriceRange,
    mark: Money,
) -> Result<(SignedMoneyRange, ExactFinancialRatioRange), InvestmentProjectionError> {
    let lower = price_range
        .lower()
        .checked_sub(mark)
        .map_err(map_financial_error)?;
    let upper = price_range
        .upper()
        .checked_sub(mark)
        .map_err(map_financial_error)?;
    let absolute = SignedMoneyRange::try_new(lower, upper)?;
    let ratios = ExactFinancialRatioRange::try_new(
        ExactFinancialRatio::try_new(lower, mark)?,
        ExactFinancialRatio::try_new(upper, mark)?,
    )?;
    Ok((absolute, ratios))
}

fn scale_money_range(
    range: SignedMoneyRange,
    scale: ExactPositionScale,
) -> Result<SignedMoneyRange, InvestmentProjectionError> {
    SignedMoneyRange::try_new(
        scale_money(range.lower, scale)?,
        scale_money(range.upper, scale)?,
    )
}

fn scale_money(
    value: Money,
    scale: ExactPositionScale,
) -> Result<Money, InvestmentProjectionError> {
    let quantity = scale
        .quantity
        .checked_to_decimal(scale.terms.lot_size())
        .map_err(|_| InvestmentProjectionError::ArithmeticOverflow)?;
    value
        .checked_mul_decimal(quantity)
        .and_then(|value| value.checked_mul_decimal(scale.terms.contract_multiplier()))
        .map_err(map_financial_error)
}

pub(super) const fn map_financial_error(
    error: market_squawk_domain::FinancialError,
) -> InvestmentProjectionError {
    match error {
        market_squawk_domain::FinancialError::CurrencyMismatch { .. } => {
            InvestmentProjectionError::CurrencyMismatch
        }
        market_squawk_domain::FinancialError::NonPositiveIncrement
        | market_squawk_domain::FinancialError::InvalidCurrency => {
            InvestmentProjectionError::InvalidFinancialValue
        }
        market_squawk_domain::FinancialError::Overflow
        | market_squawk_domain::FinancialError::UnsupportedScale { .. }
        | market_squawk_domain::FinancialError::Price(_)
        | market_squawk_domain::FinancialError::Quantity(_) => {
            InvestmentProjectionError::ArithmeticOverflow
        }
    }
}
