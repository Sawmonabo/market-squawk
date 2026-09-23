//! Pure target-lot feasibility calculations with explicit external capacity authorities.
//!
//! Proposal policy-relative parts-per-million signals are never interpreted as cash, notional, or
//! lots. Liquidity, portfolio-risk, and forward-cost sizing each require a separate exact lot or
//! notional capacity receipt.

use std::cmp::Ordering;

use market_squawk_domain::{
    AccountId, BasisPoints, InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId,
    Money, QuantityLots, RoundingPolicy, Timestamp,
};
use market_squawk_portfolio::PortfolioRevisionToken;
use rust_decimal::Decimal;

use crate::{
    DecisionContentDigest, GeneratedInvestmentProposal, PortfolioPositionState,
    RecommendationAction, TargetPriceRange,
};

use super::digest::sizing_projection_digest;
use super::outcome::{ensure_execution_terms, map_financial_error};
use super::{
    INVESTMENT_SIZING_PROJECTION_SCHEMA_VERSION, InvestmentProjectionAuthority,
    InvestmentProjectionBinding, InvestmentProjectionDigest, InvestmentProjectionError,
};

/// Inclusive nonnegative range of instrument lot counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LotRange {
    lower: QuantityLots,
    upper: QuantityLots,
}

impl LotRange {
    /// Constructs an ordered inclusive lot range.
    ///
    /// # Errors
    ///
    /// Rejects `lower > upper`.
    pub fn try_new(
        lower: QuantityLots,
        upper: QuantityLots,
    ) -> Result<Self, InvestmentProjectionError> {
        if lower > upper {
            return Err(InvestmentProjectionError::InvalidFinancialValue);
        }
        Ok(Self { lower, upper })
    }

    /// Returns the inclusive lower lot count.
    #[must_use]
    pub const fn lower(self) -> QuantityLots {
        self.lower
    }

    /// Returns the inclusive upper lot count.
    #[must_use]
    pub const fn upper(self) -> QuantityLots {
        self.upper
    }
}

/// Inclusive, same-currency nonnegative notional range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NonnegativeMoneyRange {
    lower: Money,
    upper: Money,
}

impl NonnegativeMoneyRange {
    /// Constructs an ordered, same-currency nonnegative money range.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies, negative endpoints, or `lower > upper`.
    pub fn try_new(lower: Money, upper: Money) -> Result<Self, InvestmentProjectionError> {
        if lower.currency() != upper.currency() {
            return Err(InvestmentProjectionError::CurrencyMismatch);
        }
        if lower.amount() < Decimal::ZERO
            || upper.amount() < Decimal::ZERO
            || lower.amount() > upper.amount()
        {
            return Err(InvestmentProjectionError::InvalidFinancialValue);
        }
        Ok(Self { lower, upper })
    }

    /// Returns the inclusive lower notional.
    #[must_use]
    pub const fn lower(self) -> Money {
        self.lower
    }

    /// Returns the inclusive upper notional.
    #[must_use]
    pub const fn upper(self) -> Money {
        self.upper
    }
}

/// Exact capacity representation supplied by an external authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapacityRange {
    /// Capacity already expressed in execution-term-bound integer lots.
    Lots(LotRange),
    /// Capacity expressed as an exact target-position notional interval.
    Notional(NonnegativeMoneyRange),
}

/// Identity- and time-bound capacity evidence for one exact sizing context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SizingCapacityEvidence {
    pub(super) instrument_id: InstrumentId,
    pub(super) account_id: AccountId,
    pub(super) portfolio_revision: PortfolioRevisionToken,
    pub(super) definition_revision: InstrumentDefinitionRevision,
    pub(super) reference_mark: Money,
    pub(super) range: CapacityRange,
    pub(super) content_identity: DecisionContentDigest,
    pub(super) observed_at: Timestamp,
    pub(super) available_at: Timestamp,
    pub(super) expires_at: Timestamp,
}

impl SizingCapacityEvidence {
    /// Constructs one exact capacity receipt.
    ///
    /// # Errors
    ///
    /// Requires `observed_at <= available_at < expires_at`. Context binding is checked again
    /// against the exact proposal, portfolio revision, terms revision, and selected mark during
    /// sizing.
    #[allow(
        clippy::too_many_arguments,
        reason = "capacity context, value, identity, and point-in-time coordinates remain explicit"
    )]
    pub fn try_new(
        instrument_id: InstrumentId,
        account_id: AccountId,
        portfolio_revision: PortfolioRevisionToken,
        definition_revision: InstrumentDefinitionRevision,
        reference_mark: Money,
        range: CapacityRange,
        content_identity: DecisionContentDigest,
        observed_at: Timestamp,
        available_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, InvestmentProjectionError> {
        if observed_at > available_at || available_at >= expires_at {
            return Err(InvestmentProjectionError::InvalidTimeOrder);
        }
        Ok(Self {
            instrument_id,
            account_id,
            portfolio_revision,
            definition_revision,
            reference_mark,
            range,
            content_identity,
            observed_at,
            available_at,
            expires_at,
        })
    }

    /// Returns the bound instrument.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the bound account.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the exact bound portfolio revision.
    #[must_use]
    pub const fn portfolio_revision(&self) -> &PortfolioRevisionToken {
        &self.portfolio_revision
    }

    /// Returns the exact bound instrument-definition revision.
    #[must_use]
    pub const fn definition_revision(&self) -> InstrumentDefinitionRevision {
        self.definition_revision
    }

    /// Returns the exact bound reference mark.
    #[must_use]
    pub const fn reference_mark(&self) -> Money {
        self.reference_mark
    }

    /// Returns the exact lot or notional capacity range.
    #[must_use]
    pub const fn range(&self) -> CapacityRange {
        self.range
    }

    /// Returns the external authority's complete content identity.
    #[must_use]
    pub const fn content_identity(&self) -> DecisionContentDigest {
        self.content_identity
    }

    /// Returns when the capacity fact was observed.
    #[must_use]
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns when the capacity became knowable.
    #[must_use]
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Returns the exclusive capacity expiry.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Presence of separately governed liquidity, risk, or forward-cost capacity evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SizingCapacityAvailability {
    /// Exact identity- and time-bound capacity evidence was supplied.
    Available(Box<SizingCapacityEvidence>),
    /// The external authority supplied no exact capacity evidence.
    UnavailableNotSupplied,
}

/// Exact selected-account portfolio state used by the feasibility calculation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidatePortfolioSizingState {
    pub(super) account_id: AccountId,
    pub(super) instrument_id: InstrumentId,
    pub(super) portfolio_revision: PortfolioRevisionToken,
    pub(super) marked_equity_at_selected_mark: Money,
    pub(super) settlement_available_cash: Money,
    pub(super) current_lots: QuantityLots,
}

impl CandidatePortfolioSizingState {
    /// Constructs exact current portfolio values already measured at the selected proposal mark.
    ///
    /// # Errors
    ///
    /// Requires positive equity and one currency for equity and settlement cash. Settlement cash
    /// may be negative; the reserve cap then accounts for gross proceeds available from reducing
    /// the current position.
    pub fn try_new(
        account_id: AccountId,
        instrument_id: InstrumentId,
        portfolio_revision: PortfolioRevisionToken,
        marked_equity_at_selected_mark: Money,
        settlement_available_cash: Money,
        current_lots: QuantityLots,
    ) -> Result<Self, InvestmentProjectionError> {
        if marked_equity_at_selected_mark.amount() <= Decimal::ZERO {
            return Err(InvestmentProjectionError::InvalidFinancialValue);
        }
        if marked_equity_at_selected_mark.currency() != settlement_available_cash.currency() {
            return Err(InvestmentProjectionError::CurrencyMismatch);
        }
        Ok(Self {
            account_id,
            instrument_id,
            portfolio_revision,
            marked_equity_at_selected_mark,
            settlement_available_cash,
            current_lots,
        })
    }

    /// Returns the exact selected account.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the exact candidate instrument whose current lots are represented.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact selected portfolio revision.
    #[must_use]
    pub const fn portfolio_revision(&self) -> &PortfolioRevisionToken {
        &self.portfolio_revision
    }

    /// Returns portfolio equity already measured at the exact selected mark.
    #[must_use]
    pub const fn marked_equity_at_selected_mark(&self) -> Money {
        self.marked_equity_at_selected_mark
    }

    /// Returns exact settlement-available cash before any hypothetical target change.
    #[must_use]
    pub const fn settlement_available_cash(&self) -> Money {
        self.settlement_available_cash
    }

    /// Returns exact current position lots.
    #[must_use]
    pub const fn current_lots(&self) -> QuantityLots {
        self.current_lots
    }
}

/// Explicit Desktop-policy constraints consumed by the pure sizing kernel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CandidateSizingConstraints {
    pub(super) minimum_cash_reserve: Money,
    pub(super) preferred_weight_lower_basis_points: u16,
    pub(super) preferred_weight_upper_basis_points: u16,
    pub(super) maximum_downside_loss_basis_points: u16,
}

impl CandidateSizingConstraints {
    /// Constructs explicit reserve, preferred-weight, and downside-loss constraints.
    ///
    /// # Errors
    ///
    /// The reserve must be nonnegative and each basis-point value must be within 0 through 10,000,
    /// with the preferred lower bound no greater than its upper bound.
    pub fn try_new(
        minimum_cash_reserve: Money,
        preferred_weight_lower_basis_points: u16,
        preferred_weight_upper_basis_points: u16,
        maximum_downside_loss_basis_points: u16,
    ) -> Result<Self, InvestmentProjectionError> {
        if minimum_cash_reserve.amount() < Decimal::ZERO
            || preferred_weight_lower_basis_points > preferred_weight_upper_basis_points
            || preferred_weight_upper_basis_points > 10_000
            || maximum_downside_loss_basis_points > 10_000
        {
            return Err(InvestmentProjectionError::InvalidSizingConstraint);
        }
        Ok(Self {
            minimum_cash_reserve,
            preferred_weight_lower_basis_points,
            preferred_weight_upper_basis_points,
            maximum_downside_loss_basis_points,
        })
    }

    /// Returns the exact settlement-cash reserve floor.
    #[must_use]
    pub const fn minimum_cash_reserve(self) -> Money {
        self.minimum_cash_reserve
    }

    /// Returns the preferred target-position weight lower bound in basis points.
    #[must_use]
    pub const fn preferred_weight_lower_basis_points(self) -> u16 {
        self.preferred_weight_lower_basis_points
    }

    /// Returns the preferred target-position weight upper bound in basis points.
    #[must_use]
    pub const fn preferred_weight_upper_basis_points(self) -> u16 {
        self.preferred_weight_upper_basis_points
    }

    /// Returns the maximum downside-case loss as a fraction of marked equity in basis points.
    #[must_use]
    pub const fn maximum_downside_loss_basis_points(self) -> u16 {
        self.maximum_downside_loss_basis_points
    }
}

/// Complete typed input to one pure sizing evaluation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvestmentSizingInputs {
    pub(super) evaluated_at: Timestamp,
    pub(super) execution_terms: InstrumentExecutionTerms,
    pub(super) selected_mark: Money,
    pub(super) portfolio: CandidatePortfolioSizingState,
    pub(super) constraints: CandidateSizingConstraints,
    pub(super) liquidity_capacity: SizingCapacityAvailability,
    pub(super) risk_capacity: SizingCapacityAvailability,
    pub(super) forward_cost_capacity: SizingCapacityAvailability,
}

impl InvestmentSizingInputs {
    /// Captures exact sizing state and three separately governed capacity surfaces.
    #[allow(
        clippy::too_many_arguments,
        reason = "portfolio, execution, policy, liquidity, risk, and cost authorities remain distinct"
    )]
    #[must_use]
    pub fn new(
        evaluated_at: Timestamp,
        execution_terms: InstrumentExecutionTerms,
        selected_mark: Money,
        portfolio: CandidatePortfolioSizingState,
        constraints: CandidateSizingConstraints,
        liquidity_capacity: SizingCapacityAvailability,
        risk_capacity: SizingCapacityAvailability,
        forward_cost_capacity: SizingCapacityAvailability,
    ) -> Self {
        Self {
            evaluated_at,
            execution_terms,
            selected_mark,
            portfolio,
            constraints,
            liquidity_capacity,
            risk_capacity,
            forward_cost_capacity,
        }
    }

    /// Returns the point-in-time sizing evaluation coordinate.
    #[must_use]
    pub const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    /// Returns the exact immutable instrument execution terms.
    #[must_use]
    pub const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.execution_terms
    }

    /// Returns the exact proposal mark selected for sizing.
    #[must_use]
    pub const fn selected_mark(&self) -> Money {
        self.selected_mark
    }

    /// Returns the exact selected-account portfolio state.
    #[must_use]
    pub const fn portfolio(&self) -> &CandidatePortfolioSizingState {
        &self.portfolio
    }

    /// Returns the explicit Desktop-owned policy constraints.
    #[must_use]
    pub const fn constraints(&self) -> CandidateSizingConstraints {
        self.constraints
    }

    /// Returns separately governed liquidity capacity availability.
    #[must_use]
    pub const fn liquidity_capacity(&self) -> &SizingCapacityAvailability {
        &self.liquidity_capacity
    }

    /// Returns separately governed portfolio-risk capacity availability.
    #[must_use]
    pub const fn risk_capacity(&self) -> &SizingCapacityAvailability {
        &self.risk_capacity
    }

    /// Returns separately governed forward-cost capacity availability.
    #[must_use]
    pub const fn forward_cost_capacity(&self) -> &SizingCapacityAvailability {
        &self.forward_cost_capacity
    }
}

/// Named sizing constraint or external capacity surface.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SizingConstraintKind {
    /// Settlement cash after a hypothetical target change must retain the explicit reserve.
    CashReserve,
    /// Gross downside-range loss must remain within the explicit equity-relative budget.
    DownsideLoss,
    /// Exact externally supplied liquidity capacity.
    Liquidity,
    /// Exact externally supplied portfolio-risk capacity.
    PortfolioRisk,
    /// Exact externally supplied capacity after forward-cost analysis.
    ///
    /// This is a feasibility interval, not an exact subtractable transaction-cost amount, and it
    /// cannot make outcome net P/L available.
    ForwardCost,
    /// Desktop-policy preferred target-position weight interval.
    PreferredWeight,
}

/// Why a hard or preferred lot range could not be truthfully returned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SizingUnavailableReason {
    /// No exact capacity evidence was supplied for the named external authority.
    CapacityNotSupplied(SizingConstraintKind),
    /// Capacity evidence was not yet knowable at the evaluation time.
    CapacityNotYetAvailable(SizingConstraintKind),
    /// Capacity evidence reached its exclusive expiry before evaluation.
    CapacityExpired(SizingConstraintKind),
    /// A valid exact capacity interval contains no execution-term-compatible lot count.
    CapacityRangeContainsNoLots(SizingConstraintKind),
    /// Even reducing the complete current position at the selected gross mark cannot fund reserve.
    CashReserveExceedsGrossLiquidatableValue,
    /// The available hard-cap lot intervals have no common lot count.
    NoHardFeasibleLotIntersection,
    /// The explicit preferred-weight interval contains no execution-term-compatible lot count.
    PreferredWeightRangeContainsNoLots,
    /// Hard feasibility and the rounded preferred-weight interval have no common lot count.
    NoPreferredFeasibleLotIntersection,
}

/// Exact output for one named sizing constraint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SizingConstraintCap {
    /// The constraint supplied an exact inclusive target-lot interval.
    Available {
        /// Named constraint or external authority.
        kind: SizingConstraintKind,
        /// Exact allowed target lots.
        lot_range: LotRange,
        /// Capacity receipt identity for external authorities; absent for pure local constraints.
        capacity_identity: Option<DecisionContentDigest>,
    },
    /// The constraint could not supply a truthful lot interval.
    Unavailable {
        /// Named constraint or external authority.
        kind: SizingConstraintKind,
        /// Exact fail-closed reason.
        reason: SizingUnavailableReason,
    },
}

impl SizingConstraintCap {
    /// Returns the named constraint represented by this cap.
    #[must_use]
    pub const fn kind(self) -> SizingConstraintKind {
        match self {
            Self::Available { kind, .. } | Self::Unavailable { kind, .. } => kind,
        }
    }
}

/// Availability of an exact inclusive target-lot range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FeasibleLotRangeAvailability {
    /// All mandatory caps were available and shared this exact lot interval.
    Available(LotRange),
    /// One or more exact reasons prevented a truthful feasible interval.
    Unavailable(Box<[SizingUnavailableReason]>),
}

/// Availability of an exact target-position notional range derived entirely in the backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FeasibleNotionalRangeAvailability {
    /// Exact selected-mark notionals corresponding to a feasible lot interval.
    Available(NonnegativeMoneyRange),
    /// The lot interval was unavailable for these exact fail-closed reasons.
    Unavailable(Box<[SizingUnavailableReason]>),
}

/// Exact remainder introduced by execution-lot rounding of preferred notional bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PreferredWeightRoundingRemainder {
    lower_round_up_excess: Money,
    upper_round_down_remainder: Money,
}

impl PreferredWeightRoundingRemainder {
    /// Returns target notional at the first lot on or above the lower bound, minus that bound.
    #[must_use]
    pub const fn lower_round_up_excess(self) -> Money {
        self.lower_round_up_excess
    }

    /// Returns the upper bound minus target notional at the final lot on or below it.
    #[must_use]
    pub const fn upper_round_down_remainder(self) -> Money {
        self.upper_round_down_remainder
    }
}

/// Immutable pure target-lot feasibility result.
///
/// The result deliberately contains no selected target, order side, order quantity, or execution
/// eligibility. Proposal action is retained only as immutable input metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvestmentSizingProjection {
    pub(super) binding: InvestmentProjectionBinding,
    pub(super) proposal_action: RecommendationAction,
    pub(super) authority: InvestmentProjectionAuthority,
    pub(super) inputs: InvestmentSizingInputs,
    pub(super) per_lot_notional: Money,
    pub(super) per_lot_downside_loss: Money,
    pub(super) constraint_caps: Box<[SizingConstraintCap]>,
    pub(super) hard_feasible_lots: FeasibleLotRangeAvailability,
    pub(super) preferred_feasible_lots: FeasibleLotRangeAvailability,
    pub(super) hard_feasible_target_notional: FeasibleNotionalRangeAvailability,
    pub(super) preferred_feasible_target_notional: FeasibleNotionalRangeAvailability,
    pub(super) preferred_weight_rounding: PreferredWeightRoundingRemainder,
    pub(super) hard_binding_caps: Box<[SizingConstraintKind]>,
    pub(super) preferred_binding_caps: Box<[SizingConstraintKind]>,
    pub(super) result_digest: InvestmentProjectionDigest,
}

impl InvestmentSizingProjection {
    /// Evaluates exact target-lot feasibility without selecting a target or mutating the proposal.
    ///
    /// # Errors
    ///
    /// Requires the exact proposal instrument, mark, account, portfolio revision, currency, and
    /// valid execution terms. Supplied capacity receipts must bind the same context. Missing,
    /// not-yet-available, expired, or lot-incompatible capacities become typed unavailable outputs.
    pub fn try_from_proposal(
        proposal: &GeneratedInvestmentProposal,
        inputs: InvestmentSizingInputs,
    ) -> Result<Self, InvestmentProjectionError> {
        validate_sizing_context(proposal, &inputs)?;

        let per_lot_notional = money_for_lots(
            inputs.selected_mark,
            inputs.execution_terms,
            QuantityLots::new(1).map_err(|_| InvestmentProjectionError::ArithmeticOverflow)?,
        )?;
        let downside_range = proposal.price_ladder().downside_range();
        let per_lot_downside_loss =
            per_lot_downside_loss(inputs.selected_mark, downside_range, inputs.execution_terms)?;

        let cash_cap = cash_reserve_cap(&inputs, per_lot_notional)?;
        let downside_cap = downside_loss_cap(&inputs, per_lot_downside_loss)?;
        let liquidity_cap = capacity_cap(
            SizingConstraintKind::Liquidity,
            &inputs.liquidity_capacity,
            &inputs,
            per_lot_notional,
        )?;
        let risk_cap = capacity_cap(
            SizingConstraintKind::PortfolioRisk,
            &inputs.risk_capacity,
            &inputs,
            per_lot_notional,
        )?;
        let forward_cost_cap = capacity_cap(
            SizingConstraintKind::ForwardCost,
            &inputs.forward_cost_capacity,
            &inputs,
            per_lot_notional,
        )?;
        let (preferred_weight_cap, preferred_weight_rounding) =
            preferred_weight_cap(&inputs, per_lot_notional)?;

        let hard_caps = [
            cash_cap,
            downside_cap,
            liquidity_cap,
            risk_cap,
            forward_cost_cap,
        ];
        let all_caps = [
            cash_cap,
            downside_cap,
            liquidity_cap,
            risk_cap,
            forward_cost_cap,
            preferred_weight_cap,
        ];
        let hard_feasible_lots = hard_feasible_range(&hard_caps)?;
        let preferred_feasible_lots =
            preferred_feasible_range(&hard_feasible_lots, preferred_weight_cap);
        let hard_feasible_target_notional =
            target_notional_availability(&hard_feasible_lots, per_lot_notional)?;
        let preferred_feasible_target_notional =
            target_notional_availability(&preferred_feasible_lots, per_lot_notional)?;
        let hard_binding_caps = binding_caps(&hard_caps, available_range(&hard_feasible_lots));
        let preferred_binding_caps =
            binding_caps(&all_caps, available_range(&preferred_feasible_lots));
        let constraint_caps: Box<[SizingConstraintCap]> = Box::new(all_caps);

        let binding =
            InvestmentProjectionBinding::new(proposal.proposal_id(), proposal.derivation_digest());
        let mut projection = Self {
            binding,
            proposal_action: proposal.action(),
            authority: InvestmentProjectionAuthority::AnalysisOnlyNoMutationNoExecution,
            inputs,
            per_lot_notional,
            per_lot_downside_loss,
            constraint_caps,
            hard_feasible_lots,
            preferred_feasible_lots,
            hard_feasible_target_notional,
            preferred_feasible_target_notional,
            preferred_weight_rounding,
            hard_binding_caps,
            preferred_binding_caps,
            result_digest: InvestmentProjectionDigest::from_sha256([0; 32]),
        };
        projection.result_digest = sizing_projection_digest(&projection);
        Ok(projection)
    }

    /// Returns the canonical sizing schema version committed by the result digest.
    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        INVESTMENT_SIZING_PROJECTION_SCHEMA_VERSION
    }

    /// Returns the exact generated-proposal binding.
    #[must_use]
    pub const fn binding(&self) -> InvestmentProjectionBinding {
        self.binding
    }

    /// Returns the proposal action retained only as immutable research metadata.
    #[must_use]
    pub const fn proposal_action(&self) -> RecommendationAction {
        self.proposal_action
    }

    /// Returns the analysis-only, no-mutation, no-execution marker.
    #[must_use]
    pub const fn authority(&self) -> InvestmentProjectionAuthority {
        self.authority
    }

    /// Returns every exact input consumed by this calculation.
    #[must_use]
    pub const fn inputs(&self) -> &InvestmentSizingInputs {
        &self.inputs
    }

    /// Returns exact selected-mark notional for one execution lot.
    #[must_use]
    pub const fn per_lot_notional(&self) -> Money {
        self.per_lot_notional
    }

    /// Returns gross loss at the downside range's lower endpoint for one execution lot.
    #[must_use]
    pub const fn per_lot_downside_loss(&self) -> Money {
        self.per_lot_downside_loss
    }

    /// Returns all local and external caps in canonical order.
    #[must_use]
    pub fn constraint_caps(&self) -> &[SizingConstraintCap] {
        self.constraint_caps.as_ref()
    }

    /// Returns the exact hard-feasible target-lot range, or every fail-closed reason.
    #[must_use]
    pub const fn hard_feasible_lots(&self) -> &FeasibleLotRangeAvailability {
        &self.hard_feasible_lots
    }

    /// Returns the hard-feasible range intersected with the preferred-weight lot interval.
    #[must_use]
    pub const fn preferred_feasible_lots(&self) -> &FeasibleLotRangeAvailability {
        &self.preferred_feasible_lots
    }

    /// Returns exact selected-mark target-position notionals for the hard-feasible lot range.
    #[must_use]
    pub const fn hard_feasible_target_notional(&self) -> &FeasibleNotionalRangeAvailability {
        &self.hard_feasible_target_notional
    }

    /// Returns exact selected-mark target-position notionals for the preferred-feasible lot range.
    #[must_use]
    pub const fn preferred_feasible_target_notional(&self) -> &FeasibleNotionalRangeAvailability {
        &self.preferred_feasible_target_notional
    }

    /// Returns the exact preferred-weight lot-rounding remainders.
    #[must_use]
    pub const fn preferred_weight_rounding(&self) -> PreferredWeightRoundingRemainder {
        self.preferred_weight_rounding
    }

    /// Returns every cap touching a hard-feasible range boundary.
    #[must_use]
    pub fn hard_binding_caps(&self) -> &[SizingConstraintKind] {
        self.hard_binding_caps.as_ref()
    }

    /// Returns every cap touching a preferred-feasible range boundary.
    #[must_use]
    pub fn preferred_binding_caps(&self) -> &[SizingConstraintKind] {
        self.preferred_binding_caps.as_ref()
    }

    /// Returns the versioned SHA-256 identity of all inputs and exact outputs.
    #[must_use]
    pub const fn result_digest(&self) -> InvestmentProjectionDigest {
        self.result_digest
    }
}

fn validate_sizing_context(
    proposal: &GeneratedInvestmentProposal,
    inputs: &InvestmentSizingInputs,
) -> Result<(), InvestmentProjectionError> {
    let evidence = proposal.evidence();
    let market = evidence
        .market()
        .ok_or(InvestmentProjectionError::MissingProposalEvidence)?;
    let portfolio_risk = evidence
        .portfolio_risk()
        .ok_or(InvestmentProjectionError::MissingProposalEvidence)?;
    if inputs.evaluated_at < evidence.as_of() || inputs.evaluated_at >= proposal.expires_at() {
        return Err(InvestmentProjectionError::InvalidTimeOrder);
    }
    if inputs.selected_mark != market.price() {
        return Err(InvestmentProjectionError::SelectedMarkMismatch);
    }
    if evidence.instrument_id() != market.instrument_id()
        || evidence.instrument_id() != portfolio_risk.instrument_id()
    {
        return Err(InvestmentProjectionError::InstrumentMismatch);
    }
    if evidence.currency() != inputs.selected_mark.currency()
        || evidence.currency() != portfolio_risk.currency()
        || evidence.currency() != inputs.portfolio.marked_equity_at_selected_mark.currency()
        || evidence.currency() != inputs.portfolio.settlement_available_cash.currency()
        || evidence.currency() != inputs.constraints.minimum_cash_reserve.currency()
    {
        return Err(InvestmentProjectionError::CurrencyMismatch);
    }
    if evidence.account_id() != inputs.portfolio.account_id
        || portfolio_risk.account_id() != inputs.portfolio.account_id
        || portfolio_risk.portfolio_revision().bytes()
            != inputs.portfolio.portfolio_revision.bytes()
    {
        return Err(InvestmentProjectionError::PortfolioStateMismatch);
    }
    if evidence.instrument_id() != inputs.portfolio.instrument_id {
        return Err(InvestmentProjectionError::InstrumentMismatch);
    }
    match portfolio_risk.position_state() {
        PortfolioPositionState::NoPosition if inputs.portfolio.current_lots.get() != 0 => {
            return Err(InvestmentProjectionError::PortfolioStateMismatch);
        }
        PortfolioPositionState::Position { .. } if inputs.portfolio.current_lots.get() == 0 => {
            return Err(InvestmentProjectionError::PortfolioStateMismatch);
        }
        PortfolioPositionState::NoPosition | PortfolioPositionState::Position { .. } => {}
    }
    let ladder = proposal.price_ladder();
    ensure_execution_terms(
        inputs.execution_terms,
        evidence.instrument_id(),
        evidence.currency(),
        inputs.selected_mark,
        [
            ladder.downside_range(),
            ladder.base_range(),
            ladder.upside_range(),
            ladder.entry_range(),
            ladder.add_range(),
            ladder.trim_range(),
            ladder.exit_range(),
        ],
    )
}

fn money_for_lots(
    price: Money,
    terms: InstrumentExecutionTerms,
    lots: QuantityLots,
) -> Result<Money, InvestmentProjectionError> {
    let quantity = lots
        .checked_to_decimal(terms.lot_size())
        .map_err(|_| InvestmentProjectionError::ArithmeticOverflow)?;
    price
        .checked_mul_decimal(quantity)
        .and_then(|value| value.checked_mul_decimal(terms.contract_multiplier()))
        .map_err(map_financial_error)
}

fn per_lot_downside_loss(
    mark: Money,
    downside_range: TargetPriceRange,
    terms: InstrumentExecutionTerms,
) -> Result<Money, InvestmentProjectionError> {
    let per_unit_loss = if mark.amount() > downside_range.lower().amount() {
        mark.checked_sub(downside_range.lower())
            .map_err(map_financial_error)?
    } else {
        Money::new(Decimal::ZERO, mark.currency())
    };
    let one_lot = QuantityLots::new(1)
        .map_err(|_| InvestmentProjectionError::ArithmeticOverflow)?
        .checked_to_decimal(terms.lot_size())
        .map_err(|_| InvestmentProjectionError::ArithmeticOverflow)?;
    per_unit_loss
        .checked_mul_decimal(one_lot)
        .and_then(|value| value.checked_mul_decimal(terms.contract_multiplier()))
        .map_err(map_financial_error)
}

fn cash_reserve_cap(
    inputs: &InvestmentSizingInputs,
    per_lot_notional: Money,
) -> Result<SizingConstraintCap, InvestmentProjectionError> {
    let current_notional = money_for_lots(
        inputs.selected_mark,
        inputs.execution_terms,
        inputs.portfolio.current_lots,
    )?;
    let gross_liquidatable_value = current_notional
        .checked_add(inputs.portfolio.settlement_available_cash)
        .and_then(|value| value.checked_sub(inputs.constraints.minimum_cash_reserve))
        .map_err(map_financial_error)?;
    if gross_liquidatable_value.amount() < Decimal::ZERO {
        return Ok(SizingConstraintCap::Unavailable {
            kind: SizingConstraintKind::CashReserve,
            reason: SizingUnavailableReason::CashReserveExceedsGrossLiquidatableValue,
        });
    }
    let upper = floor_lots_for_money(gross_liquidatable_value, per_lot_notional)?;
    Ok(available_local_cap(
        SizingConstraintKind::CashReserve,
        zero_to(upper)?,
    ))
}

fn downside_loss_cap(
    inputs: &InvestmentSizingInputs,
    per_lot_downside_loss: Money,
) -> Result<SizingConstraintCap, InvestmentProjectionError> {
    let loss_budget = inputs
        .portfolio
        .marked_equity_at_selected_mark
        .checked_basis_points(
            BasisPoints::new(i32::from(
                inputs.constraints.maximum_downside_loss_basis_points,
            )),
            Decimal::MAX_SCALE,
            RoundingPolicy::Floor,
        )
        .map_err(map_financial_error)?;
    let upper = if per_lot_downside_loss.amount() == Decimal::ZERO {
        QuantityLots::new(i64::MAX).map_err(|_| InvestmentProjectionError::ArithmeticOverflow)?
    } else {
        floor_lots_for_money(loss_budget, per_lot_downside_loss)?
    };
    Ok(available_local_cap(
        SizingConstraintKind::DownsideLoss,
        zero_to(upper)?,
    ))
}

fn preferred_weight_cap(
    inputs: &InvestmentSizingInputs,
    per_lot_notional: Money,
) -> Result<(SizingConstraintCap, PreferredWeightRoundingRemainder), InvestmentProjectionError> {
    let lower_notional = inputs
        .portfolio
        .marked_equity_at_selected_mark
        .checked_basis_points(
            BasisPoints::new(i32::from(
                inputs.constraints.preferred_weight_lower_basis_points,
            )),
            Decimal::MAX_SCALE,
            RoundingPolicy::Ceiling,
        )
        .map_err(map_financial_error)?;
    let upper_notional = inputs
        .portfolio
        .marked_equity_at_selected_mark
        .checked_basis_points(
            BasisPoints::new(i32::from(
                inputs.constraints.preferred_weight_upper_basis_points,
            )),
            Decimal::MAX_SCALE,
            RoundingPolicy::Floor,
        )
        .map_err(map_financial_error)?;
    let lower_lots = ceil_lots_for_money(lower_notional, per_lot_notional)?;
    let upper_lots = floor_lots_for_money(upper_notional, per_lot_notional)?;
    let lower_lot_notional = multiply_per_lot(per_lot_notional, lower_lots)?;
    let upper_lot_notional = multiply_per_lot(per_lot_notional, upper_lots)?;
    let rounding = PreferredWeightRoundingRemainder {
        lower_round_up_excess: lower_lot_notional
            .checked_sub(lower_notional)
            .map_err(map_financial_error)?,
        upper_round_down_remainder: upper_notional
            .checked_sub(upper_lot_notional)
            .map_err(map_financial_error)?,
    };
    if lower_lots > upper_lots {
        return Ok((
            SizingConstraintCap::Unavailable {
                kind: SizingConstraintKind::PreferredWeight,
                reason: SizingUnavailableReason::PreferredWeightRangeContainsNoLots,
            },
            rounding,
        ));
    }
    Ok((
        available_local_cap(
            SizingConstraintKind::PreferredWeight,
            LotRange::try_new(lower_lots, upper_lots)?,
        ),
        rounding,
    ))
}

fn capacity_cap(
    kind: SizingConstraintKind,
    availability: &SizingCapacityAvailability,
    inputs: &InvestmentSizingInputs,
    per_lot_notional: Money,
) -> Result<SizingConstraintCap, InvestmentProjectionError> {
    let evidence = match availability {
        SizingCapacityAvailability::Available(evidence) => evidence.as_ref(),
        SizingCapacityAvailability::UnavailableNotSupplied => {
            return Ok(SizingConstraintCap::Unavailable {
                kind,
                reason: SizingUnavailableReason::CapacityNotSupplied(kind),
            });
        }
    };
    validate_capacity_binding(evidence, inputs)?;
    if inputs.evaluated_at < evidence.available_at {
        return Ok(SizingConstraintCap::Unavailable {
            kind,
            reason: SizingUnavailableReason::CapacityNotYetAvailable(kind),
        });
    }
    if inputs.evaluated_at >= evidence.expires_at {
        return Ok(SizingConstraintCap::Unavailable {
            kind,
            reason: SizingUnavailableReason::CapacityExpired(kind),
        });
    }
    let lot_range = match evidence.range {
        CapacityRange::Lots(range) => Some(range),
        CapacityRange::Notional(range) => lot_range_for_notional(range, per_lot_notional)?,
    };
    match lot_range {
        Some(lot_range) => Ok(SizingConstraintCap::Available {
            kind,
            lot_range,
            capacity_identity: Some(evidence.content_identity),
        }),
        None => Ok(SizingConstraintCap::Unavailable {
            kind,
            reason: SizingUnavailableReason::CapacityRangeContainsNoLots(kind),
        }),
    }
}

fn validate_capacity_binding(
    evidence: &SizingCapacityEvidence,
    inputs: &InvestmentSizingInputs,
) -> Result<(), InvestmentProjectionError> {
    if evidence.instrument_id != inputs.execution_terms.instrument_id() {
        return Err(InvestmentProjectionError::CapacityBindingMismatch);
    }
    if evidence.account_id != inputs.portfolio.account_id
        || evidence.portfolio_revision.bytes() != inputs.portfolio.portfolio_revision.bytes()
        || evidence.definition_revision != inputs.execution_terms.definition_revision()
        || evidence.reference_mark != inputs.selected_mark
    {
        return Err(InvestmentProjectionError::CapacityBindingMismatch);
    }
    if evidence.reference_mark.currency() != inputs.selected_mark.currency() {
        return Err(InvestmentProjectionError::CurrencyMismatch);
    }
    if let CapacityRange::Notional(range) = evidence.range {
        if range.lower.currency() != inputs.selected_mark.currency() {
            return Err(InvestmentProjectionError::CurrencyMismatch);
        }
    }
    Ok(())
}

fn lot_range_for_notional(
    range: NonnegativeMoneyRange,
    per_lot_notional: Money,
) -> Result<Option<LotRange>, InvestmentProjectionError> {
    let lower = ceil_lots_for_money(range.lower, per_lot_notional)?;
    let upper = floor_lots_for_money(range.upper, per_lot_notional)?;
    if lower > upper {
        Ok(None)
    } else {
        LotRange::try_new(lower, upper).map(Some)
    }
}

fn floor_lots_for_money(
    amount: Money,
    per_lot_notional: Money,
) -> Result<QuantityLots, InvestmentProjectionError> {
    ensure_division_values(amount, per_lot_notional)?;
    let mut lower = 0_i64;
    let mut upper = i64::MAX;
    while lower < upper {
        let midpoint = i64::try_from((i128::from(lower) + i128::from(upper) + 1) / 2)
            .map_err(|_| InvestmentProjectionError::ArithmeticOverflow)?;
        if compare_lot_notional(midpoint, per_lot_notional, amount)? == Ordering::Greater {
            upper = midpoint - 1;
        } else {
            lower = midpoint;
        }
    }
    QuantityLots::new(lower).map_err(|_| InvestmentProjectionError::ArithmeticOverflow)
}

fn ceil_lots_for_money(
    amount: Money,
    per_lot_notional: Money,
) -> Result<QuantityLots, InvestmentProjectionError> {
    let floor = floor_lots_for_money(amount, per_lot_notional)?;
    if compare_lot_notional(floor.get(), per_lot_notional, amount)? == Ordering::Equal {
        return Ok(floor);
    }
    floor
        .get()
        .checked_add(1)
        .ok_or(InvestmentProjectionError::ArithmeticOverflow)
        .and_then(|value| {
            QuantityLots::new(value).map_err(|_| InvestmentProjectionError::ArithmeticOverflow)
        })
}

fn ensure_division_values(
    amount: Money,
    per_lot_notional: Money,
) -> Result<(), InvestmentProjectionError> {
    if amount.currency() != per_lot_notional.currency() {
        return Err(InvestmentProjectionError::CurrencyMismatch);
    }
    if amount.amount() < Decimal::ZERO || per_lot_notional.amount() <= Decimal::ZERO {
        return Err(InvestmentProjectionError::InvalidFinancialValue);
    }
    Ok(())
}

fn compare_lot_notional(
    lots: i64,
    per_lot_notional: Money,
    amount: Money,
) -> Result<Ordering, InvestmentProjectionError> {
    if lots < 0 {
        return Err(InvestmentProjectionError::InvalidFinancialValue);
    }
    if per_lot_notional.currency() != amount.currency() {
        return Err(InvestmentProjectionError::CurrencyMismatch);
    }
    match per_lot_notional.checked_mul_decimal(Decimal::from(lots)) {
        Ok(notional) => Ok(notional.amount().cmp(&amount.amount())),
        Err(market_squawk_domain::FinancialError::Overflow) => Ok(Ordering::Greater),
        Err(error) => Err(map_financial_error(error)),
    }
}

fn multiply_per_lot(
    per_lot_notional: Money,
    lots: QuantityLots,
) -> Result<Money, InvestmentProjectionError> {
    per_lot_notional
        .checked_mul_decimal(Decimal::from(lots.get()))
        .map_err(map_financial_error)
}

fn zero_to(upper: QuantityLots) -> Result<LotRange, InvestmentProjectionError> {
    LotRange::try_new(
        QuantityLots::new(0).map_err(|_| InvestmentProjectionError::ArithmeticOverflow)?,
        upper,
    )
}

const fn available_local_cap(
    kind: SizingConstraintKind,
    lot_range: LotRange,
) -> SizingConstraintCap {
    SizingConstraintCap::Available {
        kind,
        lot_range,
        capacity_identity: None,
    }
}

fn hard_feasible_range(
    caps: &[SizingConstraintCap],
) -> Result<FeasibleLotRangeAvailability, InvestmentProjectionError> {
    let mut intersection = Some(LotRange::try_new(
        QuantityLots::new(0).map_err(|_| InvestmentProjectionError::ArithmeticOverflow)?,
        QuantityLots::new(i64::MAX).map_err(|_| InvestmentProjectionError::ArithmeticOverflow)?,
    )?);
    let mut reasons = Vec::new();
    for cap in caps {
        match *cap {
            SizingConstraintCap::Available { lot_range, .. } => {
                intersection = intersection.and_then(|current| intersect(current, lot_range));
            }
            SizingConstraintCap::Unavailable { reason, .. } => push_unique(&mut reasons, reason),
        }
    }
    if intersection.is_none() {
        push_unique(
            &mut reasons,
            SizingUnavailableReason::NoHardFeasibleLotIntersection,
        );
    }
    if reasons.is_empty() {
        match intersection {
            Some(range) => Ok(FeasibleLotRangeAvailability::Available(range)),
            None => Err(InvestmentProjectionError::InvalidFinancialValue),
        }
    } else {
        Ok(FeasibleLotRangeAvailability::Unavailable(
            reasons.into_boxed_slice(),
        ))
    }
}

fn preferred_feasible_range(
    hard: &FeasibleLotRangeAvailability,
    preferred_cap: SizingConstraintCap,
) -> FeasibleLotRangeAvailability {
    let mut reasons = match hard {
        FeasibleLotRangeAvailability::Available(_) => Vec::new(),
        FeasibleLotRangeAvailability::Unavailable(reasons) => reasons.as_ref().to_vec(),
    };
    let preferred = match preferred_cap {
        SizingConstraintCap::Available { lot_range, .. } => Some(lot_range),
        SizingConstraintCap::Unavailable { reason, .. } => {
            push_unique(&mut reasons, reason);
            None
        }
    };
    let intersection = match (available_range(hard), preferred) {
        (Some(hard), Some(preferred)) => intersect(hard, preferred),
        _ => None,
    };
    if reasons.is_empty() && intersection.is_none() {
        reasons.push(SizingUnavailableReason::NoPreferredFeasibleLotIntersection);
    }
    match (reasons.is_empty(), intersection) {
        (true, Some(range)) => FeasibleLotRangeAvailability::Available(range),
        _ => FeasibleLotRangeAvailability::Unavailable(reasons.into_boxed_slice()),
    }
}

fn target_notional_availability(
    lots: &FeasibleLotRangeAvailability,
    per_lot_notional: Money,
) -> Result<FeasibleNotionalRangeAvailability, InvestmentProjectionError> {
    match lots {
        FeasibleLotRangeAvailability::Available(range) => Ok(
            FeasibleNotionalRangeAvailability::Available(NonnegativeMoneyRange::try_new(
                multiply_per_lot(per_lot_notional, range.lower)?,
                multiply_per_lot(per_lot_notional, range.upper)?,
            )?),
        ),
        FeasibleLotRangeAvailability::Unavailable(reasons) => Ok(
            FeasibleNotionalRangeAvailability::Unavailable(reasons.clone()),
        ),
    }
}

const fn intersect(left: LotRange, right: LotRange) -> Option<LotRange> {
    let lower = if left.lower.get() >= right.lower.get() {
        left.lower
    } else {
        right.lower
    };
    let upper = if left.upper.get() <= right.upper.get() {
        left.upper
    } else {
        right.upper
    };
    if lower.get() <= upper.get() {
        Some(LotRange { lower, upper })
    } else {
        None
    }
}

const fn available_range(availability: &FeasibleLotRangeAvailability) -> Option<LotRange> {
    match availability {
        FeasibleLotRangeAvailability::Available(range) => Some(*range),
        FeasibleLotRangeAvailability::Unavailable(_) => None,
    }
}

fn binding_caps(
    caps: &[SizingConstraintCap],
    range: Option<LotRange>,
) -> Box<[SizingConstraintKind]> {
    let Some(range) = range else {
        return Box::new([]);
    };
    caps.iter()
        .filter_map(|cap| match *cap {
            SizingConstraintCap::Available {
                kind, lot_range, ..
            } if lot_range.lower == range.lower || lot_range.upper == range.upper => Some(kind),
            SizingConstraintCap::Available { .. } | SizingConstraintCap::Unavailable { .. } => None,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn push_unique(reasons: &mut Vec<SizingUnavailableReason>, reason: SizingUnavailableReason) {
    if !reasons.contains(&reason) {
        reasons.push(reason);
    }
}
