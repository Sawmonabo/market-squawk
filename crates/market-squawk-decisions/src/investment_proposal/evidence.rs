//! Immutable evidence supplied to the pure recommendation authority.

use std::num::NonZeroU32;

use market_squawk_domain::{
    AccountId, BasisPoints, Currency, DataQuality, DigestAlgorithm, FairValueHierarchy,
    InstrumentId, Money, Timestamp,
};
use market_squawk_modeling::ForecastCentralStatistic;
use market_squawk_portfolio::PortfolioRevisionToken;
use market_squawk_valuation::{
    ApprovalStatus, DecisionId, FairValueSelectionDisposition, FairValueSelectionReceipt,
    FairValueSelectionReceiptHash, MeasurementId, ValuationAmountBasis,
};

use crate::{
    DecisionContentDigest, SelectedCandidateAnalysisEvidence, TargetPriceCases, TargetPriceRange,
};

use super::{CONFIDENCE_PARTS_PER_MILLION, InvestmentProposalError, ProposalForecastVintageId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProposalEvidenceWindow {
    pub(super) observed_at: Timestamp,
    pub(super) available_at: Timestamp,
    pub(super) expires_at: Timestamp,
    pub(super) content_identity: DecisionContentDigest,
}

impl ProposalEvidenceWindow {
    /// Constructs a point-in-time evidence window.
    ///
    /// # Errors
    ///
    /// Requires `observed_at <= available_at < expires_at`.
    pub fn try_new(
        observed_at: Timestamp,
        available_at: Timestamp,
        expires_at: Timestamp,
        content_identity: DecisionContentDigest,
    ) -> Result<Self, InvestmentProposalError> {
        if observed_at > available_at || available_at >= expires_at {
            return Err(InvestmentProposalError::InvalidTimeOrder);
        }
        Ok(Self {
            observed_at,
            available_at,
            expires_at,
            content_identity,
        })
    }

    /// Returns when the underlying fact was observed.
    #[must_use]
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    /// Returns when this exact evidence became knowable to the analysis.
    #[must_use]
    pub const fn available_at(self) -> Timestamp {
        self.available_at
    }

    /// Returns the exclusive evidence expiry.
    #[must_use]
    pub const fn expires_at(self) -> Timestamp {
        self.expires_at
    }

    /// Returns the exact evidence payload identity.
    #[must_use]
    pub const fn content_identity(self) -> DecisionContentDigest {
        self.content_identity
    }
}

/// Exact selector semantic used for the proposal start mark.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MarketReferencePriceKind {
    /// Exact last-trade observation selected at the point-in-time cutoff.
    LastTrade,
    /// Bid/ask midpoint admitted only after the selector's crossed/stale checks.
    CheckedBidAskMidpoint,
}

/// Adjustment semantics applied to the selected proposal start mark.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MarketReferenceAdjustmentBasis {
    /// Exact observed spot value without a favorable after-the-fact adjustment.
    UnadjustedSpot,
}

/// Current exact price evidence used as the proposal reference mark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketReferenceEvidence {
    pub(super) instrument_id: InstrumentId,
    pub(super) price: Money,
    pub(super) quality: DataQuality,
    pub(super) price_kind: MarketReferencePriceKind,
    pub(super) adjustment_basis: MarketReferenceAdjustmentBasis,
    pub(super) selection_receipt_identity: DecisionContentDigest,
    pub(super) selected_observation_identity: DecisionContentDigest,
    pub(super) window: ProposalEvidenceWindow,
}

impl MarketReferenceEvidence {
    /// Constructs positive current-market evidence.
    ///
    /// # Errors
    ///
    /// Rejects a zero or negative price. Quality admission remains policy-owned so quarantined or
    /// stale evidence can produce a typed unavailable result.
    pub fn try_new(
        instrument_id: InstrumentId,
        price: Money,
        quality: DataQuality,
        price_kind: MarketReferencePriceKind,
        adjustment_basis: MarketReferenceAdjustmentBasis,
        selection_receipt_identity: DecisionContentDigest,
        selected_observation_identity: DecisionContentDigest,
        window: ProposalEvidenceWindow,
    ) -> Result<Self, InvestmentProposalError> {
        ensure_positive(price)?;
        Ok(Self {
            instrument_id,
            price,
            quality,
            price_kind,
            adjustment_basis,
            selection_receipt_identity,
            selected_observation_identity,
            window,
        })
    }

    /// Returns the stable instrument identity.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact reference price and currency.
    #[must_use]
    pub const fn price(self) -> Money {
        self.price
    }

    /// Returns the source-qualified market quality.
    #[must_use]
    pub const fn quality(self) -> DataQuality {
        self.quality
    }

    /// Returns the exact selector semantic used for the start mark.
    #[must_use]
    pub const fn price_kind(self) -> MarketReferencePriceKind {
        self.price_kind
    }

    /// Returns the explicit start-mark adjustment basis.
    #[must_use]
    pub const fn adjustment_basis(self) -> MarketReferenceAdjustmentBasis {
        self.adjustment_basis
    }

    /// Returns the exact resolver receipt that selected this market observation.
    #[must_use]
    pub const fn selection_receipt_identity(self) -> DecisionContentDigest {
        self.selection_receipt_identity
    }

    /// Returns the identity of the exact selected source observation.
    #[must_use]
    pub const fn selected_observation_identity(self) -> DecisionContentDigest {
        self.selected_observation_identity
    }

    /// Returns exact market evidence timing and identity.
    #[must_use]
    pub const fn window(self) -> ProposalEvidenceWindow {
        self.window
    }
}

/// Calibrated downside, base, and upside forecast intervals.
///
/// These are empirical coverage intervals, not a probability distribution and not by themselves
/// an expected terminal value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForecastPriceRanges {
    pub(super) downside: TargetPriceRange,
    pub(super) base: TargetPriceRange,
    pub(super) upside: TargetPriceRange,
}

impl ForecastPriceRanges {
    /// Constructs three positive, same-currency, strictly separated forecast intervals.
    ///
    /// # Errors
    ///
    /// Requires `downside.upper < base.lower` and `base.upper < upside.lower`.
    pub fn try_new(
        downside: TargetPriceRange,
        base: TargetPriceRange,
        upside: TargetPriceRange,
    ) -> Result<Self, InvestmentProposalError> {
        let currency = downside.lower().currency();
        if [
            downside.upper().currency(),
            base.lower().currency(),
            base.upper().currency(),
            upside.lower().currency(),
            upside.upper().currency(),
        ]
        .into_iter()
        .any(|candidate| candidate != currency)
            || downside.lower().amount().is_zero()
            || downside.lower().amount().is_sign_negative()
            || downside.lower().amount() >= downside.upper().amount()
            || downside.upper().amount() >= base.lower().amount()
            || base.lower().amount() >= base.upper().amount()
            || base.upper().amount() >= upside.lower().amount()
            || upside.lower().amount() >= upside.upper().amount()
        {
            return Err(InvestmentProposalError::InvalidPrice);
        }
        Ok(Self {
            downside,
            base,
            upside,
        })
    }

    /// Returns the calibrated downside interval.
    #[must_use]
    pub const fn downside(self) -> TargetPriceRange {
        self.downside
    }

    /// Returns the calibrated base interval.
    #[must_use]
    pub const fn base(self) -> TargetPriceRange {
        self.base
    }

    /// Returns the calibrated upside interval.
    #[must_use]
    pub const fn upside(self) -> TargetPriceRange {
        self.upside
    }
}

/// Empirical coverage evidence for the exact forecast calibration population.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForecastCalibrationSummary {
    pub(super) nominal_coverage_ppm: u32,
    pub(super) realized_coverage_ppm: u32,
    pub(super) completed_outcomes: NonZeroU32,
}

impl ForecastCalibrationSummary {
    /// Constructs bounded nominal and realized coverage values.
    ///
    /// # Errors
    ///
    /// Rejects coverage above one million parts per million.
    pub fn try_new(
        nominal_coverage_ppm: u32,
        realized_coverage_ppm: u32,
        completed_outcomes: NonZeroU32,
    ) -> Result<Self, InvestmentProposalError> {
        ensure_ppm(nominal_coverage_ppm)?;
        ensure_ppm(realized_coverage_ppm)?;
        Ok(Self {
            nominal_coverage_ppm,
            realized_coverage_ppm,
            completed_outcomes,
        })
    }

    /// Returns the declared interval coverage.
    #[must_use]
    pub const fn nominal_coverage_ppm(self) -> u32 {
        self.nominal_coverage_ppm
    }

    /// Returns realized coverage for the exact outcome population.
    #[must_use]
    pub const fn realized_coverage_ppm(self) -> u32 {
        self.realized_coverage_ppm
    }

    /// Returns the completed, point-in-time outcome count.
    #[must_use]
    pub const fn completed_outcomes(self) -> NonZeroU32 {
        self.completed_outcomes
    }
}

/// Calibrated downside/base/upside price forecast and all identities needed to reproduce it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PriceForecastEvidence {
    pub(super) instrument_id: InstrumentId,
    pub(super) cases: TargetPriceCases,
    pub(super) ranges: ForecastPriceRanges,
    pub(super) horizon_at: Timestamp,
    pub(super) expected_terminal_statistic: Option<ForecastCentralStatistic>,
    pub(super) expected_terminal_price: Option<Money>,
    pub(super) expected_terminal_horizon_at: Option<Timestamp>,
    pub(super) expected_terminal_statistic_identity: Option<DecisionContentDigest>,
    pub(super) vintage_id: ProposalForecastVintageId,
    pub(super) output_binding_identity: DecisionContentDigest,
    pub(super) calibration_identity: DecisionContentDigest,
    pub(super) outcome_set_identity: DecisionContentDigest,
    pub(super) calibration: ForecastCalibrationSummary,
    pub(super) window: ProposalEvidenceWindow,
}

impl PriceForecastEvidence {
    /// Constructs strictly ordered, positive, same-currency forecast cases.
    ///
    /// # Errors
    ///
    /// Rejects nonpositive or non-strict cases and horizons at or before publication. Expected
    /// terminal fields must be either wholly absent or prove one model-estimated conditional mean:
    /// its price must equal the forecast base case exactly, its horizon must equal `horizon_at`,
    /// and its statistic identity must equal the exact model-output binding identity.
    #[allow(
        clippy::too_many_arguments,
        reason = "vintage, output, calibration, outcome, financial, and temporal authorities remain explicit"
    )]
    pub fn try_new(
        instrument_id: InstrumentId,
        cases: TargetPriceCases,
        ranges: ForecastPriceRanges,
        horizon_at: Timestamp,
        expected_terminal_statistic: Option<ForecastCentralStatistic>,
        expected_terminal_price: Option<Money>,
        expected_terminal_horizon_at: Option<Timestamp>,
        expected_terminal_statistic_identity: Option<DecisionContentDigest>,
        vintage_id: ProposalForecastVintageId,
        output_binding_identity: DecisionContentDigest,
        calibration_identity: DecisionContentDigest,
        outcome_set_identity: DecisionContentDigest,
        calibration: ForecastCalibrationSummary,
        window: ProposalEvidenceWindow,
    ) -> Result<Self, InvestmentProposalError> {
        ensure_positive(cases.downside())?;
        if cases.downside().currency() != ranges.downside.lower().currency()
            || cases.downside().amount() >= cases.base().amount()
            || cases.base().amount() >= cases.upside().amount()
            || !(ranges.downside.lower().amount()..=ranges.downside.upper().amount())
                .contains(&cases.downside().amount())
            || !(ranges.base.lower().amount()..=ranges.base.upper().amount())
                .contains(&cases.base().amount())
            || !(ranges.upside.lower().amount()..=ranges.upside.upper().amount())
                .contains(&cases.upside().amount())
        {
            return Err(InvestmentProposalError::InvalidPrice);
        }
        if horizon_at <= window.available_at() {
            return Err(InvestmentProposalError::InvalidTimeOrder);
        }
        match (
            expected_terminal_statistic,
            expected_terminal_price,
            expected_terminal_horizon_at,
            expected_terminal_statistic_identity,
        ) {
            (
                Some(ForecastCentralStatistic::ModelEstimatedConditionalMean),
                Some(price),
                Some(expected_horizon_at),
                Some(statistic_identity),
            ) => {
                ensure_positive(price)?;
                if price != cases.base() || price.currency() != cases.base().currency() {
                    return Err(InvestmentProposalError::InvalidPrice);
                }
                if expected_horizon_at != horizon_at {
                    return Err(InvestmentProposalError::InvalidTimeOrder);
                }
                if statistic_identity != output_binding_identity {
                    return Err(InvestmentProposalError::InvalidEvidenceMetric);
                }
            }
            (None, None, None, None) => {}
            (
                Some(
                    ForecastCentralStatistic::ModelEstimatedConditionalMean
                    | ForecastCentralStatistic::Unavailable,
                ),
                _,
                _,
                _,
            )
            | (None, _, _, _) => {
                return Err(InvestmentProposalError::InvalidEvidenceMetric);
            }
        }
        Ok(Self {
            instrument_id,
            cases,
            ranges,
            horizon_at,
            expected_terminal_statistic,
            expected_terminal_price,
            expected_terminal_horizon_at,
            expected_terminal_statistic_identity,
            vintage_id,
            output_binding_identity,
            calibration_identity,
            outcome_set_identity,
            calibration,
            window,
        })
    }

    /// Returns the stable instrument identity.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact modeled downside, base, and upside cases.
    #[must_use]
    pub const fn cases(self) -> TargetPriceCases {
        self.cases
    }

    /// Returns calibrated downside, base, and upside intervals.
    #[must_use]
    pub const fn ranges(self) -> ForecastPriceRanges {
        self.ranges
    }

    /// Returns the forecast target horizon.
    #[must_use]
    pub const fn horizon_at(self) -> Timestamp {
        self.horizon_at
    }

    /// Returns the admitted central statistic for the expected terminal price, when supplied.
    #[must_use]
    pub const fn expected_terminal_statistic(self) -> Option<ForecastCentralStatistic> {
        self.expected_terminal_statistic
    }

    /// Returns the exact positive conditional-mean terminal price, when admitted.
    #[must_use]
    pub const fn expected_terminal_price(self) -> Option<Money> {
        self.expected_terminal_price
    }

    /// Returns the exact terminal horizon bound to the admitted conditional mean.
    #[must_use]
    pub const fn expected_terminal_horizon_at(self) -> Option<Timestamp> {
        self.expected_terminal_horizon_at
    }

    /// Returns the exact output-binding identity proving the terminal statistic semantics.
    #[must_use]
    pub const fn expected_terminal_statistic_identity(self) -> Option<DecisionContentDigest> {
        self.expected_terminal_statistic_identity
    }

    /// Returns the immutable forecast vintage identity.
    #[must_use]
    pub const fn vintage_id(self) -> ProposalForecastVintageId {
        self.vintage_id
    }

    /// Returns the exact model-output semantic binding.
    #[must_use]
    pub const fn output_binding_identity(self) -> DecisionContentDigest {
        self.output_binding_identity
    }

    /// Returns the exact calibration artifact identity.
    #[must_use]
    pub const fn calibration_identity(self) -> DecisionContentDigest {
        self.calibration_identity
    }

    /// Returns the exact completed-outcome population identity.
    #[must_use]
    pub const fn outcome_set_identity(self) -> DecisionContentDigest {
        self.outcome_set_identity
    }

    /// Returns empirical coverage evidence.
    #[must_use]
    pub const fn calibration(self) -> ForecastCalibrationSummary {
        self.calibration
    }

    /// Returns exact forecast evidence timing and payload identity.
    #[must_use]
    pub const fn window(self) -> ProposalEvidenceWindow {
        self.window
    }
}

/// Governed fair-value evidence consumed independently of the forecast.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValuationEvidence {
    pub(super) instrument_id: InstrumentId,
    pub(super) fair_value: Money,
    pub(super) basis: ValuationAmountBasis,
    pub(super) horizon_at: Timestamp,
    pub(super) measurement_id: MeasurementId,
    pub(super) classification_decision_id: DecisionId,
    pub(super) selection_receipt_hash: FairValueSelectionReceiptHash,
    pub(super) window: ProposalEvidenceWindow,
}

impl ValuationEvidence {
    /// Derives governed fair-value evidence from one exact completed selection receipt.
    ///
    /// # Errors
    ///
    /// Rejects an incomplete/unselected receipt, an account/instrument/currency mismatch, a
    /// detached or unclassified selected chain, a nonpositive fair value, or a supplied evidence
    /// window that does not exactly describe the receipt. The required window uses the selected
    /// measurement time as `observed_at`, the receipt request cutoff as `available_at`, the active
    /// approval expiry as `expires_at`, and the receipt's SHA-256 hash as `content_identity`.
    pub fn try_from_fair_value_selection(
        receipt: &FairValueSelectionReceipt,
        account_id: AccountId,
        instrument_id: InstrumentId,
        currency: Currency,
        horizon_at: Timestamp,
        window: ProposalEvidenceWindow,
    ) -> Result<Self, InvestmentProposalError> {
        let request = receipt.request();
        let selected = receipt
            .selected()
            .ok_or(InvestmentProposalError::InvalidValuationSelection)?;
        let measurement = selected.measurement();
        let classification = selected.classification();
        let approval = selected.approval();
        let leading = receipt
            .eligible_order()
            .first()
            .ok_or(InvestmentProposalError::InvalidValuationSelection)?;
        let fair_value = measurement.amount().money();

        if receipt.disposition() != FairValueSelectionDisposition::Complete
            || request.account_id() != Some(account_id)
            || request.instrument_id() != instrument_id
            || request.currency() != currency
            || measurement.account_id() != account_id
            || measurement.instrument_id() != instrument_id
            || fair_value.currency() != currency
            || request.basis() != ValuationAmountBasis::PerInstrumentUnit
            || measurement.amount_basis() != ValuationAmountBasis::PerInstrumentUnit
            || classification.hierarchy() == FairValueHierarchy::Unclassified
            || selected.approval_status() != ApprovalStatus::Active
            || selected.applicable_revocation().is_some()
            || classification.measurement_id() != measurement.id()
            || approval.measurement_id() != measurement.id()
            || approval.decision_id() != classification.id()
            || classification.evidence_hash() != measurement.evidence_hash()
            || selected.evidence_hash() != measurement.evidence_hash()
            || leading.rank() != 1
            || leading.measurement_id() != measurement.id()
            || leading.decision_id() != classification.id()
            || leading.approval_id() != approval.id()
            || leading.measurement_at() != measurement.measurement_at()
            || leading.prepared_at() != measurement.prepared_at()
            || leading.classification_recorded_at() != selected.classification_recorded_at()
            || leading.approved_at() != approval.approved_at()
            || leading.approval_recorded_at() != selected.approval_recorded_at()
            || leading.expires_at() != selected.expires_at()
            || leading.hierarchy() != classification.hierarchy()
            || leading.ruleset_version() != classification.ruleset_version()
            || leading.ruleset_hash() != classification.ruleset_hash()
            || leading.evidence_hash() != selected.evidence_hash()
        {
            return Err(InvestmentProposalError::InvalidValuationSelection);
        }
        let receipt_identity = window.content_identity().evidence_digest();
        if receipt_identity.algorithm() != DigestAlgorithm::Sha256
            || receipt_identity.bytes() != receipt.hash().bytes()
        {
            return Err(InvestmentProposalError::InvalidValuationSelection);
        }
        if window.observed_at() != measurement.measurement_at()
            || window.available_at() != request.as_of()
            || window.expires_at() != selected.expires_at()
            || measurement.prepared_at() > request.as_of()
            || selected.classification_recorded_at() > request.as_of()
            || approval.approved_at() > request.as_of()
            || selected.approval_recorded_at() > request.as_of()
            || selected.expires_at() <= request.as_of()
            || horizon_at <= request.as_of()
        {
            return Err(InvestmentProposalError::InvalidTimeOrder);
        }
        Self::try_from_parts(
            instrument_id,
            fair_value,
            measurement.amount_basis(),
            horizon_at,
            measurement.id(),
            classification.id(),
            receipt.hash(),
            window,
        )
    }

    /// Recovers the fixed receipt-bound projection retained by a durable investment analysis.
    ///
    /// Ordinary production admission must use [`Self::try_from_fair_value_selection`]. This
    /// recovery boundary cannot reproduce the selection receipt after the proposal has retained
    /// only its immutable projection, so it instead revalidates the exact SHA-256 binding between
    /// the retained receipt hash and evidence window before the complete recommendation authority
    /// regenerates and verifies the persisted analysis and proposal identities.
    ///
    /// # Errors
    ///
    /// Rejects a detached receipt hash, a non-SHA-256 window identity, a nonpositive fair value, or
    /// an invalid horizon.
    #[allow(
        clippy::too_many_arguments,
        reason = "every retained receipt-bound valuation projection field remains explicit"
    )]
    pub fn try_recover_receipt_bound_projection(
        instrument_id: InstrumentId,
        fair_value: Money,
        basis: ValuationAmountBasis,
        horizon_at: Timestamp,
        measurement_id: MeasurementId,
        classification_decision_id: DecisionId,
        selection_receipt_hash: FairValueSelectionReceiptHash,
        window: ProposalEvidenceWindow,
    ) -> Result<Self, InvestmentProposalError> {
        let receipt_identity = window.content_identity().evidence_digest();
        if receipt_identity.algorithm() != DigestAlgorithm::Sha256
            || receipt_identity.bytes() != selection_receipt_hash.bytes()
        {
            return Err(InvestmentProposalError::InvalidValuationSelection);
        }
        Self::try_from_parts(
            instrument_id,
            fair_value,
            basis,
            horizon_at,
            measurement_id,
            classification_decision_id,
            selection_receipt_hash,
            window,
        )
    }

    #[cfg(test)]
    pub(crate) fn try_new_for_test(
        instrument_id: InstrumentId,
        fair_value: Money,
        basis: ValuationAmountBasis,
        horizon_at: Timestamp,
        measurement_id: MeasurementId,
        classification_decision_id: DecisionId,
        selection_receipt_hash: FairValueSelectionReceiptHash,
        window: ProposalEvidenceWindow,
    ) -> Result<Self, InvestmentProposalError> {
        Self::try_from_parts(
            instrument_id,
            fair_value,
            basis,
            horizon_at,
            measurement_id,
            classification_decision_id,
            selection_receipt_hash,
            window,
        )
    }

    fn try_from_parts(
        instrument_id: InstrumentId,
        fair_value: Money,
        basis: ValuationAmountBasis,
        horizon_at: Timestamp,
        measurement_id: MeasurementId,
        classification_decision_id: DecisionId,
        selection_receipt_hash: FairValueSelectionReceiptHash,
        window: ProposalEvidenceWindow,
    ) -> Result<Self, InvestmentProposalError> {
        ensure_positive(fair_value)?;
        if basis != ValuationAmountBasis::PerInstrumentUnit {
            return Err(InvestmentProposalError::InvalidValuationSelection);
        }
        if horizon_at <= window.available_at() {
            return Err(InvestmentProposalError::InvalidTimeOrder);
        }
        Ok(Self {
            instrument_id,
            fair_value,
            basis,
            horizon_at,
            measurement_id,
            classification_decision_id,
            selection_receipt_hash,
            window,
        })
    }

    /// Returns the stable instrument identity.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact fair-value amount and currency.
    #[must_use]
    pub const fn fair_value(self) -> Money {
        self.fair_value
    }

    /// Returns the required per-instrument-unit basis of the governed fair value.
    #[must_use]
    pub const fn basis(self) -> ValuationAmountBasis {
        self.basis
    }

    /// Returns the valuation horizon required for comparison with the price forecast.
    #[must_use]
    pub const fn horizon_at(self) -> Timestamp {
        self.horizon_at
    }

    /// Returns the governed measurement identity.
    #[must_use]
    pub const fn measurement_id(self) -> MeasurementId {
        self.measurement_id
    }

    /// Returns the valuation-classification identity.
    #[must_use]
    pub const fn classification_decision_id(self) -> DecisionId {
        self.classification_decision_id
    }

    /// Returns the deterministic valuation-selection identity.
    #[must_use]
    pub const fn selection_receipt_hash(self) -> FairValueSelectionReceiptHash {
        self.selection_receipt_hash
    }

    /// Returns exact valuation evidence timing and payload identity.
    #[must_use]
    pub const fn window(self) -> ProposalEvidenceWindow {
        self.window
    }
}

/// Cost-adjusted point-in-time backtest evidence and its complete reproducibility bindings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CostAdjustedPitBacktestEvidence {
    pub(super) instrument_id: InstrumentId,
    pub(super) currency: Currency,
    pub(super) outcome_horizon_nanos: i64,
    pub(super) net_return: BasisPoints,
    pub(super) max_drawdown: BasisPoints,
    pub(super) fee_basis_points: BasisPoints,
    pub(super) slippage_basis_points: BasisPoints,
    pub(super) maximum_random_slippage_basis_points: BasisPoints,
    pub(super) observations: NonZeroU32,
    pub(super) trials: NonZeroU32,
    pub(super) stability_ppm: u32,
    pub(super) simulation_cutoff_at: Timestamp,
    pub(super) dataset_identity: DecisionContentDigest,
    pub(super) command_identity: DecisionContentDigest,
    pub(super) terminal_identity: DecisionContentDigest,
    pub(super) report_identity: DecisionContentDigest,
    pub(super) cohort_identity: DecisionContentDigest,
    pub(super) cost_model_identity: DecisionContentDigest,
    pub(super) window: ProposalEvidenceWindow,
}

impl CostAdjustedPitBacktestEvidence {
    /// Constructs an immutable cost-adjusted backtest result.
    ///
    /// # Errors
    ///
    /// Rejects negative drawdown or modeled cost assumptions, invalid stability, or a simulation
    /// cutoff later than the evidence publication time.
    #[allow(
        clippy::too_many_arguments,
        reason = "PIT dataset, command, terminal, report, cohort, and cost authorities remain explicit"
    )]
    pub fn try_new(
        instrument_id: InstrumentId,
        currency: Currency,
        outcome_horizon_nanos: i64,
        net_return: BasisPoints,
        max_drawdown: BasisPoints,
        fee_basis_points: BasisPoints,
        slippage_basis_points: BasisPoints,
        maximum_random_slippage_basis_points: BasisPoints,
        observations: NonZeroU32,
        trials: NonZeroU32,
        stability_ppm: u32,
        simulation_cutoff_at: Timestamp,
        dataset_identity: DecisionContentDigest,
        command_identity: DecisionContentDigest,
        terminal_identity: DecisionContentDigest,
        report_identity: DecisionContentDigest,
        cohort_identity: DecisionContentDigest,
        cost_model_identity: DecisionContentDigest,
        window: ProposalEvidenceWindow,
    ) -> Result<Self, InvestmentProposalError> {
        ensure_ppm(stability_ppm)?;
        if outcome_horizon_nanos <= 0 || simulation_cutoff_at > window.available_at() {
            return Err(InvestmentProposalError::InvalidTimeOrder);
        }
        if max_drawdown.get().is_negative()
            || [
                fee_basis_points,
                slippage_basis_points,
                maximum_random_slippage_basis_points,
            ]
            .into_iter()
            .any(|value| !(0..=10_000).contains(&value.get()))
        {
            return Err(InvestmentProposalError::InvalidEvidenceMetric);
        }
        Ok(Self {
            instrument_id,
            currency,
            outcome_horizon_nanos,
            net_return,
            max_drawdown,
            fee_basis_points,
            slippage_basis_points,
            maximum_random_slippage_basis_points,
            observations,
            trials,
            stability_ppm,
            simulation_cutoff_at,
            dataset_identity,
            command_identity,
            terminal_identity,
            report_identity,
            cohort_identity,
            cost_model_identity,
            window,
        })
    }

    /// Returns the stable instrument identity.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the denomination used for cost and return analysis.
    #[must_use]
    pub const fn currency(self) -> Currency {
        self.currency
    }

    /// Returns the evaluated forecast-to-outcome horizon in nanoseconds.
    #[must_use]
    pub const fn outcome_horizon_nanos(self) -> i64 {
        self.outcome_horizon_nanos
    }

    /// Returns cost-adjusted net performance in signed basis points.
    #[must_use]
    pub const fn net_return(self) -> BasisPoints {
        self.net_return
    }

    /// Returns nonnegative maximum drawdown in basis points.
    #[must_use]
    pub const fn max_drawdown(self) -> BasisPoints {
        self.max_drawdown
    }

    /// Returns the modeled per-fill fee applied to exact filled notional.
    ///
    /// This is an input to the retained cost-adjusted result, not a synthesized round-trip cost.
    #[must_use]
    pub const fn fee_basis_points(self) -> BasisPoints {
        self.fee_basis_points
    }

    /// Returns deterministic adverse slippage modeled beyond the observed half spread.
    ///
    /// This excludes the observation-specific spread and is not a round-trip aggregate.
    #[must_use]
    pub const fn slippage_basis_points(self) -> BasisPoints {
        self.slippage_basis_points
    }

    /// Returns the seeded additional adverse-slippage ceiling used by the fill model.
    ///
    /// This is a maximum model input, not realized or round-trip cost.
    #[must_use]
    pub const fn maximum_random_slippage_basis_points(self) -> BasisPoints {
        self.maximum_random_slippage_basis_points
    }

    /// Returns the number of evaluated point-in-time observations.
    #[must_use]
    pub const fn observations(self) -> NonZeroU32 {
        self.observations
    }

    /// Returns the number of independent backtest trials.
    #[must_use]
    pub const fn trials(self) -> NonZeroU32 {
        self.trials
    }

    /// Returns code-defined backtest stability in parts per million.
    #[must_use]
    pub const fn stability_ppm(self) -> u32 {
        self.stability_ppm
    }

    /// Returns the latest historical fact admitted to the simulation.
    #[must_use]
    pub const fn simulation_cutoff_at(self) -> Timestamp {
        self.simulation_cutoff_at
    }

    /// Returns the exact point-in-time dataset identity.
    #[must_use]
    pub const fn dataset_identity(self) -> DecisionContentDigest {
        self.dataset_identity
    }

    /// Returns the canonical backtest command identity.
    #[must_use]
    pub const fn command_identity(self) -> DecisionContentDigest {
        self.command_identity
    }

    /// Returns the terminal evaluation identity.
    #[must_use]
    pub const fn terminal_identity(self) -> DecisionContentDigest {
        self.terminal_identity
    }

    /// Returns the complete report identity.
    #[must_use]
    pub const fn report_identity(self) -> DecisionContentDigest {
        self.report_identity
    }

    /// Returns the calibration/evaluation cohort identity.
    #[must_use]
    pub const fn cohort_identity(self) -> DecisionContentDigest {
        self.cohort_identity
    }

    /// Returns the exact cost-model identity.
    #[must_use]
    pub const fn cost_model_identity(self) -> DecisionContentDigest {
        self.cost_model_identity
    }

    /// Returns exact backtest publication timing and payload identity.
    #[must_use]
    pub const fn window(self) -> ProposalEvidenceWindow {
        self.window
    }
}

/// Current bounded liquidity evidence for the proposal instrument and currency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiquidityEvidence {
    pub(super) instrument_id: InstrumentId,
    pub(super) currency: Currency,
    pub(super) quoted_spread: BasisPoints,
    pub(super) capacity_ppm: u32,
    pub(super) quality: DataQuality,
    pub(super) assessment_identity: DecisionContentDigest,
    pub(super) window: ProposalEvidenceWindow,
}

impl LiquidityEvidence {
    /// Constructs nonnegative spread and bounded capacity evidence.
    ///
    /// # Errors
    ///
    /// Rejects negative spread or capacity above one million parts per million.
    pub fn try_new(
        instrument_id: InstrumentId,
        currency: Currency,
        quoted_spread: BasisPoints,
        capacity_ppm: u32,
        quality: DataQuality,
        assessment_identity: DecisionContentDigest,
        window: ProposalEvidenceWindow,
    ) -> Result<Self, InvestmentProposalError> {
        ensure_ppm(capacity_ppm)?;
        if quoted_spread.get().is_negative() {
            return Err(InvestmentProposalError::InvalidEvidenceMetric);
        }
        Ok(Self {
            instrument_id,
            currency,
            quoted_spread,
            capacity_ppm,
            quality,
            assessment_identity,
            window,
        })
    }

    /// Returns the stable instrument identity.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the denomination of the liquidity assessment.
    #[must_use]
    pub const fn currency(self) -> Currency {
        self.currency
    }

    /// Returns the quoted spread in nonnegative basis points.
    #[must_use]
    pub const fn quoted_spread(self) -> BasisPoints {
        self.quoted_spread
    }

    /// Returns policy-relative usable capacity in parts per million.
    #[must_use]
    pub const fn capacity_ppm(self) -> u32 {
        self.capacity_ppm
    }

    /// Returns the source-qualified liquidity quality.
    #[must_use]
    pub const fn quality(self) -> DataQuality {
        self.quality
    }

    /// Returns the exact liquidity-assessment identity.
    #[must_use]
    pub const fn assessment_identity(self) -> DecisionContentDigest {
        self.assessment_identity
    }

    /// Returns exact liquidity timing and payload identity.
    #[must_use]
    pub const fn window(self) -> ProposalEvidenceWindow {
        self.window
    }
}

/// Exact current position permissions used only to choose a research recommendation verb.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PortfolioPositionState {
    /// The account has no current position in the instrument.
    NoPosition,
    /// The account has a position and explicit research-policy capabilities.
    Position {
        /// Portfolio policy permits considering an add recommendation.
        add_allowed: bool,
        /// Portfolio policy permits considering a trim recommendation.
        trim_allowed: bool,
        /// Portfolio policy permits considering a full-exit recommendation.
        exit_allowed: bool,
    },
}

/// Current account-bound portfolio-risk evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortfolioRiskEvidence {
    pub(super) instrument_id: InstrumentId,
    pub(super) account_id: AccountId,
    pub(super) currency: Currency,
    pub(super) portfolio_revision: PortfolioRevisionToken,
    pub(super) position_state: PortfolioPositionState,
    pub(super) risk_capacity_ppm: u32,
    pub(super) risk_report_identity: DecisionContentDigest,
    pub(super) window: ProposalEvidenceWindow,
}

impl PortfolioRiskEvidence {
    /// Constructs exact account, revision, position-state, and risk-capacity evidence.
    ///
    /// # Errors
    ///
    /// Rejects capacity above one million parts per million. A zero portfolio revision is retained
    /// so the authority can produce an evidence-preserving unavailable result.
    #[allow(
        clippy::too_many_arguments,
        reason = "account, portfolio revision, position, risk, and evidence authorities remain explicit"
    )]
    pub fn try_new(
        instrument_id: InstrumentId,
        account_id: AccountId,
        currency: Currency,
        portfolio_revision: PortfolioRevisionToken,
        position_state: PortfolioPositionState,
        risk_capacity_ppm: u32,
        risk_report_identity: DecisionContentDigest,
        window: ProposalEvidenceWindow,
    ) -> Result<Self, InvestmentProposalError> {
        ensure_ppm(risk_capacity_ppm)?;
        Ok(Self {
            instrument_id,
            account_id,
            currency,
            portfolio_revision,
            position_state,
            risk_capacity_ppm,
            risk_report_identity,
            window,
        })
    }

    /// Returns the stable instrument identity.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact account whose portfolio risk was assessed.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the portfolio reporting currency.
    #[must_use]
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the immutable portfolio-revision precondition.
    #[must_use]
    pub const fn portfolio_revision(&self) -> &PortfolioRevisionToken {
        &self.portfolio_revision
    }

    /// Returns the current position state and research-policy capabilities.
    #[must_use]
    pub const fn position_state(&self) -> PortfolioPositionState {
        self.position_state
    }

    /// Returns available portfolio-risk capacity in parts per million.
    #[must_use]
    pub const fn risk_capacity_ppm(&self) -> u32 {
        self.risk_capacity_ppm
    }

    /// Returns the exact risk-report identity.
    #[must_use]
    pub const fn risk_report_identity(&self) -> DecisionContentDigest {
        self.risk_report_identity
    }

    /// Returns exact portfolio-risk timing and payload identity.
    #[must_use]
    pub const fn window(&self) -> ProposalEvidenceWindow {
        self.window
    }
}

/// Complete caller-supplied evidence envelope for one automated investment analysis.
///
/// Each authority field is optional so missing configured producers become a typed unavailable
/// outcome instead of forcing manual dossier or target-price entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvestmentAnalysisEvidenceInput {
    /// Stable instrument selected by ranked discovery or ad hoc analysis.
    pub instrument_id: InstrumentId,
    /// Explicit analysis denomination.
    pub currency: Currency,
    /// Exact account whose risk constraints apply.
    pub account_id: AccountId,
    /// Point-in-time analysis cutoff.
    pub as_of: Timestamp,
    /// Current market evidence, when a configured producer supplied it.
    pub market: Option<MarketReferenceEvidence>,
    /// Calibrated price-forecast evidence, when available.
    pub price_forecast: Option<PriceForecastEvidence>,
    /// Governed fair-value evidence, when available.
    pub valuation: Option<ValuationEvidence>,
    /// Cost-adjusted point-in-time backtest evidence, when available.
    pub backtest: Option<CostAdjustedPitBacktestEvidence>,
    /// Current liquidity evidence, when available.
    pub liquidity: Option<LiquidityEvidence>,
    /// Current portfolio-risk evidence, when available.
    pub portfolio_risk: Option<PortfolioRiskEvidence>,
}

/// Immutable, bounded input retained by every generated, no-action, and unavailable result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvestmentAnalysisEvidence {
    pub(super) instrument_id: InstrumentId,
    pub(super) currency: Currency,
    pub(super) account_id: AccountId,
    pub(super) as_of: Timestamp,
    pub(super) market: Option<MarketReferenceEvidence>,
    pub(super) price_forecast: Option<PriceForecastEvidence>,
    pub(super) valuation: Option<ValuationEvidence>,
    pub(super) backtest: Option<CostAdjustedPitBacktestEvidence>,
    pub(super) liquidity: Option<LiquidityEvidence>,
    pub(super) portfolio_risk: Option<PortfolioRiskEvidence>,
    pub(super) selected_candidate: Option<SelectedCandidateAnalysisEvidence>,
}

impl InvestmentAnalysisEvidence {
    /// Captures one complete evidence envelope without silently replacing absent authorities.
    ///
    /// Binding, freshness, and admissibility are intentionally evaluated by
    /// [`super::InvestmentProposalAuthority`] so incomplete requests yield persisted typed
    /// unavailable analyses.
    #[must_use]
    pub const fn new(input: InvestmentAnalysisEvidenceInput) -> Self {
        Self {
            instrument_id: input.instrument_id,
            currency: input.currency,
            account_id: input.account_id,
            as_of: input.as_of,
            market: input.market,
            price_forecast: input.price_forecast,
            valuation: input.valuation,
            backtest: input.backtest,
            liquidity: input.liquidity,
            portfolio_risk: input.portfolio_risk,
            selected_candidate: None,
        }
    }

    /// Adds an exact retained screen-candidate binding for a new selected-candidate analysis.
    ///
    /// Non-screen analyses may remain explicitly unbound through [`Self::new`]. This bound path
    /// requires the same instrument plus a screen cutoff and selection time no later than the
    /// analysis cutoff.
    pub fn try_with_selected_candidate(
        mut self,
        selected_candidate: SelectedCandidateAnalysisEvidence,
    ) -> Result<Self, InvestmentProposalError> {
        if selected_candidate.instrument_id() != self.instrument_id
            || selected_candidate.as_of() > self.as_of
            || selected_candidate.selected_at() > self.as_of
        {
            return Err(InvestmentProposalError::InvalidEvidenceMetric);
        }
        self.selected_candidate = Some(selected_candidate);
        Ok(self)
    }

    /// Returns the stable analysis instrument.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the common analysis denomination.
    #[must_use]
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the account whose portfolio evidence is bound.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the point-in-time analysis cutoff.
    #[must_use]
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns current-market evidence, when supplied.
    #[must_use]
    pub const fn market(&self) -> Option<&MarketReferenceEvidence> {
        self.market.as_ref()
    }

    /// Returns price-forecast evidence, when supplied.
    #[must_use]
    pub const fn price_forecast(&self) -> Option<&PriceForecastEvidence> {
        self.price_forecast.as_ref()
    }

    /// Returns valuation evidence, when supplied.
    #[must_use]
    pub const fn valuation(&self) -> Option<&ValuationEvidence> {
        self.valuation.as_ref()
    }

    /// Returns point-in-time backtest evidence, when supplied.
    #[must_use]
    pub const fn backtest(&self) -> Option<&CostAdjustedPitBacktestEvidence> {
        self.backtest.as_ref()
    }

    /// Returns liquidity evidence, when supplied.
    #[must_use]
    pub const fn liquidity(&self) -> Option<&LiquidityEvidence> {
        self.liquidity.as_ref()
    }

    /// Returns portfolio-risk evidence, when supplied.
    #[must_use]
    pub const fn portfolio_risk(&self) -> Option<&PortfolioRiskEvidence> {
        self.portfolio_risk.as_ref()
    }

    /// Returns the exact selected-screen-candidate binding for an opt-in analysis.
    #[must_use]
    pub const fn selected_candidate(&self) -> Option<&SelectedCandidateAnalysisEvidence> {
        self.selected_candidate.as_ref()
    }
}

fn ensure_positive(value: Money) -> Result<(), InvestmentProposalError> {
    if value.amount().is_zero() || value.amount().is_sign_negative() {
        Err(InvestmentProposalError::InvalidPrice)
    } else {
        Ok(())
    }
}

const fn ensure_ppm(value: u32) -> Result<(), InvestmentProposalError> {
    if value > CONFIDENCE_PARTS_PER_MILLION {
        Err(InvestmentProposalError::InvalidPartsPerMillion)
    } else {
        Ok(())
    }
}
