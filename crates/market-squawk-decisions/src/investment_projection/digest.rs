//! Canonical versioned SHA-256 identities for pure projection inputs and results.

use market_squawk_domain::{
    AccountId, Currency, Denomination, DigestAlgorithm, InstrumentExecutionTerms, InstrumentId,
    Money, Timestamp,
};
use market_squawk_portfolio::PortfolioRevisionToken;
use sha2::{Digest as _, Sha256};

use crate::{DecisionContentDigest, RecommendationAction, TargetPriceRange};

use super::outcome::{
    ExactFinancialRatio, ExactFinancialRatioRange, ExpectedGrossPricePnlAvailability,
    ExpectedReturnAvailability, GrossMarkRelativeRange, GrossPricePnlAvailability,
    InvestmentOutcomeProjection, MarkToZoneDistance, SignedMoneyRange,
};
use super::sizing::{
    CapacityRange, FeasibleLotRangeAvailability, FeasibleNotionalRangeAvailability,
    InvestmentSizingProjection, LotRange, NonnegativeMoneyRange, SizingCapacityAvailability,
    SizingCapacityEvidence, SizingConstraintCap, SizingConstraintKind, SizingUnavailableReason,
};
use super::{
    INVESTMENT_OUTCOME_PROJECTION_SCHEMA_VERSION, INVESTMENT_SIZING_PROJECTION_SCHEMA_VERSION,
    InvestmentProjectionAuthority, InvestmentProjectionBinding, InvestmentProjectionDigest,
};

const OUTCOME_DIGEST_DOMAIN: &[u8] = b"market-squawk/investment-outcome-projection";
const SIZING_DIGEST_DOMAIN: &[u8] = b"market-squawk/investment-sizing-projection";

pub(super) fn outcome_projection_digest(
    projection: &InvestmentOutcomeProjection,
) -> InvestmentProjectionDigest {
    let mut hash = CanonicalHasher::new(OUTCOME_DIGEST_DOMAIN);
    hash.u16(INVESTMENT_OUTCOME_PROJECTION_SCHEMA_VERSION);
    hash.binding(projection.binding);
    hash.authority(projection.authority);
    hash.instrument(projection.instrument_id);
    hash.money(projection.mark);
    hash.timestamp(projection.horizon_at);
    hash.timestamp(projection.proposal_expires_at);
    match projection.position_scale {
        Some(scale) => {
            hash.tag(1);
            hash.execution_terms(scale.terms());
            hash.i64(scale.quantity().get());
        }
        None => hash.tag(0),
    }
    hash.gross_mark_relative_range(projection.downside);
    hash.gross_mark_relative_range(projection.base);
    hash.gross_mark_relative_range(projection.upside);
    hash.zone_distance(projection.entry_distance);
    hash.zone_distance(projection.add_distance);
    hash.zone_distance(projection.trim_distance);
    hash.zone_distance(projection.exit_distance);
    match projection.expected_return {
        ExpectedReturnAvailability::Available(value) => {
            hash.tag(1);
            hash.ratio(value);
        }
        ExpectedReturnAvailability::UnavailableAdmittedExpectedTerminalValueNotSupplied => {
            hash.tag(0);
        }
    }
    match projection.expected_gross_price_pnl {
        ExpectedGrossPricePnlAvailability::Available(value) => {
            hash.tag(1);
            hash.money(value);
        }
        ExpectedGrossPricePnlAvailability::UnavailableAdmittedExpectedTerminalValueNotSupplied => {
            hash.tag(0);
        }
        ExpectedGrossPricePnlAvailability::UnavailableExactQuantityNotSupplied => hash.tag(2),
    }
    hash.tag(match projection.net_pnl {
        super::outcome::NetPnlAvailability::UnavailableExactForwardCostEvidenceNotSupplied => 0,
    });
    hash.tag(match projection.benchmark_return {
        super::outcome::BenchmarkReturnAvailability::UnavailableExactProposalTimeBenchmarkEvidenceNotSupplied => 0,
    });
    hash.tag(match projection.after_tax_pnl {
        super::outcome::AfterTaxPnlAvailability::UnavailableExactTaxEvidenceNotSupplied => 0,
    });
    InvestmentProjectionDigest::from_sha256(hash.finish())
}

pub(super) fn sizing_projection_digest(
    projection: &InvestmentSizingProjection,
) -> InvestmentProjectionDigest {
    let mut hash = CanonicalHasher::new(SIZING_DIGEST_DOMAIN);
    hash.u16(INVESTMENT_SIZING_PROJECTION_SCHEMA_VERSION);
    hash.binding(projection.binding);
    hash.recommendation_action(projection.proposal_action);
    hash.authority(projection.authority);
    let inputs = &projection.inputs;
    hash.timestamp(inputs.evaluated_at);
    hash.execution_terms(inputs.execution_terms);
    hash.money(inputs.selected_mark);
    hash.account(inputs.portfolio.account_id);
    hash.instrument(inputs.portfolio.instrument_id);
    hash.portfolio_revision(&inputs.portfolio.portfolio_revision);
    hash.money(inputs.portfolio.marked_equity_at_selected_mark);
    hash.money(inputs.portfolio.settlement_available_cash);
    hash.i64(inputs.portfolio.current_lots.get());
    hash.money(inputs.constraints.minimum_cash_reserve);
    hash.u16(inputs.constraints.preferred_weight_lower_basis_points);
    hash.u16(inputs.constraints.preferred_weight_upper_basis_points);
    hash.u16(inputs.constraints.maximum_downside_loss_basis_points);
    hash.capacity_availability(&inputs.liquidity_capacity);
    hash.capacity_availability(&inputs.risk_capacity);
    hash.capacity_availability(&inputs.forward_cost_capacity);
    hash.money(projection.per_lot_notional);
    hash.money(projection.per_lot_downside_loss);
    hash.length(projection.constraint_caps.len());
    for cap in projection.constraint_caps.as_ref() {
        hash.constraint_cap(*cap);
    }
    hash.feasible_lots(&projection.hard_feasible_lots);
    hash.feasible_lots(&projection.preferred_feasible_lots);
    hash.feasible_notional(&projection.hard_feasible_target_notional);
    hash.feasible_notional(&projection.preferred_feasible_target_notional);
    hash.money(projection.preferred_weight_rounding.lower_round_up_excess());
    hash.money(
        projection
            .preferred_weight_rounding
            .upper_round_down_remainder(),
    );
    hash.constraint_kinds(&projection.hard_binding_caps);
    hash.constraint_kinds(&projection.preferred_binding_caps);
    InvestmentProjectionDigest::from_sha256(hash.finish())
}

struct CanonicalHasher(Sha256);

impl CanonicalHasher {
    fn new(domain: &[u8]) -> Self {
        let mut value = Self(Sha256::new());
        value.bytes(domain);
        value
    }

    fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }

    fn tag(&mut self, value: u8) {
        self.0.update([value]);
    }

    fn u16(&mut self, value: u16) {
        self.0.update(value.to_be_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.0.update(value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.update(value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.0.update(value.to_be_bytes());
    }

    fn i128(&mut self, value: i128) {
        self.0.update(value.to_be_bytes());
    }

    fn length(&mut self, value: usize) {
        let length = match u64::try_from(value) {
            Ok(length) => length,
            Err(_) => u64::MAX,
        };
        self.u64(length);
    }

    fn bytes(&mut self, value: &[u8]) {
        self.length(value.len());
        self.0.update(value);
    }

    fn timestamp(&mut self, value: Timestamp) {
        self.i64(value.unix_nanos());
    }

    fn instrument(&mut self, value: InstrumentId) {
        self.0.update(value.as_uuid().as_bytes());
    }

    fn account(&mut self, value: AccountId) {
        self.0.update(value.as_uuid().as_bytes());
    }

    fn currency(&mut self, value: Currency) {
        self.0.update(value.as_str().as_bytes());
    }

    fn decimal(&mut self, value: rust_decimal::Decimal) {
        let normalized = value.normalize();
        self.i128(normalized.mantissa());
        self.u32(normalized.scale());
    }

    fn money(&mut self, value: Money) {
        self.decimal(value.amount());
        self.currency(value.currency());
    }

    fn target_range(&mut self, value: TargetPriceRange) {
        self.money(value.lower());
        self.money(value.upper());
    }

    fn signed_money_range(&mut self, value: SignedMoneyRange) {
        self.money(value.lower());
        self.money(value.upper());
    }

    fn ratio(&mut self, value: ExactFinancialRatio) {
        self.money(value.numerator());
        self.money(value.denominator());
    }

    fn ratio_range(&mut self, value: ExactFinancialRatioRange) {
        self.ratio(value.lower());
        self.ratio(value.upper());
    }

    fn gross_mark_relative_range(&mut self, value: GrossMarkRelativeRange) {
        self.target_range(value.price_range);
        self.signed_money_range(value.absolute_change);
        self.ratio_range(value.gross_return_from_mark);
        match value.gross_price_pnl {
            GrossPricePnlAvailability::Available(range) => {
                self.tag(1);
                self.signed_money_range(range);
            }
            GrossPricePnlAvailability::UnavailableExactQuantityNotSupplied => self.tag(0),
        }
    }

    fn zone_distance(&mut self, value: MarkToZoneDistance) {
        self.target_range(value.zone);
        self.signed_money_range(value.absolute_distance);
        self.ratio_range(value.relative_distance_from_mark);
    }

    fn binding(&mut self, value: InvestmentProjectionBinding) {
        self.0.update(value.proposal_id().bytes());
        self.0.update(value.derivation_digest().bytes());
    }

    fn authority(&mut self, value: InvestmentProjectionAuthority) {
        self.tag(match value {
            InvestmentProjectionAuthority::AnalysisOnlyNoMutationNoExecution => 0,
        });
    }

    fn recommendation_action(&mut self, value: RecommendationAction) {
        self.tag(match value {
            RecommendationAction::Buy => 0,
            RecommendationAction::Add => 1,
            RecommendationAction::Hold => 2,
            RecommendationAction::Trim => 3,
            RecommendationAction::Sell => 4,
        });
    }

    fn execution_terms(&mut self, value: InstrumentExecutionTerms) {
        self.instrument(value.instrument_id());
        self.u64(value.definition_revision().get());
        self.decimal(value.price_tick().as_decimal());
        self.decimal(value.lot_size().as_decimal());
        self.currency(value.quote_currency());
        match value.settlement_denomination() {
            Denomination::Currency(currency) => {
                self.tag(0);
                self.currency(currency);
            }
            Denomination::Asset(instrument_id) => {
                self.tag(1);
                self.instrument(instrument_id);
            }
        }
        self.decimal(value.contract_multiplier());
    }

    fn decision_content_digest(&mut self, value: DecisionContentDigest) {
        let digest = value.evidence_digest();
        self.tag(match digest.algorithm() {
            DigestAlgorithm::Sha256 => 0,
            DigestAlgorithm::Blake3 => 1,
        });
        self.0.update(digest.bytes());
    }

    fn portfolio_revision(&mut self, value: &PortfolioRevisionToken) {
        self.0.update(value.bytes());
    }

    fn lot_range(&mut self, value: LotRange) {
        self.i64(value.lower().get());
        self.i64(value.upper().get());
    }

    fn money_range(&mut self, value: NonnegativeMoneyRange) {
        self.money(value.lower());
        self.money(value.upper());
    }

    fn capacity_range(&mut self, value: CapacityRange) {
        match value {
            CapacityRange::Lots(range) => {
                self.tag(0);
                self.lot_range(range);
            }
            CapacityRange::Notional(range) => {
                self.tag(1);
                self.money_range(range);
            }
        }
    }

    fn capacity_evidence(&mut self, value: &SizingCapacityEvidence) {
        self.instrument(value.instrument_id);
        self.account(value.account_id);
        self.portfolio_revision(&value.portfolio_revision);
        self.u64(value.definition_revision.get());
        self.money(value.reference_mark);
        self.capacity_range(value.range);
        self.decision_content_digest(value.content_identity);
        self.timestamp(value.observed_at);
        self.timestamp(value.available_at);
        self.timestamp(value.expires_at);
    }

    fn capacity_availability(&mut self, value: &SizingCapacityAvailability) {
        match value {
            SizingCapacityAvailability::Available(evidence) => {
                self.tag(1);
                self.capacity_evidence(evidence.as_ref());
            }
            SizingCapacityAvailability::UnavailableNotSupplied => self.tag(0),
        }
    }

    fn constraint_kind(&mut self, value: SizingConstraintKind) {
        self.tag(match value {
            SizingConstraintKind::CashReserve => 0,
            SizingConstraintKind::DownsideLoss => 1,
            SizingConstraintKind::Liquidity => 2,
            SizingConstraintKind::PortfolioRisk => 3,
            SizingConstraintKind::ForwardCost => 4,
            SizingConstraintKind::PreferredWeight => 5,
        });
    }

    fn unavailable_reason(&mut self, value: SizingUnavailableReason) {
        match value {
            SizingUnavailableReason::CapacityNotSupplied(kind) => {
                self.tag(0);
                self.constraint_kind(kind);
            }
            SizingUnavailableReason::CapacityNotYetAvailable(kind) => {
                self.tag(1);
                self.constraint_kind(kind);
            }
            SizingUnavailableReason::CapacityExpired(kind) => {
                self.tag(2);
                self.constraint_kind(kind);
            }
            SizingUnavailableReason::CapacityRangeContainsNoLots(kind) => {
                self.tag(3);
                self.constraint_kind(kind);
            }
            SizingUnavailableReason::CashReserveExceedsGrossLiquidatableValue => self.tag(4),
            SizingUnavailableReason::NoHardFeasibleLotIntersection => self.tag(5),
            SizingUnavailableReason::PreferredWeightRangeContainsNoLots => self.tag(6),
            SizingUnavailableReason::NoPreferredFeasibleLotIntersection => self.tag(7),
        }
    }

    fn constraint_cap(&mut self, value: SizingConstraintCap) {
        match value {
            SizingConstraintCap::Available {
                kind,
                lot_range,
                capacity_identity,
            } => {
                self.tag(1);
                self.constraint_kind(kind);
                self.lot_range(lot_range);
                match capacity_identity {
                    Some(identity) => {
                        self.tag(1);
                        self.decision_content_digest(identity);
                    }
                    None => self.tag(0),
                }
            }
            SizingConstraintCap::Unavailable { kind, reason } => {
                self.tag(0);
                self.constraint_kind(kind);
                self.unavailable_reason(reason);
            }
        }
    }

    fn feasible_lots(&mut self, value: &FeasibleLotRangeAvailability) {
        match value {
            FeasibleLotRangeAvailability::Available(range) => {
                self.tag(1);
                self.lot_range(*range);
            }
            FeasibleLotRangeAvailability::Unavailable(reasons) => {
                self.tag(0);
                self.length(reasons.len());
                for reason in reasons.as_ref() {
                    self.unavailable_reason(*reason);
                }
            }
        }
    }

    fn feasible_notional(&mut self, value: &FeasibleNotionalRangeAvailability) {
        match value {
            FeasibleNotionalRangeAvailability::Available(range) => {
                self.tag(1);
                self.money_range(*range);
            }
            FeasibleNotionalRangeAvailability::Unavailable(reasons) => {
                self.tag(0);
                self.length(reasons.len());
                for reason in reasons.as_ref() {
                    self.unavailable_reason(*reason);
                }
            }
        }
    }

    fn constraint_kinds(&mut self, values: &[SizingConstraintKind]) {
        self.length(values.len());
        for value in values {
            self.constraint_kind(*value);
        }
    }
}
