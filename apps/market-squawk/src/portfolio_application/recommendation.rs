//! Pure, revision-bound portfolio and market prerequisites for personalized analysis.

use std::{
    num::NonZeroUsize,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

use market_squawk_adapter_portfolio::TransactionKind;
use market_squawk_analytics::{StatisticalInput, StatisticalScale, StatisticalUnit};
use market_squawk_domain::{
    AccountId, Currency, DigestAlgorithm, EvidenceDigest, InstrumentId, LotSize, Money, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_portfolio::PortfolioRevisionToken;
use rust_decimal::prelude::ToPrimitive as _;
use rust_decimal::{Decimal, RoundingStrategy};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    PortfolioApplicationServiceError, PortfolioCandidateImpactReadCapability,
    PortfolioCandidateResolutionAuthority, Runtime,
    candidate::{
        PortfolioAnalysisDepthAvailability, PortfolioAnalysisDepthSideEvidence,
        PortfolioAnalysisLiquidityEvidence, PortfolioAnalysisLiquiditySide,
        PortfolioAnalysisMarketAvailability, PortfolioAnalysisMarketSet,
        PortfolioAnalysisMarketUnavailableReason, PortfolioAnalysisSetupResolution,
        PortfolioAnalysisSetupSnapshot, PortfolioCandidateMarketEvidence,
    },
    model::{PortfolioReadImage, PublishedRevision},
};
use crate::application::recommendation::SetupRequired;

const ANALYSIS_AUTHORITY: &str = concat!(
    "analysis_only;portfolio_mutation=false;execution_authority=false;",
    "risk_approval=false;reservation=false;order=false"
);

/// Caller-owned analytical policy. It contains no inferred user or presentation-layer default.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioAnalysisPrerequisitePolicy {
    minimum_historical_return_observations: NonZeroUsize,
    digest: EvidenceDigest,
}

impl PortfolioAnalysisPrerequisitePolicy {
    pub(crate) fn try_new(
        minimum_historical_return_observations: NonZeroUsize,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let count = u64::try_from(minimum_historical_return_observations.get())
            .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/portfolio-analysis-prerequisite-policy/v1\0");
        digest.update(count.to_be_bytes());
        canonical_text(
            &mut digest,
            "exact_decimal_historical_var_expected_shortfall_95",
        );
        canonical_text(&mut digest, "exact_selected_source_side_depth");
        canonical_text(&mut digest, ANALYSIS_AUTHORITY);
        Ok(Self {
            minimum_historical_return_observations,
            digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
        })
    }

    pub(crate) const fn minimum_historical_return_observations(self) -> NonZeroUsize {
        self.minimum_historical_return_observations
    }

    pub(crate) const fn digest(self) -> EvidenceDigest {
        self.digest
    }
}

/// One complete current imported holding before current selected marks are applied.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioAnalysisHoldingSnapshot {
    instrument_id: InstrumentId,
    quantity: Decimal,
    lot_size: LotSize,
    source_market_value: Money,
    observed_at: Timestamp,
    source_reference: SourceIdentifier,
}

impl PortfolioAnalysisHoldingSnapshot {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn quantity(&self) -> Decimal {
        self.quantity
    }

    pub(crate) const fn lot_size(&self) -> LotSize {
        self.lot_size
    }

    pub(crate) const fn source_market_value(&self) -> Money {
        self.source_market_value
    }

    pub(crate) const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    pub(crate) const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}

/// One exact historical portfolio return with its two immutable revision identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioAnalysisHistoricalReturn {
    opening_revision: PortfolioRevisionToken,
    closing_revision: PortfolioRevisionToken,
    opening_effective_at: Timestamp,
    closing_effective_at: Timestamp,
    opening_value: Money,
    closing_value: Money,
    external_flow: Money,
    value: Decimal,
}

impl PortfolioAnalysisHistoricalReturn {
    pub(crate) const fn opening_revision(&self) -> &PortfolioRevisionToken {
        &self.opening_revision
    }

    pub(crate) const fn closing_revision(&self) -> &PortfolioRevisionToken {
        &self.closing_revision
    }

    pub(crate) const fn opening_value(&self) -> Money {
        self.opening_value
    }

    pub(crate) const fn closing_value(&self) -> Money {
        self.closing_value
    }

    pub(crate) const fn external_flow(&self) -> Money {
        self.external_flow
    }

    pub(crate) const fn value(&self) -> Decimal {
        self.value
    }
}

/// Exact reason historical risk cannot be calculated without inventing portfolio evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PortfolioAnalysisRiskUnavailableReason {
    MissingAvailability { revision: PortfolioRevisionToken },
    FutureEvidence { revision: PortfolioRevisionToken },
    ReportingCurrencyMismatch { revision: PortfolioRevisionToken },
    NonPositiveOpeningValue { revision: PortfolioRevisionToken },
    InsufficientHistory { required: usize, available: usize },
}

/// Typed 95% one-sided historical VaR/ES and the exact user-budget capacity calculation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioAnalysisRiskEvidence {
    account_id: AccountId,
    portfolio_revision: PortfolioRevisionToken,
    profile_digest: [u8; 32],
    returns: Box<[PortfolioAnalysisHistoricalReturn]>,
    confidence_basis_points: u16,
    value_at_risk: Decimal,
    expected_shortfall: Decimal,
    expected_shortfall_basis_points_ceil: u32,
    user_downside_budget_basis_points: u16,
    risk_capacity_ppm: u32,
    policy_digest: EvidenceDigest,
    evaluated_at: Timestamp,
    evidence_digest: EvidenceDigest,
}

impl PortfolioAnalysisRiskEvidence {
    pub(crate) const fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub(crate) const fn portfolio_revision(&self) -> &PortfolioRevisionToken {
        &self.portfolio_revision
    }

    pub(crate) const fn profile_digest(&self) -> [u8; 32] {
        self.profile_digest
    }

    pub(crate) fn returns(&self) -> &[PortfolioAnalysisHistoricalReturn] {
        &self.returns
    }

    pub(crate) const fn confidence_basis_points(&self) -> u16 {
        self.confidence_basis_points
    }

    pub(crate) const fn value_at_risk(&self) -> Decimal {
        self.value_at_risk
    }

    pub(crate) const fn expected_shortfall(&self) -> Decimal {
        self.expected_shortfall
    }

    pub(crate) const fn expected_shortfall_basis_points_ceil(&self) -> u32 {
        self.expected_shortfall_basis_points_ceil
    }

    pub(crate) const fn user_downside_budget_basis_points(&self) -> u16 {
        self.user_downside_budget_basis_points
    }

    pub(crate) const fn risk_capacity_ppm(&self) -> u32 {
        self.risk_capacity_ppm
    }

    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }

    pub(crate) const fn policy_digest(&self) -> EvidenceDigest {
        self.policy_digest
    }

    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }
}

/// Historical risk evidence or an exact point-in-time/history gap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PortfolioAnalysisRiskAvailability {
    Available(PortfolioAnalysisRiskEvidence),
    Unavailable(PortfolioAnalysisRiskUnavailableReason),
}

/// Immutable current imported portfolio, complete holding inventory, and historical-risk status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioAnalysisPortfolioSnapshot {
    setup: PortfolioAnalysisSetupSnapshot,
    account_id: AccountId,
    revision: PortfolioRevisionToken,
    reporting_currency: Currency,
    source_cash_balance: Money,
    effective_at: Timestamp,
    available_at: Timestamp,
    source_id: SourceId,
    source_coverage: Box<[SourceId]>,
    artifact_sha256: [u8; 32],
    holdings: Box<[PortfolioAnalysisHoldingSnapshot]>,
    risk: PortfolioAnalysisRiskAvailability,
    policy: PortfolioAnalysisPrerequisitePolicy,
    evidence_digest: EvidenceDigest,
}

impl PortfolioAnalysisPortfolioSnapshot {
    pub(crate) const fn setup(&self) -> &PortfolioAnalysisSetupSnapshot {
        &self.setup
    }

    pub(crate) const fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub(crate) const fn revision(&self) -> &PortfolioRevisionToken {
        &self.revision
    }

    pub(crate) const fn reporting_currency(&self) -> Currency {
        self.reporting_currency
    }

    pub(crate) const fn source_cash_balance(&self) -> Money {
        self.source_cash_balance
    }

    pub(crate) const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    pub(crate) const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) fn source_coverage(&self) -> &[SourceId] {
        &self.source_coverage
    }

    pub(crate) const fn artifact_sha256(&self) -> [u8; 32] {
        self.artifact_sha256
    }

    pub(crate) fn holdings(&self) -> &[PortfolioAnalysisHoldingSnapshot] {
        &self.holdings
    }

    pub(crate) const fn risk(&self) -> &PortfolioAnalysisRiskAvailability {
        &self.risk
    }

    pub(crate) const fn policy(&self) -> PortfolioAnalysisPrerequisitePolicy {
        self.policy
    }

    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }

    pub(crate) fn requested_instruments(
        &self,
        candidate: InstrumentId,
    ) -> Result<Vec<InstrumentId>, PortfolioApplicationServiceError> {
        let mut instruments = Vec::new();
        instruments
            .try_reserve_exact(self.holdings.len().saturating_add(1))
            .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
        instruments.extend(self.holdings.iter().map(|holding| holding.instrument_id));
        instruments.push(candidate);
        instruments.sort_unstable();
        instruments.dedup();
        Ok(instruments)
    }
}

/// One imported holding revalued with its exact current selected-market mark.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioAnalysisMarkedHolding {
    instrument_id: InstrumentId,
    quantity: Decimal,
    unit_mark: Money,
    marked_value: Money,
    observation_digest: EvidenceDigest,
    selection_digest: EvidenceDigest,
    fresh_until: Timestamp,
}

impl PortfolioAnalysisMarkedHolding {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn quantity(&self) -> Decimal {
        self.quantity
    }

    pub(crate) const fn unit_mark(&self) -> Money {
        self.unit_mark
    }

    pub(crate) const fn marked_value(&self) -> Money {
        self.marked_value
    }

    pub(crate) const fn observation_digest(&self) -> EvidenceDigest {
        self.observation_digest
    }

    pub(crate) const fn selection_digest(&self) -> EvidenceDigest {
        self.selection_digest
    }

    pub(crate) const fn fresh_until(&self) -> Timestamp {
        self.fresh_until
    }
}

/// Current candidate exposure; no position is distinct from an imported nonzero holding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PortfolioAnalysisCurrentPosition {
    NoPosition,
    Position {
        quantity: Decimal,
        marked_value: Money,
    },
}

/// Complete selected-mark revaluation of cash plus every current imported holding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioAnalysisMarkedPortfolioEvidence {
    candidate_instrument_id: InstrumentId,
    source_cash_balance: Money,
    holdings: Box<[PortfolioAnalysisMarkedHolding]>,
    marked_equity: Money,
    current_position: PortfolioAnalysisCurrentPosition,
    portfolio_revision: PortfolioRevisionToken,
    market_set_digest: EvidenceDigest,
    evaluated_at: Timestamp,
    evidence_digest: EvidenceDigest,
}

impl PortfolioAnalysisMarkedPortfolioEvidence {
    pub(crate) const fn candidate_instrument_id(&self) -> InstrumentId {
        self.candidate_instrument_id
    }

    pub(crate) const fn source_cash_balance(&self) -> Money {
        self.source_cash_balance
    }

    pub(crate) fn holdings(&self) -> &[PortfolioAnalysisMarkedHolding] {
        &self.holdings
    }

    pub(crate) const fn marked_equity(&self) -> Money {
        self.marked_equity
    }

    pub(crate) const fn current_position(&self) -> PortfolioAnalysisCurrentPosition {
        self.current_position
    }

    pub(crate) const fn portfolio_revision(&self) -> &PortfolioRevisionToken {
        &self.portfolio_revision
    }

    pub(crate) const fn market_set_digest(&self) -> EvidenceDigest {
        self.market_set_digest
    }

    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Why one action-side analytical liquidity capacity cannot be calculated exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PortfolioAnalysisLiquidityCapacityUnavailableReason {
    Depth(super::candidate::PortfolioAnalysisDepthUnavailableReason),
    NoIntegralUpperWeightCapacity,
}

/// One exact side's depth relative to the profile upper-weight analytical lot capacity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioAnalysisLiquidityCapacitySideEvidence {
    side: PortfolioAnalysisLiquiditySide,
    executable_depth_notional: Money,
    analytical_upper_weight_notional: Money,
    analytical_upper_weight_lots: u64,
    capacity_ppm: u32,
    depth_evidence_digest: EvidenceDigest,
}

impl PortfolioAnalysisLiquidityCapacitySideEvidence {
    pub(crate) const fn side(&self) -> PortfolioAnalysisLiquiditySide {
        self.side
    }

    pub(crate) const fn executable_depth_notional(&self) -> Money {
        self.executable_depth_notional
    }

    pub(crate) const fn analytical_upper_weight_notional(&self) -> Money {
        self.analytical_upper_weight_notional
    }

    pub(crate) const fn analytical_upper_weight_lots(&self) -> u64 {
        self.analytical_upper_weight_lots
    }

    pub(crate) const fn capacity_ppm(&self) -> u32 {
        self.capacity_ppm
    }

    pub(crate) const fn depth_evidence_digest(&self) -> EvidenceDigest {
        self.depth_evidence_digest
    }
}

/// Typed capacity or an exact side-specific depth/analytical-lot gap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PortfolioAnalysisLiquidityCapacityAvailability {
    Available(PortfolioAnalysisLiquidityCapacitySideEvidence),
    Unavailable(PortfolioAnalysisLiquidityCapacityUnavailableReason),
}

/// Side-aware liquidity: ask supports Buy/Add, bid supports Trim/Sell; Hold selects neither.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioAnalysisLiquidityCapacityEvidence {
    account_id: AccountId,
    portfolio_revision: PortfolioRevisionToken,
    profile_digest: [u8; 32],
    marked_portfolio_digest: EvidenceDigest,
    market_observation_digest: EvidenceDigest,
    market_selection_digest: EvidenceDigest,
    buy_add: PortfolioAnalysisLiquidityCapacityAvailability,
    trim_sell: PortfolioAnalysisLiquidityCapacityAvailability,
    quoted_spread_basis_points: Option<market_squawk_domain::BasisPoints>,
    selected_liquidity_digest: EvidenceDigest,
    evaluated_at: Timestamp,
    evidence_digest: EvidenceDigest,
}

impl PortfolioAnalysisLiquidityCapacityEvidence {
    pub(crate) const fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub(crate) const fn portfolio_revision(&self) -> &PortfolioRevisionToken {
        &self.portfolio_revision
    }

    pub(crate) const fn profile_digest(&self) -> [u8; 32] {
        self.profile_digest
    }

    pub(crate) const fn marked_portfolio_digest(&self) -> EvidenceDigest {
        self.marked_portfolio_digest
    }

    pub(crate) const fn market_observation_digest(&self) -> EvidenceDigest {
        self.market_observation_digest
    }

    pub(crate) const fn market_selection_digest(&self) -> EvidenceDigest {
        self.market_selection_digest
    }

    pub(crate) const fn buy_add(&self) -> &PortfolioAnalysisLiquidityCapacityAvailability {
        &self.buy_add
    }

    pub(crate) const fn trim_sell(&self) -> &PortfolioAnalysisLiquidityCapacityAvailability {
        &self.trim_sell
    }

    pub(crate) const fn quoted_spread_basis_points(
        &self,
    ) -> Option<market_squawk_domain::BasisPoints> {
        self.quoted_spread_basis_points
    }

    pub(crate) const fn selected_liquidity_digest(&self) -> EvidenceDigest {
        self.selected_liquidity_digest
    }

    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }

    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }
}

/// Exact reason the complete prerequisite result must abstain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PortfolioAnalysisPrerequisiteUnavailableReason {
    CurrentMarket {
        instruments: Box<[(InstrumentId, PortfolioAnalysisMarketUnavailableReason)]>,
    },
    HistoricalRisk(PortfolioAnalysisRiskUnavailableReason),
    ReportingCurrencyMismatch {
        instrument_id: InstrumentId,
    },
    HoldingExecutionTermsMismatch {
        instrument_id: InstrumentId,
    },
    NonPositiveMarkedEquity,
}

/// Complete retained facts behind an analytical abstention.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioAnalysisPrerequisiteUnavailableEvidence {
    portfolio: PortfolioAnalysisPortfolioSnapshot,
    markets: PortfolioAnalysisMarketSet,
    marked_portfolio: Option<PortfolioAnalysisMarkedPortfolioEvidence>,
    reason: PortfolioAnalysisPrerequisiteUnavailableReason,
    evaluated_at: Timestamp,
    evidence_digest: EvidenceDigest,
}

impl PortfolioAnalysisPrerequisiteUnavailableEvidence {
    pub(crate) const fn portfolio(&self) -> &PortfolioAnalysisPortfolioSnapshot {
        &self.portfolio
    }

    pub(crate) const fn markets(&self) -> &PortfolioAnalysisMarketSet {
        &self.markets
    }

    pub(crate) const fn marked_portfolio(
        &self,
    ) -> Option<&PortfolioAnalysisMarkedPortfolioEvidence> {
        self.marked_portfolio.as_ref()
    }

    pub(crate) const fn reason(&self) -> &PortfolioAnalysisPrerequisiteUnavailableReason {
        &self.reason
    }

    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Complete non-executable prerequisite evidence for future research-only proposal mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioRecommendationEvidence {
    portfolio: PortfolioAnalysisPortfolioSnapshot,
    markets: PortfolioAnalysisMarketSet,
    marked_portfolio: PortfolioAnalysisMarkedPortfolioEvidence,
    risk: PortfolioAnalysisRiskEvidence,
    liquidity_capacity: PortfolioAnalysisLiquidityCapacityEvidence,
    evaluated_at: Timestamp,
    evidence_digest: EvidenceDigest,
}

impl PortfolioRecommendationEvidence {
    pub(crate) const fn portfolio(&self) -> &PortfolioAnalysisPortfolioSnapshot {
        &self.portfolio
    }

    pub(crate) const fn markets(&self) -> &PortfolioAnalysisMarketSet {
        &self.markets
    }

    pub(crate) const fn marked_portfolio(&self) -> &PortfolioAnalysisMarkedPortfolioEvidence {
        &self.marked_portfolio
    }

    pub(crate) const fn risk(&self) -> &PortfolioAnalysisRiskEvidence {
        &self.risk
    }

    pub(crate) const fn liquidity_capacity(&self) -> &PortfolioAnalysisLiquidityCapacityEvidence {
        &self.liquidity_capacity
    }

    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }

    /// This evidence is deliberately incapable of becoming an order, approval, or reservation.
    pub(crate) const fn authority(&self) -> &'static str {
        ANALYSIS_AUTHORITY
    }
}

/// Closed resolution that never chooses an account and never fabricates missing evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PortfolioAnalysisPrerequisiteResolution {
    SetupRequired {
        catalog: super::PortfolioAccountCatalogSnapshot,
        requirement: SetupRequired,
    },
    Unavailable(PortfolioAnalysisPrerequisiteUnavailableEvidence),
    Evaluated(PortfolioRecommendationEvidence),
}

impl PortfolioCandidateImpactReadCapability {
    /// Copies the complete selected current imported portfolio and bounded historical evidence.
    pub(crate) fn snapshot_analysis_portfolio(
        &self,
        setup: PortfolioAnalysisSetupSnapshot,
        policy: PortfolioAnalysisPrerequisitePolicy,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PortfolioAnalysisPortfolioSnapshot, PortfolioApplicationServiceError> {
        let _guard = self.runtime.admit()?;
        ensure_analysis_read_live(&self.runtime, deadline, cancellation)?;
        let image = self.runtime.image.load_full();
        let snapshot = portfolio_snapshot_from_image(&image, setup, policy)?;
        ensure_analysis_read_live(&self.runtime, deadline, cancellation)?;
        if !Arc::ptr_eq(&image, &self.runtime.image.load_full()) {
            return Err(PortfolioApplicationServiceError::StateChanged);
        }
        Ok(snapshot)
    }

    /// Rebuilds and compares every portfolio, holding, history, setup, and policy fact.
    pub(crate) fn recheck_analysis_portfolio(
        &self,
        expected: &PortfolioAnalysisPortfolioSnapshot,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), PortfolioApplicationServiceError> {
        let _guard = self.runtime.admit()?;
        ensure_analysis_read_live(&self.runtime, deadline, cancellation)?;
        let image = self.runtime.image.load_full();
        let current =
            portfolio_snapshot_from_image(&image, expected.setup.clone(), expected.policy)?;
        ensure_analysis_read_live(&self.runtime, deadline, cancellation)?;
        if &current != expected || !Arc::ptr_eq(&image, &self.runtime.image.load_full()) {
            return Err(PortfolioApplicationServiceError::StateChanged);
        }
        Ok(())
    }
}

/// Resolves the generic market/portfolio prerequisites for one research candidate.
///
/// The authority resolves only explicit setup and current market observations. The reader copies
/// only immutable imported portfolio state. The result cannot mutate either owner or cross into
/// paper/execution authority.
pub(crate) async fn resolve_portfolio_analysis_prerequisites(
    authority: &Arc<dyn PortfolioCandidateResolutionAuthority>,
    reader: &PortfolioCandidateImpactReadCapability,
    candidate_instrument_id: InstrumentId,
    policy: PortfolioAnalysisPrerequisitePolicy,
    as_of: Timestamp,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<PortfolioAnalysisPrerequisiteResolution, PortfolioApplicationServiceError> {
    ensure_analysis_read_live(&reader.runtime, deadline, &cancellation)?;
    let setup = match authority
        .resolve_analysis_setup(as_of, deadline, cancellation.clone())
        .await?
    {
        PortfolioAnalysisSetupResolution::Ready(setup) => setup,
        PortfolioAnalysisSetupResolution::SetupRequired {
            catalog,
            requirement,
        } => {
            return Ok(PortfolioAnalysisPrerequisiteResolution::SetupRequired {
                catalog,
                requirement,
            });
        }
    };
    let portfolio = reader.snapshot_analysis_portfolio(setup, policy, deadline, &cancellation)?;
    let instruments = portfolio.requested_instruments(candidate_instrument_id)?;
    let markets = authority
        .resolve_analysis_markets(
            portfolio.setup(),
            &instruments,
            as_of,
            deadline,
            cancellation.clone(),
        )
        .await?;

    let unavailable_markets = markets
        .entries()
        .iter()
        .filter_map(|entry| match entry.availability() {
            PortfolioAnalysisMarketAvailability::Available { .. } => None,
            PortfolioAnalysisMarketAvailability::Unavailable(reason) => {
                Some((entry.instrument_id(), *reason))
            }
        })
        .collect::<Vec<_>>();
    if !unavailable_markets.is_empty() {
        let unavailable = prerequisite_unavailable(
            portfolio,
            markets,
            None,
            PortfolioAnalysisPrerequisiteUnavailableReason::CurrentMarket {
                instruments: unavailable_markets.into_boxed_slice(),
            },
            as_of,
        );
        recheck_all(
            authority,
            reader,
            &unavailable.portfolio,
            &unavailable.markets,
            as_of,
            deadline,
            &cancellation,
        )
        .await?;
        return Ok(PortfolioAnalysisPrerequisiteResolution::Unavailable(
            unavailable,
        ));
    }

    let marked =
        match calculate_marked_portfolio(&portfolio, &markets, candidate_instrument_id, as_of)? {
            Ok(marked) => marked,
            Err(reason) => {
                let unavailable = prerequisite_unavailable(portfolio, markets, None, reason, as_of);
                recheck_all(
                    authority,
                    reader,
                    &unavailable.portfolio,
                    &unavailable.markets,
                    as_of,
                    deadline,
                    &cancellation,
                )
                .await?;
                return Ok(PortfolioAnalysisPrerequisiteResolution::Unavailable(
                    unavailable,
                ));
            }
        };
    let risk = match portfolio.risk() {
        PortfolioAnalysisRiskAvailability::Available(risk) => risk.clone(),
        PortfolioAnalysisRiskAvailability::Unavailable(reason) => {
            let reason = reason.clone();
            let unavailable = prerequisite_unavailable(
                portfolio,
                markets,
                Some(marked),
                PortfolioAnalysisPrerequisiteUnavailableReason::HistoricalRisk(reason),
                as_of,
            );
            recheck_all(
                authority,
                reader,
                &unavailable.portfolio,
                &unavailable.markets,
                as_of,
                deadline,
                &cancellation,
            )
            .await?;
            return Ok(PortfolioAnalysisPrerequisiteResolution::Unavailable(
                unavailable,
            ));
        }
    };
    let candidate_market = available_market(&markets, candidate_instrument_id)?;
    let liquidity_capacity = calculate_liquidity_capacity(
        portfolio.setup(),
        &marked,
        candidate_market.0,
        candidate_market.1,
    )?;
    let mut evidence = PortfolioRecommendationEvidence {
        portfolio,
        markets,
        marked_portfolio: marked,
        risk,
        liquidity_capacity,
        evaluated_at: as_of,
        evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
    };
    evidence.evidence_digest = recommendation_evidence_digest(&evidence);
    recheck_all(
        authority,
        reader,
        &evidence.portfolio,
        &evidence.markets,
        as_of,
        deadline,
        &cancellation,
    )
    .await?;
    Ok(PortfolioAnalysisPrerequisiteResolution::Evaluated(evidence))
}

#[allow(
    clippy::too_many_arguments,
    reason = "both independently owned immutable authorities and all time bounds remain explicit"
)]
async fn recheck_all(
    authority: &Arc<dyn PortfolioCandidateResolutionAuthority>,
    reader: &PortfolioCandidateImpactReadCapability,
    portfolio: &PortfolioAnalysisPortfolioSnapshot,
    markets: &PortfolioAnalysisMarketSet,
    as_of: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), PortfolioApplicationServiceError> {
    reader.recheck_analysis_portfolio(portfolio, deadline, cancellation)?;
    authority
        .recheck_analysis_markets(markets, as_of, deadline, cancellation.clone())
        .await?;
    reader.recheck_analysis_portfolio(portfolio, deadline, cancellation)?;
    ensure_analysis_read_live(&reader.runtime, deadline, cancellation)
}

fn portfolio_snapshot_from_image(
    image: &PortfolioReadImage,
    setup: PortfolioAnalysisSetupSnapshot,
    policy: PortfolioAnalysisPrerequisitePolicy,
) -> Result<PortfolioAnalysisPortfolioSnapshot, PortfolioApplicationServiceError> {
    let resolved = setup.setup();
    let head = resolved.current_head();
    let account_id = head.account_id();
    let history = image
        .accounts
        .get(&account_id)
        .ok_or(PortfolioApplicationServiceError::StateChanged)?;
    let revision = history
        .revisions
        .last()
        .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
    let token = revision.token();
    let available_at = revision
        .available_at
        .ok_or(PortfolioApplicationServiceError::StateChanged)?;
    let mut source_coverage = revision.source_coverage.clone();
    source_coverage.sort_unstable();
    if &token != head.revision()
        || image
            .revisions
            .head(account_id)
            .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?
            != token
        || revision.account.account_id() != account_id
        || revision.account.currency() != head.reporting_currency()
        || revision.account.currency() != resolved.profile().reporting_currency()
        || revision.effective_at != head.effective_at()
        || Some(available_at) != head.available_at()
        || &revision.source_id != head.source_id()
        || source_coverage.as_slice() != head.source_coverage()
        || revision.artifact_sha256 != head.artifact_sha256()
        || revision.effective_at > resolved.as_of()
        || available_at > resolved.as_of()
    {
        return Err(PortfolioApplicationServiceError::StateChanged);
    }
    let currency = revision.account.currency();
    let cash = revision.account.cash_balance();
    if cash.currency() != currency {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    let mut holdings = Vec::new();
    holdings
        .try_reserve_exact(revision.holdings.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    for holding in &revision.holdings {
        if holding.account_id() != account_id || holding.as_of() > revision.effective_at {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        holdings.push(PortfolioAnalysisHoldingSnapshot {
            instrument_id: holding.instrument_id(),
            quantity: holding.quantity().as_decimal(),
            lot_size: holding.lot_size(),
            source_market_value: holding.market_value(),
            observed_at: holding.as_of(),
            source_reference: holding.source_reference().clone(),
        });
    }
    holdings.sort_unstable_by_key(|holding| holding.instrument_id);
    if holdings
        .windows(2)
        .any(|pair| pair[0].instrument_id == pair[1].instrument_id)
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    let risk = historical_risk(
        history.revisions.as_slice(),
        account_id,
        &token,
        currency,
        resolved.as_of(),
        policy,
        resolved.profile().digest(),
        resolved
            .profile()
            .maximum_downside_loss_bps_of_marked_equity(),
    )?;
    let mut snapshot = PortfolioAnalysisPortfolioSnapshot {
        setup,
        account_id,
        revision: token,
        reporting_currency: currency,
        source_cash_balance: cash,
        effective_at: revision.effective_at,
        available_at,
        source_id: revision.source_id.clone(),
        source_coverage: source_coverage.into_boxed_slice(),
        artifact_sha256: revision.artifact_sha256,
        holdings: holdings.into_boxed_slice(),
        risk,
        policy,
        evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
    };
    snapshot.evidence_digest = portfolio_snapshot_digest(&snapshot);
    Ok(snapshot)
}

fn historical_risk(
    history: &[PublishedRevision],
    account_id: AccountId,
    portfolio_revision: &PortfolioRevisionToken,
    currency: Currency,
    as_of: Timestamp,
    policy: PortfolioAnalysisPrerequisitePolicy,
    profile_digest: [u8; 32],
    user_downside_budget_basis_points: u16,
) -> Result<PortfolioAnalysisRiskAvailability, PortfolioApplicationServiceError> {
    if user_downside_budget_basis_points == 0 {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    let mut periods = Vec::new();
    periods
        .try_reserve(history.len().saturating_sub(1))
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    for revision in history {
        let Some(available_at) = revision.available_at else {
            return Ok(PortfolioAnalysisRiskAvailability::Unavailable(
                PortfolioAnalysisRiskUnavailableReason::MissingAvailability {
                    revision: revision.token(),
                },
            ));
        };
        if available_at > as_of || revision.effective_at > as_of {
            return Ok(PortfolioAnalysisRiskAvailability::Unavailable(
                PortfolioAnalysisRiskUnavailableReason::FutureEvidence {
                    revision: revision.token(),
                },
            ));
        }
        if revision.account.currency() != currency
            || revision.account.cash_balance().currency() != currency
            || revision.holdings.iter().any(|holding| {
                holding.currency() != currency || holding.market_value().currency() != currency
            })
            || revision.transactions.iter().any(|transaction| {
                transaction.kind() == TransactionKind::CashTransfer
                    && transaction.amount().currency() != currency
            })
        {
            return Ok(PortfolioAnalysisRiskAvailability::Unavailable(
                PortfolioAnalysisRiskUnavailableReason::ReportingCurrencyMismatch {
                    revision: revision.token(),
                },
            ));
        }
    }
    for pair in history.windows(2) {
        let opening = &pair[0];
        let closing = &pair[1];
        if opening.effective_at >= closing.effective_at {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        let opening_value = source_reported_total_value(opening, currency)?;
        let closing_value = source_reported_total_value(closing, currency)?;
        if opening_value.amount() <= Decimal::ZERO {
            return Ok(PortfolioAnalysisRiskAvailability::Unavailable(
                PortfolioAnalysisRiskUnavailableReason::NonPositiveOpeningValue {
                    revision: opening.token(),
                },
            ));
        }
        let external_flow = closing
            .transactions
            .iter()
            .filter(|transaction| {
                transaction.kind() == TransactionKind::CashTransfer
                    && transaction.occurred_at() > opening.effective_at
                    && transaction.occurred_at() <= closing.effective_at
            })
            .try_fold(Money::new(Decimal::ZERO, currency), |total, transaction| {
                total
                    .checked_add(transaction.amount())
                    .map_err(|_| PortfolioApplicationServiceError::Analytics)
            })?;
        let value = closing_value
            .amount()
            .checked_sub(external_flow.amount())
            .and_then(|value| value.checked_sub(opening_value.amount()))
            .and_then(|value| value.checked_div(opening_value.amount()))
            .map(|value| value.normalize())
            .ok_or(PortfolioApplicationServiceError::Analytics)?;
        periods.push(PortfolioAnalysisHistoricalReturn {
            opening_revision: opening.token(),
            closing_revision: closing.token(),
            opening_effective_at: opening.effective_at,
            closing_effective_at: closing.effective_at,
            opening_value,
            closing_value,
            external_flow,
            value,
        });
    }
    let required = policy.minimum_historical_return_observations().get();
    if periods.len() < required {
        return Ok(PortfolioAnalysisRiskAvailability::Unavailable(
            PortfolioAnalysisRiskUnavailableReason::InsufficientHistory {
                required,
                available: periods.len(),
            },
        ));
    }
    let losses = periods
        .iter()
        .map(|period| (-period.value).max(Decimal::ZERO))
        .collect::<Vec<_>>();
    let typed_losses = losses
        .iter()
        .copied()
        .map(|loss| {
            StatisticalInput::try_from_decimal(
                loss,
                StatisticalUnit::Return,
                StatisticalScale::Unit,
            )
            .map_err(|_| PortfolioApplicationServiceError::Analytics)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ExactHistoricalRiskResult {
        value_at_risk,
        expected_shortfall,
    } = exact_historical_risk_95(&losses, &typed_losses)?;
    let expected_shortfall_basis_points_ceil = expected_shortfall
        .checked_mul(Decimal::from(10_000_u32))
        .map(|value| value.round_dp_with_strategy(0, RoundingStrategy::ToPositiveInfinity))
        .and_then(|value| value.to_u32())
        .ok_or(PortfolioApplicationServiceError::Analytics)?;
    let budget = u32::from(user_downside_budget_basis_points);
    let risk_capacity_ppm = if expected_shortfall_basis_points_ceil >= budget {
        0
    } else {
        let unused_budget = budget
            .checked_sub(expected_shortfall_basis_points_ceil)
            .ok_or(PortfolioApplicationServiceError::Analytics)?;
        let scaled = u64::from(unused_budget)
            .checked_mul(1_000_000_u64)
            .ok_or(PortfolioApplicationServiceError::Analytics)?;
        let capacity = scaled
            .checked_div(u64::from(budget))
            .ok_or(PortfolioApplicationServiceError::Analytics)?;
        u32::try_from(capacity).map_err(|_| PortfolioApplicationServiceError::Analytics)?
    };
    let mut evidence = PortfolioAnalysisRiskEvidence {
        account_id,
        portfolio_revision: portfolio_revision.clone(),
        profile_digest,
        returns: periods.into_boxed_slice(),
        confidence_basis_points: 9_500,
        value_at_risk,
        expected_shortfall,
        expected_shortfall_basis_points_ceil,
        user_downside_budget_basis_points,
        risk_capacity_ppm,
        policy_digest: policy.digest(),
        evaluated_at: as_of,
        evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
    };
    evidence.evidence_digest = risk_evidence_digest(&evidence);
    Ok(PortfolioAnalysisRiskAvailability::Available(evidence))
}

fn source_reported_total_value(
    revision: &PublishedRevision,
    currency: Currency,
) -> Result<Money, PortfolioApplicationServiceError> {
    if revision.account.currency() != currency
        || revision.account.cash_balance().currency() != currency
    {
        return Err(PortfolioApplicationServiceError::Analytics);
    }
    revision
        .holdings
        .iter()
        .try_fold(revision.account.cash_balance(), |total, holding| {
            if holding.currency() != currency || holding.market_value().currency() != currency {
                return Err(PortfolioApplicationServiceError::Analytics);
            }
            total
                .checked_add(holding.market_value())
                .map_err(|_| PortfolioApplicationServiceError::Analytics)
        })
}

/// Exact-decimal authority for the code-owned 95% nearest-rank VaR and fractional-tail ES policy.
///
/// `StatisticalInput` remains the typed semantic/finiteness admission boundary. Before calculating
/// either result, every admitted input must equal the canonical `Decimal -> f64` projection bit for
/// bit and retain `Return/Unit` semantics. The floating projection is not used for financial
/// arithmetic, so binary rounding cannot become a second result authority or require a tolerance.
struct ExactHistoricalRiskResult {
    value_at_risk: Decimal,
    expected_shortfall: Decimal,
}

fn exact_historical_risk_95(
    losses: &[Decimal],
    typed_losses: &[StatisticalInput],
) -> Result<ExactHistoricalRiskResult, PortfolioApplicationServiceError> {
    if losses.is_empty() || losses.len() != typed_losses.len() {
        return Err(PortfolioApplicationServiceError::Analytics);
    }
    for (loss, typed) in losses.iter().zip(typed_losses) {
        let projected = loss
            .to_f64()
            .ok_or(PortfolioApplicationServiceError::Analytics)?;
        if *loss < Decimal::ZERO
            || typed.unit() != StatisticalUnit::Return
            || typed.source_scale() != StatisticalScale::Unit
            || typed.value().to_bits() != projected.to_bits()
        {
            return Err(PortfolioApplicationServiceError::Analytics);
        }
    }
    let value_at_risk = exact_nearest_rank_95(losses)?;
    let expected_shortfall = exact_discrete_expected_shortfall_95(losses)?;
    if expected_shortfall < value_at_risk {
        return Err(PortfolioApplicationServiceError::Analytics);
    }
    Ok(ExactHistoricalRiskResult {
        value_at_risk,
        expected_shortfall,
    })
}

fn exact_nearest_rank_95(losses: &[Decimal]) -> Result<Decimal, PortfolioApplicationServiceError> {
    if losses.is_empty() {
        return Err(PortfolioApplicationServiceError::Analytics);
    }
    let mut sorted = losses.to_vec();
    sorted.sort_unstable();
    let count = u128::try_from(sorted.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    let rank = count
        .checked_mul(95)
        .and_then(|value| value.checked_add(99))
        .and_then(|value| value.checked_div(100))
        .ok_or(PortfolioApplicationServiceError::Analytics)?;
    let index = usize::try_from(rank.saturating_sub(1))
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    sorted
        .get(index)
        .copied()
        .ok_or(PortfolioApplicationServiceError::Analytics)
}

fn exact_discrete_expected_shortfall_95(
    losses: &[Decimal],
) -> Result<Decimal, PortfolioApplicationServiceError> {
    if losses.is_empty() {
        return Err(PortfolioApplicationServiceError::Analytics);
    }
    let mut sorted = losses.to_vec();
    sorted.sort_unstable_by(|left, right| right.cmp(left));
    let complete = sorted.len() / 20;
    let remainder = sorted.len() % 20;
    let complete_sum = sorted[..complete]
        .iter()
        .try_fold(Decimal::ZERO, |total, value| total.checked_add(*value))
        .ok_or(PortfolioApplicationServiceError::Analytics)?;
    let boundary = if remainder == 0 {
        Decimal::ZERO
    } else {
        sorted
            .get(complete)
            .copied()
            .ok_or(PortfolioApplicationServiceError::Analytics)?
            .checked_mul(Decimal::from(u64::try_from(remainder)?))
            .ok_or(PortfolioApplicationServiceError::Analytics)?
    };
    complete_sum
        .checked_mul(Decimal::from(20_u32))
        .and_then(|value| value.checked_add(boundary))
        .and_then(|value| value.checked_div(Decimal::from(u64::try_from(losses.len()).ok()?)))
        .map(|value| value.normalize())
        .ok_or(PortfolioApplicationServiceError::Analytics)
}

fn available_market(
    markets: &PortfolioAnalysisMarketSet,
    instrument_id: InstrumentId,
) -> Result<
    (
        &PortfolioCandidateMarketEvidence,
        &PortfolioAnalysisLiquidityEvidence,
    ),
    PortfolioApplicationServiceError,
> {
    let entry = markets
        .entry(instrument_id)
        .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
    match entry.availability() {
        PortfolioAnalysisMarketAvailability::Available { market, liquidity } => {
            Ok((market, liquidity))
        }
        PortfolioAnalysisMarketAvailability::Unavailable(_) => {
            Err(PortfolioApplicationServiceError::CorruptPublication)
        }
    }
}

fn calculate_marked_portfolio(
    portfolio: &PortfolioAnalysisPortfolioSnapshot,
    markets: &PortfolioAnalysisMarketSet,
    candidate_instrument_id: InstrumentId,
    evaluated_at: Timestamp,
) -> Result<
    Result<
        PortfolioAnalysisMarkedPortfolioEvidence,
        PortfolioAnalysisPrerequisiteUnavailableReason,
    >,
    PortfolioApplicationServiceError,
> {
    if portfolio.setup() != markets.setup()
        || portfolio.revision() != portfolio.setup().setup().current_head().revision()
        || markets.evaluated_at() != evaluated_at
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    let currency = portfolio.reporting_currency;
    let (candidate_market, _candidate_liquidity) =
        available_market(markets, candidate_instrument_id)?;
    if candidate_market.observation().unit_mark().currency() != currency
        || candidate_market.execution_terms().quote_currency() != currency
    {
        return Ok(Err(
            PortfolioAnalysisPrerequisiteUnavailableReason::ReportingCurrencyMismatch {
                instrument_id: candidate_instrument_id,
            },
        ));
    }
    let mut marked_equity = portfolio.source_cash_balance;
    let mut marked_holdings = Vec::new();
    marked_holdings
        .try_reserve_exact(portfolio.holdings.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    let mut current_position = PortfolioAnalysisCurrentPosition::NoPosition;
    for holding in &portfolio.holdings {
        let (market, _liquidity) = available_market(markets, holding.instrument_id)?;
        if market.observation().unit_mark().currency() != currency
            || market.execution_terms().quote_currency() != currency
        {
            return Ok(Err(
                PortfolioAnalysisPrerequisiteUnavailableReason::ReportingCurrencyMismatch {
                    instrument_id: holding.instrument_id,
                },
            ));
        }
        if holding.lot_size != market.execution_terms().lot_size()
            || !is_lot_aligned(holding.quantity, holding.lot_size.as_decimal())
        {
            return Ok(Err(
                PortfolioAnalysisPrerequisiteUnavailableReason::HoldingExecutionTermsMismatch {
                    instrument_id: holding.instrument_id,
                },
            ));
        }
        let marked_value = marked_value(
            market.observation().unit_mark(),
            holding.quantity,
            market.execution_terms().contract_multiplier(),
        )?;
        marked_equity = marked_equity
            .checked_add(marked_value)
            .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
        if holding.instrument_id == candidate_instrument_id {
            current_position = PortfolioAnalysisCurrentPosition::Position {
                quantity: holding.quantity,
                marked_value,
            };
        }
        marked_holdings.push(PortfolioAnalysisMarkedHolding {
            instrument_id: holding.instrument_id,
            quantity: holding.quantity,
            unit_mark: market.observation().unit_mark(),
            marked_value,
            observation_digest: market.observation().observation_digest(),
            selection_digest: market.selection().receipt_digest(),
            fresh_until: market.observation().fresh_until(),
        });
    }
    if marked_equity.amount() <= Decimal::ZERO {
        return Ok(Err(
            PortfolioAnalysisPrerequisiteUnavailableReason::NonPositiveMarkedEquity,
        ));
    }
    let mut marked = PortfolioAnalysisMarkedPortfolioEvidence {
        candidate_instrument_id,
        source_cash_balance: portfolio.source_cash_balance,
        holdings: marked_holdings.into_boxed_slice(),
        marked_equity,
        current_position,
        portfolio_revision: portfolio.revision.clone(),
        market_set_digest: markets.digest(),
        evaluated_at,
        evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
    };
    marked.evidence_digest = marked_portfolio_digest(&marked);
    Ok(Ok(marked))
}

fn calculate_liquidity_capacity(
    setup: &PortfolioAnalysisSetupSnapshot,
    marked: &PortfolioAnalysisMarkedPortfolioEvidence,
    market: &PortfolioCandidateMarketEvidence,
    liquidity: &PortfolioAnalysisLiquidityEvidence,
) -> Result<PortfolioAnalysisLiquidityCapacityEvidence, PortfolioApplicationServiceError> {
    let currency = setup.setup().profile().reporting_currency();
    let marked_equity = marked.marked_equity;
    if marked_equity.currency() != currency
        || market.observation().unit_mark().currency() != currency
        || market.execution_terms().quote_currency() != currency
    {
        return Err(PortfolioApplicationServiceError::Analytics);
    }
    let upper_weight = Decimal::from(
        setup
            .setup()
            .profile()
            .preferred_position_weight_upper_bps(),
    );
    let upper_notional = marked_equity
        .checked_mul_decimal(upper_weight)
        .and_then(|value| value.checked_mul_decimal(Decimal::new(1, 4)))
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let per_lot_notional = marked_value(
        market.observation().unit_mark(),
        market.execution_terms().lot_size().as_decimal(),
        market.execution_terms().contract_multiplier(),
    )?;
    let max_lots = exact_floor_ratio(upper_notional.amount(), per_lot_notional.amount())?;
    let analytical_upper_weight_notional = per_lot_notional
        .checked_mul_decimal(Decimal::from(max_lots))
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let buy_add = liquidity_capacity_side(
        PortfolioAnalysisLiquiditySide::Ask,
        liquidity.ask(),
        analytical_upper_weight_notional,
        max_lots,
    )?;
    let trim_sell = liquidity_capacity_side(
        PortfolioAnalysisLiquiditySide::Bid,
        liquidity.bid(),
        analytical_upper_weight_notional,
        max_lots,
    )?;
    let mut evidence = PortfolioAnalysisLiquidityCapacityEvidence {
        account_id: setup.setup().selected_account().account_id(),
        portfolio_revision: marked.portfolio_revision.clone(),
        profile_digest: setup.setup().profile().digest(),
        marked_portfolio_digest: marked.evidence_digest,
        market_observation_digest: market.observation().observation_digest(),
        market_selection_digest: market.selection().receipt_digest(),
        buy_add,
        trim_sell,
        quoted_spread_basis_points: liquidity.quoted_spread_basis_points(),
        selected_liquidity_digest: liquidity.evidence_digest(),
        evaluated_at: marked.evaluated_at,
        evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
    };
    evidence.evidence_digest = liquidity_capacity_digest(&evidence);
    Ok(evidence)
}

fn liquidity_capacity_side(
    expected_side: PortfolioAnalysisLiquiditySide,
    depth: &PortfolioAnalysisDepthAvailability<PortfolioAnalysisDepthSideEvidence>,
    analytical_upper_weight_notional: Money,
    analytical_upper_weight_lots: u64,
) -> Result<PortfolioAnalysisLiquidityCapacityAvailability, PortfolioApplicationServiceError> {
    if analytical_upper_weight_lots == 0
        || analytical_upper_weight_notional.amount() <= Decimal::ZERO
    {
        return Ok(PortfolioAnalysisLiquidityCapacityAvailability::Unavailable(
            PortfolioAnalysisLiquidityCapacityUnavailableReason::NoIntegralUpperWeightCapacity,
        ));
    }
    let depth = match depth {
        PortfolioAnalysisDepthAvailability::Available(depth) => depth,
        PortfolioAnalysisDepthAvailability::Unavailable(reason) => {
            return Ok(PortfolioAnalysisLiquidityCapacityAvailability::Unavailable(
                PortfolioAnalysisLiquidityCapacityUnavailableReason::Depth(*reason),
            ));
        }
    };
    if depth.side() != expected_side
        || depth.total_notional().currency() != analytical_upper_weight_notional.currency()
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    let capacity_ppm = exact_floor_ratio_ppm(
        depth.total_notional().amount(),
        analytical_upper_weight_notional.amount(),
    )?;
    Ok(PortfolioAnalysisLiquidityCapacityAvailability::Available(
        PortfolioAnalysisLiquidityCapacitySideEvidence {
            side: expected_side,
            executable_depth_notional: depth.total_notional(),
            analytical_upper_weight_notional,
            analytical_upper_weight_lots,
            capacity_ppm,
            depth_evidence_digest: depth.evidence_digest(),
        },
    ))
}

fn exact_floor_ratio(
    numerator: Decimal,
    denominator: Decimal,
) -> Result<u64, PortfolioApplicationServiceError> {
    let (numerator, denominator) = exact_common_scale(numerator, denominator)?;
    u64::try_from(numerator / denominator)
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)
}

fn exact_floor_ratio_ppm(
    numerator: Decimal,
    denominator: Decimal,
) -> Result<u32, PortfolioApplicationServiceError> {
    let (numerator, denominator) = exact_common_scale(numerator, denominator)?;
    let scaled = numerator
        .min(denominator)
        .checked_mul(1_000_000)
        .ok_or(PortfolioApplicationServiceError::Analytics)?;
    u32::try_from(scaled / denominator).map_err(|_| PortfolioApplicationServiceError::Analytics)
}

fn exact_common_scale(
    left: Decimal,
    right: Decimal,
) -> Result<(u128, u128), PortfolioApplicationServiceError> {
    if left < Decimal::ZERO || right <= Decimal::ZERO {
        return Err(PortfolioApplicationServiceError::Analytics);
    }
    let left = left.normalize();
    let right = right.normalize();
    let scale = left.scale().max(right.scale());
    let left_mantissa =
        u128::try_from(left.mantissa()).map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let right_mantissa = u128::try_from(right.mantissa())
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let left = left_mantissa
        .checked_mul(decimal_power(scale - left.scale())?)
        .ok_or(PortfolioApplicationServiceError::Analytics)?;
    let right = right_mantissa
        .checked_mul(decimal_power(scale - right.scale())?)
        .ok_or(PortfolioApplicationServiceError::Analytics)?;
    if right == 0 {
        return Err(PortfolioApplicationServiceError::Analytics);
    }
    Ok((left, right))
}

fn decimal_power(power: u32) -> Result<u128, PortfolioApplicationServiceError> {
    (0..power).try_fold(1_u128, |value, _| {
        value
            .checked_mul(10)
            .ok_or(PortfolioApplicationServiceError::Analytics)
    })
}

fn marked_value(
    unit_mark: Money,
    quantity: Decimal,
    contract_multiplier: Decimal,
) -> Result<Money, PortfolioApplicationServiceError> {
    unit_mark
        .checked_mul_decimal(quantity)
        .and_then(|value| value.checked_mul_decimal(contract_multiplier))
        .map_err(|_| PortfolioApplicationServiceError::Analytics)
}

fn is_lot_aligned(quantity: Decimal, lot_size: Decimal) -> bool {
    lot_size > Decimal::ZERO
        && quantity
            .checked_rem(lot_size)
            .is_some_and(|remainder| remainder.is_zero())
}

fn prerequisite_unavailable(
    portfolio: PortfolioAnalysisPortfolioSnapshot,
    markets: PortfolioAnalysisMarketSet,
    marked_portfolio: Option<PortfolioAnalysisMarkedPortfolioEvidence>,
    reason: PortfolioAnalysisPrerequisiteUnavailableReason,
    evaluated_at: Timestamp,
) -> PortfolioAnalysisPrerequisiteUnavailableEvidence {
    let mut evidence = PortfolioAnalysisPrerequisiteUnavailableEvidence {
        portfolio,
        markets,
        marked_portfolio,
        reason,
        evaluated_at,
        evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
    };
    evidence.evidence_digest = prerequisite_unavailable_digest(&evidence);
    evidence
}

fn portfolio_snapshot_digest(snapshot: &PortfolioAnalysisPortfolioSnapshot) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-analysis-current-portfolio/v1\0");
    canonical_setup(&mut digest, snapshot.setup());
    digest.update(snapshot.account_id.as_uuid().as_bytes());
    digest.update(snapshot.revision.bytes());
    canonical_text(&mut digest, snapshot.reporting_currency.as_str());
    canonical_money(&mut digest, snapshot.source_cash_balance);
    digest.update(snapshot.effective_at.unix_nanos().to_be_bytes());
    digest.update(snapshot.available_at.unix_nanos().to_be_bytes());
    canonical_text(&mut digest, snapshot.source_id.as_str());
    digest.update((snapshot.source_coverage.len() as u64).to_be_bytes());
    for source in &snapshot.source_coverage {
        canonical_text(&mut digest, source.as_str());
    }
    digest.update(snapshot.artifact_sha256);
    digest.update((snapshot.holdings.len() as u64).to_be_bytes());
    for holding in &snapshot.holdings {
        digest.update(holding.instrument_id.as_uuid().as_bytes());
        canonical_decimal(&mut digest, holding.quantity);
        canonical_decimal(&mut digest, holding.lot_size.as_decimal());
        canonical_money(&mut digest, holding.source_market_value);
        digest.update(holding.observed_at.unix_nanos().to_be_bytes());
        canonical_text(&mut digest, holding.source_reference.as_str());
    }
    canonical_risk_availability(&mut digest, &snapshot.risk);
    canonical_evidence(&mut digest, snapshot.policy.digest());
    canonical_text(&mut digest, ANALYSIS_AUTHORITY);
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn risk_evidence_digest(evidence: &PortfolioAnalysisRiskEvidence) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-analysis-historical-risk/v1\0");
    digest.update(evidence.account_id.as_uuid().as_bytes());
    digest.update(evidence.portfolio_revision.bytes());
    digest.update(evidence.profile_digest);
    digest.update((evidence.returns.len() as u64).to_be_bytes());
    for period in &evidence.returns {
        digest.update(period.opening_revision.bytes());
        digest.update(period.closing_revision.bytes());
        digest.update(period.opening_effective_at.unix_nanos().to_be_bytes());
        digest.update(period.closing_effective_at.unix_nanos().to_be_bytes());
        canonical_money(&mut digest, period.opening_value);
        canonical_money(&mut digest, period.closing_value);
        canonical_money(&mut digest, period.external_flow);
        canonical_decimal(&mut digest, period.value);
    }
    digest.update(evidence.confidence_basis_points.to_be_bytes());
    canonical_decimal(&mut digest, evidence.value_at_risk);
    canonical_decimal(&mut digest, evidence.expected_shortfall);
    digest.update(evidence.expected_shortfall_basis_points_ceil.to_be_bytes());
    digest.update(evidence.user_downside_budget_basis_points.to_be_bytes());
    digest.update(evidence.risk_capacity_ppm.to_be_bytes());
    canonical_evidence(&mut digest, evidence.policy_digest);
    digest.update(evidence.evaluated_at.unix_nanos().to_be_bytes());
    canonical_text(
        &mut digest,
        "exact_decimal_historical_var_discrete_expected_shortfall_95",
    );
    canonical_text(&mut digest, ANALYSIS_AUTHORITY);
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn marked_portfolio_digest(evidence: &PortfolioAnalysisMarkedPortfolioEvidence) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-analysis-complete-current-marks/v1\0");
    digest.update(evidence.candidate_instrument_id.as_uuid().as_bytes());
    canonical_money(&mut digest, evidence.source_cash_balance);
    digest.update((evidence.holdings.len() as u64).to_be_bytes());
    for holding in &evidence.holdings {
        digest.update(holding.instrument_id.as_uuid().as_bytes());
        canonical_decimal(&mut digest, holding.quantity);
        canonical_money(&mut digest, holding.unit_mark);
        canonical_money(&mut digest, holding.marked_value);
        canonical_evidence(&mut digest, holding.observation_digest);
        canonical_evidence(&mut digest, holding.selection_digest);
        digest.update(holding.fresh_until.unix_nanos().to_be_bytes());
    }
    canonical_money(&mut digest, evidence.marked_equity);
    match evidence.current_position {
        PortfolioAnalysisCurrentPosition::NoPosition => digest.update([0]),
        PortfolioAnalysisCurrentPosition::Position {
            quantity,
            marked_value,
        } => {
            digest.update([1]);
            canonical_decimal(&mut digest, quantity);
            canonical_money(&mut digest, marked_value);
        }
    }
    digest.update(evidence.portfolio_revision.bytes());
    canonical_evidence(&mut digest, evidence.market_set_digest);
    digest.update(evidence.evaluated_at.unix_nanos().to_be_bytes());
    canonical_text(&mut digest, ANALYSIS_AUTHORITY);
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn liquidity_capacity_digest(
    evidence: &PortfolioAnalysisLiquidityCapacityEvidence,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-analysis-side-liquidity-capacity/v1\0");
    digest.update(evidence.account_id.as_uuid().as_bytes());
    digest.update(evidence.portfolio_revision.bytes());
    digest.update(evidence.profile_digest);
    canonical_evidence(&mut digest, evidence.marked_portfolio_digest);
    canonical_evidence(&mut digest, evidence.market_observation_digest);
    canonical_evidence(&mut digest, evidence.market_selection_digest);
    canonical_liquidity_capacity(&mut digest, &evidence.buy_add);
    canonical_liquidity_capacity(&mut digest, &evidence.trim_sell);
    match evidence.quoted_spread_basis_points {
        Some(spread) => {
            digest.update([1]);
            digest.update(spread.get().to_be_bytes());
        }
        None => digest.update([0]),
    }
    canonical_evidence(&mut digest, evidence.selected_liquidity_digest);
    digest.update(evidence.evaluated_at.unix_nanos().to_be_bytes());
    canonical_text(
        &mut digest,
        "ask_buy_add_bid_trim_sell_hold_selects_neither",
    );
    canonical_text(&mut digest, ANALYSIS_AUTHORITY);
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn prerequisite_unavailable_digest(
    evidence: &PortfolioAnalysisPrerequisiteUnavailableEvidence,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-analysis-prerequisite-unavailable/v1\0");
    canonical_evidence(&mut digest, evidence.portfolio.evidence_digest);
    canonical_evidence(&mut digest, evidence.markets.digest());
    match &evidence.marked_portfolio {
        Some(marked) => {
            digest.update([1]);
            canonical_evidence(&mut digest, marked.evidence_digest);
        }
        None => digest.update([0]),
    }
    canonical_unavailable_reason(&mut digest, &evidence.reason);
    digest.update(evidence.evaluated_at.unix_nanos().to_be_bytes());
    canonical_text(&mut digest, ANALYSIS_AUTHORITY);
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn recommendation_evidence_digest(evidence: &PortfolioRecommendationEvidence) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-recommendation-evidence/v1\0");
    canonical_evidence(&mut digest, evidence.portfolio.evidence_digest);
    canonical_evidence(&mut digest, evidence.markets.digest());
    canonical_evidence(&mut digest, evidence.marked_portfolio.evidence_digest);
    canonical_evidence(&mut digest, evidence.risk.evidence_digest);
    canonical_evidence(&mut digest, evidence.liquidity_capacity.evidence_digest);
    digest.update(evidence.evaluated_at.unix_nanos().to_be_bytes());
    canonical_text(&mut digest, ANALYSIS_AUTHORITY);
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn canonical_setup(digest: &mut Sha256, setup: &PortfolioAnalysisSetupSnapshot) {
    let resolved = setup.setup();
    digest.update(resolved.authority_revision().to_be_bytes());
    digest.update(resolved.authority_digest());
    digest.update(resolved.configuration_digest());
    digest.update(resolved.profile().digest());
    canonical_evidence(digest, resolved.catalog_digest());
    digest.update(resolved.current_head().revision().bytes());
    digest.update(resolved.as_of().unix_nanos().to_be_bytes());
}

fn canonical_risk_availability(
    digest: &mut Sha256,
    availability: &PortfolioAnalysisRiskAvailability,
) {
    match availability {
        PortfolioAnalysisRiskAvailability::Available(evidence) => {
            digest.update([1]);
            canonical_evidence(digest, evidence.evidence_digest);
        }
        PortfolioAnalysisRiskAvailability::Unavailable(reason) => {
            digest.update([0]);
            canonical_risk_unavailable(digest, reason);
        }
    }
}

fn canonical_risk_unavailable(
    digest: &mut Sha256,
    reason: &PortfolioAnalysisRiskUnavailableReason,
) {
    match reason {
        PortfolioAnalysisRiskUnavailableReason::MissingAvailability { revision } => {
            digest.update([1]);
            digest.update(revision.bytes());
        }
        PortfolioAnalysisRiskUnavailableReason::FutureEvidence { revision } => {
            digest.update([2]);
            digest.update(revision.bytes());
        }
        PortfolioAnalysisRiskUnavailableReason::ReportingCurrencyMismatch { revision } => {
            digest.update([3]);
            digest.update(revision.bytes());
        }
        PortfolioAnalysisRiskUnavailableReason::NonPositiveOpeningValue { revision } => {
            digest.update([4]);
            digest.update(revision.bytes());
        }
        PortfolioAnalysisRiskUnavailableReason::InsufficientHistory {
            required,
            available,
        } => {
            digest.update([5]);
            digest.update((*required as u64).to_be_bytes());
            digest.update((*available as u64).to_be_bytes());
        }
    }
}

fn canonical_liquidity_capacity(
    digest: &mut Sha256,
    availability: &PortfolioAnalysisLiquidityCapacityAvailability,
) {
    match availability {
        PortfolioAnalysisLiquidityCapacityAvailability::Available(evidence) => {
            digest.update([1]);
            canonical_text(digest, evidence.side.as_str());
            canonical_money(digest, evidence.executable_depth_notional);
            canonical_money(digest, evidence.analytical_upper_weight_notional);
            digest.update(evidence.analytical_upper_weight_lots.to_be_bytes());
            digest.update(evidence.capacity_ppm.to_be_bytes());
            canonical_evidence(digest, evidence.depth_evidence_digest);
        }
        PortfolioAnalysisLiquidityCapacityAvailability::Unavailable(reason) => {
            digest.update([0]);
            match reason {
                PortfolioAnalysisLiquidityCapacityUnavailableReason::Depth(reason) => {
                    digest.update([1]);
                    canonical_text(digest, reason.as_str());
                }
                PortfolioAnalysisLiquidityCapacityUnavailableReason::NoIntegralUpperWeightCapacity => {
                    digest.update([2]);
                }
            }
        }
    }
}

fn canonical_unavailable_reason(
    digest: &mut Sha256,
    reason: &PortfolioAnalysisPrerequisiteUnavailableReason,
) {
    match reason {
        PortfolioAnalysisPrerequisiteUnavailableReason::CurrentMarket { instruments } => {
            digest.update([1]);
            digest.update((instruments.len() as u64).to_be_bytes());
            for (instrument, reason) in instruments {
                digest.update(instrument.as_uuid().as_bytes());
                canonical_text(digest, reason.as_str());
            }
        }
        PortfolioAnalysisPrerequisiteUnavailableReason::HistoricalRisk(reason) => {
            digest.update([2]);
            canonical_risk_unavailable(digest, reason);
        }
        PortfolioAnalysisPrerequisiteUnavailableReason::ReportingCurrencyMismatch {
            instrument_id,
        } => {
            digest.update([3]);
            digest.update(instrument_id.as_uuid().as_bytes());
        }
        PortfolioAnalysisPrerequisiteUnavailableReason::HoldingExecutionTermsMismatch {
            instrument_id,
        } => {
            digest.update([4]);
            digest.update(instrument_id.as_uuid().as_bytes());
        }
        PortfolioAnalysisPrerequisiteUnavailableReason::NonPositiveMarkedEquity => {
            digest.update([5]);
        }
    }
}

fn canonical_evidence(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(evidence.bytes());
}

fn canonical_decimal(digest: &mut Sha256, value: Decimal) {
    let value = value.normalize();
    digest.update(value.mantissa().to_be_bytes());
    digest.update(value.scale().to_be_bytes());
}

fn canonical_money(digest: &mut Sha256, value: Money) {
    canonical_decimal(digest, value.amount());
    canonical_text(digest, value.currency().as_str());
}

fn canonical_text(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

fn ensure_analysis_read_live(
    runtime: &Runtime,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), PortfolioApplicationServiceError> {
    if cancellation.is_cancelled()
        || runtime.cancellation.is_cancelled()
        || !runtime.accepting.load(Ordering::Acquire)
    {
        Err(PortfolioApplicationServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(PortfolioApplicationServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
