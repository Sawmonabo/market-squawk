//! Least-authority candidate impact over one exact current portfolio and selected market mark.

use std::fmt;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use market_squawk_domain::{
    AccountId, DataQuality, Denomination, DigestAlgorithm, EvidenceDigest,
    InstrumentExecutionTerms, InstrumentId, Money, SourceId, Timestamp,
};
use market_squawk_portfolio::PortfolioRevisionToken;
use market_squawk_services::{
    RequestContext, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::market_selection::{
    CandidateTimestamps, FreshnessBasis, MarketInvestmentMarkBasis, MarketInvestmentObservation,
    MarketSelectionReceipt,
};

use super::import::hex;
use super::model::PublishedRevision;
use super::{PortfolioApplicationServiceError, Runtime};

const CANDIDATE_IMPACT_POLICY: &str = "selected_market_candidate_impact_v3";
const CANDIDATE_IMPACT_EVIDENCE_SCHEMA_VERSION: u16 = 1;
const PORTFOLIO_VALUE_BASIS: &str = "source_reported_holdings_with_selected_candidate_revalued";
const SCENARIO_SCOPE: &str = "candidate_position_only";
const RISK_AUTHORITY: &str = "analysis_only";
const AUTHORITY_BINDING: &str = concat!(
    "analysis_only;portfolio_mutation=false;execution_authority=false;",
    "risk_authority=analysis_only;risk_approval_required=true;",
    "reservation=false;order=false"
);

/// Closed evidence gaps that cannot be inferred from imported holdings or a selected mark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PortfolioCandidateUnavailableReason {
    /// Every holding requires an exact fresh selected mark from a portfolio-wide producer.
    PortfolioWideSelectedMarks,
    /// Current liquidity must be produced for the exact selected source generation.
    Liquidity,
    /// Cash shown by a generic import is not proof that funds are settled and available to trade.
    SettlementBackedSizing,
    /// No exact fee schedule was bound to this candidate evaluation.
    Fees,
    /// No exact executable liquidity path was available for a slippage estimate.
    Slippage,
    /// No exact factor classification was bound to the current portfolio revision.
    FactorClassification,
}

impl PortfolioCandidateUnavailableReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PortfolioWideSelectedMarks => "portfolio_wide_selected_market_marks",
            Self::Liquidity => "exact_selected_source_liquidity",
            Self::SettlementBackedSizing => "settlement_backed_sizing",
            Self::Fees => "exact_fees",
            Self::Slippage => "exact_slippage",
            Self::FactorClassification => "exact_factor_classification",
        }
    }
}

/// Typed exact evidence or the precise reason it is unavailable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PortfolioCandidateAvailability<T> {
    Available(T),
    Unavailable(PortfolioCandidateUnavailableReason),
}

/// One exact externally produced candidate cost, when a source-specific producer is available.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioCandidateCost {
    amount: Money,
    evidence_digest: EvidenceDigest,
}

impl PortfolioCandidateCost {
    pub(crate) fn try_new(
        amount: Money,
        evidence_digest: EvidenceDigest,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        if amount.amount().is_sign_negative() || evidence_digest.bytes() == [0; 32] {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        Ok(Self {
            amount,
            evidence_digest,
        })
    }

    pub(crate) const fn amount(&self) -> Money {
        self.amount
    }

    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Exact setup identity resolved by the server; clients never provide an account or revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioCandidateSetupBinding {
    account_id: AccountId,
    portfolio_revision: PortfolioRevisionToken,
    reporting_currency: market_squawk_domain::Currency,
    setup_revision: u64,
    setup_digest: [u8; 32],
    configuration_digest: [u8; 32],
    profile_digest: [u8; 32],
    catalog_digest: EvidenceDigest,
}

impl PortfolioCandidateSetupBinding {
    /// Retains the exact selected-account setup and current catalog head resolved by the server.
    pub(crate) fn try_from_resolved_setup(
        setup: &crate::application::recommendation::ResolvedRecommendationSetup,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let selected = setup.selected_account();
        let profile = setup.profile();
        let head = setup.current_head();
        if selected.account_id() != profile.account_id()
            || selected.account_id() != head.account_id()
            || selected.reporting_currency() != profile.reporting_currency()
            || selected.reporting_currency() != head.reporting_currency()
            || setup.authority_revision() == 0
            || setup.authority_digest() == [0; 32]
            || setup.configuration_digest() == [0; 32]
            || profile.digest() == [0; 32]
            || setup.catalog_digest().bytes() == [0; 32]
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        Ok(Self {
            account_id: head.account_id(),
            portfolio_revision: head.revision().clone(),
            reporting_currency: head.reporting_currency(),
            setup_revision: setup.authority_revision(),
            setup_digest: setup.authority_digest(),
            configuration_digest: setup.configuration_digest(),
            profile_digest: profile.digest(),
            catalog_digest: setup.catalog_digest(),
        })
    }

    pub(crate) const fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub(crate) const fn portfolio_revision(&self) -> &PortfolioRevisionToken {
        &self.portfolio_revision
    }

    pub(crate) const fn reporting_currency(&self) -> market_squawk_domain::Currency {
        self.reporting_currency
    }
}

/// Explicit price field selected from the admitted current-market observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PortfolioCandidateMarkKind {
    /// Latest admitted trade price.
    LastTrade,
    /// Exact midpoint calculated from the selected source's admitted bid and ask.
    Midpoint,
}

impl PortfolioCandidateMarkKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::LastTrade => "last_trade",
            Self::Midpoint => "midpoint",
        }
    }
}

/// Exact source-selection facts retained beside a candidate reference mark.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioCandidateSourceSelection {
    instrument_id: InstrumentId,
    source_id: SourceId,
    policy_revision: u32,
    policy_digest: EvidenceDigest,
    receipt_digest: EvidenceDigest,
    source_state_revision: Option<u64>,
    selected_at: Timestamp,
}

impl PortfolioCandidateSourceSelection {
    /// Test-local raw construction; production evidence is admitted only through the typed factory.
    fn try_new(
        instrument_id: InstrumentId,
        source_id: SourceId,
        policy_revision: u32,
        policy_digest: EvidenceDigest,
        receipt_digest: EvidenceDigest,
        source_state_revision: u64,
        selected_at: Timestamp,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        if policy_revision == 0 {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        Ok(Self {
            instrument_id,
            source_id,
            policy_revision,
            policy_digest,
            receipt_digest,
            source_state_revision: Some(source_state_revision),
            selected_at,
        })
    }

    fn from_market_selection(
        receipt: &MarketSelectionReceipt,
        observation: MarketInvestmentObservation<'_, '_>,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let selected = receipt
            .selected()
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        let identity = selected.candidate().identity();
        if receipt.policy_revision() == 0
            || receipt.selection_digest() != observation.selection_digest()
            || receipt.selected_at() != observation.selected_at()
            || identity != observation.selected_source().candidate().identity()
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        Ok(Self {
            instrument_id: observation.instrument_id(),
            source_id: identity.source_id().clone(),
            policy_revision: receipt.policy_revision(),
            policy_digest: receipt.policy_digest(),
            receipt_digest: receipt.selection_digest(),
            source_state_revision: observation.generation().map(|generation| generation.get()),
            selected_at: observation.selected_at(),
        })
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn policy_revision(&self) -> u32 {
        self.policy_revision
    }

    pub(crate) const fn policy_digest(&self) -> EvidenceDigest {
        self.policy_digest
    }

    pub(crate) const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }

    pub(crate) const fn source_state_revision(&self) -> Option<u64> {
        self.source_state_revision
    }

    pub(crate) const fn selected_at(&self) -> Timestamp {
        self.selected_at
    }
}

/// One exact current-market observation before it is bound to a portfolio revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioCandidateMarketObservation {
    instrument_id: InstrumentId,
    unit_mark: Money,
    mark_kind: PortfolioCandidateMarkKind,
    quality: DataQuality,
    source_id: SourceId,
    observation_digest: EvidenceDigest,
    observed_at: Timestamp,
    available_at: Timestamp,
    fresh_until: Timestamp,
}

impl PortfolioCandidateMarketObservation {
    /// Test-local raw construction; production evidence is admitted only through the typed factory.
    #[allow(
        clippy::too_many_arguments,
        reason = "the complete selected mark identity and time boundary remain explicit"
    )]
    fn try_new(
        instrument_id: InstrumentId,
        unit_mark: Money,
        mark_kind: PortfolioCandidateMarkKind,
        source_id: SourceId,
        observation_digest: EvidenceDigest,
        observed_at: Timestamp,
        available_at: Timestamp,
        fresh_until: Timestamp,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        Self::try_new_with_quality(
            instrument_id,
            unit_mark,
            mark_kind,
            DataQuality::DirectVerified,
            source_id,
            observation_digest,
            observed_at,
            available_at,
            fresh_until,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the complete selected mark identity and time boundary remain explicit"
    )]
    fn try_new_with_quality(
        instrument_id: InstrumentId,
        unit_mark: Money,
        mark_kind: PortfolioCandidateMarkKind,
        quality: DataQuality,
        source_id: SourceId,
        observation_digest: EvidenceDigest,
        observed_at: Timestamp,
        available_at: Timestamp,
        fresh_until: Timestamp,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        if unit_mark.amount() <= Decimal::ZERO
            || observed_at > available_at
            || available_at >= fresh_until
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        Ok(Self {
            instrument_id,
            unit_mark,
            mark_kind,
            quality,
            source_id,
            observation_digest,
            observed_at,
            available_at,
            fresh_until,
        })
    }

    fn from_market_selection(
        receipt: &MarketSelectionReceipt,
        observation: MarketInvestmentObservation<'_, '_>,
        source_id: SourceId,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let mark = observation.mark();
        let mark_kind = match mark.basis() {
            MarketInvestmentMarkBasis::FreshLastTrade => PortfolioCandidateMarkKind::LastTrade,
            MarketInvestmentMarkBasis::FreshBidAskMidpoint => PortfolioCandidateMarkKind::Midpoint,
        };
        let timestamps = observation.timestamps();
        let freshness = receipt.request().freshness();
        let freshness_anchor = match freshness.basis() {
            FreshnessBasis::Source => timestamps.source_timestamp(),
            FreshnessBasis::Effective => Some(timestamps.effective_at()),
            FreshnessBasis::Received => Some(timestamps.received_at()),
            FreshnessBasis::Available => Some(timestamps.available_at()),
            FreshnessBasis::Ingested => Some(timestamps.ingested_at()),
        }
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        let maximum_age = i64::try_from(freshness.maximum_age_nanos())
            .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
        let policy_fresh_until = freshness_anchor
            .checked_add_nanos(maximum_age)
            .and_then(|inclusive| inclusive.checked_add_nanos(1))
            .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
        let source_fresh_until = mark
            .fresh_until()
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
            .checked_add_nanos(1)
            .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
        let fresh_until = source_fresh_until.min(policy_fresh_until);
        let unit_mark = Money::new(mark.value(), mark.currency());
        let observation_digest = market_observation_digest(
            receipt.selection_digest(),
            mark.evidence_identity(),
            observation.instrument_id(),
            unit_mark,
            mark_kind,
            observation.quality(),
            &source_id,
            observation.generation().map(|generation| generation.get()),
            timestamps,
            freshness.basis(),
            freshness.maximum_age_nanos(),
            fresh_until,
        );
        Self::try_new_with_quality(
            observation.instrument_id(),
            unit_mark,
            mark_kind,
            observation.quality(),
            source_id,
            observation_digest,
            timestamps.effective_at(),
            timestamps.available_at(),
            fresh_until,
        )
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn unit_mark(&self) -> Money {
        self.unit_mark
    }

    pub(crate) const fn mark_kind(&self) -> PortfolioCandidateMarkKind {
        self.mark_kind
    }

    pub(crate) const fn quality(&self) -> DataQuality {
        self.quality
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn observation_digest(&self) -> EvidenceDigest {
        self.observation_digest
    }

    pub(crate) const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    pub(crate) const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    pub(crate) const fn fresh_until(&self) -> Timestamp {
        self.fresh_until
    }
}

/// A current-market observation bound to its selected source and exact portfolio revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioCandidateMarketEvidence {
    observation: PortfolioCandidateMarketObservation,
    selection: PortfolioCandidateSourceSelection,
    execution_terms: InstrumentExecutionTerms,
    fees: PortfolioCandidateAvailability<PortfolioCandidateCost>,
    slippage: PortfolioCandidateAvailability<PortfolioCandidateCost>,
    portfolio_revision: PortfolioRevisionToken,
}

/// Server-resolved setup, selected-market observation, and exact financial terms.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioCandidateResolution {
    setup: PortfolioCandidateSetupBinding,
    market: PortfolioCandidateMarketEvidence,
}

impl PortfolioCandidateResolution {
    /// Builds the only production candidate input from already-resolved typed authorities.
    pub(crate) fn try_from_authorities(
        setup: &crate::application::recommendation::ResolvedRecommendationSetup,
        receipt: &MarketSelectionReceipt,
        observation: MarketInvestmentObservation<'_, '_>,
        execution_terms: InstrumentExecutionTerms,
        fees: PortfolioCandidateAvailability<PortfolioCandidateCost>,
        slippage: PortfolioCandidateAvailability<PortfolioCandidateCost>,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let setup = PortfolioCandidateSetupBinding::try_from_resolved_setup(setup)?;
        let market = PortfolioCandidateMarketEvidence::try_from_market_selection(
            receipt,
            observation,
            execution_terms,
            fees,
            slippage,
            setup.portfolio_revision.clone(),
        )?;
        Ok(Self { setup, market })
    }

    pub(crate) const fn setup(&self) -> &PortfolioCandidateSetupBinding {
        &self.setup
    }

    pub(crate) const fn market(&self) -> &PortfolioCandidateMarketEvidence {
        &self.market
    }
}

/// Injected least-authority seam that resolves and rechecks server-owned candidate evidence.
///
/// Implementations join the durable selected-account setup, complete current account catalog,
/// unified market selection, exact [`MarketInvestmentObservation`], and instrument terms. They
/// must not inspect a paper account or create execution state.
#[async_trait]
pub(crate) trait PortfolioCandidateResolutionAuthority: Send + Sync {
    async fn resolve(
        &self,
        instrument_id: InstrumentId,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PortfolioCandidateResolution, PortfolioApplicationServiceError>;

    async fn recheck(
        &self,
        expected: &PortfolioCandidateResolution,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), PortfolioApplicationServiceError>;
}

impl PortfolioCandidateMarketEvidence {
    /// Test-local raw binding; production evidence is admitted only through the typed factory.
    fn try_new(
        observation: PortfolioCandidateMarketObservation,
        selection: PortfolioCandidateSourceSelection,
        execution_terms: InstrumentExecutionTerms,
        fees: PortfolioCandidateAvailability<PortfolioCandidateCost>,
        slippage: PortfolioCandidateAvailability<PortfolioCandidateCost>,
        portfolio_revision: PortfolioRevisionToken,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        if observation.instrument_id != selection.instrument_id
            || observation.source_id != selection.source_id
            || execution_terms.instrument_id() != observation.instrument_id
            || execution_terms.quote_currency() != observation.unit_mark.currency()
            || observation.available_at > selection.selected_at
            || selection.selected_at >= observation.fresh_until
            || !cost_evidence_matches(
                &fees,
                PortfolioCandidateUnavailableReason::Fees,
                observation.unit_mark.currency(),
            )
            || !cost_evidence_matches(
                &slippage,
                PortfolioCandidateUnavailableReason::Slippage,
                observation.unit_mark.currency(),
            )
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        Ok(Self {
            observation,
            selection,
            execution_terms,
            fees,
            slippage,
            portfolio_revision,
        })
    }

    /// Extracts an exact non-executable mark from the same retained source-selection receipt.
    ///
    /// No caller-authored source identity, mark, generation, observation digest, or freshness
    /// boundary crosses this factory.
    pub(crate) fn try_from_market_selection(
        receipt: &MarketSelectionReceipt,
        observation: MarketInvestmentObservation<'_, '_>,
        execution_terms: InstrumentExecutionTerms,
        fees: PortfolioCandidateAvailability<PortfolioCandidateCost>,
        slippage: PortfolioCandidateAvailability<PortfolioCandidateCost>,
        portfolio_revision: PortfolioRevisionToken,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        if portfolio_revision.bytes() == [0; 32]
            || receipt.selection_digest() != observation.selection_digest()
            || receipt.selected_at() != observation.selected_at()
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let selection =
            PortfolioCandidateSourceSelection::from_market_selection(receipt, observation)?;
        let market_observation = PortfolioCandidateMarketObservation::from_market_selection(
            receipt,
            observation,
            selection.source_id.clone(),
        )?;
        Self::try_new(
            market_observation,
            selection,
            execution_terms,
            fees,
            slippage,
            portfolio_revision,
        )
    }

    pub(crate) const fn observation(&self) -> &PortfolioCandidateMarketObservation {
        &self.observation
    }

    pub(crate) const fn selection(&self) -> &PortfolioCandidateSourceSelection {
        &self.selection
    }

    pub(crate) const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.execution_terms
    }

    pub(crate) const fn fees(&self) -> &PortfolioCandidateAvailability<PortfolioCandidateCost> {
        &self.fees
    }

    pub(crate) const fn slippage(&self) -> &PortfolioCandidateAvailability<PortfolioCandidateCost> {
        &self.slippage
    }

    pub(crate) const fn portfolio_revision(&self) -> &PortfolioRevisionToken {
        &self.portfolio_revision
    }
}

/// Read-only request to preview one candidate against a current imported portfolio.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioCandidateImpactRequest {
    setup: PortfolioCandidateSetupBinding,
    instrument_id: InstrumentId,
    proposed_quantity: Decimal,
    scenario_shock: Decimal,
    evaluated_at: Timestamp,
    market_evidence: PortfolioCandidateMarketEvidence,
}

impl PortfolioCandidateImpactRequest {
    /// Constructs a request whose selected mark is fresh at the exact evaluation instant.
    pub(crate) fn try_new(
        setup: PortfolioCandidateSetupBinding,
        instrument_id: InstrumentId,
        proposed_quantity: Decimal,
        scenario_shock: Decimal,
        evaluated_at: Timestamp,
        market_evidence: PortfolioCandidateMarketEvidence,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let observation = &market_evidence.observation;
        let selection = &market_evidence.selection;
        if instrument_id != observation.instrument_id
            || instrument_id != selection.instrument_id
            || instrument_id != market_evidence.execution_terms.instrument_id()
            || setup.portfolio_revision != market_evidence.portfolio_revision
            || setup.reporting_currency != observation.unit_mark.currency()
            || proposed_quantity.is_sign_negative()
            || !is_lot_aligned(
                proposed_quantity,
                market_evidence.execution_terms.lot_size().as_decimal(),
            )
            || scenario_shock < -Decimal::ONE
            || evaluated_at < selection.selected_at
            || evaluated_at >= observation.fresh_until
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        Ok(Self {
            setup,
            instrument_id,
            proposed_quantity: proposed_quantity.normalize(),
            scenario_shock: scenario_shock.normalize(),
            evaluated_at,
            market_evidence,
        })
    }

    /// Converts only a server-resolved authority join into a typed analytical request.
    pub(crate) fn try_from_resolution(
        resolution: PortfolioCandidateResolution,
        proposed_quantity: Decimal,
        scenario_shock: Decimal,
        evaluated_at: Timestamp,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let instrument_id = resolution.market.observation.instrument_id;
        Self::try_new(
            resolution.setup,
            instrument_id,
            proposed_quantity,
            scenario_shock,
            evaluated_at,
            resolution.market,
        )
    }

    pub(crate) const fn account_id(&self) -> AccountId {
        self.setup.account_id
    }

    pub(crate) const fn setup(&self) -> &PortfolioCandidateSetupBinding {
        &self.setup
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn proposed_quantity(&self) -> Decimal {
        self.proposed_quantity
    }

    pub(crate) const fn scenario_shock(&self) -> Decimal {
        self.scenario_shock
    }

    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    pub(crate) const fn market_evidence(&self) -> &PortfolioCandidateMarketEvidence {
        &self.market_evidence
    }
}

/// Whether the exact portfolio revision already contains the selected instrument.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PortfolioCandidatePositionState {
    /// The candidate is not currently held; its current exposure is exactly zero.
    ZeroPosition,
    /// The candidate is already held and is revalued using the pinned current mark.
    ExistingHolding,
}

impl PortfolioCandidatePositionState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ZeroPosition => "zero_position",
            Self::ExistingHolding => "existing_holding",
        }
    }
}

/// Closed checks in the reusable imported-portfolio risk advisory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ImportedPortfolioRiskCheck {
    SelectedAccount,
    CurrentPortfolioRevision,
    FreshSelectedMark,
    InstrumentTerms,
    PositionLotAlignment,
    PortfolioWideSelectedMarks,
    Liquidity,
    SettlementBackedSizing,
    Fees,
    Slippage,
}

impl ImportedPortfolioRiskCheck {
    const fn as_str(self) -> &'static str {
        match self {
            Self::SelectedAccount => "selected_account",
            Self::CurrentPortfolioRevision => "current_portfolio_revision",
            Self::FreshSelectedMark => "fresh_selected_mark",
            Self::InstrumentTerms => "instrument_terms",
            Self::PositionLotAlignment => "position_lot_alignment",
            Self::PortfolioWideSelectedMarks => "portfolio_wide_selected_marks",
            Self::Liquidity => "liquidity",
            Self::SettlementBackedSizing => "settlement_backed_sizing",
            Self::Fees => "fees",
            Self::Slippage => "slippage",
        }
    }
}

/// A risk advisory is never an approval and cannot reserve account or execution capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ImportedPortfolioRiskAdvisoryOutcome {
    IndeterminateAtEvaluation,
}

/// Reusable non-reserving risk evidence for an imported portfolio candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ImportedPortfolioRiskAdvisory {
    outcome: ImportedPortfolioRiskAdvisoryOutcome,
    evaluated: Box<[ImportedPortfolioRiskCheck]>,
    unavailable: Box<[ImportedPortfolioRiskCheck]>,
    evaluated_at: Timestamp,
    digest: EvidenceDigest,
}

impl ImportedPortfolioRiskAdvisory {
    pub(crate) const fn outcome(&self) -> ImportedPortfolioRiskAdvisoryOutcome {
        self.outcome
    }

    pub(crate) const fn evaluated(&self) -> &[ImportedPortfolioRiskCheck] {
        &self.evaluated
    }

    pub(crate) const fn unavailable(&self) -> &[ImportedPortfolioRiskCheck] {
        &self.unavailable
    }

    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    pub(crate) const fn digest(&self) -> EvidenceDigest {
        self.digest
    }

    /// Evaluates only the checks supported by exact imported-portfolio and market evidence.
    ///
    /// This pure advisory cannot reserve capacity, approve risk, mutate a portfolio, or create an
    /// order. Missing execution evidence remains explicit and keeps the outcome indeterminate.
    pub(crate) fn evaluate(request: &PortfolioCandidateImpactRequest) -> Self {
        let mut evaluated = vec![
            ImportedPortfolioRiskCheck::SelectedAccount,
            ImportedPortfolioRiskCheck::CurrentPortfolioRevision,
            ImportedPortfolioRiskCheck::FreshSelectedMark,
            ImportedPortfolioRiskCheck::InstrumentTerms,
            ImportedPortfolioRiskCheck::PositionLotAlignment,
        ];
        let mut unavailable = vec![
            ImportedPortfolioRiskCheck::PortfolioWideSelectedMarks,
            ImportedPortfolioRiskCheck::Liquidity,
            ImportedPortfolioRiskCheck::SettlementBackedSizing,
        ];
        match &request.market_evidence.fees {
            PortfolioCandidateAvailability::Available(_) => {
                evaluated.push(ImportedPortfolioRiskCheck::Fees);
            }
            PortfolioCandidateAvailability::Unavailable(_) => {
                unavailable.push(ImportedPortfolioRiskCheck::Fees);
            }
        }
        match &request.market_evidence.slippage {
            PortfolioCandidateAvailability::Available(_) => {
                evaluated.push(ImportedPortfolioRiskCheck::Slippage);
            }
            PortfolioCandidateAvailability::Unavailable(_) => {
                unavailable.push(ImportedPortfolioRiskCheck::Slippage);
            }
        }
        let mut advisory = Self {
            outcome: ImportedPortfolioRiskAdvisoryOutcome::IndeterminateAtEvaluation,
            evaluated: evaluated.into_boxed_slice(),
            unavailable: unavailable.into_boxed_slice(),
            evaluated_at: request.evaluated_at,
            digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/imported-portfolio-risk-advisory/v1\0");
        digest.update(request.setup.setup_digest);
        digest.update(request.setup.configuration_digest);
        digest.update(request.setup.profile_digest);
        digest.update(request.setup.catalog_digest.bytes());
        digest.update(
            request
                .market_evidence
                .observation
                .observation_digest
                .bytes(),
        );
        digest.update(request.market_evidence.portfolio_revision.bytes());
        digest.update(request.evaluated_at.unix_nanos().to_be_bytes());
        for check in &advisory.evaluated {
            canonical_text(&mut digest, check.as_str());
        }
        digest.update([0]);
        for check in &advisory.unavailable {
            canonical_text(&mut digest, check.as_str());
        }
        canonical_text(&mut digest, "analysis_only_no_reservation_no_order");
        advisory.digest = EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into());
        advisory
    }

    fn structured_content(&self) -> Value {
        json!({
            "outcome": "indeterminate_at_evaluation",
            "evaluatedAtUnixNanos": self.evaluated_at.unix_nanos().to_string(),
            "checksEvaluated": self.evaluated.iter().map(|check| check.as_str()).collect::<Vec<_>>(),
            "checksUnavailable": self.unavailable.iter().map(|check| check.as_str()).collect::<Vec<_>>(),
            "evidenceDigest": {
                "algorithm": self.digest.algorithm(),
                "bytes": hex(&self.digest.bytes()),
            },
            "authority": "analysis_only",
            "reservation": false,
            "order": false,
        })
    }
}

/// Calculated, non-mutating exposure and scenario impact for one candidate allocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioCandidateImpactPreview {
    setup: PortfolioCandidateSetupBinding,
    account_id: AccountId,
    revision: PortfolioRevisionToken,
    portfolio_effective_at: Timestamp,
    portfolio_available_at: Timestamp,
    portfolio_source_id: SourceId,
    portfolio_source_coverage: Vec<SourceId>,
    portfolio_artifact_sha256: [u8; 32],
    instrument_id: InstrumentId,
    position_state: PortfolioCandidatePositionState,
    current_quantity: Decimal,
    proposed_quantity: Decimal,
    current_market_value: Money,
    proposed_market_value: Money,
    portfolio_value: Money,
    current_weight: Decimal,
    proposed_weight: Decimal,
    weight_change: Decimal,
    capital_change: Money,
    scenario_shock: Decimal,
    current_scenario_impact: Money,
    proposed_scenario_impact: Money,
    marginal_scenario_impact: Money,
    market_evidence: PortfolioCandidateMarketEvidence,
    advisory: ImportedPortfolioRiskAdvisory,
    evaluated_at: Timestamp,
    evidence_digest: EvidenceDigest,
}

impl PortfolioCandidateImpactPreview {
    pub(crate) const fn setup(&self) -> &PortfolioCandidateSetupBinding {
        &self.setup
    }

    pub(crate) const fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub(crate) const fn revision(&self) -> &PortfolioRevisionToken {
        &self.revision
    }

    pub(crate) const fn portfolio_effective_at(&self) -> Timestamp {
        self.portfolio_effective_at
    }

    pub(crate) const fn portfolio_available_at(&self) -> Timestamp {
        self.portfolio_available_at
    }

    pub(crate) const fn portfolio_source_id(&self) -> &SourceId {
        &self.portfolio_source_id
    }

    pub(crate) fn portfolio_source_coverage(&self) -> &[SourceId] {
        &self.portfolio_source_coverage
    }

    pub(crate) const fn portfolio_artifact_sha256(&self) -> [u8; 32] {
        self.portfolio_artifact_sha256
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns whether this is a new position or an adjustment to an existing holding.
    pub(crate) const fn position_state(&self) -> PortfolioCandidatePositionState {
        self.position_state
    }

    /// Returns the exact current marked exposure.
    pub(crate) const fn current_market_value(&self) -> Money {
        self.current_market_value
    }

    pub(crate) const fn current_quantity(&self) -> Decimal {
        self.current_quantity
    }

    pub(crate) const fn proposed_quantity(&self) -> Decimal {
        self.proposed_quantity
    }

    /// Returns the exact proposed marked exposure.
    pub(crate) const fn proposed_market_value(&self) -> Money {
        self.proposed_market_value
    }

    pub(crate) const fn portfolio_value(&self) -> Money {
        self.portfolio_value
    }

    pub(crate) const fn current_weight(&self) -> Decimal {
        self.current_weight
    }

    pub(crate) const fn proposed_weight(&self) -> Decimal {
        self.proposed_weight
    }

    pub(crate) const fn weight_change(&self) -> Decimal {
        self.weight_change
    }

    pub(crate) const fn capital_change(&self) -> Money {
        self.capital_change
    }

    pub(crate) const fn scenario_shock(&self) -> Decimal {
        self.scenario_shock
    }

    pub(crate) const fn current_scenario_impact(&self) -> Money {
        self.current_scenario_impact
    }

    pub(crate) const fn proposed_scenario_impact(&self) -> Money {
        self.proposed_scenario_impact
    }

    pub(crate) const fn marginal_scenario_impact(&self) -> Money {
        self.marginal_scenario_impact
    }

    pub(crate) const fn market_evidence(&self) -> &PortfolioCandidateMarketEvidence {
        &self.market_evidence
    }

    pub(crate) const fn advisory(&self) -> &ImportedPortfolioRiskAdvisory {
        &self.advisory
    }

    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    /// Returns the canonical identity of every calculated result and admitted input binding.
    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }

    /// Produces the bounded presentation payload without granting mutation or execution authority.
    pub(crate) fn structured_content(&self) -> Value {
        let observation = &self.market_evidence.observation;
        let selection = &self.market_evidence.selection;
        let terms = self.market_evidence.execution_terms;
        let fee_evidence = match &self.market_evidence.fees {
            PortfolioCandidateAvailability::Available(cost) => json!({
                "status": "available",
                "amount": money_value(cost.amount()),
                "evidenceDigest": {
                    "algorithm": cost.evidence_digest().algorithm(),
                    "bytes": hex(&cost.evidence_digest().bytes()),
                },
            }),
            PortfolioCandidateAvailability::Unavailable(reason) => json!({
                "status": "unavailable",
                "reason": reason.as_str(),
            }),
        };
        let slippage_evidence = match &self.market_evidence.slippage {
            PortfolioCandidateAvailability::Available(cost) => json!({
                "status": "available",
                "amount": money_value(cost.amount()),
                "evidenceDigest": {
                    "algorithm": cost.evidence_digest().algorithm(),
                    "bytes": hex(&cost.evidence_digest().bytes()),
                },
            }),
            PortfolioCandidateAvailability::Unavailable(reason) => json!({
                "status": "unavailable",
                "reason": reason.as_str(),
            }),
        };
        let setup_evidence = json!({
            "setupRevision": self.setup.setup_revision.to_string(),
            "setupDigest": hex(&self.setup.setup_digest),
            "configurationDigest": hex(&self.setup.configuration_digest),
            "profileDigest": hex(&self.setup.profile_digest),
            "catalogDigest": hex(&self.setup.catalog_digest.bytes()),
        });
        let evidence_digest = json!({
            "algorithm": self.evidence_digest.algorithm(),
            "bytes": hex(&self.evidence_digest.bytes()),
        });
        let portfolio_evidence = json!({
            "revisionId": hex(&self.revision.bytes()),
            "effectiveAtUnixNanos": self.portfolio_effective_at.unix_nanos().to_string(),
            "availableAtUnixNanos": self.portfolio_available_at.unix_nanos().to_string(),
            "sourceId": self.portfolio_source_id.as_str(),
            "sourceCoverage": self.portfolio_source_coverage
                .iter()
                .map(SourceId::as_str)
                .collect::<Vec<_>>(),
            "artifactSha256": hex(&self.portfolio_artifact_sha256),
        });
        let instrument_terms = json!({
            "definitionRevision": terms.definition_revision().get().to_string(),
            "priceTick": terms.price_tick().as_decimal().to_string(),
            "lotSize": terms.lot_size().as_decimal().to_string(),
            "quoteCurrency": terms.quote_currency().as_str(),
            "settlementDenomination": denomination_value(terms.settlement_denomination()),
            "contractMultiplier": terms.contract_multiplier().to_string(),
        });
        let cost_evidence = json!({
            "fees": fee_evidence,
            "slippage": slippage_evidence,
        });
        let concentration = json!({
            "current": self.current_weight.to_string(),
            "proposed": self.proposed_weight.to_string(),
            "change": self.weight_change.to_string(),
        });
        let scenario = json!({
            "scope": SCENARIO_SCOPE,
            "shock": self.scenario_shock.to_string(),
            "currentImpact": money_value(self.current_scenario_impact),
            "proposedImpact": money_value(self.proposed_scenario_impact),
            "marginalImpact": money_value(self.marginal_scenario_impact),
        });
        let selection_evidence = json!({
            "instrumentId": selection.instrument_id.to_string(),
            "sourceId": selection.source_id.as_str(),
            "policyRevision": selection.policy_revision,
            "policyDigest": {
                "algorithm": selection.policy_digest.algorithm(),
                "bytes": hex(&selection.policy_digest.bytes()),
            },
            "receiptDigest": {
                "algorithm": selection.receipt_digest.algorithm(),
                "bytes": hex(&selection.receipt_digest.bytes()),
            },
            "sourceStateRevision": selection
                .source_state_revision
                .map(|revision| revision.to_string()),
            "selectedAtUnixNanos": selection.selected_at.unix_nanos().to_string(),
        });
        let mark_evidence = json!({
            "status": "fresh_selected_market_observation",
            "instrumentId": observation.instrument_id.to_string(),
            "unitMark": money_value(observation.unit_mark),
            "markKind": observation.mark_kind.as_str(),
            "quality": data_quality_str(observation.quality),
            "sourceId": observation.source_id.as_str(),
            "observationDigest": {
                "algorithm": observation.observation_digest.algorithm(),
                "bytes": hex(&observation.observation_digest.bytes()),
            },
            "observedAtUnixNanos": observation.observed_at.unix_nanos().to_string(),
            "availableAtUnixNanos": observation.available_at.unix_nanos().to_string(),
            "freshUntilUnixNanosExclusive": observation.fresh_until.unix_nanos().to_string(),
            "evaluatedAtUnixNanos": self.evaluated_at.unix_nanos().to_string(),
            "portfolioRevisionId": hex(&self.market_evidence.portfolio_revision.bytes()),
            "selection": selection_evidence,
        });
        let availability = json!({
            "portfolioWideSelectedMarks": {
                "status": "unavailable",
                "reason": PortfolioCandidateUnavailableReason::PortfolioWideSelectedMarks.as_str(),
            },
            "liquidity": {
                "status": "unavailable",
                "reason": PortfolioCandidateUnavailableReason::Liquidity.as_str(),
            },
            "settlementBackedSizing": {
                "status": "unavailable",
                "reason": PortfolioCandidateUnavailableReason::SettlementBackedSizing.as_str(),
            },
            "factorClassification": {
                "status": "unavailable",
                "reason": PortfolioCandidateUnavailableReason::FactorClassification.as_str(),
            },
        });
        let authority = json!({
            "analysisOnly": true,
            "portfolioMutation": false,
            "executionAuthority": false,
            "riskAuthority": RISK_AUTHORITY,
            "reservation": false,
            "order": false,
            "riskApprovalRequiredBeforeAnyOrder": true,
        });
        json!({
            "accountId": self.account_id.to_string(),
            "revisionId": hex(&self.revision.bytes()),
            "setupEvidence": setup_evidence,
            "policy": CANDIDATE_IMPACT_POLICY,
            "evidenceSchemaVersion": CANDIDATE_IMPACT_EVIDENCE_SCHEMA_VERSION,
            "evidenceDigest": evidence_digest,
            "portfolioEvidence": portfolio_evidence,
            "instrumentId": self.instrument_id.to_string(),
            "positionState": self.position_state.as_str(),
            "currentQuantity": self.current_quantity.to_string(),
            "proposedQuantity": self.proposed_quantity.to_string(),
            "currentMarketValue": money_value(self.current_market_value),
            "proposedMarketValue": money_value(self.proposed_market_value),
            "capitalChange": money_value(self.capital_change),
            "portfolioValue": money_value(self.portfolio_value),
            "portfolioValueBasis": PORTFOLIO_VALUE_BASIS,
            "instrumentTerms": instrument_terms,
            "costEvidence": cost_evidence,
            "concentration": concentration,
            "scenario": scenario,
            "markEvidence": mark_evidence,
            "availability": availability,
            "riskAdvisory": self.advisory.structured_content(),
            "authority": authority,
        })
    }

    fn canonical_evidence_digest(&self) -> EvidenceDigest {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/portfolio-candidate-impact-evidence/v1\0");
        digest.update(CANDIDATE_IMPACT_EVIDENCE_SCHEMA_VERSION.to_be_bytes());
        canonical_text(&mut digest, CANDIDATE_IMPACT_POLICY);
        canonical_text(&mut digest, PORTFOLIO_VALUE_BASIS);
        canonical_text(&mut digest, SCENARIO_SCOPE);
        for unavailable in [
            PortfolioCandidateUnavailableReason::PortfolioWideSelectedMarks,
            PortfolioCandidateUnavailableReason::Liquidity,
            PortfolioCandidateUnavailableReason::SettlementBackedSizing,
            PortfolioCandidateUnavailableReason::FactorClassification,
        ] {
            canonical_text(&mut digest, unavailable.as_str());
        }
        canonical_text(&mut digest, AUTHORITY_BINDING);
        digest.update(self.account_id.as_uuid().as_bytes());
        digest.update(self.setup.setup_revision.to_be_bytes());
        digest.update(self.setup.setup_digest);
        digest.update(self.setup.configuration_digest);
        digest.update(self.setup.profile_digest);
        canonical_evidence_identity(&mut digest, self.setup.catalog_digest);
        digest.update(self.revision.bytes());
        digest.update(self.portfolio_effective_at.unix_nanos().to_be_bytes());
        digest.update(self.portfolio_available_at.unix_nanos().to_be_bytes());
        canonical_text(&mut digest, self.portfolio_source_id.as_str());
        digest.update((self.portfolio_source_coverage.len() as u64).to_be_bytes());
        for source in &self.portfolio_source_coverage {
            canonical_text(&mut digest, source.as_str());
        }
        digest.update(self.portfolio_artifact_sha256);
        digest.update(self.instrument_id.as_uuid().as_bytes());
        canonical_text(&mut digest, self.position_state.as_str());
        canonical_decimal(&mut digest, self.current_quantity);
        canonical_decimal(&mut digest, self.proposed_quantity);
        canonical_money(&mut digest, self.current_market_value);
        canonical_money(&mut digest, self.proposed_market_value);
        canonical_money(&mut digest, self.capital_change);
        canonical_money(&mut digest, self.portfolio_value);
        canonical_decimal(&mut digest, self.current_weight);
        canonical_decimal(&mut digest, self.proposed_weight);
        canonical_decimal(&mut digest, self.weight_change);
        canonical_decimal(&mut digest, self.scenario_shock);
        canonical_money(&mut digest, self.current_scenario_impact);
        canonical_money(&mut digest, self.proposed_scenario_impact);
        canonical_money(&mut digest, self.marginal_scenario_impact);
        digest.update(self.evaluated_at.unix_nanos().to_be_bytes());
        digest.update(self.advisory.digest.bytes());

        let observation = &self.market_evidence.observation;
        digest.update(observation.instrument_id.as_uuid().as_bytes());
        canonical_money(&mut digest, observation.unit_mark);
        canonical_text(&mut digest, observation.mark_kind.as_str());
        digest.update([data_quality_tag(observation.quality)]);
        canonical_text(&mut digest, observation.source_id.as_str());
        canonical_evidence_identity(&mut digest, observation.observation_digest);
        digest.update(observation.observed_at.unix_nanos().to_be_bytes());
        digest.update(observation.available_at.unix_nanos().to_be_bytes());
        digest.update(observation.fresh_until.unix_nanos().to_be_bytes());

        let selection = &self.market_evidence.selection;
        digest.update(selection.instrument_id.as_uuid().as_bytes());
        canonical_text(&mut digest, selection.source_id.as_str());
        digest.update(selection.policy_revision.to_be_bytes());
        canonical_evidence_identity(&mut digest, selection.policy_digest);
        canonical_evidence_identity(&mut digest, selection.receipt_digest);
        canonical_optional_u64(&mut digest, selection.source_state_revision);
        digest.update(selection.selected_at.unix_nanos().to_be_bytes());
        canonical_execution_terms(&mut digest, self.market_evidence.execution_terms);
        for cost in [&self.market_evidence.fees, &self.market_evidence.slippage] {
            match cost {
                PortfolioCandidateAvailability::Available(cost) => {
                    digest.update([1]);
                    canonical_money(&mut digest, cost.amount);
                    canonical_evidence_identity(&mut digest, cost.evidence_digest);
                }
                PortfolioCandidateAvailability::Unavailable(reason) => {
                    digest.update([0]);
                    canonical_text(&mut digest, reason.as_str());
                }
            }
        }
        digest.update(self.market_evidence.portfolio_revision.bytes());
        EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
    }
}

/// Cloneable least-authority reader used by the unified instrument workspace.
#[derive(Clone)]
pub(crate) struct PortfolioCandidateImpactReadCapability {
    pub(super) runtime: Arc<Runtime>,
}

impl PortfolioCandidateImpactReadCapability {
    /// Evaluates one candidate against the exact current immutable portfolio revision.
    ///
    /// The method fails closed if the portfolio revision changed, the mark is stale or mismatched,
    /// or the reporting currency differs. Imported cash is never treated as settlement capacity.
    pub(crate) fn preview_current(
        &self,
        request: &PortfolioCandidateImpactRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PortfolioCandidateImpactPreview, PortfolioApplicationServiceError> {
        let _guard = self.runtime.admit()?;
        ensure_read_live(&self.runtime, deadline, cancellation)?;
        let image = self.runtime.image.load_full();
        let revision = image
            .accounts
            .get(&request.account_id())
            .and_then(|history| history.revisions.last())
            .ok_or(PortfolioApplicationServiceError::NotFound)?;
        if image
            .revisions
            .head(request.account_id())
            .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?
            != revision.token()
        {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        let state = CandidatePortfolioState::from_revision(revision, request)?;
        let preview = calculate_preview(state, request)?;
        ensure_read_live(&self.runtime, deadline, cancellation)?;
        let final_image = self.runtime.image.load_full();
        if !Arc::ptr_eq(&image, &final_image) {
            return Err(PortfolioApplicationServiceError::StateChanged);
        }
        let final_revision = final_image
            .accounts
            .get(&request.account_id())
            .and_then(|history| history.revisions.last())
            .ok_or(PortfolioApplicationServiceError::StateChanged)?;
        if final_revision.token() != request.market_evidence.portfolio_revision.clone()
            || final_image
                .revisions
                .head(request.account_id())
                .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?
                != final_revision.token()
        {
            return Err(PortfolioApplicationServiceError::StateChanged);
        }
        ensure_read_live(&self.runtime, deadline, cancellation)?;
        Ok(preview)
    }
}

impl fmt::Debug for PortfolioCandidateImpactReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PortfolioCandidateImpactReadCapability")
            .field("authority", &"[IMMUTABLE PORTFOLIO READ IMAGE]")
            .finish()
    }
}

pub(super) async fn call_resolved_candidate_impact(
    authority: &Arc<dyn PortfolioCandidateResolutionAuthority>,
    reader: &PortfolioCandidateImpactReadCapability,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    ensure_read_live(&reader.runtime, context.deadline(), context.cancellation())?;
    let instrument_id = request
        .arguments()
        .get("instrumentId")
        .and_then(Value::as_str)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
        .parse::<InstrumentId>()
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
    let proposed_quantity = request
        .arguments()
        .get("proposedQuantity")
        .and_then(Value::as_str)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
        .parse::<Decimal>()
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
    let scenario_shock = request
        .arguments()
        .get("scenarioShock")
        .and_then(Value::as_str)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
        .parse::<Decimal>()
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
    let evaluated_at = current_timestamp()?;
    let resolution = authority
        .resolve(
            instrument_id,
            evaluated_at,
            context.deadline(),
            context.cancellation().clone(),
        )
        .await?;
    let expected = resolution.clone();
    let request = PortfolioCandidateImpactRequest::try_from_resolution(
        resolution,
        proposed_quantity,
        scenario_shock,
        evaluated_at,
    )?;
    let preview = reader.preview_current(&request, context.deadline(), context.cancellation())?;
    authority
        .recheck(
            &expected,
            evaluated_at,
            context.deadline(),
            context.cancellation().clone(),
        )
        .await?;
    ensure_read_live(&reader.runtime, context.deadline(), context.cancellation())?;
    let metadata = ToolResultMetadata::try_complete(
        json!({
            "portfolio": preview.portfolio_source_coverage()
                .iter()
                .map(SourceId::as_str)
                .collect::<Vec<_>>(),
            "market": preview.market_evidence().selection().source_id().as_str(),
        }),
        json!({
            "portfolio": "direct_unverified",
            "market": data_quality_str(preview.market_evidence().observation().quality()),
            "executionEligible": false,
        }),
    )
    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    TypedToolResult::try_new(preview.structured_content(), 1, metadata, context.limits())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CandidatePortfolioState {
    account_id: AccountId,
    revision: PortfolioRevisionToken,
    effective_at: Timestamp,
    available_at: Timestamp,
    source_id: SourceId,
    source_coverage: Vec<SourceId>,
    artifact_sha256: [u8; 32],
    currency: market_squawk_domain::Currency,
    portfolio_value: Money,
    position_state: PortfolioCandidatePositionState,
    current_quantity: Decimal,
    current_market_value: Money,
}

impl CandidatePortfolioState {
    fn from_revision(
        revision: &PublishedRevision,
        request: &PortfolioCandidateImpactRequest,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let evidence = &request.market_evidence;
        let currency = revision.account.currency();
        let available_at = revision
            .available_at
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        if revision.account.account_id() != request.account_id()
            || revision.token() != evidence.portfolio_revision.clone()
            || revision.effective_at > request.evaluated_at
            || available_at > request.evaluated_at
            || evidence.observation.unit_mark.currency() != currency
            || request.setup.reporting_currency != currency
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }

        let mut position_state = PortfolioCandidatePositionState::ZeroPosition;
        let mut current_quantity = Decimal::ZERO;
        let mut current_market_value = Money::new(Decimal::ZERO, currency);
        let mut seen_candidate = false;
        let mut portfolio_value = revision.account.cash_balance();
        if portfolio_value.currency() != currency {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        for holding in &revision.holdings {
            if holding.currency() != currency || holding.market_value().currency() != currency {
                return Err(PortfolioApplicationServiceError::Analytics);
            }
            let value = if holding.instrument_id() == request.instrument_id {
                if seen_candidate {
                    return Err(PortfolioApplicationServiceError::CorruptPublication);
                }
                seen_candidate = true;
                position_state = PortfolioCandidatePositionState::ExistingHolding;
                current_quantity = holding.quantity().as_decimal();
                if holding.lot_size() != evidence.execution_terms.lot_size()
                    || !is_lot_aligned(
                        current_quantity,
                        evidence.execution_terms.lot_size().as_decimal(),
                    )
                {
                    return Err(PortfolioApplicationServiceError::Analytics);
                }
                current_market_value = marked_value(
                    evidence.observation.unit_mark,
                    current_quantity,
                    evidence.execution_terms.contract_multiplier(),
                )?;
                current_market_value
            } else {
                holding.market_value()
            };
            portfolio_value = portfolio_value
                .checked_add(value)
                .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
        }
        if portfolio_value.amount() <= Decimal::ZERO {
            return Err(PortfolioApplicationServiceError::Analytics);
        }
        Ok(Self {
            account_id: request.account_id(),
            revision: revision.token(),
            effective_at: revision.effective_at,
            available_at,
            source_id: revision.source_id.clone(),
            source_coverage: revision.source_coverage.clone(),
            artifact_sha256: revision.artifact_sha256,
            currency,
            portfolio_value,
            position_state,
            current_quantity,
            current_market_value,
        })
    }
}

fn calculate_preview(
    state: CandidatePortfolioState,
    request: &PortfolioCandidateImpactRequest,
) -> Result<PortfolioCandidateImpactPreview, PortfolioApplicationServiceError> {
    if state.account_id != request.account_id()
        || &state.revision != &request.market_evidence.portfolio_revision
        || state.currency != request.setup.reporting_currency
        || request.instrument_id != request.market_evidence.observation.instrument_id
        || request.instrument_id != request.market_evidence.selection.instrument_id
        || request.market_evidence.observation.source_id
            != request.market_evidence.selection.source_id
        || request.evaluated_at < request.market_evidence.selection.selected_at
        || request.evaluated_at >= request.market_evidence.observation.fresh_until
    {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    let proposed_market_value = marked_value(
        request.market_evidence.observation.unit_mark,
        request.proposed_quantity,
        request
            .market_evidence
            .execution_terms
            .contract_multiplier(),
    )?;
    let capital_change = proposed_market_value
        .checked_sub(state.current_market_value)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let current_weight = checked_div(
        state.current_market_value.amount(),
        state.portfolio_value.amount(),
    )?;
    let proposed_weight = checked_div(
        proposed_market_value.amount(),
        state.portfolio_value.amount(),
    )?;
    let weight_change = proposed_weight
        .checked_sub(current_weight)
        .map(|value| value.normalize())
        .ok_or(PortfolioApplicationServiceError::Analytics)?;
    let current_scenario_impact = state
        .current_market_value
        .checked_mul_decimal(request.scenario_shock)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let proposed_scenario_impact = proposed_market_value
        .checked_mul_decimal(request.scenario_shock)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let marginal_scenario_impact = proposed_scenario_impact
        .checked_sub(current_scenario_impact)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let mut preview = PortfolioCandidateImpactPreview {
        setup: request.setup.clone(),
        account_id: request.account_id(),
        revision: state.revision,
        portfolio_effective_at: state.effective_at,
        portfolio_available_at: state.available_at,
        portfolio_source_id: state.source_id,
        portfolio_source_coverage: state.source_coverage,
        portfolio_artifact_sha256: state.artifact_sha256,
        instrument_id: request.instrument_id,
        position_state: state.position_state,
        current_quantity: state.current_quantity,
        proposed_quantity: request.proposed_quantity,
        current_market_value: state.current_market_value,
        proposed_market_value,
        portfolio_value: state.portfolio_value,
        current_weight,
        proposed_weight,
        weight_change,
        capital_change,
        scenario_shock: request.scenario_shock,
        current_scenario_impact,
        proposed_scenario_impact,
        marginal_scenario_impact,
        market_evidence: request.market_evidence.clone(),
        advisory: ImportedPortfolioRiskAdvisory::evaluate(request),
        evaluated_at: request.evaluated_at,
        evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
    };
    preview.evidence_digest = preview.canonical_evidence_digest();
    Ok(preview)
}

fn checked_div(
    numerator: Decimal,
    denominator: Decimal,
) -> Result<Decimal, PortfolioApplicationServiceError> {
    numerator
        .checked_div(denominator)
        .map(|value| value.normalize())
        .ok_or(PortfolioApplicationServiceError::Analytics)
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

fn cost_evidence_matches(
    cost: &PortfolioCandidateAvailability<PortfolioCandidateCost>,
    unavailable_reason: PortfolioCandidateUnavailableReason,
    currency: market_squawk_domain::Currency,
) -> bool {
    match cost {
        PortfolioCandidateAvailability::Available(cost) => cost.amount.currency() == currency,
        PortfolioCandidateAvailability::Unavailable(reason) => *reason == unavailable_reason,
    }
}

fn is_lot_aligned(quantity: Decimal, lot_size: Decimal) -> bool {
    lot_size > Decimal::ZERO
        && quantity
            .checked_rem(lot_size)
            .is_some_and(|remainder| remainder.is_zero())
}

fn money_value(value: Money) -> Value {
    json!({
        "amount": value.amount().to_string(),
        "currency": value.currency().as_str(),
    })
}

fn denomination_value(value: Denomination) -> Value {
    match value {
        Denomination::Currency(currency) => json!({
            "kind": "currency",
            "currency": currency.as_str(),
        }),
        Denomination::Asset(instrument_id) => json!({
            "kind": "asset",
            "instrumentId": instrument_id.to_string(),
        }),
    }
}

fn canonical_text(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

fn canonical_decimal(digest: &mut Sha256, value: Decimal) {
    canonical_text(digest, &value.normalize().to_string());
}

fn canonical_money(digest: &mut Sha256, value: Money) {
    canonical_decimal(digest, value.amount());
    canonical_text(digest, value.currency().as_str());
}

fn canonical_execution_terms(digest: &mut Sha256, terms: InstrumentExecutionTerms) {
    digest.update(terms.instrument_id().as_uuid().as_bytes());
    digest.update(terms.definition_revision().get().to_be_bytes());
    canonical_decimal(digest, terms.price_tick().as_decimal());
    canonical_decimal(digest, terms.lot_size().as_decimal());
    canonical_text(digest, terms.quote_currency().as_str());
    match terms.settlement_denomination() {
        Denomination::Currency(currency) => {
            digest.update([1]);
            canonical_text(digest, currency.as_str());
        }
        Denomination::Asset(instrument_id) => {
            digest.update([2]);
            digest.update(instrument_id.as_uuid().as_bytes());
        }
    }
    canonical_decimal(digest, terms.contract_multiplier());
}

fn canonical_evidence_identity(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(evidence.bytes());
}

fn canonical_optional_u64(digest: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        None => digest.update([0]),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the digest binds the complete typed selected-mark and freshness evidence"
)]
fn market_observation_digest(
    selection_digest: EvidenceDigest,
    mark_evidence_digest: EvidenceDigest,
    instrument_id: InstrumentId,
    unit_mark: Money,
    mark_kind: PortfolioCandidateMarkKind,
    quality: DataQuality,
    source_id: &SourceId,
    source_state_revision: Option<u64>,
    timestamps: CandidateTimestamps,
    freshness_basis: FreshnessBasis,
    maximum_age_nanos: u64,
    fresh_until: Timestamp,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-candidate-market-observation/v1\0");
    canonical_evidence_identity(&mut digest, selection_digest);
    canonical_evidence_identity(&mut digest, mark_evidence_digest);
    digest.update(instrument_id.as_uuid().as_bytes());
    canonical_money(&mut digest, unit_mark);
    canonical_text(&mut digest, mark_kind.as_str());
    digest.update([data_quality_tag(quality)]);
    canonical_text(&mut digest, source_id.as_str());
    canonical_optional_u64(&mut digest, source_state_revision);
    digest.update(timestamps.effective_at().unix_nanos().to_be_bytes());
    match timestamps.source_timestamp() {
        Some(source_timestamp) => {
            digest.update([1]);
            digest.update(source_timestamp.unix_nanos().to_be_bytes());
        }
        None => digest.update([0]),
    }
    digest.update(timestamps.received_at().unix_nanos().to_be_bytes());
    digest.update(timestamps.available_at().unix_nanos().to_be_bytes());
    digest.update(timestamps.ingested_at().unix_nanos().to_be_bytes());
    digest.update([freshness_basis_tag(freshness_basis)]);
    digest.update(maximum_age_nanos.to_be_bytes());
    digest.update(fresh_until.unix_nanos().to_be_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

const fn freshness_basis_tag(basis: FreshnessBasis) -> u8 {
    match basis {
        FreshnessBasis::Source => 1,
        FreshnessBasis::Effective => 2,
        FreshnessBasis::Received => 3,
        FreshnessBasis::Available => 4,
        FreshnessBasis::Ingested => 5,
    }
}

const fn data_quality_tag(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}

const fn data_quality_str(quality: DataQuality) -> &'static str {
    match quality {
        DataQuality::DirectVerified => "direct_verified",
        DataQuality::DirectUnverified => "direct_unverified",
        DataQuality::OfficialDelayed => "official_delayed",
        DataQuality::Aggregated => "aggregated",
        DataQuality::Indicative => "indicative",
        DataQuality::Modeled => "modeled",
        DataQuality::Estimated => "estimated",
        DataQuality::Stale => "stale",
        DataQuality::Quarantined => "quarantined",
    }
}

fn ensure_read_live(
    runtime: &Runtime,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), PortfolioApplicationServiceError> {
    if cancellation.is_cancelled() || runtime.cancellation.is_cancelled() {
        return Err(PortfolioApplicationServiceError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(PortfolioApplicationServiceError::DeadlineExceeded);
    }
    Ok(())
}

fn current_timestamp() -> Result<Timestamp, PortfolioApplicationServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PortfolioApplicationServiceError::Authority)?;
    let nanos = i64::try_from(elapsed.as_nanos())
        .map_err(|_| PortfolioApplicationServiceError::Authority)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

#[cfg(test)]
mod tests {
    use market_squawk_domain::{
        AccountId, Currency, Denomination, DigestAlgorithm, EvidenceDigest,
        InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId, LotSize, Money,
        SourceId, TickSize, Timestamp,
    };
    use market_squawk_portfolio::PortfolioRevisionToken;
    use rust_decimal::Decimal;

    use super::{
        CandidatePortfolioState, PortfolioCandidateAvailability, PortfolioCandidateImpactRequest,
        PortfolioCandidateMarkKind, PortfolioCandidateMarketEvidence,
        PortfolioCandidateMarketObservation, PortfolioCandidatePositionState,
        PortfolioCandidateSetupBinding, PortfolioCandidateSourceSelection,
        PortfolioCandidateUnavailableReason, calculate_preview, hex,
    };
    use crate::PortfolioApplicationServiceError;

    #[test]
    fn new_position_preview_requires_matching_fresh_market_and_portfolio_evidence() {
        let account_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
            .parse::<AccountId>()
            .expect("valid account");
        let instrument_id = "22222222-2222-4222-8222-222222222222"
            .parse::<InstrumentId>()
            .expect("valid instrument");
        let other_instrument = "33333333-3333-4333-8333-333333333333"
            .parse::<InstrumentId>()
            .expect("valid instrument");
        let currency = Currency::try_from("USD").expect("valid currency");
        let source_id = SourceId::try_from("selected-us-equity-source").expect("valid source");
        let other_source = SourceId::try_from("different-source").expect("valid source");
        let portfolio_source = SourceId::try_from("portfolio-import-source").expect("valid source");
        let revision = PortfolioRevisionToken::from_bytes([7; 32]);
        let valid_request = request(
            account_id,
            instrument_id,
            currency,
            Timestamp::from_unix_nanos(115),
            evidence(
                instrument_id,
                source_id.clone(),
                instrument_id,
                source_id.clone(),
                currency,
                revision.clone(),
            )
            .expect("matching evidence"),
        )
        .expect("fresh request");
        let state = CandidatePortfolioState {
            account_id,
            revision: revision.clone(),
            effective_at: Timestamp::from_unix_nanos(90),
            available_at: Timestamp::from_unix_nanos(95),
            source_id: portfolio_source.clone(),
            source_coverage: vec![portfolio_source],
            artifact_sha256: [6; 32],
            currency,
            portfolio_value: Money::new(Decimal::from(1_000), currency),
            position_state: PortfolioCandidatePositionState::ZeroPosition,
            current_quantity: Decimal::ZERO,
            current_market_value: Money::new(Decimal::ZERO, currency),
        };

        let preview =
            calculate_preview(state.clone(), &valid_request).expect("new position preview");
        let content = preview.structured_content();
        assert_eq!(
            preview.position_state(),
            PortfolioCandidatePositionState::ZeroPosition
        );
        assert_eq!(content["concentration"]["proposed"], "0.25");
        assert_eq!(content["scenario"]["marginalImpact"]["amount"], "-50");
        assert_eq!(content["authority"]["portfolioMutation"], false);
        assert_eq!(content["authority"]["executionAuthority"], false);
        assert_eq!(content["authority"]["reservation"], false);
        assert_eq!(
            content["riskAdvisory"]["outcome"],
            "indeterminate_at_evaluation"
        );
        assert_eq!(content["instrumentTerms"]["lotSize"], "1");
        assert_eq!(
            content["instrumentTerms"]["settlementDenomination"],
            serde_json::json!({
                "kind": "asset",
                "instrumentId": instrument_id.to_string(),
            })
        );
        assert_eq!(
            content["evidenceDigest"]["bytes"],
            hex(&preview.evidence_digest().bytes())
        );
        let repeated =
            calculate_preview(state.clone(), &valid_request).expect("deterministic preview");
        let mut tampered = preview.clone();
        tampered.marginal_scenario_impact = Money::new(Decimal::from(-51), currency);
        assert_eq!(preview.evidence_digest(), repeated.evidence_digest());
        assert_ne!(
            preview.evidence_digest(),
            tampered.canonical_evidence_digest()
        );

        assert!(matches!(
            evidence(
                instrument_id,
                source_id.clone(),
                other_instrument,
                source_id.clone(),
                currency,
                revision.clone()
            ),
            Err(PortfolioApplicationServiceError::InvalidRequest)
        ));
        assert!(matches!(
            evidence(
                instrument_id,
                source_id.clone(),
                instrument_id,
                other_source,
                currency,
                revision.clone()
            ),
            Err(PortfolioApplicationServiceError::InvalidRequest)
        ));

        assert!(matches!(
            request(
                account_id,
                instrument_id,
                currency,
                Timestamp::from_unix_nanos(120),
                evidence(
                    instrument_id,
                    source_id.clone(),
                    instrument_id,
                    source_id.clone(),
                    currency,
                    revision.clone(),
                )
                .expect("matching evidence"),
            ),
            Err(PortfolioApplicationServiceError::InvalidRequest)
        ));
        assert!(matches!(
            request(
                account_id,
                instrument_id,
                Currency::try_from("EUR").expect("valid currency"),
                Timestamp::from_unix_nanos(115),
                evidence(
                    instrument_id,
                    source_id.clone(),
                    instrument_id,
                    source_id.clone(),
                    currency,
                    revision.clone(),
                )
                .expect("matching evidence"),
            ),
            Err(PortfolioApplicationServiceError::InvalidRequest)
        ));

        let wrong_revision_request = request(
            account_id,
            instrument_id,
            currency,
            Timestamp::from_unix_nanos(115),
            evidence(
                instrument_id,
                source_id.clone(),
                instrument_id,
                source_id,
                currency,
                PortfolioRevisionToken::from_bytes([8; 32]),
            )
            .expect("internally consistent alternate revision"),
        )
        .expect("fresh alternate revision request");
        assert!(matches!(
            calculate_preview(state, &wrong_revision_request),
            Err(PortfolioApplicationServiceError::InvalidRequest)
        ));
    }

    fn evidence(
        observation_instrument: InstrumentId,
        observation_source: SourceId,
        selection_instrument: InstrumentId,
        selection_source: SourceId,
        currency: Currency,
        revision: PortfolioRevisionToken,
    ) -> Result<PortfolioCandidateMarketEvidence, PortfolioApplicationServiceError> {
        PortfolioCandidateMarketEvidence::try_new(
            PortfolioCandidateMarketObservation::try_new(
                observation_instrument,
                Money::new(Decimal::from(25), currency),
                PortfolioCandidateMarkKind::Midpoint,
                observation_source,
                EvidenceDigest::new(DigestAlgorithm::Sha256, [8; 32]),
                Timestamp::from_unix_nanos(100),
                Timestamp::from_unix_nanos(105),
                Timestamp::from_unix_nanos(120),
            )?,
            PortfolioCandidateSourceSelection::try_new(
                selection_instrument,
                selection_source,
                1,
                EvidenceDigest::new(DigestAlgorithm::Sha256, [9; 32]),
                EvidenceDigest::new(DigestAlgorithm::Sha256, [10; 32]),
                4,
                Timestamp::from_unix_nanos(110),
            )?,
            terms(observation_instrument, currency),
            PortfolioCandidateAvailability::Unavailable(PortfolioCandidateUnavailableReason::Fees),
            PortfolioCandidateAvailability::Unavailable(
                PortfolioCandidateUnavailableReason::Slippage,
            ),
            revision,
        )
    }

    fn request(
        account_id: AccountId,
        instrument_id: InstrumentId,
        currency: Currency,
        evaluated_at: Timestamp,
        evidence: PortfolioCandidateMarketEvidence,
    ) -> Result<PortfolioCandidateImpactRequest, PortfolioApplicationServiceError> {
        let revision = evidence.portfolio_revision.clone();
        PortfolioCandidateImpactRequest::try_new(
            PortfolioCandidateSetupBinding {
                account_id,
                portfolio_revision: revision,
                reporting_currency: currency,
                setup_revision: 1,
                setup_digest: [11; 32],
                configuration_digest: [12; 32],
                profile_digest: [13; 32],
                catalog_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [14; 32]),
            },
            instrument_id,
            Decimal::from(10),
            Decimal::new(-2, 1),
            evaluated_at,
            evidence,
        )
    }

    fn terms(instrument_id: InstrumentId, currency: Currency) -> InstrumentExecutionTerms {
        InstrumentExecutionTerms::try_new(
            instrument_id,
            InstrumentDefinitionRevision::try_from(1_u64).expect("valid definition revision"),
            TickSize::try_from_decimal(Decimal::new(1, 2)).expect("valid tick"),
            LotSize::try_from_decimal(Decimal::ONE).expect("valid lot"),
            currency,
            Denomination::Asset(instrument_id),
            Decimal::ONE,
        )
        .expect("valid execution terms")
    }
}
