//! Immutable portfolio revision, evidence, valuation, and read-model types.

use std::collections::{BTreeMap, BTreeSet};

use market_squawk_analytics::{FeatureKey, FeatureMetadata, FeatureSemanticDigest};
use market_squawk_data::{CorporateActionPlan, DatasetManifestRef, Sha256Digest};
use market_squawk_domain::{AccountId, Currency, InstrumentId, Money, SourceIdentifier, Timestamp};
use rust_decimal::Decimal;

use crate::ledger::PortfolioLedger;
use crate::lots::Lot;
use crate::transaction::LedgerEntry;
use crate::{PortfolioError, PortfolioLimits};

/// Opaque content identity of one immutable portfolio revision.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PortfolioRevisionId(pub(crate) [u8; 32]);

/// Opaque caller precondition for read-only service queries.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PortfolioRevisionToken(PortfolioRevisionId);

impl PortfolioRevisionToken {
    /// Returns the stable bytes of the revision identity carried by this precondition.
    pub const fn bytes(&self) -> [u8; 32] {
        self.0.0
    }
}

/// One exact feature contract bound into revision evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureBinding {
    pub(crate) key: FeatureKey,
    pub(crate) semantic_digest: FeatureSemanticDigest,
}

impl FeatureBinding {
    /// Captures the canonical key and full semantic digest of Task 12 metadata.
    pub fn from_metadata(metadata: &FeatureMetadata) -> Self {
        Self {
            key: metadata.key().clone(),
            semantic_digest: metadata.semantic_digest(),
        }
    }

    /// Returns the Task 12 canonical feature key.
    pub const fn key(&self) -> &FeatureKey {
        &self.key
    }

    /// Returns the Task 12 full semantic digest.
    pub const fn semantic_digest(&self) -> FeatureSemanticDigest {
        self.semantic_digest
    }
}

/// Exact binding to one immutable Task 11 corporate-action plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CorporateActionBinding {
    pub(crate) policy_version: u32,
    pub(crate) content_identity: Sha256Digest,
    pub(crate) audit_identity: Sha256Digest,
    pub(crate) knowledge_cutoff: Timestamp,
    pub(crate) valuation_cutoff: Timestamp,
}

impl CorporateActionBinding {
    /// Captures all execution-relevant identities and cutoffs from a Task 11 plan.
    pub fn from_plan(plan: &CorporateActionPlan) -> Self {
        Self {
            policy_version: plan.policy().version().get(),
            content_identity: plan.content_hash(),
            audit_identity: plan.audit_hash(),
            knowledge_cutoff: plan.knowledge_cutoff(),
            valuation_cutoff: plan.valuation_cutoff(),
        }
    }

    /// Returns the corporate-action policy version.
    pub const fn policy_version(self) -> u32 {
        self.policy_version
    }

    /// Returns the usable plan content identity.
    pub const fn content_identity(self) -> Sha256Digest {
        self.content_identity
    }

    /// Returns the complete admitted/excluded plan audit identity.
    pub const fn audit_identity(self) -> Sha256Digest {
        self.audit_identity
    }

    /// Returns the knowledge cutoff.
    pub const fn knowledge_cutoff(self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the valuation cutoff.
    pub const fn valuation_cutoff(self) -> Timestamp {
        self.valuation_cutoff
    }
}

/// Complete immutable source and point-in-time evidence for one revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RevisionEvidence {
    pub(crate) as_of: Timestamp,
    pub(crate) dataset: DatasetManifestRef,
    pub(crate) point_in_time_content: Sha256Digest,
    pub(crate) point_in_time_audit: Sha256Digest,
    pub(crate) sources: Vec<SourceIdentifier>,
    pub(crate) features: Vec<FeatureBinding>,
    pub(crate) corporate_action: Option<CorporateActionBinding>,
}

impl RevisionEvidence {
    /// Constructs bounded evidence without manufacturing canonical Task 10–12 identities.
    ///
    /// # Errors
    ///
    /// Rejects empty, duplicate, or non-canonical source/feature evidence.
    #[allow(
        clippy::too_many_arguments,
        reason = "revision evidence remains explicit"
    )]
    pub fn try_new(
        as_of: Timestamp,
        dataset: DatasetManifestRef,
        point_in_time_content: Sha256Digest,
        point_in_time_audit: Sha256Digest,
        mut sources: Vec<SourceIdentifier>,
        mut features: Vec<FeatureBinding>,
        corporate_action: Option<CorporateActionBinding>,
    ) -> Result<Self, PortfolioError> {
        if sources.is_empty() {
            return Err(PortfolioError::EvidenceMismatch);
        }
        sources.sort_unstable();
        if sources.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(PortfolioError::EvidenceMismatch);
        }
        features.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        if features.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err(PortfolioError::EvidenceMismatch);
        }
        Ok(Self {
            as_of,
            dataset,
            point_in_time_content,
            point_in_time_audit,
            sources,
            features,
            corporate_action,
        })
    }

    /// Returns the valuation and knowledge time of the revision.
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns the exact Task 11 dataset generation.
    pub const fn dataset(&self) -> &DatasetManifestRef {
        &self.dataset
    }

    /// Returns the usable Task 11 point-in-time selection identity.
    pub const fn point_in_time_content(&self) -> Sha256Digest {
        self.point_in_time_content
    }

    /// Returns the complete Task 11 point-in-time audit identity.
    pub const fn point_in_time_audit(&self) -> Sha256Digest {
        self.point_in_time_audit
    }

    /// Returns canonical source identities in stable order.
    pub fn sources(&self) -> &[SourceIdentifier] {
        &self.sources
    }

    /// Returns exact Task 12 feature bindings in key order.
    pub fn features(&self) -> &[FeatureBinding] {
        &self.features
    }

    /// Returns the current corporate-action plan binding when one was supplied.
    pub const fn corporate_action(&self) -> Option<CorporateActionBinding> {
        self.corporate_action
    }
}
/// One exact price with explicit as-of and source provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PriceEvidence {
    pub(crate) instrument_id: InstrumentId,
    pub(crate) price: Money,
    pub(crate) as_of: Timestamp,
    pub(crate) source: SourceIdentifier,
}

impl PriceEvidence {
    /// Constructs a strictly positive instrument price.
    ///
    /// # Errors
    ///
    /// Rejects a nonpositive price.
    pub fn try_new(
        instrument_id: InstrumentId,
        price: Money,
        as_of: Timestamp,
        source: SourceIdentifier,
    ) -> Result<Self, PortfolioError> {
        if price.amount() <= Decimal::ZERO {
            return Err(PortfolioError::InvalidTransaction);
        }
        Ok(Self {
            instrument_id,
            price,
            as_of,
            source,
        })
    }

    /// Returns canonical instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns exact price per unit.
    pub const fn price(&self) -> Money {
        self.price
    }

    /// Returns price observation time.
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns price source identity.
    pub const fn source(&self) -> &SourceIdentifier {
        &self.source
    }
}

/// One exact point-in-time FX conversion into the reporting currency.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FxRateEvidence {
    pub(crate) from: Currency,
    pub(crate) to: Currency,
    pub(crate) rate: Decimal,
    pub(crate) as_of: Timestamp,
    pub(crate) source: SourceIdentifier,
}

impl FxRateEvidence {
    /// Constructs a strictly positive direct FX rate.
    ///
    /// # Errors
    ///
    /// Rejects identity pairs and nonpositive rates.
    pub fn try_new(
        from: Currency,
        to: Currency,
        rate: Decimal,
        as_of: Timestamp,
        source: SourceIdentifier,
    ) -> Result<Self, PortfolioError> {
        if from == to || rate <= Decimal::ZERO {
            return Err(PortfolioError::InvalidTransaction);
        }
        Ok(Self {
            from,
            to,
            rate: rate.normalize(),
            as_of,
            source,
        })
    }

    /// Returns source currency.
    pub const fn from(&self) -> Currency {
        self.from
    }

    /// Returns reporting currency.
    pub const fn to(&self) -> Currency {
        self.to
    }

    /// Returns direct reporting units per source unit.
    pub const fn rate(&self) -> Decimal {
        self.rate
    }

    /// Returns observation time.
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns source identity.
    pub const fn source(&self) -> &SourceIdentifier {
        &self.source
    }
}

/// Complete bounded valuation and FX evidence for one revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValuationSet {
    pub(crate) base_currency: Currency,
    pub(crate) as_of: Timestamp,
    pub(crate) dataset: DatasetManifestRef,
    pub(crate) point_in_time_content: Sha256Digest,
    pub(crate) prices: BTreeMap<InstrumentId, PriceEvidence>,
    pub(crate) fx_rates: BTreeMap<Currency, FxRateEvidence>,
}

impl ValuationSet {
    /// Constructs a canonical price/FX set after checking bounds and source times.
    ///
    /// # Errors
    ///
    /// Rejects duplicate prices/rates, mismatched FX targets/times, or excessive input.
    pub fn try_new(
        base_currency: Currency,
        as_of: Timestamp,
        dataset: DatasetManifestRef,
        point_in_time_content: Sha256Digest,
        prices: Vec<PriceEvidence>,
        fx_rates: Vec<FxRateEvidence>,
        limits: PortfolioLimits,
    ) -> Result<Self, PortfolioError> {
        if prices.len() > limits.max_instruments {
            return Err(PortfolioError::LimitExceeded {
                resource: "prices",
                observed: prices.len(),
                limit: limits.max_instruments,
            });
        }
        if fx_rates.len() > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "fx rates",
                observed: fx_rates.len(),
                limit: limits.max_results,
            });
        }
        let mut price_map = BTreeMap::new();
        for price in prices {
            if price.as_of != as_of || price_map.insert(price.instrument_id, price).is_some() {
                return Err(PortfolioError::EvidenceMismatch);
            }
        }
        let mut fx_map = BTreeMap::new();
        for fx in fx_rates {
            if fx.to != base_currency || fx.as_of != as_of || fx_map.insert(fx.from, fx).is_some() {
                return Err(PortfolioError::EvidenceMismatch);
            }
        }
        Ok(Self {
            base_currency,
            as_of,
            dataset,
            point_in_time_content,
            prices: price_map,
            fx_rates: fx_map,
        })
    }

    /// Returns reporting currency.
    pub const fn base_currency(&self) -> Currency {
        self.base_currency
    }

    /// Returns valuation time.
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns exact Task 11 dataset generation.
    pub const fn dataset(&self) -> &DatasetManifestRef {
        &self.dataset
    }

    /// Returns usable point-in-time content identity.
    pub const fn point_in_time_content(&self) -> Sha256Digest {
        self.point_in_time_content
    }

    /// Returns canonical price observations.
    pub fn prices(&self) -> impl ExactSizeIterator<Item = &PriceEvidence> {
        self.prices.values()
    }

    /// Returns canonical FX observations.
    pub fn fx_rates(&self) -> impl ExactSizeIterator<Item = &FxRateEvidence> {
        self.fx_rates.values()
    }

    pub(crate) fn convert(&self, value: Money) -> Result<Money, PortfolioError> {
        if value.currency() == self.base_currency {
            return Ok(value);
        }
        let rate = self
            .fx_rates
            .get(&value.currency())
            .ok_or(PortfolioError::CurrencyMismatch)?;
        value
            .checked_mul_decimal(rate.rate)
            .map(|money| Money::new(money.amount(), self.base_currency))
            .map_err(|_| PortfolioError::Arithmetic)
    }

    pub(crate) fn market_value(
        &self,
        instrument_id: InstrumentId,
        quantity: Decimal,
    ) -> Result<Money, PortfolioError> {
        let price = self
            .prices
            .get(&instrument_id)
            .ok_or(PortfolioError::MissingPrice)?;
        let value = price
            .price
            .checked_mul_decimal(quantity)
            .map_err(|_| PortfolioError::Arithmetic)?;
        self.convert(value)
    }
}

/// One exact currency cash balance retained without implicit conversion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CashBalance {
    pub(crate) currency: Currency,
    pub(crate) amount: Money,
}

/// Basis-dependent value that is either exact or explicitly unavailable pending allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BasisMeasurement {
    /// Every contributing lot has complete basis evidence.
    Complete(Money),
    /// At least one contributing lot lacks an evidenced basis allocation.
    Incomplete,
}

impl BasisMeasurement {
    /// Returns the exact value only when every contributing basis is complete.
    pub const fn complete(self) -> Option<Money> {
        match self {
            Self::Complete(value) => Some(value),
            Self::Incomplete => None,
        }
    }

    /// Returns whether an exact value is available.
    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Complete(_))
    }
}

impl CashBalance {
    /// Returns balance currency.
    pub const fn currency(self) -> Currency {
        self.currency
    }

    /// Returns signed exact cash.
    pub const fn amount(self) -> Money {
        self.amount
    }
}

/// One revision position aggregated from immutable open lots.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Position {
    pub(crate) instrument_id: InstrumentId,
    pub(crate) quantity: Decimal,
    pub(crate) cost_basis: BasisMeasurement,
    pub(crate) market_value: Money,
    pub(crate) unrealized_gain: BasisMeasurement,
    pub(crate) lots: Vec<Lot>,
}

impl Position {
    /// Returns canonical instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns signed units; short inventory is negative.
    pub const fn quantity(&self) -> Decimal {
        self.quantity
    }

    /// Returns aggregate long basis plus retained short opening proceeds.
    pub const fn cost_basis(&self) -> BasisMeasurement {
        self.cost_basis
    }

    /// Returns signed reporting-currency market value.
    pub const fn market_value(&self) -> Money {
        self.market_value
    }

    /// Returns exact reporting-currency unrealized gain.
    pub const fn unrealized_gain(&self) -> BasisMeasurement {
        self.unrealized_gain
    }

    /// Returns whether every component lot has completely allocated basis evidence.
    pub const fn basis_complete(&self) -> bool {
        self.cost_basis.is_complete()
    }

    /// Returns open lots in deterministic opening order.
    pub fn lots(&self) -> &[Lot] {
        &self.lots
    }
}

/// One published immutable account revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortfolioRevision {
    pub(crate) id: PortfolioRevisionId,
    pub(crate) previous_revision_id: Option<PortfolioRevisionId>,
    pub(crate) account_id: AccountId,
    pub(crate) base_currency: Currency,
    pub(crate) cash: Money,
    pub(crate) cash_balances: Vec<CashBalance>,
    pub(crate) positions: Vec<Position>,
    pub(crate) market_value: Money,
    pub(crate) gross_exposure: Money,
    pub(crate) marked_equity: Money,
    pub(crate) peak_marked_equity: Money,
    pub(crate) cost_basis: BasisMeasurement,
    pub(crate) realized_gain: Money,
    pub(crate) realized_loss: Money,
    pub(crate) unrealized_gain: BasisMeasurement,
    pub(crate) drawdown: Money,
    pub(crate) income: Money,
    pub(crate) withholding: Money,
    pub(crate) fees: Money,
    pub(crate) return_of_capital: Money,
    pub(crate) evidence: RevisionEvidence,
    pub(crate) corporate_actions: Vec<CorporateActionBinding>,
    pub(crate) retained_bytes: usize,
    pub(crate) active_entries: BTreeMap<SourceIdentifier, LedgerEntry>,
    pub(crate) seen_revisions: BTreeSet<(SourceIdentifier, u32)>,
    pub(crate) plan: Option<CorporateActionPlan>,
    pub(crate) limits: PortfolioLimits,
}

impl PortfolioRevision {
    /// Returns opaque immutable revision identity.
    pub const fn id(&self) -> PortfolioRevisionId {
        self.id
    }

    /// Returns an opaque service precondition without exposing its representation.
    pub const fn token(&self) -> PortfolioRevisionToken {
        PortfolioRevisionToken(self.id)
    }

    /// Returns prior immutable revision identity.
    pub const fn previous_revision_id(&self) -> Option<PortfolioRevisionId> {
        self.previous_revision_id
    }

    /// Returns canonical account identity.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns reporting currency.
    pub const fn base_currency(&self) -> Currency {
        self.base_currency
    }

    /// Returns reporting-currency cash using explicit revision FX evidence.
    pub const fn cash(&self) -> Money {
        self.cash
    }

    /// Returns exact currency-native cash balances.
    pub fn cash_balances(&self) -> &[CashBalance] {
        &self.cash_balances
    }

    /// Returns positions in canonical instrument order.
    pub fn positions(&self) -> &[Position] {
        &self.positions
    }

    /// Finds one position by canonical instrument identity.
    pub fn position(&self, instrument_id: InstrumentId) -> Option<&Position> {
        self.positions
            .binary_search_by_key(&instrument_id, Position::instrument_id)
            .ok()
            .and_then(|index| self.positions.get(index))
    }

    /// Returns signed net position market value.
    pub const fn market_value(&self) -> Money {
        self.market_value
    }

    /// Returns the sum of absolute position market values.
    pub const fn gross_exposure(&self) -> Money {
        self.gross_exposure
    }

    /// Returns reporting-currency cash plus signed position market value.
    pub const fn marked_equity(&self) -> Money {
        self.marked_equity
    }

    /// Returns the immutable revision lineage's high-water marked equity.
    pub const fn peak_marked_equity(&self) -> Money {
        self.peak_marked_equity
    }

    /// Returns aggregate open long basis and short opening proceeds.
    pub const fn cost_basis(&self) -> BasisMeasurement {
        self.cost_basis
    }

    /// Returns realized trading and capital-action gain.
    pub const fn realized_gain(&self) -> Money {
        self.realized_gain
    }

    /// Returns cumulative loss magnitude from negative realized outcomes.
    pub const fn realized_loss(&self) -> Money {
        self.realized_loss
    }

    /// Returns current valuation gain over open basis/proceeds.
    pub const fn unrealized_gain(&self) -> BasisMeasurement {
        self.unrealized_gain
    }

    /// Returns current high-water marked-equity drawdown.
    pub const fn drawdown(&self) -> Money {
        self.drawdown
    }

    /// Returns gross dividend and interest income.
    pub const fn income(&self) -> Money {
        self.income
    }

    /// Returns taxes withheld.
    pub const fn withholding(&self) -> Money {
        self.withholding
    }

    /// Returns trade and standalone fees.
    pub const fn fees(&self) -> Money {
        self.fees
    }

    /// Returns gross cash classified as return of capital.
    pub const fn return_of_capital(&self) -> Money {
        self.return_of_capital
    }

    /// Returns immutable source, dataset, feature, and point-in-time evidence.
    pub const fn evidence(&self) -> &RevisionEvidence {
        &self.evidence
    }

    /// Returns the most recently applied corporate-action plan binding.
    pub fn corporate_action_binding(&self) -> Option<&CorporateActionBinding> {
        self.corporate_actions.last()
    }

    /// Returns every applied corporate-action binding in deterministic order.
    pub fn corporate_action_bindings(&self) -> &[CorporateActionBinding] {
        &self.corporate_actions
    }

    /// Returns estimated Rust-visible retained bytes.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Rehydrates mutable publication state from this immutable revision.
    ///
    /// This grants only the ability to build a later portfolio revision. It does not grant order,
    /// approval, dispatch, or live-execution authority.
    pub fn into_ledger(self) -> Result<PortfolioLedger, PortfolioError> {
        let history = vec![self.clone()];
        Ok(PortfolioLedger {
            account_id: self.account_id,
            base_currency: self.base_currency,
            limits: self.limits,
            active_entries: self.active_entries,
            seen_revisions: self.seen_revisions,
            plan: self.plan,
            history,
        })
    }
}
