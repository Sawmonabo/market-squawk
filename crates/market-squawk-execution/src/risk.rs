//! Deterministic pre-authority risk assessment and atomic account reservation.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use market_squawk_domain::{
    AccountId, ApprovalId, DataQuality, Denomination, InstrumentExecutionTerms, InstrumentId,
    OrderSide, OrderType, PriceTicks, RuleVersion, Timestamp,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use market_squawk_live::{CurrentAuthorityGate, LiveExecutionCapability};
use serde::Serialize;

use crate::approval::approved_order_from_risk;
use crate::audit::{ExecutionAuditContext, ExecutionAuditEvidence, ExecutionAuditPermit};
use crate::clock::{monotonic_deadline, system_now};
use crate::{
    AccountRecoverySnapshotError, AccountReservationError, AccountRiskCoordinator,
    AccountRiskReservation, AccountRiskViolation, ApprovedOrder, ExecutionAuditEvent,
    ExecutionAuditKind, ExecutionAuditWriter, ExecutionMarketReference, ExecutionPriceBound,
    OrderIntent, OrderIntentDigest, PortfolioReadError, RiskLimits, RiskPolicyIdentity,
};

/// Structurally validated but authority-free market input for pre-dispatch risk.
///
/// This value is deliberately not live execution authority. Task 11 binds it to the actor's
/// single-use current capability before any approval or adapter submission can exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketRiskInput {
    execution_terms: InstrumentExecutionTerms,
    quality: DataQuality,
    source_eligible: bool,
    instrument_trading: bool,
    observed_at: Timestamp,
    valid_until: Timestamp,
    reference_price: PriceTicks,
    estimated_execution_price: PriceTicks,
}

impl MarketRiskInput {
    /// Canonical market-input digest format version.
    pub const DIGEST_VERSION: u8 = 1;

    /// Constructs bounded authority-free market risk input.
    ///
    /// # Errors
    ///
    /// Rejects a freshness deadline that is not strictly after the observation.
    #[allow(
        clippy::too_many_arguments,
        reason = "each independent market-risk invariant remains explicit at the boundary"
    )]
    pub fn try_new(
        execution_terms: InstrumentExecutionTerms,
        quality: DataQuality,
        source_eligible: bool,
        instrument_trading: bool,
        observed_at: Timestamp,
        valid_until: Timestamp,
        reference_price: PriceTicks,
        estimated_execution_price: PriceTicks,
    ) -> Result<Self, MarketRiskInputError> {
        if valid_until <= observed_at {
            return Err(MarketRiskInputError::InvalidFreshnessWindow);
        }
        Ok(Self {
            execution_terms,
            quality,
            source_eligible,
            instrument_trading,
            observed_at,
            valid_until,
            reference_price,
            estimated_execution_price,
        })
    }

    /// Returns the immutable instrument terms bound to the market observation.
    pub const fn execution_terms(self) -> InstrumentExecutionTerms {
        self.execution_terms
    }

    /// Returns the observation's evidence quality.
    pub const fn quality(self) -> DataQuality {
        self.quality
    }

    /// Returns whether source authorization, coverage, and health are currently eligible.
    pub const fn source_eligible(self) -> bool {
        self.source_eligible
    }

    /// Returns whether the instrument and venue are currently trading.
    pub const fn instrument_trading(self) -> bool {
        self.instrument_trading
    }

    /// Returns the trusted observation time supplied by the live boundary.
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    /// Returns the exclusive freshness deadline.
    pub const fn valid_until(self) -> Timestamp {
        self.valid_until
    }

    /// Returns the side-independent comparison price.
    pub const fn reference_price(self) -> PriceTicks {
        self.reference_price
    }

    /// Returns the side-aware estimated execution price.
    pub const fn estimated_execution_price(self) -> PriceTicks {
        self.estimated_execution_price
    }

    /// Returns the versioned SHA-256 identity of every risk-relevant market input field.
    #[must_use]
    pub fn digest(self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/market-risk-input\0");
        digest.update([Self::DIGEST_VERSION]);
        update_market_execution_terms(&mut digest, self.execution_terms);
        digest.update([data_quality_tag(self.quality)]);
        digest.update([
            u8::from(self.source_eligible),
            u8::from(self.instrument_trading),
        ]);
        digest.update(self.observed_at.unix_nanos().to_be_bytes());
        digest.update(self.valid_until.unix_nanos().to_be_bytes());
        digest.update(self.reference_price.get().to_be_bytes());
        digest.update(self.estimated_execution_price.get().to_be_bytes());
        digest.finalize().into()
    }
}

/// Structural market-risk input failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MarketRiskInputError {
    /// Freshness must remain valid for at least one nanosecond after observation.
    #[error("market freshness deadline must be later than observation time")]
    InvalidFreshnessWindow,
}

/// Stable complete pre-authority risk rejection reason.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskRejectionCode {
    /// Trusted decision clock failed.
    ClockFailure,
    /// Wall time regressed within this service instance.
    ClockRollback,
    /// The actor-owned live capability was stale, revoked, expired, or transplanted.
    Authority,
    /// A supposedly non-nil order identity could not form its one-use approval identity.
    ApprovalIdentity,
    /// Mandatory bounded audit capacity was unavailable before account mutation.
    AuditUnavailable,
    /// The exact risk policy deadline passed.
    PolicyExpired,
    /// The committed canonical book cannot supply an executable side price.
    MarketDepthUnavailable,
    /// Market data is not direct and verified.
    SourceQuality,
    /// Source authorization, coverage, or health is ineligible.
    SourceIneligible,
    /// The exclusive source freshness deadline was reached.
    SourceStale,
    /// Market observation time is later than decision time.
    MarketTimestampInFuture,
    /// Market state predates the intent signal.
    MarketPredatesSignal,
    /// Venue or instrument trading state is disabled.
    InstrumentNotTrading,
    /// Market terms differ from the intent's exact revision-bound terms.
    InstrumentDefinitionMismatch,
    /// Intent expiration was reached.
    IntentExpired,
    /// A zero reference cannot support a relative price bound.
    InvalidReferencePrice,
    /// Estimated execution violates the order's explicit limit.
    OrderPriceLimit,
    /// The current reference has not triggered a stop order.
    StopNotTriggered,
    /// Estimated adverse price movement exceeds the intent-selected bound.
    IntentSlippageLimit,
    /// Estimated adverse price movement exceeds the policy slippage bound.
    PolicySlippageLimit,
    /// Estimated price deviation exceeds the independent market-price bound.
    PriceDeviationLimit,
    /// Authoritative account state produced a typed violation.
    Account(AccountRiskViolation),
    /// Authoritative portfolio state or its execution binding is invalid.
    Portfolio(crate::PortfolioReadError),
}

/// Nonempty stable ordered risk rejection.
#[derive(Debug, Eq, PartialEq)]
pub struct RiskRejection {
    reasons: Box<[RiskRejectionCode]>,
}

impl RiskRejection {
    /// Returns all applicable reasons in stable order.
    pub const fn reasons(&self) -> &[RiskRejectionCode] {
        &self.reasons
    }

    fn new(mut reasons: Vec<RiskRejectionCode>) -> Self {
        reasons.sort_unstable();
        reasons.dedup();
        debug_assert!(!reasons.is_empty());
        Self {
            reasons: reasons.into_boxed_slice(),
        }
    }
}

/// Exact non-authoritative state generation required by one risk advisory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiskAdvisoryGeneration {
    account_id: AccountId,
    instrument_id: InstrumentId,
    account_revision: NonZeroU64,
    position_revision_digest: [u8; 32],
    position_content_digest: [u8; 32],
    position_publication_generation: u64,
    policy: RiskPolicyIdentity,
    limits_digest: [u8; 32],
}

impl RiskAdvisoryGeneration {
    /// Constructs one exact account, position, and risk-policy precondition.
    #[allow(
        clippy::too_many_arguments,
        reason = "account, position, and policy generations remain independently explicit"
    )]
    pub fn try_new(
        account_id: AccountId,
        instrument_id: InstrumentId,
        account_revision: NonZeroU64,
        position_revision_digest: [u8; 32],
        position_content_digest: [u8; 32],
        position_publication_generation: u64,
        policy: RiskPolicyIdentity,
        limits_digest: [u8; 32],
    ) -> Result<Self, RiskAdvisoryGenerationError> {
        if position_revision_digest == [0; 32]
            || position_content_digest == [0; 32]
            || position_publication_generation == 0
            || policy.digest() == [0; 32]
            || limits_digest == [0; 32]
        {
            return Err(RiskAdvisoryGenerationError::InvalidIdentity);
        }
        Ok(Self {
            account_id,
            instrument_id,
            account_revision,
            position_revision_digest,
            position_content_digest,
            position_publication_generation,
            policy,
            limits_digest,
        })
    }

    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub const fn account_revision(&self) -> NonZeroU64 {
        self.account_revision
    }

    pub const fn position_revision_digest(&self) -> [u8; 32] {
        self.position_revision_digest
    }

    pub const fn position_content_digest(&self) -> [u8; 32] {
        self.position_content_digest
    }

    pub const fn position_publication_generation(&self) -> u64 {
        self.position_publication_generation
    }

    pub const fn policy(&self) -> RiskPolicyIdentity {
        self.policy
    }

    pub const fn limits_digest(&self) -> [u8; 32] {
        self.limits_digest
    }
}

/// Structurally invalid advisory generation.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RiskAdvisoryGenerationError {
    /// Digests and monotonic generations must be nonzero.
    #[error("risk advisory generation identity is invalid")]
    InvalidIdentity,
}

/// Fully specified authority-free paper draft evaluated only for current analytical guidance.
#[derive(Debug)]
pub struct PaperRiskAdvisoryDraft<'draft> {
    intent: &'draft OrderIntent,
    market: MarketRiskInput,
    generation: &'draft RiskAdvisoryGeneration,
}

impl<'draft> PaperRiskAdvisoryDraft<'draft> {
    /// Binds a validated hypothetical order and market image to exact state generations.
    pub const fn new(
        intent: &'draft OrderIntent,
        market: MarketRiskInput,
        generation: &'draft RiskAdvisoryGeneration,
    ) -> Self {
        Self {
            intent,
            market,
            generation,
        }
    }

    pub const fn intent(&self) -> &OrderIntent {
        self.intent
    }

    pub const fn market(&self) -> MarketRiskInput {
        self.market
    }

    pub const fn generation(&self) -> &RiskAdvisoryGeneration {
        self.generation
    }
}

/// Closed advisory check families exposed without execution authority.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskAdvisoryCheck {
    /// Exact risk policy, ruleset, limits, and chronology.
    Policy,
    /// Current hypothetical market, order shape, price, and freshness.
    Market,
    /// Exact authoritative account generation.
    AccountGeneration,
    /// Exact immutable portfolio/position generation.
    PositionGeneration,
    /// Current account, position, capacity, loss, and exposure constraints.
    AccountLimits,
    /// Final account and portfolio state remained unchanged through evaluation.
    StateRecheck,
}

/// Current-time analytical conclusion that conveys no future or execution authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskAdvisoryOutcome {
    /// Every available check passed only at `evaluated_at`.
    WouldPassAtEvaluation,
    /// At least one deterministic check rejected the hypothetical draft at `evaluated_at`.
    WouldRejectAtEvaluation,
    /// No deterministic rejection was found, but one or more checks could not be evaluated.
    IndeterminateAtEvaluation,
}

/// Least-authority classification permanently attached to every advisory result.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskAdvisoryAuthority {
    /// Evidence is informational and cannot reserve, approve, dispatch, or be upgraded in place.
    AnalysisOnly,
}

/// Typed, immutable evidence from one non-reserving current-state risk advisory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiskAdvisoryEvidence {
    intent_digest: OrderIntentDigest,
    generation: RiskAdvisoryGeneration,
    market_input_digest: [u8; 32],
    evaluated_at: Timestamp,
    valid_until: Timestamp,
    kill_switch: bool,
    checks_evaluated: Box<[RiskAdvisoryCheck]>,
    checks_unavailable: Box<[RiskAdvisoryCheck]>,
    outcome: RiskAdvisoryOutcome,
    reasons: Box<[RiskRejectionCode]>,
    authority: RiskAdvisoryAuthority,
    digest: [u8; 32],
}

impl RiskAdvisoryEvidence {
    /// Canonical full-evidence digest format version.
    pub const DIGEST_VERSION: u8 = 1;

    pub const fn intent_digest(&self) -> OrderIntentDigest {
        self.intent_digest
    }

    pub const fn generation(&self) -> &RiskAdvisoryGeneration {
        &self.generation
    }

    pub const fn policy_digest(&self) -> [u8; 32] {
        self.generation.policy.digest()
    }

    pub const fn ruleset_version(&self) -> RuleVersion {
        self.generation.policy.ruleset_version()
    }

    pub const fn limits_digest(&self) -> [u8; 32] {
        self.generation.limits_digest
    }

    /// Returns the exact versioned market-input identity evaluated by this advisory.
    pub const fn market_input_digest(&self) -> [u8; 32] {
        self.market_input_digest
    }

    /// Returns the trusted current evaluation instant. A pass is true only at this instant.
    pub const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    /// Returns the conservative evidence expiry, never an assertion that a pass remains current.
    pub const fn valid_until(&self) -> Timestamp {
        self.valid_until
    }

    pub const fn kill_switch(&self) -> bool {
        self.kill_switch
    }

    pub const fn checks_evaluated(&self) -> &[RiskAdvisoryCheck] {
        &self.checks_evaluated
    }

    pub const fn checks_unavailable(&self) -> &[RiskAdvisoryCheck] {
        &self.checks_unavailable
    }

    pub const fn outcome(&self) -> RiskAdvisoryOutcome {
        self.outcome
    }

    pub const fn reasons(&self) -> &[RiskRejectionCode] {
        &self.reasons
    }

    pub const fn authority(&self) -> RiskAdvisoryAuthority {
        self.authority
    }

    /// Returns the versioned SHA-256 identity of every field in this advisory result.
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Fail-closed advisory construction or exact-state failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RiskAdvisoryError {
    /// Trusted current time could not be read.
    #[error("trusted time is unavailable for risk advisory")]
    ClockUnavailable,
    /// Exact account state could not be read without mutation.
    #[error("account state is unavailable for risk advisory: {0}")]
    AccountStateUnavailable(AccountRecoverySnapshotError),
    /// Exact portfolio/position state could not be read.
    #[error("portfolio state is unavailable for risk advisory: {0}")]
    PositionStateUnavailable(PortfolioReadError),
    /// The supplied account, position, or risk-policy generation is not current.
    #[error("risk advisory generation does not match current state")]
    GenerationMismatch,
    /// Account or position state changed while the advisory was evaluated.
    #[error("risk advisory state changed during evaluation")]
    StateChanged,
}

/// Authority-free risk outcome.
///
/// A reservation protects account capacity but cannot be converted into an approved or dispatchable
/// order. No such public types or conversion exist in this stage.
#[derive(Debug)]
pub enum PreAuthorityRiskOutcome {
    /// One or more checks failed without retaining a new account reservation.
    Rejected(RiskRejection),
    /// Every current check passed and account capacity was atomically reserved.
    Reserved(AccountRiskReservation),
}

/// Full current-authority risk result. Only the approved variant can enter dispatch.
#[allow(
    clippy::large_enum_variant,
    reason = "approval keeps bounded depth and one-use authority inline to avoid a live-path allocation"
)]
#[derive(Debug)]
pub enum RiskOutcome {
    /// One or more checks failed without retaining a new account reservation.
    Rejected(RiskRejection),
    /// Current authority and account capacity were atomically bound into one opaque approval.
    Approved(ApprovedOrder),
}

/// Startup-fixed current risk policy chronology.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskServiceConfig {
    /// Fixed policy and ruleset identity retained through approval, dispatch, and audit.
    pub policy: RiskPolicyIdentity,
    /// Inclusive risk-policy deadline.
    pub policy_valid_until: Timestamp,
    /// Maximum additional wall/monotonic lifetime of one approval.
    pub maximum_approval_lifetime: Duration,
}

/// Risk service construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RiskServiceError {
    /// Approval lifetime must make bounded positive progress.
    #[error("maximum approval lifetime must be positive")]
    ZeroApprovalLifetime,
    /// Complete fixed retained-size accounting overflowed.
    #[error("risk service retained-size calculation overflowed")]
    RetainedSizeOverflow,
}

/// Deterministic risk policy owner with authoritative account coordination and trusted time.
#[derive(Debug)]
pub struct RiskService {
    accounts: Arc<AccountRiskCoordinator>,
    portfolio: crate::PortfolioReadCapability,
    limits: RiskLimits,
    audit: ExecutionAuditWriter,
    config: RiskServiceConfig,
    last_wall_nanos: AtomicI64,
    retained_bytes: usize,
}

impl RiskService {
    /// Creates a risk service over authoritative account state and mandatory bounded audit.
    pub fn try_new(
        accounts: Arc<AccountRiskCoordinator>,
        portfolio: crate::PortfolioReadCapability,
        limits: RiskLimits,
        audit: ExecutionAuditWriter,
        config: RiskServiceConfig,
    ) -> Result<Self, RiskServiceError> {
        if config.maximum_approval_lifetime.is_zero() {
            return Err(RiskServiceError::ZeroApprovalLifetime);
        }
        let retained_bytes = Self::retained_bytes_for_limits(&limits)?;
        Ok(Self {
            accounts,
            portfolio,
            limits,
            audit,
            config,
            last_wall_nanos: AtomicI64::new(i64::MIN),
            retained_bytes,
        })
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        // The coordinator is an independently composed shared owner charged once by the
        // application memory model. A route hook retains only this Arc handle.
        self.retained_bytes
    }

    pub(crate) fn audit_writer(&self) -> ExecutionAuditWriter {
        self.audit.clone()
    }

    /// Captures the exact non-authoritative account, position, and risk-policy generation.
    ///
    /// The account image must be quiescent because this read deliberately refuses to coexist with
    /// a reservation. The returned value is only a compare-and-evaluate precondition.
    pub fn current_advisory_generation(
        &self,
        intent: &OrderIntent,
    ) -> Result<RiskAdvisoryGeneration, RiskAdvisoryError> {
        let account = self
            .accounts
            .snapshot_recovery_state(intent.account_id())
            .map_err(RiskAdvisoryError::AccountStateUnavailable)?;
        let (position, _snapshot) = self
            .portfolio
            .bind_current(
                intent.account_id(),
                intent.execution_terms().instrument_id(),
                intent.side(),
                self.limits.currency(),
            )
            .map_err(RiskAdvisoryError::PositionStateUnavailable)?;
        let rechecked_account = self
            .accounts
            .snapshot_recovery_state(intent.account_id())
            .map_err(RiskAdvisoryError::AccountStateUnavailable)?;
        if account != rechecked_account {
            return Err(RiskAdvisoryError::StateChanged);
        }
        self.portfolio
            .recheck(&position)
            .map_err(|_error| RiskAdvisoryError::StateChanged)?;
        RiskAdvisoryGeneration::try_new(
            intent.account_id(),
            intent.execution_terms().instrument_id(),
            account.revision(),
            position.revision().bytes(),
            position.content_digest(),
            position.publication_generation(),
            self.config.policy,
            self.limits.digest(),
        )
        .map_err(|_error| RiskAdvisoryError::GenerationMismatch)
    }

    /// Evaluates a hypothetical paper order without reserving or consuming any authority.
    ///
    /// This path performs no audit admission, nonce publication, rate-event publication, account
    /// reservation, live-capability consumption, approval construction, or dispatch conversion.
    /// A `WouldPassAtEvaluation` result is true only at `evaluated_at`; every later action must run
    /// the ordinary current-authority risk path from the beginning.
    pub fn evaluate_advisory(
        &self,
        draft: &PaperRiskAdvisoryDraft<'_>,
    ) -> Result<RiskAdvisoryEvidence, RiskAdvisoryError> {
        let intent = draft.intent;
        let market = draft.market;
        let generation = draft.generation;
        let limits_digest = self.limits.digest();
        if generation.account_id != intent.account_id()
            || generation.instrument_id != intent.execution_terms().instrument_id()
            || generation.policy != self.config.policy
            || generation.limits_digest != limits_digest
        {
            return Err(RiskAdvisoryError::GenerationMismatch);
        }

        let now = system_now().map_err(|_error| RiskAdvisoryError::ClockUnavailable)?;
        let account = self
            .accounts
            .snapshot_recovery_state(intent.account_id())
            .map_err(RiskAdvisoryError::AccountStateUnavailable)?;
        if account.account_id() != generation.account_id
            || account.revision() != generation.account_revision
        {
            return Err(RiskAdvisoryError::GenerationMismatch);
        }
        let (position, snapshot) = self
            .portfolio
            .bind_current(
                intent.account_id(),
                intent.execution_terms().instrument_id(),
                intent.side(),
                self.limits.currency(),
            )
            .map_err(RiskAdvisoryError::PositionStateUnavailable)?;
        if position.account_id() != generation.account_id
            || position.revision().bytes() != generation.position_revision_digest
            || position.content_digest() != generation.position_content_digest
            || position.publication_generation() != generation.position_publication_generation
        {
            return Err(RiskAdvisoryError::GenerationMismatch);
        }

        let mut evaluated = Vec::with_capacity(6);
        let mut unavailable = Vec::with_capacity(1);
        let mut reasons = Vec::new();
        evaluated.extend([
            RiskAdvisoryCheck::Policy,
            RiskAdvisoryCheck::Market,
            RiskAdvisoryCheck::AccountGeneration,
            RiskAdvisoryCheck::PositionGeneration,
        ]);
        let prior_wall_nanos = self.last_wall_nanos.load(Ordering::Acquire);
        if now.wall.unix_nanos() < prior_wall_nanos {
            reasons.push(RiskRejectionCode::ClockRollback);
        }
        if now.wall > self.config.policy_valid_until {
            reasons.push(RiskRejectionCode::PolicyExpired);
        }
        if self.limits.kill_switch() {
            reasons.push(RiskRejectionCode::Account(AccountRiskViolation::KillSwitch));
        }
        self.evaluate_market(intent, &market, now.wall, &mut reasons);
        let execution_price_bound =
            execution_price_bound(intent, market.estimated_execution_price(), &self.limits);
        if execution_price_bound.is_none() {
            reasons.push(RiskRejectionCode::Account(
                AccountRiskViolation::ArithmeticOverflow,
            ));
            unavailable.push(RiskAdvisoryCheck::AccountLimits);
        } else if let Some(execution_price_bound) = execution_price_bound {
            match self.accounts.assess_for_portfolio(
                intent,
                execution_price_bound.maximum_price(),
                &self.limits,
                &snapshot,
            ) {
                Ok(()) => evaluated.push(RiskAdvisoryCheck::AccountLimits),
                Err(rejection) if account_assessment_unavailable(&rejection) => {
                    unavailable.push(RiskAdvisoryCheck::AccountLimits);
                    extend_account_reasons(&mut reasons, &rejection);
                }
                Err(rejection) => {
                    evaluated.push(RiskAdvisoryCheck::AccountLimits);
                    extend_account_reasons(&mut reasons, &rejection);
                }
            }
        }

        self.portfolio
            .recheck(&position)
            .map_err(|_error| RiskAdvisoryError::StateChanged)?;
        let rechecked_account = self
            .accounts
            .snapshot_recovery_state(intent.account_id())
            .map_err(|_error| RiskAdvisoryError::StateChanged)?;
        if rechecked_account != account {
            return Err(RiskAdvisoryError::StateChanged);
        }
        evaluated.push(RiskAdvisoryCheck::StateRecheck);

        evaluated.sort_unstable();
        evaluated.dedup();
        unavailable.sort_unstable();
        unavailable.dedup();
        reasons.sort_unstable();
        reasons.dedup();
        let has_definitive_rejection = reasons
            .iter()
            .any(|reason| !advisory_reason_is_unavailable(*reason));
        let outcome = if has_definitive_rejection {
            RiskAdvisoryOutcome::WouldRejectAtEvaluation
        } else if unavailable.is_empty() {
            RiskAdvisoryOutcome::WouldPassAtEvaluation
        } else {
            RiskAdvisoryOutcome::IndeterminateAtEvaluation
        };
        let intent_digest = intent.digest();
        let market_input_digest = market.digest();
        let valid_until = intent
            .expires_at()
            .min(market.valid_until())
            .min(self.config.policy_valid_until);
        let kill_switch = self.limits.kill_switch();
        let authority = RiskAdvisoryAuthority::AnalysisOnly;
        let digest = advisory_evidence_digest(
            intent_digest,
            generation,
            market_input_digest,
            now.wall,
            valid_until,
            kill_switch,
            &evaluated,
            &unavailable,
            outcome,
            &reasons,
            authority,
        );
        Ok(RiskAdvisoryEvidence {
            intent_digest,
            generation: generation.clone(),
            market_input_digest,
            evaluated_at: now.wall,
            valid_until,
            kill_switch,
            checks_evaluated: evaluated.into_boxed_slice(),
            checks_unavailable: unavailable.into_boxed_slice(),
            outcome,
            reasons: reasons.into_boxed_slice(),
            authority,
            digest,
        })
    }

    /// Returns the exact checked risk graph charge used before runtime ownership transfer.
    pub fn retained_bytes_for_limits(limits: &RiskLimits) -> Result<usize, RiskServiceError> {
        let limits_bytes = limits
            .checked_retained_byte_ceiling()
            .map_err(|_| RiskServiceError::RetainedSizeOverflow)?;
        let limits_heap_bytes = limits_bytes
            .checked_sub(std::mem::size_of::<RiskLimits>())
            .ok_or(RiskServiceError::RetainedSizeOverflow)?;
        std::mem::size_of::<Self>()
            .checked_add(limits_heap_bytes)
            .ok_or(RiskServiceError::RetainedSizeOverflow)
    }

    /// Consumes actor-owned live authority exactly once and approves only after mandatory audit
    /// admission and atomic account reservation.
    pub fn evaluate(
        &mut self,
        authority_gate: &mut CurrentAuthorityGate<'_>,
        capability: LiveExecutionCapability,
        intent: OrderIntent,
        market: &ExecutionMarketReference,
    ) -> RiskOutcome {
        let order_id = intent.order_id();
        let approval_id = match ApprovalId::try_from(order_id.as_uuid()) {
            Ok(approval_id) => approval_id,
            Err(_) => {
                return RiskOutcome::Rejected(RiskRejection::new(vec![
                    RiskRejectionCode::ApprovalIdentity,
                ]));
            }
        };
        let audit = match self.audit.try_reserve() {
            Ok(permit) => permit,
            Err(_) => {
                return RiskOutcome::Rejected(RiskRejection::new(vec![
                    RiskRejectionCode::AuditUnavailable,
                ]));
            }
        };
        let now = match system_now() {
            Ok(now) => now,
            Err(_) => {
                let context = ExecutionAuditContext::from_risk(
                    approval_id,
                    &intent,
                    *market,
                    ExecutionAuditEvidence::new(None, None, None),
                    self.config.policy,
                    intent.expires_at().min(self.config.policy_valid_until),
                );
                let reasons = [RiskRejectionCode::ClockFailure];
                commit_audit(
                    audit,
                    ExecutionAuditKind::RiskRejected,
                    context,
                    intent.signal_at(),
                    &reasons,
                );
                return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
            }
        };
        let authority = match authority_gate.consume(capability) {
            Ok(authority) => authority,
            Err(_) => {
                let reasons = [RiskRejectionCode::Authority];
                let context = ExecutionAuditContext::from_risk(
                    approval_id,
                    &intent,
                    *market,
                    ExecutionAuditEvidence::new(None, None, None),
                    self.config.policy,
                    intent.expires_at().min(self.config.policy_valid_until),
                );
                commit_audit(
                    audit,
                    ExecutionAuditKind::RiskRejected,
                    context,
                    now.wall,
                    &reasons,
                );
                return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
            }
        };
        let mut reasons = Vec::new();
        let mut portfolio_binding = None;
        let mut portfolio_snapshot = None;
        let previous = self
            .last_wall_nanos
            .fetch_max(now.wall.unix_nanos(), Ordering::AcqRel);
        if now.wall.unix_nanos() < previous {
            reasons.push(RiskRejectionCode::ClockRollback);
        }
        if authority.validate_current().is_err() {
            reasons.push(RiskRejectionCode::Authority);
        }
        if now.wall > self.config.policy_valid_until {
            reasons.push(RiskRejectionCode::PolicyExpired);
        }
        let execution_price = market.execution_price(intent.side());
        self.evaluate_current_market(&intent, market, execution_price, now.wall, &mut reasons);
        let execution_price_bound = execution_price.and_then(|execution_price| {
            execution_price_bound(&intent, execution_price, &self.limits)
        });
        if execution_price.is_some() && execution_price_bound.is_none() {
            reasons.push(RiskRejectionCode::Account(
                AccountRiskViolation::ArithmeticOverflow,
            ));
        }
        match self.portfolio.bind_current(
            intent.account_id(),
            intent.execution_terms().instrument_id(),
            intent.side(),
            self.limits.currency(),
        ) {
            Ok((binding, snapshot)) => {
                portfolio_binding = Some(binding);
                portfolio_snapshot = Some(snapshot);
            }
            Err(error) => reasons.push(RiskRejectionCode::Portfolio(error)),
        }
        if let (Some(execution_price_bound), Some(snapshot)) =
            (execution_price_bound, portfolio_snapshot.as_ref())
            && let Err(rejection) = self.accounts.assess_for_portfolio(
                &intent,
                execution_price_bound.maximum_price(),
                &self.limits,
                snapshot,
            )
        {
            extend_account_reasons(&mut reasons, &rejection);
        }
        if !reasons.is_empty() {
            let context = ExecutionAuditContext::from_risk(
                approval_id,
                &intent,
                *market,
                ExecutionAuditEvidence::new(
                    Some(&authority),
                    execution_price_bound,
                    portfolio_binding.as_ref(),
                ),
                self.config.policy,
                intent
                    .expires_at()
                    .min(authority.valid_until())
                    .min(self.config.policy_valid_until),
            );
            commit_audit(
                audit,
                ExecutionAuditKind::RiskRejected,
                context,
                now.wall,
                &reasons,
            );
            return RiskOutcome::Rejected(RiskRejection::new(reasons));
        }
        let Some(execution_price_bound) = execution_price_bound else {
            let reasons = [RiskRejectionCode::MarketDepthUnavailable];
            let context = ExecutionAuditContext::from_risk(
                approval_id,
                &intent,
                *market,
                ExecutionAuditEvidence::new(Some(&authority), None, portfolio_binding.as_ref()),
                self.config.policy,
                intent
                    .expires_at()
                    .min(authority.valid_until())
                    .min(self.config.policy_valid_until),
            );
            commit_audit(
                audit,
                ExecutionAuditKind::RiskRejected,
                context,
                now.wall,
                &reasons,
            );
            return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
        };
        let Some(portfolio_binding) = portfolio_binding else {
            let reasons = [RiskRejectionCode::Portfolio(
                crate::PortfolioReadError::ContentMismatch,
            )];
            let context = ExecutionAuditContext::from_risk(
                approval_id,
                &intent,
                *market,
                ExecutionAuditEvidence::new(Some(&authority), Some(execution_price_bound), None),
                self.config.policy,
                intent
                    .expires_at()
                    .min(authority.valid_until())
                    .min(self.config.policy_valid_until),
            );
            commit_audit(
                audit,
                ExecutionAuditKind::RiskRejected,
                context,
                now.wall,
                &reasons,
            );
            return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
        };
        let Some(portfolio_snapshot) = portfolio_snapshot else {
            let reasons = [RiskRejectionCode::Portfolio(
                crate::PortfolioReadError::ContentMismatch,
            )];
            let context = ExecutionAuditContext::from_risk(
                approval_id,
                &intent,
                *market,
                ExecutionAuditEvidence::new(
                    Some(&authority),
                    Some(execution_price_bound),
                    Some(&portfolio_binding),
                ),
                self.config.policy,
                intent
                    .expires_at()
                    .min(authority.valid_until())
                    .min(self.config.policy_valid_until),
            );
            commit_audit(
                audit,
                ExecutionAuditKind::RiskRejected,
                context,
                now.wall,
                &reasons,
            );
            return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
        };
        if let Err(error) = self.portfolio.recheck(&portfolio_binding) {
            let reasons = [RiskRejectionCode::Portfolio(error)];
            let context = ExecutionAuditContext::from_risk(
                approval_id,
                &intent,
                *market,
                ExecutionAuditEvidence::new(
                    Some(&authority),
                    Some(execution_price_bound),
                    Some(&portfolio_binding),
                ),
                self.config.policy,
                intent
                    .expires_at()
                    .min(authority.valid_until())
                    .min(self.config.policy_valid_until),
            );
            commit_audit(
                audit,
                ExecutionAuditKind::RiskRejected,
                context,
                now.wall,
                &reasons,
            );
            return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
        }
        let reservation = match self.accounts.try_reserve_for_portfolio(
            &intent,
            execution_price_bound.maximum_price(),
            &self.limits,
            &portfolio_snapshot,
        ) {
            Ok(reservation) => reservation,
            Err(rejection) => {
                extend_account_reasons(&mut reasons, &rejection);
                let context = ExecutionAuditContext::from_risk(
                    approval_id,
                    &intent,
                    *market,
                    ExecutionAuditEvidence::new(
                        Some(&authority),
                        Some(execution_price_bound),
                        Some(&portfolio_binding),
                    ),
                    self.config.policy,
                    intent
                        .expires_at()
                        .min(authority.valid_until())
                        .min(self.config.policy_valid_until),
                );
                commit_audit(
                    audit,
                    ExecutionAuditKind::RiskRejected,
                    context,
                    now.wall,
                    &reasons,
                );
                return RiskOutcome::Rejected(RiskRejection::new(reasons));
            }
        };
        let valid_until = intent
            .expires_at()
            .min(authority.valid_until())
            .min(reservation.valid_until())
            .min(self.config.policy_valid_until);
        let remaining = valid_until
            .unix_nanos()
            .checked_sub(now.wall.unix_nanos())
            .unwrap_or(-1);
        let maximum =
            i64::try_from(self.config.maximum_approval_lifetime.as_nanos()).unwrap_or(i64::MAX);
        let monotonic_deadline = match monotonic_deadline(now, remaining.min(maximum)) {
            Ok(deadline) if remaining >= 0 => deadline,
            _ => {
                let reasons = [RiskRejectionCode::ClockFailure];
                let context = ExecutionAuditContext::from_risk(
                    approval_id,
                    &intent,
                    *market,
                    ExecutionAuditEvidence::new(
                        Some(&authority),
                        Some(execution_price_bound),
                        Some(&portfolio_binding),
                    ),
                    self.config.policy,
                    valid_until,
                );
                commit_audit(
                    audit,
                    ExecutionAuditKind::RiskRejected,
                    context,
                    now.wall,
                    &reasons,
                );
                return RiskOutcome::Rejected(RiskRejection::new(reasons.to_vec()));
            }
        };
        let context = ExecutionAuditContext::from_risk(
            approval_id,
            &intent,
            *market,
            ExecutionAuditEvidence::new(
                Some(&authority),
                Some(execution_price_bound),
                Some(&portfolio_binding),
            ),
            self.config.policy,
            valid_until,
        );
        commit_audit(
            audit,
            ExecutionAuditKind::RiskApproved,
            context,
            now.wall,
            &[],
        );
        RiskOutcome::Approved(approved_order_from_risk(
            approval_id,
            intent,
            *market,
            execution_price_bound,
            authority,
            reservation,
            self.portfolio.clone(),
            portfolio_binding,
            self.config.policy,
            valid_until,
            monotonic_deadline,
        ))
    }

    /// Runs every deterministic pre-authority check and atomically reserves only on success.
    ///
    /// This method accepts no caller time or account snapshot and cannot approve or dispatch an
    /// order. Task 11 consumes actor-owned live authority before wrapping a successful reservation
    /// in a private approval candidate.
    pub fn evaluate_pre_authority(
        &self,
        intent: &OrderIntent,
        market: &MarketRiskInput,
    ) -> PreAuthorityRiskOutcome {
        let now = match system_now() {
            Ok(now) => now,
            Err(_) => {
                return PreAuthorityRiskOutcome::Rejected(RiskRejection::new(vec![
                    RiskRejectionCode::ClockFailure,
                ]));
            }
        };
        let mut reasons = Vec::new();
        let mut portfolio_binding = None;
        let mut portfolio_snapshot = None;
        let wall = now.wall.unix_nanos();
        let previous = self.last_wall_nanos.fetch_max(wall, Ordering::AcqRel);
        if wall < previous {
            reasons.push(RiskRejectionCode::ClockRollback);
        }
        self.evaluate_market(intent, market, now.wall, &mut reasons);
        let execution_price_bound =
            execution_price_bound(intent, market.estimated_execution_price, &self.limits);
        if execution_price_bound.is_none() {
            reasons.push(RiskRejectionCode::Account(
                AccountRiskViolation::ArithmeticOverflow,
            ));
        }
        match self.portfolio.bind_current(
            intent.account_id(),
            intent.execution_terms().instrument_id(),
            intent.side(),
            self.limits.currency(),
        ) {
            Ok((binding, snapshot)) => {
                portfolio_binding = Some(binding);
                portfolio_snapshot = Some(snapshot);
            }
            Err(error) => reasons.push(RiskRejectionCode::Portfolio(error)),
        }
        if let (Some(execution_price_bound), Some(snapshot)) =
            (execution_price_bound, portfolio_snapshot.as_ref())
            && let Err(rejection) = self.accounts.assess_for_portfolio(
                intent,
                execution_price_bound.maximum_price(),
                &self.limits,
                snapshot,
            )
        {
            extend_account_reasons(&mut reasons, &rejection);
        }
        if !reasons.is_empty() {
            return PreAuthorityRiskOutcome::Rejected(RiskRejection::new(reasons));
        }
        let Some(execution_price_bound) = execution_price_bound else {
            return PreAuthorityRiskOutcome::Rejected(RiskRejection::new(vec![
                RiskRejectionCode::Account(AccountRiskViolation::ArithmeticOverflow),
            ]));
        };

        let (Some(portfolio_binding), Some(portfolio_snapshot)) =
            (portfolio_binding, portfolio_snapshot)
        else {
            return PreAuthorityRiskOutcome::Rejected(RiskRejection::new(vec![
                RiskRejectionCode::Portfolio(crate::PortfolioReadError::ContentMismatch),
            ]));
        };
        if let Err(error) = self.portfolio.recheck(&portfolio_binding) {
            return PreAuthorityRiskOutcome::Rejected(RiskRejection::new(vec![
                RiskRejectionCode::Portfolio(error),
            ]));
        }

        match self.accounts.try_reserve_for_portfolio(
            intent,
            execution_price_bound.maximum_price(),
            &self.limits,
            &portfolio_snapshot,
        ) {
            Ok(reservation) => PreAuthorityRiskOutcome::Reserved(reservation),
            Err(rejection) => {
                extend_account_reasons(&mut reasons, &rejection);
                PreAuthorityRiskOutcome::Rejected(RiskRejection::new(reasons))
            }
        }
    }

    fn evaluate_market(
        &self,
        intent: &OrderIntent,
        market: &MarketRiskInput,
        now: Timestamp,
        reasons: &mut Vec<RiskRejectionCode>,
    ) {
        if intent.maximum_slippage().get() > self.limits.maximum_slippage().get() {
            reasons.push(RiskRejectionCode::PolicySlippageLimit);
        }
        if market.quality != DataQuality::DirectVerified
            || intent.required_quality() != DataQuality::DirectVerified
        {
            reasons.push(RiskRejectionCode::SourceQuality);
        }
        if !market.source_eligible {
            reasons.push(RiskRejectionCode::SourceIneligible);
        }
        if market_freshness_expired(now, market.valid_until) {
            reasons.push(RiskRejectionCode::SourceStale);
        }
        if market.observed_at > now {
            reasons.push(RiskRejectionCode::MarketTimestampInFuture);
        }
        if market.observed_at < intent.signal_at() {
            reasons.push(RiskRejectionCode::MarketPredatesSignal);
        }
        if !market.instrument_trading {
            reasons.push(RiskRejectionCode::InstrumentNotTrading);
        }
        if market.execution_terms != intent.execution_terms() {
            reasons.push(RiskRejectionCode::InstrumentDefinitionMismatch);
        }
        if now > intent.expires_at() {
            reasons.push(RiskRejectionCode::IntentExpired);
        }
        if market.reference_price.get() == 0 {
            reasons.push(RiskRejectionCode::InvalidReferencePrice);
        } else {
            if deviation_exceeds(
                market.reference_price,
                market.estimated_execution_price,
                intent.maximum_slippage().get(),
            ) {
                reasons.push(RiskRejectionCode::IntentSlippageLimit);
            }
            if deviation_exceeds(
                market.reference_price,
                market.estimated_execution_price,
                self.limits.maximum_slippage().get(),
            ) {
                reasons.push(RiskRejectionCode::PolicySlippageLimit);
            }
            if deviation_exceeds(
                market.reference_price,
                market.estimated_execution_price,
                self.limits.maximum_price_deviation().get(),
            ) {
                reasons.push(RiskRejectionCode::PriceDeviationLimit);
            }
        }
        if violates_limit(intent, market.estimated_execution_price) {
            reasons.push(RiskRejectionCode::OrderPriceLimit);
        }
        if !stop_triggered(intent, market.reference_price) {
            reasons.push(RiskRejectionCode::StopNotTriggered);
        }
    }

    fn evaluate_current_market(
        &self,
        intent: &OrderIntent,
        market: &ExecutionMarketReference,
        execution_price: Option<PriceTicks>,
        now: Timestamp,
        reasons: &mut Vec<RiskRejectionCode>,
    ) {
        if intent.maximum_slippage().get() > self.limits.maximum_slippage().get() {
            reasons.push(RiskRejectionCode::PolicySlippageLimit);
        }
        if market.execution_terms() != intent.execution_terms() {
            reasons.push(RiskRejectionCode::InstrumentDefinitionMismatch);
        }
        if market.observed_at() > now {
            reasons.push(RiskRejectionCode::MarketTimestampInFuture);
        }
        if market.observed_at() < intent.signal_at() {
            reasons.push(RiskRejectionCode::MarketPredatesSignal);
        }
        if now > intent.expires_at() {
            reasons.push(RiskRejectionCode::IntentExpired);
        }
        let Some(execution_price) = execution_price else {
            reasons.push(RiskRejectionCode::MarketDepthUnavailable);
            return;
        };
        if violates_limit(intent, execution_price) {
            reasons.push(RiskRejectionCode::OrderPriceLimit);
        }
        if !stop_triggered(intent, execution_price) {
            reasons.push(RiskRejectionCode::StopNotTriggered);
        }
    }
}

fn extend_account_reasons(
    reasons: &mut Vec<RiskRejectionCode>,
    rejection: &AccountReservationError,
) {
    reasons.extend(
        rejection
            .reasons()
            .iter()
            .copied()
            .map(RiskRejectionCode::Account),
    );
}

fn account_assessment_unavailable(rejection: &AccountReservationError) -> bool {
    rejection.reasons().iter().all(|reason| {
        matches!(
            reason,
            AccountRiskViolation::AccountCoordinatorBusy
                | AccountRiskViolation::AccountCoordinatorPoisoned
                | AccountRiskViolation::ClockFailure
        )
    })
}

const fn advisory_reason_is_unavailable(reason: RiskRejectionCode) -> bool {
    matches!(
        reason,
        RiskRejectionCode::Account(
            AccountRiskViolation::AccountCoordinatorBusy
                | AccountRiskViolation::AccountCoordinatorPoisoned
                | AccountRiskViolation::ClockFailure
        )
    )
}

fn update_market_execution_terms(digest: &mut Sha256, terms: InstrumentExecutionTerms) {
    digest.update(terms.instrument_id().as_uuid().as_bytes());
    digest.update(terms.definition_revision().get().to_be_bytes());
    update_advisory_decimal(digest, terms.price_tick().as_decimal());
    update_advisory_decimal(digest, terms.lot_size().as_decimal());
    digest.update(terms.quote_currency().as_str().as_bytes());
    match terms.settlement_denomination() {
        Denomination::Currency(currency) => {
            digest.update([0]);
            digest.update(currency.as_str().as_bytes());
        }
        Denomination::Asset(instrument_id) => {
            digest.update([1]);
            digest.update(instrument_id.as_uuid().as_bytes());
        }
    }
    update_advisory_decimal(digest, terms.contract_multiplier());
}

fn update_advisory_decimal(digest: &mut Sha256, value: rust_decimal::Decimal) {
    let normalized = value.normalize();
    digest.update(normalized.mantissa().to_be_bytes());
    digest.update(normalized.scale().to_be_bytes());
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

#[allow(
    clippy::too_many_arguments,
    reason = "the canonical result identity keeps every evidence dimension explicit"
)]
fn advisory_evidence_digest(
    intent_digest: OrderIntentDigest,
    generation: &RiskAdvisoryGeneration,
    market_input_digest: [u8; 32],
    evaluated_at: Timestamp,
    valid_until: Timestamp,
    kill_switch: bool,
    checks_evaluated: &[RiskAdvisoryCheck],
    checks_unavailable: &[RiskAdvisoryCheck],
    outcome: RiskAdvisoryOutcome,
    reasons: &[RiskRejectionCode],
    authority: RiskAdvisoryAuthority,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/risk-advisory-evidence\0");
    digest.update([RiskAdvisoryEvidence::DIGEST_VERSION]);
    digest.update(intent_digest.as_bytes());
    digest.update(generation.account_id.as_uuid().as_bytes());
    digest.update(generation.instrument_id.as_uuid().as_bytes());
    digest.update(generation.account_revision.get().to_be_bytes());
    digest.update(generation.position_revision_digest);
    digest.update(generation.position_content_digest);
    digest.update(generation.position_publication_generation.to_be_bytes());
    digest.update(generation.policy.digest());
    digest.update(generation.policy.ruleset_version().get().to_be_bytes());
    digest.update(generation.limits_digest);
    digest.update(market_input_digest);
    digest.update(evaluated_at.unix_nanos().to_be_bytes());
    digest.update(valid_until.unix_nanos().to_be_bytes());
    digest.update([u8::from(kill_switch)]);
    update_advisory_checks(&mut digest, checks_evaluated);
    update_advisory_checks(&mut digest, checks_unavailable);
    digest.update([advisory_outcome_tag(outcome)]);
    digest.update(
        u32::try_from(reasons.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for reason in reasons {
        digest.update(risk_rejection_code_tag(*reason).to_be_bytes());
    }
    digest.update([advisory_authority_tag(authority)]);
    digest.finalize().into()
}

fn update_advisory_checks(digest: &mut Sha256, checks: &[RiskAdvisoryCheck]) {
    digest.update(
        u32::try_from(checks.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for check in checks {
        digest.update([advisory_check_tag(*check)]);
    }
}

const fn advisory_check_tag(check: RiskAdvisoryCheck) -> u8 {
    match check {
        RiskAdvisoryCheck::Policy => 1,
        RiskAdvisoryCheck::Market => 2,
        RiskAdvisoryCheck::AccountGeneration => 3,
        RiskAdvisoryCheck::PositionGeneration => 4,
        RiskAdvisoryCheck::AccountLimits => 5,
        RiskAdvisoryCheck::StateRecheck => 6,
    }
}

const fn advisory_outcome_tag(outcome: RiskAdvisoryOutcome) -> u8 {
    match outcome {
        RiskAdvisoryOutcome::WouldPassAtEvaluation => 1,
        RiskAdvisoryOutcome::WouldRejectAtEvaluation => 2,
        RiskAdvisoryOutcome::IndeterminateAtEvaluation => 3,
    }
}

const fn advisory_authority_tag(authority: RiskAdvisoryAuthority) -> u8 {
    match authority {
        RiskAdvisoryAuthority::AnalysisOnly => 1,
    }
}

const fn risk_rejection_code_tag(reason: RiskRejectionCode) -> u16 {
    match reason {
        RiskRejectionCode::ClockFailure => 1,
        RiskRejectionCode::ClockRollback => 2,
        RiskRejectionCode::Authority => 3,
        RiskRejectionCode::ApprovalIdentity => 4,
        RiskRejectionCode::AuditUnavailable => 5,
        RiskRejectionCode::PolicyExpired => 6,
        RiskRejectionCode::MarketDepthUnavailable => 7,
        RiskRejectionCode::SourceQuality => 8,
        RiskRejectionCode::SourceIneligible => 9,
        RiskRejectionCode::SourceStale => 10,
        RiskRejectionCode::MarketTimestampInFuture => 11,
        RiskRejectionCode::MarketPredatesSignal => 12,
        RiskRejectionCode::InstrumentNotTrading => 13,
        RiskRejectionCode::InstrumentDefinitionMismatch => 14,
        RiskRejectionCode::IntentExpired => 15,
        RiskRejectionCode::InvalidReferencePrice => 16,
        RiskRejectionCode::OrderPriceLimit => 17,
        RiskRejectionCode::StopNotTriggered => 18,
        RiskRejectionCode::IntentSlippageLimit => 19,
        RiskRejectionCode::PolicySlippageLimit => 20,
        RiskRejectionCode::PriceDeviationLimit => 21,
        RiskRejectionCode::Account(violation) => 0x0100 | account_violation_tag(violation),
        RiskRejectionCode::Portfolio(error) => 0x0200 | portfolio_error_tag(error),
    }
}

const fn account_violation_tag(violation: AccountRiskViolation) -> u16 {
    match violation {
        AccountRiskViolation::KillSwitch => 1,
        AccountRiskViolation::AccountNotFound => 2,
        AccountRiskViolation::AccountIneligible => 3,
        AccountRiskViolation::ReconciliationRequired => 4,
        AccountRiskViolation::InstrumentIneligible => 5,
        AccountRiskViolation::CurrencyMismatch => 6,
        AccountRiskViolation::PortfolioStateMismatch => 7,
        AccountRiskViolation::UnsupportedSettlement => 8,
        AccountRiskViolation::IntentExpired => 9,
        AccountRiskViolation::IntentLifetimeExceeded => 10,
        AccountRiskViolation::DuplicateClientOrder => 11,
        AccountRiskViolation::DuplicateOrder => 12,
        AccountRiskViolation::IdempotencyCapacity => 13,
        AccountRiskViolation::IdempotencyRevisionExhausted => 14,
        AccountRiskViolation::ReservationCapacity => 15,
        AccountRiskViolation::OrderRateLimit => 16,
        AccountRiskViolation::OrderNotionalLimit => 17,
        AccountRiskViolation::PositionLimit => 18,
        AccountRiskViolation::InsufficientPosition => 19,
        AccountRiskViolation::InsufficientCash => 20,
        AccountRiskViolation::ExposureLimit => 21,
        AccountRiskViolation::LeverageLimit => 22,
        AccountRiskViolation::CapitalLimit => 23,
        AccountRiskViolation::LossLimit => 24,
        AccountRiskViolation::DrawdownLimit => 25,
        AccountRiskViolation::ArithmeticOverflow => 26,
        AccountRiskViolation::AccountCoordinatorBusy => 27,
        AccountRiskViolation::AccountCoordinatorPoisoned => 28,
        AccountRiskViolation::ClockFailure => 29,
    }
}

const fn portfolio_error_tag(error: PortfolioReadError) -> u16 {
    match error {
        PortfolioReadError::RevokedCapability => 1,
        PortfolioReadError::MissingAccount => 2,
        PortfolioReadError::StaleRevision => 3,
        PortfolioReadError::RevokedRevision => 4,
        PortfolioReadError::QueryBound => 5,
        PortfolioReadError::CurrencyMismatch => 6,
        PortfolioReadError::IncompleteBasis => 7,
        PortfolioReadError::ContentMismatch => 8,
        PortfolioReadError::PublicationRollback => 9,
        PortfolioReadError::PublicationHistoryExhausted => 10,
        PortfolioReadError::PublicationGenerationExhausted => 11,
        PortfolioReadError::PublicationUnavailable => 12,
    }
}

fn commit_audit(
    permit: ExecutionAuditPermit,
    kind: ExecutionAuditKind,
    context: ExecutionAuditContext,
    observed_at: Timestamp,
    reasons: &[RiskRejectionCode],
) {
    let event = ExecutionAuditEvent::from_risk_context(kind, context, observed_at, reasons);
    permit.commit(event);
}

fn violates_limit(intent: &OrderIntent, execution_price: PriceTicks) -> bool {
    let Some(limit) = intent.limit_price() else {
        return false;
    };
    match intent.side() {
        OrderSide::Buy => execution_price > limit,
        OrderSide::Sell => execution_price < limit,
    }
}

fn stop_triggered(intent: &OrderIntent, reference_price: PriceTicks) -> bool {
    if !matches!(intent.order_type(), OrderType::Stop | OrderType::StopLimit) {
        return true;
    }
    let Some(stop) = intent.stop_price() else {
        return false;
    };
    match intent.side() {
        OrderSide::Buy => reference_price >= stop,
        OrderSide::Sell => reference_price <= stop,
    }
}

/// Derives the hard upper average execution-price ceiling used for account reservation.
///
/// Buy intent slippage and limit prices can tighten the policy ceiling. Sell limits remain price
/// floors, so the symmetric policy-deviation ceiling is retained to bound growing short exposure.
fn execution_price_bound(
    intent: &OrderIntent,
    execution_price: PriceTicks,
    limits: &RiskLimits,
) -> Option<ExecutionPriceBound> {
    if execution_price.get() <= 0
        || !(0..=10_000).contains(&intent.maximum_slippage().get())
        || !(0..=10_000).contains(&limits.maximum_price_deviation().get())
    {
        return None;
    }
    let policy_ceiling =
        checked_upper_price(execution_price, limits.maximum_price_deviation().get())?;
    let maximum_price = match intent.side() {
        OrderSide::Sell => policy_ceiling,
        OrderSide::Buy => {
            let intent_ceiling =
                checked_upper_price(execution_price, intent.maximum_slippage().get())?;
            let ceiling = policy_ceiling.min(intent_ceiling);
            intent
                .limit_price()
                .map_or(ceiling, |limit| ceiling.min(limit))
        }
    };
    ExecutionPriceBound::try_new(maximum_price).ok()
}

fn checked_upper_price(price: PriceTicks, basis_points: i32) -> Option<PriceTicks> {
    if price.get() <= 0 || !(0..=10_000).contains(&basis_points) {
        return None;
    }
    let factor = 10_000_i128.checked_add(i128::from(basis_points))?;
    let numerator = i128::from(price.get()).checked_mul(factor)?;
    let quotient = numerator / 10_000_i128;
    let remainder = numerator % 10_000_i128;
    let ceiling = if remainder == 0 {
        quotient
    } else {
        quotient.checked_add(1)?
    };
    Some(PriceTicks::new(i64::try_from(ceiling).ok()?))
}

fn deviation_exceeds(reference: PriceTicks, candidate: PriceTicks, maximum_bps: i32) -> bool {
    let reference = i128::from(reference.get());
    let candidate = i128::from(candidate.get());
    let difference = (candidate - reference).unsigned_abs();
    let reference = reference.unsigned_abs();
    difference * 10_000 > reference * maximum_bps.unsigned_abs() as u128
}

const fn market_freshness_expired(now: Timestamp, valid_until: Timestamp) -> bool {
    now.unix_nanos() >= valid_until.unix_nanos()
}

#[cfg(test)]
mod tests {
    use super::market_freshness_expired;
    use market_squawk_domain::Timestamp;

    #[test]
    fn market_freshness_deadline_is_exclusive() {
        let deadline = Timestamp::from_unix_nanos(100);
        assert!(!market_freshness_expired(
            Timestamp::from_unix_nanos(99),
            deadline,
        ));
        assert!(market_freshness_expired(deadline, deadline));
    }
}
