//! Immutable investment targets, reviews, and invalidation evidence.

use std::num::NonZeroU32;

use market_squawk_domain::{DataQuality, InstrumentId, Money, RevisionNumber, Timestamp};
use market_squawk_portfolio::PortfolioRevisionToken;
use market_squawk_valuation::DecisionId;

use crate::identity::{
    DecisionActorId, DecisionContentDigest, DecisionContractError, DossierId,
    InvestmentTargetSetId, TargetInvalidationId, TargetReviewId,
};

/// Exact reference mark and its evidence at target creation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReferenceMark {
    price: Money,
    observed_at: Timestamp,
    content_identity: DecisionContentDigest,
}

impl ReferenceMark {
    /// Constructs a nonnegative exact mark.
    ///
    /// # Errors
    ///
    /// Returns [`DecisionContractError::InvalidPriceOrder`] for a negative mark.
    pub fn try_new(
        price: Money,
        observed_at: Timestamp,
        content_identity: DecisionContentDigest,
    ) -> Result<Self, DecisionContractError> {
        if price.amount().is_sign_negative() {
            return Err(DecisionContractError::InvalidPriceOrder);
        }
        Ok(Self {
            price,
            observed_at,
            content_identity,
        })
    }

    /// Returns the exact marked price and currency.
    #[must_use]
    pub const fn price(self) -> Money {
        self.price
    }

    /// Returns the source observation time.
    #[must_use]
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    /// Returns the mark evidence identity.
    #[must_use]
    pub const fn content_identity(self) -> DecisionContentDigest {
        self.content_identity
    }
}

/// Inclusive exact price range with one currency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetPriceRange {
    lower: Money,
    upper: Money,
}

impl TargetPriceRange {
    /// Constructs an ordered inclusive price range.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies, negative prices, or `lower > upper`.
    pub fn try_new(lower: Money, upper: Money) -> Result<Self, DecisionContractError> {
        ensure_currency(lower, upper)?;
        if lower.amount().is_sign_negative()
            || upper.amount().is_sign_negative()
            || lower.amount() > upper.amount()
        {
            return Err(DecisionContractError::InvalidPriceOrder);
        }
        Ok(Self { lower, upper })
    }

    /// Returns the inclusive lower price.
    #[must_use]
    pub const fn lower(self) -> Money {
        self.lower
    }

    /// Returns the inclusive upper price.
    #[must_use]
    pub const fn upper(self) -> Money {
        self.upper
    }
}

/// Ordered downside, base, and upside exact target cases.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetPriceCases {
    downside: Money,
    base: Money,
    upside: Money,
}

impl TargetPriceCases {
    /// Constructs same-currency target cases ordered `downside <= base <= upside`.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies, negative prices, or reversed cases.
    pub fn try_new(
        downside: Money,
        base: Money,
        upside: Money,
    ) -> Result<Self, DecisionContractError> {
        ensure_currency(downside, base)?;
        ensure_currency(base, upside)?;
        if downside.amount().is_sign_negative()
            || base.amount().is_sign_negative()
            || upside.amount().is_sign_negative()
            || downside.amount() > base.amount()
            || base.amount() > upside.amount()
        {
            return Err(DecisionContractError::InvalidPriceOrder);
        }
        Ok(Self {
            downside,
            base,
            upside,
        })
    }

    /// Returns the downside case.
    #[must_use]
    pub const fn downside(self) -> Money {
        self.downside
    }

    /// Returns the base case.
    #[must_use]
    pub const fn base(self) -> Money {
        self.base
    }

    /// Returns the upside case.
    #[must_use]
    pub const fn upside(self) -> Money {
        self.upside
    }
}

/// Immutable investment target revision; it is not a portfolio rebalance target or order intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvestmentTargetSet {
    id: InvestmentTargetSetId,
    revision: RevisionNumber,
    dossier_id: DossierId,
    instrument_id: InstrumentId,
    reference_mark: ReferenceMark,
    cases: TargetPriceCases,
    entry_range: TargetPriceRange,
    trim_range: TargetPriceRange,
    exit_range: TargetPriceRange,
    created_at: Timestamp,
    horizon_at: Timestamp,
    expires_at: Timestamp,
    content_identity: DecisionContentDigest,
}

impl InvestmentTargetSet {
    /// Constructs one immutable target revision with coherent currency and time semantics.
    ///
    /// Construction does not activate the target. Activation requires separate immutable
    /// [`TargetReview`] evidence and never grants execution authority.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies or inconsistent evidence, creation, horizon, and expiry times.
    #[allow(
        clippy::too_many_arguments,
        reason = "independent dossier, financial, temporal, revision, and content identities remain explicit"
    )]
    pub fn try_new(
        id: InvestmentTargetSetId,
        revision: RevisionNumber,
        dossier_id: DossierId,
        instrument_id: InstrumentId,
        reference_mark: ReferenceMark,
        cases: TargetPriceCases,
        entry_range: TargetPriceRange,
        trim_range: TargetPriceRange,
        exit_range: TargetPriceRange,
        created_at: Timestamp,
        horizon_at: Timestamp,
        expires_at: Timestamp,
        content_identity: DecisionContentDigest,
    ) -> Result<Self, DecisionContractError> {
        let currency = reference_mark.price.currency();
        if [
            cases.downside.currency(),
            cases.base.currency(),
            cases.upside.currency(),
            entry_range.lower.currency(),
            entry_range.upper.currency(),
            trim_range.lower.currency(),
            trim_range.upper.currency(),
            exit_range.lower.currency(),
            exit_range.upper.currency(),
        ]
        .into_iter()
        .any(|candidate| candidate != currency)
        {
            return Err(DecisionContractError::CurrencyMismatch);
        }
        if reference_mark.observed_at > created_at
            || created_at > horizon_at
            || horizon_at > expires_at
        {
            return Err(DecisionContractError::InvalidTimeOrder);
        }
        Ok(Self {
            id,
            revision,
            dossier_id,
            instrument_id,
            reference_mark,
            cases,
            entry_range,
            trim_range,
            exit_range,
            created_at,
            horizon_at,
            expires_at,
            content_identity,
        })
    }

    /// Returns the stable target-set identity.
    #[must_use]
    pub const fn id(&self) -> &InvestmentTargetSetId {
        &self.id
    }

    /// Returns the one-based immutable target revision.
    #[must_use]
    pub const fn revision(&self) -> RevisionNumber {
        self.revision
    }

    /// Returns the dossier identity that supports this target.
    #[must_use]
    pub const fn dossier_id(&self) -> &DossierId {
        &self.dossier_id
    }

    /// Returns the target instrument.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact reference mark.
    #[must_use]
    pub const fn reference_mark(&self) -> ReferenceMark {
        self.reference_mark
    }

    /// Returns the downside, base, and upside target cases.
    #[must_use]
    pub const fn cases(&self) -> TargetPriceCases {
        self.cases
    }

    /// Returns the entry range.
    #[must_use]
    pub const fn entry_range(&self) -> TargetPriceRange {
        self.entry_range
    }

    /// Returns the trim range.
    #[must_use]
    pub const fn trim_range(&self) -> TargetPriceRange {
        self.trim_range
    }

    /// Returns the exit range.
    #[must_use]
    pub const fn exit_range(&self) -> TargetPriceRange {
        self.exit_range
    }

    /// Returns the target creation time.
    #[must_use]
    pub const fn created_at(&self) -> Timestamp {
        self.created_at
    }

    /// Returns the analytical horizon time.
    #[must_use]
    pub const fn horizon_at(&self) -> Timestamp {
        self.horizon_at
    }

    /// Returns the time after which the target cannot be activated.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the commitment to the complete target content.
    #[must_use]
    pub const fn content_identity(&self) -> DecisionContentDigest {
        self.content_identity
    }
}

/// Closed reviewer disposition for one immutable target revision.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TargetReviewDisposition {
    /// The reviewed revision may become active under the later Task 11 authority.
    Activate,
    /// The reviewed revision is rejected.
    Reject,
    /// The reviewed revision requires a successor before activation.
    NeedsChanges,
}

/// Immutable review evidence appended to one exact target revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetReview {
    id: TargetReviewId,
    target_id: InvestmentTargetSetId,
    target_revision: RevisionNumber,
    reviewer: DecisionActorId,
    reviewed_at: Timestamp,
    disposition: TargetReviewDisposition,
    content_identity: DecisionContentDigest,
}

impl TargetReview {
    /// Constructs review evidence without mutating the reviewed target.
    ///
    /// # Errors
    ///
    /// Rejects reviews before target creation and activation at or after expiry.
    pub fn try_new(
        id: TargetReviewId,
        target: &InvestmentTargetSet,
        reviewer: DecisionActorId,
        reviewed_at: Timestamp,
        disposition: TargetReviewDisposition,
        content_identity: DecisionContentDigest,
    ) -> Result<Self, DecisionContractError> {
        if reviewed_at < target.created_at {
            return Err(DecisionContractError::InvalidTimeOrder);
        }
        if matches!(disposition, TargetReviewDisposition::Activate)
            && reviewed_at >= target.expires_at
        {
            return Err(DecisionContractError::ExpiredActivation);
        }
        Ok(Self {
            id,
            target_id: target.id.clone(),
            target_revision: target.revision,
            reviewer,
            reviewed_at,
            disposition,
            content_identity,
        })
    }

    /// Returns the review identity.
    #[must_use]
    pub const fn id(&self) -> &TargetReviewId {
        &self.id
    }

    /// Returns the reviewed target-set identity.
    #[must_use]
    pub const fn target_id(&self) -> &InvestmentTargetSetId {
        &self.target_id
    }

    /// Returns the exact reviewed target revision.
    #[must_use]
    pub const fn target_revision(&self) -> RevisionNumber {
        self.target_revision
    }

    /// Returns the reviewer identity.
    #[must_use]
    pub const fn reviewer(&self) -> &DecisionActorId {
        &self.reviewer
    }

    /// Returns when review occurred.
    #[must_use]
    pub const fn reviewed_at(&self) -> Timestamp {
        self.reviewed_at
    }

    /// Returns the closed review disposition.
    #[must_use]
    pub const fn disposition(&self) -> TargetReviewDisposition {
        self.disposition
    }

    /// Returns the complete review content identity.
    #[must_use]
    pub const fn content_identity(&self) -> DecisionContentDigest {
        self.content_identity
    }
}

/// Closed source of evidence that can force target re-review.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum InvalidationKind {
    /// A corporate action changed instrument economics or comparability.
    CorporateAction,
    /// An admitted model or forecast was superseded or revoked.
    Model,
    /// Point-in-time data, a dataset, or its lineage changed.
    Data,
    /// The target's reference mark became stale or moved outside policy.
    ReferenceMark,
    /// A recorded thesis or valuation assumption failed.
    Assumption,
}

impl InvalidationKind {
    /// Complete invalidation family set used by deterministic evidence scanners.
    pub const ALL: [Self; 5] = [
        Self::CorporateAction,
        Self::Model,
        Self::Data,
        Self::ReferenceMark,
        Self::Assumption,
    ];
}

/// Immutable invalidation evidence for one exact target revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetInvalidation {
    id: TargetInvalidationId,
    target_id: InvestmentTargetSetId,
    target_revision: RevisionNumber,
    kind: InvalidationKind,
    actor: Option<DecisionActorId>,
    observed_at: Timestamp,
    content_identity: DecisionContentDigest,
}

impl TargetInvalidation {
    /// Constructs append-only invalidation evidence without replacing or mutating a target.
    ///
    /// # Errors
    ///
    /// Rejects evidence observed before target creation.
    pub fn try_new(
        id: TargetInvalidationId,
        target: &InvestmentTargetSet,
        kind: InvalidationKind,
        actor: DecisionActorId,
        observed_at: Timestamp,
        content_identity: DecisionContentDigest,
    ) -> Result<Self, DecisionContractError> {
        Self::try_new_inner(id, target, kind, Some(actor), observed_at, content_identity)
    }

    /// Recovers a schema-v1 invalidation that predates persisted governance principals.
    ///
    /// New workflow commits must use [`Self::try_new`], which always binds the authenticated actor
    /// selected by the service-owned governance session. This compatibility constructor exists only
    /// to preserve old immutable journal evidence during one-way recovery.
    pub fn try_new_legacy(
        id: TargetInvalidationId,
        target: &InvestmentTargetSet,
        kind: InvalidationKind,
        observed_at: Timestamp,
        content_identity: DecisionContentDigest,
    ) -> Result<Self, DecisionContractError> {
        Self::try_new_inner(id, target, kind, None, observed_at, content_identity)
    }

    fn try_new_inner(
        id: TargetInvalidationId,
        target: &InvestmentTargetSet,
        kind: InvalidationKind,
        actor: Option<DecisionActorId>,
        observed_at: Timestamp,
        content_identity: DecisionContentDigest,
    ) -> Result<Self, DecisionContractError> {
        if observed_at < target.created_at {
            return Err(DecisionContractError::InvalidTimeOrder);
        }
        Ok(Self {
            id,
            target_id: target.id.clone(),
            target_revision: target.revision,
            kind,
            actor,
            observed_at,
            content_identity,
        })
    }

    /// Returns the invalidation identity.
    #[must_use]
    pub const fn id(&self) -> &TargetInvalidationId {
        &self.id
    }

    /// Returns the affected target-set identity.
    #[must_use]
    pub const fn target_id(&self) -> &InvestmentTargetSetId {
        &self.target_id
    }

    /// Returns the exact affected target revision.
    #[must_use]
    pub const fn target_revision(&self) -> RevisionNumber {
        self.target_revision
    }

    /// Returns the invalidation family.
    #[must_use]
    pub const fn kind(&self) -> InvalidationKind {
        self.kind
    }

    /// Authenticated governance principal that committed this invalidation, when the immutable
    /// record was created by the current V1 workflow. `None` is retained only for recovered
    /// schema-v1 journal evidence that predates principal persistence.
    #[must_use]
    pub const fn actor(&self) -> Option<&DecisionActorId> {
        self.actor.as_ref()
    }

    /// Returns when invalidating evidence was observed.
    #[must_use]
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns the complete invalidation evidence identity.
    #[must_use]
    pub const fn content_identity(&self) -> DecisionContentDigest {
        self.content_identity
    }
}

fn ensure_currency(left: Money, right: Money) -> Result<(), DecisionContractError> {
    if left.currency() == right.currency() {
        Ok(())
    } else {
        Err(DecisionContractError::CurrencyMismatch)
    }
}

/// Maximum UTF-8 bytes retained in one target narrative value.
pub const MAX_DECISION_TEXT_BYTES: usize = 4_096;
/// Maximum assumptions, risks, or invalidation conditions retained by one target revision.
pub const MAX_TARGET_NARRATIVE_ITEMS: usize = 32;

/// Bounded canonical human decision narrative with no executable meaning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecisionText(Box<str>);

impl DecisionText {
    /// Constructs trimmed nonempty text without ASCII control characters.
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, DecisionContractError> {
        let value = value.as_ref();
        if value.is_empty()
            || value.len() > MAX_DECISION_TEXT_BYTES
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            return Err(DecisionContractError::InvalidText);
        }
        Ok(Self(value.into()))
    }

    /// Narrative text without allocation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Closed analytical method used to produce a target set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetMethod {
    /// Comparable-company or instrument evidence.
    ComparableEvidence,
    /// Discounted-cash-flow evidence.
    DiscountedCashFlow,
    /// Residual-income evidence.
    ResidualIncome,
    /// Admitted forecast distribution.
    ForecastDistribution,
    /// Approved fair-value measurement evidence.
    FairValueMeasurement,
}

/// One human-readable assumption bound to exact evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetAssumption {
    text: DecisionText,
    evidence_identity: DecisionContentDigest,
}

impl TargetAssumption {
    /// Constructs one bounded, evidence-bound assumption.
    #[must_use]
    pub const fn new(text: DecisionText, evidence_identity: DecisionContentDigest) -> Self {
        Self {
            text,
            evidence_identity,
        }
    }

    /// Assumption narrative.
    #[must_use]
    pub const fn text(&self) -> &DecisionText {
        &self.text
    }

    /// Exact evidence commitment.
    #[must_use]
    pub const fn evidence_identity(&self) -> DecisionContentDigest {
        self.evidence_identity
    }
}

/// Exact dossier and optional portfolio revision supporting the decision context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetDecisionContext {
    dossier_id: DossierId,
    portfolio_revision: Option<PortfolioRevisionToken>,
}

impl TargetDecisionContext {
    /// Constructs a reference-only decision context.
    #[must_use]
    pub const fn new(
        dossier_id: DossierId,
        portfolio_revision: Option<PortfolioRevisionToken>,
    ) -> Self {
        Self {
            dossier_id,
            portfolio_revision,
        }
    }

    /// Supporting dossier identity.
    #[must_use]
    pub const fn dossier_id(&self) -> &DossierId {
        &self.dossier_id
    }

    /// Immutable portfolio revision, when portfolio impact informed the target.
    #[must_use]
    pub const fn portfolio_revision(&self) -> Option<&PortfolioRevisionToken> {
        self.portfolio_revision.as_ref()
    }
}

/// Existing upstream forecast and fair-value evidence identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetEvidence {
    forecast: Option<DecisionContentDigest>,
    fair_value: Option<DecisionId>,
}

impl TargetEvidence {
    /// Constructs a path-free evidence set without changing valuation classification.
    #[must_use]
    pub const fn new(
        forecast: Option<DecisionContentDigest>,
        fair_value: Option<DecisionId>,
    ) -> Self {
        Self {
            forecast,
            fair_value,
        }
    }

    /// Forecast evidence identity, when used.
    #[must_use]
    pub const fn forecast(self) -> Option<DecisionContentDigest> {
        self.forecast
    }

    /// Existing fair-value classification decision, when used.
    #[must_use]
    pub const fn fair_value(self) -> Option<DecisionId> {
        self.fair_value
    }
}

/// Validated construction input for a complete governed target revision.
#[derive(Clone, Debug)]
pub struct TargetGovernanceInput {
    /// Frozen Task 7 financial, identity, and core-time contract.
    pub target: InvestmentTargetSet,
    /// Add case retained separately from base/upside/downside and trading ranges.
    pub add_case: Money,
    /// Closed analytical method.
    pub method: TargetMethod,
    /// Evidence-bound analytical assumptions.
    pub assumptions: Vec<TargetAssumption>,
    /// Dossier and optional portfolio context.
    pub decision_context: TargetDecisionContext,
    /// Earliest time this revision may be considered after review.
    pub effective_at: Timestamp,
    /// Mandatory review time.
    pub review_due_at: Timestamp,
    /// Exact prior revision and supersession time for a successor.
    pub supersedes: Option<(RevisionNumber, Timestamp)>,
    /// Primary target thesis.
    pub thesis: DecisionText,
    /// Bounded risk narratives.
    pub risks: Vec<DecisionText>,
    /// Bounded explicit invalidation conditions.
    pub invalidation_conditions: Vec<DecisionText>,
    /// Existing forecast and fair-value references.
    pub evidence: TargetEvidence,
    /// Quality of the exact reference mark.
    pub mark_quality: DataQuality,
    /// Target author.
    pub author: DecisionActorId,
    /// Code-owned governance ruleset version.
    pub ruleset_version: NonZeroU32,
}

/// Complete immutable target revision; it owns no order or valuation-classification authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GovernedTargetSet {
    target: InvestmentTargetSet,
    add_case: Money,
    method: TargetMethod,
    assumptions: Box<[TargetAssumption]>,
    decision_context: TargetDecisionContext,
    effective_at: Timestamp,
    review_due_at: Timestamp,
    supersedes: Option<(RevisionNumber, Timestamp)>,
    thesis: DecisionText,
    risks: Box<[DecisionText]>,
    invalidation_conditions: Box<[DecisionText]>,
    evidence: TargetEvidence,
    mark_quality: DataQuality,
    author: DecisionActorId,
    ruleset_version: NonZeroU32,
}

impl GovernedTargetSet {
    /// Validates complete financial, temporal, narrative, evidence, and supersession governance.
    pub fn try_new(input: TargetGovernanceInput) -> Result<Self, DecisionContractError> {
        let target = &input.target;
        let expected_prior = target.revision().get().checked_sub(1);
        let supersession_valid = match (target.revision().get(), input.supersedes) {
            (1, None) => true,
            (revision, Some((prior, at))) => {
                expected_prior == Some(prior.get())
                    && prior.get() < revision
                    && at >= target.created_at()
                    && at <= input.effective_at
            }
            _ => false,
        };
        if input.add_case.currency() != target.reference_mark().price().currency()
            || input.add_case.amount().is_sign_negative()
            || target.dossier_id() != input.decision_context.dossier_id()
            || input.assumptions.is_empty()
            || input.assumptions.len() > MAX_TARGET_NARRATIVE_ITEMS
            || input.risks.is_empty()
            || input.risks.len() > MAX_TARGET_NARRATIVE_ITEMS
            || input.invalidation_conditions.is_empty()
            || input.invalidation_conditions.len() > MAX_TARGET_NARRATIVE_ITEMS
            || input.effective_at < target.created_at()
            || input.effective_at >= target.expires_at()
            || input.review_due_at < input.effective_at
            || input.review_due_at > target.expires_at()
            || (input.evidence.forecast.is_none() && input.evidence.fair_value.is_none())
            || (matches!(input.method, TargetMethod::ForecastDistribution)
                && input.evidence.forecast.is_none())
            || (matches!(input.method, TargetMethod::FairValueMeasurement)
                && input.evidence.fair_value.is_none())
            || !supersession_valid
        {
            return Err(DecisionContractError::InvalidTargetGovernance);
        }
        Ok(Self {
            target: input.target,
            add_case: input.add_case,
            method: input.method,
            assumptions: input.assumptions.into_boxed_slice(),
            decision_context: input.decision_context,
            effective_at: input.effective_at,
            review_due_at: input.review_due_at,
            supersedes: input.supersedes,
            thesis: input.thesis,
            risks: input.risks.into_boxed_slice(),
            invalidation_conditions: input.invalidation_conditions.into_boxed_slice(),
            evidence: input.evidence,
            mark_quality: input.mark_quality,
            author: input.author,
            ruleset_version: input.ruleset_version,
        })
    }

    /// Frozen financial and identity core.
    #[must_use]
    pub const fn target(&self) -> &InvestmentTargetSet {
        &self.target
    }

    /// Separate add case.
    #[must_use]
    pub const fn add_case(&self) -> Money {
        self.add_case
    }

    /// Analytical method.
    #[must_use]
    pub const fn method(&self) -> TargetMethod {
        self.method
    }

    /// Evidence-bound assumptions.
    #[must_use]
    pub fn assumptions(&self) -> &[TargetAssumption] {
        &self.assumptions
    }

    /// Dossier and portfolio decision context.
    #[must_use]
    pub const fn decision_context(&self) -> &TargetDecisionContext {
        &self.decision_context
    }

    /// Earliest review-activated effective time.
    #[must_use]
    pub const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    /// Mandatory review time.
    #[must_use]
    pub const fn review_due_at(&self) -> Timestamp {
        self.review_due_at
    }

    /// Exact prior revision and supersession time.
    #[must_use]
    pub const fn supersedes(&self) -> Option<(RevisionNumber, Timestamp)> {
        self.supersedes
    }

    /// Primary thesis.
    #[must_use]
    pub const fn thesis(&self) -> &DecisionText {
        &self.thesis
    }

    /// Risk narratives.
    #[must_use]
    pub fn risks(&self) -> &[DecisionText] {
        &self.risks
    }

    /// Explicit invalidation conditions.
    #[must_use]
    pub fn invalidation_conditions(&self) -> &[DecisionText] {
        &self.invalidation_conditions
    }

    /// Forecast and fair-value evidence references.
    #[must_use]
    pub const fn evidence(&self) -> TargetEvidence {
        self.evidence
    }

    /// Reference-mark quality.
    #[must_use]
    pub const fn mark_quality(&self) -> DataQuality {
        self.mark_quality
    }

    /// Target author.
    #[must_use]
    pub const fn author(&self) -> &DecisionActorId {
        &self.author
    }

    /// Code-owned governance ruleset version.
    #[must_use]
    pub const fn ruleset_version(&self) -> NonZeroU32 {
        self.ruleset_version
    }
}

/// Effective read-side status derived only from append-only target/review/invalidation records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetStatus {
    /// No explicit review has activated or rejected the revision.
    PendingReview,
    /// The latest lifecycle evidence is an activation review.
    Active,
    /// The latest review rejected the revision.
    Rejected,
    /// The latest review requires a successor.
    NeedsChanges,
    /// A later invalidation requires explicit review before activation.
    NeedsReview,
    /// A later immutable revision supersedes this revision.
    Superseded,
}

/// Owned read model for one target revision and its latest immutable governance evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetState {
    target: GovernedTargetSet,
    status: TargetStatus,
    latest_review: Option<TargetReview>,
    latest_invalidation: Option<TargetInvalidation>,
}

impl TargetState {
    pub(crate) const fn new(
        target: GovernedTargetSet,
        status: TargetStatus,
        latest_review: Option<TargetReview>,
        latest_invalidation: Option<TargetInvalidation>,
    ) -> Self {
        Self {
            target,
            status,
            latest_review,
            latest_invalidation,
        }
    }

    /// Immutable target content.
    #[must_use]
    pub const fn target(&self) -> &GovernedTargetSet {
        &self.target
    }

    /// Append-derived effective status.
    #[must_use]
    pub const fn status(&self) -> TargetStatus {
        self.status
    }

    /// Latest explicit reviewer evidence, including reviewer and review time.
    #[must_use]
    pub const fn latest_review(&self) -> Option<&TargetReview> {
        self.latest_review.as_ref()
    }

    /// Active approval evidence, available only while the derived status remains active.
    #[must_use]
    pub fn approval(&self) -> Option<&TargetReview> {
        if self.status == TargetStatus::Active {
            self.latest_review
                .as_ref()
                .filter(|review| matches!(review.disposition(), TargetReviewDisposition::Activate))
        } else {
            None
        }
    }

    /// Latest invalidation evidence, when any was appended.
    #[must_use]
    pub const fn latest_invalidation(&self) -> Option<&TargetInvalidation> {
        self.latest_invalidation.as_ref()
    }
}
