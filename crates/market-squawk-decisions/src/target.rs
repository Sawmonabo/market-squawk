//! Immutable investment targets, reviews, and invalidation evidence.

use market_squawk_domain::{InstrumentId, Money, RevisionNumber, Timestamp};

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

/// Immutable invalidation evidence for one exact target revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetInvalidation {
    id: TargetInvalidationId,
    target_id: InvestmentTargetSetId,
    target_revision: RevisionNumber,
    kind: InvalidationKind,
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
