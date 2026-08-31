//! Least-authority personalized-recommendation setup contracts.

mod profile;

use market_squawk_domain::{AccountId, Currency, DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_portfolio::PortfolioRevisionToken;
use sha2::{Digest as _, Sha256};

use crate::portfolio_application::PortfolioAccountHead;

pub(crate) use profile::{
    RecommendationProfileInput, RecommendationSetupAuthority, RecommendationSetupConfirmation,
    RecommendationSetupError, RecommendationSetupPreview, RecommendationSetupPreviewId,
    RecommendationSetupPreviewKind, RecommendationSetupReceipt, RecommendationSetupSnapshot,
    RetainedRecommendationSetupBackup, SelectedRecommendationAccount,
    UserConfirmedAllocationProfile,
};

const SETUP_REQUIRED_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/personalized-recommendation-setup-required/v1\0";

/// Exact durable-setup and current-catalog identities retained by every setup abstention.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecommendationSetupEvidence {
    authority_revision: u64,
    authority_digest: [u8; 32],
    catalog_digest: EvidenceDigest,
    available_accounts: u64,
    as_of: Timestamp,
    selected_account_id: Option<AccountId>,
    confirmed_reporting_currency: Option<Currency>,
    confirmed_portfolio_revision: Option<[u8; 32]>,
    current_reporting_currency: Option<Currency>,
    current_portfolio_revision: Option<[u8; 32]>,
    current_portfolio_effective_at: Option<Timestamp>,
    current_portfolio_available_at: Option<Timestamp>,
    profile_digest: Option<[u8; 32]>,
    profile_review_due_at: Option<Timestamp>,
}

impl RecommendationSetupEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "abstention evidence preserves confirmed setup and current portfolio facts separately"
    )]
    pub(super) const fn new(
        authority_revision: u64,
        authority_digest: [u8; 32],
        catalog_digest: EvidenceDigest,
        available_accounts: u64,
        as_of: Timestamp,
        selected_account_id: Option<AccountId>,
        confirmed_reporting_currency: Option<Currency>,
        confirmed_portfolio_revision: Option<[u8; 32]>,
        current_reporting_currency: Option<Currency>,
        current_portfolio_revision: Option<[u8; 32]>,
        current_portfolio_effective_at: Option<Timestamp>,
        current_portfolio_available_at: Option<Timestamp>,
        profile_digest: Option<[u8; 32]>,
        profile_review_due_at: Option<Timestamp>,
    ) -> Self {
        Self {
            authority_revision,
            authority_digest,
            catalog_digest,
            available_accounts,
            as_of,
            selected_account_id,
            confirmed_reporting_currency,
            confirmed_portfolio_revision,
            current_reporting_currency,
            current_portfolio_revision,
            current_portfolio_effective_at,
            current_portfolio_available_at,
            profile_digest,
            profile_review_due_at,
        }
    }

    /// Returns the exact durable setup revision observed during resolution.
    pub(crate) const fn authority_revision(&self) -> u64 {
        self.authority_revision
    }

    /// Returns the canonical identity of the complete durable setup document.
    pub(crate) const fn authority_digest(&self) -> [u8; 32] {
        self.authority_digest
    }

    /// Returns the canonical identity of every ordered current portfolio-account head.
    pub(crate) const fn catalog_digest(&self) -> EvidenceDigest {
        self.catalog_digest
    }

    /// Returns the complete bounded current account count.
    pub(crate) const fn available_accounts(&self) -> u64 {
        self.available_accounts
    }

    /// Returns the caller-selected point in time at which setup was evaluated.
    pub(crate) const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns the durable selected account, when one was explicitly confirmed.
    pub(crate) const fn selected_account_id(&self) -> Option<AccountId> {
        self.selected_account_id
    }

    /// Returns the reporting currency explicitly confirmed with the selected account.
    pub(crate) const fn confirmed_reporting_currency(&self) -> Option<Currency> {
        self.confirmed_reporting_currency
    }

    /// Returns the portfolio head observed at confirmation, when configured.
    pub(crate) fn confirmed_portfolio_revision(&self) -> Option<PortfolioRevisionToken> {
        self.confirmed_portfolio_revision
            .map(PortfolioRevisionToken::from_bytes)
    }

    /// Returns the current selected-account head currency, independently of confirmation facts.
    pub(crate) const fn current_reporting_currency(&self) -> Option<Currency> {
        self.current_reporting_currency
    }

    /// Returns the current selected-account head revision, independently of confirmation facts.
    pub(crate) fn current_portfolio_revision(&self) -> Option<PortfolioRevisionToken> {
        self.current_portfolio_revision
            .map(PortfolioRevisionToken::from_bytes)
    }

    pub(crate) const fn current_portfolio_effective_at(&self) -> Option<Timestamp> {
        self.current_portfolio_effective_at
    }

    pub(crate) const fn current_portfolio_available_at(&self) -> Option<Timestamp> {
        self.current_portfolio_available_at
    }

    /// Returns the exact account-bound profile identity, when configured.
    pub(crate) const fn profile_digest(&self) -> Option<[u8; 32]> {
        self.profile_digest
    }

    pub(crate) const fn profile_review_due_at(&self) -> Option<Timestamp> {
        self.profile_review_due_at
    }
}

/// Stable reason family for a personalized recommendation setup abstention.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SetupRequiredKind {
    /// No account has been explicitly and durably selected, or the selection disappeared.
    NoDefaultAccount,
    /// Multiple current accounts exist and none has been explicitly selected.
    AmbiguousAccounts,
    /// The selected current portfolio head lacks point-in-time availability evidence.
    PortfolioEvidenceUnavailable,
    /// The explicitly confirmed allocation profile reached its mandatory review time.
    ProfileReviewRequired,
}

impl SetupRequiredKind {
    /// Returns the stable canonical encoding used by evidence and transport adapters.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NoDefaultAccount => "no_default_account",
            Self::AmbiguousAccounts => "ambiguous_accounts",
            Self::PortfolioEvidenceUnavailable => "portfolio_evidence_unavailable",
            Self::ProfileReviewRequired => "profile_review_required",
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::NoDefaultAccount => 1,
            Self::AmbiguousAccounts => 2,
            Self::PortfolioEvidenceUnavailable => 3,
            Self::ProfileReviewRequired => 4,
        }
    }
}

/// Typed, evidence-retaining reason personalized sizing cannot currently be produced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SetupRequired {
    /// No explicit current durable account selection exists.
    NoDefaultAccount(RecommendationSetupEvidence),
    /// More than one account exists and choosing one requires the user.
    AmbiguousAccounts(RecommendationSetupEvidence),
    /// The selected account exists but its current head cannot support point-in-time analysis.
    PortfolioEvidenceUnavailable(RecommendationSetupEvidence),
    /// The profile is retained but cannot personalize analysis until the user reviews it.
    ProfileReviewRequired(RecommendationSetupEvidence),
}

impl SetupRequired {
    pub(super) const fn no_default_account(evidence: RecommendationSetupEvidence) -> Self {
        Self::NoDefaultAccount(evidence)
    }

    pub(super) const fn ambiguous_accounts(evidence: RecommendationSetupEvidence) -> Self {
        Self::AmbiguousAccounts(evidence)
    }

    pub(super) const fn portfolio_evidence_unavailable(
        evidence: RecommendationSetupEvidence,
    ) -> Self {
        Self::PortfolioEvidenceUnavailable(evidence)
    }

    pub(super) const fn profile_review_required(evidence: RecommendationSetupEvidence) -> Self {
        Self::ProfileReviewRequired(evidence)
    }

    /// Returns the stable reason family without erasing the retained evidence.
    pub(crate) const fn kind(&self) -> SetupRequiredKind {
        match self {
            Self::NoDefaultAccount(_) => SetupRequiredKind::NoDefaultAccount,
            Self::AmbiguousAccounts(_) => SetupRequiredKind::AmbiguousAccounts,
            Self::PortfolioEvidenceUnavailable(_) => {
                SetupRequiredKind::PortfolioEvidenceUnavailable
            }
            Self::ProfileReviewRequired(_) => SetupRequiredKind::ProfileReviewRequired,
        }
    }

    /// Returns the exact setup and portfolio-catalog facts behind this abstention.
    pub(crate) const fn evidence(&self) -> &RecommendationSetupEvidence {
        match self {
            Self::NoDefaultAccount(evidence)
            | Self::AmbiguousAccounts(evidence)
            | Self::PortfolioEvidenceUnavailable(evidence)
            | Self::ProfileReviewRequired(evidence) => evidence,
        }
    }

    /// Returns the versioned SHA-256 identity of the distinct reason and all retained evidence.
    pub(crate) fn digest(&self) -> [u8; 32] {
        let evidence = self.evidence();
        let mut digest = Sha256::new();
        digest.update(SETUP_REQUIRED_DIGEST_DOMAIN);
        digest.update([self.kind().tag()]);
        digest.update(evidence.authority_revision.to_be_bytes());
        digest.update(evidence.authority_digest);
        update_evidence_digest(&mut digest, evidence.catalog_digest);
        digest.update(evidence.available_accounts.to_be_bytes());
        digest.update(evidence.as_of.unix_nanos().to_be_bytes());
        update_optional_account(&mut digest, evidence.selected_account_id);
        update_optional_currency(&mut digest, evidence.confirmed_reporting_currency);
        update_optional_bytes(&mut digest, evidence.confirmed_portfolio_revision);
        update_optional_currency(&mut digest, evidence.current_reporting_currency);
        update_optional_bytes(&mut digest, evidence.current_portfolio_revision);
        update_optional_timestamp(&mut digest, evidence.current_portfolio_effective_at);
        update_optional_timestamp(&mut digest, evidence.current_portfolio_available_at);
        update_optional_bytes(&mut digest, evidence.profile_digest);
        update_optional_timestamp(&mut digest, evidence.profile_review_due_at);
        digest.finalize().into()
    }
}

/// Exact configured setup joined to the latest matching current portfolio head.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedRecommendationSetup {
    authority_revision: u64,
    authority_digest: [u8; 32],
    configuration_digest: [u8; 32],
    selected_account: SelectedRecommendationAccount,
    profile: UserConfirmedAllocationProfile,
    current_head: PortfolioAccountHead,
    catalog_digest: EvidenceDigest,
    available_accounts: u64,
    as_of: Timestamp,
}

impl ResolvedRecommendationSetup {
    #[allow(
        clippy::too_many_arguments,
        reason = "each durable, profile, and current portfolio identity remains explicit"
    )]
    pub(super) const fn new(
        authority_revision: u64,
        authority_digest: [u8; 32],
        configuration_digest: [u8; 32],
        selected_account: SelectedRecommendationAccount,
        profile: UserConfirmedAllocationProfile,
        current_head: PortfolioAccountHead,
        catalog_digest: EvidenceDigest,
        available_accounts: u64,
        as_of: Timestamp,
    ) -> Self {
        Self {
            authority_revision,
            authority_digest,
            configuration_digest,
            selected_account,
            profile,
            current_head,
            catalog_digest,
            available_accounts,
            as_of,
        }
    }

    pub(crate) const fn authority_revision(&self) -> u64 {
        self.authority_revision
    }

    pub(crate) const fn authority_digest(&self) -> [u8; 32] {
        self.authority_digest
    }

    pub(crate) const fn configuration_digest(&self) -> [u8; 32] {
        self.configuration_digest
    }

    pub(crate) const fn selected_account(&self) -> &SelectedRecommendationAccount {
        &self.selected_account
    }

    pub(crate) const fn profile(&self) -> &UserConfirmedAllocationProfile {
        &self.profile
    }

    pub(crate) const fn current_head(&self) -> &PortfolioAccountHead {
        &self.current_head
    }

    pub(crate) const fn catalog_digest(&self) -> EvidenceDigest {
        self.catalog_digest
    }

    pub(crate) const fn available_accounts(&self) -> u64 {
        self.available_accounts
    }

    pub(crate) const fn as_of(&self) -> Timestamp {
        self.as_of
    }
}

/// Closed durable-account resolution result; it never chooses an account for the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecommendationSetupResolution {
    /// Explicit durable setup resolved to one exact current head.
    Ready(ResolvedRecommendationSetup),
    /// Personalized analysis must abstain until the named setup condition is repaired.
    SetupRequired(SetupRequired),
}

impl RecommendationSetupResolution {
    pub(crate) const fn ready(&self) -> Option<&ResolvedRecommendationSetup> {
        match self {
            Self::Ready(ready) => Some(ready),
            Self::SetupRequired(_) => None,
        }
    }

    pub(crate) const fn setup_required(&self) -> Option<&SetupRequired> {
        match self {
            Self::Ready(_) => None,
            Self::SetupRequired(required) => Some(required),
        }
    }
}

fn update_evidence_digest(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(evidence.bytes());
}

fn update_optional_account(digest: &mut Sha256, account_id: Option<AccountId>) {
    match account_id {
        Some(account_id) => {
            digest.update([1]);
            digest.update(account_id.as_uuid().as_bytes());
        }
        None => digest.update([0]),
    }
}

fn update_optional_bytes(digest: &mut Sha256, value: Option<[u8; 32]>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value);
        }
        None => digest.update([0]),
    }
}

fn update_optional_currency(digest: &mut Sha256, value: Option<Currency>) {
    match value {
        Some(value) => {
            digest.update([1]);
            let value = value.as_str();
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
        None => digest.update([0]),
    }
}

fn update_optional_timestamp(digest: &mut Sha256, value: Option<Timestamp>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.unix_nanos().to_be_bytes());
        }
        None => digest.update([0]),
    }
}
