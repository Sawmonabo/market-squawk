//! Workspace-owned durable account selection and allocation-profile authority.

use std::{
    collections::BTreeMap,
    fmt,
    path::Path,
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_domain::{
    AccountId, Currency, DigestAlgorithm, EvidenceDigest, Money, Timestamp,
};
use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};
use market_squawk_portfolio::PortfolioRevisionToken;
use market_squawk_runtime::WorkspaceId;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::portfolio_application::{PortfolioAccountCatalogSnapshot, PortfolioAccountHead};

use super::{
    RecommendationSetupEvidence, RecommendationSetupResolution, ResolvedRecommendationSetup,
    SetupRequired,
};

const FORMAT_VERSION: u16 = 1;
const AUTHORITY_DIRECTORY: &str = "recommendation-setup-authority";
const MAXIMUM_PREVIEWS: usize = 32;
const MAXIMUM_PREVIEW_BYTES: usize = 64 * 1024;
const PREVIEW_ID_ATTEMPTS: usize = 16;
const PREVIEW_LIFETIME: Duration = Duration::from_secs(15 * 60);
const PREVIEW_LIFETIME_NANOS: i64 = 15 * 60 * 1_000_000_000;
const PROFILE_REVIEW_NANOS: i64 = 365 * 24 * 60 * 60 * 1_000_000_000;
const BACKUP_FORMAT_VERSION: u16 = 1;

const GENESIS_DIGEST_DOMAIN: &[u8] = b"market-squawk/recommendation-setup-genesis/v1\0";
const SELECTION_DIGEST_DOMAIN: &[u8] = b"market-squawk/recommendation-selected-account/v1\0";
const PROFILE_DIGEST_DOMAIN: &[u8] = b"market-squawk/recommendation-allocation-profile/v1\0";
const CONFIGURATION_DIGEST_DOMAIN: &[u8] = b"market-squawk/recommendation-configuration/v1\0";
const DOCUMENT_DIGEST_DOMAIN: &[u8] = b"market-squawk/recommendation-setup-document/v1\0";
const PREVIEW_DIGEST_DOMAIN: &[u8] = b"market-squawk/recommendation-setup-preview/v1\0";
const BACKUP_DIGEST_DOMAIN: &[u8] = b"market-squawk/recommendation-setup-backup/v1\0";

/// User-authored numeric allocation preferences before account/currency admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct RecommendationProfileInput {
    account_id: AccountId,
    preferred_position_weight_lower_bps: u16,
    preferred_position_weight_upper_bps: u16,
    minimum_cash_reserve: Money,
    maximum_downside_loss_bps_of_marked_equity: u16,
    available_investment_horizon_nanos: i64,
}

impl RecommendationProfileInput {
    /// Admits explicit numeric preferences without assigning a risk label or silent preset.
    pub(crate) fn try_new(
        account_id: AccountId,
        preferred_position_weight_lower_bps: u16,
        preferred_position_weight_upper_bps: u16,
        minimum_cash_reserve: Money,
        maximum_downside_loss_bps_of_marked_equity: u16,
        available_investment_horizon_nanos: i64,
    ) -> Result<Self, RecommendationSetupError> {
        let input = Self {
            account_id,
            preferred_position_weight_lower_bps,
            preferred_position_weight_upper_bps,
            minimum_cash_reserve: Money::new(
                minimum_cash_reserve.amount(),
                minimum_cash_reserve.currency(),
            ),
            maximum_downside_loss_bps_of_marked_equity,
            available_investment_horizon_nanos,
        };
        input.validate()?;
        Ok(input)
    }

    pub(crate) const fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub(crate) const fn preferred_position_weight_lower_bps(&self) -> u16 {
        self.preferred_position_weight_lower_bps
    }

    pub(crate) const fn preferred_position_weight_upper_bps(&self) -> u16 {
        self.preferred_position_weight_upper_bps
    }

    pub(crate) const fn minimum_cash_reserve(&self) -> Money {
        self.minimum_cash_reserve
    }

    pub(crate) const fn maximum_downside_loss_bps_of_marked_equity(&self) -> u16 {
        self.maximum_downside_loss_bps_of_marked_equity
    }

    pub(crate) const fn available_investment_horizon_nanos(&self) -> i64 {
        self.available_investment_horizon_nanos
    }

    fn validate(&self) -> Result<(), RecommendationSetupError> {
        if self.preferred_position_weight_lower_bps == 0
            || self.preferred_position_weight_lower_bps > self.preferred_position_weight_upper_bps
            || self.preferred_position_weight_upper_bps > 10_000
            || self.minimum_cash_reserve.amount().is_sign_negative()
            || self.maximum_downside_loss_bps_of_marked_equity == 0
            || self.maximum_downside_loss_bps_of_marked_equity > 10_000
            || self.available_investment_horizon_nanos <= 0
        {
            return Err(RecommendationSetupError::InvalidProfile);
        }
        Ok(())
    }

    fn validate_for_currency(
        &self,
        reporting_currency: Currency,
    ) -> Result<(), RecommendationSetupError> {
        self.validate()?;
        if self.minimum_cash_reserve.currency() != reporting_currency {
            return Err(RecommendationSetupError::CurrencyMismatch);
        }
        Ok(())
    }
}

/// Exact durable account choice; the portfolio token is confirmation evidence, not a frozen head.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SelectedRecommendationAccount {
    setup_revision: u64,
    account_id: AccountId,
    reporting_currency: Currency,
    confirmed_portfolio_revision: [u8; 32],
    confirmed_catalog_digest: EvidenceDigest,
    confirmed_at: Timestamp,
    digest: [u8; 32],
}

impl SelectedRecommendationAccount {
    fn try_new(
        setup_revision: u64,
        pending: &PendingRecommendationConfiguration,
        confirmed_at: Timestamp,
    ) -> Result<Self, RecommendationSetupError> {
        let mut selected = Self {
            setup_revision,
            account_id: pending.account_id,
            reporting_currency: pending.reporting_currency,
            confirmed_portfolio_revision: pending.portfolio_revision,
            confirmed_catalog_digest: pending.catalog_digest,
            confirmed_at,
            digest: [0; 32],
        };
        selected.digest = selection_digest(&selected)?;
        selected.validate()?;
        Ok(selected)
    }

    pub(crate) const fn setup_revision(&self) -> u64 {
        self.setup_revision
    }

    pub(crate) const fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub(crate) const fn reporting_currency(&self) -> Currency {
        self.reporting_currency
    }

    pub(crate) fn confirmed_portfolio_revision(&self) -> PortfolioRevisionToken {
        PortfolioRevisionToken::from_bytes(self.confirmed_portfolio_revision)
    }

    pub(crate) const fn confirmed_catalog_digest(&self) -> EvidenceDigest {
        self.confirmed_catalog_digest
    }

    pub(crate) const fn confirmed_at(&self) -> Timestamp {
        self.confirmed_at
    }

    pub(crate) const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    fn validate(&self) -> Result<(), RecommendationSetupError> {
        if self.setup_revision == 0
            || self.confirmed_portfolio_revision == [0; 32]
            || self.confirmed_catalog_digest.algorithm() != DigestAlgorithm::Sha256
            || self.confirmed_catalog_digest.bytes() == [0; 32]
            || self.confirmed_at.unix_nanos() <= 0
            || self.digest == [0; 32]
            || selection_digest(self)? != self.digest
        {
            return Err(RecommendationSetupError::CorruptState);
        }
        Ok(())
    }

    fn rebound_to_revision(&self, setup_revision: u64) -> Result<Self, RecommendationSetupError> {
        let mut rebound = self.clone();
        rebound.setup_revision = setup_revision;
        rebound.digest = selection_digest(&rebound)?;
        rebound.validate()?;
        Ok(rebound)
    }
}

/// Explicit numeric profile accepted for exactly one account and reporting currency.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct UserConfirmedAllocationProfile {
    setup_revision: u64,
    account_id: AccountId,
    reporting_currency: Currency,
    preferred_position_weight_lower_bps: u16,
    preferred_position_weight_upper_bps: u16,
    minimum_cash_reserve: Money,
    maximum_downside_loss_bps_of_marked_equity: u16,
    available_investment_horizon_nanos: i64,
    accepted_at: Timestamp,
    review_due_at: Timestamp,
    digest: [u8; 32],
}

impl UserConfirmedAllocationProfile {
    fn try_new(
        setup_revision: u64,
        input: &RecommendationProfileInput,
        reporting_currency: Currency,
        accepted_at: Timestamp,
        review_due_at: Timestamp,
    ) -> Result<Self, RecommendationSetupError> {
        input.validate_for_currency(reporting_currency)?;
        let mut profile = Self {
            setup_revision,
            account_id: input.account_id,
            reporting_currency,
            preferred_position_weight_lower_bps: input.preferred_position_weight_lower_bps,
            preferred_position_weight_upper_bps: input.preferred_position_weight_upper_bps,
            minimum_cash_reserve: Money::new(
                input.minimum_cash_reserve.amount(),
                reporting_currency,
            ),
            maximum_downside_loss_bps_of_marked_equity: input
                .maximum_downside_loss_bps_of_marked_equity,
            available_investment_horizon_nanos: input.available_investment_horizon_nanos,
            accepted_at,
            review_due_at,
            digest: [0; 32],
        };
        profile.digest = profile_digest(&profile)?;
        profile.validate()?;
        Ok(profile)
    }

    pub(crate) const fn setup_revision(&self) -> u64 {
        self.setup_revision
    }

    pub(crate) const fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub(crate) const fn reporting_currency(&self) -> Currency {
        self.reporting_currency
    }

    pub(crate) const fn preferred_position_weight_lower_bps(&self) -> u16 {
        self.preferred_position_weight_lower_bps
    }

    pub(crate) const fn preferred_position_weight_upper_bps(&self) -> u16 {
        self.preferred_position_weight_upper_bps
    }

    pub(crate) const fn minimum_cash_reserve(&self) -> Money {
        self.minimum_cash_reserve
    }

    pub(crate) const fn maximum_downside_loss_bps_of_marked_equity(&self) -> u16 {
        self.maximum_downside_loss_bps_of_marked_equity
    }

    pub(crate) const fn available_investment_horizon_nanos(&self) -> i64 {
        self.available_investment_horizon_nanos
    }

    pub(crate) const fn accepted_at(&self) -> Timestamp {
        self.accepted_at
    }

    pub(crate) const fn review_due_at(&self) -> Timestamp {
        self.review_due_at
    }

    pub(crate) const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    fn validate(&self) -> Result<(), RecommendationSetupError> {
        RecommendationProfileInput {
            account_id: self.account_id,
            preferred_position_weight_lower_bps: self.preferred_position_weight_lower_bps,
            preferred_position_weight_upper_bps: self.preferred_position_weight_upper_bps,
            minimum_cash_reserve: self.minimum_cash_reserve,
            maximum_downside_loss_bps_of_marked_equity: self
                .maximum_downside_loss_bps_of_marked_equity,
            available_investment_horizon_nanos: self.available_investment_horizon_nanos,
        }
        .validate_for_currency(self.reporting_currency)?;
        if self.setup_revision == 0
            || self.accepted_at.unix_nanos() <= 0
            || self.review_due_at <= self.accepted_at
            || self.digest == [0; 32]
            || profile_digest(self)? != self.digest
        {
            return Err(RecommendationSetupError::CorruptState);
        }
        Ok(())
    }

    fn rebound_to_revision(&self, setup_revision: u64) -> Result<Self, RecommendationSetupError> {
        let mut rebound = self.clone();
        rebound.setup_revision = setup_revision;
        rebound.digest = profile_digest(&rebound)?;
        rebound.validate()?;
        Ok(rebound)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RecommendationConfiguration {
    selected_account: SelectedRecommendationAccount,
    profile: UserConfirmedAllocationProfile,
    digest: [u8; 32],
}

impl RecommendationConfiguration {
    fn try_new(
        setup_revision: u64,
        pending: &PendingRecommendationConfiguration,
        accepted_at: Timestamp,
    ) -> Result<Self, RecommendationSetupError> {
        let review_due_at = accepted_at
            .checked_add_nanos(PROFILE_REVIEW_NANOS)
            .map_err(|_| RecommendationSetupError::TimeUnavailable)?;
        if review_due_at <= accepted_at {
            return Err(RecommendationSetupError::TimeUnavailable);
        }
        let selected_account =
            SelectedRecommendationAccount::try_new(setup_revision, pending, accepted_at)?;
        let profile = UserConfirmedAllocationProfile::try_new(
            setup_revision,
            &pending.profile,
            pending.reporting_currency,
            accepted_at,
            review_due_at,
        )?;
        let digest = configuration_digest(&selected_account, &profile);
        let configuration = Self {
            selected_account,
            profile,
            digest,
        };
        configuration.validate(setup_revision)?;
        Ok(configuration)
    }

    fn validate(&self, setup_revision: u64) -> Result<(), RecommendationSetupError> {
        self.selected_account.validate()?;
        self.profile.validate()?;
        if setup_revision == 0
            || self.selected_account.setup_revision != setup_revision
            || self.profile.setup_revision != setup_revision
            || self.selected_account.account_id != self.profile.account_id
            || self.selected_account.reporting_currency != self.profile.reporting_currency
            || self.selected_account.confirmed_at != self.profile.accepted_at
            || self.digest == [0; 32]
            || configuration_digest(&self.selected_account, &self.profile) != self.digest
        {
            return Err(RecommendationSetupError::CorruptState);
        }
        Ok(())
    }

    fn rebound_to_revision(&self, setup_revision: u64) -> Result<Self, RecommendationSetupError> {
        let selected_account = self.selected_account.rebound_to_revision(setup_revision)?;
        let profile = self.profile.rebound_to_revision(setup_revision)?;
        let rebound = Self {
            digest: configuration_digest(&selected_account, &profile),
            selected_account,
            profile,
        };
        rebound.validate(setup_revision)?;
        Ok(rebound)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RecommendationSetupDocument {
    format_version: u16,
    owner_workspace: WorkspaceId,
    revision: u64,
    previous_digest: [u8; 32],
    transition_at: Timestamp,
    configuration: Option<RecommendationConfiguration>,
    digest: [u8; 32],
}

impl RecommendationSetupDocument {
    fn try_new(
        owner_workspace: WorkspaceId,
        revision: u64,
        previous_digest: [u8; 32],
        transition_at: Timestamp,
        configuration: Option<RecommendationConfiguration>,
    ) -> Result<Self, RecommendationSetupError> {
        let mut document = Self {
            format_version: FORMAT_VERSION,
            owner_workspace,
            revision,
            previous_digest,
            transition_at,
            configuration,
            digest: [0; 32],
        };
        document.digest = document_digest(&document);
        document.validate(owner_workspace)?;
        Ok(document)
    }

    fn decode(
        encoded: &[u8],
        expected_owner: WorkspaceId,
    ) -> Result<Self, RecommendationSetupError> {
        if encoded.is_empty() || encoded.len() > LocalAuthorityStateStore::maximum_payload_bytes() {
            return Err(RecommendationSetupError::CorruptState);
        }
        let document = serde_json::from_slice::<Self>(encoded)
            .map_err(|_| RecommendationSetupError::CorruptState)?;
        document.validate(expected_owner)?;
        Ok(document)
    }

    fn encode(&self) -> Result<Vec<u8>, RecommendationSetupError> {
        self.validate(self.owner_workspace)?;
        let encoded = serde_json::to_vec(self).map_err(|_| RecommendationSetupError::Encoding)?;
        if encoded.is_empty() || encoded.len() > LocalAuthorityStateStore::maximum_payload_bytes() {
            return Err(RecommendationSetupError::CapacityExceeded);
        }
        Ok(encoded)
    }

    fn validate(&self, expected_owner: WorkspaceId) -> Result<(), RecommendationSetupError> {
        if self.format_version != FORMAT_VERSION
            || self.owner_workspace != expected_owner
            || self.revision == 0
            || self.previous_digest == [0; 32]
            || self.transition_at.unix_nanos() <= 0
            || self.digest == [0; 32]
            || (self.revision == 1 && self.previous_digest != genesis_digest(expected_owner))
            || document_digest(self) != self.digest
        {
            return Err(RecommendationSetupError::CorruptState);
        }
        if let Some(configuration) = &self.configuration {
            configuration.validate(self.revision)?;
            if configuration.selected_account.confirmed_at() != self.transition_at
                || configuration.profile.accepted_at() != self.transition_at
            {
                return Err(RecommendationSetupError::CorruptState);
            }
        }
        Ok(())
    }

    fn rebound_to_workspace(
        &self,
        target_workspace: WorkspaceId,
    ) -> Result<Self, RecommendationSetupError> {
        let configuration = self
            .configuration
            .as_ref()
            .map(|configuration| configuration.rebound_to_revision(1))
            .transpose()?;
        Self::try_new(
            target_workspace,
            1,
            genesis_digest(target_workspace),
            self.transition_at,
            configuration,
        )
    }
}

/// Immutable current durable recommendation-setup state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecommendationSetupSnapshot {
    revision: u64,
    digest: [u8; 32],
    transition_at: Option<Timestamp>,
    selected_account: Option<SelectedRecommendationAccount>,
    profile: Option<UserConfirmedAllocationProfile>,
    configuration_digest: Option<[u8; 32]>,
}

impl RecommendationSetupSnapshot {
    pub(crate) const fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub(crate) const fn transition_at(&self) -> Option<Timestamp> {
        self.transition_at
    }

    pub(crate) const fn selected_account(&self) -> Option<&SelectedRecommendationAccount> {
        self.selected_account.as_ref()
    }

    pub(crate) const fn profile(&self) -> Option<&UserConfirmedAllocationProfile> {
        self.profile.as_ref()
    }

    pub(crate) const fn configuration_digest(&self) -> Option<[u8; 32]> {
        self.configuration_digest
    }

    pub(crate) const fn is_configured(&self) -> bool {
        self.selected_account.is_some()
            && self.profile.is_some()
            && self.configuration_digest.is_some()
    }
}

/// Opaque one-process identity for an immutable setup preview.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub(crate) struct RecommendationSetupPreviewId(Uuid);

impl RecommendationSetupPreviewId {
    /// Decodes a non-nil preview identity at the transport boundary.
    pub(crate) fn try_from_uuid(value: Uuid) -> Result<Self, RecommendationSetupError> {
        if value.is_nil() {
            Err(RecommendationSetupError::InvalidConfirmation)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the stable UUID wire representation.
    pub(crate) const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl<'de> Deserialize<'de> for RecommendationSetupPreviewId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Uuid::deserialize(deserializer)?;
        Self::try_from_uuid(value).map_err(serde::de::Error::custom)
    }
}

/// The complete atomic setup transition represented by a preview.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecommendationSetupPreviewKind {
    /// Select one exact account and install its numeric profile in one commit.
    Configure,
    /// Clear both the selected account and its profile in one commit.
    Clear,
}

impl RecommendationSetupPreviewKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Configure => "configure",
            Self::Clear => "clear",
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::Configure => 1,
            Self::Clear => 2,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PendingRecommendationConfiguration {
    account_id: AccountId,
    reporting_currency: Currency,
    portfolio_revision: [u8; 32],
    catalog_digest: EvidenceDigest,
    profile: RecommendationProfileInput,
}

/// Bounded, immutable, one-process setup preview requiring exact confirmation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct RecommendationSetupPreview {
    format_version: u16,
    preview_id: RecommendationSetupPreviewId,
    owner_workspace: WorkspaceId,
    current_revision: u64,
    resulting_revision: u64,
    current_authority_digest: [u8; 32],
    catalog_digest: EvidenceDigest,
    kind: RecommendationSetupPreviewKind,
    configuration: Option<PendingRecommendationConfiguration>,
    issued_at: Timestamp,
    expires_at: Timestamp,
    digest: [u8; 32],
}

impl RecommendationSetupPreview {
    pub(crate) const fn preview_id(&self) -> RecommendationSetupPreviewId {
        self.preview_id
    }

    pub(crate) const fn owner_workspace(&self) -> WorkspaceId {
        self.owner_workspace
    }

    pub(crate) const fn current_revision(&self) -> u64 {
        self.current_revision
    }

    pub(crate) const fn resulting_revision(&self) -> u64 {
        self.resulting_revision
    }

    pub(crate) const fn current_authority_digest(&self) -> [u8; 32] {
        self.current_authority_digest
    }

    pub(crate) const fn catalog_digest(&self) -> EvidenceDigest {
        self.catalog_digest
    }

    pub(crate) const fn kind(&self) -> RecommendationSetupPreviewKind {
        self.kind
    }

    pub(crate) fn selected_account_id(&self) -> Option<AccountId> {
        self.configuration
            .as_ref()
            .map(|configuration| configuration.account_id)
    }

    pub(crate) fn selected_portfolio_revision(&self) -> Option<PortfolioRevisionToken> {
        self.configuration.as_ref().map(|configuration| {
            PortfolioRevisionToken::from_bytes(configuration.portfolio_revision)
        })
    }

    pub(crate) fn selected_reporting_currency(&self) -> Option<Currency> {
        self.configuration
            .as_ref()
            .map(|configuration| configuration.reporting_currency)
    }

    pub(crate) fn profile(&self) -> Option<&RecommendationProfileInput> {
        self.configuration
            .as_ref()
            .map(|configuration| &configuration.profile)
    }

    pub(crate) const fn issued_at(&self) -> Timestamp {
        self.issued_at
    }

    pub(crate) const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    pub(crate) const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Exact one-use preview confirmation. Supplying it conveys no order or risk authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecommendationSetupConfirmation {
    preview_id: RecommendationSetupPreviewId,
    preview_digest: [u8; 32],
}

impl RecommendationSetupConfirmation {
    pub(crate) const fn new(
        preview_id: RecommendationSetupPreviewId,
        preview_digest: [u8; 32],
    ) -> Self {
        Self {
            preview_id,
            preview_digest,
        }
    }
}

/// Durable acknowledgement of one atomic selected-account/profile transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecommendationSetupReceipt {
    revision: u64,
    digest: [u8; 32],
    configured: bool,
    accepted_at: Timestamp,
}

impl RecommendationSetupReceipt {
    pub(crate) const fn revision(self) -> u64 {
        self.revision
    }

    pub(crate) const fn digest(self) -> [u8; 32] {
        self.digest
    }

    pub(crate) const fn configured(self) -> bool {
        self.configured
    }

    pub(crate) const fn accepted_at(self) -> Timestamp {
        self.accepted_at
    }
}

#[derive(Clone, Debug)]
struct StoredPreview {
    preview: RecommendationSetupPreview,
    expires_at: Instant,
}

struct RecommendationSetupState {
    document: Option<RecommendationSetupDocument>,
    recovery_required: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RecommendationSetupBackup {
    format_version: u16,
    owner_workspace: WorkspaceId,
    document: Option<RecommendationSetupDocument>,
    semantic_authority_sha256: [u8; 32],
}

impl RecommendationSetupBackup {
    fn try_new(
        owner_workspace: WorkspaceId,
        document: Option<RecommendationSetupDocument>,
    ) -> Result<Self, RecommendationSetupError> {
        if let Some(document) = &document {
            document.validate(owner_workspace)?;
        }
        let semantic_authority_sha256 = backup_digest(owner_workspace, document.as_ref());
        Ok(Self {
            format_version: BACKUP_FORMAT_VERSION,
            owner_workspace,
            document,
            semantic_authority_sha256,
        })
    }

    fn validate(self) -> Result<Self, RecommendationSetupError> {
        if self.format_version != BACKUP_FORMAT_VERSION {
            return Err(RecommendationSetupError::InvalidBackup);
        }
        let expected = self.semantic_authority_sha256;
        let validated = Self::try_new(self.owner_workspace, self.document)
            .map_err(|_| RecommendationSetupError::InvalidBackup)?;
        if expected != validated.semantic_authority_sha256 {
            return Err(RecommendationSetupError::InvalidBackup);
        }
        Ok(validated)
    }

    fn canonical_bytes(&self) -> Result<Vec<u8>, RecommendationSetupError> {
        let bytes = serde_json::to_vec(self).map_err(|_| RecommendationSetupError::Encoding)?;
        if bytes.is_empty() || bytes.len() > LocalAuthorityStateStore::maximum_payload_bytes() {
            return Err(RecommendationSetupError::CapacityExceeded);
        }
        Ok(bytes)
    }
}

/// Exact retained recommendation-setup bytes and semantic authority identity.
#[derive(Clone, Debug)]
pub(crate) struct RetainedRecommendationSetupBackup {
    canonical_bytes: Vec<u8>,
    authority_revision_sha256: [u8; 32],
}

impl RetainedRecommendationSetupBackup {
    pub(crate) fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub(crate) const fn authority_revision_sha256(&self) -> [u8; 32] {
        self.authority_revision_sha256
    }
}

/// Exclusive workspace owner of the selected account and its same-account numeric profile.
pub(crate) struct RecommendationSetupAuthority {
    owner_workspace: WorkspaceId,
    store: LocalAuthorityStateStore,
    state: Mutex<RecommendationSetupState>,
    previews: Mutex<BTreeMap<RecommendationSetupPreviewId, StoredPreview>>,
}

impl RecommendationSetupAuthority {
    /// Opens the crash-safe workspace authority without choosing or defaulting an account.
    pub(crate) fn try_open(
        control_root: &Path,
        owner_workspace: WorkspaceId,
    ) -> Result<Self, RecommendationSetupError> {
        let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))?;
        let document = store
            .load()?
            .map(|encoded| RecommendationSetupDocument::decode(&encoded, owner_workspace))
            .transpose()?;
        Ok(Self {
            owner_workspace,
            store,
            state: Mutex::new(RecommendationSetupState {
                document,
                recovery_required: false,
            }),
            previews: Mutex::new(BTreeMap::new()),
        })
    }

    /// Returns the exact workspace whose setup state this authority owns.
    pub(crate) const fn owner_workspace(&self) -> WorkspaceId {
        self.owner_workspace
    }

    /// Retains one complete durable setup image for the Configuration backup component.
    pub(crate) fn retain_workspace_backup(
        &self,
    ) -> Result<RetainedRecommendationSetupBackup, RecommendationSetupError> {
        let state = self
            .state
            .lock()
            .map_err(|_| RecommendationSetupError::Unavailable)?;
        ensure_recovered(&state)?;
        let backup =
            RecommendationSetupBackup::try_new(self.owner_workspace, state.document.clone())?;
        Ok(RetainedRecommendationSetupBackup {
            canonical_bytes: backup.canonical_bytes()?,
            authority_revision_sha256: backup.semantic_authority_sha256,
        })
    }

    /// Rechecks that the complete current setup image still matches a retained backup lease.
    pub(crate) fn revalidate_workspace_backup(
        &self,
        retained: &RetainedRecommendationSetupBackup,
    ) -> Result<(), RecommendationSetupError> {
        let current = self.retain_workspace_backup()?;
        if current.authority_revision_sha256 == retained.authority_revision_sha256
            && current.canonical_bytes == retained.canonical_bytes
        {
            Ok(())
        } else {
            Err(RecommendationSetupError::StateChanged)
        }
    }

    /// Proves that a fresh restore target has no recommendation-setup document.
    pub(crate) fn ensure_workspace_backup_target_absent(
        control_root: &Path,
    ) -> Result<(), RecommendationSetupError> {
        let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))?;
        if store.load()?.is_some() {
            Err(RecommendationSetupError::RestoreTargetOccupied)
        } else {
            Ok(())
        }
    }

    /// Validates a source backup and its typed target-workspace rebind without mutating storage.
    pub(crate) fn validate_workspace_backup_for_rebind(
        target_workspace: WorkspaceId,
        canonical_bytes: &[u8],
    ) -> Result<(), RecommendationSetupError> {
        let backup = decode_backup(canonical_bytes)?;
        if backup.owner_workspace == target_workspace {
            return Err(RecommendationSetupError::InvalidBackup);
        }
        backup
            .document
            .as_ref()
            .map(|document| document.rebound_to_workspace(target_workspace))
            .transpose()?;
        Ok(())
    }

    /// Restores validated setup state into a fresh workspace through a typed owner rebind.
    pub(crate) fn restore_workspace_backup_rebound_absent(
        control_root: &Path,
        target_workspace: WorkspaceId,
        canonical_bytes: &[u8],
    ) -> Result<Self, RecommendationSetupError> {
        let backup = decode_backup(canonical_bytes)?;
        if backup.owner_workspace == target_workspace {
            return Err(RecommendationSetupError::InvalidBackup);
        }
        Self::ensure_workspace_backup_target_absent(control_root)?;
        let rebound = backup
            .document
            .as_ref()
            .map(|document| document.rebound_to_workspace(target_workspace))
            .transpose()?;
        if let Some(document) = &rebound {
            let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))?;
            store.store(&document.encode()?)?;
        }
        let authority = Self::try_open(control_root, target_workspace)?;
        let restored = authority.status()?;
        match rebound {
            Some(document)
                if restored.revision() == document.revision
                    && restored.digest() == document.digest => {}
            None if restored.revision() == 0
                && restored.digest() == genesis_digest(target_workspace) => {}
            Some(_) | None => return Err(RecommendationSetupError::CorruptState),
        }
        Ok(authority)
    }

    /// Returns the exact durable state; an absent document is an explicit unconfigured genesis.
    pub(crate) fn status(&self) -> Result<RecommendationSetupSnapshot, RecommendationSetupError> {
        let state = self
            .state
            .lock()
            .map_err(|_| RecommendationSetupError::Unavailable)?;
        ensure_recovered(&state)?;
        Ok(snapshot(self.owner_workspace, state.document.as_ref()))
    }

    /// Previews one atomic explicit selection and same-account profile installation.
    pub(crate) fn preview_configuration(
        &self,
        workspace: WorkspaceId,
        expected_revision: u64,
        catalog: &PortfolioAccountCatalogSnapshot,
        profile: RecommendationProfileInput,
    ) -> Result<RecommendationSetupPreview, RecommendationSetupError> {
        self.ensure_owner(workspace)?;
        let head = catalog
            .head(profile.account_id())
            .ok_or(RecommendationSetupError::AccountUnavailable)?;
        profile.validate_for_currency(head.reporting_currency())?;
        let pending = PendingRecommendationConfiguration {
            account_id: head.account_id(),
            reporting_currency: head.reporting_currency(),
            portfolio_revision: head.revision().bytes(),
            catalog_digest: catalog.digest(),
            profile,
        };
        self.preview_change(
            workspace,
            expected_revision,
            catalog,
            RecommendationSetupPreviewKind::Configure,
            Some(pending),
        )
    }

    /// Previews clearing both selected account and profile; neither can survive independently.
    pub(crate) fn preview_clear(
        &self,
        workspace: WorkspaceId,
        expected_revision: u64,
        catalog: &PortfolioAccountCatalogSnapshot,
    ) -> Result<RecommendationSetupPreview, RecommendationSetupError> {
        self.ensure_owner(workspace)?;
        self.preview_change(
            workspace,
            expected_revision,
            catalog,
            RecommendationSetupPreviewKind::Clear,
            None,
        )
    }

    /// Consumes an exact preview and persists its selected-account/profile transition atomically.
    pub(crate) fn apply(
        &self,
        workspace: WorkspaceId,
        confirmation: RecommendationSetupConfirmation,
        current_catalog: &PortfolioAccountCatalogSnapshot,
    ) -> Result<RecommendationSetupReceipt, RecommendationSetupError> {
        self.ensure_owner(workspace)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| RecommendationSetupError::Unavailable)?;
        ensure_recovered(&state)?;
        let mut previews = self
            .previews
            .lock()
            .map_err(|_| RecommendationSetupError::Unavailable)?;
        let now = Instant::now();
        let stored = previews
            .get(&confirmation.preview_id)
            .ok_or(RecommendationSetupError::PreviewUnavailable)?;
        if stored.preview.owner_workspace != workspace {
            return Err(RecommendationSetupError::CrossWorkspacePreview);
        }
        if stored.expires_at <= now {
            previews.remove(&confirmation.preview_id);
            return Err(RecommendationSetupError::PreviewExpired);
        }
        if stored.preview.digest != confirmation.preview_digest {
            return Err(RecommendationSetupError::InvalidConfirmation);
        }
        let current = snapshot(workspace, state.document.as_ref());
        if stored.preview.current_revision != current.revision
            || stored.preview.current_authority_digest != current.digest
            || stored.preview.resulting_revision
                != current
                    .revision
                    .checked_add(1)
                    .ok_or(RecommendationSetupError::RevisionExhausted)?
        {
            return Err(RecommendationSetupError::StaleRevision);
        }
        if stored.preview.catalog_digest != current_catalog.digest() {
            return Err(RecommendationSetupError::StaleCatalog);
        }
        if let Some(configuration) = &stored.preview.configuration {
            let current_head = current_catalog
                .head(configuration.account_id)
                .ok_or(RecommendationSetupError::StaleCatalog)?;
            if current_head.revision().bytes() != configuration.portfolio_revision
                || current_head.reporting_currency() != configuration.reporting_currency
                || configuration.catalog_digest != current_catalog.digest()
            {
                return Err(RecommendationSetupError::StaleCatalog);
            }
        }

        let stored = previews
            .remove(&confirmation.preview_id)
            .ok_or(RecommendationSetupError::PreviewUnavailable)?;
        prune_expired(&mut previews, now);
        let accepted_at = current_timestamp()?;
        let configuration = match stored.preview.configuration.as_ref() {
            Some(pending) => Some(RecommendationConfiguration::try_new(
                stored.preview.resulting_revision,
                pending,
                accepted_at,
            )?),
            None if stored.preview.kind == RecommendationSetupPreviewKind::Clear => None,
            None => return Err(RecommendationSetupError::InvalidConfirmation),
        };
        let document = RecommendationSetupDocument::try_new(
            workspace,
            stored.preview.resulting_revision,
            current.digest,
            accepted_at,
            configuration,
        )?;
        let encoded = document.encode()?;
        if let Err(error) = self.store.store(&encoded) {
            state.recovery_required = true;
            return Err(RecommendationSetupError::Persistence(error));
        }
        let receipt = RecommendationSetupReceipt {
            revision: document.revision,
            digest: document.digest,
            configured: document.configuration.is_some(),
            accepted_at,
        };
        state.document = Some(document);
        Ok(receipt)
    }

    /// Resolves only an explicit durable selection; catalog order never supplies a default.
    pub(crate) fn resolve(
        &self,
        catalog: &PortfolioAccountCatalogSnapshot,
        as_of: Timestamp,
    ) -> Result<RecommendationSetupResolution, RecommendationSetupError> {
        let now = current_timestamp()?;
        if as_of.unix_nanos() <= 0 || as_of > now {
            return Err(RecommendationSetupError::InvalidAsOf);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| RecommendationSetupError::Unavailable)?;
        ensure_recovered(&state)?;
        let current = snapshot(self.owner_workspace, state.document.as_ref());
        if current
            .transition_at
            .is_some_and(|transition| transition > as_of)
        {
            return Err(RecommendationSetupError::InvalidAsOf);
        }
        let Some(document) = state.document.as_ref() else {
            return Ok(unconfigured_resolution(&current, catalog, as_of));
        };
        let Some(configuration) = document.configuration.as_ref() else {
            return Ok(unconfigured_resolution(&current, catalog, as_of));
        };
        if configuration.selected_account.confirmed_at() > as_of
            || configuration.profile.accepted_at() > as_of
        {
            return Err(RecommendationSetupError::InvalidAsOf);
        }
        let Some(head) = catalog.head(configuration.selected_account.account_id) else {
            let evidence = setup_evidence(&current, catalog, None, as_of);
            return Ok(RecommendationSetupResolution::SetupRequired(
                SetupRequired::no_default_account(evidence),
            ));
        };
        let evidence = setup_evidence(&current, catalog, Some(head), as_of);
        if head.reporting_currency() != configuration.selected_account.reporting_currency {
            return Ok(RecommendationSetupResolution::SetupRequired(
                SetupRequired::no_default_account(evidence),
            ));
        }
        if head.effective_at() > as_of
            || head.available_at().is_none()
            || head
                .available_at()
                .is_some_and(|available_at| available_at > as_of)
        {
            return Ok(RecommendationSetupResolution::SetupRequired(
                SetupRequired::portfolio_evidence_unavailable(evidence),
            ));
        }
        if as_of >= configuration.profile.review_due_at() {
            return Ok(RecommendationSetupResolution::SetupRequired(
                SetupRequired::profile_review_required(evidence),
            ));
        }
        Ok(RecommendationSetupResolution::Ready(
            ResolvedRecommendationSetup::new(
                current.revision,
                current.digest,
                configuration.digest,
                configuration.selected_account.clone(),
                configuration.profile.clone(),
                head.clone(),
                catalog.digest(),
                catalog.account_count(),
                as_of,
            ),
        ))
    }

    /// Rechecks that both durable setup and every exact current catalog head remain unchanged.
    pub(crate) fn recheck(
        &self,
        expected: &ResolvedRecommendationSetup,
        catalog: &PortfolioAccountCatalogSnapshot,
        as_of: Timestamp,
    ) -> Result<(), RecommendationSetupError> {
        match self.resolve(catalog, as_of)? {
            RecommendationSetupResolution::Ready(current) if &current == expected => Ok(()),
            RecommendationSetupResolution::Ready(_)
            | RecommendationSetupResolution::SetupRequired(_) => {
                Err(RecommendationSetupError::StateChanged)
            }
        }
    }

    fn preview_change(
        &self,
        workspace: WorkspaceId,
        expected_revision: u64,
        catalog: &PortfolioAccountCatalogSnapshot,
        kind: RecommendationSetupPreviewKind,
        configuration: Option<PendingRecommendationConfiguration>,
    ) -> Result<RecommendationSetupPreview, RecommendationSetupError> {
        if (kind == RecommendationSetupPreviewKind::Configure) != configuration.is_some()
            || catalog.digest().algorithm() != DigestAlgorithm::Sha256
            || catalog.digest().bytes() == [0; 32]
        {
            return Err(RecommendationSetupError::InvalidProfile);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| RecommendationSetupError::Unavailable)?;
        ensure_recovered(&state)?;
        let current = snapshot(workspace, state.document.as_ref());
        if expected_revision != current.revision {
            return Err(RecommendationSetupError::StaleRevision);
        }
        let resulting_revision = current
            .revision
            .checked_add(1)
            .ok_or(RecommendationSetupError::RevisionExhausted)?;
        let issued_at = current_timestamp()?;
        let expires_at = issued_at
            .checked_add_nanos(PREVIEW_LIFETIME_NANOS)
            .map_err(|_| RecommendationSetupError::TimeUnavailable)?;
        let process_now = Instant::now();
        let process_expiry = process_now
            .checked_add(PREVIEW_LIFETIME)
            .ok_or(RecommendationSetupError::TimeUnavailable)?;
        let mut previews = self
            .previews
            .lock()
            .map_err(|_| RecommendationSetupError::Unavailable)?;
        prune_expired(&mut previews, process_now);
        if previews.len() >= MAXIMUM_PREVIEWS {
            return Err(RecommendationSetupError::CapacityExceeded);
        }
        let preview_id = next_preview_id(&previews)?;
        let mut preview = RecommendationSetupPreview {
            format_version: FORMAT_VERSION,
            preview_id,
            owner_workspace: workspace,
            current_revision: current.revision,
            resulting_revision,
            current_authority_digest: current.digest,
            catalog_digest: catalog.digest(),
            kind,
            configuration,
            issued_at,
            expires_at,
            digest: [0; 32],
        };
        preview.digest = preview_digest(&preview)?;
        let encoded =
            serde_json::to_vec(&preview).map_err(|_| RecommendationSetupError::Encoding)?;
        if encoded.is_empty() || encoded.len() > MAXIMUM_PREVIEW_BYTES {
            return Err(RecommendationSetupError::CapacityExceeded);
        }
        previews.insert(
            preview_id,
            StoredPreview {
                preview: preview.clone(),
                expires_at: process_expiry,
            },
        );
        Ok(preview)
    }

    fn ensure_owner(&self, workspace: WorkspaceId) -> Result<(), RecommendationSetupError> {
        if workspace == self.owner_workspace {
            Ok(())
        } else {
            Err(RecommendationSetupError::CrossWorkspacePreview)
        }
    }
}

impl fmt::Debug for RecommendationSetupAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .write_str("RecommendationSetupAuthority([WORKSPACE ACCOUNT AND PROFILE AUTHORITY])")
    }
}

/// Fail-closed setup validation, preview, persistence, and exact-state failure.
#[derive(Debug, Error)]
pub(crate) enum RecommendationSetupError {
    #[error("recommendation allocation profile is invalid")]
    InvalidProfile,
    #[error("selected recommendation account is not in the complete current catalog")]
    AccountUnavailable,
    #[error("selected account and allocation-profile currencies do not match")]
    CurrencyMismatch,
    #[error("recommendation setup revision is stale")]
    StaleRevision,
    #[error("portfolio account catalog changed after setup preview")]
    StaleCatalog,
    #[error("recommendation setup preview is unavailable")]
    PreviewUnavailable,
    #[error("recommendation setup preview expired")]
    PreviewExpired,
    #[error("recommendation setup confirmation is invalid")]
    InvalidConfirmation,
    #[error("recommendation setup preview belongs to another workspace")]
    CrossWorkspacePreview,
    #[error("recommendation setup revision space is exhausted")]
    RevisionExhausted,
    #[error("recommendation setup capacity is exhausted")]
    CapacityExceeded,
    #[error("recommendation setup durable state is corrupt")]
    CorruptState,
    #[error("recommendation setup authority is unavailable")]
    Unavailable,
    #[error("recommendation setup persistence recovery is required")]
    RecoveryRequired,
    #[error("recommendation setup time is unavailable")]
    TimeUnavailable,
    #[error("recommendation setup evaluation time is invalid or lies in the future")]
    InvalidAsOf,
    #[error("recommendation setup encoding failed")]
    Encoding,
    #[error("recommendation setup or current portfolio state changed")]
    StateChanged,
    #[error("recommendation setup persistence failed")]
    Persistence(#[from] LocalAuthorityStateStoreError),
    #[error("recommendation setup backup is invalid")]
    InvalidBackup,
    #[error("recommendation setup restore target is not empty")]
    RestoreTargetOccupied,
}

fn snapshot(
    owner_workspace: WorkspaceId,
    document: Option<&RecommendationSetupDocument>,
) -> RecommendationSetupSnapshot {
    match document {
        Some(document) => RecommendationSetupSnapshot {
            revision: document.revision,
            digest: document.digest,
            transition_at: Some(document.transition_at),
            selected_account: document
                .configuration
                .as_ref()
                .map(|configuration| configuration.selected_account.clone()),
            profile: document
                .configuration
                .as_ref()
                .map(|configuration| configuration.profile.clone()),
            configuration_digest: document
                .configuration
                .as_ref()
                .map(|configuration| configuration.digest),
        },
        None => RecommendationSetupSnapshot {
            revision: 0,
            digest: genesis_digest(owner_workspace),
            transition_at: None,
            selected_account: None,
            profile: None,
            configuration_digest: None,
        },
    }
}

fn decode_backup(
    canonical_bytes: &[u8],
) -> Result<RecommendationSetupBackup, RecommendationSetupError> {
    if canonical_bytes.is_empty()
        || canonical_bytes.len() > LocalAuthorityStateStore::maximum_payload_bytes()
    {
        return Err(RecommendationSetupError::InvalidBackup);
    }
    serde_json::from_slice::<RecommendationSetupBackup>(canonical_bytes)
        .map_err(|_| RecommendationSetupError::InvalidBackup)?
        .validate()
}

fn unconfigured_resolution(
    current: &RecommendationSetupSnapshot,
    catalog: &PortfolioAccountCatalogSnapshot,
    as_of: Timestamp,
) -> RecommendationSetupResolution {
    let evidence = setup_evidence(current, catalog, None, as_of);
    if catalog.len() > 1 {
        RecommendationSetupResolution::SetupRequired(SetupRequired::ambiguous_accounts(evidence))
    } else {
        RecommendationSetupResolution::SetupRequired(SetupRequired::no_default_account(evidence))
    }
}

fn setup_evidence(
    current: &RecommendationSetupSnapshot,
    catalog: &PortfolioAccountCatalogSnapshot,
    current_head: Option<&PortfolioAccountHead>,
    as_of: Timestamp,
) -> RecommendationSetupEvidence {
    RecommendationSetupEvidence::new(
        current.revision,
        current.digest,
        catalog.digest(),
        catalog.account_count(),
        as_of,
        current
            .selected_account
            .as_ref()
            .map(SelectedRecommendationAccount::account_id),
        current
            .selected_account
            .as_ref()
            .map(SelectedRecommendationAccount::reporting_currency),
        current
            .selected_account
            .as_ref()
            .map(|selected| selected.confirmed_portfolio_revision().bytes()),
        current_head.map(PortfolioAccountHead::reporting_currency),
        current_head.map(|head| head.revision().bytes()),
        current_head.map(PortfolioAccountHead::effective_at),
        current_head.and_then(PortfolioAccountHead::available_at),
        current
            .profile
            .as_ref()
            .map(UserConfirmedAllocationProfile::digest),
        current
            .profile
            .as_ref()
            .map(UserConfirmedAllocationProfile::review_due_at),
    )
}

fn ensure_recovered(state: &RecommendationSetupState) -> Result<(), RecommendationSetupError> {
    if state.recovery_required {
        Err(RecommendationSetupError::RecoveryRequired)
    } else {
        Ok(())
    }
}

fn next_preview_id(
    previews: &BTreeMap<RecommendationSetupPreviewId, StoredPreview>,
) -> Result<RecommendationSetupPreviewId, RecommendationSetupError> {
    for _ in 0..PREVIEW_ID_ATTEMPTS {
        let candidate = RecommendationSetupPreviewId(Uuid::new_v4());
        if !previews.contains_key(&candidate) {
            return Ok(candidate);
        }
    }
    Err(RecommendationSetupError::CapacityExceeded)
}

fn prune_expired(
    previews: &mut BTreeMap<RecommendationSetupPreviewId, StoredPreview>,
    now: Instant,
) {
    previews.retain(|_, stored| stored.expires_at > now);
}

fn current_timestamp() -> Result<Timestamp, RecommendationSetupError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RecommendationSetupError::TimeUnavailable)?;
    let nanos =
        i64::try_from(elapsed.as_nanos()).map_err(|_| RecommendationSetupError::TimeUnavailable)?;
    if nanos <= 0 {
        return Err(RecommendationSetupError::TimeUnavailable);
    }
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn genesis_digest(owner_workspace: WorkspaceId) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(GENESIS_DIGEST_DOMAIN);
    digest.update(owner_workspace.as_uuid().as_bytes());
    digest.update(0_u64.to_be_bytes());
    digest.update([0]);
    digest.finalize().into()
}

fn backup_digest(
    owner_workspace: WorkspaceId,
    document: Option<&RecommendationSetupDocument>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(BACKUP_DIGEST_DOMAIN);
    digest.update(owner_workspace.as_uuid().as_bytes());
    match document {
        Some(document) => {
            digest.update([1]);
            digest.update(document.digest);
        }
        None => digest.update([0]),
    }
    digest.finalize().into()
}

fn selection_digest(
    selected: &SelectedRecommendationAccount,
) -> Result<[u8; 32], RecommendationSetupError> {
    let mut digest = Sha256::new();
    digest.update(SELECTION_DIGEST_DOMAIN);
    digest.update(selected.setup_revision.to_be_bytes());
    digest.update(selected.account_id.as_uuid().as_bytes());
    update_text(&mut digest, selected.reporting_currency.as_str())?;
    digest.update(selected.confirmed_portfolio_revision);
    update_evidence_digest(&mut digest, selected.confirmed_catalog_digest);
    digest.update(selected.confirmed_at.unix_nanos().to_be_bytes());
    checked_digest(digest)
}

fn profile_digest(
    profile: &UserConfirmedAllocationProfile,
) -> Result<[u8; 32], RecommendationSetupError> {
    let mut digest = Sha256::new();
    digest.update(PROFILE_DIGEST_DOMAIN);
    digest.update(profile.setup_revision.to_be_bytes());
    digest.update(profile.account_id.as_uuid().as_bytes());
    update_text(&mut digest, profile.reporting_currency.as_str())?;
    digest.update(profile.preferred_position_weight_lower_bps.to_be_bytes());
    digest.update(profile.preferred_position_weight_upper_bps.to_be_bytes());
    update_money(&mut digest, profile.minimum_cash_reserve)?;
    digest.update(
        profile
            .maximum_downside_loss_bps_of_marked_equity
            .to_be_bytes(),
    );
    digest.update(profile.available_investment_horizon_nanos.to_be_bytes());
    digest.update(profile.accepted_at.unix_nanos().to_be_bytes());
    digest.update(profile.review_due_at.unix_nanos().to_be_bytes());
    checked_digest(digest)
}

fn configuration_digest(
    selected: &SelectedRecommendationAccount,
    profile: &UserConfirmedAllocationProfile,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(CONFIGURATION_DIGEST_DOMAIN);
    digest.update(selected.digest);
    digest.update(profile.digest);
    digest.finalize().into()
}

fn document_digest(document: &RecommendationSetupDocument) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(DOCUMENT_DIGEST_DOMAIN);
    digest.update(document.format_version.to_be_bytes());
    digest.update(document.owner_workspace.as_uuid().as_bytes());
    digest.update(document.revision.to_be_bytes());
    digest.update(document.previous_digest);
    digest.update(document.transition_at.unix_nanos().to_be_bytes());
    match &document.configuration {
        Some(configuration) => {
            digest.update([1]);
            digest.update(configuration.digest);
        }
        None => digest.update([0]),
    }
    digest.finalize().into()
}

fn preview_digest(
    preview: &RecommendationSetupPreview,
) -> Result<[u8; 32], RecommendationSetupError> {
    let mut digest = Sha256::new();
    digest.update(PREVIEW_DIGEST_DOMAIN);
    digest.update(preview.format_version.to_be_bytes());
    digest.update(preview.preview_id.0.as_bytes());
    digest.update(preview.owner_workspace.as_uuid().as_bytes());
    digest.update(preview.current_revision.to_be_bytes());
    digest.update(preview.resulting_revision.to_be_bytes());
    digest.update(preview.current_authority_digest);
    update_evidence_digest(&mut digest, preview.catalog_digest);
    digest.update([preview.kind.tag()]);
    match &preview.configuration {
        Some(configuration) => {
            digest.update([1]);
            digest.update(configuration.account_id.as_uuid().as_bytes());
            update_text(&mut digest, configuration.reporting_currency.as_str())?;
            digest.update(configuration.portfolio_revision);
            update_evidence_digest(&mut digest, configuration.catalog_digest);
            update_profile_input(&mut digest, &configuration.profile)?;
        }
        None => digest.update([0]),
    }
    digest.update(preview.issued_at.unix_nanos().to_be_bytes());
    digest.update(preview.expires_at.unix_nanos().to_be_bytes());
    checked_digest(digest)
}

fn update_profile_input(
    digest: &mut Sha256,
    input: &RecommendationProfileInput,
) -> Result<(), RecommendationSetupError> {
    digest.update(input.account_id.as_uuid().as_bytes());
    digest.update(input.preferred_position_weight_lower_bps.to_be_bytes());
    digest.update(input.preferred_position_weight_upper_bps.to_be_bytes());
    update_money(digest, input.minimum_cash_reserve)?;
    digest.update(
        input
            .maximum_downside_loss_bps_of_marked_equity
            .to_be_bytes(),
    );
    digest.update(input.available_investment_horizon_nanos.to_be_bytes());
    Ok(())
}

fn update_evidence_digest(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(evidence.bytes());
}

fn update_money(digest: &mut Sha256, money: Money) -> Result<(), RecommendationSetupError> {
    update_text(digest, &money.amount().normalize().to_string())?;
    update_text(digest, money.currency().as_str())
}

fn update_text(digest: &mut Sha256, value: &str) -> Result<(), RecommendationSetupError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| RecommendationSetupError::CapacityExceeded)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

fn checked_digest(digest: Sha256) -> Result<[u8; 32], RecommendationSetupError> {
    let bytes: [u8; 32] = digest.finalize().into();
    if bytes == [0; 32] {
        Err(RecommendationSetupError::Encoding)
    } else {
        Ok(bytes)
    }
}
