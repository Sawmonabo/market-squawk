//! Shared account identity, budget, and lifetime authority for market-data providers.

use std::sync::Arc;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_platform::{AppConfig, LocalAuthorityStateStore, LocalPaths};
use market_squawk_sources::{
    AuthorizationMode, DataUseOperation, ProviderRateAuthority, SourceMetadata,
};

use crate::{ProviderActivationLease, ProviderOnboardingService};

/// Closed user-authorized market-data account surfaces supported by V1 activation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderMarketAccount {
    /// Alpaca Trading API Basic market-data account.
    AlpacaBasic,
    /// Tradier production Brokerage market-data account.
    TradierBrokerage,
    /// Kraken Spot API key admitted only for authenticated level-3 market data.
    KrakenLevel3,
}

impl ProviderMarketAccount {
    const fn surface(self) -> &'static str {
        match self {
            Self::AlpacaBasic => "alpaca.basic-market-data",
            Self::TradierBrokerage => "tradier.brokerage-market-data",
            Self::KrakenLevel3 => "kraken.spot-authenticated-level3-market-data",
        }
    }

    const fn provider(self) -> &'static str {
        match self {
            Self::AlpacaBasic => "alpaca-market-data",
            Self::TradierBrokerage => "tradier-brokerage",
            Self::KrakenLevel3 => "kraken",
        }
    }

    const fn subject_prefix(self) -> &'static str {
        match self {
            Self::AlpacaBasic => "alpaca-account-",
            Self::TradierBrokerage => "tradier-account-",
            Self::KrakenLevel3 => "kraken-l3-account-",
        }
    }

    const fn authority_root(self) -> &'static str {
        match self {
            Self::AlpacaBasic => "alpaca-market-data-account-authority",
            Self::TradierBrokerage => "tradier-market-data-account-authority",
            Self::KrakenLevel3 => "kraken-level3-account-authority",
        }
    }
}

/// Secret-free account identity derived only from one verified onboarding lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderAccountBinding {
    account: ProviderMarketAccount,
    subject: SourceIdentifier,
    verification_evidence: EvidenceDigest,
}

impl ProviderAccountBinding {
    /// Derives the stable provider-rate and metadata subject for one exact account lease.
    ///
    /// # Errors
    ///
    /// Rejects a different surface, missing credential/account verification, expired-capability
    /// shape, missing display/retrieval rights, or a mismatched account-qualified budget.
    pub fn try_from_lease(
        account: ProviderMarketAccount,
        lease: &ProviderActivationLease,
    ) -> Result<Self, ProviderAccountActivationError> {
        let budget = lease
            .provider_budget_policy()
            .ok_or(ProviderAccountActivationError::SourceBinding)?;
        let digest = lease
            .account_digest()
            .ok_or(ProviderAccountActivationError::SourceBinding)?;
        let verification_evidence = lease
            .verification_evidence_digest()
            .ok_or(ProviderAccountActivationError::SourceBinding)?;
        if lease.surface_id().as_str() != account.surface() {
            return Err(ProviderAccountActivationError::SurfaceMismatch);
        }
        if digest.algorithm() != DigestAlgorithm::Sha256
            || lease.generation().is_none()
            || lease.secret_reference().is_none()
            || lease.verification_expires_at().is_none()
            || !lease.admits(DataUseOperation::Retrieve)
            || !lease.admits(DataUseOperation::Display)
            || budget.scope().as_source_identifier().as_str() != account.provider()
            || budget.scope().authorization_account().is_none()
        {
            return Err(ProviderAccountActivationError::SourceBinding);
        }
        let subject = digest_subject(account.subject_prefix(), digest)?;
        Ok(Self {
            account,
            subject,
            verification_evidence,
        })
    }

    /// Returns the exact closed provider-account kind.
    pub const fn account(&self) -> ProviderMarketAccount {
        self.account
    }

    /// Returns the redacted digest-derived account subject used by all logical surfaces.
    pub const fn subject(&self) -> &SourceIdentifier {
        &self.subject
    }

    /// Returns the exact successful provider-verification evidence digest.
    pub const fn verification_evidence(&self) -> EvidenceDigest {
        self.verification_evidence
    }

    pub(super) fn validates_metadata(&self, metadata: &SourceMetadata) -> bool {
        metadata.provider().as_str() == self.account.provider()
            && metadata.authorization().mode() == AuthorizationMode::UserAuthorized
            && metadata.authorization().basis().as_source_identifier() == &self.subject
            && metadata.authorization().evidence().content_digest() == self.verification_evidence
    }
}

pub(super) struct ProviderAccountRuntimeAuthority {
    binding: ProviderAccountBinding,
    lease: ProviderActivationLease,
    onboarding: Arc<ProviderOnboardingService>,
    _provider_rate: ProviderRateAuthority,
    _account_authority: LocalAuthorityStateStore,
}

impl ProviderAccountRuntimeAuthority {
    pub(super) fn try_acquire(
        account: ProviderMarketAccount,
        lease: ProviderActivationLease,
        onboarding: Arc<ProviderOnboardingService>,
        app_config: &AppConfig,
        provider_rate: ProviderRateAuthority,
    ) -> Result<Self, ProviderAccountActivationError> {
        let binding = ProviderAccountBinding::try_from_lease(account, &lease)?;
        let onboarding_authority = onboarding.try_acquire_runtime_mutation_authority()?;
        onboarding_authority.require_active(&lease)?;
        provider_rate.bind_authorization_subject(
            AuthorizationMode::UserAuthorized,
            binding.verification_evidence(),
            binding.subject(),
        )?;
        let paths = LocalPaths::prepare(app_config.data_dir())?;
        let authority_root = paths
            .control_root()?
            .root()
            .join(account.authority_root())
            .join(binding.subject().as_str());
        let account_authority = LocalAuthorityStateStore::try_open(authority_root)?;
        onboarding_authority.require_active(&lease)?;
        drop(onboarding_authority);
        Ok(Self {
            binding,
            lease,
            onboarding,
            _provider_rate: provider_rate,
            _account_authority: account_authority,
        })
    }

    pub(super) const fn lease(&self) -> &ProviderActivationLease {
        &self.lease
    }

    pub(super) const fn binding(&self) -> &ProviderAccountBinding {
        &self.binding
    }

    pub(super) async fn require_current(&self) -> Result<(), crate::ProviderOnboardingError> {
        self.onboarding
            .acquire_runtime_mutation_authority()
            .await
            .require_active(&self.lease)
    }

    pub(super) fn next_persisted_nonce(
        &self,
        candidate: u64,
    ) -> Result<u64, ProviderAccountActivationError> {
        if self.binding.account() != ProviderMarketAccount::KrakenLevel3 || candidate == 0 {
            return Err(ProviderAccountActivationError::SourceBinding);
        }
        let prior = match self._account_authority.load()? {
            Some(bytes) => {
                let bytes: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| ProviderAccountActivationError::SourceBinding)?;
                u64::from_be_bytes(bytes)
            }
            None => 0,
        };
        let nonce = candidate.max(
            prior
                .checked_add(1)
                .ok_or(ProviderAccountActivationError::SourceBinding)?,
        );
        self._account_authority.store(&nonce.to_be_bytes())?;
        Ok(nonce)
    }
}

impl std::fmt::Debug for ProviderAccountRuntimeAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderAccountRuntimeAuthority")
            .field("surface_id", self.lease.surface_id())
            .field("session_id", &self.lease.session_id())
            .field("account", &self.binding.account())
            .field("subject", &"[REDACTED DIGEST SUBJECT]")
            .field("authority", &"[EXCLUSIVE LOCAL CAPABILITY]")
            .finish()
    }
}

fn digest_subject(
    prefix: &str,
    digest: EvidenceDigest,
) -> Result<SourceIdentifier, ProviderAccountActivationError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = digest.bytes();
    let mut subject = String::with_capacity(prefix.len().saturating_add(bytes.len() * 2));
    subject.push_str(prefix);
    for byte in bytes {
        subject.push(char::from(HEX[usize::from(byte >> 4)]));
        subject.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    SourceIdentifier::try_from(subject).map_err(|_| ProviderAccountActivationError::SourceBinding)
}

/// Provider-account identity, budget, and exclusive-lifetime admission failure.
#[derive(Debug, thiserror::Error)]
pub enum ProviderAccountActivationError {
    /// The lease names another provider surface.
    #[error("provider account activation surface does not match")]
    SurfaceMismatch,
    /// Required credential, evidence, rights, or budget binding is missing or inconsistent.
    #[error("provider account activation binding is invalid")]
    SourceBinding,
    /// The active onboarding generation is absent, stale, rotated, or otherwise unavailable.
    #[error(transparent)]
    Onboarding(#[from] crate::ProviderOnboardingError),
    /// Durable authorization-subject binding is unavailable or conflicts.
    #[error(transparent)]
    ProviderRate(#[from] market_squawk_sources::ProviderRateStoreError),
    /// The process cannot acquire the one account-lifetime authority.
    #[error(transparent)]
    AccountAuthority(#[from] market_squawk_platform::LocalAuthorityStateStoreError),
    /// The configured control-state path is unavailable or unsafe.
    #[error(transparent)]
    Path(#[from] market_squawk_platform::PathError),
}
