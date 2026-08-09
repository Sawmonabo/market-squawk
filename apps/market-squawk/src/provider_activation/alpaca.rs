//! Account-owned activation for Alpaca Basic IEX and indicative-options data.

use std::sync::Arc;

use market_squawk_adapter_alpaca::{
    AlpacaCredentials, AlpacaIexLiveConfig, AlpacaOptionsLiveConfig,
};
use market_squawk_domain::DataQuality;
use market_squawk_sources::ProviderRateDeclaration;
use tokio_util::sync::CancellationToken;

use crate::{ProviderActivationLease, ProviderOnboardingError};

use super::ProviderAdapterActivation;
use super::account::{
    ProviderAccountActivationError, ProviderAccountBinding, ProviderAccountRuntimeAuthority,
    ProviderMarketAccount,
};
use super::credentials::{AlpacaCredentialEnvelope, ProviderCredentialError};

/// Non-clone, account-owned Alpaca Basic runtime admission.
///
/// The owner retains the exclusive account authority and exact onboarding lease while the two
/// logical source configurations are moved once into central live supervision. Credentials are
/// shared only between those two bounded provider sources and remain zeroizing and redacted.
pub struct AlpacaBasicAccountActivation {
    authority: ProviderAccountRuntimeAuthority,
    credentials: Arc<AlpacaCredentials>,
    iex: Option<AlpacaIexLiveConfig>,
    options: Option<AlpacaOptionsLiveConfig>,
}

impl AlpacaBasicAccountActivation {
    /// Returns the immutable onboarding lease retained by this runtime owner.
    pub const fn lease(&self) -> &ProviderActivationLease {
        self.authority.lease()
    }

    /// Returns the stable, secret-free provider-account binding.
    pub const fn account_binding(&self) -> &ProviderAccountBinding {
        self.authority.binding()
    }

    /// Returns shared zeroizing credentials for construction of the two admitted sources.
    pub fn credentials(&self) -> Arc<AlpacaCredentials> {
        Arc::clone(&self.credentials)
    }

    /// Moves the exact IEX-only configuration into central supervision once.
    pub fn take_iex_config(&mut self) -> Option<AlpacaIexLiveConfig> {
        self.iex.take()
    }

    /// Moves the exact Basic indicative-options configuration into central supervision once.
    pub fn take_options_config(&mut self) -> Option<AlpacaOptionsLiveConfig> {
        self.options.take()
    }

    /// Revalidates the exact active credential generation outside the live event path.
    pub async fn require_current(&self) -> Result<(), ProviderOnboardingError> {
        self.authority.require_current().await
    }
}

impl std::fmt::Debug for AlpacaBasicAccountActivation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaBasicAccountActivation")
            .field("authority", &self.authority)
            .field("credentials", &"[REDACTED ZEROIZING CREDENTIALS]")
            .field("iex_config_available", &self.iex.is_some())
            .field("options_config_available", &self.options.is_some())
            .finish()
    }
}

impl ProviderAdapterActivation {
    /// Activates one Alpaca Basic account across its IEX and indicative-options surfaces.
    ///
    /// # Errors
    ///
    /// Fails closed for a stale/mismatched lease, duplicated account runtime, metadata that does
    /// not bind the verified account and shared budget, any quality overstatement, invalid secret
    /// envelope, or cancellation.
    pub(crate) async fn activate_alpaca_basic_account(
        &self,
        lease: ProviderActivationLease,
        iex: AlpacaIexLiveConfig,
        options: Option<AlpacaOptionsLiveConfig>,
        cancellation: CancellationToken,
    ) -> Result<AlpacaBasicAccountActivation, AlpacaBasicActivationError> {
        if cancellation.is_cancelled() {
            return Err(AlpacaBasicActivationError::Cancelled);
        }
        let binding =
            ProviderAccountBinding::try_from_lease(ProviderMarketAccount::AlpacaBasic, &lease)?;
        validate_configurations(&lease, &binding, &iex, options.as_ref())?;
        let secret = self
            .onboarding
            .read_secret_for_activation_request(&lease, cancellation)
            .await?;
        let envelope = AlpacaCredentialEnvelope::try_parse(secret.expose_secret())?;
        if envelope.account_digest()
            != lease
                .account_digest()
                .ok_or(AlpacaBasicActivationError::SourceBinding)?
        {
            return Err(AlpacaBasicActivationError::SourceBinding);
        }
        let credentials = Arc::new(envelope.into_credentials()?);
        let authority = ProviderAccountRuntimeAuthority::try_acquire(
            ProviderMarketAccount::AlpacaBasic,
            lease,
            Arc::clone(&self.onboarding),
            &self.app_config,
            self.provider_rate.clone(),
        )?;
        Ok(AlpacaBasicAccountActivation {
            authority,
            credentials,
            iex: Some(iex),
            options,
        })
    }
}

fn validate_configurations(
    lease: &ProviderActivationLease,
    binding: &ProviderAccountBinding,
    iex: &AlpacaIexLiveConfig,
    options: Option<&AlpacaOptionsLiveConfig>,
) -> Result<(), AlpacaBasicActivationError> {
    let expected_budget = ProviderRateDeclaration::try_for_authorization_subject(
        lease
            .provider_budget_policy()
            .cloned()
            .ok_or(AlpacaBasicActivationError::SourceBinding)?,
        binding.subject(),
    )
    .map_err(|_error| AlpacaBasicActivationError::SourceBinding)?
    .policy()
    .clone();
    let iex_metadata = iex.metadata();
    if !binding.validates_metadata(iex_metadata)
        || iex_metadata.quality_ceiling() != DataQuality::DirectUnverified
        || iex_metadata.budget_policy() != Some(&expected_budget)
        || iex.endpoint() != "wss://stream.data.alpaca.markets/v2/iex"
    {
        return Err(AlpacaBasicActivationError::SourceBinding);
    }
    if let Some(options) = options {
        let metadata = options.metadata();
        if !binding.validates_metadata(metadata)
            || metadata.quality_ceiling() != DataQuality::Indicative
            || metadata.budget_policy() != Some(&expected_budget)
            || options.endpoint() != "wss://stream.data.alpaca.markets/v1beta1/indicative"
        {
            return Err(AlpacaBasicActivationError::SourceBinding);
        }
    }
    Ok(())
}

/// Alpaca Basic account activation failure.
#[derive(Debug, thiserror::Error)]
pub enum AlpacaBasicActivationError {
    /// The caller cancelled before account ownership completed.
    #[error("Alpaca Basic activation was cancelled")]
    Cancelled,
    /// One logical source does not match the verified account, budget, endpoint, or quality.
    #[error("Alpaca Basic source binding is invalid")]
    SourceBinding,
    /// The common account admission failed.
    #[error(transparent)]
    Account(#[from] ProviderAccountActivationError),
    /// Secret parsing or adapter credential construction failed.
    #[error("Alpaca Basic credential material is invalid")]
    Credential,
    /// The exact active credential could not be read through the existing secret authority.
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
}

impl From<ProviderCredentialError> for AlpacaBasicActivationError {
    fn from(_error: ProviderCredentialError) -> Self {
        Self::Credential
    }
}
