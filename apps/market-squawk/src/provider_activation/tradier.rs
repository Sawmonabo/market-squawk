//! One-account activation for Tradier's distinct logical market-data surfaces.

use std::sync::Arc;

use market_squawk_adapter_tradier::{
    TradierAccessSurface, TradierAccountMarketData, TradierAccountMarketDataError,
    TradierLogicalProfile, TradierSnapshotClient, TradierSourceConfig,
    TradierSubscriptionAuthority, TradierTransportLimits,
};
use market_squawk_domain::{DataQuality, SourceIdentifier};
use market_squawk_sources::{
    LiveSourceGeneration, ProviderRateDeclaration, install_ring_tls_provider,
};
use tokio_util::sync::CancellationToken;

use crate::{ProviderActivationLease, ProviderOnboardingError};

use super::ProviderAdapterActivation;
use super::account::{
    ProviderAccountActivationError, ProviderAccountBinding, ProviderAccountRuntimeAuthority,
    ProviderAccountRuntimeCurrentness, ProviderMarketAccount,
};
use super::credentials::{ProviderCredentialError, tradier_access_token};

/// Non-clone owner of one Tradier account and all admitted logical market-data configurations.
pub struct TradierMarketDataAccountActivation {
    authority: Arc<ProviderAccountRuntimeAuthority>,
    account: Arc<TradierAccountMarketData>,
    consolidated_stream: Option<TradierSourceConfig>,
    subscriptions: TradierSubscriptionAuthority,
    consolidated_snapshots: Option<TradierSourceConfig>,
    derived_indexes: Option<TradierSourceConfig>,
}

impl TradierMarketDataAccountActivation {
    /// Returns the immutable onboarding lease retained by this runtime owner.
    pub fn lease(&self) -> &ProviderActivationLease {
        self.authority.lease()
    }

    /// Returns the stable, secret-free provider-account binding.
    pub fn account_binding(&self) -> &ProviderAccountBinding {
        self.authority.binding()
    }

    /// Returns a weak-only view for the common account-runtime currentness monitor.
    pub(crate) fn currentness(&self) -> ProviderAccountRuntimeCurrentness {
        self.authority.currentness()
    }

    /// Transfers the generation-neutral stream plan into central supervision once.
    pub fn take_streaming_activation(
        &mut self,
    ) -> Result<TradierStreamingActivation, TradierMarketDataActivationError> {
        let config = self
            .consolidated_stream
            .take()
            .ok_or(TradierMarketDataActivationError::AlreadyConsumed)?;
        Ok(TradierStreamingActivation {
            account: Arc::clone(&self.account),
            config,
            subscriptions: self.subscriptions.clone(),
        })
    }

    /// Constructs the consolidated-securities REST/bootstrap client once.
    pub fn take_consolidated_snapshot_client(
        &mut self,
        generation: LiveSourceGeneration,
    ) -> Result<TradierSnapshotClient, TradierMarketDataActivationError> {
        let config = self
            .consolidated_snapshots
            .take()
            .ok_or(TradierMarketDataActivationError::AlreadyConsumed)?;
        self.account
            .snapshot_client(config, generation)
            .map_err(Into::into)
    }

    /// Constructs the provider-derived index REST client once.
    pub fn take_derived_index_client(
        &mut self,
        generation: LiveSourceGeneration,
    ) -> Result<TradierSnapshotClient, TradierMarketDataActivationError> {
        let config = self
            .derived_indexes
            .take()
            .ok_or(TradierMarketDataActivationError::AlreadyConsumed)?;
        self.account
            .snapshot_client(config, generation)
            .map_err(Into::into)
    }

    /// Returns whether the account owner currently holds Tradier's sole market stream.
    pub fn has_active_stream(&self) -> bool {
        self.account.has_active_stream()
    }

    /// Revalidates the exact active credential generation outside the live event path.
    pub async fn require_current(&self) -> Result<(), ProviderOnboardingError> {
        self.authority.require_current().await
    }
}

/// Generation-neutral handoff retained by the central reconnecting supervisor.
#[derive(Debug)]
pub struct TradierStreamingActivation {
    account: Arc<TradierAccountMarketData>,
    config: TradierSourceConfig,
    subscriptions: TradierSubscriptionAuthority,
}

impl TradierStreamingActivation {
    /// Returns the single account owner shared with Tradier REST surfaces.
    pub fn account(&self) -> Arc<TradierAccountMarketData> {
        Arc::clone(&self.account)
    }

    /// Returns the immutable stream configuration reused to mint each fresh generation.
    pub const fn config(&self) -> &TradierSourceConfig {
        &self.config
    }

    /// Returns the latest-selection authority retained across reconnect generations.
    pub const fn subscriptions(&self) -> &TradierSubscriptionAuthority {
        &self.subscriptions
    }
}

impl std::fmt::Debug for TradierMarketDataAccountActivation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TradierMarketDataAccountActivation")
            .field("authority", &self.authority)
            .field("account", &"[NON-CLONE CREDENTIAL OWNER]")
            .field(
                "consolidated_stream_available",
                &self.consolidated_stream.is_some(),
            )
            .field(
                "consolidated_snapshots_available",
                &self.consolidated_snapshots.is_some(),
            )
            .field("derived_indexes_available", &self.derived_indexes.is_some())
            .finish()
    }
}

impl ProviderAdapterActivation {
    /// Activates one production Tradier Brokerage market-data account.
    ///
    /// The result owns exactly one token and physical stream gate while retaining separate
    /// `Aggregated` consolidated-security and `Modeled` derived-index configurations.
    pub(crate) async fn activate_tradier_market_data_account(
        &self,
        lease: ProviderActivationLease,
        consolidated_stream: TradierSourceConfig,
        consolidated_snapshots: TradierSourceConfig,
        derived_indexes: Option<TradierSourceConfig>,
        limits: TradierTransportLimits,
        initial_stream_symbols: Vec<SourceIdentifier>,
        cancellation: CancellationToken,
    ) -> Result<TradierMarketDataAccountActivation, TradierMarketDataActivationError> {
        if cancellation.is_cancelled() {
            return Err(TradierMarketDataActivationError::Cancelled);
        }
        let binding = ProviderAccountBinding::try_from_lease(
            ProviderMarketAccount::TradierBrokerage,
            &lease,
        )?;
        validate_configurations(
            &lease,
            &binding,
            &consolidated_stream,
            &consolidated_snapshots,
            derived_indexes.as_ref(),
            limits,
        )?;
        let secret = self
            .onboarding
            .read_secret_for_activation_request(&lease, cancellation)
            .await?;
        let token = tradier_access_token(secret.expose_secret())?;
        let account = Arc::new(TradierAccountMarketData::try_new(
            token,
            limits,
            install_ring_tls_provider()?,
        )?);
        let subscriptions =
            account.subscription_authority(&consolidated_stream, initial_stream_symbols)?;
        let authority = Arc::new(ProviderAccountRuntimeAuthority::try_acquire(
            ProviderMarketAccount::TradierBrokerage,
            lease,
            Arc::clone(&self.onboarding),
            &self.app_config,
            self.provider_rate.clone(),
        )?);
        Ok(TradierMarketDataAccountActivation {
            authority,
            account,
            consolidated_stream: Some(consolidated_stream),
            subscriptions,
            consolidated_snapshots: Some(consolidated_snapshots),
            derived_indexes,
        })
    }
}

fn validate_configurations(
    lease: &ProviderActivationLease,
    binding: &ProviderAccountBinding,
    consolidated_stream: &TradierSourceConfig,
    consolidated_snapshots: &TradierSourceConfig,
    derived_indexes: Option<&TradierSourceConfig>,
    limits: TradierTransportLimits,
) -> Result<(), TradierMarketDataActivationError> {
    let expected_budget = ProviderRateDeclaration::try_for_authorization_subject(
        lease
            .provider_budget_policy()
            .cloned()
            .ok_or(TradierMarketDataActivationError::SourceBinding)?,
        binding.subject(),
    )
    .map_err(|_error| TradierMarketDataActivationError::SourceBinding)?
    .policy()
    .clone();
    let required = [
        (
            consolidated_stream,
            TradierLogicalProfile::ConsolidatedSecurities,
            TradierAccessSurface::Streaming,
            DataQuality::Aggregated,
        ),
        (
            consolidated_snapshots,
            TradierLogicalProfile::ConsolidatedSecurities,
            TradierAccessSurface::RestSnapshots,
            DataQuality::Aggregated,
        ),
    ];
    for (config, profile, access, quality) in required {
        if !binding.validates_metadata(config.metadata())
            || config.profile() != profile
            || config.access_surface() != access
            || config.metadata().quality_ceiling() != quality
            || config.metadata().budget_policy() != Some(&expected_budget)
            || config.transport_limits() != limits
        {
            return Err(TradierMarketDataActivationError::SourceBinding);
        }
    }
    if let Some(config) = derived_indexes
        && (!binding.validates_metadata(config.metadata())
            || config.profile() != TradierLogicalProfile::DerivedIndexes
            || config.access_surface() != TradierAccessSurface::RestSnapshots
            || config.metadata().quality_ceiling() != DataQuality::Modeled
            || config.metadata().budget_policy() != Some(&expected_budget)
            || config.transport_limits() != limits)
    {
        return Err(TradierMarketDataActivationError::SourceBinding);
    }
    Ok(())
}

/// Tradier account activation or one-time runtime-construction failure.
#[derive(Debug, thiserror::Error)]
pub enum TradierMarketDataActivationError {
    /// The caller cancelled before account ownership completed.
    #[error("Tradier market-data activation was cancelled")]
    Cancelled,
    /// A logical source overstates or mismatches its verified account contract.
    #[error("Tradier market-data source binding is invalid")]
    SourceBinding,
    /// A one-time logical source configuration was already moved into supervision.
    #[error("Tradier market-data source configuration was already consumed")]
    AlreadyConsumed,
    /// The common account admission failed.
    #[error(transparent)]
    Account(#[from] ProviderAccountActivationError),
    /// Secret parsing or token construction failed.
    #[error("Tradier credential material is invalid")]
    Credential,
    /// The active credential could not be read through the existing secret authority.
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    /// Tradier rejected account-owner or logical-source construction.
    #[error(transparent)]
    Tradier(#[from] TradierAccountMarketDataError),
    /// The process TLS capability was unavailable.
    #[error(transparent)]
    Tls(#[from] market_squawk_sources::TlsProviderError),
}

impl From<ProviderCredentialError> for TradierMarketDataActivationError {
    fn from(_error: ProviderCredentialError) -> Self {
        Self::Credential
    }
}
