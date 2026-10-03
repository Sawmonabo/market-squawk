//! Account-owned activation for Alpaca Basic IEX and indicative-options data.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

use market_squawk_adapter_alpaca::{
    AlpacaCredentials, AlpacaError, AlpacaIexLiveConfig, AlpacaInstrumentMapping,
    AlpacaOptionChainClient, AlpacaOptionChainConfig, AlpacaOptionChainSealRejoin,
    AlpacaOptionsLiveConfig, AlpacaTradingApiEnvironment,
};
use market_squawk_domain::DataQuality;
use market_squawk_sources::{
    ProviderCaptureSealRequest, ProviderRateAuthority, ProviderRateDeclaration,
    SharedProviderBudget, SourceMetadata, SourceProtocolProfile,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{ProviderActivationLease, ProviderOnboardingError};

use super::ProviderAdapterActivation;
use super::account::{
    ProviderAccountActivationError, ProviderAccountBinding, ProviderAccountRuntimeAuthority,
    ProviderAccountRuntimeCurrentness, ProviderMarketAccount,
};
use super::credentials::{AlpacaCredentialEnvelope, ProviderCredentialError};

/// Non-clone, account-owned Alpaca Basic runtime admission.
///
/// The owner retains the exclusive account authority and exact onboarding lease while the two
/// logical source configurations are moved once into central live supervision. The already-loaded
/// credentials are shared only with those bounded live children and the runtime-owned, revocable
/// historical subordinate; they remain zeroizing and redacted.
pub struct AlpacaBasicAccountActivation {
    authority: Arc<ProviderAccountRuntimeAuthority>,
    credentials: Arc<AlpacaCredentials>,
    historical_provider_rate: ProviderRateAuthority,
    trading_api_environment: AlpacaTradingApiEnvironment,
    iex: Option<AlpacaIexLiveConfig>,
    options: Option<AlpacaOptionsLiveConfig>,
}

/// One complete raw option-chain handoff produced under an exact active account generation.
///
/// Account currentness is deliberately not embedded in the raw material. The caller must seal
/// this response even if authority expires immediately after receipt, then independently acquire
/// [`AlpacaOptionChainRuntimeAuthority::acquire_publication_authority`] for canonical publication.
pub(crate) struct AlpacaOptionChainCapture {
    rejoin: AlpacaOptionChainSealRejoin,
    seal_request: ProviderCaptureSealRequest,
}

impl AlpacaOptionChainCapture {
    pub(crate) fn into_parts(self) -> (AlpacaOptionChainSealRejoin, ProviderCaptureSealRequest) {
        (self.rejoin, self.seal_request)
    }
}

impl std::fmt::Debug for AlpacaOptionChainCapture {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaOptionChainCapture")
            .field("rejoin", &self.rejoin)
            .field("seal_request", &"OPAQUE ONE-USE PHYSICAL SEAL REQUEST")
            .finish()
    }
}

/// Revocable exact-account runtime for bounded complete indicative option-chain acquisition.
pub(crate) struct AlpacaOptionChainRuntimeAuthority {
    client: AlpacaOptionChainClient,
    metadata: SourceMetadata,
    credentials: Arc<AlpacaCredentials>,
    budget: SharedProviderBudget,
    currentness: ProviderAccountRuntimeCurrentness,
    accepting: AtomicBool,
    active: AtomicUsize,
    idle: Notify,
    cancellation: CancellationToken,
}

impl AlpacaOptionChainRuntimeAuthority {
    pub(crate) const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Reads historical renewal evidence under this exact current account lease.
    pub(crate) async fn retained_alpaca_doctor_renewal_chain(
        &self,
        original_verified_at: market_squawk_domain::Timestamp,
    ) -> Result<market_squawk_sources::AlpacaDoctorRenewalChain, AlpacaOptionChainRuntimeError>
    {
        self.ensure_accepting()?;
        self.currentness
            .retained_alpaca_doctor_renewal_chain(original_verified_at)
            .await
            .map_err(|_| AlpacaOptionChainRuntimeError::Stale)
    }

    /// Acquires all REST pages under one deadline and the process-wide account budget.
    ///
    /// A successfully received response is returned for sealing without a post-response
    /// currentness gate. This preserves raw evidence across a concurrent expiry; canonical
    /// publication must reacquire currentness through this same authority.
    pub(crate) async fn acquire_complete_chain<Retain, Retained>(
        &self,
        underlying: &AlpacaInstrumentMapping,
        reference_request: &market_squawk_adapter_alpaca::AlpacaOptionContractReferenceRequest,
        deadline: Instant,
        caller_cancellation: &CancellationToken,
        retain: Retain,
    ) -> Result<AlpacaOptionChainCapture, AlpacaOptionChainRuntimeError>
    where
        Retain: FnMut(ProviderCaptureSealRequest) -> Retained,
        Retained: Future<Output = Result<(), AlpacaError>>,
    {
        let _operation = self.admit()?;
        if !self.currentness.is_active().await {
            return Err(AlpacaOptionChainRuntimeError::Stale);
        }
        let cancellation = self.cancellation.child_token();
        let acquisition = self.client.acquire_complete_chain(
            &self.credentials, &self.budget, underlying, reference_request,
            deadline, &cancellation, retain,
        );
        tokio::pin!(acquisition);
        let result = tokio::select! { biased;
            () = caller_cancellation.cancelled() => {
                cancellation.cancel();
                let result = acquisition.await;
                if matches!(result, Err(AlpacaError::CaptureMaterial)) {
                    return Err(AlpacaError::CaptureMaterial.into());
                }
                return Err(AlpacaOptionChainRuntimeError::Cancelled);
            },
            () = self.cancellation.cancelled() => {
                cancellation.cancel();
                let result = acquisition.await;
                if matches!(result, Err(AlpacaError::CaptureMaterial)) {
                    return Err(AlpacaError::CaptureMaterial.into());
                }
                return Err(AlpacaOptionChainRuntimeError::Revoked);
            },
            result = &mut acquisition => result?,
        };
        Ok(AlpacaOptionChainCapture { rejoin: result.0, seal_request: result.1 })
    }

    /// Waits for the existing account publication guard under the demand's time bounds.
    /// The caller retains the returned guard through the canonical commit.
    pub(crate) async fn acquire_publication_authority(
        &self,
        deadline: Instant,
        caller_cancellation: &CancellationToken,
    ) -> Result<super::ProviderAccountPublicationAuthority, AlpacaOptionChainRuntimeError> {
        self.ensure_accepting()?;
        if caller_cancellation.is_cancelled() {
            return Err(AlpacaOptionChainRuntimeError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(AlpacaError::DeadlineExceeded.into());
        }
        let account = tokio::select! { biased;
            () = caller_cancellation.cancelled() => {
                return Err(AlpacaOptionChainRuntimeError::Cancelled);
            }
            () = self.cancellation.cancelled() => {
                return Err(AlpacaOptionChainRuntimeError::Revoked);
            }
            () = tokio::time::sleep_until(deadline.into()) => {
                return Err(AlpacaError::DeadlineExceeded.into());
            }
            result = self.currentness.acquire_publication_authority() => {
                result.map_err(|_| AlpacaOptionChainRuntimeError::Stale)?
            }
        };
        if caller_cancellation.is_cancelled() {
            return Err(AlpacaOptionChainRuntimeError::Cancelled);
        }
        self.ensure_accepting()?;
        if Instant::now() >= deadline {
            return Err(AlpacaError::DeadlineExceeded.into());
        }
        Ok(account)
    }

    pub(crate) async fn acquire_option_contract_references<T, Retain, Retained>(
        &self,
        request: market_squawk_adapter_alpaca::AlpacaOptionContractReferenceRequest,
        bounds: market_squawk_sources::HttpRequestBounds,
        deadline: Instant,
        caller_cancellation: &CancellationToken,
        retain: Retain,
    ) -> Result<Vec<T>, AlpacaOptionChainRuntimeError>
    where
        Retain: FnMut(market_squawk_adapter_alpaca::AlpacaPendingOptionContractReferencePage) -> Retained,
        Retained: std::future::Future<Output = Result<T, market_squawk_adapter_alpaca::AlpacaError>>,
    {
        let _operation = self.admit()?;
        if !self.currentness.is_active().await { return Err(AlpacaOptionChainRuntimeError::Stale); }
        let client = market_squawk_adapter_alpaca::AlpacaOptionContractReferenceClient::try_new(self.metadata.clone(), bounds)?;
        let cancellation = self.cancellation.child_token();
        let acquisition = client.acquire_complete(
            &self.credentials, &self.budget, request, deadline, &cancellation, retain,
        );
        tokio::pin!(acquisition);
        tokio::select! { biased;
            () = caller_cancellation.cancelled() => {
                cancellation.cancel();
                // Keep the admitted operation and existing worker owned until page custody settles.
                let result = acquisition.await;
                if matches!(result, Err(market_squawk_adapter_alpaca::AlpacaError::CaptureMaterial)) {
                    return result.map_err(Into::into);
                }
                Err(AlpacaOptionChainRuntimeError::Cancelled)
            },
            () = self.cancellation.cancelled() => {
                cancellation.cancel();
                let result = acquisition.await;
                if matches!(result, Err(market_squawk_adapter_alpaca::AlpacaError::CaptureMaterial)) {
                    return result.map_err(Into::into);
                }
                Err(AlpacaOptionChainRuntimeError::Revoked)
            },
            result = &mut acquisition => result.map_err(Into::into),
        }
    }

    pub(crate) fn begin_revocation(&self) {
        self.accepting.store(false, Ordering::Release);
        self.cancellation.cancel();
        if self.active.load(Ordering::Acquire) == 0 {
            self.idle.notify_waiters();
        }
    }

    /// Revokes new work and waits until every admitted acquisition has stopped.
    pub(crate) async fn revoke_and_drain(&self) {
        self.begin_revocation();
        while self.active.load(Ordering::Acquire) != 0 {
            let notified = self.idle.notified();
            if self.active.load(Ordering::Acquire) != 0 {
                notified.await;
            }
        }
    }

    pub(crate) fn revocation_drained(&self) -> bool {
        !self.accepting.load(Ordering::Acquire) && self.active.load(Ordering::Acquire) == 0
    }

    pub(crate) const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    fn admit(&self) -> Result<AlpacaOptionChainOperation<'_>, AlpacaOptionChainRuntimeError> {
        self.ensure_accepting()?;
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .map_err(|_error| AlpacaOptionChainRuntimeError::Unavailable)?;
        if let Err(error) = self.ensure_accepting() {
            self.finish_operation();
            return Err(error);
        }
        Ok(AlpacaOptionChainOperation { authority: self })
    }

    fn ensure_accepting(&self) -> Result<(), AlpacaOptionChainRuntimeError> {
        if self.accepting.load(Ordering::Acquire) && !self.cancellation.is_cancelled() {
            Ok(())
        } else {
            Err(AlpacaOptionChainRuntimeError::Revoked)
        }
    }

    fn finish_operation(&self) {
        let previous = self.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "option-chain operation count underflow");
        if previous == 1 {
            self.idle.notify_waiters();
        }
    }
}

impl Drop for AlpacaOptionChainRuntimeAuthority {
    fn drop(&mut self) {
        self.begin_revocation();
    }
}

impl std::fmt::Debug for AlpacaOptionChainRuntimeAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaOptionChainRuntimeAuthority")
            .field("client", &"BOUNDED ALPACA OPTION CLIENT")
            .field("credentials", &"[REDACTED ZEROIZING CREDENTIALS]")
            .field("budget", &"[SHARED PROCESS AUTHORITY]")
            .field("accepting", &self.accepting.load(Ordering::Acquire))
            .field("active", &self.active.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

struct AlpacaOptionChainOperation<'a> {
    authority: &'a AlpacaOptionChainRuntimeAuthority,
}

impl Drop for AlpacaOptionChainOperation<'_> {
    fn drop(&mut self) {
        self.authority.finish_operation();
    }
}

impl AlpacaBasicAccountActivation {
    /// Seals live configurations onto the same account owner that acquired native references.
    pub(crate) fn with_live_configurations(
        mut self,
        iex: AlpacaIexLiveConfig,
        options: Option<AlpacaOptionsLiveConfig>,
    ) -> Result<Self, AlpacaBasicActivationError> {
        if self.iex.is_some() || self.options.is_some() {
            return Err(AlpacaBasicActivationError::SourceBinding);
        }
        validate_configurations(self.lease(), self.account_binding(), &iex, options.as_ref())?;
        self.iex = Some(iex);
        self.options = options;
        Ok(self)
    }
    /// Returns the immutable onboarding lease retained by this runtime owner.
    pub fn lease(&self) -> &ProviderActivationLease {
        self.authority.lease()
    }

    /// Returns the stable, secret-free provider-account binding.
    pub fn account_binding(&self) -> &ProviderAccountBinding {
        self.authority.binding()
    }

    /// Returns shared zeroizing credentials for construction of admitted runtime children.
    pub fn credentials(&self) -> Arc<AlpacaCredentials> {
        Arc::clone(&self.credentials)
    }

    /// Delegates the same process-wide provider-rate authority to the historical subordinate.
    pub(crate) fn historical_provider_rate_authority(&self) -> ProviderRateAuthority {
        self.historical_provider_rate.clone()
    }

    /// Reuses this account's aggregate rate declaration for the pre-session asset reference GET.
    pub(crate) fn asset_reference_budget(&self) -> Result<SharedProviderBudget, AlpacaBasicActivationError> {
        let declaration = ProviderRateDeclaration::try_for_authorization_subject(
            self.lease().provider_budget_policy().cloned()
                .ok_or(AlpacaBasicActivationError::SourceBinding)?,
            self.account_binding().subject(),
        ).map_err(|_| AlpacaBasicActivationError::SourceBinding)?;
        self.historical_provider_rate.register_budget(declaration)
            .map_err(|_| AlpacaBasicActivationError::SourceBinding)
    }

    /// Returns the explicitly configured Trading API account environment used by calendar calls.
    pub(crate) const fn trading_api_environment(&self) -> AlpacaTradingApiEnvironment {
        self.trading_api_environment
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

    pub(crate) async fn require_prepared_or_active(&self) -> Result<(), ProviderOnboardingError> {
        self.authority.require_prepared_or_active().await
    }

    /// Returns a weak-only view for the common account-runtime currentness monitor.
    pub(crate) fn currentness(&self) -> ProviderAccountRuntimeCurrentness {
        self.authority.currentness()
    }

    /// Delegates currentness checks without cloning or extending the account authority lifetime.
    ///
    /// The returned validator retains only a weak reference to this activation's sole account
    /// authority. It neither rereads credentials nor acquires another runtime mutation authority.
    pub(crate) fn historical_currentness_validator(
        &self,
    ) -> impl Fn() -> Pin<Box<dyn Future<Output = bool> + Send + 'static>> + Clone + Send + Sync + 'static
    {
        let currentness = self.currentness();
        move || {
            let currentness = currentness.clone();
            Box::pin(async move { currentness.is_active().await })
                as Pin<Box<dyn Future<Output = bool> + Send + 'static>>
        }
    }

    /// Delegates a fail-closed synchronous check for post-extraction analytical callbacks.
    ///
    /// The closure retains only a weak reference to the existing account owner. It neither waits
    /// on onboarding mutation, rereads credentials, nor acquires another account/rate authority.
    pub(crate) fn historical_currentness_validator_now(
        &self,
    ) -> impl Fn() -> bool + Clone + Send + Sync + 'static {
        let currentness = self.currentness();
        move || currentness.is_active_now()
    }

    /// Binds the complete-chain REST child to this exact account owner and shared rate ledger.
    pub(crate) fn bind_option_chain_runtime(
        &self,
        config: AlpacaOptionChainConfig,
        cancellation: CancellationToken,
    ) -> Result<Arc<AlpacaOptionChainRuntimeAuthority>, AlpacaOptionChainRuntimeError> {
        if cancellation.is_cancelled() {
            return Err(AlpacaOptionChainRuntimeError::Cancelled);
        }
        let expected_budget = expected_budget(self.lease(), self.account_binding())?;
        let metadata = config.metadata();
        if !self.account_binding().validates_metadata(metadata)
            || metadata.quality_ceiling() != DataQuality::Indicative
            || metadata.budget_policy() != Some(&expected_budget)
            || !metadata.coverage().live_channels().is_empty()
            || metadata.capabilities().live()
            || !metadata.capabilities().extraction()
            || metadata.protocol_profile() != &SourceProtocolProfile::NotLive
            || config.provider_product().as_source_identifier().as_str()
                != "alpaca-basic-indicative-option-snapshots-v1"
            || config.provider_channel().as_source_identifier().as_str()
                != "rest-complete-chain-snapshots"
        {
            return Err(AlpacaOptionChainRuntimeError::SourceBinding);
        }
        let declaration = ProviderRateDeclaration::try_for_authorization_subject(
            self.lease()
                .provider_budget_policy()
                .cloned()
                .ok_or(AlpacaOptionChainRuntimeError::SourceBinding)?,
            self.account_binding().subject(),
        )
        .map_err(|_error| AlpacaOptionChainRuntimeError::SourceBinding)?;
        let budget = self
            .historical_provider_rate
            .register_budget(declaration)
            .map_err(|_error| AlpacaOptionChainRuntimeError::SourceBinding)?;
        let metadata = metadata.clone();
        let client = AlpacaOptionChainClient::try_new(config)?;
        Ok(Arc::new(AlpacaOptionChainRuntimeAuthority {
            client,
            metadata,
            credentials: self.credentials(),
            budget,
            currentness: self.currentness(),
            accepting: AtomicBool::new(true),
            active: AtomicUsize::new(0),
            idle: Notify::new(),
            cancellation,
        }))
    }


}

impl std::fmt::Debug for AlpacaBasicAccountActivation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaBasicAccountActivation")
            .field("authority", &self.authority)
            .field("credentials", &"[REDACTED ZEROIZING CREDENTIALS]")
            .field("historical_provider_rate", &"[SHARED PROCESS AUTHORITY]")
            .field("trading_api_environment", &self.trading_api_environment)
            .field("iex_config_available", &self.iex.is_some())
            .field("options_config_available", &self.options.is_some())
            .finish()
    }
}

impl ProviderAdapterActivation {
    /// Acquires the sole Alpaca account owner before native reference discovery or live bindings.
    ///
    /// # Errors
    ///
    /// Fails closed for a stale/mismatched lease, duplicated account runtime, invalid secret
    /// envelope, or cancellation. Live configurations are bound after native reference admission.
    pub(crate) async fn activate_alpaca_basic_account(
        &self,
        lease: ProviderActivationLease,
        cancellation: CancellationToken,
    ) -> Result<AlpacaBasicAccountActivation, AlpacaBasicActivationError> {
        if cancellation.is_cancelled() {
            return Err(AlpacaBasicActivationError::Cancelled);
        }
        ProviderAccountBinding::try_from_lease(ProviderMarketAccount::AlpacaBasic, &lease)?;
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
        let trading_api_environment = envelope.trading_api_environment();
        let credentials = Arc::new(envelope.into_credentials()?);
        let provider_rate = self.provider_rate.clone();
        let authority = Arc::new(
            ProviderAccountRuntimeAuthority::try_acquire_prepared_or_active(
                ProviderMarketAccount::AlpacaBasic,
                lease,
                Arc::clone(&self.onboarding),
                &self.app_config,
                provider_rate.clone(),
            )?,
        );
        Ok(AlpacaBasicAccountActivation {
            authority,
            credentials,
            historical_provider_rate: provider_rate,
            trading_api_environment,
            iex: None,
            options: None,
        })
    }

}

fn expected_budget(
    lease: &ProviderActivationLease,
    binding: &ProviderAccountBinding,
) -> Result<market_squawk_sources::ProviderBudgetPolicy, AlpacaOptionChainRuntimeError> {
    ProviderRateDeclaration::try_for_authorization_subject(
        lease
            .provider_budget_policy()
            .cloned()
            .ok_or(AlpacaOptionChainRuntimeError::SourceBinding)?,
        binding.subject(),
    )
    .map(|declaration| declaration.policy().clone())
    .map_err(|_error| AlpacaOptionChainRuntimeError::SourceBinding)
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

/// Fail-closed complete-chain runtime failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AlpacaOptionChainRuntimeError {
    #[error("Alpaca option-chain operation was cancelled")]
    Cancelled,
    #[error("Alpaca option-chain runtime was revoked")]
    Revoked,
    #[error("Alpaca option-chain account authority is stale")]
    Stale,
    #[error("Alpaca option-chain runtime capacity is unavailable")]
    Unavailable,
    #[error("Alpaca option-chain source binding is invalid")]
    SourceBinding,
    #[error(transparent)]
    Adapter(#[from] AlpacaError),
}

impl ProviderAdapterActivation {
    pub(crate) fn register_alpaca_publication_generation(
        &self,
        activation: &AlpacaBasicAccountActivation,
        metadata: &SourceMetadata,
    ) -> Result<
        crate::application::ResearchProviderRuntimeGeneration,
        crate::application::AlpacaMarketPublicationError,
    > {
        use crate::application::AlpacaMarketPublicationError as E;
        if !activation.account_binding().validates_metadata(metadata) {
            tracing::warn!(
                stage = "account_metadata",
                "Alpaca publication registration failed"
            );
            return Err(E::AuthorityInvalid);
        }
        let (generation, rights) = super::public_live_runtime_generation(activation.lease(), metadata)
            .map_err(|error| {
                tracing::warn!(stage = "generation_construction", %error, "Alpaca publication registration failed");
                E::AuthorityInvalid
            })?;
        self.research_mutation.register_provider_publication_generation(generation.clone(), rights)
            .map_err(|error| {
                tracing::warn!(stage = "coordinator_registration", %error, "Alpaca publication registration failed");
                E::AuthorityInvalid
            })?;
        Ok(generation)
    }

    pub(crate) fn bind_alpaca_publication_runtime_after_reference(
        &self,
        activation: &AlpacaBasicAccountActivation,
        generation: crate::application::ResearchProviderRuntimeGeneration,
        bindings: &[super::MarketDataInstrumentBinding],
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<crate::application::AlpacaPublicationRuntimeInput, crate::application::AlpacaMarketPublicationError> {
        self.research.bind_alpaca_publication_runtime(
            generation, bindings, activation.currentness(), cancellation, deadline,
        )
    }
}
