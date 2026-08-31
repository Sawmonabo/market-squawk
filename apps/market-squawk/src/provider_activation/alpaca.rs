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
use market_squawk_data::{IngestError, IngestPrecommitAuthority};
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
/// this response even if authority expires immediately after receipt, then independently call
/// [`AlpacaOptionChainRuntimeAuthority::require_current_now`] before canonical publication.
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

/// Exact activated Alpaca live-source set retained through each immutable publication commit.
///
/// The authority owns no account capability: it keeps only immutable metadata, a weak currentness
/// view, and the group cancellation edge. Dropping the sole account owner therefore makes every
/// later precommit fail closed.
pub(crate) struct AlpacaMarketPublicationAuthority {
    sources: Box<[SourceMetadata]>,
    currentness: ProviderAccountRuntimeCurrentness,
    accepting: AtomicBool,
    cancellation: CancellationToken,
}

impl AlpacaMarketPublicationAuthority {
    pub(crate) fn validates_metadata(&self, metadata: &SourceMetadata) -> bool {
        self.sources.iter().any(|source| source == metadata)
    }

    pub(crate) fn begin_revocation(&self) {
        self.accepting.store(false, Ordering::Release);
        self.cancellation.cancel();
    }

    fn ensure_current(&self) -> Result<(), IngestError> {
        if self.accepting.load(Ordering::Acquire)
            && !self.cancellation.is_cancelled()
            && self.currentness.is_active_now()
        {
            Ok(())
        } else {
            Err(IngestError::PublicationAuthorityRevoked)
        }
    }
}

impl IngestPrecommitAuthority for AlpacaMarketPublicationAuthority {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.ensure_current()
    }
}

impl Drop for AlpacaMarketPublicationAuthority {
    fn drop(&mut self) {
        self.begin_revocation();
    }
}

impl std::fmt::Debug for AlpacaMarketPublicationAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaMarketPublicationAuthority")
            .field(
                "source_ids",
                &self
                    .sources
                    .iter()
                    .map(SourceMetadata::source_id)
                    .collect::<Vec<_>>(),
            )
            .field("accepting", &self.accepting.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl AlpacaOptionChainRuntimeAuthority {
    pub(crate) const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Acquires all REST pages under one deadline and the process-wide account budget.
    ///
    /// A successfully received response is returned for sealing without a post-response
    /// currentness gate. This preserves raw evidence across a concurrent expiry; canonical
    /// publication must reacquire currentness through this same authority.
    pub(crate) async fn acquire_complete_chain(
        &self,
        underlying: &AlpacaInstrumentMapping,
        deadline: Instant,
        caller_cancellation: &CancellationToken,
    ) -> Result<AlpacaOptionChainCapture, AlpacaOptionChainRuntimeError> {
        let _operation = self.admit()?;
        if !self.currentness.is_active().await {
            return Err(AlpacaOptionChainRuntimeError::Stale);
        }
        let operation_cancellation = self.cancellation.child_token();
        let result = tokio::select! {
            biased;
            () = caller_cancellation.cancelled() => {
                operation_cancellation.cancel();
                return Err(AlpacaOptionChainRuntimeError::Cancelled);
            }
            () = self.cancellation.cancelled() => {
                operation_cancellation.cancel();
                return Err(AlpacaOptionChainRuntimeError::Revoked);
            }
            result = self.client.acquire_complete_chain(
                &self.credentials,
                &self.budget,
                underlying,
                deadline,
                &operation_cancellation,
            ) => result?,
        };
        Ok(AlpacaOptionChainCapture {
            rejoin: result.0,
            seal_request: result.1,
        })
    }

    /// Asynchronously revalidates the sole account authority before publication admission.
    pub(crate) async fn require_current(&self) -> Result<(), AlpacaOptionChainRuntimeError> {
        self.ensure_accepting()?;
        if self.currentness.is_active().await {
            self.ensure_accepting()
        } else {
            Err(AlpacaOptionChainRuntimeError::Stale)
        }
    }

    /// Fails closed at the final precommit boundary without waiting on onboarding mutation.
    pub(crate) fn require_current_now(&self) -> Result<(), AlpacaOptionChainRuntimeError> {
        self.ensure_accepting()?;
        if self.currentness.is_active_now() {
            self.ensure_accepting()
        } else {
            Err(AlpacaOptionChainRuntimeError::Stale)
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

impl IngestPrecommitAuthority for AlpacaOptionChainRuntimeAuthority {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.require_current_now()
            .map_err(|_error| IngestError::PublicationAuthorityRevoked)
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
            || metadata.coverage().live().is_some()
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

    /// Binds the exact activated IEX/indicative live metadata to a fail-closed commit authority.
    pub(crate) fn bind_market_publication_authority(
        &self,
        sources: Vec<SourceMetadata>,
        cancellation: CancellationToken,
    ) -> Result<Arc<AlpacaMarketPublicationAuthority>, AlpacaOptionChainRuntimeError> {
        let expected_count = 1_usize + usize::from(self.options.is_some());
        let expected_budget = expected_budget(self.lease(), self.account_binding())?;
        if cancellation.is_cancelled()
            || self.iex.is_none()
            || sources.len() != expected_count
            || sources.is_empty()
        {
            return Err(AlpacaOptionChainRuntimeError::SourceBinding);
        }
        for (ordinal, source) in sources.iter().enumerate() {
            let live = source
                .coverage()
                .live()
                .ok_or(AlpacaOptionChainRuntimeError::SourceBinding)?;
            let exact_surface = matches!(
                (
                    source.quality_ceiling(),
                    live.provider_product().as_source_identifier().as_str(),
                    live.provider_channel().as_source_identifier().as_str(),
                ),
                (
                    DataQuality::DirectUnverified,
                    "alpaca-basic-iex-configured-symbols-v1",
                    "trades+quotes+statuses",
                ) | (
                    DataQuality::Indicative,
                    "alpaca-basic-indicative-options-configured-symbols-v1",
                    "trades+quotes-msgpack",
                )
            );
            if !self.account_binding().validates_metadata(source)
                || !source.capabilities().live()
                || source.budget_policy() != Some(&expected_budget)
                || !exact_surface
                || !self
                    .iex
                    .as_ref()
                    .is_some_and(|iex| iex.metadata() == source)
                    && !self
                        .options
                        .as_ref()
                        .is_some_and(|options| options.metadata() == source)
                || sources[..ordinal]
                    .iter()
                    .any(|earlier| earlier.source_id() == source.source_id())
            {
                return Err(AlpacaOptionChainRuntimeError::SourceBinding);
            }
        }
        Ok(Arc::new(AlpacaMarketPublicationAuthority {
            sources: sources.into_boxed_slice(),
            currentness: self.currentness(),
            accepting: AtomicBool::new(true),
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
            iex: Some(iex),
            options,
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
