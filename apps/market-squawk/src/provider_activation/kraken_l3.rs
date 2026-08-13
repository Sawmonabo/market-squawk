//! Authenticated Kraken level-3 account activation and short-lived token authority.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt as _;
use market_squawk_adapter_kraken::{
    KRAKEN_L3_WEBSOCKET_ENDPOINT, KrakenL3Config, KrakenL3WebSocketToken,
};
use market_squawk_domain::{DataQuality, MarketDepth, SequenceCapability, Timestamp};
use market_squawk_sources::{
    BudgetDecision, BudgetPermit, ProviderRateDeclaration, SharedProviderBudget,
    apply_http_retry_after, install_ring_tls_provider,
};
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, RETRY_AFTER};
use serde::{Deserialize, Deserializer};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::{ProviderActivationLease, ProviderOnboardingError};

use super::ProviderAdapterActivation;
use super::account::{
    ProviderAccountActivationError, ProviderAccountBinding, ProviderAccountRuntimeAuthority,
    ProviderAccountRuntimeCurrentness, ProviderMarketAccount,
};
use super::credentials::{KrakenL3CredentialSigner, ProviderCredentialError, next_kraken_nonce};

const TOKEN_REQUEST_DEADLINE: Duration = Duration::from_secs(15);
const MAX_TOKEN_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_TOKEN_VALIDITY_SECONDS: u64 = 15 * 60;

/// Non-clone owner of one verified Kraken key and order-level source configuration.
pub struct KrakenL3AccountActivation {
    authority: Arc<ProviderAccountRuntimeAuthority>,
    signer: KrakenL3CredentialSigner,
    client: reqwest::Client,
    budget: SharedProviderBudget,
    config: Option<KrakenL3Config>,
}

impl KrakenL3AccountActivation {
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

    /// Moves the exact order-level configuration into central supervision once.
    pub fn take_config(&mut self) -> Option<KrakenL3Config> {
        self.config.take()
    }

    /// Obtains one bounded, short-lived WebSocket token through the exact Kraken endpoint.
    ///
    /// The API key and signing secret never leave this owner. The shared provider/account budget,
    /// persisted monotonic nonce, exact active lease, fixed endpoint, TLS policy, bounded response,
    /// and cancellation deadline are applied before token material is returned.
    pub async fn acquire_websocket_token(
        &self,
        cancellation: CancellationToken,
    ) -> Result<KrakenL3WebSocketTokenMaterial, KrakenL3ActivationError> {
        self.authority.require_current().await?;
        let permit = acquire_budget(&self.budget, &cancellation).await?;
        let nonce = self.authority.next_persisted_nonce(next_kraken_nonce()?)?;
        let request = self.signer.websocket_token_request(&self.client, nonce)?;
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(KrakenL3ActivationError::Cancelled);
            }
            response = tokio::time::timeout(TOKEN_REQUEST_DEADLINE, request.send()) => {
                response
                    .map_err(|_elapsed| KrakenL3ActivationError::Deadline)?
                    .map_err(|_error| KrakenL3ActivationError::Network)?
            }
        };
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let _decision = apply_http_retry_after(
                &self.budget,
                response
                    .headers()
                    .get(RETRY_AFTER)
                    .map(|value| value.as_bytes()),
                1_000,
            );
            return Err(KrakenL3ActivationError::RateLimited);
        }
        if matches!(
            response.status(),
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        ) {
            return Err(KrakenL3ActivationError::CredentialRejected);
        }
        if response.url().as_str() != market_squawk_adapter_kraken::KRAKEN_L3_GET_TOKEN_ENDPOINT
            || !response_has_single_json_content_type(response.headers())
            || response
                .headers()
                .get(CONTENT_ENCODING)
                .is_some_and(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
            || !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > MAX_TOKEN_RESPONSE_BYTES as u64)
        {
            return Err(KrakenL3ActivationError::Network);
        }
        let mut body = Zeroizing::new(Vec::new());
        let mut stream = response.bytes_stream();
        while let Some(chunk) = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(KrakenL3ActivationError::Cancelled);
            }
            next = stream.next() => next,
        } {
            let chunk = chunk.map_err(|_error| KrakenL3ActivationError::Network)?;
            let next_length = body
                .len()
                .checked_add(chunk.len())
                .filter(|length| *length <= MAX_TOKEN_RESPONSE_BYTES)
                .ok_or(KrakenL3ActivationError::Response)?;
            let additional = next_length.saturating_sub(body.len());
            body.reserve(additional);
            body.extend_from_slice(&chunk);
        }
        let response: KrakenTokenResponse =
            serde_json::from_slice(&body).map_err(|_error| KrakenL3ActivationError::Response)?;
        let result = response
            .error
            .is_empty()
            .then_some(response.result)
            .flatten()
            .ok_or(KrakenL3ActivationError::CredentialRejected)?;
        if result.expires == 0 || result.expires > MAX_TOKEN_VALIDITY_SECONDS {
            return Err(KrakenL3ActivationError::Response);
        }
        KrakenL3WebSocketToken::try_new(result.token.as_str())?;
        self.authority.require_current().await?;
        self.budget
            .record_success()
            .map_err(|_reason| KrakenL3ActivationError::RateLimited)?;
        drop(permit);
        let expires_at = timestamp_after_seconds(result.expires)?;
        Ok(KrakenL3WebSocketTokenMaterial {
            token: result.token.0,
            expires_at,
        })
    }

    /// Revalidates the exact active credential generation outside the live event path.
    pub async fn require_current(&self) -> Result<(), ProviderOnboardingError> {
        self.authority.require_current().await
    }
}

impl std::fmt::Debug for KrakenL3AccountActivation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KrakenL3AccountActivation")
            .field("authority", &self.authority)
            .field("signer", &"[REDACTED ZEROIZING SIGNER]")
            .field("config_available", &self.config.is_some())
            .finish()
    }
}

/// Owned short-lived token material whose only view is Kraken's borrowed redacted wrapper.
pub struct KrakenL3WebSocketTokenMaterial {
    token: Zeroizing<String>,
    expires_at: Timestamp,
}

impl KrakenL3WebSocketTokenMaterial {
    /// Borrows the token for one immediate authenticated subscription encoding.
    pub fn token(&self) -> Result<KrakenL3WebSocketToken<'_>, KrakenL3ActivationError> {
        KrakenL3WebSocketToken::try_new(self.token.as_str()).map_err(Into::into)
    }

    /// Returns the conservative local validity deadline from the provider response.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

impl std::fmt::Debug for KrakenL3WebSocketTokenMaterial {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KrakenL3WebSocketTokenMaterial")
            .field("token", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl ProviderAdapterActivation {
    /// Activates one authenticated Kraken order-level market-data account.
    pub(crate) async fn activate_kraken_l3_account(
        &self,
        lease: ProviderActivationLease,
        config: KrakenL3Config,
        cancellation: CancellationToken,
    ) -> Result<KrakenL3AccountActivation, KrakenL3ActivationError> {
        if cancellation.is_cancelled() {
            return Err(KrakenL3ActivationError::Cancelled);
        }
        let binding =
            ProviderAccountBinding::try_from_lease(ProviderMarketAccount::KrakenLevel3, &lease)?;
        validate_configuration(&lease, &binding, &config)?;
        let secret = self
            .onboarding
            .read_secret_for_activation_request(&lease, cancellation)
            .await?;
        let signer = KrakenL3CredentialSigner::try_parse(secret.expose_secret())?;
        if signer.account_digest()
            != lease
                .account_digest()
                .ok_or(KrakenL3ActivationError::SourceBinding)?
        {
            return Err(KrakenL3ActivationError::SourceBinding);
        }
        let declaration = ProviderRateDeclaration::try_for_authorization_subject(
            lease
                .provider_budget_policy()
                .cloned()
                .ok_or(KrakenL3ActivationError::SourceBinding)?,
            binding.subject(),
        )?;
        let budget = self.provider_rate.register_budget(declaration)?;
        let tls = install_ring_tls_provider()?;
        let _provider_identity = tls.provider_id();
        let client = reqwest::Client::builder()
            .https_only(true)
            .tls_backend_rustls()
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .retry(reqwest::retry::never())
            .referer(false)
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(Duration::from_secs(5))
            .read_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(10))
            .user_agent(concat!(
                env!("CARGO_PKG_NAME"),
                "/",
                env!("CARGO_PKG_VERSION")
            ))
            .build()
            .map_err(|_error| KrakenL3ActivationError::Client)?;
        let authority = Arc::new(ProviderAccountRuntimeAuthority::try_acquire(
            ProviderMarketAccount::KrakenLevel3,
            lease,
            Arc::clone(&self.onboarding),
            &self.app_config,
            self.provider_rate.clone(),
        )?);
        Ok(KrakenL3AccountActivation {
            authority,
            signer,
            client,
            budget,
            config: Some(config),
        })
    }
}

fn validate_configuration(
    lease: &ProviderActivationLease,
    binding: &ProviderAccountBinding,
    config: &KrakenL3Config,
) -> Result<(), KrakenL3ActivationError> {
    let metadata = config.metadata();
    let expected_budget = ProviderRateDeclaration::try_for_authorization_subject(
        lease
            .provider_budget_policy()
            .cloned()
            .ok_or(KrakenL3ActivationError::SourceBinding)?,
        binding.subject(),
    )
    .map_err(|_error| KrakenL3ActivationError::SourceBinding)?
    .policy()
    .clone();
    if !binding.validates_metadata(metadata)
        || metadata.quality_ceiling() != DataQuality::DirectUnverified
        || metadata.capabilities().sequence() != SequenceCapability::Unsupported
        || metadata.budget_policy() != Some(&expected_budget)
        || config.market_depth() != MarketDepth::OrderLevel
        || config.credential_record_id() != binding.subject()
        || config.endpoint().as_str() != KRAKEN_L3_WEBSOCKET_ENDPOINT
    {
        return Err(KrakenL3ActivationError::SourceBinding);
    }
    Ok(())
}

async fn acquire_budget(
    budget: &SharedProviderBudget,
    cancellation: &CancellationToken,
) -> Result<BudgetPermit, KrakenL3ActivationError> {
    let deadline = tokio::time::Instant::now()
        .checked_add(TOKEN_REQUEST_DEADLINE)
        .ok_or(KrakenL3ActivationError::Deadline)?;
    loop {
        match budget.try_acquire() {
            BudgetDecision::Ready(permit) => return Ok(permit),
            BudgetDecision::WaitUntil(wait_until) => {
                let wait = budget
                    .remaining_wait(wait_until)
                    .map_err(|_reason| KrakenL3ActivationError::RateLimited)?;
                if tokio::time::Instant::now()
                    .checked_add(wait)
                    .is_none_or(|ready| ready > deadline)
                {
                    return Err(KrakenL3ActivationError::RateLimited);
                }
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {
                        return Err(KrakenL3ActivationError::Cancelled);
                    }
                    () = tokio::time::sleep(wait) => {}
                }
            }
            BudgetDecision::Unavailable(_reason) => {
                return Err(KrakenL3ActivationError::RateLimited);
            }
        }
    }
}

fn timestamp_after_seconds(seconds: u64) -> Result<Timestamp, KrakenL3ActivationError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| KrakenL3ActivationError::Clock)?;
    let seconds = NonZeroU64::new(seconds).ok_or(KrakenL3ActivationError::Response)?;
    let expires = now
        .checked_add(Duration::from_secs(seconds.get()))
        .ok_or(KrakenL3ActivationError::Clock)?;
    let nanos =
        i64::try_from(expires.as_nanos()).map_err(|_error| KrakenL3ActivationError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn response_has_single_json_content_type(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    value
        .to_str()
        .ok()
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

#[derive(Deserialize)]
struct KrakenTokenResponse {
    #[serde(default)]
    error: Vec<String>,
    result: Option<KrakenTokenResult>,
}

#[derive(Deserialize)]
struct KrakenTokenResult {
    token: SecretTokenField,
    expires: u64,
}

struct SecretTokenField(Zeroizing<String>);

impl SecretTokenField {
    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<'de> Deserialize<'de> for SecretTokenField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(|value| Self(Zeroizing::new(value)))
    }
}

/// Kraken authenticated level-3 activation or token-acquisition failure.
#[derive(Debug, thiserror::Error)]
pub enum KrakenL3ActivationError {
    /// The caller cancelled the operation.
    #[error("Kraken level-3 activation was cancelled")]
    Cancelled,
    /// The active lease or source metadata does not match the exact L3 contract.
    #[error("Kraken level-3 source binding is invalid")]
    SourceBinding,
    /// A short-lived request exceeded its fixed deadline.
    #[error("Kraken level-3 token request deadline elapsed")]
    Deadline,
    /// The shared provider/account budget cannot admit the request.
    #[error("Kraken level-3 provider budget is unavailable")]
    RateLimited,
    /// The provider rejected the exact credential or least-authority key.
    #[error("Kraken level-3 credential was rejected")]
    CredentialRejected,
    /// The fixed provider request could not be completed.
    #[error("Kraken level-3 token network request failed")]
    Network,
    /// The provider returned an invalid or oversized token response.
    #[error("Kraken level-3 token response is invalid")]
    Response,
    /// The hardened HTTP client could not be constructed.
    #[error("Kraken level-3 token client could not be constructed")]
    Client,
    /// The local wall clock cannot represent the nonce or token deadline.
    #[error("Kraken level-3 token clock is unavailable")]
    Clock,
    /// The common account admission failed.
    #[error(transparent)]
    Account(#[from] ProviderAccountActivationError),
    /// The existing secret authority or active onboarding generation failed.
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    /// Secret parsing or request signing failed.
    #[error("Kraken level-3 credential material is invalid")]
    Credential,
    /// Kraken rejected configuration or token material.
    #[error(transparent)]
    Kraken(#[from] market_squawk_adapter_kraken::KrakenL3ConfigError),
    /// Provider-budget declaration or registration was inconsistent.
    #[error(transparent)]
    Budget(#[from] market_squawk_sources::BudgetPoolError),
    /// Process TLS installation failed.
    #[error(transparent)]
    Tls(#[from] market_squawk_sources::TlsProviderError),
}

impl From<ProviderCredentialError> for KrakenL3ActivationError {
    fn from(_error: ProviderCredentialError) -> Self {
        Self::Credential
    }
}
