//! Crash-recoverable protected Schwab OAuth continuation and token authority.
//!
//! Access and refresh tokens exist only in the configured [`SecretStore`]. The separate local
//! authority-state store retains opaque secret references and lifecycle metadata, including
//! prepared/indeterminate phases needed to recover conservatively after process interruption.

use std::fmt;
use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt as _;
use market_squawk_platform::{
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalSecretStoreError,
    SecretCancellation, SecretDeletionDisposition, SecretGeneration, SecretInteractionPolicy,
    SecretMutationKind, SecretMutationPlan, SecretOperationControl,
    SecretReconciliationObservation, SecretRef, SecretStore, SecretValue,
};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE,
    HeaderValue, USER_AGENT,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use zeroize::{Zeroize as _, Zeroizing};

use crate::{
    ACCESS_TOKEN_MAX_LIFETIME_SECONDS, AccessTokenAdmission, AccessTokenGeneration,
    AuthorizationRequest, OAuthCallback, OAuthTokenHttpRequest, ParseBounds,
    REFRESH_TOKEN_LIFETIME_SECONDS, RefreshTokenGeneration, RequestAdmission,
    SCHWAB_TOKEN_ENDPOINT, SchwabAccessTokenSource, SchwabAdapterError,
    SchwabApplicationCredentialEnvelope, TokenAuthorityError, TokenDecision, TokenGrant,
    TransientAccessToken, parse_token_response,
};

const AUTHORITY_STATE_VERSION: u16 = 1;
const TOKEN_SECRET_VERSION: u16 = 1;
const TOKEN_SECRET_SCOPE: &str = "market-squawk.schwab";
const TOKEN_SECRET_NAME: &str = "oauth-token";
const USER_AGENT_VALUE: &str = "market-squawk-schwab-oauth/1";

/// Local secret-operation controls for the protected token authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchwabOAuthSecretPolicy {
    timeout: Duration,
    retry_budget: u8,
}

impl SchwabOAuthSecretPolicy {
    pub fn try_new(timeout: Duration, retry_budget: u8) -> Result<Self, SchwabOAuthAuthorityError> {
        if timeout.is_zero() || retry_budget > 8 {
            return Err(SchwabOAuthAuthorityError::InvalidConfiguration);
        }
        Ok(Self {
            timeout,
            retry_budget,
        })
    }
}

/// Whether a foreground operation permits a platform-owned secret-store prompt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabOAuthInteraction {
    Background,
    Foreground,
}

/// Exact one-use guard for replacing the protected Schwab application credential generation.
///
/// Replacement is intentionally distinct from token refresh. It consumes the prior authority,
/// locally revokes every token minted under the prior application credential, and returns a new
/// authority in `AwaitingAuthorization` state. The replacement secret is validated before the
/// durable transition begins.
pub struct SchwabApplicationCredentialReplacement {
    expected: SecretRef,
    replacement: SecretRef,
}

impl SchwabApplicationCredentialReplacement {
    /// Binds a strictly newer credential generation to the exact current reference.
    pub fn try_new(
        expected: SecretRef,
        replacement: SecretRef,
    ) -> Result<Self, SchwabOAuthAuthorityError> {
        if expected.backend() != replacement.backend()
            || replacement.generation() <= expected.generation()
        {
            return Err(SchwabOAuthAuthorityError::InvalidConfiguration);
        }
        Ok(Self {
            expected,
            replacement,
        })
    }
}

impl fmt::Debug for SchwabApplicationCredentialReplacement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabApplicationCredentialReplacement")
            .field("expected_generation", &self.expected.generation())
            .field("replacement_generation", &self.replacement.generation())
            .finish_non_exhaustive()
    }
}

impl SchwabOAuthInteraction {
    const fn policy(self) -> SecretInteractionPolicy {
        match self {
            Self::Background => SecretInteractionPolicy::Forbid,
            Self::Foreground => SecretInteractionPolicy::AllowPlatformPrompt,
        }
    }
}

/// Complete code-selected configuration for one protected Schwab OAuth authority.
pub struct SchwabOAuthAuthorityConfiguration {
    secrets: Arc<dyn SecretStore>,
    wire: Arc<dyn SchwabOAuthWire>,
    application_credential: SecretRef,
    secret_policy: SchwabOAuthSecretPolicy,
    parse_bounds: ParseBounds,
    token_admission: AccessTokenAdmission,
    refresh_early_seconds: u64,
}

impl SchwabOAuthAuthorityConfiguration {
    pub fn try_new(
        secrets: Arc<dyn SecretStore>,
        wire: Arc<dyn SchwabOAuthWire>,
        application_credential: SecretRef,
        secret_policy: SchwabOAuthSecretPolicy,
        parse_bounds: ParseBounds,
        token_admission: AccessTokenAdmission,
        refresh_early_seconds: u64,
    ) -> Result<Self, SchwabOAuthAuthorityError> {
        if refresh_early_seconds == 0 || refresh_early_seconds >= ACCESS_TOKEN_MAX_LIFETIME_SECONDS
        {
            return Err(SchwabOAuthAuthorityError::InvalidConfiguration);
        }
        Ok(Self {
            secrets,
            wire,
            application_credential,
            secret_policy,
            parse_bounds,
            token_admission,
            refresh_early_seconds,
        })
    }
}

impl fmt::Debug for SchwabOAuthAuthorityConfiguration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabOAuthAuthorityConfiguration")
            .field("secrets", &"[PROTECTED STORE]")
            .field("wire", &"[TOKEN ENDPOINT CAPABILITY]")
            .field("application_credential", &"[REDACTED REFERENCE]")
            .field("secret_policy", &self.secret_policy)
            .field("parse_bounds", &self.parse_bounds)
            .field("token_admission", &self.token_admission)
            .field("refresh_early_seconds", &self.refresh_early_seconds)
            .finish()
    }
}

/// Bounded production token-endpoint transport controls.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchwabOAuthWireBounds {
    connect_timeout: Duration,
    read_timeout: Duration,
    total_timeout: Duration,
    max_response_bytes: NonZeroUsize,
}

impl SchwabOAuthWireBounds {
    pub fn try_new(
        connect_timeout: Duration,
        read_timeout: Duration,
        total_timeout: Duration,
        max_response_bytes: NonZeroUsize,
    ) -> Result<Self, SchwabOAuthAuthorityError> {
        if connect_timeout.is_zero()
            || read_timeout.is_zero()
            || total_timeout.is_zero()
            || connect_timeout > total_timeout
            || read_timeout > total_timeout
        {
            return Err(SchwabOAuthAuthorityError::InvalidConfiguration);
        }
        Ok(Self {
            connect_timeout,
            read_timeout,
            total_timeout,
            max_response_bytes,
        })
    }
}

/// Owned, zeroizing token exchange request delivered only to the sealed token endpoint wire.
pub struct SchwabOAuthWireRequest {
    authorization: Zeroizing<String>,
    form: Zeroizing<String>,
}

impl SchwabOAuthWireRequest {
    fn from_contract(request: &OAuthTokenHttpRequest<'_>) -> Result<Self, SchwabAdapterError> {
        if request.endpoint() != SCHWAB_TOKEN_ENDPOINT {
            return Err(SchwabAdapterError::RouteNotAllowed);
        }
        Ok(Self {
            authorization: request.basic_authorization_value()?,
            form: request.form_body()?,
        })
    }

    /// Explicitly exposes the one-use Basic authorization value to the selected token wire.
    pub fn expose_authorization(&self) -> &str {
        &self.authorization
    }

    /// Explicitly exposes the one-use form body to the selected token wire.
    pub fn expose_form(&self) -> &str {
        &self.form
    }
}

impl fmt::Debug for SchwabOAuthWireRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SchwabOAuthWireRequest([REDACTED])")
    }
}

/// Bounded, zeroizing token response. OAuth bodies never enter raw market capture.
pub struct SchwabOAuthWireResponse {
    status: u16,
    body: Zeroizing<Vec<u8>>,
}

impl SchwabOAuthWireResponse {
    /// Constructs a bounded response for an injected token wire without exposing token bytes.
    pub fn try_new(
        status: u16,
        body: Vec<u8>,
        max_response_bytes: NonZeroUsize,
    ) -> Result<Self, SchwabOAuthWireError> {
        let body = Zeroizing::new(body);
        if !(100..=599).contains(&status) || body.is_empty() {
            return Err(SchwabOAuthWireError::Protocol);
        }
        if body.len() > max_response_bytes.get() {
            return Err(SchwabOAuthWireError::BoundsExceeded);
        }
        Ok(Self { status, body })
    }

    /// Returns only the HTTP status; the protected body remains authority-internal.
    pub const fn status(&self) -> u16 {
        self.status
    }
}

impl fmt::Debug for SchwabOAuthWireResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabOAuthWireResponse")
            .field("status", &self.status)
            .field("body", &"[REDACTED]")
            .finish()
    }
}

/// Injectable exact token-endpoint boundary.
pub trait SchwabOAuthWire: fmt::Debug + Send + Sync {
    fn exchange(
        &self,
        request: SchwabOAuthWireRequest,
    ) -> Pin<
        Box<dyn Future<Output = Result<SchwabOAuthWireResponse, SchwabOAuthWireError>> + Send + '_>,
    >;
}

/// Hardened production token wire: HTTPS only, no redirect, proxy, retry, or decompression.
#[derive(Debug)]
pub struct ReqwestSchwabOAuthWire {
    client: reqwest::Client,
    bounds: SchwabOAuthWireBounds,
}

impl ReqwestSchwabOAuthWire {
    pub fn try_new(bounds: SchwabOAuthWireBounds) -> Result<Self, SchwabOAuthAuthorityError> {
        let client = reqwest::Client::builder()
            .https_only(true)
            .tls_backend_rustls()
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .retry(reqwest::retry::never())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(bounds.connect_timeout)
            .read_timeout(bounds.read_timeout)
            .timeout(bounds.total_timeout)
            .build()
            .map_err(|_| SchwabOAuthAuthorityError::InvalidConfiguration)?;
        Ok(Self { client, bounds })
    }
}

impl SchwabOAuthWire for ReqwestSchwabOAuthWire {
    fn exchange(
        &self,
        request: SchwabOAuthWireRequest,
    ) -> Pin<
        Box<dyn Future<Output = Result<SchwabOAuthWireResponse, SchwabOAuthWireError>> + Send + '_>,
    > {
        Box::pin(async move {
            let mut authorization = HeaderValue::from_str(request.expose_authorization())
                .map_err(|_| SchwabOAuthWireError::Protocol)?;
            authorization.set_sensitive(true);
            let response = self
                .client
                .post(SCHWAB_TOKEN_ENDPOINT)
                .header(ACCEPT, "application/json")
                .header(ACCEPT_ENCODING, "identity")
                .header(USER_AGENT, USER_AGENT_VALUE)
                .header(AUTHORIZATION, authorization)
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(request.expose_form().to_owned())
                .send()
                .await
                .map_err(|_| SchwabOAuthWireError::Network)?;
            if response.url().as_str() != SCHWAB_TOKEN_ENDPOINT {
                return Err(SchwabOAuthWireError::Protocol);
            }
            if response.headers().get_all(CONTENT_LENGTH).iter().count() > 1 {
                return Err(SchwabOAuthWireError::Protocol);
            }
            if response.headers().get_all(CONTENT_ENCODING).iter().count() > 1
                || response
                    .headers()
                    .get(CONTENT_ENCODING)
                    .is_some_and(|value| {
                        !value
                            .to_str()
                            .is_ok_and(|value| value.trim().eq_ignore_ascii_case("identity"))
                    })
            {
                return Err(SchwabOAuthWireError::Protocol);
            }
            if response
                .content_length()
                .is_some_and(|length| length > self.bounds.max_response_bytes.get() as u64)
            {
                return Err(SchwabOAuthWireError::BoundsExceeded);
            }
            if response.headers().get_all(CONTENT_TYPE).iter().count() != 1
                || !response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.split(';').next())
                    .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
            {
                return Err(SchwabOAuthWireError::Protocol);
            }
            let status = response.status().as_u16();
            let mut body = Zeroizing::new(Vec::new());
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| SchwabOAuthWireError::Network)?;
                let next = body
                    .len()
                    .checked_add(chunk.len())
                    .ok_or(SchwabOAuthWireError::BoundsExceeded)?;
                if next > self.bounds.max_response_bytes.get() {
                    return Err(SchwabOAuthWireError::BoundsExceeded);
                }
                body.try_reserve(chunk.len())
                    .map_err(|_| SchwabOAuthWireError::BoundsExceeded)?;
                body.extend_from_slice(&chunk);
            }
            SchwabOAuthWireResponse::try_new(
                status,
                std::mem::take(&mut *body),
                self.bounds.max_response_bytes,
            )
        })
    }
}

/// Secret-free wire failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SchwabOAuthWireError {
    #[error("Schwab OAuth token transport failed")]
    Network,
    #[error("Schwab OAuth token response violated the endpoint contract")]
    Protocol,
    #[error("Schwab OAuth token response exceeded local bounds")]
    BoundsExceeded,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TokenMetadata {
    reference: SecretRef,
    plan: SecretMutationPlan,
    generation: u64,
    access_issued_at_unix_seconds: u64,
    access_expires_at_unix_seconds: u64,
    refresh_authorized_at_unix_seconds: u64,
    refresh_expires_at_unix_seconds: u64,
}

impl TokenMetadata {
    fn validate(&self) -> Result<(), SchwabOAuthAuthorityError> {
        if self.generation == 0
            || self.reference.generation().get() != self.generation
            || self.plan.target() != &self.reference
            || self.access_expires_at_unix_seconds <= self.access_issued_at_unix_seconds
            || self.access_expires_at_unix_seconds
                > self
                    .access_issued_at_unix_seconds
                    .checked_add(ACCESS_TOKEN_MAX_LIFETIME_SECONDS)
                    .ok_or(SchwabOAuthAuthorityError::InvalidState)?
            || self.refresh_expires_at_unix_seconds
                != self
                    .refresh_authorized_at_unix_seconds
                    .checked_add(REFRESH_TOKEN_LIFETIME_SECONDS)
                    .ok_or(SchwabOAuthAuthorityError::InvalidState)?
            || self.access_issued_at_unix_seconds < self.refresh_authorized_at_unix_seconds
            || self.access_issued_at_unix_seconds >= self.refresh_expires_at_unix_seconds
        {
            return Err(SchwabOAuthAuthorityError::InvalidState);
        }
        Ok(())
    }

    fn decision(
        &self,
        now_unix_seconds: u64,
        refresh_early_seconds: u64,
    ) -> Result<TokenDecision, SchwabOAuthAuthorityError> {
        self.validate()?;
        if refresh_early_seconds >= ACCESS_TOKEN_MAX_LIFETIME_SECONDS
            || now_unix_seconds < self.access_issued_at_unix_seconds
        {
            return Err(SchwabOAuthAuthorityError::InvalidState);
        }
        if now_unix_seconds >= self.refresh_expires_at_unix_seconds {
            return Ok(TokenDecision::Reauthorize);
        }
        if now_unix_seconds
            >= self
                .access_expires_at_unix_seconds
                .saturating_sub(refresh_early_seconds)
        {
            Ok(TokenDecision::Refresh)
        } else {
            Ok(TokenDecision::Fresh)
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RotationKind {
    Authorization,
    Refresh,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "phase", rename_all = "snake_case")]
enum DurablePhase {
    AwaitingAuthorization {
        application: SecretRef,
        last_generation: u64,
    },
    ExchangingAuthorization {
        application: SecretRef,
        last_generation: u64,
    },
    Refreshing {
        application: SecretRef,
        prior: TokenMetadata,
    },
    Rotating {
        application: SecretRef,
        kind: RotationKind,
        prior: Option<TokenMetadata>,
        plan: SecretMutationPlan,
        candidate: TokenMetadata,
    },
    Active {
        application: SecretRef,
        current: TokenMetadata,
        retired: Option<TokenMetadata>,
    },
    ReplacingApplication {
        application: SecretRef,
        replacement: SecretRef,
        tokens: Vec<TokenMetadata>,
        last_generation: u64,
    },
    Revoking {
        application: SecretRef,
        tokens: Vec<TokenMetadata>,
        last_generation: u64,
    },
    Revoked {
        application: SecretRef,
        last_generation: u64,
    },
}

impl DurablePhase {
    fn application(&self) -> &SecretRef {
        match self {
            Self::AwaitingAuthorization { application, .. }
            | Self::ExchangingAuthorization { application, .. }
            | Self::Refreshing { application, .. }
            | Self::Rotating { application, .. }
            | Self::Active { application, .. }
            | Self::ReplacingApplication { application, .. }
            | Self::Revoking { application, .. }
            | Self::Revoked { application, .. } => application,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableEnvelope {
    version: u16,
    state: DurablePhase,
}

/// Secret-free current authority disposition for setup/doctor/currentness surfaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabOAuthAuthorityStatus {
    AwaitingAuthorization,
    Active(SchwabOAuthAuthorityReceipt),
    ReauthorizationRequired,
}

/// Opaque, non-secret token-currentness evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchwabOAuthAuthorityReceipt {
    generation: AccessTokenGeneration,
    access_issued_at_unix_seconds: u64,
    access_expires_at_unix_seconds: u64,
    refresh_authorized_at_unix_seconds: u64,
    refresh_expires_at_unix_seconds: u64,
}

impl SchwabOAuthAuthorityReceipt {
    pub const fn generation(self) -> AccessTokenGeneration {
        self.generation
    }
    pub const fn access_expires_at_unix_seconds(self) -> u64 {
        self.access_expires_at_unix_seconds
    }
    pub const fn refresh_expires_at_unix_seconds(self) -> u64 {
        self.refresh_expires_at_unix_seconds
    }
    pub const fn access_issued_at_unix_seconds(self) -> u64 {
        self.access_issued_at_unix_seconds
    }
    pub const fn refresh_authorized_at_unix_seconds(self) -> u64 {
        self.refresh_authorized_at_unix_seconds
    }
}

impl TryFrom<&TokenMetadata> for SchwabOAuthAuthorityReceipt {
    type Error = SchwabOAuthAuthorityError;

    fn try_from(value: &TokenMetadata) -> Result<Self, Self::Error> {
        value.validate()?;
        Ok(Self {
            generation: AccessTokenGeneration::new(
                NonZeroU64::new(value.generation).ok_or(Self::Error::InvalidState)?,
            ),
            access_issued_at_unix_seconds: value.access_issued_at_unix_seconds,
            access_expires_at_unix_seconds: value.access_expires_at_unix_seconds,
            refresh_authorized_at_unix_seconds: value.refresh_authorized_at_unix_seconds,
            refresh_expires_at_unix_seconds: value.refresh_expires_at_unix_seconds,
        })
    }
}

/// Sole protected OAuth continuation/token owner for one selected Schwab profile.
pub struct ProtectedSchwabOAuthAuthority {
    state: Arc<LocalAuthorityStateStore>,
    secrets: Arc<dyn SecretStore>,
    wire: Arc<dyn SchwabOAuthWire>,
    application_credential: SecretRef,
    token_key: market_squawk_platform::SecretKey,
    secret_policy: SchwabOAuthSecretPolicy,
    parse_bounds: ParseBounds,
    token_admission: AccessTokenAdmission,
    refresh_early_seconds: u64,
    gate: Mutex<()>,
}

impl fmt::Debug for ProtectedSchwabOAuthAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProtectedSchwabOAuthAuthority")
            .field("state", &"[LOCAL AUTHORITY]")
            .field("secrets", &"[PROTECTED STORE]")
            .field("wire", &"[TOKEN ENDPOINT CAPABILITY]")
            .field("application_credential", &"[REDACTED REFERENCE]")
            .field("token_key", &"[REDACTED]")
            .field("refresh_early_seconds", &self.refresh_early_seconds)
            .finish_non_exhaustive()
    }
}

impl ProtectedSchwabOAuthAuthority {
    /// Opens the crash-safe authority state beneath an application-confined profile root.
    pub async fn try_open(
        state_root: impl AsRef<Path>,
        configuration: SchwabOAuthAuthorityConfiguration,
    ) -> Result<Self, SchwabOAuthAuthorityError> {
        let SchwabOAuthAuthorityConfiguration {
            secrets,
            wire,
            application_credential,
            secret_policy,
            parse_bounds,
            token_admission,
            refresh_early_seconds,
        } = configuration;
        let root = state_root.as_ref().to_path_buf();
        let state = tokio::task::spawn_blocking(move || LocalAuthorityStateStore::try_open(root))
            .await
            .map_err(|_| SchwabOAuthAuthorityError::WorkerUnavailable)??;
        let authority = Self {
            state: Arc::new(state),
            secrets,
            wire,
            application_credential: application_credential.clone(),
            token_key: market_squawk_platform::SecretKey::try_new(
                TOKEN_SECRET_SCOPE,
                TOKEN_SECRET_NAME,
            )?,
            secret_policy,
            parse_bounds,
            token_admission,
            refresh_early_seconds,
            gate: Mutex::new(()),
        };
        let _guard = authority.gate.lock().await;
        let initial = authority.load_state_unbound().await?;
        match initial {
            None => {
                authority
                    .store_state(DurablePhase::AwaitingAuthorization {
                        application: application_credential,
                        last_generation: 0,
                    })
                    .await?;
            }
            Some(state) => {
                let configured_credential_matches = match &state {
                    DurablePhase::ReplacingApplication { replacement, .. } => {
                        replacement == &application_credential
                    }
                    _ => state.application() == &application_credential,
                };
                if !configured_credential_matches {
                    return Err(SchwabOAuthAuthorityError::ApplicationCredentialMismatch);
                }
                authority.recover_state(state).await?;
            }
        }
        drop(_guard);
        Ok(authority)
    }

    /// Builds one state-bound browser authorization request without exposing application secrets.
    ///
    /// The exact protected application credential is read only for this foreground operation and
    /// is parsed into a zeroizing envelope. Only the redacted authorization request leaves the
    /// authority boundary; callers never receive the Schwab application key or secret.
    pub async fn authorization_request(
        &self,
        state: &str,
        admission: RequestAdmission,
        interaction: SchwabOAuthInteraction,
    ) -> Result<AuthorizationRequest, SchwabOAuthAuthorityError> {
        let _guard = self.gate.lock().await;
        let durable = self.reconcile_transient_state_locked().await?;
        if !matches!(
            durable,
            DurablePhase::AwaitingAuthorization { .. } | DurablePhase::Revoked { .. }
        ) {
            return Err(SchwabOAuthAuthorityError::InvalidState);
        }
        let secret = self
            .read_secret(self.application_credential.clone(), interaction.policy())
            .await?;
        let credential = SchwabApplicationCredentialEnvelope::try_parse(secret.expose_secret())?;
        AuthorizationRequest::try_new(credential.expose_app_key(), state, admission)
            .map_err(Into::into)
    }

    /// Returns secret-free lifecycle state after exact durable-state validation.
    pub async fn status(&self) -> Result<SchwabOAuthAuthorityStatus, SchwabOAuthAuthorityError> {
        let _guard = self.gate.lock().await;
        let state = self.reconcile_transient_state_locked().await?;
        match state {
            DurablePhase::AwaitingAuthorization { .. }
            | DurablePhase::ExchangingAuthorization { .. } => {
                Ok(SchwabOAuthAuthorityStatus::AwaitingAuthorization)
            }
            DurablePhase::Active { current, .. } => Ok(SchwabOAuthAuthorityStatus::Active(
                SchwabOAuthAuthorityReceipt::try_from(&current)?,
            )),
            DurablePhase::Refreshing { .. }
            | DurablePhase::Rotating { .. }
            | DurablePhase::ReplacingApplication { .. }
            | DurablePhase::Revoking { .. }
            | DurablePhase::Revoked { .. } => {
                Ok(SchwabOAuthAuthorityStatus::ReauthorizationRequired)
            }
        }
    }

    /// Consumes this authority and replaces its exact protected application credential.
    ///
    /// Existing OAuth tokens are deleted because they were minted for the prior application.
    /// The returned authority requires a fresh owner authorization. If the process stops after
    /// the durable replacement phase begins, reopening with the replacement credential completes
    /// the local token cleanup before becoming usable.
    pub async fn replace_application_credential(
        mut self,
        replacement: SchwabApplicationCredentialReplacement,
        interaction: SchwabOAuthInteraction,
    ) -> Result<Self, SchwabOAuthAuthorityError> {
        if replacement.expected != self.application_credential
            || replacement.replacement == self.application_credential
        {
            return Err(SchwabOAuthAuthorityError::ApplicationCredentialMismatch);
        }
        let candidate_secret = self
            .read_secret(replacement.replacement.clone(), interaction.policy())
            .await?;
        let _candidate =
            SchwabApplicationCredentialEnvelope::try_parse(candidate_secret.expose_secret())?;

        let guard = self.gate.lock().await;
        let state = self.reconcile_transient_state_locked().await?;
        let (tokens, last_generation) = match state {
            DurablePhase::Active {
                current, retired, ..
            } => {
                let last_generation = current.generation;
                let mut tokens = vec![current];
                if let Some(retired) = retired {
                    tokens.push(retired);
                }
                (tokens, last_generation)
            }
            DurablePhase::AwaitingAuthorization {
                last_generation, ..
            }
            | DurablePhase::Revoked {
                last_generation, ..
            } => (Vec::new(), last_generation),
            DurablePhase::ExchangingAuthorization { .. }
            | DurablePhase::Refreshing { .. }
            | DurablePhase::Rotating { .. }
            | DurablePhase::ReplacingApplication { .. }
            | DurablePhase::Revoking { .. } => {
                return Err(SchwabOAuthAuthorityError::InvalidState);
            }
        };
        self.store_state(DurablePhase::ReplacingApplication {
            application: replacement.expected,
            replacement: replacement.replacement.clone(),
            tokens: tokens.clone(),
            last_generation,
        })
        .await?;
        for token in tokens {
            self.delete_token(token, interaction.policy()).await?;
        }
        self.store_state_unbound(DurablePhase::AwaitingAuthorization {
            application: replacement.replacement.clone(),
            last_generation,
        })
        .await?;
        drop(guard);
        self.application_credential = replacement.replacement;
        Ok(self)
    }

    /// Exchanges one validated callback and transactionally publishes the protected token pair.
    pub async fn complete_authorization(
        &self,
        callback: &OAuthCallback,
        issued_at_unix_seconds: u64,
        interaction: SchwabOAuthInteraction,
    ) -> Result<SchwabOAuthAuthorityReceipt, SchwabOAuthAuthorityError> {
        let _guard = self.gate.lock().await;
        let state = self.reconcile_transient_state_locked().await?;
        let (application, last_generation) = match state {
            DurablePhase::AwaitingAuthorization {
                application,
                last_generation,
            }
            | DurablePhase::Revoked {
                application,
                last_generation,
            } => (application, last_generation),
            _ => return Err(SchwabOAuthAuthorityError::InvalidState),
        };
        self.store_state(DurablePhase::ExchangingAuthorization {
            application: application.clone(),
            last_generation,
        })
        .await?;
        let generation = last_generation
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(SchwabOAuthAuthorityError::GenerationExhausted)?;
        let response = self
            .exchange(
                &application,
                TokenGrant::AuthorizationCode(callback),
                interaction,
            )
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.store_state(DurablePhase::AwaitingAuthorization {
                    application,
                    last_generation,
                })
                .await?;
                return Err(error);
            }
        };
        let refresh = RefreshTokenGeneration::try_new(generation, issued_at_unix_seconds)?;
        let (tokens, lifecycle) = parse_token_response(
            &response.body,
            issued_at_unix_seconds,
            refresh,
            self.parse_bounds,
        )?;
        let refresh_token = tokens
            .expose_refresh_token()
            .ok_or(SchwabOAuthAuthorityError::MissingRefreshToken)?;
        let secret = token_secret(tokens.expose_access_token(), refresh_token)?;
        let secret_generation = SecretGeneration::new(generation.get())?;
        let plan = self
            .plan_create(secret_generation, interaction.policy())
            .await?;
        let candidate = token_metadata(plan.clone(), lifecycle);
        self.store_state(DurablePhase::Rotating {
            application: application.clone(),
            kind: RotationKind::Authorization,
            prior: None,
            plan: plan.clone(),
            candidate: candidate.clone(),
        })
        .await?;
        self.execute_plan(plan, secret, interaction.policy())
            .await?;
        self.store_state(DurablePhase::Active {
            application,
            current: candidate.clone(),
            retired: None,
        })
        .await?;
        SchwabOAuthAuthorityReceipt::try_from(&candidate)
    }

    /// Deletes all locally protected token generations and leaves an explicit unavailable state.
    ///
    /// Schwab does not expose an admitted market-data revocation route here; this method never
    /// invents one. Unlink is a local authority revocation and forces fresh owner consent.
    pub async fn revoke(
        &self,
        interaction: SchwabOAuthInteraction,
    ) -> Result<(), SchwabOAuthAuthorityError> {
        let _guard = self.gate.lock().await;
        let state = self.reconcile_transient_state_locked().await?;
        let (application, tokens, last_generation) = match state {
            DurablePhase::Active {
                application,
                current,
                retired,
            } => {
                let mut tokens = vec![current.clone()];
                if let Some(retired) = retired {
                    tokens.push(retired);
                }
                (application, tokens, current.generation)
            }
            DurablePhase::AwaitingAuthorization {
                application,
                last_generation,
            }
            | DurablePhase::Revoked {
                application,
                last_generation,
            } => (application, Vec::new(), last_generation),
            _ => return Err(SchwabOAuthAuthorityError::InvalidState),
        };
        self.store_state(DurablePhase::Revoking {
            application: application.clone(),
            tokens: tokens.clone(),
            last_generation,
        })
        .await?;
        for token in tokens {
            self.delete_token(token, interaction.policy()).await?;
        }
        self.store_state(DurablePhase::Revoked {
            application,
            last_generation,
        })
        .await
    }

    async fn acquire_token(&self) -> Result<TransientAccessToken, SchwabOAuthAuthorityError> {
        let _guard = self.gate.lock().await;
        let now = unix_seconds()?;
        let mut state = self.reconcile_transient_state_locked().await?;
        if let DurablePhase::Active { current, .. } = &state
            && current.decision(now, self.refresh_early_seconds)? == TokenDecision::Refresh
        {
            self.refresh_locked(now).await?;
            state = self.required_state().await?;
        }
        let (application, current) = match state {
            DurablePhase::Active {
                application,
                current,
                ..
            } => (application, current),
            DurablePhase::AwaitingAuthorization { .. }
            | DurablePhase::Revoked { .. }
            | DurablePhase::ExchangingAuthorization { .. }
            | DurablePhase::Refreshing { .. }
            | DurablePhase::Rotating { .. }
            | DurablePhase::ReplacingApplication { .. }
            | DurablePhase::Revoking { .. } => {
                return Err(SchwabOAuthAuthorityError::ReauthorizationRequired);
            }
        };
        if current.decision(now, self.refresh_early_seconds)? == TokenDecision::Reauthorize {
            return Err(SchwabOAuthAuthorityError::ReauthorizationRequired);
        }
        let secret = match self
            .read_token(&current, SecretInteractionPolicy::Forbid)
            .await
        {
            Ok(secret) => secret,
            Err(error) if token_is_absent(&error) => {
                self.force_reauthorization(application, current).await?;
                return Err(SchwabOAuthAuthorityError::ReauthorizationRequired);
            }
            Err(error) => return Err(error),
        };
        let bundle = match ProtectedTokenSecret::try_parse(secret.expose_secret()) {
            Ok(bundle) => bundle,
            Err(_) => {
                self.force_reauthorization(application, current).await?;
                return Err(SchwabOAuthAuthorityError::ReauthorizationRequired);
            }
        };
        TransientAccessToken::try_new(
            bundle.access.to_string(),
            AccessTokenGeneration::new(
                NonZeroU64::new(current.generation)
                    .ok_or(SchwabOAuthAuthorityError::InvalidState)?,
            ),
            current.access_issued_at_unix_seconds,
            current.access_expires_at_unix_seconds,
            self.token_admission,
        )
        .map_err(Into::into)
    }

    async fn refresh_locked(
        &self,
        issued_at_unix_seconds: u64,
    ) -> Result<(), SchwabOAuthAuthorityError> {
        let state = self.required_state().await?;
        let (application, prior) = match state {
            DurablePhase::Active {
                application,
                current,
                retired: None,
            } => (application, current),
            _ => return Err(SchwabOAuthAuthorityError::InvalidState),
        };
        let prior_secret = self
            .read_token(&prior, SecretInteractionPolicy::Forbid)
            .await?;
        let prior_bundle = ProtectedTokenSecret::try_parse(prior_secret.expose_secret())?;
        let request = self
            .prepare_exchange(
                &application,
                TokenGrant::RefreshToken(&prior_bundle.refresh),
                SchwabOAuthInteraction::Background,
            )
            .await?;
        self.store_state(DurablePhase::Refreshing {
            application: application.clone(),
            prior: prior.clone(),
        })
        .await?;
        let response = self.send_exchange(request).await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                if terminal_credential_rejection(&error) {
                    self.force_reauthorization(application, prior).await?;
                } else {
                    self.restore_active_prior(application, prior).await?;
                }
                return Err(error);
            }
        };
        let generation = prior
            .generation
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(SchwabOAuthAuthorityError::GenerationExhausted)?;
        let refresh =
            RefreshTokenGeneration::try_new(generation, prior.refresh_authorized_at_unix_seconds)?;
        let (tokens, lifecycle) = match parse_token_response(
            &response.body,
            issued_at_unix_seconds,
            refresh,
            self.parse_bounds,
        ) {
            Ok(value) => value,
            Err(error) => {
                self.restore_active_prior(application, prior).await?;
                return Err(error.into());
            }
        };
        let refresh_token = tokens
            .expose_refresh_token()
            .unwrap_or(prior_bundle.refresh.as_str());
        let secret = token_secret(tokens.expose_access_token(), refresh_token)?;
        let plan = self
            .plan_replace(
                prior.reference.clone(),
                SecretGeneration::new(generation.get())?,
                SecretInteractionPolicy::Forbid,
            )
            .await?;
        let candidate = token_metadata(plan.clone(), lifecycle);
        self.store_state(DurablePhase::Rotating {
            application: application.clone(),
            kind: RotationKind::Refresh,
            prior: Some(prior.clone()),
            plan: plan.clone(),
            candidate: candidate.clone(),
        })
        .await?;
        self.execute_plan(plan, secret, SecretInteractionPolicy::Forbid)
            .await?;
        self.store_state(DurablePhase::Active {
            application: application.clone(),
            current: candidate.clone(),
            retired: Some(prior.clone()),
        })
        .await?;
        self.delete_token(prior, SecretInteractionPolicy::Forbid)
            .await?;
        self.store_state(DurablePhase::Active {
            application,
            current: candidate,
            retired: None,
        })
        .await
    }

    async fn exchange(
        &self,
        application: &SecretRef,
        grant: TokenGrant<'_>,
        interaction: SchwabOAuthInteraction,
    ) -> Result<SchwabOAuthWireResponse, SchwabOAuthAuthorityError> {
        let request = self
            .prepare_exchange(application, grant, interaction)
            .await?;
        self.send_exchange(request).await
    }

    async fn prepare_exchange(
        &self,
        application: &SecretRef,
        grant: TokenGrant<'_>,
        interaction: SchwabOAuthInteraction,
    ) -> Result<SchwabOAuthWireRequest, SchwabOAuthAuthorityError> {
        let secret = self
            .read_secret(application.clone(), interaction.policy())
            .await?;
        let application = SchwabApplicationCredentialEnvelope::try_parse(secret.expose_secret())?;
        let contract = OAuthTokenHttpRequest::try_new(
            application.expose_app_key(),
            application.expose_app_secret(),
            grant,
            crate::RequestAdmission::new(
                NonZeroUsize::new(32 * 1024)
                    .ok_or(SchwabOAuthAuthorityError::InvalidConfiguration)?,
                NonZeroUsize::MIN,
            ),
        )?;
        SchwabOAuthWireRequest::from_contract(&contract).map_err(Into::into)
    }

    async fn send_exchange(
        &self,
        request: SchwabOAuthWireRequest,
    ) -> Result<SchwabOAuthWireResponse, SchwabOAuthAuthorityError> {
        let response = self.wire.exchange(request).await?;
        if response.status != 200 {
            return if matches!(response.status, 400 | 401 | 403) {
                Err(SchwabOAuthAuthorityError::ReauthorizationRequired)
            } else {
                Err(SchwabOAuthAuthorityError::ProviderRejected)
            };
        }
        Ok(response)
    }

    async fn recover_state(&self, state: DurablePhase) -> Result<(), SchwabOAuthAuthorityError> {
        match state {
            DurablePhase::AwaitingAuthorization { .. } | DurablePhase::Revoked { .. } => Ok(()),
            DurablePhase::ExchangingAuthorization {
                application,
                last_generation,
            } => {
                self.store_state(DurablePhase::AwaitingAuthorization {
                    application,
                    last_generation,
                })
                .await
            }
            DurablePhase::Refreshing { application, prior } => {
                self.restore_active_prior(application, prior).await
            }
            DurablePhase::Rotating {
                application,
                kind,
                prior,
                plan,
                candidate,
            } => {
                match self
                    .inspect_plan(plan.clone(), SecretInteractionPolicy::Forbid)
                    .await?
                {
                    SecretReconciliationObservation::Absent => {}
                    SecretReconciliationObservation::PresentUnverified
                    | SecretReconciliationObservation::Matches
                    | SecretReconciliationObservation::Mismatch => {
                        self.delete_plan(plan, SecretInteractionPolicy::Forbid)
                            .await?;
                    }
                }
                match (kind, prior) {
                    (RotationKind::Authorization, None) => {
                        self.store_state(DurablePhase::AwaitingAuthorization {
                            application,
                            last_generation: candidate.generation.saturating_sub(1),
                        })
                        .await
                    }
                    (RotationKind::Refresh, Some(prior)) => {
                        self.restore_active_prior(application, prior).await
                    }
                    _ => Err(SchwabOAuthAuthorityError::InvalidState),
                }
            }
            DurablePhase::Active {
                application,
                current,
                retired,
            } => {
                current.validate()?;
                if let Some(retired) = retired {
                    self.delete_token(retired, SecretInteractionPolicy::Forbid)
                        .await?;
                    self.store_state(DurablePhase::Active {
                        application: application.clone(),
                        current: current.clone(),
                        retired: None,
                    })
                    .await?;
                }
                match self
                    .read_token(&current, SecretInteractionPolicy::Forbid)
                    .await
                {
                    Ok(secret) => match ProtectedTokenSecret::try_parse(secret.expose_secret()) {
                        Ok(_) => Ok(()),
                        Err(_) => self.force_reauthorization(application, current).await,
                    },
                    Err(error) if token_is_absent(&error) => {
                        self.force_reauthorization(application, current).await
                    }
                    Err(error) => Err(error),
                }
            }
            DurablePhase::ReplacingApplication {
                replacement,
                tokens,
                last_generation,
                ..
            } => {
                for token in tokens {
                    self.delete_token(token, SecretInteractionPolicy::Forbid)
                        .await?;
                }
                self.store_state_unbound(DurablePhase::AwaitingAuthorization {
                    application: replacement,
                    last_generation,
                })
                .await
            }
            DurablePhase::Revoking {
                application,
                tokens,
                last_generation,
            } => {
                for token in tokens {
                    self.delete_token(token, SecretInteractionPolicy::Forbid)
                        .await?;
                }
                self.store_state(DurablePhase::Revoked {
                    application,
                    last_generation,
                })
                .await
            }
        }
    }

    async fn force_reauthorization(
        &self,
        application: SecretRef,
        prior: TokenMetadata,
    ) -> Result<(), SchwabOAuthAuthorityError> {
        self.store_state(DurablePhase::Revoking {
            application: application.clone(),
            tokens: vec![prior.clone()],
            last_generation: prior.generation,
        })
        .await?;
        self.delete_token(prior.clone(), SecretInteractionPolicy::Forbid)
            .await?;
        self.store_state(DurablePhase::Revoked {
            application,
            last_generation: prior.generation,
        })
        .await
    }

    async fn restore_active_prior(
        &self,
        application: SecretRef,
        prior: TokenMetadata,
    ) -> Result<(), SchwabOAuthAuthorityError> {
        prior.validate()?;
        self.store_state(DurablePhase::Active {
            application,
            current: prior,
            retired: None,
        })
        .await
    }

    async fn required_state(&self) -> Result<DurablePhase, SchwabOAuthAuthorityError> {
        self.load_state()
            .await?
            .ok_or(SchwabOAuthAuthorityError::InvalidState)
    }

    async fn reconcile_transient_state_locked(
        &self,
    ) -> Result<DurablePhase, SchwabOAuthAuthorityError> {
        let state = self.required_state().await?;
        let requires_recovery = match &state {
            DurablePhase::AwaitingAuthorization { .. } | DurablePhase::Revoked { .. } => false,
            DurablePhase::Active { retired, .. } => retired.is_some(),
            DurablePhase::ExchangingAuthorization { .. }
            | DurablePhase::Refreshing { .. }
            | DurablePhase::Rotating { .. }
            | DurablePhase::ReplacingApplication { .. }
            | DurablePhase::Revoking { .. } => true,
        };
        if !requires_recovery {
            return Ok(state);
        }
        self.recover_state(state).await?;
        self.required_state().await
    }

    async fn load_state(&self) -> Result<Option<DurablePhase>, SchwabOAuthAuthorityError> {
        let state = self.load_state_unbound().await?;
        if state
            .as_ref()
            .is_some_and(|state| state.application() != &self.application_credential)
        {
            return Err(SchwabOAuthAuthorityError::ApplicationCredentialMismatch);
        }
        Ok(state)
    }

    async fn load_state_unbound(&self) -> Result<Option<DurablePhase>, SchwabOAuthAuthorityError> {
        let state = self.state.clone();
        let bytes = tokio::task::spawn_blocking(move || state.load())
            .await
            .map_err(|_| SchwabOAuthAuthorityError::WorkerUnavailable)??;
        bytes
            .map(|bytes| {
                let envelope: DurableEnvelope = serde_json::from_slice(&bytes)
                    .map_err(|_| SchwabOAuthAuthorityError::InvalidState)?;
                if envelope.version != AUTHORITY_STATE_VERSION {
                    return Err(SchwabOAuthAuthorityError::InvalidState);
                }
                validate_phase(&envelope.state)?;
                Ok(envelope.state)
            })
            .transpose()
    }

    async fn store_state(&self, state: DurablePhase) -> Result<(), SchwabOAuthAuthorityError> {
        validate_phase(&state)?;
        if state.application() != &self.application_credential {
            return Err(SchwabOAuthAuthorityError::ApplicationCredentialMismatch);
        }
        self.store_state_unbound(state).await
    }

    async fn store_state_unbound(
        &self,
        state: DurablePhase,
    ) -> Result<(), SchwabOAuthAuthorityError> {
        validate_phase(&state)?;
        let bytes = serde_json::to_vec(&DurableEnvelope {
            version: AUTHORITY_STATE_VERSION,
            state,
        })
        .map_err(|_| SchwabOAuthAuthorityError::InvalidState)?;
        let store = self.state.clone();
        tokio::task::spawn_blocking(move || store.store(&bytes))
            .await
            .map_err(|_| SchwabOAuthAuthorityError::WorkerUnavailable)??;
        Ok(())
    }

    async fn read_secret(
        &self,
        reference: SecretRef,
        interaction: SecretInteractionPolicy,
    ) -> Result<SecretValue, SchwabOAuthAuthorityError> {
        let store = self.secrets.clone();
        let policy = self.secret_policy;
        spawn_secret(move || {
            let control = secret_control(policy, interaction)?;
            store.read(&reference, &control)
        })
        .await
    }

    async fn read_token(
        &self,
        token: &TokenMetadata,
        interaction: SecretInteractionPolicy,
    ) -> Result<SecretValue, SchwabOAuthAuthorityError> {
        token.validate()?;
        let _ = self.inspect_plan(token.plan.clone(), interaction).await?;
        self.read_secret(token.reference.clone(), interaction).await
    }

    async fn plan_create(
        &self,
        generation: SecretGeneration,
        interaction: SecretInteractionPolicy,
    ) -> Result<SecretMutationPlan, SchwabOAuthAuthorityError> {
        let store = self.secrets.clone();
        let key = self.token_key.clone();
        let policy = self.secret_policy;
        spawn_secret(move || {
            let control = secret_control(policy, interaction)?;
            store.plan_create(&key, generation, &control)
        })
        .await
    }

    async fn plan_replace(
        &self,
        current: SecretRef,
        generation: SecretGeneration,
        interaction: SecretInteractionPolicy,
    ) -> Result<SecretMutationPlan, SchwabOAuthAuthorityError> {
        let store = self.secrets.clone();
        let key = self.token_key.clone();
        let policy = self.secret_policy;
        spawn_secret(move || {
            let control = secret_control(policy, interaction)?;
            store.plan_replace(&key, &current, generation, &control)
        })
        .await
    }

    async fn execute_plan(
        &self,
        plan: SecretMutationPlan,
        secret: SecretValue,
        interaction: SecretInteractionPolicy,
    ) -> Result<(), SchwabOAuthAuthorityError> {
        let store = self.secrets.clone();
        let key = self.token_key.clone();
        let policy = self.secret_policy;
        tokio::task::spawn_blocking(move || {
            let control = secret_control(policy, interaction)?;
            store
                .execute_planned(&key, &plan, secret, &control)
                .map(|_| ())
                .map_err(|failure| SchwabOAuthAuthorityError::Secret(failure.into_error()))
        })
        .await
        .map_err(|_| SchwabOAuthAuthorityError::WorkerUnavailable)??;
        Ok(())
    }

    async fn inspect_plan(
        &self,
        plan: SecretMutationPlan,
        interaction: SecretInteractionPolicy,
    ) -> Result<SecretReconciliationObservation, SchwabOAuthAuthorityError> {
        let store = self.secrets.clone();
        let key = self.token_key.clone();
        let policy = self.secret_policy;
        spawn_secret(move || {
            let control = secret_control(policy, interaction)?;
            store.inspect_planned(&key, &plan, &control)
        })
        .await
    }

    async fn delete_plan(
        &self,
        plan: SecretMutationPlan,
        interaction: SecretInteractionPolicy,
    ) -> Result<(), SchwabOAuthAuthorityError> {
        let store = self.secrets.clone();
        let key = self.token_key.clone();
        let policy = self.secret_policy;
        tokio::task::spawn_blocking(move || {
            let control = secret_control(policy, interaction)?;
            match store.delete_planned(&key, &plan, &control) {
                Ok(
                    SecretDeletionDisposition::Deleted | SecretDeletionDisposition::AlreadyAbsent,
                ) => Ok(()),
                Err(failure) => Err(SchwabOAuthAuthorityError::Secret(failure.into_error())),
            }
        })
        .await
        .map_err(|_| SchwabOAuthAuthorityError::WorkerUnavailable)??;
        Ok(())
    }

    async fn delete_reference(
        &self,
        reference: SecretRef,
        interaction: SecretInteractionPolicy,
    ) -> Result<(), SchwabOAuthAuthorityError> {
        let store = self.secrets.clone();
        let policy = self.secret_policy;
        tokio::task::spawn_blocking(move || {
            let control = secret_control(policy, interaction)?;
            match store.delete(&reference, &control) {
                Ok(()) | Err(LocalSecretStoreError::NotFound) => Ok(()),
                Err(error) => Err(SchwabOAuthAuthorityError::Secret(error)),
            }
        })
        .await
        .map_err(|_| SchwabOAuthAuthorityError::WorkerUnavailable)??;
        Ok(())
    }

    async fn delete_token(
        &self,
        token: TokenMetadata,
        interaction: SecretInteractionPolicy,
    ) -> Result<(), SchwabOAuthAuthorityError> {
        token.validate()?;
        let _ = self.inspect_plan(token.plan.clone(), interaction).await?;
        self.delete_reference(token.reference, interaction).await
    }
}

impl SchwabAccessTokenSource for ProtectedSchwabOAuthAuthority {
    fn acquire(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<TransientAccessToken, TokenAuthorityError>> + Send + '_>>
    {
        Box::pin(async move {
            self.acquire_token().await.map_err(|error| match error {
                SchwabOAuthAuthorityError::ReauthorizationRequired
                | SchwabOAuthAuthorityError::MissingRefreshToken => {
                    TokenAuthorityError::ReauthorizationRequired
                }
                _ => TokenAuthorityError::Unavailable,
            })
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenSecretWire {
    version: u16,
    access_token: String,
    refresh_token: String,
}

struct ProtectedTokenSecret {
    access: Zeroizing<String>,
    refresh: Zeroizing<String>,
}

impl ProtectedTokenSecret {
    fn try_parse(value: &str) -> Result<Self, SchwabOAuthAuthorityError> {
        let mut wire: TokenSecretWire =
            serde_json::from_str(value).map_err(|_| SchwabOAuthAuthorityError::InvalidSecret)?;
        if wire.version != TOKEN_SECRET_VERSION
            || !valid_secret_value(&wire.access_token)
            || !valid_secret_value(&wire.refresh_token)
        {
            wire.access_token.zeroize();
            wire.refresh_token.zeroize();
            return Err(SchwabOAuthAuthorityError::InvalidSecret);
        }
        Ok(Self {
            access: Zeroizing::new(std::mem::take(&mut wire.access_token)),
            refresh: Zeroizing::new(std::mem::take(&mut wire.refresh_token)),
        })
    }
}

fn token_secret(access: &str, refresh: &str) -> Result<SecretValue, SchwabOAuthAuthorityError> {
    if !valid_secret_value(access) || !valid_secret_value(refresh) {
        return Err(SchwabOAuthAuthorityError::InvalidSecret);
    }
    #[derive(Serialize)]
    struct Wire<'a> {
        version: u16,
        access_token: &'a str,
        refresh_token: &'a str,
    }
    let value = serde_json::to_string(&Wire {
        version: TOKEN_SECRET_VERSION,
        access_token: access,
        refresh_token: refresh,
    })
    .map_err(|_| SchwabOAuthAuthorityError::InvalidSecret)?;
    SecretValue::new(value).map_err(|_| SchwabOAuthAuthorityError::InvalidSecret)
}

fn valid_secret_value(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 16 * 1024
        && !value.as_bytes().contains(&0)
        && !value.contains(['\r', '\n'])
}

fn token_is_absent(error: &SchwabOAuthAuthorityError) -> bool {
    matches!(
        error,
        SchwabOAuthAuthorityError::Secret(LocalSecretStoreError::NotFound)
    )
}

fn terminal_credential_rejection(error: &SchwabOAuthAuthorityError) -> bool {
    matches!(error, SchwabOAuthAuthorityError::ReauthorizationRequired)
}

fn token_metadata(plan: SecretMutationPlan, lifecycle: crate::TokenLifecycle) -> TokenMetadata {
    let refresh = lifecycle.refresh_generation();
    let reference = plan.target().clone();
    TokenMetadata {
        generation: reference.generation().get(),
        reference,
        plan,
        access_issued_at_unix_seconds: lifecycle.access_issued_at_unix_seconds(),
        access_expires_at_unix_seconds: lifecycle.access_expires_at_unix_seconds(),
        refresh_authorized_at_unix_seconds: refresh.authorized_at_unix_seconds(),
        refresh_expires_at_unix_seconds: refresh.expires_at_unix_seconds(),
    }
}

fn validate_phase(state: &DurablePhase) -> Result<(), SchwabOAuthAuthorityError> {
    match state {
        DurablePhase::AwaitingAuthorization { .. }
        | DurablePhase::ExchangingAuthorization { .. }
        | DurablePhase::Revoked { .. } => Ok(()),
        DurablePhase::Refreshing { prior, .. } => prior.validate(),
        DurablePhase::Rotating {
            kind,
            prior,
            plan,
            candidate,
            ..
        } => {
            candidate.validate()?;
            if plan != &candidate.plan || plan.target() != &candidate.reference {
                return Err(SchwabOAuthAuthorityError::InvalidState);
            }
            match (kind, prior, plan.kind()) {
                (RotationKind::Authorization, None, SecretMutationKind::Create) => {}
                (RotationKind::Refresh, Some(prior), SecretMutationKind::Replace { current }) => {
                    prior.validate()?;
                    if current != &prior.reference
                        || prior.generation.checked_add(1) != Some(candidate.generation)
                    {
                        return Err(SchwabOAuthAuthorityError::InvalidState);
                    }
                }
                _ => {
                    return Err(SchwabOAuthAuthorityError::InvalidState);
                }
            }
            Ok(())
        }
        DurablePhase::Active {
            current, retired, ..
        } => {
            current.validate()?;
            if let Some(retired) = retired {
                retired.validate()?;
                if retired.reference == current.reference
                    || retired.generation >= current.generation
                    || !matches!(
                        current.plan.kind(),
                        SecretMutationKind::Replace { current } if current == &retired.reference
                    )
                {
                    return Err(SchwabOAuthAuthorityError::InvalidState);
                }
            }
            Ok(())
        }
        DurablePhase::ReplacingApplication {
            application,
            replacement,
            tokens,
            last_generation,
        } => {
            if application == replacement
                || application.backend() != replacement.backend()
                || replacement.generation() <= application.generation()
                || tokens.len() > 2
                || tokens
                    .windows(2)
                    .any(|pair| pair[0].reference == pair[1].reference)
            {
                return Err(SchwabOAuthAuthorityError::InvalidState);
            }
            for token in tokens {
                token.validate()?;
            }
            if tokens
                .iter()
                .map(|token| token.generation)
                .max()
                .is_some_and(|generation| generation != *last_generation)
            {
                return Err(SchwabOAuthAuthorityError::InvalidState);
            }
            Ok(())
        }
        DurablePhase::Revoking {
            tokens,
            last_generation,
            ..
        } => {
            if tokens.len() > 2
                || tokens
                    .windows(2)
                    .any(|pair| pair[0].reference == pair[1].reference)
            {
                return Err(SchwabOAuthAuthorityError::InvalidState);
            }
            for token in tokens {
                token.validate()?;
            }
            if tokens
                .iter()
                .map(|token| token.generation)
                .max()
                .is_some_and(|generation| generation != *last_generation)
            {
                return Err(SchwabOAuthAuthorityError::InvalidState);
            }
            Ok(())
        }
    }
}

fn secret_control(
    policy: SchwabOAuthSecretPolicy,
    interaction: SecretInteractionPolicy,
) -> Result<SecretOperationControl, LocalSecretStoreError> {
    let deadline = Instant::now()
        .checked_add(policy.timeout)
        .ok_or(LocalSecretStoreError::InvalidOperationControl)?;
    SecretOperationControl::try_new(
        "schwab-oauth-authority",
        deadline,
        policy.retry_budget,
        interaction,
        SecretCancellation::new(),
    )
}

async fn spawn_secret<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, LocalSecretStoreError> + Send + 'static,
) -> Result<T, SchwabOAuthAuthorityError> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|_| SchwabOAuthAuthorityError::WorkerUnavailable)?
        .map_err(Into::into)
}

fn unix_seconds() -> Result<u64, SchwabOAuthAuthorityError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| SchwabOAuthAuthorityError::Clock)
}

/// Redacted protected-authority failure.
#[derive(Debug, Error)]
pub enum SchwabOAuthAuthorityError {
    #[error("Schwab OAuth authority configuration is invalid")]
    InvalidConfiguration,
    #[error("Schwab OAuth authority state is invalid")]
    InvalidState,
    #[error("Schwab OAuth authority application credential does not match durable state")]
    ApplicationCredentialMismatch,
    #[error("Schwab OAuth protected token is invalid")]
    InvalidSecret,
    #[error("Schwab OAuth response did not provide an initial refresh token")]
    MissingRefreshToken,
    #[error("Schwab owner reauthorization is required")]
    ReauthorizationRequired,
    #[error("Schwab OAuth provider rejected the token operation")]
    ProviderRejected,
    #[error("Schwab OAuth token generation is exhausted")]
    GenerationExhausted,
    #[error("Schwab OAuth clock is unavailable")]
    Clock,
    #[error("Schwab OAuth blocking worker is unavailable")]
    WorkerUnavailable,
    #[error(transparent)]
    Adapter(#[from] SchwabAdapterError),
    #[error(transparent)]
    Transport(#[from] crate::SchwabTransportError),
    #[error(transparent)]
    Wire(#[from] SchwabOAuthWireError),
    #[error(transparent)]
    Secret(#[from] LocalSecretStoreError),
    #[error(transparent)]
    State(#[from] LocalAuthorityStateStoreError),
}
