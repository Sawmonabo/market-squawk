use std::fmt;
use std::num::NonZeroU64;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::{Deserialize, Deserializer};
use subtle::ConstantTimeEq as _;
use url::Url;
use zeroize::{Zeroize, Zeroizing};

use crate::{
    HttpMethod, ParseBounds, RequestAdmission, SCHWAB_AUTHORIZE_ENDPOINT, SCHWAB_CALLBACK_URI,
    SCHWAB_TOKEN_ENDPOINT, SchwabAdapterError,
};

/// Documented maximum Schwab access-token lifetime: 30 minutes.
pub const ACCESS_TOKEN_MAX_LIFETIME_SECONDS: u64 = 30 * 60;
/// Documented Schwab refresh-token lifetime: seven days.
pub const REFRESH_TOKEN_LIFETIME_SECONDS: u64 = 7 * 24 * 60 * 60;

const MAX_OAUTH_VALUE_BYTES: usize = 4 * 1024;
const MAX_CREDENTIAL_ENVELOPE_BYTES: usize = 16 * 1024;

/// Version-one protected application-credential envelope.
///
/// Exact wire shape:
/// `{"version":1,"app_key":"...","app_secret":"..."}`.
///
/// The credential-file importer creates this one bounded JSON secret value from
/// `SCHWAB_APP_KEY` and `SCHWAB_APP_SECRET` and stores it under the existing Schwab onboarding
/// session. Callback URLs and OAuth codes/tokens do not belong in this envelope. The type
/// implements neither `Clone` nor `Serialize`; all secret bytes zeroize on drop.
pub struct SchwabApplicationCredentialEnvelope {
    app_key: Zeroizing<String>,
    app_secret: Zeroizing<String>,
}

impl SchwabApplicationCredentialEnvelope {
    /// Parses the exact closed version-one protected secret envelope.
    pub fn try_parse(envelope: &str) -> Result<Self, SchwabAdapterError> {
        if envelope.is_empty() || envelope.len() > MAX_CREDENTIAL_ENVELOPE_BYTES {
            return Err(SchwabAdapterError::InvalidInput);
        }
        let CredentialEnvelopeWire {
            version,
            app_key: SecretField(app_key),
            app_secret: SecretField(app_secret),
        } = serde_json::from_str(envelope).map_err(|_| SchwabAdapterError::InvalidInput)?;
        if version != 1 {
            return Err(SchwabAdapterError::InvalidInput);
        }
        validate_oauth_value(&app_key)?;
        validate_oauth_value(&app_secret)?;
        Ok(Self {
            app_key,
            app_secret,
        })
    }

    /// Borrows the application key for an immediate authorization or token request.
    pub fn expose_app_key(&self) -> &str {
        &self.app_key
    }

    /// Borrows the application secret for an immediate token request.
    pub fn expose_app_secret(&self) -> &str {
        &self.app_secret
    }
}

impl fmt::Debug for SchwabApplicationCredentialEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SchwabApplicationCredentialEnvelope([REDACTED])")
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialEnvelopeWire {
    version: u8,
    app_key: SecretField,
    app_secret: SecretField,
}

struct SecretField(Zeroizing<String>);

impl<'de> Deserialize<'de> for SecretField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(|value| Self(Zeroizing::new(value)))
    }
}

/// Code-owned three-legged OAuth authorization request.
pub struct AuthorizationRequest {
    url: Zeroizing<String>,
}

impl AuthorizationRequest {
    /// Builds the exact Schwab authorization endpoint, callback, and correlation state.
    ///
    /// The returned URL contains the OAuth state and should be sent only to the user's browser.
    pub fn try_new(
        client_id: &str,
        state: &str,
        admission: RequestAdmission,
    ) -> Result<Self, SchwabAdapterError> {
        validate_oauth_value(client_id)?;
        validate_oauth_value(state)?;
        let mut url = Url::parse(SCHWAB_AUTHORIZE_ENDPOINT)
            .map_err(|_| SchwabAdapterError::RouteNotAllowed)?;
        url.query_pairs_mut()
            .append_pair("client_id", client_id)
            .append_pair("redirect_uri", SCHWAB_CALLBACK_URI)
            .append_pair("state", state);
        let encoded = url.to_string();
        if encoded.len() > admission.max_request_bytes() {
            return Err(SchwabAdapterError::RequestNotAdmitted);
        }
        Ok(Self {
            url: Zeroizing::new(encoded),
        })
    }

    /// Exact HTTP method.
    pub const fn method(&self) -> HttpMethod {
        HttpMethod::Get
    }

    /// Explicitly exposes the browser URL. Callers must not log it because it contains state.
    pub fn expose_url(&self) -> &str {
        &self.url
    }
}

impl fmt::Debug for AuthorizationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationRequest")
            .field("endpoint", &SCHWAB_AUTHORIZE_ENDPOINT)
            .field("callback", &SCHWAB_CALLBACK_URI)
            .field("url", &"[REDACTED]")
            .finish()
    }
}

/// Validated callback outcome.
pub enum CallbackOutcome {
    /// The owner granted access and Schwab supplied a one-time authorization code.
    Authorized(OAuthCallback),
    /// Schwab returned a bounded OAuth denial code and optional description.
    Denied {
        /// Provider OAuth error code.
        error: Box<str>,
        /// Optional provider description. It is bounded and contains no callback code.
        description: Option<Box<str>>,
    },
}

impl fmt::Debug for CallbackOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Authorized(_) => formatter.write_str("CallbackOutcome::Authorized([REDACTED])"),
            Self::Denied { error, .. } => formatter
                .debug_struct("CallbackOutcome::Denied")
                .field("error", error)
                .finish_non_exhaustive(),
        }
    }
}

/// One validated, transient OAuth authorization callback.
pub struct OAuthCallback {
    code: Zeroizing<String>,
    session: Option<Zeroizing<String>>,
}

impl OAuthCallback {
    /// Parses an exact HTTPS loopback callback and constant-time validates its state.
    pub fn parse(
        redirected_url: &str,
        expected_state: &str,
        admission: RequestAdmission,
    ) -> Result<CallbackOutcome, SchwabAdapterError> {
        if redirected_url.len() > admission.max_request_bytes() {
            return Err(SchwabAdapterError::RequestNotAdmitted);
        }
        validate_oauth_value(expected_state)?;
        let url = Url::parse(redirected_url).map_err(|_| SchwabAdapterError::InvalidCallback)?;
        validate_callback_origin(&url)?;

        let mut code: Option<Zeroizing<String>> = None;
        let mut session: Option<Zeroizing<String>> = None;
        let mut state: Option<Zeroizing<String>> = None;
        let mut error: Option<String> = None;
        let mut description: Option<String> = None;
        for (key, value) in url.query_pairs() {
            if value.is_empty() || value.len() > MAX_OAUTH_VALUE_BYTES {
                return Err(SchwabAdapterError::InvalidCallback);
            }
            let value = value.into_owned();
            let duplicate = match key.as_ref() {
                "code" => code.replace(Zeroizing::new(value)).is_some(),
                "session" => session.replace(Zeroizing::new(value)).is_some(),
                "state" => state.replace(Zeroizing::new(value)).is_some(),
                "error" => error.replace(value).is_some(),
                "error_description" => description.replace(value).is_some(),
                _ => return Err(SchwabAdapterError::InvalidCallback),
            };
            if duplicate {
                return Err(SchwabAdapterError::InvalidCallback);
            }
        }
        let returned_state = state.ok_or(SchwabAdapterError::InvalidCallback)?;
        if !constant_time_equal(returned_state.as_bytes(), expected_state.as_bytes()) {
            return Err(SchwabAdapterError::InvalidCallback);
        }
        match (code, error, description) {
            (Some(code), None, None) => Ok(CallbackOutcome::Authorized(Self { code, session })),
            (None, Some(error), description) if session.is_none() => Ok(CallbackOutcome::Denied {
                error: error.into_boxed_str(),
                description: description.map(String::into_boxed_str),
            }),
            _ => Err(SchwabAdapterError::InvalidCallback),
        }
    }

    /// Explicitly exposes the one-time code for the immediate token exchange.
    pub fn expose_code(&self) -> &str {
        &self.code
    }

    /// Explicitly exposes Schwab's optional transient session correlation value.
    pub fn expose_session(&self) -> Option<&str> {
        self.session.as_deref().map(String::as_str)
    }
}

impl fmt::Debug for OAuthCallback {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthCallback")
            .field("code", &"[REDACTED]")
            .field("session", &self.session.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// Transient OAuth grant used to build one form-encoded token request.
pub enum TokenGrant<'a> {
    /// Exchange a one-time callback code.
    AuthorizationCode(&'a OAuthCallback),
    /// Refresh using the current protected-store token generation.
    RefreshToken(&'a str),
}

impl fmt::Debug for TokenGrant<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AuthorizationCode(_) => formatter.write_str("AuthorizationCode([REDACTED])"),
            Self::RefreshToken(_) => formatter.write_str("RefreshToken([REDACTED])"),
        }
    }
}

/// Borrowed, non-persistable OAuth token request contract.
pub struct OAuthTokenHttpRequest<'a> {
    client_id: &'a str,
    client_secret: &'a str,
    grant: TokenGrant<'a>,
    admission: RequestAdmission,
}

impl<'a> OAuthTokenHttpRequest<'a> {
    /// Validates the borrowed credentials and grant without copying them into adapter state.
    pub fn try_new(
        client_id: &'a str,
        client_secret: &'a str,
        grant: TokenGrant<'a>,
        admission: RequestAdmission,
    ) -> Result<Self, SchwabAdapterError> {
        validate_oauth_value(client_id)?;
        validate_oauth_value(client_secret)?;
        if let TokenGrant::RefreshToken(refresh_token) = &grant {
            validate_oauth_value(refresh_token)?;
        }
        let request = Self {
            client_id,
            client_secret,
            grant,
            admission,
        };
        let header = request.basic_authorization_value()?;
        let body = request.form_body()?;
        let total = header
            .len()
            .checked_add(body.len())
            .and_then(|value| value.checked_add(SCHWAB_TOKEN_ENDPOINT.len()))
            .ok_or(SchwabAdapterError::ArithmeticOverflow)?;
        if total > admission.max_request_bytes() {
            return Err(SchwabAdapterError::RequestNotAdmitted);
        }
        Ok(request)
    }

    /// Exact HTTP method.
    pub const fn method(&self) -> HttpMethod {
        HttpMethod::Post
    }

    /// Exact token endpoint.
    pub const fn endpoint(&self) -> &'static str {
        SCHWAB_TOKEN_ENDPOINT
    }

    /// Exact media type for the request body.
    pub const fn content_type(&self) -> &'static str {
        "application/x-www-form-urlencoded"
    }

    /// Builds a zeroizing HTTP Basic Authorization value for the immediate send.
    pub fn basic_authorization_value(&self) -> Result<Zeroizing<String>, SchwabAdapterError> {
        let mut joined = Zeroizing::new(Vec::new());
        joined
            .try_reserve_exact(
                self.client_id
                    .len()
                    .checked_add(self.client_secret.len())
                    .and_then(|value| value.checked_add(1))
                    .ok_or(SchwabAdapterError::ArithmeticOverflow)?,
            )
            .map_err(|_| SchwabAdapterError::RequestNotAdmitted)?;
        joined.extend_from_slice(self.client_id.as_bytes());
        joined.push(b':');
        joined.extend_from_slice(self.client_secret.as_bytes());
        let encoded = BASE64_STANDARD.encode(&*joined);
        Ok(Zeroizing::new(format!("Basic {encoded}")))
    }

    /// Builds the zeroizing form body for the immediate send.
    pub fn form_body(&self) -> Result<Zeroizing<String>, SchwabAdapterError> {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        match &self.grant {
            TokenGrant::AuthorizationCode(callback) => {
                serializer
                    .append_pair("grant_type", "authorization_code")
                    .append_pair("code", callback.expose_code())
                    .append_pair("redirect_uri", SCHWAB_CALLBACK_URI);
            }
            TokenGrant::RefreshToken(refresh_token) => {
                serializer
                    .append_pair("grant_type", "refresh_token")
                    .append_pair("refresh_token", refresh_token);
            }
        }
        let body = serializer.finish();
        if body.len() > self.admission.max_request_bytes() {
            return Err(SchwabAdapterError::RequestNotAdmitted);
        }
        Ok(Zeroizing::new(body))
    }
}

impl fmt::Debug for OAuthTokenHttpRequest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let grant = match self.grant {
            TokenGrant::AuthorizationCode(_) => "authorization_code",
            TokenGrant::RefreshToken(_) => "refresh_token",
        };
        formatter
            .debug_struct("OAuthTokenHttpRequest")
            .field("endpoint", &SCHWAB_TOKEN_ENDPOINT)
            .field("grant", &grant)
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}

/// One refresh-token authorization generation and its hard seven-day horizon.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RefreshTokenGeneration {
    generation: NonZeroU64,
    authorized_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
}

impl RefreshTokenGeneration {
    /// Constructs a generation whose expiry is exactly seven days after authorization.
    pub fn try_new(
        generation: NonZeroU64,
        authorized_at_unix_seconds: u64,
    ) -> Result<Self, SchwabAdapterError> {
        let expires_at_unix_seconds = authorized_at_unix_seconds
            .checked_add(REFRESH_TOKEN_LIFETIME_SECONDS)
            .ok_or(SchwabAdapterError::ArithmeticOverflow)?;
        Ok(Self {
            generation,
            authorized_at_unix_seconds,
            expires_at_unix_seconds,
        })
    }

    /// Opaque protected-store generation.
    pub const fn generation(self) -> NonZeroU64 {
        self.generation
    }

    /// Owner authorization time in Unix seconds.
    pub const fn authorized_at_unix_seconds(self) -> u64 {
        self.authorized_at_unix_seconds
    }

    /// Hard reauthorization horizon in Unix seconds.
    pub const fn expires_at_unix_seconds(self) -> u64 {
        self.expires_at_unix_seconds
    }
}

/// Secret-bearing token response that zeroizes on drop and implements neither Clone nor Serialize.
pub struct TransientTokenResponse {
    access_token: Zeroizing<String>,
    refresh_token: Option<Zeroizing<String>>,
    scope: Option<Box<str>>,
}

impl TransientTokenResponse {
    /// Explicitly borrows the access token for one authenticated send.
    pub fn expose_access_token(&self) -> &str {
        &self.access_token
    }

    /// Explicitly borrows a returned refresh-token rotation candidate.
    pub fn expose_refresh_token(&self) -> Option<&str> {
        self.refresh_token.as_deref().map(String::as_str)
    }

    /// Provider-returned scope, if present.
    pub fn scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }
}

impl fmt::Debug for TransientTokenResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransientTokenResponse")
            .field("access_token", &"[REDACTED]")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("scope_present", &self.scope.is_some())
            .finish()
    }
}

/// Secret-free access/refresh lifecycle metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TokenLifecycle {
    access_issued_at_unix_seconds: u64,
    access_expires_at_unix_seconds: u64,
    refresh: RefreshTokenGeneration,
}

impl TokenLifecycle {
    /// Access-token issuance time.
    pub const fn access_issued_at_unix_seconds(self) -> u64 {
        self.access_issued_at_unix_seconds
    }

    /// Provider-bounded access-token expiry.
    pub const fn access_expires_at_unix_seconds(self) -> u64 {
        self.access_expires_at_unix_seconds
    }

    /// Refresh-token generation and hard reauthorization horizon.
    pub const fn refresh_generation(self) -> RefreshTokenGeneration {
        self.refresh
    }

    /// Returns the required action without retaining or inspecting token bytes.
    pub fn decision(
        self,
        now_unix_seconds: u64,
        refresh_early_seconds: u64,
    ) -> Result<TokenDecision, SchwabAdapterError> {
        if refresh_early_seconds >= ACCESS_TOKEN_MAX_LIFETIME_SECONDS
            || now_unix_seconds < self.access_issued_at_unix_seconds
        {
            return Err(SchwabAdapterError::InvalidTokenLifecycle);
        }
        if now_unix_seconds >= self.refresh.expires_at_unix_seconds {
            return Ok(TokenDecision::Reauthorize);
        }
        let refresh_at = self
            .access_expires_at_unix_seconds
            .saturating_sub(refresh_early_seconds);
        if now_unix_seconds >= refresh_at {
            Ok(TokenDecision::Refresh)
        } else {
            Ok(TokenDecision::Fresh)
        }
    }
}

/// Lifecycle action selected from secret-free expiry metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenDecision {
    /// Current access token is still outside the refresh window.
    Fresh,
    /// Refresh before or at access-token expiry.
    Refresh,
    /// The seven-day refresh authorization expired; interactive consent is required.
    Reauthorize,
}

/// Parses one bounded Schwab token response into transient secrets and durable-safe metadata.
pub fn parse_token_response(
    bytes: &[u8],
    issued_at_unix_seconds: u64,
    refresh: RefreshTokenGeneration,
    bounds: ParseBounds,
) -> Result<(TransientTokenResponse, TokenLifecycle), SchwabAdapterError> {
    if bytes.is_empty() || bytes.len() > bounds.max_response_bytes() {
        return Err(SchwabAdapterError::BoundsExceeded);
    }
    let mut wire: WireTokenResponse =
        serde_json::from_slice(bytes).map_err(|_| SchwabAdapterError::SchemaViolation)?;
    if wire.token_type != "Bearer"
        || wire.expires_in == 0
        || wire.expires_in > ACCESS_TOKEN_MAX_LIFETIME_SECONDS
        || refresh.authorized_at_unix_seconds > issued_at_unix_seconds
        || refresh.expires_at_unix_seconds <= issued_at_unix_seconds
    {
        wire.zeroize();
        return Err(SchwabAdapterError::InvalidTokenLifecycle);
    }
    validate_oauth_value(&wire.access_token)?;
    if let Some(token) = &wire.refresh_token {
        validate_oauth_value(token)?;
    }
    if wire.scope.as_ref().is_some_and(|scope| {
        scope.is_empty() || scope.len() > MAX_OAUTH_VALUE_BYTES || scope.contains(['\r', '\n'])
    }) {
        wire.zeroize();
        return Err(SchwabAdapterError::SchemaViolation);
    }
    let access_expires_at_unix_seconds = issued_at_unix_seconds
        .checked_add(wire.expires_in)
        .ok_or(SchwabAdapterError::ArithmeticOverflow)?;
    let lifecycle = TokenLifecycle {
        access_issued_at_unix_seconds: issued_at_unix_seconds,
        access_expires_at_unix_seconds,
        refresh,
    };
    let response = TransientTokenResponse {
        access_token: Zeroizing::new(std::mem::take(&mut wire.access_token)),
        refresh_token: wire.refresh_token.take().map(Zeroizing::new),
        scope: wire.scope.take().map(String::into_boxed_str),
    };
    wire.zeroize();
    Ok((response, lifecycle))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireTokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    token_type: String,
    expires_in: u64,
    #[serde(default)]
    scope: Option<String>,
}

impl Zeroize for WireTokenResponse {
    fn zeroize(&mut self) {
        self.access_token.zeroize();
        if let Some(refresh_token) = &mut self.refresh_token {
            refresh_token.zeroize();
        }
        self.token_type.zeroize();
        self.expires_in.zeroize();
        if let Some(scope) = &mut self.scope {
            scope.zeroize();
        }
    }
}

impl Drop for WireTokenResponse {
    fn drop(&mut self) {
        self.zeroize();
    }
}

fn validate_callback_origin(url: &Url) -> Result<(), SchwabAdapterError> {
    if url.scheme() != "https"
        || url.host_str() != Some("127.0.0.1")
        || url.port() != Some(8182)
        || url.path() != "/"
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(SchwabAdapterError::InvalidCallback);
    }
    Ok(())
}

fn validate_oauth_value(value: &str) -> Result<(), SchwabAdapterError> {
    if value.is_empty()
        || value.len() > MAX_OAUTH_VALUE_BYTES
        || value.as_bytes().contains(&0)
        || value.contains(['\r', '\n'])
    {
        return Err(SchwabAdapterError::InvalidInput);
    }
    Ok(())
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len() && bool::from(left.ct_eq(right))
}
