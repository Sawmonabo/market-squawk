use reqwest::header::{ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, HeaderValue, USER_AGENT};
use zeroize::Zeroizing;

use crate::{TiingoAdapterError, TiingoRequestSpec};

const MAX_TOKEN_BYTES: usize = 512;
const USER_AGENT_VALUE: &str = "market-squawk/1.0 tiingo-adapter";

/// User-owned Tiingo token retained in zeroizing memory and never accepted in a URL.
pub struct TiingoApiToken(Zeroizing<String>);

impl TiingoApiToken {
    /// Validates one bounded opaque Tiingo token.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, non-ASCII, whitespace-containing, or control-containing tokens.
    pub fn try_new(value: String) -> Result<Self, TiingoAdapterError> {
        if value.is_empty()
            || value.len() > MAX_TOKEN_BYTES
            || !value.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(TiingoAdapterError::InvalidToken);
        }
        Ok(Self(Zeroizing::new(value)))
    }

    fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for TiingoApiToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TiingoApiToken([REDACTED])")
    }
}

/// Builds strict code-owned Tiingo GET requests with sensitive header authentication.
pub struct TiingoRequestBuilder {
    client: reqwest::Client,
    token: TiingoApiToken,
}

impl TiingoRequestBuilder {
    /// Constructs a request builder around an already hardened shared HTTP client.
    pub const fn new(client: reqwest::Client, token: TiingoApiToken) -> Self {
        Self { client, token }
    }

    /// Builds one GET request. The token is present only in a sensitive `Authorization` header.
    ///
    /// # Errors
    ///
    /// Fails closed when the token cannot be represented by an HTTP header or request creation
    /// fails. The returned URL is credential-free.
    pub fn build(&self, spec: &TiingoRequestSpec) -> Result<reqwest::Request, TiingoAdapterError> {
        let mut authorization = Zeroizing::new(String::from("Token "));
        authorization.push_str(self.token.expose());
        let mut header =
            HeaderValue::from_str(&authorization).map_err(|_| TiingoAdapterError::RequestBuild)?;
        header.set_sensitive(true);

        self.client
            .get(spec.url().clone())
            .header(AUTHORIZATION, header)
            .header(ACCEPT, "application/json")
            .header(ACCEPT_ENCODING, "identity")
            .header(USER_AGENT, USER_AGENT_VALUE)
            .build()
            .map_err(|_| TiingoAdapterError::RequestBuild)
    }
}

impl std::fmt::Debug for TiingoRequestBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TiingoRequestBuilder")
            .field("token", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}
