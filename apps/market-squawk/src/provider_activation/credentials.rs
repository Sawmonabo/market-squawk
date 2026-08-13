//! Secret-safe credential parsing and fixed provider-verification requests.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use hmac::{Hmac, Mac as _};
use market_squawk_adapter_alpaca::{AlpacaCredentials, AlpacaError, AlpacaTradingApiEnvironment};
use market_squawk_adapter_kraken::KRAKEN_L3_GET_TOKEN_ENDPOINT;
use market_squawk_adapter_tradier::{TradierAccessToken, TradierCredentialError};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use reqwest::header::{ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use serde::{Deserialize, Deserializer};
use sha2::{Digest as _, Sha256, Sha512};
use zeroize::Zeroizing;

const KRAKEN_API_KEY_INFO_ENDPOINT: &str = "https://api.kraken.com/0/private/GetApiKeyInfo";
const KRAKEN_API_KEY_INFO_PATH: &str = "/0/private/GetApiKeyInfo";
const KRAKEN_GET_TOKEN_PATH: &str = "/0/private/GetWebSocketsToken";
const TRADIER_PROFILE_ENDPOINT: &str = "https://api.tradier.com/v1/user/profile";
const MAX_CREDENTIAL_ENVELOPE_BYTES: usize = 16 * 1024;
const MAX_KRAKEN_KEY_BYTES: usize = 4_096;
const MAX_KRAKEN_SECRET_BYTES: usize = 4_096;
const ALPACA_ACCOUNT_BINDING_DOMAIN: &[u8] = b"market-squawk/alpaca-account-binding/v2\0";
const KRAKEN_ACCOUNT_BINDING_DOMAIN: &[u8] = b"market-squawk/kraken-account-binding/v1\0";
static KRAKEN_NONCE_HIGH_WATER: AtomicU64 = AtomicU64::new(0);

type HmacSha512 = Hmac<Sha512>;

pub(crate) struct AlpacaCredentialEnvelope {
    key_id: Zeroizing<String>,
    secret_key: Zeroizing<String>,
    trading_api_environment: AlpacaTradingApiEnvironment,
}

impl AlpacaCredentialEnvelope {
    pub(crate) fn try_parse(envelope: &str) -> Result<Self, ProviderCredentialError> {
        if envelope.is_empty() || envelope.len() > MAX_CREDENTIAL_ENVELOPE_BYTES {
            return Err(ProviderCredentialError::InvalidEnvelope);
        }
        let AlpacaCredentialWire {
            version,
            key_id: SecretCredentialField(key_id),
            secret_key: SecretCredentialField(secret_key),
            trading_api_environment,
        } = serde_json::from_str(envelope)
            .map_err(|_error| ProviderCredentialError::InvalidEnvelope)?;
        if version != 1 {
            return Err(ProviderCredentialError::InvalidEnvelope);
        }
        AlpacaCredentials::try_new(key_id.to_string(), secret_key.to_string())?;
        let trading_api_environment = match trading_api_environment {
            AlpacaTradingApiEnvironmentWire::Paper => AlpacaTradingApiEnvironment::Paper,
        };
        Ok(Self {
            key_id,
            secret_key,
            trading_api_environment,
        })
    }

    pub(crate) fn account_digest(&self) -> EvidenceDigest {
        let mut hasher = Sha256::new();
        hasher.update(ALPACA_ACCOUNT_BINDING_DOMAIN);
        hasher.update((self.key_id.len() as u64).to_be_bytes());
        hasher.update(self.key_id.as_bytes());
        hasher.update([match self.trading_api_environment {
            AlpacaTradingApiEnvironment::Live => 1,
            AlpacaTradingApiEnvironment::Paper => 2,
        }]);
        EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
    }

    /// Returns the exact Trading API realm declared by this verified credential envelope.
    pub(crate) const fn trading_api_environment(&self) -> AlpacaTradingApiEnvironment {
        self.trading_api_environment
    }

    pub(crate) fn into_credentials(self) -> Result<AlpacaCredentials, ProviderCredentialError> {
        AlpacaCredentials::try_new(self.key_id.to_string(), self.secret_key.to_string())
            .map_err(Into::into)
    }
}

impl std::fmt::Debug for AlpacaCredentialEnvelope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AlpacaCredentialEnvelope([REDACTED])")
    }
}

pub(crate) struct KrakenL3CredentialSigner {
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<Vec<u8>>,
}

impl KrakenL3CredentialSigner {
    pub(crate) fn try_parse(envelope: &str) -> Result<Self, ProviderCredentialError> {
        if envelope.is_empty() || envelope.len() > MAX_CREDENTIAL_ENVELOPE_BYTES {
            return Err(ProviderCredentialError::InvalidEnvelope);
        }
        let KrakenCredentialWire {
            version,
            api_key: SecretCredentialField(api_key),
            api_secret: SecretCredentialField(api_secret),
        } = serde_json::from_str(envelope)
            .map_err(|_error| ProviderCredentialError::InvalidEnvelope)?;
        if version != 1
            || !valid_graphic_ascii(&api_key, MAX_KRAKEN_KEY_BYTES)
            || api_secret.is_empty()
            || api_secret.len() > MAX_KRAKEN_SECRET_BYTES
            || api_secret.chars().any(char::is_control)
        {
            return Err(ProviderCredentialError::InvalidEnvelope);
        }
        let decoded = Zeroizing::new(
            BASE64_STANDARD
                .decode(api_secret.as_bytes())
                .map_err(|_error| ProviderCredentialError::InvalidEnvelope)?,
        );
        if decoded.is_empty() || decoded.len() > MAX_KRAKEN_SECRET_BYTES {
            return Err(ProviderCredentialError::InvalidEnvelope);
        }
        Ok(Self {
            api_key,
            api_secret: decoded,
        })
    }

    pub(crate) fn api_key_info_request(
        &self,
        client: &reqwest::Client,
        nonce: u64,
    ) -> Result<reqwest::RequestBuilder, ProviderCredentialError> {
        self.signed_request(
            client,
            KRAKEN_API_KEY_INFO_ENDPOINT,
            KRAKEN_API_KEY_INFO_PATH,
            nonce,
        )
    }

    pub(crate) fn websocket_token_request(
        &self,
        client: &reqwest::Client,
        nonce: u64,
    ) -> Result<reqwest::RequestBuilder, ProviderCredentialError> {
        self.signed_request(
            client,
            KRAKEN_L3_GET_TOKEN_ENDPOINT,
            KRAKEN_GET_TOKEN_PATH,
            nonce,
        )
    }

    pub(crate) fn account_digest(&self) -> EvidenceDigest {
        account_digest(KRAKEN_ACCOUNT_BINDING_DOMAIN, self.api_key.as_bytes())
    }

    fn signed_request(
        &self,
        client: &reqwest::Client,
        endpoint: &'static str,
        path: &'static str,
        nonce: u64,
    ) -> Result<reqwest::RequestBuilder, ProviderCredentialError> {
        if nonce == 0 || i64::try_from(nonce).is_err() {
            return Err(ProviderCredentialError::InvalidNonce);
        }
        let nonce = nonce.to_string();
        let payload = format!(r#"{{"nonce":{nonce}}}"#);
        let mut inner = Sha256::new();
        inner.update(nonce.as_bytes());
        inner.update(payload.as_bytes());
        let inner: [u8; 32] = inner.finalize().into();
        let mut signature = HmacSha512::new_from_slice(&self.api_secret)
            .map_err(|_error| ProviderCredentialError::Signing)?;
        signature.update(path.as_bytes());
        signature.update(&inner);
        let signature = Zeroizing::new(BASE64_STANDARD.encode(signature.finalize().into_bytes()));
        let key = sensitive_header(self.api_key.as_str())?;
        let signature = sensitive_header(signature.as_str())?;
        Ok(client
            .post(endpoint)
            .header("API-Key", key)
            .header("API-Sign", signature)
            .header(ACCEPT, "application/json")
            .header(ACCEPT_ENCODING, "identity")
            .header(CONTENT_TYPE, "application/json")
            .body(payload))
    }
}

impl std::fmt::Debug for KrakenL3CredentialSigner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("KrakenL3CredentialSigner([REDACTED])")
    }
}

pub(crate) fn tradier_verification_request(
    client: &reqwest::Client,
    token: &str,
) -> Result<reqwest::RequestBuilder, ProviderCredentialError> {
    TradierAccessToken::try_new(token.to_owned())?;
    let mut bearer = Zeroizing::new(String::with_capacity(token.len().saturating_add(7)));
    bearer.push_str("Bearer ");
    bearer.push_str(token);
    let authorization = sensitive_header(bearer.as_str())?;
    Ok(client
        .get(TRADIER_PROFILE_ENDPOINT)
        .header(AUTHORIZATION, authorization)
        .header(ACCEPT, "application/json"))
}

pub(crate) fn tradier_access_token(
    token: &str,
) -> Result<TradierAccessToken, ProviderCredentialError> {
    TradierAccessToken::try_new(token.to_owned()).map_err(Into::into)
}

pub(crate) fn next_kraken_nonce() -> Result<u64, ProviderCredentialError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ProviderCredentialError::Clock)?;
    let candidate =
        u64::try_from(now.as_nanos()).map_err(|_error| ProviderCredentialError::Clock)?;
    let mut observed = KRAKEN_NONCE_HIGH_WATER.load(Ordering::Acquire);
    loop {
        let next = candidate.max(
            observed
                .checked_add(1)
                .ok_or(ProviderCredentialError::InvalidNonce)?,
        );
        match KRAKEN_NONCE_HIGH_WATER.compare_exchange_weak(
            observed,
            next,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Ok(next),
            Err(current) => observed = current,
        }
    }
}

fn sensitive_header(value: &str) -> Result<HeaderValue, ProviderCredentialError> {
    let mut value =
        HeaderValue::from_str(value).map_err(|_error| ProviderCredentialError::Header)?;
    value.set_sensitive(true);
    Ok(value)
}

fn account_digest(domain: &[u8], identifier: &[u8]) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update((identifier.len() as u64).to_be_bytes());
    hasher.update(identifier);
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn valid_graphic_ascii(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.is_ascii()
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AlpacaCredentialWire {
    version: u8,
    key_id: SecretCredentialField,
    secret_key: SecretCredentialField,
    trading_api_environment: AlpacaTradingApiEnvironmentWire,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum AlpacaTradingApiEnvironmentWire {
    Paper,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KrakenCredentialWire {
    version: u8,
    api_key: SecretCredentialField,
    api_secret: SecretCredentialField,
}

struct SecretCredentialField(Zeroizing<String>);

impl<'de> Deserialize<'de> for SecretCredentialField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(|value| Self(Zeroizing::new(value)))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProviderCredentialError {
    #[error("provider credential envelope is invalid")]
    InvalidEnvelope,
    #[error("provider credential header is invalid")]
    Header,
    #[error("Kraken request signing failed")]
    Signing,
    #[error("Kraken nonce is invalid")]
    InvalidNonce,
    #[error("system clock is unavailable")]
    Clock,
    #[error(transparent)]
    Alpaca(#[from] AlpacaError),
    #[error(transparent)]
    Tradier(#[from] TradierCredentialError),
}
