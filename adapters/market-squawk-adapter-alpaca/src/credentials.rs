use zeroize::Zeroizing;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use sha2::{Digest as _, Sha256};

use crate::AlpacaError;

const MIN_CREDENTIAL_BYTES: usize = 8;
const MAX_CREDENTIAL_BYTES: usize = 256;
const ALPACA_ACCOUNT_BINDING_DOMAIN: &[u8] = b"market-squawk/alpaca-account-binding/v2\0";

/// User-owned Alpaca Trading API key pair retained in zeroizing memory.
pub struct AlpacaCredentials {
    key_id: Zeroizing<String>,
    secret_key: Zeroizing<String>,
}

impl AlpacaCredentials {
    /// Validates a bounded ASCII credential pair.
    ///
    /// Alpaca does not publish a stable exact length for every credential generation. This
    /// constructor therefore enforces the security-relevant grammar without binding the adapter
    /// to an undocumented length.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, whitespace-containing, control-containing, or non-ASCII values.
    pub fn try_new(key_id: String, secret_key: String) -> Result<Self, AlpacaError> {
        if !valid_credential(&key_id) || !valid_credential(&secret_key) {
            return Err(AlpacaError::InvalidCredentials);
        }
        Ok(Self {
            key_id: Zeroizing::new(key_id),
            secret_key: Zeroizing::new(secret_key),
        })
    }

    pub(crate) fn key_id(&self) -> &str {
        self.key_id.as_str()
    }

    pub(crate) fn secret_key(&self) -> &str {
        self.secret_key.as_str()
    }

    /// Returns the non-secret Paper-realm principal bound to this key identifier.
    ///
    /// The secret key and raw key identifier are never exposed. The provider-observed doctor
    /// binds this digest into its sealed observation so a result cannot be transplanted between
    /// credential principals.
    pub fn paper_market_data_principal_sha256(&self) -> EvidenceDigest {
        let mut hasher = Sha256::new();
        hasher.update(ALPACA_ACCOUNT_BINDING_DOMAIN);
        hasher.update((self.key_id.len() as u64).to_be_bytes());
        hasher.update(self.key_id.as_bytes());
        hasher.update([2]);
        EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
    }
}

impl std::fmt::Debug for AlpacaCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaCredentials")
            .field("key_id", &"[REDACTED]")
            .field("secret_key", &"[REDACTED]")
            .finish()
    }
}

fn valid_credential(value: &str) -> bool {
    (MIN_CREDENTIAL_BYTES..=MAX_CREDENTIAL_BYTES).contains(&value.len())
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| !byte.is_ascii_whitespace() && !byte.is_ascii_control())
}
