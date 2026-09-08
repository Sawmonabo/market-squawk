use zeroize::Zeroizing;

use crate::OpenFigiCredentialError;

const MIN_API_KEY_BYTES: usize = 8;
const MAX_API_KEY_BYTES: usize = 512;

/// User-owned OpenFIGI API key retained in zeroizing memory.
///
/// The client borrows this value only for an authenticated request and never retains it.
pub struct OpenFigiApiKey(Zeroizing<String>);

impl OpenFigiApiKey {
    /// Validates a bounded ASCII API key without assuming an undocumented fixed length.
    ///
    /// # Errors
    ///
    /// Rejects empty, undersized, oversized, non-ASCII, whitespace-containing, or control-
    /// containing values.
    pub fn try_new(value: String) -> Result<Self, OpenFigiCredentialError> {
        if !(MIN_API_KEY_BYTES..=MAX_API_KEY_BYTES).contains(&value.len())
            || !value.is_ascii()
            || value
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            return Err(OpenFigiCredentialError::Invalid);
        }
        Ok(Self(Zeroizing::new(value)))
    }

    pub(crate) fn expose_secret(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for OpenFigiApiKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("OpenFigiApiKey")
            .field(&"[REDACTED]")
            .finish()
    }
}
