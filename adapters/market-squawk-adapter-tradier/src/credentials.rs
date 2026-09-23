use zeroize::Zeroizing;

const MAX_ACCESS_TOKEN_BYTES: usize = 4_096;

/// User-authorized Tradier bearer credential retained only in zeroizing memory.
pub struct TradierAccessToken(Zeroizing<String>);

impl TradierAccessToken {
    /// Validates a bounded printable-ASCII bearer value.
    ///
    /// Tradier does not document one stable token length or alphabet. This boundary therefore
    /// accepts its bounded HTTP bearer grammar without logging or serializing the value.
    ///
    /// # Errors
    ///
    /// Rejects an empty, oversized, whitespace-containing, control, or non-ASCII value.
    pub fn try_new(value: String) -> Result<Self, TradierCredentialError> {
        if value.is_empty()
            || value.len() > MAX_ACCESS_TOKEN_BYTES
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_graphic() || byte.is_ascii_whitespace())
        {
            return Err(TradierCredentialError::InvalidAccessToken);
        }
        Ok(Self(Zeroizing::new(value)))
    }

    pub(crate) fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for TradierAccessToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TradierAccessToken([REDACTED])")
    }
}

/// Tradier credential validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TradierCredentialError {
    /// The bearer credential is outside the bounded HTTP token boundary.
    #[error("invalid Tradier access token")]
    InvalidAccessToken,
}
