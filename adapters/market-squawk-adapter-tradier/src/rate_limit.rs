use reqwest::header::{HeaderMap, HeaderName};
use thiserror::Error;

const ALLOWED: HeaderName = HeaderName::from_static("x-ratelimit-allowed");
const USED: HeaderName = HeaderName::from_static("x-ratelimit-used");
const AVAILABLE: HeaderName = HeaderName::from_static("x-ratelimit-available");
const EXPIRY: HeaderName = HeaderName::from_static("x-ratelimit-expiry");
const MAX_RATE_HEADER_BYTES: usize = 20;

/// Exact complete Tradier request-budget response evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TradierRateLimitEvidence {
    allowed: u32,
    used: u32,
    available: u32,
    expires_at_unix_millis: u64,
}

impl TradierRateLimitEvidence {
    pub(crate) fn try_from_headers(headers: &HeaderMap) -> Result<Self, TradierRateLimitError> {
        let allowed = parse_u32(headers, &ALLOWED)?;
        let used = parse_u32(headers, &USED)?;
        let available = parse_u32(headers, &AVAILABLE)?;
        let expires_at_unix_millis = parse_u64(headers, &EXPIRY)?;
        if allowed == 0
            || used > allowed
            || available > allowed
            || used.checked_add(available) != Some(allowed)
            || expires_at_unix_millis == 0
        {
            return Err(TradierRateLimitError::Inconsistent);
        }
        Ok(Self {
            allowed,
            used,
            available,
            expires_at_unix_millis,
        })
    }

    /// Returns the provider-declared request allowance for the active minute window.
    pub const fn allowed(self) -> u32 {
        self.allowed
    }

    /// Returns the provider-declared consumed request count.
    pub const fn used(self) -> u32 {
        self.used
    }

    /// Returns the provider-declared remaining request count.
    pub const fn available(self) -> u32 {
        self.available
    }

    /// Returns the provider-declared Unix-millisecond reset time.
    pub const fn expires_at_unix_millis(self) -> u64 {
        self.expires_at_unix_millis
    }
}

fn parse_u32(headers: &HeaderMap, name: &HeaderName) -> Result<u32, TradierRateLimitError> {
    let value = parse_u64(headers, name)?;
    u32::try_from(value).map_err(|_| TradierRateLimitError::Invalid)
}

fn parse_u64(headers: &HeaderMap, name: &HeaderName) -> Result<u64, TradierRateLimitError> {
    let mut values = headers.get_all(name).iter();
    let value = values.next().ok_or(TradierRateLimitError::Missing)?;
    if values.next().is_some() {
        return Err(TradierRateLimitError::Duplicate);
    }
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || bytes.len() > MAX_RATE_HEADER_BYTES
        || !bytes.iter().all(u8::is_ascii_digit)
    {
        return Err(TradierRateLimitError::Invalid);
    }
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(TradierRateLimitError::Invalid)
}

/// Invalid or incomplete provider rate-limit evidence.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TradierRateLimitError {
    /// At least one required header was absent.
    #[error("Tradier response omitted required rate-limit evidence")]
    Missing,
    /// A singleton rate-limit header was repeated.
    #[error("Tradier response repeated a rate-limit header")]
    Duplicate,
    /// A rate-limit header was not a bounded unsigned integer.
    #[error("Tradier response contained an invalid rate-limit header")]
    Invalid,
    /// Header values contradicted one another.
    #[error("Tradier response contained inconsistent rate-limit evidence")]
    Inconsistent,
}
