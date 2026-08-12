//! Zeroizing BEA `UserID` ownership and authenticated request construction.

use std::fmt;

use subtle::ConstantTimeEq;
use url::Url;
use zeroize::{Zeroize, Zeroizing};

use crate::{BEA_API_ENDPOINT, BeaError, BeaRequest};

const BEA_USER_ID_BYTES: usize = 36;

/// User-owned 36-character BEA API `UserID` retained in zeroizing memory.
///
/// BEA calls this value a `UserID` even though account and setup screens may describe it as an API
/// key. It is always transmitted as the `UserID` query parameter; there is no separate bearer or
/// header credential in the reviewed API contract.
pub struct BeaUserId(Zeroizing<String>);

impl BeaUserId {
    /// Validates the documented credential shape without assuming UUID semantics.
    ///
    /// # Errors
    ///
    /// Rejects a value that is not exactly 36 printable ASCII bytes or that could change URL query
    /// structure.
    pub fn try_new(value: String) -> Result<Self, BeaError> {
        if value.len() != BEA_USER_ID_BYTES
            || !value.is_ascii()
            || value.bytes().any(|byte| {
                byte.is_ascii_whitespace()
                    || byte.is_ascii_control()
                    || matches!(byte, b'&' | b'=' | b'?' | b'#')
            })
        {
            return Err(BeaError::InvalidCredential);
        }
        Ok(Self(Zeroizing::new(value)))
    }

    pub(crate) fn expose_secret(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn matches_echo(&self, candidate: &str) -> bool {
        candidate.len() == self.0.len() && self.0.as_bytes().ct_eq(candidate.as_bytes()).into()
    }

    pub(crate) fn redact_from(&self, mut value: String) -> String {
        if !value.contains(self.expose_secret()) {
            return value;
        }
        let redacted = value.replace(self.expose_secret(), "[REDACTED]");
        value.zeroize();
        redacted
    }
}

impl fmt::Debug for BeaUserId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("BeaUserId")
            .field(&"[REDACTED]")
            .finish()
    }
}

/// One exact BEA GET request whose URL is zeroized on drop and redacted in `Debug` output.
pub struct BeaAuthorizedRequest {
    url: Zeroizing<String>,
    request_digest: [u8; 32],
}

impl BeaAuthorizedRequest {
    pub(crate) fn build(request: &BeaRequest, user_id: &BeaUserId) -> Result<Self, BeaError> {
        let mut url = Url::parse(BEA_API_ENDPOINT).map_err(|_| BeaError::InvalidRequest)?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("UserID", user_id.expose_secret());
            for (name, value) in request.query_pairs() {
                query.append_pair(name, value);
            }
        }
        Ok(Self {
            url: Zeroizing::new(url.into()),
            request_digest: request.request_digest(),
        })
    }

    /// Returns the authenticated request URL for immediate use by a transport.
    ///
    /// The returned value contains the credential. Callers must never log, persist, or include it
    /// in an error, receipt, trace, metric label, or raw-evidence request identity.
    pub fn expose_url(&self) -> &str {
        self.url.as_str()
    }

    /// Returns the credential-free exact request identity.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
}

impl fmt::Debug for BeaAuthorizedRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BeaAuthorizedRequest")
            .field("url", &"[REDACTED]")
            .field("request_digest", &self.request_digest)
            .finish()
    }
}
