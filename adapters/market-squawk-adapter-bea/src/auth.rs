//! Zeroizing BEA `UserID` ownership and authenticated request construction.

use std::fmt;
use std::mem;

use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use url::Url;
use zeroize::{Zeroize, Zeroizing};

use crate::{BEA_API_ENDPOINT, BeaError, BeaParseLimits, BeaRequest};

const BEA_USER_ID_BYTES: usize = 36;
pub(crate) const BEA_REDACTED_USER_ID: &[u8; BEA_USER_ID_BYTES] =
    b"************************************";

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
    /// Rejects a value that is not exactly 36 unreserved ASCII bytes. Keeping the admitted
    /// credential alphabet URL- and JSON-literal-safe lets the response sanitizer replace the
    /// one validated upstream echo before a general-purpose parser can allocate it.
    pub fn try_new(mut value: String) -> Result<Self, BeaError> {
        if value.len() != BEA_USER_ID_BYTES
            || !value.is_ascii()
            || value.as_bytes() == BEA_REDACTED_USER_ID
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
            })
        {
            value.zeroize();
            return Err(BeaError::InvalidCredential);
        }
        Ok(Self(Zeroizing::new(value)))
    }

    pub(crate) fn expose_secret(&self) -> &str {
        self.0.as_str()
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
        if request.query_pairs().any(|(name, value)| {
            name.contains(user_id.expose_secret()) || value.contains(user_id.expose_secret())
        }) {
            return Err(BeaError::InvalidRequest);
        }
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
    pub(crate) fn expose_url(&self) -> &str {
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

/// Owned transient response bytes that may still contain BEA's echoed `UserID`.
///
/// Every owned byte is zeroized on every error path. A successful parser must first validate the
/// exact original echo, after which [`Self::sanitize_validated_echo`] overwrites the sole literal
/// occurrence in place before any raw-capture or journal object can be constructed.
pub(crate) struct BeaSensitiveBody(Zeroizing<Vec<u8>>);

impl BeaSensitiveBody {
    pub(crate) fn from_vec(bytes: Vec<u8>) -> Self {
        Self(Zeroizing::new(bytes))
    }

    pub(crate) fn from_zeroizing(bytes: Zeroizing<Vec<u8>>) -> Self {
        Self(bytes)
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Validates the complete request echo and returns only fixed-length redacted bytes.
    ///
    /// The upstream body remains zeroizing until the parser has proved that the exact admitted
    /// request was echoed and that no additional decoded result field contains the credential.
    /// The retained body replaces the one literal echo in place; it never formats or retains the
    /// credential or a credential-bearing URL.
    pub(crate) fn sanitize_validated_echo(
        self,
        request: &BeaRequest,
        user_id: &BeaUserId,
        limits: BeaParseLimits,
    ) -> Result<BeaSanitizedBody, BeaError> {
        crate::parser::sanitize_response_body(self, request, user_id, limits)
    }

    pub(crate) fn into_zeroizing(mut self) -> Zeroizing<Vec<u8>> {
        mem::take(&mut self.0)
    }
}

impl fmt::Debug for BeaSensitiveBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BeaSensitiveBody")
            .field("bytes", &self.0.len())
            .field("contents", &"[ZEROIZING REDACTED]")
            .finish()
    }
}

/// Secret-free retained body and commitments to both upstream and sanitized representations.
pub(crate) struct BeaSanitizedBody {
    bytes: Bytes,
    upstream_digest: [u8; 32],
    retained_digest: [u8; 32],
}

impl BeaSanitizedBody {
    pub(crate) fn from_secret_free_vec(bytes: Vec<u8>, upstream_digest: [u8; 32]) -> Self {
        let retained_digest = Sha256::digest(&bytes).into();
        Self {
            bytes: Bytes::from(bytes),
            upstream_digest,
            retained_digest,
        }
    }

    pub(crate) const fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub(crate) const fn retained_digest(&self) -> [u8; 32] {
        self.retained_digest
    }

    /// Returns SHA-256 of the exact upstream body before fixed-length echo redaction.
    pub(crate) const fn upstream_digest(&self) -> [u8; 32] {
        self.upstream_digest
    }
}
