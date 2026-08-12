//! BEA adapter failures.

use thiserror::Error;

/// A provider-returned BEA API error with bounded, credential-free detail.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaProviderError {
    code: u32,
    description: String,
}

impl BeaProviderError {
    pub(crate) const fn new(code: u32, description: String) -> Self {
        Self { code, description }
    }

    /// Returns the documented numeric BEA API error code.
    pub const fn code(&self) -> u32 {
        self.code
    }

    /// Returns the bounded provider description. It never contains the retained `UserID`.
    pub fn description(&self) -> &str {
        &self.description
    }
}

/// A bounded credential, query, protocol, or revision-contract failure.
#[derive(Debug, Error)]
pub enum BeaError {
    /// The BEA `UserID` does not satisfy the documented 36-character credential shape.
    #[error("invalid BEA UserID")]
    InvalidCredential,
    /// A request cannot be represented by the admitted BEA query contract.
    #[error("invalid BEA request")]
    InvalidRequest,
    /// A local parser or page bound is zero or internally inconsistent.
    #[error("invalid BEA parser or page limit")]
    InvalidLimit,
    /// The exact response body exceeds its configured byte bound.
    #[error("BEA response body exceeds the configured byte bound")]
    BodyTooLarge,
    /// A returned collection exceeds its configured row or metadata-item bound.
    #[error("BEA response exceeds the configured row bound")]
    RowLimitExceeded,
    /// A retained provider string exceeds its configured bound.
    #[error("BEA provider string exceeds the configured bound")]
    StringLimitExceeded,
    /// A fallible retained allocation could not be admitted.
    #[error("BEA response allocation could not be admitted")]
    Allocation,
    /// The response is not valid JSON.
    #[error("invalid BEA JSON response: {0}")]
    InvalidJson(#[from] serde_json::Error),
    /// The response envelope, method result, or dataset-driven dimension contract is invalid.
    #[error("invalid BEA protocol field: {0}")]
    InvalidField(&'static str),
    /// The provider did not echo the exact credential-free request coordinates.
    #[error("BEA response request echo does not match the admitted request")]
    RequestEchoMismatch,
    /// The response contains a BEA API error.
    #[error("BEA API returned provider error {0:?}")]
    Provider(BeaProviderError),
    /// Error 34 proves filtered parameter discovery is unavailable for this dataset.
    #[error("BEA filtered parameter discovery is unsupported for this dataset")]
    FilteredParameterValuesUnsupported,
    /// An observed numeric cell cannot be parsed as an exact decimal.
    #[error("invalid BEA exact decimal")]
    InvalidDecimal,
    /// A provider time period is outside the closed annual/quarterly/monthly contract.
    #[error("invalid BEA time period")]
    InvalidTimePeriod,
    /// A release/revision/correction event violates append-only predecessor semantics.
    #[error("invalid BEA correction or revision evidence")]
    InvalidRevision,
}
