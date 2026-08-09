use thiserror::Error;

/// Invalid user-owned OpenFIGI credential material.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OpenFigiCredentialError {
    /// API key did not satisfy the bounded security grammar.
    #[error("OpenFIGI API key is invalid")]
    Invalid,
}

/// Invalid source-qualified listing mapping input or receipt state.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OpenFigiModelError {
    /// Symbol was not a bounded current Nasdaq directory symbol.
    #[error("OpenFIGI mapping symbol is invalid")]
    InvalidSymbol,
    /// MIC was not a four-character ISO-style market identifier.
    #[error("OpenFIGI mapping MIC is invalid")]
    InvalidMic,
    /// Source and observation timestamps were not ordered.
    #[error("OpenFIGI mapping timestamps are inconsistent")]
    InvalidTemporalOrder,
    /// Receipt results did not correspond exactly to submitted jobs.
    #[error("OpenFIGI mapping receipt is inconsistent")]
    InvalidReceipt,
}

/// Invalid bounded mapping request construction.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OpenFigiRequestError {
    /// A mapping request must contain at least one job.
    #[error("OpenFIGI mapping request is empty")]
    Empty,
    /// Request exceeded the access-mode job ceiling.
    #[error("OpenFIGI mapping request exceeds {max} jobs")]
    TooManyJobs {
        /// Maximum jobs for the selected access mode.
        max: usize,
    },
    /// Serialized request exceeded the adapter byte ceiling.
    #[error("OpenFIGI mapping request exceeds {max} bytes")]
    TooLarge {
        /// Maximum exact request bytes.
        max: usize,
    },
    /// Deterministic request serialization failed.
    #[error("OpenFIGI mapping request serialization failed")]
    Serialization,
}

/// Invalid or contradictory OpenFIGI V3 mapping response.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OpenFigiParseError {
    /// Exact response payload was empty.
    #[error("OpenFIGI mapping response is empty")]
    Empty,
    /// Exact response exceeded the adapter byte ceiling.
    #[error("OpenFIGI mapping response exceeds {max} bytes")]
    TooLarge {
        /// Maximum exact response bytes.
        max: usize,
    },
    /// Response was not valid V3 JSON.
    #[error("OpenFIGI mapping response JSON is invalid")]
    InvalidJson,
    /// Response array did not preserve one result per request job.
    #[error("OpenFIGI mapping response count does not match request count")]
    Cardinality,
    /// Parser could not reserve its bounded result storage.
    #[error("OpenFIGI mapping parser could not reserve bounded storage")]
    Allocation,
}

/// Invalid or incomplete OpenFIGI rate-window evidence.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OpenFigiRateLimitError {
    /// A required provider header was absent.
    #[error("OpenFIGI response omitted required rate-limit evidence")]
    Missing,
    /// A singleton provider header was repeated.
    #[error("OpenFIGI response repeated a rate-limit header")]
    Duplicate,
    /// A header was not a bounded unsigned decimal integer.
    #[error("OpenFIGI response contained an invalid rate-limit header")]
    Invalid,
    /// Header values contradicted one another.
    #[error("OpenFIGI response contained inconsistent rate-limit evidence")]
    Inconsistent,
}

/// OpenFIGI client construction or one-shot mapping failure.
#[derive(Debug, Error)]
pub enum OpenFigiClientError {
    /// Registered source metadata does not describe this exact adapter surface.
    #[error("OpenFIGI source metadata is invalid")]
    InvalidMetadata,
    /// Selected access mode and borrowed API-key presence disagree.
    #[error("OpenFIGI access mode and API-key presence disagree")]
    CredentialMismatch,
    /// Request could not be encoded within fixed limits.
    #[error(transparent)]
    Request(#[from] OpenFigiRequestError),
    /// Registry extraction authority rejected the operation.
    #[error(transparent)]
    Authority(#[from] market_squawk_sources::ExtractionAuthorityError),
    /// Caller cancelled the operation.
    #[error("OpenFIGI mapping was cancelled")]
    Cancelled,
    /// Caller deadline or a configured transport deadline elapsed.
    #[error("OpenFIGI mapping deadline elapsed")]
    DeadlineExceeded,
    /// Local trusted wall-clock acquisition failed.
    #[error("OpenFIGI mapping receipt time is unavailable")]
    Clock,
    /// HTTPS transport failed.
    #[error("OpenFIGI network operation failed")]
    Network,
    /// Provider rejected the supplied API key.
    #[error("OpenFIGI authorization failed")]
    Unauthorized,
    /// Provider rejected the exact request.
    #[error("OpenFIGI rejected the mapping request with HTTP status {status}")]
    ProviderRejected {
        /// Exact provider HTTP status.
        status: u16,
    },
    /// Provider failed outside a request-validation response.
    #[error("OpenFIGI is temporarily unavailable")]
    ProviderUnavailable,
    /// Successful response media type or content encoding violated the V3 contract.
    #[error("OpenFIGI response representation is invalid")]
    InvalidRepresentation,
    /// Response body exceeded a registered or adapter limit.
    #[error("OpenFIGI response exceeds {max} bytes")]
    ResponseTooLarge {
        /// Effective response limit.
        max: usize,
    },
    /// Required rate-window evidence was invalid.
    #[error(transparent)]
    RateLimit(#[from] OpenFigiRateLimitError),
    /// V3 response payload could not be safely classified.
    #[error(transparent)]
    Parse(#[from] OpenFigiParseError),
    /// Mapping receipt violated a local relational invariant.
    #[error(transparent)]
    Model(#[from] OpenFigiModelError),
    /// Latest rate-evidence synchronization failed.
    #[error("OpenFIGI rate-limit evidence is unavailable")]
    RateEvidenceUnavailable,
}
