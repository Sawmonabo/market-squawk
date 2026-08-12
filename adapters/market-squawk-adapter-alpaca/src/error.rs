use thiserror::Error;

/// Alpaca adapter configuration, protocol, or bounded transport failure.
#[derive(Debug, Error)]
pub enum AlpacaError {
    /// A source or provider identity violated the bounded domain grammar.
    #[error("Alpaca configuration contains an invalid identity")]
    Identity(#[from] market_squawk_domain::IdentityError),
    /// Source metadata contradicted the selected Alpaca Basic surface.
    #[error("Alpaca source metadata is invalid: {0}")]
    Metadata(#[from] market_squawk_sources::SourceMetadataError),
    /// An endpoint or request policy was not structurally safe.
    #[error("Alpaca network policy is invalid: {0}")]
    NetworkPolicy(#[from] market_squawk_sources::NetworkPolicyError),
    /// API credentials were empty, unbounded, or unsafe to place in request headers.
    #[error("Alpaca API credentials are invalid")]
    InvalidCredentials,
    /// Only a user-authorized Alpaca Trading API credential is accepted.
    #[error("Alpaca market data requires user-authorized account evidence")]
    InvalidAuthorization,
    /// A configured symbol, mapping, or dataset is invalid or ambiguous.
    #[error("Alpaca instrument coverage is invalid")]
    InvalidCoverage,
    /// The provider's Basic-plan subscription ceiling was exceeded.
    #[error("Alpaca Basic subscription exceeds its documented symbol limit")]
    SubscriptionLimit,
    /// Transport byte or deadline limits were invalid.
    #[error("Alpaca transport limits are invalid")]
    InvalidTransportLimits,
    /// Shared budget metadata does not retain the documented Basic historical ceiling.
    #[error("Alpaca shared provider budget does not enforce 200 historical requests per minute")]
    InvalidBudget,
    /// Historical dates, timeframe, adjustment, or page size were invalid.
    #[error("Alpaca historical request plan is invalid")]
    InvalidHistoricalPlan,
    /// JSON or MessagePack subscription construction failed.
    #[error("Alpaca protocol serialization failed")]
    Serialization,
    /// A provider payload violated the selected protocol schema.
    #[error("Alpaca provider payload is invalid")]
    Protocol,
    /// Exact raw provider responses could not satisfy the shared durable-capture contract.
    #[error("Alpaca provider response capture material is invalid")]
    CaptureMaterial,
    /// A bounded allocation failed.
    #[error("Alpaca bounded allocation failed")]
    Allocation,
    /// A remote operation failed without retaining credential material.
    #[error("Alpaca network operation failed")]
    Network,
    /// The provider response crossed the configured post-decompression ceiling.
    #[error("Alpaca response exceeded its configured byte limit")]
    BodyTooLarge,
    /// The caller's operation deadline elapsed.
    #[error("Alpaca operation deadline elapsed")]
    DeadlineExceeded,
    /// Cancellation interrupted the operation.
    #[error("Alpaca operation was cancelled")]
    Cancelled,
}
