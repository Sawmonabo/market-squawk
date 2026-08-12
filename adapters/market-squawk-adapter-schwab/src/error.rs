use thiserror::Error;

/// Closed, secret-free adapter failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SchwabAdapterError {
    /// A caller supplied an empty, oversized, duplicate, or syntactically invalid value.
    #[error("invalid Schwab adapter input")]
    InvalidInput,
    /// A URL, method, path, or query is outside the read-only provider allowlist.
    #[error("Schwab route is outside the read-only allowlist")]
    RouteNotAllowed,
    /// A bounded request would exceed its caller-admitted byte or item ceiling.
    #[error("Schwab request exceeds its runtime admission")]
    RequestNotAdmitted,
    /// A response or frame exceeded a finite caller-owned resource bound.
    #[error("Schwab response exceeds its parse bounds")]
    BoundsExceeded,
    /// Provider JSON was malformed or violated the selected native schema.
    #[error("Schwab provider payload violates the native schema")]
    SchemaViolation,
    /// Checked capacity or lifecycle arithmetic overflowed.
    #[error("Schwab checked arithmetic overflow")]
    ArithmeticOverflow,
    /// The callback did not match the code-owned HTTPS loopback origin or correlation state.
    #[error("Schwab OAuth callback validation failed")]
    InvalidCallback,
    /// The OAuth token response or lifecycle was inconsistent.
    #[error("Schwab OAuth token lifecycle validation failed")]
    InvalidTokenLifecycle,
    /// A second Streamer connection or an invalid state transition was attempted.
    #[error("Schwab Streamer state transition rejected")]
    InvalidStreamerState,
    /// The provider explicitly rejected a Streamer request.
    #[error("Schwab Streamer request was rejected")]
    StreamerRejected,
}
