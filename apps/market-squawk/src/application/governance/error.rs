use thiserror::Error;

/// Closed non-sensitive governance admission, authentication, ticket, or durability failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GovernanceError {
    /// An opaque UUID identity was nil.
    #[error("governance identity is invalid")]
    InvalidIdentity,
    /// State or lifetime limits are invalid.
    #[error("governance limits are invalid")]
    InvalidLimits,
    /// A durable local principal profile is invalid.
    #[error("governance principal is invalid")]
    InvalidPrincipal,
    /// A role set is empty, oversized, or duplicated.
    #[error("governance role set is invalid")]
    InvalidRoleSet,
    /// Preview metadata is invalid.
    #[error("governance action preview is invalid")]
    InvalidPreview,
    /// Principal registration did not exist.
    #[error("governance principal was not found")]
    PrincipalNotFound,
    /// Principal is not eligible for the exact preview.
    #[error("governance principal is not eligible")]
    PrincipalNotEligible,
    /// Preview state is absent or was pruned after expiry.
    #[error("governance action preview was not found")]
    PreviewNotFound,
    /// Preview expired before ticket issue or commit.
    #[error("governance action preview expired")]
    PreviewExpired,
    /// Protected credential did not match in constant time.
    #[error("governance reauthentication failed")]
    InvalidCredential,
    /// Bounded failed attempts for this preview/principal are exhausted.
    #[error("governance reauthentication is locked")]
    ReauthenticationLocked,
    /// A principal already holds a ticket for this preview.
    #[error("governance ticket was already issued")]
    TicketAlreadyIssued,
    /// Submitted ticket identifier was never issued or has expired.
    #[error("governance ticket was not found")]
    TicketNotFound,
    /// Ticket belongs to another canonical action preview or runtime binding.
    #[error("governance ticket does not match the action preview")]
    TicketPreviewMismatch,
    /// Ticket expired before service commit verification.
    #[error("governance ticket expired")]
    TicketExpired,
    /// Ticket was already consumed by an authorization receipt.
    #[error("governance ticket was already consumed")]
    TicketConsumed,
    /// Duplicate ticket capability appeared in one commit input.
    #[error("governance commit contains a duplicate ticket")]
    DuplicateTicket,
    /// Dual approval attempted to reuse one principal.
    #[error("governance commit requires distinct principals")]
    DuplicatePrincipal,
    /// The supplied number of tickets does not satisfy the exact preview threshold.
    #[error("governance commit ticket count is incorrect")]
    IncorrectTicketCount,
    /// Bounded state cannot admit more entries.
    #[error("governance state capacity is exhausted")]
    CapacityExceeded,
    /// Random opaque identity allocation did not yield a collision-free value.
    #[error("governance random identity is unavailable")]
    RandomUnavailable,
    /// Trusted time arithmetic failed.
    #[error("governance time is unavailable")]
    TimeUnavailable,
    /// Existing protected secret-store authority failed without backend disclosure.
    #[error("governance secret store is unavailable")]
    SecretStoreUnavailable,
    /// Durable redacted audit could not commit before authority handoff.
    #[error("governance audit is unavailable")]
    AuditUnavailable,
    /// In-memory state locking or an internal invariant was unavailable.
    #[error("governance state is unavailable")]
    StateUnavailable,
}
