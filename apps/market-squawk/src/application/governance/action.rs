use std::{
    fmt,
    num::{NonZeroU8, NonZeroUsize},
    time::Duration,
};

use serde::Serialize;
use thiserror::Error;

use super::identity::{GovernanceEffect, MAXIMUM_DISTINCT_PRINCIPALS};
use super::{
    GovernanceActionDigest, GovernanceActionKind, GovernanceError, GovernancePreviewId,
    GovernancePrincipal, GovernancePrincipalId, GovernanceReceiptId, GovernanceRequestBinding,
    GovernanceRoleSet, GovernanceTicketId, GovernanceTimestamp,
};

/// Stable, bounded presentation view of one admitted principal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GovernancePrincipalSummary {
    principal_id: GovernancePrincipalId,
    display_name: Box<str>,
    roles: GovernanceRoleSet,
}

impl From<&GovernancePrincipal> for GovernancePrincipalSummary {
    fn from(value: &GovernancePrincipal) -> Self {
        Self {
            principal_id: value.id,
            display_name: value.display_name.clone(),
            roles: value.roles.clone(),
        }
    }
}

impl GovernancePrincipalSummary {
    /// Stable server-held principal ID.
    #[must_use]
    pub const fn principal_id(&self) -> GovernancePrincipalId {
        self.principal_id
    }

    /// Configured display name returned only by bounded principal discovery.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// Server-admitted roles that determine preview eligibility.
    #[must_use]
    pub const fn roles(&self) -> &GovernanceRoleSet {
        &self.roles
    }
}

/// One bounded page for `Governance.ListPrincipals`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GovernancePrincipalPage {
    pub(super) principals: Box<[GovernancePrincipalSummary]>,
    pub(super) next_after: Option<GovernancePrincipalId>,
}

impl GovernancePrincipalPage {
    /// Principal summaries selected by the opaque cursor.
    #[must_use]
    pub const fn principals(&self) -> &[GovernancePrincipalSummary] {
        &self.principals
    }

    /// Cursor for the following bounded page.
    #[must_use]
    pub const fn next_after(&self) -> Option<GovernancePrincipalId> {
        self.next_after
    }
}

/// Server-canonical preview parameters. No content, actor, or timestamp is accepted at commit.
#[derive(Debug)]
pub struct GovernancePreviewRequest {
    pub(super) kind: GovernanceActionKind,
    pub(super) binding: GovernanceRequestBinding,
    pub(super) digest: GovernanceActionDigest,
    pub(super) required_roles: GovernanceRoleSet,
    pub(super) distinct_principal_count: u8,
    pub(super) eligible_principal_ids: Box<[GovernancePrincipalId]>,
    pub(super) requested_lifetime: Duration,
}

impl GovernancePreviewRequest {
    /// Constructs bounded preview metadata from an already persisted canonical domain action.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        kind: GovernanceActionKind,
        binding: GovernanceRequestBinding,
        digest: GovernanceActionDigest,
        required_roles: GovernanceRoleSet,
        distinct_principal_count: u8,
        eligible_principal_ids: impl IntoIterator<Item = GovernancePrincipalId>,
        requested_lifetime: Duration,
    ) -> Result<Self, GovernanceError> {
        if !(1..=MAXIMUM_DISTINCT_PRINCIPALS).contains(&distinct_principal_count)
            || requested_lifetime.is_zero()
        {
            return Err(GovernanceError::InvalidPreview);
        }
        let mut eligible_principal_ids = eligible_principal_ids.into_iter().collect::<Vec<_>>();
        if eligible_principal_ids.len() < usize::from(distinct_principal_count) {
            return Err(GovernanceError::InvalidPreview);
        }
        eligible_principal_ids.sort_unstable();
        if eligible_principal_ids
            .windows(2)
            .any(|values| values[0] == values[1])
        {
            return Err(GovernanceError::InvalidPreview);
        }
        Ok(Self {
            kind,
            binding,
            digest,
            required_roles,
            distinct_principal_count,
            eligible_principal_ids: eligible_principal_ids.into_boxed_slice(),
            requested_lifetime,
        })
    }
}

/// Presentation-safe result of storing one exact domain canonical action for reauthentication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GovernanceActionPreview {
    pub(super) preview_id: GovernancePreviewId,
    pub(super) digest: GovernanceActionDigest,
    pub(super) required_roles: GovernanceRoleSet,
    pub(super) distinct_principal_count: u8,
    pub(super) eligible_principal_ids: Box<[GovernancePrincipalId]>,
    pub(super) expires_at: GovernanceTimestamp,
    pub(super) effects: Box<[GovernanceEffect]>,
}

impl GovernanceActionPreview {
    /// Opaque identifier that the domain uses to retrieve its stored canonical action.
    #[must_use]
    pub const fn preview_id(&self) -> GovernancePreviewId {
        self.preview_id
    }

    /// Exact digest of the stored canonical action.
    #[must_use]
    pub const fn digest(&self) -> GovernanceActionDigest {
        self.digest
    }

    /// Roles every authorization ticket must derive from its admitted principal.
    #[must_use]
    pub const fn required_roles(&self) -> &GovernanceRoleSet {
        &self.required_roles
    }

    /// Exact one- or two-principal threshold.
    #[must_use]
    pub const fn distinct_principal_count(&self) -> u8 {
        self.distinct_principal_count
    }

    /// Server-validated eligible principals; no display or arbitrary actor is returned here.
    #[must_use]
    pub const fn eligible_principal_ids(&self) -> &[GovernancePrincipalId] {
        &self.eligible_principal_ids
    }

    /// Lossless server-derived preview expiry.
    #[must_use]
    pub const fn expires_at(&self) -> GovernanceTimestamp {
        self.expires_at
    }

    /// Closed presentation effects with no domain content identifiers.
    #[must_use]
    pub const fn effects(&self) -> &[GovernanceEffect] {
        &self.effects
    }
}

/// One native/service-only authentication result. Its opaque ticket is never a WebView secret.
#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GovernanceAuthenticationTicket {
    pub(super) ticket_id: GovernanceTicketId,
    pub(super) preview_id: GovernancePreviewId,
    pub(super) principal_id: GovernancePrincipalId,
    pub(super) expires_at: GovernanceTimestamp,
}

impl GovernanceAuthenticationTicket {
    /// Opaque one-use capability identifier.
    #[must_use]
    pub const fn ticket_id(&self) -> GovernanceTicketId {
        self.ticket_id
    }

    /// Exact preview accepted by this ticket.
    #[must_use]
    pub const fn preview_id(&self) -> GovernancePreviewId {
        self.preview_id
    }

    /// Server-admitted principal identifier; no caller actor string exists.
    #[must_use]
    pub const fn principal_id(&self) -> GovernancePrincipalId {
        self.principal_id
    }

    /// Short server-derived ticket expiry.
    #[must_use]
    pub const fn expires_at(&self) -> GovernanceTimestamp {
        self.expires_at
    }
}

impl fmt::Debug for GovernanceAuthenticationTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GovernanceAuthenticationTicket")
            .field("ticket_id", &"[REDACTED ONE-USE CAPABILITY]")
            .field("preview_id", &self.preview_id)
            .field("principal_id", &self.principal_id)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Derived principal/role proof delivered to a domain only after ticket consumption.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GovernanceAuthorizedPrincipal {
    pub(super) principal_id: GovernancePrincipalId,
    pub(super) roles: GovernanceRoleSet,
}

impl GovernanceAuthorizedPrincipal {
    /// Server-derived stable principal identity.
    #[must_use]
    pub const fn principal_id(&self) -> GovernancePrincipalId {
        self.principal_id
    }

    /// Exact role snapshot bound into the consumed ticket.
    #[must_use]
    pub const fn roles(&self) -> &GovernanceRoleSet {
        &self.roles
    }
}

/// Durable redacted receipt for generic authorization, before domain-specific mutation finality.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GovernanceCommitReceipt {
    pub(super) receipt_id: GovernanceReceiptId,
    pub(super) preview_id: GovernancePreviewId,
    pub(super) digest: GovernanceActionDigest,
    pub(super) committed_at: GovernanceTimestamp,
    pub(super) authorized_principals: Box<[GovernanceAuthorizedPrincipal]>,
    pub(super) effects: Box<[GovernanceEffect]>,
}

impl GovernanceCommitReceipt {
    /// Durable generic-authorization receipt identity.
    #[must_use]
    pub const fn receipt_id(&self) -> GovernanceReceiptId {
        self.receipt_id
    }

    /// Canonical action preview that was authorized.
    #[must_use]
    pub const fn preview_id(&self) -> GovernancePreviewId {
        self.preview_id
    }

    /// Server-canonical action digest.
    #[must_use]
    pub const fn digest(&self) -> GovernanceActionDigest {
        self.digest
    }

    /// Server-derived authorization time.
    #[must_use]
    pub const fn committed_at(&self) -> GovernanceTimestamp {
        self.committed_at
    }

    /// Server-derived distinct principals and roles.
    #[must_use]
    pub const fn authorized_principals(&self) -> &[GovernanceAuthorizedPrincipal] {
        &self.authorized_principals
    }

    /// Closed action effects.
    #[must_use]
    pub const fn effects(&self) -> &[GovernanceEffect] {
        &self.effects
    }
}

/// Durable, payload-free governance audit entry. Credential, display name, and action bytes never
/// enter this record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GovernanceAuditReceipt {
    pub(super) receipt_id: GovernanceReceiptId,
    pub(super) kind: GovernanceAuditKind,
    pub(super) preview_id: GovernancePreviewId,
    pub(super) binding: GovernanceRequestBinding,
    pub(super) digest: GovernanceActionDigest,
    pub(super) required_roles: GovernanceRoleSet,
    pub(super) principal_ids: Box<[GovernancePrincipalId]>,
    pub(super) occurred_at: GovernanceTimestamp,
}

/// Closed generic audit state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum GovernanceAuditKind {
    /// A protected credential authenticated one eligible principal for an exact preview.
    AuthenticationTicketIssued,
    /// Exact tickets were consumed and authorization authority was handed to the domain.
    CommitAuthorized,
}

/// The composition-owned durable sink. It must append and sync before an authorization is returned.
pub trait GovernanceAuditSink: fmt::Debug + Send + Sync {
    /// Persists one redacted receipt or returns a non-sensitive availability error.
    fn append(&self, receipt: &GovernanceAuditReceipt) -> Result<(), GovernanceAuditError>;
}

/// Durable audit failure without storage implementation detail disclosure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GovernanceAuditError {
    /// The durable local audit authority could not admit the record.
    #[error("governance audit is unavailable")]
    Unavailable,
}

/// Bounded state and reauthentication policy for one service generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GovernanceLimits {
    pub(super) maximum_principals: NonZeroUsize,
    pub(super) maximum_previews: NonZeroUsize,
    pub(super) maximum_tickets: NonZeroUsize,
    pub(super) maximum_reauthentication_attempts: NonZeroU8,
    pub(super) maximum_preview_lifetime: Duration,
    pub(super) maximum_ticket_lifetime: Duration,
}

impl GovernanceLimits {
    /// Production-safe local default; callers may only choose smaller operational bounds.
    #[must_use]
    pub fn standard() -> Result<Self, GovernanceError> {
        Self::try_new(
            64,
            256,
            512,
            3,
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
        )
    }

    /// Validates all configured state and lifetime ceilings.
    pub fn try_new(
        maximum_principals: usize,
        maximum_previews: usize,
        maximum_tickets: usize,
        maximum_reauthentication_attempts: u8,
        maximum_preview_lifetime: Duration,
        maximum_ticket_lifetime: Duration,
    ) -> Result<Self, GovernanceError> {
        let maximum_principals =
            NonZeroUsize::new(maximum_principals).ok_or(GovernanceError::InvalidLimits)?;
        let maximum_previews =
            NonZeroUsize::new(maximum_previews).ok_or(GovernanceError::InvalidLimits)?;
        let maximum_tickets =
            NonZeroUsize::new(maximum_tickets).ok_or(GovernanceError::InvalidLimits)?;
        let maximum_reauthentication_attempts = NonZeroU8::new(maximum_reauthentication_attempts)
            .ok_or(GovernanceError::InvalidLimits)?;
        if maximum_preview_lifetime.is_zero()
            || maximum_ticket_lifetime.is_zero()
            || maximum_ticket_lifetime > maximum_preview_lifetime
        {
            return Err(GovernanceError::InvalidLimits);
        }
        Ok(Self {
            maximum_principals,
            maximum_previews,
            maximum_tickets,
            maximum_reauthentication_attempts,
            maximum_preview_lifetime,
            maximum_ticket_lifetime,
        })
    }
}
