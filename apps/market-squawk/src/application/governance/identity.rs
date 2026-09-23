//! Local governance-principal authority for sensitive application actions.

use std::{
    fmt,
    time::{Duration, Instant},
};

use chrono::{DateTime, SecondsFormat, Utc};
use market_squawk_domain::Timestamp;
use market_squawk_platform::{
    LocalSecretStoreError, SecretCancellation, SecretInteractionPolicy, SecretKey,
    SecretOperationControl, SecretRef, SecretValue,
};
use market_squawk_runtime::{ClientId, ServiceGeneration, WorkspaceId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::GovernanceError;

pub(super) const GOVERNANCE_SECRET_SCOPE: &str = "governance-principal";
pub(super) const SECRET_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const MAXIMUM_PRINCIPAL_DISPLAY_BYTES: usize = 128;
const MAXIMUM_ROLES: usize = 8;
pub(super) const MAXIMUM_DISTINCT_PRINCIPALS: u8 = 2;

pub(crate) fn governance_principal_secret_key(
    id: GovernancePrincipalId,
) -> Result<SecretKey, GovernanceError> {
    SecretKey::try_new(GOVERNANCE_SECRET_SCOPE, &id.as_uuid().to_string()).map_err(map_secret_error)
}

pub(crate) fn governance_secret_operation_control(
    owner: &'static str,
) -> Result<SecretOperationControl, GovernanceError> {
    let deadline = Instant::now()
        .checked_add(SECRET_OPERATION_TIMEOUT)
        .ok_or(GovernanceError::SecretStoreUnavailable)?;
    SecretOperationControl::try_new(
        owner,
        deadline,
        1,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )
    .map_err(map_secret_error)
}

fn map_secret_error(_error: LocalSecretStoreError) -> GovernanceError {
    GovernanceError::SecretStoreUnavailable
}

macro_rules! uuid_identity {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Admits one non-nil opaque identifier.
            pub fn try_from_uuid(value: Uuid) -> Result<Self, GovernanceError> {
                if value.is_nil() {
                    Err(GovernanceError::InvalidIdentity)
                } else {
                    Ok(Self(value))
                }
            }

            /// Returns the UUID value for durable local storage or a typed transport DTO.
            #[must_use]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }
    };
}

uuid_identity!(/// Stable, locally admitted human principal identity.
    GovernancePrincipalId);
uuid_identity!(/// Opaque server-generated identifier for one canonical action preview.
    GovernancePreviewId);
uuid_identity!(/// Opaque one-use service authorization capability.
    GovernanceTicketId);
uuid_identity!(/// Opaque durable receipt identifier.
    GovernanceReceiptId);

/// Closed human roles allowed to govern V1 sensitive actions.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum GovernanceRole {
    /// May review an investment-target revision.
    DecisionReviewer,
    /// May append decision invalidation evidence.
    DecisionInvalidator,
    /// May approve a fair-value decision.
    FairValueApprover,
    /// May approve a fair-value override.
    FairValueOverrideApprover,
    /// May revoke a fair-value approval.
    FairValueRevoker,
    /// May independently approve a fair-value market-access assessment.
    FairValueMarketAccessApprover,
    /// May resolve an ambiguous locally imported portfolio record.
    PortfolioImportResolver,
}

/// Canonical sorted, non-empty role set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct GovernanceRoleSet(Box<[GovernanceRole]>);

impl GovernanceRoleSet {
    /// Admits a small, unique role set.
    pub fn try_new(
        roles: impl IntoIterator<Item = GovernanceRole>,
    ) -> Result<Self, GovernanceError> {
        let mut roles = roles.into_iter().collect::<Vec<_>>();
        if roles.is_empty() || roles.len() > MAXIMUM_ROLES {
            return Err(GovernanceError::InvalidRoleSet);
        }
        roles.sort_unstable();
        if roles.windows(2).any(|values| values[0] == values[1]) {
            return Err(GovernanceError::InvalidRoleSet);
        }
        Ok(Self(roles.into_boxed_slice()))
    }

    /// Returns the canonical roles.
    #[must_use]
    pub const fn as_slice(&self) -> &[GovernanceRole] {
        &self.0
    }

    pub(super) fn includes_all(&self, required: &Self) -> bool {
        required
            .0
            .iter()
            .all(|role| self.0.binary_search(role).is_ok())
    }
}

/// Closed domain-neutral effect family of a canonical action.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum GovernanceActionKind {
    /// Approves or rejects an investment-target review.
    DecisionReview,
    /// Appends decision invalidation evidence.
    DecisionInvalidation,
    /// Approves a fair-value decision.
    FairValueApproval,
    /// Approves a fair-value hierarchy override.
    FairValueOverride,
    /// Revokes an active fair-value approval.
    FairValueApprovalRevocation,
    /// Approves one fair-value market-access assessment with two distinct principals.
    FairValueMarketAccess,
    /// Resolves an ambiguous imported trade or income record.
    PortfolioImportResolution,
}

/// Closed presentation-safe action effect. Canonical content remains only in the domain store.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GovernanceEffect {
    kind: GovernanceActionKind,
}

impl GovernanceEffect {
    pub(super) fn for_action(kind: GovernanceActionKind) -> Self {
        Self { kind }
    }

    /// Closed action family represented by this effect.
    #[must_use]
    pub const fn kind(self) -> GovernanceActionKind {
        self.kind
    }
}

/// SHA-256 of the full server-canonical action bytes.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct GovernanceActionDigest([u8; 32]);

impl GovernanceActionDigest {
    /// Binds an action to an already computed exact SHA-256 digest.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the digest bytes for trusted domain persistence only.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }

    fn hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(64);
        for byte in self.0 {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        encoded
    }
}

impl fmt::Debug for GovernanceActionDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GovernanceActionDigest([REDACTED])")
    }
}

impl Serialize for GovernanceActionDigest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.hex())
    }
}

/// Lossless, RFC 3339 UTC nanosecond timestamp used only in governance responses and receipts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GovernanceTimestamp(Timestamp);

impl GovernanceTimestamp {
    pub(super) fn from_timestamp(value: Timestamp) -> Self {
        Self(value)
    }

    /// Returns the trusted timestamp for domain record construction.
    #[must_use]
    pub const fn timestamp(self) -> Timestamp {
        self.0
    }
}

impl Serialize for GovernanceTimestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value = DateTime::<Utc>::from_timestamp_nanos(self.0.unix_nanos())
            .to_rfc3339_opts(SecondsFormat::Nanos, true);
        serializer.serialize_str(&value)
    }
}

/// Exact runtime scope carried into every preview and ticket.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GovernanceRequestBinding {
    workspace_id: WorkspaceId,
    service_generation: ServiceGeneration,
    client_id: ClientId,
}

impl GovernanceRequestBinding {
    /// Creates an exact authenticated workspace, generation, and named-client binding.
    pub const fn try_new(
        workspace_id: WorkspaceId,
        service_generation: ServiceGeneration,
        client_id: ClientId,
    ) -> Result<Self, GovernanceError> {
        Ok(Self {
            workspace_id,
            service_generation,
            client_id,
        })
    }

    /// Active workspace identity.
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    /// Exact service generation.
    #[must_use]
    pub const fn service_generation(self) -> ServiceGeneration {
        self.service_generation
    }

    /// Exact authenticated native client.
    #[must_use]
    pub const fn client_id(self) -> ClientId {
        self.client_id
    }
}

/// Locally admitted principal profile without a credential or opaque secret locator.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GovernancePrincipal {
    pub(super) id: GovernancePrincipalId,
    pub(super) display_name: Box<str>,
    pub(super) roles: GovernanceRoleSet,
}

impl GovernancePrincipal {
    /// Validates the durable locally configured display name and role set.
    pub fn try_new(
        id: GovernancePrincipalId,
        display_name: impl Into<String>,
        roles: GovernanceRoleSet,
    ) -> Result<Self, GovernanceError> {
        let display_name = display_name.into();
        if display_name.is_empty()
            || display_name.len() > MAXIMUM_PRINCIPAL_DISPLAY_BYTES
            || display_name.trim() != display_name
            || display_name.chars().any(char::is_control)
        {
            return Err(GovernanceError::InvalidPrincipal);
        }
        Ok(Self {
            id,
            display_name: display_name.into_boxed_str(),
            roles,
        })
    }

    /// Stable server-held principal ID.
    #[must_use]
    pub const fn id(&self) -> GovernancePrincipalId {
        self.id
    }

    /// Configured display name for the bounded principal list only.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// Server-admitted roles.
    #[must_use]
    pub const fn roles(&self) -> &GovernanceRoleSet {
        &self.roles
    }
}

/// Durable non-secret registration required to reload an admitted principal after restart.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GovernancePrincipalRegistration {
    pub(super) principal: GovernancePrincipal,
    pub(super) credential: SecretRef,
}

impl GovernancePrincipalRegistration {
    /// Creates durable registration metadata after a protected credential provision succeeds.
    #[must_use]
    pub const fn new(principal: GovernancePrincipal, credential: SecretRef) -> Self {
        Self {
            principal,
            credential,
        }
    }

    /// Registered profile.
    #[must_use]
    pub const fn principal(&self) -> &GovernancePrincipal {
        &self.principal
    }

    /// Exact opaque local-secret generation reference.
    #[must_use]
    pub const fn credential(&self) -> &SecretRef {
        &self.credential
    }
}

/// Protected first-admission input. It is deliberately not serializable or debug-printable.
pub struct GovernancePrincipalAdmission {
    pub(super) principal: GovernancePrincipal,
    pub(super) credential: SecretValue,
}

impl GovernancePrincipalAdmission {
    /// Pairs an admitted profile with a protected native credential exactly once.
    #[must_use]
    pub const fn new(principal: GovernancePrincipal, credential: SecretValue) -> Self {
        Self {
            principal,
            credential,
        }
    }
}

impl fmt::Debug for GovernancePrincipalAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GovernancePrincipalAdmission")
            .field("principal", &self.principal)
            .field("credential", &"[REDACTED]")
            .finish()
    }
}
