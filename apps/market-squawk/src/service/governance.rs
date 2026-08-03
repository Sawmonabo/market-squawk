//! Transport-neutral governed-action orchestration over canonical decision and fair-value ports.
//!
//! Native code owns protected credentials and maps opaque window-scoped authorization handles to
//! `GovernanceAuthenticationTicket` values. This module deliberately receives the resolved ticket
//! values only at commit; WebView input cannot submit actor, role, time, digest, or action content.

use std::{
    collections::HashMap,
    fmt,
    num::NonZeroUsize,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use market_squawk_domain::Timestamp;
use market_squawk_platform::{SecretStore, SecretValue};
use market_squawk_runtime::{ClientId, RuntimeIdentity};
use market_squawk_services::{RequestContext, ServiceError, ToolResultMetadata, TypedToolResult};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use thiserror::Error;
use uuid::Uuid;

use super::governance_persistence::{
    GovernancePersistence, GovernancePersistenceError, GovernanceProvisioningRequest,
};
use crate::application::governance::{
    CanonicalGovernanceAction, DecisionGovernanceActionFactory, DecisionInvalidationKind,
    DecisionInvalidationProposal, DecisionReviewDisposition, DecisionReviewProposal,
    FairValueApprovalProposal, FairValueGovernanceActionFactory, FairValueMarketAccessConclusion,
    FairValueMarketAccessProposal, FairValueOverrideProposal, FairValueRequestedHierarchy,
    FairValueRevocationProposal, GovernanceActionKind, GovernanceActionPreview,
    GovernanceAuthenticationTicket, GovernanceAuthority, GovernanceCommitReceipt,
    GovernanceDomainAdapterError, GovernanceError, GovernanceLimits, GovernancePreviewId,
    GovernancePreviewRequest, GovernancePrincipalId, GovernancePrincipalPage,
    GovernanceRequestBinding, GovernanceRole, GovernanceRoleSet, GovernanceTicketId,
    GovernedActionCommitReceipt,
};

/// Bounded state and expiry policy for the domain action side of governance previews.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GovernedActionServiceLimits {
    maximum_pending_actions: NonZeroUsize,
    maximum_principal_scan: NonZeroUsize,
    preview_lifetime: Duration,
}

impl GovernedActionServiceLimits {
    /// Production-safe policy aligned with the generic governance authority defaults.
    pub(crate) fn standard() -> Result<Self, GovernedActionServiceError> {
        Self::try_new(256, 64, Duration::from_secs(5 * 60))
    }

    /// Validates bounded domain-action state and a nonzero preview lifetime.
    pub(crate) fn try_new(
        maximum_pending_actions: usize,
        maximum_principal_scan: usize,
        preview_lifetime: Duration,
    ) -> Result<Self, GovernedActionServiceError> {
        let maximum_pending_actions = NonZeroUsize::new(maximum_pending_actions)
            .ok_or(GovernedActionServiceError::InvalidLimits)?;
        let maximum_principal_scan = NonZeroUsize::new(maximum_principal_scan)
            .ok_or(GovernedActionServiceError::InvalidLimits)?;
        if preview_lifetime.is_zero() {
            return Err(GovernedActionServiceError::InvalidLimits);
        }
        Ok(Self {
            maximum_pending_actions,
            maximum_principal_scan,
            preview_lifetime,
        })
    }
}

/// Native-resolved reauthentication request. Its credential is never serializable or debugged.
pub(crate) struct GovernanceAuthenticationRequest {
    preview_id: GovernancePreviewId,
    principal_id: GovernancePrincipalId,
    credential: SecretValue,
}

impl GovernanceAuthenticationRequest {
    /// Binds one native-protected credential to one exact preview and admitted principal.
    #[must_use]
    pub(crate) const fn new(
        preview_id: GovernancePreviewId,
        principal_id: GovernancePrincipalId,
        credential: SecretValue,
    ) -> Self {
        Self {
            preview_id,
            principal_id,
            credential,
        }
    }
}

impl fmt::Debug for GovernanceAuthenticationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GovernanceAuthenticationRequest")
            .field("preview_id", &self.preview_id)
            .field("principal_id", &self.principal_id)
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

/// Native-resolved commit input. Its tickets derive only from window-scoped authorization handles.
pub(crate) struct GovernanceActionCommitRequest {
    preview_id: GovernancePreviewId,
    ticket_ids: Box<[GovernanceTicketId]>,
}

impl GovernanceActionCommitRequest {
    /// Retains one exact preview plus its native-held one-use tickets for immediate commit.
    pub(crate) fn try_new(
        preview_id: GovernancePreviewId,
        ticket_ids: impl IntoIterator<Item = GovernanceTicketId>,
    ) -> Result<Self, GovernedActionServiceError> {
        let ticket_ids = ticket_ids.into_iter().collect::<Vec<_>>();
        if ticket_ids.is_empty() || ticket_ids.len() > 2 {
            return Err(GovernedActionServiceError::InvalidCommitInput);
        }
        Ok(Self {
            preview_id,
            ticket_ids: ticket_ids.into_boxed_slice(),
        })
    }
}

impl fmt::Debug for GovernanceActionCommitRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GovernanceActionCommitRequest")
            .field("preview_id", &self.preview_id)
            .field("tickets", &"[REDACTED ONE-USE CAPABILITIES]")
            .finish()
    }
}

/// One operational service holding canonical prepared actions only until their exact preview ends.
pub(crate) struct GovernedActionService {
    authority: Arc<GovernanceAuthority>,
    decisions: Arc<dyn DecisionGovernanceActionFactory>,
    fair_value: Arc<dyn FairValueGovernanceActionFactory>,
    limits: GovernedActionServiceLimits,
    state: Mutex<GovernedActionState>,
}

impl fmt::Debug for GovernedActionService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GovernedActionService")
            .field("authority", &self.authority)
            .field("decisions", &self.decisions)
            .field("fair_value", &self.fair_value)
            .field("limits", &self.limits)
            .field("state", &"[SERVER-HELD CANONICAL ACTIONS]")
            .finish()
    }
}

impl GovernedActionService {
    /// Binds generic principal authority to canonical decision and fair-value domain adapters.
    pub(crate) fn try_new(
        authority: Arc<GovernanceAuthority>,
        decisions: Arc<dyn DecisionGovernanceActionFactory>,
        fair_value: Arc<dyn FairValueGovernanceActionFactory>,
        limits: GovernedActionServiceLimits,
    ) -> Result<Self, GovernedActionServiceError> {
        let mut actions = HashMap::new();
        actions
            .try_reserve(limits.maximum_pending_actions.get())
            .map_err(|_| GovernedActionServiceError::CapacityExceeded)?;
        Ok(Self {
            authority,
            decisions,
            fair_value,
            limits,
            state: Mutex::new(GovernedActionState { actions }),
        })
    }

    /// Returns one bounded page of locally admitted principals with no credential material.
    pub(crate) fn list_principals(
        &self,
        after: Option<GovernancePrincipalId>,
        limit: NonZeroUsize,
    ) -> Result<GovernancePrincipalPage, GovernedActionServiceError> {
        Ok(self.authority.list_principals(after, limit)?)
    }

    /// Canonicalizes a decision review before creating one exact generic governance preview.
    pub(crate) async fn preview_decision_review(
        &self,
        proposal: DecisionReviewProposal,
        binding: GovernanceRequestBinding,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernanceActionPreview, GovernedActionServiceError> {
        let action = self.decisions.prepare_review(proposal, observed_at).await?;
        self.preview_expected_action(
            GovernanceActionKind::DecisionReview,
            action,
            binding,
            now,
            observed_at,
        )
    }

    /// Canonicalizes a decision invalidation before creating one exact generic governance preview.
    pub(crate) async fn preview_decision_invalidation(
        &self,
        proposal: DecisionInvalidationProposal,
        binding: GovernanceRequestBinding,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernanceActionPreview, GovernedActionServiceError> {
        let action = self
            .decisions
            .prepare_invalidation(proposal, observed_at)
            .await?;
        self.preview_expected_action(
            GovernanceActionKind::DecisionInvalidation,
            action,
            binding,
            now,
            observed_at,
        )
    }

    /// Canonicalizes a fair-value approval before creating one exact generic governance preview.
    pub(crate) async fn preview_fair_value_approval(
        &self,
        proposal: FairValueApprovalProposal,
        binding: GovernanceRequestBinding,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernanceActionPreview, GovernedActionServiceError> {
        let action = self
            .fair_value
            .prepare_approval(proposal, observed_at)
            .await?;
        self.preview_expected_action(
            GovernanceActionKind::FairValueApproval,
            action,
            binding,
            now,
            observed_at,
        )
    }

    /// Canonicalizes a fair-value override before creating one exact generic governance preview.
    pub(crate) async fn preview_fair_value_override(
        &self,
        proposal: FairValueOverrideProposal,
        binding: GovernanceRequestBinding,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernanceActionPreview, GovernedActionServiceError> {
        let action = self
            .fair_value
            .prepare_override(proposal, observed_at)
            .await?;
        self.preview_expected_action(
            GovernanceActionKind::FairValueOverride,
            action,
            binding,
            now,
            observed_at,
        )
    }

    /// Canonicalizes a fair-value revocation before creating one exact generic governance preview.
    pub(crate) async fn preview_fair_value_revocation(
        &self,
        proposal: FairValueRevocationProposal,
        binding: GovernanceRequestBinding,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernanceActionPreview, GovernedActionServiceError> {
        let action = self
            .fair_value
            .prepare_revocation(proposal, observed_at)
            .await?;
        self.preview_expected_action(
            GovernanceActionKind::FairValueApprovalRevocation,
            action,
            binding,
            now,
            observed_at,
        )
    }

    /// Canonicalizes a dual-principal fair-value market-access action before reauthentication.
    pub(crate) async fn preview_fair_value_market_access(
        &self,
        proposal: FairValueMarketAccessProposal,
        binding: GovernanceRequestBinding,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernanceActionPreview, GovernedActionServiceError> {
        let action = self
            .fair_value
            .prepare_market_access(proposal, observed_at)
            .await?;
        self.preview_expected_action(
            GovernanceActionKind::FairValueMarketAccess,
            action,
            binding,
            now,
            observed_at,
        )
    }

    /// Reauthenticates one named admitted principal for an existing exact canonical action preview.
    pub(crate) fn authenticate_action(
        &self,
        request: GovernanceAuthenticationRequest,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernanceAuthenticationTicket, GovernedActionServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| GovernedActionServiceError::StateUnavailable)?;
        state.prune_expired(now);
        match state.actions.get(&request.preview_id) {
            Some(action) if action.disposition == CanonicalActionDisposition::Available => {}
            Some(_) => return Err(GovernedActionServiceError::CanonicalActionCommitInProgress),
            None => return Err(GovernedActionServiceError::CanonicalActionNotFound),
        }
        Ok(self.authority.authenticate_action(
            request.preview_id,
            request.principal_id,
            request.credential,
            now,
            observed_at,
        )?)
    }

    /// Consumes tickets and commits only a retained decision review or invalidation action.
    pub(crate) async fn commit_decision_action(
        &self,
        request: GovernanceActionCommitRequest,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernedActionCommitReceipt, GovernedActionServiceError> {
        self.commit_action(DomainFamily::Decision, request, now, observed_at)
            .await
    }

    /// Consumes tickets and commits only a retained fair-value governed action.
    pub(crate) async fn commit_fair_value_action(
        &self,
        request: GovernanceActionCommitRequest,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernedActionCommitReceipt, GovernedActionServiceError> {
        self.commit_action(DomainFamily::FairValue, request, now, observed_at)
            .await
    }

    fn preview_expected_action(
        &self,
        expected_kind: GovernanceActionKind,
        action: Arc<dyn CanonicalGovernanceAction>,
        binding: GovernanceRequestBinding,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernanceActionPreview, GovernedActionServiceError> {
        if action.kind() != expected_kind {
            return Err(GovernedActionServiceError::CanonicalActionKindMismatch);
        }
        self.preview_action(action, binding, now, observed_at)
    }

    fn preview_action(
        &self,
        action: Arc<dyn CanonicalGovernanceAction>,
        binding: GovernanceRequestBinding,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernanceActionPreview, GovernedActionServiceError> {
        let policy = GovernanceActionPolicy::for_kind(action.kind())?;
        let eligible_principal_ids = self.eligible_principal_ids(&policy.required_roles)?;
        if eligible_principal_ids.len() < usize::from(policy.distinct_principal_count) {
            return Err(GovernedActionServiceError::InsufficientEligiblePrincipals);
        }
        let expires_at = now
            .checked_add(self.limits.preview_lifetime)
            .ok_or(GovernedActionServiceError::TimeUnavailable)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| GovernedActionServiceError::StateUnavailable)?;
        state.prune_expired(now);
        if state.actions.len() >= self.limits.maximum_pending_actions.get() {
            return Err(GovernedActionServiceError::CapacityExceeded);
        }
        let preview = self.authority.preview_action(
            GovernancePreviewRequest::try_new(
                action.kind(),
                binding,
                action.digest(),
                policy.required_roles,
                policy.distinct_principal_count,
                eligible_principal_ids,
                self.limits.preview_lifetime,
            )?,
            now,
            observed_at,
        )?;
        state.actions.insert(
            preview.preview_id(),
            RetainedCanonicalAction {
                family: DomainFamily::for_kind(action.kind())?,
                action,
                expires_at,
                disposition: CanonicalActionDisposition::Available,
            },
        );
        Ok(preview)
    }

    async fn commit_action(
        &self,
        expected_family: DomainFamily,
        request: GovernanceActionCommitRequest,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernedActionCommitReceipt, GovernedActionServiceError> {
        let retained = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| GovernedActionServiceError::StateUnavailable)?;
            state.prune_expired(now);
            let retained = state
                .actions
                .get_mut(&request.preview_id)
                .ok_or(GovernedActionServiceError::CanonicalActionNotFound)?;
            if retained.family != expected_family {
                return Err(GovernedActionServiceError::CanonicalActionFamilyMismatch);
            }
            if retained.disposition == CanonicalActionDisposition::Committing {
                return Err(GovernedActionServiceError::CanonicalActionCommitInProgress);
            }
            retained.disposition = CanonicalActionDisposition::Committing;
            retained.clone()
        };
        let authorization = match self.authority.commit_action_ids(
            request.preview_id,
            request.ticket_ids,
            now,
            observed_at,
        ) {
            Ok(authorization) => authorization,
            Err(error) => {
                self.release_failed_commit(request.preview_id)?;
                return Err(error.into());
            }
        };
        if !receipt_matches_action(&authorization, retained.action.as_ref()) {
            self.discard_committing_action(authorization.preview_id())?;
            return Err(GovernedActionServiceError::CanonicalActionMismatch);
        }
        let retained = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| GovernedActionServiceError::StateUnavailable)?;
            let retained = state
                .actions
                .remove(&authorization.preview_id())
                .ok_or(GovernedActionServiceError::CanonicalActionNotFound)?;
            if retained.disposition != CanonicalActionDisposition::Committing {
                return Err(GovernedActionServiceError::CanonicalActionMismatch);
            }
            retained
        };
        let domain = retained.action.commit(&authorization).await?;
        Ok(GovernedActionCommitReceipt::try_new(authorization, domain)?)
    }

    fn release_failed_commit(
        &self,
        preview_id: GovernancePreviewId,
    ) -> Result<(), GovernedActionServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| GovernedActionServiceError::StateUnavailable)?;
        let action = state
            .actions
            .get_mut(&preview_id)
            .ok_or(GovernedActionServiceError::CanonicalActionNotFound)?;
        if action.disposition != CanonicalActionDisposition::Committing {
            return Err(GovernedActionServiceError::CanonicalActionMismatch);
        }
        action.disposition = CanonicalActionDisposition::Available;
        Ok(())
    }

    fn discard_committing_action(
        &self,
        preview_id: GovernancePreviewId,
    ) -> Result<(), GovernedActionServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| GovernedActionServiceError::StateUnavailable)?;
        let action = state
            .actions
            .remove(&preview_id)
            .ok_or(GovernedActionServiceError::CanonicalActionNotFound)?;
        if action.disposition != CanonicalActionDisposition::Committing {
            return Err(GovernedActionServiceError::CanonicalActionMismatch);
        }
        Ok(())
    }

    fn eligible_principal_ids(
        &self,
        required_roles: &GovernanceRoleSet,
    ) -> Result<Vec<GovernancePrincipalId>, GovernedActionServiceError> {
        let mut eligible = Vec::new();
        eligible
            .try_reserve(self.limits.maximum_principal_scan.get())
            .map_err(|_| GovernedActionServiceError::CapacityExceeded)?;
        let mut after = None;
        for _ in 0..self.limits.maximum_principal_scan.get() {
            let page = self.authority.list_principals(after, NonZeroUsize::MIN)?;
            let Some(principal) = page.principals().first() else {
                return Ok(eligible);
            };
            if roles_include_all(principal.roles(), required_roles) {
                eligible.push(principal.principal_id());
            }
            after = page.next_after();
            if after.is_none() {
                return Ok(eligible);
            }
        }
        Err(GovernedActionServiceError::CapacityExceeded)
    }
}

struct GovernedActionState {
    actions: HashMap<GovernancePreviewId, RetainedCanonicalAction>,
}

impl GovernedActionState {
    fn prune_expired(&mut self, now: Instant) {
        self.actions.retain(|_, action| now < action.expires_at);
    }
}

#[derive(Clone)]
struct RetainedCanonicalAction {
    family: DomainFamily,
    action: Arc<dyn CanonicalGovernanceAction>,
    expires_at: Instant,
    disposition: CanonicalActionDisposition,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CanonicalActionDisposition {
    Available,
    Committing,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum DomainFamily {
    Decision,
    FairValue,
}

impl DomainFamily {
    fn for_kind(kind: GovernanceActionKind) -> Result<Self, GovernedActionServiceError> {
        match kind {
            GovernanceActionKind::DecisionReview | GovernanceActionKind::DecisionInvalidation => {
                Ok(Self::Decision)
            }
            GovernanceActionKind::FairValueApproval
            | GovernanceActionKind::FairValueOverride
            | GovernanceActionKind::FairValueApprovalRevocation
            | GovernanceActionKind::FairValueMarketAccess => Ok(Self::FairValue),
            GovernanceActionKind::PortfolioImportResolution => {
                Err(GovernedActionServiceError::UnsupportedAction)
            }
        }
    }
}

struct GovernanceActionPolicy {
    required_roles: GovernanceRoleSet,
    distinct_principal_count: u8,
}

impl GovernanceActionPolicy {
    fn for_kind(kind: GovernanceActionKind) -> Result<Self, GovernedActionServiceError> {
        let (role, distinct_principal_count) = match kind {
            GovernanceActionKind::DecisionReview => (GovernanceRole::DecisionReviewer, 1),
            GovernanceActionKind::DecisionInvalidation => (GovernanceRole::DecisionInvalidator, 1),
            GovernanceActionKind::FairValueApproval => (GovernanceRole::FairValueApprover, 1),
            GovernanceActionKind::FairValueOverride => {
                (GovernanceRole::FairValueOverrideApprover, 1)
            }
            GovernanceActionKind::FairValueApprovalRevocation => {
                (GovernanceRole::FairValueRevoker, 1)
            }
            GovernanceActionKind::FairValueMarketAccess => {
                (GovernanceRole::FairValueMarketAccessApprover, 2)
            }
            GovernanceActionKind::PortfolioImportResolution => {
                return Err(GovernedActionServiceError::UnsupportedAction);
            }
        };
        Ok(Self {
            required_roles: GovernanceRoleSet::try_new([role])?,
            distinct_principal_count,
        })
    }
}

fn roles_include_all(admitted: &GovernanceRoleSet, required: &GovernanceRoleSet) -> bool {
    required
        .as_slice()
        .iter()
        .all(|role| admitted.as_slice().binary_search(role).is_ok())
}

fn receipt_matches_action(
    authorization: &GovernanceCommitReceipt,
    action: &dyn CanonicalGovernanceAction,
) -> bool {
    authorization.digest() == action.digest()
        && authorization.effects().len() == 1
        && authorization.effects()[0].kind() == action.kind()
}

const LIST_PRINCIPALS: &str = "Governance.ListPrincipals";
const PROVISIONING_STATUS: &str = "Governance.ProvisioningStatus";
const PROVISION_PRINCIPAL_SET: &str = "Governance.ProvisionPrincipalSet";
const AUTHENTICATE_ACTION: &str = "Governance.AuthenticateAction";
const PREVIEW_DECISION_ACTION: &str = "Decision.PreviewGovernanceAction";
const COMMIT_DECISION_ACTION: &str = "Decision.CommitGovernanceAction";
const PREVIEW_FAIR_VALUE_ACTION: &str = "FairValue.PreviewGovernanceAction";
const COMMIT_FAIR_VALUE_ACTION: &str = "FairValue.CommitGovernanceAction";

/// Private installed-client adapter over one optional configured governance authority.
pub(crate) struct InstalledGovernanceOperations {
    actions: RwLock<Option<Arc<GovernedActionService>>>,
    persistence: Arc<GovernancePersistence>,
    secrets: Arc<dyn SecretStore>,
    decisions: Arc<dyn DecisionGovernanceActionFactory>,
    fair_value: Arc<dyn FairValueGovernanceActionFactory>,
    authority_limits: GovernanceLimits,
    action_limits: GovernedActionServiceLimits,
    runtime: RuntimeIdentity,
    desktop_client: ClientId,
}

pub(super) struct InstalledGovernanceComposition {
    pub(super) actions: Option<Arc<GovernedActionService>>,
    pub(super) persistence: Arc<GovernancePersistence>,
    pub(super) secrets: Arc<dyn SecretStore>,
    pub(super) decisions: Arc<dyn DecisionGovernanceActionFactory>,
    pub(super) fair_value: Arc<dyn FairValueGovernanceActionFactory>,
    pub(super) authority_limits: GovernanceLimits,
    pub(super) action_limits: GovernedActionServiceLimits,
}

impl InstalledGovernanceOperations {
    pub(crate) fn new(
        composition: InstalledGovernanceComposition,
        runtime: RuntimeIdentity,
        desktop_client: ClientId,
    ) -> Self {
        Self {
            actions: RwLock::new(composition.actions),
            persistence: composition.persistence,
            secrets: composition.secrets,
            decisions: composition.decisions,
            fair_value: composition.fair_value,
            authority_limits: composition.authority_limits,
            action_limits: composition.action_limits,
            runtime,
            desktop_client,
        }
    }

    pub(crate) fn is_configured(&self) -> bool {
        match self.actions.read() {
            Ok(actions) => actions.is_some(),
            Err(_error) => false,
        }
    }

    pub(crate) fn owns(operation: &str) -> bool {
        matches!(
            operation,
            PROVISIONING_STATUS
                | PROVISION_PRINCIPAL_SET
                | LIST_PRINCIPALS
                | AUTHENTICATE_ACTION
                | PREVIEW_DECISION_ACTION
                | COMMIT_DECISION_ACTION
                | PREVIEW_FAIR_VALUE_ACTION
                | COMMIT_FAIR_VALUE_ACTION
        )
    }

    pub(crate) fn is_mutation(operation: &str) -> bool {
        !matches!(operation, PROVISIONING_STATUS | LIST_PRINCIPALS)
    }

    pub(crate) fn desktop_capabilities(&self) -> Vec<Value> {
        [
            (PROVISIONING_STATUS, true, false),
            (PROVISION_PRINCIPAL_SET, false, false),
            (LIST_PRINCIPALS, true, false),
            (AUTHENTICATE_ACTION, false, false),
            (PREVIEW_DECISION_ACTION, false, false),
            (COMMIT_DECISION_ACTION, false, true),
            (PREVIEW_FAIR_VALUE_ACTION, false, false),
            (COMMIT_FAIR_VALUE_ACTION, false, true),
        ]
        .into_iter()
        .map(|(name, read_only, destructive)| {
            json!({
                "name": name,
                "version": "1.0.0",
                "description": "Installed-client governed action authority.",
                "inputSchema": {"type": "object", "additionalProperties": false},
                "outputSchema": {"type": "object"},
                "contract": {
                    "domain": "Governance",
                    "authorization": if read_only { "read_only" } else { "local_confirmation" },
                },
                "metadata": {"privateInstalledClient": true},
                "effects": {
                    "readOnly": read_only,
                    "destructive": destructive,
                    "idempotent": read_only,
                    "openWorld": false,
                },
            })
        })
        .collect()
    }

    pub(crate) async fn call(
        &self,
        operation: &str,
        arguments: &Map<String, Value>,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        self.authorize(context)?;
        ensure_live(context)?;
        if operation == PROVISIONING_STATUS {
            let (data, item_count) = self.provisioning_status()?;
            return finish_result(data, item_count, context);
        }
        if operation == PROVISION_PRINCIPAL_SET {
            let input: ProvisionPrincipalSetInput = decode(arguments)?;
            let actions = self
                .persistence
                .provision_principal_set(
                    Arc::clone(&self.secrets),
                    GovernanceProvisioningRequest {
                        primary_display_name: input.primary_display_name,
                        primary_credential: SecretValue::new(input.primary_credential)
                            .map_err(|_| ServiceError::InvalidRequest)?,
                        reviewer_display_name: input.reviewer_display_name,
                        reviewer_credential: SecretValue::new(input.reviewer_credential)
                            .map_err(|_| ServiceError::InvalidRequest)?,
                        limits: self.authority_limits,
                    },
                    |authority| {
                        GovernedActionService::try_new(
                            Arc::new(authority),
                            Arc::clone(&self.decisions),
                            Arc::clone(&self.fair_value),
                            self.action_limits,
                        )
                        .map(Arc::new)
                        .map_err(|_error| ())
                    },
                )
                .map_err(map_governance_persistence_error)?;
            let mut installed = self
                .actions
                .write()
                .map_err(|_| ServiceError::Unavailable)?;
            if installed.is_some() {
                return Err(ServiceError::InvalidRequest);
            }
            *installed = Some(actions);
            drop(installed);
            let (data, item_count) = self.provisioning_status()?;
            return finish_result(data, item_count, context);
        }
        let actions = self
            .actions
            .read()
            .map_err(|_| ServiceError::Unavailable)?
            .clone()
            .ok_or(ServiceError::Unavailable)?;
        let now = Instant::now();
        let observed_at =
            super::runtime::current_timestamp().map_err(|_error| ServiceError::Unavailable)?;
        let binding = GovernanceRequestBinding::try_new(
            self.runtime.workspace_id(),
            self.runtime.service_generation(),
            self.desktop_client,
        )
        .map_err(|_error| ServiceError::Unavailable)?;
        let (data, item_count) = match operation {
            LIST_PRINCIPALS => {
                let input: PrincipalListInput = decode(arguments)?;
                let after = input.after.map(parse_principal_id).transpose()?;
                let limit = NonZeroUsize::new(input.limit.unwrap_or(64))
                    .filter(|limit| limit.get() <= 64)
                    .ok_or(ServiceError::InvalidRequest)?;
                let page = actions
                    .list_principals(after, limit)
                    .map_err(map_governance_service_error)?;
                let count = page.principals().len();
                (
                    serde_json::to_value(page).map_err(|_| ServiceError::Internal)?,
                    count,
                )
            }
            AUTHENTICATE_ACTION => {
                let input: AuthenticationInput = decode(arguments)?;
                let request = GovernanceAuthenticationRequest::new(
                    parse_preview_id(input.preview_id)?,
                    parse_principal_id(input.principal_id)?,
                    SecretValue::new(input.credential).map_err(|_| ServiceError::InvalidRequest)?,
                );
                let ticket = actions
                    .authenticate_action(request, now, observed_at)
                    .map_err(map_governance_service_error)?;
                (json!({"authorization": ticket}), 1)
            }
            PREVIEW_DECISION_ACTION => {
                let input: DecisionPreviewInput = decode(arguments)?;
                let preview = match input.proposal {
                    DecisionProposalInput::Review {
                        target_id,
                        target_revision,
                        disposition,
                        note,
                    } => {
                        let disposition = match disposition {
                            ReviewDispositionInput::Activate => DecisionReviewDisposition::Activate,
                            ReviewDispositionInput::Reject => DecisionReviewDisposition::Reject,
                            ReviewDispositionInput::NeedsChanges => {
                                DecisionReviewDisposition::NeedsChanges
                            }
                        };
                        actions
                            .preview_decision_review(
                                DecisionReviewProposal::try_new(
                                    target_id,
                                    target_revision,
                                    disposition,
                                    note,
                                )
                                .map_err(|_| ServiceError::InvalidRequest)?,
                                binding,
                                now,
                                observed_at,
                            )
                            .await
                    }
                    DecisionProposalInput::Invalidation {
                        target_id,
                        target_revision,
                        invalidation_kind,
                        note,
                    } => {
                        let kind = match invalidation_kind {
                            InvalidationKindInput::CorporateAction => {
                                DecisionInvalidationKind::CorporateAction
                            }
                            InvalidationKindInput::Model => DecisionInvalidationKind::Model,
                            InvalidationKindInput::Data => DecisionInvalidationKind::Data,
                            InvalidationKindInput::ReferenceMark => {
                                DecisionInvalidationKind::ReferenceMark
                            }
                            InvalidationKindInput::Assumption => {
                                DecisionInvalidationKind::Assumption
                            }
                        };
                        actions
                            .preview_decision_invalidation(
                                DecisionInvalidationProposal::try_new(
                                    target_id,
                                    target_revision,
                                    kind,
                                    note,
                                )
                                .map_err(|_| ServiceError::InvalidRequest)?,
                                binding,
                                now,
                                observed_at,
                            )
                            .await
                    }
                }
                .map_err(map_governance_service_error)?;
                (json!({"preview": preview}), 1)
            }
            COMMIT_DECISION_ACTION => {
                let input: CommitInput = decode(arguments)?;
                let request = commit_request(input)?;
                let receipt = actions
                    .commit_decision_action(request, now, observed_at)
                    .await
                    .map_err(map_governance_service_error)?;
                (json!({"receipt": receipt.authorization()}), 1)
            }
            PREVIEW_FAIR_VALUE_ACTION => {
                let input: FairValuePreviewInput = decode(arguments)?;
                let preview = match input.proposal {
                    FairValueProposalInput::Approve {
                        measurement_id,
                        decision_id,
                        expires_at,
                    } => {
                        actions
                            .preview_fair_value_approval(
                                FairValueApprovalProposal::try_new(
                                    measurement_id,
                                    decision_id,
                                    expires_at,
                                )
                                .map_err(|_| ServiceError::InvalidRequest)?,
                                binding,
                                now,
                                observed_at,
                            )
                            .await
                    }
                    FairValueProposalInput::Override {
                        measurement_id,
                        decision_id,
                        requested_hierarchy,
                        justification,
                        expires_at,
                    } => {
                        actions
                            .preview_fair_value_override(
                                FairValueOverrideProposal::try_new(
                                    measurement_id,
                                    decision_id,
                                    match requested_hierarchy {
                                        FairValueHierarchyInput::Level2 => {
                                            FairValueRequestedHierarchy::Level2
                                        }
                                        FairValueHierarchyInput::Level3 => {
                                            FairValueRequestedHierarchy::Level3
                                        }
                                    },
                                    justification,
                                    expires_at,
                                )
                                .map_err(|_| ServiceError::InvalidRequest)?,
                                binding,
                                now,
                                observed_at,
                            )
                            .await
                    }
                    FairValueProposalInput::Revoke {
                        approval_id,
                        reason,
                    } => {
                        actions
                            .preview_fair_value_revocation(
                                FairValueRevocationProposal::try_new(approval_id, reason)
                                    .map_err(|_| ServiceError::InvalidRequest)?,
                                binding,
                                now,
                                observed_at,
                            )
                            .await
                    }
                    FairValueProposalInput::MarketAccess {
                        account_id,
                        venue_id,
                        instrument_id,
                        conclusion,
                        effective_from,
                        effective_until,
                        rationale,
                    } => {
                        actions
                            .preview_fair_value_market_access(
                                FairValueMarketAccessProposal::try_new(
                                    account_id,
                                    venue_id,
                                    instrument_id,
                                    match conclusion {
                                        MarketAccessConclusionInput::Accessible => {
                                            FairValueMarketAccessConclusion::Accessible
                                        }
                                        MarketAccessConclusionInput::Inaccessible => {
                                            FairValueMarketAccessConclusion::Inaccessible
                                        }
                                    },
                                    effective_from,
                                    effective_until,
                                    rationale,
                                )
                                .map_err(|_| ServiceError::InvalidRequest)?,
                                binding,
                                now,
                                observed_at,
                            )
                            .await
                    }
                }
                .map_err(map_governance_service_error)?;
                (json!({"preview": preview}), 1)
            }
            COMMIT_FAIR_VALUE_ACTION => {
                let input: CommitInput = decode(arguments)?;
                let request = commit_request(input)?;
                let receipt = actions
                    .commit_fair_value_action(request, now, observed_at)
                    .await
                    .map_err(map_governance_service_error)?;
                (json!({"receipt": receipt.authorization()}), 1)
            }
            _ => return Err(ServiceError::NotFound),
        };
        finish_result(data, item_count, context)
    }

    fn provisioning_status(&self) -> Result<(Value, usize), ServiceError> {
        let Some(registrations) = self
            .persistence
            .load_registrations()
            .map_err(map_governance_persistence_error)?
        else {
            return Ok((
                json!({
                    "state": "unprovisioned",
                    "configured": false,
                    "principals": [],
                    "missingRoles": [
                        "decisionReviewer",
                        "decisionInvalidator",
                        "fairValueApprover",
                        "fairValueOverrideApprover",
                        "fairValueRevoker",
                        "fairValueMarketAccessApprover",
                        "portfolioImportResolver",
                    ],
                }),
                0,
            ));
        };
        let principals = registrations
            .iter()
            .map(|registration| {
                let principal = registration.principal();
                json!({
                    "principalId": principal.id(),
                    "displayName": principal.display_name(),
                    "roles": principal.roles(),
                })
            })
            .collect::<Vec<_>>();
        let item_count = principals.len();
        Ok((
            json!({
                "state": "active",
                "configured": true,
                "principals": principals,
                "missingRoles": [],
            }),
            item_count,
        ))
    }

    fn authorize(&self, context: &RequestContext) -> Result<(), ServiceError> {
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        if origin.workspace_id() != self.runtime.workspace_id().as_uuid()
            || origin.client_id() != self.desktop_client.as_uuid()
        {
            return Err(ServiceError::Unauthorized);
        }
        Ok(())
    }
}

fn finish_result(
    data: Value,
    item_count: usize,
    context: &RequestContext,
) -> Result<Value, ServiceError> {
    ensure_live(context)?;
    TypedToolResult::try_new(
        data,
        item_count,
        ToolResultMetadata::complete_not_applicable(),
        context.limits(),
    )
    .map(TypedToolResult::into_envelope)
    .map_err(Into::into)
}

impl fmt::Debug for InstalledGovernanceOperations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledGovernanceOperations")
            .field("configured", &self.is_configured())
            .field("runtime", &self.runtime)
            .field("desktop_client", &self.desktop_client)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PrincipalListInput {
    after: Option<Uuid>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ProvisionPrincipalSetInput {
    primary_display_name: String,
    primary_credential: String,
    reviewer_display_name: String,
    reviewer_credential: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct AuthenticationInput {
    preview_id: Uuid,
    principal_id: Uuid,
    credential: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DecisionPreviewInput {
    proposal: DecisionProposalInput,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
enum DecisionProposalInput {
    Review {
        target_id: String,
        target_revision: u64,
        disposition: ReviewDispositionInput,
        note: String,
    },
    Invalidation {
        target_id: String,
        target_revision: u64,
        invalidation_kind: InvalidationKindInput,
        note: String,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReviewDispositionInput {
    Activate,
    Reject,
    NeedsChanges,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum InvalidationKindInput {
    CorporateAction,
    Model,
    Data,
    ReferenceMark,
    Assumption,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct FairValuePreviewInput {
    proposal: FairValueProposalInput,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
enum FairValueProposalInput {
    Approve {
        measurement_id: String,
        decision_id: String,
        expires_at: String,
    },
    Override {
        measurement_id: String,
        decision_id: String,
        requested_hierarchy: FairValueHierarchyInput,
        justification: String,
        expires_at: String,
    },
    Revoke {
        approval_id: String,
        reason: String,
    },
    MarketAccess {
        account_id: String,
        venue_id: String,
        instrument_id: String,
        conclusion: MarketAccessConclusionInput,
        effective_from: String,
        effective_until: String,
        rationale: String,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum FairValueHierarchyInput {
    Level2,
    Level3,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum MarketAccessConclusionInput {
    Accessible,
    Inaccessible,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CommitInput {
    preview_id: Uuid,
    ticket_ids: Vec<Uuid>,
}

fn commit_request(input: CommitInput) -> Result<GovernanceActionCommitRequest, ServiceError> {
    GovernanceActionCommitRequest::try_new(
        parse_preview_id(input.preview_id)?,
        input
            .ticket_ids
            .into_iter()
            .map(GovernanceTicketId::try_from_uuid)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ServiceError::InvalidRequest)?,
    )
    .map_err(map_governance_service_error)
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(arguments.clone()))
        .map_err(|_| ServiceError::InvalidRequest)
}

fn parse_preview_id(value: Uuid) -> Result<GovernancePreviewId, ServiceError> {
    GovernancePreviewId::try_from_uuid(value).map_err(|_| ServiceError::InvalidRequest)
}

fn parse_principal_id(value: Uuid) -> Result<GovernancePrincipalId, ServiceError> {
    GovernancePrincipalId::try_from_uuid(value).map_err(|_| ServiceError::InvalidRequest)
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if Instant::now() >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    Ok(())
}

fn map_governance_persistence_error(error: GovernancePersistenceError) -> ServiceError {
    match error {
        GovernancePersistenceError::AlreadyProvisioned
        | GovernancePersistenceError::InvalidPrincipalSet
        | GovernancePersistenceError::InvalidRegistrationState => ServiceError::InvalidRequest,
        GovernancePersistenceError::Capacity => ServiceError::ResourceExhausted,
        GovernancePersistenceError::Path(_)
        | GovernancePersistenceError::State(_)
        | GovernancePersistenceError::StateUnavailable
        | GovernancePersistenceError::ProvisioningRecoveryRequired
        | GovernancePersistenceError::SecretOperation
        | GovernancePersistenceError::Governance(_)
        | GovernancePersistenceError::Io(_)
        | GovernancePersistenceError::UnsafeAuditIdentity
        | GovernancePersistenceError::InsecureAuditPermissions
        | GovernancePersistenceError::AuditAlreadyLocked
        | GovernancePersistenceError::CorruptAuditRecord
        | GovernancePersistenceError::PoisonedAudit
        | GovernancePersistenceError::AuditEncoding => ServiceError::Unavailable,
        #[cfg(unix)]
        GovernancePersistenceError::AuditOwnerMismatch => ServiceError::Unavailable,
        #[cfg(not(unix))]
        GovernancePersistenceError::AuditOwnerMismatch
        | GovernancePersistenceError::AuditPermissionProofUnavailable => ServiceError::Unavailable,
        #[cfg(not(any(unix, windows)))]
        GovernancePersistenceError::DirectoryDurabilityUnavailable => ServiceError::Unavailable,
    }
}

fn map_governance_service_error(error: GovernedActionServiceError) -> ServiceError {
    match error {
        GovernedActionServiceError::InvalidLimits
        | GovernedActionServiceError::InvalidCommitInput
        | GovernedActionServiceError::InsufficientEligiblePrincipals
        | GovernedActionServiceError::CanonicalActionNotFound
        | GovernedActionServiceError::CanonicalActionFamilyMismatch
        | GovernedActionServiceError::CanonicalActionCommitInProgress
        | GovernedActionServiceError::CanonicalActionKindMismatch
        | GovernedActionServiceError::CanonicalActionMismatch
        | GovernedActionServiceError::UnsupportedAction => ServiceError::InvalidRequest,
        GovernedActionServiceError::CapacityExceeded => ServiceError::ResourceExhausted,
        GovernedActionServiceError::Authority(source) => match source {
            GovernanceError::InvalidCredential
            | GovernanceError::ReauthenticationLocked
            | GovernanceError::PrincipalNotFound
            | GovernanceError::PrincipalNotEligible
            | GovernanceError::TicketNotFound
            | GovernanceError::TicketPreviewMismatch
            | GovernanceError::TicketExpired
            | GovernanceError::TicketConsumed
            | GovernanceError::DuplicateTicket
            | GovernanceError::DuplicatePrincipal
            | GovernanceError::IncorrectTicketCount => ServiceError::Unauthorized,
            GovernanceError::CapacityExceeded => ServiceError::ResourceExhausted,
            GovernanceError::PreviewNotFound | GovernanceError::PreviewExpired => {
                ServiceError::NotFound
            }
            GovernanceError::InvalidIdentity
            | GovernanceError::InvalidLimits
            | GovernanceError::InvalidPrincipal
            | GovernanceError::InvalidRoleSet
            | GovernanceError::InvalidPreview
            | GovernanceError::TicketAlreadyIssued => ServiceError::InvalidRequest,
            GovernanceError::RandomUnavailable
            | GovernanceError::TimeUnavailable
            | GovernanceError::SecretStoreUnavailable
            | GovernanceError::AuditUnavailable
            | GovernanceError::StateUnavailable => ServiceError::Unavailable,
        },
        GovernedActionServiceError::TimeUnavailable
        | GovernedActionServiceError::StateUnavailable
        | GovernedActionServiceError::Domain(_) => ServiceError::Unavailable,
    }
}

/// Closed non-sensitive orchestration failure for governed decision or fair-value mutation.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum GovernedActionServiceError {
    /// Domain-action orchestration bounds are invalid.
    #[error("governance action service limits are invalid")]
    InvalidLimits,
    /// Commit received no ticket or more than V1's maximum two one-use tickets.
    #[error("governance commit input is invalid")]
    InvalidCommitInput,
    /// The service cannot retain another canonical action or scan the full admitted principal set.
    #[error("governance action service capacity is exhausted")]
    CapacityExceeded,
    /// No admitted principals satisfy the exact fixed policy for the action kind.
    #[error("governance action lacks sufficient eligible principals")]
    InsufficientEligiblePrincipals,
    /// Preview expiry time could not be represented by the monotonic service clock.
    #[error("governance action time is unavailable")]
    TimeUnavailable,
    /// The in-memory canonical-action index was unavailable.
    #[error("governance action state is unavailable")]
    StateUnavailable,
    /// No server-held canonical action matched the preview ID before ticket consumption.
    #[error("governance canonical action was not found")]
    CanonicalActionNotFound,
    /// A decision commit endpoint received a fair-value preview or the inverse.
    #[error("governance canonical action does not belong to this domain")]
    CanonicalActionFamilyMismatch,
    /// Another commit is currently consuming this preview's tickets; no new ticket may issue.
    #[error("governance canonical action commit is in progress")]
    CanonicalActionCommitInProgress,
    /// A trusted factory returned an action kind other than its preparation method permits.
    #[error("governance canonical action kind does not match the preparation endpoint")]
    CanonicalActionKindMismatch,
    /// Generic authorization did not exactly bind the retained canonical action.
    #[error("governance canonical action does not match authorization")]
    CanonicalActionMismatch,
    /// The generic portfolio-import family is intentionally not admitted by this decision/fair-value adapter.
    #[error("governance action kind is unsupported")]
    UnsupportedAction,
    /// Generic principal authority rejected the request.
    #[error(transparent)]
    Authority(#[from] GovernanceError),
    /// The trusted decision or fair-value domain rejected or could not persist its canonical action.
    #[error(transparent)]
    Domain(#[from] GovernanceDomainAdapterError),
}
