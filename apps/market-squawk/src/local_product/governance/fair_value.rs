//! Concrete fair-value adapter for server-held governed actions.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use chrono::DateTime;
use market_squawk_domain::{FairValueHierarchy, Timestamp};
use market_squawk_valuation::{ActorId, FairValueError, MarketAccess};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::application::{
    FairValueDomainService,
    fair_value::{
        GovernedFairValueApprovalEvidence, GovernedFairValueDecisionEvidence,
        GovernedFairValueMarketAccessEvidence, validate_governed_override,
    },
    governance::{
        CanonicalGovernanceAction, FairValueApprovalProposal, FairValueGovernanceActionFactory,
        FairValueMarketAccessConclusion, FairValueMarketAccessProposal, FairValueOverrideProposal,
        FairValueRequestedHierarchy, FairValueRevocationProposal, GovernanceActionDigest,
        GovernanceActionKind, GovernanceCommitReceipt, GovernanceDomainAdapterError,
        GovernanceDomainReceipt,
    },
};

/// Composition-owned bridge from governed fair-value proposals to the existing durable authority.
pub(crate) struct ProductionFairValueGovernanceActionFactory {
    fair_value: Arc<FairValueDomainService>,
}

impl ProductionFairValueGovernanceActionFactory {
    /// Binds governance to the exact fair-value service used by the shipping application.
    pub(crate) const fn new(fair_value: Arc<FairValueDomainService>) -> Self {
        Self { fair_value }
    }
}

impl fmt::Debug for ProductionFairValueGovernanceActionFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductionFairValueGovernanceActionFactory")
            .field("fair_value", &"[DURABLE FAIR-VALUE AUTHORITY]")
            .finish()
    }
}

#[async_trait]
impl FairValueGovernanceActionFactory for ProductionFairValueGovernanceActionFactory {
    async fn prepare_approval(
        &self,
        proposal: FairValueApprovalProposal,
        prepared_at: Timestamp,
    ) -> Result<Arc<dyn CanonicalGovernanceAction>, GovernanceDomainAdapterError> {
        let measurement_token = parse_product_token(proposal.measurement_token())?;
        let classification_token = parse_product_token(proposal.classification_token())?;
        let expires_at = parse_timestamp(proposal.requested_expires_at())?;
        if expires_at <= prepared_at {
            return Err(GovernanceDomainAdapterError::InvalidProposal);
        }
        let evidence = self
            .fair_value
            .governed_decision_evidence_for_tokens(measurement_token, classification_token)
            .await
            .map_err(map_fair_value_error)?;
        if evidence.hierarchy() == FairValueHierarchy::Unclassified {
            return Err(GovernanceDomainAdapterError::Conflict);
        }
        Ok(Arc::new(FairValueCanonicalAction::approval(
            Arc::clone(&self.fair_value),
            evidence,
            expires_at,
        )))
    }

    async fn prepare_override(
        &self,
        proposal: FairValueOverrideProposal,
        prepared_at: Timestamp,
    ) -> Result<Arc<dyn CanonicalGovernanceAction>, GovernanceDomainAdapterError> {
        let measurement_token = parse_product_token(proposal.measurement_token())?;
        let classification_token = parse_product_token(proposal.classification_token())?;
        let requested_hierarchy = match proposal.requested_hierarchy() {
            FairValueRequestedHierarchy::Level2 => FairValueHierarchy::Level2,
            FairValueRequestedHierarchy::Level3 => FairValueHierarchy::Level3,
        };
        let expires_at = parse_timestamp(proposal.requested_expires_at())?;
        if expires_at <= prepared_at {
            return Err(GovernanceDomainAdapterError::InvalidProposal);
        }
        let evidence = self
            .fair_value
            .governed_decision_evidence_for_tokens(measurement_token, classification_token)
            .await
            .map_err(map_fair_value_error)?;
        validate_governed_override(&evidence, requested_hierarchy).map_err(map_fair_value_error)?;
        Ok(Arc::new(FairValueCanonicalAction::override_action(
            Arc::clone(&self.fair_value),
            evidence,
            requested_hierarchy,
            proposal.justification().into(),
            expires_at,
        )))
    }

    async fn prepare_revocation(
        &self,
        proposal: FairValueRevocationProposal,
        prepared_at: Timestamp,
    ) -> Result<Arc<dyn CanonicalGovernanceAction>, GovernanceDomainAdapterError> {
        let approval_token = parse_product_token(proposal.approval_token())?;
        let evidence = self
            .fair_value
            .governed_approval_evidence_for_token(approval_token, prepared_at)
            .await
            .map_err(map_fair_value_error)?;
        Ok(Arc::new(FairValueCanonicalAction::revocation(
            Arc::clone(&self.fair_value),
            evidence,
            proposal.reason().into(),
        )))
    }

    async fn prepare_market_access(
        &self,
        proposal: FairValueMarketAccessProposal,
        _prepared_at: Timestamp,
    ) -> Result<Arc<dyn CanonicalGovernanceAction>, GovernanceDomainAdapterError> {
        let market_input_token = parse_product_token(proposal.market_input_token())?;
        let conclusion = match proposal.conclusion() {
            FairValueMarketAccessConclusion::Accessible => MarketAccess::Accessible,
            FairValueMarketAccessConclusion::Inaccessible => MarketAccess::Inaccessible,
        };
        let effective_from = parse_timestamp(proposal.effective_from())?;
        let effective_until = parse_timestamp(proposal.effective_until())?;
        if effective_until < effective_from {
            return Err(GovernanceDomainAdapterError::InvalidProposal);
        }
        let evidence = self
            .fair_value
            .governed_market_access_evidence_for_token(market_input_token, effective_from)
            .await
            .map_err(map_fair_value_error)?;
        Ok(Arc::new(FairValueCanonicalAction::market_access(
            Arc::clone(&self.fair_value),
            evidence,
            conclusion,
            effective_until,
            proposal.rationale().into(),
        )))
    }
}

enum FairValueCanonicalAction {
    Approval {
        fair_value: Arc<FairValueDomainService>,
        evidence: GovernedFairValueDecisionEvidence,
        expires_at: Timestamp,
        digest: GovernanceActionDigest,
    },
    Override {
        fair_value: Arc<FairValueDomainService>,
        evidence: GovernedFairValueDecisionEvidence,
        requested_hierarchy: FairValueHierarchy,
        justification: Box<str>,
        expires_at: Timestamp,
        digest: GovernanceActionDigest,
    },
    Revocation {
        fair_value: Arc<FairValueDomainService>,
        evidence: GovernedFairValueApprovalEvidence,
        reason: Box<str>,
        digest: GovernanceActionDigest,
    },
    MarketAccess {
        fair_value: Arc<FairValueDomainService>,
        evidence: GovernedFairValueMarketAccessEvidence,
        conclusion: MarketAccess,
        effective_until: Timestamp,
        rationale: Box<str>,
        digest: GovernanceActionDigest,
    },
}

impl FairValueCanonicalAction {
    fn approval(
        fair_value: Arc<FairValueDomainService>,
        evidence: GovernedFairValueDecisionEvidence,
        expires_at: Timestamp,
    ) -> Self {
        let mut digest = ActionDigest::new(b"market-squawk/governance/fair-value-approval/v1");
        digest.decision(&evidence);
        digest.timestamp(expires_at);
        Self::Approval {
            fair_value,
            evidence,
            expires_at,
            digest: digest.finish(),
        }
    }

    fn override_action(
        fair_value: Arc<FairValueDomainService>,
        evidence: GovernedFairValueDecisionEvidence,
        requested_hierarchy: FairValueHierarchy,
        justification: Box<str>,
        expires_at: Timestamp,
    ) -> Self {
        let mut digest = ActionDigest::new(b"market-squawk/governance/fair-value-override/v1");
        digest.decision(&evidence);
        digest.byte(hierarchy_tag(requested_hierarchy));
        digest.text(&justification);
        digest.timestamp(expires_at);
        Self::Override {
            fair_value,
            evidence,
            requested_hierarchy,
            justification,
            expires_at,
            digest: digest.finish(),
        }
    }

    fn revocation(
        fair_value: Arc<FairValueDomainService>,
        evidence: GovernedFairValueApprovalEvidence,
        reason: Box<str>,
    ) -> Self {
        let mut digest = ActionDigest::new(b"market-squawk/governance/fair-value-revocation/v1");
        digest.fixed(evidence.approval_id().bytes());
        digest.decision(evidence.decision());
        digest.timestamp(evidence.expires_at());
        digest.text(&reason);
        Self::Revocation {
            fair_value,
            evidence,
            reason,
            digest: digest.finish(),
        }
    }

    fn market_access(
        fair_value: Arc<FairValueDomainService>,
        evidence: GovernedFairValueMarketAccessEvidence,
        conclusion: MarketAccess,
        effective_until: Timestamp,
        rationale: Box<str>,
    ) -> Self {
        let mut digest = ActionDigest::new(b"market-squawk/governance/fair-value-market-access/v1");
        digest.raw(evidence.account_id().as_uuid().as_bytes());
        digest.text(evidence.venue_id().as_str());
        digest.raw(evidence.instrument_id().as_uuid().as_bytes());
        digest.byte(market_access_tag(conclusion));
        digest.timestamp(evidence.effective_from());
        digest.timestamp(effective_until);
        digest.text(&rationale);
        match evidence.current_assessment_id() {
            Some(id) => {
                digest.byte(1);
                digest.fixed(id.bytes());
            }
            None => digest.byte(0),
        }
        Self::MarketAccess {
            fair_value,
            evidence,
            conclusion,
            effective_until,
            rationale,
            digest: digest.finish(),
        }
    }
}

impl fmt::Debug for FairValueCanonicalAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FairValueCanonicalAction")
            .field("kind", &self.kind())
            .field("content", &"[SERVER-HELD CANONICAL FAIR-VALUE ACTION]")
            .finish()
    }
}

#[async_trait]
impl CanonicalGovernanceAction for FairValueCanonicalAction {
    fn kind(&self) -> GovernanceActionKind {
        match self {
            Self::Approval { .. } => GovernanceActionKind::FairValueApproval,
            Self::Override { .. } => GovernanceActionKind::FairValueOverride,
            Self::Revocation { .. } => GovernanceActionKind::FairValueApprovalRevocation,
            Self::MarketAccess { .. } => GovernanceActionKind::FairValueMarketAccess,
        }
    }

    fn digest(&self) -> GovernanceActionDigest {
        match self {
            Self::Approval { digest, .. }
            | Self::Override { digest, .. }
            | Self::Revocation { digest, .. }
            | Self::MarketAccess { digest, .. } => *digest,
        }
    }

    async fn commit(
        &self,
        authorization: &GovernanceCommitReceipt,
    ) -> Result<GovernanceDomainReceipt, GovernanceDomainAdapterError> {
        let committed_at = validate_authorization(self, authorization)?;
        let domain_receipt_id = match self {
            Self::Approval {
                fair_value,
                evidence,
                expires_at,
                ..
            } => {
                let actor = single_actor(authorization)?;
                fair_value
                    .commit_governed_approval(evidence.clone(), actor, committed_at, *expires_at)
                    .await
                    .map_err(map_fair_value_error)?
                    .to_string()
            }
            Self::Override {
                fair_value,
                evidence,
                requested_hierarchy,
                justification,
                expires_at,
                ..
            } => {
                let actor = single_actor(authorization)?;
                fair_value
                    .commit_governed_override(
                        evidence.clone(),
                        *requested_hierarchy,
                        justification,
                        actor,
                        committed_at,
                        *expires_at,
                    )
                    .await
                    .map_err(map_fair_value_error)?
                    .to_string()
            }
            Self::Revocation {
                fair_value,
                evidence,
                reason,
                ..
            } => {
                let actor = single_actor(authorization)?;
                fair_value
                    .commit_governed_revocation(evidence.clone(), actor, committed_at, reason)
                    .await
                    .map_err(map_fair_value_error)?
                    .to_string()
            }
            Self::MarketAccess {
                fair_value,
                evidence,
                conclusion,
                effective_until,
                rationale,
                ..
            } => {
                let [prepared_by, approved_by] = sorted_market_access_actors(authorization)?;
                fair_value
                    .commit_governed_market_access(
                        evidence.clone(),
                        *conclusion,
                        *effective_until,
                        rationale,
                        prepared_by,
                        approved_by,
                        committed_at,
                    )
                    .await
                    .map_err(map_fair_value_error)?
                    .to_string()
            }
        };
        GovernanceDomainReceipt::try_new(self.kind(), domain_receipt_id, committed_at)
    }
}

fn validate_authorization(
    action: &dyn CanonicalGovernanceAction,
    authorization: &GovernanceCommitReceipt,
) -> Result<Timestamp, GovernanceDomainAdapterError> {
    if authorization.digest() != action.digest()
        || authorization.effects().len() != 1
        || authorization.effects()[0].kind() != action.kind()
    {
        return Err(GovernanceDomainAdapterError::ReceiptMismatch);
    }
    Ok(authorization.committed_at().timestamp())
}

fn single_actor(
    authorization: &GovernanceCommitReceipt,
) -> Result<ActorId, GovernanceDomainAdapterError> {
    let [principal] = authorization.authorized_principals() else {
        return Err(GovernanceDomainAdapterError::ReceiptMismatch);
    };
    actor_id(principal.principal_id())
}

fn sorted_market_access_actors(
    authorization: &GovernanceCommitReceipt,
) -> Result<[ActorId; 2], GovernanceDomainAdapterError> {
    let [first, second] = authorization.authorized_principals() else {
        return Err(GovernanceDomainAdapterError::ReceiptMismatch);
    };
    let mut ids = [first.principal_id(), second.principal_id()];
    ids.sort_unstable();
    if ids[0] == ids[1] {
        return Err(GovernanceDomainAdapterError::ReceiptMismatch);
    }
    Ok([actor_id(ids[0])?, actor_id(ids[1])?])
}

fn actor_id(
    principal_id: crate::application::governance::GovernancePrincipalId,
) -> Result<ActorId, GovernanceDomainAdapterError> {
    ActorId::try_from(principal_id.as_uuid().hyphenated().to_string().as_str())
        .map_err(|_| GovernanceDomainAdapterError::Internal)
}

fn parse_product_token(value: &str) -> Result<Uuid, GovernanceDomainAdapterError> {
    Uuid::parse_str(value).map_err(|_| GovernanceDomainAdapterError::InvalidProposal)
}

fn parse_timestamp(value: &str) -> Result<Timestamp, GovernanceDomainAdapterError> {
    DateTime::parse_from_rfc3339(value)
        .map_err(|_| GovernanceDomainAdapterError::InvalidProposal)?
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(GovernanceDomainAdapterError::InvalidProposal)
}

fn map_fair_value_error(error: FairValueError) -> GovernanceDomainAdapterError {
    match error {
        FairValueError::MeasurementNotFound
        | FairValueError::DecisionNotFound
        | FairValueError::ApprovalNotFound => GovernanceDomainAdapterError::NotFound,
        FairValueError::LimitExceeded { .. }
        | FairValueError::RetainedBytesExceeded { .. }
        | FairValueError::QueryLimitExceeded { .. } => {
            GovernanceDomainAdapterError::CapacityExceeded
        }
        FairValueError::Persistence => GovernanceDomainAdapterError::PersistenceUnavailable,
        FairValueError::CorruptPersistence | FairValueError::Arithmetic => {
            GovernanceDomainAdapterError::Internal
        }
        _ => GovernanceDomainAdapterError::Conflict,
    }
}

struct ActionDigest(Sha256);

impl ActionDigest {
    fn new(domain: &[u8]) -> Self {
        let mut hash = Sha256::new();
        hash.update(domain);
        Self(hash)
    }

    fn decision(&mut self, evidence: &GovernedFairValueDecisionEvidence) {
        self.fixed(evidence.measurement_id().bytes());
        self.fixed(evidence.decision_id().bytes());
        self.fixed(evidence.evidence_hash().bytes());
        self.fixed(evidence.ruleset_hash().bytes());
        self.byte(hierarchy_tag(evidence.hierarchy()));
    }

    fn fixed(&mut self, value: [u8; 32]) {
        self.0.update(value);
    }

    fn raw(&mut self, value: &[u8]) {
        self.0.update(value);
    }

    fn byte(&mut self, value: u8) {
        self.0.update([value]);
    }

    fn timestamp(&mut self, value: Timestamp) {
        self.0.update(value.unix_nanos().to_be_bytes());
    }

    fn text(&mut self, value: &str) {
        self.0
            .update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
        self.0.update(value.as_bytes());
    }

    fn finish(self) -> GovernanceActionDigest {
        GovernanceActionDigest::from_bytes(self.0.finalize().into())
    }
}

const fn hierarchy_tag(value: FairValueHierarchy) -> u8 {
    match value {
        FairValueHierarchy::Level1 => 1,
        FairValueHierarchy::Level2 => 2,
        FairValueHierarchy::Level3 => 3,
        FairValueHierarchy::Unclassified => 4,
    }
}

const fn market_access_tag(value: MarketAccess) -> u8 {
    match value {
        MarketAccess::Accessible => 1,
        MarketAccess::Inaccessible => 2,
        MarketAccess::NotAssessed => 3,
    }
}
