//! Decision-domain governance preparation and durable commit adapter.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use market_squawk_decisions::{
    DecisionActorId, DecisionContentDigest, DecisionContractError, InvalidationKind,
    InvestmentTargetSetId, TargetInvalidation, TargetInvalidationId, TargetReview,
    TargetReviewDisposition, TargetReviewId, TargetState, TargetStatus,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, RevisionNumber, Timestamp};
use sha2::{Digest as _, Sha256};

use crate::application::{
    decision::{DecisionApplication, DecisionApplicationError},
    governance::{
        CanonicalGovernanceAction, DecisionGovernanceActionFactory, DecisionInvalidationKind,
        DecisionInvalidationProposal, DecisionReviewDisposition, DecisionReviewProposal,
        GovernanceActionDigest, GovernanceActionKind, GovernanceCommitReceipt,
        GovernanceDomainAdapterError, GovernanceDomainReceipt, GovernanceRole,
    },
};

const REVIEW_DIGEST_DOMAIN: &[u8] = b"market-squawk/decision-governance-review/v1\0";
const INVALIDATION_DIGEST_DOMAIN: &[u8] = b"market-squawk/decision-governance-invalidation/v1\0";
const REVIEW_RECORD_DIGEST_DOMAIN: &[u8] = b"market-squawk/decision-governance-review-record/v1\0";
const INVALIDATION_RECORD_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/decision-governance-invalidation-record/v1\0";

/// Concrete local adapter from governed-action previews to the sole durable decision authority.
#[derive(Debug)]
pub(crate) struct DecisionGovernanceAdapter {
    decisions: Arc<DecisionApplication>,
}

impl DecisionGovernanceAdapter {
    /// Binds canonical decision actions to the recovered, single-writer decision application.
    #[must_use]
    pub(crate) fn new(decisions: Arc<DecisionApplication>) -> Self {
        Self { decisions }
    }
}

#[async_trait]
impl DecisionGovernanceActionFactory for DecisionGovernanceAdapter {
    async fn prepare_review(
        &self,
        proposal: DecisionReviewProposal,
        prepared_at: Timestamp,
    ) -> Result<Arc<dyn CanonicalGovernanceAction>, GovernanceDomainAdapterError> {
        let target = resolve_target(
            &self.decisions,
            proposal.target_id(),
            proposal.target_revision(),
        )?;
        let disposition = review_disposition(proposal.disposition());
        validate_review_preparation(&target, disposition, prepared_at)?;
        let digest = review_digest(&target, &proposal, prepared_at);
        Ok(Arc::new(PreparedDecisionReview {
            decisions: Arc::clone(&self.decisions),
            target,
            proposal,
            digest,
            prepared_at,
        }))
    }

    async fn prepare_invalidation(
        &self,
        proposal: DecisionInvalidationProposal,
        prepared_at: Timestamp,
    ) -> Result<Arc<dyn CanonicalGovernanceAction>, GovernanceDomainAdapterError> {
        let target = resolve_target(
            &self.decisions,
            proposal.target_id(),
            proposal.target_revision(),
        )?;
        if prepared_at < target.target().target().created_at() {
            return Err(GovernanceDomainAdapterError::Conflict);
        }
        let digest = invalidation_digest(&target, &proposal, prepared_at);
        Ok(Arc::new(PreparedDecisionInvalidation {
            decisions: Arc::clone(&self.decisions),
            target,
            proposal,
            digest,
            prepared_at,
        }))
    }
}

struct PreparedDecisionReview {
    decisions: Arc<DecisionApplication>,
    target: TargetState,
    proposal: DecisionReviewProposal,
    digest: GovernanceActionDigest,
    prepared_at: Timestamp,
}

impl fmt::Debug for PreparedDecisionReview {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedDecisionReview")
            .field("target_id", &self.proposal.target_id())
            .field("target_revision", &self.proposal.target_revision())
            .field("disposition", &self.proposal.disposition())
            .field("content", &"[REDACTED SERVER-CANONICAL ACTION]")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CanonicalGovernanceAction for PreparedDecisionReview {
    fn kind(&self) -> GovernanceActionKind {
        GovernanceActionKind::DecisionReview
    }

    fn digest(&self) -> GovernanceActionDigest {
        self.digest
    }

    async fn commit(
        &self,
        authorization: &GovernanceCommitReceipt,
    ) -> Result<GovernanceDomainReceipt, GovernanceDomainAdapterError> {
        let principal = authorized_principal(
            authorization,
            self.kind(),
            self.digest,
            GovernanceRole::DecisionReviewer,
            self.prepared_at,
        )?;
        let receipt_uuid = authorization.receipt_id().as_uuid();
        let record_id = format!("review.{}", receipt_uuid.simple());
        let actor = actor_id(principal.principal_id().as_uuid())?;
        let committed_at = authorization.committed_at().timestamp();
        let review = TargetReview::try_new(
            TargetReviewId::try_new(&record_id).map_err(map_contract)?,
            self.target.target().target(),
            actor.clone(),
            committed_at,
            review_disposition(self.proposal.disposition()),
            committed_content_identity(
                REVIEW_RECORD_DIGEST_DOMAIN,
                self.digest,
                &record_id,
                &actor,
                committed_at,
            )?,
        )
        .map_err(map_contract)?;
        self.decisions
            .review_target(review)
            .map_err(map_application)?;
        GovernanceDomainReceipt::try_new(self.kind(), record_id, committed_at)
    }
}

struct PreparedDecisionInvalidation {
    decisions: Arc<DecisionApplication>,
    target: TargetState,
    proposal: DecisionInvalidationProposal,
    digest: GovernanceActionDigest,
    prepared_at: Timestamp,
}

impl fmt::Debug for PreparedDecisionInvalidation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedDecisionInvalidation")
            .field("target_id", &self.proposal.target_id())
            .field("target_revision", &self.proposal.target_revision())
            .field("kind", &self.proposal.kind())
            .field("content", &"[REDACTED SERVER-CANONICAL ACTION]")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CanonicalGovernanceAction for PreparedDecisionInvalidation {
    fn kind(&self) -> GovernanceActionKind {
        GovernanceActionKind::DecisionInvalidation
    }

    fn digest(&self) -> GovernanceActionDigest {
        self.digest
    }

    async fn commit(
        &self,
        authorization: &GovernanceCommitReceipt,
    ) -> Result<GovernanceDomainReceipt, GovernanceDomainAdapterError> {
        let principal = authorized_principal(
            authorization,
            self.kind(),
            self.digest,
            GovernanceRole::DecisionInvalidator,
            self.prepared_at,
        )?;
        let receipt_uuid = authorization.receipt_id().as_uuid();
        let record_id = format!("invalidation.{}", receipt_uuid.simple());
        let actor = actor_id(principal.principal_id().as_uuid())?;
        let committed_at = authorization.committed_at().timestamp();
        let invalidation = TargetInvalidation::try_new(
            TargetInvalidationId::try_new(&record_id).map_err(map_contract)?,
            self.target.target().target(),
            invalidation_kind(self.proposal.kind()),
            actor.clone(),
            committed_at,
            committed_content_identity(
                INVALIDATION_RECORD_DIGEST_DOMAIN,
                self.digest,
                &record_id,
                &actor,
                committed_at,
            )?,
        )
        .map_err(map_contract)?;
        self.decisions
            .invalidate_target(invalidation)
            .map_err(map_application)?;
        GovernanceDomainReceipt::try_new(self.kind(), record_id, committed_at)
    }
}

fn resolve_target(
    decisions: &DecisionApplication,
    target_id: &str,
    target_revision: u64,
) -> Result<TargetState, GovernanceDomainAdapterError> {
    let target_id = InvestmentTargetSetId::try_new(target_id)
        .map_err(|_| GovernanceDomainAdapterError::InvalidProposal)?;
    let target_revision = u32::try_from(target_revision)
        .ok()
        .and_then(|revision| RevisionNumber::new(revision).ok())
        .ok_or(GovernanceDomainAdapterError::InvalidProposal)?;
    decisions
        .get_target(&target_id, target_revision)
        .map_err(map_application)
}

fn validate_review_preparation(
    target: &TargetState,
    disposition: TargetReviewDisposition,
    prepared_at: Timestamp,
) -> Result<(), GovernanceDomainAdapterError> {
    let retained = target.target();
    if target.status() == TargetStatus::Superseded
        || prepared_at < retained.target().created_at()
        || (disposition == TargetReviewDisposition::Activate
            && (prepared_at < retained.effective_at()
                || prepared_at >= retained.target().expires_at()
                || target
                    .latest_invalidation()
                    .is_some_and(|invalidation| prepared_at < invalidation.observed_at())))
    {
        return Err(GovernanceDomainAdapterError::Conflict);
    }
    Ok(())
}

fn authorized_principal(
    authorization: &GovernanceCommitReceipt,
    kind: GovernanceActionKind,
    digest: GovernanceActionDigest,
    role: GovernanceRole,
    prepared_at: Timestamp,
) -> Result<
    &crate::application::governance::GovernanceAuthorizedPrincipal,
    GovernanceDomainAdapterError,
> {
    let [principal] = authorization.authorized_principals() else {
        return Err(GovernanceDomainAdapterError::ReceiptMismatch);
    };
    if authorization.digest() != digest
        || authorization.effects().len() != 1
        || authorization.effects()[0].kind() != kind
        || authorization.committed_at().timestamp() < prepared_at
        || !principal.roles().as_slice().contains(&role)
    {
        return Err(GovernanceDomainAdapterError::ReceiptMismatch);
    }
    Ok(principal)
}

fn actor_id(principal_id: uuid::Uuid) -> Result<DecisionActorId, GovernanceDomainAdapterError> {
    DecisionActorId::try_new(format!("principal.{}", principal_id.simple())).map_err(map_contract)
}

fn committed_content_identity(
    domain: &[u8],
    action_digest: GovernanceActionDigest,
    record_id: &str,
    actor: &DecisionActorId,
    committed_at: Timestamp,
) -> Result<DecisionContentDigest, GovernanceDomainAdapterError> {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(action_digest.as_bytes());
    hash.update(length(record_id.as_bytes()));
    hash.update(record_id.as_bytes());
    hash.update(length(actor.as_str().as_bytes()));
    hash.update(actor.as_str().as_bytes());
    hash.update(committed_at.unix_nanos().to_be_bytes());
    DecisionContentDigest::try_new(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
    .map_err(map_contract)
}

fn review_digest(
    target: &TargetState,
    proposal: &DecisionReviewProposal,
    prepared_at: Timestamp,
) -> GovernanceActionDigest {
    let mut hash = canonical_target_state(REVIEW_DIGEST_DOMAIN, target, prepared_at);
    hash.update([review_disposition_code(proposal.disposition())]);
    hash.update(length(proposal.note().as_bytes()));
    hash.update(proposal.note().as_bytes());
    GovernanceActionDigest::from_bytes(hash.finalize().into())
}

fn invalidation_digest(
    target: &TargetState,
    proposal: &DecisionInvalidationProposal,
    prepared_at: Timestamp,
) -> GovernanceActionDigest {
    let mut hash = canonical_target_state(INVALIDATION_DIGEST_DOMAIN, target, prepared_at);
    hash.update([invalidation_kind_code(proposal.kind())]);
    hash.update(length(proposal.note().as_bytes()));
    hash.update(proposal.note().as_bytes());
    GovernanceActionDigest::from_bytes(hash.finalize().into())
}

fn canonical_target_state(domain: &[u8], state: &TargetState, prepared_at: Timestamp) -> Sha256 {
    let target = state.target();
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(length(target.target().id().as_str().as_bytes()));
    hash.update(target.target().id().as_str().as_bytes());
    hash.update(target.target().revision().get().to_be_bytes());
    hash_decision_digest(&mut hash, target.target().content_identity());
    hash.update(target.effective_at().unix_nanos().to_be_bytes());
    hash.update(target.target().expires_at().unix_nanos().to_be_bytes());
    hash.update([target_status_code(state.status())]);
    hash_latest_review(&mut hash, state);
    hash_latest_invalidation(&mut hash, state);
    hash.update(prepared_at.unix_nanos().to_be_bytes());
    hash
}

fn hash_latest_review(hash: &mut Sha256, state: &TargetState) {
    let Some(review) = state.latest_review() else {
        hash.update([0]);
        return;
    };
    hash.update([1]);
    hash.update(length(review.id().as_str().as_bytes()));
    hash.update(review.id().as_str().as_bytes());
    hash.update(length(review.reviewer().as_str().as_bytes()));
    hash.update(review.reviewer().as_str().as_bytes());
    hash.update(review.reviewed_at().unix_nanos().to_be_bytes());
    hash.update([target_review_disposition_code(review.disposition())]);
    hash_decision_digest(hash, review.content_identity());
}

fn hash_latest_invalidation(hash: &mut Sha256, state: &TargetState) {
    let Some(invalidation) = state.latest_invalidation() else {
        hash.update([0]);
        return;
    };
    hash.update([1]);
    hash.update(length(invalidation.id().as_str().as_bytes()));
    hash.update(invalidation.id().as_str().as_bytes());
    match invalidation.actor() {
        Some(actor) => {
            hash.update([1]);
            hash.update(length(actor.as_str().as_bytes()));
            hash.update(actor.as_str().as_bytes());
        }
        None => hash.update([0]),
    }
    hash.update(invalidation.observed_at().unix_nanos().to_be_bytes());
    hash.update([target_invalidation_kind_code(invalidation.kind())]);
    hash_decision_digest(hash, invalidation.content_identity());
}

fn hash_decision_digest(hash: &mut Sha256, digest: DecisionContentDigest) {
    let digest = digest.evidence_digest();
    hash.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hash.update(digest.bytes());
}

fn length(value: &[u8]) -> [u8; 8] {
    u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes()
}

const fn review_disposition(value: DecisionReviewDisposition) -> TargetReviewDisposition {
    match value {
        DecisionReviewDisposition::Activate => TargetReviewDisposition::Activate,
        DecisionReviewDisposition::NeedsChanges => TargetReviewDisposition::NeedsChanges,
        DecisionReviewDisposition::Reject => TargetReviewDisposition::Reject,
    }
}

const fn invalidation_kind(value: DecisionInvalidationKind) -> InvalidationKind {
    match value {
        DecisionInvalidationKind::CorporateAction => InvalidationKind::CorporateAction,
        DecisionInvalidationKind::Model => InvalidationKind::Model,
        DecisionInvalidationKind::Data => InvalidationKind::Data,
        DecisionInvalidationKind::ReferenceMark => InvalidationKind::ReferenceMark,
        DecisionInvalidationKind::Assumption => InvalidationKind::Assumption,
    }
}

const fn review_disposition_code(value: DecisionReviewDisposition) -> u8 {
    match value {
        DecisionReviewDisposition::Activate => 1,
        DecisionReviewDisposition::NeedsChanges => 2,
        DecisionReviewDisposition::Reject => 3,
    }
}

const fn invalidation_kind_code(value: DecisionInvalidationKind) -> u8 {
    match value {
        DecisionInvalidationKind::CorporateAction => 1,
        DecisionInvalidationKind::Model => 2,
        DecisionInvalidationKind::Data => 3,
        DecisionInvalidationKind::ReferenceMark => 4,
        DecisionInvalidationKind::Assumption => 5,
    }
}

const fn target_review_disposition_code(value: TargetReviewDisposition) -> u8 {
    match value {
        TargetReviewDisposition::Activate => 1,
        TargetReviewDisposition::NeedsChanges => 2,
        TargetReviewDisposition::Reject => 3,
    }
}

const fn target_invalidation_kind_code(value: InvalidationKind) -> u8 {
    match value {
        InvalidationKind::CorporateAction => 1,
        InvalidationKind::Model => 2,
        InvalidationKind::Data => 3,
        InvalidationKind::ReferenceMark => 4,
        InvalidationKind::Assumption => 5,
    }
}

const fn target_status_code(value: TargetStatus) -> u8 {
    match value {
        TargetStatus::PendingReview => 1,
        TargetStatus::Active => 2,
        TargetStatus::Rejected => 3,
        TargetStatus::NeedsChanges => 4,
        TargetStatus::NeedsReview => 5,
        TargetStatus::Superseded => 6,
    }
}

fn map_contract(_error: DecisionContractError) -> GovernanceDomainAdapterError {
    GovernanceDomainAdapterError::Conflict
}

fn map_application(error: DecisionApplicationError) -> GovernanceDomainAdapterError {
    use market_squawk_decisions::DecisionRepositoryError;

    match error {
        DecisionApplicationError::Repository(DecisionRepositoryError::NotFound) => {
            GovernanceDomainAdapterError::NotFound
        }
        DecisionApplicationError::Repository(
            DecisionRepositoryError::Conflict
            | DecisionRepositoryError::StaleRevision
            | DecisionRepositoryError::EvidenceMismatch
            | DecisionRepositoryError::InvalidLimits,
        ) => GovernanceDomainAdapterError::Conflict,
        DecisionApplicationError::Repository(
            DecisionRepositoryError::Capacity | DecisionRepositoryError::Allocation,
        )
        | DecisionApplicationError::Allocation
        | DecisionApplicationError::Capacity => GovernanceDomainAdapterError::CapacityExceeded,
        DecisionApplicationError::Unavailable => GovernanceDomainAdapterError::Unavailable,
        DecisionApplicationError::Persistence
        | DecisionApplicationError::InvalidPersistentState => {
            GovernanceDomainAdapterError::PersistenceUnavailable
        }
    }
}
