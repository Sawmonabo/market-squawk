//! Local governance-principal authority for sensitive application actions.

mod action;
mod authority;
mod domain;
mod error;
mod identity;

pub use action::{
    GovernanceActionPreview, GovernanceAuditError, GovernanceAuditKind, GovernanceAuditReceipt,
    GovernanceAuditSink, GovernanceAuthenticationTicket, GovernanceAuthorizedPrincipal,
    GovernanceCommitReceipt, GovernanceLimits, GovernancePreviewRequest, GovernancePrincipalPage,
    GovernancePrincipalSummary,
};
pub use authority::GovernanceAuthority;
pub use domain::{
    CanonicalGovernanceAction, DecisionGovernanceActionFactory, DecisionInvalidationKind,
    DecisionInvalidationProposal, DecisionReviewDisposition, DecisionReviewProposal,
    FairValueApprovalProposal, FairValueGovernanceActionFactory, FairValueMarketAccessConclusion,
    FairValueMarketAccessProposal, FairValueOverrideProposal, FairValueRequestedHierarchy,
    FairValueRevocationProposal, GovernanceDomainAdapterError, GovernanceDomainReceipt,
    GovernedActionCommitReceipt,
};
pub use error::GovernanceError;
pub use identity::{
    GovernanceActionDigest, GovernanceActionKind, GovernanceEffect, GovernancePreviewId,
    GovernancePrincipal, GovernancePrincipalAdmission, GovernancePrincipalId,
    GovernancePrincipalRegistration, GovernanceReceiptId, GovernanceRequestBinding, GovernanceRole,
    GovernanceRoleSet, GovernanceTicketId, GovernanceTimestamp,
};
pub(crate) use identity::{governance_principal_secret_key, governance_secret_operation_control};

#[cfg(test)]
use authority::TestPrincipal;

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use market_squawk_domain::Timestamp;
    use market_squawk_platform::SecretValue;
    use market_squawk_runtime::{ClientId, ServiceGeneration, WorkspaceId};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn action_bound_dual_authority_requires_distinct_principals_and_never_replays()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = Instant::now();
        let binding = GovernanceRequestBinding::try_new(
            WorkspaceId::try_from_uuid(Uuid::from_u128(1))?,
            ServiceGeneration::try_new(1)?,
            ClientId::try_from_uuid(Uuid::from_u128(2))?,
        )?;
        let alpha = test_principal(
            3,
            "Alpha",
            GovernanceRole::FairValueMarketAccessApprover,
            "alpha-credential",
        )?;
        let beta = test_principal(
            4,
            "Beta",
            GovernanceRole::FairValueMarketAccessApprover,
            "beta-credential",
        )?;
        let authority = GovernanceAuthority::for_test([alpha, beta])?;
        let preview = authority.preview_action(
            GovernancePreviewRequest::try_new(
                GovernanceActionKind::FairValueMarketAccess,
                binding,
                GovernanceActionDigest::from_bytes([7; 32]),
                GovernanceRoleSet::try_new([GovernanceRole::FairValueMarketAccessApprover])?,
                2,
                [
                    GovernancePrincipalId::try_from_uuid(Uuid::from_u128(3))?,
                    GovernancePrincipalId::try_from_uuid(Uuid::from_u128(4))?,
                ],
                Duration::from_secs(30),
            )?,
            now,
            Timestamp::from_unix_nanos(1),
        )?;
        let alpha_ticket = authority.authenticate_action(
            preview.preview_id(),
            GovernancePrincipalId::try_from_uuid(Uuid::from_u128(3))?,
            SecretValue::new("alpha-credential".to_owned())?,
            now,
            Timestamp::from_unix_nanos(1),
        )?;

        let other_preview = authority.preview_action(
            GovernancePreviewRequest::try_new(
                GovernanceActionKind::FairValueMarketAccess,
                binding,
                GovernanceActionDigest::from_bytes([8; 32]),
                GovernanceRoleSet::try_new([GovernanceRole::FairValueMarketAccessApprover])?,
                2,
                [
                    GovernancePrincipalId::try_from_uuid(Uuid::from_u128(3))?,
                    GovernancePrincipalId::try_from_uuid(Uuid::from_u128(4))?,
                ],
                Duration::from_secs(30),
            )?,
            now,
            Timestamp::from_unix_nanos(1),
        )?;
        assert_eq!(
            authority.commit_action(
                other_preview.preview_id(),
                [alpha_ticket.clone(), alpha_ticket.clone()],
                now,
                Timestamp::from_unix_nanos(1),
            ),
            Err(GovernanceError::TicketPreviewMismatch)
        );

        let beta_ticket = authority.authenticate_action(
            preview.preview_id(),
            GovernancePrincipalId::try_from_uuid(Uuid::from_u128(4))?,
            SecretValue::new("beta-credential".to_owned())?,
            now,
            Timestamp::from_unix_nanos(1),
        )?;
        let receipt = authority.commit_action(
            preview.preview_id(),
            [alpha_ticket.clone(), beta_ticket.clone()],
            now,
            Timestamp::from_unix_nanos(1),
        )?;
        assert_eq!(receipt.authorized_principals().len(), 2);
        assert_ne!(
            receipt.authorized_principals()[0].principal_id(),
            receipt.authorized_principals()[1].principal_id()
        );
        assert_eq!(
            authority.commit_action(
                preview.preview_id(),
                [alpha_ticket, beta_ticket],
                now,
                Timestamp::from_unix_nanos(1),
            ),
            Err(GovernanceError::TicketConsumed)
        );
        Ok(())
    }

    fn test_principal(
        id: u128,
        display_name: &str,
        role: GovernanceRole,
        credential: &str,
    ) -> Result<TestPrincipal, Box<dyn std::error::Error>> {
        Ok(TestPrincipal::try_new(
            GovernancePrincipal::try_new(
                GovernancePrincipalId::try_from_uuid(Uuid::from_u128(id))?,
                display_name,
                GovernanceRoleSet::try_new([role])?,
            )?,
            SecretValue::new(credential.to_owned())?,
        ))
    }
}
