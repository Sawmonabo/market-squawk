use market_squawk_domain::FairValueHierarchy;
use market_squawk_valuation::{
    ApprovalStatus, ClassificationRuleset, DecisionBasis, FairValueError, InputSignificance,
};

use super::{Scenario, actor, input, measurement, service};
use market_squawk_domain::Timestamp;

#[test]
fn override_approval_is_immutable_separated_expiring_revocable_and_audited()
-> Result<(), Box<dyn std::error::Error>> {
    let mut service = service(4);
    let base = service.classify(
        measurement(vec![input(
            Scenario {
                relation: market_squawk_valuation::InputInstrumentRelation::Similar,
                ..Scenario::default()
            },
            51,
            InputSignificance::Significant,
        )]),
        ClassificationRuleset::current(100)?,
    )?;
    assert_eq!(base.hierarchy(), FairValueHierarchy::Level2);
    assert!(matches!(
        service.propose_override(
            base.id(),
            FairValueHierarchy::Level1,
            "backdated judgment",
            actor("override-preparer"),
            Timestamp::from_unix_nanos(1_099),
            Timestamp::from_unix_nanos(1_500),
        ),
        Err(FairValueError::InvalidTime)
    ));
    assert!(matches!(
        service.approve(
            base.id(),
            actor("independent-approver"),
            Timestamp::from_unix_nanos(1_099),
            Timestamp::from_unix_nanos(1_450),
        ),
        Err(FairValueError::InvalidApprovalWindow)
    ));

    let proposal = service.propose_override(
        base.id(),
        FairValueHierarchy::Level1,
        "documented instrument-specific accounting judgment",
        actor("override-preparer"),
        Timestamp::from_unix_nanos(1_200),
        Timestamp::from_unix_nanos(1_500),
    )?;
    assert_eq!(proposal.decision().hierarchy(), FairValueHierarchy::Level1);
    assert_eq!(proposal.decision().measurement_id(), base.measurement_id());
    assert_eq!(proposal.decision().evidence_hash(), base.evidence_hash());
    assert!(matches!(
        proposal.decision().basis(),
        DecisionBasis::Override { .. }
    ));
    assert_eq!(base.hierarchy(), FairValueHierarchy::Level2);

    let separation = service.approve(
        proposal.decision().id(),
        actor("override-preparer"),
        Timestamp::from_unix_nanos(1_250),
        Timestamp::from_unix_nanos(1_450),
    );
    assert!(matches!(
        separation,
        Err(FairValueError::SeparationOfDuties)
    ));

    let approval = service.approve(
        proposal.decision().id(),
        actor("independent-approver"),
        Timestamp::from_unix_nanos(1_250),
        Timestamp::from_unix_nanos(1_450),
    )?;
    assert_eq!(
        service.approval_status(approval.id(), Timestamp::from_unix_nanos(1_249))?,
        ApprovalStatus::NotYetEffective
    );
    assert_eq!(
        service.approval_status(approval.id(), Timestamp::from_unix_nanos(1_300))?,
        ApprovalStatus::Active
    );
    assert_eq!(
        service.approval_status(approval.id(), Timestamp::from_unix_nanos(1_451))?,
        ApprovalStatus::Expired
    );

    let revocation = service.revoke_approval(
        approval.id(),
        actor("valuation-controller"),
        Timestamp::from_unix_nanos(1_350),
        "superseding measurement evidence received",
    )?;
    assert_eq!(revocation.approval_id(), approval.id());
    assert_eq!(
        service.approval_status(approval.id(), Timestamp::from_unix_nanos(1_351))?,
        ApprovalStatus::Revoked
    );

    let audit = service.audit_events(4)?;
    assert_eq!(audit.len(), 4);
    for pair in audit.windows(2) {
        assert_eq!(pair[1].previous_event_id(), Some(pair[0].id()));
    }
    assert!(matches!(
        service.audit_events(5),
        Err(FairValueError::QueryLimitExceeded { .. })
    ));
    Ok(())
}
