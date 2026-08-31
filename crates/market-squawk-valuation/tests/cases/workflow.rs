use market_squawk_domain::{FairValueHierarchy, Timestamp, VenueId};
use market_squawk_valuation::{
    ApprovalStatus, ClassificationRuleset, FairValueError, MarketAccess,
};

use super::{CatalogFixture, TestResult, account, actor, instrument, measurement};

#[test]
fn durable_workflow_recovers_exact_state_and_blocks_level_one_override() -> TestResult {
    let fixture = CatalogFixture::open()?;
    let (decision_id, approval_id, access_id, audit_tail) = {
        let mut service = fixture.service(8)?;
        let access = service.approve_market_access(
            account()?,
            VenueId::try_from("XNYS")?,
            instrument()?,
            MarketAccess::Accessible,
            Timestamp::from_unix_nanos(900),
            Timestamp::from_unix_nanos(2_000),
            "reporting entity can transact in the assessed market",
            actor("access-preparer")?,
            Timestamp::from_unix_nanos(910),
            actor("access-approver")?,
            Timestamp::from_unix_nanos(920),
        )?;
        let base = service.classify(measurement(900, 4)?, ClassificationRuleset::current(100)?)?;
        assert!(matches!(
            service.propose_override(
                base.id(),
                FairValueHierarchy::Level1,
                "unsupported promotion",
                actor("override-preparer")?,
                Timestamp::from_unix_nanos(1_300),
                Timestamp::from_unix_nanos(1_800),
            ),
            Err(FairValueError::InvalidOverride)
        ));
        let proposal = service.propose_override(
            base.id(),
            FairValueHierarchy::Level3,
            "documented unobservable calibration controls the conclusion",
            actor("override-preparer")?,
            Timestamp::from_unix_nanos(1_300),
            Timestamp::from_unix_nanos(1_800),
        )?;
        assert!(matches!(
            service.propose_override(
                proposal.decision().id(),
                FairValueHierarchy::Level2,
                "an override cannot become the basis for another override",
                actor("nested-override-preparer")?,
                Timestamp::from_unix_nanos(1_350),
                Timestamp::from_unix_nanos(1_750),
            ),
            Err(FairValueError::InvalidOverride)
        ));
        let approval = service.approve(
            proposal.decision().id(),
            actor("independent-approver")?,
            Timestamp::from_unix_nanos(1_400),
            Timestamp::from_unix_nanos(1_700),
        )?;
        service.revoke_approval(
            approval.id(),
            actor("valuation-controller")?,
            Timestamp::from_unix_nanos(1_500),
            "superseding valuation evidence",
        )?;
        let audit = service.audit_events(8)?;
        assert_eq!(audit.len(), 5);
        for pair in audit.windows(2) {
            assert_eq!(pair[1].previous_event_id(), Some(pair[0].id()));
            assert!(pair[1].occurred_at() >= pair[0].occurred_at());
        }
        let expected_ids = audit.iter().map(|event| event.id()).collect::<Vec<_>>();
        let mut cursor = None;
        let mut paged_ids = Vec::new();
        loop {
            let page = service.audit_page(cursor, 2)?;
            assert_eq!(page.total_count(), expected_ids.len());
            if let (Some(previous), Some(first)) = (cursor, page.events().first()) {
                assert_eq!(first.previous_event_id(), Some(previous.event_id()));
            }
            paged_ids.extend(page.events().iter().map(|event| event.id()));
            let Some(next) = page.next_cursor() else {
                break;
            };
            cursor = Some(next);
        }
        assert_eq!(paged_ids, expected_ids);
        (
            proposal.decision().id(),
            approval.id(),
            access.id(),
            audit.last().ok_or("missing audit tail")?.id(),
        )
    };

    let reopened = fixture.service(8)?;
    assert_eq!(
        reopened
            .decision(decision_id)
            .ok_or("missing recovered decision")?
            .hierarchy(),
        FairValueHierarchy::Level3
    );
    assert!(reopened.market_access(access_id).is_some());
    assert_eq!(
        reopened.approval_status(approval_id, Timestamp::from_unix_nanos(1_600))?,
        ApprovalStatus::Revoked
    );
    assert_eq!(
        reopened
            .audit_events(8)?
            .last()
            .ok_or("missing recovered audit")?
            .id(),
        audit_tail
    );
    Ok(())
}

#[test]
fn unclassified_evidence_cannot_be_override_promoted() -> TestResult {
    let fixture = CatalogFixture::open()?;
    let mut service = fixture.service(4)?;
    let base = service.classify(measurement(1_001, 5)?, ClassificationRuleset::current(100)?)?;
    assert_eq!(base.hierarchy(), FairValueHierarchy::Unclassified);
    assert!(matches!(
        service.propose_override(
            base.id(),
            FairValueHierarchy::Level3,
            "inadmissible evidence requires a new measurement",
            actor("override-preparer")?,
            Timestamp::from_unix_nanos(1_300),
            Timestamp::from_unix_nanos(1_800),
        ),
        Err(FairValueError::InvalidOverride)
    ));
    Ok(())
}

#[test]
fn stale_service_position_rejects_before_any_second_durable_append() -> TestResult {
    let fixture = CatalogFixture::open()?;
    let mut first = fixture.service(4)?;
    let mut stale = fixture.service(4)?;
    first.approve_market_access(
        account()?,
        VenueId::try_from("XNYS")?,
        instrument()?,
        MarketAccess::Accessible,
        Timestamp::from_unix_nanos(900),
        Timestamp::from_unix_nanos(2_000),
        "first independently approved access conclusion",
        actor("first-preparer")?,
        Timestamp::from_unix_nanos(910),
        actor("first-approver")?,
        Timestamp::from_unix_nanos(920),
    )?;
    assert!(matches!(
        stale.approve_market_access(
            account()?,
            VenueId::try_from("ARCX")?,
            instrument()?,
            MarketAccess::Inaccessible,
            Timestamp::from_unix_nanos(900),
            Timestamp::from_unix_nanos(2_000),
            "stale writer must not append this conclusion",
            actor("stale-preparer")?,
            Timestamp::from_unix_nanos(930),
            actor("stale-approver")?,
            Timestamp::from_unix_nanos(940),
        ),
        Err(FairValueError::Persistence)
    ));
    drop(first);
    drop(stale);
    let reopened = fixture.service(4)?;
    assert_eq!(reopened.audit_events(4)?.len(), 1);
    Ok(())
}
