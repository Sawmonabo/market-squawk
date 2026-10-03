use std::collections::BTreeMap;
use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::Arc;

use market_squawk_domain::{
    AccountId, Currency, DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentId, Money,
    SourceId, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;

use super::*;
use crate::evidence::FairValueEvidenceParts;
use crate::measurement::ValuationInputSpec;
use crate::{
    ActorId, EvidenceOrigin, EvidenceVerification, FairValueEvidence, InputInstrumentRelation,
    InputObservability, InputSignificance, MarketAccess, MarketActivity, PriceAdjustment,
    ValuationAmount, ValuationAmountBasis, ValuationInput, ValuationMeasurementSpec,
    ValuationMethod,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct ApprovedChain {
    measurement: Arc<ValuationMeasurement>,
    decision: Arc<ClassificationDecision>,
    approval: Arc<ValuationApproval>,
    audit: [Arc<FairValueAuditEvent>; 2],
}

#[test]
fn latest_selection_is_deterministic_and_rejects_ineligible_authority() -> TestResult {
    let account: AccountId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse()?;
    let instrument: InstrumentId = "9f3914d3-9ef4-42f7-a707-3f2dcde861d1".parse()?;
    let other_instrument: InstrumentId = "7c7560e1-7d6a-4a76-b74d-aad9776939ad".parse()?;
    let missing_instrument: InstrumentId = "a41fe0e5-9b63-4c2d-8725-9563f333bfbe".parse()?;
    let currency = Currency::try_from("USD")?;
    let bound = NonZeroUsize::new(8).ok_or("invalid selection bound")?;

    let older = approved_chain(
        account,
        instrument,
        currency,
        ValuationAmountBasis::PerInstrumentUnit,
        100,
        1_500,
        1,
        100,
    )?;
    let latest = approved_chain(
        account,
        instrument,
        currency,
        ValuationAmountBasis::PerInstrumentUnit,
        200,
        1_500,
        2,
        100,
    )?;
    let expired = approved_chain(
        account,
        instrument,
        currency,
        ValuationAmountBasis::PerInstrumentUnit,
        300,
        900,
        3,
        100,
    )?;
    let revoked = approved_chain(
        account,
        instrument,
        currency,
        ValuationAmountBasis::PerInstrumentUnit,
        400,
        1_500,
        4,
        100,
    )?;
    let cross_instrument = approved_chain(
        account,
        other_instrument,
        currency,
        ValuationAmountBasis::PerInstrumentUnit,
        500,
        1_500,
        5,
        100,
    )?;
    let mut not_yet_recorded = approved_chain(
        account,
        instrument,
        currency,
        ValuationAmountBasis::PerInstrumentUnit,
        600,
        1_500,
        6,
        100,
    )?;
    not_yet_recorded.audit = [
        audit_event(
            33,
            AuditEventKind::Classified {
                measurement_id: not_yet_recorded.measurement.id(),
                decision_id: not_yet_recorded.decision.id(),
            },
            1_100,
        )?,
        audit_event(
            34,
            AuditEventKind::Approved {
                approval_id: not_yet_recorded.approval.id(),
                decision_id: not_yet_recorded.decision.id(),
            },
            1_101,
        )?,
    ];
    let unclassified = approved_chain(
        account,
        instrument,
        currency,
        ValuationAmountBasis::PerInstrumentUnit,
        700,
        1_500,
        7,
        0,
    )?;
    let basis_mismatch = approved_chain(
        account,
        instrument,
        currency,
        ValuationAmountBasis::ReportingEntityTotal,
        250,
        1_500,
        8,
        100,
    )?;
    assert_eq!(
        unclassified.decision.hierarchy(),
        FairValueHierarchy::Unclassified
    );
    let revocation = Arc::new(ApprovalRevocation::try_new(
        &revoked.approval,
        ActorId::try_from("controller")?,
        Timestamp::from_unix_nanos(800),
        "superseded evidence",
    )?);

    let chains = [
        &older,
        &latest,
        &expired,
        &revoked,
        &cross_instrument,
        &not_yet_recorded,
        &unclassified,
        &basis_mismatch,
    ];
    let mut audit = chains
        .iter()
        .flat_map(|chain| chain.audit.iter().cloned())
        .collect::<Vec<_>>();
    audit.push(audit_event(
        30,
        AuditEventKind::Revoked {
            revocation_id: revocation.id(),
            approval_id: revoked.approval.id(),
        },
        800,
    )?);
    let measurements = chains
        .iter()
        .map(|chain| (chain.measurement.id(), Arc::clone(&chain.measurement)))
        .collect::<BTreeMap<_, _>>();
    let mut decisions = chains
        .iter()
        .map(|chain| (chain.decision.id(), Arc::clone(&chain.decision)))
        .collect::<BTreeMap<_, _>>();
    let mut approvals = chains
        .iter()
        .map(|chain| (chain.approval.id(), Arc::clone(&chain.approval)))
        .collect::<BTreeMap<_, _>>();
    let revocations = BTreeMap::from([(revoked.approval.id(), revocation)]);
    let overrides = BTreeMap::new();
    let request = FairValueSelectionRequest::new(
        instrument,
        currency,
        ValuationAmountBasis::PerInstrumentUnit,
        Some(account),
        Timestamp::from_unix_nanos(1_000),
        bound,
    );

    let selected = select_latest_from_retained(
        &measurements,
        &decisions,
        &overrides,
        &approvals,
        &revocations,
        &audit,
        request,
    )?;
    assert_eq!(
        selected.disposition(),
        FairValueSelectionDisposition::Complete
    );
    assert_eq!(selected.matching_measurements(), 5);
    assert_eq!(selected.eligible_count(), 2);
    assert_eq!(selected.eligible_order()[0].rank(), 1);
    assert_eq!(
        selected.eligible_order()[0].measurement_id(),
        latest.measurement.id()
    );
    assert_eq!(
        selected.eligible_order()[1].measurement_id(),
        older.measurement.id()
    );
    assert!(
        selected
            .eligible_order()
            .iter()
            .all(|entry| entry.measurement_id() != unclassified.measurement.id())
    );
    assert!(
        selected
            .eligible_order()
            .iter()
            .all(|entry| entry.measurement_id() != basis_mismatch.measurement.id())
    );
    let evidence = selected.selected().ok_or("missing selected evidence")?;
    assert_eq!(evidence.measurement().id(), latest.measurement.id());
    assert_eq!(evidence.classification().id(), latest.decision.id());
    assert_eq!(evidence.approval().id(), latest.approval.id());
    assert_eq!(evidence.approval_status(), ApprovalStatus::Active);
    assert_eq!(evidence.expires_at(), latest.approval.expires_at());
    assert_eq!(evidence.evidence_hash(), latest.measurement.evidence_hash());
    assert_eq!(
        evidence.classification_recorded_at(),
        Timestamp::from_unix_nanos(201)
    );
    assert_eq!(
        evidence.approval_recorded_at(),
        Timestamp::from_unix_nanos(202)
    );
    assert!(evidence.applicable_revocation().is_none());
    let repeated = select_latest_from_retained(
        &measurements,
        &decisions,
        &overrides,
        &approvals,
        &revocations,
        &audit,
        request,
    )?;
    assert_eq!(repeated.hash(), selected.hash());

    let unavailable = select_latest_from_retained(
        &measurements,
        &decisions,
        &overrides,
        &approvals,
        &revocations,
        &audit,
        FairValueSelectionRequest::new(
            instrument,
            currency,
            ValuationAmountBasis::PerInstrumentUnit,
            Some(account),
            Timestamp::from_unix_nanos(2_000),
            bound,
        ),
    )?;
    assert_eq!(
        unavailable.disposition(),
        FairValueSelectionDisposition::Unavailable
    );
    assert!(unavailable.selected().is_none());

    let empty = select_latest_from_retained(
        &measurements,
        &decisions,
        &overrides,
        &approvals,
        &revocations,
        &audit,
        FairValueSelectionRequest::new(
            missing_instrument,
            currency,
            ValuationAmountBasis::PerInstrumentUnit,
            None,
            Timestamp::from_unix_nanos(1_000),
            bound,
        ),
    )?;
    assert_eq!(empty.disposition(), FairValueSelectionDisposition::Complete);
    assert_eq!(empty.matching_measurements(), 0);
    assert!(empty.selected().is_none());

    let conflicting_decision =
        Arc::new(ClassificationRuleset::current(101)?.classify(&latest.measurement)?);
    let conflicting_approval = Arc::new(ValuationApproval::try_new(
        &conflicting_decision,
        None,
        ActorId::try_from("second-approver")?,
        Timestamp::from_unix_nanos(700),
        Timestamp::from_unix_nanos(1_500),
    )?);
    decisions.insert(conflicting_decision.id(), Arc::clone(&conflicting_decision));
    approvals.insert(conflicting_approval.id(), Arc::clone(&conflicting_approval));
    audit.push(audit_event(
        31,
        AuditEventKind::Classified {
            measurement_id: latest.measurement.id(),
            decision_id: conflicting_decision.id(),
        },
        600,
    )?);
    audit.push(audit_event(
        32,
        AuditEventKind::Approved {
            approval_id: conflicting_approval.id(),
            decision_id: conflicting_decision.id(),
        },
        700,
    )?);
    let conflict = select_latest_from_retained(
        &measurements,
        &decisions,
        &overrides,
        &approvals,
        &revocations,
        &audit,
        request,
    )?;
    assert_eq!(
        conflict.disposition(),
        FairValueSelectionDisposition::Conflict
    );
    assert!(conflict.selected().is_none());
    Ok(())
}

fn approved_chain(
    account_id: AccountId,
    instrument_id: InstrumentId,
    currency: Currency,
    measurement_basis: ValuationAmountBasis,
    measurement_at: i64,
    expires_at: i64,
    marker: u8,
    quote_age: u64,
) -> TestResult<ApprovedChain> {
    let evidence_at = Timestamp::from_unix_nanos(measurement_at - 1);
    let input_amount = ValuationAmount::try_new(
        Money::new(Decimal::from(i64::from(marker)), currency),
        0,
        ValuationAmountBasis::PositionTotal,
    )?;
    let measurement_amount = ValuationAmount::try_new(
        Money::new(Decimal::from(i64::from(marker)), currency),
        0,
        measurement_basis,
    )?;
    let evidence = FairValueEvidence::try_from_parts(FairValueEvidenceParts {
        source_id: SourceId::try_from("selector-test")?,
        source_identifier: SourceIdentifier::try_from(format!("record-{marker}"))?,
        payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [marker; 32]),
        origin: EvidenceOrigin::Portfolio {
            revision: [marker; 32],
            account_id,
            position_quantity: Decimal::ONE,
            point_in_time_digest: [marker.wrapping_add(1); 32],
        },
        source_timestamp: None,
        effective_at: Some(evidence_at),
        published_at: None,
        available_at: Some(evidence_at),
        received_at: None,
        qualification_evaluated_at: None,
        qualification_valid_until: None,
        ingested_at: evidence_at,
        verification: EvidenceVerification::Verified,
    })?;
    let input = ValuationInput::try_from_spec(ValuationInputSpec {
        subject_instrument_id: instrument_id,
        reference_instrument_id: instrument_id,
        relationship: InputInstrumentRelation::Identical,
        amount: input_amount,
        significance: InputSignificance::Significant,
        observability: InputObservability::Observable,
        adjustment: PriceAdjustment::None,
        market_activity: MarketActivity::NotAssessed,
        market_access: MarketAccess::NotAssessed,
        market_access_assessment: None,
        data_quality: DataQuality::Estimated,
        evidence,
        use_assessment: None,
    })?;
    let measurement = Arc::new(ValuationMeasurement::try_new(ValuationMeasurementSpec {
        account_id,
        instrument_id,
        amount: measurement_amount,
        measurement_at: Timestamp::from_unix_nanos(measurement_at),
        prepared_at: Timestamp::from_unix_nanos(measurement_at + 1),
        prepared_by: ActorId::try_from("preparer")?,
        method: ValuationMethod::MarketApproach,
        inputs: vec![input],
    })?);
    let decision = Arc::new(ClassificationRuleset::current(quote_age)?.classify(&measurement)?);
    let approval = Arc::new(ValuationApproval::try_new(
        &decision,
        None,
        ActorId::try_from("approver")?,
        Timestamp::from_unix_nanos(measurement_at + 2),
        Timestamp::from_unix_nanos(expires_at),
    )?);
    let event_marker = marker.checked_mul(2).ok_or("audit marker overflow")?;
    let audit = [
        audit_event(
            event_marker,
            AuditEventKind::Classified {
                measurement_id: measurement.id(),
                decision_id: decision.id(),
            },
            measurement_at + 1,
        )?,
        audit_event(
            event_marker.checked_add(1).ok_or("audit marker overflow")?,
            AuditEventKind::Approved {
                approval_id: approval.id(),
                decision_id: decision.id(),
            },
            measurement_at + 2,
        )?,
    ];
    Ok(ApprovedChain {
        measurement,
        decision,
        approval,
        audit,
    })
}

fn audit_event(
    marker: u8,
    kind: AuditEventKind,
    recorded_at: i64,
) -> TestResult<Arc<FairValueAuditEvent>> {
    Ok(Arc::new(FairValueAuditEvent {
        id: AuditEventId([marker; 32]),
        sequence: u64::from(marker),
        previous_event_id: None,
        kind,
        actor: ActorId::try_from("catalog-writer")?,
        business_at: Timestamp::from_unix_nanos(recorded_at),
        appended_at: Timestamp::from_unix_nanos(recorded_at),
        retained_bytes: 0,
    }))
}
