//! Audit-chain reconstruction and cross-record operation validation.

use super::*;

pub(super) fn recover_audit(
    snapshot: &FairValueCatalogSnapshot,
    state: &persistence::RecoveredState,
) -> Result<Vec<Arc<FairValueAuditEvent>>, FairValueError> {
    let mut revocations_by_id = BTreeMap::new();
    for revocation in state.revocations.values() {
        if revocations_by_id
            .insert(revocation.id(), Arc::clone(revocation))
            .is_some()
        {
            return Err(FairValueError::CorruptPersistence);
        }
    }
    let mut events = Vec::new();
    events
        .try_reserve_exact(snapshot.audit().len())
        .map_err(|_| FairValueError::Arithmetic)?;
    for source in snapshot.audit() {
        let kind = recovered_event_kind(source, state, &revocations_by_id)?;
        validate_operation_shape(source, kind, state, &revocations_by_id)?;
        let actor =
            ActorId::try_from(source.actor()).map_err(|_| FairValueError::CorruptPersistence)?;
        validate_audit_subject(source, kind, &actor, state, &revocations_by_id)?;
        let retained_bytes = checked_add(size_of::<FairValueAuditEvent>(), actor.retained_bytes())?;
        events.push(Arc::new(FairValueAuditEvent {
            id: AuditEventId(source.id()),
            sequence: source.sequence(),
            previous_event_id: source.previous_id().map(AuditEventId),
            kind,
            actor,
            business_at: source.business_at(),
            appended_at: source.appended_at(),
            retained_bytes,
        }));
    }
    Ok(events)
}

fn recovered_event_kind(
    event: &FairValueCatalogAuditEvent,
    state: &persistence::RecoveredState,
    revocations_by_id: &BTreeMap<ApprovalRevocationId, Arc<ApprovalRevocation>>,
) -> Result<AuditEventKind, FairValueError> {
    match event.kind() {
        FairValueOperationKind::Classify => {
            let measurement_id =
                exactly_one_record(event, FairValueRecordKind::Measurement).map(MeasurementId)?;
            let decision_id =
                exactly_one_record(event, FairValueRecordKind::Decision).map(DecisionId)?;
            let decision = state
                .decisions
                .get(&decision_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            if decision.measurement_id() != measurement_id
                || !matches!(decision.basis(), DecisionBasis::Rules)
            {
                return Err(FairValueError::CorruptPersistence);
            }
            Ok(AuditEventKind::Classified {
                measurement_id,
                decision_id,
            })
        }
        FairValueOperationKind::ProposeOverride => {
            let override_id =
                exactly_one_record(event, FairValueRecordKind::Override).map(OverrideId)?;
            let decision_id =
                exactly_one_record(event, FairValueRecordKind::Decision).map(DecisionId)?;
            let decision = state
                .decisions
                .get(&decision_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            if !matches!(
                decision.basis(),
                DecisionBasis::Override { override_id: value, .. } if value == override_id
            ) {
                return Err(FairValueError::CorruptPersistence);
            }
            Ok(AuditEventKind::OverrideProposed {
                override_id,
                decision_id,
            })
        }
        FairValueOperationKind::Approve => {
            let approval_id = exactly_one_record(event, FairValueRecordKind::Approval)
                .map(ValuationApprovalId)?;
            let approval = state
                .approvals
                .get(&approval_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            Ok(AuditEventKind::Approved {
                approval_id,
                decision_id: approval.decision_id(),
            })
        }
        FairValueOperationKind::Revoke => {
            let revocation_id = exactly_one_record(event, FairValueRecordKind::Revocation)
                .map(ApprovalRevocationId)?;
            let revocation = revocations_by_id
                .get(&revocation_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            Ok(AuditEventKind::Revoked {
                revocation_id,
                approval_id: revocation.approval_id(),
            })
        }
        FairValueOperationKind::ApproveMarketAccess => {
            let assessment_id = exactly_one_record(event, FairValueRecordKind::MarketAccess)
                .map(MarketAccessAssessmentId)?;
            if !state.market_access.contains_key(&assessment_id) {
                return Err(FairValueError::CorruptPersistence);
            }
            Ok(AuditEventKind::MarketAccessApproved { assessment_id })
        }
    }
}

fn exactly_one_record(
    event: &FairValueCatalogAuditEvent,
    kind: FairValueRecordKind,
) -> Result<[u8; 32], FairValueError> {
    let mut values = event
        .records()
        .iter()
        .filter_map(|(record_kind, id)| (*record_kind == kind).then_some(*id));
    let value = values.next().ok_or(FairValueError::CorruptPersistence)?;
    if values.next().is_some() {
        Err(FairValueError::CorruptPersistence)
    } else {
        Ok(value)
    }
}

fn validate_audit_subject(
    event: &FairValueCatalogAuditEvent,
    kind: AuditEventKind,
    actor: &ActorId,
    state: &persistence::RecoveredState,
    revocations_by_id: &BTreeMap<ApprovalRevocationId, Arc<ApprovalRevocation>>,
) -> Result<(), FairValueError> {
    let (expected_actor, expected_time) = match kind {
        AuditEventKind::Classified { measurement_id, .. } => {
            let value = state
                .measurements
                .get(&measurement_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            (value.prepared_by(), value.prepared_at())
        }
        AuditEventKind::OverrideProposed { override_id, .. } => {
            let value = state
                .overrides
                .get(&override_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            (value.prepared_by(), value.prepared_at())
        }
        AuditEventKind::Approved { approval_id, .. } => {
            let value = state
                .approvals
                .get(&approval_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            (value.approved_by(), value.approved_at())
        }
        AuditEventKind::Revoked { revocation_id, .. } => {
            let value = revocations_by_id
                .get(&revocation_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            (value.revoked_by(), value.revoked_at())
        }
        AuditEventKind::MarketAccessApproved { assessment_id } => {
            let value = state
                .market_access
                .get(&assessment_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            (value.approved_by(), value.approved_at())
        }
    };
    if expected_actor != actor || expected_time != event.business_at() {
        Err(FairValueError::CorruptPersistence)
    } else {
        Ok(())
    }
}

fn validate_operation_shape(
    event: &FairValueCatalogAuditEvent,
    kind: AuditEventKind,
    state: &persistence::RecoveredState,
    revocations_by_id: &BTreeMap<ApprovalRevocationId, Arc<ApprovalRevocation>>,
) -> Result<(), FairValueError> {
    let mut expected_records = BTreeSet::new();
    let mut expected_links = BTreeSet::new();
    match kind {
        AuditEventKind::Classified {
            measurement_id,
            decision_id,
        } => {
            let measurement = state
                .measurements
                .get(&measurement_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            for input in measurement.inputs() {
                expected_records.insert((
                    FairValueRecordKind::Evidence,
                    input.evidence().hash().bytes(),
                ));
                expected_records.insert((FairValueRecordKind::Input, input.id().bytes()));
                expected_links.insert((
                    (
                        FairValueRecordKind::Evidence,
                        input.evidence().hash().bytes(),
                    ),
                    FairValueLinkRelation::EvidenceToInput,
                    (FairValueRecordKind::Input, input.id().bytes()),
                ));
                expected_links.insert((
                    (FairValueRecordKind::Input, input.id().bytes()),
                    FairValueLinkRelation::InputToMeasurement,
                    (FairValueRecordKind::Measurement, measurement_id.bytes()),
                ));
                if let Some(access) = input.market_access_assessment() {
                    expected_links.insert((
                        (FairValueRecordKind::MarketAccess, access.id().bytes()),
                        FairValueLinkRelation::MarketAccessToInput,
                        (FairValueRecordKind::Input, input.id().bytes()),
                    ));
                }
            }
            expected_records.insert((FairValueRecordKind::Measurement, measurement_id.bytes()));
            expected_records.insert((FairValueRecordKind::Decision, decision_id.bytes()));
            expected_links.insert((
                (FairValueRecordKind::Measurement, measurement_id.bytes()),
                FairValueLinkRelation::MeasurementToDecision,
                (FairValueRecordKind::Decision, decision_id.bytes()),
            ));
        }
        AuditEventKind::OverrideProposed {
            override_id,
            decision_id,
        } => {
            let value = state
                .overrides
                .get(&override_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            expected_records.insert((FairValueRecordKind::Override, override_id.bytes()));
            expected_records.insert((FairValueRecordKind::Decision, decision_id.bytes()));
            expected_links.insert((
                (
                    FairValueRecordKind::Decision,
                    value.base_decision_id().bytes(),
                ),
                FairValueLinkRelation::DecisionToOverride,
                (FairValueRecordKind::Override, override_id.bytes()),
            ));
            expected_links.insert((
                (FairValueRecordKind::Override, override_id.bytes()),
                FairValueLinkRelation::OverrideToDecision,
                (FairValueRecordKind::Decision, decision_id.bytes()),
            ));
        }
        AuditEventKind::Approved {
            approval_id,
            decision_id,
        } => {
            expected_records.insert((FairValueRecordKind::Approval, approval_id.bytes()));
            expected_links.insert((
                (FairValueRecordKind::Decision, decision_id.bytes()),
                FairValueLinkRelation::DecisionToApproval,
                (FairValueRecordKind::Approval, approval_id.bytes()),
            ));
        }
        AuditEventKind::Revoked {
            revocation_id,
            approval_id,
        } => {
            if !revocations_by_id.contains_key(&revocation_id) {
                return Err(FairValueError::CorruptPersistence);
            }
            expected_records.insert((FairValueRecordKind::Revocation, revocation_id.bytes()));
            expected_links.insert((
                (FairValueRecordKind::Approval, approval_id.bytes()),
                FairValueLinkRelation::ApprovalToRevocation,
                (FairValueRecordKind::Revocation, revocation_id.bytes()),
            ));
        }
        AuditEventKind::MarketAccessApproved { assessment_id } => {
            expected_records.insert((FairValueRecordKind::MarketAccess, assessment_id.bytes()));
        }
    }
    let actual_records = event.records().iter().copied().collect::<BTreeSet<_>>();
    let actual_links = event
        .links()
        .iter()
        .map(|link| (link.source(), link.relation(), link.target()))
        .collect::<BTreeSet<_>>();
    if actual_records != expected_records
        || actual_records.len() != event.records().len()
        || actual_links != expected_links
        || actual_links.len() != event.links().len()
    {
        Err(FairValueError::CorruptPersistence)
    } else {
        Ok(())
    }
}
