use std::collections::BTreeMap;
use std::sync::Arc;

use crate::CanonicalHasher;

use super::*;

#[derive(Clone, Debug)]
struct EligibleFairValue {
    measurement: Arc<ValuationMeasurement>,
    classification: Arc<ClassificationDecision>,
    approval: Arc<ValuationApproval>,
    classification_recorded_at: Timestamp,
    approval_recorded_at: Timestamp,
}

#[derive(Debug)]
struct SelectionAuditIndex {
    measurement_available_at: Vec<(MeasurementId, Timestamp)>,
    classified_at: Vec<(DecisionId, MeasurementId, Timestamp)>,
    override_at: Vec<(DecisionId, OverrideId, Timestamp)>,
    approved_at: Vec<(ValuationApprovalId, DecisionId, Timestamp)>,
    revoked_at: Vec<(ValuationApprovalId, ApprovalRevocationId, Timestamp)>,
}

impl SelectionAuditIndex {
    fn try_new(audit: &[Arc<FairValueAuditEvent>]) -> Result<Self, FairValueSelectionError> {
        let mut counts = [0_usize; 4];
        for event in audit {
            let family = match event.kind() {
                AuditEventKind::Classified { .. } => Some(0),
                AuditEventKind::OverrideProposed { .. } => Some(1),
                AuditEventKind::Approved { .. } => Some(2),
                AuditEventKind::Revoked { .. } => Some(3),
                AuditEventKind::MarketAccessApproved { .. } => None,
            };
            if let Some(family) = family {
                counts[family] = counts[family]
                    .checked_add(1)
                    .ok_or(FairValueError::Arithmetic)?;
            }
        }
        let mut value = Self {
            measurement_available_at: Vec::new(),
            classified_at: Vec::new(),
            override_at: Vec::new(),
            approved_at: Vec::new(),
            revoked_at: Vec::new(),
        };
        reserve(
            &mut value.measurement_available_at,
            counts[0],
            "audit measurements",
        )?;
        reserve(&mut value.classified_at, counts[0], "audit classifications")?;
        reserve(&mut value.override_at, counts[1], "audit overrides")?;
        reserve(&mut value.approved_at, counts[2], "audit approvals")?;
        reserve(&mut value.revoked_at, counts[3], "audit revocations")?;
        for event in audit {
            let recorded_at = event.occurred_at();
            match event.kind() {
                AuditEventKind::Classified {
                    measurement_id,
                    decision_id,
                } => {
                    value
                        .measurement_available_at
                        .push((measurement_id, recorded_at));
                    value
                        .classified_at
                        .push((decision_id, measurement_id, recorded_at));
                }
                AuditEventKind::OverrideProposed {
                    override_id,
                    decision_id,
                } => value
                    .override_at
                    .push((decision_id, override_id, recorded_at)),
                AuditEventKind::Approved {
                    approval_id,
                    decision_id,
                } => value
                    .approved_at
                    .push((approval_id, decision_id, recorded_at)),
                AuditEventKind::Revoked {
                    revocation_id,
                    approval_id,
                } => value
                    .revoked_at
                    .push((approval_id, revocation_id, recorded_at)),
                AuditEventKind::MarketAccessApproved { .. } => {}
            }
        }
        value
            .measurement_available_at
            .sort_unstable_by_key(|entry| (entry.0, entry.1));
        value.measurement_available_at.dedup_by(|left, right| {
            if left.0 == right.0 {
                let earliest = left.1.min(right.1);
                left.1 = earliest;
                right.1 = earliest;
                true
            } else {
                false
            }
        });
        value.classified_at.sort_unstable_by_key(|entry| entry.0);
        value.override_at.sort_unstable_by_key(|entry| entry.0);
        value.approved_at.sort_unstable_by_key(|entry| entry.0);
        value.revoked_at.sort_unstable_by_key(|entry| entry.0);
        if has_duplicate_key(&value.classified_at, |entry| entry.0)
            || has_duplicate_key(&value.override_at, |entry| entry.0)
            || has_duplicate_key(&value.approved_at, |entry| entry.0)
            || has_duplicate_key(&value.revoked_at, |entry| entry.0)
            || value.override_at.iter().any(|entry| {
                value
                    .classified_at
                    .binary_search_by_key(&entry.0, |classified| classified.0)
                    .is_ok()
            })
        {
            return Err(FairValueError::CorruptPersistence.into());
        }
        Ok(value)
    }

    fn measurement_is_available(&self, id: MeasurementId, as_of: Timestamp) -> bool {
        find(&self.measurement_available_at, id, |entry| entry.0)
            .is_some_and(|entry| entry.1 <= as_of)
    }

    fn decision_available_at(
        &self,
        measurement_id: MeasurementId,
        decision: &ClassificationDecision,
        as_of: Timestamp,
    ) -> Result<Option<Timestamp>, FairValueError> {
        let recorded_at = match decision.basis() {
            DecisionBasis::Rules => {
                let retained = find(&self.classified_at, decision.id(), |entry| entry.0)
                    .ok_or(FairValueError::CorruptPersistence)?;
                if retained.1 != measurement_id {
                    return Err(FairValueError::CorruptPersistence);
                }
                retained.2
            }
            DecisionBasis::Override {
                base_decision_id,
                override_id,
            } => {
                let base = find(&self.classified_at, base_decision_id, |entry| entry.0)
                    .ok_or(FairValueError::CorruptPersistence)?;
                let overridden = find(&self.override_at, decision.id(), |entry| entry.0)
                    .ok_or(FairValueError::CorruptPersistence)?;
                if base.1 != measurement_id || overridden.1 != override_id {
                    return Err(FairValueError::CorruptPersistence);
                }
                base.2.max(overridden.2)
            }
        };
        Ok((recorded_at <= as_of).then_some(recorded_at))
    }

    fn approval_available_at(
        &self,
        approval: &ValuationApproval,
        as_of: Timestamp,
    ) -> Result<Option<Timestamp>, FairValueError> {
        let retained = find(&self.approved_at, approval.id(), |entry| entry.0)
            .ok_or(FairValueError::CorruptPersistence)?;
        if retained.1 != approval.decision_id() {
            return Err(FairValueError::CorruptPersistence);
        }
        Ok((retained.2 <= as_of).then_some(retained.2))
    }

    fn applicable_revocation<'a>(
        &self,
        approval_id: ValuationApprovalId,
        revocation: Option<&'a ApprovalRevocation>,
        as_of: Timestamp,
    ) -> Result<Option<&'a ApprovalRevocation>, FairValueError> {
        let retained = find(&self.revoked_at, approval_id, |entry| entry.0);
        match (revocation, retained) {
            (None, None) => Ok(None),
            (Some(value), Some(entry))
                if value.id() == entry.1 && value.approval_id() == approval_id =>
            {
                Ok((entry.2 <= as_of).then_some(value))
            }
            (None, Some(_)) | (Some(_), None) | (Some(_), Some(_)) => {
                Err(FairValueError::CorruptPersistence)
            }
        }
    }
}

pub(crate) fn select_latest_from_retained(
    measurements: &BTreeMap<MeasurementId, Arc<ValuationMeasurement>>,
    decisions: &BTreeMap<DecisionId, Arc<ClassificationDecision>>,
    overrides: &BTreeMap<OverrideId, Arc<ValuationOverride>>,
    approvals: &BTreeMap<ValuationApprovalId, Arc<ValuationApproval>>,
    revocations: &BTreeMap<ValuationApprovalId, Arc<ApprovalRevocation>>,
    audit: &[Arc<FairValueAuditEvent>],
    request: FairValueSelectionRequest,
) -> Result<FairValueSelectionReceipt, FairValueSelectionError> {
    let audit = SelectionAuditIndex::try_new(audit)?;
    let matching_measurements = measurements
        .values()
        .filter(|measurement| {
            measurement_matches(request, measurement)
                && audit.measurement_is_available(measurement.id(), request.as_of())
        })
        .count();
    if matching_measurements == 0 {
        return receipt(
            request,
            FairValueSelectionDisposition::Complete,
            0,
            Vec::new(),
            None,
        );
    }

    let mut eligible = Vec::new();
    reserve(&mut eligible, request.max_eligible(), "eligible selections")?;
    for approval in approvals.values() {
        let Some(measurement) = measurements.get(&approval.measurement_id()) else {
            continue;
        };
        if !measurement_matches(request, measurement)
            || !audit.measurement_is_available(measurement.id(), request.as_of())
        {
            continue;
        }
        let decision = decisions
            .get(&approval.decision_id())
            .ok_or(FairValueError::CorruptPersistence)?;
        validate_chain(measurement, decision, approval, decisions, overrides)?;
        if decision.hierarchy() == FairValueHierarchy::Unclassified {
            continue;
        }
        let Some(classification_recorded_at) =
            audit.decision_available_at(measurement.id(), decision, request.as_of())?
        else {
            continue;
        };
        let Some(approval_recorded_at) = audit.approval_available_at(approval, request.as_of())?
        else {
            continue;
        };
        let applicable_revocation = audit.applicable_revocation(
            approval.id(),
            revocations.get(&approval.id()).map(AsRef::as_ref),
            request.as_of(),
        )?;
        if !decision_is_time_valid(decision, overrides, request.as_of())?
            || approval_status_at(approval, applicable_revocation, request.as_of())
                != ApprovalStatus::Active
        {
            continue;
        }
        if eligible.len() >= request.max_eligible() {
            return Err(FairValueError::LimitExceeded {
                resource: "eligible fair-value selections",
                observed: eligible
                    .len()
                    .checked_add(1)
                    .ok_or(FairValueError::Arithmetic)?,
                limit: request.max_eligible(),
            }
            .into());
        }
        eligible.push(EligibleFairValue {
            measurement: Arc::clone(measurement),
            classification: Arc::clone(decision),
            approval: Arc::clone(approval),
            classification_recorded_at,
            approval_recorded_at,
        });
    }
    eligible.sort_unstable_by(eligible_order);
    if eligible.is_empty() {
        return receipt(
            request,
            FairValueSelectionDisposition::Unavailable,
            matching_measurements,
            eligible,
            None,
        );
    }

    let leading = &eligible[0];
    let conflict = eligible.iter().skip(1).any(|candidate| {
        (candidate.measurement.measurement_at() == leading.measurement.measurement_at()
            && candidate.measurement.prepared_at() == leading.measurement.prepared_at()
            && candidate.measurement.id() != leading.measurement.id())
            || (candidate.measurement.id() == leading.measurement.id()
                && candidate.classification.id() != leading.classification.id())
    });
    let disposition = if conflict {
        FairValueSelectionDisposition::Conflict
    } else {
        FairValueSelectionDisposition::Complete
    };
    let selected = (!conflict).then(|| SelectedFairValueEvidence {
        measurement: Arc::clone(&leading.measurement),
        classification: Arc::clone(&leading.classification),
        approval: Arc::clone(&leading.approval),
        approval_status: ApprovalStatus::Active,
        applicable_revocation: None,
        classification_recorded_at: leading.classification_recorded_at,
        approval_recorded_at: leading.approval_recorded_at,
        evidence_hash: leading.measurement.evidence_hash(),
    });
    receipt(
        request,
        disposition,
        matching_measurements,
        eligible,
        selected,
    )
}

fn eligible_order(left: &EligibleFairValue, right: &EligibleFairValue) -> std::cmp::Ordering {
    right
        .measurement
        .measurement_at()
        .cmp(&left.measurement.measurement_at())
        .then_with(|| {
            right
                .measurement
                .prepared_at()
                .cmp(&left.measurement.prepared_at())
        })
        .then_with(|| left.measurement.id().cmp(&right.measurement.id()))
        .then_with(|| left.classification.id().cmp(&right.classification.id()))
        .then_with(|| {
            right
                .approval
                .approved_at()
                .cmp(&left.approval.approved_at())
        })
        .then_with(|| left.approval.id().cmp(&right.approval.id()))
}

fn receipt(
    request: FairValueSelectionRequest,
    disposition: FairValueSelectionDisposition,
    matching_measurements: usize,
    eligible: Vec<EligibleFairValue>,
    selected: Option<SelectedFairValueEvidence>,
) -> Result<FairValueSelectionReceipt, FairValueSelectionError> {
    let mut eligible_order = Vec::new();
    reserve(&mut eligible_order, eligible.len(), "selection receipt")?;
    for (index, candidate) in eligible.iter().enumerate() {
        eligible_order.push(FairValueSelectionOrderEntry {
            rank: index + 1,
            measurement_id: candidate.measurement.id(),
            decision_id: candidate.classification.id(),
            approval_id: candidate.approval.id(),
            measurement_at: candidate.measurement.measurement_at(),
            prepared_at: candidate.measurement.prepared_at(),
            classification_recorded_at: candidate.classification_recorded_at,
            approved_at: candidate.approval.approved_at(),
            approval_recorded_at: candidate.approval_recorded_at,
            expires_at: candidate.approval.expires_at(),
            hierarchy: candidate.classification.hierarchy(),
            ruleset_version: candidate.classification.ruleset_version(),
            ruleset_hash: candidate.classification.ruleset_hash(),
            evidence_hash: candidate.measurement.evidence_hash(),
        });
    }
    let hash = receipt_hash(
        request,
        disposition,
        matching_measurements,
        &eligible_order,
        selected.as_ref(),
    )?;
    Ok(FairValueSelectionReceipt {
        request,
        disposition,
        matching_measurements,
        eligible_order,
        selected,
        hash,
    })
}

fn receipt_hash(
    request: FairValueSelectionRequest,
    disposition: FairValueSelectionDisposition,
    matching_measurements: usize,
    order: &[FairValueSelectionOrderEntry],
    selected: Option<&SelectedFairValueEvidence>,
) -> Result<FairValueSelectionReceiptHash, FairValueError> {
    let mut hash = CanonicalHasher::new(b"market-squawk/fair-value-selection-receipt/v2");
    hash.bytes(request.instrument_id().as_uuid().as_bytes());
    hash.bytes(request.currency().as_str().as_bytes());
    hash.u8(crate::measurement::amount_basis_tag(request.basis()));
    match request.account_id() {
        Some(account_id) => {
            hash.u8(1);
            hash.bytes(account_id.as_uuid().as_bytes());
        }
        None => hash.u8(0),
    }
    hash.i64(request.as_of().unix_nanos());
    hash.u64(as_u64(request.max_eligible())?);
    hash.u8(disposition_tag(disposition));
    hash.u64(as_u64(matching_measurements)?);
    hash.u64(as_u64(order.len())?);
    for entry in order {
        hash.u64(as_u64(entry.rank())?);
        hash.fixed(entry.measurement_id().bytes());
        hash.fixed(entry.decision_id().bytes());
        hash.fixed(entry.approval_id().bytes());
        for time in [
            entry.measurement_at(),
            entry.prepared_at(),
            entry.classification_recorded_at(),
            entry.approved_at(),
            entry.approval_recorded_at(),
            entry.expires_at(),
        ] {
            hash.i64(time.unix_nanos());
        }
        hash.u8(hierarchy_tag(entry.hierarchy()));
        hash.u32(entry.ruleset_version());
        hash.fixed(entry.ruleset_hash().bytes());
        hash.fixed(entry.evidence_hash().bytes());
    }
    match selected {
        Some(value) => {
            hash.u8(1);
            hash.fixed(value.measurement().id().bytes());
            hash.fixed(value.classification().id().bytes());
            hash.fixed(value.approval().id().bytes());
            hash.u8(approval_status_tag(value.approval_status()));
            match value.applicable_revocation() {
                Some(revocation) => {
                    hash.u8(1);
                    hash.fixed(revocation.id().bytes());
                    hash.i64(revocation.revoked_at().unix_nanos());
                }
                None => hash.u8(0),
            }
            for time in [
                value.expires_at(),
                value.classification_recorded_at(),
                value.approval_recorded_at(),
            ] {
                hash.i64(time.unix_nanos());
            }
            hash.fixed(value.evidence_hash().bytes());
        }
        None => hash.u8(0),
    }
    Ok(FairValueSelectionReceiptHash(hash.finish()))
}

fn measurement_matches(request: FairValueSelectionRequest, value: &ValuationMeasurement) -> bool {
    value.instrument_id() == request.instrument_id()
        && value.amount().money().currency() == request.currency()
        && value.amount_basis() == request.basis()
        && request
            .account_id()
            .is_none_or(|account_id| value.account_id() == account_id)
        && value.measurement_at() <= request.as_of()
        && value.prepared_at() <= request.as_of()
}

fn validate_chain(
    measurement: &ValuationMeasurement,
    decision: &ClassificationDecision,
    approval: &ValuationApproval,
    decisions: &BTreeMap<DecisionId, Arc<ClassificationDecision>>,
    overrides: &BTreeMap<OverrideId, Arc<ValuationOverride>>,
) -> Result<(), FairValueError> {
    if decision.measurement_id() != measurement.id()
        || approval.measurement_id() != measurement.id()
        || approval.decision_id() != decision.id()
        || decision.evidence_hash() != measurement.evidence_hash()
    {
        return Err(FairValueError::CorruptPersistence);
    }
    match decision.basis() {
        DecisionBasis::Rules if approval.override_id().is_none() => Ok(()),
        DecisionBasis::Override {
            base_decision_id,
            override_id,
        } if approval.override_id() == Some(override_id) => {
            let value = overrides
                .get(&override_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            let base = decisions
                .get(&base_decision_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            if value.base_decision_id() == base_decision_id
                && base.measurement_id() == measurement.id()
                && base.basis() == DecisionBasis::Rules
                && base.evidence_hash() == decision.evidence_hash()
                && base.ruleset_version() == decision.ruleset_version()
                && base.ruleset_hash() == decision.ruleset_hash()
                && value.requested_hierarchy() == decision.hierarchy()
            {
                Ok(())
            } else {
                Err(FairValueError::CorruptPersistence)
            }
        }
        DecisionBasis::Rules | DecisionBasis::Override { .. } => {
            Err(FairValueError::CorruptPersistence)
        }
    }
}

fn decision_is_time_valid(
    decision: &ClassificationDecision,
    overrides: &BTreeMap<OverrideId, Arc<ValuationOverride>>,
    as_of: Timestamp,
) -> Result<bool, FairValueError> {
    match decision.basis() {
        DecisionBasis::Rules => Ok(true),
        DecisionBasis::Override { override_id, .. } => {
            let value = overrides
                .get(&override_id)
                .ok_or(FairValueError::CorruptPersistence)?;
            Ok(value.prepared_at() <= as_of && as_of <= value.expires_at())
        }
    }
}

pub(crate) fn approval_status_at(
    approval: &ValuationApproval,
    revocation: Option<&ApprovalRevocation>,
    at: Timestamp,
) -> ApprovalStatus {
    if at < approval.approved_at() {
        ApprovalStatus::NotYetEffective
    } else if revocation.is_some_and(|value| value.revoked_at() <= at) {
        ApprovalStatus::Revoked
    } else if at > approval.expires_at() {
        ApprovalStatus::Expired
    } else {
        ApprovalStatus::Active
    }
}

fn reserve<T>(
    values: &mut Vec<T>,
    additional: usize,
    resource: &'static str,
) -> Result<(), FairValueSelectionError> {
    values
        .try_reserve_exact(additional)
        .map_err(|_| FairValueSelectionError::TemporaryCapacityUnavailable { resource })
}

fn find<T, K: Ord>(values: &[T], key: K, select: impl Fn(&T) -> K) -> Option<&T> {
    values
        .binary_search_by_key(&key, select)
        .ok()
        .and_then(|index| values.get(index))
}

fn has_duplicate_key<T, K: Eq>(values: &[T], key: impl Fn(&T) -> K) -> bool {
    values.windows(2).any(|pair| key(&pair[0]) == key(&pair[1]))
}

fn as_u64(value: usize) -> Result<u64, FairValueError> {
    u64::try_from(value).map_err(|_| FairValueError::Arithmetic)
}

const fn disposition_tag(value: FairValueSelectionDisposition) -> u8 {
    match value {
        FairValueSelectionDisposition::Complete => 1,
        FairValueSelectionDisposition::Unavailable => 2,
        FairValueSelectionDisposition::Conflict => 3,
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

const fn approval_status_tag(value: ApprovalStatus) -> u8 {
    match value {
        ApprovalStatus::NotYetEffective => 1,
        ApprovalStatus::Active => 2,
        ApprovalStatus::Expired => 3,
        ApprovalStatus::Revoked => 4,
    }
}
