//! Bounded immutable fair-value workflow service.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::Arc;

use market_squawk_domain::{FairValueHierarchy, InstrumentId, Timestamp};

use crate::approval::{
    ApprovalRevocation, ApprovalStatus, OverrideId, OverrideProposal, ValuationApproval,
    ValuationApprovalId, ValuationOverride,
};
use crate::measurement::{ActorId, MeasurementId, ValuationMeasurement};
use crate::rules::{ClassificationDecision, ClassificationRuleset, DecisionBasis, DecisionId};
use crate::{CanonicalHasher, FairValueError, checked_add};

const HARD_MAX_MEASUREMENTS: usize = 1_000_000;
const HARD_MAX_INPUTS: usize = 4_096;
const HARD_MAX_RECORDS: usize = 4_000_000;
const HARD_MAX_QUERY_RESULTS: usize = 100_000;
const HARD_MAX_RETAINED_BYTES: usize = 2 * 1024 * 1024 * 1024;

digest_id!(
    /// SHA-256 content identity of one hash-chained audit event.
    AuditEventId
);

/// Caller-selected service and query bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FairValueLimitInput {
    /// Maximum immutable measurements retained.
    pub max_measurements: usize,
    /// Maximum inputs in one measurement admitted by the service.
    pub max_inputs_per_measurement: usize,
    /// Maximum decisions, overrides, approvals, revocations, or audit events per family.
    pub max_records_per_family: usize,
    /// Maximum rows returned by one query.
    pub max_query_results: usize,
    /// Maximum estimated bytes retained by this service.
    pub max_retained_bytes: usize,
}

/// Validated hard-ceiling-bounded service limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FairValueLimits {
    max_measurements: usize,
    max_inputs_per_measurement: usize,
    max_records_per_family: usize,
    max_query_results: usize,
    max_retained_bytes: usize,
}

impl FairValueLimits {
    /// Validates positive caller limits against fixed process ceilings.
    ///
    /// # Errors
    ///
    /// Returns [`FairValueError::LimitExceeded`] for zero or excessive values.
    pub fn try_new(input: FairValueLimitInput) -> Result<Self, FairValueError> {
        let values = [
            (
                "measurements",
                input.max_measurements,
                HARD_MAX_MEASUREMENTS,
            ),
            (
                "measurement inputs",
                input.max_inputs_per_measurement,
                HARD_MAX_INPUTS,
            ),
            ("records", input.max_records_per_family, HARD_MAX_RECORDS),
            (
                "query results",
                input.max_query_results,
                HARD_MAX_QUERY_RESULTS,
            ),
            (
                "retained bytes",
                input.max_retained_bytes,
                HARD_MAX_RETAINED_BYTES,
            ),
        ];
        if let Some((resource, observed, limit)) = values
            .into_iter()
            .find(|(_, observed, limit)| *observed == 0 || observed > limit)
        {
            return Err(FairValueError::LimitExceeded {
                resource,
                observed,
                limit,
            });
        }
        Ok(Self {
            max_measurements: input.max_measurements,
            max_inputs_per_measurement: input.max_inputs_per_measurement,
            max_records_per_family: input.max_records_per_family,
            max_query_results: input.max_query_results,
            max_retained_bytes: input.max_retained_bytes,
        })
    }
}

/// Exact subject of a hash-chained workflow audit event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuditEventKind {
    /// Measurement and deterministic decision were retained atomically.
    Classified {
        /// Measurement identity.
        measurement_id: MeasurementId,
        /// Decision identity.
        decision_id: DecisionId,
    },
    /// Override and replacement decision were retained atomically.
    OverrideProposed {
        /// Override identity.
        override_id: OverrideId,
        /// Replacement decision identity.
        decision_id: DecisionId,
    },
    /// Independent approval was granted.
    Approved {
        /// Approval identity.
        approval_id: ValuationApprovalId,
        /// Exact approved decision.
        decision_id: DecisionId,
    },
    /// Approval was immutably revoked.
    Revoked {
        /// Revocation identity.
        revocation_id: crate::ApprovalRevocationId,
        /// Exact revoked approval.
        approval_id: ValuationApprovalId,
    },
}

/// One immutable, hash-chained audit event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FairValueAuditEvent {
    id: AuditEventId,
    sequence: u64,
    previous_event_id: Option<AuditEventId>,
    kind: AuditEventKind,
    actor: ActorId,
    occurred_at: Timestamp,
    retained_bytes: usize,
}

impl FairValueAuditEvent {
    fn try_new(
        sequence: u64,
        previous_event_id: Option<AuditEventId>,
        kind: AuditEventKind,
        actor: ActorId,
        occurred_at: Timestamp,
    ) -> Result<Self, FairValueError> {
        let mut hash = CanonicalHasher::new(b"market-squawk/fair-value-audit/v1");
        hash.u64(sequence);
        match previous_event_id {
            Some(value) => {
                hash.u8(1);
                hash.fixed(value.bytes());
            }
            None => hash.u8(0),
        }
        hash_event_kind(&mut hash, kind);
        hash.bytes(actor.as_str().as_bytes());
        hash.i64(occurred_at.unix_nanos());
        let retained_bytes = checked_add(size_of::<Self>(), actor.retained_bytes())?;
        Ok(Self {
            id: AuditEventId(hash.finish()),
            sequence,
            previous_event_id,
            kind,
            actor,
            occurred_at,
            retained_bytes,
        })
    }

    /// Returns event content identity.
    pub const fn id(&self) -> AuditEventId {
        self.id
    }

    /// Returns one-based append sequence.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns previous hash-chain event.
    pub const fn previous_event_id(&self) -> Option<AuditEventId> {
        self.previous_event_id
    }

    /// Returns exact event subject.
    pub const fn kind(&self) -> AuditEventKind {
        self.kind
    }

    /// Returns responsible actor.
    pub const fn actor(&self) -> &ActorId {
        &self.actor
    }

    /// Returns event time.
    pub const fn occurred_at(&self) -> Timestamp {
        self.occurred_at
    }
}

/// Bounded single-writer service retaining immutable fair-value workflow state.
#[derive(Debug)]
pub struct FairValueService {
    limits: FairValueLimits,
    measurements: BTreeMap<MeasurementId, Arc<ValuationMeasurement>>,
    decisions: BTreeMap<DecisionId, Arc<ClassificationDecision>>,
    overrides: BTreeMap<OverrideId, Arc<ValuationOverride>>,
    approvals: BTreeMap<ValuationApprovalId, Arc<ValuationApproval>>,
    revocations: BTreeMap<ValuationApprovalId, Arc<ApprovalRevocation>>,
    audit: Vec<Arc<FairValueAuditEvent>>,
    retained_bytes: usize,
}

impl FairValueService {
    /// Constructs an empty service under validated limits.
    pub fn new(limits: FairValueLimits) -> Self {
        Self {
            limits,
            measurements: BTreeMap::new(),
            decisions: BTreeMap::new(),
            overrides: BTreeMap::new(),
            approvals: BTreeMap::new(),
            revocations: BTreeMap::new(),
            audit: Vec::new(),
            retained_bytes: 0,
        }
    }

    /// Classifies and atomically retains one immutable measurement and rules decision.
    ///
    /// # Errors
    ///
    /// Rejects excessive inputs, record/byte bounds, or classification arithmetic failures.
    pub fn classify(
        &mut self,
        measurement: ValuationMeasurement,
        ruleset: ClassificationRuleset,
    ) -> Result<Arc<ClassificationDecision>, FairValueError> {
        if measurement.inputs().len() > self.limits.max_inputs_per_measurement {
            return Err(FairValueError::LimitExceeded {
                resource: "measurement inputs",
                observed: measurement.inputs().len(),
                limit: self.limits.max_inputs_per_measurement,
            });
        }
        let decision = Arc::new(ruleset.classify(&measurement)?);
        if let Some(existing) = self.decisions.get(&decision.id()) {
            return Ok(Arc::clone(existing));
        }
        let new_measurement = !self.measurements.contains_key(&measurement.id());
        self.ensure_family_capacity("decisions", self.decisions.len(), 1)?;
        if new_measurement {
            self.ensure_count(
                "measurements",
                self.measurements.len(),
                1,
                self.limits.max_measurements,
            )?;
        }
        let event = self.next_event(
            AuditEventKind::Classified {
                measurement_id: measurement.id(),
                decision_id: decision.id(),
            },
            measurement.prepared_by().clone(),
            measurement.prepared_at(),
        )?;
        let added = checked_add(
            decision.retained_bytes(),
            checked_add(
                event.retained_bytes,
                if new_measurement {
                    measurement.retained_bytes()
                } else {
                    0
                },
            )?,
        )?;
        self.ensure_retained_bytes(added)?;
        let measurement_id = measurement.id();
        if new_measurement {
            self.measurements
                .insert(measurement_id, Arc::new(measurement));
        }
        self.decisions.insert(decision.id(), Arc::clone(&decision));
        self.commit_event(event, added)?;
        Ok(decision)
    }

    /// Creates a new immutable override and decision without changing source evidence.
    ///
    /// # Errors
    ///
    /// Rejects missing base decisions, invalid judgment/lifetime, or service bounds.
    pub fn propose_override(
        &mut self,
        base_decision_id: DecisionId,
        requested_hierarchy: FairValueHierarchy,
        justification: &str,
        prepared_by: ActorId,
        prepared_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<OverrideProposal, FairValueError> {
        let base = self
            .decisions
            .get(&base_decision_id)
            .cloned()
            .ok_or(FairValueError::DecisionNotFound)?;
        let measurement = self
            .measurements
            .get(&base.measurement_id())
            .ok_or(FairValueError::MeasurementNotFound)?;
        if prepared_at < measurement.prepared_at() {
            return Err(FairValueError::InvalidTime);
        }
        let valuation_override = Arc::new(ValuationOverride::try_new(
            &base,
            requested_hierarchy,
            justification,
            prepared_by,
            prepared_at,
            expires_at,
        )?);
        let decision = Arc::new(ClassificationDecision::overridden(
            &base,
            valuation_override.id(),
            requested_hierarchy,
        )?);
        if let (Some(existing_override), Some(existing_decision)) = (
            self.overrides.get(&valuation_override.id()),
            self.decisions.get(&decision.id()),
        ) {
            return Ok(OverrideProposal::new(
                Arc::clone(existing_override),
                Arc::clone(existing_decision),
            ));
        }
        self.ensure_family_capacity("overrides", self.overrides.len(), 1)?;
        self.ensure_family_capacity("decisions", self.decisions.len(), 1)?;
        let event = self.next_event(
            AuditEventKind::OverrideProposed {
                override_id: valuation_override.id(),
                decision_id: decision.id(),
            },
            valuation_override.prepared_by().clone(),
            valuation_override.prepared_at(),
        )?;
        let added = checked_add(
            valuation_override.retained_bytes(),
            checked_add(decision.retained_bytes(), event.retained_bytes)?,
        )?;
        self.ensure_retained_bytes(added)?;
        self.overrides
            .insert(valuation_override.id(), Arc::clone(&valuation_override));
        self.decisions.insert(decision.id(), Arc::clone(&decision));
        self.commit_event(event, added)?;
        Ok(OverrideProposal::new(valuation_override, decision))
    }

    /// Independently approves one exact rules or override decision.
    ///
    /// # Errors
    ///
    /// Rejects missing state, same-actor preparation/approval, invalid lifetime, or bounds.
    pub fn approve(
        &mut self,
        decision_id: DecisionId,
        approved_by: ActorId,
        approved_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Arc<ValuationApproval>, FairValueError> {
        let decision = self
            .decisions
            .get(&decision_id)
            .cloned()
            .ok_or(FairValueError::DecisionNotFound)?;
        let measurement = self
            .measurements
            .get(&decision.measurement_id())
            .ok_or(FairValueError::MeasurementNotFound)?;
        if approved_at < measurement.prepared_at() {
            return Err(FairValueError::InvalidApprovalWindow);
        }
        if measurement.prepared_by() == &approved_by {
            return Err(FairValueError::SeparationOfDuties);
        }
        let override_id = match decision.basis() {
            DecisionBasis::Rules => None,
            DecisionBasis::Override { override_id, .. } => {
                let valuation_override = self
                    .overrides
                    .get(&override_id)
                    .ok_or(FairValueError::InvalidOverride)?;
                if valuation_override.prepared_by() == &approved_by {
                    return Err(FairValueError::SeparationOfDuties);
                }
                if approved_at < valuation_override.prepared_at()
                    || expires_at > valuation_override.expires_at()
                {
                    return Err(FairValueError::InvalidApprovalWindow);
                }
                Some(override_id)
            }
        };
        let approval = Arc::new(ValuationApproval::try_new(
            &decision,
            override_id,
            approved_by,
            approved_at,
            expires_at,
        )?);
        if let Some(existing) = self.approvals.get(&approval.id()) {
            return Ok(Arc::clone(existing));
        }
        self.ensure_family_capacity("approvals", self.approvals.len(), 1)?;
        let event = self.next_event(
            AuditEventKind::Approved {
                approval_id: approval.id(),
                decision_id,
            },
            approval.approved_by().clone(),
            approved_at,
        )?;
        let added = checked_add(approval.retained_bytes(), event.retained_bytes)?;
        self.ensure_retained_bytes(added)?;
        self.approvals.insert(approval.id(), Arc::clone(&approval));
        self.commit_event(event, added)?;
        Ok(approval)
    }

    /// Appends an immutable revocation without mutating the approval.
    ///
    /// # Errors
    ///
    /// Rejects missing/already-revoked approvals, invalid time/text, or bounds.
    pub fn revoke_approval(
        &mut self,
        approval_id: ValuationApprovalId,
        revoked_by: ActorId,
        revoked_at: Timestamp,
        reason: &str,
    ) -> Result<Arc<ApprovalRevocation>, FairValueError> {
        if self.revocations.contains_key(&approval_id) {
            return Err(FairValueError::AlreadyRevoked);
        }
        let approval = self
            .approvals
            .get(&approval_id)
            .cloned()
            .ok_or(FairValueError::ApprovalNotFound)?;
        let revocation = Arc::new(ApprovalRevocation::try_new(
            &approval, revoked_by, revoked_at, reason,
        )?);
        self.ensure_family_capacity("revocations", self.revocations.len(), 1)?;
        let event = self.next_event(
            AuditEventKind::Revoked {
                revocation_id: revocation.id(),
                approval_id,
            },
            revocation.revoked_by().clone(),
            revoked_at,
        )?;
        let added = checked_add(revocation.retained_bytes(), event.retained_bytes)?;
        self.ensure_retained_bytes(added)?;
        self.revocations
            .insert(approval_id, Arc::clone(&revocation));
        self.commit_event(event, added)?;
        Ok(revocation)
    }

    /// Evaluates immutable approval and revocation times at one query instant.
    ///
    /// # Errors
    ///
    /// Returns [`FairValueError::ApprovalNotFound`] for an unknown identity.
    pub fn approval_status(
        &self,
        approval_id: ValuationApprovalId,
        at: Timestamp,
    ) -> Result<ApprovalStatus, FairValueError> {
        let approval = self
            .approvals
            .get(&approval_id)
            .ok_or(FairValueError::ApprovalNotFound)?;
        if at < approval.approved_at() {
            return Ok(ApprovalStatus::NotYetEffective);
        }
        if self
            .revocations
            .get(&approval_id)
            .is_some_and(|revocation| revocation.revoked_at() <= at)
        {
            return Ok(ApprovalStatus::Revoked);
        }
        if at > approval.expires_at() {
            Ok(ApprovalStatus::Expired)
        } else {
            Ok(ApprovalStatus::Active)
        }
    }

    /// Returns one immutable measurement by content identity.
    pub fn measurement(&self, id: MeasurementId) -> Option<Arc<ValuationMeasurement>> {
        self.measurements.get(&id).map(Arc::clone)
    }

    /// Returns one immutable classification decision by content identity.
    pub fn decision(&self, id: DecisionId) -> Option<Arc<ClassificationDecision>> {
        self.decisions.get(&id).map(Arc::clone)
    }

    /// Returns one immutable override by content identity.
    pub fn valuation_override(&self, id: OverrideId) -> Option<Arc<ValuationOverride>> {
        self.overrides.get(&id).map(Arc::clone)
    }

    /// Returns one immutable approval by content identity.
    pub fn approval(&self, id: ValuationApprovalId) -> Option<Arc<ValuationApproval>> {
        self.approvals.get(&id).map(Arc::clone)
    }

    /// Returns an immutable revocation for an approval when one exists.
    pub fn revocation(&self, approval_id: ValuationApprovalId) -> Option<Arc<ApprovalRevocation>> {
        self.revocations.get(&approval_id).map(Arc::clone)
    }

    /// Returns decisions for one instrument in deterministic ID order under an explicit bound.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive result limits.
    pub fn decisions_for_instrument(
        &self,
        instrument_id: InstrumentId,
        limit: usize,
    ) -> Result<Vec<Arc<ClassificationDecision>>, FairValueError> {
        self.validate_query_limit(limit)?;
        Ok(self
            .decisions
            .values()
            .filter(|decision| {
                self.measurements
                    .get(&decision.measurement_id())
                    .is_some_and(|measurement| measurement.instrument_id() == instrument_id)
            })
            .take(limit)
            .map(Arc::clone)
            .collect())
    }

    /// Returns oldest-to-newest hash-chained audit events under an explicit bound.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive result limits.
    pub fn audit_events(
        &self,
        limit: usize,
    ) -> Result<Vec<Arc<FairValueAuditEvent>>, FairValueError> {
        self.validate_query_limit(limit)?;
        Ok(self.audit.iter().take(limit).map(Arc::clone).collect())
    }

    /// Returns estimated bytes retained by immutable service state.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn validate_query_limit(&self, requested: usize) -> Result<(), FairValueError> {
        if requested == 0 || requested > self.limits.max_query_results {
            Err(FairValueError::QueryLimitExceeded {
                requested,
                limit: self.limits.max_query_results,
            })
        } else {
            Ok(())
        }
    }

    fn ensure_family_capacity(
        &self,
        resource: &'static str,
        current: usize,
        added: usize,
    ) -> Result<(), FairValueError> {
        self.ensure_count(resource, current, added, self.limits.max_records_per_family)
    }

    fn ensure_count(
        &self,
        resource: &'static str,
        current: usize,
        added: usize,
        limit: usize,
    ) -> Result<(), FairValueError> {
        let observed = checked_add(current, added)?;
        if observed > limit {
            Err(FairValueError::LimitExceeded {
                resource,
                observed,
                limit,
            })
        } else {
            Ok(())
        }
    }

    fn ensure_retained_bytes(&self, added: usize) -> Result<(), FairValueError> {
        let observed = checked_add(self.retained_bytes, added)?;
        if observed > self.limits.max_retained_bytes {
            Err(FairValueError::RetainedBytesExceeded {
                observed,
                limit: self.limits.max_retained_bytes,
            })
        } else {
            Ok(())
        }
    }

    fn next_event(
        &self,
        kind: AuditEventKind,
        actor: ActorId,
        occurred_at: Timestamp,
    ) -> Result<FairValueAuditEvent, FairValueError> {
        self.ensure_family_capacity("audit events", self.audit.len(), 1)?;
        let sequence = u64::try_from(self.audit.len())
            .map_err(|_| FairValueError::Arithmetic)?
            .checked_add(1)
            .ok_or(FairValueError::Arithmetic)?;
        FairValueAuditEvent::try_new(
            sequence,
            self.audit.last().map(|event| event.id()),
            kind,
            actor,
            occurred_at,
        )
    }

    fn commit_event(
        &mut self,
        event: FairValueAuditEvent,
        added: usize,
    ) -> Result<(), FairValueError> {
        self.audit.push(Arc::new(event));
        self.retained_bytes = checked_add(self.retained_bytes, added)?;
        Ok(())
    }
}

fn hash_event_kind(hash: &mut CanonicalHasher, kind: AuditEventKind) {
    match kind {
        AuditEventKind::Classified {
            measurement_id,
            decision_id,
        } => {
            hash.u8(1);
            hash.fixed(measurement_id.bytes());
            hash.fixed(decision_id.bytes());
        }
        AuditEventKind::OverrideProposed {
            override_id,
            decision_id,
        } => {
            hash.u8(2);
            hash.fixed(override_id.bytes());
            hash.fixed(decision_id.bytes());
        }
        AuditEventKind::Approved {
            approval_id,
            decision_id,
        } => {
            hash.u8(3);
            hash.fixed(approval_id.bytes());
            hash.fixed(decision_id.bytes());
        }
        AuditEventKind::Revoked {
            revocation_id,
            approval_id,
        } => {
            hash.u8(4);
            hash.fixed(revocation_id.bytes());
            hash.fixed(approval_id.bytes());
        }
    }
}
