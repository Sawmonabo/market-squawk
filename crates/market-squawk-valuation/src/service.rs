//! Durable bounded fair-value workflow service.

mod memory;
mod operations;
mod queries;
mod recovery;

use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::sync::Arc;

use market_squawk_data::{
    FairValueCatalogAuditEvent, FairValueCatalogCapability, FairValueCatalogCommit,
    FairValueCatalogOperation, FairValueCatalogPosition, FairValueCatalogSnapshot,
    FairValueCatalogSnapshotLimits, FairValueCommitDisposition, FairValueLinkRelation,
    FairValueOperationKind, FairValueRecordKind,
};
use market_squawk_domain::{AccountId, FairValueHierarchy, InstrumentId, Timestamp, VenueId};

use self::memory::checked_mul;
use crate::approval::{
    ApprovalRevocation, ApprovalStatus, OverrideId, OverrideProposal, ValuationApproval,
    ValuationApprovalId, ValuationOverride,
};
use crate::measurement::{ActorId, MeasurementId, ValuationMeasurement};
use crate::persistence;
use crate::rules::{ClassificationDecision, ClassificationRuleset, DecisionBasis, DecisionId};
use crate::{
    ApprovalRevocationId, ApprovedMarketAccess, FairValueError, MarketAccess,
    MarketAccessAssessmentId, checked_add,
};

const HARD_MAX_MEASUREMENTS: usize = FairValueCatalogSnapshotLimits::MAX_RECORDS;
const HARD_MAX_INPUTS: usize = 4_096;
const HARD_MAX_RECORDS_PER_FAMILY: usize = FairValueCatalogSnapshotLimits::MAX_RECORDS;
const HARD_MAX_QUERY_RESULTS: usize = 100_000;
const HARD_MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;
// Conservative allocator/index charges cover BTree node slack, key + Arc storage, Arc control
// blocks, and audit Vec spare capacity. Domain objects separately report their inline/dynamic size.
const DOMAIN_INDEX_ENTRY_OVERHEAD_BYTES: usize = 256;
const IDENTITY_INDEX_ENTRY_OVERHEAD_BYTES: usize = 160;
const AUDIT_INDEX_ENTRY_OVERHEAD_BYTES: usize = 96;

digest_id!(
    /// Catalog-owned SHA-256 identity of one hash-chained audit event.
    AuditEventId
);

/// Caller-selected service and query bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FairValueLimitInput {
    /// Maximum immutable measurements retained.
    pub max_measurements: usize,
    /// Maximum inputs in one measurement admitted by the service.
    pub max_inputs_per_measurement: usize,
    /// Maximum decisions, overrides, approvals, revocations, or access records per family.
    pub max_records_per_family: usize,
    /// Maximum rows returned by one query.
    pub max_query_results: usize,
    /// Maximum estimated bytes retained by this service.
    pub max_retained_bytes: usize,
}

/// Validated service limits whose aggregate worst case remains fully recoverable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FairValueLimits {
    max_measurements: usize,
    max_inputs_per_measurement: usize,
    max_records_per_family: usize,
    max_query_results: usize,
    max_retained_bytes: usize,
    catalog_limits: FairValueCatalogSnapshotLimits,
}

impl FairValueLimits {
    /// Validates positive caller limits and their checked aggregate catalog footprint.
    ///
    /// The aggregate bound covers the worst permitted mix of classification, override, approval,
    /// revocation, and market-access operations. A configuration that could write more state than
    /// the public catalog recovery API can read is rejected before service startup.
    ///
    /// # Errors
    ///
    /// Returns [`FairValueError::LimitExceeded`] for zero, excessive, or non-recoverable values.
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
            (
                "records per family",
                input.max_records_per_family,
                HARD_MAX_RECORDS_PER_FAMILY,
            ),
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
        let family = input.max_records_per_family;
        let inputs = input.max_inputs_per_measurement;
        let input_members = checked_mul(family, inputs)?;
        let max_records = checked_add(
            checked_mul(input_members, 2)?,
            checked_add(input.max_measurements, checked_mul(family, 5)?)?,
        )?;
        let max_operations = checked_mul(family, 4)?;
        let max_memberships = checked_mul(family, checked_add(checked_mul(inputs, 2)?, 5)?)?;
        let max_links = checked_mul(family, checked_add(checked_mul(inputs, 3)?, 3)?)?;
        let catalog_limits = FairValueCatalogSnapshotLimits::try_new(
            max_records,
            max_operations,
            max_memberships,
            max_links,
        )
        .map_err(|_| FairValueError::LimitExceeded {
            resource: "aggregate recoverable catalog footprint",
            observed: max_records
                .max(max_operations)
                .max(max_memberships)
                .max(max_links),
            limit: FairValueCatalogSnapshotLimits::MAX_LINKS,
        })?;
        Ok(Self {
            max_measurements: input.max_measurements,
            max_inputs_per_measurement: input.max_inputs_per_measurement,
            max_records_per_family: input.max_records_per_family,
            max_query_results: input.max_query_results,
            max_retained_bytes: input.max_retained_bytes,
            catalog_limits,
        })
    }

    /// Returns the maximum number of producer-derived inputs in one measurement.
    pub const fn max_inputs_per_measurement(self) -> usize {
        self.max_inputs_per_measurement
    }

    /// Returns the maximum number of records exposed by one bounded query.
    pub const fn max_query_results(self) -> usize {
        self.max_query_results
    }
}

/// Exact subject of a durable hash-chained workflow audit event.
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
        revocation_id: ApprovalRevocationId,
        /// Exact revoked approval.
        approval_id: ValuationApprovalId,
    },
    /// Reporting-entity market access was independently approved.
    MarketAccessApproved {
        /// Immutable market-access assessment identity.
        assessment_id: MarketAccessAssessmentId,
    },
}

/// One immutable catalog-backed audit event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FairValueAuditEvent {
    id: AuditEventId,
    sequence: u64,
    previous_event_id: Option<AuditEventId>,
    kind: AuditEventKind,
    actor: ActorId,
    business_at: Timestamp,
    appended_at: Timestamp,
    retained_bytes: usize,
}

impl FairValueAuditEvent {
    /// Returns catalog hash-chain identity.
    pub const fn id(&self) -> AuditEventId {
        self.id
    }

    /// Returns one-based append sequence.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns previous catalog hash-chain event.
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

    /// Returns the domain business time supplied by the validated operation.
    pub const fn business_at(&self) -> Timestamp {
        self.business_at
    }

    /// Returns the catalog-trusted append time.
    pub const fn occurred_at(&self) -> Timestamp {
        self.appended_at
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CatalogUsage {
    records: usize,
    operations: usize,
    memberships: usize,
    links: usize,
}

#[derive(Debug)]
struct AuditDraft {
    kind: AuditEventKind,
    actor: ActorId,
    business_at: Timestamp,
    retained_bytes: usize,
}

impl AuditDraft {
    fn try_new(
        kind: AuditEventKind,
        actor: ActorId,
        business_at: Timestamp,
    ) -> Result<Self, FairValueError> {
        let retained_bytes = checked_add(size_of::<FairValueAuditEvent>(), actor.retained_bytes())?;
        Ok(Self {
            kind,
            actor,
            business_at,
            retained_bytes,
        })
    }

    fn finish(
        self,
        commit: FairValueCatalogCommit,
        previous_event_id: Option<AuditEventId>,
    ) -> FairValueAuditEvent {
        FairValueAuditEvent {
            id: AuditEventId(commit.audit_id()),
            sequence: commit.audit_sequence(),
            previous_event_id,
            kind: self.kind,
            actor: self.actor,
            business_at: self.business_at,
            appended_at: commit.appended_at(),
            retained_bytes: self.retained_bytes,
        }
    }
}

/// Bounded single-writer service over append-only local catalog state.
#[derive(Debug)]
pub struct FairValueService {
    catalog: FairValueCatalogCapability,
    limits: FairValueLimits,
    measurements: BTreeMap<MeasurementId, Arc<ValuationMeasurement>>,
    decisions: BTreeMap<DecisionId, Arc<ClassificationDecision>>,
    overrides: BTreeMap<OverrideId, Arc<ValuationOverride>>,
    approvals: BTreeMap<ValuationApprovalId, Arc<ValuationApproval>>,
    revocations: BTreeMap<ValuationApprovalId, Arc<ApprovalRevocation>>,
    market_access: BTreeMap<MarketAccessAssessmentId, Arc<ApprovedMarketAccess>>,
    audit: Vec<Arc<FairValueAuditEvent>>,
    record_ids: BTreeSet<(FairValueRecordKind, [u8; 32])>,
    operation_ids: BTreeSet<[u8; 32]>,
    position: FairValueCatalogPosition,
    usage: CatalogUsage,
    retained_bytes: usize,
}
