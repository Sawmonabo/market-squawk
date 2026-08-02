//! Typed append-only fair-value record and audit authority.

mod authority;
mod hash;
mod recovery;

use market_squawk_domain::Timestamp;
use rusqlite::{OptionalExtension as _, Transaction, TransactionBehavior, params};
use sha2::{Digest as _, Sha256};

use self::hash::operation_digest;
use super::runs::CatalogAuthority;
use super::storage::{ResultBudget, sha256, trusted_catalog_now};
use super::types::CatalogError;

const MAX_OPERATION_RECORDS: usize = 16_384;
const MAX_OPERATION_LINKS: usize = 16_384;
const MAX_RECORD_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const MAX_ACTOR_BYTES: usize = 128;
const MAX_SNAPSHOT_RECORDS: usize = 100_000;
const MAX_SNAPSHOT_OPERATIONS: usize = 100_000;
const MAX_SNAPSHOT_MEMBERSHIPS: usize = 100_000;
const MAX_SNAPSHOT_LINKS: usize = 200_000;
const MEMBERSHIP_DECODED_BYTES: usize = 80;
const LINK_DECODED_BYTES: usize = 176;
const AUDIT_DECODED_FIXED_BYTES: usize = 256;

/// Closed family of immutable fair-value records.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FairValueRecordKind {
    /// Producer-derived evidence.
    Evidence,
    /// One valuation input.
    Input,
    /// One complete valuation measurement.
    Measurement,
    /// One deterministic or override decision.
    Decision,
    /// One governed override.
    Override,
    /// One independent decision approval.
    Approval,
    /// One immutable approval revocation.
    Revocation,
    /// One dual-approved reporting-entity market-access assessment.
    MarketAccess,
}

impl FairValueRecordKind {
    const fn tag(self) -> i64 {
        match self {
            Self::Evidence => 1,
            Self::Input => 2,
            Self::Measurement => 3,
            Self::Decision => 4,
            Self::Override => 5,
            Self::Approval => 6,
            Self::Revocation => 7,
            Self::MarketAccess => 8,
        }
    }

    fn from_tag(value: i64) -> Option<Self> {
        match value {
            1 => Some(Self::Evidence),
            2 => Some(Self::Input),
            3 => Some(Self::Measurement),
            4 => Some(Self::Decision),
            5 => Some(Self::Override),
            6 => Some(Self::Approval),
            7 => Some(Self::Revocation),
            8 => Some(Self::MarketAccess),
            _ => None,
        }
    }

    const fn table(self) -> &'static str {
        match self {
            Self::Evidence => "fair_value_evidence",
            Self::Input => "fair_value_inputs",
            Self::Measurement => "fair_value_measurements",
            Self::Decision => "fair_value_decisions",
            Self::Override => "fair_value_overrides",
            Self::Approval => "fair_value_approvals",
            Self::Revocation => "fair_value_revocations",
            Self::MarketAccess => "fair_value_market_access",
        }
    }
}

/// Closed semantic relationship between immutable fair-value records.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FairValueLinkRelation {
    /// Evidence supports an input.
    EvidenceToInput,
    /// Input belongs to a measurement.
    InputToMeasurement,
    /// Measurement is classified by a decision.
    MeasurementToDecision,
    /// A rules decision is the base for an override.
    DecisionToOverride,
    /// An override produces a replacement decision.
    OverrideToDecision,
    /// A decision is approved.
    DecisionToApproval,
    /// An approval is revoked.
    ApprovalToRevocation,
    /// A market-access assessment supports an input.
    MarketAccessToInput,
}

impl FairValueLinkRelation {
    const fn tag(self) -> i64 {
        match self {
            Self::EvidenceToInput => 1,
            Self::InputToMeasurement => 2,
            Self::MeasurementToDecision => 3,
            Self::DecisionToOverride => 4,
            Self::OverrideToDecision => 5,
            Self::DecisionToApproval => 6,
            Self::ApprovalToRevocation => 7,
            Self::MarketAccessToInput => 8,
        }
    }

    fn from_tag(value: i64) -> Option<Self> {
        match value {
            1 => Some(Self::EvidenceToInput),
            2 => Some(Self::InputToMeasurement),
            3 => Some(Self::MeasurementToDecision),
            4 => Some(Self::DecisionToOverride),
            5 => Some(Self::OverrideToDecision),
            6 => Some(Self::DecisionToApproval),
            7 => Some(Self::ApprovalToRevocation),
            8 => Some(Self::MarketAccessToInput),
            _ => None,
        }
    }

    const fn expected(self) -> (FairValueRecordKind, FairValueRecordKind) {
        match self {
            Self::EvidenceToInput => (FairValueRecordKind::Evidence, FairValueRecordKind::Input),
            Self::InputToMeasurement => {
                (FairValueRecordKind::Input, FairValueRecordKind::Measurement)
            }
            Self::MeasurementToDecision => (
                FairValueRecordKind::Measurement,
                FairValueRecordKind::Decision,
            ),
            Self::DecisionToOverride => {
                (FairValueRecordKind::Decision, FairValueRecordKind::Override)
            }
            Self::OverrideToDecision => {
                (FairValueRecordKind::Override, FairValueRecordKind::Decision)
            }
            Self::DecisionToApproval => {
                (FairValueRecordKind::Decision, FairValueRecordKind::Approval)
            }
            Self::ApprovalToRevocation => (
                FairValueRecordKind::Approval,
                FairValueRecordKind::Revocation,
            ),
            Self::MarketAccessToInput => (
                FairValueRecordKind::MarketAccess,
                FairValueRecordKind::Input,
            ),
        }
    }
}

/// Closed fair-value catalog operation family.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FairValueOperationKind {
    /// Persist a measurement and deterministic decision.
    Classify,
    /// Persist an override and its replacement decision.
    ProposeOverride,
    /// Persist an independent approval.
    Approve,
    /// Persist an immutable revocation.
    Revoke,
    /// Persist a dual-approved market-access assessment.
    ApproveMarketAccess,
}

impl FairValueOperationKind {
    const fn tag(self) -> i64 {
        match self {
            Self::Classify => 1,
            Self::ProposeOverride => 2,
            Self::Approve => 3,
            Self::Revoke => 4,
            Self::ApproveMarketAccess => 5,
        }
    }

    fn from_tag(value: i64) -> Option<Self> {
        match value {
            1 => Some(Self::Classify),
            2 => Some(Self::ProposeOverride),
            3 => Some(Self::Approve),
            4 => Some(Self::Revoke),
            5 => Some(Self::ApproveMarketAccess),
            _ => None,
        }
    }
}

/// One bounded opaque record whose semantic identity is owned by the valuation crate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FairValueCatalogRecord {
    kind: FairValueRecordKind,
    id: [u8; 32],
    payload: Box<[u8]>,
    payload_sha256: [u8; 32],
}

impl FairValueCatalogRecord {
    /// Validates one nonempty bounded immutable payload.
    pub fn try_new(
        kind: FairValueRecordKind,
        id: [u8; 32],
        payload: Vec<u8>,
    ) -> Result<Self, CatalogError> {
        if id == [0; 32] || payload.is_empty() || payload.len() > MAX_RECORD_PAYLOAD_BYTES {
            return Err(CatalogError::InvalidRecord);
        }
        let payload_sha256 = sha256(&payload);
        Ok(Self {
            kind,
            id,
            payload: payload.into_boxed_slice(),
            payload_sha256,
        })
    }

    /// Returns the closed record family.
    pub const fn kind(&self) -> FairValueRecordKind {
        self.kind
    }

    /// Returns the valuation-owned semantic identity.
    pub const fn id(&self) -> [u8; 32] {
        self.id
    }

    /// Returns the canonical versioned payload.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns the exact stored-payload identity.
    pub const fn payload_sha256(&self) -> [u8; 32] {
        self.payload_sha256
    }
}

/// One typed immutable record relationship.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FairValueCatalogLink {
    source_kind: FairValueRecordKind,
    source_id: [u8; 32],
    relation: FairValueLinkRelation,
    target_kind: FairValueRecordKind,
    target_id: [u8; 32],
}

impl FairValueCatalogLink {
    /// Validates family direction and nonzero identities.
    pub fn try_new(
        source_kind: FairValueRecordKind,
        source_id: [u8; 32],
        relation: FairValueLinkRelation,
        target_kind: FairValueRecordKind,
        target_id: [u8; 32],
    ) -> Result<Self, CatalogError> {
        if relation.expected() != (source_kind, target_kind)
            || source_id == [0; 32]
            || target_id == [0; 32]
        {
            return Err(CatalogError::InvalidRecord);
        }
        Ok(Self {
            source_kind,
            source_id,
            relation,
            target_kind,
            target_id,
        })
    }

    /// Returns the source record family and identity.
    pub const fn source(&self) -> (FairValueRecordKind, [u8; 32]) {
        (self.source_kind, self.source_id)
    }

    /// Returns the closed relationship kind.
    pub const fn relation(&self) -> FairValueLinkRelation {
        self.relation
    }

    /// Returns the target record family and identity.
    pub const fn target(&self) -> (FairValueRecordKind, [u8; 32]) {
        (self.target_kind, self.target_id)
    }
}

/// Canonical idempotent append request spanning records, links, and one audit event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FairValueCatalogOperation {
    id: [u8; 32],
    kind: FairValueOperationKind,
    actor: Box<str>,
    business_at: Timestamp,
    records: Box<[FairValueCatalogRecord]>,
    links: Box<[FairValueCatalogLink]>,
}

impl FairValueCatalogOperation {
    /// Canonicalizes a bounded operation and derives its idempotency identity.
    pub fn try_new(
        kind: FairValueOperationKind,
        actor: impl AsRef<str>,
        business_at: Timestamp,
        mut records: Vec<FairValueCatalogRecord>,
        mut links: Vec<FairValueCatalogLink>,
    ) -> Result<Self, CatalogError> {
        let actor = actor.as_ref();
        if records.is_empty()
            || records.len() > MAX_OPERATION_RECORDS
            || links.len() > MAX_OPERATION_LINKS
            || actor.is_empty()
            || actor.len() > MAX_ACTOR_BYTES
            || actor
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            return Err(CatalogError::InvalidRecord);
        }
        records.sort_unstable_by(|left, right| {
            left.kind
                .cmp(&right.kind)
                .then_with(|| left.id.cmp(&right.id))
        });
        if records
            .windows(2)
            .any(|pair| pair[0].kind == pair[1].kind && pair[0].id == pair[1].id)
        {
            return Err(CatalogError::InvalidRecord);
        }
        links.sort_unstable();
        if links.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CatalogError::InvalidRecord);
        }
        let id = operation_digest(kind, actor, business_at, &records, &links)?;
        Ok(Self {
            id,
            kind,
            actor: actor.into(),
            business_at,
            records: records.into_boxed_slice(),
            links: links.into_boxed_slice(),
        })
    }

    /// Returns the exact idempotency identity.
    pub const fn id(&self) -> [u8; 32] {
        self.id
    }

    /// Returns canonical operation membership count.
    pub const fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Returns canonical typed-link count.
    pub const fn link_count(&self) -> usize {
        self.links.len()
    }

    /// Returns immutable record identities in canonical operation order.
    pub fn record_identities(&self) -> impl Iterator<Item = (FairValueRecordKind, [u8; 32])> + '_ {
        self.records.iter().map(|record| (record.kind, record.id))
    }
}

/// Whether an exact durable operation was inserted or replayed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FairValueCommitDisposition {
    /// The operation and audit event were newly appended.
    Inserted,
    /// The exact already-complete operation was returned without another audit event.
    Replay,
}

/// Durable commit result carrying the catalog-trusted append coordinate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FairValueCatalogCommit {
    disposition: FairValueCommitDisposition,
    audit_sequence: u64,
    audit_id: [u8; 32],
    appended_at: Timestamp,
    record_count: usize,
    operation_count: usize,
    membership_count: usize,
    link_count: usize,
    position: FairValueCatalogPosition,
}

impl FairValueCatalogCommit {
    /// Returns insert-versus-replay disposition.
    pub const fn disposition(self) -> FairValueCommitDisposition {
        self.disposition
    }

    /// Returns the one-based immutable audit sequence.
    pub const fn audit_sequence(self) -> u64 {
        self.audit_sequence
    }

    /// Returns the hash-chain event identity.
    pub const fn audit_id(self) -> [u8; 32] {
        self.audit_id
    }

    /// Returns catalog-trusted append time, distinct from business time.
    pub const fn appended_at(self) -> Timestamp {
        self.appended_at
    }

    /// Returns total immutable fair-value records after the transaction.
    pub const fn record_count(self) -> usize {
        self.record_count
    }

    /// Returns total fair-value operations after the transaction.
    pub const fn operation_count(self) -> usize {
        self.operation_count
    }

    /// Returns total operation memberships after the transaction.
    pub const fn membership_count(self) -> usize {
        self.membership_count
    }

    /// Returns total typed links after the transaction.
    pub const fn link_count(self) -> usize {
        self.link_count
    }

    /// Returns the exact durable position after the transaction.
    pub const fn position(self) -> FairValueCatalogPosition {
        self.position
    }
}

/// Opaque compare-and-swap coordinate for one exact durable fair-value head.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FairValueCatalogPosition {
    records: usize,
    operations: usize,
    memberships: usize,
    links: usize,
    last_audit_sequence: u64,
    last_audit_id: Option<[u8; 32]>,
}

impl FairValueCatalogPosition {
    /// Returns total immutable records at this position.
    pub const fn record_count(self) -> usize {
        self.records
    }

    /// Returns total operations at this position.
    pub const fn operation_count(self) -> usize {
        self.operations
    }

    /// Returns total operation memberships at this position.
    pub const fn membership_count(self) -> usize {
        self.memberships
    }

    /// Returns total typed links at this position.
    pub const fn link_count(self) -> usize {
        self.links
    }

    /// Returns the latest one-based audit sequence, or zero for an empty catalog.
    pub const fn last_audit_sequence(self) -> u64 {
        self.last_audit_sequence
    }

    /// Returns the latest audit identity, or `None` for an empty catalog.
    pub const fn last_audit_id(self) -> Option<[u8; 32]> {
        self.last_audit_id
    }
}

/// One validated durable fair-value audit event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FairValueCatalogAuditEvent {
    sequence: u64,
    id: [u8; 32],
    previous_id: Option<[u8; 32]>,
    operation_id: [u8; 32],
    kind: FairValueOperationKind,
    actor: Box<str>,
    business_at: Timestamp,
    appended_at: Timestamp,
    records: Box<[(FairValueRecordKind, [u8; 32])]>,
    links: Box<[FairValueCatalogLink]>,
}

impl FairValueCatalogAuditEvent {
    /// Returns the one-based append sequence.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the event hash identity.
    pub const fn id(&self) -> [u8; 32] {
        self.id
    }

    /// Returns the previous event identity.
    pub const fn previous_id(&self) -> Option<[u8; 32]> {
        self.previous_id
    }

    /// Returns the exact idempotent operation identity.
    pub const fn operation_id(&self) -> [u8; 32] {
        self.operation_id
    }

    /// Returns the closed operation family.
    pub const fn kind(&self) -> FairValueOperationKind {
        self.kind
    }

    /// Returns the responsible actor.
    pub fn actor(&self) -> &str {
        &self.actor
    }

    /// Returns the caller's validated business timestamp.
    pub const fn business_at(&self) -> Timestamp {
        self.business_at
    }

    /// Returns catalog-trusted append time.
    pub const fn appended_at(&self) -> Timestamp {
        self.appended_at
    }

    /// Returns canonical operation membership without decoding record payloads again.
    pub fn records(&self) -> &[(FairValueRecordKind, [u8; 32])] {
        &self.records
    }

    /// Returns canonical typed relationships retained by the operation.
    pub fn links(&self) -> &[FairValueCatalogLink] {
        &self.links
    }
}

/// Bounded, fully validated recovery snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FairValueCatalogSnapshot {
    records: Box<[FairValueCatalogRecord]>,
    audit: Box<[FairValueCatalogAuditEvent]>,
    membership_count: usize,
    link_count: usize,
    position: FairValueCatalogPosition,
}

/// Explicit independent ceilings for one complete fair-value recovery read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FairValueCatalogSnapshotLimits {
    max_records: usize,
    max_operations: usize,
    max_memberships: usize,
    max_links: usize,
}

impl FairValueCatalogSnapshotLimits {
    /// Process-wide maximum immutable records recoverable in one snapshot.
    pub const MAX_RECORDS: usize = MAX_SNAPSHOT_RECORDS;
    /// Process-wide maximum operations recoverable in one snapshot.
    pub const MAX_OPERATIONS: usize = MAX_SNAPSHOT_OPERATIONS;
    /// Process-wide maximum operation memberships recoverable in one snapshot.
    pub const MAX_MEMBERSHIPS: usize = MAX_SNAPSHOT_MEMBERSHIPS;
    /// Process-wide maximum typed links recoverable in one snapshot.
    pub const MAX_LINKS: usize = MAX_SNAPSHOT_LINKS;

    /// Validates positive recovery ceilings against fixed process-wide bounds.
    pub fn try_new(
        max_records: usize,
        max_operations: usize,
        max_memberships: usize,
        max_links: usize,
    ) -> Result<Self, CatalogError> {
        if max_records == 0
            || max_records > MAX_SNAPSHOT_RECORDS
            || max_operations == 0
            || max_operations > MAX_SNAPSHOT_OPERATIONS
            || max_memberships == 0
            || max_memberships > MAX_SNAPSHOT_MEMBERSHIPS
            || max_links == 0
            || max_links > MAX_SNAPSHOT_LINKS
        {
            return Err(CatalogError::InvalidLimit);
        }
        Ok(Self {
            max_records,
            max_operations,
            max_memberships,
            max_links,
        })
    }

    /// Returns the configured immutable-record ceiling.
    pub const fn max_records(self) -> usize {
        self.max_records
    }

    /// Returns the configured operation ceiling.
    pub const fn max_operations(self) -> usize {
        self.max_operations
    }

    /// Returns the configured operation-membership ceiling.
    pub const fn max_memberships(self) -> usize {
        self.max_memberships
    }

    /// Returns the configured typed-link ceiling.
    pub const fn max_links(self) -> usize {
        self.max_links
    }
}

impl FairValueCatalogSnapshot {
    /// Returns all immutable records in family/identity order.
    pub fn records(&self) -> &[FairValueCatalogRecord] {
        &self.records
    }

    /// Returns the validated audit chain in append order.
    pub fn audit(&self) -> &[FairValueCatalogAuditEvent] {
        &self.audit
    }

    /// Returns the validated global operation-membership count.
    pub const fn membership_count(&self) -> usize {
        self.membership_count
    }

    /// Returns the validated global typed-link count.
    pub const fn link_count(&self) -> usize {
        self.link_count
    }

    /// Returns the exact durable head validated by this snapshot.
    pub const fn position(&self) -> FairValueCatalogPosition {
        self.position
    }
}
