//! Deterministic point-in-time fair-value selection receipts.

mod engine;

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::Arc;

use market_squawk_domain::Currency;
use thiserror::Error;

use crate::ValuationAmountBasis;

use super::super::*;

pub(crate) use engine::{approval_status_at, select_latest_from_retained};

digest_id!(
    /// Versioned canonical identity of one complete fair-value selection receipt.
    FairValueSelectionReceiptHash
);

/// Typed failures specific to bounded fair-value selection.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FairValueSelectionError {
    /// The retained fair-value domain state or configured query bound rejected the request.
    #[error(transparent)]
    FairValue(#[from] FairValueError),
    /// A bounded temporary selection allocation could not be reserved.
    #[error("fair-value temporary {resource} capacity is unavailable")]
    TemporaryCapacityUnavailable {
        /// Bounded temporary resource.
        resource: &'static str,
    },
}

/// Caller requirements for one deterministic point-in-time fair-value selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FairValueSelectionRequest {
    instrument_id: InstrumentId,
    currency: Currency,
    basis: ValuationAmountBasis,
    account_id: Option<AccountId>,
    as_of: Timestamp,
    max_eligible: NonZeroUsize,
}

impl FairValueSelectionRequest {
    /// Creates an exact-instrument request under a nonzero eligible-result bound.
    pub const fn new(
        instrument_id: InstrumentId,
        currency: Currency,
        basis: ValuationAmountBasis,
        account_id: Option<AccountId>,
        as_of: Timestamp,
        max_eligible: NonZeroUsize,
    ) -> Self {
        Self {
            instrument_id,
            currency,
            basis,
            account_id,
            as_of,
            max_eligible,
        }
    }

    /// Returns the exact measured instrument.
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the required measurement currency.
    pub const fn currency(self) -> Currency {
        self.currency
    }

    /// Returns the required economic basis of the selected measurement amount.
    pub const fn basis(self) -> ValuationAmountBasis {
        self.basis
    }

    /// Returns the optional exact reporting-account scope.
    pub const fn account_id(self) -> Option<AccountId> {
        self.account_id
    }

    /// Returns the inclusive point-in-time cutoff.
    pub const fn as_of(self) -> Timestamp {
        self.as_of
    }

    /// Returns the maximum admitted eligible approval chains.
    pub const fn max_eligible(self) -> usize {
        self.max_eligible.get()
    }
}

/// Resolution state for a bounded point-in-time fair-value selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FairValueSelectionDisposition {
    /// Selection completed without ambiguity; `selected` may be empty when nothing matched.
    Complete,
    /// Matching measurements exist, but none has a time-valid approved evidence chain.
    Unavailable,
    /// Co-leading measurements or active decisions prevent a least-authority choice.
    Conflict,
}

/// One member of the complete deterministic eligible order bound into a selection receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FairValueSelectionOrderEntry {
    pub(super) rank: usize,
    pub(super) measurement_id: MeasurementId,
    pub(super) decision_id: DecisionId,
    pub(super) approval_id: ValuationApprovalId,
    pub(super) measurement_at: Timestamp,
    pub(super) prepared_at: Timestamp,
    pub(super) classification_recorded_at: Timestamp,
    pub(super) approved_at: Timestamp,
    pub(super) approval_recorded_at: Timestamp,
    pub(super) expires_at: Timestamp,
    pub(super) hierarchy: FairValueHierarchy,
    pub(super) ruleset_version: u32,
    pub(super) ruleset_hash: crate::RulesetHash,
    pub(super) evidence_hash: crate::FairValueEvidenceHash,
}

impl FairValueSelectionOrderEntry {
    /// Returns the one-based deterministic rank.
    pub const fn rank(self) -> usize {
        self.rank
    }

    /// Returns the exact measurement identity.
    pub const fn measurement_id(self) -> MeasurementId {
        self.measurement_id
    }

    /// Returns the exact classification identity.
    pub const fn decision_id(self) -> DecisionId {
        self.decision_id
    }

    /// Returns the exact approval identity.
    pub const fn approval_id(self) -> ValuationApprovalId {
        self.approval_id
    }

    /// Returns the measurement instant used as the primary order key.
    pub const fn measurement_at(self) -> Timestamp {
        self.measurement_at
    }

    /// Returns the preparation-completion instant used as the secondary order key.
    pub const fn prepared_at(self) -> Timestamp {
        self.prepared_at
    }

    /// Returns the catalog-trusted classification append time.
    pub const fn classification_recorded_at(self) -> Timestamp {
        self.classification_recorded_at
    }

    /// Returns the approval business time.
    pub const fn approved_at(self) -> Timestamp {
        self.approved_at
    }

    /// Returns the catalog-trusted approval append time.
    pub const fn approval_recorded_at(self) -> Timestamp {
        self.approval_recorded_at
    }

    /// Returns the immutable approval expiry.
    pub const fn expires_at(self) -> Timestamp {
        self.expires_at
    }

    /// Returns the accounting hierarchy without implying data quality or forecast confidence.
    pub const fn hierarchy(self) -> FairValueHierarchy {
        self.hierarchy
    }

    /// Returns the code-owned classification-rules version.
    pub const fn ruleset_version(self) -> u32 {
        self.ruleset_version
    }

    /// Returns the exact classification-rules identity.
    pub const fn ruleset_hash(self) -> crate::RulesetHash {
        self.ruleset_hash
    }

    /// Returns the exact immutable valuation-evidence identity.
    pub const fn evidence_hash(self) -> crate::FairValueEvidenceHash {
        self.evidence_hash
    }
}

/// Complete immutable authority chain selected for one point-in-time fair-value read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectedFairValueEvidence {
    pub(super) measurement: Arc<ValuationMeasurement>,
    pub(super) classification: Arc<ClassificationDecision>,
    pub(super) approval: Arc<ValuationApproval>,
    pub(super) approval_status: ApprovalStatus,
    pub(super) applicable_revocation: Option<Arc<ApprovalRevocation>>,
    pub(super) classification_recorded_at: Timestamp,
    pub(super) approval_recorded_at: Timestamp,
    pub(super) evidence_hash: crate::FairValueEvidenceHash,
}

impl SelectedFairValueEvidence {
    /// Returns the exact retained measurement; the selector never manufactures one.
    pub fn measurement(&self) -> &ValuationMeasurement {
        &self.measurement
    }

    /// Returns the exact retained classification and ruleset binding.
    pub fn classification(&self) -> &ClassificationDecision {
        &self.classification
    }

    /// Returns the exact active approval.
    pub fn approval(&self) -> &ValuationApproval {
        &self.approval
    }

    /// Returns the approval state at the receipt's `as_of` cutoff.
    pub const fn approval_status(&self) -> ApprovalStatus {
        self.approval_status
    }

    /// Returns a revocation applicable at the cutoff.
    ///
    /// A selected chain is active, so this is `None`; later revocations are omitted to prevent
    /// historical look-ahead.
    pub fn applicable_revocation(&self) -> Option<&ApprovalRevocation> {
        self.applicable_revocation.as_deref()
    }

    /// Returns the catalog-trusted classification append time admitted by the cutoff.
    pub const fn classification_recorded_at(&self) -> Timestamp {
        self.classification_recorded_at
    }

    /// Returns the catalog-trusted approval append time admitted by the cutoff.
    pub const fn approval_recorded_at(&self) -> Timestamp {
        self.approval_recorded_at
    }

    /// Returns the immutable approval expiry known when approval was granted.
    pub fn expires_at(&self) -> Timestamp {
        self.approval.expires_at()
    }

    /// Returns the exact immutable valuation-evidence identity.
    pub const fn evidence_hash(&self) -> crate::FairValueEvidenceHash {
        self.evidence_hash
    }
}

/// Bounded auditable result of selecting the latest usable fair-value authority chain.
#[derive(Debug, Eq, PartialEq)]
pub struct FairValueSelectionReceipt {
    pub(super) request: FairValueSelectionRequest,
    pub(super) disposition: FairValueSelectionDisposition,
    pub(super) matching_measurements: usize,
    pub(super) eligible_order: Vec<FairValueSelectionOrderEntry>,
    pub(super) selected: Option<SelectedFairValueEvidence>,
    pub(super) hash: FairValueSelectionReceiptHash,
}

impl FairValueSelectionReceipt {
    /// Returns the exact request bound into this result.
    pub const fn request(&self) -> FairValueSelectionRequest {
        self.request
    }

    /// Returns whether selection completed, was unavailable, or found an authority conflict.
    pub const fn disposition(&self) -> FairValueSelectionDisposition {
        self.disposition
    }

    /// Returns exact instrument/account/currency measurements known by the cutoff.
    pub const fn matching_measurements(&self) -> usize {
        self.matching_measurements
    }

    /// Returns the complete bounded eligible order.
    pub fn eligible_order(&self) -> &[FairValueSelectionOrderEntry] {
        &self.eligible_order
    }

    /// Returns the number of eligible approval chains.
    pub fn eligible_count(&self) -> usize {
        self.eligible_order.len()
    }

    /// Returns the selected chain only for an unambiguous complete result.
    pub fn selected(&self) -> Option<&SelectedFairValueEvidence> {
        self.selected.as_ref()
    }

    /// Returns the versioned canonical identity of the complete receipt.
    pub const fn hash(&self) -> FairValueSelectionReceiptHash {
        self.hash
    }
}
