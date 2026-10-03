//! Exact original-request authority retained before consuming one-use analytical inputs.

use market_squawk_domain::{EvidenceDigest, SourceIdentifier};
use market_squawk_services::RequestId;
use serde::Serialize;

use super::{JobId, JobOrigin, JobSnapshot};

/// Authenticated original request and the exact admitted operation/argument commitment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobStartBinding {
    origin: JobOrigin,
    request_id: RequestId,
    operation: SourceIdentifier,
    arguments_digest: EvidenceDigest,
}

impl JobStartBinding {
    /// Binds an authenticated origin to one immutable start request.
    #[must_use]
    pub const fn new(
        origin: JobOrigin,
        request_id: RequestId,
        operation: SourceIdentifier,
        arguments_digest: EvidenceDigest,
    ) -> Self {
        Self {
            origin,
            request_id,
            operation,
            arguments_digest,
        }
    }

    /// Original authenticated workspace and client.
    #[must_use]
    pub const fn origin(&self) -> &JobOrigin {
        &self.origin
    }

    /// Exact original transport request identity.
    #[must_use]
    pub const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    /// Exact admitted start operation.
    #[must_use]
    pub const fn operation(&self) -> &SourceIdentifier {
        &self.operation
    }

    /// SHA-256 of the complete admitted argument object.
    #[must_use]
    pub const fn arguments_digest(&self) -> EvidenceDigest {
        self.arguments_digest
    }
}

/// One-owner permission to prepare immutable input and admit the reserved job identity.
#[derive(Debug)]
pub struct JobStartPermit {
    pub(crate) id: JobId,
    pub(crate) binding: JobStartBinding,
}

impl JobStartPermit {
    /// Stable identity reserved before one-use preparation is consumed.
    #[must_use]
    pub const fn id(&self) -> JobId {
        self.id
    }

    /// Exact request that owns this admission.
    #[must_use]
    pub const fn binding(&self) -> &JobStartBinding {
        &self.binding
    }
}

/// Durable state of one exact original start request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStartState {
    /// No durable reservation exists; a delayed original handler may still arrive.
    Unknown,
    /// Preparation owns the request; no job has yet been durably admitted.
    Pending,
    /// A durable fence prohibits this original request from admitting any job.
    NotAdmitted,
    /// The original request admitted the returned exact job identity.
    Admitted,
}

/// Reconciled request state and its latest exact execution generation when admitted.
#[derive(Clone, Debug)]
pub struct JobStartReconciliation {
    pub(crate) state: JobStartState,
    pub(crate) snapshot: Option<JobSnapshot>,
}

impl JobStartReconciliation {
    /// Durable request admission disposition.
    #[must_use]
    pub const fn state(&self) -> JobStartState {
        self.state
    }

    /// Original job's latest generation, including cancellation and restart recovery.
    #[must_use]
    pub const fn snapshot(&self) -> Option<&JobSnapshot> {
        self.snapshot.as_ref()
    }
}

/// New preparation ownership or an already retained original-request disposition.
#[derive(Debug)]
pub enum JobStartAdmission {
    /// Exactly one caller may consume preparation and start the reserved identity.
    Execute(JobStartPermit),
    /// The original request already has this durable disposition.
    Existing(JobStartReconciliation),
}
