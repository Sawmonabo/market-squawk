//! Secret-minimizing request admission and result audit envelopes.

use std::{fmt, sync::Arc, time::SystemTime};

use market_squawk_services::{RequestId, ServiceLimits};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Honest local identity evidence available to a plain inherited stdio transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalProcessIdentityClass {
    /// The installed service authenticated an owner-scoped loopback credential before dispatch.
    AuthenticatedInstalledClient,
    /// The peer reached this process over inherited stdio, which does not authenticate the peer.
    InheritedStdioUnverified,
    /// I/O was supplied by the caller; locality, inheritance, and peer identity are unverified.
    CallerSuppliedIoUnverified,
}

/// Audited protocol operation without attacker-controlled unknown method text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuditOperation {
    /// MCP initialization.
    Initialize,
    /// MCP ping.
    Ping,
    /// Registered tool discovery.
    ListTools,
    /// Registered service operation and its contract version.
    CallTool {
        /// Exact registered operation name.
        name: Arc<str>,
        /// Exact registered contract version.
        version: Arc<str>,
    },
    /// Known notification or unsupported request.
    Other,
}

/// Audit lifecycle phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuditPhase {
    /// Request passed outer admission and entered protocol handling.
    Admitted,
    /// A mutating request atomically persisted admission and reserved both terminal records.
    MutationAdmitted,
    /// A mutating service reached its authoritative outcome, independently of delivery.
    MutationServiceCompleted,
    /// Response delivery reached its terminal class.
    Completed,
}

/// Stable terminal result class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuditResultClass {
    /// Request produced a bounded inline result.
    Succeeded,
    /// Request produced an opaque artifact reference.
    ArtifactPublished,
    /// Protocol or lifecycle rejected the request.
    ProtocolRejected,
    /// Application service rejected the call.
    ServiceRejected,
    /// Request was cancelled.
    Cancelled,
    /// Request exceeded its deadline.
    DeadlineExceeded,
    /// A configured resource ceiling rejected the request or result.
    ResourceExhausted,
    /// Peer output closed or timed out.
    OutputUnavailable,
}

/// One immutable, payload-free audit event.
#[derive(Clone)]
pub struct AuditEvent {
    phase: AuditPhase,
    request_id_sha256: Arc<str>,
    identity_class: LocalProcessIdentityClass,
    operation: AuditOperation,
    limits: ServiceLimits,
    occurred_at: SystemTime,
    content_sha256: Arc<str>,
    result_class: Option<AuditResultClass>,
}

impl AuditEvent {
    pub(crate) fn admitted(
        request_id: &RequestId,
        identity_class: LocalProcessIdentityClass,
        operation: AuditOperation,
        limits: ServiceLimits,
        request_bytes: &[u8],
    ) -> Result<Self, AuditError> {
        Ok(Self {
            phase: AuditPhase::Admitted,
            request_id_sha256: hash_request_id(request_id)?,
            identity_class,
            operation,
            limits,
            occurred_at: SystemTime::now(),
            content_sha256: hash_bytes(request_bytes),
            result_class: None,
        })
    }
}

/// Payload-free completion envelope accepted before response publication.
///
/// The sink reserves durable or bounded capacity for this envelope synchronously, while the
/// terminal result class is committed only after the transport knows whether publication
/// succeeded. This preserves fail-closed ordering without misclassifying output failures.
#[derive(Clone)]
pub struct AuditCompletion {
    event: AuditEvent,
}

impl AuditCompletion {
    pub(crate) fn new(
        request_id: &RequestId,
        identity_class: LocalProcessIdentityClass,
        operation: AuditOperation,
        limits: ServiceLimits,
        response_bytes: &[u8],
    ) -> Result<Self, AuditError> {
        Self::with_phase(
            request_id,
            identity_class,
            operation,
            limits,
            response_bytes,
            AuditPhase::Completed,
        )
    }

    fn with_phase(
        request_id: &RequestId,
        identity_class: LocalProcessIdentityClass,
        operation: AuditOperation,
        limits: ServiceLimits,
        response_bytes: &[u8],
        phase: AuditPhase,
    ) -> Result<Self, AuditError> {
        Ok(Self {
            event: AuditEvent {
                phase,
                request_id_sha256: hash_request_id(request_id)?,
                identity_class,
                operation,
                limits,
                occurred_at: SystemTime::now(),
                content_sha256: hash_bytes(response_bytes),
                result_class: None,
            },
        })
    }

    fn into_event(mut self, result_class: AuditResultClass) -> AuditEvent {
        self.event.result_class = Some(result_class);
        self.event
    }
}

/// Atomically admitted mutation audit material.
///
/// An [`AuditSink`] must reserve capacity for this admission, its authoritative service outcome,
/// and its independent delivery outcome before it persists admission and returns a
/// [`MutationAuditReservation`].
pub struct MutationAuditBundle {
    admission: AuditEvent,
    service_completion: Box<AuditCompletion>,
    delivery_fallback: Box<AuditCompletion>,
}

impl MutationAuditBundle {
    pub(crate) fn new(
        request_id: &RequestId,
        identity_class: LocalProcessIdentityClass,
        operation: AuditOperation,
        limits: ServiceLimits,
        request_bytes: &[u8],
    ) -> Result<Self, AuditError> {
        let mut admission = AuditEvent::admitted(
            request_id,
            identity_class,
            operation.clone(),
            limits,
            request_bytes,
        )?;
        admission.phase = AuditPhase::MutationAdmitted;
        let service_completion = Box::new(AuditCompletion::with_phase(
            request_id,
            identity_class,
            operation.clone(),
            limits,
            b"authoritative mutation service outcome",
            AuditPhase::MutationServiceCompleted,
        )?);
        let delivery_fallback = Box::new(AuditCompletion::new(
            request_id,
            identity_class,
            operation,
            limits,
            b"mutation delivery unavailable",
        )?);
        Ok(Self {
            admission,
            service_completion,
            delivery_fallback,
        })
    }
}

impl fmt::Debug for MutationAuditBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MutationAuditBundle")
            .field("admission", &self.admission)
            .field("service_completion", &self.service_completion)
            .field("delivery_fallback", &self.delivery_fallback)
            .finish()
    }
}

/// Capacity-backed mutation outcome commits after durable admission.
pub struct MutationAuditReservation {
    service_completion: Option<Box<AuditCompletion>>,
    delivery_fallback: Option<Box<AuditCompletion>>,
    service_commit: Option<Box<AuditCommit>>,
    delivery_commit: Option<Box<AuditCommit>>,
}

type AuditCommit = dyn FnOnce(AuditEvent) -> Result<(), AuditError> + Send + 'static;

impl MutationAuditReservation {
    /// Persists mutation admission and binds the two pre-reserved terminal slots.
    ///
    /// Callers must atomically reserve capacity for all three records before invoking this
    /// constructor. `persist_admission` must durably persist the admission record before it
    /// returns success. Terminal callbacks must synchronously report whether the event is durable;
    /// on failure they retain ownership for a later sink drain because the reservation is one-use.
    ///
    /// # Errors
    ///
    /// Returns the admission persistence failure without creating mutation authority.
    pub fn try_new<P, S, D>(
        bundle: MutationAuditBundle,
        persist_admission: P,
        service_commit: S,
        delivery_commit: D,
    ) -> Result<Self, AuditError>
    where
        P: FnOnce(AuditEvent) -> Result<(), AuditError>,
        S: FnOnce(AuditEvent) -> Result<(), AuditError> + Send + 'static,
        D: FnOnce(AuditEvent) -> Result<(), AuditError> + Send + 'static,
    {
        let MutationAuditBundle {
            admission,
            service_completion,
            delivery_fallback,
        } = bundle;
        let service_commit: Box<AuditCommit> = Box::new(service_commit);
        let delivery_commit: Box<AuditCommit> = Box::new(delivery_commit);
        persist_admission(admission)?;
        Ok(Self {
            service_completion: Some(service_completion),
            delivery_fallback: Some(delivery_fallback),
            service_commit: Some(service_commit),
            delivery_commit: Some(delivery_commit),
        })
    }

    pub(crate) fn commit_service(
        &mut self,
        result_class: AuditResultClass,
    ) -> Result<(), AuditError> {
        if let (Some(completion), Some(commit)) =
            (self.service_completion.take(), self.service_commit.take())
        {
            commit((*completion).into_event(result_class))?;
        }
        Ok(())
    }

    pub(crate) const fn service_pending(&self) -> bool {
        self.service_completion.is_some() && self.service_commit.is_some()
    }

    pub(crate) const fn delivery_pending(&self) -> bool {
        self.delivery_fallback.is_some() && self.delivery_commit.is_some()
    }

    pub(crate) const fn is_terminalized(&self) -> bool {
        !self.service_pending() && !self.delivery_pending()
    }

    pub(crate) fn reserve_delivery(
        &mut self,
        completion: AuditCompletion,
    ) -> Result<AuditCompletionReservation, AuditError> {
        let Some(delivery_fallback) = self.delivery_fallback.take() else {
            return Err(AuditError::Unavailable);
        };
        let Some(commit) = self.delivery_commit.take() else {
            self.delivery_fallback = Some(delivery_fallback);
            return Err(AuditError::Unavailable);
        };
        drop(delivery_fallback);
        Ok(AuditCompletionReservation::new(completion, commit))
    }
}

impl fmt::Debug for MutationAuditReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MutationAuditReservation")
            .field("service_pending", &self.service_completion.is_some())
            .field("delivery_pending", &self.delivery_fallback.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for MutationAuditReservation {
    fn drop(&mut self) {
        let _ = self.commit_service(AuditResultClass::OutputUnavailable);
        if let (Some(completion), Some(commit)) =
            (self.delivery_fallback.take(), self.delivery_commit.take())
        {
            let _ = commit((*completion).into_event(AuditResultClass::OutputUnavailable));
        }
    }
}

impl fmt::Debug for AuditCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuditCompletion")
            .field("event", &self.event)
            .finish()
    }
}

/// Result-bearing one-shot terminal commit backed by capacity accepted from an [`AuditSink`].
pub struct AuditCompletionReservation {
    completion: Option<AuditCompletion>,
    commit: Option<Box<AuditCommit>>,
}

impl AuditCompletionReservation {
    /// Creates a reservation after the sink has durably or boundedly accepted its capacity.
    ///
    /// The callback must synchronously report whether the event is durable and retain it for later
    /// drain on failure. Dropping an uncommitted reservation attempts to record
    /// [`AuditResultClass::OutputUnavailable`].
    #[must_use]
    pub fn new<F>(completion: AuditCompletion, commit: F) -> Self
    where
        F: FnOnce(AuditEvent) -> Result<(), AuditError> + Send + 'static,
    {
        Self {
            completion: Some(completion),
            commit: Some(Box::new(commit)),
        }
    }

    pub(crate) fn commit(mut self, result_class: AuditResultClass) -> Result<(), AuditError> {
        self.finish(result_class)
    }

    fn finish(&mut self, result_class: AuditResultClass) -> Result<(), AuditError> {
        if let (Some(completion), Some(commit)) = (self.completion.take(), self.commit.take()) {
            commit(completion.into_event(result_class))?;
        }
        Ok(())
    }
}

impl fmt::Debug for AuditCompletionReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuditCompletionReservation")
            .field("completion", &self.completion)
            .field("commit", &"[AUDIT COMMIT]")
            .finish()
    }
}

impl Drop for AuditCompletionReservation {
    fn drop(&mut self) {
        let _ = self.finish(AuditResultClass::OutputUnavailable);
    }
}

impl AuditEvent {
    /// Event lifecycle phase.
    #[must_use]
    pub const fn phase(&self) -> AuditPhase {
        self.phase
    }

    /// Stable terminal class, present only for completed events.
    #[must_use]
    pub const fn result_class(&self) -> Option<AuditResultClass> {
        self.result_class
    }

    /// Honest process-identity evidence class.
    #[must_use]
    pub const fn identity_class(&self) -> LocalProcessIdentityClass {
        self.identity_class
    }

    /// Registered, bounded operation identity.
    #[must_use]
    pub const fn operation(&self) -> &AuditOperation {
        &self.operation
    }

    /// Correlation hash; the raw request identifier is never stored.
    #[must_use]
    pub fn request_id_sha256(&self) -> &str {
        &self.request_id_sha256
    }

    /// Content hash; full request and financial payloads are never stored in this envelope.
    #[must_use]
    pub fn content_sha256(&self) -> &str {
        &self.content_sha256
    }

    /// Limits admitted with the request.
    #[must_use]
    pub const fn limits(&self) -> ServiceLimits {
        self.limits
    }

    /// Wall-clock event time for local audit ordering.
    #[must_use]
    pub const fn occurred_at(&self) -> SystemTime {
        self.occurred_at
    }
}

impl fmt::Debug for AuditEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuditEvent")
            .field("phase", &self.phase)
            .field("request_id_sha256", &self.request_id_sha256)
            .field("identity_class", &self.identity_class)
            .field("operation", &self.operation)
            .field("limits", &self.limits)
            .field("occurred_at", &self.occurred_at)
            .field("content_sha256", &self.content_sha256)
            .field("result_class", &self.result_class)
            .finish()
    }
}

fn hash_request_id(request_id: &RequestId) -> Result<Arc<str>, AuditError> {
    let encoded = request_id
        .canonical_bytes()
        .map_err(|_| AuditError::Encoding)?;
    Ok(hash_bytes(&encoded))
}

fn hash_bytes(bytes: &[u8]) -> Arc<str> {
    Arc::from(format!("{:x}", Sha256::digest(bytes)))
}

/// Nonblocking local audit sink.
///
/// Implementations must take ownership of the event or fail synchronously; protocol dispatch fails
/// closed when an admitted or completed event cannot be recorded.
pub trait AuditSink: Send + Sync + 'static {
    /// Records one payload-free audit event.
    ///
    /// # Errors
    ///
    /// Returns [`AuditError`] when the event cannot be durably or boundedly accepted.
    fn record(&self, event: AuditEvent) -> Result<(), AuditError>;

    /// Reserves one completion envelope before any corresponding response bytes are written.
    ///
    /// # Errors
    ///
    /// Returns [`AuditError`] unless the returned reservation owns capacity for exactly one
    /// terminal event without further capacity acquisition. Its terminal commit remains
    /// result-bearing because durability can fail after reservation.
    fn reserve_completion(
        &self,
        completion: AuditCompletion,
    ) -> Result<AuditCompletionReservation, AuditError>;

    /// Atomically reserves all mutation records and durably persists admission before dispatch.
    ///
    /// # Errors
    ///
    /// Returns [`AuditError`] unless admission is durable and both terminal records own capacity
    /// without additional acquisition. Terminal commits remain result-bearing because durability
    /// can fail after reservation.
    fn reserve_mutation(
        &self,
        bundle: MutationAuditBundle,
    ) -> Result<MutationAuditReservation, AuditError>;
}

/// Audit admission or encoding failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AuditError {
    /// Sink could not accept the event.
    #[error("local audit sink is unavailable")]
    Unavailable,
    /// Correlation identity could not be encoded.
    #[error("audit correlation encoding failed")]
    Encoding,
}
