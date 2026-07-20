//! Secret-minimizing request admission and result audit envelopes.

use std::{fmt, sync::Arc, time::SystemTime};

use market_squawk_services::{RequestId, ServiceLimits};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Honest local identity evidence available to a plain inherited stdio transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalProcessIdentityClass {
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
    /// Protocol or service handling reached a terminal class.
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

    pub(crate) fn completed(
        request_id: &RequestId,
        identity_class: LocalProcessIdentityClass,
        operation: AuditOperation,
        limits: ServiceLimits,
        response_bytes: &[u8],
        result_class: AuditResultClass,
    ) -> Result<Self, AuditError> {
        Ok(Self {
            phase: AuditPhase::Completed,
            request_id_sha256: hash_request_id(request_id)?,
            identity_class,
            operation,
            limits,
            occurred_at: SystemTime::now(),
            content_sha256: hash_bytes(response_bytes),
            result_class: Some(result_class),
        })
    }

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
