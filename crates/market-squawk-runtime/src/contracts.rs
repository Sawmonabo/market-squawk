use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use market_squawk_domain::{EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ServiceLimits, validate_json_contract,
};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const MAXIMUM_REQUEST_LIFETIME_NANOS: i64 = 300_000_000_000;
const MAXIMUM_EVENT_PAGE_ITEMS: usize = 4_096;

/// Invalid installed-runtime contract input.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RuntimeContractError {
    /// UUID identities must not use the nil value.
    #[error("runtime identity must not be nil")]
    NilIdentity,
    /// Service and workspace generations are one-based.
    #[error("runtime generation must be nonzero")]
    ZeroGeneration,
    /// The request deadline has elapsed.
    #[error("request deadline has elapsed")]
    ExpiredDeadline,
    /// The request deadline exceeds the hard five-minute admission window.
    #[error("request deadline exceeds the admitted lifetime")]
    DeadlineTooDistant,
    /// Request JSON violates the admitted structural or encoded-size limits.
    #[error("request payload exceeds its admitted limits")]
    InvalidPayload,
    /// A requested event page size was zero or exceeded its hard ceiling.
    #[error("event page limit is invalid")]
    InvalidPageLimit,
    /// Input length must be nonzero.
    #[error("input byte length must be nonzero")]
    EmptyInput,
}

macro_rules! uuid_identity {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates a non-nil identity.
            pub fn try_from_uuid(value: Uuid) -> Result<Self, RuntimeContractError> {
                if value.is_nil() {
                    Err(RuntimeContractError::NilIdentity)
                } else {
                    Ok(Self(value))
                }
            }

            /// Returns the UUID value.
            #[must_use]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }
    };
}

uuid_identity!(/// Stable identity for one Market Squawk installation.
    InstallationId);
uuid_identity!(/// Stable identity for one local workspace.
    WorkspaceId);
uuid_identity!(/// Stable identity for one registered native client.
    ClientId);
uuid_identity!(/// Opaque identity for one staged native input.
    InputTicketId);

/// One-based generation for a running installed service.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ServiceGeneration(NonZeroU64);

impl ServiceGeneration {
    /// Creates a one-based service generation.
    pub fn try_new(value: u64) -> Result<Self, RuntimeContractError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(RuntimeContractError::ZeroGeneration)
    }

    /// Returns the one-based generation.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Exact application protocol revision negotiated by native clients.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationProtocolVersion {
    major: u16,
    minor: u16,
}

impl ApplicationProtocolVersion {
    /// First installed-product application protocol revision.
    pub const V1: Self = Self { major: 1, minor: 0 };

    /// Returns the major protocol revision.
    #[must_use]
    pub const fn major(self) -> u16 {
        self.major
    }

    /// Returns the minor protocol revision.
    #[must_use]
    pub const fn minor(self) -> u16 {
        self.minor
    }
}

/// Closed transport-neutral request admitted by the installed service.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppRequestEnvelope {
    request_id: RequestId,
    installation_id: InstallationId,
    workspace_id: WorkspaceId,
    service_generation: ServiceGeneration,
    client_id: ClientId,
    protocol: ApplicationProtocolVersion,
    deadline: Timestamp,
    operation: SourceIdentifier,
    arguments: Value,
}

impl AppRequestEnvelope {
    /// Admits a bounded request whose wall-clock deadline is still usable.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        request_id: RequestId,
        installation_id: InstallationId,
        workspace_id: WorkspaceId,
        service_generation: ServiceGeneration,
        client_id: ClientId,
        deadline: Timestamp,
        now: Timestamp,
        operation: SourceIdentifier,
        arguments: Value,
        structure_limits: JsonStructureLimits,
        maximum_encoded_bytes: usize,
    ) -> Result<Self, RuntimeContractError> {
        let remaining = deadline
            .unix_nanos()
            .checked_sub(now.unix_nanos())
            .ok_or(RuntimeContractError::DeadlineTooDistant)?;
        validate_deadline_delta(remaining)?;
        validate_json_contract(&arguments, structure_limits, maximum_encoded_bytes)
            .map_err(|_error| RuntimeContractError::InvalidPayload)?;
        Ok(Self {
            request_id,
            installation_id,
            workspace_id,
            service_generation,
            client_id,
            protocol: ApplicationProtocolVersion::V1,
            deadline,
            operation,
            arguments,
        })
    }

    /// Converts the admitted wall-clock deadline once into a monotonic service context.
    pub fn to_request_context(
        &self,
        now: Timestamp,
        monotonic_now: Instant,
        cancellation: CancellationToken,
        limits: ServiceLimits,
    ) -> Result<RequestContext, RuntimeContractError> {
        let remaining = self
            .deadline
            .unix_nanos()
            .checked_sub(now.unix_nanos())
            .ok_or(RuntimeContractError::DeadlineTooDistant)?;
        validate_deadline_delta(remaining)?;
        let nanos =
            u64::try_from(remaining).map_err(|_error| RuntimeContractError::ExpiredDeadline)?;
        let deadline = monotonic_now
            .checked_add(Duration::from_nanos(nanos))
            .ok_or(RuntimeContractError::DeadlineTooDistant)?;
        Ok(RequestContext::new(
            self.request_id.clone(),
            cancellation,
            deadline,
            limits,
        ))
    }

    /// Exact installation expected by this request.
    #[must_use]
    pub const fn installation_id(&self) -> InstallationId {
        self.installation_id
    }

    /// Exact workspace expected by this request.
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    /// Exact service generation expected by this request.
    #[must_use]
    pub const fn service_generation(&self) -> ServiceGeneration {
        self.service_generation
    }

    /// Registered client that owns this request.
    #[must_use]
    pub const fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// Selected application protocol revision.
    #[must_use]
    pub const fn protocol(&self) -> ApplicationProtocolVersion {
        self.protocol
    }

    /// Registered application operation.
    #[must_use]
    pub const fn operation(&self) -> &SourceIdentifier {
        &self.operation
    }

    /// Bounded operation arguments.
    #[must_use]
    pub const fn arguments(&self) -> &Value {
        &self.arguments
    }
}

fn validate_deadline_delta(remaining: i64) -> Result<(), RuntimeContractError> {
    if remaining <= 0 {
        Err(RuntimeContractError::ExpiredDeadline)
    } else if remaining > MAXIMUM_REQUEST_LIFETIME_NANOS {
        Err(RuntimeContractError::DeadlineTooDistant)
    } else {
        Ok(())
    }
}

/// Immutable metadata for one native-streamed input staged by the service.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InputTicket {
    id: InputTicketId,
    installation_id: InstallationId,
    workspace_id: WorkspaceId,
    generation: ServiceGeneration,
    media_type: SourceIdentifier,
    byte_length: u64,
    digest: EvidenceDigest,
    expires_at: Timestamp,
}

impl InputTicket {
    /// Creates an opaque expiring reference after native streaming and digest verification.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        id: InputTicketId,
        installation_id: InstallationId,
        workspace_id: WorkspaceId,
        generation: ServiceGeneration,
        media_type: SourceIdentifier,
        byte_length: u64,
        digest: EvidenceDigest,
        expires_at: Timestamp,
        now: Timestamp,
    ) -> Result<Self, RuntimeContractError> {
        if byte_length == 0 {
            return Err(RuntimeContractError::EmptyInput);
        }
        if expires_at <= now {
            return Err(RuntimeContractError::ExpiredDeadline);
        }
        Ok(Self {
            id,
            installation_id,
            workspace_id,
            generation,
            media_type,
            byte_length,
            digest,
            expires_at,
        })
    }

    /// Returns the opaque input identity.
    #[must_use]
    pub const fn id(&self) -> InputTicketId {
        self.id
    }

    /// Exact installation that owns the staged bytes.
    #[must_use]
    pub const fn installation_id(&self) -> InstallationId {
        self.installation_id
    }

    /// Exact workspace that owns the staged bytes.
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    /// Service generation that created this ticket.
    #[must_use]
    pub const fn generation(&self) -> ServiceGeneration {
        self.generation
    }

    /// Verified staged byte length.
    #[must_use]
    pub const fn byte_length(&self) -> u64 {
        self.byte_length
    }

    /// Verified digest of the staged bytes.
    #[must_use]
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

/// Reconnect cursor for one bounded service-generation event stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventCursor {
    generation: ServiceGeneration,
    sequence: u64,
    expires_at: Timestamp,
}

impl EventCursor {
    /// Creates an expiring cursor. Sequence zero means no event observed yet.
    pub fn try_new(
        generation: ServiceGeneration,
        sequence: u64,
        expires_at: Timestamp,
    ) -> Result<Self, RuntimeContractError> {
        if expires_at.unix_nanos() <= 0 {
            return Err(RuntimeContractError::ExpiredDeadline);
        }
        Ok(Self {
            generation,
            sequence,
            expires_at,
        })
    }

    /// Ensures this cursor still belongs to the current generation and has not expired.
    pub fn ensure_current(
        &self,
        generation: ServiceGeneration,
        now: Timestamp,
    ) -> Result<(), EventCursorError> {
        if self.generation != generation {
            Err(EventCursorError::GenerationChanged)
        } else if self.expires_at <= now {
            Err(EventCursorError::Expired)
        } else {
            Ok(())
        }
    }

    /// Last observed sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
}

/// Event cursor cannot continue without a fresh snapshot.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum EventCursorError {
    /// The service or workspace switched generations.
    #[error("event cursor generation changed; snapshot resynchronization is required")]
    GenerationChanged,
    /// The bounded cursor retention window elapsed.
    #[error("event cursor expired; snapshot resynchronization is required")]
    Expired,
}

/// Maximum records returned by one application-event page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventPageLimit(usize);

impl EventPageLimit {
    /// Creates a positive limit no larger than 4,096 events.
    pub fn try_new(value: usize) -> Result<Self, RuntimeContractError> {
        if value == 0 || value > MAXIMUM_EVENT_PAGE_ITEMS {
            Err(RuntimeContractError::InvalidPageLimit)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the admitted item count.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

/// Closed client error without transport secrets or raw provider payloads.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ApplicationClientError {
    /// The request was rejected before dispatch.
    #[error("application request was rejected")]
    Rejected,
    /// The service is not reachable at the authenticated rendezvous.
    #[error("application service is unavailable")]
    Unavailable,
    /// The request was cancelled or exceeded its deadline.
    #[error("application request did not complete")]
    Interrupted,
    /// The service response violated the closed application contract.
    #[error("application response is invalid")]
    InvalidResponse,
}

/// Transport-neutral response from the installed application authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppResponseEnvelope {
    request_id: RequestId,
    service_generation: ServiceGeneration,
    result: Value,
}

impl AppResponseEnvelope {
    /// Creates a response correlated to the original request and exact service generation.
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        service_generation: ServiceGeneration,
        result: Value,
    ) -> Self {
        Self {
            request_id,
            service_generation,
            result,
        }
    }

    /// Correlation identity copied from the request.
    #[must_use]
    pub const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    /// Exact service generation that produced the result.
    #[must_use]
    pub const fn service_generation(&self) -> ServiceGeneration {
        self.service_generation
    }

    /// Closed application result payload.
    #[must_use]
    pub const fn result(&self) -> &Value {
        &self.result
    }
}

/// Request metadata for native-streamed input staging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputAdmission {
    media_type: SourceIdentifier,
    expected_bytes: u64,
    expected_digest: EvidenceDigest,
}

impl InputAdmission {
    /// Admits a nonempty byte stream with exact content evidence.
    pub fn try_new(
        media_type: SourceIdentifier,
        expected_bytes: u64,
        expected_digest: EvidenceDigest,
    ) -> Result<Self, RuntimeContractError> {
        if expected_bytes == 0 {
            Err(RuntimeContractError::EmptyInput)
        } else {
            Ok(Self {
                media_type,
                expected_bytes,
                expected_digest,
            })
        }
    }

    /// Declared media type of the staged bytes.
    #[must_use]
    pub const fn media_type(&self) -> &SourceIdentifier {
        &self.media_type
    }

    /// Exact byte length required from the stream.
    #[must_use]
    pub const fn expected_bytes(&self) -> u64 {
        self.expected_bytes
    }

    /// Exact digest required from the stream.
    #[must_use]
    pub const fn expected_digest(&self) -> EvidenceDigest {
        self.expected_digest
    }
}

/// Native client for the single installed service authority.
#[async_trait]
pub trait ApplicationClient: std::fmt::Debug + Send + Sync {
    /// Invokes one admitted application operation.
    async fn invoke(
        &self,
        request: AppRequestEnvelope,
        cancellation: CancellationToken,
    ) -> Result<AppResponseEnvelope, ApplicationClientError>;

    /// Streams controlled input through the native client boundary and returns an opaque ticket.
    async fn stage_input(
        &self,
        admission: InputAdmission,
        input: &mut (dyn AsyncRead + Send + Unpin),
        cancellation: CancellationToken,
    ) -> Result<InputTicket, ApplicationClientError>;

    /// Reads one bounded event page; cursor errors require a fresh snapshot.
    async fn read_events(
        &self,
        cursor: Option<EventCursor>,
        limit: EventPageLimit,
        cancellation: CancellationToken,
    ) -> Result<(Arc<[Value]>, EventCursor), ApplicationClientError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_identities_reject_nil_values() {
        let nil = Uuid::nil();
        assert_eq!(
            InstallationId::try_from_uuid(nil),
            Err(RuntimeContractError::NilIdentity)
        );
        assert_eq!(
            WorkspaceId::try_from_uuid(nil),
            Err(RuntimeContractError::NilIdentity)
        );
        assert_eq!(
            ClientId::try_from_uuid(nil),
            Err(RuntimeContractError::NilIdentity)
        );
    }

    #[test]
    fn event_cursor_detects_expiry_and_generation_change() -> Result<(), Box<dyn std::error::Error>>
    {
        let generation = ServiceGeneration::try_new(1)?;
        let next_generation = ServiceGeneration::try_new(2)?;
        let cursor = EventCursor::try_new(generation, 7, Timestamp::from_unix_nanos(20))?;

        assert_eq!(
            cursor.ensure_current(next_generation, Timestamp::from_unix_nanos(10)),
            Err(EventCursorError::GenerationChanged),
        );
        assert_eq!(
            cursor.ensure_current(generation, Timestamp::from_unix_nanos(20)),
            Err(EventCursorError::Expired),
        );
        Ok(())
    }

    #[test]
    fn request_deadline_is_admitted_once_and_preserves_request_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let structure = JsonStructureLimits::try_new(8, 1_024, 64, 64)?;
        let service_limits = ServiceLimits::try_new(1_024, 64, 4_096, 256, structure)?;
        let envelope = AppRequestEnvelope::try_new(
            RequestId::Integer(7),
            InstallationId::try_from_uuid(Uuid::from_u128(1))?,
            WorkspaceId::try_from_uuid(Uuid::from_u128(2))?,
            ServiceGeneration::try_new(1)?,
            ClientId::try_from_uuid(Uuid::from_u128(3))?,
            Timestamp::from_unix_nanos(200),
            Timestamp::from_unix_nanos(100),
            SourceIdentifier::try_from("Market.Snapshot")?,
            serde_json::json!({}),
            structure,
            1_024,
        )?;

        let context = envelope.to_request_context(
            Timestamp::from_unix_nanos(100),
            Instant::now(),
            CancellationToken::new(),
            service_limits,
        )?;
        assert_eq!(context.request_id(), &RequestId::Integer(7));
        Ok(())
    }
}
