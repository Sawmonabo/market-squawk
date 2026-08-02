use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ServiceLimits, validate_json_contract,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{CredentialGeneration, EventCursor, EventPageLimit, InputAdmission, InputTicket};

const MAXIMUM_REQUEST_LIFETIME_NANOS: i64 = 300_000_000_000;

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
    /// Runtime identity did not match the active service authority.
    #[error("runtime identity is not current")]
    IdentityMismatch,
    /// Protocol range is empty or reversed.
    #[error("application protocol range is invalid")]
    InvalidProtocolRange,
    /// A response violated the configured output contract.
    #[error("response payload exceeds its admitted limits")]
    InvalidResponse,
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

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = Uuid::deserialize(deserializer)?;
                Self::try_from_uuid(value).map_err(serde::de::Error::custom)
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
uuid_identity!(/// Stable correlation identity shared across related application requests.
    CorrelationId);

/// One-based generation for a running installed service.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationProtocolVersion {
    major: u16,
    minor: u16,
}

/// Closed range of application protocol revisions admitted by one service generation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ApplicationProtocolRange {
    minimum: ApplicationProtocolVersion,
    maximum: ApplicationProtocolVersion,
}

impl ApplicationProtocolRange {
    /// Creates an inclusive, non-reversed protocol range.
    pub fn try_new(
        minimum: ApplicationProtocolVersion,
        maximum: ApplicationProtocolVersion,
    ) -> Result<Self, RuntimeContractError> {
        if minimum > maximum {
            Err(RuntimeContractError::InvalidProtocolRange)
        } else {
            Ok(Self { minimum, maximum })
        }
    }

    /// Creates a range containing exactly one protocol revision.
    #[must_use]
    pub const fn single(version: ApplicationProtocolVersion) -> Self {
        Self {
            minimum: version,
            maximum: version,
        }
    }

    /// Returns whether the supplied revision is admitted.
    #[must_use]
    pub const fn contains(self, version: ApplicationProtocolVersion) -> bool {
        self.minimum.major <= version.major
            && (self.minimum.major != version.major || self.minimum.minor <= version.minor)
            && version.major <= self.maximum.major
            && (version.major != self.maximum.major || version.minor <= self.maximum.minor)
    }
}

/// Exact installation, workspace, and running-service authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RuntimeIdentity {
    installation_id: InstallationId,
    workspace_id: WorkspaceId,
    service_generation: ServiceGeneration,
}

impl RuntimeIdentity {
    /// Creates an exact runtime identity from validated components.
    pub const fn try_new(
        installation_id: InstallationId,
        workspace_id: WorkspaceId,
        service_generation: ServiceGeneration,
    ) -> Result<Self, RuntimeContractError> {
        Ok(Self {
            installation_id,
            workspace_id,
            service_generation,
        })
    }

    /// Admits only requests for this exact runtime authority.
    pub fn admit(&self, request: &AppRequestEnvelope) -> Result<(), RuntimeAdmissionError> {
        if self.installation_id != request.installation_id {
            Err(RuntimeAdmissionError::InstallationMismatch)
        } else if self.workspace_id != request.workspace_id {
            Err(RuntimeAdmissionError::WorkspaceMismatch)
        } else if self.service_generation != request.service_generation {
            Err(RuntimeAdmissionError::GenerationMismatch)
        } else {
            Ok(())
        }
    }

    /// Installation identity.
    #[must_use]
    pub const fn installation_id(self) -> InstallationId {
        self.installation_id
    }

    /// Workspace identity.
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    /// Running service generation.
    #[must_use]
    pub const fn service_generation(self) -> ServiceGeneration {
        self.service_generation
    }
}

/// Request identity does not name the active runtime authority.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RuntimeAdmissionError {
    /// Installation identity differs.
    #[error("application request names another installation")]
    InstallationMismatch,
    /// Workspace identity differs.
    #[error("application request names another workspace")]
    WorkspaceMismatch,
    /// Running service generation differs.
    #[error("application request names a stale service generation")]
    GenerationMismatch,
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
    credential_generation: CredentialGeneration,
    correlation_id: CorrelationId,
    protocol: ApplicationProtocolVersion,
    deadline: Timestamp,
    operation: SourceIdentifier,
    arguments: Value,
}

/// Fixed identity and payload limits used by a native client to construct requests.
#[derive(Clone, Debug)]
pub struct ApplicationRequestScope {
    runtime: RuntimeIdentity,
    client_id: ClientId,
    credential_generation: CredentialGeneration,
    correlation_id: CorrelationId,
    structure_limits: JsonStructureLimits,
    maximum_encoded_bytes: usize,
}

impl ApplicationRequestScope {
    /// Creates a scope for one authenticated named-client generation.
    pub fn try_new(
        runtime: RuntimeIdentity,
        client_id: ClientId,
        credential_generation: CredentialGeneration,
        correlation_id: CorrelationId,
        structure_limits: JsonStructureLimits,
        maximum_encoded_bytes: usize,
    ) -> Result<Self, RuntimeContractError> {
        if maximum_encoded_bytes == 0 {
            return Err(RuntimeContractError::InvalidPayload);
        }
        Ok(Self {
            runtime,
            client_id,
            credential_generation,
            correlation_id,
            structure_limits,
            maximum_encoded_bytes,
        })
    }

    /// Constructs one fully bound request without exposing platform secret types.
    pub fn request(
        &self,
        request_id: RequestId,
        deadline: Timestamp,
        now: Timestamp,
        operation: SourceIdentifier,
        arguments: Value,
    ) -> Result<AppRequestEnvelope, RuntimeContractError> {
        AppRequestEnvelope::try_new(
            request_id,
            self.runtime.installation_id(),
            self.runtime.workspace_id(),
            self.runtime.service_generation(),
            self.client_id,
            self.credential_generation,
            self.correlation_id,
            deadline,
            now,
            operation,
            arguments,
            self.structure_limits,
            self.maximum_encoded_bytes,
        )
    }

    /// Exact runtime targeted by this scope.
    #[must_use]
    pub const fn runtime(&self) -> RuntimeIdentity {
        self.runtime
    }

    /// Exact client identity carried by this scope.
    #[must_use]
    pub const fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// Exact credential generation carried by this scope.
    #[must_use]
    pub const fn credential_generation(&self) -> CredentialGeneration {
        self.credential_generation
    }
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
        credential_generation: CredentialGeneration,
        correlation_id: CorrelationId,
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
            credential_generation,
            correlation_id,
            protocol: ApplicationProtocolVersion::V1,
            deadline,
            operation,
            arguments,
        })
    }

    /// Decodes one closed request and reapplies all dynamic admission limits.
    pub fn decode(
        encoded: &[u8],
        now: Timestamp,
        structure_limits: JsonStructureLimits,
        maximum_encoded_bytes: usize,
    ) -> Result<Self, RuntimeContractError> {
        if encoded.len() > maximum_encoded_bytes {
            return Err(RuntimeContractError::InvalidPayload);
        }
        let wire: AppRequestWire =
            serde_json::from_slice(encoded).map_err(|_| RuntimeContractError::InvalidPayload)?;
        if wire.protocol != ApplicationProtocolVersion::V1 {
            return Err(RuntimeContractError::InvalidPayload);
        }
        Self::try_new(
            wire.request_id.into_request_id()?,
            wire.installation_id,
            wire.workspace_id,
            wire.service_generation,
            wire.client_id,
            wire.credential_generation,
            wire.correlation_id,
            wire.deadline,
            now,
            wire.operation,
            wire.arguments,
            structure_limits,
            maximum_encoded_bytes,
        )
    }

    /// Converts the admitted wall-clock deadline once into a monotonic service context.
    pub fn to_request_context(
        &self,
        now: Timestamp,
        monotonic_now: Instant,
        cancellation: CancellationToken,
        limits: ServiceLimits,
    ) -> Result<RequestContext, RuntimeContractError> {
        let remaining = self.remaining_lifetime(now)?;
        let deadline = monotonic_now
            .checked_add(remaining)
            .ok_or(RuntimeContractError::DeadlineTooDistant)?;
        Ok(RequestContext::new(
            self.request_id.clone(),
            cancellation,
            deadline,
            limits,
        ))
    }

    /// Returns the validated remaining wall-clock lifetime for transport admission.
    pub fn remaining_lifetime(&self, now: Timestamp) -> Result<Duration, RuntimeContractError> {
        let remaining = self
            .deadline
            .unix_nanos()
            .checked_sub(now.unix_nanos())
            .ok_or(RuntimeContractError::DeadlineTooDistant)?;
        validate_deadline_delta(remaining)?;
        let nanos =
            u64::try_from(remaining).map_err(|_error| RuntimeContractError::ExpiredDeadline)?;
        Ok(Duration::from_nanos(nanos))
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

    /// Exact named-client credential generation required by this request.
    #[must_use]
    pub const fn credential_generation(&self) -> CredentialGeneration {
        self.credential_generation
    }

    /// Correlation identity shared across related work.
    #[must_use]
    pub const fn correlation_id(&self) -> CorrelationId {
        self.correlation_id
    }

    /// Stable request identity reused by the application service context.
    #[must_use]
    pub const fn request_id(&self) -> &RequestId {
        &self.request_id
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct AppRequestWire {
    request_id: RequestIdWire,
    installation_id: InstallationId,
    workspace_id: WorkspaceId,
    service_generation: ServiceGeneration,
    client_id: ClientId,
    credential_generation: CredentialGeneration,
    correlation_id: CorrelationId,
    protocol: ApplicationProtocolVersion,
    deadline: Timestamp,
    operation: SourceIdentifier,
    arguments: Value,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RequestIdWire {
    Integer(i64),
    String(String),
}

impl RequestIdWire {
    fn into_request_id(self) -> Result<RequestId, RuntimeContractError> {
        match self {
            Self::Integer(value) => Ok(RequestId::Integer(value)),
            Self::String(value) => {
                RequestId::try_string(value).map_err(|_| RuntimeContractError::InvalidPayload)
            }
        }
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
#[serde(deny_unknown_fields)]
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

    /// Creates a success response after validating its complete encoded result.
    pub fn try_success(
        request_id: RequestId,
        service_generation: ServiceGeneration,
        result: Value,
        structure_limits: JsonStructureLimits,
        maximum_encoded_bytes: usize,
    ) -> Result<Self, RuntimeContractError> {
        validate_json_contract(&result, structure_limits, maximum_encoded_bytes)
            .map_err(|_error| RuntimeContractError::InvalidResponse)?;
        Ok(Self::new(request_id, service_generation, result))
    }

    /// Decodes and validates a response for one exact request and service generation.
    pub fn decode_expected(
        encoded: &[u8],
        expected_request: &RequestId,
        expected_generation: ServiceGeneration,
        structure_limits: JsonStructureLimits,
        maximum_encoded_bytes: usize,
    ) -> Result<Self, RuntimeContractError> {
        if encoded.len() > maximum_encoded_bytes {
            return Err(RuntimeContractError::InvalidResponse);
        }
        let wire: AppResponseWire =
            serde_json::from_slice(encoded).map_err(|_| RuntimeContractError::InvalidResponse)?;
        let response = Self {
            request_id: wire
                .request_id
                .into_request_id()
                .map_err(|_| RuntimeContractError::InvalidResponse)?,
            service_generation: wire.service_generation,
            result: wire.result,
        };
        if response.request_id != *expected_request
            || response.service_generation != expected_generation
        {
            return Err(RuntimeContractError::IdentityMismatch);
        }
        validate_json_contract(&response.result, structure_limits, maximum_encoded_bytes)
            .map_err(|_| RuntimeContractError::InvalidResponse)?;
        Ok(response)
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct AppResponseWire {
    request_id: RequestIdWire,
    service_generation: ServiceGeneration,
    result: Value,
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
