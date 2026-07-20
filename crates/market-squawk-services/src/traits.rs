//! Application-service capability and dispatch contracts.

use std::{collections::HashSet, fmt, sync::Arc};

use async_trait::async_trait;
use serde_json::{Map, Value};
use thiserror::Error;

use crate::{
    JsonStructureLimits, ProgressError, RequestContext, ServiceContractError, TypedToolResult,
    validate_json_contract,
};

const MAXIMUM_TOOL_NAME_BYTES: usize = 128;
const MAXIMUM_TOOL_VERSION_BYTES: usize = 64;
const MAXIMUM_TOOL_DESCRIPTION_BYTES: usize = 1024;
const MAXIMUM_TOOLS: usize = 256;
const MAXIMUM_DESCRIPTOR_SCHEMA_BYTES: usize = 64 * 1024;
const MAXIMUM_TOOL_ARGUMENT_BYTES: usize = 1024 * 1024;

/// Versioned, schema-bearing description of one application-service operation.
#[derive(Clone)]
pub struct ToolDescriptor {
    name: Arc<str>,
    version: Arc<str>,
    description: Arc<str>,
    input_schema: Map<String, Value>,
    input_schema_bytes: usize,
    effects: ToolEffects,
    input_admission: Arc<dyn ToolInputAdmission>,
}

impl fmt::Debug for ToolDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolDescriptor")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("description", &self.description)
            .field("input_schema", &"[INPUT SCHEMA REDACTED]")
            .field("input_schema_bytes", &self.input_schema_bytes)
            .field("effects", &self.effects)
            .field("input_admission", &"[TYPED ADMISSION REDACTED]")
            .finish()
    }
}

impl ToolDescriptor {
    /// Creates a descriptor whose input schema is a closed JSON object.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceCapabilityError`] when text or schema invariants are violated.
    pub fn try_new<A>(
        name: impl Into<Arc<str>>,
        version: impl Into<Arc<str>>,
        description: impl Into<Arc<str>>,
        input_schema: Value,
        effects: ToolEffects,
        input_admission: A,
    ) -> Result<Self, ServiceCapabilityError>
    where
        A: ToolInputAdmission,
    {
        let name = name.into();
        let version = version.into();
        let description = description.into();
        if !valid_tool_name(&name) {
            return Err(ServiceCapabilityError::InvalidName);
        }
        if version.is_empty() || version.len() > MAXIMUM_TOOL_VERSION_BYTES {
            return Err(ServiceCapabilityError::InvalidVersion);
        }
        if description.is_empty() || description.len() > MAXIMUM_TOOL_DESCRIPTION_BYTES {
            return Err(ServiceCapabilityError::InvalidDescription);
        }
        let Value::Object(input_schema) = input_schema else {
            return Err(ServiceCapabilityError::InvalidSchema);
        };
        if input_schema.get("type").and_then(Value::as_str) != Some("object")
            || input_schema
                .get("additionalProperties")
                .and_then(Value::as_bool)
                != Some(false)
        {
            return Err(ServiceCapabilityError::InvalidSchema);
        }
        let schema_limits = JsonStructureLimits::try_new(16, 8 * 1024, 1_000, 1_000)
            .map_err(|_| ServiceCapabilityError::InvalidSchema)?;
        let bounded_schema = Value::Object(input_schema);
        let input_schema_bytes = validate_json_contract(
            &bounded_schema,
            schema_limits,
            MAXIMUM_DESCRIPTOR_SCHEMA_BYTES,
        )
        .map_err(|_| ServiceCapabilityError::InvalidSchema)?;
        let Value::Object(input_schema) = bounded_schema else {
            return Err(ServiceCapabilityError::InvalidSchema);
        };
        Ok(Self {
            name,
            version,
            description,
            input_schema,
            input_schema_bytes,
            effects,
            input_admission: Arc::new(input_admission),
        })
    }

    /// Stable operation name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Operation contract version.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Concise public description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Closed JSON object input schema.
    #[must_use]
    pub const fn input_schema(&self) -> &Map<String, Value> {
        &self.input_schema
    }

    /// Explicit operation side-effect annotations advertised to MCP clients.
    #[must_use]
    pub const fn effects(&self) -> ToolEffects {
        self.effects
    }

    /// Atomically validates arguments and creates the only dispatchable request type.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError::InvalidRequest`] when descriptor-owned typed admission rejects the
    /// arguments.
    pub fn admit(&self, arguments: Map<String, Value>) -> Result<TypedToolRequest, ServiceError> {
        let bounded_arguments = Value::Object(arguments);
        let argument_limits = JsonStructureLimits::try_new(32, 64 * 1024, 10_000, 2_000)
            .map_err(|_| ServiceError::Internal)?;
        validate_json_contract(
            &bounded_arguments,
            argument_limits,
            MAXIMUM_TOOL_ARGUMENT_BYTES,
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        let Value::Object(arguments) = bounded_arguments else {
            return Err(ServiceError::Internal);
        };
        self.input_admission
            .admit(&arguments)
            .map_err(|_| ServiceError::InvalidRequest)?;
        Ok(TypedToolRequest {
            name: Arc::clone(&self.name),
            arguments,
        })
    }
}

/// Descriptor-owned typed argument admission shared by every transport.
///
/// Implementations should deserialize into the operation's typed request or perform equivalent
/// domain validation. Dynamic provider details must not be returned through this boundary.
pub trait ToolInputAdmission: Send + Sync + 'static {
    /// Validates one structurally bounded JSON object.
    ///
    /// # Errors
    ///
    /// Returns [`ToolInputError::Invalid`] for any caller-invalid input.
    fn admit(&self, arguments: &Map<String, Value>) -> Result<(), ToolInputError>;
}

impl<F> ToolInputAdmission for F
where
    F: Fn(&Map<String, Value>) -> Result<(), ToolInputError> + Send + Sync + 'static,
{
    fn admit(&self, arguments: &Map<String, Value>) -> Result<(), ToolInputError> {
        self(arguments)
    }
}

/// Deliberately opaque typed-admission failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ToolInputError {
    /// Arguments do not satisfy the operation's typed contract.
    #[error("tool arguments are invalid")]
    Invalid,
}

/// Explicit MCP side-effect hints for one registered operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ToolEffects {
    read_only: bool,
    destructive: bool,
    idempotent: bool,
    open_world: bool,
}

impl ToolEffects {
    /// Creates internally consistent effect annotations.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceCapabilityError::InvalidEffects`] when an operation is simultaneously
    /// read-only and destructive.
    pub const fn try_new(
        read_only: bool,
        destructive: bool,
        idempotent: bool,
        open_world: bool,
    ) -> Result<Self, ServiceCapabilityError> {
        if read_only && destructive {
            return Err(ServiceCapabilityError::InvalidEffects);
        }
        Ok(Self {
            read_only,
            destructive,
            idempotent,
            open_world,
        })
    }

    /// Explicit read-only, non-destructive, idempotent, closed-world operation.
    #[must_use]
    pub const fn read_only_closed_world() -> Self {
        Self {
            read_only: true,
            destructive: false,
            idempotent: true,
            open_world: false,
        }
    }

    /// Read-only hint.
    #[must_use]
    pub const fn read_only(self) -> bool {
        self.read_only
    }

    /// Destructive hint.
    #[must_use]
    pub const fn destructive(self) -> bool {
        self.destructive
    }

    /// Idempotency hint.
    #[must_use]
    pub const fn idempotent(self) -> bool {
        self.idempotent
    }

    /// Open-world interaction hint.
    #[must_use]
    pub const fn open_world(self) -> bool {
        self.open_world
    }
}

fn valid_tool_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    name.len() <= MAXIMUM_TOOL_NAME_BYTES
        && first.is_ascii_alphabetic()
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// Validated, deterministic application-service capability set.
#[derive(Clone, Debug, Default)]
pub struct ServiceCapabilities {
    tools: Arc<[ToolDescriptor]>,
}

impl ServiceCapabilities {
    /// Creates an empty capability set. No tools capability may be advertised for this value.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Validates uniqueness and returns a deterministic name-sorted capability set.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceCapabilityError`] for duplicate names or more than 256 operations.
    pub fn try_new(mut tools: Vec<ToolDescriptor>) -> Result<Self, ServiceCapabilityError> {
        if tools.len() > MAXIMUM_TOOLS {
            return Err(ServiceCapabilityError::TooManyTools {
                maximum: MAXIMUM_TOOLS,
            });
        }
        tools.sort_unstable_by(|left, right| left.name().cmp(right.name()));
        let mut names = HashSet::with_capacity(tools.len());
        if tools.iter().any(|tool| !names.insert(tool.name())) {
            return Err(ServiceCapabilityError::DuplicateName);
        }
        Ok(Self {
            tools: tools.into(),
        })
    }

    /// Registered operations in deterministic order.
    #[must_use]
    pub fn tools(&self) -> &[ToolDescriptor] {
        &self.tools
    }

    /// Returns the registered descriptor for an exact operation name.
    #[must_use]
    pub fn find(&self, name: &str) -> Option<&ToolDescriptor> {
        self.tools
            .binary_search_by(|tool| tool.name().cmp(name))
            .ok()
            .and_then(|index| self.tools.get(index))
    }

    /// True when at least one operation is registered.
    #[must_use]
    pub fn has_tools(&self) -> bool {
        !self.tools.is_empty()
    }
}

/// Invalid service capability definition.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ServiceCapabilityError {
    /// Tool name is empty, oversized, or outside the portable grammar.
    #[error("invalid service tool name")]
    InvalidName,
    /// Version is empty or oversized.
    #[error("invalid service tool version")]
    InvalidVersion,
    /// Description is empty or oversized.
    #[error("invalid service tool description")]
    InvalidDescription,
    /// Input schema is not a closed JSON object schema.
    #[error("service tool input schema must be a closed object")]
    InvalidSchema,
    /// Side-effect annotations contradict each other.
    #[error("service tool effect annotations are inconsistent")]
    InvalidEffects,
    /// Tool names must be unique.
    #[error("service capability contains a duplicate tool name")]
    DuplicateName,
    /// Capability set exceeded its hard ceiling.
    #[error("service capability exceeds {maximum} tools")]
    TooManyTools {
        /// Maximum operations in one local service.
        maximum: usize,
    },
}

/// Validated generic request dispatched to a registered typed domain service.
#[derive(Clone)]
pub struct TypedToolRequest {
    name: Arc<str>,
    arguments: Map<String, Value>,
}

impl TypedToolRequest {
    /// Registered operation name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Schema-admitted argument object.
    #[must_use]
    pub const fn arguments(&self) -> &Map<String, Value> {
        &self.arguments
    }
}

impl fmt::Debug for TypedToolRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TypedToolRequest")
            .field("name", &self.name)
            .field("arguments", &"[ARGUMENTS REDACTED]")
            .finish()
    }
}

/// Stable service-error class used by transports and audit records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceErrorClass {
    /// Request failed validation.
    InvalidRequest,
    /// Requested operation or object does not exist.
    NotFound,
    /// Authority is absent or insufficient.
    Unauthorized,
    /// Bounded resource ceiling was reached.
    ResourceExhausted,
    /// Request cancellation won the lifecycle race.
    Cancelled,
    /// Request deadline elapsed.
    DeadlineExceeded,
    /// Required local service is unavailable.
    Unavailable,
    /// Service returned a contract-invalid result.
    InvalidResult,
    /// Internal failure with no caller-safe detail.
    Internal,
}

/// Typed application-service failure with deliberately bounded, non-secret display text.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ServiceError {
    /// Invalid request.
    #[error("service request is invalid")]
    InvalidRequest,
    /// Operation not found.
    #[error("service operation was not found")]
    NotFound,
    /// Missing authority.
    #[error("service request is not authorized")]
    Unauthorized,
    /// Resource ceiling reached.
    #[error("service resource limit exceeded")]
    ResourceExhausted,
    /// Request cancelled.
    #[error("service request was cancelled")]
    Cancelled,
    /// Request deadline elapsed.
    #[error("service request deadline exceeded")]
    DeadlineExceeded,
    /// Required local dependency unavailable.
    #[error("service is unavailable")]
    Unavailable,
    /// Result violated its transport-neutral contract.
    #[error("service returned an invalid result")]
    InvalidResult,
    /// Internal failure with no dynamic payload.
    #[error("service failed internally")]
    Internal,
}

impl ServiceError {
    /// Stable classification without provider or secret payloads.
    #[must_use]
    pub const fn class(self) -> ServiceErrorClass {
        match self {
            Self::InvalidRequest => ServiceErrorClass::InvalidRequest,
            Self::NotFound => ServiceErrorClass::NotFound,
            Self::Unauthorized => ServiceErrorClass::Unauthorized,
            Self::ResourceExhausted => ServiceErrorClass::ResourceExhausted,
            Self::Cancelled => ServiceErrorClass::Cancelled,
            Self::DeadlineExceeded => ServiceErrorClass::DeadlineExceeded,
            Self::Unavailable => ServiceErrorClass::Unavailable,
            Self::InvalidResult => ServiceErrorClass::InvalidResult,
            Self::Internal => ServiceErrorClass::Internal,
        }
    }
}

impl From<ServiceContractError> for ServiceError {
    fn from(_source: ServiceContractError) -> Self {
        Self::InvalidResult
    }
}

impl From<ProgressError> for ServiceError {
    fn from(source: ProgressError) -> Self {
        match source {
            ProgressError::Cancelled => Self::Cancelled,
            ProgressError::DeadlineExceeded => Self::DeadlineExceeded,
            ProgressError::TooManyUpdates | ProgressError::MessageTooLong => {
                Self::ResourceExhausted
            }
            ProgressError::NonMonotonic | ProgressError::InvalidValue => Self::InvalidRequest,
            ProgressError::Unavailable | ProgressError::Delivery => Self::Unavailable,
            ProgressError::State => Self::Internal,
        }
    }
}

/// Application-service surface shared by CLI and protocol transports.
#[async_trait]
pub trait ToolServices: Send + Sync + 'static {
    /// Returns the complete validated capability set for this service instance.
    fn capabilities(&self) -> ServiceCapabilities;

    /// Executes one schema-admitted operation under the supplied cancellation, deadline, and
    /// result limits.
    ///
    /// Implementations must observe [`RequestContext::cancellation`] and
    /// [`RequestContext::deadline`] before authoritative mutation and at safe interruption
    /// boundaries. Once the request cancellation token is cancelled, the returned future must be
    /// immediately safe to drop; the bounded session-shutdown timeout is only best-effort
    /// operational grace and is not a prerequisite for Drop safety. Externally visible mutation
    /// must remain committed or rolled back according to the operation's atomicity contract.
    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError>;
}
