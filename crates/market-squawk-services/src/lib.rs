//! Transport-neutral, bounded application-service contracts.
//!
//! This crate owns the request, response, cancellation, deadline, and result-limit contracts shared
//! by local transports. It deliberately contains no protocol framing or business-domain handlers.

mod contract;
mod progress;
mod request;
mod response;
mod traits;

pub use contract::{
    ScopeRequirement, ServiceDomain, SourceEvidencePolicy, TOOL_CONFIRMATION_FIELD,
    TOOL_INSTRUMENT_IDS_FIELD, TOOL_RESULT_LIMITS_FIELD, TOOL_SOURCE_COVERAGE_FIELD,
    TOOL_TIME_RANGE_FIELD, ToolArtifactPolicy, ToolAuthorization, ToolContract, ToolResultPolicy,
    ToolScope,
};
pub use progress::{
    ProgressDelivery, ProgressError, ProgressLimits, ProgressLimitsError, ProgressReporter,
    ProgressSink, ProgressUpdate,
};

pub use request::{
    JsonContractError, JsonStructureLimits, RequestContext, RequestId, RequestIdError,
    ServiceLimits, ServiceLimitsError, validate_json_contract,
};
pub use response::{ResultCompleteness, ServiceContractError, ToolResultMetadata, TypedToolResult};
pub use traits::{
    ServiceCapabilities, ServiceCapabilityError, ServiceError, ServiceErrorClass, ToolDescriptor,
    ToolEffects, ToolInputAdmission, ToolInputError, ToolServices, TypedToolRequest,
};
