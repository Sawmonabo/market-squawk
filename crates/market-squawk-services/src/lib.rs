//! Transport-neutral, bounded application-service contracts.
//!
//! This crate owns the request, response, cancellation, deadline, and result-limit contracts shared
//! by local transports. It deliberately contains no protocol framing or business-domain handlers.

mod progress;
mod request;
mod response;
mod traits;

pub use progress::{
    ProgressError, ProgressLimits, ProgressLimitsError, ProgressReporter, ProgressSink,
    ProgressUpdate,
};

pub use request::{
    JsonContractError, JsonStructureLimits, RequestContext, RequestId, RequestIdError,
    ServiceLimits, ServiceLimitsError, validate_json_contract,
};
pub use response::{ServiceContractError, TypedToolResult};
pub use traits::{
    ServiceCapabilities, ServiceCapabilityError, ServiceError, ServiceErrorClass, ToolDescriptor,
    ToolEffects, ToolInputAdmission, ToolInputError, ToolServices, TypedToolRequest,
};
