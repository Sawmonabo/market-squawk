//! Resource-bounded entry point for fuzzing the production MCP request-admission path.

use market_squawk_services::validate_json_contract;
use rmcp::model::ClientJsonRpcMessage;
use serde_json::Value;

use crate::{McpLimitSpec, McpLimits};

/// Exercises structural admission and official-SDK decoding for one bounded client frame.
///
/// Invalid and unsupported messages are normal fuzz inputs. This entry point deliberately returns
/// no decoded value and exists only with the `fuzzing` feature.
pub fn fuzz_decode_client_message(bytes: &[u8]) {
    let Ok(limits) = McpLimits::try_from(McpLimitSpec::default()) else {
        return;
    };
    if bytes.len() > limits.maximum_body_bytes() {
        return;
    }
    let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
        return;
    };
    if validate_json_contract(
        &value,
        limits.input_structure(),
        limits.maximum_body_bytes(),
    )
    .is_err()
    {
        return;
    }
    let _message = serde_json::from_value::<ClientJsonRpcMessage>(value);
}
