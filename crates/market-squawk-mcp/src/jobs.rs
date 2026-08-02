//! Stable MCP resource addressing for durable Market Squawk jobs.

use market_squawk_jobs::JobId;
use thiserror::Error;

/// Stable URI template advertised for durable job inspection.
pub const JOB_RESOURCE_TEMPLATE: &str = "market-squawk://jobs/{job_id}";

const JOB_RESOURCE_PREFIX: &str = "market-squawk://jobs/";

/// Invalid or unsupported durable-job resource URI.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum JobResourceError {
    /// The URI is not the stable Market Squawk job resource form.
    #[error("job resource URI is invalid")]
    InvalidUri,
}

/// Formats one typed job identity as its stable MCP resource URI.
#[must_use]
pub fn job_resource_uri(job_id: JobId) -> String {
    format!("{JOB_RESOURCE_PREFIX}{}", job_id.as_uuid())
}

/// Parses the stable job resource URI into its typed durable identity.
///
/// # Errors
///
/// Returns [`JobResourceError::InvalidUri`] for another scheme, an empty path, extra path
/// segments, an invalid UUID, or the nil UUID.
pub fn parse_job_resource_uri(uri: &str) -> Result<JobId, JobResourceError> {
    let value = uri
        .strip_prefix(JOB_RESOURCE_PREFIX)
        .filter(|value| !value.is_empty() && !value.contains('/'))
        .ok_or(JobResourceError::InvalidUri)?;
    value.parse().map_err(|_error| JobResourceError::InvalidUri)
}
