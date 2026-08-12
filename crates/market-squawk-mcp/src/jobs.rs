//! Stable MCP resource addressing for durable Market Squawk jobs.

use market_squawk_jobs::{JobGeneration, JobId};
use thiserror::Error;

/// Stable URI template advertised for durable job inspection.
pub const JOB_RESOURCE_TEMPLATE: &str = "market-squawk://jobs/{job_id}/generations/{generation}";

const JOB_RESOURCE_PREFIX: &str = "market-squawk://jobs/";

/// Exact durable-job generation addressed by one MCP resource URI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobResourceIdentity {
    job_id: JobId,
    generation: JobGeneration,
}

impl JobResourceIdentity {
    /// Binds one stable job identity to one exact execution generation.
    #[must_use]
    pub const fn new(job_id: JobId, generation: JobGeneration) -> Self {
        Self { job_id, generation }
    }

    /// Stable durable-job identity.
    #[must_use]
    pub const fn job_id(self) -> JobId {
        self.job_id
    }

    /// Exact one-based execution generation.
    #[must_use]
    pub const fn generation(self) -> JobGeneration {
        self.generation
    }
}

/// Invalid or unsupported durable-job resource URI.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum JobResourceError {
    /// The URI is not the stable Market Squawk job resource form.
    #[error("job resource URI is invalid")]
    InvalidUri,
}

/// Formats one typed job generation as its exact MCP resource URI.
#[must_use]
pub fn job_resource_uri(job_id: JobId, generation: JobGeneration) -> String {
    format!(
        "{JOB_RESOURCE_PREFIX}{}/generations/{}",
        job_id.as_uuid(),
        generation.get()
    )
}

/// Parses the stable job resource URI into its exact typed durable generation.
///
/// # Errors
///
/// Returns [`JobResourceError::InvalidUri`] for another scheme, a non-canonical path, an invalid
/// or nil UUID, a zero generation, or a generation outside the unsigned 64-bit range.
pub fn parse_job_resource_uri(uri: &str) -> Result<JobResourceIdentity, JobResourceError> {
    let value = uri
        .strip_prefix(JOB_RESOURCE_PREFIX)
        .ok_or(JobResourceError::InvalidUri)?;
    let mut segments = value.split('/');
    let job_id_text = segments.next().ok_or(JobResourceError::InvalidUri)?;
    if segments.next() != Some("generations") {
        return Err(JobResourceError::InvalidUri);
    }
    let generation_text = segments.next().ok_or(JobResourceError::InvalidUri)?;
    if segments.next().is_some() {
        return Err(JobResourceError::InvalidUri);
    }

    let job_id: JobId = job_id_text
        .parse()
        .map_err(|_error| JobResourceError::InvalidUri)?;
    if job_id.as_uuid().hyphenated().to_string() != job_id_text {
        return Err(JobResourceError::InvalidUri);
    }
    let generation_bytes = generation_text.as_bytes();
    if generation_bytes.is_empty()
        || generation_bytes[0] == b'0'
        || generation_bytes.len() > 20
        || !generation_bytes.iter().all(u8::is_ascii_digit)
    {
        return Err(JobResourceError::InvalidUri);
    }
    let generation = generation_text
        .parse::<u64>()
        .ok()
        .and_then(|value| JobGeneration::try_new(value).ok())
        .ok_or(JobResourceError::InvalidUri)?;
    Ok(JobResourceIdentity::new(job_id, generation))
}
