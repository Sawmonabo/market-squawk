use std::num::NonZeroUsize;

use crate::SchwabAdapterError;

/// Caller-owned finite parser bounds.
///
/// These values are resource controls, not Schwab rate, batch, response, or symbol limits. The
/// shared scheduler supplies them from local policy and retained runtime evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParseBounds {
    max_response_bytes: NonZeroUsize,
    max_records: NonZeroUsize,
    max_json_nodes: NonZeroUsize,
    max_json_depth: NonZeroUsize,
    max_unknown_fields: usize,
    max_unknown_bytes: usize,
}

impl ParseBounds {
    /// Constructs explicit finite parse bounds without assigning provider capacity.
    pub const fn new(
        max_response_bytes: NonZeroUsize,
        max_records: NonZeroUsize,
        max_json_nodes: NonZeroUsize,
        max_json_depth: NonZeroUsize,
        max_unknown_fields: usize,
        max_unknown_bytes: usize,
    ) -> Self {
        Self {
            max_response_bytes,
            max_records,
            max_json_nodes,
            max_json_depth,
            max_unknown_fields,
            max_unknown_bytes,
        }
    }

    /// Maximum admitted response or frame bytes.
    pub const fn max_response_bytes(self) -> usize {
        self.max_response_bytes.get()
    }

    /// Maximum provider-native records admitted from one payload.
    pub const fn max_records(self) -> usize {
        self.max_records.get()
    }

    /// Maximum JSON scalar, array, and object nodes.
    pub const fn max_json_nodes(self) -> usize {
        self.max_json_nodes.get()
    }

    /// Maximum JSON nesting depth, with the root at depth one.
    pub const fn max_json_depth(self) -> usize {
        self.max_json_depth.get()
    }

    /// Maximum unrecognized provider-native fields retained in diagnostics.
    pub const fn max_unknown_fields(self) -> usize {
        self.max_unknown_fields
    }

    /// Maximum canonical JSON bytes represented by unknown-field diagnostics.
    pub const fn max_unknown_bytes(self) -> usize {
        self.max_unknown_bytes
    }
}

/// Runtime-owned admission applied while building a request.
///
/// `max_items` is the current measured/application admission for this one request, not a provider
/// guarantee. No default or hidden Schwab ceiling exists in this crate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestAdmission {
    max_request_bytes: NonZeroUsize,
    max_items: NonZeroUsize,
}

impl RequestAdmission {
    /// Constructs a finite request admission selected by the shared scheduler.
    pub const fn new(max_request_bytes: NonZeroUsize, max_items: NonZeroUsize) -> Self {
        Self {
            max_request_bytes,
            max_items,
        }
    }

    /// Maximum encoded URL or frame bytes for this request.
    pub const fn max_request_bytes(self) -> usize {
        self.max_request_bytes.get()
    }

    /// Maximum items admitted for this request at this runtime generation.
    pub const fn max_items(self) -> usize {
        self.max_items.get()
    }
}

/// Unit attached to capacity counters so requests are never confused with observations.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CapacityUnit {
    /// HTTP requests.
    Requests,
    /// Provider symbols.
    Symbols,
    /// Option contracts.
    OptionContracts,
    /// Historical candles.
    Candles,
    /// Stream frames.
    Frames,
    /// Stream events or content records.
    StreamEvents,
}

/// One retained provider-capacity observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityObservation {
    /// Unit measured by requested/returned/missing dispositions.
    pub unit: CapacityUnit,
    /// Work submitted to the provider.
    pub requested: u64,
    /// Valid unique units returned.
    pub returned: u64,
    /// Requested units absent from the response.
    pub missing: u64,
    /// Extra duplicate units, never counted as valid returns.
    pub duplicates: u64,
    /// Returned units rejected by native validation.
    pub malformed: u64,
    /// Unrequested units returned by the provider.
    pub unexpected: u64,
    /// Exact encoded request bytes.
    pub request_bytes: u64,
    /// Exact received response bytes.
    pub response_bytes: u64,
    /// End-to-end request latency in milliseconds.
    pub latency_ms: u64,
    /// Provider HTTP status, or zero for a Streamer frame.
    pub status: u16,
    /// Whether a Retry-After value was present and validated by the transport.
    pub retry_after_present: bool,
    /// Whether parsing or semantic validation failed.
    pub validation_failed: bool,
}

impl CapacityObservation {
    /// Validates internally consistent request/return accounting.
    pub fn validate(self) -> Result<Self, SchwabAdapterError> {
        if self.unit == CapacityUnit::Requests {
            if self.requested == 0 || self.returned > self.requested {
                return Err(SchwabAdapterError::InvalidInput);
            }
            return Ok(self);
        }
        let disposed = self
            .returned
            .checked_add(self.missing)
            .and_then(|value| value.checked_add(self.malformed))
            .ok_or(SchwabAdapterError::ArithmeticOverflow)?;
        if self.requested == 0 || disposed != self.requested {
            return Err(SchwabAdapterError::InvalidInput);
        }
        Ok(self)
    }

    /// Converts measured evidence into a non-numeric scheduler signal.
    pub const fn assessment(self) -> AdaptiveAssessment {
        if self.status == 429 || self.retry_after_present {
            AdaptiveAssessment::RateLimited
        } else if self.validation_failed || self.malformed > 0 || self.unexpected > 0 {
            AdaptiveAssessment::IntegrityPressure
        } else if self.missing > 0 {
            AdaptiveAssessment::Partial
        } else {
            AdaptiveAssessment::Complete
        }
    }
}

/// Evidence-only signal consumed by the shared adaptive scheduler.
///
/// This crate never turns a signal into an invented RPM, batch, or symbol ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdaptiveAssessment {
    /// Every requested unit was returned and validated.
    Complete,
    /// Some requested units were absent.
    Partial,
    /// Provider retry/rate pressure was observed.
    RateLimited,
    /// Malformed, unexpected, or schema-invalid data was observed.
    IntegrityPressure,
}

/// Checked aggregate runtime counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CapacityCounters {
    /// Provider requests or frames attempted.
    pub attempts: u64,
    /// Requested same-unit work.
    pub requested: u64,
    /// Valid unique same-unit work.
    pub returned: u64,
    /// Missing requested work.
    pub missing: u64,
    /// Duplicate work.
    pub duplicates: u64,
    /// Malformed work.
    pub malformed: u64,
    /// Unexpected work.
    pub unexpected: u64,
    /// Encoded request bytes.
    pub request_bytes: u64,
    /// Received response bytes.
    pub response_bytes: u64,
    /// HTTP 429 observations.
    pub rate_limited: u64,
    /// Validation failures.
    pub validation_failures: u64,
}

impl CapacityCounters {
    /// Adds one validated observation using checked arithmetic.
    pub fn record(&mut self, observation: CapacityObservation) -> Result<(), SchwabAdapterError> {
        let observation = observation.validate()?;
        self.attempts = checked_add(self.attempts, 1)?;
        self.requested = checked_add(self.requested, observation.requested)?;
        self.returned = checked_add(self.returned, observation.returned)?;
        self.missing = checked_add(self.missing, observation.missing)?;
        self.duplicates = checked_add(self.duplicates, observation.duplicates)?;
        self.malformed = checked_add(self.malformed, observation.malformed)?;
        self.unexpected = checked_add(self.unexpected, observation.unexpected)?;
        self.request_bytes = checked_add(self.request_bytes, observation.request_bytes)?;
        self.response_bytes = checked_add(self.response_bytes, observation.response_bytes)?;
        if observation.status == 429 {
            self.rate_limited = checked_add(self.rate_limited, 1)?;
        }
        if observation.validation_failed {
            self.validation_failures = checked_add(self.validation_failures, 1)?;
        }
        Ok(())
    }
}

fn checked_add(left: u64, right: u64) -> Result<u64, SchwabAdapterError> {
    left.checked_add(right)
        .ok_or(SchwabAdapterError::ArithmeticOverflow)
}
