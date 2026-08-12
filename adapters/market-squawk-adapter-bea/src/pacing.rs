//! Official BEA ceilings and conservative shared-queue application policy.

use std::time::Duration;

/// Verified BEA ceiling: requests in the preceding one-minute window.
pub const BEA_OFFICIAL_REQUESTS_PER_MINUTE: u32 = 100;
/// Verified BEA ceiling: response bytes in the preceding one-minute window (100 decimal MB).
pub const BEA_OFFICIAL_RESPONSE_BYTES_PER_MINUTE: u64 = 100_000_000;
/// Verified BEA ceiling: errors in the preceding one-minute window.
pub const BEA_OFFICIAL_ERRORS_PER_MINUTE: u32 = 30;

/// Application policy: requests reserved for the shared BEA queue each minute.
pub const BEA_APPLICATION_REQUESTS_PER_MINUTE: u32 = 60;
/// Application policy: response bytes reserved for the shared BEA queue each minute.
pub const BEA_APPLICATION_RESPONSE_BYTES_PER_MINUTE: u64 = 60_000_000;
/// Application policy: errors admitted by the shared BEA queue each minute.
pub const BEA_APPLICATION_ERRORS_PER_MINUTE: u32 = 10;
/// Application policy: normal acquisition is serialized.
pub const BEA_APPLICATION_MAX_IN_FLIGHT: u32 = 1;
/// Application policy: a serialized 60 RPM schedule leaves at least one second between starts.
pub const BEA_MINIMUM_REQUEST_INTERVAL: Duration = Duration::from_secs(1);

/// One request/byte/error window declaration for shared provider-rate integration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BeaWindowBudget {
    requests: u32,
    response_bytes: u64,
    errors: u32,
    window: Duration,
}

impl BeaWindowBudget {
    /// Returns the request ceiling in this window.
    pub const fn requests(self) -> u32 {
        self.requests
    }

    /// Returns the response-byte ceiling in this window.
    pub const fn response_bytes(self) -> u64 {
        self.response_bytes
    }

    /// Returns the provider-error ceiling in this window.
    pub const fn errors(self) -> u32 {
        self.errors
    }

    /// Returns the fixed one-minute window.
    pub const fn window(self) -> Duration {
        self.window
    }
}

/// BEA pacing declarations for registration with Market Squawk's shared provider-rate authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BeaPacingPolicy;

impl BeaPacingPolicy {
    /// Returns provider-published limits. These are evidence, not the scheduler target.
    pub const fn official() -> BeaWindowBudget {
        BeaWindowBudget {
            requests: BEA_OFFICIAL_REQUESTS_PER_MINUTE,
            response_bytes: BEA_OFFICIAL_RESPONSE_BYTES_PER_MINUTE,
            errors: BEA_OFFICIAL_ERRORS_PER_MINUTE,
            window: Duration::from_secs(60),
        }
    }

    /// Returns the lower application budget shared by every job using one BEA `UserID`.
    pub const fn application() -> BeaWindowBudget {
        BeaWindowBudget {
            requests: BEA_APPLICATION_REQUESTS_PER_MINUTE,
            response_bytes: BEA_APPLICATION_RESPONSE_BYTES_PER_MINUTE,
            errors: BEA_APPLICATION_ERRORS_PER_MINUTE,
            window: Duration::from_secs(60),
        }
    }

    /// Returns the normal serialization ceiling.
    pub const fn max_in_flight() -> u32 {
        BEA_APPLICATION_MAX_IN_FLIGHT
    }

    /// Returns the minimum interval between normal request starts.
    pub const fn minimum_request_interval() -> Duration {
        BEA_MINIMUM_REQUEST_INTERVAL
    }

    /// BEA HTTP 429 responses must install the provider's current `Retry-After` cooldown.
    pub const fn honors_retry_after() -> bool {
        true
    }
}
