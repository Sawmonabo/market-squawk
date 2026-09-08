//! Verified page capacity and the separate conservative application admission boundary.

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::time::Duration;

use market_squawk_sources::{
    BackoffPolicy, BudgetScope, BudgetWindowSemantics, NetworkPolicyError, ProviderBudgetPolicy,
    ProviderBudgetWindow,
};

use crate::{EIA_MAX_JSON_PAGE_ROWS, EiaError};

/// Market Squawk's minimum interval between admitted EIA requests.
pub const EIA_APPLICATION_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(1);

/// Market Squawk admits only one in-flight EIA request until runtime evidence supports less
/// conservative policy.
pub const EIA_APPLICATION_MAX_CONCURRENT_REQUESTS: u16 = 1;

const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// Constructs the exact lower application policy registered through `ProviderRateAuthority`.
///
/// The maintained first-party evidence does not publish a numeric request-rate or concurrency
/// ceiling. Runtime admission therefore uses this one-request sliding second and one in-flight
/// slot until a separately reviewed application policy replaces it.
pub fn eia_application_provider_budget(
    scope: BudgetScope,
    backoff: BackoffPolicy,
) -> Result<ProviderBudgetPolicy, NetworkPolicyError> {
    ProviderBudgetPolicy::try_new_conjunctive(
        scope,
        &[ProviderBudgetWindow::try_new(
            NonZeroU32::new(1).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
            NonZeroU64::new(NANOS_PER_SECOND).ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
            BudgetWindowSemantics::Sliding,
        )?],
        NonZeroU16::new(EIA_APPLICATION_MAX_CONCURRENT_REQUESTS)
            .ok_or(NetworkPolicyError::InvalidBudgetPolicy)?,
        backoff,
    )
}

pub(crate) fn matches_application_provider_budget(policy: &ProviderBudgetPolicy) -> bool {
    policy.window_count() == 1
        && policy.window(0).is_some_and(|window| {
            window.requests_per_window() == 1
                && window.window_nanos() == NANOS_PER_SECOND
                && window.semantics() == BudgetWindowSemantics::Sliding
        })
        && policy.max_concurrent() == EIA_APPLICATION_MAX_CONCURRENT_REQUESTS
}

/// Evidence classification for one capacity statement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EiaEvidenceClass {
    /// A fact established by maintained first-party provider documentation.
    VerifiedProviderFact,
    /// A Market Squawk scheduling policy, not a provider limit.
    ApplicationPolicy,
}

/// Current maintained provider capacity facts, kept distinct from application admission policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EiaCapacityGuidance {
    max_json_page_rows: u16,
    evidence_class: EiaEvidenceClass,
}

impl EiaCapacityGuidance {
    /// Returns the current maintained first-party capacity facts.
    pub const fn current() -> Self {
        Self {
            max_json_page_rows: EIA_MAX_JSON_PAGE_ROWS as u16,
            evidence_class: EiaEvidenceClass::VerifiedProviderFact,
        }
    }

    /// Returns the documented maximum JSON rows in one response page.
    pub const fn max_json_page_rows(self) -> u16 {
        self.max_json_page_rows
    }

    /// Returns maintained sustained-rate guidance when the provider publishes one.
    ///
    /// The current reviewed first-party contract publishes no numeric request-rate ceiling.
    pub const fn sustained_requests_per_hour(self) -> Option<u32> {
        None
    }

    /// Returns maintained burst-rate guidance when the provider publishes one.
    ///
    /// The current reviewed first-party contract publishes no numeric burst ceiling.
    pub const fn burst_requests_per_second(self) -> Option<u32> {
        None
    }

    /// Returns the evidence classification.
    pub const fn evidence_class(self) -> EiaEvidenceClass {
        self.evidence_class
    }
}

/// Admitted application policy for a shared durable provider-rate authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EiaApplicationBudget {
    minimum_request_interval: Duration,
    max_concurrent_requests: u16,
    max_json_page_rows: u16,
    evidence_class: EiaEvidenceClass,
}

impl EiaApplicationBudget {
    /// Constructs a budget that may only be more conservative than the maintained policy.
    pub fn try_new(
        minimum_request_interval: Duration,
        max_concurrent_requests: u16,
        max_json_page_rows: u16,
    ) -> Result<Self, EiaError> {
        if minimum_request_interval < EIA_APPLICATION_MIN_REQUEST_INTERVAL
            || max_concurrent_requests == 0
            || max_concurrent_requests > EIA_APPLICATION_MAX_CONCURRENT_REQUESTS
            || max_json_page_rows == 0
            || usize::from(max_json_page_rows) > EIA_MAX_JSON_PAGE_ROWS
        {
            return Err(EiaError::InvalidLimit);
        }
        Ok(Self {
            minimum_request_interval,
            max_concurrent_requests,
            max_json_page_rows,
            evidence_class: EiaEvidenceClass::ApplicationPolicy,
        })
    }

    /// Returns the maintained initial policy.
    pub const fn production_default() -> Self {
        Self {
            minimum_request_interval: EIA_APPLICATION_MIN_REQUEST_INTERVAL,
            max_concurrent_requests: EIA_APPLICATION_MAX_CONCURRENT_REQUESTS,
            max_json_page_rows: EIA_MAX_JSON_PAGE_ROWS as u16,
            evidence_class: EiaEvidenceClass::ApplicationPolicy,
        }
    }

    /// Returns the minimum interval between admitted requests.
    pub const fn minimum_request_interval(self) -> Duration {
        self.minimum_request_interval
    }

    /// Returns the maximum number of concurrent requests.
    pub const fn max_concurrent_requests(self) -> u16 {
        self.max_concurrent_requests
    }

    /// Returns the maximum requested JSON page size.
    pub const fn max_json_page_rows(self) -> u16 {
        self.max_json_page_rows
    }

    /// Returns the evidence classification.
    pub const fn evidence_class(self) -> EiaEvidenceClass {
        self.evidence_class
    }
}
