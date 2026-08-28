//! Exact multidimensional BEA quota declaration for the shared durable provider-rate authority.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use market_squawk_sources::ProviderRateDeclaration;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{BeaPacingPolicy, BeaSourceError, BeaWindowBudget, bea_provider_rate_declaration};

/// Invalid provider-local quota declaration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BeaQuotaDeclarationError {
    /// Request, byte, error, window, or shared-authority identity evidence is inconsistent.
    #[error("invalid BEA quota declaration")]
    InvalidDeclaration,
}

/// Post-response dimensions the shared durable provider-rate authority must settle.
///
/// The current shared request permit owns request-window and concurrency admission. It does not
/// yet expose durable response-byte or provider/body-error settlement, so root composition must
/// implement both dimensions before claiming the full BEA 60 MB/10-error application policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BeaRequiredSharedSettlement {
    /// Settle exact received provider body bytes into the shared sliding window.
    ResponseBytes,
    /// Settle HTTP and BEA body-level provider errors into the shared sliding window.
    ProviderErrors,
}

const REQUIRED_SHARED_SETTLEMENTS: [BeaRequiredSharedSettlement; 2] = [
    BeaRequiredSharedSettlement::ResponseBytes,
    BeaRequiredSharedSettlement::ProviderErrors,
];

/// Serializable request/response-byte/error ceiling for one exact sliding window.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BeaQuotaWindowDeclaration {
    requests: u32,
    response_bytes: u64,
    errors: u32,
    window_nanos: u64,
}

impl BeaQuotaWindowDeclaration {
    fn try_from_budget(value: BeaWindowBudget) -> Result<Self, BeaQuotaDeclarationError> {
        let window_nanos = u64::try_from(value.window().as_nanos())
            .map_err(|_| BeaQuotaDeclarationError::InvalidDeclaration)?;
        if value.requests() == 0
            || value.response_bytes() == 0
            || value.errors() == 0
            || window_nanos == 0
        {
            return Err(BeaQuotaDeclarationError::InvalidDeclaration);
        }
        Ok(Self {
            requests: value.requests(),
            response_bytes: value.response_bytes(),
            errors: value.errors(),
            window_nanos,
        })
    }

    /// Returns the request ceiling.
    pub const fn requests(self) -> u32 {
        self.requests
    }
    /// Returns the exact received-body byte ceiling.
    pub const fn response_bytes(self) -> u64 {
        self.response_bytes
    }
    /// Returns the provider-error ceiling.
    pub const fn errors(self) -> u32 {
        self.errors
    }
    /// Returns the sliding-window duration.
    pub const fn window_nanos(self) -> u64 {
        self.window_nanos
    }
}

/// Provider-local target declaration joined to the shared request authority.
///
/// This is evidence and configuration, not an adapter-local counter. The embedded
/// [`ProviderRateDeclaration`] covers request windows and concurrency. Exact response bytes,
/// provider/body errors, cooldown, and crash recovery must be settled by shared root authority;
/// `required_shared_settlements` makes the two currently missing dimensions explicit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BeaProviderQuotaDeclaration {
    shared_request_declaration: ProviderRateDeclaration,
    official: BeaQuotaWindowDeclaration,
    application: BeaQuotaWindowDeclaration,
    maximum_in_flight: u32,
    minimum_request_interval_nanos: u64,
    honors_retry_after: bool,
    declaration_digest: EvidenceDigest,
}

impl BeaProviderQuotaDeclaration {
    /// Builds and validates the complete BEA request/byte/error declaration.
    pub fn try_new() -> Result<Self, BeaSourceError> {
        let shared_request_declaration = bea_provider_rate_declaration()?;
        let official = BeaQuotaWindowDeclaration::try_from_budget(BeaPacingPolicy::official())
            .map_err(|_| BeaSourceError::InvalidConfiguration)?;
        let application =
            BeaQuotaWindowDeclaration::try_from_budget(BeaPacingPolicy::application())
                .map_err(|_| BeaSourceError::InvalidConfiguration)?;
        let maximum_in_flight = BeaPacingPolicy::max_in_flight();
        let minimum_request_interval_nanos =
            u64::try_from(BeaPacingPolicy::minimum_request_interval().as_nanos())
                .map_err(|_| BeaSourceError::InvalidConfiguration)?;
        let honors_retry_after = BeaPacingPolicy::honors_retry_after();
        let shared_policy = shared_request_declaration.policy();
        let has_pacing_window = (0..shared_policy.window_count()).any(|index| {
            shared_policy.window(index).is_some_and(|window| {
                window.requests_per_window() == 1
                    && window.window_nanos() == minimum_request_interval_nanos
                    && window.semantics() == market_squawk_sources::BudgetWindowSemantics::Sliding
            })
        });
        let has_application_window = (0..shared_policy.window_count()).any(|index| {
            shared_policy.window(index).is_some_and(|window| {
                window.requests_per_window() == application.requests
                    && window.window_nanos() == application.window_nanos
                    && window.semantics() == market_squawk_sources::BudgetWindowSemantics::Sliding
            })
        });
        if application.requests >= official.requests
            || application.response_bytes >= official.response_bytes
            || application.errors >= official.errors
            || application.window_nanos != official.window_nanos
            || maximum_in_flight == 0
            || maximum_in_flight > application.requests
            || minimum_request_interval_nanos == 0
            || !honors_retry_after
            || shared_policy.window_count() != 2
            || !has_pacing_window
            || !has_application_window
            || u32::from(shared_policy.max_concurrent()) != maximum_in_flight
        {
            return Err(BeaSourceError::InvalidConfiguration);
        }
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk/bea-provider-quota-declaration/v1");
        hasher.update(shared_request_declaration.declaration_digest().bytes());
        for value in [official, application] {
            hasher.update(value.requests.to_be_bytes());
            hasher.update(value.response_bytes.to_be_bytes());
            hasher.update(value.errors.to_be_bytes());
            hasher.update(value.window_nanos.to_be_bytes());
        }
        hasher.update(maximum_in_flight.to_be_bytes());
        hasher.update(minimum_request_interval_nanos.to_be_bytes());
        hasher.update([u8::from(honors_retry_after)]);
        let declaration_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into());
        Ok(Self {
            shared_request_declaration,
            official,
            application,
            maximum_in_flight,
            minimum_request_interval_nanos,
            honors_retry_after,
            declaration_digest,
        })
    }

    /// Returns request-window/concurrency admission for the sole durable provider-rate authority.
    ///
    /// This declaration does not claim response-byte or provider-error settlement.
    pub const fn shared_request_declaration(&self) -> &ProviderRateDeclaration {
        &self.shared_request_declaration
    }

    /// Returns the exact missing shared post-response settlement dimensions.
    pub const fn required_shared_settlements(&self) -> &'static [BeaRequiredSharedSettlement; 2] {
        &REQUIRED_SHARED_SETTLEMENTS
    }
    /// Returns provider-published ceilings.
    pub const fn official(&self) -> BeaQuotaWindowDeclaration {
        self.official
    }
    /// Returns lower application target ceilings; see `required_shared_settlements`.
    pub const fn application(&self) -> BeaQuotaWindowDeclaration {
        self.application
    }
    /// Returns the shared concurrency ceiling.
    pub const fn maximum_in_flight(&self) -> u32 {
        self.maximum_in_flight
    }
    /// Returns the shared scheduler's normal lower start-interval target.
    pub const fn minimum_request_interval_nanos(&self) -> u64 {
        self.minimum_request_interval_nanos
    }
    /// Returns whether shared cooldown must honor a valid `Retry-After` response.
    pub const fn honors_retry_after(&self) -> bool {
        self.honors_retry_after
    }
    /// Returns the complete multidimensional declaration commitment.
    pub const fn declaration_digest(&self) -> EvidenceDigest {
        self.declaration_digest
    }
}

/// Builds the exact BEA declaration that shared app composition must register and settle.
pub fn bea_provider_quota_declaration() -> Result<BeaProviderQuotaDeclaration, BeaSourceError> {
    BeaProviderQuotaDeclaration::try_new()
}
