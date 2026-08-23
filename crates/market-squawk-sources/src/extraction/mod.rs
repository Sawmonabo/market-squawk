//! Bounded discovery and research extraction contracts.

mod batch;
mod capture;
mod contracts;
mod revisions;

use futures_util::future::BoxFuture;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::registry::ExtractionAuthority;
use crate::{
    AuthorizedRequest, BudgetDispatchDecision, BudgetPermit, BudgetReservation,
    BudgetUnavailableReason, HttpClientProfile, HttpRequestBounds, MonotonicInstant,
    NetworkPolicyError, RedirectAuthorization, SourceError, SourceMetadataProvider,
};

pub use batch::{ExtractionBatch, ExtractionBatchAccumulator, ExtractionContentIdentity};
pub use capture::{
    MAX_PROVIDER_CAPTURE_BYTES, MAX_PROVIDER_CAPTURE_PAGE_BYTES, MAX_PROVIDER_CAPTURE_PAGES,
    ProviderCaptureError, ProviderCaptureMaterial, ProviderCaptureMaterialSealError,
    ProviderCapturePageReceipt, ProviderCaptureRequestGraphComponent, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt,
    SourceObjectCaptureIdentity,
};
pub use contracts::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryBatch, DiscoveryRequest,
    DiscoveryRequestId, ExtractionError, ExtractionRecord, ExtractionRequest, ExtractionRequestId,
    MAX_DISCOVERY_OBJECTS, MAX_EXTRACTION_BATCH_BYTES, MAX_EXTRACTION_RECORD_BYTES,
    MAX_EXTRACTION_RECORDS, MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, SourceObject,
    payload_matches_exact_evidence,
};
pub use revisions::{
    CanonicalObservationFamily, CanonicalObservationPayload, ExtractionRevisionEvidence,
    ExtractionRevisionPlan, MAX_OBSERVED_REVISION_BATCH_BYTES, MAX_OBSERVED_REVISION_BATCH_RECORDS,
    MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES, MAX_OBSERVED_VERSION_EVIDENCE_BYTES,
    ObservedProviderOrder, ObservedRevisionAssignments, ObservedRevisionAuthority,
    ObservedRevisionBatch, ObservedRevisionError, ObservedRevisionRecord, ObservedSemanticPayload,
    ObservedVersionEvidence, ObservedVersionKind, PitV1CanonicalEncoder, PitV1EncodingControl,
    PitV1EncodingError,
};

/// Object-safe research extraction contract with one boxed future per request.
pub trait ExtractionSource: SourceMetadataProvider + Sync {
    /// Discovers a bounded set of versioned source objects.
    ///
    /// Every provider HTTP request, including each pagination request, must acquire its own
    /// [`ExtractionRequestPermit`] from `authority`. The permit must be consumed immediately
    /// before sending the exact request target, and the resulting in-flight permit must be held
    /// until that response has been fully validated or discarded.
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>>;

    /// Extracts one source object into a bounded normalized batch.
    ///
    /// Every provider HTTP request, including each pagination request, must acquire its own
    /// [`ExtractionRequestPermit`] from `authority`. The permit must be consumed immediately
    /// before sending the exact request target, and the resulting in-flight permit must be held
    /// until that response has been fully validated or discarded.
    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>>;
}

/// Failure to admit or retain one extraction operation under current registry authority.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ExtractionAuthorityError {
    /// Metadata was replaced, revoked, or its authoritative registry was dropped.
    #[error("extraction authority is no longer current")]
    NotCurrent,
    /// Authorization or coverage is no longer effective at sealed registry time.
    #[error("extraction authority is outside its effective interval")]
    NotEffective,
    /// The sealed registry clock could not provide a reading.
    #[error("extraction authority trusted time is unavailable")]
    TrustedTimeUnavailable,
    /// Registry authority-time continuity is permanently invalid.
    #[error("extraction authority trusted time is discontinuous")]
    TrustedTimeDiscontinuous,
    /// Local-only source metadata denies network access.
    #[error("extraction authority denies network access")]
    NetworkDenied,
    /// The exact target or response violated the metadata-bound network policy.
    #[error("extraction network policy rejected the operation: {0}")]
    NetworkPolicy(#[from] NetworkPolicyError),
    /// A one-use request admission was presented for a different exact target.
    #[error("extraction request admission does not match the exact authorized target")]
    RequestTargetMismatch,
    /// Remote source metadata did not retain a registry-coordinated provider budget.
    #[error("extraction provider budget is not configured")]
    BudgetNotConfigured,
    /// Shared request capacity is unavailable until the inclusive monotonic deadline.
    #[error("extraction provider budget is cooling down")]
    BudgetWaitUntil {
        /// Process-local inclusive retry deadline.
        deadline: MonotonicInstant,
    },
    /// Shared provider-budget state is terminally unavailable.
    #[error("extraction provider budget is unavailable: {reason:?}")]
    BudgetUnavailable {
        /// Exact fail-closed budget reason.
        reason: BudgetUnavailableReason,
    },
}

/// Non-clone request admission retaining current authority and one in-flight budget reservation.
pub struct ExtractionRequestPermit {
    authority: ExtractionAuthority,
    authorization: AuthorizedRequest,
    budget: BudgetReservation,
    redirects_followed: u8,
}

impl std::fmt::Debug for ExtractionRequestPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExtractionRequestPermit")
            .field("authority", &self.authority)
            .field(
                "contains_sensitive_query",
                &self.authorization.contains_sensitive_query(),
            )
            .finish_non_exhaustive()
    }
}

impl ExtractionRequestPermit {
    pub(crate) const fn new(
        authority: ExtractionAuthority,
        authorization: AuthorizedRequest,
        budget: BudgetReservation,
    ) -> Self {
        Self {
            authority,
            authorization,
            budget,
            redirects_followed: 0,
        }
    }

    /// Returns redacted sensitivity metadata for the authorized exact target.
    pub const fn authorization(&self) -> AuthorizedRequest {
        self.authorization
    }

    /// Revalidates currentness and effective time during paged or streamed response handling.
    pub fn validate_current(&self) -> Result<(), ExtractionAuthorityError> {
        self.authority.validate_current()
    }

    /// Returns hardened HTTP client construction requirements bound to the registered metadata.
    pub fn client_profile(&self) -> Result<HttpClientProfile, ExtractionAuthorityError> {
        self.validate_current()?;
        Ok(self.endpoint_policy()?.client_profile())
    }

    /// Returns request deadlines, redirect limits, and response-size bounds.
    pub fn request_bounds(&self) -> Result<HttpRequestBounds, ExtractionAuthorityError> {
        self.validate_current()?;
        Ok(self.endpoint_policy()?.request_bounds())
    }

    /// Consumes this one-use admission for the exact final target immediately before HTTP send.
    pub fn authorize_send(
        self,
        target: &str,
    ) -> Result<InFlightExtractionRequest, ExtractionAuthorityError> {
        self.validate_current()?;
        if !self.authorization.matches_exact_target(target) {
            return Err(ExtractionAuthorityError::RequestTargetMismatch);
        }
        let budget = match self.budget.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => permit,
            BudgetDispatchDecision::WaitUntil(deadline) => {
                return Err(ExtractionAuthorityError::BudgetWaitUntil { deadline });
            }
            BudgetDispatchDecision::Unavailable(reason) => {
                return Err(ExtractionAuthorityError::BudgetUnavailable { reason });
            }
        };
        self.authority.validate_current()?;
        Ok(InFlightExtractionRequest {
            authority: self.authority,
            authorization: self.authorization,
            budget,
            redirects_followed: self.redirects_followed,
        })
    }

    fn endpoint_policy(&self) -> Result<&crate::EndpointPolicy, ExtractionAuthorityError> {
        match self.authority.metadata().network_policy() {
            crate::NetworkAccessPolicy::Allowlisted(policy) => Ok(policy),
            crate::NetworkAccessPolicy::Denied => Err(ExtractionAuthorityError::NetworkDenied),
        }
    }

    /// Cancels before send, releasing concurrency without consuming a request window.
    pub fn release(self) {
        self.budget.release();
    }
}

/// One exact, already-authorized provider request whose in-flight slot spans response handling.
pub struct InFlightExtractionRequest {
    authority: ExtractionAuthority,
    authorization: AuthorizedRequest,
    budget: BudgetPermit,
    redirects_followed: u8,
}

impl std::fmt::Debug for InFlightExtractionRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InFlightExtractionRequest")
            .field("authority", &self.authority)
            .field(
                "contains_sensitive_query",
                &self.authorization.contains_sensitive_query(),
            )
            .finish_non_exhaustive()
    }
}

impl InFlightExtractionRequest {
    /// Revalidates currentness during streamed response handling.
    pub fn validate_current(&self) -> Result<(), ExtractionAuthorityError> {
        self.authority.validate_current()
    }

    /// Enforces the registered response-size ceiling before further buffering.
    pub fn validate_response_size(&self, size: u64) -> Result<(), ExtractionAuthorityError> {
        self.validate_current()?;
        let policy = match self.authority.metadata().network_policy() {
            crate::NetworkAccessPolicy::Allowlisted(policy) => policy,
            crate::NetworkAccessPolicy::Denied => {
                return Err(ExtractionAuthorityError::NetworkDenied);
            }
        };
        policy.validate_response_size(size)?;
        self.validate_current()
    }

    /// Applies one provider HTTP `Retry-After` response to this request's shared allocation.
    ///
    /// Missing or malformed fields use the existing capped refusal backoff. Valid fields retain
    /// their provider-supplied deadline, and instructions beyond configured policy fail closed.
    /// The operation consumes the completed in-flight response so one response cannot apply the
    /// refusal more than once, and releases its concurrency slot on return. No provider-budget
    /// admission capability is exposed to the adapter.
    ///
    /// # Errors
    ///
    /// Fails when this request's extraction authority is stale, the coordinated budget is absent,
    /// persistence or budget state is unavailable, or the refusal terminally violates policy.
    pub fn apply_retry_after_header(
        self,
        field: Option<&[u8]>,
        fallback_jitter_sample_basis_points: u16,
    ) -> Result<MonotonicInstant, ExtractionAuthorityError> {
        self.validate_current()?;
        match self
            .authority
            .apply_retry_after_header(field, fallback_jitter_sample_basis_points)?
        {
            crate::BudgetDecision::WaitUntil(deadline) => Ok(deadline),
            crate::BudgetDecision::Unavailable(reason) => {
                Err(ExtractionAuthorityError::BudgetUnavailable { reason })
            }
            crate::BudgetDecision::Ready(permit) => {
                permit.release();
                Err(ExtractionAuthorityError::BudgetUnavailable {
                    reason: BudgetUnavailableReason::StateCorrupt,
                })
            }
        }
    }

    /// Records one completely handled successful provider response on the shared allocation.
    ///
    /// Callers must invoke this only after the response status, bounds, and payload have all been
    /// validated. The operation consumes the in-flight request so one response cannot reset
    /// refusal escalation more than once, and releases its concurrency slot on return. A success
    /// does not erase a provider-directed cooldown established by another in-flight response.
    ///
    /// # Errors
    ///
    /// Fails when this request's extraction authority is stale, the coordinated budget is absent,
    /// or shared provider-budget state cannot durably record the successful response.
    pub fn record_success(self) -> Result<(), ExtractionAuthorityError> {
        self.authority.record_success()
    }

    /// Completes one redirect response and admits the next exact request hop.
    ///
    /// The current in-flight slot is released before the next hop reserves a distinct shared
    /// provider-budget request. The returned permit retains the same registry authority, exact
    /// target binding, bounded redirect count, and sensitive-header forwarding decision.
    ///
    /// # Errors
    ///
    /// Rejects a mismatched previous target, a denied or cross-origin target, a redirect beyond
    /// the configured chain limit, stale authority, or unavailable budget for the next hop.
    pub fn authorize_redirect_from(
        self,
        previous: &str,
        target: &str,
        carried_sensitive_headers: bool,
    ) -> Result<ExtractionRedirectPermit, ExtractionAuthorityError> {
        self.validate_current()?;
        if !self.authorization.matches_exact_target(previous) {
            return Err(ExtractionAuthorityError::RequestTargetMismatch);
        }
        let policy = match self.authority.metadata().network_policy() {
            crate::NetworkAccessPolicy::Allowlisted(policy) => policy,
            crate::NetworkAccessPolicy::Denied => {
                return Err(ExtractionAuthorityError::NetworkDenied);
            }
        };
        let redirect_count = self.redirects_followed.saturating_add(1);
        let max_redirects = policy.request_bounds().max_redirects();
        if redirect_count > max_redirects {
            return Err(ExtractionAuthorityError::NetworkPolicy(
                NetworkPolicyError::TooManyRedirects {
                    actual: usize::from(redirect_count),
                    max: max_redirects,
                },
            ));
        }
        let redirect =
            policy.authorize_redirect_from(previous, target, carried_sensitive_headers)?;
        let authority = self.authority.clone();
        self.budget.release();
        let mut request = authority.try_network_request(target)?;
        request.redirects_followed = redirect_count;
        Ok(ExtractionRedirectPermit { request, redirect })
    }

    /// Explicitly completes response handling and releases the in-flight slot.
    pub fn release(self) {
        self.budget.release();
    }
}

/// Admitted next hop in a bounded redirect chain with its sensitive-header decision.
pub struct ExtractionRedirectPermit {
    request: ExtractionRequestPermit,
    redirect: RedirectAuthorization,
}

impl std::fmt::Debug for ExtractionRedirectPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExtractionRedirectPermit")
            .field("request", &self.request)
            .field("redirect", &self.redirect)
            .finish_non_exhaustive()
    }
}

impl ExtractionRedirectPermit {
    /// Returns the policy decision for forwarding sensitive headers to this exact next hop.
    pub const fn redirect_authorization(&self) -> RedirectAuthorization {
        self.redirect
    }

    /// Consumes this one-use redirect admission immediately before sending the exact target.
    pub fn authorize_send(
        self,
        target: &str,
    ) -> Result<InFlightExtractionRequest, ExtractionAuthorityError> {
        self.request.authorize_send(target)
    }

    /// Cancels the redirect before send without consuming a request window.
    pub fn release(self) {
        self.request.release();
    }
}

/// Adapter-facing extraction failure preserving transport and contract classes.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ExtractionSourceError {
    /// Source transport/lifecycle failure.
    #[error("source extraction transport failed: {0}")]
    Source(#[from] SourceError),
    /// Bounded extraction contract failure.
    #[error("source extraction contract failed: {0}")]
    Contract(#[from] ExtractionError),
    /// Registry-minted extraction authority rejected or expired.
    #[error("source extraction authority failed: {0}")]
    Authority(#[from] ExtractionAuthorityError),
    /// Request deadline elapsed.
    #[error("source extraction deadline elapsed")]
    DeadlineExceeded,
    /// Cancellation was requested.
    #[error("source extraction was cancelled")]
    Cancelled,
}
