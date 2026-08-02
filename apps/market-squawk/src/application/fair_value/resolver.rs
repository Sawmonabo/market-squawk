//! Bounded producer-receipt authority for fair-value input resolution.

use std::{fmt, mem::size_of, num::NonZeroUsize, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_data::{PinnedFeatureMonetaryValue, PinnedMonetaryValue};
use market_squawk_live::CommittedQualifiedMarketObservation;
use market_squawk_modeling::{ModelAdmissionError, ProductionFeatureRegistry};
use market_squawk_portfolio::PortfolioRevision;
use market_squawk_valuation::{
    CommittedMarketInputRequest, MarketAccess, MarketPriceSelection, ValuationInput,
};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::{
    FairValueInputResolutionError, FairValueInputResolutionRequest, FairValueInputResolver,
    FairValueProducerKind,
};

mod identity;
mod publishers;
#[cfg(test)]
mod tests;

use identity::valid_reference;

const MAXIMUM_RECEIPTS: usize = 65_536;
const MAXIMUM_RETAINED_BYTES: usize = 1024 * 1024 * 1024;
const MAXIMUM_LIVE_RECEIPTS_PER_INPUT: usize = 4_096;

/// Caller-selected process bounds for fair-value producer receipts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FairValueInputAuthorityLimitInput {
    /// Maximum retained opaque receipt selectors.
    pub maximum_receipts: NonZeroUsize,
    /// Maximum conservative retained bytes including preallocated slots.
    pub maximum_retained_bytes: NonZeroUsize,
    /// Maximum committed live observations bound to one selector.
    pub maximum_live_receipts_per_input: NonZeroUsize,
}

/// Validated fixed process bounds for fair-value producer receipts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FairValueInputAuthorityLimits {
    maximum_receipts: usize,
    maximum_retained_bytes: usize,
    maximum_live_receipts_per_input: usize,
}

impl FairValueInputAuthorityLimits {
    /// Returns conservative local defaults below every hard process ceiling.
    #[must_use]
    pub const fn standard() -> Self {
        Self {
            maximum_receipts: 8_192,
            maximum_retained_bytes: 128 * 1024 * 1024,
            maximum_live_receipts_per_input: MAXIMUM_LIVE_RECEIPTS_PER_INPUT,
        }
    }

    /// Validates caller-selected limits against fixed process ceilings.
    pub fn try_new(
        input: FairValueInputAuthorityLimitInput,
    ) -> Result<Self, FairValueInputAuthorityError> {
        let limits = Self {
            maximum_receipts: input.maximum_receipts.get(),
            maximum_retained_bytes: input.maximum_retained_bytes.get(),
            maximum_live_receipts_per_input: input.maximum_live_receipts_per_input.get(),
        };
        if limits.maximum_receipts > MAXIMUM_RECEIPTS
            || limits.maximum_retained_bytes > MAXIMUM_RETAINED_BYTES
            || limits.maximum_live_receipts_per_input > MAXIMUM_LIVE_RECEIPTS_PER_INPUT
        {
            return Err(FairValueInputAuthorityError::InvalidLimits);
        }
        Ok(limits)
    }
}

/// Opaque application-minted selector for one immutable producer receipt.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct FairValueReceiptReference(Box<str>);

impl FairValueReceiptReference {
    /// Returns the bounded selector accepted by `FairValue.Measure`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for FairValueReceiptReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FairValueReceiptReference([OPAQUE PRODUCER RECEIPT])")
    }
}

/// Result of publishing one genuine producer receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FairValueReceiptRegistration {
    reference: FairValueReceiptReference,
    replay: bool,
}

impl FairValueReceiptRegistration {
    /// Returns the immutable receipt selector.
    pub const fn reference(&self) -> &FairValueReceiptReference {
        &self.reference
    }

    /// Returns whether an identical receipt was already retained.
    pub const fn is_replay(&self) -> bool {
        self.replay
    }
}

/// Fair-value receipt publication or startup failure.
#[derive(Debug, Error)]
pub enum FairValueInputAuthorityError {
    /// A configured count or byte ceiling is zero, inconsistent, or above the hard limit.
    #[error("fair-value input authority limits are invalid")]
    InvalidLimits,
    /// Fixed or per-receipt memory could not be fallibly reserved.
    #[error("fair-value input authority memory allocation failed")]
    Allocation,
    /// Checked retained-byte accounting overflowed.
    #[error("fair-value input authority retained-byte accounting overflowed")]
    RetainedSizeOverflow,
    /// A producer receipt or its bounded selection is invalid.
    #[error("fair-value producer receipt is invalid")]
    InvalidReceipt,
    /// A selector collision attempted to bind different immutable producer evidence.
    #[error("fair-value producer receipt conflicts with retained authority")]
    ReceiptConflict,
    /// The configured count or retained-byte ceiling was reached.
    #[error("fair-value input authority resource limit was exceeded")]
    ResourceExhausted,
    /// Publication was cancelled.
    #[error("fair-value input publication was cancelled")]
    Cancelled,
    /// Publication exceeded its absolute deadline.
    #[error("fair-value input publication deadline elapsed")]
    DeadlineExceeded,
    /// The code-owned production feature registry could not be constructed.
    #[error("fair-value feature registry is unavailable: {0}")]
    FeatureRegistry(#[from] ModelAdmissionError),
}

struct SharedAuthority {
    limits: FairValueInputAuthorityLimits,
    state: Mutex<AuthorityState>,
}

struct AuthorityState {
    receipts: Vec<StoredReceipt>,
    retained_bytes: usize,
}

struct StoredReceipt {
    reference: FairValueReceiptReference,
    producer: ProducerReceipt,
    retained_bytes: usize,
}

// Inline variants keep every preallocated receipt slot inside the authority's fixed byte budget;
// boxing the portfolio revision would introduce a separate allocation and accounting path.
#[allow(clippy::large_enum_variant)]
enum ProducerReceipt {
    Live {
        observations: Vec<CommittedQualifiedMarketObservation>,
        selected_index: usize,
        selection: MarketPriceSelection,
    },
    Research(PinnedMonetaryValue),
    Analytics(PinnedFeatureMonetaryValue),
    Portfolio(PortfolioRevision),
}

impl ProducerReceipt {
    const fn kind(&self) -> FairValueProducerKind {
        match self {
            Self::Live { .. } => FairValueProducerKind::Live,
            Self::Research(_) => FairValueProducerKind::Research,
            Self::Analytics(_) => FairValueProducerKind::Analytics,
            Self::Portfolio(_) => FairValueProducerKind::Portfolio,
        }
    }

    fn same_receipt(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Live {
                    observations: left,
                    selected_index: left_index,
                    selection: left_selection,
                },
                Self::Live {
                    observations: right,
                    selected_index: right_index,
                    selection: right_selection,
                },
            ) => left == right && left_index == right_index && left_selection == right_selection,
            (Self::Research(left), Self::Research(right)) => left == right,
            (Self::Analytics(left), Self::Analytics(right)) => left == right,
            (Self::Portfolio(left), Self::Portfolio(right)) => left == right,
            _ => false,
        }
    }
}

/// Complete process-local producer authority and its separated publication handles.
pub struct ProductionFairValueInputAuthority {
    resolver: Arc<ProductionFairValueInputResolver>,
}

impl ProductionFairValueInputAuthority {
    /// Starts one empty, fail-closed authority with preallocated count capacity.
    ///
    /// No persisted record, DTO, path, SQL text, or caller value can restore receipt authority.
    /// Producers must republish genuine receipts after every process start.
    pub fn try_new(
        limits: FairValueInputAuthorityLimits,
    ) -> Result<Self, FairValueInputAuthorityError> {
        if limits.maximum_receipts == 0
            || limits.maximum_retained_bytes == 0
            || limits.maximum_live_receipts_per_input == 0
            || limits.maximum_receipts > MAXIMUM_RECEIPTS
            || limits.maximum_retained_bytes > MAXIMUM_RETAINED_BYTES
            || limits.maximum_live_receipts_per_input > MAXIMUM_LIVE_RECEIPTS_PER_INPUT
        {
            return Err(FairValueInputAuthorityError::InvalidLimits);
        }
        let mut receipts = Vec::new();
        receipts
            .try_reserve_exact(limits.maximum_receipts)
            .map_err(|_| FairValueInputAuthorityError::Allocation)?;
        let retained_bytes = size_of::<AuthorityState>()
            .checked_add(
                receipts
                    .capacity()
                    .checked_mul(size_of::<StoredReceipt>())
                    .ok_or(FairValueInputAuthorityError::RetainedSizeOverflow)?,
            )
            .ok_or(FairValueInputAuthorityError::RetainedSizeOverflow)?;
        if retained_bytes > limits.maximum_retained_bytes {
            return Err(FairValueInputAuthorityError::InvalidLimits);
        }
        let shared = Arc::new(SharedAuthority {
            limits,
            state: Mutex::new(AuthorityState {
                receipts,
                retained_bytes,
            }),
        });
        Ok(Self {
            resolver: Arc::new(ProductionFairValueInputResolver {
                shared,
                feature_registry: ProductionFeatureRegistry::try_new()?,
            }),
        })
    }

    /// Returns the read-only resolver passed directly to [`super::FairValueDomainService`].
    pub fn resolver(&self) -> Arc<ProductionFairValueInputResolver> {
        Arc::clone(&self.resolver)
    }

    /// Returns publication authority accepting only committed live-export leases.
    pub fn live_publisher(&self) -> LiveFairValueInputPublisher {
        LiveFairValueInputPublisher {
            shared: Arc::clone(&self.resolver.shared),
        }
    }

    /// Returns publication authority accepting only producer-issued research receipts.
    pub fn research_publisher(&self) -> ResearchFairValueInputPublisher {
        ResearchFairValueInputPublisher {
            shared: Arc::clone(&self.resolver.shared),
        }
    }

    /// Returns publication authority accepting only registered feature receipts.
    pub fn analytics_publisher(&self) -> AnalyticsFairValueInputPublisher {
        AnalyticsFairValueInputPublisher {
            shared: Arc::clone(&self.resolver.shared),
        }
    }

    /// Returns publication authority accepting only immutable portfolio revisions.
    pub fn portfolio_publisher(&self) -> PortfolioFairValueInputPublisher {
        PortfolioFairValueInputPublisher {
            shared: Arc::clone(&self.resolver.shared),
        }
    }
}

impl fmt::Debug for ProductionFairValueInputAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductionFairValueInputAuthority")
            .field("limits", &self.resolver.shared.limits)
            .field("receipts", &"[BOUNDED PRODUCER AUTHORITY]")
            .finish()
    }
}

/// Read-only production resolver over genuine retained producer receipts.
pub struct ProductionFairValueInputResolver {
    shared: Arc<SharedAuthority>,
    feature_registry: ProductionFeatureRegistry,
}

impl fmt::Debug for ProductionFairValueInputResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductionFairValueInputResolver")
            .field("limits", &self.shared.limits)
            .field("feature_registry", &"[CODE-OWNED READ AUTHORITY]")
            .finish()
    }
}

/// Publisher restricted to genuine live-export leases.
#[derive(Clone)]
pub struct LiveFairValueInputPublisher {
    shared: Arc<SharedAuthority>,
}

/// Publisher restricted to producer-issued canonical research monetary receipts.
#[derive(Clone)]
pub struct ResearchFairValueInputPublisher {
    shared: Arc<SharedAuthority>,
}

/// Publisher restricted to producer-issued registered monetary feature receipts.
#[derive(Clone)]
pub struct AnalyticsFairValueInputPublisher {
    shared: Arc<SharedAuthority>,
}

/// Publisher restricted to genuine immutable portfolio revisions.
#[derive(Clone)]
pub struct PortfolioFairValueInputPublisher {
    shared: Arc<SharedAuthority>,
}

macro_rules! publisher_debug {
    ($type:ty) => {
        impl fmt::Debug for $type {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($type))
                    .field("authority", &"[PRODUCER-SPECIFIC PUBLISHER]")
                    .finish()
            }
        }
    };
}

publisher_debug!(LiveFairValueInputPublisher);
publisher_debug!(ResearchFairValueInputPublisher);
publisher_debug!(AnalyticsFairValueInputPublisher);
publisher_debug!(PortfolioFairValueInputPublisher);

#[async_trait]
impl FairValueInputResolver for ProductionFairValueInputResolver {
    async fn resolve(
        &self,
        request: FairValueInputResolutionRequest,
    ) -> Result<ValuationInput, FairValueInputResolutionError> {
        ensure_resolution_live(&request)?;
        if !valid_reference(request.producer(), request.receipt_id()) {
            return Err(FairValueInputResolutionError::InvalidReference);
        }
        let deadline = tokio::time::Instant::from_std(request.deadline());
        let state = tokio::select! {
            biased;
            _ = request.cancellation().cancelled() => {
                return Err(FairValueInputResolutionError::Cancelled);
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(FairValueInputResolutionError::DeadlineExceeded);
            }
            state = self.shared.state.lock() => state,
        };
        ensure_resolution_live(&request)?;
        let index = state
            .receipts
            .binary_search_by(|entry| entry.reference.as_str().cmp(request.receipt_id()))
            .map_err(|_| FairValueInputResolutionError::NotFound)?;
        let entry = state
            .receipts
            .get(index)
            .ok_or(FairValueInputResolutionError::Internal)?;
        if entry.producer.kind() != request.producer() {
            return Err(FairValueInputResolutionError::InvalidReference);
        }
        if entry.producer.kind() != FairValueProducerKind::Live
            && request.market_access_assessment().is_some()
        {
            return Err(FairValueInputResolutionError::InvalidReference);
        }
        let resolved = resolve_entry(
            &entry.producer,
            &request,
            self.feature_registry.feature_registry(),
        )?;
        drop(state);
        ensure_resolution_live(&request)?;
        Ok(resolved)
    }
}

async fn register(
    shared: &SharedAuthority,
    receipt: StoredReceipt,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<FairValueReceiptRegistration, FairValueInputAuthorityError> {
    ensure_publication_live(deadline, cancellation)?;
    let deadline_at = tokio::time::Instant::from_std(deadline);
    let mut state = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(FairValueInputAuthorityError::Cancelled),
        _ = tokio::time::sleep_until(deadline_at) => {
            return Err(FairValueInputAuthorityError::DeadlineExceeded);
        }
        state = shared.state.lock() => state,
    };
    ensure_publication_live(deadline, cancellation)?;
    match state
        .receipts
        .binary_search_by(|entry| entry.reference.cmp(&receipt.reference))
    {
        Ok(index) => {
            let retained = state
                .receipts
                .get(index)
                .ok_or(FairValueInputAuthorityError::ReceiptConflict)?;
            if !retained.producer.same_receipt(&receipt.producer) {
                return Err(FairValueInputAuthorityError::ReceiptConflict);
            }
            Ok(FairValueReceiptRegistration {
                reference: retained.reference.clone(),
                replay: true,
            })
        }
        Err(index) => {
            if state.receipts.len() == shared.limits.maximum_receipts {
                return Err(FairValueInputAuthorityError::ResourceExhausted);
            }
            let new_retained = state
                .retained_bytes
                .checked_add(receipt.retained_bytes)
                .ok_or(FairValueInputAuthorityError::RetainedSizeOverflow)?;
            if new_retained > shared.limits.maximum_retained_bytes {
                return Err(FairValueInputAuthorityError::ResourceExhausted);
            }
            let reference = receipt.reference.clone();
            state.receipts.insert(index, receipt);
            state.retained_bytes = new_retained;
            Ok(FairValueReceiptRegistration {
                reference,
                replay: false,
            })
        }
    }
}

fn resolve_entry(
    receipt: &ProducerReceipt,
    request: &FairValueInputResolutionRequest,
    feature_registry: &market_squawk_analytics::FeatureRegistry,
) -> Result<ValuationInput, FairValueInputResolutionError> {
    let result = match receipt {
        ProducerReceipt::Live {
            observations,
            selected_index,
            selection,
        } => {
            let market_access = request
                .market_access_assessment()
                .ok_or(FairValueInputResolutionError::Unauthorized)?;
            if market_access.conclusion() != MarketAccess::Accessible {
                return Err(FairValueInputResolutionError::Unauthorized);
            }
            if observations.len()
                > request
                    .ruleset()
                    .market_activity_policy()
                    .maximum_receipts()
                || observations.iter().any(|observation| {
                    observation.instrument_id() != request.instrument_id()
                        || observation.received_at() > request.measurement_at()
                        || observation.available_at() > request.measurement_at()
                        || observation.ingested_at() > request.measurement_at()
                        || observation.qualification_evaluated_at() > request.measurement_at()
                        || observation.qualification_valid_until() < request.measurement_at()
                        || observation
                            .source_timestamp()
                            .is_some_and(|value| value > request.measurement_at())
                })
            {
                return Err(FairValueInputResolutionError::InvalidReference);
            }
            ValuationInput::from_committed_market(CommittedMarketInputRequest {
                receipts: observations,
                selected_index: *selected_index,
                selection: *selection,
                significance: request.significance(),
                account_id: request.account_id(),
                measurement_at: request.measurement_at(),
                ruleset: request.ruleset(),
                market_access_assessment: Some(market_access),
            })
        }
        ProducerReceipt::Research(value) => {
            if value.instrument_id() != Some(request.instrument_id())
                || value.ingested_at() > request.measurement_at()
                || value.received_at() > request.measurement_at()
                || value.available_at().unwrap_or(value.ingested_at()) > request.measurement_at()
                || value
                    .published_at()
                    .is_some_and(|time| time > request.measurement_at())
                || value
                    .effective_at()
                    .is_some_and(|time| time > request.measurement_at())
                || value
                    .source_timestamp()
                    .is_some_and(|time| time > request.measurement_at())
            {
                return Err(FairValueInputResolutionError::InvalidReference);
            }
            ValuationInput::from_research(value, request.significance())
        }
        ProducerReceipt::Analytics(value) => {
            if value.instrument_id() != request.instrument_id()
                || value.cutoff_at() > request.measurement_at()
            {
                return Err(FairValueInputResolutionError::InvalidReference);
            }
            ValuationInput::from_analytics(value, feature_registry, request.significance())
        }
        ProducerReceipt::Portfolio(revision) => {
            if revision.account_id() != request.account_id() {
                return Err(FairValueInputResolutionError::Unauthorized);
            }
            if revision.evidence().as_of() > request.measurement_at()
                || revision.position(request.instrument_id()).is_none()
            {
                return Err(FairValueInputResolutionError::InvalidReference);
            }
            ValuationInput::from_portfolio_position(
                revision,
                request.instrument_id(),
                request.significance(),
            )
        }
    };
    result.map_err(|_| FairValueInputResolutionError::InvalidReference)
}

fn ensure_publication_live(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), FairValueInputAuthorityError> {
    if cancellation.is_cancelled() {
        Err(FairValueInputAuthorityError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(FairValueInputAuthorityError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn ensure_resolution_live(
    request: &FairValueInputResolutionRequest,
) -> Result<(), FairValueInputResolutionError> {
    if request.cancellation().is_cancelled() {
        Err(FairValueInputResolutionError::Cancelled)
    } else if Instant::now() >= request.deadline() {
        Err(FairValueInputResolutionError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
