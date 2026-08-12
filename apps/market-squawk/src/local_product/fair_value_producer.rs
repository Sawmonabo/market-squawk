//! Genuine local producer selection for one in-process fair-value measurement.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use market_squawk_data::{AnalyticalReadCapability, AnalyticalReadError, QueryError, QueryLimits};
use market_squawk_portfolio::PortfolioRevision;

use crate::PortfolioFairValueReadCapability;
use crate::application::{
    AnalyticsFairValueInputPublisher, FairValueInputAuthorityError, FairValueProducerSelection,
    FairValueProducerSelectionAuthority, FairValueProducerSelectionError,
    FairValueProducerSelectionRequest, FairValueReceiptRegistration, LiveFairValueInputPublisher,
    LiveFairValueObservationBuffer, LiveFairValueObservationBufferError,
    PortfolioFairValueInputPublisher, ResearchFairValueInputPublisher,
};

const QUERY_MAXIMUM_BYTES: u64 = 64 * 1024;
const QUERY_MAXIMUM_MEMORY_BYTES: u64 = 8 * 1024 * 1024;
const QUERY_MAXIMUM_PARTITIONS: usize = 1;
const QUERY_MAXIMUM_AST_NODES: usize = 256;
const QUERY_MAXIMUM_PLAN_NODES: usize = 512;
const QUERY_MAXIMUM_DURATION: Duration = Duration::from_secs(60);

/// Production selector over read-only genuine producers and separated receipt publishers.
pub(super) struct ProductionFairValueProducerSelectionAuthority {
    analytical: AnalyticalReadCapability,
    portfolio: PortfolioFairValueReadCapability,
    live: Arc<LiveFairValueObservationBuffer>,
    live_publisher: LiveFairValueInputPublisher,
    research_publisher: ResearchFairValueInputPublisher,
    analytics_publisher: AnalyticsFairValueInputPublisher,
    portfolio_publisher: PortfolioFairValueInputPublisher,
}

impl ProductionFairValueProducerSelectionAuthority {
    /// Binds only the capabilities needed to read and publish genuine immutable evidence.
    pub(super) const fn new(
        analytical: AnalyticalReadCapability,
        portfolio: PortfolioFairValueReadCapability,
        live: Arc<LiveFairValueObservationBuffer>,
        live_publisher: LiveFairValueInputPublisher,
        research_publisher: ResearchFairValueInputPublisher,
        analytics_publisher: AnalyticsFairValueInputPublisher,
        portfolio_publisher: PortfolioFairValueInputPublisher,
    ) -> Self {
        Self {
            analytical,
            portfolio,
            live,
            live_publisher,
            research_publisher,
            analytics_publisher,
            portfolio_publisher,
        }
    }

    async fn publish_live(
        &self,
        request: &FairValueProducerSelectionRequest,
        venue_id: &market_squawk_domain::VenueId,
        selection: market_squawk_valuation::MarketPriceSelection,
    ) -> Result<FairValueReceiptRegistration, FairValueProducerSelectionError> {
        let lease = self
            .live
            .take(
                venue_id.clone(),
                request.instrument_id(),
                selection,
                request.deadline(),
                request.cancellation(),
            )
            .await
            .map_err(map_live_buffer_error)?;
        let mut leases = Vec::new();
        leases
            .try_reserve_exact(1)
            .map_err(|_| FairValueProducerSelectionError::ResourceExhausted)?;
        leases.push(lease);
        self.live_publisher
            .publish(
                leases,
                0,
                selection,
                request.deadline(),
                request.cancellation(),
            )
            .await
            .map_err(map_publication_error)
    }

    async fn publish_research(
        &self,
        request: &FairValueProducerSelectionRequest,
        dataset_id: &market_squawk_data::DatasetId,
        row: usize,
    ) -> Result<FairValueReceiptRegistration, FairValueProducerSelectionError> {
        let generation = self
            .analytical
            .latest(dataset_id, request.deadline(), request.cancellation())
            .map_err(map_analytical_error)?
            .ok_or(FairValueProducerSelectionError::NotFound)?;
        let value = self
            .analytical
            .research_monetary_value(
                generation.manifest(),
                row,
                query_limits(request.deadline())?,
                request.deadline(),
                request.cancellation().clone(),
            )
            .await
            .map_err(map_analytical_error)?;
        if value.instrument_id() != Some(request.instrument_id()) {
            return Err(FairValueProducerSelectionError::Unauthorized);
        }
        self.research_publisher
            .publish(value, request.deadline(), request.cancellation())
            .await
            .map_err(map_publication_error)
    }

    async fn publish_analytics(
        &self,
        request: &FairValueProducerSelectionRequest,
        dataset_id: &market_squawk_data::DatasetId,
        row: usize,
    ) -> Result<FairValueReceiptRegistration, FairValueProducerSelectionError> {
        let generation = self
            .analytical
            .latest(dataset_id, request.deadline(), request.cancellation())
            .map_err(map_analytical_error)?
            .ok_or(FairValueProducerSelectionError::NotFound)?;
        let value = self
            .analytical
            .feature_monetary_value(
                generation.manifest(),
                row,
                query_limits(request.deadline())?,
                request.deadline(),
                request.cancellation().clone(),
            )
            .await
            .map_err(map_analytical_error)?;
        if value.instrument_id() != request.instrument_id() {
            return Err(FairValueProducerSelectionError::Unauthorized);
        }
        self.analytics_publisher
            .publish(value, request.deadline(), request.cancellation())
            .await
            .map_err(map_publication_error)
    }

    async fn publish_portfolio(
        &self,
        request: &FairValueProducerSelectionRequest,
    ) -> Result<FairValueReceiptRegistration, FairValueProducerSelectionError> {
        let revision = self
            .portfolio
            .current_revision(
                request.account_id(),
                request.deadline(),
                request.cancellation(),
            )
            .map_err(map_portfolio_error)?;
        ensure_position(&revision, request)?;
        self.portfolio_publisher
            .publish(revision, request.deadline(), request.cancellation())
            .await
            .map_err(map_publication_error)
    }
}

#[async_trait]
impl FairValueProducerSelectionAuthority for ProductionFairValueProducerSelectionAuthority {
    async fn publish(
        &self,
        request: FairValueProducerSelectionRequest,
    ) -> Result<FairValueReceiptRegistration, FairValueProducerSelectionError> {
        ensure_live(&request)?;
        let result = match request.selection() {
            FairValueProducerSelection::Live {
                venue_id,
                selection,
                ..
            } => self.publish_live(&request, venue_id, *selection).await,
            FairValueProducerSelection::Research {
                dataset_id, row, ..
            } => self.publish_research(&request, dataset_id, *row).await,
            FairValueProducerSelection::Analytics {
                dataset_id, row, ..
            } => self.publish_analytics(&request, dataset_id, *row).await,
            FairValueProducerSelection::Portfolio { .. } => self.publish_portfolio(&request).await,
        }?;
        ensure_live(&request)?;
        Ok(result)
    }
}

impl std::fmt::Debug for ProductionFairValueProducerSelectionAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductionFairValueProducerSelectionAuthority")
            .field("analytical", &self.analytical)
            .field("portfolio", &self.portfolio)
            .field("live", &self.live)
            .field("publishers", &"[SEPARATED GENUINE-PRODUCER AUTHORITY]")
            .finish()
    }
}

fn ensure_position(
    revision: &PortfolioRevision,
    request: &FairValueProducerSelectionRequest,
) -> Result<(), FairValueProducerSelectionError> {
    if revision.account_id() != request.account_id()
        || revision.position(request.instrument_id()).is_none()
    {
        Err(FairValueProducerSelectionError::Unauthorized)
    } else {
        Ok(())
    }
}

fn query_limits(deadline: Instant) -> Result<QueryLimits, FairValueProducerSelectionError> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(FairValueProducerSelectionError::DeadlineExceeded)?
        .min(QUERY_MAXIMUM_DURATION);
    if remaining.is_zero() {
        return Err(FairValueProducerSelectionError::DeadlineExceeded);
    }
    QueryLimits::try_new(
        1,
        QUERY_MAXIMUM_BYTES,
        QUERY_MAXIMUM_MEMORY_BYTES,
        QUERY_MAXIMUM_PARTITIONS,
        QUERY_MAXIMUM_AST_NODES,
        QUERY_MAXIMUM_PLAN_NODES,
        remaining,
    )
    .map_err(|_| FairValueProducerSelectionError::Internal)
}

fn ensure_live(
    request: &FairValueProducerSelectionRequest,
) -> Result<(), FairValueProducerSelectionError> {
    if request.cancellation().is_cancelled() {
        Err(FairValueProducerSelectionError::Cancelled)
    } else if Instant::now() >= request.deadline() {
        Err(FairValueProducerSelectionError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_analytical_error(error: AnalyticalReadError) -> FairValueProducerSelectionError {
    match error {
        AnalyticalReadError::Query(QueryError::Cancelled) => {
            FairValueProducerSelectionError::Cancelled
        }
        AnalyticalReadError::Query(QueryError::DeadlineExceeded) => {
            FairValueProducerSelectionError::DeadlineExceeded
        }
        AnalyticalReadError::Query(
            QueryError::AstLimitExceeded
            | QueryError::PlanLimitExceeded
            | QueryError::PartitionLimitExceeded
            | QueryError::RowLimitExceeded { .. }
            | QueryError::ByteLimitExceeded { .. }
            | QueryError::MemoryLimitExceeded { .. }
            | QueryError::BlockingTaskLimitExceeded
            | QueryError::ReaderMemoryBoundExceeded,
        ) => FairValueProducerSelectionError::ResourceExhausted,
        AnalyticalReadError::Query(
            QueryError::PinnedQuerySourceRequired
            | QueryError::MonetaryValueRequiresInlineResult
            | QueryError::MonetaryCellOutOfBounds
            | QueryError::InvalidMonetaryCell
            | QueryError::UnsupportedMonetaryScale
            | QueryError::InvalidSource
            | QueryError::ManifestPinMismatch,
        )
        | AnalyticalReadError::InvalidMarketBarLimit
        | AnalyticalReadError::InvalidMarketBarEffectiveRange
        | AnalyticalReadError::InvalidFundNavLimit
        | AnalyticalReadError::InvalidFundNavDateRange
        | AnalyticalReadError::InvalidMacroSeriesAllowlist
        | AnalyticalReadError::MacroSnapshotSourceOwnerMismatch
        | AnalyticalReadError::InvalidOutcomeMarketBarWindow
        | AnalyticalReadError::InvalidObservationSchema => {
            FairValueProducerSelectionError::InvalidSelection
        }
        AnalyticalReadError::Manifest(_)
        | AnalyticalReadError::ForecastDatasetUnavailable
        | AnalyticalReadError::Parquet(_)
        | AnalyticalReadError::PythonDataset(_)
        | AnalyticalReadError::InvalidLimit
        | AnalyticalReadError::InstrumentLimitExceeded
        | AnalyticalReadError::InvalidKnowledgeRange
        | AnalyticalReadError::MarketBarResultRequiresInline
        | AnalyticalReadError::InvalidMarketBarResult
        | AnalyticalReadError::FundNavResultRequiresInline
        | AnalyticalReadError::InvalidFundNavResult
        | AnalyticalReadError::MacroSnapshotResultRequiresInline
        | AnalyticalReadError::MacroSnapshotCandidateSetSaturated
        | AnalyticalReadError::MacroSnapshotRevisionConflict
        | AnalyticalReadError::MacroSnapshotIncomplete
        | AnalyticalReadError::InvalidMacroSnapshotResult
        | AnalyticalReadError::Query(_) => FairValueProducerSelectionError::Internal,
    }
}

fn map_portfolio_error(
    error: crate::PortfolioApplicationServiceError,
) -> FairValueProducerSelectionError {
    use crate::PortfolioApplicationServiceError as Error;
    match error {
        Error::InvalidLimits | Error::InvalidRequest | Error::Import => {
            FairValueProducerSelectionError::InvalidSelection
        }
        Error::NotFound => FairValueProducerSelectionError::NotFound,
        Error::ResourceExhausted => FairValueProducerSelectionError::ResourceExhausted,
        Error::Cancelled => FairValueProducerSelectionError::Cancelled,
        Error::DeadlineExceeded => FairValueProducerSelectionError::DeadlineExceeded,
        Error::Path
        | Error::Authority
        | Error::SnapshotUnavailable
        | Error::StateChanged
        | Error::RestoreTargetNotFresh => FairValueProducerSelectionError::Unavailable,
        Error::CorruptPublication | Error::Publication | Error::Analytics => {
            FairValueProducerSelectionError::Internal
        }
    }
}

fn map_publication_error(error: FairValueInputAuthorityError) -> FairValueProducerSelectionError {
    match error {
        FairValueInputAuthorityError::InvalidReceipt
        | FairValueInputAuthorityError::ReceiptConflict => {
            FairValueProducerSelectionError::InvalidSelection
        }
        FairValueInputAuthorityError::ResourceExhausted => {
            FairValueProducerSelectionError::ResourceExhausted
        }
        FairValueInputAuthorityError::Cancelled => FairValueProducerSelectionError::Cancelled,
        FairValueInputAuthorityError::DeadlineExceeded => {
            FairValueProducerSelectionError::DeadlineExceeded
        }
        FairValueInputAuthorityError::InvalidLimits
        | FairValueInputAuthorityError::Allocation
        | FairValueInputAuthorityError::RetainedSizeOverflow
        | FairValueInputAuthorityError::FeatureRegistry(_) => {
            FairValueProducerSelectionError::Internal
        }
    }
}

fn map_live_buffer_error(
    error: LiveFairValueObservationBufferError,
) -> FairValueProducerSelectionError {
    match error {
        LiveFairValueObservationBufferError::NotFound => FairValueProducerSelectionError::NotFound,
        LiveFairValueObservationBufferError::AmbiguousSource => {
            FairValueProducerSelectionError::InvalidSelection
        }
        LiveFairValueObservationBufferError::ResourceExhausted
        | LiveFairValueObservationBufferError::Allocation => {
            FairValueProducerSelectionError::ResourceExhausted
        }
        LiveFairValueObservationBufferError::Cancelled => {
            FairValueProducerSelectionError::Cancelled
        }
        LiveFairValueObservationBufferError::DeadlineExceeded => {
            FairValueProducerSelectionError::DeadlineExceeded
        }
        LiveFairValueObservationBufferError::NonMonotonicObservation
        | LiveFairValueObservationBufferError::DrainFailed => {
            FairValueProducerSelectionError::Unavailable
        }
        LiveFairValueObservationBufferError::InvalidCapacity
        | LiveFairValueObservationBufferError::InvalidExportConfiguration => {
            FairValueProducerSelectionError::Internal
        }
    }
}
