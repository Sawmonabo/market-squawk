//! Durable-authority boundary for imported-portfolio candidate impact.

use std::{fmt, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_data::{InstrumentDefinitionReadCapability, MarketDataInstrumentReadCapability};
use market_squawk_domain::{InstrumentId, Timestamp};
use market_squawk_services::ServiceError;
use tokio_util::sync::CancellationToken;

use crate::{
    application::{
        market_runtime::MarketRuntimeRegistry,
        recommendation::{RecommendationSetupAuthority, RecommendationSetupError},
    },
    portfolio_application::{
        PortfolioAccountCatalogError, PortfolioAccountCatalogReadCapability,
        PortfolioAnalysisMarketSet, PortfolioAnalysisSetupResolution,
        PortfolioAnalysisSetupSnapshot, PortfolioApplicationServiceError,
        PortfolioCandidateResolution, PortfolioCandidateResolutionAuthority,
    },
};

/// Factory retained by composition until a durable point-in-time market read is supplied.
///
/// The existing constructor signature remains stable for the installed application composition,
/// but hot runtime, current-definition, and market-data-definition inputs are deliberately not
/// retained. They can authorize display only and therefore cannot construct investment evidence.
#[derive(Clone)]
pub(in crate::application::paper) struct ProductionPortfolioCandidateResolutionFactory {
    maximum_mark_age_nanos: u64,
}

impl ProductionPortfolioCandidateResolutionFactory {
    pub(in crate::application::paper) fn try_new(
        _registry: Arc<MarketRuntimeRegistry>,
        _instrument_definitions: InstrumentDefinitionReadCapability,
        _market_data_instruments: MarketDataInstrumentReadCapability,
        maximum_mark_age_nanos: u64,
    ) -> Result<Self, ServiceError> {
        if maximum_mark_age_nanos == 0 || maximum_mark_age_nanos > i64::MAX as u64 {
            return Err(ServiceError::Internal);
        }
        Ok(Self {
            maximum_mark_age_nanos,
        })
    }

    /// Binds only durable setup and immutable imported-portfolio read capabilities.
    pub(in crate::application::paper) fn bind(
        &self,
        setup: Arc<RecommendationSetupAuthority>,
        catalog: PortfolioAccountCatalogReadCapability,
    ) -> Arc<dyn PortfolioCandidateResolutionAuthority> {
        Arc::new(ProductionPortfolioCandidateResolutionAuthority { setup, catalog })
    }
}

impl fmt::Debug for ProductionPortfolioCandidateResolutionFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductionPortfolioCandidateResolutionFactory")
            .field("market", &"[DURABLE PIT MARKET AUTHORITY UNAVAILABLE]")
            .field("maximum_mark_age_nanos", &self.maximum_mark_age_nanos)
            .finish()
    }
}

struct ProductionPortfolioCandidateResolutionAuthority {
    setup: Arc<RecommendationSetupAuthority>,
    catalog: PortfolioAccountCatalogReadCapability,
}

#[async_trait]
impl PortfolioCandidateResolutionAuthority for ProductionPortfolioCandidateResolutionAuthority {
    async fn resolve(
        &self,
        _instrument_id: InstrumentId,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PortfolioCandidateResolution, PortfolioApplicationServiceError> {
        ensure_before(as_of, deadline, &cancellation)?;
        match self
            .resolve_analysis_setup(as_of, deadline, cancellation.clone())
            .await?
        {
            PortfolioAnalysisSetupResolution::Ready(_) => {}
            PortfolioAnalysisSetupResolution::SetupRequired { .. } => {
                return Err(PortfolioApplicationServiceError::InvalidRequest);
            }
        }
        ensure_before(as_of, deadline, &cancellation)?;
        Err(PortfolioApplicationServiceError::Authority)
    }

    async fn recheck(
        &self,
        expected: &PortfolioCandidateResolution,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), PortfolioApplicationServiceError> {
        ensure_before(as_of, deadline, &cancellation)?;
        if expected.setup().as_of() != as_of {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        self.recheck_setup(expected, as_of, deadline, &cancellation)?;
        ensure_before(as_of, deadline, &cancellation)?;
        Err(PortfolioApplicationServiceError::Authority)
    }

    async fn resolve_analysis_setup(
        &self,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PortfolioAnalysisSetupResolution, PortfolioApplicationServiceError> {
        ensure_before(as_of, deadline, &cancellation)?;
        let catalog = self
            .catalog
            .snapshot_current(deadline, &cancellation)
            .map_err(map_catalog_error)?;
        let resolution = self
            .setup
            .resolve(&catalog, as_of)
            .map_err(map_setup_error)?;
        let resolution =
            PortfolioAnalysisSetupResolution::try_from_resolution(resolution, catalog)?;
        ensure_before(as_of, deadline, &cancellation)?;
        Ok(resolution)
    }

    async fn resolve_analysis_markets(
        &self,
        setup: &PortfolioAnalysisSetupSnapshot,
        instrument_ids: &[InstrumentId],
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PortfolioAnalysisMarketSet, PortfolioApplicationServiceError> {
        ensure_before(as_of, deadline, &cancellation)?;
        if setup.setup().as_of() != as_of
            || instrument_ids.is_empty()
            || instrument_ids.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        self.catalog
            .recheck(setup.catalog(), deadline, &cancellation)
            .map_err(map_catalog_error)?;
        self.setup
            .recheck(setup.setup(), setup.catalog(), as_of)
            .map_err(map_setup_error)?;
        ensure_before(as_of, deadline, &cancellation)?;
        Err(PortfolioApplicationServiceError::Authority)
    }

    async fn recheck_analysis_markets(
        &self,
        expected: &PortfolioAnalysisMarketSet,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), PortfolioApplicationServiceError> {
        ensure_before(as_of, deadline, &cancellation)?;
        if expected.evaluated_at() != as_of {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        self.catalog
            .recheck(expected.setup().catalog(), deadline, &cancellation)
            .map_err(map_catalog_error)?;
        self.setup
            .recheck(expected.setup().setup(), expected.setup().catalog(), as_of)
            .map_err(map_setup_error)?;
        ensure_before(as_of, deadline, &cancellation)?;
        Err(PortfolioApplicationServiceError::Authority)
    }
}

impl ProductionPortfolioCandidateResolutionAuthority {
    fn recheck_setup(
        &self,
        expected: &PortfolioCandidateResolution,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), PortfolioApplicationServiceError> {
        self.catalog
            .recheck(expected.catalog(), deadline, cancellation)
            .map_err(map_catalog_error)?;
        self.setup
            .recheck(expected.setup(), expected.catalog(), as_of)
            .map_err(map_setup_error)
    }
}

fn ensure_before(
    as_of: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), PortfolioApplicationServiceError> {
    if as_of.unix_nanos() <= 0 {
        Err(PortfolioApplicationServiceError::InvalidRequest)
    } else if cancellation.is_cancelled() {
        Err(PortfolioApplicationServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(PortfolioApplicationServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_catalog_error(error: PortfolioAccountCatalogError) -> PortfolioApplicationServiceError {
    match error {
        PortfolioAccountCatalogError::Portfolio(error) => error,
        PortfolioAccountCatalogError::CorruptPublication => {
            PortfolioApplicationServiceError::CorruptPublication
        }
        PortfolioAccountCatalogError::ResourceExhausted => {
            PortfolioApplicationServiceError::ResourceExhausted
        }
        PortfolioAccountCatalogError::CatalogChanged => {
            PortfolioApplicationServiceError::StateChanged
        }
    }
}

fn map_setup_error(error: RecommendationSetupError) -> PortfolioApplicationServiceError {
    match error {
        RecommendationSetupError::InvalidProfile
        | RecommendationSetupError::AccountUnavailable
        | RecommendationSetupError::CurrencyMismatch
        | RecommendationSetupError::InvalidAsOf => PortfolioApplicationServiceError::InvalidRequest,
        RecommendationSetupError::CapacityExceeded => {
            PortfolioApplicationServiceError::ResourceExhausted
        }
        RecommendationSetupError::StateChanged
        | RecommendationSetupError::StaleRevision
        | RecommendationSetupError::StaleCatalog => PortfolioApplicationServiceError::StateChanged,
        RecommendationSetupError::CorruptState | RecommendationSetupError::Encoding => {
            PortfolioApplicationServiceError::CorruptPublication
        }
        RecommendationSetupError::Unavailable
        | RecommendationSetupError::RecoveryRequired
        | RecommendationSetupError::TimeUnavailable
        | RecommendationSetupError::Persistence(_)
        | RecommendationSetupError::PreviewUnavailable
        | RecommendationSetupError::PreviewExpired
        | RecommendationSetupError::InvalidConfirmation
        | RecommendationSetupError::CrossWorkspacePreview
        | RecommendationSetupError::RevisionExhausted
        | RecommendationSetupError::InvalidBackup
        | RecommendationSetupError::RestoreTargetOccupied => {
            PortfolioApplicationServiceError::Authority
        }
    }
}
