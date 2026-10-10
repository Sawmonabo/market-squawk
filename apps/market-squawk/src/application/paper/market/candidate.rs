//! Durable-authority boundary for imported-portfolio candidate impact.

use std::{fmt, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_data::{InstrumentDefinitionReadCapability, MarketDataInstrumentReadCapability};
use market_squawk_domain::{InstrumentId, MarketDepth, MarketEvent, Timestamp};
use market_squawk_services::ServiceError;
use tokio_util::sync::CancellationToken;

use crate::{
    application::{
        market_selection::{MarketInvestmentReadCapability, MarketInvestmentReadReceipt},
        recommendation::{RecommendationSetupAuthority, RecommendationSetupError},
    },
    portfolio_application::{
        PortfolioAccountCatalogError, PortfolioAccountCatalogReadCapability,
        PortfolioAnalysisDepthLevelsInput, PortfolioAnalysisDepthUnavailableReason,
        PortfolioAnalysisLiquidityEvidence, PortfolioAnalysisMarketAvailability,
        PortfolioAnalysisMarketEntry, PortfolioAnalysisMarketSet,
        PortfolioAnalysisMarketUnavailableReason, PortfolioAnalysisSetupResolution,
        PortfolioAnalysisSetupSnapshot, PortfolioApplicationServiceError,
        PortfolioCandidateAvailability, PortfolioCandidateMarketEvidence,
        PortfolioCandidateResolution, PortfolioCandidateResolutionAuthority,
        PortfolioCandidateSetupBinding, PortfolioCandidateUnavailableReason,
    },
    research_service::ResearchService,
};

/// Composes the existing durable market, definition and imported-portfolio read authorities.
#[derive(Clone)]
pub(in crate::application::paper) struct ProductionPortfolioCandidateResolutionFactory {
    markets: MarketInvestmentReadCapability,
}

impl ProductionPortfolioCandidateResolutionFactory {
    pub(in crate::application::paper) fn try_new(
        research: Arc<ResearchService>,
        instrument_definitions: InstrumentDefinitionReadCapability,
        market_data_instruments: MarketDataInstrumentReadCapability,
        maximum_mark_age_nanos: u64,
    ) -> Result<Self, ServiceError> {
        Ok(Self {
            markets: MarketInvestmentReadCapability::try_new(
                research,
                instrument_definitions,
                market_data_instruments,
                maximum_mark_age_nanos,
            )?,
        })
    }

    /// Binds the caller's validated financial policy without changing any source ceiling or
    /// borrowing the paper runtime's age default for analytical portfolio reads.
    pub(in crate::application::paper) fn with_maximum_mark_age_nanos(
        &self,
        maximum_mark_age_nanos: u64,
    ) -> Result<Self, ServiceError> {
        Ok(Self {
            markets: self
                .markets
                .with_maximum_mark_age_nanos(maximum_mark_age_nanos)?,
        })
    }

    /// Binds only durable setup and immutable imported-portfolio read capabilities.
    pub(in crate::application::paper) fn bind(
        &self,
        setup: Arc<RecommendationSetupAuthority>,
        catalog: PortfolioAccountCatalogReadCapability,
    ) -> Arc<dyn PortfolioCandidateResolutionAuthority> {
        Arc::new(ProductionPortfolioCandidateResolutionAuthority {
            setup,
            catalog,
            markets: self.markets.clone(),
        })
    }
}

impl fmt::Debug for ProductionPortfolioCandidateResolutionFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductionPortfolioCandidateResolutionFactory")
            .field("market", &self.markets)
            .finish()
    }
}

struct ProductionPortfolioCandidateResolutionAuthority {
    setup: Arc<RecommendationSetupAuthority>,
    catalog: PortfolioAccountCatalogReadCapability,
    markets: MarketInvestmentReadCapability,
}

#[async_trait]
impl PortfolioCandidateResolutionAuthority for ProductionPortfolioCandidateResolutionAuthority {
    async fn resolve(
        &self,
        binding: &PortfolioCandidateSetupBinding,
        instrument_id: InstrumentId,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PortfolioCandidateResolution, PortfolioApplicationServiceError> {
        ensure_before(as_of, deadline, &cancellation)?;
        if !binding.is_explicit_account() {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let catalog = self
            .catalog
            .snapshot_current(deadline, &cancellation)
            .map_err(map_catalog_error)?;
        let head = catalog
            .head(binding.account_id())
            .ok_or(PortfolioApplicationServiceError::StateChanged)?;
        if head.revision() != binding.portfolio_revision()
            || head.reporting_currency() != binding.reporting_currency()
        {
            return Err(PortfolioApplicationServiceError::StateChanged);
        }
        let receipt = self
            .markets
            .read(instrument_id, as_of, deadline, cancellation.clone())
            .await
            .map_err(map_market_error)?
            .ok_or(PortfolioApplicationServiceError::Authority)?;
        let entry = portfolio_market_entry(binding.portfolio_revision(), &receipt)?;
        let PortfolioAnalysisMarketAvailability::Available { market, .. } = entry.availability()
        else {
            return Err(PortfolioApplicationServiceError::Authority);
        };
        self.catalog
            .recheck(&catalog, deadline, &cancellation)
            .map_err(map_catalog_error)?;
        let result = PortfolioCandidateResolution::try_from_explicit_account(
            binding.clone(),
            catalog,
            market.clone(),
            as_of,
        )?;
        ensure_before(as_of, deadline, &cancellation)?;
        Ok(result)
    }

    async fn recheck(
        &self,
        expected: &PortfolioCandidateResolution,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), PortfolioApplicationServiceError> {
        ensure_before(as_of, deadline, &cancellation)?;
        if expected.evaluated_at() != as_of {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        self.catalog
            .recheck(expected.catalog(), deadline, &cancellation)
            .map_err(map_catalog_error)?;
        let current = self
            .resolve(
                expected.binding(),
                expected.market().observation().instrument_id(),
                as_of,
                deadline,
                cancellation.clone(),
            )
            .await?;
        if &current != expected {
            return Err(PortfolioApplicationServiceError::StateChanged);
        }
        ensure_before(as_of, deadline, &cancellation)?;
        Ok(())
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
            || instrument_ids.len() > 4096
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
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(instrument_ids.len())
            .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
        for instrument_id in instrument_ids {
            ensure_before(as_of, deadline, &cancellation)?;
            let selected = self
                .markets
                .read(*instrument_id, as_of, deadline, cancellation.clone())
                .await
                .map_err(map_market_error)?;
            entries.push(match selected {
                Some(receipt) => {
                    portfolio_market_entry(setup.setup().current_head().revision(), &receipt)?
                }
                None => PortfolioAnalysisMarketEntry::unavailable(
                    *instrument_id,
                    PortfolioAnalysisMarketUnavailableReason::NoEligibleSelectedSource,
                ),
            });
        }
        self.catalog
            .recheck(setup.catalog(), deadline, &cancellation)
            .map_err(map_catalog_error)?;
        self.setup
            .recheck(setup.setup(), setup.catalog(), as_of)
            .map_err(map_setup_error)?;
        ensure_before(as_of, deadline, &cancellation)?;
        PortfolioAnalysisMarketSet::try_new(setup.clone(), entries, as_of)
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
        let instruments = expected
            .entries()
            .iter()
            .map(PortfolioAnalysisMarketEntry::instrument_id)
            .collect::<Vec<_>>();
        let current = self
            .resolve_analysis_markets(
                expected.setup(),
                &instruments,
                as_of,
                deadline,
                cancellation.clone(),
            )
            .await?;
        if &current != expected {
            return Err(PortfolioApplicationServiceError::StateChanged);
        }
        ensure_before(as_of, deadline, &cancellation)?;
        Ok(())
    }
}

fn portfolio_market_entry(
    revision: &market_squawk_portfolio::PortfolioRevisionToken,
    receipt: &MarketInvestmentReadReceipt,
) -> Result<PortfolioAnalysisMarketEntry, PortfolioApplicationServiceError> {
    let observation = receipt
        .observation()
        .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
    let instrument_id = observation.instrument_id();
    let Some(terms) = receipt.execution_terms() else {
        return Ok(PortfolioAnalysisMarketEntry::unavailable(
            instrument_id,
            PortfolioAnalysisMarketUnavailableReason::InstrumentDefinitionUnavailable,
        ));
    };
    let market = PortfolioCandidateMarketEvidence::try_from_market_selection(
        receipt.selection(),
        observation,
        terms,
        PortfolioCandidateAvailability::Unavailable(PortfolioCandidateUnavailableReason::Fees),
        PortfolioCandidateAvailability::Unavailable(PortfolioCandidateUnavailableReason::Slippage),
        revision.clone(),
    )?;
    let liquidity = match receipt
        .event()
        .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?
    {
        MarketEvent::Quote(quote) => {
            let side = |level: Option<market_squawk_domain::BookLevel>| match level {
                Some(level) => PortfolioAnalysisDepthLevelsInput::Available(vec![(
                    level.price(),
                    level.quantity(),
                )]),
                None => PortfolioAnalysisDepthLevelsInput::Unavailable(
                    PortfolioAnalysisDepthUnavailableReason::SideUnavailable,
                ),
            };
            PortfolioAnalysisLiquidityEvidence::try_from_selected_depth(
                &market,
                MarketDepth::TopOfBook,
                quote.provenance().connection_generation().get(),
                market.observation().observed_at(),
                market.observation().available_at(),
                market.observation().fresh_until(),
                side(quote.bid()),
                side(quote.ask()),
            )?
        }
        MarketEvent::MarketDataQuote(quote) => {
            // Native quote size currently has only absent/null/unresolved-unit variants. Genuine
            // lot terms do not establish that source field's unit, so do not manufacture depth.
            let side = |level: Option<&market_squawk_domain::MarketDataQuoteSide>| {
                PortfolioAnalysisDepthLevelsInput::Unavailable(if level.is_some() {
                    PortfolioAnalysisDepthUnavailableReason::SideIncomplete
                } else {
                    PortfolioAnalysisDepthUnavailableReason::SideUnavailable
                })
            };
            PortfolioAnalysisLiquidityEvidence::try_from_selected_depth(
                &market,
                MarketDepth::TopOfBook,
                quote.provenance().connection_generation().get(),
                market.observation().observed_at(),
                market.observation().available_at(),
                market.observation().fresh_until(),
                side(quote.bid()),
                side(quote.ask()),
            )?
        }
        _ => PortfolioAnalysisLiquidityEvidence::unavailable(
            &market,
            PortfolioAnalysisDepthUnavailableReason::SourceDoesNotSupplyDepth,
        ),
    };
    Ok(PortfolioAnalysisMarketEntry::available(
        instrument_id,
        market,
        liquidity,
    ))
}

fn map_market_error(error: ServiceError) -> PortfolioApplicationServiceError {
    match error {
        ServiceError::Cancelled => PortfolioApplicationServiceError::Cancelled,
        ServiceError::DeadlineExceeded => PortfolioApplicationServiceError::DeadlineExceeded,
        ServiceError::ResourceExhausted => PortfolioApplicationServiceError::ResourceExhausted,
        ServiceError::InvalidRequest => PortfolioApplicationServiceError::InvalidRequest,
        ServiceError::InvalidResult => PortfolioApplicationServiceError::CorruptPublication,
        ServiceError::Unauthorized
        | ServiceError::NotFound
        | ServiceError::Unavailable
        | ServiceError::Internal => PortfolioApplicationServiceError::Authority,
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
