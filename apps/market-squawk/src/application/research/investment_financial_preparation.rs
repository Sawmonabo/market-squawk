//! On-demand issuer preparation for a selected investment. Reads remain independent of acquisition.
use super::{
    corporate_actions::map_research_error,
    instrument_context::{
        InstrumentContextOutcome, InstrumentContextReadCapability, InstrumentContextRequest,
    },
    investment_financials::{
        InvestmentFinancialReadCapability, InvestmentFinancialResult, InvestmentFinancialSection,
    },
};
use crate::application::market_selection::product::MarketProductSelectionReadCapability;
use crate::{
    ResearchService,
    provider_activation::{ProviderAdapterActivation, SecSelectedCompanyAcquisition},
};
use chrono::Utc;
use market_squawk_data::ListingReferenceReadCapability;
use market_squawk_domain::{AssetClass, Timestamp};
use market_squawk_services::{ServiceError, ServiceLimits};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Instant,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

/// Only requests for the same selected investment share acquisition; ordinary page reads do not.
#[derive(Debug)]
pub(super) struct InvestmentFinancialPreparation {
    research: Arc<ResearchService>,
    selections: MarketProductSelectionReadCapability,
    references: InstrumentContextReadCapability,
    listings: ListingReferenceReadCapability,
    activation: Arc<ProviderAdapterActivation>,
    acquiring: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
}

impl InvestmentFinancialPreparation {
    pub(super) fn new(
        research: Arc<ResearchService>,
        references: InstrumentContextReadCapability,
        listings: ListingReferenceReadCapability,
        activation: Arc<ProviderAdapterActivation>,
    ) -> Self {
        Self {
            selections: MarketProductSelectionReadCapability::new(
                Arc::clone(&research),
                research.market_data_instruments(),
            ),
            research,
            references,
            listings,
            activation,
            acquiring: Mutex::new(HashMap::new()),
        }
    }

    pub(super) async fn read(
        &self,
        financials: &InvestmentFinancialReadCapability,
        selection: &str,
        section: InvestmentFinancialSection,
        cursor: Option<&str>,
        limit: usize,
        limits: ServiceLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InvestmentFinancialResult, ServiceError> {
        let first = financials
            .read(
                selection,
                section,
                cursor,
                limit,
                limits,
                deadline,
                cancellation,
            )
            .await?;
        if cursor.is_some() || !first.needs_acquisition() {
            return Ok(first);
        }
        if let Some(token) = first.read_token() {
            financials.close(selection, token)?;
        }
        let gate = {
            let mut gates = self
                .acquiring
                .lock()
                .map_err(|_| ServiceError::Unavailable)?;
            gates.retain(|_, value| value.strong_count() > 0);
            if let Some(gate) = gates.get(selection).and_then(Weak::upgrade) {
                gate
            } else {
                let gate = Arc::new(AsyncMutex::new(()));
                gates.insert(selection.to_owned(), Arc::downgrade(&gate));
                gate
            }
        };
        let _guard = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ServiceError::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => return Err(ServiceError::DeadlineExceeded),
            guard = gate.lock() => guard,
        };
        // Another panel may have completed this issuer while this request awaited its own gate.
        let retained = financials
            .read(
                selection,
                section,
                None,
                limit,
                limits,
                deadline,
                cancellation,
            )
            .await?;
        if !retained.needs_acquisition() {
            return Ok(retained);
        }
        let acquired = self.acquire(selection, deadline, cancellation).await;
        match acquired {
            Ok(false) => Ok(retained),
            Ok(true) => {
                if let Some(token) = retained.read_token() {
                    financials.close(selection, token)?;
                }
                // Freeze the display cutoff after newly published evidence becomes available.
                financials
                    .read(
                        selection,
                        section,
                        None,
                        limit,
                        limits,
                        deadline,
                        cancellation,
                    )
                    .await
            }
            Err(error) => {
                if let Some(token) = retained.read_token() {
                    financials.close(selection, token)?;
                }
                Err(error)
            }
        }
    }

    async fn acquire(
        &self,
        selection: &str,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<bool, ServiceError> {
        let now = Utc::now()
            .timestamp_nanos_opt()
            .filter(|time| *time > 0)
            .map(Timestamp::from_unix_nanos)
            .ok_or(ServiceError::Unavailable)?;
        let instrument = self
            .selections
            .resolve(selection, now, deadline, cancellation)
            .await?;
        let references = self.references.clone();
        let listings = self.listings.clone();
        let listing = self
            .research
            .run_owned_research_read(deadline, cancellation, move |owned| {
                let request = InstrumentContextRequest::try_new(instrument, now, now)
                    .map_err(|_| ServiceError::InvalidRequest)?;
                let read = references
                    .read(request, deadline, &owned)
                    .map_err(|_| ServiceError::Unavailable)?;
                let InstrumentContextOutcome::Exact(context) = read.outcome() else {
                    return Ok(None);
                };
                if context.asset_class() != AssetClass::Equity || context.exchange_traded_fund() {
                    return Ok(None);
                }
                listings
                    .exact_current(
                        context.listed_symbol(),
                        context.listing_venue(),
                        deadline,
                        &owned,
                    )
                    .map_err(|_| ServiceError::Unavailable)
            })
            .await
            .map_err(map_research_error)??;
        let Some(listing) = listing else {
            return Ok(false);
        };
        match self
            .activation
            .publish_sec_company_for_listing(
                instrument,
                &listing,
                deadline,
                cancellation.child_token(),
            )
            .await
        {
            Ok(SecSelectedCompanyAcquisition::Published) => Ok(true),
            Ok(
                SecSelectedCompanyAcquisition::MissingIssuer
                | SecSelectedCompanyAcquisition::AmbiguousIssuer,
            ) => Ok(false),
            Err(crate::provider_activation::SecFundProductError::SetupRequired) => Ok(false),
            Err(_) if cancellation.is_cancelled() => Err(ServiceError::Cancelled),
            Err(_) if Instant::now() >= deadline => Err(ServiceError::DeadlineExceeded),
            Err(_) => Err(ServiceError::Unavailable),
        }
    }
}
