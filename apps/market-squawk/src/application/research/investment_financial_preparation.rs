//! Retained financial reads and independently admitted selected-company preparation.
use super::{
    corporate_actions::map_research_error,
    instrument_context::{
        InstrumentContextOutcome, InstrumentContextReadCapability, InstrumentContextRequest,
    },
};
use crate::application::market_selection::product::MarketProductSelectionReadCapability;
use crate::{
    ResearchService,
    provider_activation::{ProviderAdapterActivation, SecSelectedCompanyAcquisition},
};
use market_squawk_data::{ListingReferenceReadCapability, ListingReferenceRecord};
use market_squawk_domain::{AssetClass, InstrumentId, Timestamp};
use market_squawk_services::ServiceError;
use std::{sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub(crate) struct InvestmentFinancialPreparation {
    research: Arc<ResearchService>,
    selections: MarketProductSelectionReadCapability,
    references: InstrumentContextReadCapability,
    listings: ListingReferenceReadCapability,
    activation: Arc<ProviderAdapterActivation>,
}

/// Process-owned exact local admission. Only this capability can construct it; no caller may
/// substitute a symbol, issuer or listing record after the product token has been resolved.
#[derive(Debug)]
pub(crate) struct InvestmentFinancialPreparationInput {
    selection_token: String,
    instrument: InstrumentId,
    listing: ListingReferenceRecord,
    captured_at: Timestamp,
}

impl InvestmentFinancialPreparationInput {
    pub(crate) const fn instrument(&self) -> InstrumentId {
        self.instrument
    }
    pub(crate) fn selection_token(&self) -> &str {
        &self.selection_token
    }
    pub(crate) const fn captured_at(&self) -> Timestamp {
        self.captured_at
    }
    pub(crate) fn coordinates(&self) -> serde_json::Value {
        serde_json::json!({
            "selectionToken": self.selection_token,
            "instrumentId": self.instrument,
            "capturedAtUnixNanos": self.captured_at.unix_nanos().to_string(),
            "listingSource": self.listing.generation().source_id(),
            "listingDataset": self.listing.generation().dataset(),
            "listingGenerationDigest": self.listing.generation().generation_digest(),
            "listingRecordDigest": self.listing.record_digest(),
            "listingRevision": self.listing.record_revision(),
        })
    }
}

impl InvestmentFinancialPreparation {
    pub(crate) fn new(
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
        }
    }

    /// Admission uses only retained exact selection and listing evidence under the start request.
    pub(crate) async fn admit_selection(
        &self,
        selection: &str,
        captured_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InvestmentFinancialPreparationInput, ServiceError> {
        let instrument = self
            .selections
            .resolve(selection, captured_at, deadline, cancellation)
            .await?;
        let references = self.references.clone();
        let listings = self.listings.clone();
        let listing = self
            .research
            .run_owned_research_read(deadline, cancellation, move |owned| {
                let request =
                    InstrumentContextRequest::try_new(instrument, captured_at, captured_at)
                        .map_err(|_| ServiceError::InvalidRequest)?;
                let read = references
                    .read(request, deadline, &owned)
                    .map_err(|_| ServiceError::Unavailable)?;
                let InstrumentContextOutcome::Exact(context) = read.outcome() else {
                    return Err(ServiceError::Unavailable);
                };
                if context.asset_class() != AssetClass::Equity || context.exchange_traded_fund() {
                    return Err(ServiceError::InvalidRequest);
                }
                listings
                    .exact_current(
                        context.listed_symbol(),
                        context.listing_venue(),
                        deadline,
                        &owned,
                    )
                    .map_err(|_| ServiceError::Unavailable)?
                    .ok_or(ServiceError::Unavailable)
            })
            .await
            .map_err(map_research_error)??;
        Ok(InvestmentFinancialPreparationInput {
            selection_token: selection.to_owned(),
            instrument,
            listing,
            captured_at,
        })
    }

    /// The installed job supplies the independent deadline/cancellation. Genuine intermediate
    /// family and relationship publications remain retained if a later stage is interrupted.
    pub(crate) async fn acquire(
        &self,
        input: &InvestmentFinancialPreparationInput,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SecSelectedCompanyAcquisition, ServiceError> {
        self.activation
            .publish_sec_company_for_listing(
                input.instrument,
                &input.listing,
                deadline,
                cancellation.child_token(),
            )
            .await
            .map_err(|error| {
                tracing::warn!(stage = "acquisition", error = ?error, "selected financial preparation failed");
                if cancellation.is_cancelled() {
                    ServiceError::Cancelled
                } else if Instant::now() >= deadline {
                    ServiceError::DeadlineExceeded
                } else {
                    ServiceError::Unavailable
                }
            })
    }

    pub(crate) fn validate_completion(
        &self,
        outcome: &SecSelectedCompanyAcquisition,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        if cancellation.is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(ServiceError::DeadlineExceeded);
        }
        self.activation
            .validate_selected_company_preparation(outcome)
            .map_err(|_| ServiceError::Unavailable)
    }
}
