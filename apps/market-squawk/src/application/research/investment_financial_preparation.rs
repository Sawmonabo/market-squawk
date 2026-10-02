//! Retained financial reads and independently admitted selected-company preparation.
use super::{
    company_research::{
        CompanyResearchReadCapability, CompanyResearchRequest, ResearchRevisionPolicy,
    },
    corporate_actions::map_research_error,
    instrument_context::{
        InstrumentContextOutcome, InstrumentContextReadCapability, InstrumentContextRequest,
    },
    investment_financials::authorize_financial_manifest,
};
use crate::application::market_selection::product::MarketProductSelectionReadCapability;
use crate::{
    ResearchService,
    provider_activation::{ProviderAdapterActivation, SecSelectedCompanyAcquisition},
};
use market_squawk_data::{
    DatasetManifestRef, ListingReferenceReadCapability, ListingReferenceRecord, SecResearchFamily,
    SecResearchPreparationOutcome, SecResearchResolvedOutcome,
};
use market_squawk_domain::{AssetClass, InstrumentId, ResearchTemporalCoordinate, Timestamp};
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

    /// Prepare retained families before requesting missing source data. Available sections remain
    /// durable even when a later source acquisition fails or the job is cancelled.
    pub(crate) async fn acquire(
        &self,
        input: &InvestmentFinancialPreparationInput,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InvestmentFinancialPreparedResult, ServiceError> {
        let request = financial_request(input.instrument)?;
        let (mut result, missing) = self
            .prepare_retained(&request, deadline, cancellation)
            .await?;
        if !missing.is_empty() {
            let acquired = self.activation.publish_sec_company_for_listing(
                input.instrument, &input.listing, &missing, deadline, cancellation.child_token(),
            ).await.map_err(|error| {
                tracing::warn!(stage = "acquisition", error = ?error, "selected financial preparation failed");
                operation_error(deadline, cancellation)
            })?;
            let request = financial_request(input.instrument)?;
            result = self
                .prepare_retained(&request, deadline, cancellation)
                .await?
                .0;
            result.acquisition = Some(acquired);
        }
        self.validate_completion(&result, deadline, cancellation)
            .await?;
        Ok(result)
    }

    async fn prepare_retained(
        &self,
        request: &CompanyResearchRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(InvestmentFinancialPreparedResult, Vec<SecResearchFamily>), ServiceError> {
        let reader = CompanyResearchReadCapability::new(Arc::clone(&self.research));
        let mut result = InvestmentFinancialPreparedResult {
            acquisition: None,
            manifests: Vec::new(),
            families: Vec::new(),
        };
        let mut missing = Vec::new();
        for family in [
            SecResearchFamily::CompanyFacts,
            SecResearchFamily::Submissions,
            SecResearchFamily::FilingXbrl,
        ] {
            let resolved = reader
                .resolve_company_family(request, family, deadline, cancellation)
                .await
                .map_err(super::investment_financials::canonical_error)?;
            let state = match resolved.outcome() {
                SecResearchResolvedOutcome::Exact(exact) => {
                    if !authorize_financial_manifest(
                        &self.research,
                        exact.manifest(),
                        false,
                        deadline,
                        cancellation,
                    )
                    .await?
                    {
                        "rights_unavailable"
                    } else {
                        let prepared = reader
                            .prepare_company_family(request, family, deadline, cancellation)
                            .await
                            .map_err(super::investment_financials::canonical_error)?;
                        if prepared.identity() != resolved.identity() {
                            return Err(ServiceError::InvalidResult);
                        }
                        match prepared.outcome() {
                            SecResearchPreparationOutcome::Prepared(receipt) => {
                                result.manifests.push(exact.manifest().clone());
                                result.families.push(serde_json::json!({
                                    "family": super::investment_financials::family_name(family),
                                    "state": "prepared", "generation": receipt.generation_key(),
                                    "artifactId": receipt.artifact().artifact_id(), "rowCount": receipt.row_count(),
                                }));
                                continue;
                            }
                            SecResearchPreparationOutcome::Missing => {
                                missing.push(family);
                                "missing"
                            }
                            SecResearchPreparationOutcome::Ambiguous => "ambiguous",
                            SecResearchPreparationOutcome::Stale => {
                                missing.push(family);
                                "stale"
                            }
                            SecResearchPreparationOutcome::Revoked => "revoked",
                        }
                    }
                }
                SecResearchResolvedOutcome::Missing => {
                    missing.push(family);
                    "missing"
                }
                SecResearchResolvedOutcome::Ambiguous => "ambiguous",
                SecResearchResolvedOutcome::Stale => {
                    missing.push(family);
                    "stale"
                }
                SecResearchResolvedOutcome::Revoked => "revoked",
            };
            result.families.push(serde_json::json!({
                "family": super::investment_financials::family_name(family), "state": state,
            }));
        }
        Ok((result, missing))
    }

    pub(crate) async fn validate_completion(
        &self,
        outcome: &InvestmentFinancialPreparedResult,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Err(operation_error(deadline, cancellation));
        }
        if let Some(acquisition) = &outcome.acquisition {
            self.activation
                .validate_selected_company_preparation(acquisition)
                .map_err(|_| ServiceError::Unavailable)?;
        }
        for manifest in &outcome.manifests {
            if !authorize_financial_manifest(
                &self.research,
                manifest,
                false,
                deadline,
                cancellation,
            )
            .await?
            {
                return Err(ServiceError::Unavailable);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct InvestmentFinancialPreparedResult {
    acquisition: Option<SecSelectedCompanyAcquisition>,
    manifests: Vec<DatasetManifestRef>,
    families: Vec<serde_json::Value>,
}
impl InvestmentFinancialPreparedResult {
    pub(crate) fn value(&self) -> serde_json::Value {
        serde_json::json!({
            "state": if self.manifests.is_empty() { "unavailable" } else { "prepared" },
            "families": self.families,
            "acquisition": self.acquisition.as_ref().map(SecSelectedCompanyAcquisition::value),
        })
    }
}
fn financial_request(instrument: InstrumentId) -> Result<CompanyResearchRequest, ServiceError> {
    let now = chrono::Utc::now()
        .timestamp_nanos_opt()
        .filter(|value| *value > 0)
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::Unavailable)?;
    CompanyResearchRequest::try_new(
        instrument,
        now,
        ResearchTemporalCoordinate::calendar_date(
            now.utc_calendar_date()
                .map_err(|_| ServiceError::Unavailable)?,
        ),
        ResearchRevisionPolicy::LatestKnown,
    )
    .map_err(super::investment_financials::canonical_error)
}
fn operation_error(deadline: Instant, cancellation: &CancellationToken) -> ServiceError {
    if cancellation.is_cancelled() {
        ServiceError::Cancelled
    } else if Instant::now() >= deadline {
        ServiceError::DeadlineExceeded
    } else {
        ServiceError::Unavailable
    }
}
