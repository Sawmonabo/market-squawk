//! Canonical identity selection and the existing sealed action-publication handoff.

use market_squawk_adapter_alpaca::{
    AlpacaCorporateActionIdentity, AlpacaCorporateActionsCoverage, AlpacaCorporateActionsRequest,
    AlpacaError, AlpacaPreparedCorporateActionsPublication,
};
use market_squawk_data::{
    CorporateActionQueryIdentityError, CorporateActionQueryIdentityPrecommitAuthority,
    CorporateActionQueryIdentitySelection, IngestPrecommitAuthority,
    MarketDataInstrumentReadCapability, MarketDataProviderIdentitySelection,
};
use market_squawk_domain::{CalendarDate, InstrumentId, SourceId, Timestamp};
use market_squawk_sources::{ExtractionRequest, SealedProviderCaptureBinding};
use std::{sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

/// Request selected by the existing canonical catalog before provider dispatch. Query identities
/// and their original selection cutoff are independent of returned event cardinality.
#[derive(Debug)]
pub(crate) struct PreparedSourceActionQuery {
    reader: MarketDataInstrumentReadCapability,
    identity: CorporateActionQueryIdentitySelection,
    request: AlpacaCorporateActionsRequest,
}
impl PreparedSourceActionQuery {
    pub(crate) fn select(
        reader: MarketDataInstrumentReadCapability,
        source: SourceId,
        instruments: Vec<InstrumentId>,
        selected_at: Timestamp,
        process_dates: (CalendarDate, CalendarDate),
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, SourceActionQueryError> {
        let identity = reader.select_corporate_action_query_identities(
            source,
            instruments,
            selected_at,
            deadline,
            cancellation,
        )?;
        let request = AlpacaCorporateActionsRequest::try_new(
            identity.symbols().map(str::to_owned).collect(),
            process_dates.0,
            process_dates.1,
        )?;
        Ok(Self {
            reader,
            identity,
            request,
        })
    }
    pub(crate) fn identity(&self) -> &CorporateActionQueryIdentitySelection {
        &self.identity
    }
    /// Dispatch this exact all-family source request through the existing runtime/capture owner.
    pub(crate) const fn request(&self) -> &AlpacaCorporateActionsRequest {
        &self.request
    }

    /// Rejoins the terminal original capture, source-date action identities and exact query-level
    /// reference selection. The ordinary canonical publisher receives the resulting binding and
    /// composed currentness guard together; this leaf creates no second publisher or store.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bind(
        self,
        prepared: AlpacaPreparedCorporateActionsPublication,
        extraction_request: &ExtractionRequest,
        actions: &[AlpacaCorporateActionIdentity],
        event_identities: Vec<MarketDataProviderIdentitySelection>,
        ingested_at: Timestamp,
        source_authority: Arc<dyn IngestPrecommitAuthority>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SourceActionQueryPublication, SourceActionQueryError> {
        let (binding, coverage) = prepared.try_into_binding(
            extraction_request,
            actions,
            self.identity.retained(),
            ingested_at,
        )?;
        if coverage.request() != &self.request {
            return Err(SourceActionQueryError::Mismatch);
        }
        let authority = self
            .identity
            .publication_authority(
                self.reader,
                &binding,
                source_authority,
                deadline,
                cancellation,
            )?
            .with_event_identities(event_identities)?;
        Ok(SourceActionQueryPublication {
            binding,
            coverage,
            authority,
        })
    }
}

/// One-use input to the existing canonical ingest operation. Even a zero-action result retains
/// one real query summary, its exact original capture and all selected identity receipts.
#[derive(Debug)]
pub(crate) struct SourceActionQueryPublication {
    binding: SealedProviderCaptureBinding,
    coverage: AlpacaCorporateActionsCoverage,
    authority: CorporateActionQueryIdentityPrecommitAuthority,
}
impl SourceActionQueryPublication {
    pub(crate) const fn coverage(&self) -> &AlpacaCorporateActionsCoverage {
        &self.coverage
    }
    pub(crate) fn into_parts(
        self,
    ) -> (
        SealedProviderCaptureBinding,
        Arc<dyn IngestPrecommitAuthority>,
    ) {
        (self.binding, Arc::new(self.authority))
    }
}
#[derive(Debug, thiserror::Error)]
pub(crate) enum SourceActionQueryError {
    #[error("action capture differs from its selected source query")]
    Mismatch,
    #[error(transparent)]
    Identity(#[from] CorporateActionQueryIdentityError),
    #[error(transparent)]
    Source(#[from] AlpacaError),
}
