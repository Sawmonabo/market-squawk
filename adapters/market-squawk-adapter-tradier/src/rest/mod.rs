mod model;
mod normalize;
mod transport;

use market_squawk_domain::{CalendarDate, SourceIdentifier};
use market_squawk_sources::{
    ActiveLiveSourceGeneration, LiveSourceGeneration, SharedProviderBudget, SourceError,
    SourceMetadata, SourceMetadataProvider,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::{
    TRADIER_OPTIONS_CHAIN_ENDPOINT, TRADIER_QUOTES_ENDPOINT, TradierAccessSurface,
    TradierInstrumentKind, TradierLogicalProfile, TradierSourceConfig,
};
use crate::source::{TradierAccountInner, TradierAccountMarketData, TradierAccountMarketDataError};

pub use model::{
    TradierDerivedIndexBatch, TradierDerivedIndexObservation, TradierOptionChain,
    TradierOptionContract, TradierOptionGreeks, TradierOptionSide, TradierQuoteBatch,
    TradierQuoteRequest, TradierQuoteSide, TradierQuoteSnapshot, TradierRestEvidence,
};

/// Bounded REST/bootstrap client for one exact logical source generation.
#[derive(Debug)]
pub struct TradierSnapshotClient {
    config: TradierSourceConfig,
    authority: ActiveLiveSourceGeneration,
    account: std::sync::Arc<TradierAccountInner>,
    budget: SharedProviderBudget,
}

impl TradierAccountMarketData {
    /// Creates a REST/bootstrap client under the same account and request budget as streaming.
    ///
    /// Separate clients may represent consolidated securities and derived indexes, but the
    /// account owner rejects different budget allocations for those logical surfaces.
    ///
    /// # Errors
    ///
    /// Rejects mismatched bounds, absent/different budget authority, or a stale generation.
    pub fn snapshot_client(
        &self,
        config: TradierSourceConfig,
        generation: LiveSourceGeneration,
    ) -> Result<TradierSnapshotClient, TradierAccountMarketDataError> {
        if config.access_surface() != TradierAccessSurface::RestSnapshots {
            return Err(TradierAccountMarketDataError::RestUnsupported);
        }
        self.ensure_limits(&config)?;
        let authority = generation.try_start(config.metadata())?;
        let budget = authority
            .budget()?
            .cloned()
            .ok_or(TradierAccountMarketDataError::MissingBudget)?;
        self.bind_budget(&budget)?;
        Ok(TradierSnapshotClient {
            config,
            authority,
            account: std::sync::Arc::clone(&self.inner),
            budget,
        })
    }
}

impl TradierSnapshotClient {
    /// Retrieves a complete exact-set quote bootstrap for configured symbols.
    ///
    /// # Errors
    ///
    /// Fails closed for an unauthorized symbol, budget/transport failure, partial result,
    /// duplicate result, inexact numeric value, or stale generation authority.
    pub async fn fetch_quotes(
        &mut self,
        request: TradierQuoteRequest,
        cancellation: CancellationToken,
    ) -> Result<TradierQuoteBatch, TradierRestError> {
        self.validate_requested_symbols(&request)?;
        let url = quote_url(&request)?;
        let evidence = transport::fetch_json(
            &mut self.authority,
            &self.account,
            &self.budget,
            &self.config,
            url,
            cancellation,
        )
        .await?;
        normalize::quotes(&self.config, &request, evidence)
    }

    /// Retrieves all configured NDX/RUT/COMP derived values as `Modeled` observations.
    ///
    /// # Errors
    ///
    /// Rejects a securities profile and otherwise applies the same exact-set and transport rules
    /// as [`Self::fetch_quotes`].
    pub async fn fetch_derived_indexes(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<TradierDerivedIndexBatch, TradierRestError> {
        if self.config.profile() != TradierLogicalProfile::DerivedIndexes {
            return Err(TradierRestError::InvalidProfile);
        }
        let request = TradierQuoteRequest::try_new(
            self.config
                .mappings()
                .iter()
                .map(|mapping| mapping.symbol().clone())
                .collect(),
            false,
        )?;
        let url = quote_url(&request)?;
        let evidence = transport::fetch_json(
            &mut self.authority,
            &self.account,
            &self.budget,
            &self.config,
            url,
            cancellation,
        )
        .await?;
        normalize::derived_indexes(&self.config, &request, evidence)
    }

    /// Retrieves one bounded option chain with exact decimals and retained raw provenance.
    ///
    /// # Errors
    ///
    /// Rejects non-securities profiles, unconfigured/non-equity underlyings, malformed dates,
    /// excessive response counts, and all transport/budget/protocol failures.
    pub async fn fetch_option_chain(
        &mut self,
        underlying: SourceIdentifier,
        expiration: CalendarDate,
        include_greeks: bool,
        cancellation: CancellationToken,
    ) -> Result<TradierOptionChain, TradierRestError> {
        if self.config.profile() != TradierLogicalProfile::ConsolidatedSecurities {
            return Err(TradierRestError::InvalidProfile);
        }
        let mapping = self
            .config
            .mapping(underlying.as_str())
            .ok_or(TradierRestError::UnknownSymbol)?;
        if !matches!(
            mapping.kind(),
            TradierInstrumentKind::Equity | TradierInstrumentKind::Etf
        ) {
            return Err(TradierRestError::InvalidRequest);
        }
        let url = option_chain_url(&underlying, expiration, include_greeks)?;
        let evidence = transport::fetch_json(
            &mut self.authority,
            &self.account,
            &self.budget,
            &self.config,
            url,
            cancellation,
        )
        .await?;
        normalize::option_chain(underlying, expiration, evidence)
    }

    fn validate_requested_symbols(
        &self,
        request: &TradierQuoteRequest,
    ) -> Result<(), TradierRestError> {
        if request
            .symbols()
            .iter()
            .any(|symbol| self.config.mapping(symbol.as_str()).is_none())
        {
            Err(TradierRestError::UnknownSymbol)
        } else {
            Ok(())
        }
    }
}

impl SourceMetadataProvider for TradierSnapshotClient {
    fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

fn quote_url(request: &TradierQuoteRequest) -> Result<Url, TradierRestError> {
    let symbols = request
        .symbols()
        .iter()
        .map(SourceIdentifier::as_str)
        .collect::<Vec<_>>()
        .join(",");
    let mut url = Url::parse(TRADIER_QUOTES_ENDPOINT).map_err(|_| TradierRestError::Url)?;
    url.query_pairs_mut()
        .append_pair("symbols", &symbols)
        .append_pair(
            "greeks",
            if request.include_greeks() {
                "true"
            } else {
                "false"
            },
        );
    Ok(url)
}

fn option_chain_url(
    underlying: &SourceIdentifier,
    expiration: CalendarDate,
    include_greeks: bool,
) -> Result<Url, TradierRestError> {
    let mut url = Url::parse(TRADIER_OPTIONS_CHAIN_ENDPOINT).map_err(|_| TradierRestError::Url)?;
    url.query_pairs_mut()
        .append_pair("symbol", underlying.as_str())
        .append_pair("expiration", &expiration.to_string())
        .append_pair("greeks", if include_greeks { "true" } else { "false" });
    Ok(url)
}

/// Bounded REST request, transport, or normalization failure.
#[derive(Debug, Error)]
pub enum TradierRestError {
    /// The method is not valid for this logical quality surface.
    #[error("Tradier REST method is invalid for this logical source profile")]
    InvalidProfile,
    /// The request count, symbol kind, or parameter combination was invalid.
    #[error("Tradier REST request is invalid")]
    InvalidRequest,
    /// A requested symbol is outside the source's exact configured coverage.
    #[error("Tradier REST request contains an unconfigured symbol")]
    UnknownSymbol,
    /// The final request URI could not be represented.
    #[error("Tradier REST request URL is invalid")]
    Url,
    /// The final URI was not authorized by immutable source metadata.
    #[error("Tradier REST request is outside the source network policy")]
    NetworkPolicy,
    /// The operation was explicitly cancelled.
    #[error("Tradier REST operation was cancelled")]
    Cancelled,
    /// The provider response omitted or contradicted required rate-limit headers.
    #[error("Tradier REST response has invalid rate-limit evidence")]
    InvalidRateLimitEvidence,
    /// The response body or headers contradicted the provider protocol.
    #[error("Tradier REST response is invalid")]
    InvalidResponse,
    /// The provider returned fewer configured observations than requested.
    #[error("Tradier REST response is missing a requested observation")]
    MissingObservation,
    /// The provider returned a symbol or type outside the exact request.
    #[error("Tradier REST response contains an unexpected observation")]
    UnexpectedObservation,
    /// A provider observation appeared more than once.
    #[error("Tradier REST response contains a duplicate observation")]
    DuplicateObservation,
    /// A provider number was not an exact bounded decimal.
    #[error("Tradier REST response contains an invalid exact decimal")]
    InvalidDecimal,
    /// A provider timestamp was invalid or outside the domain range.
    #[error("Tradier REST response contains an invalid timestamp")]
    InvalidTimestamp,
    /// A provider calendar date was invalid.
    #[error("Tradier REST response contains an invalid calendar date")]
    InvalidDate,
    /// The number of returned observations exceeded the local ceiling.
    #[error("Tradier REST response exceeds the local observation ceiling")]
    ResponseLimitExceeded,
    /// A bounded allocation could not be reserved.
    #[error("Tradier REST bounded allocation failed")]
    Allocation,
    /// Registry, network, budget, or frame authority failed closed.
    #[error("Tradier REST source failure: {0}")]
    Source(#[from] SourceError),
}
