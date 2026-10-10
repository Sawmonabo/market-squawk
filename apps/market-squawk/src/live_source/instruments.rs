//! Validated platform-to-adapter instrument composition.

use market_squawk_adapter_coinbase::{CoinbaseConfigError, CoinbaseProductMapping};
use market_squawk_domain::{ProviderProduct, SourceIdentifier};
use market_squawk_platform::CoinbaseSourceConfig;
use market_squawk_data::{CatalogLimit, InstrumentDefinitionReadCapability, MarketDataInstrumentReadCapability};
use market_squawk_live::LiveRouteConfig;
use market_squawk_domain::Timestamp;
use std::time::Instant;
use tokio_util::sync::CancellationToken;
use market_squawk_sources::{ProviderIdentitySelectionEvidence, ProviderNativeIdentityRequest};
use std::sync::Arc;
use thiserror::Error;

/// Catalog-owned native selections supplied by reference acquisition before source startup.
/// The requests grant no authority: the production registry selects them again through `reader`.
#[derive(Clone, Debug)]
pub(crate) struct ProductionCatalogSelection {
    reader: MarketDataInstrumentReadCapability,
    requests: Arc<[ProviderNativeIdentityRequest]>,
}

impl ProductionCatalogSelection {
    pub(crate) fn try_new(
        reader: MarketDataInstrumentReadCapability,
        requests: Vec<ProviderNativeIdentityRequest>,
    ) -> Result<Self, ProductionInstrumentError> {
        if requests.is_empty() || requests.len() > 4_096 {
            return Err(ProductionInstrumentError::InvalidCatalogRoutes);
        }
        for (index, request) in requests.iter().enumerate() {
            if requests[index.saturating_add(1)..].iter().any(|other| {
                other.instrument == request.instrument && other.venue == request.venue
            }) {
                return Err(ProductionInstrumentError::InvalidCatalogRoutes);
            }
        }
        Ok(Self { reader, requests: requests.into() })
    }

    pub(super) fn reader(&self) -> MarketDataInstrumentReadCapability {
        self.reader.clone()
    }

    pub(super) fn requests(&self) -> &[ProviderNativeIdentityRequest] {
        &self.requests
    }

    /// Ensures each live actor's execution terms are the catalog's current terms, and each
    /// source route matches the selected market-data definition before any actor can start.
    pub(super) fn validate_live_routes(
        &self,
        execution: &InstrumentDefinitionReadCapability,
        routes: &[LiveRouteConfig],
        at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductionInstrumentError> {
        if routes.len() != self.requests.len() {
            return Err(ProductionInstrumentError::InvalidCatalogRoutes);
        }
        let limit = CatalogLimit::new(10_000)
            .map_err(|_| ProductionInstrumentError::InvalidCatalogRoutes)?;
        for route in routes {
            let request = self.requests.iter().find(|request| {
                request.venue == *route.route().venue()
                    && request.instrument == route.route().instrument()
            }).ok_or(ProductionInstrumentError::InvalidCatalogRoutes)?;
            let record = self.reader.latest(request.instrument, deadline, cancellation)
                .map_err(|_| ProductionInstrumentError::CatalogUnavailable)?
                .ok_or(ProductionInstrumentError::CatalogUnavailable)?;
            let definition = record.definition();
            if definition.instrument_id() != request.instrument
                || definition.asset_class() != route.definition().asset_class()
                || definition.quote_currency() != route.definition().quote_currency()
                || definition.provider_identity_at(
                    &request.namespace,
                    &request.provider_instrument_id,
                    at,
                ).is_none()
                || !definition.venue_mappings().iter().any(|mapping| {
                    mapping.venue_id() == &request.venue
                        && mapping.venue_symbol() == &request.venue_symbol
                })
            {
                return Err(ProductionInstrumentError::CatalogUnavailable);
            }
            let pinned = execution.pin(
                &[request.instrument], at, limit, deadline, cancellation,
            ).map_err(|_| ProductionInstrumentError::CatalogUnavailable)?;
            if pinned.execution_terms_at(request.instrument, at)
                != Some(route.definition().execution_terms())
            {
                return Err(ProductionInstrumentError::ExecutionTermsMismatch);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct ProductionInstrumentSet {
    adapter_mappings: Box<[CoinbaseProductMapping]>,
}

impl ProductionInstrumentSet {
    pub(super) fn adapter_mappings(&self) -> &[CoinbaseProductMapping] {
        &self.adapter_mappings
    }

    /// Builds the public adapter route from catalog-selected evidence after reference admission.
    /// The registry independently reselects the same native coordinates before a live session.
    pub(super) fn try_from_selected_public(
        config: &CoinbaseSourceConfig,
        selected: &[ProviderIdentitySelectionEvidence],
    ) -> Result<Self, ProductionInstrumentError> {
        if selected.len() != config.instruments().len() {
            return Err(ProductionInstrumentError::CatalogUnavailable);
        }
        let mut adapter_mappings = Vec::new();
        adapter_mappings
            .try_reserve_exact(selected.len())
            .map_err(|_| ProductionInstrumentError::AllocationFailed)?;
        for mapping in config.instruments() {
            let instrument = mapping.definition().instrument_id();
            let evidence = selected
                .iter()
                .find(|evidence| {
                    evidence.native.instrument == instrument
                        && evidence.native.provider_instrument_id.as_str() == mapping.product()
                })
                .ok_or(ProductionInstrumentError::CatalogUnavailable)?;
            let product = ProviderProduct::new(SourceIdentifier::try_from(mapping.product())?);
            adapter_mappings.push(CoinbaseProductMapping::try_new_selected_public(
                product,
                instrument,
                evidence.clone(),
            )?);
        }
        Ok(Self {
            adapter_mappings: adapter_mappings.into_boxed_slice(),
        })
    }
}

impl TryFrom<&CoinbaseSourceConfig> for ProductionInstrumentSet {
    type Error = ProductionInstrumentError;

    fn try_from(config: &CoinbaseSourceConfig) -> Result<Self, Self::Error> {
        let mut adapter_mappings = Vec::new();
        adapter_mappings
            .try_reserve_exact(config.instruments().len())
            .map_err(|_error| ProductionInstrumentError::AllocationFailed)?;
        for mapping in config.instruments() {
            let definition = mapping.definition().clone();
            let product = ProviderProduct::new(SourceIdentifier::try_from(mapping.product())?);
            adapter_mappings.push(CoinbaseProductMapping::try_new(
                product,
                definition.instrument_id(),
            )?);
        }
        Ok(Self {
            adapter_mappings: adapter_mappings.into_boxed_slice(),
        })
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ProductionInstrumentError {
    #[error("production instrument mapping allocation failed")]
    AllocationFailed,
    #[error("accepted native catalog route set is missing, duplicated, or oversized")]
    InvalidCatalogRoutes,
    #[error("current accepted native catalog definition is unavailable")]
    CatalogUnavailable,
    #[error("live actor execution terms differ from the current canonical catalog")]
    ExecutionTermsMismatch,
    #[error("production instrument identity is invalid")]
    Identity(#[from] market_squawk_domain::IdentityError),
    #[error("Coinbase adapter mapping rejected validated configuration")]
    Adapter(#[from] CoinbaseConfigError),
}
