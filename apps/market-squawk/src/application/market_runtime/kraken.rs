//! Source-preserving presenter projection for authenticated Kraken order-level state.

use std::sync::Arc;

use market_squawk_domain::{InstrumentExecutionTerms, InstrumentId, MarketDepth, SourceIdentifier};
use market_squawk_live::OrderLevelPriceProjection;
use market_squawk_services::ServiceError;
use market_squawk_sources::SourceMetadata;

use crate::{
    live_source::order_level::{OrderLevelBookKey, OrderLevelPriceRead},
    provider_activation::PreparedKrakenL3MarketConfiguration,
};

use super::configuration::AccountMarketSurface;

/// Immutable configured identity retained independently from the moving connection generation.
#[derive(Debug)]
pub(super) struct KrakenSourceDescriptor {
    surface_id: SourceIdentifier,
    metadata: SourceMetadata,
    source_depth: MarketDepth,
    symbols: Box<[KrakenInstrumentSymbol]>,
}

impl KrakenSourceDescriptor {
    pub(super) fn try_from_prepared(
        prepared: &PreparedKrakenL3MarketConfiguration,
    ) -> Result<Arc<Self>, ServiceError> {
        if prepared.instruments().is_empty()
            || prepared.config().market_depth() != MarketDepth::OrderLevel
        {
            return Err(ServiceError::InvalidRequest);
        }
        let mut symbols = Vec::new();
        symbols
            .try_reserve_exact(prepared.instruments().len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for binding in prepared.instruments() {
            let provider_symbol = SourceIdentifier::try_from(binding.provider_symbol())
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            if symbols.iter().any(|prior: &KrakenInstrumentSymbol| {
                prior.instrument_id == binding.instrument_id()
                    || prior.provider_symbol == provider_symbol
            }) {
                return Err(ServiceError::InvalidRequest);
            }
            symbols.push(KrakenInstrumentSymbol {
                instrument_id: binding.instrument_id(),
                provider_symbol,
                execution_terms: binding.execution_terms(),
            });
        }
        Ok(Arc::new(Self {
            surface_id: SourceIdentifier::try_from(AccountMarketSurface::KrakenLevel3.surface_id())
                .map_err(|_error| ServiceError::ResourceExhausted)?,
            metadata: prepared.config().metadata().clone(),
            source_depth: prepared.config().market_depth(),
            symbols: symbols.into_boxed_slice(),
        }))
    }

    pub(super) fn instrument_count(&self) -> usize {
        self.symbols.len()
    }

    pub(super) fn append_instrument_ids(&self, destination: &mut Vec<InstrumentId>) {
        destination.extend(self.symbols.iter().map(|symbol| symbol.instrument_id));
    }

    pub(super) fn supports(&self, instrument_id: InstrumentId) -> bool {
        self.binding_for(instrument_id).is_some()
    }

    fn binding_for(&self, instrument_id: InstrumentId) -> Option<&KrakenInstrumentSymbol> {
        self.symbols
            .iter()
            .find(|symbol| symbol.instrument_id == instrument_id)
    }
}

#[derive(Debug)]
struct KrakenInstrumentSymbol {
    instrument_id: InstrumentId,
    provider_symbol: SourceIdentifier,
    execution_terms: InstrumentExecutionTerms,
}

/// One permit-retaining aggregate derived from an exact authenticated order-level generation.
#[derive(Debug)]
pub(crate) struct MarketKrakenPriceProjectionLease {
    descriptor: Arc<KrakenSourceDescriptor>,
    provider_symbol: SourceIdentifier,
    execution_terms: InstrumentExecutionTerms,
    key: OrderLevelBookKey,
    read: OrderLevelPriceRead,
}

impl MarketKrakenPriceProjectionLease {
    pub(super) fn try_new(
        descriptor: Arc<KrakenSourceDescriptor>,
        key: OrderLevelBookKey,
        read: OrderLevelPriceRead,
    ) -> Result<Self, ServiceError> {
        let projection = read.projection();
        let route = projection.route();
        let binding = descriptor
            .binding_for(route.instrument_id())
            .filter(|binding| {
                binding.provider_symbol.as_str() == route.provider_instrument().as_str()
            })
            .ok_or(ServiceError::Unavailable)?;
        if descriptor.metadata.source_id() != route.source_id()
            || !descriptor
                .metadata
                .coverage()
                .topology()
                .contains_venue(route.venue_id())
            || route.source_id() != key.source_id()
            || route.venue_id() != key.venue_id()
            || route.instrument_id() != key.instrument_id()
            || route.generation() != key.generation()
            || super::quality_rank(projection.quality())
                < super::quality_rank(descriptor.metadata.quality_ceiling())
        {
            return Err(ServiceError::Unavailable);
        }
        let provider_symbol = SourceIdentifier::try_from(binding.provider_symbol.as_str())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let execution_terms = binding.execution_terms;
        Ok(Self {
            descriptor,
            provider_symbol,
            execution_terms,
            key,
            read,
        })
    }

    pub(crate) fn surface_id(&self) -> &SourceIdentifier {
        &self.descriptor.surface_id
    }

    pub(crate) fn metadata(&self) -> &SourceMetadata {
        &self.descriptor.metadata
    }

    pub(crate) const fn provider_symbol(&self) -> &SourceIdentifier {
        &self.provider_symbol
    }

    /// Returns the exact revision-bound terms used by the admitted order-level runtime.
    pub(crate) const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.execution_terms
    }

    pub(crate) const fn key(&self) -> &OrderLevelBookKey {
        &self.key
    }

    /// Returns the upstream source depth without relabelling the aggregate projection itself.
    pub(crate) fn source_depth(&self) -> MarketDepth {
        self.descriptor.source_depth
    }

    pub(crate) const fn projection(&self) -> &OrderLevelPriceProjection {
        self.read.projection()
    }

    pub(super) const fn descriptor(&self) -> &Arc<KrakenSourceDescriptor> {
        &self.descriptor
    }
}
