//! Source-preserving bounded display-market read projection.

use std::sync::Arc;

use market_squawk_domain::{InstrumentId, SourceIdentifier};
use market_squawk_services::ServiceError;
use market_squawk_sources::SourceMetadata;

use crate::{
    live_source::display_market::{DisplayMarketKey, DisplayMarketSnapshotLease},
    provider_activation::MarketDataInstrumentBinding,
};

/// Immutable provider-symbol authority for one configured display child.
#[derive(Debug)]
pub(super) struct DisplaySourceDescriptor {
    surface_id: SourceIdentifier,
    metadata: SourceMetadata,
    symbols: Box<[DisplayInstrumentSymbol]>,
}

impl DisplaySourceDescriptor {
    pub(super) fn try_new(
        surface_id: &str,
        metadata: SourceMetadata,
        bindings: Box<[MarketDataInstrumentBinding]>,
    ) -> Result<Arc<Self>, ServiceError> {
        if bindings.is_empty() {
            return Err(ServiceError::InvalidRequest);
        }
        let mut symbols = Vec::new();
        symbols
            .try_reserve_exact(bindings.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for binding in bindings {
            let symbol = SourceIdentifier::try_from(binding.provisional_subscription_symbol())
                .map_err(|_error| ServiceError::InvalidRequest)?;
            if symbols.iter().any(|prior: &DisplayInstrumentSymbol| {
                prior.instrument_id == binding.instrument_id() || prior.provider_symbol == symbol
            }) {
                return Err(ServiceError::InvalidRequest);
            }
            symbols.push(DisplayInstrumentSymbol {
                instrument_id: binding.instrument_id(),
                provider_symbol: symbol,
            });
        }
        Ok(Arc::new(Self {
            surface_id: SourceIdentifier::try_from(surface_id)
                .map_err(|_error| ServiceError::InvalidRequest)?,
            metadata,
            symbols: symbols.into_boxed_slice(),
        }))
    }

    pub(super) fn supports(&self, key: &DisplayMarketKey) -> bool {
        self.metadata.source_id() == key.source_id()
            && self
                .symbols
                .iter()
                .any(|binding| binding.instrument_id == key.instrument_id())
    }

    pub(super) fn matches_snapshot(&self, lease: &DisplayMarketSnapshotLease) -> bool {
        self.supports(lease.key()) && snapshot_matches_metadata(lease, &self.metadata)
    }

    pub(super) fn instrument_count(&self) -> usize {
        self.symbols.len()
    }

    pub(super) fn append_instrument_ids(&self, destination: &mut Vec<InstrumentId>) {
        destination.extend(self.symbols.iter().map(|binding| binding.instrument_id));
    }

    fn symbol_for(&self, instrument_id: InstrumentId) -> Option<&SourceIdentifier> {
        self.symbols
            .iter()
            .find(|binding| binding.instrument_id == instrument_id)
            .map(|binding| &binding.provider_symbol)
    }
}

#[derive(Debug)]
struct DisplayInstrumentSymbol {
    instrument_id: InstrumentId,
    provider_symbol: SourceIdentifier,
}

/// One bounded display snapshot joined only to its exact prepared provider-symbol authority.
#[derive(Debug)]
pub(crate) struct MarketDisplaySnapshotLease {
    descriptor: Arc<DisplaySourceDescriptor>,
    provider_symbol: SourceIdentifier,
    lease: DisplayMarketSnapshotLease,
}

impl MarketDisplaySnapshotLease {
    pub(super) fn try_new(
        descriptor: Arc<DisplaySourceDescriptor>,
        lease: DisplayMarketSnapshotLease,
    ) -> Result<Self, ServiceError> {
        if !descriptor.matches_snapshot(&lease) {
            return Err(ServiceError::Unavailable);
        }
        let provider_symbol = descriptor
            .symbol_for(lease.key().instrument_id())
            .ok_or(ServiceError::Unavailable)
            .and_then(|symbol| {
                SourceIdentifier::try_from(symbol.as_str())
                    .map_err(|_error| ServiceError::ResourceExhausted)
            })?;
        Ok(Self {
            descriptor,
            provider_symbol,
            lease,
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

    pub(crate) const fn lease(&self) -> &DisplayMarketSnapshotLease {
        &self.lease
    }

    pub(super) const fn descriptor(&self) -> &Arc<DisplaySourceDescriptor> {
        &self.descriptor
    }
}

/// Complete, exact-key-ordered result of one bounded display-market instrument read.
#[derive(Debug)]
pub(crate) struct MarketDisplaySnapshotBatch {
    snapshots: Vec<MarketDisplaySnapshotLease>,
}

impl MarketDisplaySnapshotBatch {
    pub(super) const fn new(snapshots: Vec<MarketDisplaySnapshotLease>) -> Self {
        Self { snapshots }
    }

    pub(crate) fn snapshots(&self) -> &[MarketDisplaySnapshotLease] {
        &self.snapshots
    }
}

fn snapshot_matches_metadata(
    lease: &DisplayMarketSnapshotLease,
    metadata: &SourceMetadata,
) -> bool {
    let revision = metadata.revision();
    [lease.trade(), lease.quote(), lease.status()]
        .into_iter()
        .flatten()
        .all(|read| read.observation().provenance().metadata_revision() == revision)
}
