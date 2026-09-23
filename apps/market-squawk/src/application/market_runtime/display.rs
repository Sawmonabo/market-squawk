//! Source-preserving bounded display-market read projection.

use std::sync::Arc;

use market_squawk_data::MarketDataInstrumentRecord;
use market_squawk_domain::{
    AssetClass, EffectiveInterval, EvidenceDigest, InstrumentId, MarketDataInstrumentDefinition,
    RevisionBoundPayloadEvidence, SourceIdentifier,
};
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
                prior.definition_identity.instrument_id() == binding.instrument_id()
                    || prior.provider_symbol == symbol
            }) {
                return Err(ServiceError::InvalidRequest);
            }
            symbols.push(DisplayInstrumentSymbol {
                definition_identity: Arc::new(DisplayMarketDataDefinitionIdentity::from_binding(
                    &binding,
                )),
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
            && self.supports_instrument(key.instrument_id())
    }

    pub(super) fn supports_instrument(&self, instrument_id: InstrumentId) -> bool {
        self.symbols
            .iter()
            .any(|binding| binding.definition_identity.instrument_id() == instrument_id)
    }

    pub(super) fn matches_snapshot(&self, lease: &DisplayMarketSnapshotLease) -> bool {
        self.supports(lease.key()) && snapshot_matches_metadata(lease, &self.metadata)
    }

    pub(super) fn instrument_count(&self) -> usize {
        self.symbols.len()
    }

    pub(super) fn append_instrument_ids(&self, destination: &mut Vec<InstrumentId>) {
        destination.extend(
            self.symbols
                .iter()
                .map(|binding| binding.definition_identity.instrument_id()),
        );
    }

    fn binding_for(&self, instrument_id: InstrumentId) -> Option<&DisplayInstrumentSymbol> {
        self.symbols
            .iter()
            .find(|binding| binding.definition_identity.instrument_id() == instrument_id)
    }
}

#[derive(Debug)]
struct DisplayInstrumentSymbol {
    definition_identity: Arc<DisplayMarketDataDefinitionIdentity>,
    provider_symbol: SourceIdentifier,
}

/// Exact immutable market-data-definition authority retained by a configured display route.
#[derive(Debug, Eq, PartialEq)]
struct DisplayMarketDataDefinitionIdentity {
    instrument_id: InstrumentId,
    asset_class: AssetClass,
    reference_evidence: RevisionBoundPayloadEvidence,
    effective_interval: EffectiveInterval,
    revision_digest: EvidenceDigest,
}

impl DisplayMarketDataDefinitionIdentity {
    fn from_binding(binding: &MarketDataInstrumentBinding) -> Self {
        Self {
            instrument_id: binding.instrument_id(),
            asset_class: binding.asset_class(),
            reference_evidence: binding.definition_reference_evidence().clone(),
            effective_interval: binding.definition_effective(),
            revision_digest: binding.definition_revision_digest(),
        }
    }

    const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    fn matches_record(&self, record: &MarketDataInstrumentRecord) -> bool {
        self.matches_definition(record.definition())
            && self.revision_digest == record.revision_digest()
    }

    fn matches_definition(&self, definition: &MarketDataInstrumentDefinition) -> bool {
        self.instrument_id == definition.instrument_id()
            && self.asset_class == definition.asset_class()
            && &self.reference_evidence == definition.reference_evidence()
            && self.effective_interval == definition.effective_interval()
    }
}

/// One bounded display snapshot joined only to its exact prepared provider-symbol authority.
#[derive(Debug)]
pub(crate) struct MarketDisplaySnapshotLease {
    descriptor: Arc<DisplaySourceDescriptor>,
    definition_identity: Arc<DisplayMarketDataDefinitionIdentity>,
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
        let binding = descriptor
            .binding_for(lease.key().instrument_id())
            .ok_or(ServiceError::Unavailable)?;
        let provider_symbol = SourceIdentifier::try_from(binding.provider_symbol.as_str())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let definition_identity = Arc::clone(&binding.definition_identity);
        Ok(Self {
            descriptor,
            definition_identity,
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

    /// Requires the latest durable record to be the exact definition configured for this lease.
    pub(crate) fn matches_definition_record(&self, record: &MarketDataInstrumentRecord) -> bool {
        self.definition_identity.matches_record(record)
    }

    /// Rechecks every retained definition coordinate available to a definition-only consumer.
    pub(crate) fn matches_definition(&self, definition: &MarketDataInstrumentDefinition) -> bool {
        self.definition_identity.matches_definition(definition)
    }

    /// Returns the exact whole-definition digest retained by this configured display route.
    pub(crate) fn definition_revision_digest(&self) -> EvidenceDigest {
        self.definition_identity.revision_digest
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
