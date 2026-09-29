//! Accepted source-qualified canonical reference for Schwab read-only quote acquisition.

use super::{
    MarketInstrumentReferenceBinding, MarketSubscriptionPriority, SchwabMarketRuntimeStartError,
};
use market_squawk_data::MarketDataInstrumentRecord;
use market_squawk_domain::{
    InstrumentId, MarketDataInstrumentDefinition, MarketDataReference, ProviderIdentityRecord,
    Timestamp,
};

pub(crate) const SCHWAB_INSTRUMENT_REFERENCE_SOURCE: &str = "schwab-trader-api-instruments";

/// Retains the complete actual catalog record and its original provider assertion namespace.
/// Construction grants no execution terms, status, executable size, or trading authority.
#[derive(Clone, Debug)]
pub(crate) struct SchwabQuoteReferenceBinding {
    record: MarketDataInstrumentRecord,
    provider_identity: ProviderIdentityRecord,
    reference: MarketInstrumentReferenceBinding,
    priority: MarketSubscriptionPriority,
}

impl SchwabQuoteReferenceBinding {
    pub(crate) fn try_new(
        record: MarketDataInstrumentRecord,
        provider_identity: ProviderIdentityRecord,
        reference: MarketInstrumentReferenceBinding,
        priority: MarketSubscriptionPriority,
        at: Timestamp,
    ) -> Result<Self, SchwabMarketRuntimeStartError> {
        let definition = record.definition();
        if record.published_at() > at
            || provider_identity.source_id().as_str() != SCHWAB_INSTRUMENT_REFERENCE_SOURCE
            || definition.provider_identity_at(
                provider_identity.source_id(),
                provider_identity.provider_instrument_id(),
                at,
            ) != Some(&provider_identity)
            || provider_identity.instrument_id() != definition.instrument_id()
            || market_squawk_adapter_schwab::ProviderIdentifier::try_new(
                provider_identity
                    .provider_instrument_id()
                    .as_str()
                    .to_owned(),
            )
            .is_err()
        {
            return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
        }
        match &reference {
            MarketInstrumentReferenceBinding::NasdaqListing(listing) => {
                if listing.provider_symbol() != provider_identity.provider_instrument_id().as_str()
                    || !definition.identifiers().iter().any(|identifier| {
                        matches!(identifier.identifier(), market_squawk_domain::ExternalIdentifier::Ticker(ticker) if ticker.as_str() == listing.provider_symbol())
                    })
                {
                    return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
                }
            }
            MarketInstrumentReferenceBinding::AssignedExternalIdentifier(identifier) => {
                if !definition.identifiers().contains(identifier) {
                    return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
                }
            }
        }
        let result = Self {
            record,
            provider_identity,
            reference,
            priority,
        };
        result.quote_reference(at)?;
        Ok(result)
    }
    pub(crate) fn quote_reference(
        &self,
        at: Timestamp,
    ) -> Result<MarketDataReference, SchwabMarketRuntimeStartError> {
        MarketDataReference::try_new(
            self.definition(),
            self.record.revision_digest(),
            &self.provider_identity,
            at,
        )
        .map_err(|_| SchwabMarketRuntimeStartError::CanonicalIdentity)
    }
    pub(crate) fn instrument_id(&self) -> InstrumentId {
        self.record.definition().instrument_id()
    }
    pub(crate) fn provider_symbol(&self) -> &str {
        self.provider_identity.provider_instrument_id().as_str()
    }
    pub(crate) const fn provider_identity(&self) -> &ProviderIdentityRecord {
        &self.provider_identity
    }
    pub(crate) const fn canonical_record(&self) -> &MarketDataInstrumentRecord {
        &self.record
    }
    pub(crate) fn definition(&self) -> &MarketDataInstrumentDefinition {
        self.record.definition()
    }
    pub(crate) const fn reference(&self) -> &MarketInstrumentReferenceBinding {
        &self.reference
    }
    pub(crate) const fn priority(&self) -> MarketSubscriptionPriority {
        self.priority
    }
}

/// Registry-minted catalog selections paired with the exact live source generation.
///
/// The copied evidence returned for archive rows is never itself publication authority. This
/// owner retains both opaque inputs until precommit, so account/health/catalog revocation still
/// prevents a stale selected row from becoming durable.
#[derive(Clone, Debug)]
pub(crate) struct SchwabQuotePublicationSelection {
    source: market_squawk_sources::CurrentSourceAuthorityLease,
    selected:
        std::collections::BTreeMap<InstrumentId, market_squawk_sources::CurrentProviderIdentity>,
}

impl SchwabQuotePublicationSelection {
    pub(crate) fn try_new<'a>(
        source: market_squawk_sources::CurrentSourceAuthorityLease,
        selected: Vec<market_squawk_sources::CurrentProviderIdentity>,
        bindings: impl IntoIterator<Item = &'a SchwabQuoteReferenceBinding>,
        venue: &market_squawk_domain::VenueId,
        at: Timestamp,
    ) -> Result<Self, SchwabMarketRuntimeStartError> {
        let bindings = bindings.into_iter().collect::<Vec<_>>();
        if selected.len() != bindings.len() || bindings.is_empty() {
            return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
        }
        let mut by_instrument = std::collections::BTreeMap::new();
        for binding in bindings {
            let identity = selected
                .iter()
                .find(|identity| identity.evidence().native.instrument == binding.instrument_id())
                .ok_or(SchwabMarketRuntimeStartError::CanonicalIdentity)?;
            let evidence = identity.evidence();
            if &evidence.native.namespace != binding.provider_identity().source_id()
                || &evidence.native.provider_instrument_id
                    != binding.provider_identity().provider_instrument_id()
                || &evidence.native.venue != venue
                || evidence.native.venue_symbol.as_str() != binding.provider_symbol()
                || evidence.definition_digest != binding.canonical_record().revision_digest()
                || evidence.definition_sequence != binding.canonical_record().revision_sequence()
                || evidence.definition_published_at != binding.canonical_record().published_at()
                || evidence.definition_validity != binding.definition().effective_interval()
                || &evidence.provider_revision != binding.provider_identity().metadata_revision()
                || evidence.provider_payload_digest
                    != binding.provider_identity().evidence().content_digest()
                || evidence.provider_validity != binding.provider_identity().validity()
                || identity.source_id() != source.binding().source_id()
                || identity.source_revision().metadata_revision()
                    != source.binding().metadata_revision()
                || source.validate_provider_identity_at(identity, at).is_err()
                || by_instrument
                    .insert(binding.instrument_id(), identity.clone())
                    .is_some()
            {
                return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
            }
        }
        Ok(Self {
            source,
            selected: by_instrument,
        })
    }

    /// Checks source, catalog and registered selection against the same current clock.
    pub(crate) fn validate_at(&self, at: Timestamp) -> Result<(), SchwabMarketRuntimeStartError> {
        for identity in self.selected.values() {
            self.source
                .validate_provider_identity_at(identity, at)
                .map_err(|_| SchwabMarketRuntimeStartError::CanonicalIdentity)?;
        }
        Ok(())
    }

    /// Projects selected identity in accepted canonical order; source cohorts stay unbound.
    pub(crate) fn for_events(
        &self,
        events: &[market_squawk_domain::MarketEvent],
        at: Timestamp,
    ) -> Result<
        Vec<Option<market_squawk_sources::ProviderIdentitySelectionEvidence>>,
        SchwabMarketRuntimeStartError,
    > {
        use market_squawk_domain::MarketEvent;
        if events.is_empty() {
            return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
        }
        self.validate_at(at)?;
        let mut selections = Vec::new();
        selections
            .try_reserve_exact(events.len())
            .map_err(|_| SchwabMarketRuntimeStartError::CanonicalIdentity)?;
        for event in events {
            let (provenance, reference) = match event {
                MarketEvent::MarketDataQuote(event) => {
                    (event.provenance(), Some(event.reference()))
                }
                MarketEvent::MarketDataBook(event) => (event.provenance(), Some(event.reference())),
                MarketEvent::MarketDataChart(event) => {
                    (event.provenance(), Some(event.reference()))
                }
                MarketEvent::MarketDataScreener(event) => {
                    let row = event.provenance().binding();
                    let expected_venue = &self
                        .selected
                        .values()
                        .next()
                        .ok_or(SchwabMarketRuntimeStartError::CanonicalIdentity)?
                        .evidence()
                        .native
                        .venue;
                    if row.instrument_id().is_some()
                        || row.source_id() != self.source.binding().source_id()
                        || row.metadata_revision() != self.source.binding().metadata_revision()
                        || row.venue_id() != expected_venue
                    {
                        return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
                    }
                    selections.push(None);
                    continue;
                }
                _ => return Err(SchwabMarketRuntimeStartError::CanonicalIdentity),
            };
            let row = provenance.binding();
            let instrument = row
                .instrument_id()
                .ok_or(SchwabMarketRuntimeStartError::CanonicalIdentity)?;
            let identity = self
                .selected
                .get(&instrument)
                .ok_or(SchwabMarketRuntimeStartError::CanonicalIdentity)?;
            let evidence = identity.evidence();
            let reference = reference.ok_or(SchwabMarketRuntimeStartError::CanonicalIdentity)?;
            if row.source_id() != self.source.binding().source_id()
                || row.metadata_revision() != self.source.binding().metadata_revision()
                || row.venue_id() != &evidence.native.venue
                || row.source_identifier().as_str()
                    != evidence.native.provider_instrument_id.as_str()
                || reference.instrument_id() != instrument
                || reference.definition_digest() != evidence.definition_digest
                || reference.provider_identity().is_none_or(|provider| {
                    provider.source_id() != &evidence.native.namespace
                        || provider.provider_instrument_id()
                            != &evidence.native.provider_instrument_id
                })
                || evidence.native.knowledge_at > provenance.received_at()
                || evidence.native.effective_at > provenance.received_at()
            {
                return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
            }
            selections.push(Some(evidence.clone()));
        }
        Ok(selections)
    }
}
