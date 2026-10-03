//! Inert action-specific coordinates copied from an original opaque catalog selection.

use market_squawk_domain::{
    CorporateActionEventInstrumentIdentity, InstrumentId, ProviderIdentityRecord, VenueMapping,
};

use crate::{AlpacaError, AlpacaInstrumentMapping};

/// A catalog-selected mapping retained for one action economic coordinate. Construction validates
/// exact copied values only. The data publisher must receive and replay the original opaque
/// catalog selection; this wrapper cannot authorize a provider identity on its own.
#[derive(Clone, Debug)]
pub struct AlpacaCorporateActionInstrument {
    mapping: AlpacaInstrumentMapping,
    retained: CorporateActionEventInstrumentIdentity,
}

impl AlpacaCorporateActionInstrument {
    /// Requires the exact provider assertion and venue record returned for the original selection.
    /// No ticker-only hash, fabricated historical validity, or implicit namespace is accepted.
    pub fn try_new(
        mapping: AlpacaInstrumentMapping,
        provider_identity: &ProviderIdentityRecord,
        venue_mapping: &VenueMapping,
        retained: CorporateActionEventInstrumentIdentity,
    ) -> Result<Self, AlpacaError> {
        let selection = &retained.selection;
        if !retained.valid_for_event(selection.knowledge_at)
            || retained.source_id.as_str() != "alpaca-basic-iex-market-data"
            || retained.venue_id.as_str() != crate::config::IEX_VENUE
            || mapping.instrument() != selection.instrument_id
            || mapping.symbol() != retained.provider_instrument_id.as_str()
            || provider_identity.instrument_id() != selection.instrument_id
            || provider_identity.source_id() != &retained.source_id
            || provider_identity.provider_instrument_id() != &retained.provider_instrument_id
            || provider_identity.metadata_revision() != &selection.provider_identity_revision
            || provider_identity.evidence().content_digest()
                != selection.provider_identity_payload_digest
            || provider_identity.validity() != selection.provider_identity_validity
            || provider_identity.observed_at() > selection.knowledge_at
            || provider_identity
                .source_timestamp()
                .is_some_and(|published| published > selection.knowledge_at)
            || venue_mapping.venue_id() != &retained.venue_id
            || venue_mapping.venue_symbol() != &retained.venue_symbol
        {
            return Err(AlpacaError::InvalidCoverage);
        }
        Ok(Self { mapping, retained })
    }

    /// Returns the exact source symbol selected at the original economic coordinate.
    pub fn symbol(&self) -> &str {
        self.mapping.symbol()
    }

    /// Returns the exact catalog-selected stable instrument.
    pub const fn instrument(&self) -> InstrumentId {
        self.mapping.instrument()
    }

    /// Returns inert coordinates retained for exact catalog replay, not replacement authority.
    pub const fn retained(&self) -> &CorporateActionEventInstrumentIdentity {
        &self.retained
    }
}
