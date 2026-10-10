//! Actual historical canonical selections for source-returned event identities.

use super::*;
use crate::application::market_calendar::CompletedMarketSessionDateReceipt;
use market_squawk_adapter_alpaca::{AlpacaCorporateActionInstrument, AlpacaInstrumentMapping};
use market_squawk_data::{MarketDataProviderIdentityQuery, MarketDataProviderIdentitySelection};
use market_squawk_domain::{
    CorporateActionEventInstrumentIdentity, CorporateActionQueryInstrumentIdentity,
    ProviderInstrumentId, SourceId, SourceIdentifier,
};

impl SourceActionPreparationCapability {
    /// The source reported a symbol and civil event date. Only the existing catalog, queried at
    /// the genuine session and actual local observation clock, can resolve that symbol. Values
    /// extracted below remain inert; the opaque selection accompanies canonical precommit and
    /// is reconstructed from the same original native coordinates on every source reopen.
    pub(super) fn select_event_identity(
        &self,
        source: &SourceId,
        symbol: &str,
        session: &CompletedMarketSessionDateReceipt,
        knowledge_at: Timestamp,
        context: &RequestContext,
    ) -> Result<
        Option<(
            AlpacaCorporateActionInstrument,
            MarketDataProviderIdentitySelection,
        )>,
        ServiceError,
    > {
        check(context)?;
        let query = MarketDataProviderIdentityQuery::try_new(
            source.clone(),
            ProviderInstrumentId::try_from(symbol).map_err(|_| ServiceError::InvalidResult)?,
            knowledge_at,
            session.opens_at(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let reader = self.research.market_data_instruments();
        let Some(selection) = reader
            .select_provider_identity_as_of(query, context.deadline(), context.cancellation())
            .map_err(|_| controlled(context, ServiceError::Unavailable))?
        else {
            return Ok(None);
        };
        let record = reader
            .read_selected_provider_definition(
                &selection,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(|_| controlled(context, ServiceError::Unavailable))?;
        let exact = selection
            .exact_receipt()
            .map_err(|_| ServiceError::InvalidResult)?;
        let definition = record.definition();
        let interval = definition.effective_interval();
        let provider = definition
            .provider_identity_at(
                source,
                selection.query().provider_instrument_id(),
                session.opens_at(),
            )
            .ok_or(ServiceError::InvalidResult)?;
        if interval.starts_at() > session.opens_at()
            || interval
                .ends_at()
                .is_some_and(|end| end < session.closes_at_exclusive())
            || provider.validity().starts_at() > session.opens_at()
            || provider
                .validity()
                .ends_at()
                .is_some_and(|end| end < session.closes_at_exclusive())
            || record.published_at() > knowledge_at
            || exact.instrument_id() != definition.instrument_id()
            || exact.definition_revision_digest() != record.revision_digest()
        {
            return Ok(None);
        }
        let mut venues = definition.venue_mappings().iter().filter(|venue| {
            exact.matching_venues().contains(venue.venue_id())
                && venue.venue_symbol().as_str() == symbol
        });
        let Some(venue) = venues.next() else {
            return Ok(None);
        };
        if venues.next().is_some() {
            return Ok(None);
        }
        let retained = CorporateActionEventInstrumentIdentity {
            source_id: source.clone(),
            provider_instrument_id: selection.query().provider_instrument_id().clone(),
            venue_id: venue.venue_id().clone(),
            venue_symbol: venue.venue_symbol().clone(),
            selection: CorporateActionQueryInstrumentIdentity {
                symbol: SourceIdentifier::try_from(symbol)
                    .map_err(|_| ServiceError::InvalidResult)?,
                instrument_id: exact.instrument_id(),
                knowledge_at: selection.query().knowledge_at(),
                effective_at: selection.query().effective_at(),
                definition_revision_digest: exact.definition_revision_digest(),
                definition_revision_sequence: exact.definition_revision_sequence(),
                definition_published_at: exact.definition_published_at(),
                definition_reference_revision: exact.definition_reference_revision().clone(),
                definition_reference_payload_digest: exact.definition_reference_payload_digest(),
                provider_identity_revision: exact.provider_identity_revision().clone(),
                provider_identity_payload_digest: exact.provider_identity_payload_digest(),
                provider_identity_validity: exact.provider_identity_validity(),
                selection_digest: selection.selection_digest(),
            },
            resolution_receipt_digest: selection.resolution_receipt_digest(),
        };
        let mapping = AlpacaInstrumentMapping::try_new(
            symbol.to_owned(),
            definition.instrument_id(),
            definition.asset_class(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let identity = AlpacaCorporateActionInstrument::try_new(mapping, provider, venue, retained)
            .map_err(|_| ServiceError::InvalidResult)?;
        check(context)?;
        Ok(Some((identity, selection)))
    }
}
