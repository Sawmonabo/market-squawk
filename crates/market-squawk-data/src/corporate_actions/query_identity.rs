//! Existing canonical-reference selections bound to every corporate-action query filter.

mod current_ordinary;

use crate::corporate_actions::source_capture::retained_corporate_action_event_identities;
use crate::{
    CatalogAuthority, CorporateActionSourceSnapshot, IngestError, IngestPrecommitAuthority,
    MarketDataInstrumentCatalogError, MarketDataInstrumentPopulationDisposition,
    MarketDataInstrumentPopulationQuery, MarketDataInstrumentReadCapability,
    MarketDataInstrumentRecord, MarketDataProviderIdentityQuery,
    MarketDataProviderIdentitySelection,
};
use market_squawk_domain::{
    CorporateActionEventInstrumentIdentity, CorporateActionQueryInstrumentIdentity, EvidenceDigest,
    InstrumentId, ProviderInstrumentId, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::SealedProviderCaptureBinding;
use std::{sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

/// Opaque catalog selection. A provider symbol is obtained from the selected current definition,
/// never guessed from an input ticker. This remains required when the query returns no actions.
#[derive(Clone, Debug)]
pub struct CorporateActionQueryIdentitySelection {
    selections: Box<[MarketDataProviderIdentitySelection]>,
    definitions: Box<[MarketDataInstrumentRecord]>,
    retained: Box<[CorporateActionQueryInstrumentIdentity]>,
    source_receipt_digest: Option<EvidenceDigest>,
    economic_query: bool,
    event_identities: Box<[CorporateActionEventInstrumentIdentity]>,
    event_selections: Box<[MarketDataProviderIdentitySelection]>,
    event_definitions: Box<[MarketDataInstrumentRecord]>,
}
impl CorporateActionQueryIdentitySelection {
    /// Value coordinates for the source publisher; these cannot recreate this opaque selection.
    pub fn retained(&self) -> &[CorporateActionQueryInstrumentIdentity] {
        &self.retained
    }
    /// Original selected definitions; values remain read evidence, never caller-created authority.
    pub fn selected_definitions(&self) -> &[MarketDataInstrumentRecord] {
        &self.definitions
    }
    /// Original immutable definition for an event whose catalog identity was replayed for this
    /// source. The application checks its full interval against the genuine native-date session.
    pub fn event_definition(
        &self,
        identity: &CorporateActionEventInstrumentIdentity,
    ) -> Option<&MarketDataInstrumentRecord> {
        let index = self
            .event_identities
            .binary_search_by_key(&identity.selection.selection_digest.bytes(), |value| {
                value.selection.selection_digest.bytes()
            })
            .ok()?;
        (self.event_identities[index] == *identity).then(|| &self.event_definitions[index])
    }
    /// Exact requested source symbols, in the order required by the bounded source request.
    pub fn symbols(&self) -> impl Iterator<Item = &str> {
        self.retained
            .iter()
            .map(|identity| identity.symbol.as_str())
    }
    /// Confirms that the catalog rejoin was performed for this exact retained source generation.
    pub fn matches_source(&self, source: &CorporateActionSourceSnapshot) -> bool {
        self.source_receipt_digest == Some(source.receipt_digest())
            && self.retained.as_ref() == source.scope().query_instruments.as_slice()
            && self.event_identities.as_ref() == source.event_identities()
            && self.event_selections.len() == self.event_identities.len()
            && self.event_definitions.len() == self.event_identities.len()
    }

    /// Composes the existing provider publication guard with the exact selected reference records.
    /// The caller passes this guard to the same canonical ingest that consumes `binding`.
    pub fn publication_authority(
        self,
        reader: MarketDataInstrumentReadCapability,
        binding: &SealedProviderCaptureBinding,
        inner: Arc<dyn IngestPrecommitAuthority>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CorporateActionQueryIdentityPrecommitAuthority, CorporateActionQueryIdentityError>
    {
        binding
            .validate()
            .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?;
        let capture = binding.capture_evidence();
        let first = capture
            .pages()
            .first()
            .ok_or(CorporateActionQueryIdentityError::Mismatch)?;
        let native = binding
            .native_lineage()
            .batch_sidecar()
            .ok_or(CorporateActionQueryIdentityError::Mismatch)?;
        let sidecar: QuerySidecar = serde_json::from_slice(native.semantic_payload())
            .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?;
        if self.definitions.iter().any(|record| {
            let interval = record.definition().effective_interval();
            interval.starts_at() > first.received_at()
                || interval
                    .ends_at()
                    .is_some_and(|end| first.received_at() >= end)
        }) || binding.native_lineage().schema().implementation()
            != market_squawk_sources::ProviderNativeLineageImplementation::AlpacaCorporateActionsV1
            || self
                .selections
                .iter()
                .any(|selection| selection.query().source_id() != capture.source_id())
            || sidecar.coverage.query_instruments.as_slice() != self.retained.as_ref()
            || sidecar
                .coverage
                .request
                .symbols
                .iter()
                .map(String::as_str)
                .ne(self.symbols())
            || self
                .retained
                .iter()
                .any(|identity| !identity.valid_for_capture(first.received_at()))
        {
            return Err(CorporateActionQueryIdentityError::Mismatch);
        }
        let event_identities = retained_corporate_action_event_identities(
            binding
                .native_lineage()
                .rows()
                .iter()
                .map(|row| row.semantic_payload().as_ref()),
        )
        .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?;
        if event_identities
            .iter()
            .any(|identity| &identity.source_id != capture.source_id())
        {
            return Err(CorporateActionQueryIdentityError::Mismatch);
        }
        let event_selections = event_identities
            .is_empty()
            .then(|| Vec::new().into_boxed_slice());
        let authority = CorporateActionQueryIdentityPrecommitAuthority {
            reader,
            selection: self,
            inner,
            deadline,
            cancellation,
            binding_digest: binding.evidence_digest().evidence(),
            event_identities,
            event_selections,
            require_current_definitions: true,
        };
        authority
            .validate_query_precommit()
            .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?;
        Ok(authority)
    }
}

impl MarketDataInstrumentReadCapability {
    /// Selects every query symbol from the actual canonical population at this local selection
    /// time. This current query coordinate never establishes historical alias validity.
    pub fn select_corporate_action_query_identities(
        &self,
        source: SourceId,
        instruments: Vec<InstrumentId>,
        selected_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CorporateActionQueryIdentitySelection, CorporateActionQueryIdentityError> {
        if instruments.is_empty() || instruments.len() > 32 {
            return Err(CorporateActionQueryIdentityError::Mismatch);
        }
        let population = self.pin_population_as_of(
            MarketDataInstrumentPopulationQuery::try_new(instruments, selected_at, selected_at)?,
            deadline,
            cancellation,
        )?;
        if population.disposition() != MarketDataInstrumentPopulationDisposition::Complete
            || !population.exclusions().is_empty()
        {
            return Err(CorporateActionQueryIdentityError::MissingIdentity);
        }
        let mut selected = Vec::new();
        selected
            .try_reserve_exact(population.records().len())
            .map_err(|_| CorporateActionQueryIdentityError::ResourceBound)?;
        for record in population.records() {
            let mut aliases = record
                .definition()
                .provider_identities()
                .iter()
                .filter(|identity| {
                    identity.source_id() == &source
                        && identity.validity().starts_at() <= selected_at
                        && identity
                            .validity()
                            .ends_at()
                            .is_none_or(|end| selected_at < end)
                });
            let identity = aliases
                .next()
                .ok_or(CorporateActionQueryIdentityError::MissingIdentity)?;
            if aliases.next().is_some() {
                return Err(CorporateActionQueryIdentityError::MissingIdentity);
            }
            let query = MarketDataProviderIdentityQuery::try_new(
                source.clone(),
                identity.provider_instrument_id().clone(),
                selected_at,
                selected_at,
            )?;
            let selection = self
                .select_provider_identity_as_of(query, deadline, cancellation)?
                .ok_or(CorporateActionQueryIdentityError::MissingIdentity)?;
            if selection.exact_receipt()?.instrument_id() != record.definition().instrument_id()
                || selection.exact_receipt()?.definition_revision_digest()
                    != record.revision_digest()
                || self.read_selected_provider_definition(&selection, deadline, cancellation)?
                    != *record
            {
                return Err(CorporateActionQueryIdentityError::Mismatch);
            }
            selected.push((retained_identity(&selection)?, selection, record.clone()));
        }
        assemble(selected, None)
    }

    /// Replays the original source-qualified query and immutable reference revision from an
    /// authentic retained action snapshot. No caller-created action list or empty-set proof enters.
    pub fn reopen_corporate_action_query_identities(
        &self,
        source: &CorporateActionSourceSnapshot,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CorporateActionQueryIdentitySelection, CorporateActionQueryIdentityError> {
        if source.scope().query_instruments.is_empty()
            || source.scope().query_instruments.len() > 32
        {
            return Err(CorporateActionQueryIdentityError::Mismatch);
        }
        let mut selected = Vec::new();
        selected
            .try_reserve_exact(source.scope().query_instruments.len())
            .map_err(|_| CorporateActionQueryIdentityError::ResourceBound)?;
        for retained in &source.scope().query_instruments {
            if !retained.valid_for_capture(source.summary().context().provenance().received_at())
                || retained.knowledge_at > source.knowledge_cutoff()
            {
                return Err(CorporateActionQueryIdentityError::Mismatch);
            }
            let query = MarketDataProviderIdentityQuery::try_new(
                source.summary().context().provenance().source_id().clone(),
                ProviderInstrumentId::try_from(retained.symbol.as_str())
                    .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?,
                retained.knowledge_at,
                retained.effective_at,
            )?;
            let selection = self
                .select_provider_identity_as_of(query, deadline, cancellation)?
                .ok_or(CorporateActionQueryIdentityError::MissingIdentity)?;
            let record =
                self.read_selected_provider_definition(&selection, deadline, cancellation)?;
            let replay = retained_identity(&selection)?;
            if replay != *retained {
                return Err(CorporateActionQueryIdentityError::Mismatch);
            }
            selected.push((replay, selection, record));
        }
        let mut selection = assemble(selected, Some(source.receipt_digest()))?;
        let mut event_selections = Vec::new();
        let mut event_definitions = Vec::new();
        event_definitions
            .try_reserve_exact(source.event_identities().len())
            .map_err(|_| CorporateActionQueryIdentityError::ResourceBound)?;
        event_selections
            .try_reserve_exact(source.event_identities().len())
            .map_err(|_| CorporateActionQueryIdentityError::ResourceBound)?;
        let mut event_budget = EventReadBudget {
            remaining: 64 * 1024 * 1024,
        };
        for retained in source.event_identities() {
            if retained.source_id != *source.summary().context().provenance().source_id()
                || !retained.valid_for_event(source.knowledge_cutoff())
            {
                return Err(CorporateActionQueryIdentityError::Mismatch);
            }
            let query = MarketDataProviderIdentityQuery::try_new(
                retained.source_id.clone(),
                retained.provider_instrument_id.clone(),
                retained.selection.knowledge_at,
                retained.selection.effective_at,
            )?;
            let event = self
                .select_provider_identity_as_of(query, deadline, cancellation)?
                .ok_or(CorporateActionQueryIdentityError::MissingIdentity)?;
            let definition = verify_event_identity(self, retained, &event, deadline, cancellation)?;
            // Charge before retaining each additional definition; catalog reads independently
            // bound the one transient row. Counting serialization allocates no second payload.
            serde_json::to_writer(&mut event_budget, &(retained, definition.definition()))
                .map_err(|_| CorporateActionQueryIdentityError::ResourceBound)?;
            event_budget
                .charge(1024)
                .map_err(|_| CorporateActionQueryIdentityError::ResourceBound)?;
            event_definitions.push(definition);
            event_selections.push(event);
        }
        selection.event_identities = source.event_identities().into();
        selection.event_selections = event_selections.into_boxed_slice();
        selection.event_definitions = event_definitions.into_boxed_slice();
        if !selection.matches_source(source) {
            return Err(CorporateActionQueryIdentityError::Mismatch);
        }
        Ok(selection)
    }
}

fn retained_identity(
    selection: &MarketDataProviderIdentitySelection,
) -> Result<CorporateActionQueryInstrumentIdentity, CorporateActionQueryIdentityError> {
    let exact = selection.exact_receipt()?;
    let query = selection.query();
    Ok(CorporateActionQueryInstrumentIdentity {
        symbol: SourceIdentifier::try_from(query.provider_instrument_id().as_str())
            .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?,
        instrument_id: exact.instrument_id(),
        knowledge_at: query.knowledge_at(),
        effective_at: query.effective_at(),
        definition_revision_digest: exact.definition_revision_digest(),
        definition_revision_sequence: exact.definition_revision_sequence(),
        definition_published_at: exact.definition_published_at(),
        definition_reference_revision: exact.definition_reference_revision().clone(),
        definition_reference_payload_digest: exact.definition_reference_payload_digest(),
        provider_identity_revision: exact.provider_identity_revision().clone(),
        provider_identity_payload_digest: exact.provider_identity_payload_digest(),
        provider_identity_validity: exact.provider_identity_validity(),
        selection_digest: selection.selection_digest(),
    })
}
fn assemble(
    mut values: Vec<(
        CorporateActionQueryInstrumentIdentity,
        MarketDataProviderIdentitySelection,
        MarketDataInstrumentRecord,
    )>,
    source_receipt_digest: Option<EvidenceDigest>,
) -> Result<CorporateActionQueryIdentitySelection, CorporateActionQueryIdentityError> {
    values.sort_by(|left, right| left.0.symbol.as_str().cmp(right.0.symbol.as_str()));
    if values
        .windows(2)
        .any(|pair| pair[0].0.symbol == pair[1].0.symbol)
        || values.iter().enumerate().any(|(index, value)| {
            values[..index]
                .iter()
                .any(|prior| prior.0.instrument_id == value.0.instrument_id)
        })
    {
        return Err(CorporateActionQueryIdentityError::Mismatch);
    }
    let mut retained = Vec::new();
    let mut selections = Vec::new();
    let mut definitions = Vec::new();
    retained
        .try_reserve_exact(values.len())
        .map_err(|_| CorporateActionQueryIdentityError::ResourceBound)?;
    selections
        .try_reserve_exact(values.len())
        .map_err(|_| CorporateActionQueryIdentityError::ResourceBound)?;
    definitions
        .try_reserve_exact(values.len())
        .map_err(|_| CorporateActionQueryIdentityError::ResourceBound)?;
    for (value, selection, record) in values {
        retained.push(value);
        selections.push(selection);
        definitions.push(record);
    }
    Ok(CorporateActionQueryIdentitySelection {
        retained: retained.into_boxed_slice(),
        selections: selections.into_boxed_slice(),
        definitions: definitions.into_boxed_slice(),
        source_receipt_digest,
        economic_query: false,
        event_identities: Box::new([]),
        event_selections: Box::new([]),
        event_definitions: Box::new([]),
    })
}

/// Existing ingest precommit authority extended with the exact immutable definition positions.
#[derive(Debug)]
pub struct CorporateActionQueryIdentityPrecommitAuthority {
    reader: MarketDataInstrumentReadCapability,
    selection: CorporateActionQueryIdentitySelection,
    inner: Arc<dyn IngestPrecommitAuthority>,
    deadline: Instant,
    cancellation: CancellationToken,
    binding_digest: EvidenceDigest,
    event_identities: Box<[CorporateActionEventInstrumentIdentity]>,
    event_selections: Option<Box<[MarketDataProviderIdentitySelection]>>,
    // Set only by the authenticated economic-query child constructor.
    require_current_definitions: bool,
}
impl CorporateActionQueryIdentityPrecommitAuthority {
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    /// Closes the exact native subject/successor set with original opaque catalog selections.
    /// Repeated uses of the same selection are coalesced; omissions and unrelated extras fail.
    /// Until this succeeds, a nonempty native event set cannot authorize publication.
    pub fn with_event_identities(
        mut self,
        mut selections: Vec<MarketDataProviderIdentitySelection>,
    ) -> Result<Self, CorporateActionQueryIdentityError> {
        if selections.len() > 32_000 {
            return Err(CorporateActionQueryIdentityError::ResourceBound);
        }
        selections.sort_unstable_by_key(|selection| selection.selection_digest().bytes());
        selections.dedup();
        if selections.len() != self.event_identities.len() {
            return Err(CorporateActionQueryIdentityError::Mismatch);
        }
        for (retained, selection) in self.event_identities.iter().zip(&selections) {
            verify_event_identity(
                &self.reader,
                retained,
                selection,
                self.deadline,
                &self.cancellation,
            )?;
        }
        self.event_selections = Some(selections.into_boxed_slice());
        self.validate_precommit().map_err(|error| match error {
            IngestError::Cancelled => CorporateActionQueryIdentityError::Catalog(
                MarketDataInstrumentCatalogError::Cancelled,
            ),
            IngestError::DeadlineExceeded => CorporateActionQueryIdentityError::Catalog(
                MarketDataInstrumentCatalogError::DeadlineExceeded,
            ),
            _ => CorporateActionQueryIdentityError::Mismatch,
        })?;
        Ok(self)
    }

    fn validate_query_precommit(&self) -> Result<(), IngestError> {
        self.inner.validate_precommit()?;
        for selection in &self.selection.selections {
            self.reader
                .verify_provider_identity_selection_restart(
                    selection,
                    self.deadline,
                    &self.cancellation,
                )
                .map_err(precommit_error)?;
        }
        Ok(())
    }
}

/// Checks every retained coordinate against the actual original catalog selection and definition.
/// Historical aliases use their original economic clock, never today's current definition.
fn verify_event_identity(
    reader: &MarketDataInstrumentReadCapability,
    retained: &CorporateActionEventInstrumentIdentity,
    selection: &MarketDataProviderIdentitySelection,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<MarketDataInstrumentRecord, CorporateActionQueryIdentityError> {
    if !retained.valid_for_event(retained.selection.knowledge_at)
        || &retained.source_id != selection.query().source_id()
        || &retained.provider_instrument_id != selection.query().provider_instrument_id()
        || retained.selection != retained_identity(selection)?
        || retained.resolution_receipt_digest != selection.resolution_receipt_digest()
        || !selection
            .exact_receipt()?
            .matching_venues()
            .contains(&retained.venue_id)
    {
        return Err(CorporateActionQueryIdentityError::Mismatch);
    }
    let record = reader.read_selected_provider_definition(selection, deadline, cancellation)?;
    let definition = record.definition();
    let effective_at = retained.selection.effective_at;
    let provider = definition
        .provider_identity_at(
            &retained.source_id,
            &retained.provider_instrument_id,
            effective_at,
        )
        .ok_or(CorporateActionQueryIdentityError::Mismatch)?;
    if provider.observed_at() > retained.selection.knowledge_at
        || provider
            .source_timestamp()
            .is_some_and(|time| time > retained.selection.knowledge_at)
    {
        return Err(CorporateActionQueryIdentityError::Mismatch);
    }
    if definition.instrument_id() != retained.selection.instrument_id
        || record.revision_digest() != retained.selection.definition_revision_digest
        || definition.effective_interval().starts_at() > effective_at
        || definition
            .effective_interval()
            .ends_at()
            .is_some_and(|end| effective_at >= end)
        || !definition.venue_mappings().iter().any(|venue| {
            venue.venue_id() == &retained.venue_id && venue.venue_symbol() == &retained.venue_symbol
        })
    {
        return Err(CorporateActionQueryIdentityError::Mismatch);
    }
    Ok(record)
}

impl IngestPrecommitAuthority for CorporateActionQueryIdentityPrecommitAuthority {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        let selections = self
            .event_selections
            .as_ref()
            .ok_or(IngestError::PublicationAuthorityRevoked)?;
        self.validate_query_precommit()?;
        for selection in selections {
            self.reader
                .verify_provider_identity_selection_restart(
                    selection,
                    self.deadline,
                    &self.cancellation,
                )
                .map_err(precommit_error)?;
        }
        Ok(())
    }
    fn validate_catalog_precommit(&self, catalog: &CatalogAuthority) -> Result<(), IngestError> {
        let selections = self
            .event_selections
            .as_ref()
            .ok_or(IngestError::PublicationAuthorityRevoked)?;
        self.inner.validate_catalog_precommit(catalog)?;
        if self.require_current_definitions {
            for record in &self.selection.definitions {
                self.reader
                    .require_current_in_catalog(catalog, record, self.deadline, &self.cancellation)
                    .map_err(precommit_error)?;
            }
        }
        // Replay query resolutions as well: a newly conflicting alias must revoke authority even
        // when the original instrument's current definition itself has not changed.
        for selection in self.selection.selections.iter().chain(selections.iter()) {
            self.reader
                .verify_provider_identity_selection_in_catalog(
                    catalog,
                    selection,
                    self.deadline,
                    &self.cancellation,
                )
                .map_err(precommit_error)?;
        }
        Ok(())
    }
}
fn precommit_error(error: MarketDataInstrumentCatalogError) -> IngestError {
    match error {
        MarketDataInstrumentCatalogError::Cancelled => IngestError::Cancelled,
        MarketDataInstrumentCatalogError::DeadlineExceeded => IngestError::DeadlineExceeded,
        _ => IngestError::PublicationAuthorityRevoked,
    }
}
#[derive(serde::Deserialize)]
struct QuerySidecar {
    coverage: QueryCoverage,
}
#[derive(serde::Deserialize)]
struct QueryCoverage {
    request: QueryRequest,
    query_instruments: Vec<CorporateActionQueryInstrumentIdentity>,
}
#[derive(serde::Deserialize)]
struct QueryRequest {
    symbols: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum CorporateActionQueryIdentityError {
    #[error("source query has no exact canonical instrument selection")]
    MissingIdentity,
    #[error("source query identity differs from its original catalog or capture evidence")]
    Mismatch,
    #[error("source query identity exceeds its finite admission bound")]
    ResourceBound,
    #[error(transparent)]
    Catalog(#[from] MarketDataInstrumentCatalogError),
}

// Conservative aggregate retained-event admission. Four times the serialized payload plus a
// fixed per-entry charge covers collection/object overhead without building a second JSON buffer.
struct EventReadBudget {
    remaining: usize,
}
impl EventReadBudget {
    fn charge(&mut self, bytes: usize) -> std::io::Result<()> {
        self.remaining = self
            .remaining
            .checked_sub(bytes)
            .ok_or_else(|| std::io::Error::other("event identity byte bound"))?;
        Ok(())
    }
}
impl std::io::Write for EventReadBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.charge(
            bytes
                .len()
                .checked_mul(4)
                .ok_or_else(|| std::io::Error::other("event identity byte bound"))?,
        )?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
