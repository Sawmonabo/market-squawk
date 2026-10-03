//! Original economic-date alias selection; no current ticker fallback.
use super::*;
use market_squawk_domain::CorporateActionEconomicSourceScope;
impl MarketDataInstrumentReadCapability {
    /// Selects aliases at the actual economic session using original pre-request knowledge.
    pub fn select_current_ordinary_query_identities(
        &self,
        source: SourceId,
        instruments: Vec<InstrumentId>,
        knowledge_at: Timestamp,
        effective_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CorporateActionQueryIdentitySelection, CorporateActionQueryIdentityError> {
        if instruments.is_empty() || instruments.len() > 32 || effective_at > knowledge_at {
            return Err(CorporateActionQueryIdentityError::Mismatch);
        }
        let population = self.pin_population_as_of(
            MarketDataInstrumentPopulationQuery::try_new(instruments, knowledge_at, effective_at)?,
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
                        && identity.validity().starts_at() <= effective_at
                        && identity
                            .validity()
                            .ends_at()
                            .is_none_or(|end| effective_at < end)
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
                knowledge_at,
                effective_at,
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
        let mut selection = assemble(selected, None)?;
        selection.economic_query = true;
        Ok(selection)
    }
}

impl CorporateActionQueryIdentitySelection {
    /// Joins this original economic selection to the exact native capture before publication.
    pub fn current_ordinary_publication_authority(
        self,
        reader: MarketDataInstrumentReadCapability,
        binding: &SealedProviderCaptureBinding,
        inner: Arc<dyn IngestPrecommitAuthority>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CorporateActionQueryIdentityPrecommitAuthority, CorporateActionQueryIdentityError>
    {
        use crate::corporate_actions::current_ordinary::source::{
            CurrentOrdinarySidecar, current_ordinary_event_identities, request_identity,
            validate_scope,
        };
        binding
            .validate()
            .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?;
        let capture = binding.capture_evidence();
        let [page] = capture.pages() else {
            return Err(CorporateActionQueryIdentityError::Mismatch);
        };
        let native_sidecar = binding
            .native_lineage()
            .batch_sidecar()
            .ok_or(CorporateActionQueryIdentityError::Mismatch)?
            .semantic_payload();
        if native_sidecar.len() > 16 * 1024 * 1024 {
            return Err(CorporateActionQueryIdentityError::ResourceBound);
        }
        let sidecar: CurrentOrdinarySidecar = serde_json::from_slice(native_sidecar)
            .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?;
        validate_scope(&sidecar.query_scope, page.received_at())
            .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?;
        let (url, request) = request_identity(&sidecar.query_scope)
            .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?;
        if !self.economic_query
            || binding.native_lineage().schema().implementation()!=market_squawk_sources::ProviderNativeLineageImplementation::TiingoCorporateActionsV1
            || capture.source_id().as_str()!="tiingo-starter"
            || capture.terminal()!=market_squawk_sources::ProviderCaptureTerminalDisposition::StandaloneResponse
            || sidecar.version!=1 || sidecar.request_url!=url || sidecar.request_identity!=request
            || capture.request_set_identity()!=request || page.request_identity()!=request
            || sidecar.query_scope.query_instruments.as_slice()!=self.retained.as_ref()
            || sidecar.query_scope.dataset!=*capture.dataset()
            || sidecar.query_scope.capture_observation_digest!=capture.observation_digest()
            || sidecar.query_scope.sealed_capture_receipt_digest!=binding.sealed_capture_receipt_digest()
            || sidecar.received_at!=page.received_at()
            || self.selections.iter().any(|s|s.query().source_id()!=capture.source_id())
        {return Err(CorporateActionQueryIdentityError::Mismatch);}
        let event_identities = current_ordinary_event_identities(
            binding
                .native_lineage()
                .rows()
                .iter()
                .map(|row| row.semantic_payload().as_ref()),
        )
        .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?;
        if event_identities
            .iter()
            .any(|id| id.source_id != *capture.source_id())
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
            require_current_definitions: false,
        };
        authority
            .validate_query_precommit()
            .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?;
        Ok(authority)
    }
}
impl MarketDataInstrumentReadCapability {
    /// Private replay accepts scope only after the original reader validates native membership.
    pub(crate) fn reopen_current_ordinary_query_identities(
        &self,
        scope: &CorporateActionEconomicSourceScope,
        events: &[CorporateActionEventInstrumentIdentity],
        received_at: Timestamp,
        knowledge_cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CorporateActionQueryIdentitySelection, CorporateActionQueryIdentityError> {
        use crate::corporate_actions::current_ordinary::source::validate_scope;
        validate_scope(scope, received_at)
            .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?;
        let source = SourceId::try_from("tiingo-starter")
            .map_err(|_| CorporateActionQueryIdentityError::Mismatch)?;
        let mut selected = Vec::new();
        for retained in &scope.query_instruments {
            if retained.knowledge_at > knowledge_cutoff {
                return Err(CorporateActionQueryIdentityError::Mismatch);
            }
            let query = MarketDataProviderIdentityQuery::try_new(
                source.clone(),
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
            if retained_identity(&selection)? != *retained {
                return Err(CorporateActionQueryIdentityError::Mismatch);
            }
            selected.push((retained.clone(), selection, record));
        }
        let mut result = assemble(selected, None)?;
        result.economic_query = true;
        let mut event_budget = EventReadBudget {
            remaining: 64 * 1024 * 1024,
        };
        let mut selections = Vec::new();
        let mut definitions = Vec::new();
        for retained in events {
            if retained.source_id != source || !retained.valid_for_event(knowledge_cutoff) {
                return Err(CorporateActionQueryIdentityError::Mismatch);
            }
            let query = MarketDataProviderIdentityQuery::try_new(
                source.clone(),
                retained.provider_instrument_id.clone(),
                retained.selection.knowledge_at,
                retained.selection.effective_at,
            )?;
            let selection = self
                .select_provider_identity_as_of(query, deadline, cancellation)?
                .ok_or(CorporateActionQueryIdentityError::MissingIdentity)?;
            let record = verify_event_identity(self, retained, &selection, deadline, cancellation)?;
            serde_json::to_writer(&mut event_budget, &(retained, record.definition()))
                .map_err(|_| CorporateActionQueryIdentityError::ResourceBound)?;
            event_budget
                .charge(1024)
                .map_err(|_| CorporateActionQueryIdentityError::ResourceBound)?;
            selections.push(selection);
            definitions.push(record);
        }
        result.event_identities = events.into();
        result.event_selections = selections.into_boxed_slice();
        result.event_definitions = definitions.into_boxed_slice();
        Ok(result)
    }
}
