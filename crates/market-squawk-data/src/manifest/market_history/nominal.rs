//! Native daily-date history through the existing immutable publication authority.
use super::*;

const SOURCE: &str = "tiingo-starter";
const RAW_FEED: &str = "tiingo-starter-daily-eod-raw";
const ALL_FEED: &str = "tiingo-starter-daily-eod-adjusted-all-v1";
const INTERVAL: &str = "tiingo-calendar-day";
const RULESET: &str = "tiingo-eod-native-nominal-date-v1";

pub(super) fn date_key(date: CalendarDate) -> i64 {
    i64::from(date.year()) * 10_000 + i64::from(date.month()) * 100 + i64::from(date.day())
}
pub(super) fn hash_date(hash: &mut Sha256, date: CalendarDate) {
    hash.update(date.year().to_be_bytes());
    hash.update([date.month(), date.day()]);
}
fn nominal_date_digest(
    graph: &CompleteMarketBarDateWindowsV1,
) -> Result<Sha256Digest, ManifestCatalogError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/market-bar-history-original-dates/v1");
    hash.update((graph.sessions().len() as u64).to_be_bytes());
    for session in graph.sessions() {
        hash_date(&mut hash, session.date);
    }
    nonzero_sha256(hash.finalize().into())
}
fn nominal_bars_digest<'a>(
    bars: impl Iterator<Item = &'a MarketBarObservation>,
    count: usize,
) -> Result<Sha256Digest, ManifestCatalogError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/market-bar-history-nominal-bars/v1");
    hash.update((count as u64).to_be_bytes());
    let mut actual = 0;
    for bar in bars {
        let date = bar
            .time_semantics()
            .nominal_daily_date()
            .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?;
        hash_date(&mut hash, date.date());
        let payload = CanonicalObservationPayload::try_from_observation(
            &ResearchObservation::MarketBar(bar.clone()),
        )
        .map_err(|_| ManifestCatalogError::MarketBarHistoryMismatch)?;
        hash_evidence(&mut hash, payload.identity());
        actual += 1;
    }
    if actual != count {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    }
    nonzero_sha256(hash.finalize().into())
}

impl CompleteMarketBarHistoryRequest {
    /// Exact original civil dates. No aggregation instant is inferred.
    #[allow(clippy::too_many_arguments)]
    pub fn try_exact_nominal(
        instrument_id: InstrumentId,
        start: CalendarDate,
        end: CalendarDate,
        provider_instrument_id: ProviderInstrumentId,
        venue_id: VenueId,
        feed: SourceIdentifier,
        interval: SourceIdentifier,
        adjustment: MarketBarAdjustment,
        ruleset: SourceIdentifier,
        knowledge_cutoff: Timestamp,
        manifest: DatasetManifestRef,
    ) -> Result<Self, ManifestCatalogError> {
        let mut request = Self::try_latest_nominal(
            instrument_id,
            start,
            end,
            provider_instrument_id,
            venue_id,
            feed,
            interval,
            adjustment,
            ruleset,
            knowledge_cutoff,
        )?;
        request.exact_manifest = Some(manifest);
        Ok(request)
    }
    /// Latest complete publication for the exact source-authored date window.
    #[allow(clippy::too_many_arguments)]
    pub fn try_latest_nominal(
        instrument_id: InstrumentId,
        start: CalendarDate,
        end: CalendarDate,
        provider_instrument_id: ProviderInstrumentId,
        venue_id: VenueId,
        feed: SourceIdentifier,
        interval: SourceIdentifier,
        adjustment: MarketBarAdjustment,
        ruleset: SourceIdentifier,
        knowledge_cutoff: Timestamp,
    ) -> Result<Self, ManifestCatalogError> {
        if start > end
            || interval.as_str() != INTERVAL
            || ruleset.as_str() != RULESET
            || !matches!(
                (adjustment, feed.as_str()),
                (MarketBarAdjustment::Raw, RAW_FEED) | (MarketBarAdjustment::All, ALL_FEED)
            )
        {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        Ok(Self {
            instrument_id,
            requested_start: None,
            requested_end: None,
            requested_dates: Some((start, end)),
            provider_instrument_id,
            venue_id,
            feed,
            interval,
            adjustment,
            timestamp_basis: None,
            session_kind: None,
            session_ruleset: ruleset,
            knowledge_cutoff,
            exact_manifest: None,
            surface_requirement: MarketHistoryPriceSurfaceRequirement::SelectedOnly,
        })
    }
    pub const fn requested_dates(&self) -> Option<(CalendarDate, CalendarDate)> {
        self.requested_dates
    }
    pub const fn surface_requirement(&self) -> MarketHistoryPriceSurfaceRequirement {
        self.surface_requirement
    }
    /// Requires the selected raw surface and independently retained adjusted bars on every date.
    pub fn try_with_surface_requirement(
        mut self,
        requirement: MarketHistoryPriceSurfaceRequirement,
    ) -> Result<Self, ManifestCatalogError> {
        if requirement == MarketHistoryPriceSurfaceRequirement::RawWithAll
            && (self.adjustment != MarketBarAdjustment::Raw || self.requested_dates.is_none())
        {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        self.surface_requirement = requirement;
        Ok(self)
    }
}
impl MarketBarHistoryPublicationReceipt {
    pub fn requested_dates(&self) -> Option<(CalendarDate, CalendarDate)> {
        self.date_windows
            .as_ref()
            .map(CompleteMarketBarDateWindowsV1::requested_dates)
    }
    pub const fn date_windows(&self) -> Option<&CompleteMarketBarDateWindowsV1> {
        self.date_windows.as_ref()
    }
    pub const fn origin_record_count(&self) -> u32 {
        self.origin_record_count
    }
    pub(super) fn validate_nominal_bars(
        &self,
        bars: &[MarketBarObservation],
    ) -> Result<(), ManifestCatalogError> {
        let graph = self
            .date_windows
            .as_ref()
            .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?;
        if bars.len() != graph.sessions().len()
            || bars.len() != self.expected_bar_count
            || nominal_date_digest(graph)? != self.expected_timestamp_set_digest
        {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        for (bar, session) in bars.iter().zip(graph.sessions()) {
            validate_nominal_bar(bar, session, graph, &self.source_id, self.adjustment)?;
            let provenance = bar.context().provenance();
            if provenance.received_at() > self.max_received_at
                || provenance.ingested_at() > self.max_ingested_at
                || provenance
                    .availability()
                    .conservative_available_at()
                    .is_none_or(|clock| clock > self.max_available_at)
            {
                return Err(ManifestCatalogError::MarketBarHistoryMismatch);
            }
        }
        if nominal_bars_digest(bars.iter(), bars.len())? != self.bar_set_digest {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        Ok(())
    }
    pub(crate) fn validate_companion_bars(
        &self,
        bars: &[MarketBarObservation],
    ) -> Result<(), ManifestCatalogError> {
        let graph = self
            .date_windows
            .as_ref()
            .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?;
        let adjustment = if self.adjustment == MarketBarAdjustment::Raw {
            MarketBarAdjustment::All
        } else {
            MarketBarAdjustment::Raw
        };
        if bars.len() != graph.sessions().len() {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        for (bar, session) in bars.iter().zip(graph.sessions()) {
            validate_nominal_bar(bar, session, graph, &self.source_id, adjustment)?;
        }
        let expected = if adjustment == MarketBarAdjustment::All {
            self.all_bar_set_digest
        } else {
            self.raw_bar_set_digest
        };
        if let Some(expected) = expected {
            if nominal_bars_digest(bars.iter(), bars.len())? != expected {
                return Err(ManifestCatalogError::MarketBarHistoryMismatch);
            }
        }
        Ok(())
    }
}
impl CompleteMarketBarHistorySelection {
    pub const fn surface_requirement(&self) -> MarketHistoryPriceSurfaceRequirement {
        self.surface_requirement
    }
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }
}
fn validate_nominal_bar(
    bar: &MarketBarObservation,
    session: &market_squawk_sources::CompleteMarketBarDateSessionV1,
    graph: &CompleteMarketBarDateWindowsV1,
    source: &SourceId,
    adjustment: MarketBarAdjustment,
) -> Result<(), ManifestCatalogError> {
    let invalid = || ManifestCatalogError::MarketBarHistoryMismatch;
    let provenance = bar.context().provenance();
    let available = match provenance.availability() {
        ResearchAvailabilityEvidence::LocalFirstObserved { observed_at } => *observed_at,
        _ => return Err(invalid()),
    };
    let nominal = bar
        .time_semantics()
        .nominal_daily_date()
        .ok_or_else(invalid)?;
    if bar.time_semantics() != &session.time
        || nominal.date() != session.date
        || nominal.ruleset().as_str() != RULESET
        || bar.context().time().effective()
            != &ResearchTemporalCoordinate::calendar_date(session.date)
        || bar.context().time().published().is_some()
        || bar.context().time().superseded().is_some()
        || provenance.instrument_id() != Some(graph.instrument_id())
        || provenance.venue_id() != Some(graph.venue_id())
        || provenance.source_id() != source
        || provenance.source_timestamp().is_some()
        || provenance.quality() != DataQuality::Aggregated
        || bar.provider_instrument_id() != graph.provider_instrument_id()
        || bar.feed().as_str()
            != if adjustment == MarketBarAdjustment::Raw {
                RAW_FEED
            } else {
                ALL_FEED
            }
        || bar.interval() != graph.interval()
        || bar.adjustment() != adjustment
        || bar.currency() != graph.normalization().currency
        || available != provenance.received_at()
        || available > provenance.ingested_at()
    {
        return Err(invalid());
    }
    Ok(())
}

impl MarketBarHistoryPublicationCandidate {
    pub(super) fn try_from_nominal_batch(
        batch: &ExtractionBatch,
        observations: &[ResearchObservation],
        prepared: &PreparedProviderCaptureBinding,
        graph: &CompleteMarketBarDateWindowsV1,
    ) -> Result<Option<Self>, ManifestCatalogError> {
        let invalid = || ManifestCatalogError::MarketBarHistoryMismatch;
        let capture = prepared.evidence.capture();
        if capture.source_id().as_str() != SOURCE
            || capture.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
            || graph.interval().as_str() != INTERVAL
            || graph.sessions().is_empty()
            || batch.records().len() != observations.len()
            || observations.len() > 4 * MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS
            || prepared.evidence.rows().len() != observations.len()
            || capture.pages().len() != graph.windows().len() + 1
        {
            return Err(invalid());
        }
        let mut raw = Vec::new();
        let mut all = Vec::new();
        raw.try_reserve_exact(graph.sessions().len())
            .map_err(|_| invalid())?;
        all.try_reserve_exact(graph.sessions().len())
            .map_err(|_| invalid())?;
        let mut clocks = None;
        let mut action_suffix = false;
        for ((observation, record), native_row) in observations
            .iter()
            .zip(batch.records())
            .zip(prepared.evidence.rows())
        {
            let provenance = match observation {
                ResearchObservation::MarketBar(bar) => bar.context().provenance(),
                ResearchObservation::CorporateAction(action) => action.context().provenance(),
                _ => return Err(invalid()),
            };
            let available = match record.availability() {
                ExtractionAvailabilityEvidence::LocalFirstObserved { observed_at } => *observed_at,
                _ => return Err(invalid()),
            };
            let observed = match provenance.availability() {
                ResearchAvailabilityEvidence::LocalFirstObserved { observed_at } => *observed_at,
                _ => return Err(invalid()),
            };
            if available != observed
                || available != provenance.received_at()
                || available > provenance.ingested_at()
                || provenance.source_id() != capture.source_id()
                || provenance.instrument_id() != Some(graph.instrument_id())
                || provenance.source_timestamp().is_some()
                || record.published_time().is_some()
                || record.superseded_time().is_some()
                || native_row.capture_page_ordinal() == 0
                || native_row.segment_ordinal() != native_row.capture_page_ordinal()
                || native_row.physical_frame_ordinal() != 0
                || capture
                    .pages()
                    .get(native_row.capture_page_ordinal() as usize)
                    .is_none_or(|page| page.received_at() != provenance.received_at())
                || graph
                    .windows()
                    .get(native_row.capture_page_ordinal() as usize - 1)
                    .is_none_or(|window| {
                        window.ingested_at != provenance.ingested_at()
                            || !graph.sessions().iter().any(|session| {
                                session.date >= window.start_date
                                    && session.date <= window.end_date
                                    && match provenance.payload_reference() {
                                        PayloadReference::ContentHash(digest) => {
                                            digest.digest() == session.row_digest.bytes()
                                        }
                                        _ => false,
                                    }
                            })
                    })
            {
                return Err(invalid());
            }
            clocks = Some(match clocks {
                None => (
                    available,
                    provenance.received_at(),
                    provenance.ingested_at(),
                ),
                Some((a, r, i)) => (
                    available.max(a),
                    provenance.received_at().max(r),
                    provenance.ingested_at().max(i),
                ),
            });
            match observation {
                ResearchObservation::MarketBar(bar) if !action_suffix => match bar.adjustment() {
                    MarketBarAdjustment::Raw => raw.push(bar),
                    MarketBarAdjustment::All => all.push(bar),
                    _ => return Err(invalid()),
                },
                ResearchObservation::CorporateAction(_) => {
                    action_suffix = true;
                }
                _ => return Err(invalid()),
            }
        }
        if raw.len() > graph.sessions().len() || all.len() > graph.sessions().len() {
            return Err(invalid());
        }
        if raw.len() != graph.sessions().len() && all.len() != graph.sessions().len() {
            return Ok(None);
        }
        let mut previous = None;
        for bar in &raw {
            let date = bar
                .time_semantics()
                .nominal_daily_date()
                .ok_or_else(invalid)?
                .date();
            let index = graph
                .sessions()
                .binary_search_by_key(&date, |session| session.date)
                .map_err(|_| invalid())?;
            if previous.is_some_and(|previous| previous >= date) {
                return Err(invalid());
            }
            previous = Some(date);
            validate_nominal_bar(
                bar,
                &graph.sessions()[index],
                graph,
                capture.source_id(),
                MarketBarAdjustment::Raw,
            )?;
        }
        // Adjusted absence never supplies a raw price. Every present adjusted row is checked on
        // its own original date; RawWithAll admission later requires exact full cardinality.
        let mut previous = None;
        for bar in &all {
            let date = bar
                .time_semantics()
                .nominal_daily_date()
                .ok_or_else(invalid)?
                .date();
            let index = graph
                .sessions()
                .binary_search_by_key(&date, |session| session.date)
                .map_err(|_| invalid())?;
            if previous.is_some_and(|value| value >= date) {
                return Err(invalid());
            }
            previous = Some(date);
            validate_nominal_bar(
                bar,
                &graph.sessions()[index],
                graph,
                capture.source_id(),
                MarketBarAdjustment::All,
            )?;
        }
        let (max_available_at, max_received_at, max_ingested_at) = clocks.ok_or_else(invalid)?;
        let all_bar_set_digest = if all.len() == graph.sessions().len() {
            Some(nominal_bars_digest(all.iter().copied(), all.len())?)
        } else {
            None
        };
        let raw_bar_set_digest = if raw.len() == graph.sessions().len() {
            Some(nominal_bars_digest(raw.iter().copied(), raw.len())?)
        } else {
            None
        };
        let adjustment = if raw_bar_set_digest.is_some() {
            MarketBarAdjustment::Raw
        } else {
            MarketBarAdjustment::All
        };
        let first_component = capture
            .request_graph_components()
            .get(1)
            .ok_or_else(invalid)?;
        // Component zero is metadata, never a calendar. Nominal graphs carry no timestamped
        // calendar component: the immutable graph separately binds original calendar authority.
        Ok(Some(Self {
            binding_digest: sha256_evidence(prepared.binding_digest())?,
            source_id: capture.source_id().clone(),
            capture_receipt_digest: sha256_evidence(
                prepared.evidence.sealed_capture_receipt_digest(),
            )?,
            capture_content_digest: sha256_evidence(capture.content_digest())?,
            capture_observation_digest: sha256_evidence(capture.observation_digest())?,
            provider_dataset: capture.dataset().clone(),
            instrument_id: graph.instrument_id(),
            instrument_revision_digest: sha256_evidence(graph.instrument_revision_digest())?,
            admitted_plan_digest: sha256_evidence(graph.admitted_plan_digest())?,
            provider_instrument_id: graph.provider_instrument_id().clone(),
            venue_id: graph.venue_id().clone(),
            feed: SourceIdentifier::try_from(if adjustment == MarketBarAdjustment::Raw {
                RAW_FEED
            } else {
                ALL_FEED
            })
            .map_err(|_| invalid())?,
            interval: graph.interval().clone(),
            adjustment,
            timestamp_basis: None,
            session_kind: None,
            session_ruleset: SourceIdentifier::try_from(RULESET).map_err(|_| invalid())?,
            graph_purpose: graph.graph_purpose().clone(),
            requested_start: None,
            requested_end: None,
            coverage_first: None,
            coverage_last: None,
            coverage_last_complete: None,
            expected_bar_count: graph.sessions().len(),
            expected_timestamp_set_digest: nominal_date_digest(graph)?,
            bar_set_digest: if adjustment == MarketBarAdjustment::Raw {
                raw_bar_set_digest
            } else {
                all_bar_set_digest
            }
            .ok_or_else(invalid)?,
            completeness_evidence_digest: sha256_evidence(graph.completeness_evidence())?,
            market_bar_component_ordinal: Some(first_component.ordinal()),
            market_bar_component_content_digest: Some(sha256_evidence(
                first_component.content_digest(),
            )?),
            market_bar_component_page_count: Some(first_component.page_count().get()),
            session_calendar_component_ordinal: None,
            session_calendar_component_content_digest: None,
            session_calendar_component_page_count: None,
            currency: graph.normalization().currency,
            max_available_at,
            max_received_at,
            max_ingested_at,
            date_windows: Some(graph.clone()),
            origin_record_count: u32::try_from(observations.len()).map_err(|_| invalid())?,
            raw_bar_count: u32::try_from(raw.len()).map_err(|_| invalid())?,
            raw_bar_set_digest,
            all_bar_count: u32::try_from(all.len()).map_err(|_| invalid())?,
            all_bar_set_digest,
        }))
    }
}

pub(super) fn validate_nominal_instrument(
    connection: &Connection,
    graph: &CompleteMarketBarDateWindowsV1,
    source: &SourceId,
    admitted_at: Timestamp,
) -> Result<AssetClass, ManifestCatalogError> {
    let at = graph.normalization().resolved_at;
    if at > admitted_at {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    }
    let asset = validate_exact_instrument_revision(
        connection,
        sha256_evidence(graph.instrument_revision_digest())?,
        graph.instrument_id(),
        source,
        graph.provider_instrument_id(),
        graph.normalization().currency,
        at,
        at,
        admitted_at,
    )?;
    if graph.normalization().is_exchange_traded_fund != (asset == AssetClass::Fund) {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    }
    Ok(asset)
}

pub(super) fn nominal_wire_valid(wire: &MarketBarHistoryReceiptWire) -> bool {
    let Some(graph) = &wire.date_windows else {
        return false;
    };
    wire.source_id.as_str() == SOURCE
        && wire.interval.as_str() == INTERVAL
        && wire.feed.as_str()
            == if wire.adjustment == MarketBarAdjustment::Raw {
                RAW_FEED
            } else {
                ALL_FEED
            }
        && matches!(
            wire.adjustment,
            MarketBarAdjustment::Raw | MarketBarAdjustment::All
        )
        && wire.session_ruleset.as_str() == RULESET
        && wire.timestamp_basis.is_none()
        && wire.session_kind.is_none()
        && wire.requested_start_ns.is_none()
        && wire.requested_end_ns.is_none()
        && wire.coverage_first_ns.is_none()
        && wire.coverage_last_ns.is_none()
        && wire.coverage_last_complete_ns.is_none()
        && wire.session_calendar_component_ordinal.is_none()
        && wire.session_calendar_component_content_digest.is_none()
        && wire.session_calendar_component_page_count.is_none()
        && wire.expected_bar_count as usize == graph.sessions().len()
        && wire.raw_bar_count <= wire.expected_bar_count
        && wire.raw_bar_set_digest.is_some() == (wire.raw_bar_count == wire.expected_bar_count)
        && wire.all_bar_count <= wire.expected_bar_count
        && wire.all_bar_set_digest.is_some() == (wire.all_bar_count == wire.expected_bar_count)
        && wire.origin_record_count >= wire.raw_bar_count + wire.all_bar_count
        && wire.origin_record_count as usize <= 4 * MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS
}
pub(super) fn validate_nominal_capture(
    capture: &ProviderCaptureSetReceipt,
    wire: &MarketBarHistoryReceiptWire,
    graph: &CompleteMarketBarDateWindowsV1,
) -> Result<(), ManifestCatalogError> {
    let invalid = || ManifestCatalogError::CorruptCatalog;
    if !nominal_wire_valid(wire)
        || capture.semantic_binding()
            != Some(&ProviderCaptureSemanticBinding::CompleteMarketBarDateWindowsV1(graph.clone()))
        || capture.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
        || capture.source_id() != &wire.source_id
        || capture.dataset() != &wire.provider_dataset
        || capture.content_digest().bytes() != wire.capture_content_digest
        || capture.observation_digest().bytes() != wire.capture_observation_digest
        || capture.metadata_revision() != &graph.normalization().source_contract_revision
        || graph.instrument_id() != wire.instrument_id
        || graph.instrument_revision_digest().bytes() != wire.instrument_revision_digest
        || graph.admitted_plan_digest().bytes() != wire.admitted_plan_digest
        || graph.provider_instrument_id() != &wire.provider_instrument_id
        || graph.venue_id() != &wire.venue_id
        || graph.interval() != &wire.interval
        || graph.graph_purpose() != &wire.graph_purpose
        || graph.normalization().currency != wire.currency
        || graph.completeness_evidence().bytes() != wire.completeness_evidence_digest
        || nominal_date_digest(graph)?.bytes() != wire.expected_timestamp_set_digest
        || capture.pages().len() != graph.windows().len() + 1
        || capture.request_graph_components().len() != capture.pages().len()
    {
        return Err(invalid());
    }
    for (index, (component, page)) in capture
        .request_graph_components()
        .iter()
        .zip(capture.pages())
        .enumerate()
    {
        if component.ordinal() as usize != index
            || component.first_page_ordinal() as usize != index
            || component.page_count().get() != 1
            || component.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
            || component.source_id() != capture.source_id()
            || component.metadata_revision() != capture.metadata_revision()
            || component.request_set_identity() != page.request_identity()
            || page.received_at().unix_nanos() > wire.max_received_at_ns
        {
            return Err(invalid());
        }
        if index > 0 {
            let window = &graph.windows()[index - 1];
            if component.request_set_identity() != window.request_identity
                || page.received_at() > window.decoded_at
                || window.ingested_at.unix_nanos() > wire.max_ingested_at_ns
            {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

pub(super) fn select_nominal_history(
    connection: &Connection,
    max_objects: usize,
    request: &CompleteMarketBarHistoryRequest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<CompleteMarketBarHistorySelection>, ManifestCatalogError> {
    check_operation(deadline, cancellation)?;
    let (start, end) = request
        .requested_dates
        .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?;
    let schema = DatasetSchemaRegistry::local().canonical_research_observations()?;
    if request
        .exact_manifest
        .as_ref()
        .is_some_and(|pin| pin.schema() != &schema)
    {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    }
    let exact = request.exact_manifest.as_ref();
    let mut statement=connection.prepare(
        "SELECT selected_generation.dataset_id, selected_generation.manifest_version, selected_generation.content_hash,
                publication.publication_receipt_digest, publication.published_at_ns, publication.origin_generation_sequence
         FROM analytical_available_generations AS selected_generation
         JOIN dataset_manifests AS selected_manifest ON selected_manifest.manifest_id=selected_generation.anchor_manifest_id
         JOIN artifacts AS selected_artifact ON selected_artifact.artifact_id=selected_manifest.artifact_id
         JOIN ingest_runs AS selected_run ON selected_run.run_id=selected_artifact.run_id
         JOIN analytical_generation_market_bar_history_inputs AS history_input ON history_input.generation_sequence=selected_generation.generation_sequence
         JOIN market_bar_history_publications AS publication USING(publication_receipt_digest)
         JOIN ingest_runs AS origin_run ON origin_run.run_id=publication.origin_run_id
         JOIN provider_capture_bindings AS binding ON binding.binding_digest=publication.binding_digest
         JOIN provider_raw_observations AS capture ON capture.capture_observation_digest=binding.capture_observation_digest
         JOIN analytical_generation_provider_capture_bindings AS selected_capture
           ON selected_capture.generation_sequence=selected_generation.generation_sequence AND selected_capture.binding_digest=publication.binding_digest
         WHERE publication.instrument_id=?1 AND publication.source_id='tiingo-starter'
           AND publication.requested_start_date=?2 AND publication.requested_end_date=?3
           AND publication.provider_instrument_id=?4 AND publication.venue_id=?5
           AND publication.bar_interval='tiingo-calendar-day' AND publication.feed IN ('tiingo-starter-daily-eod-raw','tiingo-starter-daily-eod-adjusted-all-v1')
           AND publication.adjustment IN ('raw','all') AND publication.timestamp_basis IS NULL AND publication.session_kind IS NULL
           AND publication.session_ruleset='tiingo-eod-native-nominal-date-v1'
           AND publication.asset_class IN ('equity','fund')
           AND selected_generation.schema_name=?7 AND selected_generation.schema_version=?8 AND selected_generation.schema_fingerprint=?9
           AND (?10 IS NULL OR (selected_generation.dataset_id=?10 AND selected_generation.manifest_version=?11 AND selected_generation.content_hash=?12))
           AND selected_generation.available_at_ns<=?6 AND selected_manifest.created_at_ns<=?6 AND selected_artifact.created_at_ns<=?6
           AND selected_run.state='succeeded' AND selected_run.operation='persist' AND selected_run.source_id=publication.source_id
           AND selected_run.requested_at_ns<=?6 AND selected_run.completed_at_ns<=?6
           AND origin_run.state='succeeded' AND origin_run.operation='persist' AND origin_run.source_id=publication.source_id
           AND origin_run.requested_at_ns<=?6 AND origin_run.completed_at_ns<=?6
           AND capture.recorded_at_ns<=?6 AND publication.capture_recorded_at_ns<=?6
           AND publication.max_available_at_ns<=?6 AND publication.max_received_at_ns<=?6 AND publication.max_ingested_at_ns<=?6
           AND publication.published_at_ns<=?6 AND publication.admission_class='current_research_only'
           AND publication.current_research_eligible=1 AND publication.point_in_time_eligible=0 AND publication.backtest_eligible=0
           AND publication.retrospective_training_eligible=0
           AND (?13=0 OR json_extract(publication.receipt_json,'$.all_bar_count')=publication.expected_bar_count)
           AND (?14=0 OR json_extract(publication.receipt_json,'$.raw_bar_count')=publication.expected_bar_count)
         ORDER BY publication.published_at_ns DESC, publication.origin_generation_sequence DESC,
                  selected_generation.available_at_ns DESC, selected_generation.generation_sequence DESC
         LIMIT 2")?;
    let mut rows = statement.query(params![
        request.instrument_id.to_string(),
        date_key(start),
        date_key(end),
        request.provider_instrument_id.as_str(),
        request.venue_id.as_str(),
        request.knowledge_cutoff.unix_nanos(),
        schema.name(),
        schema.version().get(),
        schema.fingerprint(),
        exact.map(|pin| pin.dataset_id().as_str()),
        exact
            .map(|pin| i64::try_from(pin.manifest_version()))
            .transpose()
            .map_err(|_| ManifestCatalogError::CountOverflow)?,
        exact.map(|pin| pin.content_hash().bytes()),
        i64::from(
            request.adjustment == MarketBarAdjustment::All
                || request.surface_requirement == MarketHistoryPriceSurfaceRequirement::RawWithAll
        ),
        i64::from(request.adjustment == MarketBarAdjustment::Raw)
    ])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let dataset: String = row.get(0)?;
    let version: i64 = row.get(1)?;
    let content: Vec<u8> = row.get(2)?;
    let digest: Vec<u8> = row.get(3)?;
    let rank: (i64, i64) = (row.get(4)?, row.get(5)?);
    let publication_digest = parse_sha256(&digest)?;
    if let Some(other) = rows.next()? {
        let other_rank: (i64, i64) = (other.get(4)?, other.get(5)?);
        let other_digest: Vec<u8> = other.get(3)?;
        if rank == other_rank && other_digest != digest {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
    }
    drop(rows);
    drop(statement);
    let manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from(dataset.as_str())?,
        u64::try_from(version).map_err(|_| ManifestCatalogError::CorruptCatalog)?,
        schema,
        parse_sha256(&content)?,
    )?;
    let pinned = load_pinned(connection, &manifest, max_objects)?;
    let mut receipt = load_market_bar_history_receipt(
        connection,
        publication_digest,
        request.instrument_id,
        request.knowledge_cutoff,
    )?;
    receipt.adjustment = request.adjustment;
    receipt.feed = request.feed.clone();
    receipt.bar_set_digest = match request.adjustment {
        MarketBarAdjustment::Raw
            if receipt.raw_bar_count as usize == receipt.expected_bar_count =>
        {
            receipt.raw_bar_set_digest
        }
        MarketBarAdjustment::All
            if receipt.all_bar_count as usize == receipt.expected_bar_count =>
        {
            receipt.all_bar_set_digest
        }
        _ => None,
    }
    .ok_or(ManifestCatalogError::CorruptCatalog)?;
    if !request.matches_receipt(&receipt) {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/complete-nominal-history-policy/v1");
    hash_text(&mut hash, SOURCE);
    hash_text(&mut hash, RULESET);
    hash_text(&mut hash, request.feed.as_str());
    hash.update([request.surface_requirement as u8]);
    let policy_digest = nonzero_sha256(hash.finalize().into())?;
    let selection_digest =
        history_selection_digest(policy_digest, request, &manifest, publication_digest)?;
    check_operation(deadline, cancellation)?;
    Ok(Some(CompleteMarketBarHistorySelection {
        pinned,
        receipt,
        policy_digest,
        selection_digest,
        surface_requirement: request.surface_requirement,
        knowledge_cutoff: request.knowledge_cutoff,
    }))
}

pub(super) fn resolve_nominal_request(
    connection: &Connection,
    request: &CanonicalMarketBarHistoryRequest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<CompleteMarketBarHistoryRequest>, ManifestCatalogError> {
    // This source publishes raw and all-adjusted surfaces, never split-only prices.
    if !matches!(
        request.selection_policy.adjustment(),
        MarketBarAdjustment::Raw | MarketBarAdjustment::All
    ) {
        return Ok(None);
    }
    let (start, end) = request
        .requested_dates
        .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?;
    let exact = request.exact_manifest.as_ref();
    let mut statement=connection.prepare(
        "SELECT DISTINCT publication.provider_instrument_id,publication.venue_id
         FROM market_bar_history_publications AS publication
         JOIN analytical_generation_market_bar_history_inputs AS input USING(publication_receipt_digest)
         JOIN analytical_available_generations AS generation ON generation.generation_sequence=input.generation_sequence
         WHERE publication.instrument_id=?1 AND publication.source_id='tiingo-starter'
           AND publication.requested_start_date=?2 AND publication.requested_end_date=?3 AND publication.published_at_ns<=?4
           AND generation.available_at_ns<=?4 AND (?5 IS NULL OR (generation.dataset_id=?5 AND generation.manifest_version=?6 AND generation.content_hash=?7))
         ORDER BY publication.provider_instrument_id,publication.venue_id LIMIT 2")?;
    let mut rows = statement.query(params![
        request.instrument_id.to_string(),
        date_key(start),
        date_key(end),
        request.knowledge_cutoff.unix_nanos(),
        exact.map(|pin| pin.dataset_id().as_str()),
        exact
            .map(|pin| i64::try_from(pin.manifest_version()))
            .transpose()
            .map_err(|_| ManifestCatalogError::CountOverflow)?,
        exact.map(|pin| pin.content_hash().bytes())
    ])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let provider: String = row.get(0)?;
    let venue: String = row.get(1)?;
    if rows.next()?.is_some() {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    }
    let adjustment = request.selection_policy.adjustment();
    let mut resolved = CompleteMarketBarHistoryRequest::try_latest_nominal(
        request.instrument_id,
        start,
        end,
        ProviderInstrumentId::try_from(provider.as_str())
            .map_err(|_| ManifestCatalogError::CorruptCatalog)?,
        VenueId::try_from(venue.as_str()).map_err(|_| ManifestCatalogError::CorruptCatalog)?,
        SourceIdentifier::try_from(if adjustment == MarketBarAdjustment::Raw {
            RAW_FEED
        } else {
            ALL_FEED
        })
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?,
        SourceIdentifier::try_from(INTERVAL).map_err(|_| ManifestCatalogError::CorruptCatalog)?,
        adjustment,
        SourceIdentifier::try_from(RULESET).map_err(|_| ManifestCatalogError::CorruptCatalog)?,
        request.knowledge_cutoff,
    )?;
    resolved.exact_manifest = exact.cloned();
    check_operation(deadline, cancellation)?;
    Ok(Some(resolved))
}

/// The complete-daily policy prefers a complete nominal EOD window when that admitted source
/// exists. It never compares a civil date to an invented instant or merges heterogeneous feeds.
pub(super) fn latest_nominal_window(
    connection: &Connection,
    max_objects: usize,
    request: &LatestCanonicalMarketBarHistoryWindowRequest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<LatestCanonicalMarketBarHistoryWindowSelection>, ManifestCatalogError> {
    // An unsupported nominal surface must not shadow a genuine timestamped publication.
    if !matches!(
        request.selection_policy.adjustment(),
        MarketBarAdjustment::Raw | MarketBarAdjustment::All
    ) {
        return Ok(None);
    }
    let mut statement=connection.prepare(
        "SELECT receipt_json FROM market_bar_history_publications
         WHERE instrument_id=?1 AND source_id='tiingo-starter' AND requested_start_date IS NOT NULL
           AND published_at_ns<=?2 AND max_available_at_ns<=?2 AND max_received_at_ns<=?2 AND max_ingested_at_ns<=?2 AND capture_recorded_at_ns<=?2
           AND ((?3=1 AND json_extract(receipt_json,'$.all_bar_count')=expected_bar_count)
                OR (?3=0 AND json_extract(receipt_json,'$.raw_bar_count')=expected_bar_count))
         ORDER BY requested_end_date DESC,expected_bar_count DESC,requested_start_date ASC,published_at_ns DESC
         LIMIT 1")?;
    let value: Option<String> = statement
        .query_row(
            params![
                request.instrument_id.to_string(),
                request.knowledge_cutoff.unix_nanos(),
                i64::from(request.selection_policy.adjustment() == MarketBarAdjustment::All)
            ],
            |row| row.get(0),
        )
        .optional()?;
    drop(statement);
    let Some(value) = value else { return Ok(None) };
    let wire: MarketBarHistoryReceiptWire =
        serde_json::from_str(&value).map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    if !nominal_wire_valid(&wire) {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let dates = wire
        .date_windows
        .as_ref()
        .ok_or(ManifestCatalogError::CorruptCatalog)?
        .requested_dates();
    let canonical = CanonicalMarketBarHistoryRequest::try_latest_nominal(
        request.instrument_id,
        dates.0,
        dates.1,
        request.selection_policy,
        request.knowledge_cutoff,
    )?;
    let resolved = resolve_nominal_request(connection, &canonical, deadline, cancellation)?
        .ok_or(ManifestCatalogError::CorruptCatalog)?;
    let Some(selected) =
        select_nominal_history(connection, max_objects, &resolved, deadline, cancellation)?
    else {
        return Ok(None);
    };
    let exact_request = CanonicalMarketBarHistoryRequest::try_exact_nominal(
        request.instrument_id,
        dates.0,
        dates.1,
        request.selection_policy,
        request.knowledge_cutoff,
        selected.pinned.manifest().clone(),
    )?;
    let mut hash = Sha256::new();
    hash.update(LATEST_CANONICAL_HISTORY_WINDOW_SELECTION_DOMAIN);
    hash.update(selected.selection_digest.bytes());
    hash.update(request.knowledge_cutoff.unix_nanos().to_be_bytes());
    Ok(Some(LatestCanonicalMarketBarHistoryWindowSelection {
        exact_request,
        lookup_digest: nonzero_sha256(hash.finalize().into())?,
    }))
}
