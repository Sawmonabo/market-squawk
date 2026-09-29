//! Complete nominal EOD replay from the shared logical publication and original page seals.
use crate::{
    AnalyticalDataService, CompleteMarketBarHistoryCursor, CorporateActionRecord, IngestError,
    PersistedProviderLogicalPublicationBinding, ProviderLogicalPublicationOrigin,
};
use market_squawk_adapter_tiingo::{
    TiingoDecoder, TiingoEodCashUnitEvidence, TiingoEodDailyActionDisposition,
    TiingoEodHistoryPageEvidence, TiingoEodHistoryStage, TiingoHistoryPlan, TiingoRequestSpec,
    ValidatedTiingoEodHistory,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, ResearchObservation, Timestamp};
use market_squawk_platform::{
    ResearchObjectControl, ResearchObjectControlPoint, SealedResearchJournalStore,
    VerifiedResearchObject,
};
use market_squawk_sources::{LogicalObjectRole, SealedProviderCaptureSetReceipt};
use sha2::{Digest as _, Sha256};
use std::io::{Read, Seek, Write};

/// Original read authority, issued only after every retained page is physically reopened and
/// its indexed canonical, session and action projections reproduce the admitted publication.
#[derive(Debug)]
pub struct RetainedTiingoEodActionHistory {
    history: CompleteMarketBarHistoryCursor,
    binding: PersistedProviderLogicalPublicationBinding,
    actions: ValidatedTiingoEodHistory,
    _scratch: crate::OperationScratchDirectory,
    evidence_digest: EvidenceDigest,
}
impl RetainedTiingoEodActionHistory {
    pub fn matches_calendar(&self, calendar: &super::RetainedCorporateActionCalendar) -> bool {
        let receipt = self.history.selection().receipt();
        let Some(graph) = receipt.date_windows() else {
            return false;
        };
        let retained = graph.calendar();
        if calendar.knowledge_cutoff() != self.knowledge_cutoff()
            || calendar.available_at() > self.knowledge_cutoff()
            || calendar.manifest().content_hash().bytes() != retained.origin_content_digest.bytes()
            || calendar.binding_digest() != retained.capture_binding_digest
            || !retained.relationship.matches(
                calendar.venue_id(),
                graph.venue_id(),
                graph.requested_dates(),
            )
        {
            return false;
        }
        let mut dates = Sha256::new();
        dates.update(b"market-squawk/market-bar-history-original-dates/v1");
        dates.update((graph.session_count() as u64).to_be_bytes());
        let mut count = 0usize;
        for date in calendar.native_dates_in(graph.requested_dates()) {
            dates.update(date.year().to_be_bytes());
            dates.update([date.month(), date.day()]);
            count += 1;
        }
        count == graph.session_count() && dates.finalize().as_slice() == graph.date_digest().bytes()
    }
    pub const fn history(&self) -> &CompleteMarketBarHistoryCursor {
        &self.history
    }
    pub const fn binding(&self) -> &PersistedProviderLogicalPublicationBinding {
        &self.binding
    }
    pub const fn actions(&self) -> &ValidatedTiingoEodHistory {
        &self.actions
    }
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.history.read_receipt().knowledge_cutoff()
    }
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
    pub fn action_row_count(&self) -> usize {
        self.actions.descriptor().source_action_count()
    }
    pub fn action_rows(
        &self,
    ) -> impl Iterator<Item = Result<TiingoEodDailyActionDisposition, IngestError>> + '_ {
        (0..self.action_row_count()).map(|index| {
            self.actions
                .action_disposition_at(index)
                .map_err(stage_error)?
                .ok_or_else(invalid)
        })
    }
    pub fn record(&self, index: usize) -> Result<Option<CorporateActionRecord>, IngestError> {
        if index >= self.history.source_action_count() {
            return Ok(None);
        }
        let expected = self
            .actions
            .normalized_action_at(index)
            .map_err(stage_error)?
            .ok_or_else(invalid)?;
        let observation = self
            .history
            .source_action_at(index)
            .map_err(history_error)?
            .ok_or_else(invalid)?;
        let original = ResearchObservation::CorporateAction(expected.observation);
        let bytes = serde_json::to_vec(&original).map_err(|_| invalid())?;
        if original
            .with_revision(observation.context().time().revision())
            .map_err(|_| invalid())?
            != ResearchObservation::CorporateAction(observation.clone())
        {
            return Err(invalid());
        }
        Ok(Some(CorporateActionRecord::new(
            observation,
            self.history.read_receipt().origin_manifest().clone(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into()),
        )))
    }
    pub fn records(&self) -> impl Iterator<Item = Result<CorporateActionRecord, IngestError>> + '_ {
        (0..self.history.source_action_count()).map(|index| self.record(index)?.ok_or_else(invalid))
    }
}
impl AnalyticalDataService {
    /// Runs on the existing controlled original-reader lane. The catalog locator does not mint
    /// a seal: every independent original is reopened and rebound by the shared source owner.
    pub fn rejoin_tiingo_eod_action_history(
        &self,
        history: CompleteMarketBarHistoryCursor,
        owned: &ProviderLogicalPublicationOrigin,
        store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
        deadline: std::time::Instant,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<RetainedTiingoEodActionHistory, IngestError> {
        let checkpoint = || {
            if cancellation.is_cancelled() {
                return Err(IngestError::Cancelled);
            }
            if std::time::Instant::now() >= deadline {
                return Err(IngestError::DeadlineExceeded);
            }
            control
                .checkpoint(ResearchObjectControlPoint::BeforeVerification)
                .map_err(|error| {
                    IngestError::SealedProviderCapture(
                        market_squawk_platform::SealedResearchJournalStoreError::ObjectControl(
                            error,
                        ),
                    )
                })
        };
        checkpoint()?;
        let receipt = history.selection().receipt();
        let graph = receipt.date_windows().ok_or_else(invalid)?;
        let binding = owned.publication();
        if owned.manifest() != receipt.origin_manifest()
            || binding.binding_digest().bytes() != receipt.binding_digest().bytes()
            || binding.terminal().source_id() != receipt.source_id()
            || binding.terminal().receipt_digest().bytes()
                != receipt.capture_receipt_digest().bytes()
            || binding.terminal().total_canonical_rows() != u64::from(receipt.origin_record_count())
            || binding.objects().len() != 5
            || receipt.published_at() > history.read_receipt().knowledge_cutoff()
        {
            return Err(invalid());
        }
        let (instrument, contract) =
            market_squawk_adapter_tiingo::reconstruct_eod_history_descriptor_mapping(graph)
                .map_err(|_| invalid())?;
        let normalization = graph.normalization();
        let plan = TiingoHistoryPlan::try_new(
            instrument.ticker().clone(),
            graph.requested_dates().0,
            graph.requested_dates().1,
        )
        .map_err(|_| invalid())?;
        if plan.pages().len() != graph.page_count() {
            return Err(invalid());
        }
        let decoder = TiingoDecoder::new(
            normalization.native_schema_revision.clone(),
            normalization.entitlement_generation.clone(),
        );
        let unit = normalization
            .cash_unit
            .as_ref()
            .map(|unit| {
                TiingoEodCashUnitEvidence::try_new_with_status(
                    instrument.instrument_id(),
                    normalization.contract_identity,
                    unit.currency,
                    unit.assertion.clone(),
                    unit.available_at,
                    unit.status,
                )
            })
            .transpose()
            .map_err(|_| invalid())?;
        let mut objects = Vec::with_capacity(5);
        for (ordinal, object) in binding.objects().iter().enumerate() {
            checkpoint()?;
            let role = match ordinal {
                0 => LogicalObjectRole::ProviderPayload,
                1 => LogicalObjectRole::Catalog,
                _ => LogicalObjectRole::ProviderComponent,
            };
            if object.ordinal() != ordinal as u32 || object.role() != role {
                return Err(invalid());
            }
            objects.push(store.open_verified_logical_object_claim(object.claim(), control)?);
        }
        let action_index = objects.pop().ok_or_else(invalid)?;
        let session_index = objects.pop().ok_or_else(invalid)?;
        let mut pages = objects.pop().ok_or_else(invalid)?;
        let descriptor = objects.pop().ok_or_else(invalid)?;
        let pack = objects.pop().ok_or_else(invalid)?;
        let descriptor_bytes = serde_json::to_vec(graph).map_err(|_| invalid())?;
        if descriptor.content_digest().bytes()
            != <[u8; 32]>::from(Sha256::digest(&descriptor_bytes))
            || descriptor.size_bytes() != descriptor_bytes.len() as u64
        {
            return Err(invalid());
        }
        let scratch = self.operation_scratch()?;
        let mut stage = None;
        for ordinal in 0..=graph.page_count() {
            checkpoint()?;
            let evidence: TiingoEodHistoryPageEvidence = read_frame(&mut pages, ordinal)?;
            if evidence.ordinal != ordinal
                || evidence.capture.source_id() != receipt.source_id()
                || evidence.capture.metadata_revision() != contract.source_contract_revision()
            {
                return Err(invalid());
            }
            let [page] = evidence.capture.pages() else {
                return Err(invalid());
            };
            let request = if ordinal == 0 {
                TiingoRequestSpec::metadata(instrument.ticker().clone()).map_err(|_| invalid())?
            } else {
                plan.pages().get(ordinal - 1).cloned().ok_or_else(invalid)?
            };
            if page.request_identity() != request.request_identity()
                || page.received_at() > history.read_receipt().knowledge_cutoff()
            {
                return Err(invalid());
            }
            let segment = store
                .open_verified_claim_with_control(&evidence.original_segment_claim, control)?;
            let sealed = SealedProviderCaptureSetReceipt::try_bind(
                evidence.capture.clone(),
                segment.receipt().clone(),
            )
            .map_err(|_| invalid())?;
            if sealed.receipt_digest() != evidence.sealed_receipt_digest
                || segment.records().len() != 1
            {
                return Err(invalid());
            }
            let body = segment.records()[0].payload();
            if ordinal == 0 {
                if evidence.window.is_some() {
                    return Err(invalid());
                }
                let metadata = decoder
                    .decode_metadata(
                        request,
                        page.http_status(),
                        body,
                        page.received_at(),
                        normalization.metadata_decoded_at,
                    )
                    .map_err(|_| invalid())?;
                stage = Some(
                    TiingoEodHistoryStage::try_new(
                        plan.clone(),
                        metadata,
                        sealed,
                        instrument.clone(),
                        contract.clone(),
                        unit.clone(),
                        graph.admitted_plan_digest(),
                        scratch.path(),
                    )
                    .map_err(stage_error)?,
                );
            } else {
                let window = evidence.window.as_ref().ok_or_else(invalid)?;
                if window.component_ordinal as usize != ordinal
                    || window.ingested_at > history.read_receipt().knowledge_cutoff()
                    || window.decoded_at > window.ingested_at
                {
                    return Err(invalid());
                }
                let response = decoder
                    .decode_eod(
                        request,
                        page.http_status(),
                        body,
                        page.received_at(),
                        window.decoded_at,
                    )
                    .map_err(|_| invalid())?;
                stage
                    .as_mut()
                    .ok_or_else(invalid)?
                    .push_page(&response, &sealed, window.ingested_at, &cancellation)
                    .map_err(stage_error)?;
            }
        }
        require_end(&mut pages)?;
        let actions = stage
            .ok_or_else(invalid)?
            .finish_replay(graph, &cancellation)
            .map_err(stage_error)?;
        // Compare canonical encodings emitted by the same adapter index writer. This checks
        // every page/window, session and explicit zero/missing/action disposition without
        // retaining the index objects or reconstructing a second authority representation.
        for (index, object) in [&pages, &session_index, &action_index]
            .into_iter()
            .enumerate()
        {
            checkpoint()?;
            let mut sink = IndexDigest {
                hash: Sha256::new(),
                bytes: 0,
                control,
            };
            match index {
                0 => actions.write_page_index(&mut sink, &cancellation),
                1 => actions.write_session_index(&mut sink, &cancellation),
                _ => actions.write_action_index(&mut sink, &cancellation),
            }
            .map_err(|error| checkpoint().err().unwrap_or_else(|| stage_error(error)))?;
            if sink.bytes != object.size_bytes()
                || sink.hash.finalize().as_slice() != object.content_digest().bytes()
            {
                return Err(invalid());
            }
        }
        if graph.normalized_action_count() != history.source_action_count() {
            return Err(invalid());
        }
        // Replay the exact selected bar and action identities. The cursor independently checked
        // the complete admitted bar payload hash; revision assignment remains catalog-owned.
        for (ordinal, bar) in history.bars().enumerate() {
            checkpoint()?;
            let bar = bar.map_err(|_| invalid())?;
            let session = actions
                .session_at(ordinal)
                .map_err(stage_error)?
                .ok_or_else(invalid)?;
            if bar.time_semantics() != &session.time {
                return Err(invalid());
            }
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/retained-tiingo-eod-action-history/v2\0");
        hash.update(owned.manifest().content_hash().bytes());
        hash.update(binding.binding_digest().bytes());
        hash.update(receipt.receipt_digest().bytes());
        hash.update(history.read_receipt().source_result_digest().bytes());
        hash.update(actions.completion_identity().bytes());
        hash.update(actions.request_set_identity().bytes());
        let result = RetainedTiingoEodActionHistory {
            history,
            binding: binding.clone(),
            actions,
            _scratch: scratch,
            evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
        };
        for record in result.records() {
            checkpoint()?;
            record?;
        }
        for object in [pack, descriptor, pages, session_index, action_index] {
            checkpoint()?;
            object.reverify_for_commit(control)?;
        }
        checkpoint()?;
        Ok(result)
    }
}
fn invalid() -> IngestError {
    IngestError::ProviderCaptureRequired
}
fn stage_error(error: market_squawk_adapter_tiingo::TiingoEodHistoryStageError) -> IngestError {
    match error {
        market_squawk_adapter_tiingo::TiingoEodHistoryStageError::Cancelled => {
            IngestError::Cancelled
        }
        market_squawk_adapter_tiingo::TiingoEodHistoryStageError::Sql(error) => {
            IngestError::Catalog(crate::CatalogError::Sqlite(error))
        }
        market_squawk_adapter_tiingo::TiingoEodHistoryStageError::Io(error) => {
            IngestError::Parquet(crate::ParquetStoreError::Io(error))
        }
        market_squawk_adapter_tiingo::TiingoEodHistoryStageError::Json(error) => {
            IngestError::Serialization(error)
        }
        _ => invalid(),
    }
}
fn history_error(error: crate::AnalyticalReadError) -> IngestError {
    match error {
        crate::AnalyticalReadError::Query(crate::QueryError::Cancelled) => IngestError::Cancelled,
        crate::AnalyticalReadError::Query(crate::QueryError::DeadlineExceeded) => {
            IngestError::DeadlineExceeded
        }
        crate::AnalyticalReadError::Parquet(error) => IngestError::Parquet(error),
        _ => invalid(),
    }
}
fn read_frame<T: serde::de::DeserializeOwned>(
    reader: &mut VerifiedResearchObject,
    ordinal: usize,
) -> Result<T, IngestError> {
    let mut header = [0u8; 16];
    reader.read_exact(&mut header).map_err(|_| invalid())?;
    let index = u64::from_le_bytes(header[..8].try_into().map_err(|_| invalid())?);
    let length = u64::from_le_bytes(header[8..].try_into().map_err(|_| invalid())?);
    if index != ordinal as u64 || length == 0 || length > 16 * 1024 * 1024 {
        return Err(invalid());
    }
    let position = reader.stream_position().map_err(|_| invalid())?;
    if position
        .checked_add(length)
        .is_none_or(|end| end > reader.size_bytes())
    {
        return Err(invalid());
    }
    let mut bytes = vec![0; usize::try_from(length).map_err(|_| invalid())?];
    reader.read_exact(&mut bytes).map_err(|_| invalid())?;
    serde_json::from_slice(&bytes).map_err(|_| invalid())
}
fn require_end(reader: &mut VerifiedResearchObject) -> Result<(), IngestError> {
    if reader.stream_position().map_err(|_| invalid())? != reader.size_bytes() {
        return Err(invalid());
    }
    Ok(())
}

struct IndexDigest<'a> {
    hash: Sha256,
    bytes: u64,
    control: &'a dyn ResearchObjectControl,
}
impl Write for IndexDigest<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.control
            .checkpoint(ResearchObjectControlPoint::BeforeVerification)
            .map_err(std::io::Error::other)?;
        self.bytes = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("index byte count overflow"))?;
        self.hash.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
