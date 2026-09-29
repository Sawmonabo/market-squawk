//! One original metadata/window capture, canonical raw/adjusted bars and native action columns.

use super::*;
use crate::{
    TiingoEodActionError, TiingoEodCashUnitEvidence, TiingoEodExpectedSessionAuthority,
    TiingoEodFinancialCoverageDisposition, TiingoEodHistoryActionProjection,
    TiingoEodInstrumentKind, TiingoPendingEodHistoryPublication, TiingoSealedHistoryPage,
    normalize_eod_history_actions,
};
use market_squawk_domain::{CalendarDate, CorporateActionKind};
use market_squawk_sources::{
    CompleteMarketBarDateSessionV1, CompleteMarketBarDateWindowV1,
    CompleteMarketBarDateWindowsInputV1, CompleteMarketBarDateWindowsV1,
    ProviderCaptureSemanticBinding, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    ProviderOrderedCaptureSegments, RetainedMarketHistoryCalendarV1, RetainedMarketHistoryCashUnitV1,
    RetainedMarketHistoryNativeCoverageV1, RetainedMarketHistoryNormalizationV1,
};

const HISTORY_DATASET: &str = "tiingo-complete-eod-history";
const HISTORY_PURPOSE: &str = "tiingo-eod-complete-date-windows/v1";

/// One-use original sealed native history, ready for the existing common publication owner.
#[derive(Debug)]
pub struct TiingoPreparedEodHistoryCapture {
    history: TiingoPendingEodHistoryPublication,
    metadata: TiingoMetadataReceipt,
    responses: Vec<TiingoEodReceipt>,
    actions: TiingoEodHistoryActionProjection,
    token: ProviderOrderedCaptureSegments,
}

impl TiingoPreparedEodHistoryCapture {
    /// Rejoins exact original page tokens only after native rows equal the calendar-closed mapper
    /// handoff. The constructor accepts no caller-authored bar, action or date completeness flag.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        history: TiingoPendingEodHistoryPublication,
        metadata: TiingoMetadataReceipt,
        responses: Vec<TiingoEodReceipt>,
        tokens: Vec<ProviderWholeCaptureToken>,
        instrument_definition_digest: EvidenceDigest,
        admitted_plan_digest: EvidenceDigest,
        cash_unit: Option<&TiingoEodCashUnitEvidence>,
        expected_session_authority: &dyn TiingoEodExpectedSessionAuthority,
    ) -> Result<Self, TiingoEodHistoryPublicationError> {
        let invalid = || TiingoEodHistoryPublicationError::CaptureMismatch;
        if history.financial_coverage() != TiingoEodFinancialCoverageDisposition::Complete
            || !history.missing_expected_sessions().is_empty()
            || history.returned_sessions().is_empty()
            || responses.len() != history.pages().len()
            || tokens.len() != responses.len() + 1
            || tokens.len() > market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES
            || history.returned_sessions().len() > market_squawk_sources::MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS
        {
            return Err(invalid());
        }
        let first = history.pages().first().ok_or_else(invalid)?;
        let instrument = first.instrument();
        let contract = first.contract();
        let metadata_seal = tokens.first().ok_or_else(invalid)?.persisted_receipt();
        let metadata_capture = metadata_seal.capture();
        let [metadata_page] = metadata_capture.pages() else { return Err(invalid()); };
        let metadata_evidence = metadata.evidence();
        if instrument_definition_digest != instrument.instrument_definition().payload_evidence().content_digest()
            || metadata_capture.source_id() != contract.source_id()
            || metadata_capture.metadata_revision() != contract.source_contract_revision()
            || metadata_capture.dataset().as_str() != crate::canonical::TIINGO_METADATA_DATASET
            || metadata_capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
            || metadata_evidence.request().endpoint() != TiingoEndpointFamily::Metadata
            || metadata_evidence.request().scope() != &TiingoRequestScope::Metadata
            || metadata_page.request_identity() != metadata_evidence.request().request_identity()
            || metadata_capture.request_set_identity() != metadata_page.request_identity()
            || metadata_page.http_status() != metadata_evidence.status()
            || metadata_page.body_digest() != metadata_evidence.body_digest()
            || metadata_page.body_bytes() != metadata_evidence.response_bytes()
            || metadata_page.received_at() != metadata_evidence.received_at()
            || metadata_evidence != first.metadata_evidence()
        {
            return Err(invalid());
        }
        for (index, ((response, token), page)) in responses.iter().zip(&tokens[1..]).zip(history.pages()).enumerate() {
            let sealed = TiingoSealedHistoryPage::try_new(
                &history.capture().plan().pages()[index], response, token.persisted_receipt(),
            )?;
            if &sealed != &history.capture().pages()[index] {
                return Err(invalid());
            }
            let reconstructed = map_eod_page_candidate(TiingoEodMappingInput {
                response, metadata: &metadata,
                sealed_capture: token.persisted_receipt(), sealed_metadata_capture: metadata_seal,
                instrument, contract, ingested_at: page.ingested_at(),
            })?;
            if &reconstructed != page {
                return Err(invalid());
            }
        }
        let expected = history.expected_session_evidence();
        let validation = expected_session_authority.validate_current(expected)?;
        let retained_validation = history.expected_session_validation();
        if validation.evidence_identity() != expected.evidence_identity()
            || validation.authority_generation() != expected.authority_generation()
            || validation.validated_at() < retained_validation.validated_at()
            || expected.expected_sessions() != history.returned_sessions()
        {
            return Err(invalid());
        }
        let actions = normalize_eod_history_actions(&history, cash_unit)?;
        let mut windows = Vec::new();
        let mut sessions = Vec::new();
        windows.try_reserve_exact(responses.len()).map_err(|_| invalid())?;
        sessions.try_reserve_exact(history.returned_sessions().len()).map_err(|_| invalid())?;
        for (index, (response, page)) in responses.iter().zip(history.pages()).enumerate() {
            let TiingoRequestScope::History { start_date, end_date, .. } = response.evidence().request().scope()
                else { return Err(invalid()); };
            windows.push(CompleteMarketBarDateWindowV1 {
                component_ordinal: u16::try_from(index + 1).map_err(|_| invalid())?,
                request_identity: response.evidence().request().request_identity(),
                start_date: *start_date, end_date: *end_date,
                first_session_ordinal: u32::try_from(sessions.len()).map_err(|_| invalid())?,
                returned_session_count: u32::try_from(response.rows().len()).map_err(|_| invalid())?,
                decoded_at: response.evidence().decoded_at(), ingested_at: page.ingested_at(),
            });
            for row in response.rows() {
                sessions.push(CompleteMarketBarDateSessionV1 {
                    date: row.date(), time: crate::eod::nominal_time(row)?, row_digest: row.row_digest(),
                });
            }
        }
        let normalization = RetainedMarketHistoryNormalizationV1 {
            instrument_definition: instrument.instrument_definition().clone(),
            provider_mapping_evidence: instrument.provider_mapping_evidence().clone(),
            provider_exchange_code: instrument.provider_exchange_code().clone(),
            is_exchange_traded_fund: instrument.kind() == TiingoEodInstrumentKind::ExchangeTradedFund,
            resolved_at: instrument.resolved_at(), currency: instrument.currency(),
            source_contract_revision: contract.source_contract_revision().clone(),
            source_contract_evidence: contract.source_contract_evidence().clone(),
            native_schema_revision: contract.native_schema_revision().clone(),
            native_schema_evidence: contract.native_schema_evidence().clone(),
            entitlement_generation: contract.entitlement_generation_identity().clone(),
            entitlement_generation_number: contract.entitlement_generation(),
            entitlement_evidence: contract.entitlement_evidence(),
            adjusted_surface_evidence: contract.adjusted_surface_evidence().clone(),
            contract_identity: contract.mapping_identity(),
            metadata_decoded_at: metadata.evidence().decoded_at(),
            native_coverage: match metadata.metadata().coverage() {
                TiingoCoverage::Supported { start_date, end_date } => RetainedMarketHistoryNativeCoverageV1::Supported { start: start_date, end: end_date },
                TiingoCoverage::Unsupported => RetainedMarketHistoryNativeCoverageV1::Unsupported,
            },
            cash_unit: cash_unit.map(|unit| RetainedMarketHistoryCashUnitV1 {
                status: unit.status(),
                currency: unit.currency(), assertion: unit.assertion().clone(), available_at: unit.available_at(),
            }),
            calendar: RetainedMarketHistoryCalendarV1 {
                request_identity: expected.request_identity(), calendar_id: expected.calendar_id().clone(),
                origin_content_digest: expected.origin_content_digest(),
                capture_binding_digest: expected.capture_binding_digest(),
                relationship: expected.relationship().clone(),
                calendar_revision: expected.calendar_revision().clone(),
                authority_generation: expected.authority_generation().clone(),
                calendar_available_at: expected.calendar_available_at(), resolved_at: expected.resolved_at(),
                resolution_receipt: expected.resolution_receipt(), evidence_identity: expected.evidence_identity(),
                validated_at: retained_validation.validated_at(), authority_receipt: retained_validation.authority_receipt(),
                validation_identity: retained_validation.receipt_identity(),
            },
        };
        let (requested_start, requested_end) = history.capture().plan().interval();
        let graph = CompleteMarketBarDateWindowsV1::try_new(CompleteMarketBarDateWindowsInputV1 {
            requested_start, requested_end, instrument_id: instrument.instrument_id(),
            instrument_revision_digest: instrument_definition_digest, admitted_plan_digest,
            provider_instrument_id: instrument.provider_instrument_id().clone(),
            venue_id: instrument.venue_id().clone(), interval: identifier("tiingo-calendar-day")?,
            graph_purpose: identifier(HISTORY_PURPOSE)?, windows, sessions,
            completeness_evidence: history.completion_identity(), normalization,
        })?;
        let token = ProviderOrderedCaptureSegments::try_rejoin_request_graph(
            identifier(HISTORY_DATASET)?, tokens,
            ProviderCaptureSemanticBinding::CompleteMarketBarDateWindowsV1(graph),
        )?;
        Ok(Self { history, metadata, responses, actions, token })
    }

    /// Complete nominal request graph backed by the original exclusive physical seals.
    pub const fn capture(&self) -> &ProviderCaptureSetReceipt { self.token.root_capture() }

    /// Emits raw/adjusted bars first in original page/row/surface order, then native cash/split
    /// actions. Every canonical row is bound to its original one-page seal and native row.
    pub fn try_into_publication(self, request: ExtractionRequest) -> Result<TiingoSealedEodPublication, TiingoEodHistoryPublicationError> {
        let invalid = || TiingoEodHistoryPublicationError::CaptureMismatch;
        let record_count = usize::try_from(self.history.total_bars()).map_err(|_| invalid())?
            .checked_add(self.actions.observations().len()).ok_or_else(invalid)?;
        let capture = self.token.root_capture();
        let object = request.object();
        let first = capture.pages().first().ok_or_else(invalid)?;
        let last = capture.pages().last().ok_or_else(invalid)?;
        if object.source_id() != capture.source_id() || object.metadata_revision() != capture.metadata_revision()
            || object.dataset() != capture.dataset() || object.evidence().content_digest() != capture.content_digest()
            || object.capture_identity() != SourceObjectCaptureIdentity::try_from_capture(capture)?
            || object.media_type().as_str() != TIINGO_CANONICAL_MEDIA_TYPE
            || object.effective_interval().starts_at() != first.received_at()
            || object.effective_interval().ends_at().is_some() || object.published_at().is_some()
            || object.expected_bytes() != Some(capture.total_body_bytes())
            || object.availability().conservative_available_at() != Some(last.received_at())
            || record_count == 0 || record_count > request.max_records() as usize
            || self.history.pages().iter().any(|page| page.ingested_at() >= request.deadline())
        { return Err(invalid()); }
        let mut accumulator = ExtractionBatchAccumulator::try_new(&request)?;
        let mut row_pages = Vec::new();
        row_pages.try_reserve_exact(record_count).map_err(|_| invalid())?;
        for (index, page) in self.history.pages().iter().enumerate() {
            for bar in page.bars() {
                let observation = eod_observation(page, bar)?;
                accumulator.push(extraction_record(&request, &observation,
                    bar.time_semantics().effective_coordinate(), bar.received_at())?)?;
                row_pages.push(u16::try_from(index + 1).map_err(|_| invalid())?);
            }
        }
        for action in self.actions.observations() {
            let observation = ResearchObservation::CorporateAction(action.observation.clone());
            accumulator.push(extraction_record(&request, &observation,
                action.observation.context().time().effective().clone(),
                action.observation.context().provenance().received_at())?)?;
            row_pages.push(u16::try_from(action.history_page_index + 1).map_err(|_| invalid())?);
        }
        let batch = accumulator.finish()?.try_bind_provider_capture(capture)?;
        let mut native = ProviderNativeLineageBatchBuilder::try_new(ProviderNativeLineageImplementation::TiingoEodMarketBarV1, &batch)?;
        native.try_set_batch_sidecar(&history_sidecar(&self.metadata, &self.responses)?)?;
        for (page, response) in self.history.pages().iter().zip(&self.responses) {
            for bar in page.bars() {
                let row = response.rows().get(bar.provider_row_index() as usize).ok_or_else(invalid)?;
                native.try_push(&TiingoNativeDailyRowV1::from_row(row, surface_name(bar.surface())))?;
            }
        }
        for action in self.actions.observations() {
            let row = self.responses.get(action.history_page_index)
                .and_then(|response| response.rows().get(action.provider_row_index as usize)).ok_or_else(invalid)?;
            let selected = match action.observation.action() {
                CorporateActionKind::Split { .. } => "split",
                CorporateActionKind::CashDividend { .. } => "dividend",
                _ => return Err(invalid()),
            };
            native.try_push(&TiingoNativeDailyRowV1::from_row(row, selected))?;
        }
        let native = native.finish()?;
        let revision_plan = ExtractionRevisionPlan::locally_observed_with_native_lineage(batch.records().len())?;
        let binding = SealedProviderCaptureBinding::try_ordered_segments(self.token, batch, native, row_pages)?;
        binding.validate()?;
        Ok(TiingoSealedEodPublication { revision_plan, binding })
    }
}

/// Reconstructs exact original mapping values for controlled immutable raw replay. These values
/// convey no publication token, entitlement renewal, calendar currentness or new source authority.
pub fn reconstruct_eod_history_mapping(graph: &CompleteMarketBarDateWindowsV1) -> Result<(TiingoEodInstrumentAuthority, TiingoEodContractEvidence), TiingoEodHistoryPublicationError> {
    let invalid = || TiingoEodHistoryPublicationError::CaptureMismatch;
    let native = graph.normalization();
    if graph.graph_purpose().as_str() != HISTORY_PURPOSE || graph.interval().as_str() != "tiingo-calendar-day" {
        return Err(invalid());
    }
    let instrument = TiingoEodInstrumentAuthority::try_new(
        graph.instrument_id(), graph.venue_id().clone(), graph.provider_instrument_id().clone(),
        crate::TiingoTicker::try_new(graph.provider_instrument_id().as_str())?,
        native.provider_exchange_code.clone(),
        if native.is_exchange_traded_fund { TiingoEodInstrumentKind::ExchangeTradedFund } else { TiingoEodInstrumentKind::Equity },
        native.instrument_definition.clone(), native.provider_mapping_evidence.clone(), native.resolved_at, native.currency,
    )?;
    let contract = TiingoEodContractEvidence::try_new(
        native.source_contract_revision.clone(), native.source_contract_evidence.clone(),
        native.native_schema_revision.clone(), native.native_schema_evidence.clone(),
        native.entitlement_generation_number, native.entitlement_generation.clone(),
        native.entitlement_evidence, native.adjusted_surface_evidence.clone(),
    )?;
    if contract.mapping_identity() != native.contract_identity
        || instrument.instrument_definition().payload_evidence().content_digest() != graph.instrument_revision_digest()
    { return Err(invalid()); }
    Ok((instrument, contract))
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct HistorySidecar<'a> {
    metadata: TiingoMetadataNativeV1<'a>,
    metadata_request: TiingoRequestNativeV1<'a>,
    metadata_disposition: TiingoDispositionNativeV1,
    history_windows: Vec<HistoryWindowNative<'a>>,
}
#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct HistoryWindowNative<'a> {
    request: TiingoRequestNativeV1<'a>,
    start_date: CalendarDate,
    end_date: CalendarDate,
    disposition: TiingoDispositionNativeV1,
    rows: Vec<TiingoNativeDailyRowV1>,
}
fn history_sidecar<'a>(metadata: &'a TiingoMetadataReceipt, responses: &'a [TiingoEodReceipt]) -> Result<HistorySidecar<'a>, TiingoEodHistoryPublicationError> {
    let invalid = || TiingoEodHistoryPublicationError::CaptureMismatch;
    if responses.is_empty() || responses.len() >= market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES {
        return Err(invalid());
    }
    let total_rows = responses.iter().try_fold(0_usize, |sum, response| sum.checked_add(response.rows().len())).ok_or_else(invalid)?;
    if total_rows > market_squawk_sources::MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS {
        return Err(invalid());
    }
    let mut history_windows = Vec::new();
    history_windows.try_reserve_exact(responses.len()).map_err(|_| invalid())?;
    for response in responses {
        let TiingoRequestScope::History { start_date, end_date, .. } = response.evidence().request().scope()
            else { return Err(invalid()); };
        let mut request = TiingoRequestNativeV1::latest(response.evidence().request());
        request.pagination = "application_date_window_without_provider_cursor";
        let mut rows = Vec::new();
        rows.try_reserve_exact(response.rows().len()).map_err(|_| invalid())?;
        rows.extend(response.rows().iter().map(|row| TiingoNativeDailyRowV1::from_row(row, "source")));
        history_windows.push(HistoryWindowNative {
            request, start_date: *start_date, end_date: *end_date,
            disposition: response.disposition().into(), rows,
        });
    }
    Ok(HistorySidecar {
        metadata: TiingoMetadataNativeV1::from_receipt(metadata),
        metadata_request: TiingoRequestNativeV1::latest(metadata.evidence().request()),
        metadata_disposition: metadata.disposition().into(), history_windows,
    })
}

/// Checks every original provider column, including gaps and explicit zero-event source fields.
pub fn verify_eod_history_native_sidecar(metadata: &TiingoMetadataReceipt, responses: &[TiingoEodReceipt], retained: &[u8]) -> Result<(), TiingoEodHistoryPublicationError> {
    if retained.len() > market_squawk_sources::MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES {
        return Err(TiingoEodHistoryPublicationError::CaptureMismatch);
    }
    let expected = serde_json::to_vec(&history_sidecar(metadata, responses)?).map_err(|_| TiingoEodHistoryPublicationError::CaptureMismatch)?;
    if expected != retained { return Err(TiingoEodHistoryPublicationError::CaptureMismatch); }
    Ok(())
}

/// Checks exact original native row values and their selected canonical surface/action column.
pub fn verify_eod_history_native_row(row: &crate::TiingoEodRow, selected: &str, retained: &[u8]) -> Result<(), TiingoEodHistoryPublicationError> {
    let selected = match selected { "raw" => "raw", "adjusted" => "adjusted", "split" => "split", "dividend" => "dividend", _ => return Err(TiingoEodHistoryPublicationError::CaptureMismatch) };
    let expected = serde_json::to_vec(&TiingoNativeDailyRowV1::from_row(row, selected)).map_err(|_| TiingoEodHistoryPublicationError::CaptureMismatch)?;
    if expected != retained { return Err(TiingoEodHistoryPublicationError::CaptureMismatch); }
    Ok(())
}

/// Exact code-owned parser/model implementation identity used by source preparation.
pub fn tiingo_eod_native_schema_evidence() -> ExactPayloadEvidence {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/tiingo/native-decoder-schema/v1\0");
    for bytes in [include_bytes!("../decoder.rs").as_slice(), include_bytes!("../model.rs").as_slice()] {
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()))
}

#[derive(Debug, Error)]
pub enum TiingoEodHistoryPublicationError {
    #[error("Tiingo original native history and immutable capture do not match")]
    CaptureMismatch,
    #[error(transparent)] Latest(#[from] TiingoLatestPublicationError),
    #[error(transparent)] Eod(#[from] TiingoEodMapError),
    #[error(transparent)] Actions(#[from] TiingoEodActionError),
    #[error(transparent)] Capture(#[from] ProviderCaptureError),
    #[error(transparent)] Evidence(#[from] crate::TiingoHistoryEvidenceError),
    #[error(transparent)] Adapter(#[from] TiingoAdapterError),
    #[error(transparent)] Extraction(#[from] ExtractionError),
    #[error(transparent)] Native(#[from] ProviderNativeLineageError),
    #[error(transparent)] Revision(#[from] ObservedRevisionError),
}
