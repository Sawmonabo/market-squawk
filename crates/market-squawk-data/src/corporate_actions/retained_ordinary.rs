//! Sole complete native EOD rejoin used by application actions and source-admitted data plans.
//! No caller projection, row digest, zero-event flag, or calendar mapping mints this read.

use crate::{
    AnalyticalDataService, CompleteMarketBarHistoryOutput, CorporateActionRecord,
    GenerationOwnedProviderCaptureEvidence, IngestError, PersistedProviderCaptureBindingEvidence,
};
use market_squawk_adapter_tiingo::{
    TiingoDecoder, TiingoEodActionReplayPage, TiingoEodCashUnitEvidence,
    TiingoEodHistoryActionProjection, TiingoEodReceipt, TiingoHistoryPlan, TiingoRequestSpec,
    rejoin_eod_history_actions, verify_eod_history_native_row, verify_eod_history_native_sidecar,
};
use market_squawk_domain::{
    CorporateActionKind, DigestAlgorithm, EvidenceDigest, MarketBarAdjustment, ResearchObservation,
    Timestamp,
};
use market_squawk_platform::{ResearchObjectControl, SealedResearchJournalStore};
use market_squawk_sources::{
    ProviderCapturePageReceipt, ProviderCaptureSemanticBinding, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt,
};
use sha2::{Digest as _, Sha256};

/// Genuine original canonical/native read. Construction requires the existing exact data read,
/// original generation evidence and controlled physical raw replay; this value is not deserializable.
#[derive(Debug)]
pub struct RetainedTiingoEodActionHistory {
    history: CompleteMarketBarHistoryOutput,
    binding: PersistedProviderCaptureBindingEvidence,
    actions: TiingoEodHistoryActionProjection,
    records: Box<[CorporateActionRecord]>,
    evidence_digest: EvidenceDigest,
}
impl RetainedTiingoEodActionHistory {
    /// Rejoins the original independently retained calendar under the source graph's exact
    /// reviewed venue relation. Matching labels alone cannot confer calendar authority.
    pub fn matches_calendar(&self, calendar: &super::RetainedCorporateActionCalendar) -> bool {
        let Some(graph) = self.history.selection().receipt().date_windows() else {
            return false;
        };
        let retained = graph.calendar();
        calendar.knowledge_cutoff() == self.knowledge_cutoff()
            && calendar.available_at() <= self.knowledge_cutoff()
            && calendar.manifest().content_hash().bytes() == retained.origin_content_digest.bytes()
            && calendar.binding_digest() == retained.capture_binding_digest
            && retained.relationship.matches(
                calendar.venue_id(),
                graph.venue_id(),
                graph.requested_dates(),
            )
            && calendar
                .native_dates_in(graph.requested_dates())
                .eq(graph.sessions().iter().map(|session| session.date))
    }

    pub const fn history(&self) -> &CompleteMarketBarHistoryOutput {
        &self.history
    }
    pub const fn binding(&self) -> &PersistedProviderCaptureBindingEvidence {
        &self.binding
    }
    pub const fn actions(&self) -> &TiingoEodHistoryActionProjection {
        &self.actions
    }
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.history.read_receipt().knowledge_cutoff()
    }
    pub fn records(&self) -> &[CorporateActionRecord] {
        &self.records
    }
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}
impl AnalyticalDataService {
    /// Synchronous work belongs on the existing owned controlled raw-reader lane. Both receipts
    /// below are opaque data reads; every original component is physically reopened and verified.
    pub fn rejoin_tiingo_eod_action_history(
        &self,
        history: CompleteMarketBarHistoryOutput,
        owned: &GenerationOwnedProviderCaptureEvidence,
        store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
    ) -> Result<RetainedTiingoEodActionHistory, IngestError> {
        let invalid = || IngestError::ProviderCaptureRequired;
        let checkpoint = || {
            control
                .checkpoint(market_squawk_platform::ResearchObjectControlPoint::BeforeVerification)
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
        let cutoff = history.read_receipt().knowledge_cutoff();
        let graph = receipt.date_windows().ok_or_else(invalid)?;
        let (instrument, contract) =
            market_squawk_adapter_tiingo::reconstruct_eod_history_mapping(graph)
                .map_err(|_| invalid())?;
        if receipt.source_id().as_str() != "tiingo-starter"
            || owned.pinned().manifest() != receipt.origin_manifest()
            || owned.source_id() != receipt.source_id()
            || owned.published_at() != receipt.published_at()
            || receipt.published_at() > cutoff
            || contract.source_id() != receipt.source_id()
            || graph.instrument_id() != instrument.instrument_id()
            || graph.instrument_revision_digest()
                != instrument
                    .instrument_definition()
                    .payload_evidence()
                    .content_digest()
            || graph.venue_id() != instrument.venue_id()
            || instrument.currency() != receipt.currency()
        {
            return Err(invalid());
        }
        let object = owned
            .objects()
            .iter()
            .find(|object| {
                object.generation_object_ordinal() == usize::from(receipt.origin_object_ordinal())
            })
            .ok_or_else(invalid)?;
        if object.object().artifact_id() != receipt.origin_artifact_id()
            || object.inputs().len() != 1
        {
            return Err(invalid());
        }
        let binding = object.inputs()[0].binding();
        let capture = binding.capture();
        if binding.binding_digest().bytes() != receipt.binding_digest().bytes()
            || binding.sealed_capture_receipt_digest().bytes()
                != receipt.capture_receipt_digest().bytes()
            || (
                capture.content_digest().bytes(),
                capture.observation_digest().bytes(),
            ) != (
                receipt.capture_graph_digests().0.bytes(),
                receipt.capture_graph_digests().1.bytes(),
            )
            || capture.metadata_revision() != contract.source_contract_revision()
            || capture.semantic_binding()
                != Some(
                    &ProviderCaptureSemanticBinding::CompleteMarketBarDateWindowsV1(graph.clone()),
                )
            || binding.layout() != "ordered_segments"
            || binding.scope() != "whole"
            || binding.record_count() != receipt.origin_record_count() as usize
            || capture.pages().len() != graph.windows().len() + 1
            || capture.request_graph_components().len() != capture.pages().len()
            || binding.physical_claims().len() != capture.pages().len()
        {
            return Err(invalid());
        }
        let (start, end) = graph.requested_dates();
        let plan = TiingoHistoryPlan::try_new(instrument.ticker().clone(), start, end)
            .map_err(|_| invalid())?;
        let normalization = graph.normalization();
        let decoder = TiingoDecoder::new(
            normalization.native_schema_revision.clone(),
            normalization.entitlement_generation.clone(),
        );
        let mut metadata = None;
        let mut responses: Vec<TiingoEodReceipt> = Vec::new();
        responses
            .try_reserve_exact(graph.windows().len())
            .map_err(|_| invalid())?;
        for (index, ((component, page), physical)) in capture
            .request_graph_components()
            .iter()
            .zip(capture.pages())
            .zip(binding.physical_claims())
            .enumerate()
        {
            let request = if index == 0 {
                TiingoRequestSpec::metadata(instrument.ticker().clone()).map_err(|_| invalid())?
            } else {
                plan.pages().get(index - 1).cloned().ok_or_else(invalid)?
            };
            if component.ordinal() as usize != index
                || component.first_page_ordinal() as usize != index
                || component.page_count().get() != 1
                || component.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
                || component.request_set_identity() != request.request_identity()
                || page.request_identity() != request.request_identity()
                || component.source_id() != capture.source_id()
                || component.metadata_revision() != capture.metadata_revision()
                || component.content_digest() != physical.capture_content_digest()
                || component.observation_digest() != physical.capture_observation_digest()
                || page.received_at() > cutoff
            {
                return Err(invalid());
            }
            let segment = store.open_verified_claim_with_control(physical.claim(), control)?;
            let standalone = ProviderCaptureSetReceipt::try_new(
                component.source_id().clone(),
                component.metadata_revision().clone(),
                component.dataset().clone(),
                component.request_set_identity(),
                component.terminal(),
                vec![
                    ProviderCapturePageReceipt::try_new(
                        0,
                        page.request_identity(),
                        None,
                        None,
                        page.http_status(),
                        page.body_bytes(),
                        page.body_digest(),
                        page.received_at(),
                    )
                    .map_err(|_| invalid())?,
                ],
            )
            .map_err(|_| invalid())?;
            if standalone.content_digest() != component.content_digest()
                || standalone.observation_digest() != component.observation_digest()
            {
                return Err(invalid());
            }
            let sealed =
                SealedProviderCaptureSetReceipt::try_bind(standalone, segment.receipt().clone())
                    .map_err(|_| invalid())?;
            if sealed.receipt_digest() != physical.sealed_capture_receipt_digest()
                || segment.records().len() != 1
            {
                return Err(invalid());
            }
            let body = segment.records()[0].payload();
            if index == 0 {
                metadata = Some(
                    decoder
                        .decode_metadata(
                            request,
                            page.http_status(),
                            body,
                            page.received_at(),
                            normalization.metadata_decoded_at,
                        )
                        .map_err(|_| invalid())?,
                );
            } else {
                let window = &graph.windows()[index - 1];
                if window.ingested_at > cutoff || window.decoded_at > window.ingested_at {
                    return Err(invalid());
                }
                responses.push(
                    decoder
                        .decode_eod(
                            request,
                            page.http_status(),
                            body,
                            page.received_at(),
                            window.decoded_at,
                        )
                        .map_err(|_| invalid())?,
                );
            }
        }
        let metadata = metadata.ok_or_else(invalid)?;
        let native_coverage = match metadata.metadata().coverage() {
            market_squawk_adapter_tiingo::TiingoCoverage::Supported {
                start_date,
                end_date,
            } => market_squawk_sources::RetainedMarketHistoryNativeCoverageV1::Supported {
                start: start_date,
                end: end_date,
            },
            market_squawk_adapter_tiingo::TiingoCoverage::Unsupported => {
                market_squawk_sources::RetainedMarketHistoryNativeCoverageV1::Unsupported
            }
        };
        if metadata.metadata().ticker() != instrument.ticker()
            || metadata.metadata().exchange_code() != instrument.provider_exchange_code().as_str()
            || native_coverage != normalization.native_coverage
        {
            return Err(invalid());
        }

        verify_eod_history_native_sidecar(
            &metadata,
            &responses,
            binding
                .native_lineage()
                .batch_sidecar_semantic_payload()
                .ok_or_else(invalid)?,
        )
        .map_err(|_| invalid())?;
        let row_digests: Vec<Vec<EvidenceDigest>> = responses
            .iter()
            .map(|response| response.rows().iter().map(|row| row.row_digest()).collect())
            .collect();
        let pages: Vec<_> = responses
            .iter()
            .zip(&row_digests)
            .zip(graph.windows())
            .map(
                |((response, native_row_digests), window)| TiingoEodActionReplayPage {
                    response,
                    ingested_at: window.ingested_at,
                    native_row_digests,
                },
            )
            .collect();
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
        let actions =
            rejoin_eod_history_actions(graph, &plan, &instrument, &contract, &pages, unit.as_ref())
                .map_err(|_| invalid())?;
        if actions.observations().len() != history.source_actions().len() {
            return Err(invalid());
        }
        let action_start = binding
            .rows()
            .len()
            .checked_sub(actions.observations().len())
            .ok_or_else(invalid)?;
        // Walk the original canonical bar prefix once in source page/row/surface order.
        // Missing source fields consume no canonical bar and remain in the full sidecar.
        let mut native_bar_ordinal = 0_usize;
        for (page_index, response) in responses.iter().enumerate() {
            for native in response.rows() {
                checkpoint()?;
                for (ohlc, volume, component) in [
                    (native.raw_ohlc(), native.volume(), "raw"),
                    (native.adjusted_ohlc(), native.adjusted_volume(), "adjusted"),
                ] {
                    if !matches!(ohlc, (Some(_), Some(_), Some(_), Some(_))) || volume.is_none() {
                        continue;
                    }
                    let retained = binding
                        .rows()
                        .get(native_bar_ordinal)
                        .filter(|_| native_bar_ordinal < action_start)
                        .ok_or_else(invalid)?;
                    verify_eod_history_native_row(
                        native,
                        component,
                        retained.native_semantic_payload(),
                    )
                    .map_err(|_| invalid())?;
                    if retained.capture_page_ordinal() as usize != page_index + 1
                        || retained.segment_ordinal() != retained.capture_page_ordinal()
                        || retained.physical_frame_ordinal() != 0
                        || retained.received_at() != response.evidence().received_at()
                    {
                        return Err(invalid());
                    }
                    native_bar_ordinal += 1;
                }
            }
        }
        if native_bar_ordinal != action_start {
            return Err(invalid());
        }
        for (index, (expected, actual)) in actions
            .observations()
            .iter()
            .zip(history.source_actions())
            .enumerate()
        {
            checkpoint()?;
            let expected_value = ResearchObservation::CorporateAction(expected.observation.clone())
                .with_revision(actual.context().time().revision())
                .map_err(|_| invalid())?;
            if expected_value != ResearchObservation::CorporateAction(actual.clone()) {
                return Err(invalid());
            }
            let native = responses
                .get(expected.history_page_index)
                .and_then(|response| response.rows().get(expected.provider_row_index as usize))
                .ok_or_else(invalid)?;
            let selected = match actual.action() {
                CorporateActionKind::Split { .. } => "split",
                CorporateActionKind::CashDividend { .. } => "dividend",
                _ => return Err(invalid()),
            };
            let retained = &binding.rows()[action_start + index];
            verify_eod_history_native_row(native, selected, retained.native_semantic_payload())
                .map_err(|_| invalid())?;
            if retained.capture_page_ordinal() as usize != expected.history_page_index + 1
                || retained.segment_ordinal() != retained.capture_page_ordinal()
                || retained.physical_frame_ordinal() != 0
                || retained.received_at() != actual.context().provenance().received_at()
            {
                return Err(invalid());
            }
        }
        // Selected OHLCV must be equal to its own native surface; the companion can never
        // supply missing prices. Complete graph/date membership is checked again by projection.
        for (bar, session) in history.bars().iter().zip(graph.sessions()) {
            checkpoint()?;
            let page_index = graph
                .windows()
                .partition_point(|window| window.end_date < session.date);
            let response = responses.get(page_index).ok_or_else(invalid)?;
            let row_index = response
                .rows()
                .binary_search_by_key(&session.date, |row| row.date())
                .map_err(|_| invalid())?;
            let native = &response.rows()[row_index];
            let (ohlc, volume) = match bar.adjustment() {
                MarketBarAdjustment::Raw => (native.raw_ohlc(), native.volume()),
                MarketBarAdjustment::All => (native.adjusted_ohlc(), native.adjusted_volume()),
                _ => return Err(invalid()),
            };
            if ohlc
                != (
                    Some(bar.open().amount()),
                    Some(bar.high().amount()),
                    Some(bar.low().amount()),
                    Some(bar.close().amount()),
                )
                || volume != Some(bar.volume())
                || bar.time_semantics() != &session.time
                || bar.currency() != instrument.currency()
                || bar.trade_count().is_some()
                || bar.vwap().is_some()
                || bar.context().provenance().received_at() != response.evidence().received_at()
                || bar.context().provenance().ingested_at()
                    != graph.windows()[page_index].ingested_at
            {
                return Err(invalid());
            }
        }
        let mut records = Vec::new();
        records
            .try_reserve_exact(history.source_actions().len())
            .map_err(|_| invalid())?;
        for (observation, row) in history
            .source_actions()
            .iter()
            .zip(&binding.rows()[action_start..])
        {
            checkpoint()?;
            records.push(CorporateActionRecord::new(
                observation.clone(),
                history.read_receipt().origin_manifest().clone(),
                row.canonical_row_digest(),
            ));
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/retained-tiingo-eod-action-history/v1\0");
        hash.update(owned.receipt_digest().bytes());
        hash.update(binding.binding_digest().bytes());
        hash.update(receipt.receipt_digest().bytes());
        hash.update(history.read_receipt().source_result_digest().bytes());
        hash.update(actions.completion_identity().bytes());
        hash.update(actions.request_set_identity().bytes());
        let evidence_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
        checkpoint()?;
        Ok(RetainedTiingoEodActionHistory {
            binding: binding.clone(),
            history,
            actions,
            records: records.into_boxed_slice(),
            evidence_digest,
        })
    }
}
