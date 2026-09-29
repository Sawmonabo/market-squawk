//! Original economic-query capture sealed into neutral source evidence and admitted economics.
use crate::decoder::digest;
use crate::{
    TiingoAdapterError, TiingoCapturedPage, TiingoCorporateActionReceipt,
    TiingoCorporateActionValue, TiingoRequestScope,
};
use bytes::Bytes;
use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailability, CalendarDate,
    CorporateActionEconomicQueryContract, CorporateActionEconomicSourceScope,
    CorporateActionEconomicTerms, CorporateActionEventInstrumentIdentity,
    CorporateActionObservation, CorporateActionQueryInstrumentIdentity,
    CorporateActionSourceDisposition, CorporateActionSourceObservation,
    CorporateActionSourceObservationInput, CorporateActionSourcePayload, EffectiveInterval,
    EvidenceDigest, ExactPayloadEvidence, InstrumentId, PayloadHash, PayloadReference,
    ResearchContext, ResearchObservation, ResearchProvenance, ResearchProvenanceInput,
    ResearchTemporalCoordinate, ResearchTime, RevisionNumber, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryRequest, ExtractionBatch,
    ExtractionBatchAccumulator, ExtractionRecord, ExtractionRequest, ExtractionRevisionPlan,
    ProviderNativeLineageBatchBuilder, ProviderNativeLineageImplementation,
    ProviderWholeCaptureToken, SealedProviderCaptureBinding, SourceMetadata, SourceObject,
    SourceObjectCaptureIdentity,
};
use serde_json::{Value, json};
const MEDIA_TYPE: &str = "application/vnd.market-squawk.tiingo-corporate-actions+json";

/// Sealed response plus original decoded rows. No caller-created row vector enters this owner.
#[derive(Debug)]
pub struct TiingoPreparedCorporateActionsPublication {
    captured: TiingoCapturedPage<TiingoCorporateActionReceipt>,
    authority: ProviderWholeCaptureToken,
    metadata: SourceMetadata,
}
impl TiingoPreparedCorporateActionsPublication {
    /// Rejoins the successful original response to its exact physical capture token.
    pub fn try_new(
        captured: TiingoCapturedPage<TiingoCorporateActionReceipt>,
        authority: ProviderWholeCaptureToken,
        metadata: SourceMetadata,
    ) -> Result<Self, TiingoAdapterError> {
        let original = captured.raw().capture();
        if authority.persisted_receipt().capture() != original
            || metadata.source_id() != original.source_id()
            || metadata.revision() != original.metadata_revision()
            || metadata.source_id().as_str() != "tiingo-starter"
        {
            return Err(invalid());
        }
        Ok(Self {
            captured,
            authority,
            metadata,
        })
    }
    /// Original code-owned source dataset.
    pub fn dataset(&self) -> &SourceIdentifier {
        self.captured.raw().capture().dataset()
    }
    /// Original source metadata, preserving its publication authority.
    pub fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
    /// Builds an exact discovery object from the sealed original response.
    pub fn source_object(
        &self,
        request: &DiscoveryRequest,
    ) -> Result<SourceObject, TiingoAdapterError> {
        if request.dataset() != self.dataset() || request.effective_at().is_some() {
            return Err(invalid());
        }
        let capture = self.authority.persisted_receipt().capture();
        let received = self.captured.decoded().evidence().received_at();
        SourceObject::try_new_with_capture_identity(
            self.metadata.source_id().clone(),
            self.metadata.revision().clone(),
            request,
            identifier(&format!(
                "tiingo:corporate-actions:object:{}",
                hex(capture.content_digest())
            ))?,
            identifier(MEDIA_TYPE)?,
            ExactPayloadEvidence::from_content_digest(capture.content_digest()),
            SourceObjectCaptureIdentity::try_from_capture(capture).map_err(|_| invalid())?,
            EffectiveInterval::new(received, None).map_err(|_| invalid())?,
            None,
            AvailabilityEvidence::LocalFirstObserved {
                observed_at: received,
            },
            Some(capture.total_body_bytes()),
        )
        .map_err(|_| invalid())
    }
    /// Publishes each original disposition and only HIGH-admitted economic terms. Inert catalog
    /// coordinates here must be rejoined by the application's original precommit/read authority.
    pub fn try_into_binding(
        self,
        request: &ExtractionRequest,
        query_instruments: &[CorporateActionQueryInstrumentIdentity],
        event_identities: &[CorporateActionEventInstrumentIdentity],
        ingested_at: Timestamp,
    ) -> Result<SealedProviderCaptureBinding, TiingoAdapterError> {
        let receipt = self.captured.decoded();
        let evidence = receipt.evidence();
        let capture = self.authority.persisted_receipt().capture();
        let received = evidence.received_at();
        let (date, contract) = match evidence.request().scope() {
            TiingoRequestScope::Distributions { date } => (
                *date,
                CorporateActionEconomicQueryContract::TiingoDistributionsTickerExDatesV1,
            ),
            TiingoRequestScope::Splits { date } => (
                *date,
                CorporateActionEconomicQueryContract::TiingoSplitsAllSymbolsExDateV1,
            ),
            _ => return Err(invalid()),
        };
        if query_instruments.len() != 1
            || !query_instruments[0].valid_for_economic_capture(received)
            || query_instruments[0].symbol.as_str() != evidence.request().ticker().as_str()
            || ingested_at < evidence.decoded_at()
            || request.object().source_id() != self.metadata.source_id()
            || request.object().metadata_revision() != self.metadata.revision()
            || request.object().dataset() != self.dataset()
            || request.object().media_type().as_str() != MEDIA_TYPE
            || request.object().evidence().content_digest() != capture.content_digest()
            || request.object().object_id().as_str()
                != format!(
                    "tiingo:corporate-actions:object:{}",
                    hex(capture.content_digest())
                )
            || request.object().capture_identity()
                != SourceObjectCaptureIdentity::try_from_capture(capture).map_err(|_| invalid())?
            || request.object().expected_bytes() != Some(capture.total_body_bytes())
            || request.object().published_at().is_some()
            || request.object().availability()
                != &(AvailabilityEvidence::LocalFirstObserved {
                    observed_at: received,
                })
            || request.object().effective_interval()
                != EffectiveInterval::new(received, None).map_err(|_| invalid())?
        {
            return Err(invalid());
        }
        for (index, identity) in event_identities.iter().enumerate() {
            if !identity.valid_for_event(ingested_at)
                || identity.source_id != *self.metadata.source_id()
                || identity.selection.instrument_id != query_instruments[0].instrument_id
                || !receipt
                    .rows()
                    .iter()
                    .any(|row| row.ticker().as_str() == identity.provider_instrument_id.as_str())
                || event_identities[..index]
                    .iter()
                    .any(|prior| prior.selection.instrument_id == identity.selection.instrument_id)
            {
                return Err(invalid());
            }
        }
        let scope = CorporateActionEconomicSourceScope {
            dataset: self.dataset().clone(),
            ex_date_start: date,
            ex_date_end: date,
            query_contract: contract,
            query_instruments: query_instruments.to_vec(),
            capture_observation_digest: capture.observation_digest(),
            sealed_capture_receipt_digest: self.authority.persisted_receipt().receipt_digest(),
        };
        let mut accumulator =
            ExtractionBatchAccumulator::try_new(request).map_err(|_| invalid())?;
        let mut native_rows = Vec::new();
        let mut raw_rows = Vec::new();
        let mut normalized_count = 0u32;
        for (index, row) in receipt.rows().iter().enumerate() {
            let original: Value =
                serde_json::from_slice(row.native_payload()).map_err(|_| invalid())?;
            raw_rows.push(original.clone());
            let id = identifier(&format!("tiingo:corporate-action:{}", hex(row.digest())))?;
            let identity = event_identities
                .iter()
                .find(|identity| identity.provider_instrument_id.as_str() == row.ticker().as_str());
            let (terms, status, payable) = match row.value() {
                TiingoCorporateActionValue::Distribution {
                    amount,
                    frequency,
                    payment_date,
                    ..
                } => (
                    CorporateActionEconomicTerms::Distribution {
                        distribution: *amount,
                    },
                    identifier(frequency)?,
                    *payment_date,
                ),
                TiingoCorporateActionValue::Split {
                    from,
                    to,
                    factor,
                    status,
                } => (
                    CorporateActionEconomicTerms::Split {
                        split_from: *from,
                        split_to: *to,
                        split_factor: *factor,
                    },
                    identifier(status)?,
                    None,
                ),
            };
            let context = source_context(
                &self.metadata,
                evidence,
                ingested_at,
                id.clone(),
                identity.map(|v| v.selection.instrument_id),
                row.ex_date(),
            )?;
            let native = |kind: &str| json!({"row_kind":kind,"provider_row_index":index,"action_id":id,"native_row":original,"subject_identity":identity});
            let disposition = match terms.normalize_kind(None, Some(&status)) {
                Ok(kind) if identity.is_some() => {
                    let observation = CorporateActionObservation::new(context.clone(), kind)
                        .map_err(|_| invalid())?;
                    push_record(
                        &mut accumulator,
                        request,
                        ResearchObservation::CorporateAction(observation),
                    )?;
                    native_rows.push(native("economic"));
                    normalized_count = normalized_count.checked_add(1).ok_or_else(invalid)?;
                    CorporateActionSourceDisposition::Normalized
                }
                Ok(_) => CorporateActionSourceDisposition::MissingIdentity,
                Err(disposition) => disposition,
            };
            let observation =
                CorporateActionSourceObservation::try_new(CorporateActionSourceObservationInput {
                    context,
                    payload: CorporateActionSourcePayload::EconomicReturnedAction {
                        action_id: id.clone(),
                        query_contract: contract,
                        native_symbol: identifier(row.ticker().as_str())?,
                        native_perma_ticker: Some(identifier(row.perma_ticker())?),
                        ex_date: row.ex_date(),
                        payable_date: payable,
                        native_status: Some(status),
                        native_terms: terms,
                        native_row_digest: row.digest(),
                        currency: None,
                        disposition,
                    },
                })
                .map_err(|_| invalid())?;
            push_record(
                &mut accumulator,
                request,
                ResearchObservation::CorporateActionSource(observation),
            )?;
            native_rows.push(native("source_disposition"));
        }
        let summary =
            CorporateActionSourceObservation::try_new(CorporateActionSourceObservationInput {
                context: source_context(
                    &self.metadata,
                    evidence,
                    ingested_at,
                    identifier(&format!(
                        "tiingo:corporate-actions:coverage:{}",
                        hex(capture.observation_digest())
                    ))?,
                    None,
                    date,
                )?,
                payload: CorporateActionSourcePayload::EconomicQuerySummary {
                    scope: scope.clone(),
                    returned_count: u32::try_from(receipt.rows().len()).map_err(|_| invalid())?,
                    normalized_count,
                },
            })
            .map_err(|_| invalid())?;
        push_record(
            &mut accumulator,
            request,
            ResearchObservation::CorporateActionSource(summary),
        )?;
        native_rows.push(json!({"row_kind":"economic_query_summary","capture_observation_digest":capture.observation_digest()}));
        let batch = accumulator.finish().map_err(|_| invalid())?;
        let mut native = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::TiingoCorporateActionsV1,
            &batch,
        )
        .map_err(|_| invalid())?;
        native.try_set_batch_sidecar(&json!({"version":1,"request_url":evidence.request().url().as_str(),"request_identity":evidence.request().request_identity(),"query_scope":scope,"native_contract_revision":evidence.native_contract_revision(),"entitlement_generation":evidence.entitlement_generation(),"received_at":received,"decoded_at":evidence.decoded_at(),"raw_rows":raw_rows})).map_err(|_|invalid())?;
        for row in &native_rows {
            native.try_push(row).map_err(|_| invalid())?;
        }
        let lineage = native.finish().map_err(|_| invalid())?;
        SealedProviderCaptureBinding::try_whole(
            self.authority,
            batch,
            lineage,
            vec![0; native_rows.len()],
        )
        .map_err(|_| invalid())
    }
    /// Uses the existing observed-content revision authority, never an inferred event revision.
    pub fn revision_plan(
        batch: &ExtractionBatch,
    ) -> Result<ExtractionRevisionPlan, TiingoAdapterError> {
        if batch.request().object().media_type().as_str() != MEDIA_TYPE {
            return Err(invalid());
        }
        ExtractionRevisionPlan::locally_observed(batch.records().len()).map_err(|_| invalid())
    }
}
fn source_context(
    metadata: &SourceMetadata,
    evidence: &crate::TiingoResponseEvidence,
    ingested_at: Timestamp,
    id: SourceIdentifier,
    instrument: Option<InstrumentId>,
    date: CalendarDate,
) -> Result<ResearchContext, TiingoAdapterError> {
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: metadata.source_id().clone(),
        instrument_id: instrument,
        venue_id: None,
        source_identifier: id,
        source_timestamp: None,
        received_at: evidence.received_at(),
        ingested_at,
        quality: metadata.quality_ceiling(),
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            evidence.body_digest().algorithm(),
            evidence.body_digest().bytes(),
        )),
        availability: ResearchAvailability::local_first_observed(evidence.received_at()),
    })
    .map_err(|_| invalid())?;
    let time = ResearchTime::try_new_with_coordinates(
        ResearchTemporalCoordinate::calendar_date(date),
        None,
        RevisionNumber::new(1).map_err(|_| invalid())?,
        None,
    )
    .map_err(|_| invalid())?;
    ResearchContext::new(provenance, time).map_err(|_| invalid())
}
fn push_record(
    accumulator: &mut ExtractionBatchAccumulator,
    request: &ExtractionRequest,
    observation: ResearchObservation,
) -> Result<(), TiingoAdapterError> {
    let context = match &observation {
        ResearchObservation::CorporateAction(v) => v.context(),
        ResearchObservation::CorporateActionSource(v) => v.context(),
        _ => return Err(invalid()),
    };
    let effective = context.time().effective().clone();
    let received = context.provenance().received_at();
    let payload = Bytes::from(serde_json::to_vec(&observation).map_err(|_| invalid())?);
    let evidence = ExactPayloadEvidence::from_content_digest(digest(&payload));
    let observation_id = identifier(&format!("observed:{}", hex(evidence.content_digest())))?;
    accumulator
        .push(
            ExtractionRecord::try_new_with_time(
                request,
                identifier(CURRENT_RESEARCH_RECORD_SCHEMA)?,
                evidence,
                effective,
                None,
                AvailabilityEvidence::LocalFirstObserved {
                    observed_at: received,
                },
                observation_id,
                None,
                payload,
            )
            .map_err(|_| invalid())?,
        )
        .map_err(|_| invalid())
}
fn identifier(value: &str) -> Result<SourceIdentifier, TiingoAdapterError> {
    SourceIdentifier::try_from(value).map_err(|_| invalid())
}
fn hex(value: EvidenceDigest) -> String {
    value
        .bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn invalid() -> TiingoAdapterError {
    TiingoAdapterError::InvalidResponseSelection
}
