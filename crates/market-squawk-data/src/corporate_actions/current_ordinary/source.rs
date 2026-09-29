//! Exact native membership closure and sole issuer of current ordinary source reads.
use super::{
    CurrentOrdinaryActionDisposition, CurrentOrdinaryActionFamily, CurrentOrdinaryActionRow,
    CurrentOrdinaryActionSourceRead,
};
use crate::corporate_actions::source_capture::CorporateActionSourceCaptureError;
use crate::{
    AnalyticalReadCapability, CorporateActionRecord, CorporateActionSourceReadError as ReadError,
    GenerationOwnedProviderCaptureEvidence, MarketDataInstrumentReadCapability,
    PersistedProviderCaptureBindingEvidence,
};
use market_squawk_domain::{
    CalendarDate, CorporateActionEconomicQueryContract as Contract,
    CorporateActionEconomicSourceScope, CorporateActionEconomicTerms,
    CorporateActionEventInstrumentIdentity, CorporateActionSourceDisposition as Disposition,
    CorporateActionSourcePayload as Payload, DigestAlgorithm, EvidenceDigest, InstrumentId,
    ResearchObservation, SourceIdentifier, Timestamp,
};
use market_squawk_sources::ProviderCaptureTerminalDisposition;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, time::Instant};
use tokio_util::sync::CancellationToken;
type Error = CorporateActionSourceCaptureError;
fn invalid() -> Error {
    CorporateActionSourceCaptureError
}
const MAX_NATIVE_ROWS: usize = 4096;
const MAX_AUDIT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CurrentOrdinarySidecar {
    pub(crate) version: u16,
    pub(crate) request_url: String,
    pub(crate) request_identity: EvidenceDigest,
    pub(crate) query_scope: CorporateActionEconomicSourceScope,
    pub(crate) native_contract_revision: SourceIdentifier,
    pub(crate) entitlement_generation: SourceIdentifier,
    pub(crate) received_at: Timestamp,
    pub(crate) decoded_at: Timestamp,
    pub(crate) raw_rows: Vec<Value>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeRow {
    row_kind: String,
    provider_row_index: u32,
    action_id: SourceIdentifier,
    native_row: Value,
    subject_identity: Option<CorporateActionEventInstrumentIdentity>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeSummary {
    row_kind: String,
    capture_observation_digest: EvidenceDigest,
}
struct NativeTerms {
    symbol: SourceIdentifier,
    perma_ticker: SourceIdentifier,
    date: CalendarDate,
    payable: Option<CalendarDate>,
    status: SourceIdentifier,
    terms: CorporateActionEconomicTerms,
    digest: EvidenceDigest,
    action_id: SourceIdentifier,
}

pub(crate) fn validate_current_ordinary_capture(
    observations: &[ResearchObservation],
    binding: &PersistedProviderCaptureBindingEvidence,
) -> Result<(), Error> {
    binding.verify_integrity().map_err(|_| invalid())?;
    let capture = binding.capture();
    let [page] = capture.pages() else {
        return Err(invalid());
    };
    if binding.native_lineage().implementation() != "tiingo_corporate_actions_v1"
        || capture.source_id().as_str() != "tiingo-starter"
        || binding.scope() != "whole"
        || binding.layout() != "whole_single_segment"
        || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        || page.ordinal() != 0
        || page.request_page_token_digest().is_some()
        || page.response_next_page_token_digest().is_some()
        || page.body_bytes() > 2 * 1024 * 1024
        || observations.len() != binding.record_count()
        || observations.len() > 8193
    {
        return Err(invalid());
    }
    let audit = binding
        .native_lineage()
        .batch_sidecar_semantic_payload()
        .ok_or_else(invalid)?;
    if audit.len() > MAX_AUDIT_BYTES {
        return Err(invalid());
    }
    let sidecar: CurrentOrdinarySidecar = serde_json::from_slice(audit).map_err(|_| invalid())?;
    let scope = &sidecar.query_scope;
    validate_scope(scope, sidecar.received_at)?;
    let (url, request) = request_identity(scope)?;
    if sidecar.version != 1
        || sidecar.raw_rows.len() > MAX_NATIVE_ROWS
        || sidecar.request_url != url
        || sidecar.request_identity != request
        || request != page.request_identity()
        || request != capture.request_set_identity()
        || scope.dataset != *capture.dataset()
        || scope.capture_observation_digest != capture.observation_digest()
        || scope.sealed_capture_receipt_digest != binding.sealed_capture_receipt_digest()
        || sidecar.received_at != page.received_at()
        || sidecar.decoded_at < sidecar.received_at
        || sidecar.native_contract_revision.as_str().is_empty()
        || sidecar.entitlement_generation.as_str().is_empty()
    {
        return Err(invalid());
    }
    let mut cursor = 0usize;
    let mut normalized = 0u32;
    let mut seen = std::collections::BTreeSet::new();
    for (index, raw) in sidecar.raw_rows.iter().enumerate() {
        let original = native_terms(raw, scope.query_contract)?;
        if !seen.insert(original.digest.bytes())
            || original.date != scope.ex_date_start
            || (scope.query_contract == Contract::TiingoDistributionsTickerExDatesV1
                && original.symbol != scope.query_instruments[0].symbol)
        {
            return Err(invalid());
        }
        let mut economic = None;
        if matches!(
            observations.get(cursor),
            Some(ResearchObservation::CorporateAction(_))
        ) {
            economic = Some(cursor);
            cursor += 1;
        }
        let Some(ResearchObservation::CorporateActionSource(source)) = observations.get(cursor)
        else {
            return Err(invalid());
        };
        let native: NativeRow =
            serde_json::from_slice(binding.rows()[cursor].native_semantic_payload())
                .map_err(|_| invalid())?;
        let Payload::EconomicReturnedAction {
            action_id,
            query_contract,
            native_symbol,
            native_perma_ticker,
            ex_date,
            payable_date,
            native_status,
            native_terms,
            native_row_digest,
            currency,
            disposition,
        } = source.payload()
        else {
            return Err(invalid());
        };
        let expected = match original.terms.normalize_kind(None, Some(&original.status)) {
            Ok(_) if native.subject_identity.is_some() => Disposition::Normalized,
            Ok(_) => Disposition::MissingIdentity,
            Err(value) => value,
        };
        if native.row_kind != "source_disposition"
            || native.provider_row_index as usize != index
            || native.action_id != original.action_id
            || native.native_row != *raw
            || action_id != &original.action_id
            || *query_contract != scope.query_contract
            || native_symbol != &original.symbol
            || native_perma_ticker.as_ref() != Some(&original.perma_ticker)
            || *ex_date != original.date
            || *payable_date != original.payable
            || native_status.as_ref() != Some(&original.status)
            || native_terms != &original.terms
            || *native_row_digest != original.digest
            || currency.is_some()
            || *disposition != expected
            || source.context().provenance().source_identifier() != &original.action_id
            || source.context().provenance().instrument_id()
                != native
                    .subject_identity
                    .as_ref()
                    .map(|v| v.selection.instrument_id)
            || source.context().time().effective().calendar_date_value() != Some(original.date)
        {
            return Err(invalid());
        }
        if let Some(identity) = &native.subject_identity {
            if identity.source_id != *capture.source_id()
                || identity.provider_instrument_id.as_str() != original.symbol.as_str()
                || identity.selection.instrument_id != scope.query_instruments[0].instrument_id
                || !identity.valid_for_event(source.context().provenance().ingested_at())
            {
                return Err(invalid());
            }
        }
        match (economic, expected) {
            (Some(economic), Disposition::Normalized) => {
                let ResearchObservation::CorporateAction(action) = &observations[economic] else {
                    return Err(invalid());
                };
                let economic_native: NativeRow =
                    serde_json::from_slice(binding.rows()[economic].native_semantic_payload())
                        .map_err(|_| invalid())?;
                if economic_native.row_kind != "economic"
                    || economic_native.provider_row_index != native.provider_row_index
                    || economic_native.action_id != native.action_id
                    || economic_native.native_row != native.native_row
                    || economic_native.subject_identity != native.subject_identity
                    || action.action()
                        != &original
                            .terms
                            .normalize_kind(None, Some(&original.status))
                            .map_err(|_| invalid())?
                    || action.context().provenance() != source.context().provenance()
                    || action.context().time().effective().calendar_date_value()
                        != Some(original.date)
                {
                    return Err(invalid());
                }
                normalized = normalized.checked_add(1).ok_or_else(invalid)?;
            }
            (None, Disposition::Normalized) | (Some(_), _) => return Err(invalid()),
            (None, _) => {}
        }
        cursor += 1;
    }
    if cursor + 1 != observations.len() {
        return Err(invalid());
    }
    let ResearchObservation::CorporateActionSource(summary) = &observations[cursor] else {
        return Err(invalid());
    };
    let Payload::EconomicQuerySummary {
        scope: retained_scope,
        returned_count,
        normalized_count,
    } = summary.payload()
    else {
        return Err(invalid());
    };
    let native: NativeSummary =
        serde_json::from_slice(binding.rows()[cursor].native_semantic_payload())
            .map_err(|_| invalid())?;
    if retained_scope != scope
        || *returned_count as usize != sidecar.raw_rows.len()
        || *normalized_count != normalized
        || native.row_kind != "economic_query_summary"
        || native.capture_observation_digest != capture.observation_digest()
        || summary.context().provenance().instrument_id().is_some()
        || summary.context().provenance().source_identifier().as_str()
            != format!(
                "tiingo:corporate-actions:coverage:{}",
                hex(capture.observation_digest())
            )
        || summary.context().time().effective().calendar_date_value() != Some(scope.ex_date_start)
    {
        return Err(invalid());
    }
    for (observation, row) in observations.iter().zip(binding.rows()) {
        let context = match observation {
            ResearchObservation::CorporateActionSource(v) => v.context(),
            ResearchObservation::CorporateAction(v) => v.context(),
            _ => return Err(invalid()),
        };
        let p = context.provenance();
        if p.source_id() != capture.source_id()
            || p.quality() != market_squawk_domain::DataQuality::Aggregated
            || p.received_at() != page.received_at()
            || p.ingested_at() < sidecar.decoded_at
            || p.source_timestamp().is_some()
            || p.venue_id().is_some()
            || context.time().published().is_some()
            || p.availability().conservative_available_at() != Some(page.received_at())
            || p.payload_reference()
                != &market_squawk_domain::PayloadReference::ContentHash(
                    market_squawk_domain::PayloadHash::new(
                        page.body_digest().algorithm(),
                        page.body_digest().bytes(),
                    ),
                )
            || row.capture_page_ordinal() != 0
            || row.received_at() != page.received_at()
            || row.page_body_digest() != page.body_digest()
        {
            return Err(invalid());
        }
    }
    Ok(())
}

pub(crate) fn validate_scope(
    scope: &CorporateActionEconomicSourceScope,
    received_at: Timestamp,
) -> Result<(), Error> {
    if scope.query_instruments.len() != 1
        || scope.ex_date_start != scope.ex_date_end
        || !scope.query_instruments[0].valid_for_economic_capture(received_at)
        || scope.dataset.as_str()
            != match scope.query_contract {
                Contract::TiingoDistributionsTickerExDatesV1 => {
                    "tiingo-corporate-action-distributions"
                }
                Contract::TiingoSplitsAllSymbolsExDateV1 => "tiingo-corporate-action-splits",
            }
    {
        return Err(invalid());
    }
    Ok(())
}
pub(crate) fn request_identity(
    scope: &CorporateActionEconomicSourceScope,
) -> Result<(String, EvidenceDigest), Error> {
    let [identity] = scope.query_instruments.as_slice() else {
        return Err(invalid());
    };
    let ticker = identity.symbol.as_str();
    if ticker.is_empty()
        || ticker.len() > 64
        || !ticker
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
    {
        return Err(invalid());
    }
    let mut url = url::Url::parse("https://api.tiingo.com/").map_err(|_| invalid())?;
    let split = scope.query_contract == Contract::TiingoSplitsAllSymbolsExDateV1;
    {
        let mut path = url.path_segments_mut().map_err(|_| invalid())?;
        path.push("tiingo").push("corporate-actions");
        if !split {
            path.push(ticker);
        }
        path.push(if split { "splits" } else { "distributions" });
    }
    let date = scope.ex_date_start.to_string();
    if split {
        url.query_pairs_mut().append_pair("exDate", &date);
    } else {
        url.query_pairs_mut()
            .append_pair("startExDate", &date)
            .append_pair("endExDate", &date);
    }
    let tag = if split { 4u8 } else { 3u8 };
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/tiingo/request/v1\0");
    hash.update(b"GET\0");
    hash.update(url.as_str().as_bytes());
    hash.update([tag]);
    hash.update([tag]);
    hash.update(date.as_bytes());
    if split {
        hash.update(ticker.as_bytes());
    }
    Ok((
        url.into(),
        EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
    ))
}

pub(crate) fn current_ordinary_event_identities<'a>(
    payloads: impl Iterator<Item = &'a [u8]>,
) -> Result<Box<[CorporateActionEventInstrumentIdentity]>, Error> {
    let mut values = BTreeMap::new();
    let mut remaining = MAX_AUDIT_BYTES;
    for (index, payload) in payloads.enumerate() {
        if index >= 8193 || payload.len() > MAX_AUDIT_BYTES {
            return Err(invalid());
        }
        remaining = remaining.checked_sub(payload.len()).ok_or_else(invalid)?;
        let value: Value = serde_json::from_slice(payload).map_err(|_| invalid())?;
        if value.get("row_kind").and_then(Value::as_str) == Some("economic_query_summary") {
            continue;
        }
        let row: NativeRow = serde_json::from_value(value).map_err(|_| invalid())?;
        if let Some(identity) = row.subject_identity {
            let key = identity.selection.selection_digest.bytes();
            if values.get(&key).is_some_and(|prior| prior != &identity) {
                return Err(invalid());
            }
            values.insert(key, identity);
        }
    }
    Ok(values.into_values().collect())
}
fn hex(value: EvidenceDigest) -> String {
    value.bytes().iter().map(|b| format!("{b:02x}")).collect()
}
fn native_terms(value: &Value, contract: Contract) -> Result<NativeTerms, Error> {
    let object = value.as_object().ok_or_else(invalid)?;
    let names: &[&str] = match contract {
        Contract::TiingoDistributionsTickerExDatesV1 => &[
            "permaTicker",
            "ticker",
            "exDate",
            "paymentDate",
            "recordDate",
            "declarationDate",
            "distribution",
            "distributionFrequency",
        ],
        Contract::TiingoSplitsAllSymbolsExDateV1 => &[
            "permaTicker",
            "ticker",
            "exDate",
            "splitFrom",
            "splitTo",
            "splitFactor",
            "splitStatus",
        ],
    };
    if object.len() != names.len() || names.iter().any(|name| !object.contains_key(*name)) {
        return Err(invalid());
    }
    let text = |key: &str, maximum: usize| -> Result<SourceIdentifier, Error> {
        let s = object
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= maximum)
            .ok_or_else(invalid)?;
        SourceIdentifier::try_from(s).map_err(|_| invalid())
    };
    let decimal = |key: &str| -> Result<rust_decimal::Decimal, Error> {
        let n = object
            .get(key)
            .and_then(Value::as_number)
            .ok_or_else(invalid)?;
        rust_decimal::Decimal::from_str_exact(&n.to_string()).map_err(|_| invalid())
    };
    let symbol = text("ticker", 64)?;
    if !symbol
        .as_str()
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
    {
        return Err(invalid());
    }
    let perma_ticker = text("permaTicker", 128)?;
    let date = native_date(object.get("exDate").ok_or_else(invalid)?)?.ok_or_else(invalid)?;
    let (terms, status, payable) = match contract {
        Contract::TiingoDistributionsTickerExDatesV1 => {
            let amount = decimal("distribution")?;
            let status = text("distributionFrequency", 8)?;
            if amount < rust_decimal::Decimal::ZERO
                || !["w", "bm", "m", "tm", "q", "sa", "a", "ir", "f", "u", "c"]
                    .contains(&status.as_str())
            {
                return Err(invalid());
            }
            let payable = native_date(&object["paymentDate"])?;
            let _ = native_date(&object["recordDate"])?;
            let _ = native_date(&object["declarationDate"])?;
            (
                CorporateActionEconomicTerms::Distribution {
                    distribution: amount,
                },
                status,
                payable,
            )
        }
        Contract::TiingoSplitsAllSymbolsExDateV1 => {
            let from = decimal("splitFrom")?;
            let to = decimal("splitTo")?;
            let factor = decimal("splitFactor")?;
            let status = text("splitStatus", 1)?;
            if from <= rust_decimal::Decimal::ZERO
                || to <= rust_decimal::Decimal::ZERO
                || factor <= rust_decimal::Decimal::ZERO
                || !matches!(status.as_str(), "a" | "c")
                || from.checked_mul(factor) != Some(to)
            {
                return Err(invalid());
            }
            (
                CorporateActionEconomicTerms::Split {
                    split_from: from,
                    split_to: to,
                    split_factor: factor,
                },
                status,
                None,
            )
        }
    };
    let bytes = serde_json::to_vec(value).map_err(|_| invalid())?;
    let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&bytes).into());
    let action_id = SourceIdentifier::try_from(format!("tiingo:corporate-action:{}", hex(digest)))
        .map_err(|_| invalid())?;
    Ok(NativeTerms {
        symbol,
        perma_ticker,
        date,
        payable,
        status,
        terms,
        digest,
        action_id,
    })
}
fn native_date(value: &Value) -> Result<Option<CalendarDate>, Error> {
    use chrono::{Datelike as _, Timelike as _};
    if value.is_null() {
        return Ok(None);
    }
    let text = value.as_str().ok_or_else(invalid)?;
    let date = if text.len() == 10 {
        chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|_| invalid())?
    } else {
        let t = chrono::DateTime::parse_from_rfc3339(text).map_err(|_| invalid())?;
        if t.offset().local_minus_utc() != 0
            || t.hour() != 0
            || t.minute() != 0
            || t.second() != 0
            || t.nanosecond() != 0
        {
            return Err(invalid());
        }
        t.date_naive()
    };
    CalendarDate::new(
        u16::try_from(date.year()).map_err(|_| invalid())?,
        u8::try_from(date.month()).map_err(|_| invalid())?,
        u8::try_from(date.day()).map_err(|_| invalid())?,
    )
    .map(Some)
    .map_err(|_| invalid())
}

impl AnalyticalReadCapability {
    /// Reopens one original economic-query generation and its original catalog identity positions.
    pub async fn read_current_ordinary_source(
        &self,
        source: &GenerationOwnedProviderCaptureEvidence,
        identities: &MarketDataInstrumentReadCapability,
        instrument_id: InstrumentId,
        knowledge_cutoff: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CurrentOrdinaryActionSourceRead, ReadError> {
        let original = self
            .read_original_current_ordinary_capture(
                source,
                knowledge_cutoff,
                deadline,
                cancellation.clone(),
            )
            .await?;
        let binding = &original.binding;
        let audit = binding
            .native_lineage()
            .batch_sidecar_semantic_payload()
            .ok_or(ReadError::InvalidEvidence)?;
        let sidecar: CurrentOrdinarySidecar =
            serde_json::from_slice(audit).map_err(|_| ReadError::InvalidEvidence)?;
        let scope = &sidecar.query_scope;
        let retained_events = current_ordinary_event_identities(
            binding
                .rows()
                .iter()
                .map(|row| row.native_semantic_payload()),
        )
        .map_err(|_| ReadError::InvalidEvidence)?;
        let selection = identities
            .reopen_current_ordinary_query_identities(
                scope,
                &retained_events,
                sidecar.received_at,
                knowledge_cutoff,
                deadline,
                &cancellation,
            )
            .map_err(|error| {
                use crate::{CorporateActionQueryIdentityError as E, MarketDataInstrumentCatalogError as C};
                match error {
                    E::ResourceBound | E::Catalog(C::BatchLimitExceeded { .. } | C::RevisionLimitExceeded | C::ResultByteLimitExceeded) => ReadError::ResourceBound,
                    E::Catalog(C::Cancelled | C::DeadlineExceeded) => ReadError::Interrupted,
                    _ => ReadError::InvalidEvidence,
                }
            })?;
        let [query_identity] = selection.retained() else {
            return Err(ReadError::InvalidEvidence);
        };
        let [instrument] = selection.selected_definitions() else {
            return Err(ReadError::InvalidEvidence);
        };
        if query_identity.instrument_id != instrument_id
            || instrument.definition().instrument_id() != instrument_id
        {
            return Err(ReadError::InvalidEvidence);
        }
        let mut event_identities = Vec::new();
        for identity in &retained_events {
            if identity.selection.instrument_id != instrument_id {
                return Err(ReadError::InvalidEvidence);
            }
            let definition = selection
                .event_definition(identity)
                .ok_or(ReadError::InvalidEvidence)?
                .clone();
            event_identities.push((scope.ex_date_start, identity.clone(), definition));
        }
        let mut economic = BTreeMap::new();
        let mut rows = Vec::new();
        for (observation, row) in original.observations.into_iter().zip(binding.rows()) {
            if cancellation.is_cancelled() || Instant::now() >= deadline {
                return Err(ReadError::Interrupted);
            }
            match observation {
                ResearchObservation::CorporateAction(action) => {
                    let id = action.context().provenance().source_identifier().clone();
                    if economic
                        .insert(
                            id,
                            CorporateActionRecord::new(
                                action,
                                original.manifest.clone(),
                                row.canonical_row_digest(),
                            ),
                        )
                        .is_some()
                    {
                        return Err(ReadError::InvalidEvidence);
                    }
                }
                ResearchObservation::CorporateActionSource(action) => {
                    let Payload::EconomicReturnedAction {
                        action_id,
                        native_symbol,
                        ex_date,
                        payable_date,
                        native_terms,
                        disposition,
                        native_row_digest,
                        ..
                    } = action.payload()
                    else {
                        continue;
                    };
                    if native_symbol != &query_identity.symbol {
                        continue;
                    }
                    let disposition = match disposition {
                        Disposition::Cancelled => CurrentOrdinaryActionDisposition::Cancelled {
                            evidence: *native_row_digest,
                        },
                        Disposition::MissingCurrency => {
                            let CorporateActionEconomicTerms::Distribution { distribution } =
                                native_terms
                            else {
                                return Err(ReadError::InvalidEvidence);
                            };
                            CurrentOrdinaryActionDisposition::MissingUnit {
                                evidence: *native_row_digest,
                                instrument: action.context().provenance().instrument_id(),
                                distribution: *distribution,
                                payable_date: *payable_date,
                            }
                        }
                        Disposition::Normalized => CurrentOrdinaryActionDisposition::Normalized {
                            record: economic
                                .remove(action_id)
                                .ok_or(ReadError::InvalidEvidence)?,
                            payable_date: *payable_date,
                        },
                        _ => CurrentOrdinaryActionDisposition::Unsupported {
                            evidence: *native_row_digest,
                        },
                    };
                    rows.push(CurrentOrdinaryActionRow {
                        date: *ex_date,
                        disposition,
                    });
                }
                _ => return Err(ReadError::InvalidEvidence),
            }
        }
        if !economic.is_empty() {
            return Err(ReadError::InvalidEvidence);
        }
        let source_audit=serde_json::to_vec(&serde_json::json!({
            "version":1,"source_capture_receipt_digest":source.receipt_digest(),
            "published_at":source.published_at(),"binding_digest":binding.binding_digest(),
            "knowledge_cutoff":knowledge_cutoff,"original_native_sidecar":serde_json::from_slice::<Value>(audit).map_err(|_|ReadError::InvalidEvidence)?,
            "original_event_identities":retained_events,
        })).map_err(|_|ReadError::ResourceBound)?;
        if source_audit.len() > MAX_AUDIT_BYTES {
            return Err(ReadError::ResourceBound);
        }
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Err(ReadError::Interrupted);
        }
        let result = CurrentOrdinaryActionSourceRead {
            family: match scope.query_contract {
                Contract::TiingoDistributionsTickerExDatesV1 => {
                    CurrentOrdinaryActionFamily::Distributions
                }
                Contract::TiingoSplitsAllSymbolsExDateV1 => CurrentOrdinaryActionFamily::Splits,
            },
            manifest: original.manifest,
            binding_digest: binding.binding_digest(),
            captured_at: sidecar.received_at,
            knowledge_cutoff,
            instrument: instrument.clone(),
            query_identity: query_identity.clone(),
            event_identities: event_identities.into_boxed_slice(),
            interval: (scope.ex_date_start, scope.ex_date_end),
            rows: rows.into_boxed_slice(),
            source_audit: source_audit.into_boxed_slice(),
        };
        if result
            .retained_bytes()
            .is_none_or(|bytes| bytes > 64 * 1024 * 1024)
        {
            return Err(ReadError::ResourceBound);
        }
        Ok(result)
    }
}

impl CurrentOrdinaryActionSourceRead {
    /// Conservative actual retained-object charge, using the original EventReadBudget convention.
    /// Serialized dynamic graphs are counted without allocating another encoding and charged four
    /// times their exact encoded bytes, alongside actual inline/boxed sizes and raw audit bytes.
    pub fn retained_bytes(&self) -> Option<usize> {
        let mut count = RetainedCount {
            bytes: std::mem::size_of::<Self>(),
        };
        count.add(self.source_audit.len())?;
        count.add(
            self.rows
                .len()
                .checked_mul(std::mem::size_of::<CurrentOrdinaryActionRow>())?,
        )?;
        count.add(
            self.event_identities
                .len()
                .checked_mul(std::mem::size_of::<(
                    CalendarDate,
                    CorporateActionEventInstrumentIdentity,
                    crate::MarketDataInstrumentRecord,
                )>())?,
        )?;
        // Inline manifest fields are included in Self; charge both owned strings,
        // as in corporate_actions::retained::record_dynamic_bytes.
        count.add(self.manifest.dataset_id().as_str().len())?;
        count.add(self.manifest.schema().name().len())?;
        count.serialized(self.instrument.definition())?;
        count.serialized(&self.query_identity)?;
        for (_, identity, definition) in &self.event_identities {
            count.serialized(identity)?;
            count.serialized(definition.definition())?;
        }
        for row in &self.rows {
            if let CurrentOrdinaryActionDisposition::Normalized { record, .. } = &row.disposition {
                count.serialized(record.observation())?;
                count.add(record.source_manifest().dataset_id().as_str().len())?;
                count.add(record.source_manifest().schema().name().len())?;
            }
        }
        Some(count.bytes)
    }
}
struct RetainedCount {
    bytes: usize,
}
impl RetainedCount {
    fn add(&mut self, bytes: usize) -> Option<()> {
        self.bytes = self.bytes.checked_add(bytes)?;
        Some(())
    }
    fn serialized<T: serde::Serialize + ?Sized>(&mut self, value: &T) -> Option<()> {
        serde_json::to_writer(self, value).ok()
    }
}
impl std::io::Write for RetainedCount {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.add(
            bytes
                .len()
                .checked_mul(4)
                .ok_or_else(|| std::io::Error::other("source read retained bytes overflow"))?,
        )
        .ok_or_else(|| std::io::Error::other("source read retained bytes overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
