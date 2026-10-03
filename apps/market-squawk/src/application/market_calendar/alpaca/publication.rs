//! Canonical calendar coverage and native session rows from one sealed authenticated response.

use std::{
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    time::Instant,
};

use bytes::Bytes;
use market_squawk_adapter_alpaca::{
    ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES, AlpacaAuthenticatedCalendarResponse,
};
use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailability, DataQuality, DigestAlgorithm, EffectiveInterval,
    EvidenceDigest, ExactPayloadEvidence, MARKET_CALENDAR_DATE_MEMBERSHIP_DOMAIN,
    MarketCalendarBoundary, MarketCalendarCompleteness, MarketCalendarDateScope, MarketCalendarDay,
    MarketCalendarDayInput, MarketCalendarDayStatus, MarketCalendarField, MarketCalendarInterval,
    MarketCalendarMetadata, MarketCalendarObservation, MarketCalendarObservationInput,
    MarketCalendarPayload, MarketCalendarScope, MarketCalendarSessionPresence,
    MarketCalendarSessionRole, MarketSourceText, MetadataRevision, PayloadHash, PayloadReference,
    ProviderChannel, ProviderProduct, ResearchContext, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime, RevisionNumber,
    SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryRequest, ExtractionBatch,
    ExtractionBatchAccumulator, ExtractionRecord, ExtractionRequest,
    MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, ProviderNativeLineageBatchBuilder,
    ProviderNativeLineageImplementation, ProviderWholeCaptureToken, SealedProviderCaptureBinding,
    SourceObject, SourceObjectCaptureIdentity,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    AlpacaCalendarDayWire, AlpacaMarketWire, OptionalWire, completed::CalendarRangeWire,
    new_york_civil_day, parse_calendar_date, parse_calendar_day, validate_market_identity_for,
};
use crate::application::market_calendar::{
    CompletedMarketSessionError, MarketCalendarClock, SystemMarketCalendarClock,
};

/// Normalizes and consumes one complete, physically sealed range response for existing ingest.
/// Source and dataset coordinates come from the non-cloneable seal, never a caller's session list.
pub(crate) fn try_bind_alpaca_calendar_publication(
    response: &AlpacaAuthenticatedCalendarResponse,
    token: ProviderWholeCaptureToken,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<SealedProviderCaptureBinding, CompletedMarketSessionError> {
    ensure_before(deadline, cancellation)?;
    let capture = token.persisted_receipt().capture();
    let request_digest = response
        .request()
        .capture_request_identity()
        .map_err(invalid)?;
    let body_digest = sha256(response.body());
    if response.status() != 200
        || response.body().is_empty()
        || response.body().len() > ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES
        || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        || capture.request_set_identity() != request_digest
        || capture.pages().len() != 1
    {
        return Err(CompletedMarketSessionError::InvalidEvidence);
    }
    let page = &capture.pages()[0];
    if page.http_status() != 200
        || page.request_identity() != request_digest
        || page.request_page_token_digest().is_some()
        || page.response_next_page_token_digest().is_some()
        || page.body_digest() != body_digest
        || page.body_bytes() != u64::try_from(response.body().len()).map_err(invalid)?
        || page.received_at() != response.received_at()
    {
        return Err(CompletedMarketSessionError::InvalidEvidence);
    }
    let wire: CalendarRangeWire = serde_json::from_slice(response.body()).map_err(invalid)?;
    validate_market_identity_for(&wire.market, response.request().market()).map_err(invalid)?;
    let ingested_at = SystemMarketCalendarClock.now().map_err(invalid)?;
    let scope = scope_for(response.request())?;
    let count = u32::try_from(wire.calendar.0.len()).map_err(invalid)?;
    let mut dates_digest = Sha256::new();
    dates_digest.update(MARKET_CALENDAR_DATE_MEMBERSHIP_DOMAIN);
    dates_digest.update(count.to_be_bytes());
    let mut previous = None;
    for day in &wire.calendar.0 {
        ensure_before(deadline, cancellation)?;
        let date = parse_calendar_date(&day.date).map_err(invalid)?;
        if date < response.request().start_date()
            || date > response.request().end_date()
            || previous.is_some_and(|previous| previous >= date)
        {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        previous = Some(date);
        dates_digest.update(date.year().to_be_bytes());
        dates_digest.update([date.month(), date.day()]);
    }
    let reported_days_digest =
        EvidenceDigest::new(DigestAlgorithm::Sha256, dates_digest.finalize().into());
    let request = extraction_request(capture, count, ingested_at, deadline)?;
    let mut records = ExtractionBatchAccumulator::try_new(&request).map_err(invalid)?;
    let coverage = observation(
        capture,
        &scope,
        response.received_at(),
        ingested_at,
        body_digest,
        MarketCalendarPayload::Coverage {
            market: native_market(&wire.market)?,
            completeness: MarketCalendarCompleteness::CompleteSessionEnumeration,
            reported_day_count: count,
            reported_days_digest,
        },
    )?;
    push_record(&mut records, &request, coverage)?;
    for day in &wire.calendar.0 {
        ensure_before(deadline, cancellation)?;
        let canonical = normalize_day(day)?;
        let value = observation(
            capture,
            &scope,
            response.received_at(),
            ingested_at,
            body_digest,
            MarketCalendarPayload::SessionDay { day: canonical },
        )?;
        push_record(&mut records, &request, value)?;
    }
    let batch: ExtractionBatch = records.finish().map_err(invalid)?;
    let mut native = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::AlpacaCalendarV1,
        &batch,
    )
    .map_err(invalid)?;
    native
        .try_push(&NativeMarket::from(&wire.market))
        .map_err(invalid)?;
    for day in &wire.calendar.0 {
        ensure_before(deadline, cancellation)?;
        native.try_push(&NativeDay::from(day)).map_err(invalid)?;
    }
    let native = native.finish().map_err(invalid)?;
    let mut pages = Vec::new();
    pages
        .try_reserve_exact(batch.records().len())
        .map_err(resource)?;
    pages.resize(batch.records().len(), 0_u16);
    ensure_before(deadline, cancellation)?;
    SealedProviderCaptureBinding::try_whole(token, batch, native, pages).map_err(invalid)
}

/// Reconciles the entire native sibling row set against the exact retained response. An empty
/// calendar still has its Coverage row; omission authority cannot arise from a partial row set.
pub(super) fn validate_retained_calendar_native_rows(
    request: &market_squawk_adapter_alpaca::AlpacaAuthenticatedCalendarRequest,
    body: &[u8],
    binding: &market_squawk_data::PersistedProviderCaptureBindingEvidence,
    observations: &[MarketCalendarObservation],
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CompletedMarketSessionError> {
    ensure_before(deadline, cancellation)?;
    if body.is_empty()
        || body.len() > ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES
        || binding.native_lineage().implementation() != "alpaca_calendar_v1"
    {
        return Err(invalid_unit());
    }
    let wire: CalendarRangeWire = serde_json::from_slice(body).map_err(invalid)?;
    validate_market_identity_for(&wire.market, request.market()).map_err(invalid)?;
    if binding.record_count()
        != wire
            .calendar
            .0
            .len()
            .checked_add(1)
            .ok_or_else(invalid_unit)?
        || binding.rows().len() != binding.record_count()
        || observations.len() != binding.record_count()
    {
        return Err(invalid_unit());
    }
    let scope = scope_for(request)?;
    let mut hash = Sha256::new();
    hash.update(MARKET_CALENDAR_DATE_MEMBERSHIP_DOMAIN);
    hash.update(
        u32::try_from(wire.calendar.0.len())
            .map_err(invalid)?
            .to_be_bytes(),
    );
    let mut previous = None;
    for day in &wire.calendar.0 {
        let date = parse_calendar_date(&day.date).map_err(invalid)?;
        if previous.is_some_and(|previous| previous >= date)
            || date < request.start_date()
            || date > request.end_date()
        {
            return Err(invalid_unit());
        }
        previous = Some(date);
        hash.update(date.year().to_be_bytes());
        hash.update([date.month(), date.day()]);
    }
    let reported_days_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
    let body_digest = sha256(body);
    for (ordinal, actual) in observations.iter().enumerate() {
        ensure_before(deadline, cancellation)?;
        let payload = if ordinal == 0 {
            MarketCalendarPayload::Coverage {
                market: native_market(&wire.market)?,
                completeness: MarketCalendarCompleteness::CompleteSessionEnumeration,
                reported_day_count: u32::try_from(wire.calendar.0.len()).map_err(invalid)?,
                reported_days_digest,
            }
        } else {
            MarketCalendarPayload::SessionDay {
                day: normalize_day(&wire.calendar.0[ordinal - 1])?,
            }
        };
        let expected = observation(
            binding.capture(),
            &scope,
            binding.capture().pages()[0].received_at(),
            actual.context().provenance().ingested_at(),
            body_digest,
            payload,
        )?
        .with_revision(actual.context().time().revision());
        if &expected != actual {
            return Err(invalid_unit());
        }
    }
    let coverage = binding.rows().first().ok_or_else(invalid_unit)?;
    if coverage.native_semantic_payload()
        != serde_json::to_vec(&NativeMarket::from(&wire.market)).map_err(invalid)?
    {
        return Err(invalid_unit());
    }
    for (day, retained) in wire.calendar.0.iter().zip(&binding.rows()[1..]) {
        ensure_before(deadline, cancellation)?;
        if retained.native_semantic_payload()
            != serde_json::to_vec(&NativeDay::from(day)).map_err(invalid)?
        {
            return Err(invalid_unit());
        }
    }
    Ok(())
}

fn scope_for(
    request: &market_squawk_adapter_alpaca::AlpacaAuthenticatedCalendarRequest,
) -> Result<MarketCalendarScope, CompletedMarketSessionError> {
    let request_digest = request.capture_request_identity().map_err(invalid)?;
    Ok(MarketCalendarScope {
        provider_product: ProviderProduct::new(identifier("alpaca-market-calendar")?),
        provider_channel: ProviderChannel::new(identifier("v3-calendar")?),
        source_contract_revision: MetadataRevision::new(identifier(
            "alpaca-v3-market-utc-calendar-v1",
        )?),
        native_market_type: MarketCalendarField::Missing,
        native_product: text(request.market().request_code())?,
        requested_timezone: MarketCalendarField::Reported(text("UTC")?),
        date_scope: MarketCalendarDateScope::RequestedRange {
            start_date: request.start_date(),
            end_date: request.end_date(),
        },
        request_evidence: ExactPayloadEvidence::from_content_digest(request_digest),
    })
}

fn extraction_request(
    capture: &ProviderCaptureSetReceipt,
    reported_days: u32,
    now: Timestamp,
    deadline: Instant,
) -> Result<ExtractionRequest, CompletedMarketSessionError> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(CompletedMarketSessionError::DeadlineExceeded)?;
    let wall_deadline = now
        .checked_add_nanos(i64::try_from(remaining.as_nanos()).map_err(invalid)?)
        .map_err(invalid)?;
    let discovery = DiscoveryRequest::try_new(
        capture.dataset().clone(),
        None,
        NonZeroU16::new(1).ok_or_else(invalid_unit)?,
        wall_deadline,
    )
    .map_err(invalid)?;
    let object = SourceObject::try_new_with_capture_identity(
        capture.source_id().clone(),
        capture.metadata_revision().clone(),
        &discovery,
        identifier(&format!(
            "calendar-{}",
            digest_hex(capture.content_digest())?
        ))?,
        identifier("application/vnd.market-squawk.alpaca-calendar+json")?,
        ExactPayloadEvidence::from_content_digest(capture.content_digest()),
        SourceObjectCaptureIdentity::try_from_capture(capture).map_err(invalid)?,
        EffectiveInterval::new(capture.pages()[0].received_at(), None).map_err(invalid)?,
        None,
        AvailabilityEvidence::LocalFirstObserved {
            observed_at: capture.pages()[0].received_at(),
        },
        Some(capture.pages()[0].body_bytes()),
    )
    .map_err(invalid)?;
    ExtractionRequest::try_new(
        object,
        NonZeroU32::new(reported_days.checked_add(1).ok_or_else(invalid_unit)?)
            .ok_or_else(invalid_unit)?,
        NonZeroU64::new(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES).ok_or_else(invalid_unit)?,
        wall_deadline,
    )
    .map_err(invalid)
}

fn observation(
    capture: &ProviderCaptureSetReceipt,
    scope: &MarketCalendarScope,
    received_at: Timestamp,
    ingested_at: Timestamp,
    body_digest: EvidenceDigest,
    payload: MarketCalendarPayload,
) -> Result<MarketCalendarObservation, CompletedMarketSessionError> {
    let (date, suffix) = match &payload {
        MarketCalendarPayload::Coverage { .. } => (scope.date_scope.start_date(), "coverage"),
        MarketCalendarPayload::SessionDay { day } => (day.date(), "day"),
    };
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: capture.source_id().clone(),
        instrument_id: None,
        venue_id: Some(
            VenueId::try_from(match scope.native_product.as_str() {
                "IEX" => "iex",
                "XNYS" => "XNYS",
                "XNAS" => "XNAS",
                _ => return Err(invalid_unit()),
            })
            .map_err(invalid)?,
        ),
        source_identifier: identifier(&format!(
            "alpaca-calendar/{}/{}/{suffix}/{date}",
            scope.date_scope.start_date(),
            scope.date_scope.end_date(),
        ))?,
        source_timestamp: None,
        received_at,
        ingested_at,
        quality: DataQuality::Aggregated,
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            DigestAlgorithm::Sha256,
            body_digest.bytes(),
        )),
        availability: ResearchAvailability::local_first_observed(received_at),
    })
    .map_err(invalid)?;
    let time = ResearchTime::try_new_with_coordinates(
        ResearchTemporalCoordinate::calendar_date(date),
        None,
        RevisionNumber::new(1).map_err(invalid)?,
        None,
    )
    .map_err(invalid)?;
    MarketCalendarObservation::try_new(MarketCalendarObservationInput {
        context: ResearchContext::new(provenance, time).map_err(invalid)?,
        scope: scope.clone(),
        observed_at: received_at,
        payload,
    })
    .map_err(invalid)
}

fn push_record(
    records: &mut ExtractionBatchAccumulator,
    request: &ExtractionRequest,
    observation: MarketCalendarObservation,
) -> Result<(), CompletedMarketSessionError> {
    let effective = observation.context().time().effective().clone();
    let observed_at = observation.observed_at();
    let payload = Bytes::from(
        serde_json::to_vec(&ResearchObservation::MarketCalendar(observation)).map_err(invalid)?,
    );
    let evidence = sha256(&payload);
    let record = ExtractionRecord::try_new_with_time(
        request,
        identifier(CURRENT_RESEARCH_RECORD_SCHEMA)?,
        ExactPayloadEvidence::from_content_digest(evidence),
        effective,
        None,
        AvailabilityEvidence::LocalFirstObserved { observed_at },
        identifier(&format!("calendar-{}", digest_hex(evidence)?))?,
        None,
        payload,
    )
    .map_err(invalid)?;
    records.push(record).map_err(invalid)
}

fn normalize_day(
    day: &AlpacaCalendarDayWire,
) -> Result<MarketCalendarDay, CompletedMarketSessionError> {
    let date = parse_calendar_date(&day.date).map_err(invalid)?;
    let (_, start, end) = new_york_civil_day(date).map_err(invalid)?;
    let parsed = parse_calendar_day(day.clone(), date, start, end).map_err(invalid)?;
    let mut intervals = Vec::new();
    intervals.try_reserve_exact(4).map_err(resource)?;
    for (native_kind, role, interval) in [
        ("core", MarketCalendarSessionRole::Core, Some(parsed.core)),
        ("pre", MarketCalendarSessionRole::Pre, parsed.pre),
        ("post", MarketCalendarSessionRole::Post, parsed.post),
        (
            "lunch",
            MarketCalendarSessionRole::Intermission,
            parsed.lunch,
        ),
    ] {
        if let Some(interval) = interval {
            intervals.push(MarketCalendarInterval {
                native_kind: text(native_kind)?,
                native_ordinal: 0,
                role,
                start: MarketCalendarBoundary::try_new(interval.start, 0).map_err(invalid)?,
                end: MarketCalendarBoundary::try_new(interval.end, 0).map_err(invalid)?,
            });
        }
    }
    MarketCalendarDay::try_new(MarketCalendarDayInput {
        date,
        status: MarketCalendarDayStatus::ScheduledSessions,
        category: MarketCalendarField::Missing,
        settlement_date: parsed
            .settlement_date
            .map_or(MarketCalendarField::Missing, MarketCalendarField::Reported),
        session_presence: MarketCalendarSessionPresence::Reported,
        intervals,
    })
    .map_err(invalid)
}

fn native_market(
    market: &AlpacaMarketWire,
) -> Result<MarketCalendarMetadata, CompletedMarketSessionError> {
    Ok(MarketCalendarMetadata {
        acronym: MarketCalendarField::Reported(text(&market.acronym)?),
        name: MarketCalendarField::Reported(text(&market.name)?),
        timezone: MarketCalendarField::Reported(text(&market.timezone)?),
        bic: optional_text(&market.bic)?,
        mic: optional_text(&market.mic)?,
    })
}

fn optional_text(
    value: &OptionalWire<String>,
) -> Result<MarketCalendarField<MarketSourceText>, CompletedMarketSessionError> {
    match value {
        OptionalWire::Missing => Ok(MarketCalendarField::Missing),
        OptionalWire::Present(value) => text(value).map(MarketCalendarField::Reported),
    }
}

#[derive(Serialize)]
struct NativeMarket<'a> {
    acronym: &'a str,
    name: &'a str,
    timezone: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    bic: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mic: Option<&'a str>,
}
impl<'a> From<&'a AlpacaMarketWire> for NativeMarket<'a> {
    fn from(value: &'a AlpacaMarketWire) -> Self {
        Self {
            acronym: &value.acronym,
            name: &value.name,
            timezone: &value.timezone,
            bic: value.bic.as_deref(),
            mic: value.mic.as_deref(),
        }
    }
}

#[derive(Serialize)]
struct NativeDay<'a> {
    date: &'a str,
    core_start: &'a str,
    core_end: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pre_start: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pre_end: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    post_start: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    post_end: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lunch_start: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lunch_end: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    settlement_date: Option<&'a str>,
}
impl<'a> From<&'a AlpacaCalendarDayWire> for NativeDay<'a> {
    fn from(value: &'a AlpacaCalendarDayWire) -> Self {
        Self {
            date: &value.date,
            core_start: &value.core_start,
            core_end: &value.core_end,
            pre_start: value.pre_start.as_deref(),
            pre_end: value.pre_end.as_deref(),
            post_start: value.post_start.as_deref(),
            post_end: value.post_end.as_deref(),
            lunch_start: value.lunch_start.as_deref(),
            lunch_end: value.lunch_end.as_deref(),
            settlement_date: value.settlement_date.as_deref(),
        }
    }
}

fn text(value: &str) -> Result<MarketSourceText, CompletedMarketSessionError> {
    MarketSourceText::try_new(value).map_err(invalid)
}
fn identifier(value: &str) -> Result<SourceIdentifier, CompletedMarketSessionError> {
    SourceIdentifier::try_from(value).map_err(invalid)
}
fn sha256(value: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
}
fn digest_hex(value: EvidenceDigest) -> Result<String, CompletedMarketSessionError> {
    use std::fmt::Write as _;
    let mut encoded = String::new();
    encoded.try_reserve_exact(64).map_err(resource)?;
    for byte in value.bytes() {
        write!(&mut encoded, "{byte:02x}").map_err(invalid)?;
    }
    Ok(encoded)
}
fn ensure_before(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CompletedMarketSessionError> {
    if cancellation.is_cancelled() {
        return Err(CompletedMarketSessionError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(CompletedMarketSessionError::DeadlineExceeded);
    }
    Ok(())
}
fn invalid<T>(_: T) -> CompletedMarketSessionError {
    invalid_unit()
}
fn invalid_unit() -> CompletedMarketSessionError {
    CompletedMarketSessionError::InvalidEvidence
}
fn resource<T>(_: T) -> CompletedMarketSessionError {
    CompletedMarketSessionError::ResourceBoundExceeded
}
