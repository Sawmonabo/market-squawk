//! Exact sealed REST hours to the existing source-calendar research family.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use chrono::{DateTime, Datelike, NaiveDate};
use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailability, CalendarDate, DigestAlgorithm, EffectiveInterval,
    EvidenceDigest, ExactPayloadEvidence, MARKET_CALENDAR_DATE_MEMBERSHIP_DOMAIN,
    MAX_MARKET_CALENDAR_INTERVALS, MarketCalendarBoundary, MarketCalendarCompleteness,
    MarketCalendarDateScope, MarketCalendarDay, MarketCalendarDayInput, MarketCalendarDayStatus,
    MarketCalendarField, MarketCalendarInterval, MarketCalendarMetadata, MarketCalendarObservation,
    MarketCalendarObservationInput, MarketCalendarPayload, MarketCalendarScope,
    MarketCalendarSessionPresence, MarketCalendarSessionRole, MarketSourceText, MetadataRevision,
    PayloadHash, PayloadReference, ResearchContext, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime, RevisionNumber,
    SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryRequest,
    ExtractionBatchAccumulator, ExtractionRecord, ExtractionRequest, ExtractionRevisionPlan,
    ProviderNativeLineageBatchBuilder, ProviderNativeLineageImplementation, SchwabMarketDataFamily,
    SealedProviderCaptureBinding, SourceObject, SourceObjectCaptureIdentity,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use url::Url;

use crate::{
    MarketHours, NativeField, ReadOnlyRoute, SchwabMarketDataQualification, SchwabRestPayload,
    SchwabSealedRestResponse,
};

const MAX_PRODUCTS: usize = 64;
const MEDIA_TYPE: &str = "application/vnd.market-squawk.schwab-market-hours+json";

/// Exact existing extraction and doctor-derived family semantics for a sealed hours response.
#[derive(Debug)]
pub struct SchwabMarketHoursPublicationRequest {
    extraction: ExtractionRequest,
    qualification: SchwabMarketDataQualification,
    ingested_at: Timestamp,
}
impl SchwabMarketHoursPublicationRequest {
    /// Value construction does not grant capture, OAuth, catalog or rights authority.
    pub fn new(
        extraction: ExtractionRequest,
        qualification: SchwabMarketDataQualification,
        ingested_at: Timestamp,
    ) -> Self {
        Self {
            extraction,
            qualification,
            ingested_at,
        }
    }
}

/// Complete one-use calendar extraction, with one coverage and one day row per returned product.
#[derive(Debug)]
pub struct SchwabSealedMarketHoursPublication {
    revisions: ExtractionRevisionPlan,
    binding: SealedProviderCaptureBinding,
    returned_products: usize,
}
impl SchwabSealedMarketHoursPublication {
    /// Actual returned product count, independent of requested markets and session counts.
    pub const fn returned_products(&self) -> usize {
        self.returned_products
    }
    /// Releases the exact existing publisher inputs; no alternate storage or capture token.
    pub fn into_parts(self) -> (ExtractionRevisionPlan, SealedProviderCaptureBinding) {
        (self.revisions, self.binding)
    }
}

/// Hours cannot become canonical when clocks, scope, native intervals or physical evidence differ.
#[derive(Debug, Error)]
pub enum SchwabMarketHoursPublicationError {
    #[error("Schwab market-hours source, request or sealed evidence is inconsistent")]
    InvalidEvidence,
    #[error("Schwab market-hours response contains an invalid native date or session")]
    InvalidNativeHours,
    #[error("Schwab market-hours response exceeds the bounded product or interval limit")]
    ResourceBound,
}
use SchwabMarketHoursPublicationError::{InvalidEvidence, InvalidNativeHours, ResourceBound};

impl SchwabSealedRestResponse {
    /// Derives the sole source object from the original physical seal, never caller-made source
    /// clocks, response hashes or source identity. Discovery contributes only bounded query scope.
    pub fn market_hours_source_object(
        &self,
        discovery: &DiscoveryRequest,
    ) -> Result<SourceObject, SchwabMarketHoursPublicationError> {
        let parts = self.parts();
        if !matches!(&parts.payload, SchwabRestPayload::MarketHours(_))
            || !matches!(
                parts.receipt.route(),
                ReadOnlyRoute::Markets | ReadOnlyRoute::SingleMarket
            )
            || discovery.dataset() != parts.coordinates.dataset()
            || discovery.effective_at().is_some()
        {
            return Err(InvalidEvidence);
        }
        let received_at = Timestamp::from_unix_nanos(
            i64::try_from(parts.receipt.received_at_unix_millis())
                .ok()
                .and_then(|value| value.checked_mul(1_000_000))
                .ok_or(InvalidEvidence)?,
        );
        let capture = parts.token.persisted_receipt().capture();
        SourceObject::try_new_with_capture_identity(
            parts.coordinates.source_id().clone(),
            parts.coordinates.metadata_revision().clone(),
            discovery,
            SourceIdentifier::try_from(format!(
                "schwab:hours:object:{}",
                hex(capture.content_digest().bytes())
            ))
            .map_err(|_| InvalidEvidence)?,
            SourceIdentifier::try_from(MEDIA_TYPE).map_err(|_| InvalidEvidence)?,
            ExactPayloadEvidence::from_content_digest(sha256_digest(parts.receipt.body_sha256())),
            SourceObjectCaptureIdentity::try_from_capture(capture).map_err(|_| InvalidEvidence)?,
            EffectiveInterval::new(received_at, None).map_err(|_| InvalidEvidence)?,
            None,
            AvailabilityEvidence::LocalFirstObserved {
                observed_at: received_at,
            },
            Some(parts.receipt.body_bytes()),
        )
        .map_err(|_| InvalidEvidence)
    }
    /// Maps only the physically sealed native MarketHours payload. Explicit source closure stays
    /// distinct from omitted hours, and returned products never prove missing products/dates closed.
    pub fn into_market_hours_publication(
        self,
        request: SchwabMarketHoursPublicationRequest,
    ) -> Result<SchwabSealedMarketHoursPublication, SchwabMarketHoursPublicationError> {
        let parts = self.into_parts();
        let receipt = &parts.receipt;
        let SchwabRestPayload::MarketHours(parsed) = &parts.payload else {
            return Err(InvalidEvidence);
        };
        let received_at = Timestamp::from_unix_nanos(
            i64::try_from(receipt.received_at_unix_millis())
                .ok()
                .and_then(|n| n.checked_mul(1_000_000))
                .ok_or(InvalidEvidence)?,
        );
        let capture = parts.token.persisted_receipt().capture();
        let [page] = capture.pages() else {
            return Err(InvalidEvidence);
        };
        let object = request.extraction.object();
        if !matches!(
            receipt.route(),
            ReadOnlyRoute::Markets | ReadOnlyRoute::SingleMarket
        ) || receipt.status() != 200
            || !request
                .qualification
                .validates_rest_receipt(SchwabMarketDataFamily::MarketHours, receipt)
            || parsed.raw_sha256() != receipt.body_sha256()
            || object.source_id() != parts.coordinates.source_id()
            || object.metadata_revision() != parts.coordinates.metadata_revision()
            || object.dataset() != parts.coordinates.dataset()
            || object.media_type().as_str() != MEDIA_TYPE
            || object.object_id().as_str()
                != format!(
                    "schwab:hours:object:{}",
                    hex(capture.content_digest().bytes())
                )
            || object.capture_identity()
                != SourceObjectCaptureIdentity::try_from_capture(capture)
                    .map_err(|_| InvalidEvidence)?
            || object.evidence().content_digest() != sha256_digest(receipt.body_sha256())
            || object.expected_bytes() != Some(receipt.body_bytes())
            || object.effective_interval().starts_at() != received_at
            || object.effective_interval().ends_at().is_some()
            || object.published_at().is_some()
            || object.availability().conservative_available_at() != Some(received_at)
            || request.ingested_at < received_at
            || request.extraction.deadline() <= request.ingested_at
            || page.body_digest() != sha256_digest(receipt.body_sha256())
            || page.received_at() != received_at
            || parts.accounting.provider_records != parsed.value().len() as u64
        {
            return Err(InvalidEvidence);
        }
        let hours = parsed.value();
        if hours.is_empty() || hours.len() > MAX_PRODUCTS {
            return Err(ResourceBound);
        }
        let (markets, requested_date) = request_scope(receipt.request_url(), receipt.route())?;
        let mut seen = BTreeSet::new();
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(hours.len() * 2)
            .map_err(|_| ResourceBound)?;
        for (ordinal, native) in hours.iter().enumerate() {
            if !markets
                .iter()
                .any(|market| market.eq_ignore_ascii_case(&native.market_type))
                || !seen.insert((native.market_type.as_ref(), native.product.as_ref()))
            {
                return Err(InvalidEvidence);
            }
            let date = parse_date(&native.date)?;
            if requested_date.is_some_and(|expected| expected != date) {
                return Err(InvalidEvidence);
            }
            let scope = MarketCalendarScope {
                provider_product: request.qualification.provider_product().clone(),
                provider_channel: request.qualification.provider_channel().clone(),
                source_contract_revision: MetadataRevision::new(
                    SourceIdentifier::try_from("schwab-market-hours-v1")
                        .map_err(|_| InvalidEvidence)?,
                ),
                native_market_type: MarketCalendarField::Reported(source_text(
                    &native.market_type,
                )?),
                native_product: source_text(&native.product)?,
                requested_timezone: MarketCalendarField::Missing,
                date_scope: requested_date.map_or(
                    MarketCalendarDateScope::ReturnedDate { date },
                    |date| MarketCalendarDateScope::RequestedRange {
                        start_date: date,
                        end_date: date,
                    },
                ),
                request_evidence: ExactPayloadEvidence::from_content_digest(sha256_digest(
                    receipt.request_sha256(),
                )),
            };
            let day = native_day(native, date)?;
            let payloads = [
                MarketCalendarPayload::Coverage {
                    market: MarketCalendarMetadata {
                        acronym: MarketCalendarField::Missing,
                        name: MarketCalendarField::Missing,
                        timezone: MarketCalendarField::Missing,
                        bic: MarketCalendarField::Missing,
                        mic: MarketCalendarField::Missing,
                    },
                    completeness: MarketCalendarCompleteness::ReturnedEntriesOnly,
                    reported_day_count: 1,
                    reported_days_digest: date_digest(date),
                },
                MarketCalendarPayload::SessionDay { day },
            ];
            for (kind, payload) in payloads.into_iter().enumerate() {
                let id = source_identifier(&native.market_type, &native.product, date, kind)?;
                let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
                    source_id: parts.coordinates.source_id().clone(),
                    instrument_id: None,
                    venue_id: None,
                    source_identifier: id,
                    source_timestamp: None,
                    received_at,
                    ingested_at: request.ingested_at,
                    quality: request.qualification.quality(),
                    payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                        DigestAlgorithm::Sha256,
                        receipt.body_sha256(),
                    )),
                    availability: ResearchAvailability::local_first_observed(received_at),
                })
                .map_err(|_| InvalidEvidence)?;
                let time = ResearchTime::try_new_with_coordinates(
                    ResearchTemporalCoordinate::calendar_date(date),
                    None,
                    RevisionNumber::new(1).map_err(|_| InvalidEvidence)?,
                    None,
                )
                .map_err(|_| InvalidEvidence)?;
                let observation =
                    MarketCalendarObservation::try_new(MarketCalendarObservationInput {
                        context: ResearchContext::new(provenance, time)
                            .map_err(|_| InvalidEvidence)?,
                        scope: scope.clone(),
                        observed_at: received_at,
                        payload,
                    })
                    .map_err(|_| InvalidNativeHours)?;
                observations.push((ordinal, kind, observation));
            }
        }
        let mut batch = ExtractionBatchAccumulator::try_new(&request.extraction)
            .map_err(|_| InvalidEvidence)?;
        for (_, _, observation) in &observations {
            let effective = observation.context().time().effective().clone();
            let payload = Bytes::from(
                serde_json::to_vec(&ResearchObservation::MarketCalendar(observation.clone()))
                    .map_err(|_| InvalidEvidence)?,
            );
            let digest = sha256_digest(Sha256::digest(&payload).into());
            batch
                .push(
                    ExtractionRecord::try_new_with_time(
                        &request.extraction,
                        SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
                            .map_err(|_| InvalidEvidence)?,
                        ExactPayloadEvidence::from_content_digest(digest),
                        effective,
                        None,
                        AvailabilityEvidence::LocalFirstObserved {
                            observed_at: received_at,
                        },
                        SourceIdentifier::try_from(format!("observed:{}", hex(digest.bytes())))
                            .map_err(|_| InvalidEvidence)?,
                        None,
                        payload,
                    )
                    .map_err(|_| InvalidEvidence)?,
                )
                .map_err(|_| InvalidEvidence)?;
        }
        let batch = batch
            .finish()
            .map_err(|_| InvalidEvidence)?
            .try_bind_provider_capture(capture)
            .map_err(|_| InvalidEvidence)?;
        let mut lineage = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::SchwabRestMarketDataV1,
            &batch,
        )
        .map_err(|_| InvalidEvidence)?;
        let unknown = parsed.unknown_fields();
        lineage
            .try_set_batch_sidecar(&HoursSidecar {
                version: 1,
                family: "schwab.market-hours",
                request_url: receipt.request_url(),
                request_sha256: receipt.request_sha256(),
                response_sha256: receipt.body_sha256(),
                response_bytes: receipt.body_bytes(),
                received_at_unix_millis: receipt.received_at_unix_millis(),
                provider_schema: parsed.schema_name(),
                provider_schema_version: parsed.schema_version(),
                token_generation: receipt.token_generation().get(),
                qualification_receipt: request.qualification.receipt_evidence(),
                qualification_observation: request.qualification.observation_evidence(),
                returned_products: hours.len(),
                requested_markets: markets.iter().map(String::as_str).collect(),
                unknown_field_count: unknown.field_count(),
                unknown_field_bytes: unknown.encoded_bytes(),
                unknown_field_paths: unknown.paths(),
                unknown_field_digest: unknown.digest(),
            })
            .map_err(|_| InvalidEvidence)?;
        for (ordinal, kind, observation) in &observations {
            let native = &hours[*ordinal];
            let mut sessions = Vec::new();
            sessions
                .try_reserve_exact(native.sessions.len())
                .map_err(|_| ResourceBound)?;
            for field in &native.sessions {
                sessions.push((
                    field.name().as_ref(),
                    field.value().text().ok_or(InvalidNativeHours)?,
                ));
            }
            lineage
                .try_push(&HoursNativeRow {
                    row_kind: if *kind == 0 {
                        "coverage"
                    } else {
                        "session_day"
                    },
                    source_row_ordinal: *ordinal,
                    market_type: &native.market_type,
                    product: &native.product,
                    date: &native.date,
                    is_open: native.is_open,
                    category: native_text(&native.category)?,
                    session_presence: match native.session_presence {
                        NativeField::Absent => MarketCalendarSessionPresence::Missing,
                        NativeField::Null => MarketCalendarSessionPresence::SourceNull,
                        NativeField::Value(()) => MarketCalendarSessionPresence::Reported,
                    },
                    sessions,
                    scope: observation.scope(),
                })
                .map_err(|_| InvalidEvidence)?;
        }
        let lineage = lineage.finish().map_err(|_| InvalidEvidence)?;
        let revisions =
            ExtractionRevisionPlan::locally_observed_with_native_lineage(batch.records().len())
                .map_err(|_| InvalidEvidence)?;
        let row_pages = vec![0; batch.records().len()];
        let binding =
            SealedProviderCaptureBinding::try_whole(parts.token, batch, lineage, row_pages)
                .map_err(|_| InvalidEvidence)?;
        binding.validate().map_err(|_| InvalidEvidence)?;
        Ok(SchwabSealedMarketHoursPublication {
            revisions,
            binding,
            returned_products: hours.len(),
        })
    }
}

fn native_day(
    native: &MarketHours,
    date: CalendarDate,
) -> Result<MarketCalendarDay, SchwabMarketHoursPublicationError> {
    let presence = match native.session_presence {
        NativeField::Absent => MarketCalendarSessionPresence::Missing,
        NativeField::Null => MarketCalendarSessionPresence::SourceNull,
        NativeField::Value(()) => MarketCalendarSessionPresence::Reported,
    };
    if native.sessions.len() > MAX_MARKET_CALENDAR_INTERVALS * 2 {
        return Err(ResourceBound);
    }
    let mut pairs = BTreeMap::<(&str, u16), (Option<&str>, Option<&str>)>::new();
    for field in &native.sessions {
        let (kind, rest) = field.name().split_once('[').ok_or(InvalidNativeHours)?;
        let (ordinal, boundary) = rest.split_once("].").ok_or(InvalidNativeHours)?;
        let index = ordinal.parse::<u16>().map_err(|_| InvalidNativeHours)?;
        if ordinal != index.to_string() || usize::from(index) >= MAX_MARKET_CALENDAR_INTERVALS {
            return Err(InvalidNativeHours);
        }
        let value = field.value().text().ok_or(InvalidNativeHours)?;
        let pair = pairs.entry((kind, index)).or_default();
        let target = match boundary {
            "start" => &mut pair.0,
            "end" => &mut pair.1,
            _ => return Err(InvalidNativeHours),
        };
        if target.replace(value).is_some() {
            return Err(InvalidNativeHours);
        }
    }
    let mut intervals = Vec::new();
    intervals
        .try_reserve_exact(pairs.len())
        .map_err(|_| ResourceBound)?;
    for ((kind, ordinal), (start, end)) in pairs {
        let role = match kind {
            "regularMarket" => MarketCalendarSessionRole::Core,
            "preMarket" => MarketCalendarSessionRole::Pre,
            "postMarket" => MarketCalendarSessionRole::Post,
            _ => MarketCalendarSessionRole::SourceDefined,
        };
        intervals.push(MarketCalendarInterval {
            native_kind: source_text(kind)?,
            native_ordinal: ordinal,
            role,
            start: boundary(start.ok_or(InvalidNativeHours)?)?,
            end: boundary(end.ok_or(InvalidNativeHours)?)?,
        });
    }
    MarketCalendarDay::try_new(MarketCalendarDayInput {
        date,
        status: if native.is_open {
            MarketCalendarDayStatus::ExplicitlyOpen
        } else {
            MarketCalendarDayStatus::ExplicitlyClosed
        },
        category: native_text(&native.category)?,
        settlement_date: MarketCalendarField::Missing,
        session_presence: presence,
        intervals,
    })
    .map_err(|_| InvalidNativeHours)
}

fn request_scope(
    url: &str,
    route: ReadOnlyRoute,
) -> Result<(BTreeSet<String>, Option<CalendarDate>), SchwabMarketHoursPublicationError> {
    let url = Url::parse(url).map_err(|_| InvalidEvidence)?;
    let mut date = None;
    let mut markets = BTreeSet::new();
    if route == ReadOnlyRoute::SingleMarket {
        let market = url
            .path()
            .strip_prefix("/marketdata/v1/markets/")
            .ok_or(InvalidEvidence)?;
        markets.insert(market.to_owned());
    }
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "date" if date.is_none() => date = Some(parse_date(&value)?),
            "markets" if route == ReadOnlyRoute::Markets && markets.is_empty() => {
                for market in value.split(',') {
                    if !markets.insert(market.to_owned()) {
                        return Err(InvalidEvidence);
                    }
                }
            }
            _ => return Err(InvalidEvidence),
        }
    }
    if markets.is_empty()
        || markets.iter().any(|v| {
            !matches!(
                v.as_str(),
                "equity" | "option" | "bond" | "future" | "forex"
            )
        })
    {
        return Err(InvalidEvidence);
    }
    Ok((markets, date))
}

fn parse_date(text: &str) -> Result<CalendarDate, SchwabMarketHoursPublicationError> {
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|_| InvalidNativeHours)?;
    if text.len() != 10 || date.to_string() != text {
        return Err(InvalidNativeHours);
    }
    CalendarDate::new(
        u16::try_from(date.year()).map_err(|_| InvalidNativeHours)?,
        date.month() as u8,
        date.day() as u8,
    )
    .map_err(|_| InvalidNativeHours)
}
fn boundary(text: &str) -> Result<MarketCalendarBoundary, SchwabMarketHoursPublicationError> {
    let value = DateTime::parse_from_rfc3339(text).map_err(|_| InvalidNativeHours)?;
    // -00:00 represents an unknown local offset, not observed UTC offset zero.
    if text.ends_with("-00:00") || value.timestamp_subsec_nanos() >= 1_000_000_000 {
        return Err(InvalidNativeHours);
    }
    MarketCalendarBoundary::try_new(
        Timestamp::from_unix_nanos(value.timestamp_nanos_opt().ok_or(InvalidNativeHours)?),
        value.offset().local_minus_utc(),
    )
    .map_err(|_| InvalidNativeHours)
}
fn source_text(value: &str) -> Result<MarketSourceText, SchwabMarketHoursPublicationError> {
    MarketSourceText::try_new(value).map_err(|_| InvalidNativeHours)
}
fn native_text(
    value: &NativeField<Box<str>>,
) -> Result<MarketCalendarField<MarketSourceText>, SchwabMarketHoursPublicationError> {
    Ok(match value {
        NativeField::Absent => MarketCalendarField::Missing,
        NativeField::Null => MarketCalendarField::SourceNull,
        NativeField::Value(text) => MarketCalendarField::Reported(source_text(text)?),
    })
}
fn date_digest(date: CalendarDate) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(MARKET_CALENDAR_DATE_MEMBERSHIP_DOMAIN);
    hash.update(1u32.to_be_bytes());
    hash.update(date.year().to_be_bytes());
    hash.update([date.month(), date.day()]);
    sha256_digest(hash.finalize().into())
}
fn source_identifier(
    market: &str,
    product: &str,
    date: CalendarDate,
    kind: usize,
) -> Result<SourceIdentifier, SchwabMarketHoursPublicationError> {
    let bytes = serde_json::to_vec(&(market, product, date, kind)).map_err(|_| InvalidEvidence)?;
    SourceIdentifier::try_from(format!(
        "schwab:hours:{}",
        hex(Sha256::digest(bytes).into())
    ))
    .map_err(|_| InvalidEvidence)
}
fn sha256_digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}
fn hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Serialize)]
struct HoursSidecar<'a> {
    version: u16,
    family: &'static str,
    request_url: &'a str,
    request_sha256: [u8; 32],
    response_sha256: [u8; 32],
    response_bytes: u64,
    received_at_unix_millis: u64,
    provider_schema: &'static str,
    provider_schema_version: u16,
    token_generation: u64,
    qualification_receipt: EvidenceDigest,
    qualification_observation: EvidenceDigest,
    returned_products: usize,
    requested_markets: Vec<&'a str>,
    unknown_field_count: usize,
    unknown_field_bytes: usize,
    unknown_field_paths: &'a [Box<str>],
    unknown_field_digest: [u8; 32],
}
#[derive(Serialize)]
struct HoursNativeRow<'a> {
    row_kind: &'static str,
    source_row_ordinal: usize,
    market_type: &'a str,
    product: &'a str,
    date: &'a str,
    is_open: bool,
    category: MarketCalendarField<MarketSourceText>,
    session_presence: MarketCalendarSessionPresence,
    sessions: Vec<(&'a str, &'a str)>,
    scope: &'a MarketCalendarScope,
}
