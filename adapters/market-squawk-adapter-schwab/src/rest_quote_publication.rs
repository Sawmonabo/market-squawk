//! Seal-first Schwab REST quote publication.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use market_squawk_domain::{
    ConnectionGeneration as DomainConnectionGeneration, DataQuality, DigestAlgorithm,
    EvidenceDigest, InstrumentId, LiveEventClass, LiveProvenance, LotSize, MarketDepth,
    ProviderChannel, ProviderProduct, SourceIdentifier, TickSize, Timestamp, VenueId,
};
use market_squawk_sources::{
    ProviderCaptureError, ProviderMarketEventBatch, ProviderMarketEventNativeLineageBatch,
    ProviderNativeLineageImplementation, SchwabMarketDataFamily,
    SealedProviderResponseMarketEventBinding,
};
use serde::Serialize;
use thiserror::Error;

use crate::transport::SchwabSealedRestResponseParts;
use crate::{
    NativeField, NativeScalar, QuoteComponentField, ReadOnlyRoute, SchwabCanonicalError,
    SchwabMarketDataDelay, SchwabMarketDataQualification, SchwabQuote, SchwabQuoteAbstention,
    SchwabQuoteCanonicalOutcome, SchwabResolvedProviderIdentity, SchwabRestPayload,
    SchwabSealedRestResponse, canonicalize_quote,
};

/// Exact REST feed/venue/depth/delay and live-provenance binding for one quote record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabRestQuoteMarketDataEvidence {
    session_id: SourceIdentifier,
    connection_generation: DomainConnectionGeneration,
    reference_venue_id: VenueId,
    qualification: SchwabMarketDataQualification,
}

impl SchwabRestQuoteMarketDataEvidence {
    /// Constructs explicit top-of-book REST quote evidence.
    pub fn try_new(
        session_id: SourceIdentifier,
        connection_generation: DomainConnectionGeneration,
        reference_venue_id: VenueId,
        qualification: SchwabMarketDataQualification,
    ) -> Result<Self, SchwabRestQuotePublicationError> {
        if qualification.family() != SchwabMarketDataFamily::Quotes
            || qualification.depth().canonical() != Some(MarketDepth::TopOfBook)
        {
            return Err(SchwabRestQuotePublicationError::InvalidEvidence);
        }
        Ok(Self {
            session_id,
            connection_generation,
            reference_venue_id,
            qualification,
        })
    }

    /// Exact application-owned REST observation-session identity.
    pub const fn session_id(&self) -> &SourceIdentifier {
        &self.session_id
    }

    /// Exact application-owned REST observation generation.
    pub const fn connection_generation(&self) -> DomainConnectionGeneration {
        self.connection_generation
    }

    /// Exact qualified provider feed label.
    pub const fn feed(&self) -> &SourceIdentifier {
        self.qualification.feed()
    }

    /// Canonical/reference venue retained for product identity; the REST payload reports no venue.
    pub const fn venue_id(&self) -> &VenueId {
        &self.reference_venue_id
    }

    /// Exact top-of-book depth classification.
    pub const fn depth(&self) -> MarketDepth {
        MarketDepth::TopOfBook
    }

    /// Explicit provider delay state.
    pub const fn delay(&self) -> SchwabMarketDataDelay {
        self.qualification.delay()
    }

    /// Exact archival quality retained by canonical provenance.
    pub const fn quality(&self) -> DataQuality {
        self.qualification.quality()
    }

    /// Exact provider product retained by canonical provenance.
    pub const fn provider_product(&self) -> &ProviderProduct {
        self.qualification.provider_product()
    }

    /// Exact provider channel retained by canonical provenance.
    pub const fn provider_channel(&self) -> &ProviderChannel {
        self.qualification.provider_channel()
    }

    /// Nonzero SHA-256 evidence for the supplied market semantics.
    pub const fn qualification_evidence(&self) -> EvidenceDigest {
        self.qualification.observation_evidence()
    }
}

/// Exact semantic inputs for one provider-symbol record in a sealed quote response.
#[derive(Debug)]
pub struct SchwabRestQuoteRecordRequest {
    identity: SchwabResolvedProviderIdentity,
    instrument_id: InstrumentId,
    source_identifier: SourceIdentifier,
    provenance: LiveProvenance,
    tick_size: TickSize,
    lot_size: LotSize,
    market_data: SchwabRestQuoteMarketDataEvidence,
}

impl SchwabRestQuoteRecordRequest {
    /// Constructs one exact quote mapping input. Validation occurs against the sealed response.
    #[allow(
        clippy::too_many_arguments,
        reason = "identity, economics, provenance, and market semantics remain explicit"
    )]
    pub fn new(
        identity: SchwabResolvedProviderIdentity,
        instrument_id: InstrumentId,
        source_identifier: SourceIdentifier,
        provenance: LiveProvenance,
        tick_size: TickSize,
        lot_size: LotSize,
        market_data: SchwabRestQuoteMarketDataEvidence,
    ) -> Self {
        Self {
            identity,
            instrument_id,
            source_identifier,
            provenance,
            tick_size,
            lot_size,
            market_data,
        }
    }
}

/// Complete caller-owned mapping inputs for one already sealed REST quote response.
#[derive(Debug)]
pub struct SchwabRestQuotePublicationRequest {
    records: Vec<SchwabRestQuoteRecordRequest>,
}

impl SchwabRestQuotePublicationRequest {
    /// Constructs exact mapping inputs; duplicate provider symbols are rejected on consumption.
    pub fn new(records: Vec<SchwabRestQuoteRecordRequest>) -> Self {
        Self { records }
    }
}

/// Why one exact REST quote record did not become a canonical quote event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabRestQuoteDispositionReason {
    /// No mapping/identity input was supplied for the exact returned provider symbol.
    MissingMappingInput,
    /// Provider data was retained but did not contain a publishable quote side.
    QuoteAbstention(SchwabQuoteAbstention),
    /// Exact provider values failed the closed canonical mapper.
    CanonicalMappingRejected,
}

/// Explicit non-publication result for one provider REST quote record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabRestQuoteDisposition {
    provider_record_ordinal: u32,
    provider_symbol: crate::ProviderIdentifier,
    reason: SchwabRestQuoteDispositionReason,
}

impl SchwabRestQuoteDisposition {
    /// Zero-based provider-record ordinal inside the typed response.
    pub const fn provider_record_ordinal(&self) -> u32 {
        self.provider_record_ordinal
    }

    /// Exact provider symbol/key.
    pub const fn provider_symbol(&self) -> &crate::ProviderIdentifier {
        &self.provider_symbol
    }

    /// Explicit reason no canonical row was minted.
    pub const fn reason(&self) -> SchwabRestQuoteDispositionReason {
        self.reason
    }
}

/// Result of attempting typed quote publication from an already sealed REST response.
#[derive(Debug)]
pub enum SchwabRestQuotePublicationOutcome {
    /// At least one canonical quote row is bound to the exact sealed response page.
    Published(Box<SchwabSealedRestQuotePublication>),
    /// No canonical row existed; the non-cloneable sealed raw response remains recoverable.
    SealedRaw(Box<SchwabSealedRawRestQuotePublication>),
}

/// Sealed raw quote response with explicit reasons no typed event batch was minted.
#[derive(Debug)]
pub struct SchwabSealedRawRestQuotePublication {
    response: SchwabSealedRestResponse,
    dispositions: Box<[SchwabRestQuoteDisposition]>,
}

impl SchwabSealedRawRestQuotePublication {
    /// Exact provider-record dispositions.
    pub const fn dispositions(&self) -> &[SchwabRestQuoteDisposition] {
        &self.dispositions
    }

    /// Returns the still non-cloneable sealed response for another truthful typed attempt.
    pub fn into_response(self) -> SchwabSealedRestResponse {
        self.response
    }
}

/// Non-cloneable canonical/native/physical REST quote publication authority.
#[derive(Debug)]
pub struct SchwabSealedRestQuotePublication {
    binding: SealedProviderResponseMarketEventBinding,
    dispositions: Box<[SchwabRestQuoteDisposition]>,
}

impl SchwabSealedRestQuotePublication {
    /// Sole shared typed response-event publication authority.
    pub const fn binding(&self) -> &SealedProviderResponseMarketEventBinding {
        &self.binding
    }

    /// Exact provider records retained raw but omitted from canonical rows.
    pub const fn dispositions(&self) -> &[SchwabRestQuoteDisposition] {
        &self.dispositions
    }

    /// Consumes the adapter handoff into the shared one-shot response-event authority.
    pub fn into_binding(self) -> SealedProviderResponseMarketEventBinding {
        self.binding
    }
}

impl SchwabSealedRestResponse {
    /// Maps named REST quotes only after the exact response body has crossed the physical seal.
    pub fn into_quote_publication(
        self,
        request: SchwabRestQuotePublicationRequest,
    ) -> Result<SchwabRestQuotePublicationOutcome, SchwabRestQuotePublicationError> {
        if !matches!(
            self.route(),
            ReadOnlyRoute::Quotes | ReadOnlyRoute::SingleQuote
        ) || self.family() != crate::SchwabRestFamily::Quotes
        {
            return Err(SchwabRestQuotePublicationError::FamilyMismatch);
        }
        let mut inputs = BTreeMap::new();
        for input in request.records {
            let symbol = input.identity.provider_symbol().clone();
            if inputs.insert(symbol, input).is_some() {
                return Err(SchwabRestQuotePublicationError::MappingMismatch);
            }
        }

        let parts = self.parts();
        let SchwabRestPayload::Quotes(parsed) = &parts.payload else {
            return Err(SchwabRestQuotePublicationError::FamilyMismatch);
        };
        if parsed.raw_sha256() != parts.receipt.body_sha256()
            || parts.accounting.provider_records
                != u64::try_from(parsed.value().quotes().len())
                    .map_err(|_| SchwabRestQuotePublicationError::InvalidEvidence)?
        {
            return Err(SchwabRestQuotePublicationError::InvalidEvidence);
        }
        if inputs.values().any(|input| {
            !input
                .market_data
                .qualification
                .validates_rest_receipt(SchwabMarketDataFamily::Quotes, &parts.receipt)
        }) {
            return Err(SchwabRestQuotePublicationError::InvalidEvidence);
        }
        let mut used = BTreeSet::new();
        let mut events = Vec::new();
        let mut native_rows = Vec::new();
        let mut dispositions = Vec::new();
        for (provider_record_ordinal, quote) in parsed.value().quotes().iter().enumerate() {
            let ordinal = u32::try_from(provider_record_ordinal)
                .map_err(|_| SchwabRestQuotePublicationError::InvalidEvidence)?;
            let Some(input) = inputs.get(quote.symbol()) else {
                dispositions.push(SchwabRestQuoteDisposition {
                    provider_record_ordinal: ordinal,
                    provider_symbol: quote.symbol().clone(),
                    reason: SchwabRestQuoteDispositionReason::MissingMappingInput,
                });
                continue;
            };
            used.insert(quote.symbol().clone());
            validate_quote_mapping(parts, quote, input)?;
            match canonicalize_quote(
                quote,
                input.identity.clone(),
                input.provenance.clone(),
                input.tick_size,
                input.lot_size,
            ) {
                Ok(SchwabQuoteCanonicalOutcome::Mapped {
                    provider_instrument_id,
                    resolution_evidence,
                    event,
                }) => {
                    if &provider_instrument_id != input.identity.provider_instrument_id()
                        || resolution_evidence != input.identity.resolution_evidence()
                    {
                        return Err(SchwabRestQuotePublicationError::InvalidEvidence);
                    }
                    native_rows.push(encode_quote_native_row(
                        ordinal,
                        quote,
                        input,
                        resolution_evidence,
                    )?);
                    events.push(*event);
                }
                Ok(SchwabQuoteCanonicalOutcome::Abstained { reason, .. }) => {
                    dispositions.push(SchwabRestQuoteDisposition {
                        provider_record_ordinal: ordinal,
                        provider_symbol: quote.symbol().clone(),
                        reason: SchwabRestQuoteDispositionReason::QuoteAbstention(reason),
                    });
                }
                Err(_) => dispositions.push(SchwabRestQuoteDisposition {
                    provider_record_ordinal: ordinal,
                    provider_symbol: quote.symbol().clone(),
                    reason: SchwabRestQuoteDispositionReason::CanonicalMappingRejected,
                }),
            }
        }
        if used.len() != inputs.len() {
            return Err(SchwabRestQuotePublicationError::MappingMismatch);
        }
        if events.is_empty() {
            return Ok(SchwabRestQuotePublicationOutcome::SealedRaw(Box::new(
                SchwabSealedRawRestQuotePublication {
                    response: self,
                    dispositions: dispositions.into_boxed_slice(),
                },
            )));
        }

        let sidecar = encode_quote_sidecar(parts, &dispositions)?;
        let SchwabSealedRestResponseParts {
            coordinates, token, ..
        } = self.into_parts();
        let batch = ProviderMarketEventBatch::try_new(
            coordinates.source_id().clone(),
            coordinates.metadata_revision().clone(),
            coordinates.dataset().clone(),
            events,
        )?;
        let native_lineage = ProviderMarketEventNativeLineageBatch::try_new(
            ProviderNativeLineageImplementation::SchwabRestMarketDataV1,
            &batch,
            native_rows,
            Some(sidecar),
        )?;
        let row_capture_page_ordinals = vec![0; batch.events().len()];
        let binding = SealedProviderResponseMarketEventBinding::try_new(
            token,
            batch,
            native_lineage,
            row_capture_page_ordinals,
        )?;
        binding.validate()?;
        Ok(SchwabRestQuotePublicationOutcome::Published(Box::new(
            SchwabSealedRestQuotePublication {
                binding,
                dispositions: dispositions.into_boxed_slice(),
            },
        )))
    }
}

fn validate_quote_mapping(
    parts: &SchwabSealedRestResponseParts,
    quote: &SchwabQuote,
    input: &SchwabRestQuoteRecordRequest,
) -> Result<(), SchwabRestQuotePublicationError> {
    let binding = input.provenance.binding();
    let received_at = timestamp_from_millis(parts.receipt.received_at_unix_millis())?;
    let source_timestamp = quote_source_timestamp(quote)?;
    if input.identity.provider_symbol() != quote.symbol()
        || input.identity.resolution_evidence().algorithm() != DigestAlgorithm::Sha256
        || input.identity.resolution_evidence().bytes() == [0; 32]
        || input.market_data.depth() != MarketDepth::TopOfBook
        || binding.source_id() != parts.coordinates.source_id()
        || binding.metadata_revision() != parts.coordinates.metadata_revision()
        || binding.session_id() != &input.market_data.session_id
        || binding.connection_generation() != input.market_data.connection_generation
        || binding.venue_id() != input.market_data.venue_id()
        || binding.instrument_id() != input.instrument_id
        || binding.provider_product() != input.market_data.provider_product()
        || binding.provider_channel() != input.market_data.provider_channel()
        || binding.event_class() != LiveEventClass::Quote
        || binding.source_identifier() != &input.source_identifier
        || binding.payload_digest()
            != EvidenceDigest::new(DigestAlgorithm::Sha256, parts.receipt.body_sha256())
        || input.provenance.source_timestamp() != source_timestamp
        || input.provenance.received_at() != received_at
        || input.provenance.recorded_quality() != input.market_data.quality()
        || realtime_delay_conflicts(quote, input.market_data.delay())?
    {
        return Err(SchwabRestQuotePublicationError::InvalidEvidence);
    }
    Ok(())
}

fn realtime_delay_conflicts(
    quote: &SchwabQuote,
    delay: SchwabMarketDataDelay,
) -> Result<bool, SchwabRestQuotePublicationError> {
    Ok(match quote.realtime() {
        NativeField::Value(true) => delay != SchwabMarketDataDelay::RealTime,
        NativeField::Value(false) => delay == SchwabMarketDataDelay::RealTime,
        NativeField::Absent | NativeField::Null => false,
    })
}

fn quote_source_timestamp(
    quote: &SchwabQuote,
) -> Result<Option<Timestamp>, SchwabRestQuotePublicationError> {
    match quote
        .quote_fields()
        .iter()
        .find(|field| field.name() == &QuoteComponentField::QuoteTime)
        .map(|field| field.value())
    {
        None | Some(NativeScalar::Null) => Ok(None),
        Some(NativeScalar::Number(value)) => value
            .as_str()
            .parse::<u64>()
            .map_err(|_| SchwabRestQuotePublicationError::InvalidEvidence)
            .and_then(timestamp_from_millis)
            .map(Some),
        Some(NativeScalar::Bool(_) | NativeScalar::Text(_)) => {
            Err(SchwabRestQuotePublicationError::InvalidEvidence)
        }
    }
}

fn timestamp_from_millis(value: u64) -> Result<Timestamp, SchwabRestQuotePublicationError> {
    let nanos = i64::try_from(value)
        .ok()
        .and_then(|value| value.checked_mul(1_000_000))
        .ok_or(SchwabRestQuotePublicationError::InvalidEvidence)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabRestQuoteNativeRowV1<'a> {
    version: u16,
    family: &'static str,
    provider_record_ordinal: u32,
    provider_symbol: &'a str,
    asset_main_type: SchwabNativeFieldV1<'a>,
    asset_sub_type: SchwabNativeFieldV1<'a>,
    realtime: SchwabNativeFieldV1<'a>,
    ssid: SchwabNativeFieldV1<'a>,
    bid_price: SchwabNativeFieldV1<'a>,
    bid_size: SchwabNativeFieldV1<'a>,
    ask_price: SchwabNativeFieldV1<'a>,
    ask_size: SchwabNativeFieldV1<'a>,
    quote_time: SchwabNativeFieldV1<'a>,
    instrument_id: InstrumentId,
    provider_instrument_id: &'a str,
    source_identifier: &'a str,
    resolution_evidence: EvidenceDigest,
    feed: &'a str,
    reference_venue: &'a str,
    provider_reported_venue: Option<&'a str>,
    depth: MarketDepth,
    delay: SchwabMarketDataDelay,
    quality: DataQuality,
    provider_product: &'a str,
    provider_channel: &'a str,
    qualification_evidence: EvidenceDigest,
    qualification_receipt_evidence: EvidenceDigest,
    qualification_family: SchwabMarketDataFamily,
    qualification_observed_at: Timestamp,
    qualification_response_observed_at: Timestamp,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
enum SchwabNativeFieldV1<'a> {
    Absent,
    Null,
    Bool(bool),
    Unsigned(u64),
    Number(&'a str),
    Text(&'a str),
}

fn encode_quote_native_row(
    provider_record_ordinal: u32,
    quote: &SchwabQuote,
    input: &SchwabRestQuoteRecordRequest,
    resolution_evidence: EvidenceDigest,
) -> Result<Bytes, SchwabRestQuotePublicationError> {
    serde_json::to_vec(&SchwabRestQuoteNativeRowV1 {
        version: 1,
        family: "schwab.rest.quote",
        provider_record_ordinal,
        provider_symbol: quote.symbol().as_str(),
        asset_main_type: native_text_field(quote.asset_main_type()),
        asset_sub_type: native_text_field(quote.asset_sub_type()),
        realtime: native_bool_field(quote.realtime()),
        ssid: native_u64_field(quote.ssid()),
        bid_price: quote_field(quote, QuoteComponentField::BidPrice),
        bid_size: quote_field(quote, QuoteComponentField::BidSize),
        ask_price: quote_field(quote, QuoteComponentField::AskPrice),
        ask_size: quote_field(quote, QuoteComponentField::AskSize),
        quote_time: quote_field(quote, QuoteComponentField::QuoteTime),
        instrument_id: input.instrument_id,
        provider_instrument_id: input.identity.provider_instrument_id().as_str(),
        source_identifier: input.source_identifier.as_str(),
        resolution_evidence,
        feed: input.market_data.feed().as_str(),
        reference_venue: input.market_data.venue_id().as_str(),
        provider_reported_venue: None,
        depth: input.market_data.depth(),
        delay: input.market_data.delay(),
        quality: input.market_data.quality(),
        provider_product: input
            .market_data
            .provider_product()
            .as_source_identifier()
            .as_str(),
        provider_channel: input
            .market_data
            .provider_channel()
            .as_source_identifier()
            .as_str(),
        qualification_evidence: input.market_data.qualification_evidence(),
        qualification_receipt_evidence: input.market_data.qualification.receipt_evidence(),
        qualification_family: input.market_data.qualification.family(),
        qualification_observed_at: input.market_data.qualification.family_observed_at(),
        qualification_response_observed_at: input.market_data.qualification.response_observed_at(),
    })
    .map(Bytes::from)
    .map_err(|_| SchwabRestQuotePublicationError::NativeEncoding)
}

fn quote_field(quote: &SchwabQuote, name: QuoteComponentField) -> SchwabNativeFieldV1<'_> {
    quote
        .quote_fields()
        .iter()
        .find(|field| field.name() == &name)
        .map(|field| native_scalar(field.value()))
        .unwrap_or(SchwabNativeFieldV1::Absent)
}

fn native_scalar(value: &NativeScalar) -> SchwabNativeFieldV1<'_> {
    match value {
        NativeScalar::Null => SchwabNativeFieldV1::Null,
        NativeScalar::Bool(value) => SchwabNativeFieldV1::Bool(*value),
        NativeScalar::Number(value) => SchwabNativeFieldV1::Number(value.as_str()),
        NativeScalar::Text(value) => SchwabNativeFieldV1::Text(value),
    }
}

fn native_text_field(value: &NativeField<Box<str>>) -> SchwabNativeFieldV1<'_> {
    match value {
        NativeField::Absent => SchwabNativeFieldV1::Absent,
        NativeField::Null => SchwabNativeFieldV1::Null,
        NativeField::Value(value) => SchwabNativeFieldV1::Text(value),
    }
}

fn native_bool_field(value: &NativeField<bool>) -> SchwabNativeFieldV1<'_> {
    match value {
        NativeField::Absent => SchwabNativeFieldV1::Absent,
        NativeField::Null => SchwabNativeFieldV1::Null,
        NativeField::Value(value) => SchwabNativeFieldV1::Bool(*value),
    }
}

fn native_u64_field(value: &NativeField<u64>) -> SchwabNativeFieldV1<'_> {
    match value {
        NativeField::Absent => SchwabNativeFieldV1::Absent,
        NativeField::Null => SchwabNativeFieldV1::Null,
        NativeField::Value(value) => SchwabNativeFieldV1::Unsigned(*value),
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabRestQuoteNativeSidecarV1<'a> {
    version: u16,
    family: &'static str,
    service: &'static str,
    route: &'static str,
    provider_schema: &'static str,
    provider_schema_version: u16,
    request_url: &'a str,
    request_sha256: [u8; 32],
    response_sha256: [u8; 32],
    response_status: u16,
    response_bytes: u64,
    declared_response_bytes: Option<u64>,
    received_at_unix_millis: u64,
    latency_millis: u64,
    token_generation: u64,
    response_headers: Vec<SchwabRestQuoteHeaderV1<'a>>,
    requested_items: u64,
    returned_items: u64,
    missing_items: u64,
    unexpected_items: u64,
    provider_records: u64,
    unknown_field_count: usize,
    unknown_field_bytes: usize,
    unknown_field_paths: &'a [Box<str>],
    unknown_field_digest: [u8; 32],
    dispositions: Vec<SchwabRestQuoteDispositionSidecarV1<'a>>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabRestQuoteHeaderV1<'a> {
    name: &'a str,
    value: &'a [u8],
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabRestQuoteDispositionSidecarV1<'a> {
    provider_record_ordinal: u32,
    provider_symbol: &'a str,
    reason: &'static str,
}

fn encode_quote_sidecar(
    parts: &SchwabSealedRestResponseParts,
    dispositions: &[SchwabRestQuoteDisposition],
) -> Result<Bytes, SchwabRestQuotePublicationError> {
    let SchwabRestPayload::Quotes(parsed) = &parts.payload else {
        return Err(SchwabRestQuotePublicationError::FamilyMismatch);
    };
    let unknown = parsed.unknown_fields();
    let response_headers = parts
        .receipt
        .headers()
        .iter()
        .map(|header| SchwabRestQuoteHeaderV1 {
            name: header.name(),
            value: header.value(),
        })
        .collect();
    let disposition_rows = dispositions
        .iter()
        .map(|disposition| SchwabRestQuoteDispositionSidecarV1 {
            provider_record_ordinal: disposition.provider_record_ordinal,
            provider_symbol: disposition.provider_symbol.as_str(),
            reason: disposition_reason(disposition.reason),
        })
        .collect();
    serde_json::to_vec(&SchwabRestQuoteNativeSidecarV1 {
        version: 1,
        family: "schwab.rest.quotes",
        service: "schwab-market-data-rest",
        route: match parts.receipt.route() {
            ReadOnlyRoute::Quotes => "quotes",
            ReadOnlyRoute::SingleQuote => "single-quote",
            _ => return Err(SchwabRestQuotePublicationError::FamilyMismatch),
        },
        provider_schema: parsed.schema_name(),
        provider_schema_version: parsed.schema_version(),
        request_url: parts.receipt.request_url(),
        request_sha256: parts.receipt.request_sha256(),
        response_sha256: parts.receipt.body_sha256(),
        response_status: parts.receipt.status(),
        response_bytes: parts.receipt.body_bytes(),
        declared_response_bytes: parts.receipt.declared_body_bytes(),
        received_at_unix_millis: parts.receipt.received_at_unix_millis(),
        latency_millis: parts.receipt.latency_ms(),
        token_generation: parts.receipt.token_generation().get(),
        response_headers,
        requested_items: parts.accounting.requested,
        returned_items: parts.accounting.returned,
        missing_items: parts.accounting.missing,
        unexpected_items: parts.accounting.unexpected,
        provider_records: parts.accounting.provider_records,
        unknown_field_count: unknown.field_count(),
        unknown_field_bytes: unknown.encoded_bytes(),
        unknown_field_paths: unknown.paths(),
        unknown_field_digest: unknown.digest(),
        dispositions: disposition_rows,
    })
    .map(Bytes::from)
    .map_err(|_| SchwabRestQuotePublicationError::NativeEncoding)
}

const fn disposition_reason(reason: SchwabRestQuoteDispositionReason) -> &'static str {
    match reason {
        SchwabRestQuoteDispositionReason::MissingMappingInput => "missing_mapping_input",
        SchwabRestQuoteDispositionReason::QuoteAbstention(SchwabQuoteAbstention::NoQuotedSide) => {
            "no_quoted_side"
        }
        SchwabRestQuoteDispositionReason::QuoteAbstention(SchwabQuoteAbstention::IncompleteBid) => {
            "incomplete_bid"
        }
        SchwabRestQuoteDispositionReason::QuoteAbstention(SchwabQuoteAbstention::IncompleteAsk) => {
            "incomplete_ask"
        }
        SchwabRestQuoteDispositionReason::QuoteAbstention(SchwabQuoteAbstention::ZeroSize) => {
            "zero_size"
        }
        SchwabRestQuoteDispositionReason::CanonicalMappingRejected => "canonical_mapping_rejected",
    }
}

/// Secret-free Schwab REST quote seal/publication failure.
#[derive(Debug, Error)]
pub enum SchwabRestQuotePublicationError {
    /// The sealed response is not the quotes or single-quote family.
    #[error("sealed Schwab REST response is not a quote response")]
    FamilyMismatch,
    /// Source, identity, clock, feed, venue, delay, depth, or payload evidence differs.
    #[error("Schwab REST quote publication evidence is invalid")]
    InvalidEvidence,
    /// Caller mapping inputs do not align exactly to returned provider symbols.
    #[error("Schwab REST quote mapping inputs do not match the sealed response")]
    MappingMismatch,
    /// Provider-native semantic evidence could not be encoded.
    #[error("Schwab REST quote provider-native evidence could not be encoded")]
    NativeEncoding,
    /// Common typed response publication binding failed closed.
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
}

impl From<SchwabCanonicalError> for SchwabRestQuotePublicationError {
    fn from(_error: SchwabCanonicalError) -> Self {
        Self::InvalidEvidence
    }
}
