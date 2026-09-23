//! Seal-first Yahoo quote, history, and option publication.
//!
//! Yahoo remains explicit-demand indicative enrichment. Canonical identity and calendar semantics
//! arrive from the application; this adapter verifies them against the exact typed response and
//! consumes the already sealed raw authority into the matching shared publication family.

use std::collections::BTreeMap;
use std::num::NonZeroU16;

use bytes::Bytes;
use chrono::{DateTime, Datelike as _, Utc};
use market_squawk_domain::{
    BarTimeSemantics, BookLevel, CalendarDate, Currency, DataQuality, DigestAlgorithm,
    EvidenceDigest, ExactPayloadEvidence, InstrumentId, LotSize, MarketBarAdjustment,
    MarketBarObservation, MarketEvent, MetadataRevision, Money, OptionComponent,
    OptionComponentState, OptionKind, OptionSnapshotObservation, PayloadHash, PayloadReference,
    PriceTicks, ProviderInstrumentId, QuantityLots, ResearchContext, ResearchObservation,
    ResearchProvenance, ResearchProvenanceInput, ResearchTime, RevisionNumber, SourceIdentifier,
    TickSize, Timestamp, VenueId,
};
use market_squawk_sources::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, ExtractionBatch,
    ExtractionBatchAccumulator, ExtractionRecord, ExtractionRequest, ExtractionRevisionPlan,
    OptionMarketBatchDisposition, OptionMarketCompleteness, OptionMarketCompletenessInput,
    OptionMarketCursorState, OptionMarketRequestScope, ProviderMarketEventBatch,
    ProviderMarketEventNativeLineageBatch, ProviderNativeLineageBatch,
    ProviderNativeLineageBatchBuilder, ProviderNativeLineageImplementation,
    ProviderOptionMarketBatch, ProviderOptionMarketNativeLineageBatch,
    SealedProviderCaptureBinding, SealedProviderOptionMarketBinding,
    SealedProviderResponseMarketEventBinding,
};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::native::{YahooChartRequestEvidence, YahooNativePublicationEvidence};
use crate::{
    EvidenceAuthority, ProviderField, YahooBar, YahooChart, YahooEnrichment,
    YahooHttpAttemptReceipt, YahooOptionChain, YahooOptionContract, YahooOptionSide,
    YahooParsedResponse, YahooPublicationBinding, YahooPublicationBridgeError, YahooQuote,
    YahooRawReceipt, YahooRequestFamily, YahooSealedPublication, YahooSealedPublicationFamily,
    YahooSymbol,
};

const YAHOO_CANONICAL_MEDIA_TYPE: &str = "application-json";
const YAHOO_CANONICAL_FEED: &str = "yahoo-finance-experimental-chart";
const YAHOO_CANONICAL_REVISION_PREFIX: &str = "yahoo-local";

/// Externally resolved Yahoo-symbol identity. Yahoo never creates this authority itself.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct YahooCanonicalInstrumentAuthority {
    symbol: YahooSymbol,
    instrument_id: InstrumentId,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: Option<VenueId>,
    currency: Option<Currency>,
    mapping_revision: MetadataRevision,
    mapping_evidence: EvidenceDigest,
}

impl YahooCanonicalInstrumentAuthority {
    #[allow(
        clippy::too_many_arguments,
        reason = "identity, venue, currency, revision, and evidence remain explicit"
    )]
    pub fn try_new(
        symbol: YahooSymbol,
        instrument_id: InstrumentId,
        provider_instrument_id: ProviderInstrumentId,
        venue_id: Option<VenueId>,
        currency: Option<Currency>,
        mapping_revision: MetadataRevision,
        mapping_evidence: EvidenceDigest,
    ) -> Result<Self, YahooPublicationBridgeError> {
        if provider_instrument_id.as_str() != symbol.as_str()
            || mapping_evidence.algorithm() != DigestAlgorithm::Sha256
            || mapping_evidence.bytes() == [0; 32]
        {
            return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
        }
        Ok(Self {
            symbol,
            instrument_id,
            provider_instrument_id,
            venue_id,
            currency,
            mapping_revision,
            mapping_evidence,
        })
    }

    pub const fn symbol(&self) -> &YahooSymbol {
        &self.symbol
    }

    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    pub const fn venue_id(&self) -> Option<&VenueId> {
        self.venue_id.as_ref()
    }

    pub const fn currency(&self) -> Option<Currency> {
        self.currency
    }

    pub const fn mapping_revision(&self) -> &MetadataRevision {
        &self.mapping_revision
    }

    pub const fn mapping_evidence(&self) -> EvidenceDigest {
        self.mapping_evidence
    }
}

/// Application-owned identity and bar-time authority for one Yahoo chart response.
#[derive(Debug)]
pub struct YahooHistoricalPublicationRequest {
    extraction_request: ExtractionRequest,
    instrument: YahooCanonicalInstrumentAuthority,
    chart_time_semantics: Vec<BarTimeSemantics>,
    ingested_at: Timestamp,
}

impl YahooHistoricalPublicationRequest {
    pub fn new(
        extraction_request: ExtractionRequest,
        instrument: YahooCanonicalInstrumentAuthority,
        chart_time_semantics: Vec<BarTimeSemantics>,
        ingested_at: Timestamp,
    ) -> Self {
        Self {
            extraction_request,
            instrument,
            chart_time_semantics,
            ingested_at,
        }
    }
}

/// Complete history publication inputs for application ingestion.
#[derive(Debug)]
pub struct YahooSealedHistoricalPublication {
    revision_plan: ExtractionRevisionPlan,
    binding: SealedProviderCaptureBinding,
}

impl YahooSealedHistoricalPublication {
    pub const fn authority(&self) -> EvidenceAuthority {
        EvidenceAuthority::ExperimentalSupplementOnly
    }

    pub const fn governed_override_permitted(&self) -> bool {
        false
    }

    pub const fn revision_plan(&self) -> &ExtractionRevisionPlan {
        &self.revision_plan
    }

    pub const fn binding(&self) -> &SealedProviderCaptureBinding {
        &self.binding
    }

    pub fn into_parts(self) -> (ExtractionRevisionPlan, SealedProviderCaptureBinding) {
        (self.revision_plan, self.binding)
    }
}

impl YahooSealedPublication {
    /// Consumes a sealed chart response into canonical historical rows and local revision evidence.
    pub fn into_historical_publication(
        self,
        request: YahooHistoricalPublicationRequest,
    ) -> Result<YahooSealedHistoricalPublication, YahooPublicationBridgeError> {
        if self.family() != YahooSealedPublicationFamily::HistoricalBars {
            return Err(YahooPublicationBridgeError::InvalidCanonicalRequest);
        }
        let (_, token, raw, parsed, binding) = self.into_parts();
        let native_evidence = YahooNativePublicationEvidence::try_new(&raw, &parsed)?;
        let capture = token.persisted_receipt().capture();
        validate_historical_request(&raw, &binding, &request, capture)?;
        let YahooParsedResponse::Chart(chart) = parsed.as_ref() else {
            return Err(YahooPublicationBridgeError::InvalidCanonicalRequest);
        };
        let canonical = historical_batch(&raw, chart, &native_evidence, request)?;
        let batch = canonical
            .batch
            .try_bind_provider_capture(capture)
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let native_lineage = historical_native_lineage(
            &raw,
            chart,
            &native_evidence,
            &batch,
            &canonical.native_rows,
            &canonical.authority,
            &canonical.chart_time_semantics,
        )?;
        let revision_plan =
            ExtractionRevisionPlan::locally_observed_with_native_lineage(batch.records().len())
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let row_count = batch.records().len();
        let sealed = SealedProviderCaptureBinding::try_whole(
            token,
            batch,
            native_lineage,
            vec![0; row_count],
        )?;
        sealed.validate()?;
        Ok(YahooSealedHistoricalPublication {
            revision_plan,
            binding: sealed,
        })
    }
}

fn validate_historical_request(
    raw: &YahooRawReceipt,
    binding: &YahooPublicationBinding,
    request: &YahooHistoricalPublicationRequest,
    capture: &market_squawk_sources::ProviderCaptureSetReceipt,
) -> Result<(), YahooPublicationBridgeError> {
    let object = request.extraction_request.object();
    let received_at = timestamp_from_millis(raw.received_at_unix_ms)?;
    let available_at = timestamp_from_millis(raw.available_at_unix_ms)?;
    let body_bytes = u64::try_from(raw.response_bytes.len())
        .map_err(|_| YahooPublicationBridgeError::InvalidBodyLength)?;
    let body_digest = digest_from_hex(&raw.response_sha256_hex)?;
    let [target] = raw.request.requested_targets.as_slice() else {
        return Err(YahooPublicationBridgeError::InvalidCanonicalRequest);
    };
    if raw.request_family != YahooRequestFamily::ChartHistory
        || target.symbol != request.instrument.symbol
        || object.source_id() != binding.source_id()
        || object.metadata_revision() != binding.metadata_revision()
        || object.dataset().as_str() != super::http::dataset_identity(raw.request_family)
        || capture.source_id() != object.source_id()
        || capture.metadata_revision() != object.metadata_revision()
        || capture.dataset() != object.dataset()
        || capture.pages().len() != 1
        || capture.pages()[0].ordinal() != 0
        || capture.pages()[0].body_digest() != body_digest
        || capture.pages()[0].body_bytes() != body_bytes
        || capture.pages()[0].received_at() != received_at
        || object.media_type().as_str() != YAHOO_CANONICAL_MEDIA_TYPE
        || object.evidence().content_digest() != body_digest
        || object.expected_bytes() != Some(body_bytes)
        || object.effective_interval().starts_at() != received_at
        || object.effective_interval().ends_at().is_some()
        || object.published_at().is_some()
        || object.availability().conservative_available_at() != Some(available_at)
        || request.ingested_at < available_at
        || request.extraction_request.deadline() <= request.ingested_at
    {
        return Err(YahooPublicationBridgeError::InvalidCanonicalRequest);
    }
    Ok(())
}

struct HistoricalBuild {
    batch: ExtractionBatch,
    native_rows: Vec<YahooHistoricalNativeRowV1>,
    authority: YahooHistoricalNativeAuthorityV1,
    chart_time_semantics: Vec<BarTimeSemantics>,
}

fn historical_batch(
    raw: &YahooRawReceipt,
    chart_enrichment: &YahooEnrichment<YahooChart>,
    native_evidence: &YahooNativePublicationEvidence,
    request: YahooHistoricalPublicationRequest,
) -> Result<HistoricalBuild, YahooPublicationBridgeError> {
    let chart = chart_enrichment
        .data
        .as_ref()
        .ok_or(YahooPublicationBridgeError::EmptyCanonicalOutput)?;
    if chart.symbol != request.instrument.symbol
        || chart.bars.len() != request.chart_time_semantics.len()
    {
        return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
    }
    if let ProviderField::Value(provider_currency) = &chart.currency
        && Some(
            Currency::try_from(provider_currency.as_str())
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalAuthority)?,
        ) != request.instrument.currency
    {
        return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
    }
    let request_evidence = native_evidence
        .chart_request_evidence()
        .ok_or(YahooPublicationBridgeError::InvalidCanonicalOutput)?;
    let interval = identifier(&format!(
        "yahoo-interval-{}",
        request_evidence.interval().provider_value()
    ))?;
    let mut output =
        HistoricalAccumulator::try_new(&request.extraction_request, raw, request.ingested_at)?;
    for (ordinal, (bar, semantics)) in chart
        .bars
        .iter()
        .zip(request.chart_time_semantics.iter().cloned())
        .enumerate()
    {
        let (
            ProviderField::Value(open),
            ProviderField::Value(high),
            ProviderField::Value(low),
            ProviderField::Value(close),
            ProviderField::Value(volume),
        ) = (&bar.open, &bar.high, &bar.low, &bar.close, &bar.volume)
        else {
            continue;
        };
        output.push_market_bar(
            ordinal,
            &chart.symbol,
            bar,
            &request.instrument,
            interval.clone(),
            semantics,
            *open,
            *high,
            *low,
            *close,
            *volume,
        )?;
    }
    let (batch, native_rows) = output.finish()?;
    Ok(HistoricalBuild {
        batch,
        native_rows,
        authority: YahooHistoricalNativeAuthorityV1::from(&request.instrument),
        chart_time_semantics: request.chart_time_semantics,
    })
}

struct HistoricalAccumulator<'a> {
    request: &'a ExtractionRequest,
    raw: &'a YahooRawReceipt,
    ingested_at: Timestamp,
    payload_reference: PayloadReference,
    batch: ExtractionBatchAccumulator,
    native_rows: Vec<YahooHistoricalNativeRowV1>,
}

impl<'a> HistoricalAccumulator<'a> {
    fn try_new(
        request: &'a ExtractionRequest,
        raw: &'a YahooRawReceipt,
        ingested_at: Timestamp,
    ) -> Result<Self, YahooPublicationBridgeError> {
        let digest = digest_from_hex(&raw.response_sha256_hex)?;
        Ok(Self {
            request,
            raw,
            ingested_at,
            payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                DigestAlgorithm::Sha256,
                digest.bytes(),
            )),
            batch: ExtractionBatchAccumulator::try_new(request)
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
            native_rows: Vec::new(),
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "canonical economics and native row coordinates stay explicit"
    )]
    fn push_market_bar(
        &mut self,
        provider_record_ordinal: usize,
        symbol: &YahooSymbol,
        native_bar: &YahooBar,
        authority: &YahooCanonicalInstrumentAuthority,
        interval: SourceIdentifier,
        time_semantics: BarTimeSemantics,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: u64,
    ) -> Result<(), YahooPublicationBridgeError> {
        let currency = authority
            .currency
            .ok_or(YahooPublicationBridgeError::InvalidCanonicalAuthority)?;
        let venue = authority
            .venue_id
            .as_ref()
            .ok_or(YahooPublicationBridgeError::InvalidCanonicalAuthority)?;
        let provider_timestamp = time_semantics.provider_timestamp();
        if provider_timestamp.unix_nanos()
            != native_bar
                .timestamp_unix_seconds
                .checked_mul(1_000_000_000)
                .ok_or(YahooPublicationBridgeError::InvalidTimestamp)?
        {
            return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
        }
        let received_at = timestamp_from_millis(self.raw.received_at_unix_ms)?;
        let available_at = timestamp_from_millis(self.raw.available_at_unix_ms)?;
        let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: self.request.object().source_id().clone(),
            instrument_id: Some(authority.instrument_id),
            venue_id: Some(venue.clone()),
            source_identifier: SourceIdentifier::try_from(format!(
                "yahoo-chart-bar-{provider_record_ordinal}"
            ))
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
            source_timestamp: Some(provider_timestamp),
            received_at,
            ingested_at: self.ingested_at,
            quality: DataQuality::Indicative,
            payload_reference: self.payload_reference.clone(),
            availability: market_squawk_domain::AvailabilityEvidence::local_first_observed(
                available_at,
            ),
        })
        .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let time = ResearchTime::new(
            provider_timestamp,
            None,
            RevisionNumber::new(1)
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
            None,
        )
        .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let context = ResearchContext::new(provenance, time)
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let observation = MarketBarObservation::new(
            context,
            authority.provider_instrument_id.clone(),
            SourceIdentifier::try_from(YAHOO_CANONICAL_FEED)
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
            interval,
            time_semantics,
            MarketBarAdjustment::Raw,
            Money::new(open, currency),
            Money::new(high, currency),
            Money::new(low, currency),
            Money::new(close, currency),
            Decimal::from(volume),
            None,
            None,
        )
        .map(ResearchObservation::MarketBar)
        .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let payload = serde_json::to_vec(&observation)
            .map(Bytes::from)
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&payload).into());
        let schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let revision = SourceIdentifier::try_from(format!(
            "{YAHOO_CANONICAL_REVISION_PREFIX}-{}-{}",
            self.native_rows.len(),
            &lower_hex(digest.bytes())[..16]
        ))
        .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        self.batch
            .push(
                ExtractionRecord::try_new(
                    self.request,
                    schema,
                    ExactPayloadEvidence::from_content_digest(digest),
                    provider_timestamp,
                    None,
                    AvailabilityEvidence::LocalFirstObserved {
                        observed_at: available_at,
                    },
                    revision,
                    None,
                    payload,
                )
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
            )
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        self.native_rows.push(YahooHistoricalNativeRowV1 {
            version: 1,
            provider_record_ordinal,
            provider_symbol: symbol.as_str().to_owned(),
            canonical_field: identifier("yahoo.raw-ohlcv-bar")?,
            native_value: serde_json::to_value(native_bar)
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
        });
        Ok(())
    }

    fn finish(
        self,
    ) -> Result<(ExtractionBatch, Vec<YahooHistoricalNativeRowV1>), YahooPublicationBridgeError>
    {
        if self.native_rows.is_empty() {
            return Err(YahooPublicationBridgeError::EmptyCanonicalOutput);
        }
        let batch = self
            .batch
            .finish()
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        if batch.records().len() != self.native_rows.len() {
            return Err(YahooPublicationBridgeError::InvalidCanonicalOutput);
        }
        Ok((batch, self.native_rows))
    }
}

/// One externally resolved canonical Yahoo quote row.
#[derive(Debug)]
pub struct YahooQuoteCanonicalRow {
    response_observation_ordinal: usize,
    instrument: YahooCanonicalInstrumentAuthority,
    event: MarketEvent,
    tick_size: TickSize,
    lot_size: LotSize,
}

impl YahooQuoteCanonicalRow {
    pub fn try_new(
        response_observation_ordinal: usize,
        instrument: YahooCanonicalInstrumentAuthority,
        event: MarketEvent,
        tick_size: TickSize,
        lot_size: LotSize,
    ) -> Result<Self, YahooPublicationBridgeError> {
        if !matches!(event, MarketEvent::Quote(_)) {
            return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
        }
        Ok(Self {
            response_observation_ordinal,
            instrument,
            event,
            tick_size,
            lot_size,
        })
    }
}

#[derive(Debug)]
pub struct YahooQuotePublicationRequest {
    rows: Vec<YahooQuoteCanonicalRow>,
}

impl YahooQuotePublicationRequest {
    pub fn new(rows: Vec<YahooQuoteCanonicalRow>) -> Self {
        Self { rows }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct YahooQuoteAbstention {
    response_observation_ordinal: usize,
    symbol: Option<String>,
}

impl YahooQuoteAbstention {
    /// Ordinal in the typed response, including synthetic entries for requested-but-missing symbols.
    pub const fn response_observation_ordinal(&self) -> usize {
        self.response_observation_ordinal
    }

    pub fn symbol(&self) -> Option<&str> {
        self.symbol.as_deref()
    }
}

#[derive(Debug)]
pub enum YahooQuotePublicationOutcome {
    Published(YahooSealedQuotePublication),
    SealedRaw {
        response: YahooSealedPublication,
        abstentions: Box<[YahooQuoteAbstention]>,
    },
}

#[derive(Debug)]
pub struct YahooSealedQuotePublication {
    binding: SealedProviderResponseMarketEventBinding,
    abstentions: Box<[YahooQuoteAbstention]>,
}

impl YahooSealedQuotePublication {
    pub const fn binding(&self) -> &SealedProviderResponseMarketEventBinding {
        &self.binding
    }

    pub const fn abstentions(&self) -> &[YahooQuoteAbstention] {
        &self.abstentions
    }

    pub fn into_binding(self) -> SealedProviderResponseMarketEventBinding {
        self.binding
    }
}

impl YahooSealedPublication {
    /// Consumes a sealed Yahoo quote response into the shared live market-event family.
    pub fn into_quote_publication(
        self,
        request: YahooQuotePublicationRequest,
    ) -> Result<YahooQuotePublicationOutcome, YahooPublicationBridgeError> {
        if self.family() != YahooSealedPublicationFamily::CurrentQuotes {
            return Err(YahooPublicationBridgeError::InvalidCanonicalRequest);
        }
        let YahooParsedResponse::Quote(values) = self.parsed_response() else {
            return Err(YahooPublicationBridgeError::InvalidCanonicalRequest);
        };
        let raw = self.raw_receipt();
        let publication_binding = self.publication_binding();
        let mut supplied = BTreeMap::new();
        for row in request.rows {
            let key = (
                row.response_observation_ordinal,
                row.instrument.symbol.clone(),
            );
            if supplied.insert(key, row).is_some() {
                return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
            }
        }
        let mut events = Vec::new();
        let mut native_rows = Vec::new();
        let mut abstentions = Vec::new();
        for (ordinal, enrichment) in values.observations.iter().enumerate() {
            let symbol = enrichment.data.as_ref().map(|quote| quote.symbol.clone());
            let Some(symbol) = symbol else {
                abstentions.push(YahooQuoteAbstention {
                    response_observation_ordinal: ordinal,
                    symbol: None,
                });
                continue;
            };
            let Some(row) = supplied.remove(&(ordinal, symbol.clone())) else {
                abstentions.push(YahooQuoteAbstention {
                    response_observation_ordinal: ordinal,
                    symbol: Some(symbol.as_str().to_owned()),
                });
                continue;
            };
            validate_quote_row(
                raw,
                publication_binding,
                enrichment,
                &row.instrument,
                &row.event,
                row.tick_size,
                row.lot_size,
            )?;
            native_rows.push(Bytes::from(
                serde_json::to_vec(&YahooQuoteNativeRowV1 {
                    version: 1,
                    response_observation_ordinal: ordinal,
                    symbol: symbol.as_str(),
                    canonical_authority: &row.instrument,
                    enrichment,
                })
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
            ));
            events.push(row.event);
        }
        if !supplied.is_empty() {
            return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
        }
        if events.is_empty() {
            return Ok(YahooQuotePublicationOutcome::SealedRaw {
                response: self,
                abstentions: abstentions.into_boxed_slice(),
            });
        }
        let sidecar = Bytes::from(
            serde_json::to_vec(&YahooQuoteNativeSidecarV1 {
                version: 1,
                request: &raw.request,
                request_identity_sha256_hex: &raw.request_identity_sha256_hex,
                response_sha256_hex: &raw.response_sha256_hex,
                attempts: &raw.attempts,
                requested_symbols: &values.requested_symbols,
                missing_symbols: &values.missing_symbols,
                rejected_symbols: &values.rejected_symbols,
                abstentions: &abstentions,
            })
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
        );
        let (_, token, _, _, binding) = self.into_parts();
        let batch = ProviderMarketEventBatch::try_new(
            binding.source_id().clone(),
            binding.metadata_revision().clone(),
            token.persisted_receipt().capture().dataset().clone(),
            events,
        )?;
        let native = ProviderMarketEventNativeLineageBatch::try_new(
            ProviderNativeLineageImplementation::YahooEnrichmentV1,
            &batch,
            native_rows,
            Some(sidecar),
        )?;
        let row_count = batch.events().len();
        let sealed = SealedProviderResponseMarketEventBinding::try_new(
            token,
            batch,
            native,
            vec![0; row_count],
        )?;
        sealed.validate()?;
        Ok(YahooQuotePublicationOutcome::Published(
            YahooSealedQuotePublication {
                binding: sealed,
                abstentions: abstentions.into_boxed_slice(),
            },
        ))
    }
}

fn validate_quote_row(
    raw: &YahooRawReceipt,
    publication: &YahooPublicationBinding,
    enrichment: &YahooEnrichment<YahooQuote>,
    authority: &YahooCanonicalInstrumentAuthority,
    event: &MarketEvent,
    tick_size: TickSize,
    lot_size: LotSize,
) -> Result<(), YahooPublicationBridgeError> {
    let MarketEvent::Quote(quote_event) = event else {
        return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
    };
    let native = enrichment
        .data
        .as_ref()
        .ok_or(YahooPublicationBridgeError::InvalidCanonicalAuthority)?;
    let provenance = quote_event.provenance();
    let binding = provenance.binding();
    let source_at = provider_seconds(&native.regular_market_time_unix_seconds)?;
    let received_at = timestamp_from_millis(raw.received_at_unix_ms)?;
    let available_at = timestamp_from_millis(raw.available_at_unix_ms)?;
    let bid = yahoo_quote_side(&native.bid, &native.bid_size, tick_size, lot_size)?;
    let ask = yahoo_quote_side(&native.ask, &native.ask_size, tick_size, lot_size)?;
    let currency_matches = match &native.currency {
        ProviderField::Value(currency) => {
            authority.currency
                == Some(
                    Currency::try_from(currency.as_str())
                        .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalAuthority)?,
                )
        }
        ProviderField::Missing | ProviderField::Null => true,
        ProviderField::Invalid => false,
    };
    if native.symbol != authority.symbol
        || provenance.instrument_id() != Some(authority.instrument_id)
        || provenance.venue_id() != authority.venue_id.as_ref()
        || !currency_matches
        || binding.source_id() != publication.source_id()
        || binding.metadata_revision() != publication.metadata_revision()
        || binding.event_class() != market_squawk_domain::LiveEventClass::Quote
        || binding.payload_digest() != digest_from_hex(&raw.response_sha256_hex)?
        || provenance.source_timestamp() != source_at
        || provenance.received_at() != received_at
        || provenance.available_at() != available_at
        || provenance.recorded_quality() != DataQuality::Indicative
        || quote_event.bid() != bid
        || quote_event.ask() != ask
    {
        return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
    }
    Ok(())
}

fn yahoo_quote_side(
    price: &ProviderField<Decimal>,
    size: &ProviderField<u64>,
    tick_size: TickSize,
    lot_size: LotSize,
) -> Result<Option<BookLevel>, YahooPublicationBridgeError> {
    match (price, size) {
        (ProviderField::Value(price), ProviderField::Value(size)) => {
            let price = PriceTicks::try_from_decimal(*price, tick_size)
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalAuthority)?;
            let quantity = QuantityLots::try_from_decimal(Decimal::from(*size), lot_size)
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalAuthority)?;
            if quantity.get() == 0 {
                return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
            }
            BookLevel::new(price, quantity)
                .map(Some)
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalAuthority)
        }
        (ProviderField::Value(_), _) | (_, ProviderField::Value(_)) => {
            Err(YahooPublicationBridgeError::InvalidCanonicalAuthority)
        }
        _ => Ok(None),
    }
}

/// Caller-resolved option row aligned to one exact Yahoo contract ordinal.
#[derive(Debug)]
pub struct YahooOptionCanonicalRow {
    provider_record_ordinal: usize,
    observation: OptionSnapshotObservation,
    mapping_evidence: EvidenceDigest,
}

impl YahooOptionCanonicalRow {
    pub fn try_new(
        provider_record_ordinal: usize,
        observation: OptionSnapshotObservation,
        mapping_evidence: EvidenceDigest,
    ) -> Result<Self, YahooPublicationBridgeError> {
        if mapping_evidence.algorithm() != DigestAlgorithm::Sha256
            || mapping_evidence.bytes() == [0; 32]
        {
            return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
        }
        Ok(Self {
            provider_record_ordinal,
            observation,
            mapping_evidence,
        })
    }
}

#[derive(Debug)]
pub struct YahooOptionPublicationRequest {
    scope: OptionMarketRequestScope,
    rows: Vec<YahooOptionCanonicalRow>,
}

impl YahooOptionPublicationRequest {
    pub fn new(scope: OptionMarketRequestScope, rows: Vec<YahooOptionCanonicalRow>) -> Self {
        Self { scope, rows }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct YahooOptionAbstention {
    provider_record_ordinal: usize,
    contract_symbol: String,
}

impl YahooOptionAbstention {
    pub const fn provider_record_ordinal(&self) -> usize {
        self.provider_record_ordinal
    }

    pub fn contract_symbol(&self) -> &str {
        &self.contract_symbol
    }
}

#[derive(Debug)]
pub enum YahooOptionPublicationOutcome {
    Published(YahooSealedOptionPublication),
    SealedRaw {
        response: YahooSealedPublication,
        abstentions: Box<[YahooOptionAbstention]>,
    },
}

#[derive(Debug)]
pub struct YahooSealedOptionPublication {
    revision_plan: ExtractionRevisionPlan,
    binding: SealedProviderOptionMarketBinding,
    abstentions: Box<[YahooOptionAbstention]>,
}

impl YahooSealedOptionPublication {
    pub const fn revision_plan(&self) -> &ExtractionRevisionPlan {
        &self.revision_plan
    }

    pub const fn binding(&self) -> &SealedProviderOptionMarketBinding {
        &self.binding
    }

    pub const fn abstentions(&self) -> &[YahooOptionAbstention] {
        &self.abstentions
    }

    pub fn into_parts(self) -> (ExtractionRevisionPlan, SealedProviderOptionMarketBinding) {
        (self.revision_plan, self.binding)
    }
}

impl YahooSealedPublication {
    /// Consumes a sealed Yahoo option response into the shared option snapshot family.
    pub fn into_option_publication(
        self,
        request: YahooOptionPublicationRequest,
    ) -> Result<YahooOptionPublicationOutcome, YahooPublicationBridgeError> {
        if self.family() != YahooSealedPublicationFamily::Options {
            return Err(YahooPublicationBridgeError::InvalidCanonicalRequest);
        }
        let YahooParsedResponse::OptionChain(enrichment) = self.parsed_response() else {
            return Err(YahooPublicationBridgeError::InvalidCanonicalRequest);
        };
        let chain = enrichment
            .data
            .as_ref()
            .ok_or(YahooPublicationBridgeError::EmptyCanonicalOutput)?;
        validate_option_scope(
            self.raw_receipt(),
            self.publication_binding(),
            self.sealed_capture_receipt(),
            chain,
            &request.scope,
        )?;
        let mut supplied = BTreeMap::new();
        for row in request.rows {
            if supplied.insert(row.provider_record_ordinal, row).is_some() {
                return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
            }
        }
        let mut rows = Vec::new();
        let mut native_rows = Vec::new();
        let mut abstentions = Vec::new();
        let raw_evidence = digest_from_hex(&self.raw_receipt().response_sha256_hex)?;
        for (ordinal, contract) in chain.contracts.iter().enumerate() {
            let Some(row) = supplied.remove(&ordinal) else {
                abstentions.push(YahooOptionAbstention {
                    provider_record_ordinal: ordinal,
                    contract_symbol: contract.contract_symbol.as_str().to_owned(),
                });
                continue;
            };
            validate_option_row(chain, contract, &request.scope, raw_evidence, &row)?;
            native_rows.push(Bytes::from(
                serde_json::to_vec(&YahooOptionNativeRowV1 {
                    version: 1,
                    provider_record_ordinal: ordinal,
                    mapping_evidence: row.mapping_evidence,
                    contract,
                })
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
            ));
            rows.push(row.observation);
        }
        if !supplied.is_empty() {
            return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
        }
        if rows.is_empty() {
            return Ok(YahooOptionPublicationOutcome::SealedRaw {
                response: self,
                abstentions: abstentions.into_boxed_slice(),
            });
        }
        let returned = u64::try_from(rows.len())
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let expected = u64::try_from(chain.contracts.len())
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let missing = expected
            .checked_sub(returned)
            .ok_or(YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let completeness = OptionMarketCompleteness::try_new(OptionMarketCompletenessInput {
            expected_records: Some(expected),
            returned_records: returned,
            missing_records: missing,
            unexpected_records: 0,
            provider_reported_records: Some(expected),
            page_count: NonZeroU16::MIN,
            cursor: OptionMarketCursorState::NotApplicable,
            disposition: if missing == 0 {
                OptionMarketBatchDisposition::Complete
            } else {
                OptionMarketBatchDisposition::Unavailable
            },
        })?;
        let sidecar = Bytes::from(
            serde_json::to_vec(&YahooOptionNativeSidecarV1 {
                version: 1,
                request: &self.raw_receipt().request,
                request_identity_sha256_hex: &self.raw_receipt().request_identity_sha256_hex,
                response_sha256_hex: &self.raw_receipt().response_sha256_hex,
                attempts: &self.raw_receipt().attempts,
                chain,
                abstentions: &abstentions,
            })
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
        );
        let batch = ProviderOptionMarketBatch::try_snapshots(request.scope, completeness, rows)?;
        let native = ProviderOptionMarketNativeLineageBatch::try_new(
            ProviderNativeLineageImplementation::YahooEnrichmentV1,
            &batch,
            native_rows,
            sidecar,
        )?;
        let row_count = batch.row_count();
        let revision_plan = ExtractionRevisionPlan::locally_observed_with_native_lineage(row_count)
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let (_, token, _, _, _) = self.into_parts();
        let sealed =
            SealedProviderOptionMarketBinding::try_new(token, batch, native, vec![0; row_count])?;
        sealed.validate()?;
        Ok(YahooOptionPublicationOutcome::Published(
            YahooSealedOptionPublication {
                revision_plan,
                binding: sealed,
                abstentions: abstentions.into_boxed_slice(),
            },
        ))
    }
}

fn validate_option_scope(
    raw: &YahooRawReceipt,
    publication: &YahooPublicationBinding,
    capture: &market_squawk_sources::SealedProviderCaptureSetReceipt,
    chain: &YahooOptionChain,
    scope: &OptionMarketRequestScope,
) -> Result<(), YahooPublicationBridgeError> {
    let capture_request = digest_from_hex(&raw.request_identity_sha256_hex)?;
    let received_at = timestamp_from_millis(raw.received_at_unix_ms)?;
    let available_at = timestamp_from_millis(raw.available_at_unix_ms)?;
    if scope.source_id() != publication.source_id()
        || scope.metadata_revision() != publication.metadata_revision()
        || scope.dataset().as_str() != super::http::dataset_identity(raw.request_family)
        || scope.request_identity() != capture_request
        || scope.observation_identity() != capture.capture().observation_digest()
        || scope.received_at() != received_at
        || scope.available_at() != available_at
        || scope.ingested_at() < available_at
        || scope.provider_instrument_id().as_str() != chain.underlying_symbol.as_str()
    {
        return Err(YahooPublicationBridgeError::InvalidCanonicalRequest);
    }
    Ok(())
}

fn validate_option_row(
    chain: &YahooOptionChain,
    contract: &YahooOptionContract,
    scope: &OptionMarketRequestScope,
    raw_evidence: EvidenceDigest,
    row: &YahooOptionCanonicalRow,
) -> Result<(), YahooPublicationBridgeError> {
    let terms = row.observation.terms();
    let expiration_seconds = match &chain.returned_expiration_unix_seconds {
        ProviderField::Value(value) => *value,
        ProviderField::Missing | ProviderField::Null | ProviderField::Invalid => {
            return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
        }
    };
    let expiration = calendar_date_from_seconds(expiration_seconds)?;
    let expected_kind = match contract.side {
        YahooOptionSide::Call => OptionKind::Call,
        YahooOptionSide::Put => OptionKind::Put,
    };
    let ProviderField::Value(strike) = &contract.strike else {
        return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
    };
    if terms.underlying_instrument_id() != scope.underlying_instrument_id()
        || terms.underlying_definition_revision() != scope.underlying_definition_revision()
        || terms.provider_instrument_id().as_str() != contract.contract_symbol.as_str()
        || terms.expiration() != expiration
        || terms.kind() != expected_kind
        || terms.strike().amount() != *strike
        || !money_component_matches(
            &contract.bid,
            row.observation.bid_price(),
            terms.strike().currency(),
            None,
        )?
        || !money_component_matches(
            &contract.ask,
            row.observation.ask_price(),
            terms.strike().currency(),
            None,
        )?
        || !money_component_matches(
            &contract.last_price,
            row.observation.last_price(),
            terms.strike().currency(),
            provider_seconds(&contract.last_trade_time_unix_seconds)?,
        )?
        || !u64_component_matches(&contract.volume, row.observation.volume())
        || !u64_component_matches(&contract.open_interest, row.observation.open_interest())
        || !decimal_component_matches(
            &contract.implied_volatility,
            row.observation.implied_volatility(),
        )
        || !component_is_absent(row.observation.bid_size())
        || !component_is_absent(row.observation.ask_size())
        || !component_is_absent(row.observation.last_size())
        || !component_is_absent(row.observation.mark_price())
        || !component_is_absent(row.observation.trade_conditions())
        || !component_is_absent(row.observation.delta())
        || !component_is_absent(row.observation.gamma())
        || !component_is_absent(row.observation.theta())
        || !component_is_absent(row.observation.vega())
        || !component_is_absent(row.observation.rho())
        || row.observation.underlying().evidence() != raw_evidence
        || !underlying_component_matches(
            &chain.underlying_quote,
            row.observation.underlying().price(),
            terms.strike().currency(),
        )?
    {
        return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
    }
    if let ProviderField::Value(currency) = &contract.currency
        && Currency::try_from(currency.as_str())
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalAuthority)?
            != terms.strike().currency()
    {
        return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
    }
    Ok(())
}

fn money_component_matches(
    native: &ProviderField<Decimal>,
    canonical: &OptionComponent<Money>,
    currency: Currency,
    source_at: Option<Timestamp>,
) -> Result<bool, YahooPublicationBridgeError> {
    Ok(match native {
        ProviderField::Value(value) => {
            canonical.value() == Some(&Money::new(*value, currency))
                && canonical.source_at() == source_at
        }
        ProviderField::Missing => {
            component_unavailable(canonical, OptionComponentState::ProviderAbsent)
        }
        ProviderField::Null => component_unavailable(canonical, OptionComponentState::ProviderNull),
        ProviderField::Invalid => component_unavailable(canonical, OptionComponentState::Invalid),
    })
}

fn u64_component_matches(native: &ProviderField<u64>, canonical: &OptionComponent<u64>) -> bool {
    match native {
        ProviderField::Value(value) => {
            canonical.value() == Some(value) && canonical.source_at().is_none()
        }
        ProviderField::Missing => {
            component_unavailable(canonical, OptionComponentState::ProviderAbsent)
        }
        ProviderField::Null => component_unavailable(canonical, OptionComponentState::ProviderNull),
        ProviderField::Invalid => component_unavailable(canonical, OptionComponentState::Invalid),
    }
}

fn decimal_component_matches(
    native: &ProviderField<Decimal>,
    canonical: &OptionComponent<Decimal>,
) -> bool {
    match native {
        ProviderField::Value(value) => {
            canonical.value() == Some(value) && canonical.source_at().is_none()
        }
        ProviderField::Missing => {
            component_unavailable(canonical, OptionComponentState::ProviderAbsent)
        }
        ProviderField::Null => component_unavailable(canonical, OptionComponentState::ProviderNull),
        ProviderField::Invalid => component_unavailable(canonical, OptionComponentState::Invalid),
    }
}

fn component_unavailable<T>(component: &OptionComponent<T>, reason: OptionComponentState) -> bool {
    component.unavailable_reason() == Some(reason) && component.source_at().is_none()
}

fn component_is_absent<T>(component: &OptionComponent<T>) -> bool {
    component_unavailable(component, OptionComponentState::ProviderAbsent)
}

fn underlying_component_matches(
    native: &ProviderField<YahooQuote>,
    canonical: &OptionComponent<Money>,
    currency: Currency,
) -> Result<bool, YahooPublicationBridgeError> {
    Ok(match native {
        ProviderField::Value(quote) => {
            if let ProviderField::Value(native_currency) = &quote.currency
                && Currency::try_from(native_currency.as_str())
                    .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalAuthority)?
                    != currency
            {
                return Ok(false);
            }
            money_component_matches(
                &quote.regular_market_price,
                canonical,
                currency,
                provider_seconds(&quote.regular_market_time_unix_seconds)?,
            )?
        }
        ProviderField::Missing => {
            component_unavailable(canonical, OptionComponentState::ProviderAbsent)
        }
        ProviderField::Null => component_unavailable(canonical, OptionComponentState::ProviderNull),
        ProviderField::Invalid => component_unavailable(canonical, OptionComponentState::Invalid),
    })
}

fn provider_seconds(
    field: &ProviderField<i64>,
) -> Result<Option<Timestamp>, YahooPublicationBridgeError> {
    match field {
        ProviderField::Value(seconds) => seconds
            .checked_mul(1_000_000_000)
            .map(Timestamp::from_unix_nanos)
            .map(Some)
            .ok_or(YahooPublicationBridgeError::InvalidTimestamp),
        ProviderField::Missing | ProviderField::Null | ProviderField::Invalid => Ok(None),
    }
}

fn calendar_date_from_seconds(value: i64) -> Result<CalendarDate, YahooPublicationBridgeError> {
    let date = DateTime::<Utc>::from_timestamp(value, 0)
        .ok_or(YahooPublicationBridgeError::InvalidTimestamp)?
        .date_naive();
    CalendarDate::new(
        u16::try_from(date.year()).map_err(|_| YahooPublicationBridgeError::InvalidTimestamp)?,
        u8::try_from(date.month()).map_err(|_| YahooPublicationBridgeError::InvalidTimestamp)?,
        u8::try_from(date.day()).map_err(|_| YahooPublicationBridgeError::InvalidTimestamp)?,
    )
    .map_err(|_| YahooPublicationBridgeError::InvalidTimestamp)
}

fn identifier(value: &str) -> Result<SourceIdentifier, YahooPublicationBridgeError> {
    SourceIdentifier::try_from(value.to_owned())
        .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)
}

fn timestamp_from_millis(value: i64) -> Result<Timestamp, YahooPublicationBridgeError> {
    value
        .checked_mul(1_000_000)
        .map(Timestamp::from_unix_nanos)
        .ok_or(YahooPublicationBridgeError::InvalidTimestamp)
}

fn digest_from_hex(value: &str) -> Result<EvidenceDigest, YahooPublicationBridgeError> {
    if value.len() != 64 {
        return Err(YahooPublicationBridgeError::InvalidDigest);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0]).ok_or(YahooPublicationBridgeError::InvalidDigest)?
            << 4)
            | hex_nibble(pair[1]).ok_or(YahooPublicationBridgeError::InvalidDigest)?;
    }
    if bytes == [0; 32] {
        return Err(YahooPublicationBridgeError::InvalidDigest);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn lower_hex(bytes: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[derive(Debug, Serialize)]
struct YahooHistoricalNativeRowV1 {
    version: u16,
    provider_record_ordinal: usize,
    provider_symbol: String,
    canonical_field: SourceIdentifier,
    native_value: Value,
}

#[derive(Debug, Serialize)]
struct YahooHistoricalNativeAuthorityV1 {
    symbol: String,
    instrument_id: InstrumentId,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: Option<VenueId>,
    currency: Option<Currency>,
    mapping_revision: MetadataRevision,
    mapping_evidence: EvidenceDigest,
}

impl From<&YahooCanonicalInstrumentAuthority> for YahooHistoricalNativeAuthorityV1 {
    fn from(value: &YahooCanonicalInstrumentAuthority) -> Self {
        Self {
            symbol: value.symbol.as_str().to_owned(),
            instrument_id: value.instrument_id,
            provider_instrument_id: value.provider_instrument_id.clone(),
            venue_id: value.venue_id.clone(),
            currency: value.currency,
            mapping_revision: value.mapping_revision.clone(),
            mapping_evidence: value.mapping_evidence,
        }
    }
}

#[derive(Serialize)]
struct YahooHistoricalNativeSidecarV1<'a> {
    version: u16,
    authority: &'static str,
    governed_override_permitted: bool,
    request: &'a crate::YahooHttpRequest,
    request_identity_sha256_hex: &'a str,
    response_sha256_hex: &'a str,
    attempts: &'a [YahooHttpAttemptReceipt],
    chart: &'a YahooEnrichment<YahooChart>,
    chart_request_evidence: &'a YahooChartRequestEvidence,
    canonical_authority: &'a YahooHistoricalNativeAuthorityV1,
    chart_time_semantics: &'a [BarTimeSemantics],
}

fn historical_native_lineage(
    raw: &YahooRawReceipt,
    chart: &YahooEnrichment<YahooChart>,
    native_evidence: &YahooNativePublicationEvidence,
    batch: &ExtractionBatch,
    native_rows: &[YahooHistoricalNativeRowV1],
    authority: &YahooHistoricalNativeAuthorityV1,
    chart_time_semantics: &[BarTimeSemantics],
) -> Result<ProviderNativeLineageBatch, YahooPublicationBridgeError> {
    if native_rows.len() != batch.records().len() {
        return Err(YahooPublicationBridgeError::InvalidCanonicalOutput);
    }
    let request_evidence = native_evidence
        .chart_request_evidence()
        .ok_or(YahooPublicationBridgeError::InvalidCanonicalOutput)?;
    let mut builder = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::YahooEnrichmentV1,
        batch,
    )
    .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
    builder
        .try_set_batch_sidecar(&YahooHistoricalNativeSidecarV1 {
            version: 1,
            authority: "experimental-supplement-only",
            governed_override_permitted: false,
            request: &raw.request,
            request_identity_sha256_hex: &raw.request_identity_sha256_hex,
            response_sha256_hex: &raw.response_sha256_hex,
            attempts: &raw.attempts,
            chart,
            chart_request_evidence: request_evidence,
            canonical_authority: authority,
            chart_time_semantics,
        })
        .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
    for row in native_rows {
        builder
            .try_push(row)
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
    }
    builder
        .finish()
        .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)
}

#[derive(Serialize)]
struct YahooQuoteNativeRowV1<'a> {
    version: u16,
    response_observation_ordinal: usize,
    symbol: &'a str,
    canonical_authority: &'a YahooCanonicalInstrumentAuthority,
    enrichment: &'a YahooEnrichment<YahooQuote>,
}

#[derive(Serialize)]
struct YahooQuoteNativeSidecarV1<'a> {
    version: u16,
    request: &'a crate::YahooHttpRequest,
    request_identity_sha256_hex: &'a str,
    response_sha256_hex: &'a str,
    attempts: &'a [YahooHttpAttemptReceipt],
    requested_symbols: &'a [YahooSymbol],
    missing_symbols: &'a [YahooSymbol],
    rejected_symbols: &'a [YahooSymbol],
    abstentions: &'a [YahooQuoteAbstention],
}

#[derive(Serialize)]
struct YahooOptionNativeRowV1<'a> {
    version: u16,
    provider_record_ordinal: usize,
    mapping_evidence: EvidenceDigest,
    contract: &'a YahooOptionContract,
}

#[derive(Serialize)]
struct YahooOptionNativeSidecarV1<'a> {
    version: u16,
    request: &'a crate::YahooHttpRequest,
    request_identity_sha256_hex: &'a str,
    response_sha256_hex: &'a str,
    attempts: &'a [YahooHttpAttemptReceipt],
    chain: &'a YahooOptionChain,
    abstentions: &'a [YahooOptionAbstention],
}
