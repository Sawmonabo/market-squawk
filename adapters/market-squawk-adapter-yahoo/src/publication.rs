//! Seal-first canonical publication for explicitly requested Yahoo enrichment.
//!
//! Yahoo remains indicative supplement evidence. This boundary consumes an exact application
//! extraction request and externally resolved instrument/time authority, aligns every emitted
//! canonical row with bounded provider-native semantics, and withholds the resulting batch until
//! the matching raw response has crossed the common physical seal.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use market_squawk_domain::{
    AlternativeDataObservation, BarTimeSemantics, Currency, DataQuality, DigestAlgorithm,
    EvidenceDigest, ExactPayloadEvidence, InstrumentId, MarketBarAdjustment, MarketBarObservation,
    MetadataRevision, Money, PayloadHash, PayloadReference, ProviderInstrumentId, ResearchContext,
    ResearchObservation, ResearchProvenance, ResearchProvenanceInput, ResearchTime, RevisionNumber,
    SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, ExtractionBatch,
    ExtractionBatchAccumulator, ExtractionRecord, ExtractionRequest, ExtractionRevisionPlan,
    ProviderCaptureSetReceipt, ProviderNativeLineageBatch, ProviderNativeLineageBatchBuilder,
    ProviderNativeLineageImplementation, ProviderWholeCaptureToken, SealedProviderCaptureBinding,
};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::native::{YahooChartRequestEvidence, YahooNativePublicationEvidence};
use crate::{
    EvidenceAuthority, ProviderField, YahooBar, YahooEnrichment, YahooFundData,
    YahooHttpAttemptReceipt, YahooLookupHint, YahooOptionChain, YahooOptionContract,
    YahooParsedResponse, YahooPublicationBinding, YahooPublicationBridgeError, YahooQuote,
    YahooRawReceipt, YahooReference, YahooRequestFamily, YahooReturnedDisposition, YahooSymbol,
};

const YAHOO_CANONICAL_MEDIA_TYPE: &str = "application-json";
const YAHOO_CANONICAL_FEED: &str = "yahoo-finance-experimental-chart";
const YAHOO_CANONICAL_REVISION_PREFIX: &str = "yahoo-local";

/// Externally resolved canonical identity used only to scope Yahoo enrichment.
///
/// The value does not make Yahoo authoritative. It proves that one provider symbol was resolved
/// before this adapter emits instrument-scoped research rows.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    /// Constructs one exact Yahoo-symbol-to-canonical-instrument mapping.
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

/// Complete application-owned semantic input for one Yahoo response publication.
#[derive(Debug)]
pub struct YahooCanonicalPublicationRequest {
    extraction_request: ExtractionRequest,
    instruments: BTreeMap<YahooSymbol, YahooCanonicalInstrumentAuthority>,
    chart_time_semantics: Vec<BarTimeSemantics>,
    ingested_at: Timestamp,
}

impl YahooCanonicalPublicationRequest {
    /// Builds a bounded publication request. Response-specific validation occurs when it is
    /// consumed together with the exact network result.
    pub fn try_new(
        extraction_request: ExtractionRequest,
        instruments: Vec<YahooCanonicalInstrumentAuthority>,
        chart_time_semantics: Vec<BarTimeSemantics>,
        ingested_at: Timestamp,
    ) -> Result<Self, YahooPublicationBridgeError> {
        let mut mapped = BTreeMap::new();
        for authority in instruments {
            if mapped.insert(authority.symbol.clone(), authority).is_some() {
                return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
            }
        }
        Ok(Self {
            extraction_request,
            instruments: mapped,
            chart_time_semantics,
            ingested_at,
        })
    }

    pub const fn extraction_request(&self) -> &ExtractionRequest {
        &self.extraction_request
    }

    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }
}

/// Complete non-cloneable Yahoo raw/canonical/native handoff for shared publication.
#[derive(Debug)]
pub struct YahooSealedPublication {
    revision_plan: ExtractionRevisionPlan,
    sealed_capture_binding: SealedProviderCaptureBinding,
}

impl YahooSealedPublication {
    /// Yahoo is structurally retained only as experimental supplemental evidence.
    pub const fn authority(&self) -> EvidenceAuthority {
        EvidenceAuthority::ExperimentalSupplementOnly
    }

    /// Yahoo evidence cannot replace a governed observation by itself.
    pub const fn governed_override_permitted(&self) -> bool {
        false
    }

    pub const fn revision_plan(&self) -> &ExtractionRevisionPlan {
        &self.revision_plan
    }

    pub const fn sealed_capture_binding(&self) -> &SealedProviderCaptureBinding {
        &self.sealed_capture_binding
    }

    /// Consumes the candidate into the exact shared one-shot publication inputs.
    pub fn into_parts(self) -> (ExtractionRevisionPlan, SealedProviderCaptureBinding) {
        (self.revision_plan, self.sealed_capture_binding)
    }
}

/// Private canonical/native state held behind the physical-seal continuation.
#[derive(Debug)]
pub(crate) struct YahooPreparedCanonicalPublication {
    batch: ExtractionBatch,
    native_lineage: ProviderNativeLineageBatch,
    revision_plan: ExtractionRevisionPlan,
    row_capture_page_ordinals: Vec<u16>,
}

impl YahooPreparedCanonicalPublication {
    pub(crate) fn try_new(
        raw: &YahooRawReceipt,
        parsed: &YahooParsedResponse,
        native_evidence: &YahooNativePublicationEvidence,
        binding: &YahooPublicationBinding,
        request: YahooCanonicalPublicationRequest,
        capture: &ProviderCaptureSetReceipt,
    ) -> Result<Self, YahooPublicationBridgeError> {
        validate_publication_request(raw, binding, &request, capture)?;
        let canonical = canonical_batch(raw, parsed, native_evidence, request)?;
        let batch = canonical
            .batch
            .try_bind_provider_capture(capture)
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let native_lineage = native_lineage(
            raw,
            parsed,
            native_evidence,
            &batch,
            &canonical.native_rows,
            &canonical.authorities,
            &canonical.chart_time_semantics,
        )?;
        let revision_plan =
            ExtractionRevisionPlan::locally_observed_with_native_lineage(batch.records().len())
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        let row_capture_page_ordinals = vec![0; batch.records().len()];
        Ok(Self {
            batch,
            native_lineage,
            revision_plan,
            row_capture_page_ordinals,
        })
    }

    pub(crate) fn record_count(&self) -> usize {
        self.batch.records().len()
    }

    pub(crate) fn finish(
        self,
        token: ProviderWholeCaptureToken,
    ) -> Result<YahooSealedPublication, YahooPublicationBridgeError> {
        let sealed_capture_binding = SealedProviderCaptureBinding::try_whole(
            token,
            self.batch,
            self.native_lineage,
            self.row_capture_page_ordinals,
        )
        .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        sealed_capture_binding
            .validate()
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        Ok(YahooSealedPublication {
            revision_plan: self.revision_plan,
            sealed_capture_binding,
        })
    }
}

fn validate_publication_request(
    raw: &YahooRawReceipt,
    binding: &YahooPublicationBinding,
    request: &YahooCanonicalPublicationRequest,
    capture: &ProviderCaptureSetReceipt,
) -> Result<(), YahooPublicationBridgeError> {
    let object = request.extraction_request.object();
    let received_at = timestamp_from_millis(raw.received_at_unix_ms)?;
    let available_at = timestamp_from_millis(raw.available_at_unix_ms)?;
    let body_bytes = u64::try_from(raw.response_bytes.len())
        .map_err(|_| YahooPublicationBridgeError::InvalidBodyLength)?;
    let body_digest = digest_from_hex(&raw.response_sha256_hex)?;
    if object.source_id() != binding.source_id()
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

struct CanonicalBuild {
    batch: ExtractionBatch,
    native_rows: Vec<YahooNativeRowV1>,
    authorities: Vec<YahooNativeAuthorityV1>,
    chart_time_semantics: Vec<BarTimeSemantics>,
}

struct CanonicalAccumulator<'a> {
    request: &'a ExtractionRequest,
    raw: &'a YahooRawReceipt,
    ingested_at: Timestamp,
    payload_reference: PayloadReference,
    batch: ExtractionBatchAccumulator,
    native_rows: Vec<YahooNativeRowV1>,
}

impl<'a> CanonicalAccumulator<'a> {
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
        reason = "canonical and native row coordinates stay explicit"
    )]
    fn push_alternative(
        &mut self,
        family: YahooRequestFamily,
        provider_record_ordinal: usize,
        provider_symbol: Option<&YahooSymbol>,
        parsed_response_pointer: String,
        native_value: Value,
        field: SourceIdentifier,
        value: Decimal,
        unit: Option<SourceIdentifier>,
        authority: Option<&YahooCanonicalInstrumentAuthority>,
        source_timestamp: Option<Timestamp>,
    ) -> Result<(), YahooPublicationBridgeError> {
        let effective =
            source_timestamp.unwrap_or(timestamp_from_millis(self.raw.received_at_unix_ms)?);
        let observation = ResearchObservation::AlternativeData(AlternativeDataObservation::new(
            self.context(authority, source_timestamp, effective)?,
            self.request.object().dataset().clone(),
            field.clone(),
            value,
            unit,
        ));
        self.push_observation(
            family,
            provider_record_ordinal,
            provider_symbol,
            parsed_response_pointer,
            native_value,
            field,
            effective,
            observation,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "canonical and native row coordinates stay explicit"
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
        let context = self.context(
            Some(authority),
            Some(provider_timestamp),
            provider_timestamp,
        )?;
        if context.provenance().venue_id() != Some(venue) {
            return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
        }
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
        self.push_observation(
            YahooRequestFamily::ChartHistory,
            provider_record_ordinal,
            Some(symbol),
            format!("/payload/data/bars/{provider_record_ordinal}"),
            serde_json::to_value(native_bar)
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
            SourceIdentifier::try_from("yahoo.raw-ohlcv-bar")
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
            provider_timestamp,
            observation,
        )
    }

    fn context(
        &self,
        authority: Option<&YahooCanonicalInstrumentAuthority>,
        source_timestamp: Option<Timestamp>,
        effective: Timestamp,
    ) -> Result<ResearchContext, YahooPublicationBridgeError> {
        let received_at = timestamp_from_millis(self.raw.received_at_unix_ms)?;
        let available_at = timestamp_from_millis(self.raw.available_at_unix_ms)?;
        let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: self.request.object().source_id().clone(),
            instrument_id: authority.map(|value| value.instrument_id),
            venue_id: authority.and_then(|value| value.venue_id.clone()),
            source_identifier: SourceIdentifier::try_from(format!(
                "yahoo-observation-{}",
                self.native_rows.len()
            ))
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
            source_timestamp,
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
            effective,
            None,
            RevisionNumber::new(1)
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
            None,
        )
        .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        ResearchContext::new(provenance, time)
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "canonical and native row coordinates stay explicit"
    )]
    fn push_observation(
        &mut self,
        family: YahooRequestFamily,
        provider_record_ordinal: usize,
        provider_symbol: Option<&YahooSymbol>,
        parsed_response_pointer: String,
        native_value: Value,
        canonical_field: SourceIdentifier,
        effective: Timestamp,
        observation: ResearchObservation,
    ) -> Result<(), YahooPublicationBridgeError> {
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
                    effective,
                    None,
                    AvailabilityEvidence::LocalFirstObserved {
                        observed_at: timestamp_from_millis(self.raw.available_at_unix_ms)?,
                    },
                    revision,
                    None,
                    payload,
                )
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
            )
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
        self.native_rows.push(YahooNativeRowV1 {
            version: 1,
            family,
            provider_record_ordinal,
            provider_symbol: provider_symbol.map(|value| value.as_str().to_owned()),
            parsed_response_pointer,
            canonical_field,
            native_value,
        });
        Ok(())
    }

    fn finish(
        self,
    ) -> Result<(ExtractionBatch, Vec<YahooNativeRowV1>), YahooPublicationBridgeError> {
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

fn canonical_batch(
    raw: &YahooRawReceipt,
    parsed: &YahooParsedResponse,
    native_evidence: &YahooNativePublicationEvidence,
    request: YahooCanonicalPublicationRequest,
) -> Result<CanonicalBuild, YahooPublicationBridgeError> {
    validate_authority_coverage(raw, parsed, &request)?;
    let YahooCanonicalPublicationRequest {
        extraction_request,
        instruments,
        chart_time_semantics,
        ingested_at,
    } = request;
    let authorities = instruments
        .values()
        .map(YahooNativeAuthorityV1::from)
        .collect();
    let mut accumulator = CanonicalAccumulator::try_new(&extraction_request, raw, ingested_at)?;
    match parsed {
        YahooParsedResponse::Quote(values) => {
            map_quotes(&mut accumulator, values, &instruments)?;
        }
        YahooParsedResponse::Chart(value) => map_chart(
            &mut accumulator,
            value,
            native_evidence
                .chart_request_evidence()
                .ok_or(YahooPublicationBridgeError::InvalidCanonicalOutput)?,
            &instruments,
            &chart_time_semantics,
        )?,
        YahooParsedResponse::Reference(value) => {
            map_reference(&mut accumulator, value, &instruments)?;
        }
        YahooParsedResponse::Fund(value) => map_fund(&mut accumulator, value, &instruments)?,
        YahooParsedResponse::OptionChain(value) => {
            map_options(&mut accumulator, value, &instruments)?;
        }
        YahooParsedResponse::Lookup(values) => map_lookup(&mut accumulator, values)?,
    }
    let (batch, native_rows) = accumulator.finish()?;
    Ok(CanonicalBuild {
        batch,
        native_rows,
        authorities,
        chart_time_semantics,
    })
}

fn validate_authority_coverage(
    raw: &YahooRawReceipt,
    parsed: &YahooParsedResponse,
    request: &YahooCanonicalPublicationRequest,
) -> Result<(), YahooPublicationBridgeError> {
    let requested = raw
        .request
        .requested_targets
        .iter()
        .map(|target| target.symbol.clone())
        .collect::<BTreeSet<_>>();
    let supplied = request.instruments.keys().cloned().collect::<BTreeSet<_>>();
    let needs_instruments = !matches!(parsed, YahooParsedResponse::Lookup(_));
    if (needs_instruments && requested != supplied)
        || (!needs_instruments && !supplied.is_empty())
        || (matches!(parsed, YahooParsedResponse::Chart(_))
            != !request.chart_time_semantics.is_empty())
    {
        return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
    }
    Ok(())
}

fn map_quotes(
    output: &mut CanonicalAccumulator<'_>,
    values: &YahooReturnedDisposition<YahooQuote>,
    authorities: &BTreeMap<YahooSymbol, YahooCanonicalInstrumentAuthority>,
) -> Result<(), YahooPublicationBridgeError> {
    for (record_ordinal, enrichment) in values.observations.iter().enumerate() {
        let Some(quote) = enrichment.data.as_ref() else {
            continue;
        };
        let authority = authorities
            .get(&quote.symbol)
            .ok_or(YahooPublicationBridgeError::InvalidCanonicalAuthority)?;
        let regular_time = provider_seconds(&quote.regular_market_time_unix_seconds)?;
        let pre_time = provider_seconds(&quote.pre_market_time_unix_seconds)?;
        let post_time = provider_seconds(&quote.post_market_time_unix_seconds)?;
        let currency_unit = currency_unit(&quote.currency)?;
        for (name, provider_field, field, timestamp) in [
            (
                "regular-market-price",
                "regular_market_price",
                &quote.regular_market_price,
                regular_time,
            ),
            ("bid", "bid", &quote.bid, None),
            ("ask", "ask", &quote.ask, None),
            ("open", "open", &quote.open, regular_time),
            ("day-low", "day_low", &quote.day_low, regular_time),
            ("day-high", "day_high", &quote.day_high, regular_time),
            (
                "previous-close",
                "previous_close",
                &quote.previous_close,
                regular_time,
            ),
            (
                "pre-market-price",
                "pre_market_price",
                &quote.pre_market_price,
                pre_time,
            ),
            (
                "post-market-price",
                "post_market_price",
                &quote.post_market_price,
                post_time,
            ),
        ] {
            push_decimal_field(
                output,
                YahooRequestFamily::Quote,
                record_ordinal,
                Some(&quote.symbol),
                format!("/payload/observations/{record_ordinal}/data/{provider_field}"),
                name,
                field,
                currency_unit.clone(),
                Some(authority),
                timestamp,
            )?;
        }
        for (name, provider_field, field) in [
            ("bid-size", "bid_size", &quote.bid_size),
            ("ask-size", "ask_size", &quote.ask_size),
            ("volume", "volume", &quote.volume),
        ] {
            push_u64_field(
                output,
                YahooRequestFamily::Quote,
                record_ordinal,
                Some(&quote.symbol),
                format!("/payload/observations/{record_ordinal}/data/{provider_field}"),
                name,
                field,
                Some(identifier("shares")?),
                Some(authority),
                None,
            )?;
        }
    }
    Ok(())
}

fn map_chart(
    output: &mut CanonicalAccumulator<'_>,
    enrichment: &YahooEnrichment<crate::YahooChart>,
    request_evidence: &YahooChartRequestEvidence,
    authorities: &BTreeMap<YahooSymbol, YahooCanonicalInstrumentAuthority>,
    time_semantics: &[BarTimeSemantics],
) -> Result<(), YahooPublicationBridgeError> {
    let chart = enrichment
        .data
        .as_ref()
        .ok_or(YahooPublicationBridgeError::EmptyCanonicalOutput)?;
    let authority = authorities
        .get(&chart.symbol)
        .ok_or(YahooPublicationBridgeError::InvalidCanonicalAuthority)?;
    if chart.bars.len() != time_semantics.len() {
        return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
    }
    if let ProviderField::Value(provider_currency) = &chart.currency
        && Some(
            Currency::try_from(provider_currency.as_str())
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalAuthority)?,
        ) != authority.currency
    {
        return Err(YahooPublicationBridgeError::InvalidCanonicalAuthority);
    }
    let interval = identifier(&format!(
        "yahoo-interval-{}",
        request_evidence.interval().provider_value()
    ))?;
    for (record_ordinal, (bar, semantics)) in chart
        .bars
        .iter()
        .zip(time_semantics.iter().cloned())
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
            record_ordinal,
            &chart.symbol,
            bar,
            authority,
            interval.clone(),
            semantics,
            *open,
            *high,
            *low,
            *close,
            *volume,
        )?;
    }
    Ok(())
}

fn map_reference(
    output: &mut CanonicalAccumulator<'_>,
    enrichment: &YahooEnrichment<YahooReference>,
    authorities: &BTreeMap<YahooSymbol, YahooCanonicalInstrumentAuthority>,
) -> Result<(), YahooPublicationBridgeError> {
    let Some(reference) = enrichment.data.as_ref() else {
        return Ok(());
    };
    let authority = authorities
        .get(&reference.symbol)
        .ok_or(YahooPublicationBridgeError::InvalidCanonicalAuthority)?;
    let source_time = provider_seconds(&reference.regular_market_time_unix_seconds)?;
    let currency = currency_unit(&reference.currency)?;
    for (name, provider_field, field, unit) in [
        (
            "regular-market-price",
            "regular_market_price",
            &reference.regular_market_price,
            currency.clone(),
        ),
        (
            "nav-price",
            "nav_price",
            &reference.nav_price,
            currency.clone(),
        ),
        (
            "total-assets",
            "total_assets",
            &reference.total_assets,
            currency,
        ),
    ] {
        push_decimal_field(
            output,
            YahooRequestFamily::ReferenceSummary,
            0,
            Some(&reference.symbol),
            format!("/payload/data/{provider_field}"),
            name,
            field,
            unit,
            Some(authority),
            source_time,
        )?;
    }
    Ok(())
}

fn map_fund(
    output: &mut CanonicalAccumulator<'_>,
    enrichment: &YahooEnrichment<YahooFundData>,
    authorities: &BTreeMap<YahooSymbol, YahooCanonicalInstrumentAuthority>,
) -> Result<(), YahooPublicationBridgeError> {
    let Some(fund) = enrichment.data.as_ref() else {
        return Ok(());
    };
    let authority = authorities
        .get(&fund.symbol)
        .ok_or(YahooPublicationBridgeError::InvalidCanonicalAuthority)?;
    for (name, provider_field, field, unit) in [
        (
            "annual-report-expense-ratio",
            "annual_report_expense_ratio",
            &fund.annual_report_expense_ratio,
            Some(identifier("ratio")?),
        ),
        (
            "annual-holdings-turnover",
            "annual_holdings_turnover",
            &fund.annual_holdings_turnover,
            Some(identifier("ratio")?),
        ),
        (
            "total-net-assets",
            "total_net_assets",
            &fund.total_net_assets,
            None,
        ),
    ] {
        push_decimal_field(
            output,
            YahooRequestFamily::FundSummary,
            0,
            Some(&fund.symbol),
            format!("/payload/data/{provider_field}"),
            name,
            field,
            unit,
            Some(authority),
            None,
        )?;
    }
    for (group, provider_field, values) in [
        ("asset-class", "asset_classes", &fund.asset_classes),
        ("equity-metric", "equity_metrics", &fund.equity_metrics),
        ("bond-metric", "bond_metrics", &fund.bond_metrics),
        ("bond-rating", "bond_ratings", &fund.bond_ratings),
        (
            "sector-weighting",
            "sector_weightings",
            &fund.sector_weightings,
        ),
    ] {
        for (key, value) in values {
            let field_name = format!("{group}-{}", stable_key(key));
            push_decimal_field(
                output,
                YahooRequestFamily::FundSummary,
                0,
                Some(&fund.symbol),
                format!(
                    "/payload/data/{provider_field}/{}",
                    json_pointer_segment(key)
                ),
                &field_name,
                value,
                Some(identifier("ratio")?),
                Some(authority),
                None,
            )?;
        }
    }
    for (index, holding) in fund.top_holdings.iter().enumerate() {
        push_decimal_field(
            output,
            YahooRequestFamily::FundSummary,
            index,
            Some(&fund.symbol),
            format!("/payload/data/top_holdings/{index}/holding_percent"),
            &format!("top-holding-{index}-percent"),
            &holding.holding_percent,
            Some(identifier("ratio")?),
            Some(authority),
            None,
        )?;
    }
    Ok(())
}

fn map_options(
    output: &mut CanonicalAccumulator<'_>,
    enrichment: &YahooEnrichment<YahooOptionChain>,
    authorities: &BTreeMap<YahooSymbol, YahooCanonicalInstrumentAuthority>,
) -> Result<(), YahooPublicationBridgeError> {
    let Some(chain) = enrichment.data.as_ref() else {
        return Ok(());
    };
    let authority = authorities
        .get(&chain.underlying_symbol)
        .ok_or(YahooPublicationBridgeError::InvalidCanonicalAuthority)?;
    for (contract_ordinal, contract) in chain.contracts.iter().enumerate() {
        map_option_contract(output, contract_ordinal, contract, authority)?;
    }
    Ok(())
}

fn map_option_contract(
    output: &mut CanonicalAccumulator<'_>,
    ordinal: usize,
    contract: &YahooOptionContract,
    authority: &YahooCanonicalInstrumentAuthority,
) -> Result<(), YahooPublicationBridgeError> {
    let source_time = provider_seconds(&contract.last_trade_time_unix_seconds)?;
    let currency = currency_unit(&contract.currency)?;
    for (name, provider_field, field, unit, timestamp) in [
        ("strike", "strike", &contract.strike, currency.clone(), None),
        (
            "last-price",
            "last_price",
            &contract.last_price,
            currency.clone(),
            source_time,
        ),
        ("bid", "bid", &contract.bid, currency.clone(), None),
        ("ask", "ask", &contract.ask, currency.clone(), None),
        (
            "change",
            "change",
            &contract.change,
            currency.clone(),
            source_time,
        ),
        (
            "percent-change",
            "percent_change",
            &contract.percent_change,
            Some(identifier("ratio")?),
            source_time,
        ),
        (
            "implied-volatility",
            "implied_volatility",
            &contract.implied_volatility,
            Some(identifier("ratio")?),
            None,
        ),
    ] {
        push_decimal_field(
            output,
            YahooRequestFamily::OptionChain,
            ordinal,
            Some(&contract.contract_symbol),
            format!("/payload/data/contracts/{ordinal}/{provider_field}"),
            &format!("option-{ordinal}-{name}"),
            field,
            unit,
            Some(authority),
            timestamp,
        )?;
    }
    for (name, provider_field, field) in [
        ("volume", "volume", &contract.volume),
        ("open-interest", "open_interest", &contract.open_interest),
    ] {
        push_u64_field(
            output,
            YahooRequestFamily::OptionChain,
            ordinal,
            Some(&contract.contract_symbol),
            format!("/payload/data/contracts/{ordinal}/{provider_field}"),
            &format!("option-{ordinal}-{name}"),
            field,
            Some(identifier("contracts")?),
            Some(authority),
            None,
        )?;
    }
    push_bool_field(
        output,
        YahooRequestFamily::OptionChain,
        ordinal,
        Some(&contract.contract_symbol),
        format!("/payload/data/contracts/{ordinal}/in_the_money"),
        &format!("option-{ordinal}-in-the-money"),
        &contract.in_the_money,
        Some(authority),
    )
}

fn map_lookup(
    output: &mut CanonicalAccumulator<'_>,
    values: &YahooReturnedDisposition<YahooLookupHint>,
) -> Result<(), YahooPublicationBridgeError> {
    for (ordinal, enrichment) in values.observations.iter().enumerate() {
        let Some(hint) = enrichment.data.as_ref() else {
            continue;
        };
        push_decimal_field(
            output,
            output.raw.request_family,
            ordinal,
            Some(&hint.symbol),
            format!("/payload/observations/{ordinal}/data/score"),
            &format!("lookup-{ordinal}-score"),
            &hint.score,
            Some(identifier("provider-score")?),
            None,
            None,
        )?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "provider row and canonical coordinates stay explicit"
)]
fn push_decimal_field(
    output: &mut CanonicalAccumulator<'_>,
    family: YahooRequestFamily,
    record_ordinal: usize,
    symbol: Option<&YahooSymbol>,
    parsed_response_pointer: String,
    field_name: &str,
    field: &ProviderField<Decimal>,
    unit: Option<SourceIdentifier>,
    authority: Option<&YahooCanonicalInstrumentAuthority>,
    source_timestamp: Option<Timestamp>,
) -> Result<(), YahooPublicationBridgeError> {
    let ProviderField::Value(value) = field else {
        return Ok(());
    };
    output.push_alternative(
        family,
        record_ordinal,
        symbol,
        parsed_response_pointer,
        serde_json::to_value(field)
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
        identifier(&format!("yahoo.{field_name}"))?,
        *value,
        unit,
        authority,
        source_timestamp,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "provider row and canonical coordinates stay explicit"
)]
fn push_u64_field(
    output: &mut CanonicalAccumulator<'_>,
    family: YahooRequestFamily,
    record_ordinal: usize,
    symbol: Option<&YahooSymbol>,
    parsed_response_pointer: String,
    field_name: &str,
    field: &ProviderField<u64>,
    unit: Option<SourceIdentifier>,
    authority: Option<&YahooCanonicalInstrumentAuthority>,
    source_timestamp: Option<Timestamp>,
) -> Result<(), YahooPublicationBridgeError> {
    let ProviderField::Value(value) = field else {
        return Ok(());
    };
    output.push_alternative(
        family,
        record_ordinal,
        symbol,
        parsed_response_pointer,
        serde_json::to_value(field)
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
        identifier(&format!("yahoo.{field_name}"))?,
        Decimal::from(*value),
        unit,
        authority,
        source_timestamp,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "provider row and canonical coordinates stay explicit"
)]
fn push_bool_field(
    output: &mut CanonicalAccumulator<'_>,
    family: YahooRequestFamily,
    record_ordinal: usize,
    symbol: Option<&YahooSymbol>,
    parsed_response_pointer: String,
    field_name: &str,
    field: &ProviderField<bool>,
    authority: Option<&YahooCanonicalInstrumentAuthority>,
) -> Result<(), YahooPublicationBridgeError> {
    let ProviderField::Value(value) = field else {
        return Ok(());
    };
    output.push_alternative(
        family,
        record_ordinal,
        symbol,
        parsed_response_pointer,
        serde_json::to_value(field)
            .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?,
        identifier(&format!("yahoo.{field_name}"))?,
        if *value { Decimal::ONE } else { Decimal::ZERO },
        Some(identifier("boolean")?),
        authority,
        None,
    )
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

fn currency_unit(
    field: &ProviderField<String>,
) -> Result<Option<SourceIdentifier>, YahooPublicationBridgeError> {
    match field {
        ProviderField::Value(value) => {
            let currency = Currency::try_from(value.as_str())
                .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
            Ok(Some(identifier(&format!(
                "currency-{}",
                currency.as_str().to_ascii_lowercase()
            ))?))
        }
        ProviderField::Missing | ProviderField::Null | ProviderField::Invalid => Ok(None),
    }
}

fn identifier(value: &str) -> Result<SourceIdentifier, YahooPublicationBridgeError> {
    SourceIdentifier::try_from(value.to_owned())
        .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)
}

fn stable_key(value: &str) -> String {
    let digest: [u8; 32] = Sha256::digest(value.as_bytes()).into();
    lower_hex(digest)[..16].to_owned()
}

fn json_pointer_segment(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
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
#[serde(deny_unknown_fields)]
struct YahooNativeRowV1 {
    version: u16,
    family: YahooRequestFamily,
    provider_record_ordinal: usize,
    provider_symbol: Option<String>,
    /// RFC 6901 pointer into `YahooNativeSidecarV1.parsed_response`.
    parsed_response_pointer: String,
    canonical_field: SourceIdentifier,
    native_value: Value,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct YahooNativeAuthorityV1 {
    symbol: String,
    instrument_id: InstrumentId,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: Option<VenueId>,
    currency: Option<Currency>,
    mapping_revision: MetadataRevision,
    mapping_evidence: EvidenceDigest,
}

impl From<&YahooCanonicalInstrumentAuthority> for YahooNativeAuthorityV1 {
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
#[serde(deny_unknown_fields)]
struct YahooNativeSidecarV1<'a> {
    version: u16,
    authority: &'static str,
    governed_override_permitted: bool,
    pinned_client_version: &'static str,
    pinned_client_commit: &'static str,
    request_family: YahooRequestFamily,
    request: &'a crate::YahooHttpRequest,
    request_identity_sha256_hex: &'a str,
    response_status: u16,
    response_content_type: Option<&'a str>,
    response_sha256_hex: &'a str,
    response_bytes: usize,
    received_at_unix_ms: i64,
    available_at_unix_ms: i64,
    attempts: &'a [YahooHttpAttemptReceipt],
    parsed_response: &'a YahooParsedResponse,
    chart_request_evidence: Option<&'a YahooChartRequestEvidence>,
    canonical_authorities: &'a [YahooNativeAuthorityV1],
    chart_time_semantics: &'a [BarTimeSemantics],
}

fn native_lineage(
    raw: &YahooRawReceipt,
    parsed: &YahooParsedResponse,
    native_evidence: &YahooNativePublicationEvidence,
    batch: &ExtractionBatch,
    native_rows: &[YahooNativeRowV1],
    authorities: &[YahooNativeAuthorityV1],
    chart_time_semantics: &[BarTimeSemantics],
) -> Result<ProviderNativeLineageBatch, YahooPublicationBridgeError> {
    if native_rows.len() != batch.records().len() {
        return Err(YahooPublicationBridgeError::InvalidCanonicalOutput);
    }
    let mut builder = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::YahooEnrichmentV1,
        batch,
    )
    .map_err(|_| YahooPublicationBridgeError::InvalidCanonicalOutput)?;
    builder
        .try_set_batch_sidecar(&YahooNativeSidecarV1 {
            version: 1,
            authority: "experimental-supplement-only",
            governed_override_permitted: false,
            pinned_client_version: crate::PINNED_YFINANCE_VERSION,
            pinned_client_commit: crate::PINNED_YFINANCE_COMMIT,
            request_family: raw.request_family,
            request: &raw.request,
            request_identity_sha256_hex: &raw.request_identity_sha256_hex,
            response_status: raw.response_status,
            response_content_type: raw.response_content_type.as_deref(),
            response_sha256_hex: &raw.response_sha256_hex,
            response_bytes: raw.response_bytes.len(),
            received_at_unix_ms: raw.received_at_unix_ms,
            available_at_unix_ms: raw.available_at_unix_ms,
            attempts: &raw.attempts,
            parsed_response: parsed,
            chart_request_evidence: native_evidence.chart_request_evidence(),
            canonical_authorities: authorities,
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
