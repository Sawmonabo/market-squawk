//! Strict canonical historical-bar mapping for sealed Yahoo chart responses.

use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailabilityEvidence, BarTimeSemantics, Currency, DataQuality,
    DigestAlgorithm, EvidenceDigest, InstrumentId, MarketBarAdjustment, MarketBarObservation,
    MetadataRevision, Money, PayloadHash, PayloadReference, ProviderInstrumentId, ResearchContext,
    ResearchObservation, ResearchProvenance, ResearchProvenanceInput, ResearchTime, RevisionNumber,
    SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    ExtractionRevisionPlan, ObservedRevisionError, ProviderCaptureTerminalDisposition,
    SealedProviderCaptureSetReceipt,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use url::Url;

use crate::{
    AdapterBounds, PINNED_YFINANCE_COMMIT, PINNED_YFINANCE_VERSION, ParseContext, ProviderField,
    QualityIssue, YAHOO_FINANCE_EXPERIMENTAL, YAHOO_SOURCE_ID, YahooBar, YahooChartEvent,
    YahooParsedResponse, YahooRequestFamily, YahooSealedPublication, YahooSymbol,
    parse_chart_response,
};

const YAHOO_CHART_FEED: &str = "yahoo-finance-experimental-chart";

/// Canonical identity and exact provider-to-venue mapping admitted for one Yahoo chart symbol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct YahooChartInstrumentAuthority {
    source_id: SourceId,
    source_contract_revision: MetadataRevision,
    instrument_id: InstrumentId,
    venue_id: VenueId,
    provider_instrument_id: ProviderInstrumentId,
    provider_symbol: YahooSymbol,
    provider_exchange: SourceIdentifier,
    currency: Currency,
}

impl YahooChartInstrumentAuthority {
    /// Constructs one exact identity mapping. Provider instrument identity must remain the exact
    /// Yahoo symbol; aliases and unresolved search hints cannot enter canonical history.
    #[allow(
        clippy::too_many_arguments,
        reason = "source, canonical identity, provider identity, venue, and currency stay explicit"
    )]
    pub fn try_new(
        source_id: SourceId,
        source_contract_revision: MetadataRevision,
        instrument_id: InstrumentId,
        venue_id: VenueId,
        provider_instrument_id: ProviderInstrumentId,
        provider_symbol: YahooSymbol,
        provider_exchange: SourceIdentifier,
        currency: Currency,
    ) -> Result<Self, YahooChartMapError> {
        if source_id.as_str() != YAHOO_SOURCE_ID
            || provider_instrument_id.as_str() != provider_symbol.as_str()
        {
            return Err(YahooChartMapError::AuthorityMismatch);
        }
        Ok(Self {
            source_id,
            source_contract_revision,
            instrument_id,
            venue_id,
            provider_instrument_id,
            provider_symbol,
            provider_exchange,
            currency,
        })
    }

    /// Returns the exact selected Yahoo source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact activated source-contract revision.
    pub const fn source_contract_revision(&self) -> &MetadataRevision {
        &self.source_contract_revision
    }
}

/// Least-authority request for one provider-authored bar timestamp.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct YahooHistoricalBarTimeRequest {
    instrument_id: InstrumentId,
    venue_id: VenueId,
    provider_instrument_id: ProviderInstrumentId,
    interval: SourceIdentifier,
    provider_timestamp: Timestamp,
}

impl YahooHistoricalBarTimeRequest {
    /// Returns the stable canonical instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact canonical venue identity.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the exact Yahoo provider symbol identity.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the exact provider interval retained by the chart response.
    pub const fn interval(&self) -> &SourceIdentifier {
        &self.interval
    }

    /// Returns the provider-authored timestamp without calendar rewriting.
    pub const fn provider_timestamp(&self) -> Timestamp {
        self.provider_timestamp
    }
}

/// Revocable authority for exact chart aggregation periods and venue-session evidence.
///
/// Yahoo timestamps alone do not establish exchange-calendar boundaries. The mapper therefore
/// cannot infer daily or intraday completion rules from an interval label.
pub trait YahooHistoricalBarTimeAuthority: Send + Sync {
    /// Rejects use after the independently governed session/calendar mapping is revoked.
    fn validate_current(&self) -> Result<(), YahooChartMapError>;

    /// Resolves one exact provider timestamp to validated period/session semantics.
    fn resolve(
        &self,
        request: &YahooHistoricalBarTimeRequest,
    ) -> Result<BarTimeSemantics, YahooChartMapError>;
}

/// Complete pure mapping input for one already sealed network-owned chart response.
pub struct YahooChartMappingInput<'a> {
    /// Exact typed network response consumed after sealing into the shared immutable journal.
    pub publication: YahooSealedPublication,
    /// Exact canonical/provider identity mapping for the requested symbol.
    pub instrument: &'a YahooChartInstrumentAuthority,
    /// Independent provider-calendar/session authority.
    pub bar_time_authority: &'a dyn YahooHistoricalBarTimeAuthority,
    /// Exact application bounds used to reparse the sealed response before canonical mapping.
    pub adapter_bounds: AdapterBounds,
    /// Nonzero placeholder replaced by shared observed-revision authority before publication.
    pub authority_seed_revision: RevisionNumber,
    /// Time canonical ingestion completed locally.
    pub ingested_at: Timestamp,
}

impl std::fmt::Debug for YahooChartMappingInput<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("YahooChartMappingInput")
            .field(
                "sealed_capture",
                &self.publication.sealed_capture().receipt_digest(),
            )
            .field("instrument", self.instrument)
            .field("bar_time_authority", &"[REVOCABLE AUTHORITY]")
            .field("adapter_bounds", &self.adapter_bounds)
            .field("authority_seed_revision", &self.authority_seed_revision)
            .field("ingested_at", &self.ingested_at)
            .finish()
    }
}

/// Validated canonical bars plus provider-native events/issues that remain supplemental evidence.
#[derive(Debug, Eq, PartialEq)]
pub struct YahooMappedChartHistory {
    observations: Vec<ResearchObservation>,
    revision_plan: ExtractionRevisionPlan,
    provider_events: Vec<YahooChartEvent>,
    quality_issues: Vec<QualityIssue>,
    sealed_capture: SealedProviderCaptureSetReceipt,
}

impl YahooMappedChartHistory {
    /// Consumes the complete provider-local handoff, including its actual physical raw seal.
    pub fn into_parts(
        self,
    ) -> (
        Vec<ResearchObservation>,
        ExtractionRevisionPlan,
        Vec<YahooChartEvent>,
        Vec<QualityIssue>,
        SealedProviderCaptureSetReceipt,
    ) {
        (
            self.observations,
            self.revision_plan,
            self.provider_events,
            self.quality_issues,
            self.sealed_capture,
        )
    }
}

/// Maps complete raw Yahoo OHLCV rows into canonical raw-price market bars.
///
/// Adjusted close is deliberately not substituted for raw close because the selected chart shape
/// does not prove adjusted OHLC. Provider-native action events remain attached to the mapping
/// handoff instead of being silently converted into canonical corporate actions.
pub fn map_chart_bars(
    input: YahooChartMappingInput<'_>,
) -> Result<YahooMappedChartHistory, YahooChartMapError> {
    let raw = input.publication.raw_receipt();
    let YahooParsedResponse::Chart(enrichment) = input.publication.parsed_response() else {
        return Err(YahooChartMapError::WrongResponseFamily);
    };
    let chart = enrichment
        .data
        .as_ref()
        .ok_or(YahooChartMapError::NoCanonicalBars)?;
    validate_capture(&input, raw)?;
    let reparsed = parse_chart_response(
        &raw.request,
        &ParseContext {
            received_at_unix_ms: raw.received_at_unix_ms,
            available_at_unix_ms: raw.available_at_unix_ms,
        },
        input.adapter_bounds,
        &raw.response_bytes,
    )
    .map_err(|_| YahooChartMapError::ParsedResponseMismatch)?;
    if &reparsed != enrichment {
        return Err(YahooChartMapError::ParsedResponseMismatch);
    }
    validate_chart_authority(&input, chart)?;
    let interval = chart_interval(raw, chart)?;
    let received_at = timestamp_from_millis(raw.received_at_unix_ms)?;
    let available_at = timestamp_from_millis(raw.available_at_unix_ms)?;
    if received_at > available_at || available_at > input.ingested_at {
        return Err(YahooChartMapError::InvalidChronology);
    }
    let raw_digest = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(&raw.response_bytes).into(),
    );
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(chart.bars.len())
        .map_err(|_| YahooChartMapError::Allocation)?;
    let mut previous_timestamp = None;
    for bar in &chart.bars {
        let provider_timestamp = timestamp_from_seconds(bar.timestamp_unix_seconds)?;
        if previous_timestamp.is_some_and(|previous| previous >= provider_timestamp) {
            return Err(YahooChartMapError::InvalidBarOrdering);
        }
        previous_timestamp = Some(provider_timestamp);
        observations.push(map_bar(
            &input,
            bar,
            &interval,
            provider_timestamp,
            received_at,
            available_at,
            raw_digest,
        )?);
    }
    if observations.is_empty() {
        return Err(YahooChartMapError::NoCanonicalBars);
    }
    let revision_plan = ExtractionRevisionPlan::locally_observed(observations.len())?;
    let provider_events = chart.events.clone();
    let quality_issues = enrichment.issues.clone();
    let sealed_capture = input.publication.into_sealed_capture();
    Ok(YahooMappedChartHistory {
        observations,
        revision_plan,
        provider_events,
        quality_issues,
        sealed_capture,
    })
}

#[allow(clippy::too_many_arguments)]
fn map_bar(
    input: &YahooChartMappingInput<'_>,
    bar: &YahooBar,
    interval: &SourceIdentifier,
    provider_timestamp: Timestamp,
    received_at: Timestamp,
    available_at: Timestamp,
    raw_digest: EvidenceDigest,
) -> Result<ResearchObservation, YahooChartMapError> {
    let (open, high, low, close, volume) = complete_raw_bar(bar)?;
    input.bar_time_authority.validate_current()?;
    let request = YahooHistoricalBarTimeRequest {
        instrument_id: input.instrument.instrument_id,
        venue_id: input.instrument.venue_id.clone(),
        provider_instrument_id: input.instrument.provider_instrument_id.clone(),
        interval: interval.clone(),
        provider_timestamp,
    };
    let time_semantics = input.bar_time_authority.resolve(&request)?;
    input.bar_time_authority.validate_current()?;
    if time_semantics.provider_timestamp() != provider_timestamp
        || time_semantics.period_end_exclusive() > available_at
    {
        return Err(YahooChartMapError::InvalidTimeAuthority);
    }
    let source_identifier = SourceIdentifier::try_from(format!(
        "yahoo-chart:{}:{}:{}",
        input.instrument.provider_symbol.as_str(),
        interval.as_str(),
        bar.timestamp_unix_seconds
    ))
    .map_err(|_| YahooChartMapError::InvalidCanonicalIdentity)?;
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: input.instrument.source_id.clone(),
        instrument_id: Some(input.instrument.instrument_id),
        venue_id: Some(input.instrument.venue_id.clone()),
        source_identifier,
        source_timestamp: Some(provider_timestamp),
        received_at,
        ingested_at: input.ingested_at,
        quality: DataQuality::Aggregated,
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            raw_digest.algorithm(),
            raw_digest.bytes(),
        )),
        availability: ResearchAvailabilityEvidence::local_first_observed(available_at),
    })
    .map_err(|_| YahooChartMapError::InvalidCanonicalEvidence)?;
    let time = ResearchTime::new(
        provider_timestamp,
        None,
        input.authority_seed_revision,
        None,
    )
    .map_err(|_| YahooChartMapError::InvalidCanonicalEvidence)?;
    let context = ResearchContext::new(provenance, time)
        .map_err(|_| YahooChartMapError::InvalidCanonicalEvidence)?;
    MarketBarObservation::new(
        context,
        input.instrument.provider_instrument_id.clone(),
        identifier(YAHOO_CHART_FEED)?,
        interval.clone(),
        time_semantics,
        MarketBarAdjustment::Raw,
        Money::new(open, input.instrument.currency),
        Money::new(high, input.instrument.currency),
        Money::new(low, input.instrument.currency),
        Money::new(close, input.instrument.currency),
        Decimal::from(volume),
        None,
        None,
    )
    .map(ResearchObservation::MarketBar)
    .map_err(|_| YahooChartMapError::InvalidCanonicalEvidence)
}

fn complete_raw_bar(
    bar: &YahooBar,
) -> Result<(Decimal, Decimal, Decimal, Decimal, u64), YahooChartMapError> {
    let (
        ProviderField::Value(open),
        ProviderField::Value(high),
        ProviderField::Value(low),
        ProviderField::Value(close),
        ProviderField::Value(volume),
    ) = (&bar.open, &bar.high, &bar.low, &bar.close, &bar.volume)
    else {
        return Err(YahooChartMapError::IncompleteBar {
            provider_timestamp_unix_seconds: bar.timestamp_unix_seconds,
        });
    };
    Ok((*open, *high, *low, *close, *volume))
}

fn validate_capture(
    input: &YahooChartMappingInput<'_>,
    raw: &crate::YahooRawReceipt,
) -> Result<(), YahooChartMapError> {
    let capture = input.publication.sealed_capture().capture();
    let Some(page) = capture.pages().first() else {
        return Err(YahooChartMapError::CaptureMismatch);
    };
    let request_identity = digest_from_hex(&raw.request_identity_sha256_hex)?;
    let body_digest = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(&raw.response_bytes).into(),
    );
    let received_at = timestamp_from_millis(raw.received_at_unix_ms)?;
    if raw.request_family != YahooRequestFamily::ChartHistory
        || raw.request.family != raw.request_family
        || raw.request.target != raw.request_target_without_crumb
        || raw.request.request_key != raw.request_target_without_crumb
        || raw.request.effective_arguments != raw.effective_arguments
        || crate::http::request_identity(&raw.request) != raw.request_identity_sha256_hex
        || capture.pages().len() != 1
        || capture.source_id() != &input.instrument.source_id
        || capture.metadata_revision() != &input.instrument.source_contract_revision
        || capture.dataset().as_str()
            != crate::http::dataset_identity(YahooRequestFamily::ChartHistory)
        || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        || capture.request_set_identity() != request_identity
        || capture.total_body_bytes() != u64::try_from(raw.response_bytes.len()).unwrap_or(u64::MAX)
        || page.request_identity() != request_identity
        || page.http_status() != raw.response_status
        || page.body_bytes() != u64::try_from(raw.response_bytes.len()).unwrap_or(u64::MAX)
        || page.body_digest() != body_digest
        || page.received_at() != received_at
        || raw.response_sha256_hex != lower_hex(body_digest.bytes())
    {
        return Err(YahooChartMapError::CaptureMismatch);
    }
    Ok(())
}

fn validate_chart_authority(
    input: &YahooChartMappingInput<'_>,
    chart: &crate::YahooChart,
) -> Result<(), YahooChartMapError> {
    let raw = input.publication.raw_receipt();
    let YahooParsedResponse::Chart(enrichment) = input.publication.parsed_response() else {
        return Err(YahooChartMapError::WrongResponseFamily);
    };
    let currency_matches = matches!(
        &chart.currency,
        ProviderField::Value(value) if value.eq_ignore_ascii_case(input.instrument.currency.as_str())
    );
    let provider_symbol_matches = matches!(
        &enrichment.provenance.provider_symbol,
        ProviderField::Value(value) if value == &input.instrument.provider_symbol
    );
    let exchange_matches = matches!(
        &enrichment.provenance.exchange,
        ProviderField::Value(value) if value == input.instrument.provider_exchange.as_str()
    );
    if chart.symbol != input.instrument.provider_symbol
        || enrichment.provenance.provider != YAHOO_FINANCE_EXPERIMENTAL
        || enrichment.provenance.pinned_client_version != PINNED_YFINANCE_VERSION
        || enrichment.provenance.pinned_client_commit != PINNED_YFINANCE_COMMIT
        || enrichment.provenance.request_target != raw.request_target_without_crumb
        || enrichment.provenance.received_at_unix_ms != raw.received_at_unix_ms
        || enrichment.provenance.available_at_unix_ms != raw.available_at_unix_ms
        || !provider_symbol_matches
        || !exchange_matches
        || !currency_matches
        || raw
            .effective_arguments
            .get("auto_adjust")
            .map(String::as_str)
            != Some("false")
        || raw.effective_arguments.get("repair").map(String::as_str) != Some("false")
        || raw
            .effective_arguments
            .get("transient_retries")
            .map(String::as_str)
            != Some("0")
        || raw
            .effective_arguments
            .get("pinned_yfinance_version")
            .map(String::as_str)
            != Some(PINNED_YFINANCE_VERSION)
        || raw
            .effective_arguments
            .get("pinned_yfinance_commit")
            .map(String::as_str)
            != Some(PINNED_YFINANCE_COMMIT)
    {
        return Err(YahooChartMapError::AuthorityMismatch);
    }
    Ok(())
}

fn chart_interval(
    raw: &crate::YahooRawReceipt,
    chart: &crate::YahooChart,
) -> Result<SourceIdentifier, YahooChartMapError> {
    let url = Url::parse(&raw.request_target_without_crumb)
        .map_err(|_| YahooChartMapError::AuthorityMismatch)?;
    let intervals = url
        .query_pairs()
        .filter(|(key, _)| key == "interval")
        .map(|(_, value)| value.into_owned())
        .collect::<Vec<_>>();
    let [interval] = intervals.as_slice() else {
        return Err(YahooChartMapError::AuthorityMismatch);
    };
    if !matches!(&chart.data_granularity, ProviderField::Value(value) if value == interval) {
        return Err(YahooChartMapError::AuthorityMismatch);
    }
    identifier(&format!("yahoo-chart-{interval}"))
}

fn timestamp_from_millis(value: i64) -> Result<Timestamp, YahooChartMapError> {
    value
        .checked_mul(1_000_000)
        .map(Timestamp::from_unix_nanos)
        .ok_or(YahooChartMapError::InvalidChronology)
}

fn timestamp_from_seconds(value: i64) -> Result<Timestamp, YahooChartMapError> {
    value
        .checked_mul(1_000_000_000)
        .map(Timestamp::from_unix_nanos)
        .ok_or(YahooChartMapError::InvalidChronology)
}

fn digest_from_hex(value: &str) -> Result<EvidenceDigest, YahooChartMapError> {
    if value.len() != 64 {
        return Err(YahooChartMapError::CaptureMismatch);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or(YahooChartMapError::CaptureMismatch)?;
        let low = hex_nibble(pair[1]).ok_or(YahooChartMapError::CaptureMismatch)?;
        bytes[index] = (high << 4) | low;
    }
    if bytes == [0; 32] {
        return Err(YahooChartMapError::CaptureMismatch);
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
    let mut value = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(value, "{byte:02x}");
    }
    value
}

fn identifier(value: &str) -> Result<SourceIdentifier, YahooChartMapError> {
    SourceIdentifier::try_from(value).map_err(|_| YahooChartMapError::InvalidCanonicalIdentity)
}

/// Fail-closed canonical Yahoo chart mapping errors.
#[derive(Debug, Error)]
pub enum YahooChartMapError {
    #[error("Yahoo canonical chart mapper received another response family")]
    WrongResponseFamily,
    #[error("sealed Yahoo capture does not match the chart response")]
    CaptureMismatch,
    #[error("typed Yahoo chart result does not match reparsing the exact sealed response")]
    ParsedResponseMismatch,
    #[error("Yahoo chart identity, exchange, currency, or request authority does not match")]
    AuthorityMismatch,
    #[error("Yahoo receive, availability, or ingest chronology is invalid")]
    InvalidChronology,
    #[error("Yahoo chart contains no canonical raw-price bars")]
    NoCanonicalBars,
    #[error("Yahoo chart bar at {provider_timestamp_unix_seconds} has incomplete raw OHLCV")]
    IncompleteBar {
        provider_timestamp_unix_seconds: i64,
    },
    #[error("Yahoo chart bar timestamps are not strictly increasing")]
    InvalidBarOrdering,
    #[error("Yahoo bar-time authority returned inconsistent period/session evidence")]
    InvalidTimeAuthority,
    #[error("Yahoo canonical identity is invalid")]
    InvalidCanonicalIdentity,
    #[error("Yahoo canonical provenance or bar invariants rejected the evidence")]
    InvalidCanonicalEvidence,
    #[error("Yahoo canonical bar allocation failed")]
    Allocation,
    #[error("Yahoo local-content revision evidence is invalid")]
    Revision(#[from] ObservedRevisionError),
}
