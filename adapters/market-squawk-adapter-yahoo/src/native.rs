//! Provider-native Yahoo response evidence retained until shared raw publication is sealed.
//!
//! This module deliberately does not assign canonical instrument, calendar, revision, point-in-
//! time, storage, or application authority. It validates the pinned Yahoo request shape and keeps
//! the provider's own fields—including null/missing distinctions—available to the later common
//! consuming publication boundary.

use std::collections::BTreeMap;
use std::fmt;

use market_squawk_domain::{MetadataRevision, SourceId};
use serde::Serialize;
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::{
    ChartInterval, ChartWindow, PINNED_YFINANCE_COMMIT, PINNED_YFINANCE_VERSION,
    YAHOO_FINANCE_EXPERIMENTAL, YahooChart, YahooEnrichment, YahooParsedResponse,
    YahooPublicationBinding, YahooRawReceipt, YahooRequestFamily,
};

/// Price fields requested and retained by the pinned chart request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum YahooChartAdjustmentMode {
    /// OHLCV remains provider-raw while adjusted close is retained as a separate provider field.
    RawOhlcvWithSeparateAdjustedClose,
}

/// Corporate-action families requested alongside the chart response.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum YahooChartActionScope {
    DividendsSplitsAndCapitalGains,
}

/// Provider request scope for extended-session rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum YahooChartSessionScope {
    RegularOnly,
    IncludePreAndPost,
}

/// Exact provider-request semantics retained beside one chart response.
///
/// This value is provider evidence, not exchange-calendar authority. Yahoo does not classify each
/// returned chart timestamp as pre-market, regular, or post-market in this response family.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct YahooChartRequestEvidence {
    interval: ChartInterval,
    window: ChartWindow,
    session_scope: YahooChartSessionScope,
    adjustment_mode: YahooChartAdjustmentMode,
    action_scope: YahooChartActionScope,
}

impl YahooChartRequestEvidence {
    pub const fn interval(&self) -> ChartInterval {
        self.interval
    }

    pub const fn window(&self) -> ChartWindow {
        self.window
    }

    pub const fn session_scope(&self) -> YahooChartSessionScope {
        self.session_scope
    }

    /// Yahoo's chart payload does not provide a trustworthy per-row session classification.
    pub const fn provider_classifies_each_bar_session(&self) -> bool {
        false
    }

    pub const fn adjustment_mode(&self) -> YahooChartAdjustmentMode {
        self.adjustment_mode
    }

    pub const fn action_scope(&self) -> YahooChartActionScope {
        self.action_scope
    }
}

/// Borrowed view of one noncloneable, unsealed Yahoo chart continuation.
///
/// The owning continuation must later be consumed with the common material-bound physical seal.
/// This view cannot mint, clone, serialize, or publish evidence.
pub struct YahooPendingChartHistory<'a> {
    binding: &'a YahooPublicationBinding,
    raw: &'a YahooRawReceipt,
    enrichment: &'a YahooEnrichment<YahooChart>,
    request_evidence: &'a YahooChartRequestEvidence,
}

impl fmt::Debug for YahooPendingChartHistory<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("YahooPendingChartHistory")
            .field("source_id", &self.binding.source_id())
            .field("metadata_revision", &self.binding.metadata_revision())
            .field(
                "request_identity_sha256_hex",
                &self.raw.request_identity_sha256_hex,
            )
            .field(
                "raw_body_identity_sha256_hex",
                &self.raw.response_sha256_hex,
            )
            .field("request_evidence", &self.request_evidence)
            .field("sealed_transition", &"PENDING")
            .finish()
    }
}

impl YahooPendingChartHistory<'_> {
    pub const fn source_id(&self) -> &SourceId {
        self.binding.source_id()
    }

    pub const fn metadata_revision(&self) -> &MetadataRevision {
        self.binding.metadata_revision()
    }

    pub const fn event_id(&self) -> Uuid {
        self.binding.event_id()
    }

    pub const fn connection_id(&self) -> Uuid {
        self.binding.connection_id()
    }

    pub fn request_identity_sha256_hex(&self) -> &str {
        &self.raw.request_identity_sha256_hex
    }

    pub fn raw_body_identity_sha256_hex(&self) -> &str {
        &self.raw.response_sha256_hex
    }

    pub const fn request_evidence(&self) -> &YahooChartRequestEvidence {
        self.request_evidence
    }

    /// Returns provider-native chart fields and missingness parsed from the exact retained body.
    pub const fn enrichment(&self) -> &YahooEnrichment<YahooChart> {
        self.enrichment
    }
}

pub(crate) enum YahooNativePublicationEvidence {
    Chart(YahooChartRequestEvidence),
    Other,
}

impl YahooNativePublicationEvidence {
    pub(crate) fn try_new(
        raw: &YahooRawReceipt,
        parsed: &YahooParsedResponse,
    ) -> Result<Self, YahooNativeEvidenceError> {
        validate_typed_family(raw.request_family, parsed)?;
        validate_schema_pin(raw)?;
        match parsed {
            YahooParsedResponse::Chart(enrichment) => {
                validate_chart_provenance(raw, enrichment)?;
                parse_chart_request_evidence(raw, enrichment).map(Self::Chart)
            }
            YahooParsedResponse::Quote(_)
            | YahooParsedResponse::Reference(_)
            | YahooParsedResponse::Fund(_)
            | YahooParsedResponse::OptionChain(_)
            | YahooParsedResponse::Lookup(_) => Ok(Self::Other),
        }
    }

    pub(crate) fn pending_chart_history<'a>(
        &'a self,
        binding: &'a YahooPublicationBinding,
        raw: &'a YahooRawReceipt,
        parsed: &'a YahooParsedResponse,
    ) -> Option<YahooPendingChartHistory<'a>> {
        let Self::Chart(request_evidence) = self else {
            return None;
        };
        let YahooParsedResponse::Chart(enrichment) = parsed else {
            return None;
        };
        Some(YahooPendingChartHistory {
            binding,
            raw,
            enrichment,
            request_evidence,
        })
    }

    pub(crate) const fn chart_request_evidence(&self) -> Option<&YahooChartRequestEvidence> {
        match self {
            Self::Chart(evidence) => Some(evidence),
            Self::Other => None,
        }
    }
}

fn validate_typed_family(
    family: YahooRequestFamily,
    parsed: &YahooParsedResponse,
) -> Result<(), YahooNativeEvidenceError> {
    let matches = matches!(
        (family, parsed),
        (YahooRequestFamily::Quote, YahooParsedResponse::Quote(_))
            | (
                YahooRequestFamily::ChartHistory,
                YahooParsedResponse::Chart(_)
            )
            | (
                YahooRequestFamily::ReferenceSummary,
                YahooParsedResponse::Reference(_)
            )
            | (
                YahooRequestFamily::FundSummary,
                YahooParsedResponse::Fund(_)
            )
            | (
                YahooRequestFamily::OptionChain,
                YahooParsedResponse::OptionChain(_)
            )
            | (
                YahooRequestFamily::Search | YahooRequestFamily::Lookup,
                YahooParsedResponse::Lookup(_)
            )
    );
    if matches {
        Ok(())
    } else {
        Err(YahooNativeEvidenceError::TypedFamilyMismatch)
    }
}

fn validate_schema_pin(raw: &YahooRawReceipt) -> Result<(), YahooNativeEvidenceError> {
    if raw.request.family != raw.request_family
        || raw.request.target != raw.request_target_without_crumb
        || raw.request.request_key != raw.request_target_without_crumb
        || raw.request.effective_arguments != raw.effective_arguments
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
        return Err(YahooNativeEvidenceError::SchemaPinMismatch);
    }
    Ok(())
}

fn validate_chart_provenance(
    raw: &YahooRawReceipt,
    enrichment: &YahooEnrichment<YahooChart>,
) -> Result<(), YahooNativeEvidenceError> {
    let provenance = &enrichment.provenance;
    if provenance.provider != YAHOO_FINANCE_EXPERIMENTAL
        || provenance.pinned_client_version != PINNED_YFINANCE_VERSION
        || provenance.pinned_client_commit != PINNED_YFINANCE_COMMIT
        || provenance.request_family != "chart-history"
        || provenance.request_target != raw.request_target_without_crumb
        || provenance.received_at_unix_ms != raw.received_at_unix_ms
        || provenance.available_at_unix_ms != raw.available_at_unix_ms
        || raw.received_at_unix_ms > raw.available_at_unix_ms
    {
        return Err(YahooNativeEvidenceError::ChartProvenanceMismatch);
    }
    Ok(())
}

fn parse_chart_request_evidence(
    raw: &YahooRawReceipt,
    enrichment: &YahooEnrichment<YahooChart>,
) -> Result<YahooChartRequestEvidence, YahooNativeEvidenceError> {
    let [target] = raw.request.requested_targets.as_slice() else {
        return Err(YahooNativeEvidenceError::ChartRequestMismatch);
    };
    if enrichment
        .data
        .as_ref()
        .is_some_and(|chart| chart.symbol != target.symbol)
    {
        return Err(YahooNativeEvidenceError::ChartRequestMismatch);
    }

    let url = Url::parse(&raw.request_target_without_crumb)
        .map_err(|_| YahooNativeEvidenceError::ChartRequestMismatch)?;
    if url.scheme() != "https"
        || url.host_str() != Some("query2.finance.yahoo.com")
        || url.fragment().is_some()
        || url.username() != ""
        || url.password().is_some()
    {
        return Err(YahooNativeEvidenceError::ChartRequestMismatch);
    }
    let mut expected_url = Url::parse("https://query2.finance.yahoo.com/v8/finance/chart/")
        .map_err(|_| YahooNativeEvidenceError::ChartRequestMismatch)?;
    expected_url
        .path_segments_mut()
        .map_err(|_| YahooNativeEvidenceError::ChartRequestMismatch)?
        .pop_if_empty()
        .push(target.symbol.as_str());
    if url.path() != expected_url.path() {
        return Err(YahooNativeEvidenceError::ChartRequestMismatch);
    }

    let mut parameters = BTreeMap::<String, Vec<String>>::new();
    for (key, value) in url.query_pairs() {
        parameters
            .entry(key.into_owned())
            .or_default()
            .push(value.into_owned());
    }
    let interval_text = take_exact(&mut parameters, "interval")?;
    let interval = ChartInterval::from_provider_value(&interval_text)
        .ok_or(YahooNativeEvidenceError::ChartRequestMismatch)?;
    let include_pre_post = take_exact(&mut parameters, "includePrePost")?;
    let session_scope = match include_pre_post.as_str() {
        "false" => YahooChartSessionScope::RegularOnly,
        "true" => YahooChartSessionScope::IncludePreAndPost,
        _ => return Err(YahooNativeEvidenceError::ChartRequestMismatch),
    };
    if take_exact(&mut parameters, "includeAdjustedClose")? != "true"
        || take_exact(&mut parameters, "events")? != "div,splits,capitalGains"
    {
        return Err(YahooNativeEvidenceError::ChartRequestMismatch);
    }
    let window = chart_window(&mut parameters)?;
    if !parameters.is_empty() {
        return Err(YahooNativeEvidenceError::ChartRequestMismatch);
    }

    let expected_arguments = BTreeMap::from([
        ("auto_adjust".to_owned(), "false".to_owned()),
        (
            "pinned_yfinance_commit".to_owned(),
            PINNED_YFINANCE_COMMIT.to_owned(),
        ),
        (
            "pinned_yfinance_version".to_owned(),
            PINNED_YFINANCE_VERSION.to_owned(),
        ),
        ("repair".to_owned(), "false".to_owned()),
        ("transient_retries".to_owned(), "0".to_owned()),
    ]);
    if raw.effective_arguments != expected_arguments {
        return Err(YahooNativeEvidenceError::SchemaPinMismatch);
    }

    if let Some(chart) = &enrichment.data {
        if matches!(
            &chart.data_granularity,
            crate::ProviderField::Value(value) if value != interval.provider_value()
        ) {
            return Err(YahooNativeEvidenceError::ChartResponseMismatch);
        }
        if let ChartWindow::UnixRange { .. } = window {
            // Yahoo may describe an explicit timestamp window with a provider-selected range label.
        } else if matches!(
            &chart.range,
            crate::ProviderField::Value(value)
                if Some(value.as_str()) != window.provider_range_value()
        ) {
            return Err(YahooNativeEvidenceError::ChartResponseMismatch);
        }
    }

    Ok(YahooChartRequestEvidence {
        interval,
        window,
        session_scope,
        adjustment_mode: YahooChartAdjustmentMode::RawOhlcvWithSeparateAdjustedClose,
        action_scope: YahooChartActionScope::DividendsSplitsAndCapitalGains,
    })
}

fn take_exact(
    parameters: &mut BTreeMap<String, Vec<String>>,
    key: &str,
) -> Result<String, YahooNativeEvidenceError> {
    let Some(values) = parameters.remove(key) else {
        return Err(YahooNativeEvidenceError::ChartRequestMismatch);
    };
    let [value] = values.as_slice() else {
        return Err(YahooNativeEvidenceError::ChartRequestMismatch);
    };
    Ok(value.clone())
}

fn chart_window(
    parameters: &mut BTreeMap<String, Vec<String>>,
) -> Result<ChartWindow, YahooNativeEvidenceError> {
    if parameters.contains_key("range") {
        if parameters.contains_key("period1") || parameters.contains_key("period2") {
            return Err(YahooNativeEvidenceError::ChartRequestMismatch);
        }
        return ChartWindow::from_provider_range(&take_exact(parameters, "range")?)
            .ok_or(YahooNativeEvidenceError::ChartRequestMismatch);
    }
    let start_unix_seconds = take_exact(parameters, "period1")?
        .parse::<i64>()
        .map_err(|_| YahooNativeEvidenceError::ChartRequestMismatch)?;
    let end_exclusive_unix_seconds = take_exact(parameters, "period2")?
        .parse::<i64>()
        .map_err(|_| YahooNativeEvidenceError::ChartRequestMismatch)?;
    if start_unix_seconds >= end_exclusive_unix_seconds {
        return Err(YahooNativeEvidenceError::ChartRequestMismatch);
    }
    Ok(ChartWindow::UnixRange {
        start_unix_seconds,
        end_exclusive_unix_seconds,
    })
}

/// Fail-closed errors at the Yahoo provider-native pending boundary.
#[derive(Debug, Error)]
pub enum YahooNativeEvidenceError {
    #[error("Yahoo request family does not match its typed parsed response")]
    TypedFamilyMismatch,
    #[error("Yahoo response does not retain the exact pinned client/request schema")]
    SchemaPinMismatch,
    #[error("Yahoo chart response provenance does not match its exact raw response")]
    ChartProvenanceMismatch,
    #[error("Yahoo chart request semantics are ambiguous or outside the pinned request shape")]
    ChartRequestMismatch,
    #[error("Yahoo chart response semantics contradict its exact request")]
    ChartResponseMismatch,
}
