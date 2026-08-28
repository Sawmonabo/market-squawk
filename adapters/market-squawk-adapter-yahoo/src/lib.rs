//! Bounded Yahoo Finance enrichment primitives pinned to the inspected yfinance lineage.
//!
//! Yahoo is an experimental, explicit-demand supplement. This crate deliberately contains no
//! scheduler, background scan, automatic retry loop, or governed-source selector. It builds the
//! selected provider-native requests, performs one bounded source-grounded cookie/crumb session,
//! parses bounded response bytes into typed evidence, and shares actual-attempt telemetry, cache,
//! coalescing, and circuit state across every clone in one process.

mod admission;
mod durable;
mod error;
mod http;
mod model;
mod native;
mod parse;
mod publication;
mod request;

pub use admission::{
    AdmissionDecision, AdmissionPolicy, AdmissionRejection, AdmissionSnapshot, AttemptDisposition,
    AttemptKind, AttemptOutcome, AttemptPermit, CircuitSnapshot,
    YAHOO_MISSING_RETRY_AFTER_COOLDOWN_FLOOR_MS, YahooAdmission,
};
pub use durable::{
    MAX_YAHOO_DURABLE_CACHE_BODY_BYTES, YahooDurableStateError, YahooDurableStateStore,
};
pub use error::YahooAdapterError;
pub use http::{
    YahooAttemptTarget, YahooExecutionDisposition, YahooExecutionLimits, YahooHttpAttemptReceipt,
    YahooHttpFailure, YahooHttpFailureKind, YahooHttpResult, YahooHttpSession,
    YahooHttpSessionConfig, YahooParsedResponse, YahooPendingPublication, YahooPublicationBinding,
    YahooPublicationBridgeError, YahooPublicationSealRejoin, YahooRawReceipt,
};
pub use model::{
    EvidenceAuthority, ExplicitDemand, ExplicitDemandPurpose, ParseContext, ProviderField,
    QualityIssue, YahooAssetClass, YahooBar, YahooChart, YahooChartActions, YahooChartEvent,
    YahooChartEventKind, YahooChartIndicatorContainers, YahooEnrichment, YahooEnrichmentState,
    YahooFundData, YahooFundHolding, YahooLookupHint, YahooOptionChain, YahooOptionContract,
    YahooOptionSide, YahooProvenance, YahooQuote, YahooReference, YahooReturnedDisposition,
    YahooSymbol, YahooTarget,
};
pub use native::{
    YahooChartActionScope, YahooChartAdjustmentMode, YahooChartRequestEvidence,
    YahooChartSessionScope, YahooNativeEvidenceError, YahooPendingChartHistory,
};
pub use parse::{
    parse_chart_response, parse_fund_response, parse_lookup_response, parse_option_response,
    parse_quote_response, parse_reference_response,
};
pub use publication::{
    YahooCanonicalInstrumentAuthority, YahooCanonicalPublicationRequest, YahooSealedPublication,
};
pub use request::{
    AdapterBounds, ChartInterval, ChartWindow, LookupKind, YahooHttpMethod, YahooHttpRequest,
    YahooLocale, YahooRequestFamily, YahooRequestPlan, YahooRequestPlanner,
};

/// Stable source identity carried by every parsed observation.
pub const YAHOO_FINANCE_EXPERIMENTAL: &str = "YAHOO_FINANCE_EXPERIMENTAL";

/// Exact shared source namespace admitted for Yahoo publication receipts.
pub const YAHOO_SOURCE_ID: &str = "yahoo-finance-experimental";

/// Pinned client release whose request behavior informed this adapter contract.
pub const PINNED_YFINANCE_VERSION: &str = "1.5.2";

/// Exact pinned yfinance source commit whose request behavior informed this adapter contract.
pub const PINNED_YFINANCE_COMMIT: &str = "beac22d981ab37362a70c9e4e49261ac622acbe4";

#[cfg(test)]
mod tests;
