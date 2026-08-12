//! Bounded Yahoo Finance enrichment primitives pinned to the inspected yfinance lineage.
//!
//! Yahoo is an experimental, explicit-demand supplement. This crate deliberately contains no
//! scheduler, background scan, automatic retry loop, or governed-source selector. It builds the
//! selected provider-native requests, performs one bounded source-grounded cookie/crumb session,
//! parses bounded response bytes into typed evidence, and shares actual-attempt telemetry, cache,
//! coalescing, and circuit state across every clone in one process.

mod admission;
mod error;
mod http;
mod model;
mod parse;
mod request;

pub use admission::{
    AdmissionDecision, AdmissionPolicy, AdmissionRejection, AdmissionSnapshot, AttemptDisposition,
    AttemptKind, AttemptOutcome, AttemptPermit, CircuitSnapshot, YahooAdmission,
};
pub use error::YahooAdapterError;
pub use http::{
    YahooAttemptTarget, YahooExecutionDisposition, YahooExecutionLimits, YahooHttpAttemptReceipt,
    YahooHttpFailure, YahooHttpFailureKind, YahooHttpResult, YahooHttpSession,
    YahooHttpSessionConfig, YahooParsedResponse, YahooPublicationBinding,
    YahooPublicationBridgeError, YahooRawReceipt,
};
pub use model::{
    EvidenceAuthority, ExplicitDemand, ExplicitDemandPurpose, ParseContext, ProviderField,
    QualityIssue, YahooAssetClass, YahooBar, YahooChart, YahooChartEvent, YahooChartEventKind,
    YahooEnrichment, YahooEnrichmentState, YahooFundData, YahooFundHolding, YahooLookupHint,
    YahooOptionChain, YahooOptionContract, YahooOptionSide, YahooProvenance, YahooQuote,
    YahooReference, YahooReturnedDisposition, YahooSymbol, YahooTarget,
};
pub use parse::{
    parse_chart_response, parse_fund_response, parse_lookup_response, parse_option_response,
    parse_quote_response, parse_reference_response,
};
pub use request::{
    AdapterBounds, ChartInterval, ChartWindow, LookupKind, YahooHttpMethod, YahooHttpRequest,
    YahooLocale, YahooRequestFamily, YahooRequestPlan, YahooRequestPlanner,
};

/// Stable source identity carried by every parsed observation.
pub const YAHOO_FINANCE_EXPERIMENTAL: &str = "YAHOO_FINANCE_EXPERIMENTAL";

/// Pinned client release whose request behavior informed this adapter contract.
pub const PINNED_YFINANCE_VERSION: &str = "1.5.2";

/// Exact pinned yfinance source commit whose request behavior informed this adapter contract.
pub const PINNED_YFINANCE_COMMIT: &str = "beac22d981ab37362a70c9e4e49261ac622acbe4";

#[cfg(test)]
mod tests;
