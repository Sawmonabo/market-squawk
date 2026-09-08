//! Provider-neutral fund/share-class research for ordinary product consumers.
//!
//! The projection consumes only exact canonical point-in-time fund evidence and, when composed,
//! an exact latest-known daily NAV read. It never promotes a ticker or provider association to
//! identity, never substitutes market price for NAV, and never exposes filing coordinates,
//! provider fields, manifests, digests, or runtime state.

use std::io::{self, Write};

use market_squawk_data::{
    AnalyticalFundNavOutput, AnalyticalFundNavReadRequest, PointInTimeRevisionMode,
};
use market_squawk_domain::{
    CalendarDate, Currency, FundAmendmentState, FundConflictState, FundCurrencyAmount,
    FundHoldingQuantity, FundHoldingUnit, FundMissingState, FundNavCorrectionState,
    FundNavFinality, FundNavMissingState, FundNavObservation, FundNavValue, FundReportedDecimal,
    FundReportedValue, FundRevisionStatus, InstrumentId, Money, Timestamp,
};
use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

use super::company_product::ResearchProductIdentity;
use super::company_research::{
    FundResearchFamily, FundResearchOutcome, FundResearchRead, FundResearchSnapshot,
    FundResearchUnavailableReason,
};
use super::sec_fund_product::{
    FundHoldingData, FundResearchAvailability, FundResearchData, FundResearchFilingState,
};
use crate::application::domain_support::{ProductTextCopyError, try_boxed_product_text};

const MAX_FUND_PRODUCT_PROJECTED_BYTES: usize = 64 * 1024 * 1024;
const MAX_FUND_PRODUCT_SERIALIZED_BYTES: usize = MAX_FUND_PRODUCT_PROJECTED_BYTES;
const MAX_FUND_PRODUCT_DECIMAL_BYTES: usize = 128;

/// Exact typed NAV output whose complete point-in-time request is retained by the output itself.
#[derive(Clone, Copy)]
pub(crate) struct FundNavProductRead<'read> {
    output: &'read AnalyticalFundNavOutput,
}

impl<'read> FundNavProductRead<'read> {
    pub(crate) const fn new(output: &'read AnalyticalFundNavOutput) -> Self {
        Self { output }
    }
}

impl std::fmt::Debug for FundNavProductRead<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FundNavProductRead")
            .field("read", &"[PRIVATE CANONICAL NAV READ]")
            .finish()
    }
}

/// The two canonical report-family reads required by the fund product.
#[derive(Clone, Copy)]
pub(crate) struct FundProductReadSet<'read> {
    portfolio: &'read FundResearchRead,
    annual: &'read FundResearchRead,
}

impl<'read> FundProductReadSet<'read> {
    pub(crate) const fn new(
        portfolio: &'read FundResearchRead,
        annual: &'read FundResearchRead,
    ) -> Self {
        Self { portfolio, annual }
    }
}

impl std::fmt::Debug for FundProductReadSet<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FundProductReadSet")
            .field("reads", &"[PRIVATE CANONICAL FUND READS]")
            .finish()
    }
}

/// One closed fund/share-class result with no provider or storage vocabulary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundProductResult {
    #[serde(skip)]
    fund_share_class_instrument_id: InstrumentId,
    identity: Option<ResearchProductIdentity>,
    availability: FundProductAvailability,
    holdings: FundHoldingsProduct,
    annual_information: FundAnnualInformationProduct,
    current_research: FundCurrentResearchProduct,
    portfolio_analysis: FundPortfolioAnalysisProduct,
    clocks: FundProductClocks,
    coverage: FundProductCoverage,
    limitations: Box<[FundProductLimitation]>,
}

impl FundProductResult {
    fn bind_display_identities(
        &mut self,
        instrument_id: InstrumentId,
        identity: ResearchProductIdentity,
        holdings: Vec<Option<ResearchProductIdentity>>,
        budget: &mut FundProductByteBudget,
    ) -> Result<(), FundProductProjectionError> {
        if self.fund_share_class_instrument_id != instrument_id
            || self.identity.is_some()
            || self.holdings.items.len() != holdings.len()
        {
            return Err(FundProductProjectionError::InvalidEvidence);
        }
        for (holding, identity) in self.holdings.items.iter().zip(&holdings) {
            if holding.instrument_id.is_none() && identity.is_some() {
                return Err(FundProductProjectionError::InvalidEvidence);
            }
        }
        budget.charge_serialized(&identity)?;
        for identity in holdings.iter().flatten() {
            budget.charge_serialized(identity)?;
        }
        for (holding, identity) in self.holdings.items.iter_mut().zip(holdings) {
            holding.identity = identity;
        }
        self.identity = Some(identity);
        Ok(())
    }

    pub(crate) const fn availability(&self) -> FundProductAvailability {
        self.availability
    }

    pub(crate) const fn holdings(&self) -> &FundHoldingsProduct {
        &self.holdings
    }

    pub(crate) const fn annual_information(&self) -> &FundAnnualInformationProduct {
        &self.annual_information
    }

    pub(crate) const fn current_research(&self) -> &FundCurrentResearchProduct {
        &self.current_research
    }

    pub(crate) const fn portfolio_analysis(&self) -> &FundPortfolioAnalysisProduct {
        &self.portfolio_analysis
    }

    pub(crate) const fn clocks(&self) -> &FundProductClocks {
        &self.clocks
    }

    pub(crate) const fn coverage(&self) -> FundProductCoverage {
        self.coverage
    }

    pub(crate) fn limitations(&self) -> &[FundProductLimitation] {
        &self.limitations
    }

    pub(crate) fn compare_overlap(
        &self,
        other: &Self,
    ) -> Result<FundOverlapProduct, FundProductProjectionError> {
        compare_fund_overlap(self, other)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FundProductAvailability {
    Available,
    Partial,
    Missing,
    Conflict,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundHoldingsProduct {
    state: FundProductSectionState,
    filing: Option<FundFilingProduct>,
    items: Box<[FundHoldingProduct]>,
}

impl FundHoldingsProduct {
    pub(crate) const fn state(&self) -> FundProductSectionState {
        self.state
    }

    pub(crate) const fn filing(&self) -> Option<&FundFilingProduct> {
        self.filing.as_ref()
    }

    pub(crate) fn items(&self) -> &[FundHoldingProduct] {
        &self.items
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FundProductSectionState {
    Reported,
    Missing,
    Conflict,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundFilingProduct {
    report_period_end: FundProductValue<CalendarDate>,
    report_date: FundProductValue<CalendarDate>,
    filed_date: FundProductValue<CalendarDate>,
    accepted_at: FundProductValue<Timestamp>,
    available_at: Timestamp,
    amendment: FundProductAmendmentState,
    revision: FundProductRevisionState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FundProductAmendmentState {
    Original,
    Amendment,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FundProductRevisionState {
    Current,
    Superseded,
    Conflict,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundAnnualInformationProduct {
    state: FundProductSectionState,
    filing: Option<FundFilingProduct>,
    facts: Option<FundAnnualFactsProduct>,
}

impl FundAnnualInformationProduct {
    pub(crate) const fn state(&self) -> FundProductSectionState {
        self.state
    }

    pub(crate) const fn filing(&self) -> Option<&FundFilingProduct> {
        self.filing.as_ref()
    }

    pub(crate) const fn facts(&self) -> Option<&FundAnnualFactsProduct> {
        self.facts.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundAnnualFactsProduct {
    reporting_period_less_than_twelve_months: FundProductValue<bool>,
    reporting_currency: FundProductValue<Currency>,
    monthly_average_net_assets: FundProductValue<FundMoneyProduct>,
    daily_average_net_assets: FundProductValue<FundMoneyProduct>,
    is_etf: FundProductValue<bool>,
    is_index: FundProductValue<bool>,
    collateral_required: FundProductValue<bool>,
    shares_per_creation_unit: FundProductValue<Box<str>>,
    shares_per_redemption_unit: FundProductValue<Box<str>>,
    in_kind: FundProductValue<bool>,
}

/// One holding. `instrument_id` remains null unless an authoritative canonical mapping exists.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundHoldingProduct {
    #[serde(skip)]
    instrument_id: Option<InstrumentId>,
    identity: Option<ResearchProductIdentity>,
    #[serde(skip)]
    percentage_weight: Option<Decimal>,
    #[serde(skip)]
    percentage_weight_conflict: bool,
    quantity: FundProductValue<FundQuantityProduct>,
    value: FundProductValue<FundMoneyProduct>,
    percentage_of_net_assets: FundProductValue<Box<str>>,
}

impl FundHoldingProduct {
    pub(crate) const fn quantity(&self) -> &FundProductValue<FundQuantityProduct> {
        &self.quantity
    }

    pub(crate) const fn value(&self) -> &FundProductValue<FundMoneyProduct> {
        &self.value
    }

    pub(crate) const fn percentage_of_net_assets(&self) -> &FundProductValue<Box<str>> {
        &self.percentage_of_net_assets
    }
}

/// One exact product value, one plain missing reason, or one plain conflict reason.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub(crate) enum FundProductValue<T> {
    Reported(T),
    Missing(FundProductMissingReason),
    Conflict(FundProductConflictReason),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundQuantityProduct {
    amount: Box<str>,
    unit: FundQuantityProductUnit,
}

impl FundQuantityProduct {
    pub(crate) const fn amount(&self) -> &str {
        &self.amount
    }

    pub(crate) const fn unit(&self) -> FundQuantityProductUnit {
        self.unit
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FundQuantityProductUnit {
    Shares,
    Principal,
    Contracts,
    Currency(Currency),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundMoneyProduct {
    amount: Box<str>,
    currency: Currency,
}

impl FundMoneyProduct {
    pub(crate) const fn amount(&self) -> &str {
        &self.amount
    }

    pub(crate) const fn currency(&self) -> Currency {
        self.currency
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FundProductMissingReason {
    NotReported,
    NotApplicable,
    Withheld,
    Invalid,
    Unavailable,
    UnresolvedIdentity,
    CoverageLimited,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FundProductConflictReason {
    ConflictingReportedValues,
    ConflictingRevisions,
    ConflictingIdentity,
    IncompatibleUnits,
}

/// Current research is deliberately separate from holdings and never treats price as NAV.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundCurrentResearchProduct {
    net_asset_value: FundNavProduct,
}

impl FundCurrentResearchProduct {
    pub(crate) const fn net_asset_value(&self) -> &FundNavProduct {
        &self.net_asset_value
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum FundNavProduct {
    Reported {
        nav_date: CalendarDate,
        value: Money,
        available_at: Timestamp,
        correction: FundNavCorrectionState,
        finality: FundNavFinality,
    },
    Missing {
        nav_date: Option<CalendarDate>,
        reason: FundNavProductMissingReason,
    },
    Conflict {
        nav_date: CalendarDate,
    },
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FundNavProductMissingReason {
    NotYetPublished,
    Unsupported,
    NotReported,
    Invalid,
    Unavailable,
}

/// Reusable portfolio research derived only from exact point-in-time holdings.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundPortfolioAnalysisProduct {
    policy: FundAggregationPolicy,
    coverage: FundAggregationCoverageProduct,
    exposure: FundExposureProduct,
    concentration: FundConcentrationProduct,
}

impl FundPortfolioAnalysisProduct {
    pub(crate) const fn policy(&self) -> FundAggregationPolicy {
        self.policy
    }

    pub(crate) const fn coverage(&self) -> FundAggregationCoverageProduct {
        self.coverage
    }

    pub(crate) const fn exposure(&self) -> &FundExposureProduct {
        &self.exposure
    }

    pub(crate) const fn concentration(&self) -> &FundConcentrationProduct {
        &self.concentration
    }
}

/// V1 aggregation: retain raw signed/gross rows, net duplicate canonical instruments, then use
/// normalized absolute nonzero net-instrument exposure for concentration and overlap.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FundAggregationPolicy {
    NetInstrumentAbsoluteV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FundAnalysisState {
    Reported,
    Partial,
    Missing,
    Conflict,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundExposureProduct {
    state: FundAnalysisState,
    raw_net_weight: Option<Decimal>,
    raw_gross_weight: Option<Decimal>,
    identified_raw_gross_weight: Option<Decimal>,
    net_instrument_gross_weight: Option<Decimal>,
}

impl FundExposureProduct {
    pub(crate) const fn state(&self) -> FundAnalysisState {
        self.state
    }

    pub(crate) const fn raw_net_weight(&self) -> Option<Decimal> {
        self.raw_net_weight
    }

    pub(crate) const fn raw_gross_weight(&self) -> Option<Decimal> {
        self.raw_gross_weight
    }

    pub(crate) const fn net_instrument_gross_weight(&self) -> Option<Decimal> {
        self.net_instrument_gross_weight
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundAggregationCoverageProduct {
    raw_holdings: usize,
    weighted_raw_holdings: usize,
    identified_weighted_raw_holdings: usize,
    analyzed_net_instruments: usize,
    excluded_raw_holdings: usize,
    analyzed_absolute_net_weight: Option<Decimal>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundConcentrationProduct {
    state: FundAnalysisState,
    largest_position_share: Option<Decimal>,
    top_five_share: Option<Decimal>,
    top_ten_share: Option<Decimal>,
    herfindahl_index: Option<Decimal>,
}

impl FundConcentrationProduct {
    pub(crate) const fn state(&self) -> FundAnalysisState {
        self.state
    }

    pub(crate) const fn largest_position_share(&self) -> Option<Decimal> {
        self.largest_position_share
    }

    pub(crate) const fn herfindahl_index(&self) -> Option<Decimal> {
        self.herfindahl_index
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundEffectiveHoldingsPeriodProduct {
    report_period_end: FundProductValue<CalendarDate>,
    report_date: FundProductValue<CalendarDate>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FundOverlapAlignmentState {
    Aligned,
    Misaligned,
    Unavailable,
    Conflict,
}

/// Pairwise same-instrument overlap with honest incomplete-identity coverage.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundOverlapProduct {
    state: FundAnalysisState,
    policy: FundAggregationPolicy,
    alignment: FundOverlapAlignmentState,
    overlap_share: Option<Decimal>,
    matched_holdings: usize,
    left_effective_period: FundEffectiveHoldingsPeriodProduct,
    right_effective_period: FundEffectiveHoldingsPeriodProduct,
    left_coverage: FundAggregationCoverageProduct,
    right_coverage: FundAggregationCoverageProduct,
    knowledge_cutoff: Timestamp,
}

impl FundOverlapProduct {
    pub(crate) const fn state(&self) -> FundAnalysisState {
        self.state
    }

    pub(crate) const fn alignment(&self) -> FundOverlapAlignmentState {
        self.alignment
    }

    pub(crate) const fn overlap_share(&self) -> Option<Decimal> {
        self.overlap_share
    }

    pub(crate) const fn matched_holdings(&self) -> usize {
        self.matched_holdings
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundProductClocks {
    knowledge_cutoff: Timestamp,
    latest_fund_information_known_at: Option<Timestamp>,
}

impl FundProductClocks {
    pub(crate) const fn knowledge_cutoff(self) -> Timestamp {
        self.knowledge_cutoff
    }

    pub(crate) const fn latest_fund_information_known_at(self) -> Option<Timestamp> {
        self.latest_fund_information_known_at
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FundProductCoverage {
    reports: usize,
    share_class_records: usize,
    holdings: usize,
    identified_holdings: usize,
    reported_quantities: usize,
    reported_values: usize,
    reported_weights: usize,
    missing_fields: usize,
    conflicting_fields: usize,
    other_quantity_units: usize,
    annual_reported_fields: usize,
    annual_missing_fields: usize,
    annual_conflicting_fields: usize,
    annual_limited_fields: usize,
    filing_conflicting_fields: usize,
    filing_limited_fields: usize,
}

impl FundProductCoverage {
    pub(crate) const fn reports(self) -> usize {
        self.reports
    }

    pub(crate) const fn share_class_records(self) -> usize {
        self.share_class_records
    }

    pub(crate) const fn holdings(self) -> usize {
        self.holdings
    }

    pub(crate) const fn identified_holdings(self) -> usize {
        self.identified_holdings
    }

    pub(crate) const fn reported_quantities(self) -> usize {
        self.reported_quantities
    }

    pub(crate) const fn reported_values(self) -> usize {
        self.reported_values
    }

    pub(crate) const fn reported_weights(self) -> usize {
        self.reported_weights
    }

    pub(crate) const fn missing_fields(self) -> usize {
        self.missing_fields
    }

    pub(crate) const fn conflicting_fields(self) -> usize {
        self.conflicting_fields
    }

    pub(crate) const fn other_quantity_units(self) -> usize {
        self.other_quantity_units
    }

    pub(crate) const fn annual_reported_fields(self) -> usize {
        self.annual_reported_fields
    }

    pub(crate) const fn annual_missing_fields(self) -> usize {
        self.annual_missing_fields
    }

    pub(crate) const fn annual_conflicting_fields(self) -> usize {
        self.annual_conflicting_fields
    }

    pub(crate) const fn annual_limited_fields(self) -> usize {
        self.annual_limited_fields
    }

    pub(crate) const fn filing_conflicting_fields(self) -> usize {
        self.filing_conflicting_fields
    }

    pub(crate) const fn filing_limited_fields(self) -> usize {
        self.filing_limited_fields
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FundProductLimitation {
    HoldingsMissingAtCutoff,
    HoldingsConflict,
    HoldingsUnavailable,
    SomeHoldingIdentitiesUnresolved,
    SomeHoldingFieldsMissing,
    SomeHoldingFieldsConflict,
    SomeHoldingUnitsUnspecified,
    FundFilingInformationConflict,
    FundFilingInformationLimited,
    AnnualFundInformationMissing,
    AnnualFundInformationConflict,
    AnnualFundInformationUnavailable,
    AnnualFundInformationLimited,
    DailyNavUnavailable,
    DailyNavConflict,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum FundProductProjectionError {
    #[error("fund research evidence is inconsistent")]
    InvalidEvidence,
    #[error("fund research projection exceeded its fixed resource bound")]
    ResourceExhausted,
}

struct ProjectedAnnualInformation {
    product: FundAnnualInformationProduct,
    reported_fields: usize,
    missing_fields: usize,
    conflicting_fields: usize,
    limited_fields: usize,
}

#[derive(Default)]
struct ProjectedFieldCoverage {
    reported: usize,
    missing: usize,
    conflicting: usize,
    limited: usize,
}

struct FundProductByteBudget {
    remaining: usize,
}

impl FundProductByteBudget {
    fn new() -> Result<Self, FundProductProjectionError> {
        let mut budget = Self {
            remaining: MAX_FUND_PRODUCT_PROJECTED_BYTES,
        };
        budget.charge(std::mem::size_of::<FundProductResult>())?;
        budget.charge(
            std::mem::size_of::<FundProductLimitation>()
                .checked_mul(8)
                .ok_or(FundProductProjectionError::ResourceExhausted)?,
        )?;
        Ok(budget)
    }

    fn charge(&mut self, bytes: usize) -> Result<(), FundProductProjectionError> {
        self.remaining = self
            .remaining
            .checked_sub(bytes)
            .ok_or(FundProductProjectionError::ResourceExhausted)?;
        Ok(())
    }

    fn charge_serialized<T: Serialize>(
        &mut self,
        value: &T,
    ) -> Result<(), FundProductProjectionError> {
        let bytes = fund_serialized_bytes_with_limit(value, self.remaining)?;
        self.charge(bytes)
    }
}

struct FundBoundedCountingWriter {
    remaining: usize,
    written: usize,
    exhausted: bool,
}

impl Write for FundBoundedCountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > self.remaining {
            self.exhausted = true;
            return Err(io::Error::other(
                "fund product serialization bound exceeded",
            ));
        }
        self.remaining -= buffer.len();
        self.written = self
            .written
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("fund product serialization length overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn fund_serialized_bytes_with_limit<T: Serialize>(
    value: &T,
    limit: usize,
) -> Result<usize, FundProductProjectionError> {
    let mut writer = FundBoundedCountingWriter {
        remaining: limit,
        written: 0,
        exhausted: false,
    };
    if serde_json::to_writer(&mut writer, value).is_err() {
        return Err(if writer.exhausted {
            FundProductProjectionError::ResourceExhausted
        } else {
            FundProductProjectionError::InvalidEvidence
        });
    }
    Ok(writer.written)
}

fn ensure_fund_product_serialized_bound(
    product: &FundProductResult,
) -> Result<(), FundProductProjectionError> {
    fund_serialized_bytes_with_limit(product, MAX_FUND_PRODUCT_SERIALIZED_BYTES).map(|_| ())
}

/// Projects a verified canonical fund read and an optional exact latest-known NAV read.
pub(crate) fn project_fund_product(
    reads: FundProductReadSet<'_>,
    nav: Option<FundNavProductRead<'_>>,
    identity: ResearchProductIdentity,
    holding_identities: Vec<Option<ResearchProductIdentity>>,
) -> Result<FundProductResult, FundProductProjectionError> {
    let request = reads.portfolio.request();
    let annual_request = reads.annual.request();
    if request.family() != FundResearchFamily::PortfolioHoldings
        || annual_request.family() != FundResearchFamily::AnnualFundReport
        || annual_request.fund_instrument_id() != request.fund_instrument_id()
        || annual_request.knowledge_at() != request.knowledge_at()
        || annual_request.revision_policy() != request.revision_policy()
    {
        return Err(FundProductProjectionError::InvalidEvidence);
    }
    let mut budget = FundProductByteBudget::new()?;
    let instrument_id = request.fund_instrument_id();
    let knowledge_cutoff = request.knowledge_at();
    let nav = project_nav(instrument_id, knowledge_cutoff, nav)?;
    let annual = classify_annual(instrument_id, knowledge_cutoff, reads.annual.outcome())?;
    let annual_product = project_annual_information(annual, &mut budget)?;

    let mut result = match reads.portfolio.outcome() {
        FundResearchOutcome::Available(snapshot) => project_snapshot(
            instrument_id,
            knowledge_cutoff,
            snapshot,
            annual,
            annual_product,
            nav,
            &mut budget,
        ),
        FundResearchOutcome::Missing => Ok(empty_result(
            instrument_id,
            knowledge_cutoff,
            FundProductAvailability::Missing,
            FundProductSectionState::Missing,
            FundProductLimitation::HoldingsMissingAtCutoff,
            annual,
            annual_product,
            nav,
        )),
        FundResearchOutcome::Ambiguous => Ok(empty_result(
            instrument_id,
            knowledge_cutoff,
            FundProductAvailability::Conflict,
            FundProductSectionState::Conflict,
            FundProductLimitation::HoldingsConflict,
            annual,
            annual_product,
            nav,
        )),
        FundResearchOutcome::Unavailable(reason) => {
            let (availability, state, limitation) = match reason {
                FundResearchUnavailableReason::RevisionConflict
                | FundResearchUnavailableReason::MultipleCurrentRevisions
                | FundResearchUnavailableReason::MultipleReportVersions => (
                    FundProductAvailability::Conflict,
                    FundProductSectionState::Conflict,
                    FundProductLimitation::HoldingsConflict,
                ),
                FundResearchUnavailableReason::IncompleteReportCoverage
                | FundResearchUnavailableReason::RevisionUnavailable
                | FundResearchUnavailableReason::UnresolvedRevisionLink
                | FundResearchUnavailableReason::BrokenRevisionChain
                | FundResearchUnavailableReason::NoCurrentRevision => (
                    FundProductAvailability::Unavailable,
                    FundProductSectionState::Unavailable,
                    FundProductLimitation::HoldingsUnavailable,
                ),
            };
            Ok(empty_result(
                instrument_id,
                knowledge_cutoff,
                availability,
                state,
                limitation,
                annual,
                annual_product,
                nav,
            ))
        }
    }?;
    result.bind_display_identities(instrument_id, identity, holding_identities, &mut budget)?;
    ensure_fund_product_serialized_bound(&result)?;
    Ok(result)
}

fn project_snapshot(
    instrument_id: InstrumentId,
    knowledge_cutoff: Timestamp,
    snapshot: &FundResearchSnapshot,
    annual: AnnualFundState<'_>,
    annual_product: ProjectedAnnualInformation,
    nav: FundNavProduct,
    budget: &mut FundProductByteBudget,
) -> Result<FundProductResult, FundProductProjectionError> {
    let data = snapshot.holdings();
    if data.fund_instrument_id() != instrument_id
        || data.as_of() != knowledge_cutoff
        || data.availability() != FundResearchAvailability::Available
        || snapshot.exposure().holdings() != data.holdings().len()
    {
        return Err(FundProductProjectionError::InvalidEvidence);
    }

    let mut filing_coverage = ProjectedFieldCoverage::default();
    let filing = project_filing_state(
        data.filing_state()
            .ok_or(FundProductProjectionError::InvalidEvidence)?,
        Some(&mut filing_coverage),
        budget,
    )?;
    match filing.revision {
        FundProductRevisionState::Conflict => {
            return closed_snapshot_result(
                instrument_id,
                knowledge_cutoff,
                data,
                annual,
                annual_product,
                nav,
                filing,
                FundProductAvailability::Conflict,
                FundProductSectionState::Conflict,
                FundProductLimitation::HoldingsConflict,
            );
        }
        FundProductRevisionState::Unavailable => {
            return closed_snapshot_result(
                instrument_id,
                knowledge_cutoff,
                data,
                annual,
                annual_product,
                nav,
                filing,
                FundProductAvailability::Unavailable,
                FundProductSectionState::Unavailable,
                FundProductLimitation::HoldingsUnavailable,
            );
        }
        FundProductRevisionState::Superseded => {
            filing_coverage.limited = increment(filing_coverage.limited)?;
        }
        FundProductRevisionState::Current => {}
    }

    let mut holdings = Vec::new();
    budget.charge(
        std::mem::size_of::<FundHoldingProduct>()
            .checked_mul(data.holdings().len())
            .ok_or(FundProductProjectionError::ResourceExhausted)?,
    )?;
    holdings
        .try_reserve_exact(data.holdings().len())
        .map_err(|_| FundProductProjectionError::ResourceExhausted)?;
    let annual_data = annual.data();
    let mut coverage = FundProductCoverage {
        reports: data.report_count(),
        share_class_records: data.share_class_count(),
        holdings: data.holdings().len(),
        identified_holdings: 0,
        reported_quantities: 0,
        reported_values: 0,
        reported_weights: 0,
        missing_fields: 0,
        conflicting_fields: 0,
        other_quantity_units: 0,
        annual_reported_fields: annual_product.reported_fields,
        annual_missing_fields: annual_product.missing_fields,
        annual_conflicting_fields: annual_product.conflicting_fields,
        annual_limited_fields: annual_product.limited_fields,
        filing_conflicting_fields: filing_coverage.conflicting,
        filing_limited_fields: filing_coverage.limited,
    };
    if let Some(annual_data) = annual_data {
        coverage.reports = coverage
            .reports
            .checked_add(annual_data.report_count())
            .ok_or(FundProductProjectionError::ResourceExhausted)?;
        coverage.share_class_records = coverage
            .share_class_records
            .checked_add(annual_data.share_class_count())
            .ok_or(FundProductProjectionError::ResourceExhausted)?;
    }
    for holding in data.holdings() {
        holdings.push(project_holding(holding, &mut coverage, budget)?);
    }
    if coverage.identified_holdings != snapshot.exposure().identified_holdings()
        || coverage.reported_weights != snapshot.exposure().reported_weights()
        || count_missing_weights(data.holdings())? != snapshot.exposure().missing_weights()
        || count_conflicting_weights(data.holdings())? != snapshot.exposure().conflicting_weights()
    {
        return Err(FundProductProjectionError::InvalidEvidence);
    }

    let portfolio_analysis = project_portfolio_analysis(&holdings, &coverage)?;
    let limitations = limitations_for(&coverage, annual, &nav)?;
    let availability = snapshot_availability(&coverage, annual, &nav);
    Ok(FundProductResult {
        fund_share_class_instrument_id: instrument_id,
        identity: None,
        availability,
        holdings: FundHoldingsProduct {
            state: FundProductSectionState::Reported,
            filing: Some(filing),
            items: holdings.into_boxed_slice(),
        },
        annual_information: annual_product.product,
        current_research: FundCurrentResearchProduct {
            net_asset_value: nav,
        },
        portfolio_analysis,
        clocks: FundProductClocks {
            knowledge_cutoff,
            latest_fund_information_known_at: latest_timestamp(
                data.latest_known_at(),
                annual_data.and_then(|value| value.latest_known_at()),
            ),
        },
        coverage,
        limitations,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "closed result retains independent product states"
)]
fn closed_snapshot_result(
    instrument_id: InstrumentId,
    knowledge_cutoff: Timestamp,
    data: &FundResearchData,
    annual: AnnualFundState<'_>,
    annual_product: ProjectedAnnualInformation,
    nav: FundNavProduct,
    filing: FundFilingProduct,
    primary_availability: FundProductAvailability,
    holdings_state: FundProductSectionState,
    primary_limitation: FundProductLimitation,
) -> Result<FundProductResult, FundProductProjectionError> {
    let annual_data = annual.data();
    let mut reports = data.report_count();
    let mut share_class_records = data.share_class_count();
    if let Some(annual_data) = annual_data {
        reports = reports
            .checked_add(annual_data.report_count())
            .ok_or(FundProductProjectionError::ResourceExhausted)?;
        share_class_records = share_class_records
            .checked_add(annual_data.share_class_count())
            .ok_or(FundProductProjectionError::ResourceExhausted)?;
    }
    let ProjectedAnnualInformation {
        product: annual_information,
        reported_fields: annual_reported_fields,
        missing_fields: annual_missing_fields,
        conflicting_fields: annual_conflicting_fields,
        limited_fields: annual_limited_fields,
    } = annual_product;
    let annual_state = annual_information.state;
    let filing_conflicting_fields =
        usize::from(filing.revision == FundProductRevisionState::Conflict);
    let filing_limited_fields =
        usize::from(filing.revision == FundProductRevisionState::Unavailable);
    let availability = empty_availability(primary_availability, annual_state, &nav);
    let limitations = result_limitations(
        primary_limitation,
        annual,
        annual_state,
        annual_limited_fields > 0,
        &nav,
    );
    Ok(FundProductResult {
        fund_share_class_instrument_id: instrument_id,
        identity: None,
        availability,
        holdings: FundHoldingsProduct {
            state: holdings_state,
            filing: Some(filing),
            items: Box::new([]),
        },
        annual_information,
        current_research: FundCurrentResearchProduct {
            net_asset_value: nav,
        },
        portfolio_analysis: empty_portfolio_analysis(analysis_state_for_section(holdings_state)),
        clocks: FundProductClocks {
            knowledge_cutoff,
            latest_fund_information_known_at: latest_timestamp(
                data.latest_known_at(),
                annual_data.and_then(FundResearchData::latest_known_at),
            ),
        },
        coverage: FundProductCoverage {
            reports,
            share_class_records,
            holdings: data.holdings().len(),
            identified_holdings: 0,
            reported_quantities: 0,
            reported_values: 0,
            reported_weights: 0,
            missing_fields: 0,
            conflicting_fields: 0,
            other_quantity_units: 0,
            annual_reported_fields,
            annual_missing_fields,
            annual_conflicting_fields,
            annual_limited_fields,
            filing_conflicting_fields,
            filing_limited_fields,
        },
        limitations,
    })
}

fn project_holding(
    holding: &FundHoldingData,
    coverage: &mut FundProductCoverage,
    budget: &mut FundProductByteBudget,
) -> Result<FundHoldingProduct, FundProductProjectionError> {
    if holding.instrument_id().is_some() {
        coverage.identified_holdings = increment(coverage.identified_holdings)?;
    }
    Ok(FundHoldingProduct {
        instrument_id: holding.instrument_id(),
        identity: None,
        percentage_weight: reported_decimal(holding.percentage_of_net_assets())?,
        percentage_weight_conflict: holding.percentage_of_net_assets().conflict().is_some(),
        quantity: project_quantity(holding.quantity(), coverage, budget)?,
        value: project_money(holding.value(), coverage, budget)?,
        percentage_of_net_assets: project_decimal(
            holding.percentage_of_net_assets(),
            coverage,
            budget,
        )?,
    })
}

fn project_quantity(
    value: &FundReportedValue<FundHoldingQuantity>,
    coverage: &mut FundProductCoverage,
    budget: &mut FundProductByteBudget,
) -> Result<FundProductValue<FundQuantityProduct>, FundProductProjectionError> {
    match value {
        FundReportedValue::Reported(quantity) => {
            let unit = match quantity.unit() {
                FundHoldingUnit::Shares => FundQuantityProductUnit::Shares,
                FundHoldingUnit::Principal => FundQuantityProductUnit::Principal,
                FundHoldingUnit::Contracts => FundQuantityProductUnit::Contracts,
                FundHoldingUnit::Currency(currency) => FundQuantityProductUnit::Currency(*currency),
                FundHoldingUnit::Other(_) => {
                    coverage.other_quantity_units = increment(coverage.other_quantity_units)?;
                    coverage.missing_fields = increment(coverage.missing_fields)?;
                    return Ok(FundProductValue::Missing(
                        FundProductMissingReason::CoverageLimited,
                    ));
                }
            };
            coverage.reported_quantities = increment(coverage.reported_quantities)?;
            Ok(FundProductValue::Reported(FundQuantityProduct {
                amount: copy_decimal(quantity.amount(), budget)?,
                unit,
            }))
        }
        FundReportedValue::Missing(reason) => {
            coverage.missing_fields = increment(coverage.missing_fields)?;
            Ok(FundProductValue::Missing(product_missing(*reason)))
        }
        FundReportedValue::Conflict(reason) => {
            coverage.conflicting_fields = increment(coverage.conflicting_fields)?;
            Ok(FundProductValue::Conflict(product_conflict(*reason)))
        }
    }
}

fn project_money(
    value: &FundReportedValue<FundCurrencyAmount>,
    coverage: &mut FundProductCoverage,
    budget: &mut FundProductByteBudget,
) -> Result<FundProductValue<FundMoneyProduct>, FundProductProjectionError> {
    match value {
        FundReportedValue::Reported(value) => {
            coverage.reported_values = increment(coverage.reported_values)?;
            Ok(FundProductValue::Reported(FundMoneyProduct {
                amount: copy_decimal(value.amount(), budget)?,
                currency: value.currency(),
            }))
        }
        FundReportedValue::Missing(reason) => {
            coverage.missing_fields = increment(coverage.missing_fields)?;
            Ok(FundProductValue::Missing(product_missing(*reason)))
        }
        FundReportedValue::Conflict(reason) => {
            coverage.conflicting_fields = increment(coverage.conflicting_fields)?;
            Ok(FundProductValue::Conflict(product_conflict(*reason)))
        }
    }
}

fn project_decimal(
    value: &FundReportedValue<FundReportedDecimal>,
    coverage: &mut FundProductCoverage,
    budget: &mut FundProductByteBudget,
) -> Result<FundProductValue<Box<str>>, FundProductProjectionError> {
    match value {
        FundReportedValue::Reported(value) => {
            coverage.reported_weights = increment(coverage.reported_weights)?;
            Ok(FundProductValue::Reported(copy_decimal(value, budget)?))
        }
        FundReportedValue::Missing(reason) => {
            coverage.missing_fields = increment(coverage.missing_fields)?;
            Ok(FundProductValue::Missing(product_missing(*reason)))
        }
        FundReportedValue::Conflict(reason) => {
            coverage.conflicting_fields = increment(coverage.conflicting_fields)?;
            Ok(FundProductValue::Conflict(product_conflict(*reason)))
        }
    }
}

fn project_annual_information(
    annual: AnnualFundState<'_>,
    budget: &mut FundProductByteBudget,
) -> Result<ProjectedAnnualInformation, FundProductProjectionError> {
    let AnnualFundState::Available(data) = annual else {
        let state = match annual {
            AnnualFundState::Missing => FundProductSectionState::Missing,
            AnnualFundState::Conflict => FundProductSectionState::Conflict,
            AnnualFundState::Unavailable => FundProductSectionState::Unavailable,
            AnnualFundState::Available(_) => unreachable!(),
        };
        return Ok(ProjectedAnnualInformation {
            product: FundAnnualInformationProduct {
                state,
                filing: None,
                facts: None,
            },
            reported_fields: 0,
            missing_fields: 0,
            conflicting_fields: 0,
            limited_fields: 0,
        });
    };
    let filing_state = data
        .filing_state()
        .ok_or(FundProductProjectionError::InvalidEvidence)?;
    let mut coverage = ProjectedFieldCoverage::default();
    let filing = project_filing_state(filing_state, Some(&mut coverage), budget)?;
    match filing.revision {
        FundProductRevisionState::Conflict => {
            coverage.conflicting = increment(coverage.conflicting)?;
            return Ok(projected_annual_without_facts(
                FundProductSectionState::Conflict,
                filing,
                coverage,
            ));
        }
        FundProductRevisionState::Unavailable => {
            coverage.limited = increment(coverage.limited)?;
            return Ok(projected_annual_without_facts(
                FundProductSectionState::Unavailable,
                filing,
                coverage,
            ));
        }
        FundProductRevisionState::Superseded => {
            coverage.limited = increment(coverage.limited)?;
        }
        FundProductRevisionState::Current => {}
    }
    let information = data
        .annual_information()
        .ok_or(FundProductProjectionError::InvalidEvidence)?;
    budget.charge(std::mem::size_of::<FundAnnualFactsProduct>())?;
    let reporting_currency =
        project_annual_scalar(information.reporting_currency(), &mut coverage)?;
    let monthly_average_net_assets = project_annual_money(
        information.monthly_average_net_assets(),
        information.reporting_currency(),
        &mut coverage,
        budget,
    )?;
    let daily_average_net_assets = project_annual_money(
        information.daily_average_net_assets(),
        information.reporting_currency(),
        &mut coverage,
        budget,
    )?;
    let facts = FundAnnualFactsProduct {
        reporting_period_less_than_twelve_months: project_annual_scalar(
            information.reporting_period_less_than_twelve_months(),
            &mut coverage,
        )?,
        reporting_currency,
        monthly_average_net_assets,
        daily_average_net_assets,
        is_etf: project_annual_scalar(information.is_etf(), &mut coverage)?,
        is_index: project_annual_scalar(information.is_index(), &mut coverage)?,
        collateral_required: project_annual_scalar(
            information.collateral_required(),
            &mut coverage,
        )?,
        shares_per_creation_unit: project_annual_decimal(
            information.shares_per_creation_unit(),
            &mut coverage,
            budget,
        )?,
        shares_per_redemption_unit: project_annual_decimal(
            information.shares_per_redemption_unit(),
            &mut coverage,
            budget,
        )?,
        in_kind: project_annual_scalar(information.in_kind(), &mut coverage)?,
    };
    let state = if coverage.conflicting > 0 {
        FundProductSectionState::Conflict
    } else if coverage.reported > 0 {
        FundProductSectionState::Reported
    } else {
        FundProductSectionState::Missing
    };
    Ok(ProjectedAnnualInformation {
        product: FundAnnualInformationProduct {
            state,
            filing: Some(filing),
            facts: Some(facts),
        },
        reported_fields: coverage.reported,
        missing_fields: coverage.missing,
        conflicting_fields: coverage.conflicting,
        limited_fields: coverage.limited,
    })
}

fn projected_annual_without_facts(
    state: FundProductSectionState,
    filing: FundFilingProduct,
    coverage: ProjectedFieldCoverage,
) -> ProjectedAnnualInformation {
    ProjectedAnnualInformation {
        product: FundAnnualInformationProduct {
            state,
            filing: Some(filing),
            facts: None,
        },
        reported_fields: coverage.reported,
        missing_fields: coverage.missing,
        conflicting_fields: coverage.conflicting,
        limited_fields: coverage.limited,
    }
}

fn project_filing_state(
    state: &FundResearchFilingState,
    mut coverage: Option<&mut ProjectedFieldCoverage>,
    budget: &mut FundProductByteBudget,
) -> Result<FundFilingProduct, FundProductProjectionError> {
    budget.charge(std::mem::size_of::<FundFilingProduct>())?;
    Ok(FundFilingProduct {
        report_period_end: project_scalar_value(
            state.report_period_end(),
            coverage.as_deref_mut(),
        )?,
        report_date: project_scalar_value(state.report_date(), coverage.as_deref_mut())?,
        filed_date: project_scalar_value(state.filed_date(), coverage.as_deref_mut())?,
        accepted_at: project_scalar_value(state.accepted_at(), coverage.as_deref_mut())?,
        available_at: state.available_at(),
        amendment: match state.amendment() {
            FundAmendmentState::Original => FundProductAmendmentState::Original,
            FundAmendmentState::Amendment => FundProductAmendmentState::Amendment,
        },
        revision: match state.revision_status() {
            FundRevisionStatus::Current => FundProductRevisionState::Current,
            FundRevisionStatus::Superseded => FundProductRevisionState::Superseded,
            FundRevisionStatus::Conflict => FundProductRevisionState::Conflict,
            FundRevisionStatus::Unavailable => FundProductRevisionState::Unavailable,
        },
    })
}

fn project_scalar_value<T: Copy>(
    value: &FundReportedValue<T>,
    coverage: Option<&mut ProjectedFieldCoverage>,
) -> Result<FundProductValue<T>, FundProductProjectionError> {
    match value {
        FundReportedValue::Reported(value) => {
            if let Some(coverage) = coverage {
                coverage.reported = increment(coverage.reported)?;
            }
            Ok(FundProductValue::Reported(*value))
        }
        FundReportedValue::Missing(reason) => {
            if let Some(coverage) = coverage {
                coverage.missing = increment(coverage.missing)?;
                if *reason != FundMissingState::NotApplicable {
                    coverage.limited = increment(coverage.limited)?;
                }
            }
            Ok(FundProductValue::Missing(product_missing(*reason)))
        }
        FundReportedValue::Conflict(reason) => {
            if let Some(coverage) = coverage {
                coverage.conflicting = increment(coverage.conflicting)?;
            }
            Ok(FundProductValue::Conflict(product_conflict(*reason)))
        }
    }
}

fn project_annual_scalar<T: Copy>(
    value: &FundReportedValue<T>,
    coverage: &mut ProjectedFieldCoverage,
) -> Result<FundProductValue<T>, FundProductProjectionError> {
    project_scalar_value(value, Some(coverage))
}

fn project_annual_decimal(
    value: &FundReportedValue<FundReportedDecimal>,
    coverage: &mut ProjectedFieldCoverage,
    budget: &mut FundProductByteBudget,
) -> Result<FundProductValue<Box<str>>, FundProductProjectionError> {
    match value {
        FundReportedValue::Reported(value) => {
            coverage.reported = increment(coverage.reported)?;
            Ok(FundProductValue::Reported(copy_decimal(value, budget)?))
        }
        FundReportedValue::Missing(reason) => {
            coverage.missing = increment(coverage.missing)?;
            if *reason != FundMissingState::NotApplicable {
                coverage.limited = increment(coverage.limited)?;
            }
            Ok(FundProductValue::Missing(product_missing(*reason)))
        }
        FundReportedValue::Conflict(reason) => {
            coverage.conflicting = increment(coverage.conflicting)?;
            Ok(FundProductValue::Conflict(product_conflict(*reason)))
        }
    }
}

fn project_annual_money(
    amount: &FundReportedValue<FundReportedDecimal>,
    currency: &FundReportedValue<Currency>,
    coverage: &mut ProjectedFieldCoverage,
    budget: &mut FundProductByteBudget,
) -> Result<FundProductValue<FundMoneyProduct>, FundProductProjectionError> {
    match amount {
        FundReportedValue::Missing(reason) => {
            coverage.missing = increment(coverage.missing)?;
            if *reason != FundMissingState::NotApplicable {
                coverage.limited = increment(coverage.limited)?;
            }
            Ok(FundProductValue::Missing(product_missing(*reason)))
        }
        FundReportedValue::Conflict(reason) => {
            coverage.conflicting = increment(coverage.conflicting)?;
            Ok(FundProductValue::Conflict(product_conflict(*reason)))
        }
        FundReportedValue::Reported(amount) => match currency {
            FundReportedValue::Reported(currency) => {
                coverage.reported = increment(coverage.reported)?;
                Ok(FundProductValue::Reported(FundMoneyProduct {
                    amount: copy_decimal(amount, budget)?,
                    currency: *currency,
                }))
            }
            FundReportedValue::Missing(reason) => {
                coverage.missing = increment(coverage.missing)?;
                coverage.limited = increment(coverage.limited)?;
                Ok(FundProductValue::Missing(product_missing(*reason)))
            }
            FundReportedValue::Conflict(reason) => {
                coverage.conflicting = increment(coverage.conflicting)?;
                Ok(FundProductValue::Conflict(product_conflict(*reason)))
            }
        },
    }
}

fn copy_decimal(
    value: &FundReportedDecimal,
    budget: &mut FundProductByteBudget,
) -> Result<Box<str>, FundProductProjectionError> {
    budget.charge(value.as_str().len())?;
    try_boxed_product_text(value.as_str(), MAX_FUND_PRODUCT_DECIMAL_BYTES).map_err(|error| {
        match error {
            ProductTextCopyError::BoundExceeded => FundProductProjectionError::InvalidEvidence,
            ProductTextCopyError::AllocationFailed => FundProductProjectionError::ResourceExhausted,
        }
    })
}

fn project_nav(
    instrument_id: InstrumentId,
    knowledge_cutoff: Timestamp,
    read: Option<FundNavProductRead<'_>>,
) -> Result<FundNavProduct, FundProductProjectionError> {
    let Some(read) = read else {
        return Ok(FundNavProduct::Unavailable);
    };
    let request = read.output.request();
    let observations = read.output.observations();
    if request.instrument_id() != instrument_id
        || request.knowledge_cutoff() != knowledge_cutoff
        || request.revision_mode() != PointInTimeRevisionMode::LatestKnown
        || read.output.output().manifest() != request.manifest()
        || !read.output.selection_complete()
        || read.output.returned_count() != observations.len()
    {
        return Err(FundProductProjectionError::InvalidEvidence);
    }
    for observation in observations {
        validate_nav_observation(request, observation)?;
    }
    let Some(latest_date) = observations.iter().map(FundNavObservation::nav_date).max() else {
        return Ok(FundNavProduct::Missing {
            nav_date: None,
            reason: FundNavProductMissingReason::NotReported,
        });
    };
    let mut latest = observations
        .iter()
        .filter(|observation| observation.nav_date() == latest_date);
    let observation = latest
        .next()
        .ok_or(FundProductProjectionError::InvalidEvidence)?;
    if latest.next().is_some() {
        return Ok(FundNavProduct::Conflict {
            nav_date: latest_date,
        });
    }
    let available_at = observation
        .context()
        .provenance()
        .availability()
        .conservative_available_at()
        .ok_or(FundProductProjectionError::InvalidEvidence)?;
    match observation.value() {
        FundNavValue::Observed(money) => Ok(FundNavProduct::Reported {
            nav_date: observation.nav_date(),
            value: money,
            available_at,
            correction: observation.revision_evidence().correction(),
            finality: observation.revision_evidence().finality(),
        }),
        FundNavValue::Missing(reason) => Ok(FundNavProduct::Missing {
            nav_date: Some(observation.nav_date()),
            reason: product_nav_missing(reason),
        }),
    }
}

fn validate_nav_observation(
    request: &AnalyticalFundNavReadRequest,
    observation: &FundNavObservation,
) -> Result<(), FundProductProjectionError> {
    let provenance = observation.context().provenance();
    let available_at = provenance
        .availability()
        .conservative_available_at()
        .ok_or(FundProductProjectionError::InvalidEvidence)?;
    if provenance.instrument_id() != Some(request.instrument_id())
        || available_at > request.knowledge_cutoff()
        || provenance.received_at() > request.knowledge_cutoff()
        || provenance.ingested_at() > request.knowledge_cutoff()
        || observation.canonical_published_at() > request.knowledge_cutoff()
        || request.date_range().is_some_and(|range| {
            observation.nav_date() < range.start() || observation.nav_date() > range.end()
        })
    {
        return Err(FundProductProjectionError::InvalidEvidence);
    }
    Ok(())
}

fn empty_result(
    instrument_id: InstrumentId,
    knowledge_cutoff: Timestamp,
    availability: FundProductAvailability,
    section_state: FundProductSectionState,
    primary_limitation: FundProductLimitation,
    annual: AnnualFundState<'_>,
    annual_product: ProjectedAnnualInformation,
    nav: FundNavProduct,
) -> FundProductResult {
    let annual_data = annual.data();
    let ProjectedAnnualInformation {
        product: annual_information,
        reported_fields: annual_reported_fields,
        missing_fields: annual_missing_fields,
        conflicting_fields: annual_conflicting_fields,
        limited_fields: annual_limited_fields,
    } = annual_product;
    let availability = empty_availability(availability, annual_information.state, &nav);
    let limitations = result_limitations(
        primary_limitation,
        annual,
        annual_information.state,
        annual_limited_fields > 0,
        &nav,
    );
    FundProductResult {
        fund_share_class_instrument_id: instrument_id,
        identity: None,
        availability,
        holdings: FundHoldingsProduct {
            state: section_state,
            filing: None,
            items: Box::new([]),
        },
        annual_information,
        current_research: FundCurrentResearchProduct {
            net_asset_value: nav,
        },
        portfolio_analysis: empty_portfolio_analysis(analysis_state_for_section(section_state)),
        clocks: FundProductClocks {
            knowledge_cutoff,
            latest_fund_information_known_at: annual_data.and_then(|value| value.latest_known_at()),
        },
        coverage: FundProductCoverage {
            reports: annual_data.map_or(0, |value| value.report_count()),
            share_class_records: annual_data.map_or(0, |value| value.share_class_count()),
            holdings: 0,
            identified_holdings: 0,
            reported_quantities: 0,
            reported_values: 0,
            reported_weights: 0,
            missing_fields: 0,
            conflicting_fields: 0,
            other_quantity_units: 0,
            annual_reported_fields,
            annual_missing_fields,
            annual_conflicting_fields,
            annual_limited_fields,
            filing_conflicting_fields: 0,
            filing_limited_fields: 0,
        },
        limitations,
    }
}

fn limitations_for(
    coverage: &FundProductCoverage,
    annual: AnnualFundState<'_>,
    nav: &FundNavProduct,
) -> Result<Box<[FundProductLimitation]>, FundProductProjectionError> {
    let mut limitations = Vec::new();
    limitations
        .try_reserve_exact(8)
        .map_err(|_| FundProductProjectionError::ResourceExhausted)?;
    if coverage.identified_holdings < coverage.holdings {
        limitations.push(FundProductLimitation::SomeHoldingIdentitiesUnresolved);
    }
    if coverage.missing_fields > 0 {
        limitations.push(FundProductLimitation::SomeHoldingFieldsMissing);
    }
    if coverage.conflicting_fields > 0 {
        limitations.push(FundProductLimitation::SomeHoldingFieldsConflict);
    }
    if coverage.other_quantity_units > 0 {
        limitations.push(FundProductLimitation::SomeHoldingUnitsUnspecified);
    }
    if coverage.filing_conflicting_fields > 0 {
        limitations.push(FundProductLimitation::FundFilingInformationConflict);
    } else if coverage.filing_limited_fields > 0 {
        limitations.push(FundProductLimitation::FundFilingInformationLimited);
    }
    if let Some(limitation) = annual.limitation() {
        limitations.push(limitation);
    } else if coverage.annual_conflicting_fields > 0 {
        limitations.push(FundProductLimitation::AnnualFundInformationConflict);
    } else if coverage.annual_limited_fields > 0 {
        limitations.push(FundProductLimitation::AnnualFundInformationLimited);
    } else if coverage.annual_reported_fields == 0 {
        limitations.push(FundProductLimitation::AnnualFundInformationMissing);
    }
    if let Some(limitation) = nav_limitation(nav) {
        limitations.push(limitation);
    }
    Ok(limitations.into_boxed_slice())
}

fn snapshot_availability(
    coverage: &FundProductCoverage,
    annual: AnnualFundState<'_>,
    nav: &FundNavProduct,
) -> FundProductAvailability {
    if coverage.conflicting_fields > 0
        || coverage.annual_conflicting_fields > 0
        || coverage.filing_conflicting_fields > 0
        || matches!(annual, AnnualFundState::Conflict)
        || matches!(nav, FundNavProduct::Conflict { .. })
    {
        FundProductAvailability::Conflict
    } else if coverage.identified_holdings < coverage.holdings
        || coverage.missing_fields > 0
        || coverage.other_quantity_units > 0
        || coverage.annual_reported_fields == 0
        || coverage.annual_limited_fields > 0
        || coverage.filing_limited_fields > 0
        || !matches!(annual, AnnualFundState::Available(_))
        || !matches!(nav, FundNavProduct::Reported { .. })
    {
        FundProductAvailability::Partial
    } else {
        FundProductAvailability::Available
    }
}

fn empty_availability(
    primary: FundProductAvailability,
    annual: FundProductSectionState,
    nav: &FundNavProduct,
) -> FundProductAvailability {
    if primary == FundProductAvailability::Conflict
        || annual == FundProductSectionState::Conflict
        || matches!(nav, FundNavProduct::Conflict { .. })
    {
        FundProductAvailability::Conflict
    } else if annual == FundProductSectionState::Reported
        || matches!(nav, FundNavProduct::Reported { .. })
    {
        FundProductAvailability::Partial
    } else {
        primary
    }
}

fn result_limitations(
    primary: FundProductLimitation,
    annual: AnnualFundState<'_>,
    annual_section: FundProductSectionState,
    annual_limited: bool,
    nav: &FundNavProduct,
) -> Box<[FundProductLimitation]> {
    let annual_limitation = annual.limitation().or_else(|| match annual_section {
        FundProductSectionState::Conflict => {
            Some(FundProductLimitation::AnnualFundInformationConflict)
        }
        FundProductSectionState::Unavailable => {
            Some(FundProductLimitation::AnnualFundInformationUnavailable)
        }
        FundProductSectionState::Missing => {
            Some(FundProductLimitation::AnnualFundInformationMissing)
        }
        FundProductSectionState::Reported if annual_limited => {
            Some(FundProductLimitation::AnnualFundInformationLimited)
        }
        FundProductSectionState::Reported => None,
    });
    match (annual_limitation, nav_limitation(nav)) {
        (Some(annual), Some(nav)) => Box::new([primary, annual, nav]),
        (Some(annual), None) => Box::new([primary, annual]),
        (None, Some(nav)) => Box::new([primary, nav]),
        (None, None) => Box::new([primary]),
    }
}

fn nav_limitation(nav: &FundNavProduct) -> Option<FundProductLimitation> {
    match nav {
        FundNavProduct::Reported { .. } => None,
        FundNavProduct::Conflict { .. } => Some(FundProductLimitation::DailyNavConflict),
        FundNavProduct::Missing { .. } | FundNavProduct::Unavailable => {
            Some(FundProductLimitation::DailyNavUnavailable)
        }
    }
}

#[derive(Clone, Copy)]
enum AnnualFundState<'read> {
    Available(&'read super::sec_fund_product::FundResearchData),
    Missing,
    Conflict,
    Unavailable,
}

impl<'read> AnnualFundState<'read> {
    const fn data(self) -> Option<&'read super::sec_fund_product::FundResearchData> {
        match self {
            Self::Available(data) => Some(data),
            Self::Missing | Self::Conflict | Self::Unavailable => None,
        }
    }

    const fn limitation(self) -> Option<FundProductLimitation> {
        match self {
            Self::Available(_) => None,
            Self::Missing => Some(FundProductLimitation::AnnualFundInformationMissing),
            Self::Conflict => Some(FundProductLimitation::AnnualFundInformationConflict),
            Self::Unavailable => Some(FundProductLimitation::AnnualFundInformationUnavailable),
        }
    }
}

fn classify_annual<'read>(
    instrument_id: InstrumentId,
    knowledge_cutoff: Timestamp,
    outcome: &'read FundResearchOutcome,
) -> Result<AnnualFundState<'read>, FundProductProjectionError> {
    match outcome {
        FundResearchOutcome::Available(snapshot) => {
            let data = snapshot.holdings();
            if data.fund_instrument_id() != instrument_id
                || data.as_of() != knowledge_cutoff
                || data.availability() != FundResearchAvailability::Available
                || !data.holdings().is_empty()
                || snapshot.exposure().holdings() != 0
            {
                return Err(FundProductProjectionError::InvalidEvidence);
            }
            Ok(AnnualFundState::Available(data))
        }
        FundResearchOutcome::Missing => Ok(AnnualFundState::Missing),
        FundResearchOutcome::Ambiguous => Ok(AnnualFundState::Conflict),
        FundResearchOutcome::Unavailable(reason) => match reason {
            FundResearchUnavailableReason::RevisionConflict
            | FundResearchUnavailableReason::MultipleCurrentRevisions
            | FundResearchUnavailableReason::MultipleReportVersions => {
                Ok(AnnualFundState::Conflict)
            }
            FundResearchUnavailableReason::IncompleteReportCoverage
            | FundResearchUnavailableReason::RevisionUnavailable
            | FundResearchUnavailableReason::UnresolvedRevisionLink
            | FundResearchUnavailableReason::BrokenRevisionChain
            | FundResearchUnavailableReason::NoCurrentRevision => Ok(AnnualFundState::Unavailable),
        },
    }
}

fn latest_timestamp(left: Option<Timestamp>, right: Option<Timestamp>) -> Option<Timestamp> {
    match (left, right) {
        (Some(left), Some(right)) => Some(if left > right { left } else { right }),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn count_missing_weights(
    holdings: &[FundHoldingData],
) -> Result<usize, FundProductProjectionError> {
    holdings.iter().try_fold(0_usize, |count, holding| {
        if holding.percentage_of_net_assets().missing().is_some() {
            increment(count)
        } else {
            Ok(count)
        }
    })
}

fn count_conflicting_weights(
    holdings: &[FundHoldingData],
) -> Result<usize, FundProductProjectionError> {
    holdings.iter().try_fold(0_usize, |count, holding| {
        if holding.percentage_of_net_assets().conflict().is_some() {
            increment(count)
        } else {
            Ok(count)
        }
    })
}

fn reported_decimal(
    value: &FundReportedValue<FundReportedDecimal>,
) -> Result<Option<Decimal>, FundProductProjectionError> {
    value
        .reported()
        .map(|reported| {
            Decimal::from_str_exact(reported.as_str())
                .map_err(|_| FundProductProjectionError::InvalidEvidence)
        })
        .transpose()
}

fn project_portfolio_analysis(
    holdings: &[FundHoldingProduct],
    coverage: &FundProductCoverage,
) -> Result<FundPortfolioAnalysisProduct, FundProductProjectionError> {
    let portfolio = weighted_portfolio(holdings)?;
    if portfolio.weighted_raw_holdings != coverage.reported_weights
        || portfolio.identified_weighted_raw_holdings > coverage.identified_holdings
    {
        return Err(FundProductProjectionError::InvalidEvidence);
    }
    let exposure_state = analysis_state(
        holdings.len(),
        portfolio.weighted_raw_holdings,
        portfolio.identified_weighted_raw_holdings,
        portfolio.conflicting_weight_holdings,
    );
    let concentration_state = if portfolio.conflicting_weight_holdings > 0 {
        FundAnalysisState::Conflict
    } else if holdings.is_empty() {
        FundAnalysisState::Missing
    } else if portfolio.analyzed_absolute_net_weight.is_none() {
        FundAnalysisState::Unavailable
    } else if portfolio.excluded_holdings == 0 {
        FundAnalysisState::Reported
    } else {
        FundAnalysisState::Partial
    };
    let mut absolute_weights = Vec::new();
    absolute_weights
        .try_reserve_exact(portfolio.positions.len())
        .map_err(|_| FundProductProjectionError::ResourceExhausted)?;
    if let Some(total) = portfolio.analyzed_absolute_net_weight {
        for position in &portfolio.positions {
            absolute_weights.push(
                position
                    .weight
                    .abs()
                    .checked_div(total)
                    .ok_or(FundProductProjectionError::InvalidEvidence)?,
            );
        }
    }
    absolute_weights.sort_unstable_by(|left, right| right.cmp(left));
    let largest_position_share = absolute_weights.first().copied();
    let top_five_share = sum_weights(absolute_weights.iter().take(5).copied())?;
    let top_ten_share = sum_weights(absolute_weights.iter().take(10).copied())?;
    let mut herfindahl_index = Decimal::ZERO;
    for share in &absolute_weights {
        herfindahl_index = herfindahl_index
            .checked_add(
                share
                    .checked_mul(*share)
                    .ok_or(FundProductProjectionError::InvalidEvidence)?,
            )
            .ok_or(FundProductProjectionError::InvalidEvidence)?;
    }
    Ok(FundPortfolioAnalysisProduct {
        policy: FundAggregationPolicy::NetInstrumentAbsoluteV1,
        coverage: portfolio.coverage(holdings.len()),
        exposure: FundExposureProduct {
            state: exposure_state,
            raw_net_weight: (portfolio.weighted_raw_holdings > 0)
                .then_some(portfolio.raw_net_weight),
            raw_gross_weight: (portfolio.weighted_raw_holdings > 0)
                .then_some(portfolio.raw_gross_weight),
            identified_raw_gross_weight: (portfolio.identified_weighted_raw_holdings > 0)
                .then_some(portfolio.identified_raw_gross_weight),
            net_instrument_gross_weight: portfolio.analyzed_absolute_net_weight,
        },
        concentration: FundConcentrationProduct {
            state: concentration_state,
            largest_position_share,
            top_five_share,
            top_ten_share,
            herfindahl_index: (!absolute_weights.is_empty()).then_some(herfindahl_index),
        },
    })
}

fn analysis_state(
    holdings: usize,
    weighted_holdings: usize,
    identified_weighted_holdings: usize,
    conflicting_weight_holdings: usize,
) -> FundAnalysisState {
    if conflicting_weight_holdings > 0 {
        FundAnalysisState::Conflict
    } else if holdings == 0 {
        FundAnalysisState::Missing
    } else if weighted_holdings == 0 {
        FundAnalysisState::Unavailable
    } else if weighted_holdings == holdings && identified_weighted_holdings == holdings {
        FundAnalysisState::Reported
    } else {
        FundAnalysisState::Partial
    }
}

fn analysis_state_for_section(state: FundProductSectionState) -> FundAnalysisState {
    match state {
        FundProductSectionState::Reported => FundAnalysisState::Reported,
        FundProductSectionState::Missing => FundAnalysisState::Missing,
        FundProductSectionState::Conflict => FundAnalysisState::Conflict,
        FundProductSectionState::Unavailable => FundAnalysisState::Unavailable,
    }
}

fn empty_portfolio_analysis(state: FundAnalysisState) -> FundPortfolioAnalysisProduct {
    FundPortfolioAnalysisProduct {
        policy: FundAggregationPolicy::NetInstrumentAbsoluteV1,
        coverage: empty_aggregation_coverage(),
        exposure: FundExposureProduct {
            state,
            raw_net_weight: None,
            raw_gross_weight: None,
            identified_raw_gross_weight: None,
            net_instrument_gross_weight: None,
        },
        concentration: FundConcentrationProduct {
            state,
            largest_position_share: None,
            top_five_share: None,
            top_ten_share: None,
            herfindahl_index: None,
        },
    }
}

const fn empty_aggregation_coverage() -> FundAggregationCoverageProduct {
    FundAggregationCoverageProduct {
        raw_holdings: 0,
        weighted_raw_holdings: 0,
        identified_weighted_raw_holdings: 0,
        analyzed_net_instruments: 0,
        excluded_raw_holdings: 0,
        analyzed_absolute_net_weight: None,
    }
}

#[derive(Clone, Copy)]
struct WeightedPosition {
    instrument_id: InstrumentId,
    weight: Decimal,
}

struct WeightedPortfolio {
    positions: Vec<WeightedPosition>,
    raw_net_weight: Decimal,
    raw_gross_weight: Decimal,
    identified_raw_gross_weight: Decimal,
    analyzed_absolute_net_weight: Option<Decimal>,
    weighted_raw_holdings: usize,
    identified_weighted_raw_holdings: usize,
    excluded_holdings: usize,
    conflicting_weight_holdings: usize,
}

impl WeightedPortfolio {
    fn coverage(&self, raw_holdings: usize) -> FundAggregationCoverageProduct {
        FundAggregationCoverageProduct {
            raw_holdings,
            weighted_raw_holdings: self.weighted_raw_holdings,
            identified_weighted_raw_holdings: self.identified_weighted_raw_holdings,
            analyzed_net_instruments: self.positions.len(),
            excluded_raw_holdings: self.excluded_holdings,
            analyzed_absolute_net_weight: self.analyzed_absolute_net_weight,
        }
    }
}

fn weighted_portfolio(
    holdings: &[FundHoldingProduct],
) -> Result<WeightedPortfolio, FundProductProjectionError> {
    let mut positions = Vec::new();
    positions
        .try_reserve_exact(holdings.len())
        .map_err(|_| FundProductProjectionError::ResourceExhausted)?;
    let mut excluded_holdings = 0_usize;
    let mut conflicting_weight_holdings = 0_usize;
    let mut raw_net_weight = Decimal::ZERO;
    let mut raw_gross_weight = Decimal::ZERO;
    let mut identified_raw_gross_weight = Decimal::ZERO;
    let mut weighted_raw_holdings = 0_usize;
    let mut identified_weighted_raw_holdings = 0_usize;
    for holding in holdings {
        if holding.percentage_weight_conflict {
            excluded_holdings = increment(excluded_holdings)?;
            conflicting_weight_holdings = increment(conflicting_weight_holdings)?;
            continue;
        }
        match (holding.instrument_id, holding.percentage_weight) {
            (Some(instrument_id), Some(weight)) => {
                weighted_raw_holdings = increment(weighted_raw_holdings)?;
                identified_weighted_raw_holdings = increment(identified_weighted_raw_holdings)?;
                raw_net_weight = checked_add_decimal(raw_net_weight, weight)?;
                raw_gross_weight = checked_add_decimal(raw_gross_weight, weight.abs())?;
                identified_raw_gross_weight =
                    checked_add_decimal(identified_raw_gross_weight, weight.abs())?;
                positions.push(WeightedPosition {
                    instrument_id,
                    weight,
                });
            }
            (None, Some(weight)) => {
                weighted_raw_holdings = increment(weighted_raw_holdings)?;
                raw_net_weight = checked_add_decimal(raw_net_weight, weight)?;
                raw_gross_weight = checked_add_decimal(raw_gross_weight, weight.abs())?;
                excluded_holdings = increment(excluded_holdings)?;
            }
            (_, None) => excluded_holdings = increment(excluded_holdings)?,
        }
    }
    positions.sort_unstable_by_key(|position| position.instrument_id);
    let mut merged: Vec<WeightedPosition> = Vec::new();
    merged
        .try_reserve_exact(positions.len())
        .map_err(|_| FundProductProjectionError::ResourceExhausted)?;
    for position in positions {
        if let Some(existing) = merged
            .last_mut()
            .filter(|existing| existing.instrument_id == position.instrument_id)
        {
            existing.weight = existing
                .weight
                .checked_add(position.weight)
                .ok_or(FundProductProjectionError::InvalidEvidence)?;
        } else {
            merged.push(position);
        }
    }
    merged.retain(|position| !position.weight.is_zero());
    let mut analyzed_absolute_net_weight = Decimal::ZERO;
    for position in &merged {
        analyzed_absolute_net_weight =
            checked_add_decimal(analyzed_absolute_net_weight, position.weight.abs())?;
    }
    Ok(WeightedPortfolio {
        positions: merged,
        raw_net_weight,
        raw_gross_weight,
        identified_raw_gross_weight,
        analyzed_absolute_net_weight: (!analyzed_absolute_net_weight.is_zero())
            .then_some(analyzed_absolute_net_weight),
        weighted_raw_holdings,
        identified_weighted_raw_holdings,
        excluded_holdings,
        conflicting_weight_holdings,
    })
}

fn checked_add_decimal(
    left: Decimal,
    right: Decimal,
) -> Result<Decimal, FundProductProjectionError> {
    left.checked_add(right)
        .ok_or(FundProductProjectionError::InvalidEvidence)
}

fn sum_weights(
    weights: impl Iterator<Item = Decimal>,
) -> Result<Option<Decimal>, FundProductProjectionError> {
    let mut total = Decimal::ZERO;
    let mut count = 0_usize;
    for weight in weights {
        total = total
            .checked_add(weight)
            .ok_or(FundProductProjectionError::InvalidEvidence)?;
        count = increment(count)?;
    }
    Ok((count > 0).then_some(total))
}

/// Compares two exact projected fund reads without exposing their private selector evidence.
pub(crate) fn compare_fund_overlap(
    left: &FundProductResult,
    right: &FundProductResult,
) -> Result<FundOverlapProduct, FundProductProjectionError> {
    if left.clocks.knowledge_cutoff != right.clocks.knowledge_cutoff {
        return Err(FundProductProjectionError::InvalidEvidence);
    }
    let knowledge_cutoff = left.clocks.knowledge_cutoff;
    let left_effective_period = effective_holdings_period(left);
    let right_effective_period = effective_holdings_period(right);
    let alignment = overlap_alignment(&left_effective_period, &right_effective_period);
    let left_portfolio = weighted_portfolio(&left.holdings.items)?;
    let right_portfolio = weighted_portfolio(&right.holdings.items)?;
    if alignment != FundOverlapAlignmentState::Aligned {
        let state = if alignment == FundOverlapAlignmentState::Conflict {
            FundAnalysisState::Conflict
        } else {
            FundAnalysisState::Unavailable
        };
        return Ok(overlap_without_value(
            state,
            alignment,
            left_effective_period,
            right_effective_period,
            &left_portfolio,
            &right_portfolio,
            left.holdings.items.len(),
            right.holdings.items.len(),
            knowledge_cutoff,
        ));
    }
    if left.holdings.state != FundProductSectionState::Reported
        || right.holdings.state != FundProductSectionState::Reported
    {
        let state = if left.holdings.state == FundProductSectionState::Conflict
            || right.holdings.state == FundProductSectionState::Conflict
        {
            FundAnalysisState::Conflict
        } else if left.holdings.state == FundProductSectionState::Missing
            || right.holdings.state == FundProductSectionState::Missing
        {
            FundAnalysisState::Missing
        } else {
            FundAnalysisState::Unavailable
        };
        return Ok(overlap_without_value(
            state,
            alignment,
            left_effective_period,
            right_effective_period,
            &left_portfolio,
            &right_portfolio,
            left.holdings.items.len(),
            right.holdings.items.len(),
            knowledge_cutoff,
        ));
    }

    if left_portfolio.conflicting_weight_holdings > 0
        || right_portfolio.conflicting_weight_holdings > 0
    {
        return Ok(overlap_without_value(
            FundAnalysisState::Conflict,
            alignment,
            left_effective_period,
            right_effective_period,
            &left_portfolio,
            &right_portfolio,
            left.holdings.items.len(),
            right.holdings.items.len(),
            knowledge_cutoff,
        ));
    }
    let (Some(left_total), Some(right_total)) = (
        left_portfolio.analyzed_absolute_net_weight,
        right_portfolio.analyzed_absolute_net_weight,
    ) else {
        return Ok(overlap_without_value(
            FundAnalysisState::Unavailable,
            alignment,
            left_effective_period,
            right_effective_period,
            &left_portfolio,
            &right_portfolio,
            left.holdings.items.len(),
            right.holdings.items.len(),
            knowledge_cutoff,
        ));
    };
    let mut left_index = 0_usize;
    let mut right_index = 0_usize;
    let mut matched_holdings = 0_usize;
    let mut overlap_share = Decimal::ZERO;
    while left_index < left_portfolio.positions.len()
        && right_index < right_portfolio.positions.len()
    {
        let left_position = left_portfolio.positions[left_index];
        let right_position = right_portfolio.positions[right_index];
        match left_position
            .instrument_id
            .cmp(&right_position.instrument_id)
        {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                let left_share = left_position
                    .weight
                    .abs()
                    .checked_div(left_total)
                    .ok_or(FundProductProjectionError::InvalidEvidence)?;
                let right_share = right_position
                    .weight
                    .abs()
                    .checked_div(right_total)
                    .ok_or(FundProductProjectionError::InvalidEvidence)?;
                overlap_share = checked_add_decimal(overlap_share, left_share.min(right_share))?;
                matched_holdings = increment(matched_holdings)?;
                left_index += 1;
                right_index += 1;
            }
        }
    }
    let excluded = left_portfolio.excluded_holdings > 0 || right_portfolio.excluded_holdings > 0;
    Ok(FundOverlapProduct {
        state: if excluded {
            FundAnalysisState::Partial
        } else {
            FundAnalysisState::Reported
        },
        policy: FundAggregationPolicy::NetInstrumentAbsoluteV1,
        alignment,
        overlap_share: Some(overlap_share),
        matched_holdings,
        left_effective_period,
        right_effective_period,
        left_coverage: left_portfolio.coverage(left.holdings.items.len()),
        right_coverage: right_portfolio.coverage(right.holdings.items.len()),
        knowledge_cutoff,
    })
}

fn effective_holdings_period(result: &FundProductResult) -> FundEffectiveHoldingsPeriodProduct {
    result
        .holdings
        .filing
        .as_ref()
        .map_or_else(unavailable_effective_period, |filing| {
            FundEffectiveHoldingsPeriodProduct {
                report_period_end: filing.report_period_end.clone(),
                report_date: filing.report_date.clone(),
            }
        })
}

fn unavailable_effective_period() -> FundEffectiveHoldingsPeriodProduct {
    FundEffectiveHoldingsPeriodProduct {
        report_period_end: FundProductValue::Missing(FundProductMissingReason::Unavailable),
        report_date: FundProductValue::Missing(FundProductMissingReason::Unavailable),
    }
}

fn overlap_alignment(
    left: &FundEffectiveHoldingsPeriodProduct,
    right: &FundEffectiveHoldingsPeriodProduct,
) -> FundOverlapAlignmentState {
    match (
        &left.report_period_end,
        &left.report_date,
        &right.report_period_end,
        &right.report_date,
    ) {
        (FundProductValue::Conflict(_), _, _, _)
        | (_, FundProductValue::Conflict(_), _, _)
        | (_, _, FundProductValue::Conflict(_), _)
        | (_, _, _, FundProductValue::Conflict(_)) => FundOverlapAlignmentState::Conflict,
        (
            FundProductValue::Reported(left_period_end),
            FundProductValue::Reported(left_report_date),
            FundProductValue::Reported(right_period_end),
            FundProductValue::Reported(right_report_date),
        ) if left_period_end == right_period_end && left_report_date == right_report_date => {
            FundOverlapAlignmentState::Aligned
        }
        (
            FundProductValue::Reported(_),
            FundProductValue::Reported(_),
            FundProductValue::Reported(_),
            FundProductValue::Reported(_),
        ) => FundOverlapAlignmentState::Misaligned,
        _ => FundOverlapAlignmentState::Unavailable,
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "closed overlap retains both effective periods, both coverages, and one cutoff"
)]
fn overlap_without_value(
    state: FundAnalysisState,
    alignment: FundOverlapAlignmentState,
    left_effective_period: FundEffectiveHoldingsPeriodProduct,
    right_effective_period: FundEffectiveHoldingsPeriodProduct,
    left: &WeightedPortfolio,
    right: &WeightedPortfolio,
    left_raw_holdings: usize,
    right_raw_holdings: usize,
    knowledge_cutoff: Timestamp,
) -> FundOverlapProduct {
    FundOverlapProduct {
        state,
        policy: FundAggregationPolicy::NetInstrumentAbsoluteV1,
        alignment,
        overlap_share: None,
        matched_holdings: 0,
        left_effective_period,
        right_effective_period,
        left_coverage: left.coverage(left_raw_holdings),
        right_coverage: right.coverage(right_raw_holdings),
        knowledge_cutoff,
    }
}

fn product_missing(reason: FundMissingState) -> FundProductMissingReason {
    match reason {
        FundMissingState::SourceAbsent => FundProductMissingReason::NotReported,
        FundMissingState::NotApplicable => FundProductMissingReason::NotApplicable,
        FundMissingState::ConfidentialOrOmitted => FundProductMissingReason::Withheld,
        FundMissingState::Invalid => FundProductMissingReason::Invalid,
        FundMissingState::Unavailable => FundProductMissingReason::Unavailable,
        FundMissingState::UnresolvedIdentity => FundProductMissingReason::UnresolvedIdentity,
        FundMissingState::DeclaredCoverageGap => FundProductMissingReason::CoverageLimited,
    }
}

fn product_conflict(reason: FundConflictState) -> FundProductConflictReason {
    match reason {
        FundConflictState::CompetingSourceRows => {
            FundProductConflictReason::ConflictingReportedValues
        }
        FundConflictState::CompetingRevisions => FundProductConflictReason::ConflictingRevisions,
        FundConflictState::ConflictingIdentity => FundProductConflictReason::ConflictingIdentity,
        FundConflictState::IncompatibleUnitOrCurrency => {
            FundProductConflictReason::IncompatibleUnits
        }
    }
}

fn product_nav_missing(reason: FundNavMissingState) -> FundNavProductMissingReason {
    match reason {
        FundNavMissingState::NotYetPublished => FundNavProductMissingReason::NotYetPublished,
        FundNavMissingState::Unsupported => FundNavProductMissingReason::Unsupported,
        FundNavMissingState::SourceMissing => FundNavProductMissingReason::NotReported,
        FundNavMissingState::Invalid => FundNavProductMissingReason::Invalid,
        FundNavMissingState::Unavailable => FundNavProductMissingReason::Unavailable,
    }
}

fn increment(value: usize) -> Result<usize, FundProductProjectionError> {
    value
        .checked_add(1)
        .ok_or(FundProductProjectionError::ResourceExhausted)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn fund_aggregation_nets_duplicates_and_requires_effective_period_alignment()
    -> anyhow::Result<()> {
        let instrument_a = InstrumentId::from_str("11111111-1111-4111-8111-111111111111")?;
        let instrument_b = InstrumentId::from_str("22222222-2222-4222-8222-222222222222")?;
        let left_holdings = vec![
            holding(instrument_a, 60),
            holding(instrument_a, -20),
            holding(instrument_b, 40),
        ];
        let right_holdings = vec![holding(instrument_a, 50), holding(instrument_b, 50)];
        let report_period_end = CalendarDate::new(2025, 12, 31)?;
        let report_date = CalendarDate::new(2026, 1, 31)?;
        let knowledge_cutoff = Timestamp::from_unix_nanos(1_800_000_000_000_000_000);
        let left = fund_result(
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            left_holdings,
            report_period_end,
            report_date,
            knowledge_cutoff,
        )?;
        let right = fund_result(
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            right_holdings,
            report_period_end,
            report_date,
            knowledge_cutoff,
        )?;

        let analysis = left.portfolio_analysis();
        assert_eq!(
            analysis.policy(),
            FundAggregationPolicy::NetInstrumentAbsoluteV1
        );
        assert_eq!(
            analysis.exposure().raw_net_weight(),
            Some(Decimal::from(80))
        );
        assert_eq!(
            analysis.exposure().raw_gross_weight(),
            Some(Decimal::from(120))
        );
        assert_eq!(
            analysis.exposure().net_instrument_gross_weight(),
            Some(Decimal::from(80))
        );
        assert_eq!(analysis.coverage().raw_holdings, 3);
        assert_eq!(analysis.coverage().analyzed_net_instruments, 2);
        assert_eq!(
            analysis.coverage().analyzed_absolute_net_weight,
            Some(Decimal::from(80))
        );
        assert_eq!(
            analysis.concentration().largest_position_share(),
            Some(Decimal::new(5, 1))
        );
        assert_eq!(
            analysis.concentration().herfindahl_index(),
            Some(Decimal::new(5, 1))
        );

        let aligned = left.compare_overlap(&right)?;
        assert_eq!(aligned.alignment(), FundOverlapAlignmentState::Aligned);
        assert_eq!(aligned.overlap_share(), Some(Decimal::ONE));
        assert_eq!(aligned.matched_holdings(), 2);

        let mut misaligned = right.clone();
        misaligned
            .holdings
            .filing
            .as_mut()
            .expect("test filing")
            .report_date = FundProductValue::Reported(CalendarDate::new(2026, 2, 28)?);
        let withheld = left.compare_overlap(&misaligned)?;
        assert_eq!(withheld.alignment(), FundOverlapAlignmentState::Misaligned);
        assert_eq!(withheld.state(), FundAnalysisState::Unavailable);
        assert_eq!(withheld.overlap_share(), None);

        let mut bounded = left.clone();
        let holding_identity = ResearchProductIdentity::try_new("Example holding", "HOLD")?;
        let holding_identities = vec![Some(holding_identity); bounded.holdings.items.len()];
        let mut budget = FundProductByteBudget::new()?;
        let remaining_before_identities = budget.remaining;
        bounded.bind_display_identities(
            bounded.fund_share_class_instrument_id,
            ResearchProductIdentity::try_new("Example fund", "FUND")?,
            holding_identities,
            &mut budget,
        )?;
        assert!(budget.remaining < remaining_before_identities);
        ensure_fund_product_serialized_bound(&bounded)?;
        Ok(())
    }

    fn holding(instrument_id: InstrumentId, weight: i64) -> FundHoldingProduct {
        FundHoldingProduct {
            instrument_id: Some(instrument_id),
            identity: None,
            percentage_weight: Some(Decimal::from(weight)),
            percentage_weight_conflict: false,
            quantity: FundProductValue::Missing(FundProductMissingReason::NotReported),
            value: FundProductValue::Missing(FundProductMissingReason::NotReported),
            percentage_of_net_assets: FundProductValue::Reported(
                weight.to_string().into_boxed_str(),
            ),
        }
    }

    fn fund_result(
        instrument_id: &str,
        holdings: Vec<FundHoldingProduct>,
        report_period_end: CalendarDate,
        report_date: CalendarDate,
        knowledge_cutoff: Timestamp,
    ) -> anyhow::Result<FundProductResult> {
        let coverage = FundProductCoverage {
            reports: 1,
            share_class_records: 1,
            holdings: holdings.len(),
            identified_holdings: holdings.len(),
            reported_quantities: 0,
            reported_values: 0,
            reported_weights: holdings.len(),
            missing_fields: 0,
            conflicting_fields: 0,
            other_quantity_units: 0,
            annual_reported_fields: 0,
            annual_missing_fields: 0,
            annual_conflicting_fields: 0,
            annual_limited_fields: 0,
            filing_conflicting_fields: 0,
            filing_limited_fields: 0,
        };
        let portfolio_analysis = project_portfolio_analysis(&holdings, &coverage)?;
        Ok(FundProductResult {
            fund_share_class_instrument_id: InstrumentId::from_str(instrument_id)?,
            identity: None,
            availability: FundProductAvailability::Available,
            holdings: FundHoldingsProduct {
                state: FundProductSectionState::Reported,
                filing: Some(FundFilingProduct {
                    report_period_end: FundProductValue::Reported(report_period_end),
                    report_date: FundProductValue::Reported(report_date),
                    filed_date: FundProductValue::Missing(FundProductMissingReason::Unavailable),
                    accepted_at: FundProductValue::Missing(FundProductMissingReason::Unavailable),
                    available_at: knowledge_cutoff,
                    amendment: FundProductAmendmentState::Original,
                    revision: FundProductRevisionState::Current,
                }),
                items: holdings.into_boxed_slice(),
            },
            annual_information: FundAnnualInformationProduct {
                state: FundProductSectionState::Missing,
                filing: None,
                facts: None,
            },
            current_research: FundCurrentResearchProduct {
                net_asset_value: FundNavProduct::Unavailable,
            },
            portfolio_analysis,
            clocks: FundProductClocks {
                knowledge_cutoff,
                latest_fund_information_known_at: Some(knowledge_cutoff),
            },
            coverage,
            limitations: Box::new([]),
        })
    }
}
