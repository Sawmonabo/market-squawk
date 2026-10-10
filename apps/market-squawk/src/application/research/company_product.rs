//! Provider-neutral company fundamentals and filings for ordinary product consumers.
//!
//! This leaf exposes only the canonical instrument, exact reported financial facts, code-owned
//! statement meaning, exact-input ratios, filing meaning, knowledge clocks, coverage, and honest
//! limitations. Selection and persistence evidence remain private.

use std::{
    fmt,
    io::{self, Write},
};

use market_squawk_domain::{
    CalendarDate, Currency, FundamentalAmendmentStatus, FundamentalCadence,
    FundamentalConsolidation, FundamentalPeriod, InstrumentId, ResearchTemporalCoordinate,
    RevisionNumber, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

use super::company_research::{
    CompanyFactScope, CompanyResearchDimensionState, CompanyResearchFact, CompanyResearchFiling,
    CompanyResearchFiscalPeriod, CompanyResearchOutcome, CompanyResearchRead,
    CompanyResearchRestatementState, CompanyResearchRevisionState, CompanyResearchSnapshot,
    CompanyResearchSurfaceAvailability, CompanyResearchUnavailableReason, financial_input_bit,
};
use crate::application::domain_support::{ProductTextCopyError, try_boxed_product_text};

const COMPANY_SOURCE_SURFACES: usize = 3;
const COMPANY_PRODUCT_SECTIONS: usize = 4;
const MAX_PRODUCT_FILING_FORM_BYTES: usize = 64;
const MAX_COMPANY_PRODUCT_SERIALIZED_BYTES: usize = 128 * 1024 * 1024;

/// One closed company-research result with no provider or storage vocabulary.
#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyProductResult {
    #[serde(skip)]
    instrument_id: InstrumentId,
    identity: Option<ResearchProductIdentity>,
    availability: CompanyProductAvailability,
    facts: CompanyFactsProduct,
    statements: CompanyStatementsProduct,
    ratios: CompanyRatiosProduct,
    filings: CompanyFilingsProduct,
    clocks: CompanyProductClocks,
    coverage: CompanyProductCoverage,
    limitations: Box<[CompanyProductLimitation]>,
}

impl fmt::Debug for CompanyProductResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompanyProductResult")
            .field("instrument_id", &self.instrument_id)
            .field("identity", &self.identity)
            .field("availability", &self.availability)
            .field("facts", &self.facts)
            .field("statements", &self.statements)
            .field("ratios", &self.ratios)
            .field("filings", &self.filings)
            .field("clocks", &self.clocks)
            .field("coverage", &self.coverage)
            .field("limitations", &self.limitations)
            .finish()
    }
}

impl CompanyProductResult {
    fn bind_identity(
        &mut self,
        instrument_id: InstrumentId,
        identity: ResearchProductIdentity,
    ) -> Result<(), CompanyProductProjectionError> {
        if self.instrument_id != instrument_id || self.identity.is_some() {
            return Err(CompanyProductProjectionError::InvalidEvidence);
        }
        self.identity = Some(identity);
        Ok(())
    }

    pub(crate) const fn availability(&self) -> CompanyProductAvailability {
        self.availability
    }

    pub(crate) const fn facts(&self) -> &CompanyFactsProduct {
        &self.facts
    }

    pub(crate) const fn statements(&self) -> &CompanyStatementsProduct {
        &self.statements
    }

    pub(crate) const fn ratios(&self) -> &CompanyRatiosProduct {
        &self.ratios
    }

    pub(crate) const fn filings(&self) -> &CompanyFilingsProduct {
        &self.filings
    }

    pub(crate) const fn clocks(&self) -> &CompanyProductClocks {
        &self.clocks
    }

    pub(crate) const fn coverage(&self) -> &CompanyProductCoverage {
        &self.coverage
    }

    pub(crate) fn limitations(&self) -> &[CompanyProductLimitation] {
        &self.limitations
    }
}

/// Bounded display identity resolved through exact canonical instrument authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResearchProductIdentity {
    display_name: Box<str>,
    canonical_symbol: Box<str>,
}

impl ResearchProductIdentity {
    pub(crate) fn try_new(
        display_name: &str,
        canonical_symbol: &str,
    ) -> Result<Self, CompanyProductProjectionError> {
        Ok(Self {
            display_name: try_boxed_product_text(display_name, 240)
                .map_err(map_product_text_error)?,
            canonical_symbol: try_boxed_product_text(canonical_symbol, 64)
                .map_err(map_product_text_error)?,
        })
    }
}

/// Overall availability of the requested company information.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyProductAvailability {
    Available,
    Partial,
    Missing,
    Conflict,
    Unavailable,
}

/// Availability of one ordinary product section.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyProductSectionState {
    Reported,
    Missing,
    Conflict,
    Unavailable,
}

/// Exact reported facts and their aggregate state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyFactsProduct {
    state: CompanyProductSectionState,
    items: Box<[CompanyFactProduct]>,
}

impl CompanyFactsProduct {
    pub(crate) const fn state(&self) -> CompanyProductSectionState {
        self.state
    }

    pub(crate) fn items(&self) -> &[CompanyFactProduct] {
        &self.items
    }
}

/// One exact reported financial fact. Missing and conflict states remain section-level because
/// the canonical read does not invent absent taxonomy coordinates.
#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyFactProduct {
    #[serde(skip)]
    lineage: CompanyFactPrivateLineage,
    scope: CompanyFactProductScope,
    revision: CompanyProductRevisionState,
    metric: CompanyFinancialMetric,
    display_name: &'static str,
    value: Decimal,
    unit: CompanyFactUnit,
    period: FundamentalPeriod,
    fiscal_context: CompanyFactFiscalContext,
    reporting_context: CompanyFactReportingContext,
    filed_on: Option<CalendarDate>,
    effective: CompanyProductTime,
    known_at: Timestamp,
}

#[derive(Clone, Eq, PartialEq)]
struct CompanyFactPrivateLineage {
    filing_identity: Box<str>,
    publication_identity: [u8; 32],
    xbrl_identity: Option<(SourceIdentifier, SourceIdentifier)>,
    nonnumeric_inputs: Option<u16>,
}

impl fmt::Debug for CompanyFactPrivateLineage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[PRIVATE FACT LINEAGE]")
    }
}

impl fmt::Debug for CompanyFactProduct {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompanyFactProduct")
            .field("scope", &self.scope)
            .field("revision", &self.revision)
            .field("metric", &self.metric)
            .field("display_name", &self.display_name)
            .field("value", &self.value)
            .field("unit", &self.unit)
            .field("period", &self.period)
            .field("fiscal_context", &self.fiscal_context)
            .field("reporting_context", &self.reporting_context)
            .field("filed_on", &self.filed_on)
            .field("effective", &self.effective)
            .field("known_at", &self.known_at)
            .finish()
    }
}

impl CompanyFactProduct {
    pub(crate) const fn scope(&self) -> CompanyFactProductScope {
        self.scope
    }

    pub(crate) const fn revision(&self) -> CompanyProductRevisionState {
        self.revision
    }

    pub(crate) const fn metric(&self) -> CompanyFinancialMetric {
        self.metric
    }

    pub(crate) const fn display_name(&self) -> &'static str {
        self.display_name
    }

    pub(crate) const fn value(&self) -> Decimal {
        self.value
    }

    pub(crate) const fn unit(&self) -> CompanyFactUnit {
        self.unit
    }

    pub(crate) const fn period(&self) -> FundamentalPeriod {
        self.period
    }

    pub(crate) const fn fiscal_context(&self) -> CompanyFactFiscalContext {
        self.fiscal_context
    }

    pub(crate) const fn reporting_context(&self) -> CompanyFactReportingContext {
        self.reporting_context
    }

    pub(crate) const fn filed_on(&self) -> Option<CalendarDate> {
        self.filed_on
    }

    pub(crate) const fn effective(&self) -> &CompanyProductTime {
        &self.effective
    }

    pub(crate) const fn known_at(&self) -> Timestamp {
        self.known_at
    }
}

/// Code-owned financial meaning for the deliberately bounded Product V1 fact set.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyFinancialMetric {
    CashAndCashEquivalents,
    AccountsReceivableNetCurrent,
    InventoryNet,
    CurrentAssets,
    TotalAssets,
    CurrentLiabilities,
    TotalLiabilities,
    CurrentLongTermDebt,
    NoncurrentLongTermDebt,
    ShareholdersEquity,
    TotalEquityIncludingNoncontrollingInterests,
    Revenue,
    NetSales,
    CustomerRevenueExcludingAssessedTax,
    CostOfRevenue,
    GrossProfit,
    OperatingExpenses,
    OperatingIncome,
    NetIncome,
    CommonNetIncome,
    PreferredDividendsAndAdjustments,
    ProfitOrLossIncludingNoncontrollingInterests,
    BasicEarningsPerShare,
    DilutedEarningsPerShare,
    OperatingCashFlow,
    InvestingCashFlow,
    FinancingCashFlow,
    PropertyPlantAndEquipmentPurchases,
    LongTermBorrowingProceeds,
    LongTermDebtRepayments,
    PreferredDividendsPaid,
    PreferredStockIssuedValue,
    EntityCommonSharesOutstanding,
    CommonStockSharesOutstanding,
    WeightedAverageBasicShares,
    WeightedAverageDilutedShares,
}

impl CompanyFinancialMetric {
    pub(crate) const fn display_name(self) -> &'static str {
        match self {
            Self::CashAndCashEquivalents => "Cash and cash equivalents",
            Self::AccountsReceivableNetCurrent => "Current accounts receivable, net",
            Self::InventoryNet => "Inventory, net",
            Self::CurrentAssets => "Current assets",
            Self::TotalAssets => "Total assets",
            Self::CurrentLiabilities => "Current liabilities",
            Self::TotalLiabilities => "Total liabilities",
            Self::CurrentLongTermDebt => "Current portion of long-term debt",
            Self::NoncurrentLongTermDebt => "Long-term debt, noncurrent",
            Self::ShareholdersEquity => "Shareholders' equity",
            Self::TotalEquityIncludingNoncontrollingInterests => {
                "Total equity including noncontrolling interests"
            }
            Self::Revenue => "Revenue",
            Self::NetSales => "Net sales",
            Self::CustomerRevenueExcludingAssessedTax => "Customer revenue excluding assessed tax",
            Self::CostOfRevenue => "Cost of revenue",
            Self::GrossProfit => "Gross profit",
            Self::OperatingExpenses => "Operating expenses",
            Self::OperatingIncome => "Operating income or loss",
            Self::NetIncome => "Net income or loss attributable to parent",
            Self::CommonNetIncome => "Net income or loss available to common stockholders",
            Self::PreferredDividendsAndAdjustments => {
                "Preferred dividends and other income adjustments"
            }
            Self::ProfitOrLossIncludingNoncontrollingInterests => {
                "Profit or loss including noncontrolling interests"
            }
            Self::BasicEarningsPerShare => "Basic earnings per share",
            Self::DilutedEarningsPerShare => "Diluted earnings per share",
            Self::OperatingCashFlow => "Operating cash flow",
            Self::InvestingCashFlow => "Investing cash flow",
            Self::FinancingCashFlow => "Financing cash flow",
            Self::PropertyPlantAndEquipmentPurchases => "Property, plant, and equipment purchases",
            Self::LongTermBorrowingProceeds => "Proceeds from long-term borrowing",
            Self::LongTermDebtRepayments => "Repayments of long-term debt",
            Self::PreferredDividendsPaid => "Preferred dividends paid",
            Self::PreferredStockIssuedValue => "Preferred stock issued value",
            Self::EntityCommonSharesOutstanding => "Entity common shares outstanding",
            Self::CommonStockSharesOutstanding => "Common stock shares outstanding",
            Self::WeightedAverageBasicShares => "Weighted-average basic shares",
            Self::WeightedAverageDilutedShares => "Weighted-average diluted shares",
        }
    }

    const fn expected_unit(self) -> CompanyMetricUnit {
        match self {
            Self::BasicEarningsPerShare | Self::DilutedEarningsPerShare => {
                CompanyMetricUnit::CurrencyPerShare
            }
            Self::EntityCommonSharesOutstanding
            | Self::CommonStockSharesOutstanding
            | Self::WeightedAverageBasicShares
            | Self::WeightedAverageDilutedShares => CompanyMetricUnit::Shares,
            _ => CompanyMetricUnit::Currency,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompanyMetricUnit {
    Currency,
    Shares,
    CurrencyPerShare,
}

/// Product-semantic unit; source unit keys never cross this boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(crate) enum CompanyFactUnit {
    Currency { currency: Currency },
    Shares,
    CurrencyPerShare { currency: Currency },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyFactFiscalContext {
    fiscal_year: Option<u16>,
    fiscal_period: CompanyFactFiscalPeriod,
    cadence: CompanyFactCadence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyFactFiscalPeriod {
    FiscalYear,
    CalendarYear,
    FirstQuarter,
    SecondQuarter,
    ThirdQuarter,
    FourthQuarter,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyFactCadence {
    Annual,
    Quarterly,
    Other,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyFactReportingContext {
    dimensionality: CompanyFactDimensionality,
    consolidation: CompanyFactConsolidation,
    amendment: CompanyFactAmendment,
    restatement: CompanyFactRestatement,
    occurrence: RevisionNumber,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyFactDimensionality {
    Unavailable,
    NoDimensions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyFactConsolidation {
    ReportedConsolidated,
    ReportedNonConsolidated,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyFactAmendment {
    Original,
    Amendment,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyFactRestatement {
    ReportedRestated,
    ReportedNotRestated,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyFactProductScope {
    CompanyWide,
    FilingDetail,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyProductRevisionState {
    Current,
    Superseded,
    IncomparableHistory,
}

/// Exact reporting envelope shared by one statement group or ratio calculation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyReportingEnvelopeProduct {
    scope: CompanyFactProductScope,
    period: FundamentalPeriod,
    fiscal_context: CompanyFactFiscalContext,
    reporting_context: CompanyEnvelopeReportingContext,
    filed_on: Option<CalendarDate>,
    effective: CompanyProductTime,
    known_at: Timestamp,
}

/// A reporting context contains multiple original fact occurrences.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CompanyEnvelopeReportingContext {
    dimensionality: CompanyFactDimensionality,
    consolidation: CompanyFactConsolidation,
    amendment: CompanyFactAmendment,
    restatement: CompanyFactRestatement,
}

/// Exact reported facts grouped only inside one reporting and filing envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyStatementsProduct {
    state: CompanyProductSectionState,
    groups: Box<[CompanyStatementGroupProduct]>,
}

impl CompanyStatementsProduct {
    pub(crate) const fn state(&self) -> CompanyProductSectionState {
        self.state
    }

    pub(crate) fn groups(&self) -> &[CompanyStatementGroupProduct] {
        &self.groups
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyStatementGroupProduct {
    statement: CompanyStatementKind,
    envelope: CompanyReportingEnvelopeProduct,
    items: Box<[CompanyFactProduct]>,
}

impl CompanyStatementGroupProduct {
    pub(crate) const fn statement(&self) -> CompanyStatementKind {
        self.statement
    }

    pub(crate) const fn envelope(&self) -> CompanyReportingEnvelopeProduct {
        self.envelope
    }

    pub(crate) fn items(&self) -> &[CompanyFactProduct] {
        &self.items
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyStatementKind {
    FinancialPosition,
    Operations,
    CashFlows,
    ShareData,
}

/// Deterministic ratios with an explicit outcome and complete exact input lineage.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyRatiosProduct {
    state: CompanyProductSectionState,
    items: Box<[CompanyRatioProduct]>,
}

impl CompanyRatiosProduct {
    pub(crate) const fn state(&self) -> CompanyProductSectionState {
        self.state
    }

    pub(crate) fn items(&self) -> &[CompanyRatioProduct] {
        &self.items
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyRatioProduct {
    metric: CompanyRatioMetric,
    display_name: &'static str,
    state: CompanyRatioState,
    value: Option<Decimal>,
    unit: CompanyRatioUnit,
    envelope: Option<CompanyReportingEnvelopeProduct>,
    inputs: Box<[CompanyRatioInputProduct]>,
}

impl CompanyRatioProduct {
    pub(crate) const fn metric(&self) -> CompanyRatioMetric {
        self.metric
    }

    pub(crate) const fn state(&self) -> CompanyRatioState {
        self.state
    }

    pub(crate) const fn value(&self) -> Option<Decimal> {
        self.value
    }

    pub(crate) const fn envelope(&self) -> Option<CompanyReportingEnvelopeProduct> {
        self.envelope
    }

    pub(crate) fn inputs(&self) -> &[CompanyRatioInputProduct] {
        &self.inputs
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyRatioMetric {
    CurrentRatio,
    GrossMargin,
    OperatingMargin,
    NetMargin,
}

impl CompanyRatioMetric {
    const fn display_name(self) -> &'static str {
        match self {
            Self::CurrentRatio => "Current ratio",
            Self::GrossMargin => "Gross margin",
            Self::OperatingMargin => "Operating margin",
            Self::NetMargin => "Net margin",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyRatioUnit {
    Ratio,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyRatioState {
    Reported,
    MissingInput,
    ConflictingInput,
    IncompatibleUnits,
    ZeroDenominator,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyRatioInputProduct {
    role: CompanyRatioInputRole,
    fact: CompanyFactProduct,
}

impl CompanyRatioInputProduct {
    pub(crate) const fn role(&self) -> CompanyRatioInputRole {
        self.role
    }

    pub(crate) const fn fact(&self) -> &CompanyFactProduct {
        &self.fact
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyRatioInputRole {
    Numerator,
    Denominator,
}

/// Filing events stripped of source-native filing coordinates.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyFilingsProduct {
    state: CompanyProductSectionState,
    items: Box<[CompanyFilingProduct]>,
}

impl CompanyFilingsProduct {
    pub(crate) const fn state(&self) -> CompanyProductSectionState {
        self.state
    }

    pub(crate) fn items(&self) -> &[CompanyFilingProduct] {
        &self.items
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyFilingProduct {
    revision: CompanyProductRevisionState,
    form: Box<str>,
    effective: CompanyProductTime,
    published: Option<CompanyProductTime>,
    known_at: Timestamp,
}

impl CompanyFilingProduct {
    pub(crate) const fn revision(&self) -> CompanyProductRevisionState {
        self.revision
    }

    pub(crate) fn form(&self) -> &str {
        &self.form
    }

    pub(crate) const fn effective(&self) -> &CompanyProductTime {
        &self.effective
    }

    pub(crate) const fn published(&self) -> Option<&CompanyProductTime> {
        self.published.as_ref()
    }

    pub(crate) const fn known_at(&self) -> Timestamp {
        self.known_at
    }
}

/// Product-relevant knowledge and effective-time coordinates.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyProductClocks {
    knowledge_cutoff: Timestamp,
    fact_effective_cutoff: CompanyProductTime,
    latest_known_at: Option<Timestamp>,
}

impl CompanyProductClocks {
    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    pub(crate) const fn fact_effective_cutoff(&self) -> &CompanyProductTime {
        &self.fact_effective_cutoff
    }

    pub(crate) const fn latest_known_at(&self) -> Option<Timestamp> {
        self.latest_known_at
    }
}

/// Exact research time without internal schema or source-period coordinates.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "precision", content = "value", rename_all = "snake_case")]
pub(crate) enum CompanyProductTime {
    Timestamp(Timestamp),
    CalendarDate(CalendarDate),
}

/// Honest materialized coverage without storage or source coordinates.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyProductCoverage {
    requested_sections: usize,
    available_sections: usize,
    reported_facts: usize,
    omitted_facts: usize,
    statement_lines: usize,
    evaluated_ratios: usize,
    reported_ratios: usize,
    filing_events: usize,
}

impl CompanyProductCoverage {
    pub(crate) const fn requested_sections(self) -> usize {
        self.requested_sections
    }

    pub(crate) const fn available_sections(self) -> usize {
        self.available_sections
    }

    pub(crate) const fn reported_facts(self) -> usize {
        self.reported_facts
    }

    pub(crate) const fn omitted_facts(self) -> usize {
        self.omitted_facts
    }

    pub(crate) const fn statement_lines(self) -> usize {
        self.statement_lines
    }

    pub(crate) const fn evaluated_ratios(self) -> usize {
        self.evaluated_ratios
    }

    pub(crate) const fn reported_ratios(self) -> usize {
        self.reported_ratios
    }

    pub(crate) const fn filing_events(self) -> usize {
        self.filing_events
    }
}

/// Closed limitations suitable for plain-language presentation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyProductLimitation {
    SomeCompanyInformationMissing,
    NoCompanyInformationAtCutoff,
    IdentityAmbiguous,
    IdentityUnavailable,
    RevisionConflict,
    SomeReportedCompanyFactsNotShown,
    NoSupportedCompanyInformationToShow,
    NoSupportedRatiosAtCutoff,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum CompanyProductProjectionError {
    #[error("company research evidence is inconsistent")]
    InvalidEvidence,
    #[error("company research projection exceeded its fixed resource bound")]
    ResourceExhausted,
}

struct CompanySerializedBudget {
    remaining: usize,
}

impl CompanySerializedBudget {
    const fn new() -> Self {
        Self {
            remaining: MAX_COMPANY_PRODUCT_SERIALIZED_BYTES,
        }
    }

    fn charge<T: Serialize>(&mut self, value: &T) -> Result<(), CompanyProductProjectionError> {
        let bytes = serialized_bytes_with_limit(value, self.remaining)?;
        self.remaining = self
            .remaining
            .checked_sub(bytes)
            .ok_or(CompanyProductProjectionError::ResourceExhausted)?;
        Ok(())
    }
}

struct BoundedCountingWriter {
    remaining: usize,
    written: usize,
    exhausted: bool,
}

impl Write for BoundedCountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > self.remaining {
            self.exhausted = true;
            return Err(io::Error::other(
                "company product serialization bound exceeded",
            ));
        }
        self.remaining -= buffer.len();
        self.written = self
            .written
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("company product serialization length overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_bytes_with_limit<T: Serialize>(
    value: &T,
    limit: usize,
) -> Result<usize, CompanyProductProjectionError> {
    let mut writer = BoundedCountingWriter {
        remaining: limit,
        written: 0,
        exhausted: false,
    };
    if serde_json::to_writer(&mut writer, value).is_err() {
        return Err(if writer.exhausted {
            CompanyProductProjectionError::ResourceExhausted
        } else {
            CompanyProductProjectionError::InvalidEvidence
        });
    }
    Ok(writer.written)
}

fn ensure_company_product_serialized_bound(
    product: &CompanyProductResult,
) -> Result<(), CompanyProductProjectionError> {
    serialized_bytes_with_limit(product, MAX_COMPANY_PRODUCT_SERIALIZED_BYTES).map(|_| ())
}

/// Projects a verified canonical read without exposing its private evidence receipts.
pub(crate) fn project_company_product(
    read: &CompanyResearchRead,
    identity: ResearchProductIdentity,
) -> Result<CompanyProductResult, CompanyProductProjectionError> {
    let request = read.request();
    let fact_effective_cutoff = product_time(request.fact_effective_cutoff())?;
    let mut result = match read.outcome() {
        CompanyResearchOutcome::Available(snapshot) => project_snapshot(
            request.instrument_id(),
            request.knowledge_at(),
            fact_effective_cutoff,
            snapshot,
            false,
        ),
        CompanyResearchOutcome::Partial(snapshot) => project_snapshot(
            request.instrument_id(),
            request.knowledge_at(),
            fact_effective_cutoff,
            snapshot,
            true,
        ),
        CompanyResearchOutcome::Missing => empty_result(
            request.instrument_id(),
            request.knowledge_at(),
            fact_effective_cutoff,
            CompanyProductAvailability::Missing,
            CompanyProductSectionState::Missing,
            CompanyProductLimitation::NoCompanyInformationAtCutoff,
        ),
        CompanyResearchOutcome::Ambiguous => empty_result(
            request.instrument_id(),
            request.knowledge_at(),
            fact_effective_cutoff,
            CompanyProductAvailability::Conflict,
            CompanyProductSectionState::Conflict,
            CompanyProductLimitation::IdentityAmbiguous,
        ),
        CompanyResearchOutcome::Unavailable(reason) => {
            let (availability, section, limitation) = match reason {
                CompanyResearchUnavailableReason::ConflictingRevisionEvidence => (
                    CompanyProductAvailability::Conflict,
                    CompanyProductSectionState::Conflict,
                    CompanyProductLimitation::RevisionConflict,
                ),
                CompanyResearchUnavailableReason::StaleIdentity
                | CompanyResearchUnavailableReason::RevokedIdentity
                | CompanyResearchUnavailableReason::ConflictingIdentityState => (
                    CompanyProductAvailability::Unavailable,
                    CompanyProductSectionState::Unavailable,
                    CompanyProductLimitation::IdentityUnavailable,
                ),
            };
            empty_result(
                request.instrument_id(),
                request.knowledge_at(),
                fact_effective_cutoff,
                availability,
                section,
                limitation,
            )
        }
    }?;
    result.bind_identity(request.instrument_id(), identity)?;
    ensure_company_product_serialized_bound(&result)?;
    Ok(result)
}

/// Exact private grouping key, shared with the disk-backed selected-investment reader.
pub(crate) fn fact_envelope_bytes(
    fact: &CompanyFactProduct,
) -> Result<Vec<u8>, CompanyProductProjectionError> {
    serde_json::to_vec(&fact_envelope_key(fact))
        .map_err(|_| CompanyProductProjectionError::InvalidEvidence)
}

/// Receives one complete reporting envelope, never a page of arbitrary source facts.
pub(crate) fn project_financial_envelope(
    facts: &[CompanyFactProduct],
    ratios: bool,
) -> Result<Vec<serde_json::Value>, CompanyProductProjectionError> {
    if facts.is_empty()
        || facts
            .iter()
            .any(|fact| fact_envelope_key(fact) != fact_envelope_key(&facts[0]))
    {
        return Err(CompanyProductProjectionError::InvalidEvidence);
    }
    let mut budget = CompanySerializedBudget::new();
    if ratios {
        project_ratios(facts, CompanyProductSectionState::Reported, &mut budget)?
            .items
            .iter()
            .map(|item| {
                serde_json::to_value(item)
                    .map_err(|_| CompanyProductProjectionError::InvalidEvidence)
            })
            .collect()
    } else {
        project_statements(facts, CompanyProductSectionState::Reported, &mut budget)?
            .groups
            .iter()
            .map(|item| {
                serde_json::to_value(item)
                    .map_err(|_| CompanyProductProjectionError::InvalidEvidence)
            })
            .collect()
    }
}

fn project_snapshot(
    instrument_id: InstrumentId,
    knowledge_cutoff: Timestamp,
    fact_effective_cutoff: CompanyProductTime,
    snapshot: &CompanyResearchSnapshot,
    partial: bool,
) -> Result<CompanyProductResult, CompanyProductProjectionError> {
    if snapshot.instrument_id() != instrument_id || snapshot.as_of() != knowledge_cutoff {
        return Err(CompanyProductProjectionError::InvalidEvidence);
    }
    let mut budget = CompanySerializedBudget::new();

    let mut facts = Vec::new();
    facts
        .try_reserve_exact(snapshot.facts().len())
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    let mut omitted_facts = 0_usize;
    for fact in snapshot.facts() {
        if let Some(fact) = project_fact(fact, knowledge_cutoff)? {
            budget.charge(&fact)?;
            facts.push(fact);
        } else {
            omitted_facts = omitted_facts
                .checked_add(1)
                .ok_or(CompanyProductProjectionError::ResourceExhausted)?;
        }
    }

    let mut filings = Vec::new();
    filings
        .try_reserve_exact(snapshot.filing_events().len())
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    for filing in snapshot.filing_events() {
        let filing = project_filing(filing, knowledge_cutoff)?;
        budget.charge(&filing)?;
        filings.push(filing);
    }

    let available_sections = [
        snapshot.company_facts(),
        snapshot.filings(),
        snapshot.filing_details(),
    ]
    .into_iter()
    .filter(|state| *state == CompanyResearchSurfaceAvailability::Available)
    .count();
    let complete_source_sections = available_sections == COMPANY_SOURCE_SURFACES;
    if complete_source_sections == partial {
        return Err(CompanyProductProjectionError::InvalidEvidence);
    }

    let fact_source_available = snapshot.company_facts()
        == CompanyResearchSurfaceAvailability::Available
        || snapshot.filing_details() == CompanyResearchSurfaceAvailability::Available;
    let facts_state = if !fact_source_available {
        CompanyProductSectionState::Missing
    } else if facts.is_empty() {
        CompanyProductSectionState::Unavailable
    } else {
        CompanyProductSectionState::Reported
    };
    let filings_state = if snapshot.filings() == CompanyResearchSurfaceAvailability::Available
        && !filings.is_empty()
    {
        CompanyProductSectionState::Reported
    } else {
        CompanyProductSectionState::Missing
    };

    let statements = project_statements(&facts, facts_state, &mut budget)?;
    let ratios = project_ratios(&facts, facts_state, &mut budget)?;
    let mut limitations = Vec::new();
    limitations
        .try_reserve_exact(4)
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    if partial {
        limitations.push(CompanyProductLimitation::SomeCompanyInformationMissing);
    }
    if omitted_facts != 0 {
        limitations.push(CompanyProductLimitation::SomeReportedCompanyFactsNotShown);
    }
    if facts_state == CompanyProductSectionState::Reported
        && ratios.state != CompanyProductSectionState::Reported
    {
        limitations.push(CompanyProductLimitation::NoSupportedRatiosAtCutoff);
    }
    let available_product_sections = [facts_state, statements.state, ratios.state, filings_state]
        .into_iter()
        .filter(|state| *state == CompanyProductSectionState::Reported)
        .count();
    let availability =
        company_product_availability(available_product_sections, complete_source_sections);
    if availability == CompanyProductAvailability::Unavailable {
        limitations.push(CompanyProductLimitation::NoSupportedCompanyInformationToShow);
    }
    let reported_facts = facts.len();
    let statement_lines = statements
        .groups
        .iter()
        .try_fold(0_usize, |count, group| count.checked_add(group.items.len()))
        .ok_or(CompanyProductProjectionError::ResourceExhausted)?;
    let evaluated_ratios = ratios.items.len();
    let reported_ratios = ratios
        .items
        .iter()
        .filter(|ratio| ratio.state == CompanyRatioState::Reported)
        .count();
    Ok(CompanyProductResult {
        instrument_id,
        identity: None,
        availability,
        facts: CompanyFactsProduct {
            state: facts_state,
            items: facts.into_boxed_slice(),
        },
        statements,
        ratios,
        filings: CompanyFilingsProduct {
            state: filings_state,
            items: filings.into_boxed_slice(),
        },
        clocks: CompanyProductClocks {
            knowledge_cutoff,
            fact_effective_cutoff,
            latest_known_at: snapshot.latest_known_at(),
        },
        coverage: CompanyProductCoverage {
            requested_sections: COMPANY_PRODUCT_SECTIONS,
            available_sections: available_product_sections,
            reported_facts,
            omitted_facts,
            statement_lines,
            evaluated_ratios,
            reported_ratios,
            filing_events: snapshot.filing_events().len(),
        },
        limitations: limitations.into_boxed_slice(),
    })
}

const fn company_product_availability(
    available_product_sections: usize,
    complete_selected_input_coverage: bool,
) -> CompanyProductAvailability {
    match available_product_sections {
        0 => CompanyProductAvailability::Unavailable,
        COMPANY_PRODUCT_SECTIONS if complete_selected_input_coverage => {
            CompanyProductAvailability::Available
        }
        _ => CompanyProductAvailability::Partial,
    }
}

fn project_statements(
    facts: &[CompanyFactProduct],
    source_state: CompanyProductSectionState,
    budget: &mut CompanySerializedBudget,
) -> Result<CompanyStatementsProduct, CompanyProductProjectionError> {
    if source_state != CompanyProductSectionState::Reported {
        return Ok(CompanyStatementsProduct {
            state: source_state,
            groups: Box::new([]),
        });
    }
    if facts.is_empty() {
        return Err(CompanyProductProjectionError::InvalidEvidence);
    }

    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(facts.len())
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    for fact in facts {
        candidates.push(EnvelopeFactRef {
            key: fact_envelope_key(fact),
            fact,
        });
    }
    candidates.sort_unstable_by_key(|candidate| candidate.key);

    let mut groups = Vec::new();
    let mut start = 0_usize;
    while start < candidates.len() {
        let key = candidates[start].key;
        let mut end = start + 1;
        while end < candidates.len() && candidates[end].key == key {
            end += 1;
        }
        let envelope = &candidates[start..end];
        for statement in [
            CompanyStatementKind::FinancialPosition,
            CompanyStatementKind::Operations,
            CompanyStatementKind::CashFlows,
            CompanyStatementKind::ShareData,
        ] {
            append_statement_group(&mut groups, envelope, statement, budget)?;
        }
        start = end;
    }
    Ok(CompanyStatementsProduct {
        state: CompanyProductSectionState::Reported,
        groups: groups.into_boxed_slice(),
    })
}

fn append_statement_group(
    groups: &mut Vec<CompanyStatementGroupProduct>,
    envelope: &[EnvelopeFactRef<'_>],
    statement: CompanyStatementKind,
    budget: &mut CompanySerializedBudget,
) -> Result<(), CompanyProductProjectionError> {
    let mut items = Vec::new();
    for candidate in envelope
        .iter()
        .filter(|candidate| statement_for_metric(candidate.fact.metric) == statement)
    {
        items
            .try_reserve(1)
            .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
        items.push(candidate.fact.clone());
    }
    if items.is_empty() {
        return Ok(());
    }
    let product = CompanyStatementGroupProduct {
        statement,
        envelope: reporting_envelope(envelope[0].fact),
        items: items.into_boxed_slice(),
    };
    budget.charge(&product)?;
    groups
        .try_reserve(1)
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    groups.push(product);
    Ok(())
}

const fn statement_for_metric(metric: CompanyFinancialMetric) -> CompanyStatementKind {
    match metric {
        CompanyFinancialMetric::CashAndCashEquivalents
        | CompanyFinancialMetric::AccountsReceivableNetCurrent
        | CompanyFinancialMetric::InventoryNet
        | CompanyFinancialMetric::CurrentAssets
        | CompanyFinancialMetric::TotalAssets
        | CompanyFinancialMetric::CurrentLiabilities
        | CompanyFinancialMetric::TotalLiabilities
        | CompanyFinancialMetric::CurrentLongTermDebt
        | CompanyFinancialMetric::NoncurrentLongTermDebt
        | CompanyFinancialMetric::ShareholdersEquity
        | CompanyFinancialMetric::PreferredStockIssuedValue
        | CompanyFinancialMetric::TotalEquityIncludingNoncontrollingInterests => {
            CompanyStatementKind::FinancialPosition
        }
        CompanyFinancialMetric::Revenue
        | CompanyFinancialMetric::NetSales
        | CompanyFinancialMetric::CustomerRevenueExcludingAssessedTax
        | CompanyFinancialMetric::CostOfRevenue
        | CompanyFinancialMetric::GrossProfit
        | CompanyFinancialMetric::OperatingExpenses
        | CompanyFinancialMetric::OperatingIncome
        | CompanyFinancialMetric::NetIncome
        | CompanyFinancialMetric::CommonNetIncome
        | CompanyFinancialMetric::PreferredDividendsAndAdjustments
        | CompanyFinancialMetric::ProfitOrLossIncludingNoncontrollingInterests
        | CompanyFinancialMetric::BasicEarningsPerShare
        | CompanyFinancialMetric::DilutedEarningsPerShare => CompanyStatementKind::Operations,
        CompanyFinancialMetric::OperatingCashFlow
        | CompanyFinancialMetric::InvestingCashFlow
        | CompanyFinancialMetric::FinancingCashFlow
        | CompanyFinancialMetric::LongTermBorrowingProceeds
        | CompanyFinancialMetric::LongTermDebtRepayments
        | CompanyFinancialMetric::PreferredDividendsPaid
        | CompanyFinancialMetric::PropertyPlantAndEquipmentPurchases => {
            CompanyStatementKind::CashFlows
        }
        CompanyFinancialMetric::EntityCommonSharesOutstanding
        | CompanyFinancialMetric::CommonStockSharesOutstanding
        | CompanyFinancialMetric::WeightedAverageBasicShares
        | CompanyFinancialMetric::WeightedAverageDilutedShares => CompanyStatementKind::ShareData,
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct CompanyFactEnvelopeKey<'fact> {
    filing_identity: &'fact str,
    publication_identity: [u8; 32],
    scope: u8,
    revision: u8,
    period_kind: u8,
    period_start: i32,
    period_end: i32,
    fiscal_year_present: bool,
    fiscal_year: u16,
    fiscal_period: u8,
    cadence: u8,
    dimensionality: u8,
    consolidation: u8,
    amendment: u8,
    restatement: u8,
    // Per-concept revision ordinals belong to facts, not the shared reporting context.
    xbrl_context_identity: Option<&'fact str>,
    filed_on_present: bool,
    filed_on: i32,
    effective_kind: u8,
    effective_value: i64,
    known_at: i64,
}

#[derive(Clone, Copy)]
struct EnvelopeFactRef<'fact> {
    key: CompanyFactEnvelopeKey<'fact>,
    fact: &'fact CompanyFactProduct,
}

fn project_ratios(
    facts: &[CompanyFactProduct],
    source_state: CompanyProductSectionState,
    budget: &mut CompanySerializedBudget,
) -> Result<CompanyRatiosProduct, CompanyProductProjectionError> {
    if source_state != CompanyProductSectionState::Reported {
        return unavailable_ratio_set(source_state, budget);
    }

    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(facts.len())
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    // Statement meaning supplies candidate envelopes; exact operand presence below
    // determines which ratios apply. Unrelated notes retain their facts and statements.
    for fact in facts.iter().filter(|fact| {
        matches!(
            (fact.period, statement_for_metric(fact.metric)),
            (
                FundamentalPeriod::Instant { .. },
                CompanyStatementKind::FinancialPosition
            ) | (
                FundamentalPeriod::Duration { .. },
                CompanyStatementKind::Operations
            )
        )
    }) {
        candidates.push(EnvelopeFactRef {
            key: fact_envelope_key(fact),
            fact,
        });
    }
    candidates.sort_unstable_by_key(|candidate| candidate.key);
    let mut ratios = Vec::new();
    let mut start = 0_usize;
    while start < candidates.len() {
        let key = candidates[start].key;
        let mut end = start + 1;
        while end < candidates.len() && candidates[end].key == key {
            end += 1;
        }
        let group = &candidates[start..end];
        match group[0].fact.period {
            FundamentalPeriod::Instant { .. } => append_ratio(
                &mut ratios,
                group,
                CompanyRatioMetric::CurrentRatio,
                &[CompanyFinancialMetric::CurrentAssets],
                &[CompanyFinancialMetric::CurrentLiabilities],
                budget,
            )?,
            FundamentalPeriod::Duration { .. } => {
                let revenue = revenue_metrics(group);
                // Parent-attributable and consolidated income are different bases, not
                // conflicting observations. Presence selects the basis before validation.
                let net_income = if group.iter().any(|candidate| {
                    reports_input(candidate.fact, CompanyFinancialMetric::NetIncome)
                }) {
                    CompanyFinancialMetric::NetIncome
                } else {
                    CompanyFinancialMetric::ProfitOrLossIncludingNoncontrollingInterests
                };
                append_ratio(
                    &mut ratios,
                    group,
                    CompanyRatioMetric::GrossMargin,
                    &[CompanyFinancialMetric::GrossProfit],
                    revenue,
                    budget,
                )?;
                append_ratio(
                    &mut ratios,
                    group,
                    CompanyRatioMetric::OperatingMargin,
                    &[CompanyFinancialMetric::OperatingIncome],
                    revenue,
                    budget,
                )?;
                append_ratio(
                    &mut ratios,
                    group,
                    CompanyRatioMetric::NetMargin,
                    &[net_income],
                    revenue,
                    budget,
                )?;
            }
        }
        start = end;
    }
    let state = if ratios
        .iter()
        .any(|ratio| ratio.state == CompanyRatioState::Reported)
    {
        CompanyProductSectionState::Reported
    } else {
        CompanyProductSectionState::Unavailable
    };
    Ok(CompanyRatiosProduct {
        state,
        items: ratios.into_boxed_slice(),
    })
}

fn unavailable_ratio_set(
    state: CompanyProductSectionState,
    budget: &mut CompanySerializedBudget,
) -> Result<CompanyRatiosProduct, CompanyProductProjectionError> {
    let mut items = Vec::new();
    items
        .try_reserve_exact(4)
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    for metric in [
        CompanyRatioMetric::CurrentRatio,
        CompanyRatioMetric::GrossMargin,
        CompanyRatioMetric::OperatingMargin,
        CompanyRatioMetric::NetMargin,
    ] {
        let product = CompanyRatioProduct {
            metric,
            display_name: metric.display_name(),
            state: CompanyRatioState::Unavailable,
            value: None,
            unit: CompanyRatioUnit::Ratio,
            envelope: None,
            inputs: Box::new([]),
        };
        budget.charge(&product)?;
        items.push(product);
    }
    Ok(CompanyRatiosProduct {
        state,
        items: items.into_boxed_slice(),
    })
}

fn revenue_metrics(group: &[EnvelopeFactRef<'_>]) -> &'static [CompanyFinancialMetric] {
    // Total revenue takes precedence over narrower revenue concepts, regardless of their
    // values. An invalid selected total must not fall back to a different financial basis.
    if group
        .iter()
        .any(|candidate| reports_input(candidate.fact, CompanyFinancialMetric::Revenue))
    {
        &[CompanyFinancialMetric::Revenue]
    } else {
        // Either concept alone can supply the fallback; both together remain ambiguous.
        &[
            CompanyFinancialMetric::CustomerRevenueExcludingAssessedTax,
            CompanyFinancialMetric::NetSales,
        ]
    }
}

fn input_bit(metric: CompanyFinancialMetric) -> u16 {
    financial_input_bit(match metric {
        CompanyFinancialMetric::CurrentAssets => "AssetsCurrent",
        CompanyFinancialMetric::CurrentLiabilities => "LiabilitiesCurrent",
        CompanyFinancialMetric::Revenue => "Revenues",
        CompanyFinancialMetric::NetSales => "SalesRevenueNet",
        CompanyFinancialMetric::CustomerRevenueExcludingAssessedTax => {
            "RevenueFromContractWithCustomerExcludingAssessedTax"
        }
        CompanyFinancialMetric::GrossProfit => "GrossProfit",
        CompanyFinancialMetric::OperatingIncome => "OperatingIncomeLoss",
        CompanyFinancialMetric::NetIncome => "NetIncomeLoss",
        CompanyFinancialMetric::ProfitOrLossIncludingNoncontrollingInterests => "ProfitLoss",
        _ => "",
    })
}

fn reports_input(fact: &CompanyFactProduct, metric: CompanyFinancialMetric) -> bool {
    fact.metric == metric
        || fact
            .lineage
            .nonnumeric_inputs
            .is_some_and(|inputs| inputs & input_bit(metric) != 0)
}

fn append_ratio(
    ratios: &mut Vec<CompanyRatioProduct>,
    group: &[EnvelopeFactRef<'_>],
    metric: CompanyRatioMetric,
    numerator_metrics: &[CompanyFinancialMetric],
    denominator_metrics: &[CompanyFinancialMetric],
    budget: &mut CompanySerializedBudget,
) -> Result<(), CompanyProductProjectionError> {
    let mut inputs = Vec::new();
    append_ratio_inputs(
        &mut inputs,
        group,
        numerator_metrics,
        CompanyRatioInputRole::Numerator,
    )?;
    let numerator_count = inputs.len();
    append_ratio_inputs(
        &mut inputs,
        group,
        denominator_metrics,
        CompanyRatioInputRole::Denominator,
    )?;
    let denominator_count = inputs.len() - numerator_count;
    let relevant_inputs = numerator_metrics
        .iter()
        .chain(denominator_metrics)
        .fold(0, |bits, metric| bits | input_bit(*metric));
    let nonnumeric_input = group.iter().any(|candidate| {
        candidate
            .fact
            .lineage
            .nonnumeric_inputs
            .is_some_and(|bits| bits & relevant_inputs != 0)
    });
    if inputs.is_empty()
        && group.iter().all(|candidate| {
            candidate
                .fact
                .lineage
                .nonnumeric_inputs
                .is_some_and(|bits| bits & relevant_inputs == 0)
        })
    {
        // A complete context reporting neither operand is not an attempted calculation.
        // Keep its original facts/statements; do not borrow another filing's operands.
        return Ok(());
    }
    let numerator = ratio_operand(&inputs[..numerator_count]);
    let denominator = ratio_operand(&inputs[numerator_count..]);
    let (state, value) = if numerator_count == 0 || denominator_count == 0 || nonnumeric_input {
        (CompanyRatioState::MissingInput, None)
    } else if numerator.is_none() || denominator.is_none() {
        (CompanyRatioState::ConflictingInput, None)
    } else {
        let numerator = numerator.ok_or(CompanyProductProjectionError::InvalidEvidence)?;
        let denominator = denominator.ok_or(CompanyProductProjectionError::InvalidEvidence)?;
        if numerator.unit != denominator.unit {
            (CompanyRatioState::IncompatibleUnits, None)
        } else if denominator.value.is_zero() {
            (CompanyRatioState::ZeroDenominator, None)
        } else if let Some(value) = numerator.value.checked_div(denominator.value) {
            (CompanyRatioState::Reported, Some(value))
        } else {
            (CompanyRatioState::Unavailable, None)
        }
    };
    let display_name = match (metric, numerator_metrics, numerator_count) {
        (CompanyRatioMetric::NetMargin, [CompanyFinancialMetric::NetIncome], 1..) => {
            "Net margin attributable to parent"
        }
        (
            CompanyRatioMetric::NetMargin,
            [CompanyFinancialMetric::ProfitOrLossIncludingNoncontrollingInterests],
            1..,
        ) => "Consolidated net margin",
        _ => metric.display_name(),
    };
    let product = CompanyRatioProduct {
        metric,
        display_name,
        state,
        value,
        unit: CompanyRatioUnit::Ratio,
        envelope: Some(reporting_envelope(group[0].fact)),
        inputs: inputs.into_boxed_slice(),
    };
    budget.charge(&product)?;
    ratios
        .try_reserve(1)
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    ratios.push(product);
    Ok(())
}

// Repeated exact filing occurrences are evidence of one operand, never amounts to sum.
// CompanyFacts and alternative financial concepts retain their existing conflict semantics.
fn ratio_operand(inputs: &[CompanyRatioInputProduct]) -> Option<&CompanyFactProduct> {
    let first = &inputs.first()?.fact;
    if inputs.len() == 1 {
        return Some(first);
    }
    (first.scope == CompanyFactProductScope::FilingDetail
        && first.lineage.xbrl_identity.is_some()
        && inputs.iter().all(|input| {
            input.fact.metric == first.metric
                && input.fact.unit == first.unit
                && input.fact.value == first.value
                && fact_envelope_key(&input.fact) == fact_envelope_key(first)
        }))
    .then_some(first)
}

fn append_ratio_inputs(
    inputs: &mut Vec<CompanyRatioInputProduct>,
    group: &[EnvelopeFactRef<'_>],
    metrics: &[CompanyFinancialMetric],
    role: CompanyRatioInputRole,
) -> Result<(), CompanyProductProjectionError> {
    for candidate in group
        .iter()
        .filter(|candidate| metrics.contains(&candidate.fact.metric))
    {
        inputs
            .try_reserve(1)
            .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
        inputs.push(CompanyRatioInputProduct {
            role,
            fact: candidate.fact.clone(),
        });
    }
    Ok(())
}

fn fact_envelope_key(fact: &CompanyFactProduct) -> CompanyFactEnvelopeKey<'_> {
    let (period_kind, period_start, period_end) = match fact.period {
        FundamentalPeriod::Instant { instant } => (
            0,
            instant.days_since_unix_epoch(),
            instant.days_since_unix_epoch(),
        ),
        FundamentalPeriod::Duration { start, end } => (
            1,
            start.days_since_unix_epoch(),
            end.days_since_unix_epoch(),
        ),
    };
    let (effective_kind, effective_value) = match fact.effective {
        CompanyProductTime::Timestamp(timestamp) => (0, timestamp.unix_nanos()),
        CompanyProductTime::CalendarDate(date) => (1, i64::from(date.days_since_unix_epoch())),
    };
    CompanyFactEnvelopeKey {
        filing_identity: &fact.lineage.filing_identity,
        publication_identity: fact.lineage.publication_identity,
        scope: match fact.scope {
            CompanyFactProductScope::CompanyWide => 0,
            CompanyFactProductScope::FilingDetail => 1,
        },
        revision: match fact.revision {
            CompanyProductRevisionState::Current => 0,
            CompanyProductRevisionState::Superseded => 1,
            CompanyProductRevisionState::IncomparableHistory => 2,
        },
        period_kind,
        period_start,
        period_end,
        fiscal_year_present: fact.fiscal_context.fiscal_year.is_some(),
        fiscal_year: fact.fiscal_context.fiscal_year.unwrap_or_default(),
        fiscal_period: match fact.fiscal_context.fiscal_period {
            CompanyFactFiscalPeriod::FiscalYear => 0,
            CompanyFactFiscalPeriod::CalendarYear => 1,
            CompanyFactFiscalPeriod::FirstQuarter => 2,
            CompanyFactFiscalPeriod::SecondQuarter => 3,
            CompanyFactFiscalPeriod::ThirdQuarter => 4,
            CompanyFactFiscalPeriod::FourthQuarter => 5,
            CompanyFactFiscalPeriod::Unavailable => 6,
        },
        cadence: match fact.fiscal_context.cadence {
            CompanyFactCadence::Annual => 0,
            CompanyFactCadence::Quarterly => 1,
            CompanyFactCadence::Other => 2,
            CompanyFactCadence::Unavailable => 3,
        },
        dimensionality: match fact.reporting_context.dimensionality {
            CompanyFactDimensionality::Unavailable => 0,
            CompanyFactDimensionality::NoDimensions => 1,
        },
        consolidation: match fact.reporting_context.consolidation {
            CompanyFactConsolidation::ReportedConsolidated => 0,
            CompanyFactConsolidation::ReportedNonConsolidated => 1,
            CompanyFactConsolidation::Unavailable => 2,
        },
        amendment: match fact.reporting_context.amendment {
            CompanyFactAmendment::Original => 0,
            CompanyFactAmendment::Amendment => 1,
            CompanyFactAmendment::Unavailable => 2,
        },
        restatement: match fact.reporting_context.restatement {
            CompanyFactRestatement::ReportedRestated => 0,
            CompanyFactRestatement::ReportedNotRestated => 1,
            CompanyFactRestatement::Unavailable => 2,
        },
        xbrl_context_identity: fact
            .lineage
            .xbrl_identity
            .as_ref()
            .map(|(context, _)| context.as_str()),
        filed_on_present: fact.filed_on.is_some(),
        filed_on: fact.filed_on.map_or(0, CalendarDate::days_since_unix_epoch),
        effective_kind,
        effective_value,
        known_at: fact.known_at.unix_nanos(),
    }
}

const fn reporting_envelope(fact: &CompanyFactProduct) -> CompanyReportingEnvelopeProduct {
    CompanyReportingEnvelopeProduct {
        scope: fact.scope,
        period: fact.period,
        fiscal_context: fact.fiscal_context,
        reporting_context: CompanyEnvelopeReportingContext {
            dimensionality: fact.reporting_context.dimensionality,
            consolidation: fact.reporting_context.consolidation,
            amendment: fact.reporting_context.amendment,
            restatement: fact.reporting_context.restatement,
        },
        filed_on: fact.filed_on,
        effective: fact.effective,
        known_at: fact.known_at,
    }
}

pub(crate) fn project_fact(
    fact: &CompanyResearchFact,
    knowledge_cutoff: Timestamp,
) -> Result<Option<CompanyFactProduct>, CompanyProductProjectionError> {
    if fact.known_at() > knowledge_cutoff {
        return Err(CompanyProductProjectionError::InvalidEvidence);
    }
    let Some(metric) = product_metric(fact.metric()) else {
        return Ok(None);
    };
    let Some(unit) = product_unit(metric, fact.unit()) else {
        return Ok(None);
    };
    let Some(fiscal_context) = product_fiscal_context(fact)? else {
        return Ok(None);
    };
    let Some(reporting_context) = product_reporting_context(fact) else {
        return Ok(None);
    };
    if (fact.scope() == CompanyFactScope::FilingDetail) != fact.lineage().xbrl_identity().is_some()
    {
        return Err(CompanyProductProjectionError::InvalidEvidence);
    }
    Ok(Some(CompanyFactProduct {
        lineage: CompanyFactPrivateLineage {
            filing_identity: try_boxed_product_text(fact.lineage().filing_identity(), 256)
                .map_err(map_product_text_error)?,
            publication_identity: fact.lineage().publication_identity().bytes(),
            xbrl_identity: fact.lineage().xbrl_identity().cloned(),
            nonnumeric_inputs: fact.nonnumeric_inputs(),
        },
        scope: match fact.scope() {
            CompanyFactScope::CompanyWide => CompanyFactProductScope::CompanyWide,
            CompanyFactScope::FilingDetail => CompanyFactProductScope::FilingDetail,
        },
        revision: product_revision(fact.revision()),
        metric,
        display_name: metric.display_name(),
        value: fact.value(),
        unit,
        period: fact.period(),
        fiscal_context,
        reporting_context,
        filed_on: fact.filed_on(),
        effective: product_time(fact.effective())?,
        known_at: fact.known_at(),
    }))
}

pub(crate) fn product_metric(source_metric: &str) -> Option<CompanyFinancialMetric> {
    match source_metric {
        "us-gaap:CashAndCashEquivalentsAtCarryingValue" => {
            Some(CompanyFinancialMetric::CashAndCashEquivalents)
        }
        "us-gaap:AccountsReceivableNetCurrent" => {
            Some(CompanyFinancialMetric::AccountsReceivableNetCurrent)
        }
        "us-gaap:InventoryNet" => Some(CompanyFinancialMetric::InventoryNet),
        "us-gaap:AssetsCurrent" => Some(CompanyFinancialMetric::CurrentAssets),
        "us-gaap:Assets" => Some(CompanyFinancialMetric::TotalAssets),
        "us-gaap:LiabilitiesCurrent" => Some(CompanyFinancialMetric::CurrentLiabilities),
        "us-gaap:Liabilities" => Some(CompanyFinancialMetric::TotalLiabilities),
        "us-gaap:LongTermDebtCurrent" => Some(CompanyFinancialMetric::CurrentLongTermDebt),
        "us-gaap:LongTermDebtNoncurrent" => Some(CompanyFinancialMetric::NoncurrentLongTermDebt),
        "us-gaap:StockholdersEquity" => Some(CompanyFinancialMetric::ShareholdersEquity),
        "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest" => {
            Some(CompanyFinancialMetric::TotalEquityIncludingNoncontrollingInterests)
        }
        "us-gaap:Revenues" => Some(CompanyFinancialMetric::Revenue),
        "us-gaap:SalesRevenueNet" => Some(CompanyFinancialMetric::NetSales),
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax" => {
            Some(CompanyFinancialMetric::CustomerRevenueExcludingAssessedTax)
        }
        "us-gaap:CostOfRevenue" => Some(CompanyFinancialMetric::CostOfRevenue),
        "us-gaap:GrossProfit" => Some(CompanyFinancialMetric::GrossProfit),
        "us-gaap:OperatingExpenses" => Some(CompanyFinancialMetric::OperatingExpenses),
        "us-gaap:OperatingIncomeLoss" => Some(CompanyFinancialMetric::OperatingIncome),
        "us-gaap:NetIncomeLoss" => Some(CompanyFinancialMetric::NetIncome),
        "us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic" => {
            Some(CompanyFinancialMetric::CommonNetIncome)
        }
        "us-gaap:PreferredStockDividendsAndOtherAdjustments" => {
            Some(CompanyFinancialMetric::PreferredDividendsAndAdjustments)
        }
        "us-gaap:ProceedsFromIssuanceOfLongTermDebt" => {
            Some(CompanyFinancialMetric::LongTermBorrowingProceeds)
        }
        "us-gaap:RepaymentsOfLongTermDebt" => Some(CompanyFinancialMetric::LongTermDebtRepayments),
        "us-gaap:PaymentsOfDividendsPreferredStockAndPreferenceStock" => {
            Some(CompanyFinancialMetric::PreferredDividendsPaid)
        }
        "us-gaap:PreferredStockValue" => Some(CompanyFinancialMetric::PreferredStockIssuedValue),
        "us-gaap:ProfitLoss" => {
            Some(CompanyFinancialMetric::ProfitOrLossIncludingNoncontrollingInterests)
        }
        "us-gaap:EarningsPerShareBasic" => Some(CompanyFinancialMetric::BasicEarningsPerShare),
        "us-gaap:EarningsPerShareDiluted" => Some(CompanyFinancialMetric::DilutedEarningsPerShare),
        "us-gaap:NetCashProvidedByUsedInOperatingActivities" => {
            Some(CompanyFinancialMetric::OperatingCashFlow)
        }
        "us-gaap:NetCashProvidedByUsedInInvestingActivities" => {
            Some(CompanyFinancialMetric::InvestingCashFlow)
        }
        "us-gaap:NetCashProvidedByUsedInFinancingActivities" => {
            Some(CompanyFinancialMetric::FinancingCashFlow)
        }
        "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment" => {
            Some(CompanyFinancialMetric::PropertyPlantAndEquipmentPurchases)
        }
        "dei:EntityCommonStockSharesOutstanding" => {
            Some(CompanyFinancialMetric::EntityCommonSharesOutstanding)
        }
        "us-gaap:CommonStockSharesOutstanding" => {
            Some(CompanyFinancialMetric::CommonStockSharesOutstanding)
        }
        "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic" => {
            Some(CompanyFinancialMetric::WeightedAverageBasicShares)
        }
        "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding" => {
            Some(CompanyFinancialMetric::WeightedAverageDilutedShares)
        }
        _ => None,
    }
}

fn product_unit(metric: CompanyFinancialMetric, source_unit: &str) -> Option<CompanyFactUnit> {
    match metric.expected_unit() {
        CompanyMetricUnit::Currency => {
            product_currency(source_unit).map(|currency| CompanyFactUnit::Currency { currency })
        }
        CompanyMetricUnit::Shares if matches!(source_unit, "shares" | "xbrli:shares") => {
            Some(CompanyFactUnit::Shares)
        }
        CompanyMetricUnit::Shares => None,
        CompanyMetricUnit::CurrencyPerShare => product_per_share_currency(source_unit)
            .map(|currency| CompanyFactUnit::CurrencyPerShare { currency }),
    }
}

fn product_currency(source_unit: &str) -> Option<Currency> {
    let code = source_unit.strip_prefix("iso4217:").unwrap_or(source_unit);
    Currency::try_from(code).ok()
}

fn product_per_share_currency(source_unit: &str) -> Option<Currency> {
    if let Some(currency) = source_unit.strip_suffix("/shares") {
        return product_currency(currency);
    }
    source_unit
        .strip_prefix("divide(iso4217:")
        .and_then(|value| value.strip_suffix("/xbrli:shares)"))
        .and_then(|currency| Currency::try_from(currency).ok())
}

fn product_fiscal_context(
    fact: &CompanyResearchFact,
) -> Result<Option<CompanyFactFiscalContext>, CompanyProductProjectionError> {
    let fiscal_period = match fact.fiscal_period() {
        CompanyResearchFiscalPeriod::FiscalYear => CompanyFactFiscalPeriod::FiscalYear,
        CompanyResearchFiscalPeriod::CalendarYear => CompanyFactFiscalPeriod::CalendarYear,
        CompanyResearchFiscalPeriod::FirstQuarter => CompanyFactFiscalPeriod::FirstQuarter,
        CompanyResearchFiscalPeriod::SecondQuarter => CompanyFactFiscalPeriod::SecondQuarter,
        CompanyResearchFiscalPeriod::ThirdQuarter => CompanyFactFiscalPeriod::ThirdQuarter,
        CompanyResearchFiscalPeriod::FourthQuarter => CompanyFactFiscalPeriod::FourthQuarter,
        CompanyResearchFiscalPeriod::Unavailable => CompanyFactFiscalPeriod::Unavailable,
        CompanyResearchFiscalPeriod::Unsupported => return Ok(None),
    };
    let cadence = match fact.cadence() {
        FundamentalCadence::Annual => CompanyFactCadence::Annual,
        FundamentalCadence::Quarterly => CompanyFactCadence::Quarterly,
        FundamentalCadence::Other => CompanyFactCadence::Other,
        FundamentalCadence::Unavailable => CompanyFactCadence::Unavailable,
    };
    let valid_pair = matches!(
        (fiscal_period, cadence),
        (
            CompanyFactFiscalPeriod::FiscalYear | CompanyFactFiscalPeriod::CalendarYear,
            CompanyFactCadence::Annual
        ) | (
            CompanyFactFiscalPeriod::FirstQuarter
                | CompanyFactFiscalPeriod::SecondQuarter
                | CompanyFactFiscalPeriod::ThirdQuarter
                | CompanyFactFiscalPeriod::FourthQuarter,
            CompanyFactCadence::Quarterly
        ) | (
            CompanyFactFiscalPeriod::Unavailable,
            CompanyFactCadence::Unavailable
        )
    );
    if !valid_pair {
        return Err(CompanyProductProjectionError::InvalidEvidence);
    }
    Ok(Some(CompanyFactFiscalContext {
        fiscal_year: fact.fiscal_year(),
        fiscal_period,
        cadence,
    }))
}

fn product_reporting_context(fact: &CompanyResearchFact) -> Option<CompanyFactReportingContext> {
    let dimensionality = match fact.dimension_state() {
        CompanyResearchDimensionState::Unavailable => CompanyFactDimensionality::Unavailable,
        CompanyResearchDimensionState::NoDimensions => CompanyFactDimensionality::NoDimensions,
        CompanyResearchDimensionState::Dimensions { .. } => return None,
    };
    Some(CompanyFactReportingContext {
        dimensionality,
        consolidation: match fact.consolidation() {
            FundamentalConsolidation::SourceReportedConsolidated => {
                CompanyFactConsolidation::ReportedConsolidated
            }
            FundamentalConsolidation::SourceReportedNonConsolidated => {
                CompanyFactConsolidation::ReportedNonConsolidated
            }
            FundamentalConsolidation::Unavailable => CompanyFactConsolidation::Unavailable,
        },
        amendment: match fact.amendment_status() {
            FundamentalAmendmentStatus::Original => CompanyFactAmendment::Original,
            FundamentalAmendmentStatus::Amendment => CompanyFactAmendment::Amendment,
            FundamentalAmendmentStatus::Unavailable => CompanyFactAmendment::Unavailable,
        },
        restatement: match fact.restatement_state() {
            CompanyResearchRestatementState::Unavailable => CompanyFactRestatement::Unavailable,
            CompanyResearchRestatementState::ReportedNotRestated => {
                CompanyFactRestatement::ReportedNotRestated
            }
            CompanyResearchRestatementState::ReportedRestated => {
                CompanyFactRestatement::ReportedRestated
            }
        },
        occurrence: fact.occurrence(),
    })
}

pub(crate) fn project_filing(
    filing: &CompanyResearchFiling,
    knowledge_cutoff: Timestamp,
) -> Result<CompanyFilingProduct, CompanyProductProjectionError> {
    if filing.known_at() > knowledge_cutoff {
        return Err(CompanyProductProjectionError::InvalidEvidence);
    }
    Ok(CompanyFilingProduct {
        revision: product_revision(filing.revision()),
        form: try_boxed_product_text(filing.form(), MAX_PRODUCT_FILING_FORM_BYTES)
            .map_err(map_product_text_error)?,
        effective: product_time(filing.effective())?,
        published: filing.published().map(product_time).transpose()?,
        known_at: filing.known_at(),
    })
}

fn product_revision(revision: CompanyResearchRevisionState) -> CompanyProductRevisionState {
    match revision {
        CompanyResearchRevisionState::Current => CompanyProductRevisionState::Current,
        CompanyResearchRevisionState::Superseded => CompanyProductRevisionState::Superseded,
        CompanyResearchRevisionState::IncomparableHistory => {
            CompanyProductRevisionState::IncomparableHistory
        }
    }
}

fn empty_result(
    instrument_id: InstrumentId,
    knowledge_cutoff: Timestamp,
    fact_effective_cutoff: CompanyProductTime,
    availability: CompanyProductAvailability,
    section_state: CompanyProductSectionState,
    primary_limitation: CompanyProductLimitation,
) -> Result<CompanyProductResult, CompanyProductProjectionError> {
    let mut budget = CompanySerializedBudget::new();
    let ratios = unavailable_ratio_set(section_state, &mut budget)?;
    Ok(CompanyProductResult {
        instrument_id,
        identity: None,
        availability,
        facts: CompanyFactsProduct {
            state: section_state,
            items: Box::new([]),
        },
        statements: CompanyStatementsProduct {
            state: section_state,
            groups: Box::new([]),
        },
        ratios,
        filings: CompanyFilingsProduct {
            state: section_state,
            items: Box::new([]),
        },
        clocks: CompanyProductClocks {
            knowledge_cutoff,
            fact_effective_cutoff,
            latest_known_at: None,
        },
        coverage: CompanyProductCoverage {
            requested_sections: COMPANY_PRODUCT_SECTIONS,
            available_sections: 0,
            reported_facts: 0,
            omitted_facts: 0,
            statement_lines: 0,
            evaluated_ratios: 4,
            reported_ratios: 0,
            filing_events: 0,
        },
        limitations: Box::new([primary_limitation]),
    })
}

fn product_time(
    value: &ResearchTemporalCoordinate,
) -> Result<CompanyProductTime, CompanyProductProjectionError> {
    if let Some(timestamp) = value.exact_timestamp() {
        Ok(CompanyProductTime::Timestamp(timestamp))
    } else if let Some(date) = value.calendar_date_value() {
        Ok(CompanyProductTime::CalendarDate(date))
    } else {
        Err(CompanyProductProjectionError::InvalidEvidence)
    }
}

fn map_product_text_error(error: ProductTextCopyError) -> CompanyProductProjectionError {
    match error {
        ProductTextCopyError::BoundExceeded => CompanyProductProjectionError::InvalidEvidence,
        ProductTextCopyError::AllocationFailed => CompanyProductProjectionError::ResourceExhausted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn statement_and_ratio_projection_requires_one_exact_filing_envelope()
    -> anyhow::Result<()> {
        let year_start = CalendarDate::new(2025, 1, 1)?;
        let year_end = CalendarDate::new(2025, 12, 31)?;
        let duration = FundamentalPeriod::duration(year_start, year_end)?;
        let instant = FundamentalPeriod::instant(year_end);
        let known_at = Timestamp::from_unix_nanos(1_800_000_000_000_000_000);
        let usd = Currency::try_from("USD")?;
        let eur = Currency::try_from("EUR")?;
        let facts = vec![
            fact(
                CompanyFinancialMetric::CurrentAssets,
                200,
                usd,
                instant,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
            fact(
                CompanyFinancialMetric::CurrentLiabilities,
                100,
                usd,
                instant,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
            fact(
                CompanyFinancialMetric::CustomerRevenueExcludingAssessedTax,
                100,
                usd,
                duration,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
            fact(
                CompanyFinancialMetric::GrossProfit,
                40,
                usd,
                duration,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
            fact(
                CompanyFinancialMetric::OperatingIncome,
                20,
                usd,
                duration,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
            fact(
                CompanyFinancialMetric::NetIncome,
                10,
                usd,
                duration,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
        ];

        let mut budget = CompanySerializedBudget::new();
        let statements =
            project_statements(&facts, CompanyProductSectionState::Reported, &mut budget)?;
        assert_eq!(statements.state(), CompanyProductSectionState::Reported);
        assert_eq!(
            statements
                .groups()
                .iter()
                .map(|group| group.items().len())
                .sum::<usize>(),
            facts.len()
        );
        for group in statements.groups() {
            let first = &group.items()[0].lineage;
            assert!(group.items().iter().all(|item| {
                item.lineage.filing_identity == first.filing_identity
                    && item.lineage.publication_identity == first.publication_identity
            }));
        }
        let debug = format!("{:?}", facts[0]);
        assert!(!debug.contains("filing-a"));
        assert!(!debug.contains("publication_identity"));
        assert_eq!(format!("{:?}", facts[0].lineage), "[PRIVATE FACT LINEAGE]");
        assert_eq!(
            company_product_availability(COMPANY_PRODUCT_SECTIONS, false),
            CompanyProductAvailability::Partial
        );

        let ratios = project_ratios(&facts, CompanyProductSectionState::Reported, &mut budget)?;
        assert_eq!(ratios.state(), CompanyProductSectionState::Reported);
        assert_eq!(ratios.items().len(), 4);
        assert_eq!(
            ratios
                .items()
                .iter()
                .map(CompanyRatioProduct::metric)
                .collect::<Vec<_>>(),
            vec![
                CompanyRatioMetric::CurrentRatio,
                CompanyRatioMetric::GrossMargin,
                CompanyRatioMetric::OperatingMargin,
                CompanyRatioMetric::NetMargin,
            ]
        );
        assert_eq!(
            ratios
                .items()
                .iter()
                .find(|ratio| {
                    ratio.metric() == CompanyRatioMetric::CurrentRatio
                        && ratio.state() == CompanyRatioState::Reported
                })
                .map(CompanyRatioProduct::value),
            Some(Some(Decimal::from(2_u8)))
        );
        for ratio in ratios.items() {
            assert_eq!(ratio.state(), CompanyRatioState::Reported);
            assert_eq!(
                ratio.envelope().map(|envelope| envelope.period),
                Some(if ratio.metric() == CompanyRatioMetric::CurrentRatio {
                    instant
                } else {
                    duration
                })
            );
            assert_eq!(ratio.inputs().len(), 2);
            assert_eq!(ratio.inputs()[0].role(), CompanyRatioInputRole::Numerator);
            assert_eq!(ratio.inputs()[1].role(), CompanyRatioInputRole::Denominator);
            for input in ratio.inputs() {
                assert_eq!(
                    input.fact().reporting_context().amendment,
                    CompanyFactAmendment::Original
                );
                assert_eq!(input.fact().known_at(), known_at);
                assert_eq!(input.fact().filed_on(), Some(year_end));
            }
        }

        // A filing's per-concept occurrence ordinal is not a reporting context. One
        // context has complete operands even when repetitions consume different ordinals.
        let mut filing_facts = facts[2..].to_vec();
        for (index, (fact, ordinal)) in filing_facts.iter_mut().zip([3, 1, 5, 2]).enumerate() {
            fact.scope = CompanyFactProductScope::FilingDetail;
            fact.lineage.xbrl_identity = Some((
                SourceIdentifier::try_from("annual-context")?,
                SourceIdentifier::try_from(format!("source-fact-{index}"))?,
            ));
            fact.reporting_context.occurrence = RevisionNumber::new(ordinal)?;
        }
        for (index, ordinal) in [(0, 8), (3, 5)] {
            let mut repeated = filing_facts[index].clone();
            repeated.lineage.xbrl_identity = Some((
                SourceIdentifier::try_from("annual-context")?,
                SourceIdentifier::try_from(format!("repeated-fact-{index}"))?,
            ));
            repeated.reporting_context.occurrence = RevisionNumber::new(ordinal)?;
            filing_facts.push(repeated);
        }
        let originals = filing_facts.clone();
        let filing_ratios = project_ratios(
            &filing_facts,
            CompanyProductSectionState::Reported,
            &mut CompanySerializedBudget::new(),
        )?;
        assert_eq!(filing_ratios.items().len(), 3);
        assert!(filing_ratios.items().iter().all(|ratio| {
            ratio.state() == CompanyRatioState::Reported
                && ratio
                    .envelope()
                    .is_some_and(|envelope| envelope.scope == CompanyFactProductScope::FilingDetail)
        }));
        for (ratio, expected) in filing_ratios.items().iter().zip([40, 20, 10]) {
            assert_eq!(ratio.value(), Some(Decimal::new(expected, 2)));
            assert_eq!(ratio.inputs().len(), if expected == 10 { 4 } else { 3 });
            assert!(
                ratio
                    .inputs()
                    .iter()
                    .all(|input| originals.contains(input.fact()))
            );
        }
        assert_eq!(filing_facts, originals);
        let paged_ratios = project_financial_envelope(&filing_facts, true)?;
        assert_eq!(paged_ratios.len(), 3);
        assert!(paged_ratios.iter().all(|ratio| {
            ratio["envelope"]["scope"] == "filing_detail"
                && ratio["envelope"]["reportingContext"]
                    .get("occurrence")
                    .is_none()
                && ratio["inputs"].as_array().is_some_and(|inputs| {
                    inputs
                        .iter()
                        .all(|input| input["fact"]["reportingContext"]["occurrence"].is_number())
                })
        }));
        let filing_statements = project_statements(
            &filing_facts,
            CompanyProductSectionState::Reported,
            &mut CompanySerializedBudget::new(),
        )?;
        assert_eq!(filing_statements.groups().len(), 1);
        assert_eq!(filing_statements.groups()[0].items().len(), originals.len());
        for original in &originals {
            assert!(filing_statements.groups()[0].items().contains(original));
        }

        let mut other_context = filing_facts[1].clone();
        other_context.lineage.xbrl_identity = Some((
            SourceIdentifier::try_from("other-annual-context")?,
            SourceIdentifier::try_from("other-context-profit")?,
        ));
        let split_contexts = [filing_facts[0].clone(), other_context];
        assert_ne!(
            fact_envelope_bytes(&split_contexts[0])?,
            fact_envelope_bytes(&split_contexts[1])?
        );
        assert_eq!(
            project_financial_envelope(&split_contexts, true),
            Err(CompanyProductProjectionError::InvalidEvidence)
        );
        assert!(
            project_ratios(
                &split_contexts,
                CompanyProductSectionState::Reported,
                &mut CompanySerializedBudget::new(),
            )?
            .items()
            .iter()
            .all(|ratio| ratio.state() == CompanyRatioState::MissingInput)
        );

        // Retaining all occurrences must not select a winner when repeated values differ.
        let mut conflicting_occurrences = filing_facts.clone();
        conflicting_occurrences[4].value += Decimal::ONE;
        let conflicting_ratios = project_ratios(
            &conflicting_occurrences,
            CompanyProductSectionState::Reported,
            &mut CompanySerializedBudget::new(),
        )?;
        assert!(conflicting_ratios.items().iter().all(|ratio| {
            ratio.state() == CompanyRatioState::ConflictingInput && ratio.value().is_none()
        }));
        // Equal values from alternative concepts are not duplicate source occurrences.
        conflicting_occurrences[4] = filing_facts[4].clone();
        conflicting_occurrences[4].metric = CompanyFinancialMetric::NetSales;
        assert!(
            project_ratios(
                &conflicting_occurrences,
                CompanyProductSectionState::Reported,
                &mut CompanySerializedBudget::new(),
            )?
            .items()
            .iter()
            .all(|ratio| ratio.state() == CompanyRatioState::ConflictingInput)
        );

        // TSLA's H1 filing reports both total/customer revenue and parent/consolidated
        // income. Choose a defined basis, preserving source alternatives in statements.
        let tsla_end = CalendarDate::new(2026, 6, 30)?;
        let tsla_period = FundamentalPeriod::duration(CalendarDate::new(2026, 1, 1)?, tsla_end)?;
        let project_basis = |items: &[CompanyFactProduct]| {
            project_ratios(
                items,
                CompanyProductSectionState::Reported,
                &mut CompanySerializedBudget::new(),
            )
        };
        for scope in [
            CompanyFactProductScope::CompanyWide,
            CompanyFactProductScope::FilingDetail,
        ] {
            let mut tsla = Vec::new();
            for (index, (metric, value)) in [
                (CompanyFinancialMetric::Revenue, 50_623_000_000),
                (
                    CompanyFinancialMetric::CustomerRevenueExcludingAssessedTax,
                    50_623_000_000,
                ),
                (CompanyFinancialMetric::GrossProfit, 9_471_000_000),
                (CompanyFinancialMetric::OperatingIncome, 1_339_000_000),
                (CompanyFinancialMetric::NetIncome, 1_591_000_000),
                (
                    CompanyFinancialMetric::ProfitOrLossIncludingNoncontrollingInterests,
                    1_619_000_000,
                ),
            ]
            .into_iter()
            .enumerate()
            {
                let mut item = fact(
                    metric,
                    value,
                    usd,
                    tsla_period,
                    CalendarDate::new(2026, 7, 23)?,
                    known_at,
                    "0001628280-26-049270",
                    3,
                )?;
                item.scope = scope;
                item.effective = CompanyProductTime::CalendarDate(tsla_end);
                item.fiscal_context = CompanyFactFiscalContext {
                    fiscal_year: Some(2026),
                    fiscal_period: CompanyFactFiscalPeriod::SecondQuarter,
                    cadence: CompanyFactCadence::Quarterly,
                };
                if scope == CompanyFactProductScope::FilingDetail {
                    item.lineage.xbrl_identity = Some((
                        SourceIdentifier::try_from("c-1")?,
                        SourceIdentifier::try_from(format!("tsla-{index}"))?,
                    ));
                }
                tsla.push(item);
            }
            if scope == CompanyFactProductScope::FilingDetail {
                for index in [0, 4] {
                    let mut repeated = tsla[index].clone();
                    repeated.reporting_context.occurrence = RevisionNumber::new(2)?;
                    repeated.lineage.xbrl_identity = Some((
                        SourceIdentifier::try_from("c-1")?,
                        SourceIdentifier::try_from(format!("tsla-repeat-{index}"))?,
                    ));
                    tsla.push(repeated);
                }
            }
            let selected = project_basis(&tsla)?;
            assert_eq!(selected.items().len(), 3);
            for (ratio, numerator) in selected.items().iter().zip([
                CompanyFinancialMetric::GrossProfit,
                CompanyFinancialMetric::OperatingIncome,
                CompanyFinancialMetric::NetIncome,
            ]) {
                assert_eq!(ratio.state(), CompanyRatioState::Reported);
                let mut expected_inputs: Vec<_> = tsla
                    .iter()
                    .filter(|fact| {
                        fact.metric == numerator || fact.metric == CompanyFinancialMetric::Revenue
                    })
                    .map(|fact| (fact.metric == CompanyFinancialMetric::Revenue, fact))
                    .collect();
                let mut actual_inputs: Vec<_> = ratio
                    .inputs()
                    .iter()
                    .map(|input| {
                        (
                            input.role() == CompanyRatioInputRole::Denominator,
                            input.fact(),
                        )
                    })
                    .collect();
                expected_inputs.sort_unstable_by_key(|(denominator, fact)| {
                    (*denominator, fact.reporting_context.occurrence.get())
                });
                actual_inputs.sort_unstable_by_key(|(denominator, fact)| {
                    (*denominator, fact.reporting_context.occurrence.get())
                });
                assert_eq!(actual_inputs, expected_inputs);
            }
            assert_eq!(
                selected.items()[2].value(),
                Some(Decimal::from(1591) / Decimal::from(50623))
            );
            assert_eq!(
                selected.items()[2].display_name,
                "Net margin attributable to parent"
            );
            // Total-revenue selection depends on financial meaning, not equal values.
            tsla[1].value = Decimal::ONE;
            assert_eq!(project_basis(&tsla)?, selected);
            for selected_index in [0, 4] {
                let mut conflicted = tsla.clone();
                let mut disagreeing = conflicted[selected_index].clone();
                disagreeing.value += Decimal::ONE;
                if scope == CompanyFactProductScope::FilingDetail {
                    disagreeing.lineage.xbrl_identity = Some((
                        SourceIdentifier::try_from("c-1")?,
                        SourceIdentifier::try_from("tsla-conflicting-occurrence")?,
                    ));
                }
                conflicted.push(disagreeing);
                let rejected = project_basis(&conflicted)?;
                for ratio in rejected.items() {
                    if selected_index == 0 || ratio.metric() == CompanyRatioMetric::NetMargin {
                        assert_eq!(ratio.state(), CompanyRatioState::ConflictingInput);
                        assert!(ratio.value().is_none());
                    } else {
                        assert_eq!(ratio.state(), CompanyRatioState::Reported);
                    }
                }
            }
            // Consolidated income is admitted only when no parent-attributable fact exists.
            tsla.retain(|fact| fact.metric != CompanyFinancialMetric::NetIncome);
            let consolidated = project_basis(&tsla)?;
            let net = &consolidated.items()[2];
            assert_eq!(net.state(), CompanyRatioState::Reported);
            assert_eq!(
                net.value(),
                Some(Decimal::from(1619) / Decimal::from(50623))
            );
            assert_eq!(net.display_name, "Consolidated net margin");
            assert!(
                net.inputs()
                    .iter()
                    .filter(|input| input.role() == CompanyRatioInputRole::Numerator)
                    .all(|input| input.fact().metric()
                        == CompanyFinancialMetric::ProfitOrLossIncludingNoncontrollingInterests)
            );
        }

        // A proxy filing repeats NVDA's annual income without revenue. Display history
        // must retain the complete original 10-K, not join its revenue to the proxy.
        {
            use market_squawk_data::{
                DatasetId, DatasetManifestRef, DatasetSchemaRegistry, PointInTimeCandidate,
                PointInTimeLimits, PointInTimePolicy, PointInTimeRequest, PointInTimeRevisionMode,
                PointInTimeRevisionState, PointInTimeService, SecResearchFamily, Sha256Digest,
            };
            use market_squawk_domain::{
                AvailabilityEvidence, CompanyIdentityObservation, CompanyIdentityObservationInput,
                CompanyIdentitySurface, CompanyObservationSubject, DataQuality, DigestAlgorithm,
                EvidenceDigest, ExactPayloadEvidence, FilingForm, FundamentalDimensionContext,
                FundamentalFactContext, FundamentalFactContextInput, FundamentalObservation,
                FundamentalRestatementStatus, FundamentalRevisionOrder, PayloadHash,
                PayloadReference, ResearchContext, ResearchObservation, ResearchProvenance,
                ResearchProvenanceInput, ResearchTime, SchemaVersion, SourceId,
            };
            use std::{
                num::NonZeroU32,
                time::{Duration, Instant},
            };
            use tokio_util::sync::CancellationToken;

            let source = SourceId::try_from("sec-edgar")?;
            let issuer = SourceIdentifier::try_from("0001045810")?;
            let publication = EvidenceDigest::new(DigestAlgorithm::Sha256, [11; 32]);
            let observed_at = Timestamp::from_unix_nanos(1_791_515_336_128_582_000);
            let nvda_end = CalendarDate::new(2026, 1, 25)?;
            let nvda_period =
                FundamentalPeriod::duration(CalendarDate::new(2025, 1, 27)?, nvda_end)?;
            let annual_filed = CalendarDate::new(2026, 2, 25)?;
            let proxy_filed = CalendarDate::new(2026, 5, 12)?;
            let company = CompanyIdentityObservation::try_new(CompanyIdentityObservationInput {
                schema_version: SchemaVersion::CURRENT,
                source_id: source.clone(),
                provider_company_id: issuer.clone(),
                surface: CompanyIdentitySurface::SecCompanyFacts,
                conformed_name: "NVIDIA CORP".to_owned(),
                former_names: Vec::new(),
                entity_type: None,
                sic: None,
                sic_description: None,
                associations: Vec::new(),
                parent_ingest_payload_evidence: ExactPayloadEvidence::from_content_digest(
                    publication,
                ),
                identity_payload_evidence: ExactPayloadEvidence::from_content_digest(publication),
                received_at: observed_at,
                availability: AvailabilityEvidence::local_first_observed(observed_at),
                ingested_at: observed_at,
                quality: DataQuality::OfficialDelayed,
            })?;
            let manifest = DatasetManifestRef::try_new_with_schema(
                DatasetId::try_from("nvda-annual-report-history")?,
                1,
                DatasetSchemaRegistry::local().canonical_research_observations()?,
                Sha256Digest::new([11; 32]),
            )?;
            let mut candidates = Vec::new();
            for (concept, value, ordinal, accession, form, filed_on, annual) in [
                (
                    "NetIncomeLoss",
                    120_067_000_000_i64,
                    1,
                    "0001045810-26-000021",
                    "10-K",
                    annual_filed,
                    true,
                ),
                (
                    "Revenues",
                    215_938_000_000,
                    1,
                    "0001045810-26-000021",
                    "10-K",
                    annual_filed,
                    true,
                ),
                (
                    "NetIncomeLoss",
                    120_067_000_000,
                    2,
                    "0001045810-26-000036",
                    "DEF 14A",
                    proxy_filed,
                    false,
                ),
            ] {
                let revision = RevisionNumber::new(ordinal)?;
                let context = ResearchContext::new(
                    ResearchProvenance::try_new(ResearchProvenanceInput {
                        source_id: source.clone(),
                        instrument_id: None,
                        venue_id: None,
                        source_identifier: SourceIdentifier::try_from(format!(
                            "{accession}:{concept}"
                        ))?,
                        source_timestamp: None,
                        received_at: observed_at,
                        ingested_at: observed_at,
                        quality: DataQuality::OfficialDelayed,
                        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                            DigestAlgorithm::Sha256,
                            [11; 32],
                        )),
                        availability: AvailabilityEvidence::local_first_observed(observed_at),
                    })?,
                    ResearchTime::try_new_with_coordinates(
                        ResearchTemporalCoordinate::calendar_date(nvda_end),
                        Some(ResearchTemporalCoordinate::calendar_date(filed_on)),
                        revision,
                        None,
                    )?,
                )?;
                let fact_context = FundamentalFactContext::try_new(FundamentalFactContextInput {
                    schema_version: SchemaVersion::CURRENT,
                    period: nvda_period,
                    unit: SourceIdentifier::try_from("USD")?,
                    accession: SourceIdentifier::try_from(accession)?,
                    filing_form: Some(FilingForm::try_from(form)?),
                    amendment_status: FundamentalAmendmentStatus::Original,
                    filed_on: Some(filed_on),
                    frame: None,
                    fiscal_year: annual.then_some(2026),
                    fiscal_period: annual
                        .then(|| SourceIdentifier::try_from("FY"))
                        .transpose()?,
                    cadence: if annual {
                        FundamentalCadence::Annual
                    } else {
                        FundamentalCadence::Unavailable
                    },
                    xbrl_context_id: None,
                    dimensions: FundamentalDimensionContext::unavailable(),
                    consolidation: FundamentalConsolidation::Unavailable,
                    revision_order: FundamentalRevisionOrder::new(
                        revision,
                        SourceIdentifier::try_from("sec-companyfacts-revision-order-v1")?,
                    ),
                    restatement_status: FundamentalRestatementStatus::Unavailable,
                })?;
                candidates.push(PointInTimeCandidate::new(
                    ResearchObservation::Fundamental(FundamentalObservation::new(
                        context,
                        CompanyObservationSubject::Issuer(issuer.clone()),
                        SourceIdentifier::try_from(format!("us-gaap:{concept}"))?,
                        Decimal::from(value),
                        fact_context,
                    )?),
                    manifest.clone(),
                ));
            }
            for (mode, cutoff, expected_rows) in [
                (PointInTimeRevisionMode::LatestKnown, observed_at, 2),
                (PointInTimeRevisionMode::AllKnown, observed_at, 3),
                (
                    PointInTimeRevisionMode::AllKnown,
                    Timestamp::from_unix_nanos(observed_at.unix_nanos() - 1),
                    0,
                ),
            ] {
                let request = PointInTimeRequest::try_new(
                    PointInTimePolicy::try_new(NonZeroU32::MIN, mode)?,
                    cutoff,
                    None,
                    ResearchTemporalCoordinate::calendar_date(nvda_end),
                    None,
                    PointInTimeLimits::try_new(3, 2, 2, 3, 1024 * 1024)?,
                )?;
                let selected = PointInTimeService::new()
                    .select(
                        &request,
                        &candidates,
                        &CancellationToken::new(),
                        Instant::now() + Duration::from_secs(10),
                    )
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                assert_eq!(selected.records().len(), expected_rows);
                if expected_rows == 0 {
                    assert_eq!(selected.exclusion_counts().availability_after_as_of(), 3);
                    continue;
                }
                let mut selected_facts = Vec::new();
                for record in selected.records() {
                    // No explicit supersession is recorded in either original filing.
                    assert_eq!(record.revision_state(), PointInTimeRevisionState::Current);
                    let (fact, filing) = super::super::company_research::company_source_row(
                        SecResearchFamily::CompanyFacts,
                        &company,
                        publication,
                        record.revision_state(),
                        record.candidate().observation().clone(),
                        cutoff,
                    )?;
                    assert!(filing.is_none());
                    selected_facts.push(
                        project_fact(
                            &fact.ok_or_else(|| anyhow::anyhow!("missing selected fact"))?,
                            cutoff,
                        )?
                        .ok_or_else(|| anyhow::anyhow!("unsupported selected fact"))?,
                    );
                }
                let ratios = project_basis(&selected_facts)?;
                let margins: Vec<_> = ratios
                    .items()
                    .iter()
                    .filter(|ratio| ratio.metric() == CompanyRatioMetric::NetMargin)
                    .collect();
                assert_eq!(margins.len(), 2);
                let annual = margins
                    .iter()
                    .find(|ratio| {
                        ratio
                            .envelope()
                            .is_some_and(|envelope| envelope.filed_on == Some(annual_filed))
                    })
                    .ok_or_else(|| anyhow::anyhow!("missing annual envelope"))?;
                let proxy = margins
                    .iter()
                    .find(|ratio| {
                        ratio
                            .envelope()
                            .is_some_and(|envelope| envelope.filed_on == Some(proxy_filed))
                    })
                    .ok_or_else(|| anyhow::anyhow!("missing proxy envelope"))?;
                assert_eq!(proxy.state(), CompanyRatioState::MissingInput);
                assert_eq!(proxy.inputs().len(), 1);
                assert_eq!(
                    proxy.inputs()[0].fact().lineage.filing_identity.as_ref(),
                    "0001045810-26-000036"
                );
                if mode == PointInTimeRevisionMode::LatestKnown {
                    assert_eq!(selected.exclusion_counts().lower_revision(), 1);
                    assert_eq!(annual.state(), CompanyRatioState::MissingInput);
                    continue;
                }
                assert_eq!(annual.state(), CompanyRatioState::Reported);
                assert_eq!(
                    annual.value(),
                    Some(Decimal::from(120067) / Decimal::from(215938))
                );
                assert_eq!(annual.inputs().len(), 2);
                let annual_facts: Vec<_> = annual
                    .inputs()
                    .iter()
                    .map(|input| input.fact().clone())
                    .collect();
                assert!(
                    annual_facts
                        .iter()
                        .all(|fact| fact.lineage.filing_identity.as_ref()
                            == "0001045810-26-000021"
                            && fact.lineage.publication_identity == publication.bytes()
                            && fact.reporting_context.occurrence.get() == 1
                            && fact.revision == CompanyProductRevisionState::Current)
                );
                assert!(
                    project_financial_envelope(&annual_facts, true)?
                        .contains(&serde_json::to_value(annual)?)
                );
            }
        }

        // TSLA's comparative quarter has three net-income occurrences but only two
        // revenue occurrences. Those concept-local revisions do not split one filing.
        let comparative_end = CalendarDate::new(2024, 9, 30)?;
        let comparative_period =
            FundamentalPeriod::duration(CalendarDate::new(2024, 7, 1)?, comparative_end)?;
        let mut comparative = Vec::new();
        for (metric, value, ordinal) in [
            (CompanyFinancialMetric::Revenue, 25_182_000_000, 2),
            (CompanyFinancialMetric::NetIncome, 2_173_000_000, 3),
            (
                CompanyFinancialMetric::ProfitOrLossIncludingNoncontrollingInterests,
                2_189_000_000,
                2,
            ),
        ] {
            let mut item = fact(
                metric,
                value,
                usd,
                comparative_period,
                CalendarDate::new(2025, 10, 23)?,
                known_at,
                "0001628280-25-045968",
                7,
            )?;
            item.effective = CompanyProductTime::CalendarDate(comparative_end);
            item.fiscal_context = CompanyFactFiscalContext {
                fiscal_year: Some(2025),
                fiscal_period: CompanyFactFiscalPeriod::ThirdQuarter,
                cadence: CompanyFactCadence::Quarterly,
            };
            item.reporting_context.occurrence = RevisionNumber::new(ordinal)?;
            comparative.push(item);
        }
        let original_comparative = comparative.clone();
        assert_eq!(
            fact_envelope_bytes(&comparative[0])?,
            fact_envelope_bytes(&comparative[1])?
        );
        let comparative_ratios = project_basis(&comparative)?;
        let parent_margin = &comparative_ratios.items()[2];
        assert_eq!(parent_margin.metric(), CompanyRatioMetric::NetMargin);
        assert_eq!(parent_margin.state(), CompanyRatioState::Reported);
        assert_eq!(
            parent_margin.display_name,
            "Net margin attributable to parent"
        );
        assert_eq!(
            parent_margin.value(),
            Some(Decimal::from(2173) / Decimal::from(25182))
        );
        assert_eq!(parent_margin.inputs().len(), 2);
        assert_eq!(parent_margin.inputs()[0].fact(), &comparative[1]);
        assert_eq!(parent_margin.inputs()[1].fact(), &comparative[0]);
        assert_eq!(
            project_financial_envelope(&comparative, true)?[2],
            serde_json::to_value(parent_margin)?
        );
        let comparative_statements = project_statements(
            &comparative,
            CompanyProductSectionState::Reported,
            &mut CompanySerializedBudget::new(),
        )?;
        assert_eq!(comparative_statements.groups().len(), 1);
        assert_eq!(
            comparative_statements.groups()[0].items().len(),
            comparative.len()
        );
        for original in &original_comparative {
            assert!(
                comparative_statements.groups()[0]
                    .items()
                    .contains(original)
            );
        }
        assert_eq!(comparative, original_comparative);

        // Removing the unrelated ordinal must not hide conflicting values of one concept,
        // select a later ordinal itself, or fall back to consolidated income.
        let mut disagreeing = comparative[1].clone();
        disagreeing.reporting_context.occurrence = RevisionNumber::new(4)?;
        disagreeing.value += Decimal::ONE;
        let mut conflict = comparative.clone();
        conflict.push(disagreeing);
        assert_eq!(
            ratio_states(&project_basis(&conflict)?, CompanyRatioMetric::NetMargin),
            vec![CompanyRatioState::ConflictingInput]
        );
        assert_eq!(
            project_financial_envelope(&conflict, true)?[2]["state"],
            "conflicting_input"
        );

        // The selected revision state, publication, precise period and knowledge clock
        // still prevent cross-envelope joins even when the source ordinal happens to match.
        let mut other_revision = comparative[1].clone();
        other_revision.revision = CompanyProductRevisionState::Superseded;
        let mut other_publication = comparative[1].clone();
        other_publication.lineage.publication_identity = [8; 32];
        let mut other_period = comparative[1].clone();
        other_period.period =
            FundamentalPeriod::duration(CalendarDate::new(2024, 1, 1)?, comparative_end)?;
        let mut other_knowledge = comparative[1].clone();
        other_knowledge.known_at = Timestamp::from_unix_nanos(known_at.unix_nanos() + 1);
        for mut other in [
            other_revision,
            other_publication,
            other_period,
            other_knowledge,
        ] {
            other.reporting_context.occurrence = comparative[0].reporting_context.occurrence;
            assert_ne!(
                fact_envelope_bytes(&comparative[0])?,
                fact_envelope_bytes(&other)?
            );
            let separate = [comparative[0].clone(), other];
            assert_eq!(
                project_financial_envelope(&separate, true),
                Err(CompanyProductProjectionError::InvalidEvidence)
            );
            assert!(
                project_basis(&separate)?
                    .items()
                    .iter()
                    .all(|ratio| ratio.state() == CompanyRatioState::MissingInput)
            );
        }

        let distinct_filings = vec![
            fact(
                CompanyFinancialMetric::CurrentAssets,
                200,
                usd,
                instant,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
            fact(
                CompanyFinancialMetric::CurrentLiabilities,
                100,
                usd,
                instant,
                year_end,
                known_at,
                "filing-b",
                2,
            )?,
        ];
        // The disk index must use the identical full private envelope key. Equal public
        // dates and periods never join distinct original filing/publication receipts.
        assert_ne!(
            fact_envelope_bytes(&distinct_filings[0])?,
            fact_envelope_bytes(&distinct_filings[1])?
        );
        assert_eq!(
            project_financial_envelope(&distinct_filings, true),
            Err(CompanyProductProjectionError::InvalidEvidence)
        );
        let instant_envelope = &facts[..2];
        assert_eq!(
            fact_envelope_bytes(&instant_envelope[0])?,
            fact_envelope_bytes(&instant_envelope[1])?
        );
        let projected = project_financial_envelope(instant_envelope, true)?;
        assert_eq!(projected.len(), 1);
        assert!(projected.iter().any(|ratio| {
            ratio["metric"] == "current_ratio"
                && ratio["state"] == "reported"
                && ratio["inputs"]
                    .as_array()
                    .is_some_and(|inputs| inputs.len() == 2)
        }));

        // Share and cash-flow contexts do not create unsupported ratio-period rows.
        for (metric, period) in [
            (
                CompanyFinancialMetric::EntityCommonSharesOutstanding,
                instant,
            ),
            (CompanyFinancialMetric::WeightedAverageBasicShares, duration),
            (CompanyFinancialMetric::OperatingCashFlow, duration),
        ] {
            let mut unrelated = fact(metric, 100, usd, period, year_end, known_at, "filing-a", 1)?;
            if metric.expected_unit() == CompanyMetricUnit::Shares {
                unrelated.unit = CompanyFactUnit::Shares;
            }
            assert!(project_financial_envelope(&[unrelated], true)?.is_empty());
        }

        // Unrelated balance-sheet/operating notes report neither operand. They retain
        // their facts and statements but must not manufacture missing default ratios.
        for (metric, period) in [
            (CompanyFinancialMetric::TotalAssets, instant),
            (CompanyFinancialMetric::OperatingExpenses, duration),
        ] {
            let unrelated = fact(metric, 100, usd, period, year_end, known_at, "filing-a", 1)?;
            let empty = project_ratios(
                std::slice::from_ref(&unrelated),
                CompanyProductSectionState::Reported,
                &mut CompanySerializedBudget::new(),
            )?;
            assert_eq!(empty.state(), CompanyProductSectionState::Unavailable);
            assert!(empty.items().is_empty());
            assert!(project_financial_envelope(std::slice::from_ref(&unrelated), true)?.is_empty());
            assert_eq!(project_financial_envelope(&[unrelated], false)?.len(), 1);
        }

        // Later equity-only filing contexts must not eclipse the complete same-date
        // balance sheet. Both consumers use the same applicability and exact lineage.
        for scope in [
            CompanyFactProductScope::CompanyWide,
            CompanyFactProductScope::FilingDetail,
        ] {
            let mut complete = facts[..2].to_vec();
            let mut note = fact(
                CompanyFinancialMetric::ShareholdersEquity,
                300,
                usd,
                instant,
                CalendarDate::new(2026, 3, 1)?,
                known_at,
                "later-equity-note",
                9,
            )?;
            note.effective = CompanyProductTime::CalendarDate(year_end);
            note.scope = scope;
            for (index, item) in complete.iter_mut().enumerate() {
                item.scope = scope;
                if scope == CompanyFactProductScope::FilingDetail {
                    item.lineage.xbrl_identity = Some((
                        SourceIdentifier::try_from("balance-context")?,
                        SourceIdentifier::try_from(format!("balance-{index}"))?,
                    ));
                }
            }
            if scope == CompanyFactProductScope::FilingDetail {
                note.lineage.xbrl_identity = Some((
                    SourceIdentifier::try_from("equity-context")?,
                    SourceIdentifier::try_from("equity-note")?,
                ));
            }
            let original_note = note.clone();
            let mut combined = vec![note.clone()];
            combined.extend(complete.clone());
            let whole = project_ratios(
                &combined,
                CompanyProductSectionState::Reported,
                &mut CompanySerializedBudget::new(),
            )?;
            assert_eq!(whole.items().len(), 1);
            assert_eq!(whole.items()[0].value(), Some(Decimal::from(2)));
            assert_eq!(
                whole.items()[0].envelope(),
                Some(reporting_envelope(&complete[0]))
            );
            assert!(project_financial_envelope(std::slice::from_ref(&note), true)?.is_empty());
            assert_eq!(
                project_financial_envelope(&complete, true)?[0],
                serde_json::to_value(&whole.items()[0])?
            );
            let evidence = project_statements(
                &combined,
                CompanyProductSectionState::Reported,
                &mut CompanySerializedBudget::new(),
            )?;
            assert!(
                evidence
                    .groups()
                    .iter()
                    .any(|group| group.items().contains(&original_note))
            );

            // Explicit nil/nonnumeric operands and unchecked sidecars are not absence.
            // Neither can silently fall back to the earlier complete candidate.
            for coverage in [
                None,
                Some(financial_input_bit("AssetsCurrent")),
                Some(financial_input_bit("LiabilitiesCurrent")),
            ] {
                note.lineage.nonnumeric_inputs = coverage;
                let unavailable = project_financial_envelope(std::slice::from_ref(&note), true)?;
                assert_eq!(unavailable.len(), 1);
                assert_eq!(unavailable[0]["state"], "missing_input");
                assert!(unavailable[0]["value"].is_null());
            }
        }

        // Nonnumeric total revenue is still the chosen financial basis. It cannot be
        // bypassed by a numeric narrower revenue concept or by operand omission.
        let mut nil_revenue = facts[2..].to_vec();
        for item in &mut nil_revenue {
            item.lineage.nonnumeric_inputs = Some(financial_input_bit("Revenues"));
        }
        assert!(
            project_ratios(
                &nil_revenue,
                CompanyProductSectionState::Reported,
                &mut CompanySerializedBudget::new()
            )?
            .items()
            .iter()
            .all(
                |ratio| ratio.state() == CompanyRatioState::MissingInput && ratio.value().is_none()
            )
        );
        let mut nil_note = fact(
            CompanyFinancialMetric::OperatingExpenses,
            100,
            usd,
            duration,
            year_end,
            known_at,
            "nil-note",
            4,
        )?;
        nil_note.lineage.nonnumeric_inputs = Some(financial_input_bit("Revenues"));
        let nil_margins = project_financial_envelope(&[nil_note], true)?;
        assert_eq!(nil_margins.len(), 3);
        assert!(
            nil_margins
                .iter()
                .all(|ratio| ratio["state"] == "missing_input")
        );
        let missing_margins = project_financial_envelope(&facts[2..3], true)?;
        assert_eq!(missing_margins.len(), 3);
        assert!(missing_margins.iter().all(|ratio| {
            ratio["state"] == "missing_input"
                && ratio["inputs"]
                    .as_array()
                    .is_some_and(|inputs| inputs.len() == 1 && inputs[0]["role"] == "denominator")
        }));

        let distinct_ratios = project_ratios(
            &distinct_filings,
            CompanyProductSectionState::Reported,
            &mut CompanySerializedBudget::new(),
        )?;
        assert_eq!(
            ratio_states(&distinct_ratios, CompanyRatioMetric::CurrentRatio),
            vec![
                CompanyRatioState::MissingInput,
                CompanyRatioState::MissingInput
            ]
        );

        let conflict = vec![
            fact(
                CompanyFinancialMetric::CurrentAssets,
                200,
                usd,
                instant,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
            fact(
                CompanyFinancialMetric::CurrentAssets,
                210,
                usd,
                instant,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
            fact(
                CompanyFinancialMetric::CurrentLiabilities,
                100,
                usd,
                instant,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
        ];
        assert_eq!(
            ratio_states(
                &project_ratios(
                    &conflict,
                    CompanyProductSectionState::Reported,
                    &mut CompanySerializedBudget::new(),
                )?,
                CompanyRatioMetric::CurrentRatio,
            ),
            vec![CompanyRatioState::ConflictingInput]
        );

        let incompatible = vec![
            fact(
                CompanyFinancialMetric::CurrentAssets,
                200,
                usd,
                instant,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
            fact(
                CompanyFinancialMetric::CurrentLiabilities,
                100,
                eur,
                instant,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
        ];
        assert_eq!(
            ratio_states(
                &project_ratios(
                    &incompatible,
                    CompanyProductSectionState::Reported,
                    &mut CompanySerializedBudget::new(),
                )?,
                CompanyRatioMetric::CurrentRatio,
            ),
            vec![CompanyRatioState::IncompatibleUnits]
        );

        let zero_denominator = vec![
            fact(
                CompanyFinancialMetric::CurrentAssets,
                200,
                usd,
                instant,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
            fact(
                CompanyFinancialMetric::CurrentLiabilities,
                0,
                usd,
                instant,
                year_end,
                known_at,
                "filing-a",
                1,
            )?,
        ];
        assert_eq!(
            ratio_states(
                &project_ratios(
                    &zero_denominator,
                    CompanyProductSectionState::Reported,
                    &mut CompanySerializedBudget::new(),
                )?,
                CompanyRatioMetric::CurrentRatio,
            ),
            vec![CompanyRatioState::ZeroDenominator]
        );
        Ok(())
    }

    fn ratio_states(
        ratios: &CompanyRatiosProduct,
        metric: CompanyRatioMetric,
    ) -> Vec<CompanyRatioState> {
        ratios
            .items()
            .iter()
            .filter(|ratio| ratio.metric() == metric)
            .map(CompanyRatioProduct::state)
            .collect()
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one exact ratio-envelope fixture"
    )]
    fn fact(
        metric: CompanyFinancialMetric,
        value: i64,
        currency: Currency,
        period: FundamentalPeriod,
        filed_on: CalendarDate,
        known_at: Timestamp,
        filing_identity: &str,
        publication_byte: u8,
    ) -> anyhow::Result<CompanyFactProduct> {
        Ok(CompanyFactProduct {
            lineage: CompanyFactPrivateLineage {
                filing_identity: filing_identity.into(),
                publication_identity: [publication_byte; 32],
                xbrl_identity: None,
                nonnumeric_inputs: Some(0),
            },
            scope: CompanyFactProductScope::CompanyWide,
            revision: CompanyProductRevisionState::Current,
            metric,
            display_name: metric.display_name(),
            value: Decimal::from(value),
            unit: CompanyFactUnit::Currency { currency },
            period,
            fiscal_context: CompanyFactFiscalContext {
                fiscal_year: Some(2025),
                fiscal_period: CompanyFactFiscalPeriod::FiscalYear,
                cadence: CompanyFactCadence::Annual,
            },
            reporting_context: CompanyFactReportingContext {
                dimensionality: CompanyFactDimensionality::NoDimensions,
                consolidation: CompanyFactConsolidation::ReportedConsolidated,
                amendment: CompanyFactAmendment::Original,
                restatement: CompanyFactRestatement::ReportedNotRestated,
                occurrence: RevisionNumber::new(1)?,
            },
            filed_on: Some(filed_on),
            effective: CompanyProductTime::CalendarDate(filed_on),
            known_at,
        })
    }
}
