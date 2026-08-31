//! Provider-neutral company fundamentals and filings for ordinary product consumers.
//!
//! This leaf exposes only the canonical instrument, exact reported financial facts, code-owned
//! statement meaning, exact-input ratios, filing meaning, knowledge clocks, coverage, and honest
//! limitations. Selection and persistence evidence remain private.

use market_squawk_domain::{
    CalendarDate, Currency, FundamentalAmendmentStatus, FundamentalCadence,
    FundamentalConsolidation, FundamentalPeriod, InstrumentId, ResearchTemporalCoordinate,
    RevisionNumber, Timestamp,
};
use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

use super::company_research::{
    CompanyFactScope, CompanyResearchDimensionState, CompanyResearchFact, CompanyResearchFiling,
    CompanyResearchFiscalPeriod, CompanyResearchOutcome, CompanyResearchRead,
    CompanyResearchRestatementState, CompanyResearchRevisionState, CompanyResearchSnapshot,
    CompanyResearchSurfaceAvailability, CompanyResearchUnavailableReason,
};
use crate::application::domain_support::{ProductTextCopyError, try_boxed_product_text};

const COMPANY_SOURCE_SURFACES: usize = 3;
const COMPANY_PRODUCT_SECTIONS: usize = 4;
const MAX_PRODUCT_FILING_FORM_BYTES: usize = 64;

/// One closed company-research result with no provider or storage vocabulary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyFactProduct {
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
    ProfitOrLossIncludingNoncontrollingInterests,
    BasicEarningsPerShare,
    DilutedEarningsPerShare,
    OperatingCashFlow,
    InvestingCashFlow,
    FinancingCashFlow,
    PropertyPlantAndEquipmentPurchases,
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
            Self::NetIncome => "Net income or loss",
            Self::ProfitOrLossIncludingNoncontrollingInterests => {
                "Profit or loss including noncontrolling interests"
            }
            Self::BasicEarningsPerShare => "Basic earnings per share",
            Self::DilutedEarningsPerShare => "Diluted earnings per share",
            Self::OperatingCashFlow => "Operating cash flow",
            Self::InvestingCashFlow => "Investing cash flow",
            Self::FinancingCashFlow => "Financing cash flow",
            Self::PropertyPlantAndEquipmentPurchases => "Property, plant, and equipment purchases",
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

/// Exact reported facts grouped by code-owned financial-statement meaning.
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
    items: Box<[CompanyFactProduct]>,
}

impl CompanyStatementGroupProduct {
    pub(crate) const fn statement(&self) -> CompanyStatementKind {
        self.statement
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

/// Deterministic ratios whose complete exact fact lineage remains attached.
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
    value: Decimal,
    unit: CompanyRatioUnit,
    period: FundamentalPeriod,
    fiscal_context: CompanyFactFiscalContext,
    reporting_context: CompanyFactReportingContext,
    filed_on: Option<CalendarDate>,
    effective: CompanyProductTime,
    known_at: Timestamp,
    inputs: Box<[CompanyRatioInputProduct]>,
}

impl CompanyRatioProduct {
    pub(crate) const fn metric(&self) -> CompanyRatioMetric {
        self.metric
    }

    pub(crate) const fn value(&self) -> Decimal {
        self.value
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
    calculated_ratios: usize,
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

    pub(crate) const fn calculated_ratios(self) -> usize {
        self.calculated_ratios
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
        CompanyResearchOutcome::Missing => Ok(empty_result(
            request.instrument_id(),
            request.knowledge_at(),
            fact_effective_cutoff,
            CompanyProductAvailability::Missing,
            CompanyProductSectionState::Missing,
            CompanyProductLimitation::NoCompanyInformationAtCutoff,
        )),
        CompanyResearchOutcome::Ambiguous => Ok(empty_result(
            request.instrument_id(),
            request.knowledge_at(),
            fact_effective_cutoff,
            CompanyProductAvailability::Conflict,
            CompanyProductSectionState::Conflict,
            CompanyProductLimitation::IdentityAmbiguous,
        )),
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
            Ok(empty_result(
                request.instrument_id(),
                request.knowledge_at(),
                fact_effective_cutoff,
                availability,
                section,
                limitation,
            ))
        }
    }?;
    result.bind_identity(request.instrument_id(), identity)?;
    Ok(result)
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

    let mut facts = Vec::new();
    facts
        .try_reserve_exact(snapshot.facts().len())
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    let mut omitted_facts = 0_usize;
    for fact in snapshot.facts() {
        if let Some(fact) = project_fact(fact, knowledge_cutoff)? {
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
        filings.push(project_filing(filing, knowledge_cutoff)?);
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

    let statements = project_statements(&facts, facts_state)?;
    let ratios = project_ratios(&facts, facts_state)?;
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
    let availability = match available_product_sections {
        0 => {
            limitations.push(CompanyProductLimitation::NoSupportedCompanyInformationToShow);
            CompanyProductAvailability::Unavailable
        }
        COMPANY_PRODUCT_SECTIONS => CompanyProductAvailability::Available,
        _ => CompanyProductAvailability::Partial,
    };
    let reported_facts = facts.len();
    let statement_lines = statements
        .groups
        .iter()
        .try_fold(0_usize, |count, group| count.checked_add(group.items.len()))
        .ok_or(CompanyProductProjectionError::ResourceExhausted)?;
    let calculated_ratios = ratios.items.len();
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
            calculated_ratios,
            filing_events: snapshot.filing_events().len(),
        },
        limitations: limitations.into_boxed_slice(),
    })
}

fn project_statements(
    facts: &[CompanyFactProduct],
    source_state: CompanyProductSectionState,
) -> Result<CompanyStatementsProduct, CompanyProductProjectionError> {
    if source_state != CompanyProductSectionState::Reported {
        return Ok(CompanyStatementsProduct {
            state: source_state,
            groups: Box::new([]),
        });
    }

    let mut financial_position = Vec::new();
    let mut operations = Vec::new();
    let mut cash_flows = Vec::new();
    let mut share_data = Vec::new();
    for fact in facts {
        let target = match statement_for_metric(fact.metric) {
            CompanyStatementKind::FinancialPosition => &mut financial_position,
            CompanyStatementKind::Operations => &mut operations,
            CompanyStatementKind::CashFlows => &mut cash_flows,
            CompanyStatementKind::ShareData => &mut share_data,
        };
        target
            .try_reserve(1)
            .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
        target.push(fact.clone());
    }
    if facts.is_empty() {
        return Err(CompanyProductProjectionError::InvalidEvidence);
    }

    let mut groups = Vec::new();
    groups
        .try_reserve_exact(4)
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    append_statement_group(
        &mut groups,
        CompanyStatementKind::FinancialPosition,
        financial_position,
    );
    append_statement_group(&mut groups, CompanyStatementKind::Operations, operations);
    append_statement_group(&mut groups, CompanyStatementKind::CashFlows, cash_flows);
    append_statement_group(&mut groups, CompanyStatementKind::ShareData, share_data);
    Ok(CompanyStatementsProduct {
        state: CompanyProductSectionState::Reported,
        groups: groups.into_boxed_slice(),
    })
}

fn append_statement_group(
    groups: &mut Vec<CompanyStatementGroupProduct>,
    statement: CompanyStatementKind,
    items: Vec<CompanyFactProduct>,
) {
    if !items.is_empty() {
        groups.push(CompanyStatementGroupProduct {
            statement,
            items: items.into_boxed_slice(),
        });
    }
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
        | CompanyFinancialMetric::ProfitOrLossIncludingNoncontrollingInterests
        | CompanyFinancialMetric::BasicEarningsPerShare
        | CompanyFinancialMetric::DilutedEarningsPerShare => CompanyStatementKind::Operations,
        CompanyFinancialMetric::OperatingCashFlow
        | CompanyFinancialMetric::InvestingCashFlow
        | CompanyFinancialMetric::FinancingCashFlow
        | CompanyFinancialMetric::PropertyPlantAndEquipmentPurchases => {
            CompanyStatementKind::CashFlows
        }
        CompanyFinancialMetric::EntityCommonSharesOutstanding
        | CompanyFinancialMetric::CommonStockSharesOutstanding
        | CompanyFinancialMetric::WeightedAverageBasicShares
        | CompanyFinancialMetric::WeightedAverageDilutedShares => CompanyStatementKind::ShareData,
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RatioLineageKey {
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
    occurrence: u32,
    filed_on_present: bool,
    filed_on: i32,
    effective_kind: u8,
    effective_value: i64,
}

#[derive(Clone, Copy)]
struct RatioFactRef<'fact> {
    key: RatioLineageKey,
    fact: &'fact CompanyFactProduct,
}

fn project_ratios(
    facts: &[CompanyFactProduct],
    source_state: CompanyProductSectionState,
) -> Result<CompanyRatiosProduct, CompanyProductProjectionError> {
    if source_state != CompanyProductSectionState::Reported {
        return Ok(CompanyRatiosProduct {
            state: source_state,
            items: Box::new([]),
        });
    }

    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(facts.len())
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    for fact in facts.iter().filter(|fact| ratio_metric(fact.metric)) {
        candidates.push(RatioFactRef {
            key: ratio_lineage_key(fact),
            fact,
        });
    }
    candidates.sort_unstable_by_key(|candidate| candidate.key);

    let mut ratios = Vec::new();
    ratios
        .try_reserve(facts.len().min(4_096))
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    let mut start = 0_usize;
    while start < candidates.len() {
        let key = candidates[start].key;
        let mut end = start + 1;
        while end < candidates.len() && candidates[end].key == key {
            end += 1;
        }
        let group = &candidates[start..end];
        append_ratio(
            &mut ratios,
            group,
            CompanyRatioMetric::CurrentRatio,
            &[CompanyFinancialMetric::CurrentAssets],
            &[CompanyFinancialMetric::CurrentLiabilities],
        )?;
        append_ratio(
            &mut ratios,
            group,
            CompanyRatioMetric::GrossMargin,
            &[CompanyFinancialMetric::GrossProfit],
            revenue_metrics(),
        )?;
        append_ratio(
            &mut ratios,
            group,
            CompanyRatioMetric::OperatingMargin,
            &[CompanyFinancialMetric::OperatingIncome],
            revenue_metrics(),
        )?;
        append_ratio(
            &mut ratios,
            group,
            CompanyRatioMetric::NetMargin,
            &[
                CompanyFinancialMetric::NetIncome,
                CompanyFinancialMetric::ProfitOrLossIncludingNoncontrollingInterests,
            ],
            revenue_metrics(),
        )?;
        start = end;
    }
    let state = if ratios.is_empty() {
        CompanyProductSectionState::Missing
    } else {
        CompanyProductSectionState::Reported
    };
    Ok(CompanyRatiosProduct {
        state,
        items: ratios.into_boxed_slice(),
    })
}

const fn revenue_metrics() -> &'static [CompanyFinancialMetric] {
    &[
        CompanyFinancialMetric::CustomerRevenueExcludingAssessedTax,
        CompanyFinancialMetric::Revenue,
        CompanyFinancialMetric::NetSales,
    ]
}

const fn ratio_metric(metric: CompanyFinancialMetric) -> bool {
    matches!(
        metric,
        CompanyFinancialMetric::CurrentAssets
            | CompanyFinancialMetric::CurrentLiabilities
            | CompanyFinancialMetric::Revenue
            | CompanyFinancialMetric::NetSales
            | CompanyFinancialMetric::CustomerRevenueExcludingAssessedTax
            | CompanyFinancialMetric::GrossProfit
            | CompanyFinancialMetric::OperatingIncome
            | CompanyFinancialMetric::NetIncome
            | CompanyFinancialMetric::ProfitOrLossIncludingNoncontrollingInterests
    )
}

fn append_ratio(
    ratios: &mut Vec<CompanyRatioProduct>,
    group: &[RatioFactRef<'_>],
    metric: CompanyRatioMetric,
    numerator_metrics: &[CompanyFinancialMetric],
    denominator_metrics: &[CompanyFinancialMetric],
) -> Result<(), CompanyProductProjectionError> {
    let Ok(Some(numerator)) = unique_preferred_fact(group, numerator_metrics) else {
        return Ok(());
    };
    let Ok(Some(denominator)) = unique_preferred_fact(group, denominator_metrics) else {
        return Ok(());
    };
    if numerator.unit != denominator.unit || denominator.value.is_zero() {
        return Ok(());
    }
    let Some(value) = numerator.value.checked_div(denominator.value) else {
        return Ok(());
    };

    let mut inputs = Vec::new();
    inputs
        .try_reserve_exact(2)
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    inputs.push(CompanyRatioInputProduct {
        role: CompanyRatioInputRole::Numerator,
        fact: numerator.clone(),
    });
    inputs.push(CompanyRatioInputProduct {
        role: CompanyRatioInputRole::Denominator,
        fact: denominator.clone(),
    });
    ratios
        .try_reserve(1)
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    ratios.push(CompanyRatioProduct {
        metric,
        display_name: metric.display_name(),
        value,
        unit: CompanyRatioUnit::Ratio,
        period: numerator.period,
        fiscal_context: numerator.fiscal_context,
        reporting_context: numerator.reporting_context,
        filed_on: numerator.filed_on,
        effective: numerator.effective,
        known_at: numerator.known_at.max(denominator.known_at),
        inputs: inputs.into_boxed_slice(),
    });
    Ok(())
}

fn unique_preferred_fact<'fact>(
    group: &[RatioFactRef<'fact>],
    metrics: &[CompanyFinancialMetric],
) -> Result<Option<&'fact CompanyFactProduct>, ()> {
    for metric in metrics {
        let mut matching = group
            .iter()
            .filter(|candidate| candidate.fact.metric == *metric);
        let Some(candidate) = matching.next() else {
            continue;
        };
        if matching.next().is_some() {
            return Err(());
        }
        return Ok(Some(candidate.fact));
    }
    Ok(None)
}

fn ratio_lineage_key(fact: &CompanyFactProduct) -> RatioLineageKey {
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
    RatioLineageKey {
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
        occurrence: fact.reporting_context.occurrence.get(),
        filed_on_present: fact.filed_on.is_some(),
        filed_on: fact.filed_on.map_or(0, CalendarDate::days_since_unix_epoch),
        effective_kind,
        effective_value,
    }
}

fn project_fact(
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
    Ok(Some(CompanyFactProduct {
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

fn product_metric(source_metric: &str) -> Option<CompanyFinancialMetric> {
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

fn project_filing(
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
) -> CompanyProductResult {
    CompanyProductResult {
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
        ratios: CompanyRatiosProduct {
            state: section_state,
            items: Box::new([]),
        },
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
            calculated_ratios: 0,
            filing_events: 0,
        },
        limitations: Box::new([primary_limitation]),
    }
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

    #[test]
    fn statement_and_ratio_projection_preserves_exact_point_in_time_inputs() -> anyhow::Result<()> {
        let year_start = CalendarDate::new(2025, 1, 1)?;
        let year_end = CalendarDate::new(2025, 12, 31)?;
        let duration = FundamentalPeriod::duration(year_start, year_end)?;
        let instant = FundamentalPeriod::instant(year_end);
        let known_at = Timestamp::from_unix_nanos(1_800_000_000_000_000_000);
        let facts = vec![
            fact(
                CompanyFinancialMetric::CurrentAssets,
                200,
                instant,
                year_end,
                known_at,
            )?,
            fact(
                CompanyFinancialMetric::CurrentLiabilities,
                100,
                instant,
                year_end,
                known_at,
            )?,
            fact(
                CompanyFinancialMetric::CustomerRevenueExcludingAssessedTax,
                100,
                duration,
                year_end,
                known_at,
            )?,
            fact(
                CompanyFinancialMetric::GrossProfit,
                40,
                duration,
                year_end,
                known_at,
            )?,
            fact(
                CompanyFinancialMetric::OperatingIncome,
                20,
                duration,
                year_end,
                known_at,
            )?,
            fact(
                CompanyFinancialMetric::NetIncome,
                10,
                duration,
                year_end,
                known_at,
            )?,
        ];

        let statements = project_statements(&facts, CompanyProductSectionState::Reported)?;
        assert_eq!(statements.state(), CompanyProductSectionState::Reported);
        assert_eq!(
            statements
                .groups()
                .iter()
                .map(|group| group.items().len())
                .sum::<usize>(),
            facts.len()
        );
        let ratios = project_ratios(&facts, CompanyProductSectionState::Reported)?;
        assert_eq!(ratios.state(), CompanyProductSectionState::Reported);
        assert_eq!(ratios.items().len(), 4);
        assert_eq!(
            ratios
                .items()
                .iter()
                .find(|ratio| ratio.metric() == CompanyRatioMetric::CurrentRatio)
                .map(CompanyRatioProduct::value),
            Some(Decimal::from(2_u8))
        );
        for ratio in ratios.items() {
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
        Ok(())
    }

    fn fact(
        metric: CompanyFinancialMetric,
        value: i64,
        period: FundamentalPeriod,
        filed_on: CalendarDate,
        known_at: Timestamp,
    ) -> anyhow::Result<CompanyFactProduct> {
        let currency = Currency::try_from("USD")?;
        Ok(CompanyFactProduct {
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
