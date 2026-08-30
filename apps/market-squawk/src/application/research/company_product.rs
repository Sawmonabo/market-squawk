//! Provider-neutral company fundamentals and filings for ordinary product consumers.
//!
//! The durable SEC selector retains provider, filing-coordinate, object, manifest, digest, and
//! restart evidence privately. This leaf exposes only the canonical instrument, exact reported
//! financial facts, filing meaning, knowledge clocks, coverage, and honest limitations. It does
//! not derive statements or ratios from taxonomy labels: those sections remain unavailable until
//! an exact canonical calculation read is composed.

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
    statements: CompanyCalculationProduct,
    ratios: CompanyCalculationProduct,
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

    pub(crate) const fn statements(&self) -> &CompanyCalculationProduct {
        &self.statements
    }

    pub(crate) const fn ratios(&self) -> &CompanyCalculationProduct {
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

/// Statements and ratios are absent until a typed canonical calculation supplies them.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompanyCalculationProduct {
    availability: CompanyCalculationAvailability,
}

impl CompanyCalculationProduct {
    const UNAVAILABLE: Self = Self {
        availability: CompanyCalculationAvailability::NotCalculated,
    };

    pub(crate) const fn availability(self) -> CompanyCalculationAvailability {
        self.availability
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompanyCalculationAvailability {
    NotCalculated,
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
    StatementsNotCalculated,
    RatiosNotCalculated,
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

    let mut limitations = calculation_limitations()?;
    if partial {
        limitations
            .try_reserve(1)
            .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
        limitations.push(CompanyProductLimitation::SomeCompanyInformationMissing);
    }
    if omitted_facts != 0 {
        limitations
            .try_reserve(1)
            .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
        limitations.push(CompanyProductLimitation::SomeReportedCompanyFactsNotShown);
    }
    let available_product_sections =
        usize::from(facts_state == CompanyProductSectionState::Reported)
            .checked_add(usize::from(
                filings_state == CompanyProductSectionState::Reported,
            ))
            .ok_or(CompanyProductProjectionError::ResourceExhausted)?;
    let availability = match available_product_sections {
        0 => {
            limitations
                .try_reserve(1)
                .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
            limitations.push(CompanyProductLimitation::NoSupportedCompanyInformationToShow);
            CompanyProductAvailability::Unavailable
        }
        COMPANY_PRODUCT_SECTIONS => CompanyProductAvailability::Available,
        _ => CompanyProductAvailability::Partial,
    };
    let reported_facts = facts.len();
    Ok(CompanyProductResult {
        instrument_id,
        identity: None,
        availability,
        facts: CompanyFactsProduct {
            state: facts_state,
            items: facts.into_boxed_slice(),
        },
        statements: CompanyCalculationProduct::UNAVAILABLE,
        ratios: CompanyCalculationProduct::UNAVAILABLE,
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
            filing_events: snapshot.filing_events().len(),
        },
        limitations: limitations.into_boxed_slice(),
    })
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
        statements: CompanyCalculationProduct::UNAVAILABLE,
        ratios: CompanyCalculationProduct::UNAVAILABLE,
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
            filing_events: 0,
        },
        limitations: Box::new([
            primary_limitation,
            CompanyProductLimitation::StatementsNotCalculated,
            CompanyProductLimitation::RatiosNotCalculated,
        ]),
    }
}

fn calculation_limitations() -> Result<Vec<CompanyProductLimitation>, CompanyProductProjectionError>
{
    let mut limitations = Vec::new();
    limitations
        .try_reserve_exact(2)
        .map_err(|_| CompanyProductProjectionError::ResourceExhausted)?;
    limitations.push(CompanyProductLimitation::StatementsNotCalculated);
    limitations.push(CompanyProductLimitation::RatiosNotCalculated);
    Ok(limitations)
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
