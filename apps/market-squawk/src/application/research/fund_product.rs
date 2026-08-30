//! Provider-neutral fund/share-class research for ordinary product consumers.
//!
//! The projection consumes only exact canonical point-in-time fund evidence and, when composed,
//! an exact latest-known daily NAV read. It never promotes a ticker or provider association to
//! identity, never substitutes market price for NAV, and never exposes filing coordinates,
//! provider fields, manifests, digests, or runtime state.

use market_squawk_data::{
    AnalyticalFundNavOutput, AnalyticalFundNavReadRequest, PointInTimeRevisionMode,
};
use market_squawk_domain::{
    CalendarDate, Currency, FundAmendmentState, FundConflictState, FundCurrencyAmount,
    FundHoldingQuantity, FundHoldingUnit, FundMissingState, FundNavCorrectionState,
    FundNavFinality, FundNavMissingState, FundNavObservation, FundNavValue, FundReportedDecimal,
    FundReportedValue, FundRevisionStatus, InstrumentId, Money, Timestamp,
};
use serde::Serialize;
use thiserror::Error;

use super::company_product::ResearchProductIdentity;
use super::company_research::{
    FundResearchFamily, FundResearchOutcome, FundResearchRead, FundResearchSnapshot,
    FundResearchUnavailableReason,
};
use super::sec_fund_product::{
    FundAnnualInformationData, FundHoldingData, FundResearchAvailability, FundResearchData,
    FundResearchFilingState,
};
use crate::application::domain_support::{ProductTextCopyError, try_boxed_product_text};

const MAX_FUND_PRODUCT_PROJECTED_BYTES: usize = 64 * 1024 * 1024;
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
    clocks: FundProductClocks,
    coverage: FundProductCoverage,
    limitations: Box<[FundProductLimitation]>,
}

impl FundProductResult {
    fn bind_display_identities(
        &mut self,
        instrument_id: InstrumentId,
        identity: ResearchProductIdentity,
        holdings: &[Option<ResearchProductIdentity>],
    ) -> Result<(), FundProductProjectionError> {
        if self.fund_share_class_instrument_id != instrument_id
            || self.identity.is_some()
            || (!self.holdings.items.is_empty() && self.holdings.items.len() != holdings.len())
        {
            return Err(FundProductProjectionError::InvalidEvidence);
        }
        for (holding, identity) in self.holdings.items.iter_mut().zip(holdings) {
            if holding.instrument_id.is_none() && identity.is_some() {
                return Err(FundProductProjectionError::InvalidEvidence);
            }
            holding.identity = identity.clone();
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

    pub(crate) const fn clocks(&self) -> &FundProductClocks {
        &self.clocks
    }

    pub(crate) const fn coverage(&self) -> FundProductCoverage {
        self.coverage
    }

    pub(crate) fn limitations(&self) -> &[FundProductLimitation] {
        &self.limitations
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
    monthly_average_net_assets: FundProductValue<Box<str>>,
    daily_average_net_assets: FundProductValue<Box<str>>,
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
}

/// Projects a verified canonical fund read and an optional exact latest-known NAV read.
pub(crate) fn project_fund_product(
    reads: FundProductReadSet<'_>,
    nav: Option<FundNavProductRead<'_>>,
    identity: ResearchProductIdentity,
    holding_identities: &[Option<ResearchProductIdentity>],
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
    result.bind_display_identities(instrument_id, identity, holding_identities)?;
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
    let facts = FundAnnualFactsProduct {
        reporting_period_less_than_twelve_months: project_annual_scalar(
            information.reporting_period_less_than_twelve_months(),
            &mut coverage,
        )?,
        reporting_currency: project_annual_scalar(information.reporting_currency(), &mut coverage)?,
        monthly_average_net_assets: project_annual_decimal(
            information.monthly_average_net_assets(),
            &mut coverage,
            budget,
        )?,
        daily_average_net_assets: project_annual_decimal(
            information.daily_average_net_assets(),
            &mut coverage,
            budget,
        )?,
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
