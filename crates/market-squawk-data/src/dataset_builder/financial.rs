//! Native reported fiscal amounts admitted from the existing sealed SEC selection.

use market_squawk_domain::{
    Currency, DataQuality, EvidenceDigest, FundamentalCadence, FundamentalFactContext,
    FundamentalObservation, FundamentalPeriod, HistoricalStudyBasis, InstrumentId,
    ResearchObservation, ResearchTemporalCoordinate, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    mem::size_of,
    num::{NonZeroU16, NonZeroU32},
    sync::Arc,
    time::Instant,
};

use super::{
    ComponentAdjustmentEvidence, ComponentKind, ComponentScope, ComponentSelector, ComponentValue,
    CorporateActionSensitivity, DatasetBuildError, DatasetBuilderService, DatasetExample,
    DatasetStudyPolicy, DatasetTargetHorizon, FeatureLabelComponentInput,
    FeatureLabelComponentSpec, FeatureLabelMeasurement,
};
use crate::{
    DatasetManifestRef, PointInTimeCandidate, SecResearchIdentityOutcome,
    SecResearchIdentitySelection, SecResearchSelection,
};
use tokio_util::sync::CancellationToken;

pub(super) const FINANCIAL_FEATURE: &str = "research.reported-financial-amount";
pub(super) const FINANCIAL_LABEL: &str = "research.fiscal-forward-financial-amount";
pub(super) const FINANCIAL_RECIPE: &str = "native-fiscal-reported-financial-amount-v1";
pub(super) const FINANCIAL_STUDY_RECIPE: &str = "native-fiscal-reported-financial-study-inputs-v1";
const CADENCE_RULE: &str = "sec-frame-native-contiguous-periods-v1";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinancialAmountBasis {
    ReportingEntityTotal,
    TotalCommonEquity,
    PerCommonShare,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinancialShareConvention {
    ReportedBasicEarningsPerShare,
    ReportedDilutedEarningsPerShare,
    ReportedCommonDividendPerShare,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinancialAmountRole {
    CommonNetIncome,
    ParentNetIncome,
    ParentBookEquity,
    PreferredIncomeAdjustments,
    OperatingCashFlow,
    PropertyPlantAndEquipmentPurchases,
    LongTermBorrowingProceeds,
    LongTermDebtRepayments,
    PreferredDividendsPaid,
    PreferredStockIssuedValue,
    CommonBookEquity,
    CommonEquityCashFlow,
    CommonDividend,
}

/// Selection intent only. Exact source concept/unit/context determines admitted meaning.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinancialAmountSelection {
    pub role: FinancialAmountRole,
    pub basis: FinancialAmountBasis,
    pub share_convention: Option<FinancialShareConvention>,
}

impl FinancialAmountSelection {
    pub(crate) fn mapping(self) -> Result<(&'static str, &'static str, bool), DatasetBuildError> {
        use FinancialAmountBasis::{PerCommonShare, ReportingEntityTotal, TotalCommonEquity};
        use FinancialAmountRole::*;
        use FinancialShareConvention::*;
        Ok(match (self.role, self.basis, self.share_convention) {
            (CommonNetIncome, TotalCommonEquity, None) => (
                "NetIncomeLossAvailableToCommonStockholdersBasic",
                "msq.income.common",
                false,
            ),
            (CommonNetIncome, PerCommonShare, Some(ReportedBasicEarningsPerShare)) => {
                ("EarningsPerShareBasic", "msq.income.basic-share", false)
            }
            (CommonNetIncome, PerCommonShare, Some(ReportedDilutedEarningsPerShare)) => {
                ("EarningsPerShareDiluted", "msq.income.diluted-share", false)
            }
            (ParentNetIncome, ReportingEntityTotal, None) => {
                ("NetIncomeLoss", "msq.income.parent", false)
            }
            (CommonBookEquity, TotalCommonEquity, None) => {
                ("StockholdersEquity", "msq.book.common", true)
            }
            (ParentBookEquity, ReportingEntityTotal, None) => {
                ("StockholdersEquity", "msq.book.parent", true)
            }
            (PreferredIncomeAdjustments, ReportingEntityTotal, None) => (
                "PreferredStockDividendsAndOtherAdjustments",
                "msq.pref.income-adjust",
                false,
            ),
            (OperatingCashFlow, ReportingEntityTotal, None) => (
                "NetCashProvidedByUsedInOperatingActivities",
                "msq.cfo.entity",
                false,
            ),
            (PropertyPlantAndEquipmentPurchases, ReportingEntityTotal, None) => (
                "PaymentsToAcquirePropertyPlantAndEquipment",
                "msq.ppe-purchases.entity",
                false,
            ),
            (LongTermBorrowingProceeds, ReportingEntityTotal, None) => (
                "ProceedsFromIssuanceOfLongTermDebt",
                "msq.lt-borrow.entity",
                false,
            ),
            (LongTermDebtRepayments, ReportingEntityTotal, None) => {
                ("RepaymentsOfLongTermDebt", "msq.lt-repay.entity", false)
            }
            (PreferredDividendsPaid, ReportingEntityTotal, None) => (
                "PaymentsOfDividendsPreferredStockAndPreferenceStock",
                "msq.pref-dividend.entity",
                false,
            ),
            (PreferredStockIssuedValue, ReportingEntityTotal, None) => {
                ("PreferredStockValue", "msq.pref-issued.entity", true)
            }
            _ => return Err(DatasetBuildError::ComponentEvidenceMismatch),
        })
    }
    pub(crate) fn from_unit(unit: &str) -> Option<Self> {
        use FinancialAmountBasis::*;
        use FinancialAmountRole::*;
        use FinancialShareConvention::*;
        let (role, basis, share_convention) = match unit {
            "msq.income.common" => (CommonNetIncome, TotalCommonEquity, None),
            "msq.income.basic-share" => (
                CommonNetIncome,
                PerCommonShare,
                Some(ReportedBasicEarningsPerShare),
            ),
            "msq.income.diluted-share" => (
                CommonNetIncome,
                PerCommonShare,
                Some(ReportedDilutedEarningsPerShare),
            ),
            "msq.income.parent" => (ParentNetIncome, ReportingEntityTotal, None),
            "msq.book.common" => (CommonBookEquity, TotalCommonEquity, None),
            "msq.book.parent" => (ParentBookEquity, ReportingEntityTotal, None),
            "msq.pref.income-adjust" => (PreferredIncomeAdjustments, ReportingEntityTotal, None),
            "msq.cfo.entity" => (OperatingCashFlow, ReportingEntityTotal, None),
            "msq.ppe-purchases.entity" => (
                PropertyPlantAndEquipmentPurchases,
                ReportingEntityTotal,
                None,
            ),
            "msq.lt-borrow.entity" => (LongTermBorrowingProceeds, ReportingEntityTotal, None),
            "msq.lt-repay.entity" => (LongTermDebtRepayments, ReportingEntityTotal, None),
            "msq.pref-dividend.entity" => (PreferredDividendsPaid, ReportingEntityTotal, None),
            "msq.pref-issued.entity" => (PreferredStockIssuedValue, ReportingEntityTotal, None),
            _ => return None,
        };
        Some(Self {
            role,
            basis,
            share_convention,
        })
    }
    pub(crate) const fn measurement(self, currency: Currency) -> FeatureLabelMeasurement {
        FeatureLabelMeasurement::FinancialAmount {
            currency,
            role: self.role,
            basis: self.basis,
            share_convention: self.share_convention,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FinancialSeriesLimits {
    max_facts: usize,
    max_periods: usize,
    max_bytes: usize,
}
impl FinancialSeriesLimits {
    pub fn try_new(
        max_facts: usize,
        max_periods: usize,
        max_bytes: usize,
    ) -> Result<Self, DatasetBuildError> {
        if max_facts == 0
            || max_facts > 4096
            || max_periods == 0
            || max_periods > 1024
            || max_bytes == 0
            || max_bytes > 32 * 1024 * 1024
        {
            return Err(DatasetBuildError::InvalidLimits);
        }
        Ok(Self {
            max_facts,
            max_periods,
            max_bytes,
        })
    }
}

/// One closed recipe over original source inputs. These values grant no source authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum FinancialSourceInputs<T> {
    Reported {
        amount: T,
    },
    CommonBookEquity {
        parent_equity: T,
        preferred_equity: T,
    },
}
impl<T: Eq> Eq for FinancialSourceInputs<T> {}
impl<T> FinancialSourceInputs<T> {
    pub(crate) fn primary(&self) -> &T {
        match self {
            Self::Reported { amount } => amount,
            Self::CommonBookEquity { parent_equity, .. } => parent_equity,
        }
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        let (first, second) = match self {
            Self::Reported { amount } => (amount, None),
            Self::CommonBookEquity {
                parent_equity,
                preferred_equity,
            } => (parent_equity, Some(preferred_equity)),
        };
        std::iter::once(first).chain(second)
    }
    fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        let (first, second) = match self {
            Self::Reported { amount } => (amount, None),
            Self::CommonBookEquity {
                parent_equity,
                preferred_equity,
            } => (parent_equity, Some(preferred_equity)),
        };
        std::iter::once(first).chain(second)
    }
    pub(crate) fn try_map<U, E>(
        &self,
        mut convert: impl FnMut(&T) -> Result<U, E>,
    ) -> Result<FinancialSourceInputs<U>, E> {
        Ok(match self {
            Self::Reported { amount } => FinancialSourceInputs::Reported {
                amount: convert(amount)?,
            },
            Self::CommonBookEquity {
                parent_equity,
                preferred_equity,
            } => FinancialSourceInputs::CommonBookEquity {
                parent_equity: convert(parent_equity)?,
                preferred_equity: convert(preferred_equity)?,
            },
        })
    }
}
impl FinancialSourceInputs<FundamentalObservation> {
    pub(crate) fn amount(
        &self,
        selection: FinancialAmountSelection,
    ) -> Result<Decimal, DatasetBuildError> {
        let (concept, _, instant) = selection.mapping()?;
        let primary = self.primary();
        if primary.concept().as_str().strip_prefix("us-gaap:") != Some(concept)
            || matches!(
                primary.fact_context().period(),
                FundamentalPeriod::Instant { .. }
            ) != instant
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        match (
            self,
            selection.role,
            selection.basis,
            selection.share_convention,
        ) {
            (
                Self::CommonBookEquity {
                    parent_equity,
                    preferred_equity,
                },
                FinancialAmountRole::CommonBookEquity,
                FinancialAmountBasis::TotalCommonEquity,
                None,
            ) => {
                if preferred_equity.concept().as_str()
                    != "us-gaap:PreferredStockIncludingAdditionalPaidInCapitalNetOfDiscount"
                    || !same_scope(parent_equity, preferred_equity)
                    || !common_book_context_matches(
                        parent_equity.fact_context(),
                        preferred_equity.fact_context(),
                    )
                    || parent_equity.unit() != preferred_equity.unit()
                {
                    return Err(DatasetBuildError::ComponentEvidenceMismatch);
                }
                parent_equity
                    .value()
                    .checked_sub(preferred_equity.value())
                    .ok_or(DatasetBuildError::ComponentEvidenceMismatch)
            }
            (Self::Reported { amount }, role, _, _)
                if role != FinancialAmountRole::CommonBookEquity =>
            {
                Ok(amount.value())
            }
            _ => Err(DatasetBuildError::ComponentEvidenceMismatch),
        }
    }
}

/// One exact source-selected row reference; no label amount is present in period evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinancialPeriodRowReference {
    row_ordinal: u32,
    canonical_row_digest: EvidenceDigest,
    observation_digest: EvidenceDigest,
    point_in_time_evidence: [u8; 32],
    fact_context: FundamentalFactContext,
}
impl FinancialPeriodRowReference {
    fn dynamic_retained_bytes(&self) -> usize {
        let context = &self.fact_context;
        // The source producer and binding decoder admit only unavailable or empty dimensions.
        // Both representations retain no dimension elements or nested XML allocations.
        let identifiers = [
            Some(context.unit()),
            Some(context.accession()),
            context.filing_form(),
            context.frame(),
            context.fiscal_period(),
            context.xbrl_context_id(),
            Some(context.revision_order().ruleset()),
            match context.restatement_status() {
                market_squawk_domain::FundamentalRestatementStatus::Unavailable => None,
                market_squawk_domain::FundamentalRestatementStatus::SourceReported {
                    source_status,
                    ..
                } => Some(source_status),
            },
        ];
        identifiers
            .into_iter()
            .flatten()
            .fold(0_usize, |bytes, value| {
                bytes.saturating_add(value.retained_bytes())
            })
    }
    pub const fn row_ordinal(&self) -> u32 {
        self.row_ordinal
    }
    pub const fn canonical_row_digest(&self) -> EvidenceDigest {
        self.canonical_row_digest
    }
    pub const fn observation_digest(&self) -> EvidenceDigest {
        self.observation_digest
    }
    pub const fn fact_context(&self) -> &FundamentalFactContext {
        &self.fact_context
    }
}

/// Native fiscal target proof. Only a source-authenticated series can mint this value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FinancialFiscalTargetBinding {
    observed_period: FundamentalPeriod,
    target_period: Option<FundamentalPeriod>,
    observed_ordinal: u32,
    target_ordinal: u32,
    cadence: FundamentalCadence,
    source_selection_digest: EvidenceDigest,
    identity_receipt_digest: EvidenceDigest,
    cadence_rule: String,
    cadence_revision: u32,
    observed_inputs: FinancialSourceInputs<FinancialPeriodRowReference>,
    target_inputs: Option<FinancialSourceInputs<FinancialPeriodRowReference>>,
    duration_chain: Box<[FinancialPeriodRowReference]>,
}
impl FinancialFiscalTargetBinding {
    /// Returns this binding's inline storage and its actual owned string and row allocations.
    pub fn retained_bytes(&self) -> usize {
        let fixed = size_of::<Self>()
            .saturating_add(self.cadence_rule.capacity())
            .saturating_add(
                self.duration_chain
                    .len()
                    .saturating_mul(size_of::<FinancialPeriodRowReference>()),
            );
        self.observed_source_rows()
            .chain(self.target_source_rows())
            .chain(self.duration_chain.iter())
            .fold(fixed, |bytes, row| {
                bytes.saturating_add(row.dynamic_retained_bytes())
            })
    }
    pub const fn observed_period(&self) -> FundamentalPeriod {
        self.observed_period
    }
    pub const fn target_period(&self) -> Option<FundamentalPeriod> {
        self.target_period
    }
    pub const fn observed_ordinal(&self) -> u32 {
        self.observed_ordinal
    }
    pub const fn target_ordinal(&self) -> u32 {
        self.target_ordinal
    }
    pub const fn cadence(&self) -> FundamentalCadence {
        self.cadence
    }
    pub const fn source_selection_digest(&self) -> EvidenceDigest {
        self.source_selection_digest
    }
    pub const fn identity_receipt_digest(&self) -> EvidenceDigest {
        self.identity_receipt_digest
    }
    pub fn duration_chain(&self) -> &[FinancialPeriodRowReference] {
        &self.duration_chain
    }
    pub fn observed_source_rows(&self) -> impl Iterator<Item = &FinancialPeriodRowReference> {
        self.observed_inputs.iter()
    }
    pub fn target_source_rows(&self) -> impl Iterator<Item = &FinancialPeriodRowReference> {
        self.target_inputs
            .iter()
            .flat_map(FinancialSourceInputs::iter)
    }
    pub(super) fn observed_inputs(&self) -> &FinancialSourceInputs<FinancialPeriodRowReference> {
        &self.observed_inputs
    }
    pub(super) fn target_inputs(
        &self,
    ) -> Option<&FinancialSourceInputs<FinancialPeriodRowReference>> {
        self.target_inputs.as_ref()
    }
    pub fn target_horizon(&self) -> Result<DatasetTargetHorizon, DatasetBuildError> {
        let distance = self
            .target_ordinal
            .checked_sub(self.observed_ordinal)
            .and_then(|v| u16::try_from(v).ok())
            .and_then(NonZeroU16::new)
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        Ok(DatasetTargetHorizon::FiscalPeriods {
            cadence: self.cadence,
            periods_ahead: distance,
        })
    }
    pub(crate) fn validate(&self) -> Result<(), DatasetBuildError> {
        self.target_horizon()?.validate()?;
        if self.cadence_rule != CADENCE_RULE
            || self.cadence_revision != 1
            || self.source_selection_digest.bytes() == [0; 32]
            || self.identity_receipt_digest.bytes() == [0; 32]
            || self.observed_period != self.observed_inputs.primary().fact_context.period()
            || self.target_period
                != self
                    .target_inputs
                    .as_ref()
                    .map(|r| r.primary().fact_context.period())
            || self.duration_chain.is_empty()
            || self.duration_chain.len() > 1024
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        for row in self
            .observed_inputs
            .iter()
            .chain(
                self.target_inputs
                    .iter()
                    .flat_map(FinancialSourceInputs::iter),
            )
            .chain(self.duration_chain.iter())
        {
            if row.canonical_row_digest.bytes() == [0; 32]
                || row.observation_digest.bytes() == [0; 32]
                || row.point_in_time_evidence == [0; 32]
                || row
                    .fact_context
                    .dimensions()
                    .dimensions()
                    .is_some_and(|values| !values.is_empty())
            {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
        }
        for inputs in std::iter::once(&self.observed_inputs).chain(self.target_inputs.iter()) {
            if let FinancialSourceInputs::CommonBookEquity {
                parent_equity,
                preferred_equity,
            } = inputs
            {
                if parent_equity.row_ordinal == preferred_equity.row_ordinal
                    || !common_book_context_matches(
                        &parent_equity.fact_context,
                        &preferred_equity.fact_context,
                    )
                {
                    return Err(DatasetBuildError::ComponentEvidenceMismatch);
                }
            }
            if std::mem::discriminant(inputs) != std::mem::discriminant(&self.observed_inputs) {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
        }
        for row in &self.duration_chain {
            if frame_cadence(&row.fact_context)? != Some(self.cadence)
                || row.canonical_row_digest.bytes() == [0; 32]
                || row.observation_digest.bytes() == [0; 32]
                || row.point_in_time_evidence == [0; 32]
            {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
        }
        for pair in self.duration_chain.windows(2) {
            if !contiguous(pair[0].fact_context.period(), pair[1].fact_context.period())
                || pair[0].fact_context.dimensions() != pair[1].fact_context.dimensions()
                || pair[0].fact_context.consolidation() != pair[1].fact_context.consolidation()
            {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
        }
        let first = &self.duration_chain[0];
        if !anchor_matches(
            &self.observed_inputs.primary().fact_context,
            &first.fact_context,
        ) {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        if let Some(target) = &self.target_inputs {
            let distance = usize::try_from(self.target_ordinal - self.observed_ordinal)
                .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
            if self.duration_chain.len() != distance + 1
                || !anchor_matches(
                    &target.primary().fact_context,
                    &self.duration_chain[distance].fact_context,
                )
            {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
        } else if self.duration_chain.len() != 1 {
            // No future observation metadata is required or exposed for an unobserved target.
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        Ok(())
    }
    pub(crate) fn decode(value: serde_json::Value) -> Result<Self, DatasetBuildError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            observed_period: FundamentalPeriod,
            target_period: Option<FundamentalPeriod>,
            observed_ordinal: u32,
            target_ordinal: u32,
            cadence: FundamentalCadence,
            source_selection_digest: EvidenceDigest,
            identity_receipt_digest: EvidenceDigest,
            cadence_rule: String,
            cadence_revision: u32,
            observed_inputs: FinancialSourceInputs<FinancialPeriodRowReference>,
            target_inputs: Option<FinancialSourceInputs<FinancialPeriodRowReference>>,
            duration_chain: Box<[FinancialPeriodRowReference]>,
        }
        let w: Wire = serde_json::from_value(value)
            .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
        let result = Self {
            observed_period: w.observed_period,
            target_period: w.target_period,
            observed_ordinal: w.observed_ordinal,
            target_ordinal: w.target_ordinal,
            cadence: w.cadence,
            source_selection_digest: w.source_selection_digest,
            identity_receipt_digest: w.identity_receipt_digest,
            cadence_rule: w.cadence_rule,
            cadence_revision: w.cadence_revision,
            observed_inputs: w.observed_inputs,
            target_inputs: w.target_inputs,
            duration_chain: w.duration_chain,
        };
        result.validate()?;
        Ok(result)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceRow {
    reference: FinancialPeriodRowReference,
    canonical_json: Box<[u8]>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FinancialSeriesSource {
    pub(super) manifest: DatasetManifestRef,
    pub(super) source_receipt: crate::SecResearchSelectionReceipt,
    pub(super) identity_receipt: Box<[u8]>,
    pub(super) instrument_id: InstrumentId,
    pub(super) selected_as_of: Timestamp,
    pub(super) measurement: FeatureLabelMeasurement,
    selection: FinancialAmountSelection,
    cadence: FundamentalCadence,
    rows: Box<[SourceRow]>,
    // (amount row, authentic duration anchor row), both indexes into the one retained row set.
    periods: Box<[(FinancialSourceInputs<usize>, usize)]>,
    pub(super) retained_bytes: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinancialDatasetSeries {
    pub(super) source: Arc<FinancialSeriesSource>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FinancialExampleSource {
    pub(super) source: Arc<FinancialSeriesSource>,
    pub(super) binding: FinancialFiscalTargetBinding,
}

impl DatasetBuilderService<'_> {
    /// Reduces an existing source-authority selection to the exact bounded fiscal recipe inputs.
    pub fn financial_series(
        &self,
        source: SecResearchIdentitySelection,
        selection: FinancialAmountSelection,
        cadence: FundamentalCadence,
        limits: FinancialSeriesLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<FinancialDatasetSeries, DatasetBuildError> {
        check_control(deadline, cancellation)?;
        selection.mapping()?;
        let SecResearchIdentityOutcome::Exact(selected) = source.outcome() else {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        };
        if source.identity().disposition() != crate::CompanySecurityIdentityDisposition::Complete
            || selected.disposition() != crate::SecResearchDisposition::Selected
            || selected.request().family() != crate::SecResearchFamily::CompanyFacts
            || selected.request().revision_mode() != crate::PointInTimeRevisionMode::LatestKnown
            || !selected.conflicts().is_empty()
            || !matches!(
                cadence,
                FundamentalCadence::Annual | FundamentalCadence::Quarterly
            )
            || self
                .service
                .pinned(selected.origin().manifest())?
                .manifest()
                != selected.origin().manifest()
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let identity_receipt = source
            .identity()
            .receipt()
            .canonical_bytes()
            .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?
            .into_boxed_slice();
        let (concept, _, instant) = selection.mapping()?;
        let mut pairs = Vec::<(FinancialSourceInputs<usize>, usize)>::new();
        let mut currency = None;
        let mut scope: Option<FundamentalObservation> = None;
        let mut facts = 0usize;
        for (index, selected_row) in selected.selected().iter().enumerate() {
            if index % 32 == 0 {
                check_control(deadline, cancellation)?;
            }
            let row = selected_fact(selected, selected_row.row().row_ordinal())?;
            if row.concept().as_str().strip_prefix("us-gaap:") != Some(concept) {
                continue;
            }
            facts = facts
                .checked_add(1)
                .ok_or(DatasetBuildError::LimitExceeded)?;
            if facts > limits.max_facts {
                return Err(DatasetBuildError::LimitExceeded);
            }
            if matches!(
                row.fact_context().period(),
                FundamentalPeriod::Instant { .. }
            ) != instant
            {
                continue;
            }
            let row_currency = source_currency(
                row.unit().as_str(),
                selection.basis == FinancialAmountBasis::PerCommonShare,
            )?;
            if currency.is_some_and(|v| v != row_currency)
                || scope.as_ref().is_some_and(|v| !same_scope(v, &row))
            {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
            currency = Some(row_currency);
            if scope.is_none() {
                scope = Some(row.clone());
            }
            validate_fact(
                &row,
                source.request().instrument_id(),
                selected.request().knowledge_at(),
            )?;
            if transition(row.fact_context()) {
                continue;
            }
            let anchor = if frame_cadence(row.fact_context())? == Some(cadence) {
                Some(index)
            } else if row.fact_context().frame().is_none() || instant {
                let mut anchor: Option<usize> = None;
                for (anchor_index, selected_anchor) in selected.selected().iter().enumerate() {
                    if anchor_index % 32 == 0 {
                        check_control(deadline, cancellation)?;
                    }
                    let candidate = selected_fact(selected, selected_anchor.row().row_ordinal())?;
                    if frame_cadence(candidate.fact_context())? == Some(cadence)
                        && anchor_matches(row.fact_context(), candidate.fact_context())
                        && same_scope(&row, &candidate)
                        && source_currency(candidate.unit().as_str(), false).ok()
                            == Some(row_currency)
                    {
                        // Different concepts in the same envelope may attest the identical interval;
                        // select the first canonical source row only when all anchor context agrees.
                        if let Some(previous) = anchor {
                            let previous = selected_fact(
                                selected,
                                selected.selected()[previous].row().row_ordinal(),
                            )?;
                            if previous.fact_context().period() != candidate.fact_context().period()
                            {
                                return Err(DatasetBuildError::ComponentEvidenceMismatch);
                            }
                        } else {
                            anchor = Some(anchor_index);
                        }
                    }
                }
                anchor
            } else {
                None
            };
            let Some(anchor) = anchor else {
                continue;
            };
            let inputs = if selection.role == FinancialAmountRole::CommonBookEquity {
                let mut preferred = None;
                for (preferred_index, selected_preferred) in selected.selected().iter().enumerate()
                {
                    if preferred_index % 32 == 0 {
                        check_control(deadline, cancellation)?;
                    }
                    let fact = selected_fact(selected, selected_preferred.row().row_ordinal())?;
                    if fact.concept().as_str()
                        == "us-gaap:PreferredStockIncludingAdditionalPaidInCapitalNetOfDiscount"
                        && same_scope(&row, &fact)
                        && common_book_context_matches(row.fact_context(), fact.fact_context())
                        && row.unit() == fact.unit()
                    {
                        validate_fact(
                            &fact,
                            source.request().instrument_id(),
                            selected.request().knowledge_at(),
                        )?;
                        if preferred.replace(preferred_index).is_some() {
                            return Err(DatasetBuildError::ComponentEvidenceMismatch);
                        }
                    }
                }
                let Some(preferred_equity) = preferred else {
                    continue;
                };
                FinancialSourceInputs::CommonBookEquity {
                    parent_equity: index,
                    preferred_equity,
                }
            } else {
                FinancialSourceInputs::Reported { amount: index }
            };
            if pairs.len() >= limits.max_periods {
                return Err(DatasetBuildError::LimitExceeded);
            }
            pairs
                .try_reserve(1)
                .map_err(|_| DatasetBuildError::LimitExceeded)?;
            pairs.push((inputs, anchor));
        }
        if pairs.is_empty() {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let mut sort_error = None;
        pairs.sort_unstable_by_key(|(amount, _)| {
            match selected_fact(
                selected,
                selected.selected()[*amount.primary()].row().row_ordinal(),
            ) {
                Ok(value) => Some(value.fact_context().period().end()),
                Err(error) => {
                    sort_error = Some(error);
                    None
                }
            }
        });
        if let Some(error) = sort_error {
            return Err(error);
        }
        for pair in pairs.windows(2) {
            let left = selected_fact(selected, selected.selected()[pair[0].1].row().row_ordinal())?;
            let right =
                selected_fact(selected, selected.selected()[pair[1].1].row().row_ordinal())?;
            if !contiguous(left.fact_context().period(), right.fact_context().period()) {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
        }
        let mut rows = Vec::<SourceRow>::new();
        let mut retained = size_of::<FinancialSeriesSource>()
            + identity_receipt.len()
            + selected.origin().manifest().dataset_id().as_str().len()
            + selected.origin().manifest().schema().name().len();
        for pair in &mut pairs {
            for index in pair.0.iter_mut().chain(std::iter::once(&mut pair.1)) {
                let source_row = &selected.selected()[*index];
                let ordinal = source_row.row().row_ordinal();
                if let Some(existing) = rows.iter().position(|r| r.reference.row_ordinal == ordinal)
                {
                    *index = existing;
                    continue;
                }
                let observation = selected_fact(selected, ordinal)?;
                let bytes = serde_json::to_vec(&observation)
                    .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
                retained = retained
                    .checked_add(size_of::<SourceRow>())
                    .and_then(|v| v.checked_add(bytes.len() * 2))
                    .ok_or(DatasetBuildError::LimitExceeded)?;
                if retained > limits.max_bytes {
                    return Err(DatasetBuildError::LimitExceeded);
                }
                rows.try_reserve_exact(1)
                    .map_err(|_| DatasetBuildError::LimitExceeded)?;
                rows.push(SourceRow {
                    reference: FinancialPeriodRowReference {
                        row_ordinal: ordinal,
                        canonical_row_digest: source_row.row().canonical_row_digest(),
                        observation_digest: source_row.row().observation_digest(),
                        point_in_time_evidence: source_row
                            .point_in_time()
                            .evidence_identity()
                            .bytes(),
                        fact_context: observation.fact_context().clone(),
                    },
                    canonical_json: bytes.into_boxed_slice(),
                });
                *index = rows.len() - 1;
            }
        }
        retained = retained
            .checked_add(pairs.len() * size_of::<(FinancialSourceInputs<usize>, usize)>())
            .ok_or(DatasetBuildError::LimitExceeded)?;
        if retained > limits.max_bytes {
            return Err(DatasetBuildError::LimitExceeded);
        }
        Ok(FinancialDatasetSeries {
            source: Arc::new(FinancialSeriesSource {
                manifest: selected.origin().manifest().clone(),
                source_receipt: selected.receipt(),
                identity_receipt,
                instrument_id: source.request().instrument_id(),
                selected_as_of: selected.request().knowledge_at(),
                measurement: selection
                    .measurement(currency.ok_or(DatasetBuildError::ComponentEvidenceMismatch)?),
                selection,
                cadence,
                rows: rows.into_boxed_slice(),
                periods: pairs.into_boxed_slice(),
                retained_bytes: retained,
            }),
        })
    }
}

impl FinancialDatasetSeries {
    pub fn len(&self) -> usize {
        self.source.periods.len()
    }
    pub fn is_empty(&self) -> bool {
        self.source.periods.is_empty()
    }
    pub fn measurement(&self) -> FeatureLabelMeasurement {
        self.source.measurement
    }
    pub fn source_manifest(&self) -> &DatasetManifestRef {
        &self.source.manifest
    }
    pub fn retained_bytes(&self) -> usize {
        self.source.retained_bytes
    }
    pub fn observed_period(&self, ordinal: u32) -> Option<FundamentalPeriod> {
        let (row, _) = self.source.periods.get(usize::try_from(ordinal).ok()?)?;
        Some(
            self.source.rows[*row.primary()]
                .reference
                .fact_context
                .period(),
        )
    }
    pub fn try_example(
        &self,
        example_id: &str,
        observed_ordinal: u32,
        policy: &DatasetStudyPolicy,
        source_selection_as_of: Timestamp,
        label_selection_as_of: Option<Timestamp>,
        decision_coordinate: ResearchTemporalCoordinate,
    ) -> Result<DatasetExample, DatasetBuildError> {
        let DatasetTargetHorizon::FiscalPeriods {
            cadence,
            periods_ahead,
        } = policy.target_horizon()
        else {
            return Err(DatasetBuildError::InvalidRequest);
        };
        if cadence != self.source.cadence
            || source_selection_as_of > self.source.selected_as_of
            || policy.snapshot_as_of() < self.source.selected_as_of
            || (policy.basis() == HistoricalStudyBasis::HistoricalAsKnown
                && source_selection_as_of != self.source.selected_as_of)
            || (policy.basis() == HistoricalStudyBasis::RetrospectiveFrozenSnapshot
                && policy.snapshot_as_of() != self.source.selected_as_of)
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let target_ordinal = observed_ordinal
            .checked_add(u32::from(periods_ahead.get()))
            .ok_or(DatasetBuildError::InvalidRequest)?;
        let observed_index =
            usize::try_from(observed_ordinal).map_err(|_| DatasetBuildError::InvalidRequest)?;
        let target_index =
            usize::try_from(target_ordinal).map_err(|_| DatasetBuildError::InvalidRequest)?;
        let (origin, anchor) = self
            .source
            .periods
            .get(observed_index)
            .ok_or(DatasetBuildError::InvalidRequest)?;
        let target = self.source.periods.get(target_index).map(|(row, _)| row);
        let observed = &self.source.rows[*origin.primary()];
        let terminal = target.map(|rows| &self.source.rows[*rows.primary()]);
        let identity = crate::CompanySecurityIdentitySelectionReceipt::from_canonical_bytes(
            &self.source.identity_receipt,
        )
        .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
        if policy.basis() == HistoricalStudyBasis::HistoricalAsKnown
            && identity.knowledge_at() > source_selection_as_of
        {
            return Err(DatasetBuildError::TemporalLeakage);
        }
        let mut chain = Vec::new();
        let chain_end = if terminal.is_some() {
            target_index
        } else {
            observed_index
        };
        chain
            .try_reserve_exact(chain_end - observed_index + 1)
            .map_err(|_| DatasetBuildError::LimitExceeded)?;
        for (_, anchor) in &self.source.periods[observed_index..=chain_end] {
            chain.push(self.source.rows[*anchor].reference.clone());
        }
        let binding = FinancialFiscalTargetBinding {
            observed_period: observed.reference.fact_context.period(),
            target_period: terminal.map(|row| row.reference.fact_context.period()),
            observed_ordinal,
            target_ordinal,
            cadence,
            source_selection_digest: self.source.source_receipt.result_digest(),
            identity_receipt_digest: identity.receipt_digest(),
            cadence_rule: CADENCE_RULE.into(),
            cadence_revision: 1,
            observed_inputs: origin.try_map(|index| {
                Ok::<_, DatasetBuildError>(self.source.rows[*index].reference.clone())
            })?,
            target_inputs: target
                .map(|inputs| {
                    inputs.try_map(|index| {
                        Ok::<_, DatasetBuildError>(self.source.rows[*index].reference.clone())
                    })
                })
                .transpose()?,
            duration_chain: chain.into_boxed_slice(),
        };
        binding.validate()?;
        let feature_end = ResearchTemporalCoordinate::calendar_date(binding.observed_period.end());
        let target_end = binding
            .target_period
            .map(|p| ResearchTemporalCoordinate::calendar_date(p.end()));
        let amount = origin.try_map(|index| {
            self.source
                .observation(self.source.rows[*index].reference.row_ordinal)
        })?;
        for fact in amount.iter() {
            validate_fact(fact, self.source.instrument_id, source_selection_as_of)?;
        }
        let anchor_fact: FundamentalObservation =
            serde_json::from_slice(&self.source.rows[*anchor].canonical_json)
                .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
        validate_fact(
            &anchor_fact,
            self.source.instrument_id,
            source_selection_as_of,
        )?;
        let mut components = vec![self.source.component(
            &amount,
            ComponentKind::Feature,
            feature_end.clone(),
            None,
        )?];
        if policy.purpose() == super::DatasetBuildPurpose::Training {
            let target = target.ok_or(DatasetBuildError::TemporalLeakage)?;
            let fact = target.try_map(|index| {
                self.source
                    .observation(self.source.rows[*index].reference.row_ordinal)
            })?;
            for fact in fact.iter() {
                validate_fact(
                    fact,
                    self.source.instrument_id,
                    label_selection_as_of.ok_or(DatasetBuildError::TemporalLeakage)?,
                )?;
            }
            components.push(self.source.component(
                &fact,
                ComponentKind::Label,
                feature_end.clone(),
                target_end.clone(),
            )?);
        }
        let example = DatasetExample::from_financial(
            example_id,
            self.source.instrument_id,
            source_selection_as_of,
            label_selection_as_of,
            decision_coordinate,
            feature_end,
            target_end,
            components,
            FinancialExampleSource {
                source: Arc::clone(&self.source),
                binding,
            },
        )?;
        policy.validate_example(&example)?;
        Ok(example)
    }
}
impl FinancialSeriesSource {
    fn component(
        &self,
        fact: &FinancialSourceInputs<FundamentalObservation>,
        kind: ComponentKind,
        effective: ResearchTemporalCoordinate,
        target: Option<ResearchTemporalCoordinate>,
    ) -> Result<FeatureLabelComponentInput, DatasetBuildError> {
        let (_, unit, _) = self.selection.mapping()?;
        let FeatureLabelMeasurement::FinancialAmount { currency, .. } = self.measurement else {
            return Err(DatasetBuildError::InvalidRequest);
        };
        let mut selectors = Vec::new();
        selectors
            .try_reserve_exact(fact.iter().count())
            .map_err(|_| DatasetBuildError::LimitExceeded)?;
        for fact in fact.iter() {
            let candidate = PointInTimeCandidate::new(
                ResearchObservation::Fundamental(fact.clone()),
                self.manifest.clone(),
            );
            selectors.push(ComponentSelector::new(
                candidate
                    .family_key()
                    .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?,
            ));
        }
        FeatureLabelComponentInput::try_new(
            component_spec(kind)?,
            ComponentValue::decimal(
                fact.amount(self.selection)?,
                Some(
                    SourceIdentifier::try_from(unit)
                        .map_err(|_| DatasetBuildError::InvalidRequest)?,
                ),
                Some(currency),
            )?,
            selectors,
            effective,
            target,
            ComponentAdjustmentEvidence::NotApplicable,
        )
    }
    pub(super) fn revalidate(
        &self,
        candidates: &crate::pit::disk::CandidateStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), DatasetBuildError> {
        for row in &self.rows {
            check_control(deadline, cancellation)?;
            let fact: FundamentalObservation = serde_json::from_slice(&row.canonical_json)
                .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
            let count = candidates
                .count_observation(&ResearchObservation::Fundamental(fact), &self.manifest)
                .map_err(DatasetBuildError::IndexedPointInTime)?;
            if count != 1 {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
        }
        Ok(())
    }
    pub(super) fn observation(
        &self,
        ordinal: u32,
    ) -> Result<FundamentalObservation, DatasetBuildError> {
        let row = self
            .rows
            .iter()
            .find(|r| r.reference.row_ordinal == ordinal)
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        serde_json::from_slice(&row.canonical_json)
            .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)
    }
}
impl FinancialExampleSource {
    pub(super) fn validate_component(
        &self,
        component: &FeatureLabelComponentInput,
        selection: &crate::pit::disk::Selection,
    ) -> Result<(), DatasetBuildError> {
        let references = match component.spec().kind() {
            ComponentKind::Feature => self.binding.observed_inputs(),
            ComponentKind::Label => self
                .binding
                .target_inputs()
                .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?,
        };
        let expected = references.try_map(|row| self.source.observation(row.row_ordinal()))?;
        for fact in expected.iter() {
            let observed=selection.records().iter().filter(|r|r.candidate().source_manifest()==&self.source.manifest
                && matches!(r.candidate().observation(),ResearchObservation::Fundamental(value) if value==fact)).count();
            if observed != 1 {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
        }
        if component.value()
            != self
                .source
                .component(
                    &expected,
                    component.spec().kind(),
                    component.selection_effective_cutoff().clone(),
                    component.label_selection_effective_cutoff().cloned(),
                )?
                .value()
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        Ok(())
    }
}
pub(super) fn component_spec(
    kind: ComponentKind,
) -> Result<FeatureLabelComponentSpec, DatasetBuildError> {
    FeatureLabelComponentSpec::try_new(
        kind,
        ComponentScope::Instrument,
        CorporateActionSensitivity::NotApplicable,
        if kind == ComponentKind::Feature {
            FINANCIAL_FEATURE
        } else {
            FINANCIAL_LABEL
        },
        NonZeroU32::MIN,
    )
}
fn selected_fact(
    source: &SecResearchSelection,
    ordinal: u32,
) -> Result<FundamentalObservation, DatasetBuildError> {
    match source
        .decoded_rows()
        .get(ordinal as usize)
        .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?
    {
        Some(ResearchObservation::Fundamental(value)) => Ok(value),
        _ => Err(DatasetBuildError::ComponentEvidenceMismatch),
    }
}
pub(crate) fn source_currency(unit: &str, per_share: bool) -> Result<Currency, DatasetBuildError> {
    let unit = unit.strip_prefix("iso4217:").unwrap_or(unit);
    let currency = if per_share {
        unit.strip_suffix("/shares")
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?
    } else {
        unit
    };
    Currency::try_from(currency).map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)
}
pub(super) fn same_scope(left: &FundamentalObservation, right: &FundamentalObservation) -> bool {
    left.context().provenance().instrument_id() == right.context().provenance().instrument_id()
        && left.context().provenance().source_id() == right.context().provenance().source_id()
        && left.fact_context().dimensions() == right.fact_context().dimensions()
        && left.fact_context().consolidation() == right.fact_context().consolidation()
}
pub(super) fn anchor_matches(
    amount: &FundamentalFactContext,
    anchor: &FundamentalFactContext,
) -> bool {
    let same_period = match amount.period() {
        FundamentalPeriod::Duration { .. } => amount.period() == anchor.period(),
        FundamentalPeriod::Instant { instant } => instant == anchor.period().end(),
    };
    same_period
        && amount.accession() == anchor.accession()
        && amount.filing_form() == anchor.filing_form()
        && amount.filed_on() == anchor.filed_on()
        && amount.dimensions() == anchor.dimensions()
        && amount.consolidation() == anchor.consolidation()
}
fn contiguous(left: FundamentalPeriod, right: FundamentalPeriod) -> bool {
    right.start().is_some_and(|start| {
        left.end().days_since_unix_epoch().checked_add(1) == Some(start.days_since_unix_epoch())
    })
}
pub(super) fn frame_cadence(
    context: &FundamentalFactContext,
) -> Result<Option<FundamentalCadence>, DatasetBuildError> {
    if transition(context) {
        return Ok(None);
    }
    let Some(frame) = context.frame() else {
        return Ok(None);
    };
    let bytes = frame.as_str().as_bytes();
    if bytes.len() < 6
        || &bytes[..2] != b"CY"
        || !bytes[2..6].iter().all(u8::is_ascii_digit)
        || &bytes[2..6] == b"0000"
    {
        return Ok(None);
    }
    let cadence = if bytes.len() == 6 {
        FundamentalCadence::Annual
    } else if bytes.len() == 8 && bytes[6] == b'Q' && (b'1'..=b'4').contains(&bytes[7]) {
        FundamentalCadence::Quarterly
    } else {
        return Ok(None);
    };
    let FundamentalPeriod::Duration { start, end } = context.period() else {
        return Ok(None);
    };
    let days = end.days_since_unix_epoch() - start.days_since_unix_epoch() + 1;
    if match cadence {
        FundamentalCadence::Annual => !(335..=395).contains(&days),
        _ => !(61..=121).contains(&days),
    } {
        return Err(DatasetBuildError::ComponentEvidenceMismatch);
    }
    Ok(Some(cadence))
}
pub(crate) fn validate_fact(
    fact: &FundamentalObservation,
    instrument: InstrumentId,
    known: Timestamp,
) -> Result<(), DatasetBuildError> {
    let p = fact.context().provenance();
    if p.instrument_id() != Some(instrument)
        || p.availability()
            .conservative_available_at()
            .is_none_or(|t| t > known)
        || fact.fact_context().period().end()
            > known
                .utc_calendar_date()
                .map_err(|_| DatasetBuildError::TemporalLeakage)?
        || fact
            .fact_context()
            .dimensions()
            .dimensions()
            .is_some_and(|d| !d.is_empty())
        || matches!(
            p.quality(),
            DataQuality::Modeled | DataQuality::Stale | DataQuality::Quarantined
        )
    {
        return Err(DatasetBuildError::ComponentEvidenceMismatch);
    }
    Ok(())
}
fn check_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), DatasetBuildError> {
    if cancellation.is_cancelled() {
        Err(DatasetBuildError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(DatasetBuildError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn transition(context: &FundamentalFactContext) -> bool {
    context
        .filing_form()
        .is_some_and(|f| matches!(f.as_str(), "10-KT" | "10-KT/A" | "10-QT" | "10-QT/A"))
}

fn common_book_context_matches(
    left: &FundamentalFactContext,
    right: &FundamentalFactContext,
) -> bool {
    matches!(left.period(), FundamentalPeriod::Instant { .. })
        && left.period() == right.period()
        && left.unit() == right.unit()
        && left.accession() == right.accession()
        && left.filing_form().is_some()
        && left.filing_form() == right.filing_form()
        && left.filed_on().is_some()
        && left.filed_on() == right.filed_on()
        && left.amendment_status() == right.amendment_status()
        && left.restatement_status() == right.restatement_status()
        && left.xbrl_context_id() == right.xbrl_context_id()
        && left.dimensions() == right.dimensions()
        && left.consolidation() == right.consolidation()
        && left.fiscal_year() == right.fiscal_year()
        && left.fiscal_period() == right.fiscal_period()
        && left.cadence() == right.cadence()
}
