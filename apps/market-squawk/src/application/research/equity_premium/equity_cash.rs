//! Source-scoped cash-distribution annual equity holding returns.

use super::super::{
    RecommendationBenchmarkSelection, RecommendationBenchmarkSelectionReference,
    TiingoCompletedEodActionRead, TiingoCompletedEodHistoryReference,
};
use super::EquityPremiumUnavailable;
use market_squawk_adapter_tiingo::TiingoEodActionFieldDisposition;
use market_squawk_analytics::cash_distribution_holding_return;
use market_squawk_data::{DatasetBuildPurpose, FeatureDatasetInputEpoch};
use market_squawk_domain::{
    CalendarDate, Currency, DigestAlgorithm, EvidenceDigest, HistoricalStudyBasis,
    MarketBarAdjustment, Money, Timestamp,
};
use market_squawk_valuation::EQUITY_PREMIUM_SAMPLE_YEARS;
use rust_decimal::{Decimal, RoundingStrategy};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

const ENDPOINTS: usize = EQUITY_PREMIUM_SAMPLE_YEARS + 1;

/// Exact inert source/identity recipe; source owners must freshly reopen both references.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AnnualEquityCashReturnReference {
    history: TiingoCompletedEodHistoryReference,
    benchmark: RecommendationBenchmarkSelectionReference,
    evidence_digest: EvidenceDigest,
}
impl AnnualEquityCashReturnReference {
    pub(crate) const fn history(&self) -> &TiingoCompletedEodHistoryReference {
        &self.history
    }
    pub(crate) const fn benchmark(&self) -> &RecommendationBenchmarkSelectionReference {
        &self.benchmark
    }
    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Sealed provider-neutral result. It cannot be constructed from price vectors, a raw action
/// projection, caller cash units, or a digest. The authenticated source read stays owned here.
#[derive(Debug)]
pub(crate) struct AnnualEquityCashReturnRead {
    source: TiingoCompletedEodActionRead,
    benchmark: RecommendationBenchmarkSelection,
    reference: AnnualEquityCashReturnReference,
    economic_origin: Timestamp,
    closing_dates: [CalendarDate; ENDPOINTS],
    closing_prices: [Money; ENDPOINTS],
    cash_entitlements: [Money; EQUITY_PREMIUM_SAMPLE_YEARS],
    annual_returns: [Decimal; EQUITY_PREMIUM_SAMPLE_YEARS],
}

impl AnnualEquityCashReturnRead {
    /// Only complete source-owned native/canonical evidence can enter this narrow convention.
    /// Cash entitlements are valued at face and retained within each year. This is not a
    /// reinvested index, evidence of payment, all-action completeness, or an executable portfolio.
    pub(crate) fn from_source(
        source: TiingoCompletedEodActionRead,
        benchmark: RecommendationBenchmarkSelection,
    ) -> Result<Self, EquityPremiumUnavailable> {
        Self::from_source_at_origin(source, benchmark, None)
    }

    /// Historical sampling keeps the actual acquisition cutoff while selecting only complete
    /// economic years preceding the actual original study epoch. None selects the current
    /// source cutoff; no caller-authored economic timestamp is accepted.
    pub(super) fn from_source_at_origin(
        source: TiingoCompletedEodActionRead,
        benchmark: RecommendationBenchmarkSelection,
        epoch: Option<&FeatureDatasetInputEpoch>,
    ) -> Result<Self, EquityPremiumUnavailable> {
        let mismatch = EquityPremiumUnavailable::SourceIdentityMismatch;
        let economic_origin = match epoch {
            None => source.knowledge_cutoff(),
            Some(epoch) => {
                let origin = epoch.target_origin().ok_or(mismatch)?;
                let decision = epoch.decision_at().ok_or(mismatch)?;
                if epoch.purpose() != DatasetBuildPurpose::StudyInputs
                    || epoch.market_bar().is_none()
                    || epoch.source_selection_as_of() != source.knowledge_cutoff()
                    || benchmark.effective_at() != origin
                    || origin > decision
                    || origin > source.knowledge_cutoff()
                    || (epoch.basis() == HistoricalStudyBasis::HistoricalAsKnown
                        && source.knowledge_cutoff() > decision)
                    || (epoch.basis() == HistoricalStudyBasis::RetrospectiveFrozenSnapshot
                        && source.knowledge_cutoff() != epoch.snapshot_as_of())
                {
                    return Err(mismatch);
                }
                origin
            }
        };
        let currency = Currency::try_from("USD").map_err(|_| mismatch)?;
        let knowledge_cutoff = source.knowledge_cutoff();
        if benchmark.knowledge_at() != knowledge_cutoff
            || source.history().selection().receipt().instrument_id()
                != benchmark.primary().instrument_id()
            || source.history().selection().receipt().currency() != currency
            || source.history().selection().receipt().adjustment() != MarketBarAdjustment::Raw
        {
            return Err(mismatch);
        }
        let year = super::calendar_date(economic_origin)?.year();
        let opening_year = year
            .checked_sub(ENDPOINTS as u16)
            .ok_or(EquityPremiumUnavailable::IncompleteTenYearHistory)?;
        let requested_start = CalendarDate::new(opening_year, 12, 24).map_err(|_| mismatch)?;
        let requested_end = CalendarDate::new(year - 1, 12, 31).map_err(|_| mismatch)?;
        let graph = source
            .history()
            .selection()
            .receipt()
            .date_windows()
            .ok_or(EquityPremiumUnavailable::NativeDateAuthorityMissing)?;
        let native = source
            .history()
            .native_sessions()
            .ok_or(EquityPremiumUnavailable::NativeDateAuthorityMissing)?;
        if native.calendar_origin_content_digest() != graph.calendar().origin_content_digest
            || native.calendar_capture_binding_digest() != graph.calendar().capture_binding_digest
            || native.sessions().iter().any(|session| {
                !session.bar_present()
                    || session.provider_timestamp().is_some()
                    || session.provider_period().is_some()
            })
        {
            return Err(EquityPremiumUnavailable::NativeDateAuthorityMissing);
        }
        let (start, end) = graph.requested_dates();
        if start > requested_start || end < requested_end {
            return Err(EquityPremiumUnavailable::IncompleteTenYearHistory);
        }
        let mut prices = BTreeMap::new();
        for bar in source.history().bars() {
            let date = bar
                .time_semantics()
                .nominal_daily_date()
                .map(|nominal| nominal.date())
                .ok_or(EquityPremiumUnavailable::NativeDateAuthorityMissing)?;
            if bar.context().time().effective().calendar_date_value() != Some(date)
                || bar.currency() != currency
                || bar.adjustment() != MarketBarAdjustment::Raw
                || prices.insert(date, bar.close()).is_some()
            {
                return Err(mismatch);
            }
        }
        let rows = source.actions().rows();
        if rows.is_empty() || rows.windows(2).any(|pair| pair[0].date >= pair[1].date) {
            return Err(EquityPremiumUnavailable::IncompleteTenYearHistory);
        }
        // The complete expected-session graph, not weekday arithmetic, establishes the final
        // native session in each year. A missing final price never borrows an earlier close.
        let mut closing_dates = Vec::with_capacity(ENDPOINTS);
        let mut closing_prices = Vec::with_capacity(ENDPOINTS);
        for offset in 0..ENDPOINTS {
            let target_year = opening_year + offset as u16;
            let closing_date = native
                .sessions()
                .iter()
                .rev()
                .map(|session| session.native_date())
                .find(|date| date.year() == target_year)
                .filter(|date| date.month() == 12 && date.day() >= 24)
                .ok_or(EquityPremiumUnavailable::IncompleteTenYearHistory)?;
            let row = rows
                .binary_search_by_key(&closing_date, |row| row.date)
                .ok()
                .and_then(|index| rows.get(index))
                .ok_or(EquityPremiumUnavailable::MissingAnnualClosingPrice)?;
            let close = prices
                .get(&row.date)
                .copied()
                .filter(|price| price.amount() > Decimal::ZERO)
                .ok_or(EquityPremiumUnavailable::MissingAnnualClosingPrice)?;
            closing_dates.push(row.date);
            closing_prices.push(close);
        }
        let mut cash = [Money::new(Decimal::ZERO, currency); EQUITY_PREMIUM_SAMPLE_YEARS];
        let first = closing_dates[0];
        let last = closing_dates[ENDPOINTS - 1];
        let mut digest = Sha256::new();
        digest
            .update(b"market-squawk/source-ordinary-cash-equity-ten-complete-years/no-splits/v1\0");
        digest.update(
            source
                .history()
                .read_receipt()
                .source_result_digest()
                .bytes(),
        );
        digest.update(source.binding().binding_digest().bytes());
        digest.update(native.mapping_digest().bytes());
        digest.update(benchmark.selection_digest().bytes());
        digest.update(knowledge_cutoff.unix_nanos().to_be_bytes());
        if economic_origin != knowledge_cutoff {
            digest.update(b"original-economic-sample-origin\0");
            digest.update(economic_origin.unix_nanos().to_be_bytes());
        }
        for row in rows
            .iter()
            .filter(|row| row.date > first && row.date <= last)
        {
            if !prices.contains_key(&row.date) {
                return Err(EquityPremiumUnavailable::IncompleteTenYearHistory);
            }
            if row.split_factor != Some(Decimal::ONE)
                || row.shares != TiingoEodActionFieldDisposition::ExplicitNoEvent
            {
                return Err(match (row.split_factor, row.cash_dividend) {
                    (Some(split), Some(dividend))
                        if split != Decimal::ONE && dividend > Decimal::ZERO =>
                    {
                        EquityPremiumUnavailable::DividendShareBasisUnproven
                    }
                    (None, _) => EquityPremiumUnavailable::MissingActionField,
                    _ => EquityPremiumUnavailable::SplitAccountingRequired,
                });
            }
            let value = row
                .cash_dividend
                .ok_or(EquityPremiumUnavailable::MissingActionField)?;
            if value < Decimal::ZERO {
                return Err(mismatch);
            }
            match (value.is_zero(), row.cash) {
                (true, TiingoEodActionFieldDisposition::ExplicitNoEvent) => {}
                (false, TiingoEodActionFieldDisposition::MissingMonetaryUnit) => {
                    return Err(EquityPremiumUnavailable::DividendCurrencyUnproven);
                }
                (false, TiingoEodActionFieldDisposition::Normalized { .. }) => {
                    let unit = source
                        .actions()
                        .cash_unit()
                        .ok_or(EquityPremiumUnavailable::DividendCurrencyUnproven)?;
                    if unit.currency() != currency
                        || unit.instrument() != benchmark.primary().instrument_id()
                        || unit.available_at() > knowledge_cutoff
                    {
                        return Err(EquityPremiumUnavailable::DividendCurrencyUnproven);
                    }
                }
                _ => return Err(EquityPremiumUnavailable::MissingActionField),
            }
            let index = closing_dates
                .windows(2)
                .position(|pair| row.date > pair[0] && row.date <= pair[1])
                .ok_or(mismatch)?;
            cash[index] = cash[index]
                .checked_add(Money::new(value, currency))
                .map_err(|_| EquityPremiumUnavailable::Arithmetic)?;
            digest.update(row.row_digest.bytes());
            digest.update(row.date.days_since_unix_epoch().to_be_bytes());
            digest.update(value.normalize().mantissa().to_be_bytes());
            digest.update(value.normalize().scale().to_be_bytes());
        }
        let mut annual_returns = Vec::with_capacity(EQUITY_PREMIUM_SAMPLE_YEARS);
        for index in 0..EQUITY_PREMIUM_SAMPLE_YEARS {
            let value = cash_distribution_holding_return(
                closing_prices[index],
                closing_prices[index + 1],
                cash[index],
            )
            .map_err(|_| EquityPremiumUnavailable::Arithmetic)?
            .value();
            let value = Decimal::from_f64_retain(value)
                .map(|value| {
                    value
                        .round_dp_with_strategy(12, RoundingStrategy::MidpointNearestEven)
                        .normalize()
                })
                .ok_or(EquityPremiumUnavailable::Arithmetic)?;
            annual_returns.push(value);
        }
        for (date, price) in closing_dates.iter().zip(&closing_prices) {
            digest.update(date.days_since_unix_epoch().to_be_bytes());
            digest.update(price.amount().normalize().mantissa().to_be_bytes());
            digest.update(price.amount().normalize().scale().to_be_bytes());
        }
        let evidence_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into());
        if evidence_digest.bytes() == [0; 32] {
            return Err(mismatch);
        }
        Ok(Self {
            reference: AnnualEquityCashReturnReference {
                history: source.reference().clone(),
                benchmark: benchmark.reference().clone(),
                evidence_digest,
            },
            source,
            benchmark,
            economic_origin,
            closing_dates: closing_dates.try_into().map_err(|_| mismatch)?,
            closing_prices: closing_prices.try_into().map_err(|_| mismatch)?,
            cash_entitlements: cash,
            annual_returns: annual_returns.try_into().map_err(|_| mismatch)?,
        })
    }
    pub(crate) const fn reference(&self) -> &AnnualEquityCashReturnReference {
        &self.reference
    }
    pub(crate) const fn source(&self) -> &TiingoCompletedEodActionRead {
        &self.source
    }
    pub(crate) const fn benchmark(&self) -> &RecommendationBenchmarkSelection {
        &self.benchmark
    }
    /// Actual source cutoff for current use, or the authenticated original study epoch origin.
    pub(crate) const fn economic_origin(&self) -> Timestamp {
        self.economic_origin
    }
    pub(crate) const fn closing_dates(&self) -> &[CalendarDate; ENDPOINTS] {
        &self.closing_dates
    }
    pub(crate) const fn closing_prices(&self) -> &[Money; ENDPOINTS] {
        &self.closing_prices
    }
    pub(crate) const fn cash_entitlements(&self) -> &[Money; EQUITY_PREMIUM_SAMPLE_YEARS] {
        &self.cash_entitlements
    }
    pub(crate) const fn annual_returns(&self) -> &[Decimal; EQUITY_PREMIUM_SAMPLE_YEARS] {
        &self.annual_returns
    }
}
