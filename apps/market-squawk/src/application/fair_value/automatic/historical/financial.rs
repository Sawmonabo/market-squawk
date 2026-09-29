//! Original native fiscal inference, source rates, and shared checked annual valuation kernels.
use super::methods::{HistoricalMethodReceipt, HistoricalValueBasis, issue_method_receipt};
use super::*;
use crate::application::research::{
    HistoricalOriginEquityPremiumRead, HistoricalOriginFinancialForecast, MacroInvestmentContext,
};
use market_squawk_data::{FeatureLabelMeasurement, FinancialAmountBasis, FinancialAmountRole};
use market_squawk_valuation::{AnnualEquityArithmetic, AutomaticValuationMethod};
use std::num::NonZeroU32;

struct NativeHistoricalInputs<'a> {
    method: AutomaticValuationMethod,
    sources: Vec<&'a HistoricalOriginFinancialForecast>,
    values: Vec<Decimal>,
}

impl<'a> NativeHistoricalInputs<'a> {
    fn select(
        epoch: &FeatureDatasetInputEpoch,
        sources: &'a [HistoricalOriginFinancialForecast],
        method: AutomaticValuationMethod,
    ) -> Result<Self, ServiceError> {
        use crate::application::research::fiscal_projection::FISCAL_PROJECTION_EXPLICIT_PERIODS;
        let n = usize::from(FISCAL_PROJECTION_EXPLICIT_PERIODS);
        if n != 3 || sources.len() > 9 {
            return Err(ServiceError::InvalidRequest);
        }
        let origin_identity = Sha256Digest::new(
            Sha256::digest(
                epoch
                    .canonical_bytes()
                    .map_err(|_| ServiceError::InvalidResult)?,
            )
            .into(),
        );
        let currency = epoch
            .current_unit_price()
            .map_err(|_| ServiceError::Unavailable)?
            .currency();
        let mut result = Self {
            method,
            sources: Vec::new(),
            values: Vec::new(),
        };
        result
            .sources
            .try_reserve_exact(6)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        result
            .values
            .try_reserve_exact(6)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        let mut select = |role, offset: u32, current: bool| -> Result<(), ServiceError> {
            let mut matches = sources.iter().filter(|source| {
                let native = source.forecast().epoch();
                source.origin_identity() == origin_identity
                    && native.instrument_id() == epoch.instrument_id()
                    && native.basis() == epoch.basis()
                    && native.source_selection_as_of() == epoch.source_selection_as_of()
                    && native.snapshot_as_of() == epoch.snapshot_as_of()
                    && native.financial_measurement()
                        == Some(FeatureLabelMeasurement::FinancialAmount {
                            currency,
                            role,
                            basis: FinancialAmountBasis::TotalCommonEquity,
                            share_convention: None,
                        })
                    && native.financial_period().is_some_and(|period| {
                        period.cadence() == FundamentalCadence::Annual
                            && period
                                .target_ordinal()
                                .checked_sub(period.observed_ordinal())
                                == Some(offset)
                    })
            });
            let source = matches.next().ok_or(ServiceError::Unavailable)?;
            if matches.next().is_some() {
                return Err(ServiceError::InvalidResult);
            }
            let binding = source
                .forecast()
                .epoch()
                .financial_period()
                .ok_or(ServiceError::InvalidResult)?;
            if result.sources.first().is_some_and(|first| {
                first
                    .forecast()
                    .epoch()
                    .financial_period()
                    .is_none_or(|first| {
                        first.observed_period().end() != binding.observed_period().end()
                            || first.identity_receipt_digest() != binding.identity_receipt_digest()
                    })
            }) {
                return Err(ServiceError::Unavailable);
            }
            let value = if current {
                source
                    .forecast()
                    .epoch()
                    .current_financial_amount()
                    .map_err(|_| ServiceError::InvalidResult)?
            } else {
                let [point] = source.forecast().native_distribution().path().points() else {
                    return Err(ServiceError::InvalidResult);
                };
                decimal(point.central())?
            };
            result.sources.push(source);
            result.values.push(value);
            Ok(())
        };
        match method {
            AutomaticValuationMethod::DiscountedCashFlow => {
                for offset in 1..=4 {
                    select(FinancialAmountRole::CommonEquityCashFlow, offset, false)?;
                }
            }
            AutomaticValuationMethod::ResidualIncome => {
                select(FinancialAmountRole::CommonBookEquity, 1, true)?;
                for offset in 1..=3 {
                    select(FinancialAmountRole::CommonNetIncome, offset, false)?;
                }
                for offset in 1..=2 {
                    select(FinancialAmountRole::CommonBookEquity, offset, false)?;
                }
            }
            _ => return Err(ServiceError::InvalidRequest),
        }
        Ok(result)
    }

    fn value(
        &self,
        values: &[Decimal],
        rate: Decimal,
        cap: Decimal,
    ) -> Result<Decimal, ServiceError> {
        let residual = self.method == AutomaticValuationMethod::ResidualIncome;
        if values.len() != if residual { 6 } else { 4 } {
            return Err(ServiceError::InvalidResult);
        }
        let mut value = if residual { values[0] } else { Decimal::ZERO };
        for offset in 1..=3 {
            let amount = if residual {
                let book = if offset == 1 {
                    values[0]
                } else {
                    values[3 + offset - 1]
                };
                values[offset]
                    .checked_sub(book.checked_mul(rate).ok_or(ServiceError::InvalidResult)?)
                    .ok_or(ServiceError::InvalidResult)?
            } else {
                values[offset - 1]
            };
            value = value
                .checked_add(
                    AnnualEquityArithmetic::discounted_amount(
                        amount,
                        rate,
                        NonZeroU32::new(offset as u32).ok_or(ServiceError::Internal)?,
                    )
                    .map_err(|_| ServiceError::Unavailable)?,
                )
                .ok_or(ServiceError::InvalidResult)?;
        }
        if !residual {
            let growth = AnnualEquityArithmetic::conditional_terminal_growth(
                values[2], values[3], cap, rate,
            )
            .map_err(|_| ServiceError::Unavailable)?
            .1;
            value = value
                .checked_add(
                    AnnualEquityArithmetic::discounted_terminal_fcfe(
                        values[3],
                        rate,
                        growth,
                        NonZeroU32::new(3).ok_or(ServiceError::Internal)?,
                    )
                    .map_err(|_| ServiceError::Unavailable)?,
                )
                .ok_or(ServiceError::InvalidResult)?;
        }
        Ok(value)
    }
}

fn decimal(value: market_squawk_modeling::ForecastValue) -> Result<Decimal, ServiceError> {
    Decimal::try_from_i128_with_scale(value.mantissa(), u32::from(value.scale()))
        .map_err(|_| ServiceError::InvalidResult)
}

impl FairValueDomainService {
    pub(super) async fn calculate_historical_native_valuation(
        &self,
        research: &ResearchService,
        epoch: &FeatureDatasetInputEpoch,
        sources: &[HistoricalOriginFinancialForecast],
        macro_context: &MacroInvestmentContext,
        premium: &HistoricalOriginEquityPremiumRead,
        method: AutomaticValuationMethod,
        request: &AutomaticForecastValuationRequest,
        context: &RequestContext,
    ) -> Result<HistoricalMethodReceipt, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let origin = epoch.target_origin().ok_or(ServiceError::InvalidRequest)?;
        let economic_date = origin
            .utc_calendar_date()
            .map_err(|_| ServiceError::InvalidResult)?;
        let epoch_identity = Sha256Digest::new(
            Sha256::digest(
                epoch
                    .canonical_bytes()
                    .map_err(|_| ServiceError::InvalidResult)?,
            )
            .into(),
        );
        let risk_free = macro_context.valuation_rates().ten_year_reference();
        let rate_date = risk_free
            .effective()
            .calendar_date_value()
            .ok_or(ServiceError::Unavailable)?;
        let age = economic_date
            .days_since_unix_epoch()
            .checked_sub(rate_date.days_since_unix_epoch())
            .ok_or(ServiceError::InvalidResult)?;
        if premium.epoch_identity() != epoch_identity
            || macro_context.knowledge_cutoff() != epoch.source_selection_as_of()
            || macro_context.effective_date_cutoff() != economic_date
            || !(0..=30).contains(&age)
            || risk_free.available_at() > epoch.source_selection_as_of()
            || epoch
                .current_unit_price()
                .map_err(|_| ServiceError::Unavailable)?
                .currency()
                .as_str()
                != "USD"
        {
            return Err(ServiceError::Unavailable);
        }
        let native = NativeHistoricalInputs::select(epoch, sources, method)?;
        let cap = risk_free
            .annual_yield_percent()
            .checked_div(Decimal::from(100_u32))
            .ok_or(ServiceError::InvalidResult)?;
        let rate = cap
            .checked_add(premium.annual_premium())
            .ok_or(ServiceError::InvalidResult)?;
        if rate <= Decimal::ZERO {
            return Err(ServiceError::Unavailable);
        }
        let mut roots = premium.parent_manifests().to_vec();
        for parent in macro_context.parent_manifests() {
            if !roots.contains(parent) {
                roots.push(parent.clone());
            }
        }
        for source in &native.sources {
            let distribution = source.forecast().native_distribution();
            for parent in [
                distribution.path().dataset().manifest(),
                distribution.input_manifest(),
                distribution.epoch().source_manifest(),
            ] {
                if !roots.contains(parent) {
                    roots.push(parent.clone());
                }
            }
        }
        // All source parents are admitted before arithmetic, even for an unavailable range.
        let authorization = super::methods::authorize_sources(research, &roots, context)?;
        let value = native.value(&native.values, rate, cap)?;
        let (mut lower, mut upper) = (value, value);
        let mut evidence = Sha256::new();
        evidence.update(b"market-squawk/historical-native-one-input-support/v1\0");
        evidence.update(premium.identity().bytes());
        evidence.update(macro_context.evidence_digest().bytes());
        for (index, source) in native.sources.iter().enumerate() {
            ensure_request_live(context, &self.lifecycle)?;
            evidence.update(source.forecast().native_distribution().identity().bytes());
            evidence.update(source.inference_rights_decision().bytes());
            evidence.update(source.inference_rights_graph().bytes());
            evidence.update(source.training_build().bytes());
            evidence.update(source.inputs_build().bytes());
            if method == AutomaticValuationMethod::ResidualIncome && index == 0 {
                continue;
            }
            let points = source.forecast().native_distribution().points();
            if points.is_empty() || points.len() > 126 {
                return Err(ServiceError::Unavailable);
            }
            for point in [points.first(), points.last()] {
                let mut values = native.values.clone();
                values[index] = decimal(point.ok_or(ServiceError::InvalidResult)?.value())?;
                let scenario = native.value(&values, rate, cap)?;
                lower = lower.min(scenario);
                upper = upper.max(scenario);
                evidence.update((index as u64).to_be_bytes());
                evidence.update(scenario.mantissa().to_be_bytes());
                evidence.update(scenario.scale().to_be_bytes());
            }
        }
        // These are conditional one-input sensitivity limits, not a joint probability interval.
        issue_method_receipt(
            epoch,
            method,
            HistoricalValueBasis::TotalCommonEquity,
            (value, lower, upper),
            request,
            EvidenceDigest::new(DigestAlgorithm::Sha256, evidence.finalize().into()),
            roots,
            authorization,
            context,
        )
    }
}
