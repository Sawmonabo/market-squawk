//! One atomic original-to-current monetary projection. Source recipes remain application-owned.

use super::*;
use market_squawk_data::{
    ForecastCurrentShareConversion, ProviderMarketEventPointInTimeSelection,
    ShareConversionRounding,
};
use market_squawk_valuation::CurrentShareValuationProjection;

/// Original application authorization coordinates committed by the decision projection.
///
/// This inert input grants no market or source authority and cannot be deserialized as a
/// serving proof. The application supplies it from the same actual market read receipt as
/// `market_publication`; replay must reopen sources and match the original projection identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CurrentShareMarketAdmission {
    pub market: MarketReferenceEvidence,
    pub authorized_at: Timestamp,
    pub authorization_expires_at: Timestamp,
    pub authorization_decision_digest: DecisionContentDigest,
}

/// Source-proven common frame for the market, forecast, valuation and financial model.
///
/// Original forecasts, calibration, probabilities and model assumptions retain their native
/// meaning. Only monetary decision comparisons use the projected values. No scalar or durable
/// decoder can construct this proof: replay must reopen sources and run the same projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentShareDecisionProjection {
    identity: DecisionContentDigest,
    original_monetary_identity: DecisionContentDigest,
    original_forecast: PriceForecastEvidence,
    original_valuation: ValuationEvidence,
    original_financial_model: FinancialModelEvidence,
    market: MarketReferenceEvidence,
    market_admission: CurrentShareMarketAdmission,
    conversion: ForecastCurrentShareConversion,
    valuation_projection: CurrentShareValuationProjection,
}

impl CurrentShareDecisionProjection {
    /// Complete original/current derivation, including the application's exact selected mark.
    pub const fn identity(&self) -> DecisionContentDigest {
        self.identity
    }
    /// Commitment to all original financial values and their identities.
    pub const fn original_monetary_identity(&self) -> DecisionContentDigest {
        self.original_monetary_identity
    }
    /// Original calibrated forecast, before monetary projection.
    pub const fn original_forecast(&self) -> PriceForecastEvidence {
        self.original_forecast
    }
    /// Original governed research valuation, before monetary projection.
    pub const fn original_valuation(&self) -> ValuationEvidence {
        self.original_valuation
    }
    /// Original assumptions, scenario and sensitivity amounts.
    pub const fn original_financial_model(&self) -> &FinancialModelEvidence {
        &self.original_financial_model
    }
    /// Exact current application mark and resolver identities.
    pub const fn market(&self) -> MarketReferenceEvidence {
        self.market
    }
    /// Exact original market authorization interval and decision, bound to this projection.
    pub const fn market_admission(&self) -> CurrentShareMarketAdmission {
        self.market_admission
    }
    /// Source-minted conversion, retained for bounded chart overlays and recipe comparison.
    pub const fn conversion(&self) -> &ForecastCurrentShareConversion {
        &self.conversion
    }
    /// Valuation-owned source and calculation validation.
    pub const fn valuation_projection(&self) -> CurrentShareValuationProjection {
        self.valuation_projection
    }
}

impl InvestmentAnalysisEvidence {
    /// Projects all monetary decision inputs together after native forecast/source admission.
    ///
    /// `market_publication` must be obtained from the same current application market receipt
    /// used to construct `market`. The publication identity and canonical event are independently
    /// checked against the sealed valuation/source receipts; application resolver and mark hashes
    /// remain separate parents. `market_admission` records that same receipt's original
    /// authorization, not a new grant. This never extends a source cutoff, expiry or horizon.
    pub fn try_project_current_share_units(
        mut self,
        conversion: ForecastCurrentShareConversion,
        valuation_projection: CurrentShareValuationProjection,
        market_publication: &ProviderMarketEventPointInTimeSelection,
        market_admission: CurrentShareMarketAdmission,
    ) -> Result<Self, InvestmentProposalError> {
        let invalid = InvestmentProposalError::InvalidEvidenceMetric;
        let market = self.market.ok_or(invalid)?;
        let original_forecast = self.price_forecast.ok_or(invalid)?;
        let original_valuation = self.valuation.ok_or(invalid)?;
        let original_financial_model = self.financial_model.as_ref().ok_or(invalid)?;
        let [source] = market_publication.sources() else {
            return Err(invalid);
        };
        let [candidate] = source.tied_candidates() else {
            return Err(invalid);
        };
        let ValuationEvidenceProvenance::ResearchCalculation {
            account_id,
            method,
            calculation_identity,
            input_set_identity,
        } = original_valuation.provenance
        else {
            return Err(invalid);
        };
        if self.current_share_projection.is_some()
            || market_admission.market != market
            || market_admission.authorized_at > self.admitted_at
            || self.admitted_at >= market_admission.authorization_expires_at
            || market.window.expires_at > market_admission.authorization_expires_at
            || market_admission.authorization_decision_digest.evidence_digest().algorithm()
                != DigestAlgorithm::Sha256
            || conversion.instrument_id() != self.instrument_id
            || conversion.currency() != self.currency
            || market.instrument_id != self.instrument_id
            || original_forecast.instrument_id != self.instrument_id
            || original_valuation.instrument_id != self.instrument_id
            || original_financial_model.instrument_id != self.instrument_id
            || account_id != self.account_id
            || original_financial_model.account_id != self.account_id
            || valuation_projection.account_id() != self.account_id
            || valuation_projection.instrument_id() != self.instrument_id
            || method != AutomaticValuationMethod::ForecastDistribution
            || original_financial_model.method != method
            || valuation_projection.calculation_identity() != calculation_identity
            || valuation_projection.input_set_identity() != input_set_identity
            || original_financial_model.calculation_identity
                != sha256_content(calculation_identity.bytes())?
            || original_financial_model.pit_input_set_identity
                != sha256_content(input_set_identity.bytes())?
            || valuation_projection.conversion_identity() != conversion.identity()
            || valuation_projection.original_basis_identity()
                != conversion.original_basis_identity()
            || market_publication.selection_digest().bytes()
                != conversion.current_selection_identity().bytes()
            || candidate.coordinate().canonical_event_digest().bytes()
                != conversion.current_observation_identity().bytes()
            || valuation_projection.market_selection_identity()
                != conversion.current_selection_identity()
            || valuation_projection.market_observation_identity()
                != conversion.current_observation_identity()
            || market.price != valuation_projection.market_price()
            || market.window.observed_at != conversion.quote_at()
            || market.window.source_knowledge_cutoff != conversion.market_cutoff()
            || market.window.expires_at <= self.admitted_at
            || conversion.market_cutoff() > self.admitted_at
            || conversion.knowledge_cutoff() > self.admitted_at
            || valuation_projection.admitted_at() != self.admitted_at
            || valuation_projection.expires_at() <= self.admitted_at
            || original_forecast.vintage_id.bytes()
                != valuation_projection.vintage_identity().bytes()
            || original_forecast.horizon_at != valuation_projection.horizon_at()
            || original_valuation.horizon_at != valuation_projection.horizon_at()
            || original_financial_model.horizon_at != valuation_projection.horizon_at()
            || original_forecast.window.source_knowledge_cutoff
                != original_valuation.window.source_knowledge_cutoff
            || original_financial_model.range.central() != original_valuation.fair_value
            || self.forecast_chart.as_ref().is_some_and(|chart| {
                chart.basis_identity().evidence_digest().bytes()
                    != conversion.original_basis_identity().bytes()
            })
        {
            return Err(invalid);
        }

        let original_range = valuation_projection.original_range();
        if original_valuation.fair_value != original_range.central().money()
            || original_financial_model.range.lower() != original_range.lower().money()
            || original_financial_model.range.central() != original_range.central().money()
            || original_financial_model.range.upper() != original_range.upper().money()
            || original_financial_model.scenarios.downside() != original_range.lower().money()
            || original_financial_model.scenarios.base() != original_range.central().money()
            || original_financial_model.scenarios.upside() != original_range.upper().money()
            || original_financial_model.sensitivity_range.lower()
                != valuation_projection.original_sensitivity_lower()
            || original_financial_model.sensitivity_range.upper()
                != valuation_projection.original_sensitivity_upper()
        {
            return Err(InvestmentProposalError::InvalidValuationSelection);
        }

        // Reuse the canonical evidence encoder on exactly the four original monetary inputs.
        // Statistical studies, probabilities and charts are intentionally not transformed.
        let originals = Self::new(InvestmentAnalysisEvidenceInput {
            instrument_id: self.instrument_id,
            currency: self.currency,
            account_id: self.account_id,
            as_of: self.as_of,
            admitted_at: self.admitted_at,
            market: Some(market),
            price_forecast: Some(original_forecast),
            valuation: Some(original_valuation),
            financial_model: Some(original_financial_model.clone()),
            backtest: None,
            out_of_sample: None,
            harmonic_pattern: None,
            liquidity: None,
            portfolio_risk: None,
        });
        let original_monetary_identity =
            sha256_content(super::super::digest::hash_evidence(&originals))?;
        let project = |money, rounding| {
            conversion
                .project_money(money, rounding)
                .map_err(|_| InvestmentProposalError::ArithmeticOverflow)
        };
        let range = |value: TargetPriceRange| -> Result<TargetPriceRange, InvestmentProposalError> {
            strict_range(
                project(value.lower(), ShareConversionRounding::Lower)?,
                project(value.upper(), ShareConversionRounding::Upper)?,
            )
        };
        let forecast_ranges = ForecastPriceRanges::try_new(
            range(original_forecast.ranges.downside)?,
            range(original_forecast.ranges.base)?,
            range(original_forecast.ranges.upside)?,
        )?;
        let forecast_cases = TargetPriceCases::try_new(
            project(
                original_forecast.cases.downside(),
                ShareConversionRounding::Central,
            )?,
            project(
                original_forecast.cases.base(),
                ShareConversionRounding::Central,
            )?,
            project(
                original_forecast.cases.upside(),
                ShareConversionRounding::Central,
            )?,
        )
        .map_err(|_| InvestmentProposalError::InvalidPrice)?;
        let forecast = PriceForecastEvidence::try_new(
            original_forecast.instrument_id,
            forecast_cases,
            forecast_ranges,
            original_forecast.horizon_at,
            original_forecast.expected_terminal_statistic,
            original_forecast
                .expected_terminal_price
                .map(|money| project(money, ShareConversionRounding::Central))
                .transpose()?,
            original_forecast.expected_terminal_horizon_at,
            original_forecast.expected_terminal_statistic_identity,
            original_forecast.vintage_id,
            original_forecast.output_binding_identity,
            original_forecast.calibration_identity,
            original_forecast.outcome_set_identity,
            original_forecast.calibration,
            original_forecast.window,
        )?;
        let projected_range = valuation_projection.range();
        let model_range = FinancialModelValueRange::try_new(
            project(
                original_financial_model.range.lower(),
                ShareConversionRounding::Lower,
            )?,
            project(
                original_financial_model.range.central(),
                ShareConversionRounding::Central,
            )?,
            project(
                original_financial_model.range.upper(),
                ShareConversionRounding::Upper,
            )?,
        )?;
        let sensitivity_range = range(original_financial_model.sensitivity_range)?;
        let scenarios = TargetPriceCases::try_new(
            project(
                original_financial_model.scenarios.downside(),
                ShareConversionRounding::Lower,
            )?,
            project(
                original_financial_model.scenarios.base(),
                ShareConversionRounding::Central,
            )?,
            project(
                original_financial_model.scenarios.upside(),
                ShareConversionRounding::Upper,
            )?,
        )
        .map_err(|_| InvestmentProposalError::InvalidPrice)?;
        let fair_value = project(
            original_valuation.fair_value,
            ShareConversionRounding::Central,
        )?;
        if model_range.lower() != projected_range.lower().money()
            || model_range.central() != projected_range.central().money()
            || model_range.upper() != projected_range.upper().money()
            || fair_value != model_range.central()
            || scenarios.downside() != model_range.lower()
            || scenarios.base() != model_range.central()
            || scenarios.upside() != model_range.upper()
            || sensitivity_range.lower() != valuation_projection.sensitivity_lower()
            || sensitivity_range.upper() != valuation_projection.sensitivity_upper()
            || model_range.lower().amount() >= model_range.upper().amount()
            || scenarios.downside().amount() >= scenarios.base().amount()
            || scenarios.base().amount() >= scenarios.upside().amount()
        {
            return Err(InvestmentProposalError::InvalidPrice);
        }

        let mut hash = Sha256::new();
        hash.update(b"market-squawk/current-share-decision-projection/v2\0");
        hash.update(original_monetary_identity.evidence_digest().bytes());
        hash.update(conversion.identity().bytes());
        hash.update(valuation_projection.identity().bytes());
        hash.update(market_admission.authorized_at.unix_nanos().to_be_bytes());
        hash.update(market_admission.authorization_expires_at.unix_nanos().to_be_bytes());
        hash.update(market_admission.authorization_decision_digest.evidence_digest().bytes());
        let identity = sha256_content(hash.finalize().into())?;
        let proof = CurrentShareDecisionProjection {
            identity,
            original_monetary_identity,
            original_forecast,
            original_valuation,
            original_financial_model: original_financial_model.clone(),
            market,
            market_admission,
            conversion,
            valuation_projection,
        };
        let mut model = original_financial_model.clone();
        model.range = model_range;
        model.scenarios = scenarios;
        model.sensitivity_range = sensitivity_range;
        model.scenario_identity = sha256_content(valuation_projection.scenario_identity().bytes())?;
        model.sensitivity_identity =
            sha256_content(valuation_projection.sensitivity_identity().bytes())?;
        model.window.expires_at = model
            .window
            .expires_at
            .min(valuation_projection.expires_at());
        // Original assumptions and their source clocks describe the original calculation.
        // Their monetary endpoints are exposed in proof.original_financial_model, never rewritten.
        let mut valuation = original_valuation;
        valuation.fair_value = fair_value;
        valuation.window.expires_at = valuation
            .window
            .expires_at
            .min(valuation_projection.expires_at());
        self.price_forecast = Some(forecast);
        self.valuation = Some(valuation);
        self.financial_model = Some(model);
        self.current_share_projection = Some(proof);
        Ok(self)
    }

    /// Common monetary derivation; original statistical/chart evidence is available separately.
    pub const fn current_share_projection(&self) -> Option<&CurrentShareDecisionProjection> {
        self.current_share_projection.as_ref()
    }
}

fn strict_range(lower: Money, upper: Money) -> Result<TargetPriceRange, InvestmentProposalError> {
    if lower.amount() >= upper.amount() {
        return Err(InvestmentProposalError::InvalidPrice);
    }
    TargetPriceRange::try_new(lower, upper).map_err(|_| InvestmentProposalError::InvalidPrice)
}
