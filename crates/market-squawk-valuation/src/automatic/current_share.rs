//! Original calculation projected into one source-proven current quote's share units.

use super::*;
use market_squawk_data::{
    FeatureDatasetInputEpoch, ForecastCurrentShareConversion, ShareConversionRounding,
};
use sha2::{Digest as _, Sha256};

/// Derived research values, not a replacement calculation or execution authority.
///
/// Only an original, revalidated method receipt, genuine share-basis evidence and a source-issued
/// price conversion can produce this value. Persist its coordinates with the original source recipes; recovery must
/// reopen those sources and call the same constructor. There is no scalar or JSON constructor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentShareValuationProjection {
    identity: EvidenceDigest,
    calculation_identity: AutomaticValuationIdentity,
    input_set_identity: AutomaticValuationInputSetIdentity,
    instrument_id: InstrumentId,
    account_id: AccountId,
    source_identity: EvidenceDigest,
    method: AutomaticValuationMethod,
    forecast_selected_at: Timestamp,
    fundamental_sources: Option<Box<[u8]>>,
    vintage_identity: Option<market_squawk_data::Sha256Digest>,
    distribution_identity: Option<market_squawk_data::Sha256Digest>,
    conversion_identity: market_squawk_data::Sha256Digest,
    original_basis_identity: market_squawk_data::Sha256Digest,
    market_selection_identity: market_squawk_data::Sha256Digest,
    market_observation_identity: market_squawk_data::Sha256Digest,
    market_price: Money,
    quote_at: Timestamp,
    knowledge_cutoff: Timestamp,
    admitted_at: Timestamp,
    expires_at: Timestamp,
    horizon_at: Timestamp,
    original_range: AutomaticValuationRange,
    original_sensitivity_lower: Money,
    original_sensitivity_upper: Money,
    range: AutomaticValuationRange,
    sensitivity_lower: Money,
    sensitivity_upper: Money,
    scenario_identity: EvidenceDigest,
    sensitivity_identity: EvidenceDigest,
}

impl AutomaticValuationMethodReceipt {
    /// Projects the original saved range and source-derived sensitivity into exact current units.
    ///
    /// Application callers must separately recheck current market authorization and physically
    /// reopen the source conversion on restart. This function rejects a different forecast epoch,
    /// quote, calculation or unit basis, even if currency, dates and rounded prices happen to match.
    pub fn project_current_share_units(
        &self,
        epoch: &FeatureDatasetInputEpoch,
        conversion: &ForecastCurrentShareConversion,
        admitted_at: Timestamp,
    ) -> Result<CurrentShareValuationProjection, AutomaticValuationError> {
        let invalid = AutomaticValuationError::Conflict(AutomaticValuationConflict::Evidence);
        if self.method != AutomaticValuationMethod::ForecastDistribution
            || self.range.central.basis() != ValuationAmountBasis::PerInstrumentUnit
            || self.instrument_id != conversion.instrument_id()
            || self.range.central.money().currency() != conversion.currency()
            || self.calculated_at > admitted_at
            || conversion.knowledge_cutoff() > admitted_at
            || admitted_at >= self.expires_at
            || epoch.instrument_id() != self.instrument_id
            || epoch.target_at() != self.forecast_terminal_at
        {
            return Err(invalid);
        }
        // Recompute source/input membership, complete outcome mass, every intermediate, bounds
        // and both original identities before deriving any new monetary value.
        verify_recovered_receipt(self)?;
        if input_set_identity(&self.inputs)? != self.input_set_id
            || receipt_identity(self)? != self.id
        {
            return Err(invalid);
        }
        let market = recovered_input(self, self.current_market_input)?;
        let EvidenceOrigin::PublishedMarket {
            evidence: market_origin,
        } = market.input().evidence().origin()
        else {
            return Err(invalid);
        };
        if market_origin.selection_digest.bytes() != conversion.current_selection_identity().bytes()
            || market_origin.canonical_event_digest.bytes()
                != conversion.current_observation_identity().bytes()
            || market_origin.knowledge_at != conversion.market_cutoff()
            || market.input().evidence().effective_at() != Some(conversion.quote_at())
        {
            return Err(invalid);
        }
        let first = self.intermediates.first().ok_or(invalid)?;
        let input = recovered_input(self, first.primary_input)?;
        let EvidenceOrigin::ForecastDistribution { evidence } = input.input().evidence().origin()
        else {
            return Err(invalid);
        };
        let source = evidence.source();
        verify_price_source_epoch(source, epoch, conversion)?;
        if source.reference().knowledge_at() != self.measurement_at
            || source.distribution().target_at() != self.forecast_terminal_at
        {
            return Err(invalid);
        }
        let project = |amount: Money, rounding| {
            conversion
                .project_money(amount, rounding)
                .map_err(|_| AutomaticValuationError::Arithmetic)
        };
        let scale = u8::try_from(conversion.output_scale()).map_err(|_| invalid)?;
        let amount = |money| {
            ValuationAmount::try_new(money, scale, ValuationAmountBasis::PerInstrumentUnit)
                .map_err(|_| AutomaticValuationError::Arithmetic)
        };
        let range = AutomaticValuationRange::try_new(
            amount(project(
                self.range.lower.money(),
                ShareConversionRounding::Lower,
            )?)?,
            amount(project(
                self.range.central.money(),
                ShareConversionRounding::Central,
            )?)?,
            amount(project(
                self.range.upper.money(),
                ShareConversionRounding::Upper,
            )?)?,
        )?;
        if (self.range.lower.money().amount() < self.range.upper.money().amount()
            && range.lower.money().amount() >= range.upper.money().amount())
            || range.lower.money().amount() <= Decimal::ZERO
        {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::Uncertainty,
            ));
        }
        // Reproduce the saved method's leave-one-outcome-out sensitivity in original units.
        // Only then project its retained endpoints, each with one exact outward share conversion.
        let (raw_lower, raw_upper) = forecast_sensitivity(self)?;
        let currency = conversion.currency();
        let sensitivity_lower = project(
            Money::new(raw_lower, currency),
            ShareConversionRounding::Lower,
        )?;
        let sensitivity_upper = project(
            Money::new(raw_upper, currency),
            ShareConversionRounding::Upper,
        )?;
        if sensitivity_lower.amount() > range.central.money().amount()
            || sensitivity_upper.amount() < range.central.money().amount()
            || sensitivity_lower.amount() > sensitivity_upper.amount()
        {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::Uncertainty,
            ));
        }
        let zero = EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]);
        let mut value = CurrentShareValuationProjection {
            identity: zero,
            calculation_identity: self.id,
            input_set_identity: self.input_set_id,
            instrument_id: self.instrument_id,
            account_id: self.account_id,
            source_identity: source.reference().identity(),
            method: self.method,
            forecast_selected_at: source.reference().selected_at(),
            fundamental_sources: None,
            vintage_identity: Some(source.reference().vintage_id()),
            distribution_identity: Some(source.reference().distribution_identity()),
            conversion_identity: conversion.identity(),
            original_basis_identity: conversion.original_basis_identity(),
            market_selection_identity: conversion.current_selection_identity(),
            market_observation_identity: conversion.current_observation_identity(),
            market_price: market.input().amount().money(),
            quote_at: conversion.quote_at(),
            knowledge_cutoff: conversion.knowledge_cutoff(),
            admitted_at,
            expires_at: self.expires_at,
            horizon_at: self.forecast_terminal_at.ok_or(invalid)?,
            original_range: self.range,
            original_sensitivity_lower: Money::new(raw_lower, currency),
            original_sensitivity_upper: Money::new(raw_upper, currency),
            range,
            sensitivity_lower,
            sensitivity_upper,
            scenario_identity: zero,
            sensitivity_identity: zero,
        };
        value.scenario_identity =
            projection_digest(&value, b"market-squawk/current-share-model-scenarios/v1");
        value.sensitivity_identity =
            projection_digest(&value, b"market-squawk/current-share-model-sensitivity/v1");
        value.identity = projection_digest(&value, b"market-squawk/current-share-valuation/v1");
        Ok(value)
    }
}

fn forecast_sensitivity(
    receipt: &AutomaticValuationMethodReceipt,
) -> Result<(Decimal, Decimal), AutomaticValuationError> {
    let arithmetic = AutomaticValuationError::Arithmetic;
    let total = receipt
        .intermediates
        .iter()
        .try_fold(Decimal::ZERO, |sum, step| {
            sum.checked_add(step.result).ok_or(arithmetic)
        })?;
    let mut lower = receipt.range.central.money().amount();
    let mut upper = lower;
    for step in &receipt.intermediates {
        let value = if receipt.intermediates.len() == 1 {
            total
        } else {
            total
                .checked_sub(step.result)
                .and_then(|remaining| {
                    Decimal::ONE
                        .checked_sub(step.factor)
                        .and_then(|mass| remaining.checked_div(mass))
                })
                .ok_or(arithmetic)?
        };
        let value = round(
            value,
            receipt.range.central.scale(),
            receipt.arithmetic_policy.rounding(),
        );
        lower = lower.min(value);
        upper = upper.max(value);
    }
    Ok((lower, upper))
}

fn projection_digest(value: &CurrentShareValuationProjection, domain: &[u8]) -> EvidenceDigest {
    let mut hash = CanonicalHasher::new(domain);
    for bytes in [
        value.calculation_identity.bytes(),
        value.input_set_identity.bytes(),
        value.source_identity.bytes(),
        value.conversion_identity.bytes(),
        value.original_basis_identity.bytes(),
        value.market_selection_identity.bytes(),
        value.market_observation_identity.bytes(),
    ] {
        hash.fixed(bytes);
    }
    hash.u8(match value.method {
        AutomaticValuationMethod::DiscountedCashFlow => 1,
        AutomaticValuationMethod::ComparableCompanies => 2,
        AutomaticValuationMethod::ResidualIncome => 3,
        AutomaticValuationMethod::ForecastDistribution => 4,
    });
    match (value.vintage_identity, value.distribution_identity) {
        (Some(vintage), Some(distribution)) => {
            hash.u8(1);
            hash.fixed(vintage.bytes());
            hash.fixed(distribution.bytes());
        }
        _ => hash.u8(0),
    }
    if let Some(reference) = &value.fundamental_sources {
        hash.bytes(reference);
    }
    for at in [
        value.forecast_selected_at,
        value.quote_at,
        value.knowledge_cutoff,
        value.admitted_at,
        value.expires_at,
        value.horizon_at,
    ] {
        hash.i64(at.unix_nanos());
    }
    hash.u8(value.original_range.central.scale());
    hash.u8(value.range.central.scale());
    for money in [
        value.market_price,
        value.original_range.lower.money(),
        value.original_range.central.money(),
        value.original_range.upper.money(),
        value.original_sensitivity_lower,
        value.original_sensitivity_upper,
        value.range.lower.money(),
        value.range.central.money(),
        value.range.upper.money(),
        value.sensitivity_lower,
        value.sensitivity_upper,
    ] {
        hash.bytes(&money.amount().mantissa().to_be_bytes());
        hash.u32(money.amount().scale());
        hash.bytes(money.currency().as_str().as_bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finish())
}

impl CurrentShareValuationProjection {
    pub const fn method(&self) -> AutomaticValuationMethod {
        self.method
    }
    pub const fn forecast_selected_at(&self) -> Timestamp {
        self.forecast_selected_at
    }
    /// Inert source recipe; recovery must reopen every source and compare this projection.
    pub fn fundamental_source_reference(&self) -> Option<&[u8]> {
        self.fundamental_sources.as_deref()
    }

    /// Complete commitment to the original receipt, source conversion, timing and projected values.
    pub const fn identity(&self) -> EvidenceDigest {
        self.identity
    }
    /// Original immutable calculation identity; projection does not replace it.
    pub const fn calculation_identity(&self) -> AutomaticValuationIdentity {
        self.calculation_identity
    }
    /// Original point-in-time input set identity.
    pub const fn input_set_identity(&self) -> AutomaticValuationInputSetIdentity {
        self.input_set_identity
    }
    /// Exact security shared by the original calculation and current source conversion.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Original reporting account.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }
    /// Exact original forecast source or source-derived fundamental share-basis identity.
    pub const fn source_identity(&self) -> EvidenceDigest {
        self.source_identity
    }
    /// Exact original forecast vintage.
    pub const fn vintage_identity(&self) -> Option<market_squawk_data::Sha256Digest> {
        self.vintage_identity
    }
    /// Exact original empirical distribution, including model/output evidence.
    pub const fn distribution_identity(&self) -> Option<market_squawk_data::Sha256Digest> {
        self.distribution_identity
    }
    /// Sealed source conversion required for all these projected prices.
    pub const fn conversion_identity(&self) -> market_squawk_data::Sha256Digest {
        self.conversion_identity
    }
    /// Original forecast share-unit frame.
    pub const fn original_basis_identity(&self) -> market_squawk_data::Sha256Digest {
        self.original_basis_identity
    }
    /// Exact current published market selection.
    pub const fn market_selection_identity(&self) -> market_squawk_data::Sha256Digest {
        self.market_selection_identity
    }
    /// Exact canonical current event, distinct from application receipt identity.
    pub const fn market_observation_identity(&self) -> market_squawk_data::Sha256Digest {
        self.market_observation_identity
    }
    /// Exact original published current mark; its selection and event are conversion-bound.
    pub const fn market_price(&self) -> Money {
        self.market_price
    }
    /// Effective instant of the current quote share frame.
    pub const fn quote_at(&self) -> Timestamp {
        self.quote_at
    }
    /// Knowledge cutoff of the complete source conversion.
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }
    /// Saved admission instant to reproduce when reconstructing this projection.
    pub const fn admitted_at(&self) -> Timestamp {
        self.admitted_at
    }
    /// Original calculation expiry, including its exact current market lifetime.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
    /// Admitted comparison horizon; a share conversion does not extend it.
    pub const fn horizon_at(&self) -> Timestamp {
        self.horizon_at
    }
    /// Exact original saved range, before share conversion or output-scale projection.
    pub const fn original_range(&self) -> AutomaticValuationRange {
        self.original_range
    }
    /// Exact original model sensitivity lower endpoint, before share conversion.
    pub const fn original_sensitivity_lower(&self) -> Money {
        self.original_sensitivity_lower
    }
    /// Exact original model sensitivity upper endpoint, before share conversion.
    pub const fn original_sensitivity_upper(&self) -> Money {
        self.original_sensitivity_upper
    }
    /// Empirical support cases: lower, original calculated center and upper in current shares.
    pub const fn range(&self) -> AutomaticValuationRange {
        self.range
    }
    /// Original method sensitivity lower endpoint converted with downward rounding.
    pub const fn sensitivity_lower(&self) -> Money {
        self.sensitivity_lower
    }
    /// Original method sensitivity upper endpoint converted with upward rounding.
    pub const fn sensitivity_upper(&self) -> Money {
        self.sensitivity_upper
    }
    /// Derived scenario identity retaining original calculation and conversion identities.
    pub const fn scenario_identity(&self) -> EvidenceDigest {
        self.scenario_identity
    }
    /// Derived sensitivity identity retaining original calculation and conversion identities.
    pub const fn sensitivity_identity(&self) -> EvidenceDigest {
        self.sensitivity_identity
    }
}

impl AutomaticValuationMethodReceipt {
    /// Original method sensitivity, kept in its native amount basis.
    /// Native financial methods use the source-derived one-input support range retained by their
    /// uncertainty assumptions. This does not estimate missing continuing value or dilution.
    pub fn original_share_projection_sensitivity(
        &self,
    ) -> Result<(Money, Money), AutomaticValuationError> {
        verify_recovered_receipt(self)?;
        let currency = self.range.central.money().currency();
        let (lower, upper) = match self.method {
            AutomaticValuationMethod::ForecastDistribution => forecast_sensitivity(self)?,
            AutomaticValuationMethod::DiscountedCashFlow
            | AutomaticValuationMethod::ResidualIncome => (
                self.range.lower.money().amount(),
                self.range.upper.money().amount(),
            ),
            AutomaticValuationMethod::ComparableCompanies => {
                let invalid = AutomaticValuationError::InvalidContract;
                let subject = self
                    .intermediates
                    .iter()
                    .find(|step| {
                        step.kind == AutomaticValuationIntermediateKind::ComparableSubjectValue
                    })
                    .ok_or(invalid)?;
                let peers: Vec<_> = self
                    .intermediates
                    .iter()
                    .filter(|step| {
                        step.kind == AutomaticValuationIntermediateKind::WeightedComparableMultiple
                    })
                    .collect();
                if peers.len() < 2 {
                    return Err(invalid);
                }
                let sum = peers.iter().try_fold(Decimal::ZERO, |sum, step| {
                    sum.checked_add(step.result)
                        .ok_or(AutomaticValuationError::Arithmetic)
                })?;
                let mut lo = self.range.central.money().amount();
                let mut hi = lo;
                for step in peers {
                    let value = sum
                        .checked_sub(step.result)
                        .and_then(|sum| {
                            Decimal::ONE
                                .checked_sub(step.factor)
                                .and_then(|mass| sum.checked_div(mass))
                        })
                        .and_then(|multiple| multiple.checked_mul(subject.amount))
                        .ok_or(AutomaticValuationError::Arithmetic)?;
                    let value = round(
                        value,
                        self.range.central.scale(),
                        self.arithmetic_policy.rounding(),
                    );
                    lo = lo.min(value);
                    hi = hi.max(value);
                }
                (lo, hi)
            }
        };
        Ok((Money::new(lower, currency), Money::new(upper, currency)))
    }

    /// Projects genuine fundamental values independently of the forecast share conversion.
    /// The retained calculation remains total common equity for DCF/residual. The consumer
    /// horizon is the recommendation's comparison horizon, not a forecast of terminal equity.
    pub fn project_fundamental_current_share_units(
        &self,
        epoch: &FeatureDatasetInputEpoch,
        price_source: &crate::ForecastValuationSource,
        conversion: &ForecastCurrentShareConversion,
        bases: &[CommonShareValuationBasis],
        source_reference: Box<[u8]>,
        admitted_at: Timestamp,
        consumer_horizon: Timestamp,
    ) -> Result<CurrentShareValuationProjection, AutomaticValuationError> {
        let invalid = AutomaticValuationError::InvalidContract;
        let denominator = self.validate_common_share_bases(bases)?;
        verify_price_source_epoch(price_source, epoch, conversion)?;
        if price_source.distribution().target_at() != Some(consumer_horizon) {
            return Err(invalid);
        }
        let expected_basis = if self.method == AutomaticValuationMethod::ComparableCompanies {
            ValuationAmountBasis::PerInstrumentUnit
        } else {
            ValuationAmountBasis::TotalCommonEquity
        };
        if self.method == AutomaticValuationMethod::ForecastDistribution
            || self.range.central.basis() != expected_basis
            || self.instrument_id != conversion.instrument_id()
            || self.range.central.money().currency() != conversion.currency()
            || self.calculated_at > admitted_at
            || admitted_at >= self.expires_at
            || consumer_horizon <= admitted_at
            || conversion.knowledge_cutoff() > admitted_at
            || source_reference.is_empty()
            || source_reference.len() > 262_144
        {
            return Err(invalid);
        }
        let market = recovered_input(self, self.current_market_input)?;
        let EvidenceOrigin::PublishedMarket { evidence } = market.input().evidence().origin()
        else {
            return Err(invalid);
        };
        if evidence.selection_digest.bytes() != conversion.current_selection_identity().bytes()
            || evidence.canonical_event_digest.bytes()
                != conversion.current_observation_identity().bytes()
            || market.input().evidence().effective_at() != Some(conversion.quote_at())
            || evidence.knowledge_at != conversion.market_cutoff()
            || bases
                .first()
                .is_none_or(|basis| basis.filing().instrument_id() != self.instrument_id)
        {
            return Err(invalid);
        }
        let scale = u8::try_from(conversion.output_scale()).map_err(|_| invalid)?;
        let project = |money: Money, rounding| -> Result<Money, AutomaticValuationError> {
            Ok(Money::new(
                super::common_shares::divide_common_shares(
                    money.amount(),
                    denominator,
                    u32::from(scale),
                    rounding,
                )?,
                money.currency(),
            ))
        };
        let amount = |money| {
            ValuationAmount::try_new(money, scale, ValuationAmountBasis::PerInstrumentUnit)
                .map_err(|_| AutomaticValuationError::Arithmetic)
        };
        let range = AutomaticValuationRange::try_new(
            amount(project(
                self.range.lower.money(),
                ShareConversionRounding::Lower,
            )?)?,
            amount(project(
                self.range.central.money(),
                ShareConversionRounding::Central,
            )?)?,
            amount(project(
                self.range.upper.money(),
                ShareConversionRounding::Upper,
            )?)?,
        )?;
        let (original_sensitivity_lower, original_sensitivity_upper) =
            self.original_share_projection_sensitivity()?;
        let sensitivity_lower =
            project(original_sensitivity_lower, ShareConversionRounding::Lower)?;
        let sensitivity_upper =
            project(original_sensitivity_upper, ShareConversionRounding::Upper)?;
        if range.lower.money().amount() <= Decimal::ZERO
            || range.lower.money().amount() >= range.upper.money().amount()
            || sensitivity_lower.amount() > range.central.money().amount()
            || sensitivity_upper.amount() < range.central.money().amount()
        {
            return Err(invalid);
        }
        let mut source =
            CanonicalHasher::new(b"market-squawk/fundamental-share-projection-sources/v1");
        for basis in bases {
            source.fixed(basis.identity().bytes());
        }
        source.bytes(REPORTED_COMMON_SHARE_ASSUMPTION.as_bytes());
        let zero = EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]);
        let mut value = CurrentShareValuationProjection {
            identity: zero,
            calculation_identity: self.id,
            input_set_identity: self.input_set_id,
            instrument_id: self.instrument_id,
            account_id: self.account_id,
            source_identity: EvidenceDigest::new(DigestAlgorithm::Sha256, source.finish()),
            method: self.method,
            forecast_selected_at: price_source.reference().selected_at(),
            fundamental_sources: Some(source_reference),
            vintage_identity: Some(price_source.reference().vintage_id()),
            distribution_identity: Some(price_source.reference().distribution_identity()),
            conversion_identity: conversion.identity(),
            original_basis_identity: conversion.original_basis_identity(),
            market_selection_identity: conversion.current_selection_identity(),
            market_observation_identity: conversion.current_observation_identity(),
            market_price: market.input().amount().money(),
            quote_at: conversion.quote_at(),
            knowledge_cutoff: conversion.knowledge_cutoff(),
            admitted_at,
            expires_at: self.expires_at,
            horizon_at: consumer_horizon,
            original_range: self.range,
            original_sensitivity_lower,
            original_sensitivity_upper,
            range,
            sensitivity_lower,
            sensitivity_upper,
            scenario_identity: zero,
            sensitivity_identity: zero,
        };
        value.scenario_identity =
            projection_digest(&value, b"market-squawk/current-share-model-scenarios/v1");
        value.sensitivity_identity =
            projection_digest(&value, b"market-squawk/current-share-model-sensitivity/v1");
        value.identity = projection_digest(&value, b"market-squawk/current-share-valuation/v1");
        Ok(value)
    }
}

fn verify_price_source_epoch(
    source: &crate::ForecastValuationSource,
    epoch: &FeatureDatasetInputEpoch,
    conversion: &ForecastCurrentShareConversion,
) -> Result<(), AutomaticValuationError> {
    let invalid = AutomaticValuationError::Conflict(AutomaticValuationConflict::Evidence);
    let crate::ForecastValuationOriginIdentity::CurrentPriceEpoch(origin_identity) =
        source.reference().source_origin()
    else {
        return Err(invalid);
    };
    let epoch_bytes = epoch.canonical_bytes().map_err(|_| invalid)?;
    let raw_epoch_identity: [u8; 32] = Sha256::digest(&epoch_bytes).into();
    let mut epoch_hash = Sha256::new();
    epoch_hash.update(b"market-squawk/forecast-history/forecast-history-input-epoch/v1\0");
    // This is the data owner's domain-separated canonical byte-array commitment, distinct
    // from the valuation source's raw canonical epoch SHA-256.
    epoch_hash.update(serde_json::to_vec(&epoch_bytes).map_err(|_| invalid)?);
    let conversion_epoch: [u8; 32] = epoch_hash.finalize().into();
    if origin_identity.bytes() != raw_epoch_identity
        || conversion.input_epoch_identity().bytes() != conversion_epoch
        || source.reference().knowledge_at() != epoch.source_selection_as_of()
        || source.distribution().observed_through() != epoch.target_origin()
        || source.origin_bar() != epoch.market_bar()
    {
        return Err(invalid);
    }
    Ok(())
}
