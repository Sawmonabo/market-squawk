//! Saved four-method audit. These projections never grant calculation or source authority.

use super::*;
use crate::MeasurementId;
mod residual;
mod selection;
pub use residual::ResidualIncomeTerminalAudit;
pub use selection::{
    AutomaticValuationForecastPurpose, AutomaticValuationForecastReadAudit,
    AutomaticValuationRecommendationAudit, AutomaticValuationRecommendationOutcome,
};

/// The precise source or calculation stage that actually returned an outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutomaticValuationStage {
    SourceSelection,
    NativeFiscalSources,
    AnnualDiscountSources,
    CalculationPublication,
    NativeCalculationPublication,
}

/// Bounded service failure retained without provider payloads or credentials.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutomaticValuationFailure {
    InvalidRequest,
    NotFound,
    Unauthorized,
    ResourceExhausted,
    Unavailable,
    InvalidResult,
    Internal,
}

/// Saved conditional model disclosure; this scalar mirror grants no source authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DcfTerminalGrowthAudit {
    final_explicit_input: InputId,
    next_period_input: InputId,
    annual_reference_identity: EvidenceDigest,
    annual_rate_identity: EvidenceDigest,
    uncapped_growth: Decimal,
    nominal_risk_free_cap: Decimal,
    applied_growth: Decimal,
    assumption_identity: EvidenceDigest,
}
impl DcfTerminalGrowthAudit {
    /// Restores only a saved disclosure mirror; the actual receipt authenticates all operands.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact saved continuation operands remain distinct"
    )]
    pub fn try_recover(
        final_explicit_input: InputId,
        next_period_input: InputId,
        annual_reference_identity: EvidenceDigest,
        annual_rate_identity: EvidenceDigest,
        uncapped_growth: Decimal,
        nominal_risk_free_cap: Decimal,
        applied_growth: Decimal,
        assumption_identity: EvidenceDigest,
    ) -> Result<Self, AutomaticValuationError> {
        if final_explicit_input.bytes() == [0; 32]
            || next_period_input.bytes() == [0; 32]
            || final_explicit_input == next_period_input
            || [
                annual_reference_identity,
                annual_rate_identity,
                assumption_identity,
            ]
            .into_iter()
            .any(|value| !valid_sha256(value))
            || applied_growth != uncapped_growth.min(nominal_risk_free_cap)
            || applied_growth <= -Decimal::ONE
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        Ok(Self {
            final_explicit_input,
            next_period_input,
            annual_reference_identity,
            annual_rate_identity,
            uncapped_growth: uncapped_growth.normalize(),
            nominal_risk_free_cap: nominal_risk_free_cap.normalize(),
            applied_growth: applied_growth.normalize(),
            assumption_identity,
        })
    }
    pub const fn final_explicit_input(&self) -> InputId {
        self.final_explicit_input
    }
    pub const fn next_period_input(&self) -> InputId {
        self.next_period_input
    }
    pub const fn annual_reference_identity(&self) -> EvidenceDigest {
        self.annual_reference_identity
    }
    pub const fn annual_rate_identity(&self) -> EvidenceDigest {
        self.annual_rate_identity
    }
    pub const fn uncapped_growth(&self) -> Decimal {
        self.uncapped_growth
    }
    pub const fn nominal_risk_free_cap(&self) -> Decimal {
        self.nominal_risk_free_cap
    }
    pub const fn applied_growth(&self) -> Decimal {
        self.applied_growth
    }
    pub const fn assumption_identity(&self) -> EvidenceDigest {
        self.assumption_identity
    }
    pub fn was_capped(&self) -> bool {
        self.uncapped_growth > self.nominal_risk_free_cap
    }
    pub const fn assumption_description(&self) -> &'static str {
        super::terminal::CONTINUATION_ASSUMPTION
    }
}

/// Exact durable calculation reference and its same-unit displayed range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticValuationResultAudit {
    measurement_id: MeasurementId,
    calculation_identity: AutomaticValuationIdentity,
    input_set_identity: AutomaticValuationInputSetIdentity,
    range: AutomaticValuationRange,
    calculated_at: Timestamp,
    expires_at: Timestamp,
    terminal_growth: Option<DcfTerminalGrowthAudit>,
    residual_terminal: Option<ResidualIncomeTerminalAudit>,
    recommendation: AutomaticValuationRecommendationAudit,
}

impl AutomaticValuationResultAudit {
    /// Projects a genuine retained calculation; the projection itself is not an active receipt.
    pub fn from_receipt(
        measurement_id: MeasurementId,
        receipt: &AutomaticValuationMethodReceipt,
        recommendation: AutomaticValuationRecommendationAudit,
    ) -> Result<Self, AutomaticValuationError> {
        let terminal_growth = receipt
            .terminal_growth_policy()?
            .map(|policy| {
                DcfTerminalGrowthAudit::try_recover(
                    policy.final_explicit_input(),
                    policy.next_period_input(),
                    policy.annual_reference_identity(),
                    policy.annual_rate_identity(),
                    policy.uncapped_growth(),
                    policy.nominal_risk_free_cap(),
                    policy.assumption().value(),
                    policy.assumption().evidence(),
                )
            })
            .transpose()?;
        Self::try_recover(
            measurement_id,
            receipt.id(),
            receipt.input_set_id(),
            receipt.range(),
            receipt.calculated_at(),
            receipt.expires_at(),
            terminal_growth,
            receipt
                .residual_terminal()
                .map(ResidualIncomeTerminalAudit::from_receipt),
            recommendation,
        )
    }

    /// Restores a saved display/reference mirror. Active use must reopen the actual calculation.
    pub fn try_recover(
        measurement_id: MeasurementId,
        calculation_identity: AutomaticValuationIdentity,
        input_set_identity: AutomaticValuationInputSetIdentity,
        range: AutomaticValuationRange,
        calculated_at: Timestamp,
        expires_at: Timestamp,
        terminal_growth: Option<DcfTerminalGrowthAudit>,
        residual_terminal: Option<ResidualIncomeTerminalAudit>,
        recommendation: AutomaticValuationRecommendationAudit,
    ) -> Result<Self, AutomaticValuationError> {
        if measurement_id.bytes() == [0; 32]
            || calculation_identity.bytes() == [0; 32]
            || input_set_identity.bytes() == [0; 32]
            || expires_at <= calculated_at
            || recommendation.assessed_at() < calculated_at
            || (recommendation.outcome()
                == AutomaticValuationRecommendationOutcome::NotPerInstrumentUnit)
                != (range.central().basis() != ValuationAmountBasis::PerInstrumentUnit)
            || (recommendation.outcome() == AutomaticValuationRecommendationOutcome::Selected
                && recommendation.assessed_at() >= expires_at)
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        Ok(Self {
            measurement_id,
            calculation_identity,
            input_set_identity,
            range,
            calculated_at,
            expires_at,
            terminal_growth,
            residual_terminal,
            recommendation,
        })
    }
    pub const fn recommendation(&self) -> AutomaticValuationRecommendationAudit {
        self.recommendation
    }
    pub const fn terminal_growth(&self) -> Option<&DcfTerminalGrowthAudit> {
        self.terminal_growth.as_ref()
    }
    pub const fn residual_terminal(&self) -> Option<&ResidualIncomeTerminalAudit> {
        self.residual_terminal.as_ref()
    }
    pub const fn measurement_id(&self) -> MeasurementId {
        self.measurement_id
    }
    pub const fn calculation_identity(&self) -> AutomaticValuationIdentity {
        self.calculation_identity
    }
    pub const fn input_set_identity(&self) -> AutomaticValuationInputSetIdentity {
        self.input_set_identity
    }
    pub const fn range(&self) -> AutomaticValuationRange {
        self.range
    }
    pub const fn calculated_at(&self) -> Timestamp {
        self.calculated_at
    }
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// One completed method. Cancellation/deadline do not claim four-method completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticValuationAttemptAudit {
    method: AutomaticValuationMethod,
    started_at: Timestamp,
    completed_at: Timestamp,
    stage: AutomaticValuationStage,
    outcome: Result<AutomaticValuationResultAudit, AutomaticValuationFailure>,
}
impl AutomaticValuationAttemptAudit {
    /// Validates a saved audit projection; it cannot mint a method receipt or active evidence.
    pub fn try_new(
        method: AutomaticValuationMethod,
        started_at: Timestamp,
        completed_at: Timestamp,
        stage: AutomaticValuationStage,
        outcome: Result<AutomaticValuationResultAudit, AutomaticValuationFailure>,
    ) -> Result<Self, AutomaticValuationError> {
        if completed_at < started_at
            || outcome.as_ref().is_ok_and(|value| {
                value.calculated_at < started_at || value.calculated_at > completed_at
            })
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        Ok(Self {
            method,
            started_at,
            completed_at,
            stage,
            outcome,
        })
    }
    pub const fn method(&self) -> AutomaticValuationMethod {
        self.method
    }
    pub const fn started_at(&self) -> Timestamp {
        self.started_at
    }
    pub const fn completed_at(&self) -> Timestamp {
        self.completed_at
    }
    pub const fn stage(&self) -> AutomaticValuationStage {
        self.stage
    }
    pub const fn outcome(
        &self,
    ) -> &Result<AutomaticValuationResultAudit, AutomaticValuationFailure> {
        &self.outcome
    }
}

/// Immutable completed outcome set retained in the existing analysis bundle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticValuationMethodSetAudit {
    account_id: AccountId,
    instrument_id: InstrumentId,
    source_cutoff: Timestamp,
    market_cutoff: Timestamp,
    profile_identity: EvidenceDigest,
    attempts: [AutomaticValuationAttemptAudit; 4],
    forecast_reads: Box<[AutomaticValuationForecastReadAudit]>,
    selected_measurement_id: Option<MeasurementId>,
    completed_at: Timestamp,
    identity: EvidenceDigest,
}
impl AutomaticValuationMethodSetAudit {
    /// Validates and commits the exact four ordered audit outcomes, including all-failed runs.
    #[allow(
        clippy::too_many_arguments,
        reason = "saved execution coordinates remain distinct"
    )]
    pub fn try_new(
        account_id: AccountId,
        instrument_id: InstrumentId,
        source_cutoff: Timestamp,
        market_cutoff: Timestamp,
        profile_identity: EvidenceDigest,
        attempts: [AutomaticValuationAttemptAudit; 4],
        forecast_reads: Box<[AutomaticValuationForecastReadAudit]>,
        selected_measurement_id: Option<MeasurementId>,
        completed_at: Timestamp,
    ) -> Result<Self, AutomaticValuationError> {
        let required = [
            AutomaticValuationMethod::DiscountedCashFlow,
            AutomaticValuationMethod::ComparableCompanies,
            AutomaticValuationMethod::ResidualIncome,
            AutomaticValuationMethod::ForecastDistribution,
        ];
        if forecast_reads.len() > 17
            || forecast_reads
                .iter()
                .filter(|read| {
                    read.purpose() == AutomaticValuationForecastPurpose::PriceDistribution
                })
                .count()
                > 1
            || (attempts[3].outcome.is_ok()
                && !forecast_reads.iter().any(|read| {
                    read.purpose() == AutomaticValuationForecastPurpose::PriceDistribution
                        && read.outcome().is_ok()
                }))
            || forecast_reads
                .iter()
                .any(|read| read.started_at() < market_cutoff || read.completed_at() > completed_at)
            || forecast_reads
                .windows(2)
                .any(|pair| pair[0].completed_at() > pair[1].started_at())
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        if attempts.iter().any(|attempt| {
            attempt.outcome.as_ref().is_ok_and(|result| {
                result.recommendation.assessed_at() > completed_at
                    || ((result.recommendation.outcome()
                        == AutomaticValuationRecommendationOutcome::Selected)
                        != (selected_measurement_id == Some(result.measurement_id)))
                    || (selected_measurement_id.is_none()
                        && result.recommendation.outcome()
                            == AutomaticValuationRecommendationOutcome::NotCheckedAfterSelection)
            })
        }) {
            return Err(AutomaticValuationError::InvalidContract);
        }
        if !valid_sha256(profile_identity)
            || market_cutoff < source_cutoff
            || market_cutoff > attempts[0].started_at
            || attempts.iter().zip(required).any(|(attempt, method)| {
                attempt.method != method
                    || attempt.completed_at > completed_at
                    || attempt.outcome.as_ref().is_ok_and(|result| {
                        (result.terminal_growth.is_some()
                            != (method == AutomaticValuationMethod::DiscountedCashFlow))
                            || (result.residual_terminal.is_some()
                                != (method == AutomaticValuationMethod::ResidualIncome))
                            || ((result.recommendation.outcome()
                                == AutomaticValuationRecommendationOutcome::ShareUnitBasisUnproven)
                                != (method == AutomaticValuationMethod::ComparableCompanies
                                    && result.range.central().basis()
                                        == ValuationAmountBasis::PerInstrumentUnit))
                    })
            })
            || attempts
                .windows(2)
                .any(|pair| pair[0].completed_at > pair[1].started_at)
            || selected_measurement_id.is_some_and(|id| {
                !attempts.iter().any(|attempt| {
                    attempt.outcome.as_ref().is_ok_and(|result| {
                        result.measurement_id == id
                            && result.range.central().basis()
                                == ValuationAmountBasis::PerInstrumentUnit
                    })
                })
            })
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        let mut value = Self {
            account_id,
            instrument_id,
            source_cutoff,
            market_cutoff,
            profile_identity,
            attempts,
            forecast_reads,
            selected_measurement_id,
            completed_at,
            identity: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        value.identity = value.calculate_identity()?;
        Ok(value)
    }
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub const fn source_cutoff(&self) -> Timestamp {
        self.source_cutoff
    }
    pub const fn market_cutoff(&self) -> Timestamp {
        self.market_cutoff
    }
    pub const fn profile_identity(&self) -> EvidenceDigest {
        self.profile_identity
    }
    pub const fn attempts(&self) -> &[AutomaticValuationAttemptAudit; 4] {
        &self.attempts
    }
    pub fn forecast_reads(&self) -> &[AutomaticValuationForecastReadAudit] {
        &self.forecast_reads
    }
    pub const fn selected_measurement_id(&self) -> Option<MeasurementId> {
        self.selected_measurement_id
    }
    pub const fn completed_at(&self) -> Timestamp {
        self.completed_at
    }
    pub const fn identity(&self) -> EvidenceDigest {
        self.identity
    }

    fn calculate_identity(&self) -> Result<EvidenceDigest, AutomaticValuationError> {
        let mut hash = CanonicalHasher::new(b"market-squawk/automatic-valuation-method-set/v1");
        hash.bytes(self.account_id.as_uuid().as_bytes());
        hash.bytes(self.instrument_id.as_uuid().as_bytes());
        hash.i64(self.source_cutoff.unix_nanos());
        hash.i64(self.market_cutoff.unix_nanos());
        hash.fixed(self.profile_identity.bytes());
        for attempt in &self.attempts {
            hash.u8(method_tag(attempt.method));
            hash.i64(attempt.started_at.unix_nanos());
            hash.i64(attempt.completed_at.unix_nanos());
            hash.u8(match attempt.stage {
                AutomaticValuationStage::SourceSelection => 1,
                AutomaticValuationStage::NativeFiscalSources => 2,
                AutomaticValuationStage::AnnualDiscountSources => 3,
                AutomaticValuationStage::CalculationPublication => 4,
                AutomaticValuationStage::NativeCalculationPublication => 5,
            });
            match &attempt.outcome {
                Ok(result) => {
                    hash.u8(1);
                    hash.fixed(result.measurement_id.bytes());
                    hash.fixed(result.calculation_identity.bytes());
                    hash.fixed(result.input_set_identity.bytes());
                    for amount in [
                        result.range.lower(),
                        result.range.central(),
                        result.range.upper(),
                    ] {
                        hash.bytes(amount.money().currency().as_str().as_bytes());
                        hash.bytes(amount.money().amount().mantissa().to_be_bytes().as_slice());
                        hash.u32(amount.money().amount().scale());
                        hash.u8(amount.scale());
                        hash.u8(match amount.basis() {
                            ValuationAmountBasis::PerInstrumentUnit => 1,
                            ValuationAmountBasis::ReportingEntityTotal => 2,
                            ValuationAmountBasis::PositionTotal => 3,
                            ValuationAmountBasis::TotalCommonEquity => 4,
                        });
                    }
                    match &result.terminal_growth {
                        Some(policy) => {
                            hash.u8(1);
                            hash.fixed(policy.final_explicit_input.bytes());
                            hash.fixed(policy.next_period_input.bytes());
                            hash.fixed(policy.annual_reference_identity.bytes());
                            hash.fixed(policy.annual_rate_identity.bytes());
                            hash.fixed(policy.assumption_identity.bytes());
                            for value in [
                                policy.uncapped_growth,
                                policy.nominal_risk_free_cap,
                                policy.applied_growth,
                            ] {
                                hash.bytes(&value.mantissa().to_be_bytes());
                                hash.u32(value.scale());
                            }
                        }
                        None => hash.u8(0),
                    }
                    match &result.residual_terminal {
                        Some(condition) => {
                            hash.u8(1);
                            hash.fixed(condition.identity().bytes());
                        }
                        None => hash.u8(0),
                    }
                    result.recommendation.hash_into(&mut hash);
                    hash.i64(result.calculated_at.unix_nanos());
                    hash.i64(result.expires_at.unix_nanos());
                }
                Err(error) => {
                    hash.u8(2);
                    hash.u8(failure_tag(*error));
                }
            }
        }
        hash.u64(self.forecast_reads.len() as u64);
        for read in &self.forecast_reads {
            read.hash_into(&mut hash)?;
        }
        match self.selected_measurement_id {
            Some(id) => {
                hash.u8(1);
                hash.fixed(id.bytes());
            }
            None => hash.u8(0),
        }
        hash.i64(self.completed_at.unix_nanos());
        Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finish()))
    }
}

fn failure_tag(error: AutomaticValuationFailure) -> u8 {
    match error {
        AutomaticValuationFailure::InvalidRequest => 1,
        AutomaticValuationFailure::NotFound => 2,
        AutomaticValuationFailure::Unauthorized => 3,
        AutomaticValuationFailure::ResourceExhausted => 4,
        AutomaticValuationFailure::Unavailable => 5,
        AutomaticValuationFailure::InvalidResult => 6,
        AutomaticValuationFailure::Internal => 7,
    }
}
