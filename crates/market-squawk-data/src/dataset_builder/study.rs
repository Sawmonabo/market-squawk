//! Explicit study clocks and qualification inside the existing dataset authority.

use super::DatasetBuildError;
use market_squawk_domain::{
    FundamentalCadence, HistoricalStudyBasis, HistoricalStudyLimitation,
    ResearchTemporalCoordinate, Timestamp,
};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU16;
use std::time::Duration;

/// Whether a qualified publication contains mature labels or only predeclared scoring inputs.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetBuildPurpose {
    Training,
    StudyInputs,
}

/// Exact elapsed time and native reporting ordinals have independent semantics.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DatasetTargetHorizon {
    ExactElapsed(Duration),
    FiscalPeriods {
        cadence: FundamentalCadence,
        periods_ahead: NonZeroU16,
    },
}
impl DatasetTargetHorizon {
    pub const fn exact_elapsed(self) -> Option<Duration> {
        match self {
            Self::ExactElapsed(value) => Some(value),
            Self::FiscalPeriods { .. } => None,
        }
    }
    pub(crate) fn validate(self) -> Result<(), DatasetBuildError> {
        match self {
            Self::ExactElapsed(value)
                if !value.is_zero() && value.as_nanos() <= i64::MAX as u128 =>
            {
                Ok(())
            }
            Self::FiscalPeriods {
                cadence: FundamentalCadence::Annual | FundamentalCadence::Quarterly,
                ..
            } => Ok(()),
            _ => Err(DatasetBuildError::InvalidRequest),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum TargetHorizonWire {
    ExactElapsed {
        nanos: u64,
    },
    FiscalPeriods {
        cadence: FundamentalCadence,
        periods_ahead: NonZeroU16,
    },
}
impl TargetHorizonWire {
    pub(crate) fn new(value: DatasetTargetHorizon) -> Self {
        match value {
            DatasetTargetHorizon::ExactElapsed(value) => Self::ExactElapsed {
                nanos: value.as_nanos() as u64,
            },
            DatasetTargetHorizon::FiscalPeriods {
                cadence,
                periods_ahead,
            } => Self::FiscalPeriods {
                cadence,
                periods_ahead,
            },
        }
    }
    pub(crate) fn decode(self) -> Result<DatasetTargetHorizon, DatasetBuildError> {
        let value = match self {
            Self::ExactElapsed { nanos } => {
                DatasetTargetHorizon::ExactElapsed(Duration::from_nanos(nanos))
            }
            Self::FiscalPeriods {
                cadence,
                periods_ahead,
            } => DatasetTargetHorizon::FiscalPeriods {
                cadence,
                periods_ahead,
            },
        };
        value.validate()?;
        Ok(value)
    }
}

/// A requested clock policy. Only the existing producer can admit it with authentic sources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DatasetStudyPolicy {
    basis: HistoricalStudyBasis,
    purpose: DatasetBuildPurpose,
    snapshot_as_of: Timestamp,
    decision_lag: Option<Duration>,
    target_horizon: DatasetTargetHorizon,
}

impl DatasetStudyPolicy {
    pub fn try_new(
        basis: HistoricalStudyBasis,
        purpose: DatasetBuildPurpose,
        snapshot_as_of: Timestamp,
        decision_lag: Option<Duration>,
        target_horizon: DatasetTargetHorizon,
    ) -> Result<Self, DatasetBuildError> {
        target_horizon.validate()?;
        let invalid_lag = match basis {
            HistoricalStudyBasis::HistoricalAsKnown => decision_lag.is_some(),
            HistoricalStudyBasis::RetrospectiveFrozenSnapshot => match target_horizon {
                DatasetTargetHorizon::ExactElapsed(horizon) => {
                    decision_lag.is_none_or(|lag| lag >= horizon)
                }
                DatasetTargetHorizon::FiscalPeriods { .. } => decision_lag != Some(Duration::ZERO),
            },
        };
        if invalid_lag {
            return Err(DatasetBuildError::InvalidRequest);
        }
        Ok(Self {
            basis,
            purpose,
            snapshot_as_of,
            decision_lag,
            target_horizon,
        })
    }
    pub const fn basis(&self) -> HistoricalStudyBasis {
        self.basis
    }
    pub const fn purpose(&self) -> DatasetBuildPurpose {
        self.purpose
    }
    pub const fn snapshot_as_of(&self) -> Timestamp {
        self.snapshot_as_of
    }
    pub const fn decision_lag(&self) -> Option<Duration> {
        self.decision_lag
    }
    pub const fn target_horizon(&self) -> DatasetTargetHorizon {
        self.target_horizon
    }
    pub const fn limitations(&self) -> &'static [HistoricalStudyLimitation] {
        limitations(self.basis)
    }
    pub(super) fn chronological_at(
        &self,
        example: &super::DatasetExample,
    ) -> ResearchTemporalCoordinate {
        match self.basis {
            HistoricalStudyBasis::HistoricalAsKnown => {
                ResearchTemporalCoordinate::exact(example.source_selection_as_of())
            }
            HistoricalStudyBasis::RetrospectiveFrozenSnapshot => {
                example.decision_coordinate().clone()
            }
        }
    }
    pub(super) fn validate_example(
        &self,
        example: &super::DatasetExample,
    ) -> Result<(), DatasetBuildError> {
        if example.source_selection_as_of() > self.snapshot_as_of {
            return Err(DatasetBuildError::TemporalLeakage);
        }
        match self.target_horizon {
            DatasetTargetHorizon::ExactElapsed(horizon) => {
                let origin = example
                    .exact_target_coordinates()
                    .map(|value| value.0)
                    .ok_or(DatasetBuildError::InvalidRequest)?;
                if example.nominal_daily_source().is_some()
                    && self.basis != HistoricalStudyBasis::RetrospectiveFrozenSnapshot
                    && !(self.basis == HistoricalStudyBasis::HistoricalAsKnown
                        && self.purpose == DatasetBuildPurpose::StudyInputs
                        && example.source_selection_as_of() == self.snapshot_as_of
                        && example.decision_at() == Some(self.snapshot_as_of))
                {
                    return Err(DatasetBuildError::TemporalLeakage);
                }
                let target = example
                    .label_effective_cutoff()
                    .and_then(ResearchTemporalCoordinate::exact_timestamp)
                    .ok_or(DatasetBuildError::InvalidRequest)?;
                let decision = example
                    .decision_at()
                    .ok_or(DatasetBuildError::InvalidRequest)?;
                if example.financial_source().is_some()
                    || origin.unix_nanos().checked_add(horizon.as_nanos() as i64)
                        != Some(target.unix_nanos())
                    || decision < origin
                    || decision >= target
                {
                    return Err(DatasetBuildError::TemporalLeakage);
                }
                if self.basis == HistoricalStudyBasis::RetrospectiveFrozenSnapshot
                    && origin.unix_nanos().checked_add(
                        self.decision_lag
                            .ok_or(DatasetBuildError::InvalidRequest)?
                            .as_nanos() as i64,
                    ) != Some(decision.unix_nanos())
                {
                    return Err(DatasetBuildError::TemporalLeakage);
                }
            }
            DatasetTargetHorizon::FiscalPeriods { .. } => {
                let financial = example
                    .financial_source()
                    .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
                let binding = &financial.binding;
                binding.validate()?;
                if binding.target_horizon()? != self.target_horizon
                    || example.effective_cutoff().calendar_date_value()
                        != Some(binding.observed_period().end())
                    || example
                        .label_effective_cutoff()
                        .and_then(ResearchTemporalCoordinate::calendar_date_value)
                        != binding.target_period().map(|v| v.end())
                    || (self.basis == HistoricalStudyBasis::RetrospectiveFrozenSnapshot
                        && example.decision_coordinate().calendar_date_value()
                            != Some(binding.observed_period().end()))
                {
                    return Err(DatasetBuildError::TemporalLeakage);
                }
                if self.basis == HistoricalStudyBasis::HistoricalAsKnown {
                    let decision = example
                        .decision_at()
                        .ok_or(DatasetBuildError::InvalidRequest)?;
                    if decision
                        .utc_calendar_date()
                        .map_err(|_| DatasetBuildError::TemporalLeakage)?
                        < binding.observed_period().end()
                        || binding.target_period().is_some_and(|p| {
                            decision.utc_calendar_date().is_ok_and(|d| d >= p.end())
                        })
                    {
                        return Err(DatasetBuildError::TemporalLeakage);
                    }
                }
            }
        }
        match self.basis {
            HistoricalStudyBasis::HistoricalAsKnown => {
                if example
                    .decision_at()
                    .is_none_or(|decision| example.source_selection_as_of() > decision)
                    || example
                        .label_selection_as_of()
                        .is_some_and(|label| label <= example.source_selection_as_of())
                {
                    return Err(DatasetBuildError::TemporalLeakage);
                }
            }
            HistoricalStudyBasis::RetrospectiveFrozenSnapshot => {
                if example.source_selection_as_of() != self.snapshot_as_of
                    || example
                        .label_selection_as_of()
                        .is_some_and(|label| label != self.snapshot_as_of)
                {
                    return Err(DatasetBuildError::TemporalLeakage);
                }
            }
        }
        let mut labels = example
            .components()
            .iter()
            .filter(|value| value.spec().kind() == super::ComponentKind::Label);
        match self.purpose {
            DatasetBuildPurpose::Training => {
                let known = example
                    .label_selection_as_of()
                    .ok_or(DatasetBuildError::InvalidRequest)?;
                let target = example
                    .label_effective_cutoff()
                    .ok_or(DatasetBuildError::TemporalLeakage)?;
                let mature = if let Some(target) = target.exact_timestamp() {
                    target <= known
                } else if let Some(target) = target.calendar_date_value() {
                    known.utc_calendar_date().is_ok_and(|d| target <= d)
                } else {
                    false
                };
                if known > self.snapshot_as_of
                    || !mature
                    || labels.clone().next().is_none()
                    || labels.any(|value| value.value().is_missing())
                {
                    return Err(DatasetBuildError::TemporalLeakage);
                }
            }
            DatasetBuildPurpose::StudyInputs => {
                if example.label_selection_as_of().is_some() || labels.count() != 0 {
                    return Err(DatasetBuildError::InvalidRequest);
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StudyWire {
    pub(crate) basis: HistoricalStudyBasis,
    pub(crate) purpose: DatasetBuildPurpose,
    pub(crate) snapshot_as_of_unix_nanos: i64,
    pub(crate) decision_lag_nanos: Option<u64>,
    pub(crate) target_horizon: TargetHorizonWire,
    pub(crate) limitations: Vec<HistoricalStudyLimitation>,
    pub(crate) source_snapshot_sha256: String,
}
impl StudyWire {
    pub(crate) fn new(policy: DatasetStudyPolicy, snapshot: crate::Sha256Digest) -> Self {
        Self {
            basis: policy.basis,
            purpose: policy.purpose,
            snapshot_as_of_unix_nanos: policy.snapshot_as_of.unix_nanos(),
            decision_lag_nanos: policy.decision_lag.map(|lag| lag.as_nanos() as u64),
            target_horizon: TargetHorizonWire::new(policy.target_horizon),
            limitations: policy.limitations().to_vec(),
            source_snapshot_sha256: crate::schema::encode_hex(snapshot.bytes()),
        }
    }
    pub(crate) fn decode(
        &self,
    ) -> Result<(DatasetStudyPolicy, crate::Sha256Digest), DatasetBuildError> {
        let policy = DatasetStudyPolicy::try_new(
            self.basis,
            self.purpose,
            Timestamp::from_unix_nanos(self.snapshot_as_of_unix_nanos),
            self.decision_lag_nanos.map(Duration::from_nanos),
            self.target_horizon.decode()?,
        )?;
        if self.limitations != policy.limitations() {
            return Err(DatasetBuildError::InvalidRequest);
        }
        let snapshot = crate::schema::decode_hex(&self.source_snapshot_sha256)
            .filter(|value| *value != [0; 32])
            .ok_or(DatasetBuildError::InvalidRequest)?;
        Ok((policy, crate::Sha256Digest::new(snapshot)))
    }
}

pub(crate) fn source_snapshot_from_parents(
    snapshot_as_of: Timestamp,
    parents: &[crate::DatasetManifestRef],
) -> crate::Sha256Digest {
    super::canonical::source_snapshot_from_parents(snapshot_as_of, parents)
}

pub(crate) const fn limitations(
    basis: HistoricalStudyBasis,
) -> &'static [HistoricalStudyLimitation] {
    use HistoricalStudyLimitation::{
        HistoricalRevisionCoverageUnproven, LaterVintageInputs, PresentDayFixedCohort,
        SimulatedAvailability,
    };
    match basis {
        HistoricalStudyBasis::HistoricalAsKnown => &[PresentDayFixedCohort],
        HistoricalStudyBasis::RetrospectiveFrozenSnapshot => &[
            HistoricalRevisionCoverageUnproven,
            LaterVintageInputs,
            PresentDayFixedCohort,
            SimulatedAvailability,
        ],
    }
}
