//! Original binary outcomes retained during the existing single-pass receipt verification.

use super::descriptor::Descriptor;
use super::{
    PythonDatasetCatalogError as Error, PythonDatasetRow, PythonDatasetValue,
    PythonDatasetVerificationLimits,
};
use crate::{
    ComponentKind, DatasetSplit, FeatureLabelComponentSpec, ProbabilityEventTarget, Sha256Digest,
};
use market_squawk_domain::{HistoricalStudyBasis, InstrumentId, Timestamp};
use rust_decimal::Decimal;
use std::mem::size_of;

/// One complete original feature/label example. There is no caller constructor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbabilityLabelObservation {
    example_id: Box<str>,
    instrument_id: InstrumentId,
    split: DatasetSplit,
    origin: Timestamp,
    decision: Timestamp,
    partition_origin: Timestamp,
    label_maturity: Timestamp,
    target_at: Timestamp,
    value: bool,
    label_lineage: Sha256Digest,
    features: Box<[Decimal]>,
}
impl ProbabilityLabelObservation {
    pub fn example_id(&self) -> &str {
        &self.example_id
    }
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub const fn split(&self) -> DatasetSplit {
        self.split
    }
    pub const fn origin(&self) -> Timestamp {
        self.origin
    }
    pub const fn decision(&self) -> Timestamp {
        self.decision
    }
    pub const fn partition_origin(&self) -> Timestamp {
        self.partition_origin
    }
    pub const fn label_maturity(&self) -> Timestamp {
        self.label_maturity
    }
    pub const fn target_at(&self) -> Timestamp {
        self.target_at
    }
    pub const fn value(&self) -> bool {
        self.value
    }
    pub const fn label_lineage(&self) -> Sha256Digest {
        self.label_lineage
    }
    pub fn features(&self) -> &[Decimal] {
        &self.features
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProbabilityObservedDataset {
    pub(super) label: FeatureLabelComponentSpec,
    pub(super) feature_specs: Box<[FeatureLabelComponentSpec]>,
    pub(super) observations: Box<[ProbabilityLabelObservation]>,
}

pub(super) struct ProbabilityObservationCollector {
    label: FeatureLabelComponentSpec,
    event: ProbabilityEventTarget,
    feature_specs: Box<[FeatureLabelComponentSpec]>,
    retrospective: bool,
    split_policy: crate::ChronologicalSplitPolicy,
    observations: Vec<ProbabilityLabelObservation>,
    maximum_observations: usize,
    retained_admission: usize,
    current_key: Option<(Box<str>, [u8; 16])>,
    features: Vec<Decimal>,
    complete: bool,
}

impl ProbabilityObservationCollector {
    pub(super) fn new(
        descriptor: &Descriptor,
        limits: PythonDatasetVerificationLimits,
    ) -> Result<Option<Self>, Error> {
        let mut event_label = None;
        for component in &descriptor.components {
            if let Some(event) = component.probability_event_target()? {
                if event_label.replace((component.spec()?, event)).is_some() {
                    return Err(Error::CorruptAdmission);
                }
            }
        }
        let Some((label, event)) = event_label else {
            return Ok(None);
        };
        let mut feature_specs = Vec::new();
        feature_specs
            .try_reserve_exact(descriptor.components.len())
            .map_err(|_| Error::LimitExceeded)?;
        for component in &descriptor.components {
            let spec = component.spec()?;
            if spec.kind() == ComponentKind::Feature {
                feature_specs.push(spec);
            }
        }
        if feature_specs.is_empty() {
            return Err(Error::CorruptAdmission);
        }
        let maximum_observations = descriptor
            .split_counts
            .train
            .checked_add(descriptor.split_counts.validation)
            .and_then(|value| value.checked_add(descriptor.split_counts.test))
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| *value <= limits.max_rows())
            .ok_or(Error::LimitExceeded)?;
        // All rows are already bounded. Pre-admit complete vectors and maximum original IDs before
        // reserving; subsequent object verification includes these bytes in its aggregate budget.
        let per_example = size_of::<ProbabilityLabelObservation>()
            .checked_add(256)
            .and_then(|value| {
                value.checked_add(feature_specs.len().checked_mul(size_of::<Decimal>())?)
            })
            .ok_or(Error::LimitExceeded)?;
        let retained_admission = per_example
            .checked_mul(maximum_observations)
            .and_then(|value| {
                value.checked_add(
                    feature_specs
                        .len()
                        .checked_mul(size_of::<FeatureLabelComponentSpec>() + 256)?,
                )
            })
            .and_then(|value| value.checked_add(per_example))
            .filter(|value| *value <= limits.max_bytes())
            .ok_or(Error::LimitExceeded)?;
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(maximum_observations)
            .map_err(|_| Error::LimitExceeded)?;
        let mut features = Vec::new();
        features
            .try_reserve_exact(feature_specs.len())
            .map_err(|_| Error::LimitExceeded)?;
        let study = descriptor
            .study
            .as_ref()
            .ok_or(Error::CorruptAdmission)?
            .decode()
            .map_err(|_| Error::CorruptAdmission)?
            .0;
        if study.purpose() != crate::DatasetBuildPurpose::Training {
            return Err(Error::CorruptAdmission);
        }
        Ok(Some(Self {
            label,
            event,
            feature_specs: feature_specs.into_boxed_slice(),
            retrospective: study.basis() == HistoricalStudyBasis::RetrospectiveFrozenSnapshot,
            split_policy: descriptor.split_policy.decode()?,
            observations,
            maximum_observations,
            retained_admission,
            current_key: None,
            features,
            complete: true,
        }))
    }

    pub(super) const fn retained_admission(&self) -> usize {
        self.retained_admission
    }

    pub(super) fn update(&mut self, row: &PythonDatasetRow, selected: bool) -> Result<(), Error> {
        let same = self
            .current_key
            .as_ref()
            .is_some_and(|(example, instrument)| {
                example.as_ref() == row.example_id.as_ref() && *instrument == row.instrument_id
            });
        if !same {
            if row.example_id.len() > 256 {
                return Err(Error::LimitExceeded);
            }
            self.current_key = Some((row.example_id.clone(), row.instrument_id));
            self.features.clear();
            self.complete = true;
        }
        if row.component_kind == 1 {
            let expected = self
                .feature_specs
                .get(self.features.len())
                .ok_or(Error::CorruptAdmission)?;
            if expected.name() != row.component_name.as_ref()
                || expected.version().get() != row.component_version
            {
                return Err(Error::CorruptAdmission);
            }
            let value = match row.value {
                PythonDatasetValue::Decimal { mantissa, scale } => {
                    Decimal::try_from_i128_with_scale(mantissa, u32::from(scale))
                        .map_err(|_| Error::CorruptAdmission)?
                }
                PythonDatasetValue::Missing(_) => {
                    self.complete = false;
                    Decimal::ZERO
                }
                PythonDatasetValue::Float(_) => return Err(Error::CorruptAdmission),
            };
            self.complete &= selected;
            self.features.push(value);
            return Ok(());
        }
        if row.component_kind != 2
            || row.component_name.as_ref() != self.label.name()
            || row.component_version != self.label.version().get()
        {
            return Err(Error::CorruptAdmission);
        }
        if self.features.len() != self.feature_specs.len() {
            return Err(Error::CorruptAdmission);
        }
        let binary = match row.value {
            PythonDatasetValue::Decimal { mantissa, scale } => {
                let value = Decimal::try_from_i128_with_scale(mantissa, u32::from(scale))
                    .map_err(|_| Error::CorruptAdmission)?;
                if value != Decimal::ZERO && value != Decimal::ONE {
                    return Err(Error::CorruptAdmission);
                }
                Some(value == Decimal::ONE)
            }
            PythonDatasetValue::Missing(_) => None,
            PythonDatasetValue::Float(_) => return Err(Error::CorruptAdmission),
        };
        if !selected || !self.complete || binary.is_none() {
            return Ok(());
        }
        let origin = row.observed_effective_at.ok_or(Error::CorruptAdmission)?;
        let target_at = row.label_effective_at.ok_or(Error::CorruptAdmission)?;
        let decision = row
            .decision_coordinate
            .exact_timestamp()
            .ok_or(Error::CorruptAdmission)?;
        let split = match row.split {
            1 => DatasetSplit::Train,
            2 => DatasetSplit::Validation,
            3 => DatasetSplit::Test,
            _ => return Err(Error::CorruptAdmission),
        };
        let window_end = match self.event {
            ProbabilityEventTarget::ProfitAfterCosts { policy } => target_at
                .checked_add_nanos(policy.maximum_exit_lag_nanos)
                .map_err(|_| Error::CorruptAdmission)?,
            _ => target_at,
        };
        let label_maturity = if self.retrospective {
            window_end
        } else {
            row.label_selection_as_of
                .ok_or(Error::CorruptAdmission)?
                .max(window_end)
        };
        if self
            .split_policy
            .split_end(split)
            .exact_timestamp()
            .is_none_or(|end| label_maturity > end)
            || self.observations.len() >= self.maximum_observations
        {
            return Err(Error::CorruptAdmission);
        }
        let instrument_id = InstrumentId::try_from(uuid::Uuid::from_bytes(row.instrument_id))
            .map_err(|_| Error::CorruptAdmission)?;
        let mut features = Vec::new();
        features
            .try_reserve_exact(self.features.len())
            .map_err(|_| Error::LimitExceeded)?;
        features.extend_from_slice(&self.features);
        self.observations.push(ProbabilityLabelObservation {
            example_id: row.example_id.clone(),
            instrument_id,
            split,
            origin,
            decision,
            label_maturity,
            partition_origin: if self.retrospective {
                decision
            } else {
                row.source_selection_as_of
            },
            target_at,
            value: binary.ok_or(Error::CorruptAdmission)?,
            label_lineage: Sha256Digest::new(row.lineage),
            features: features.into_boxed_slice(),
        });
        Ok(())
    }

    pub(super) fn finish(mut self) -> ProbabilityObservedDataset {
        self.observations.sort_unstable_by(|left, right| {
            (
                left.partition_origin,
                left.instrument_id,
                left.example_id.as_ref(),
            )
                .cmp(&(
                    right.partition_origin,
                    right.instrument_id,
                    right.example_id.as_ref(),
                ))
        });
        ProbabilityObservedDataset {
            label: self.label,
            feature_specs: self.feature_specs,
            observations: self.observations.into_boxed_slice(),
        }
    }
}
