//! Native fiscal hindcasts over the same source-owned epochs and admitted runtime as price studies.
//!
//! No current forecast vintage, caller monetary value or invented fiscal timestamp is admitted.

use super::*;
use market_squawk_data::FeatureLabelMeasurement;
use market_squawk_modeling::ForecastTargetMeaning;

/// Actual fit-only native financial inference. This carries study evidence and no live authority.
#[derive(Clone, Debug)]
pub(crate) struct HistoricalFinancialForecast {
    distribution: ForecastStudyDistribution,
    runtime_generation: Sha256Digest,
    runtime_selected_at: Timestamp,
    runtime_reopened_at: Option<Timestamp>,
    calculated_at: Timestamp,
}

impl SelectedForecastRuntime {
    /// Reuses the source-owned fiscal feature extraction, native request and admitted backend.
    /// The shared distribution producer verifies original population/source snapshot identity,
    /// native fiscal target and strict training/calibration-before-scoring chronology.
    pub(crate) fn forecast_financial_coordinate(
        &self,
        coordinate: FeatureDatasetInputCoordinate<'_>,
        context: &RequestContext,
    ) -> Result<HistoricalFinancialForecast, ServiceError> {
        ensure_live(context)?;
        let epoch = coordinate.epoch();
        let period = epoch.financial_period().ok_or(ServiceError::Unavailable)?;
        let FeatureLabelMeasurement::FinancialAmount {
            currency,
            role,
            basis,
            share_convention,
        } = epoch
            .financial_measurement()
            .ok_or(ServiceError::Unavailable)?
        else {
            return Err(ServiceError::Unavailable);
        };
        let metadata = self.metadata();
        let distance = period
            .target_ordinal()
            .checked_sub(period.observed_ordinal())
            .and_then(|value| u16::try_from(value).ok())
            .and_then(NonZeroU16::new)
            .ok_or(ServiceError::InvalidResult)?;
        if epoch.purpose() != DatasetBuildPurpose::StudyInputs
            || epoch.target_origin().is_some()
            || epoch.target_at().is_some()
            || epoch.market_bar().is_some()
            || metadata.output_binding().measurement()
                != (ForecastMeasurement::FinancialAmount {
                    currency,
                    role,
                    basis,
                    share_convention,
                })
            || metadata.output_binding().target()
                != (ForecastTargetMeaning::FinancialPeriod {
                    cadence: period.cadence(),
                    periods_ahead: distance,
                })
        {
            return Err(ServiceError::Unavailable);
        }
        let raw = super::super::financial_feature_values(metadata, coordinate)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(raw.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for (binding, number) in metadata.features().iter().zip(raw) {
            ensure_live(context)?;
            let mut value = ModelFeatureValue::from_binding(binding);
            value
                .try_set_value(number)
                .map_err(|_| ServiceError::InvalidResult)?;
            values.push(value);
        }
        let input =
            ModelInput::try_new(metadata, &values).map_err(|_| ServiceError::InvalidResult)?;
        let inputs = [input];
        let request = ForecastRequest::try_for_financial_coordinate(coordinate, 12, &inputs)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let fit = metadata
            .forecast_calibration()
            .ok_or(ServiceError::Unavailable)?;
        let calibration = CalibrationEvidence::try_new(
            metadata,
            fit.method(),
            fit.window(),
            fit.policy_hash(),
            fit.residuals_hash(),
            *fit.bands(),
            fit.dependence_assumptions(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let path = self.retained.image.backends[self.backend_ordinal]
            .forecast(&request, Some(&calibration))
            .map_err(|_| ServiceError::Unavailable)?;
        ensure_live(context)?;
        let bundle = self
            .retained
            .image
            .registry
            .get(metadata.bundle_id(), metadata.bundle_version())
            .map_err(|_| ServiceError::Unavailable)?
            .ok_or(ServiceError::Unavailable)?;
        let distribution =
            ForecastStudyDistribution::try_from_admitted_path(&bundle, path, coordinate)
                .map_err(|_| ServiceError::InvalidResult)?;
        let calculated_at = wall_now()?;
        if calculated_at < epoch.calculated_at()
            || calculated_at < epoch.snapshot_as_of()
            || calculated_at < self.selected_at()
            || self
                .reopened_at()
                .is_some_and(|time| time < self.selected_at() || time > calculated_at)
        {
            return Err(ServiceError::Unavailable);
        }
        Ok(HistoricalFinancialForecast {
            distribution,
            runtime_generation: self.runtime_generation(),
            runtime_selected_at: self.selected_at(),
            runtime_reopened_at: self.reopened_at(),
            calculated_at,
        })
    }
}

impl HistoricalFinancialForecast {
    pub(crate) const fn native_distribution(&self) -> &ForecastStudyDistribution {
        &self.distribution
    }
    pub(crate) const fn epoch(&self) -> &FeatureDatasetInputEpoch {
        self.distribution.epoch()
    }
    pub(crate) const fn runtime_generation(&self) -> Sha256Digest {
        self.runtime_generation
    }
    pub(crate) const fn runtime_selected_at(&self) -> Timestamp {
        self.runtime_selected_at
    }
    pub(crate) const fn runtime_reopened_at(&self) -> Option<Timestamp> {
        self.runtime_reopened_at
    }
    pub(crate) const fn calculated_at(&self) -> Timestamp {
        self.calculated_at
    }
}
