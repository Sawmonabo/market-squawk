//! Research-only multi-horizon evaluation over the unchanged scalar inference contract.

use super::*;

/// Separate research forecasting contract implemented over an admitted scalar backend.
pub trait ResearchForecastBackend: InferenceBackend {
    /// Evaluates one exact input for every horizon and attaches only admitted intervals.
    ///
    /// Live [`InferenceBackend::infer`] behavior is unchanged. Failure returns no partial path.
    fn forecast(
        &self,
        request: &ForecastRequest<'_>,
        calibration: Option<&CalibrationEvidence>,
    ) -> Result<ForecastPath, ForecastError> {
        let metadata = self.metadata();
        let output_binding = metadata.output_binding();
        if !output_binding.admits_path_horizon(request.horizon) {
            return Err(ForecastError::InvalidHorizon);
        }
        let price_bound = matches!(
            output_binding.measurement(),
            ForecastMeasurement::Price { .. }
        );
        if calibration.is_some_and(|value| !value.matches(metadata, request.observed_cutoff)) {
            return Err(ForecastError::CalibrationIdentityMismatch);
        }
        let mut points = Vec::new();
        points
            .try_reserve_exact(request.inputs.len())
            .map_err(|_| ForecastError::Capacity)?;
        for (index, input) in request.inputs.iter().enumerate() {
            let output = self.infer(input)?;
            let central = ForecastValue::try_from_f64(output.score(), request.decimal_scale)?;
            if price_bound && central.mantissa() <= 0 {
                return Err(ForecastError::InvalidDecimal);
            }
            let intervals = calibration
                .map(|evidence| ForecastIntervals::from_calibration(central, evidence))
                .transpose()?;
            if price_bound
                && intervals.is_some_and(|value| value.interval_95().lower().mantissa() <= 0)
            {
                return Err(ForecastError::InvalidInterval);
            }
            points.push(ForecastPoint {
                target_at: request.horizon.target_at(request.observed_cutoff, index)?,
                central,
                intervals,
            });
        }
        Ok(ForecastPath {
            instrument_id: request.instrument_id,
            observed_cutoff: request.observed_cutoff,
            available_at: request.available_at,
            horizon: request.horizon,
            observed_history: request.observed_history.into(),
            points: points.into_boxed_slice(),
            model_id: metadata.model_id(),
            bundle_id: metadata.bundle_id().clone(),
            bundle_version: metadata.bundle_version(),
            metadata_hash: metadata.metadata_hash(),
            artifact_hash: metadata.artifact_hash(),
            training_run_hash: metadata.training_run_hash(),
            output_binding: output_binding.clone(),
            dataset: metadata.dataset().clone(),
            universe_id: metadata.universe_id().clone(),
            training_period: metadata.training_period(),
            feature_semantic_digests: metadata.feature_semantic_digests().into(),
            calibration: calibration.cloned(),
            quality: DataQuality::Modeled,
            limitations: metadata.limitations().into(),
            fallback_reason: metadata.fallback_reason().into(),
        })
    }
}

impl<T> ResearchForecastBackend for T where T: InferenceBackend + ?Sized {}
