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
        if request
            .financial_measurement
            .is_some_and(|measurement| measurement != output_binding.measurement())
            || (request.financial_target.is_some()
                != matches!(
                    output_binding.measurement(),
                    ForecastMeasurement::FinancialAmount { .. }
                ))
        {
            return Err(ForecastError::InvalidOutputBinding);
        }
        let calibration_cutoff = request
            .financial_decision
            .cloned()
            .map_or_else(|| request.observed_coordinate(), Ok)?;
        let probability =
            output_binding.output_semantics() == ModelOutputSemantics::BinaryProbability;
        if probability
            && (calibration.is_some()
                || !request.observed_history.is_empty()
                || metadata.probability_calibration().is_none_or(|proof| {
                    calibration_cutoff
                        .exact_timestamp()
                        .is_none_or(|cutoff| !proof.evaluation_window().ends_by(cutoff))
                }))
        {
            return Err(ForecastError::InvalidCalibration);
        }
        let price_bound = matches!(
            output_binding.measurement(),
            ForecastMeasurement::Price { .. }
        );
        if calibration.is_some_and(|value| !value.matches_coordinate(metadata, &calibration_cutoff))
        {
            return Err(ForecastError::CalibrationIdentityMismatch);
        }
        let mut points = Vec::new();
        points
            .try_reserve_exact(request.inputs.len())
            .map_err(|_| ForecastError::Capacity)?;
        for (index, input) in request.inputs.iter().enumerate() {
            let output = self.infer(input)?;
            if probability
                && (!output.score().is_finite() || !(0.0..=1.0).contains(&output.score()))
            {
                return Err(ForecastError::InvalidDecimal);
            }
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
                target_at: request
                    .observed_cutoff
                    .map(|cutoff| request.horizon.target_at(cutoff, index))
                    .transpose()?,
                financial_target: request.financial_target.map(|target| {
                    super::contracts::ForecastFinancialTarget {
                        ordinal: target.target_ordinal(),
                        period: target.target_period(),
                    }
                }),
                central,
                intervals,
            });
        }
        Ok(ForecastPath {
            instrument_id: request.instrument_id,
            observed_cutoff: request.observed_cutoff,
            financial_target: request.financial_target.cloned().map(Box::new),
            calibration_cutoff,
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
            probability_calibration: metadata.probability_calibration().cloned(),
            quality: DataQuality::Modeled,
            limitations: metadata.limitations().into(),
            fallback_reason: metadata.fallback_reason().into(),
        })
    }
}

impl<T> ResearchForecastBackend for T where T: InferenceBackend + ?Sized {}
