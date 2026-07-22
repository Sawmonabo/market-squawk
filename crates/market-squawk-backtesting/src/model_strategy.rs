//! Research adapter for the same immutable model inference contract used by execution.

use std::fmt;

use market_squawk_execution::{BoundedOrderIntents, StrategyError};
use market_squawk_modeling::{
    InferenceBackend, ModelFailure, ModelFeatureValue, ModelInput, ModelOutput,
};

use crate::{BacktestContext, BacktestStrategy};

/// Strategy-owned mapping from one successful model output to bounded typed intents.
pub trait BacktestModelDecisionMapper: Send + fmt::Debug {
    /// Maps a successful identity-bound model result for the current PIT observation.
    fn map(
        &mut self,
        context: &BacktestContext<'_>,
        output: &ModelOutput,
    ) -> Result<BoundedOrderIntents, StrategyError>;
}

/// Research strategy adapter that converts every typed model failure into audited no-action.
pub struct BacktestModelStrategy {
    backend: Result<Box<dyn InferenceBackend>, ModelFailure>,
    mapper: Box<dyn BacktestModelDecisionMapper>,
}

impl BacktestModelStrategy {
    /// Owns either one admitted immutable backend generation or its typed admission failure.
    #[must_use]
    pub fn new(
        backend: Result<Box<dyn InferenceBackend>, ModelFailure>,
        mapper: Box<dyn BacktestModelDecisionMapper>,
    ) -> Self {
        Self { backend, mapper }
    }
}

impl fmt::Debug for BacktestModelStrategy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BacktestModelStrategy")
            .field(
                "backend",
                &match self.backend {
                    Ok(_) => "[ADMITTED INFERENCE BACKEND]",
                    Err(_) => "[TYPED MODEL FAILURE]",
                },
            )
            .field("mapper", &self.mapper)
            .finish()
    }
}

impl BacktestStrategy for BacktestModelStrategy {
    fn on_observation(
        &mut self,
        context: &BacktestContext<'_>,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        let backend = match &self.backend {
            Ok(backend) => backend,
            Err(failure) => return Ok(BoundedOrderIntents::from_model_failure(*failure)),
        };
        let metadata = backend.metadata();
        let mut values = Vec::new();
        values
            .try_reserve_exact(metadata.features().len())
            .map_err(|_| StrategyError::RetainedSize)?;
        for binding in metadata.features() {
            let Some(value) =
                context.versioned_feature(binding.key().name(), binding.key().version().get())
            else {
                return Ok(BoundedOrderIntents::from_model_failure(
                    market_squawk_modeling::ModelInputError::FeatureUnavailable.into(),
                ));
            };
            let mut feature = ModelFeatureValue::from_binding(binding);
            if let Err(error) = feature.try_set_value(value) {
                return Ok(BoundedOrderIntents::from_model_failure(error.into()));
            }
            values.push(feature);
        }
        let input = match ModelInput::try_new(metadata, &values) {
            Ok(input) => input,
            Err(error) => return Ok(BoundedOrderIntents::from_model_failure(error.into())),
        };
        let output = match backend.infer(&input) {
            Ok(output) => output,
            Err(error) => return Ok(BoundedOrderIntents::from_model_failure(error.into())),
        };
        self.mapper.map(context, &output)
    }
}
