//! Research-only multi-horizon forecasts, calibration, vintages, and outcome evidence.
//!
//! Live [`crate::InferenceBackend::infer`] and [`crate::ModelOutput`] remain scalar decision
//! contracts. This module owns a distinct bounded research path and never promotes modeled values
//! to direct market evidence.

use std::{
    cmp::Ordering,
    mem::size_of,
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
};

use market_squawk_analytics::FeatureSemanticDigest;
use market_squawk_data::{
    ComponentKind, ComponentScope, CorporateActionSensitivity, FeatureLabelComponentSpec,
    Sha256Digest, UniverseId,
};
use market_squawk_domain::{Currency, DataQuality, InstrumentId, ModelId, Timestamp};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    BundleId, InferenceBackend, InferenceError, ModelInput, ModelMetadata, ModelOutputSemantics,
    TrainingDatasetIdentity, TrainingPeriod,
};

mod calibration;
mod contracts;
mod engine;
mod evidence;

pub use calibration::{
    CalibrationBand, CalibrationEvidence, CalibrationMethod, CalibrationWindow, ForecastCoverage,
    RealizedCoverage,
};
pub use contracts::{
    ForecastCentralStatistic, ForecastError, ForecastEstimatorProfile, ForecastHorizon,
    ForecastInterval, ForecastIntervals, ForecastMeasurement, ForecastObservedPoint,
    ForecastOutputBinding, ForecastPath, ForecastPoint, ForecastRequest, ForecastTargetMeaning,
    ForecastTrainingObjective, ForecastTransform, ForecastValue, MAX_FORECAST_DECIMAL_SCALE,
    MAX_FORECAST_OBSERVED_POINTS, MAX_FORECAST_POINTS,
};
pub use engine::ResearchForecastBackend;
pub use evidence::{
    ForecastOutcome, ForecastOutcomeId, ForecastVintage, ForecastVintageId,
    verify_forecast_vintage_identity,
};
