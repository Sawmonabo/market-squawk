use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr;
use std::sync::Arc;

use market_squawk_data::Sha256Digest;
use market_squawk_domain::{InstrumentId, Timestamp};
use market_squawk_modeling::{
    CalibrationBand, CalibrationEvidence, CalibrationMethod, CalibrationWindow, ForecastCoverage,
    ForecastHorizon, ForecastRequest, InferenceBackend, InferenceError, MAX_MODEL_FEATURES,
    ModelDecision, ModelFeatureValue, ModelInput, ModelInputError, NativeLinearBackend,
    RealizedCoverage, ResearchForecastBackend,
};

use crate::bundle::{TestResult, valid_fixture};

#[test]
fn native_linear_and_logistic_inference_are_deterministic_and_identity_bound() -> TestResult {
    let linear_fixture = valid_fixture("native_linear", 1, 1, |_, _| {})?;
    let linear_bundle = Arc::new(linear_fixture.load()?);
    let linear = NativeLinearBackend::try_from_bundle(Arc::clone(&linear_bundle))?;
    let mut linear_values = [
        ModelFeatureValue::try_new(linear_fixture.feature(0)?, 3.0)?,
        ModelFeatureValue::try_new(linear_fixture.feature(1)?, 14.0)?,
    ];
    let linear_input = ModelInput::try_new(linear.metadata(), &linear_values)?;
    let first = linear.infer(&linear_input)?;
    let second = linear.infer(&linear_input)?;
    assert_eq!(first.score().to_bits(), 4.5_f64.to_bits());
    assert_eq!(first.score().to_bits(), second.score().to_bits());
    assert_eq!(first.confidence().to_bits(), second.confidence().to_bits());
    assert!(std::ptr::eq(first.identity(), second.identity()));
    assert_eq!(first.decision(), ModelDecision::Positive);
    assert_eq!(first.model_id(), linear.metadata().model_id());
    assert_eq!(first.bundle_id(), linear.metadata().bundle_id());
    assert_eq!(first.bundle_version(), linear.metadata().bundle_version());
    assert_eq!(
        first.dataset().manifest(),
        linear.metadata().dataset().manifest()
    );
    assert_eq!(
        first.feature_semantic_digests(),
        linear.metadata().feature_semantic_digests()
    );
    linear_values[0].try_set_value(4.0)?;
    let reused_input = ModelInput::try_new(linear.metadata(), &linear_values)?;
    assert_eq!(
        linear.infer(&reused_input)?.score().to_bits(),
        6.5_f64.to_bits()
    );

    let logistic_fixture = valid_fixture("native_logistic", 1, 1, |_, _| {})?;
    let logistic = NativeLinearBackend::try_from_bundle(Arc::new(logistic_fixture.load()?))?;
    let logistic_values = [
        ModelFeatureValue::try_new(logistic_fixture.feature(0)?, 3.0)?,
        ModelFeatureValue::try_new(logistic_fixture.feature(1)?, 10.0)?,
    ];
    let logistic_input = ModelInput::try_new(logistic.metadata(), &logistic_values)?;
    let output = logistic.infer(&logistic_input)?;
    assert!(output.score().is_finite());
    assert!(output.score() > 0.5 && output.score() < 1.0);
    assert_eq!(output.decision(), ModelDecision::Positive);
    Ok(())
}

#[test]
fn native_input_and_inference_reject_shape_value_and_bundle_mismatches() -> TestResult {
    let fixture = valid_fixture("native_linear", 1, 1, |_, _| {})?;
    let bundle = Arc::new(fixture.load()?);
    let backend = NativeLinearBackend::try_from_bundle(bundle)?;
    assert_eq!(
        ModelFeatureValue::try_new(fixture.feature(0)?, f64::NAN),
        Err(ModelInputError::NonFiniteValue)
    );
    assert_eq!(
        ModelInput::try_new(
            backend.metadata(),
            &[ModelFeatureValue::try_new(fixture.feature(0)?, 1.0)?]
        ),
        Err(ModelInputError::FeatureShapeMismatch)
    );
    let repeated = ModelFeatureValue::try_new(fixture.feature(0)?, 1.0)?;
    let oversized = vec![repeated; MAX_MODEL_FEATURES + 1];
    assert_eq!(
        ModelInput::try_new(backend.metadata(), &oversized),
        Err(ModelInputError::FeatureShapeMismatch)
    );
    let reversed = [
        ModelFeatureValue::try_new(fixture.feature(1)?, 1.0)?,
        ModelFeatureValue::try_new(fixture.feature(0)?, 2.0)?,
    ];
    assert_eq!(
        ModelInput::try_new(backend.metadata(), &reversed),
        Err(ModelInputError::FeatureIdentityMismatch)
    );

    let other_fixture = valid_fixture("native_linear", 1, 2, |_, _| {})?;
    let other_bundle = other_fixture.load()?;
    let other_values = [
        ModelFeatureValue::try_new(other_fixture.feature(0)?, 1.0)?,
        ModelFeatureValue::try_new(other_fixture.feature(1)?, 10.0)?,
    ];
    let other_input = ModelInput::try_new(other_bundle.metadata(), &other_values)?;
    assert_eq!(
        backend.infer(&other_input),
        Err(InferenceError::BundleMismatch)
    );
    Ok(())
}

#[test]
fn native_artifact_rejects_nonfinite_weights_and_multiple_outputs() -> TestResult {
    let nonfinite = valid_fixture("native_linear", 1, 1, |_, artifact| {
        artifact["weights"] = serde_json::json!([1e300, 1e300]);
        artifact["bias"] = serde_json::json!(1e300);
    })?;
    let bundle = Arc::new(nonfinite.load()?);
    let backend = NativeLinearBackend::try_from_bundle(bundle)?;
    let values = [
        ModelFeatureValue::try_new(nonfinite.feature(0)?, 1e300)?,
        ModelFeatureValue::try_new(nonfinite.feature(1)?, 1e300)?,
    ];
    let input = ModelInput::try_new(backend.metadata(), &values)?;
    assert_eq!(
        backend.infer(&input),
        Err(InferenceError::NonFiniteComputation)
    );

    let multiple = valid_fixture("native_linear", 1, 1, |_, artifact| {
        artifact["output_count"] = serde_json::json!(2);
    })?;
    let observed = multiple
        .load()
        .err()
        .ok_or_else(|| std::io::Error::other("multiple artifact outputs were accepted"))?;
    assert_eq!(
        observed,
        market_squawk_modeling::BundleError::UnsupportedOutputShape
    );
    Ok(())
}

#[test]
fn native_research_forecast_preserves_exact_horizons_identity_and_calibrated_nested_intervals()
-> TestResult {
    let fixture = valid_fixture("native_linear", 1, 1, |_, _| {})?;
    let backend = NativeLinearBackend::try_from_bundle(Arc::new(fixture.load()?))?;
    let values = [
        [
            ModelFeatureValue::try_new(fixture.feature(0)?, 3.0)?,
            ModelFeatureValue::try_new(fixture.feature(1)?, 14.0)?,
        ],
        [
            ModelFeatureValue::try_new(fixture.feature(0)?, 4.0)?,
            ModelFeatureValue::try_new(fixture.feature(1)?, 14.0)?,
        ],
        [
            ModelFeatureValue::try_new(fixture.feature(0)?, 5.0)?,
            ModelFeatureValue::try_new(fixture.feature(1)?, 14.0)?,
        ],
    ];
    let inputs = [
        ModelInput::try_new(backend.metadata(), &values[0])?,
        ModelInput::try_new(backend.metadata(), &values[1])?,
        ModelInput::try_new(backend.metadata(), &values[2])?,
    ];
    let horizon = ForecastHorizon::try_new(
        NonZeroU16::new(3).ok_or("horizon")?,
        NonZeroU64::new(60).ok_or("step")?,
    )?;
    let request = ForecastRequest::try_new(
        InstrumentId::from_str("018f3c2a-91ab-7ccd-b3de-123456789aaa")?,
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(900),
        horizon,
        2,
        &inputs,
    )?;
    let window = CalibrationWindow::try_new(
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(800),
        NonZeroU32::new(80).ok_or("calibration observations")?,
    )?;
    let calibration = CalibrationEvidence::try_new(
        backend.metadata(),
        CalibrationMethod::MapieEnbpi,
        window,
        Sha256Digest::new([71; 32]),
        Sha256Digest::new([72; 32]),
        [
            CalibrationBand::try_new(
                ForecastCoverage::Fifty,
                -0.5,
                0.5,
                RealizedCoverage::try_new(41, NonZeroU64::new(80).ok_or("coverage total")?)?,
            )?,
            CalibrationBand::try_new(
                ForecastCoverage::Eighty,
                -1.0,
                1.0,
                RealizedCoverage::try_new(65, NonZeroU64::new(80).ok_or("coverage total")?)?,
            )?,
            CalibrationBand::try_new(
                ForecastCoverage::NinetyFive,
                -2.0,
                2.0,
                RealizedCoverage::try_new(76, NonZeroU64::new(80).ok_or("coverage total")?)?,
            )?,
        ],
        "block bootstrap residual dependence; marginal empirical coverage is not a per-observation guarantee",
    )?;

    let forecast = backend.forecast(&request, Some(&calibration))?;

    assert_eq!(forecast.horizon(), horizon);
    assert_eq!(
        forecast.observed_cutoff(),
        Timestamp::from_unix_nanos(1_000)
    );
    assert_eq!(forecast.available_at(), Timestamp::from_unix_nanos(900));
    assert_eq!(forecast.model_id(), backend.metadata().model_id());
    assert_eq!(forecast.bundle_id(), backend.metadata().bundle_id());
    assert_eq!(
        forecast.bundle_version(),
        backend.metadata().bundle_version()
    );
    assert_eq!(forecast.dataset(), backend.metadata().dataset());
    assert_eq!(
        forecast.feature_semantic_digests(),
        backend.metadata().feature_semantic_digests()
    );
    assert_eq!(forecast.calibration(), Some(&calibration));
    assert_eq!(forecast.points().len(), 3);
    for (index, point) in forecast.points().iter().enumerate() {
        assert_eq!(
            point.target_at().unix_nanos(),
            1_000 + 60 * i64::try_from(index + 1)?
        );
        assert!(point.central().to_f64().is_finite());
        let intervals = point.intervals().ok_or("calibrated intervals missing")?;
        assert!(intervals.interval_95().lower() <= intervals.interval_80().lower());
        assert!(intervals.interval_80().lower() <= intervals.interval_50().lower());
        assert!(intervals.interval_50().lower() <= point.central());
        assert!(point.central() <= intervals.interval_50().upper());
        assert!(intervals.interval_50().upper() <= intervals.interval_80().upper());
        assert!(intervals.interval_80().upper() <= intervals.interval_95().upper());
    }
    Ok(())
}
