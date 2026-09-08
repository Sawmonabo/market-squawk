use std::sync::Arc;

use crate::{
    InferenceBackend, InferenceError, MAX_MODEL_FEATURES, ModelDecision, ModelFeatureValue,
    ModelInput, ModelInputError, NativeLinearBackend,
};

use super::bundle::{TestResult, valid_fixture};

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
    assert_eq!(observed, crate::BundleError::UnsupportedOutputShape);
    Ok(())
}
