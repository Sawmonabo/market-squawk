use market_squawk_modeling::{
    BundleError, InferenceError, ModelFailure, ModelFailurePhase, ModelInputError,
    ModelRegistryError, NativeBackendError,
};

#[test]
fn every_model_failure_plane_maps_to_typed_nonzero_audit_evidence() {
    let failures = [
        ModelFailure::from(BundleError::MetadataHashMismatch),
        ModelFailure::from(ModelRegistryError::RegistryFull),
        ModelFailure::from(ModelInputError::FeatureShapeMismatch),
        ModelFailure::from(NativeBackendError::UnsupportedBundleFormat),
        ModelFailure::from(InferenceError::NonFiniteComputation),
    ]
    .into_iter();
    #[cfg(feature = "onnx-tract")]
    let failures = failures.chain([
        ModelFailure::from(market_squawk_modeling::OnnxBackendError::Policy(
            market_squawk_modeling::OnnxPolicyError::ExternalData,
        )),
        ModelFailure::from(market_squawk_modeling::OnnxBackendError::WarmUp),
    ]);

    for failure in failures {
        let expected_phase = failure.phase();
        let audit = failure.audit();
        assert_eq!(audit.phase(), expected_phase);
        assert_ne!(audit.source_code().get(), 0);
        assert_ne!(audit.source_digest(), [0; 32]);
    }
}

#[test]
fn validation_load_and_inference_failures_keep_distinct_no_action_phases() {
    assert_eq!(
        ModelFailure::from(BundleError::InvalidNormalizer).phase(),
        ModelFailurePhase::Validation
    );
    assert_eq!(
        ModelFailure::from(ModelRegistryError::RegistryUnavailable).phase(),
        ModelFailurePhase::Load
    );
    assert_eq!(
        ModelFailure::from(InferenceError::BundleMismatch).phase(),
        ModelFailurePhase::Inference
    );
    #[cfg(feature = "onnx-tract")]
    {
        assert_eq!(
            ModelFailure::from(market_squawk_modeling::OnnxBackendError::Policy(
                market_squawk_modeling::OnnxPolicyError::DisallowedOperator,
            ))
            .phase(),
            ModelFailurePhase::Validation
        );
        assert_eq!(
            ModelFailure::from(market_squawk_modeling::OnnxBackendError::WarmUp).phase(),
            ModelFailurePhase::Load
        );
    }
}
