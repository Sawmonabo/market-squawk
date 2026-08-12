#[cfg(feature = "release-evidence")]
use std::fs;
#[cfg(feature = "release-evidence")]
use std::path::Path;
use std::time::Duration;

use market_squawk_data::Sha256Digest;
#[cfg(feature = "release-evidence")]
use market_squawk_modeling::ReleaseEvidenceInferenceFixture;
use market_squawk_modeling::{
    ModelOutputSemantics, OnnxFallbackPolicy, OnnxModelPolicy, OnnxPolicyError,
};
use prost::Message;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tract_onnx::pb::{ModelProto, StringStringEntryProto, tensor_shape_proto};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const FIXTURE_MANIFEST: &str = include_str!("../fixtures/onnx/manifest.json");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureManifest {
    schema_version: u32,
    models: Vec<FixtureModel>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureModel {
    artifact_sha256: String,
    id: String,
    input_shape: Vec<usize>,
    model_hex: String,
    opset: u32,
    output_shape: Vec<usize>,
}

#[test]
fn onnx_policy_rejects_hostile_graphs_before_runtime_load() -> TestResult {
    let model = golden_model()?;
    let policy = policy_for(&model)?;
    let admitted = policy.preflight(&model)?;
    assert_eq!(admitted.node_count(), 1);
    assert_eq!(admitted.tensor_count(), 4);

    let binary_policy = OnnxModelPolicy::try_new_with_output_semantics(
        digest(&model),
        13,
        &[1, 2],
        &[1, 1],
        ModelOutputSemantics::BinaryProbability,
        Duration::from_millis(250),
        OnnxFallbackPolicy::NoAction,
    )?;
    assert_eq!(
        binary_policy.preflight(&model),
        Err(OnnxPolicyError::OutputSemanticsMismatch)
    );

    let proto = ModelProto::decode(model.as_slice())?;

    let mut external_data = proto.clone();
    external_data
        .graph
        .as_mut()
        .ok_or("golden graph missing")?
        .initializer[0]
        .external_data
        .push(StringStringEntryProto {
            key: "location".to_owned(),
            value: "weights.bin".to_owned(),
        });

    let mut disallowed_operator = proto.clone();
    disallowed_operator
        .graph
        .as_mut()
        .ok_or("golden graph missing")?
        .node[0]
        .op_type = "If".to_owned();

    let mut dynamic_shape = proto.clone();
    dynamic_shape
        .graph
        .as_mut()
        .and_then(|graph| graph.input[0].r#type.as_mut())
        .and_then(|value| value.value.as_mut())
        .and_then(|value| match value {
            tract_onnx::pb::type_proto::Value::TensorType(tensor) => tensor.shape.as_mut(),
        })
        .and_then(|shape| shape.dim.get_mut(1))
        .ok_or("golden input shape missing")?
        .value = Some(tensor_shape_proto::dimension::Value::DimParam(
        "N".to_owned(),
    ));

    let mut nonfinite_tensor = proto;
    nonfinite_tensor
        .graph
        .as_mut()
        .ok_or("golden graph missing")?
        .initializer[0]
        .float_data[0] = f32::NAN;

    for (hostile, expected) in [
        (external_data, OnnxPolicyError::ExternalData),
        (disallowed_operator, OnnxPolicyError::DisallowedOperator),
        (dynamic_shape, OnnxPolicyError::DynamicShape),
        (nonfinite_tensor, OnnxPolicyError::NonFiniteTensor),
    ] {
        let bytes = hostile.encode_to_vec();
        assert_eq!(policy_for(&bytes)?.preflight(&bytes), Err(expected));
    }

    let wrong_digest = OnnxModelPolicy::try_new(
        Sha256Digest::new([9; 32]),
        13,
        &[1, 2],
        &[1, 1],
        Duration::from_millis(250),
        OnnxFallbackPolicy::NoAction,
    )?;
    assert_eq!(
        wrong_digest.preflight(&model),
        Err(OnnxPolicyError::ModelDigestMismatch)
    );
    Ok(())
}

#[cfg(feature = "release-evidence")]
#[test]
fn admitted_worker_runs_the_exact_fixture_with_bounded_evidence() -> TestResult {
    let worker = Path::new(env!("CARGO_BIN_EXE_market-squawk-onnx-worker"));
    let worker_digest = Sha256::digest(fs::read(worker)?).into();
    let fixture = ReleaseEvidenceInferenceFixture::try_new(worker, worker_digest)?;

    let output = fixture.infer_onnx()?;
    assert_eq!(output.score().to_bits(), 4.5_f64.to_bits());
    assert!(output.confidence().is_finite());

    let identity = fixture.identity();
    assert_ne!(identity.onnx_artifact_digest(), [0; 32]);
    assert_ne!(identity.onnx_policy_digest(), [0; 32]);
    assert_ne!(identity.onnx_worker_digest(), [0; 32]);
    assert_ne!(identity.onnx_runtime_semantics_digest(), [0; 32]);
    assert_ne!(identity.onnx_warm_up_digest(), [0; 32]);
    assert!(identity.onnx_retained_bytes() > 0);
    Ok(())
}

fn policy_for(model: &[u8]) -> Result<OnnxModelPolicy, OnnxPolicyError> {
    OnnxModelPolicy::try_new(
        digest(model),
        13,
        &[1, 2],
        &[1, 1],
        Duration::from_millis(250),
        OnnxFallbackPolicy::NoAction,
    )
}

fn golden_model() -> TestResult<Vec<u8>> {
    let manifest: FixtureManifest = serde_json::from_str(FIXTURE_MANIFEST)?;
    let fixture = match manifest.models.as_slice() {
        [fixture]
            if manifest.schema_version == 1
                && fixture.id == "bounded-gemm-v1"
                && fixture.opset == 13
                && fixture.input_shape == [1, 2]
                && fixture.output_shape == [1, 1] =>
        {
            fixture
        }
        _ => return Err("unexpected ONNX fixture manifest".into()),
    };
    let model = decode_hex(&fixture.model_hex)?;
    if digest(&model).bytes() != decode_digest(&fixture.artifact_sha256)? {
        return Err("ONNX fixture digest differs".into());
    }
    Ok(model)
}

fn digest(value: &[u8]) -> Sha256Digest {
    Sha256Digest::new(Sha256::digest(value).into())
}

fn decode_digest(value: &str) -> TestResult<[u8; 32]> {
    decode_hex(value)?
        .try_into()
        .map_err(|_| "fixture digest length differs".into())
}

fn decode_hex(value: &str) -> TestResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err("hex fixture length is odd".into());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair)?;
            Ok(u8::from_str_radix(pair, 16)?)
        })
        .collect()
}
