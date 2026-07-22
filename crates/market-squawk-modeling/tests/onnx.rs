#[cfg(feature = "onnx-runtime")]
use std::env;
#[cfg(feature = "onnx-runtime")]
use std::fs;
#[cfg(feature = "onnx-runtime")]
use std::path::{Component, Path};
use std::sync::Arc;
use std::time::Duration;

use market_squawk_data::Sha256Digest;
#[cfg(feature = "onnx-runtime")]
use market_squawk_modeling::{
    ControlledOnnxRuntimeRoot, ExternalOnnxRuntimeBackend, ExternalOnnxRuntimeReference,
    ExternalRuntimePlatform, OPTIONAL_ONNX_RUNTIME_VERSION,
};
use market_squawk_modeling::{
    InferenceBackend, ModelFeatureValue, ModelInput, OnnxBackendError, OnnxFallbackPolicy,
    OnnxModelPolicy, OnnxPolicyError, TractOnnxBackend,
};
use prost::Message;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tract_onnx::pb::{ModelProto, StringStringEntryProto, tensor_shape_proto};

use crate::bundle::{TestResult, valid_onnx_fixture};

const FIXTURE_MANIFEST: &str = include_str!("../fixtures/onnx/manifest.json");

#[derive(Deserialize)]
struct FixtureManifest {
    schema_version: u32,
    models: Vec<FixtureModel>,
}

#[derive(Deserialize)]
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
    let golden = golden_model()?;
    let golden_policy = policy_for(&golden, 13, &[1, 2], &[1, 1])?;
    let admitted = golden_policy.preflight(&golden)?;
    assert_eq!(admitted.node_count(), 1);
    assert_eq!(admitted.tensor_count(), 4);

    let proto = ModelProto::decode(golden.as_slice())?;
    let mut cases = Vec::new();

    let mut external = proto.clone();
    external
        .graph
        .as_mut()
        .ok_or("golden graph missing")?
        .initializer[0]
        .external_data
        .push(StringStringEntryProto {
            key: "location".to_owned(),
            value: "weights.bin".to_owned(),
        });
    cases.push((external, OnnxPolicyError::ExternalData));

    let mut operator = proto.clone();
    operator.graph.as_mut().ok_or("golden graph missing")?.node[0].op_type = "If".to_owned();
    cases.push((operator, OnnxPolicyError::DisallowedOperator));

    let mut domain = proto.clone();
    domain.graph.as_mut().ok_or("golden graph missing")?.node[0].domain =
        "vendor.custom".to_owned();
    cases.push((domain, OnnxPolicyError::CustomDomain));

    let mut dynamic = proto.clone();
    let dimension = dynamic.graph.as_mut().ok_or("golden graph missing")?.input[0]
        .r#type
        .as_mut()
        .and_then(|value| value.value.as_mut())
        .and_then(|value| match value {
            tract_onnx::pb::type_proto::Value::TensorType(tensor) => tensor.shape.as_mut(),
        })
        .and_then(|shape| shape.dim.get_mut(1))
        .ok_or("golden input shape missing")?;
    dimension.value = Some(tensor_shape_proto::dimension::Value::DimParam(
        "N".to_owned(),
    ));
    cases.push((dynamic, OnnxPolicyError::DynamicShape));

    let mut nonfinite = proto.clone();
    nonfinite
        .graph
        .as_mut()
        .ok_or("golden graph missing")?
        .initializer[0]
        .float_data[0] = f32::NAN;
    cases.push((nonfinite, OnnxPolicyError::NonFiniteTensor));

    let mut too_many_nodes = proto.clone();
    let graph = too_many_nodes
        .graph
        .as_mut()
        .ok_or("golden graph missing")?;
    let node = graph.node[0].clone();
    graph.node.resize(1_025, node);
    cases.push((too_many_nodes, OnnxPolicyError::NodeLimit));

    let mut too_many_tensors = proto;
    let graph = too_many_tensors
        .graph
        .as_mut()
        .ok_or("golden graph missing")?;
    let tensor = graph.initializer[0].clone();
    graph.initializer.resize(257, tensor);
    cases.push((too_many_tensors, OnnxPolicyError::TensorLimit));

    for (hostile, expected) in cases {
        let bytes = hostile.encode_to_vec();
        let policy = policy_for(&bytes, 13, &[1, 2], &[1, 1])?;
        assert_eq!(policy.preflight(&bytes), Err(expected));
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
        wrong_digest.preflight(&golden),
        Err(OnnxPolicyError::ModelDigestMismatch)
    );
    assert_eq!(
        golden_policy.preflight(b"not protobuf"),
        Err(OnnxPolicyError::ModelDigestMismatch)
    );

    let mut oversized_intermediate = ModelProto::decode(golden.as_slice())?;
    let graph = oversized_intermediate
        .graph
        .as_mut()
        .ok_or("golden graph missing")?;
    graph.initializer.truncate(1);
    graph.initializer[0].dims = vec![1_000_000, 1];
    graph.initializer[0].float_data.clear();
    graph.initializer[0].raw_data = vec![0; 4_000_000];
    let mut add = graph.node[0].clone();
    add.name = "oversized-broadcast".to_owned();
    add.op_type = "Add".to_owned();
    add.input = vec!["X".to_owned(), "W".to_owned()];
    add.output = vec!["H".to_owned()];
    add.attribute.clear();
    let mut reduce = add.clone();
    reduce.name = "bounded-output".to_owned();
    reduce.op_type = "ReduceMean".to_owned();
    reduce.input = vec!["H".to_owned()];
    reduce.output = vec!["Y".to_owned()];
    graph.node = vec![add, reduce];
    let oversized_bytes = oversized_intermediate.encode_to_vec();
    let oversized_policy = policy_for(&oversized_bytes, 13, &[1, 2], &[1, 1])?;
    oversized_policy.preflight(&oversized_bytes)?;
    let oversized_fixture = valid_onnx_fixture(&oversized_bytes)?;
    let error =
        TractOnnxBackend::try_from_bundle(Arc::new(oversized_fixture.load()?), oversized_policy)
            .err()
            .ok_or("oversized inferred intermediate was accepted")?;
    assert_eq!(error, OnnxBackendError::IntermediateLimit);
    Ok(())
}

#[test]
fn tract_backend_runs_the_exact_bundle_with_finite_bounded_output() -> TestResult {
    let model = golden_model()?;
    let fixture = valid_onnx_fixture(&model)?;
    let bundle = Arc::new(fixture.load()?);
    let policy = policy_for(&model, 13, &[1, 2], &[1, 1])?;
    let backend = TractOnnxBackend::try_from_bundle(bundle, policy)?;
    let values = [
        ModelFeatureValue::try_new(fixture.feature(0)?, 3.0)?,
        ModelFeatureValue::try_new(fixture.feature(1)?, 14.0)?,
    ];
    let input = ModelInput::try_new(backend.metadata(), &values)?;
    let output = backend.infer(&input)?;
    assert_eq!(output.score().to_bits(), 4.5_f64.to_bits());
    assert!(output.confidence().is_finite());
    assert_ne!(backend.runtime_evidence().warm_up_digest(), [0; 32]);
    Ok(())
}

#[cfg(feature = "onnx-runtime")]
#[test]
fn external_runtime_matches_tract_when_explicitly_configured() -> TestResult {
    let library = match env::var("MARKET_SQUAWK_ONNX_RUNTIME") {
        Ok(value) if !value.is_empty() => value,
        _ => return Ok(()),
    };
    let root_path = fs::canonicalize(env::var("MARKET_SQUAWK_ONNX_RUNTIME_ROOT")?)?;
    let evidence_path = fs::canonicalize(env::var("MARKET_SQUAWK_ONNX_RUNTIME_EVIDENCE")?)?;
    let library_path = fs::canonicalize(library)?;
    let policy_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/verification/onnx-runtime-policy.json");
    let library_digest = decode_digest(&env::var("MARKET_SQUAWK_ONNX_RUNTIME_SHA256")?)?;
    let evidence_digest = Sha256::digest(fs::read(&evidence_path)?).into();
    let policy_digest = Sha256::digest(fs::read(policy_path)?).into();
    let reference = ExternalOnnxRuntimeReference::try_new(
        controlled_relative(&root_path, &library_path)?,
        controlled_relative(&root_path, &evidence_path)?,
        library_digest,
        evidence_digest,
        policy_digest,
        OPTIONAL_ONNX_RUNTIME_VERSION,
        current_external_platform()?,
    )?;
    let root = ControlledOnnxRuntimeRoot::open_ambient(&root_path)?;
    let admission = root.admit(&reference)?;

    let model = golden_model()?;
    let fixture = valid_onnx_fixture(&model)?;
    let required = Arc::new(TractOnnxBackend::try_from_bundle(
        Arc::new(fixture.load()?),
        policy_for(&model, 13, &[1, 2], &[1, 1])?,
    )?);
    let external = ExternalOnnxRuntimeBackend::try_from_tract(&required, admission)?;
    let values = [
        ModelFeatureValue::try_new(fixture.feature(0)?, 3.0)?,
        ModelFeatureValue::try_new(fixture.feature(1)?, 14.0)?,
    ];
    let input = ModelInput::try_new(required.metadata(), &values)?;
    let required_output = required.infer(&input)?;
    let external_output = external.infer(&input)?;
    assert!((external_output.score() - required_output.score()).abs() <= 1.0e-5);
    Ok(())
}

fn policy_for(
    bytes: &[u8],
    opset: u32,
    input_shape: &[usize],
    output_shape: &[usize],
) -> TestResult<OnnxModelPolicy> {
    Ok(OnnxModelPolicy::try_new(
        Sha256Digest::new(Sha256::digest(bytes).into()),
        opset,
        input_shape,
        output_shape,
        Duration::from_millis(250),
        OnnxFallbackPolicy::NoAction,
    )?)
}

fn golden_model() -> TestResult<Vec<u8>> {
    let manifest: FixtureManifest = serde_json::from_str(FIXTURE_MANIFEST)?;
    if manifest.schema_version != 1 || manifest.models.len() != 1 {
        return Err("unexpected ONNX fixture manifest".into());
    }
    let fixture = manifest
        .models
        .first()
        .ok_or("golden ONNX fixture is missing")?;
    if fixture.id != "bounded-gemm-v1"
        || fixture.opset != 13
        || fixture.input_shape != [1, 2]
        || fixture.output_shape != [1, 1]
    {
        return Err("golden ONNX fixture identity differs".into());
    }
    let model = decode_hex(&fixture.model_hex)?;
    if Sha256::digest(&model).as_slice() != decode_digest(&fixture.artifact_sha256)? {
        return Err("golden ONNX fixture digest differs".into());
    }
    Ok(model)
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

fn decode_digest(value: &str) -> TestResult<[u8; 32]> {
    let bytes = decode_hex(value)?;
    bytes
        .try_into()
        .map_err(|_| "fixture digest length differs".into())
}

#[cfg(feature = "onnx-runtime")]
fn controlled_relative(root: &Path, path: &Path) -> TestResult<String> {
    let relative = path.strip_prefix(root)?;
    let mut segments = Vec::new();
    for component in relative.components() {
        let Component::Normal(segment) = component else {
            return Err("runtime path is not controlled".into());
        };
        segments.push(segment.to_str().ok_or("runtime path is not UTF-8")?);
    }
    if segments.is_empty() {
        return Err("runtime path is empty".into());
    }
    Ok(segments.join("/"))
}

#[cfg(all(feature = "onnx-runtime", target_os = "macos", target_arch = "aarch64"))]
fn current_external_platform() -> TestResult<ExternalRuntimePlatform> {
    Ok(ExternalRuntimePlatform::MacosArm64MachO)
}

#[cfg(all(feature = "onnx-runtime", target_os = "macos", target_arch = "x86_64"))]
fn current_external_platform() -> TestResult<ExternalRuntimePlatform> {
    Ok(ExternalRuntimePlatform::MacosX8664MachO)
}

#[cfg(all(feature = "onnx-runtime", target_os = "linux", target_arch = "aarch64"))]
fn current_external_platform() -> TestResult<ExternalRuntimePlatform> {
    Ok(ExternalRuntimePlatform::LinuxArm64Elf)
}

#[cfg(all(feature = "onnx-runtime", target_os = "linux", target_arch = "x86_64"))]
fn current_external_platform() -> TestResult<ExternalRuntimePlatform> {
    Ok(ExternalRuntimePlatform::LinuxX8664Elf)
}

#[cfg(all(
    feature = "onnx-runtime",
    target_os = "windows",
    target_arch = "aarch64"
))]
fn current_external_platform() -> TestResult<ExternalRuntimePlatform> {
    Ok(ExternalRuntimePlatform::WindowsArm64Pe)
}

#[cfg(all(
    feature = "onnx-runtime",
    target_os = "windows",
    target_arch = "x86_64"
))]
fn current_external_platform() -> TestResult<ExternalRuntimePlatform> {
    Ok(ExternalRuntimePlatform::WindowsX8664Pe)
}

#[cfg(all(
    feature = "onnx-runtime",
    not(any(
        all(
            target_os = "macos",
            any(target_arch = "aarch64", target_arch = "x86_64")
        ),
        all(
            target_os = "linux",
            any(target_arch = "aarch64", target_arch = "x86_64")
        ),
        all(
            target_os = "windows",
            any(target_arch = "aarch64", target_arch = "x86_64")
        )
    ))
))]
fn current_external_platform() -> TestResult<ExternalRuntimePlatform> {
    Err("host platform is not admitted for optional ONNX Runtime".into())
}
