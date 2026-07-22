#[cfg(feature = "onnx-runtime")]
use std::env;
use std::fs;
#[cfg(feature = "onnx-runtime")]
use std::path::Component;
use std::path::Path;
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
    OnnxModelPolicy, OnnxPolicyError, OnnxWorkerProgram, TractOnnxBackend,
};
use prost::Message;
use serde::Deserialize;
use sha2::{Digest, Sha256};
#[cfg(feature = "onnx-runtime")]
use tempfile::TempDir;
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
    let program = worker_program()?;
    let error = TractOnnxBackend::try_from_bundle(
        Arc::new(oversized_fixture.load()?),
        oversized_policy,
        &program,
    )
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
    let program = worker_program()?;
    let backend = TractOnnxBackend::try_from_bundle(bundle, policy, &program)?;
    assert_eq!(program.active_generations(), 1);
    let values = [
        ModelFeatureValue::try_new(fixture.feature(0)?, 3.0)?,
        ModelFeatureValue::try_new(fixture.feature(1)?, 14.0)?,
    ];
    let input = ModelInput::try_new(backend.metadata(), &values)?;
    let output = backend.infer(&input)?;
    assert_eq!(output.score().to_bits(), 4.5_f64.to_bits());
    assert!(output.confidence().is_finite());
    assert_ne!(backend.runtime_evidence().warm_up_digest(), [0; 32]);
    drop(backend);
    assert_eq!(program.active_generations(), 0);
    Ok(())
}

#[test]
fn tract_worker_deadline_terminates_and_reaps_failed_generation() -> TestResult {
    let model = golden_model()?;
    let fixture = valid_onnx_fixture(&model)?;
    let policy = OnnxModelPolicy::try_new(
        Sha256Digest::new(Sha256::digest(&model).into()),
        13,
        &[1, 2],
        &[1, 1],
        Duration::from_nanos(1),
        OnnxFallbackPolicy::NoAction,
    )?;
    let program = worker_program()?;

    let error = TractOnnxBackend::try_from_bundle(Arc::new(fixture.load()?), policy, &program)
        .err()
        .ok_or("expired worker generation was published")?;

    assert_eq!(error, OnnxBackendError::WarmUp);
    assert_eq!(program.active_generations(), 0);
    Ok(())
}

#[test]
fn tract_worker_rejects_graph_over_compute_budget_before_warm_up() -> TestResult {
    let model = compute_heavy_model()?;
    let fixture = valid_onnx_fixture(&model)?;
    let policy = policy_for(&model, 13, &[1, 2], &[1, 1])?;
    let program = worker_program()?;

    let error = TractOnnxBackend::try_from_bundle(Arc::new(fixture.load()?), policy, &program)
        .err()
        .ok_or("compute-heavy graph was published")?;

    assert_eq!(error, OnnxBackendError::IntermediateLimit);
    assert_eq!(program.active_generations(), 0);
    Ok(())
}

#[cfg(feature = "onnx-runtime")]
#[test]
fn external_runtime_matches_tract_when_explicitly_configured() -> TestResult {
    let library = match env::var("MARKET_SQUAWK_ONNX_RUNTIME") {
        Ok(value) if !value.is_empty() => value,
        _ => return Ok(()),
    };
    let configured_root = fs::canonicalize(env::var("MARKET_SQUAWK_ONNX_RUNTIME_ROOT")?)?;
    let evidence_path = fs::canonicalize(env::var("MARKET_SQUAWK_ONNX_RUNTIME_EVIDENCE")?)?;
    let library_path = fs::canonicalize(library)?;
    let policy_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/verification/onnx-runtime-policy.json");
    let library_digest = decode_digest(&env::var("MARKET_SQUAWK_ONNX_RUNTIME_SHA256")?)?;
    let evidence_digest = Sha256::digest(fs::read(&evidence_path)?).into();
    let policy_digest = Sha256::digest(fs::read(policy_path)?).into();
    let library_relative = controlled_relative(&configured_root, &library_path)?;
    let evidence_relative = controlled_relative(&configured_root, &evidence_path)?;
    let source_root = TempDir::new()?;
    let seal_root = TempDir::new()?;
    let local_library = source_root.path().join(&library_relative);
    let local_evidence = source_root.path().join(&evidence_relative);
    fs::create_dir_all(
        local_library
            .parent()
            .ok_or("runtime library parent missing")?,
    )?;
    fs::create_dir_all(
        local_evidence
            .parent()
            .ok_or("runtime evidence parent missing")?,
    )?;
    fs::copy(&library_path, &local_library)?;
    fs::copy(&evidence_path, &local_evidence)?;

    let reference = ExternalOnnxRuntimeReference::try_new(
        &library_relative,
        &evidence_relative,
        library_digest,
        evidence_digest,
        policy_digest,
        OPTIONAL_ONNX_RUNTIME_VERSION,
        current_external_platform()?,
    )?;
    let root = ControlledOnnxRuntimeRoot::open_ambient(source_root.path(), seal_root.path())?;
    let admission = root.admit(&reference)?;
    fs::write(&local_library, b"source-substituted-after-admission")?;

    let model = golden_model()?;
    let fixture = valid_onnx_fixture(&model)?;
    let program = worker_program()?;
    let required = Arc::new(TractOnnxBackend::try_from_bundle(
        Arc::new(fixture.load()?),
        policy_for(&model, 13, &[1, 2], &[1, 1])?,
        &program,
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
    assert_eq!(program.active_generations(), 2);
    drop(external);
    assert_eq!(program.active_generations(), 1);
    Ok(())
}

fn worker_program() -> TestResult<OnnxWorkerProgram> {
    let path = Path::new(env!("CARGO_BIN_EXE_market-squawk-onnx-worker"));
    let digest = Sha256::digest(fs::read(path)?).into();
    Ok(OnnxWorkerProgram::admit(path, digest)?)
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

fn compute_heavy_model() -> TestResult<Vec<u8>> {
    let mut proto = ModelProto::decode(golden_model()?.as_slice())?;
    let graph = proto.graph.as_mut().ok_or("golden graph missing")?;
    if graph.initializer.len() != 2 || graph.node.len() != 1 {
        return Err("golden graph topology differs".into());
    }
    for tensor in &mut graph.initializer {
        tensor.dims = vec![400, 400];
        tensor.float_data.clear();
        tensor.raw_data = vec![0; 400 * 400 * std::mem::size_of::<f32>()];
    }
    graph.initializer[0].name = "A".to_owned();
    graph.initializer[1].name = "B".to_owned();

    let template = graph.node[0].clone();
    let mut reduce_input = template.clone();
    reduce_input.name = "reduce-input".to_owned();
    reduce_input.op_type = "ReduceMean".to_owned();
    reduce_input.input = vec!["X".to_owned()];
    reduce_input.output = vec!["S".to_owned()];
    reduce_input.attribute.clear();

    let mut broadcast = template.clone();
    broadcast.name = "broadcast-input".to_owned();
    broadcast.op_type = "Add".to_owned();
    broadcast.input = vec!["A".to_owned(), "S".to_owned()];
    broadcast.output = vec!["C".to_owned()];
    broadcast.attribute.clear();

    let mut matrix = template.clone();
    matrix.name = "bounded-matrix".to_owned();
    matrix.op_type = "MatMul".to_owned();
    matrix.input = vec!["C".to_owned(), "B".to_owned()];
    matrix.output = vec!["D".to_owned()];
    matrix.attribute.clear();

    let mut reduce_output = template;
    reduce_output.name = "reduce-output".to_owned();
    reduce_output.op_type = "ReduceMean".to_owned();
    reduce_output.input = vec!["D".to_owned()];
    reduce_output.output = vec!["Y".to_owned()];
    reduce_output.attribute.clear();
    graph.node = vec![reduce_input, broadcast, matrix, reduce_output];
    Ok(proto.encode_to_vec())
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
