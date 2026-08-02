//! Runtime-independent ONNX graph admission.

use std::collections::HashSet;
use std::io::Cursor;
use std::time::Duration;

use market_squawk_data::Sha256Digest;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tract_onnx::pb::{AttributeProto, ModelProto, TensorProto, TypeProto, tensor_proto};
use tract_onnx::prelude::Framework;

use crate::ModelOutputSemantics;

/// Maximum bytes in one self-contained ONNX protobuf.
pub const MAX_ONNX_MODEL_BYTES: usize = 64 * 1024 * 1024;
/// Maximum executable nodes in one admitted graph.
pub const MAX_ONNX_NODES: usize = 1_024;
/// Maximum declared tensors in one admitted graph.
pub const MAX_ONNX_TENSORS: usize = 256;
/// Maximum input plus output elements in one inference request.
pub const MAX_ONNX_REQUEST_ELEMENTS: usize = 1_000_000;
const MAX_ONNX_RANK: usize = 8;
const MIN_ONNX_OPSET: u32 = 13;
const MAX_ONNX_OPSET: u32 = 24;
const MAX_INFERENCE_DEADLINE: Duration = Duration::from_secs(5);

const ALLOWED_OPERATORS: &[&str] = &[
    "Add",
    "Cast",
    "Clip",
    "Concat",
    "Div",
    "Gather",
    "Gemm",
    "Identity",
    "MatMul",
    "Mul",
    "ReduceMean",
    "Relu",
    "Reshape",
    "Sigmoid",
    "Softmax",
    "Sqrt",
    "Sub",
    "Tanh",
    "Transpose",
];

/// Stable failure behavior selected before loading a runtime.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OnnxFallbackPolicy {
    /// Return a typed error for conversion into audited no-action evidence.
    NoAction,
}

/// Complete immutable graph policy bound to one exact ONNX artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OnnxModelPolicy {
    model_digest: Sha256Digest,
    opset: u32,
    input_shape: Box<[usize]>,
    output_shape: Box<[usize]>,
    output_semantics: ModelOutputSemantics,
    output_semantics_bound: bool,
    inference_deadline: Duration,
    fallback: OnnxFallbackPolicy,
    policy_digest: [u8; 32],
}

impl OnnxModelPolicy {
    /// Constructs a closed, exact ONNX admission policy.
    ///
    /// # Errors
    ///
    /// Rejects reserved digests, unsupported opsets, dynamic-equivalent shapes, non-scalar output,
    /// excessive request elements, or an unbounded deadline.
    pub fn try_new(
        model_digest: Sha256Digest,
        opset: u32,
        input_shape: &[usize],
        output_shape: &[usize],
        inference_deadline: Duration,
        fallback: OnnxFallbackPolicy,
    ) -> Result<Self, OnnxPolicyError> {
        Self::try_new_internal(
            model_digest,
            opset,
            input_shape,
            output_shape,
            ModelOutputSemantics::Regression,
            false,
            inference_deadline,
            fallback,
        )
    }

    /// Constructs a policy that binds the graph's scalar-output interpretation.
    ///
    /// # Errors
    ///
    /// Applies the same closed graph, shape, resource, and deadline validation as [`Self::try_new`].
    #[allow(
        clippy::too_many_arguments,
        reason = "all independent ONNX authorities remain explicit"
    )]
    pub fn try_new_with_output_semantics(
        model_digest: Sha256Digest,
        opset: u32,
        input_shape: &[usize],
        output_shape: &[usize],
        output_semantics: ModelOutputSemantics,
        inference_deadline: Duration,
        fallback: OnnxFallbackPolicy,
    ) -> Result<Self, OnnxPolicyError> {
        Self::try_new_internal(
            model_digest,
            opset,
            input_shape,
            output_shape,
            output_semantics,
            true,
            inference_deadline,
            fallback,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "all independent ONNX authorities remain explicit"
    )]
    fn try_new_internal(
        model_digest: Sha256Digest,
        opset: u32,
        input_shape: &[usize],
        output_shape: &[usize],
        output_semantics: ModelOutputSemantics,
        output_semantics_bound: bool,
        inference_deadline: Duration,
        fallback: OnnxFallbackPolicy,
    ) -> Result<Self, OnnxPolicyError> {
        if model_digest.bytes() == [0; 32] {
            return Err(OnnxPolicyError::InvalidPolicy);
        }
        if !(MIN_ONNX_OPSET..=MAX_ONNX_OPSET).contains(&opset) {
            return Err(OnnxPolicyError::UnsupportedOpset);
        }
        let input_elements = shape_elements(input_shape)?;
        let output_elements = shape_elements(output_shape)?;
        if output_elements != 1
            || input_elements
                .checked_add(output_elements)
                .is_none_or(|elements| elements > MAX_ONNX_REQUEST_ELEMENTS)
            || inference_deadline.is_zero()
            || inference_deadline > MAX_INFERENCE_DEADLINE
        {
            return Err(OnnxPolicyError::InvalidPolicy);
        }
        let policy_digest = digest_policy(
            model_digest,
            opset,
            input_shape,
            output_shape,
            output_semantics_bound.then_some(output_semantics),
            inference_deadline,
            fallback,
        );
        Ok(Self {
            model_digest,
            opset,
            input_shape: input_shape.into(),
            output_shape: output_shape.into(),
            output_semantics,
            output_semantics_bound,
            inference_deadline,
            fallback,
            policy_digest,
        })
    }

    /// Parses and validates the complete graph before either runtime sees it.
    ///
    /// # Errors
    ///
    /// Returns a typed digest, protobuf, operator, shape, tensor, or resource failure.
    pub fn preflight(&self, bytes: &[u8]) -> Result<ValidatedOnnxModel, OnnxPolicyError> {
        if bytes.len() > MAX_ONNX_MODEL_BYTES {
            return Err(OnnxPolicyError::ModelByteLimit);
        }
        if Sha256::digest(bytes).as_slice() != self.model_digest.bytes() {
            return Err(OnnxPolicyError::ModelDigestMismatch);
        }
        super::wire::prescan(bytes)?;
        let proto = tract_onnx::onnx()
            .proto_model_for_read(&mut Cursor::new(bytes))
            .map_err(|_| OnnxPolicyError::InvalidProtobuf)?;
        validate_proto(self, &proto)
    }

    /// Returns the exact admitted model digest.
    #[must_use]
    pub const fn model_digest(&self) -> Sha256Digest {
        self.model_digest
    }

    /// Returns the exact admitted ONNX operator-set version.
    #[must_use]
    pub const fn opset(&self) -> u32 {
        self.opset
    }

    /// Returns the exact static input shape.
    #[must_use]
    pub fn input_shape(&self) -> &[usize] {
        &self.input_shape
    }

    /// Returns the exact static scalar-output shape.
    #[must_use]
    pub fn output_shape(&self) -> &[usize] {
        &self.output_shape
    }

    /// Returns the scalar-output interpretation used by runtime decision semantics.
    #[must_use]
    pub const fn output_semantics(&self) -> ModelOutputSemantics {
        self.output_semantics
    }

    /// Returns whether the policy digest and graph preflight explicitly bind output semantics.
    #[must_use]
    pub const fn output_semantics_bound(&self) -> bool {
        self.output_semantics_bound
    }

    /// Returns the bounded per-inference deadline.
    #[must_use]
    pub const fn inference_deadline(&self) -> Duration {
        self.inference_deadline
    }

    /// Returns the complete versioned policy identity.
    #[must_use]
    pub const fn policy_digest(&self) -> [u8; 32] {
        self.policy_digest
    }

    /// Returns the configured failure disposition.
    #[must_use]
    pub const fn fallback(&self) -> OnnxFallbackPolicy {
        self.fallback
    }
}

/// Runtime-independent evidence retained after exact graph preflight.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedOnnxModel {
    model_digest: Sha256Digest,
    opset: u32,
    node_count: usize,
    tensor_count: usize,
    input_elements: usize,
    output_elements: usize,
}

impl ValidatedOnnxModel {
    /// Returns the executable graph node count.
    #[must_use]
    pub const fn node_count(&self) -> usize {
        self.node_count
    }

    /// Returns the complete declared tensor count.
    #[must_use]
    pub const fn tensor_count(&self) -> usize {
        self.tensor_count
    }

    pub(crate) const fn input_elements(&self) -> usize {
        self.input_elements
    }
}

/// Common ONNX admission failure before runtime construction.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OnnxPolicyError {
    #[error("ONNX policy is internally invalid")]
    InvalidPolicy,
    #[error("ONNX model exceeds the 64 MiB byte ceiling")]
    ModelByteLimit,
    #[error("ONNX model digest does not match the bundle policy")]
    ModelDigestMismatch,
    #[error("ONNX protobuf is corrupt or unsupported")]
    InvalidProtobuf,
    #[error("ONNX protobuf exceeds the pre-decode structural resource ceiling")]
    ProtobufResourceLimit,
    #[error("ONNX opset is unsupported or differs from policy")]
    UnsupportedOpset,
    #[error("ONNX graph uses a custom operator domain")]
    CustomDomain,
    #[error("ONNX graph uses a disallowed operator")]
    DisallowedOperator,
    #[error("ONNX graph uses external tensor data")]
    ExternalData,
    #[error("ONNX graph contains a dynamic, absent, or unbounded shape")]
    DynamicShape,
    #[error("ONNX graph shape differs from policy")]
    ShapeMismatch,
    #[error("ONNX graph output link differs from its declared semantics")]
    OutputSemanticsMismatch,
    #[error("ONNX graph exceeds the node ceiling")]
    NodeLimit,
    #[error("ONNX graph exceeds the tensor ceiling")]
    TensorLimit,
    #[error("ONNX graph exceeds the request-element ceiling")]
    ElementLimit,
    #[error("ONNX embedded tensor contains nonfinite floating point data")]
    NonFiniteTensor,
    #[error("ONNX graph contains training, function, sparse, or nested graph state")]
    UnsupportedGraphState,
}

fn validate_proto(
    policy: &OnnxModelPolicy,
    proto: &ModelProto,
) -> Result<ValidatedOnnxModel, OnnxPolicyError> {
    if !proto.training_info.is_empty() || !proto.functions.is_empty() {
        return Err(OnnxPolicyError::UnsupportedGraphState);
    }
    if proto.opset_import.len() != 1 {
        return Err(OnnxPolicyError::UnsupportedOpset);
    }
    let import = &proto.opset_import[0];
    if !matches!(import.domain.as_str(), "" | "ai.onnx")
        || u32::try_from(import.version).ok() != Some(policy.opset)
    {
        return Err(OnnxPolicyError::UnsupportedOpset);
    }
    let graph = proto
        .graph
        .as_ref()
        .ok_or(OnnxPolicyError::InvalidProtobuf)?;
    if graph.node.is_empty() || graph.node.len() > MAX_ONNX_NODES {
        return Err(OnnxPolicyError::NodeLimit);
    }
    if !graph.sparse_initializer.is_empty() {
        return Err(OnnxPolicyError::UnsupportedGraphState);
    }
    for node in &graph.node {
        if !matches!(node.domain.as_str(), "" | "ai.onnx") {
            return Err(OnnxPolicyError::CustomDomain);
        }
        if !ALLOWED_OPERATORS.contains(&node.op_type.as_str()) {
            return Err(OnnxPolicyError::DisallowedOperator);
        }
        for attribute in &node.attribute {
            validate_attribute(attribute)?;
        }
    }
    for tensor in &graph.initializer {
        validate_tensor(tensor)?;
    }

    let tensor_count = graph
        .input
        .len()
        .checked_add(graph.output.len())
        .and_then(|count| count.checked_add(graph.value_info.len()))
        .and_then(|count| count.checked_add(graph.initializer.len()))
        .and_then(|count| {
            graph.node.iter().try_fold(count, |count, node| {
                node.attribute.iter().try_fold(count, |count, attribute| {
                    count
                        .checked_add(usize::from(attribute.t.is_some()))
                        .and_then(|value| value.checked_add(attribute.tensors.len()))
                })
            })
        })
        .ok_or(OnnxPolicyError::TensorLimit)?;
    if tensor_count > MAX_ONNX_TENSORS {
        return Err(OnnxPolicyError::TensorLimit);
    }

    let initializer_names: HashSet<_> = graph
        .initializer
        .iter()
        .map(|tensor| tensor.name.as_str())
        .collect();
    let external_inputs: Vec<_> = graph
        .input
        .iter()
        .filter(|input| !initializer_names.contains(input.name.as_str()))
        .collect();
    if external_inputs.len() != 1 || graph.output.len() != 1 {
        return Err(OnnxPolicyError::ShapeMismatch);
    }
    let input_shape = static_f32_shape(
        external_inputs[0]
            .r#type
            .as_ref()
            .ok_or(OnnxPolicyError::DynamicShape)?,
    )?;
    let output_shape = static_f32_shape(
        graph.output[0]
            .r#type
            .as_ref()
            .ok_or(OnnxPolicyError::DynamicShape)?,
    )?;
    if input_shape != policy.input_shape.as_ref() || output_shape != policy.output_shape.as_ref() {
        return Err(OnnxPolicyError::ShapeMismatch);
    }
    if policy.output_semantics_bound {
        let output_name = graph.output[0].name.as_str();
        let mut producers = graph
            .node
            .iter()
            .filter(|node| node.output.iter().any(|value| value == output_name));
        let producer = producers
            .next()
            .ok_or(OnnxPolicyError::OutputSemanticsMismatch)?;
        if producers.next().is_some()
            || match policy.output_semantics {
                ModelOutputSemantics::Regression => {
                    matches!(producer.op_type.as_str(), "Sigmoid" | "Softmax")
                }
                ModelOutputSemantics::BinaryProbability => producer.op_type != "Sigmoid",
            }
        {
            return Err(OnnxPolicyError::OutputSemanticsMismatch);
        }
    }
    let input_elements = shape_elements(&input_shape)?;
    let output_elements = shape_elements(&output_shape)?;
    if input_elements
        .checked_add(output_elements)
        .is_none_or(|elements| elements > MAX_ONNX_REQUEST_ELEMENTS)
    {
        return Err(OnnxPolicyError::ElementLimit);
    }
    Ok(ValidatedOnnxModel {
        model_digest: policy.model_digest,
        opset: policy.opset,
        node_count: graph.node.len(),
        tensor_count,
        input_elements,
        output_elements,
    })
}

fn validate_attribute(attribute: &AttributeProto) -> Result<(), OnnxPolicyError> {
    if attribute.g.is_some()
        || !attribute.graphs.is_empty()
        || attribute.sparse_tensor.is_some()
        || !attribute.sparse_tensors.is_empty()
    {
        return Err(OnnxPolicyError::UnsupportedGraphState);
    }
    if let Some(tensor) = &attribute.t {
        validate_tensor(tensor)?;
    }
    for tensor in &attribute.tensors {
        validate_tensor(tensor)?;
    }
    if !attribute.f.is_finite() || attribute.floats.iter().any(|value| !value.is_finite()) {
        return Err(OnnxPolicyError::NonFiniteTensor);
    }
    Ok(())
}

fn validate_tensor(tensor: &TensorProto) -> Result<(), OnnxPolicyError> {
    if !tensor.external_data.is_empty()
        || tensor.data_location == Some(tensor_proto::DataLocation::External as i32)
    {
        return Err(OnnxPolicyError::ExternalData);
    }
    let dimensions = tensor
        .dims
        .iter()
        .map(|dimension| usize::try_from(*dimension).ok().filter(|value| *value > 0))
        .collect::<Option<Vec<_>>>()
        .ok_or(OnnxPolicyError::DynamicShape)?;
    if shape_elements(&dimensions)? > MAX_ONNX_REQUEST_ELEMENTS {
        return Err(OnnxPolicyError::ElementLimit);
    }
    if tensor.float_data.iter().any(|value| !value.is_finite())
        || tensor.double_data.iter().any(|value| !value.is_finite())
    {
        return Err(OnnxPolicyError::NonFiniteTensor);
    }
    match tensor_proto::DataType::try_from(tensor.data_type).ok() {
        Some(tensor_proto::DataType::Float) => {
            if !tensor.raw_data.chunks_exact(4).remainder().is_empty()
                || tensor
                    .raw_data
                    .chunks_exact(4)
                    .map(|bytes| f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
                    .any(|value| !value.is_finite())
            {
                return Err(OnnxPolicyError::NonFiniteTensor);
            }
        }
        Some(tensor_proto::DataType::Double) => {
            if !tensor.raw_data.chunks_exact(8).remainder().is_empty()
                || tensor
                    .raw_data
                    .chunks_exact(8)
                    .map(|bytes| {
                        f64::from_le_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                            bytes[7],
                        ])
                    })
                    .any(|value| !value.is_finite())
            {
                return Err(OnnxPolicyError::NonFiniteTensor);
            }
        }
        Some(_) => {}
        None => return Err(OnnxPolicyError::InvalidProtobuf),
    }
    Ok(())
}

fn static_f32_shape(value: &TypeProto) -> Result<Vec<usize>, OnnxPolicyError> {
    let tensor = match value.value.as_ref() {
        Some(tract_onnx::pb::type_proto::Value::TensorType(tensor)) => tensor,
        None => return Err(OnnxPolicyError::DynamicShape),
    };
    if tensor.elem_type != tensor_proto::DataType::Float as i32 {
        return Err(OnnxPolicyError::ShapeMismatch);
    }
    let shape = tensor.shape.as_ref().ok_or(OnnxPolicyError::DynamicShape)?;
    shape
        .dim
        .iter()
        .map(|dimension| match &dimension.value {
            Some(tract_onnx::pb::tensor_shape_proto::dimension::Value::DimValue(value)) => {
                usize::try_from(*value)
                    .ok()
                    .filter(|value| *value > 0)
                    .ok_or(OnnxPolicyError::DynamicShape)
            }
            _ => Err(OnnxPolicyError::DynamicShape),
        })
        .collect()
}

fn shape_elements(shape: &[usize]) -> Result<usize, OnnxPolicyError> {
    if shape.is_empty() || shape.len() > MAX_ONNX_RANK || shape.contains(&0) {
        return Err(OnnxPolicyError::DynamicShape);
    }
    shape.iter().try_fold(1_usize, |product, dimension| {
        product
            .checked_mul(*dimension)
            .filter(|value| *value <= MAX_ONNX_REQUEST_ELEMENTS)
            .ok_or(OnnxPolicyError::ElementLimit)
    })
}

fn digest_policy(
    model_digest: Sha256Digest,
    opset: u32,
    input_shape: &[usize],
    output_shape: &[usize],
    output_semantics: Option<ModelOutputSemantics>,
    deadline: Duration,
    fallback: OnnxFallbackPolicy,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    bind_bytes(
        &mut digest,
        b"namespace",
        if output_semantics.is_some() {
            b"market-squawk/onnx-policy/v3"
        } else {
            b"market-squawk/onnx-policy/v2"
        },
    );
    bind_bytes(
        &mut digest,
        b"wire-admission",
        &super::wire::admission_semantics_digest(),
    );
    bind_bytes(&mut digest, b"model-digest", &model_digest.bytes());
    bind_u128(&mut digest, b"opset", u128::from(opset));
    for (name, value) in [
        (b"max-model-bytes".as_slice(), MAX_ONNX_MODEL_BYTES),
        (b"max-nodes".as_slice(), MAX_ONNX_NODES),
        (b"max-tensors".as_slice(), MAX_ONNX_TENSORS),
        (
            b"max-request-elements".as_slice(),
            MAX_ONNX_REQUEST_ELEMENTS,
        ),
        (b"max-rank".as_slice(), MAX_ONNX_RANK),
    ] {
        bind_usize(&mut digest, name, value);
    }
    bind_u128(&mut digest, b"min-opset", u128::from(MIN_ONNX_OPSET));
    bind_u128(&mut digest, b"max-opset", u128::from(MAX_ONNX_OPSET));
    bind_u128(
        &mut digest,
        b"max-deadline-nanoseconds",
        MAX_INFERENCE_DEADLINE.as_nanos(),
    );
    for (name, shape) in [
        (b"input-shape".as_slice(), input_shape),
        (b"output-shape".as_slice(), output_shape),
    ] {
        bind_usize(&mut digest, name, shape.len());
        for dimension in shape {
            bind_usize(&mut digest, b"dimension", *dimension);
        }
    }
    if let Some(output_semantics) = output_semantics {
        bind_u128(
            &mut digest,
            b"output-semantics",
            u128::from(match output_semantics {
                ModelOutputSemantics::Regression => 1_u8,
                ModelOutputSemantics::BinaryProbability => 2_u8,
            }),
        );
    }
    bind_u128(&mut digest, b"deadline-nanoseconds", deadline.as_nanos());
    bind_u128(
        &mut digest,
        b"fallback",
        u128::from(match fallback {
            OnnxFallbackPolicy::NoAction => 1_u8,
        }),
    );
    bind_usize(
        &mut digest,
        b"allowed-operator-count",
        ALLOWED_OPERATORS.len(),
    );
    for operator in ALLOWED_OPERATORS {
        bind_bytes(&mut digest, b"allowed-operator", operator.as_bytes());
    }
    digest.finalize().into()
}

fn bind_usize(digest: &mut Sha256, name: &[u8], value: usize) {
    bind_u128(
        digest,
        name,
        u128::try_from(value).map_or(u128::MAX, |value| value),
    );
}

fn bind_u128(digest: &mut Sha256, name: &[u8], value: u128) {
    bind_bytes(digest, b"field", name);
    digest.update(value.to_be_bytes());
}

fn bind_bytes(digest: &mut Sha256, name: &[u8], value: &[u8]) {
    digest.update(
        u128::try_from(name.len())
            .map_or(u128::MAX, |length| length)
            .to_be_bytes(),
    );
    digest.update(name);
    digest.update(
        u128::try_from(value.len())
            .map_or(u128::MAX, |length| length)
            .to_be_bytes(),
    );
    digest.update(value);
}
