//! Allocation-free structural admission for the pinned ONNX protobuf schema.
//!
//! The decoded-heap charge covers the generated Prost object model rather than estimating from
//! wire bytes alone. [`ModelProto`] charges its complete inline representation up front. Each
//! repeated field charges one monotonic Rust 1.97.1 `Vec` allocation-growth peak across every
//! occurrence and packed segment, including minimum non-zero capacity and old-plus-new growth.
//! String and byte payloads use the same allocation model. Singular message, string, and bytes
//! handles are already inline in their charged parent, so only their nested fields or payload are
//! added. The fixed field-count and depth ceilings separately bound allocator bookkeeping and stack
//! use. Unknown fields are discarded by Prost and this scanner skips them without allocating.

use std::mem::size_of;

use sha2::{Digest, Sha256};
use tract_onnx::pb::{
    AttributeProto, FunctionProto, GraphProto, ModelProto, NodeProto, OperatorSetIdProto,
    SparseTensorProto, StringStringEntryProto, TensorAnnotation, TensorProto, TensorShapeProto,
    TrainingInfoProto, TypeProto, ValueInfoProto, tensor_proto, tensor_shape_proto, type_proto,
};

use super::policy::MAX_ONNX_MODEL_BYTES;
use super::policy::{MAX_ONNX_NODES, MAX_ONNX_REQUEST_ELEMENTS, MAX_ONNX_TENSORS, OnnxPolicyError};

const MAX_SCHEMA_TAG: usize = 25;
const SCANNER_REVISION: u32 = 3;
const RUST_VEC_BYTE_MIN_CAPACITY: usize = 8;
const RUST_VEC_MODERATE_MIN_CAPACITY: usize = 4;
const RUST_VEC_LARGE_MIN_CAPACITY: usize = 1;
const RUST_VEC_MODERATE_ELEMENT_MAX_BYTES: usize = 1_024;
const RUST_VEC_GROWTH_FACTOR: usize = 2;
// The admitted non-recursive schema reaches depth five. Three additional levels preserve forward
// scalar/message compatibility without admitting recursively nested GraphProto allocation.
const MAX_NESTING_DEPTH: usize = 8;
// Every protobuf field occupies at least a one-byte key and one-byte value or length. This is the
// maximum possible field count under the existing wire-byte ceiling, including unpacked numerics.
const MAX_WIRE_FIELDS: usize = MAX_ONNX_MODEL_BYTES.div_ceil(2);
// The scanner charges each container's monotonic old-plus-new growth peak. Payload containers are
// independently charged, preserving a finite, conservative admission ceiling.
const MAX_DECODED_HEAP_BYTES: usize = MAX_ONNX_MODEL_BYTES * 4;
const MAX_GRAPH_VALUES: usize = MAX_ONNX_TENSORS;
const MAX_ONNX_RANK: usize = 8;
const MAX_UNSUPPORTED_RECORDS: usize = 1;

#[derive(Clone, Copy, Debug)]
enum MessageKind {
    Attribute,
    Dimension,
    Function,
    Graph,
    Model,
    Node,
    OperatorSet,
    Segment,
    SparseTensor,
    StringEntry,
    Tensor,
    TensorAnnotation,
    TensorShape,
    TrainingInfo,
    Type,
    TypeTensor,
    ValueInfo,
}

impl MessageKind {
    const ALL: [Self; 17] = [
        Self::Attribute,
        Self::Dimension,
        Self::Function,
        Self::Graph,
        Self::Model,
        Self::Node,
        Self::OperatorSet,
        Self::Segment,
        Self::SparseTensor,
        Self::StringEntry,
        Self::Tensor,
        Self::TensorAnnotation,
        Self::TensorShape,
        Self::TrainingInfo,
        Self::Type,
        Self::TypeTensor,
        Self::ValueInfo,
    ];

    const fn wire_id(self) -> u8 {
        match self {
            Self::Attribute => 1,
            Self::Dimension => 2,
            Self::Function => 3,
            Self::Graph => 4,
            Self::Model => 5,
            Self::Node => 6,
            Self::OperatorSet => 7,
            Self::Segment => 8,
            Self::SparseTensor => 9,
            Self::StringEntry => 10,
            Self::Tensor => 11,
            Self::TensorAnnotation => 12,
            Self::TensorShape => 13,
            Self::TrainingInfo => 14,
            Self::Type => 15,
            Self::TypeTensor => 16,
            Self::ValueInfo => 17,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ValueKind {
    Varint,
    Fixed32,
    String,
    Bytes,
    Message(MessageKind),
    RepeatedVarint { element_bytes: usize },
    RepeatedFixed32,
    RepeatedFixed64,
    RepeatedString,
    RepeatedBytes,
    RepeatedMessage(MessageKind),
}

#[derive(Clone, Copy, Debug)]
struct FieldSpec {
    value: ValueKind,
    max_items: usize,
    limit_error: OnnxPolicyError,
}

impl FieldSpec {
    const fn singular(value: ValueKind) -> Self {
        Self {
            value,
            max_items: 1,
            limit_error: OnnxPolicyError::ProtobufResourceLimit,
        }
    }

    const fn repeated(value: ValueKind, max_items: usize) -> Self {
        Self {
            value,
            max_items,
            limit_error: OnnxPolicyError::ProtobufResourceLimit,
        }
    }

    const fn repeated_with_error(
        value: ValueKind,
        max_items: usize,
        limit_error: OnnxPolicyError,
    ) -> Self {
        Self {
            value,
            max_items,
            limit_error,
        }
    }
}

#[derive(Debug)]
struct Budget {
    fields: usize,
    decoded_heap_bytes: usize,
}

impl Budget {
    fn new() -> Result<Self, OnnxPolicyError> {
        let mut budget = Self {
            fields: 0,
            decoded_heap_bytes: 0,
        };
        budget.charge(size_of::<ModelProto>())?;
        Ok(budget)
    }

    fn field(&mut self) -> Result<(), OnnxPolicyError> {
        self.fields = self
            .fields
            .checked_add(1)
            .filter(|count| *count <= MAX_WIRE_FIELDS)
            .ok_or(OnnxPolicyError::ProtobufResourceLimit)?;
        Ok(())
    }

    fn charge(&mut self, bytes: usize) -> Result<(), OnnxPolicyError> {
        self.decoded_heap_bytes = self
            .decoded_heap_bytes
            .checked_add(bytes)
            .filter(|total| *total <= MAX_DECODED_HEAP_BYTES)
            .ok_or(OnnxPolicyError::ProtobufResourceLimit)?;
        Ok(())
    }

    fn charge_vec_peak(
        &mut self,
        items: usize,
        element_bytes: usize,
    ) -> Result<(), OnnxPolicyError> {
        self.charge(vec_peak_allocation(items, element_bytes)?)
    }

    fn charge_vec_growth(
        &mut self,
        prior_items: usize,
        new_items: usize,
        element_bytes: usize,
    ) -> Result<(), OnnxPolicyError> {
        let prior_peak = vec_peak_allocation(prior_items, element_bytes)?;
        let new_peak = vec_peak_allocation(new_items, element_bytes)?;
        self.charge(
            new_peak
                .checked_sub(prior_peak)
                .ok_or(OnnxPolicyError::ProtobufResourceLimit)?,
        )
    }
}

fn vec_peak_allocation(items: usize, element_bytes: usize) -> Result<usize, OnnxPolicyError> {
    if items == 0 || element_bytes == 0 {
        return Ok(0);
    }
    let minimum_capacity = if element_bytes == 1 {
        RUST_VEC_BYTE_MIN_CAPACITY
    } else if element_bytes <= RUST_VEC_MODERATE_ELEMENT_MAX_BYTES {
        RUST_VEC_MODERATE_MIN_CAPACITY
    } else {
        RUST_VEC_LARGE_MIN_CAPACITY
    };
    let mut capacity = 0_usize;
    let mut peak_elements = 0_usize;
    while capacity < items {
        let required = capacity
            .checked_add(1)
            .ok_or(OnnxPolicyError::ProtobufResourceLimit)?;
        let next = capacity
            .checked_mul(RUST_VEC_GROWTH_FACTOR)
            .map(|doubled| doubled.max(required).max(minimum_capacity))
            .ok_or(OnnxPolicyError::ProtobufResourceLimit)?;
        peak_elements = peak_elements.max(
            capacity
                .checked_add(next)
                .ok_or(OnnxPolicyError::ProtobufResourceLimit)?,
        );
        capacity = next;
    }
    peak_elements
        .checked_mul(element_bytes)
        .ok_or(OnnxPolicyError::ProtobufResourceLimit)
}

pub(super) fn prescan(bytes: &[u8]) -> Result<(), OnnxPolicyError> {
    let mut budget = Budget::new()?;
    scan_message(MessageKind::Model, bytes, 0, &mut budget)
}

fn scan_message(
    message: MessageKind,
    bytes: &[u8],
    depth: usize,
    budget: &mut Budget,
) -> Result<(), OnnxPolicyError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(OnnxPolicyError::ProtobufResourceLimit);
    }
    let mut cursor = 0;
    let mut items_by_tag = [0_usize; MAX_SCHEMA_TAG + 1];
    while cursor < bytes.len() {
        budget.field()?;
        let key = read_varint(bytes, &mut cursor)?;
        let tag = usize::try_from(key >> 3).map_err(|_| OnnxPolicyError::InvalidProtobuf)?;
        if tag == 0 || tag > 0x1fff_ffff {
            return Err(OnnxPolicyError::InvalidProtobuf);
        }
        let wire_type = u8::try_from(key & 0x07).map_err(|_| OnnxPolicyError::InvalidProtobuf)?;
        let Some(spec) = field_spec(message, tag) else {
            skip_unknown(bytes, &mut cursor, wire_type)?;
            continue;
        };
        let items = scan_value(spec.value, bytes, &mut cursor, wire_type, depth, budget)?;
        let prior = *items_by_tag
            .get(tag)
            .ok_or(OnnxPolicyError::InvalidProtobuf)?;
        let total = prior
            .checked_add(items)
            .filter(|items| *items <= spec.max_items)
            .ok_or(spec.limit_error)?;
        if let Some(element_bytes) = repeated_element_bytes(spec.value) {
            budget.charge_vec_growth(prior, total, element_bytes)?;
        }
        items_by_tag[tag] = total;
    }
    Ok(())
}

fn scan_value(
    value: ValueKind,
    bytes: &[u8],
    cursor: &mut usize,
    wire_type: u8,
    depth: usize,
    budget: &mut Budget,
) -> Result<usize, OnnxPolicyError> {
    match value {
        ValueKind::Varint => {
            require_wire(wire_type, 0)?;
            read_varint(bytes, cursor)?;
            Ok(1)
        }
        ValueKind::Fixed32 => {
            require_wire(wire_type, 5)?;
            take_fixed(bytes, cursor, 4)?;
            Ok(1)
        }
        ValueKind::String => {
            require_wire(wire_type, 2)?;
            let payload = take_length_delimited(bytes, cursor)?;
            std::str::from_utf8(payload).map_err(|_| OnnxPolicyError::InvalidProtobuf)?;
            budget.charge_vec_peak(payload.len(), 1)?;
            Ok(1)
        }
        ValueKind::Bytes => {
            require_wire(wire_type, 2)?;
            let payload = take_length_delimited(bytes, cursor)?;
            budget.charge_vec_peak(payload.len(), 1)?;
            Ok(1)
        }
        ValueKind::Message(kind) => {
            require_wire(wire_type, 2)?;
            let payload = take_length_delimited(bytes, cursor)?;
            scan_message(kind, payload, depth + 1, budget)?;
            Ok(1)
        }
        ValueKind::RepeatedVarint { .. } => {
            let items = if wire_type == 2 {
                let payload = take_length_delimited(bytes, cursor)?;
                count_varints(payload)?
            } else {
                require_wire(wire_type, 0)?;
                read_varint(bytes, cursor)?;
                1
            };
            Ok(items)
        }
        ValueKind::RepeatedFixed32 => {
            let items = scan_repeated_fixed(bytes, cursor, wire_type, 5, 4)?;
            Ok(items)
        }
        ValueKind::RepeatedFixed64 => {
            let items = scan_repeated_fixed(bytes, cursor, wire_type, 1, 8)?;
            Ok(items)
        }
        ValueKind::RepeatedString => {
            require_wire(wire_type, 2)?;
            let payload = take_length_delimited(bytes, cursor)?;
            std::str::from_utf8(payload).map_err(|_| OnnxPolicyError::InvalidProtobuf)?;
            budget.charge_vec_peak(payload.len(), 1)?;
            Ok(1)
        }
        ValueKind::RepeatedBytes => {
            require_wire(wire_type, 2)?;
            let payload = take_length_delimited(bytes, cursor)?;
            budget.charge_vec_peak(payload.len(), 1)?;
            Ok(1)
        }
        ValueKind::RepeatedMessage(kind) => {
            require_wire(wire_type, 2)?;
            let payload = take_length_delimited(bytes, cursor)?;
            scan_message(kind, payload, depth + 1, budget)?;
            Ok(1)
        }
    }
}

fn repeated_element_bytes(value: ValueKind) -> Option<usize> {
    match value {
        ValueKind::RepeatedVarint { element_bytes } => Some(element_bytes),
        ValueKind::RepeatedFixed32 => Some(size_of::<f32>()),
        ValueKind::RepeatedFixed64 => Some(size_of::<f64>()),
        ValueKind::RepeatedString => Some(size_of::<String>()),
        ValueKind::RepeatedBytes => Some(size_of::<Vec<u8>>()),
        ValueKind::RepeatedMessage(kind) => Some(message_size(kind)),
        ValueKind::Varint
        | ValueKind::Fixed32
        | ValueKind::String
        | ValueKind::Bytes
        | ValueKind::Message(_) => None,
    }
}

fn scan_repeated_fixed(
    bytes: &[u8],
    cursor: &mut usize,
    wire_type: u8,
    scalar_wire_type: u8,
    element_bytes: usize,
) -> Result<usize, OnnxPolicyError> {
    if wire_type == 2 {
        let payload = take_length_delimited(bytes, cursor)?;
        if !payload.len().is_multiple_of(element_bytes) {
            return Err(OnnxPolicyError::InvalidProtobuf);
        }
        Ok(payload.len() / element_bytes)
    } else {
        require_wire(wire_type, scalar_wire_type)?;
        take_fixed(bytes, cursor, element_bytes)?;
        Ok(1)
    }
}

fn count_varints(bytes: &[u8]) -> Result<usize, OnnxPolicyError> {
    let mut cursor = 0;
    let mut count = 0_usize;
    while cursor < bytes.len() {
        read_varint(bytes, &mut cursor)?;
        count = count
            .checked_add(1)
            .ok_or(OnnxPolicyError::ProtobufResourceLimit)?;
    }
    Ok(count)
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, OnnxPolicyError> {
    let mut value = 0_u64;
    for index in 0..10 {
        let byte = *bytes.get(*cursor).ok_or(OnnxPolicyError::InvalidProtobuf)?;
        *cursor = cursor
            .checked_add(1)
            .ok_or(OnnxPolicyError::InvalidProtobuf)?;
        if index == 9 && byte > 1 {
            return Err(OnnxPolicyError::InvalidProtobuf);
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(OnnxPolicyError::InvalidProtobuf)
}

fn take_fixed<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'a [u8], OnnxPolicyError> {
    let end = cursor
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or(OnnxPolicyError::InvalidProtobuf)?;
    let value = &bytes[*cursor..end];
    *cursor = end;
    Ok(value)
}

fn take_length_delimited<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
) -> Result<&'a [u8], OnnxPolicyError> {
    let length = usize::try_from(read_varint(bytes, cursor)?)
        .map_err(|_| OnnxPolicyError::InvalidProtobuf)?;
    take_fixed(bytes, cursor, length)
}

fn require_wire(actual: u8, expected: u8) -> Result<(), OnnxPolicyError> {
    (actual == expected)
        .then_some(())
        .ok_or(OnnxPolicyError::InvalidProtobuf)
}

fn skip_unknown(bytes: &[u8], cursor: &mut usize, wire_type: u8) -> Result<(), OnnxPolicyError> {
    match wire_type {
        0 => {
            read_varint(bytes, cursor)?;
        }
        1 => {
            take_fixed(bytes, cursor, 8)?;
        }
        2 => {
            take_length_delimited(bytes, cursor)?;
        }
        5 => {
            take_fixed(bytes, cursor, 4)?;
        }
        _ => return Err(OnnxPolicyError::InvalidProtobuf),
    }
    Ok(())
}

fn message_size(message: MessageKind) -> usize {
    match message {
        MessageKind::Attribute => size_of::<AttributeProto>(),
        MessageKind::Dimension => size_of::<tensor_shape_proto::Dimension>(),
        MessageKind::Function => size_of::<FunctionProto>(),
        MessageKind::Graph => size_of::<GraphProto>(),
        MessageKind::Model => size_of::<ModelProto>(),
        MessageKind::Node => size_of::<NodeProto>(),
        MessageKind::OperatorSet => size_of::<OperatorSetIdProto>(),
        MessageKind::Segment => size_of::<tensor_proto::Segment>(),
        MessageKind::SparseTensor => size_of::<SparseTensorProto>(),
        MessageKind::StringEntry => size_of::<StringStringEntryProto>(),
        MessageKind::Tensor => size_of::<TensorProto>(),
        MessageKind::TensorAnnotation => size_of::<TensorAnnotation>(),
        MessageKind::TensorShape => size_of::<TensorShapeProto>(),
        MessageKind::TrainingInfo => size_of::<TrainingInfoProto>(),
        MessageKind::Type => size_of::<TypeProto>(),
        MessageKind::TypeTensor => size_of::<type_proto::Tensor>(),
        MessageKind::ValueInfo => size_of::<ValueInfoProto>(),
    }
}

pub(super) fn admission_semantics_digest() -> [u8; 32] {
    let mut digest = Sha256::new();
    bind_bytes(
        &mut digest,
        b"namespace",
        b"market-squawk/onnx-wire-admission/v3",
    );
    bind_u128(
        &mut digest,
        b"scanner-revision",
        u128::from(SCANNER_REVISION),
    );
    bind_bytes(
        &mut digest,
        b"rust-vec-semantics",
        b"rustc-1.97.1/alloc/raw_vec/grow_amortized",
    );
    bind_bytes(
        &mut digest,
        b"tract-onnx-crate",
        b"0.23.4/a3215dd27bddd2a041a20fee750013b400135186d3485501c9274c755b19ceb0",
    );
    bind_bytes(
        &mut digest,
        b"onnx-proto3",
        b"12ec4e4a1ec0a707827e0fde751512ce849b41600ed3edc68563a2faa7220fae",
    );
    bind_bytes(
        &mut digest,
        b"generated-onnx-prost",
        b"2ab8a70959912929c568dba8536840034317894cd430c1defd28c099ca72b8d2",
    );
    bind_bytes(
        &mut digest,
        b"prost-decoder",
        b"prost-0.14.3/d2ea70524a2f82d518bce41317d0fae74151505651af45faf1ffbd6fd33f0568",
    );
    for (name, value) in [
        (b"max-schema-tag".as_slice(), MAX_SCHEMA_TAG),
        (b"max-nesting-depth".as_slice(), MAX_NESTING_DEPTH),
        (b"max-wire-fields".as_slice(), MAX_WIRE_FIELDS),
        (b"max-decoded-heap-bytes".as_slice(), MAX_DECODED_HEAP_BYTES),
        (b"max-graph-values".as_slice(), MAX_GRAPH_VALUES),
        (b"max-onnx-rank".as_slice(), MAX_ONNX_RANK),
        (
            b"max-unsupported-records".as_slice(),
            MAX_UNSUPPORTED_RECORDS,
        ),
        (b"max-onnx-model-bytes".as_slice(), MAX_ONNX_MODEL_BYTES),
        (b"max-onnx-nodes".as_slice(), MAX_ONNX_NODES),
        (b"max-onnx-tensors".as_slice(), MAX_ONNX_TENSORS),
        (
            b"max-onnx-request-elements".as_slice(),
            MAX_ONNX_REQUEST_ELEMENTS,
        ),
        (
            b"vec-byte-min-capacity".as_slice(),
            RUST_VEC_BYTE_MIN_CAPACITY,
        ),
        (
            b"vec-moderate-min-capacity".as_slice(),
            RUST_VEC_MODERATE_MIN_CAPACITY,
        ),
        (
            b"vec-large-min-capacity".as_slice(),
            RUST_VEC_LARGE_MIN_CAPACITY,
        ),
        (
            b"vec-moderate-element-max-bytes".as_slice(),
            RUST_VEC_MODERATE_ELEMENT_MAX_BYTES,
        ),
        (b"vec-growth-factor".as_slice(), RUST_VEC_GROWTH_FACTOR),
        (b"target-usize-bytes".as_slice(), size_of::<usize>()),
        (b"target-string-bytes".as_slice(), size_of::<String>()),
        (b"target-byte-vec-bytes".as_slice(), size_of::<Vec<u8>>()),
    ] {
        bind_usize(&mut digest, name, value);
    }
    bind_usize(&mut digest, b"message-kind-count", MessageKind::ALL.len());
    for message in MessageKind::ALL {
        digest.update([message.wire_id()]);
        bind_usize(&mut digest, b"message-layout-bytes", message_size(message));
        for tag in 1..=MAX_SCHEMA_TAG {
            bind_usize(&mut digest, b"tag", tag);
            match field_spec(message, tag) {
                Some(spec) => {
                    digest.update([1]);
                    bind_value_kind(&mut digest, spec.value);
                    bind_usize(&mut digest, b"max-items", spec.max_items);
                    digest.update([policy_error_id(spec.limit_error)]);
                }
                None => digest.update([0]),
            }
        }
    }
    digest.finalize().into()
}

fn bind_value_kind(digest: &mut Sha256, value: ValueKind) {
    match value {
        ValueKind::Varint => digest.update([1]),
        ValueKind::Fixed32 => digest.update([2]),
        ValueKind::String => digest.update([3]),
        ValueKind::Bytes => digest.update([4]),
        ValueKind::Message(message) => digest.update([5, message.wire_id()]),
        ValueKind::RepeatedVarint { element_bytes } => {
            digest.update([6]);
            bind_usize(digest, b"element-bytes", element_bytes);
        }
        ValueKind::RepeatedFixed32 => digest.update([7]),
        ValueKind::RepeatedFixed64 => digest.update([8]),
        ValueKind::RepeatedString => digest.update([9]),
        ValueKind::RepeatedBytes => digest.update([10]),
        ValueKind::RepeatedMessage(message) => digest.update([11, message.wire_id()]),
    }
}

const fn policy_error_id(error: OnnxPolicyError) -> u8 {
    match error {
        OnnxPolicyError::InvalidPolicy => 1,
        OnnxPolicyError::ModelByteLimit => 2,
        OnnxPolicyError::ModelDigestMismatch => 3,
        OnnxPolicyError::InvalidProtobuf => 4,
        OnnxPolicyError::ProtobufResourceLimit => 5,
        OnnxPolicyError::UnsupportedOpset => 6,
        OnnxPolicyError::CustomDomain => 7,
        OnnxPolicyError::DisallowedOperator => 8,
        OnnxPolicyError::ExternalData => 9,
        OnnxPolicyError::DynamicShape => 10,
        OnnxPolicyError::ShapeMismatch => 11,
        OnnxPolicyError::NodeLimit => 12,
        OnnxPolicyError::TensorLimit => 13,
        OnnxPolicyError::ElementLimit => 14,
        OnnxPolicyError::NonFiniteTensor => 15,
        OnnxPolicyError::UnsupportedGraphState => 16,
    }
}

fn bind_usize(digest: &mut Sha256, name: &[u8], value: usize) {
    bind_u128(digest, name, value as u128);
}

fn bind_u128(digest: &mut Sha256, name: &[u8], value: u128) {
    bind_bytes(digest, b"field", name);
    digest.update(value.to_be_bytes());
}

fn bind_bytes(digest: &mut Sha256, name: &[u8], value: &[u8]) {
    digest.update((name.len() as u128).to_be_bytes());
    digest.update(name);
    digest.update((value.len() as u128).to_be_bytes());
    digest.update(value);
}

#[allow(
    clippy::too_many_lines,
    reason = "the complete pinned ONNX wire schema stays auditable"
)]
fn field_spec(message: MessageKind, tag: usize) -> Option<FieldSpec> {
    let singular = FieldSpec::singular;
    let repeated = FieldSpec::repeated;
    let repeated_with_error = FieldSpec::repeated_with_error;
    match (message, tag) {
        (MessageKind::Model, 1 | 5) => Some(singular(ValueKind::Varint)),
        (MessageKind::Model, 2 | 3 | 4 | 6) => Some(singular(ValueKind::String)),
        (MessageKind::Model, 7) => Some(singular(ValueKind::Message(MessageKind::Graph))),
        (MessageKind::Model, 8) => Some(repeated_with_error(
            ValueKind::RepeatedMessage(MessageKind::OperatorSet),
            1,
            OnnxPolicyError::UnsupportedOpset,
        )),
        (MessageKind::Model, 14) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::StringEntry),
            MAX_WIRE_FIELDS,
        )),
        (MessageKind::Model, 20) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::TrainingInfo),
            MAX_UNSUPPORTED_RECORDS,
        )),
        (MessageKind::Model, 25) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::Function),
            MAX_UNSUPPORTED_RECORDS,
        )),

        (MessageKind::Graph, 1) => Some(repeated_with_error(
            ValueKind::RepeatedMessage(MessageKind::Node),
            MAX_ONNX_NODES,
            OnnxPolicyError::NodeLimit,
        )),
        (MessageKind::Graph, 2 | 10) => Some(singular(ValueKind::String)),
        (MessageKind::Graph, 5) => Some(repeated_with_error(
            ValueKind::RepeatedMessage(MessageKind::Tensor),
            MAX_GRAPH_VALUES,
            OnnxPolicyError::TensorLimit,
        )),
        (MessageKind::Graph, 15) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::SparseTensor),
            MAX_UNSUPPORTED_RECORDS,
        )),
        (MessageKind::Graph, 11..=13) => Some(repeated_with_error(
            ValueKind::RepeatedMessage(MessageKind::ValueInfo),
            MAX_GRAPH_VALUES,
            OnnxPolicyError::TensorLimit,
        )),
        (MessageKind::Graph, 14) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::TensorAnnotation),
            MAX_WIRE_FIELDS,
        )),

        (MessageKind::Node, 1 | 2) => Some(repeated(ValueKind::RepeatedString, MAX_WIRE_FIELDS)),
        (MessageKind::Node, 3 | 4 | 6 | 7) => Some(singular(ValueKind::String)),
        (MessageKind::Node, 5) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::Attribute),
            MAX_WIRE_FIELDS,
        )),

        (MessageKind::Attribute, 1 | 13 | 21) => Some(singular(ValueKind::String)),
        (MessageKind::Attribute, 20 | 3) => Some(singular(ValueKind::Varint)),
        (MessageKind::Attribute, 2) => Some(singular(ValueKind::Fixed32)),
        (MessageKind::Attribute, 4) => Some(singular(ValueKind::Bytes)),
        (MessageKind::Attribute, 5) => Some(singular(ValueKind::Message(MessageKind::Tensor))),
        (MessageKind::Attribute, 6) => Some(singular(ValueKind::Message(MessageKind::Graph))),
        (MessageKind::Attribute, 22) => {
            Some(singular(ValueKind::Message(MessageKind::SparseTensor)))
        }
        (MessageKind::Attribute, 7) => Some(repeated(ValueKind::RepeatedFixed32, MAX_WIRE_FIELDS)),
        (MessageKind::Attribute, 8) => Some(repeated(
            ValueKind::RepeatedVarint { element_bytes: 8 },
            MAX_WIRE_FIELDS,
        )),
        (MessageKind::Attribute, 9) => Some(repeated(ValueKind::RepeatedBytes, MAX_WIRE_FIELDS)),
        (MessageKind::Attribute, 10) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::Tensor),
            MAX_ONNX_TENSORS,
        )),
        (MessageKind::Attribute, 11) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::Graph),
            MAX_UNSUPPORTED_RECORDS,
        )),
        (MessageKind::Attribute, 23) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::SparseTensor),
            MAX_UNSUPPORTED_RECORDS,
        )),
        (MessageKind::Attribute, 15) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::Type),
            MAX_WIRE_FIELDS,
        )),

        (MessageKind::ValueInfo, 1 | 3) => Some(singular(ValueKind::String)),
        (MessageKind::ValueInfo, 2) => Some(singular(ValueKind::Message(MessageKind::Type))),
        (MessageKind::StringEntry, 1 | 2) => Some(singular(ValueKind::String)),
        (MessageKind::TensorAnnotation, 1) => Some(singular(ValueKind::String)),
        (MessageKind::TensorAnnotation, 2) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::StringEntry),
            MAX_WIRE_FIELDS,
        )),

        (MessageKind::TrainingInfo, 1 | 2) => {
            Some(singular(ValueKind::Message(MessageKind::Graph)))
        }
        (MessageKind::TrainingInfo, 3 | 4) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::StringEntry),
            MAX_WIRE_FIELDS,
        )),

        (MessageKind::Tensor, 1) => Some(repeated(
            ValueKind::RepeatedVarint { element_bytes: 8 },
            MAX_ONNX_RANK,
        )),
        (MessageKind::Tensor, 2 | 14) => Some(singular(ValueKind::Varint)),
        (MessageKind::Tensor, 3) => Some(singular(ValueKind::Message(MessageKind::Segment))),
        (MessageKind::Tensor, 4) => Some(repeated(
            ValueKind::RepeatedFixed32,
            MAX_ONNX_REQUEST_ELEMENTS,
        )),
        (MessageKind::Tensor, 5) => Some(repeated(
            ValueKind::RepeatedVarint { element_bytes: 4 },
            MAX_ONNX_REQUEST_ELEMENTS,
        )),
        (MessageKind::Tensor, 6) => Some(repeated(
            ValueKind::RepeatedBytes,
            MAX_ONNX_REQUEST_ELEMENTS,
        )),
        (MessageKind::Tensor, 7) => Some(repeated(
            ValueKind::RepeatedVarint { element_bytes: 8 },
            MAX_ONNX_REQUEST_ELEMENTS,
        )),
        (MessageKind::Tensor, 8 | 12) => Some(singular(ValueKind::String)),
        (MessageKind::Tensor, 9) => Some(singular(ValueKind::Bytes)),
        (MessageKind::Tensor, 10) => Some(repeated(
            ValueKind::RepeatedFixed64,
            MAX_ONNX_REQUEST_ELEMENTS,
        )),
        (MessageKind::Tensor, 11) => Some(repeated(
            ValueKind::RepeatedVarint { element_bytes: 8 },
            MAX_ONNX_REQUEST_ELEMENTS,
        )),
        (MessageKind::Tensor, 13) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::StringEntry),
            MAX_UNSUPPORTED_RECORDS,
        )),
        (MessageKind::Segment, 1 | 2) => Some(singular(ValueKind::Varint)),

        (MessageKind::SparseTensor, 1 | 2) => {
            Some(singular(ValueKind::Message(MessageKind::Tensor)))
        }
        (MessageKind::SparseTensor, 3) => Some(repeated(
            ValueKind::RepeatedVarint { element_bytes: 8 },
            MAX_ONNX_RANK,
        )),
        (MessageKind::TensorShape, 1) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::Dimension),
            MAX_ONNX_RANK,
        )),
        (MessageKind::Dimension, 1) => Some(singular(ValueKind::Varint)),
        (MessageKind::Dimension, 2 | 3) => Some(singular(ValueKind::String)),
        (MessageKind::Type, 1) => Some(singular(ValueKind::Message(MessageKind::TypeTensor))),
        (MessageKind::Type, 6) => Some(singular(ValueKind::String)),
        (MessageKind::TypeTensor, 1) => Some(singular(ValueKind::Varint)),
        (MessageKind::TypeTensor, 2) => {
            Some(singular(ValueKind::Message(MessageKind::TensorShape)))
        }
        (MessageKind::OperatorSet, 1) => Some(singular(ValueKind::String)),
        (MessageKind::OperatorSet, 2) => Some(singular(ValueKind::Varint)),

        (MessageKind::Function, 1 | 8 | 10) => Some(singular(ValueKind::String)),
        (MessageKind::Function, 4..=6) => {
            Some(repeated(ValueKind::RepeatedString, MAX_WIRE_FIELDS))
        }
        (MessageKind::Function, 7) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::Node),
            MAX_ONNX_NODES,
        )),
        (MessageKind::Function, 9) => Some(repeated(
            ValueKind::RepeatedMessage(MessageKind::OperatorSet),
            1,
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prescan_rejects_many_nested_minimum_capacity_vectors()
    -> Result<(), Box<dyn std::error::Error>> {
        let annotation_bytes = size_of::<TensorAnnotation>();
        let entry_bytes = size_of::<StringStringEntryProto>();
        assert!((1..=RUST_VEC_MODERATE_ELEMENT_MAX_BYTES).contains(&annotation_bytes));
        assert!((1..=RUST_VEC_MODERATE_ELEMENT_MAX_BYTES).contains(&entry_bytes));
        let per_annotation = entry_bytes
            .checked_mul(RUST_VEC_MODERATE_MIN_CAPACITY)
            .ok_or("nested allocation charge overflowed")?;
        let count = MAX_DECODED_HEAP_BYTES
            .checked_div(per_annotation)
            .and_then(|count| count.checked_add(1))
            .ok_or("nested annotation count overflowed")?;
        let outer_only = vec_peak_allocation(count, annotation_bytes)?;
        assert!(outer_only < MAX_DECODED_HEAP_BYTES);

        let mut graph = Vec::new();
        graph.try_reserve_exact(
            count
                .checked_mul(4)
                .ok_or("nested annotation wire size overflowed")?,
        )?;
        for _ in 0..count {
            graph.extend_from_slice(&[0x72, 0x02, 0x12, 0x00]);
        }
        let mut model = vec![0x3a];
        append_varint(
            u64::try_from(graph.len()).map_err(|_| "graph length does not fit protobuf")?,
            &mut model,
        );
        model.extend_from_slice(&graph);

        assert_eq!(prescan(&model), Err(OnnxPolicyError::ProtobufResourceLimit));
        Ok(())
    }

    #[test]
    fn repeated_message_larger_than_one_kibibyte_charges_cumulative_growth()
    -> Result<(), Box<dyn std::error::Error>> {
        const MESSAGE_COUNT: usize = 3;
        let element_bytes = size_of::<AttributeProto>();
        assert!(element_bytes > RUST_VEC_MODERATE_ELEMENT_MAX_BYTES);
        let mut node = Vec::new();
        node.try_reserve_exact(MESSAGE_COUNT * 2)?;
        for _ in 0..MESSAGE_COUNT {
            node.extend_from_slice(&[0x2a, 0x00]);
        }

        let mut budget = Budget {
            fields: 0,
            decoded_heap_bytes: 0,
        };
        scan_message(MessageKind::Node, &node, 0, &mut budget)?;

        assert_eq!(
            budget.decoded_heap_bytes,
            vec_peak_allocation(MESSAGE_COUNT, element_bytes)?
        );
        Ok(())
    }

    #[test]
    fn segmented_packed_numeric_growth_is_charged_as_one_container()
    -> Result<(), Box<dyn std::error::Error>> {
        const ITEMS_PER_SEGMENT: usize = 1_024;
        const SEGMENTS: usize = 3;
        let payload_bytes = ITEMS_PER_SEGMENT * size_of::<f32>();
        let mut tensor = Vec::new();
        for _ in 0..SEGMENTS {
            tensor.push(0x22);
            append_varint(u64::try_from(payload_bytes)?, &mut tensor);
            tensor.resize(tensor.len() + payload_bytes, 0);
        }

        let mut budget = Budget {
            fields: 0,
            decoded_heap_bytes: 0,
        };
        scan_message(MessageKind::Tensor, &tensor, 0, &mut budget)?;

        assert_eq!(
            budget.decoded_heap_bytes,
            vec_peak_allocation(ITEMS_PER_SEGMENT * SEGMENTS, size_of::<f32>())?
        );
        Ok(())
    }

    fn append_varint(mut value: u64, bytes: &mut Vec<u8>) {
        while value >= 0x80 {
            bytes.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        bytes.push(value as u8);
    }
}
