//! Allocation-free structural admission for the pinned ONNX protobuf schema.
//!
//! The decoded-heap charge covers the generated Prost object model rather than estimating from
//! wire bytes alone. [`ModelProto`] charges its complete inline representation up front. Repeated
//! messages charge twice their concrete element width for `Vec` capacity growth and are then
//! scanned recursively; repeated strings and bytes additionally charge their payload; packed and
//! unpacked numerics charge twice their decoded scalar width. Singular message, string, and bytes
//! handles are already inline in their charged parent, so only their nested fields or payload are
//! added. The fixed field-count and depth ceilings separately bound allocator bookkeeping and stack
//! use. Unknown fields are discarded by Prost and this scanner skips them without allocating.

use std::mem::size_of;

use tract_onnx::pb::{
    AttributeProto, FunctionProto, GraphProto, ModelProto, NodeProto, OperatorSetIdProto,
    SparseTensorProto, StringStringEntryProto, TensorAnnotation, TensorProto, TensorShapeProto,
    TrainingInfoProto, TypeProto, ValueInfoProto, tensor_proto, tensor_shape_proto, type_proto,
};

use super::policy::MAX_ONNX_MODEL_BYTES;
use super::policy::{MAX_ONNX_NODES, MAX_ONNX_REQUEST_ELEMENTS, MAX_ONNX_TENSORS, OnnxPolicyError};

const MAX_SCHEMA_TAG: usize = 25;
// The admitted non-recursive schema reaches depth five. Three additional levels preserve forward
// scalar/message compatibility without admitting recursively nested GraphProto allocation.
const MAX_NESTING_DEPTH: usize = 8;
// Every protobuf field occupies at least a one-byte key and one-byte value or length. This is the
// maximum possible field count under the existing wire-byte ceiling, including unpacked numerics.
const MAX_WIRE_FIELDS: usize = MAX_ONNX_MODEL_BYTES.div_ceil(2);
// Prost container growth can approach twice the logical payload. Charging repeated containers at
// twice their element width inside four wire images leaves a conservative, finite decode ceiling.
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

    fn charge_repeated(
        &mut self,
        items: usize,
        element_bytes: usize,
    ) -> Result<(), OnnxPolicyError> {
        let bytes = items
            .checked_mul(element_bytes)
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or(OnnxPolicyError::ProtobufResourceLimit)?;
        self.charge(bytes)
    }
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
        let prior = items_by_tag
            .get_mut(tag)
            .ok_or(OnnxPolicyError::InvalidProtobuf)?;
        *prior = prior
            .checked_add(items)
            .filter(|items| *items <= spec.max_items)
            .ok_or(spec.limit_error)?;
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
            budget.charge(payload.len())?;
            Ok(1)
        }
        ValueKind::Bytes => {
            require_wire(wire_type, 2)?;
            let payload = take_length_delimited(bytes, cursor)?;
            budget.charge(payload.len())?;
            Ok(1)
        }
        ValueKind::Message(kind) => {
            require_wire(wire_type, 2)?;
            let payload = take_length_delimited(bytes, cursor)?;
            scan_message(kind, payload, depth + 1, budget)?;
            Ok(1)
        }
        ValueKind::RepeatedVarint { element_bytes } => {
            let items = if wire_type == 2 {
                let payload = take_length_delimited(bytes, cursor)?;
                count_varints(payload)?
            } else {
                require_wire(wire_type, 0)?;
                read_varint(bytes, cursor)?;
                1
            };
            budget.charge_repeated(items, element_bytes)?;
            Ok(items)
        }
        ValueKind::RepeatedFixed32 => {
            let items = scan_repeated_fixed(bytes, cursor, wire_type, 5, 4)?;
            budget.charge_repeated(items, 4)?;
            Ok(items)
        }
        ValueKind::RepeatedFixed64 => {
            let items = scan_repeated_fixed(bytes, cursor, wire_type, 1, 8)?;
            budget.charge_repeated(items, 8)?;
            Ok(items)
        }
        ValueKind::RepeatedString => {
            require_wire(wire_type, 2)?;
            let payload = take_length_delimited(bytes, cursor)?;
            std::str::from_utf8(payload).map_err(|_| OnnxPolicyError::InvalidProtobuf)?;
            budget.charge_repeated(1, size_of::<String>())?;
            budget.charge(payload.len())?;
            Ok(1)
        }
        ValueKind::RepeatedBytes => {
            require_wire(wire_type, 2)?;
            let payload = take_length_delimited(bytes, cursor)?;
            budget.charge_repeated(1, size_of::<Vec<u8>>())?;
            budget.charge(payload.len())?;
            Ok(1)
        }
        ValueKind::RepeatedMessage(kind) => {
            require_wire(wire_type, 2)?;
            let payload = take_length_delimited(bytes, cursor)?;
            budget.charge_repeated(1, message_size(kind))?;
            scan_message(kind, payload, depth + 1, budget)?;
            Ok(1)
        }
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
