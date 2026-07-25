"""Deterministic dependency-free encoding for the two supported fitted ONNX families."""

from __future__ import annotations

import math
import struct
from typing import Sequence


ONNX_OPSET = 13
MAX_FEATURES = 1_024


class OnnxEncodingError(ValueError):
    """The fitted model cannot be represented by the closed ONNX graph grammar."""


def encode_fitted_model(
    weights: Sequence[float],
    bias: float,
    *,
    model_kind: str,
) -> bytes:
    """Encode one static `[1, features] -> [1, 1]` Gemm graph.

    Logistic models append the code-owned Sigmoid node. No caller-controlled graph,
    operator, domain, attribute, external tensor, or shape is accepted.
    """

    if model_kind not in {"linear", "logistic"}:
        raise OnnxEncodingError("ONNX model family is unsupported")
    if not isinstance(weights, (tuple, list)) or not 1 <= len(weights) <= MAX_FEATURES:
        raise OnnxEncodingError("ONNX feature count is invalid")
    quantized_weights, quantized_bias = quantize_fitted_model(weights, bias)
    weight_data = b"".join(_encode_f32(value) for value in quantized_weights)
    bias_data = _encode_f32(quantized_bias)
    feature_count = len(weights)

    nodes = [
        _node(
            name="fitted-linear-score",
            op_type="Gemm",
            inputs=("X", "W", "B"),
            outputs=("Z" if model_kind == "logistic" else "Y",),
        )
    ]
    if model_kind == "logistic":
        nodes.append(
            _node(
                name="fitted-logistic-probability",
                op_type="Sigmoid",
                inputs=("Z",),
                outputs=("Y",),
            )
        )

    graph = b"".join(_message(1, node) for node in nodes)
    graph += _string(2, "market-squawk-fitted-model")
    graph += _message(5, _tensor("W", (feature_count, 1), weight_data))
    graph += _message(5, _tensor("B", (1,), bias_data))
    graph += _message(11, _value_info("X", (1, feature_count)))
    graph += _message(12, _value_info("Y", (1, 1)))

    model = _varint_field(1, 8)
    model += _string(2, "market-squawk")
    model += _string(3, "sealed-fitted-v1")
    model += _message(7, graph)
    model += _message(8, _varint_field(2, ONNX_OPSET))
    return model


def quantize_fitted_model(
    weights: Sequence[float], bias: float
) -> tuple[tuple[float, ...], float]:
    """Return the exact finite float32 tensor values encoded into the candidate."""

    if not isinstance(weights, (tuple, list)) or not 1 <= len(weights) <= MAX_FEATURES:
        raise OnnxEncodingError("ONNX feature count is invalid")
    return tuple(quantize_float32(value) for value in weights), quantize_float32(bias)


def quantize_float32(value: float) -> float:
    """Apply the candidate/runtime float32 conversion with overflow rejection."""

    return struct.unpack("<f", _encode_f32(value))[0]


def _node(
    *,
    name: str,
    op_type: str,
    inputs: tuple[str, ...],
    outputs: tuple[str, ...],
) -> bytes:
    value = b"".join(_string(1, item) for item in inputs)
    value += b"".join(_string(2, item) for item in outputs)
    value += _string(3, name)
    value += _string(4, op_type)
    return value


def _tensor(name: str, dimensions: tuple[int, ...], raw_data: bytes) -> bytes:
    value = b"".join(_varint_field(1, dimension) for dimension in dimensions)
    value += _varint_field(2, 1)
    value += _string(8, name)
    value += _bytes_field(9, raw_data)
    return value


def _value_info(name: str, dimensions: tuple[int, ...]) -> bytes:
    shape = b"".join(
        _message(1, _varint_field(1, dimension)) for dimension in dimensions
    )
    tensor_type = _varint_field(1, 1) + _message(2, shape)
    return _string(1, name) + _message(2, _message(1, tensor_type))


def _encode_f32(value: float) -> bytes:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OnnxEncodingError("ONNX tensor value is not numeric")
    converted = float(value)
    if not math.isfinite(converted):
        raise OnnxEncodingError("ONNX tensor value is nonfinite")
    try:
        encoded = struct.pack("<f", converted)
    except (OverflowError, struct.error) as error:
        raise OnnxEncodingError("ONNX tensor value exceeds float32") from error
    if not math.isfinite(struct.unpack("<f", encoded)[0]):
        raise OnnxEncodingError("ONNX tensor value exceeds float32")
    return encoded


def _message(field: int, value: bytes) -> bytes:
    return _bytes_field(field, value)


def _string(field: int, value: str) -> bytes:
    return _bytes_field(field, value.encode("ascii"))


def _bytes_field(field: int, value: bytes) -> bytes:
    return _varint((field << 3) | 2) + _varint(len(value)) + value


def _varint_field(field: int, value: int) -> bytes:
    return _varint(field << 3) + _varint(value)


def _varint(value: int) -> bytes:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise OnnxEncodingError("ONNX integer field is invalid")
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)
