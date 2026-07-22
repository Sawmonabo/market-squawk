"""Deterministic bounded native linear/logistic training and validated export."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import random
import re
from typing import Any, Mapping, Sequence

from .bundle import BundleCandidate, BundleReceipt


MAX_TRAINING_ROWS = 100_000
MAX_FEATURES = 1_024
MAX_CELLS = 2_000_000
IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._-]{0,126}[a-z0-9]$|^[a-z0-9]$")
HEX = re.compile(r"^[0-9a-f]{64}$")


class TrainingValidationError(ValueError):
    """A reproducibility, input, resource, or finite-arithmetic contract failed."""


@dataclass(frozen=True)
class _FittedModel:
    weights: tuple[float, ...]
    bias: float
    means: tuple[float, ...]
    scales: tuple[float, ...]
    metric_name: str
    metric_value: float


@dataclass(frozen=True)
class TrainingRun:
    dataset: Mapping[str, Any]
    features: Sequence[Mapping[str, Any]]
    label: Mapping[str, Any]
    universe_id: str
    split_sha256: str
    seed: int
    missing_policy: str
    training_code_revision: str
    environment_sha256: str
    model_id: str
    bundle_id: str
    bundle_version: int
    training_start_unix_nanos: int
    training_end_unix_nanos: int

    def fit_evaluate_export(
        self,
        feature_rows: Sequence[Sequence[float | None]],
        labels: Sequence[float | None],
        splits: Sequence[str],
        output_root: Path | str,
        *,
        model_kind: str,
        validator: Path | str | None = None,
    ) -> BundleReceipt:
        config = self._validated_config(model_kind)
        rows, targets, admitted_splits = _admit_rows(
            feature_rows,
            labels,
            splits,
            len(config["features"]),
            self.missing_policy,
        )
        train = [index for index, split in enumerate(admitted_splits) if split == "train"]
        validation = [index for index, split in enumerate(admitted_splits) if split == "validation"]
        if len(train) <= len(config["features"]) or not validation:
            raise TrainingValidationError("training and validation boundaries are insufficient")
        fitted = _fit(model_kind, rows, targets, train, validation, self.seed)
        trial = {
            "schema_version": 1,
            "dataset": dict(config["dataset"]),
            "features": list(config["features"]),
            "label": dict(config["label"]),
            "universe_id": self.universe_id,
            "split_sha256": self.split_sha256,
            "seed": self.seed,
            "missing_policy": self.missing_policy,
            "training_code_revision": self.training_code_revision,
            "environment_sha256": self.environment_sha256,
            "model_kind": model_kind,
            "train_rows": len(train),
            "validation_rows": len(validation),
            "test_rows": sum(split == "test" for split in admitted_splits),
        }
        trial_sha256 = hashlib.sha256(_canonical(trial)).hexdigest()
        run_record = dict(trial)
        run_record["trial_sha256"] = trial_sha256
        run_record["validation_metrics"] = {fitted.metric_name: fitted.metric_value}
        artifact = {
            "schema_version": 1,
            "format": f"native_{model_kind}",
            "format_version": 1,
            "feature_semantic_sha256": [feature["semantic_sha256"] for feature in config["features"]],
            "weights": list(fitted.weights),
            "bias": fitted.bias,
            "output_count": 1,
        }
        feature_metadata = []
        for feature, mean, scale in zip(config["features"], fitted.means, fitted.scales, strict=True):
            feature_metadata.append(
                {
                    "name": feature["name"],
                    "version": feature["version"],
                    "input_schema_sha256": feature["input_schema_sha256"],
                    "semantic_sha256": feature["semantic_sha256"],
                    "normalizer": {"kind": "standard", "mean": mean, "scale": scale},
                }
            )
        thresholds = (
            {"negative_max": 0.4, "positive_min": 0.6, "minimum_confidence": 0.0}
            if model_kind == "logistic"
            else {"negative_max": -0.5, "positive_min": 0.5, "minimum_confidence": 0.0}
        )
        metadata = {
            "schema_version": 1,
            "bundle_id": self.bundle_id,
            "bundle_version": self.bundle_version,
            "model_id": self.model_id,
            "artifact": {
                "path": "artifact.json",
                "sha256": "0" * 64,
                "size_bytes": 1,
                "format": f"native_{model_kind}",
                "format_version": 1,
            },
            "features": feature_metadata,
            "training_dataset": dict(config["dataset"]),
            "training_universe_id": self.universe_id,
            "training_period": {
                "start_unix_nanos": self.training_start_unix_nanos,
                "end_unix_nanos": self.training_end_unix_nanos,
            },
            "label": dict(config["label"]),
            "training_code_revision": self.training_code_revision,
            "validation_metrics": [{"name": fitted.metric_name, "value": fitted.metric_value}],
            "decision_thresholds": thresholds,
            "intended_use": "bounded local research trained from one exact point-in-time generation",
            "limitations": ["candidate requires independent Rust admission before production use"],
            "fallback": {"policy": "no_action", "reason": "model contract unavailable"},
        }
        expectations = {
            "schema_version": 1,
            "model_id": self.model_id,
            "bundle_id": self.bundle_id,
            "bundle_version": self.bundle_version,
            "dataset": dict(config["dataset"]),
            "universe_id": self.universe_id,
            "training_period": dict(metadata["training_period"]),
            "label": dict(config["label"]),
            "training_code_revision": self.training_code_revision,
        }
        return BundleCandidate(metadata, artifact, expectations, run_record).write(
            output_root,
            validator=validator,
        )

    def _validated_config(self, model_kind: str) -> dict[str, Any]:
        if model_kind not in {"linear", "logistic"}:
            raise TrainingValidationError("model kind is unsupported")
        if not isinstance(self.seed, int) or isinstance(self.seed, bool) or not 0 <= self.seed < 2**64:
            raise TrainingValidationError("training seed is invalid")
        if self.missing_policy not in {"reject", "drop_row"}:
            raise TrainingValidationError("missing-value policy is unsupported")
        if not self.training_start_unix_nanos < self.training_end_unix_nanos:
            raise TrainingValidationError("training period is invalid")
        for value in (self.split_sha256, self.environment_sha256):
            _hex(value)
        for value in (self.universe_id, self.training_code_revision, self.bundle_id):
            _identifier(value)
        if not isinstance(self.bundle_version, int) or self.bundle_version <= 0:
            raise TrainingValidationError("bundle version is invalid")
        if not isinstance(self.model_id, str) or len(self.model_id) != 36:
            raise TrainingValidationError("model identity is invalid")
        dataset_keys = {
            "dataset_id",
            "manifest_version",
            "schema_name",
            "schema_version",
            "schema_sha256",
            "manifest_sha256",
            "build_spec_sha256",
            "universe_sha256",
            "policy_sha256",
        }
        dataset = dict(self.dataset)
        if set(dataset) != dataset_keys:
            raise TrainingValidationError("training dataset identity is incomplete")
        _identifier(dataset["dataset_id"])
        _identifier(dataset["schema_name"])
        for name in (
            "schema_sha256",
            "manifest_sha256",
            "build_spec_sha256",
            "universe_sha256",
            "policy_sha256",
        ):
            _hex(dataset[name])
        if any(not isinstance(dataset[name], int) or dataset[name] <= 0 for name in ("manifest_version", "schema_version")):
            raise TrainingValidationError("training dataset version is invalid")
        label = dict(self.label)
        if set(label) != {"kind", "scope", "corporate_action_sensitivity", "name", "version"}:
            raise TrainingValidationError("label identity is incomplete")
        if label["kind"] != "label" or label["scope"] not in {"instrument", "account", "global"}:
            raise TrainingValidationError("label kind or scope is invalid")
        if label["corporate_action_sensitivity"] not in {"not_applicable", "requires_adjustment"}:
            raise TrainingValidationError("label corporate-action policy is invalid")
        _identifier(label["name"])
        if not isinstance(label["version"], int) or label["version"] <= 0:
            raise TrainingValidationError("label version is invalid")
        features = [dict(feature) for feature in self.features]
        if not features or len(features) > MAX_FEATURES:
            raise TrainingValidationError("feature count is invalid")
        identities: set[tuple[str, int]] = set()
        for feature in features:
            if set(feature) != {"name", "version", "input_schema_sha256", "semantic_sha256"}:
                raise TrainingValidationError("feature identity is incomplete")
            _identifier(feature["name"])
            _hex(feature["input_schema_sha256"])
            _hex(feature["semantic_sha256"])
            identity = (feature["name"], feature["version"])
            if not isinstance(feature["version"], int) or feature["version"] <= 0 or identity in identities:
                raise TrainingValidationError("feature identity is invalid or duplicated")
            identities.add(identity)
        return {"dataset": dataset, "label": label, "features": features}


def _admit_rows(
    feature_rows: Sequence[Sequence[float | None]],
    labels: Sequence[float | None],
    splits: Sequence[str],
    feature_count: int,
    missing_policy: str,
) -> tuple[list[list[float]], list[float], list[str]]:
    if not feature_rows or len(feature_rows) > MAX_TRAINING_ROWS:
        raise TrainingValidationError("training row count is invalid")
    if len(labels) != len(feature_rows) or len(splits) != len(feature_rows):
        raise TrainingValidationError("training columns have different lengths")
    if len(feature_rows) * feature_count > MAX_CELLS:
        raise TrainingValidationError("training matrix exceeds its retained-cell bound")
    rows: list[list[float]] = []
    targets: list[float] = []
    admitted: list[str] = []
    for row, target, split in zip(feature_rows, labels, splits, strict=True):
        if len(row) != feature_count or split not in {"train", "validation", "test"}:
            raise TrainingValidationError("training row shape or split is invalid")
        missing = target is None or any(value is None for value in row)
        if missing and missing_policy == "reject":
            raise TrainingValidationError("missing training value was rejected")
        if missing:
            continue
        numeric = [float(value) for value in row if value is not None]
        numeric_target = float(target) if target is not None else math.nan
        if any(not math.isfinite(value) for value in numeric) or not math.isfinite(numeric_target):
            raise TrainingValidationError("training values must be finite")
        rows.append(numeric)
        targets.append(numeric_target)
        admitted.append(split)
    return rows, targets, admitted


def _fit(
    kind: str,
    rows: list[list[float]],
    targets: list[float],
    train: list[int],
    validation: list[int],
    seed: int,
) -> _FittedModel:
    feature_count = len(rows[0])
    means = tuple(sum(rows[index][column] for index in train) / len(train) for column in range(feature_count))
    scales = []
    for column, mean in enumerate(means):
        variance = sum((rows[index][column] - mean) ** 2 for index in train) / len(train)
        scale = math.sqrt(variance)
        if not math.isfinite(scale) or scale <= 0.0:
            raise TrainingValidationError("training feature has zero or invalid scale")
        scales.append(scale)
    normalized = [[(value - means[column]) / scales[column] for column, value in enumerate(row)] for row in rows]
    if kind == "linear":
        weights, bias = _linear_fit(normalized, targets, train)
        errors = [(_predict_linear(normalized[index], weights, bias) - targets[index]) ** 2 for index in validation]
        metric_name = "mean_squared_error"
        metric = sum(errors) / len(errors)
    else:
        if any(target not in {0.0, 1.0} for target in targets):
            raise TrainingValidationError("logistic labels must be exactly zero or one")
        weights, bias = _logistic_fit(normalized, targets, train, seed)
        correct = sum((_predict_logistic(normalized[index], weights, bias) >= 0.5) == bool(targets[index]) for index in validation)
        metric_name = "accuracy"
        metric = correct / len(validation)
    values = [*weights, bias, metric, *means, *scales]
    if any(not math.isfinite(value) for value in values):
        raise TrainingValidationError("training produced a nonfinite model")
    return _FittedModel(tuple(weights), bias, means, tuple(scales), metric_name, metric)


def _linear_fit(rows: list[list[float]], targets: list[float], train: list[int]) -> tuple[list[float], float]:
    width = len(rows[0]) + 1
    matrix = [[0.0 for _ in range(width)] for _ in range(width)]
    vector = [0.0 for _ in range(width)]
    for index in train:
        augmented = [*rows[index], 1.0]
        for left in range(width):
            vector[left] += augmented[left] * targets[index]
            for right in range(width):
                matrix[left][right] += augmented[left] * augmented[right]
    for index in range(width - 1):
        matrix[index][index] += 1e-12
    solution = _solve(matrix, vector)
    return solution[:-1], solution[-1]


def _solve(matrix: list[list[float]], vector: list[float]) -> list[float]:
    size = len(vector)
    augmented = [row[:] + [vector[index]] for index, row in enumerate(matrix)]
    for column in range(size):
        pivot = max(range(column, size), key=lambda row: abs(augmented[row][column]))
        if abs(augmented[pivot][column]) < 1e-15:
            raise TrainingValidationError("training system is singular")
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        divisor = augmented[column][column]
        augmented[column] = [value / divisor for value in augmented[column]]
        for row in range(size):
            if row == column:
                continue
            factor = augmented[row][column]
            augmented[row] = [left - factor * right for left, right in zip(augmented[row], augmented[column], strict=True)]
    return [augmented[index][-1] for index in range(size)]


def _logistic_fit(rows: list[list[float]], targets: list[float], train: list[int], seed: int) -> tuple[list[float], float]:
    generator = random.Random(seed)
    weights = [generator.uniform(-1e-6, 1e-6) for _ in rows[0]]
    bias = generator.uniform(-1e-6, 1e-6)
    for _ in range(400):
        gradients = [0.0 for _ in weights]
        bias_gradient = 0.0
        for index in train:
            error = _predict_logistic(rows[index], weights, bias) - targets[index]
            for column, value in enumerate(rows[index]):
                gradients[column] += error * value
            bias_gradient += error
        rate = 0.1 / len(train)
        weights = [weight - rate * gradient for weight, gradient in zip(weights, gradients, strict=True)]
        bias -= rate * bias_gradient
    return weights, bias


def _predict_linear(row: Sequence[float], weights: Sequence[float], bias: float) -> float:
    return sum(value * weight for value, weight in zip(row, weights, strict=True)) + bias


def _predict_logistic(row: Sequence[float], weights: Sequence[float], bias: float) -> float:
    score = _predict_linear(row, weights, bias)
    if score >= 0:
        return 1.0 / (1.0 + math.exp(-score))
    exponential = math.exp(score)
    return exponential / (1.0 + exponential)


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()


def _hex(value: Any) -> None:
    if not isinstance(value, str) or HEX.fullmatch(value) is None or value == "0" * 64:
        raise TrainingValidationError("reproducibility digest is invalid")


def _identifier(value: Any) -> None:
    if not isinstance(value, str) or IDENTIFIER.fullmatch(value) is None or len(value.encode()) > 128:
        raise TrainingValidationError("reproducibility identity is invalid")
