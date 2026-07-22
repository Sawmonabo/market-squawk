"""Deterministic bounded native linear/logistic training and validated export."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import random
import re
from typing import Any, Mapping, Sequence
from uuid import UUID

from .bundle import BundleAuthorityRef, BundleCandidate, BundleReceipt
from .data import ComponentIdentity, DatasetResult


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
class TrainingProposal:
    """Deterministic candidate awaiting independent operator/catalog authorization."""

    candidate: BundleCandidate
    authority_request: Mapping[str, Any]

    @property
    def authority_bytes(self) -> bytes:
        return _canonical(self.authority_request)

    @property
    def authority_sha256(self) -> str:
        return hashlib.sha256(self.authority_bytes).hexdigest()

    @property
    def training_run_sha256(self) -> str:
        return self.candidate.training_run_sha256

    def export(
        self,
        output_root: Path | str,
        authority: BundleAuthorityRef,
        *,
        validator: Path | str | None = None,
    ) -> BundleReceipt:
        if authority.sha256 != self.authority_sha256:
            raise TrainingValidationError("operator authority does not match this proposal")
        return self.candidate.write(output_root, authority, validator=validator)


@dataclass(frozen=True)
class TrainingRun:
    dataset: DatasetResult
    features: Sequence[Mapping[str, Any]]
    label: ComponentIdentity | Mapping[str, Any]
    seed: int
    missing_policy: str
    training_code_revision: str
    environment_sha256: str
    model_id: str
    bundle_id: str
    bundle_version: int

    def fit_evaluate(self, *, model_kind: str) -> TrainingProposal:
        config = self._validated_config(model_kind)
        rows, targets, admitted_splits, split_sha256, split_counts, period = _dataset_matrix(
            self.dataset,
            config["features"],
            config["label"],
            self.missing_policy,
        )
        train = [index for index, split in enumerate(admitted_splits) if split == "train"]
        validation = [index for index, split in enumerate(admitted_splits) if split == "validation"]
        if len(train) <= len(config["features"]) or not validation:
            raise TrainingValidationError("training and validation boundaries are insufficient")
        fitted = _fit(model_kind, rows, targets, train, validation, self.seed)
        trial = {
            "bundle_id": self.bundle_id,
            "bundle_version": self.bundle_version,
            "dataset": dict(config["dataset"]),
            "dataset_export_sha256": self.dataset.export_sha256,
            "environment_sha256": self.environment_sha256,
            "features": list(config["features"]),
            "label": dict(config["label"]),
            "missing_policy": self.missing_policy,
            "model_id": self.model_id,
            "model_kind": f"native_{model_kind}",
            "seed": self.seed,
            "split_counts": split_counts,
            "split_sha256": split_sha256,
            "training_code_revision": self.training_code_revision,
            "training_period": period,
            "universe_id": self.dataset.universe_id,
        }
        trial_sha256 = hashlib.sha256(_canonical(trial)).hexdigest()
        metrics = [{"name": fitted.metric_name, "value": fitted.metric_value}]
        run_record = {
            "schema_version": 1,
            "trial": trial,
            "trial_sha256": trial_sha256,
            "validation_metrics": metrics,
        }
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
            "schema_version": 2,
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
            "training_run": {
                "path": "training-run.json",
                "sha256": "0" * 64,
                "size_bytes": 1,
            },
            "features": feature_metadata,
            "training_dataset": dict(config["dataset"]),
            "training_universe_id": self.dataset.universe_id,
            "training_period": period,
            "label": dict(config["label"]),
            "training_code_revision": self.training_code_revision,
            "validation_metrics": metrics,
            "decision_thresholds": thresholds,
            "intended_use": "bounded local research trained from one exact point-in-time generation",
            "limitations": ["candidate requires independent Rust admission before production use"],
            "fallback": {"policy": "no_action", "reason": "model contract unavailable"},
        }
        candidate = BundleCandidate(metadata, artifact, run_record)
        authority = {
            "schema_version": 2,
            "model_id": self.model_id,
            "bundle_id": self.bundle_id,
            "bundle_version": self.bundle_version,
            "dataset": dict(config["dataset"]),
            "universe_id": self.dataset.universe_id,
            "training_period": period,
            "label": dict(config["label"]),
            "training_code_revision": self.training_code_revision,
            "training_run_sha256": candidate.training_run_sha256,
        }
        return TrainingProposal(candidate, authority)

    def _validated_config(self, model_kind: str) -> dict[str, Any]:
        if model_kind not in {"linear", "logistic"}:
            raise TrainingValidationError("model kind is unsupported")
        if not isinstance(self.seed, int) or isinstance(self.seed, bool) or not 0 <= self.seed < 2**64:
            raise TrainingValidationError("training seed is invalid")
        if self.missing_policy not in {"reject", "drop_row"}:
            raise TrainingValidationError("missing-value policy is unsupported")
        _hex(self.environment_sha256)
        for value in (self.training_code_revision, self.bundle_id):
            _identifier(value)
        if not isinstance(self.bundle_version, int) or self.bundle_version <= 0:
            raise TrainingValidationError("bundle version is invalid")
        try:
            parsed_model_id = UUID(self.model_id) if isinstance(self.model_id, str) else None
        except (ValueError, AttributeError) as error:
            raise TrainingValidationError("model identity is invalid") from error
        if parsed_model_id is None or str(parsed_model_id) != self.model_id:
            raise TrainingValidationError("model identity is invalid")
        if not isinstance(self.dataset, DatasetResult) or not self.dataset.complete:
            raise TrainingValidationError("training requires one complete admitted Task 11 export")
        dataset = dict(self.dataset.identity.bundle_mapping())
        label = dict(self.label.mapping() if isinstance(self.label, ComponentIdentity) else self.label)
        if set(label) != {"kind", "scope", "corporate_action_sensitivity", "name", "version"}:
            raise TrainingValidationError("label identity is incomplete")
        if label["kind"] != "label" or label["scope"] not in {"instrument", "account", "global"}:
            raise TrainingValidationError("label kind or scope is invalid")
        if label["corporate_action_sensitivity"] not in {"not_applicable", "requires_adjustment"}:
            raise TrainingValidationError("label corporate-action policy is invalid")
        _identifier(label["name"])
        if not isinstance(label["version"], int) or label["version"] <= 0:
            raise TrainingValidationError("label version is invalid")
        if not self.features or len(self.features) > MAX_FEATURES:
            raise TrainingValidationError("feature count is invalid")
        features = []
        identities: set[tuple[str, int]] = set()
        for supplied in self.features:
            feature = dict(supplied)
            if set(feature) != {"name", "version", "input_schema_sha256", "semantic_sha256"}:
                raise TrainingValidationError("feature identity is incomplete")
            _identifier(feature["name"])
            _hex(feature["input_schema_sha256"])
            _hex(feature["semantic_sha256"])
            identity = (feature["name"], feature["version"])
            if not isinstance(feature["version"], int) or feature["version"] <= 0 or identity in identities:
                raise TrainingValidationError("feature identity is invalid or duplicated")
            identities.add(identity)
            features.append(feature)
        dataset_components = {
            (component.kind, component.name, component.version): component
            for component in self.dataset.components
        }
        if ("label", label["name"], label["version"]) not in dataset_components:
            raise TrainingValidationError("label is absent from the Task 11 component contract")
        admitted_label = dataset_components[("label", label["name"], label["version"])]
        if dict(admitted_label.mapping()) != label:
            raise TrainingValidationError("label differs from the Task 11 component contract")
        if any(("feature", feature["name"], feature["version"]) not in dataset_components for feature in features):
            raise TrainingValidationError("feature is absent from the Task 11 component contract")
        return {"dataset": dataset, "label": label, "features": features}


def _dataset_matrix(
    dataset: DatasetResult,
    features: Sequence[Mapping[str, Any]],
    label: Mapping[str, Any],
    missing_policy: str,
) -> tuple[list[list[float]], list[float], list[str], str, Mapping[str, int], Mapping[str, int]]:
    if not dataset.rows:
        raise TrainingValidationError("training row count is invalid")
    component_count = len(dataset.components)
    if len(dataset.rows) % component_count:
        raise TrainingValidationError("training component rows are incomplete")
    example_count = len(dataset.rows) // component_count
    if example_count > MAX_TRAINING_ROWS or example_count * len(features) > MAX_CELLS:
        raise TrainingValidationError("training matrix exceeds its retained-cell bound")
    rows: list[list[float]] = []
    targets: list[float] = []
    admitted: list[str] = []
    evidence: list[Mapping[str, Any]] = []
    feature_keys = [("feature", item["name"], item["version"]) for item in features]
    label_key = ("label", label["name"], label["version"])
    for offset in range(0, len(dataset.rows), component_count):
        group = dataset.rows[offset : offset + component_count]
        values = {
            (row["component_kind"], row["component_name"], row["component_version"]): _numeric(row)
            for row in group
        }
        feature_row = [values[key] for key in feature_keys]
        target = values[label_key]
        split = group[0]["split"]
        missing = target is None or any(value is None for value in feature_row)
        if missing and missing_policy == "reject":
            raise TrainingValidationError("missing training value was rejected")
        if missing:
            continue
        numeric = [float(value) for value in feature_row if value is not None]
        numeric_target = float(target) if target is not None else math.nan
        if any(not math.isfinite(value) for value in numeric) or not math.isfinite(numeric_target):
            raise TrainingValidationError("training values must be finite")
        rows.append(numeric)
        targets.append(numeric_target)
        admitted.append(split)
        evidence.append(
            {
                "components": [
                    {
                        "kind": row["component_kind"],
                        "lineage_sha256": row["lineage_sha256"].hex(),
                        "name": row["component_name"],
                        "version": row["component_version"],
                    }
                    for row in group
                ],
                "cutoff_unix_nanos": group[0]["cutoff_at"].unix_nanos,
                "example_id": group[0]["example_id"],
                "instrument_id": group[0]["instrument_id"],
                "split": split,
            }
        )
    if not rows:
        raise TrainingValidationError("missing policy removed every training row")
    split_sha256 = hashlib.sha256(
        _canonical(
            {
                "dataset_export_sha256": dataset.export_sha256,
                "examples": evidence,
                "schema_version": 1,
            }
        )
    ).hexdigest()
    counts = {name: admitted.count(name) for name in ("train", "validation", "test")}
    training_cutoffs = [
        evidence[index]["cutoff_unix_nanos"]
        for index, split in enumerate(admitted)
        if split == "train"
    ]
    if not training_cutoffs or max(training_cutoffs) >= 2**63 - 1:
        raise TrainingValidationError("training period cannot be represented exactly")
    period = {
        "start_unix_nanos": min(training_cutoffs),
        "end_unix_nanos": max(training_cutoffs) + 1,
    }
    return rows, targets, admitted, split_sha256, counts, period


def _numeric(row: Mapping[str, Any]) -> float | None:
    if row["value_f64"] is not None:
        return float(row["value_f64"])
    if row["value_decimal_mantissa"] is not None:
        value = Decimal(row["value_decimal_mantissa"]).scaleb(-row["value_decimal_scale"])
        converted = float(value)
        if not math.isfinite(converted):
            raise TrainingValidationError("decimal training value exceeds the finite ML domain")
        return converted
    return None


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
