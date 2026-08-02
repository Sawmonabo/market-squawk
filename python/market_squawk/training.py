"""Deterministic bounded native linear/logistic training and validated export."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import random
import re
from typing import Any, Mapping, Sequence
from uuid import UUID

from . import _native
from .bundle import BundleAuthorityRef, BundleCandidate, BundleReceipt
from .data import ComponentIdentity, DatasetIntegrityError, DatasetResult, _verify_dataset_receipt
from .finance import OperationContext
from .forecasting import (
    ConformalMethod,
    ForecastFit,
    ForecastSpecification,
    ForecastValidationError,
    fit_forecast,
)
from ._onnx import (
    OnnxEncodingError,
    encode_fitted_model,
    quantize_fitted_model,
    quantize_float32,
)


MAX_TRAINING_ROWS = 100_000
MAX_FEATURES = 1_024
MAX_CELLS = 2_000_000
MAX_TRAINING_OPERATIONS = 50_000_000
LOGISTIC_EPOCHS = 400
CONTROL_CHECK_INTERVAL = 128
IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._-]{0,126}[a-z0-9]$|^[a-z0-9]$")
HEX = re.compile(r"^[0-9a-f]{64}$")


class TrainingValidationError(ValueError):
    """A reproducibility, input, resource, or finite-arithmetic contract failed."""


TrainingEnvironmentReceipt = _native.TrainingEnvironmentReceipt


def training_environment_receipt() -> TrainingEnvironmentReceipt:
    """Return the builder-authored native training environment for this wheel."""

    return _native.training_environment_receipt()


@dataclass(frozen=True)
class _FittedModel:
    weights: tuple[float, ...]
    bias: float
    means: tuple[float, ...]
    scales: tuple[float, ...]
    metric_name: str
    metric_value: float


@dataclass(frozen=True, init=False)
class TrainingProposal:
    """Deterministic candidate awaiting independent operator/catalog authorization."""

    candidate: BundleCandidate
    authority_bytes: bytes = field(repr=False)
    authority_sha256: str
    dataset: DatasetResult = field(repr=False, compare=False)

    def __init__(
        self,
        candidate: BundleCandidate,
        authority_request: Mapping[str, Any],
        dataset: DatasetResult,
    ) -> None:
        authority_bytes = _canonical(authority_request)
        object.__setattr__(self, "candidate", candidate)
        object.__setattr__(self, "authority_bytes", authority_bytes)
        object.__setattr__(self, "authority_sha256", hashlib.sha256(authority_bytes).hexdigest())
        object.__setattr__(self, "dataset", dataset)

    @property
    def training_run_sha256(self) -> str:
        return self.candidate.training_run_sha256

    def export(
        self,
        output_root: Path | str,
        authority: BundleAuthorityRef,
        *,
        context: OperationContext,
    ) -> BundleReceipt:
        if authority.sha256 != self.authority_sha256:
            raise TrainingValidationError("operator authority does not match this proposal")
        try:
            _verify_dataset_receipt(self.dataset, context)
        except DatasetIntegrityError as error:
            raise TrainingValidationError("dataset receipt failed immediately before export") from error
        return self.candidate.write(
            output_root,
            authority,
            dataset_receipt=self.dataset._receipt,
        )


@dataclass(frozen=True)
class ForecastArtifact:
    """One immutable bundle-ready forecast artifact."""

    path: str
    content: bytes = field(repr=False)
    sha256: str


@dataclass(frozen=True)
class ForecastTrainingProposal:
    """Dataset-bound central ONNX path plus hashed calibration artifacts."""

    fit: ForecastFit
    training_run_bytes: bytes = field(repr=False)
    training_run_sha256: str
    artifacts: tuple[ForecastArtifact, ...]
    forecast_metadata: Mapping[str, Any]
    dataset: DatasetResult = field(repr=False, compare=False)


@dataclass(frozen=True)
class TrainingRun:
    dataset: DatasetResult
    features: Sequence[Mapping[str, Any]]
    label: ComponentIdentity | Mapping[str, Any]
    seed: int
    missing_policy: str
    environment: TrainingEnvironmentReceipt
    model_id: str
    bundle_id: str
    bundle_version: int

    @property
    def training_code_revision(self) -> str:
        return self.environment.training_code_revision

    @property
    def environment_sha256(self) -> str:
        return self.environment.sha256

    def fit_evaluate(
        self,
        *,
        model_kind: str,
        artifact_format: str = "native",
        context: OperationContext,
    ) -> TrainingProposal:
        try:
            _verify_dataset_receipt(self.dataset, context)
        except DatasetIntegrityError as error:
            raise TrainingValidationError("training dataset receipt is invalid") from error
        config = self._validated_config(model_kind, artifact_format)
        _admit_operation_context(
            context,
            _training_operation_estimate(
                self.dataset,
                len(config["features"]),
                model_kind,
                artifact_format,
            ),
        )
        rows, targets, admitted_splits, split_sha256, split_counts, period = _dataset_matrix(
            self.dataset,
            config["features"],
            config["label"],
            self.missing_policy,
            context,
        )
        train = [index for index, split in enumerate(admitted_splits) if split == "train"]
        validation = [index for index, split in enumerate(admitted_splits) if split == "validation"]
        if len(train) <= len(config["features"]) or not validation:
            raise TrainingValidationError("training and validation boundaries are insufficient")
        try:
            _verify_dataset_receipt(self.dataset, context)
        except DatasetIntegrityError as error:
            raise TrainingValidationError("dataset receipt failed immediately before fit") from error
        fitted = _fit(model_kind, rows, targets, train, validation, self.seed, context)
        if artifact_format == "onnx":
            try:
                fitted = _quantized_onnx_fit(
                    fitted,
                    model_kind,
                    rows,
                    targets,
                    validation,
                    context,
                )
            except OnnxEncodingError as error:
                raise TrainingValidationError(
                    "fitted model cannot be represented by finite ONNX float tensors"
                ) from error
        output_semantics = (
            "binary_probability" if model_kind == "logistic" else "regression"
        )
        bundle_format = (
            f"native_{model_kind}" if artifact_format == "native" else "onnx"
        )
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
            "model_kind": (
                f"native_{model_kind}" if artifact_format == "native" else model_kind
            ),
            "seed": self.seed,
            "split_counts": split_counts,
            "split_sha256": split_sha256,
            "training_code_revision": self.training_code_revision,
            "training_period": period,
            "universe_id": self.dataset.universe_id,
        }
        if artifact_format == "onnx":
            trial["output_semantics"] = output_semantics
        trial_sha256 = hashlib.sha256(_canonical(trial)).hexdigest()
        metrics = [{"name": fitted.metric_name, "value": fitted.metric_value}]
        run_record = {
            "schema_version": 2 if artifact_format == "native" else 3,
            "trial": trial,
            "trial_sha256": trial_sha256,
            "validation_metrics": metrics,
        }
        artifact = {
            "schema_version": 1,
            "format": bundle_format,
            "format_version": 1,
            "feature_semantic_sha256": [
                feature["semantic_sha256"] for feature in config["features"]
            ],
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
            "schema_version": 4 if artifact_format == "native" else 5,
            "bundle_id": self.bundle_id,
            "bundle_version": self.bundle_version,
            "model_id": self.model_id,
            "artifact": {
                "path": (
                    "artifact.json" if artifact_format == "native" else "model.onnx"
                ),
                "sha256": "0" * 64,
                "size_bytes": 1,
                "format": bundle_format,
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
            "training_environment_sha256": self.environment_sha256,
            "validation_metrics": metrics,
            "decision_thresholds": thresholds,
            "intended_use": "bounded local research trained from one exact point-in-time generation",
            "limitations": ["candidate requires independent Rust admission before production use"],
            "fallback": {"policy": "no_action", "reason": "model contract unavailable"},
        }
        if artifact_format == "onnx":
            metadata["output_semantics"] = output_semantics
        if artifact_format == "native":
            candidate = BundleCandidate(metadata, artifact, run_record)
        else:
            try:
                onnx_artifact = encode_fitted_model(
                    fitted.weights,
                    fitted.bias,
                    model_kind=model_kind,
                )
            except OnnxEncodingError as error:
                raise TrainingValidationError(
                    "fitted model cannot be encoded as ONNX"
                ) from error
            candidate = BundleCandidate.onnx(metadata, onnx_artifact, run_record)
        authority = {
            "schema_version": 5 if artifact_format == "native" else 6,
            "model_id": self.model_id,
            "bundle_id": self.bundle_id,
            "bundle_version": self.bundle_version,
            "bundle_metadata_sha256": candidate.metadata_sha256,
            "artifact_sha256": candidate.artifact_sha256,
            "dataset": dict(config["dataset"]),
            "universe_id": self.dataset.universe_id,
            "training_period": period,
            "label": dict(config["label"]),
            "training_code_revision": self.training_code_revision,
            "training_environment_sha256": self.environment_sha256,
            "training_run_sha256": candidate.training_run_sha256,
        }
        if artifact_format == "onnx":
            authority["output_semantics"] = output_semantics
        return TrainingProposal(candidate, authority, self.dataset)

    def fit_research_forecast(
        self,
        specification: ForecastSpecification,
        *,
        future_exogenous: Sequence[Sequence[float]],
        conformal_method: ConformalMethod | None,
        dependence_assumptions: str | None,
        context: OperationContext,
    ) -> ForecastTrainingProposal:
        """Fit a distinct research forecast against one exact PIT dataset receipt.

        The returned artifacts are not independently authoritative bundles.  They
        are immutable candidate inputs for the normal Rust admission path.
        """

        try:
            _verify_dataset_receipt(self.dataset, context)
        except DatasetIntegrityError as error:
            raise TrainingValidationError("forecast dataset receipt is invalid") from error
        if (
            not isinstance(specification, ForecastSpecification)
            or not specification.horizons
            or not specification.lags
        ):
            raise TrainingValidationError("forecast specification is invalid")
        config = self._validated_config("linear", "onnx")
        estimate = _checked_mul(
            max(1, len(self.dataset.rows)),
            _checked_mul(
                max(1, len(config["features"])),
                _checked_mul(
                    max(1, len(specification.horizons)),
                    max(1, specification.rolling_splits),
                ),
            ),
        )
        _admit_operation_context(context, estimate)
        rows, targets, splits, split_sha256, split_counts, period = _dataset_matrix(
            self.dataset,
            config["features"],
            config["label"],
            self.missing_policy,
            context,
        )
        retained = [index for index, split in enumerate(splits) if split != "test"]
        if len(retained) <= max(specification.lags) + max(specification.horizons):
            raise TrainingValidationError("non-test history is insufficient for forecasting")
        observed = [targets[index] for index in retained]
        exogenous = [rows[index] for index in retained]
        selected_cutoffs = []
        component_count = len(self.dataset.components)
        for offset in range(0, len(self.dataset.rows), component_count):
            group = self.dataset.rows[offset : offset + component_count]
            if group[0]["split"] != "test":
                selected_cutoffs.append(group[0]["cutoff_at"].unix_nanos)
        if (
            len(selected_cutoffs) != len(observed)
            or not selected_cutoffs
            or selected_cutoffs != sorted(selected_cutoffs)
            or specification.observed_cutoff_unix_nanos != selected_cutoffs[-1]
            or specification.observed_cutoff_unix_nanos
            > self.dataset.identity.selection_as_of_unix_nanos
        ):
            raise TrainingValidationError("forecast cutoff differs from the exact PIT history")
        try:
            fit = fit_forecast(
                observed,
                specification,
                exogenous=exogenous,
                future_exogenous=future_exogenous,
                quantile_intervals=True,
                conformal_method=conformal_method,
                dependence_assumptions=dependence_assumptions,
            )
        except ForecastValidationError as error:
            raise TrainingValidationError("research forecast fit was rejected") from error
        _checkpoint(context)
        selected = fit.conformal_intervals or fit.quantile_intervals
        if selected is None:
            raise TrainingValidationError("forecast calibration artifact is unavailable")
        residual_path = "calibration/residuals.f64le"
        policy_path = "calibration/policy.json"
        start_index = selected.calibration_start_index
        if not 0 <= start_index < len(selected_cutoffs):
            raise TrainingValidationError("calibration window is outside the PIT history")
        policy_record = {
            "schema_version": 1,
            "kind": selected.kind.value,
            "method": selected.method,
            "calibration_window": {
                "start_unix_nanos": selected_cutoffs[start_index],
                "end_unix_nanos": specification.observed_cutoff_unix_nanos + 1,
                "observations": len(selected.residuals_bytes) // 8,
            },
            "dependence_assumptions": selected.dependence_assumptions,
            "residuals_sha256": selected.residuals_sha256,
            "bands": [
                {
                    "target_coverage_basis_points": int(
                        band.target_coverage * 10_000
                    ),
                    "lower_offset": band.lower[0] - fit.central[0],
                    "upper_offset": band.upper[0] - fit.central[0],
                    "realized_covered": band.realized_covered,
                    "realized_total": band.realized_total,
                }
                for band in selected.bands
            ],
        }
        policy_bytes = _canonical(policy_record)
        policy_sha256 = hashlib.sha256(policy_bytes).hexdigest()
        forecast_metadata = {
            "residuals": {
                "path": residual_path,
                "sha256": selected.residuals_sha256,
                "size_bytes": len(selected.residuals_bytes),
            },
            "policy": {
                "path": policy_path,
                "sha256": policy_sha256,
                "size_bytes": len(policy_bytes),
            },
        }
        artifacts = [
            ForecastArtifact("model.onnx", fit.onnx_bytes, fit.onnx_sha256),
            ForecastArtifact(
                residual_path,
                selected.residuals_bytes,
                selected.residuals_sha256,
            ),
            ForecastArtifact(policy_path, policy_bytes, policy_sha256),
        ]
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
            "model_kind": "linear",
            "output_semantics": "regression",
            "seed": self.seed,
            "split_counts": split_counts,
            "split_sha256": split_sha256,
            "training_code_revision": self.training_code_revision,
            "training_period": period,
            "universe_id": self.dataset.universe_id,
            "forecast": {
                "strategy": fit.strategy.value,
                "horizons": list(specification.horizons),
                "lags": list(specification.lags),
                "observed_cutoff_unix_nanos": specification.observed_cutoff_unix_nanos,
                "rolling_splits": specification.rolling_splits,
                "ridge_alpha": specification.ridge_alpha,
                "selection_sha256": fit.validation.selection_sha256,
                "package_versions": dict(fit.package_versions),
            },
        }
        record = {
            "schema_version": 4,
            "trial": trial,
            "trial_sha256": hashlib.sha256(_canonical(trial)).hexdigest(),
            "validation_metrics": [
                {
                    "name": "mean_squared_error",
                    "value": fit.validation.mean_squared_error,
                }
            ],
            "forecast_calibration": forecast_metadata,
        }
        encoded = _canonical(record)
        try:
            _verify_dataset_receipt(self.dataset, context)
        except DatasetIntegrityError as error:
            raise TrainingValidationError(
                "dataset receipt failed immediately after forecast fit"
            ) from error
        return ForecastTrainingProposal(
            fit,
            encoded,
            hashlib.sha256(encoded).hexdigest(),
            tuple(artifacts),
            forecast_metadata,
            self.dataset,
        )

    def _validated_config(
        self, model_kind: str, artifact_format: str
    ) -> dict[str, Any]:
        if not isinstance(self.environment, TrainingEnvironmentReceipt):
            raise TypeError("training environment must be the native builder-authored receipt")
        current_environment = training_environment_receipt()
        if (
            self.environment.sha256 != current_environment.sha256
            or self.environment.training_code_revision
            != current_environment.training_code_revision
        ):
            raise TrainingValidationError("training environment changed after receipt admission")
        if model_kind not in {"linear", "logistic"}:
            raise TrainingValidationError("model kind is unsupported")
        if artifact_format not in {"native", "onnx"}:
            raise TrainingValidationError("artifact format is unsupported")
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
    context: OperationContext,
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
        if offset % (component_count * CONTROL_CHECK_INTERVAL) == 0:
            _checkpoint(context)
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
    _checkpoint(context)
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
    context: OperationContext,
) -> _FittedModel:
    feature_count = len(rows[0])
    means = []
    for column in range(feature_count):
        _checkpoint(context)
        means.append(sum(rows[index][column] for index in train) / len(train))
    means = tuple(means)
    scales = []
    for column, mean in enumerate(means):
        _checkpoint(context)
        variance = sum((rows[index][column] - mean) ** 2 for index in train) / len(train)
        scale = math.sqrt(variance)
        if not math.isfinite(scale) or scale <= 0.0:
            raise TrainingValidationError("training feature has zero or invalid scale")
        scales.append(scale)
    normalized = []
    for index, row in enumerate(rows):
        if index % CONTROL_CHECK_INTERVAL == 0:
            _checkpoint(context)
        normalized.append(
            [(value - means[column]) / scales[column] for column, value in enumerate(row)]
        )
    if kind == "linear":
        weights, bias = _linear_fit(normalized, targets, train, context)
        errors = [(_predict_linear(normalized[index], weights, bias) - targets[index]) ** 2 for index in validation]
        metric_name = "mean_squared_error"
        metric = sum(errors) / len(errors)
    else:
        if any(target not in {0.0, 1.0} for target in targets):
            raise TrainingValidationError("logistic labels must be exactly zero or one")
        weights, bias = _logistic_fit(normalized, targets, train, seed, context)
        correct = sum((_predict_logistic(normalized[index], weights, bias) >= 0.5) == bool(targets[index]) for index in validation)
        metric_name = "accuracy"
        metric = correct / len(validation)
    values = [*weights, bias, metric, *means, *scales]
    if any(not math.isfinite(value) for value in values):
        raise TrainingValidationError("training produced a nonfinite model")
    _checkpoint(context)
    return _FittedModel(tuple(weights), bias, means, tuple(scales), metric_name, metric)


def _quantized_onnx_fit(
    fitted: _FittedModel,
    model_kind: str,
    rows: Sequence[Sequence[float]],
    targets: Sequence[float],
    validation: Sequence[int],
    context: OperationContext,
) -> _FittedModel:
    _checkpoint(context)
    weights, bias = quantize_fitted_model(fitted.weights, fitted.bias)
    scores = []
    for position, index in enumerate(validation):
        if position % CONTROL_CHECK_INTERVAL == 0:
            _checkpoint(context)
        normalized = []
        for column in range(len(weights)):
            if column % CONTROL_CHECK_INTERVAL == 0:
                _checkpoint(context)
            normalized.append(
                quantize_float32(
                    (rows[index][column] - fitted.means[column])
                    / fitted.scales[column]
                )
            )
        score = bias
        for column, (value, weight) in enumerate(
            zip(normalized, weights, strict=True)
        ):
            if column % CONTROL_CHECK_INTERVAL == 0:
                _checkpoint(context)
            contribution = quantize_float32(value * weight)
            score = quantize_float32(score + contribution)
        if model_kind == "logistic":
            if score >= 0.0:
                score = quantize_float32(1.0 / (1.0 + math.exp(-score)))
            else:
                exponential = math.exp(score)
                score = quantize_float32(exponential / (1.0 + exponential))
            if not 0.0 <= score <= 1.0:
                raise OnnxEncodingError(
                    "logistic ONNX candidate produced a non-probability"
                )
        scores.append(score)
    if model_kind == "linear":
        metric_name = "mean_squared_error"
        metric_value = sum(
            (score - targets[index]) ** 2
            for score, index in zip(scores, validation, strict=True)
        ) / len(validation)
    else:
        metric_name = "accuracy"
        metric_value = sum(
            (score >= 0.5) == bool(targets[index])
            for score, index in zip(scores, validation, strict=True)
        ) / len(validation)
    if not math.isfinite(metric_value):
        raise OnnxEncodingError("ONNX candidate metric is nonfinite")
    _checkpoint(context)
    return _FittedModel(
        weights,
        bias,
        fitted.means,
        fitted.scales,
        metric_name,
        metric_value,
    )


def _linear_fit(
    rows: list[list[float]],
    targets: list[float],
    train: list[int],
    context: OperationContext,
) -> tuple[list[float], float]:
    width = len(rows[0]) + 1
    matrix = [[0.0 for _ in range(width)] for _ in range(width)]
    vector = [0.0 for _ in range(width)]
    for position, index in enumerate(train):
        if position % CONTROL_CHECK_INTERVAL == 0:
            _checkpoint(context)
        augmented = [*rows[index], 1.0]
        for left in range(width):
            vector[left] += augmented[left] * targets[index]
            for right in range(width):
                matrix[left][right] += augmented[left] * augmented[right]
    for index in range(width - 1):
        matrix[index][index] += 1e-12
    solution = _solve(matrix, vector, context)
    return solution[:-1], solution[-1]


def _solve(
    matrix: list[list[float]], vector: list[float], context: OperationContext
) -> list[float]:
    size = len(vector)
    augmented = [row[:] + [vector[index]] for index, row in enumerate(matrix)]
    for column in range(size):
        _checkpoint(context)
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


def _logistic_fit(
    rows: list[list[float]],
    targets: list[float],
    train: list[int],
    seed: int,
    context: OperationContext,
) -> tuple[list[float], float]:
    generator = random.Random(seed)
    weights = [generator.uniform(-1e-6, 1e-6) for _ in rows[0]]
    bias = generator.uniform(-1e-6, 1e-6)
    for _ in range(LOGISTIC_EPOCHS):
        _checkpoint(context)
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


def _training_operation_estimate(
    dataset: DatasetResult,
    feature_count: int,
    model_kind: str,
    artifact_format: str,
) -> int:
    component_count = len(dataset.components)
    if component_count == 0:
        raise TrainingValidationError("training component contract is empty")
    example_count = (len(dataset.rows) + component_count - 1) // component_count
    width = _checked_add(feature_count, 1)
    common = _checked_add(
        _checked_mul(len(dataset.rows), 8),
        _checked_mul(_checked_mul(example_count, feature_count), 12),
    )
    if model_kind == "linear":
        fitting = _checked_add(
            _checked_mul(_checked_mul(example_count, width), _checked_mul(width, 4)),
            _checked_mul(_checked_mul(width, width), _checked_mul(width, 6)),
        )
    else:
        per_example = _checked_add(_checked_mul(feature_count, 8), 32)
        fitting = _checked_mul(
            LOGISTIC_EPOCHS, _checked_mul(example_count, per_example)
        )
    estimate = _checked_add(common, fitting)
    if artifact_format == "onnx":
        per_example = _checked_add(_checked_mul(feature_count, 16), 48)
        estimate = _checked_add(
            estimate,
            _checked_mul(example_count, per_example),
        )
    return estimate


def _checked_add(left: int, right: int) -> int:
    result = left + right
    if result <= 0 or result > MAX_TRAINING_OPERATIONS:
        raise TrainingValidationError("training operation budget is exceeded")
    return result


def _checked_mul(left: int, right: int) -> int:
    result = left * right
    if result <= 0 or result > MAX_TRAINING_OPERATIONS:
        raise TrainingValidationError("training operation budget is exceeded")
    return result


def _admit_operation_context(context: OperationContext, operations: int) -> None:
    if not isinstance(context, OperationContext):
        raise TrainingValidationError("training operation context is invalid")
    try:
        context.reserve(operations)
    except ValueError as error:
        raise TrainingValidationError("training operation context rejected the workload") from error


def _checkpoint(context: OperationContext) -> None:
    try:
        context.checkpoint()
    except ValueError as error:
        raise TrainingValidationError("training operation was cancelled or expired") from error


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
