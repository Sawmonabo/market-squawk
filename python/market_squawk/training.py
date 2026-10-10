"""Deterministic bounded native linear/logistic training and validated export."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import random
import re
import struct
import warnings
from types import MappingProxyType
from typing import Any, Iterable, Iterator, Mapping, Sequence
from uuid import UUID

from . import _native
from .bundle import BundleAuthorityRef, BundleCandidate, BundleReceipt
from .data import ComponentIdentity, DatasetIntegrityError, DatasetResult, _verify_dataset_receipt
from .finance import OperationContext
from .forecasting import (
    ConformalMethod,
    ForecastChronology,
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
    """Return native source provenance or verified installed-release coordinates."""

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
    output_measurement: Mapping[str, Any]
    output_statistic: Mapping[str, Any]
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
        rows, targets, admitted_splits, split_sha256, split_counts, period, examples = _dataset_matrix(
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
        output_measurement = config["output_measurement"]
        output_statistic = _output_statistic(
            config["output_target"], output_measurement, model_kind, config["label"]
        )
        calibration_artifacts: Mapping[str, bytes] | None = None
        probability_metrics: list[Mapping[str, Any]] | None = None
        if model_kind == "logistic":
            fitted, calibration_artifacts, probability_metrics = _binary_event_calibration(
                self.dataset, fitted, rows, targets, examples, config["output_target"],
                artifact_format, split_sha256, period, context,
            )
        elif output_statistic["statistic"] == "model_estimated_conditional_mean":
            calibration_artifacts = _direct_forecast_calibration(
                self.dataset,
                fitted,
                rows,
                targets,
                examples,
                config["output_target"],
                artifact_format,
                context,
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
            "output_measurement": output_measurement,
            "output_statistic": output_statistic,
            "output_semantics": output_semantics,
            "seed": self.seed,
            "split_counts": split_counts,
            "split_sha256": split_sha256,
            "training_code_revision": self.training_code_revision,
            "training_period": period,
            "universe_id": self.dataset.universe_id,
        }
        trial_sha256 = hashlib.sha256(_canonical(trial)).hexdigest()
        metrics = probability_metrics or [{"name": fitted.metric_name, "value": fitted.metric_value}]
        run_record = {
            "schema_version": 7,
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
            "schema_version": 9,
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
            "output_measurement": output_measurement,
            "output_statistic": output_statistic,
            "output_semantics": output_semantics,
        }
        if artifact_format == "native":
            candidate = BundleCandidate(
                metadata, artifact, run_record, calibration_artifacts=calibration_artifacts
            )
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
            candidate = BundleCandidate.onnx(
                metadata, onnx_artifact, run_record, calibration_artifacts=calibration_artifacts
            )
        authority = {
            "schema_version": 8,
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
            "output_measurement": output_measurement,
            "output_statistic": output_statistic,
            "output_semantics": output_semantics,
        }
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
            or specification.seed != self.seed
        ):
            raise TrainingValidationError("forecast specification is invalid")
        config = self._validated_config("linear", "onnx")
        output_measurement = config["output_measurement"]
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
        rows, targets, splits, split_sha256, split_counts, period, examples = _dataset_matrix(
            self.dataset,
            config["features"],
            config["label"],
            self.missing_policy,
            context,
        )
        if len({example["instrument_id"] for example in examples}) != 1:
            raise TrainingValidationError("lagged research forecasting requires one instrument series")
        observed = targets
        exogenous = rows
        selected_cutoffs = [example["partition_coordinate_unix_nanos"] for example in examples]
        target_ends = [example["label_effective_unix_nanos"] for example in examples]
        if (
            len(selected_cutoffs) != len(observed)
            or not selected_cutoffs
            or any(type(value) is not int for value in selected_cutoffs)
            or self.dataset.split_policy.mapping()["kind"] != "exact_time"
            or selected_cutoffs != sorted(selected_cutoffs)
            or any(type(value) is not int for value in target_ends)
            or specification.observed_cutoff_unix_nanos != max(target_ends)
            or specification.observed_cutoff_unix_nanos
            > self.dataset.identity.selection_as_of_unix_nanos
        ):
            raise TrainingValidationError("forecast cutoff differs from the exact PIT history")
        try:
            fit = fit_forecast(
                observed,
                specification,
                chronology=ForecastChronology(
                    tuple(selected_cutoffs),
                    tuple(example["label_maturity_unix_nanos"] for example in examples),
                    self.dataset.split_policy.boundaries,
                ),
                exogenous=exogenous,
                future_exogenous=future_exogenous,
                quantile_intervals=True,
                conformal_method=conformal_method,
                dependence_assumptions=dependence_assumptions,
            )
        except ForecastValidationError as error:
            raise TrainingValidationError("research forecast fit was rejected") from error
        _checkpoint(context)
        output_statistic = _research_output_statistic(
            config["output_target"], specification, fit.estimator_parameters
        )
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
            "fit_window": {
                "kind": "exact_time",
                "start_unix_nanos": selected_cutoffs[start_index],
                "end_unix_nanos": self.dataset.split_policy.validation_end_unix_nanos + 1,
                "observations": selected.calibration_observations,
            },
            "coverage_evaluation": {
                "window": {
                    "kind": "exact_time",
                    "start_unix_nanos": selected_cutoffs[selected.evaluation_start_index],
                    "end_unix_nanos": self.dataset.split_policy.test_end_unix_nanos + 1,
                    "observations": selected.evaluation_observations,
                },
                "realized": [{"covered": band.realized_covered, "total": band.realized_total} for band in selected.bands],
            },
            "dependence_assumptions": selected.dependence_assumptions,
            "residuals_sha256": selected.residuals_sha256,
            "bands": [
                {
                    "target_coverage_basis_points": int(
                        band.target_coverage * 10_000
                    ),
                    "lower_offset": band.lower_offset,
                    "upper_offset": band.upper_offset,
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
            "output_measurement": output_measurement,
            "output_statistic": output_statistic,
            "seed": self.seed,
            "split_counts": split_counts,
            "split_sha256": split_sha256,
            "training_code_revision": self.training_code_revision,
            "training_period": period,
            "universe_id": self.dataset.universe_id,
            "forecast": {
                "estimator_parameters": dict(fit.estimator_parameters),
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
            "schema_version": 7,
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
            MappingProxyType(dict(output_measurement)),
            MappingProxyType(dict(output_statistic)),
            self.dataset,
        )

    def _validated_config(
        self, model_kind: str, artifact_format: str
    ) -> dict[str, Any]:
        if not isinstance(self.environment, TrainingEnvironmentReceipt):
            raise TypeError("training environment must be the native provenance receipt")
        current_environment = training_environment_receipt()
        if (
            self.environment.sha256 != current_environment.sha256
            or self.environment.origin != current_environment.origin
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
        if self.dataset.identity.study is not None and self.dataset.identity.study.purpose != "training":
            raise TrainingValidationError("label-free study inputs cannot fit a model")
        population = self.dataset.population.basis
        study = self.dataset.identity.study
        if population == "current_listed_snapshot" or (
                population == "present_day_fixed_cohort" and (
                    study is None or study.basis != "retrospective_frozen_snapshot"
                    or study.purpose != "training" or study.target_horizon.kind not in {"fiscal_periods", "exact_elapsed"})):
            raise TrainingValidationError("population does not authorize this training recipe")
        dataset = dict(self.dataset.identity.bundle_mapping())
        label = dict(self.label.mapping() if isinstance(self.label, ComponentIdentity) else self.label)
        if set(label) != {"kind", "scope", "corporate_action_sensitivity", "name", "version"}:
            raise TrainingValidationError("label identity is incomplete")
        if label["kind"] != "label" or label["scope"] != "instrument":
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
        if admitted_label.measurement is None:
            raise TrainingValidationError(
                "training requires a measurement-bound Task 11 label"
            )
        output_measurement = dict(admitted_label.measurement.mapping())
        output_target = dict(admitted_label.target.mapping())
        compatible = (
            model_kind == "linear"
            and output_measurement["kind"]
            in {"price", "return", "financial_amount", "other_regression"}
        ) or (
            model_kind == "logistic"
            and output_measurement["kind"] == "probability"
        )
        if not compatible:
            raise TrainingValidationError(
                "model output semantics contradict the admitted label measurement"
            )
        if model_kind == "logistic":
            event_labels = {
                "price_higher": "research.fixed-horizon-price-higher",
                "benchmark_outperformance": "research.fixed-horizon-benchmark-outperformance",
                "profit_after_costs": "research.fixed-horizon-profit-after-costs",
            }
            if (
                output_target.get("kind") != "fixed_horizon_event"
                or output_target.get("origin_basis") not in {
                    "completed_bar_close", "named_session_close_for_nominal_daily_bar",
                }
                or label["name"] != event_labels.get(output_target.get("event", {}).get("kind"))
                or label["version"] != 1
                or label["corporate_action_sensitivity"] != "requires_adjustment"
            ):
                raise TrainingValidationError("probability requires an original fixed-horizon event label")
        return {
            "dataset": dataset,
            "label": label,
            "features": features,
            "output_measurement": output_measurement,
            "output_target": output_target,
        }


def _output_statistic(
    target: Mapping[str, Any], measurement: Mapping[str, Any], model_kind: str,
    label: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Seal the direct arithmetic target and squared-error estimator meaning."""

    if model_kind == "linear":
        statistic = (
            "model_estimated_conditional_mean"
            if (
                measurement["kind"] == "price"
                and target["kind"] == "fixed_horizon_terminal"
            ) or _direct_return_target(target, measurement, label) or (
                measurement["kind"] == "financial_amount"
                and target["kind"] == "financial_period"
                and target["cadence"] in {"annual", "quarterly"}
                and label["name"] == "research.fiscal-forward-financial-amount"
                and label["version"] == 1
            )
            else "unavailable"
        )
        objective = "squared_error"
        output_transform = "identity"
        estimator = {"kind": "sealed_direct_least_squares_v1"}
    else:
        statistic = "unavailable"
        objective = "binary_cross_entropy"
        output_transform = "logistic"
        estimator = {"kind": "sealed_binary_logistic_v1"}
    return {
        "statistic": statistic,
        "target": dict(target),
        "target_transform": "identity",
        "output_transform": output_transform,
        "objective": objective,
        "estimator": estimator,
    }


def _direct_return_target(
    target: Mapping[str, Any], measurement: Mapping[str, Any], label: Mapping[str, Any]
) -> bool:
    return (
        measurement["kind"] == "return"
        and target["kind"] == "fixed_horizon_terminal"
        and target["origin_basis"] in {"completed_bar_close", "named_session_close_for_nominal_daily_bar"}
        and label["name"] == "research.fixed-horizon-forward-return"
        and label["version"] == 1
        and label["corporate_action_sensitivity"] == "requires_adjustment"
    )


@dataclass(frozen=True)
class _ProbabilityFitControl:
    """Use the locked sklearn callback contract to bound every L-BFGS iteration."""

    operation: OperationContext
    observations: int

    def setup(self, estimator: Any, context: Any) -> None:
        _checkpoint(self.operation)

    def teardown(self, estimator: Any, context: Any) -> None:
        _checkpoint(self.operation)

    def on_fit_task_begin(self, estimator: Any, context: Any, **kwargs: Any) -> None:
        _checkpoint(self.operation)
        if context.task_name == "lbfgs-iter":
            # Locked sklearn 1.9 L-BFGS allows 50 line-search evaluations per
            # iteration. Reserve before work, including the initial evaluation,
            # for the one-feature binary loss/gradient and two-coordinate solve.
            _admit_operation_context(
                self.operation, _checked_mul(self.observations, 51 * 64),
            )

    def on_fit_task_end(self, estimator: Any, context: Any, **kwargs: Any) -> bool:
        _checkpoint(self.operation)
        return False


def _binary_event_calibration(
    dataset: DatasetResult,
    fitted: _FittedModel,
    rows: Sequence[Sequence[float]],
    targets: Sequence[float],
    examples: Sequence[Mapping[str, Any]],
    target: Mapping[str, Any],
    artifact_format: str,
    split_sha256: str,
    training_period: Mapping[str, Any],
    context: OperationContext,
) -> tuple[_FittedModel, Mapping[str, bytes], list[Mapping[str, Any]]]:
    """Fit validation-only calibration and retain untouched original test outcomes.

    The dataset's native receipt owns every binary outcome and its source proof.
    This function neither derives outcomes from returns nor chooses origins.
    Calibration changes only the frozen affine coefficients, so the existing
    native and ONNX inference owners execute the calibrated probability directly.
    """

    import numpy as np
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.linear_model import LogisticRegression

    _checkpoint(context)
    policy = dataset.split_policy
    if policy.kind != "exact_time" or target["kind"] != "fixed_horizon_event":
        raise TrainingValidationError("binary event calibration requires exact chronological partitions")
    boundaries = dict(zip(("train", "validation", "test"), policy.boundaries, strict=True))
    partitions: dict[str, list[int]] = {name: [] for name in boundaries}
    coordinate_kind = 3 if target["origin_basis"] == "completed_bar_close" else 5
    for index, example in enumerate(examples):
        if index % CONTROL_CHECK_INTERVAL == 0:
            _checkpoint(context)
        split = example["split"]
        origin = example["partition_coordinate_unix_nanos"]
        maturity = example["label_maturity_unix_nanos"]
        observed = example["observed_effective_unix_nanos"]
        terminal = example["label_effective_unix_nanos"]
        decision = example["decision_unix_nanos"]
        if (
            split not in partitions
            or any(type(value) is not int for value in (origin, maturity, observed, terminal, decision))
            or example["target_coordinate_kind"] != coordinate_kind
            or not observed <= decision < terminal
            or terminal - observed != target["horizon_nanos"]
            or not origin <= maturity <= boundaries[split]
            or policy.split_for(origin) != split
            or targets[index] not in {0.0, 1.0}
        ):
            raise TrainingValidationError("binary event chronology or original label is invalid")
        partitions[split].append(index)
    train, calibration, test = (partitions[name] for name in ("train", "validation", "test"))
    if (
        {targets[index] for index in train} != {0.0, 1.0}
        or {targets[index] for index in calibration} != {0.0, 1.0}
        or len({examples[index]["partition_coordinate"] for index in calibration}) < 2
        or len({examples[index]["partition_coordinate"] for index in test}) < 2
        or boundaries["test"] >= 2**63 - 1
    ):
        raise TrainingValidationError("binary event calibration or untouched evaluation is insufficient")

    def logits(model: _FittedModel, indices: Sequence[int]) -> Iterator[float]:
        if artifact_format == "onnx":
            yield from _onnx_scores(model, "linear", rows, indices, context)
        else:
            for index in indices:
                _checkpoint(context)
                yield _fitted_score(model, rows[index], context)

    # Test rows do not enter the calibrator, normalizers, or initial fit. Even
    # their raw scores are generated only after the final coefficients freeze.
    raw_calibration = list(logits(fitted, calibration))
    calibrator = LogisticRegression(
        C=1_000_000.0, l1_ratio=0.0, fit_intercept=True,
        solver="lbfgs", max_iter=400, tol=1e-10,
    )
    calibrator.set_callbacks(_ProbabilityFitControl(context, len(calibration)))
    _checkpoint(context)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", ConvergenceWarning)
            calibrator.fit(
                np.asarray(raw_calibration, dtype=np.float64).reshape(-1, 1),
                np.asarray([targets[index] for index in calibration], dtype=np.float64),
            )
    except TrainingValidationError:
        raise
    except (ConvergenceWarning, ArithmeticError, ValueError) as error:
        raise TrainingValidationError("held-out probability calibration did not converge") from error
    _checkpoint(context)
    slope = float(calibrator.coef_[0, 0])
    intercept = float(calibrator.intercept_[0])
    weights = tuple(slope * value for value in fitted.weights)
    bias = slope * fitted.bias + intercept
    if any(not math.isfinite(value) for value in (slope, intercept, bias, *weights)):
        raise TrainingValidationError("calibrated probability coefficients are nonfinite")
    if artifact_format == "onnx":
        try:
            weights, bias = quantize_fitted_model(weights, bias)
        except OnnxEncodingError as error:
            raise TrainingValidationError("calibrated coefficients exceed the ONNX domain") from error
    calibrated = _FittedModel(weights, bias, fitted.means, fitted.scales, "accuracy", 0.0)
    held_out = calibration + test
    raw_scores = raw_calibration + list(logits(fitted, test))
    if artifact_format == "onnx":
        probabilities = _onnx_scores(calibrated, "logistic", rows, held_out, context)
    else:
        probabilities = (_sigmoid(score) for score in logits(calibrated, held_out))
    outcomes = bytearray(40 * len(held_out))
    bins: list[list[float | int]] = [[0, 0.0, 0.0] for _ in range(10)]
    brier = log_loss = 0.0
    correct = 0
    for position, (index, raw, probability) in enumerate(zip(held_out, raw_scores, probabilities, strict=True)):
        _checkpoint(context)
        transformed = slope * raw + intercept
        if (
            not math.isfinite(transformed)
            or not math.isfinite(probability)
            or not 0.0 <= probability <= 1.0
            or abs(probability - _sigmoid(transformed)) > (2e-5 if artifact_format == "onnx" else 1e-12)
        ):
            raise TrainingValidationError("emitted probability differs from the retained calibration")
        label = targets[index]
        example = examples[index]
        struct.pack_into(
            "<dddqq", outcomes, position * 40, raw, probability, label,
            example["partition_coordinate_unix_nanos"], example["label_maturity_unix_nanos"],
        )
        if position < len(calibration):
            continue
        brier += (probability - label) ** 2
        clipped = min(max(probability, 1e-15), 1.0 - 1e-15)
        log_loss -= label * math.log(clipped) + (1.0 - label) * math.log1p(-clipped)
        correct += (probability >= 0.5) == bool(label)
        selected = bins[min(int(probability * 10), 9)]
        selected[0] += 1
        selected[1] += probability
        selected[2] += label
    outcomes_bytes = bytes(outcomes)
    evaluation = {
        "brier_score": brier / len(test),
        "log_loss": log_loss / len(test),
        "reliability_bins": [
            {"count": count, "mean_probability": total / count if count else None,
             "observed_frequency": positives / count if count else None}
            for count, total, positives in bins
        ],
    }
    def window(indices: Sequence[int], boundary: int) -> Mapping[str, Any]:
        return {
            **_period_mapping(policy, examples[indices[0]]["partition_coordinate"], boundary + 1),
            "observations": len(indices),
        }
    policy_bytes = _canonical({
        "schema_version": 1,
        "method": "sigmoid_logit_affine_v1",
        "target": dict(target),
        "dataset_export_sha256": dataset.export_sha256,
        "dataset_selection_sha256": dataset.identity.selection_sha256,
        "split_sha256": split_sha256,
        "train_window": {**training_period, "observations": len(train)},
        "calibration_window": window(calibration, boundaries["validation"]),
        "evaluation_window": window(test, boundaries["test"]),
        "slope": slope,
        "intercept": intercept,
        "regularization_c": 1_000_000.0,
        "outcomes_sha256": hashlib.sha256(outcomes_bytes).hexdigest(),
        "evaluation": evaluation,
    })
    metrics = [
        {"name": "accuracy", "value": correct / len(test)},
        {"name": "log_loss", "value": evaluation["log_loss"]},
    ]
    calibrated = _FittedModel(
        weights, bias, fitted.means, fitted.scales, "accuracy", correct / len(test),
    )
    _checkpoint(context)
    return calibrated, {
        "calibration/probability-outcomes.bin": outcomes_bytes,
        "calibration/probability-policy.json": policy_bytes,
    }, metrics


def _sigmoid(score: float) -> float:
    if score >= 0.0:
        return 1.0 / (1.0 + math.exp(-score))
    exponential = math.exp(score)
    return exponential / (1.0 + exponential)


def _direct_forecast_calibration(
    dataset: DatasetResult,
    fitted: _FittedModel,
    rows: Sequence[Sequence[float]],
    targets: Sequence[float],
    examples: Sequence[Mapping[str, Any]],
    target: Mapping[str, Any],
    artifact_format: str,
    context: OperationContext,
) -> Mapping[str, bytes]:
    """Score advancing held-out origins with the exact frozen direct fit.

    The sealed dataset builder requires every label's *knowledge* cutoff to be
    no later than its split end. That conservative bound, rather than an assumed
    availability equal to label effective time, purges the train/validation/test
    boundaries. No later split changes the fitted normalizers or coefficients.
    """

    fiscal = target["kind"] == "financial_period"
    target_coordinate_kind = 4 if fiscal else {
        "completed_bar_close": 3, "exact_effective_timestamp": 1,
        "named_session_close_for_nominal_daily_bar": 5,
    }.get(target.get("origin_basis"))
    if target_coordinate_kind is None:
        raise TrainingValidationError("direct forecast origin basis is invalid")
    policy = dataset.split_policy
    train_end, validation_end, test_end = policy.boundaries
    ends = dict(zip(("train", "validation", "test"), (train_end, validation_end, test_end), strict=True))
    validation: list[float] = []
    test: list[float] = []
    validation_origins: set[int] = set()
    test_origins: set[int] = set()
    validation_maturities: list[int] = []
    test_maturities: list[int] = []
    onnx_scores = (
        _onnx_scores(
            fitted, "linear", rows,
            (index for index, example in enumerate(examples) if example["split"] != "train"),
            context,
        )
        if artifact_format == "onnx" else None
    )
    for index, example in enumerate(examples):
        if index % CONTROL_CHECK_INTERVAL == 0:
            _checkpoint(context)
        origin = example["partition_coordinate"]
        observed = example["observed_effective_unix_nanos"]
        terminal = example["label_effective_unix_nanos"]
        split = example["split"]
        if fiscal:
            binding = example["financial_target"]
            target_matches = (
                binding is not None and observed is None and terminal is None
                and binding["cadence"] == target["cadence"]
                and binding["target_ordinal"] - binding["observed_ordinal"] == target["periods_ahead"]
                and _reported_period_end(binding["observed_period"]) < _reported_period_end(binding["target_period"])
            )
        else:
            target_matches = (
                observed is not None and terminal is not None
                and example["decision_unix_nanos"] is not None
                and observed <= example["decision_unix_nanos"] < terminal
                and terminal - observed == target["horizon_nanos"]
            )
        if (
            example["target_coordinate_kind"] != target_coordinate_kind
            or not target_matches
            or example["label_maturity"] > ends[split]
            or policy.split_for(origin) != split
        ):
            raise TrainingValidationError("direct forecast target chronology is invalid")
        if split == "train":
            continue
        try:
            score = (
                next(onnx_scores) if onnx_scores is not None
                else _fitted_score(fitted, rows[index], context)
            )
        except OnnxEncodingError as error:
            raise TrainingValidationError("direct forecast exceeds finite ONNX arithmetic") from error
        residual = targets[index] - score
        if not math.isfinite(residual):
            raise TrainingValidationError("direct forecast residual is nonfinite")
        if split == "validation":
            validation.append(residual)
            validation_origins.add(origin)
            validation_maturities.append(example["label_maturity"])
        else:
            test.append(residual)
            test_origins.add(origin)
            test_maturities.append(example["label_maturity"])
    if len(validation_origins) < 2 or not test:
        raise TrainingValidationError("direct forecast needs rolling validation and held-out test outcomes")
    if test_end >= 2**63 - 1:
        raise TrainingValidationError("calibration availability window cannot be represented")
    ordered = sorted(validation)
    bands = []
    realized = []
    for coverage in (5_000, 8_000, 9_500):
        # Outward empirical order statistics, including the fitted mean. These
        # are residual quantiles, not a conformal or independent-sample promise.
        denominator = 20_000
        lower_index = (10_000 - coverage) * (len(ordered) - 1) // denominator
        upper_numerator = (10_000 + coverage) * (len(ordered) - 1)
        upper_index = (upper_numerator + denominator - 1) // denominator
        lower = min(0.0, ordered[lower_index])
        upper = max(0.0, ordered[upper_index])
        covered = 0
        for index, residual in enumerate(test):
            if index % CONTROL_CHECK_INTERVAL == 0:
                _checkpoint(context)
            covered += lower <= residual <= upper
        bands.append({
            "target_coverage_basis_points": coverage,
            "lower_offset": lower,
            "upper_offset": upper,
        })
        realized.append({"covered": covered, "total": len(test)})
    residuals = bytearray(8 * (len(validation) + len(test)))
    offset = 0
    for partition in (validation, test):
        for residual in partition:
            if offset % CONTROL_CHECK_INTERVAL == 0:
                _checkpoint(context)
            struct.pack_into("<d", residuals, 8 * offset, residual)
            offset += 1
    residual_bytes = bytes(residuals)
    policy_bytes = _canonical({
        "schema_version": 1,
        "kind": "residual_quantile",
        "method": "residual_quantile",
        "fit_window": {**_period_mapping(policy, min(validation_origins), validation_end + 1),
            "observations": len(validation)},
        "coverage_evaluation": {
            "window": {**_period_mapping(policy, min(test_origins), test_end + 1),
                "observations": len(test)},
            "realized": realized,
        },
        "dependence_assumptions": (
            "Frozen direct least-squares fit and normalizers use train only. Signed residuals "
            "retain validation then test order; counts follow missing policy in the training run. "
            "Validation outward empirical residual bounds are widened to include zero residual; "
            "coverage uses untouched test outcomes. "
            "Basis-qualified split ends bound label knowledge or economic maturity and purge targets. Origins advance "
            "without refitting; targets may overlap and remain dependent. Empirical coverage "
            "does not guarantee future coverage."
        ),
        "residuals_sha256": hashlib.sha256(residual_bytes).hexdigest(),
        "bands": bands,
    })
    _checkpoint(context)
    return {
        "calibration/residuals.f64le": residual_bytes,
        "calibration/policy.json": policy_bytes,
    }


def _fitted_score(
    fitted: _FittedModel, row: Sequence[float], context: OperationContext,
) -> float:
    """Use the native backend's bias-first finite affine accumulation."""

    score = fitted.bias
    for column, weight in enumerate(fitted.weights):
        if column % CONTROL_CHECK_INTERVAL == 0:
            _checkpoint(context)
        normalized = (row[column] - fitted.means[column]) / fitted.scales[column]
        contribution = normalized * weight
        score += contribution
        if not all(math.isfinite(value) for value in (normalized, contribution, score)):
            raise TrainingValidationError("direct forecast scoring is nonfinite")
    return score


def _onnx_scores(
    fitted: _FittedModel, model_kind: str, rows: Sequence[Sequence[float]],
    indices: Iterable[int], context: OperationContext,
) -> Iterator[float]:
    """Execute the exported graph with the existing locked ONNX reference evaluator.

    Gemm owns float32 accumulation and bias application. Rounding a handwritten
    affine loop after each addition is not equivalent. Only one bounded feature
    row is retained, and the configured environment binds the evaluator dependencies.
    """

    import numpy as np
    import onnx
    from onnx.reference import ReferenceEvaluator

    _checkpoint(context)
    graph = encode_fitted_model(fitted.weights, fitted.bias, model_kind=model_kind)
    evaluator = ReferenceEvaluator(onnx.load_model_from_string(graph))
    tensor = np.empty((1, len(fitted.weights)), dtype=np.float32)
    for index in indices:
        _checkpoint(context)
        for column in range(len(fitted.weights)):
            if column % CONTROL_CHECK_INTERVAL == 0:
                _checkpoint(context)
            tensor[0, column] = quantize_float32(
                (rows[index][column] - fitted.means[column]) / fitted.scales[column]
            )
        with np.errstate(over="raise", invalid="raise", divide="raise"):
            try:
                outputs = evaluator.run(["Y"], {"X": tensor})
            except (ArithmeticError, RuntimeError, ValueError) as error:
                raise OnnxEncodingError("ONNX candidate evaluation failed") from error
        if len(outputs) != 1 or outputs[0].shape != (1, 1):
            raise OnnxEncodingError("ONNX candidate evaluation shape is invalid")
        score = float(outputs[0][0, 0])
        if not math.isfinite(score) or (model_kind == "logistic" and not 0.0 <= score <= 1.0):
            raise OnnxEncodingError("ONNX candidate evaluation is nonfinite or invalid")
        _checkpoint(context)
        yield score


def _research_output_statistic(
    target: Mapping[str, Any], specification: ForecastSpecification,
    parameters: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Bind Ridge provenance while refusing expected-value semantics for offset-index paths."""

    estimator = {
        "kind": "sealed_direct_ridge_v1",
        "ridge_alpha": specification.ridge_alpha,
    }
    if parameters["conformal_method"] == "enbpi":
        estimator = {
            "kind": "sealed_oob_mean_block_bootstrap_ridge_v1",
            "ridge_alpha": specification.ridge_alpha,
            "resampling_block_length": parameters["resampling_block_length"],
            "resampling_count": parameters["resampling_count"],
            "resampling_seed": parameters["resampling_seed"],
        }
    return {
        "statistic": "unavailable",
        "target": dict(target),
        "target_transform": "identity",
        "output_transform": "identity",
        "objective": "squared_error",
        "estimator": estimator,
    }


def _dataset_matrix(
    dataset: DatasetResult,
    features: Sequence[Mapping[str, Any]],
    label: Mapping[str, Any],
    missing_policy: str,
    context: OperationContext,
) -> tuple[
    list[list[float]], list[float], list[str], str, Mapping[str, int],
    Mapping[str, int], Sequence[Mapping[str, Any]],
]:
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
    evidence: list[dict[str, Any]] = []
    feature_keys = [("feature", item["name"], item["version"]) for item in features]
    label_key = ("label", label["name"], label["version"])
    label_target = next(
        component.target.mapping() for component in dataset.components
        if (component.kind, component.name, component.version) == label_key
    )
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
        temporal = _training_temporal_evidence(dataset, group[0], label_target)
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
                "source_selection_as_of_unix_nanos": group[0]["source_selection_as_of"].unix_nanos,
                "label_selection_as_of_unix_nanos": group[0]["label_selection_as_of"].unix_nanos,
                **temporal,
                "observed_effective_unix_nanos": (
                    None
                    if group[0]["observed_effective_at"] is None
                    else group[0]["observed_effective_at"].unix_nanos
                ),
                "label_effective_unix_nanos": (
                    None
                    if group[0]["label_effective_at"] is None
                    else group[0]["label_effective_at"].unix_nanos
                ),
                "target_coordinate_kind": group[0]["target_coordinate_kind"],
                "example_id": group[0]["example_id"],
                "instrument_id": group[0]["instrument_id"],
                "split": split,
            }
        )
    if not rows:
        raise TrainingValidationError("missing policy removed every training row")
    # Native physical rows use decision order. Historical knowledge partitions
    # and retrospective economic partitions remain independently explicit.
    order = sorted(range(len(evidence)), key=lambda index: (
        evidence[index]["partition_coordinate"],
        evidence[index]["instrument_id"], evidence[index]["example_id"]))
    rows = [rows[index] for index in order]
    targets = [targets[index] for index in order]
    admitted = [admitted[index] for index in order]
    evidence = [evidence[index] for index in order]
    _checkpoint(context)
    split_sha256 = hashlib.sha256(
        _canonical(
            {
                "dataset_export_sha256": dataset.export_sha256,
                "examples": evidence,
                "schema_version": 2,
            }
        )
    ).hexdigest()
    counts = {name: admitted.count(name) for name in ("train", "validation", "test")}
    training_cutoffs = [
        evidence[index]["partition_coordinate"]
        for index, split in enumerate(admitted)
        if split == "train"
    ]
    if not training_cutoffs or max(training_cutoffs) >= 2**63 - 1:
        raise TrainingValidationError("training period cannot be represented exactly")
    period = _period_mapping(dataset.split_policy, min(training_cutoffs),
        max(example["label_maturity"] for example in evidence if example["split"] == "train") + 1)
    # Component identities have entered the split digest; calibration needs only
    # the small temporal coordinates, not a second retained feature manifest.
    for example in evidence:
        del example["components"]
    return rows, targets, admitted, split_sha256, counts, period, evidence


def _period_mapping(policy: Any, start: int, end: int) -> dict[str, Any]:
    if end <= start:
        raise TrainingValidationError("training interval is not ordered")
    if policy.mapping()["kind"] == "exact_time":
        if not -(2**63) <= start < end < 2**63:
            raise TrainingValidationError("training interval exceeds timestamp bounds")
        return {"kind": "exact_time", "start_unix_nanos": start, "end_unix_nanos": end}
    def calendar(days: int) -> dict[str, int]:
        try:
            value = date(1970, 1, 1) + timedelta(days=days)
        except (OverflowError, ValueError) as error:
            raise TrainingValidationError("fiscal interval exceeds native calendar bounds") from error
        return {"year": value.year, "month": value.month, "day": value.day}
    return {"kind": "fiscal_dates", "start": calendar(start), "end": calendar(end)}


def _reported_period_end(period: Mapping[str, Any]) -> date:
    value = period["instant"] if period["kind"] == "instant" else period["end"]
    try:
        return date(value["year"], value["month"], value["day"])
    except (ValueError, TypeError, KeyError) as error:
        raise TrainingValidationError("reported fiscal endpoint is invalid") from error


def _training_temporal_evidence(
    dataset: DatasetResult, row: Mapping[str, Any], target: Mapping[str, Any],
) -> dict[str, Any]:
    retrospective = dataset.identity.study is not None and dataset.identity.study.basis == "retrospective_frozen_snapshot"
    decision = row["decision_at"] if row["decision_at"] is not None else row["decision_on"]
    partition = decision if retrospective else row["source_selection_as_of"]
    financial = None
    if row["target_coordinate_kind"] == 4:
        raw = row["input_epoch_json"]
        if not isinstance(raw, bytes) or not 0 < len(raw) <= 64 * 1024:
            raise TrainingValidationError("fiscal source epoch is unavailable")
        epoch = json.loads(raw)
        if epoch["kind"] != "financial_period":
            raise TrainingValidationError("fiscal source epoch has another target")
        binding = epoch["input"]["period"]
        if binding["target_period"] is None:
            raise TrainingValidationError("unobserved fiscal targets cannot train a model")
        financial = {key: binding[key] for key in ("cadence", "observed_ordinal", "target_ordinal", "observed_period", "target_period")}
        mature = _reported_period_end(binding["target_period"]) if retrospective else row["label_selection_as_of"]
    else:
        mature = row["label_effective_at"] if retrospective else row["label_selection_as_of"]
    origin_number = dataset.split_policy.coordinate(partition)
    maturity_number = dataset.split_policy.coordinate(mature)
    if target["kind"] == "fixed_horizon_event" and target["event"]["kind"] == "profit_after_costs":
        terminal = row["label_effective_at"]
        if terminal is None or dataset.split_policy.kind != "exact_time":
            raise TrainingValidationError("after-cost event lacks its original terminal coordinate")
        window_end = terminal.unix_nanos + target["event"]["policy"]["maximum_exit_lag_nanos"]
        if not -(2**63) <= window_end < 2**63:
            raise TrainingValidationError("after-cost event completion window overflows")
        maturity_number = window_end if retrospective else max(maturity_number, window_end)
    precision = dataset.split_policy.mapping()["kind"]
    return {
        "coordinate_precision": precision,
        "partition_coordinate": origin_number,
        "label_maturity": maturity_number,
        "decision_unix_nanos": None if row["decision_at"] is None else row["decision_at"].unix_nanos,
        "partition_coordinate_unix_nanos": origin_number if precision == "exact_time" else None,
        "label_maturity_unix_nanos": maturity_number if precision == "exact_time" else None,
        "financial_target": financial,
    }


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
    quantized = _FittedModel(
        weights, bias, fitted.means, fitted.scales, fitted.metric_name, fitted.metric_value
    )
    scores = list(_onnx_scores(quantized, model_kind, rows, validation, context))
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
        _checked_mul(_checked_mul(example_count, feature_count), 40),
    )
    common = _checked_add(
        common,
        _checked_mul(example_count, 64 + max(1, example_count.bit_length())),
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
    if model_kind == "logistic":
        # Calibration fits are admitted per bounded solver iteration. Reserve
        # the additional frozen-model scoring and outcome accounting here.
        estimate = _checked_add(
            estimate, _checked_mul(example_count, _checked_add(_checked_mul(feature_count, 48), 160)),
        )
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
