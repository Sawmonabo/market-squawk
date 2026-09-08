"""Bounded deterministic multi-horizon research forecasting.

This module deliberately does not share the live scalar-decision path.  It owns
lag/cutoff orchestration, temporal validation, and optional interval evidence;
the fitted sklearn estimator remains the only object exported to ONNX.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import hashlib
import importlib.metadata
import json
import math
from typing import Any, Mapping, Sequence

import numpy as np
from sklearn.base import clone
from sklearn.linear_model import Ridge
from sklearn.model_selection import TimeSeriesSplit
from sklearn.multioutput import MultiOutputRegressor, RegressorChain
from skl2onnx import to_onnx


MAX_FORECAST_OBSERVATIONS = 100_000
MAX_FORECAST_HORIZONS = 512
MAX_FORECAST_LAGS = 1_024
MAX_FORECAST_CELLS = 2_000_000
MAX_ROLLING_SPLITS = 32
TARGET_COVERAGES = (0.50, 0.80, 0.95)


class ForecastValidationError(ValueError):
    """A boundedness, chronology, finite-arithmetic, or calibration rule failed."""


class ForecastStrategy(StrEnum):
    """Closed deterministic central-forecast strategies."""

    DIRECT = "direct"
    RECURSIVE = "recursive"
    MULTI_OUTPUT = "multi_output"
    CHAINED = "chained"


class IntervalKind(StrEnum):
    """Interval semantics; quantile bands are never relabelled as conformal."""

    QUANTILE = "residual_quantile"
    CONFORMAL = "mapie_time_series_conformal"


class ConformalMethod(StrEnum):
    """Admitted MAPIE time-series methods."""

    ENBPI = "enbpi"
    ACI = "aci"


@dataclass(frozen=True)
class ForecastSpecification:
    """One exact lag/horizon/cutoff policy."""

    strategy: ForecastStrategy
    horizons: tuple[int, ...]
    lags: tuple[int, ...]
    observed_cutoff_unix_nanos: int
    seed: int
    rolling_splits: int = 5
    ridge_alpha: float = 1.0


@dataclass(frozen=True)
class IntervalBand:
    """One finite interval and its observed marginal validation coverage."""

    target_coverage: float
    lower: tuple[float, ...]
    upper: tuple[float, ...]
    realized_covered: int
    realized_total: int


@dataclass(frozen=True)
class IntervalEvidence:
    """Typed interval result plus immutable calibration artifact evidence."""

    kind: IntervalKind
    method: str
    calibration_start_index: int
    calibration_end_index: int
    dependence_assumptions: str
    bands: tuple[IntervalBand, ...]
    residuals_bytes: bytes
    residuals_sha256: str
    policy_bytes: bytes
    policy_sha256: str


@dataclass(frozen=True)
class RollingOriginEvidence:
    """Proper point-loss and selection evidence from chronological folds."""

    mean_absolute_error: float
    mean_squared_error: float
    split_count: int
    prediction_count: int
    selection_sha256: str


@dataclass(frozen=True)
class ForecastFit:
    """Central path and bundle-ready immutable research evidence."""

    central: tuple[float, ...]
    target_offsets: tuple[int, ...]
    observed_cutoff_unix_nanos: int
    strategy: ForecastStrategy
    validation: RollingOriginEvidence
    quantile_intervals: IntervalEvidence | None
    conformal_intervals: IntervalEvidence | None
    estimator_parameters: Mapping[str, Any]
    package_versions: Mapping[str, str]
    onnx_bytes: bytes
    onnx_sha256: str


def fit_forecast(
    observed: Sequence[float],
    specification: ForecastSpecification,
    *,
    exogenous: Sequence[Sequence[float]] | None = None,
    future_exogenous: Sequence[Sequence[float]] | None = None,
    quantile_intervals: bool = True,
    conformal_method: ConformalMethod | None = None,
    dependence_assumptions: str | None = None,
) -> ForecastFit:
    """Fit one bounded deterministic path strictly after the supplied cutoff.

    Missing or unrequested conformal calibration returns ``None``.  A requested
    MAPIE method either produces finite nested 50/80/95 bands or raises; this
    function never substitutes a synthetic band.
    """

    spec = _validate_specification(specification)
    values = _finite_matrix(observed, "observed values", one_dimensional=True).reshape(-1)
    if len(values) > MAX_FORECAST_OBSERVATIONS:
        raise ForecastValidationError("observation count exceeds its hard bound")
    external = _optional_features(exogenous, len(values), "historical exogenous features")
    future = _optional_features(
        future_exogenous,
        max(spec.horizons),
        "future exogenous features",
        allow_longer=True,
    )
    if external.shape[1] != future.shape[1] and future.size:
        raise ForecastValidationError("historical and future exogenous widths differ")
    if external.shape[1] and not future.size:
        raise ForecastValidationError("future exogenous features are required")

    x, y, origins = _supervised(values, external, spec)
    if x.shape[0] <= spec.rolling_splits + 1:
        raise ForecastValidationError("history is insufficient for temporal validation")
    if x.size + y.size > MAX_FORECAST_CELLS:
        raise ForecastValidationError("forecast matrix exceeds its retained-cell bound")

    validation_predictions, validation_actuals, selection = _rolling_origin(x, y, origins, spec)
    estimator = _estimator(spec)
    estimator.fit(x, _fit_targets(y, spec.strategy))
    central = _future_path(estimator, values, external, future, spec)
    _finite_vector(central, "central forecast")

    residuals = validation_actuals - validation_predictions
    quantile = (
        _residual_intervals(
            central,
            residuals,
            IntervalKind.QUANTILE,
            "empirical_absolute_residual_quantiles",
            "rolling-origin residuals are treated as marginal empirical errors; coverage is not a per-observation guarantee",
            int(origins[0]),
            int(origins[-1] + 1),
        )
        if quantile_intervals
        else None
    )
    conformal = None
    if conformal_method is not None:
        if not dependence_assumptions:
            raise ForecastValidationError("conformal dependence assumptions are required")
        conformal = _mapie_intervals(
            x,
            y,
            central,
            validation_predictions,
            validation_actuals,
            origins,
            spec,
            conformal_method,
            dependence_assumptions,
        )

    onnx = _export_onnx(estimator, x)
    parameters = {
        "strategy": spec.strategy.value,
        "horizons": list(spec.horizons),
        "lags": list(spec.lags),
        "ridge_alpha": spec.ridge_alpha,
        "rolling_splits": spec.rolling_splits,
        "seed": spec.seed,
    }
    versions = {
        name: importlib.metadata.version(name)
        for name in ("numpy", "scikit-learn", "mapie", "skl2onnx", "onnx")
    }
    return ForecastFit(
        central=tuple(float(value) for value in central),
        target_offsets=spec.horizons,
        observed_cutoff_unix_nanos=spec.observed_cutoff_unix_nanos,
        strategy=spec.strategy,
        validation=selection,
        quantile_intervals=quantile,
        conformal_intervals=conformal,
        estimator_parameters=parameters,
        package_versions=versions,
        onnx_bytes=onnx,
        onnx_sha256=hashlib.sha256(onnx).hexdigest(),
    )


def _validate_specification(value: ForecastSpecification) -> ForecastSpecification:
    if not isinstance(value, ForecastSpecification):
        raise TypeError("forecast specification is required")
    try:
        strategy = ForecastStrategy(value.strategy)
    except ValueError as error:
        raise ForecastValidationError("forecast strategy is unsupported") from error
    if (
        not value.horizons
        or len(value.horizons) > MAX_FORECAST_HORIZONS
        or tuple(sorted(set(value.horizons))) != value.horizons
        or any(not isinstance(item, int) or isinstance(item, bool) or item <= 0 for item in value.horizons)
    ):
        raise ForecastValidationError("forecast horizons must be unique increasing positive offsets")
    if (
        not value.lags
        or len(value.lags) > MAX_FORECAST_LAGS
        or tuple(sorted(set(value.lags))) != value.lags
        or any(not isinstance(item, int) or isinstance(item, bool) or item <= 0 for item in value.lags)
    ):
        raise ForecastValidationError("forecast lags must be unique increasing positive offsets")
    if not isinstance(value.seed, int) or isinstance(value.seed, bool) or not 0 <= value.seed < 2**32:
        raise ForecastValidationError("forecast seed is invalid")
    if not 2 <= value.rolling_splits <= MAX_ROLLING_SPLITS:
        raise ForecastValidationError("rolling-origin split count is invalid")
    if not math.isfinite(value.ridge_alpha) or value.ridge_alpha < 0.0:
        raise ForecastValidationError("ridge alpha is invalid")
    return ForecastSpecification(
        strategy,
        value.horizons,
        value.lags,
        value.observed_cutoff_unix_nanos,
        value.seed,
        value.rolling_splits,
        value.ridge_alpha,
    )


def _finite_matrix(values: Any, name: str, *, one_dimensional: bool = False) -> np.ndarray:
    try:
        result = np.asarray(values, dtype=np.float64)
    except (TypeError, ValueError) as error:
        raise ForecastValidationError(f"{name} are not numeric") from error
    expected = 1 if one_dimensional else 2
    if result.ndim != expected or not result.size or not np.isfinite(result).all():
        raise ForecastValidationError(f"{name} must be a nonempty finite {expected}-D array")
    return result


def _optional_features(
    values: Sequence[Sequence[float]] | None,
    rows: int,
    name: str,
    *,
    allow_longer: bool = False,
) -> np.ndarray:
    if values is None:
        return np.empty((rows, 0), dtype=np.float64)
    result = _finite_matrix(values, name)
    if (allow_longer and result.shape[0] < rows) or (not allow_longer and result.shape[0] != rows):
        raise ForecastValidationError(f"{name} row count is invalid")
    return result


def _supervised(
    values: np.ndarray,
    external: np.ndarray,
    spec: ForecastSpecification,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    first_origin = max(spec.lags)
    final_origin = len(values) - max(spec.horizons)
    if final_origin <= first_origin:
        raise ForecastValidationError("history is shorter than the lag and horizon contract")
    origins = np.arange(first_origin, final_origin, dtype=np.int64)
    x = np.asarray(
        [
            [*(values[origin - lag] for lag in spec.lags), *external[origin]]
            for origin in origins
        ],
        dtype=np.float64,
    )
    y = np.asarray(
        [[values[origin + horizon] for horizon in spec.horizons] for origin in origins],
        dtype=np.float64,
    )
    return x, y, origins


def _estimator(spec: ForecastSpecification):
    base = Ridge(alpha=spec.ridge_alpha, solver="svd")
    if spec.strategy is ForecastStrategy.DIRECT:
        return MultiOutputRegressor(base, n_jobs=1)
    if spec.strategy is ForecastStrategy.CHAINED:
        return RegressorChain(base, order=list(range(len(spec.horizons))), random_state=spec.seed)
    return base


def _fit_targets(y: np.ndarray, strategy: ForecastStrategy) -> np.ndarray:
    if strategy is ForecastStrategy.RECURSIVE:
        return y[:, 0]
    return y


def _rolling_origin(
    x: np.ndarray,
    y: np.ndarray,
    origins: np.ndarray,
    spec: ForecastSpecification,
) -> tuple[np.ndarray, np.ndarray, RollingOriginEvidence]:
    predicted: list[np.ndarray] = []
    actual: list[np.ndarray] = []
    selections: list[Mapping[str, Any]] = []
    splitter = TimeSeriesSplit(n_splits=spec.rolling_splits)
    for fold, (train, validation) in enumerate(splitter.split(x)):
        estimator = _estimator(spec)
        estimator.fit(x[train], _fit_targets(y[train], spec.strategy))
        if spec.strategy is ForecastStrategy.RECURSIVE:
            one = np.asarray(estimator.predict(x[validation]), dtype=np.float64).reshape(-1, 1)
            fold_prediction = np.repeat(one, len(spec.horizons), axis=1)
        else:
            fold_prediction = np.asarray(estimator.predict(x[validation]), dtype=np.float64)
        _finite_vector(fold_prediction.reshape(-1), "rolling-origin predictions")
        predicted.append(fold_prediction)
        actual.append(y[validation])
        selections.append(
            {
                "fold": fold,
                "train_origin_start": int(origins[train[0]]),
                "train_origin_end": int(origins[train[-1]]),
                "validation_origin_start": int(origins[validation[0]]),
                "validation_origin_end": int(origins[validation[-1]]),
            }
        )
    predictions = np.concatenate(predicted)
    actuals = np.concatenate(actual)
    errors = actuals - predictions
    mae = float(np.mean(np.abs(errors)))
    mse = float(np.mean(np.square(errors)))
    if not math.isfinite(mae) or not math.isfinite(mse):
        raise ForecastValidationError("rolling-origin loss is nonfinite")
    evidence = _canonical_json(
        {"schema_version": 1, "specification": _spec_mapping(spec), "splits": selections}
    )
    return predictions, actuals, RollingOriginEvidence(
        mae, mse, spec.rolling_splits, int(errors.size), hashlib.sha256(evidence).hexdigest()
    )


def _future_path(estimator, values, external, future, spec) -> np.ndarray:
    if spec.strategy is not ForecastStrategy.RECURSIVE:
        feature = np.asarray(
            [[*(values[len(values) - lag] for lag in spec.lags), *future[0]]],
            dtype=np.float64,
        )
        result = np.asarray(estimator.predict(feature), dtype=np.float64).reshape(-1)
        return result
    history = list(float(value) for value in values)
    results: dict[int, float] = {}
    for step in range(1, max(spec.horizons) + 1):
        feature = np.asarray(
            [[*(history[len(history) - lag] for lag in spec.lags), *future[step - 1]]],
            dtype=np.float64,
        )
        prediction = float(estimator.predict(feature)[0])
        if not math.isfinite(prediction):
            raise ForecastValidationError("recursive forecast is nonfinite")
        history.append(prediction)
        if step in spec.horizons:
            results[step] = prediction
    return np.asarray([results[horizon] for horizon in spec.horizons], dtype=np.float64)


def _residual_intervals(
    central: np.ndarray,
    residuals: np.ndarray,
    kind: IntervalKind,
    method: str,
    assumptions: str,
    start: int,
    end: int,
) -> IntervalEvidence:
    absolute = np.abs(residuals.reshape(-1))
    widths = np.quantile(absolute, TARGET_COVERAGES, method="higher")
    bands = []
    for coverage, width in zip(TARGET_COVERAGES, widths, strict=True):
        lower = central - float(width)
        upper = central + float(width)
        covered = int(np.count_nonzero(absolute <= width))
        bands.append(
            IntervalBand(
                coverage,
                tuple(float(value) for value in lower),
                tuple(float(value) for value in upper),
                covered,
                int(absolute.size),
            )
        )
    return _interval_evidence(kind, method, start, end, assumptions, bands, residuals)


def _mapie_intervals(
    x,
    y,
    central,
    validation_predictions,
    validation_actuals,
    origins,
    spec,
    method,
    assumptions,
) -> IntervalEvidence:
    from mapie.regression import TimeSeriesRegressor
    from mapie.subsample import BlockBootstrap

    estimator = Ridge(alpha=spec.ridge_alpha, solver="svd")
    block_length = max(1, int(math.sqrt(len(x))))
    cv = BlockBootstrap(
        n_resamplings=min(30, max(2, len(x) // block_length)),
        length=block_length,
        overlapping=False,
        random_state=spec.seed,
    )
    calibrator = TimeSeriesRegressor(
        estimator=estimator,
        method=method.value,
        cv=cv,
        n_jobs=1,
        agg_function="mean",
        random_state=spec.seed,
    )
    # MAPIE's maintained time-series regressor is scalar.  The admitted policy
    # calibrates the first horizon, then applies its marginal residual offsets to
    # every central point; that dependence limitation is retained verbatim.
    calibrator.fit(x, y[:, 0])
    feature = x[-1:].copy()
    _, bounds = calibrator.predict(
        feature,
        ensemble=True,
        confidence_level=list(TARGET_COVERAGES),
        optimize_beta=False,
        allow_infinite_bounds=False,
    )
    bounds = np.asarray(bounds, dtype=np.float64)
    if bounds.shape != (1, 2, 3) or not np.isfinite(bounds).all():
        raise ForecastValidationError("MAPIE returned an unsupported interval shape")
    predicted_first = float(calibrator.predict(feature)[0])
    lower_offsets = bounds[0, 0, :] - predicted_first
    upper_offsets = bounds[0, 1, :] - predicted_first
    residuals = validation_actuals - validation_predictions
    first_residuals = residuals[:, 0]
    bands = []
    for index, coverage in enumerate(TARGET_COVERAGES):
        lower = central + float(lower_offsets[index])
        upper = central + float(upper_offsets[index])
        covered = int(
            np.count_nonzero(
                (first_residuals >= lower_offsets[index])
                & (first_residuals <= upper_offsets[index])
            )
        )
        bands.append(
            IntervalBand(
                coverage,
                tuple(float(value) for value in lower),
                tuple(float(value) for value in upper),
                covered,
                int(first_residuals.size),
            )
        )
    return _interval_evidence(
        IntervalKind.CONFORMAL,
        f"mapie_{method.value}",
        int(origins[0]),
        int(origins[-1] + 1),
        assumptions,
        bands,
        first_residuals,
    )


def _interval_evidence(kind, method, start, end, assumptions, bands, residuals):
    if not assumptions or any(ord(character) < 32 for character in assumptions):
        raise ForecastValidationError("interval dependence assumptions are invalid")
    previous_lower = None
    previous_upper = None
    for band in bands:
        lower = np.asarray(band.lower)
        upper = np.asarray(band.upper)
        if not np.isfinite(lower).all() or not np.isfinite(upper).all() or np.any(lower > upper):
            raise ForecastValidationError("interval values are nonfinite or unordered")
        if previous_lower is not None and (np.any(lower > previous_lower) or np.any(upper < previous_upper)):
            raise ForecastValidationError("interval values are not nested")
        previous_lower, previous_upper = lower, upper
    residuals_bytes = np.asarray(residuals, dtype="<f8").tobytes(order="C")
    policy = _canonical_json(
        {
            "schema_version": 1,
            "kind": kind.value,
            "method": method,
            "target_coverages": list(TARGET_COVERAGES),
            "calibration_start_index": start,
            "calibration_end_index": end,
            "dependence_assumptions": assumptions,
        }
    )
    return IntervalEvidence(
        kind,
        method,
        start,
        end,
        assumptions,
        tuple(bands),
        residuals_bytes,
        hashlib.sha256(residuals_bytes).hexdigest(),
        policy,
        hashlib.sha256(policy).hexdigest(),
    )


def _export_onnx(estimator, x: np.ndarray) -> bytes:
    try:
        model = to_onnx(estimator, x[:1].astype(np.float32), target_opset=13)
        encoded = model.SerializeToString(deterministic=True)
    except Exception as error:  # sklearn-onnx exposes multiple converter exception classes
        raise ForecastValidationError("central forecast cannot be exported to admitted ONNX") from error
    if not encoded:
        raise ForecastValidationError("central ONNX artifact is empty")
    return encoded


def _finite_vector(values: np.ndarray, name: str) -> None:
    if not values.size or not np.isfinite(values).all():
        raise ForecastValidationError(f"{name} are empty or nonfinite")


def _spec_mapping(spec: ForecastSpecification) -> Mapping[str, Any]:
    return {
        "strategy": spec.strategy.value,
        "horizons": list(spec.horizons),
        "lags": list(spec.lags),
        "observed_cutoff_unix_nanos": spec.observed_cutoff_unix_nanos,
        "seed": spec.seed,
        "rolling_splits": spec.rolling_splits,
        "ridge_alpha": spec.ridge_alpha,
    }


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
