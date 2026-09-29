"""Bounded deterministic multi-horizon research forecasting.

This advanced training procedure owns lag orchestration, chronological fitting,
and interval evidence. Its fitted linear predictor is exported to the existing
admitted ONNX runtime. Row-index outputs remain research diagnostics until the
same target contract admits genuine economic coordinates and horizon evidence.
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
from sklearn.linear_model import Ridge
from sklearn.model_selection import TimeSeriesSplit
from sklearn.multioutput import MultiOutputRegressor, RegressorChain
from skl2onnx import to_onnx
from skl2onnx.common.data_types import FloatTensorType


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
class ForecastChronology:
    """Predeclared economic/knowledge coordinates for every retained observation."""

    origins: tuple[int, ...]
    label_ends: tuple[int, ...]
    partition_ends: tuple[int, int, int]


@dataclass(frozen=True)
class IntervalBand:
    """One finite interval and its observed marginal validation coverage."""

    target_coverage: float
    lower: tuple[float, ...]
    upper: tuple[float, ...]
    lower_offset: float
    upper_offset: float
    realized_covered: int
    realized_total: int


@dataclass(frozen=True)
class IntervalEvidence:
    """Typed interval result plus immutable calibration artifact evidence."""

    kind: IntervalKind
    method: str
    calibration_start_index: int
    calibration_end_index: int
    evaluation_start_index: int
    evaluation_end_index: int
    calibration_observations: int
    evaluation_observations: int
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
    input_lags: tuple[int, ...]
    exogenous_feature_count: int
    output_horizons: tuple[int, ...]


def fit_forecast(
    observed: Sequence[float],
    specification: ForecastSpecification,
    *,
    chronology: ForecastChronology,
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
    if conformal_method is not None:
        try:
            conformal_method = ConformalMethod(conformal_method)
        except ValueError as error:
            raise ForecastValidationError("conformal method is unsupported") from error
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

    train, calibration, evaluation, label_ends = _partition_origins(origins, chronology, len(values), spec)
    _, _, selection = _rolling_origin(
        x[train], y[train], origins[train], spec, chronology, label_ends[train])
    estimator = _estimator(spec)
    estimator.fit(x[train], _fit_targets(y[train], spec.strategy))
    calibrators = None
    resampling = None
    if conformal_method is not None:
        if not dependence_assumptions:
            raise ForecastValidationError("conformal dependence assumptions are required")
        estimator, calibrators, resampling = _fit_mapie_estimators(
            estimator, x[train], y[train], spec, ConformalMethod(conformal_method)
        )
    onnx = _export_onnx(estimator, x[train], spec)
    predictor = _SerializedPredictor(onnx)
    central = _future_path(predictor, values, external, future, spec)
    _finite_vector(central, "central forecast")

    calibration_predictions = _origin_predictions(predictor, values, external, origins[calibration], x[calibration], spec)
    evaluation_predictions = _origin_predictions(predictor, values, external, origins[evaluation], x[evaluation], spec)
    residuals = y[calibration] - calibration_predictions
    evaluation_residuals = y[evaluation] - evaluation_predictions
    quantile = (
        _residual_intervals(
            central,
            residuals,
            evaluation_residuals,
            IntervalKind.QUANTILE,
            "residual_quantile",
            "Train-only serialized model; calibration-only empirical offsets; untouched test coverage; overlapping targets remain dependent.",
            int(origins[calibration[0]]),
            int(origins[calibration[-1]] + 1),
            int(origins[evaluation[0]]),
            int(origins[evaluation[-1]] + 1),
        )
        if quantile_intervals
        else None
    )
    conformal = None
    if conformal_method is not None:
        if not dependence_assumptions:
            raise ForecastValidationError("conformal dependence assumptions are required")
        conformal = _mapie_intervals(
            calibrators,
            central,
            residuals,
            evaluation_residuals,
            x[calibration],
            y[calibration],
            origins[calibration],
            origins[evaluation],
            predictor,
            spec,
            conformal_method,
            "Train-only bootstrap mean EnbPI or single-center ACI; exact serialized predictor. ACI seeds scores on the first calibration half and adapts on the second, then freezes. Shared offsets envelope calibrated outputs and include zero. Recursive calibration is one-step; untouched pooled-horizon coverage is empirical, not simultaneous or profit probability. " + dependence_assumptions,
        )

    parameters = {
        "strategy": spec.strategy.value,
        "horizons": list(spec.horizons),
        "lags": list(spec.lags),
        "ridge_alpha": spec.ridge_alpha,
        "rolling_splits": spec.rolling_splits,
        "seed": spec.seed,
        "partition_ends": list(chronology.partition_ends),
        "horizon_origin": "next_index_after_last_observed",
        "conformal_method": conformal_method.value if conformal_method is not None else None,
        "conformal_center": "oob_weighted_bootstrap_mean" if conformal_method is ConformalMethod.ENBPI else "single_fitted_model",
        "bootstrap_aggregation": "oob_weighted_mean" if conformal_method is ConformalMethod.ENBPI else None,
        "resampling_block_length": resampling[0] if resampling is not None else None,
        "resampling_count": resampling[1] if resampling is not None else None,
        "resampling_overlapping": False if resampling is not None else None,
        "resampling_seed": spec.seed if resampling is not None else None,
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
        input_lags=spec.lags,
        exogenous_feature_count=external.shape[1],
        output_horizons=(1,) if spec.strategy is ForecastStrategy.RECURSIVE else spec.horizons,
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
    if strategy is ForecastStrategy.RECURSIVE and value.horizons[0] != 1:
        raise ForecastValidationError("recursive forecasting requires its fitted one-step horizon")
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


def _partition_origins(origins, chronology, observations, spec):
    if (not isinstance(chronology, ForecastChronology)
            or len(chronology.origins) != observations or len(chronology.label_ends) != observations
            or len(chronology.partition_ends) != 3
            or any(type(value) is not int or not -(2**63) <= value < 2**63
                   for values in (chronology.origins, chronology.label_ends, chronology.partition_ends)
                   for value in values)
            or any(left >= right for left, right in zip(chronology.origins, chronology.origins[1:]))
            or any(left >= right for left, right in zip(chronology.partition_ends, chronology.partition_ends[1:]))):
        raise ForecastValidationError("forecast chronology is invalid")
    partitions = [[], [], []]
    ends = []
    for row, origin in enumerate(origins):
        coordinate = chronology.origins[int(origin)]
        terminal = max(chronology.label_ends[int(origin + horizon - 1)] for horizon in spec.horizons)
        ends.append(terminal)
        if any(chronology.label_ends[int(origin - lag)] > coordinate for lag in spec.lags):
            continue
        if (spec.strategy is ForecastStrategy.RECURSIVE
                and any(max(chronology.label_ends[
                    int(origin-lag):min(int(origin), int(origin-lag)+max(spec.horizons))
                ]) > coordinate for lag in spec.lags)):
            continue
        for partition, end in enumerate(chronology.partition_ends):
            if coordinate <= end:
                if terminal <= end:
                    partitions[partition].append(row)
                break
    if any(not values for values in partitions) or len(partitions[1]) < 2:
        raise ForecastValidationError("purged train/calibration/evaluation populations are insufficient")
    return *(np.asarray(values, dtype=np.int64) for values in partitions), np.asarray(ends, dtype=np.int64)


class _SerializedPredictor:
    """Execute the exact exported graph for central and held-out inference."""

    def __init__(self, encoded: bytes):
        import onnx
        from onnx.reference import ReferenceEvaluator
        self._evaluator = ReferenceEvaluator(onnx.load_model_from_string(encoded))
        self._input = self._evaluator.input_names[0]

    def predict(self, features):
        encoded = np.asarray(features, dtype=np.float32)
        _finite_vector(encoded.reshape(-1), "serialized forecast inputs")
        try:
            output = self._evaluator.run(None, {self._input: encoded})
        except Exception as error:
            raise ForecastValidationError("serialized forecast evaluation failed") from error
        if len(output) != 1:
            raise ForecastValidationError("serialized forecast output count differs")
        result = np.asarray(output[0], dtype=np.float64)
        _finite_vector(result.reshape(-1), "serialized forecast output")
        return result


class _SerializedMapieCenter:
    """Exact graph representation of the admitted MAPIE center.

    EnbPI's fixed mean of linear bootstrap members is collapsed algebraically
    before export. ACI uses the single fitted model. Both return the same center
    to MAPIE's bounds/adaptation and to actual retained residual evaluation.
    """

    def __init__(self, predictor, output, ensemble):
        self.predictor = predictor
        self.output = output
        self.ensemble = ensemble

    def predict(self, features, ensemble=False, return_multi_pred=True, **_parameters):
        if ensemble != self.ensemble:
            raise ForecastValidationError("MAPIE prediction center differs from the exported policy")
        result = self.predictor.predict(features)
        point = np.asarray(result).reshape(len(features), -1)[:, self.output]
        if not return_multi_pred:
            return point
        return point, point[:, np.newaxis], point[:, np.newaxis]


def _origin_predictions(estimator, values, external, origins, features, spec):
    if spec.strategy is not ForecastStrategy.RECURSIVE:
        return np.asarray(estimator.predict(features), dtype=np.float64).reshape(len(origins), len(spec.horizons))
    predictions = []
    for origin in origins:
        # Only the origin's exogenous state is available to this recursive path.
        future = np.repeat(external[int(origin):int(origin)+1], max(spec.horizons), axis=0)
        predictions.append(_future_path(estimator, values[:int(origin)], external[:int(origin)], future, spec))
    return np.asarray(predictions, dtype=np.float64)


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
    final_origin = len(values) - max(spec.horizons) + 1
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
        [[values[origin + horizon - 1] for horizon in spec.horizons] for origin in origins],
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
    chronology: ForecastChronology,
    label_ends: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, RollingOriginEvidence]:
    predicted: list[np.ndarray] = []
    actual: list[np.ndarray] = []
    selections: list[Mapping[str, Any]] = []
    splitter = TimeSeriesSplit(n_splits=spec.rolling_splits)
    for fold, (train, validation) in enumerate(splitter.split(x)):
        train = train[label_ends[train] < chronology.origins[int(origins[validation[0]])]]
        if len(train) <= x.shape[1]:
            raise ForecastValidationError("purged rolling-origin prefix is insufficient")
        estimator = _estimator(spec)
        estimator.fit(x[train], _fit_targets(y[train], spec.strategy))
        if spec.strategy is ForecastStrategy.RECURSIVE:
            one = np.asarray(estimator.predict(x[validation]), dtype=np.float64).reshape(-1, 1)
            fold_prediction = one
        else:
            fold_prediction = np.asarray(estimator.predict(x[validation]), dtype=np.float64)
        _finite_vector(fold_prediction.reshape(-1), "rolling-origin predictions")
        predicted.append(fold_prediction)
        actual.append(y[validation, :1] if spec.strategy is ForecastStrategy.RECURSIVE else y[validation])
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
        prediction = float(np.asarray(estimator.predict(feature)).reshape(-1)[0])
        if not math.isfinite(prediction):
            raise ForecastValidationError("recursive forecast is nonfinite")
        history.append(prediction)
        if step in spec.horizons:
            results[step] = prediction
    return np.asarray([results[horizon] for horizon in spec.horizons], dtype=np.float64)


def _residual_intervals(
    central: np.ndarray,
    residuals: np.ndarray,
    evaluation_residuals: np.ndarray,
    kind: IntervalKind,
    method: str,
    assumptions: str,
    start: int,
    end: int,
    evaluation_start: int,
    evaluation_end: int,
) -> IntervalEvidence:
    absolute = np.abs(residuals.reshape(-1))
    widths = np.quantile(absolute, TARGET_COVERAGES, method="higher")
    bands = []
    for coverage, width in zip(TARGET_COVERAGES, widths, strict=True):
        lower = central - float(width)
        upper = central + float(width)
        covered = int(np.count_nonzero(np.abs(evaluation_residuals.reshape(-1)) <= width))
        bands.append(
            IntervalBand(
                coverage,
                tuple(float(value) for value in lower),
                tuple(float(value) for value in upper),
                -float(width),
                float(width),
                covered,
                int(evaluation_residuals.size),
            )
        )
    return _interval_evidence(kind, method, start, end, evaluation_start, evaluation_end, assumptions, bands, residuals, evaluation_residuals)


def _linear_parameters(estimator, spec, feature_count):
    """Collapse only fitted linear models; chaining is an affine composition."""
    if spec.strategy is ForecastStrategy.DIRECT:
        return np.vstack([member.coef_ for member in estimator.estimators_]), np.asarray([member.intercept_ for member in estimator.estimators_])
    if spec.strategy is ForecastStrategy.CHAINED:
        coefficients, intercepts = [], []
        if list(estimator.order_) != list(range(len(estimator.estimators_))):
            raise ForecastValidationError("linear chain order differs from the fitted policy")
        for index, member in enumerate(estimator.estimators_):
            coefficient = np.asarray(member.coef_[:feature_count], dtype=np.float64).copy()
            intercept = float(member.intercept_)
            if index:
                previous = np.asarray(member.coef_[feature_count:], dtype=np.float64)
                coefficient += previous @ np.asarray(coefficients)
                intercept += float(previous @ np.asarray(intercepts))
            coefficients.append(coefficient)
            intercepts.append(intercept)
        return np.asarray(coefficients), np.asarray(intercepts)
    return np.asarray(estimator.coef_).reshape(-1, feature_count), np.asarray(estimator.intercept_).reshape(-1)


def _linear_estimator(coefficients, intercepts, *, scalar=False):
    coefficients = np.asarray(coefficients, dtype=np.float64)
    intercepts = np.asarray(intercepts, dtype=np.float64)
    _finite_vector(coefficients.reshape(-1), "fitted linear coefficients")
    _finite_vector(intercepts.reshape(-1), "fitted linear intercepts")
    estimator = Ridge(solver="svd")
    estimator.n_features_in_ = coefficients.shape[1]
    estimator.coef_ = coefficients[0].copy() if scalar else coefficients.copy()
    estimator.intercept_ = intercepts[0] if scalar else intercepts.copy()
    return estimator


def _fit_mapie_estimators(estimator, x, y, spec, method):
    from mapie.regression import TimeSeriesRegressor
    from mapie.subsample import BlockBootstrap

    outputs = 1 if spec.strategy is ForecastStrategy.RECURSIVE else y.shape[1]
    block_length = max(1, int(math.sqrt(len(x))))
    resamplings = min(30, max(2, len(x) // block_length))
    if (resamplings * outputs * (x.shape[1] + 1)
            + outputs * len(x) * resamplings > MAX_FORECAST_CELLS):
        raise ForecastValidationError("MAPIE fitted members exceed the retained-cell bound")
    cv = BlockBootstrap(n_resamplings=resamplings, length=block_length,
                        overlapping=False, random_state=spec.seed)
    fitted_coefficients, fitted_intercepts = _linear_parameters(estimator, spec, x.shape[1])
    chain_members = None
    if spec.strategy is ForecastStrategy.CHAINED and method is ConformalMethod.ENBPI:
        chain_members = []
        for train, _ in cv.split(x, y[:, 0]):
            member = _estimator(spec)
            member.fit(x[train], y[train])
            chain_members.append(_linear_parameters(member, spec, x.shape[1]))
    calibrators, coefficients, intercepts = [], [], []
    for output in range(outputs):
        calibrator = TimeSeriesRegressor(
            estimator=Ridge(alpha=spec.ridge_alpha, solver="svd"),
            method=method.value, cv=cv, n_jobs=1, agg_function="mean", random_state=spec.seed,
        )
        calibrator.fit(x, y[:, output])
        calibrator.estimator_.single_estimator_ = _linear_estimator(
            fitted_coefficients[output:output+1], fitted_intercepts[output:output+1], scalar=True
        )
        if chain_members is not None:
            calibrator.estimator_.estimators_ = [
                _linear_estimator(coef[output:output+1], intercept[output:output+1], scalar=True)
                for coef, intercept in chain_members
            ]
        if method is ConformalMethod.ENBPI:
            # MAPIE first averages the OOB members selected by each training row,
            # then averages those row aggregates. The fixed mask induces these
            # exact member weights; a uniform member mean would be different.
            mask = np.nan_to_num(calibrator.estimator_.k_, nan=0.0)
            counts = np.sum(mask, axis=1)
            eligible = counts > 0
            if not np.any(eligible):
                raise ForecastValidationError("bootstrap produced no out-of-bag aggregate")
            weights = np.mean(mask[eligible] / counts[eligible, np.newaxis], axis=0)
            members = calibrator.estimator_.estimators_
            coefficient = weights @ np.asarray([member.coef_ for member in members])
            intercept = float(weights @ np.asarray([member.intercept_ for member in members]))
        else:
            coefficient = fitted_coefficients[output]
            intercept = float(fitted_intercepts[output])
        collapsed = x @ coefficient + intercept
        actual = np.asarray(calibrator.predict(x, ensemble=method is ConformalMethod.ENBPI))
        tolerance = np.finfo(np.float64).eps * max(64, x.shape[1] * resamplings * 8)
        if not np.allclose(collapsed, actual, rtol=tolerance, atol=tolerance):
            raise ForecastValidationError("MAPIE center is not the exported linear mean")
        coefficients.append(coefficient)
        intercepts.append(intercept)
        calibrators.append(calibrator)
    return _linear_estimator(coefficients, intercepts, scalar=spec.strategy is ForecastStrategy.RECURSIVE), calibrators, (block_length, resamplings)


def _mapie_intervals(
    calibrators, central, calibration_residuals, evaluation_residuals,
    calibration_x, calibration_y, calibration_origins, evaluation_origins,
    predictor, spec, method, assumptions,
) -> IntervalEvidence:
    lower_offsets, upper_offsets = np.zeros(3), np.zeros(3)
    for output, calibrator in enumerate(calibrators):
        ensemble = method is ConformalMethod.ENBPI
        # The fitted method is unchanged. This is the exact exported affine
        # representation of its verified ensemble/single center, so float32
        # runtime quantization is also present during adaptation and scoring.
        calibrator.estimator_ = _SerializedMapieCenter(predictor, output, ensemble)
        score_count = len(calibration_x) // 2 if method is ConformalMethod.ACI else len(calibration_x)
        calibrator.conformity_scores_ = np.asarray(calibration_residuals[:score_count, output], dtype=np.float64).copy()
        if method is ConformalMethod.ACI:
            calibrator.adapt_conformal_inference(
                calibration_x[score_count:], calibration_y[score_count:, output], gamma=0.01,
                confidence_level=list(TARGET_COVERAGES), ensemble=False,
            )
        feature = calibration_x[-1:].copy()
        point, bounds = calibrator.predict(
            feature, ensemble=ensemble, confidence_level=list(TARGET_COVERAGES),
            optimize_beta=False, allow_infinite_bounds=False,
        )
        bounds = np.asarray(bounds, dtype=np.float64)
        if bounds.shape != (1, 2, 3) or not np.isfinite(bounds).all():
            raise ForecastValidationError("MAPIE returned an unsupported interval shape")
        lower_offsets = np.minimum(lower_offsets, bounds[0, 0, :] - float(point[0]))
        upper_offsets = np.maximum(upper_offsets, bounds[0, 1, :] - float(point[0]))
    residuals = evaluation_residuals.reshape(-1)
    bands = []
    for index, coverage in enumerate(TARGET_COVERAGES):
        lower, upper = float(lower_offsets[index]), float(upper_offsets[index])
        covered = int(np.count_nonzero((residuals >= lower) & (residuals <= upper)))
        bands.append(IntervalBand(
            coverage, tuple(float(value) for value in central + lower),
            tuple(float(value) for value in central + upper), lower, upper, covered, int(residuals.size),
        ))
    return _interval_evidence(
        IntervalKind.CONFORMAL, f"mapie_{method.value}",
        int(calibration_origins[0]), int(calibration_origins[-1] + 1),
        int(evaluation_origins[0]), int(evaluation_origins[-1] + 1),
        assumptions, bands, calibration_residuals[:, :len(calibrators)], residuals,
    )


def _interval_evidence(kind, method, start, end, evaluation_start, evaluation_end, assumptions, bands, residuals, evaluation_residuals):
    if not assumptions or len(assumptions.encode("utf-8")) > 512 or any(ord(character) < 32 for character in assumptions):
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
    residuals_bytes = np.concatenate((np.asarray(residuals).reshape(-1), np.asarray(evaluation_residuals).reshape(-1))).astype("<f8").tobytes(order="C")
    policy = _canonical_json(
        {
            "schema_version": 1,
            "kind": kind.value,
            "method": method,
            "target_coverages": list(TARGET_COVERAGES),
            "calibration_start_index": start,
            "calibration_end_index": end,
            "evaluation_start_index": evaluation_start,
            "evaluation_end_index": evaluation_end,
            "dependence_assumptions": assumptions,
        }
    )
    return IntervalEvidence(
        kind,
        method,
        start,
        end,
        evaluation_start,
        evaluation_end,
        int(residuals.size),
        int(evaluation_residuals.size),
        assumptions,
        tuple(bands),
        residuals_bytes,
        hashlib.sha256(residuals_bytes).hexdigest(),
        policy,
        hashlib.sha256(policy).hexdigest(),
    )


def _export_onnx(estimator, x: np.ndarray, spec: ForecastSpecification) -> bytes:
    # Collapse fitted chains before conversion; recursive models remain the exact
    # one-step affine predictor used by _future_path at each recursive step.
    if isinstance(estimator, (MultiOutputRegressor, RegressorChain)):
        coefficients, intercepts = _linear_parameters(estimator, spec, x.shape[1])
        estimator = _linear_estimator(coefficients, intercepts)
    offsets = (1,) if spec.strategy is ForecastStrategy.RECURSIVE else spec.horizons
    try:
        import onnx
        model = to_onnx(
            estimator,
            name="market-squawk-research-affine",
            initial_types=[("X", FloatTensorType([1, x.shape[1]]))],
            final_types=[("Y", FloatTensorType([1, len(offsets)]))],
            target_opset=13,
            black_op={"LinearRegressor"},
        )
        onnx.helper.set_model_props(model, {
            "market_squawk.forecast.horizons": ",".join(map(str, offsets)),
            "market_squawk.forecast.lags": ",".join(map(str, spec.lags)),
            "market_squawk.forecast.strategy": spec.strategy.value,
        })
        onnx.checker.check_model(model)
        encoded = model.SerializeToString(deterministic=True)
    except Exception as error:  # converter/checker expose multiple exception classes
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
        "horizon_origin": "next_index_after_last_observed",
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
