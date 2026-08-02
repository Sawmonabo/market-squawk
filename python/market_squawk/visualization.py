"""Bounded self-contained chart specifications and static SVG rendering."""

from __future__ import annotations

from decimal import Decimal
from html import escape
import math
from typing import Any, Mapping, Sequence

from .data import UtcNanoseconds


MAX_CHART_POINTS = 2_000
MAX_TEXT_BYTES = 128


class VisualizationError(ValueError):
    """A chart value, label, or retained-point bound was invalid."""


def chart_spec(
    rows: Sequence[Mapping[str, Any]],
    *,
    x: str,
    y: str,
    title: str = "Market Squawk research",
    max_points: int = 1_000,
) -> dict[str, Any]:
    """Return a deterministic self-contained line-chart specification."""

    _bounds(rows, max_points)
    _text(x)
    _text(y)
    _text(title)
    values = [{"x": _x(row.get(x)), "y": _y(row.get(y))} for row in rows]
    return {
        "$schema": "market-squawk.chart/v1",
        "title": title,
        "mark": "line",
        "encoding": {"x": {"field": "x"}, "y": {"field": "y", "type": "quantitative"}},
        "data": {"values": values},
    }


def static_svg(
    rows: Sequence[Mapping[str, Any]],
    *,
    x: str,
    y: str,
    title: str = "Market Squawk research",
    max_points: int = 1_000,
    width: int = 640,
    height: int = 360,
) -> str:
    """Render a deterministic standalone SVG without scripts or external resources."""

    spec = chart_spec(rows, x=x, y=y, title=title, max_points=max_points)
    if not 240 <= width <= 2_000 or not 160 <= height <= 2_000:
        raise VisualizationError("chart dimensions are outside the supported bound")
    values = spec["data"]["values"]
    x_values = [float(value["x"]) for value in values]
    y_values = [float(value["y"]) for value in values]
    left, right, top, bottom = 48.0, float(width - 20), 40.0, float(height - 36)
    x_min, x_max = min(x_values), max(x_values)
    y_min, y_max = min(y_values), max(y_values)

    def coordinate(value: float, minimum: float, maximum: float, start: float, end: float) -> float:
        if maximum == minimum:
            return (start + end) / 2.0
        return start + (value - minimum) * (end - start) / (maximum - minimum)

    points = " ".join(
        f"{coordinate(x_value, x_min, x_max, left, right):.3f},"
        f"{coordinate(y_value, y_min, y_max, bottom, top):.3f}"
        for x_value, y_value in zip(x_values, y_values, strict=True)
    )
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img">'
        f'<title>{escape(title)}</title>'
        f'<rect width="{width}" height="{height}" fill="white"/>'
        f'<path d="M {left:.3f} {top:.3f} V {bottom:.3f} H {right:.3f}" '
        'fill="none" stroke="#4b5563"/>'
        f'<polyline points="{points}" fill="none" stroke="#0369a1" stroke-width="2"/>'
        "</svg>"
    )


def _bounds(rows: Sequence[Mapping[str, Any]], maximum: int) -> None:
    if not isinstance(maximum, int) or not 1 <= maximum <= MAX_CHART_POINTS:
        raise VisualizationError("chart point limit is invalid")
    if not rows or len(rows) > maximum:
        raise VisualizationError("chart point count exceeds its requested bound")


def _text(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode()) > MAX_TEXT_BYTES
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
        or "http://" in value.lower()
        or "https://" in value.lower()
        or "file://" in value.lower()
        or "\\" in value
        or value.startswith(("/", "~/"))
    ):
        raise VisualizationError("chart text is invalid")
    return value


def _x(value: Any) -> int | float:
    if isinstance(value, UtcNanoseconds):
        return value.unix_nanos
    return _finite_number(value)


def _y(value: Any) -> float:
    return float(_finite_number(value))


def _finite_number(value: Any) -> int | float:
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise VisualizationError("chart values must be finite numeric scalars")
    return value
