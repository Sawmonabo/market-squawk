"""Execute one complete local point-in-time research read and chart with no downloads."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from market_squawk.data import UtcNanoseconds, open_dataset
from market_squawk.visualization import chart_spec


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "pit_example"
EXPORT_SHA256 = (FIXTURE / "feature-label-export.sha256").read_text().strip()
dataset = open_dataset(FIXTURE, EXPORT_SHA256, UtcNanoseconds(120))
feature_rows = tuple(row for row in dataset.rows if row["component_kind"] == "feature")
specification = chart_spec(
    feature_rows,
    x="cutoff_at",
    y="value_f64",
    title="Local PIT fixture",
)
encoded = json.dumps(specification, sort_keys=True, separators=(",", ":")).encode()
RESULT = {
    "dataset": dataset.dataset_id,
    "rows": len(feature_rows),
    "chart_sha256": hashlib.sha256(encoded).hexdigest(),
}

if __name__ == "__main__":
    print(json.dumps(RESULT, sort_keys=True))
