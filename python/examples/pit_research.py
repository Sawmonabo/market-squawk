"""Execute one admitted local point-in-time research read and chart with no downloads.

Set ``MARKET_SQUAWK_EXAMPLE_DATASET_ROOT`` to a local admitted dataset root and
``MARKET_SQUAWK_EXAMPLE_EXPORT_SHA256`` to its exact export digest.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from market_squawk.data import UtcNanoseconds, open_dataset
from market_squawk.finance import OperationContext
from market_squawk.visualization import chart_spec


root_value = os.environ.get("MARKET_SQUAWK_EXAMPLE_DATASET_ROOT", "")
EXPORT_SHA256 = os.environ.get("MARKET_SQUAWK_EXAMPLE_EXPORT_SHA256", "")
if not 1 <= len(os.fsencode(root_value)) <= 4_096:
    raise RuntimeError("MARKET_SQUAWK_EXAMPLE_DATASET_ROOT is required and must be bounded")
if len(EXPORT_SHA256) != 64 or any(value not in "0123456789abcdef" for value in EXPORT_SHA256):
    raise RuntimeError("MARKET_SQUAWK_EXAMPLE_EXPORT_SHA256 must be a lowercase SHA-256 digest")
FIXTURE = Path(root_value)
dataset = open_dataset(
    FIXTURE,
    EXPORT_SHA256,
    UtcNanoseconds(120),
    max_rows=16,
    max_bytes=1_000_000,
    context=OperationContext(60_000, 1_000_000),
)
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
