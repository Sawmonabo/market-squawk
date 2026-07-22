"""Execute one complete local point-in-time research read and chart with no downloads."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from market_squawk.data import UtcNanoseconds, open_dataset
from market_squawk.visualization import chart_spec


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "pit_example"
MANIFEST_SHA256 = (FIXTURE / "manifest.sha256").read_text().strip()
dataset = open_dataset(FIXTURE, MANIFEST_SHA256, UtcNanoseconds(2_500))
specification = chart_spec(dataset.rows, x="observed_at", y="value", title="Local PIT fixture")
encoded = json.dumps(specification, sort_keys=True, separators=(",", ":")).encode()
RESULT = {
    "dataset": dataset.dataset_id,
    "rows": len(dataset.rows),
    "chart_sha256": hashlib.sha256(encoded).hexdigest(),
}

if __name__ == "__main__":
    print(json.dumps(RESULT, sort_keys=True))
