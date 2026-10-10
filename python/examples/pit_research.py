"""Execute one admitted local point-in-time research read and chart with no downloads.

Set ``MARKET_SQUAWK_EXAMPLE_DATASET_ROOT`` to a local admitted dataset root and
``MARKET_SQUAWK_EXAMPLE_EXPORT_SHA256`` to its exact export digest.
"""

from __future__ import annotations

from decimal import Decimal
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
    product_contract="market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-forward-return.training/v1",
    max_rows=128,
    max_bytes=64 * 1024 * 1024,
    context=OperationContext(60_000, 1_000_000),
)
feature_rows = tuple(
    {"decision_at": row["decision_at"], "price_return": Decimal(row["value_decimal_mantissa"]).scaleb(-row["value_decimal_scale"])}
    for row in dataset.rows
    if row["component_kind"] == "feature" and row["component_name"] == "research.price-return"
)
specification = chart_spec(
    feature_rows,
    x="decision_at",
    y="price_return",
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
