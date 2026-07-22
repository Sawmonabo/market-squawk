from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from market_squawk.data import (
    DatasetIntegrityError,
    UtcNanoseconds,
    _preflight_parquet,
    open_dataset,
)


BUILD = "33" * 32
MANIFEST = "31" * 32
POLICY = "35" * 32
SCHEMA = "32" * 32
UNIVERSE = "34" * 32
INSTRUMENT = "0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(root: Path, *, corrupt_split: bool = False) -> str:
    object_path = root / "objects" / "aa" / "fixture.parquet"
    object_path.parent.mkdir(parents=True)
    metadata = {
        b"market_squawk.build_sha256": BUILD.encode(),
        b"market_squawk.component_layout": b"typed-long-form-v1",
        b"market_squawk.dataset": b"fixture-training",
        b"market_squawk.policy_sha256": POLICY.encode(),
        b"market_squawk.schema": b"market_squawk.feature_label_components",
        b"market_squawk.schema_fingerprint_sha256": SCHEMA.encode(),
        b"market_squawk.schema_version": b"1",
        b"market_squawk.timestamp_timezone": b"UTC",
        b"market_squawk.universe_sha256": UNIVERSE.encode(),
    }
    schema = pa.schema(
        [
            pa.field("example_id", pa.string(), nullable=False),
            pa.field("instrument_id", pa.string(), nullable=False),
            pa.field("cutoff_at", pa.timestamp("ns", tz="+00:00"), nullable=False),
            pa.field("split", pa.string(), nullable=False),
            pa.field("component_kind", pa.string(), nullable=False),
            pa.field("component_name", pa.string(), nullable=False),
            pa.field("component_version", pa.uint32(), nullable=False),
            pa.field("value_f64", pa.float64(), nullable=True),
            pa.field("value_decimal_mantissa", pa.decimal128(38, 0), nullable=True),
            pa.field("value_decimal_scale", pa.uint8(), nullable=True),
            pa.field("unit", pa.string(), nullable=True),
            pa.field("currency", pa.string(), nullable=True),
            pa.field("missing_reason", pa.string(), nullable=True),
            pa.field("lineage_sha256", pa.binary(32), nullable=False),
        ],
        metadata=metadata,
    )
    example_ids = [f"example-{index}" for index in range(1, 7)]
    cutoffs = [100, 120, 140, 160, 300, 320]
    splits = ["train"] * 4 + ["validation"] * 2
    feature_values = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
    label_mantissas = [-5, 25, 35, 65, 75, 105]
    lineages = [bytes([index]) * 32 for index in range(1, 13)]
    table = pa.Table.from_arrays(
        [
            pa.array([value for value in example_ids for _ in range(2)]),
            pa.array([INSTRUMENT] * 12),
            pa.array([value for value in cutoffs for _ in range(2)], type=schema.field("cutoff_at").type),
            pa.array(
                ["validation"] * 12
                if corrupt_split
                else [value for value in splits for _ in range(2)]
            ),
            pa.array(["feature", "label"] * 6),
            pa.array(["research.price-return", "forward-return"] * 6),
            pa.array([1] * 12, type=pa.uint32()),
            pa.array([value for feature in feature_values for value in (feature, None)], type=pa.float64()),
            pa.array(
                [value for label in label_mantissas for value in (None, Decimal(label))],
                type=pa.decimal128(38, 0),
            ),
            pa.array([value for _ in label_mantissas for value in (None, 1)], type=pa.uint8()),
            pa.array(["ratio", None] * 6),
            pa.array([None, None] * 6),
            pa.array([None] * 12),
            pa.array(lineages, type=pa.binary(32)),
        ],
        schema=schema,
    )
    pq.write_table(table, object_path, compression="zstd")
    object_lineage = hashlib.sha256()
    object_lineage.update(b"market-squawk/feature-label-object-lineage/v1")
    object_lineage.update(bytes.fromhex(BUILD))
    for lineage in lineages:
        object_lineage.update(lineage)
    export = {
        "components": [
            {
                "corporate_action_sensitivity": "requires_adjustment",
                "kind": "feature",
                "name": "research.price-return",
                "scope": "instrument",
                "version": 1,
            },
            {
                "corporate_action_sensitivity": "requires_adjustment",
                "kind": "label",
                "name": "forward-return",
                "scope": "instrument",
                "version": 1,
            },
        ],
        "dataset": {
            "build_spec_sha256": BUILD,
            "dataset_id": "fixture-training",
            "manifest_sha256": MANIFEST,
            "manifest_version": 1,
            "policy_sha256": POLICY,
            "schema_name": "market_squawk.feature_label_components",
            "schema_sha256": SCHEMA,
            "schema_version": 1,
            "universe_id": "fixture-universe",
            "universe_sha256": UNIVERSE,
        },
        "missing_value_policy": "reject",
        "objects": [
            {
                "artifact_id": "018f3c2a-91ab-7ccd-b3de-123456789abc",
                "lineage_sha256": object_lineage.hexdigest(),
                "path": "objects/aa/fixture.parquet",
                "row_count": 12,
                "sha256": _sha256(object_path),
                "size_bytes": object_path.stat().st_size,
            }
        ],
        "parents": [
            {
                "manifest": {
                    "dataset_id": "fixture-source",
                    "manifest_sha256": "41" * 32,
                    "manifest_version": 1,
                    "schema_name": "market_squawk.research_observations",
                    "schema_sha256": "42" * 32,
                    "schema_version": 3,
                },
                "relation": "derived_input",
            }
        ],
        "point_in_time": {"revision_mode": "latest_known", "version": 1},
        "schema_version": 1,
        "split_counts": {"test": 0, "train": 4, "validation": 2},
        "split_policy": {
            "test_end_unix_nanos": 600,
            "train_end_unix_nanos": 200,
            "validation_end_unix_nanos": 400,
        },
    }
    export_bytes = json.dumps(export, sort_keys=True, separators=(",", ":")).encode()
    (root / "feature-label-export.json").write_bytes(export_bytes)
    return hashlib.sha256(export_bytes).hexdigest()


class DatasetContracts(unittest.TestCase):
    def test_task11_export_bound_pit_read_preserves_exact_values_and_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root)
            result = open_dataset(root, digest, UtcNanoseconds(100), max_rows=16, max_bytes=1_000_000)

            self.assertEqual(result.export_sha256, digest)
            self.assertEqual(result.manifest_sha256, MANIFEST)
            self.assertEqual(result.as_of.unix_nanos, 100)
            self.assertEqual(len(result.rows), 2)
            self.assertEqual(result.rows[0]["value_f64"], 0.0)
            self.assertEqual(result.rows[1]["value_decimal_mantissa"], Decimal("-5"))
            self.assertEqual(result.rows[1]["value_decimal_scale"], 1)
            self.assertEqual(result.rows[0]["lineage_sha256"], bytes([1]) * 32)
            self.assertFalse(result.complete)

            aware = datetime(1970, 1, 1, tzinfo=timezone.utc)
            self.assertEqual(UtcNanoseconds.from_datetime(aware).unix_nanos, 0)
            with self.assertRaises(ValueError):
                UtcNanoseconds.from_datetime(datetime(1970, 1, 1))

    def test_hash_path_and_chronological_split_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root)
            object_path = root / "objects" / "aa" / "fixture.parquet"
            object_path.write_bytes(object_path.read_bytes() + b"tampered")
            with self.assertRaises(DatasetIntegrityError):
                open_dataset(root, digest, UtcNanoseconds(600))

    def test_parquet_expansion_is_rejected_before_arrow_materialization(self) -> None:
        table = pa.table(
            {"payload": [f"{index:06d}-" + "x" * 512 for index in range(4_096)]}
        )
        output = pa.BufferOutputStream()
        pq.write_table(table, output, compression="zstd", use_dictionary=False)
        content = output.getvalue().to_pybytes()
        bound = len(content) * 2
        self.assertLess(bound, table.nbytes)
        with self.assertRaises(DatasetIntegrityError):
            _preflight_parquet(content, max_bytes=bound, expected_rows=table.num_rows)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root)
            object_path = root / "objects" / "aa" / "fixture.parquet"
            original = root / "original.parquet"
            object_path.replace(original)
            object_path.symlink_to(original)
            with self.assertRaises(DatasetIntegrityError):
                open_dataset(root, digest, UtcNanoseconds(600))

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root, corrupt_split=True)
            with self.assertRaises(DatasetIntegrityError):
                open_dataset(root, digest, UtcNanoseconds(600))


if __name__ == "__main__":
    unittest.main()
