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

from market_squawk.data import DatasetIntegrityError, UtcNanoseconds, open_dataset


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(root: Path) -> str:
    objects = root / "objects"
    objects.mkdir()
    schema = pa.schema(
        [
            pa.field("value", pa.decimal128(12, 4), nullable=False),
            pa.field("observed_at", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("available_at", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("source_identifier", pa.string(), nullable=False),
            pa.field("quality", pa.string(), nullable=False),
            pa.field("payload_sha256", pa.binary(32), nullable=False),
            pa.field("note", pa.string(), nullable=True),
        ],
        metadata={b"market_squawk.schema": b"test.pit", b"market_squawk.schema_version": b"1"},
    )
    table = pa.Table.from_arrays(
        [
            pa.array([Decimal("12.3400"), Decimal("99.0000")], type=schema.field("value").type),
            pa.array([1_001, 3_001], type=schema.field("observed_at").type),
            pa.array([1_500, 3_500], type=schema.field("available_at").type),
            pa.array(["fixture", "fixture"]),
            pa.array(["row-1", "row-2"]),
            pa.array(["OfficialDelayed", "OfficialDelayed"]),
            pa.array([bytes([1]) * 32, bytes([2]) * 32], type=pa.binary(32)),
            pa.array([None, "future"]),
        ],
        schema=schema,
    )
    object_path = objects / "part-000.parquet"
    pq.write_table(table, object_path, compression="zstd")
    manifest = {
        "schema_version": 1,
        "dataset_id": "fixture-pit",
        "manifest_version": 1,
        "schema": {
            "name": "test.pit",
            "version": 1,
            "arrow_sha256": hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
        },
        "as_of_column": "available_at",
        "required_provenance": [
            "source_id",
            "source_identifier",
            "quality",
            "payload_sha256",
        ],
        "objects": [
            {
                "path": "objects/part-000.parquet",
                "sha256": _sha256(object_path),
                "size_bytes": object_path.stat().st_size,
                "row_count": 2,
            }
        ],
    }
    manifest_bytes = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    (root / "manifest.json").write_bytes(manifest_bytes)
    return hashlib.sha256(manifest_bytes).hexdigest()


class DatasetContracts(unittest.TestCase):
    def test_manifest_bound_pit_read_preserves_exact_values_and_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root)
            result = open_dataset(
                root,
                digest,
                UtcNanoseconds(2_000),
                max_rows=8,
                max_bytes=1_000_000,
            )

            self.assertEqual(result.manifest_sha256, digest)
            self.assertEqual(result.as_of.unix_nanos, 2_000)
            self.assertEqual(len(result.rows), 1)
            self.assertEqual(result.rows[0]["value"], Decimal("12.3400"))
            self.assertEqual(result.rows[0]["observed_at"].unix_nanos, 1_001)
            self.assertIsNone(result.rows[0]["note"])
            self.assertEqual(result.rows[0]["source_identifier"], "row-1")

            aware = datetime(1970, 1, 1, tzinfo=timezone.utc)
            self.assertEqual(UtcNanoseconds.from_datetime(aware).unix_nanos, 0)
            with self.assertRaises(ValueError):
                UtcNanoseconds.from_datetime(datetime(1970, 1, 1))

    def test_hash_schema_and_controlled_path_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root)
            object_path = root / "objects" / "part-000.parquet"
            object_path.write_bytes(object_path.read_bytes() + b"tampered")
            with self.assertRaises(DatasetIntegrityError):
                open_dataset(root, digest, UtcNanoseconds(4_000))

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root)
            object_path = root / "objects" / "part-000.parquet"
            original = root / "original.parquet"
            object_path.replace(original)
            object_path.symlink_to(original)
            with self.assertRaises(DatasetIntegrityError):
                open_dataset(root, digest, UtcNanoseconds(4_000))


if __name__ == "__main__":
    unittest.main()
