from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from market_squawk.data import (
    DatasetIntegrityError,
    UtcNanoseconds,
    _preflight_parquet,
    _verify_dataset_receipt,
    open_dataset,
)
from market_squawk._data_validation import _RowValidator
from market_squawk.finance import OperationContext


PRODUCT_CONTRACT = "market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-forward-return.training/v1"
FIXTURE_TEST = "point_in_time_builder_publishes_one_authorized_queryable_phase_one_generation"
FIXTURE_MAX_BYTES = 64 * 1024 * 1024


def _fixture(root: Path) -> str:
    """Ask the existing Rust producer test to publish one genuinely admitted dataset."""
    root = root.resolve(strict=True)
    if not root.is_dir() or any(root.iterdir()):
        raise ValueError("dataset fixture requires an empty directory")
    value = os.environ.get("MARKET_SQUAWK_DATASET_FIXTURE_EXECUTABLE")
    if not value:
        raise RuntimeError("the existing Rust dataset fixture executable is required")
    executable = Path(value)
    if not executable.is_absolute() or executable.is_symlink() or not executable.is_file():
        raise RuntimeError("dataset fixture executable is not an absolute regular file")
    expected = os.environ.get("MARKET_SQUAWK_DATASET_FIXTURE_SHA256")
    if expected is not None:
        with executable.open("rb") as content:
            if hashlib.file_digest(content, "sha256").hexdigest() != expected:
                raise RuntimeError("dataset fixture executable changed after its build")
    environment = dict(os.environ)
    environment["MARKET_SQUAWK_TEST_DATASET_ROOT"] = str(root)
    with tempfile.TemporaryFile() as output:
        completed = subprocess.run(
            [str(executable), FIXTURE_TEST, "--exact", "--nocapture"],
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            env=environment,
            timeout=120,
            check=False,
        )
        if completed.returncode != 0:
            output.seek(max(0, output.tell() - 4096))
            raise RuntimeError(
                "the genuine dataset producer failed: "
                + output.read(4096).decode("utf-8", "replace")
            )
    export_path = root / "fixture-export.json"
    receipt_path = root / "fixture-receipt.json"
    if not 0 < export_path.stat().st_size <= 1024 * 1024 or not receipt_path.is_file():
        raise RuntimeError("dataset producer did not return its bounded export and receipt")
    return hashlib.sha256(export_path.read_bytes()).hexdigest()


def _fixture_object_path(root: Path) -> Path:
    export = json.loads((root / "fixture-export.json").read_bytes())
    if len(export["objects"]) != 1:
        raise RuntimeError("fixture does not contain exactly one derived Parquet object")
    artifact_root = (root / "artifacts").resolve(strict=True)
    path = artifact_root / export["objects"][0]["path"]
    path.resolve(strict=True).relative_to(artifact_root)
    return path


class DatasetContracts(unittest.TestCase):
    def test_task11_export_bound_pit_read_preserves_exact_values_and_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root)
            early = open_dataset(
                root, digest, UtcNanoseconds(100),
                max_rows=128, max_bytes=FIXTURE_MAX_BYTES,
                product_contract=PRODUCT_CONTRACT,
                context=OperationContext(60_000, 1_000_000),
            )
            self.assertEqual(len(early.rows), len(early.components) - 1)
            self.assertTrue(all(row["component_kind"] == "feature" for row in early.rows))
            _verify_dataset_receipt(early, OperationContext(60_000, 1_000_000))
            boundary = open_dataset(
                root, digest, UtcNanoseconds(110),
                max_rows=128, max_bytes=FIXTURE_MAX_BYTES,
                product_contract=PRODUCT_CONTRACT,
                context=OperationContext(60_000, 1_000_000),
            )
            self.assertEqual(len(boundary.rows), len(boundary.components))
            boundary_label = next(row for row in boundary.rows if row["component_kind"] == "label")
            self.assertEqual(boundary_label["label_selection_as_of"], UtcNanoseconds(110))
            _verify_dataset_receipt(boundary, OperationContext(60_000, 1_000_000))
            result = open_dataset(
                root, digest, UtcNanoseconds(150),
                max_rows=128, max_bytes=FIXTURE_MAX_BYTES,
                product_contract=PRODUCT_CONTRACT,
                context=OperationContext(60_000, 1_000_000),
            )
            export = json.loads((root / "fixture-export.json").read_bytes())
            self.assertEqual(result.export_sha256, digest)
            self.assertEqual(result.manifest_sha256, export["dataset"]["manifest_sha256"])
            self.assertEqual(result.as_of.unix_nanos, 150)
            self.assertEqual(len(result.rows), len(result.components))
            feature = next(row for row in result.rows if row["component_name"] == "research.price-return")
            label = next(row for row in result.rows if row["component_kind"] == "label")
            self.assertEqual(Decimal(feature["value_decimal_mantissa"]).scaleb(-feature["value_decimal_scale"]), Decimal("0"))
            self.assertEqual(Decimal(label["value_decimal_mantissa"]).scaleb(-label["value_decimal_scale"]), Decimal("0.005"))
            table = pq.read_table(_fixture_object_path(root))
            self.assertEqual(result.rows[0]["lineage_sha256"], table["lineage_sha256"][0].as_py())
            self.assertNotEqual(result.rows[0]["lineage_sha256"], bytes(32))
            self.assertEqual(feature["observed_effective_at"], UtcNanoseconds(95))
            self.assertEqual(label["label_effective_at"], UtcNanoseconds(105))
            self.assertEqual(label["label_selection_as_of"], UtcNanoseconds(110))
            target = next(component.target for component in result.components if component.kind == "label")
            self.assertEqual(
                (target.kind, target.horizon_nanos, target.origin_basis),
                ("fixed_horizon_terminal", 10, "completed_bar_close"),
            )
            self.assertEqual(label, boundary_label)
            _verify_dataset_receipt(result, OperationContext(60_000, 1_000_000))
            self.assertFalse(result.complete)
            aware = datetime(1970, 1, 1, tzinfo=timezone.utc)
            self.assertEqual(UtcNanoseconds.from_datetime(aware).unix_nanos, 0)
            with self.assertRaises(ValueError):
                UtcNanoseconds.from_datetime(datetime(1970, 1, 1))

    def test_hash_path_and_chronological_split_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root)
            object_path = _fixture_object_path(root)
            object_path.write_bytes(object_path.read_bytes() + b"tampered")
            with self.assertRaises(DatasetIntegrityError):
                open_dataset(
                    root, digest, UtcNanoseconds(700),
                    product_contract=PRODUCT_CONTRACT,
                    context=OperationContext(60_000, 10_000_000),
                )

    def test_parquet_expansion_is_rejected_before_arrow_materialization(self) -> None:
        table = pa.table({"payload": [f"{index:06d}-" + "x" * 512 for index in range(4_096)]})
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
            object_path = _fixture_object_path(root)
            original = root / "original.parquet"
            object_path.replace(original)
            object_path.symlink_to(original)
            with self.assertRaises(DatasetIntegrityError):
                open_dataset(
                    root, digest, UtcNanoseconds(700),
                    product_contract=PRODUCT_CONTRACT,
                    context=OperationContext(60_000, 10_000_000),
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root)
            dataset = open_dataset(
                root, digest, UtcNanoseconds(700),
                max_rows=128, max_bytes=FIXTURE_MAX_BYTES,
                product_contract=PRODUCT_CONTRACT,
                context=OperationContext(60_000, 10_000_000),
            )
            export = json.loads((root / "fixture-export.json").read_bytes())
            validator = _RowValidator(
                dataset.components, dataset.split_policy, dataset.split_counts,
                dataset.identity.study, dataset.population,
                export["dataset"]["price_input_origin"],
            )
            wrong_split = dict(dataset.rows[0], split="validation")
            with self.assertRaisesRegex(DatasetIntegrityError, "chronological split policy"):
                validator.consume(wrong_split)


if __name__ == "__main__":
    unittest.main()
