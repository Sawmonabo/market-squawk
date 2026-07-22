from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from market_squawk.bundle import BundleAuthorityRef, BundleExportError
from market_squawk.data import UtcNanoseconds, open_dataset
from market_squawk.finance import feature_contracts
from market_squawk.finance import OperationContext
from market_squawk.training import TrainingRun, TrainingValidationError
from test_data import _fixture


def _run(dataset) -> TrainingRun:
    feature = next(
        value
        for value in feature_contracts(context=OperationContext(60_000, 1_000_000))
        if value["name"] == "research.price-return"
    )
    label = next(value for value in dataset.components if value.kind == "label")
    return TrainingRun(
        dataset=dataset,
        features=[feature],
        label=label,
        seed=17,
        missing_policy="reject",
        training_code_revision="python-train-v1",
        environment_sha256="37" * 32,
        model_id="018f3c2a-91ab-7ccd-b3de-123456789abc",
        bundle_id="fixture-linear",
        bundle_version=1,
    )


class TrainingBundleContracts(unittest.TestCase):
    def test_task11_bound_training_exports_identical_externally_authorized_bundle(self) -> None:
        with (
            tempfile.TemporaryDirectory() as dataset_root,
            tempfile.TemporaryDirectory() as authority_root,
            tempfile.TemporaryDirectory() as left,
            tempfile.TemporaryDirectory() as right,
            tempfile.TemporaryDirectory() as rejected,
        ):
            digest = _fixture(Path(dataset_root))
            dataset = open_dataset(
                Path(dataset_root),
                digest,
                UtcNanoseconds(600),
                max_rows=32,
                context=OperationContext(60_000, 1_000_000),
            )
            first_proposal = _run(dataset).fit_evaluate(
                model_kind="linear", context=OperationContext(60_000, 1_000_000)
            )
            second_proposal = _run(dataset).fit_evaluate(
                model_kind="linear", context=OperationContext(60_000, 1_000_000)
            )
            self.assertEqual(first_proposal.training_run_sha256, second_proposal.training_run_sha256)
            self.assertEqual(first_proposal.authority_bytes, second_proposal.authority_bytes)

            authority_path = Path(authority_root) / "bundle-authority.json"
            authority_path.write_bytes(first_proposal.authority_bytes)
            authority = BundleAuthorityRef.exact(
                Path(authority_root),
                "bundle-authority.json",
                first_proposal.authority_sha256,
            )
            with self.assertRaises(TypeError):
                first_proposal.export(
                    Path(rejected),
                    authority,
                    context=OperationContext(60_000, 1_000_000),
                    validator="/tmp/fake-validator",
                )
            first = first_proposal.export(
                Path(left), authority, context=OperationContext(60_000, 1_000_000)
            )
            second = second_proposal.export(
                Path(right), authority, context=OperationContext(60_000, 1_000_000)
            )

            self.assertTrue(first.validated_by_rust)
            self.assertEqual(first.metadata_sha256, second.metadata_sha256)
            self.assertEqual(first.artifact_sha256, second.artifact_sha256)
            self.assertEqual(first.training_run_sha256, second.training_run_sha256)
            run_record = json.loads(first.run_record.read_text())
            self.assertEqual(run_record["trial"]["seed"], 17)
            self.assertEqual(run_record["trial"]["dataset_export_sha256"], digest)
            self.assertEqual(run_record["trial"]["split_counts"], {"test": 0, "train": 4, "validation": 2})
            self.assertNotEqual(run_record["trial"]["split_sha256"], "36" * 32)
            self.assertFalse((first.root / "expectations.json").exists())

    def test_partial_dataset_and_mutated_external_authority_fail_before_publication(self) -> None:
        with (
            tempfile.TemporaryDirectory() as dataset_root,
            tempfile.TemporaryDirectory() as authority_root,
            tempfile.TemporaryDirectory() as output_root,
        ):
            digest = _fixture(Path(dataset_root))
            partial = open_dataset(
                Path(dataset_root),
                digest,
                UtcNanoseconds(100),
                max_rows=8,
                context=OperationContext(60_000, 1_000_000),
            )
            with self.assertRaises(TrainingValidationError):
                _run(partial).fit_evaluate(
                    model_kind="linear", context=OperationContext(60_000, 1_000_000)
                )

            complete = open_dataset(
                Path(dataset_root),
                digest,
                UtcNanoseconds(600),
                max_rows=32,
                context=OperationContext(60_000, 1_000_000),
            )
            proposal = _run(complete).fit_evaluate(
                model_kind="linear", context=OperationContext(60_000, 1_000_000)
            )
            authority_path = Path(authority_root) / "bundle-authority.json"
            authority_path.write_bytes(proposal.authority_bytes + b" ")
            with self.assertRaises(BundleExportError):
                BundleAuthorityRef.exact(
                    Path(authority_root),
                    "bundle-authority.json",
                    proposal.authority_sha256,
                )
            self.assertEqual(list(Path(output_root).iterdir()), [])


if __name__ == "__main__":
    unittest.main()
