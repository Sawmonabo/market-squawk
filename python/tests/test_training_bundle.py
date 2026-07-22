from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from market_squawk.finance import feature_contracts
from market_squawk.training import TrainingRun, TrainingValidationError


HEX = "31" * 32


def _run() -> TrainingRun:
    features = feature_contracts()[:2]
    return TrainingRun(
        dataset={
            "dataset_id": "fixture-training",
            "manifest_version": 1,
            "schema_name": "market_squawk.feature_label_components",
            "schema_version": 1,
            "schema_sha256": "32" * 32,
            "manifest_sha256": HEX,
            "build_spec_sha256": "33" * 32,
            "universe_sha256": "34" * 32,
            "policy_sha256": "35" * 32,
        },
        features=features,
        label={
            "kind": "label",
            "scope": "instrument",
            "corporate_action_sensitivity": "requires_adjustment",
            "name": "forward-return",
            "version": 1,
        },
        universe_id="fixture-universe",
        split_sha256="36" * 32,
        seed=17,
        missing_policy="reject",
        training_code_revision="python-train-v1",
        environment_sha256="37" * 32,
        model_id="018f3c2a-91ab-7ccd-b3de-123456789abc",
        bundle_id="fixture-linear",
        bundle_version=1,
        training_start_unix_nanos=1,
        training_end_unix_nanos=7,
    )


class TrainingBundleContracts(unittest.TestCase):
    def test_seeded_training_exports_identical_rust_validated_bundle(self) -> None:
        rows = [[0.0, 1.0], [1.0, 0.0], [2.0, 1.0], [3.0, 0.0], [4.0, 1.0], [5.0, 0.0]]
        labels = [-0.5, 2.5, 3.5, 6.5, 7.5, 10.5]
        splits = ["train", "train", "train", "train", "validation", "validation"]
        with tempfile.TemporaryDirectory() as left, tempfile.TemporaryDirectory() as right:
            first = _run().fit_evaluate_export(rows, labels, splits, Path(left), model_kind="linear")
            second = _run().fit_evaluate_export(rows, labels, splits, Path(right), model_kind="linear")

            self.assertTrue(first.validated_by_rust)
            self.assertEqual(first.metadata_sha256, second.metadata_sha256)
            self.assertEqual(first.artifact_sha256, second.artifact_sha256)
            run_record = json.loads(first.run_record.read_text())
            self.assertEqual(run_record["seed"], 17)
            self.assertEqual(run_record["split_sha256"], "36" * 32)
            self.assertEqual(run_record["environment_sha256"], "37" * 32)

    def test_nonfinite_training_input_fails_before_publication(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            with self.assertRaises(TrainingValidationError):
                _run().fit_evaluate_export(
                    [[0.0, 1.0], [float("inf"), 0.0]],
                    [0.0, 1.0],
                    ["train", "validation"],
                    output,
                    model_kind="linear",
                )
            self.assertEqual(list(output.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
