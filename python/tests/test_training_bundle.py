from __future__ import annotations

import base64
import csv
from dataclasses import replace
import hashlib
import io
import json
from pathlib import Path
import stat
import sys
import tempfile
import unittest

import market_squawk.training as installed_training
import pyarrow as pa
from market_squawk import training_environment_receipt
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
        environment=training_environment_receipt(),
        model_id="018f3c2a-91ab-7ccd-b3de-123456789abc",
        bundle_id="fixture-linear",
        bundle_version=1,
    )


class TrainingBundleContracts(unittest.TestCase):
    def test_signed_environment_rejects_regenerated_record_and_receipt(self) -> None:
        baseline = training_environment_receipt().sha256
        self.assertEqual(len(baseline), 64)
        authority = Path(sys.prefix) / "share/market-squawk"
        receipt = authority / "training-environment.json"
        envelope = json.loads(receipt.read_text(encoding="ascii"))
        self.assertEqual(
            [value["name"] for value in envelope["payload"]["runtime_distributions"]],
            ["pyarrow"],
        )
        dependency_source = Path(pa.__file__)
        dependency_content = dependency_source.read_bytes()
        dependency_mode = stat.S_IMODE(dependency_source.stat().st_mode)
        try:
            dependency_source.chmod(dependency_mode | stat.S_IWUSR)
            dependency_source.write_bytes(dependency_content + b"\n# untrusted mutation\n")
            with self.assertRaises(ValueError):
                training_environment_receipt()
        finally:
            dependency_source.write_bytes(dependency_content)
            dependency_source.chmod(dependency_mode)
        self.assertEqual(training_environment_receipt().sha256, baseline)

        added_dependency = dependency_source.parent / "_market_squawk_unrecorded.py"
        self.assertFalse(added_dependency.exists())
        try:
            added_dependency.write_bytes(b"raise RuntimeError('untrusted')\n")
            with self.assertRaises(ValueError):
                training_environment_receipt()
        finally:
            added_dependency.unlink(missing_ok=True)
        self.assertEqual(training_environment_receipt().sha256, baseline)

        record = Path(sys.prefix) / envelope["payload"]["project_distribution"][
            "record_relative_path"
        ]
        training_source = Path(installed_training.__file__)
        original = {
            path: (path.read_bytes(), stat.S_IMODE(path.stat().st_mode))
            for path in (receipt, record, training_source)
        }
        authority_mode = stat.S_IMODE(authority.stat().st_mode)
        try:
            authority.chmod(0o755)
            for path in (receipt, record, training_source):
                path.chmod(original[path][1] | stat.S_IWUSR)
            mutated = original[training_source][0] + b"\n# untrusted installed mutation\n"
            training_source.write_bytes(mutated)
            rows = list(csv.reader(io.StringIO(original[record][0].decode("utf-8"))))
            site_packages = record.parent.parent
            source_name = training_source.relative_to(site_packages).as_posix()
            for row in rows:
                if row[0] == source_name:
                    digest = hashlib.sha256(mutated).digest()
                    row[1] = "sha256=" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
                    row[2] = str(len(mutated))
                    break
            else:
                self.fail("installed training source is absent from RECORD")
            rendered = io.StringIO(newline="")
            csv.writer(rendered, lineterminator="\n").writerows(rows)
            forged_record = rendered.getvalue().encode("utf-8")
            record.write_bytes(forged_record)
            entries = {}
            record_name = record.relative_to(site_packages).as_posix()
            for name, encoded_digest, encoded_size in rows:
                if name == record_name:
                    continue
                if not encoded_digest and not encoded_size:
                    installed = (site_packages / name).read_bytes()
                    entries[name] = (hashlib.sha256(installed).digest(), len(installed))
                else:
                    padded = encoded_digest.removeprefix("sha256=")
                    padded += "=" * (-len(padded) % 4)
                    entries[name] = (base64.urlsafe_b64decode(padded), int(encoded_size))
            file_set = hashlib.sha256(b"market-squawk-record-set-v1\0")
            for name, (digest, size) in sorted(entries.items()):
                encoded_name = name.encode("utf-8")
                file_set.update(len(encoded_name).to_bytes(4, "big"))
                file_set.update(encoded_name)
                file_set.update(size.to_bytes(8, "big"))
                file_set.update(digest)
            distribution = envelope["payload"]["project_distribution"]
            distribution["file_set_sha256"] = file_set.hexdigest()
            distribution["record_sha256"] = hashlib.sha256(forged_record).hexdigest()
            distribution["record_size_bytes"] = len(forged_record)
            receipt.write_text(
                json.dumps(envelope, sort_keys=True, separators=(",", ":")),
                encoding="ascii",
            )

            with self.assertRaises(ValueError):
                training_environment_receipt()
        finally:
            for path, (content, mode) in original.items():
                path.chmod(mode | stat.S_IWUSR)
                path.write_bytes(content)
                path.chmod(mode)
            authority.chmod(authority_mode)

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
            self.assertEqual(
                run_record["trial"]["environment_sha256"],
                training_environment_receipt().sha256,
            )
            self.assertEqual(
                run_record["trial"]["training_code_revision"],
                training_environment_receipt().training_code_revision,
            )
            self.assertEqual(run_record["trial"]["seed"], 17)
            self.assertEqual(run_record["trial"]["dataset_export_sha256"], digest)
            self.assertEqual(run_record["trial"]["split_counts"], {"test": 0, "train": 4, "validation": 2})
            self.assertNotEqual(run_record["trial"]["split_sha256"], "36" * 32)
            self.assertFalse((first.root / "expectations.json").exists())

            with self.assertRaises(TypeError):
                replace(_run(dataset), environment={"sha256": "37" * 32}).fit_evaluate(
                    model_kind="linear", context=OperationContext(60_000, 1_000_000)
                )

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
