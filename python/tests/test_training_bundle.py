from __future__ import annotations

import base64
import csv
from dataclasses import replace
import hashlib
import io
import json
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest

import market_squawk
import market_squawk.training as installed_training
import pyarrow as pa
from market_squawk import training_environment_receipt
from market_squawk.bundle import (
    BundleAuthorityRef,
    BundleExportError,
    _native_release_executable,
    _native_subprocess_environment,
)
from market_squawk.data import UtcNanoseconds, open_dataset
from market_squawk.finance import feature_contracts
from market_squawk.finance import OperationContext
from market_squawk.training import TrainingRun, TrainingValidationError
from market_squawk.training_driver import (
    _strict_regular_file_coordinate,
    finalize_candidate,
    write_proposal,
)
from market_squawk.worker_protocol import (
    MAX_EVENT_BYTES,
    CandidateEvidence,
    WorkerProtocolWriter,
)
from test_data import _fixture


requires_sealed_release = unittest.skipUnless(
    market_squawk.__market_squawk_build_identity__ == "sealed-release-v1",
    "requires the sealed installed Python product",
)


def _run(
    dataset,
    *,
    model_id: str = "018f3c2a-91ab-7ccd-b3de-123456789abc",
    bundle_id: str = "fixture-linear",
) -> TrainingRun:
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
        model_id=model_id,
        bundle_id=bundle_id,
        bundle_version=1,
    )


def _write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")),
        encoding="ascii",
    )


def _driver_config(
    data_root: Path,
    digest: str,
    dataset,
    *,
    model_kind: str,
    model_id: str,
    bundle_id: str,
) -> dict[str, object]:
    feature = next(
        value
        for value in feature_contracts(context=OperationContext(60_000, 1_000_000))
        if value["name"] == "research.price-return"
    )
    label = next(value for value in dataset.components if value.kind == "label")
    return {
        "schemaVersion": 1,
        "dataset": {
            "root": str(data_root.resolve()),
            "exportSha256": digest,
            "asOfUnixNanos": 600,
            "maximumRows": 32,
            "maximumBytes": 256 * 1024 * 1024,
        },
        "training": {
            "features": [feature],
            "label": dict(label.mapping()),
            "seed": 17,
            "missingPolicy": "reject",
            "modelId": model_id,
            "bundleId": bundle_id,
            "bundleVersion": 1,
            "modelKind": model_kind,
            "artifactFormat": "onnx",
        },
        "operation": {
            "timeoutMilliseconds": 60_000,
            "maximumOperations": 1_000_000,
        },
        "onnx": {
            "opset": 13,
            "inferenceDeadlineMilliseconds": 250,
            "fallback": "no_action",
        },
    }


def _signed_prediction_attempt(
    data_root: Path,
    request_root: Path,
    *,
    model_id: str,
    bundle_id: str,
) -> subprocess.CompletedProcess[bytes]:
    request = request_root / "prediction.json"
    _write_json(
        request,
        {
            "modelId": model_id,
            "input": {
                "bundleId": bundle_id,
                "bundleVersion": 1,
                "featureValues": [0.25],
            },
        },
    )
    request = _strict_regular_file_coordinate(request, "signed prediction request")
    return subprocess.run(
        [
            str(_native_release_executable("market-squawk")),
            "--data-dir",
            str(data_root),
            "--training-release-root",
            str(Path(sys.prefix).resolve(strict=True)),
            "--output",
            "json",
            "model",
            "predict",
            str(request),
        ],
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=70,
        env=_native_subprocess_environment(),
    )


def _initialize_signed_data_root(data_root: Path) -> None:
    release_root = Path(sys.prefix).resolve(strict=True)
    application = _native_release_executable("market-squawk").resolve(strict=True)
    completed = subprocess.run(
        [
            str(application),
            "--data-dir",
            str(data_root),
            "--training-release-root",
            str(release_root),
            "init",
        ],
        check=False,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=70,
        env=_native_subprocess_environment(),
    )
    if completed.returncode != 0:
        raise AssertionError(
            "sealed native initialization failed\n"
            f"stdout: {completed.stdout[-4096:].decode('utf-8', 'replace')}\n"
            f"stderr: {completed.stderr[-4096:].decode('utf-8', 'replace')}"
        )


class TrainingBundleContracts(unittest.TestCase):
    def test_worker_protocol_is_ordered_bounded_and_terminal_once(self) -> None:
        stream = io.BytesIO()
        worker = WorkerProtocolWriter(
            stream,
            run_id="018f3c2a-91ab-7ccd-b3de-123456789abc",
            generation=7,
        )
        worker.progress("validation", "Training request validated.", 1, 2)
        worker.result(
            "complete",
            "Model candidate produced for Rust validation.",
            CandidateEvidence(
                admission_request_sha256="99" * 32,
                candidate_directory="models/fixture-v1/candidate",
                metadata_sha256="11" * 32,
                artifact_sha256="22" * 32,
                training_run_sha256="33" * 32,
                authority_sha256="44" * 32,
                dataset_export_sha256="55" * 32,
                dataset_selection_sha256="66" * 32,
                catalog_identity_sha256="77" * 32,
                training_environment_sha256="88" * 32,
                training_code_revision="fixture-revision",
            ),
            completed_units=2,
            total_units=2,
        )
        frames = stream.getvalue().splitlines()
        self.assertEqual([json.loads(frame)["sequence"] for frame in frames], [0, 1])
        self.assertTrue(all(0 < len(frame) <= MAX_EVENT_BYTES for frame in frames))
        self.assertEqual([json.loads(frame)["kind"] for frame in frames], ["progress", "result"])
        with self.assertRaises(ValueError):
            worker.progress("complete", "Late event.", 2, 2)

    def test_worker_cancellation_is_terminal_and_never_returns_candidate(self) -> None:
        stream = io.BytesIO()
        worker = WorkerProtocolWriter(
            stream,
            run_id="018f3c2a-91ab-7ccd-b3de-123456789abc",
            generation=9,
        )
        worker.progress("training", "Training candidate.", 1, 4)
        worker.error("cancelled", "Training was cancelled.", "TRAINING_CANCELLED", 1, 4)
        frames = [json.loads(frame) for frame in stream.getvalue().splitlines()]
        self.assertEqual([frame["kind"] for frame in frames], ["progress", "error"])
        self.assertTrue(all(frame["result"] is None for frame in frames))
        with self.assertRaises(ValueError):
            worker.error("cancelled", "Training was cancelled.", "TRAINING_CANCELLED", 1, 4)

    def test_worker_candidate_contains_only_rust_revalidation_evidence(self) -> None:
        evidence = CandidateEvidence(
            admission_request_sha256="99" * 32,
            candidate_directory="models/fixture-v1/candidate",
            metadata_sha256="11" * 32,
            artifact_sha256="22" * 32,
            training_run_sha256="33" * 32,
            authority_sha256="44" * 32,
            dataset_export_sha256="55" * 32,
            dataset_selection_sha256="66" * 32,
            catalog_identity_sha256="77" * 32,
            training_environment_sha256="88" * 32,
            training_code_revision="fixture-revision",
        )
        self.assertEqual(
            set(evidence.as_mapping()),
            {
                "admissionRequestSha256",
                "candidateDirectory",
                "metadataSha256",
                "artifactSha256",
                "trainingRunSha256",
                "authoritySha256",
                "datasetExportSha256",
                "datasetSelectionSha256",
                "catalogIdentitySha256",
                "trainingEnvironmentSha256",
                "trainingCodeRevision",
            },
        )

    @requires_sealed_release
    def test_signed_environment_rejects_regenerated_record_and_receipt(self) -> None:
        baseline = training_environment_receipt().sha256
        self.assertEqual(len(baseline), 64)
        authority = Path(sys.prefix) / "share/market-squawk"
        receipt = authority / "training-environment.json"
        envelope = json.loads(receipt.read_text(encoding="ascii"))

        def reject_before_fresh_import(
            source: Path, replacement: bytes, sentinel: Path
        ) -> None:
            content = source.read_bytes()
            mode = stat.S_IMODE(source.stat().st_mode)
            try:
                source.chmod(mode | stat.S_IWUSR)
                source.write_bytes(replacement)
                with self.assertRaises(ValueError):
                    training_environment_receipt()
                completed = subprocess.run(
                    [sys.executable, "-I", "-B", "-c", "import market_squawk"],
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=30,
                )
                self.assertNotEqual(completed.returncode, 0, completed.stdout)
                self.assertFalse(sentinel.exists(), completed.stdout)
            finally:
                source.write_bytes(content)
                source.chmod(mode)
            self.assertEqual(training_environment_receipt().sha256, baseline)

        with tempfile.TemporaryDirectory() as temporary:
            sentinel = Path(temporary) / "project-code-executed"
            payload = (
                "from pathlib import Path\n"
                f"Path({str(sentinel)!r}).write_text('executed', encoding='ascii')\n"
            ).encode("utf-8")
            package_source = Path(sys.modules["market_squawk"].__file__)
            implementation_source = (
                package_source
                if package_source.suffix == ".py"
                else Path(installed_training.__file__)
            )
            implementation_content = implementation_source.read_bytes()
            if implementation_source == package_source:
                replacement = payload + implementation_content
            else:
                future = b"from __future__ import annotations\n"
                self.assertIn(future, implementation_content)
                replacement = implementation_content.replace(future, future + payload, 1)
            reject_before_fresh_import(implementation_source, replacement, sentinel)

        dependency_source = Path(pa.__file__)
        with tempfile.TemporaryDirectory() as temporary:
            sentinel = Path(temporary) / "dependency-executed"
            replacement = (
                "from pathlib import Path\n"
                f"Path({str(sentinel)!r}).write_text('executed', encoding='ascii')\n"
            ).encode("utf-8")
            reject_before_fresh_import(dependency_source, replacement, sentinel)

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

    @requires_sealed_release
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

    @requires_sealed_release
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

    @requires_sealed_release
    def test_sealed_driver_produces_deterministic_onnx_and_exact_admission_request(self) -> None:
        cases = (
            (
                "linear",
                "regression",
                "018f3c2a-91ab-7ccd-b3de-123456789abc",
                "fixture-linear",
                (5, 25, 35, 65, 75, 105),
                {"kind": "price", "currency": "USD"},
                False,
            ),
            (
                "logistic",
                "binary_probability",
                "018f3c2a-91ab-7ccd-b3de-223456789abc",
                "fixture-logistic",
                (0, 10, 0, 10, 0, 10),
                {"kind": "probability"},
                True,
            ),
        )
        with tempfile.TemporaryDirectory() as release_proof_root:
            for case in cases:
                (
                    model_kind,
                    output_semantics,
                    model_id,
                    bundle_id,
                    label_mantissas,
                    output_measurement,
                    terminal_sigmoid,
                ) = case
                with self.subTest(model_kind=model_kind):
                    case_root = Path(release_proof_root) / bundle_id
                    data_root = case_root / "data"
                    authority_root = case_root / "authority"
                    request_root = case_root / "requests"
                    for path in (data_root, authority_root, request_root):
                        path.mkdir(parents=True)
                    digest = _fixture(
                        data_root,
                        label_mantissas=label_mantissas,
                        label_measurement=output_measurement,
                        initialize_root=_initialize_signed_data_root,
                    )
                    dataset = open_dataset(
                        data_root,
                        digest,
                        UtcNanoseconds(600),
                        max_rows=32,
                        context=OperationContext(60_000, 1_000_000),
                    )
                    run = _run(dataset, model_id=model_id, bundle_id=bundle_id)
                    first = run.fit_evaluate(
                        model_kind=model_kind,
                        artifact_format="onnx",
                        context=OperationContext(60_000, 1_000_000),
                    )
                    second = run.fit_evaluate(
                        model_kind=model_kind,
                        artifact_format="onnx",
                        context=OperationContext(60_000, 1_000_000),
                    )
                    self.assertEqual(
                        (
                            first.authority_bytes,
                            first.candidate.artifact_bytes,
                            first.candidate.metadata_bytes,
                            first.candidate.training_run_bytes,
                        ),
                        (
                            second.authority_bytes,
                            second.candidate.artifact_bytes,
                            second.candidate.metadata_bytes,
                            second.candidate.training_run_bytes,
                        ),
                    )
                    self.assertEqual(
                        b"Sigmoid" in first.candidate.artifact_bytes,
                        terminal_sigmoid,
                    )

                    config_path = request_root / "training.json"
                    _write_json(
                        config_path,
                        _driver_config(
                            data_root,
                            digest,
                            dataset,
                            model_kind=model_kind,
                            model_id=model_id,
                            bundle_id=bundle_id,
                        ),
                    )
                    proposal_path = request_root / "proposal.json"
                    write_proposal(config_path, proposal_path)
                    self.assertEqual(proposal_path.read_bytes(), first.authority_bytes)

                    authority_path = authority_root / "bundle-authority.json"
                    authority_path.write_bytes(proposal_path.read_bytes())
                    request_path = request_root / "admission.json"
                    finalized = finalize_candidate(
                        config_path,
                        authority_path,
                        f"models/{bundle_id}-v1",
                        request_path,
                    )
                    self.assertEqual(
                        finalized["admissionRequestSha256"],
                        hashlib.sha256(request_path.read_bytes()).hexdigest(),
                    )
                    request = json.loads(request_path.read_text(encoding="ascii"))
                    self.assertEqual(
                        [
                            json.loads(first.candidate.metadata_bytes)[
                                "output_semantics"
                            ],
                            json.loads(first.authority_bytes)["output_semantics"],
                            request["backend"]["outputSemantics"],
                        ],
                        [output_semantics] * 3,
                    )
                    self.assertEqual(
                        [
                            json.loads(first.candidate.training_run_bytes)["trial"][
                                "output_measurement"
                            ],
                            json.loads(first.candidate.metadata_bytes)[
                                "output_measurement"
                            ],
                            json.loads(first.authority_bytes)["output_measurement"],
                        ],
                        [output_measurement] * 3,
                    )
                    expected_statistic = {
                        "estimator": {
                            "kind": (
                                "sealed_direct_least_squares_v1"
                                if model_kind == "linear"
                                else "sealed_binary_logistic_v1"
                            )
                        },
                        "objective": (
                            "squared_error"
                            if model_kind == "linear"
                            else "binary_cross_entropy"
                        ),
                        "output_transform": (
                            "identity" if model_kind == "linear" else "logistic"
                        ),
                        "statistic": (
                            "model_estimated_conditional_mean"
                            if model_kind == "linear"
                            else "unavailable"
                        ),
                        "target": {
                            "horizon_nanos": 10,
                            "kind": "fixed_horizon_terminal",
                        },
                        "target_transform": "identity",
                    }
                    self.assertEqual(
                        [
                            json.loads(first.candidate.training_run_bytes)["trial"][
                                "output_statistic"
                            ],
                            json.loads(first.candidate.metadata_bytes)["output_statistic"],
                            json.loads(first.authority_bytes)["output_statistic"],
                        ],
                        [expected_statistic] * 3,
                    )
                    self.assertEqual(
                        request["backend"]["modelSha256"],
                        first.candidate.artifact_sha256,
                    )

                    self.assertNotIn("admitted", request)
                    self.assertNotIn("disposition", request)
                    rejected = _signed_prediction_attempt(
                        data_root,
                        request_root,
                        model_id=model_id,
                        bundle_id=bundle_id,
                    )
                    self.assertNotEqual(rejected.returncode, 0)


if __name__ == "__main__":
    unittest.main()
