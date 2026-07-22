from __future__ import annotations

import importlib.util
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "build_python_release", ROOT / "scripts" / "build_python_release.py"
)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("release builder module could not be loaded")
builder = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = builder
SPEC.loader.exec_module(builder)


class PythonReleaseBuilderContracts(unittest.TestCase):
    def test_release_signing_seed_is_zeroed_when_the_build_fails(self) -> None:
        signer = builder.ReleaseSigner.__new__(builder.ReleaseSigner)
        signer._key = bytearray(b"s" * 32)

        with self.assertRaises(builder.ReleaseBuildError):
            with signer:
                raise builder.ReleaseBuildError("fixture failure")

        self.assertEqual(signer._key, bytearray())

    def test_lock_rejects_unknown_or_unhashed_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            lock_path = Path(temporary) / "lock.json"
            lock_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "python": {"minimum": "3.10", "maximum_exclusive": "3.15"},
                        "platform": "macos-arm64",
                        "artifacts": [
                            {
                                "project": "fixture",
                                "version": "1.0",
                                "license": "MIT",
                                "filename": "fixture-1.0-py3-none-any.whl",
                                "sha256": "00" * 32,
                                "size_bytes": 1,
                                "url": "https://files.pythonhosted.org/fixture.whl",
                            }
                        ],
                        "sources": [],
                    }
                )
            )
            with self.assertRaises(builder.ReleaseBuildError):
                builder.load_lock(lock_path)

    def test_offline_admission_never_fetches_a_missing_wheel(self) -> None:
        lock = builder.ReleaseLock.for_test(
            filename="fixture-1.0-py3-none-any.whl",
            sha256="11" * 32,
            size_bytes=1,
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifact_root = root / "artifacts"
            layout = builder.admit_artifact_root(artifact_root, ROOT)
            with self.assertRaises(builder.ReleaseBuildError):
                builder.admit_wheelhouse(lock, layout.wheelhouse, (3, 12))

    def test_unowned_artifact_root_is_never_deleted_or_claimed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact_root = Path(temporary) / "artifacts"
            artifact_root.mkdir()
            sentinel = artifact_root / "keep.txt"
            sentinel.write_text("operator-owned")
            with self.assertRaises(builder.ReleaseBuildError):
                builder.admit_artifact_root(artifact_root, ROOT)
            self.assertEqual(sentinel.read_text(), "operator-owned")

    def test_toolchain_boundary_rejects_the_previous_patch_release(self) -> None:
        with self.assertRaises(builder.ReleaseBuildError):
            builder._require_tool_release(
                "rustc 1.97.0 (old)\nbinary: rustc", "rustc", builder.RUST_TOOLCHAIN
            )

    def test_venv_is_created_by_its_admitted_runtime(self) -> None:
        runtime = builder.PythonRuntime(Path("/runtime/cp312"), (3, 12, 12))
        with mock.patch.object(builder, "_run") as run, mock.patch.object(
            builder, "_admit_created_runtime"
        ) as admit:
            builder._create_venv(runtime, Path("/owned/release-cp312"), ROOT)
        run.assert_called_once_with(
            ["/runtime/cp312", "-I", "-m", "venv", "/owned/release-cp312"],
            ROOT,
            None,
        )
        admit.assert_called_once_with(
            Path("/owned/release-cp312/bin/python"), (3, 12, 12), ROOT, None
        )

    def test_training_receipts_bind_build_foundation_and_release_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "Cargo.lock").write_text("cargo-lock", encoding="utf-8")
            requirements = root / "requirements.lock"
            requirements.write_text("requirements-lock", encoding="utf-8")
            wheelhouse_lock = root / "wheelhouse-lock.json"
            wheelhouse_lock.write_text("wheelhouse-lock", encoding="utf-8")
            interpreter = root / "python"
            interpreter.write_bytes(b"python-runtime")
            lock = builder.ReleaseLock(
                (3, 12),
                (3, 14),
                "macos-arm64",
                (),
                (builder.Source("python/training.py", "11" * 32, 7),),
            )

            foundation, digest = builder.build_training_foundation_receipt(
                root,
                requirements,
                wheelhouse_lock,
                lock,
                builder.PythonRuntime(interpreter, (3, 12, 12)),
                {"rustc": {"sha256": "22" * 32}},
                "44" * 32,
                "55" * 32,
            )
            project_wheel = root / "market_squawk-0.1.0-cp310-abi3-macosx_12_0_arm64.whl"
            project_wheel.write_bytes(b"wheel")
            signer = mock.Mock()
            signer.sign.return_value = "66" * 64
            manifest, manifest_digest = builder.build_release_manifest(
                digest,
                project_wheel,
                "cp310",
                "abi3",
                "macosx_12_0_arm64",
                7,
                "33" * 32,
                signer,
            )

            value = json.loads(foundation)
            release = json.loads(manifest)
            self.assertEqual(hashlib.sha256(foundation).hexdigest(), digest)
            self.assertEqual(value["training_code_revision"], value["source_closure_sha256"])
            self.assertEqual(hashlib.sha256(manifest).hexdigest(), manifest_digest)
            self.assertEqual(value["release_public_key"], "44" * 32)
            self.assertEqual(release["payload"]["foundation_sha256"], digest)
            self.assertEqual(release["payload"]["validator"]["sha256"], "33" * 32)
            self.assertEqual(release["signature"], "66" * 64)
            self.assertEqual(
                release["payload"]["project_wheel"]["sha256"],
                hashlib.sha256(b"wheel").hexdigest(),
            )


if __name__ == "__main__":
    unittest.main()
