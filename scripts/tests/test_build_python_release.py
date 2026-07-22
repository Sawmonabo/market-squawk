from __future__ import annotations

import importlib.util
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
            ["/runtime/cp312", "-m", "venv", "/owned/release-cp312"], ROOT
        )
        admit.assert_called_once_with(
            Path("/owned/release-cp312/bin/python"), (3, 12, 12), ROOT
        )


if __name__ == "__main__":
    unittest.main()
