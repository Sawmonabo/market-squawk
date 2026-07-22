from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest


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
            with self.assertRaises(builder.ReleaseBuildError):
                builder.admit_wheelhouse(lock, Path(temporary), offline=True)


if __name__ == "__main__":
    unittest.main()
