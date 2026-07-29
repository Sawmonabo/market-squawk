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
    def test_platform_profiles_cover_the_exact_release_matrix(self) -> None:
        expected = {
            "aarch64-apple-darwin": ("macOS 12", "bin/python", ""),
            "x86_64-apple-darwin": ("macOS 12", "bin/python", ""),
            "x86_64-pc-windows-msvc": (
                "Windows 10 1809",
                "python.exe",
                ".exe",
            ),
            "x86_64-unknown-linux-gnu": (
                "Ubuntu 24.04-compatible",
                "bin/python",
                "",
            ),
        }

        self.assertEqual(set(builder.PLATFORM_PROFILES), set(expected))
        for target, contract in expected.items():
            with self.subTest(target=target):
                profile = builder.platform_profile(target)
                self.assertEqual(
                    (
                        profile.minimum_system,
                        profile.interpreter_relative_path,
                        profile.executable_suffix,
                    ),
                    contract,
                )

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

    def test_repository_lock_admits_the_complete_source_closure(self) -> None:
        lock = builder.load_lock(ROOT / "python" / "wheelhouse-lock.json")

        expected = builder.expected_source_paths(ROOT)
        self.assertIn("apps/market-squawk/Cargo.toml", expected)
        self.assertIn("apps/market-squawk/src/main.rs", expected)
        self.assertIn("crates/market-squawk-platform/build_support.rs", expected)
        self.assertIn("docs/verification/onnx-runtime-policy.json", expected)
        self.assertIn(
            "docs/reports/performance/2026-07-17-q2-a4-writer-runtime-proof.md",
            expected,
        )
        builder.admit_sources(lock, ROOT)

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

    def test_release_reset_never_follows_an_intermediate_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            layout = builder.admit_artifact_root(root / "artifacts", ROOT)
            release = layout.releases[0][1]
            builder._admit_owned_child(release, layout.root, "release-cp314")
            external = root / "external"
            authority = external / "market-squawk"
            authority.mkdir(parents=True, mode=0o755)
            sentinel = authority / "operator-owned"
            sentinel.write_text("preserve", encoding="utf-8")
            (release / "share").symlink_to(external, target_is_directory=True)

            with self.assertRaises(builder.ReleaseBuildError):
                builder._reset_owned_child(release, layout.root, "release-cp314")

            self.assertEqual(authority.stat().st_mode & 0o777, 0o755)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "preserve")

    def test_toolchain_boundary_rejects_the_previous_patch_release(self) -> None:
        with self.assertRaises(builder.ReleaseBuildError):
            builder._require_tool_release(
                "rustc 1.97.0 (old)\nbinary: rustc", "rustc", builder.RUST_TOOLCHAIN
            )

    def test_venv_is_created_by_its_admitted_runtime(self) -> None:
        runtime = builder.PythonRuntime(Path("/runtime/cp314"), (3, 14, 6))
        with mock.patch.object(builder, "_run") as run, mock.patch.object(
            builder, "_admit_created_runtime"
        ) as admit:
            builder._create_venv(runtime, Path("/owned/release-cp314"), ROOT)
        run.assert_called_once_with(
            ["/runtime/cp314", "-I", "-m", "venv", "/owned/release-cp314"],
            ROOT,
            None,
        )
        admit.assert_called_once_with(
            Path("/owned/release-cp314/bin/python"), (3, 14, 6), ROOT, None
        )

    def test_native_release_build_enables_only_application_release_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cargo = root / "cargo"
            cargo.write_bytes(b"cargo")
            profile = builder.host_profile()
            release = root / "target" / profile.target / "release"
            release.mkdir(parents=True)
            for name in (
                "market-squawk",
                "market-squawk-model-validator",
                "market-squawk-onnx-worker",
                "market-squawk-train",
            ):
                (release / name).write_bytes(name.encode("ascii"))
            environment = {"CARGO_INCREMENTAL": "0"}

            with mock.patch.object(builder, "_run") as run:
                executables = builder._build_native_release_executables(
                    root,
                    {
                        "cargo": builder._tool_binding(cargo),
                        "target": profile.target,
                    },
                    environment,
                )

            run.assert_called_once_with(
                [
                    str(cargo),
                    "build",
                    "-p",
                    "market-squawk",
                    "--bin",
                    "market-squawk",
                    "-p",
                    "market-squawk-modeling",
                    "--bin",
                    "market-squawk-model-validator",
                    "--bin",
                    "market-squawk-onnx-worker",
                    "--bin",
                    "market-squawk-train",
                    "--no-default-features",
                    "--features",
                    "market-squawk/release-evidence",
                    "--release",
                    "--locked",
                ],
                root,
                environment,
            )
            self.assertEqual(executables.application, release / "market-squawk")

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
                (3, 14),
                (3, 15),
                "aarch64-apple-darwin",
                (),
                (builder.Source("python/training.py", "11" * 32, 7),),
            )

            foundation, digest = builder.build_training_foundation_receipt(
                root,
                requirements,
                wheelhouse_lock,
                lock,
                "33" * 32,
                builder.PythonRuntime(interpreter, (3, 14, 6)),
                {"rustc": {"sha256": "22" * 32}},
                (builder.RuntimeRequirement("pyarrow", "25.0.0"),),
                "44" * 32,
                "55" * 32,
            )
            project_wheel = root / "market_squawk-1.0.0-cp310-abi3-macosx_12_0_arm64.whl"
            project_wheel.write_bytes(b"wheel")
            layout = builder.admit_artifact_root(root / "artifacts", ROOT)
            canonical_release = layout.releases[0][1]
            builder._admit_owned_child(
                canonical_release,
                layout.root,
                "release-cp314",
            )
            native_bin = canonical_release / "bin"
            native_bin.mkdir()
            built_bin = root / "target" / "release"
            built_bin.mkdir(parents=True)
            built = builder.NativeReleaseExecutables(
                application=built_bin / "market-squawk",
                onnx_worker=built_bin / "market-squawk-onnx-worker",
                training_driver=built_bin / "market-squawk-train",
                validator=built_bin / "market-squawk-model-validator",
            )
            built.application.write_bytes(b"application")
            built.onnx_worker.write_bytes(b"onnx-worker")
            built.training_driver.write_bytes(b"training-driver")
            built.validator.write_bytes(b"validator")
            installed = builder._copy_native_release_executables(
                built,
                canonical_release,
            )
            application = installed.application
            onnx_worker = installed.onnx_worker
            training_driver = installed.training_driver
            validator = installed.validator
            self.assertEqual(application.stat().st_mode & 0o777, 0o555)
            self.assertEqual(training_driver.stat().st_mode & 0o777, 0o555)
            self.assertNotEqual(application.stat().st_ino, built.application.stat().st_ino)
            signer = mock.Mock()
            signer.sign.return_value = "66" * 64
            manifest, manifest_digest = builder.build_release_manifest(
                digest,
                project_wheel,
                "cp310",
                "abi3",
                "macosx_12_0_arm64",
                canonical_release,
                builder.platform_profile("aarch64-apple-darwin"),
                signer,
            )

            value = json.loads(foundation)
            release = json.loads(manifest)
            self.assertEqual(hashlib.sha256(foundation).hexdigest(), digest)
            self.assertEqual(value["training_code_revision"], value["source_closure_sha256"])
            self.assertEqual(hashlib.sha256(manifest).hexdigest(), manifest_digest)
            self.assertEqual(value["release_public_key"], "44" * 32)
            self.assertEqual(value["release_components_sha256"], "33" * 32)
            self.assertEqual(
                value["runtime_distributions"],
                [{"name": "pyarrow", "version": "25.0.0"}],
            )
            self.assertEqual(release["payload"]["foundation_sha256"], digest)
            self.assertEqual(release["schema_version"], 3)
            self.assertEqual(release["payload"]["schema_version"], 3)
            self.assertEqual(
                release["payload"]["project_wheel"]["target"],
                "aarch64-apple-darwin",
            )
            self.assertEqual(
                release["payload"]["application"]["sha256"],
                hashlib.sha256(b"application").hexdigest(),
            )
            self.assertEqual(
                release["payload"]["onnx_worker"]["sha256"],
                hashlib.sha256(b"onnx-worker").hexdigest(),
            )
            self.assertEqual(
                release["payload"]["training_driver"]["sha256"],
                hashlib.sha256(b"training-driver").hexdigest(),
            )
            self.assertEqual(
                release["payload"]["validator"]["sha256"],
                hashlib.sha256(b"validator").hexdigest(),
            )
            self.assertEqual(release["signature"], "66" * 64)
            self.assertEqual(
                release["payload"]["project_wheel"]["sha256"],
                hashlib.sha256(b"wheel").hexdigest(),
            )
            self.assertEqual(
                builder.MAX_APPLICATION_EXECUTABLE_BYTES,
                768 * 1024 * 1024,
            )
            self.assertEqual(
                builder.MAX_ONNX_WORKER_EXECUTABLE_BYTES,
                256 * 1024 * 1024,
            )
            self.assertEqual(
                builder.MAX_VALIDATOR_EXECUTABLE_BYTES,
                256 * 1024 * 1024,
            )

            for executable, maximum_bytes, original in (
                (
                    application,
                    builder.MAX_APPLICATION_EXECUTABLE_BYTES,
                    b"application",
                ),
                (
                    onnx_worker,
                    builder.MAX_ONNX_WORKER_EXECUTABLE_BYTES,
                    b"onnx-worker",
                ),
                (
                    validator,
                    builder.MAX_VALIDATOR_EXECUTABLE_BYTES,
                    b"validator",
                ),
            ):
                with self.subTest(executable=executable.name):
                    executable.chmod(0o755)
                    with executable.open("r+b") as oversized:
                        oversized.truncate(maximum_bytes + 1)
                    signer.reset_mock()
                    with self.assertRaises(builder.ReleaseBuildError):
                        builder.build_release_manifest(
                            digest,
                            project_wheel,
                            "cp310",
                            "abi3",
                            "macosx_12_0_arm64",
                            canonical_release,
                            builder.platform_profile("aarch64-apple-darwin"),
                            signer,
                        )
                    signer.sign.assert_not_called()
                    executable.write_bytes(original)
                    executable.chmod(0o555)


if __name__ == "__main__":
    unittest.main()
