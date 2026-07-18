"""Bounded parser tests for diagnostic capture build evidence."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from scripts.capture_benchmark_prepare_build_evidence import (
    BUILD_COMMAND,
    BUILD_ENVIRONMENT_POLICY,
    EVIDENCE_MODE,
    EXPECTED_CARGO_FEATURES,
    FEATURE,
    MAX_BINDINGS_BYTES,
    MAX_CARGO_JSON_BYTES,
    MAX_CARGO_STDERR_BYTES,
    PROFILE,
    RESULT_SCHEMA_VERSION,
    TARGET,
    GateError,
    artifact_hash,
    benchmark_backend_binding,
    canonical_json,
    parse_cargo_artifact,
    runner_bindings,
    run_closed_cargo_build,
    sanitized_build_environment,
    sha256,
    strict_json,
    external_tool_hash,
    validate_current_bindings,
    validate_baseline_inputs,
    validate_candidate_current_bindings,
    recompute_current_bindings,
)
from scripts.capture_benchmark_process import bounded_process

REPOSITORY = Path(__file__).resolve().parents[2]


def host_tool_bindings() -> tuple[Path, str, Path, str]:
    cargo = Path(os.path.realpath(shutil.which("cargo") or ""))
    git = Path(os.path.realpath(shutil.which("git") or ""))
    if not cargo.is_absolute() or not git.is_absolute():
        raise RuntimeError("test host lacks Cargo or Git")
    return cargo, external_tool_hash(cargo), git, external_tool_hash(git)


def cargo_message(executable: Path) -> dict:
    return {
        "reason": "compiler-artifact",
        "target": {"name": TARGET, "kind": ["bench"]},
        "profile": {"opt_level": "3", "debug_assertions": False},
        "features": list(EXPECTED_CARGO_FEATURES),
        "executable": str(executable),
    }


def binding_value() -> dict:
    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "runner": TARGET,
        "evidence_mode": EVIDENCE_MODE,
        "evidence_backend": "standard",
        "queue_transport": "standard_sync_channel",
        "queue_private_storage_accounting": "not_measured",
        "build_profile": PROFILE,
        "measured_code_head": "1" * 40,
        "clean_build_enforced": True,
        "build_environment_policy": BUILD_ENVIRONMENT_POLICY,
        "build_command_sha256": "f" * 64,
        "build_environment_sha256": "1" * 64,
        "source_inventory_sha256": "2" * 64,
        "cargo_lock_sha256": "3" * 64,
        "workspace_manifest_sha256": "4" * 64,
        "package_manifest_sha256": "5" * 64,
        "build_script_sha256": "b" * 64,
        "build_support_sha256": "2" * 64,
        "cargo_executable_sha256": "8" * 64,
        "git_executable_sha256": "9" * 64,
        "host_gate_shell_sha256": "c" * 64,
        "host_gate_python_sha256": "d" * 64,
        "host_gate_process_sha256": "0" * 64,
        "host_gate_evidence_io_sha256": "1" * 64,
        "host_gate_cli_sha256": "2" * 64,
        "host_gate_schema_sha256": "3" * 64,
        "host_gate_execution_sha256": "6" * 64,
        "host_gate_observation_sha256": "4" * 64,
        "host_gate_measured_sha256": "5" * 64,
        "build_evidence_python_sha256": "e" * 64,
        "platform_source_sha256": "6" * 64,
        "domain_source_sha256": "7" * 64,
        "entrypoint_sha256": "8" * 64,
        "backend_dispatcher_sha256": "2" * 64,
        "selected_backend_source_path": (
            "crates/market-squawk-platform/benches/"
            "capture_admission/backend/standard.rs"
        ),
        "selected_backend_source_sha256": "3" * 64,
        "backend_sha256": "9" * 64,
        "criterion_sha256": "a" * 64,
        "observer_sha256": "1" * 64,
        "baseline_lock_path": None,
        "baseline_lock_sha256": None,
        "baseline_manifest_path": None,
        "baseline_manifest_sha256": None,
        "baseline_measured_code_head": None,
        "immutable_module_sha256": {"fixture": "b" * 64},
    }


def baseline_fixture(current_head: str = "f" * 40) -> tuple[bytes, bytes, dict, dict]:
    digest = "1" * 64
    immutable = {"fixture": "2" * 64}
    tools = {
        "build.rs": "3" * 64,
        "build_support.rs": "2" * 64,
        "capture_benchmark_host_gate.sh": "4" * 64,
        "capture_benchmark_host_gate.py": "5" * 64,
        "capture_benchmark_process.py": "6" * 64,
        "capture_benchmark_evidence_io.py": "a" * 64,
        "capture_benchmark_host_cli.py": "b" * 64,
        "capture_benchmark_host_schema.py": "c" * 64,
        "capture_benchmark_host_execution.py": "f" * 64,
        "capture_benchmark_host_observation.py": "d" * 64,
        "capture_benchmark_host_measured.py": "e" * 64,
        "capture_benchmark_prepare_build_evidence.py": "7" * 64,
        "cargo-executable": "8" * 64,
        "git-executable": "9" * 64,
    }
    manifest = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "runner": TARGET,
        "evidence_mode": EVIDENCE_MODE,
        "criterion_evidence_mode": "exploratory_zero_authority",
        "measured_code_head": "a" * 40,
        "build_evidence_sha256": digest,
        "baseline_manifest_sha256": None,
        "baseline_lock_sha256": None,
        "build_environment_policy": BUILD_ENVIRONMENT_POLICY,
        "build_command_sha256": digest,
        "build_environment_sha256": digest,
        "cargo_executable_sha256": digest,
        "git_executable_sha256": "9" * 64,
        "cargo_json_sha256": digest,
        "source_inventory_sha256": digest,
        "cargo_lock_sha256": digest,
        "criterion_sha256": "9" * 64,
        "observer_sha256": "b" * 64,
        "backend": "standard",
        "queue_transport": "standard_sync_channel",
        "queue_private_storage_accounting": "not_measured",
        "benchmark_support_feature": FEATURE,
        "fixtures": ["matrix", "comparable_full", "sustained_rss"],
        "repetitions": [1, 2, 3, 4, 5],
        "executable_path": "./capture_admission_evidence-exe",
        "executable_sha256": digest,
        "immutable_module_sha256": immutable,
        "entrypoint_sha256": "c" * 64,
        "backend_sha256": "d" * 64,
        "production_library_sha256": {"platform": digest},
        "repetition_sha256": {"repetition-1.json": digest},
        "artifact_sha256": {"artifact": digest},
        "tool_sha256": tools,
        "host_fingerprint_sha256": digest,
        "toolchain_fingerprint_sha256": digest,
        "release_profile_sha256": digest,
        "host_gate": {
            "valid": True,
            "preflight_sha256": digest,
            "postflight_sha256": digest,
            "comparison_sha256": digest,
            "monitor_sha256": digest,
        },
    }
    manifest_bytes = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
    lock = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "state": "frozen_standard_baseline",
        "baseline_head": manifest["measured_code_head"],
        "manifest_sha256": sha256(manifest_bytes),
        "manifest_reference": (
            "target/q2-a4-capture-benchmark/standard-"
            f"{manifest['measured_code_head']}/manifest.json"
        ),
        "report_reference": (
            "docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md"
        ),
        "report_sha256": "e" * 64,
        "approval_state": "independent_seed_review_approved",
        "approval_identity": "q2-a4-seed-checkpoint-review",
        "backend": "standard",
        "queue_transport": manifest["queue_transport"],
        "queue_private_storage_accounting": manifest[
            "queue_private_storage_accounting"
        ],
        "backend_sha256": manifest["backend_sha256"],
        "build_evidence_sha256": manifest["build_evidence_sha256"],
        "immutable_module_sha256": immutable,
        "entrypoint_sha256": manifest["entrypoint_sha256"],
        "criterion_sha256": manifest["criterion_sha256"],
        "observer_sha256": manifest["observer_sha256"],
        "tool_sha256": tools,
        "artifact_sha256": manifest["artifact_sha256"],
        "repetition_sha256": manifest["repetition_sha256"],
        "host_fingerprint_sha256": manifest["host_fingerprint_sha256"],
        "toolchain_fingerprint_sha256": manifest["toolchain_fingerprint_sha256"],
        "release_profile_sha256": manifest["release_profile_sha256"],
    }
    lock_bytes = (json.dumps(lock, sort_keys=True, separators=(",", ":")) + "\n").encode()
    self = {
        "immutable_module_sha256": immutable,
        "entrypoint_sha256": manifest["entrypoint_sha256"],
        "criterion_sha256": manifest["criterion_sha256"],
        "observer_sha256": manifest["observer_sha256"],
        "backend_sha256": "e" * 64,
        "build_script_sha256": tools["build.rs"],
        "build_support_sha256": tools["build_support.rs"],
        "cargo_executable_sha256": tools["cargo-executable"],
        "git_executable_sha256": tools["git-executable"],
        "host_gate_shell_sha256": tools["capture_benchmark_host_gate.sh"],
        "host_gate_python_sha256": tools["capture_benchmark_host_gate.py"],
        "host_gate_process_sha256": tools["capture_benchmark_process.py"],
        "host_gate_evidence_io_sha256": tools["capture_benchmark_evidence_io.py"],
        "host_gate_cli_sha256": tools["capture_benchmark_host_cli.py"],
        "host_gate_schema_sha256": tools["capture_benchmark_host_schema.py"],
        "host_gate_execution_sha256": tools[
            "capture_benchmark_host_execution.py"
        ],
        "host_gate_observation_sha256": tools[
            "capture_benchmark_host_observation.py"
        ],
        "host_gate_measured_sha256": tools["capture_benchmark_host_measured.py"],
        "build_evidence_python_sha256": tools[
            "capture_benchmark_prepare_build_evidence.py"
        ],
    }
    return lock_bytes, manifest_bytes, self, manifest


def initialize_fixture_repository(root: Path) -> tuple[Path, str]:
    repository = (root / "repository").resolve()
    repository.mkdir()
    for name in ("Cargo.toml", "Cargo.lock"):
        shutil.copy2(REPOSITORY / name, repository / name)
    platform = repository / "crates/market-squawk-platform"
    platform.mkdir(parents=True)
    for name in ("Cargo.toml", "build.rs"):
        shutil.copy2(REPOSITORY / "crates/market-squawk-platform" / name, platform / name)
    shutil.copy2(
        REPOSITORY / "crates/market-squawk-platform/build_support.rs",
        platform / "build_support.rs",
    )
    shutil.copytree(REPOSITORY / "crates/market-squawk-platform/src", platform / "src")
    shutil.copytree(REPOSITORY / "crates/market-squawk-platform/benches", platform / "benches")
    domain = repository / "crates/market-squawk-domain"
    domain.mkdir(parents=True)
    shutil.copytree(REPOSITORY / "crates/market-squawk-domain/src", domain / "src")
    scripts = repository / "scripts"
    scripts.mkdir()
    for name in (
        "capture_benchmark_host_gate.sh",
        "capture_benchmark_host_gate.py",
        "capture_benchmark_process.py",
        "capture_benchmark_evidence_io.py",
        "capture_benchmark_host_cli.py",
        "capture_benchmark_host_schema.py",
        "capture_benchmark_host_execution.py",
        "capture_benchmark_host_observation.py",
        "capture_benchmark_host_measured.py",
        "capture_benchmark_prepare_build_evidence.py",
    ):
        shutil.copy2(REPOSITORY / "scripts" / name, scripts / name)
    (repository / ".gitignore").write_text("target/\n")
    subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.name", "Fixture"], cwd=repository, check=True)
    subprocess.run(
        ["git", "config", "user.email", "fixture@example.invalid"],
        cwd=repository,
        check=True,
    )
    subprocess.run(["git", "add", "."], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=repository, check=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return repository, head


class BuildEvidenceTest(unittest.TestCase):
    def test_backend_binding_has_canonical_golden_vectors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            repository = Path(temporary).resolve()
            benchmark = (
                repository
                / "crates/market-squawk-platform/benches/capture_admission"
            )
            selected = benchmark / "backend"
            selected.mkdir(parents=True)
            (benchmark / "backend.rs").write_bytes(b"closed dispatcher")
            (selected / "standard.rs").write_bytes(b"bounded standard channel")
            (selected / "candidate.rs").write_bytes(b"bounded fixed ring")

            standard = benchmark_backend_binding(repository, "standard")
            candidate = benchmark_backend_binding(repository, "candidate")
            self.assertEqual(
                standard,
                {
                    "backend_dispatcher_sha256": (
                        "47718f8722eb36c9acbcdfb146ca49a5f4ea84ef2cf8f0eb2b4d72521ce917ec"
                    ),
                    "selected_backend_source_path": (
                        "crates/market-squawk-platform/benches/"
                        "capture_admission/backend/standard.rs"
                    ),
                    "selected_backend_source_sha256": (
                        "86d2ead4846f7aa630094faa003ece14b740742f601f33e23c262da386273ce5"
                    ),
                    "backend_sha256": (
                        "170a74eb9f9c520c126897147812d966c37bf9019858d557e8242bae25fcc3e4"
                    ),
                },
            )
            self.assertEqual(
                candidate["selected_backend_source_path"],
                (
                    "crates/market-squawk-platform/benches/"
                    "capture_admission/backend/candidate.rs"
                ),
            )
            self.assertEqual(
                candidate["selected_backend_source_sha256"],
                "b19b8841668286305384ee37f26677d4df5ee65ab4087fb4b10e7e4bfa8d9d48",
            )
            self.assertEqual(
                candidate["backend_sha256"],
                "1ca93724f5c4a5e6ece59b4f90eeb980cdb51f0ec0b2d63ac5da6e7426086fc8",
            )
            self.assertNotEqual(
                standard["backend_sha256"], candidate["backend_sha256"]
            )

            (selected / "candidate.rs").write_bytes(b"bounded standard channel")
            with self.assertRaises(GateError):
                benchmark_backend_binding(repository, "candidate")
            with self.assertRaises(GateError):
                benchmark_backend_binding(repository, "STANDARD")

    def test_cargo_parser_accepts_one_exact_all_feature_release_artifact(self) -> None:
        executable = REPOSITORY / "target" / "fixture-executable"
        encoded = (json.dumps(cargo_message(executable)) + "\n").encode()
        path, message = parse_cargo_artifact(encoded, REPOSITORY)
        self.assertEqual(path, executable)
        self.assertEqual(message["target"]["name"], TARGET)

    def test_real_rust_runner_matches_python_binding_contract(self) -> None:
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("CAPTURE_BENCH_")
        }
        environment["CAPTURE_BENCH_DEVELOPMENT_BACKEND"] = "standard"
        build = bounded_process(
            list(BUILD_COMMAND),
            cwd=REPOSITORY,
            env=environment,
            timeout_seconds=600,
            maximum_stdout=MAX_CARGO_JSON_BYTES,
            maximum_stderr=MAX_CARGO_STDERR_BYTES,
        )
        self.assertEqual(build.returncode, 0, build.stderr.decode(errors="replace"))
        executable, message = parse_cargo_artifact(build.stdout, REPOSITORY)
        self.assertEqual(message["features"], list(EXPECTED_CARGO_FEATURES))
        emitted = bounded_process(
            [str(executable), "--print-build-bindings"],
            env={},
            timeout_seconds=10,
            maximum_stdout=MAX_BINDINGS_BYTES,
            maximum_stderr=0,
        )
        self.assertEqual(emitted.returncode, 0)
        emitted_bindings = strict_json(emitted.stdout)
        self.assertEqual(emitted.stdout, canonical_json(emitted_bindings))
        self.assertEqual(emitted_bindings["schema_version"], RESULT_SCHEMA_VERSION)
        self.assertEqual(
            {
                field: emitted_bindings[field]
                for field in (
                    "evidence_backend",
                    "queue_transport",
                    "queue_private_storage_accounting",
                    "clean_build_enforced",
                    "build_environment_policy",
                )
            },
            {
                "evidence_backend": "standard",
                "queue_transport": "standard_sync_channel",
                "queue_private_storage_accounting": "not_measured",
                "clean_build_enforced": False,
                "build_environment_policy": "development-unverified",
            },
        )
        backend = benchmark_backend_binding(REPOSITORY, "standard")
        self.assertEqual(
            {field: emitted_bindings[field] for field in backend},
            backend,
        )

    def test_cargo_parser_rejects_duplicate_target_artifacts(self) -> None:
        executable = REPOSITORY / "target" / "fixture-executable"
        line = json.dumps(cargo_message(executable)) + "\n"
        with self.assertRaises(GateError):
            parse_cargo_artifact((line + line).encode(), REPOSITORY)

    def test_cargo_parser_rejects_missing_feature(self) -> None:
        executable = REPOSITORY / "target" / "fixture-executable"
        message = cargo_message(executable)
        message["features"] = [
            feature for feature in EXPECTED_CARGO_FEATURES if feature != FEATURE
        ]
        with self.assertRaises(GateError):
            parse_cargo_artifact((json.dumps(message) + "\n").encode(), REPOSITORY)

    def test_cargo_parser_rejects_extra_feature(self) -> None:
        executable = REPOSITORY / "target" / "fixture-executable"
        message = cargo_message(executable)
        message["features"].append("unexpected")
        with self.assertRaises(GateError):
            parse_cargo_artifact((json.dumps(message) + "\n").encode(), REPOSITORY)

    def test_artifact_hash_rejects_symlink(self) -> None:
        target_root = REPOSITORY / "target"
        target_root.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=target_root) as temporary:
            directory = Path(temporary)
            real = directory / "real"
            real.write_bytes(b"artifact")
            linked = directory / "linked"
            linked.symlink_to(real)
            with self.assertRaises(GateError):
                artifact_hash(linked, REPOSITORY)

    def test_artifact_hash_rejects_path_identity_change(self) -> None:
        target_root = REPOSITORY / "target"
        target_root.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=target_root) as temporary:
            artifact = Path(temporary) / "artifact"
            artifact.write_bytes(b"artifact")
            real_stat = os.stat

            def changed_stat(path: object, *args: object, **kwargs: object) -> os.stat_result:
                result = real_stat(path, *args, **kwargs)
                if Path(path) == artifact and kwargs.get("follow_symlinks") is False:
                    values = list(result)
                    values[1] += 1
                    return os.stat_result(values)
                return result

            with mock.patch(
                "scripts.capture_benchmark_prepare_build_evidence.os.stat",
                side_effect=changed_stat,
            ):
                with self.assertRaises(GateError):
                    artifact_hash(artifact, REPOSITORY)

    def test_runner_binding_schema_is_exact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            script = Path(temporary) / "runner"
            script.write_text(
                "#!/bin/sh\nprintf '%s\\n' '"
                + json.dumps(binding_value(), separators=(",", ":"))
                + "'\n"
            )
            os.chmod(script, 0o700)
            self.assertEqual(runner_bindings(script)["runner"], TARGET)

    def test_runner_binding_extra_key_is_rejected(self) -> None:
        value = binding_value()
        value["unexpected"] = True
        with tempfile.TemporaryDirectory() as temporary:
            script = Path(temporary) / "runner"
            script.write_text(
                "#!/bin/sh\nprintf '%s\\n' '"
                + json.dumps(value, separators=(",", ":"))
                + "'\n"
            )
            os.chmod(script, 0o700)
            with self.assertRaises(GateError):
                runner_bindings(script)

    def test_runner_binding_uppercase_digest_is_rejected(self) -> None:
        value = binding_value()
        value["host_gate_python_sha256"] = "A" * 64
        with tempfile.TemporaryDirectory() as temporary:
            script = Path(temporary) / "runner"
            script.write_text(
                "#!/bin/sh\nprintf '%s\\n' '"
                + json.dumps(value, separators=(",", ":"))
                + "'\n"
            )
            os.chmod(script, 0o700)
            with self.assertRaises(GateError):
                runner_bindings(script)

    def test_runner_binding_requires_clean_build_enforcement(self) -> None:
        value = binding_value()
        value["clean_build_enforced"] = False
        with tempfile.TemporaryDirectory() as temporary:
            script = Path(temporary) / "runner"
            script.write_text(
                "#!/bin/sh\nprintf '%s\\n' '"
                + json.dumps(value, separators=(",", ":"))
                + "'\n"
            )
            os.chmod(script, 0o700)
            with self.assertRaises(GateError):
                runner_bindings(script)

    def test_runner_binding_rejects_transport_storage_identity_forgery(self) -> None:
        cases = (
            ("standard-exact", "standard", "standard_sync_channel", "exact"),
            ("standard-fixed", "standard", "candidate_fixed_ring", "not_measured"),
            ("candidate-opaque", "candidate", "candidate_fixed_ring", "not_measured"),
            ("candidate-standard", "candidate", "standard_sync_channel", "exact"),
        )
        for name, backend, transport, accounting in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                value = binding_value()
                value["evidence_backend"] = backend
                value["queue_transport"] = transport
                value["queue_private_storage_accounting"] = accounting
                value["selected_backend_source_path"] = (
                    "crates/market-squawk-platform/benches/capture_admission/backend/"
                    f"{backend}.rs"
                )
                if backend == "candidate":
                    value["baseline_lock_path"] = "./baseline-lock.json"
                    value["baseline_lock_sha256"] = "c" * 64
                    value["baseline_manifest_path"] = "./baseline-manifest.json"
                    value["baseline_manifest_sha256"] = "d" * 64
                    value["baseline_measured_code_head"] = "2" * 40
                script = Path(temporary) / "runner"
                script.write_text(
                    "#!/bin/sh\nprintf '%s\\n' '"
                    + json.dumps(value, separators=(",", ":"))
                    + "'\n"
                )
                os.chmod(script, 0o700)
                with self.assertRaises(GateError):
                    runner_bindings(script)

    def test_runner_binding_rejects_retired_schema_version(self) -> None:
        value = binding_value()
        value["schema_version"] = RESULT_SCHEMA_VERSION - 1
        with tempfile.TemporaryDirectory() as temporary:
            script = Path(temporary) / "runner"
            script.write_text(
                "#!/bin/sh\nprintf '%s\\n' '"
                + json.dumps(value, separators=(",", ":"))
                + "'\n"
            )
            os.chmod(script, 0o700)
            with self.assertRaises(GateError):
                runner_bindings(script)

    def test_sanitized_environment_binds_exact_command_and_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repository = root / "repository"
            home = root / "home"
            repository.mkdir()
            home.mkdir()
            environment, command_digest, environment_digest = sanitized_build_environment(
                repository,
                {"PATH": "/usr/bin:/bin", "HOME": str(home), "IGNORED_SECRET": "secret"},
                *host_tool_bindings(),
            )
            self.assertEqual(command_digest, sha256(canonical_json(list(BUILD_COMMAND))))
            self.assertEqual(environment["CAPTURE_BENCH_BUILD_POLICY"], BUILD_ENVIRONMENT_POLICY)
            self.assertEqual(environment["CAPTURE_BENCH_BUILD_ENV_SHA256"], environment_digest)
            self.assertNotIn("IGNORED_SECRET", environment)

    def test_sanitized_environment_rejects_profile_override(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repository = root / "repository"
            home = root / "home"
            repository.mkdir()
            home.mkdir()
            with self.assertRaises(GateError):
                sanitized_build_environment(
                    repository,
                    {
                        "PATH": "/usr/bin:/bin",
                        "HOME": str(home),
                        "CARGO_PROFILE_RELEASE_LTO": "false",
                    },
                    *host_tool_bindings(),
                )

    def test_sanitized_environment_rejects_loader_injection(self) -> None:
        for name in ("LD_PRELOAD", "DYLD_INSERT_LIBRARIES", "DYLD_LIBRARY_PATH"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                repository = root / "repository"
                home = root / "home"
                repository.mkdir()
                home.mkdir()
                with self.assertRaises(GateError):
                    sanitized_build_environment(
                        repository,
                        {
                            "PATH": "/usr/bin:/bin",
                            "HOME": str(home),
                            name: "/tmp/injected",
                        },
                        *host_tool_bindings(),
                    )

    def test_sanitized_environment_rejects_discovered_cargo_config(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repository = root / "repository"
            home = root / "home"
            repository.mkdir()
            (home / ".cargo").mkdir(parents=True)
            (home / ".cargo/config.toml").write_text("[profile.release]\nlto = false\n")
            with self.assertRaises(GateError):
                sanitized_build_environment(
                    repository,
                    {"PATH": "/usr/bin:/bin", "HOME": str(home)},
                    *host_tool_bindings(),
                )

    def test_current_binding_drift_is_rejected(self) -> None:
        value = binding_value()
        current_fields = {
            "source_inventory_sha256",
            "cargo_lock_sha256",
            "workspace_manifest_sha256",
            "package_manifest_sha256",
            "build_script_sha256",
            "build_support_sha256",
            "cargo_executable_sha256",
            "git_executable_sha256",
            "host_gate_shell_sha256",
            "host_gate_python_sha256",
            "host_gate_process_sha256",
            "host_gate_evidence_io_sha256",
            "host_gate_cli_sha256",
            "host_gate_schema_sha256",
            "host_gate_execution_sha256",
            "host_gate_observation_sha256",
            "host_gate_measured_sha256",
            "build_evidence_python_sha256",
            "platform_source_sha256",
            "domain_source_sha256",
            "entrypoint_sha256",
            "backend_dispatcher_sha256",
            "selected_backend_source_path",
            "selected_backend_source_sha256",
            "backend_sha256",
            "criterion_sha256",
            "observer_sha256",
            "immutable_module_sha256",
        }
        current = {field: value[field] for field in current_fields}
        validate_current_bindings(value, current)
        missing_execution = dict(current)
        del missing_execution["host_gate_execution_sha256"]
        with self.assertRaises(GateError):
            validate_current_bindings(value, missing_execution)
        mismatched_execution = dict(current)
        mismatched_execution["host_gate_execution_sha256"] = "0" * 64
        with self.assertRaises(GateError):
            validate_current_bindings(value, mismatched_execution)

    def test_tracked_baseline_lock_rejection_matrix(self) -> None:
        cases = (
            "success",
            "missing_lock_member",
            "tampered_manifest",
            "self_consistent_forged_manifest",
            "wrong_head",
            "wrong_harness",
            "wrong_tool",
            "matrix_drift",
            "baseline_equals_candidate",
            "same_backend",
            "false_exact_storage",
        )
        for case in cases:
            with self.subTest(case=case):
                lock_bytes, manifest_bytes, current, manifest = baseline_fixture()
                current_head = "f" * 40
                if case == "missing_lock_member":
                    lock = json.loads(lock_bytes)
                    del lock["state"]
                    lock_bytes = (json.dumps(lock, sort_keys=True) + "\n").encode()
                elif case == "tampered_manifest":
                    manifest["source_inventory_sha256"] = "0" * 64
                    manifest_bytes = (json.dumps(manifest, sort_keys=True) + "\n").encode()
                elif case == "self_consistent_forged_manifest":
                    manifest["measured_code_head"] = "0" * 40
                    manifest_bytes = (json.dumps(manifest, sort_keys=True) + "\n").encode()
                elif case == "wrong_head":
                    lock = json.loads(lock_bytes)
                    lock["baseline_head"] = "0" * 40
                    lock_bytes = (json.dumps(lock, sort_keys=True) + "\n").encode()
                elif case == "baseline_equals_candidate":
                    current_head = manifest["measured_code_head"]
                elif case == "false_exact_storage":
                    manifest["queue_private_storage_accounting"] = "exact"
                    manifest_bytes = (
                        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
                    ).encode()
                    lock = json.loads(lock_bytes)
                    lock["queue_private_storage_accounting"] = "exact"
                    lock["manifest_sha256"] = sha256(manifest_bytes)
                    lock_bytes = (
                        json.dumps(lock, sort_keys=True, separators=(",", ":")) + "\n"
                    ).encode()
                baseline_error = case in {
                    "missing_lock_member",
                    "tampered_manifest",
                    "self_consistent_forged_manifest",
                    "wrong_head",
                    "baseline_equals_candidate",
                    "false_exact_storage",
                }
                if baseline_error:
                    with self.assertRaises(GateError):
                        validate_baseline_inputs(
                            REPOSITORY,
                            current_head,
                            lock_bytes,
                            manifest_bytes,
                        )
                    continue
                baseline = validate_baseline_inputs(
                    REPOSITORY,
                    current_head,
                    lock_bytes,
                    manifest_bytes,
                )
                if case == "wrong_harness":
                    current["entrypoint_sha256"] = "0" * 64
                elif case == "wrong_tool":
                    current["host_gate_python_sha256"] = "0" * 64
                elif case == "matrix_drift":
                    current["immutable_module_sha256"] = {"fixture": "0" * 64}
                elif case == "same_backend":
                    current["backend_sha256"] = manifest["backend_sha256"]
                if case == "success":
                    validate_candidate_current_bindings(baseline, current, "8" * 64)
                else:
                    with self.assertRaises(GateError):
                        validate_candidate_current_bindings(baseline, current, "8" * 64)

    def test_closed_cargo_policy_matrix(self) -> None:
        cases = (
            "success_exact_argv_env_and_eof_stdin",
            "nonzero_rejected",
            "empty_stdout_rejected",
            "oversized_stdout_rejected",
            "oversized_stderr_rejected",
            "sustained_stdout_terminated_at_cap",
            "timeout_rejected",
            "exited_parent_with_inherited_pipe_child_reaped",
            "missing_cargo_rejected",
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repository = root / "repository"
            repository.mkdir()
            home = root / "home"
            home.mkdir()
            for case in cases:
                with self.subTest(case=case):
                    binary = root / case / "bin"
                    binary.mkdir(parents=True)
                    record = root / case / "record"
                    environment = {
                        "PATH": str(binary),
                        "HOME": str(home),
                        "CAPTURE_BENCH_BUILD_COMMAND_SHA256": "a" * 64,
                        "CAPTURE_BENCH_BUILD_ENV_SHA256": "b" * 64,
                        "CAPTURE_BENCH_BUILD_POLICY": BUILD_ENVIRONMENT_POLICY,
                        "CAPTURE_BENCH_PROCESS_GROUP_POLICY": "inherit-outer-v1",
                        "FAKE_CARGO_RECORD": str(record),
                    }
                    cargo = binary / "cargo"
                    if case != "missing_cargo_rejected":
                        body = "printf '{}\\n'\n"
                        if case == "success_exact_argv_env_and_eof_stdin":
                            body = (
                                "if IFS= read -r line; then exit 71; fi\n"
                                "printf '%s\\n' \"$*\" > \"$FAKE_CARGO_RECORD\"\n"
                                "printf '%s|%s|%s\\n' \"$CAPTURE_BENCH_BUILD_POLICY\" "
                                "\"$CAPTURE_BENCH_BUILD_COMMAND_SHA256\" "
                                "\"$CAPTURE_BENCH_BUILD_ENV_SHA256\" >> \"$FAKE_CARGO_RECORD\"\n"
                                "printf '{}\\n'\n"
                            )
                        elif case == "nonzero_rejected":
                            body = "exit 19\n"
                        elif case == "empty_stdout_rejected":
                            body = "exit 0\n"
                        elif case == "oversized_stdout_rejected":
                            body = f'"{sys.executable}" -c \'print("x" * 65)\'\n'
                        elif case == "oversized_stderr_rejected":
                            body = f'"{sys.executable}" -c \'import sys; print("x" * 65, file=sys.stderr)\'\n'
                        elif case == "sustained_stdout_terminated_at_cap":
                            body = (
                                'printf \'%s\\n\' "$$" > "$FAKE_CARGO_RECORD"\n'
                                "while :; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; done\n"
                            )
                        elif case == "timeout_rejected":
                            body = (
                                "/bin/sleep 30 &\n"
                                'printf \'%s\\n\' "$!" > "$FAKE_CARGO_RECORD"\n'
                                "wait\n"
                            )
                        elif case == "exited_parent_with_inherited_pipe_child_reaped":
                            body = (
                                "/bin/sleep 30 &\n"
                                'printf \'%s\\n\' "$!" > "$FAKE_CARGO_RECORD"\n'
                                "exit 0\n"
                            )
                        cargo.write_text("#!/bin/sh\n" + body)
                        os.chmod(cargo, 0o700)
                    patches = []
                    if case in {
                        "oversized_stdout_rejected",
                        "sustained_stdout_terminated_at_cap",
                    }:
                        patches.append(
                            mock.patch(
                                "scripts.capture_benchmark_prepare_build_evidence.MAX_CARGO_JSON_BYTES",
                                32,
                            )
                        )
                    if case == "oversized_stderr_rejected":
                        patches.append(
                            mock.patch(
                                "scripts.capture_benchmark_prepare_build_evidence.MAX_CARGO_STDERR_BYTES",
                                32,
                            )
                        )
                    for patch in patches:
                        patch.start()
                    try:
                        cargo_digest = (
                            external_tool_hash(cargo) if cargo.exists() else "0" * 64
                        )
                        if case == "success_exact_argv_env_and_eof_stdin":
                            cargo_json, _tool, command_digest, environment_digest = (
                                run_closed_cargo_build(
                                    repository,
                                    environment,
                                    cargo,
                                    cargo_digest,
                                    timeout_seconds=2,
                                )
                            )
                            self.assertEqual(cargo_json, b"{}\n")
                            self.assertEqual(command_digest, "a" * 64)
                            self.assertEqual(environment_digest, "b" * 64)
                            lines = record.read_text().splitlines()
                            self.assertEqual(lines[0], " ".join(BUILD_COMMAND[1:]))
                            self.assertEqual(
                                lines[1], f"{BUILD_ENVIRONMENT_POLICY}|{'a' * 64}|{'b' * 64}"
                            )
                        else:
                            with self.assertRaises(GateError):
                                run_closed_cargo_build(
                                    repository,
                                    environment,
                                    cargo,
                                    cargo_digest,
                                    timeout_seconds=0.5
                                    if case
                                    in {
                                        "timeout_rejected",
                                        "exited_parent_with_inherited_pipe_child_reaped",
                                    }
                                    else 2,
                                )
                            if case in {
                                "sustained_stdout_terminated_at_cap",
                                "timeout_rejected",
                                "exited_parent_with_inherited_pipe_child_reaped",
                            }:
                                pid = int(record.read_text().strip())
                                deadline = time.monotonic() + 2
                                while time.monotonic() < deadline:
                                    try:
                                        os.kill(pid, 0)
                                    except ProcessLookupError:
                                        break
                                    time.sleep(0.01)
                                else:
                                    self.fail(f"bounded process left PID {pid} alive for {case}")
                    finally:
                        for patch in reversed(patches):
                            patch.stop()

    def test_cli_fixture_success_is_exact_and_no_clobber(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            command, environment, run = self._cli_fixture(Path(temporary), cargo_success=True)
            first = subprocess.run(command, env=environment, capture_output=True, text=True)
            self.assertEqual(first.returncode, 0, first.stderr)
            expected = {
                "build-evidence.json",
                "capture-bench-build.json",
                "capture_admission_evidence-exe",
            }
            self.assertEqual({path.name for path in run.iterdir()}, expected)
            evidence = json.loads((run / "build-evidence.json").read_text())
            self.assertTrue(evidence["test_fixture"])
            self.assertEqual(evidence["build_command"], list(BUILD_COMMAND))
            before = {path.name: path.read_bytes() for path in run.iterdir()}
            second = subprocess.run(command, env=environment, capture_output=True, text=True)
            self.assertNotEqual(second.returncode, 0)
            self.assertEqual({path.name: path.read_bytes() for path in run.iterdir()}, before)

    def test_cli_fixture_failed_cargo_publishes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            command, environment, run = self._cli_fixture(Path(temporary), cargo_success=False)
            result = subprocess.run(command, env=environment, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(list(run.iterdir()), [])

    def test_cli_fixture_rolls_back_every_validation_and_publication_boundary(self) -> None:
        cases = (
            "after-runner-validation",
            "after-current-validation",
            "after-cargo-publication",
            "after-executable-publication",
            "after-build-evidence-publication",
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for case in cases:
                with self.subTest(case=case):
                    command, environment, run = self._cli_fixture(
                        root / case,
                        cargo_success=True,
                        failure_injection=case,
                    )
                    sentinel = run / "sentinel"
                    sentinel.write_bytes(b"unchanged")
                    os.chmod(sentinel, 0o600)
                    before = {path.name: path.read_bytes() for path in run.iterdir()}
                    result = subprocess.run(
                        command, env=environment, capture_output=True, text=True
                    )
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(
                        {path.name: path.read_bytes() for path in run.iterdir()}, before
                    )

    def _cli_fixture(
        self,
        root: Path,
        *,
        cargo_success: bool,
        failure_injection: str | None = None,
    ) -> tuple[list[str], dict[str, str], Path]:
        root.mkdir(parents=True, exist_ok=True)
        repository, head = initialize_fixture_repository(root)
        controlled = (root / "controlled").resolve()
        controlled.mkdir(mode=0o700)
        run = controlled / "run"
        run.mkdir(mode=0o700)
        home = root / "home"
        home.mkdir()
        binary = root / "bin"
        binary.mkdir()
        environment = {"PATH": f"{binary}:/usr/bin:/bin", "HOME": str(home)}
        cargo = binary / "cargo"
        if cargo_success:
            expected = " ".join(BUILD_COMMAND[1:])
            cargo.write_text(
                "#!/bin/sh\n"
                + f'[ "$*" = \'{expected}\' ] || exit 81\n'
                + "if IFS= read -r line; then exit 85; fi\n"
                + "cat target/fixture-cargo-message\n"
            )
        else:
            cargo.write_text("#!/bin/sh\nexit 19\n")
        os.chmod(cargo, 0o700)
        cargo = Path(os.path.realpath(cargo))
        cargo_sha256 = external_tool_hash(cargo)
        git = Path(os.path.realpath(shutil.which("git", path=environment["PATH"]) or ""))
        git_sha256 = external_tool_hash(git)
        build_environment, command_digest, environment_digest = sanitized_build_environment(
            repository,
            environment,
            cargo,
            cargo_sha256,
            git,
            git_sha256,
        )
        value = binding_value()
        value.update(recompute_current_bindings(repository, cargo_sha256, git_sha256))
        value["measured_code_head"] = head
        value["build_command_sha256"] = command_digest
        value["build_environment_sha256"] = environment_digest
        artifact = repository / "target/fixture-runner"
        artifact.parent.mkdir()
        artifact.write_text(
            "#!/bin/sh\nprintf '%s\\n' '"
            + json.dumps(value, separators=(",", ":"))
            + "'\n"
        )
        os.chmod(artifact, 0o700)
        message = json.dumps(cargo_message(artifact), separators=(",", ":"))
        (repository / "target/fixture-cargo-message").write_text(message + "\n")
        self.assertEqual(
            build_environment["CAPTURE_BENCH_BUILD_COMMAND_SHA256"], command_digest
        )
        command = [
            sys.executable,
            str(REPOSITORY / "scripts/capture_benchmark_prepare_build_evidence.py"),
            "--run-dir",
            str(run),
            "--evidence-mode",
            "fixture",
            "--fixture-repository",
            str(repository),
            "--fixture-controlled-root",
            str(controlled),
        ]
        if failure_injection is not None:
            command.extend(("--fixture-failure-injection", failure_injection))
        return command, environment, run


if __name__ == "__main__":
    unittest.main()
