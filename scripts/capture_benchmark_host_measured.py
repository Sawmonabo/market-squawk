#!/usr/bin/env python3
"""Measured-input and immutable execution bindings for capture benchmarks."""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

if __package__:
    from .capture_benchmark_evidence_io import (
        CapabilityRoot,
        GateError,
        NOFOLLOW,
        digest_bytes,
        is_lower_digest,
        read_json_bytes,
        sync_directory,
    )
    from .capture_benchmark_host_schema import PROFILE, is_git_head
    from .capture_benchmark_process import bounded_process
else:
    from capture_benchmark_evidence_io import (
        CapabilityRoot,
        GateError,
        NOFOLLOW,
        digest_bytes,
        is_lower_digest,
        read_json_bytes,
        sync_directory,
    )
    from capture_benchmark_host_schema import PROFILE, is_git_head
    from capture_benchmark_process import bounded_process

MAX_BUILD_EVIDENCE_BYTES = 1024 * 1024
MAX_EXECUTABLE_BYTES = 256 * 1024 * 1024
RUNNER_SCHEMA_VERSION = 4
SCRIPT_DIRECTORY = Path(__file__).resolve().parent
AUTHORITY_MODULE_FILES = {
    "host_gate_shell_sha256": SCRIPT_DIRECTORY / "capture_benchmark_host_gate.sh",
    "host_gate_python_sha256": SCRIPT_DIRECTORY / "capture_benchmark_host_gate.py",
    "host_gate_process_sha256": SCRIPT_DIRECTORY / "capture_benchmark_process.py",
    "host_gate_evidence_io_sha256": SCRIPT_DIRECTORY / "capture_benchmark_evidence_io.py",
    "host_gate_cli_sha256": SCRIPT_DIRECTORY / "capture_benchmark_host_cli.py",
    "host_gate_schema_sha256": SCRIPT_DIRECTORY / "capture_benchmark_host_schema.py",
    "host_gate_execution_sha256": SCRIPT_DIRECTORY
    / "capture_benchmark_host_execution.py",
    "host_gate_observation_sha256": SCRIPT_DIRECTORY
    / "capture_benchmark_host_observation.py",
    "host_gate_measured_sha256": SCRIPT_DIRECTORY / "capture_benchmark_host_measured.py",
}
BUILD_COMMAND = [
    "cargo",
    "bench",
    "-p",
    "market-squawk-platform",
    "--bench",
    "capture_admission_evidence",
    "--all-features",
    "--locked",
    "--no-run",
    "--message-format=json-render-diagnostics",
]
BUILD_EVIDENCE_FIELDS = {
    "schema_version",
    "runner",
    "evidence_mode",
    "evidence_backend",
    "queue_transport",
    "queue_private_storage_accounting",
    "cargo_target",
    "benchmark_feature",
    "build_profile",
    "measured_code_head",
    "clean_build_enforced",
    "build_command",
    "build_environment_policy",
    "build_command_sha256",
    "build_environment_sha256",
    "cargo_executable_sha256",
    "git_executable_sha256",
    "rustc_executable_sha256",
    "git_tree_clean",
    "cargo_locked",
    "all_features",
    "release",
    "executable_path",
    "executable_sha256",
    "cargo_json_path",
    "cargo_json_sha256",
    "source_inventory_sha256",
    "cargo_lock_sha256",
    "workspace_manifest_sha256",
    "package_manifest_sha256",
    "build_script_sha256",
    "build_support_sha256",
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
    "baseline_lock_path",
    "baseline_lock_sha256",
    "baseline_manifest_path",
    "baseline_manifest_sha256",
    "baseline_measured_code_head",
}
RUNNER_BINDING_FIELDS = {
    "schema_version",
    "runner",
    "evidence_mode",
    "evidence_backend",
    "queue_transport",
    "queue_private_storage_accounting",
    "build_profile",
    "measured_code_head",
    "clean_build_enforced",
    "build_environment_policy",
    "build_command_sha256",
    "build_environment_sha256",
    "cargo_executable_sha256",
    "git_executable_sha256",
    "rustc_executable_sha256",
    "source_inventory_sha256",
    "cargo_lock_sha256",
    "workspace_manifest_sha256",
    "package_manifest_sha256",
    "build_script_sha256",
    "build_support_sha256",
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
    "baseline_lock_path",
    "baseline_lock_sha256",
    "baseline_manifest_path",
    "baseline_manifest_sha256",
    "baseline_measured_code_head",
    "immutable_module_sha256",
}
DIGEST_FIELDS = {
    "build_command_sha256",
    "build_environment_sha256",
    "cargo_executable_sha256",
    "git_executable_sha256",
    "rustc_executable_sha256",
    "executable_sha256",
    "cargo_json_sha256",
    "source_inventory_sha256",
    "cargo_lock_sha256",
    "workspace_manifest_sha256",
    "package_manifest_sha256",
    "build_script_sha256",
    "build_support_sha256",
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
    "selected_backend_source_sha256",
    "backend_sha256",
    "criterion_sha256",
    "observer_sha256",
}


@dataclass(frozen=True)
class MeasurementContract:
    """Closed launch contract parsed from descriptor-bound build evidence."""

    backend: str
    expected_fixtures: str
    measured_code_head: str
    executable_sha256: str
    cargo_executable_sha256: str
    git_executable_sha256: str
    rustc_executable_sha256: str
    baseline_manifest_sha256: str | None
    baseline_lock_sha256: str | None
    baseline_measured_code_head: str | None
    runner_bindings: dict[str, Any]
    authority_module_sha256: dict[str, str]

    def environment(
        self, run_directory: Path, build_evidence: Path, repetition: int
    ) -> dict[str, str]:
        if type(repetition) is not int or repetition not in range(1, 6):
            raise GateError("capture benchmark repetition is outside 1..=5")
        environment = {
            "CAPTURE_BENCH_BACKEND": self.backend,
            "CAPTURE_BENCH_REPETITION": str(repetition),
            "CAPTURE_BENCH_EXPECTED_FIXTURES": self.expected_fixtures,
            "CAPTURE_BENCH_OUTPUT": str(run_directory),
            "CAPTURE_BENCH_BUILD_EVIDENCE": str(build_evidence),
        }
        if self.backend == "candidate":
            environment.update(
                {
                    "CAPTURE_BENCH_BASELINE_MANIFEST": str(
                        run_directory / "baseline-manifest.json"
                    ),
                    "CAPTURE_BENCH_BASELINE_LOCK": str(run_directory / "baseline-lock.json"),
                }
            )
        return environment


def attestation_identity(root: CapabilityRoot, path: Path) -> dict[str, Any]:
    value, device, inode = root.read_file_identity(path, 128)
    if value != b"no-other-active-agents\n":
        raise GateError("active-agent attestation is absent or invalid")
    return {"device": device, "inode": inode, "sha256": digest_bytes(value)}


def measured_file_binding(root: CapabilityRoot, path: Path, maximum: int) -> dict[str, Any]:
    value, device, inode = root.read_file_identity(path, maximum)
    if not value:
        raise GateError("measured input is empty")
    return {
        "device": device,
        "inode": inode,
        "size": len(value),
        "sha256": digest_bytes(value),
    }


def require_measured_file_binding(
    root: CapabilityRoot,
    path: Path,
    maximum: int,
    expected: dict[str, Any],
    *,
    hash_contents: bool,
) -> None:
    if hash_contents:
        current = measured_file_binding(root, path, maximum)
    else:
        device, inode, size = root.stat_file_identity(path, maximum)
        current = {
            "device": device,
            "inode": inode,
            "size": size,
            "sha256": expected["sha256"],
        }
    if current != expected:
        raise GateError("measured input identity or digest changed during the interval")


def build_evidence_contract(
    root: CapabilityRoot, path: Path
) -> tuple[dict[str, Any], MeasurementContract]:
    encoded, device, inode = root.read_file_identity(path, MAX_BUILD_EVIDENCE_BYTES)
    binding = {
        "device": device,
        "inode": inode,
        "size": len(encoded),
        "sha256": digest_bytes(encoded),
    }
    value = read_json_bytes(encoded)
    if not isinstance(value, dict) or set(value) != BUILD_EVIDENCE_FIELDS:
        raise GateError("build evidence has an invalid launch schema")
    backend = value["evidence_backend"]
    baseline = (
        value["baseline_lock_path"],
        value["baseline_lock_sha256"],
        value["baseline_manifest_path"],
        value["baseline_manifest_sha256"],
        value["baseline_measured_code_head"],
    )
    baseline_valid = (
        backend == "standard" and all(item is None for item in baseline)
    ) or (
        backend == "candidate"
        and value["baseline_lock_path"] == "./baseline-lock.json"
        and is_lower_digest(value["baseline_lock_sha256"])
        and value["baseline_manifest_path"] == "./baseline-manifest.json"
        and is_lower_digest(value["baseline_manifest_sha256"])
        and is_git_head(value["baseline_measured_code_head"])
        and value["baseline_measured_code_head"] != value["measured_code_head"]
    )
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != RUNNER_SCHEMA_VERSION
        or value["runner"] != "capture_admission_evidence"
        or value["evidence_mode"] != "diagnostic_fixed_quota"
        or not isinstance(backend, str)
        or backend not in {"standard", "candidate"}
        or (
            value["queue_transport"],
            value["queue_private_storage_accounting"],
        )
        != {
            "standard": ("standard_sync_channel", "not_measured"),
            "candidate": ("candidate_fixed_ring", "exact"),
        }[backend]
        or value["cargo_target"] != "capture_admission_evidence"
        or value["benchmark_feature"] != "capture-benchmark"
        or value["build_profile"] != PROFILE
        or not is_git_head(value["measured_code_head"])
        or value["clean_build_enforced"] is not True
        or value["git_tree_clean"] is not True
        or value["cargo_locked"] is not True
        or value["all_features"] is not True
        or value["release"] is not True
        or value["build_environment_policy"] != "sanitized-cargo-bench-v2"
        or value["executable_path"] != "./capture_admission_evidence-exe"
        or value["cargo_json_path"] != "./capture-bench-build.json"
        or value["selected_backend_source_path"]
        != (
            "crates/market-squawk-platform/benches/capture_admission/backend/"
            f"{backend}.rs"
        )
        or value["build_command"] != BUILD_COMMAND
        or any(not is_lower_digest(value[field]) for field in DIGEST_FIELDS)
        or not isinstance(value["immutable_module_sha256"], dict)
        or not value["immutable_module_sha256"]
        or any(
            not isinstance(name, str)
            or not name
            or len(name.encode()) > 4096
            or not is_lower_digest(digest)
            for name, digest in value["immutable_module_sha256"].items()
        )
        or not baseline_valid
    ):
        raise GateError("build evidence launch contract is invalid")
    fixtures = (
        "matrix,comparable_full,sustained_rss"
        if backend == "standard"
        else "matrix,comparable_full,forced_lock,sustained_rss"
    )
    runner_bindings = {field: value[field] for field in RUNNER_BINDING_FIELDS}
    return binding, MeasurementContract(
        backend=backend,
        expected_fixtures=fixtures,
        measured_code_head=value["measured_code_head"],
        executable_sha256=value["executable_sha256"],
        cargo_executable_sha256=value["cargo_executable_sha256"],
        git_executable_sha256=value["git_executable_sha256"],
        rustc_executable_sha256=value["rustc_executable_sha256"],
        baseline_manifest_sha256=value["baseline_manifest_sha256"],
        baseline_lock_sha256=value["baseline_lock_sha256"],
        baseline_measured_code_head=value["baseline_measured_code_head"],
        runner_bindings=runner_bindings,
        authority_module_sha256={
            field: value[field] for field in AUTHORITY_MODULE_FILES
        },
    )


def verify_build_tool_identities(
    contract: MeasurementContract, stable_toolchain: dict[str, Any]
) -> None:
    identities = stable_toolchain.get("tool_identities")
    if not isinstance(identities, dict):
        raise GateError("host toolchain identities are absent")
    for name in ("cargo", "git", "rustc"):
        identity = identities.get(name)
        expected = getattr(contract, f"{name}_executable_sha256")
        if (
            not isinstance(identity, dict)
            or identity.get("state") != "available"
            or identity.get("sha256") != expected
        ):
            raise GateError("host toolchain differs from the build-bound executables")


def verify_authority_module_bindings(contract: MeasurementContract) -> None:
    """Require every executing host-gate module to match the build contract."""

    if set(contract.authority_module_sha256) != set(AUTHORITY_MODULE_FILES):
        raise GateError("host authority module binding inventory is incomplete")
    for field, path in AUTHORITY_MODULE_FILES.items():
        if _authority_file_digest(path) != contract.authority_module_sha256[field]:
            raise GateError("host authority module digest disagrees with build evidence")


def _authority_file_digest(path: Path) -> str:
    descriptor = os.open(path, os.O_RDONLY | NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or not 0 < before.st_size <= MAX_BUILD_EVIDENCE_BYTES
            or stat.S_IMODE(before.st_mode) & 0o022 != 0
        ):
            raise GateError("host authority module metadata is unsafe")
        contents = bytearray()
        while len(contents) < before.st_size:
            chunk = os.read(descriptor, min(1024 * 1024, before.st_size - len(contents)))
            if not chunk:
                raise GateError("host authority module ended before its declared size")
            contents.extend(chunk)
        if os.read(descriptor, 1):
            raise GateError("host authority module grew beyond its declared size")
        after = os.fstat(descriptor)
        current = os.stat(path, follow_symlinks=False)
        identity = (before.st_dev, before.st_ino, before.st_size, before.st_mode)
        if (
            (after.st_dev, after.st_ino, after.st_size, after.st_mode) != identity
            or (current.st_dev, current.st_ino, current.st_size, current.st_mode) != identity
        ):
            raise GateError("host authority module changed during its read")
        return digest_bytes(bytes(contents))
    finally:
        os.close(descriptor)


def create_execution_runner(
    root: CapabilityRoot,
    run_directory: Path,
    runner: Path,
    expected: dict[str, Any],
) -> tuple[Path, dict[str, Any]]:
    runner_bytes, device, inode = root.read_file_identity(runner, MAX_EXECUTABLE_BYTES)
    if {
        "device": device,
        "inode": inode,
        "size": len(runner_bytes),
        "sha256": digest_bytes(runner_bytes),
    } != expected:
        raise GateError("runner changed before immutable execution copy creation")
    directory = root.open_directory(root.relative(run_directory))
    name = f".capture-execution-{os.urandom(16).hex()}"
    descriptor = -1
    try:
        descriptor = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | NOFOLLOW,
            0o500,
            dir_fd=directory,
        )
        written = 0
        while written < len(runner_bytes):
            count = os.write(descriptor, runner_bytes[written:])
            if count <= 0:
                raise GateError("execution runner copy write made no progress")
            written += count
        os.fchmod(descriptor, 0o500)
        os.fsync(descriptor)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o500
            or metadata.st_size != len(runner_bytes)
        ):
            raise GateError("execution runner copy metadata is invalid")
        os.close(descriptor)
        descriptor = -1
        sync_directory(directory)
        path = run_directory / name
        binding = measured_file_binding(root, path, MAX_EXECUTABLE_BYTES)
        if binding["sha256"] != expected["sha256"]:
            raise GateError("execution runner copy digest differs from the bound runner")
        return path, binding
    except Exception:
        try:
            os.unlink(name, dir_fd=directory)
            sync_directory(directory)
        except OSError:
            pass
        raise
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(directory)


def remove_execution_runner(
    root: CapabilityRoot, path: Path, expected: dict[str, Any]
) -> None:
    parts = root.relative(path)
    parent = root.open_directory(parts[:-1])
    try:
        current = os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)
        if (
            not stat.S_ISREG(current.st_mode)
            or current.st_nlink != 1
            or stat.S_IMODE(current.st_mode) != 0o500
            or (current.st_dev, current.st_ino, current.st_size)
            != (expected["device"], expected["inode"], expected["size"])
        ):
            raise GateError("execution runner changed before cleanup")
        os.unlink(parts[-1], dir_fd=parent)
        sync_directory(parent)
    finally:
        os.close(parent)


def verify_execution_contract(
    execution_runner: Path,
    run_directory: Path,
    contract: MeasurementContract,
) -> None:
    completed = bounded_process(
        [str(execution_runner), "--print-build-bindings"],
        cwd=run_directory,
        env={},
        timeout_seconds=10,
        maximum_stdout=MAX_BUILD_EVIDENCE_BYTES,
        maximum_stderr=MAX_BUILD_EVIDENCE_BYTES,
    )
    if completed.returncode != 0 or not completed.stdout:
        raise GateError("execution runner rejected its build-binding query")
    bindings = read_json_bytes(completed.stdout)
    if not isinstance(bindings, dict) or set(bindings) != RUNNER_BINDING_FIELDS:
        raise GateError("execution runner build-binding schema is invalid")
    if bindings != contract.runner_bindings:
        raise GateError("execution runner disagrees with descriptor-bound build evidence")
