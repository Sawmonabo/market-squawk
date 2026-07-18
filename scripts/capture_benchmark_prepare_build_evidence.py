#!/usr/bin/env python3
"""Create no-clobber build evidence for the diagnostic capture runner."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from capture_benchmark_host_gate import (  # noqa: E402
    CapabilityRoot,
    bounded_process,
    bounded_session_leader_exec,
    GateError,
    NOFOLLOW,
    canonical_json,
    production_root,
    publish_bytes,
    publish_json,
    require_platform_primitives,
)
from capture_benchmark_host_measured import RUNNER_SCHEMA_VERSION  # noqa: E402

MAX_BINDINGS_BYTES = 1024 * 1024
MAX_CARGO_JSON_BYTES = 64 * 1024 * 1024
MAX_CARGO_STDERR_BYTES = 16 * 1024 * 1024
MAX_EXECUTABLE_BYTES = 256 * 1024 * 1024
TARGET = "capture_admission_evidence"
FEATURE = "capture-benchmark"
EXPECTED_CARGO_FEATURES = (FEATURE, "capture-test", "default")
RESULT_SCHEMA_VERSION = RUNNER_SCHEMA_VERSION
EVIDENCE_MODE = "diagnostic_fixed_quota"
PROFILE = (
    "cargo-bench-inherits-release:opt-level=3:lto=thin:codegen-units=1:"
    "panic=abort:strip=symbols"
)
BUILD_ENVIRONMENT_POLICY = "sanitized-cargo-bench-v1"
BUILD_COMMAND = (
    "cargo",
    "bench",
    "-p",
    "market-squawk-platform",
    "--bench",
    TARGET,
    "--all-features",
    "--locked",
    "--no-run",
    "--message-format=json-render-diagnostics",
)
PASSTHROUGH_ENVIRONMENT = (
    "PATH",
    "HOME",
    "CARGO_HOME",
    "RUSTUP_HOME",
    "TMPDIR",
    "TMP",
    "TEMP",
    "SDKROOT",
    "MACOSX_DEPLOYMENT_TARGET",
    "DEVELOPER_DIR",
)
IMMUTABLE_MODULES = {
    "benchmark_identity": "benchmark_identity.rs",
    "collector": "collector.rs",
    "endpoints": "endpoints.rs",
    "evidence_io": "evidence_io.rs",
    "fixture": "fixture.rs",
    "producer_inventory": "producer_inventory.rs",
    "schema": "schema.rs",
    "workload": "workload.rs",
}
BASELINE_LOCK_RELATIVE = Path(
    "docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.lock.json"
)
MAX_BASELINE_BYTES = 4 * 1024 * 1024
BACKEND_DIGEST_DOMAIN = b"market-squawk:capture-benchmark-backend:v1\0"
BACKEND_DISPATCHER_RELATIVE = Path("benches/capture_admission/backend.rs")
BACKEND_SOURCE_RELATIVE = {
    "standard": Path("benches/capture_admission/backend/standard.rs"),
    "candidate": Path("benches/capture_admission/backend/candidate.rs"),
}


def strict_json(value: bytes) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise GateError("baseline JSON contains a duplicate member")
            result[key] = item
        return result

    try:
        parsed = json.loads(value, object_pairs_hook=unique)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise GateError("baseline JSON is malformed") from error
    if not isinstance(parsed, dict):
        raise GateError("baseline JSON root is not an object")
    return parsed


def command(program: Path, arguments: list[str], repository: Path) -> str:
    completed = bounded_process(
        [str(program), *arguments],
        cwd=repository,
        env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        timeout_seconds=10,
        maximum_stdout=MAX_BINDINGS_BYTES,
        maximum_stderr=MAX_BINDINGS_BYTES,
    )
    if completed.returncode != 0:
        raise GateError("build-evidence local command failed")
    return completed.stdout.decode("utf-8", "strict").strip()


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def cargo_configuration_paths(repository: Path, environment: dict[str, str]) -> list[Path]:
    candidates = []
    current = repository
    while True:
        candidates.extend((current / ".cargo/config", current / ".cargo/config.toml"))
        if current.parent == current:
            break
        current = current.parent
    cargo_home = environment.get("CARGO_HOME")
    if cargo_home:
        home = Path(cargo_home)
    elif environment.get("HOME"):
        home = Path(environment["HOME"]) / ".cargo"
    else:
        raise GateError("closed Cargo build requires a bound Cargo home")
    candidates.extend((home / "config", home / "config.toml"))
    return candidates


def sanitized_build_environment(
    repository: Path,
    environment: dict[str, str],
    cargo_executable: Path,
    cargo_sha256: str,
    git_executable: Path,
    git_sha256: str,
    evidence_backend: str = "standard",
    baseline_lock_sha256: str | None = None,
    baseline_manifest_sha256: str | None = None,
    baseline_measured_code_head: str | None = None,
) -> tuple[dict[str, str], str, str]:
    forbidden = [
        key
        for key in environment
        if key
        in {
            "RUSTFLAGS",
            "RUSTDOCFLAGS",
            "RUSTC",
            "RUSTC_WRAPPER",
            "RUSTC_WORKSPACE_WRAPPER",
            "CC",
            "CXX",
            "AR",
        }
        or key.startswith(("CC_", "CXX_", "AR_"))
        or key.startswith("CARGO_PROFILE_")
        or key.startswith("CARGO_TARGET_")
        or key.startswith("CARGO_BUILD_")
        or key == "CARGO_ENCODED_RUSTFLAGS"
        or key == "LD_PRELOAD"
        or key.startswith("DYLD_")
    ]
    if forbidden:
        raise GateError(
            "closed Cargo build forbids compiler, profile, target, or loader overrides"
        )
    configs = [path for path in cargo_configuration_paths(repository, environment) if path.exists()]
    if configs:
        raise GateError("closed Cargo build forbids discovered configuration overrides")
    selected = {
        key: environment[key]
        for key in PASSTHROUGH_ENVIRONMENT
        if key in environment and environment[key]
    }
    if "PATH" not in selected or "HOME" not in selected:
        raise GateError("closed Cargo build requires bounded PATH and HOME inputs")
    path_entries = selected["PATH"].split(os.pathsep)
    if not path_entries or any(not Path(entry).is_absolute() for entry in path_entries):
        raise GateError("closed Cargo build requires only absolute nonempty PATH entries")
    if (
        not cargo_executable.is_absolute()
        or not git_executable.is_absolute()
        or Path(os.path.realpath(cargo_executable)) != cargo_executable
        or Path(os.path.realpath(git_executable)) != git_executable
        or not is_lower_digest(cargo_sha256)
        or not is_lower_digest(git_sha256)
        or external_tool_hash(cargo_executable) != cargo_sha256
        or external_tool_hash(git_executable) != git_sha256
    ):
        raise GateError("closed Cargo build tool bindings are invalid")
    if evidence_backend == "standard":
        if any(
            value is not None
            for value in (
                baseline_lock_sha256,
                baseline_manifest_sha256,
                baseline_measured_code_head,
            )
        ):
            raise GateError("standard build forbids candidate baseline bindings")
    elif evidence_backend == "candidate":
        if (
            not is_lower_digest(baseline_lock_sha256)
            or not is_lower_digest(baseline_manifest_sha256)
            or not is_git_head(baseline_measured_code_head)
        ):
            raise GateError("candidate build requires canonical tracked baseline bindings")
    else:
        raise GateError("build evidence backend is not a closed identity")
    command_sha256 = sha256(canonical_json(list(BUILD_COMMAND)))
    environment_binding = {
        "policy": BUILD_ENVIRONMENT_POLICY,
        "passthrough": selected,
        "fixed": {
            "CAPTURE_BENCH_REQUIRE_CLEAN_BUILD": "1",
            "CAPTURE_BENCH_BUILD_POLICY": BUILD_ENVIRONMENT_POLICY,
            "CAPTURE_BENCH_BUILD_COMMAND_SHA256": command_sha256,
            "CARGO_TERM_COLOR": "never",
            "CAPTURE_BENCH_EVIDENCE_BACKEND": evidence_backend,
            "CAPTURE_BENCH_CARGO_EXECUTABLE": str(cargo_executable),
            "CAPTURE_BENCH_CARGO_EXECUTABLE_SHA256": cargo_sha256,
            "CAPTURE_BENCH_GIT_EXECUTABLE": str(git_executable),
            "CAPTURE_BENCH_GIT_EXECUTABLE_SHA256": git_sha256,
            "CAPTURE_BENCH_PROCESS_GROUP_POLICY": "inherit-outer-v1",
        },
    }
    if evidence_backend == "candidate":
        environment_binding["fixed"].update(
            {
                "CAPTURE_BENCH_BASELINE_LOCK_SHA256": baseline_lock_sha256,
                "CAPTURE_BENCH_BASELINE_MANIFEST_SHA256": baseline_manifest_sha256,
                "CAPTURE_BENCH_BASELINE_MEASURED_CODE_HEAD": baseline_measured_code_head,
            }
        )
    environment_sha256 = sha256(canonical_json(environment_binding))
    selected.update(environment_binding["fixed"])
    selected["CAPTURE_BENCH_BUILD_ENV_SHA256"] = environment_sha256
    return selected, command_sha256, environment_sha256


def validate_current_bindings(bindings: dict[str, Any], current: dict[str, Any]) -> None:
    if set(current) != {
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
    } or any(bindings.get(field) != value for field, value in current.items()):
        raise GateError("embedded build binding differs from the current clean tree")


def resolve_external_tool(name: str, environment: dict[str, str]) -> tuple[Path, str]:
    path = shutil.which(name, path=environment.get("PATH"))
    if path is None:
        raise GateError(f"closed build could not resolve required {name} executable")
    discovered = Path(path)
    canonical = Path(os.path.realpath(discovered))
    if not canonical.is_absolute():
        raise GateError("closed build tool did not resolve to an absolute path")
    rustup_path = shutil.which("rustup", path=environment.get("PATH"))
    rustup = Path(os.path.realpath(rustup_path)) if rustup_path is not None else None
    rustup_proxy = canonical.name in {"rustup", "rustup.exe"}
    if rustup is not None:
        try:
            rustup_proxy = rustup_proxy or os.path.samefile(discovered, rustup_path)
        except OSError as error:
            raise GateError("closed build could not bind the rustup proxy identity") from error
    if rustup_proxy:
        if name not in {"cargo", "rustc"}:
            raise GateError("closed build found an unexpected rustup proxy")
        rustup = rustup or canonical
        external_tool_hash(rustup)
        resolver_environment = {
            key: environment[key]
            for key in ("PATH", "HOME", "RUSTUP_HOME")
            if key in environment and environment[key]
        }
        resolved = bounded_process(
            [str(rustup), "which", name],
            cwd=Path(__file__).resolve().parents[1],
            env=resolver_environment,
            timeout_seconds=10,
            maximum_stdout=4096,
            maximum_stderr=4096,
        )
        if resolved.returncode != 0:
            raise GateError("rustup could not resolve the pinned direct build tool")
        try:
            resolved_path = resolved.stdout.decode("utf-8", "strict").strip()
        except UnicodeDecodeError as error:
            raise GateError("rustup returned a non-UTF-8 build tool path") from error
        if not resolved_path or "\n" in resolved_path or "\r" in resolved_path:
            raise GateError("rustup returned an ambiguous build tool path")
        canonical = Path(os.path.realpath(resolved_path))
        expected_names = {name, f"{name}.exe"}
        if not canonical.is_absolute() or canonical.name not in expected_names:
            raise GateError("rustup did not resolve the requested direct build tool")
    return canonical, external_tool_hash(canonical)


def run_closed_cargo_build(
    repository: Path,
    environment: dict[str, str],
    cargo_path: Path,
    cargo_sha256: str,
    timeout_seconds: float = 1800,
    wrapper_python: Path | None = None,
) -> tuple[bytes, str, str, str]:
    try:
        if external_tool_hash(cargo_path) != cargo_sha256:
            raise GateError("closed Cargo executable changed before invocation")
        completed = bounded_session_leader_exec(
            [str(cargo_path), *BUILD_COMMAND[1:]],
            executable=cargo_path,
            wrapper_python=(wrapper_python or Path(sys.executable).resolve()),
            cwd=repository,
            env=environment,
            timeout_seconds=timeout_seconds,
            maximum_stdout=MAX_CARGO_JSON_BYTES,
            maximum_stderr=MAX_CARGO_STDERR_BYTES,
        )
    except (OSError, GateError) as error:
        raise GateError("closed Cargo build failed to execute within its bounds") from error
    if completed.returncode != 0 or not completed.stdout:
        raise GateError("closed Cargo build failed or emitted no Cargo JSON")
    cargo_json = completed.stdout
    return cargo_json, cargo_sha256, environment["CAPTURE_BENCH_BUILD_COMMAND_SHA256"], environment[
        "CAPTURE_BENCH_BUILD_ENV_SHA256"
    ]


def external_tool_hash(path: Path) -> str:
    descriptor = os.open(path, os.O_RDONLY | NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > 64 * 1024 * 1024:
            raise GateError("build tool is not a bounded regular file")
        digest = hashlib.sha256()
        observed = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
            observed += len(chunk)
        after = os.fstat(descriptor)
        current = os.stat(path, follow_symlinks=False)
        if (
            observed != before.st_size
            or (before.st_dev, before.st_ino, before.st_size)
            != (after.st_dev, after.st_ino, after.st_size)
            or (before.st_dev, before.st_ino, before.st_size)
            != (current.st_dev, current.st_ino, current.st_size)
        ):
            raise GateError("build tool changed during descriptor hashing")
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def current_file_hash(path: Path, repository: Path) -> str:
    require_platform_primitives()
    if Path(os.path.realpath(path)) != path or repository not in path.parents:
        raise GateError("current binding path is not a canonical repository file")
    descriptor = os.open(path, os.O_RDONLY | NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size > 4 * 1024 * 1024
        ):
            raise GateError("current binding is not a bounded unambiguous regular file")
        digest = hashlib.sha256()
        observed = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
            observed += len(chunk)
        after = os.fstat(descriptor)
        current = os.stat(path, follow_symlinks=False)
        if (
            observed != before.st_size
            or (before.st_dev, before.st_ino, before.st_size)
            != (after.st_dev, after.st_ino, after.st_size)
            or (before.st_dev, before.st_ino, before.st_size)
            != (current.st_dev, current.st_ino, current.st_size)
        ):
            raise GateError("current binding changed during descriptor hashing")
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def benchmark_backend_binding(repository: Path, backend: str) -> dict[str, str]:
    """Return the closed, descriptor-checked binding for one selected backend source."""
    if backend not in BACKEND_SOURCE_RELATIVE:
        raise GateError("capture benchmark backend is not a closed identity")
    package = repository / "crates/market-squawk-platform"
    dispatcher = package / BACKEND_DISPATCHER_RELATIVE
    sources = {
        identity: current_file_hash(package / relative, repository)
        for identity, relative in BACKEND_SOURCE_RELATIVE.items()
    }
    if sources["standard"] == sources["candidate"]:
        raise GateError("capture benchmark backend sources are byte-identical")
    dispatcher_sha256 = current_file_hash(dispatcher, repository)
    selected_relative = BACKEND_SOURCE_RELATIVE[backend]
    selected_sha256 = sources[backend]
    digest = hashlib.sha256(BACKEND_DIGEST_DOMAIN)
    for label, value in (
        (str(BACKEND_DISPATCHER_RELATIVE).encode(), dispatcher_sha256.encode()),
        (str(selected_relative).encode(), selected_sha256.encode()),
        (b"identity", backend.encode()),
    ):
        digest.update(len(label).to_bytes(8, "little"))
        digest.update(label)
        digest.update(len(value).to_bytes(8, "little"))
        digest.update(value)
    return {
        "backend_dispatcher_sha256": dispatcher_sha256,
        "selected_backend_source_path": str(
            (package / selected_relative).relative_to(repository)
        ),
        "selected_backend_source_sha256": selected_sha256,
        "backend_sha256": digest.hexdigest(),
    }


def rust_files(root: Path) -> list[Path]:
    files = []
    for directory, names, filenames in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        for name in names:
            if (directory_path / name).is_symlink():
                raise GateError("source inventory contains a symlinked directory")
        for name in filenames:
            path = directory_path / name
            if path.suffix == ".rs":
                if path.is_symlink():
                    raise GateError("source inventory contains a symlinked file")
                files.append(path)
    return sorted(files)


def tree_hash(repository: Path, files: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in files:
        digest.update(str(path.relative_to(repository)).encode())
        digest.update(b"\0")
        digest.update(current_file_hash(path, repository).encode())
        digest.update(b"\0")
    return digest.hexdigest()


def recompute_current_bindings(
    repository: Path,
    cargo_sha256: str,
    git_sha256: str,
    evidence_backend: str = "standard",
) -> dict[str, Any]:
    platform = rust_files(repository / "crates/market-squawk-platform/src")
    domain = rust_files(repository / "crates/market-squawk-domain/src")
    inventory = hashlib.sha256()
    for path in sorted(platform + domain):
        inventory.update(str(path.relative_to(repository)).encode())
        inventory.update(b"\0")
    bench = repository / "crates/market-squawk-platform/benches/capture_admission"
    immutable = {
        name: current_file_hash(bench / filename, repository)
        for name, filename in IMMUTABLE_MODULES.items()
    }
    backend_binding = benchmark_backend_binding(repository, evidence_backend)
    return {
        "source_inventory_sha256": inventory.hexdigest(),
        "cargo_lock_sha256": current_file_hash(repository / "Cargo.lock", repository),
        "workspace_manifest_sha256": current_file_hash(repository / "Cargo.toml", repository),
        "package_manifest_sha256": current_file_hash(
            repository / "crates/market-squawk-platform/Cargo.toml", repository
        ),
        "build_script_sha256": current_file_hash(
            repository / "crates/market-squawk-platform/build.rs", repository
        ),
        "build_support_sha256": current_file_hash(
            repository / "crates/market-squawk-platform/build_support.rs", repository
        ),
        "cargo_executable_sha256": cargo_sha256,
        "git_executable_sha256": git_sha256,
        "host_gate_shell_sha256": current_file_hash(
            repository / "scripts/capture_benchmark_host_gate.sh", repository
        ),
        "host_gate_python_sha256": current_file_hash(
            repository / "scripts/capture_benchmark_host_gate.py", repository
        ),
        "host_gate_process_sha256": current_file_hash(
            repository / "scripts/capture_benchmark_process.py", repository
        ),
        "host_gate_evidence_io_sha256": current_file_hash(
            repository / "scripts/capture_benchmark_evidence_io.py", repository
        ),
        "host_gate_cli_sha256": current_file_hash(
            repository / "scripts/capture_benchmark_host_cli.py", repository
        ),
        "host_gate_schema_sha256": current_file_hash(
            repository / "scripts/capture_benchmark_host_schema.py", repository
        ),
        "host_gate_execution_sha256": current_file_hash(
            repository / "scripts/capture_benchmark_host_execution.py", repository
        ),
        "host_gate_observation_sha256": current_file_hash(
            repository / "scripts/capture_benchmark_host_observation.py", repository
        ),
        "host_gate_measured_sha256": current_file_hash(
            repository / "scripts/capture_benchmark_host_measured.py", repository
        ),
        "build_evidence_python_sha256": current_file_hash(
            repository / "scripts/capture_benchmark_prepare_build_evidence.py", repository
        ),
        "platform_source_sha256": tree_hash(repository, platform),
        "domain_source_sha256": tree_hash(repository, domain),
        "entrypoint_sha256": current_file_hash(
            repository / "crates/market-squawk-platform/benches/capture_admission.rs",
            repository,
        ),
        **backend_binding,
        "criterion_sha256": current_file_hash(
            repository / "crates/market-squawk-platform/benches/capture_admission_criterion.rs",
            repository,
        ),
        "observer_sha256": current_file_hash(
            repository
            / "crates/market-squawk-platform/src/capture/benchmark_support/observer.rs",
            repository,
        ),
        "immutable_module_sha256": immutable,
    }


def artifact_bytes(path: Path, repository: Path) -> bytes:
    require_platform_primitives()
    canonical = Path(os.path.realpath(path))
    if canonical != path or repository / "target" not in canonical.parents:
        raise GateError("Cargo executable path is outside the canonical target directory")
    descriptor = os.open(path, os.O_RDONLY | NOFOLLOW)
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_size > MAX_EXECUTABLE_BYTES
    ):
        os.close(descriptor)
        raise GateError("Cargo executable is not a bounded unambiguous regular file")
    try:
        chunks = []
        observed = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
            observed += len(chunk)
        after = os.fstat(descriptor)
        current = os.stat(path, follow_symlinks=False)
        if (
            observed != metadata.st_size
            or (after.st_dev, after.st_ino, after.st_size)
            != (metadata.st_dev, metadata.st_ino, metadata.st_size)
            or (current.st_dev, current.st_ino, current.st_size)
            != (metadata.st_dev, metadata.st_ino, metadata.st_size)
        ):
            raise GateError("Cargo executable changed during descriptor hashing")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def artifact_hash(path: Path, repository: Path) -> str:
    return sha256(artifact_bytes(path, repository))


def parse_cargo_artifact(cargo_json: bytes, repository: Path) -> tuple[Path, dict[str, Any]]:
    matches = []
    for line in cargo_json.splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise GateError("Cargo JSON stream is malformed") from error
        if (
            isinstance(value, dict)
            and value.get("reason") == "compiler-artifact"
            and isinstance(value.get("target"), dict)
            and value["target"].get("name") == TARGET
            and value.get("executable") is not None
        ):
            matches.append(value)
    if len(matches) != 1:
        raise GateError("Cargo JSON does not contain exactly one evidence-runner artifact")
    artifact = matches[0]
    target = artifact["target"]
    profile = artifact.get("profile")
    features = artifact.get("features")
    if (
        target.get("kind") != ["bench"]
        or not isinstance(profile, dict)
        or str(profile.get("opt_level")) != "3"
        or profile.get("debug_assertions") is not False
        or not isinstance(features, list)
        or features != list(EXPECTED_CARGO_FEATURES)
    ):
        raise GateError("Cargo artifact profile, kind, or feature set is invalid")
    path = Path(artifact["executable"])
    if not path.is_absolute():
        path = repository / path
    return path, artifact


def runner_bindings(executable: Path) -> dict[str, Any]:
    completed = bounded_process(
        [str(executable), "--print-build-bindings"],
        env={},
        timeout_seconds=10,
        maximum_stdout=MAX_BINDINGS_BYTES,
        maximum_stderr=0,
    )
    if completed.returncode != 0:
        raise GateError("copied evidence runner did not emit bounded build bindings")
    value = strict_json(completed.stdout)
    expected = {
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
        "baseline_lock_path",
        "baseline_lock_sha256",
        "baseline_manifest_path",
        "baseline_manifest_sha256",
        "baseline_measured_code_head",
        "immutable_module_sha256",
    }
    if (
        not isinstance(value, dict)
        or set(value) != expected
        or value["schema_version"] != RESULT_SCHEMA_VERSION
        or value["runner"] != TARGET
        or value["evidence_mode"] != EVIDENCE_MODE
        or value["evidence_backend"] not in {"standard", "candidate"}
        or (
            value["queue_transport"],
            value["queue_private_storage_accounting"],
        )
        != {
            "standard": ("standard_sync_channel", "not_measured"),
            "candidate": ("candidate_fixed_ring", "exact"),
        }[value["evidence_backend"]]
        or value["build_profile"] != PROFILE
        or not is_git_head(value["measured_code_head"])
        or value["clean_build_enforced"] is not True
        or value["build_environment_policy"] != BUILD_ENVIRONMENT_POLICY
        or not is_lower_digest(value["build_command_sha256"])
        or not is_lower_digest(value["build_environment_sha256"])
    ):
        raise GateError("runner build binding schema or authority identity is invalid")
    digest_fields = expected - {
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
        "selected_backend_source_path",
        "immutable_module_sha256",
        "baseline_lock_path",
        "baseline_lock_sha256",
        "baseline_manifest_path",
        "baseline_manifest_sha256",
        "baseline_measured_code_head",
    }
    for field in digest_fields:
        if not is_lower_digest(value[field]):
            raise GateError("runner build binding contains a noncanonical digest")
    selected_source = (
        "crates/market-squawk-platform/benches/capture_admission/backend/"
        f"{value['evidence_backend']}.rs"
    )
    if value["selected_backend_source_path"] != selected_source:
        raise GateError("runner selected backend source path is inconsistent")
    if not isinstance(value["immutable_module_sha256"], dict) or any(
        not is_lower_digest(digest) for digest in value["immutable_module_sha256"].values()
    ):
        raise GateError("runner immutable module binding is invalid")
    baseline = (
        value["baseline_lock_path"],
        value["baseline_lock_sha256"],
        value["baseline_manifest_path"],
        value["baseline_manifest_sha256"],
        value["baseline_measured_code_head"],
    )
    if value["evidence_backend"] == "standard":
        if any(item is not None for item in baseline):
            raise GateError("standard runner contains candidate baseline bindings")
    elif (
        value["baseline_lock_path"] != "./baseline-lock.json"
        or not is_lower_digest(value["baseline_lock_sha256"])
        or value["baseline_manifest_path"] != "./baseline-manifest.json"
        or not is_lower_digest(value["baseline_manifest_sha256"])
        or not is_git_head(value["baseline_measured_code_head"])
    ):
        raise GateError("candidate runner baseline bindings are malformed")
    return value


def is_lower_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def is_git_head(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 40
        and all(character in "0123456789abcdef" for character in value)
    )


def repository_file_bytes(path: Path, repository: Path, maximum: int) -> bytes:
    require_platform_primitives()
    if Path(os.path.realpath(path)) != path or repository not in path.parents:
        raise GateError("tracked baseline lock is not a canonical repository file")
    descriptor = os.open(path, os.O_RDONLY | NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size <= 0
            or before.st_size > maximum
        ):
            raise GateError("tracked baseline lock is not a bounded regular file")
        chunks = []
        observed = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
            observed += len(chunk)
        after = os.fstat(descriptor)
        current = os.stat(path, follow_symlinks=False)
        identity = (before.st_dev, before.st_ino, before.st_size)
        if (
            observed != before.st_size
            or (after.st_dev, after.st_ino, after.st_size) != identity
            or (current.st_dev, current.st_ino, current.st_size) != identity
            or current.st_nlink != 1
        ):
            raise GateError("tracked baseline lock changed during read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def validate_baseline_inputs(
    repository: Path,
    current_head: str,
    lock_bytes: bytes,
    manifest_bytes: bytes,
) -> dict[str, Any]:
    lock = strict_json(lock_bytes)
    manifest = strict_json(manifest_bytes)
    lock_fields = {
        "schema_version",
        "state",
        "baseline_head",
        "manifest_sha256",
        "manifest_reference",
        "report_reference",
        "report_sha256",
        "approval_state",
        "approval_identity",
        "backend",
        "queue_transport",
        "queue_private_storage_accounting",
        "backend_sha256",
        "build_evidence_sha256",
        "immutable_module_sha256",
        "entrypoint_sha256",
        "criterion_sha256",
        "observer_sha256",
        "tool_sha256",
        "artifact_sha256",
        "repetition_sha256",
        "host_fingerprint_sha256",
        "toolchain_fingerprint_sha256",
        "release_profile_sha256",
    }
    manifest_fields = {
        "schema_version",
        "runner",
        "evidence_mode",
        "criterion_evidence_mode",
        "measured_code_head",
        "build_evidence_sha256",
        "baseline_manifest_sha256",
        "baseline_lock_sha256",
        "build_environment_policy",
        "build_command_sha256",
        "build_environment_sha256",
        "cargo_executable_sha256",
        "git_executable_sha256",
        "cargo_json_sha256",
        "source_inventory_sha256",
        "cargo_lock_sha256",
        "criterion_sha256",
        "observer_sha256",
        "backend",
        "queue_transport",
        "queue_private_storage_accounting",
        "benchmark_support_feature",
        "fixtures",
        "repetitions",
        "executable_path",
        "executable_sha256",
        "immutable_module_sha256",
        "entrypoint_sha256",
        "backend_sha256",
        "production_library_sha256",
        "repetition_sha256",
        "artifact_sha256",
        "tool_sha256",
        "host_fingerprint_sha256",
        "toolchain_fingerprint_sha256",
        "release_profile_sha256",
        "host_gate",
    }
    manifest_digest = sha256(manifest_bytes)
    lock_digest = sha256(lock_bytes)
    baseline_head = manifest.get("measured_code_head")
    expected_reference = (
        f"target/q2-a4-capture-benchmark/standard-{baseline_head}/manifest.json"
    )
    if (
        set(lock) != lock_fields
        or set(manifest) != manifest_fields
        or lock.get("schema_version") != RESULT_SCHEMA_VERSION
        or lock.get("state") != "frozen_standard_baseline"
        or manifest.get("schema_version") != RESULT_SCHEMA_VERSION
        or manifest.get("runner") != TARGET
        or manifest.get("evidence_mode") != EVIDENCE_MODE
        or manifest.get("criterion_evidence_mode") != "exploratory_zero_authority"
        or manifest.get("backend") != "standard"
        or manifest.get("queue_transport") != "standard_sync_channel"
        or manifest.get("queue_private_storage_accounting") != "not_measured"
        or manifest.get("benchmark_support_feature") != FEATURE
        or manifest.get("fixtures") != ["matrix", "comparable_full", "sustained_rss"]
        or manifest.get("repetitions") != [1, 2, 3, 4, 5]
        or manifest.get("baseline_manifest_sha256") is not None
        or manifest.get("baseline_lock_sha256") is not None
        or not is_git_head(baseline_head)
        or baseline_head == current_head
        or lock.get("baseline_head") != baseline_head
        or lock.get("manifest_sha256") != manifest_digest
        or lock.get("manifest_reference") != expected_reference
        or lock.get("report_reference")
        != "docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md"
        or not is_lower_digest(lock.get("report_sha256"))
        or lock.get("approval_state") != "independent_seed_review_approved"
        or lock.get("approval_identity") != "q2-a4-seed-checkpoint-review"
        or lock.get("backend") != "standard"
        or lock.get("queue_transport") != manifest.get("queue_transport")
        or lock.get("queue_private_storage_accounting")
        != manifest.get("queue_private_storage_accounting")
    ):
        raise GateError("tracked baseline lock does not authorize the supplied manifest")
    for field in (
        "immutable_module_sha256",
        "entrypoint_sha256",
        "criterion_sha256",
        "observer_sha256",
        "tool_sha256",
        "backend_sha256",
        "build_evidence_sha256",
        "artifact_sha256",
        "repetition_sha256",
        "host_fingerprint_sha256",
        "toolchain_fingerprint_sha256",
        "release_profile_sha256",
    ):
        if lock.get(field) != manifest.get(field):
            raise GateError("tracked baseline lock and manifest harness bindings differ")
    map_fields = (
        "immutable_module_sha256",
        "tool_sha256",
        "artifact_sha256",
        "repetition_sha256",
    )
    if any(not isinstance(lock[field], dict) or not lock[field] for field in map_fields):
        raise GateError("tracked baseline lock maps are malformed")
    digest_values = [
        lock["manifest_sha256"],
        lock["entrypoint_sha256"],
        lock["criterion_sha256"],
        lock["observer_sha256"],
        *lock["immutable_module_sha256"].values(),
        *lock["tool_sha256"].values(),
        *lock["artifact_sha256"].values(),
        *lock["repetition_sha256"].values(),
        lock["backend_sha256"],
        lock["build_evidence_sha256"],
        lock["host_fingerprint_sha256"],
        lock["toolchain_fingerprint_sha256"],
        lock["release_profile_sha256"],
    ]
    if any(not is_lower_digest(value) for value in digest_values):
        raise GateError("tracked baseline lock contains a noncanonical digest")
    return {
        "lock_bytes": lock_bytes,
        "lock_sha256": lock_digest,
        "manifest_bytes": manifest_bytes,
        "manifest_sha256": manifest_digest,
        "baseline_head": baseline_head,
        "lock": lock,
        "manifest": manifest,
    }


def validate_candidate_current_bindings(
    baseline: dict[str, Any], current: dict[str, Any], cargo_sha256: str
) -> None:
    lock = baseline["lock"]
    current_tools = {
        "build.rs": current["build_script_sha256"],
        "build_support.rs": current["build_support_sha256"],
        "capture_benchmark_host_gate.sh": current["host_gate_shell_sha256"],
        "capture_benchmark_host_gate.py": current["host_gate_python_sha256"],
        "capture_benchmark_process.py": current["host_gate_process_sha256"],
        "capture_benchmark_evidence_io.py": current["host_gate_evidence_io_sha256"],
        "capture_benchmark_host_cli.py": current["host_gate_cli_sha256"],
        "capture_benchmark_host_schema.py": current["host_gate_schema_sha256"],
        "capture_benchmark_host_execution.py": current[
            "host_gate_execution_sha256"
        ],
        "capture_benchmark_host_observation.py": current[
            "host_gate_observation_sha256"
        ],
        "capture_benchmark_host_measured.py": current["host_gate_measured_sha256"],
        "capture_benchmark_prepare_build_evidence.py": current[
            "build_evidence_python_sha256"
        ],
        "cargo-executable": cargo_sha256,
        "git-executable": current["git_executable_sha256"],
    }
    if (
        lock["immutable_module_sha256"] != current["immutable_module_sha256"]
        or lock["entrypoint_sha256"] != current["entrypoint_sha256"]
        or lock["criterion_sha256"] != current["criterion_sha256"]
        or lock["observer_sha256"] != current["observer_sha256"]
        or lock["tool_sha256"] != current_tools
        or current["cargo_executable_sha256"] != cargo_sha256
        or baseline["manifest"]["backend_sha256"] == current["backend_sha256"]
    ):
        raise GateError("candidate harness, tools, or backend distinction failed baseline lock")


def publish_build_bundle(
    directory: int,
    cargo_json: bytes,
    executable: bytes,
    evidence: dict[str, Any],
    baseline: dict[str, Any] | None = None,
    failure_injection: str | None = None,
) -> None:
    members: list[tuple[str, bytes, int, int, str | None]] = []
    if baseline is not None:
        members.extend(
            (
                (
                    "baseline-lock.json",
                    baseline["lock_bytes"],
                    0o600,
                    MAX_BASELINE_BYTES,
                    "after-baseline-lock-publication",
                ),
                (
                    "baseline-manifest.json",
                    baseline["manifest_bytes"],
                    0o600,
                    MAX_BASELINE_BYTES,
                    "after-baseline-manifest-publication",
                ),
            )
        )
    members.extend(
        (
            (
                "capture-bench-build.json",
                cargo_json,
                0o600,
                MAX_CARGO_JSON_BYTES,
                "after-cargo-publication",
            ),
            (
                "capture_admission_evidence-exe",
                executable,
                0o700,
                MAX_EXECUTABLE_BYTES,
                "after-executable-publication",
            ),
        )
    )
    names = tuple(member[0] for member in members) + (
        "build-evidence.json",
    )
    for name in names:
        try:
            os.stat(name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            continue
        raise GateError("build evidence bundle destination already exists")
    created: list[tuple[str, int, int]] = []
    try:
        for name, value, mode, maximum, injection in members:
            device, inode = publish_bytes(directory, name, value, mode, maximum)
            created.append((name, device, inode))
            if failure_injection == injection:
                raise GateError(f"fixture-injected failure after {name} publication")
        device, inode = publish_json(directory, "build-evidence.json", evidence)
        created.append(("build-evidence.json", device, inode))
        if failure_injection == "after-build-evidence-publication":
            raise GateError("fixture-injected failure after build-evidence publication")
    except Exception as error:
        rollback_error = None
        for name, device, inode in reversed(created):
            try:
                current = os.stat(name, dir_fd=directory, follow_symlinks=False)
                if (
                    not stat.S_ISREG(current.st_mode)
                    or current.st_dev != device
                    or current.st_ino != inode
                    or current.st_nlink != 1
                ):
                    raise GateError("published bundle member changed before rollback")
                os.unlink(name, dir_fd=directory)
            except Exception as current_error:
                rollback_error = current_error
                break
        os.fsync(directory)
        if rollback_error is not None:
            raise GateError("build evidence bundle rollback failed closed") from rollback_error
        raise error


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument(
        "--benchmark-backend", choices=("standard", "candidate"), default="standard"
    )
    parser.add_argument("--baseline-manifest", type=Path)
    parser.add_argument("--evidence-mode", choices=("production", "fixture"), default="production")
    parser.add_argument("--fixture-repository", type=Path)
    parser.add_argument("--fixture-controlled-root", type=Path)
    parser.add_argument(
        "--fixture-failure-injection",
        choices=(
            "after-runner-validation",
            "after-current-validation",
            "after-baseline-lock-publication",
            "after-baseline-manifest-publication",
            "after-cargo-publication",
            "after-executable-publication",
            "after-build-evidence-publication",
        ),
    )
    parsed = parser.parse_args()
    fixture_values = (parsed.fixture_repository, parsed.fixture_controlled_root)
    if parsed.evidence_mode == "production" and (
        any(value is not None for value in fixture_values)
        or parsed.fixture_failure_injection is not None
    ):
        parser.error("production build evidence forbids fixture path overrides")
    if parsed.evidence_mode == "fixture" and any(value is None for value in fixture_values):
        parser.error("fixture build evidence requires its repository and controlled root")
    if parsed.benchmark_backend == "standard" and parsed.baseline_manifest is not None:
        parser.error("standard build forbids a candidate baseline manifest")
    if parsed.benchmark_backend == "candidate" and parsed.baseline_manifest is None:
        parser.error("candidate build requires a baseline manifest")
    return parsed


def main() -> int:
    parsed = arguments()
    try:
        ambient_environment = dict(os.environ)
        git_executable, git_sha256 = resolve_external_tool("git", ambient_environment)
        cargo_executable, cargo_sha256 = resolve_external_tool("cargo", ambient_environment)
        controlled_root_path = (
            parsed.fixture_controlled_root
            if parsed.evidence_mode == "fixture"
            else production_root()
        )
        root = CapabilityRoot.open(controlled_root_path)
        try:
            run_parts = root.relative(parsed.run_dir)
            run_directory = root.open_directory(run_parts)
            try:
                repository = (
                    parsed.fixture_repository
                    if parsed.evidence_mode == "fixture"
                    else Path(__file__).resolve().parents[1]
                )
                if Path(
                    command(git_executable, ["rev-parse", "--show-toplevel"], repository)
                ) != repository:
                    raise GateError("build evidence script is not under the exact Git root")
                git_head = command(git_executable, ["rev-parse", "HEAD"], repository)
                if command(
                    git_executable,
                    ["status", "--porcelain=v1", "--untracked-files=all"],
                    repository,
                ):
                    raise GateError("build evidence requires an exact clean Git tree")
                baseline = None
                if parsed.benchmark_backend == "candidate":
                    lock_path = (repository / BASELINE_LOCK_RELATIVE).resolve()
                    if command(
                        git_executable,
                        ["ls-files", "--error-unmatch", str(BASELINE_LOCK_RELATIVE)],
                        repository,
                    ) != str(BASELINE_LOCK_RELATIVE):
                        raise GateError("candidate baseline lock is not tracked at the exact path")
                    lock_bytes = repository_file_bytes(
                        lock_path, repository, MAX_BASELINE_BYTES
                    )
                    manifest_bytes = root.read_file(
                        parsed.baseline_manifest, MAX_BASELINE_BYTES
                    )
                    baseline = validate_baseline_inputs(
                        repository,
                        git_head,
                        lock_bytes,
                        manifest_bytes,
                    )
                build_environment, command_sha256, environment_sha256 = (
                    sanitized_build_environment(
                        repository,
                        ambient_environment,
                        cargo_executable,
                        cargo_sha256,
                        git_executable,
                        git_sha256,
                        parsed.benchmark_backend,
                        baseline["lock_sha256"] if baseline is not None else None,
                        baseline["manifest_sha256"] if baseline is not None else None,
                        baseline["baseline_head"] if baseline is not None else None,
                    )
                )
                cargo_json, cargo_sha256, observed_command_sha256, observed_environment_sha256 = (
                    run_closed_cargo_build(
                        repository,
                        build_environment,
                        cargo_executable,
                        cargo_sha256,
                    )
                )
                if (
                    observed_command_sha256 != command_sha256
                    or observed_environment_sha256 != environment_sha256
                ):
                    raise GateError("closed Cargo build contract changed during invocation")
                cargo_artifact, _message = parse_cargo_artifact(cargo_json, repository)
                executable_bytes = artifact_bytes(cargo_artifact, repository)
                bindings = runner_bindings(cargo_artifact)
                if bindings["measured_code_head"] != git_head:
                    raise GateError("embedded build head differs from the clean current head")
                if (
                    bindings["build_command_sha256"] != command_sha256
                    or bindings["build_environment_sha256"] != environment_sha256
                ):
                    raise GateError(
                        "embedded build environment differs from the owned invocation: "
                        f"observed={bindings['build_environment_sha256']} "
                        f"expected={environment_sha256}"
                    )
                if bindings["evidence_backend"] != parsed.benchmark_backend:
                    raise GateError("embedded runner backend differs from the requested build")
                if parsed.fixture_failure_injection == "after-runner-validation":
                    raise GateError("fixture-injected failure after runner validation")
                current_bindings = recompute_current_bindings(
                    repository,
                    cargo_sha256,
                    git_sha256,
                    parsed.benchmark_backend,
                )
                validate_current_bindings(bindings, current_bindings)
                if baseline is not None:
                    validate_candidate_current_bindings(baseline, current_bindings, cargo_sha256)
                if parsed.fixture_failure_injection == "after-current-validation":
                    raise GateError("fixture-injected failure after current-tree validation")
                executable_digest = sha256(executable_bytes)
                if command(git_executable, ["rev-parse", "HEAD"], repository) != git_head or command(
                    git_executable,
                    ["status", "--porcelain=v1", "--untracked-files=all"],
                    repository,
                ):
                    raise GateError("Git head or cleanliness changed during build-evidence proof")
                evidence = dict(bindings)
                evidence.update(
                    {
                        "cargo_target": TARGET,
                        "benchmark_feature": FEATURE,
                        "git_tree_clean": True,
                        "cargo_locked": True,
                        "all_features": True,
                        "release": True,
                        "build_command": list(BUILD_COMMAND),
                        "build_environment_policy": BUILD_ENVIRONMENT_POLICY,
                        "build_command_sha256": command_sha256,
                        "build_environment_sha256": environment_sha256,
                        "cargo_executable_sha256": cargo_sha256,
                        "git_executable_sha256": git_sha256,
                        "executable_path": "./capture_admission_evidence-exe",
                        "executable_sha256": executable_digest,
                        "cargo_json_path": "./capture-bench-build.json",
                        "cargo_json_sha256": sha256(cargo_json),
                    }
                )
                if parsed.evidence_mode == "fixture":
                    evidence["test_fixture"] = True
                publish_build_bundle(
                    run_directory,
                    cargo_json,
                    executable_bytes,
                    evidence,
                    baseline,
                    parsed.fixture_failure_injection,
                )
            finally:
                os.close(run_directory)
        finally:
            root.close()
        return 0
    except (GateError, OSError, ValueError, TypeError, UnicodeError) as error:
        print(f"capture benchmark build evidence failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
