#!/usr/bin/env python3
"""Fail-closed local host evidence gate for authoritative capture measurements."""

from __future__ import annotations

import os
import secrets
import stat
import subprocess
import sys
import time
from contextlib import nullcontext
from pathlib import Path
from typing import Any

if __package__:
    from . import capture_benchmark_evidence_io as _evidence_io
    from .capture_benchmark_host_cli import parse_arguments
    from .capture_benchmark_evidence_io import (
        CapabilityRoot,
        GateError,
        NOFOLLOW,
        canonical_json,
        digest_bytes,
        digest_json,
        production_root,
        publish_bytes,
        publish_json,
        read_exact_descriptor,
        read_json_bytes,
        require_platform_primitives,
        sync_directory,
        sync_file,
        write_owner_all,
    )
    from .capture_benchmark_host_measured import (
        MeasurementContract,
        attestation_identity,
        build_evidence_contract,
        create_execution_runner,
        measured_file_binding,
        remove_execution_runner,
        require_measured_file_binding,
        verify_authority_module_bindings,
        verify_build_tool_identities,
        verify_execution_contract,
    )
    from .capture_benchmark_host_observation import (
        fixture_observation,
        normalize_observation,
        normalize_processes,
        production_observation,
        production_processes,
    )
    from .capture_benchmark_host_schema import (
        PRODUCTION_MODE,
        SCHEMA_VERSION,
        bind_active_agent_attestation,
        validate_phase_evidence,
        validate_phase_stability,
    )
    from .capture_benchmark_process import (
        bounded_process,
        bounded_session_leader_exec,
        process_group_exists,
        terminate_process_group,
    )
else:
    import capture_benchmark_evidence_io as _evidence_io
    from capture_benchmark_host_cli import parse_arguments
    from capture_benchmark_evidence_io import (
        CapabilityRoot,
        GateError,
        NOFOLLOW,
        canonical_json,
        digest_bytes,
        digest_json,
        production_root,
        publish_bytes,
        publish_json,
        read_exact_descriptor,
        read_json_bytes,
        require_platform_primitives,
        sync_directory,
        sync_file,
        write_owner_all,
    )
    from capture_benchmark_host_measured import (
        MeasurementContract,
        attestation_identity,
        build_evidence_contract,
        create_execution_runner,
        measured_file_binding,
        remove_execution_runner,
        require_measured_file_binding,
        verify_authority_module_bindings,
        verify_build_tool_identities,
        verify_execution_contract,
    )
    from capture_benchmark_host_observation import (
        fixture_observation,
        normalize_observation,
        normalize_processes,
        production_observation,
        production_processes,
    )
    from capture_benchmark_host_schema import (
        PRODUCTION_MODE,
        SCHEMA_VERSION,
        bind_active_agent_attestation,
        validate_phase_evidence,
        validate_phase_stability,
    )
    from capture_benchmark_process import (
        bounded_process,
        bounded_session_leader_exec,
        process_group_exists,
        terminate_process_group,
    )

MAX_JSON_BYTES = 1024 * 1024
MAX_CLOCK_DRIFT_NS = 2_000_000_000
MAX_NORMALIZED_PREFLIGHT_LOAD = 0.10
MAX_MONITOR_SAMPLES = 100_000
MONITOR_SAMPLE_INTERVAL_SECONDS = 0.25
MONITOR_SAMPLE_INTERVAL_NS = 250_000_000
MAX_MONITOR_DURATION_NS = MAX_MONITOR_SAMPLES * MONITOR_SAMPLE_INTERVAL_NS


def require_owner(lock: int) -> tuple[dict[str, Any], os.stat_result]:
    descriptor = os.open("owner.json", os.O_RDONLY | NOFOLLOW, dir_fd=lock)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_size > 4096:
            raise GateError("host-gate owner record is unsafe")
        owner = read_json_bytes(read_exact_descriptor(descriptor, metadata.st_size, 4096))
        after = os.fstat(descriptor)
        current = os.stat("owner.json", dir_fd=lock, follow_symlinks=False)
        identity = (metadata.st_dev, metadata.st_ino, metadata.st_size)
        if (
            (after.st_dev, after.st_ino, after.st_size) != identity
            or (current.st_dev, current.st_ino, current.st_size) != identity
            or current.st_nlink != 1
        ):
            raise GateError("host-gate owner changed during its read")
    finally:
        os.close(descriptor)
    if not isinstance(owner, dict) or set(owner) != {
        "schema_version",
        "nonce",
        "lock_dev",
        "lock_ino",
        "owner_dev",
        "owner_ino",
        "uid",
    }:
        raise GateError("host-gate owner record has an invalid schema")
    lock_metadata = os.fstat(lock)
    if (
        owner["schema_version"] != SCHEMA_VERSION
        or owner["uid"] != os.getuid()
        or owner["lock_dev"] != lock_metadata.st_dev
        or owner["lock_ino"] != lock_metadata.st_ino
        or owner["owner_dev"] != metadata.st_dev
        or owner["owner_ino"] != metadata.st_ino
        or not isinstance(owner["nonce"], str)
        or len(owner["nonce"]) != 64
    ):
        raise GateError("host-gate owner binding changed")
    return owner, metadata


def observe(
    root: CapabilityRoot,
    fixture: Path | None,
    phase: str,
    mode: str,
    contract: MeasurementContract | None = None,
) -> dict[str, Any]:
    raw = fixture_observation(root, fixture, phase) if fixture else production_observation()
    return normalize_observation(
        raw,
        mode,
        expected_measured_code_head=(contract.measured_code_head if contract else None),
        benchmark_backend=(contract.backend if contract else "standard"),
        baseline_manifest_sha256=(contract.baseline_manifest_sha256 if contract else None),
        baseline_lock_sha256=(contract.baseline_lock_sha256 if contract else None),
    )


def preflight(
    root: CapabilityRoot,
    lock_path: Path,
    output_path: Path,
    attestation: Path,
    fixture: Path | None,
    mode: str,
    measurement_inputs: dict[str, Any] | None = None,
    contract: MeasurementContract | None = None,
) -> dict[str, Any]:
    lock = root.open_directory(root.relative(lock_path))
    output = None
    owner_created = False
    try:
        lock_metadata = os.fstat(lock)
        owner = {
            "schema_version": SCHEMA_VERSION,
            "nonce": secrets.token_hex(32),
            "lock_dev": lock_metadata.st_dev,
            "lock_ino": lock_metadata.st_ino,
            "owner_dev": 0,
            "owner_ino": 0,
            "uid": os.getuid(),
        }
        temporary_name = f".owner-{secrets.token_hex(16)}"
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | NOFOLLOW,
            0o600,
            dir_fd=lock,
        )
        try:
            metadata = os.fstat(descriptor)
            owner["owner_dev"], owner["owner_ino"] = metadata.st_dev, metadata.st_ino
            encoded = canonical_json(owner)
            write_owner_all(descriptor, encoded)
            sync_file(descriptor)
            os.link(
                temporary_name,
                "owner.json",
                src_dir_fd=lock,
                dst_dir_fd=lock,
                follow_symlinks=False,
            )
            owner_created = True
            os.unlink(temporary_name, dir_fd=lock)
            sync_directory(lock)
        except FileExistsError as error:
            raise GateError("exclusive host-gate lock is already owned") from error
        finally:
            try:
                os.unlink(temporary_name, dir_fd=lock)
            except FileNotFoundError:
                pass
            os.close(descriptor)
        if _evidence_io._failure_injected(
            _evidence_io.FailureInjection.AFTER_OWNER
        ):
            raise GateError("fixture-injected interruption after owner publication")
        output = root.open_directory(root.relative(output_path), create_final=True)
        observed = observe(root, fixture, "preflight", mode, contract)
        if mode == PRODUCTION_MODE and contract is not None:
            verify_build_tool_identities(contract, observed["stable_toolchain"])
        observed["phase"] = "preflight"
        observed["lock_nonce_sha256"] = digest_bytes(owner["nonce"].encode())
        observed["lock_identity"] = {
            "device": lock_metadata.st_dev,
            "inode": lock_metadata.st_ino,
        }
        observed["owner_identity"] = {
            "device": owner["owner_dev"],
            "inode": owner["owner_ino"],
        }
        output_metadata = os.fstat(output)
        observed["output_identity"] = {
            "device": output_metadata.st_dev,
            "inode": output_metadata.st_ino,
        }
        bind_active_agent_attestation(
            observed, attestation_identity(root, attestation)
        )
        observed["measurement_inputs"] = measurement_inputs
        if observed["normalized_load"] > MAX_NORMALIZED_PREFLIGHT_LOAD:
            raise GateError("preflight normalized load exceeds the idle threshold")
        if observed["competitor_pids"]:
            raise GateError("preflight found a competing build or benchmark process")
        publish_json(output, "preflight.json", validate_phase_evidence(observed, "preflight"))
        return observed
    except Exception:
        if owner_created:
            try:
                os.unlink("owner.json", dir_fd=lock)
                sync_directory(lock)
            except OSError:
                pass
        raise
    finally:
        if output is not None:
            os.close(output)
        os.close(lock)


def postflight(
    root: CapabilityRoot,
    lock_path: Path,
    output_path: Path,
    attestation: Path,
    fixture: Path | None,
    mode: str,
    monitor: dict[str, Any] | None = None,
    measurement_inputs: dict[str, Any] | None = None,
    contract: MeasurementContract | None = None,
) -> None:
    lock = root.open_directory(root.relative(lock_path))
    output = root.open_directory(root.relative(output_path))
    try:
        owner, _metadata = require_owner(lock)
        pre = validate_phase_evidence(
            read_json_bytes(root.read_file(output_path / "preflight.json")), "preflight"
        )
        post = observe(root, fixture, "postflight", mode, contract)
        post["phase"] = "postflight"
        post["lock_nonce_sha256"] = digest_bytes(owner["nonce"].encode())
        lock_metadata = os.fstat(lock)
        owner_metadata = os.stat("owner.json", dir_fd=lock, follow_symlinks=False)
        output_metadata = os.fstat(output)
        post["lock_identity"] = {
            "device": lock_metadata.st_dev,
            "inode": lock_metadata.st_ino,
        }
        post["owner_identity"] = {
            "device": owner_metadata.st_dev,
            "inode": owner_metadata.st_ino,
        }
        post["output_identity"] = {
            "device": output_metadata.st_dev,
            "inode": output_metadata.st_ino,
        }
        bind_active_agent_attestation(post, attestation_identity(root, attestation))
        post["measurement_inputs"] = measurement_inputs
        validate_phase_stability(pre, post)
        if post["competitor_pids"]:
            raise GateError("postflight found a competing build or benchmark process")
        monotonic_delta = post["monotonic_ns"] - pre["monotonic_ns"]
        wall_delta = post["wall_ns"] - pre["wall_ns"]
        if (
            monotonic_delta <= 0
            or wall_delta <= 0
            or abs(wall_delta - monotonic_delta) > MAX_CLOCK_DRIFT_NS
        ):
            raise GateError("postflight detected sleep, wake, or clock discontinuity")
        publish_json(output, "postflight.json", validate_phase_evidence(post, "postflight"))
        if _evidence_io._failure_injected(
            _evidence_io.FailureInjection.AFTER_POSTFLIGHT
        ):
            raise GateError("fixture-injected interruption after postflight publication")
        comparison = {
            "schema_version": SCHEMA_VERSION,
            "evidence_mode": mode,
            "valid": True,
            "host_fingerprint_sha256": post["host_fingerprint_sha256"],
            "toolchain_fingerprint_sha256": post["toolchain_fingerprint_sha256"],
            "release_profile_sha256": post["release_profile_sha256"],
            "preflight_sha256": digest_bytes(root.read_file(output_path / "preflight.json")),
            "postflight_sha256": digest_bytes(root.read_file(output_path / "postflight.json")),
            "lock_nonce_sha256": post["lock_nonce_sha256"],
            "wall_elapsed_ns": wall_delta,
            "monotonic_elapsed_ns": monotonic_delta,
            "continuous_monitor": monitor is not None,
            "monitor_sha256": digest_json(monitor) if monitor is not None else "0" * 64,
            "runner_sha256": monitor["runner_sha256"] if monitor is not None else "0" * 64,
            "build_evidence_sha256": (
                monitor["build_evidence_sha256"] if monitor is not None else "0" * 64
            ),
            "baseline_manifest_sha256": post["baseline_manifest_sha256"],
            "baseline_lock_sha256": post["baseline_lock_sha256"],
            "monitored_repetitions": monitor["repetitions"] if monitor is not None else 0,
            "monitor_samples": monitor["samples"] if monitor is not None else 0,
        }
        publish_json(output, "comparison.json", comparison)
    finally:
        os.close(output)
        os.close(lock)


def measure(
    root: CapabilityRoot,
    lock_path: Path,
    output_path: Path,
    attestation: Path,
    runner: Path,
    build_evidence: Path,
    fixture: Path | None,
    mode: str,
) -> None:
    run_directory = output_path.parent
    if runner != run_directory / "capture_admission_evidence-exe" or build_evidence != (
        run_directory / "build-evidence.json"
    ):
        raise GateError("monitored runner inputs are not exact evidence-local artifacts")
    build_binding, contract = build_evidence_contract(root, build_evidence)
    runner_binding = measured_file_binding(root, runner, 256 * 1024 * 1024)
    if runner_binding["sha256"] != contract.executable_sha256:
        raise GateError("descriptor-bound runner digest disagrees with build evidence")
    measurement_inputs = {
        "runner": runner_binding,
        "build_evidence": build_binding,
    }
    if contract.backend == "candidate":
        baseline_manifest = run_directory / "baseline-manifest.json"
        baseline_lock = run_directory / "baseline-lock.json"
        manifest_binding = measured_file_binding(root, baseline_manifest, MAX_JSON_BYTES)
        lock_binding = measured_file_binding(root, baseline_lock, MAX_JSON_BYTES)
        if (
            manifest_binding["sha256"] != contract.baseline_manifest_sha256
            or lock_binding["sha256"] != contract.baseline_lock_sha256
        ):
            raise GateError("candidate baseline inputs disagree with build evidence")
        measurement_inputs["baseline_manifest"] = manifest_binding
        measurement_inputs["baseline_lock"] = lock_binding
    execution_runner, execution_binding = create_execution_runner(
        root, run_directory, runner, measurement_inputs["runner"]
    )
    measurement_inputs["execution_runner"] = execution_binding
    samples = 0
    repetition_checks = []
    process_observer = None
    process_observer_invocations = 0
    monitor_started_ns = time.monotonic_ns()
    try:
        verify_authority_module_bindings(contract)
        verify_execution_contract(execution_runner, run_directory, contract)
        pre = preflight(
            root,
            lock_path,
            output_path,
            attestation,
            fixture,
            mode,
            measurement_inputs,
            contract,
        )
        ps_identity = pre["stable_toolchain"]["tool_identities"]["ps"]
        for repetition in range(1, 6):
            _require_measurement_bindings(
                root,
                run_directory,
                execution_runner,
                measurement_inputs,
                hash_contents=True,
            )
            environment = contract.environment(run_directory, build_evidence, repetition)
            process = subprocess.Popen(
                [str(execution_runner), "--bench"],
                cwd=run_directory,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            try:
                while process.poll() is None:
                    if _evidence_io._failure_injected(
                        _evidence_io.FailureInjection.MONITOR_COMPETITOR
                    ):
                        raise GateError("fixture-injected interval competitor")
                    if mode == PRODUCTION_MODE:
                        execution_records: list[dict[str, Any]] = []
                        _redacted, competitors = normalize_processes(
                            production_processes(ps_identity, execution_records),
                            {process.pid},
                        )
                        if len(execution_records) != 1:
                            raise GateError("continuous process observer receipt is incomplete")
                        if process_observer is None:
                            process_observer = execution_records[0]
                        elif execution_records[0] != process_observer:
                            raise GateError("continuous process observer identity changed")
                        process_observer_invocations += 1
                        if competitors:
                            raise GateError("continuous monitor found a competing process")
                    _require_measurement_bindings(
                        root,
                        run_directory,
                        execution_runner,
                        measurement_inputs,
                        hash_contents=False,
                    )
                    samples += 1
                    if samples > MAX_MONITOR_SAMPLES:
                        raise GateError("continuous monitor exceeded its sample bound")
                    if time.monotonic_ns() - monitor_started_ns > MAX_MONITOR_DURATION_NS:
                        raise GateError("continuous monitor exceeded its time bound")
                    time.sleep(MONITOR_SAMPLE_INTERVAL_SECONDS)
            except Exception:
                terminate_process_group(process)
                _wait_for_process_group_extinction(process.pid)
                raise
            if process_group_exists(process.pid):
                terminate_process_group(process)
                _wait_for_process_group_extinction(process.pid)
                raise GateError("authoritative runner left a background descendant")
            if process.returncode != 0:
                raise GateError("authoritative runner failed during monitored execution")
            _require_measurement_bindings(
                root,
                run_directory,
                execution_runner,
                measurement_inputs,
                hash_contents=True,
            )
            if time.monotonic_ns() - monitor_started_ns > MAX_MONITOR_DURATION_NS:
                raise GateError("continuous monitor exceeded its time bound")
            repetition_checks.append({"repetition": repetition, **measurement_inputs})
        if samples < 5:
            raise GateError("continuous monitor did not observe every repetition")
        if mode == PRODUCTION_MODE and (
            process_observer is None or process_observer_invocations != samples
        ):
            raise GateError("continuous process observer coverage is incomplete")
        monitor = {
            "schema_version": SCHEMA_VERSION,
            "evidence_mode": mode,
            "runner_sha256": measurement_inputs["runner"]["sha256"],
            "build_evidence_sha256": measurement_inputs["build_evidence"]["sha256"],
            "measured_code_head": contract.measured_code_head,
            "benchmark_backend": contract.backend,
            "baseline_manifest_sha256": contract.baseline_manifest_sha256,
            "baseline_lock_sha256": contract.baseline_lock_sha256,
            "measurement_inputs": measurement_inputs,
            "repetition_checks": repetition_checks,
            "repetitions": 5,
            "samples": samples,
            "process_observer": process_observer,
            "process_observer_invocations": process_observer_invocations,
            "process_observer_sha256": (
                digest_json(process_observer) if process_observer is not None else "0" * 64
            ),
            "duration_ns": time.monotonic_ns() - monitor_started_ns,
            "maximum_duration_ns": MAX_MONITOR_DURATION_NS,
            "maximum_samples": MAX_MONITOR_SAMPLES,
            "sample_interval_ns": MONITOR_SAMPLE_INTERVAL_NS,
            "interval_complete": True,
        }
        output = root.open_directory(root.relative(output_path))
        try:
            publish_json(output, "monitor.json", monitor)
        finally:
            os.close(output)
        postflight(
            root,
            lock_path,
            output_path,
            attestation,
            fixture,
            mode,
            monitor,
            measurement_inputs,
            contract,
        )
    finally:
        remove_execution_runner(
            root, execution_runner, measurement_inputs["execution_runner"]
        )


def _require_measurement_bindings(
    root: CapabilityRoot,
    run_directory: Path,
    execution_runner: Path,
    expected: dict[str, Any],
    *,
    hash_contents: bool,
) -> None:
    paths = {
        "runner": (run_directory / "capture_admission_evidence-exe", 256 * 1024 * 1024),
        "execution_runner": (execution_runner, 256 * 1024 * 1024),
        "build_evidence": (run_directory / "build-evidence.json", MAX_JSON_BYTES),
        "baseline_manifest": (run_directory / "baseline-manifest.json", MAX_JSON_BYTES),
        "baseline_lock": (run_directory / "baseline-lock.json", MAX_JSON_BYTES),
    }
    for name, binding in expected.items():
        path, maximum = paths[name]
        require_measured_file_binding(
            root,
            path,
            maximum,
            binding,
            hash_contents=hash_contents,
        )


def _wait_for_process_group_extinction(process_group_id: int) -> None:
    deadline = time.monotonic() + 5
    while process_group_exists(process_group_id) and time.monotonic() < deadline:
        time.sleep(0.01)
    if process_group_exists(process_group_id):
        raise GateError("owned runner process group survived forced termination")


def release(
    root: CapabilityRoot,
    lock_path: Path,
    release_ticket: Path,
    caller_expected: dict[str, Any],
) -> None:
    parts = root.relative(lock_path)
    parent = root.open_directory(parts[:-1]) if len(parts) > 1 else os.dup(root.descriptor)
    lock = root.open_directory(parts)
    try:
        owner, metadata = require_owner(lock)
        ticket = validate_phase_evidence(
            read_json_bytes(root.read_file(release_ticket)), "preflight"
        )
        lock_metadata = os.fstat(lock)
        expected = {
            "lock_identity": {
                "device": lock_metadata.st_dev,
                "inode": lock_metadata.st_ino,
            },
            "owner_identity": {
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
            },
            "lock_nonce_sha256": digest_bytes(owner["nonce"].encode()),
        }
        if (
            any(ticket.get(field) != value for field, value in expected.items())
            or expected != caller_expected
        ):
            raise GateError("release ticket does not bind the exact current lock owner")
        if sorted(os.listdir(lock)) != ["owner.json"]:
            raise GateError("host-gate lock contains unexpected entries")
        current = os.stat("owner.json", dir_fd=lock, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise GateError("host-gate owner changed before release")
        os.unlink("owner.json", dir_fd=lock)
        sync_directory(lock)
        os.close(lock)
        lock = -1
        os.rmdir(parts[-1], dir_fd=parent)
        sync_directory(parent)
    finally:
        if lock >= 0:
            os.close(lock)
        os.close(parent)


def main() -> int:
    parsed = parse_arguments()
    try:
        injection_scope = nullcontext()
        if parsed.failure_injection is not None:
            injection_scope = _evidence_io._fixture_failure_injection(
                _evidence_io.FailureInjection(parsed.failure_injection),
                parsed.evidence_mode,
            )
        with injection_scope:
            root_path = (
                parsed.controlled_root if parsed.controlled_root else production_root()
            )
            root = CapabilityRoot.open(root_path)
            try:
                if parsed.phase == "preflight":
                    preflight(
                        root,
                        parsed.lock_dir,
                        parsed.output_dir,
                        parsed.active_agent_attestation,
                        parsed.observation_fixture,
                        parsed.evidence_mode,
                    )
                elif parsed.phase == "postflight":
                    postflight(
                        root,
                        parsed.lock_dir,
                        parsed.output_dir,
                        parsed.active_agent_attestation,
                        parsed.observation_fixture,
                        parsed.evidence_mode,
                    )
                elif parsed.phase == "measure":
                    measure(
                        root,
                        parsed.lock_dir,
                        parsed.output_dir,
                        parsed.active_agent_attestation,
                        parsed.runner,
                        parsed.build_evidence,
                        parsed.observation_fixture,
                        parsed.evidence_mode,
                    )
                else:
                    release(
                        root,
                        parsed.lock_dir,
                        parsed.release_ticket,
                        {
                            "lock_identity": {
                                "device": parsed.expected_lock_device,
                                "inode": parsed.expected_lock_inode,
                            },
                            "owner_identity": {
                                "device": parsed.expected_owner_device,
                                "inode": parsed.expected_owner_inode,
                            },
                            "lock_nonce_sha256": parsed.expected_nonce_sha256,
                        },
                    )
            finally:
                root.close()
        return 0
    except (GateError, OSError, ValueError, TypeError) as error:
        print(f"capture benchmark host gate failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
