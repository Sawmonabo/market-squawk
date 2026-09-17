"""Capability-root fixtures and exact inventory for host-gate tests."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[2]
GATE = REPOSITORY / "scripts" / "capture_benchmark_host_gate.sh"
SECRET = "MSQ_HOST_GATE_SECRET_DO_NOT_PERSIST"
PROFILE = (
    "cargo-release-binary:opt-level=3:lto=thin:codegen-units=1:"
    "panic=abort:strip=symbols"
)
RUNNER_SCHEMA_VERSION = 5
BUILD_COMMAND = [
    "cargo",
    "build",
    "-p",
    "market-squawk-platform",
    "--bin",
    "capture-admission-evidence",
    "--all-features",
    "--release",
    "--locked",
    "--message-format=json-render-diagnostics",
]
RUNNER_BINDING_FIELDS = {
    "schema_version",
    "runner",
    "evidence_mode",
    "evidence_backend",
    "queue_transport",
    "queue_private_storage_accounting",
    "build_profile",
    "clean_build_enforced",
    "build_environment_policy",
    "build_command_sha256",
    "build_environment_sha256",
    "measured_source_closure_sha256",
    "cargo_executable_sha256",
    "git_executable_sha256",
    "rustc_executable_sha256",
    "source_inventory_sha256",
    "cargo_lock_sha256",
    "workspace_manifest_sha256",
    "package_manifest_sha256",
    "build_script_sha256",
    "build_support_sha256",
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


def tool_identity(name: str, index: int) -> dict:
    return {
        "state": "available",
        "path": f"/fixture/bin/{name}",
        "device": 1,
        "inode": index + 1,
        "size": 4096 + index,
        "uid": os.getuid(),
        "mode": 0o500,
        "nlink": 1,
        "flags": 0,
        "execution_strategy": "ephemeral-copy",
        "sha256": f"{index + 1:x}" * 64,
    }


def tool_execution(name: str, identity: dict) -> dict:
    executed = {
        field: identity[field]
        for field in (
            "path",
            "device",
            "inode",
            "size",
            "uid",
            "mode",
            "nlink",
            "flags",
            "sha256",
        )
    }
    executed.update(
        {
            "path": (
                f"/fixture/bin/.market-squawk-bound-{identity['inode']:032x}"
            ),
            "inode": identity["inode"] + 100,
            "mode": 0o500,
            "nlink": 1,
            "flags": 0,
        }
    )
    return {
        "tool": name,
        "strategy": "ephemeral-copy",
        "source_sha256": identity["sha256"],
        "executed": executed,
        "protection": "exclusive-current-uid-attestation",
        "path_removed": True,
    }


def benchmark_artifacts(
    *,
    backend: str = "standard",
    bench_body: str = "/bin/sleep 0.2\nexit 0",
    baseline_manifest_sha256: str | None = None,
    baseline_lock_sha256: str | None = None,
) -> tuple[bytes, bytes]:
    """Return a fake bound runner and its exact canonical build evidence."""

    if backend not in {"standard", "candidate"}:
        raise ValueError("unsupported fixture backend")
    candidate = backend == "candidate"
    if candidate != (
        baseline_manifest_sha256 is not None and baseline_lock_sha256 is not None
    ):
        raise ValueError("candidate fixture baseline digests are incomplete")
    digest = "a" * 64
    evidence = {
        "schema_version": RUNNER_SCHEMA_VERSION,
        "runner": "capture_admission_evidence",
        "evidence_mode": "diagnostic_fixed_quota",
        "evidence_backend": backend,
        "queue_transport": "candidate_fixed_ring" if candidate else "standard_sync_channel",
        "queue_private_storage_accounting": "exact" if candidate else "not_measured",
        "cargo_target": "capture-admission-evidence",
        "benchmark_feature": "capture-benchmark",
        "build_profile": PROFILE,
        "measured_code_head": ("2" if candidate else "1") * 40,
        "clean_build_enforced": True,
        "build_command": BUILD_COMMAND,
        "build_environment_policy": "sanitized-cargo-release-runner-v3",
        "build_command_sha256": digest,
        "build_environment_sha256": digest,
        "cargo_executable_sha256": digest,
        "git_executable_sha256": digest,
        "rustc_executable_sha256": digest,
        "git_tree_clean": True,
        "cargo_locked": True,
        "all_features": True,
        "release": True,
        "executable_path": "./capture_admission_evidence-exe",
        "executable_sha256": "0" * 64,
        "cargo_json_path": "./capture-bench-build.json",
        "cargo_json_sha256": digest,
        "measured_source_closure_sha256": digest,
        "source_inventory_sha256": digest,
        "cargo_lock_sha256": digest,
        "workspace_manifest_sha256": digest,
        "package_manifest_sha256": digest,
        "build_script_sha256": digest,
        "build_support_sha256": digest,
        "host_gate_shell_sha256": hashlib.sha256(
            (REPOSITORY / "scripts/capture_benchmark_host_gate.sh").read_bytes()
        ).hexdigest(),
        "host_gate_python_sha256": hashlib.sha256(
            (REPOSITORY / "scripts/capture_benchmark_host_gate.py").read_bytes()
        ).hexdigest(),
        "host_gate_process_sha256": hashlib.sha256(
            (REPOSITORY / "scripts/capture_benchmark_process.py").read_bytes()
        ).hexdigest(),
        "host_gate_evidence_io_sha256": hashlib.sha256(
            (REPOSITORY / "scripts/capture_benchmark_evidence_io.py").read_bytes()
        ).hexdigest(),
        "host_gate_cli_sha256": hashlib.sha256(
            (REPOSITORY / "scripts/capture_benchmark_host_cli.py").read_bytes()
        ).hexdigest(),
        "host_gate_schema_sha256": hashlib.sha256(
            (REPOSITORY / "scripts/capture_benchmark_host_schema.py").read_bytes()
        ).hexdigest(),
        "host_gate_execution_sha256": hashlib.sha256(
            (REPOSITORY / "scripts/capture_benchmark_host_execution.py").read_bytes()
        ).hexdigest(),
        "host_gate_observation_sha256": hashlib.sha256(
            (REPOSITORY / "scripts/capture_benchmark_host_observation.py").read_bytes()
        ).hexdigest(),
        "host_gate_measured_sha256": hashlib.sha256(
            (REPOSITORY / "scripts/capture_benchmark_host_measured.py").read_bytes()
        ).hexdigest(),
        "build_evidence_python_sha256": digest,
        "platform_source_sha256": digest,
        "domain_source_sha256": digest,
        "entrypoint_sha256": digest,
        "backend_dispatcher_sha256": digest,
        "selected_backend_source_path": (
            "crates/market-squawk-platform/benches/capture_admission/backend/"
            f"{backend}.rs"
        ),
        "selected_backend_source_sha256": digest,
        "backend_sha256": digest,
        "criterion_sha256": digest,
        "observer_sha256": digest,
        "immutable_module_sha256": {"fixture": digest},
        "baseline_lock_path": "./baseline-lock.json" if candidate else None,
        "baseline_lock_sha256": baseline_lock_sha256,
        "baseline_manifest_path": "./baseline-manifest.json" if candidate else None,
        "baseline_manifest_sha256": baseline_manifest_sha256,
        "baseline_measured_code_head": "1" * 40 if candidate else None,
    }
    bindings = {name: evidence[name] for name in RUNNER_BINDING_FIELDS}
    bindings_json = json.dumps(bindings, sort_keys=True, separators=(",", ":"))
    script = (
        "#!/bin/sh\n"
        "if [ \"${1-}\" = \"--print-build-bindings\" ]; then\n"
        f"  printf '%s\\n' '{bindings_json}'\n"
        "  exit 0\n"
        "fi\n"
        "if [ \"${1-}\" != \"--bench\" ]; then\n"
        "  exit 64\n"
        "fi\n"
        f"{bench_body}\n"
    ).encode()
    evidence["executable_sha256"] = hashlib.sha256(script).hexdigest()
    evidence_json = (json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n").encode()
    return script, evidence_json

def observation(wall: int, monotonic: int, load: float = 0.5) -> dict:
    tools = {
        name: tool_identity(name, index)
        for index, name in enumerate(("cargo", "git", "ps", "python", "rustc"))
    }
    tools.update(
        {
            "pmset": {"state": "unavailable", "reason": "not-required-on-platform"},
            "rustup": {
                "state": "unavailable",
                "reason": "not-used-for-direct-toolchain",
            },
            "sysctl": {"state": "unavailable", "reason": "not-required-on-platform"},
        }
    )
    return {
        "host": {
            "system": "FixtureOS",
            "machine": "fixture64",
            "node": "fixture-host",
            "boot_id": "fixture-boot",
            "logical_cpus": 10,
            "affinity": list(range(10)),
            "nice": 0,
            "scheduler": 0,
            "power_hash": "a" * 64,
            "thermal_hash": "b" * 64,
        },
        "toolchain": {
            "rustc": "rustc fixture",
            "cargo": "cargo fixture",
            "target": "fixture-target",
            "release_profile": PROFILE,
            "tool_identities": tools,
        },
        "measured_code_head": "1" * 40,
        "tool_executions": [
            tool_execution(name, tools[name])
            for name in ("cargo", "git", "ps", "python", "rustc")
        ],
        "load_one_minute": load,
        "processes": [
            {
                "pid": 101,
                "ppid": 1,
                "state": "S",
                "comm": "fixture",
                "argv": f"fixture --token={SECRET}",
            }
        ],
        "wall_ns": wall,
        "monotonic_ns": monotonic,
    }


class HostGateFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        os.chmod(self.root, 0o700)
        self.lock = self.root / ".exclusive-lock"
        self.lock.mkdir(mode=0o700)
        self.run = self.root / "standard"
        self.run.mkdir(mode=0o700)
        self.output = self.run / "host-gate"
        self.attestation = self.run / "active-agent-attestation.txt"
        self._write_private(self.attestation, b"no-other-active-agents\n")
        self.fixture = self.run / "observation.json"
        self.fixture_value = {
            "preflight": observation(1_000_000_000_000, 500_000_000_000),
            "postflight": observation(1_010_000_000_000, 510_000_000_000, load=100.0),
        }
        self._write_fixture()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_private(self, path: Path, value: bytes) -> None:
        path.write_bytes(value)
        os.chmod(path, 0o600)

    def _write_fixture(self) -> None:
        self._write_private(
            self.fixture,
            (json.dumps(self.fixture_value, sort_keys=True) + "\n").encode(),
        )

    def _run(
        self,
        phase: str,
        *,
        check: bool = False,
        extra: tuple[str, ...] = (),
    ) -> subprocess.CompletedProcess[str]:
        arguments = [
            str(GATE),
            phase,
            "--lock-dir",
            str(self.lock),
            "--evidence-mode",
            "fixture",
            "--observation-fixture",
            str(self.fixture),
            "--controlled-root",
            str(self.root),
        ]
        if phase != "release":
            arguments.extend(
                [
                    "--active-agent-attestation",
                    str(self.attestation),
                    "--output-dir",
                    str(self.output),
                ]
            )
        else:
            preflight = json.loads((self.output / "preflight.json").read_text())
            arguments.extend(
                [
                    "--release-ticket",
                    str(self.output / "preflight.json"),
                    "--expected-lock-device",
                    str(preflight["lock_identity"]["device"]),
                    "--expected-lock-inode",
                    str(preflight["lock_identity"]["inode"]),
                    "--expected-owner-device",
                    str(preflight["owner_identity"]["device"]),
                    "--expected-owner-inode",
                    str(preflight["owner_identity"]["inode"]),
                    "--expected-nonce-sha256",
                    preflight["lock_nonce_sha256"],
                ]
            )
        arguments.extend(extra)
        return subprocess.run(arguments, check=check, capture_output=True, text=True)

    def _preflight(self) -> None:
        self._run("preflight", check=True)

    def _pre_and_post(self) -> None:
        self._preflight()
        self._run("postflight", check=True)
