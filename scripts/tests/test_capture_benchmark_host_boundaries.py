"""Focused contracts for capture-benchmark host boundary modules."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY / "scripts"))
import capture_benchmark_host_execution as execution  # noqa: E402
import capture_benchmark_host_measured as measured  # noqa: E402
import capture_benchmark_host_observation as observation  # noqa: E402
import capture_benchmark_host_schema as schema  # noqa: E402
from capture_benchmark_process import GateError  # noqa: E402
from capture_benchmark_evidence_io import CapabilityRoot  # noqa: E402


def available_tool(name: str, index: int) -> dict:
    return {
        "state": "available",
        "path": f"/fixture/bin/{name}",
        "device": 1,
        "inode": index + 1,
        "size": 1024 + index,
        "uid": os.getuid(),
        "mode": 0o500,
        "nlink": 1,
        "flags": 0,
        "execution_strategy": "ephemeral-copy",
        "sha256": f"{index + 1:x}" * 64,
    }


def unavailable_tool(reason: str = "not-required-on-platform") -> dict:
    return {"state": "unavailable", "reason": reason}


def tool_execution(name: str, identity: dict) -> dict:
    executed = {
        field: identity[field]
        for field in schema.EXECUTED_IDENTITY_FIELDS
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


def raw_observation(head: str) -> dict:
    tools = {
        name: available_tool(name, index)
        for index, name in enumerate(("cargo", "git", "ps", "python", "rustc"))
    }
    tools.update(
        {
            "pmset": unavailable_tool(),
            "rustup": unavailable_tool("not-used-for-direct-toolchain"),
            "sysctl": unavailable_tool(),
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
            "release_profile": schema.PROFILE,
            "tool_identities": tools,
        },
        "measured_code_head": head,
        "tool_executions": [
            tool_execution(name, tools[name])
            for name in ("cargo", "git", "ps", "python", "rustc")
        ],
        "load_one_minute": 0.5,
        "processes": [
            {"pid": 101, "ppid": 1, "state": "S", "comm": "fixture", "argv": "fixture"}
        ],
        "wall_ns": 1_000_000_000,
        "monotonic_ns": 500_000_000,
    }


def build_evidence_value(backend: str) -> dict:
    digest = "a" * 64
    candidate = backend == "candidate"
    value = {
        "schema_version": measured.RUNNER_SCHEMA_VERSION,
        "runner": "capture_admission_evidence",
        "evidence_mode": "diagnostic_fixed_quota",
        "evidence_backend": backend,
        "queue_transport": "candidate_fixed_ring" if candidate else "standard_sync_channel",
        "queue_private_storage_accounting": "exact" if candidate else "not_measured",
        "cargo_target": "capture-admission-evidence",
        "benchmark_feature": "capture-benchmark",
        "build_profile": schema.PROFILE,
        "measured_code_head": ("2" if candidate else "1") * 40,
        "clean_build_enforced": True,
        "build_command": measured.BUILD_COMMAND,
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
        "executable_sha256": "b" * 64,
        "cargo_json_path": "./capture-bench-build.json",
        "cargo_json_sha256": digest,
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
        "baseline_lock_sha256": "c" * 64 if candidate else None,
        "baseline_manifest_path": "./baseline-manifest.json" if candidate else None,
        "baseline_manifest_sha256": "d" * 64 if candidate else None,
        "baseline_measured_code_head": "1" * 40 if candidate else None,
    }
    return value


class ObservationContractTest(unittest.TestCase):
    def test_code_head_is_separate_from_the_toolchain_fingerprint(self) -> None:
        baseline = observation.normalize_observation(raw_observation("1" * 40), "fixture")
        candidate = observation.normalize_observation(raw_observation("2" * 40), "fixture")
        self.assertNotEqual(baseline["measured_code_head"], candidate["measured_code_head"])
        self.assertEqual(
            baseline["toolchain_fingerprint_sha256"],
            candidate["toolchain_fingerprint_sha256"],
        )
        self.assertEqual(baseline["stable_toolchain"], candidate["stable_toolchain"])
        with self.assertRaises(GateError):
            schema.validate_phase_stability(baseline, candidate)

    def test_normalization_requires_the_build_bound_head(self) -> None:
        with self.assertRaises(GateError):
            observation.normalize_observation(
                raw_observation("1" * 40),
                "fixture",
                expected_measured_code_head="2" * 40,
            )

    def test_tool_identity_schema_is_typed_and_exact(self) -> None:
        self.assertTrue(schema.validate_tool_identity(available_tool("rustc", 1)))
        self.assertTrue(schema.validate_tool_identity(unavailable_tool()))
        self.assertFalse(schema.validate_tool_identity({"state": "available", "version": "x"}))
        self.assertFalse(schema.validate_tool_identity({"state": "unavailable"}))
        self.assertFalse(
            schema.validate_tool_identity(
                {"state": "unavailable", "reason": "not-required-on-platform", "path": None}
            )
        )
        wrong_owner = available_tool("rustc", 1)
        wrong_owner["uid"] = os.getuid() + 10_000
        self.assertFalse(schema.validate_tool_identity(wrong_owner))
        hardlinked = available_tool("rustc", 1)
        hardlinked["nlink"] = 2
        self.assertFalse(schema.validate_tool_identity(hardlinked))

    def test_execution_receipts_reject_missing_extra_and_source_only_records(self) -> None:
        raw = raw_observation("1" * 40)
        identities = raw["toolchain"]["tool_identities"]
        self.assertTrue(schema.validate_tool_executions(raw["tool_executions"], identities))
        self.assertFalse(
            schema.validate_tool_executions(raw["tool_executions"][:-1], identities)
        )
        self.assertFalse(
            schema.validate_tool_executions(
                [*raw["tool_executions"], raw["tool_executions"][0]], identities
            )
        )
        source_only = json.loads(json.dumps(raw["tool_executions"]))
        source_only[0]["executed"]["path"] = identities["cargo"]["path"]
        self.assertFalse(schema.validate_tool_executions(source_only, identities))
        false_immutable = json.loads(json.dumps(raw["tool_executions"]))
        false_immutable[0]["protection"] = "user-immutable-flag"
        false_immutable[0]["executed"]["flags"] = 0
        self.assertFalse(schema.validate_tool_executions(false_immutable, identities))

    def test_same_uid_attestation_rejects_a_stale_process_interval_binding(self) -> None:
        phase = observation.normalize_observation(raw_observation("1" * 40), "fixture")
        phase.update(
            {
                "phase": "preflight",
                "lock_nonce_sha256": "a" * 64,
                "lock_identity": {"device": 1, "inode": 11},
                "owner_identity": {"device": 1, "inode": 12},
                "output_identity": {"device": 1, "inode": 13},
                "measurement_inputs": None,
            }
        )
        schema.bind_active_agent_attestation(
            phase, {"device": 1, "inode": 14, "sha256": "b" * 64}
        )
        schema.validate_phase_evidence(phase, "preflight")
        stale = json.loads(json.dumps(phase))
        stale["processes"][0]["comm"] = "changed-after-attestation"
        stale["process_inventory_sha256"] = schema.digest_json(stale["processes"])
        with self.assertRaises(GateError):
            schema.validate_phase_evidence(stale, "preflight")

    def test_root_owned_executable_rejects_an_unsafe_high_ancestor(self) -> None:
        path = Path("/usr/bin/git")
        if not path.exists():
            self.skipTest("system Git executable is unavailable")
        real_lstat = os.lstat

        def unsafe_lstat(selected: object, *args: object, **kwargs: object) -> os.stat_result:
            metadata = real_lstat(selected, *args, **kwargs)
            if Path(os.fsdecode(selected)) == Path("/usr"):
                changed = list(metadata)
                changed[0] |= stat.S_IWOTH
                return os.stat_result(changed)
            return metadata

        with mock.patch.object(observation.os, "lstat", side_effect=unsafe_lstat):
            with self.assertRaises(GateError):
                observation.executable_identity(path)

    def test_tool_resolution_does_not_consult_ambient_path(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fake_bin = Path(temporary)
            marker = fake_bin / "ambient-tool-ran"
            for name in schema.TOOL_NAMES:
                fake = fake_bin / name
                fake.write_text(
                    f"#!/bin/sh\nprintf ran > '{marker}'\nexit 99\n",
                    encoding="utf-8",
                )
                os.chmod(fake, 0o700)
            previous_path = os.environ.get("PATH")
            os.environ["PATH"] = str(fake_bin)
            try:
                identities = observation._production_tool_identities([])
            finally:
                if previous_path is None:
                    del os.environ["PATH"]
                else:
                    os.environ["PATH"] = previous_path
            self.assertFalse(marker.exists())
            self.assertTrue(
                all(
                    not identity.get("path", "").startswith(str(fake_bin))
                    for identity in identities.values()
                )
            )

    def test_executable_identity_binds_and_runs_the_exact_resolved_python(self) -> None:
        identity = observation.executable_identity(
            Path(sys.executable), execution_strategy="current-process"
        )
        resolved = Path(sys.executable).resolve(strict=True)
        self.assertEqual(identity["state"], "available")
        self.assertEqual(identity["path"], str(resolved))
        self.assertIn(identity["uid"], {0, os.getuid()})
        self.assertEqual(identity["mode"], stat.S_IMODE(resolved.stat().st_mode))
        self.assertEqual(identity["nlink"], resolved.stat().st_nlink)
        self.assertEqual(identity["execution_strategy"], "current-process")
        self.assertEqual(identity["sha256"], hashlib.sha256(resolved.read_bytes()).hexdigest())
        with self.assertRaises(GateError):
            observation.run_bound_tool(identity, ["-c", "print('bound-tool')"])

    def test_bound_tool_preserves_an_explicit_proxy_invocation_role(self) -> None:
        identity = observation.executable_identity(Path("/bin/sh"))
        self.assertEqual(
            observation.run_bound_tool(
                identity,
                ["-c", "printf '%s' \"$0\""],
                invocation_name="cargo",
            ),
            "cargo",
        )

    def test_toolchain_resolver_accepts_symlinked_and_hardlinked_rustup_proxies(
        self,
    ) -> None:
        for layout in ("symlink", "hardlink"):
            with self.subTest(layout=layout), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                bin_dir = root / "bin"
                toolchain_dir = root / "toolchain"
                bin_dir.mkdir(mode=0o700)
                toolchain_dir.mkdir(mode=0o700)
                direct = {}
                for name in ("cargo", "rustc"):
                    direct[name] = toolchain_dir / name
                    direct[name].write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
                    direct[name].chmod(0o500)
                rustup = bin_dir / "rustup"
                rustup.write_text(
                    "#!/bin/sh\nprintf '%s\\n' "
                    f'"{toolchain_dir}/$2"\n',
                    encoding="utf-8",
                )
                rustup.chmod(0o500)
                for name in ("cargo", "rustc"):
                    proxy = bin_dir / name
                    if layout == "symlink":
                        proxy.symlink_to(rustup.name)
                    else:
                        os.link(rustup, proxy)
                paths = {
                    "cargo": bin_dir / "cargo",
                    "rustc": bin_dir / "rustc",
                    "rustup": rustup,
                }
                records: list[dict] = []
                with mock.patch.object(
                    observation,
                    "_closed_tool_path",
                    side_effect=lambda name: paths[name],
                ):
                    identities = observation._resolve_toolchain_identities(records)
                self.assertEqual(
                    Path(identities["cargo"]["path"]), direct["cargo"].resolve()
                )
                self.assertEqual(
                    Path(identities["rustc"]["path"]), direct["rustc"].resolve()
                )
                self.assertEqual(
                    identities["rustup"]["nlink"],
                    1 if layout == "symlink" else 3,
                )
                self.assertEqual(
                    [record["tool"] for record in records], ["rustup", "rustup"]
                )

    def test_production_observation_binds_real_toolchain_executables(self) -> None:
        raw = observation.production_observation()
        identities = raw["toolchain"]["tool_identities"]
        self.assertTrue(schema.validate_tool_executions(raw["tool_executions"], identities))
        normalized = observation.normalize_observation(raw, schema.PRODUCTION_MODE)
        self.assertEqual(
            normalized["tool_execution_sha256"],
            schema.digest_json(raw["tool_executions"]),
        )
        self.assertEqual(Path(identities["cargo"]["path"]).name, "cargo")
        self.assertEqual(Path(identities["rustc"]["path"]).name, "rustc")
        if identities["rustup"]["state"] == "available":
            self.assertNotEqual(identities["cargo"]["sha256"], identities["rustup"]["sha256"])
            self.assertNotEqual(identities["rustc"]["sha256"], identities["rustup"]["sha256"])
        by_tool = {}
        for execution in raw["tool_executions"]:
            by_tool.setdefault(execution["tool"], []).append(execution)
        for tool in ("cargo", "rustc", "rustup"):
            with self.subTest(tool=tool):
                self.assertGreaterEqual(len(by_tool[tool]), 1)
                for execution in by_tool[tool]:
                    self.assertEqual(execution["strategy"], "ephemeral-copy")
                    self.assertEqual(
                        execution["executed"]["sha256"], identities[tool]["sha256"]
                    )
                    self.assertNotEqual(
                        execution["executed"]["path"], identities[tool]["path"]
                    )
                    self.assertTrue(execution["path_removed"])
                    self.assertFalse(Path(execution["executed"]["path"]).exists())

    def test_failed_bound_tool_execution_removes_its_ephemeral_copy(self) -> None:
        records: list[dict] = []
        identities = observation._production_tool_identities(records)
        captured: list[Path] = []
        original = observation._create_ephemeral_execution

        def capture(identity: dict) -> dict:
            binding = original(identity)
            captured.append(binding["path"])
            return binding

        with mock.patch.object(observation, "_create_ephemeral_execution", side_effect=capture):
            with self.assertRaises(GateError):
                observation.run_bound_tool(identities["cargo"], ["--invalid-bound-option"])
        self.assertTrue(captured)
        self.assertEqual([path for path in captured if path.exists()], [])

    def test_prelaunch_identity_failure_still_removes_the_ephemeral_copy(self) -> None:
        records: list[dict] = []
        identities = observation._production_tool_identities(records)
        captured: list[Path] = []
        original_create = observation._create_ephemeral_execution

        def capture(identity: dict) -> dict:
            binding = original_create(identity)
            captured.append(binding["path"])
            return binding

        with (
            mock.patch.object(
                observation, "_create_ephemeral_execution", side_effect=capture
            ),
            mock.patch.object(
                observation,
                "_require_ephemeral_execution",
                side_effect=GateError("injected identity disagreement"),
            ),
        ):
            with self.assertRaises(GateError):
                observation.run_bound_tool(identities["cargo"], ["-V"])
        self.assertTrue(captured)
        self.assertEqual([path for path in captured if path.exists()], [])

    def test_copy_creation_validation_failure_removes_the_ephemeral_copy(self) -> None:
        records: list[dict] = []
        identities = observation._production_tool_identities(records)
        source = Path(identities["cargo"]["path"])
        before = set(source.parent.glob(".market-squawk-bound-*"))

        def reject_identity(_path: Path, **_kwargs: object) -> dict:
            raise GateError("injected copied identity rejection")

        with self.assertRaises(GateError):
            execution.create_ephemeral_execution(identities["cargo"], reject_identity)
        self.assertEqual(set(source.parent.glob(".market-squawk-bound-*")), before)

    def test_unsupported_immutable_flag_uses_the_attested_uid_exclusion(self) -> None:
        records: list[dict] = []
        identities = observation._production_tool_identities(records)
        binding = None
        with (
            mock.patch.object(execution.stat, "UF_IMMUTABLE", 2, create=True),
            mock.patch.object(
                execution.os,
                "chflags",
                side_effect=OSError(errno.ENOTSUP, "fixture unsupported flag"),
                create=True,
            ),
        ):
            binding = execution.create_ephemeral_execution(
                identities["cargo"], observation.executable_identity
            )
        try:
            self.assertEqual(
                binding["protection"], "exclusive-current-uid-attestation"
            )
        finally:
            if binding is not None:
                execution.remove_ephemeral_execution(binding)

    def test_ephemeral_cleanup_attempts_both_descriptor_closes(self) -> None:
        binding = {
            "path": Path("/fixture/bin/.market-squawk-bound-" + "a" * 32),
            "name": ".market-squawk-bound-" + "a" * 32,
            "descriptor": 11,
            "directory": 12,
            "identity": {},
            "immutable": False,
        }
        close = mock.Mock(side_effect=(OSError("first close failed"), None))
        with (
            mock.patch.object(execution, "require_ephemeral_execution"),
            mock.patch.object(execution.os, "unlink"),
            mock.patch.object(execution, "sync_directory"),
            mock.patch.object(
                execution.os, "stat", side_effect=FileNotFoundError()
            ),
            mock.patch.object(execution.os, "close", close),
        ):
            with self.assertRaisesRegex(GateError, "cleanup failed"):
                execution.remove_ephemeral_execution(binding)
        self.assertEqual([call.args[0] for call in close.call_args_list], [11, 12])


class MeasurementContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root_path = Path(self.temporary.name).resolve()
        os.chmod(self.root_path, 0o700)
        self.root = CapabilityRoot.open(self.root_path)

    def tearDown(self) -> None:
        self.root.close()
        self.temporary.cleanup()

    def _contract(self, backend: str) -> measured.MeasurementContract:
        path = self._write_build_evidence(backend, build_evidence_value(backend))
        _binding, contract = measured.build_evidence_contract(self.root, path)
        return contract

    def _write_build_evidence(self, name: str, value: dict) -> Path:
        path = self.root_path / f"{name}.json"
        path.write_bytes(
            (
                json.dumps(value, sort_keys=True, separators=(",", ":"))
                + "\n"
            ).encode()
        )
        os.chmod(path, 0o600)
        return path

    def test_standard_and_candidate_environments_derive_from_build_evidence(self) -> None:
        standard = self._contract("standard")
        candidate = self._contract("candidate")
        standard_environment = standard.environment(
            self.root_path, self.root_path / "standard.json", 3
        )
        candidate_environment = candidate.environment(
            self.root_path, self.root_path / "candidate.json", 4
        )
        self.assertEqual(standard_environment["CAPTURE_BENCH_BACKEND"], "standard")
        self.assertEqual(
            standard_environment["CAPTURE_BENCH_EXPECTED_FIXTURES"],
            "matrix,comparable_full,sustained_rss",
        )
        self.assertNotIn("CAPTURE_BENCH_BASELINE_MANIFEST", standard_environment)
        self.assertNotIn("CAPTURE_BENCH_BASELINE_LOCK", standard_environment)
        self.assertEqual(candidate_environment["CAPTURE_BENCH_BACKEND"], "candidate")
        self.assertEqual(
            candidate_environment["CAPTURE_BENCH_EXPECTED_FIXTURES"],
            "matrix,comparable_full,forced_lock,sustained_rss",
        )
        self.assertEqual(
            candidate_environment["CAPTURE_BENCH_BASELINE_MANIFEST"],
            str(self.root_path / "baseline-manifest.json"),
        )
        self.assertEqual(
            candidate_environment["CAPTURE_BENCH_BASELINE_LOCK"],
            str(self.root_path / "baseline-lock.json"),
        )

    def test_measurement_requires_the_build_bound_toolchain(self) -> None:
        contract = self._contract("standard")
        identities = {
            name: {
                "state": "available",
                "sha256": contract.runner_bindings[f"{name}_executable_sha256"],
            }
            for name in ("cargo", "git", "rustc")
        }
        toolchain = {"tool_identities": identities}
        measured.verify_build_tool_identities(contract, toolchain)
        identities["rustc"]["sha256"] = "0" * 64
        with self.assertRaises(GateError):
            measured.verify_build_tool_identities(contract, toolchain)

    def test_invalid_build_evidence_is_rejected_before_launch(self) -> None:
        cases = (
            ("candidate-baseline", "candidate", "baseline_manifest_sha256", None),
            ("standard-storage", "standard", "queue_private_storage_accounting", "exact"),
            (
                "candidate-storage",
                "candidate",
                "queue_private_storage_accounting",
                "not_measured",
            ),
        )
        for name, backend, field, replacement in cases:
            with self.subTest(name=name):
                invalid = build_evidence_value(backend)
                invalid[field] = replacement
                path = self._write_build_evidence(name, invalid)
                with self.assertRaises(GateError):
                    measured.build_evidence_contract(self.root, path)

    def test_runner_disagreement_is_rejected_before_benchmark_launch(self) -> None:
        build_evidence = build_evidence_value("candidate")
        build_path = self.root_path / "build-evidence.json"
        build_path.write_bytes(
            (json.dumps(build_evidence, sort_keys=True, separators=(",", ":")) + "\n").encode()
        )
        os.chmod(build_path, 0o600)
        _binding, contract = measured.build_evidence_contract(self.root, build_path)

        disagreed = contract.runner_bindings.copy()
        disagreed["evidence_backend"] = "standard"
        marker = self.root_path / "bench-launched"
        runner = self.root_path / "runner"
        runner.write_text(
            "#!/bin/sh\n"
            "if [ \"${1-}\" = \"--print-build-bindings\" ]; then\n"
            f"  printf '%s\\n' '{json.dumps(disagreed, sort_keys=True, separators=(',', ':'))}'\n"
            "  exit 0\n"
            "fi\n"
            f"printf launched > '{marker}'\n",
            encoding="utf-8",
        )
        os.chmod(runner, 0o500)

        with self.assertRaises(GateError):
            measured.verify_execution_contract(runner, self.root_path, contract)
        self.assertFalse(marker.exists())

    def test_authority_binding_missing_or_mismatch_is_rejected(self) -> None:
        missing = build_evidence_value("standard")
        del missing["host_gate_execution_sha256"]
        missing_path = self._write_build_evidence("authority-missing", missing)
        with self.assertRaises(GateError):
            measured.build_evidence_contract(self.root, missing_path)

        mismatched = build_evidence_value("standard")
        mismatched["host_gate_execution_sha256"] = "f" * 64
        mismatch_path = self._write_build_evidence("authority-mismatch", mismatched)
        _binding, contract = measured.build_evidence_contract(self.root, mismatch_path)
        with self.assertRaises(GateError):
            measured.verify_authority_module_bindings(contract)


if __name__ == "__main__":
    unittest.main()
