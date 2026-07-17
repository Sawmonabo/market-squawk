"""Closed adversarial inventory for the capture benchmark host gate."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import unittest

if __package__:
    from .capture_benchmark_evidence_io_test_support import (
        ADVERSARIAL_CASES,
        EXPECTED_ADVERSARIAL_CASE_COUNT,
        GATE,
        SECRET,
        HostGateFixture,
        benchmark_artifacts,
    )
else:
    from capture_benchmark_evidence_io_test_support import (
        ADVERSARIAL_CASES,
        EXPECTED_ADVERSARIAL_CASE_COUNT,
        GATE,
        SECRET,
        HostGateFixture,
        benchmark_artifacts,
    )


class ClosedHostGateInventoryTest(unittest.TestCase):
    def test_all_closed_adversarial_cases(self) -> None:
        self.assertEqual(len(ADVERSARIAL_CASES), EXPECTED_ADVERSARIAL_CASE_COUNT)
        self.assertEqual(len(set(ADVERSARIAL_CASES)), EXPECTED_ADVERSARIAL_CASE_COUNT)
        for case in ADVERSARIAL_CASES:
            with self.subTest(case=case):
                scenario = HostGateFixture(methodName="runTest")
                scenario.setUp()
                try:
                    self._exercise(scenario, case)
                finally:
                    scenario.tearDown()

    def _exercise(self, scenario: HostGateFixture, case: str) -> None:
        if case == "success_private_redacted":
            scenario._pre_and_post()
            self.assertNotIn(SECRET, (scenario.output / "comparison.json").read_text())
            return
        if case == "post_load_high_allowed":
            scenario.fixture_value["postflight"]["load_one_minute"] = 10_000.0
            scenario._write_fixture()
            scenario._pre_and_post()
            return
        pre_success = {
            "pre_load_zero_allowed": 0.0,
            "pre_load_boundary_allowed": 1.0,
        }
        if case in pre_success:
            scenario.fixture_value["preflight"]["load_one_minute"] = pre_success[case]
            scenario._write_fixture()
            scenario._preflight()
            return
        pre_field_failures = {
            "pre_load_one_over_rejected": ("load_one_minute", 1.000_001),
            "negative_load_rejected": ("load_one_minute", -0.1),
        }
        if case in pre_field_failures:
            field, value = pre_field_failures[case]
            scenario.fixture_value["preflight"][field] = value
            return self._fixture_preflight_failure(scenario)
        cpu_values = {
            "logical_cpus_zero_rejected": 0,
            "logical_cpus_missing_rejected": None,
            "logical_cpus_wrong_type_rejected": "ten",
        }
        if case in cpu_values:
            if cpu_values[case] is None:
                del scenario.fixture_value["preflight"]["host"]["logical_cpus"]
            else:
                scenario.fixture_value["preflight"]["host"]["logical_cpus"] = cpu_values[case]
            return self._fixture_preflight_failure(scenario)
        if case == "clock_drift_exact_boundary_allowed":
            scenario.fixture_value["postflight"]["wall_ns"] += 2_000_000_000
            scenario._write_fixture()
            scenario._pre_and_post()
            return
        post_scalar_mutations = {
            "clock_drift_one_over_rejected": ("wall_ns", 12_000_000_001),
            "wall_clock_backwards_rejected": ("wall_ns", -1),
            "monotonic_clock_backwards_rejected": ("monotonic_ns", -1),
        }
        if case in post_scalar_mutations:
            field, delta = post_scalar_mutations[case]
            scenario.fixture_value["postflight"][field] = (
                scenario.fixture_value["preflight"][field] + delta
            )
            return self._fixture_postflight_failure(scenario)
        host_mutations = {
            "boot_id_drift_rejected": ("boot_id", "other-boot"),
            "machine_drift_rejected": ("machine", "other-machine"),
            "node_drift_rejected": ("node", "other-node"),
            "affinity_drift_rejected": ("affinity", [0]),
            "scheduler_drift_rejected": ("scheduler", 2),
            "nice_drift_rejected": ("nice", 1),
            "power_drift_rejected": ("power_hash", "c" * 64),
            "thermal_drift_rejected": ("thermal_hash", "d" * 64),
        }
        if case in host_mutations:
            field, value = host_mutations[case]
            scenario.fixture_value["postflight"]["host"][field] = value
            return self._fixture_postflight_failure(scenario)
        toolchain_mutations = {
            "rustc_drift_rejected": ("rustc", "different-rustc"),
            "cargo_toolchain_drift_rejected": ("cargo", "different-cargo"),
            "target_drift_rejected": ("target", "different-target"),
            "release_profile_drift_rejected": ("release_profile", "different-profile"),
        }
        if case in toolchain_mutations:
            field, value = toolchain_mutations[case]
            scenario.fixture_value["postflight"]["toolchain"][field] = value
            return self._fixture_postflight_failure(scenario)
        if case == "git_head_drift_rejected":
            scenario.fixture_value["postflight"]["measured_code_head"] = "2" * 40
            return self._fixture_postflight_failure(scenario)
        competitor_cases = {
            "pre_competing_cargo_rejected": ("cargo", "cargo test"),
            "pre_competing_rustc_rejected": ("rustc", "rustc crate.rs"),
            "pre_competing_evidence_runner_rejected": (
                "capture",
                "capture_admission_evidence --bench",
            ),
            "pre_competing_criterion_rejected": (
                "criterion",
                "capture_admission_criterion --bench",
            ),
        }
        if case in competitor_cases:
            comm, argv = competitor_cases[case]
            scenario.fixture_value["preflight"]["processes"].append(
                {"pid": 999, "ppid": 1, "state": "R", "comm": comm, "argv": argv}
            )
            return self._fixture_preflight_failure(scenario)
        if case == "post_competing_cargo_rejected":
            scenario.fixture_value["postflight"]["processes"].append(
                {"pid": 999, "ppid": 1, "state": "R", "comm": "cargo", "argv": "cargo test"}
            )
            return self._fixture_postflight_failure(scenario)
        if case == "secret_other_process_redacted":
            scenario._pre_and_post()
            self.assertNotIn(SECRET, (scenario.output / "preflight.json").read_text())
            return
        if case == "secret_competitor_not_emitted":
            scenario.fixture_value["preflight"]["processes"].append(
                {
                    "pid": 999,
                    "ppid": 1,
                    "state": "R",
                    "comm": "cargo",
                    "argv": f"cargo test --token={SECRET}",
                }
            )
            scenario._write_fixture()
            result = scenario._run("preflight")
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn(SECRET, result.stdout + result.stderr)
            return
        if case == "attestation_content_rejected":
            scenario._write_private(scenario.attestation, b"invalid\n")
            return self._preflight_failure(scenario)
        if case == "attestation_post_swap_rejected":
            scenario._preflight()
            scenario._write_private(scenario.attestation, b"invalid\n")
            return self._postflight_failure(scenario)
        if case in {"attestation_symlink_rejected", "fixture_symlink_rejected"}:
            path = scenario.attestation if case.startswith("attestation") else scenario.fixture
            target = path.with_name(path.name + ".real")
            path.rename(target)
            path.symlink_to(target)
            return self._preflight_failure(scenario)
        if case in {"attestation_permissive_mode_rejected", "fixture_permissive_mode_rejected"}:
            path = scenario.attestation if case.startswith("attestation") else scenario.fixture
            os.chmod(path, 0o644)
            return self._preflight_failure(scenario)
        if case == "output_existing_rejected":
            scenario.output.mkdir(mode=0o700)
            return self._preflight_failure(scenario)
        if case == "output_final_symlink_rejected":
            alternate = scenario.root / "alternate-output"
            alternate.mkdir(mode=0o700)
            scenario.output.symlink_to(alternate, target_is_directory=True)
            return self._preflight_failure(scenario)
        if case == "output_parent_symlink_rejected":
            alternate = scenario.root / "alternate-parent"
            alternate.mkdir(mode=0o700)
            linked = scenario.root / "linked-parent"
            linked.symlink_to(alternate, target_is_directory=True)
            scenario.output = linked / "host-gate"
            return self._preflight_failure(scenario)
        if case in {"lock_symlink_rejected", "lock_non_directory_rejected"}:
            scenario.lock.rmdir()
            if case == "lock_symlink_rejected":
                target = scenario.root / "other-lock"
                target.mkdir(mode=0o700)
                scenario.lock.symlink_to(target, target_is_directory=True)
            else:
                scenario._write_private(scenario.lock, b"not-directory")
            return self._preflight_failure(scenario)
        if case in {"lock_permissive_mode_rejected", "run_parent_permissive_mode_rejected"}:
            os.chmod(scenario.lock if case.startswith("lock") else scenario.run, 0o755)
            return self._preflight_failure(scenario)
        if case == "fixture_missing_rejected":
            scenario.fixture.unlink()
            return self._preflight_failure(scenario)
        if case == "fixture_truncated_rejected":
            scenario._write_private(scenario.fixture, b'{"preflight":')
            return self._preflight_failure(scenario)
        if case == "fixture_oversized_rejected":
            scenario._write_private(scenario.fixture, b"x" * (1024 * 1024 + 1))
            return self._preflight_failure(scenario)
        if case == "fixture_unknown_top_level_rejected":
            scenario.fixture_value["unknown"] = {}
            return self._fixture_preflight_failure(scenario)
        if case == "fixture_unknown_phase_rejected":
            scenario.fixture_value["preflight"]["unknown"] = True
            return self._fixture_preflight_failure(scenario)
        if case == "fixture_duplicate_json_member_rejected":
            scenario._write_private(
                scenario.fixture,
                b'{"preflight":{},"preflight":{},"postflight":{}}\n',
            )
            return self._preflight_failure(scenario)
        process_mutations = {
            "process_unknown_member_rejected": ("unknown", True),
            "process_argv_over_bound_rejected": ("argv", "x" * (16 * 1024 + 1)),
            "process_comm_over_bound_rejected": ("comm", "x" * 257),
            "process_state_over_bound_rejected": ("state", "x" * 17),
            "process_pid_zero_rejected": ("pid", 0),
            "process_ppid_negative_rejected": ("ppid", -1),
        }
        if case in process_mutations:
            field, value = process_mutations[case]
            scenario.fixture_value["preflight"]["processes"][0][field] = value
            return self._fixture_preflight_failure(scenario)
        owner_field_mutations = {
            "owner_nonce_swap_rejected": ("nonce", "f" * 64),
            "owner_lock_device_swap_rejected": ("lock_dev", -1),
            "owner_lock_inode_swap_rejected": ("lock_ino", -1),
            "owner_device_swap_rejected": ("owner_dev", -1),
            "owner_inode_field_swap_rejected": ("owner_ino", -1),
        }
        if case in owner_field_mutations:
            scenario._preflight()
            field, value = owner_field_mutations[case]
            self._rewrite_owner(scenario, field, value)
            return self._postflight_failure(scenario)
        if case == "owner_hardlink_rejected":
            scenario._preflight()
            os.link(scenario.lock / "owner.json", scenario.lock / "owner-link.json")
            return self._postflight_failure(scenario)
        if case == "owner_inode_replacement_rejected":
            scenario._preflight()
            owner = json.loads((scenario.lock / "owner.json").read_text())
            replacement = scenario.lock / "replacement"
            scenario._write_private(replacement, (json.dumps(owner) + "\n").encode())
            os.replace(replacement, scenario.lock / "owner.json")
            return self._postflight_failure(scenario)
        if case == "lock_inode_replacement_rejected":
            scenario._preflight()
            old = scenario.root / "old-lock"
            scenario.lock.rename(old)
            scenario.lock.mkdir(mode=0o700)
            (old / "owner.json").rename(scenario.lock / "owner.json")
            return self._postflight_failure(scenario)
        if case == "second_postflight_no_clobber":
            scenario._pre_and_post()
            before = (scenario.output / "postflight.json").read_bytes()
            self._postflight_failure(scenario)
            self.assertEqual((scenario.output / "postflight.json").read_bytes(), before)
            return
        injection_cases = {
            "preflight_file_fsync_failure_cleanup": "file-fsync",
            "preflight_dir_fsync_failure_cleanup": "dir-fsync",
            "interrupted_after_owner_cleanup": "after-owner",
        }
        if case in injection_cases:
            result = scenario._run(
                "preflight", extra=("--failure-injection", injection_cases[case])
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((scenario.lock / "owner.json").exists())
            return
        if case == "postflight_file_fsync_failure_preserves_preflight":
            scenario._preflight()
            result = scenario._run(
                "postflight", extra=("--failure-injection", "file-fsync")
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue((scenario.output / "preflight.json").exists())
            return
        if case == "interrupted_after_postflight_has_no_comparison":
            scenario._preflight()
            result = scenario._run(
                "postflight", extra=("--failure-injection", "after-postflight")
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue((scenario.output / "postflight.json").exists())
            self.assertFalse((scenario.output / "comparison.json").exists())
            return
        if case == "release_exact_owner_success":
            scenario._preflight()
            scenario._run("release", check=True)
            self.assertFalse(scenario.lock.exists())
            return
        release_mutations = {
            "release_malformed_nonce_preserves_lock": ("nonce", "bad"),
            "release_device_mismatch_preserves_lock": ("owner_dev", -1),
        }
        if case in release_mutations:
            scenario._preflight()
            field, value = release_mutations[case]
            self._rewrite_owner(scenario, field, value)
            self.assertNotEqual(scenario._run("release").returncode, 0)
            self.assertTrue(scenario.lock.exists())
            return
        if case == "release_inode_replacement_preserves_lock":
            scenario._preflight()
            owner = json.loads((scenario.lock / "owner.json").read_text())
            replacement = scenario.lock / "replacement"
            scenario._write_private(replacement, (json.dumps(owner) + "\n").encode())
            os.replace(replacement, scenario.lock / "owner.json")
            self.assertNotEqual(scenario._run("release").returncode, 0)
            self.assertTrue(scenario.lock.exists())
            return
        if case == "release_hardlink_preserves_lock":
            scenario._preflight()
            os.link(scenario.lock / "owner.json", scenario.lock / "owner-link.json")
            self.assertNotEqual(scenario._run("release").returncode, 0)
            self.assertTrue(scenario.lock.exists())
            return
        if case == "production_fixture_override_rejected":
            result = subprocess.run(
                [
                    str(GATE),
                    "preflight",
                    "--lock-dir",
                    str(scenario.lock),
                    "--active-agent-attestation",
                    str(scenario.attestation),
                    "--output-dir",
                    str(scenario.output),
                    "--observation-fixture",
                    str(scenario.fixture),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            return
        if case == "fixture_mode_missing_override_rejected":
            result = subprocess.run(
                [
                    str(GATE),
                    "preflight",
                    "--lock-dir",
                    str(scenario.lock),
                    "--evidence-mode",
                    "fixture",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            return
        if case == "missing_attestation_argument_rejected":
            result = subprocess.run(
                [
                    str(GATE),
                    "preflight",
                    "--lock-dir",
                    str(scenario.lock),
                    "--output-dir",
                    str(scenario.output),
                    "--evidence-mode",
                    "fixture",
                    "--observation-fixture",
                    str(scenario.fixture),
                    "--controlled-root",
                    str(scenario.root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            return
        primitive_injections = {
            "missing_platform_primitives_rejected": "missing-primitives",
            "root_open_identity_mismatch_rejected": "root-open-identity-mismatch",
            "post_read_identity_mismatch_rejected": "post-read-identity-mismatch",
        }
        if case in primitive_injections:
            result = scenario._run(
                "preflight", extra=("--failure-injection", primitive_injections[case])
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((scenario.lock / "owner.json").exists())
            return
        if case == "partial_descriptor_reads_succeed":
            scenario._run(
                "preflight",
                check=True,
                extra=("--failure-injection", "partial-descriptor-read"),
            )
            return
        owner_write_injections = {
            "partial_owner_write_cleanup": "partial-owner-write",
            "owner_write_failure_cleanup": "owner-write-failure",
        }
        if case in owner_write_injections:
            result = scenario._run(
                "preflight", extra=("--failure-injection", owner_write_injections[case])
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((scenario.lock / "owner.json").exists())
            self.assertEqual(list(scenario.lock.iterdir()), [])
            return
        if case == "output_identity_replacement_rejected":
            scenario._preflight()
            old = scenario.run / "old-host-gate"
            scenario.output.rename(old)
            scenario.output.mkdir(mode=0o700)
            (old / "preflight.json").rename(scenario.output / "preflight.json")
            return self._postflight_failure(scenario)
        if case == "attestation_same_content_inode_replacement_rejected":
            scenario._preflight()
            replacement = scenario.run / "replacement-attestation"
            scenario._write_private(replacement, b"no-other-active-agents\n")
            os.replace(replacement, scenario.attestation)
            return self._postflight_failure(scenario)
        if case == "self_consistent_owner_replacement_rejected":
            scenario._preflight()
            owner_path = scenario.lock / "owner.json"
            owner = json.loads(owner_path.read_text())
            replacement = scenario.lock / "replacement-owner"
            scenario._write_private(replacement, b"placeholder")
            metadata = replacement.stat()
            owner["owner_dev"] = metadata.st_dev
            owner["owner_ino"] = metadata.st_ino
            scenario._write_private(
                replacement, (json.dumps(owner, sort_keys=True) + "\n").encode()
            )
            os.replace(replacement, owner_path)
            return self._postflight_failure(scenario)
        owner_json_cases = {
            "owner_duplicate_json_rejected": b'{"schema_version":1,"schema_version":1}\n',
            "owner_truncated_json_rejected": b'{"schema_version":',
        }
        if case in owner_json_cases:
            scenario._preflight()
            scenario._write_private(scenario.lock / "owner.json", owner_json_cases[case])
            return self._postflight_failure(scenario)
        if case == "owner_unknown_json_rejected":
            scenario._preflight()
            owner_path = scenario.lock / "owner.json"
            owner = json.loads(owner_path.read_text())
            owner["unknown"] = True
            scenario._write_private(
                owner_path, (json.dumps(owner, sort_keys=True) + "\n").encode()
            )
            return self._postflight_failure(scenario)
        if case in {
            "preflight_duplicate_json_rejected",
            "preflight_truncated_json_rejected",
            "preflight_unknown_json_rejected",
        }:
            scenario._preflight()
            preflight = scenario.output / "preflight.json"
            if case == "preflight_duplicate_json_rejected":
                scenario._write_private(preflight, b'{"phase":"preflight","phase":"preflight"}\n')
            elif case == "preflight_truncated_json_rejected":
                scenario._write_private(preflight, b'{"phase":')
            else:
                value = json.loads(preflight.read_text())
                value["unknown"] = True
                scenario._write_private(
                    preflight, (json.dumps(value, sort_keys=True) + "\n").encode()
                )
            return self._postflight_failure(scenario)
        if case == "release_extra_ticket_member_preserves_lock":
            scenario._preflight()
            ticket = scenario.output / "preflight.json"
            value = json.loads(ticket.read_text())
            value["unknown"] = True
            scenario._write_private(ticket, (json.dumps(value, sort_keys=True) + "\n").encode())
            self.assertNotEqual(scenario._run("release").returncode, 0)
            self.assertTrue((scenario.lock / "owner.json").exists())
            return
        if case == "release_extra_lock_entry_preserves_owner":
            scenario._preflight()
            scenario._write_private(scenario.lock / "unexpected", b"unexpected")
            self.assertNotEqual(scenario._run("release").returncode, 0)
            self.assertTrue((scenario.lock / "owner.json").exists())
            return
        caller_mismatches = {
            "release_caller_lock_device_mismatch_preserves_lock": (
                "--expected-lock-device",
                "-1",
            ),
            "release_caller_lock_inode_mismatch_preserves_lock": (
                "--expected-lock-inode",
                "-1",
            ),
            "release_caller_owner_device_mismatch_preserves_lock": (
                "--expected-owner-device",
                "-1",
            ),
            "release_caller_owner_inode_mismatch_preserves_lock": (
                "--expected-owner-inode",
                "-1",
            ),
            "release_caller_nonce_mismatch_preserves_lock": (
                "--expected-nonce-sha256",
                "f" * 64,
            ),
        }
        if case in caller_mismatches:
            scenario._preflight()
            option, value = caller_mismatches[case]
            self.assertNotEqual(
                scenario._run("release", extra=(option, value)).returncode, 0
            )
            self.assertTrue((scenario.lock / "owner.json").exists())
            return
        if case.startswith("measure_"):
            return self._exercise_measure(scenario, case)
        if case == "production_failure_injection_rejected":
            result = subprocess.run(
                [
                    str(GATE),
                    "preflight",
                    "--lock-dir",
                    str(scenario.lock),
                    "--active-agent-attestation",
                    str(scenario.attestation),
                    "--output-dir",
                    str(scenario.output),
                    "--failure-injection",
                    "missing-primitives",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            return
        self.fail(f"unimplemented host-gate adversarial case: {case}")

    def _exercise_measure(self, scenario: HostGateFixture, case: str) -> None:
        runner = scenario.run / "capture_admission_evidence-exe"
        build_evidence = scenario.run / "build-evidence.json"
        bench_body = "/bin/sleep 0.2\nexit 0"
        if case == "measure_runner_failure_rejected":
            bench_body = "exit 19"
        elif case == "measure_runner_replace_restore_rejected":
            bench_body = """\
restore() {
  trap - EXIT TERM
  /bin/rm -f capture_admission_evidence-exe
  /bin/mv capture_admission_evidence-exe.saved capture_admission_evidence-exe
  exit 0
}
trap restore EXIT TERM
/bin/mv capture_admission_evidence-exe capture_admission_evidence-exe.saved
/bin/cp "$0" capture_admission_evidence-exe
/bin/chmod 700 capture_admission_evidence-exe
/bin/sleep 2
"""
        elif case == "measure_build_replace_restore_rejected":
            bench_body = """\
restore() {
  trap - EXIT TERM
  /bin/rm -f build-evidence.json
  /bin/mv build-evidence.json.saved build-evidence.json
  exit 0
}
trap restore EXIT TERM
/bin/mv build-evidence.json build-evidence.json.saved
printf '%s\n' '{"replacement":true}' > build-evidence.json
/bin/chmod 600 build-evidence.json
/bin/sleep 2
"""
        script, evidence = benchmark_artifacts(bench_body=bench_body)
        scenario._write_private(runner, script)
        os.chmod(runner, 0o700)
        scenario._write_private(build_evidence, evidence)
        supplied_runner = runner
        supplied_build = build_evidence
        if case == "measure_runner_path_mismatch_rejected":
            supplied_runner = scenario.run / "other-runner"
            scenario._write_private(supplied_runner, script)
            os.chmod(supplied_runner, 0o700)
        if case == "measure_build_path_mismatch_rejected":
            supplied_build = scenario.run / "other-build.json"
            scenario._write_private(supplied_build, b'{"fixture":true}\n')
        extra = (
            "--runner",
            str(supplied_runner),
            "--build-evidence",
            str(supplied_build),
        )
        if case == "measure_competitor_rejected":
            extra += ("--failure-injection", "monitor-competitor")
        result = scenario._run("measure", extra=extra)
        if case == "measure_success_binds_inputs_continuously":
            self.assertEqual(result.returncode, 0, result.stderr)
            comparison = json.loads((scenario.output / "comparison.json").read_text())
            monitor = json.loads((scenario.output / "monitor.json").read_text())
            preflight = json.loads((scenario.output / "preflight.json").read_text())
            postflight = json.loads((scenario.output / "postflight.json").read_text())
            self.assertTrue(comparison["continuous_monitor"])
            self.assertEqual(monitor["repetitions"], 5)
            self.assertEqual(len(monitor["repetition_checks"]), 5)
            self.assertEqual(preflight["measurement_inputs"], monitor["measurement_inputs"])
            self.assertEqual(postflight["measurement_inputs"], monitor["measurement_inputs"])
        else:
            self.assertNotEqual(result.returncode, 0)
        self.assertEqual(list(scenario.run.glob(".capture-execution-*")), [])

    def _fixture_preflight_failure(self, scenario: HostGateFixture) -> None:
        scenario._write_fixture()
        self._preflight_failure(scenario)

    def _fixture_postflight_failure(self, scenario: HostGateFixture) -> None:
        scenario._write_fixture()
        scenario._preflight()
        self._postflight_failure(scenario)

    def _preflight_failure(self, scenario: HostGateFixture) -> None:
        self.assertNotEqual(scenario._run("preflight").returncode, 0)

    def _postflight_failure(self, scenario: HostGateFixture) -> None:
        self.assertNotEqual(scenario._run("postflight").returncode, 0)

    def _rewrite_owner(self, scenario: HostGateFixture, field: str, value: object) -> None:
        path = scenario.lock / "owner.json"
        owner = json.loads(path.read_text())
        owner[field] = value
        scenario._write_private(path, (json.dumps(owner, sort_keys=True) + "\n").encode())


if __name__ == "__main__":
    unittest.main()

