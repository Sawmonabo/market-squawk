"""Deterministic adversarial tests for the capture benchmark host gate."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import stat
import subprocess
import time
import unittest
from copy import deepcopy

if __package__:
    from .capture_benchmark_evidence_io_test_support import (
        GATE,
        SECRET,
        HostGateFixture,
        benchmark_artifacts,
    )
else:
    from capture_benchmark_evidence_io_test_support import (
        GATE,
        SECRET,
        HostGateFixture,
        benchmark_artifacts,
    )


class HostGateTest(HostGateFixture):
    def test_success_is_private_canonical_redacted_and_releasable(self) -> None:
        self._pre_and_post()
        for name in ("preflight.json", "postflight.json", "comparison.json"):
            artifact = self.output / name
            self.assertEqual(stat.S_IMODE(artifact.stat().st_mode), 0o600)
            self.assertNotIn(SECRET, artifact.read_text())
        comparison = json.loads((self.output / "comparison.json").read_text())
        self.assertTrue(comparison["valid"])
        self.assertEqual(comparison["evidence_mode"], "fixture")
        self._run("release", check=True)
        self.assertFalse(self.lock.exists())

    def test_high_postflight_load_is_diagnostic_not_invalidation(self) -> None:
        self.fixture_value["postflight"]["load_one_minute"] = 1_000.0
        self._write_fixture()
        self._pre_and_post()

    def test_high_preflight_load_fails_and_removes_owner(self) -> None:
        self.fixture_value["preflight"]["load_one_minute"] = 2.0
        self._write_fixture()
        result = self._run("preflight")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.lock / "owner.json").exists())

    def test_competing_cargo_is_rejected_without_persisting_argv(self) -> None:
        self.fixture_value["preflight"]["processes"].append(
            {"pid": 202, "ppid": 1, "state": "R", "comm": "cargo", "argv": f"cargo test {SECRET}"}
        )
        self._write_fixture()
        result = self._run("preflight")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(SECRET, result.stdout + result.stderr)
        if self.output.exists():
            for artifact in self.output.iterdir():
                self.assertNotIn(SECRET, artifact.read_text())

    def test_host_fingerprint_change_invalidates_postflight(self) -> None:
        self.fixture_value["postflight"]["host"]["boot_id"] = "different-boot"
        self._write_fixture()
        self._preflight()
        self.assertNotEqual(self._run("postflight").returncode, 0)

    def test_wall_monotonic_drift_invalidates_postflight(self) -> None:
        self.fixture_value["postflight"]["wall_ns"] += 3_000_000_000
        self._write_fixture()
        self._preflight()
        self.assertNotEqual(self._run("postflight").returncode, 0)

    def test_owner_nonce_change_invalidates_postflight(self) -> None:
        self._preflight()
        owner_path = self.lock / "owner.json"
        owner = json.loads(owner_path.read_text())
        owner["nonce"] = "f" * 64
        self._write_private(owner_path, (json.dumps(owner, sort_keys=True) + "\n").encode())
        self.assertNotEqual(self._run("postflight").returncode, 0)

    def test_existing_output_directory_is_no_clobber_failure(self) -> None:
        self.output.mkdir(mode=0o700)
        self.assertNotEqual(self._run("preflight").returncode, 0)

    def test_symlinked_output_parent_is_rejected(self) -> None:
        alternate = self.root / "alternate"
        alternate.mkdir(mode=0o700)
        linked_parent = self.root / "linked-parent"
        linked_parent.symlink_to(alternate, target_is_directory=True)
        self.output = linked_parent / "host-gate"
        self.assertNotEqual(self._run("preflight").returncode, 0)

    def test_second_postflight_cannot_clobber_evidence(self) -> None:
        self._pre_and_post()
        original = (self.output / "postflight.json").read_bytes()
        self.assertNotEqual(self._run("postflight").returncode, 0)
        self.assertEqual((self.output / "postflight.json").read_bytes(), original)

    def test_production_mode_rejects_fixture_override(self) -> None:
        result = subprocess.run(
            [
                str(GATE),
                "preflight",
                "--lock-dir",
                str(self.lock),
                "--active-agent-attestation",
                str(self.attestation),
                "--output-dir",
                str(self.output),
                "--observation-fixture",
                str(self.fixture),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)

    def test_unknown_fixture_schema_is_rejected(self) -> None:
        invalid = deepcopy(self.fixture_value)
        invalid["preflight"]["unexpected"] = True
        self.fixture_value = invalid
        self._write_fixture()
        self.assertNotEqual(self._run("preflight").returncode, 0)

    def test_capability_identity_replacements_are_rejected(self) -> None:
        for case in ("attestation", "owner", "lock", "output"):
            with self.subTest(case=case):
                scenario = HostGateFixture(methodName="runTest")
                scenario.setUp()
                try:
                    scenario._preflight()
                    if case == "attestation":
                        replacement = scenario.run / "replacement-attestation"
                        scenario._write_private(
                            replacement, b"no-other-active-agents\n"
                        )
                        os.replace(replacement, scenario.attestation)
                    elif case == "owner":
                        owner_path = scenario.lock / "owner.json"
                        owner = json.loads(owner_path.read_text())
                        replacement = scenario.lock / "replacement-owner"
                        scenario._write_private(replacement, b"placeholder")
                        metadata = replacement.stat()
                        owner["owner_dev"] = metadata.st_dev
                        owner["owner_ino"] = metadata.st_ino
                        scenario._write_private(
                            replacement,
                            (json.dumps(owner, sort_keys=True) + "\n").encode(),
                        )
                        os.replace(replacement, owner_path)
                    elif case == "lock":
                        previous = scenario.root / "previous-lock"
                        scenario.lock.rename(previous)
                        scenario.lock.mkdir(mode=0o700)
                        (previous / "owner.json").rename(
                            scenario.lock / "owner.json"
                        )
                    else:
                        previous = scenario.run / "previous-output"
                        scenario.output.rename(previous)
                        scenario.output.mkdir(mode=0o700)
                        (previous / "preflight.json").rename(
                            scenario.output / "preflight.json"
                        )

                    self.assertNotEqual(
                        scenario._run("postflight").returncode,
                        0,
                    )
                finally:
                    scenario.tearDown()

    def test_release_identity_mismatch_preserves_owner_lock(self) -> None:
        self._preflight()

        result = self._run(
            "release",
            extra=("--expected-nonce-sha256", "f" * 64),
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertTrue((self.lock / "owner.json").exists())

    def test_candidate_measure_binds_baselines_and_launches_candidate_contract(self) -> None:
        manifest = b'{"fixture":"manifest"}\n'
        lock = b'{"fixture":"lock"}\n'
        manifest_path = self.run / "baseline-manifest.json"
        lock_path = self.run / "baseline-lock.json"
        self._write_private(manifest_path, manifest)
        self._write_private(lock_path, lock)
        bench_body = """\
test "$CAPTURE_BENCH_BACKEND" = candidate || exit 71
test "$CAPTURE_BENCH_EXPECTED_FIXTURES" = \
matrix,comparable_full,forced_lock,sustained_rss || exit 72
test "$CAPTURE_BENCH_BASELINE_MANIFEST" = "$CAPTURE_BENCH_OUTPUT/baseline-manifest.json" || exit 73
test "$CAPTURE_BENCH_BASELINE_LOCK" = "$CAPTURE_BENCH_OUTPUT/baseline-lock.json" || exit 74
printf '%s\n' "$CAPTURE_BENCH_REPETITION" >> candidate-launches.txt
/bin/sleep 0.2
exit 0
"""
        script, evidence = benchmark_artifacts(
            backend="candidate",
            bench_body=bench_body,
            baseline_manifest_sha256=hashlib.sha256(manifest).hexdigest(),
            baseline_lock_sha256=hashlib.sha256(lock).hexdigest(),
        )
        runner = self.run / "capture_admission_evidence-exe"
        build_evidence = self.run / "build-evidence.json"
        self._write_private(runner, script)
        os.chmod(runner, 0o700)
        self._write_private(build_evidence, evidence)
        for phase in ("preflight", "postflight"):
            self.fixture_value[phase]["measured_code_head"] = "2" * 40
        self._write_fixture()

        result = self._run(
            "measure",
            extra=("--runner", str(runner), "--build-evidence", str(build_evidence)),
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.run / "candidate-launches.txt").read_text(), "1\n2\n3\n4\n5\n")
        monitor = json.loads((self.output / "monitor.json").read_text())
        self.assertEqual(monitor["benchmark_backend"], "candidate")
        self.assertEqual(monitor["baseline_manifest_sha256"], hashlib.sha256(manifest).hexdigest())
        self.assertEqual(monitor["baseline_lock_sha256"], hashlib.sha256(lock).hexdigest())
        self.assertEqual(monitor["maximum_samples"], 100_000)
        self.assertEqual(monitor["sample_interval_ns"], 250_000_000)
        self.assertIsNone(monitor["process_observer"])
        self.assertEqual(monitor["process_observer_invocations"], 0)
        self.assertEqual(monitor["process_observer_sha256"], "0" * 64)
        self.assertLessEqual(monitor["duration_ns"], monitor["maximum_duration_ns"])
        self.assertEqual(
            set(monitor["measurement_inputs"]),
            {
                "runner",
                "execution_runner",
                "build_evidence",
                "baseline_manifest",
                "baseline_lock",
            },
        )

    def test_measure_rejects_and_terminates_background_descendant(self) -> None:
        bench_body = """\
printf '%s\n' "$$" > runner-pgid.txt
/bin/sleep 60 &
printf '%s\n' "$!" > descendant-pid.txt
exit 0
"""
        script, evidence = benchmark_artifacts(bench_body=bench_body)
        runner = self.run / "capture_admission_evidence-exe"
        build_evidence = self.run / "build-evidence.json"
        self._write_private(runner, script)
        os.chmod(runner, 0o700)
        self._write_private(build_evidence, evidence)

        result = self._run(
            "measure",
            extra=("--runner", str(runner), "--build-evidence", str(build_evidence)),
        )

        self.assertNotEqual(result.returncode, 0)
        process_group_id = int((self.run / "runner-pgid.txt").read_text())
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                os.killpg(process_group_id, 0)
            except ProcessLookupError:
                break
            time.sleep(0.01)
        else:
            os.killpg(process_group_id, signal.SIGKILL)
            self.fail("the rejected runner process group survived host-gate cleanup")
        self.assertEqual(list(self.run.glob(".capture-execution-*")), [])

    def test_measurement_inputs_remain_continuously_bound(self) -> None:
        runner_replacement = """\
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
        build_replacement = """\
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
        cases = (
            ("success", "/bin/sleep 0.2\nexit 0", None, True),
            ("competitor", "/bin/sleep 0.2\nexit 0", "monitor-competitor", False),
            ("runner-replacement", runner_replacement, None, False),
            ("build-replacement", build_replacement, None, False),
        )
        for case, bench_body, injection, succeeds in cases:
            with self.subTest(case=case):
                scenario = HostGateFixture(methodName="runTest")
                scenario.setUp()
                try:
                    runner = scenario.run / "capture_admission_evidence-exe"
                    build_evidence = scenario.run / "build-evidence.json"
                    script, evidence = benchmark_artifacts(bench_body=bench_body)
                    scenario._write_private(runner, script)
                    os.chmod(runner, 0o700)
                    scenario._write_private(build_evidence, evidence)
                    extra = (
                        "--runner",
                        str(runner),
                        "--build-evidence",
                        str(build_evidence),
                    )
                    if injection is not None:
                        extra += ("--failure-injection", injection)

                    result = scenario._run("measure", extra=extra)

                    if succeeds:
                        self.assertEqual(result.returncode, 0, result.stderr)
                        comparison = json.loads(
                            (scenario.output / "comparison.json").read_text()
                        )
                        monitor = json.loads(
                            (scenario.output / "monitor.json").read_text()
                        )
                        preflight = json.loads(
                            (scenario.output / "preflight.json").read_text()
                        )
                        postflight = json.loads(
                            (scenario.output / "postflight.json").read_text()
                        )
                        self.assertTrue(comparison["continuous_monitor"])
                        self.assertEqual(
                            preflight["measurement_inputs"],
                            monitor["measurement_inputs"],
                        )
                        self.assertEqual(
                            postflight["measurement_inputs"],
                            monitor["measurement_inputs"],
                        )
                    else:
                        self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(
                        list(scenario.run.glob(".capture-execution-*")),
                        [],
                    )
                finally:
                    scenario.tearDown()




if __name__ == "__main__":
    unittest.main()
