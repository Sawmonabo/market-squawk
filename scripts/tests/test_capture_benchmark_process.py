"""Deterministic tests for bounded capture-benchmark subprocesses."""

from __future__ import annotations

import os
import io
import json
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import scripts.capture_benchmark_process as process_module
from scripts.capture_benchmark_process import (
    EXPECTED_PROCESS_GROUP_ENV,
    INHERIT_OUTER_POLICY,
    PROCESS_GROUP_POLICY_ENV,
    WRAPPER_ENV_KEYS,
    GateError,
    bounded_process,
    bounded_session_leader_exec,
)

REAL_SELECTOR = process_module.selectors.DefaultSelector


def bounded_environment() -> dict[str, str]:
    """Return the minimal environment required by the test subprocesses."""
    return {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}


class FailingSelector:
    """Delegate to a real selector while failing one requested registration."""

    def __init__(self, fail_registration: int | None = None) -> None:
        self.inner = REAL_SELECTOR()
        self.fail_registration = fail_registration
        self.registrations = 0
        self.closed = False

    def register(self, *args: object, **kwargs: object) -> object:
        self.registrations += 1
        if self.registrations == self.fail_registration:
            raise OSError(f"injected registration {self.registrations} failure")
        return self.inner.register(*args, **kwargs)

    def unregister(self, *args: object, **kwargs: object) -> object:
        return self.inner.unregister(*args, **kwargs)

    def select(self, *args: object, **kwargs: object) -> object:
        return self.inner.select(*args, **kwargs)

    def get_map(self) -> object:
        return self.inner.get_map()

    def close(self) -> None:
        self.closed = True
        self.inner.close()


class BoundedProcessTest(unittest.TestCase):
    def test_bound_session_exec_injects_the_exact_owned_process_group(self) -> None:
        program = (
            "import json, os; "
            "print(json.dumps({"
            "'pid': os.getpid(), 'pgid': os.getpgrp(), "
            f"'expected': os.environ[{EXPECTED_PROCESS_GROUP_ENV!r}], "
            f"'policy': os.environ[{PROCESS_GROUP_POLICY_ENV!r}], "
            f"'wrapper_keys_present': {WRAPPER_ENV_KEYS!r} in os.environ"
            "}, sort_keys=True))"
        )
        result = bounded_session_leader_exec(
            [sys.executable, "-c", program],
            executable=Path(sys.executable).resolve(strict=True),
            wrapper_python=Path(sys.executable).resolve(strict=True),
            env={
                PROCESS_GROUP_POLICY_ENV: INHERIT_OUTER_POLICY,
                "FIXTURE_TOKEN": "bound",
            },
            timeout_seconds=2,
            maximum_stdout=4096,
            maximum_stderr=4096,
        )
        observed = json.loads(result.stdout)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(observed["pid"], observed["pgid"])
        self.assertEqual(observed["expected"], str(observed["pgid"]))
        self.assertEqual(observed["policy"], INHERIT_OUTER_POLICY)
        self.assertFalse(observed["wrapper_keys_present"])

    def test_bound_session_exec_rejects_a_preseeded_expected_group(self) -> None:
        with mock.patch.object(process_module.subprocess, "Popen") as popen:
            with self.assertRaisesRegex(GateError, "contract is invalid"):
                bounded_session_leader_exec(
                    [sys.executable, "-c", "raise SystemExit(0)"],
                    executable=Path(sys.executable).resolve(strict=True),
                    wrapper_python=Path(sys.executable).resolve(strict=True),
                    env={
                        PROCESS_GROUP_POLICY_ENV: INHERIT_OUTER_POLICY,
                        EXPECTED_PROCESS_GROUP_ENV: "1",
                    },
                    timeout_seconds=2,
                    maximum_stdout=0,
                    maximum_stderr=0,
                )
        popen.assert_not_called()

    def test_success_preserves_streams_status_and_eof_stdin(self) -> None:
        program = (
            "import sys; "
            "data = sys.stdin.buffer.read(); "
            "sys.stdout.buffer.write(b'out'); "
            "sys.stderr.buffer.write(b'err'); "
            "raise SystemExit(17 if data else 7)"
        )

        result = bounded_process(
            [sys.executable, "-c", program],
            env=bounded_environment(),
            timeout_seconds=2,
            maximum_stdout=3,
            maximum_stderr=3,
        )

        self.assertEqual(result.returncode, 7)
        self.assertEqual(result.stdout, b"out")
        self.assertEqual(result.stderr, b"err")

    def test_contract_rejects_invalid_bounds(self) -> None:
        cases = (
            ([], 1.0, 0, 0),
            ([sys.executable], 0.0, 0, 0),
            ([sys.executable], -1.0, 0, 0),
            ([sys.executable], 1.0, -1, 0),
            ([sys.executable], 1.0, 0, -1),
        )

        for arguments, timeout, maximum_stdout, maximum_stderr in cases:
            with self.subTest(
                arguments=arguments,
                timeout=timeout,
                maximum_stdout=maximum_stdout,
                maximum_stderr=maximum_stderr,
            ):
                with self.assertRaisesRegex(GateError, "contract is invalid"):
                    bounded_process(
                        arguments,
                        env=bounded_environment(),
                        timeout_seconds=timeout,
                        maximum_stdout=maximum_stdout,
                        maximum_stderr=maximum_stderr,
                    )

    def test_live_stdout_and_stderr_caps_fail_closed(self) -> None:
        cases = (
            ("stdout", "import sys; sys.stdout.buffer.write(b'abc')"),
            ("stderr", "import sys; sys.stderr.buffer.write(b'abc')"),
        )

        for stream, program in cases:
            with self.subTest(stream=stream):
                with self.assertRaisesRegex(GateError, "output byte limit"):
                    bounded_process(
                        [sys.executable, "-c", program],
                        env=bounded_environment(),
                        timeout_seconds=2,
                        maximum_stdout=2 if stream == "stdout" else 0,
                        maximum_stderr=2 if stream == "stderr" else 0,
                    )

    def test_timeout_terminates_exited_leaders_pipe_inheriting_descendant(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            child_pid_path = Path(temporary) / "child.pid"
            command = [
                "/bin/sh",
                "-c",
                "/bin/sleep 30 & printf '%s\\n' \"$!\" > \"$1\"; exit 0",
                "exited-session-leader",
                str(child_pid_path),
            ]

            started = time.monotonic()
            try:
                with self.assertRaisesRegex(GateError, "time limit"):
                    bounded_process(
                        command,
                        env=bounded_environment(),
                        timeout_seconds=0.2,
                        maximum_stdout=0,
                        maximum_stderr=0,
                    )
                self.assertLess(time.monotonic() - started, 3.0)
                child_pid = int(child_pid_path.read_text().strip())
                self._assert_pid_gone(child_pid)
            finally:
                self._kill_recorded_pid(child_pid_path)

    def test_timeout_escalates_from_term_to_kill(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            pid_path = Path(temporary) / "leader.pid"
            program = (
                "import os, pathlib, signal, sys, time; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); "
                "time.sleep(30)"
            )
            try:
                with self.assertRaisesRegex(GateError, "time limit"):
                    bounded_process(
                        [sys.executable, "-c", program, str(pid_path)],
                        env=bounded_environment(),
                        timeout_seconds=0.2,
                        maximum_stdout=0,
                        maximum_stderr=0,
                    )
                pid = int(pid_path.read_text().strip())
                self._assert_pid_gone(pid)
            finally:
                self._kill_recorded_pid(pid_path)

    def test_outer_build_deadline_extinguishes_a_stalled_helper_spawn_tree(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            outer_pid_path = root / "cargo.pid"
            helper_pid_path = root / "build-helper.pid"
            leaf_pid_path = root / "inner-spawn.pid"
            helper = (
                "import os, pathlib, subprocess, sys, time; "
                "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); "
                "leaf = subprocess.Popen(['/bin/sleep', '30'], "
                "stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, "
                "stderr=subprocess.DEVNULL); "
                "pathlib.Path(sys.argv[2]).write_text(str(leaf.pid)); "
                "time.sleep(30)"
            )
            outer = (
                "import os, pathlib, subprocess, sys; "
                "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); "
                f"helper = subprocess.Popen([sys.executable, '-c', {helper!r}, "
                "sys.argv[2], sys.argv[3]]); helper.wait()"
            )
            paths = (outer_pid_path, helper_pid_path, leaf_pid_path)
            try:
                with self.assertRaisesRegex(GateError, "time limit"):
                    bounded_process(
                        [
                            sys.executable,
                            "-c",
                            outer,
                            str(outer_pid_path),
                            str(helper_pid_path),
                            str(leaf_pid_path),
                        ],
                        env=bounded_environment(),
                        timeout_seconds=1.0,
                        maximum_stdout=0,
                        maximum_stderr=0,
                    )
                self.assertEqual([path.name for path in paths if not path.exists()], [])
                for path in paths:
                    self._assert_pid_gone(int(path.read_text().strip()))
            finally:
                for path in paths:
                    self._kill_recorded_pid(path)

    def test_zero_exit_leader_with_pipe_closed_descendant_is_rejected_and_terminated(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            child_pid_path = Path(temporary) / "child.pid"
            program = (
                "import pathlib, subprocess, sys; "
                "child = subprocess.Popen([\"/bin/sleep\", \"30\"], "
                "stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, "
                "stderr=subprocess.DEVNULL); "
                "pathlib.Path(sys.argv[1]).write_text(str(child.pid))"
            )
            try:
                with self.assertRaisesRegex(GateError, "left a surviving process group"):
                    bounded_process(
                        [sys.executable, "-c", program, str(child_pid_path)],
                        env=bounded_environment(),
                        timeout_seconds=2,
                        maximum_stdout=0,
                        maximum_stderr=0,
                    )
                child_pid = int(child_pid_path.read_text().strip())
                self._assert_pid_gone(child_pid)
            finally:
                self._kill_recorded_pid(child_pid_path)

    def test_process_group_survival_after_kill_is_a_cleanup_failure(self) -> None:
        process = mock.Mock(pid=42)
        with (
            mock.patch.object(process_module, "signal_process_group", return_value=True),
            mock.patch.object(process_module, "process_group_exists", return_value=True),
            mock.patch.object(process_module, "wait_for_session_leader"),
            mock.patch.object(process_module.time, "sleep"),
            mock.patch.object(
                process_module.time,
                "monotonic",
                side_effect=(0.0, 2.0, 2.0, 8.0),
            ),
        ):
            with self.assertRaisesRegex(GateError, "survived forced termination"):
                process_module.terminate_process_group(process)

    def test_selector_construction_failure_extinguishes_the_spawned_group(self) -> None:
        self._assert_setup_failure_cleans_group(
            selector_failure=OSError("injected selector construction failure")
        )

    def test_second_selector_registration_failure_closes_the_first_registration(self) -> None:
        selector = FailingSelector(fail_registration=2)
        self._assert_setup_failure_cleans_group(selector=selector)
        self.assertTrue(selector.closed)
        self.assertEqual(selector.registrations, 2)

    def test_reader_failure_extinguishes_group_and_closes_selector(self) -> None:
        selector = FailingSelector()
        self._assert_setup_failure_cleans_group(
            selector=selector,
            read_failure=OSError("injected reader failure"),
        )
        self.assertTrue(selector.closed)

    def test_process_group_probe_failure_still_extinguishes_descendants(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            child_pid_path = Path(temporary) / "child.pid"
            program = (
                "import pathlib, subprocess, sys; "
                "child = subprocess.Popen(['/bin/sleep', '30'], "
                "stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, "
                "stderr=subprocess.DEVNULL); "
                "pathlib.Path(sys.argv[1]).write_text(str(child.pid))"
            )
            real_probe = process_module.process_group_exists
            calls = 0

            def fail_first_probe(process_group_id: int) -> bool:
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise OSError("injected process-group probe failure")
                return real_probe(process_group_id)

            try:
                with mock.patch.object(
                    process_module,
                    "process_group_exists",
                    side_effect=fail_first_probe,
                ):
                    with self.assertRaisesRegex(OSError, "probe failure"):
                        bounded_process(
                            [
                                sys.executable,
                                "-c",
                                program,
                                str(child_pid_path),
                            ],
                            env=bounded_environment(),
                            timeout_seconds=2,
                            maximum_stdout=0,
                            maximum_stderr=0,
                        )
                child_pid = int(child_pid_path.read_text().strip())
                self._assert_pid_gone(child_pid)
                self.assertGreater(calls, 1)
            finally:
                self._kill_recorded_pid(child_pid_path)

    def test_primary_and_cleanup_failures_preserve_causality(self) -> None:
        fake = mock.Mock(pid=42)
        fake.stdout = io.BytesIO()
        fake.stderr = io.BytesIO()
        primary = OSError("primary selector failure")
        cleanup = GateError("cleanup extinction failure")
        with (
            mock.patch.object(process_module.subprocess, "Popen", return_value=fake),
            mock.patch.object(
                process_module.selectors, "DefaultSelector", side_effect=primary
            ),
            mock.patch.object(
                process_module, "terminate_process_group", side_effect=cleanup
            ),
        ):
            with self.assertRaises(GateError) as raised:
                bounded_process(
                    [sys.executable],
                    env=bounded_environment(),
                    timeout_seconds=1,
                    maximum_stdout=0,
                    maximum_stderr=0,
                )
        self.assertIs(raised.exception, cleanup)
        self.assertIs(raised.exception.__cause__, primary)
        self.assertTrue(fake.stdout.closed)
        self.assertTrue(fake.stderr.closed)

    def _assert_setup_failure_cleans_group(
        self,
        *,
        selector: FailingSelector | None = None,
        selector_failure: BaseException | None = None,
        read_failure: BaseException | None = None,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            child_pid_path = Path(temporary) / "child.pid"
            program = (
                "import pathlib, subprocess, sys, time; "
                "child = subprocess.Popen(['/bin/sleep', '30'], "
                "stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, "
                "stderr=subprocess.DEVNULL); "
                "pathlib.Path(sys.argv[1]).write_text(str(child.pid)); "
                "sys.stdout.buffer.write(b'x'); sys.stdout.buffer.flush(); "
                "time.sleep(30)"
            )
            real_popen = subprocess.Popen
            real_read = os.read
            captured: dict[str, object] = {}

            def launch(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
                spawned = real_popen(*args, **kwargs)
                captured["process"] = spawned
                captured["stdout"] = spawned.stdout
                captured["stderr"] = spawned.stderr
                deadline = time.monotonic() + 2
                while not child_pid_path.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                if not child_pid_path.exists():
                    spawned.kill()
                    spawned.wait()
                    raise AssertionError("fixture child did not publish its PID")
                if read_failure is not None:
                    process_module.os.read = mock.Mock(side_effect=read_failure)
                return spawned

            selector_effect: object
            if selector_failure is not None:
                selector_effect = mock.Mock(side_effect=selector_failure)
            else:
                selector_effect = mock.Mock(return_value=selector)
            try:
                with (
                    mock.patch.object(
                        process_module.subprocess, "Popen", side_effect=launch
                    ),
                    mock.patch.object(
                        process_module.selectors,
                        "DefaultSelector",
                        side_effect=selector_effect,
                    ),
                ):
                    try:
                        with self.assertRaises((OSError, GateError)):
                            bounded_process(
                                [
                                    sys.executable,
                                    "-c",
                                    program,
                                    str(child_pid_path),
                                ],
                                env=bounded_environment(),
                                timeout_seconds=2,
                                maximum_stdout=1,
                                maximum_stderr=0,
                            )
                    finally:
                        process_module.os.read = real_read
                spawned = captured["process"]
                self.assertIsInstance(spawned, subprocess.Popen)
                self.assertIsNotNone(spawned.poll())
                self.assertFalse(process_module.process_group_exists(spawned.pid))
                child_pid = int(child_pid_path.read_text().strip())
                self._assert_pid_gone(child_pid)
                for name in ("stdout", "stderr"):
                    pipe = captured[name]
                    self.assertTrue(pipe is None or pipe.closed)
            finally:
                self._kill_recorded_pid(child_pid_path)

    def _assert_pid_gone(self, pid: int) -> None:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.01)
        self.fail(f"bounded process left PID {pid} alive")

    def _kill_recorded_pid(self, pid_path: Path) -> None:
        if not pid_path.exists():
            return
        try:
            pid = int(pid_path.read_text().strip())
            os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, ValueError):
            pass


if __name__ == "__main__":
    unittest.main()
