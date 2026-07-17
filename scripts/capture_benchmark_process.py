#!/usr/bin/env python3
"""Bounded process execution for authoritative capture-benchmark tooling."""

from __future__ import annotations

import os
import selectors
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

TERM_GRACE_SECONDS = 1.0
KILL_GRACE_SECONDS = 5.0
GROUP_PROBE_INTERVAL_SECONDS = 0.01
PROCESS_GROUP_POLICY_ENV = "CAPTURE_BENCH_PROCESS_GROUP_POLICY"
EXPECTED_PROCESS_GROUP_ENV = "CAPTURE_BENCH_EXPECTED_PROCESS_GROUP_ID"
INHERIT_OUTER_POLICY = "inherit-outer-v1"
SESSION_EXEC_MODE = "--capture-benchmark-session-exec"
WRAPPER_ENV_KEYS = "CAPTURE_BENCH_WRAPPER_ENV_KEYS"


class GateError(Exception):
    """Bounded non-secret gate failure."""


@dataclass(frozen=True)
class BoundedProcessResult:
    """Captured output and status from a bounded subprocess."""

    returncode: int
    stdout: bytes
    stderr: bytes


def bounded_process(
    arguments: list[str],
    *,
    env: dict[str, str],
    timeout_seconds: float,
    maximum_stdout: int,
    maximum_stderr: int,
    cwd: Path | None = None,
    executable: Path | None = None,
) -> BoundedProcessResult:
    """Supervise one process group under a deadline charged before synchronous spawn.

    Python cannot interrupt a `Popen` call stalled inside the operating system. The
    charged deadline includes that elapsed time once `Popen` returns; all subsequent
    setup, execution, output collection, and owned-process-group cleanup are
    fail-closed. Callers must bind child policy that forbids descendants from creating
    a different process group or session.
    """
    if (
        not arguments
        or timeout_seconds <= 0
        or maximum_stdout < 0
        or maximum_stderr < 0
    ):
        raise GateError("bounded process contract is invalid")
    stdout = bytearray()
    stderr = bytearray()
    selector: selectors.BaseSelector | None = None
    result: BoundedProcessResult | None = None
    primary_error: BaseException | None = None
    deadline = time.monotonic() + timeout_seconds
    process = subprocess.Popen(
        arguments,
        executable=executable,
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        selector = selectors.DefaultSelector()
        if process.stdout is None or process.stderr is None:
            raise GateError("bounded process pipes are unavailable")
        selector.register(
            process.stdout, selectors.EVENT_READ, (stdout, maximum_stdout)
        )
        selector.register(
            process.stderr, selectors.EVENT_READ, (stderr, maximum_stderr)
        )
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise GateError("bounded process exceeded its time limit")
            events = selector.select(min(remaining, 0.1))
            if not events and process.poll() is not None:
                continue
            for key, _mask in events:
                output, maximum = key.data
                request = min(64 * 1024, maximum - len(output) + 1)
                chunk = os.read(key.fileobj.fileno(), max(1, request))
                if not chunk:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                output.extend(chunk)
                if len(output) > maximum:
                    raise GateError("bounded process exceeded an output byte limit")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise GateError("bounded process exceeded its time limit")
        returncode = process.wait(timeout=remaining)
        result = BoundedProcessResult(returncode, bytes(stdout), bytes(stderr))
        if process_group_exists(process.pid):
            raise GateError("bounded process leader left a surviving process group")
    except BaseException as error:
        primary_error = error

    cleanup_error: BaseException | None = None
    if primary_error is not None:
        try:
            terminate_process_group(process)
        except BaseException as error:
            cleanup_error = error
    if selector is not None:
        try:
            selector.close()
        except BaseException as error:
            if cleanup_error is None:
                cleanup_error = error
    for pipe in (process.stdout, process.stderr):
        if pipe is not None and not pipe.closed:
            try:
                pipe.close()
            except BaseException as error:
                if cleanup_error is None:
                    cleanup_error = error
    if cleanup_error is not None:
        if primary_error is not None:
            raise cleanup_error from primary_error
        raise cleanup_error
    if primary_error is not None:
        raise primary_error
    if result is None:
        raise GateError("bounded process completed without a result")
    return result


def bounded_session_leader_exec(
    arguments: list[str],
    *,
    executable: Path,
    wrapper_python: Path,
    env: dict[str, str],
    timeout_seconds: float,
    maximum_stdout: int,
    maximum_stderr: int,
    cwd: Path | None = None,
) -> BoundedProcessResult:
    """Exec a target from a bound wrapper that injects its exact outer PGID."""

    if (
        not arguments
        or not executable.is_absolute()
        or not wrapper_python.is_absolute()
        or env.get(PROCESS_GROUP_POLICY_ENV) != INHERIT_OUTER_POLICY
        or EXPECTED_PROCESS_GROUP_ENV in env
        or WRAPPER_ENV_KEYS in env
        or any(
            not isinstance(key, str)
            or not key
            or any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for character in key)
            or not isinstance(value, str)
            or "\x00" in key
            or "\x00" in value
            for key, value in env.items()
        )
    ):
        raise GateError("session-leader exec contract is invalid")
    wrapper_environment = dict(env)
    wrapper_environment[WRAPPER_ENV_KEYS] = ",".join(sorted(env))
    module = Path(__file__).resolve(strict=True)
    return bounded_process(
        [
            str(wrapper_python),
            str(module),
            SESSION_EXEC_MODE,
            str(executable),
            *arguments,
        ],
        executable=wrapper_python,
        cwd=cwd,
        env=wrapper_environment,
        timeout_seconds=timeout_seconds,
        maximum_stdout=maximum_stdout,
        maximum_stderr=maximum_stderr,
    )


def _session_leader_exec(arguments: list[str]) -> None:
    if (
        len(arguments) < 2
        or not hasattr(os, "getsid")
        or os.getpid() != os.getpgrp()
        or os.getpid() != os.getsid(0)
    ):
        raise GateError("session exec wrapper does not own its process group")
    target = Path(arguments[0])
    if not target.is_absolute():
        raise GateError("session exec target is not absolute")
    encoded_keys = os.environ.get(WRAPPER_ENV_KEYS)
    if encoded_keys is None:
        raise GateError("session exec wrapper environment inventory is absent")
    keys = encoded_keys.split(",") if encoded_keys else []
    if keys != sorted(set(keys)) or any(key not in os.environ for key in keys):
        raise GateError("session exec wrapper environment inventory is malformed")
    environment = {key: os.environ[key] for key in keys}
    if (
        environment.get(PROCESS_GROUP_POLICY_ENV) != INHERIT_OUTER_POLICY
        or EXPECTED_PROCESS_GROUP_ENV in environment
    ):
        raise GateError("session exec wrapper process-group policy is invalid")
    environment[EXPECTED_PROCESS_GROUP_ENV] = str(os.getpgrp())
    os.execve(target, arguments[1:], environment)


def terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    """Terminate an owned process group and prove bounded extinction."""

    process_group_id = process.pid
    if not signal_process_group(process_group_id, signal.SIGTERM):
        wait_for_session_leader(process)
        return
    if not wait_for_process_group_extinction(
        process_group_id, TERM_GRACE_SECONDS, process
    ):
        signal_process_group(process_group_id, signal.SIGKILL)
        if not wait_for_process_group_extinction(
            process_group_id, KILL_GRACE_SECONDS, process
        ):
            wait_for_session_leader(process)
            raise GateError("owned process group survived forced termination")
    wait_for_session_leader(process)


def wait_for_process_group_extinction(
    process_group_id: int,
    timeout_seconds: float,
    process: subprocess.Popen[bytes] | None = None,
) -> bool:
    """Probe until a process group is extinct or the bounded deadline expires."""

    deadline = time.monotonic() + timeout_seconds
    while True:
        if process is not None:
            process.poll()
        if not process_group_exists(process_group_id):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(GROUP_PROBE_INTERVAL_SECONDS)


def signal_process_group(process_group_id: int, requested_signal: signal.Signals) -> bool:
    """Signal an owned group without consulting the session leader's exit state."""
    try:
        os.killpg(process_group_id, requested_signal)
    except ProcessLookupError:
        return False
    except PermissionError:
        # macOS may report EPERM briefly for an exited session leader until it
        # is reaped. The bounded extinction probe distinguishes that state
        # from a genuinely surviving group.
        return True
    return True


def process_group_exists(process_group_id: int) -> bool:
    """Return whether the owned process group still has a process."""
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # POSIX defines EPERM as a failed permission check against an existing
        # process. macOS can report it briefly while a terminated leader is
        # being reaped, so it must not be mistaken for group extinction.
        return True
    return True


def wait_for_session_leader(process: subprocess.Popen[bytes]) -> None:
    """Reap the direct child after its process group has been signalled."""
    process.wait(timeout=5)


def _main() -> int:
    try:
        if len(sys.argv) < 4 or sys.argv[1] != SESSION_EXEC_MODE:
            raise GateError("process helper mode is invalid")
        _session_leader_exec(sys.argv[2:])
    except (GateError, OSError, ValueError, TypeError) as error:
        print(f"capture benchmark process helper failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
