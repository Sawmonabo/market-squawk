#!/usr/bin/env python3
"""Bound host and tool observation for capture-benchmark evidence."""

from __future__ import annotations

import hashlib
import math
import os
import platform
import pwd
import stat
import sys
import time
from pathlib import Path
from typing import Any

if __package__:
    from .capture_benchmark_evidence_io import (
        CapabilityRoot,
        GateError,
        NOFOLLOW,
        bounded_command,
        digest_bytes,
        digest_json,
        is_lower_digest,
        read_json_bytes,
    )
    from .capture_benchmark_host_schema import (
        FIXTURE_MODE,
        HOST_FIELDS,
        MAX_PROCESSES,
        PROFILE,
        PRODUCTION_MODE,
        SCHEMA_VERSION,
        TOOLCHAIN_FIELDS,
        TOOL_NAMES,
        is_git_head,
        validate_stable_host,
        validate_stable_toolchain,
        validate_tool_identity,
        validate_tool_executions,
    )
    from .capture_benchmark_host_execution import (
        create_ephemeral_execution,
        execution_identity as _execution_identity,
        remove_ephemeral_execution as _remove_ephemeral_execution,
        require_ephemeral_execution as _require_ephemeral_execution,
    )
else:
    from capture_benchmark_evidence_io import (
        CapabilityRoot,
        GateError,
        NOFOLLOW,
        bounded_command,
        digest_bytes,
        digest_json,
        is_lower_digest,
        read_json_bytes,
    )
    from capture_benchmark_host_schema import (
        FIXTURE_MODE,
        HOST_FIELDS,
        MAX_PROCESSES,
        PROFILE,
        PRODUCTION_MODE,
        SCHEMA_VERSION,
        TOOLCHAIN_FIELDS,
        TOOL_NAMES,
        is_git_head,
        validate_stable_host,
        validate_stable_toolchain,
        validate_tool_identity,
        validate_tool_executions,
    )
    from capture_benchmark_host_execution import (
        create_ephemeral_execution,
        execution_identity as _execution_identity,
        remove_ephemeral_execution as _remove_ephemeral_execution,
        require_ephemeral_execution as _require_ephemeral_execution,
    )

MAX_ARGV_BYTES = 16 * 1024
MAX_TOOL_BYTES = 256 * 1024 * 1024


def executable_identity(
    path: Path,
    *,
    execution_strategy: str | None = None,
    allow_source_hardlinks: bool = False,
) -> dict[str, Any]:
    """Hash one canonical executable through a no-follow descriptor."""

    resolved = path.resolve(strict=True)
    if not resolved.is_absolute() or Path(os.path.realpath(resolved)) != resolved:
        raise GateError("observation tool path is not canonical")
    descriptor = os.open(resolved, os.O_RDONLY | NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        if not _safe_executable(before, allow_source_hardlinks):
            raise GateError("observation tool is not a bounded executable")
        _validate_executable_directory_chain(resolved, before.st_uid)
        digest = hashlib.sha256()
        observed = 0
        while observed < before.st_size:
            chunk = os.read(descriptor, min(1024 * 1024, before.st_size - observed))
            if not chunk:
                raise GateError("observation tool ended before its declared size")
            digest.update(chunk)
            observed += len(chunk)
        if os.read(descriptor, 1):
            raise GateError("observation tool grew beyond its declared size")
        after = os.fstat(descriptor)
        current = os.stat(resolved, follow_symlinks=False)
        identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mode,
            before.st_uid,
            before.st_nlink,
            getattr(before, "st_flags", 0),
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        if (
            not _safe_executable(after, allow_source_hardlinks)
            or not _safe_executable(current, allow_source_hardlinks)
            or (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mode,
                after.st_uid,
                after.st_nlink,
                getattr(after, "st_flags", 0),
                after.st_mtime_ns,
                after.st_ctime_ns,
            )
            != identity
            or (
                current.st_dev,
                current.st_ino,
                current.st_size,
                current.st_mode,
                current.st_uid,
                current.st_nlink,
                getattr(current, "st_flags", 0),
                current.st_mtime_ns,
                current.st_ctime_ns,
            )
            != identity
        ):
            raise GateError("observation tool changed during descriptor hashing")
        strategy = execution_strategy or (
            "trusted-direct" if before.st_uid == 0 else "ephemeral-copy"
        )
        if (
            strategy not in {"trusted-direct", "ephemeral-copy", "current-process"}
            or strategy == "trusted-direct"
            and before.st_uid != 0
            or strategy == "ephemeral-copy"
            and (
                before.st_uid != os.getuid()
                or before.st_nlink != 1
                and not allow_source_hardlinks
            )
            or strategy == "current-process"
            and resolved != Path(sys.executable).resolve(strict=True)
        ):
            raise GateError("observation tool execution strategy is invalid")
        return {
            "state": "available",
            "path": str(resolved),
            "device": before.st_dev,
            "inode": before.st_ino,
            "size": before.st_size,
            "uid": before.st_uid,
            "mode": stat.S_IMODE(before.st_mode),
            "nlink": before.st_nlink,
            "flags": getattr(before, "st_flags", 0),
            "execution_strategy": strategy,
            "sha256": digest.hexdigest(),
        }
    finally:
        os.close(descriptor)


def _safe_executable(
    metadata: os.stat_result, allow_source_hardlinks: bool = False
) -> bool:
    return (
        stat.S_ISREG(metadata.st_mode)
        and 0 < metadata.st_size <= MAX_TOOL_BYTES
        and metadata.st_uid in {0, os.getuid()}
        and metadata.st_nlink >= 1
        and (
            metadata.st_uid == 0
            or metadata.st_nlink == 1
            or allow_source_hardlinks
        )
        and stat.S_IMODE(metadata.st_mode) & 0o111 != 0
        and stat.S_IMODE(metadata.st_mode) & 0o022 == 0
    )


def _validate_executable_directory_chain(path: Path, owner: int) -> None:
    current = path.parent
    current_groups = {os.getgid(), *os.getgroups()}
    while True:
        metadata = os.lstat(current)
        expected_owner = 0 if owner == 0 else os.getuid()
        mode = stat.S_IMODE(metadata.st_mode)
        group_writable_by_current_process = (
            owner == 0
            and bool(mode & stat.S_IWGRP)
            and metadata.st_gid in current_groups
        )
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != expected_owner
            or bool(mode & stat.S_IWOTH)
            or (owner != 0 and bool(mode & stat.S_IWGRP))
            or group_writable_by_current_process
            or Path(os.path.realpath(current)) != current
        ):
            raise GateError(
                "observation tool directory authority is unsafe "
                f"(path={current}, uid={metadata.st_uid}, gid={metadata.st_gid}, "
                f"mode={mode:#o})"
            )
        if owner != 0 or current.parent == current:
            return
        current = current.parent


def resolve_tool_identity(
    name: str,
    *,
    explicit_path: Path | None = None,
    required: bool = True,
    unavailable_reason: str = "not-required-on-platform",
    execution_strategy: str | None = None,
    allow_source_hardlinks: bool = False,
) -> dict[str, Any]:
    if name not in TOOL_NAMES:
        raise GateError("observation tool name is outside the closed inventory")
    if not required:
        if explicit_path is not None or not unavailable_reason:
            raise GateError("unavailable tool identity is malformed")
        return {"state": "unavailable", "reason": unavailable_reason}
    if explicit_path is None:
        raise GateError("required observation tool is unavailable")
    return executable_identity(
        explicit_path,
        execution_strategy=execution_strategy,
        allow_source_hardlinks=allow_source_hardlinks,
    )


def _create_ephemeral_execution(identity: dict[str, Any]) -> dict[str, Any]:
    return create_ephemeral_execution(identity, executable_identity)


def run_bound_tool(
    identity: dict[str, Any],
    arguments: list[str],
    *,
    invocation_name: str | None = None,
    tool_name: str | None = None,
    execution_records: list[dict[str, Any]] | None = None,
) -> str:
    rustup_hardlinks = _allows_bound_rustup_hardlinks(
        identity, invocation_name, tool_name
    )
    if (
        not validate_tool_identity(
            identity, allow_source_hardlinks=rustup_hardlinks
        )
        or identity["state"] != "available"
    ):
        raise GateError("bound observation tool identity is invalid")
    if any(
        not isinstance(argument, str) or len(argument.encode()) > MAX_ARGV_BYTES
        for argument in arguments
    ):
        raise GateError("bound observation tool argument is invalid")
    if invocation_name is not None and invocation_name not in TOOL_NAMES:
        raise GateError("bound observation tool invocation role is invalid")
    if (
        execution_records is None
        and tool_name is not None
        or execution_records is not None
        and tool_name not in TOOL_NAMES
    ):
        raise GateError("bound observation tool execution record has no tool identity")
    path = Path(identity["path"])
    if (
        executable_identity(
            path,
            execution_strategy=identity["execution_strategy"],
            allow_source_hardlinks=rustup_hardlinks,
        )
        != identity
    ):
        raise GateError("observation tool changed before execution")
    binding = None
    execution_path = path
    protection = "root-owned-directory-chain"
    if identity["execution_strategy"] == "ephemeral-copy":
        binding = _create_ephemeral_execution(identity)
        execution_path = binding["path"]
        protection = binding["protection"]
    elif identity["execution_strategy"] != "trusted-direct":
        raise GateError("current-process identity cannot launch an observation command")
    executed = _execution_identity(identity)
    path_removed = False
    try:
        if binding is not None:
            executed = binding["identity"]
            _require_ephemeral_execution(binding)
        result = bounded_command(
            [invocation_name or str(path), *arguments],
            executable=execution_path,
        )
        if binding is not None:
            _require_ephemeral_execution(binding)
    finally:
        if binding is not None:
            path_removed = _remove_ephemeral_execution(binding)
    if (
        executable_identity(
            path,
            execution_strategy=identity["execution_strategy"],
            allow_source_hardlinks=rustup_hardlinks,
        )
        != identity
    ):
        raise GateError("observation tool changed during execution")
    if execution_records is not None:
        execution_records.append(
            {
                "tool": tool_name,
                "strategy": identity["execution_strategy"],
                "source_sha256": identity["sha256"],
                "executed": executed,
                "protection": protection,
                "path_removed": path_removed,
            }
        )
    return result


def _allows_bound_rustup_hardlinks(
    identity: dict[str, Any], invocation_name: str | None, tool_name: str | None
) -> bool:
    if (
        tool_name != "rustup"
        or invocation_name not in {None, "rustup"}
        or identity.get("state") != "available"
        or Path(identity.get("path", "")).name.casefold()
        not in {"rustup", "rustup.exe"}
    ):
        return False
    try:
        rustup = _closed_tool_path("rustup").resolve(strict=True)
    except (GateError, OSError):
        return False
    return Path(identity["path"]) == rustup


def process_class(argv: str) -> str:
    lowered = argv.lower()
    if "capture_admission_evidence" in lowered:
        return "capture-evidence"
    if "capture_admission_criterion" in lowered:
        return "criterion-zero-authority"
    if "rustc" in lowered:
        return "rustc"
    if "cargo" in lowered:
        return "cargo"
    return "other"


def normalize_processes(
    raw: list[dict[str, Any]], allowed_pids: set[int] | None = None
) -> tuple[list[dict[str, Any]], list[int]]:
    if not isinstance(raw, list) or len(raw) > MAX_PROCESSES:
        raise GateError("process inventory exceeds its entry bound")
    normalized = []
    competitors = []
    own = {os.getpid(), os.getppid()}
    allowed = allowed_pids or set()
    for item in raw:
        if not isinstance(item, dict) or set(item) != {"pid", "ppid", "state", "comm", "argv"}:
            raise GateError("process inventory entry has an invalid schema")
        pid, ppid = item["pid"], item["ppid"]
        state, comm, argv = item["state"], item["comm"], item["argv"]
        if (
            not isinstance(pid, int)
            or isinstance(pid, bool)
            or not isinstance(ppid, int)
            or isinstance(ppid, bool)
            or pid <= 0
            or ppid < 0
            or not isinstance(state, str)
            or not isinstance(comm, str)
            or not isinstance(argv, str)
            or len(state) > 16
            or len(comm.encode()) > 256
            or len(argv.encode()) > MAX_ARGV_BYTES
        ):
            raise GateError("process inventory entry exceeds a bound")
        command_class = process_class(argv)
        normalized.append(
            {
                "pid": pid,
                "ppid": ppid,
                "state": state,
                "comm": comm,
                "command_class": command_class,
            }
        )
        if pid not in own and pid not in allowed and command_class in {
            "capture-evidence",
            "criterion-zero-authority",
            "rustc",
            "cargo",
        }:
            competitors.append(pid)
    normalized.sort(key=lambda item: item["pid"])
    return normalized, sorted(competitors)


def production_processes(
    ps_identity: dict[str, Any],
    execution_records: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    output = run_bound_tool(
        ps_identity,
        ["-ww", "-axo", "pid=,ppid=,state=,comm=,command="],
        tool_name="ps" if execution_records is not None else None,
        execution_records=execution_records,
    )
    result = []
    for line in output.splitlines():
        fields = line.strip().split(None, 4)
        if len(fields) != 5:
            raise GateError("process inventory output is malformed")
        try:
            pid, ppid = int(fields[0]), int(fields[1])
        except ValueError as error:
            raise GateError("process inventory identity is malformed") from error
        result.append(
            {"pid": pid, "ppid": ppid, "state": fields[2], "comm": fields[3], "argv": fields[4]}
        )
    return result


def fixture_observation(root: CapabilityRoot, fixture: Path, phase: str) -> dict[str, Any]:
    value = read_json_bytes(root.read_file(fixture))
    if (
        not isinstance(value, dict)
        or set(value) != {"preflight", "postflight"}
        or phase not in value
    ):
        raise GateError("observation fixture has an invalid top-level schema")
    selected = value[phase]
    if not isinstance(selected, dict):
        raise GateError("observation fixture phase is invalid")
    return selected


def _current_process_execution(
    tool: str, identity: dict[str, Any]
) -> dict[str, Any]:
    if (
        tool != "python"
        or identity.get("state") != "available"
        or identity.get("execution_strategy") != "current-process"
        or Path(identity["path"]) != Path(sys.executable).resolve(strict=True)
    ):
        raise GateError("current process execution identity is invalid")
    current = executable_identity(
        Path(identity["path"]), execution_strategy="current-process"
    )
    if current != identity:
        raise GateError("current process executable changed during observation")
    return {
        "tool": tool,
        "strategy": "current-process",
        "source_sha256": identity["sha256"],
        "executed": _execution_identity(identity),
        "protection": "already-running-current-process",
        "path_removed": False,
    }


def production_observation() -> dict[str, Any]:
    executions: list[dict[str, Any]] = []
    identities = _production_tool_identities(executions)
    executions.append(_current_process_execution("python", identities["python"]))
    rustc = run_bound_tool(
        identities["rustc"], ["-vV"], tool_name="rustc", execution_records=executions
    )
    cargo = run_bound_tool(
        identities["cargo"], ["-V"], tool_name="cargo", execution_records=executions
    )
    measured_code_head = run_bound_tool(
        identities["git"],
        ["rev-parse", "HEAD"],
        tool_name="git",
        execution_records=executions,
    )
    if sys.platform.startswith("linux"):
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    else:
        boot_id = run_bound_tool(
            identities["sysctl"],
            ["-n", "kern.boottime"],
            tool_name="sysctl",
            execution_records=executions,
        )
    affinity = (
        sorted(os.sched_getaffinity(0))
        if hasattr(os, "sched_getaffinity")
        else "unavailable"
    )
    scheduler = os.sched_getscheduler(0) if hasattr(os, "sched_getscheduler") else "unavailable"
    power_hash = digest_bytes(_bounded_power_observation(identities, executions).encode())
    thermal_hash = digest_bytes(
        _bounded_thermal_observation(identities, executions).encode()
    )
    processes = production_processes(identities["ps"], executions)
    if not validate_tool_executions(executions, identities):
        raise GateError("production observation execution receipts are incomplete")
    return {
        "host": {
            "system": platform.system(),
            "machine": platform.machine(),
            "node": platform.node(),
            "boot_id": boot_id,
            "logical_cpus": os.cpu_count(),
            "affinity": affinity,
            "nice": os.getpriority(os.PRIO_PROCESS, 0),
            "scheduler": scheduler,
            "power_hash": power_hash,
            "thermal_hash": thermal_hash,
        },
        "toolchain": {
            "rustc": rustc,
            "cargo": cargo,
            "target": next(
                (line[6:] for line in rustc.splitlines() if line.startswith("host: ")), ""
            ),
            "release_profile": PROFILE,
            "tool_identities": identities,
        },
        "measured_code_head": measured_code_head,
        "tool_executions": executions,
        "load_one_minute": os.getloadavg()[0],
        "processes": processes,
        "wall_ns": time.time_ns(),
        "monotonic_ns": time.monotonic_ns(),
    }


def _production_tool_identities(
    execution_records: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    identities = {
        "git": resolve_tool_identity("git", explicit_path=_closed_tool_path("git")),
        "ps": resolve_tool_identity("ps", explicit_path=_closed_tool_path("ps")),
        "python": resolve_tool_identity(
            "python",
            explicit_path=Path(sys.executable),
            execution_strategy="current-process",
        ),
    }
    identities.update(_resolve_toolchain_identities(execution_records))
    if sys.platform == "darwin":
        identities["pmset"] = resolve_tool_identity(
            "pmset", explicit_path=_closed_tool_path("pmset")
        )
        identities["sysctl"] = resolve_tool_identity(
            "sysctl", explicit_path=_closed_tool_path("sysctl")
        )
    elif sys.platform.startswith("linux"):
        identities["pmset"] = resolve_tool_identity("pmset", required=False)
        identities["sysctl"] = resolve_tool_identity("sysctl", required=False)
    else:
        identities["pmset"] = resolve_tool_identity("pmset", required=False)
        identities["sysctl"] = resolve_tool_identity(
            "sysctl", explicit_path=_closed_tool_path("sysctl")
        )
    if tuple(sorted(identities)) != TOOL_NAMES:
        raise GateError("production tool inventory is incomplete")
    return identities


def _resolve_toolchain_identities(
    execution_records: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    paths = {name: _closed_tool_path(name) for name in ("cargo", "rustc")}
    try:
        rustup_path = _closed_tool_path("rustup")
    except GateError:
        rustup_path = None
    launchers = {
        name: resolve_tool_identity(
            name,
            explicit_path=path,
            allow_source_hardlinks=rustup_path is not None,
        )
        for name, path in paths.items()
    }
    rustup = (
        resolve_tool_identity(
            "rustup",
            explicit_path=rustup_path,
            allow_source_hardlinks=True,
        )
        if rustup_path is not None
        else None
    )
    proxied = {
        name
        for name, identity in launchers.items()
        if Path(identity["path"]).name.casefold() in {"rustup", "rustup.exe"}
        or rustup is not None
        and _same_executable_identity(identity, rustup)
    }
    if not proxied:
        if not all(validate_tool_identity(identity) for identity in launchers.values()):
            raise GateError("non-proxy Rust toolchain executable has ambiguous links")
        return {
            **launchers,
            "rustup": resolve_tool_identity(
                "rustup",
                required=False,
                unavailable_reason="not-used-for-direct-toolchain",
            ),
        }
    if proxied != {"cargo", "rustc"}:
        raise GateError("Rust toolchain proxy resolution is inconsistent")
    if rustup is None:
        rustup = launchers["cargo"]
    if not all(
        _same_executable_identity(identity, rustup)
        for identity in launchers.values()
    ):
        raise GateError("Rust toolchain proxy resolution is inconsistent")
    resolved = {}
    for name in ("cargo", "rustc"):
        path = run_bound_tool(
            rustup,
            ["which", name],
            invocation_name="rustup",
            tool_name="rustup",
            execution_records=execution_records,
        )
        identity = executable_identity(Path(path))
        expected_names = {name.casefold(), f"{name}.exe".casefold()}
        if Path(identity["path"]).name.casefold() not in expected_names:
            raise GateError("Rust toolchain executable resolution is malformed")
        resolved[name] = identity
    return {**resolved, "rustup": rustup}


def _same_executable_identity(
    left: dict[str, Any], right: dict[str, Any]
) -> bool:
    return all(
        left.get(field) == right.get(field)
        for field in (
            "device",
            "inode",
            "size",
            "uid",
            "mode",
            "nlink",
            "flags",
            "sha256",
        )
    )


def _closed_tool_path(name: str) -> Path:
    """Resolve a tool only from the fixed OS/user installation inventory."""

    user_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    candidates = {
        "cargo": (user_home / ".cargo/bin/cargo",),
        "rustc": (user_home / ".cargo/bin/rustc",),
        "rustup": (user_home / ".cargo/bin/rustup",),
        "git": (Path("/usr/bin/git"), Path("/bin/git")),
        "ps": (Path("/bin/ps"), Path("/usr/bin/ps")),
        "pmset": (Path("/usr/bin/pmset"),),
        "sysctl": (Path("/usr/sbin/sysctl"), Path("/sbin/sysctl")),
    }
    if name not in candidates:
        raise GateError("observation tool is outside the fixed path inventory")
    for candidate in candidates[name]:
        if candidate.exists():
            return candidate
    raise GateError("required fixed-path observation tool is unavailable")


def _bounded_power_observation(
    identities: dict[str, dict[str, Any]], execution_records: list[dict[str, Any]]
) -> str:
    if sys.platform == "darwin":
        return run_bound_tool(
            identities["pmset"],
            ["-g", "custom"],
            tool_name="pmset",
            execution_records=execution_records,
        )
    return "platform-power-observation-unavailable"


def _bounded_thermal_observation(
    identities: dict[str, dict[str, Any]], execution_records: list[dict[str, Any]]
) -> str:
    if sys.platform == "darwin":
        return run_bound_tool(
            identities["pmset"],
            ["-g", "therm"],
            tool_name="pmset",
            execution_records=execution_records,
        )
    return "platform-thermal-observation-unavailable"


def normalize_observation(
    raw: dict[str, Any],
    evidence_mode: str,
    *,
    expected_measured_code_head: str | None = None,
    benchmark_backend: str = "standard",
    baseline_manifest_sha256: str | None = None,
    baseline_lock_sha256: str | None = None,
) -> dict[str, Any]:
    required = {
        "host",
        "toolchain",
        "measured_code_head",
        "tool_executions",
        "load_one_minute",
        "processes",
        "wall_ns",
        "monotonic_ns",
    }
    if not isinstance(raw, dict) or set(raw) != required:
        raise GateError("host observation has an invalid schema")
    host, toolchain = raw["host"], raw["toolchain"]
    head = raw["measured_code_head"]
    load, wall_ns, monotonic_ns = raw["load_one_minute"], raw["wall_ns"], raw["monotonic_ns"]
    baseline_valid = (
        benchmark_backend == "standard"
        and baseline_manifest_sha256 is None
        and baseline_lock_sha256 is None
    ) or (
        benchmark_backend == "candidate"
        and is_lower_digest(baseline_manifest_sha256)
        and is_lower_digest(baseline_lock_sha256)
    )
    if (
        evidence_mode not in {PRODUCTION_MODE, FIXTURE_MODE}
        or not validate_stable_host(host)
        or not validate_stable_toolchain(toolchain, evidence_mode)
        or not validate_tool_executions(raw["tool_executions"], toolchain["tool_identities"])
        or not is_git_head(head)
        or expected_measured_code_head is not None
        and head != expected_measured_code_head
        or not baseline_valid
        or not isinstance(load, (int, float))
        or isinstance(load, bool)
        or not math.isfinite(load)
        or load < 0
        or not isinstance(wall_ns, int)
        or isinstance(wall_ns, bool)
        or not isinstance(monotonic_ns, int)
        or isinstance(monotonic_ns, bool)
        or wall_ns <= 0
        or monotonic_ns <= 0
    ):
        raise GateError("host observation values are invalid")
    normalized_processes, competitors = normalize_processes(raw["processes"])
    stable_host = dict(host)
    stable_toolchain = dict(toolchain)
    return {
        "schema_version": SCHEMA_VERSION,
        "evidence_mode": evidence_mode,
        "stable_host": stable_host,
        "stable_toolchain": stable_toolchain,
        "host_fingerprint_sha256": digest_json(stable_host),
        "toolchain_fingerprint_sha256": digest_json(stable_toolchain),
        "tool_executions": raw["tool_executions"],
        "tool_execution_sha256": digest_json(raw["tool_executions"]),
        "release_profile_sha256": digest_bytes(PROFILE.encode()),
        "measured_code_head": head,
        "benchmark_backend": benchmark_backend,
        "baseline_manifest_sha256": baseline_manifest_sha256,
        "baseline_lock_sha256": baseline_lock_sha256,
        "load_one_minute": float(load),
        "normalized_load": float(load) / host["logical_cpus"],
        "process_inventory_sha256": digest_json(normalized_processes),
        "processes": normalized_processes,
        "competitor_pids": competitors,
        "wall_ns": wall_ns,
        "monotonic_ns": monotonic_ns,
    }
