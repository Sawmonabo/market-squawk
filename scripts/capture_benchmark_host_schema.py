#!/usr/bin/env python3
"""Strict host phase-evidence schemas for capture-benchmark authority."""

from __future__ import annotations

import os
import stat
from typing import Any

if __package__:
    from .capture_benchmark_evidence_io import GateError, digest_bytes, digest_json, is_lower_digest
else:
    from capture_benchmark_evidence_io import GateError, digest_bytes, digest_json, is_lower_digest

SCHEMA_VERSION = 1
PRODUCTION_MODE = "production"
FIXTURE_MODE = "fixture"
PROFILE = (
    "cargo-bench-inherits-release:opt-level=3:lto=thin:codegen-units=1:"
    "panic=abort:strip=symbols"
)
MAX_PROCESSES = 16_384
MAX_TOOL_EXECUTIONS = 64
ACTIVE_AGENT_CLAIM = "exclusive-current-uid-tool-authority-v1"
HOST_FIELDS = {
    "system",
    "machine",
    "node",
    "boot_id",
    "logical_cpus",
    "affinity",
    "nice",
    "scheduler",
    "power_hash",
    "thermal_hash",
}
TOOL_NAMES = ("cargo", "git", "pmset", "ps", "python", "rustc", "rustup", "sysctl")
TOOL_IDENTITY_FIELDS = {
    "state",
    "path",
    "device",
    "inode",
    "size",
    "uid",
    "mode",
    "nlink",
    "flags",
    "execution_strategy",
    "sha256",
    "reason",
}
TOOLCHAIN_FIELDS = {"rustc", "cargo", "target", "release_profile", "tool_identities"}
EXECUTED_IDENTITY_FIELDS = {
    "path",
    "device",
    "inode",
    "size",
    "uid",
    "mode",
    "nlink",
    "flags",
    "sha256",
}
TOOL_EXECUTION_FIELDS = {
    "tool",
    "strategy",
    "source_sha256",
    "executed",
    "protection",
    "path_removed",
}
PHASE_EVIDENCE_FIELDS = {
    "schema_version",
    "evidence_mode",
    "stable_host",
    "stable_toolchain",
    "host_fingerprint_sha256",
    "toolchain_fingerprint_sha256",
    "tool_executions",
    "tool_execution_sha256",
    "release_profile_sha256",
    "measured_code_head",
    "benchmark_backend",
    "baseline_manifest_sha256",
    "baseline_lock_sha256",
    "load_one_minute",
    "normalized_load",
    "process_inventory_sha256",
    "processes",
    "competitor_pids",
    "wall_ns",
    "monotonic_ns",
    "phase",
    "lock_nonce_sha256",
    "lock_identity",
    "owner_identity",
    "output_identity",
    "active_agent_attestation",
    "measurement_inputs",
}
STABLE_PHASE_FIELDS = {
    "schema_version",
    "evidence_mode",
    "host_fingerprint_sha256",
    "toolchain_fingerprint_sha256",
    "release_profile_sha256",
    "measured_code_head",
    "benchmark_backend",
    "baseline_manifest_sha256",
    "baseline_lock_sha256",
    "lock_nonce_sha256",
    "lock_identity",
    "owner_identity",
    "output_identity",
    "active_agent_attestation",
    "measurement_inputs",
}


def is_git_head(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 40
        and all(character in "0123456789abcdef" for character in value)
    )


def validate_tool_identity(value: object) -> bool:
    if not isinstance(value, dict) or value.get("state") not in {"available", "unavailable"}:
        return False
    if value["state"] == "unavailable":
        return (
            set(value) == {"state", "reason"}
            and isinstance(value["reason"], str)
            and 0 < len(value["reason"].encode()) <= 128
        )
    exact = {
        "state",
        "path",
        "device",
        "inode",
        "size",
        "uid",
        "mode",
        "nlink",
        "flags",
        "execution_strategy",
        "sha256",
    }
    strategy = value.get("execution_strategy")
    uid = value.get("uid")
    nlink = value.get("nlink")
    flags = value.get("flags")
    mode = value.get("mode")
    strategy_valid = (
        strategy == "trusted-direct" and uid == 0
    ) or (
        strategy == "ephemeral-copy" and uid == os.getuid() and nlink == 1
    ) or (
        strategy == "current-process" and uid in {0, os.getuid()}
    )
    return (
        set(value) == exact
        and isinstance(value["path"], str)
        and value["path"].startswith("/")
        and os.path.normpath(value["path"]) == value["path"]
        and 0 < len(value["path"].encode()) <= 4096
        and isinstance(value["device"], int)
        and not isinstance(value["device"], bool)
        and value["device"] >= 0
        and isinstance(value["inode"], int)
        and not isinstance(value["inode"], bool)
        and value["inode"] > 0
        and isinstance(value["size"], int)
        and not isinstance(value["size"], bool)
        and value["size"] > 0
        and isinstance(uid, int)
        and not isinstance(uid, bool)
        and uid in {0, os.getuid()}
        and isinstance(mode, int)
        and not isinstance(mode, bool)
        and 0 < mode <= 0o7777
        and mode & 0o111 != 0
        and mode & 0o022 == 0
        and isinstance(nlink, int)
        and not isinstance(nlink, bool)
        and nlink > 0
        and isinstance(flags, int)
        and not isinstance(flags, bool)
        and flags >= 0
        and strategy_valid
        and is_lower_digest(value["sha256"])
    )


def validate_stable_host(value: object) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == HOST_FIELDS
        and isinstance(value["logical_cpus"], int)
        and not isinstance(value["logical_cpus"], bool)
        and value["logical_cpus"] > 0
        and all(
            isinstance(value[field], str) and bool(value[field])
            for field in ("system", "machine", "node", "boot_id")
        )
        and isinstance(value["affinity"], (list, str))
        and isinstance(value["nice"], int)
        and not isinstance(value["nice"], bool)
        and isinstance(value["scheduler"], (int, str))
        and not isinstance(value["scheduler"], bool)
        and is_lower_digest(value["power_hash"])
        and is_lower_digest(value["thermal_hash"])
    )


def validate_stable_toolchain(value: object, evidence_mode: str) -> bool:
    if not isinstance(value, dict) or set(value) != TOOLCHAIN_FIELDS:
        return False
    identities = value["tool_identities"]
    if (
        not isinstance(identities, dict)
        or tuple(sorted(identities)) != TOOL_NAMES
        or not all(validate_tool_identity(identity) for identity in identities.values())
        or not all(
            isinstance(value[field], str) and bool(value[field])
            for field in ("rustc", "cargo", "target")
        )
        or value["release_profile"] != PROFILE
    ):
        return False
    required = {"cargo", "git", "ps", "python", "rustc"}
    if any(identities[name]["state"] != "available" for name in required):
        return False
    if evidence_mode == PRODUCTION_MODE:
        # Conditional tools are validated at collection time. Historical phase
        # validation retains their explicit available/unavailable states.
        return all(name in identities for name in ("pmset", "sysctl"))
    return True


def _validate_executed_identity(value: object) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == EXECUTED_IDENTITY_FIELDS
        and isinstance(value["path"], str)
        and value["path"].startswith("/")
        and os.path.normpath(value["path"]) == value["path"]
        and 0 < len(value["path"].encode()) <= 4096
        and isinstance(value["device"], int)
        and not isinstance(value["device"], bool)
        and value["device"] >= 0
        and isinstance(value["inode"], int)
        and not isinstance(value["inode"], bool)
        and value["inode"] > 0
        and isinstance(value["size"], int)
        and not isinstance(value["size"], bool)
        and value["size"] > 0
        and isinstance(value["uid"], int)
        and not isinstance(value["uid"], bool)
        and value["uid"] in {0, os.getuid()}
        and isinstance(value["mode"], int)
        and not isinstance(value["mode"], bool)
        and 0 < value["mode"] <= 0o7777
        and value["mode"] & 0o111 != 0
        and value["mode"] & 0o022 == 0
        and isinstance(value["nlink"], int)
        and not isinstance(value["nlink"], bool)
        and value["nlink"] > 0
        and isinstance(value["flags"], int)
        and not isinstance(value["flags"], bool)
        and value["flags"] >= 0
        and is_lower_digest(value["sha256"])
    )


def _is_ephemeral_execution_path(path: str) -> bool:
    prefix = ".market-squawk-bound-"
    name = os.path.basename(path)
    suffix = name[len(prefix) :] if name.startswith(prefix) else ""
    return len(suffix) == 32 and all(character in "0123456789abcdef" for character in suffix)


def validate_tool_executions(value: object, identities: object) -> bool:
    """Validate exact source-to-execution receipts and role multiplicity."""

    if (
        not isinstance(value, list)
        or not 0 < len(value) <= MAX_TOOL_EXECUTIONS
        or not isinstance(identities, dict)
        or tuple(sorted(identities)) != TOOL_NAMES
        or not all(validate_tool_identity(identity) for identity in identities.values())
    ):
        return False
    counts = {name: 0 for name in TOOL_NAMES}
    for receipt in value:
        if not isinstance(receipt, dict) or set(receipt) != TOOL_EXECUTION_FIELDS:
            return False
        tool = receipt["tool"]
        if tool not in counts:
            return False
        source = identities[tool]
        executed = receipt["executed"]
        if (
            source.get("state") != "available"
            or not _validate_executed_identity(executed)
            or receipt["strategy"] != source["execution_strategy"]
            or receipt["source_sha256"] != source["sha256"]
            or executed["sha256"] != source["sha256"]
            or executed["size"] != source["size"]
        ):
            return False
        source_execution = {
            field: source[field] for field in EXECUTED_IDENTITY_FIELDS
        }
        strategy = receipt["strategy"]
        if strategy == "ephemeral-copy":
            immutable_flag = getattr(stat, "UF_IMMUTABLE", 0)
            immutable_protection = receipt["protection"] == "user-immutable-flag"
            if (
                executed["path"] == source["path"]
                or os.path.dirname(executed["path"])
                != os.path.dirname(source["path"])
                or not _is_ephemeral_execution_path(executed["path"])
                or executed["uid"] != os.getuid()
                or executed["mode"] != 0o500
                or executed["nlink"] != 1
                or receipt["protection"]
                not in {
                    "user-immutable-flag",
                    "exclusive-current-uid-attestation",
                }
                or immutable_protection
                and (
                    immutable_flag == 0 or executed["flags"] & immutable_flag == 0
                )
                or not immutable_protection
                and immutable_flag != 0
                and executed["flags"] & immutable_flag != 0
                or receipt["path_removed"] is not True
            ):
                return False
        elif strategy == "trusted-direct":
            if (
                executed != source_execution
                or receipt["protection"] != "root-owned-directory-chain"
                or receipt["path_removed"] is not False
            ):
                return False
        elif strategy == "current-process":
            if (
                executed != source_execution
                or receipt["protection"] != "already-running-current-process"
                or receipt["path_removed"] is not False
            ):
                return False
        else:
            return False
        counts[tool] += 1
    expected = {
        "cargo": 1,
        "git": 1,
        "pmset": 2 if identities["pmset"]["state"] == "available" else 0,
        "ps": 1,
        "python": 1,
        "rustc": 1,
        "rustup": 2 if identities["rustup"]["state"] == "available" else 0,
        "sysctl": 1 if identities["sysctl"]["state"] == "available" else 0,
    }
    return counts == expected


def validate_identity(value: object) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == {"device", "inode"}
        and isinstance(value["device"], int)
        and not isinstance(value["device"], bool)
        and value["device"] >= 0
        and isinstance(value["inode"], int)
        and not isinstance(value["inode"], bool)
        and value["inode"] > 0
    )


def bind_active_agent_attestation(
    phase_evidence: dict[str, Any], attestation: dict[str, Any]
) -> dict[str, Any]:
    """Bind an operator assertion to one phase; malicious same-UID actors are out of scope."""

    if (
        not isinstance(attestation, dict)
        or set(attestation) != {"device", "inode", "sha256"}
        or not validate_identity(
            {"device": attestation.get("device"), "inode": attestation.get("inode")}
        )
        or not is_lower_digest(attestation.get("sha256"))
    ):
        raise GateError("active-agent attestation identity is malformed")
    bound = {
        **attestation,
        "claim": ACTIVE_AGENT_CLAIM,
        "uid": os.getuid(),
        "authority_binding_sha256": "",
    }
    phase_evidence["active_agent_attestation"] = bound
    bound["authority_binding_sha256"] = digest_json(
        _attestation_authority_binding(phase_evidence)
    )
    return bound


def validate_measurement_inputs(value: object, backend: str) -> bool:
    if value is None:
        return backend == "standard"
    required = {"runner", "execution_runner", "build_evidence"}
    if backend == "candidate":
        required.update({"baseline_manifest", "baseline_lock"})
    if not isinstance(value, dict) or set(value) != required:
        return False
    return all(
        isinstance(binding, dict)
        and set(binding) == {"device", "inode", "size", "sha256"}
        and validate_identity({"device": binding.get("device"), "inode": binding.get("inode")})
        and isinstance(binding["size"], int)
        and not isinstance(binding["size"], bool)
        and binding["size"] > 0
        and is_lower_digest(binding["sha256"])
        for binding in value.values()
    )


def validate_redacted_processes(value: object) -> bool:
    if not isinstance(value, list) or len(value) > MAX_PROCESSES:
        return False
    previous_pid = 0
    for process in value:
        if (
            not isinstance(process, dict)
            or set(process) != {"pid", "ppid", "state", "comm", "command_class"}
            or not isinstance(process["pid"], int)
            or isinstance(process["pid"], bool)
            or process["pid"] <= previous_pid
            or not isinstance(process["ppid"], int)
            or isinstance(process["ppid"], bool)
            or process["ppid"] < 0
            or not isinstance(process["state"], str)
            or len(process["state"]) > 16
            or not isinstance(process["comm"], str)
            or len(process["comm"].encode()) > 256
            or process["command_class"]
            not in {"capture-evidence", "criterion-zero-authority", "rustc", "cargo", "other"}
        ):
            return False
        previous_pid = process["pid"]
    return True


def validate_phase_evidence(value: object, phase: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != PHASE_EVIDENCE_FIELDS:
        raise GateError("host phase evidence has an invalid schema")
    backend = value["benchmark_backend"]
    baseline_manifest = value["baseline_manifest_sha256"]
    baseline_lock = value["baseline_lock_sha256"]
    baseline_valid = (
        backend == "standard" and baseline_manifest is None and baseline_lock is None
    ) or (
        backend == "candidate"
        and is_lower_digest(baseline_manifest)
        and is_lower_digest(baseline_lock)
    )
    if (
        value["schema_version"] != SCHEMA_VERSION
        or value["evidence_mode"] not in {PRODUCTION_MODE, FIXTURE_MODE}
        or value["phase"] != phase
        or not validate_stable_host(value["stable_host"])
        or not validate_stable_toolchain(value["stable_toolchain"], value["evidence_mode"])
        or digest_json(value["stable_host"]) != value["host_fingerprint_sha256"]
        or digest_json(value["stable_toolchain"]) != value["toolchain_fingerprint_sha256"]
        or not validate_tool_executions(
            value["tool_executions"], value["stable_toolchain"]["tool_identities"]
        )
        or digest_json(value["tool_executions"]) != value["tool_execution_sha256"]
        or value["release_profile_sha256"] != digest_bytes(PROFILE.encode())
        or not is_git_head(value["measured_code_head"])
        or not baseline_valid
        or not is_lower_digest(value["process_inventory_sha256"])
        or not validate_redacted_processes(value["processes"])
        or digest_json(value["processes"]) != value["process_inventory_sha256"]
        or not isinstance(value["competitor_pids"], list)
        or not all(
            isinstance(pid, int) and not isinstance(pid, bool) and pid > 0
            for pid in value["competitor_pids"]
        )
        or not isinstance(value["wall_ns"], int)
        or isinstance(value["wall_ns"], bool)
        or value["wall_ns"] <= 0
        or not isinstance(value["monotonic_ns"], int)
        or isinstance(value["monotonic_ns"], bool)
        or value["monotonic_ns"] <= 0
        or not isinstance(value["load_one_minute"], float)
        or value["load_one_minute"] < 0
        or not isinstance(value["normalized_load"], float)
        or value["normalized_load"] < 0
        or not is_lower_digest(value["lock_nonce_sha256"])
        or not validate_identity(value["lock_identity"])
        or not validate_identity(value["owner_identity"])
        or not validate_identity(value["output_identity"])
        or not isinstance(value["active_agent_attestation"], dict)
        or set(value["active_agent_attestation"])
        != {
            "device",
            "inode",
            "sha256",
            "claim",
            "uid",
            "authority_binding_sha256",
        }
        or not validate_identity(
            {
                "device": value["active_agent_attestation"].get("device"),
                "inode": value["active_agent_attestation"].get("inode"),
            }
        )
        or not is_lower_digest(value["active_agent_attestation"].get("sha256"))
        or value["active_agent_attestation"].get("claim") != ACTIVE_AGENT_CLAIM
        or not isinstance(value["active_agent_attestation"].get("uid"), int)
        or isinstance(value["active_agent_attestation"].get("uid"), bool)
        or value["active_agent_attestation"].get("uid") != os.getuid()
        or value["active_agent_attestation"].get("authority_binding_sha256")
        != digest_json(_attestation_authority_binding(value))
        or not validate_measurement_inputs(value["measurement_inputs"], backend)
    ):
        raise GateError("host phase evidence values are invalid")
    return value


def validate_phase_stability(preflight: dict[str, Any], postflight: dict[str, Any]) -> None:
    stable_attestation = {"device", "inode", "sha256", "claim", "uid"}
    if (
        any(
            preflight.get(field) != postflight.get(field)
            for field in STABLE_PHASE_FIELDS - {"active_agent_attestation"}
        )
        or {
            field: preflight.get("active_agent_attestation", {}).get(field)
            for field in stable_attestation
        }
        != {
            field: postflight.get("active_agent_attestation", {}).get(field)
            for field in stable_attestation
        }
    ):
        raise GateError("postflight stable host evidence changed")


def _attestation_authority_binding(value: dict[str, Any]) -> dict[str, Any]:
    attestation = value["active_agent_attestation"]
    return {
        "claim": attestation.get("claim"),
        "uid": attestation.get("uid"),
        "attestation": {
            field: attestation.get(field) for field in ("device", "inode", "sha256")
        },
        "phase": value.get("phase"),
        "host_fingerprint_sha256": value.get("host_fingerprint_sha256"),
        "toolchain_fingerprint_sha256": value.get("toolchain_fingerprint_sha256"),
        "tool_execution_sha256": value.get("tool_execution_sha256"),
        "process_inventory_sha256": value.get("process_inventory_sha256"),
        "measured_code_head": value.get("measured_code_head"),
        "wall_ns": value.get("wall_ns"),
        "monotonic_ns": value.get("monotonic_ns"),
        "lock_nonce_sha256": value.get("lock_nonce_sha256"),
        "lock_identity": value.get("lock_identity"),
        "owner_identity": value.get("owner_identity"),
    }
