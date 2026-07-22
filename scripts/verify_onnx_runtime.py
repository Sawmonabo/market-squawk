#!/usr/bin/env python3
"""Admit one exact operator-supplied ONNX Runtime library and emit local evidence."""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import platform
import stat
import struct
import sys
import tempfile
from pathlib import Path
from typing import Any

MAX_POLICY_BYTES = 64 * 1024
MAX_PATH_BYTES = 4 * 1024
SHA256_HEX_BYTES = 64


class AdmissionError(Exception):
    """Expected closed admission failure."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify a configured local ONNX Runtime library and write hash-bound admission "
            "evidence. This command performs no download, installation, or network request."
        )
    )
    parser.add_argument("--policy", required=True, type=Path, help="tracked verifier policy")
    parser.add_argument("--library", required=True, type=Path, help="operator-supplied library")
    parser.add_argument("--head", help="exact 40-character release commit identity")
    parser.add_argument("--tree", help="exact 40-character release tree identity")
    parser.add_argument("--report", type=Path, help="write exact-head release evidence")
    return parser.parse_args()


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise AdmissionError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def read_bounded_bytes(path: Path, limit: int) -> bytes:
    with path.open("rb") as handle:
        original_size = os.fstat(handle.fileno()).st_size
        if original_size <= 0 or original_size > limit:
            raise AdmissionError("file is empty or exceeds its size limit")
        data = bytearray()
        while len(data) <= limit:
            chunk = handle.read(min(64 * 1024, limit + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
        final_size = os.fstat(handle.fileno()).st_size
    if len(data) != original_size or final_size != original_size:
        raise AdmissionError("file size changed during its bounded read")
    return bytes(data)


def read_policy(path: Path) -> tuple[dict[str, Any], bytes, Path]:
    canonical = path.resolve(strict=True)
    if not canonical.is_file():
        raise AdmissionError("policy must be a regular file")
    raw = read_bounded_bytes(canonical, MAX_POLICY_BYTES)
    try:
        value = json.loads(raw, object_pairs_hook=reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AdmissionError("policy is not canonical JSON") from error
    if not isinstance(value, dict):
        raise AdmissionError("policy root must be an object")
    return value, raw, canonical


def exact_keys(value: Any, keys: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise AdmissionError(f"{name} has an unexpected schema")
    return value


def exact_string(value: Any, name: str, limit: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > limit:
        raise AdmissionError(f"{name} must be a bounded nonempty string")
    return value


def exact_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AdmissionError(f"{name} must be an integer")
    return value


def lower_sha256(value: str, name: str) -> str:
    invalid_character = any(character not in "0123456789abcdef" for character in value)
    if len(value) != SHA256_HEX_BYTES or invalid_character:
        raise AdmissionError(f"{name} must be a lowercase SHA-256 digest")
    if value == "0" * SHA256_HEX_BYTES:
        raise AdmissionError(f"{name} cannot be the reserved zero digest")
    return value


def sha256_file(path: Path, limit: int) -> tuple[str, int]:
    size = path.stat().st_size
    if size <= 0 or size > limit:
        raise AdmissionError("file size is outside the configured limit")
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as handle:
        while chunk := handle.read(64 * 1024):
            total += len(chunk)
            if total > limit:
                raise AdmissionError("file grew beyond the configured limit")
            digest.update(chunk)
    if total != size:
        raise AdmissionError("file size changed during verification")
    return digest.hexdigest(), size


def environment_value(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value or len(value.encode("utf-8")) > MAX_PATH_BYTES:
        raise AdmissionError(f"required environment variable {name} is missing or oversized")
    return value


def controlled_path(
    root: Path,
    raw_path: Path,
    *,
    allow_missing_leaf: bool,
) -> tuple[Path, str]:
    candidate = Path(os.path.abspath(raw_path))
    try:
        relative = candidate.relative_to(root)
    except ValueError as error:
        raise AdmissionError("configured path escapes the runtime root") from error
    if not relative.parts or len(relative.as_posix().encode("utf-8")) > MAX_PATH_BYTES:
        raise AdmissionError("configured path is empty or oversized")

    current = root
    for index, component in enumerate(relative.parts):
        current /= component
        is_leaf = index == len(relative.parts) - 1
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            if allow_missing_leaf and is_leaf:
                continue
            raise AdmissionError("configured path does not exist") from None
        if stat.S_ISLNK(metadata.st_mode):
            raise AdmissionError("configured path contains a symlink")

    if allow_missing_leaf and not candidate.exists():
        parent = candidate.parent.resolve(strict=True)
        if not parent.is_relative_to(root):
            raise AdmissionError("evidence parent escapes the runtime root")
        return candidate, relative.as_posix()

    canonical = candidate.resolve(strict=True)
    if not canonical.is_relative_to(root):
        raise AdmissionError("configured path escapes the runtime root")
    return canonical, relative.as_posix()


def current_platform() -> str:
    operating_system = platform.system().lower()
    machine = platform.machine().lower()
    architecture = {
        "aarch64": "arm64",
        "arm64": "arm64",
        "amd64": "x86_64",
        "x86_64": "x86_64",
    }.get(machine)
    system_name = {"darwin": "macos", "linux": "linux", "windows": "windows"}.get(
        operating_system
    )
    format_name = {"darwin": "macho", "linux": "elf", "windows": "pe"}.get(operating_system)
    if architecture is None or system_name is None or format_name is None:
        raise AdmissionError("host platform is not admitted")
    return f"{system_name}-{architecture}-{format_name}"


def verify_binary_header(path: Path, platform_id: str) -> None:
    with path.open("rb") as handle:
        header = handle.read(64)
        if len(header) != 64:
            raise AdmissionError("runtime library header is truncated")
        if platform_id == "macos-arm64-macho":
            valid = (
                header[:4] == b"\xcf\xfa\xed\xfe"
                and struct.unpack_from("<I", header, 4)[0] == 0x0100000C
            )
            valid = valid and struct.unpack_from("<I", header, 12)[0] == 6
        elif platform_id == "macos-x86_64-macho":
            valid = (
                header[:4] == b"\xcf\xfa\xed\xfe"
                and struct.unpack_from("<I", header, 4)[0] == 0x01000007
            )
            valid = valid and struct.unpack_from("<I", header, 12)[0] == 6
        elif platform_id == "linux-arm64-elf":
            valid = (
                header[:6] == b"\x7fELF\x02\x01"
                and struct.unpack_from("<H", header, 16)[0] == 3
                and struct.unpack_from("<H", header, 18)[0] == 183
            )
        elif platform_id == "linux-x86_64-elf":
            valid = (
                header[:6] == b"\x7fELF\x02\x01"
                and struct.unpack_from("<H", header, 16)[0] == 3
                and struct.unpack_from("<H", header, 18)[0] == 62
            )
        elif platform_id in {"windows-arm64-pe", "windows-x86_64-pe"}:
            pe_offset = struct.unpack_from("<I", header, 60)[0]
            handle.seek(pe_offset)
            pe_header = handle.read(24)
            expected_machine = 0xAA64 if platform_id == "windows-arm64-pe" else 0x8664
            valid = (
                header[:2] == b"MZ"
                and len(pe_header) == 24
                and pe_header[:4] == b"PE\0\0"
                and struct.unpack_from("<H", pe_header, 4)[0] == expected_machine
                and struct.unpack_from("<H", pe_header, 22)[0] & 0x2000 != 0
            )
        else:
            valid = False
    if not valid:
        raise AdmissionError(
            "runtime library does not match the host binary format and architecture"
        )


def runtime_version(path: Path) -> str:
    try:
        if os.name == "nt":
            library = ctypes.WinDLL(str(path))
            function_type = ctypes.WINFUNCTYPE
        else:
            mode = os.RTLD_LOCAL | os.RTLD_NOW
            library = ctypes.CDLL(str(path), mode=mode)
            function_type = ctypes.CFUNCTYPE

        get_version_type = function_type(ctypes.c_char_p)

        class OrtApiBase(ctypes.Structure):
            _fields_ = [("GetApi", ctypes.c_void_p), ("GetVersionString", get_version_type)]

        get_api_base = library.OrtGetApiBase
        get_api_base.argtypes = []
        get_api_base.restype = ctypes.POINTER(OrtApiBase)
        api_base = get_api_base()
        if not api_base:
            raise AdmissionError("OrtGetApiBase returned null")
        raw_version = api_base.contents.GetVersionString()
        if raw_version is None:
            raise AdmissionError("GetVersionString returned null")
        version = raw_version.decode("ascii", errors="strict")
    except (AttributeError, OSError, UnicodeDecodeError, ValueError) as error:
        raise AdmissionError("runtime library identity could not be queried") from error
    if not version or len(version) > 32:
        raise AdmissionError("runtime version is empty or oversized")
    return version


def write_atomic(path: Path, payload: bytes) -> str:
    if path.exists() and path.is_symlink():
        raise AdmissionError("evidence path is a symlink")
    descriptor, temporary_name = tempfile.mkstemp(prefix=".onnx-admission-", dir=path.parent)
    temporary = Path(temporary_name)
    descriptor_open = True
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor_open = False
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        if descriptor_open:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise
    return hashlib.sha256(payload).hexdigest()


def verify() -> dict[str, str]:
    arguments = parse_args()
    if (arguments.head is None) != (arguments.tree is None):
        raise AdmissionError("--head and --tree must be supplied together")
    if arguments.report is not None and arguments.head is None:
        raise AdmissionError("--report requires --head and --tree")
    for name, value in (("head", arguments.head), ("tree", arguments.tree)):
        if value is not None and (
            len(value) != 40 or any(character not in "0123456789abcdef" for character in value)
        ):
            raise AdmissionError(f"{name} must be a lowercase 40-character Git identity")
    policy, policy_bytes, policy_path = read_policy(arguments.policy)
    exact_keys(
        policy,
        {
            "environment",
            "evidence_schema_version",
            "notice",
            "platforms",
            "runtime",
            "schema_version",
            "sources",
        },
        "policy",
    )
    if exact_integer(policy["schema_version"], "schema_version") != 1:
        raise AdmissionError("unsupported policy schema")
    evidence_schema = exact_integer(policy["evidence_schema_version"], "evidence_schema_version")
    if evidence_schema != 1:
        raise AdmissionError("unsupported evidence schema")

    environment = exact_keys(
        policy["environment"],
        {"evidence_path", "expected_sha256", "runtime_root"},
        "environment policy",
    )
    runtime = exact_keys(
        policy["runtime"], {"api_version", "max_library_bytes", "version"}, "runtime policy"
    )
    notice = exact_keys(policy["notice"], {"path", "sha256"}, "notice policy")
    runtime_version_expected = exact_string(runtime["version"], "runtime version", 32)
    api_version = exact_integer(runtime["api_version"], "API version")
    if runtime_version_expected != "1.24.4" or api_version != 24:
        raise AdmissionError("runtime policy differs from the compiled optional backend")
    max_library_bytes = exact_integer(runtime["max_library_bytes"], "library size limit")
    if max_library_bytes != 512 * 1024 * 1024:
        raise AdmissionError("library size limit differs from the compiled optional backend")

    platforms = policy["platforms"]
    valid_platforms = (
        isinstance(platforms, list)
        and bool(platforms)
        and all(isinstance(item, str) for item in platforms)
    )
    if not valid_platforms:
        raise AdmissionError("platform policy must be a nonempty string list")
    platform_id = current_platform()
    if platform_id not in platforms:
        raise AdmissionError("host platform is not present in policy")

    repository_root = policy_path.parents[2]
    notice_relative = Path(exact_string(notice["path"], "notice path"))
    notice_path = (repository_root / notice_relative).resolve(strict=True)
    if not notice_path.is_relative_to(repository_root) or not notice_path.is_file():
        raise AdmissionError("notice path escapes the repository or is not a file")
    notice_digest, _ = sha256_file(notice_path, MAX_POLICY_BYTES)
    expected_notice_digest = lower_sha256(
        exact_string(notice["sha256"], "notice digest"), "notice digest"
    )
    if notice_digest != expected_notice_digest:
        raise AdmissionError("ONNX Runtime notice digest differs")

    root_name = exact_string(environment["runtime_root"], "runtime-root variable")
    digest_name = exact_string(environment["expected_sha256"], "digest variable")
    evidence_name = exact_string(environment["evidence_path"], "evidence variable")
    root = Path(environment_value(root_name)).resolve(strict=True)
    if not root.is_dir():
        raise AdmissionError("runtime root is not a directory")
    library, library_relative = controlled_path(root, arguments.library, allow_missing_leaf=False)
    if not library.is_file():
        raise AdmissionError("runtime library is not a regular file")
    expected_digest = lower_sha256(environment_value(digest_name), "configured library digest")
    actual_digest, library_size = sha256_file(library, max_library_bytes)
    if actual_digest != expected_digest:
        raise AdmissionError("runtime library digest differs")
    verify_binary_header(library, platform_id)
    actual_version = runtime_version(library)
    if actual_version != runtime_version_expected:
        raise AdmissionError("runtime library version differs")

    evidence_path, _ = controlled_path(
        root, Path(environment_value(evidence_name)), allow_missing_leaf=True
    )
    if evidence_path == library:
        raise AdmissionError("evidence path must differ from the runtime library")
    evidence = {
        "library_relative_path": library_relative,
        "library_sha256": actual_digest,
        "library_size_bytes": library_size,
        "platform": platform_id,
        "policy_sha256": hashlib.sha256(policy_bytes).hexdigest(),
        "runtime_version": actual_version,
        "schema_version": evidence_schema,
    }
    evidence_bytes = (json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n").encode()
    evidence_digest = write_atomic(evidence_path, evidence_bytes)
    result = {
        "evidence_path": str(evidence_path),
        "evidence_sha256": evidence_digest,
        "library_sha256": actual_digest,
        "platform": platform_id,
        "policy_sha256": evidence["policy_sha256"],
        "runtime_version": actual_version,
    }
    if arguments.report is not None:
        report_path = Path(os.path.abspath(arguments.report))
        if not report_path.parent.resolve(strict=True).is_dir():
            raise AdmissionError("report parent is not a directory")
        if report_path in {library, evidence_path, policy_path, notice_path}:
            raise AdmissionError("report path collides with an admitted input or evidence file")
        report = {
            "evidence_sha256": evidence_digest,
            "head": arguments.head,
            "library_sha256": actual_digest,
            "library_size_bytes": library_size,
            "notice_sha256": notice_digest,
            "platform": platform_id,
            "policy_sha256": evidence["policy_sha256"],
            "runtime_version": actual_version,
            "schema_version": 1,
            "tree": arguments.tree,
        }
        report_bytes = (json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n").encode()
        result["report_sha256"] = write_atomic(report_path, report_bytes)
    return result


def main() -> int:
    try:
        result = verify()
    except (AdmissionError, OSError) as error:
        print(f"ONNX Runtime admission failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
