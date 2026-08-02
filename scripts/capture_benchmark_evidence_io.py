#!/usr/bin/env python3
"""Descriptor-capability I/O for authoritative capture-benchmark evidence."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePath
from typing import Any

if __package__:
    from .capture_benchmark_process import GateError, bounded_process
else:
    from capture_benchmark_process import GateError, bounded_process

MAX_JSON_BYTES = 1024 * 1024
MAX_COMMAND_BYTES = 4 * 1024 * 1024
NOFOLLOW = getattr(os, "O_NOFOLLOW", None)
DIRECTORY = getattr(os, "O_DIRECTORY", None)
REPOSITORY = Path(__file__).resolve().parents[1]
CLOSED_COMMAND_ENVIRONMENT = {
    "PATH": "",
    "LANG": "C",
    "LC_ALL": "C",
    "TZ": "UTC",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_CONFIG_GLOBAL": "/dev/null",
    "GIT_TERMINAL_PROMPT": "0",
    "RUSTUP_AUTO_INSTALL": "0",
    "CARGO_NET_OFFLINE": "true",
}


class FailureInjection(str, Enum):
    """Closed fixture-only faults exercised by the host-gate contract suite."""

    FILE_FSYNC = "file-fsync"
    DIRECTORY_FSYNC = "dir-fsync"
    AFTER_OWNER = "after-owner"
    AFTER_POSTFLIGHT = "after-postflight"
    MISSING_PRIMITIVES = "missing-primitives"
    PARTIAL_OWNER_WRITE = "partial-owner-write"
    OWNER_WRITE_FAILURE = "owner-write-failure"
    MONITOR_COMPETITOR = "monitor-competitor"
    ROOT_OPEN_IDENTITY_MISMATCH = "root-open-identity-mismatch"
    PARTIAL_DESCRIPTOR_READ = "partial-descriptor-read"
    POST_READ_IDENTITY_MISMATCH = "post-read-identity-mismatch"
    PUBLICATION_IDENTITY_MISMATCH = "publication-identity-mismatch"


_ACTIVE_FAILURE_INJECTION: ContextVar[FailureInjection | None] = ContextVar(
    "capture_benchmark_evidence_io_failure_injection", default=None
)


@contextmanager
def _fixture_failure_injection(
    injection: FailureInjection, evidence_mode: str
) -> Iterator[None]:
    """Activate one exact fault inside a non-nestable fixture-only context."""

    if evidence_mode != "fixture" or type(injection) is not FailureInjection:
        raise GateError("failure injection requires an exact fixture-only fault")
    if _ACTIVE_FAILURE_INJECTION.get() is not None:
        raise GateError("fixture failure-injection scopes cannot be nested")
    token = _ACTIVE_FAILURE_INJECTION.set(injection)
    try:
        yield
    finally:
        _ACTIVE_FAILURE_INJECTION.reset(token)


def _failure_injected(injection: FailureInjection) -> bool:
    return _ACTIVE_FAILURE_INJECTION.get() is injection


def require_platform_primitives() -> None:
    """Require the descriptor-relative primitives used to enforce capability I/O."""

    required_dir_fd = {os.open, os.mkdir, os.stat, os.unlink, os.rmdir, os.link}
    if (
        _failure_injected(FailureInjection.MISSING_PRIMITIVES)
        or NOFOLLOW is None
        or NOFOLLOW == 0
        or DIRECTORY is None
        or DIRECTORY == 0
        or not required_dir_fd.issubset(os.supports_dir_fd)
        or os.stat not in os.supports_follow_symlinks
        or os.link not in os.supports_follow_symlinks
    ):
        raise GateError("required descriptor-relative no-follow primitives are unavailable")


def canonical_json(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8", "strict")
    except (TypeError, ValueError, RecursionError, UnicodeError) as error:
        raise GateError("evidence JSON cannot be encoded canonically") from error


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_json(value: Any) -> str:
    return digest_bytes(canonical_json(value))


def is_lower_digest(value: object, length: int = 64) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )


def bounded_command(arguments: list[str], *, executable: Path) -> str:
    """Run one bounded local observation command and return strict UTF-8."""

    if not executable.is_absolute():
        raise GateError("bounded command executable is not an absolute binding")
    try:
        completed = bounded_process(
            arguments,
            cwd=REPOSITORY,
            env=dict(CLOSED_COMMAND_ENVIRONMENT),
            timeout_seconds=10,
            maximum_stdout=MAX_COMMAND_BYTES,
            maximum_stderr=MAX_COMMAND_BYTES,
            executable=executable,
        )
    except (OSError, subprocess.SubprocessError, GateError) as error:
        raise GateError("required local observation command failed") from error
    if completed.returncode != 0:
        raise GateError("required local observation command was invalid")
    try:
        return completed.stdout.decode("utf-8", "strict").strip()
    except UnicodeDecodeError as error:
        raise GateError("required local observation command was not UTF-8") from error


@dataclass
class CapabilityRoot:
    """An owned descriptor for a canonical, private evidence directory tree."""

    path: Path
    descriptor: int

    @classmethod
    def open(cls, path: Path) -> "CapabilityRoot":
        require_platform_primitives()
        absolute = Path(os.path.abspath(path))
        metadata = os.lstat(absolute)
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise GateError("controlled evidence root is not a real directory")
        if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise GateError("controlled evidence root ownership or mode is unsafe")
        if Path(os.path.realpath(absolute)) != absolute:
            raise GateError("controlled evidence root is not canonical")
        descriptor = os.open(absolute, os.O_RDONLY | DIRECTORY | NOFOLLOW)
        opened = os.fstat(descriptor)
        if (
            _failure_injected(FailureInjection.ROOT_OPEN_IDENTITY_MISMATCH)
            or (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
            or not stat.S_ISDIR(opened.st_mode)
            or opened.st_uid != metadata.st_uid
            or stat.S_IMODE(opened.st_mode) != stat.S_IMODE(metadata.st_mode)
        ):
            os.close(descriptor)
            raise GateError("controlled evidence root changed before descriptor open")
        return cls(absolute, descriptor)

    def close(self) -> None:
        os.close(self.descriptor)

    def relative(self, path: Path) -> tuple[str, ...]:
        absolute = Path(os.path.abspath(path))
        try:
            relative = absolute.relative_to(self.path)
        except ValueError as error:
            raise GateError("evidence path escapes the controlled root") from error
        parts = relative.parts
        if not parts or any(part in ("", ".", "..") for part in parts):
            raise GateError("evidence path is not a bounded descendant")
        return parts

    def open_directory(self, parts: tuple[str, ...], create_final: bool = False) -> int:
        current = os.dup(self.descriptor)
        try:
            for index, part in enumerate(parts):
                if len(part.encode()) > 255:
                    raise GateError("evidence path component is too long")
                final = index == len(parts) - 1
                if final and create_final:
                    try:
                        os.mkdir(part, 0o700, dir_fd=current)
                    except FileExistsError as error:
                        raise GateError("evidence output directory already exists") from error
                next_descriptor = os.open(
                    part,
                    os.O_RDONLY | DIRECTORY | NOFOLLOW,
                    dir_fd=current,
                )
                next_metadata = os.fstat(next_descriptor)
                if (
                    next_metadata.st_uid != os.getuid()
                    or stat.S_IMODE(next_metadata.st_mode) & 0o077
                ):
                    os.close(next_descriptor)
                    raise GateError("evidence path component ownership or mode is unsafe")
                os.close(current)
                current = next_descriptor
            metadata = os.fstat(current)
            if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
                raise GateError("evidence directory ownership or mode is unsafe")
            return current
        except Exception:
            os.close(current)
            raise

    def read_file(self, path: Path, maximum: int = MAX_JSON_BYTES) -> bytes:
        value, _device, _inode = self.read_file_identity(path, maximum)
        return value

    def read_file_identity(
        self, path: Path, maximum: int = MAX_JSON_BYTES
    ) -> tuple[bytes, int, int]:
        parts = self.relative(path)
        parent = self.open_directory(parts[:-1]) if len(parts) > 1 else os.dup(self.descriptor)
        try:
            descriptor = os.open(parts[-1], os.O_RDONLY | NOFOLLOW, dir_fd=parent)
            try:
                metadata = os.fstat(descriptor)
                if not _private_regular_metadata(metadata, maximum, allow_empty=True):
                    raise GateError("evidence input is not a bounded private regular file")
                value = read_exact_descriptor(descriptor, metadata.st_size, maximum)
                after = os.fstat(descriptor)
                current = os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)
                if (
                    _failure_injected(FailureInjection.POST_READ_IDENTITY_MISMATCH)
                    or not _same_private_regular_identity(
                        metadata, after, current, maximum, allow_empty=True
                    )
                ):
                    raise GateError("evidence input changed during its read")
                return value, metadata.st_dev, metadata.st_ino
            finally:
                os.close(descriptor)
        finally:
            os.close(parent)

    def stat_file_identity(self, path: Path, maximum: int) -> tuple[int, int, int]:
        parts = self.relative(path)
        parent = self.open_directory(parts[:-1]) if len(parts) > 1 else os.dup(self.descriptor)
        try:
            descriptor = os.open(parts[-1], os.O_RDONLY | NOFOLLOW, dir_fd=parent)
            try:
                opened = os.fstat(descriptor)
                current = os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)
                if not _same_private_regular_identity(
                    opened, opened, current, maximum, allow_empty=False
                ):
                    raise GateError("measured input identity is unsafe or changed")
                return opened.st_dev, opened.st_ino, opened.st_size
            finally:
                os.close(descriptor)
        finally:
            os.close(parent)


def _private_regular_metadata(
    metadata: os.stat_result, maximum: int, allow_empty: bool
) -> bool:
    minimum = 0 if allow_empty else 1
    return (
        stat.S_ISREG(metadata.st_mode)
        and metadata.st_uid == os.getuid()
        and metadata.st_nlink == 1
        and stat.S_IMODE(metadata.st_mode) & 0o077 == 0
        and minimum <= metadata.st_size <= maximum
    )


def _same_private_regular_identity(
    initial: os.stat_result,
    after: os.stat_result,
    current: os.stat_result,
    maximum: int,
    allow_empty: bool,
) -> bool:
    """Validate identity and every security property across a descriptor read."""

    identity = (initial.st_dev, initial.st_ino, initial.st_size)
    return (
        _private_regular_metadata(initial, maximum, allow_empty)
        and _private_regular_metadata(after, maximum, allow_empty)
        and _private_regular_metadata(current, maximum, allow_empty)
        and (after.st_dev, after.st_ino, after.st_size) == identity
        and (current.st_dev, current.st_ino, current.st_size) == identity
    )


def read_exact_descriptor(descriptor: int, expected: int, maximum: int) -> bytes:
    if expected < 0 or expected > maximum:
        raise GateError("descriptor input exceeds its byte bound")
    chunks: list[bytes] = []
    observed = 0
    while observed < expected:
        request = min(1024 * 1024, expected - observed)
        if _failure_injected(FailureInjection.PARTIAL_DESCRIPTOR_READ):
            request = min(request, 7)
        chunk = os.read(descriptor, request)
        if not chunk:
            raise GateError("descriptor input ended before its declared size")
        chunks.append(chunk)
        observed += len(chunk)
    if os.read(descriptor, 1):
        raise GateError("descriptor input grew beyond its declared size")
    return b"".join(chunks)


def publish_json(directory: int, name: str, value: Any) -> tuple[int, int]:
    return publish_bytes(directory, name, canonical_json(value), 0o600, MAX_JSON_BYTES)


def publish_bytes(
    directory: int,
    name: str,
    encoded: bytes,
    mode: int,
    maximum: int,
) -> tuple[int, int]:
    if PurePath(name).name != name or name.startswith(".") or len(name.encode()) > 128:
        raise GateError("evidence artifact name is invalid")
    if not encoded or len(encoded) > maximum or mode not in {0o600, 0o700}:
        raise GateError("evidence artifact exceeds its byte bound")
    temporary = f".tmp-{secrets.token_hex(16)}"
    temporary_exists = True
    final_linked = False
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | NOFOLLOW,
        mode,
        dir_fd=directory,
    )
    metadata = os.fstat(descriptor)
    try:
        written = 0
        while written < len(encoded):
            count = os.write(descriptor, encoded[written:])
            if count <= 0:
                raise GateError("evidence artifact write made no progress")
            written += count
        sync_file(descriptor)
        metadata = os.fstat(descriptor)
        if stat.S_IMODE(metadata.st_mode) != mode or metadata.st_nlink != 1:
            raise GateError("temporary evidence artifact has unsafe metadata")
        try:
            os.link(
                temporary,
                name,
                src_dir_fd=directory,
                dst_dir_fd=directory,
                follow_symlinks=False,
            )
            final_linked = True
        except FileExistsError as error:
            raise GateError("evidence artifact already exists") from error
        final = os.stat(name, dir_fd=directory, follow_symlinks=False)
        if (
            _failure_injected(FailureInjection.PUBLICATION_IDENTITY_MISMATCH)
            or (final.st_dev, final.st_ino, final.st_size)
            != (metadata.st_dev, metadata.st_ino, metadata.st_size)
            or not stat.S_ISREG(final.st_mode)
            or final.st_uid != os.getuid()
            or stat.S_IMODE(final.st_mode) != mode
            or final.st_nlink != 2
        ):
            raise GateError("evidence artifact publication changed identity")
        os.unlink(temporary, dir_fd=directory)
        temporary_exists = False
        final = os.stat(name, dir_fd=directory, follow_symlinks=False)
        if (
            (final.st_dev, final.st_ino, final.st_size)
            != (metadata.st_dev, metadata.st_ino, metadata.st_size)
            or not stat.S_ISREG(final.st_mode)
            or final.st_uid != os.getuid()
            or stat.S_IMODE(final.st_mode) != mode
            or final.st_nlink != 1
        ):
            raise GateError("published evidence artifact metadata is unsafe")
        sync_directory(directory)
        final_linked = False
        return final.st_dev, final.st_ino
    except Exception as original_error:
        rollback_error: OSError | GateError | None = None
        removed = False
        if final_linked:
            try:
                current = os.stat(name, dir_fd=directory, follow_symlinks=False)
                if (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
                    raise GateError("published artifact changed before rollback")
                os.unlink(name, dir_fd=directory)
                final_linked = False
                removed = True
            except (OSError, GateError) as error:
                rollback_error = error
        try:
            if temporary_exists:
                current = os.stat(temporary, dir_fd=directory, follow_symlinks=False)
                if (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
                    raise GateError("temporary artifact changed before rollback")
                os.unlink(temporary, dir_fd=directory)
                temporary_exists = False
                removed = True
        except (OSError, GateError) as error:
            rollback_error = rollback_error or error
        if removed:
            try:
                os.fsync(directory)
            except OSError as error:
                rollback_error = rollback_error or error
        if rollback_error is not None:
            raise GateError("evidence artifact rollback failed closed") from rollback_error
        raise original_error
    finally:
        os.close(descriptor)


def sync_file(descriptor: int) -> None:
    if _failure_injected(FailureInjection.FILE_FSYNC):
        raise GateError("fixture-injected file synchronization failure")
    os.fsync(descriptor)


def sync_directory(descriptor: int) -> None:
    if _failure_injected(FailureInjection.DIRECTORY_FSYNC):
        raise GateError("fixture-injected directory synchronization failure")
    os.fsync(descriptor)


def write_owner_all(descriptor: int, encoded: bytes) -> None:
    if _failure_injected(FailureInjection.OWNER_WRITE_FAILURE):
        raise GateError("fixture-injected owner write failure")
    written = 0
    while written < len(encoded):
        limit = len(encoded)
        if _failure_injected(FailureInjection.PARTIAL_OWNER_WRITE):
            limit = max(1, len(encoded) // 2)
        count = os.write(descriptor, encoded[written:limit])
        if count <= 0:
            raise GateError("owner record write made no progress")
        written += count
        if _failure_injected(FailureInjection.PARTIAL_OWNER_WRITE):
            raise GateError("fixture-injected partial owner write")


def read_json_bytes(value: bytes) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for name, item in pairs:
            if name in result:
                raise GateError("evidence JSON contains a duplicate member")
            result[name] = item
        return result

    def reject_non_finite(name: str) -> None:
        raise GateError(f"evidence JSON contains non-finite constant {name}")

    try:
        decoded = json.loads(
            value,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_non_finite,
        )
    except (UnicodeError, json.JSONDecodeError, RecursionError, GateError) as error:
        raise GateError("evidence JSON is malformed") from error
    if not isinstance(decoded, dict):
        raise GateError("evidence JSON root is not an object")
    return decoded


def production_root() -> Path:
    """Resolve the worktree-shared controlled evidence root below `target`."""

    return _production_root_from_repository(REPOSITORY)


def _production_root_from_repository(repository: Path) -> Path:
    repository = Path(os.path.abspath(repository))
    git_entry = repository / ".git"
    try:
        metadata = os.lstat(git_entry)
    except OSError as error:
        raise GateError("repository Git control entry is unavailable") from error
    if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
        common = _require_control_directory(git_entry)
    elif stat.S_ISREG(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
        line = _read_control_line(git_entry, 4096)
        prefix = "gitdir: "
        if not line.startswith(prefix) or not line[len(prefix) :]:
            raise GateError("worktree Git control file is malformed")
        selected = Path(line[len(prefix) :])
        git_directory = selected if selected.is_absolute() else repository / selected
        git_directory = _require_control_directory(git_directory)
        common_line = _read_control_line(git_directory / "commondir", 4096)
        selected_common = Path(common_line)
        common = (
            selected_common
            if selected_common.is_absolute()
            else git_directory / selected_common
        )
        common = _require_control_directory(common)
        if git_directory.parent.parent != common:
            raise GateError("worktree Git control directory escapes its common directory")
    else:
        raise GateError("repository Git control entry is unsafe")
    if common.name != ".git":
        raise GateError("repository Git common directory is malformed")
    repository = _require_control_directory(common.parent)
    return repository / "target" / "q2-a4-capture-benchmark"


def _read_control_line(path: Path, maximum: int) -> str:
    absolute = Path(os.path.abspath(path))
    try:
        descriptor = os.open(absolute, os.O_RDONLY | NOFOLLOW)
    except OSError as error:
        raise GateError("repository Git control file cannot be opened safely") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid not in {0, os.getuid()}
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) & 0o022
            or not 0 < before.st_size <= maximum
        ):
            raise GateError("repository Git control file metadata is unsafe")
        encoded = read_exact_descriptor(descriptor, before.st_size, maximum)
        after = os.fstat(descriptor)
        current = os.stat(absolute, follow_symlinks=False)
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
            (
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
            raise GateError("repository Git control file changed during its read")
    finally:
        os.close(descriptor)
    try:
        value = encoded.decode("utf-8", "strict")
    except UnicodeDecodeError as error:
        raise GateError("repository Git control file is not UTF-8") from error
    lines = value.splitlines()
    if len(lines) != 1 or not lines[0] or "\x00" in lines[0]:
        raise GateError("repository Git control file is not one bounded line")
    return lines[0]


def _require_control_directory(path: Path) -> Path:
    absolute = Path(os.path.abspath(path))
    try:
        metadata = os.lstat(absolute)
    except OSError as error:
        raise GateError("repository Git control directory is unavailable") from error
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid not in {0, os.getuid()}
        or stat.S_IMODE(metadata.st_mode) & 0o022
        or Path(os.path.realpath(absolute)) != absolute
    ):
        raise GateError("repository Git control directory is unsafe")
    return absolute
