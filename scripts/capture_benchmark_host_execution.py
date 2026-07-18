#!/usr/bin/env python3
"""Retained same-parent executable copies for bound host observations."""

from __future__ import annotations

import errno
import hashlib
import os
import secrets
import stat
from collections.abc import Callable
from pathlib import Path
from typing import Any

if __package__:
    from .capture_benchmark_evidence_io import (
        DIRECTORY,
        GateError,
        NOFOLLOW,
        digest_bytes,
        sync_directory,
    )
else:
    from capture_benchmark_evidence_io import (
        DIRECTORY,
        GateError,
        NOFOLLOW,
        digest_bytes,
        sync_directory,
    )

IdentityProvider = Callable[..., dict[str, Any]]


def create_ephemeral_execution(
    identity: dict[str, Any], identity_provider: IdentityProvider
) -> dict[str, Any]:
    """Create and retain one no-clobber copy beside a user-owned tool."""

    source = Path(identity["path"])
    contents = _read_bound_source(identity)
    parent = source.parent
    name = f".market-squawk-bound-{secrets.token_hex(16)}"
    descriptor = -1
    path = parent / name
    immutable = False
    directory = os.open(parent, os.O_RDONLY | DIRECTORY | NOFOLLOW)
    try:
        directory_metadata = os.fstat(directory)
        current_directory = os.stat(parent, follow_symlinks=False)
        if (
            not stat.S_ISDIR(directory_metadata.st_mode)
            or directory_metadata.st_uid != os.getuid()
            or stat.S_IMODE(directory_metadata.st_mode) & 0o022
            or (directory_metadata.st_dev, directory_metadata.st_ino)
            != (current_directory.st_dev, current_directory.st_ino)
        ):
            raise GateError("ephemeral execution directory authority is unsafe")
        descriptor = os.open(
            name,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | NOFOLLOW,
            0o500,
            dir_fd=directory,
        )
        written = 0
        while written < len(contents):
            count = os.write(descriptor, contents[written:])
            if count <= 0:
                raise GateError("ephemeral executable copy write made no progress")
            written += count
        os.fchmod(descriptor, 0o500)
        os.fsync(descriptor)
        immutable_flag = getattr(stat, "UF_IMMUTABLE", 0)
        if immutable_flag and hasattr(os, "chflags"):
            current_flags = getattr(os.fstat(descriptor), "st_flags", 0)
            try:
                os.chflags(path, current_flags | immutable_flag, follow_symlinks=False)
            except OSError as error:
                unsupported = {errno.ENOTSUP, errno.EPERM}
                if hasattr(errno, "EOPNOTSUPP"):
                    unsupported.add(errno.EOPNOTSUPP)
                if error.errno not in unsupported:
                    raise
            else:
                immutable = True
        sync_directory(directory)
        copied = identity_provider(path, execution_strategy="ephemeral-copy")
        if copied["sha256"] != identity["sha256"] or copied["size"] != identity["size"]:
            raise GateError("ephemeral executable differs from its source binding")
        if immutable:
            protection = "user-immutable-flag"
        else:
            # This is an evidence-model exclusion, not protection from a
            # malicious process already operating as the current UID.
            protection = "exclusive-current-uid-attestation"
        return {
            "path": path,
            "name": name,
            "descriptor": descriptor,
            "directory": directory,
            "identity": execution_identity(copied),
            "protection": protection,
            "immutable": immutable,
        }
    except BaseException:
        cleanup_failure: BaseException | None = None
        if immutable:
            try:
                _clear_immutable(path)
            except BaseException as error:
                cleanup_failure = error
        try:
            os.unlink(name, dir_fd=directory)
            sync_directory(directory)
        except FileNotFoundError:
            pass
        except BaseException as error:
            if cleanup_failure is None:
                cleanup_failure = error
        for opened in (descriptor, directory):
            if opened < 0:
                continue
            try:
                os.close(opened)
            except BaseException as error:
                if cleanup_failure is None:
                    cleanup_failure = error
        if cleanup_failure is not None:
            raise GateError(
                f"ephemeral executable creation cleanup failed; inspect {path}"
            ) from cleanup_failure
        raise


def _read_bound_source(identity: dict[str, Any]) -> bytes:
    path = Path(identity["path"])
    descriptor = os.open(path, os.O_RDONLY | NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        if execution_identity_from_metadata(
            path, before, identity["sha256"]
        ) != execution_identity(identity):
            raise GateError("source executable changed before its copy")
        contents = bytearray()
        while len(contents) < before.st_size:
            chunk = os.read(
                descriptor, min(1024 * 1024, before.st_size - len(contents))
            )
            if not chunk:
                raise GateError("source executable ended before its declared size")
            contents.extend(chunk)
        if os.read(descriptor, 1) or digest_bytes(bytes(contents)) != identity["sha256"]:
            raise GateError("source executable content changed during its copy")
        after = os.fstat(descriptor)
        if execution_identity_from_metadata(
            path, after, identity["sha256"]
        ) != execution_identity(identity):
            raise GateError("source executable changed during its copy")
        return bytes(contents)
    finally:
        os.close(descriptor)


def require_ephemeral_execution(binding: dict[str, Any]) -> None:
    """Revalidate the retained descriptor, path identity, and content digest."""

    metadata = os.fstat(binding["descriptor"])
    current = os.stat(binding["path"], follow_symlinks=False)
    expected = binding["identity"]
    if (
        execution_identity_from_metadata(
            binding["path"],
            metadata,
            _descriptor_digest(binding["descriptor"], metadata.st_size),
        )
        != expected
        or execution_identity_from_metadata(
            binding["path"], current, expected["sha256"]
        )
        != expected
    ):
        raise GateError("ephemeral executable identity changed around execution")


def remove_ephemeral_execution(binding: dict[str, Any]) -> bool:
    """Remove a retained copy and surface its exact path on cleanup failure."""

    directory = binding["directory"]
    descriptor = binding["descriptor"]
    validation_failure: BaseException | None = None
    cleanup_failure: BaseException | None = None
    removed = False
    try:
        try:
            require_ephemeral_execution(binding)
        except BaseException as error:
            validation_failure = error
        if binding["immutable"]:
            try:
                _clear_immutable(binding["path"])
            except BaseException as error:
                cleanup_failure = error
        try:
            os.unlink(binding["name"], dir_fd=directory)
            sync_directory(directory)
        except BaseException as error:
            if cleanup_failure is None:
                cleanup_failure = error
        try:
            os.stat(binding["name"], dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            removed = True
        except BaseException as error:
            if cleanup_failure is None:
                cleanup_failure = error
        else:
            if cleanup_failure is None:
                cleanup_failure = GateError(
                    "ephemeral executable remained after cleanup"
                )
    finally:
        for opened in (descriptor, directory):
            try:
                os.close(opened)
            except BaseException as error:
                if cleanup_failure is None:
                    cleanup_failure = error
    if cleanup_failure is not None or not removed:
        raise GateError(
            f"ephemeral executable cleanup failed; inspect {binding['path']}"
        ) from cleanup_failure
    if validation_failure is not None:
        raise GateError(
            "ephemeral executable identity changed; retained copy was removed"
        ) from validation_failure
    return True


def _clear_immutable(path: Path) -> None:
    metadata = os.stat(path, follow_symlinks=False)
    immutable_flag = getattr(stat, "UF_IMMUTABLE", 0)
    os.chflags(
        path,
        getattr(metadata, "st_flags", 0) & ~immutable_flag,
        follow_symlinks=False,
    )


def _descriptor_digest(descriptor: int, size: int) -> str:
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    observed = 0
    while observed < size:
        chunk = os.read(descriptor, min(1024 * 1024, size - observed))
        if not chunk:
            raise GateError("ephemeral executable ended before its declared size")
        digest.update(chunk)
        observed += len(chunk)
    if os.read(descriptor, 1):
        raise GateError("ephemeral executable grew beyond its declared size")
    return digest.hexdigest()


def execution_identity(identity: dict[str, Any]) -> dict[str, Any]:
    return {
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


def execution_identity_from_metadata(
    path: Path, metadata: os.stat_result, digest: str
) -> dict[str, Any]:
    return {
        "path": str(path),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "size": metadata.st_size,
        "uid": metadata.st_uid,
        "mode": stat.S_IMODE(metadata.st_mode),
        "nlink": metadata.st_nlink,
        "flags": getattr(metadata, "st_flags", 0),
        "sha256": digest,
    }
