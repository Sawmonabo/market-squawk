#!/usr/bin/env python3
"""Reject generated, credential-file-shaped, or opaque repository inputs."""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
from pathlib import Path, PurePosixPath
import stat
import subprocess
import sys
from typing import NamedTuple


MAX_REPOSITORY_FILE_BYTES = 5 * 1024 * 1024


class ReviewedBinaryIdentity(NamedTuple):
    size_bytes: int
    sha256: str


# Human-approved canonical visual baselines are admitted only at their exact reviewed content
# identity. A same-path replacement must return to design and policy review.
REVIEWED_BINARY_IDENTITIES: Mapping[str, ReviewedBinaryIdentity] = {
    "docs/superpowers/specs/assets/2026-07-28-market-squawk-obsidian-signal.png": ReviewedBinaryIdentity(
        size_bytes=170_712,
        sha256="13584db11237399eb7bafa638f9a34b7970b90c44443c62a76b34e7a9859fe43",
    ),
    "docs/superpowers/specs/assets/2026-07-29-market-squawk-logo.png": ReviewedBinaryIdentity(
        size_bytes=985_371,
        sha256="567a3dc04acb791b67eb2a0bb5eed256cb6c9d84c0441ed2dcefc7ae9b7d6ee6",
    ),
}

# Binary protocol fixtures and shipping visual assets must be individually reviewed and listed by
# exact repository path.
ALLOWED_BINARY_FILES: frozenset[str] = frozenset(
    {
        "apps/market-squawk-desktop/src-tauri/icons/128x128.png",
        "apps/market-squawk-desktop/src-tauri/icons/128x128@2x.png",
        "apps/market-squawk-desktop/src-tauri/icons/32x32.png",
        "apps/market-squawk-desktop/src-tauri/icons/Square107x107Logo.png",
        "apps/market-squawk-desktop/src-tauri/icons/Square142x142Logo.png",
        "apps/market-squawk-desktop/src-tauri/icons/Square150x150Logo.png",
        "apps/market-squawk-desktop/src-tauri/icons/Square284x284Logo.png",
        "apps/market-squawk-desktop/src-tauri/icons/Square30x30Logo.png",
        "apps/market-squawk-desktop/src-tauri/icons/Square310x310Logo.png",
        "apps/market-squawk-desktop/src-tauri/icons/Square44x44Logo.png",
        "apps/market-squawk-desktop/src-tauri/icons/Square71x71Logo.png",
        "apps/market-squawk-desktop/src-tauri/icons/Square89x89Logo.png",
        "apps/market-squawk-desktop/src-tauri/icons/StoreLogo.png",
        "apps/market-squawk-desktop/src-tauri/icons/icon.icns",
        "apps/market-squawk-desktop/src-tauri/icons/icon.ico",
        "apps/market-squawk-desktop/src-tauri/icons/icon.png",
    }
)

BLOCKED_DIRECTORY_COMPONENTS = frozenset(
    {
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".worktrees",
        "__pycache__",
        "build",
        "coverage",
        "dist",
        "node_modules",
        "target",
    }
)
BLOCKED_EXACT_NAMES = frozenset(
    {
        ".coverage",
        ".DS_Store",
        "Desktop.ini",
        "Thumbs.db",
    }
)
BLOCKED_SUFFIXES = frozenset(
    {
        ".cer",
        ".crt",
        ".gcda",
        ".gcno",
        ".key",
        ".log",
        ".mobileprovision",
        ".p12",
        ".pem",
        ".pfx",
        ".profraw",
        ".pyc",
        ".pyo",
        ".swo",
        ".swp",
        ".tmp",
    }
)
ALLOWED_ENVIRONMENT_TEMPLATES = frozenset({".env.example", ".env.sample"})


def repository_root() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return Path(result.stdout.strip())


def git_paths(root: Path, arguments: list[str]) -> set[str]:
    result = subprocess.run(
        ["git", "ls-files", "-z", *arguments],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
    )
    return {
        path.decode("utf-8", errors="surrogateescape")
        for path in result.stdout.split(b"\0")
        if path
    }


def repository_inputs(root: Path) -> list[str]:
    """Return active tracked plus unignored untracked inputs using Git's path authority."""

    listed = git_paths(root, ["--cached", "--others", "--exclude-standard"])
    deleted = git_paths(root, ["--deleted"])
    return sorted(listed - deleted)


def path_violations(path: str) -> list[str]:
    violations: list[str] = []
    parsed = PurePosixPath(path)
    parts = parsed.parts
    if (
        parsed.is_absolute()
        or not parts
        or parsed.as_posix() != path
        or "\\" in path
        or any(part in {"", ".", ".."} for part in parts)
    ):
        return ["path is not a canonical repository-relative path"]

    blocked_directories = sorted(BLOCKED_DIRECTORY_COMPONENTS.intersection(parts[:-1]))
    for directory in blocked_directories:
        violations.append(f"generated directory component {directory!r} is forbidden")

    name = parts[-1]
    if name in BLOCKED_EXACT_NAMES:
        violations.append(f"generated operating-system/tool file {name!r} is forbidden")

    lower_name = name.lower()
    if lower_name == ".env" or (
        lower_name.startswith(".env.") and lower_name not in ALLOWED_ENVIRONMENT_TEMPLATES
    ):
        violations.append("environment secret files are forbidden; commit only example templates")

    for suffix in sorted(BLOCKED_SUFFIXES):
        if lower_name.endswith(suffix):
            violations.append(f"generated or secret-bearing suffix {suffix!r} is forbidden")
            break

    return violations


def content_violations(
    path: str,
    content: bytes,
    *,
    allowed_binary_files: frozenset[str] = ALLOWED_BINARY_FILES,
    reviewed_binary_identities: Mapping[
        str, ReviewedBinaryIdentity
    ] = REVIEWED_BINARY_IDENTITIES,
) -> list[str]:
    if len(content) > MAX_REPOSITORY_FILE_BYTES:
        return [
            f"file exceeds reviewed repository input limit of {MAX_REPOSITORY_FILE_BYTES} bytes"
        ]
    reviewed_identity = reviewed_binary_identities.get(path)
    if reviewed_identity is not None:
        if len(content) != reviewed_identity.size_bytes:
            return [
                "reviewed binary size mismatch: "
                f"expected {reviewed_identity.size_bytes} bytes, found {len(content)}"
            ]
        digest = hashlib.sha256(content).hexdigest()
        if digest != reviewed_identity.sha256:
            return [
                "reviewed binary SHA-256 mismatch: "
                f"expected {reviewed_identity.sha256}, found {digest}"
            ]
        return []
    if path in allowed_binary_files:
        return []
    if b"\0" in content:
        return ["opaque binary content requires an exact reviewed allowance"]
    try:
        content.decode("utf-8")
    except UnicodeDecodeError:
        return ["non-UTF-8 content requires an exact reviewed binary allowance"]
    return []


def inspect_file(root: Path, path: str) -> list[str]:
    violations = path_violations(path)
    absolute_path = root / path
    try:
        metadata = absolute_path.lstat()
    except OSError as error:
        return [*violations, f"file cannot be inspected: {error}"]

    if stat.S_ISLNK(metadata.st_mode):
        return [*violations, "symbolic links require explicit architectural review"]
    if not stat.S_ISREG(metadata.st_mode):
        return [*violations, "repository input is not a regular file"]
    if metadata.st_size > MAX_REPOSITORY_FILE_BYTES:
        return [
            *violations,
            f"file exceeds reviewed repository input limit of {MAX_REPOSITORY_FILE_BYTES} bytes",
        ]

    try:
        content = absolute_path.read_bytes()
    except OSError as error:
        return [*violations, f"file cannot be read: {error}"]
    return [*violations, *content_violations(path, content)]


def main() -> int:
    try:
        root = repository_root()
        inputs = repository_inputs(root)
    except (OSError, subprocess.CalledProcessError) as error:
        print(
            f"generated-artifact check failed to enumerate repository inputs: {error}",
            file=sys.stderr,
        )
        return 1

    violations: list[str] = []
    for path in inputs:
        for violation in inspect_file(root, path):
            violations.append(f"{path}: {violation}")

    for violation in violations:
        print(violation)
    return int(bool(violations))


if __name__ == "__main__":
    raise SystemExit(main())
