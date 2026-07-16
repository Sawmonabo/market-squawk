#!/usr/bin/env python3
"""Reject generated, secret-bearing, or opaque files from repository inputs."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
import stat
import subprocess
import sys


MAX_REPOSITORY_FILE_BYTES = 5 * 1024 * 1024

# Binary protocol fixtures must be individually reviewed and listed by exact repository path.
ALLOWED_BINARY_FILES: frozenset[str] = frozenset()

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


def repository_inputs(root: Path) -> list[str]:
    """Return tracked plus unignored untracked inputs using Git's path authority."""

    result = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
    )
    return sorted(
        path.decode("utf-8", errors="surrogateescape")
        for path in result.stdout.split(b"\0")
        if path
    )


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
) -> list[str]:
    if len(content) > MAX_REPOSITORY_FILE_BYTES:
        return [
            f"file exceeds reviewed repository input limit of {MAX_REPOSITORY_FILE_BYTES} bytes"
        ]
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
