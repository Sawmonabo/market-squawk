#!/usr/bin/env python3
"""Reject stale Market Squawk predecessor branding in repository text files."""

from __future__ import annotations

import os
from pathlib import Path
import stat
import subprocess


MAX_TEXT_BYTES = 2 * 1024 * 1024
TOKENS = (
    "market" + "-engine",
    "Market" + " Engine",
    "." + "mej",
    "ME" + "J1",
)

# Keys are (repository-relative path, one-based line, TOKENS index). Line zero is reserved for an
# explicitly approved compatibility filename. Exact locations keep compatibility references
# visible and prevent a path-wide exception from hiding new branding.
ALLOWED_OCCURRENCES = {
    (".gitattributes", 2, 2),
    ("apps/market-squawk/src/journal.rs", 33, 3),
    ("apps/market-squawk/tests/journal.rs", 31, 3),
    ("apps/market-squawk/tests/journal.rs", 62, 2),
    ("apps/market-squawk/tests/journal.rs", 63, 3),
    ("docs/architecture/current-state.md", 9, 0),
    ("docs/architecture/current-state.md", 208, 2),
    ("docs/architecture/current-state.md", 208, 3),
    ("docs/architecture/target-state.md", 513, 2),
    ("docs/architecture/target-state.md", 513, 3),
    ("docs/plans/implementation-plan.md", 130, 2),
    ("docs/plans/implementation-plan.md", 130, 3),
    ("docs/plans/implementation-plan.md", 146, 3),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 41, 2),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 41, 3),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 103, 3),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 139, 3),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 153, 0),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 154, 1),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 154, 2),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 154, 3),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 1501, 2),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 1501, 3),
}


def repository_root() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return Path(result.stdout.strip())


def repository_files(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "-z", "-co", "--exclude-standard"],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
    )
    return sorted(
        path.decode("utf-8", errors="surrogateescape")
        for path in result.stdout.split(b"\0")
        if path
    )


def read_bounded_text(path: Path) -> tuple[str | None, str | None]:
    try:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode):
            return None, None
        with path.open("rb") as stream:
            content = stream.read(MAX_TEXT_BYTES + 1)
    except FileNotFoundError:
        return None, None
    except OSError as error:
        return None, f"unreadable ({error})"

    if len(content) > MAX_TEXT_BYTES:
        return None, f"text file exceeds {MAX_TEXT_BYTES} byte scan limit"
    if b"\0" in content:
        return None, None
    try:
        return content.decode("utf-8"), None
    except UnicodeDecodeError:
        return None, None


def is_allowed(path: str, line_number: int, token_index: int) -> bool:
    return (path, line_number, token_index) in ALLOWED_OCCURRENCES


def scan_path(path: str) -> list[str]:
    violations: list[str] = []
    for token_index, token in enumerate(TOKENS):
        if token in path and not is_allowed(path, 0, token_index):
            violations.append(f"{path}:0:{token}")
    return violations


def scan_text(path: str, content: str) -> list[str]:
    violations: list[str] = []
    for line_number, line in enumerate(content.splitlines(), start=1):
        for token_index, token in enumerate(TOKENS):
            if token in line and not is_allowed(path, line_number, token_index):
                violations.append(f"{path}:{line_number}:{token}")
    return violations


def main() -> int:
    root = repository_root()
    violations: list[str] = []

    for relative_path in repository_files(root):
        violations.extend(scan_path(relative_path))
        text, read_error = read_bounded_text(root / relative_path)
        if read_error is not None:
            violations.append(f"{relative_path}:0:{read_error}")
            continue
        if text is None:
            continue

        violations.extend(scan_text(relative_path, text))

    for violation in violations:
        print(violation)
    return int(bool(violations))


if __name__ == "__main__":
    raise SystemExit(main())
