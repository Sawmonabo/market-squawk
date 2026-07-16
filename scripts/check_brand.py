#!/usr/bin/env python3
"""Reject stale Market Squawk predecessor branding in repository text files."""

from __future__ import annotations

import os
from pathlib import Path
import stat
import subprocess
from typing import NamedTuple


MAX_TEXT_BYTES = 2 * 1024 * 1024
TOKENS = (
    "market" + "-engine",
    "Market" + " Engine",
    "." + "mej",
    "ME" + "J1",
)

class AllowedOccurrence(NamedTuple):
    container: str
    count: int = 1


# Keys are (repository-relative path, one-based line, TOKENS index). Line zero is reserved for an
# explicitly approved compatibility filename. Each allowance pins the complete containing path or
# line and the exact occurrence count, so changed, duplicated, missing, and stale references fail.
ALLOWED_OCCURRENCES = {
    (".gitattributes", 2, 2): AllowedOccurrence(f"*{TOKENS[2]} binary"),
    ("apps/market-squawk/src/journal.rs", 33, 3): AllowedOccurrence(
        f'            b"{TOKENS[3]}" => Ok(Self::LegacyMej1),'
    ),
    ("apps/market-squawk/tests/journal.rs", 31, 3): AllowedOccurrence(
        f'    let bytes = fixture_with_magic(*b"{TOKENS[3]}")?;'
    ),
    ("apps/market-squawk/tests/journal.rs", 76, 2): AllowedOccurrence(
        f'    let path = directory.path().join("legacy{TOKENS[2]}");'
    ),
    ("apps/market-squawk/tests/journal.rs", 77, 3): AllowedOccurrence(
        f'    let original = fixture_with_magic(*b"{TOKENS[3]}")?;'
    ),
    ("apps/market-squawk/tests/journal_path_integration.rs", 35, 2): AllowedOccurrence(
        f'    let legacy = journal_dir.join(format!("{{SOURCE}}{TOKENS[2]}"));'
    ),
    ("apps/market-squawk/tests/journal_path_integration.rs", 51, 3): AllowedOccurrence(
        f'        .copy_from_slice(b"{TOKENS[3]}");'
    ),
    ("README.md", 204, 2): AllowedOccurrence(
        f"Readers retain bounded compatibility with legacy `{TOKENS[3]}/{TOKENS[2]}` journals, but writers never create or"
    ),
    ("README.md", 204, 3): AllowedOccurrence(
        f"Readers retain bounded compatibility with legacy `{TOKENS[3]}/{TOKENS[2]}` journals, but writers never create or"
    ),
    ("docs/architecture/current-state.md", 9, 0): AllowedOccurrence(
        f"- Working tree: modified by an in-progress `{TOKENS[0]}` to `market-squawk` rename"
    ),
    ("docs/architecture/current-state.md", 208, 2): AllowedOccurrence(
        f"The committed predecessor used `{TOKENS[3]}` and `{TOKENS[2]}`; the working tree changes those to `MSJ1` and"
    ),
    ("docs/architecture/current-state.md", 208, 3): AllowedOccurrence(
        f"The committed predecessor used `{TOKENS[3]}` and `{TOKENS[2]}`; the working tree changes those to `MSJ1` and"
    ),
    ("docs/architecture/target-state.md", 513, 2): AllowedOccurrence(
        f"- Readers accept committed `{TOKENS[3]}/{TOKENS[2]}` and renamed `MSJ1/.msj` journals."
    ),
    ("docs/architecture/target-state.md", 513, 3): AllowedOccurrence(
        f"- Readers accept committed `{TOKENS[3]}/{TOKENS[2]}` and renamed `MSJ1/.msj` journals."
    ),
    ("docs/plans/implementation-plan.md", 130, 2): AllowedOccurrence(
        f"- [ ] Confirm `{TOKENS[3]}/{TOKENS[2]}` and `MSJ1/.msj` backward read compatibility."
    ),
    ("docs/plans/implementation-plan.md", 130, 3): AllowedOccurrence(
        f"- [ ] Confirm `{TOKENS[3]}/{TOKENS[2]}` and `MSJ1/.msj` backward read compatibility."
    ),
    ("docs/plans/implementation-plan.md", 146, 3): AllowedOccurrence(
        f"- [ ] Add `{TOKENS[3]}` and `MSJ1` reader fixtures before changing journal code."
    ),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 41, 2): AllowedOccurrence(
        f"- Keep legacy `{TOKENS[3]}/{TOKENS[2]}` journals readable. Write only the current documented `MSJ1/.msj` format."
    ),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 41, 3): AllowedOccurrence(
        f"- Keep legacy `{TOKENS[3]}/{TOKENS[2]}` journals readable. Write only the current documented `MSJ1/.msj` format."
    ),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 103, 3): AllowedOccurrence(
        f'    let bytes = fixture_with_magic(*b"{TOKENS[3]}");'
    ),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 139, 3): AllowedOccurrence(
        f'            b"{TOKENS[3]}" => Ok(Self::LegacyMej1),'
    ),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 153, 0): AllowedOccurrence(
        f"citations. It must report `path:line:token` and exit nonzero on an unapproved `{TOKENS[0]}`,"
    ),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 154, 1): AllowedOccurrence(
        f"`{TOKENS[1]}`, `{TOKENS[2]}`, or `{TOKENS[3]}` occurrence."
    ),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 154, 2): AllowedOccurrence(
        f"`{TOKENS[1]}`, `{TOKENS[2]}`, or `{TOKENS[3]}` occurrence."
    ),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 154, 3): AllowedOccurrence(
        f"`{TOKENS[1]}`, `{TOKENS[2]}`, or `{TOKENS[3]}` occurrence."
    ),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 1501, 2): AllowedOccurrence(
        f"- [ ] Legacy `{TOKENS[3]}/{TOKENS[2]}` journals read; current writes use `MSJ1/.msj`."
    ),
    ("docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md", 1501, 3): AllowedOccurrence(
        f"- [ ] Legacy `{TOKENS[3]}/{TOKENS[2]}` journals read; current writes use `MSJ1/.msj`."
    ),
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


def is_allowed(path: str, line_number: int, token_index: int, container: str) -> bool:
    allowance = ALLOWED_OCCURRENCES.get((path, line_number, token_index))
    return (
        allowance is not None
        and allowance.container == container
        and container.count(TOKENS[token_index]) == allowance.count
    )


def scan_path_with_usage(path: str) -> tuple[list[str], set[tuple[str, int, int]]]:
    violations: list[str] = []
    used: set[tuple[str, int, int]] = set()
    for token_index, token in enumerate(TOKENS):
        if token not in path:
            continue
        key = (path, 0, token_index)
        if is_allowed(path, 0, token_index, path):
            used.add(key)
        else:
            violations.append(f"{path}:0:{token}")
    return violations, used


def scan_path(path: str) -> list[str]:
    return scan_path_with_usage(path)[0]


def scan_text_with_usage(
    path: str, content: str
) -> tuple[list[str], set[tuple[str, int, int]]]:
    violations: list[str] = []
    used: set[tuple[str, int, int]] = set()
    for line_number, line in enumerate(content.splitlines(), start=1):
        for token_index, token in enumerate(TOKENS):
            if token not in line:
                continue
            key = (path, line_number, token_index)
            if is_allowed(path, line_number, token_index, line):
                used.add(key)
            else:
                violations.append(f"{path}:{line_number}:{token}")
    return violations, used


def scan_text(path: str, content: str) -> list[str]:
    return scan_text_with_usage(path, content)[0]


def unused_allowance_violations(used: set[tuple[str, int, int]]) -> list[str]:
    return [
        f"{path}:{line}:{TOKENS[token_index]}:unused compatibility allowance"
        for path, line, token_index in sorted(ALLOWED_OCCURRENCES.keys() - used)
    ]


def main() -> int:
    root = repository_root()
    violations: list[str] = []
    used: set[tuple[str, int, int]] = set()

    for relative_path in repository_files(root):
        path_violations, path_used = scan_path_with_usage(relative_path)
        violations.extend(path_violations)
        used.update(path_used)
        text, read_error = read_bounded_text(root / relative_path)
        if read_error is not None:
            violations.append(f"{relative_path}:0:{read_error}")
            continue
        if text is None:
            continue

        text_violations, text_used = scan_text_with_usage(relative_path, text)
        violations.extend(text_violations)
        used.update(text_used)

    violations.extend(unused_allowance_violations(used))

    for violation in violations:
        print(violation)
    return int(bool(violations))


if __name__ == "__main__":
    raise SystemExit(main())
