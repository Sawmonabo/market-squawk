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
    token_count: int = 1
    expected_occurrences: int = 1


# Keys are (repository-relative path, TOKENS index, exact containing path or line). Physical line
# numbers are deliberately excluded: unrelated preceding edits must not invalidate a compatibility
# allowance. Exact containers and counts ensure changed, duplicated, missing, stale, and additional
# token occurrences still fail closed.
ALLOWED_OCCURRENCES = {
    (".gitattributes", 2, f"*{TOKENS[2]} binary"): AllowedOccurrence(),
    (
        "apps/market-squawk/tests/journal.rs",
        3,
        f'    let bytes = fixture_with_magic(*b"{TOKENS[3]}")?;',
    ): AllowedOccurrence(),
    (
        "apps/market-squawk/tests/journal.rs",
        3,
        f'    let original = fixture_with_magic(*b"{TOKENS[3]}")?;',
    ): AllowedOccurrence(),
    (
        "apps/market-squawk/tests/journal_path_integration.rs",
        2,
        f'    let legacy = paths.journal_dir().join(format!("{{SOURCE}}{TOKENS[2]}"));',
    ): AllowedOccurrence(),
    (
        "apps/market-squawk/tests/journal_path_integration.rs",
        3,
        f'        .copy_from_slice(b"{TOKENS[3]}");',
    ): AllowedOccurrence(),
    (
        "crates/market-squawk-platform/src/journal.rs",
        3,
        f"//! `{TOKENS[3]}` and `MSJ1` preserve source-faithful diagnostic bytes, raw connection identity, and",
    ): AllowedOccurrence(),
    (
        "crates/market-squawk-platform/src/journal.rs",
        3,
        f"/// Explicit authority limitation of every committed `{TOKENS[3]}`/`MSJ1` replay.",
    ): AllowedOccurrence(),
    (
        "crates/market-squawk-platform/src/journal.rs",
        3,
        f'            b"{TOKENS[3]}" => Ok(Self::LegacyMej1),',
    ): AllowedOccurrence(),
    (
        "crates/market-squawk-platform/src/journal.rs",
        3,
        f"/// Streaming reader for committed `MSJ1` and legacy `{TOKENS[3]}` records.",
    ): AllowedOccurrence(),
    (
        "crates/market-squawk-platform/src/paths.rs",
        2,
        f"    /// Legacy `{TOKENS[3]}/{TOKENS[2]}` format.",
    ): AllowedOccurrence(),
    (
        "crates/market-squawk-platform/src/paths.rs",
        3,
        f"    /// Legacy `{TOKENS[3]}/{TOKENS[2]}` format.",
    ): AllowedOccurrence(),
    (
        "crates/market-squawk-platform/src/raw_record.rs",
        3,
        f"/// `{TOKENS[3]}` and `MSJ1` historically accepted nil UUIDs and otherwise unconstrained field values as",
    ): AllowedOccurrence(),
    (
        "crates/market-squawk-platform/tests/journal_compatibility.rs",
        3,
        f'    for magic in [*b"MSJ1", *b"{TOKENS[3]}"] {{',
    ): AllowedOccurrence(),
    (
        "crates/market-squawk-platform/tests/journal_compatibility.rs",
        3,
        f'    for magic in [*b"{TOKENS[3]}", *b"MSJ1"] {{',
    ): AllowedOccurrence(),
    (
        "crates/market-squawk-platform/tests/journal_compatibility.rs",
        3,
        f'    std::fs::write(&current, fixture_with_magic(*b"{TOKENS[3]}")?)?;',
    ): AllowedOccurrence(),
    (
        "README.md",
        2,
        f"Readers retain bounded compatibility with legacy `{TOKENS[3]}/{TOKENS[2]}` journals, but writers never create or",
    ): AllowedOccurrence(),
    (
        "README.md",
        3,
        f"Readers retain bounded compatibility with legacy `{TOKENS[3]}/{TOKENS[2]}` journals, but writers never create or",
    ): AllowedOccurrence(),
    (
        "docs/architecture/target-state.md",
        2,
        f"- Readers accept committed `{TOKENS[3]}/{TOKENS[2]}` and renamed `MSJ1/.msj` journals.",
    ): AllowedOccurrence(),
    (
        "docs/architecture/target-state.md",
        3,
        f"- Readers accept committed `{TOKENS[3]}/{TOKENS[2]}` and renamed `MSJ1/.msj` journals.",
    ): AllowedOccurrence(),
    (
        "docs/plans/implementation-plan.md",
        2,
        f"- [ ] Confirm `{TOKENS[3]}/{TOKENS[2]}` and `MSJ1/.msj` backward read compatibility.",
    ): AllowedOccurrence(),
    (
        "docs/plans/implementation-plan.md",
        3,
        f"- [ ] Confirm `{TOKENS[3]}/{TOKENS[2]}` and `MSJ1/.msj` backward read compatibility.",
    ): AllowedOccurrence(),
    (
        "docs/plans/implementation-plan.md",
        3,
        f"- [ ] Add `{TOKENS[3]}` and `MSJ1` reader fixtures before changing journal code.",
    ): AllowedOccurrence(),
    (
        "docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md",
        2,
        f"- Keep legacy `{TOKENS[3]}/{TOKENS[2]}` journals readable. Write only the current documented `MSJ1/.msj` format.",
    ): AllowedOccurrence(),
    (
        "docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md",
        3,
        f"- Keep legacy `{TOKENS[3]}/{TOKENS[2]}` journals readable. Write only the current documented `MSJ1/.msj` format.",
    ): AllowedOccurrence(),
    (
        "docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md",
        3,
        f'    let bytes = fixture_with_magic(*b"{TOKENS[3]}");',
    ): AllowedOccurrence(),
    (
        "docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md",
        3,
        f'            b"{TOKENS[3]}" => Ok(Self::LegacyMej1),',
    ): AllowedOccurrence(),
    (
        "docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md",
        0,
        f"citations. It must report `path:line:token` and exit nonzero on an unapproved `{TOKENS[0]}`,",
    ): AllowedOccurrence(),
    (
        "docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md",
        1,
        f"`{TOKENS[1]}`, `{TOKENS[2]}`, or `{TOKENS[3]}` occurrence.",
    ): AllowedOccurrence(),
    (
        "docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md",
        2,
        f"`{TOKENS[1]}`, `{TOKENS[2]}`, or `{TOKENS[3]}` occurrence.",
    ): AllowedOccurrence(),
    (
        "docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md",
        3,
        f"`{TOKENS[1]}`, `{TOKENS[2]}`, or `{TOKENS[3]}` occurrence.",
    ): AllowedOccurrence(),
    (
        "docs/research/2026-07-16-journal-durability.md",
        3,
        f"- Literal legacy `{TOKENS[3]}` fixtures remain read-only. New writes use only `MSJ1`; existing legacy data",
    ): AllowedOccurrence(),
    (
        "docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md",
        2,
        f"- [ ] Legacy `{TOKENS[3]}/{TOKENS[2]}` journals read; current writes use `MSJ1/.msj`.",
    ): AllowedOccurrence(),
    (
        "docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md",
        3,
        f"- [ ] Legacy `{TOKENS[3]}/{TOKENS[2]}` journals read; current writes use `MSJ1/.msj`.",
    ): AllowedOccurrence(),
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


AllowanceKey = tuple[str, int, str]
AllowanceUsage = dict[AllowanceKey, int]


def is_allowed(path: str, token_index: int, container: str) -> bool:
    allowance = ALLOWED_OCCURRENCES.get((path, token_index, container))
    return (
        allowance is not None
        and container.count(TOKENS[token_index]) == allowance.token_count
    )


def record_allowance_usage(
    path: str,
    token_index: int,
    container: str,
    usage: AllowanceUsage,
) -> bool:
    key = (path, token_index, container)
    allowance = ALLOWED_OCCURRENCES.get(key)
    if not is_allowed(path, token_index, container) or allowance is None:
        return False
    observed = usage.get(key, 0) + 1
    usage[key] = observed
    return observed <= allowance.expected_occurrences


def scan_path_with_usage(path: str) -> tuple[list[str], AllowanceUsage]:
    violations: list[str] = []
    usage: AllowanceUsage = {}
    for token_index, token in enumerate(TOKENS):
        if token not in path:
            continue
        if not record_allowance_usage(path, token_index, path, usage):
            violations.append(f"{path}:0:{token}")
    return violations, usage


def scan_path(path: str) -> list[str]:
    return scan_path_with_usage(path)[0]


def scan_text_with_usage(
    path: str, content: str
) -> tuple[list[str], AllowanceUsage]:
    violations: list[str] = []
    usage: AllowanceUsage = {}
    for line_number, line in enumerate(content.splitlines(), start=1):
        for token_index, token in enumerate(TOKENS):
            if token not in line:
                continue
            if not record_allowance_usage(path, token_index, line, usage):
                violations.append(f"{path}:{line_number}:{token}")
    return violations, usage


def scan_text(path: str, content: str) -> list[str]:
    return scan_text_with_usage(path, content)[0]


def merge_allowance_usage(total: AllowanceUsage, observed: AllowanceUsage) -> None:
    for key, count in observed.items():
        total[key] = total.get(key, 0) + count


def allowance_count_violations(usage: AllowanceUsage) -> list[str]:
    violations: list[str] = []
    for key, allowance in sorted(ALLOWED_OCCURRENCES.items()):
        path, token_index, _container = key
        observed = usage.get(key, 0)
        if observed != allowance.expected_occurrences:
            violations.append(
                f"{path}:*:{TOKENS[token_index]}:compatibility allowance expected "
                f"{allowance.expected_occurrences}, found {observed}"
            )
    return violations


def main() -> int:
    root = repository_root()
    violations: list[str] = []
    usage: AllowanceUsage = {}

    for relative_path in repository_files(root):
        path_violations, path_used = scan_path_with_usage(relative_path)
        violations.extend(path_violations)
        merge_allowance_usage(usage, path_used)
        text, read_error = read_bounded_text(root / relative_path)
        if read_error is not None:
            violations.append(f"{relative_path}:0:{read_error}")
            continue
        if text is None:
            continue

        text_violations, text_used = scan_text_with_usage(relative_path, text)
        violations.extend(text_violations)
        merge_allowance_usage(usage, text_used)

    violations.extend(allowance_count_violations(usage))

    for violation in violations:
        print(violation)
    return int(bool(violations))


if __name__ == "__main__":
    raise SystemExit(main())
