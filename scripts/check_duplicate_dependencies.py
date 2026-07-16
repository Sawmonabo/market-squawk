#!/usr/bin/env python3
"""Reject dependency-version duplication outside a reviewed exact inventory."""

from __future__ import annotations

from collections import defaultdict
import json
from pathlib import Path
import subprocess
import sys
from typing import Any


# These are transitive families in the locked Rust 1.97 graph. Any family or version change is a
# review event. Cargo-deny can replace this focused gate once Task 15 installs the full audit policy.
ALLOWED_DUPLICATES: dict[str, tuple[str, ...]] = {
    "getrandom": ("0.2.17", "0.3.4", "0.4.3"),
    "hashbrown": ("0.12.3", "0.17.1"),
    # rust-bitcoin 0.32 uses the current crate; secp256k1 retains the 0.2 wire helper.
    "hex-conservative": ("0.2.2", "1.2.0"),
    # cap-std 4 uses io-lifetimes 3 directly while its fs-set-times dependency retains version 2.
    "io-lifetimes": ("2.0.4", "3.0.1"),
    "r-efi": ("5.3.0", "6.0.0"),
    "rand": ("0.8.7", "0.9.5"),
    "rand_chacha": ("0.3.1", "0.9.0"),
    "rand_core": ("0.6.4", "0.9.5"),
    "syn": ("1.0.109", "2.0.119"),
    "webpki-roots": ("0.26.11", "1.0.8"),
    # ring retains 0.52; cap-std 4's fs-set-times/winx and io-extras paths retain 0.59/0.60;
    # Tokio, Clap, Rustix, and current platform dependencies use 0.61.
    "windows-sys": ("0.52.0", "0.59.0", "0.60.2", "0.61.2"),
    # windows-sys 0.52/0.59 share the 0.52 target family; windows-sys 0.60 uses 0.53.
    "windows-targets": ("0.52.6", "0.53.5"),
    "windows_aarch64_gnullvm": ("0.52.6", "0.53.1"),
    "windows_aarch64_msvc": ("0.52.6", "0.53.1"),
    "windows_i686_gnu": ("0.52.6", "0.53.1"),
    "windows_i686_gnullvm": ("0.52.6", "0.53.1"),
    "windows_i686_msvc": ("0.52.6", "0.53.1"),
    "windows_x86_64_gnu": ("0.52.6", "0.53.1"),
    "windows_x86_64_gnullvm": ("0.52.6", "0.53.1"),
    "windows_x86_64_msvc": ("0.52.6", "0.53.1"),
}


def cargo_metadata_command() -> list[str]:
    return [
        "cargo",
        "metadata",
        "--format-version",
        "1",
        "--all-features",
        "--locked",
    ]


def duplicate_inventory(packages: list[dict[str, Any]]) -> dict[str, tuple[str, ...]]:
    versions: dict[str, set[str]] = defaultdict(set)
    for package in packages:
        versions[str(package["name"])].add(str(package["version"]))
    return {
        name: tuple(sorted(found_versions))
        for name, found_versions in sorted(versions.items())
        if len(found_versions) > 1
    }


def inventory_violations(
    actual: dict[str, tuple[str, ...]],
    allowed: dict[str, tuple[str, ...]],
) -> list[str]:
    violations: list[str] = []
    for name, versions in sorted(actual.items()):
        expected = allowed.get(name)
        rendered = ", ".join(versions)
        if expected is None:
            violations.append(f"unexpected duplicate {name}: {rendered}")
        elif versions != expected:
            violations.append(
                f"duplicate {name} changed: expected {', '.join(expected)}; found {rendered}"
            )
    for name in sorted(allowed.keys() - actual.keys()):
        violations.append(f"reviewed duplicate {name} is no longer present; remove its allowance")
    return violations


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        cargo_metadata_command(),
        cwd=root,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr, end="")
        return result.returncode
    try:
        metadata = json.loads(result.stdout)
        packages = metadata["packages"]
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        print(f"invalid cargo metadata: {error}", file=sys.stderr)
        return 1
    violations = inventory_violations(
        duplicate_inventory(packages),
        ALLOWED_DUPLICATES,
    )
    for violation in violations:
        print(violation)
    return int(bool(violations))


if __name__ == "__main__":
    raise SystemExit(main())
