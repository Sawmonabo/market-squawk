#!/usr/bin/env python3
"""Enforce Market Squawk workspace package and dependency boundaries."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tomllib
from typing import Any


EXPECTED_PACKAGE_FIELDS = {
    "version": "0.1.0",
    "edition": "2024",
    "rust_version": "1.97.1",
    "license": "Apache-2.0 OR MIT",
}
REQUIRED_MEMBERS = {
    "apps/market-squawk/Cargo.toml",
    "crates/market-squawk-domain/Cargo.toml",
}
EXPECTED_DEFAULT_MEMBERS = {"apps/market-squawk/Cargo.toml"}


def cargo_metadata(root: Path) -> dict[str, Any]:
    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"],
        cwd=root,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr, end="")
        raise RuntimeError("cargo metadata failed")
    return json.loads(result.stdout)


def load_manifest(path: Path) -> dict[str, Any]:
    with path.open("rb") as manifest:
        return tomllib.load(manifest)


def relative_manifest(root: Path, manifest_path: str) -> str:
    return Path(manifest_path).resolve().relative_to(root).as_posix()


def inherits_workspace_value(package: dict[str, Any], field: str) -> bool:
    value = package.get(field)
    return isinstance(value, dict) and value.get("workspace") is True


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    try:
        metadata = cargo_metadata(root)
    except (json.JSONDecodeError, OSError, RuntimeError) as error:
        print(f"workspace boundary check failed: {error}", file=sys.stderr)
        return 1

    violations: list[str] = []
    root_manifest_path = root / "Cargo.toml"
    root_manifest = load_manifest(root_manifest_path)
    workspace = root_manifest.get("workspace")
    if not isinstance(workspace, dict) or "package" in root_manifest:
        violations.append("Cargo.toml: root must be a virtual workspace")
        workspace = {}

    if workspace.get("resolver") != "3":
        violations.append("Cargo.toml: workspace resolver must be 3")

    expected_member_patterns = ["apps/*", "crates/*"]
    if any((root / "adapters").glob("*/Cargo.toml")):
        expected_member_patterns.append("adapters/*")
    if workspace.get("members") != expected_member_patterns:
        violations.append(
            f"Cargo.toml: workspace members must be {expected_member_patterns!r}"
        )

    workspace_package = workspace.get("package", {})
    for metadata_field, expected in EXPECTED_PACKAGE_FIELDS.items():
        manifest_field = metadata_field.replace("_", "-")
        if workspace_package.get(manifest_field) != expected:
            violations.append(
                f"Cargo.toml: workspace.package.{manifest_field} must be {expected!r}"
            )

    packages_by_id = {package["id"]: package for package in metadata["packages"]}
    workspace_packages = [packages_by_id[member] for member in metadata["workspace_members"]]
    member_manifests = {
        relative_manifest(root, package["manifest_path"]): package
        for package in workspace_packages
    }

    existing_manifests = {
        manifest.relative_to(root).as_posix()
        for parent in ("apps", "crates", "adapters")
        for manifest in (root / parent).glob("*/Cargo.toml")
    }
    for unregistered_manifest in sorted(existing_manifests - member_manifests.keys()):
        violations.append(
            f"{unregistered_manifest}: package is not registered as a workspace member"
        )

    for required_manifest in sorted(REQUIRED_MEMBERS - member_manifests.keys()):
        violations.append(f"{required_manifest}: required workspace member is missing")

    default_member_manifests = {
        relative_manifest(root, packages_by_id[member]["manifest_path"])
        for member in metadata["workspace_default_members"]
    }
    if default_member_manifests != EXPECTED_DEFAULT_MEMBERS:
        violations.append(
            "Cargo.toml: default-members must contain only apps/market-squawk"
        )

    for relative_path, package in sorted(member_manifests.items()):
        manifest = load_manifest(root / relative_path)
        package_manifest = manifest.get("package", {})

        for metadata_field, expected in EXPECTED_PACKAGE_FIELDS.items():
            actual = package.get(metadata_field)
            if actual != expected:
                violations.append(
                    f"{relative_path}: resolved {metadata_field.replace('_', '-')} "
                    f"must be {expected!r}, found {actual!r}"
                )

            manifest_field = metadata_field.replace("_", "-")
            if not inherits_workspace_value(package_manifest, manifest_field):
                violations.append(
                    f"{relative_path}: package.{manifest_field}.workspace must be true"
                )

        if manifest.get("lints", {}).get("workspace") is not True:
            violations.append(f"{relative_path}: lints.workspace must be true")

        is_app = relative_path.startswith("apps/")
        if not is_app and any(
            dependency.get("name") == "anyhow" for dependency in package["dependencies"]
        ):
            violations.append(f"{relative_path}: non-app packages may not depend on anyhow")

    for violation in violations:
        print(violation)
    return int(bool(violations))


if __name__ == "__main__":
    raise SystemExit(main())
