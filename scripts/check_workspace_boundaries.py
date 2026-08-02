#!/usr/bin/env python3
"""Enforce Market Squawk workspace package and dependency boundaries."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import subprocess
import sys
import tomllib
from typing import Any


EXPECTED_PACKAGE_FIELDS = {
    "version": "1.0.0",
    "edition": "2024",
    "rust_version": "1.97.1",
    "license": "Apache-2.0 OR MIT",
}
EXPECTED_MANIFESTS = {
    "market-squawk": "apps/market-squawk/Cargo.toml",
    "market-squawk-desktop": "apps/market-squawk-desktop/src-tauri/Cargo.toml",
    "market-squawk-installer": "apps/market-squawk-installer/Cargo.toml",
    "market-squawk-domain": "crates/market-squawk-domain/Cargo.toml",
    "market-squawk-platform": "crates/market-squawk-platform/Cargo.toml",
    "market-squawk-sources": "crates/market-squawk-sources/Cargo.toml",
    "market-squawk-live": "crates/market-squawk-live/Cargo.toml",
    "market-squawk-jobs": "crates/market-squawk-jobs/Cargo.toml",
    "market-squawk-data": "crates/market-squawk-data/Cargo.toml",
    "market-squawk-analytics": "crates/market-squawk-analytics/Cargo.toml",
    "market-squawk-services": "crates/market-squawk-services/Cargo.toml",
    "market-squawk-modeling": "crates/market-squawk-modeling/Cargo.toml",
    "market-squawk-portfolio": "crates/market-squawk-portfolio/Cargo.toml",
    "market-squawk-runtime": "crates/market-squawk-runtime/Cargo.toml",
    "market-squawk-backtesting": "crates/market-squawk-backtesting/Cargo.toml",
    "market-squawk-execution": "crates/market-squawk-execution/Cargo.toml",
    "market-squawk-valuation": "crates/market-squawk-valuation/Cargo.toml",
    "market-squawk-mcp": "crates/market-squawk-mcp/Cargo.toml",
    "market-squawk-python": "crates/market-squawk-python/Cargo.toml",
    "market-squawk-adapter-coinbase": (
        "adapters/market-squawk-adapter-coinbase/Cargo.toml"
    ),
    "market-squawk-adapter-kraken": "adapters/market-squawk-adapter-kraken/Cargo.toml",
    "market-squawk-adapter-sec": "adapters/market-squawk-adapter-sec/Cargo.toml",
    "market-squawk-adapter-fred": "adapters/market-squawk-adapter-fred/Cargo.toml",
    "market-squawk-adapter-bls": "adapters/market-squawk-adapter-bls/Cargo.toml",
    "market-squawk-adapter-treasury": (
        "adapters/market-squawk-adapter-treasury/Cargo.toml"
    ),
    "market-squawk-adapter-files": "adapters/market-squawk-adapter-files/Cargo.toml",
    "market-squawk-adapter-portfolio": (
        "adapters/market-squawk-adapter-portfolio/Cargo.toml"
    ),
    "market-squawk-adapter-paper": "adapters/market-squawk-adapter-paper/Cargo.toml",
}
REQUIRED_MEMBERS = {
    "apps/market-squawk/Cargo.toml",
    "apps/market-squawk-desktop/src-tauri/Cargo.toml",
    "crates/market-squawk-domain/Cargo.toml",
}
EXPECTED_DEFAULT_MEMBERS = {"apps/market-squawk/Cargo.toml"}
CORE_LOCAL_DEPENDENCIES = {
    "market-squawk-domain": set(),
    "market-squawk-analytics": {"market-squawk-domain"},
    "market-squawk-platform": {"market-squawk-domain"},
    "market-squawk-sources": {"market-squawk-domain", "market-squawk-platform"},
    "market-squawk-live": {
        "market-squawk-domain",
        "market-squawk-sources",
        "market-squawk-analytics",
    },
    "market-squawk-data": {
        "market-squawk-domain",
        "market-squawk-platform",
        "market-squawk-sources",
    },
    "market-squawk-services": {"market-squawk-domain", "market-squawk-platform"},
    "market-squawk-jobs": {
        "market-squawk-domain",
        "market-squawk-platform",
        "market-squawk-services",
    },
    "market-squawk-runtime": {
        "market-squawk-domain",
        "market-squawk-jobs",
        "market-squawk-platform",
        "market-squawk-services",
    },
    "market-squawk-modeling": {
        "market-squawk-domain",
        "market-squawk-analytics",
        "market-squawk-data",
    },
    "market-squawk-portfolio": {
        "market-squawk-domain",
        "market-squawk-analytics",
        "market-squawk-data",
    },
    "market-squawk-backtesting": {
        "market-squawk-domain",
        "market-squawk-analytics",
        "market-squawk-data",
        "market-squawk-modeling",
        "market-squawk-portfolio",
        "market-squawk-execution",
    },
    "market-squawk-execution": {
        "market-squawk-domain",
        "market-squawk-live",
        "market-squawk-analytics",
        "market-squawk-modeling",
        "market-squawk-portfolio",
    },
    "market-squawk-valuation": {
        "market-squawk-domain",
        "market-squawk-analytics",
        "market-squawk-data",
        "market-squawk-live",
        "market-squawk-portfolio",
    },
    "market-squawk-mcp": {
        "market-squawk-domain",
        "market-squawk-jobs",
        "market-squawk-platform",
        "market-squawk-runtime",
        "market-squawk-services",
    },
    "market-squawk-python": {
        "market-squawk-domain",
        "market-squawk-analytics",
        "market-squawk-data",
        # The sealed native package verifies Python training receipts through modeling's
        # code-owned environment authority before any mutable Python module is imported.
        "market-squawk-modeling",
    },
}
PROVIDER_ADAPTERS = {
    "market-squawk-adapter-coinbase",
    "market-squawk-adapter-kraken",
    "market-squawk-adapter-sec",
    "market-squawk-adapter-fred",
    "market-squawk-adapter-bls",
    "market-squawk-adapter-treasury",
    "market-squawk-adapter-files",
    "market-squawk-adapter-portfolio",
}
PROVIDER_DEPENDENCIES = {
    "market-squawk-domain",
    "market-squawk-platform",
    "market-squawk-sources",
}
DEV_ONLY_LOCAL_DEPENDENCIES = {
    # These package-local verticals prove that canonical adapter output reaches the data-plane
    # ingestion boundary. The production adapter graphs remain forbidden from depending on data.
    "market-squawk-adapter-files": {"market-squawk-data"},
    "market-squawk-adapter-portfolio": {"market-squawk-data"},
    # The required Kraken package-local vertical drives decoded source events through the public
    # execution and paper APIs. It may use those two crates only as test harnesses; provider
    # production edges remain restricted to domain/source/platform contracts.
    "market-squawk-adapter-kraken": {
        "market-squawk-execution",
        "market-squawk-adapter-paper",
    },
    # The Task 16 execution integration harness constructs genuine immutable portfolio revisions
    # from Task 11 dataset evidence. Data remains forbidden as a normal execution dependency; the
    # production boundary is execution -> portfolio only.
    "market-squawk-execution": {"market-squawk-data"},
    # The fair-value integration harness opens a local catalog fixture through retained platform
    # path authority. Valuation production code remains confined to its declared domain/live/data/
    # analytics/portfolio inputs.
    "market-squawk-valuation": {"market-squawk-platform"},
}
HOT_PATH_PACKAGES = {"market-squawk-domain", "market-squawk-live"}
HOT_PATH_FORBIDDEN = {
    "market-squawk-data",
    "market-squawk-mcp",
    "market-squawk-python",
    *PROVIDER_ADAPTERS,
}
MAX_DIAGNOSTICS = 100


@dataclass
class Diagnostics:
    """Collect a bounded set of deterministic policy violations."""

    messages: list[str] = field(default_factory=list)
    omitted: int = 0

    def add(self, message: str) -> None:
        if len(self.messages) < MAX_DIAGNOSTICS:
            self.messages.append(message)
        else:
            self.omitted += 1

    def emit(self) -> None:
        for message in self.messages:
            print(message)
        if self.omitted:
            print(f"... {self.omitted} additional violation(s) omitted")

    @property
    def failed(self) -> bool:
        return bool(self.messages) or self.omitted > 0


def cargo_metadata(root: Path) -> dict[str, Any]:
    result = subprocess.run(
        [
            "cargo",
            "metadata",
            "--format-version",
            "1",
            "--locked",
            "--all-features",
        ],
        cwd=root,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr, end="")
        raise RuntimeError("cargo metadata --locked --all-features failed")
    return json.loads(result.stdout)


def load_manifest(path: Path) -> dict[str, Any]:
    with path.open("rb") as manifest:
        return tomllib.load(manifest)


def relative_manifest(root: Path, manifest_path: str) -> str:
    return Path(manifest_path).resolve().relative_to(root).as_posix()


def inherits_workspace_value(package: dict[str, Any], field: str) -> bool:
    value = package.get(field)
    return isinstance(value, dict) and value.get("workspace") is True


def allowed_local_dependencies(package_name: str) -> set[str]:
    if package_name == "market-squawk":
        return set(EXPECTED_MANIFESTS) - {package_name}
    if package_name == "market-squawk-desktop":
        return {
            "market-squawk",
            "market-squawk-data",
            "market-squawk-installer",
            "market-squawk-platform",
            "market-squawk-runtime",
            "market-squawk-services",
        }
    if package_name == "market-squawk-installer":
        return {"market-squawk-runtime"}
    if package_name in PROVIDER_ADAPTERS:
        return PROVIDER_DEPENDENCIES
    if package_name == "market-squawk-adapter-paper":
        return {
            "market-squawk-domain",
            "market-squawk-execution",
            "market-squawk-platform",
        }
    return CORE_LOCAL_DEPENDENCIES.get(package_name, set())


def resolved_local_edges(
    metadata: dict[str, Any],
    packages_by_id: dict[str, dict[str, Any]],
    diagnostics: Diagnostics,
) -> dict[str, dict[str, set[str]]]:
    """Return direct workspace edges separated by Cargo dependency kind.

    Cargo metadata combines normal, build, and development edges in each resolved node. Keeping
    the kinds separate prevents a package-local integration harness from silently becoming an
    allowed production edge. Normal and build edges use the production DAG; development edges use
    that DAG plus the narrow explicit test-only allowlist above.
    """

    workspace_ids = set(metadata["workspace_members"])
    resolve = metadata.get("resolve")
    if not isinstance(resolve, dict):
        diagnostics.add("cargo metadata: resolved dependency graph is missing")
        return {}

    nodes = resolve.get("nodes")
    if not isinstance(nodes, list):
        diagnostics.add("cargo metadata: resolved dependency nodes are missing")
        return {}

    edges: dict[str, dict[str, set[str]]] = {}
    for node in nodes:
        package_id = node.get("id")
        if package_id not in workspace_ids:
            continue
        package = packages_by_id.get(package_id)
        if package is None:
            diagnostics.add(f"cargo metadata: package details missing for {package_id!r}")
            continue
        package_edges = edges.setdefault(package["name"], {})
        for dependency in node.get("deps", []):
            dependency_id = dependency.get("pkg")
            if dependency_id in workspace_ids:
                dependency_package = packages_by_id.get(dependency_id)
                if dependency_package is None:
                    diagnostics.add(
                        f"cargo metadata: dependency details missing for {dependency_id!r}"
                    )
                    continue
                dependency_name = dependency_package["name"]
                dependency_kinds = dependency.get("dep_kinds", [])
                if not dependency_kinds:
                    diagnostics.add(
                        f"cargo metadata: dependency kinds missing for "
                        f"{package['name']} -> {dependency_name}"
                    )
                    continue
                for dependency_kind in dependency_kinds:
                    raw_kind = dependency_kind.get("kind")
                    kind = "normal" if raw_kind is None else str(raw_kind)
                    package_edges.setdefault(kind, set()).add(dependency_name)
    return edges


def validate_dependency_graph(
    local_edges: dict[str, dict[str, set[str]]], diagnostics: Diagnostics
) -> None:
    for package_name, edges_by_kind in sorted(local_edges.items()):
        production_allowed = allowed_local_dependencies(package_name)
        for dependency_kind, dependencies in sorted(edges_by_kind.items()):
            allowed = set(production_allowed)
            if dependency_kind == "dev":
                allowed.update(DEV_ONLY_LOCAL_DEPENDENCIES.get(package_name, set()))
            elif dependency_kind not in {"normal", "build"}:
                diagnostics.add(
                    f"{package_name}: unsupported Cargo dependency kind {dependency_kind!r}"
                )

            for dependency in sorted(dependencies - allowed):
                diagnostics.add(
                    f"{package_name}: {dependency_kind} dependency on {dependency} "
                    "violates the allowed DAG"
                )

            if package_name in HOT_PATH_PACKAGES:
                for dependency in sorted(dependencies & HOT_PATH_FORBIDDEN):
                    diagnostics.add(
                        f"{package_name}: {dependency_kind} hot-path dependency on "
                        f"{dependency} is forbidden"
                    )

            if package_name.startswith("market-squawk-adapter-"):
                for dependency in sorted(
                    dependencies & {"market-squawk-data", "market-squawk-live"}
                ):
                    if (
                        dependency_kind == "dev"
                        and dependency
                        in DEV_ONLY_LOCAL_DEPENDENCIES.get(package_name, set())
                    ):
                        continue
                    diagnostics.add(
                        f"{package_name}: {dependency_kind} adapter dependency on "
                        f"{dependency} is forbidden"
                    )


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    diagnostics = Diagnostics()
    try:
        metadata = cargo_metadata(root)
        root_manifest = load_manifest(root / "Cargo.toml")
    except (json.JSONDecodeError, OSError, RuntimeError, tomllib.TOMLDecodeError) as error:
        print(f"workspace boundary check failed: {error}", file=sys.stderr)
        return 1

    workspace = root_manifest.get("workspace")
    if not isinstance(workspace, dict) or "package" in root_manifest:
        diagnostics.add("Cargo.toml: root must be a virtual workspace")
        workspace = {}

    if workspace.get("resolver") != "3":
        diagnostics.add("Cargo.toml: workspace resolver must be 3")

    expected_member_patterns = [
        "apps/market-squawk",
        "apps/market-squawk-desktop/src-tauri",
        "apps/market-squawk-installer",
        "crates/*",
    ]
    if any((root / "adapters").glob("*/Cargo.toml")):
        expected_member_patterns.append("adapters/*")
    if workspace.get("members") != expected_member_patterns:
        diagnostics.add(
            f"Cargo.toml: workspace members must be {expected_member_patterns!r}"
        )

    workspace_package = workspace.get("package", {})
    for metadata_field, expected in EXPECTED_PACKAGE_FIELDS.items():
        manifest_field = metadata_field.replace("_", "-")
        if workspace_package.get(manifest_field) != expected:
            diagnostics.add(
                f"Cargo.toml: workspace.package.{manifest_field} must be {expected!r}"
            )

    packages_by_id = {package["id"]: package for package in metadata["packages"]}
    workspace_packages = [
        packages_by_id[member]
        for member in metadata["workspace_members"]
        if member in packages_by_id
    ]
    member_manifests: dict[str, dict[str, Any]] = {}
    package_names: set[str] = set()
    for package in workspace_packages:
        try:
            manifest_path = relative_manifest(root, package["manifest_path"])
        except ValueError:
            diagnostics.add(
                f"{package['name']}: workspace manifest is outside the repository root"
            )
            continue
        if package["name"] in package_names:
            diagnostics.add(f"{package['name']}: duplicate workspace package name")
        package_names.add(package["name"])
        member_manifests[manifest_path] = package

        expected_manifest = EXPECTED_MANIFESTS.get(package["name"])
        if expected_manifest is None:
            diagnostics.add(f"{manifest_path}: workspace package is not in the release allowlist")
        elif manifest_path != expected_manifest:
            diagnostics.add(
                f"{package['name']}: manifest must be {expected_manifest}, found {manifest_path}"
            )

    existing_manifests = {
        manifest.relative_to(root).as_posix()
        for pattern in (
            "apps/*/Cargo.toml",
            "apps/*/src-tauri/Cargo.toml",
            "crates/*/Cargo.toml",
            "adapters/*/Cargo.toml",
        )
        for manifest in root.glob(pattern)
    }
    for unregistered_manifest in sorted(existing_manifests - member_manifests.keys()):
        diagnostics.add(
            f"{unregistered_manifest}: package is not registered as a workspace member"
        )

    for required_manifest in sorted(REQUIRED_MEMBERS - member_manifests.keys()):
        diagnostics.add(f"{required_manifest}: required workspace member is missing")

    default_member_manifests = {
        relative_manifest(root, packages_by_id[member]["manifest_path"])
        for member in metadata["workspace_default_members"]
        if member in packages_by_id
    }
    if default_member_manifests != EXPECTED_DEFAULT_MEMBERS:
        diagnostics.add("Cargo.toml: default-members must contain only apps/market-squawk")

    for relative_path, package in sorted(member_manifests.items()):
        try:
            manifest = load_manifest(root / relative_path)
        except (OSError, tomllib.TOMLDecodeError) as error:
            diagnostics.add(f"{relative_path}: cannot read package manifest: {error}")
            continue
        package_manifest = manifest.get("package", {})

        for metadata_field, expected in EXPECTED_PACKAGE_FIELDS.items():
            actual = package.get(metadata_field)
            if actual != expected:
                diagnostics.add(
                    f"{relative_path}: resolved {metadata_field.replace('_', '-')} "
                    f"must be {expected!r}, found {actual!r}"
                )

            manifest_field = metadata_field.replace("_", "-")
            if not inherits_workspace_value(package_manifest, manifest_field):
                diagnostics.add(
                    f"{relative_path}: package.{manifest_field}.workspace must be true"
                )

        if manifest.get("lints", {}).get("workspace") is not True:
            diagnostics.add(f"{relative_path}: lints.workspace must be true")

        is_app = relative_path.startswith("apps/")
        if not is_app and any(
            dependency.get("name") == "anyhow" for dependency in package["dependencies"]
        ):
            diagnostics.add(f"{relative_path}: non-app packages may not depend on anyhow")

    local_edges = resolved_local_edges(metadata, packages_by_id, diagnostics)
    validate_dependency_graph(local_edges, diagnostics)

    diagnostics.emit()
    return int(diagnostics.failed)


if __name__ == "__main__":
    raise SystemExit(main())
