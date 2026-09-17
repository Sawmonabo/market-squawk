#!/usr/bin/env python3
"""Assemble one deterministic, complete Market Squawk platform release."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit
import zipfile


MAXIMUM_FILES = 32_768
MAXIMUM_FILE_BYTES = 1024 * 1024 * 1024
MAXIMUM_EXPANDED_BYTES = 4 * 1024 * 1024 * 1024
MAXIMUM_ARCHIVE_BYTES = 2 * 1024 * 1024 * 1024
MAXIMUM_PLATFORM_MANIFEST_BYTES = 8 * 1024 * 1024
MAXIMUM_UPDATE_METADATA_BYTES = 1024 * 1024
COPY_BUFFER_BYTES = 1024 * 1024
ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)
VERSION_PATTERN = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
OBJECT_PATTERN = re.compile(r"[0-9a-f]{40}")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
SIGNATURE_PATTERN = re.compile(r"[0-9a-f]{128}")
UPDATE_CHANNEL_PATH = "share/market-squawk/update/channel.json"
PINNED_UPDATE_ROOT_PATH = "share/market-squawk/update/1.root.json"
UPDATE_SPEC_VERSION = "1.0.35"
BUILD_ONLY_PYTHON_PATHS = frozenset({".lock", ".market-squawk-owned-v1"})
LOCKED_PYTHON_PORTABILITY_EXCLUSIONS = {
    "x86_64-unknown-linux-gnu": {
        "share/terminfo/2/2621A": (
            622,
            "c60f9af6a0de5ac8a208c2bbcd31a731cad53fc5f2ced90572a6c82fecb34782",
        ),
        "share/terminfo/E/Eterm": (
            2_224,
            "9c6c23dd46de071e5f5ee24cb2144f82e46365c9011bd8574bf210f0c8245043",
        ),
        "share/terminfo/E/Eterm-color": (
            2_224,
            "9c6c23dd46de071e5f5ee24cb2144f82e46365c9011bd8574bf210f0c8245043",
        ),
        "share/terminfo/h/hp2621A": (
            622,
            "c60f9af6a0de5ac8a208c2bbcd31a731cad53fc5f2ced90572a6c82fecb34782",
        ),
        "share/terminfo/h/hp70092A": (
            678,
            "a43410a1af8685b35113f51fc14c793e32fb066a7fa7b087fe0548b941709dc6",
        ),
        "share/terminfo/L/LFT-PC850": (
            1_295,
            "9d096ca5252ed4d6d3bd6310b43d5f27dfecb7e1d12e91c6139c54e18ad0440e",
        ),
        "share/terminfo/N/NCR260VT300WPP": (
            1_850,
            "58968550e6fec57b87b956feb41b70a010f2ac35579045cc0b66f1f4371f01c3",
        ),
        "share/terminfo/N/NCRVT100WPP": (
            1_509,
            "f0d65694177cbf905049c78ea268fa5f8eb2b449f20bacb108f5596a0c0e927d",
        ),
        "share/terminfo/P/P12-M-W": (
            694,
            "f232d14d6146890caa90759f2aba06fa82c164435a4b920275172fc85f820e2b",
        ),
        "share/terminfo/P/P12-M": (
            666,
            "1284842ef226fc27d0a8614f8ee0b8c658ff129cee1f536c0dbbf3e211e14c2b",
        ),
        "share/terminfo/P/P12-W": (
            1_140,
            "bd307bb71a65c91964f607c16dbb5dd6bec3c1021af40d131c4fff0d21ccb114",
        ),
        "share/terminfo/P/P12": (
            1_149,
            "39ee96a05890edf7c95cf17d5a5a466335e10e8774c55d2f10fdee4a1aa44e19",
        ),
        "share/terminfo/P/P14-M-W": (
            694,
            "97535297b9704349ae55bf36441bbe0c5b80684cd239cfa42e1c708eabeeb097",
        ),
        "share/terminfo/P/P14-M": (
            666,
            "23737c9a205bbbbbc85a62c80254d4ec8cb8eb32a64e986dcf820f4ad768b325",
        ),
        "share/terminfo/P/P14-W": (
            1_140,
            "6ee79958d432a872619163990ee6910b53d49327444cef231e7746ac7b2b7d53",
        ),
        "share/terminfo/P/P14": (
            1_149,
            "c53f7b78e43b0b246426a0f26e960ac105a80ccc9be3e3279766b0041f2e130d",
        ),
        "share/terminfo/P/P4": (
            743,
            "6167139fd4c2d8625340b4a827a34db12cc9d0449ae45c70187fdd7445db8ade",
        ),
        "share/terminfo/P/P5": (
            743,
            "c348160ac726d6bb6a723d5a58eb5cabd462298091582ef4d06ea29fd7740ac8",
        ),
        "share/terminfo/P/P7": (
            553,
            "d497651d116ac3449e8970687e4c5a4197315778d213e7ac701eb84e73de174f",
        ),
        "share/terminfo/P/P8-W": (
            614,
            "edae34a978db3b2242adee4100ceaa5d7912a7f437e5e257da83535aef3e6374",
        ),
        "share/terminfo/P/P8": (
            582,
            "1c0b7f48758dfd628ab01d198de7359655758465b4019bcbe77cd294d207baf5",
        ),
        "share/terminfo/P/P9-8-W": (
            694,
            "48a81d8e94b5d7fa313a690e6fea6908d3b376eb305642bb97aeea2e2594bd78",
        ),
        "share/terminfo/P/P9-8": (
            652,
            "88bcea1dec4da1516465faa17334139187fccfaa7424ac348b3a65a93640a226",
        ),
        "share/terminfo/P/P9-W": (
            1_136,
            "f5ca03cf8e26f1fe39b116d20324f7476ef6c6dd6d1df87be4ed2fbc6b19cb61",
        ),
        "share/terminfo/P/P9": (
            1_145,
            "f5803baffa6cb3270c1a5f1451f9ed2156c3b2f49f2d469dda262a2d48caa710",
        ),
    },
}
LICENSE_INPUTS = (
    ("LICENSE-APACHE", "licenses/LICENSE-APACHE"),
    ("LICENSE-MIT", "licenses/LICENSE-MIT"),
    ("docs/licenses/geist-ofl-license.txt", "licenses/geist-ofl-license.txt"),
    ("docs/licenses/geist-mono-ofl-license.txt", "licenses/geist-mono-ofl-license.txt"),
)
NOTICE_INPUTS = (
    ("docs/licenses/onnx-runtime-notice.md", "notices/onnx-runtime-notice.md"),
    ("docs/licenses/tauri-mpl-notice.md", "notices/tauri-mpl-notice.md"),
    ("docs/licenses/tract-onnx-notice.md", "notices/tract-onnx-notice.md"),
    ("distribution/release-components.json", "notices/release-components.json"),
)


class ReleaseBuildError(RuntimeError):
    """A complete release input or output violated its closed contract."""


@dataclass(frozen=True)
class TargetProfile:
    target: str
    executable_suffix: str

    @property
    def native_inputs(self) -> tuple[tuple[str, str], ...]:
        suffix = self.executable_suffix
        return (
            (f"market-squawk-desktop{suffix}", f"bin/market-squawk-desktop{suffix}"),
            (f"market-squawk-service{suffix}", f"bin/market-squawk-service{suffix}"),
            (
                f"market-squawk-mcp-relay{suffix}",
                f"bin/market-squawk-mcp-relay{suffix}",
            ),
            (
                f"market-squawk-capture-helper{suffix}",
                f"bin/market-squawk-capture-helper{suffix}",
            ),
            (
                f"market-squawk-installer{suffix}",
                f"bin/market-squawk-installer{suffix}",
            ),
            (f"uv{suffix}", f"tools/uv{suffix}"),
        )


TARGETS = {
    target.target: target
    for target in (
        TargetProfile("aarch64-apple-darwin", ""),
        TargetProfile("x86_64-apple-darwin", ""),
        TargetProfile("x86_64-pc-windows-msvc", ".exe"),
        TargetProfile("x86_64-unknown-linux-gnu", ""),
    )
}
MINIMUM_SYSTEMS = {
    "aarch64-apple-darwin": "macOS 12",
    "x86_64-apple-darwin": "macOS 12",
    "x86_64-pc-windows-msvc": "Windows 10 1809",
    "x86_64-unknown-linux-gnu": "Ubuntu 24.04-compatible",
}
NATIVE_PACKAGE_SUFFIXES = {
    "aarch64-apple-darwin": (".dmg",),
    "x86_64-apple-darwin": (".dmg",),
    "x86_64-pc-windows-msvc": (".msi", "-setup.exe"),
    "x86_64-unknown-linux-gnu": (".AppImage", ".deb"),
}
NATIVE_TRUST_MODES = {
    "aarch64-apple-darwin": {
        "developer-id-signed-and-notarized",
        "provenance-only",
    },
    "x86_64-apple-darwin": {
        "developer-id-signed-and-notarized",
        "provenance-only",
    },
    "x86_64-pc-windows-msvc": {
        "authenticode-signed",
        "provenance-only",
    },
    "x86_64-unknown-linux-gnu": {"provenance-only"},
}
ALL_NATIVE_TRUST_MODES = tuple(
    sorted({mode for modes in NATIVE_TRUST_MODES.values() for mode in modes})
)


@dataclass(frozen=True)
class Options:
    target: TargetProfile
    native_trust_mode: str
    version: str
    commit: str
    tree: str
    python_release: Path
    native_bundle: Path
    output: Path
    update_repository_base_url: str | None
    pinned_update_root: Path | None
    minimum_workspace_schema_version: int | None
    maximum_workspace_schema_version: int | None


@dataclass(frozen=True)
class AggregateOptions:
    inputs: tuple[Path, ...]
    install_template: Path
    output: Path
    update_metadata: Path | None


@dataclass(frozen=True)
class CollectOptions:
    target: TargetProfile
    native_package_trust_mode: str
    version: str
    release_output: Path
    native_bundle: Path
    output: Path


def main() -> int:
    try:
        if sys.argv[1:2] == ["aggregate"]:
            aggregate_release(parse_aggregate_options(sys.argv[2:]))
            return 0
        if sys.argv[1:2] == ["collect"]:
            collect_native_packages(parse_collect_options(sys.argv[2:]))
            return 0
        options = parse_options()
        root = Path(__file__).resolve().parents[1]
        validate_repository_identity(root, options)
        output = claim_output(options.output, root)
        with tempfile.TemporaryDirectory(prefix="market-squawk-complete-release-") as temporary:
            staging = Path(temporary) / "staging"
            staging.mkdir(mode=0o700)
            assemble_staging(root, staging, options)
            bundle = output / (
                f"market-squawk-{options.version}-{options.target.target}.zip"
            )
            write_deterministic_zip(staging, bundle)
            manifest = output / "market-squawk-release.json"
            build_manifest(root, staging, bundle, manifest, options)
            bootstrap = output / (
                f"market-squawk-bootstrap-{options.target.target}"
                f"{options.target.executable_suffix}"
            )
            copy_stable(
                options.native_bundle
                / f"market-squawk-installer{options.target.executable_suffix}",
                bootstrap,
                executable=True,
            )
            write_checksums(output, (bundle, manifest, bootstrap))
            verify_output_set(output, (bundle, manifest, bootstrap))
    except (OSError, ReleaseBuildError, subprocess.SubprocessError, zipfile.BadZipFile) as error:
        print(f"complete release rejected: {error}", file=sys.stderr)
        return 2
    return 0


def parse_aggregate_options(arguments: list[str]) -> AggregateOptions:
    parser = argparse.ArgumentParser(
        description="Assemble the complete cross-platform GitHub Release asset set."
    )
    parser.add_argument("--input", required=True, action="append", type=Path)
    parser.add_argument("--install-template", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--update-metadata",
        type=Path,
        help="closed directory containing externally signed TUF top-level metadata",
    )
    values = parser.parse_args(arguments)
    return AggregateOptions(
        inputs=tuple(path.expanduser().absolute() for path in values.input),
        install_template=values.install_template.expanduser().absolute(),
        output=values.output.expanduser().absolute(),
        update_metadata=(
            values.update_metadata.expanduser().absolute()
            if values.update_metadata is not None
            else None
        ),
    )


def parse_collect_options(arguments: list[str]) -> CollectOptions:
    parser = argparse.ArgumentParser(
        description="Collect one platform's complete bundle and native packages."
    )
    parser.add_argument("--target", required=True, choices=tuple(TARGETS))
    parser.add_argument(
        "--native-package-trust-mode",
        required=True,
        choices=ALL_NATIVE_TRUST_MODES,
    )
    parser.add_argument("--version", required=True)
    parser.add_argument("--release-output", required=True, type=Path)
    parser.add_argument("--native-bundle", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    values = parser.parse_args(arguments)
    if VERSION_PATTERN.fullmatch(values.version) is None:
        raise ReleaseBuildError("native package version is malformed")
    if values.native_package_trust_mode not in NATIVE_TRUST_MODES[values.target]:
        raise ReleaseBuildError("native package trust mode is unsupported for the target")
    return CollectOptions(
        target=TARGETS[values.target],
        native_package_trust_mode=values.native_package_trust_mode,
        version=values.version,
        release_output=values.release_output.expanduser().absolute(),
        native_bundle=values.native_bundle.expanduser().absolute(),
        output=values.output.expanduser().absolute(),
    )


def collect_native_packages(options: CollectOptions) -> None:
    root = Path(__file__).resolve().parents[1]
    release_output = controlled_directory(options.release_output, "platform release output")
    native_bundle = controlled_directory(options.native_bundle, "native package output")
    output = claim_output(options.output, root)
    target = options.target.target
    suffix = options.target.executable_suffix
    base_names = {
        "SHA256SUMS",
        "market-squawk-release.json",
        f"market-squawk-{options.version}-{target}.zip",
        f"market-squawk-bootstrap-{target}{suffix}",
    }
    if set(list_regular_paths(release_output)) != base_names:
        raise ReleaseBuildError("platform release output set is incomplete")
    for name in sorted(base_names):
        copy_stable(
            release_output / name,
            output / name,
            executable=name.startswith("market-squawk-bootstrap-"),
        )

    expected_suffixes = NATIVE_PACKAGE_SUFFIXES[target]
    selected: dict[str, Path] = {}
    for package_suffix in expected_suffixes:
        matches = [
            path
            for path in native_bundle.rglob("*")
            if path.is_file()
            and not path.is_symlink()
            and path.name.endswith(package_suffix)
        ]
        if len(matches) != 1:
            raise ReleaseBuildError("native package output has no unique expected artifact")
        selected[package_suffix] = matches[0]
    for package_suffix, source in sorted(selected.items()):
        destination = (
            output
            / f"market-squawk-{options.version}-{target}{package_suffix}"
        )
        copy_stable(
            source,
            destination,
            executable=package_suffix == ".AppImage",
        )
    trust_receipt = output / "native-package-trust.json"
    _write_json(
        trust_receipt,
        {
            "native_trust_mode": options.native_package_trust_mode,
            "schema_version": 1,
            "target": target,
        },
    )
    expected = base_names | {
        f"market-squawk-{options.version}-{target}{package_suffix}"
        for package_suffix in expected_suffixes
    } | {trust_receipt.name}
    if {path.name for path in output.iterdir()} != expected:
        raise ReleaseBuildError("platform publish output set is not closed")


def aggregate_release(options: AggregateOptions) -> None:
    root = Path(__file__).resolve().parents[1]
    if len(options.inputs) != len(TARGETS):
        raise ReleaseBuildError("release aggregation requires exactly four platform inputs")
    template = options.install_template.resolve(strict=True)
    if template != root / "distribution/install.sh" or template.is_symlink():
        raise ReleaseBuildError("release installer template is not the repository authority")
    output = claim_output(options.output, root)

    releases = [_admit_platform_publish_input(path) for path in options.inputs]
    shared_fields = (
        "schema_version",
        "product",
        "version",
        "tag",
        "repository",
        "commit_sha",
        "tree_sha",
        "generated_at",
    )
    reference = releases[0]["manifest"]
    if any(
        tuple(release["manifest"][field] for field in shared_fields)
        != tuple(reference[field] for field in shared_fields)
        for release in releases[1:]
    ):
        raise ReleaseBuildError("platform release manifests do not share one exact revision")

    targets = [release["target"]["target"] for release in releases]
    if sorted(targets) != sorted(TARGETS) or len(set(targets)) != len(TARGETS):
        raise ReleaseBuildError("platform release input set is incomplete or duplicated")

    update_contracts = [_embedded_update_contract(release) for release in releases]
    channel_bytes, channel, pinned_root_bytes = update_contracts[0]
    if any(contract != update_contracts[0] for contract in update_contracts[1:]):
        raise ReleaseBuildError("platform packages do not share one update trust contract")
    available = channel.get("availability") == "available"
    if available != (options.update_metadata is not None):
        raise ReleaseBuildError(
            "signed update metadata and the packaged update channel availability disagree"
        )

    index_targets = []
    expected_outputs: set[str] = set()
    bootstrap_digests: dict[str, str] = {}
    for release in sorted(releases, key=lambda value: value["target"]["target"]):
        target = release["target"]["target"]
        source = release["root"]
        manifest_name = f"market-squawk-release-{target}.json"
        copied = []
        for name in sorted(release["asset_names"]):
            destination_name = manifest_name if name == "market-squawk-release.json" else name
            destination = output / destination_name
            copy_stable(
                source / name,
                destination,
                executable=name.startswith("market-squawk-bootstrap-")
                or name.endswith(".AppImage"),
            )
            expected_outputs.add(destination_name)
            copied.append(destination_name)

        manifest_artifact = _release_artifact(reference["tag"], output / manifest_name)
        package_names = sorted(
            name for name in copied if name in release["package_names"]
        )
        index_targets.append(
            {
                "archive": release["target"]["archive"],
                "manifest": manifest_artifact,
                "minimum_system": release["target"]["minimum_system"],
                "native_packages": [
                    _release_artifact(
                        reference["tag"],
                        output / name,
                        native_trust_mode=release["native_package_trust_mode"],
                    )
                    for name in package_names
                ],
                "native_trust_mode": release["target"]["native_trust_mode"],
                "target": target,
            }
        )
        bootstrap_name = release["bootstrap_name"]
        bootstrap_digests[target] = file_sha256(output / bootstrap_name)

    index = {
        "commit_sha": reference["commit_sha"],
        "generated_at": reference["generated_at"],
        "kind": "market-squawk-release-index",
        "product": reference["product"],
        "repository": reference["repository"],
        "schema_version": 2,
        "tag": reference["tag"],
        "targets": index_targets,
        "tree_sha": reference["tree_sha"],
        "version": reference["version"],
    }
    index_path = output / "market-squawk-release.json"
    _write_json(index_path, index)
    expected_outputs.add(index_path.name)

    install_path = output / "install.sh"
    _render_install_template(
        template,
        install_path,
        reference["tag"],
        bootstrap_digests,
    )
    expected_outputs.add(install_path.name)

    artifacts = tuple(
        path
        for path in sorted(output.iterdir())
        if path.is_file() and path.name != "SHA256SUMS"
    )
    write_checksums(output, artifacts)
    expected_outputs.add("SHA256SUMS")

    update_root = output / "update"
    update_root.mkdir(mode=0o755)
    _write_bytes(update_root / "channel.json", channel_bytes)
    expected_outputs.add("update/channel.json")
    if options.update_metadata is not None:
        if pinned_root_bytes is None:
            raise ReleaseBuildError("available update channel has no pinned root")
        expected_outputs.update(
            assemble_update_repository(
                options.update_metadata,
                update_root,
                channel,
                pinned_root_bytes,
                releases,
                output,
            )
        )

    observed = set(list_regular_paths(output))
    if observed != expected_outputs:
        raise ReleaseBuildError("aggregated GitHub Release asset set is not closed")


def _admit_platform_publish_input(path: Path) -> dict[str, object]:
    root = controlled_directory(path, "platform release input")
    manifest_path = root / "market-squawk-release.json"
    if (
        manifest_path.is_symlink()
        or not manifest_path.is_file()
        or manifest_path.stat().st_size == 0
        or manifest_path.stat().st_size > MAXIMUM_PLATFORM_MANIFEST_BYTES
    ):
        raise ReleaseBuildError("platform release manifest exceeds its fixed bound")
    try:
        manifest = json.loads(manifest_path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseBuildError("platform release manifest is malformed") from error
    expected_manifest_fields = {
        "commit_sha",
        "generated_at",
        "product",
        "repository",
        "schema_version",
        "tag",
        "targets",
        "tree_sha",
        "version",
    }
    if (
        not isinstance(manifest, dict)
        or set(manifest) != expected_manifest_fields
        or manifest["schema_version"] != 2
        or manifest["product"] != "market-squawk"
        or manifest["repository"] != "Sawmonabo/market-squawk"
        or VERSION_PATTERN.fullmatch(str(manifest["version"])) is None
        or manifest["tag"] != f"v{manifest['version']}"
        or OBJECT_PATTERN.fullmatch(str(manifest["commit_sha"])) is None
        or OBJECT_PATTERN.fullmatch(str(manifest["tree_sha"])) is None
        or not isinstance(manifest["targets"], list)
        or len(manifest["targets"]) != 1
    ):
        raise ReleaseBuildError("platform release manifest identity is invalid")
    target_release = manifest["targets"][0]
    if (
        not isinstance(target_release, dict)
        or set(target_release)
        != {
            "archive",
            "components",
            "minimum_system",
            "native_trust_mode",
            "target",
        }
        or target_release["target"] not in TARGETS
        or target_release["minimum_system"]
        != MINIMUM_SYSTEMS[target_release["target"]]
        or target_release["native_trust_mode"]
        not in NATIVE_TRUST_MODES[target_release["target"]]
    ):
        raise ReleaseBuildError("platform release target identity is invalid")
    target = target_release["target"]
    version = manifest["version"]
    suffix = TARGETS[target].executable_suffix
    bundle_name = f"market-squawk-{version}-{target}.zip"
    bootstrap_name = f"market-squawk-bootstrap-{target}{suffix}"
    package_names = {
        f"market-squawk-{version}-{target}{package_suffix}"
        for package_suffix in NATIVE_PACKAGE_SUFFIXES[target]
    }
    expected_names = {
        "SHA256SUMS",
        "market-squawk-release.json",
        "native-package-trust.json",
        bundle_name,
        bootstrap_name,
        *package_names,
    }
    observed = {child.name for child in root.iterdir()}
    if observed != expected_names or any(
        child.is_symlink() or not child.is_file() for child in root.iterdir()
    ):
        raise ReleaseBuildError("platform release publish input set is not closed")
    _admit_manifest_target(root, manifest["tag"], target_release, bundle_name)
    _admit_platform_checksums(root, bundle_name, bootstrap_name)
    trust_path = root / "native-package-trust.json"
    if (
        trust_path.is_symlink()
        or not trust_path.is_file()
        or trust_path.stat().st_size == 0
        or trust_path.stat().st_size > 1024
    ):
        raise ReleaseBuildError("native package trust receipt exceeds its fixed bound")
    try:
        package_trust = json.loads(trust_path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseBuildError("native package trust receipt is malformed") from error
    if (
        not isinstance(package_trust, dict)
        or set(package_trust)
        != {"native_trust_mode", "schema_version", "target"}
        or package_trust["schema_version"] != 1
        or package_trust["target"] != target
        or package_trust["native_trust_mode"] not in NATIVE_TRUST_MODES[target]
    ):
        raise ReleaseBuildError("native package trust receipt is invalid")
    return {
        "asset_names": expected_names - {"SHA256SUMS", "native-package-trust.json"},
        "bootstrap_name": bootstrap_name,
        "manifest": manifest,
        "native_package_trust_mode": package_trust["native_trust_mode"],
        "package_names": package_names,
        "root": root,
        "target": target_release,
    }


def _embedded_update_contract(
    release: dict[str, object],
) -> tuple[bytes, dict[str, object], bytes | None]:
    root = release["root"]
    manifest = release["manifest"]
    target = release["target"]
    if not isinstance(root, Path) or not isinstance(manifest, dict) or not isinstance(target, dict):
        raise ReleaseBuildError("platform release update contract is unavailable")
    target_name = target["target"]
    bundle = root / f"market-squawk-{manifest['version']}-{target_name}.zip"
    component_by_path = {
        component["path"]: component
        for component in target["components"]
        if isinstance(component, dict) and isinstance(component.get("path"), str)
    }
    try:
        with zipfile.ZipFile(bundle, "r") as archive:
            channel_bytes = archive.read(UPDATE_CHANNEL_PATH)
            root_bytes = (
                archive.read(PINNED_UPDATE_ROOT_PATH)
                if PINNED_UPDATE_ROOT_PATH in archive.namelist()
                else None
            )
    except KeyError as error:
        raise ReleaseBuildError("platform package omits its update channel descriptor") from error
    _verify_embedded_component(component_by_path, UPDATE_CHANNEL_PATH, channel_bytes)
    channel = _parse_update_channel(channel_bytes)
    if channel["availability"] == "available":
        if root_bytes is None:
            raise ReleaseBuildError("available platform package omits its pinned update root")
        _verify_embedded_component(component_by_path, PINNED_UPDATE_ROOT_PATH, root_bytes)
        root_envelope = _metadata_envelope(root_bytes, "root")
        signed_root = root_envelope["signed"]
        if signed_root.get("version") != 1 or signed_root.get("consistent_snapshot") is not True:
            raise ReleaseBuildError("packaged pinned update root is invalid")
        pinned = channel["pinnedRoot"]
        if (
            pinned["size"] != len(root_bytes)
            or pinned["sha256"] != hashlib.sha256(root_bytes).hexdigest()
        ):
            raise ReleaseBuildError("packaged update root differs from its channel descriptor")
    elif root_bytes is not None:
        raise ReleaseBuildError("unavailable update channel must not ship a trust root")
    return channel_bytes, channel, root_bytes


def _verify_embedded_component(
    components: dict[str, dict[str, object]], path: str, bytes_value: bytes
) -> None:
    component = components.get(path)
    if (
        component is None
        or component.get("size") != len(bytes_value)
        or component.get("sha256") != hashlib.sha256(bytes_value).hexdigest()
        or component.get("executable") is not False
    ):
        raise ReleaseBuildError("packaged update component differs from its release manifest")


def _parse_update_channel(bytes_value: bytes) -> dict[str, object]:
    if not bytes_value or len(bytes_value) > MAXIMUM_UPDATE_METADATA_BYTES:
        raise ReleaseBuildError("update channel descriptor exceeds its fixed bound")
    try:
        channel = json.loads(bytes_value)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseBuildError("update channel descriptor is malformed") from error
    if not isinstance(channel, dict) or channel.get("schemaVersion") != 1:
        raise ReleaseBuildError("update channel descriptor is invalid")
    if channel.get("availability") == "unavailable":
        if set(channel) != {"availability", "reason", "schemaVersion"} or channel.get(
            "reason"
        ) != "production-signing-material-unavailable":
            raise ReleaseBuildError("unavailable update channel descriptor is invalid")
        return channel
    if set(channel) != {
        "availability",
        "maximumWorkspaceSchemaVersion",
        "minimumWorkspaceSchemaVersion",
        "pinnedRoot",
        "repositoryBaseUrl",
        "schemaVersion",
        "targets",
    } or channel.get("availability") != "available":
        raise ReleaseBuildError("available update channel descriptor is invalid")
    minimum = channel["minimumWorkspaceSchemaVersion"]
    maximum = channel["maximumWorkspaceSchemaVersion"]
    pinned = channel["pinnedRoot"]
    targets = channel["targets"]
    if (
        type(minimum) is not int
        or type(maximum) is not int
        or minimum < 1
        or minimum > maximum
        or not isinstance(pinned, dict)
        or set(pinned) != {"path", "sha256", "size"}
        or pinned["path"] != "1.root.json"
        or not isinstance(pinned["sha256"], str)
        or SHA256_PATTERN.fullmatch(pinned["sha256"]) is None
        or type(pinned["size"]) is not int
        or pinned["size"] < 1
        or pinned["size"] > MAXIMUM_UPDATE_METADATA_BYTES
        or not isinstance(targets, dict)
        or set(targets) != set(TARGETS)
    ):
        raise ReleaseBuildError("available update channel contract is invalid")
    if not isinstance(channel["repositoryBaseUrl"], str):
        raise ReleaseBuildError("update repository base URL is invalid")
    _validate_update_base_url(channel["repositoryBaseUrl"])
    for target_name, paths in targets.items():
        if paths != _update_target_paths(target_name):
            raise ReleaseBuildError("update channel target selection is invalid")
    return channel


def assemble_update_repository(
    metadata_directory: Path,
    update_root: Path,
    channel: dict[str, object],
    pinned_root_bytes: bytes,
    releases: list[dict[str, object]],
    aggregate_root: Path,
) -> set[str]:
    metadata_root = controlled_directory(metadata_directory, "signed update metadata")
    paths = list_regular_paths(metadata_root)
    target_names = [path for path in paths if re.fullmatch(r"[1-9][0-9]*\.targets\.json", path)]
    snapshot_names = [
        path for path in paths if re.fullmatch(r"[1-9][0-9]*\.snapshot\.json", path)
    ]
    if (
        len(target_names) != 1
        or len(snapshot_names) != 1
        or set(paths)
        != {"1.root.json", "timestamp.json", target_names[0], snapshot_names[0]}
    ):
        raise ReleaseBuildError("signed update metadata directory is not a closed role set")

    copy_stable(metadata_root / "1.root.json", update_root / "1.root.json", executable=False)
    copy_stable(
        metadata_root / snapshot_names[0],
        update_root / snapshot_names[0],
        executable=False,
    )
    copy_stable(
        metadata_root / target_names[0],
        update_root / target_names[0],
        executable=False,
    )
    copy_stable(metadata_root / "timestamp.json", update_root / "timestamp.json", executable=False)
    root_bytes = (update_root / "1.root.json").read_bytes()
    if root_bytes != pinned_root_bytes:
        raise ReleaseBuildError("signed update repository root differs from the packaged pin")
    root_envelope = _metadata_envelope(root_bytes, "root")
    if (
        root_envelope["signed"].get("version") != 1
        or root_envelope["signed"].get("consistent_snapshot") is not True
    ):
        raise ReleaseBuildError("signed update repository root is invalid")

    timestamp_bytes = (update_root / "timestamp.json").read_bytes()
    snapshot_bytes = (update_root / snapshot_names[0]).read_bytes()
    targets_bytes = (update_root / target_names[0]).read_bytes()
    timestamp = _metadata_envelope(timestamp_bytes, "timestamp")["signed"]
    snapshot = _metadata_envelope(snapshot_bytes, "snapshot")["signed"]
    targets = _metadata_envelope(targets_bytes, "targets")["signed"]
    if targets.get("delegations") is not None:
        raise ReleaseBuildError("delegated update targets are unsupported")
    if snapshot_names[0] != f"{snapshot['version']}.snapshot.json" or target_names[
        0
    ] != f"{targets['version']}.targets.json":
        raise ReleaseBuildError("signed update metadata filenames do not match role versions")
    _verify_metadata_description(
        timestamp,
        "snapshot.json",
        snapshot["version"],
        snapshot_bytes,
    )
    _verify_metadata_description(
        snapshot,
        "targets.json",
        targets["version"],
        targets_bytes,
    )

    described_targets = targets.get("targets")
    expected_sources: dict[str, Path] = {}
    for release in releases:
        manifest = release["manifest"]
        target = release["target"]["target"]
        selected = channel["targets"][target]
        expected_sources[selected["manifestTargetPath"]] = (
            aggregate_root / f"market-squawk-release-{target}.json"
        )
        expected_sources[selected["archiveTargetPath"]] = (
            aggregate_root / f"market-squawk-{manifest['version']}-{target}.zip"
        )
    if not isinstance(described_targets, dict) or set(described_targets) != set(expected_sources):
        raise ReleaseBuildError("signed update targets do not match the complete platform set")

    minimum = channel["minimumWorkspaceSchemaVersion"]
    maximum = channel["maximumWorkspaceSchemaVersion"]
    release_version = releases[0]["manifest"]["version"]
    expected_paths = {
        "update/channel.json",
        "update/1.root.json",
        f"update/{snapshot_names[0]}",
        f"update/{target_names[0]}",
        "update/timestamp.json",
    }
    for logical_path, source in sorted(expected_sources.items()):
        description = described_targets[logical_path]
        is_manifest = logical_path.endswith("/manifest.json")
        _verify_target_description(
            description,
            source,
            release_version=release_version if is_manifest else None,
            minimum_schema_version=minimum if is_manifest else None,
            maximum_schema_version=maximum if is_manifest else None,
        )
        digest = file_sha256(source)
        consistent_path = _consistent_target_path(logical_path, digest)
        copy_stable(source, update_root / consistent_path, executable=False)
        expected_paths.add(f"update/{consistent_path}")
    return expected_paths


def _verify_metadata_description(
    parent: dict[str, object],
    role_name: str,
    version: int,
    child_bytes: bytes,
) -> None:
    meta = parent.get("meta")
    description = meta.get(role_name) if isinstance(meta, dict) else None
    if (
        not isinstance(description, dict)
        or set(description) != {"hashes", "length", "version"}
        or type(description.get("version")) is not int
        or type(description.get("length")) is not int
        or description.get("version") != version
        or description.get("length") != len(child_bytes)
        or description.get("hashes")
        != {"sha256": hashlib.sha256(child_bytes).hexdigest()}
    ):
        raise ReleaseBuildError("signed update metadata parent identity is invalid")


def _verify_target_description(
    description: object,
    source: Path,
    *,
    release_version: str | None,
    minimum_schema_version: int | None,
    maximum_schema_version: int | None,
) -> None:
    if not isinstance(description, dict):
        raise ReleaseBuildError("signed update target identity is invalid")
    expected_keys = {"hashes", "length"}
    if release_version is not None:
        expected_keys.add("custom")
    if (
        set(description) != expected_keys
        or type(description.get("length")) is not int
        or description.get("length") != source.stat().st_size
        or description.get("hashes") != {"sha256": file_sha256(source)}
    ):
        raise ReleaseBuildError("signed update target differs from the release artifact")
    if release_version is not None and description.get("custom") != {
        "marketSquawk": {
            "maximumSchemaVersion": maximum_schema_version,
            "minimumSchemaVersion": minimum_schema_version,
            "releaseVersion": release_version,
            "schemaVersion": 1,
        }
    }:
        raise ReleaseBuildError("signed update manifest compatibility is invalid")


def _consistent_target_path(logical_path: str, digest: str) -> str:
    parent, name = logical_path.rsplit("/", 1)
    path = f"{parent}/{digest}.{name}"
    validate_portable_path(path)
    return path


def _admit_manifest_target(
    root: Path,
    tag: str,
    target: dict[str, object],
    bundle_name: str,
) -> None:
    archive = target["archive"]
    components = target["components"]
    if (
        not isinstance(archive, dict)
        or set(archive) != {"sha256", "size", "url"}
        or archive["url"]
        != (
            "https://github.com/Sawmonabo/market-squawk/releases/download/"
            f"{tag}/{bundle_name}"
        )
        or archive["size"] != (root / bundle_name).stat().st_size
        or archive["sha256"] != file_sha256(root / bundle_name)
        or not isinstance(components, list)
        or not components
        or len(components) > MAXIMUM_FILES
    ):
        raise ReleaseBuildError("platform release archive identity is invalid")
    previous = None
    expanded = 0
    for component in components:
        if not isinstance(component, dict) or set(component) != {
            "executable",
            "path",
            "role",
            "sha256",
            "size",
        }:
            raise ReleaseBuildError("platform release component identity is invalid")
        path = component["path"]
        if not isinstance(path, str):
            raise ReleaseBuildError("platform release component path is invalid")
        validate_portable_path(path)
        if (
            previous is not None
            and previous >= path
            or not isinstance(component["size"], int)
            or component["size"] < 0
            or component["size"] > MAXIMUM_FILE_BYTES
            or re.fullmatch(r"[0-9a-f]{64}", str(component["sha256"])) is None
            or not isinstance(component["executable"], bool)
        ):
            raise ReleaseBuildError("platform release component identity is invalid")
        expanded += component["size"]
        if expanded > MAXIMUM_EXPANDED_BYTES:
            raise ReleaseBuildError("platform release component set is oversized")
        previous = path


def _admit_platform_checksums(root: Path, bundle_name: str, bootstrap_name: str) -> None:
    checksum = root / "SHA256SUMS"
    expected_names = {bundle_name, bootstrap_name, "market-squawk-release.json"}
    observed: dict[str, str] = {}
    for line in checksum.read_text(encoding="ascii").splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9][A-Za-z0-9._-]*)", line)
        if match is None or match.group(2) in observed:
            raise ReleaseBuildError("platform release checksum file is malformed")
        observed[match.group(2)] = match.group(1)
    if set(observed) != expected_names or any(
        observed[name] != file_sha256(root / name) for name in expected_names
    ):
        raise ReleaseBuildError("platform release checksum file is incomplete")


def _release_artifact(
    tag: str,
    path: Path,
    *,
    native_trust_mode: str | None = None,
) -> dict[str, object]:
    artifact = {
        "sha256": file_sha256(path),
        "size": path.stat().st_size,
        "url": (
            "https://github.com/Sawmonabo/market-squawk/releases/download/"
            f"{tag}/{path.name}"
        ),
    }
    if native_trust_mode is not None:
        artifact["native_trust_mode"] = native_trust_mode
    return artifact


def _write_json(path: Path, value: dict[str, object]) -> None:
    encoded = (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("ascii")
    if len(encoded) > 1024 * 1024:
        raise ReleaseBuildError("release index exceeds its fixed byte bound")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _write_bytes(path: Path, value: bytes) -> None:
    if not value or len(value) > MAXIMUM_UPDATE_METADATA_BYTES:
        raise ReleaseBuildError("update metadata output exceeds its fixed bound")
    path.parent.mkdir(parents=True, mode=0o755, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())


def _metadata_envelope(bytes_value: bytes, expected_role: str) -> dict[str, object]:
    if not bytes_value or len(bytes_value) > MAXIMUM_UPDATE_METADATA_BYTES:
        raise ReleaseBuildError(f"{expected_role} metadata exceeds its fixed bound")
    try:
        value = json.loads(bytes_value)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseBuildError(f"{expected_role} metadata is malformed") from error
    if not isinstance(value, dict) or set(value) != {"signatures", "signed"}:
        raise ReleaseBuildError(f"{expected_role} metadata envelope is invalid")
    signatures = value["signatures"]
    signed = value["signed"]
    if (
        not isinstance(signatures, list)
        or not signatures
        or len(signatures) > 64
        or not isinstance(signed, dict)
        or signed.get("_type") != expected_role
        or signed.get("spec_version") != UPDATE_SPEC_VERSION
        or type(signed.get("version")) is not int
        or signed["version"] < 1
        or not isinstance(signed.get("expires"), str)
        or not signed["expires"]
    ):
        raise ReleaseBuildError(f"{expected_role} signed metadata is invalid")
    seen = set()
    for signature in signatures:
        if (
            not isinstance(signature, dict)
            or set(signature) != {"keyid", "sig"}
            or not isinstance(signature["keyid"], str)
            or not isinstance(signature["sig"], str)
            or SHA256_PATTERN.fullmatch(signature["keyid"]) is None
            or SIGNATURE_PATTERN.fullmatch(signature["sig"]) is None
            or signature["keyid"] in seen
        ):
            raise ReleaseBuildError(f"{expected_role} metadata signatures are invalid")
        seen.add(signature["keyid"])
    if expected_role == "root":
        _validate_root_metadata(signed)
    return value


def _validate_root_metadata(signed: dict[str, object]) -> None:
    keys = signed.get("keys")
    roles = signed.get("roles")
    if (
        signed.get("consistent_snapshot") is not True
        or not isinstance(keys, dict)
        or not 1 <= len(keys) <= 64
        or not isinstance(roles, dict)
        or not {"root", "targets", "snapshot", "timestamp"}.issubset(roles)
    ):
        raise ReleaseBuildError("root trust roles are invalid")
    for key_id, key in keys.items():
        if (
            not isinstance(key_id, str)
            or SHA256_PATTERN.fullmatch(key_id) is None
            or not isinstance(key, dict)
            or key.get("keytype") != "ed25519"
            or key.get("scheme") != "ed25519"
            or not isinstance(key.get("keyval"), dict)
            or not isinstance(key["keyval"].get("public"), str)
            or SHA256_PATTERN.fullmatch(key["keyval"]["public"]) is None
        ):
            raise ReleaseBuildError("root public key is invalid")
    for role in roles.values():
        if not isinstance(role, dict) or set(role) != {"keyids", "threshold"}:
            raise ReleaseBuildError("root role definition is invalid")
        key_ids = role["keyids"]
        threshold = role["threshold"]
        if (
            not isinstance(key_ids, list)
            or any(not isinstance(key_id, str) for key_id in key_ids)
            or type(threshold) is not int
            or threshold < 1
            or threshold > len(key_ids)
            or len(set(key_ids)) != len(key_ids)
            or any(key_id not in keys for key_id in key_ids)
        ):
            raise ReleaseBuildError("root role threshold is invalid")


def _validate_update_base_url(value: str) -> None:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.endswith("/")
        or parsed.path.startswith("//")
    ):
        raise ReleaseBuildError("update repository base URL is invalid")


def _render_install_template(
    template: Path,
    output: Path,
    tag: str,
    bootstrap_digests: dict[str, str],
) -> None:
    replacements = {
        "__MARKET_SQUAWK_TAG__": tag,
        "__MARKET_SQUAWK_BOOTSTRAP_AARCH64_APPLE_DARWIN_SHA256__": bootstrap_digests[
            "aarch64-apple-darwin"
        ],
        "__MARKET_SQUAWK_BOOTSTRAP_X86_64_APPLE_DARWIN_SHA256__": bootstrap_digests[
            "x86_64-apple-darwin"
        ],
        "__MARKET_SQUAWK_BOOTSTRAP_X86_64_UNKNOWN_LINUX_GNU_SHA256__": bootstrap_digests[
            "x86_64-unknown-linux-gnu"
        ],
    }
    rendered = template.read_text(encoding="ascii")
    for token, value in replacements.items():
        if rendered.count(token) != 1:
            raise ReleaseBuildError("release installer template token count is invalid")
        rendered = rendered.replace(token, value)
    if "__MARKET_SQUAWK_" in rendered:
        raise ReleaseBuildError("release installer template is not fully rendered")
    with output.open("x", encoding="ascii", newline="\n") as stream:
        stream.write(rendered)
        stream.flush()
        os.fsync(stream.fileno())
    output.chmod(0o755)


def parse_options() -> Options:
    parser = argparse.ArgumentParser(
        description="Build one complete immutable Market Squawk platform bundle."
    )
    parser.add_argument("--target", required=True, choices=tuple(TARGETS))
    parser.add_argument(
        "--native-trust-mode",
        required=True,
        choices=ALL_NATIVE_TRUST_MODES,
    )
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--tree", required=True)
    parser.add_argument("--python-release", required=True, type=Path)
    parser.add_argument("--native-bundle", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--update-repository-base-url")
    parser.add_argument("--pinned-update-root", type=Path)
    parser.add_argument("--minimum-workspace-schema-version", type=int)
    parser.add_argument("--maximum-workspace-schema-version", type=int)
    values = parser.parse_args()
    update_values = (
        values.update_repository_base_url,
        values.pinned_update_root,
        values.minimum_workspace_schema_version,
        values.maximum_workspace_schema_version,
    )
    if (
        VERSION_PATTERN.fullmatch(values.version) is None
        or OBJECT_PATTERN.fullmatch(values.commit) is None
        or OBJECT_PATTERN.fullmatch(values.tree) is None
        or values.native_trust_mode not in NATIVE_TRUST_MODES[values.target]
        or any(value is None for value in update_values)
        and any(value is not None for value in update_values)
    ):
        raise ReleaseBuildError("release identity or native trust mode is invalid")
    if all(value is not None for value in update_values):
        _validate_update_base_url(values.update_repository_base_url)
        if (
            values.minimum_workspace_schema_version < 1
            or values.minimum_workspace_schema_version
            > values.maximum_workspace_schema_version
        ):
            raise ReleaseBuildError("workspace update compatibility range is invalid")
    return Options(
        target=TARGETS[values.target],
        native_trust_mode=values.native_trust_mode,
        version=values.version,
        commit=values.commit,
        tree=values.tree,
        python_release=values.python_release.expanduser().absolute(),
        native_bundle=values.native_bundle.expanduser().absolute(),
        output=values.output.expanduser().absolute(),
        update_repository_base_url=values.update_repository_base_url,
        pinned_update_root=(
            values.pinned_update_root.expanduser().absolute()
            if values.pinned_update_root is not None
            else None
        ),
        minimum_workspace_schema_version=values.minimum_workspace_schema_version,
        maximum_workspace_schema_version=values.maximum_workspace_schema_version,
    )


def validate_repository_identity(root: Path, options: Options) -> None:
    head = git(root, "rev-parse", "HEAD")
    tree = git(root, "rev-parse", "HEAD^{tree}")
    status = git(root, "status", "--porcelain=v1", "--untracked-files=all")
    if head != options.commit or tree != options.tree or status:
        raise ReleaseBuildError("release inputs require one clean exact repository revision")
    cargo_version = git_file_value(root / "Cargo.toml", 'version = "')
    if cargo_version != options.version:
        raise ReleaseBuildError("release version differs from the workspace version")
    host = subprocess.run(
        ["rustc", "--print", "host-tuple"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if host != options.target.target:
        raise ReleaseBuildError("release target differs from the native build host")


def git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def git_file_value(path: Path, prefix: str) -> str:
    matches = [
        line.removeprefix(prefix).removesuffix('"')
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.startswith(prefix) and line.endswith('"')
    ]
    if len(matches) != 1:
        raise ReleaseBuildError("workspace release version is not unique")
    return matches[0]


def claim_output(path: Path, repository_root: Path) -> Path:
    if path.is_symlink():
        raise ReleaseBuildError("release output must not be a symbolic link")
    if path.exists():
        if not path.is_dir() or any(path.iterdir()):
            raise ReleaseBuildError("release output must be a new or empty directory")
    else:
        parent = path.parent.resolve(strict=True)
        if parent == repository_root or parent.is_relative_to(repository_root):
            raise ReleaseBuildError("generated release output must remain outside source")
        path.mkdir(mode=0o700)
    output = path.resolve(strict=True)
    if output == repository_root or output.is_relative_to(repository_root):
        raise ReleaseBuildError("generated release output must remain outside source")
    return output


def assemble_staging(root: Path, staging: Path, options: Options) -> None:
    python_release = controlled_directory(options.python_release, "Python release")
    if python_release.name != "release-cp314":
        raise ReleaseBuildError("Python release root is not the canonical CPython 3.14 product")
    native_bundle = controlled_directory(options.native_bundle, "native bundle")
    expected_native = {source for source, _destination in options.target.native_inputs}
    if set(list_regular_paths(native_bundle)) != expected_native:
        raise ReleaseBuildError("native bundle does not contain its exact closed file set")

    portability_exclusions = dict(
        LOCKED_PYTHON_PORTABILITY_EXCLUSIONS.get(options.target.target, {})
    )
    for relative in list_regular_paths(python_release):
        if relative in BUILD_ONLY_PYTHON_PATHS:
            continue
        source = python_release / relative
        exclusion = portability_exclusions.pop(relative, None)
        if exclusion is not None:
            expected_size, expected_sha256 = exclusion
            admit_portability_exclusion(source, expected_size, expected_sha256)
            continue
        copy_stable(source, staging / relative, executable=is_executable(source))
    if portability_exclusions:
        raise ReleaseBuildError("locked Python portability exclusion is unavailable")
    for source_name, destination_name in options.target.native_inputs:
        copy_stable(
            native_bundle / source_name,
            staging / destination_name,
            executable=True,
        )
    write_package_update_contract(staging, options)
    for source_name, destination_name in (*LICENSE_INPUTS, *NOTICE_INPUTS):
        copy_stable(root / source_name, staging / destination_name, executable=False)

    paths = list_regular_paths(staging)
    required = {
        destination for _source, destination in options.target.native_inputs
    } | {
        f"bin/market-squawk{options.target.executable_suffix}",
        f"bin/market-squawk-onnx-worker{options.target.executable_suffix}",
        f"bin/market-squawk-model-validator{options.target.executable_suffix}",
        (
            "Scripts/market-squawk-train.exe"
            if options.target.executable_suffix
            else "bin/market-squawk-train"
        ),
        "python.exe" if options.target.executable_suffix else "bin/python",
    }
    if not required.issubset(paths):
        raise ReleaseBuildError("complete staging tree is missing a required product component")


def write_package_update_contract(staging: Path, options: Options) -> None:
    channel_path = staging / UPDATE_CHANNEL_PATH
    channel_path.parent.mkdir(parents=True, mode=0o755)
    if options.pinned_update_root is None:
        _write_json(
            channel_path,
            {
                "availability": "unavailable",
                "reason": "production-signing-material-unavailable",
                "schemaVersion": 1,
            },
        )
        return

    root = controlled_regular_file(
        options.pinned_update_root,
        "pinned update root",
        MAXIMUM_UPDATE_METADATA_BYTES,
    )
    root_path = staging / PINNED_UPDATE_ROOT_PATH
    copy_stable(root, root_path, executable=False)
    root_bytes = root_path.read_bytes()
    root_envelope = _metadata_envelope(root_bytes, "root")
    if root_envelope["signed"].get("version") != 1:
        raise ReleaseBuildError("pinned update root must be version 1")
    minimum = options.minimum_workspace_schema_version
    maximum = options.maximum_workspace_schema_version
    if minimum is None or maximum is None or options.update_repository_base_url is None:
        raise ReleaseBuildError("available update channel is incomplete")
    _write_json(
        channel_path,
        {
            "availability": "available",
            "minimumWorkspaceSchemaVersion": minimum,
            "maximumWorkspaceSchemaVersion": maximum,
            "pinnedRoot": {
                "path": "1.root.json",
                "sha256": hashlib.sha256(root_bytes).hexdigest(),
                "size": len(root_bytes),
            },
            "repositoryBaseUrl": options.update_repository_base_url,
            "schemaVersion": 1,
            "targets": {
                target: _update_target_paths(target) for target in sorted(TARGETS)
            },
        },
    )


def _update_target_paths(target: str) -> dict[str, str]:
    return {
        "archiveTargetPath": f"channels/stable/{target}/bundle.zip",
        "manifestTargetPath": f"channels/stable/{target}/manifest.json",
    }


def controlled_directory(path: Path, label: str) -> Path:
    metadata = path.lstat()
    if path.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        raise ReleaseBuildError(f"{label} is not a controlled directory")
    return path.resolve(strict=True)


def controlled_regular_file(path: Path, label: str, maximum: int) -> Path:
    metadata = path.lstat()
    if (
        path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_size == 0
        or metadata.st_size > maximum
    ):
        raise ReleaseBuildError(f"{label} is not a bounded regular file")
    return path.resolve(strict=True)


def list_regular_paths(root: Path) -> tuple[str, ...]:
    paths = []
    total = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        for child in sorted(directory.iterdir(), key=lambda value: value.name):
            metadata = child.lstat()
            if child.is_symlink():
                raise ReleaseBuildError("release input contains a symbolic link")
            if stat.S_ISDIR(metadata.st_mode):
                pending.append(child)
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise ReleaseBuildError("release input contains a special file")
            relative = child.relative_to(root).as_posix()
            validate_portable_path(relative)
            paths.append(relative)
            total += metadata.st_size
            if (
                len(paths) > MAXIMUM_FILES
                or metadata.st_size > MAXIMUM_FILE_BYTES
                or total > MAXIMUM_EXPANDED_BYTES
            ):
                raise ReleaseBuildError("release input exceeds its fixed size bounds")
    return tuple(sorted(paths))


def validate_portable_path(value: str) -> None:
    parts = value.split("/")
    if (
        not value
        or value.startswith("/")
        or value.endswith("/")
        or "\\" in value
        or any(part in {"", ".", ".."} for part in parts)
        or any(len(part.encode("utf-8")) > 255 for part in parts)
        or len(value.encode("utf-8")) > 1024
    ):
        raise ReleaseBuildError("release input contains a non-portable path")


def copy_stable(source: Path, destination: Path, *, executable: bool) -> None:
    before = source.lstat()
    if (
        source.is_symlink()
        or not stat.S_ISREG(before.st_mode)
        or before.st_size > MAXIMUM_FILE_BYTES
    ):
        raise ReleaseBuildError("release source is not a bounded regular file")
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
    if destination.exists() or destination.is_symlink():
        raise ReleaseBuildError("release staging path is duplicated")

    digest = hashlib.sha256()
    observed = 0
    with source.open("rb") as reader, destination.open("xb") as writer:
        while chunk := reader.read(COPY_BUFFER_BYTES):
            observed += len(chunk)
            if observed > before.st_size:
                raise ReleaseBuildError("release source changed while copying")
            digest.update(chunk)
            writer.write(chunk)
        writer.flush()
        os.fsync(writer.fileno())
    after = source.lstat()
    if stable_identity(before) != stable_identity(after) or observed != before.st_size:
        raise ReleaseBuildError("release source changed while copying")
    destination.chmod(0o755 if executable else 0o644)
    if file_sha256(destination) != digest.hexdigest():
        raise ReleaseBuildError("release staging copy changed after writing")


def admit_portability_exclusion(
    source: Path,
    expected_size: int,
    expected_sha256: str,
) -> None:
    before = source.lstat()
    if (
        source.is_symlink()
        or not stat.S_ISREG(before.st_mode)
        or before.st_size != expected_size
    ):
        raise ReleaseBuildError("locked Python portability exclusion changed")
    digest = hashlib.sha256()
    observed = 0
    with source.open("rb") as stream:
        while chunk := stream.read(COPY_BUFFER_BYTES):
            observed += len(chunk)
            if observed > expected_size:
                raise ReleaseBuildError("locked Python portability exclusion changed")
            digest.update(chunk)
    after = source.lstat()
    if (
        stable_identity(before) != stable_identity(after)
        or observed != expected_size
        or digest.hexdigest() != expected_sha256
    ):
        raise ReleaseBuildError("locked Python portability exclusion changed")


def stable_identity(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        stat.S_IFMT(metadata.st_mode),
    )


def is_executable(path: Path) -> bool:
    return path.lstat().st_mode & 0o111 != 0


def write_deterministic_zip(staging: Path, output: Path) -> None:
    if output.exists() or output.is_symlink():
        raise ReleaseBuildError("release archive output already exists")
    with zipfile.ZipFile(
        output,
        mode="x",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
        allowZip64=True,
    ) as archive:
        for relative in list_regular_paths(staging):
            source = staging / relative
            mode = 0o755 if is_executable(source) else 0o644
            information = zipfile.ZipInfo(relative, ZIP_TIMESTAMP)
            information.create_system = 3
            information.compress_type = zipfile.ZIP_DEFLATED
            information.external_attr = (stat.S_IFREG | mode) << 16
            information.flag_bits |= 0x800
            with source.open("rb") as reader, archive.open(information, "w") as writer:
                shutil.copyfileobj(reader, writer, COPY_BUFFER_BYTES)
    if output.stat().st_size == 0 or output.stat().st_size > MAXIMUM_ARCHIVE_BYTES:
        raise ReleaseBuildError("release archive exceeds its fixed byte bound")
    with zipfile.ZipFile(output, "r") as archive:
        if tuple(member.filename for member in archive.infolist()) != list_regular_paths(staging):
            raise ReleaseBuildError("release archive inventory changed after construction")
        bad = archive.testzip()
        if bad is not None:
            raise ReleaseBuildError(f"release archive failed integrity verification: {bad}")


def build_manifest(
    root: Path,
    staging: Path,
    bundle: Path,
    manifest: Path,
    options: Options,
) -> None:
    installer = staging / (
        f"bin/market-squawk-installer{options.target.executable_suffix}"
    )
    generated_at = git(root, "show", "-s", "--format=%cI", options.commit)
    archive_url = (
        "https://github.com/Sawmonabo/market-squawk/releases/download/"
        f"v{options.version}/{bundle.name}"
    )
    subprocess.run(
        [
            str(installer),
            "--json",
            "manifest",
            "build",
            "--version",
            options.version,
            "--commit",
            options.commit,
            "--tree",
            options.tree,
            "--generated-at",
            generated_at,
            "--staging-root",
            str(staging),
            "--bundle",
            str(bundle),
            "--archive-url",
            archive_url,
            "--native-trust-mode",
            options.native_trust_mode,
            "--output",
            str(manifest),
        ],
        cwd=root,
        check=True,
    )


def write_checksums(output: Path, artifacts: tuple[Path, ...]) -> None:
    checksum = output / "SHA256SUMS"
    lines = [f"{file_sha256(path)}  {path.name}\n" for path in sorted(artifacts)]
    with checksum.open("x", encoding="ascii", newline="\n") as stream:
        stream.writelines(lines)
        stream.flush()
        os.fsync(stream.fileno())


def verify_output_set(output: Path, artifacts: tuple[Path, ...]) -> None:
    expected = {path.name for path in artifacts} | {"SHA256SUMS"}
    observed = {path.name for path in output.iterdir()}
    if observed != expected or any(path.is_symlink() or not path.is_file() for path in output.iterdir()):
        raise ReleaseBuildError("complete release output set is not closed")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(COPY_BUFFER_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
