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
import zipfile


MAXIMUM_FILES = 32_768
MAXIMUM_FILE_BYTES = 1024 * 1024 * 1024
MAXIMUM_EXPANDED_BYTES = 4 * 1024 * 1024 * 1024
MAXIMUM_ARCHIVE_BYTES = 2 * 1024 * 1024 * 1024
MAXIMUM_PLATFORM_MANIFEST_BYTES = 8 * 1024 * 1024
COPY_BUFFER_BYTES = 1024 * 1024
ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)
VERSION_PATTERN = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
OBJECT_PATTERN = re.compile(r"[0-9a-f]{40}")
BUILD_ONLY_PYTHON_PATHS = frozenset({".lock", ".market-squawk-owned-v1"})
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


@dataclass(frozen=True)
class Options:
    target: TargetProfile
    version: str
    commit: str
    tree: str
    python_release: Path
    native_bundle: Path
    output: Path


@dataclass(frozen=True)
class AggregateOptions:
    inputs: tuple[Path, ...]
    install_template: Path
    output: Path


@dataclass(frozen=True)
class CollectOptions:
    target: TargetProfile
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
    values = parser.parse_args(arguments)
    return AggregateOptions(
        inputs=tuple(path.expanduser().absolute() for path in values.input),
        install_template=values.install_template.expanduser().absolute(),
        output=values.output.expanduser().absolute(),
    )


def parse_collect_options(arguments: list[str]) -> CollectOptions:
    parser = argparse.ArgumentParser(
        description="Collect one platform's complete bundle and native packages."
    )
    parser.add_argument("--target", required=True, choices=tuple(TARGETS))
    parser.add_argument("--version", required=True)
    parser.add_argument("--release-output", required=True, type=Path)
    parser.add_argument("--native-bundle", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    values = parser.parse_args(arguments)
    if VERSION_PATTERN.fullmatch(values.version) is None:
        raise ReleaseBuildError("native package version is malformed")
    return CollectOptions(
        target=TARGETS[values.target],
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
    expected = base_names | {
        f"market-squawk-{options.version}-{target}{package_suffix}"
        for package_suffix in expected_suffixes
    }
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
                    _release_artifact(reference["tag"], output / name)
                    for name in package_names
                ],
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
        "schema_version": 1,
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
        path for path in sorted(output.iterdir()) if path.name != "SHA256SUMS"
    )
    write_checksums(output, artifacts)
    expected_outputs.add("SHA256SUMS")
    observed = {path.name for path in output.iterdir()}
    if observed != expected_outputs or any(
        path.is_symlink() or not path.is_file() for path in output.iterdir()
    ):
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
        or manifest["schema_version"] != 1
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
        or set(target_release) != {"archive", "components", "minimum_system", "target"}
        or target_release["target"] not in TARGETS
        or target_release["minimum_system"]
        != MINIMUM_SYSTEMS[target_release["target"]]
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
    return {
        "asset_names": expected_names - {"SHA256SUMS"},
        "bootstrap_name": bootstrap_name,
        "manifest": manifest,
        "package_names": package_names,
        "root": root,
        "target": target_release,
    }


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


def _release_artifact(tag: str, path: Path) -> dict[str, object]:
    return {
        "sha256": file_sha256(path),
        "size": path.stat().st_size,
        "url": (
            "https://github.com/Sawmonabo/market-squawk/releases/download/"
            f"{tag}/{path.name}"
        ),
    }


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
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--tree", required=True)
    parser.add_argument("--python-release", required=True, type=Path)
    parser.add_argument("--native-bundle", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    values = parser.parse_args()
    if (
        VERSION_PATTERN.fullmatch(values.version) is None
        or OBJECT_PATTERN.fullmatch(values.commit) is None
        or OBJECT_PATTERN.fullmatch(values.tree) is None
    ):
        raise ReleaseBuildError("version, commit, or tree identity is malformed")
    return Options(
        target=TARGETS[values.target],
        version=values.version,
        commit=values.commit,
        tree=values.tree,
        python_release=values.python_release.expanduser().absolute(),
        native_bundle=values.native_bundle.expanduser().absolute(),
        output=values.output.expanduser().absolute(),
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

    for relative in list_regular_paths(python_release):
        if relative in BUILD_ONLY_PYTHON_PATHS:
            continue
        source = python_release / relative
        copy_stable(source, staging / relative, executable=is_executable(source))
    for source_name, destination_name in options.target.native_inputs:
        copy_stable(
            native_bundle / source_name,
            staging / destination_name,
            executable=True,
        )
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


def controlled_directory(path: Path, label: str) -> Path:
    metadata = path.lstat()
    if path.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        raise ReleaseBuildError(f"{label} is not a controlled directory")
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
