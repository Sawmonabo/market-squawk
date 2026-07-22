#!/usr/bin/env python3
"""Build and verify the hash-locked Market Squawk Python product offline."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from email.parser import BytesParser
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import zipfile


MAX_LOCK_BYTES = 2 * 1024 * 1024
MAX_ARTIFACTS = 64
MAX_SOURCES = 2_048
RUST_TOOLCHAIN = "1.97.1"
RUST_TOOLCHAIN_FULL = "1.97.1-aarch64-apple-darwin"
MACOS_DEPLOYMENT_TARGET = "12.0"
PROJECT_WHEEL_PLATFORM_TAG = (
    f"macosx_{MACOS_DEPLOYMENT_TARGET.replace('.', '_')}_arm64"
)
SUPPORTED_PYTHONS = ((3, 12), (3, 13))
ROOT_MARKER = ".market-squawk-python-artifacts-v1"
CHILD_MARKER = ".market-squawk-owned-v1"
ROOT_PURPOSE = "market-squawk-python-release-artifacts"
ALLOWED_LICENSES = {
    "Apache-2.0",
    "MIT",
    "BSD-2-Clause",
    "MIT OR Apache-2.0",
    "Apache-2.0 OR BSD-2-Clause",
}
REQUIRED_PROJECTS = {
    "maturin",
    "pyarrow",
    "pytest",
    "packaging",
    "pluggy",
    "iniconfig",
    "pygments",
}
FOCUSED_TESTS = (
    "python/tests/test_data.py",
    "python/tests/test_finance_parity.py",
    "python/tests/test_training_bundle.py",
    "python/tests/test_visualization_examples.py",
)


class ReleaseBuildError(RuntimeError):
    """A release authority, source, wheel, runtime, or build contract failed."""


@dataclass(frozen=True)
class Artifact:
    project: str
    version: str
    license: str
    filename: str
    sha256: str
    size_bytes: int
    url: str


@dataclass(frozen=True)
class Source:
    path: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class ReleaseLock:
    minimum: tuple[int, int]
    maximum_exclusive: tuple[int, int]
    platform: str
    artifacts: tuple[Artifact, ...]
    sources: tuple[Source, ...]

    @classmethod
    def for_test(cls, *, filename: str, sha256: str, size_bytes: int) -> "ReleaseLock":
        return cls(
            (3, 12),
            (3, 14),
            "macos-arm64",
            (
                Artifact(
                    "fixture",
                    "1.0",
                    "MIT",
                    filename,
                    sha256,
                    size_bytes,
                    "https://files.pythonhosted.org/fixture.whl",
                ),
            ),
            (),
        )


@dataclass(frozen=True)
class PythonRuntime:
    executable: Path
    version: tuple[int, int, int]


@dataclass(frozen=True)
class ArtifactLayout:
    root: Path
    wheelhouse: Path
    cargo_home: Path
    build_home: Path
    build_venv: Path
    distribution: Path
    releases: tuple[tuple[tuple[int, int], Path], ...]


def load_lock(path: Path) -> ReleaseLock:
    try:
        if path.is_symlink():
            raise ReleaseBuildError("Python release lock must not be a symbolic link")
        raw = path.read_bytes()
        if not raw or len(raw) > MAX_LOCK_BYTES:
            raise ReleaseBuildError("Python release lock exceeds its byte bound")
        value = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseBuildError("Python release lock is unreadable") from error
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "python",
        "platform",
        "artifacts",
        "sources",
    }:
        raise ReleaseBuildError("Python release lock shape is invalid")
    if value["schema_version"] != 1 or value["platform"] != "macos-arm64":
        raise ReleaseBuildError("Python release lock version or platform is unsupported")
    python = value["python"]
    if not isinstance(python, dict) or set(python) != {"minimum", "maximum_exclusive"}:
        raise ReleaseBuildError("Python interpreter matrix is invalid")
    minimum = _version(python["minimum"])
    maximum = _version(python["maximum_exclusive"])
    if minimum != (3, 12) or maximum != (3, 14):
        raise ReleaseBuildError("Python lock must claim only the verified CPython 3.12-3.13 matrix")
    artifacts_value = value["artifacts"]
    if (
        not isinstance(artifacts_value, list)
        or not artifacts_value
        or len(artifacts_value) > MAX_ARTIFACTS
    ):
        raise ReleaseBuildError("Python wheel count is invalid")
    artifacts = []
    names: set[str] = set()
    for item in artifacts_value:
        if not isinstance(item, dict) or set(item) != {
            "project",
            "version",
            "license",
            "filename",
            "sha256",
            "size_bytes",
            "url",
        }:
            raise ReleaseBuildError("Python wheel identity is incomplete")
        if item["filename"] in names or item["license"] not in ALLOWED_LICENSES:
            raise ReleaseBuildError("Python wheel identity or license is invalid")
        _sha256(item["sha256"])
        if (
            item["sha256"] == "0" * 64
            or not isinstance(item["size_bytes"], int)
            or item["size_bytes"] <= 0
        ):
            raise ReleaseBuildError("Python wheel hash or size is invalid")
        parsed = urllib.parse.urlparse(item["url"])
        if (
            parsed.scheme != "https"
            or parsed.hostname != "files.pythonhosted.org"
            or Path(parsed.path).name != item["filename"]
        ):
            raise ReleaseBuildError("Python wheel URL is not an exact official artifact")
        _wheel_tags(item["filename"])
        artifacts.append(Artifact(**item))
        names.add(item["filename"])
    sources_value = value["sources"]
    if not isinstance(sources_value, list) or len(sources_value) > MAX_SOURCES:
        raise ReleaseBuildError("Python source lock is invalid")
    sources = []
    source_paths: set[str] = set()
    for item in sources_value:
        if not isinstance(item, dict) or set(item) != {"path", "sha256", "size_bytes"}:
            raise ReleaseBuildError("Python source identity is incomplete")
        path = Path(item["path"])
        if item["path"] in source_paths or path.is_absolute() or ".." in path.parts:
            raise ReleaseBuildError("Python source path is invalid")
        _sha256(item["sha256"])
        if (
            item["sha256"] == "0" * 64
            or not isinstance(item["size_bytes"], int)
            or item["size_bytes"] <= 0
        ):
            raise ReleaseBuildError("Python source hash or size is invalid")
        sources.append(Source(**item))
        source_paths.add(item["path"])
    return ReleaseLock(minimum, maximum, value["platform"], tuple(artifacts), tuple(sources))


def admit_artifact_root(path: Path, repository_root: Path) -> ArtifactLayout:
    """Claim or re-open one explicit root; only its marked direct children are mutable."""

    repository_root = repository_root.resolve(strict=True)
    home = Path.home().resolve(strict=True)
    candidate = path.expanduser().absolute()
    if candidate.is_symlink():
        raise ReleaseBuildError("artifact root must not be a symbolic link")
    if candidate.exists():
        if not candidate.is_dir():
            raise ReleaseBuildError("artifact root must be a directory")
        canonical = candidate.resolve(strict=True)
    else:
        parent = candidate.parent.resolve(strict=True)
        canonical = parent / candidate.name
    if canonical in {Path("/"), home, repository_root}:
        raise ReleaseBuildError("artifact root resolves to a protected root")
    marker = canonical / ROOT_MARKER
    expected = _marker_content(canonical, ROOT_PURPOSE)
    if canonical.exists():
        entries = tuple(canonical.iterdir())
        if marker not in entries:
            if entries:
                raise ReleaseBuildError("existing artifact root is not owned by this builder")
            marker.write_text(expected, encoding="utf-8")
        elif marker.is_symlink() or not marker.is_file() or marker.read_text() != expected:
            raise ReleaseBuildError("artifact root ownership marker is invalid")
    else:
        canonical.mkdir()
        marker.write_text(expected, encoding="utf-8")
    releases = tuple(
        (version, canonical / f"release-cp{version[0]}{version[1]}")
        for version in SUPPORTED_PYTHONS
    )
    return ArtifactLayout(
        canonical,
        canonical / "wheelhouse",
        canonical / "cargo-home",
        canonical / "build-home",
        canonical / "build-venv",
        canonical / "dist",
        releases,
    )


def admit_wheelhouse(
    lock: ReleaseLock,
    wheelhouse: Path,
    version: tuple[int, int],
) -> tuple[Path, ...]:
    _admit_owned_child(wheelhouse, wheelhouse.parent, "wheelhouse")
    selected = tuple(
        artifact for artifact in lock.artifacts if _compatible(artifact.filename, version)
    )
    projects = {artifact.project.lower() for artifact in selected}
    if not REQUIRED_PROJECTS <= projects:
        raise ReleaseBuildError("wheelhouse has no complete compatible dependency set")
    admitted = []
    for artifact in selected:
        path = wheelhouse / artifact.filename
        if path.is_symlink() or not path.is_file():
            raise ReleaseBuildError("offline wheelhouse is missing a locked artifact")
        if _file_digest(path) != (artifact.size_bytes, artifact.sha256):
            raise ReleaseBuildError("offline wheelhouse artifact hash or size mismatch")
        _admit_license(path, artifact.license)
        admitted.append(path)
    return tuple(admitted)


def prepare_wheelhouse(
    lock: ReleaseLock,
    wheelhouse: Path,
    source_cache: Path | None,
) -> None:
    if os.environ.get("MARKET_SQUAWK_PYTHON_WHEEL_PREPARE_NETWORK") != "1" and source_cache is None:
        raise ReleaseBuildError("wheel preparation requires an explicit cache or network authorization")
    _admit_owned_child(wheelhouse, wheelhouse.parent, "wheelhouse")
    selected = {
        artifact.filename: artifact
        for artifact in lock.artifacts
        if any(_compatible(artifact.filename, version) for version in SUPPORTED_PYTHONS)
    }
    for artifact in selected.values():
        destination = wheelhouse / artifact.filename
        if destination.is_file() and _file_digest(destination) == (
            artifact.size_bytes,
            artifact.sha256,
        ):
            continue
        temporary = destination.with_suffix(destination.suffix + ".part")
        temporary.unlink(missing_ok=True)
        cached = source_cache / artifact.filename if source_cache is not None else None
        if cached is not None and cached.is_file():
            shutil.copyfile(cached, temporary)
        elif os.environ.get("MARKET_SQUAWK_PYTHON_WHEEL_PREPARE_NETWORK") == "1":
            try:
                with urllib.request.urlopen(
                    artifact.url, timeout=60
                ) as response, temporary.open("wb") as output:
                    shutil.copyfileobj(response, output, length=1024 * 1024)
            except (OSError, urllib.error.URLError) as error:
                temporary.unlink(missing_ok=True)
                raise ReleaseBuildError("authorized wheel preparation failed") from error
        else:
            raise ReleaseBuildError("locked wheel is absent from the explicit source cache")
        if _file_digest(temporary) != (artifact.size_bytes, artifact.sha256):
            temporary.unlink(missing_ok=True)
            raise ReleaseBuildError("prepared wheel hash or size mismatch")
        _admit_license(temporary, artifact.license)
        os.replace(temporary, destination)


def prepare_cargo_cache(
    root: Path, cargo_home: Path, toolchain: dict[str, object]
) -> None:
    """Populate or verify one explicitly owned Cargo cache for the locked graph."""

    _admit_owned_child(cargo_home, cargo_home.parent, "cargo-home")
    network = os.environ.get("MARKET_SQUAWK_PYTHON_WHEEL_PREPARE_NETWORK") == "1"
    environment = _cargo_environment(
        root,
        cargo_home,
        cargo_home,
        toolchain,
        offline=not network,
    )
    command = [
        str(_bound_tool(toolchain, "cargo")),
        "fetch",
        "--locked",
        "--manifest-path",
        str(root / "Cargo.toml"),
    ]
    if not network:
        command.append("--offline")
    _run(command, root, environment)


def expected_source_paths(root: Path) -> tuple[str, ...]:
    """Compute the complete local Rust build closure plus Python release material."""

    root = root.resolve(strict=True)
    paths = {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml"}
    for relative in (
        "python/market_squawk",
        "python/tests",
        "python/examples",
        "python/fixtures",
    ):
        paths.update(_regular_files(root, root / relative))
    paths.update(
        {
            "python/pyproject.toml",
            "python/requirements.lock",
            "scripts/build_python_release.py",
            "scripts/tests/test_build_python_release.py",
        }
    )
    workspace = _toml(root / "Cargo.toml")
    workspace_dependencies = workspace["workspace"]["dependencies"]
    pending = [
        root / "crates/market-squawk-python/Cargo.toml",
        root / "crates/market-squawk-modeling/Cargo.toml",
    ]
    visited: set[Path] = set()
    while pending:
        manifest = pending.pop().resolve(strict=True)
        if manifest in visited:
            continue
        if root not in manifest.parents:
            raise ReleaseBuildError("local Rust dependency escapes the repository")
        visited.add(manifest)
        package_root = manifest.parent
        paths.add(manifest.relative_to(root).as_posix())
        build_script = package_root / "build.rs"
        if build_script.is_file():
            paths.add(build_script.relative_to(root).as_posix())
        for material in ("src", "build_support", "migrations"):
            directory = package_root / material
            if directory.is_dir():
                paths.update(_regular_files(root, directory))
        value = _toml(manifest)
        for dependency in _dependency_specs(value):
            dependency_path = dependency.get("path")
            if dependency_path is None and dependency.get("workspace") is True:
                name = dependency["_name"]
                workspace_spec = workspace_dependencies.get(name)
                if isinstance(workspace_spec, dict):
                    dependency_path = workspace_spec.get("path")
                    if dependency_path is not None:
                        pending.append((root / dependency_path / "Cargo.toml"))
                continue
            if isinstance(dependency_path, str):
                pending.append(package_root / dependency_path / "Cargo.toml")
    return tuple(sorted(paths))


def admit_sources(lock: ReleaseLock, root: Path) -> None:
    expected = expected_source_paths(root)
    locked = tuple(sorted(source.path for source in lock.sources))
    if not locked or locked != expected:
        raise ReleaseBuildError("Python release lock does not bind the complete source closure")
    for source in lock.sources:
        path = root / source.path
        if path.is_symlink() or not path.is_file():
            raise ReleaseBuildError("Python release source is unavailable")
        if _file_digest(path) != (source.size_bytes, source.sha256):
            raise ReleaseBuildError("Python release source identity mismatch")


def admit_toolchain(root: Path) -> dict[str, object]:
    """Bind direct Rust/Xcode executables, stdlib, SDK, and target policy."""

    configured = _toml(root / "rust-toolchain.toml")["toolchain"]["channel"]
    rust_version = _toml(root / "Cargo.toml")["workspace"]["package"]["rust-version"]
    if configured != RUST_TOOLCHAIN or rust_version != RUST_TOOLCHAIN:
        raise ReleaseBuildError("repository Rust policy is not pinned to 1.97.1")
    evidence_environment = {"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"}
    rustup_discovered = shutil.which("rustup")
    if rustup_discovered is None:
        raise ReleaseBuildError("rustup is required to resolve direct pinned Rust tools")
    rustup = Path(rustup_discovered).resolve(strict=True)
    cargo_path = _direct_rust_tool(rustup, "cargo", root, evidence_environment)
    rustc_path = _direct_rust_tool(rustup, "rustc", root, evidence_environment)
    cargo_version = _run_output([str(cargo_path), "-vV"], root, evidence_environment)
    rustc_version = _run_output([str(rustc_path), "-vV"], root, evidence_environment)
    _require_tool_release(cargo_version, "cargo", RUST_TOOLCHAIN)
    _require_tool_release(rustc_version, "rustc", RUST_TOOLCHAIN)

    xcrun = Path("/usr/bin/xcrun").resolve(strict=True)
    clang = _xcode_tool(xcrun, "clang", root, evidence_environment)
    clangxx = _xcode_tool(xcrun, "clang++", root, evidence_environment)
    linker = _xcode_tool(xcrun, "ld", root, evidence_environment)
    archiver = _xcode_tool(xcrun, "ar", root, evidence_environment)
    ranlib = _xcode_tool(xcrun, "ranlib", root, evidence_environment)
    xcodebuild = _xcode_tool(xcrun, "xcodebuild", root, evidence_environment)
    sdk = Path(
        _run_output(
            [str(xcrun), "--sdk", "macosx", "--show-sdk-path"],
            root,
            evidence_environment,
        )
    ).resolve(strict=True)
    sdk_version = _run_output(
        [str(xcrun), "--sdk", "macosx", "--show-sdk-version"],
        root,
        evidence_environment,
    )
    developer_dir = next(
        (
            candidate
            for candidate in (sdk, *sdk.parents)
            if candidate.name == "Developer" and candidate.parent.name == "Contents"
        ),
        None,
    )
    if developer_dir is None:
        raise ReleaseBuildError("macOS SDK is outside a recognized Xcode developer directory")
    sysroot = Path(
        _run_output(
            [str(rustc_path), "--print", "sysroot"], root, evidence_environment
        )
    ).resolve(strict=True)
    if sysroot != rustc_path.parent.parent:
        raise ReleaseBuildError("direct rustc and reported sysroot identities differ")
    sdk_settings = tuple(
        _file_binding(path)
        for path in (
            sdk / "SDKSettings.json",
            sdk / "SDKSettings.plist",
            sdk / "System/Library/CoreServices/SystemVersion.plist",
        )
    )
    return {
        "schema_version": 1,
        "target": "aarch64-apple-darwin",
        "macos_deployment_target": MACOS_DEPLOYMENT_TARGET,
        "cargo": _tool_binding(cargo_path, cargo_version),
        "rustc": _tool_binding(rustc_path, rustc_version),
        "rust_stdlib": _tree_binding(sysroot / "lib/rustlib/aarch64-apple-darwin/lib"),
        "rustup": _tool_binding(
            rustup,
            _run_output([str(rustup), "--version"], root, evidence_environment),
        ),
        "xcrun": _tool_binding(
            xcrun,
            _run_output([str(xcrun), "--version"], root, evidence_environment),
        ),
        "clang": _tool_binding(
            clang,
            _run_output([str(clang), "--version"], root, evidence_environment),
        ),
        "clangxx": _tool_binding(
            clangxx,
            _run_output([str(clangxx), "--version"], root, evidence_environment),
        ),
        "linker": _tool_binding(linker),
        "archiver": _tool_binding(archiver),
        "ranlib": _tool_binding(ranlib),
        "xcode": _tool_binding(
            xcodebuild,
            _run_output([str(xcodebuild), "-version"], root, evidence_environment),
        ),
        "developer_dir": str(developer_dir),
        "sdk": {
            "path": str(sdk),
            "version": sdk_version,
            "settings": sdk_settings,
            "settings_sha256": _mapping_sha256(sdk_settings),
        },
    }


def _direct_rust_tool(
    rustup: Path,
    name: str,
    root: Path,
    environment: dict[str, str],
) -> Path:
    path = Path(
        _run_output(
            [
                str(rustup),
                "which",
                "--toolchain",
                RUST_TOOLCHAIN_FULL,
                name,
            ],
            root,
            environment,
        )
    ).resolve(strict=True)
    if path.name != name or not path.is_file():
        raise ReleaseBuildError("rustup returned an invalid direct Rust tool")
    return path


def _xcode_tool(
    xcrun: Path,
    name: str,
    root: Path,
    environment: dict[str, str],
) -> Path:
    path = Path(
        _run_output(
            [str(xcrun), "--sdk", "macosx", "--find", name], root, environment
        )
    ).resolve(strict=True)
    if not path.is_file():
        raise ReleaseBuildError("xcrun returned an invalid Xcode tool")
    return path


def _tool_binding(path: Path, version: str | None = None) -> dict[str, object]:
    binding = _file_binding(path)
    if version is not None:
        binding["version"] = version
    return binding


def _file_binding(path: Path) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        raise ReleaseBuildError("release input is not a direct regular file")
    size, digest = _file_digest(path)
    return {"path": str(path), "sha256": digest, "size_bytes": size}


def _tree_binding(root: Path) -> dict[str, object]:
    if root.is_symlink() or not root.is_dir():
        raise ReleaseBuildError("Rust standard-library root is invalid")
    files = []
    total = 0
    digest = hashlib.sha256(b"market-squawk/python-rust-stdlib-tree/v1\0")
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ReleaseBuildError("Rust standard-library tree contains an unsafe entry")
        if path.is_dir():
            continue
        if not path.is_file():
            raise ReleaseBuildError("Rust standard-library tree contains an unsafe entry")
        size, content_sha256 = _file_digest(path)
        total += size
        if len(files) >= 512 or total > 512 * 1024 * 1024:
            raise ReleaseBuildError("Rust standard-library tree exceeds its evidence bound")
        relative = path.relative_to(root).as_posix()
        digest.update(len(relative.encode()).to_bytes(8, "big"))
        digest.update(relative.encode())
        digest.update(size.to_bytes(8, "big"))
        digest.update(bytes.fromhex(content_sha256))
        files.append(relative)
    if not files:
        raise ReleaseBuildError("Rust standard-library tree is empty")
    return {
        "root": str(root),
        "file_count": len(files),
        "size_bytes": total,
        "sha256": digest.hexdigest(),
    }


def admit_runtimes(paths: tuple[Path, ...], lock: ReleaseLock) -> tuple[PythonRuntime, ...]:
    if len(paths) != len(SUPPORTED_PYTHONS):
        raise ReleaseBuildError("one interpreter is required for each supported Python minor")
    admitted: dict[tuple[int, int], PythonRuntime] = {}
    canonical_paths: set[Path] = set()
    for path in paths:
        try:
            executable = path.expanduser().resolve(strict=True)
        except OSError as error:
            raise ReleaseBuildError("Python interpreter is unavailable") from error
        if not executable.is_file() or executable in canonical_paths:
            raise ReleaseBuildError("Python interpreter identity is invalid")
        evidence, version = _interpreter_evidence(executable, executable.parent)
        minor = version[:2]
        if (
            evidence.get("implementation") != "cpython"
            or evidence.get("system") != "Darwin"
            or evidence.get("machine") != "arm64"
            or len(version) != 3
            or not lock.minimum <= minor < lock.maximum_exclusive
            or minor not in SUPPORTED_PYTHONS
            or minor in admitted
        ):
            raise ReleaseBuildError("Python interpreter is outside the exact support matrix")
        canonical_paths.add(executable)
        admitted[minor] = PythonRuntime(executable, version)
    if tuple(sorted(admitted)) != SUPPORTED_PYTHONS:
        raise ReleaseBuildError("Python 3.12 and 3.13 must both be independently admitted")
    return tuple(admitted[minor] for minor in SUPPORTED_PYTHONS)


def build_release(
    root: Path,
    lock_path: Path,
    lock: ReleaseLock,
    layout: ArtifactLayout,
    runtimes: tuple[PythonRuntime, ...],
    toolchain: dict[str, object],
) -> None:
    admit_sources(lock, root)
    for runtime in runtimes:
        admit_wheelhouse(lock, layout.wheelhouse, runtime.version[:2])
    _admit_owned_child(layout.cargo_home, layout.root, "cargo-home")
    _reset_owned_child(layout.build_home, layout.root, "build-home")
    (layout.build_home / "tmp").mkdir()
    _reset_owned_child(layout.build_venv, layout.root, "build-venv")
    _reset_owned_child(layout.distribution, layout.root, "dist")
    bootstrap_environment = _cargo_environment(
        root,
        layout.cargo_home,
        layout.build_home,
        toolchain,
        offline=True,
    )
    build_runtime = runtimes[0]
    build_python = _create_venv(
        build_runtime,
        layout.build_venv,
        root,
        bootstrap_environment,
    )
    build_wheels = admit_wheelhouse(lock, layout.wheelhouse, build_runtime.version[:2])
    maturin = next(path for path in build_wheels if path.name.startswith("maturin-"))
    _run(
        [
            build_python,
            "-I",
            "-m",
            "pip",
            "install",
            "--no-index",
            "--no-deps",
            str(maturin),
        ],
        root,
        bootstrap_environment,
    )
    environment = dict(bootstrap_environment)
    environment["PYO3_PYTHON"] = build_python
    _run(
        [
            build_python,
            "-I",
            "-m",
            "maturin",
            "build",
            "--release",
            "--locked",
            "--out",
            str(layout.distribution),
        ],
        root / "python",
        environment,
    )
    project_wheels = list(layout.distribution.glob("market_squawk-0.1.0-*.whl"))
    if len(project_wheels) != 1:
        raise ReleaseBuildError("maturin did not create exactly one project wheel")
    project_wheel = project_wheels[0]
    python_tag, abi_tag, platform_tag = _wheel_tags(project_wheel.name)
    if (
        python_tag != "cp310"
        or abi_tag != "abi3"
        or platform_tag != PROJECT_WHEEL_PLATFORM_TAG
    ):
        raise ReleaseBuildError(
            "project wheel does not carry the exact pinned cp310-abi3 macOS platform tag"
        )
    _run(
        [
            str(_bound_tool(toolchain, "cargo")),
            "build",
            "-p",
            "market-squawk-modeling",
            "--bin",
            "market-squawk-model-validator",
            "--release",
            "--locked",
        ],
        root,
        environment,
    )
    validator = root / "target/release/market-squawk-model-validator"
    if not validator.is_file():
        raise ReleaseBuildError("Rust model validator executable was not produced")
    matrix_evidence = []
    for runtime, (minor, release_venv) in zip(runtimes, layout.releases, strict=True):
        if runtime.version[:2] != minor:
            raise ReleaseBuildError("release environment and interpreter identity differ")
        _reset_owned_child(release_venv, layout.root, f"release-cp{minor[0]}{minor[1]}")
        release_python = _create_venv(
            runtime,
            release_venv,
            root,
            bootstrap_environment,
        )
        runtime_environment = dict(bootstrap_environment)
        _run(
            [
                release_python,
                "-I",
                "-m",
                "pip",
                "install",
                "--no-index",
                "--find-links",
                str(layout.wheelhouse),
                "--require-hashes",
                "--only-binary",
                ":all:",
                "-r",
                str(root / "python/requirements.lock"),
            ],
            root,
            runtime_environment,
        )
        _run(
            [
                release_python,
                "-I",
                "-m",
                "pip",
                "install",
                "--no-index",
                "--no-deps",
                str(project_wheel),
            ],
            root,
            runtime_environment,
        )
        validator_destination = release_venv / "bin/market-squawk-model-validator"
        shutil.copy2(validator, validator_destination)
        validator_destination.chmod(0o755)
        _run(
            [
                release_python,
                "-I",
                "-c",
                "import market_squawk, market_squawk._native",
            ],
            root,
            runtime_environment,
        )
        _run(
            [
                release_python,
                "-I",
                "-m",
                "pytest",
                *(str(root / path) for path in FOCUSED_TESTS),
                "-q",
            ],
            root,
            runtime_environment,
        )
        matrix_evidence.append(
            {
                "python": _run_output(
                    [release_python, "--version"], root, runtime_environment
                ),
                "python_executable_sha256": _file_digest(runtime.executable)[1],
                "pip": _run_output(
                    [release_python, "-I", "-m", "pip", "--version"],
                    root,
                    runtime_environment,
                ),
                "focused_tests": list(FOCUSED_TESTS),
                "validator_sha256": _file_digest(validator_destination)[1],
            }
        )
    evidence = {
        "schema_version": 3,
        "support_matrix": matrix_evidence,
        "toolchain": toolchain,
        "build_environment": {
            "sha256": _mapping_sha256(environment),
            "values": environment,
        },
        "cargo_lock_sha256": _file_digest(root / "Cargo.lock")[1],
        "wheelhouse_lock_sha256": _file_digest(lock_path)[1],
        "requirements_lock_sha256": _file_digest(root / "python/requirements.lock")[1],
        "source_closure_count": len(lock.sources),
        "project_wheel": {
            "filename": project_wheel.name,
            "sha256": _file_digest(project_wheel)[1],
            "python_tag": python_tag,
            "abi_tag": abi_tag,
            "platform_tag": platform_tag,
            "macos_deployment_target": MACOS_DEPLOYMENT_TARGET,
        },
    }
    (layout.root / "market-squawk-release.json").write_text(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    _remove_owned_child(layout.build_venv, layout.root, "build-venv")
    _remove_owned_child(layout.build_home, layout.root, "build-home")


def _marker_content(path: Path, purpose: str) -> str:
    return json.dumps(
        {"canonical_root": str(path), "purpose": purpose, "schema_version": 1},
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n"


def _admit_owned_child(path: Path, root: Path, purpose: str) -> None:
    root = root.resolve(strict=True)
    if path.parent != root or path.is_symlink():
        raise ReleaseBuildError("artifact child is not a direct non-symlink child")
    expected = _marker_content(path, purpose)
    marker = path / CHILD_MARKER
    if path.exists():
        if not path.is_dir() or marker.is_symlink() or not marker.is_file():
            raise ReleaseBuildError("artifact child is not owned by this builder")
        if marker.read_text(encoding="utf-8") != expected:
            raise ReleaseBuildError("artifact child ownership marker is invalid")
        return
    path.mkdir()
    marker.write_text(expected, encoding="utf-8")


def _reset_owned_child(path: Path, root: Path, purpose: str) -> None:
    _admit_owned_child(path, root, purpose)
    shutil.rmtree(path)
    path.mkdir()
    (path / CHILD_MARKER).write_text(_marker_content(path, purpose), encoding="utf-8")


def _remove_owned_child(path: Path, root: Path, purpose: str) -> None:
    _admit_owned_child(path, root, purpose)
    shutil.rmtree(path)


def _regular_files(root: Path, directory: Path) -> set[str]:
    if directory.is_symlink() or not directory.is_dir():
        raise ReleaseBuildError("source closure directory is unavailable")
    paths = set()
    for path in directory.rglob("*"):
        relative = path.relative_to(directory)
        if "__pycache__" in relative.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        if path.is_symlink():
            raise ReleaseBuildError("source closure contains a symbolic link")
        if path.is_file():
            paths.add(path.relative_to(root).as_posix())
    return paths


def _dependency_specs(manifest: dict[str, object]) -> tuple[dict[str, object], ...]:
    specs = []
    tables = []
    for name in ("dependencies", "build-dependencies"):
        value = manifest.get(name)
        if isinstance(value, dict):
            tables.append(value)
    targets = manifest.get("target")
    if isinstance(targets, dict):
        for target in targets.values():
            if isinstance(target, dict):
                for name in ("dependencies", "build-dependencies"):
                    value = target.get(name)
                    if isinstance(value, dict):
                        tables.append(value)
    for table in tables:
        for name, value in table.items():
            if isinstance(value, dict):
                specs.append({"_name": name, **value})
    return tuple(specs)


def _toml(path: Path) -> dict[str, object]:
    try:
        if path.is_symlink():
            raise ReleaseBuildError("release TOML input must not be a symbolic link")
        value = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise ReleaseBuildError("release TOML input is unreadable") from error
    if not isinstance(value, dict):
        raise ReleaseBuildError("release TOML root is invalid")
    return value


def _require_tool_release(output: str, tool: str, expected: str) -> None:
    first = output.splitlines()[0].split()
    release = first[1] if len(first) >= 2 and first[0] == tool else None
    if release != expected:
        raise ReleaseBuildError(f"{tool} must execute at exact release {expected}")


def _create_venv(
    runtime: PythonRuntime,
    path: Path,
    root: Path,
    environment: dict[str, str] | None = None,
) -> str:
    """Create a venv with its admitted runtime and re-admit the resulting interpreter."""

    _run([str(runtime.executable), "-I", "-m", "venv", str(path)], root, environment)
    executable = Path(_venv_python(path))
    _admit_created_runtime(executable, runtime.version, root, environment)
    return str(executable)


def _admit_created_runtime(
    executable: Path,
    expected: tuple[int, int, int],
    root: Path,
    environment: dict[str, str] | None = None,
) -> None:
    evidence, version = _interpreter_evidence(executable, root, environment)
    if (
        evidence.get("implementation") != "cpython"
        or evidence.get("system") != "Darwin"
        or evidence.get("machine") != "arm64"
        or version != expected
    ):
        raise ReleaseBuildError("created venv does not use its exact admitted interpreter")


def _interpreter_evidence(
    executable: Path,
    root: Path,
    environment: dict[str, str] | None = None,
) -> tuple[dict[str, object], tuple[int, ...]]:
    output = _run_output(
        [
            str(executable),
            "-I",
            "-c",
            (
                "import json,platform,sys;"
                "print(json.dumps({'implementation':sys.implementation.name,"
                "'version':list(sys.version_info[:3]),'system':platform.system(),"
                "'machine':platform.machine()},sort_keys=True))"
            ),
        ],
        root,
        environment or {"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin", "TZ": "UTC"},
    )
    try:
        evidence = json.loads(output)
        raw_version = evidence["version"]
        if not isinstance(raw_version, list) or any(not isinstance(value, int) for value in raw_version):
            raise TypeError
        version = tuple(raw_version)
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise ReleaseBuildError("Python interpreter evidence is invalid") from error
    return evidence, version


def _cargo_environment(
    root: Path,
    cargo_home: Path,
    build_home: Path,
    toolchain: dict[str, object],
    *,
    offline: bool,
) -> dict[str, str]:
    _reject_cargo_configuration((root, root / "python"), cargo_home)
    cargo = _bound_tool(toolchain, "cargo")
    rustc = _bound_tool(toolchain, "rustc")
    clang = _bound_tool(toolchain, "clang")
    clangxx = _bound_tool(toolchain, "clangxx")
    archiver = _bound_tool(toolchain, "archiver")
    ranlib = _bound_tool(toolchain, "ranlib")
    sdk = toolchain.get("sdk")
    if (
        not isinstance(sdk, dict)
        or not isinstance(sdk.get("path"), str)
        or toolchain.get("target") != "aarch64-apple-darwin"
        or toolchain.get("macos_deployment_target") != MACOS_DEPLOYMENT_TARGET
        or not isinstance(toolchain.get("developer_dir"), str)
    ):
        raise ReleaseBuildError("bound macOS build policy is invalid")
    path = os.pathsep.join(
        dict.fromkeys(
            [
                str(cargo.parent),
                str(rustc.parent),
                str(clang.parent),
                "/usr/bin",
                "/bin",
            ]
        )
    )
    temporary = build_home / "tmp"
    if not temporary.is_dir():
        temporary = Path("/tmp")
    environment = {
        "AR": str(archiver),
        "CARGO_HOME": str(cargo_home),
        "CARGO_INCREMENTAL": "0",
        "CARGO_NET_OFFLINE": "true" if offline else "false",
        "CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER": str(clang),
        "CARGO_TERM_COLOR": "never",
        "CC": str(clang),
        "CXX": str(clangxx),
        "DEVELOPER_DIR": str(toolchain["developer_dir"]),
        "HOME": str(build_home),
        "LANG": "C",
        "LC_ALL": "C",
        "MACOSX_DEPLOYMENT_TARGET": MACOS_DEPLOYMENT_TARGET,
        "PATH": path,
        "PIP_CONFIG_FILE": os.devnull,
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "PIP_NO_INDEX": "1" if offline else "0",
        "PYTHONHASHSEED": "0",
        "PYTHONNOUSERSITE": "1",
        "RANLIB": str(ranlib),
        "RUSTC": str(rustc),
        "SDKROOT": sdk["path"],
        "TMPDIR": str(temporary),
        "TZ": "UTC",
    }
    _mapping_sha256(environment)
    return environment


def _bound_tool(toolchain: dict[str, object], name: str) -> Path:
    binding = toolchain.get(name)
    if not isinstance(binding, dict):
        raise ReleaseBuildError("release tool binding is absent")
    path_value = binding.get("path")
    digest = binding.get("sha256")
    size = binding.get("size_bytes")
    if not isinstance(path_value, str) or not isinstance(digest, str) or not isinstance(size, int):
        raise ReleaseBuildError("release tool binding is malformed")
    path = Path(path_value)
    if _file_digest(path) != (size, digest):
        raise ReleaseBuildError("release tool changed after admission")
    return path


def _reject_cargo_configuration(
    cargo_working_directories: tuple[Path, ...], cargo_home: Path
) -> None:
    candidates = {cargo_home / "config", cargo_home / "config.toml"}
    for working_directory in cargo_working_directories:
        current = working_directory.resolve(strict=True)
        while True:
            candidates.update((current / ".cargo/config", current / ".cargo/config.toml"))
            if current.parent == current:
                break
            current = current.parent
    if any(path.exists() or path.is_symlink() for path in candidates):
        raise ReleaseBuildError("closed Python release build forbids Cargo configuration overrides")


def _mapping_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _compatible(filename: str, version: tuple[int, int]) -> bool:
    python_tag, _abi, platform_tag = _wheel_tags(filename)
    current = f"cp{version[0]}{version[1]}"
    python_ok = python_tag == "py3" or current in python_tag.split(".")
    platform_ok = platform_tag == "any" or (
        platform.system() == "Darwin"
        and platform.machine() == "arm64"
        and "arm64" in platform_tag
    )
    return python_ok and platform_ok


def _admit_license(path: Path, expected: str) -> None:
    try:
        with zipfile.ZipFile(path) as archive:
            names = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
            if len(names) != 1:
                raise ReleaseBuildError("wheel has no unique core metadata")
            metadata = BytesParser().parsebytes(archive.read(names[0]))
    except (OSError, zipfile.BadZipFile, KeyError) as error:
        raise ReleaseBuildError("wheel metadata is unreadable") from error
    observed = metadata.get("License-Expression") or metadata.get("License")
    if observed is None or observed.strip() != expected:
        raise ReleaseBuildError("wheel license differs from the locked expression")


def _wheel_tags(filename: str) -> tuple[str, str, str]:
    if (
        not isinstance(filename, str)
        or not filename.endswith(".whl")
        or "/" in filename
        or "\\" in filename
    ):
        raise ReleaseBuildError("wheel filename is invalid")
    parts = filename[:-4].rsplit("-", 3)
    if len(parts) != 4 or any(not part for part in parts):
        raise ReleaseBuildError("wheel compatibility tags are invalid")
    return parts[1], parts[2], parts[3]


def _version(value: object) -> tuple[int, int]:
    if not isinstance(value, str) or len(value.split(".")) != 2:
        raise ReleaseBuildError("Python version bound is invalid")
    try:
        major, minor = (int(part) for part in value.split("."))
    except ValueError as error:
        raise ReleaseBuildError("Python version bound is invalid") from error
    return major, minor


def _sha256(value: object) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ReleaseBuildError("SHA-256 identity is invalid")


def _file_digest(path: Path) -> tuple[int, str]:
    content = path.read_bytes()
    return len(content), hashlib.sha256(content).hexdigest()


def _venv_python(root: Path) -> str:
    return str(root / ("Scripts/python.exe" if os.name == "nt" else "bin/python"))


def _run(command: list[str], root: Path, environment: dict[str, str] | None = None) -> None:
    try:
        completed = subprocess.run(command, cwd=root, env=environment, check=False)
    except OSError as error:
        raise ReleaseBuildError("offline Python release subprocess could not start") from error
    if completed.returncode != 0:
        raise ReleaseBuildError("offline Python release subprocess failed")


def _run_output(
    command: list[str],
    root: Path,
    environment: dict[str, str] | None = None,
) -> str:
    try:
        completed = subprocess.run(
            command,
            cwd=root,
            env=environment,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except OSError as error:
        raise ReleaseBuildError("release tool evidence command could not start") from error
    if completed.returncode != 0 or not completed.stdout or len(completed.stdout) > 16_384:
        raise ReleaseBuildError("release tool evidence command failed")
    return completed.stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", required=True, type=Path)
    parser.add_argument("--artifact-root", required=True, type=Path)
    parser.add_argument("--python", required=True, action="append", type=Path)
    parser.add_argument("--source-cache", type=Path)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--prepare-cache-only", action="store_true")
    options = parser.parse_args()
    try:
        root = Path(__file__).resolve().parents[1]
        lock_path = options.lock.expanduser().resolve(strict=True)
        source_cache = (
            options.source_cache.expanduser().resolve(strict=True)
            if options.source_cache is not None
            else None
        )
        lock = load_lock(lock_path)
        toolchain = admit_toolchain(root)
        runtimes = admit_runtimes(tuple(options.python), lock)
        admit_sources(lock, root)
        layout = admit_artifact_root(options.artifact_root, root)
        if options.prepare_cache_only:
            if options.offline:
                raise ReleaseBuildError("cache preparation and offline build modes are exclusive")
            prepare_wheelhouse(lock, layout.wheelhouse, source_cache)
            prepare_cargo_cache(root, layout.cargo_home, toolchain)
            for runtime in runtimes:
                admit_wheelhouse(lock, layout.wheelhouse, runtime.version[:2])
        else:
            if not options.offline:
                raise ReleaseBuildError("release build requires --offline")
            build_release(root, lock_path, lock, layout, runtimes, toolchain)
    except (OSError, ReleaseBuildError) as error:
        print(f"python release rejected: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
