#!/usr/bin/env python3
"""Build and verify the hash-locked Market Squawk Python product offline."""

from __future__ import annotations

import argparse
import base64
import copy
from contextlib import ExitStack
import csv
from dataclasses import dataclass
from email.parser import BytesParser
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import shutil
import stat
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
MAX_RUNTIME_DISTRIBUTIONS = 32
MAX_DISTRIBUTION_FILES = 8_192
MAX_DISTRIBUTION_ROOTS = 64
MAX_DISTRIBUTION_FILE_BYTES = 256 * 1024 * 1024
MAX_APPLICATION_EXECUTABLE_BYTES = 768 * 1024 * 1024
MAX_ONNX_WORKER_EXECUTABLE_BYTES = 256 * 1024 * 1024
MAX_VALIDATOR_EXECUTABLE_BYTES = 256 * 1024 * 1024
MAX_DISTRIBUTION_BYTES = 1024 * 1024 * 1024
MAX_RECORD_BYTES = 2 * 1024 * 1024
TRAINING_DRIVER_RECORD_PATH = "../../../bin/market-squawk-train"
TRAINING_DRIVER_RELEASE_PATH = Path("bin/market-squawk-train")
RUST_TOOLCHAIN = "1.97.1"
RUST_TOOLCHAIN_FULL = "1.97.1-aarch64-apple-darwin"
MACOS_DEPLOYMENT_TARGET = "12.0"
SOURCE_DATE_EPOCH = "946684800"
PROJECT_WHEEL_PLATFORM_TAG = (
    f"macosx_{MACOS_DEPLOYMENT_TARGET.replace('.', '_')}_arm64"
)
RELEASE_MANIFEST_DOMAIN = b"market-squawk-release-manifest-v1\0"
ENVIRONMENT_RECEIPT_DOMAIN = b"market-squawk-training-environment-v1\0"
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
class RuntimeRequirement:
    name: str
    version: str


@dataclass(frozen=True)
class ArtifactLayout:
    root: Path
    wheelhouse: Path
    cargo_home: Path
    build_home: Path
    build_venv: Path
    distribution: Path
    releases: tuple[tuple[tuple[int, int], Path], ...]


@dataclass(frozen=True)
class InstalledDistribution:
    name: str
    version: str
    roots: tuple[str, ...]
    record: Path
    record_sha256: str
    record_size: int
    file_count: int
    file_set_sha256: str
    native_extension: Path | None
    native_extension_sha256: str | None
    native_extension_size: int | None


@dataclass(frozen=True)
class NativeReleaseExecutables:
    application: Path
    onnx_worker: Path
    validator: Path


class ReleaseSigner:
    """Ephemeral release key mediated only by the exact Rust signing helper."""

    def __init__(
        self,
        helper: Path,
        helper_sha256: str,
        root: Path,
        environment: dict[str, str],
    ) -> None:
        self._helper = helper
        self._helper_sha256 = helper_sha256
        self._root = root
        self._environment = environment
        self._key = bytearray(os.urandom(32))

    def public_key(self) -> str:
        return self._invoke("public", b"").hex()

    def sign(self, domain: bytes, payload: bytes) -> str:
        message = domain + payload
        if not payload or len(message) > 64 * 1024:
            raise ReleaseBuildError("release signature payload exceeds its byte bound")
        return self._invoke("sign", message).hex()

    def close(self) -> None:
        for index in range(len(self._key)):
            self._key[index] = 0
        self._key.clear()

    def __enter__(self) -> ReleaseSigner:
        return self

    def __exit__(
        self,
        _exception_type: object,
        _exception: object,
        _traceback: object,
    ) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def _invoke(self, mode: str, message: bytes) -> bytes:
        if len(self._key) != 32 or _file_digest(self._helper)[1] != self._helper_sha256:
            raise ReleaseBuildError("release signing authority changed or was destroyed")
        expected_size = 32
        request_size = 32
        if mode == "sign":
            request_size += 4 + len(message)
            expected_size = 64
        request = bytearray(request_size)
        request[:32] = self._key
        if mode == "sign":
            request[32:36] = len(message).to_bytes(4, "big")
            request[36:] = message
        try:
            try:
                completed = subprocess.run(
                    [str(self._helper), mode],
                    cwd=self._root,
                    env=self._environment,
                    input=request,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=10,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired) as error:
                raise ReleaseBuildError("release signing helper could not complete") from error
        finally:
            for index in range(len(request)):
                request[index] = 0
            request.clear()
        if (
            completed.returncode != 0
            or len(completed.stdout) != expected_size
            or len(completed.stderr) > 4096
            or _file_digest(self._helper)[1] != self._helper_sha256
        ):
            raise ReleaseBuildError("release signing helper rejected the request")
        return completed.stdout


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


def locked_runtime_requirements(
    root: Path, lock: ReleaseLock
) -> tuple[RuntimeRequirement, ...]:
    """Resolve every exact Python project dependency against each supported wheel set."""

    project = _toml(root / "python/pyproject.toml").get("project")
    dependencies = project.get("dependencies") if isinstance(project, dict) else None
    if (
        not isinstance(dependencies, list)
        or not dependencies
        or len(dependencies) > MAX_RUNTIME_DISTRIBUTIONS
    ):
        raise ReleaseBuildError("Python runtime dependency policy is invalid")
    requirements = []
    names = set()
    for dependency in dependencies:
        if not isinstance(dependency, str):
            raise ReleaseBuildError("Python runtime dependency is not exact")
        match = re.fullmatch(r"([A-Za-z0-9][A-Za-z0-9._-]*)==([A-Za-z0-9][A-Za-z0-9._+-]*)", dependency)
        if match is None:
            raise ReleaseBuildError("Python runtime dependency is not exact")
        name = _normalize_project_name(match.group(1))
        version = match.group(2)
        if name in names:
            raise ReleaseBuildError("Python runtime dependency is duplicated")
        for python_version in SUPPORTED_PYTHONS:
            candidates = [
                artifact
                for artifact in lock.artifacts
                if _normalize_project_name(artifact.project) == name
                and artifact.version == version
                and _compatible(artifact.filename, python_version)
            ]
            if len(candidates) != 1:
                raise ReleaseBuildError(
                    "Python runtime dependency has no unique locked wheel"
                )
        requirements.append(RuntimeRequirement(name, version))
        names.add(name)
    return tuple(sorted(requirements, key=lambda value: value.name))


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
        root / "apps/market-squawk/Cargo.toml",
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
        if build_script.is_symlink():
            raise ReleaseBuildError(
                "local Rust package input must not be a symbolic link"
            )
        if build_script.exists():
            if not build_script.is_file():
                raise ReleaseBuildError("local Rust package input is invalid")
            try:
                package_inputs = tuple(package_root.glob("*.rs"))
            except OSError as error:
                raise ReleaseBuildError(
                    "local Rust package inputs are unavailable"
                ) from error
        else:
            package_inputs = ()
        for package_path in package_inputs:
            if package_path.is_symlink():
                raise ReleaseBuildError(
                    "local Rust package input must not be a symbolic link"
                )
            if not package_path.is_file():
                continue
            try:
                canonical_package_path = package_path.resolve(strict=True)
            except OSError as error:
                raise ReleaseBuildError(
                    "local Rust package input is unavailable"
                ) from error
            if (
                root not in canonical_package_path.parents
                or not canonical_package_path.is_file()
            ):
                raise ReleaseBuildError("local Rust package input is invalid")
            paths.add(canonical_package_path.relative_to(root).as_posix())
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
    paths.update(_literal_rust_include_paths(root, paths))
    return tuple(sorted(paths))


def _literal_rust_include_paths(root: Path, source_paths: set[str]) -> set[str]:
    """Resolve repository-confined literal Rust include inputs, recursively for include!."""

    root = root.resolve(strict=True)
    pending = [
        root / relative for relative in source_paths if Path(relative).suffix == ".rs"
    ]
    visited: set[Path] = set()
    included: set[str] = set()
    while pending:
        source = pending.pop()
        try:
            canonical_source = source.resolve(strict=True)
        except OSError as error:
            raise ReleaseBuildError("Rust source include owner is unavailable") from error
        if (
            canonical_source in visited
            or root not in canonical_source.parents
            or source.is_symlink()
            or not canonical_source.is_file()
        ):
            if canonical_source in visited:
                continue
            raise ReleaseBuildError("Rust source include owner is invalid")
        visited.add(canonical_source)
        try:
            text = canonical_source.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise ReleaseBuildError("Rust source include owner is unreadable") from error
        for macro, literal in _rust_literal_includes(text):
            if not literal or Path(literal).is_absolute() or "\x00" in literal:
                raise ReleaseBuildError("Rust include path is invalid")
            lexical = Path(os.path.abspath(canonical_source.parent / literal))
            if root not in lexical.parents:
                raise ReleaseBuildError("Rust include path escapes the repository")
            current = root
            for component in lexical.relative_to(root).parts:
                current /= component
                if current.is_symlink():
                    raise ReleaseBuildError("Rust include path contains a symbolic link")
            try:
                target = lexical.resolve(strict=True)
            except OSError as error:
                raise ReleaseBuildError("Rust include input is unavailable") from error
            if root not in target.parents or not target.is_file():
                raise ReleaseBuildError("Rust include input is invalid")
            relative = target.relative_to(root).as_posix()
            included.add(relative)
            if macro == "include" and target.suffix == ".rs" and target not in visited:
                pending.append(target)
    return included


def _rust_literal_includes(source: str) -> tuple[tuple[str, str], ...]:
    """Return include macro names and literal paths, rejecting non-literal invocations."""

    includes = []
    offset = 0
    while offset < len(source):
        if source.startswith("//", offset):
            newline = source.find("\n", offset + 2)
            offset = len(source) if newline < 0 else newline + 1
            continue
        if source.startswith("/*", offset):
            offset = _rust_block_comment_end(source, offset)
            continue
        character = _rust_char_literal_end(source, offset)
        if character is not None:
            offset = character
            continue
        raw = _rust_raw_string(source, offset)
        if raw is not None:
            _value, offset = raw
            continue
        if source[offset] == '"':
            offset = _rust_quoted_string_end(source, offset)
            continue
        if source[offset].isalpha() or source[offset] == "_":
            end = offset + 1
            while end < len(source) and (
                source[end].isalnum() or source[end] == "_"
            ):
                end += 1
            macro = source[offset:end]
            if macro in {"include", "include_bytes", "include_str"}:
                invocation = _rust_include_invocation(source, end)
                if invocation is not None:
                    literal, offset = invocation
                    includes.append((macro, literal))
                    continue
            offset = end
            continue
        offset += 1
    return tuple(includes)


def _rust_include_invocation(source: str, offset: int) -> tuple[str, int] | None:
    offset = _skip_rust_trivia(source, offset)
    if offset >= len(source) or source[offset] != "!":
        return None
    offset = _skip_rust_trivia(source, offset + 1)
    delimiters = {"(": ")", "[": "]", "{": "}"}
    if offset >= len(source) or source[offset] not in delimiters:
        raise ReleaseBuildError("Rust include invocation is unresolved")
    closing = delimiters[source[offset]]
    offset = _skip_rust_trivia(source, offset + 1)
    raw = _rust_raw_string(source, offset)
    if raw is not None:
        literal, offset = raw
    elif offset < len(source) and source[offset] == '"':
        literal, offset = _rust_include_quoted_literal(source, offset)
    else:
        raise ReleaseBuildError("Rust include path is not a literal")
    offset = _skip_rust_trivia(source, offset)
    if offset < len(source) and source[offset] == ",":
        offset = _skip_rust_trivia(source, offset + 1)
    if offset >= len(source) or source[offset] != closing:
        raise ReleaseBuildError("Rust include invocation is not a lone literal")
    return literal, offset + 1


def _rust_raw_string(source: str, offset: int) -> tuple[str, int] | None:
    start = offset
    if source.startswith("br", offset):
        offset += 2
    elif offset < len(source) and source[offset] == "r":
        offset += 1
    else:
        return None
    hashes = 0
    while offset < len(source) and source[offset] == "#":
        hashes += 1
        offset += 1
    if offset >= len(source) or source[offset] != '"':
        return None
    content_start = offset + 1
    terminator = '"' + ("#" * hashes)
    content_end = source.find(terminator, content_start)
    if content_end < 0:
        raise ReleaseBuildError("Rust raw string literal is unterminated")
    if source.startswith("br", start):
        return "", content_end + len(terminator)
    return source[content_start:content_end], content_end + len(terminator)


def _rust_char_literal_end(source: str, offset: int) -> int | None:
    if offset >= len(source) or source[offset] != "'" or offset + 2 >= len(source):
        return None
    end = offset + 1
    if source[end] == "\\":
        end += 1
        if end >= len(source):
            return None
        if source[end] == "u" and end + 1 < len(source) and source[end + 1] == "{":
            end = source.find("}", end + 2)
            if end < 0:
                return None
            end += 1
        else:
            end += 1
    else:
        end += 1
    return end + 1 if end < len(source) and source[end] == "'" else None


def _rust_include_quoted_literal(source: str, offset: int) -> tuple[str, int]:
    end = offset + 1
    while end < len(source):
        if source[end] == '"':
            return source[offset + 1 : end], end + 1
        if source[end] == "\\" or source[end] in "\r\n":
            raise ReleaseBuildError("Rust include path must be an unescaped literal")
        end += 1
    raise ReleaseBuildError("Rust include path literal is unterminated")


def _rust_quoted_string_end(source: str, offset: int) -> int:
    offset += 1
    while offset < len(source):
        if source[offset] == '"':
            return offset + 1
        if source[offset] == "\\":
            offset += 2
        else:
            offset += 1
    raise ReleaseBuildError("Rust string literal is unterminated")


def _rust_block_comment_end(source: str, offset: int) -> int:
    depth = 1
    offset += 2
    while offset < len(source) and depth:
        if source.startswith("/*", offset):
            depth += 1
            offset += 2
        elif source.startswith("*/", offset):
            depth -= 1
            offset += 2
        else:
            offset += 1
    if depth:
        raise ReleaseBuildError("Rust block comment is unterminated")
    return offset


def _skip_rust_trivia(source: str, offset: int) -> int:
    while offset < len(source):
        if source[offset].isspace():
            offset += 1
            continue
        if source.startswith("//", offset):
            newline = source.find("\n", offset + 2)
            offset = len(source) if newline < 0 else newline + 1
            continue
        if source.startswith("/*", offset):
            offset = _rust_block_comment_end(source, offset)
            continue
        break
    return offset


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
    with ExitStack() as cleanup:
        _build_release(root, lock_path, lock, layout, runtimes, toolchain, cleanup)


def _build_release(
    root: Path,
    lock_path: Path,
    lock: ReleaseLock,
    layout: ArtifactLayout,
    runtimes: tuple[PythonRuntime, ...],
    toolchain: dict[str, object],
    cleanup: ExitStack,
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
    _run(
        [
            str(_bound_tool(toolchain, "cargo")),
            "build",
            "-p",
            "market-squawk-modeling",
            "--bin",
            "market-squawk-release-signer",
            "--release",
            "--locked",
        ],
        root,
        bootstrap_environment,
    )
    signer_helper = root / "target/release/market-squawk-release-signer"
    if not signer_helper.is_file():
        raise ReleaseBuildError("Rust release signing helper was not produced")
    signer_helper_sha256 = _file_digest(signer_helper)[1]
    signer = cleanup.enter_context(
        ReleaseSigner(
            signer_helper,
            signer_helper_sha256,
            root,
            bootstrap_environment,
        )
    )
    release_public_key = signer.public_key()
    build_runtime = runtimes[0]
    runtime_requirements = locked_runtime_requirements(root, lock)
    foundation, foundation_sha256 = build_training_foundation_receipt(
        root,
        root / "python/requirements.lock",
        lock_path,
        lock,
        build_runtime,
        toolchain,
        runtime_requirements,
        release_public_key,
        signer_helper_sha256,
    )
    training_code_revision = json.loads(foundation)["training_code_revision"]
    if not isinstance(training_code_revision, str):
        raise ReleaseBuildError("training foundation revision is invalid")
    (layout.root / "training-foundation.json").write_bytes(foundation)
    bootstrap_environment["MARKET_SQUAWK_TRAINING_FOUNDATION_RECEIPT"] = foundation.decode(
        "ascii"
    )
    _run(
        [
            str(_bound_tool(toolchain, "cargo")),
            "build",
            "-p",
            "market-squawk",
            "--bin",
            "market-squawk",
            "-p",
            "market-squawk-modeling",
            "--bin",
            "market-squawk-model-validator",
            "--bin",
            "market-squawk-onnx-worker",
            "--release",
            "--locked",
        ],
        root,
        bootstrap_environment,
    )
    validator = root / "target/release/market-squawk-model-validator"
    application = root / "target/release/market-squawk"
    onnx_worker = root / "target/release/market-squawk-onnx-worker"
    executables = NativeReleaseExecutables(
        application=application,
        onnx_worker=onnx_worker,
        validator=validator,
    )
    if any(
        path.is_symlink() or not path.is_file()
        for path in (
            executables.application,
            executables.onnx_worker,
            executables.validator,
        )
    ):
        raise ReleaseBuildError("bound Rust release executables were not produced")
    validator_size, validator_sha256 = _file_digest(validator)
    application_size, application_sha256 = _file_digest(application)
    onnx_worker_size, onnx_worker_sha256 = _file_digest(onnx_worker)
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
    environment["MARKET_SQUAWK_MODEL_VALIDATOR_SHA256"] = validator_sha256
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
    harden_project_wheel(project_wheel)
    python_tag, abi_tag, platform_tag = _wheel_tags(project_wheel.name)
    if (
        python_tag != "cp310"
        or abi_tag != "abi3"
        or platform_tag != PROJECT_WHEEL_PLATFORM_TAG
    ):
        raise ReleaseBuildError(
            "project wheel does not carry the exact pinned cp310-abi3 macOS platform tag"
        )
    release_manifest, release_manifest_sha256 = build_release_manifest(
        foundation_sha256,
        project_wheel,
        python_tag,
        abi_tag,
        platform_tag,
        executables,
        signer,
    )
    (layout.root / "market-squawk-release.json").write_bytes(release_manifest)
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
        for executable, expected_identity in (
            (application, (application_size, application_sha256)),
            (onnx_worker, (onnx_worker_size, onnx_worker_sha256)),
            (validator, (validator_size, validator_sha256)),
        ):
            destination = release_venv / "bin" / executable.name
            shutil.copy2(executable, destination)
            destination.chmod(0o755)
            if _file_digest(destination) != expected_identity:
                raise ReleaseBuildError("installed native release identity changed")
        distribution = inspect_installed_distribution(
            release_venv,
            runtime,
            RuntimeRequirement("market-squawk", "0.1.0"),
            native_prefix="market_squawk/__init__.",
            require_training_driver=True,
        )
        runtime_distributions = tuple(
            inspect_installed_distribution(release_venv, runtime, requirement)
            for requirement in runtime_requirements
        )
        _require_disjoint_distribution_roots((distribution, *runtime_distributions))
        environment_sha256 = install_training_environment(
            release_venv,
            release_python,
            runtime,
            foundation_sha256,
            training_code_revision,
            release_manifest,
            release_manifest_sha256,
            project_wheel,
            validator_sha256,
            distribution,
            runtime_distributions,
            signer,
        )
        _run(
            [
                release_python,
                "-I",
                "-c",
                "import market_squawk as native;"
                "raise SystemExit(0 if getattr(native, "
                "'__market_squawk_build_identity__', None) == 'sealed-release-v1' else 2)",
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
                "training_environment_sha256": environment_sha256,
                "runtime_distributions": [
                    {
                        "name": value.name,
                        "version": value.version,
                        "file_set_sha256": value.file_set_sha256,
                    }
                    for value in runtime_distributions
                ],
                "validator_sha256": validator_sha256,
                "application_sha256": application_sha256,
                "onnx_worker_sha256": onnx_worker_sha256,
            }
        )
    evidence = {
        "schema_version": 5,
        "support_matrix": matrix_evidence,
        "toolchain": toolchain,
        "build_environment": {
            "sha256": _mapping_sha256(environment),
            "values": environment,
        },
        "cargo_lock_sha256": _file_digest(root / "Cargo.lock")[1],
        "wheelhouse_lock_sha256": _file_digest(lock_path)[1],
        "requirements_lock_sha256": _file_digest(root / "python/requirements.lock")[1],
        "training_foundation": {
            "path": "training-foundation.json",
            "sha256": foundation_sha256,
        },
        "release_manifest": {
            "path": "market-squawk-release.json",
            "sha256": release_manifest_sha256,
        },
        "source_closure_count": len(lock.sources),
        "project_wheel": {
            "filename": project_wheel.name,
            "sha256": _file_digest(project_wheel)[1],
            "python_tag": python_tag,
            "abi_tag": abi_tag,
            "platform_tag": platform_tag,
            "macos_deployment_target": MACOS_DEPLOYMENT_TARGET,
        },
        "native_executables": {
            "application_sha256": application_sha256,
            "onnx_worker_sha256": onnx_worker_sha256,
            "validator_sha256": validator_sha256,
        },
    }
    (layout.root / "market-squawk-release-evidence.json").write_text(
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
    if purpose in {"release-cp312", "release-cp313"}:
        _unseal_owned_release_authority(path)
    shutil.rmtree(path)
    path.mkdir()
    (path / CHILD_MARKER).write_text(_marker_content(path, purpose), encoding="utf-8")


def _unseal_owned_release_authority(path: Path) -> None:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    descriptors = []
    try:
        descriptors.append(os.open(path, flags))
        try:
            descriptors.append(os.open("share", flags, dir_fd=descriptors[-1]))
        except FileNotFoundError:
            return
        try:
            descriptors.append(os.open("market-squawk", flags, dir_fd=descriptors[-1]))
        except FileNotFoundError:
            return
        os.fchmod(descriptors[-1], 0o700)
    except OSError as error:
        raise ReleaseBuildError("sealed release authority is invalid") from error
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


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
        "SOURCE_DATE_EPOCH": SOURCE_DATE_EPOCH,
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


def build_training_foundation_receipt(
    root: Path,
    requirements_lock: Path,
    wheelhouse_lock: Path,
    lock: ReleaseLock,
    runtime: PythonRuntime,
    toolchain: dict[str, object],
    runtime_requirements: tuple[RuntimeRequirement, ...],
    release_public_key: str,
    release_signer_sha256: str,
) -> tuple[bytes, str]:
    """Return the canonical build foundation embedded in both native executables."""

    _sha256(release_public_key)
    _sha256(release_signer_sha256)
    if (
        not runtime_requirements
        or len(runtime_requirements) > MAX_RUNTIME_DISTRIBUTIONS
        or tuple(sorted(runtime_requirements, key=lambda value: value.name))
        != runtime_requirements
        or len({value.name for value in runtime_requirements})
        != len(runtime_requirements)
    ):
        raise ReleaseBuildError("Python runtime dependency policy is invalid")
    source_closure = [
        {"path": source.path, "sha256": source.sha256, "size_bytes": source.size_bytes}
        for source in sorted(lock.sources, key=lambda value: value.path)
    ]
    source_closure_sha256 = _mapping_sha256(source_closure)
    receipt = {
        "build_python_sha256": _file_digest(runtime.executable)[1],
        "build_python_version": ".".join(str(value) for value in runtime.version),
        "cargo_lock_sha256": _file_digest(root / "Cargo.lock")[1],
        "requirements_lock_sha256": _file_digest(requirements_lock)[1],
        "release_public_key": release_public_key,
        "release_signer_sha256": release_signer_sha256,
        "runtime_distributions": [
            {"name": value.name, "version": value.version}
            for value in runtime_requirements
        ],
        "schema_version": 1,
        "source_closure_sha256": source_closure_sha256,
        "toolchain_sha256": _mapping_sha256(toolchain),
        "training_code_revision": source_closure_sha256,
        "wheelhouse_lock_sha256": _file_digest(wheelhouse_lock)[1],
    }
    encoded = json.dumps(
        receipt,
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return encoded, hashlib.sha256(encoded).hexdigest()


def build_release_manifest(
    foundation_sha256: str,
    project_wheel: Path,
    python_tag: str,
    abi_tag: str,
    platform_tag: str,
    executables: NativeReleaseExecutables,
    signer: ReleaseSigner,
) -> tuple[bytes, str]:
    """Bind the exact wheel and native product without a self-referential digest."""

    _sha256(foundation_sha256)
    wheel_size, wheel_sha256 = _file_digest(project_wheel)
    expected_names = {
        "application": "market-squawk",
        "onnx_worker": "market-squawk-onnx-worker",
        "validator": "market-squawk-model-validator",
    }
    native_files = {
        name: getattr(executables, name) for name in expected_names
    }
    if any(
        path.name != expected_names[name] or path.is_symlink() or not path.is_file()
        for name, path in native_files.items()
    ):
        raise ReleaseBuildError("native release executable identity is invalid")
    native_limits = {
        "application": MAX_APPLICATION_EXECUTABLE_BYTES,
        "onnx_worker": MAX_ONNX_WORKER_EXECUTABLE_BYTES,
        "validator": MAX_VALIDATOR_EXECUTABLE_BYTES,
    }
    try:
        native_sizes = {name: path.stat().st_size for name, path in native_files.items()}
    except OSError as error:
        raise ReleaseBuildError("native release executable identity is invalid") from error
    if any(
        size <= 0 or size > native_limits[name]
        for name, size in native_sizes.items()
    ):
        raise ReleaseBuildError("native release executable exceeds its byte bound")
    native_identities = {
        name: _file_digest(path) for name, path in native_files.items()
    }
    if any(
        size != native_sizes[name] or size > native_limits[name]
        for name, (size, _digest) in native_identities.items()
    ):
        raise ReleaseBuildError("native release executable exceeds its byte bound")
    payload = {
        "application": {
            "sha256": native_identities["application"][1],
            "size_bytes": native_identities["application"][0],
        },
        "foundation_sha256": foundation_sha256,
        "onnx_worker": {
            "sha256": native_identities["onnx_worker"][1],
            "size_bytes": native_identities["onnx_worker"][0],
        },
        "project_wheel": {
            "abi_tag": abi_tag,
            "filename": project_wheel.name,
            "macos_deployment_target": MACOS_DEPLOYMENT_TARGET,
            "platform_tag": platform_tag,
            "python_tag": python_tag,
            "sha256": wheel_sha256,
            "size_bytes": wheel_size,
        },
        "schema_version": 2,
        "validator": {
            "sha256": native_identities["validator"][1],
            "size_bytes": native_identities["validator"][0],
        },
    }
    payload_bytes = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    manifest = {
        "payload": payload,
        "schema_version": 2,
        "signature": signer.sign(RELEASE_MANIFEST_DOMAIN, payload_bytes),
    }
    encoded = json.dumps(
        manifest,
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return encoded, hashlib.sha256(encoded).hexdigest()


def harden_project_wheel(project_wheel: Path) -> None:
    """Replace Maturin's Python bootstrap with the independently loaded native initializer."""

    if (
        project_wheel.is_symlink()
        or not project_wheel.is_file()
        or project_wheel.stat().st_size > MAX_DISTRIBUTION_BYTES
    ):
        raise ReleaseBuildError("project wheel is not a bounded regular file")
    mutable_bootstrap = "market_squawk/__init__.py"
    nested_native_prefix = "market_squawk/market_squawk."
    temporary = project_wheel.with_name(f".{project_wheel.name}.native-bootstrap")
    if temporary.exists() or temporary.is_symlink():
        raise ReleaseBuildError("project wheel hardening output already exists")
    wheel_mode = stat.S_IMODE(project_wheel.stat().st_mode)
    entries: dict[str, tuple[str, int]] = {}
    try:
        with zipfile.ZipFile(project_wheel, "r") as source:
            members = source.infolist()
            if not members or len(members) > MAX_DISTRIBUTION_FILES:
                raise ReleaseBuildError("project wheel file count exceeds its bound")
            names = [member.filename for member in members]
            if len(names) != len(set(names)):
                raise ReleaseBuildError("project wheel contains duplicate paths")
            records = [
                member
                for member in members
                if member.filename.endswith(".dist-info/RECORD")
            ]
            nested_native = [
                member
                for member in members
                if member.filename.startswith(nested_native_prefix)
                and member.filename.endswith(".so")
            ]
            if (
                mutable_bootstrap not in names
                or len(records) != 1
                or len(nested_native) != 1
                or any(
                    name.startswith("market_squawk/__init__.")
                    and name != mutable_bootstrap
                    for name in names
                )
            ):
                raise ReleaseBuildError("project wheel bootstrap layout is invalid")
            native_suffix = nested_native[0].filename.removeprefix(nested_native_prefix)
            if native_suffix != "abi3.so":
                raise ReleaseBuildError("project wheel native initializer ABI is invalid")
            native_initializer = f"market_squawk/__init__.{native_suffix}"
            if native_initializer in names:
                raise ReleaseBuildError("project wheel native initializer is duplicated")

            with zipfile.ZipFile(temporary, "x", allowZip64=True) as destination:
                total_size = 0
                for member in members:
                    name = member.filename
                    path = _safe_record_path(name)
                    mode = member.external_attr >> 16
                    if (
                        path.as_posix() != name
                        or "\0" in name
                        or "__pycache__" in path.parts
                        or path.suffix in {".pyc", ".pyo"}
                        or member.is_dir()
                        or member.flag_bits & 0x1
                        or stat.S_IFMT(mode) not in (0, stat.S_IFREG)
                        or member.file_size < 0
                        or member.file_size > MAX_DISTRIBUTION_FILE_BYTES
                    ):
                        raise ReleaseBuildError("project wheel contains an invalid file")
                    if member == records[0] or name == mutable_bootstrap:
                        continue
                    output_name = (
                        native_initializer if member == nested_native[0] else name
                    )
                    output_info = copy.copy(member)
                    output_info.filename = output_name
                    output_info.orig_filename = output_name
                    digest = hashlib.sha256()
                    copied = 0
                    with source.open(member, "r") as reader, destination.open(
                        output_info, "w", force_zip64=True
                    ) as writer:
                        while True:
                            chunk = reader.read(1024 * 1024)
                            if not chunk:
                                break
                            copied += len(chunk)
                            total_size += len(chunk)
                            if total_size > MAX_DISTRIBUTION_BYTES:
                                raise ReleaseBuildError(
                                    "project wheel expanded size exceeds its bound"
                                )
                            digest.update(chunk)
                            writer.write(chunk)
                    if copied != member.file_size or output_name in entries:
                        raise ReleaseBuildError("project wheel file changed during hardening")
                    entries[output_name] = (digest.hexdigest(), copied)

                rendered = io.StringIO(newline="")
                record_writer = csv.writer(rendered, lineterminator="\n")
                for name, (digest, size) in sorted(entries.items()):
                    encoded = base64.urlsafe_b64encode(bytes.fromhex(digest)).rstrip(b"=")
                    record_writer.writerow(
                        [name, f"sha256={encoded.decode('ascii')}", str(size)]
                    )
                record_writer.writerow([records[0].filename, "", ""])
                record_bytes = rendered.getvalue().encode("utf-8")
                if len(record_bytes) > MAX_RECORD_BYTES:
                    raise ReleaseBuildError("project wheel RECORD exceeds its bound")
                record_info = copy.copy(records[0])
                destination.writestr(record_info, record_bytes)
        temporary.chmod(wheel_mode)
        os.replace(temporary, project_wheel)
    except ReleaseBuildError:
        raise
    except (
        OSError,
        EOFError,
        KeyError,
        RuntimeError,
        UnicodeError,
        ValueError,
        zipfile.BadZipFile,
        csv.Error,
    ) as error:
        raise ReleaseBuildError("project wheel hardening failed") from error
    finally:
        temporary.unlink(missing_ok=True)


def inspect_installed_distribution(
    release_root: Path,
    runtime: PythonRuntime,
    requirement: RuntimeRequirement,
    *,
    native_prefix: str | None = None,
    require_training_driver: bool = False,
) -> InstalledDistribution:
    """Verify one installed distribution and its complete owned file roots."""

    site_packages = (
        release_root
        / "lib"
        / f"python{runtime.version[0]}.{runtime.version[1]}"
        / "site-packages"
    )
    if site_packages.is_symlink() or not site_packages.is_dir():
        raise ReleaseBuildError("installed distribution root is invalid")
    candidates = []
    for metadata_path in site_packages.glob("*.dist-info/METADATA"):
        if metadata_path.is_symlink() or not metadata_path.is_file():
            continue
        try:
            if metadata_path.stat().st_size > 256 * 1024:
                continue
            metadata = BytesParser().parsebytes(metadata_path.read_bytes())
            names = metadata.get_all("Name", [])
            versions = metadata.get_all("Version", [])
            if (
                len(names) == 1
                and len(versions) == 1
                and _normalize_project_name(names[0]) == requirement.name
                and versions[0] == requirement.version
            ):
                candidates.append(metadata_path.parent)
        except (OSError, UnicodeError):
            continue
    if len(candidates) != 1:
        raise ReleaseBuildError("installed distribution identity is not unique")
    record = candidates[0] / "RECORD"
    if record.is_symlink() or not record.is_file() or record.stat().st_size > MAX_RECORD_BYTES:
        raise ReleaseBuildError("installed distribution has no bounded wheel RECORD")
    record_name = record.relative_to(site_packages).as_posix()
    entries: dict[str, tuple[str, int]] = {}
    hashed_entries: set[str] = set()
    checked_hash_bytecode: list[tuple[str, str]] = []
    saw_record = False
    saw_training_driver = False
    total_size = 0
    try:
        with record.open("r", encoding="utf-8", newline="") as stream:
            for row in csv.reader(stream):
                if len(row) != 3 or len(entries) >= MAX_DISTRIBUTION_FILES:
                    raise ReleaseBuildError("installed distribution RECORD exceeds its bounds")
                name, encoded_digest, encoded_size = row
                if name == record_name:
                    if saw_record or encoded_digest or encoded_size:
                        raise ReleaseBuildError(
                            "installed distribution RECORD self-entry is invalid"
                        )
                    saw_record = True
                    continue
                is_training_driver = (
                    require_training_driver and name == TRAINING_DRIVER_RECORD_PATH
                )
                if is_training_driver:
                    if saw_training_driver or not encoded_digest or not encoded_size:
                        raise ReleaseBuildError(
                            "installed training driver RECORD entry is invalid"
                        )
                    path = release_root / TRAINING_DRIVER_RELEASE_PATH
                    observed_record_path = os.path.relpath(path, site_packages).replace(
                        os.sep, "/"
                    )
                    if (
                        observed_record_path != TRAINING_DRIVER_RECORD_PATH
                        or path.parent.is_symlink()
                    ):
                        raise ReleaseBuildError(
                            "installed training driver path is invalid"
                        )
                    saw_training_driver = True
                else:
                    relative = _safe_record_path(name)
                    path = site_packages / relative
                if path.is_symlink() or not path.is_file():
                    raise ReleaseBuildError("installed distribution file is unavailable")
                observed_size, observed_sha256, header = (
                    _inspect_installed_distribution_file(path)
                )
                if not encoded_digest and not encoded_size:
                    source_name = _checked_hash_bytecode_source(name, runtime)
                    if (
                        source_name is None
                        or len(header) != 16
                        or int.from_bytes(header[4:8], "little") != 0b11
                    ):
                        raise ReleaseBuildError(
                            "installed distribution has an unsafe blank RECORD identity"
                        )
                    checked_hash_bytecode.append((name, source_name))
                    size = observed_size
                else:
                    if _is_python_bytecode(name):
                        raise ReleaseBuildError(
                            "installed distribution contains wheel-authored bytecode"
                        )
                    if not encoded_digest.startswith("sha256="):
                        raise ReleaseBuildError(
                            "installed distribution RECORD hash is unsupported"
                        )
                    try:
                        size = int(encoded_size)
                    except ValueError as error:
                        raise ReleaseBuildError(
                            "installed distribution RECORD size is invalid"
                        ) from error
                    expected = base64.urlsafe_b64encode(
                        bytes.fromhex(observed_sha256)
                    ).rstrip(b"=")
                    if (
                        observed_size != size
                        or encoded_digest.removeprefix("sha256=").encode("ascii")
                        != expected
                    ):
                        raise ReleaseBuildError(
                            "installed distribution RECORD identity mismatch"
                        )
                    hashed_entries.add(name)
                if size < 0 or size > MAX_DISTRIBUTION_FILE_BYTES:
                    raise ReleaseBuildError(
                        "installed distribution file exceeds its byte bound"
                    )
                total_size += size
                if total_size > MAX_DISTRIBUTION_BYTES or name in entries:
                    raise ReleaseBuildError(
                        "installed distribution RECORD is duplicated or oversized"
                    )
                entries[name] = (observed_sha256, size)
    except (OSError, UnicodeError, csv.Error) as error:
        raise ReleaseBuildError("installed distribution RECORD is unreadable") from error
    if (
        not saw_record
        or not entries
        or require_training_driver != saw_training_driver
        or any(
            source_name not in hashed_entries
            for _bytecode_name, source_name in checked_hash_bytecode
        )
    ):
        raise ReleaseBuildError("installed distribution RECORD is incomplete")
    internal_entries = {
        name for name in entries if name != TRAINING_DRIVER_RECORD_PATH
    }
    roots = tuple(
        sorted(
            {Path(name).parts[0] for name in internal_entries}
            | {Path(record_name).parts[0]}
        )
    )
    if not roots or len(roots) > MAX_DISTRIBUTION_ROOTS:
        raise ReleaseBuildError("installed distribution roots exceed their bound")
    expected_paths = internal_entries
    expected_paths.add(record_name)
    if _installed_paths_under_roots(site_packages, roots) != expected_paths:
        raise ReleaseBuildError("installed distribution contains an unrecorded file")
    native = None
    native_sha256 = None
    native_size = None
    if native_prefix is not None:
        native_entries = [
            name
            for name in entries
            if name.startswith(native_prefix) and name.endswith(".so")
        ]
        if len(native_entries) != 1:
            raise ReleaseBuildError("installed project has no unique native extension")
        native_name = native_entries[0]
        native = site_packages / _safe_record_path(native_name)
        native_sha256, native_size = entries[native_name]
    record_size, record_sha256 = _file_digest(record)
    return InstalledDistribution(
        name=requirement.name,
        version=requirement.version,
        roots=roots,
        record=record.relative_to(release_root),
        record_sha256=record_sha256,
        record_size=record_size,
        file_count=len(entries),
        file_set_sha256=_record_set_sha256(entries),
        native_extension=native.relative_to(release_root) if native is not None else None,
        native_extension_sha256=native_sha256,
        native_extension_size=native_size,
    )


def _installed_paths_under_roots(site_packages: Path, roots: tuple[str, ...]) -> set[str]:
    paths = set()
    directories = 0
    maximum_paths = MAX_DISTRIBUTION_FILES * 3 + 1
    for root_name in roots:
        root = site_packages / _safe_record_path(root_name)
        if root.is_symlink() or not root.exists():
            raise ReleaseBuildError("installed distribution root is unavailable")
        pending = [root]
        while pending:
            current = pending.pop()
            if current.is_symlink():
                raise ReleaseBuildError("installed distribution contains a symbolic link")
            if current.is_file():
                relative = current.relative_to(site_packages).as_posix()
                _safe_record_path(relative)
                paths.add(relative)
                if len(paths) > MAX_DISTRIBUTION_FILES + 1:
                    raise ReleaseBuildError("installed distribution file count is unbounded")
                continue
            if not current.is_dir():
                raise ReleaseBuildError("installed distribution contains a special file")
            directories += 1
            if directories > MAX_DISTRIBUTION_FILES * 2:
                raise ReleaseBuildError("installed distribution directory count is unbounded")
            try:
                with os.scandir(current) as stream:
                    for entry in stream:
                        if len(pending) + len(paths) + directories >= maximum_paths:
                            raise ReleaseBuildError(
                                "installed distribution path count is unbounded"
                            )
                        pending.append(Path(entry.path))
            except OSError as error:
                raise ReleaseBuildError("installed distribution root is unreadable") from error
    return paths


def _require_disjoint_distribution_roots(
    distributions: tuple[InstalledDistribution, ...],
) -> None:
    roots = set()
    for distribution in distributions:
        for root in distribution.roots:
            if root in roots:
                raise ReleaseBuildError("installed distribution roots overlap")
            roots.add(root)


def install_training_environment(
    release_root: Path,
    release_python: str,
    runtime: PythonRuntime,
    foundation_sha256: str,
    training_code_revision: str,
    release_manifest: bytes,
    release_manifest_sha256: str,
    project_wheel: Path,
    validator_sha256: str,
    distribution: InstalledDistribution,
    runtime_distributions: tuple[InstalledDistribution, ...],
    signer: ReleaseSigner,
) -> str:
    """Install one fixed-path release receipt outside the wheel's RECORD file set."""

    for digest in (
        foundation_sha256,
        training_code_revision,
        release_manifest_sha256,
        validator_sha256,
    ):
        _sha256(digest)
    interpreter = Path(release_python)
    try:
        interpreter_relative = interpreter.relative_to(release_root).as_posix()
    except ValueError as error:
        raise ReleaseBuildError("release interpreter escapes its environment") from error
    if interpreter_relative != "bin/python":
        raise ReleaseBuildError("release interpreter path is not canonical")
    if (
        distribution.native_extension is None
        or distribution.native_extension_sha256 is None
        or distribution.native_extension_size is None
    ):
        raise ReleaseBuildError("installed project native extension is absent")
    _require_disjoint_distribution_roots((distribution, *runtime_distributions))
    interpreter_size, interpreter_sha256 = _file_digest(interpreter)
    authority_parent = release_root / "share"
    if authority_parent.is_symlink() or (
        authority_parent.exists() and not authority_parent.is_dir()
    ):
        raise ReleaseBuildError("release authority parent is invalid")
    authority_parent.mkdir(exist_ok=True)
    authority = authority_parent / "market-squawk"
    if authority.exists() or authority.is_symlink():
        raise ReleaseBuildError("release training authority already exists")
    authority.mkdir(mode=0o700)
    wheel_destination = authority / project_wheel.name
    shutil.copyfile(project_wheel, wheel_destination)
    manifest_path = authority / "market-squawk-release.json"
    manifest_path.write_bytes(release_manifest)
    payload = {
        "foundation_sha256": foundation_sha256,
        "interpreter": {
            "executable_relative_path": "bin/python",
            "implementation": "cpython",
            "python_tag": f"cp{runtime.version[0]}{runtime.version[1]}",
            "sha256": interpreter_sha256,
            "size_bytes": interpreter_size,
            "version": ".".join(str(value) for value in runtime.version),
        },
        "native_extension": {
            "relative_path": distribution.native_extension.as_posix(),
            "sha256": distribution.native_extension_sha256,
            "size_bytes": distribution.native_extension_size,
        },
        "project_distribution": _distribution_payload(distribution),
        "release_manifest_sha256": release_manifest_sha256,
        "runtime_distributions": [
            _distribution_payload(value) for value in runtime_distributions
        ],
        "training_code_revision": training_code_revision,
        "validator_sha256": validator_sha256,
    }
    payload_bytes = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    receipt = {
        "payload": payload,
        "schema_version": 1,
        "signature": signer.sign(ENVIRONMENT_RECEIPT_DOMAIN, payload_bytes),
    }
    encoded = json.dumps(
        receipt,
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    receipt_path = authority / "training-environment.json"
    receipt_path.write_bytes(encoded)
    for path in (wheel_destination, manifest_path, receipt_path):
        path.chmod(0o444)
    authority.chmod(0o555)
    return hashlib.sha256(encoded).hexdigest()


def _distribution_payload(distribution: InstalledDistribution) -> dict[str, object]:
    return {
        "file_count": distribution.file_count,
        "file_set_sha256": distribution.file_set_sha256,
        "name": distribution.name,
        "record_relative_path": distribution.record.as_posix(),
        "record_sha256": distribution.record_sha256,
        "record_size_bytes": distribution.record_size,
        "roots": list(distribution.roots),
        "version": distribution.version,
    }


def _is_python_bytecode(value: str) -> bool:
    return Path(value).suffix in {".pyc", ".pyo"}


def _checked_hash_bytecode_source(
    value: str,
    runtime: PythonRuntime,
) -> str | None:
    path = _safe_record_path(value)
    suffix = f".cpython-{runtime.version[0]}{runtime.version[1]}.pyc"
    if (
        path.parent.name != "__pycache__"
        or "__pycache__" in path.parent.parent.parts
        or not path.name.endswith(suffix)
    ):
        return None
    stem = path.name.removesuffix(suffix)
    if not stem:
        return None
    return (path.parent.parent / f"{stem}.py").as_posix()


def _inspect_installed_distribution_file(path: Path) -> tuple[int, str, bytes]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ReleaseBuildError("installed distribution file is unavailable") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size < 0
            or before.st_size > MAX_DISTRIBUTION_FILE_BYTES
        ):
            raise ReleaseBuildError(
                "installed distribution file exceeds its byte bound"
            )
        digest = hashlib.sha256()
        header = bytearray()
        observed = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            observed += len(chunk)
            if observed > before.st_size:
                raise ReleaseBuildError(
                    "installed distribution file changed during inspection"
                )
            if len(header) < 16:
                header.extend(chunk[: 16 - len(header)])
            digest.update(chunk)
        after = os.fstat(descriptor)
    except OSError as error:
        raise ReleaseBuildError(
            "installed distribution file is unreadable"
        ) from error
    finally:
        os.close(descriptor)
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_size,
        before.st_mtime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_size,
        after.st_mtime_ns,
    )
    if observed != before.st_size or identity_before != identity_after:
        raise ReleaseBuildError(
            "installed distribution file changed during inspection"
        )
    return observed, digest.hexdigest(), bytes(header)


def _safe_record_path(value: str) -> Path:
    path = Path(value)
    if (
        not value
        or len(value.encode("utf-8")) > 1024
        or "\\" in value
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ReleaseBuildError("installed project RECORD path is invalid")
    return path


def _record_set_sha256(entries: dict[str, tuple[str, int]]) -> str:
    digest = hashlib.sha256(b"market-squawk-record-set-v1\0")
    for path, (sha256, size) in sorted(entries.items()):
        encoded = path.encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
        digest.update(size.to_bytes(8, "big"))
        digest.update(bytes.fromhex(sha256))
    return digest.hexdigest()


def _normalize_project_name(value: str) -> str:
    normalized = re.sub(r"[-_.]+", "-", value).lower()
    if not normalized or len(normalized) > 128:
        raise ReleaseBuildError("Python distribution name is invalid")
    return normalized


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
