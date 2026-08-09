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
from pathlib import Path, PurePosixPath, PureWindowsPath
import platform
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import zipfile


MAX_LOCK_BYTES = 2 * 1024 * 1024
MAX_ARTIFACTS = 64
MAX_SOURCES = 2_048
MAX_RUNTIME_DISTRIBUTIONS = 32
MAX_DISTRIBUTION_FILES = 16_384
MAX_DISTRIBUTION_EXTERNAL_PATHS = 256
MAX_DISTRIBUTION_ROOTS = 64
MAX_DISTRIBUTION_FILE_BYTES = 256 * 1024 * 1024
MAX_APPLICATION_EXECUTABLE_BYTES = 768 * 1024 * 1024
MAX_ONNX_WORKER_EXECUTABLE_BYTES = 256 * 1024 * 1024
MAX_VALIDATOR_EXECUTABLE_BYTES = 256 * 1024 * 1024
MAX_TRAINING_LAUNCHER_BYTES = 32 * 1024 * 1024
MAX_DISTRIBUTION_BYTES = 1024 * 1024 * 1024
MAX_RECORD_BYTES = 2 * 1024 * 1024
RUST_TOOLCHAIN = "1.97.1"
MACOS_DEPLOYMENT_TARGET = "12.0"
SOURCE_DATE_EPOCH = "946684800"
RELEASE_MANIFEST_DOMAIN = b"market-squawk-release-manifest-v1\0"
ENVIRONMENT_RECEIPT_DOMAIN = b"market-squawk-training-environment-v2\0"
SUPPORTED_PYTHONS = ((3, 14),)
REQUIRED_PYTHON = (3, 14, 6)
CANONICAL_RELEASE = "release-cp314"
PACKAGING_VERSION = "26.2"
PACKAGING_BOOTSTRAP = {
    "filename": "packaging-26.2-py3-none-any.whl",
    "project": "packaging",
    "sha256": "5fc45236b9446107ff2415ce77c807cee2862cb6fac22b8a73826d0693b0980e",
    "size_bytes": 100_195,
    "url": (
        "https://files.pythonhosted.org/packages/df/b2/"
        "87e62e8c3e2f4b32e5fe99e0b86d576da1312593b39f47d8ceef365e95ed/"
        "packaging-26.2-py3-none-any.whl"
    ),
    "version": PACKAGING_VERSION,
}
ROOT_MARKER = ".market-squawk-python-artifacts-v1"
CHILD_MARKER = ".market-squawk-owned-v1"
ROOT_PURPOSE = "market-squawk-python-release-artifacts"
COMPONENT_ROOT_MARKER = ".market-squawk-release-components-v1"
COMPONENT_ROOT_PURPOSE = "market-squawk-locked-release-components"
DEVELOPMENT_ROOT_MARKER = ".market-squawk-development-runtime-v1"
DEVELOPMENT_ROOT_PURPOSE = "market-squawk-development-model-runtime"
DEVELOPMENT_RUNTIME_RELATIVE = Path(".market-squawk/development-model-runtime")
ALLOWED_LICENSES = {
    "Apache-2.0",
    "MIT",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "MIT OR Apache-2.0",
    "Apache-2.0 OR BSD-2-Clause",
    "BSD-3-Clause AND 0BSD AND MIT AND Zlib AND CC0-1.0",
    "PSF-2.0",
}
PYTHON_LICENSE_POLICY = {
    "colorama": "BSD-3-Clause",
    "iniconfig": "MIT",
    "joblib": "BSD-3-Clause",
    "mapie": "BSD-3-Clause",
    "maturin": "MIT OR Apache-2.0",
    "ml-dtypes": "Apache-2.0",
    "narwhals": "MIT",
    "numpy": "BSD-3-Clause AND 0BSD AND MIT AND Zlib AND CC0-1.0",
    "onnx": "Apache-2.0",
    "packaging": "Apache-2.0 OR BSD-2-Clause",
    "pluggy": "MIT",
    "protobuf": "BSD-3-Clause",
    "pyarrow": "Apache-2.0",
    "pygments": "BSD-2-Clause",
    "pytest": "MIT",
    "scikit-learn": "BSD-3-Clause",
    "scipy": "BSD-3-Clause",
    "skl2onnx": "Apache-2.0",
    "threadpoolctl": "BSD-3-Clause",
    "typing-extensions": "PSF-2.0",
}
REQUIRED_PROJECTS = set(PYTHON_LICENSE_POLICY)
FOCUSED_TESTS = (
    "python/tests/test_data.py",
    "python/tests/test_finance_parity.py",
    "python/tests/test_training_bundle.py",
    "python/tests/test_visualization_examples.py",
)


class ReleaseBuildError(RuntimeError):
    """A release authority, source, wheel, runtime, or build contract failed."""


def bootstrap_locked_packaging(
    lock_path: Path,
    wheelhouse: Path,
    source_cache: Path | None,
    *,
    allow_network: bool,
) -> None:
    """Load the exact hash-locked build-time packaging wheel without host mutation."""

    try:
        if lock_path.is_symlink():
            raise ReleaseBuildError("Python release lock must not be a symbolic link")
        raw = lock_path.read_bytes()
        if not raw or len(raw) > MAX_LOCK_BYTES:
            raise ReleaseBuildError("Python release lock exceeds its byte bound")
        value = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseBuildError("Python release lock is unreadable") from error
    artifacts = value.get("artifacts") if isinstance(value, dict) else None
    if not isinstance(artifacts, list) or len(artifacts) > MAX_ARTIFACTS:
        raise ReleaseBuildError("Python release lock has no bounded artifact inventory")
    matches = [
        artifact
        for artifact in artifacts
        if isinstance(artifact, dict)
        and artifact.get("project") == PACKAGING_BOOTSTRAP["project"]
    ]
    if len(matches) != 1 or any(
        matches[0].get(field) != expected
        for field, expected in PACKAGING_BOOTSTRAP.items()
    ):
        raise ReleaseBuildError("Python release lock lacks the admitted packaging bootstrap")

    _admit_owned_child(wheelhouse, wheelhouse.parent, "wheelhouse")
    destination = wheelhouse / str(PACKAGING_BOOTSTRAP["filename"])
    expected = (
        int(PACKAGING_BOOTSTRAP["size_bytes"]),
        str(PACKAGING_BOOTSTRAP["sha256"]),
    )
    if destination.exists() or destination.is_symlink():
        if destination.is_symlink() or not destination.is_file():
            raise ReleaseBuildError("packaging bootstrap cache path is unsafe")
        if _file_digest(destination) != expected:
            raise ReleaseBuildError("cached packaging bootstrap identity differs")
    else:
        temporary = destination.with_suffix(destination.suffix + ".part")
        temporary.unlink(missing_ok=True)
        cached = (
            source_cache / destination.name if source_cache is not None else None
        )
        if cached is not None and cached.is_file() and not cached.is_symlink():
            shutil.copyfile(cached, temporary)
        elif allow_network:
            request = urllib.request.Request(
                str(PACKAGING_BOOTSTRAP["url"]),
                headers={
                    "Accept": "application/octet-stream",
                    "User-Agent": "market-squawk-release-builder",
                },
            )
            observed = 0
            digest = hashlib.sha256()
            try:
                with urllib.request.urlopen(request, timeout=60) as response, temporary.open(
                    "xb"
                ) as output:
                    final = urllib.parse.urlparse(response.geturl())
                    if (
                        final.scheme != "https"
                        or final.hostname != "files.pythonhosted.org"
                    ):
                        raise ReleaseBuildError(
                            "packaging bootstrap redirected outside PyPI files"
                        )
                    declared = response.headers.get("Content-Length")
                    try:
                        declared_size = int(declared) if declared is not None else None
                    except ValueError as error:
                        raise ReleaseBuildError(
                            "packaging bootstrap response length is invalid"
                        ) from error
                    if declared_size is not None and declared_size != expected[0]:
                        raise ReleaseBuildError("packaging bootstrap response length differs")
                    while chunk := response.read(1024 * 1024):
                        observed += len(chunk)
                        if observed > expected[0]:
                            raise ReleaseBuildError(
                                "packaging bootstrap exceeded its locked size"
                            )
                        digest.update(chunk)
                        output.write(chunk)
                    output.flush()
                    os.fsync(output.fileno())
            except (OSError, urllib.error.URLError, ReleaseBuildError):
                temporary.unlink(missing_ok=True)
                raise
            if observed != expected[0] or digest.hexdigest() != expected[1]:
                temporary.unlink(missing_ok=True)
                raise ReleaseBuildError("downloaded packaging bootstrap identity differs")
        else:
            raise ReleaseBuildError(
                "locked packaging bootstrap is absent from the offline cache"
            )
        if _file_digest(temporary) != expected:
            temporary.unlink(missing_ok=True)
            raise ReleaseBuildError("packaging bootstrap identity differs")
        os.replace(temporary, destination)

    for module_name in tuple(sys.modules):
        if module_name == "packaging" or module_name.startswith("packaging."):
            del sys.modules[module_name]
    admitted_wheel = destination.resolve(strict=True)
    sys.path.insert(0, str(admitted_wheel))
    try:
        import packaging
    except ImportError as error:
        raise ReleaseBuildError("locked packaging bootstrap could not load") from error
    loader_archive = getattr(getattr(packaging, "__loader__", None), "archive", None)
    try:
        loaded_wheel = Path(loader_archive).resolve(strict=True)
    except (OSError, TypeError):
        loaded_wheel = None
    if packaging.__version__ != PACKAGING_VERSION or loaded_wheel != admitted_wheel:
        raise ReleaseBuildError("locked packaging bootstrap identity differs")


@dataclass(frozen=True)
class PlatformProfile:
    target: str
    system: str
    machines: tuple[str, ...]
    executable_suffix: str
    interpreter_relative_path: str
    wheel_platform_tag: str
    minimum_system: str
    native_extension_suffix: str
    maturin_compatibility: str | None = None


PLATFORM_PROFILES = {
    profile.target: profile
    for profile in (
        PlatformProfile(
            "aarch64-apple-darwin",
            "Darwin",
            ("arm64", "aarch64"),
            "",
            "bin/python",
            "macosx_12_0_arm64",
            "macOS 12",
            "abi3.so",
        ),
        PlatformProfile(
            "x86_64-apple-darwin",
            "Darwin",
            ("x86_64",),
            "",
            "bin/python",
            "macosx_12_0_x86_64",
            "macOS 12",
            "abi3.so",
        ),
        PlatformProfile(
            "x86_64-pc-windows-msvc",
            "Windows",
            ("AMD64", "x86_64"),
            ".exe",
            "python.exe",
            "win_amd64",
            "Windows 10 1809",
            "pyd",
        ),
        PlatformProfile(
            "x86_64-unknown-linux-gnu",
            "Linux",
            ("x86_64", "AMD64"),
            "",
            "bin/python",
            "manylinux_2_28_x86_64",
            "Ubuntu 24.04-compatible",
            "abi3.so",
            "manylinux_2_28",
        ),
    )
}


def platform_profile(target: str) -> PlatformProfile:
    try:
        return PLATFORM_PROFILES[target]
    except KeyError as error:
        raise ReleaseBuildError("release target is unsupported") from error


def host_profile() -> PlatformProfile:
    system = platform.system()
    machine = platform.machine()
    matches = [
        profile
        for profile in PLATFORM_PROFILES.values()
        if profile.system == system and _machine_matches(profile, machine)
    ]
    if len(matches) != 1:
        raise ReleaseBuildError("host is not one exact supported release target")
    return matches[0]


def _machine_matches(profile: PlatformProfile, machine: object) -> bool:
    return isinstance(machine, str) and machine.casefold() in {
        supported.casefold() for supported in profile.machines
    }


def _interpreter_platform_matches(
    profile: PlatformProfile,
    evidence: dict[str, object],
) -> bool:
    machine = evidence.get("machine")
    if isinstance(machine, str) and machine:
        return _machine_matches(profile, machine)

    configured = evidence.get("configured_platform")
    if not isinstance(configured, str):
        return False
    normalized = configured.casefold().replace("_", "-")
    if profile.target == "x86_64-pc-windows-msvc":
        return normalized == "win-amd64"
    if profile.target == "x86_64-unknown-linux-gnu":
        return normalized == "linux-x86-64"
    if profile.target == "aarch64-apple-darwin":
        return normalized.startswith("macosx-") and normalized.endswith("-arm64")
    if profile.target == "x86_64-apple-darwin":
        return normalized.startswith("macosx-") and normalized.endswith("-x86-64")
    return False


@dataclass(frozen=True)
class LicenseFile:
    path: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class Artifact:
    project: str
    version: str
    license: str
    filename: str
    sha256: str
    size_bytes: int
    url: str
    requires_python: str = ""
    metadata_sha256: str = ""
    tags: tuple[str, ...] = ()
    license_files: tuple[LicenseFile, ...] = ()


@dataclass(frozen=True)
class Source:
    path: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class ReleaseLock:
    minimum: tuple[int, int]
    maximum_exclusive: tuple[int, int]
    target: str
    artifacts: tuple[Artifact, ...]
    sources: tuple[Source, ...]

    @classmethod
    def for_test(cls, *, filename: str, sha256: str, size_bytes: int) -> "ReleaseLock":
        return cls(
            (3, 14),
            (3, 15),
            "aarch64-apple-darwin",
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
class AcquiredReleaseComponents:
    python: Path
    uv: Path
    zig: Path | None


@dataclass(frozen=True)
class InstalledDistribution:
    name: str
    version: str
    roots: tuple[str, ...]
    external_paths: tuple[str, ...]
    owned_paths: tuple[Path, ...]
    owned_file_identities: tuple[tuple[int, int], ...]
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
    training_driver: Path
    validator: Path


@dataclass(frozen=True)
class NativeCodeSigning:
    system: str
    identity: str
    tool: Path

    def evidence(self) -> dict[str, object]:
        return {
            "identity": self.identity,
            "status": "signed",
            "system": self.system,
            "tool_sha256": _file_digest(self.tool)[1],
            "timestamped": True,
        }


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


def load_lock(
    path: Path,
    target: str | None = None,
    *,
    platform_lock_directory: Path | None = None,
) -> ReleaseLock:
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
        "artifacts",
        "sources",
        "inventory_generation",
    }:
        raise ReleaseBuildError("Python release lock shape is invalid")
    if value["schema_version"] != 3:
        raise ReleaseBuildError("Python release lock version is unsupported")
    _sha256(value["inventory_generation"])
    python = value["python"]
    if not isinstance(python, dict) or set(python) != {"minimum", "maximum_exclusive"}:
        raise ReleaseBuildError("Python interpreter matrix is invalid")
    minimum = _version(python["minimum"])
    maximum = _version(python["maximum_exclusive"])
    if minimum != (3, 14) or maximum != (3, 15):
        raise ReleaseBuildError("Python lock must claim only the verified CPython 3.14 product")
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
        required = {
            "project",
            "version",
            "license",
            "filename",
            "sha256",
            "size_bytes",
            "url",
            "requires_python",
            "metadata_sha256",
            "tags",
            "license_files",
            "upload_time",
            "yanked",
        }
        if (
            not isinstance(item, dict)
            or set(item) != required
        ):
            raise ReleaseBuildError("Python wheel identity is incomplete")
        if any(
            not isinstance(item[field], str)
            for field in (
                "project",
                "version",
                "license",
                "filename",
                "sha256",
                "url",
                "requires_python",
                "metadata_sha256",
                "upload_time",
            )
        ):
            raise ReleaseBuildError("Python wheel identity has an invalid type")
        if item["filename"] in names or item["license"] not in ALLOWED_LICENSES:
            raise ReleaseBuildError("Python wheel identity or license is invalid")
        _sha256(item["sha256"])
        _sha256(item["metadata_sha256"])
        license_files = item["license_files"]
        if not isinstance(license_files, list) or not license_files:
            raise ReleaseBuildError("Python wheel license identity is invalid")
        parsed_license_files = []
        for license_file in license_files:
            if not isinstance(license_file, dict) or set(license_file) != {
                "path",
                "sha256",
                "size_bytes",
            }:
                raise ReleaseBuildError("Python wheel license identity is invalid")
            if (
                not isinstance(license_file["path"], str)
                or not license_file["path"]
                or license_file["path"].startswith(("/", "\\"))
                or "\\" in license_file["path"]
                or not isinstance(license_file["size_bytes"], int)
                or license_file["size_bytes"] <= 0
            ):
                raise ReleaseBuildError("Python wheel license identity is invalid")
            _sha256(license_file["sha256"])
            license_path = PurePosixPath(license_file["path"])
            if license_path.is_absolute() or any(
                part in {"", ".", ".."} for part in license_path.parts
            ):
                raise ReleaseBuildError("Python wheel license identity is invalid")
            parsed_license_files.append(LicenseFile(**license_file))
        if (
            item["sha256"] == "0" * 64
            or item["metadata_sha256"] == "0" * 64
            or not isinstance(item["size_bytes"], int)
            or item["size_bytes"] <= 0
            or item["yanked"] is not False
            or not isinstance(item["requires_python"], str)
            or not item["requires_python"]
            or not isinstance(item["upload_time"], str)
            or not item["upload_time"]
        ):
            raise ReleaseBuildError("Python wheel hash or size is invalid")
        parsed = urllib.parse.urlparse(item["url"])
        if (
            parsed.scheme != "https"
            or parsed.hostname != "files.pythonhosted.org"
            or Path(parsed.path).name != item["filename"]
        ):
            raise ReleaseBuildError("Python wheel URL is not an exact official artifact")
        parsed_project, parsed_release, parsed_build, parsed_tags = _parse_wheel(
            item["filename"]
        )
        project = _normalize_project_name(item["project"])
        if (
            str(parsed_project) != project
            or str(parsed_release) != item["version"]
            or parsed_build
            or project not in PYTHON_LICENSE_POLICY
            or item["license"] != PYTHON_LICENSE_POLICY[project]
        ):
            raise ReleaseBuildError("Python wheel metadata differs from its filename")
        try:
            from packaging.specifiers import InvalidSpecifier, SpecifierSet
        except ImportError as error:
            raise ReleaseBuildError(
                "packaging 26.2 is required for wheel admission"
            ) from error
        try:
            requires_python = SpecifierSet(item["requires_python"])
        except InvalidSpecifier as error:
            raise ReleaseBuildError(
                "Python wheel interpreter requirement is invalid"
            ) from error
        if not requires_python.contains("3.14.6", prereleases=False):
            raise ReleaseBuildError("Python wheel excludes the admitted interpreter")
        tags = item["tags"]
        if (
            not isinstance(tags, list)
            or tags != sorted(tags)
            or len(tags) != len(set(tags))
            or set(tags) != {str(tag) for tag in parsed_tags}
        ):
            raise ReleaseBuildError("Python wheel tag identity is invalid")
        artifact_value = dict(item)
        artifact_value["tags"] = tuple(tags)
        artifact_value["license_files"] = tuple(parsed_license_files)
        artifact_value.pop("upload_time")
        artifact_value.pop("yanked")
        artifacts.append(Artifact(**artifact_value))
        names.add(item["filename"])
    profile = platform_profile(target or host_profile().target)
    profile_path = (
        platform_lock_directory or path.parent / "wheelhouse"
    ) / f"{profile.target}.json"
    profile_artifacts = _load_platform_wheel_lock(
        profile_path, profile, value["inventory_generation"]
    )
    selected = {
        artifact.filename: artifact
        for artifact in artifacts
        if artifact.filename in profile_artifacts
    }
    if set(selected) != profile_artifacts:
        raise ReleaseBuildError("platform wheel set is absent from the common release lock")
    if any(
        not _compatible(artifact.filename, minimum, profile)
        for artifact in selected.values()
    ):
        raise ReleaseBuildError("platform wheel set contains an incompatible artifact")
    selected_projects = {artifact.project for artifact in selected.values()}
    required_projects = REQUIRED_PROJECTS
    if profile.system != "Windows":
        required_projects = REQUIRED_PROJECTS - {"colorama"}
    if selected_projects != required_projects:
        raise ReleaseBuildError("platform wheel project closure is incomplete or unexpected")
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
    return ReleaseLock(
        minimum,
        maximum,
        profile.target,
        tuple(selected[name] for name in sorted(selected)),
        tuple(sources),
    )


def _load_platform_wheel_lock(
    path: Path, profile: PlatformProfile, inventory_generation: str
) -> set[str]:
    try:
        if path.is_symlink():
            raise ReleaseBuildError("platform wheel lock must not be a symbolic link")
        raw = path.read_bytes()
        if not raw or len(raw) > 64 * 1024:
            raise ReleaseBuildError("platform wheel lock exceeds its byte bound")
        value = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseBuildError("platform wheel lock is unreadable") from error
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "target",
        "minimum_system",
        "wheel_platform_tag",
        "artifacts",
        "inventory_generation",
    }:
        raise ReleaseBuildError("platform wheel lock shape is invalid")
    artifacts = value["artifacts"]
    if (
        value["schema_version"] != 2
        or value["target"] != profile.target
        or value["inventory_generation"] != inventory_generation
        or value["minimum_system"] != profile.minimum_system
        or value["wheel_platform_tag"] != profile.wheel_platform_tag
        or not isinstance(artifacts, list)
        or len(artifacts) != len(set(artifacts))
        or artifacts != sorted(artifacts)
        or any(
            not isinstance(name, str)
            or not name.endswith(".whl")
            or "/" in name
            or "\\" in name
            for name in artifacts
        )
    ):
        raise ReleaseBuildError("platform wheel lock identity is invalid")
    return set(artifacts)


def admit_release_components(
    path: Path,
    profile: PlatformProfile,
    uv_executable: Path,
    zig_executable: Path | None,
    root: Path,
) -> str:
    """Admit the exact uv, CPython, PyArrow, and Linux linker component matrix."""

    raw, target = load_release_components(path, profile)
    selected = target["uv"]
    try:
        executable = uv_executable.expanduser().resolve(strict=True)
    except OSError as error:
        raise ReleaseBuildError("locked uv executable is unavailable") from error
    if (
        executable.is_symlink()
        or not executable.is_file()
        or _file_digest(executable)
        != (selected["binary_size_bytes"], selected["binary_sha256"])
    ):
        raise ReleaseBuildError("locked uv executable identity differs")
    version = _run_output([str(executable), "--version"], root)
    if not version.startswith("uv 0.12.1 "):
        raise ReleaseBuildError("locked uv executable reports the wrong version")
    selected_zig = target.get("zig")
    if profile.system == "Linux":
        if not isinstance(selected_zig, dict) or zig_executable is None:
            raise ReleaseBuildError("locked Zig executable is unavailable")
        try:
            zig = zig_executable.expanduser().resolve(strict=True)
        except OSError as error:
            raise ReleaseBuildError("locked Zig executable is unavailable") from error
        if (
            zig.is_symlink()
            or not zig.is_file()
            or _file_digest(zig)
            != (selected_zig["binary_size_bytes"], selected_zig["binary_sha256"])
            or _run_output([str(zig), "version"], root) != "0.16.0"
        ):
            raise ReleaseBuildError("locked Zig executable identity differs")
    elif selected_zig is not None or zig_executable is not None:
        raise ReleaseBuildError("Zig is permitted only for the Linux release target")
    return hashlib.sha256(raw).hexdigest()


def load_release_components(
    path: Path,
    profile: PlatformProfile,
) -> tuple[bytes, dict[str, object]]:
    """Load the closed component matrix and select one already-validated target."""

    try:
        if path.is_symlink():
            raise ReleaseBuildError("release component lock must not be a symbolic link")
        raw = path.read_bytes()
        if not raw or len(raw) > 256 * 1024:
            raise ReleaseBuildError("release component lock exceeds its byte bound")
        value = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseBuildError("release component lock is unreadable") from error
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "uv",
        "zig",
        "python",
        "targets",
    }:
        raise ReleaseBuildError("release component lock shape is invalid")
    uv = value["uv"]
    zig = value["zig"]
    python = value["python"]
    targets = value["targets"]
    if (
        value["schema_version"] != 1
        or not isinstance(uv, dict)
        or set(uv) != {"version", "license", "release_url"}
        or uv["version"] != "0.12.1"
        or uv["license"] != "Apache-2.0 OR MIT"
        or uv["release_url"] != "https://github.com/astral-sh/uv/releases/tag/0.12.1"
        or not isinstance(zig, dict)
        or set(zig) != {"version", "license", "release_url"}
        or zig["version"] != "0.16.0"
        or zig["license"] != "MIT"
        or zig["release_url"] != "https://ziglang.org/download/0.16.0/"
        or not isinstance(python, dict)
        or set(python)
        != {
            "implementation",
            "version",
            "variant",
            "provider",
            "license",
            "provider_license",
            "release_url",
        }
        or python["implementation"] != "cpython"
        or python["version"] != "3.14.6"
        or python["variant"] != "standard-gil"
        or python["provider"] != "astral-sh/python-build-standalone"
        or python["license"] != "Python-2.0"
        or python["provider_license"] != "MPL-2.0"
        or python["release_url"]
        != "https://github.com/astral-sh/python-build-standalone/releases/tag/20260728"
        or not isinstance(targets, dict)
        or set(targets) != set(PLATFORM_PROFILES)
    ):
        raise ReleaseBuildError("release component matrix identity is invalid")
    for target, target_value in targets.items():
        target_profile = platform_profile(target)
        expected_fields = {
            "minimum_system",
            "wheel_platform_tag",
            "uv",
            "python",
            "pyarrow",
        }
        if target_profile.system == "Linux":
            expected_fields.add("zig")
        if not isinstance(target_value, dict) or set(target_value) != expected_fields:
            raise ReleaseBuildError("target release-component shape is invalid")
        if (
            target_value["minimum_system"] != target_profile.minimum_system
            or target_value["wheel_platform_tag"]
            != target_profile.wheel_platform_tag
        ):
            raise ReleaseBuildError("target release-component contract is invalid")
        _admit_download_identity(
            target_value["uv"],
            "github.com",
            binary_archive="uv",
        )
        _admit_download_identity(target_value["python"], "github.com")
        _admit_download_identity(target_value["pyarrow"], "files.pythonhosted.org")
        if target_profile.system == "Linux":
            _admit_download_identity(
                target_value["zig"],
                "ziglang.org",
                binary_archive="Zig",
            )

    selected = targets[profile.target]
    if not isinstance(selected, dict):
        raise ReleaseBuildError("selected release-component target is invalid")
    return raw, selected


def _admit_download_identity(
    value: object,
    host: str,
    *,
    binary_archive: str | None = None,
) -> None:
    required = {"url", "size_bytes", "sha256"}
    if binary_archive is not None:
        required.update(
            {
                "format",
                "binary_path",
                "binary_size_bytes",
                "binary_sha256",
            }
        )
    elif host == "files.pythonhosted.org":
        required.update({"filename", "version", "license"})
    else:
        required.update({"format", "archive_path"})
    if not isinstance(value, dict) or set(value) != required:
        raise ReleaseBuildError("release download identity shape is invalid")
    parsed = urllib.parse.urlparse(value["url"])
    if (
        parsed.scheme != "https"
        or parsed.hostname != host
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not isinstance(value["size_bytes"], int)
        or value["size_bytes"] <= 0
    ):
        raise ReleaseBuildError("release download URL or size is invalid")
    _sha256(value["sha256"])
    if binary_archive is not None:
        if (
            value["format"] not in {"tar.gz", "tar.xz", "zip"}
            or not isinstance(value["binary_path"], str)
            or not value["binary_path"]
            or not isinstance(value["binary_size_bytes"], int)
            or value["binary_size_bytes"] <= 0
        ):
            raise ReleaseBuildError(f"{binary_archive} archive member identity is invalid")
        _sha256(value["binary_sha256"])
    elif host == "files.pythonhosted.org":
        if (
            value["version"] != "25.0.0"
            or value["license"] != "Apache-2.0"
            or Path(parsed.path).name != value["filename"]
        ):
            raise ReleaseBuildError("PyArrow release identity is invalid")
    elif value["format"] != "tar.gz" or value["archive_path"] != "python":
        raise ReleaseBuildError("managed Python archive identity is invalid")


def acquire_release_components(
    path: Path,
    profile: PlatformProfile,
    component_root: Path,
    repository_root: Path,
    *,
    allow_network: bool,
    development_runtime_root: Path | None = None,
) -> AcquiredReleaseComponents:
    """Acquire and safely expand the exact uv, CPython, and Linux Zig archives."""

    _raw, target = load_release_components(path, profile)
    root = _admit_component_root(
        component_root,
        repository_root,
        profile,
        development_runtime_root=development_runtime_root,
    )
    downloads = root / "downloads"
    expanded = root / "expanded"
    for directory in (downloads, expanded):
        if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
            raise ReleaseBuildError("release component directory is unsafe")
        directory.mkdir(exist_ok=True)

    uv = target["uv"]
    python = target["python"]
    if not isinstance(uv, dict) or not isinstance(python, dict):
        raise ReleaseBuildError("selected release-component inputs are invalid")
    uv_archive = _acquire_locked_download(
        downloads / "uv.archive",
        uv,
        allow_network=allow_network,
    )
    python_archive = _acquire_locked_download(
        downloads / "python.archive",
        python,
        allow_network=allow_network,
    )
    uv_root = _replace_extracted_archive(
        uv_archive,
        expanded / "uv",
        str(uv["format"]),
    )
    python_root = _replace_extracted_archive(
        python_archive,
        expanded / "python",
        str(python["format"]),
    )
    uv_executable = (uv_root / str(uv["binary_path"])).resolve(strict=True)
    python_executable = (
        python_root
        / str(python["archive_path"])
        / profile.interpreter_relative_path
    ).resolve(strict=True)
    zig_executable = None
    zig = target.get("zig")
    if profile.system == "Linux":
        if not isinstance(zig, dict):
            raise ReleaseBuildError("selected Zig release component is invalid")
        zig_archive = _acquire_locked_download(
            downloads / "zig.archive",
            zig,
            allow_network=allow_network,
        )
        zig_root = _replace_extracted_archive(
            zig_archive,
            expanded / "zig",
            str(zig["format"]),
        )
        zig_executable = (zig_root / str(zig["binary_path"])).resolve(strict=True)
    if (
        not uv_executable.is_file()
        or _file_digest(uv_executable)
        != (uv["binary_size_bytes"], uv["binary_sha256"])
        or not python_executable.is_file()
        or (
            zig_executable is not None
            and (
                not zig_executable.is_file()
                or _file_digest(zig_executable)
                != (zig["binary_size_bytes"], zig["binary_sha256"])
            )
        )
    ):
        raise ReleaseBuildError("expanded release component identity is invalid")
    return AcquiredReleaseComponents(
        python=python_executable,
        uv=uv_executable,
        zig=zig_executable,
    )


def _admit_component_root(
    path: Path,
    repository_root: Path,
    profile: PlatformProfile,
    *,
    development_runtime_root: Path | None = None,
) -> Path:
    repository_root = repository_root.resolve(strict=True)
    home = Path.home().resolve(strict=True)
    candidate = path.expanduser().absolute()
    if candidate.is_symlink():
        raise ReleaseBuildError("release component root must not be a symbolic link")
    if candidate.exists():
        if not candidate.is_dir():
            raise ReleaseBuildError("release component root must be a directory")
        canonical = candidate.resolve(strict=True)
    else:
        canonical = candidate.parent.resolve(strict=True) / candidate.name
    allowed_development_root = (
        development_runtime_root.resolve(strict=True) / "components"
        if development_runtime_root is not None
        else None
    )
    if canonical in {Path("/"), home, repository_root} or (
        canonical.is_relative_to(repository_root)
        and canonical != allowed_development_root
    ):
        raise ReleaseBuildError("release component root resolves to a protected location")
    purpose = f"{COMPONENT_ROOT_PURPOSE}:{profile.target}"
    expected = _marker_content(canonical, purpose)
    marker = canonical / COMPONENT_ROOT_MARKER
    if canonical.exists():
        entries = tuple(canonical.iterdir())
        if marker not in entries:
            if entries:
                raise ReleaseBuildError("release component root is not builder-owned")
            marker.write_text(expected, encoding="utf-8")
        elif marker.is_symlink() or not marker.is_file() or marker.read_text() != expected:
            raise ReleaseBuildError("release component root ownership marker is invalid")
    else:
        canonical.mkdir(mode=0o700)
        marker.write_text(expected, encoding="utf-8")
    if os.name != "nt":
        canonical.chmod(0o700)
    return canonical


def _acquire_locked_download(
    destination: Path,
    identity: dict[str, object],
    *,
    allow_network: bool,
) -> Path:
    expected = (identity["size_bytes"], identity["sha256"])
    if destination.is_file() and not destination.is_symlink():
        if _file_digest(destination) == expected:
            return destination
        if not allow_network:
            raise ReleaseBuildError("cached release component identity differs")
        destination.unlink()
    elif destination.exists() or destination.is_symlink():
        raise ReleaseBuildError("release component download path is unsafe")
    if not allow_network:
        raise ReleaseBuildError("locked release component is absent from the offline cache")

    temporary = destination.with_suffix(".part")
    temporary.unlink(missing_ok=True)
    expected_size = int(identity["size_bytes"])
    digest = hashlib.sha256()
    observed = 0
    request = urllib.request.Request(
        str(identity["url"]),
        headers={
            "Accept": "application/octet-stream",
            "User-Agent": "market-squawk-release-builder",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response, temporary.open(
            "xb"
        ) as output:
            final = urllib.parse.urlparse(response.geturl())
            if (
                final.scheme != "https"
                or final.hostname
                not in {
                    "github.com",
                    "objects.githubusercontent.com",
                    "release-assets.githubusercontent.com",
                    "ziglang.org",
                }
            ):
                raise ReleaseBuildError("release component redirected outside its allowed hosts")
            declared = response.headers.get("Content-Length")
            if declared is not None and int(declared) != expected_size:
                raise ReleaseBuildError("release component response length differs")
            while chunk := response.read(1024 * 1024):
                observed += len(chunk)
                if observed > expected_size:
                    raise ReleaseBuildError("release component exceeded its locked byte size")
                digest.update(chunk)
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
    except (OSError, ValueError, urllib.error.URLError, ReleaseBuildError):
        temporary.unlink(missing_ok=True)
        raise
    if observed != expected_size or digest.hexdigest() != identity["sha256"]:
        temporary.unlink(missing_ok=True)
        raise ReleaseBuildError("release component download identity differs")
    if os.name != "nt":
        temporary.chmod(0o600)
    os.replace(temporary, destination)
    return destination


def _replace_extracted_archive(archive: Path, destination: Path, archive_format: str) -> Path:
    parent = destination.parent.resolve(strict=True)
    if destination.parent != parent or destination.is_symlink():
        raise ReleaseBuildError("release component extraction path is unsafe")
    temporary = parent / f".{destination.name}.extracting"
    if temporary.exists() or temporary.is_symlink():
        if not temporary.is_dir() or temporary.is_symlink():
            raise ReleaseBuildError("release component temporary path is unsafe")
        shutil.rmtree(temporary)
    temporary.mkdir(mode=0o700)
    try:
        if archive_format in {"tar.gz", "tar.xz"}:
            _extract_tar_archive(archive, temporary, archive_format)
        elif archive_format == "zip":
            _extract_zip_archive(archive, temporary)
        else:
            raise ReleaseBuildError("release component archive format is unsupported")
        _admit_extracted_tree(temporary)
        if destination.exists():
            if not destination.is_dir() or destination.is_symlink():
                raise ReleaseBuildError("release component destination is unsafe")
            shutil.rmtree(destination)
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    return destination.resolve(strict=True)


def _extract_tar_archive(
    archive: Path,
    destination: Path,
    archive_format: str,
) -> None:
    mode = "r:gz" if archive_format == "tar.gz" else "r:xz"
    with tarfile.open(archive, mode=mode) as source:
        members = source.getmembers()
        total = 0
        if not members or len(members) > 32_768:
            raise ReleaseBuildError("release component archive entry count is invalid")
        for member in members:
            _validate_archive_path(member.name)
            if not (member.isdir() or member.isreg() or member.issym() or member.islnk()):
                raise ReleaseBuildError("release component archive contains a special entry")
            if member.size < 0 or member.size > MAX_DISTRIBUTION_FILE_BYTES:
                raise ReleaseBuildError("release component archive entry is oversized")
            total += member.size
            if total > MAX_DISTRIBUTION_BYTES:
                raise ReleaseBuildError("release component archive is oversized")
        source.extractall(destination, members=members, filter="data")


def _extract_zip_archive(archive: Path, destination: Path) -> None:
    with zipfile.ZipFile(archive, mode="r") as source:
        members = source.infolist()
        total = 0
        if not members or len(members) > 32_768:
            raise ReleaseBuildError("release component archive entry count is invalid")
        for member in members:
            _validate_archive_path(member.filename)
            mode = member.external_attr >> 16
            file_type = stat.S_IFMT(mode)
            if (
                member.flag_bits & 0x1
                or member.file_size < 0
                or member.file_size > MAX_DISTRIBUTION_FILE_BYTES
                or file_type not in (0, stat.S_IFREG, stat.S_IFDIR)
            ):
                raise ReleaseBuildError("release component archive contains an invalid entry")
            total += member.file_size
            if total > MAX_DISTRIBUTION_BYTES:
                raise ReleaseBuildError("release component archive is oversized")
            output = destination.joinpath(*PurePosixPath(member.filename).parts)
            if member.is_dir():
                output.mkdir(parents=True, exist_ok=True)
                continue
            output.parent.mkdir(parents=True, exist_ok=True)
            with source.open(member) as reader, output.open("xb") as writer:
                shutil.copyfileobj(reader, writer, 1024 * 1024)


def _validate_archive_path(value: str) -> None:
    path = PurePosixPath(value)
    if (
        not value
        or value.startswith(("/", "\\"))
        or "\\" in value
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ReleaseBuildError("release component archive contains an unsafe path")


def _admit_extracted_tree(root: Path) -> None:
    files = 0
    total = 0
    for path in root.rglob("*"):
        metadata = path.lstat()
        if path.is_symlink():
            resolved = path.resolve(strict=True)
            if not resolved.is_relative_to(root) or not resolved.is_file():
                raise ReleaseBuildError("release component archive contains an unsafe link")
            continue
        if path.is_dir():
            continue
        if not stat.S_ISREG(metadata.st_mode):
            raise ReleaseBuildError("release component archive contains a special file")
        files += 1
        total += metadata.st_size
        if files > 32_768 or total > MAX_DISTRIBUTION_BYTES:
            raise ReleaseBuildError("release component extraction exceeds its fixed bounds")


def admit_development_runtime_root(
    path: Path,
    repository_root: Path,
    *,
    create: bool,
) -> Path:
    """Admit the one ignored, builder-owned development model-runtime root."""

    repository_root = repository_root.resolve(strict=True)
    expected = repository_root / DEVELOPMENT_RUNTIME_RELATIVE
    candidate = Path(os.path.abspath(path.expanduser()))
    if candidate != expected or candidate.is_symlink():
        raise ReleaseBuildError("development runtime root is not the repository-owned path")

    parent = expected.parent
    if parent.is_symlink() or (parent.exists() and not parent.is_dir()):
        raise ReleaseBuildError("development runtime parent is unsafe")
    if not parent.exists():
        if not create:
            raise ReleaseBuildError("development runtime is not prepared")
        parent.mkdir(mode=0o700)

    marker = expected / DEVELOPMENT_ROOT_MARKER
    marker_content = _marker_content(expected, DEVELOPMENT_ROOT_PURPOSE)
    if expected.exists():
        if not expected.is_dir() or expected.is_symlink():
            raise ReleaseBuildError("development runtime root is unsafe")
        entries = tuple(expected.iterdir())
        if marker not in entries:
            if entries or not create:
                raise ReleaseBuildError("development runtime root is not builder-owned")
            marker.write_text(marker_content, encoding="utf-8")
        elif (
            marker.is_symlink()
            or not marker.is_file()
            or marker.read_text(encoding="utf-8") != marker_content
        ):
            raise ReleaseBuildError("development runtime ownership marker is invalid")
    elif create:
        expected.mkdir(mode=0o700)
        marker.write_text(marker_content, encoding="utf-8")
    else:
        raise ReleaseBuildError("development runtime is not prepared")
    if os.name != "nt":
        expected.chmod(0o700)
        marker.chmod(0o600)
    return expected.resolve(strict=True)


def reset_development_runtime_root(path: Path, repository_root: Path) -> None:
    """Remove only the admitted ignored development model-runtime cache."""

    repository_root = repository_root.resolve(strict=True)
    expected = repository_root / DEVELOPMENT_RUNTIME_RELATIVE
    candidate = Path(os.path.abspath(path.expanduser()))
    if candidate != expected:
        raise ReleaseBuildError("development runtime root is not the repository-owned path")
    if not candidate.exists() and not candidate.is_symlink():
        return
    admitted = admit_development_runtime_root(candidate, repository_root, create=False)
    release = admitted / "python" / CANONICAL_RELEASE
    if release.exists() and not release.is_symlink():
        _unseal_owned_release_authority(release)
    _remove_owned_tree(admitted)


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
    profile = platform_profile(lock.target)
    selected = tuple(
        artifact
        for artifact in lock.artifacts
        if _compatible(artifact.filename, version, profile)
    )
    projects = {artifact.project.lower() for artifact in selected}
    required_projects = (
        REQUIRED_PROJECTS
        if profile.system == "Windows"
        else REQUIRED_PROJECTS - {"colorama"}
    )
    if not required_projects <= projects:
        raise ReleaseBuildError("wheelhouse has no complete compatible dependency set")
    admitted = []
    for artifact in selected:
        path = wheelhouse / artifact.filename
        if path.is_symlink() or not path.is_file():
            raise ReleaseBuildError("offline wheelhouse is missing a locked artifact")
        if _file_digest(path) != (artifact.size_bytes, artifact.sha256):
            raise ReleaseBuildError("offline wheelhouse artifact hash or size mismatch")
        _admit_license(path, artifact.license, artifact.license_files)
        admitted.append(path)
    return tuple(admitted)


def locked_runtime_requirements(
    root: Path, lock: ReleaseLock
) -> tuple[RuntimeRequirement, ...]:
    """Resolve every exact Python project dependency against each supported wheel set."""

    del root
    requirements = []
    names = set()
    for artifact in lock.artifacts:
        name = _normalize_project_name(artifact.project)
        if name == "maturin":
            continue
        if name in names:
            raise ReleaseBuildError("Python runtime dependency is duplicated")
        requirements.append(RuntimeRequirement(name, artifact.version))
        names.add(name)
    if not requirements or len(requirements) > MAX_RUNTIME_DISTRIBUTIONS:
        raise ReleaseBuildError("Python runtime dependency policy is invalid")
    return tuple(sorted(requirements, key=lambda value: value.name))


def prepare_wheelhouse(
    lock: ReleaseLock,
    wheelhouse: Path,
    source_cache: Path | None,
) -> None:
    if (
        os.environ.get("MARKET_SQUAWK_PYTHON_WHEEL_PREPARE_NETWORK") != "1"
        and source_cache is None
    ):
        raise ReleaseBuildError(
            "wheel preparation requires an explicit cache or network authorization"
        )
    _admit_owned_child(wheelhouse, wheelhouse.parent, "wheelhouse")
    selected = {
        artifact.filename: artifact
        for artifact in lock.artifacts
        if any(
            _compatible(
                artifact.filename,
                version,
                platform_profile(lock.target),
            )
            for version in SUPPORTED_PYTHONS
        )
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
        _admit_license(temporary, artifact.license, artifact.license_files)
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
    for relative in ("python/market_squawk",):
        paths.update(_regular_files(root, root / relative))
    paths.update(
        {
            "distribution/release-components.json",
            "python/pyproject.toml",
            "python/requirements.lock",
            "python/wheelhouse/aarch64-apple-darwin.json",
            "python/wheelhouse/x86_64-apple-darwin.json",
            "python/wheelhouse/x86_64-pc-windows-msvc.json",
            "python/wheelhouse/x86_64-unknown-linux-gnu.json",
            "scripts/build_python_release.py",
        }
    )
    workspace = _toml(root / "Cargo.toml")
    workspace_dependencies = workspace["workspace"]["dependencies"]
    pending = [
        root / "apps/market-squawk/Cargo.toml",
        root / "crates/market-squawk-python/Cargo.toml",
        root / "crates/market-squawk-modeling/Cargo.toml",
    ]
    patches = workspace.get("patch")
    if isinstance(patches, dict):
        for registry in patches.values():
            if not isinstance(registry, dict):
                continue
            for patch in registry.values():
                if isinstance(patch, dict) and isinstance(patch.get("path"), str):
                    pending.append(root / patch["path"] / "Cargo.toml")
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


def refresh_source_closure(lock_path: Path, root: Path) -> None:
    """Atomically replace only the complete, stable source identity closure."""

    root = root.resolve(strict=True)
    lock_path = lock_path.expanduser().absolute()
    if lock_path.is_symlink() or not lock_path.is_file():
        raise ReleaseBuildError("Python release lock must be one regular file")
    lock_before = lock_path.stat(follow_symlinks=False)
    try:
        raw = lock_path.read_bytes()
        value = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseBuildError("Python release lock is unreadable") from error
    if (
        not raw
        or len(raw) > MAX_LOCK_BYTES
        or not isinstance(value, dict)
        or set(value)
        != {"schema_version", "python", "artifacts", "sources", "inventory_generation"}
        or value.get("schema_version") != 3
    ):
        raise ReleaseBuildError("Python release lock shape is invalid")

    entries = []
    identities: list[tuple[Path, tuple[int, int, int, int, int]]] = []
    for relative in expected_source_paths(root):
        candidate = root.joinpath(*PurePosixPath(relative).parts)
        if candidate.is_symlink() or not candidate.is_file():
            raise ReleaseBuildError("Python release source is unavailable")
        before = candidate.stat(follow_symlinks=False)
        size, digest, _header, _file_identity = _inspect_installed_distribution_file(
            candidate
        )
        identity = (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_size,
            before.st_mtime_ns,
        )
        identities.append((candidate, identity))
        entries.append({"path": relative, "sha256": digest, "size_bytes": size})

    for candidate, expected in identities:
        observed = candidate.stat(follow_symlinks=False)
        if candidate.is_symlink() or (
            observed.st_dev,
            observed.st_ino,
            observed.st_mode,
            observed.st_size,
            observed.st_mtime_ns,
        ) != expected:
            raise ReleaseBuildError("Python release source changed during refresh")
    current = lock_path.stat(follow_symlinks=False)
    if (
        current.st_dev,
        current.st_ino,
        current.st_mode,
        current.st_size,
        current.st_mtime_ns,
    ) != (
        lock_before.st_dev,
        lock_before.st_ino,
        lock_before.st_mode,
        lock_before.st_size,
        lock_before.st_mtime_ns,
    ):
        raise ReleaseBuildError("Python release lock changed during refresh")

    value["sources"] = entries
    _atomic_write_json(lock_path, value)


def refresh_development_source_closure(
    template_path: Path,
    development_root: Path,
    repository_root: Path,
) -> Path:
    """Create a development lock with current sources without changing the shipping lock."""

    repository_root = repository_root.resolve(strict=True)
    expected_template = repository_root / "python/wheelhouse-lock.json"
    template_candidate = template_path.expanduser().absolute()
    if template_candidate != expected_template or template_candidate.is_symlink():
        raise ReleaseBuildError("development source template is not the shipping lock")
    template_path = template_candidate.resolve(strict=True)
    before = template_path.stat(follow_symlinks=False)
    raw = template_path.read_bytes()
    if not raw or len(raw) > MAX_LOCK_BYTES:
        raise ReleaseBuildError("Python release lock exceeds its byte bound")

    destination = development_root / "source-lock.json"
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        raise ReleaseBuildError("development source lock path is unsafe")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".source-lock.", suffix=".refresh", dir=development_root
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            if os.name != "nt":
                os.fchmod(output.fileno(), 0o600)
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        refresh_source_closure(temporary, repository_root)
        after = template_path.stat(follow_symlinks=False)
        if template_path.is_symlink() or (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_size,
            after.st_mtime_ns,
        ) != (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_size,
            before.st_mtime_ns,
        ):
            raise ReleaseBuildError("shipping source lock changed during development refresh")
        os.replace(temporary, destination)
        if os.name != "nt":
            destination.chmod(0o600)
            directory = os.open(
                development_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def admit_development_source_template(lock_path: Path, repository_root: Path) -> None:
    """Require the development lock to differ from shipping only in source identities."""

    development = _load_refresh_lock(lock_path)
    shipping = _load_refresh_lock(repository_root / "python/wheelhouse-lock.json")
    development = dict(development)
    shipping = dict(shipping)
    development.pop("sources", None)
    shipping.pop("sources", None)
    if development != shipping:
        raise ReleaseBuildError("development lock differs from the shipping dependency authority")


def _atomic_write_json(path: Path, value: object) -> None:
    encoded = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    if not encoded or len(encoded) > MAX_LOCK_BYTES:
        raise ReleaseBuildError("refreshed release manifest exceeds its byte bound")
    destination = path.stat(follow_symlinks=False)
    if path.is_symlink() or not stat.S_ISREG(destination.st_mode):
        raise ReleaseBuildError("release manifest destination is unsafe")
    destination_mode = stat.S_IMODE(destination.st_mode)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".refresh", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            if os.name != "nt":
                os.fchmod(output.fileno(), destination_mode)
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        if path.is_symlink():
            raise ReleaseBuildError("release manifest destination became unsafe")
        os.replace(temporary, path)
        if os.name != "nt":
            directory = os.open(
                path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def refresh_lock_manifests(
    requirements_path: Path,
    lock_path: Path,
    targets: tuple[str, ...],
    root: Path,
) -> None:
    """Resolve exact wheel metadata into one fail-closed four-target lock generation."""

    if len(targets) != len(PLATFORM_PROFILES) or set(targets) != set(PLATFORM_PROFILES):
        raise ReleaseBuildError("lock refresh requires the exact supported target matrix")
    targets = tuple(PLATFORM_PROFILES)
    requirements = _locked_requirement_set(requirements_path)
    build_system = _toml(root / "python/pyproject.toml").get("build-system")
    build_requirements = (
        build_system.get("requires") if isinstance(build_system, dict) else None
    )
    if build_requirements != ["maturin==1.14.1"]:
        raise ReleaseBuildError("Python build-system requirement is not exact")
    requirements["maturin"] = ("1.14.1", None, frozenset())
    if set(requirements) != REQUIRED_PROJECTS:
        raise ReleaseBuildError("resolved Python project set is incomplete or unexpected")

    metadata = {
        name: _pypi_release_metadata(name, version)
        for name, (version, _marker, _hashes) in sorted(requirements.items())
    }
    selected_by_target: dict[str, tuple[dict[str, object], ...]] = {}
    selected_union: dict[str, dict[str, object]] = {}
    metadata_by_filename: dict[str, object] = {}
    with tempfile.TemporaryDirectory(prefix="market-squawk-wheel-refresh-") as temporary:
        cache = Path(temporary)
        for target in targets:
            profile = platform_profile(target)
            environment = _target_marker_environment(profile)
            selected = []
            for name, (version, marker, hashes) in sorted(requirements.items()):
                if marker is not None and not marker.evaluate(environment=environment):
                    continue
                artifact = _select_target_wheel(
                    name,
                    version,
                    metadata[name],
                    hashes,
                    profile,
                )
                filename = str(artifact["filename"])
                if filename not in selected_union:
                    inspected, wheel_metadata = _inspect_selected_wheel(
                        artifact,
                        PYTHON_LICENSE_POLICY[name],
                        cache,
                    )
                    selected_union[filename] = inspected
                    metadata_by_filename[filename] = wheel_metadata
                selected.append(selected_union[filename])
            selected_by_target[target] = tuple(
                sorted(selected, key=lambda value: str(value["filename"]))
            )

    for target, selected in selected_by_target.items():
        _validate_resolved_dependency_closure(
            selected,
            metadata_by_filename,
            _target_marker_environment(platform_profile(target)),
        )
    old = _load_refresh_lock(lock_path)
    master = {
        "artifacts": [selected_union[name] for name in sorted(selected_union)],
        "inventory_generation": "",
        "python": {"maximum_exclusive": "3.15", "minimum": "3.14"},
        "schema_version": 3,
        "sources": old["sources"],
    }
    generation_input = {
        "artifacts": master["artifacts"],
        "targets": {
            target: [value["filename"] for value in selected_by_target[target]]
            for target in targets
        },
    }
    generation = _mapping_sha256(generation_input)
    master["inventory_generation"] = generation
    inventories = {}
    for target in targets:
        profile = platform_profile(target)
        inventories[lock_path.parent / "wheelhouse" / f"{target}.json"] = {
            "artifacts": [value["filename"] for value in selected_by_target[target]],
            "inventory_generation": generation,
            "minimum_system": profile.minimum_system,
            "schema_version": 2,
            "target": target,
            "wheel_platform_tag": profile.wheel_platform_tag,
        }
    for path, value in inventories.items():
        _atomic_write_json(path, value)
    _atomic_write_json(lock_path, master)
    for target in targets:
        load_lock(lock_path, target)


def _load_refresh_lock(path: Path) -> dict[str, object]:
    try:
        if path.is_symlink():
            raise ReleaseBuildError("Python release lock must not be a symbolic link")
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseBuildError("Python release lock is unreadable") from error
    if (
        not raw
        or len(raw) > MAX_LOCK_BYTES
        or not isinstance(value, dict)
        or value.get("schema_version") not in {2, 3}
        or not isinstance(value.get("sources"), list)
    ):
        raise ReleaseBuildError("Python release lock shape is invalid")
    return value


def _locked_requirement_set(
    path: Path,
) -> dict[str, tuple[str, object | None, frozenset[str]]]:
    try:
        import packaging
        from packaging.requirements import InvalidRequirement, Requirement
    except ImportError as error:
        raise ReleaseBuildError("packaging 26.2 is required for lock refresh") from error
    if packaging.__version__ != PACKAGING_VERSION:
        raise ReleaseBuildError("lock refresh requires exact packaging 26.2")
    try:
        if path.is_symlink():
            raise ReleaseBuildError("requirements lock must not be a symbolic link")
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise ReleaseBuildError("requirements lock is unreadable") from error
    if not raw or len(raw.encode("utf-8")) > MAX_LOCK_BYTES:
        raise ReleaseBuildError("requirements lock exceeds its byte bound")
    blocks: list[list[str]] = []
    current: list[str] | None = None
    for line in raw.splitlines():
        if line and not line[0].isspace() and not line.startswith("#"):
            current = [line]
            blocks.append(current)
        elif current is not None and ("--hash=" in line or line.lstrip().startswith("#")):
            current.append(line)
    result = {}
    for block in blocks:
        requirement_text = block[0].removesuffix("\\").strip()
        try:
            requirement = Requirement(requirement_text)
        except InvalidRequirement as error:
            raise ReleaseBuildError("requirements lock contains an invalid requirement") from error
        name = _normalize_project_name(requirement.name)
        specifiers = tuple(requirement.specifier)
        if requirement.url is not None or requirement.extras or len(specifiers) != 1:
            raise ReleaseBuildError("requirements lock is not exact")
        specifier = specifiers[0]
        if specifier.operator != "==" or "*" in specifier.version:
            raise ReleaseBuildError("requirements lock is not exact")
        hashes = frozenset(
            match.group(1)
            for line in block
            if (match := re.search(r"--hash=sha256:([0-9a-f]{64})", line))
        )
        if not hashes or name in result:
            raise ReleaseBuildError("requirements lock hash set is incomplete")
        result[name] = (specifier.version, requirement.marker, hashes)
    return result


def _pypi_release_metadata(name: str, version: str) -> dict[str, object]:
    url = f"https://pypi.org/pypi/{urllib.parse.quote(name)}/{urllib.parse.quote(version)}/json"
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "market-squawk-release-builder"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.geturl() != url:
                raise ReleaseBuildError("PyPI metadata redirected unexpectedly")
            payload = response.read(MAX_LOCK_BYTES + 1)
    except (OSError, urllib.error.URLError) as error:
        raise ReleaseBuildError("PyPI release metadata is unavailable") from error
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseBuildError("PyPI release metadata is invalid") from error
    if (
        len(payload) > MAX_LOCK_BYTES
        or not isinstance(value, dict)
        or not isinstance(value.get("info"), dict)
        or value["info"].get("version") != version
        or not isinstance(value.get("urls"), list)
    ):
        raise ReleaseBuildError("PyPI release metadata identity is invalid")
    return value


def _select_target_wheel(
    name: str,
    version: str,
    release: dict[str, object],
    requirement_hashes: frozenset[str],
    profile: PlatformProfile,
) -> dict[str, object]:
    try:
        from packaging.specifiers import InvalidSpecifier, SpecifierSet
        from packaging.version import Version
    except ImportError as error:
        raise ReleaseBuildError("packaging 26.2 is required for lock refresh") from error
    supported = _ordered_supported_wheel_tags((3, 14), profile)
    ranking = {tag: index for index, tag in enumerate(supported)}
    candidates = []
    for value in release["urls"]:
        if not isinstance(value, dict) or value.get("packagetype") != "bdist_wheel":
            continue
        filename = value.get("filename")
        if not isinstance(filename, str):
            continue
        parsed_name, parsed_version, build, tags = _parse_wheel(filename)
        if (
            _normalize_project_name(str(parsed_name)) != name
            or parsed_version != Version(version)
            or parsed_version.is_prerelease
            or build
            or value.get("yanked") is not False
        ):
            continue
        matching = tags.intersection(ranking)
        if not matching:
            continue
        requires_python = value.get("requires_python") or release["info"].get("requires_python")
        try:
            supported_python = SpecifierSet(str(requires_python)).contains(
                "3.14.6", prereleases=False
            )
        except InvalidSpecifier as error:
            raise ReleaseBuildError("wheel Requires-Python is invalid") from error
        digest = value.get("digests", {}).get("sha256")
        core_metadata = value.get("core-metadata")
        if (
            not supported_python
            or not isinstance(digest, str)
            or (requirement_hashes and digest not in requirement_hashes)
            or not isinstance(core_metadata, dict)
            or not isinstance(core_metadata.get("sha256"), str)
        ):
            continue
        candidates.append(
            ((min(ranking[tag] for tag in matching), len(tags)), value, tags)
        )
    if not candidates:
        raise ReleaseBuildError(f"{name} has no admitted wheel for {profile.target}")
    candidates.sort(key=lambda item: item[0])
    if len(candidates) > 1 and candidates[0][0] == candidates[1][0]:
        raise ReleaseBuildError(f"{name} has ambiguous wheels for {profile.target}")
    selected = dict(candidates[0][1])
    selected["parsed_tags"] = tuple(sorted(str(tag) for tag in candidates[0][2]))
    return selected


def _target_marker_environment(profile: PlatformProfile) -> dict[str, str]:
    if profile.system == "Darwin":
        machine = "arm64" if profile.target.startswith("aarch64") else "x86_64"
        os_name, sys_platform = "posix", "darwin"
    elif profile.system == "Windows":
        machine, os_name, sys_platform = "AMD64", "nt", "win32"
    else:
        machine, os_name, sys_platform = "x86_64", "posix", "linux"
    return {
        "implementation_name": "cpython",
        "implementation_version": "3.14.6",
        "os_name": os_name,
        "platform_machine": machine,
        "platform_python_implementation": "CPython",
        "platform_release": "",
        "platform_system": profile.system,
        "platform_version": "",
        "python_full_version": "3.14.6",
        "python_version": "3.14",
        "sys_platform": sys_platform,
        "extra": "",
    }


def _inspect_selected_wheel(
    artifact: dict[str, object],
    license_policy: str,
    cache: Path,
) -> tuple[dict[str, object], object]:
    filename = str(artifact["filename"])
    destination = cache / filename
    _download_locked_wheel(artifact, destination)
    try:
        with zipfile.ZipFile(destination) as archive:
            names = archive.namelist()
            metadata_names = [name for name in names if name.endswith(".dist-info/METADATA")]
            record_names = [name for name in names if name.endswith(".dist-info/RECORD")]
            if len(metadata_names) != 1 or len(record_names) != 1:
                raise ReleaseBuildError("wheel metadata or RECORD is not unique")
            metadata_payload = archive.read(metadata_names[0])
            expected_metadata = artifact["core-metadata"]["sha256"]
            if hashlib.sha256(metadata_payload).hexdigest() != expected_metadata:
                raise ReleaseBuildError("wheel metadata digest differs from PyPI")
            metadata = BytesParser().parsebytes(metadata_payload)
            observed_expression = metadata.get("License-Expression")
            if observed_expression is not None and observed_expression.strip() != license_policy:
                raise ReleaseBuildError("wheel SPDX expression differs from policy")
            license_files = _inspect_wheel_license_files(
                archive,
                metadata_names[0],
                metadata,
            )
            _validate_wheel_record(archive, record_names[0])
    except (OSError, zipfile.BadZipFile, KeyError) as error:
        raise ReleaseBuildError("selected wheel is unreadable") from error
    url = str(artifact["url"])
    parsed = urllib.parse.urlparse(url)
    digest = str(artifact["digests"]["sha256"])
    if (
        parsed.scheme != "https"
        or parsed.hostname != "files.pythonhosted.org"
        or Path(parsed.path).name != filename
        or artifact.get("yanked") is not False
    ):
        raise ReleaseBuildError("selected wheel origin is invalid")
    project, version, _build, _tags = _parse_wheel(filename)
    value = {
        "filename": filename,
        "license": license_policy,
        "license_files": license_files,
        "metadata_sha256": str(artifact["core-metadata"]["sha256"]),
        "project": _normalize_project_name(str(project)),
        "requires_python": str(artifact.get("requires_python") or ""),
        "sha256": digest,
        "size_bytes": int(artifact["size"]),
        "tags": list(artifact["parsed_tags"]),
        "upload_time": str(artifact.get("upload_time_iso_8601") or ""),
        "url": url,
        "version": str(version),
        "yanked": False,
    }
    return value, metadata


def _download_locked_wheel(artifact: dict[str, object], destination: Path) -> None:
    expected_size = artifact.get("size")
    expected_digest = artifact.get("digests", {}).get("sha256")
    if (
        not isinstance(expected_size, int)
        or expected_size <= 0
        or expected_size > MAX_DISTRIBUTION_FILE_BYTES
        or not isinstance(expected_digest, str)
    ):
        raise ReleaseBuildError("selected wheel identity is invalid")
    request = urllib.request.Request(
        str(artifact["url"]),
        headers={
            "Accept": "application/octet-stream",
            "User-Agent": "market-squawk-release-builder",
        },
    )
    digest = hashlib.sha256()
    observed = 0
    try:
        with urllib.request.urlopen(request, timeout=60) as response, destination.open(
            "xb"
        ) as output:
            final = urllib.parse.urlparse(response.geturl())
            if final.scheme != "https" or final.hostname != "files.pythonhosted.org":
                raise ReleaseBuildError("selected wheel redirected outside PyPI files")
            while chunk := response.read(1024 * 1024):
                observed += len(chunk)
                if observed > expected_size:
                    raise ReleaseBuildError("selected wheel exceeded its locked size")
                digest.update(chunk)
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
    except (OSError, urllib.error.URLError):
        destination.unlink(missing_ok=True)
        raise
    if observed != expected_size or digest.hexdigest() != expected_digest:
        destination.unlink(missing_ok=True)
        raise ReleaseBuildError("selected wheel identity differs")


def _inspect_wheel_license_files(
    archive: zipfile.ZipFile,
    metadata_name: str,
    metadata: object,
) -> list[dict[str, object]]:
    dist_info = PurePosixPath(metadata_name).parent
    declared = metadata.get_all("License-File", [])
    candidates = []
    if declared:
        for value in declared:
            relative = PurePosixPath(value)
            if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
                raise ReleaseBuildError("wheel license path is invalid")
            candidates.append((dist_info / "licenses" / relative).as_posix())
    else:
        for name in archive.namelist():
            path = PurePosixPath(name)
            if (
                not name.endswith("/")
                and dist_info in path.parents
                and path.name.upper().startswith(("LICENSE", "COPYING", "NOTICE"))
            ):
                candidates.append(name)
    candidates = sorted(set(candidates))
    if not candidates:
        raise ReleaseBuildError("wheel contains no declared license material")
    result = []
    for name in candidates:
        try:
            payload = archive.read(name)
        except KeyError as error:
            raise ReleaseBuildError("declared wheel license file is absent") from error
        if not payload or len(payload) > MAX_RECORD_BYTES:
            raise ReleaseBuildError("wheel license material exceeds its byte bound")
        result.append(
            {
                "path": name,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
            }
        )
    return result


def _validate_wheel_record(archive: zipfile.ZipFile, record_name: str) -> None:
    try:
        rows = list(csv.reader(io.TextIOWrapper(archive.open(record_name), encoding="utf-8")))
    except (UnicodeError, csv.Error) as error:
        raise ReleaseBuildError("wheel RECORD is unreadable") from error
    if any(len(row) != 3 or not row[0] for row in rows):
        raise ReleaseBuildError("wheel RECORD contains a malformed entry")
    records = {row[0]: row[1:] for row in rows}
    if len(records) != len(rows):
        raise ReleaseBuildError("wheel RECORD contains a duplicate entry")
    files = {name for name in archive.namelist() if name and not name.endswith("/")}
    if set(records) != files:
        raise ReleaseBuildError("wheel RECORD does not cover the exact archive")
    for name in sorted(files - {record_name}):
        digest_value, size_value = records[name]
        if not digest_value.startswith("sha256=") or not size_value.isdigit():
            raise ReleaseBuildError("wheel RECORD entry lacks SHA-256 or size")
        payload = archive.read(name)
        encoded = base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b"=").decode()
        if digest_value != f"sha256={encoded}" or int(size_value) != len(payload):
            raise ReleaseBuildError("wheel RECORD entry identity differs")


def _validate_resolved_dependency_closure(
    artifacts: tuple[dict[str, object], ...],
    metadata_by_filename: dict[str, object],
    environment: dict[str, str],
) -> None:
    try:
        from packaging.requirements import InvalidRequirement, Requirement
        from packaging.version import Version
    except ImportError as error:
        raise ReleaseBuildError("packaging 26.2 is required for lock refresh") from error
    installed = {
        str(value["project"]): Version(str(value["version"])) for value in artifacts
    }
    for value in artifacts:
        metadata = metadata_by_filename[str(value["filename"])]
        for raw_requirement in metadata.get_all("Requires-Dist", []):
            try:
                requirement = Requirement(raw_requirement)
            except InvalidRequirement as error:
                raise ReleaseBuildError("wheel dependency metadata is invalid") from error
            if requirement.marker is not None and not requirement.marker.evaluate(
                environment=environment
            ):
                continue
            dependency = _normalize_project_name(requirement.name)
            observed = installed.get(dependency)
            if observed is None or observed not in requirement.specifier:
                raise ReleaseBuildError("selected wheel dependency closure is incomplete")


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


def admit_toolchain(
    root: Path,
    profile: PlatformProfile | None = None,
    zig_executable: Path | None = None,
) -> dict[str, object]:
    """Bind direct Rust and native host tools for one exact release target."""

    profile = profile or host_profile()
    if host_profile() != profile:
        raise ReleaseBuildError("release target does not match the native build host")
    configured = _toml(root / "rust-toolchain.toml")["toolchain"]["channel"]
    rust_version = _toml(root / "Cargo.toml")["workspace"]["package"]["rust-version"]
    if configured != RUST_TOOLCHAIN or rust_version != RUST_TOOLCHAIN:
        raise ReleaseBuildError("repository Rust policy is not pinned to 1.97.1")
    evidence_environment = {"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"}
    rustup_discovered = shutil.which("rustup")
    if rustup_discovered is None:
        raise ReleaseBuildError("rustup is required to resolve direct pinned Rust tools")
    rustup_launcher = Path(rustup_discovered).absolute()
    expected_rustup_name = "rustup.exe" if profile.system == "Windows" else "rustup"
    rustup = rustup_launcher.resolve(strict=True)
    if (
        rustup_launcher.name.casefold() != expected_rustup_name.casefold()
        or not rustup.is_file()
    ):
        raise ReleaseBuildError("rustup launcher identity is invalid")
    cargo_path = _direct_rust_tool(
        rustup_launcher, "cargo", profile, root, evidence_environment
    )
    rustc_path = _direct_rust_tool(
        rustup_launcher, "rustc", profile, root, evidence_environment
    )
    cargo_version = _run_output([str(cargo_path), "-vV"], root, evidence_environment)
    rustc_version = _run_output([str(rustc_path), "-vV"], root, evidence_environment)
    _require_tool_release(cargo_version, "cargo", RUST_TOOLCHAIN)
    _require_tool_release(rustc_version, "rustc", RUST_TOOLCHAIN)

    sysroot = Path(
        _run_output(
            [str(rustc_path), "--print", "sysroot"], root, evidence_environment
        )
    ).resolve(strict=True)
    if sysroot != rustc_path.parent.parent:
        raise ReleaseBuildError("direct rustc and reported sysroot identities differ")
    toolchain = {
        "schema_version": 2,
        "target": profile.target,
        "minimum_system": profile.minimum_system,
        "cargo": _tool_binding(cargo_path, cargo_version),
        "rustc": _tool_binding(rustc_path, rustc_version),
        "rust_stdlib": _tree_binding(sysroot / "lib/rustlib" / profile.target / "lib"),
        "rustup": _tool_binding(
            rustup,
            _run_output([str(rustup_launcher), "--version"], root, evidence_environment),
        ),
    }
    if profile.system == "Darwin":
        toolchain.update(_admit_macos_toolchain(root, evidence_environment))
    elif profile.system == "Linux":
        toolchain.update(
            _admit_linux_toolchain(
                root,
                evidence_environment,
                zig_executable,
            )
        )
    elif profile.system == "Windows":
        toolchain.update(_admit_windows_toolchain())
    else:
        raise ReleaseBuildError("native release toolchain is unsupported")
    return toolchain


def _admit_macos_toolchain(
    root: Path, evidence_environment: dict[str, str]
) -> dict[str, object]:
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
    sdk_settings = tuple(
        _file_binding(path)
        for path in (
            sdk / "SDKSettings.json",
            sdk / "SDKSettings.plist",
            sdk / "System/Library/CoreServices/SystemVersion.plist",
        )
    )
    return {
        "macos_deployment_target": MACOS_DEPLOYMENT_TARGET,
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


def _admit_linux_toolchain(
    root: Path,
    evidence_environment: dict[str, str],
    zig_executable: Path | None,
) -> dict[str, object]:
    if zig_executable is None:
        raise ReleaseBuildError("the locked Linux Zig linker is unavailable")
    zig = zig_executable.expanduser().resolve(strict=True)
    cc = _system_tool("cc")
    cxx = _system_tool("c++")
    archiver = _system_tool("ar")
    ranlib = _system_tool("ranlib")
    return {
        "cc": _tool_binding(
            cc, _run_output([str(cc), "--version"], root, evidence_environment)
        ),
        "cxx": _tool_binding(
            cxx, _run_output([str(cxx), "--version"], root, evidence_environment)
        ),
        "linker": _tool_binding(cc),
        "archiver": _tool_binding(archiver),
        "ranlib": _tool_binding(ranlib),
        "zig": _tool_binding(
            zig,
            _run_output([str(zig), "version"], root, evidence_environment),
        ),
    }


def _admit_windows_toolchain() -> dict[str, object]:
    compiler = _system_tool("cl.exe")
    linker = _system_tool("link.exe")
    archiver = _system_tool("lib.exe")
    assembler = _system_tool("ml64.exe")
    developer_directory_value = os.environ.get("VCINSTALLDIR")
    if not developer_directory_value:
        raise ReleaseBuildError("MSVC developer environment is incomplete")
    developer_directory = Path(developer_directory_value).resolve(strict=True)
    if not developer_directory.is_dir():
        raise ReleaseBuildError("MSVC developer directory is invalid")
    target_architecture = os.environ.get("VSCMD_ARG_TGT_ARCH")
    if target_architecture != "x64":
        raise ReleaseBuildError("MSVC target architecture is not x64")
    return {
        "compiler": _tool_binding(compiler),
        "linker": _tool_binding(linker),
        "archiver": _tool_binding(archiver),
        "assembler": _tool_binding(assembler),
        "developer_directory": str(developer_directory),
        "target_architecture": target_architecture,
    }


def _system_tool(name: str) -> Path:
    discovered = shutil.which(name)
    if discovered is None:
        raise ReleaseBuildError(f"required native tool is unavailable: {name}")
    path = Path(discovered).resolve(strict=True)
    if not path.is_file():
        raise ReleaseBuildError(f"required native tool is invalid: {name}")
    return path


def _direct_rust_tool(
    rustup: Path,
    name: str,
    profile: PlatformProfile,
    root: Path,
    environment: dict[str, str],
) -> Path:
    path = Path(
        _run_output(
            [
                str(rustup),
                "which",
                "--toolchain",
                f"{RUST_TOOLCHAIN}-{profile.target}",
                name,
            ],
            root,
            environment,
        )
    ).resolve(strict=True)
    expected_name = f"{name}.exe" if profile.system == "Windows" else name
    if path.name.casefold() != expected_name.casefold() or not path.is_file():
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
        raise ReleaseBuildError("exactly one CPython 3.14.6 interpreter is required")
    profile = platform_profile(lock.target)
    if host_profile() != profile:
        raise ReleaseBuildError("release target does not match the native build host")
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
            or evidence.get("system") != profile.system
            or not _interpreter_platform_matches(profile, evidence)
            or len(version) != 3
            or version != REQUIRED_PYTHON
            or not lock.minimum <= minor < lock.maximum_exclusive
            or minor not in SUPPORTED_PYTHONS
            or minor in admitted
        ):
            raise ReleaseBuildError(
                "Python interpreter is outside the exact support matrix "
                f"(implementation={evidence.get('implementation')!r}, "
                f"system={evidence.get('system')!r}, "
                f"machine={evidence.get('machine')!r}, "
                f"configured_platform={evidence.get('configured_platform')!r}, "
                f"version={version!r})"
            )
        canonical_paths.add(executable)
        admitted[minor] = PythonRuntime(executable, version)
    if tuple(sorted(admitted)) != SUPPORTED_PYTHONS:
        raise ReleaseBuildError("CPython 3.14.6 was not independently admitted")
    return tuple(admitted[minor] for minor in SUPPORTED_PYTHONS)


def build_release(
    root: Path,
    lock_path: Path,
    lock: ReleaseLock,
    layout: ArtifactLayout,
    runtimes: tuple[PythonRuntime, ...],
    toolchain: dict[str, object],
    release_components_sha256: str,
    uv_executable: Path,
    native_code_signing: NativeCodeSigning | None,
    *,
    development_runtime: bool = False,
) -> None:
    with ExitStack() as cleanup:
        _build_release(
            root,
            lock_path,
            lock,
            layout,
            runtimes,
            toolchain,
            release_components_sha256,
            uv_executable,
            native_code_signing,
            development_runtime,
            cleanup,
        )


def _build_release(
    root: Path,
    lock_path: Path,
    lock: ReleaseLock,
    layout: ArtifactLayout,
    runtimes: tuple[PythonRuntime, ...],
    toolchain: dict[str, object],
    release_components_sha256: str,
    uv_executable: Path,
    native_code_signing: NativeCodeSigning | None,
    development_runtime: bool,
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
    profile = platform_profile(lock.target)
    signer_helper = _cargo_release_dir(root, profile) / (
        f"market-squawk-release-signer{profile.executable_suffix}"
    )
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
        release_components_sha256,
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
    built_executables = _build_native_release_executables(
        root,
        toolchain,
        bootstrap_environment,
        native_code_signing,
    )
    validator_size, validator_sha256 = _file_digest(built_executables.validator)
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
            str(uv_executable),
            "pip",
            "install",
            "--python",
            build_python,
            "--no-config",
            "--offline",
            "--no-cache",
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
    maturin_command = [
        build_python,
        "-I",
        "-m",
        "maturin",
        "build",
        "--release",
        "--locked",
        "--out",
        str(layout.distribution),
    ]
    if profile.maturin_compatibility is not None:
        maturin_command.extend(["--compatibility", profile.maturin_compatibility])
        maturin_command.append("--zig")
    _run(maturin_command, root / "python", environment)
    project_version = _project_version(root)
    project_wheels = list(
        layout.distribution.glob(f"market_squawk-{project_version}-*.whl")
    )
    if len(project_wheels) != 1:
        raise ReleaseBuildError("maturin did not create exactly one project wheel")
    project_wheel = project_wheels[0]
    harden_project_wheel(project_wheel)
    project_tag = _single_wheel_tag(project_wheel.name)
    python_tag = project_tag.interpreter
    abi_tag = project_tag.abi
    platform_tag = project_tag.platform
    if (
        python_tag != "cp310"
        or abi_tag != "abi3"
        or platform_tag != profile.wheel_platform_tag
    ):
        raise ReleaseBuildError(
            "project wheel does not carry the exact pinned cp310-abi3 platform tag"
        )
    prepared_releases = []
    for runtime, (minor, release_venv) in zip(runtimes, layout.releases, strict=True):
        if runtime.version[:2] != minor:
            raise ReleaseBuildError("release environment and interpreter identity differ")
        _reset_owned_child(release_venv, layout.root, f"release-cp{minor[0]}{minor[1]}")
        release_python = _copy_runtime_release(
            runtime,
            release_venv,
            root,
            bootstrap_environment,
        )
        installed_executables = _copy_native_release_executables(
            built_executables,
            release_venv,
        )
        prepared_releases.append(
            (
                runtime,
                minor,
                release_venv,
                release_python,
                installed_executables,
            )
        )
    if (
        len(prepared_releases) != 1
        or prepared_releases[0][1] != (3, 14)
        or prepared_releases[0][2] != layout.root / CANONICAL_RELEASE
    ):
        raise ReleaseBuildError("canonical CPython 3.14 release is unavailable")
    canonical_executables = prepared_releases[0][4]
    application_size, application_sha256 = _file_digest(
        canonical_executables.application
    )
    onnx_worker_size, onnx_worker_sha256 = _file_digest(
        canonical_executables.onnx_worker
    )
    canonical_validator_size, canonical_validator_sha256 = _file_digest(
        canonical_executables.validator
    )
    training_driver_size, training_driver_sha256 = _file_digest(
        canonical_executables.training_driver
    )
    if (
        (canonical_validator_size, canonical_validator_sha256)
        != (validator_size, validator_sha256)
    ):
        raise ReleaseBuildError("canonical release validator identity changed")
    release_manifest, release_manifest_sha256 = build_release_manifest(
        foundation_sha256,
        project_wheel,
        python_tag,
        abi_tag,
        platform_tag,
        prepared_releases[0][2],
        profile,
        signer,
    )
    (layout.root / "market-squawk-release.json").write_bytes(release_manifest)
    matrix_evidence = []
    signed_runtime_paths: list[Path] = []
    for runtime, minor, release_venv, release_python, installed_executables in prepared_releases:
        runtime_environment = dict(bootstrap_environment)
        _run(
            [
                str(uv_executable),
                "pip",
                "install",
                "--python",
                release_python,
                "--prefix",
                str(release_venv),
                "--no-config",
                "--offline",
                "--no-cache",
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
                str(uv_executable),
                "pip",
                "install",
                "--python",
                release_python,
                "--prefix",
                str(release_venv),
                "--no-config",
                "--offline",
                "--no-cache",
                "--no-index",
                "--no-deps",
                str(project_wheel),
            ],
            root,
            runtime_environment,
        )
        install_native_training_driver(
            release_venv,
            runtime,
            built_executables.training_driver,
            project_version,
        )
        signed_runtime_paths.extend(
            sign_release_tree(
                release_venv,
                runtime,
                native_code_signing,
                installed_executables,
            )
        )
        if (
            _file_digest(installed_executables.application)
            != (application_size, application_sha256)
            or _file_digest(installed_executables.onnx_worker)
            != (onnx_worker_size, onnx_worker_sha256)
            or _file_digest(installed_executables.training_driver)
            != (training_driver_size, training_driver_sha256)
            or _file_digest(installed_executables.validator)
            != (canonical_validator_size, canonical_validator_sha256)
        ):
            raise ReleaseBuildError("installed native release identity changed")
        distribution = inspect_installed_distribution(
            release_venv,
            runtime,
            RuntimeRequirement("market-squawk", project_version),
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
                "-B",
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
                "-B",
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
                "release_directory": f"release-cp{minor[0]}{minor[1]}",
                "python_tag": f"cp{runtime.version[0]}{runtime.version[1]}",
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
                "training_driver_sha256": training_driver_sha256,
            }
        )
    evidence = {
        "schema_version": 6,
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
            "minimum_system": profile.minimum_system,
            "target": profile.target,
        },
        "native_executables": {
            "application_sha256": application_sha256,
            "onnx_worker_sha256": onnx_worker_sha256,
            "training_driver_sha256": training_driver_sha256,
            "validator_sha256": validator_sha256,
        },
        "native_code_signing": (
            native_code_signing.evidence()
            if native_code_signing is not None
            else {
                "status": "not-requested",
                "system": profile.system,
                "timestamped": False,
            }
        ),
        "signed_runtime_file_count": len(signed_runtime_paths),
    }
    (layout.root / "market-squawk-release-evidence.json").write_text(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    if development_runtime:
        _write_development_runtime_receipt(
            root,
            layout.root.parent,
            layout,
            lock_path,
            profile,
        )
    _remove_owned_child(layout.build_venv, layout.root, "build-venv")
    _remove_owned_child(layout.build_home, layout.root, "build-home")


def _build_native_release_executables(
    root: Path,
    toolchain: dict[str, object],
    environment: dict[str, str],
    native_code_signing: NativeCodeSigning | None = None,
) -> NativeReleaseExecutables:
    profile = platform_profile(str(toolchain.get("target")))
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
            "--bin",
            "market-squawk-train",
            "--no-default-features",
            "--features",
            "market-squawk/release-evidence",
            "--release",
            "--locked",
        ],
        root,
        environment,
    )
    release = _cargo_release_dir(root, profile)
    suffix = profile.executable_suffix
    executables = NativeReleaseExecutables(
        application=release / f"market-squawk{suffix}",
        onnx_worker=release / f"market-squawk-onnx-worker{suffix}",
        training_driver=release / f"market-squawk-train{suffix}",
        validator=release / f"market-squawk-model-validator{suffix}",
    )
    if any(
        path.is_symlink() or not path.is_file()
        for path in (
            executables.application,
            executables.onnx_worker,
            executables.training_driver,
            executables.validator,
        )
    ):
        raise ReleaseBuildError("bound Rust release executables were not produced")
    sign_native_files(
        (
            executables.application,
            executables.onnx_worker,
            executables.training_driver,
            executables.validator,
        ),
        native_code_signing,
    )
    return executables


def _development_runtime_receipt(
    root: Path,
    development_root: Path,
    layout: ArtifactLayout,
    lock_path: Path,
    profile: PlatformProfile,
) -> dict[str, object]:
    suffix = profile.executable_suffix
    native_bin = _cargo_release_dir(root, profile)
    release_bin = layout.root / CANONICAL_RELEASE / "bin"
    program_paths = {
        "application": native_bin / f"market-squawk{suffix}",
        "onnx_worker": native_bin / f"market-squawk-onnx-worker{suffix}",
    }
    programs = {}
    for name, path in program_paths.items():
        maximum_bytes = (
            MAX_ONNX_WORKER_EXECUTABLE_BYTES
            if name == "onnx_worker"
            else MAX_APPLICATION_EXECUTABLE_BYTES
        )
        if (
            path.is_symlink()
            or not path.is_file()
            or path.stat().st_size == 0
            or path.stat().st_size > maximum_bytes
        ):
            raise ReleaseBuildError("development runtime program is unavailable")
        size_bytes, sha256 = _file_digest(path)
        programs[name] = {
            "path": path.relative_to(root).as_posix(),
            "sha256": sha256,
            "size_bytes": size_bytes,
        }
    installed_application = release_bin / f"market-squawk{suffix}"
    installed_worker = release_bin / f"market-squawk-onnx-worker{suffix}"
    if (
        installed_application.is_symlink()
        or not installed_application.is_file()
        or installed_application.stat().st_size == 0
        or installed_application.stat().st_size > MAX_APPLICATION_EXECUTABLE_BYTES
        or installed_worker.is_symlink()
        or not installed_worker.is_file()
        or installed_worker.stat().st_size == 0
        or installed_worker.stat().st_size > MAX_ONNX_WORKER_EXECUTABLE_BYTES
    ):
        raise ReleaseBuildError("development runtime installed program is unavailable")
    if (
        _file_digest(installed_application)
        != _file_digest(program_paths["application"])
        or _file_digest(installed_worker) != _file_digest(program_paths["onnx_worker"])
    ):
        raise ReleaseBuildError("development runtime sibling identity differs")
    return {
        "foundation_sha256": _file_digest(layout.root / "training-foundation.json")[1],
        "programs": programs,
        "release_manifest_sha256": _file_digest(
            layout.root / "market-squawk-release.json"
        )[1],
        "release_root": (layout.root / CANONICAL_RELEASE)
        .relative_to(development_root)
        .as_posix(),
        "schema_version": 2,
        "source_lock_sha256": _file_digest(lock_path)[1],
        "target": profile.target,
    }


def _write_development_runtime_receipt(
    root: Path,
    development_root: Path,
    layout: ArtifactLayout,
    lock_path: Path,
    profile: PlatformProfile,
) -> None:
    destination = development_root / "runtime.json"
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        raise ReleaseBuildError("development runtime receipt path is unsafe")
    if not destination.exists():
        descriptor = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(descriptor)
    _atomic_write_json(
        destination,
        _development_runtime_receipt(
            root,
            development_root,
            layout,
            lock_path,
            profile,
        ),
    )


def _cargo_release_dir(root: Path, profile: PlatformProfile) -> Path:
    return root / "target" / profile.target / "release"


def admit_development_runtime(
    root: Path,
    development_root: Path,
    lock: ReleaseLock,
) -> None:
    """Verify the cached release and its training-authority programs."""

    artifact_root = development_root / "python"
    canonical_release = artifact_root / CANONICAL_RELEASE
    if (
        not artifact_root.is_dir()
        or artifact_root.is_symlink()
        or not (artifact_root / ROOT_MARKER).is_file()
        or not canonical_release.is_dir()
        or canonical_release.is_symlink()
        or not (canonical_release / CHILD_MARKER).is_file()
    ):
        raise ReleaseBuildError("development model runtime is not materialized")
    layout = admit_artifact_root(artifact_root, root)
    _admit_owned_child(canonical_release, layout.root, CANONICAL_RELEASE)

    profile = platform_profile(lock.target)
    suffix = profile.executable_suffix
    installed_bin = canonical_release / "bin"
    native_bin = _cargo_release_dir(root, profile)
    installed_application = installed_bin / f"market-squawk{suffix}"
    installed_worker = installed_bin / f"market-squawk-onnx-worker{suffix}"
    native_application = native_bin / f"market-squawk{suffix}"
    native_worker = native_bin / f"market-squawk-onnx-worker{suffix}"
    receipt_path = development_root / "runtime.json"
    required = (
        receipt_path,
        artifact_root / "training-foundation.json",
        artifact_root / "market-squawk-release.json",
        artifact_root / "market-squawk-release-evidence.json",
        canonical_release / "share/market-squawk/training-environment.json",
        canonical_release / "share/market-squawk/market-squawk-release.json",
        installed_application,
        installed_worker,
        installed_bin / f"market-squawk-model-validator{suffix}",
        installed_bin / f"market-squawk-train{suffix}",
        native_application,
        native_worker,
    )
    if any(path.is_symlink() or not path.is_file() for path in required):
        raise ReleaseBuildError("development model runtime is incomplete")
    receipt_size = receipt_path.stat().st_size
    if receipt_size == 0 or receipt_size > 64 * 1024:
        raise ReleaseBuildError("development runtime receipt exceeds its byte bound")
    try:
        raw_receipt = receipt_path.read_bytes()
        receipt = json.loads(raw_receipt)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseBuildError("development runtime receipt is unreadable") from error
    expected_receipt = _development_runtime_receipt(
        root,
        development_root,
        layout,
        development_root / "source-lock.json",
        profile,
    )
    if len(raw_receipt) != receipt_size or receipt != expected_receipt:
        raise ReleaseBuildError("development runtime receipt identity differs")


def _copy_native_release_executables(
    executables: NativeReleaseExecutables,
    release_root: Path,
) -> NativeReleaseExecutables:
    profile = host_profile()
    suffix = profile.executable_suffix
    native_bin = release_root / "bin"
    if native_bin.is_symlink() or (
        native_bin.exists() and not native_bin.is_dir()
    ):
        raise ReleaseBuildError("release executable directory is invalid")
    native_bin.mkdir(exist_ok=True)
    installed = NativeReleaseExecutables(
        application=native_bin / f"market-squawk{suffix}",
        onnx_worker=native_bin / f"market-squawk-onnx-worker{suffix}",
        training_driver=release_root / _training_driver_path(profile),
        validator=native_bin / f"market-squawk-model-validator{suffix}",
    )
    installed.training_driver.parent.mkdir(exist_ok=True)
    for source, destination in (
        (executables.application, installed.application),
        (executables.onnx_worker, installed.onnx_worker),
        (executables.training_driver, installed.training_driver),
        (executables.validator, installed.validator),
    ):
        if (
            source.is_symlink()
            or not source.is_file()
            or destination.exists()
            or destination.is_symlink()
        ):
            raise ReleaseBuildError("native release executable identity is invalid")
        expected_identity = _file_digest(source)
        shutil.copyfile(source, destination)
        if os.name != "nt":
            destination.chmod(0o555)
        if _file_digest(destination) != expected_identity:
            raise ReleaseBuildError("installed native release identity changed")
    return installed


def install_native_training_driver(
    release_root: Path,
    runtime: PythonRuntime,
    source: Path,
    project_version: str,
) -> None:
    """Replace the path-bound wheel script with the signed relocatable launcher."""

    profile = host_profile()
    destination = release_root / _training_driver_path(profile)
    site_packages = _site_packages_path(release_root, runtime, profile)
    record_candidates = tuple(
        site_packages.glob(f"market_squawk-{project_version}.dist-info/RECORD")
    )
    if (
        source.is_symlink()
        or not source.is_file()
        or source.stat().st_size == 0
        or source.stat().st_size > MAX_TRAINING_LAUNCHER_BYTES
        or destination.is_symlink()
        or not destination.is_file()
        or len(record_candidates) != 1
    ):
        raise ReleaseBuildError("installed training launcher identity is invalid")
    record = record_candidates[0]
    if record.is_symlink() or not record.is_file() or record.stat().st_size > MAX_RECORD_BYTES:
        raise ReleaseBuildError("installed project RECORD is invalid")

    training_record_path = os.path.relpath(destination, site_packages).replace(os.sep, "/")
    replacement = destination.with_name(f".{destination.name}.native")
    if replacement.exists() or replacement.is_symlink():
        raise ReleaseBuildError("training launcher replacement path already exists")
    expected_size, expected_sha256 = _file_digest(source)
    try:
        shutil.copyfile(source, replacement)
        if os.name != "nt":
            replacement.chmod(0o555)
        if _file_digest(replacement) != (expected_size, expected_sha256):
            raise ReleaseBuildError("training launcher changed during installation")
        os.replace(replacement, destination)

        rows = []
        matched = 0
        with record.open("r", encoding="utf-8", newline="") as stream:
            for row in csv.reader(stream):
                if len(row) != 3:
                    raise ReleaseBuildError("installed project RECORD is malformed")
                if row[0] == training_record_path:
                    matched += 1
                    encoded = base64.urlsafe_b64encode(
                        bytes.fromhex(expected_sha256)
                    ).rstrip(b"=").decode("ascii")
                    row = [training_record_path, f"sha256={encoded}", str(expected_size)]
                rows.append(row)
        if matched != 1:
            raise ReleaseBuildError("installed project RECORD omits the training launcher")

        temporary_record = record.with_name(".RECORD.native")
        if temporary_record.exists() or temporary_record.is_symlink():
            raise ReleaseBuildError("project RECORD replacement path already exists")
        try:
            with temporary_record.open("x", encoding="utf-8", newline="") as stream:
                csv.writer(stream, lineterminator="\n").writerows(rows)
            if temporary_record.stat().st_size > MAX_RECORD_BYTES:
                raise ReleaseBuildError("installed project RECORD exceeds its bound")
            os.replace(temporary_record, record)
        finally:
            temporary_record.unlink(missing_ok=True)
    except (OSError, UnicodeError, csv.Error) as error:
        raise ReleaseBuildError("native training launcher installation failed") from error
    finally:
        replacement.unlink(missing_ok=True)


def admit_native_code_signing(
    profile: PlatformProfile,
    requested: bool,
) -> NativeCodeSigning | None:
    if not requested:
        return None
    if profile.system == "Darwin":
        identity = os.environ.get("MARKET_SQUAWK_APPLE_SIGNING_IDENTITY", "")
        tool = Path("/usr/bin/codesign")
        if (
            not identity
            or len(identity) > 256
            or any(character in identity for character in "\r\n\0")
            or tool.is_symlink()
            or not tool.is_file()
        ):
            raise ReleaseBuildError("Apple native signing authority is unavailable")
        return NativeCodeSigning(profile.system, identity, tool)
    if profile.system == "Windows":
        identity = os.environ.get(
            "MARKET_SQUAWK_WINDOWS_CERTIFICATE_THUMBPRINT",
            "",
        ).upper()
        discovered = shutil.which("signtool.exe") or shutil.which("signtool")
        if re.fullmatch(r"[0-9A-F]{40}", identity) is None or discovered is None:
            raise ReleaseBuildError("Windows native signing authority is unavailable")
        tool = Path(discovered).resolve(strict=True)
        if tool.is_symlink() or not tool.is_file():
            raise ReleaseBuildError("Windows native signing tool is unavailable")
        return NativeCodeSigning(profile.system, identity, tool)
    raise ReleaseBuildError("native code signing is unavailable for this release target")


def sign_native_files(
    paths: tuple[Path, ...],
    signing: NativeCodeSigning | None,
) -> tuple[Path, ...]:
    if signing is None:
        return ()
    signed = []
    for path in paths:
        if path.is_symlink() or not path.is_file() or not _is_native_code(path, signing.system):
            raise ReleaseBuildError("native signing input is not an admitted executable")
        if signing.system == "Darwin":
            command = [
                str(signing.tool),
                "--force",
                "--options",
                "runtime",
                "--timestamp",
                "--sign",
                signing.identity,
                str(path),
            ]
            verify = [
                str(signing.tool),
                "--verify",
                "--strict",
                "--verbose=2",
                str(path),
            ]
        else:
            command = [
                str(signing.tool),
                "sign",
                "/fd",
                "SHA256",
                "/sha1",
                signing.identity,
                "/tr",
                "http://timestamp.digicert.com",
                "/td",
                "SHA256",
                str(path),
            ]
            verify = [
                str(signing.tool),
                "verify",
                "/pa",
                "/v",
                str(path),
            ]
        _run_native_signing(command)
        _run_native_signing(verify)
        signed.append(path.resolve(strict=True))
    return tuple(signed)


def sign_release_tree(
    release_root: Path,
    runtime: PythonRuntime,
    signing: NativeCodeSigning | None,
    installed_executables: NativeReleaseExecutables,
) -> tuple[Path, ...]:
    if signing is None:
        return ()
    excluded = {
        path.resolve(strict=True)
        for path in (
            installed_executables.application,
            installed_executables.onnx_worker,
            installed_executables.training_driver,
            installed_executables.validator,
        )
    }
    candidates = tuple(
        path
        for path in sorted(release_root.rglob("*"))
        if path.is_file()
        and not path.is_symlink()
        and path.resolve(strict=True) not in excluded
        and _is_native_code(path, signing.system)
    )
    signed = sign_native_files(candidates, signing)
    rewrite_signed_record_entries(release_root, runtime, signed)
    return signed


def rewrite_signed_record_entries(
    release_root: Path,
    runtime: PythonRuntime,
    signed_paths: tuple[Path, ...],
) -> None:
    if not signed_paths:
        return
    profile = host_profile()
    site_packages = _site_packages_path(release_root, runtime, profile).resolve(strict=True)
    signed = {path.resolve(strict=True) for path in signed_paths}
    required_owners = {
        path
        for path in signed
        if path.is_relative_to(site_packages)
    }
    observed_owners: set[Path] = set()
    rewrites: list[tuple[Path, list[list[str]]]] = []
    for record in sorted(site_packages.glob("*.dist-info/RECORD")):
        if record.is_symlink() or not record.is_file() or record.stat().st_size > MAX_RECORD_BYTES:
            raise ReleaseBuildError("installed distribution RECORD is invalid")
        rows = []
        changed = False
        try:
            with record.open("r", encoding="utf-8", newline="") as stream:
                for row in csv.reader(stream):
                    if len(row) != 3:
                        raise ReleaseBuildError("installed distribution RECORD is malformed")
                    candidate, _is_internal = _installed_record_path(
                        row[0], site_packages, release_root, profile
                    )
                    candidate = candidate.resolve(strict=True)
                    if candidate in signed:
                        if candidate in observed_owners:
                            raise ReleaseBuildError(
                                "signed installed distribution ownership overlaps"
                            )
                        size, digest = _file_digest(candidate)
                        encoded = (
                            base64.urlsafe_b64encode(bytes.fromhex(digest))
                            .rstrip(b"=")
                            .decode("ascii")
                        )
                        row = [row[0], f"sha256={encoded}", str(size)]
                        observed_owners.add(candidate)
                        changed = True
                    rows.append(row)
        except (OSError, UnicodeError, csv.Error) as error:
            raise ReleaseBuildError("installed distribution RECORD is unreadable") from error
        if changed:
            rewrites.append((record, rows))
    if not required_owners <= observed_owners:
        raise ReleaseBuildError("signed installed distribution file is absent from RECORD")
    for record, rows in rewrites:
        temporary = record.with_name(f".{record.name}.signed")
        if temporary.exists() or temporary.is_symlink():
            raise ReleaseBuildError("signed RECORD replacement path already exists")
        try:
            with temporary.open("x", encoding="utf-8", newline="") as stream:
                csv.writer(stream, lineterminator="\n").writerows(rows)
                stream.flush()
                os.fsync(stream.fileno())
            if temporary.stat().st_size > MAX_RECORD_BYTES:
                raise ReleaseBuildError("signed distribution RECORD exceeds its bound")
            os.replace(temporary, record)
        finally:
            temporary.unlink(missing_ok=True)


def _is_native_code(path: Path, system: str) -> bool:
    with path.open("rb") as stream:
        prefix = stream.read(4)
    if system == "Windows":
        return path.suffix.lower() in {".dll", ".exe", ".pyd"} and prefix[:2] == b"MZ"
    return prefix in {
        b"\xbe\xba\xfe\xca",
        b"\xbf\xba\xfe\xca",
        b"\xca\xfe\xba\xbe",
        b"\xca\xfe\xba\xbf",
        b"\xce\xfa\xed\xfe",
        b"\xcf\xfa\xed\xfe",
        b"\xfe\xed\xfa\xce",
        b"\xfe\xed\xfa\xcf",
    }


def _run_native_signing(command: list[str]) -> None:
    try:
        completed = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=300,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ReleaseBuildError("native signing tool could not complete") from error
    if completed.returncode != 0 or len(completed.stdout) > 64 * 1024:
        raise ReleaseBuildError("native signing or verification failed")


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
    if purpose == CANONICAL_RELEASE:
        _unseal_owned_release_authority(path)
    _remove_owned_tree(path)
    path.mkdir()
    (path / CHILD_MARKER).write_text(_marker_content(path, purpose), encoding="utf-8")


def _unseal_owned_release_authority(path: Path) -> None:
    if os.name == "nt":
        _admit_windows_release_authority(path)
        return
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


def _admit_windows_release_authority(path: Path) -> None:
    current = path
    for component in (None, "share", "market-squawk"):
        if component is not None:
            current = current / component
        try:
            status = current.lstat()
        except FileNotFoundError:
            return
        attributes = getattr(status, "st_file_attributes", 0)
        if (
            not stat.S_ISDIR(status.st_mode)
            or stat.S_ISLNK(status.st_mode)
            or attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        ):
            raise ReleaseBuildError("sealed release authority is invalid")


def _remove_windows_readonly(function: object, path: str, error: BaseException) -> None:
    if not isinstance(error, PermissionError) or not callable(function):
        raise error
    os.chmod(path, stat.S_IREAD | stat.S_IWRITE, follow_symlinks=False)
    function(path)


def _remove_owned_tree(path: Path) -> None:
    if os.name == "nt":
        shutil.rmtree(path, onexc=_remove_windows_readonly)
    else:
        shutil.rmtree(path)


def _remove_owned_child(path: Path, root: Path, purpose: str) -> None:
    _admit_owned_child(path, root, purpose)
    _remove_owned_tree(path)


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


def _project_version(root: Path) -> str:
    workspace = _toml(root / "Cargo.toml")
    workspace_table = workspace.get("workspace")
    workspace_package = (
        workspace_table.get("package") if isinstance(workspace_table, dict) else None
    )
    rust_version = (
        workspace_package.get("version")
        if isinstance(workspace_package, dict)
        else None
    )
    python = _toml(root / "python/pyproject.toml")
    python_project = python.get("project")
    python_version = (
        python_project.get("version") if isinstance(python_project, dict) else None
    )
    if (
        not isinstance(rust_version, str)
        or not isinstance(python_version, str)
        or rust_version != python_version
        or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", rust_version) is None
    ):
        raise ReleaseBuildError("Rust and Python release versions are inconsistent")
    return rust_version


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


def _copy_runtime_release(
    runtime: PythonRuntime,
    release_root: Path,
    root: Path,
    environment: dict[str, str] | None = None,
) -> str:
    """Copy one admitted self-contained CPython distribution into the release root."""

    profile = host_profile()
    source_root = (
        runtime.executable.parent
        if profile.system == "Windows"
        else runtime.executable.parent.parent
    )
    expected_source = source_root / profile.interpreter_relative_path
    if expected_source.resolve(strict=True) != runtime.executable:
        raise ReleaseBuildError("managed Python runtime layout is not canonical")
    observed_files = 0
    observed_bytes = 0
    for source in source_root.rglob("*"):
        metadata = source.lstat()
        if source.is_symlink():
            resolved = source.resolve(strict=True)
            if not resolved.is_file() or not resolved.is_relative_to(source_root):
                raise ReleaseBuildError("managed Python runtime contains an unsafe link")
            size = resolved.stat().st_size
        elif source.is_file():
            size = metadata.st_size
        elif source.is_dir():
            continue
        else:
            raise ReleaseBuildError("managed Python runtime contains a special file")
        observed_files += 1
        observed_bytes += size
        if observed_files > 32_768 or observed_bytes > 2 * 1024 * 1024 * 1024:
            raise ReleaseBuildError("managed Python runtime exceeds its copy bounds")
    shutil.copytree(
        source_root,
        release_root,
        dirs_exist_ok=True,
        symlinks=False,
        copy_function=shutil.copy2,
    )
    for copied in release_root.rglob("*"):
        if copied.is_symlink():
            raise ReleaseBuildError("copied Python runtime retained a symbolic link")
    executable = release_root / profile.interpreter_relative_path
    _admit_created_runtime(executable, runtime.version, root, environment)
    return str(executable)


def _admit_created_runtime(
    executable: Path,
    expected: tuple[int, int, int],
    root: Path,
    environment: dict[str, str] | None = None,
) -> None:
    evidence, version = _interpreter_evidence(executable, root, environment)
    profile = host_profile()
    if (
        evidence.get("implementation") != "cpython"
        or evidence.get("system") != profile.system
        or not _interpreter_platform_matches(profile, evidence)
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
                "import json,platform,sys,sysconfig;"
                "print(json.dumps({'implementation':sys.implementation.name,"
                "'version':list(sys.version_info[:3]),'system':platform.system(),"
                "'machine':platform.machine(),"
                "'configured_platform':sysconfig.get_platform()},sort_keys=True))"
            ),
        ],
        root,
        environment or {"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin", "TZ": "UTC"},
    )
    try:
        evidence = json.loads(output)
        raw_version = evidence["version"]
        if not isinstance(raw_version, list) or any(
            not isinstance(value, int) for value in raw_version
        ):
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
    target = toolchain.get("target")
    if not isinstance(target, str):
        raise ReleaseBuildError("bound native build target is invalid")
    profile = platform_profile(target)
    if host_profile() != profile:
        raise ReleaseBuildError("bound native build target differs from the host")
    temporary = build_home / "tmp"
    if not temporary.is_dir():
        temporary = Path("/tmp")
    environment = {
        "CARGO_HOME": str(cargo_home),
        "CARGO_BUILD_TARGET": profile.target,
        "CARGO_INCREMENTAL": "0",
        "CARGO_NET_OFFLINE": "true" if offline else "false",
        "CARGO_TERM_COLOR": "never",
        "HOME": str(build_home),
        "LANG": "C",
        "LC_ALL": "C",
        "PIP_CONFIG_FILE": os.devnull,
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "PIP_NO_INDEX": "1" if offline else "0",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONHASHSEED": "0",
        "PYTHONNOUSERSITE": "1",
        "RUSTC": str(rustc),
        "SOURCE_DATE_EPOCH": SOURCE_DATE_EPOCH,
        "TMPDIR": str(temporary),
        "TZ": "UTC",
        "UV_LINK_MODE": "copy",
        "UV_NO_CONFIG": "1",
        "UV_NO_PROGRESS": "1",
        "UV_OFFLINE": "true" if offline else "false",
        "UV_PYTHON_DOWNLOADS": "never",
    }
    target_key = profile.target.upper().replace("-", "_")
    if profile.system == "Darwin":
        clang = _bound_tool(toolchain, "clang")
        clangxx = _bound_tool(toolchain, "clangxx")
        archiver = _bound_tool(toolchain, "archiver")
        ranlib = _bound_tool(toolchain, "ranlib")
        sdk = toolchain.get("sdk")
        if (
            not isinstance(sdk, dict)
            or not isinstance(sdk.get("path"), str)
            or toolchain.get("macos_deployment_target") != MACOS_DEPLOYMENT_TARGET
            or not isinstance(toolchain.get("developer_dir"), str)
        ):
            raise ReleaseBuildError("bound macOS build policy is invalid")
        environment.update(
            {
                "AR": str(archiver),
                "CC": str(clang),
                "CXX": str(clangxx),
                "DEVELOPER_DIR": str(toolchain["developer_dir"]),
                f"CARGO_TARGET_{target_key}_LINKER": str(clang),
                "MACOSX_DEPLOYMENT_TARGET": MACOS_DEPLOYMENT_TARGET,
                "PATH": os.pathsep.join(
                    dict.fromkeys(
                        [
                            str(cargo.parent),
                            str(rustc.parent),
                            str(clang.parent),
                            "/usr/bin",
                            "/bin",
                        ]
                    )
                ),
                "RANLIB": str(ranlib),
                "SDKROOT": sdk["path"],
            }
        )
    elif profile.system == "Linux":
        cc = _bound_tool(toolchain, "cc")
        cxx = _bound_tool(toolchain, "cxx")
        archiver = _bound_tool(toolchain, "archiver")
        ranlib = _bound_tool(toolchain, "ranlib")
        zig = _bound_tool(toolchain, "zig")
        environment.update(
            {
                "AR": str(archiver),
                "CARGO_ZIGBUILD_ZIG_PATH": str(zig),
                "CC": str(cc),
                "CXX": str(cxx),
                "PATH": os.pathsep.join(
                    dict.fromkeys(
                        [
                            str(cargo.parent),
                            str(rustc.parent),
                            str(cc.parent),
                            str(zig.parent),
                            "/usr/bin",
                            "/bin",
                        ]
                    )
                ),
                "RANLIB": str(ranlib),
            }
        )
    elif profile.system == "Windows":
        compiler = _bound_tool(toolchain, "compiler")
        linker = _bound_tool(toolchain, "linker")
        archiver = _bound_tool(toolchain, "archiver")
        assembler = _bound_tool(toolchain, "assembler")
        developer_directory = toolchain.get("developer_directory")
        target_architecture = toolchain.get("target_architecture")
        if (
            not isinstance(developer_directory, str)
            or not Path(developer_directory).resolve(strict=True).is_dir()
            or target_architecture != "x64"
        ):
            raise ReleaseBuildError("bound MSVC developer environment is invalid")
        required = ("INCLUDE", "LIB", "PATH", "PATHEXT", "SystemRoot")
        if any(not os.environ.get(name) for name in required):
            raise ReleaseBuildError("MSVC developer environment is incomplete")
        environment.update({name: os.environ[name] for name in required})
        environment["VCINSTALLDIR"] = developer_directory
        environment["VSCMD_ARG_TGT_ARCH"] = target_architecture
        environment["PATH"] = os.pathsep.join(
            dict.fromkeys(
                [
                    str(cargo.parent),
                    str(rustc.parent),
                    str(compiler.parent),
                    str(linker.parent),
                    str(archiver.parent),
                    str(assembler.parent),
                    environment["PATH"],
                ]
            )
        )
        environment["TEMP"] = str(temporary)
        environment["TMP"] = str(temporary)
        if os.environ.get("LIBPATH"):
            environment["LIBPATH"] = os.environ["LIBPATH"]
    else:
        raise ReleaseBuildError("bound native build target is unsupported")
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
    release_components_sha256: str,
    runtime: PythonRuntime,
    toolchain: dict[str, object],
    runtime_requirements: tuple[RuntimeRequirement, ...],
    release_public_key: str,
    release_signer_sha256: str,
) -> tuple[bytes, str]:
    """Return the canonical build foundation embedded in both native executables."""

    _sha256(release_public_key)
    _sha256(release_signer_sha256)
    _sha256(release_components_sha256)
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
        "release_components_sha256": release_components_sha256,
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
    canonical_release: Path,
    profile: PlatformProfile,
    signer: ReleaseSigner,
) -> tuple[bytes, str]:
    """Bind the exact wheel and canonical CPython 3.14 native product."""

    _sha256(foundation_sha256)
    wheel_size, wheel_sha256 = _file_digest(project_wheel)
    if canonical_release.name != CANONICAL_RELEASE:
        raise ReleaseBuildError("native release executable identity is not canonical")
    _admit_owned_child(canonical_release, canonical_release.parent, CANONICAL_RELEASE)
    suffix = profile.executable_suffix
    expected_names = {
        "application": f"market-squawk{suffix}",
        "onnx_worker": f"market-squawk-onnx-worker{suffix}",
        "training_driver": f"market-squawk-train{suffix}",
        "validator": f"market-squawk-model-validator{suffix}",
    }
    native_files = {
        "application": canonical_release / "bin" / expected_names["application"],
        "onnx_worker": canonical_release / "bin" / expected_names["onnx_worker"],
        "training_driver": canonical_release / _training_driver_path(profile),
        "validator": canonical_release / "bin" / expected_names["validator"],
    }
    if any(
        path.name != expected_names[name] or path.is_symlink() or not path.is_file()
        for name, path in native_files.items()
    ):
        raise ReleaseBuildError("native release executable identity is invalid")
    native_limits = {
        "application": MAX_APPLICATION_EXECUTABLE_BYTES,
        "onnx_worker": MAX_ONNX_WORKER_EXECUTABLE_BYTES,
        "training_driver": MAX_TRAINING_LAUNCHER_BYTES,
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
            "minimum_system": profile.minimum_system,
            "platform_tag": platform_tag,
            "python_tag": python_tag,
            "sha256": wheel_sha256,
            "size_bytes": wheel_size,
            "target": profile.target,
        },
        "schema_version": 3,
        "training_driver": {
            "sha256": native_identities["training_driver"][1],
            "size_bytes": native_identities["training_driver"][0],
        },
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
        "schema_version": 3,
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

    profile = host_profile()
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
                and member.filename.endswith(profile.native_extension_suffix)
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
            if native_suffix != profile.native_extension_suffix:
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
        if os.name != "nt":
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

    profile = host_profile()
    site_packages = _site_packages_path(release_root, runtime, profile)
    training_driver = _training_driver_path(profile)
    training_driver_record = os.path.relpath(
        release_root / training_driver, site_packages
    ).replace(os.sep, "/")
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
    external_entries: set[str] = set()
    owned_paths: set[Path] = set()
    owned_file_identities: set[tuple[int, int]] = set()
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
                    require_training_driver and name == training_driver_record
                )
                if is_training_driver:
                    if saw_training_driver or not encoded_digest or not encoded_size:
                        raise ReleaseBuildError(
                            "installed training driver RECORD entry is invalid"
                        )
                    path = release_root / training_driver
                    observed_record_path = os.path.relpath(path, site_packages).replace(
                        os.sep, "/"
                    )
                    if (
                        observed_record_path != training_driver_record
                        or path.parent.is_symlink()
                    ):
                        raise ReleaseBuildError(
                            "installed training driver path is invalid"
                        )
                    external_entries.add(name)
                    saw_training_driver = True
                else:
                    path, is_internal = _installed_record_path(
                        name, site_packages, release_root, profile
                    )
                    if not is_internal:
                        external_entries.add(name)
                if len(external_entries) > MAX_DISTRIBUTION_EXTERNAL_PATHS:
                    raise ReleaseBuildError(
                        "installed distribution external path count is unbounded"
                    )
                if path.is_symlink() or not path.is_file():
                    raise ReleaseBuildError("installed distribution file is unavailable")
                observed_size, observed_sha256, header, file_identity = (
                    _inspect_installed_distribution_file(path)
                )
                resolved_path = path.resolve(strict=True)
                if (
                    resolved_path in owned_paths
                    or file_identity in owned_file_identities
                ):
                    raise ReleaseBuildError(
                        "installed distribution physical ownership overlaps"
                    )
                owned_paths.add(resolved_path)
                owned_file_identities.add(file_identity)
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
        name
        for name in entries
        if name != training_driver_record and name not in external_entries
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
            if name.startswith(native_prefix)
            and name.endswith(profile.native_extension_suffix)
        ]
        if len(native_entries) != 1:
            raise ReleaseBuildError("installed project has no unique native extension")
        native_name = native_entries[0]
        native = site_packages / _safe_record_path(native_name)
        native_sha256, native_size = entries[native_name]
    record_size, record_sha256, _header, record_identity = (
        _inspect_installed_distribution_file(record)
    )
    resolved_record = record.resolve(strict=True)
    if resolved_record in owned_paths or record_identity in owned_file_identities:
        raise ReleaseBuildError("installed distribution physical ownership overlaps")
    owned_paths.add(resolved_record)
    owned_file_identities.add(record_identity)
    return InstalledDistribution(
        name=requirement.name,
        version=requirement.version,
        roots=roots,
        external_paths=tuple(sorted(external_entries)),
        owned_paths=tuple(sorted(owned_paths)),
        owned_file_identities=tuple(sorted(owned_file_identities)),
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
    external_paths = set()
    owned_paths = set()
    owned_file_identities = set()
    for distribution in distributions:
        for root in distribution.roots:
            if root in roots:
                raise ReleaseBuildError("installed distribution roots overlap")
            roots.add(root)
        for path in distribution.external_paths:
            if path in external_paths:
                raise ReleaseBuildError("installed distribution ownership overlaps")
            external_paths.add(path)
        for path in distribution.owned_paths:
            if path in owned_paths:
                raise ReleaseBuildError("installed distribution ownership overlaps")
            owned_paths.add(path)
        for identity in distribution.owned_file_identities:
            if identity in owned_file_identities:
                raise ReleaseBuildError("installed distribution ownership overlaps")
            owned_file_identities.add(identity)


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
    profile = host_profile()
    if interpreter_relative != profile.interpreter_relative_path:
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
            "executable_relative_path": profile.interpreter_relative_path,
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
        "schema_version": 2,
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
    if os.name != "nt":
        for path in (wheel_destination, manifest_path, receipt_path):
            path.chmod(0o444)
        authority.chmod(0o555)
    return hashlib.sha256(encoded).hexdigest()


def _distribution_payload(distribution: InstalledDistribution) -> dict[str, object]:
    return {
        "external_paths": list(distribution.external_paths),
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


def _inspect_installed_distribution_file(
    path: Path,
) -> tuple[int, str, bytes, tuple[int, int]]:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
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
    return observed, digest.hexdigest(), bytes(header), (before.st_dev, before.st_ino)


def _safe_record_path(value: str) -> Path:
    parts = _record_path_parts(value, allow_leading_parents=False)
    return Path(*parts)


def _installed_record_path(
    value: str,
    site_packages: Path,
    release_root: Path,
    profile: PlatformProfile,
) -> tuple[Path, bool]:
    """Resolve an installed RECORD path within its owned installation scheme."""

    try:
        return site_packages / _safe_record_path(value), True
    except ReleaseBuildError:
        parts = _record_path_parts(value, allow_leading_parents=True)
    if parts[0] != "..":
        raise ReleaseBuildError("installed project RECORD path is invalid")
    first_owned = next(
        (index for index, part in enumerate(parts) if part != ".."), None
    )
    if first_owned is None or any(part == ".." for part in parts[first_owned:]):
        raise ReleaseBuildError("installed project RECORD path is invalid")

    try:
        canonical_release = release_root.resolve(strict=True)
        canonical_site_packages = site_packages.resolve(strict=True)
        scripts = release_root / _training_driver_path(profile).parent
        if scripts.is_symlink() or not scripts.is_dir():
            raise ReleaseBuildError("installed project RECORD path is invalid")
        canonical_scripts = scripts.resolve(strict=True)
        path = canonical_site_packages.joinpath(*parts).resolve(strict=False)
    except OSError as error:
        raise ReleaseBuildError("installed project RECORD path is invalid") from error
    observed = os.path.relpath(path, canonical_site_packages).replace(os.sep, "/")
    if (
        not canonical_site_packages.is_relative_to(canonical_release)
        or canonical_scripts.parent != canonical_release
        or path.parent != canonical_scripts
        or observed != value
    ):
        raise ReleaseBuildError("installed project RECORD path is invalid")
    return path, False


def _record_path_parts(
    value: str, *, allow_leading_parents: bool
) -> tuple[str, ...]:
    posix = PurePosixPath(value)
    windows = PureWindowsPath(value)
    raw_parts = value.split("/")
    if (
        not value
        or len(value.encode("utf-8")) > 1024
        or "\0" in value
        or "\\" in value
        or posix.is_absolute()
        or windows.is_absolute()
        or bool(windows.drive)
        or any(part in {"", "."} for part in raw_parts)
        or (not allow_leading_parents and ".." in raw_parts)
    ):
        raise ReleaseBuildError("installed project RECORD path is invalid")
    return tuple(raw_parts)


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


def _compatible(
    filename: str,
    version: tuple[int, int],
    profile: PlatformProfile,
) -> bool:
    _name, _wheel_version, _build, wheel_tags = _parse_wheel(filename)
    return bool(wheel_tags.intersection(_supported_wheel_tags(version, profile)))


def _admit_license(
    path: Path,
    expected: str,
    expected_files: tuple[LicenseFile, ...],
) -> None:
    try:
        with zipfile.ZipFile(path) as archive:
            names = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
            if len(names) != 1:
                raise ReleaseBuildError("wheel has no unique core metadata")
            metadata = BytesParser().parsebytes(archive.read(names[0]))
            observed = metadata.get("License-Expression")
            if observed is not None and observed.strip() != expected:
                raise ReleaseBuildError("wheel license differs from the locked expression")
            observed_files = _inspect_wheel_license_files(
                archive,
                names[0],
                metadata,
            )
    except (OSError, zipfile.BadZipFile, KeyError) as error:
        raise ReleaseBuildError("wheel metadata is unreadable") from error
    expected_value = [
        {"path": value.path, "sha256": value.sha256, "size_bytes": value.size_bytes}
        for value in expected_files
    ]
    if observed_files != expected_value:
        raise ReleaseBuildError("wheel license file differs from its locked identity")


def _parse_wheel(filename: str) -> tuple[object, object, object, frozenset[object]]:
    if (
        not isinstance(filename, str)
        or not filename.endswith(".whl")
        or "/" in filename
        or "\\" in filename
    ):
        raise ReleaseBuildError("wheel filename is invalid")
    try:
        import packaging
        from packaging.utils import InvalidWheelFilename, parse_wheel_filename
    except ImportError as error:
        raise ReleaseBuildError(
            "packaging 26.2 is required for wheel admission"
        ) from error
    if packaging.__version__ != PACKAGING_VERSION:
        raise ReleaseBuildError("wheel admission requires exact packaging 26.2")
    try:
        return parse_wheel_filename(filename, validate_order=False)
    except InvalidWheelFilename as error:
        raise ReleaseBuildError("wheel compatibility tags are invalid") from error


def _single_wheel_tag(filename: str) -> object:
    _name, _wheel_version, _build, tags = _parse_wheel(filename)
    if len(tags) != 1:
        raise ReleaseBuildError("project wheel must have one exact compatibility tag")
    return next(iter(tags))


def _supported_wheel_tags(
    version: tuple[int, int], profile: PlatformProfile
) -> frozenset[object]:
    return frozenset(_ordered_supported_wheel_tags(version, profile))


def _ordered_supported_wheel_tags(
    version: tuple[int, int], profile: PlatformProfile
) -> tuple[object, ...]:
    try:
        import packaging
        from packaging.tags import compatible_tags, cpython_tags, mac_platforms
    except ImportError as error:
        raise ReleaseBuildError(
            "packaging 26.2 is required for wheel admission"
        ) from error
    if packaging.__version__ != PACKAGING_VERSION or version != (3, 14):
        raise ReleaseBuildError("wheel admission requires exact packaging 26.2 and CPython 3.14")
    if profile.target == "aarch64-apple-darwin":
        platforms = tuple(mac_platforms((12, 0), "arm64"))
    elif profile.target == "x86_64-apple-darwin":
        platforms = tuple(mac_platforms((12, 0), "x86_64"))
    elif profile.target == "x86_64-pc-windows-msvc":
        platforms = ("win_amd64",)
    elif profile.target == "x86_64-unknown-linux-gnu":
        platforms = tuple(
            f"manylinux_2_{minor}_x86_64" for minor in range(28, 4, -1)
        ) + ("manylinux2014_x86_64", "manylinux2010_x86_64", "manylinux1_x86_64")
    else:
        raise ReleaseBuildError("release target is unsupported")
    tags = tuple(
        cpython_tags(
            python_version=version,
            abis=("cp314",),
            platforms=platforms,
        )
    ) + tuple(
        compatible_tags(
            python_version=version,
            interpreter="cp314",
            platforms=platforms,
        )
    )
    return tuple(dict.fromkeys(tags))


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


def _site_packages_path(
    root: Path, runtime: PythonRuntime, profile: PlatformProfile
) -> Path:
    if profile.system == "Windows":
        return root / "Lib" / "site-packages"
    return root / "lib" / f"python{runtime.version[0]}.{runtime.version[1]}" / "site-packages"


def _training_driver_path(profile: PlatformProfile) -> Path:
    if profile.system == "Windows":
        return Path("Scripts/market-squawk-train.exe")
    return Path("bin/market-squawk-train")


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
    label = " ".join((Path(command[0]).name, *command[1:]))
    if completed.returncode != 0:
        raise ReleaseBuildError(
            f"release tool evidence command failed ({label}; exit {completed.returncode})"
        )
    if not completed.stdout:
        raise ReleaseBuildError(f"release tool evidence command returned no output ({label})")
    if len(completed.stdout) > 16_384:
        raise ReleaseBuildError(f"release tool evidence output exceeded its bound ({label})")
    return completed.stdout.strip()


def _packaging_is_exact() -> bool:
    try:
        import packaging
    except ImportError:
        return False
    return packaging.__version__ == PACKAGING_VERSION


def _run_refresh_with_locked_packaging(requirements: Path) -> int:
    uv = shutil.which("uv")
    if uv is None:
        raise ReleaseBuildError("lock refresh requires exact uv 0.12.1")
    if not _run_output([uv, "--version"], Path.cwd()).startswith("uv 0.12.1 "):
        raise ReleaseBuildError("lock refresh requires exact uv 0.12.1")
    raw = requirements.read_text(encoding="utf-8")
    lines = raw.splitlines()
    selected = []
    active = False
    for line in lines:
        if line.startswith(f"packaging=={PACKAGING_VERSION} "):
            active = True
            selected.append(line)
            continue
        if active and ("--hash=sha256:" in line or line.lstrip().startswith("#")):
            selected.append(line)
            continue
        if active and line and not line[0].isspace():
            break
    if (
        not selected
        or sum("--hash=sha256:" in line for line in selected) < 2
    ):
        raise ReleaseBuildError("requirements lock lacks exact packaging 26.2 hashes")
    with tempfile.TemporaryDirectory(prefix="market-squawk-packaging-tool-") as temporary:
        tool_root = Path(temporary)
        tool_requirements = tool_root / "requirements.txt"
        tool_requirements.write_text("\n".join(selected) + "\n", encoding="utf-8")
        environment = {
            "HOME": str(tool_root),
            "LANG": "C",
            "LC_ALL": "C",
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "PYTHONNOUSERSITE": "1",
            "UV_NO_CONFIG": "1",
            "UV_NO_PROGRESS": "1",
        }
        venv = tool_root / "venv"
        _run([uv, "venv", "--python", sys.executable, str(venv)], Path.cwd(), environment)
        interpreter = _venv_python(venv)
        _run(
            [
                uv,
                "pip",
                "sync",
                "--python",
                interpreter,
                "--require-hashes",
                "--strict",
                "--only-binary",
                ":all:",
                str(tool_requirements),
            ],
            Path.cwd(),
            environment,
        )
        completed = subprocess.run(
            [interpreter, "-I", str(Path(__file__).resolve()), *sys.argv[1:], "--packaging-ready"],
            cwd=Path.cwd(),
            env=environment,
            check=False,
        )
        return completed.returncode


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", type=Path)
    parser.add_argument(
        "--target",
        choices=tuple(PLATFORM_PROFILES),
    )
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--requirements", type=Path)
    parser.add_argument("--targets")
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--refresh-lock-manifests", action="store_true")
    actions.add_argument("--refresh-source-closure", action="store_true")
    actions.add_argument("--verify-source-closure", action="store_true")
    actions.add_argument("--verify-development-runtime", action="store_true")
    actions.add_argument("--reset-development-runtime", action="store_true")
    parser.add_argument(
        "--development-runtime-root",
        type=Path,
        help=(
            "Use the single ignored repository development model-runtime cache; "
            "release builds never select this mode."
        ),
    )
    parser.add_argument("--packaging-ready", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--python", action="append", type=Path)
    parser.add_argument("--uv", type=Path)
    parser.add_argument("--zig", type=Path)
    parser.add_argument(
        "--component-root",
        type=Path,
        help="Builder-owned cache for the exact locked uv, CPython, and Linux Zig archives.",
    )
    parser.add_argument("--source-cache", type=Path)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--prepare-cache-only", action="store_true")
    parser.add_argument(
        "--sign-native",
        action="store_true",
        help="Timestamp and platform-sign every native release component.",
    )
    options = parser.parse_args()
    try:
        root = Path(__file__).resolve().parents[1]
        build_arguments = (
            options.target,
            options.artifact_root,
            options.requirements,
            options.targets,
            options.python,
            options.uv,
            options.zig,
            options.component_root,
            options.source_cache,
            options.offline,
            options.prepare_cache_only,
            options.sign_native,
        )
        if options.reset_development_runtime:
            if (
                options.development_runtime_root is None
                or options.lock is not None
                or any(value is not None and value is not False for value in build_arguments)
            ):
                raise ReleaseBuildError("development runtime reset arguments are invalid")
            reset_development_runtime_root(options.development_runtime_root, root)
            return 0
        if options.refresh_lock_manifests:
            if (
                options.development_runtime_root is not None
                or options.requirements is None
                or options.lock is None
                or options.targets is None
            ):
                raise ReleaseBuildError("lock refresh arguments are incomplete")
            requirements_path = options.requirements.expanduser().resolve(strict=True)
            if not _packaging_is_exact():
                if options.packaging_ready:
                    raise ReleaseBuildError("sealed packaging 26.2 bootstrap failed")
                return _run_refresh_with_locked_packaging(requirements_path)
            targets = tuple(options.targets.split(","))
            refresh_lock_manifests(
                requirements_path,
                options.lock.expanduser().resolve(strict=True),
                targets,
                root,
            )
            return 0
        if options.refresh_source_closure:
            if (
                options.lock is None
                or any(value is not None and value is not False for value in build_arguments)
            ):
                raise ReleaseBuildError("source refresh arguments are incomplete")
            if options.development_runtime_root is None:
                refresh_source_closure(options.lock, root)
            else:
                development_root = admit_development_runtime_root(
                    options.development_runtime_root,
                    root,
                    create=True,
                )
                refresh_development_source_closure(
                    options.lock,
                    development_root,
                    root,
                )
            return 0
        if options.verify_source_closure:
            if (
                any(value is not None and value is not False for value in build_arguments)
                or not _packaging_is_exact()
            ):
                raise ReleaseBuildError("source verification arguments are incomplete")
            if options.development_runtime_root is None:
                if options.lock is None:
                    raise ReleaseBuildError("source verification arguments are incomplete")
                lock_path = options.lock.expanduser().resolve(strict=True)
                platform_lock_directory = None
            else:
                if options.lock is not None:
                    raise ReleaseBuildError("source verification arguments are incomplete")
                development_root = admit_development_runtime_root(
                    options.development_runtime_root,
                    root,
                    create=False,
                )
                lock_path = (development_root / "source-lock.json").resolve(strict=True)
                platform_lock_directory = root / "python/wheelhouse"
                admit_development_source_template(lock_path, root)
            lock = load_lock(
                lock_path,
                platform_lock_directory=platform_lock_directory,
            )
            admit_sources(lock, root)
            return 0
        if options.verify_development_runtime:
            if (
                options.development_runtime_root is None
                or options.lock is not None
                or any(value is not None and value is not False for value in build_arguments)
                or not _packaging_is_exact()
            ):
                raise ReleaseBuildError("development runtime verification arguments are invalid")
            development_root = admit_development_runtime_root(
                options.development_runtime_root,
                root,
                create=False,
            )
            lock_path = (development_root / "source-lock.json").resolve(strict=True)
            admit_development_source_template(lock_path, root)
            lock = load_lock(
                lock_path,
                platform_lock_directory=root / "python/wheelhouse",
            )
            admit_development_runtime(root, development_root, lock)
            return 0
        development_root = None
        if options.development_runtime_root is not None:
            if any(
                value is not None
                for value in (
                    options.lock,
                    options.artifact_root,
                    options.component_root,
                    options.source_cache,
                    options.python,
                    options.uv,
                    options.zig,
                    options.requirements,
                    options.targets,
                )
            ) or options.sign_native:
                raise ReleaseBuildError("development runtime build arguments are invalid")
            development_root = admit_development_runtime_root(
                options.development_runtime_root,
                root,
                create=False,
            )
            lock_path = (development_root / "source-lock.json").resolve(strict=True)
            admit_development_source_template(lock_path, root)
            artifact_root = development_root / "python"
            component_root = development_root / "components"
            selected_target = options.target or host_profile().target
        else:
            if options.lock is None or options.target is None or options.artifact_root is None:
                raise ReleaseBuildError("release build arguments are incomplete")
            lock_path = options.lock.expanduser().resolve(strict=True)
            artifact_root = options.artifact_root
            component_root = options.component_root
            selected_target = options.target
        source_cache = (
            options.source_cache.expanduser().resolve(strict=True)
            if options.source_cache is not None
            else None
        )
        profile = platform_profile(selected_target)
        if host_profile() != profile:
            raise ReleaseBuildError("release target does not match the native build host")
        layout = admit_artifact_root(artifact_root, root)
        allow_network = (
            options.prepare_cache_only
            and os.environ.get("MARKET_SQUAWK_PYTHON_WHEEL_PREPARE_NETWORK") == "1"
        )
        bootstrap_locked_packaging(
            lock_path,
            layout.wheelhouse,
            source_cache,
            allow_network=allow_network,
        )
        if component_root is not None:
            if (
                options.python is not None
                or options.uv is not None
                or options.zig is not None
            ):
                raise ReleaseBuildError(
                    "component-root cannot be combined with explicit component paths"
                )
            acquired = acquire_release_components(
                root / "distribution/release-components.json",
                profile,
                component_root,
                root,
                allow_network=allow_network,
                development_runtime_root=development_root,
            )
            python_paths = (acquired.python,)
            uv_path = acquired.uv
            zig_path = acquired.zig
        else:
            if options.python is None or options.uv is None:
                raise ReleaseBuildError(
                    "explicit Python and uv paths or one component-root are required"
                )
            python_paths = tuple(options.python)
            uv_path = options.uv
            zig_path = options.zig
        lock = load_lock(
            lock_path,
            profile.target,
            platform_lock_directory=(
                root / "python/wheelhouse" if development_root is not None else None
            ),
        )
        components_sha256 = admit_release_components(
            root / "distribution/release-components.json",
            profile,
            uv_path,
            zig_path,
            root,
        )
        native_code_signing = admit_native_code_signing(
            profile,
            options.sign_native,
        )
        uv_executable = uv_path.expanduser().resolve(strict=True)
        toolchain = admit_toolchain(root, profile, zig_path)
        runtimes = admit_runtimes(python_paths, lock)
        admit_sources(lock, root)
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
            build_release(
                root,
                lock_path,
                lock,
                layout,
                runtimes,
                toolchain,
                components_sha256,
                uv_executable,
                native_code_signing,
                development_runtime=development_root is not None,
            )
    except (OSError, ReleaseBuildError) as error:
        print(f"python release rejected: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
