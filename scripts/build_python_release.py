#!/usr/bin/env python3
"""Build and install the hash-locked Market Squawk Python product without network access."""

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
import urllib.error
import urllib.parse
import urllib.request
import venv
import zipfile


MAX_LOCK_BYTES = 512 * 1024
MAX_ARTIFACTS = 64
ALLOWED_LICENSES = {
    "Apache-2.0",
    "MIT",
    "BSD-2-Clause",
    "MIT OR Apache-2.0",
    "Apache-2.0 OR BSD-2-Clause",
}


class ReleaseBuildError(RuntimeError):
    """A locked source, wheel, interpreter, license, or offline-build contract failed."""


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
            (3, 10),
            (3, 15),
            "macos-arm64",
            (Artifact("fixture", "1.0", "MIT", filename, sha256, size_bytes, "https://files.pythonhosted.org/fixture.whl"),),
            (),
        )


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
    if not isinstance(value, dict) or set(value) != {"schema_version", "python", "platform", "artifacts", "sources"}:
        raise ReleaseBuildError("Python release lock shape is invalid")
    if value["schema_version"] != 1 or value["platform"] != "macos-arm64":
        raise ReleaseBuildError("Python release lock version or platform is unsupported")
    python = value["python"]
    if not isinstance(python, dict) or set(python) != {"minimum", "maximum_exclusive"}:
        raise ReleaseBuildError("Python interpreter matrix is invalid")
    minimum = _version(python["minimum"])
    maximum = _version(python["maximum_exclusive"])
    artifacts_value = value["artifacts"]
    if not isinstance(artifacts_value, list) or not artifacts_value or len(artifacts_value) > MAX_ARTIFACTS:
        raise ReleaseBuildError("Python wheel count is invalid")
    artifacts = []
    names: set[str] = set()
    for item in artifacts_value:
        if not isinstance(item, dict) or set(item) != {"project", "version", "license", "filename", "sha256", "size_bytes", "url"}:
            raise ReleaseBuildError("Python wheel identity is incomplete")
        if item["filename"] in names or item["license"] not in ALLOWED_LICENSES:
            raise ReleaseBuildError("Python wheel identity or license is invalid")
        _sha256(item["sha256"])
        if item["sha256"] == "0" * 64 or not isinstance(item["size_bytes"], int) or item["size_bytes"] <= 0:
            raise ReleaseBuildError("Python wheel hash or size is invalid")
        parsed = urllib.parse.urlparse(item["url"])
        if parsed.scheme != "https" or parsed.hostname != "files.pythonhosted.org" or Path(parsed.path).name != item["filename"]:
            raise ReleaseBuildError("Python wheel URL is not an exact official artifact")
        _wheel_tags(item["filename"])
        artifacts.append(Artifact(**item))
        names.add(item["filename"])
    sources_value = value["sources"]
    if not isinstance(sources_value, list) or len(sources_value) > 128:
        raise ReleaseBuildError("Python source lock is invalid")
    sources = []
    source_paths: set[str] = set()
    for item in sources_value:
        if not isinstance(item, dict) or set(item) != {"path", "sha256", "size_bytes"}:
            raise ReleaseBuildError("Python source identity is incomplete")
        if item["path"] in source_paths or Path(item["path"]).is_absolute() or ".." in Path(item["path"]).parts:
            raise ReleaseBuildError("Python source path is invalid")
        _sha256(item["sha256"])
        if item["sha256"] == "0" * 64 or not isinstance(item["size_bytes"], int) or item["size_bytes"] <= 0:
            raise ReleaseBuildError("Python source hash or size is invalid")
        sources.append(Source(**item))
        source_paths.add(item["path"])
    return ReleaseLock(minimum, maximum, value["platform"], tuple(artifacts), tuple(sources))


def admit_wheelhouse(lock: ReleaseLock, wheelhouse: Path, *, offline: bool) -> tuple[Path, ...]:
    del offline
    if wheelhouse.is_symlink() or not wheelhouse.is_dir():
        raise ReleaseBuildError("explicit wheelhouse is unavailable")
    selected = tuple(artifact for artifact in lock.artifacts if _compatible(artifact.filename))
    projects = {artifact.project.lower() for artifact in selected}
    if not {"maturin", "pyarrow", "pytest", "packaging", "pluggy", "iniconfig", "pygments"} <= projects:
        raise ReleaseBuildError("wheelhouse has no complete compatible dependency set")
    admitted = []
    for artifact in selected:
        path = wheelhouse / artifact.filename
        if path.is_symlink() or not path.is_file():
            raise ReleaseBuildError("offline wheelhouse is missing a locked artifact")
        content = path.read_bytes()
        if len(content) != artifact.size_bytes or hashlib.sha256(content).hexdigest() != artifact.sha256:
            raise ReleaseBuildError("offline wheelhouse artifact hash or size mismatch")
        _admit_license(path, artifact.license)
        admitted.append(path)
    return tuple(admitted)


def prepare_wheelhouse(lock: ReleaseLock, wheelhouse: Path, source_cache: Path | None) -> None:
    if os.environ.get("MARKET_SQUAWK_PYTHON_WHEEL_PREPARE_NETWORK") != "1" and source_cache is None:
        raise ReleaseBuildError("wheel preparation requires an explicit cache or network authorization")
    if wheelhouse.is_symlink():
        raise ReleaseBuildError("explicit wheelhouse must not be a symbolic link")
    wheelhouse.mkdir(parents=True, exist_ok=True)
    for artifact in (item for item in lock.artifacts if _compatible(item.filename)):
        destination = wheelhouse / artifact.filename
        if destination.is_file() and _file_digest(destination) == (artifact.size_bytes, artifact.sha256):
            continue
        temporary = destination.with_suffix(destination.suffix + ".part")
        temporary.unlink(missing_ok=True)
        cached = source_cache / artifact.filename if source_cache is not None else None
        if cached is not None and cached.is_file():
            shutil.copyfile(cached, temporary)
        elif os.environ.get("MARKET_SQUAWK_PYTHON_WHEEL_PREPARE_NETWORK") == "1":
            try:
                with urllib.request.urlopen(artifact.url, timeout=60) as response, temporary.open("wb") as output:
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


def prepare_cargo_cache(root: Path, cargo_home: Path) -> None:
    """Populate or verify one explicit Cargo cache for the locked workspace graph."""

    _cargo_home(cargo_home, create=True)
    network_authorized = os.environ.get("MARKET_SQUAWK_PYTHON_WHEEL_PREPARE_NETWORK") == "1"
    environment = os.environ.copy()
    environment.update(
        {
            "CARGO_HOME": str(cargo_home.resolve()),
            "CARGO_INCREMENTAL": "0",
            "CARGO_NET_OFFLINE": "false" if network_authorized else "true",
        }
    )
    command = [
        "cargo",
        "fetch",
        "--locked",
        "--manifest-path",
        str(root / "Cargo.toml"),
    ]
    if not network_authorized:
        command.append("--offline")
    _run(command, root, environment)


def admit_sources(lock: ReleaseLock, root: Path) -> None:
    if not lock.sources:
        raise ReleaseBuildError("Python release source lock is empty")
    for source in lock.sources:
        path = root / source.path
        if path.is_symlink() or not path.is_file() or _file_digest(path) != (source.size_bytes, source.sha256):
            raise ReleaseBuildError("Python release source identity mismatch")


def build_release(
    lock_path: Path,
    lock: ReleaseLock,
    wheelhouse: Path,
    cargo_home: Path,
    release_venv: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    _admit_runtime(lock)
    admit_sources(lock, root)
    admitted = admit_wheelhouse(lock, wheelhouse, offline=True)
    _cargo_home(cargo_home, create=False)
    build_venv = release_venv.with_name(release_venv.name + "-build")
    build_output = release_venv.with_name(release_venv.name + "-dist")
    for path in (build_venv, build_output, release_venv):
        if path.is_symlink() or (path.exists() and not path.is_dir()):
            raise ReleaseBuildError("release output path is not a controlled directory")
        if path.exists():
            shutil.rmtree(path)
    venv.EnvBuilder(with_pip=True, clear=True).create(build_venv)
    build_python = _venv_python(build_venv)
    maturin = next(path for path in admitted if path.name.startswith("maturin-"))
    _run([build_python, "-m", "pip", "install", "--no-index", "--no-deps", str(maturin)], root)
    build_output.mkdir()
    environment = os.environ.copy()
    environment.update(
        {
            "CARGO_HOME": str(cargo_home.resolve()),
            "CARGO_NET_OFFLINE": "true",
            "CARGO_INCREMENTAL": "0",
            "PYO3_PYTHON": build_python,
        }
    )
    _run(
        [
            build_python,
            "-m",
            "maturin",
            "build",
            "--release",
            "--locked",
            "--out",
            str(build_output.resolve()),
        ],
        root / "python",
        environment,
    )
    project_wheels = list(build_output.glob("market_squawk-0.1.0-*.whl"))
    if len(project_wheels) != 1:
        raise ReleaseBuildError("maturin did not create exactly one project wheel")
    project_wheel = project_wheels[0]
    python_tag, abi_tag, platform_tag = _wheel_tags(project_wheel.name)
    if python_tag != "cp310" or abi_tag != "abi3" or "arm64" not in platform_tag:
        raise ReleaseBuildError("project wheel is not the pinned cp310-abi3 platform artifact")
    _run(
        ["cargo", "build", "-p", "market-squawk-modeling", "--bin", "market-squawk-model-validator", "--release", "--locked"],
        root,
        environment,
    )
    validator = root / "target" / "release" / "market-squawk-model-validator"
    if not validator.is_file():
        raise ReleaseBuildError("Rust model validator executable was not produced")
    venv.EnvBuilder(with_pip=True, clear=True).create(release_venv)
    release_python = _venv_python(release_venv)
    _run(
        [
            release_python,
            "-m",
            "pip",
            "install",
            "--no-index",
            "--find-links",
            str(wheelhouse),
            "--require-hashes",
            "--only-binary",
            ":all:",
            "-r",
            str(root / "python" / "requirements.lock"),
        ],
        root,
    )
    _run([release_python, "-m", "pip", "install", "--no-index", "--no-deps", str(project_wheel)], root)
    validator_destination = release_venv / ("Scripts" if os.name == "nt" else "bin") / "market-squawk-model-validator"
    shutil.copy2(validator, validator_destination)
    validator_destination.chmod(0o755)
    evidence = {
        "schema_version": 1,
        "python": _run_output([release_python, "--version"], root),
        "pip": _run_output([release_python, "-m", "pip", "--version"], root),
        "rustc": _run_output(["rustc", "--version", "--verbose"], root),
        "cargo": _run_output(["cargo", "--version", "--verbose"], root),
        "cargo_lock_sha256": hashlib.sha256((root / "Cargo.lock").read_bytes()).hexdigest(),
        "wheelhouse_lock_sha256": hashlib.sha256(lock_path.read_bytes()).hexdigest(),
        "requirements_lock_sha256": hashlib.sha256((root / "python" / "requirements.lock").read_bytes()).hexdigest(),
        "project_wheel": {"filename": project_wheel.name, "sha256": hashlib.sha256(project_wheel.read_bytes()).hexdigest()},
        "validator_sha256": hashlib.sha256(validator_destination.read_bytes()).hexdigest(),
        "admitted_wheels": [{"filename": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in admitted],
    }
    (release_venv / "market-squawk-release.json").write_text(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n"
    )
    shutil.rmtree(build_venv)


def _admit_runtime(lock: ReleaseLock) -> None:
    version = sys.version_info[:2]
    if sys.implementation.name != "cpython" or not lock.minimum <= version < lock.maximum_exclusive:
        raise ReleaseBuildError("current interpreter is outside the locked CPython matrix")
    if platform.system() != "Darwin" or platform.machine() != "arm64":
        raise ReleaseBuildError("current platform is outside this release wheelhouse")


def _cargo_home(path: Path, *, create: bool) -> None:
    if path.is_symlink():
        raise ReleaseBuildError("explicit Cargo cache must not be a symbolic link")
    if create:
        path.mkdir(parents=True, exist_ok=True)
    if not path.is_dir():
        raise ReleaseBuildError("explicit Cargo cache is unavailable")


def _compatible(filename: str) -> bool:
    python_tag, _abi, platform_tag = _wheel_tags(filename)
    current = f"cp{sys.version_info.major}{sys.version_info.minor}"
    python_ok = python_tag == "py3" or current in python_tag.split(".")
    platform_ok = platform_tag == "any" or (platform.system() == "Darwin" and platform.machine() == "arm64" and "arm64" in platform_tag)
    return python_ok and platform_ok


def _admit_license(path: Path, expected: str) -> None:
    try:
        with zipfile.ZipFile(path) as archive:
            metadata_names = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
            if len(metadata_names) != 1:
                raise ReleaseBuildError("wheel has no unique core metadata")
            metadata = BytesParser().parsebytes(archive.read(metadata_names[0]))
    except (OSError, zipfile.BadZipFile, KeyError) as error:
        raise ReleaseBuildError("wheel metadata is unreadable") from error
    observed = metadata.get("License-Expression") or metadata.get("License")
    if observed is None or observed.strip() != expected:
        raise ReleaseBuildError("wheel license differs from the locked expression")


def _wheel_tags(filename: str) -> tuple[str, str, str]:
    if not isinstance(filename, str) or not filename.endswith(".whl") or "/" in filename or "\\" in filename:
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
    if not isinstance(value, str) or len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
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


def _run_output(command: list[str], root: Path) -> str:
    try:
        completed = subprocess.run(
            command,
            cwd=root,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except OSError as error:
        raise ReleaseBuildError("release tool evidence command could not start") from error
    if completed.returncode != 0 or len(completed.stdout) > 16_384:
        raise ReleaseBuildError("release tool evidence command failed")
    return completed.stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", required=True, type=Path)
    parser.add_argument("--wheelhouse", required=True, type=Path)
    parser.add_argument("--cargo-home", required=True, type=Path)
    parser.add_argument("--venv", type=Path)
    parser.add_argument("--source-cache", type=Path)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--prepare-cache-only", action="store_true")
    options = parser.parse_args()
    try:
        lock_path = options.lock.absolute()
        wheelhouse = options.wheelhouse.absolute()
        cargo_home = options.cargo_home.absolute()
        release_venv = options.venv.absolute() if options.venv is not None else None
        source_cache = options.source_cache.absolute() if options.source_cache is not None else None
        lock = load_lock(lock_path)
        _admit_runtime(lock)
        if options.prepare_cache_only:
            if options.offline:
                raise ReleaseBuildError("cache preparation and offline build modes are exclusive")
            prepare_wheelhouse(lock, wheelhouse, source_cache)
            prepare_cargo_cache(Path(__file__).resolve().parents[1], cargo_home)
            admit_wheelhouse(lock, wheelhouse, offline=True)
        else:
            if not options.offline or release_venv is None:
                raise ReleaseBuildError("release build requires --offline and --venv")
            build_release(lock_path, lock, wheelhouse, cargo_home, release_venv)
    except ReleaseBuildError as error:
        print(f"python release rejected: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
