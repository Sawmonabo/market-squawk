#!/usr/bin/env python3
"""Assemble one deterministic, complete Market Squawk platform release."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
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


@dataclass(frozen=True)
class Options:
    target: TargetProfile
    version: str
    commit: str
    tree: str
    python_release: Path
    native_bundle: Path
    output: Path


def main() -> int:
    try:
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
