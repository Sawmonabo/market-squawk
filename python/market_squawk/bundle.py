"""Bounded native bundle candidate publication through the Rust Task 13 validator."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
from typing import Any, Mapping

from . import _native


MAX_ARTIFACT_BYTES = 1024 * 1024
MAX_ONNX_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_METADATA_BYTES = 256 * 1024
MAX_AUTHORITY_BYTES = 256 * 1024
MAX_VALIDATOR_BYTES = 128 * 1024 * 1024
VALIDATOR_READ_BYTES = 1024 * 1024
NATIVE_EXECUTABLES = frozenset(
    {
        "market-squawk",
        "market-squawk-model-validator",
        "market-squawk-onnx-worker",
    }
)


class BundleExportError(ValueError):
    """A bundle candidate was not safely written and admitted by Rust."""


def _native_release_executable(name: str) -> Path:
    if name not in NATIVE_EXECUTABLES:
        raise BundleExportError("native release executable name is invalid")
    suffix = ".exe" if os.name == "nt" else ""
    return Path(sys.prefix).resolve(strict=True) / "bin" / f"{name}{suffix}"


def _native_subprocess_environment() -> dict[str, str]:
    environment = {"LANG": "C", "LC_ALL": "C", "TZ": "UTC"}
    if os.name != "nt":
        environment["PATH"] = "/usr/bin:/bin"
        return environment
    for name in ("SYSTEMROOT", "WINDIR", "TEMP", "TMP"):
        if value := os.environ.get(name):
            environment[name] = value
    system_root = environment.get("SYSTEMROOT") or environment.get("WINDIR")
    environment["PATH"] = str(Path(system_root) / "System32") if system_root else ""
    return environment


def _windows_reparse_point(metadata: os.stat_result) -> bool:
    if os.name != "nt":
        return False
    return bool(
        getattr(metadata, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    )


def _windows_reparse_path(path: Path) -> bool:
    if os.name != "nt":
        return False
    try:
        metadata = os.stat(path, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return _windows_reparse_point(metadata)


def _binary_open_flags() -> int:
    return getattr(os, "O_BINARY", 0) | getattr(os, "O_NOINHERIT", 0)


@dataclass(frozen=True)
class BundleReceipt:
    root: Path
    metadata: Path
    artifact: Path
    run_record: Path
    metadata_sha256: str
    artifact_sha256: str
    training_run_sha256: str
    authority_sha256: str
    dataset_export_sha256: str
    dataset_selection_sha256: str
    catalog_identity_sha256: str
    validated_by_rust: bool


@dataclass(frozen=True)
class BundleAuthorityRef:
    """Operator/catalog-owned authority kept outside the untrusted candidate root."""

    root: Path
    relative_path: str
    sha256: str

    @classmethod
    def exact(
        cls,
        root: Path | str,
        relative_path: str,
        sha256: str,
    ) -> BundleAuthorityRef:
        authority_root = Path(root)
        if (
            authority_root.is_symlink()
            or _windows_reparse_path(authority_root)
            or not authority_root.is_dir()
        ):
            raise BundleExportError("bundle authority root is not a controlled directory")
        parts = _relative_parts(relative_path)
        path = authority_root.joinpath(*parts)
        if path.is_symlink() or _windows_reparse_path(path) or not path.is_file():
            raise BundleExportError("bundle authority is not a controlled regular file")
        content = path.read_bytes()
        if len(content) > MAX_AUTHORITY_BYTES or hashlib.sha256(content).hexdigest() != sha256:
            raise BundleExportError("bundle authority hash or size mismatch")
        return cls(authority_root.resolve(), relative_path, sha256)


@dataclass(frozen=True, init=False)
class BundleCandidate:
    """Final immutable candidate bytes awaiting independent authorization."""

    metadata_bytes: bytes = field(repr=False)
    artifact_bytes: bytes = field(repr=False)
    training_run_bytes: bytes = field(repr=False)
    artifact_path: str
    metadata_sha256: str
    artifact_sha256: str
    training_run_sha256: str

    def __init__(
        self,
        metadata: Mapping[str, Any],
        artifact: Mapping[str, Any],
        run_record: Mapping[str, Any],
    ) -> None:
        artifact_bytes = _canonical(artifact, MAX_ARTIFACT_BYTES)
        self._initialize(metadata, artifact_bytes, run_record, "artifact.json")

    @classmethod
    def onnx(
        cls,
        metadata: Mapping[str, Any],
        artifact: bytes,
        run_record: Mapping[str, Any],
    ) -> BundleCandidate:
        """Construct a candidate containing one exact code-owned ONNX protobuf."""

        if type(artifact) is not bytes or not artifact or len(artifact) > MAX_ONNX_ARTIFACT_BYTES:
            raise BundleExportError("ONNX artifact exceeds its closed byte contract")
        candidate = object.__new__(cls)
        candidate._initialize(metadata, artifact, run_record, "model.onnx")
        return candidate

    def _initialize(
        self,
        metadata: Mapping[str, Any],
        artifact_bytes: bytes,
        run_record: Mapping[str, Any],
        artifact_path: str,
    ) -> None:
        training_run_bytes = _canonical(run_record, MAX_METADATA_BYTES)
        artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
        training_run_sha256 = hashlib.sha256(training_run_bytes).hexdigest()
        try:
            finalized_metadata = dict(metadata)
            finalized_artifact = dict(finalized_metadata["artifact"])
            finalized_training_run = dict(finalized_metadata["training_run"])
        except (KeyError, TypeError, ValueError) as error:
            raise BundleExportError("bundle metadata references are invalid") from error
        expected_format = "onnx" if artifact_path == "model.onnx" else None
        observed_format = finalized_artifact.get("format")
        if (
            finalized_artifact.get("path") != artifact_path
            or not isinstance(observed_format, str)
            or (
                expected_format is not None
                and observed_format != expected_format
            )
            or (
                expected_format is None
                and observed_format not in {"native_linear", "native_logistic"}
            )
        ):
            raise BundleExportError("bundle artifact path or format is invalid")
        finalized_artifact["sha256"] = artifact_sha256
        finalized_artifact["size_bytes"] = len(artifact_bytes)
        finalized_training_run["sha256"] = training_run_sha256
        finalized_training_run["size_bytes"] = len(training_run_bytes)
        finalized_metadata["artifact"] = finalized_artifact
        finalized_metadata["training_run"] = finalized_training_run
        metadata_bytes = _canonical(finalized_metadata, MAX_METADATA_BYTES)
        object.__setattr__(self, "metadata_bytes", metadata_bytes)
        object.__setattr__(self, "artifact_bytes", artifact_bytes)
        object.__setattr__(self, "training_run_bytes", training_run_bytes)
        object.__setattr__(self, "artifact_path", artifact_path)
        object.__setattr__(self, "metadata_sha256", hashlib.sha256(metadata_bytes).hexdigest())
        object.__setattr__(self, "artifact_sha256", artifact_sha256)
        object.__setattr__(self, "training_run_sha256", training_run_sha256)

    def write(
        self,
        output_root: Path | str,
        authority: BundleAuthorityRef,
        *,
        dataset_receipt: Any,
    ) -> BundleReceipt:
        if type(dataset_receipt) is not _native.DatasetReceipt:
            raise BundleExportError("bundle publication requires a native dataset receipt")
        output_root = Path(output_root)
        if (
            output_root.is_symlink()
            or _windows_reparse_path(output_root)
            or not output_root.is_dir()
        ):
            raise BundleExportError("bundle output root is not a controlled directory")
        resolved_output = output_root.resolve()
        if resolved_output == authority.root or resolved_output.is_relative_to(authority.root):
            raise BundleExportError("bundle candidate cannot contain its independent authority")
        if authority.root.is_relative_to(resolved_output):
            raise BundleExportError("bundle authority cannot be nested below candidate output")
        final = output_root / "candidate"
        if final.exists() or final.is_symlink() or _windows_reparse_path(final):
            raise BundleExportError("bundle candidate generation already exists")
        temporary = Path(tempfile.mkdtemp(prefix=".candidate-", dir=output_root))
        try:
            _write_exact(temporary / self.artifact_path, self.artifact_bytes)
            _write_exact(temporary / "bundle.json", self.metadata_bytes)
            _write_exact(temporary / "training-run.json", self.training_run_bytes)
            _fsync_directory(temporary)
            _validate_with_rust(
                temporary,
                self.metadata_sha256,
                authority,
                dataset_receipt,
            )
            os.replace(temporary, final)
            _fsync_directory(output_root)
            return BundleReceipt(
                root=final,
                metadata=final / "bundle.json",
                artifact=final / self.artifact_path,
                run_record=final / "training-run.json",
                metadata_sha256=self.metadata_sha256,
                artifact_sha256=self.artifact_sha256,
                training_run_sha256=self.training_run_sha256,
                authority_sha256=authority.sha256,
                dataset_export_sha256=dataset_receipt.export_sha256,
                dataset_selection_sha256=dataset_receipt.selection_sha256,
                catalog_identity_sha256=dataset_receipt.catalog_identity,
                validated_by_rust=True,
            )
        except Exception:
            shutil.rmtree(temporary, ignore_errors=True)
            raise


def _canonical(value: Mapping[str, Any], maximum: int) -> bytes:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (TypeError, ValueError) as error:
        raise BundleExportError("bundle candidate contains an unsupported value") from error
    if not encoded or len(encoded) > maximum:
        raise BundleExportError("bundle candidate exceeds its byte bound")
    return encoded


def _write_exact(path: Path, content: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | _binary_open_flags()
    descriptor = os.open(path, flags, 0o600)
    try:
        offset = 0
        while offset < len(content):
            written = os.write(descriptor, content[offset:])
            if written <= 0:
                raise BundleExportError("bundle candidate write did not make progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        # Windows has no portable directory-fsync contract. Each file is flushed before the
        # validated same-volume rename, matching the native platform's file-sync authority.
        return
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _expected_validator_sha256() -> str:
    try:
        value = _native.expected_model_validator_sha256()
    except Exception as error:
        raise BundleExportError("native validator identity is unavailable") from error
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value == "0" * 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise BundleExportError("native validator identity is invalid")
    return value


def _validator_digest(path: Path) -> str:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | _binary_open_flags()
    )
    try:
        named_before = os.stat(path, follow_symlinks=False)
        descriptor = os.open(path, flags)
    except OSError as error:
        raise BundleExportError("Rust model bundle validator is unavailable") from error
    try:
        before = os.fstat(descriptor)
        executable = (
            path.suffix.lower() == ".exe" if os.name == "nt" else before.st_mode & 0o111
        )
        if (
            not stat.S_ISREG(before.st_mode)
            or not stat.S_ISREG(named_before.st_mode)
            or _windows_reparse_point(named_before)
            or _windows_reparse_point(before)
            or not os.path.samestat(before, named_before)
            or before.st_size <= 0
            or before.st_size > MAX_VALIDATOR_BYTES
            or not executable
        ):
            raise BundleExportError("Rust model bundle validator is not a bounded executable")
        digest = hashlib.sha256()
        observed = 0
        while True:
            chunk = os.read(descriptor, VALIDATOR_READ_BYTES)
            if not chunk:
                break
            observed += len(chunk)
            if observed > before.st_size or observed > MAX_VALIDATOR_BYTES:
                raise BundleExportError("Rust model bundle validator changed during admission")
            digest.update(chunk)
        after = os.fstat(descriptor)
        named_after = os.stat(path, follow_symlinks=False)
    except OSError as error:
        raise BundleExportError("Rust model bundle validator could not be admitted") from error
    finally:
        os.close(descriptor)
    identity_before = (before.st_dev, before.st_ino, before.st_mode, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_mode, after.st_size, after.st_mtime_ns)
    if (
        observed != before.st_size
        or identity_before != identity_after
        or _windows_reparse_point(named_after)
        or not os.path.samestat(after, named_after)
    ):
        raise BundleExportError("Rust model bundle validator changed during admission")
    return digest.hexdigest()


def _validator_path() -> tuple[Path, str]:
    expected_sha256 = _expected_validator_sha256()
    path = _native_release_executable("market-squawk-model-validator")
    if _validator_digest(path) != expected_sha256:
        raise BundleExportError("Rust model bundle validator identity mismatch")
    return path, expected_sha256


def _validate_with_rust(
    root: Path,
    metadata_sha256: str,
    authority: BundleAuthorityRef,
    dataset_receipt: Any,
) -> None:
    validator, validator_sha256 = _validator_path()
    command = [
        str(validator),
        "--root",
        str(root),
        "--metadata",
        "bundle.json",
        "--metadata-sha256",
        metadata_sha256,
        "--authority-root",
        str(authority.root),
        "--authority",
        authority.relative_path,
        "--authority-sha256",
        authority.sha256,
        "--catalog-root",
        dataset_receipt.catalog_root,
        "--dataset-export-sha256",
        dataset_receipt.export_sha256,
        "--dataset-as-of-unix-nanos",
        str(dataset_receipt.as_of_unix_nanos),
        "--dataset-selection-sha256",
        dataset_receipt.selection_sha256,
        "--catalog-identity-sha256",
        dataset_receipt.catalog_identity,
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            env=_native_subprocess_environment(),
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        if _validator_digest(validator) != validator_sha256:
            raise BundleExportError("Rust model bundle validator identity changed") from error
        raise BundleExportError("Rust model bundle validation could not complete") from error
    if _validator_digest(validator) != validator_sha256:
        raise BundleExportError("Rust model bundle validator identity changed")
    if completed.returncode != 0 or len(completed.stdout) > 4096 or len(completed.stderr) > 4096:
        raise BundleExportError("Rust model bundle validation rejected the candidate")
    try:
        result = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BundleExportError("Rust model bundle validator returned invalid evidence") from error
    if result != {"metadata_sha256": metadata_sha256, "status": "valid"}:
        raise BundleExportError("Rust model bundle validator returned mismatched evidence")


def _relative_parts(value: str) -> tuple[str, ...]:
    path = Path(value)
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode()) > 256
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
        or any(not all(character.isalnum() or character in "._-" for character in part) for part in path.parts)
    ):
        raise BundleExportError("bundle authority path is outside the controlled grammar")
    return path.parts
