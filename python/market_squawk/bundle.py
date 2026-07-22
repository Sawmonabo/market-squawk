"""Bounded native bundle candidate publication through the Rust Task 13 validator."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Mapping

from . import _native


MAX_ARTIFACT_BYTES = 1024 * 1024
MAX_METADATA_BYTES = 256 * 1024
MAX_AUTHORITY_BYTES = 256 * 1024


class BundleExportError(ValueError):
    """A bundle candidate was not safely written and admitted by Rust."""


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
        if authority_root.is_symlink() or not authority_root.is_dir():
            raise BundleExportError("bundle authority root is not a controlled directory")
        parts = _relative_parts(relative_path)
        path = authority_root.joinpath(*parts)
        if path.is_symlink() or not path.is_file():
            raise BundleExportError("bundle authority is not a controlled regular file")
        content = path.read_bytes()
        if len(content) > MAX_AUTHORITY_BYTES or hashlib.sha256(content).hexdigest() != sha256:
            raise BundleExportError("bundle authority hash or size mismatch")
        return cls(authority_root.resolve(), relative_path, sha256)


@dataclass(frozen=True)
class BundleCandidate:
    metadata: Mapping[str, Any]
    artifact: Mapping[str, Any]
    run_record: Mapping[str, Any]

    @property
    def training_run_sha256(self) -> str:
        return hashlib.sha256(_canonical(self.run_record, MAX_METADATA_BYTES)).hexdigest()

    def write(
        self,
        output_root: Path | str,
        authority: BundleAuthorityRef,
        *,
        dataset_receipt: Any,
        validator: Path | str | None = None,
    ) -> BundleReceipt:
        if type(dataset_receipt) is not _native.DatasetReceipt:
            raise BundleExportError("bundle publication requires a native dataset receipt")
        output_root = Path(output_root)
        if output_root.is_symlink() or not output_root.is_dir():
            raise BundleExportError("bundle output root is not a controlled directory")
        resolved_output = output_root.resolve()
        if resolved_output == authority.root or resolved_output.is_relative_to(authority.root):
            raise BundleExportError("bundle candidate cannot contain its independent authority")
        if authority.root.is_relative_to(resolved_output):
            raise BundleExportError("bundle authority cannot be nested below candidate output")
        final = output_root / "candidate"
        if final.exists() or final.is_symlink():
            raise BundleExportError("bundle candidate generation already exists")
        temporary = Path(tempfile.mkdtemp(prefix=".candidate-", dir=output_root))
        try:
            artifact_bytes = _canonical(self.artifact, MAX_ARTIFACT_BYTES)
            artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
            run_bytes = _canonical(self.run_record, MAX_METADATA_BYTES)
            training_run_sha256 = hashlib.sha256(run_bytes).hexdigest()
            metadata = dict(self.metadata)
            metadata["artifact"] = dict(metadata["artifact"])
            metadata["artifact"]["sha256"] = artifact_sha256
            metadata["artifact"]["size_bytes"] = len(artifact_bytes)
            metadata["training_run"] = dict(metadata["training_run"])
            metadata["training_run"]["sha256"] = training_run_sha256
            metadata["training_run"]["size_bytes"] = len(run_bytes)
            metadata_bytes = _canonical(metadata, MAX_METADATA_BYTES)
            metadata_sha256 = hashlib.sha256(metadata_bytes).hexdigest()
            _write_exact(temporary / "artifact.json", artifact_bytes)
            _write_exact(temporary / "bundle.json", metadata_bytes)
            _write_exact(temporary / "training-run.json", run_bytes)
            _fsync_directory(temporary)
            _validate_with_rust(
                temporary,
                metadata_sha256,
                authority,
                dataset_receipt,
                validator,
            )
            os.replace(temporary, final)
            _fsync_directory(output_root)
            return BundleReceipt(
                root=final,
                metadata=final / "bundle.json",
                artifact=final / "artifact.json",
                run_record=final / "training-run.json",
                metadata_sha256=metadata_sha256,
                artifact_sha256=artifact_sha256,
                training_run_sha256=training_run_sha256,
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
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
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
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validator_path(configured: Path | str | None) -> Path:
    if configured is not None:
        path = Path(configured)
    else:
        adjacent = Path(sys.executable).parent / "market-squawk-model-validator"
        discovered = shutil.which("market-squawk-model-validator")
        path = adjacent if adjacent.is_file() else Path(discovered) if discovered else adjacent
    if path.is_symlink() or not path.is_file() or not os.access(path, os.X_OK):
        raise BundleExportError("Rust model bundle validator is unavailable")
    return path


def _validate_with_rust(
    root: Path,
    metadata_sha256: str,
    authority: BundleAuthorityRef,
    dataset_receipt: Any,
    configured_validator: Path | str | None,
) -> None:
    command = [
        str(_validator_path(configured_validator)),
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
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise BundleExportError("Rust model bundle validation could not complete") from error
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
