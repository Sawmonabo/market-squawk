"""Manifest-bound, bounded local Parquet access with exact financial conversions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
from typing import Any, Mapping

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


MAX_MANIFEST_BYTES = 256 * 1024
MAX_OBJECTS = 128
DEFAULT_MAX_ROWS = 100_000
DEFAULT_MAX_BYTES = 256 * 1024 * 1024


class DatasetIntegrityError(ValueError):
    """An immutable dataset identity, resource bound, or PIT contract failed."""


@dataclass(frozen=True, order=True)
class UtcNanoseconds:
    """Exact signed Unix nanoseconds known to represent UTC."""

    unix_nanos: int

    def __post_init__(self) -> None:
        if not isinstance(self.unix_nanos, int) or isinstance(self.unix_nanos, bool):
            raise TypeError("UTC nanoseconds must be an integer")
        if not -(2**63) <= self.unix_nanos < 2**63:
            raise ValueError("UTC nanoseconds exceed the signed 64-bit domain")

    @classmethod
    def from_datetime(cls, value: datetime) -> "UtcNanoseconds":
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")
        utc = value.astimezone(timezone.utc)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        delta = utc - epoch
        nanos = (
            delta.days * 86_400_000_000_000
            + delta.seconds * 1_000_000_000
            + delta.microseconds * 1_000
        )
        return cls(nanos)

    def to_datetime(self) -> datetime:
        seconds, nanos = divmod(self.unix_nanos, 1_000_000_000)
        return datetime.fromtimestamp(seconds, timezone.utc).replace(microsecond=nanos // 1_000)


@dataclass(frozen=True)
class DatasetResult:
    """A bounded immutable PIT selection and its admitted generation identity."""

    dataset_id: str
    manifest_version: int
    manifest_sha256: str
    schema_name: str
    schema_version: int
    as_of: UtcNanoseconds
    rows: tuple[Mapping[str, Any], ...]
    required_provenance: tuple[str, ...]


class _ControlledRoot:
    def __init__(self, root: Path) -> None:
        root = Path(root)
        if root.is_symlink() or not root.is_dir():
            raise DatasetIntegrityError("dataset root is not a controlled directory")
        self._fd = os.open(
            root,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
        )

    def close(self) -> None:
        os.close(self._fd)

    def read(self, relative: str, maximum: int) -> bytes:
        parts = _path_parts(relative)
        directory_fd = os.dup(self._fd)
        try:
            for part in parts[:-1]:
                next_fd = os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=directory_fd,
                )
                os.close(directory_fd)
                directory_fd = next_fd
            file_fd = os.open(
                parts[-1],
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory_fd,
            )
            try:
                metadata = os.fstat(file_fd)
                if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > maximum:
                    raise DatasetIntegrityError("dataset object is not a bounded regular file")
                chunks: list[bytes] = []
                retained = 0
                while True:
                    chunk = os.read(file_fd, min(1024 * 1024, maximum + 1 - retained))
                    if not chunk:
                        break
                    retained += len(chunk)
                    if retained > maximum:
                        raise DatasetIntegrityError("dataset object exceeds its byte bound")
                    chunks.append(chunk)
                return b"".join(chunks)
            finally:
                os.close(file_fd)
        except OSError as error:
            raise DatasetIntegrityError("controlled dataset read failed") from error
        finally:
            os.close(directory_fd)


def open_dataset(
    root: Path | str,
    manifest_sha256: str,
    as_of: UtcNanoseconds | datetime,
    *,
    max_rows: int = DEFAULT_MAX_ROWS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> DatasetResult:
    """Open one exact local generation and select rows available no later than ``as_of``.

    The reader never discovers files. It reads only ``manifest.json`` and the exact controlled
    relative objects named by that hash-bound manifest.
    """

    digest = _digest(manifest_sha256)
    if not 1 <= max_rows <= DEFAULT_MAX_ROWS or not 1 <= max_bytes <= DEFAULT_MAX_BYTES:
        raise DatasetIntegrityError("dataset result limits are invalid")
    cutoff = as_of if isinstance(as_of, UtcNanoseconds) else UtcNanoseconds.from_datetime(as_of)
    controlled = _ControlledRoot(Path(root))
    try:
        manifest_bytes = controlled.read("manifest.json", MAX_MANIFEST_BYTES)
        if hashlib.sha256(manifest_bytes).digest() != digest:
            raise DatasetIntegrityError("dataset manifest hash mismatch")
        manifest = _manifest(manifest_bytes)
        objects = manifest["objects"]
        total_declared_bytes = sum(_positive_int(item["size_bytes"]) for item in objects)
        if total_declared_bytes > max_bytes:
            raise DatasetIntegrityError("dataset objects exceed the requested byte bound")

        tables: list[pa.Table] = []
        expected_schema = _digest(manifest["schema"]["arrow_sha256"])
        for item in objects:
            object_bytes = controlled.read(item["path"], max_bytes)
            if len(object_bytes) != _positive_int(item["size_bytes"]):
                raise DatasetIntegrityError("dataset object size mismatch")
            if hashlib.sha256(object_bytes).digest() != _digest(item["sha256"]):
                raise DatasetIntegrityError("dataset object hash mismatch")
            try:
                table = pq.read_table(pa.BufferReader(object_bytes))
            except (pa.ArrowException, OSError) as error:
                raise DatasetIntegrityError("dataset Parquet object is invalid") from error
            if table.num_rows != _positive_int(item["row_count"]):
                raise DatasetIntegrityError("dataset object row count mismatch")
            if hashlib.sha256(table.schema.serialize().to_pybytes()).digest() != expected_schema:
                raise DatasetIntegrityError("dataset Arrow schema mismatch")
            tables.append(table)

        table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        column = manifest["as_of_column"]
        if column not in table.column_names:
            raise DatasetIntegrityError("dataset availability column is absent")
        field = table.schema.field(column)
        if field.type != pa.timestamp("ns", tz="UTC") or table[column].null_count:
            raise DatasetIntegrityError("dataset availability evidence is not exact UTC nanoseconds")
        for provenance in manifest["required_provenance"]:
            if provenance not in table.column_names or table[provenance].null_count:
                raise DatasetIntegrityError("dataset provenance is incomplete")
        mask = pc.less_equal(table[column], pa.scalar(cutoff.unix_nanos, type=field.type))
        selected = table.filter(mask)
        if selected.num_rows > max_rows or selected.nbytes > max_bytes:
            raise DatasetIntegrityError("dataset result exceeds its requested bound")
        rows = tuple(_row(selected, index) for index in range(selected.num_rows))
        return DatasetResult(
            dataset_id=manifest["dataset_id"],
            manifest_version=_positive_int(manifest["manifest_version"]),
            manifest_sha256=manifest_sha256,
            schema_name=manifest["schema"]["name"],
            schema_version=_positive_int(manifest["schema"]["version"]),
            as_of=cutoff,
            rows=rows,
            required_provenance=tuple(manifest["required_provenance"]),
        )
    finally:
        controlled.close()


def _manifest(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DatasetIntegrityError("dataset manifest syntax is invalid") from error
    expected = {
        "schema_version",
        "dataset_id",
        "manifest_version",
        "schema",
        "as_of_column",
        "required_provenance",
        "objects",
    }
    if not isinstance(value, dict) or set(value) != expected or value["schema_version"] != 1:
        raise DatasetIntegrityError("dataset manifest version or shape is unsupported")
    if not _identifier(value["dataset_id"]) or not _identifier(value["as_of_column"]):
        raise DatasetIntegrityError("dataset manifest identity is invalid")
    schema = value["schema"]
    if not isinstance(schema, dict) or set(schema) != {"name", "version", "arrow_sha256"}:
        raise DatasetIntegrityError("dataset schema identity is invalid")
    if not _identifier(schema["name"]):
        raise DatasetIntegrityError("dataset schema name is invalid")
    provenance = value["required_provenance"]
    if (
        not isinstance(provenance, list)
        or not provenance
        or len(provenance) > 32
        or len(set(provenance)) != len(provenance)
        or any(not _identifier(item) for item in provenance)
    ):
        raise DatasetIntegrityError("dataset provenance contract is invalid")
    objects = value["objects"]
    if not isinstance(objects, list) or not objects or len(objects) > MAX_OBJECTS:
        raise DatasetIntegrityError("dataset object count is invalid")
    paths: set[str] = set()
    for item in objects:
        if not isinstance(item, dict) or set(item) != {
            "path",
            "sha256",
            "size_bytes",
            "row_count",
        }:
            raise DatasetIntegrityError("dataset object identity is invalid")
        _path_parts(item["path"])
        _digest(item["sha256"])
        _positive_int(item["size_bytes"])
        _positive_int(item["row_count"])
        if item["path"] in paths:
            raise DatasetIntegrityError("dataset object path is duplicated")
        paths.add(item["path"])
    _digest(schema["arrow_sha256"])
    _positive_int(value["manifest_version"])
    _positive_int(schema["version"])
    return value


def _row(table: pa.Table, index: int) -> Mapping[str, Any]:
    values: dict[str, Any] = {}
    for name in table.column_names:
        scalar = table[name][index]
        if not scalar.is_valid:
            values[name] = None
        elif pa.types.is_timestamp(scalar.type):
            values[name] = UtcNanoseconds(int(scalar.value))
        else:
            value = scalar.as_py()
            values[name] = bytes(value) if isinstance(value, (bytes, bytearray)) else value
    return values


def _path_parts(value: Any) -> tuple[str, ...]:
    if not isinstance(value, str) or not value or len(value.encode()) > 256 or "\\" in value or ":" in value:
        raise DatasetIntegrityError("dataset object path is invalid")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise DatasetIntegrityError("dataset object path is invalid")
    if any(not all(character.isalnum() or character in "._-" for character in part) for part in path.parts):
        raise DatasetIntegrityError("dataset object path is invalid")
    return path.parts


def _digest(value: Any) -> bytes:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise DatasetIntegrityError("SHA-256 identity is invalid")
    return bytes.fromhex(value)


def _positive_int(value: Any) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise DatasetIntegrityError("manifest count is invalid")
    return value


def _identifier(value: Any) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value.encode()) <= 128
        and all(character.isalnum() or character in "._-" for character in value)
    )
