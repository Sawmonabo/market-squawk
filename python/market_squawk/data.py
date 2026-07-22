"""Task 11 export-bound, bounded local feature/label dataset access."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import stat
from types import MappingProxyType
from typing import Any, Mapping
from uuid import UUID

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


EXPORT_NAME = "feature-label-export.json"
MAX_EXPORT_BYTES = 1024 * 1024
MAX_OBJECTS = 128
MAX_COMPONENTS = 1_024
MAX_PARENTS = 64
DEFAULT_MAX_ROWS = 100_000
DEFAULT_MAX_BYTES = 256 * 1024 * 1024
MAX_PARQUET_ROW_GROUPS = 4_096
SCHEMA_NAME = "market_squawk.feature_label_components"
SCHEMA_VERSION = 1
REQUIRED_METADATA = {
    b"market_squawk.build_sha256",
    b"market_squawk.component_layout",
    b"market_squawk.dataset",
    b"market_squawk.policy_sha256",
    b"market_squawk.schema",
    b"market_squawk.schema_fingerprint_sha256",
    b"market_squawk.schema_version",
    b"market_squawk.timestamp_timezone",
    b"market_squawk.universe_sha256",
}


class DatasetIntegrityError(ValueError):
    """An immutable Task 11 identity, resource bound, or PIT contract failed."""


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
    def from_datetime(cls, value: datetime) -> UtcNanoseconds:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")
        utc = value.astimezone(timezone.utc)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        delta = utc - epoch
        return cls(
            delta.days * 86_400_000_000_000
            + delta.seconds * 1_000_000_000
            + delta.microseconds * 1_000
        )

    def to_datetime(self) -> datetime:
        seconds, nanos = divmod(self.unix_nanos, 1_000_000_000)
        return datetime.fromtimestamp(seconds, timezone.utc).replace(microsecond=nanos // 1_000)


@dataclass(frozen=True)
class DatasetIdentity:
    dataset_id: str
    manifest_version: int
    schema_name: str
    schema_version: int
    schema_sha256: str
    manifest_sha256: str
    build_spec_sha256: str
    universe_sha256: str
    policy_sha256: str

    def bundle_mapping(self) -> Mapping[str, Any]:
        return MappingProxyType(
            {
                "dataset_id": self.dataset_id,
                "manifest_version": self.manifest_version,
                "schema_name": self.schema_name,
                "schema_version": self.schema_version,
                "schema_sha256": self.schema_sha256,
                "manifest_sha256": self.manifest_sha256,
                "build_spec_sha256": self.build_spec_sha256,
                "universe_sha256": self.universe_sha256,
                "policy_sha256": self.policy_sha256,
            }
        )


@dataclass(frozen=True, order=True)
class ComponentIdentity:
    corporate_action_sensitivity: str
    kind: str
    name: str
    scope: str
    version: int

    def mapping(self) -> Mapping[str, Any]:
        return MappingProxyType(
            {
                "corporate_action_sensitivity": self.corporate_action_sensitivity,
                "kind": self.kind,
                "name": self.name,
                "scope": self.scope,
                "version": self.version,
            }
        )


@dataclass(frozen=True)
class SplitPolicy:
    train_end_unix_nanos: int
    validation_end_unix_nanos: int
    test_end_unix_nanos: int

    def split_for(self, cutoff: int) -> str | None:
        if cutoff <= self.train_end_unix_nanos:
            return "train"
        if cutoff <= self.validation_end_unix_nanos:
            return "validation"
        if cutoff <= self.test_end_unix_nanos:
            return "test"
        return None


@dataclass(frozen=True)
class SplitCounts:
    train: int
    validation: int
    test: int


@dataclass(frozen=True)
class DatasetResult:
    """A bounded PIT selection tied to one exact Task 11 producer export."""

    export_sha256: str
    identity: DatasetIdentity
    universe_id: str
    as_of: UtcNanoseconds
    rows: tuple[Mapping[str, Any], ...]
    components: tuple[ComponentIdentity, ...]
    split_policy: SplitPolicy
    split_counts: SplitCounts
    missing_value_policy: str
    complete: bool

    @property
    def dataset_id(self) -> str:
        return self.identity.dataset_id

    @property
    def manifest_version(self) -> int:
        return self.identity.manifest_version

    @property
    def manifest_sha256(self) -> str:
        return self.identity.manifest_sha256

    @property
    def schema_name(self) -> str:
        return self.identity.schema_name

    @property
    def schema_version(self) -> int:
        return self.identity.schema_version


class _ControlledRoot:
    def __init__(self, root: Path) -> None:
        root = Path(root)
        if root.is_symlink() or not root.is_dir():
            raise DatasetIntegrityError("dataset root is not a controlled directory")
        self._fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))

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
    export_sha256: str,
    as_of: UtcNanoseconds | datetime,
    *,
    max_rows: int = DEFAULT_MAX_ROWS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> DatasetResult:
    """Read one exact Task 11 export and its explicitly named content-addressed objects."""

    export_digest = _digest(export_sha256)
    if not 1 <= max_rows <= DEFAULT_MAX_ROWS or not 1 <= max_bytes <= DEFAULT_MAX_BYTES:
        raise DatasetIntegrityError("dataset result limits are invalid")
    cutoff = as_of if isinstance(as_of, UtcNanoseconds) else UtcNanoseconds.from_datetime(as_of)
    controlled = _ControlledRoot(Path(root))
    try:
        export_bytes = controlled.read(EXPORT_NAME, MAX_EXPORT_BYTES)
        if hashlib.sha256(export_bytes).digest() != export_digest:
            raise DatasetIntegrityError("Task 11 export hash mismatch")
        manifest = _export(export_bytes)
        objects = manifest["objects"]
        total_rows = sum(_positive_int(item["row_count"]) for item in objects)
        total_bytes = sum(_positive_int(item["size_bytes"]) for item in objects)
        if total_rows > DEFAULT_MAX_ROWS or total_bytes > max_bytes:
            raise DatasetIntegrityError("dataset objects exceed the reader bounds")

        tables: list[pa.Table] = []
        retained_table_bytes = 0
        for item in objects:
            object_bytes = controlled.read(item["path"], max_bytes)
            if len(object_bytes) != item["size_bytes"]:
                raise DatasetIntegrityError("dataset object size mismatch")
            if hashlib.sha256(object_bytes).digest() != _digest(item["sha256"]):
                raise DatasetIntegrityError("dataset object hash mismatch")
            try:
                parquet_file = _preflight_parquet(
                    object_bytes,
                    max_bytes=max_bytes - retained_table_bytes,
                    expected_rows=item["row_count"],
                )
                table = parquet_file.read()
            except (pa.ArrowException, OSError, OverflowError, ValueError) as error:
                raise DatasetIntegrityError("dataset Parquet object is invalid") from error
            if table.num_rows != item["row_count"]:
                raise DatasetIntegrityError("dataset object row count mismatch")
            retained_table_bytes += table.nbytes
            if retained_table_bytes > max_bytes:
                raise DatasetIntegrityError("dataset Arrow tables exceed the retained-byte bound")
            _validate_schema(table.schema, manifest["dataset"])
            _validate_object_lineage(table, item["lineage_sha256"], manifest["dataset"])
            tables.append(table)

        table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        components = tuple(_component(value) for value in manifest["components"])
        split_policy = _split_policy(manifest["split_policy"])
        expected_counts = _split_counts(manifest["split_counts"])
        _validate_rows(table, components, split_policy, expected_counts)
        field = table.schema.field("cutoff_at")
        selected = table.filter(
            pc.less_equal(table["cutoff_at"], pa.scalar(cutoff.unix_nanos, type=field.type))
        )
        if selected.num_rows > max_rows or selected.nbytes > max_bytes:
            raise DatasetIntegrityError("dataset result exceeds its requested bound")
        rows = tuple(
            MappingProxyType(dict(_row(selected, index))) for index in range(selected.num_rows)
        )
        dataset = manifest["dataset"]
        identity = DatasetIdentity(
            dataset_id=dataset["dataset_id"],
            manifest_version=dataset["manifest_version"],
            schema_name=dataset["schema_name"],
            schema_version=dataset["schema_version"],
            schema_sha256=dataset["schema_sha256"],
            manifest_sha256=dataset["manifest_sha256"],
            build_spec_sha256=dataset["build_spec_sha256"],
            universe_sha256=dataset["universe_sha256"],
            policy_sha256=dataset["policy_sha256"],
        )
        return DatasetResult(
            export_sha256=export_sha256,
            identity=identity,
            universe_id=dataset["universe_id"],
            as_of=cutoff,
            rows=rows,
            components=components,
            split_policy=split_policy,
            split_counts=expected_counts,
            missing_value_policy=manifest["missing_value_policy"],
            complete=cutoff.unix_nanos >= split_policy.test_end_unix_nanos,
        )
    finally:
        controlled.close()


def _preflight_parquet(
    content: bytes,
    *,
    max_bytes: int,
    expected_rows: int,
) -> pq.ParquetFile:
    """Reject declared Parquet expansion before allocating Arrow column buffers."""

    if max_bytes <= 0:
        raise DatasetIntegrityError("dataset Arrow tables exceed the retained-byte bound")
    parquet_file = pq.ParquetFile(pa.BufferReader(content))
    metadata = parquet_file.metadata
    if (
        metadata is None
        or metadata.num_rows != expected_rows
        or not 1 <= metadata.num_row_groups <= MAX_PARQUET_ROW_GROUPS
    ):
        raise DatasetIntegrityError("dataset Parquet metadata is invalid")
    uncompressed = 0
    for row_group_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(row_group_index)
        for column_index in range(row_group.num_columns):
            size = row_group.column(column_index).total_uncompressed_size
            if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                raise DatasetIntegrityError("dataset Parquet column size is invalid")
            uncompressed += size
            if uncompressed > max_bytes:
                raise DatasetIntegrityError("dataset Parquet expansion exceeds the byte bound")
    return parquet_file


def _export(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DatasetIntegrityError("Task 11 export syntax is invalid") from error
    top = {
        "components",
        "dataset",
        "missing_value_policy",
        "objects",
        "parents",
        "point_in_time",
        "schema_version",
        "split_counts",
        "split_policy",
    }
    if not isinstance(value, dict) or set(value) != top or value["schema_version"] != 1:
        raise DatasetIntegrityError("Task 11 export version or shape is unsupported")
    dataset = value["dataset"]
    dataset_keys = {
        "build_spec_sha256",
        "dataset_id",
        "manifest_sha256",
        "manifest_version",
        "policy_sha256",
        "schema_name",
        "schema_sha256",
        "schema_version",
        "universe_id",
        "universe_sha256",
    }
    if not isinstance(dataset, dict) or set(dataset) != dataset_keys:
        raise DatasetIntegrityError("Task 11 dataset identity is incomplete")
    if (
        dataset["schema_name"] != SCHEMA_NAME
        or dataset["schema_version"] != SCHEMA_VERSION
        or not _identifier(dataset["dataset_id"])
        or not _identifier(dataset["universe_id"])
        or _positive_int(dataset["manifest_version"]) < 1
    ):
        raise DatasetIntegrityError("Task 11 dataset identity is unsupported")
    for name in (
        "build_spec_sha256",
        "manifest_sha256",
        "policy_sha256",
        "schema_sha256",
        "universe_sha256",
    ):
        if _digest(dataset[name]) == bytes(32):
            raise DatasetIntegrityError("Task 11 dataset identity is reserved")
    _validate_components(value["components"])
    _validate_objects(value["objects"])
    _validate_parents(value["parents"])
    if value["point_in_time"] != {"revision_mode": "latest_known", "version": 1}:
        raise DatasetIntegrityError("Task 11 point-in-time policy is unsupported")
    if value["missing_value_policy"] not in {"reject", "preserve", "drop_example"}:
        raise DatasetIntegrityError("Task 11 missing-value policy is unsupported")
    _split_policy(value["split_policy"])
    counts = _split_counts(value["split_counts"])
    if counts.train + counts.validation + counts.test == 0:
        raise DatasetIntegrityError("Task 11 split counts are empty")
    return value


def _validate_components(values: Any) -> None:
    keys = {"corporate_action_sensitivity", "kind", "name", "scope", "version"}
    if not isinstance(values, list) or not 1 < len(values) <= MAX_COMPONENTS:
        raise DatasetIntegrityError("Task 11 component contract count is invalid")
    identities: set[tuple[str, str, int]] = set()
    kinds: set[str] = set()
    for value in values:
        if not isinstance(value, dict) or set(value) != keys:
            raise DatasetIntegrityError("Task 11 component contract is invalid")
        component = _component(value)
        identity = (component.kind, component.name, component.version)
        if identity in identities:
            raise DatasetIntegrityError("Task 11 component contract is duplicated")
        identities.add(identity)
        kinds.add(component.kind)
    if kinds != {"feature", "label"}:
        raise DatasetIntegrityError("Task 11 export needs feature and label contracts")


def _validate_objects(values: Any) -> None:
    keys = {"artifact_id", "lineage_sha256", "path", "row_count", "sha256", "size_bytes"}
    if not isinstance(values, list) or not values or len(values) > MAX_OBJECTS:
        raise DatasetIntegrityError("Task 11 object count is invalid")
    paths: set[str] = set()
    for value in values:
        if not isinstance(value, dict) or set(value) != keys:
            raise DatasetIntegrityError("Task 11 object identity is invalid")
        _canonical_uuid(value["artifact_id"])
        _path_parts(value["path"])
        for name in ("sha256", "lineage_sha256"):
            if _digest(value[name]) == bytes(32):
                raise DatasetIntegrityError("Task 11 object digest is reserved")
        _positive_int(value["row_count"])
        _positive_int(value["size_bytes"])
        if value["path"] in paths:
            raise DatasetIntegrityError("Task 11 object path is duplicated")
        paths.add(value["path"])


def _validate_parents(values: Any) -> None:
    manifest_keys = {
        "dataset_id",
        "manifest_sha256",
        "manifest_version",
        "schema_name",
        "schema_sha256",
        "schema_version",
    }
    if not isinstance(values, list) or not values or len(values) > MAX_PARENTS:
        raise DatasetIntegrityError("Task 11 parent count is invalid")
    for value in values:
        if (
            not isinstance(value, dict)
            or set(value) != {"manifest", "relation"}
            or value["relation"] != "derived_input"
            or not isinstance(value["manifest"], dict)
            or set(value["manifest"]) != manifest_keys
        ):
            raise DatasetIntegrityError("Task 11 parent identity is invalid")
        manifest = value["manifest"]
        if not _identifier(manifest["dataset_id"]) or not _identifier(manifest["schema_name"]):
            raise DatasetIntegrityError("Task 11 parent identity is invalid")
        _positive_int(manifest["manifest_version"])
        _positive_int(manifest["schema_version"])
        _digest(manifest["manifest_sha256"])
        _digest(manifest["schema_sha256"])


def _validate_schema(schema: pa.Schema, dataset: Mapping[str, Any]) -> None:
    fields = [
        ("example_id", pa.string(), False),
        ("instrument_id", pa.string(), False),
        ("cutoff_at", pa.timestamp("ns", tz="+00:00"), False),
        ("split", pa.string(), False),
        ("component_kind", pa.string(), False),
        ("component_name", pa.string(), False),
        ("component_version", pa.uint32(), False),
        ("value_f64", pa.float64(), True),
        ("value_decimal_mantissa", pa.decimal128(38, 0), True),
        ("value_decimal_scale", pa.uint8(), True),
        ("unit", pa.string(), True),
        ("currency", pa.string(), True),
        ("missing_reason", pa.string(), True),
        ("lineage_sha256", pa.binary(32), False),
    ]
    if len(schema) != len(fields) or any(
        schema.field(index).name != name
        or schema.field(index).type != arrow_type
        or schema.field(index).nullable != nullable
        for index, (name, arrow_type, nullable) in enumerate(fields)
    ):
        raise DatasetIntegrityError("dataset Arrow field schema is unsupported")
    expected_metadata = {
        b"market_squawk.build_sha256": dataset["build_spec_sha256"].encode(),
        b"market_squawk.component_layout": b"typed-long-form-v1",
        b"market_squawk.dataset": dataset["dataset_id"].encode(),
        b"market_squawk.policy_sha256": dataset["policy_sha256"].encode(),
        b"market_squawk.schema": SCHEMA_NAME.encode(),
        b"market_squawk.schema_fingerprint_sha256": dataset["schema_sha256"].encode(),
        b"market_squawk.schema_version": b"1",
        b"market_squawk.timestamp_timezone": b"UTC",
        b"market_squawk.universe_sha256": dataset["universe_sha256"].encode(),
    }
    if set(schema.metadata or {}) != REQUIRED_METADATA or schema.metadata != expected_metadata:
        raise DatasetIntegrityError("dataset Arrow authority metadata mismatch")


def _validate_object_lineage(table: pa.Table, expected: str, dataset: Mapping[str, Any]) -> None:
    digest = hashlib.sha256()
    digest.update(b"market-squawk/feature-label-object-lineage/v1")
    digest.update(_digest(dataset["build_spec_sha256"]))
    for value in table["lineage_sha256"].to_pylist():
        if not isinstance(value, bytes) or len(value) != 32 or value == bytes(32):
            raise DatasetIntegrityError("dataset row lineage is invalid")
        digest.update(value)
    if digest.digest() != _digest(expected):
        raise DatasetIntegrityError("dataset object lineage mismatch")


def _validate_rows(
    table: pa.Table,
    components: tuple[ComponentIdentity, ...],
    policy: SplitPolicy,
    expected_counts: SplitCounts,
) -> None:
    required = (
        "example_id",
        "instrument_id",
        "cutoff_at",
        "split",
        "component_kind",
        "component_name",
        "component_version",
        "lineage_sha256",
    )
    if any(table[name].null_count for name in required):
        raise DatasetIntegrityError("dataset required row identity is null")
    expected_component_keys = tuple((item.kind, item.name, item.version) for item in components)
    previous_example: tuple[int, str, str] | None = None
    current_example: tuple[int, str, str] | None = None
    current_components: list[tuple[str, str, int]] = []
    counts = {"train": 0, "validation": 0, "test": 0}
    for index in range(table.num_rows):
        row = _row(table, index)
        cutoff = row["cutoff_at"].unix_nanos
        key = (cutoff, row["instrument_id"], row["example_id"])
        _canonical_uuid(row["instrument_id"])
        if not _identifier(row["example_id"]):
            raise DatasetIntegrityError("dataset example identity is invalid")
        expected_split = policy.split_for(cutoff)
        if expected_split is None or row["split"] != expected_split:
            raise DatasetIntegrityError("dataset row violates chronological split policy")
        component_key = (
            row["component_kind"],
            row["component_name"],
            row["component_version"],
        )
        if current_example is None:
            current_example = key
        elif key != current_example:
            _close_example(previous_example, current_example, current_components, expected_component_keys)
            previous_example = current_example
            counts[row_for_split(policy, current_example[0])] += 1
            current_example = key
            current_components = []
        current_components.append(component_key)
        _validate_value(row)
    if current_example is None:
        raise DatasetIntegrityError("dataset contains no feature/label rows")
    _close_example(previous_example, current_example, current_components, expected_component_keys)
    counts[row_for_split(policy, current_example[0])] += 1
    if counts != {
        "train": expected_counts.train,
        "validation": expected_counts.validation,
        "test": expected_counts.test,
    }:
        raise DatasetIntegrityError("dataset split counts differ from Task 11 export")


def _close_example(
    previous: tuple[int, str, str] | None,
    current: tuple[int, str, str],
    components: list[tuple[str, str, int]],
    expected_components: tuple[tuple[str, str, int], ...],
) -> None:
    if previous is not None and current <= previous:
        raise DatasetIntegrityError("dataset examples are not in deterministic chronological order")
    if tuple(components) != expected_components:
        raise DatasetIntegrityError("dataset example component contract is incomplete or reordered")


def _validate_value(row: Mapping[str, Any]) -> None:
    present = sum(
        value is not None
        for value in (row["value_f64"], row["value_decimal_mantissa"], row["missing_reason"])
    )
    scale = row["value_decimal_scale"]
    if (
        present != 1
        or (row["value_decimal_mantissa"] is None) != (scale is None)
        or (scale is not None and not 0 <= scale <= 28)
        or (row["value_f64"] is not None and not math.isfinite(row["value_f64"]))
        or not _identifier(row["component_name"])
        or row["component_kind"] not in {"feature", "label"}
        or not isinstance(row["component_version"], int)
        or row["component_version"] <= 0
        or (row["missing_reason"] is not None and not _identifier(row["missing_reason"]))
    ):
        raise DatasetIntegrityError("dataset component value is invalid")


def row_for_split(policy: SplitPolicy, cutoff: int) -> str:
    value = policy.split_for(cutoff)
    if value is None:
        raise DatasetIntegrityError("dataset cutoff is outside split policy")
    return value


def _component(value: Mapping[str, Any]) -> ComponentIdentity:
    if (
        value["kind"] not in {"feature", "label"}
        or value["scope"] not in {"instrument", "account", "global"}
        or value["corporate_action_sensitivity"]
        not in {"not_applicable", "requires_adjustment"}
        or not _identifier(value["name"])
        or not isinstance(value["version"], int)
        or isinstance(value["version"], bool)
        or value["version"] <= 0
    ):
        raise DatasetIntegrityError("Task 11 component contract is invalid")
    return ComponentIdentity(
        corporate_action_sensitivity=value["corporate_action_sensitivity"],
        kind=value["kind"],
        name=value["name"],
        scope=value["scope"],
        version=value["version"],
    )


def _split_policy(value: Any) -> SplitPolicy:
    keys = {"test_end_unix_nanos", "train_end_unix_nanos", "validation_end_unix_nanos"}
    if not isinstance(value, dict) or set(value) != keys:
        raise DatasetIntegrityError("Task 11 split policy is invalid")
    ends = tuple(value[name] for name in ("train_end_unix_nanos", "validation_end_unix_nanos", "test_end_unix_nanos"))
    if any(not isinstance(item, int) or isinstance(item, bool) for item in ends) or not ends[0] < ends[1] < ends[2]:
        raise DatasetIntegrityError("Task 11 split policy is invalid")
    return SplitPolicy(*ends)


def _split_counts(value: Any) -> SplitCounts:
    if not isinstance(value, dict) or set(value) != {"test", "train", "validation"}:
        raise DatasetIntegrityError("Task 11 split counts are invalid")
    counts = tuple(value[name] for name in ("train", "validation", "test"))
    if any(not isinstance(item, int) or isinstance(item, bool) or item < 0 for item in counts):
        raise DatasetIntegrityError("Task 11 split counts are invalid")
    return SplitCounts(*counts)


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
        raise DatasetIntegrityError("Task 11 count is invalid")
    return value


def _identifier(value: Any) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value.encode()) <= 256
        and value[0].isalnum()
        and value[-1].isalnum()
        and all(character.islower() or character.isdigit() or character in "._-:/" for character in value)
    )


def _canonical_uuid(value: Any) -> None:
    try:
        parsed = UUID(value) if isinstance(value, str) else None
    except (ValueError, AttributeError) as error:
        raise DatasetIntegrityError("UUID identity is invalid") from error
    if parsed is None or str(parsed) != value:
        raise DatasetIntegrityError("UUID identity is invalid")
