"""Closed Task 11 descriptor, Arrow schema, row, and value validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any, Mapping
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq


MAX_EXPORT_BYTES = 1024 * 1024
MAX_OBJECTS = 128
MAX_COMPONENTS = 1_024
MAX_PARENTS = 64
DEFAULT_MAX_ROWS = 100_000
DEFAULT_MAX_BYTES = 256 * 1024 * 1024
MAX_PARQUET_ROW_GROUPS = 4_096
SCHEMA_NAME = "market_squawk.feature_label_components"
SCHEMA_VERSION = 2
DECODED_BYTES_PER_ROW = 1_024
DECODED_ROW_GROUP_OVERHEAD = 64 * 1_024
SELECTED_ROW_RETAINED_BYTES = 4_096
CONTROL_RETAINED_OVERHEAD = 64 * 1_024
CONTROL_EXPANSION = 16
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
    catalog_identity_sha256: str
    export_sha256: str
    selection_sha256: str
    selection_as_of_unix_nanos: int
    selected_component_rows: int

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
                "catalog_identity_sha256": self.catalog_identity_sha256,
                "export_sha256": self.export_sha256,
                "selection_sha256": self.selection_sha256,
                "selection_as_of_unix_nanos": self.selection_as_of_unix_nanos,
                "selected_component_rows": self.selected_component_rows,
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
    if not _fixed_schema_shape(parquet_file.schema_arrow):
        raise DatasetIntegrityError("dataset Parquet physical schema is unsupported")
    for row_group_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(row_group_index)
        uncompressed = 0
        for column_index in range(row_group.num_columns):
            column = row_group.column(column_index)
            size = column.total_uncompressed_size
            if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                raise DatasetIntegrityError("dataset Parquet column size is invalid")
            uncompressed += size
            if any("DICTIONARY" in str(encoding) for encoding in column.encodings):
                raise DatasetIntegrityError("dataset Parquet dictionary encoding is unsupported")
        if max(_decoded_row_group_bound(row_group.num_rows), uncompressed) > max_bytes:
            raise DatasetIntegrityError("dataset Parquet expansion exceeds the byte bound")
    return parquet_file


def _decoded_row_group_bound(rows: int) -> int:
    if not isinstance(rows, int) or isinstance(rows, bool) or rows <= 0:
        raise DatasetIntegrityError("dataset Parquet row-group count is invalid")
    return rows * DECODED_BYTES_PER_ROW + DECODED_ROW_GROUP_OVERHEAD


def _row_group_workspace_bound(row_group: Any) -> int:
    uncompressed = sum(
        row_group.column(index).total_uncompressed_size
        for index in range(row_group.num_columns)
    )
    return max(_decoded_row_group_bound(row_group.num_rows), uncompressed)


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
    if not isinstance(value, dict) or set(value) != top or value["schema_version"] != 2:
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
    if not _fixed_schema_shape(schema):
        raise DatasetIntegrityError("dataset Arrow field schema is unsupported")
    expected_metadata = {
        b"market_squawk.build_sha256": dataset["build_spec_sha256"].encode(),
        b"market_squawk.component_layout": b"fixed-width-long-form-v2",
        b"market_squawk.dataset": dataset["dataset_id"].encode(),
        b"market_squawk.policy_sha256": dataset["policy_sha256"].encode(),
        b"market_squawk.schema": SCHEMA_NAME.encode(),
        b"market_squawk.schema_fingerprint_sha256": dataset["schema_sha256"].encode(),
        b"market_squawk.schema_version": b"2",
        b"market_squawk.timestamp_timezone": b"UTC",
        b"market_squawk.universe_sha256": dataset["universe_sha256"].encode(),
    }
    if set(schema.metadata or {}) != REQUIRED_METADATA or schema.metadata != expected_metadata:
        raise DatasetIntegrityError("dataset Arrow authority metadata mismatch")


def _fixed_schema_shape(schema: pa.Schema) -> bool:
    fields = [
        ("example_id", pa.binary(256), False),
        ("instrument_id", pa.binary(16), False),
        ("cutoff_at", pa.timestamp("ns", tz="+00:00"), False),
        ("split", pa.uint8(), False),
        ("component_kind", pa.uint8(), False),
        ("component_name", pa.binary(256), False),
        ("component_version", pa.uint32(), False),
        ("value_f64", pa.float64(), True),
        ("value_decimal_mantissa", pa.decimal128(38, 0), True),
        ("value_decimal_scale", pa.uint8(), True),
        ("unit", pa.binary(32), True),
        ("currency", pa.binary(3), True),
        ("missing_reason", pa.binary(256), True),
        ("lineage_sha256", pa.binary(32), False),
    ]
    if len(schema) != len(fields) or any(
        schema.field(index).name != name
        or schema.field(index).type != arrow_type
        or schema.field(index).nullable != nullable
        for index, (name, arrow_type, nullable) in enumerate(fields)
    ):
        return False
    return True


class _RowValidator:
    def __init__(
        self,
        components: tuple[ComponentIdentity, ...],
        policy: SplitPolicy,
        expected_counts: SplitCounts,
    ) -> None:
        self._policy = policy
        self._expected_counts = expected_counts
        self._expected_components = tuple(
            (item.kind, item.name, item.version) for item in components
        )
        self._previous: tuple[int, str, str] | None = None
        self._current: tuple[int, str, str] | None = None
        self._components: list[tuple[str, str, int]] = []
        self._counts = {"train": 0, "validation": 0, "test": 0}

    def validate_table(self, table: pa.Table) -> None:
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

    def consume(self, row: Mapping[str, Any]) -> None:
        cutoff = row["cutoff_at"].unix_nanos
        key = (cutoff, row["instrument_id"], row["example_id"])
        _canonical_uuid(row["instrument_id"])
        if not _identifier(row["example_id"]):
            raise DatasetIntegrityError("dataset example identity is invalid")
        expected_split = self._policy.split_for(cutoff)
        if expected_split is None or row["split"] != expected_split:
            raise DatasetIntegrityError("dataset row violates chronological split policy")
        component = (
            row["component_kind"],
            row["component_name"],
            row["component_version"],
        )
        if self._current is None:
            self._current = key
        elif key != self._current:
            self._close_current()
            self._current = key
        self._components.append(component)
        _validate_value(row)

    def finish(self) -> None:
        if self._current is None:
            raise DatasetIntegrityError("dataset contains no feature/label rows")
        self._close_current()
        if self._counts != {
            "train": self._expected_counts.train,
            "validation": self._expected_counts.validation,
            "test": self._expected_counts.test,
        }:
            raise DatasetIntegrityError("dataset split counts differ from Task 11 export")

    def _close_current(self) -> None:
        if self._current is None:
            raise DatasetIntegrityError("dataset example state is invalid")
        _close_example(
            self._previous,
            self._current,
            self._components,
            self._expected_components,
        )
        self._counts[row_for_split(self._policy, self._current[0])] += 1
        self._previous = self._current
        self._components = []


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
            if name in {"example_id", "component_name", "unit", "currency", "missing_reason"}:
                values[name] = _fixed_text(value)
            elif name == "instrument_id":
                values[name] = str(UUID(bytes=bytes(value)))
            elif name == "split":
                values[name] = {1: "train", 2: "validation", 3: "test"}.get(value)
            elif name == "component_kind":
                values[name] = {1: "feature", 2: "label"}.get(value)
            else:
                values[name] = bytes(value) if isinstance(value, (bytes, bytearray)) else value
    return values


def _fixed_text(value: Any) -> str:
    if not isinstance(value, (bytes, bytearray)):
        raise DatasetIntegrityError("dataset fixed-width text value is invalid")
    raw = bytes(value)
    end = raw.find(b"\0")
    end = len(raw) if end < 0 else end
    if end == 0 or any(raw[end:]):
        raise DatasetIntegrityError("dataset fixed-width text padding is invalid")
    try:
        return raw[:end].decode("utf-8")
    except UnicodeDecodeError as error:
        raise DatasetIntegrityError("dataset fixed-width text is invalid") from error


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
