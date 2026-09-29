"""Closed Task 11 descriptor, Arrow schema, row, and value validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
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
MAX_PARENTS = 256
DEFAULT_MAX_ROWS = 100_000
DEFAULT_MAX_BYTES = 256 * 1024 * 1024
MAX_PARQUET_ROW_GROUPS = 4_096
SCHEMA_NAME = "market_squawk.feature_label_components"
SCHEMA_VERSION = 3
DECODED_BYTES_PER_ROW = 1_024
MAX_INPUT_EPOCH_BYTES = 64 * 1_024
# One bounded epoch JSON parse is live at a time beside the Arrow row group.
INPUT_EPOCH_PARSE_EXPANSION = 32
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


@dataclass(frozen=True, order=True)
class TargetHorizon:
    """Elapsed nanoseconds or a source-proven count of fiscal periods."""

    kind: str
    nanos: int | None = None
    cadence: str | None = None
    periods_ahead: int | None = None

    def mapping(self) -> Mapping[str, Any]:
        if self.kind == "exact_elapsed":
            return MappingProxyType({"kind": self.kind, "nanos": self.nanos})
        return MappingProxyType({"kind": self.kind, "cadence": self.cadence,
                                 "periods_ahead": self.periods_ahead})


@dataclass(frozen=True)
class StudyPolicy:
    """Basis-qualified source snapshot retained by native dataset admission."""

    basis: str
    purpose: str
    snapshot_as_of_unix_nanos: int
    decision_lag_nanos: int | None
    target_horizon: TargetHorizon
    limitations: tuple[str, ...]
    source_snapshot_sha256: str

    @property
    def target_horizon_nanos(self) -> int | None:
        return self.target_horizon.nanos

    def mapping(self) -> Mapping[str, Any]:
        return MappingProxyType({
            "basis": self.basis,
            "purpose": self.purpose,
            "snapshot_as_of_unix_nanos": self.snapshot_as_of_unix_nanos,
            "decision_lag_nanos": self.decision_lag_nanos,
            "target_horizon": dict(self.target_horizon.mapping()),
            "limitations": list(self.limitations),
            "source_snapshot_sha256": self.source_snapshot_sha256,
        })


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
    study: StudyPolicy | None
    split_policy: SplitPolicy

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
                "study": None if self.study is None else dict(self.study.mapping()),
                "split_policy": dict(self.split_policy.mapping()),
            }
        )


@dataclass(frozen=True, order=True)
class LabelMeasurement:
    """Closed measurement transported from verified Task 11 label rows."""

    kind: str
    currency: str | None = None
    role: str | None = None
    basis: str | None = None
    share_convention: str | None = None

    def mapping(self) -> Mapping[str, Any]:
        value: dict[str, Any] = {"kind": self.kind}
        if self.kind in {"price", "financial_amount"}:
            value["currency"] = self.currency
        if self.kind == "financial_amount":
            value.update(role=self.role, basis=self.basis, share_convention=self.share_convention)
        return MappingProxyType(value)


@dataclass(frozen=True, order=True)
class LabelTarget:
    """Closed exact-time or native fiscal target derived from admitted dataset rows."""

    kind: str
    horizon_nanos: int | None = None
    origin_basis: str | None = None
    cadence: str | None = None
    periods_ahead: int | None = None
    event_json: str | None = None

    def mapping(self) -> Mapping[str, Any]:
        value: dict[str, Any] = {"kind": self.kind}
        if self.kind in {"fixed_horizon_terminal", "fixed_horizon_event"}:
            value["horizon_nanos"] = self.horizon_nanos
            value["origin_basis"] = self.origin_basis
            if self.kind == "fixed_horizon_event":
                value["event"] = json.loads(self.event_json)
        elif self.kind == "financial_period":
            value.update(cadence=self.cadence, periods_ahead=self.periods_ahead)
        return MappingProxyType(value)


@dataclass(frozen=True)
class ComponentIdentity:
    corporate_action_sensitivity: str
    kind: str
    name: str
    scope: str
    version: int
    measurement: LabelMeasurement | None = None
    target: LabelTarget = LabelTarget("not_applicable")

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
    """Chronological boundaries at exactly one native precision."""

    kind: str
    train_end: int | date
    validation_end: int | date
    test_end: int | date

    def __post_init__(self) -> None:
        ends = (self.train_end, self.validation_end, self.test_end)
        if self.kind == "exact_time":
            valid = all(type(value) is int and -(2**63) <= value < 2**63 for value in ends)
        elif self.kind == "fiscal_dates":
            valid = all(type(value) is date for value in ends)
        else:
            valid = False
        if not valid or not ends[0] < ends[1] < ends[2]:
            raise DatasetIntegrityError("split boundaries must be ordered at one native precision")

    @property
    def train_end_unix_nanos(self) -> int | None:
        return self.train_end if self.kind == "exact_time" else None

    @property
    def validation_end_unix_nanos(self) -> int | None:
        return self.validation_end if self.kind == "exact_time" else None

    @property
    def test_end_unix_nanos(self) -> int | None:
        return self.test_end if self.kind == "exact_time" else None

    @property
    def boundaries(self) -> tuple[int, int, int]:
        ends = (self.train_end, self.validation_end, self.test_end)
        if self.kind == "exact_time":
            return ends
        return tuple(_date_offset(value) for value in ends)

    def coordinate(self, value: UtcNanoseconds | date) -> int:
        if self.kind == "exact_time" and isinstance(value, UtcNanoseconds):
            return value.unix_nanos
        if self.kind == "fiscal_dates" and type(value) is date:
            return _date_offset(value)
        raise DatasetIntegrityError("split coordinate precision differs from its boundaries")

    def mapping(self) -> Mapping[str, Any]:
        names = ("train_end", "validation_end", "test_end")
        ends = (self.train_end, self.validation_end, self.test_end)
        if self.kind == "exact_time":
            return MappingProxyType({"kind": self.kind, **{
                name + "_unix_nanos": value for name, value in zip(names, ends, strict=True)
            }})
        return MappingProxyType({"kind": self.kind, **{
            name: _date_mapping(value) for name, value in zip(names, ends, strict=True)
        }})

    def split_for(self, cutoff: int) -> str | None:
        if type(cutoff) is not int:
            raise DatasetIntegrityError("split coordinate must retain its native integer value")
        for name, boundary in zip(("train", "validation", "test"), self.boundaries, strict=True):
            if cutoff <= boundary:
                return name
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
        for column_index in range(row_group.num_columns):
            column = row_group.column(column_index)
            size = column.total_uncompressed_size
            if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                raise DatasetIntegrityError("dataset Parquet column size is invalid")
            if any("DICTIONARY" in str(encoding) for encoding in column.encodings):
                raise DatasetIntegrityError("dataset Parquet dictionary encoding is unsupported")
        if _row_group_workspace_bound(row_group) > max_bytes:
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
    # The registered schema keeps the bounded epoch Binary field at ordinal 19.
    # Plain, nondictionary encoding bounds one JSON payload by this column's bytes.
    epoch_bytes = row_group.column(19).total_uncompressed_size
    parse_workspace = min(MAX_INPUT_EPOCH_BYTES, epoch_bytes) * INPUT_EPOCH_PARSE_EXPANSION
    return _decoded_row_group_bound(row_group.num_rows) + uncompressed + parse_workspace


def _export(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw, object_pairs_hook=_epoch_object,
                           parse_constant=_epoch_nonfinite, parse_float=_epoch_float)
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
        "study",
    }
    if (
        not isinstance(value, dict)
        or set(value) != top
        or value["schema_version"] != 4
    ):
        raise DatasetIntegrityError("Task 11 export version or shape is unsupported")
    dataset = value["dataset"]
    dataset_keys = {
        "build_spec_sha256",
        "dataset_id",
        "manifest_sha256",
        "manifest_version",
        "policy_sha256",
        "population_basis", "population_member_count", "population_unavailable",
        "population_partition", "population_source_use", "price_input_origin",
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
    study = _study_policy(value["study"])
    price_origin = dataset["price_input_origin"]
    if price_origin not in (None, "completed_bar_close", "named_session_close_for_nominal_daily_bar",
                            "mixed_completed_and_named_session_closes"):
        raise DatasetIntegrityError("dataset price input origin is invalid")
    if price_origin == "mixed_completed_and_named_session_closes" and not (
            dataset["population_basis"] == "current_listed_snapshot"
            and study is not None and study.basis == "historical_as_known"
            and study.purpose == "study_inputs"):
        raise DatasetIntegrityError("mixed price inputs require current label-free population evidence")
    _validate_components(value["components"], study)
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
    _population(dataset, study, counts)
    return value


@dataclass(frozen=True)
class PopulationPartition:
    """Inert exact partition metadata; the native receipt remains its authority."""

    full_population_digest: bytes
    ordinal: int
    partition_count: int
    member_ids: tuple[str, ...]
    partition_digest: bytes


@dataclass(frozen=True)
class PopulationEvidence:
    """Original population scope, missing inputs, and admitted source-use reference."""

    basis: str
    member_count: int
    unavailable: tuple[tuple[str, str], ...]
    partition: PopulationPartition | None
    source_use_json: bytes | None


def _population_digest(value: Any) -> bytes:
    if (not isinstance(value, list) or len(value) != 32
            or any(type(item) is not int or not 0 <= item <= 255 for item in value)
            or not any(value)):
        raise DatasetIntegrityError("population evidence digest is invalid")
    return bytes(value)


def _population_evidence_digest(value: Any) -> None:
    if (not isinstance(value, dict) or set(value) != {"algorithm", "bytes"}
            or not isinstance(value["algorithm"], str)
            or value["algorithm"] not in {"sha256", "blake3"}):
        raise DatasetIntegrityError("population evidence algorithm is invalid")
    _population_digest(value["bytes"])


def _population(dataset: Mapping[str, Any], study: StudyPolicy | None,
                counts: SplitCounts) -> PopulationEvidence:
    basis = dataset["population_basis"]
    count = dataset["population_member_count"]
    missing = dataset["population_unavailable"]
    partition = dataset["population_partition"]
    source_use = dataset["population_source_use"]
    if (not isinstance(basis, str) or basis not in {
            "published_historical_membership", "current_listed_snapshot", "present_day_fixed_cohort"}
            or type(count) is not int or not 0 < count < 2**64 or not isinstance(missing, list)):
        raise DatasetIntegrityError("population metadata is invalid")
    if basis == "published_historical_membership":
        if missing or partition is not None or source_use is not None:
            raise DatasetIntegrityError("historical membership has a current population claim")
        return PopulationEvidence(basis, count, (), None, None)
    if (count > 65_536 or study is None or not isinstance(partition, dict)
            or set(partition) != {"full_population_digest", "ordinal", "partition_count", "member_ids", "partition_digest"}):
        raise DatasetIntegrityError("current population partition is absent or invalid")
    if basis == "current_listed_snapshot":
        if (study.basis != "historical_as_known" or study.purpose != "study_inputs"
                or study.target_horizon.kind != "exact_elapsed"
                or counts.train != 0 or counts.validation != 0):
            raise DatasetIntegrityError("current population cannot grant historical study authority")
    elif (study.basis != "retrospective_frozen_snapshot"
            or study.target_horizon.kind not in {"fiscal_periods", "exact_elapsed"}):
        raise DatasetIntegrityError("fixed current cohort requires qualified source research")
    ordinal, total = partition["ordinal"], partition["partition_count"]
    members = partition["member_ids"]
    if (type(ordinal) is not int or type(total) is not int
            or not (count + 127) // 128 <= total <= count or not 0 <= ordinal < total
            or not isinstance(members, list) or not 1 <= len(members) <= min(128, count)):
        raise DatasetIntegrityError("population partition range is invalid")
    for member in members:
        _canonical_uuid(member)
        if UUID(member).int == 0:
            raise DatasetIntegrityError("population member is reserved")
    if members != sorted(set(members)):
        raise DatasetIntegrityError("population partition members are not sorted and unique")
    full_digest = _population_digest(partition["full_population_digest"])
    partition_digest = _population_digest(partition["partition_digest"])
    digest = hashlib.sha256(b"market-squawk/current-population-partition/v1\0")
    digest.update(full_digest)
    for number in (ordinal, total, len(members)):
        digest.update(number.to_bytes(8, "big"))
    for member in members:
        digest.update(UUID(member).bytes)
    if digest.digest() != partition_digest:
        raise DatasetIntegrityError("population partition digest differs from its members")
    reasons = {"source_history_unavailable", "required_feature_unavailable", "source_rights_unavailable",
               "freshness_unavailable", "calendar_unavailable", "execution_definition_unavailable"}
    absent: list[tuple[str, str]] = []
    if len(missing) >= len(members):
        raise DatasetIntegrityError("empty population partitions cannot publish a dataset")
    for item in missing:
        if (not isinstance(item, dict) or set(item) != {"instrument_id", "reason"}
                or not isinstance(item["reason"], str) or item["reason"] not in reasons
                or item["instrument_id"] not in members):
            raise DatasetIntegrityError("population missing-input disposition is invalid")
        absent.append((item["instrument_id"], item["reason"]))
    if absent != sorted(absent) or len({item[0] for item in absent}) != len(absent):
        raise DatasetIntegrityError("population missing-input dispositions repeat a member")
    examples = counts.train + counts.validation + counts.test
    if ((basis == "current_listed_snapshot" and examples + len(absent) != len(members))
            or (basis == "present_day_fixed_cohort" and (absent or examples < len(members)))):
        raise DatasetIntegrityError("population coverage differs from published examples")
    if (not isinstance(source_use, dict)
            or set(source_use) != {"source_id", "directory_generation", "rights_id", "requested_use", "grant"}
            or not _identifier(source_use["source_id"])
            or source_use["requested_use"] != ("train" if study.purpose == "training" else "local_analysis")):
        raise DatasetIntegrityError("population source use differs from the admitted purpose")
    for name in ("directory_generation", "rights_id"):
        _population_digest(source_use[name])
    grant = source_use["grant"]
    if (not isinstance(grant, dict) or set(grant) != {
            "rights_basis_digest", "authorization_evidence", "rights_expires_at",
            "research_grant_id", "grant_evidence", "grant_expires_at"}):
        raise DatasetIntegrityError("population source grant is incomplete")
    for name in ("rights_basis_digest", "research_grant_id"):
        _population_digest(grant[name])
    for name in ("authorization_evidence", "grant_evidence"):
        _population_evidence_digest(grant[name])
    for name in ("rights_expires_at", "grant_expires_at"):
        expiry = grant[name]
        if expiry is not None and (type(expiry) is not int or not -(2**63) <= expiry < 2**63):
            raise DatasetIntegrityError("population source grant expiry is invalid")
    # Native verification checks the original grant against current revocations. These bytes
    # retain only its immutable reference; a parsed enum cannot manufacture a use permit.
    retained_use = json.dumps(source_use, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return PopulationEvidence(basis, count, tuple(absent),
                              PopulationPartition(full_digest, ordinal, total, tuple(members), partition_digest),
                              retained_use)


def _validate_components(values: Any, study: StudyPolicy | None) -> None:
    keys = {
        "corporate_action_sensitivity",
        "kind",
        "measurement",
        "name",
        "scope",
        "target",
        "version",
    }
    if not isinstance(values, list) or not 1 <= len(values) <= MAX_COMPONENTS:
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
    expected_kinds = {"feature"} if study is not None and study.purpose == "study_inputs" else {"feature", "label"}
    if kinds != expected_kinds:
        raise DatasetIntegrityError("Task 11 components contradict the declared build purpose")


def _target_horizon(value: Any) -> TargetHorizon:
    if not isinstance(value, dict):
        raise DatasetIntegrityError("study target horizon is invalid")
    if value.get("kind") == "exact_elapsed" and set(value) == {"kind", "nanos"}:
        if type(value["nanos"]) is int and 0 < value["nanos"] < 2**63:
            return TargetHorizon("exact_elapsed", nanos=value["nanos"])
    if value.get("kind") == "fiscal_periods" and set(value) == {"kind", "cadence", "periods_ahead"}:
        if _valid_fiscal_horizon(value["cadence"], value["periods_ahead"]):
            return TargetHorizon("fiscal_periods", cadence=value["cadence"],
                                 periods_ahead=value["periods_ahead"])
    raise DatasetIntegrityError("study target horizon is invalid")


def _valid_fiscal_horizon(cadence: Any, periods: Any) -> bool:
    return (isinstance(cadence, str) and cadence in {"annual", "quarterly"}
            and type(periods) is int and 0 < periods < 2**16)


def _study_policy(value: Any) -> StudyPolicy | None:
    if value is None:
        return None
    keys = {"basis", "purpose", "snapshot_as_of_unix_nanos", "decision_lag_nanos",
            "target_horizon", "limitations", "source_snapshot_sha256"}
    if not isinstance(value, dict) or set(value) != keys:
        raise DatasetIntegrityError("Task 11 study policy is invalid")
    basis, purpose = value["basis"], value["purpose"]
    snapshot, lag = value["snapshot_as_of_unix_nanos"], value["decision_lag_nanos"]
    horizon = _target_horizon(value["target_horizon"])
    if (not isinstance(basis, str) or basis not in {"historical_as_known", "retrospective_frozen_snapshot"}
            or not isinstance(purpose, str) or purpose not in {"training", "study_inputs"}
            or type(snapshot) is not int or not -(2**63) <= snapshot < 2**63
            or (basis == "historical_as_known" and lag is not None)
            or (basis == "retrospective_frozen_snapshot" and
                (type(lag) is not int or (lag != 0 if horizon.kind == "fiscal_periods"
                                          else not 0 <= lag < horizon.nanos)))):
        raise DatasetIntegrityError("Task 11 study clocks are invalid")
    limitations = value["limitations"]
    expected = (["present_day_fixed_cohort"] if basis == "historical_as_known" else [
        "historical_revision_coverage_unproven", "later_vintage_inputs",
        "present_day_fixed_cohort", "simulated_availability"])
    if limitations != expected or _digest(value["source_snapshot_sha256"]) == bytes(32):
        raise DatasetIntegrityError("Task 11 study qualification is invalid")
    return StudyPolicy(basis, purpose, snapshot, lag, horizon, tuple(limitations), value["source_snapshot_sha256"])


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
        b"market_squawk.component_layout": b"long-form-native-financial-study-input-epoch-v3",
        b"market_squawk.dataset": dataset["dataset_id"].encode(),
        b"market_squawk.policy_sha256": dataset["policy_sha256"].encode(),
        b"market_squawk.schema": SCHEMA_NAME.encode(),
        b"market_squawk.schema_fingerprint_sha256": dataset["schema_sha256"].encode(),
        b"market_squawk.schema_version": b"3",
        b"market_squawk.timestamp_timezone": b"UTC",
        b"market_squawk.universe_sha256": dataset["universe_sha256"].encode(),
    }
    if set(schema.metadata or {}) != REQUIRED_METADATA or schema.metadata != expected_metadata:
        raise DatasetIntegrityError("dataset Arrow authority metadata mismatch")


def _fixed_schema_shape(schema: pa.Schema) -> bool:
    fields = [
        ("example_id", pa.binary(256), False),
        ("instrument_id", pa.binary(16), False),
        ("source_selection_as_of", pa.timestamp("ns", tz="+00:00"), False),
        ("observed_effective_at", pa.timestamp("ns", tz="+00:00"), True),
        ("label_effective_at", pa.timestamp("ns", tz="+00:00"), True),
        ("target_coordinate_kind", pa.uint8(), False),
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
        ("decision_at", pa.timestamp("ns", tz="+00:00"), True),
        ("label_selection_as_of", pa.timestamp("ns", tz="+00:00"), True),
        ("lineage_sha256", pa.binary(32), False),
        ("input_epoch_json", pa.binary(), True),
        ("decision_on", pa.date32(), True),
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
        study: StudyPolicy | None,
        population: PopulationEvidence,
        price_input_origin: str | None,
    ) -> None:
        self._expected_price_origin_mask = {None: 0, "completed_bar_close": 1,
            "named_session_close_for_nominal_daily_bar": 2,
            "mixed_completed_and_named_session_closes": 3}[price_input_origin]
        self._observed_price_origin_mask = 0
        self._population = population
        self._population_members: set[str] = set()
        self._policy = policy
        self._study = study
        self._expected_counts = expected_counts
        self._expected_components = tuple(
            (item.kind, item.name, item.version) for item in components
        )
        self._previous: tuple[tuple[int, int], str, str] | None = None
        self._current: tuple[Any, ...] | None = None
        self._components: list[tuple[str, str, int]] = []
        self._counts = {"train": 0, "validation": 0, "test": 0}
        self._expected_measurements = {
            (item.kind, item.name, item.version): item.measurement
            for item in components
            if item.kind == "label"
        }
        self._observed_measurements: dict[
            tuple[str, str, int], LabelMeasurement
        ] = {}
        self._expected_targets = {
            (item.kind, item.name, item.version): item.target
            for item in components
            if item.kind == "label"
        }
        self._observed_horizons: dict[tuple[str, str, int], LabelTarget] = {}

    def validate_table(self, table: pa.Table) -> None:
        required = (
            "example_id",
            "instrument_id",
            "source_selection_as_of",
            "target_coordinate_kind",
            "split",
            "component_kind",
            "component_name",
            "component_version",
            "lineage_sha256",
        )
        if any(table[name].null_count for name in required):
            raise DatasetIntegrityError("dataset required row identity is null")

    def consume(self, row: Mapping[str, Any]) -> None:
        cutoff = row["source_selection_as_of"].unix_nanos
        decision = _decision_coordinate(row)
        label_selection = row["label_selection_as_of"]
        split_coordinate = self._policy.coordinate(
            decision if self._study is not None and self._study.basis == "retrospective_frozen_snapshot"
            else row["source_selection_as_of"]
        )
        observed_effective = row["observed_effective_at"]
        label_effective = row["label_effective_at"]
        target_kind = row["target_coordinate_kind"]
        if target_kind in {1, 3, 5}:
            if (
                not isinstance(observed_effective, UtcNanoseconds)
                or not isinstance(label_effective, UtcNanoseconds)
                or label_effective.unix_nanos <= observed_effective.unix_nanos
            ):
                raise DatasetIntegrityError("dataset exact terminal coordinates are invalid")
            candidate_horizon = LabelTarget(
                "fixed_horizon_terminal",
                label_effective.unix_nanos - observed_effective.unix_nanos,
                {1: "exact_effective_timestamp", 3: "completed_bar_close",
                 5: "named_session_close_for_nominal_daily_bar"}[target_kind],
            )
        elif target_kind in {2, 4} and observed_effective is None and label_effective is None:
            candidate_horizon = LabelTarget("unsupported")
            if target_kind == 4:
                if self._study is None or self._study.target_horizon.kind != "fiscal_periods":
                    raise DatasetIntegrityError("fiscal row requires a native fiscal study horizon")
                candidate_horizon = LabelTarget(
                    "financial_period", cadence=self._study.target_horizon.cadence,
                    periods_ahead=self._study.target_horizon.periods_ahead,
                )
        else:
            raise DatasetIntegrityError("dataset target coordinate tag is invalid")
        epoch = _validate_input_epoch(row, self._study)
        self._observed_price_origin_mask |= {3: 1, 5: 2}.get(target_kind, 0)
        if epoch is not None and epoch["population_basis"] != self._population.basis:
            raise DatasetIntegrityError("input epoch population differs from its dataset")
        if self._population.basis != "published_historical_membership":
            if epoch is None:
                raise DatasetIntegrityError("current population requires its sealed source epoch")
            if self._population.basis == "present_day_fixed_cohort" and target_kind not in {4, 5}:
                raise DatasetIntegrityError("fixed cohort requires its native fiscal or named-session source")
            if self._population.basis == "current_listed_snapshot" and (
                    decision != row["source_selection_as_of"] or row["split"] != "test"):
                raise DatasetIntegrityError("current population row claims a historical decision")
        _validate_study_row(row, self._study, self._policy, split_coordinate, epoch,
            self._expected_targets.get((row["component_kind"], row["component_name"], row["component_version"])))
        key = (
            (1, decision.unix_nanos) if isinstance(decision, UtcNanoseconds)
            else (2, _date_offset(decision)),
            row["instrument_id"],
            row["example_id"],
            target_kind,
            None if observed_effective is None else observed_effective.unix_nanos,
            None if label_effective is None else label_effective.unix_nanos,
            row["input_epoch_json"],
            cutoff,
            None if label_selection is None else label_selection.unix_nanos,
            split_coordinate,
        )
        _canonical_uuid(row["instrument_id"])
        if not _identifier(row["example_id"]):
            raise DatasetIntegrityError("dataset example identity is invalid")
        expected_split = self._policy.split_for(split_coordinate)
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
        if row["component_kind"] == "label" and row["missing_reason"] is None:
            measurement = _measurement_from_row(row)
            if measurement.kind == "price" and not (
                (row["value_f64"] is not None and row["value_f64"] > 0.0)
                or (
                    row["value_decimal_mantissa"] is not None
                    and row["value_decimal_mantissa"] > 0
                )
            ):
                raise DatasetIntegrityError("dataset price label must be positive")
            retained = self._observed_measurements.get(component)
            if retained is not None and retained != measurement:
                raise DatasetIntegrityError(
                    "dataset label rows carry conflicting measurements"
                )
            self._observed_measurements[component] = measurement
            expected_target = self._expected_targets.get(component)
            if expected_target is not None and expected_target.kind == "fixed_horizon_event":
                if (candidate_horizon.kind != "fixed_horizon_terminal"
                        or measurement.kind != "probability"
                        or row["value_f64"] not in (None, 0.0, 1.0)
                        or (row["value_decimal_mantissa"] is not None
                            and row["value_decimal_mantissa"] not in (0, 10 ** row["value_decimal_scale"]))):
                    raise DatasetIntegrityError("event label is not a binary original terminal outcome")
                candidate_horizon = LabelTarget("fixed_horizon_event", candidate_horizon.horizon_nanos,
                                                candidate_horizon.origin_basis, event_json=expected_target.event_json)
            retained_horizon = self._observed_horizons.get(component)
            self._observed_horizons[component] = (
                candidate_horizon
                if retained_horizon is None or retained_horizon == candidate_horizon
                else LabelTarget("unsupported")
            )

    def finish(self) -> None:
        if self._observed_price_origin_mask != self._expected_price_origin_mask:
            raise DatasetIntegrityError("dataset price origins differ from its complete row evidence")
        if self._current is None:
            raise DatasetIntegrityError("dataset contains no feature/label rows")
        self._close_current()
        if self._population.partition is not None:
            absent = {item[0] for item in self._population.unavailable}
            if (self._population_members & absent
                    or self._population_members | absent != set(self._population.partition.member_ids)):
                raise DatasetIntegrityError("population inputs and missing dispositions do not cover the partition")
        if self._counts != {
            "train": self._expected_counts.train,
            "validation": self._expected_counts.validation,
            "test": self._expected_counts.test,
        }:
            raise DatasetIntegrityError("dataset split counts differ from Task 11 export")
        if any(
            self._observed_measurements.get(identity) != measurement
            for identity, measurement in self._expected_measurements.items()
        ):
            raise DatasetIntegrityError(
                "dataset label measurement differs from Task 11 export"
            )
        if any(
            self._observed_horizons.get(identity)
            != target
            for identity, target in self._expected_targets.items()
            if self._expected_measurements.get(identity) is not None
        ):
            raise DatasetIntegrityError(
                "dataset terminal horizon differs from Task 11 export"
            )

    def _close_current(self) -> None:
        if self._current is None:
            raise DatasetIntegrityError("dataset example state is invalid")
        _close_example(
            self._previous,
            self._current[:3],
            self._components,
            self._expected_components,
        )
        if self._population.partition is not None:
            member = self._current[1]
            if member not in self._population.partition.member_ids:
                raise DatasetIntegrityError("dataset example is outside its population partition")
            if self._population.basis == "current_listed_snapshot" and member in self._population_members:
                raise DatasetIntegrityError("current population repeats a member epoch")
            self._population_members.add(member)
        self._counts[row_for_split(self._policy, self._current[-1])] += 1
        self._previous = self._current[:3]
        self._components = []


def _epoch_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise DatasetIntegrityError("dataset input epoch has duplicate JSON keys")
        value[key] = item
    return value


def _epoch_nonfinite(value: str) -> None:
    raise DatasetIntegrityError("dataset input epoch has a nonfinite JSON value")


def _epoch_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise DatasetIntegrityError("dataset input epoch has a nonfinite JSON number")
    return parsed


def _validate_study_row(row: Mapping[str, Any], study: StudyPolicy | None,
                        policy: SplitPolicy, split_coordinate: int,
                        epoch: Mapping[str, Any] | None, event_target: LabelTarget | None = None) -> None:
    source = row["source_selection_as_of"].unix_nanos
    decision = _decision_coordinate(row)
    label_selection = row["label_selection_as_of"]
    maturity: UtcNanoseconds | date | None = label_selection
    if study is None:
        if decision != row["source_selection_as_of"] or label_selection is None or label_selection.unix_nanos <= source:
            raise DatasetIntegrityError("generic dataset knowledge chronology is invalid")
    else:
        if source > study.snapshot_as_of_unix_nanos:
            raise DatasetIntegrityError("study source exceeds its retained snapshot")
        retrospective = study.basis == "retrospective_frozen_snapshot"
        if study.target_horizon.kind == "fiscal_periods":
            if row["target_coordinate_kind"] != 4 or epoch is None:
                raise DatasetIntegrityError("fiscal study requires its native input epoch")
            observed = _period_end(epoch["period"]["observed_period"])
            terminal_raw = epoch["period"]["target_period"]
            terminal = None if terminal_raw is None else _period_end(terminal_raw)
            if retrospective:
                if decision != observed:
                    raise DatasetIntegrityError("fiscal retrospective decision differs from the native period end")
            elif (not isinstance(decision, UtcNanoseconds)
                  or decision.to_datetime().date() < observed
                  or (terminal is not None and decision.to_datetime().date() >= terminal)):
                raise DatasetIntegrityError("fiscal historical decision is outside its native periods")
            if label_selection is not None and (terminal is None or terminal > label_selection.to_datetime().date()):
                raise DatasetIntegrityError("fiscal training label is not mature")
        else:
            observed, terminal = row["observed_effective_at"], row["label_effective_at"]
            if (row["target_coordinate_kind"] not in {3, 5} or observed is None or terminal is None
                    or not isinstance(decision, UtcNanoseconds)
                    or terminal.unix_nanos - observed.unix_nanos != study.target_horizon_nanos
                    or not observed.unix_nanos <= decision.unix_nanos < terminal.unix_nanos):
                raise DatasetIntegrityError("study origin and target clocks are invalid")
            if retrospective and decision.unix_nanos != observed.unix_nanos + study.decision_lag_nanos:
                raise DatasetIntegrityError("retrospective decision lag differs")
            if label_selection is not None and terminal > label_selection:
                raise DatasetIntegrityError("training label is not mature")
        if study.basis == "historical_as_known":
            if (not isinstance(decision, UtcNanoseconds) or source > decision.unix_nanos
                    or (label_selection is not None and label_selection.unix_nanos <= source)):
                raise DatasetIntegrityError("historical source knowledge is invalid")
        elif (source != study.snapshot_as_of_unix_nanos
                or (label_selection is not None and label_selection.unix_nanos != source)):
            raise DatasetIntegrityError("retrospective source snapshot differs")
        if (study.purpose == "study_inputs") != (label_selection is None):
            raise DatasetIntegrityError("dataset label selection contradicts its purpose")
        if label_selection is not None and label_selection.unix_nanos > study.snapshot_as_of_unix_nanos:
            raise DatasetIntegrityError("training label exceeds its source snapshot")
        if retrospective:
            maturity = terminal
    if (label_selection is not None and event_target is not None
            and event_target.kind == "fixed_horizon_event"):
        event = json.loads(event_target.event_json)
        if event["kind"] == "profit_after_costs":
            endpoint = row["label_effective_at"].unix_nanos + event["policy"]["maximum_exit_lag_nanos"]
            if not -(2**63) <= endpoint < 2**63 or not isinstance(maturity, UtcNanoseconds):
                raise DatasetIntegrityError("event exit window is outside the source clock domain")
            maturity = UtcNanoseconds(max(maturity.unix_nanos, endpoint))
    split = policy.split_for(split_coordinate)
    if split is None:
        raise DatasetIntegrityError("dataset split coordinate exceeds the declared partitions")
    if label_selection is not None:
        partition_end = dict(zip(("train", "validation", "test"), policy.boundaries, strict=True))[split]
        if maturity is None or policy.coordinate(maturity) > partition_end:
            raise DatasetIntegrityError("dataset label crosses a purged partition boundary")


def _validate_input_epoch(row: Mapping[str, Any], study: StudyPolicy | None) -> Mapping[str, Any] | None:
    raw = row["input_epoch_json"]
    if row["target_coordinate_kind"] not in {3, 4, 5}:
        if raw is not None:
            raise DatasetIntegrityError("generic target cannot declare a source input epoch")
        return
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_INPUT_EPOCH_BYTES:
        raise DatasetIntegrityError("dataset input epoch exceeds its byte bound")
    try:
        epoch = json.loads(
            raw, object_pairs_hook=_epoch_object,
            parse_constant=_epoch_nonfinite, parse_float=_epoch_float,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as error:
        raise DatasetIntegrityError("dataset input epoch JSON is invalid") from error
    expected_kind = {3: "completed_bar_close", 4: "financial_period",
                     5: "named_session_close_for_nominal_daily_bar"}[row["target_coordinate_kind"]]
    if (not isinstance(epoch, dict) or set(epoch) != {"kind", "input"}
            or epoch["kind"] != expected_kind or not isinstance(epoch["input"], dict)):
        raise DatasetIntegrityError("dataset input epoch source tag is invalid")
    epoch = epoch["input"]
    if expected_kind == "financial_period":
        _validate_financial_epoch(row, study, epoch)
        return epoch
    _validate_completed_epoch(row, study, epoch)
    return epoch


def _validate_completed_epoch(row: Mapping[str, Any], study: StudyPolicy | None,
                              epoch: Mapping[str, Any]) -> None:
    keys = {
        "example_id", "instrument_id", "source_selection_as_of", "decision_at", "target_origin", "target_at",
        "calculated_at", "market_bar", "source_manifest", "source_evidence",
        "point_in_time_content", "point_in_time_audit", "universe_content", "universe_audit",
        "adjustment_plan_content", "adjustment_plan_audit", "adjustment_implementation",
        "origin_price_factors", "named_session_origin",
        "basis", "purpose", "snapshot_as_of", "source_snapshot_digest", "limitations", "population_basis",
        "decision_lag_nanos", "target_horizon_nanos",
    }
    if not isinstance(epoch, dict) or set(epoch) != keys:
        raise DatasetIntegrityError("dataset input epoch shape is invalid")
    clocks = ("source_selection_as_of", "decision_at", "snapshot_as_of", "target_origin", "target_at", "calculated_at")
    if any(type(epoch[name]) is not int or not -(2**63) <= epoch[name] < 2**63 for name in clocks):
        raise DatasetIntegrityError("dataset input epoch clock is invalid")
    if (type(epoch["target_horizon_nanos"]) is not int
            or not 0 < epoch["target_horizon_nanos"] < 2**64
            or (epoch["decision_lag_nanos"] is not None
                and (type(epoch["decision_lag_nanos"]) is not int
                     or not 0 <= epoch["decision_lag_nanos"] < 2**64))):
        raise DatasetIntegrityError("dataset input epoch horizon or lag is invalid")
    if (
        study is None
        or study.target_horizon.kind != "exact_elapsed"
        or not isinstance(row["decision_at"], UtcNanoseconds)
        or row["decision_on"] is not None
        or epoch["example_id"] != row["example_id"]
        or epoch["instrument_id"] != row["instrument_id"]
        or epoch["source_selection_as_of"] != row["source_selection_as_of"].unix_nanos
        or epoch["decision_at"] != row["decision_at"].unix_nanos
        or epoch["basis"] != study.basis
        or epoch["purpose"] != study.purpose
        or epoch["snapshot_as_of"] != study.snapshot_as_of_unix_nanos
        or epoch["limitations"] != list(study.limitations)
        or epoch["decision_lag_nanos"] != study.decision_lag_nanos
        or epoch["target_horizon_nanos"] != study.target_horizon_nanos
        or epoch["source_snapshot_digest"] != list(_digest(study.source_snapshot_sha256))
        or epoch["target_origin"] != row["observed_effective_at"].unix_nanos
        or epoch["target_at"] != row["label_effective_at"].unix_nanos
        or not epoch["target_origin"] <= epoch["decision_at"] < epoch["target_at"]
        or epoch["calculated_at"] < max(epoch["source_selection_as_of"], epoch["snapshot_as_of"])
        or not isinstance(epoch["market_bar"], dict)
        or not isinstance(epoch["source_manifest"], dict)
        or not isinstance(epoch["adjustment_implementation"], dict)
    ):
        raise DatasetIntegrityError("dataset input epoch differs from its original row")
    if row["target_coordinate_kind"] == 5:
        _validate_named_session_origin(epoch, study)
    elif epoch["named_session_origin"] is not None:
        raise DatasetIntegrityError("exact provider completion cannot carry a nominal date origin")
    factors = epoch["origin_price_factors"]
    if (not isinstance(factors, list) or len(factors) > 1_024
            or any(not isinstance(pair, list) or len(pair) != 2
                   or any(type(item) is not int or not 0 < item < 2**32 for item in pair)
                   for pair in factors)):
        raise DatasetIntegrityError("dataset input epoch price factors are invalid")
    for name in ("source_evidence", "point_in_time_content", "point_in_time_audit",
                 "universe_content", "universe_audit", "adjustment_plan_content",
                 "adjustment_plan_audit"):
        digest = epoch[name]
        if (not isinstance(digest, list) or len(digest) != 32
                or any(type(item) is not int or not 0 <= item <= 255 for item in digest)
                or not any(digest)):
            raise DatasetIntegrityError("dataset input epoch evidence digest is invalid")


def _validate_named_session_origin(epoch: Mapping[str, Any], study: StudyPolicy) -> None:
    origin = epoch["named_session_origin"]
    keys = {"identity_qualification", "current", "prior", "terminal", "history_parent_sha256",
            "history_origin_sha256", "history_publication_receipt_sha256", "history_content_sha256",
            "history_read_sha256", "mapping_digest", "source_replay_digest", "capture_receipt_digest",
            "calendar_origin_content_digest", "calendar_capture_binding_digest", "calendar_component_digest",
            "calendar_received_at", "calendar_published_at", "history_published_at", "history_knowledge_cutoff"}
    if (not isinstance(origin, dict) or set(origin) != keys
            or not (study.basis == "retrospective_frozen_snapshot"
                    or (study.basis == "historical_as_known" and study.purpose == "study_inputs"))
            or origin["identity_qualification"] != "provider_assigned_history_at_observed_revision"):
        raise DatasetIntegrityError("nominal daily origin lacks qualified source identity")
    for name in ("history_parent_sha256", "history_origin_sha256", "history_publication_receipt_sha256",
                 "history_content_sha256", "history_read_sha256"):
        _population_digest(origin[name])
    for name in ("mapping_digest", "source_replay_digest", "capture_receipt_digest",
                 "calendar_origin_content_digest", "calendar_capture_binding_digest"):
        _population_evidence_digest(origin[name])
    if origin["calendar_component_digest"] is not None:
        _population_evidence_digest(origin["calendar_component_digest"])
    for name in ("calendar_received_at", "calendar_published_at", "history_published_at", "history_knowledge_cutoff"):
        if type(origin[name]) is not int or not -(2**63) <= origin[name] < 2**63:
            raise DatasetIntegrityError("nominal daily source clock is invalid")
    if (origin["history_knowledge_cutoff"] != study.snapshot_as_of_unix_nanos
            or origin["calendar_received_at"] > origin["calendar_published_at"]
            or origin["calendar_published_at"] > study.snapshot_as_of_unix_nanos
            or origin["history_published_at"] > study.snapshot_as_of_unix_nanos
            or origin["history_parent_sha256"] != epoch["source_manifest"].get("manifest_sha256")):
        raise DatasetIntegrityError("nominal daily origin differs from its retained source snapshot")
    if study.basis == "historical_as_known" and (
            epoch["source_selection_as_of"] != study.snapshot_as_of_unix_nanos
            or epoch["decision_at"] != study.snapshot_as_of_unix_nanos):
        raise DatasetIntegrityError("current nominal source differs from its actual saved cutoff")
    if study.purpose == "study_inputs" and origin["terminal"] is not None:
        raise DatasetIntegrityError("feature-only nominal daily origin cannot retain a label target")
    session_rows: list[tuple[date, int]] = []
    for name in ("prior", "current", "terminal"):
        session = origin[name]
        if name == "terminal" and session is None:
            if study.purpose == "training":
                raise DatasetIntegrityError("nominal daily training target is unobserved")
            continue
        if (not isinstance(session, dict) or set(session) != {
                "native_date", "opens_at", "closes_at_exclusive", "original_bar_sha256"}
                or any(type(session[clock]) is not int or not -(2**63) <= session[clock] < 2**63
                       for clock in ("opens_at", "closes_at_exclusive"))
                or session["opens_at"] >= session["closes_at_exclusive"]):
            raise DatasetIntegrityError("nominal daily named session is invalid")
        native_date = _calendar_date(session["native_date"])
        _population_digest(session["original_bar_sha256"])
        session_rows.append((native_date, session["closes_at_exclusive"]))
    if any(left[0] >= right[0] or left[1] >= right[1]
           for left, right in zip(session_rows, session_rows[1:])):
        raise DatasetIntegrityError("nominal daily sessions are not chronologically ordered")
    semantics = epoch["market_bar"].get("time_semantics")
    if (not isinstance(semantics, dict) or set(semantics) != {"precision", "semantics"}
            or semantics["precision"] != "nominal_daily_date"
            or not isinstance(semantics["semantics"], dict)
            or _calendar_date(semantics["semantics"].get("date")) != session_rows[1][0]
            or origin["current"]["closes_at_exclusive"] != epoch["target_origin"]
            or origin["current"]["closes_at_exclusive"] > study.snapshot_as_of_unix_nanos
            or (origin["terminal"] is not None
                and origin["terminal"]["closes_at_exclusive"] != epoch["target_at"])):
        raise DatasetIntegrityError("nominal daily source date does not bind its financial origin")
    # Original bar hashes and physical capture/calendar authority are independently checked
    # by native row admission. This parser preserves their exact values and cross-row clocks.


def _period_end(value: Any) -> date:
    if not isinstance(value, dict):
        raise DatasetIntegrityError("native fiscal period is invalid")
    if value.get("kind") == "instant" and set(value) == {"kind", "instant"}:
        return _calendar_date(value["instant"])
    if value.get("kind") == "duration" and set(value) == {"kind", "start", "end"}:
        start, end = _calendar_date(value["start"]), _calendar_date(value["end"])
        if start <= end:
            return end
    raise DatasetIntegrityError("native fiscal period is invalid")


def _financial_source_inputs(value: Any, kind: str) -> tuple[Mapping[str, Any], ...]:
    # Match the native closed recipe without replacing original facts with a derived fact.
    names = ("parent_equity", "preferred_equity") if kind == "common_book_equity" else ("amount",)
    if (not isinstance(value, dict) or set(value) != {"kind", *names}
            or value["kind"] != kind or any(not isinstance(value[name], dict) for name in names)):
        raise DatasetIntegrityError("financial source inputs differ from their amount recipe")
    return tuple(value[name] for name in names)


def _financial_row_context(reference: Any) -> Mapping[str, Any]:
    keys = {"row_ordinal", "canonical_row_digest", "observation_digest",
            "point_in_time_evidence", "fact_context"}
    if (not isinstance(reference, dict) or set(reference) != keys
            or type(reference["row_ordinal"]) is not int
            or not 0 <= reference["row_ordinal"] < 2**32
            or not isinstance(reference["fact_context"], dict)):
        raise DatasetIntegrityError("financial native row reference is invalid")
    for name in ("canonical_row_digest", "observation_digest", "point_in_time_evidence"):
        digest = reference[name]
        if name != "point_in_time_evidence":
            if (not isinstance(digest, dict) or set(digest) != {"algorithm", "bytes"}
                    or not isinstance(digest["algorithm"], str)
                    or digest["algorithm"] not in {"sha256", "blake3"}):
                raise DatasetIntegrityError("financial native row digest is invalid")
            digest = digest["bytes"]
        if (not isinstance(digest, list) or len(digest) != 32
                or any(type(item) is not int or not 0 <= item <= 255 for item in digest)
                or not any(digest)):
            raise DatasetIntegrityError("financial native row digest is invalid")
    return reference["fact_context"]


def _validate_financial_epoch(row: Mapping[str, Any], study: StudyPolicy | None,
                              epoch: Mapping[str, Any]) -> None:
    keys = {"example_id", "instrument_id", "source_selection_as_of", "decision_coordinate",
            "basis", "purpose", "snapshot_as_of", "source_snapshot_digest", "calculated_at", "population_basis",
            "current_inputs", "current_anchor", "selection", "source_manifest", "source_evidence",
            "point_in_time_content", "point_in_time_audit", "universe_content", "universe_audit", "period"}
    if set(epoch) != keys or study is None or study.target_horizon.kind != "fiscal_periods":
        raise DatasetIntegrityError("financial input epoch shape or study is invalid")
    clocks = ("source_selection_as_of", "snapshot_as_of", "calculated_at")
    if any(type(epoch[name]) is not int or not -(2**63) <= epoch[name] < 2**63 for name in clocks):
        raise DatasetIntegrityError("financial input epoch clock is invalid")
    decision = _decision_coordinate(row)
    coordinate = {"schema_version": 2, "coordinate": {
        "precision": "exact_timestamp" if isinstance(decision, UtcNanoseconds) else "calendar_date",
        "value": decision.unix_nanos if isinstance(decision, UtcNanoseconds) else _date_mapping(decision),
    }}
    if (epoch["example_id"] != row["example_id"]
            or epoch["instrument_id"] != row["instrument_id"]
            or epoch["source_selection_as_of"] != row["source_selection_as_of"].unix_nanos
            or epoch["decision_coordinate"] != coordinate
            or epoch["basis"] != study.basis or epoch["purpose"] != study.purpose
            or epoch["snapshot_as_of"] != study.snapshot_as_of_unix_nanos
            or epoch["source_snapshot_digest"] != list(_digest(study.source_snapshot_sha256))
            or epoch["calculated_at"] < epoch["snapshot_as_of"]
            or any(not isinstance(epoch[name], dict) for name in ("current_inputs", "current_anchor", "source_manifest"))):
        raise DatasetIntegrityError("financial input epoch differs from its original row")
    for name in ("source_evidence", "point_in_time_content", "point_in_time_audit", "universe_content", "universe_audit"):
        digest = epoch[name]
        if (not isinstance(digest, list) or len(digest) != 32
                or any(type(item) is not int or not 0 <= item <= 255 for item in digest)
                or not any(digest)):
            raise DatasetIntegrityError("financial input epoch evidence digest is invalid")
    period = epoch["period"]
    period_keys = {"observed_period", "target_period", "observed_ordinal", "target_ordinal", "cadence",
                   "source_selection_digest", "identity_receipt_digest", "cadence_rule", "cadence_revision",
                   "observed_inputs", "target_inputs", "duration_chain"}
    if not isinstance(period, dict) or set(period) != period_keys:
        raise DatasetIntegrityError("financial input epoch period binding is invalid")
    observed, target = period["observed_ordinal"], period["target_ordinal"]
    if (type(observed) is not int or type(target) is not int
            or not 0 <= observed < target < 2**32
            or period["cadence"] != study.target_horizon.cadence
            or target - observed != study.target_horizon.periods_ahead
            or period["cadence_rule"] != "sec-frame-native-contiguous-periods-v1"
            or type(period["cadence_revision"]) is not int or period["cadence_revision"] != 1):
        raise DatasetIntegrityError("financial input epoch fiscal horizon differs")
    _period_end(period["observed_period"])
    terminal = period["target_period"]
    if terminal is not None:
        if _period_end(terminal) <= _period_end(period["observed_period"]):
            raise DatasetIntegrityError("financial target does not follow its observed period")
    chain = period["duration_chain"]
    if (not isinstance(chain, list) or not 1 <= len(chain) <= 1_024
            or len(chain) != (1 if terminal is None else target - observed + 1)
            or (period["target_inputs"] is None) != (terminal is None)):
        raise DatasetIntegrityError("financial native duration chain is invalid")
    measurement = _measurement_from_row(row)
    selection = {"role": measurement.role, "basis": measurement.basis,
                 "share_convention": measurement.share_convention}
    if (measurement.kind != "financial_amount" or epoch["selection"] != selection
            or row["value_decimal_mantissa"] is None or row["value_f64"] is not None
            or row["missing_reason"] is not None):
        raise DatasetIntegrityError("financial input epoch measurement differs from its row")
    kind = "common_book_equity" if measurement.role == "common_book_equity" else "reported"
    current = _financial_source_inputs(epoch["current_inputs"], kind)
    observed_inputs = _financial_source_inputs(period["observed_inputs"], kind)
    target_inputs = (() if terminal is None
                     else _financial_source_inputs(period["target_inputs"], kind))
    for inputs, expected_period in ((observed_inputs, period["observed_period"]),
                                    (target_inputs, terminal)):
        for reference in inputs:
            if _financial_row_context(reference).get("period") != expected_period:
                raise DatasetIntegrityError("financial native row reference differs from its period")
        if len(inputs) == 2 and inputs[0]["row_ordinal"] == inputs[1]["row_ordinal"]:
            raise DatasetIntegrityError("common book equity requires two distinct source rows")
    fact_keys = {"context", "concept", "value", "fact_context", "xbrl_evidence"}
    for fact, reference in zip(current, observed_inputs, strict=True):
        if set(fact) != fact_keys or fact["fact_context"] != reference["fact_context"]:
            raise DatasetIntegrityError("financial current input differs from its exact source row")
    for reference in chain:
        _period_end(_financial_row_context(reference).get("period"))
    if epoch["current_anchor"].get("fact_context") != chain[0]["fact_context"]:
        raise DatasetIntegrityError("financial current anchor differs from its exact source row")
    # Native admission verifies the source observations, complete cadence chain, and exact
    # feature amount. This mirror retains their sealed bytes and checks transport consistency.


def _close_example(
    previous: tuple[tuple[int, int], str, str] | None,
    current: tuple[tuple[int, int], str, str],
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
        or not _valid_unit(row["unit"])
        or not _valid_currency(row["currency"])
        or (
            row["missing_reason"] is not None
            and (row["unit"] is not None or row["currency"] is not None)
        )
    ):
        raise DatasetIntegrityError("dataset component value is invalid")


# Exact code-owned unit meanings from dataset_builder/financial.rs. Unsupported role/basis
# requests have no row unit and cannot be promoted by a Python descriptor.
_FINANCIAL_UNITS = {
    "msq.income.common": ("common_net_income", "total_common_equity", None),
    "msq.income.basic-share": ("common_net_income", "per_common_share", "reported_basic_earnings_per_share"),
    "msq.income.diluted-share": ("common_net_income", "per_common_share", "reported_diluted_earnings_per_share"),
    "msq.income.parent": ("parent_net_income", "reporting_entity_total", None),
    "msq.book.parent": ("parent_book_equity", "reporting_entity_total", None),
    "msq.book.common": ("common_book_equity", "total_common_equity", None),
    "msq.pref.income-adjust": ("preferred_income_adjustments", "reporting_entity_total", None),
    "msq.cfo.entity": ("operating_cash_flow", "reporting_entity_total", None),
    "msq.ppe-purchases.entity": ("property_plant_and_equipment_purchases", "reporting_entity_total", None),
    "msq.lt-borrow.entity": ("long_term_borrowing_proceeds", "reporting_entity_total", None),
    "msq.lt-repay.entity": ("long_term_debt_repayments", "reporting_entity_total", None),
    "msq.pref-dividend.entity": ("preferred_dividends_paid", "reporting_entity_total", None),
    "msq.pref-issued.entity": ("preferred_stock_issued_value", "reporting_entity_total", None),
}


def _measurement_from_row(row: Mapping[str, Any]) -> LabelMeasurement:
    unit = row["unit"]
    currency = row["currency"]
    if currency is not None:
        if unit in _FINANCIAL_UNITS:
            if row["target_coordinate_kind"] != 4:
                raise DatasetIntegrityError("financial amount lacks its native period binding")
            return LabelMeasurement("financial_amount", currency, *_FINANCIAL_UNITS[unit])
        if unit is not None:
            raise DatasetIntegrityError("monetary label measurement is ambiguous")
        return LabelMeasurement("price", currency)
    if unit == "market-squawk.return":
        return LabelMeasurement("return")
    if unit == "market-squawk.probability":
        return LabelMeasurement("probability")
    return LabelMeasurement("other_regression")


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
    measurement = None
    target = _target(value["kind"], value["target"])
    if value["kind"] == "feature" and value["measurement"] is not None:
        raise DatasetIntegrityError("feature component cannot declare an output measurement")
    if value["kind"] == "label" and value["measurement"] is not None:
        measurement = _measurement(value["measurement"])
    if target.kind == "fixed_horizon_event":
        event = json.loads(target.event_json)
        name = {"price_higher": "research.fixed-horizon-price-higher",
                "benchmark_outperformance": "research.fixed-horizon-benchmark-outperformance",
                "profit_after_costs": "research.fixed-horizon-profit-after-costs"}[event["kind"]]
        if (measurement != LabelMeasurement("probability") or value["name"] != name
                or value["version"] != 1 or value["scope"] != "instrument"
                or value["corporate_action_sensitivity"] != "requires_adjustment"):
            raise DatasetIntegrityError("event target differs from its original label recipe")
    return ComponentIdentity(
        corporate_action_sensitivity=value["corporate_action_sensitivity"],
        kind=value["kind"],
        name=value["name"],
        scope=value["scope"],
        version=value["version"],
        measurement=measurement,
        target=target,
    )


def _measurement(value: Any) -> LabelMeasurement:
    if not isinstance(value, dict) or "kind" not in value:
        raise DatasetIntegrityError("Task 11 label measurement is invalid")
    kind = value["kind"]
    if kind == "financial_amount":
        if (set(value) != {"kind", "currency", "role", "basis", "share_convention"}
                or not isinstance(value["currency"], str) or not _valid_currency(value["currency"])
                or (value["role"], value["basis"], value["share_convention"]) not in _FINANCIAL_UNITS.values()):
            raise DatasetIntegrityError("Task 11 financial measurement is invalid")
        return LabelMeasurement(kind, value["currency"], value["role"], value["basis"], value["share_convention"])
    if kind == "price":
        if (
            set(value) != {"kind", "currency"}
            or not isinstance(value["currency"], str)
            or not _valid_currency(value["currency"])
        ):
            raise DatasetIntegrityError("Task 11 price measurement is invalid")
        return LabelMeasurement(kind, value["currency"])
    if kind not in {"return", "probability", "other_regression"} or set(value) != {"kind"}:
        raise DatasetIntegrityError("Task 11 label measurement is invalid")
    return LabelMeasurement(kind)


def _target(component_kind: str, value: Any) -> LabelTarget:
    if not isinstance(value, dict) or "kind" not in value:
        raise DatasetIntegrityError("Task 11 target contract is invalid")
    kind = value["kind"]
    if component_kind == "feature":
        if value != {"kind": "not_applicable"}:
            raise DatasetIntegrityError("feature component target must be not applicable")
        return LabelTarget("not_applicable")
    if kind == "unsupported" and value == {"kind": "unsupported"}:
        return LabelTarget("unsupported")
    if kind == "financial_period" and set(value) == {"kind", "cadence", "periods_ahead"}:
        if _valid_fiscal_horizon(value["cadence"], value["periods_ahead"]):
            return LabelTarget(kind, cadence=value["cadence"], periods_ahead=value["periods_ahead"])
    if kind in {"fixed_horizon_terminal", "fixed_horizon_event"} and set(value) == (
            {"kind", "horizon_nanos", "origin_basis"} | ({"event"} if kind == "fixed_horizon_event" else set())):
        horizon = value["horizon_nanos"]
        if (type(horizon) is int and 0 < horizon < 2**64
                and isinstance(value["origin_basis"], str)
                and value["origin_basis"] in {"completed_bar_close", "exact_effective_timestamp",
                                               "named_session_close_for_nominal_daily_bar"}):
            if kind == "fixed_horizon_event":
                if value["origin_basis"] == "exact_effective_timestamp":
                    raise DatasetIntegrityError("event origin lacks its original bar policy")
                _probability_event(value["event"])
                return LabelTarget(kind, horizon, value["origin_basis"], event_json=json.dumps(
                    value["event"], sort_keys=True, separators=(",", ":"), allow_nan=False))
            return LabelTarget(kind, horizon, value["origin_basis"])
    raise DatasetIntegrityError("Task 11 label target contract is invalid")



def _probability_event(value: Any) -> None:
    if not isinstance(value, dict):
        raise DatasetIntegrityError("probability event is not a closed target")
    kind = value.get("kind")
    if kind == "price_higher" and set(value) == {"kind"}:
        return
    if kind == "benchmark_outperformance" and set(value) == {"kind", "benchmark_instrument_id", "benchmark_definition"}:
        _canonical_uuid(value["benchmark_instrument_id"])
        if UUID(value["benchmark_instrument_id"]).int == 0:
            raise DatasetIntegrityError("probability benchmark identity is reserved")
        _population_evidence_digest(value["benchmark_definition"])
        return
    if kind != "profit_after_costs" or set(value) != {"kind", "policy"}:
        raise DatasetIntegrityError("probability event kind is unsupported")
    policy = value["policy"]
    integer_fields = {"version", "execution_policy_version", "fee_basis_points", "slippage_basis_points",
        "maximum_random_slippage_basis_points", "maximum_participation_basis_points", "latency_nanos",
        "fee_decimal_scale", "quantity_lots", "maximum_entry_lag_nanos", "maximum_exit_lag_nanos", "seed"}
    keys = integer_fields | {"allow_partial_fills", "reporting_currency", "execution_basis",
        "daily_bar_assumed_spread_basis_points", "liquidity_priority", "convention"}
    if (not isinstance(policy, dict) or set(policy) != keys
            or any(type(policy[key]) is not int for key in integer_fields)
            or policy["version"] != 1 or policy["execution_policy_version"] != 3
            or any(not 0 <= policy[key] <= 10_000 for key in ("fee_basis_points", "slippage_basis_points",
                "maximum_random_slippage_basis_points", "maximum_participation_basis_points"))
            or policy["maximum_participation_basis_points"] == 0
            or not 0 <= policy["fee_decimal_scale"] <= 28
            or not 0 < policy["quantity_lots"] < 2**63
            or not 0 < policy["latency_nanos"] <= policy["maximum_entry_lag_nanos"] < 2**63
            or not policy["latency_nanos"] <= policy["maximum_exit_lag_nanos"] < 2**63
            or not 0 <= policy["seed"] < 2**64 or type(policy["allow_partial_fills"]) is not bool
            or not isinstance(policy["reporting_currency"], str) or not _valid_currency(policy["reporting_currency"])
            or policy["liquidity_priority"] != "signal_time_then_order_id"
            or policy["convention"] != "long_round_trip_total_wealth_including_entitlements"):
        raise DatasetIntegrityError("probability cost policy is invalid")
    spread = policy["daily_bar_assumed_spread_basis_points"]
    if not ((policy["execution_basis"] == "observed_quote_depth" and spread is None)
            or (policy["execution_basis"] == "completed_daily_bar"
                and type(spread) is int and 0 <= spread <= 10_000)):
        raise DatasetIntegrityError("probability cost execution basis is invalid")


def _valid_unit(value: Any) -> bool:
    return value is None or (
        isinstance(value, str)
        and 0 < len(value.encode()) <= 32
        and all(character.isalnum() or character in "-_./%" for character in value)
    )


def _valid_currency(value: Any) -> bool:
    return value is None or (
        isinstance(value, str)
        and len(value) == 3
        and value.isascii()
        and value.isalpha()
        and value.isupper()
    )


def _calendar_date(value: Any) -> date:
    if (not isinstance(value, dict) or set(value) != {"year", "month", "day"}
            or any(type(value[name]) is not int for name in ("year", "month", "day"))):
        raise DatasetIntegrityError("native calendar date is invalid")
    try:
        return date(value["year"], value["month"], value["day"])
    except ValueError as error:
        raise DatasetIntegrityError("native calendar date is outside the Python date domain") from error


def _date_mapping(value: date) -> dict[str, int]:
    if type(value) is not date:
        raise DatasetIntegrityError("native date cannot carry a time or timezone")
    return {"year": value.year, "month": value.month, "day": value.day}


def _date_offset(value: date) -> int:
    if type(value) is not date:
        raise DatasetIntegrityError("native date cannot carry a time or timezone")
    return (value - date(1970, 1, 1)).days


def _decision_coordinate(row: Mapping[str, Any]) -> UtcNanoseconds | date:
    exact, civil = row["decision_at"], row["decision_on"]
    if isinstance(exact, UtcNanoseconds) and civil is None:
        return exact
    if exact is None and type(civil) is date:
        return civil
    raise DatasetIntegrityError("dataset decision must have exactly one temporal precision")


def _split_policy(value: Any) -> SplitPolicy:
    if not isinstance(value, dict):
        raise DatasetIntegrityError("Task 11 split policy is invalid")
    names = ("train_end", "validation_end", "test_end")
    kind = value.get("kind")
    if kind == "exact_time" and set(value) == {"kind", *(name + "_unix_nanos" for name in names)}:
        ends = tuple(value[name + "_unix_nanos"] for name in names)
        if any(type(item) is not int or not -(2**63) <= item < 2**63 for item in ends):
            raise DatasetIntegrityError("Task 11 split timestamps are invalid")
    elif kind == "fiscal_dates" and set(value) == {"kind", *names}:
        ends = tuple(_calendar_date(value[name]) for name in names)
    else:
        raise DatasetIntegrityError("Task 11 split policy is invalid")
    if not ends[0] < ends[1] < ends[2]:
        raise DatasetIntegrityError("Task 11 split boundaries are not strictly ordered")
    return SplitPolicy(kind, *ends)


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
