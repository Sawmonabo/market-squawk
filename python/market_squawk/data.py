"""Task 11 export-bound, bounded local feature/label dataset access."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import os
from pathlib import Path
import stat
from types import MappingProxyType
from typing import Any, Mapping

import pyarrow as pa

from . import _native
from ._data_validation import (
    CONTROL_EXPANSION,
    CONTROL_RETAINED_OVERHEAD,
    DEFAULT_MAX_BYTES,
    DEFAULT_MAX_ROWS,
    MAX_EXPORT_BYTES,
    SELECTED_ROW_RETAINED_BYTES,
    ComponentIdentity,
    DatasetIdentity,
    DatasetIntegrityError,
    SplitCounts,
    SplitPolicy,
    UtcNanoseconds,
    _component,
    _digest,
    _export,
    _path_parts,
    _preflight_parquet,
    _RowValidator,
    _row,
    _row_group_workspace_bound,
    _split_counts,
    _split_policy,
    _validate_schema,
)
from .finance import OperationContext


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
    _receipt: Any = field(repr=False, compare=False)
    _descriptor_bytes: bytes = field(repr=False, compare=False)

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

    def read(self, relative: str, maximum: int, context: OperationContext) -> bytearray:
        context.checkpoint()
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
                content = bytearray(metadata.st_size)
                retained = 0
                view = memoryview(content)
                while retained < metadata.st_size:
                    context.checkpoint()
                    read = os.readv(file_fd, [view[retained:]])
                    if read <= 0:
                        raise DatasetIntegrityError("dataset object changed during controlled read")
                    retained += read
                return content
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
    context: OperationContext,
) -> DatasetResult:
    """Read one exact Task 11 export and its explicitly named content-addressed objects."""

    export_digest = _digest(export_sha256)
    if not 1 <= max_rows <= DEFAULT_MAX_ROWS or not 1 <= max_bytes <= DEFAULT_MAX_BYTES:
        raise DatasetIntegrityError("dataset result limits are invalid")
    if not isinstance(context, OperationContext):
        raise DatasetIntegrityError("dataset operation context is invalid")
    cutoff = as_of if isinstance(as_of, UtcNanoseconds) else UtcNanoseconds.from_datetime(as_of)
    try:
        receipt, native_descriptor = _native.open_dataset_admission(
            str(Path(root)),
            export_sha256,
            cutoff.unix_nanos,
            max_rows,
            max_bytes,
            context,
        )
    except (OSError, ValueError) as error:
        raise DatasetIntegrityError("native catalog admission rejected the dataset") from error
    export_bytes = bytes(native_descriptor)
    if len(export_bytes) > min(MAX_EXPORT_BYTES, max_bytes):
        raise DatasetIntegrityError("Task 11 export exceeds the retained-byte bound")
    if hashlib.sha256(export_bytes).digest() != export_digest:
        raise DatasetIntegrityError("native Task 11 export identity mismatch")
    control_retained = len(export_bytes) * CONTROL_EXPANSION + CONTROL_RETAINED_OVERHEAD
    if control_retained >= max_bytes:
        raise DatasetIntegrityError("dataset control metadata exceeds the retained-byte bound")
    manifest = _export(export_bytes)
    objects = manifest["objects"]
    total_rows = sum(_positive_int(item["row_count"]) for item in objects)
    total_bytes = sum(_positive_int(item["size_bytes"]) for item in objects)
    if total_rows > DEFAULT_MAX_ROWS or total_bytes > max_bytes - control_retained:
        raise DatasetIntegrityError("dataset objects exceed the reader bounds")

    controlled = _ControlledRoot(Path(root) / "artifacts")
    try:
        components = tuple(_component(value) for value in manifest["components"])
        split_policy = _split_policy(manifest["split_policy"])
        expected_counts = _split_counts(manifest["split_counts"])
        validator = _RowValidator(components, split_policy, expected_counts)
        selected_rows: list[Mapping[str, Any]] = []
        selected_retained = 0
        for item in objects:
            context.checkpoint()
            object_limit = max_bytes - control_retained - selected_retained
            object_bytes = controlled.read(item["path"], object_limit, context)
            if len(object_bytes) != item["size_bytes"]:
                raise DatasetIntegrityError("dataset object size mismatch")
            if hashlib.sha256(object_bytes).digest() != _digest(item["sha256"]):
                raise DatasetIntegrityError("dataset object hash mismatch")
            try:
                parquet_file = _preflight_parquet(
                    object_bytes,
                    max_bytes=max_bytes
                    - control_retained
                    - selected_retained
                    - len(object_bytes),
                    expected_rows=item["row_count"],
                )
                lineage = hashlib.sha256()
                lineage.update(b"market-squawk/feature-label-object-lineage/v1")
                lineage.update(_digest(manifest["dataset"]["build_spec_sha256"]))
                object_rows = 0
                for row_group_index in range(parquet_file.metadata.num_row_groups):
                    context.checkpoint()
                    row_group = parquet_file.metadata.row_group(row_group_index)
                    workspace = _row_group_workspace_bound(row_group)
                    if (
                        control_retained
                        + selected_retained
                        + len(object_bytes)
                        + workspace
                        > max_bytes
                    ):
                        raise DatasetIntegrityError(
                            "dataset decoded row group exceeds the retained-byte bound"
                        )
                    row_group_table = parquet_file.read_row_group(row_group_index)
                    if row_group_table.nbytes > _decoded_row_group_bound(row_group.num_rows):
                        raise DatasetIntegrityError(
                            "dataset decoded row group violated its admitted bound"
                        )
                    _validate_schema(row_group_table.schema, manifest["dataset"])
                    validator.validate_table(row_group_table)
                    object_rows += row_group_table.num_rows
                    for index in range(row_group_table.num_rows):
                        if index % 128 == 0:
                            context.checkpoint()
                        row = _row(row_group_table, index)
                        validator.consume(row)
                        row_lineage = row["lineage_sha256"]
                        if (
                            not isinstance(row_lineage, bytes)
                            or len(row_lineage) != 32
                            or row_lineage == bytes(32)
                        ):
                            raise DatasetIntegrityError("dataset row lineage is invalid")
                        lineage.update(row_lineage)
                        if row["cutoff_at"].unix_nanos <= cutoff.unix_nanos:
                            if len(selected_rows) >= max_rows:
                                raise DatasetIntegrityError(
                                    "dataset result exceeds its requested row bound"
                                )
                            next_retained = selected_retained + SELECTED_ROW_RETAINED_BYTES
                            if (
                                control_retained
                                + len(object_bytes)
                                + workspace
                                + next_retained
                                > max_bytes
                            ):
                                raise DatasetIntegrityError(
                                    "dataset selected rows exceed the retained-byte bound"
                                )
                            selected_rows.append(MappingProxyType(row))
                            selected_retained = next_retained
                    del row_group_table
            except (pa.ArrowException, OSError, OverflowError, ValueError) as error:
                raise DatasetIntegrityError("dataset Parquet object is invalid") from error
            if object_rows != item["row_count"]:
                raise DatasetIntegrityError("dataset object row count mismatch")
            if lineage.digest() != _digest(item["lineage_sha256"]):
                raise DatasetIntegrityError("dataset object lineage mismatch")
            del parquet_file
            del object_bytes
        validator.finish()
        rows = tuple(selected_rows)
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
            catalog_identity_sha256=receipt.catalog_identity,
            export_sha256=receipt.export_sha256,
            selection_sha256=receipt.selection_sha256,
            selection_as_of_unix_nanos=receipt.as_of_unix_nanos,
            selected_component_rows=len(rows),
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
            _receipt=receipt,
            _descriptor_bytes=export_bytes,
        )
    finally:
        controlled.close()


def _verify_dataset_receipt(
    dataset: DatasetResult, context: OperationContext
) -> Mapping[str, Any]:
    """Revalidate the opaque native receipt and every Python-visible descriptor claim."""

    if not isinstance(dataset, DatasetResult) or not isinstance(context, OperationContext):
        raise DatasetIntegrityError("dataset receipt inputs are invalid")
    try:
        dataset._receipt.verify(dataset._descriptor_bytes, dataset.rows, context)
    except (AttributeError, TypeError, ValueError) as error:
        raise DatasetIntegrityError("dataset receipt revalidation failed") from error
    manifest = _export(dataset._descriptor_bytes)
    source = manifest["dataset"]
    expected_identity = DatasetIdentity(
        dataset_id=source["dataset_id"],
        manifest_version=source["manifest_version"],
        schema_name=source["schema_name"],
        schema_version=source["schema_version"],
        schema_sha256=source["schema_sha256"],
        manifest_sha256=source["manifest_sha256"],
        build_spec_sha256=source["build_spec_sha256"],
        universe_sha256=source["universe_sha256"],
        policy_sha256=source["policy_sha256"],
        catalog_identity_sha256=dataset._receipt.catalog_identity,
        export_sha256=dataset._receipt.export_sha256,
        selection_sha256=dataset._receipt.selection_sha256,
        selection_as_of_unix_nanos=dataset._receipt.as_of_unix_nanos,
        selected_component_rows=len(dataset.rows),
    )
    expected_components = tuple(_component(value) for value in manifest["components"])
    expected_policy = _split_policy(manifest["split_policy"])
    expected_counts = _split_counts(manifest["split_counts"])
    expected_complete = dataset._receipt.as_of_unix_nanos >= expected_policy.test_end_unix_nanos
    if (
        dataset.export_sha256 != dataset._receipt.export_sha256
        or dataset.identity != expected_identity
        or dataset.universe_id != source["universe_id"]
        or dataset.as_of.unix_nanos != dataset._receipt.as_of_unix_nanos
        or dataset.components != expected_components
        or dataset.split_policy != expected_policy
        or dataset.split_counts != expected_counts
        or dataset.missing_value_policy != manifest["missing_value_policy"]
        or dataset.complete != expected_complete
    ):
        raise DatasetIntegrityError("dataset claims differ from the native catalog receipt")
    return MappingProxyType(manifest)
