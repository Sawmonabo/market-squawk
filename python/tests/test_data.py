from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq

from market_squawk.data import (
    DatasetIntegrityError,
    UtcNanoseconds,
    _preflight_parquet,
    open_dataset,
)
from market_squawk.finance import OperationContext


BUILD = "33" * 32
MANIFEST = "31" * 32
POLICY = "35" * 32
SCHEMA = "ca7f3447c5c353181b3776f2980a55dcbc54ae69a75b9231d5925912444322a4"
UNIVERSE = "34" * 32
INSTRUMENT = "0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1"
PARENT_SCHEMA = "8d7dd570339626df758de3aab0bf49677551ec31491cd20e3eadd3748232c6c8"
MIGRATIONS = (
    Path(__file__).resolve().parents[2]
    / "crates"
    / "market-squawk-data"
    / "migrations"
)


def _fixed(values, width: int) -> pa.Array:
    encoded = []
    for value in values:
        if value is None:
            encoded.append(None)
        else:
            raw = value.encode()
            encoded.append(raw + bytes(width - len(raw)))
    return pa.array(encoded, type=pa.binary(width))


def _fixture(
    root: Path,
    *,
    corrupt_split: bool = False,
    label_mantissas: tuple[int, ...] | None = None,
    label_measurement: dict[str, str] | None = None,
    initialize_root: Callable[[Path], None] | None = None,
) -> str:
    output_measurement = (
        {"kind": "other_regression"}
        if label_measurement is None
        else label_measurement
    )
    for directory in ("artifacts", "control", "journal"):
        (root / directory).mkdir()
    if initialize_root is not None:
        initialize_root(root)
        _verify_initialized_root(root)
    metadata = {
        b"market_squawk.build_sha256": BUILD.encode(),
        b"market_squawk.component_layout": b"fixed-width-long-form-v3",
        b"market_squawk.dataset": b"fixture-training",
        b"market_squawk.policy_sha256": POLICY.encode(),
        b"market_squawk.schema": b"market_squawk.feature_label_components",
        b"market_squawk.schema_fingerprint_sha256": SCHEMA.encode(),
        b"market_squawk.schema_version": b"3",
        b"market_squawk.timestamp_timezone": b"UTC",
        b"market_squawk.universe_sha256": UNIVERSE.encode(),
    }
    schema = pa.schema(
        [
            pa.field("example_id", pa.binary(256), nullable=False),
            pa.field("instrument_id", pa.binary(16), nullable=False),
            pa.field("cutoff_at", pa.timestamp("ns", tz="+00:00"), nullable=False),
            pa.field(
                "observed_effective_at",
                pa.timestamp("ns", tz="+00:00"),
                nullable=True,
            ),
            pa.field(
                "label_effective_at",
                pa.timestamp("ns", tz="+00:00"),
                nullable=True,
            ),
            pa.field("target_coordinate_kind", pa.uint8(), nullable=False),
            pa.field("split", pa.uint8(), nullable=False),
            pa.field("component_kind", pa.uint8(), nullable=False),
            pa.field("component_name", pa.binary(256), nullable=False),
            pa.field("component_version", pa.uint32(), nullable=False),
            pa.field("value_f64", pa.float64(), nullable=True),
            pa.field("value_decimal_mantissa", pa.decimal128(38, 0), nullable=True),
            pa.field("value_decimal_scale", pa.uint8(), nullable=True),
            pa.field("unit", pa.binary(32), nullable=True),
            pa.field("currency", pa.binary(3), nullable=True),
            pa.field("missing_reason", pa.binary(256), nullable=True),
            pa.field("lineage_sha256", pa.binary(32), nullable=False),
        ],
        metadata=metadata,
    )
    example_ids = [f"example-{index}" for index in range(1, 7)]
    cutoffs = [100, 120, 140, 160, 300, 320]
    splits = ["train"] * 4 + ["validation"] * 2
    feature_values = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
    label_mantissas = (
        (-5, 25, 35, 65, 75, 105)
        if label_mantissas is None
        else label_mantissas
    )
    if len(label_mantissas) != len(example_ids):
        raise ValueError("fixture labels must match the deterministic example set")
    lineages = [bytes([index]) * 32 for index in range(1, 13)]
    table = pa.Table.from_arrays(
        [
            _fixed([value for value in example_ids for _ in range(2)], 256),
            pa.array([UUID(INSTRUMENT).bytes] * 12, type=pa.binary(16)),
            pa.array(
                [value for value in cutoffs for _ in range(2)],
                type=schema.field("cutoff_at").type,
            ),
            pa.array(
                [value - 20 for value in cutoffs for _ in range(2)],
                type=schema.field("observed_effective_at").type,
            ),
            pa.array(
                [value - 10 for value in cutoffs for _ in range(2)],
                type=schema.field("label_effective_at").type,
            ),
            pa.array([1] * 12, type=pa.uint8()),
            pa.array(
                [2] * 12
                if corrupt_split
                else [
                    {"train": 1, "validation": 2}[value]
                    for value in splits
                    for _ in range(2)
                ],
                type=pa.uint8(),
            ),
            pa.array([1, 2] * 6, type=pa.uint8()),
            _fixed(["research.price-return", "forward-return"] * 6, 256),
            pa.array([1] * 12, type=pa.uint32()),
            pa.array(
                [value for feature in feature_values for value in (feature, None)],
                type=pa.float64(),
            ),
            pa.array(
                [value for label in label_mantissas for value in (None, Decimal(label))],
                type=pa.decimal128(38, 0),
            ),
            pa.array(
                [value for _ in label_mantissas for value in (None, 1)],
                type=pa.uint8(),
            ),
            _fixed(
                [
                    value
                    for _ in label_mantissas
                    for value in (
                        "ratio",
                        {
                            "return": "market-squawk.return",
                            "probability": "market-squawk.probability",
                        }.get(output_measurement.get("kind")),
                    )
                ],
                32,
            ),
            _fixed(
                [
                    value
                    for _ in label_mantissas
                    for value in (
                        None,
                        output_measurement.get("currency"),
                    )
                ],
                3,
            ),
            _fixed([None] * 12, 256),
            pa.array(lineages, type=pa.binary(32)),
        ],
        schema=schema,
    )
    encoded = pa.BufferOutputStream()
    pq.write_table(table, encoded, compression="zstd", use_dictionary=False)
    object_bytes = encoded.getvalue().to_pybytes()
    object_sha256 = hashlib.sha256(object_bytes).hexdigest()
    object_reference = f"objects/sha256/{object_sha256[:2]}/{object_sha256}.parquet"
    object_path = root / "artifacts" / object_reference
    object_path.parent.mkdir(parents=True)
    object_path.write_bytes(object_bytes)
    object_lineage = hashlib.sha256()
    object_lineage.update(b"market-squawk/feature-label-object-lineage/v1")
    object_lineage.update(bytes.fromhex(BUILD))
    for lineage in lineages:
        object_lineage.update(lineage)
    export = {
        "components": [
            {
                "corporate_action_sensitivity": "requires_adjustment",
                "kind": "feature",
                "measurement": None,
                "name": "research.price-return",
                "scope": "instrument",
                "target": {"kind": "not_applicable"},
                "version": 1,
            },
            {
                "corporate_action_sensitivity": "requires_adjustment",
                "kind": "label",
                "measurement": output_measurement,
                "name": "forward-return",
                "scope": "instrument",
                "target": {
                    "horizon_nanos": 10,
                    "kind": "fixed_horizon_terminal",
                },
                "version": 1,
            },
        ],
        "dataset": {
            "build_spec_sha256": BUILD,
            "dataset_id": "fixture-training",
            "manifest_sha256": MANIFEST,
            "manifest_version": 1,
            "policy_sha256": POLICY,
            "schema_name": "market_squawk.feature_label_components",
            "schema_sha256": SCHEMA,
            "schema_version": 3,
            "universe_id": "fixture-universe",
            "universe_sha256": UNIVERSE,
        },
        "missing_value_policy": "reject",
        "objects": [
            {
                "artifact_id": "018f3c2a-91ab-7ccd-b3de-123456789abc",
                "lineage_sha256": object_lineage.hexdigest(),
                "path": object_reference,
                "row_count": 12,
                "sha256": object_sha256,
                "size_bytes": object_path.stat().st_size,
            }
        ],
        "parents": [
            {
                "manifest": {
                    "dataset_id": "fixture-source",
                    "manifest_sha256": "41" * 32,
                    "manifest_version": 1,
                    "schema_name": "market_squawk.research_observations",
                    "schema_sha256": PARENT_SCHEMA,
                    "schema_version": 3,
                },
                "relation": "derived_input",
            }
        ],
        "point_in_time": {"revision_mode": "latest_known", "version": 1},
        "schema_version": 4,
        "split_counts": {"test": 0, "train": 4, "validation": 2},
        "split_policy": {
            "test_end_unix_nanos": 600,
            "train_end_unix_nanos": 200,
            "validation_end_unix_nanos": 400,
        },
    }
    export_bytes = json.dumps(export, sort_keys=True, separators=(",", ":")).encode()
    export_sha256 = hashlib.sha256(export_bytes).hexdigest()
    _install_catalog(
        root,
        export_bytes,
        export_sha256,
        export,
        initialized_root=initialize_root is not None,
    )
    return export_sha256


def _verify_initialized_root(root: Path) -> None:
    catalog = (root / "catalog.sqlite3").resolve(strict=True)
    connection = sqlite3.connect(f"{catalog.as_uri()}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            """SELECT sequence, format_version, event_kind, transition_kind,
                      authority_generation, root_binding_generation,
                      root_marker_record_digest IS NOT NULL,
                      stable_root_identity IS NOT NULL,
                      root_binding_record_digest IS NOT NULL
               FROM analytical_artifact_root_authority_events
               ORDER BY sequence"""
        ).fetchall()
    finally:
        connection.close()
    if len(rows) != 2:
        raise RuntimeError("signed fixture initialization did not commit two authority events")
    prepared, bound = rows
    generation = bound[4]
    if (
        prepared[:4] != (1, 2, "prepared", "initialize")
        or bound[:4] != (2, 2, "bound", "initialize")
        or not isinstance(generation, int)
        or generation <= 0
        or prepared[4:] != (generation, generation, 0, 0, 0)
        or bound[4:] != (generation, generation, 1, 1, 1)
    ):
        raise RuntimeError("signed fixture initialization did not commit bound v2 authority")
    artifact_root = root / "artifacts"
    identity = artifact_root / ".analytical-root.identity.v2"
    binding = artifact_root / f".analytical-root-catalog.binding.{generation:016}"
    if (
        not identity.is_file()
        or identity.stat().st_size == 0
        or not binding.is_file()
        or binding.stat().st_size == 0
        or identity.with_name(f"{identity.name}.pending").exists()
        or binding.with_name(f"{binding.name}.pending").exists()
    ):
        raise RuntimeError("signed fixture initialization controls are not committed")


def _install_catalog(
    root: Path,
    export_bytes: bytes,
    export_sha256: str,
    export: dict,
    *,
    initialized_root: bool = False,
) -> None:
    catalog = root / "catalog.sqlite3"
    connection = sqlite3.connect(catalog)
    try:
        os.chmod(catalog, 0o600)
        connection.execute("PRAGMA foreign_keys=ON")
        if connection.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower() != "wal":
            raise RuntimeError("test catalog did not enter WAL mode")
        if not initialized_root:
            connection.execute("PRAGMA application_id=1297305931")
            migration_script = ["BEGIN IMMEDIATE;"]
            for version, migration in enumerate(
                sorted(MIGRATIONS.glob("*.sql")), start=1
            ):
                sql = migration.read_text(encoding="utf-8")
                digest = hashlib.sha256(sql.encode()).hexdigest()
                migration_script.extend(
                    (
                        sql,
                        "INSERT INTO schema_migrations(version, sha256, applied_at_ns) "
                        f"VALUES ({version}, X'{digest}', 1);",
                    )
                )
            migration_script.append("COMMIT;")
            connection.executescript("\n".join(migration_script))
        identity = _catalog_identity(catalog)
        _insert_catalog_fixture(connection, identity, export_bytes, export_sha256, export)
        connection.commit()
    finally:
        connection.close()
        os.chmod(catalog, 0o600)


def _catalog_identity(catalog: Path) -> bytes:
    resolved = str(catalog.resolve(strict=True))
    if os.name == "nt" and not resolved.startswith("\\\\?\\"):
        resolved = (
            f"\\\\?\\UNC\\{resolved[2:]}"
            if resolved.startswith("\\\\")
            else f"\\\\?\\{resolved}"
        )
    path = os.fsencode(resolved)
    metadata = os.stat(catalog, follow_symlinks=False)
    device = metadata.st_dev
    inode = metadata.st_ino
    if os.name == "nt":
        device &= (1 << 32) - 1
        inode &= (1 << 64) - 1
    digest = hashlib.sha256()
    digest.update(b"market-squawk/catalog-artifact-root-binding/v2")
    digest.update(len(path).to_bytes(8, "big"))
    digest.update(path)
    digest.update(device.to_bytes(8, "big"))
    digest.update(inode.to_bytes(8, "big"))
    return digest.digest()


def _insert_catalog_fixture(
    connection: sqlite3.Connection,
    catalog_identity: bytes,
    export_bytes: bytes,
    export_sha256: str,
    export: dict,
) -> None:
    source_id = "fixture-source-adapter"
    revision = bytes.fromhex("51" * 32)
    rights = bytes.fromhex("52" * 32)
    payload = bytes.fromhex("53" * 32)
    connection.execute(
        "INSERT INTO sources VALUES (?1, ?2, 1, 1)", (source_id, revision)
    )
    connection.execute(
        "INSERT INTO source_revisions VALUES (?1, ?2, '{}', 1)",
        (source_id, revision),
    )
    connection.execute(
        """INSERT INTO source_rights (
               rights_id, source_id, payload_algorithm, payload_digest, retrieved_at_ns,
               basis_reference, basis_algorithm, basis_digest, authorization_algorithm,
               authorization_digest, authorization_expires_at_ns, operation_mask,
               admitted_at_ns, basis_kind, basis_root_algorithm, basis_root_digest,
               fingerprint_version
           ) VALUES (?1, ?2, 2, ?3, 1, 'fixture', 2, ?4, 2, ?5, NULL, 63, 1,
                     'reviewed_terms', NULL, NULL, 2)""",
        (rights, source_id, payload, bytes.fromhex("54" * 32), bytes.fromhex("55" * 32)),
    )
    runs = [
        "018f3c2a-91ab-7ccd-b3de-123456789ab0",
        "018f3c2a-91ab-7ccd-b3de-123456789ab1",
    ]
    artifacts = [
        ("018f3c2a-91ab-7ccd-b3de-123456789ab3", "control/parent.json", "61" * 32, 1),
        ("018f3c2a-91ab-7ccd-b3de-123456789ab4", "control/child.json", "62" * 32, 1),
        (
            export["objects"][0]["artifact_id"],
            export["objects"][0]["path"],
            export["objects"][0]["sha256"],
            export["objects"][0]["size_bytes"],
        ),
    ]
    artifact_groups = ((artifacts[0],), (artifacts[1], artifacts[2]))
    for index, (run_id, group) in enumerate(zip(runs, artifact_groups, strict=True)):
        connection.execute(
            "INSERT INTO ingest_runs VALUES (?1, ?2, ?3, 2, ?4, 'persist', ?5, "
            "'reserved', 2, NULL)",
            (run_id, f"fixture-{index}", source_id, payload, rights),
        )
        for ordinal, artifact in enumerate(group):
            connection.execute(
                """INSERT INTO artifacts (
                       artifact_id, run_id, publication_ordinal, relative_reference,
                       content_algorithm, content_digest, size_bytes, created_at_ns
                   ) VALUES (?1, ?2, ?3, ?4, 2, ?5, ?6, 4)""",
                (
                    artifact[0],
                    run_id,
                    ordinal,
                    artifact[1],
                    bytes.fromhex(artifact[2]),
                    artifact[3],
                ),
            )
    connection.execute(
        """INSERT INTO dataset_manifests (
               manifest_id, run_id, dataset_name, schema_version, artifact_id,
               content_algorithm, content_digest, created_at_ns
           ) VALUES (?1, ?2, 'fixture-source', 3, ?3, 2, ?4, 4)""",
        (
            "018f3c2a-91ab-7ccd-b3de-123456789ab5",
            runs[0],
            artifacts[0][0],
            bytes.fromhex("41" * 32),
        ),
    )
    connection.execute(
        """INSERT INTO dataset_manifests (
               manifest_id, run_id, dataset_name, schema_version, artifact_id,
               content_algorithm, content_digest, created_at_ns
           ) VALUES (?1, ?2, 'fixture-training', 3, ?3, 2, ?4, 4)""",
        (
            "018f3c2a-91ab-7ccd-b3de-123456789ab6",
            runs[1],
            artifacts[2][0],
            bytes.fromhex(MANIFEST),
        ),
    )
    for run_id in runs:
        connection.execute(
            "UPDATE ingest_runs SET state='succeeded', completed_at_ns=4 WHERE run_id=?1",
            (run_id,),
        )
    connection.execute(
        """INSERT INTO analytical_generations VALUES (
               1, 'fixture-source', 1, ?1, ?2, 1, 1,
               'market_squawk.research_observations', 3, ?3, ?4,
               'ingest', 0, NULL, 4)""",
        (
            bytes.fromhex("41" * 32),
            bytes.fromhex("63" * 32),
            bytes.fromhex(PARENT_SCHEMA),
            "018f3c2a-91ab-7ccd-b3de-123456789ab5",
        ),
    )
    object_entry = export["objects"][0]
    connection.execute(
        """INSERT INTO analytical_generations VALUES (
               2, 'fixture-training', 1, ?1, ?2, 12, ?3,
               'market_squawk.feature_label_components', 3, ?4, ?5,
               'derived', 1, ?6, 4)""",
        (
            bytes.fromhex(MANIFEST),
            bytes.fromhex(object_entry["lineage_sha256"]),
            object_entry["size_bytes"],
            bytes.fromhex(SCHEMA),
            "018f3c2a-91ab-7ccd-b3de-123456789ab6",
            bytes.fromhex(BUILD),
        ),
    )
    connection.execute(
        "INSERT INTO analytical_generation_objects VALUES "
        "('fixture-training', 1, 0, ?1, ?2, 12, ?3, ?4)",
        (
            object_entry["artifact_id"],
            bytes.fromhex(object_entry["sha256"]),
            object_entry["size_bytes"],
            bytes.fromhex(object_entry["lineage_sha256"]),
        ),
    )
    connection.execute(
        """INSERT INTO analytical_generation_parents VALUES (
               'fixture-training', 1, 0, 'derived_input', 1, 'fixture-source', 1,
               'market_squawk.research_observations', 3, ?1, ?2)""",
        (bytes.fromhex(PARENT_SCHEMA), bytes.fromhex("41" * 32)),
    )
    connection.execute(
        "INSERT INTO python_dataset_admissions VALUES (?1, ?2, 'fixture-training', 1, ?3, 2, 4)",
        (bytes.fromhex(export_sha256), catalog_identity, export_bytes),
    )


def _fixture_object_path(root: Path) -> Path:
    objects = tuple((root / "artifacts" / "objects" / "sha256").glob("*/*.parquet"))
    if len(objects) != 1:
        raise RuntimeError("fixture does not contain exactly one Parquet object")
    return objects[0]


class DatasetContracts(unittest.TestCase):
    def test_task11_export_bound_pit_read_preserves_exact_values_and_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root)
            result = open_dataset(
                root,
                digest,
                UtcNanoseconds(100),
                max_rows=16,
                max_bytes=1_000_000,
                context=OperationContext(60_000, 1_000_000),
            )

            self.assertEqual(result.export_sha256, digest)
            self.assertEqual(result.manifest_sha256, MANIFEST)
            self.assertEqual(result.as_of.unix_nanos, 100)
            self.assertEqual(len(result.rows), 2)
            self.assertEqual(result.rows[0]["value_f64"], 0.0)
            self.assertEqual(result.rows[1]["value_decimal_mantissa"], Decimal("-5"))
            self.assertEqual(result.rows[1]["value_decimal_scale"], 1)
            self.assertEqual(result.rows[0]["lineage_sha256"], bytes([1]) * 32)
            self.assertEqual(
                result.rows[0]["observed_effective_at"], UtcNanoseconds(80)
            )
            self.assertEqual(result.rows[0]["label_effective_at"], UtcNanoseconds(90))
            target = next(
                component.target
                for component in result.components
                if component.kind == "label"
            )
            self.assertEqual(
                (target.kind, target.horizon_nanos),
                ("fixed_horizon_terminal", 10),
            )
            self.assertFalse(result.complete)

            aware = datetime(1970, 1, 1, tzinfo=timezone.utc)
            self.assertEqual(UtcNanoseconds.from_datetime(aware).unix_nanos, 0)
            with self.assertRaises(ValueError):
                UtcNanoseconds.from_datetime(datetime(1970, 1, 1))

    def test_hash_path_and_chronological_split_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root)
            object_path = _fixture_object_path(root)
            object_path.write_bytes(object_path.read_bytes() + b"tampered")
            with self.assertRaises(DatasetIntegrityError):
                open_dataset(
                    root,
                    digest,
                    UtcNanoseconds(600),
                    context=OperationContext(60_000, 10_000_000),
                )

    def test_parquet_expansion_is_rejected_before_arrow_materialization(self) -> None:
        table = pa.table(
            {"payload": [f"{index:06d}-" + "x" * 512 for index in range(4_096)]}
        )
        output = pa.BufferOutputStream()
        pq.write_table(table, output, compression="zstd", use_dictionary=False)
        content = output.getvalue().to_pybytes()
        bound = len(content) * 2
        self.assertLess(bound, table.nbytes)
        with self.assertRaises(DatasetIntegrityError):
            _preflight_parquet(content, max_bytes=bound, expected_rows=table.num_rows)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root)
            object_path = _fixture_object_path(root)
            original = root / "original.parquet"
            object_path.replace(original)
            object_path.symlink_to(original)
            with self.assertRaises(DatasetIntegrityError):
                open_dataset(
                    root,
                    digest,
                    UtcNanoseconds(600),
                    context=OperationContext(60_000, 10_000_000),
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            digest = _fixture(root, corrupt_split=True)
            with self.assertRaises(DatasetIntegrityError):
                open_dataset(
                    root,
                    digest,
                    UtcNanoseconds(600),
                    context=OperationContext(60_000, 10_000_000),
                )


if __name__ == "__main__":
    unittest.main()
