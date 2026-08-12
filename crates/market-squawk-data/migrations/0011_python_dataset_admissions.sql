DROP TRIGGER analytical_generations_registered_schema_insert;

CREATE TRIGGER analytical_generations_registered_schema_insert
BEFORE INSERT ON analytical_generations
WHEN NOT (
    (
        NEW.schema_name = 'market_squawk.research_observations'
        AND NEW.schema_version = 3
        AND NEW.schema_fingerprint =
            X'4c11d3d99dd0128d236783b2891f92dd54a38182eebe8432ccf7a582fb3d5f68'
    ) OR (
        NEW.schema_name = 'market_squawk.feature_label_components'
        AND NEW.schema_version = 3
        AND NEW.schema_fingerprint =
            X'ca7f3447c5c353181b3776f2980a55dcbc54ae69a75b9231d5925912444322a4'
    )
) BEGIN
    SELECT RAISE(ABORT, 'analytical generation schema identity is not registered');
END;

CREATE TABLE python_dataset_admissions (
    export_sha256 BLOB PRIMARY KEY CHECK (
        length(export_sha256) = 32
        AND export_sha256 <>
            X'0000000000000000000000000000000000000000000000000000000000000000'
    ),
    catalog_identity BLOB NOT NULL CHECK (
        length(catalog_identity) = 32
        AND catalog_identity <>
            X'0000000000000000000000000000000000000000000000000000000000000000'
    ),
    dataset_id TEXT NOT NULL,
    manifest_version INTEGER NOT NULL CHECK (manifest_version > 0),
    descriptor_json BLOB NOT NULL CHECK (
        length(descriptor_json) BETWEEN 1 AND 1048576
        AND json_valid(CAST(descriptor_json AS TEXT))
    ),
    selection_digest_version INTEGER NOT NULL CHECK (selection_digest_version = 2),
    registered_at_ns INTEGER NOT NULL,
    UNIQUE (dataset_id, manifest_version),
    FOREIGN KEY (dataset_id, manifest_version)
        REFERENCES analytical_generations(dataset_id, manifest_version)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER python_dataset_admissions_immutable_update
BEFORE UPDATE ON python_dataset_admissions BEGIN
    SELECT RAISE(ABORT, 'Python dataset admissions are immutable');
END;

CREATE TRIGGER python_dataset_admissions_immutable_delete
BEFORE DELETE ON python_dataset_admissions BEGIN
    SELECT RAISE(ABORT, 'Python dataset admissions are immutable');
END;
