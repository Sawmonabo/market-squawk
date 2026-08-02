DROP TRIGGER analytical_generations_registered_schema_insert;

CREATE TRIGGER analytical_generations_registered_schema_insert
BEFORE INSERT ON analytical_generations
WHEN NOT (
    (
        NEW.schema_name = 'market_squawk.research_observations'
        AND NEW.schema_version = 3
        AND NEW.schema_fingerprint =
            X'8d7dd570339626df758de3aab0bf49677551ec31491cd20e3eadd3748232c6c8'
    ) OR (
        NEW.schema_name = 'market_squawk.feature_label_components'
        AND NEW.schema_version = 2
        AND NEW.schema_fingerprint =
            X'12a2745f755b8614ff52e8210fa5e7c9ffb621e6301afdba58d60fa63a838ce9'
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
    selection_digest_version INTEGER NOT NULL CHECK (selection_digest_version = 1),
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
