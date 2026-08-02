CREATE TABLE analytical_generations (
    dataset_id TEXT NOT NULL CHECK (
        length(CAST(dataset_id AS BLOB)) BETWEEN 1 AND 256
    ),
    manifest_version INTEGER NOT NULL CHECK (manifest_version > 0),
    content_hash BLOB NOT NULL CHECK (length(content_hash) = 32),
    lineage_hash BLOB NOT NULL CHECK (length(lineage_hash) = 32),
    row_count INTEGER NOT NULL CHECK (row_count > 0),
    total_bytes INTEGER NOT NULL CHECK (total_bytes > 0),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    anchor_manifest_id TEXT NOT NULL UNIQUE REFERENCES dataset_manifests(manifest_id),
    parent_version INTEGER,
    generation_kind TEXT NOT NULL CHECK (generation_kind IN ('ingest', 'compaction')),
    created_at_ns INTEGER NOT NULL,
    PRIMARY KEY (dataset_id, manifest_version),
    UNIQUE (dataset_id, content_hash),
    FOREIGN KEY (dataset_id, parent_version)
        REFERENCES analytical_generations(dataset_id, manifest_version),
    CHECK (
        (manifest_version = 1 AND parent_version IS NULL)
        OR (manifest_version > 1 AND parent_version = manifest_version - 1)
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE analytical_generation_objects (
    dataset_id TEXT NOT NULL,
    manifest_version INTEGER NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
    content_hash BLOB NOT NULL CHECK (length(content_hash) = 32),
    row_count INTEGER NOT NULL CHECK (row_count > 0),
    size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
    lineage_hash BLOB NOT NULL CHECK (length(lineage_hash) = 32),
    PRIMARY KEY (dataset_id, manifest_version, ordinal),
    UNIQUE (dataset_id, manifest_version, artifact_id),
    FOREIGN KEY (dataset_id, manifest_version)
        REFERENCES analytical_generations(dataset_id, manifest_version)
) STRICT, WITHOUT ROWID;

CREATE INDEX analytical_generations_latest
ON analytical_generations(dataset_id, manifest_version DESC);

CREATE INDEX analytical_generation_objects_content
ON analytical_generation_objects(content_hash);

CREATE TRIGGER analytical_generations_immutable_update
BEFORE UPDATE ON analytical_generations BEGIN
    SELECT RAISE(ABORT, 'analytical generations are immutable');
END;

CREATE TRIGGER analytical_generations_immutable_delete
BEFORE DELETE ON analytical_generations BEGIN
    SELECT RAISE(ABORT, 'analytical generations are immutable');
END;

CREATE TRIGGER analytical_generation_objects_immutable_update
BEFORE UPDATE ON analytical_generation_objects BEGIN
    SELECT RAISE(ABORT, 'analytical generation objects are immutable');
END;

CREATE TRIGGER analytical_generation_objects_immutable_delete
BEFORE DELETE ON analytical_generation_objects BEGIN
    SELECT RAISE(ABORT, 'analytical generation objects are immutable');
END;
