DROP TRIGGER analytical_generation_objects_immutable_delete;
DROP TRIGGER analytical_generation_objects_immutable_update;
DROP TRIGGER analytical_generations_immutable_delete;
DROP TRIGGER analytical_generations_immutable_update;
DROP TRIGGER analytical_generations_parent_schema_insert;
DROP TRIGGER analytical_generations_registered_schema_insert;
DROP INDEX analytical_generation_objects_content;
DROP INDEX analytical_generations_latest;

ALTER TABLE analytical_generation_objects
RENAME TO analytical_generation_objects_v6;

ALTER TABLE analytical_generations
RENAME TO analytical_generations_v6;

CREATE TABLE analytical_generations (
    generation_sequence INTEGER PRIMARY KEY,
    dataset_id TEXT NOT NULL CHECK (
        length(CAST(dataset_id AS BLOB)) BETWEEN 1 AND 256
    ),
    manifest_version INTEGER NOT NULL CHECK (manifest_version > 0),
    content_hash BLOB NOT NULL CHECK (length(content_hash) = 32),
    lineage_hash BLOB NOT NULL CHECK (length(lineage_hash) = 32),
    row_count INTEGER NOT NULL CHECK (row_count > 0),
    total_bytes INTEGER NOT NULL CHECK (total_bytes > 0),
    schema_name TEXT NOT NULL CHECK (
        length(CAST(schema_name AS BLOB)) BETWEEN 1 AND 128
        AND schema_name NOT GLOB '*[^a-z0-9._]*'
    ),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    schema_fingerprint BLOB NOT NULL CHECK (length(schema_fingerprint) = 32),
    anchor_manifest_id TEXT NOT NULL UNIQUE REFERENCES dataset_manifests(manifest_id),
    generation_kind TEXT NOT NULL CHECK (
        generation_kind IN ('ingest', 'compaction', 'derived')
    ),
    parent_count INTEGER NOT NULL DEFAULT 0 CHECK (parent_count BETWEEN 0 AND 256),
    build_spec_digest BLOB CHECK (
        build_spec_digest IS NULL
        OR (
            length(build_spec_digest) = 32
            AND build_spec_digest <>
                X'0000000000000000000000000000000000000000000000000000000000000000'
        )
    ),
    created_at_ns INTEGER NOT NULL,
    UNIQUE (dataset_id, manifest_version),
    UNIQUE (dataset_id, content_hash),
    UNIQUE (
        dataset_id,
        manifest_version,
        schema_name,
        schema_version,
        schema_fingerprint,
        content_hash
    ),
    CHECK (
        (
            generation_kind = 'ingest'
            AND build_spec_digest IS NULL
            AND (
                (manifest_version = 1 AND parent_count = 0)
                OR (manifest_version > 1 AND parent_count = 1)
            )
        ) OR (
            generation_kind = 'compaction'
            AND manifest_version > 1
            AND parent_count = 1
            AND build_spec_digest IS NULL
        ) OR (
            generation_kind = 'derived'
            AND parent_count BETWEEN 1 AND 256
            AND build_spec_digest IS NOT NULL
        )
    )
) STRICT;

INSERT INTO analytical_generations (
    generation_sequence,
    dataset_id,
    manifest_version,
    content_hash,
    lineage_hash,
    row_count,
    total_bytes,
    schema_name,
    schema_version,
    schema_fingerprint,
    anchor_manifest_id,
    generation_kind,
    parent_count,
    build_spec_digest,
    created_at_ns
)
SELECT
    ROW_NUMBER() OVER (ORDER BY dataset_id, manifest_version),
    dataset_id,
    manifest_version,
    content_hash,
    lineage_hash,
    row_count,
    total_bytes,
    schema_name,
    schema_version,
    schema_fingerprint,
    anchor_manifest_id,
    generation_kind,
    CASE WHEN parent_version IS NULL THEN 0 ELSE 1 END,
    NULL,
    created_at_ns
FROM analytical_generations_v6
ORDER BY dataset_id, manifest_version;

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

INSERT INTO analytical_generation_objects
SELECT * FROM analytical_generation_objects_v6;

CREATE TABLE analytical_generation_parents (
    child_dataset_id TEXT NOT NULL,
    child_manifest_version INTEGER NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 255),
    relation TEXT NOT NULL CHECK (
        relation IN ('append_predecessor', 'compaction_predecessor', 'derived_input')
    ),
    parent_generation_sequence INTEGER NOT NULL,
    parent_dataset_id TEXT NOT NULL,
    parent_manifest_version INTEGER NOT NULL CHECK (parent_manifest_version > 0),
    parent_schema_name TEXT NOT NULL,
    parent_schema_version INTEGER NOT NULL CHECK (parent_schema_version > 0),
    parent_schema_fingerprint BLOB NOT NULL CHECK (length(parent_schema_fingerprint) = 32),
    parent_content_hash BLOB NOT NULL CHECK (length(parent_content_hash) = 32),
    PRIMARY KEY (child_dataset_id, child_manifest_version, ordinal),
    UNIQUE (
        child_dataset_id,
        child_manifest_version,
        parent_dataset_id,
        parent_manifest_version
    ),
    FOREIGN KEY (child_dataset_id, child_manifest_version)
        REFERENCES analytical_generations(dataset_id, manifest_version),
    FOREIGN KEY (parent_generation_sequence)
        REFERENCES analytical_generations(generation_sequence),
    FOREIGN KEY (
        parent_dataset_id,
        parent_manifest_version,
        parent_schema_name,
        parent_schema_version,
        parent_schema_fingerprint,
        parent_content_hash
    ) REFERENCES analytical_generations(
        dataset_id,
        manifest_version,
        schema_name,
        schema_version,
        schema_fingerprint,
        content_hash
    )
) STRICT, WITHOUT ROWID;

INSERT INTO analytical_generation_parents (
    child_dataset_id,
    child_manifest_version,
    ordinal,
    relation,
    parent_generation_sequence,
    parent_dataset_id,
    parent_manifest_version,
    parent_schema_name,
    parent_schema_version,
    parent_schema_fingerprint,
    parent_content_hash
)
SELECT
    child.dataset_id,
    child.manifest_version,
    0,
    CASE child.generation_kind
        WHEN 'ingest' THEN 'append_predecessor'
        WHEN 'compaction' THEN 'compaction_predecessor'
    END,
    parent.generation_sequence,
    parent.dataset_id,
    parent.manifest_version,
    parent.schema_name,
    parent.schema_version,
    parent.schema_fingerprint,
    parent.content_hash
FROM analytical_generations_v6 AS retained
JOIN analytical_generations AS child
  ON child.dataset_id = retained.dataset_id
 AND child.manifest_version = retained.manifest_version
JOIN analytical_generations AS parent
  ON parent.dataset_id = retained.dataset_id
 AND parent.manifest_version = retained.parent_version
WHERE retained.parent_version IS NOT NULL;

DROP TABLE analytical_generation_objects_v6;
DROP TABLE analytical_generations_v6;

CREATE INDEX analytical_generations_latest
ON analytical_generations(dataset_id, manifest_version DESC);

CREATE INDEX analytical_generation_objects_content
ON analytical_generation_objects(content_hash);

CREATE INDEX analytical_generation_parents_parent
ON analytical_generation_parents(parent_dataset_id, parent_manifest_version);

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
        AND NEW.schema_version = 1
        AND NEW.schema_fingerprint =
            X'81158728aafbfa7ef4e8b8469f1880e01c28e601a76beb60f64d7f9ac911d939'
    )
) BEGIN
    SELECT RAISE(ABORT, 'analytical generation schema identity is not registered');
END;

CREATE TRIGGER analytical_generation_parents_guarded_insert
BEFORE INSERT ON analytical_generation_parents
WHEN NOT EXISTS (
    SELECT 1
    FROM analytical_generations AS child
    JOIN analytical_generations AS parent
      ON parent.generation_sequence = NEW.parent_generation_sequence
     AND parent.dataset_id = NEW.parent_dataset_id
     AND parent.manifest_version = NEW.parent_manifest_version
     AND parent.schema_name = NEW.parent_schema_name
     AND parent.schema_version = NEW.parent_schema_version
     AND parent.schema_fingerprint = NEW.parent_schema_fingerprint
     AND parent.content_hash = NEW.parent_content_hash
    WHERE child.dataset_id = NEW.child_dataset_id
      AND child.manifest_version = NEW.child_manifest_version
      AND NEW.ordinal < child.parent_count
      AND parent.generation_sequence < child.generation_sequence
      AND (
          (
              child.generation_kind = 'ingest'
              AND NEW.relation = 'append_predecessor'
              AND NEW.ordinal = 0
              AND parent.dataset_id = child.dataset_id
              AND parent.manifest_version = child.manifest_version - 1
              AND parent.schema_name = child.schema_name
              AND parent.schema_version = child.schema_version
              AND parent.schema_fingerprint = child.schema_fingerprint
          ) OR (
              child.generation_kind = 'compaction'
              AND NEW.relation = 'compaction_predecessor'
              AND NEW.ordinal = 0
              AND parent.dataset_id = child.dataset_id
              AND parent.manifest_version = child.manifest_version - 1
              AND parent.schema_name = child.schema_name
              AND parent.schema_version = child.schema_version
              AND parent.schema_fingerprint = child.schema_fingerprint
          ) OR (
              child.generation_kind = 'derived'
              AND NEW.relation = 'derived_input'
          )
      )
) BEGIN
    SELECT RAISE(ABORT, 'analytical generation parent edge is invalid');
END;

CREATE TRIGGER analytical_generation_parents_canonical_insert
BEFORE INSERT ON analytical_generation_parents
WHEN NEW.ordinal > 0 AND NOT EXISTS (
    SELECT 1
    FROM analytical_generation_parents AS prior
    WHERE prior.child_dataset_id = NEW.child_dataset_id
      AND prior.child_manifest_version = NEW.child_manifest_version
      AND prior.ordinal = NEW.ordinal - 1
      AND (
          prior.parent_dataset_id,
          prior.parent_manifest_version,
          prior.parent_schema_name,
          prior.parent_schema_version,
          prior.parent_schema_fingerprint,
          prior.parent_content_hash
      ) < (
          NEW.parent_dataset_id,
          NEW.parent_manifest_version,
          NEW.parent_schema_name,
          NEW.parent_schema_version,
          NEW.parent_schema_fingerprint,
          NEW.parent_content_hash
      )
) BEGIN
    SELECT RAISE(ABORT, 'analytical generation parents are not canonical');
END;

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

CREATE TRIGGER analytical_generation_parents_immutable_update
BEFORE UPDATE ON analytical_generation_parents BEGIN
    SELECT RAISE(ABORT, 'analytical generation parents are immutable');
END;

CREATE TRIGGER analytical_generation_parents_immutable_delete
BEFORE DELETE ON analytical_generation_parents BEGIN
    SELECT RAISE(ABORT, 'analytical generation parents are immutable');
END;

CREATE TABLE analytical_generation_lineage_migration_guard (
    must_be_zero INTEGER NOT NULL CHECK (must_be_zero = 0)
) STRICT;

INSERT INTO analytical_generation_lineage_migration_guard(must_be_zero)
SELECT 1
WHERE EXISTS (
    SELECT 1
    FROM analytical_generations AS child
    LEFT JOIN analytical_generation_parents AS parent
      ON parent.child_dataset_id = child.dataset_id
     AND parent.child_manifest_version = child.manifest_version
    GROUP BY child.dataset_id, child.manifest_version, child.parent_count
    HAVING COUNT(parent.ordinal) <> child.parent_count
       OR MIN(parent.ordinal) <> CASE WHEN child.parent_count = 0 THEN NULL ELSE 0 END
       OR MAX(parent.ordinal) <> CASE
           WHEN child.parent_count = 0 THEN NULL
           ELSE child.parent_count - 1
       END
);

DROP TABLE analytical_generation_lineage_migration_guard;
