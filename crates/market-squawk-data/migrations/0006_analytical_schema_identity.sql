CREATE TABLE analytical_schema_identity_migration_guard (
    must_be_zero INTEGER NOT NULL CHECK (must_be_zero = 0)
) STRICT;

INSERT INTO analytical_schema_identity_migration_guard(must_be_zero)
SELECT 1
WHERE EXISTS (
    SELECT 1 FROM analytical_generations WHERE schema_version <> 3
);

DROP TABLE analytical_schema_identity_migration_guard;

ALTER TABLE analytical_generations ADD COLUMN schema_name TEXT NOT NULL
    DEFAULT 'market_squawk.research_observations'
    CHECK (
        length(CAST(schema_name AS BLOB)) BETWEEN 1 AND 128
        AND schema_name NOT GLOB '*[^a-z0-9._]*'
    );

ALTER TABLE analytical_generations ADD COLUMN schema_fingerprint BLOB NOT NULL
    DEFAULT X'8d7dd570339626df758de3aab0bf49677551ec31491cd20e3eadd3748232c6c8'
    CHECK (length(schema_fingerprint) = 32);

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

CREATE TRIGGER analytical_generations_parent_schema_insert
BEFORE INSERT ON analytical_generations
WHEN NEW.parent_version IS NOT NULL AND NOT EXISTS (
    SELECT 1
    FROM analytical_generations AS parent
    WHERE parent.dataset_id = NEW.dataset_id
      AND parent.manifest_version = NEW.parent_version
      AND parent.schema_name = NEW.schema_name
      AND parent.schema_version = NEW.schema_version
      AND parent.schema_fingerprint = NEW.schema_fingerprint
) BEGIN
    SELECT RAISE(ABORT, 'analytical generation parent schema identity conflicts');
END;
