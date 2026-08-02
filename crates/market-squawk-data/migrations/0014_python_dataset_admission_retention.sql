CREATE TABLE python_dataset_admission_retention (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    retained_rows INTEGER NOT NULL CHECK (
        retained_rows BETWEEN 0 AND 4096
    ),
    retained_descriptor_bytes INTEGER NOT NULL CHECK (
        retained_descriptor_bytes BETWEEN 0 AND 268435456
    )
) STRICT, WITHOUT ROWID;

INSERT INTO python_dataset_admission_retention
    (singleton, retained_rows, retained_descriptor_bytes)
SELECT
    1,
    COUNT(*),
    COALESCE(SUM(length(descriptor_json)), 0)
FROM python_dataset_admissions;

CREATE TRIGGER python_dataset_admission_retention_insert_guard
BEFORE INSERT ON python_dataset_admissions
WHEN NOT EXISTS (
    SELECT 1
    FROM python_dataset_admissions
    WHERE export_sha256 = NEW.export_sha256
       OR (
           dataset_id = NEW.dataset_id
           AND manifest_version = NEW.manifest_version
       )
)
AND (
    (
        SELECT retained_rows
        FROM python_dataset_admission_retention
        WHERE singleton = 1
    ) >= 4096
    OR length(NEW.descriptor_json) > 268435456 - (
        SELECT retained_descriptor_bytes
        FROM python_dataset_admission_retention
        WHERE singleton = 1
    )
) BEGIN
    SELECT RAISE(ABORT, 'Python dataset admission retention limit exceeded');
END;

CREATE TRIGGER python_dataset_admission_retention_insert
AFTER INSERT ON python_dataset_admissions BEGIN
    UPDATE python_dataset_admission_retention
    SET
        retained_rows = retained_rows + 1,
        retained_descriptor_bytes =
            retained_descriptor_bytes + length(NEW.descriptor_json)
    WHERE singleton = 1;
END;

CREATE TRIGGER python_dataset_admission_retention_singleton_insert
BEFORE INSERT ON python_dataset_admission_retention BEGIN
    SELECT RAISE(ABORT, 'Python dataset admission retention state is immutable');
END;

CREATE TRIGGER python_dataset_admission_retention_singleton_delete
BEFORE DELETE ON python_dataset_admission_retention BEGIN
    SELECT RAISE(ABORT, 'Python dataset admission retention state is immutable');
END;
