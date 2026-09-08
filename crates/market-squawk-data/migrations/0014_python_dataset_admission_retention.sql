CREATE TABLE feature_dataset_production_admission_retention (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    retained_rows INTEGER NOT NULL CHECK (
        retained_rows BETWEEN 0 AND 4096
    ),
    retained_payload_bytes INTEGER NOT NULL CHECK (
        retained_payload_bytes BETWEEN 0 AND 268435456
    )
) STRICT, WITHOUT ROWID;

INSERT INTO feature_dataset_production_admission_retention
    (singleton, retained_rows, retained_payload_bytes)
SELECT
    1,
    COUNT(*),
    COALESCE(SUM(length(descriptor_json) + length(receipt_json)), 0)
FROM feature_dataset_production_admissions;

CREATE TRIGGER feature_dataset_production_admission_retention_insert_guard
BEFORE INSERT ON feature_dataset_production_admissions
WHEN NOT EXISTS (
    SELECT 1
    FROM feature_dataset_production_admissions
    WHERE production_identity_sha256 = NEW.production_identity_sha256
       OR export_sha256 = NEW.export_sha256
       OR (
           dataset_id = NEW.dataset_id
           AND manifest_version = NEW.manifest_version
       )
)
AND (
    (
        SELECT retained_rows
        FROM feature_dataset_production_admission_retention
        WHERE singleton = 1
    ) >= 4096
    OR length(NEW.descriptor_json) + length(NEW.receipt_json) > 268435456 - (
        SELECT retained_payload_bytes
        FROM feature_dataset_production_admission_retention
        WHERE singleton = 1
    )
) BEGIN
    SELECT RAISE(ABORT, 'feature-dataset production admission retention limit exceeded');
END;

CREATE TRIGGER feature_dataset_production_admission_retention_insert
AFTER INSERT ON feature_dataset_production_admissions BEGIN
    UPDATE feature_dataset_production_admission_retention
    SET
        retained_rows = retained_rows + 1,
        retained_payload_bytes =
            retained_payload_bytes + length(NEW.descriptor_json) + length(NEW.receipt_json)
    WHERE singleton = 1;
END;

CREATE TRIGGER feature_dataset_production_admission_retention_singleton_insert
BEFORE INSERT ON feature_dataset_production_admission_retention BEGIN
    SELECT RAISE(ABORT, 'feature-dataset production admission retention state is immutable');
END;

CREATE TRIGGER feature_dataset_production_admission_retention_singleton_delete
BEFORE DELETE ON feature_dataset_production_admission_retention BEGIN
    SELECT RAISE(ABORT, 'feature-dataset production admission retention state is immutable');
END;
