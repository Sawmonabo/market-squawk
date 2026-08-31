CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    sha256 BLOB NOT NULL CHECK (length(sha256) = 32),
    applied_at_ns INTEGER NOT NULL
) STRICT;

CREATE TABLE sources (
    source_id TEXT PRIMARY KEY CHECK (length(CAST(source_id AS BLOB)) BETWEEN 1 AND 128),
    current_revision_digest BLOB NOT NULL CHECK (length(current_revision_digest) = 32),
    current_registered_at_ns INTEGER NOT NULL,
    first_registered_at_ns INTEGER NOT NULL,
    FOREIGN KEY (source_id, current_revision_digest)
        REFERENCES source_revisions(source_id, revision_digest)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE source_revisions (
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    revision_digest BLOB NOT NULL CHECK (length(revision_digest) = 32),
    metadata_json TEXT NOT NULL CHECK (
        length(CAST(metadata_json AS BLOB)) BETWEEN 1 AND 1048576
        AND json_valid(metadata_json)
    ),
    registered_at_ns INTEGER NOT NULL,
    PRIMARY KEY (source_id, revision_digest)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER source_revisions_immutable_update
BEFORE UPDATE ON source_revisions BEGIN
    SELECT RAISE(ABORT, 'source revisions are immutable');
END;

CREATE TRIGGER source_revisions_immutable_delete
BEFORE DELETE ON source_revisions BEGIN
    SELECT RAISE(ABORT, 'source revisions are immutable');
END;

CREATE TABLE source_rights (
    rights_id BLOB PRIMARY KEY CHECK (length(rights_id) = 32),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    payload_algorithm INTEGER NOT NULL CHECK (payload_algorithm IN (1, 2)),
    payload_digest BLOB NOT NULL CHECK (length(payload_digest) = 32),
    retrieved_at_ns INTEGER NOT NULL,
    terms_url TEXT NOT NULL CHECK (length(CAST(terms_url AS BLOB)) BETWEEN 1 AND 2048),
    terms_algorithm INTEGER NOT NULL CHECK (terms_algorithm IN (1, 2)),
    terms_digest BLOB NOT NULL CHECK (length(terms_digest) = 32),
    authorization_algorithm INTEGER NOT NULL CHECK (authorization_algorithm IN (1, 2)),
    authorization_digest BLOB NOT NULL CHECK (length(authorization_digest) = 32),
    authorization_expires_at_ns INTEGER,
    operation_mask INTEGER NOT NULL CHECK (operation_mask > 0 AND operation_mask <= 63),
    admitted_at_ns INTEGER NOT NULL,
    CHECK (retrieved_at_ns <= admitted_at_ns),
    CHECK (
        authorization_expires_at_ns IS NULL
        OR admitted_at_ns < authorization_expires_at_ns
    )
) STRICT;

CREATE TRIGGER source_rights_immutable_update
BEFORE UPDATE ON source_rights BEGIN
    SELECT RAISE(ABORT, 'source rights are immutable');
END;

CREATE TRIGGER source_rights_immutable_delete
BEFORE DELETE ON source_rights BEGIN
    SELECT RAISE(ABORT, 'source rights are immutable');
END;

CREATE TABLE ingest_runs (
    run_id TEXT PRIMARY KEY CHECK (length(CAST(run_id AS BLOB)) = 36),
    idempotency_key TEXT NOT NULL CHECK (
        length(CAST(idempotency_key AS BLOB)) BETWEEN 1 AND 512
    ),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    payload_algorithm INTEGER NOT NULL CHECK (payload_algorithm IN (1, 2)),
    payload_digest BLOB NOT NULL CHECK (length(payload_digest) = 32),
    operation TEXT NOT NULL CHECK (
        operation IN ('retrieve', 'display', 'persist', 'cache', 'redistribute', 'train')
    ),
    rights_id BLOB NOT NULL REFERENCES source_rights(rights_id),
    state TEXT NOT NULL CHECK (state IN ('reserved', 'succeeded', 'failed')),
    requested_at_ns INTEGER NOT NULL,
    completed_at_ns INTEGER,
    CHECK (
        (state = 'reserved' AND completed_at_ns IS NULL)
        OR (
            state IN ('succeeded', 'failed')
            AND completed_at_ns IS NOT NULL
            AND completed_at_ns >= requested_at_ns
        )
    ),
    UNIQUE (source_id, operation, idempotency_key)
) STRICT;

CREATE TRIGGER ingest_runs_rights_admitted_before_request
BEFORE INSERT ON ingest_runs
WHEN NOT EXISTS (
    SELECT 1 FROM source_rights
    WHERE rights_id = NEW.rights_id AND admitted_at_ns <= NEW.requested_at_ns
)
BEGIN
    SELECT RAISE(ABORT, 'ingest run predates rights admission');
END;

CREATE TRIGGER ingest_runs_guarded_update
BEFORE UPDATE ON ingest_runs
WHEN OLD.state <> 'reserved'
    OR OLD.completed_at_ns IS NOT NULL
    OR NEW.run_id <> OLD.run_id
    OR NEW.idempotency_key <> OLD.idempotency_key
    OR NEW.source_id <> OLD.source_id
    OR NEW.payload_algorithm <> OLD.payload_algorithm
    OR NEW.payload_digest <> OLD.payload_digest
    OR NEW.operation <> OLD.operation
    OR NEW.rights_id <> OLD.rights_id
    OR NEW.requested_at_ns <> OLD.requested_at_ns
    OR NEW.state NOT IN ('succeeded', 'failed')
    OR NEW.completed_at_ns IS NULL
    OR NEW.completed_at_ns < OLD.requested_at_ns
BEGIN
    SELECT RAISE(ABORT, 'invalid ingest run transition');
END;

CREATE TRIGGER ingest_runs_immutable_delete
BEFORE DELETE ON ingest_runs BEGIN
    SELECT RAISE(ABORT, 'ingest runs are immutable');
END;

CREATE TABLE source_cursors (
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    cursor_name TEXT NOT NULL CHECK (
        length(CAST(cursor_name AS BLOB)) BETWEEN 1 AND 128
    ),
    cursor_value TEXT NOT NULL CHECK (
        length(CAST(cursor_value AS BLOB)) BETWEEN 1 AND 4096
    ),
    updated_at_ns INTEGER NOT NULL,
    PRIMARY KEY (source_id, cursor_name)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER source_cursors_monotonic_update
BEFORE UPDATE ON source_cursors
WHEN NEW.source_id <> OLD.source_id
    OR NEW.cursor_name <> OLD.cursor_name
    OR NEW.updated_at_ns < OLD.updated_at_ns
    OR (
        NEW.updated_at_ns = OLD.updated_at_ns
        AND NEW.cursor_value <> OLD.cursor_value
    )
BEGIN
    SELECT RAISE(ABORT, 'invalid source cursor transition');
END;

CREATE TRIGGER source_cursors_immutable_delete
BEFORE DELETE ON source_cursors BEGIN
    SELECT RAISE(ABORT, 'source cursors cannot be deleted');
END;

CREATE TABLE catalog_authority_clock (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    last_timestamp_ns INTEGER NOT NULL CHECK (last_timestamp_ns >= 0)
) STRICT;

INSERT INTO catalog_authority_clock(singleton, last_timestamp_ns) VALUES (1, 0);

CREATE TRIGGER catalog_authority_clock_guarded_update
BEFORE UPDATE ON catalog_authority_clock
WHEN NEW.singleton <> OLD.singleton OR NEW.last_timestamp_ns < OLD.last_timestamp_ns
BEGIN
    SELECT RAISE(ABORT, 'catalog authority clock cannot move backwards');
END;

CREATE TRIGGER catalog_authority_clock_immutable_delete
BEFORE DELETE ON catalog_authority_clock BEGIN
    SELECT RAISE(ABORT, 'catalog authority clock cannot be deleted');
END;

CREATE TABLE artifacts (
    artifact_id TEXT PRIMARY KEY CHECK (length(CAST(artifact_id AS BLOB)) = 36),
    run_id TEXT NOT NULL REFERENCES ingest_runs(run_id),
    publication_ordinal INTEGER NOT NULL CHECK (
        publication_ordinal BETWEEN 0 AND 1023
    ),
    relative_reference TEXT NOT NULL UNIQUE CHECK (
        length(CAST(relative_reference AS BLOB)) BETWEEN 1 AND 1024
    ),
    content_algorithm INTEGER NOT NULL CHECK (content_algorithm IN (1, 2)),
    content_digest BLOB NOT NULL CHECK (length(content_digest) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    created_at_ns INTEGER NOT NULL,
    UNIQUE (run_id, publication_ordinal),
    UNIQUE (artifact_id, run_id)
) STRICT;

CREATE TABLE dataset_manifests (
    manifest_id TEXT PRIMARY KEY CHECK (length(CAST(manifest_id AS BLOB)) = 36),
    run_id TEXT NOT NULL UNIQUE REFERENCES ingest_runs(run_id),
    dataset_name TEXT NOT NULL CHECK (
        length(CAST(dataset_name AS BLOB)) BETWEEN 1 AND 512
    ),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    artifact_id TEXT NOT NULL UNIQUE CHECK (length(CAST(artifact_id AS BLOB)) = 36),
    content_algorithm INTEGER NOT NULL CHECK (content_algorithm IN (1, 2)),
    content_digest BLOB NOT NULL CHECK (length(content_digest) = 32),
    created_at_ns INTEGER NOT NULL,
    UNIQUE (dataset_name, content_algorithm, content_digest),
    FOREIGN KEY (artifact_id, run_id) REFERENCES artifacts(artifact_id, run_id)
) STRICT;

CREATE TRIGGER artifacts_guarded_insert
BEFORE INSERT ON artifacts
WHEN NOT EXISTS (
    SELECT 1
    FROM ingest_runs AS run
    WHERE run.run_id = NEW.run_id
      AND run.state = 'reserved'
      AND NOT EXISTS (
          SELECT 1 FROM dataset_manifests AS manifest
          WHERE manifest.run_id = run.run_id
      )
      AND (SELECT COUNT(*) FROM artifacts AS retained
           WHERE retained.run_id = run.run_id) < 1024
      AND NEW.publication_ordinal = (
          SELECT COUNT(*) FROM artifacts AS retained
          WHERE retained.run_id = run.run_id
      )
)
BEGIN
    SELECT RAISE(ABORT, 'artifact publication ordinal is invalid');
END;

CREATE TRIGGER dataset_manifests_guarded_insert
BEFORE INSERT ON dataset_manifests
WHEN NOT EXISTS (
    SELECT 1
    FROM ingest_runs AS run
    JOIN artifacts AS anchor
      ON anchor.run_id = run.run_id
     AND anchor.artifact_id = NEW.artifact_id
    WHERE run.run_id = NEW.run_id
      AND run.state = 'reserved'
      AND (SELECT COUNT(*) FROM artifacts AS member
           WHERE member.run_id = run.run_id) BETWEEN 1 AND 1024
      AND anchor.publication_ordinal = (
          SELECT COUNT(*) - 1 FROM artifacts AS member
          WHERE member.run_id = run.run_id
      )
      AND (SELECT MIN(member.publication_ordinal) FROM artifacts AS member
           WHERE member.run_id = run.run_id) = 0
      AND (SELECT MAX(member.publication_ordinal) FROM artifacts AS member
           WHERE member.run_id = run.run_id) = (
          SELECT COUNT(*) - 1 FROM artifacts AS member
          WHERE member.run_id = run.run_id
      )
      AND NEW.created_at_ns >= (
          SELECT MAX(member.created_at_ns) FROM artifacts AS member
          WHERE member.run_id = run.run_id
      )
)
BEGIN
    SELECT RAISE(ABORT, 'dataset manifest does not close an exact artifact group');
END;

CREATE TRIGGER ingest_runs_publication_guarded_success
BEFORE UPDATE ON ingest_runs
WHEN NEW.state = 'succeeded'
 AND NEW.operation IN ('persist', 'cache')
 AND NOT EXISTS (
    SELECT 1
    FROM dataset_manifests AS manifest
    JOIN artifacts AS anchor
      ON anchor.artifact_id = manifest.artifact_id
     AND anchor.run_id = manifest.run_id
    WHERE manifest.run_id = NEW.run_id
      AND (SELECT COUNT(*) FROM artifacts AS member
           WHERE member.run_id = NEW.run_id) BETWEEN 1 AND 1024
      AND anchor.publication_ordinal = (
          SELECT COUNT(*) - 1 FROM artifacts AS member
          WHERE member.run_id = NEW.run_id
      )
      AND (SELECT MIN(member.publication_ordinal) FROM artifacts AS member
           WHERE member.run_id = NEW.run_id) = 0
      AND (SELECT MAX(member.publication_ordinal) FROM artifacts AS member
           WHERE member.run_id = NEW.run_id) = (
          SELECT COUNT(*) - 1 FROM artifacts AS member
          WHERE member.run_id = NEW.run_id
      )
)
BEGIN
    SELECT RAISE(ABORT, 'successful ingest run lacks a closed artifact group');
END;

CREATE TRIGGER artifacts_immutable_update
BEFORE UPDATE ON artifacts BEGIN
    SELECT RAISE(ABORT, 'artifacts are immutable');
END;

CREATE TRIGGER artifacts_immutable_delete
BEFORE DELETE ON artifacts BEGIN
    SELECT RAISE(ABORT, 'artifacts are immutable');
END;

CREATE TRIGGER dataset_manifests_immutable_update
BEFORE UPDATE ON dataset_manifests BEGIN
    SELECT RAISE(ABORT, 'dataset manifests are immutable');
END;

CREATE TRIGGER dataset_manifests_immutable_delete
BEFORE DELETE ON dataset_manifests BEGIN
    SELECT RAISE(ABORT, 'dataset manifests are immutable');
END;

CREATE TABLE audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL CHECK (
        length(CAST(event_type AS BLOB)) BETWEEN 1 AND 128
    ),
    subject_id TEXT NOT NULL CHECK (
        length(CAST(subject_id AS BLOB)) BETWEEN 1 AND 512
    ),
    details_digest BLOB NOT NULL CHECK (length(details_digest) = 32),
    occurred_at_ns INTEGER NOT NULL
) STRICT;

CREATE TRIGGER audit_events_immutable_update
BEFORE UPDATE ON audit_events BEGIN
    SELECT RAISE(ABORT, 'audit events are immutable');
END;

CREATE TRIGGER audit_events_immutable_delete
BEFORE DELETE ON audit_events BEGIN
    SELECT RAISE(ABORT, 'audit events are immutable');
END;
