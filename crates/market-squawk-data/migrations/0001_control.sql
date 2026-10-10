CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    sha256 BLOB NOT NULL CHECK (length(sha256) = 32),
    applied_at_ns INTEGER NOT NULL
) STRICT;

-- Durable model history is independent of the models currently active in memory.
CREATE TABLE model_inventory_series (
    model_id TEXT PRIMARY KEY,
    bundle_id TEXT UNIQUE NOT NULL
) STRICT;

CREATE TABLE model_inventory_records (
    sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
    model_id TEXT NOT NULL REFERENCES model_inventory_series(model_id),
    bundle_id TEXT NOT NULL REFERENCES model_inventory_series(bundle_id),
    bundle_version BLOB NOT NULL CHECK (length(bundle_version) = 8),
    candidate_directory TEXT UNIQUE NOT NULL,
    product_token TEXT UNIQUE NOT NULL,
    record BLOB NOT NULL,
    record_sha256 BLOB NOT NULL CHECK (length(record_sha256) = 32),
    chain_sha256 BLOB NOT NULL CHECK (length(chain_sha256) = 32),
    UNIQUE (bundle_id, bundle_version)
) STRICT;

CREATE INDEX model_inventory_by_model_version
ON model_inventory_records(model_id, bundle_version);

CREATE TRIGGER model_inventory_series_immutable_update
BEFORE UPDATE ON model_inventory_series BEGIN
    SELECT RAISE(ABORT, 'model inventory series are immutable');
END;
CREATE TRIGGER model_inventory_series_immutable_delete
BEFORE DELETE ON model_inventory_series BEGIN
    SELECT RAISE(ABORT, 'model inventory series are immutable');
END;
CREATE TRIGGER model_inventory_records_immutable_update
BEFORE UPDATE ON model_inventory_records BEGIN
    SELECT RAISE(ABORT, 'model inventory records are immutable');
END;
CREATE TRIGGER model_inventory_records_immutable_delete
BEFORE DELETE ON model_inventory_records BEGIN
    SELECT RAISE(ABORT, 'model inventory records are immutable');
END;

CREATE TABLE chart_projection_headers (
    source_sha256 BLOB PRIMARY KEY CHECK (length(source_sha256) = 32),
    projection_sha256 BLOB NOT NULL CHECK (length(projection_sha256) = 32),
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    series_count INTEGER NOT NULL CHECK (series_count > 0),
    first_time INTEGER,
    last_time INTEGER,
    metadata BLOB NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE chart_projection_rows (
    source_sha256 BLOB NOT NULL REFERENCES chart_projection_headers(source_sha256)
        DEFERRABLE INITIALLY DEFERRED,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    time_nanos INTEGER NOT NULL,
    payload BLOB NOT NULL,
    payload_sha256 BLOB NOT NULL CHECK (length(payload_sha256) = 32),
    PRIMARY KEY (source_sha256, ordinal)
) STRICT, WITHOUT ROWID;
CREATE INDEX chart_projection_time ON chart_projection_rows(source_sha256, time_nanos, ordinal);

CREATE TRIGGER chart_projection_headers_immutable_update
BEFORE UPDATE ON chart_projection_headers BEGIN
    SELECT RAISE(ABORT, 'chart projections are immutable');
END;
CREATE TRIGGER chart_projection_headers_immutable_delete
BEFORE DELETE ON chart_projection_headers BEGIN
    SELECT RAISE(ABORT, 'chart projections are immutable');
END;
CREATE TRIGGER chart_projection_rows_immutable_update
BEFORE UPDATE ON chart_projection_rows BEGIN
    SELECT RAISE(ABORT, 'chart projection rows are immutable');
END;
CREATE TRIGGER chart_projection_rows_immutable_delete
BEFORE DELETE ON chart_projection_rows BEGIN
    SELECT RAISE(ABORT, 'chart projection rows are immutable');
END;

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

CREATE TABLE forecast_inventory_vintages (
    sequence INTEGER PRIMARY KEY,
    vintage_id TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL UNIQUE,
    product_token TEXT NOT NULL UNIQUE,
    artifact_id TEXT NOT NULL UNIQUE,
    instrument_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    available_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    record BLOB NOT NULL,
    record_sha256 BLOB NOT NULL CHECK(length(record_sha256)=32)
) STRICT;
CREATE INDEX forecast_inventory_instrument_time ON forecast_inventory_vintages(instrument_id,created_at);
CREATE INDEX forecast_inventory_instrument_sequence ON forecast_inventory_vintages(instrument_id,sequence);
CREATE TABLE forecast_inventory_outcomes (
    sequence INTEGER PRIMARY KEY,
    outcome_id TEXT NOT NULL UNIQUE,
    vintage_id TEXT NOT NULL REFERENCES forecast_inventory_vintages(vintage_id),
    target_at INTEGER NOT NULL,
    record BLOB NOT NULL,
    record_sha256 BLOB NOT NULL CHECK(length(record_sha256)=32),
    UNIQUE(vintage_id,target_at)
) STRICT;
CREATE INDEX forecast_inventory_outcome_parent ON forecast_inventory_outcomes(vintage_id,sequence);
CREATE TRIGGER forecast_inventory_vintages_immutable_update BEFORE UPDATE ON forecast_inventory_vintages BEGIN SELECT RAISE(ABORT, 'forecast inventory is immutable'); END;
CREATE TRIGGER forecast_inventory_vintages_immutable_delete BEFORE DELETE ON forecast_inventory_vintages BEGIN SELECT RAISE(ABORT, 'forecast inventory is immutable'); END;
CREATE TRIGGER forecast_inventory_outcomes_immutable_update BEFORE UPDATE ON forecast_inventory_outcomes BEGIN SELECT RAISE(ABORT, 'forecast inventory is immutable'); END;
CREATE TRIGGER forecast_inventory_outcomes_immutable_delete BEFORE DELETE ON forecast_inventory_outcomes BEGIN SELECT RAISE(ABORT, 'forecast inventory is immutable'); END;

-- Completed calculations retain immutable artifact coordinates; Save adds a separate marker.
CREATE TABLE portfolio_planning_completions (
    sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
    calculation_token TEXT NOT NULL UNIQUE,
    account_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    snapshot_token TEXT NOT NULL,
    calculated_at_ns INTEGER NOT NULL,
    portfolio_effective_at_ns INTEGER NOT NULL,
    portfolio_available_at_ns INTEGER,
    artifact_id TEXT NOT NULL,
    artifact_sha256 BLOB NOT NULL CHECK (length(artifact_sha256) = 32),
    artifact_byte_length INTEGER NOT NULL CHECK (artifact_byte_length > 0),
    artifact_media_type TEXT NOT NULL,
    record_sha256 BLOB NOT NULL CHECK (length(record_sha256) = 32),
    chain_sha256 BLOB NOT NULL CHECK (length(chain_sha256) = 32),
    UNIQUE (calculation_token, account_id)
) STRICT;
CREATE TABLE portfolio_planning_saves (
    sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
    calculation_token TEXT NOT NULL UNIQUE,
    account_id TEXT NOT NULL,
    saved_at_ns INTEGER NOT NULL,
    record_sha256 BLOB NOT NULL CHECK (length(record_sha256) = 32),
    chain_sha256 BLOB NOT NULL CHECK (length(chain_sha256) = 32),
    FOREIGN KEY (calculation_token, account_id)
        REFERENCES portfolio_planning_completions(calculation_token, account_id)
) STRICT;
CREATE INDEX portfolio_planning_saves_account_sequence
ON portfolio_planning_saves(account_id, sequence);
CREATE TRIGGER portfolio_planning_completions_immutable_update
BEFORE UPDATE ON portfolio_planning_completions BEGIN
    SELECT RAISE(ABORT, 'portfolio planning completions are immutable');
END;
CREATE TRIGGER portfolio_planning_completions_immutable_delete
BEFORE DELETE ON portfolio_planning_completions BEGIN
    SELECT RAISE(ABORT, 'portfolio planning completions are immutable');
END;
CREATE TRIGGER portfolio_planning_saves_immutable_update
BEFORE UPDATE ON portfolio_planning_saves BEGIN
    SELECT RAISE(ABORT, 'portfolio planning saves are immutable');
END;
CREATE TRIGGER portfolio_planning_saves_immutable_delete
BEFORE DELETE ON portfolio_planning_saves BEGIN
    SELECT RAISE(ABORT, 'portfolio planning saves are immutable');
END;
