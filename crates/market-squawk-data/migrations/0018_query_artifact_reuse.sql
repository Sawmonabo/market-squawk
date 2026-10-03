DROP TRIGGER query_artifact_results_reservation_guard;
DROP TRIGGER query_artifact_reservations_guarded_update;
DROP TRIGGER query_artifact_results_immutable_update;
DROP TRIGGER query_artifact_results_immutable_delete;
DROP INDEX query_artifact_results_content;

ALTER TABLE query_artifact_results RENAME TO query_artifact_results_legacy;

CREATE TABLE query_artifact_results (
    reservation_id TEXT PRIMARY KEY
        REFERENCES query_artifact_reservations(reservation_id),
    artifact_id TEXT NOT NULL CHECK (length(CAST(artifact_id AS BLOB)) = 36),
    relative_reference TEXT NOT NULL CHECK (
        length(CAST(relative_reference AS BLOB)) BETWEEN 1 AND 1024
        AND substr(relative_reference, 1, 1) <> '/'
        AND instr(relative_reference, '\') = 0
    ),
    content_algorithm INTEGER NOT NULL CHECK (content_algorithm = 1),
    content_digest BLOB NOT NULL CHECK (length(content_digest) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
    created_at_ns INTEGER NOT NULL,
    bound_at_ns INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

INSERT INTO query_artifact_results (
    reservation_id,
    artifact_id,
    relative_reference,
    content_algorithm,
    content_digest,
    size_bytes,
    created_at_ns,
    bound_at_ns
)
SELECT
    reservation_id,
    artifact_id,
    relative_reference,
    content_algorithm,
    content_digest,
    size_bytes,
    created_at_ns,
    bound_at_ns
FROM query_artifact_results_legacy;

DROP TABLE query_artifact_results_legacy;

CREATE INDEX query_artifact_results_content
ON query_artifact_results(content_digest, artifact_id);

CREATE INDEX query_artifact_results_artifact
ON query_artifact_results(artifact_id, reservation_id);

CREATE TRIGGER query_artifact_results_reservation_guard
BEFORE INSERT ON query_artifact_results
WHEN NOT EXISTS (
    SELECT 1 FROM query_artifact_reservations
    WHERE reservation_id = NEW.reservation_id
      AND state = 'reserved'
      AND bound_at_ns IS NULL
      AND NEW.size_bytes <= max_bytes
      AND NEW.created_at_ns >= requested_at_ns
      AND NEW.bound_at_ns >= NEW.created_at_ns
      AND NEW.bound_at_ns < expires_at_ns
)
BEGIN
    SELECT RAISE(ABORT, 'query artifact result lacks a live matching reservation');
END;

CREATE TRIGGER query_artifact_reservations_guarded_update
BEFORE UPDATE ON query_artifact_reservations
WHEN OLD.state <> 'reserved'
    OR OLD.bound_at_ns IS NOT NULL
    OR NEW.reservation_id <> OLD.reservation_id
    OR NEW.owner <> OLD.owner
    OR NEW.request_algorithm <> OLD.request_algorithm
    OR NEW.request_digest <> OLD.request_digest
    OR NEW.max_bytes <> OLD.max_bytes
    OR NEW.requested_at_ns <> OLD.requested_at_ns
    OR NEW.expires_at_ns <> OLD.expires_at_ns
    OR NEW.state <> 'published'
    OR NEW.bound_at_ns IS NULL
    OR NEW.bound_at_ns < OLD.requested_at_ns
    OR NEW.bound_at_ns >= OLD.expires_at_ns
    OR NOT EXISTS (
        SELECT 1 FROM query_artifact_results
        WHERE reservation_id = OLD.reservation_id
          AND bound_at_ns = NEW.bound_at_ns
    )
BEGIN
    SELECT RAISE(ABORT, 'query artifact reservation transition is invalid');
END;

CREATE TRIGGER query_artifact_results_immutable_update
BEFORE UPDATE ON query_artifact_results BEGIN
    SELECT RAISE(ABORT, 'query artifact results are immutable');
END;

CREATE TRIGGER query_artifact_results_immutable_delete
BEFORE DELETE ON query_artifact_results BEGIN
    SELECT RAISE(ABORT, 'query artifact results are immutable');
END;
