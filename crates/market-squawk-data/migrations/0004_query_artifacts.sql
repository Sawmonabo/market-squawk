CREATE TABLE query_artifact_reservations (
    reservation_id TEXT PRIMARY KEY CHECK (length(CAST(reservation_id AS BLOB)) = 36),
    owner TEXT NOT NULL CHECK (length(CAST(owner AS BLOB)) BETWEEN 1 AND 512),
    request_algorithm INTEGER NOT NULL CHECK (request_algorithm = 1),
    request_digest BLOB NOT NULL CHECK (length(request_digest) = 32),
    max_bytes INTEGER NOT NULL CHECK (max_bytes > 0),
    requested_at_ns INTEGER NOT NULL,
    expires_at_ns INTEGER NOT NULL CHECK (
        requested_at_ns < expires_at_ns
        AND expires_at_ns <= requested_at_ns + 86400000000000
    ),
    state TEXT NOT NULL CHECK (state IN ('reserved', 'published')),
    bound_at_ns INTEGER,
    CHECK (
        (state = 'reserved' AND bound_at_ns IS NULL)
        OR (state = 'published' AND bound_at_ns IS NOT NULL AND bound_at_ns >= requested_at_ns)
    )
) STRICT;

CREATE TABLE query_artifact_results (
    reservation_id TEXT PRIMARY KEY
        REFERENCES query_artifact_reservations(reservation_id),
    artifact_id TEXT NOT NULL UNIQUE CHECK (length(CAST(artifact_id AS BLOB)) = 36),
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

CREATE INDEX query_artifact_results_content
ON query_artifact_results(content_digest);

CREATE INDEX query_artifact_reservations_reachability
ON query_artifact_reservations(state, expires_at_ns);

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

CREATE TRIGGER query_artifact_reservations_immutable_delete
BEFORE DELETE ON query_artifact_reservations BEGIN
    SELECT RAISE(ABORT, 'query artifact reservations are immutable');
END;

CREATE TRIGGER query_artifact_results_immutable_update
BEFORE UPDATE ON query_artifact_results BEGIN
    SELECT RAISE(ABORT, 'query artifact results are immutable');
END;

CREATE TRIGGER query_artifact_results_immutable_delete
BEFORE DELETE ON query_artifact_results BEGIN
    SELECT RAISE(ABORT, 'query artifact results are immutable');
END;
