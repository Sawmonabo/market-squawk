CREATE TABLE provider_onboarding_stream_heads (
    session_id TEXT PRIMARY KEY
        REFERENCES provider_onboarding_sessions(session_id),
    stream_version INTEGER NOT NULL CHECK (stream_version = 1),
    event_count INTEGER NOT NULL CHECK (event_count BETWEEN 0 AND 1024),
    last_event_sequence INTEGER CHECK (
        last_event_sequence BETWEEN 1 AND 1024
    ),
    last_audit_sequence INTEGER REFERENCES audit_events(sequence),
    cumulative_sha256 BLOB NOT NULL CHECK (
        length(cumulative_sha256) = 32
        AND cumulative_sha256 <> zeroblob(32)
    ),
    CHECK (
        (
            event_count = 0
            AND last_event_sequence IS NULL
            AND last_audit_sequence IS NULL
        )
        OR (
            event_count > 0
            AND last_event_sequence = event_count
            AND last_audit_sequence IS NOT NULL
        )
    )
) STRICT, WITHOUT ROWID;

CREATE TRIGGER provider_onboarding_stream_heads_checked_update
BEFORE UPDATE ON provider_onboarding_stream_heads
WHEN NEW.session_id <> OLD.session_id
OR NEW.stream_version <> OLD.stream_version
OR NEW.event_count <> OLD.event_count + 1
OR NEW.last_event_sequence <> NEW.event_count
OR NEW.last_audit_sequence IS NULL
OR NEW.last_audit_sequence <= COALESCE(
    OLD.last_audit_sequence,
    (
        SELECT reservation_audit_sequence
        FROM provider_onboarding_sessions
        WHERE session_id = NEW.session_id
    )
)
OR NEW.cumulative_sha256 = OLD.cumulative_sha256
OR NOT EXISTS (
    SELECT 1
    FROM provider_onboarding_events AS event
    WHERE event.session_id = NEW.session_id
      AND event.sequence = NEW.last_event_sequence
      AND event.audit_sequence = NEW.last_audit_sequence
)
OR (
    SELECT COUNT(*)
    FROM provider_onboarding_events AS event
    WHERE event.session_id = NEW.session_id
) <> NEW.event_count
BEGIN
    SELECT RAISE(ABORT, 'provider onboarding stream head update is invalid');
END;

CREATE TRIGGER provider_onboarding_stream_heads_immutable_delete
BEFORE DELETE ON provider_onboarding_stream_heads
BEGIN
    SELECT RAISE(ABORT, 'provider onboarding stream heads cannot be deleted');
END;
