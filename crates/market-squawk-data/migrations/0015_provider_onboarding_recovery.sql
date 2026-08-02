DROP TRIGGER provider_onboarding_events_contiguous_insert;
DROP TRIGGER provider_onboarding_events_immutable_update;
DROP TRIGGER provider_onboarding_events_immutable_delete;

ALTER TABLE provider_onboarding_events
RENAME TO provider_onboarding_events_before_recovery;

CREATE TABLE provider_onboarding_events (
    session_id TEXT NOT NULL REFERENCES provider_onboarding_sessions(session_id),
    sequence INTEGER NOT NULL CHECK (sequence BETWEEN 1 AND 1024),
    event_kind TEXT NOT NULL CHECK (
        event_kind IN (
            'credential_imported',
            'secret_store_planned',
            'secret_store_reconciliation_required',
            'secret_store_cleared',
            'protocol_validated',
            'credential_stored',
            'authority_verified',
            'rights_admitted',
            'rate_policy_admitted',
            'runtime_verified',
            'activate',
            'renewal_required',
            'begin_rotation',
            'candidate_cancelled_no_effect',
            'cutover',
            'remote_revocation',
            'local_deletion',
            'retire',
            'tombstone',
            'refresh_required',
            'unavailable',
            'indeterminate_remote_state',
            'cleanup_required',
            'blocked',
            'cancelled'
        )
    ),
    event_sha256 BLOB NOT NULL CHECK (
        length(event_sha256) = 32
        AND event_sha256 <> zeroblob(32)
    ),
    event_json BLOB NOT NULL CHECK (
        length(event_json) BETWEEN 1 AND 65536
        AND json_valid(CAST(event_json AS TEXT))
    ),
    resulting_state TEXT NOT NULL CHECK (
        resulting_state IN (
            'unavailable',
            'anonymous_available',
            'user_action_required',
            'credential_imported_unverified',
            'protocol_validated',
            'stored_unverified',
            'secret_reconciliation_required',
            'verified_least_privilege',
            'rights_admission_pending',
            'runtime_verification_pending',
            'active_scoped',
            'renewal_required',
            'refresh_required',
            'rotation_pending',
            'revocation_unconfirmed',
            'indeterminate_remote_state',
            'cleanup_required',
            'blocked'
        )
    ),
    credential_generation INTEGER CHECK (credential_generation > 0),
    prior_generation INTEGER CHECK (prior_generation > 0),
    occurred_at_ns INTEGER NOT NULL,
    audit_sequence INTEGER NOT NULL UNIQUE REFERENCES audit_events(sequence),
    PRIMARY KEY (session_id, sequence)
) STRICT, WITHOUT ROWID;

INSERT INTO provider_onboarding_events (
    session_id,
    sequence,
    event_kind,
    event_sha256,
    event_json,
    resulting_state,
    credential_generation,
    prior_generation,
    occurred_at_ns,
    audit_sequence
)
SELECT
    session_id,
    sequence,
    event_kind,
    event_sha256,
    event_json,
    resulting_state,
    credential_generation,
    prior_generation,
    occurred_at_ns,
    audit_sequence
FROM provider_onboarding_events_before_recovery;

DROP TABLE provider_onboarding_events_before_recovery;

CREATE TRIGGER provider_onboarding_events_contiguous_insert
BEFORE INSERT ON provider_onboarding_events
WHEN NEW.sequence <> COALESCE(
    (
        SELECT MAX(sequence) + 1
        FROM provider_onboarding_events
        WHERE session_id = NEW.session_id
    ),
    1
)
OR NEW.occurred_at_ns < (
    SELECT created_at_ns
    FROM provider_onboarding_sessions
    WHERE session_id = NEW.session_id
)
BEGIN
    SELECT RAISE(ABORT, 'provider onboarding event is not contiguous');
END;

CREATE TRIGGER provider_onboarding_events_immutable_update
BEFORE UPDATE ON provider_onboarding_events
BEGIN
    SELECT RAISE(ABORT, 'provider onboarding events are immutable');
END;

CREATE TRIGGER provider_onboarding_events_immutable_delete
BEFORE DELETE ON provider_onboarding_events
BEGIN
    SELECT RAISE(ABORT, 'provider onboarding events are immutable');
END;
