CREATE TABLE provider_capability_revisions (
    surface_id TEXT NOT NULL CHECK (
        length(CAST(surface_id AS BLOB)) BETWEEN 1 AND 128
    ),
    revision INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 256),
    capability_sha256 BLOB NOT NULL CHECK (
        length(capability_sha256) = 32
        AND capability_sha256 <> zeroblob(32)
    ),
    capability_json BLOB NOT NULL CHECK (
        length(capability_json) BETWEEN 1 AND 65536
        AND json_valid(CAST(capability_json AS TEXT))
    ),
    registered_at_ns INTEGER NOT NULL,
    PRIMARY KEY (surface_id, revision),
    UNIQUE (surface_id, revision, capability_sha256)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER provider_capability_revisions_contiguous_insert
BEFORE INSERT ON provider_capability_revisions
WHEN NEW.revision <> COALESCE(
    (
        SELECT MAX(revision) + 1
        FROM provider_capability_revisions
        WHERE surface_id = NEW.surface_id
    ),
    1
)
BEGIN
    SELECT RAISE(ABORT, 'provider capability revision is not contiguous');
END;

CREATE TRIGGER provider_capability_revisions_immutable_update
BEFORE UPDATE ON provider_capability_revisions
BEGIN
    SELECT RAISE(ABORT, 'provider capability revisions are immutable');
END;

CREATE TRIGGER provider_capability_revisions_immutable_delete
BEFORE DELETE ON provider_capability_revisions
BEGIN
    SELECT RAISE(ABORT, 'provider capability revisions are immutable');
END;

CREATE TABLE provider_onboarding_sessions (
    session_id TEXT PRIMARY KEY CHECK (length(CAST(session_id AS BLOB)) = 36),
    surface_id TEXT NOT NULL,
    capability_revision INTEGER NOT NULL CHECK (
        capability_revision BETWEEN 1 AND 256
    ),
    capability_sha256 BLOB NOT NULL CHECK (
        length(capability_sha256) = 32
        AND capability_sha256 <> zeroblob(32)
    ),
    setup_mode TEXT NOT NULL CHECK (
        setup_mode IN (
            'no_credential',
            'manual_api_key_import',
            'oauth_authorization_code_pkce',
            'oauth_device',
            'dynamic_client_registration'
        )
    ),
    actor_class TEXT NOT NULL CHECK (
        length(CAST(actor_class AS BLOB)) BETWEEN 1 AND 128
    ),
    operation_owner TEXT NOT NULL CHECK (
        length(CAST(operation_owner AS BLOB)) BETWEEN 1 AND 128
    ),
    requested_authority_sha256 BLOB NOT NULL CHECK (
        length(requested_authority_sha256) = 32
        AND requested_authority_sha256 <> zeroblob(32)
    ),
    requested_authority_json BLOB NOT NULL CHECK (
        length(requested_authority_json) BETWEEN 1 AND 16384
        AND json_valid(CAST(requested_authority_json AS TEXT))
    ),
    initial_state TEXT NOT NULL CHECK (
        initial_state IN (
            'unavailable',
            'anonymous_available',
            'user_action_required',
            'credential_imported_unverified',
            'protocol_validated',
            'stored_unverified',
            'verified_least_privilege',
            'rights_admission_pending',
            'runtime_verification_pending',
            'active_scoped',
            'refresh_required',
            'rotation_pending',
            'revocation_unconfirmed',
            'indeterminate_remote_state',
            'cleanup_required',
            'blocked'
        )
    ),
    deadline_at_ns INTEGER NOT NULL,
    retry_budget INTEGER NOT NULL CHECK (retry_budget BETWEEN 0 AND 8),
    created_at_ns INTEGER NOT NULL,
    reservation_audit_sequence INTEGER NOT NULL UNIQUE
        REFERENCES audit_events(sequence),
    CHECK (created_at_ns < deadline_at_ns),
    FOREIGN KEY (surface_id, capability_revision, capability_sha256)
        REFERENCES provider_capability_revisions(
            surface_id,
            revision,
            capability_sha256
        )
) STRICT, WITHOUT ROWID;

CREATE TRIGGER provider_onboarding_sessions_immutable_update
BEFORE UPDATE ON provider_onboarding_sessions
BEGIN
    SELECT RAISE(ABORT, 'provider onboarding sessions are immutable');
END;

CREATE TRIGGER provider_onboarding_sessions_immutable_delete
BEFORE DELETE ON provider_onboarding_sessions
BEGIN
    SELECT RAISE(ABORT, 'provider onboarding sessions are immutable');
END;

CREATE TABLE provider_onboarding_events (
    session_id TEXT NOT NULL REFERENCES provider_onboarding_sessions(session_id),
    sequence INTEGER NOT NULL CHECK (sequence BETWEEN 1 AND 1024),
    event_kind TEXT NOT NULL CHECK (
        event_kind IN (
            'credential_imported',
            'protocol_validated',
            'credential_stored',
            'authority_verified',
            'rights_admitted',
            'rate_policy_admitted',
            'runtime_verified',
            'activate',
            'begin_rotation',
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
            'verified_least_privilege',
            'rights_admission_pending',
            'runtime_verification_pending',
            'active_scoped',
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
