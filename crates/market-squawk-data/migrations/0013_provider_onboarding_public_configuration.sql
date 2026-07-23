ALTER TABLE provider_onboarding_sessions
ADD COLUMN reservation_schema_version INTEGER NOT NULL DEFAULT 1
CHECK (reservation_schema_version IN (1, 2));

ALTER TABLE provider_onboarding_sessions
ADD COLUMN public_configuration_sha256 BLOB NOT NULL
DEFAULT X'44136FA355B3678A1146AD16F7E8649E94FB4FC21FE77E8310C060F61CAAFF8A'
CHECK (
    length(public_configuration_sha256) = 32
    AND public_configuration_sha256 <> zeroblob(32)
);

ALTER TABLE provider_onboarding_sessions
ADD COLUMN public_configuration_json BLOB NOT NULL DEFAULT X'7B7D'
CHECK (
    length(public_configuration_json) BETWEEN 2 AND 4096
    AND json_valid(CAST(public_configuration_json AS TEXT))
    AND json_type(CAST(public_configuration_json AS TEXT)) = 'object'
);

CREATE INDEX provider_onboarding_sessions_newest_idx
ON provider_onboarding_sessions(created_at_ns DESC, surface_id, session_id);

CREATE INDEX provider_onboarding_sessions_surface_latest_idx
ON provider_onboarding_sessions(surface_id, created_at_ns DESC, session_id DESC);
