CREATE TABLE instruments (
    instrument_id TEXT PRIMARY KEY CHECK (length(CAST(instrument_id AS BLOB)) = 36),
    current_revision_digest BLOB NOT NULL CHECK (length(current_revision_digest) = 32),
    current_observed_at_ns INTEGER NOT NULL,
    first_observed_at_ns INTEGER NOT NULL,
    FOREIGN KEY (instrument_id, current_revision_digest)
        REFERENCES instrument_revisions(instrument_id, revision_digest)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE instrument_revisions (
    instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    revision_digest BLOB NOT NULL CHECK (length(revision_digest) = 32),
    definition_json TEXT NOT NULL CHECK (
        length(CAST(definition_json AS BLOB)) BETWEEN 1 AND 1048576
        AND json_valid(definition_json)
    ),
    observed_at_ns INTEGER NOT NULL,
    PRIMARY KEY (instrument_id, revision_digest)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER instrument_revisions_immutable_update
BEFORE UPDATE ON instrument_revisions BEGIN
    SELECT RAISE(ABORT, 'instrument revisions are immutable');
END;

CREATE TRIGGER instrument_revisions_immutable_delete
BEFORE DELETE ON instrument_revisions BEGIN
    SELECT RAISE(ABORT, 'instrument revisions are immutable');
END;

CREATE TABLE venues (
    venue_id TEXT PRIMARY KEY CHECK (length(CAST(venue_id AS BLOB)) BETWEEN 1 AND 64),
    first_observed_at_ns INTEGER NOT NULL
) STRICT;

CREATE TABLE instrument_identifiers (
    instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    identifier_json TEXT NOT NULL CHECK (
        length(CAST(identifier_json AS BLOB)) BETWEEN 1 AND 1048576
        AND json_valid(identifier_json)
    ),
    identifier_digest BLOB NOT NULL CHECK (length(identifier_digest) = 32),
    PRIMARY KEY (instrument_id, identifier_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_instrument_ids (
    instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    provider_instrument_id TEXT NOT NULL CHECK (
        length(CAST(provider_instrument_id AS BLOB)) BETWEEN 1 AND 256
    ),
    record_json TEXT NOT NULL CHECK (
        length(CAST(record_json AS BLOB)) BETWEEN 1 AND 1048576
        AND json_valid(record_json)
    ),
    record_digest BLOB NOT NULL CHECK (length(record_digest) = 32),
    PRIMARY KEY (source_id, provider_instrument_id, record_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE symbol_history (
    instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    venue_id TEXT NOT NULL REFERENCES venues(venue_id),
    venue_symbol TEXT NOT NULL CHECK (
        length(CAST(venue_symbol AS BLOB)) BETWEEN 1 AND 128
    ),
    starts_at_ns INTEGER NOT NULL,
    ends_at_ns INTEGER,
    record_json TEXT NOT NULL CHECK (
        length(CAST(record_json AS BLOB)) BETWEEN 1 AND 1048576
        AND json_valid(record_json)
    ),
    record_digest BLOB NOT NULL CHECK (length(record_digest) = 32),
    CHECK (ends_at_ns IS NULL OR ends_at_ns > starts_at_ns),
    PRIMARY KEY (instrument_id, venue_id, venue_symbol, starts_at_ns)
) STRICT, WITHOUT ROWID;

CREATE TABLE lifecycle_transitions (
    instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    effective_at_ns INTEGER NOT NULL,
    transition_json TEXT NOT NULL CHECK (
        length(CAST(transition_json AS BLOB)) BETWEEN 1 AND 1048576
        AND json_valid(transition_json)
    ),
    transition_digest BLOB NOT NULL CHECK (length(transition_digest) = 32),
    PRIMARY KEY (instrument_id, effective_at_ns)
) STRICT, WITHOUT ROWID;

CREATE TABLE contract_rolls (
    from_instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    to_instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    effective_at_ns INTEGER NOT NULL,
    roll_json TEXT NOT NULL CHECK (
        length(CAST(roll_json AS BLOB)) BETWEEN 1 AND 1048576
        AND json_valid(roll_json)
    ),
    roll_digest BLOB NOT NULL CHECK (length(roll_digest) = 32),
    CHECK (from_instrument_id <> to_instrument_id),
    PRIMARY KEY (from_instrument_id, to_instrument_id, effective_at_ns)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER venues_immutable_update
BEFORE UPDATE ON venues BEGIN
    SELECT RAISE(ABORT, 'venues are immutable');
END;

CREATE TRIGGER venues_immutable_delete
BEFORE DELETE ON venues BEGIN
    SELECT RAISE(ABORT, 'venues are immutable');
END;

CREATE TRIGGER instrument_identifiers_immutable_update
BEFORE UPDATE ON instrument_identifiers BEGIN
    SELECT RAISE(ABORT, 'instrument identifiers are immutable');
END;

CREATE TRIGGER instrument_identifiers_immutable_delete
BEFORE DELETE ON instrument_identifiers BEGIN
    SELECT RAISE(ABORT, 'instrument identifiers are immutable');
END;

CREATE TRIGGER provider_instrument_ids_immutable_update
BEFORE UPDATE ON provider_instrument_ids BEGIN
    SELECT RAISE(ABORT, 'provider instrument identities are immutable');
END;

CREATE TRIGGER provider_instrument_ids_immutable_delete
BEFORE DELETE ON provider_instrument_ids BEGIN
    SELECT RAISE(ABORT, 'provider instrument identities are immutable');
END;

CREATE TRIGGER symbol_history_immutable_update
BEFORE UPDATE ON symbol_history BEGIN
    SELECT RAISE(ABORT, 'symbol history is immutable');
END;

CREATE TRIGGER symbol_history_immutable_delete
BEFORE DELETE ON symbol_history BEGIN
    SELECT RAISE(ABORT, 'symbol history is immutable');
END;

CREATE TRIGGER lifecycle_transitions_immutable_update
BEFORE UPDATE ON lifecycle_transitions BEGIN
    SELECT RAISE(ABORT, 'lifecycle transitions are immutable');
END;

CREATE TRIGGER lifecycle_transitions_immutable_delete
BEFORE DELETE ON lifecycle_transitions BEGIN
    SELECT RAISE(ABORT, 'lifecycle transitions are immutable');
END;

CREATE TRIGGER contract_rolls_immutable_update
BEFORE UPDATE ON contract_rolls BEGIN
    SELECT RAISE(ABORT, 'contract rolls are immutable');
END;

CREATE TRIGGER contract_rolls_immutable_delete
BEFORE DELETE ON contract_rolls BEGIN
    SELECT RAISE(ABORT, 'contract rolls are immutable');
END;

CREATE TABLE corporate_actions (
    instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    effective_at_ns INTEGER NOT NULL,
    action_json TEXT NOT NULL CHECK (
        length(CAST(action_json AS BLOB)) BETWEEN 1 AND 1048576
        AND json_valid(action_json)
    ),
    action_digest BLOB NOT NULL CHECK (length(action_digest) = 32),
    PRIMARY KEY (instrument_id, source_id, action_digest)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER corporate_actions_immutable_update
BEFORE UPDATE ON corporate_actions BEGIN
    SELECT RAISE(ABORT, 'corporate actions are immutable');
END;

CREATE TRIGGER corporate_actions_immutable_delete
BEFORE DELETE ON corporate_actions BEGIN
    SELECT RAISE(ABORT, 'corporate actions are immutable');
END;
