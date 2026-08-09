CREATE TABLE market_data_instrument_identities (
    instrument_id TEXT PRIMARY KEY CHECK (
        length(CAST(instrument_id AS BLOB)) = 36
    ),
    permanent_figi TEXT NOT NULL UNIQUE CHECK (
        length(CAST(permanent_figi AS BLOB)) = 12
        AND permanent_figi = upper(permanent_figi)
    ),
    created_at_ns INTEGER NOT NULL,
    UNIQUE (instrument_id, permanent_figi)
) STRICT, WITHOUT ROWID;

CREATE TABLE market_data_instrument_revisions (
    revision_digest BLOB PRIMARY KEY CHECK (
        length(revision_digest) = 32
        AND revision_digest <> zeroblob(32)
    ),
    instrument_id TEXT NOT NULL,
    permanent_figi TEXT NOT NULL,
    revision_sequence INTEGER NOT NULL CHECK (
        revision_sequence BETWEEN 1 AND 16384
    ),
    previous_revision_digest BLOB
        REFERENCES market_data_instrument_revisions(revision_digest),
    effective_start_ns INTEGER NOT NULL,
    effective_end_ns INTEGER,
    reference_revision TEXT NOT NULL CHECK (
        length(CAST(reference_revision AS BLOB)) BETWEEN 1 AND 512
    ),
    reference_algorithm INTEGER NOT NULL CHECK (reference_algorithm IN (1, 2)),
    reference_payload_digest BLOB NOT NULL CHECK (
        length(reference_payload_digest) = 32
        AND reference_payload_digest <> zeroblob(32)
    ),
    figi_source_id TEXT NOT NULL CHECK (
        length(CAST(figi_source_id AS BLOB)) BETWEEN 1 AND 128
    ),
    figi_source_algorithm INTEGER NOT NULL CHECK (figi_source_algorithm IN (1, 2)),
    figi_source_payload_digest BLOB NOT NULL CHECK (
        length(figi_source_payload_digest) = 32
        AND figi_source_payload_digest <> zeroblob(32)
    ),
    figi_source_timestamp_ns INTEGER,
    figi_observed_at_ns INTEGER NOT NULL,
    figi_rights_policy_id TEXT NOT NULL CHECK (
        length(CAST(figi_rights_policy_id AS BLOB)) BETWEEN 1 AND 512
    ),
    figi_entitlement TEXT NOT NULL CHECK (figi_entitlement = 'public_domain'),
    figi_terms_reference TEXT NOT NULL CHECK (
        length(CAST(figi_terms_reference AS BLOB)) BETWEEN 1 AND 512
    ),
    definition_json TEXT NOT NULL CHECK (
        length(CAST(definition_json AS BLOB)) BETWEEN 2 AND 16777216
    ),
    published_at_ns INTEGER NOT NULL,
    UNIQUE (instrument_id, revision_sequence),
    UNIQUE (instrument_id, effective_start_ns),
    UNIQUE (permanent_figi, effective_start_ns),
    UNIQUE (revision_digest, instrument_id, permanent_figi),
    FOREIGN KEY (instrument_id, permanent_figi)
        REFERENCES market_data_instrument_identities(instrument_id, permanent_figi),
    CHECK (effective_end_ns IS NULL OR effective_end_ns > effective_start_ns),
    CHECK (
        (revision_sequence = 1 AND previous_revision_digest IS NULL)
        OR (revision_sequence > 1 AND previous_revision_digest IS NOT NULL)
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE market_data_instrument_current (
    instrument_id TEXT PRIMARY KEY,
    permanent_figi TEXT NOT NULL UNIQUE,
    revision_digest BLOB NOT NULL UNIQUE,
    advanced_at_ns INTEGER NOT NULL,
    FOREIGN KEY (instrument_id, permanent_figi)
        REFERENCES market_data_instrument_identities(instrument_id, permanent_figi),
    FOREIGN KEY (revision_digest, instrument_id, permanent_figi)
        REFERENCES market_data_instrument_revisions(
            revision_digest,
            instrument_id,
            permanent_figi
        )
) STRICT, WITHOUT ROWID;

CREATE TABLE market_data_instrument_search_terms (
    revision_digest BLOB NOT NULL
        REFERENCES market_data_instrument_revisions(revision_digest),
    term_kind TEXT NOT NULL CHECK (
        term_kind IN ('figi', 'display_name', 'venue_symbol', 'provider_symbol')
    ),
    term_ordinal INTEGER NOT NULL CHECK (term_ordinal BETWEEN 0 AND 255),
    normalized_term TEXT NOT NULL CHECK (
        length(CAST(normalized_term AS BLOB)) BETWEEN 1 AND 512
    ),
    display_term TEXT NOT NULL CHECK (
        length(CAST(display_term AS BLOB)) BETWEEN 1 AND 512
    ),
    PRIMARY KEY (revision_digest, term_kind, term_ordinal),
    UNIQUE (revision_digest, term_kind, normalized_term, display_term)
) STRICT, WITHOUT ROWID;

CREATE INDEX market_data_instrument_search_lookup
ON market_data_instrument_search_terms(normalized_term, term_kind, display_term);

CREATE TRIGGER market_data_instrument_revisions_contiguous_insert
BEFORE INSERT ON market_data_instrument_revisions
WHEN NEW.revision_sequence <> COALESCE(
        (
            SELECT revisions.revision_sequence + 1
            FROM market_data_instrument_current AS current_
            JOIN market_data_instrument_revisions AS revisions
              ON revisions.revision_digest = current_.revision_digest
            WHERE current_.instrument_id = NEW.instrument_id
        ),
        1
    )
    OR NEW.previous_revision_digest IS NOT (
        SELECT revision_digest
        FROM market_data_instrument_current
        WHERE instrument_id = NEW.instrument_id
    )
BEGIN
    SELECT RAISE(ABORT, 'market-data definition is not a contiguous successor');
END;

CREATE TRIGGER market_data_instrument_current_successor_update
BEFORE UPDATE ON market_data_instrument_current
WHEN NEW.instrument_id <> OLD.instrument_id
    OR NEW.permanent_figi <> OLD.permanent_figi
    OR NEW.revision_digest = OLD.revision_digest
    OR NEW.advanced_at_ns < OLD.advanced_at_ns
    OR NOT EXISTS (
        SELECT 1
        FROM market_data_instrument_revisions AS successor
        JOIN market_data_instrument_revisions AS predecessor
          ON predecessor.revision_digest = OLD.revision_digest
        WHERE successor.revision_digest = NEW.revision_digest
          AND successor.instrument_id = OLD.instrument_id
          AND successor.permanent_figi = OLD.permanent_figi
          AND successor.previous_revision_digest = OLD.revision_digest
          AND successor.revision_sequence = predecessor.revision_sequence + 1
          AND successor.effective_start_ns > predecessor.effective_start_ns
    )
BEGIN
    SELECT RAISE(ABORT, 'invalid market-data current-definition successor');
END;

CREATE TRIGGER market_data_instrument_identities_immutable_update
BEFORE UPDATE ON market_data_instrument_identities BEGIN
    SELECT RAISE(ABORT, 'market-data instrument identities are immutable');
END;

CREATE TRIGGER market_data_instrument_identities_immutable_delete
BEFORE DELETE ON market_data_instrument_identities BEGIN
    SELECT RAISE(ABORT, 'market-data instrument identities are immutable');
END;

CREATE TRIGGER market_data_instrument_revisions_immutable_update
BEFORE UPDATE ON market_data_instrument_revisions BEGIN
    SELECT RAISE(ABORT, 'market-data instrument revisions are immutable');
END;

CREATE TRIGGER market_data_instrument_revisions_immutable_delete
BEFORE DELETE ON market_data_instrument_revisions BEGIN
    SELECT RAISE(ABORT, 'market-data instrument revisions are immutable');
END;

CREATE TRIGGER market_data_instrument_search_terms_immutable_update
BEFORE UPDATE ON market_data_instrument_search_terms BEGIN
    SELECT RAISE(ABORT, 'market-data instrument search terms are immutable');
END;

CREATE TRIGGER market_data_instrument_search_terms_immutable_delete
BEFORE DELETE ON market_data_instrument_search_terms BEGIN
    SELECT RAISE(ABORT, 'market-data instrument search terms are immutable');
END;

CREATE TRIGGER market_data_instrument_current_no_delete
BEFORE DELETE ON market_data_instrument_current BEGIN
    SELECT RAISE(ABORT, 'market-data current-definition pointers cannot be deleted');
END;
