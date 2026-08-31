CREATE TABLE listing_reference_generations (
    generation_digest BLOB NOT NULL CHECK (
        length(generation_digest) = 32
        AND generation_digest <> zeroblob(32)
    ),
    dataset_id TEXT NOT NULL CHECK (
        length(CAST(dataset_id AS BLOB)) BETWEEN 1 AND 512
    ),
    generation_sequence INTEGER NOT NULL CHECK (
        generation_sequence BETWEEN 1 AND 16384
    ),
    previous_generation_digest BLOB REFERENCES listing_reference_generations(generation_digest),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    source_revision TEXT NOT NULL CHECK (
        length(CAST(source_revision AS BLOB)) BETWEEN 1 AND 512
    ),
    source_revision_digest BLOB NOT NULL CHECK (
        length(source_revision_digest) = 32
        AND source_revision_digest <> zeroblob(32)
    ),
    rights_id BLOB NOT NULL REFERENCES source_rights(rights_id),
    rights_state TEXT NOT NULL CHECK (rights_state = 'admitted_scoped'),
    file_count INTEGER NOT NULL CHECK (file_count = 2),
    record_count INTEGER NOT NULL CHECK (record_count BETWEEN 2 AND 65536),
    records_digest BLOB NOT NULL CHECK (
        length(records_digest) = 32
        AND records_digest <> zeroblob(32)
    ),
    published_at_ns INTEGER NOT NULL,
    PRIMARY KEY (generation_digest),
    UNIQUE (dataset_id, generation_sequence),
    FOREIGN KEY (source_id, source_revision_digest)
        REFERENCES source_revisions(source_id, revision_digest),
    CHECK (
        (generation_sequence = 1 AND previous_generation_digest IS NULL)
        OR (generation_sequence > 1 AND previous_generation_digest IS NOT NULL)
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE listing_reference_files (
    generation_digest BLOB NOT NULL
        REFERENCES listing_reference_generations(generation_digest),
    file_kind TEXT NOT NULL CHECK (file_kind IN ('nasdaq_listed', 'other_listed')),
    source_object_id TEXT NOT NULL CHECK (
        length(CAST(source_object_id AS BLOB)) BETWEEN 1 AND 512
    ),
    source_reference TEXT NOT NULL CHECK (
        length(CAST(source_reference AS BLOB)) BETWEEN 1 AND 512
    ),
    file_creation_time TEXT NOT NULL CHECK (
        length(CAST(file_creation_time AS BLOB)) = 13
    ),
    payload_algorithm INTEGER NOT NULL CHECK (payload_algorithm IN (1, 2)),
    payload_digest BLOB NOT NULL CHECK (length(payload_digest) = 32),
    payload_locator_reference TEXT CHECK (
        payload_locator_reference IS NULL
        OR length(CAST(payload_locator_reference AS BLOB)) BETWEEN 1 AND 512
    ),
    payload_locator_version TEXT CHECK (
        payload_locator_version IS NULL
        OR length(CAST(payload_locator_version AS BLOB)) BETWEEN 1 AND 512
    ),
    source_last_modified_at_ns INTEGER NOT NULL,
    received_at_ns INTEGER NOT NULL,
    available_at_ns INTEGER NOT NULL,
    ingested_at_ns INTEGER NOT NULL,
    record_count INTEGER NOT NULL CHECK (record_count BETWEEN 1 AND 32768),
    PRIMARY KEY (generation_digest, file_kind),
    UNIQUE (generation_digest, source_object_id),
    CHECK (source_last_modified_at_ns <= received_at_ns),
    CHECK (source_last_modified_at_ns <= available_at_ns),
    CHECK (available_at_ns >= received_at_ns),
    CHECK (ingested_at_ns >= received_at_ns),
    CHECK (ingested_at_ns >= available_at_ns),
    CHECK ((payload_locator_reference IS NULL) = (payload_locator_version IS NULL))
) STRICT, WITHOUT ROWID;

CREATE TABLE listing_reference_values (
    value_digest BLOB PRIMARY KEY CHECK (
        length(value_digest) = 32
        AND value_digest <> zeroblob(32)
    ),
    file_kind TEXT NOT NULL CHECK (file_kind IN ('nasdaq_listed', 'other_listed')),
    provider_symbol TEXT NOT NULL CHECK (
        length(CAST(provider_symbol AS BLOB)) BETWEEN 1 AND 14
    ),
    normalized_provider_symbol TEXT NOT NULL CHECK (
        length(CAST(normalized_provider_symbol AS BLOB)) BETWEEN 1 AND 14
    ),
    security_name TEXT NOT NULL CHECK (
        length(CAST(security_name AS BLOB)) BETWEEN 1 AND 255
    ),
    normalized_security_name TEXT NOT NULL CHECK (
        length(CAST(normalized_security_name AS BLOB)) BETWEEN 1 AND 255
    ),
    listing_venue TEXT NOT NULL CHECK (
        length(CAST(listing_venue AS BLOB)) BETWEEN 1 AND 64
    ),
    exchange_code TEXT CHECK (
        exchange_code IS NULL OR length(CAST(exchange_code AS BLOB)) = 1
    ),
    cqs_symbol TEXT CHECK (
        cqs_symbol IS NULL
        OR length(CAST(cqs_symbol AS BLOB)) BETWEEN 1 AND 14
    ),
    nasdaq_symbol TEXT CHECK (
        nasdaq_symbol IS NULL
        OR length(CAST(nasdaq_symbol AS BLOB)) BETWEEN 1 AND 14
    ),
    market_category TEXT CHECK (
        market_category IS NULL OR market_category IN ('Q', 'G', 'S')
    ),
    financial_status TEXT CHECK (
        financial_status IS NULL OR financial_status IN ('N', 'D', 'E', 'Q', 'G', 'H', 'J', 'K')
    ),
    is_etf INTEGER NOT NULL CHECK (is_etf IN (0, 1)),
    is_test_issue INTEGER NOT NULL CHECK (is_test_issue IN (0, 1)),
    round_lot_size INTEGER NOT NULL CHECK (round_lot_size BETWEEN 1 AND 999999),
    is_next_shares INTEGER CHECK (is_next_shares IS NULL OR is_next_shares IN (0, 1)),
    directory_presence TEXT NOT NULL CHECK (directory_presence = 'current_directory'),
    data_quality TEXT NOT NULL CHECK (data_quality = 'official_delayed'),
    authority_class TEXT NOT NULL CHECK (authority_class = 'reference_only'),
    CHECK (
        (
            file_kind = 'nasdaq_listed'
            AND listing_venue = 'XNAS'
            AND exchange_code IS NULL
            AND cqs_symbol IS NULL
            AND nasdaq_symbol IS NULL
            AND market_category IS NOT NULL
            AND financial_status IS NOT NULL
            AND is_next_shares IS NOT NULL
        )
        OR (
            file_kind = 'other_listed'
            AND exchange_code IN ('A', 'N', 'P', 'M', 'Z', 'V')
            AND cqs_symbol IS NOT NULL
            AND nasdaq_symbol IS NOT NULL
            AND market_category IS NULL
            AND financial_status IS NULL
            AND is_next_shares IS NULL
        )
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE listing_reference_memberships (
    generation_digest BLOB NOT NULL,
    file_kind TEXT NOT NULL,
    provider_row_number INTEGER NOT NULL CHECK (
        provider_row_number BETWEEN 2 AND 32769
    ),
    provider_symbol TEXT NOT NULL CHECK (
        length(CAST(provider_symbol AS BLOB)) BETWEEN 1 AND 14
    ),
    record_revision TEXT NOT NULL CHECK (
        length(CAST(record_revision AS BLOB)) BETWEEN 1 AND 512
    ),
    record_algorithm INTEGER NOT NULL CHECK (record_algorithm IN (1, 2)),
    record_payload_digest BLOB NOT NULL CHECK (length(record_payload_digest) = 32),
    record_locator_reference TEXT CHECK (
        record_locator_reference IS NULL
        OR length(CAST(record_locator_reference AS BLOB)) BETWEEN 1 AND 512
    ),
    record_locator_version TEXT CHECK (
        record_locator_version IS NULL
        OR length(CAST(record_locator_version AS BLOB)) BETWEEN 1 AND 512
    ),
    value_digest BLOB NOT NULL REFERENCES listing_reference_values(value_digest),
    record_digest BLOB NOT NULL CHECK (
        length(record_digest) = 32
        AND record_digest <> zeroblob(32)
    ),
    PRIMARY KEY (generation_digest, file_kind, provider_row_number),
    UNIQUE (generation_digest, file_kind, provider_symbol),
    UNIQUE (generation_digest, record_revision),
    UNIQUE (generation_digest, record_digest),
    FOREIGN KEY (generation_digest, file_kind)
        REFERENCES listing_reference_files(generation_digest, file_kind),
    CHECK ((record_locator_reference IS NULL) = (record_locator_version IS NULL))
) STRICT, WITHOUT ROWID;

CREATE INDEX listing_reference_generations_current
ON listing_reference_generations(dataset_id, generation_sequence DESC);

CREATE INDEX listing_reference_values_symbol
ON listing_reference_values(normalized_provider_symbol, provider_symbol, listing_venue);

CREATE INDEX listing_reference_values_name
ON listing_reference_values(normalized_security_name, provider_symbol, listing_venue);

CREATE INDEX listing_reference_memberships_generation_value
ON listing_reference_memberships(generation_digest, value_digest);

CREATE TRIGGER listing_reference_generations_contiguous_insert
BEFORE INSERT ON listing_reference_generations
WHEN NEW.generation_sequence <> COALESCE(
        (
            SELECT MAX(generation_sequence) + 1
            FROM listing_reference_generations
            WHERE dataset_id = NEW.dataset_id
        ),
        1
    )
    OR (
        NEW.generation_sequence > 1
        AND NEW.previous_generation_digest IS NOT (
            SELECT generation_digest
            FROM listing_reference_generations
            WHERE dataset_id = NEW.dataset_id
            ORDER BY generation_sequence DESC
            LIMIT 1
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'listing reference generation is not a contiguous successor');
END;

CREATE TRIGGER listing_reference_generations_rights_insert
BEFORE INSERT ON listing_reference_generations
WHEN NOT EXISTS (
    SELECT 1
    FROM source_rights
    WHERE rights_id = NEW.rights_id
      AND source_id = NEW.source_id
      AND (operation_mask & 6) = 6
      AND admitted_at_ns <= NEW.published_at_ns
      AND (
          authorization_expires_at_ns IS NULL
          OR authorization_expires_at_ns > NEW.published_at_ns
      )
)
BEGIN
    SELECT RAISE(ABORT, 'listing reference generation lacks current persist and display rights');
END;

CREATE TRIGGER listing_reference_memberships_value_insert
BEFORE INSERT ON listing_reference_memberships
WHEN NOT EXISTS (
    SELECT 1
    FROM listing_reference_values
    WHERE value_digest = NEW.value_digest
      AND file_kind = NEW.file_kind
      AND provider_symbol = NEW.provider_symbol
)
BEGIN
    SELECT RAISE(ABORT, 'listing reference membership does not match its provider value');
END;

CREATE TRIGGER listing_reference_generations_immutable_update
BEFORE UPDATE ON listing_reference_generations BEGIN
    SELECT RAISE(ABORT, 'listing reference generations are immutable');
END;

CREATE TRIGGER listing_reference_generations_immutable_delete
BEFORE DELETE ON listing_reference_generations BEGIN
    SELECT RAISE(ABORT, 'listing reference generations are immutable');
END;

CREATE TRIGGER listing_reference_files_immutable_update
BEFORE UPDATE ON listing_reference_files BEGIN
    SELECT RAISE(ABORT, 'listing reference files are immutable');
END;

CREATE TRIGGER listing_reference_files_immutable_delete
BEFORE DELETE ON listing_reference_files BEGIN
    SELECT RAISE(ABORT, 'listing reference files are immutable');
END;

CREATE TRIGGER listing_reference_values_immutable_update
BEFORE UPDATE ON listing_reference_values BEGIN
    SELECT RAISE(ABORT, 'listing reference values are immutable');
END;

CREATE TRIGGER listing_reference_values_immutable_delete
BEFORE DELETE ON listing_reference_values BEGIN
    SELECT RAISE(ABORT, 'listing reference values are immutable');
END;

CREATE TRIGGER listing_reference_memberships_immutable_update
BEFORE UPDATE ON listing_reference_memberships BEGIN
    SELECT RAISE(ABORT, 'listing reference memberships are immutable');
END;

CREATE TRIGGER listing_reference_memberships_immutable_delete
BEFORE DELETE ON listing_reference_memberships BEGIN
    SELECT RAISE(ABORT, 'listing reference memberships are immutable');
END;
