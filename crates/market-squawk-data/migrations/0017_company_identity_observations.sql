CREATE TABLE company_identity_observations (
    record_digest BLOB PRIMARY KEY CHECK (length(record_digest) = 32),
    run_id TEXT NOT NULL UNIQUE REFERENCES ingest_runs(run_id),
    manifest_id TEXT NOT NULL REFERENCES dataset_manifests(manifest_id),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    source_surface TEXT NOT NULL CHECK (
        source_surface IN ('sec_submissions', 'sec_company_facts')
    ),
    provider_company_id TEXT NOT NULL CHECK (
        length(CAST(provider_company_id AS BLOB)) BETWEEN 1 AND 512
    ),
    record_json TEXT NOT NULL CHECK (
        length(CAST(record_json AS BLOB)) BETWEEN 1 AND 1048576
        AND json_valid(record_json)
    ),
    received_at_ns INTEGER NOT NULL,
    available_at_ns INTEGER,
    ingested_at_ns INTEGER NOT NULL,
    CHECK (ingested_at_ns >= received_at_ns)
) STRICT, WITHOUT ROWID;

CREATE TABLE company_identity_search_terms (
    record_digest BLOB NOT NULL REFERENCES company_identity_observations(record_digest),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    term_kind TEXT NOT NULL CHECK (
        term_kind IN (
            'current_name',
            'former_name',
            'provider_company_id',
            'ticker',
            'exchange',
            'entity_type',
            'sic',
            'sic_description'
        )
    ),
    display_value TEXT NOT NULL CHECK (
        length(CAST(display_value AS BLOB)) BETWEEN 1 AND 2048
    ),
    normalized_value TEXT NOT NULL CHECK (
        length(CAST(normalized_value AS BLOB)) BETWEEN 1 AND 2048
        AND normalized_value = lower(normalized_value)
    ),
    association_ordinal INTEGER CHECK (association_ordinal >= 0),
    PRIMARY KEY (record_digest, ordinal)
) STRICT, WITHOUT ROWID;

CREATE INDEX company_identity_search_terms_lookup
ON company_identity_search_terms(normalized_value, term_kind, record_digest);

CREATE INDEX company_identity_observations_company_history
ON company_identity_observations(source_id, provider_company_id, ingested_at_ns DESC);

CREATE TRIGGER company_identity_observations_validate_insert
BEFORE INSERT ON company_identity_observations BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM ingest_runs AS runs
        JOIN artifacts ON artifacts.run_id = runs.run_id
        JOIN dataset_manifests ON dataset_manifests.artifact_id = artifacts.artifact_id
        WHERE runs.run_id = NEW.run_id
          AND runs.source_id = NEW.source_id
          AND runs.state IN ('reserved', 'succeeded')
          AND dataset_manifests.manifest_id = NEW.manifest_id
    ) THEN RAISE(ABORT, 'company identity publication authority mismatch') END;
END;

CREATE TRIGGER company_identity_observations_immutable_update
BEFORE UPDATE ON company_identity_observations BEGIN
    SELECT RAISE(ABORT, 'company identity observations are immutable');
END;

CREATE TRIGGER company_identity_observations_immutable_delete
BEFORE DELETE ON company_identity_observations BEGIN
    SELECT RAISE(ABORT, 'company identity observations are immutable');
END;

CREATE TRIGGER company_identity_search_terms_immutable_update
BEFORE UPDATE ON company_identity_search_terms BEGIN
    SELECT RAISE(ABORT, 'company identity search terms are immutable');
END;

CREATE TRIGGER company_identity_search_terms_immutable_delete
BEFORE DELETE ON company_identity_search_terms BEGIN
    SELECT RAISE(ABORT, 'company identity search terms are immutable');
END;
