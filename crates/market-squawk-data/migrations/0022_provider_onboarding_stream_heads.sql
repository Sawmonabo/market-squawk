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

CREATE TABLE official_options_reference_generations (
    generation_digest BLOB PRIMARY KEY CHECK (
        length(generation_digest)=32 AND generation_digest<>zeroblob(32)
    ),
    dataset_id TEXT NOT NULL CHECK (length(CAST(dataset_id AS BLOB)) BETWEEN 1 AND 512),
    generation_sequence INTEGER NOT NULL CHECK (generation_sequence BETWEEN 1 AND 16384),
    previous_generation_digest BLOB
        REFERENCES official_options_reference_generations(generation_digest),
    request_id TEXT NOT NULL CHECK (length(CAST(request_id AS BLOB)) BETWEEN 1 AND 512),
    requested_at_ns INTEGER NOT NULL,
    request_deadline_ns INTEGER NOT NULL,
    strict_row_set_digest BLOB NOT NULL CHECK (
        length(strict_row_set_digest)=32 AND strict_row_set_digest<>zeroblob(32)
    ),
    alias_assertion_count INTEGER NOT NULL CHECK (
        alias_assertion_count BETWEEN 1 AND 36000000
    ),
    alias_assertion_closure_digest BLOB NOT NULL CHECK (
        length(alias_assertion_closure_digest)=32
        AND alias_assertion_closure_digest<>zeroblob(32)
    ),
    total_payload_bytes INTEGER NOT NULL CHECK (
        total_payload_bytes BETWEEN 1 AND 2147483648
    ),
    strict_row_count INTEGER NOT NULL CHECK (strict_row_count BETWEEN 1 AND 12000000),
    record_count INTEGER NOT NULL CHECK (record_count BETWEEN 1 AND 12000000),
    object_count INTEGER NOT NULL CHECK (object_count BETWEEN 2 AND 64),
    alias_resolution_count INTEGER NOT NULL CHECK (
        alias_resolution_count BETWEEN 1 AND 36000000
    ),
    conflict_count INTEGER NOT NULL CHECK (conflict_count BETWEEN 0 AND 100000),
    record_set_digest BLOB NOT NULL CHECK (
        length(record_set_digest)=32 AND record_set_digest<>zeroblob(32)
    ),
    alias_resolution_set_digest BLOB NOT NULL CHECK (
        length(alias_resolution_set_digest)=32
        AND alias_resolution_set_digest<>zeroblob(32)
    ),
    conflict_set_digest BLOB NOT NULL CHECK (
        length(conflict_set_digest)=32 AND conflict_set_digest<>zeroblob(32)
    ),
    published_at_ns INTEGER NOT NULL,
    UNIQUE (dataset_id, generation_sequence),
    UNIQUE (dataset_id, request_id),
    CHECK (requested_at_ns<request_deadline_ns),
    CHECK (requested_at_ns<=published_at_ns),
    CHECK (
        (generation_sequence=1 AND previous_generation_digest IS NULL)
        OR (generation_sequence>1 AND previous_generation_digest IS NOT NULL)
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE official_options_reference_generation_sources (
    generation_digest BLOB NOT NULL
        REFERENCES official_options_reference_generations(generation_digest),
    provider TEXT NOT NULL CHECK (provider IN ('occ','cboe')),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    source_revision TEXT NOT NULL CHECK (
        length(CAST(source_revision AS BLOB)) BETWEEN 1 AND 512
    ),
    source_revision_digest BLOB NOT NULL CHECK (
        length(source_revision_digest)=32 AND source_revision_digest<>zeroblob(32)
    ),
    rights_id BLOB NOT NULL REFERENCES source_rights(rights_id),
    source_payload_set_digest BLOB NOT NULL CHECK (
        length(source_payload_set_digest)=32 AND source_payload_set_digest<>zeroblob(32)
    ),
    PRIMARY KEY (generation_digest, provider),
    UNIQUE (generation_digest, source_id),
    FOREIGN KEY (source_id, source_revision_digest)
        REFERENCES source_revisions(source_id, revision_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE official_options_reference_objects (
    generation_digest BLOB NOT NULL
        REFERENCES official_options_reference_generations(generation_digest),
    object_ordinal INTEGER NOT NULL CHECK (object_ordinal BETWEEN 0 AND 63),
    provider TEXT NOT NULL CHECK (provider IN ('occ','cboe')),
    source_id TEXT NOT NULL,
    surface_json TEXT NOT NULL CHECK (
        length(CAST(surface_json AS BLOB)) BETWEEN 2 AND 1024 AND json_valid(surface_json)
    ),
    surface_key TEXT NOT NULL CHECK (length(CAST(surface_key AS BLOB)) BETWEEN 1 AND 512),
    object_id TEXT NOT NULL CHECK (length(CAST(object_id AS BLOB)) BETWEEN 1 AND 512),
    native_schema TEXT NOT NULL CHECK (
        length(CAST(native_schema AS BLOB)) BETWEEN 1 AND 128
    ),
    raw_claim_digest BLOB NOT NULL REFERENCES sealed_raw_objects(raw_claim_digest),
    physical_receipt_digest BLOB NOT NULL CHECK (
        length(physical_receipt_digest)=32 AND physical_receipt_digest<>zeroblob(32)
    ),
    payload_digest BLOB NOT NULL CHECK (
        length(payload_digest)=32 AND payload_digest<>zeroblob(32)
    ),
    payload_bytes INTEGER NOT NULL CHECK (payload_bytes BETWEEN 1 AND 2147483648),
    source_timestamp_ns INTEGER,
    available_at_ns INTEGER NOT NULL,
    received_at_ns INTEGER NOT NULL,
    strict_row_set_digest BLOB NOT NULL CHECK (
        length(strict_row_set_digest)=32 AND strict_row_set_digest<>zeroblob(32)
    ),
    strict_row_count INTEGER NOT NULL CHECK (strict_row_count BETWEEN 0 AND 12000000),
    PRIMARY KEY (generation_digest, object_ordinal),
    UNIQUE (generation_digest, provider, surface_key),
    UNIQUE (generation_digest, provider, object_id),
    FOREIGN KEY (generation_digest, provider)
        REFERENCES official_options_reference_generation_sources(generation_digest, provider),
    FOREIGN KEY (raw_claim_digest, physical_receipt_digest)
        REFERENCES sealed_raw_objects(raw_claim_digest, physical_receipt_digest),
    CHECK (source_timestamp_ns IS NULL OR source_timestamp_ns<=received_at_ns),
    CHECK (available_at_ns<=received_at_ns)
) STRICT, WITHOUT ROWID;

CREATE TABLE official_options_reference_values (
    value_digest BLOB PRIMARY KEY CHECK (
        length(value_digest)=32 AND value_digest<>zeroblob(32)
    ),
    provider TEXT NOT NULL CHECK (provider IN ('occ','cboe')),
    record_kind TEXT NOT NULL CHECK (record_kind IN ('cboe_series','occ_product')),
    provider_symbol TEXT NOT NULL CHECK (
        length(CAST(provider_symbol AS BLOB)) BETWEEN 1 AND 512
    ),
    normalized_provider_symbol TEXT NOT NULL CHECK (
        length(CAST(normalized_provider_symbol AS BLOB)) BETWEEN 1 AND 512
    ),
    secondary_symbol TEXT NOT NULL CHECK (
        length(CAST(secondary_symbol AS BLOB)) BETWEEN 1 AND 512
    ),
    normalized_search_text TEXT NOT NULL CHECK (
        length(CAST(normalized_search_text AS BLOB)) BETWEEN 1 AND 1024
    ),
    venue TEXT,
    osi TEXT,
    occ_product_type TEXT,
    value_json TEXT NOT NULL CHECK (
        length(CAST(value_json AS BLOB)) BETWEEN 2 AND 65536 AND json_valid(value_json)
    ),
    CHECK (
        (record_kind='cboe_series' AND provider='cboe'
            AND venue IS NOT NULL AND osi IS NOT NULL AND occ_product_type IS NULL)
        OR
        (record_kind='occ_product' AND provider='occ'
            AND venue IS NULL AND osi IS NULL AND occ_product_type IS NOT NULL)
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE official_options_reference_memberships (
    generation_digest BLOB NOT NULL,
    object_ordinal INTEGER NOT NULL,
    provider_row_number INTEGER NOT NULL CHECK (
        provider_row_number BETWEEN 1 AND 12000001
    ),
    record_id TEXT NOT NULL CHECK (length(CAST(record_id AS BLOB)) BETWEEN 1 AND 512),
    value_digest BLOB NOT NULL REFERENCES official_options_reference_values(value_digest),
    record_digest BLOB NOT NULL CHECK (
        length(record_digest)=32 AND record_digest<>zeroblob(32)
    ),
    PRIMARY KEY (generation_digest, object_ordinal, provider_row_number),
    UNIQUE (generation_digest, record_id),
    UNIQUE (generation_digest, record_digest),
    FOREIGN KEY (generation_digest, object_ordinal)
        REFERENCES official_options_reference_objects(generation_digest, object_ordinal)
) STRICT, WITHOUT ROWID;

CREATE TABLE official_options_reference_alias_resolutions (
    generation_digest BLOB NOT NULL
        REFERENCES official_options_reference_generations(generation_digest),
    key_digest BLOB NOT NULL CHECK (length(key_digest)=32 AND key_digest<>zeroblob(32)),
    key_kind TEXT NOT NULL CHECK (
        key_kind IN ('cboe_symbol','cboe_osi','cboe_venue_symbol','occ_product')
    ),
    provider_symbol TEXT,
    venue TEXT,
    osi TEXT,
    occ_product_type TEXT,
    key_json TEXT NOT NULL CHECK (
        length(CAST(key_json AS BLOB)) BETWEEN 2 AND 4096 AND json_valid(key_json)
    ),
    state TEXT NOT NULL CHECK (state IN ('exact','ambiguous')),
    observation_count INTEGER NOT NULL CHECK (observation_count BETWEEN 1 AND 36000000),
    conflict_count INTEGER NOT NULL CHECK (conflict_count BETWEEN 0 AND 100000),
    resolution_digest BLOB NOT NULL CHECK (
        length(resolution_digest)=32 AND resolution_digest<>zeroblob(32)
    ),
    PRIMARY KEY (generation_digest, key_digest),
    UNIQUE (generation_digest, resolution_digest),
    CHECK (
        (state='exact' AND conflict_count=0)
        OR (state='ambiguous' AND conflict_count>0)
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE official_options_reference_conflicts (
    generation_digest BLOB NOT NULL,
    conflict_ordinal INTEGER NOT NULL CHECK (conflict_ordinal BETWEEN 0 AND 99999),
    key_digest BLOB NOT NULL,
    conflict_kind TEXT NOT NULL CHECK (
        conflict_kind IN (
            'cboe_symbol_maps_multiple_osi',
            'cboe_osi_maps_multiple_symbols',
            'cboe_symbol_maps_multiple_underlying',
            'duplicate_provider_record'
        )
    ),
    first_evidence TEXT NOT NULL CHECK (
        length(CAST(first_evidence AS BLOB)) BETWEEN 1 AND 512
    ),
    second_evidence TEXT NOT NULL CHECK (
        length(CAST(second_evidence AS BLOB)) BETWEEN 1 AND 512
    ),
    conflict_digest BLOB NOT NULL CHECK (
        length(conflict_digest)=32 AND conflict_digest<>zeroblob(32)
    ),
    PRIMARY KEY (generation_digest, conflict_ordinal),
    UNIQUE (generation_digest, conflict_digest),
    FOREIGN KEY (generation_digest, key_digest)
        REFERENCES official_options_reference_alias_resolutions(generation_digest, key_digest),
    FOREIGN KEY (generation_digest, first_evidence)
        REFERENCES official_options_reference_memberships(generation_digest, record_id),
    FOREIGN KEY (generation_digest, second_evidence)
        REFERENCES official_options_reference_memberships(generation_digest, record_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX official_options_reference_generation_current
ON official_options_reference_generations(dataset_id, generation_sequence DESC);
CREATE INDEX official_options_reference_generation_as_of
ON official_options_reference_generations(dataset_id, published_at_ns DESC, generation_sequence DESC);
CREATE INDEX official_options_reference_value_symbol
ON official_options_reference_values(normalized_provider_symbol, record_kind, provider_symbol);
CREATE INDEX official_options_reference_value_exact_symbol
ON official_options_reference_values(
    record_kind, provider_symbol, venue, occ_product_type, value_digest
);
CREATE INDEX official_options_reference_value_osi
ON official_options_reference_values(osi, record_kind);
CREATE INDEX official_options_reference_value_text
ON official_options_reference_values(normalized_search_text, record_kind, provider_symbol);
CREATE INDEX official_options_reference_membership_value
ON official_options_reference_memberships(generation_digest, value_digest);

CREATE TRIGGER official_options_reference_generation_contiguous_insert
BEFORE INSERT ON official_options_reference_generations
WHEN NEW.generation_sequence<>COALESCE(
        (SELECT MAX(generation_sequence)+1
         FROM official_options_reference_generations WHERE dataset_id=NEW.dataset_id), 1
    )
    OR (
        NEW.generation_sequence>1
        AND NEW.previous_generation_digest IS NOT (
            SELECT generation_digest FROM official_options_reference_generations
            WHERE dataset_id=NEW.dataset_id ORDER BY generation_sequence DESC LIMIT 1
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'official options reference generation is not a contiguous successor');
END;

CREATE TRIGGER official_options_reference_source_rights_insert
BEFORE INSERT ON official_options_reference_generation_sources
WHEN NOT EXISTS (
    SELECT 1 FROM source_rights AS rights
    JOIN official_options_reference_generations AS generation
      ON generation.generation_digest=NEW.generation_digest
    WHERE rights.rights_id=NEW.rights_id
      AND rights.source_id=NEW.source_id
      AND rights.payload_algorithm=1
      AND rights.payload_digest=NEW.source_payload_set_digest
      AND (rights.operation_mask & 6)=6
      AND rights.admitted_at_ns<=generation.published_at_ns
      AND (
          rights.authorization_expires_at_ns IS NULL
          OR rights.authorization_expires_at_ns>generation.published_at_ns
      )
)
BEGIN
    SELECT RAISE(ABORT, 'official options reference source lacks persist and display rights');
END;

CREATE TRIGGER official_options_reference_generation_immutable_update
BEFORE UPDATE ON official_options_reference_generations BEGIN
    SELECT RAISE(ABORT, 'official options reference generations are immutable');
END;
CREATE TRIGGER official_options_reference_generation_immutable_delete
BEFORE DELETE ON official_options_reference_generations BEGIN
    SELECT RAISE(ABORT, 'official options reference generations are immutable');
END;
CREATE TRIGGER official_options_reference_sources_immutable_update
BEFORE UPDATE ON official_options_reference_generation_sources BEGIN
    SELECT RAISE(ABORT, 'official options reference sources are immutable');
END;
CREATE TRIGGER official_options_reference_sources_immutable_delete
BEFORE DELETE ON official_options_reference_generation_sources BEGIN
    SELECT RAISE(ABORT, 'official options reference sources are immutable');
END;
CREATE TRIGGER official_options_reference_objects_immutable_update
BEFORE UPDATE ON official_options_reference_objects BEGIN
    SELECT RAISE(ABORT, 'official options reference objects are immutable');
END;
CREATE TRIGGER official_options_reference_objects_immutable_delete
BEFORE DELETE ON official_options_reference_objects BEGIN
    SELECT RAISE(ABORT, 'official options reference objects are immutable');
END;
CREATE TRIGGER official_options_reference_values_immutable_update
BEFORE UPDATE ON official_options_reference_values BEGIN
    SELECT RAISE(ABORT, 'official options reference values are immutable');
END;
CREATE TRIGGER official_options_reference_values_immutable_delete
BEFORE DELETE ON official_options_reference_values BEGIN
    SELECT RAISE(ABORT, 'official options reference values are immutable');
END;
CREATE TRIGGER official_options_reference_memberships_immutable_update
BEFORE UPDATE ON official_options_reference_memberships BEGIN
    SELECT RAISE(ABORT, 'official options reference memberships are immutable');
END;
CREATE TRIGGER official_options_reference_memberships_immutable_delete
BEFORE DELETE ON official_options_reference_memberships BEGIN
    SELECT RAISE(ABORT, 'official options reference memberships are immutable');
END;
CREATE TRIGGER official_options_reference_resolutions_immutable_update
BEFORE UPDATE ON official_options_reference_alias_resolutions BEGIN
    SELECT RAISE(ABORT, 'official options reference resolutions are immutable');
END;
CREATE TRIGGER official_options_reference_resolutions_immutable_delete
BEFORE DELETE ON official_options_reference_alias_resolutions BEGIN
    SELECT RAISE(ABORT, 'official options reference resolutions are immutable');
END;
CREATE TRIGGER official_options_reference_conflicts_immutable_update
BEFORE UPDATE ON official_options_reference_conflicts BEGIN
    SELECT RAISE(ABORT, 'official options reference conflicts are immutable');
END;
CREATE TRIGGER official_options_reference_conflicts_immutable_delete
BEFORE DELETE ON official_options_reference_conflicts BEGIN
    SELECT RAISE(ABORT, 'official options reference conflicts are immutable');
END;

-- One common durable SEC fund job is claimed only at the provider-logical precommit boundary.
-- The admission is intentionally separate from the immutable result: a crash or rollback before
-- the analytical transaction leaves an honest pending/rolled-back row, never a published result.
CREATE TABLE sec_fund_job_commit_admissions (
    job_id TEXT NOT NULL CHECK (length(CAST(job_id AS BLOB))=36),
    job_generation INTEGER NOT NULL CHECK (job_generation>0),
    admitted_request_digest BLOB NOT NULL CHECK (
        length(admitted_request_digest)=32 AND admitted_request_digest<>zeroblob(32)
    ),
    binding_digest BLOB NOT NULL CHECK (
        length(binding_digest)=32 AND binding_digest<>zeroblob(32)
    ),
    ingest_run_id TEXT NOT NULL REFERENCES ingest_runs(run_id),
    preparation_digest BLOB NOT NULL CHECK (
        length(preparation_digest)=32 AND preparation_digest<>zeroblob(32)
    ),
    family TEXT NOT NULL CHECK (family IN ('nport','ncen')),
    filing_year INTEGER NOT NULL CHECK (filing_year BETWEEN 1993 AND 9999),
    filing_quarter INTEGER NOT NULL CHECK (filing_quarter BETWEEN 1 AND 4),
    accession TEXT NOT NULL CHECK (
        length(CAST(accession AS BLOB)) BETWEEN 1 AND 512
    ),
    fund_id TEXT CHECK (
        fund_id IS NULL OR length(CAST(fund_id AS BLOB)) BETWEEN 1 AND 512
    ),
    fund_instrument_id BLOB NOT NULL CHECK (
        length(fund_instrument_id)=16 AND fund_instrument_id<>zeroblob(16)
    ),
    expected_row_count INTEGER NOT NULL CHECK (expected_row_count>0),
    expected_logical_object_bytes INTEGER NOT NULL CHECK (
        expected_logical_object_bytes>0
    ),
    expected_logical_object_count INTEGER NOT NULL CHECK (
        expected_logical_object_count BETWEEN 1 AND 64
    ),
    state TEXT NOT NULL CHECK (state IN ('pending','rolled_back','committed')),
    admitted_at_ns INTEGER NOT NULL,
    resolved_at_ns INTEGER,
    PRIMARY KEY (job_id, job_generation, admitted_request_digest),
    CHECK (
        (family='nport' AND fund_id IS NULL)
        OR (family='ncen' AND fund_id IS NOT NULL)
    ),
    CHECK (
        (state='pending' AND resolved_at_ns IS NULL)
        OR (
            state IN ('rolled_back','committed')
            AND resolved_at_ns IS NOT NULL
            AND resolved_at_ns>=admitted_at_ns
        )
    )
) STRICT, WITHOUT ROWID;

CREATE UNIQUE INDEX sec_fund_job_one_pending_binding
ON sec_fund_job_commit_admissions(binding_digest)
WHERE state='pending';

CREATE UNIQUE INDEX sec_fund_job_one_pending_ingest
ON sec_fund_job_commit_admissions(ingest_run_id)
WHERE state='pending';

CREATE TABLE sec_fund_job_publications (
    job_id TEXT NOT NULL,
    job_generation INTEGER NOT NULL,
    admitted_request_digest BLOB NOT NULL,
    ingest_run_id TEXT NOT NULL,
    generation_sequence INTEGER NOT NULL UNIQUE
        REFERENCES analytical_generations(generation_sequence),
    dataset_id TEXT NOT NULL,
    manifest_version INTEGER NOT NULL,
    binding_digest BLOB NOT NULL UNIQUE,
    preparation_digest BLOB NOT NULL CHECK (
        length(preparation_digest)=32 AND preparation_digest<>zeroblob(32)
    ),
    fund_instrument_id BLOB NOT NULL CHECK (
        length(fund_instrument_id)=16 AND fund_instrument_id<>zeroblob(16)
    ),
    generation_row_count INTEGER NOT NULL CHECK (generation_row_count>0),
    generation_total_bytes INTEGER NOT NULL CHECK (generation_total_bytes>0),
    generation_object_count INTEGER NOT NULL CHECK (generation_object_count>0),
    publication_row_count INTEGER NOT NULL CHECK (publication_row_count>0),
    logical_object_bytes INTEGER NOT NULL CHECK (logical_object_bytes>0),
    logical_object_count INTEGER NOT NULL CHECK (
        logical_object_count BETWEEN 1 AND 64
    ),
    committed_at_ns INTEGER NOT NULL,
    PRIMARY KEY (job_id, job_generation, admitted_request_digest),
    FOREIGN KEY (job_id, job_generation, admitted_request_digest)
        REFERENCES sec_fund_job_commit_admissions(
            job_id, job_generation, admitted_request_digest
        ),
    FOREIGN KEY (dataset_id, manifest_version)
        REFERENCES analytical_generations(dataset_id, manifest_version),
    FOREIGN KEY (generation_sequence, binding_digest)
        REFERENCES analytical_generation_provider_publication_bindings(
            generation_sequence, publication_digest
        ),
    FOREIGN KEY (ingest_run_id, binding_digest)
        REFERENCES ingest_run_provider_publication_bindings(
            run_id, publication_digest
        )
) STRICT, WITHOUT ROWID;

CREATE TRIGGER sec_fund_job_admissions_guarded_update
BEFORE UPDATE ON sec_fund_job_commit_admissions
WHEN OLD.state<>'pending'
 OR NEW.state NOT IN ('rolled_back','committed')
 OR NEW.resolved_at_ns IS NULL
 OR NEW.resolved_at_ns<OLD.admitted_at_ns
 OR NEW.job_id<>OLD.job_id
 OR NEW.job_generation<>OLD.job_generation
 OR NEW.admitted_request_digest<>OLD.admitted_request_digest
 OR NEW.binding_digest<>OLD.binding_digest
 OR NEW.ingest_run_id<>OLD.ingest_run_id
 OR NEW.preparation_digest<>OLD.preparation_digest
 OR NEW.family<>OLD.family
 OR NEW.filing_year<>OLD.filing_year
 OR NEW.filing_quarter<>OLD.filing_quarter
 OR NEW.accession<>OLD.accession
 OR NEW.fund_id IS NOT OLD.fund_id
 OR NEW.fund_instrument_id<>OLD.fund_instrument_id
 OR NEW.expected_row_count<>OLD.expected_row_count
 OR NEW.expected_logical_object_bytes<>OLD.expected_logical_object_bytes
 OR NEW.expected_logical_object_count<>OLD.expected_logical_object_count
 OR NEW.admitted_at_ns<>OLD.admitted_at_ns
BEGIN
    SELECT RAISE(ABORT, 'SEC fund job admission transition is invalid');
END;

CREATE TRIGGER sec_fund_job_admissions_immutable_delete
BEFORE DELETE ON sec_fund_job_commit_admissions BEGIN
    SELECT RAISE(ABORT, 'SEC fund job admissions are immutable');
END;

CREATE TRIGGER sec_fund_job_publications_guarded_insert
BEFORE INSERT ON sec_fund_job_publications
WHEN NOT EXISTS (
    SELECT 1
    FROM sec_fund_job_commit_admissions AS admission
    JOIN analytical_generations AS generation
      ON generation.generation_sequence=NEW.generation_sequence
     AND generation.dataset_id=NEW.dataset_id
     AND generation.manifest_version=NEW.manifest_version
    JOIN analytical_generation_provider_publication_bindings AS logical
      ON logical.generation_sequence=NEW.generation_sequence
     AND logical.publication_digest=NEW.binding_digest
     AND logical.publication_kind='provider_logical'
     AND logical.source_id='sec-edgar'
     AND logical.run_id=NEW.ingest_run_id
    JOIN provider_logical_publication_bindings AS binding
      ON binding.binding_digest=NEW.binding_digest
     AND binding.source_id='sec-edgar'
    WHERE admission.job_id=NEW.job_id
      AND admission.job_generation=NEW.job_generation
      AND admission.admitted_request_digest=NEW.admitted_request_digest
      AND admission.state='pending'
      AND admission.binding_digest=NEW.binding_digest
      AND admission.ingest_run_id=NEW.ingest_run_id
      AND admission.preparation_digest=NEW.preparation_digest
      AND admission.fund_instrument_id=NEW.fund_instrument_id
      AND admission.expected_row_count=NEW.publication_row_count
      AND admission.expected_logical_object_bytes=NEW.logical_object_bytes
      AND admission.expected_logical_object_count=NEW.logical_object_count
      AND generation.dataset_id='sec.fund-holdings.v1'
      AND generation.generation_kind='ingest'
      AND generation.schema_name='market_squawk.fund_holdings'
      AND generation.row_count=NEW.generation_row_count
      AND generation.total_bytes=NEW.generation_total_bytes
      AND NEW.generation_object_count=(
          SELECT COUNT(*) FROM analytical_generation_objects AS object
          WHERE object.dataset_id=generation.dataset_id
            AND object.manifest_version=generation.manifest_version
      )
      AND binding.object_count=NEW.logical_object_count
      AND CAST(json_extract(binding.terminal_json, '$.total_canonical_rows') AS INTEGER)
          =NEW.publication_row_count
      AND CAST(json_extract(binding.terminal_json, '$.total_logical_object_bytes') AS INTEGER)
          =NEW.logical_object_bytes
      AND NEW.committed_at_ns=generation.created_at_ns
)
BEGIN
    SELECT RAISE(ABORT, 'SEC fund job publication is invalid');
END;

-- Propagation of the exact provider-logical input occurs after every generation object is
-- inserted and before the ingest run succeeds. This trigger therefore extends that existing
-- BEGIN IMMEDIATE transaction without creating a second result store or a post-commit window.
CREATE TRIGGER sec_fund_job_publication_from_generation
AFTER INSERT ON analytical_generation_provider_publication_bindings
WHEN NEW.publication_kind='provider_logical'
 AND EXISTS (
     SELECT 1 FROM sec_fund_job_commit_admissions AS admission
     WHERE admission.binding_digest=NEW.publication_digest
       AND admission.ingest_run_id=NEW.run_id
       AND admission.state='pending'
 )
BEGIN
    INSERT INTO sec_fund_job_publications (
        job_id, job_generation, admitted_request_digest, ingest_run_id, generation_sequence,
        dataset_id, manifest_version, binding_digest, preparation_digest,
        fund_instrument_id, generation_row_count, generation_total_bytes,
        generation_object_count, publication_row_count, logical_object_bytes,
        logical_object_count, committed_at_ns
    )
    SELECT admission.job_id, admission.job_generation,
           admission.admitted_request_digest, admission.ingest_run_id,
           generation.generation_sequence,
           generation.dataset_id, generation.manifest_version, admission.binding_digest,
           admission.preparation_digest, admission.fund_instrument_id,
           generation.row_count, generation.total_bytes,
           (SELECT COUNT(*) FROM analytical_generation_objects AS object
            WHERE object.dataset_id=generation.dataset_id
              AND object.manifest_version=generation.manifest_version),
           admission.expected_row_count,
           admission.expected_logical_object_bytes,
           admission.expected_logical_object_count, generation.created_at_ns
    FROM sec_fund_job_commit_admissions AS admission
    JOIN analytical_generations AS generation
      ON generation.generation_sequence=NEW.generation_sequence
    WHERE admission.binding_digest=NEW.publication_digest
      AND admission.ingest_run_id=NEW.run_id
      AND admission.state='pending';

    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM sec_fund_job_publications AS publication
        WHERE publication.generation_sequence=NEW.generation_sequence
          AND publication.binding_digest=NEW.publication_digest
          AND publication.ingest_run_id=NEW.run_id
    ) THEN RAISE(ABORT, 'SEC fund job atomic publication was not retained') END;

    UPDATE sec_fund_job_commit_admissions
    SET state='committed', resolved_at_ns=(
        SELECT created_at_ns FROM analytical_generations
        WHERE generation_sequence=NEW.generation_sequence
    )
    WHERE binding_digest=NEW.publication_digest
      AND ingest_run_id=NEW.run_id
      AND state='pending';
END;

CREATE TRIGGER sec_fund_job_publications_immutable_update
BEFORE UPDATE ON sec_fund_job_publications BEGIN
    SELECT RAISE(ABORT, 'SEC fund job publications are immutable');
END;

CREATE TRIGGER sec_fund_job_publications_immutable_delete
BEFORE DELETE ON sec_fund_job_publications BEGIN
    SELECT RAISE(ABORT, 'SEC fund job publications are immutable');
END;
