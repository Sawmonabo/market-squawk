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

CREATE TABLE company_security_link_events (
    link_digest BLOB PRIMARY KEY CHECK (
        length(link_digest) = 32 AND link_digest <> zeroblob(32)
    ),
    company_source_id TEXT NOT NULL CHECK (
        length(CAST(company_source_id AS BLOB)) BETWEEN 1 AND 128
    ),
    provider_company_id TEXT NOT NULL CHECK (
        length(CAST(provider_company_id AS BLOB)) BETWEEN 1 AND 512
    ),
    company_surface TEXT NOT NULL CHECK (
        company_surface IN ('sec_submissions', 'sec_company_facts')
    ),
    company_observation_digest BLOB NOT NULL
        REFERENCES company_identity_observations(record_digest),
    instrument_id TEXT NOT NULL CHECK (
        length(CAST(instrument_id AS BLOB)) = 36
    ),
    permanent_figi TEXT NOT NULL CHECK (
        length(CAST(permanent_figi AS BLOB)) = 12
        AND permanent_figi = upper(permanent_figi)
    ),
    market_revision_digest BLOB NOT NULL,
    event_sequence INTEGER NOT NULL CHECK (
        event_sequence BETWEEN 1 AND 16384
    ),
    security_kind TEXT NOT NULL CHECK (
        security_kind IN (
            'common_equity', 'preferred_equity', 'depositary_receipt',
            'debt', 'fund_interest', 'other'
        )
    ),
    relationship_kind TEXT NOT NULL CHECK (
        relationship_kind IN (
            'issuer', 'guarantor', 'depositary_underlying', 'fund_sponsor', 'other'
        )
    ),
    common_equity_suitability TEXT NOT NULL CHECK (
        common_equity_suitability IN ('suitable_issuer_common_equity', 'not_suitable')
    ),
    event_kind TEXT NOT NULL CHECK (event_kind IN ('active', 'revoked')),
    previous_link_digest BLOB REFERENCES company_security_link_events(link_digest),
    effective_start_ns INTEGER NOT NULL,
    effective_end_ns INTEGER,
    resolution_kind TEXT NOT NULL CHECK (
        resolution_kind IN ('direct_authoritative_crosswalk', 'operator_authorized_resolution')
    ),
    resolution_evidence_algorithm INTEGER NOT NULL CHECK (
        resolution_evidence_algorithm IN (1, 2)
    ),
    resolution_evidence_digest BLOB NOT NULL CHECK (
        length(resolution_evidence_digest) = 32
        AND resolution_evidence_digest <> zeroblob(32)
    ),
    relationship_rights_policy_id TEXT NOT NULL CHECK (
        length(CAST(relationship_rights_policy_id AS BLOB)) BETWEEN 1 AND 512
    ),
    relationship_rights_entitlement TEXT NOT NULL CHECK (
        relationship_rights_entitlement IN (
            'public_domain', 'user_owned', 'licensed_internal_use',
            'licensed_redistribution'
        )
    ),
    relationship_rights_terms_reference TEXT NOT NULL CHECK (
        length(CAST(relationship_rights_terms_reference AS BLOB)) BETWEEN 1 AND 512
    ),
    available_at_ns INTEGER NOT NULL,
    ingested_at_ns INTEGER NOT NULL,
    link_json TEXT NOT NULL CHECK (
        length(CAST(link_json AS BLOB)) BETWEEN 2 AND 1048576
    ),
    published_at_ns INTEGER NOT NULL,
    UNIQUE (
        company_source_id, provider_company_id, company_surface,
        instrument_id, event_sequence
    ),
    UNIQUE (
        link_digest, company_source_id, provider_company_id,
        company_surface, instrument_id
    ),
    FOREIGN KEY (market_revision_digest, instrument_id, permanent_figi)
        REFERENCES market_data_instrument_revisions(
            revision_digest, instrument_id, permanent_figi
        ),
    CHECK (effective_end_ns IS NULL OR effective_end_ns > effective_start_ns),
    CHECK (available_at_ns <= ingested_at_ns),
    CHECK (
        (event_kind = 'active')
        OR (event_kind = 'revoked' AND previous_link_digest IS NOT NULL)
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE company_security_link_current (
    company_source_id TEXT NOT NULL,
    provider_company_id TEXT NOT NULL,
    company_surface TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    link_digest BLOB NOT NULL UNIQUE,
    advanced_at_ns INTEGER NOT NULL,
    PRIMARY KEY (
        company_source_id, provider_company_id, company_surface, instrument_id
    ),
    FOREIGN KEY (
        link_digest, company_source_id, provider_company_id,
        company_surface, instrument_id
    ) REFERENCES company_security_link_events(
        link_digest, company_source_id, provider_company_id,
        company_surface, instrument_id
    )
) STRICT, WITHOUT ROWID;

CREATE INDEX company_security_link_company_as_of
ON company_security_link_events(
    company_source_id, provider_company_id, company_surface,
    published_at_ns DESC, event_sequence DESC, instrument_id, link_digest
);

CREATE INDEX company_security_link_instrument_history
ON company_security_link_events(instrument_id, published_at_ns DESC, link_digest);

CREATE TRIGGER company_security_link_validate_parents_insert
BEFORE INSERT ON company_security_link_events
WHEN NOT EXISTS (
        SELECT 1
        FROM company_identity_observations AS observations
        JOIN ingest_runs AS runs ON runs.run_id = observations.run_id
        WHERE observations.record_digest = NEW.company_observation_digest
          AND observations.source_id = NEW.company_source_id
          AND observations.provider_company_id = NEW.provider_company_id
          AND observations.source_surface = NEW.company_surface
          AND runs.state = 'succeeded'
    )
    OR NOT EXISTS (
        SELECT 1
        FROM market_data_instrument_revisions AS revisions
        WHERE revisions.revision_digest = NEW.market_revision_digest
          AND revisions.instrument_id = NEW.instrument_id
          AND revisions.permanent_figi = NEW.permanent_figi
    )
BEGIN
    SELECT RAISE(ABORT, 'company/security parent authority mismatch');
END;

CREATE TRIGGER company_security_link_contiguous_insert
BEFORE INSERT ON company_security_link_events
WHEN NEW.event_sequence <> COALESCE(
        (
            SELECT events.event_sequence + 1
            FROM company_security_link_current AS current_
            JOIN company_security_link_events AS events
              ON events.link_digest = current_.link_digest
            WHERE current_.company_source_id = NEW.company_source_id
              AND current_.provider_company_id = NEW.provider_company_id
              AND current_.company_surface = NEW.company_surface
              AND current_.instrument_id = NEW.instrument_id
        ),
        1
    )
    OR (
        NEW.previous_link_digest IS NULL
        AND EXISTS (
            SELECT 1 FROM company_security_link_current AS current_
            WHERE current_.company_source_id = NEW.company_source_id
              AND current_.provider_company_id = NEW.provider_company_id
              AND current_.company_surface = NEW.company_surface
              AND current_.instrument_id = NEW.instrument_id
        )
    )
    OR (
        NEW.previous_link_digest IS NOT NULL
        AND NEW.previous_link_digest IS NOT (
            SELECT current_.link_digest
            FROM company_security_link_current AS current_
            WHERE current_.company_source_id = NEW.company_source_id
              AND current_.provider_company_id = NEW.provider_company_id
              AND current_.company_surface = NEW.company_surface
              AND current_.instrument_id = NEW.instrument_id
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'company/security event is not a contiguous successor');
END;

CREATE TRIGGER company_security_link_current_successor_update
BEFORE UPDATE ON company_security_link_current
WHEN NEW.company_source_id <> OLD.company_source_id
    OR NEW.provider_company_id <> OLD.provider_company_id
    OR NEW.company_surface <> OLD.company_surface
    OR NEW.instrument_id <> OLD.instrument_id
    OR NEW.link_digest = OLD.link_digest
    OR NEW.advanced_at_ns < OLD.advanced_at_ns
    OR NOT EXISTS (
        SELECT 1
        FROM company_security_link_events AS successor
        WHERE successor.link_digest = NEW.link_digest
          AND successor.company_source_id = OLD.company_source_id
          AND successor.provider_company_id = OLD.provider_company_id
          AND successor.company_surface = OLD.company_surface
          AND successor.instrument_id = OLD.instrument_id
          AND successor.previous_link_digest = OLD.link_digest
    )
BEGIN
    SELECT RAISE(ABORT, 'invalid company/security current-link successor');
END;

CREATE TRIGGER company_security_link_events_immutable_update
BEFORE UPDATE ON company_security_link_events BEGIN
    SELECT RAISE(ABORT, 'company/security link events are immutable');
END;

CREATE TRIGGER company_security_link_events_immutable_delete
BEFORE DELETE ON company_security_link_events BEGIN
    SELECT RAISE(ABORT, 'company/security link events are immutable');
END;

CREATE TRIGGER company_security_link_current_no_delete
BEFORE DELETE ON company_security_link_current BEGIN
    SELECT RAISE(ABORT, 'company/security current-link pointers cannot be deleted');
END;

CREATE TABLE provider_capture_sets (
    capture_receipt_digest BLOB PRIMARY KEY CHECK (
        length(capture_receipt_digest) = 32
        AND capture_receipt_digest <> zeroblob(32)
    ),
    capture_content_digest BLOB NOT NULL CHECK (
        length(capture_content_digest) = 32
        AND capture_content_digest <> zeroblob(32)
    ),
    capture_observation_digest BLOB NOT NULL CHECK (
        length(capture_observation_digest) = 32
        AND capture_observation_digest <> zeroblob(32)
    ),
    source_id TEXT NOT NULL,
    source_revision_digest BLOB NOT NULL CHECK (
        length(source_revision_digest) = 32
        AND source_revision_digest <> zeroblob(32)
    ),
    metadata_revision TEXT NOT NULL CHECK (
        length(CAST(metadata_revision AS BLOB)) BETWEEN 1 AND 512
    ),
    provider_dataset TEXT NOT NULL CHECK (
        length(CAST(provider_dataset AS BLOB)) BETWEEN 1 AND 512
    ),
    request_set_identity BLOB NOT NULL CHECK (
        length(request_set_identity) = 32
        AND request_set_identity <> zeroblob(32)
    ),
    terminal_disposition TEXT NOT NULL CHECK (
        terminal_disposition IN (
            'standalone_response',
            'exhausted_without_next_page',
            'complete_request_graph'
        )
    ),
    page_count INTEGER NOT NULL CHECK (page_count BETWEEN 1 AND 64),
    total_body_bytes INTEGER NOT NULL CHECK (
        total_body_bytes BETWEEN 1 AND 67108864
    ),
    capture_json TEXT NOT NULL CHECK (
        length(CAST(capture_json AS BLOB)) BETWEEN 2 AND 2097152
        AND json_valid(capture_json)
    ),
    sealed_relative_reference TEXT NOT NULL UNIQUE CHECK (
        length(CAST(sealed_relative_reference AS BLOB)) BETWEEN 1 AND 1024
    ),
    sealed_content_digest BLOB NOT NULL UNIQUE CHECK (
        length(sealed_content_digest) = 32
        AND sealed_content_digest <> zeroblob(32)
    ),
    sealed_size_bytes INTEGER NOT NULL CHECK (
        sealed_size_bytes BETWEEN 1 AND 536870912
    ),
    sealed_physical_receipt_digest BLOB NOT NULL UNIQUE CHECK (
        length(sealed_physical_receipt_digest) = 32
        AND sealed_physical_receipt_digest <> zeroblob(32)
    ),
    segment_claim_json TEXT NOT NULL CHECK (
        length(CAST(segment_claim_json AS BLOB)) BETWEEN 2 AND 2097152
        AND json_valid(segment_claim_json)
    ),
    recorded_at_ns INTEGER NOT NULL,
    FOREIGN KEY (source_id, source_revision_digest)
        REFERENCES source_revisions(source_id, revision_digest),
    UNIQUE (capture_observation_digest, sealed_physical_receipt_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_capture_pages (
    capture_receipt_digest BLOB NOT NULL
        REFERENCES provider_capture_sets(capture_receipt_digest),
    page_ordinal INTEGER NOT NULL CHECK (page_ordinal BETWEEN 0 AND 63),
    request_identity BLOB NOT NULL CHECK (
        length(request_identity) = 32 AND request_identity <> zeroblob(32)
    ),
    request_page_token_digest BLOB CHECK (
        request_page_token_digest IS NULL
        OR (
            length(request_page_token_digest) = 32
            AND request_page_token_digest <> zeroblob(32)
        )
    ),
    response_next_page_token_digest BLOB CHECK (
        response_next_page_token_digest IS NULL
        OR (
            length(response_next_page_token_digest) = 32
            AND response_next_page_token_digest <> zeroblob(32)
        )
    ),
    http_status INTEGER NOT NULL CHECK (http_status BETWEEN 200 AND 299),
    body_bytes INTEGER NOT NULL CHECK (body_bytes BETWEEN 1 AND 16777216),
    body_digest BLOB NOT NULL CHECK (
        length(body_digest) = 32 AND body_digest <> zeroblob(32)
    ),
    received_at_ns INTEGER NOT NULL,
    PRIMARY KEY (capture_receipt_digest, page_ordinal)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_capture_frames (
    capture_receipt_digest BLOB NOT NULL,
    frame_ordinal INTEGER NOT NULL CHECK (frame_ordinal BETWEEN 0 AND 63),
    frame_offset INTEGER NOT NULL CHECK (frame_offset >= 4),
    framed_bytes INTEGER NOT NULL CHECK (framed_bytes > 8),
    provider_payload_bytes INTEGER NOT NULL CHECK (
        provider_payload_bytes BETWEEN 1 AND 16777216
    ),
    provider_payload_digest BLOB NOT NULL CHECK (
        length(provider_payload_digest) = 32
        AND provider_payload_digest <> zeroblob(32)
    ),
    received_at_ns INTEGER NOT NULL,
    source_sequence INTEGER CHECK (source_sequence IS NULL OR source_sequence >= 0),
    PRIMARY KEY (capture_receipt_digest, frame_ordinal),
    FOREIGN KEY (capture_receipt_digest, frame_ordinal)
        REFERENCES provider_capture_pages(capture_receipt_digest, page_ordinal)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER provider_capture_pages_set_match_insert
BEFORE INSERT ON provider_capture_pages
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_capture_sets AS capture
    WHERE capture.capture_receipt_digest = NEW.capture_receipt_digest
      AND NEW.page_ordinal < capture.page_count
      AND NEW.received_at_ns <= capture.recorded_at_ns
)
BEGIN
    SELECT RAISE(ABORT, 'provider capture page does not match its set receipt');
END;

CREATE TRIGGER provider_capture_frames_page_match_insert
BEFORE INSERT ON provider_capture_frames
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_capture_pages AS page
    WHERE page.capture_receipt_digest = NEW.capture_receipt_digest
      AND page.page_ordinal = NEW.frame_ordinal
      AND page.body_bytes = NEW.provider_payload_bytes
      AND page.body_digest = NEW.provider_payload_digest
      AND page.received_at_ns = NEW.received_at_ns
)
BEGIN
    SELECT RAISE(ABORT, 'provider capture frame does not match its page receipt');
END;

CREATE TABLE ingest_run_capture_inputs (
    run_id TEXT NOT NULL REFERENCES ingest_runs(run_id),
    input_ordinal INTEGER NOT NULL CHECK (input_ordinal = 0),
    capture_receipt_digest BLOB NOT NULL UNIQUE
        REFERENCES provider_capture_sets(capture_receipt_digest),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    PRIMARY KEY (run_id, input_ordinal),
    UNIQUE (run_id, capture_receipt_digest)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER ingest_run_capture_inputs_guarded_insert
BEFORE INSERT ON ingest_run_capture_inputs
WHEN NOT EXISTS (
    SELECT 1
    FROM ingest_runs AS run
    JOIN provider_capture_sets AS capture
      ON capture.capture_receipt_digest = NEW.capture_receipt_digest
    WHERE run.run_id = NEW.run_id
      AND run.state = 'reserved'
      AND run.operation = 'persist'
      AND run.source_id = NEW.source_id
      AND capture.source_id = NEW.source_id
)
BEGIN
    SELECT RAISE(ABORT, 'ingest-run provider capture input is invalid');
END;

CREATE TABLE analytical_generation_capture_inputs (
    generation_sequence INTEGER NOT NULL
        REFERENCES analytical_generations(generation_sequence),
    input_ordinal INTEGER NOT NULL CHECK (input_ordinal BETWEEN 0 AND 4095),
    capture_receipt_digest BLOB NOT NULL
        REFERENCES provider_capture_sets(capture_receipt_digest),
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    PRIMARY KEY (generation_sequence, input_ordinal),
    UNIQUE (generation_sequence, capture_receipt_digest),
    FOREIGN KEY (run_id, capture_receipt_digest)
        REFERENCES ingest_run_capture_inputs(run_id, capture_receipt_digest)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER analytical_generation_capture_inputs_guarded_insert
BEFORE INSERT ON analytical_generation_capture_inputs
WHEN NOT EXISTS (
    SELECT 1
    FROM analytical_generations AS generation
    JOIN analytical_generation_source_inputs AS source_input
      ON source_input.generation_sequence = generation.generation_sequence
    JOIN ingest_run_capture_inputs AS capture_input
      ON capture_input.run_id = source_input.run_id
    WHERE generation.generation_sequence = NEW.generation_sequence
      AND generation.generation_kind = 'ingest'
      AND capture_input.capture_receipt_digest = NEW.capture_receipt_digest
      AND capture_input.run_id = NEW.run_id
      AND capture_input.source_id = NEW.source_id
)
AND NOT EXISTS (
    SELECT 1
    FROM analytical_generations AS child
    JOIN analytical_generation_parents AS edge
      ON edge.child_dataset_id = child.dataset_id
     AND edge.child_manifest_version = child.manifest_version
    JOIN analytical_generations AS parent
      ON parent.generation_sequence = edge.parent_generation_sequence
    JOIN analytical_generation_capture_inputs AS parent_input
      ON parent_input.generation_sequence = parent.generation_sequence
    WHERE child.generation_sequence = NEW.generation_sequence
      AND parent_input.capture_receipt_digest = NEW.capture_receipt_digest
      AND parent_input.run_id = NEW.run_id
      AND parent_input.source_id = NEW.source_id
)
BEGIN
    SELECT RAISE(ABORT, 'analytical generation provider capture input is invalid');
END;

CREATE TRIGGER provider_capture_sets_immutable_update
BEFORE UPDATE ON provider_capture_sets BEGIN
    SELECT RAISE(ABORT, 'provider capture sets are immutable');
END;

CREATE TRIGGER provider_capture_sets_immutable_delete
BEFORE DELETE ON provider_capture_sets BEGIN
    SELECT RAISE(ABORT, 'provider capture sets are immutable');
END;

CREATE TRIGGER provider_capture_pages_immutable_update
BEFORE UPDATE ON provider_capture_pages BEGIN
    SELECT RAISE(ABORT, 'provider capture pages are immutable');
END;

CREATE TRIGGER provider_capture_pages_immutable_delete
BEFORE DELETE ON provider_capture_pages BEGIN
    SELECT RAISE(ABORT, 'provider capture pages are immutable');
END;

CREATE TRIGGER provider_capture_frames_immutable_update
BEFORE UPDATE ON provider_capture_frames BEGIN
    SELECT RAISE(ABORT, 'provider capture frames are immutable');
END;

CREATE TRIGGER provider_capture_frames_immutable_delete
BEFORE DELETE ON provider_capture_frames BEGIN
    SELECT RAISE(ABORT, 'provider capture frames are immutable');
END;

CREATE TRIGGER ingest_run_capture_inputs_immutable_update
BEFORE UPDATE ON ingest_run_capture_inputs BEGIN
    SELECT RAISE(ABORT, 'ingest-run provider capture inputs are immutable');
END;

CREATE TRIGGER ingest_run_capture_inputs_immutable_delete
BEFORE DELETE ON ingest_run_capture_inputs BEGIN
    SELECT RAISE(ABORT, 'ingest-run provider capture inputs are immutable');
END;

CREATE TRIGGER analytical_generation_capture_inputs_immutable_update
BEFORE UPDATE ON analytical_generation_capture_inputs BEGIN
    SELECT RAISE(ABORT, 'analytical generation provider capture inputs are immutable');
END;

CREATE TRIGGER analytical_generation_capture_inputs_immutable_delete
BEFORE DELETE ON analytical_generation_capture_inputs BEGIN
    SELECT RAISE(ABORT, 'analytical generation provider capture inputs are immutable');
END;
