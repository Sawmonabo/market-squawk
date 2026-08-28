-- Greenfield-register the durable typed market-event row schema beside the existing
-- analytical schemas. Publication remains closed to exact code-owned fingerprints.
DROP TRIGGER analytical_generations_registered_schema_insert;

CREATE TRIGGER analytical_generations_registered_schema_insert
BEFORE INSERT ON analytical_generations
WHEN NOT (
    (
        NEW.schema_name = 'market_squawk.research_observations'
        AND NEW.schema_version = 3
        AND NEW.schema_fingerprint =
            X'adbb26ab67e0389eb0a4422f1eb29b54be518fbb01a6b0624da41cb862e61c0a'
    ) OR (
        NEW.schema_name = 'market_squawk.feature_label_components'
        AND NEW.schema_version = 3
        AND NEW.schema_fingerprint =
            X'ca7f3447c5c353181b3776f2980a55dcbc54ae69a75b9231d5925912444322a4'
    ) OR (
        NEW.schema_name = 'market_squawk.market_events'
        AND NEW.schema_version = 1
        AND NEW.schema_fingerprint =
            X'e20fc1a2797ff2d8564c4be99c4ffbc162aad82b02e5f714c6384db815fe0912'
    )
) BEGIN
    SELECT RAISE(ABORT, 'analytical generation schema identity is not registered');
END;

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

CREATE TABLE provider_raw_observations (
    capture_observation_digest BLOB PRIMARY KEY CHECK (
        length(capture_observation_digest) = 32
        AND capture_observation_digest <> zeroblob(32)
    ),
    capture_content_digest BLOB NOT NULL CHECK (
        length(capture_content_digest) = 32
        AND capture_content_digest <> zeroblob(32)
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
    recorded_at_ns INTEGER NOT NULL,
    FOREIGN KEY (source_id, source_revision_digest)
        REFERENCES source_revisions(source_id, revision_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_raw_observation_pages (
    capture_observation_digest BLOB NOT NULL
        REFERENCES provider_raw_observations(capture_observation_digest),
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
    PRIMARY KEY (capture_observation_digest, page_ordinal)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_capture_recovery_capacity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    physical_claims INTEGER NOT NULL CHECK (
        physical_claims BETWEEN 0 AND 25000
    ),
    physical_bytes INTEGER NOT NULL CHECK (
        physical_bytes BETWEEN 0 AND 549755813888
    )
) STRICT, WITHOUT ROWID;

INSERT INTO provider_capture_recovery_capacity
    (singleton, physical_claims, physical_bytes)
VALUES (1, 0, 0);

CREATE TABLE sealed_raw_objects (
    raw_claim_digest BLOB PRIMARY KEY CHECK (
        length(raw_claim_digest) = 32 AND raw_claim_digest <> zeroblob(32)
    ),
    physical_receipt_digest BLOB NOT NULL CHECK (
        length(physical_receipt_digest) = 32
        AND physical_receipt_digest <> zeroblob(32)
    ),
    relative_reference TEXT NOT NULL CHECK (
        length(CAST(relative_reference AS BLOB)) BETWEEN 1 AND 1024
    ),
    content_digest BLOB NOT NULL CHECK (
        length(content_digest) = 32 AND content_digest <> zeroblob(32)
    ),
    size_bytes INTEGER NOT NULL CHECK (size_bytes BETWEEN 1 AND 536870912),
    frame_count INTEGER NOT NULL CHECK (frame_count BETWEEN 1 AND 64),
    raw_claim_json TEXT NOT NULL CHECK (
        length(CAST(raw_claim_json AS BLOB)) BETWEEN 2 AND 2097152
        AND json_valid(raw_claim_json)
    ),
    recorded_at_ns INTEGER NOT NULL,
    UNIQUE (raw_claim_digest, physical_receipt_digest)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER sealed_raw_objects_recovery_capacity_insert
BEFORE INSERT ON sealed_raw_objects
WHEN NOT EXISTS (
    SELECT 1 FROM sealed_raw_objects WHERE raw_claim_digest = NEW.raw_claim_digest
) AND EXISTS (
    SELECT 1
    FROM provider_capture_recovery_capacity
    WHERE singleton = 1
      AND (
          physical_claims >= 25000
          OR physical_bytes > 549755813888 - NEW.size_bytes
      )
)
BEGIN
    SELECT RAISE(ABORT, 'sealed provider raw-object recovery capacity exceeded');
END;

CREATE TRIGGER sealed_raw_objects_recovery_capacity_account
AFTER INSERT ON sealed_raw_objects
BEGIN
    UPDATE provider_capture_recovery_capacity
    SET physical_claims = physical_claims + 1,
        physical_bytes = physical_bytes + NEW.size_bytes
    WHERE singleton = 1;
END;

CREATE TABLE provider_raw_observation_objects (
    capture_observation_digest BLOB NOT NULL
        REFERENCES provider_raw_observations(capture_observation_digest),
    input_ordinal INTEGER NOT NULL CHECK (input_ordinal BETWEEN 0 AND 63),
    raw_claim_digest BLOB NOT NULL REFERENCES sealed_raw_objects(raw_claim_digest),
    physical_receipt_digest BLOB NOT NULL CHECK (
        length(physical_receipt_digest) = 32
        AND physical_receipt_digest <> zeroblob(32)
    ),
    object_capture_content_digest BLOB NOT NULL CHECK (
        length(object_capture_content_digest) = 32
        AND object_capture_content_digest <> zeroblob(32)
    ),
    object_capture_observation_digest BLOB NOT NULL CHECK (
        length(object_capture_observation_digest) = 32
        AND object_capture_observation_digest <> zeroblob(32)
    ),
    capture_receipt_digest BLOB NOT NULL CHECK (
        length(capture_receipt_digest) = 32
        AND capture_receipt_digest <> zeroblob(32)
    ),
    PRIMARY KEY (
        capture_observation_digest,
        input_ordinal,
        raw_claim_digest,
        physical_receipt_digest
    ),
    UNIQUE (
        capture_observation_digest,
        raw_claim_digest,
        physical_receipt_digest
    ),
    UNIQUE (
        capture_receipt_digest,
        capture_observation_digest,
        physical_receipt_digest
    ),
    FOREIGN KEY (raw_claim_digest, physical_receipt_digest)
        REFERENCES sealed_raw_objects(raw_claim_digest, physical_receipt_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_raw_observation_frames (
    capture_observation_digest BLOB NOT NULL,
    observation_unit_ordinal INTEGER NOT NULL CHECK (
        observation_unit_ordinal BETWEEN 0 AND 63
    ),
    raw_object_input_ordinal INTEGER NOT NULL CHECK (
        raw_object_input_ordinal BETWEEN 0 AND 63
    ),
    raw_claim_digest BLOB NOT NULL,
    physical_receipt_digest BLOB NOT NULL CHECK (
        length(physical_receipt_digest) = 32
        AND physical_receipt_digest <> zeroblob(32)
    ),
    raw_unit_ordinal INTEGER NOT NULL CHECK (raw_unit_ordinal BETWEEN 0 AND 63),
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
    source_sequence BLOB CHECK (
        source_sequence IS NULL OR length(source_sequence) = 8
    ),
    PRIMARY KEY (
        capture_observation_digest,
        raw_claim_digest,
        physical_receipt_digest,
        raw_unit_ordinal
    ),
    UNIQUE (
        capture_observation_digest,
        raw_claim_digest,
        physical_receipt_digest,
        observation_unit_ordinal
    ),
    UNIQUE (
        capture_observation_digest,
        raw_object_input_ordinal,
        raw_claim_digest,
        physical_receipt_digest,
        raw_unit_ordinal
    ),
    FOREIGN KEY (capture_observation_digest, observation_unit_ordinal)
        REFERENCES provider_raw_observation_pages(
            capture_observation_digest,
            page_ordinal
        ),
    FOREIGN KEY (
        capture_observation_digest,
        raw_object_input_ordinal,
        raw_claim_digest,
        physical_receipt_digest
    )
        REFERENCES provider_raw_observation_objects(
            capture_observation_digest,
            input_ordinal,
            raw_claim_digest,
            physical_receipt_digest
        )
) STRICT, WITHOUT ROWID;

CREATE TRIGGER provider_raw_observation_pages_set_match_insert
BEFORE INSERT ON provider_raw_observation_pages
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_raw_observations AS capture
    WHERE capture.capture_observation_digest = NEW.capture_observation_digest
      AND NEW.page_ordinal < capture.page_count
      AND NEW.received_at_ns <= capture.recorded_at_ns
)
BEGIN
    SELECT RAISE(ABORT, 'provider capture page does not match its set receipt');
END;

CREATE TRIGGER provider_raw_observation_frames_page_match_insert
BEFORE INSERT ON provider_raw_observation_frames
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_raw_observation_pages AS page
    WHERE page.capture_observation_digest = NEW.capture_observation_digest
      AND page.page_ordinal = NEW.observation_unit_ordinal
      AND page.body_bytes = NEW.provider_payload_bytes
      AND page.body_digest = NEW.provider_payload_digest
      AND page.received_at_ns = NEW.received_at_ns
)
BEGIN
    SELECT RAISE(ABORT, 'provider capture frame does not match its page receipt');
END;

CREATE TABLE provider_capture_bindings (
    binding_digest BLOB PRIMARY KEY CHECK (
        length(binding_digest) = 32 AND binding_digest <> zeroblob(32)
    ),
    binding_format_version INTEGER NOT NULL CHECK (binding_format_version = 1),
    capture_observation_digest BLOB NOT NULL
        REFERENCES provider_raw_observations(capture_observation_digest),
    sealed_capture_receipt_digest BLOB NOT NULL CHECK (
        length(sealed_capture_receipt_digest) = 32
        AND sealed_capture_receipt_digest <> zeroblob(32)
    ),
    capture_scope TEXT NOT NULL CHECK (capture_scope IN ('whole', 'component')),
    binding_layout TEXT NOT NULL CHECK (
        binding_layout IN (
            'whole_single_segment',
            'request_graph_component',
            'ordered_segments'
        )
    ),
    request_graph_component_ordinal INTEGER CHECK (
        request_graph_component_ordinal IS NULL
        OR request_graph_component_ordinal BETWEEN 0 AND 63
    ),
    extraction_content_digest BLOB NOT NULL CHECK (
        length(extraction_content_digest) = 32
        AND extraction_content_digest <> zeroblob(32)
    ),
    canonical_record_count INTEGER NOT NULL CHECK (
        canonical_record_count BETWEEN 1 AND 100000
    ),
    row_mapping_digest BLOB NOT NULL CHECK (
        length(row_mapping_digest) = 32 AND row_mapping_digest <> zeroblob(32)
    ),
    recorded_at_ns INTEGER NOT NULL,
    UNIQUE (binding_digest, capture_observation_digest),
    CHECK (
        (capture_scope = 'component'
            AND binding_layout = 'request_graph_component'
            AND request_graph_component_ordinal IS NOT NULL)
        OR (capture_scope = 'whole'
            AND binding_layout IN ('whole_single_segment', 'ordered_segments')
            AND request_graph_component_ordinal IS NULL)
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_capture_binding_native_lineage (
    binding_digest BLOB PRIMARY KEY
        REFERENCES provider_capture_bindings(binding_digest),
    schema_version INTEGER NOT NULL CHECK (schema_version BETWEEN 1 AND 65535),
    implementation TEXT NOT NULL CHECK (
        length(CAST(implementation AS BLOB)) BETWEEN 1 AND 128
    ),
    schema_fingerprint BLOB NOT NULL CHECK (
        length(schema_fingerprint) = 32 AND schema_fingerprint <> zeroblob(32)
    ),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 100000),
    batch_digest BLOB NOT NULL CHECK (
        length(batch_digest) = 32 AND batch_digest <> zeroblob(32)
    ),
    batch_sidecar_payload BLOB CHECK (
        batch_sidecar_payload IS NULL
        OR length(batch_sidecar_payload) BETWEEN 1 AND 4194304
    ),
    batch_sidecar_digest BLOB CHECK (
        batch_sidecar_digest IS NULL
        OR (
            length(batch_sidecar_digest) = 32
            AND batch_sidecar_digest <> zeroblob(32)
        )
    ),
    CHECK (
        (batch_sidecar_payload IS NULL AND batch_sidecar_digest IS NULL)
        OR (batch_sidecar_payload IS NOT NULL AND batch_sidecar_digest IS NOT NULL)
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_capture_binding_objects (
    binding_digest BLOB NOT NULL,
    input_ordinal INTEGER NOT NULL CHECK (input_ordinal BETWEEN 0 AND 63),
    capture_observation_digest BLOB NOT NULL,
    raw_claim_digest BLOB NOT NULL,
    physical_receipt_digest BLOB NOT NULL CHECK (
        length(physical_receipt_digest) = 32
        AND physical_receipt_digest <> zeroblob(32)
    ),
    PRIMARY KEY (binding_digest, input_ordinal),
    UNIQUE (
        binding_digest,
        input_ordinal,
        raw_claim_digest,
        physical_receipt_digest
    ),
    FOREIGN KEY (binding_digest, capture_observation_digest)
        REFERENCES provider_capture_bindings(binding_digest, capture_observation_digest),
    FOREIGN KEY (
        capture_observation_digest,
        input_ordinal,
        raw_claim_digest,
        physical_receipt_digest
    ) REFERENCES provider_raw_observation_objects(
        capture_observation_digest,
        input_ordinal,
        raw_claim_digest,
        physical_receipt_digest
    )
) STRICT, WITHOUT ROWID;

CREATE INDEX provider_capture_binding_objects_by_raw_object
ON provider_capture_binding_objects(raw_claim_digest, physical_receipt_digest);

CREATE TABLE provider_capture_binding_rows (
    binding_digest BLOB NOT NULL,
    capture_observation_digest BLOB NOT NULL,
    canonical_row_ordinal INTEGER NOT NULL CHECK (
        canonical_row_ordinal BETWEEN 0 AND 99999
    ),
    canonical_record_digest BLOB NOT NULL CHECK (
        length(canonical_record_digest) = 32
        AND canonical_record_digest <> zeroblob(32)
    ),
    native_semantic_payload BLOB NOT NULL CHECK (
        length(native_semantic_payload) BETWEEN 1 AND 65536
    ),
    native_semantic_digest BLOB NOT NULL CHECK (
        length(native_semantic_digest) = 32
        AND native_semantic_digest <> zeroblob(32)
    ),
    capture_page_ordinal INTEGER NOT NULL CHECK (capture_page_ordinal BETWEEN 0 AND 63),
    segment_ordinal INTEGER NOT NULL CHECK (segment_ordinal BETWEEN 0 AND 63),
    raw_claim_digest BLOB NOT NULL,
    physical_receipt_digest BLOB NOT NULL CHECK (
        length(physical_receipt_digest) = 32
        AND physical_receipt_digest <> zeroblob(32)
    ),
    physical_frame_ordinal INTEGER NOT NULL CHECK (
        physical_frame_ordinal BETWEEN 0 AND 63
    ),
    page_body_digest BLOB NOT NULL CHECK (
        length(page_body_digest) = 32 AND page_body_digest <> zeroblob(32)
    ),
    received_at_ns INTEGER NOT NULL,
    source_sequence BLOB CHECK (
        source_sequence IS NULL OR length(source_sequence) = 8
    ),
    PRIMARY KEY (binding_digest, canonical_row_ordinal),
    FOREIGN KEY (binding_digest, capture_observation_digest)
        REFERENCES provider_capture_bindings(binding_digest, capture_observation_digest),
    FOREIGN KEY (
        binding_digest,
        segment_ordinal,
        raw_claim_digest,
        physical_receipt_digest
    ) REFERENCES provider_capture_binding_objects(
        binding_digest,
        input_ordinal,
        raw_claim_digest,
        physical_receipt_digest
    ),
    FOREIGN KEY (capture_observation_digest, capture_page_ordinal)
        REFERENCES provider_raw_observation_pages(
            capture_observation_digest,
            page_ordinal
        ),
    FOREIGN KEY (
        capture_observation_digest,
        raw_claim_digest,
        physical_receipt_digest,
        physical_frame_ordinal
    ) REFERENCES provider_raw_observation_frames(
        capture_observation_digest,
        raw_claim_digest,
        physical_receipt_digest,
        raw_unit_ordinal
    )
) STRICT, WITHOUT ROWID;

-- Typed current-market events decoded from HTTP retain HTTP response semantics, while their
-- canonical rows remain distinct from research observations/provider_capture_binding_rows.
CREATE TABLE provider_response_market_event_bindings (
    response_event_binding_digest BLOB PRIMARY KEY CHECK (
        length(response_event_binding_digest) = 32
        AND response_event_binding_digest <> zeroblob(32)
    ),
    binding_format_version INTEGER NOT NULL CHECK (binding_format_version = 1),
    capture_observation_digest BLOB NOT NULL
        REFERENCES provider_raw_observations(capture_observation_digest),
    sealed_capture_receipt_digest BLOB NOT NULL CHECK (
        length(sealed_capture_receipt_digest) = 32
        AND sealed_capture_receipt_digest <> zeroblob(32)
    ),
    canonical_schema_fingerprint BLOB NOT NULL CHECK (
        length(canonical_schema_fingerprint) = 32
        AND canonical_schema_fingerprint <> zeroblob(32)
    ),
    canonical_content_digest BLOB NOT NULL CHECK (
        length(canonical_content_digest) = 32
        AND canonical_content_digest <> zeroblob(32)
    ),
    canonical_event_count INTEGER NOT NULL CHECK (canonical_event_count BETWEEN 1 AND 64),
    row_mapping_digest BLOB NOT NULL CHECK (
        length(row_mapping_digest) = 32 AND row_mapping_digest <> zeroblob(32)
    ),
    recorded_at_ns INTEGER NOT NULL,
    UNIQUE (response_event_binding_digest, capture_observation_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_response_market_event_binding_native_lineage (
    response_event_binding_digest BLOB PRIMARY KEY
        REFERENCES provider_response_market_event_bindings(response_event_binding_digest),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    implementation TEXT NOT NULL CHECK (
        length(CAST(implementation AS BLOB)) BETWEEN 1 AND 128
    ),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 64),
    batch_digest BLOB NOT NULL CHECK (
        length(batch_digest) = 32 AND batch_digest <> zeroblob(32)
    ),
    batch_sidecar_payload BLOB CHECK (
        batch_sidecar_payload IS NULL
        OR length(batch_sidecar_payload) BETWEEN 1 AND 4194304
    ),
    batch_sidecar_digest BLOB CHECK (
        batch_sidecar_digest IS NULL
        OR (
            length(batch_sidecar_digest) = 32
            AND batch_sidecar_digest <> zeroblob(32)
        )
    ),
    CHECK (
        (batch_sidecar_payload IS NULL AND batch_sidecar_digest IS NULL)
        OR (batch_sidecar_payload IS NOT NULL AND batch_sidecar_digest IS NOT NULL)
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_response_market_event_binding_rows (
    response_event_binding_digest BLOB NOT NULL,
    capture_observation_digest BLOB NOT NULL,
    canonical_row_ordinal INTEGER NOT NULL CHECK (canonical_row_ordinal BETWEEN 0 AND 63),
    canonical_event_digest BLOB NOT NULL CHECK (
        length(canonical_event_digest) = 32
        AND canonical_event_digest <> zeroblob(32)
    ),
    native_semantic_payload BLOB NOT NULL CHECK (
        length(native_semantic_payload) BETWEEN 1 AND 65536
    ),
    native_semantic_digest BLOB NOT NULL CHECK (
        length(native_semantic_digest) = 32
        AND native_semantic_digest <> zeroblob(32)
    ),
    capture_page_ordinal INTEGER NOT NULL CHECK (capture_page_ordinal BETWEEN 0 AND 63),
    physical_frame_ordinal INTEGER NOT NULL CHECK (
        physical_frame_ordinal BETWEEN 0 AND 63
    ),
    payload_digest BLOB NOT NULL CHECK (
        length(payload_digest) = 32 AND payload_digest <> zeroblob(32)
    ),
    received_at_ns INTEGER NOT NULL,
    source_sequence BLOB CHECK (
        source_sequence IS NULL OR length(source_sequence) = 8
    ),
    PRIMARY KEY (response_event_binding_digest, canonical_row_ordinal),
    FOREIGN KEY (response_event_binding_digest, capture_observation_digest)
        REFERENCES provider_response_market_event_bindings(
            response_event_binding_digest,
            capture_observation_digest
        ),
    FOREIGN KEY (capture_observation_digest, capture_page_ordinal)
        REFERENCES provider_raw_observation_pages(
            capture_observation_digest,
            page_ordinal
        )
) STRICT, WITHOUT ROWID;

-- Live event microbatches retain their stream semantics independently from HTTP response pages.
-- The immutable journal object is shared physical storage only; no event is represented as a
-- provider_raw_observation_page.
CREATE TABLE provider_event_microbatches (
    event_observation_digest BLOB PRIMARY KEY CHECK (
        length(event_observation_digest) = 32
        AND event_observation_digest <> zeroblob(32)
    ),
    event_content_digest BLOB NOT NULL CHECK (
        length(event_content_digest) = 32
        AND event_content_digest <> zeroblob(32)
    ),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    source_revision_digest BLOB NOT NULL CHECK (
        length(source_revision_digest) = 32
        AND source_revision_digest <> zeroblob(32)
    ),
    dataset TEXT NOT NULL CHECK (length(CAST(dataset AS BLOB)) BETWEEN 1 AND 256),
    stream_identity TEXT NOT NULL CHECK (
        length(CAST(stream_identity AS BLOB)) BETWEEN 1 AND 256
    ),
    frame_count INTEGER NOT NULL CHECK (frame_count BETWEEN 1 AND 64),
    total_payload_bytes INTEGER NOT NULL CHECK (
        total_payload_bytes BETWEEN 1 AND 67108864
    ),
    capture_json TEXT NOT NULL CHECK (
        length(CAST(capture_json AS BLOB)) BETWEEN 2 AND 2097152
        AND json_valid(capture_json)
    ),
    recorded_at_ns INTEGER NOT NULL,
    UNIQUE (event_observation_digest, source_id),
    FOREIGN KEY (source_id, source_revision_digest)
        REFERENCES source_revisions(source_id, revision_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_event_microbatch_frames (
    event_observation_digest BLOB NOT NULL
        REFERENCES provider_event_microbatches(event_observation_digest),
    event_frame_ordinal INTEGER NOT NULL CHECK (event_frame_ordinal BETWEEN 0 AND 63),
    event_id BLOB NOT NULL CHECK (length(event_id) = 16 AND event_id <> zeroblob(16)),
    connection_id BLOB NOT NULL CHECK (
        length(connection_id) = 16 AND connection_id <> zeroblob(16)
    ),
    source_sequence BLOB CHECK (
        source_sequence IS NULL OR length(source_sequence) = 8
    ),
    exchange_at_ns INTEGER,
    received_at_ns INTEGER NOT NULL,
    payload_bytes INTEGER NOT NULL CHECK (payload_bytes BETWEEN 1 AND 16777216),
    payload_digest BLOB NOT NULL CHECK (
        length(payload_digest) = 32 AND payload_digest <> zeroblob(32)
    ),
    PRIMARY KEY (event_observation_digest, event_frame_ordinal),
    UNIQUE (event_observation_digest, event_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_event_microbatch_objects (
    event_observation_digest BLOB PRIMARY KEY
        REFERENCES provider_event_microbatches(event_observation_digest),
    raw_claim_digest BLOB NOT NULL REFERENCES sealed_raw_objects(raw_claim_digest),
    physical_receipt_digest BLOB NOT NULL CHECK (
        length(physical_receipt_digest) = 32
        AND physical_receipt_digest <> zeroblob(32)
    ),
    sealed_event_receipt_digest BLOB NOT NULL UNIQUE CHECK (
        length(sealed_event_receipt_digest) = 32
        AND sealed_event_receipt_digest <> zeroblob(32)
    ),
    UNIQUE (event_observation_digest, sealed_event_receipt_digest),
    UNIQUE (event_observation_digest, raw_claim_digest, physical_receipt_digest),
    FOREIGN KEY (raw_claim_digest, physical_receipt_digest)
        REFERENCES sealed_raw_objects(raw_claim_digest, physical_receipt_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_event_bindings (
    event_binding_digest BLOB PRIMARY KEY CHECK (
        length(event_binding_digest) = 32 AND event_binding_digest <> zeroblob(32)
    ),
    binding_format_version INTEGER NOT NULL CHECK (binding_format_version = 1),
    event_observation_digest BLOB NOT NULL
        REFERENCES provider_event_microbatches(event_observation_digest),
    sealed_event_receipt_digest BLOB NOT NULL CHECK (
        length(sealed_event_receipt_digest) = 32
        AND sealed_event_receipt_digest <> zeroblob(32)
    ),
    canonical_schema_fingerprint BLOB NOT NULL CHECK (
        length(canonical_schema_fingerprint) = 32
        AND canonical_schema_fingerprint <> zeroblob(32)
    ),
    canonical_content_digest BLOB NOT NULL CHECK (
        length(canonical_content_digest) = 32
        AND canonical_content_digest <> zeroblob(32)
    ),
    canonical_event_count INTEGER NOT NULL CHECK (canonical_event_count BETWEEN 1 AND 64),
    row_mapping_digest BLOB NOT NULL CHECK (
        length(row_mapping_digest) = 32 AND row_mapping_digest <> zeroblob(32)
    ),
    recorded_at_ns INTEGER NOT NULL,
    UNIQUE (event_binding_digest, event_observation_digest),
    FOREIGN KEY (event_observation_digest, sealed_event_receipt_digest)
        REFERENCES provider_event_microbatch_objects(
            event_observation_digest,
            sealed_event_receipt_digest
        )
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_event_binding_native_lineage (
    event_binding_digest BLOB PRIMARY KEY
        REFERENCES provider_event_bindings(event_binding_digest),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    implementation TEXT NOT NULL CHECK (
        length(CAST(implementation AS BLOB)) BETWEEN 1 AND 128
    ),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 64),
    batch_digest BLOB NOT NULL CHECK (
        length(batch_digest) = 32 AND batch_digest <> zeroblob(32)
    ),
    batch_sidecar_payload BLOB CHECK (
        batch_sidecar_payload IS NULL
        OR length(batch_sidecar_payload) BETWEEN 1 AND 4194304
    ),
    batch_sidecar_digest BLOB CHECK (
        batch_sidecar_digest IS NULL
        OR (
            length(batch_sidecar_digest) = 32
            AND batch_sidecar_digest <> zeroblob(32)
        )
    ),
    CHECK (
        (batch_sidecar_payload IS NULL AND batch_sidecar_digest IS NULL)
        OR (batch_sidecar_payload IS NOT NULL AND batch_sidecar_digest IS NOT NULL)
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_event_binding_rows (
    event_binding_digest BLOB NOT NULL,
    event_observation_digest BLOB NOT NULL,
    canonical_row_ordinal INTEGER NOT NULL CHECK (canonical_row_ordinal BETWEEN 0 AND 63),
    canonical_event_digest BLOB NOT NULL CHECK (
        length(canonical_event_digest) = 32
        AND canonical_event_digest <> zeroblob(32)
    ),
    native_semantic_payload BLOB NOT NULL CHECK (
        length(native_semantic_payload) BETWEEN 1 AND 65536
    ),
    native_semantic_digest BLOB NOT NULL CHECK (
        length(native_semantic_digest) = 32
        AND native_semantic_digest <> zeroblob(32)
    ),
    event_frame_ordinal INTEGER NOT NULL CHECK (event_frame_ordinal BETWEEN 0 AND 63),
    physical_frame_ordinal INTEGER NOT NULL CHECK (
        physical_frame_ordinal BETWEEN 0 AND 63
    ),
    event_id BLOB NOT NULL CHECK (length(event_id) = 16 AND event_id <> zeroblob(16)),
    connection_id BLOB NOT NULL CHECK (
        length(connection_id) = 16 AND connection_id <> zeroblob(16)
    ),
    payload_digest BLOB NOT NULL CHECK (
        length(payload_digest) = 32 AND payload_digest <> zeroblob(32)
    ),
    exchange_at_ns INTEGER,
    received_at_ns INTEGER NOT NULL,
    source_sequence BLOB CHECK (
        source_sequence IS NULL OR length(source_sequence) = 8
    ),
    PRIMARY KEY (event_binding_digest, canonical_row_ordinal),
    FOREIGN KEY (event_binding_digest, event_observation_digest)
        REFERENCES provider_event_bindings(event_binding_digest, event_observation_digest),
    FOREIGN KEY (event_observation_digest, event_frame_ordinal)
        REFERENCES provider_event_microbatch_frames(
            event_observation_digest,
            event_frame_ordinal
        )
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_composite_response_event_bindings (
    composite_binding_digest BLOB PRIMARY KEY CHECK (
        length(composite_binding_digest) = 32
        AND composite_binding_digest <> zeroblob(32)
    ),
    response_binding_digest BLOB NOT NULL UNIQUE
        REFERENCES provider_response_market_event_bindings(response_event_binding_digest),
    event_binding_digest BLOB NOT NULL UNIQUE
        REFERENCES provider_event_bindings(event_binding_digest),
    response_row_count INTEGER NOT NULL CHECK (response_row_count BETWEEN 1 AND 64),
    event_row_count INTEGER NOT NULL CHECK (event_row_count BETWEEN 1 AND 64),
    recorded_at_ns INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TRIGGER provider_composite_response_event_bindings_guarded_insert
BEFORE INSERT ON provider_composite_response_event_bindings
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_response_market_event_bindings AS response
    JOIN provider_raw_observations AS response_capture
      ON response_capture.capture_observation_digest=response.capture_observation_digest
    JOIN provider_event_bindings AS event
      ON event.event_binding_digest=NEW.event_binding_digest
    JOIN provider_event_microbatches AS event_capture
      ON event_capture.event_observation_digest=event.event_observation_digest
    WHERE response.response_event_binding_digest=NEW.response_binding_digest
      AND response.canonical_event_count=NEW.response_row_count
      AND event.canonical_event_count=NEW.event_row_count
      AND response_capture.source_id=event_capture.source_id
      AND response_capture.source_revision_digest=event_capture.source_revision_digest
)
BEGIN
    SELECT RAISE(ABORT, 'composite response-event binding is invalid');
END;

CREATE TABLE ingest_run_provider_capture_bindings (
    run_id TEXT NOT NULL REFERENCES ingest_runs(run_id),
    input_ordinal INTEGER NOT NULL CHECK (input_ordinal BETWEEN 0 AND 4095),
    binding_digest BLOB NOT NULL UNIQUE
        REFERENCES provider_capture_bindings(binding_digest),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    PRIMARY KEY (run_id, input_ordinal),
    UNIQUE (run_id, binding_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE ingest_run_provider_publication_bindings (
    run_id TEXT NOT NULL REFERENCES ingest_runs(run_id),
    input_ordinal INTEGER NOT NULL CHECK (input_ordinal BETWEEN 0 AND 4095),
    publication_digest BLOB NOT NULL UNIQUE CHECK (
        length(publication_digest) = 32 AND publication_digest <> zeroblob(32)
    ),
    publication_kind TEXT NOT NULL CHECK (
        publication_kind IN (
            'response_market_event',
            'event_microbatch',
            'composite_response_event'
        )
    ),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    response_binding_digest BLOB
        REFERENCES provider_response_market_event_bindings(response_event_binding_digest),
    event_binding_digest BLOB REFERENCES provider_event_bindings(event_binding_digest),
    composite_binding_digest BLOB
        REFERENCES provider_composite_response_event_bindings(composite_binding_digest),
    PRIMARY KEY (run_id, input_ordinal),
    UNIQUE (run_id, publication_digest),
    CHECK (
        (publication_kind='response_market_event'
            AND response_binding_digest IS NOT NULL
            AND event_binding_digest IS NULL
            AND composite_binding_digest IS NULL
            AND publication_digest=response_binding_digest)
        OR (publication_kind='event_microbatch'
            AND response_binding_digest IS NULL
            AND event_binding_digest IS NOT NULL
            AND composite_binding_digest IS NULL
            AND publication_digest=event_binding_digest)
        OR (publication_kind='composite_response_event'
            AND response_binding_digest IS NOT NULL
            AND event_binding_digest IS NOT NULL
            AND composite_binding_digest IS NOT NULL
            AND publication_digest=composite_binding_digest)
    )
) STRICT, WITHOUT ROWID;

CREATE TRIGGER ingest_run_provider_publication_bindings_guarded_insert
BEFORE INSERT ON ingest_run_provider_publication_bindings
WHEN NOT EXISTS (
    SELECT 1
    FROM ingest_runs AS run
    WHERE run.run_id=NEW.run_id
      AND run.state='reserved'
      AND run.operation='persist'
      AND run.source_id=NEW.source_id
      AND (
          (NEW.publication_kind='response_market_event' AND EXISTS (
              SELECT 1
              FROM provider_response_market_event_bindings AS response
              JOIN provider_raw_observations AS capture
                ON capture.capture_observation_digest=response.capture_observation_digest
              JOIN provider_response_market_event_binding_native_lineage AS native
                ON native.response_event_binding_digest=response.response_event_binding_digest
              WHERE response.response_event_binding_digest=NEW.response_binding_digest
                AND NEW.publication_digest=response.response_event_binding_digest
                AND capture.source_id=NEW.source_id
                AND native.row_count=response.canonical_event_count
                AND (SELECT COUNT(*)
                     FROM provider_response_market_event_binding_rows AS row
                     WHERE row.response_event_binding_digest=response.response_event_binding_digest)
                    = response.canonical_event_count
          ))
          OR (NEW.publication_kind='event_microbatch' AND EXISTS (
              SELECT 1
              FROM provider_event_bindings AS event
              JOIN provider_event_microbatches AS capture
                ON capture.event_observation_digest=event.event_observation_digest
              JOIN provider_event_binding_native_lineage AS native
                ON native.event_binding_digest=event.event_binding_digest
              WHERE event.event_binding_digest=NEW.event_binding_digest
                AND NEW.publication_digest=event.event_binding_digest
                AND capture.source_id=NEW.source_id
                AND native.row_count=event.canonical_event_count
                AND (SELECT COUNT(*) FROM provider_event_binding_rows AS row
                     WHERE row.event_binding_digest=event.event_binding_digest)
                    = event.canonical_event_count
          ))
          OR (NEW.publication_kind='composite_response_event' AND EXISTS (
              SELECT 1 FROM provider_composite_response_event_bindings AS composite
              WHERE composite.composite_binding_digest=NEW.composite_binding_digest
                AND composite.response_binding_digest=NEW.response_binding_digest
                AND composite.event_binding_digest=NEW.event_binding_digest
                AND NEW.publication_digest=composite.composite_binding_digest
          ))
      )
)
BEGIN
    SELECT RAISE(ABORT, 'ingest-run provider event publication is invalid');
END;

CREATE TRIGGER ingest_run_provider_capture_bindings_guarded_insert
BEFORE INSERT ON ingest_run_provider_capture_bindings
WHEN NOT EXISTS (
    SELECT 1
    FROM ingest_runs AS run
    JOIN provider_capture_bindings AS binding
      ON binding.binding_digest = NEW.binding_digest
    JOIN provider_raw_observations AS capture
      ON capture.capture_observation_digest = binding.capture_observation_digest
    JOIN provider_capture_binding_native_lineage AS native
      ON native.binding_digest = binding.binding_digest
    WHERE run.run_id = NEW.run_id
      AND run.state = 'reserved'
      AND run.operation = 'persist'
      AND run.source_id = NEW.source_id
      AND capture.source_id = NEW.source_id
      AND native.row_count = binding.canonical_record_count
      AND (SELECT COUNT(*) FROM provider_capture_binding_rows AS row
           WHERE row.binding_digest = binding.binding_digest)
          = binding.canonical_record_count
)
BEGIN
    SELECT RAISE(ABORT, 'ingest-run provider capture binding is invalid');
END;

CREATE TABLE analytical_generation_provider_capture_bindings (
    generation_sequence INTEGER NOT NULL
        REFERENCES analytical_generations(generation_sequence),
    input_ordinal INTEGER NOT NULL CHECK (input_ordinal BETWEEN 0 AND 4095),
    binding_digest BLOB NOT NULL
        REFERENCES provider_capture_bindings(binding_digest),
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    PRIMARY KEY (generation_sequence, input_ordinal),
    UNIQUE (generation_sequence, binding_digest),
    FOREIGN KEY (run_id, binding_digest)
        REFERENCES ingest_run_provider_capture_bindings(run_id, binding_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE analytical_generation_provider_publication_bindings (
    generation_sequence INTEGER NOT NULL
        REFERENCES analytical_generations(generation_sequence),
    input_ordinal INTEGER NOT NULL CHECK (input_ordinal BETWEEN 0 AND 4095),
    publication_digest BLOB NOT NULL CHECK (
        length(publication_digest) = 32 AND publication_digest <> zeroblob(32)
    ),
    publication_kind TEXT NOT NULL CHECK (
        publication_kind IN (
            'response_market_event',
            'event_microbatch',
            'composite_response_event'
        )
    ),
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    PRIMARY KEY (generation_sequence, input_ordinal),
    UNIQUE (generation_sequence, publication_digest),
    FOREIGN KEY (run_id, publication_digest)
        REFERENCES ingest_run_provider_publication_bindings(run_id, publication_digest)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER analytical_generation_provider_publication_bindings_guarded_insert
BEFORE INSERT ON analytical_generation_provider_publication_bindings
WHEN NOT EXISTS (
    SELECT 1
    FROM analytical_generations AS generation
    JOIN analytical_generation_source_inputs AS source_input
      ON source_input.generation_sequence=generation.generation_sequence
    JOIN ingest_run_provider_publication_bindings AS publication
      ON publication.run_id=source_input.run_id
    WHERE generation.generation_sequence=NEW.generation_sequence
      AND generation.generation_kind='ingest'
      AND publication.publication_digest=NEW.publication_digest
      AND publication.publication_kind=NEW.publication_kind
      AND publication.run_id=NEW.run_id
      AND publication.source_id=NEW.source_id
)
AND NOT EXISTS (
    SELECT 1
    FROM analytical_generations AS child
    JOIN analytical_generation_parents AS edge
      ON edge.child_dataset_id=child.dataset_id
     AND edge.child_manifest_version=child.manifest_version
    JOIN analytical_generations AS parent
      ON parent.generation_sequence=edge.parent_generation_sequence
    JOIN analytical_generation_provider_publication_bindings AS parent_input
      ON parent_input.generation_sequence=parent.generation_sequence
    WHERE child.generation_sequence=NEW.generation_sequence
      AND parent_input.publication_digest=NEW.publication_digest
      AND parent_input.publication_kind=NEW.publication_kind
      AND parent_input.run_id=NEW.run_id
      AND parent_input.source_id=NEW.source_id
)
BEGIN
    SELECT RAISE(ABORT, 'analytical generation provider event publication is invalid');
END;

CREATE TRIGGER analytical_generation_provider_capture_bindings_guarded_insert
BEFORE INSERT ON analytical_generation_provider_capture_bindings
WHEN NOT EXISTS (
    SELECT 1
    FROM analytical_generations AS generation
    JOIN analytical_generation_source_inputs AS source_input
      ON source_input.generation_sequence = generation.generation_sequence
    JOIN ingest_run_provider_capture_bindings AS capture_input
      ON capture_input.run_id = source_input.run_id
    WHERE generation.generation_sequence = NEW.generation_sequence
      AND generation.generation_kind = 'ingest'
      AND capture_input.binding_digest = NEW.binding_digest
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
    JOIN analytical_generation_provider_capture_bindings AS parent_input
      ON parent_input.generation_sequence = parent.generation_sequence
    WHERE child.generation_sequence = NEW.generation_sequence
      AND parent_input.binding_digest = NEW.binding_digest
      AND parent_input.run_id = NEW.run_id
      AND parent_input.source_id = NEW.source_id
)
BEGIN
    SELECT RAISE(ABORT, 'analytical generation provider capture binding is invalid');
END;

CREATE TRIGGER provider_raw_observations_immutable_update
BEFORE UPDATE ON provider_raw_observations BEGIN
    SELECT RAISE(ABORT, 'provider raw observations are immutable');
END;

CREATE TRIGGER provider_raw_observations_immutable_delete
BEFORE DELETE ON provider_raw_observations BEGIN
    SELECT RAISE(ABORT, 'provider raw observations are immutable');
END;

CREATE TRIGGER provider_raw_observation_pages_immutable_update
BEFORE UPDATE ON provider_raw_observation_pages BEGIN
    SELECT RAISE(ABORT, 'provider raw-observation pages are immutable');
END;

CREATE TRIGGER provider_raw_observation_pages_immutable_delete
BEFORE DELETE ON provider_raw_observation_pages BEGIN
    SELECT RAISE(ABORT, 'provider raw-observation pages are immutable');
END;

CREATE TRIGGER provider_raw_observation_frames_immutable_update
BEFORE UPDATE ON provider_raw_observation_frames BEGIN
    SELECT RAISE(ABORT, 'provider raw-observation frames are immutable');
END;

CREATE TRIGGER provider_raw_observation_frames_immutable_delete
BEFORE DELETE ON provider_raw_observation_frames BEGIN
    SELECT RAISE(ABORT, 'provider raw-observation frames are immutable');
END;

CREATE TRIGGER sealed_raw_objects_immutable_update
BEFORE UPDATE ON sealed_raw_objects BEGIN
    SELECT RAISE(ABORT, 'sealed raw objects are immutable');
END;

CREATE TRIGGER sealed_raw_objects_immutable_delete
BEFORE DELETE ON sealed_raw_objects BEGIN
    SELECT RAISE(ABORT, 'sealed raw objects are immutable');
END;

CREATE TRIGGER provider_raw_observation_objects_immutable_update
BEFORE UPDATE ON provider_raw_observation_objects BEGIN
    SELECT RAISE(ABORT, 'provider raw-observation objects are immutable');
END;

CREATE TRIGGER provider_raw_observation_objects_immutable_delete
BEFORE DELETE ON provider_raw_observation_objects BEGIN
    SELECT RAISE(ABORT, 'provider raw-observation objects are immutable');
END;

CREATE TRIGGER provider_capture_bindings_immutable_update
BEFORE UPDATE ON provider_capture_bindings BEGIN
    SELECT RAISE(ABORT, 'provider capture bindings are immutable');
END;

CREATE TRIGGER provider_capture_bindings_immutable_delete
BEFORE DELETE ON provider_capture_bindings BEGIN
    SELECT RAISE(ABORT, 'provider capture bindings are immutable');
END;

CREATE TRIGGER provider_capture_binding_native_lineage_immutable_update
BEFORE UPDATE ON provider_capture_binding_native_lineage BEGIN
    SELECT RAISE(ABORT, 'provider capture native lineage is immutable');
END;

CREATE TRIGGER provider_capture_binding_native_lineage_immutable_delete
BEFORE DELETE ON provider_capture_binding_native_lineage BEGIN
    SELECT RAISE(ABORT, 'provider capture native lineage is immutable');
END;

CREATE TRIGGER provider_capture_binding_objects_immutable_update
BEFORE UPDATE ON provider_capture_binding_objects BEGIN
    SELECT RAISE(ABORT, 'provider capture binding objects are immutable');
END;

CREATE TRIGGER provider_capture_binding_objects_immutable_delete
BEFORE DELETE ON provider_capture_binding_objects BEGIN
    SELECT RAISE(ABORT, 'provider capture binding objects are immutable');
END;

CREATE TRIGGER provider_capture_binding_rows_immutable_update
BEFORE UPDATE ON provider_capture_binding_rows BEGIN
    SELECT RAISE(ABORT, 'provider capture binding rows are immutable');
END;

CREATE TRIGGER provider_capture_binding_rows_immutable_delete
BEFORE DELETE ON provider_capture_binding_rows BEGIN
    SELECT RAISE(ABORT, 'provider capture binding rows are immutable');
END;

CREATE TRIGGER provider_event_microbatches_immutable_update
BEFORE UPDATE ON provider_event_microbatches BEGIN
    SELECT RAISE(ABORT, 'provider event microbatches are immutable');
END;

CREATE TRIGGER provider_response_market_event_bindings_immutable_update
BEFORE UPDATE ON provider_response_market_event_bindings BEGIN
    SELECT RAISE(ABORT, 'provider response-market-event bindings are immutable');
END;

CREATE TRIGGER provider_response_market_event_bindings_immutable_delete
BEFORE DELETE ON provider_response_market_event_bindings BEGIN
    SELECT RAISE(ABORT, 'provider response-market-event bindings are immutable');
END;

CREATE TRIGGER provider_response_market_event_binding_native_lineage_immutable_update
BEFORE UPDATE ON provider_response_market_event_binding_native_lineage BEGIN
    SELECT RAISE(ABORT, 'provider response-market-event native lineage is immutable');
END;

CREATE TRIGGER provider_response_market_event_binding_native_lineage_immutable_delete
BEFORE DELETE ON provider_response_market_event_binding_native_lineage BEGIN
    SELECT RAISE(ABORT, 'provider response-market-event native lineage is immutable');
END;

CREATE TRIGGER provider_response_market_event_binding_rows_immutable_update
BEFORE UPDATE ON provider_response_market_event_binding_rows BEGIN
    SELECT RAISE(ABORT, 'provider response-market-event rows are immutable');
END;

CREATE TRIGGER provider_response_market_event_binding_rows_immutable_delete
BEFORE DELETE ON provider_response_market_event_binding_rows BEGIN
    SELECT RAISE(ABORT, 'provider response-market-event rows are immutable');
END;

CREATE TRIGGER provider_event_microbatches_immutable_delete
BEFORE DELETE ON provider_event_microbatches BEGIN
    SELECT RAISE(ABORT, 'provider event microbatches are immutable');
END;

CREATE TRIGGER provider_event_microbatch_frames_immutable_update
BEFORE UPDATE ON provider_event_microbatch_frames BEGIN
    SELECT RAISE(ABORT, 'provider event microbatch frames are immutable');
END;

CREATE TRIGGER provider_event_microbatch_frames_immutable_delete
BEFORE DELETE ON provider_event_microbatch_frames BEGIN
    SELECT RAISE(ABORT, 'provider event microbatch frames are immutable');
END;

CREATE TRIGGER provider_event_microbatch_objects_immutable_update
BEFORE UPDATE ON provider_event_microbatch_objects BEGIN
    SELECT RAISE(ABORT, 'provider event microbatch objects are immutable');
END;

CREATE TRIGGER provider_event_microbatch_objects_immutable_delete
BEFORE DELETE ON provider_event_microbatch_objects BEGIN
    SELECT RAISE(ABORT, 'provider event microbatch objects are immutable');
END;

CREATE TRIGGER provider_event_bindings_immutable_update
BEFORE UPDATE ON provider_event_bindings BEGIN
    SELECT RAISE(ABORT, 'provider event bindings are immutable');
END;

CREATE TRIGGER provider_event_bindings_immutable_delete
BEFORE DELETE ON provider_event_bindings BEGIN
    SELECT RAISE(ABORT, 'provider event bindings are immutable');
END;

CREATE TRIGGER provider_event_binding_native_lineage_immutable_update
BEFORE UPDATE ON provider_event_binding_native_lineage BEGIN
    SELECT RAISE(ABORT, 'provider event native lineage is immutable');
END;

CREATE TRIGGER provider_event_binding_native_lineage_immutable_delete
BEFORE DELETE ON provider_event_binding_native_lineage BEGIN
    SELECT RAISE(ABORT, 'provider event native lineage is immutable');
END;

CREATE TRIGGER provider_event_binding_rows_immutable_update
BEFORE UPDATE ON provider_event_binding_rows BEGIN
    SELECT RAISE(ABORT, 'provider event binding rows are immutable');
END;

CREATE TRIGGER provider_event_binding_rows_immutable_delete
BEFORE DELETE ON provider_event_binding_rows BEGIN
    SELECT RAISE(ABORT, 'provider event binding rows are immutable');
END;

CREATE TRIGGER provider_composite_response_event_bindings_immutable_update
BEFORE UPDATE ON provider_composite_response_event_bindings BEGIN
    SELECT RAISE(ABORT, 'provider composite response-event bindings are immutable');
END;

CREATE TRIGGER provider_composite_response_event_bindings_immutable_delete
BEFORE DELETE ON provider_composite_response_event_bindings BEGIN
    SELECT RAISE(ABORT, 'provider composite response-event bindings are immutable');
END;

CREATE TRIGGER ingest_run_provider_capture_bindings_immutable_update
BEFORE UPDATE ON ingest_run_provider_capture_bindings BEGIN
    SELECT RAISE(ABORT, 'ingest-run provider capture bindings are immutable');
END;

CREATE TRIGGER ingest_run_provider_capture_bindings_immutable_delete
BEFORE DELETE ON ingest_run_provider_capture_bindings BEGIN
    SELECT RAISE(ABORT, 'ingest-run provider capture bindings are immutable');
END;

CREATE TRIGGER ingest_run_provider_publication_bindings_immutable_update
BEFORE UPDATE ON ingest_run_provider_publication_bindings BEGIN
    SELECT RAISE(ABORT, 'ingest-run provider event publications are immutable');
END;

CREATE TRIGGER ingest_run_provider_publication_bindings_immutable_delete
BEFORE DELETE ON ingest_run_provider_publication_bindings BEGIN
    SELECT RAISE(ABORT, 'ingest-run provider event publications are immutable');
END;

CREATE TRIGGER analytical_generation_provider_capture_bindings_immutable_update
BEFORE UPDATE ON analytical_generation_provider_capture_bindings BEGIN
    SELECT RAISE(ABORT, 'analytical generation provider capture bindings are immutable');
END;

CREATE TRIGGER analytical_generation_provider_capture_bindings_immutable_delete
BEFORE DELETE ON analytical_generation_provider_capture_bindings BEGIN
    SELECT RAISE(ABORT, 'analytical generation provider capture bindings are immutable');
END;

CREATE TRIGGER analytical_generation_provider_publication_bindings_immutable_update
BEFORE UPDATE ON analytical_generation_provider_publication_bindings BEGIN
    SELECT RAISE(ABORT, 'analytical generation provider event publications are immutable');
END;

CREATE TRIGGER analytical_generation_provider_publication_bindings_immutable_delete
BEFORE DELETE ON analytical_generation_provider_publication_bindings BEGIN
    SELECT RAISE(ABORT, 'analytical generation provider event publications are immutable');
END;

CREATE TABLE market_bar_history_publications (
    publication_receipt_digest BLOB PRIMARY KEY CHECK (
        length(publication_receipt_digest) = 32
        AND publication_receipt_digest <> zeroblob(32)
    ),
    receipt_version INTEGER NOT NULL CHECK (receipt_version = 1),
    origin_generation_sequence INTEGER NOT NULL UNIQUE
        REFERENCES analytical_generations(generation_sequence),
    origin_run_id TEXT NOT NULL UNIQUE REFERENCES ingest_runs(run_id),
    origin_anchor_manifest_id TEXT NOT NULL UNIQUE
        REFERENCES dataset_manifests(manifest_id),
    origin_artifact_id TEXT NOT NULL UNIQUE REFERENCES artifacts(artifact_id),
    origin_object_ordinal INTEGER NOT NULL CHECK (
        origin_object_ordinal BETWEEN 0 AND 1023
    ),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    binding_digest BLOB NOT NULL UNIQUE
        REFERENCES provider_capture_bindings(binding_digest),
    capture_receipt_digest BLOB NOT NULL UNIQUE CHECK (
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
    capture_recorded_at_ns INTEGER NOT NULL,
    provider_dataset TEXT NOT NULL CHECK (
        length(CAST(provider_dataset AS BLOB)) BETWEEN 1 AND 512
    ),
    instrument_id TEXT NOT NULL
        REFERENCES market_data_instrument_identities(instrument_id),
    asset_class TEXT NOT NULL CHECK (asset_class IN ('equity', 'fund')),
    instrument_revision_digest BLOB NOT NULL
        REFERENCES market_data_instrument_revisions(revision_digest),
    admitted_plan_digest BLOB NOT NULL CHECK (
        length(admitted_plan_digest) = 32
        AND admitted_plan_digest <> zeroblob(32)
    ),
    provider_instrument_id TEXT NOT NULL CHECK (
        length(CAST(provider_instrument_id AS BLOB)) BETWEEN 1 AND 256
    ),
    venue_id TEXT NOT NULL CHECK (
        length(CAST(venue_id AS BLOB)) BETWEEN 1 AND 128
    ),
    feed TEXT NOT NULL CHECK (length(CAST(feed AS BLOB)) BETWEEN 1 AND 128),
    bar_interval TEXT NOT NULL CHECK (
        length(CAST(bar_interval AS BLOB)) BETWEEN 1 AND 128
    ),
    adjustment TEXT NOT NULL CHECK (
        adjustment IN ('raw', 'split', 'dividend', 'spin_off', 'all')
    ),
    timestamp_basis TEXT NOT NULL CHECK (
        timestamp_basis IN ('period_start', 'period_end')
    ),
    session_kind TEXT NOT NULL CHECK (
        session_kind IN ('regular', 'extended', 'continuous', 'provider_defined')
    ),
    session_ruleset TEXT NOT NULL CHECK (
        length(CAST(session_ruleset AS BLOB)) BETWEEN 1 AND 512
    ),
    graph_purpose TEXT NOT NULL CHECK (
        length(CAST(graph_purpose AS BLOB)) BETWEEN 1 AND 512
    ),
    currency TEXT NOT NULL CHECK (
        length(CAST(currency AS BLOB)) BETWEEN 1 AND 16
    ),
    requested_start_ns INTEGER NOT NULL,
    requested_end_ns INTEGER NOT NULL CHECK (requested_end_ns > requested_start_ns),
    coverage_first_ns INTEGER NOT NULL CHECK (
        coverage_first_ns BETWEEN requested_start_ns AND requested_end_ns
    ),
    coverage_last_ns INTEGER NOT NULL CHECK (
        coverage_last_ns BETWEEN coverage_first_ns AND requested_end_ns
    ),
    coverage_last_complete_ns INTEGER NOT NULL CHECK (
        coverage_last_complete_ns >= coverage_last_ns
        AND coverage_last_complete_ns <= requested_end_ns
    ),
    expected_bar_count INTEGER NOT NULL CHECK (
        expected_bar_count BETWEEN 1 AND 10000
    ),
    returned_bar_count INTEGER NOT NULL CHECK (
        returned_bar_count = expected_bar_count
    ),
    expected_timestamp_set_digest BLOB NOT NULL CHECK (
        length(expected_timestamp_set_digest) = 32
        AND expected_timestamp_set_digest <> zeroblob(32)
    ),
    bar_set_digest BLOB NOT NULL CHECK (
        length(bar_set_digest) = 32 AND bar_set_digest <> zeroblob(32)
    ),
    completeness_evidence_digest BLOB NOT NULL CHECK (
        length(completeness_evidence_digest) = 32
        AND completeness_evidence_digest <> zeroblob(32)
    ),
    market_bar_component_ordinal INTEGER NOT NULL CHECK (
        market_bar_component_ordinal BETWEEN 0 AND 63
    ),
    market_bar_component_content_digest BLOB NOT NULL CHECK (
        length(market_bar_component_content_digest) = 32
        AND market_bar_component_content_digest <> zeroblob(32)
    ),
    market_bar_component_page_count INTEGER NOT NULL CHECK (
        market_bar_component_page_count BETWEEN 1 AND 64
    ),
    session_calendar_component_ordinal INTEGER NOT NULL CHECK (
        session_calendar_component_ordinal BETWEEN 0 AND 63
        AND session_calendar_component_ordinal <> market_bar_component_ordinal
    ),
    session_calendar_component_content_digest BLOB NOT NULL CHECK (
        length(session_calendar_component_content_digest) = 32
        AND session_calendar_component_content_digest <> zeroblob(32)
    ),
    session_calendar_component_page_count INTEGER NOT NULL CHECK (
        session_calendar_component_page_count BETWEEN 1 AND 64
    ),
    max_available_at_ns INTEGER NOT NULL,
    max_received_at_ns INTEGER NOT NULL,
    max_ingested_at_ns INTEGER NOT NULL CHECK (
        max_ingested_at_ns >= max_available_at_ns
        AND max_ingested_at_ns >= max_received_at_ns
    ),
    published_at_ns INTEGER NOT NULL CHECK (published_at_ns >= max_ingested_at_ns),
    admission_class TEXT NOT NULL CHECK (
        admission_class = 'current_research_only'
    ),
    current_research_eligible INTEGER NOT NULL CHECK (current_research_eligible = 1),
    point_in_time_eligible INTEGER NOT NULL CHECK (point_in_time_eligible = 0),
    backtest_eligible INTEGER NOT NULL CHECK (backtest_eligible = 0),
    retrospective_training_eligible INTEGER NOT NULL CHECK (
        retrospective_training_eligible = 0
    ),
    admission_reason TEXT NOT NULL CHECK (
        admission_reason = 'local_first_observed_without_provider_publication_time'
    ),
    receipt_json TEXT NOT NULL CHECK (
        length(CAST(receipt_json AS BLOB)) BETWEEN 2 AND 4194304
        AND json_valid(receipt_json)
    ),
    UNIQUE (origin_generation_sequence, binding_digest),
    FOREIGN KEY (origin_generation_sequence, binding_digest)
        REFERENCES analytical_generation_provider_capture_bindings(
            generation_sequence, binding_digest
        )
) STRICT, WITHOUT ROWID;

CREATE TABLE analytical_generation_market_bar_history_inputs (
    generation_sequence INTEGER NOT NULL
        REFERENCES analytical_generations(generation_sequence),
    input_ordinal INTEGER NOT NULL CHECK (input_ordinal BETWEEN 0 AND 4095),
    publication_receipt_digest BLOB NOT NULL
        REFERENCES market_bar_history_publications(publication_receipt_digest),
    PRIMARY KEY (generation_sequence, input_ordinal),
    UNIQUE (generation_sequence, publication_receipt_digest)
) STRICT, WITHOUT ROWID;

CREATE INDEX market_bar_history_publications_latest
ON market_bar_history_publications(
    instrument_id,
    published_at_ns DESC,
    origin_generation_sequence DESC,
    publication_receipt_digest
);

CREATE INDEX analytical_generation_market_bar_history_receipt
ON analytical_generation_market_bar_history_inputs(
    publication_receipt_digest,
    generation_sequence
);

CREATE TRIGGER market_bar_history_publications_guarded_insert
BEFORE INSERT ON market_bar_history_publications
WHEN NOT EXISTS (
    SELECT 1
    FROM analytical_generations AS generation
    JOIN analytical_generation_source_inputs AS source_input
      ON source_input.generation_sequence = generation.generation_sequence
    JOIN ingest_runs AS run ON run.run_id = source_input.run_id
    JOIN dataset_manifests AS manifest
      ON manifest.manifest_id = generation.anchor_manifest_id
    JOIN artifacts AS artifact ON artifact.artifact_id = manifest.artifact_id
    JOIN analytical_generation_objects AS object
      ON object.dataset_id = generation.dataset_id
     AND object.manifest_version = generation.manifest_version
     AND object.ordinal = NEW.origin_object_ordinal
    JOIN analytical_generation_provider_capture_bindings AS capture_input
      ON capture_input.generation_sequence = generation.generation_sequence
     AND capture_input.run_id = run.run_id
    JOIN provider_capture_bindings AS binding
      ON binding.binding_digest = capture_input.binding_digest
    JOIN provider_raw_observations AS capture
      ON capture.capture_observation_digest = binding.capture_observation_digest
    JOIN market_data_instrument_revisions AS instrument_revision
      ON instrument_revision.revision_digest = NEW.instrument_revision_digest
     AND instrument_revision.instrument_id = NEW.instrument_id
    WHERE generation.generation_sequence = NEW.origin_generation_sequence
      AND generation.generation_kind = 'ingest'
      AND run.run_id = NEW.origin_run_id
      AND run.state = 'reserved'
      AND run.operation = 'persist'
      AND run.source_id = NEW.source_id
      AND generation.anchor_manifest_id = NEW.origin_anchor_manifest_id
      AND artifact.artifact_id = NEW.origin_artifact_id
      AND object.artifact_id = NEW.origin_artifact_id
      AND object.row_count = NEW.returned_bar_count
      AND capture_input.binding_digest = NEW.binding_digest
      AND capture_input.source_id = NEW.source_id
      AND capture.source_id = NEW.source_id
      AND capture.provider_dataset = NEW.provider_dataset
      AND capture.terminal_disposition = 'complete_request_graph'
      AND capture.capture_content_digest = NEW.capture_content_digest
      AND capture.capture_observation_digest = NEW.capture_observation_digest
      AND capture.recorded_at_ns = NEW.capture_recorded_at_ns
      AND instrument_revision.published_at_ns <= run.requested_at_ns
      AND instrument_revision.effective_start_ns <= NEW.requested_start_ns
      AND (
          instrument_revision.effective_end_ns IS NULL
          OR NEW.requested_end_ns < instrument_revision.effective_end_ns
      )
      AND generation.created_at_ns = NEW.published_at_ns
      AND manifest.created_at_ns = NEW.published_at_ns
)
BEGIN
    SELECT RAISE(ABORT, 'market-bar history publication lineage is invalid');
END;

CREATE TRIGGER analytical_generation_market_bar_history_inputs_guarded_insert
BEFORE INSERT ON analytical_generation_market_bar_history_inputs
WHEN NOT EXISTS (
    SELECT 1
    FROM market_bar_history_publications AS publication
    JOIN analytical_generation_provider_capture_bindings AS capture_input
      ON capture_input.generation_sequence = NEW.generation_sequence
     AND capture_input.binding_digest = publication.binding_digest
    WHERE publication.publication_receipt_digest = NEW.publication_receipt_digest
      AND publication.origin_generation_sequence = NEW.generation_sequence
)
AND NOT EXISTS (
    SELECT 1
    FROM analytical_generations AS child
    JOIN analytical_generation_parents AS edge
      ON edge.child_dataset_id = child.dataset_id
     AND edge.child_manifest_version = child.manifest_version
    JOIN analytical_generation_market_bar_history_inputs AS parent_input
      ON parent_input.generation_sequence = edge.parent_generation_sequence
    JOIN market_bar_history_publications AS publication
      ON publication.publication_receipt_digest = parent_input.publication_receipt_digest
    JOIN analytical_generation_provider_capture_bindings AS capture_input
      ON capture_input.generation_sequence = NEW.generation_sequence
     AND capture_input.binding_digest = publication.binding_digest
    WHERE child.generation_sequence = NEW.generation_sequence
      AND child.generation_kind IN ('ingest', 'compaction')
      AND parent_input.publication_receipt_digest = NEW.publication_receipt_digest
)
BEGIN
    SELECT RAISE(ABORT, 'analytical generation market-bar history input is invalid');
END;

CREATE TRIGGER market_bar_history_publications_immutable_update
BEFORE UPDATE ON market_bar_history_publications BEGIN
    SELECT RAISE(ABORT, 'market-bar history publications are immutable');
END;

CREATE TRIGGER market_bar_history_publications_immutable_delete
BEFORE DELETE ON market_bar_history_publications BEGIN
    SELECT RAISE(ABORT, 'market-bar history publications are immutable');
END;

CREATE TRIGGER analytical_generation_market_bar_history_inputs_immutable_update
BEFORE UPDATE ON analytical_generation_market_bar_history_inputs BEGIN
    SELECT RAISE(ABORT, 'analytical generation market-bar history inputs are immutable');
END;

CREATE TRIGGER analytical_generation_market_bar_history_inputs_immutable_delete
BEFORE DELETE ON analytical_generation_market_bar_history_inputs BEGIN
    SELECT RAISE(ABORT, 'analytical generation market-bar history inputs are immutable');
END;
