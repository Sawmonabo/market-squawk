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
            X'ff8f8b2c282a4386b1a0075da64aaf692fe6dc7a8c72fc648fed6aa24d44b1d2'
    ) OR (
        NEW.schema_name = 'market_squawk.feature_label_components'
        AND NEW.schema_version = 3
        AND NEW.schema_fingerprint =
            X'9f66fb43d2269bbab58354505292c545cfeed08c8490c4c3e64fc7f4634860e9'
    ) OR (
        NEW.schema_name = 'market_squawk.market_events'
        AND NEW.schema_version = 1
        AND NEW.schema_fingerprint =
            X'e0bf8cc9a74c880cc772d3987907b13eb3d4d8fc2dc3ca1a239873d650a151f0'
    )
) BEGIN
    SELECT RAISE(ABORT, 'analytical generation schema identity is not registered');
END;

CREATE TABLE market_data_instrument_identities (
    instrument_id TEXT PRIMARY KEY CHECK (
        length(CAST(instrument_id AS BLOB)) = 36
    ),
    created_at_ns INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE market_data_instrument_revisions (
    revision_digest BLOB PRIMARY KEY CHECK (
        length(revision_digest) = 32
        AND revision_digest <> zeroblob(32)
    ),
    instrument_id TEXT NOT NULL,
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
    definition_json TEXT NOT NULL CHECK (
        length(CAST(definition_json AS BLOB)) BETWEEN 2 AND 16777216
    ),
    published_at_ns INTEGER NOT NULL,
    UNIQUE (instrument_id, revision_sequence),
    UNIQUE (instrument_id, effective_start_ns),
    UNIQUE (revision_digest, instrument_id),
    FOREIGN KEY (instrument_id)
        REFERENCES market_data_instrument_identities(instrument_id),
    CHECK (effective_end_ns IS NULL OR effective_end_ns > effective_start_ns),
    CHECK (
        (revision_sequence = 1 AND previous_revision_digest IS NULL)
        OR (revision_sequence > 1 AND previous_revision_digest IS NOT NULL)
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE market_data_instrument_current (
    instrument_id TEXT PRIMARY KEY,
    revision_digest BLOB NOT NULL UNIQUE,
    advanced_at_ns INTEGER NOT NULL,
    FOREIGN KEY (instrument_id)
        REFERENCES market_data_instrument_identities(instrument_id),
    FOREIGN KEY (revision_digest, instrument_id)
        REFERENCES market_data_instrument_revisions(
            revision_digest,
            instrument_id
        )
) STRICT, WITHOUT ROWID;

CREATE TABLE market_data_instrument_search_terms (
    revision_digest BLOB NOT NULL
        REFERENCES market_data_instrument_revisions(revision_digest),
    term_kind TEXT NOT NULL CHECK (
        term_kind IN (
            'external_identifier', 'display_name', 'venue_symbol', 'provider_symbol'
        )
    ),
    term_ordinal INTEGER NOT NULL CHECK (term_ordinal BETWEEN 0 AND 255),
    normalized_term TEXT NOT NULL CHECK (
        length(CAST(normalized_term AS BLOB)) BETWEEN 1 AND 512
    ),
    display_term TEXT NOT NULL CHECK (
        length(CAST(display_term AS BLOB)) BETWEEN 1 AND 512
    ),
    source_id TEXT CHECK (
        source_id IS NULL
        OR length(CAST(source_id AS BLOB)) BETWEEN 1 AND 128
    ),
    effective_start_ns INTEGER NOT NULL,
    effective_end_ns INTEGER,
    PRIMARY KEY (revision_digest, term_kind, term_ordinal),
    UNIQUE (
        revision_digest, term_kind, normalized_term, display_term,
        source_id, effective_start_ns, effective_end_ns
    ),
    CHECK (effective_end_ns IS NULL OR effective_end_ns > effective_start_ns),
    CHECK (
        (term_kind = 'provider_symbol' AND source_id IS NOT NULL)
        OR (term_kind <> 'provider_symbol' AND source_id IS NULL)
    )
) STRICT, WITHOUT ROWID;

CREATE INDEX market_data_instrument_search_lookup
ON market_data_instrument_search_terms(
    normalized_term, term_kind, source_id,
    effective_start_ns, effective_end_ns, display_term
);

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
    OR NEW.revision_digest = OLD.revision_digest
    OR NEW.advanced_at_ns < OLD.advanced_at_ns
    OR NOT EXISTS (
        SELECT 1
        FROM market_data_instrument_revisions AS successor
        JOIN market_data_instrument_revisions AS predecessor
          ON predecessor.revision_digest = OLD.revision_digest
        WHERE successor.revision_digest = NEW.revision_digest
          AND successor.instrument_id = OLD.instrument_id
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
    FOREIGN KEY (market_revision_digest, instrument_id)
        REFERENCES market_data_instrument_revisions(
            revision_digest, instrument_id
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
    raw_claim_kind TEXT NOT NULL CHECK (
        raw_claim_kind IN ('journal_segment', 'logical_object')
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
    size_bytes INTEGER NOT NULL CHECK (size_bytes BETWEEN 1 AND 68719476736),
    integrity_chunk_bytes INTEGER CHECK (
        integrity_chunk_bytes IS NULL
        OR integrity_chunk_bytes BETWEEN 1 AND 16777216
    ),
    unit_count INTEGER NOT NULL CHECK (unit_count BETWEEN 1 AND 4096),
    raw_claim_json TEXT NOT NULL CHECK (
        length(CAST(raw_claim_json AS BLOB)) BETWEEN 2 AND 2097152
        AND json_valid(raw_claim_json)
    ),
    recorded_at_ns INTEGER NOT NULL,
    UNIQUE (raw_claim_digest, physical_receipt_digest),
    CHECK (
        (raw_claim_kind = 'journal_segment'
            AND size_bytes <= 536870912
            AND integrity_chunk_bytes IS NULL
            AND unit_count <= 64)
        OR (raw_claim_kind = 'logical_object'
            AND integrity_chunk_bytes IS NOT NULL
            AND size_bytes <= unit_count * integrity_chunk_bytes
            AND size_bytes > (unit_count - 1) * integrity_chunk_bytes)
    )
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
    identity_selection BLOB CHECK (
        identity_selection IS NULL OR length(identity_selection) BETWEEN 1 AND 65536
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
    identity_selection BLOB CHECK (
        identity_selection IS NULL OR length(identity_selection) BETWEEN 1 AND 65536
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

-- One sealed option response is published as a coherent batch. The canonical count may be zero;
-- its mandatory Parquet batch-header row keeps the empty or unavailable response immutable.
CREATE TABLE provider_option_market_bindings (
    option_binding_digest BLOB PRIMARY KEY CHECK (
        length(option_binding_digest) = 32 AND option_binding_digest <> zeroblob(32)
    ),
    binding_format_version INTEGER NOT NULL CHECK (binding_format_version = 1),
    capture_observation_digest BLOB NOT NULL
        REFERENCES provider_raw_observations(capture_observation_digest),
    sealed_capture_receipt_digest BLOB NOT NULL CHECK (
        length(sealed_capture_receipt_digest) = 32
        AND sealed_capture_receipt_digest <> zeroblob(32)
    ),
    publication_kind TEXT NOT NULL CHECK (
        publication_kind IN ('option_snapshots', 'option_expirations')
    ),
    canonical_schema_fingerprint BLOB NOT NULL CHECK (
        length(canonical_schema_fingerprint) = 32
        AND canonical_schema_fingerprint <> zeroblob(32)
    ),
    canonical_content_digest BLOB NOT NULL CHECK (
        length(canonical_content_digest) = 32
        AND canonical_content_digest <> zeroblob(32)
    ),
    canonical_row_count INTEGER NOT NULL CHECK (
        canonical_row_count BETWEEN 0 AND 100000
    ),
    scope_json BLOB NOT NULL CHECK (length(scope_json) BETWEEN 2 AND 67108864),
    scope_digest BLOB NOT NULL CHECK (
        length(scope_digest) = 32 AND scope_digest <> zeroblob(32)
    ),
    completeness_json BLOB NOT NULL CHECK (
        length(completeness_json) BETWEEN 2 AND 1048576
    ),
    completeness_digest BLOB NOT NULL CHECK (
        length(completeness_digest) = 32 AND completeness_digest <> zeroblob(32)
    ),
    filter_json BLOB NOT NULL CHECK (length(filter_json) BETWEEN 2 AND 4194304),
    filter_digest BLOB NOT NULL CHECK (
        length(filter_digest) = 32 AND filter_digest <> zeroblob(32)
    ),
    underlying_instrument_id BLOB NOT NULL CHECK (length(underlying_instrument_id) = 16),
    available_at_ns INTEGER NOT NULL,
    received_at_ns INTEGER NOT NULL,
    ingested_at_ns INTEGER NOT NULL,
    disposition TEXT NOT NULL CHECK (disposition IN ('complete', 'unavailable')),
    row_mapping_digest BLOB NOT NULL CHECK (
        length(row_mapping_digest) = 32 AND row_mapping_digest <> zeroblob(32)
    ),
    reference_dependencies_json TEXT NOT NULL CHECK (
        length(CAST(reference_dependencies_json AS BLOB)) BETWEEN 2 AND 8388608
        AND json_valid(reference_dependencies_json)
        AND json_type(reference_dependencies_json)='array'
        AND json_array_length(reference_dependencies_json)<=32
    ),
    recorded_at_ns INTEGER NOT NULL,
    UNIQUE (option_binding_digest, capture_observation_digest),
    CHECK (available_at_ns <= ingested_at_ns AND received_at_ns <= ingested_at_ns)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER provider_option_market_bindings_guarded_insert
BEFORE INSERT ON provider_option_market_bindings
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_raw_observation_objects AS object
    WHERE object.capture_observation_digest=NEW.capture_observation_digest
      AND object.capture_receipt_digest=NEW.sealed_capture_receipt_digest
)
BEGIN
    SELECT RAISE(ABORT, 'provider option-market binding lacks its sealed response object');
END;

CREATE TABLE provider_option_market_binding_native_lineage (
    option_binding_digest BLOB PRIMARY KEY
        REFERENCES provider_option_market_bindings(option_binding_digest),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    implementation TEXT NOT NULL CHECK (
        length(CAST(implementation AS BLOB)) BETWEEN 1 AND 128
    ),
    schema_fingerprint BLOB NOT NULL CHECK (
        length(schema_fingerprint) = 32 AND schema_fingerprint <> zeroblob(32)
    ),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 0 AND 100000),
    batch_digest BLOB NOT NULL CHECK (
        length(batch_digest) = 32 AND batch_digest <> zeroblob(32)
    ),
    batch_sidecar_payload BLOB NOT NULL CHECK (
        length(batch_sidecar_payload) BETWEEN 1 AND 4194304
    ),
    batch_sidecar_digest BLOB NOT NULL CHECK (
        length(batch_sidecar_digest) = 32 AND batch_sidecar_digest <> zeroblob(32)
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_option_market_binding_rows (
    option_binding_digest BLOB NOT NULL,
    capture_observation_digest BLOB NOT NULL,
    canonical_row_ordinal INTEGER NOT NULL CHECK (
        canonical_row_ordinal BETWEEN 0 AND 99999
    ),
    canonical_row_digest BLOB NOT NULL CHECK (
        length(canonical_row_digest) = 32 AND canonical_row_digest <> zeroblob(32)
    ),
    native_semantic_payload BLOB NOT NULL CHECK (
        length(native_semantic_payload) BETWEEN 1 AND 65536
    ),
    native_semantic_digest BLOB NOT NULL CHECK (
        length(native_semantic_digest) = 32 AND native_semantic_digest <> zeroblob(32)
    ),
    capture_page_ordinal INTEGER NOT NULL CHECK (capture_page_ordinal BETWEEN 0 AND 63),
    physical_frame_ordinal INTEGER NOT NULL CHECK (physical_frame_ordinal BETWEEN 0 AND 63),
    payload_digest BLOB NOT NULL CHECK (
        length(payload_digest) = 32 AND payload_digest <> zeroblob(32)
    ),
    received_at_ns INTEGER NOT NULL,
    source_sequence BLOB CHECK (source_sequence IS NULL OR length(source_sequence) = 8),
    PRIMARY KEY (option_binding_digest, canonical_row_ordinal),
    FOREIGN KEY (option_binding_digest, capture_observation_digest)
        REFERENCES provider_option_market_bindings(
            option_binding_digest,
            capture_observation_digest
        ),
    FOREIGN KEY (capture_observation_digest, capture_page_ordinal)
        REFERENCES provider_raw_observation_pages(
            capture_observation_digest,
            page_ordinal
        )
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

-- Large provider surfaces retain one common logical-object graph. These tables bind the exact
-- terminal receipt, ordered raw objects, evidence partitions, and canonical partition
-- expectations without introducing a provider-specific or second physical raw-object store.
CREATE TABLE provider_logical_publication_bindings (
    binding_digest BLOB PRIMARY KEY CHECK (
        length(binding_digest) = 32 AND binding_digest <> zeroblob(32)
    ),
    binding_format_version INTEGER NOT NULL CHECK (binding_format_version = 1),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    terminal_receipt_digest BLOB NOT NULL UNIQUE CHECK (
        length(terminal_receipt_digest) = 32
        AND terminal_receipt_digest <> zeroblob(32)
    ),
    terminal_json BLOB NOT NULL CHECK (
        length(terminal_json) BETWEEN 2 AND 2097152
        AND json_valid(terminal_json)
    ),
    required_family_count INTEGER NOT NULL CHECK (
        required_family_count BETWEEN 1 AND 6
    ),
    object_count INTEGER NOT NULL CHECK (object_count BETWEEN 1 AND 64),
    partition_count INTEGER NOT NULL CHECK (partition_count BETWEEN 1 AND 4096),
    canonical_partition_count INTEGER NOT NULL CHECK (
        canonical_partition_count BETWEEN 0 AND 1024
    ),
    recorded_at_ns INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_logical_publication_required_families (
    binding_digest BLOB NOT NULL
        REFERENCES provider_logical_publication_bindings(binding_digest),
    family_ordinal INTEGER NOT NULL CHECK (family_ordinal BETWEEN 0 AND 5),
    family TEXT NOT NULL CHECK (
        family IN (
            'decoded_event', 'provider_native', 'canonical_row_map',
            'resolver_assertion', 'resolver_outcome', 'resolver_conflict'
        )
    ),
    PRIMARY KEY (binding_digest, family_ordinal),
    UNIQUE (binding_digest, family),
    UNIQUE (binding_digest, family_ordinal, family)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_logical_publication_objects (
    binding_digest BLOB NOT NULL
        REFERENCES provider_logical_publication_bindings(binding_digest),
    object_ordinal INTEGER NOT NULL CHECK (object_ordinal BETWEEN 0 AND 63),
    object_role TEXT NOT NULL CHECK (
        object_role IN (
            'catalog', 'provider_payload', 'expanded_payload', 'provider_component'
        )
    ),
    semantic_identity BLOB NOT NULL CHECK (
        length(semantic_identity) = 32 AND semantic_identity <> zeroblob(32)
    ),
    raw_claim_digest BLOB NOT NULL REFERENCES sealed_raw_objects(raw_claim_digest),
    physical_receipt_digest BLOB NOT NULL CHECK (
        length(physical_receipt_digest) = 32
        AND physical_receipt_digest <> zeroblob(32)
    ),
    PRIMARY KEY (binding_digest, object_ordinal),
    UNIQUE (binding_digest, raw_claim_digest, physical_receipt_digest),
    FOREIGN KEY (raw_claim_digest, physical_receipt_digest)
        REFERENCES sealed_raw_objects(raw_claim_digest, physical_receipt_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_logical_publication_partitions (
    binding_digest BLOB NOT NULL
        REFERENCES provider_logical_publication_bindings(binding_digest),
    partition_family_ordinal INTEGER NOT NULL CHECK (
        partition_family_ordinal BETWEEN 0 AND 5
    ),
    partition_family TEXT NOT NULL CHECK (
        partition_family IN (
            'decoded_event', 'provider_native', 'canonical_row_map',
            'resolver_assertion', 'resolver_outcome', 'resolver_conflict'
        )
    ),
    partition_ordinal INTEGER NOT NULL CHECK (
        partition_ordinal BETWEEN 0 AND 4095
    ),
    first_item_ordinal INTEGER NOT NULL CHECK (first_item_ordinal >= 0),
    item_count INTEGER NOT NULL CHECK (item_count BETWEEN 1 AND 4294967295),
    schema_identity BLOB NOT NULL CHECK (
        length(schema_identity) = 32 AND schema_identity <> zeroblob(32)
    ),
    semantic_digest BLOB NOT NULL CHECK (
        length(semantic_digest) = 32 AND semantic_digest <> zeroblob(32)
    ),
    raw_claim_digest BLOB NOT NULL REFERENCES sealed_raw_objects(raw_claim_digest),
    physical_receipt_digest BLOB NOT NULL CHECK (
        length(physical_receipt_digest) = 32
        AND physical_receipt_digest <> zeroblob(32)
    ),
    PRIMARY KEY (binding_digest, partition_family_ordinal, partition_ordinal),
    UNIQUE (binding_digest, partition_family, partition_ordinal),
    UNIQUE (binding_digest, semantic_digest),
    FOREIGN KEY (binding_digest, partition_family_ordinal, partition_family)
        REFERENCES provider_logical_publication_required_families(
            binding_digest, family_ordinal, family
        ),
    FOREIGN KEY (raw_claim_digest, physical_receipt_digest)
        REFERENCES sealed_raw_objects(raw_claim_digest, physical_receipt_digest)
) STRICT, WITHOUT ROWID;

CREATE INDEX provider_logical_publication_partitions_by_raw_object
ON provider_logical_publication_partitions(raw_claim_digest, physical_receipt_digest);

CREATE TABLE provider_logical_publication_canonical_expectations (
    binding_digest BLOB NOT NULL
        REFERENCES provider_logical_publication_bindings(binding_digest),
    partition_ordinal INTEGER NOT NULL CHECK (
        partition_ordinal BETWEEN 0 AND 1023
    ),
    first_row_ordinal INTEGER NOT NULL CHECK (first_row_ordinal >= 0),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 4294967295),
    schema_identity BLOB NOT NULL CHECK (
        length(schema_identity) = 32 AND schema_identity <> zeroblob(32)
    ),
    semantic_digest BLOB NOT NULL CHECK (
        length(semantic_digest) = 32 AND semantic_digest <> zeroblob(32)
    ),
    aligned_native_partition INTEGER NOT NULL CHECK (
        aligned_native_partition BETWEEN 0 AND 4095
    ),
    aligned_row_map_partition INTEGER NOT NULL CHECK (
        aligned_row_map_partition BETWEEN 0 AND 4095
    ),
    PRIMARY KEY (binding_digest, partition_ordinal),
    UNIQUE (binding_digest, semantic_digest)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER provider_logical_publication_objects_guarded_insert
BEFORE INSERT ON provider_logical_publication_objects
WHEN NOT EXISTS (
    SELECT 1
    FROM sealed_raw_objects AS claim
    WHERE claim.raw_claim_digest=NEW.raw_claim_digest
      AND claim.physical_receipt_digest=NEW.physical_receipt_digest
      AND claim.raw_claim_kind='logical_object'
)
BEGIN
    SELECT RAISE(ABORT, 'provider logical object lacks its sealed logical raw claim');
END;

CREATE TRIGGER provider_logical_publication_partitions_guarded_insert
BEFORE INSERT ON provider_logical_publication_partitions
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_logical_publication_required_families AS family
    JOIN sealed_raw_objects AS claim
      ON claim.raw_claim_digest=NEW.raw_claim_digest
     AND claim.physical_receipt_digest=NEW.physical_receipt_digest
    WHERE family.binding_digest=NEW.binding_digest
      AND family.family_ordinal=NEW.partition_family_ordinal
      AND family.family=NEW.partition_family
      AND claim.raw_claim_kind='logical_object'
)
BEGIN
    SELECT RAISE(ABORT, 'provider logical partition evidence is invalid');
END;

CREATE TRIGGER provider_logical_publication_canonical_expectations_guarded_insert
BEFORE INSERT ON provider_logical_publication_canonical_expectations
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_logical_publication_partitions AS native
    JOIN provider_logical_publication_partitions AS row_map
      ON row_map.binding_digest=native.binding_digest
    WHERE native.binding_digest=NEW.binding_digest
      AND native.partition_family='provider_native'
      AND native.partition_ordinal=NEW.aligned_native_partition
      AND row_map.partition_family='canonical_row_map'
      AND row_map.partition_ordinal=NEW.aligned_row_map_partition
      AND native.first_item_ordinal=NEW.first_row_ordinal
      AND native.item_count=NEW.row_count
      AND row_map.first_item_ordinal=NEW.first_row_ordinal
      AND row_map.item_count=NEW.row_count
)
BEGIN
    SELECT RAISE(ABORT, 'provider logical canonical alignment is invalid');
END;

-- Reusable original metadata evidence, retained by canonical macro inputs in their commit.
CREATE TABLE provider_capture_metadata_dependencies (
    dependency_digest BLOB PRIMARY KEY CHECK (length(dependency_digest)=32 AND dependency_digest<>zeroblob(32)),
    capture_observation_digest BLOB NOT NULL REFERENCES provider_raw_observations(capture_observation_digest),
    raw_object_input_ordinal INTEGER NOT NULL CHECK (raw_object_input_ordinal=0),
    raw_claim_digest BLOB NOT NULL REFERENCES sealed_raw_objects(raw_claim_digest),
    physical_receipt_digest BLOB NOT NULL CHECK (length(physical_receipt_digest)=32 AND physical_receipt_digest<>zeroblob(32)),
    sealed_capture_receipt_digest BLOB NOT NULL CHECK (length(sealed_capture_receipt_digest)=32 AND sealed_capture_receipt_digest<>zeroblob(32)),
    FOREIGN KEY (capture_observation_digest,raw_object_input_ordinal,raw_claim_digest,physical_receipt_digest)
        REFERENCES provider_raw_observation_objects(capture_observation_digest,input_ordinal,raw_claim_digest,physical_receipt_digest)
) STRICT, WITHOUT ROWID;
CREATE INDEX provider_capture_metadata_by_raw ON provider_capture_metadata_dependencies(raw_claim_digest,physical_receipt_digest);

CREATE TABLE ingest_run_provider_capture_bindings (
    run_id TEXT NOT NULL REFERENCES ingest_runs(run_id),
    input_ordinal INTEGER NOT NULL CHECK (input_ordinal BETWEEN 0 AND 4095),
    output_artifact_ordinal INTEGER NOT NULL CHECK (
        output_artifact_ordinal BETWEEN 0 AND 1023
    ),
    object_input_ordinal INTEGER NOT NULL CHECK (
        object_input_ordinal BETWEEN 0 AND 4095
    ),
    binding_digest BLOB NOT NULL UNIQUE
        REFERENCES provider_capture_bindings(binding_digest),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    metadata_dependency_digest BLOB REFERENCES provider_capture_metadata_dependencies(dependency_digest),
    PRIMARY KEY (run_id, input_ordinal),
    UNIQUE (run_id, binding_digest),
    UNIQUE (run_id, output_artifact_ordinal, object_input_ordinal),
    FOREIGN KEY (run_id, output_artifact_ordinal)
        REFERENCES artifacts(run_id, publication_ordinal)
) STRICT, WITHOUT ROWID;

CREATE INDEX ingest_run_capture_metadata_dependency ON ingest_run_provider_capture_bindings(metadata_dependency_digest);

CREATE TABLE market_event_storage_heads (
    dataset_id TEXT PRIMARY KEY CHECK (length(CAST(dataset_id AS BLOB)) BETWEEN 1 AND 256),
    committed_sequence INTEGER NOT NULL CHECK (committed_sequence>=0),
    content_digest BLOB CHECK (length(content_digest)=32),
    CHECK ((committed_sequence=0 AND content_digest IS NULL)
        OR (committed_sequence>0 AND content_digest IS NOT NULL))
) STRICT, WITHOUT ROWID;

CREATE TABLE market_event_commits (
    dataset_id TEXT NOT NULL REFERENCES market_event_storage_heads(dataset_id),
    commit_sequence INTEGER NOT NULL CHECK (commit_sequence>0),
    run_id TEXT NOT NULL UNIQUE REFERENCES ingest_runs(run_id),
    publication_digest BLOB NOT NULL UNIQUE CHECK (length(publication_digest)=32),
    publication_kind TEXT NOT NULL CHECK (publication_kind IN
        ('response_market_event','event_microbatch','composite_response_event')),
    schema_name TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    schema_fingerprint BLOB NOT NULL CHECK (length(schema_fingerprint)=32),
    previous_content_digest BLOB CHECK (length(previous_content_digest)=32),
    content_digest BLOB NOT NULL CHECK (length(content_digest)=32),
    lineage_digest BLOB NOT NULL CHECK (length(lineage_digest)=32),
    row_count INTEGER NOT NULL CHECK (row_count>0),
    available_at_ns INTEGER NOT NULL,
    PRIMARY KEY (dataset_id,commit_sequence),
    UNIQUE (dataset_id,commit_sequence,publication_digest),
    CHECK ((commit_sequence=1 AND previous_content_digest IS NULL)
        OR (commit_sequence>1 AND previous_content_digest IS NOT NULL)),
    CHECK (schema_name='market_squawk.market_events' AND schema_version=1
        AND schema_fingerprint=X'e0bf8cc9a74c880cc772d3987907b13eb3d4d8fc2dc3ca1a239873d650a151f0')
) STRICT, WITHOUT ROWID;

CREATE TRIGGER market_event_commits_guarded_insert
BEFORE INSERT ON market_event_commits
WHEN NOT EXISTS (
    SELECT 1 FROM market_event_storage_heads AS head
    JOIN ingest_runs AS run ON run.run_id=NEW.run_id
    WHERE head.dataset_id=NEW.dataset_id
      AND head.committed_sequence=NEW.commit_sequence-1
      AND head.content_digest IS NEW.previous_content_digest
      AND run.state='reserved' AND run.operation='persist'
      AND run.payload_algorithm=1 AND run.payload_digest=NEW.publication_digest
      AND NEW.available_at_ns>=run.requested_at_ns
      AND (NEW.commit_sequence=1 OR EXISTS (
          SELECT 1 FROM market_event_commits AS previous
          WHERE previous.dataset_id=NEW.dataset_id
            AND previous.commit_sequence=NEW.commit_sequence-1
            AND previous.content_digest=NEW.previous_content_digest
            AND previous.available_at_ns<=NEW.available_at_ns
      ))
) BEGIN SELECT RAISE(ABORT,'invalid active event commit'); END;

CREATE TRIGGER market_event_heads_guarded_insert
BEFORE INSERT ON market_event_storage_heads
WHEN NEW.committed_sequence<>0 OR NEW.content_digest IS NOT NULL
BEGIN SELECT RAISE(ABORT,'invalid active event head'); END;

CREATE TRIGGER market_event_heads_guarded_update
BEFORE UPDATE ON market_event_storage_heads
WHEN NEW.dataset_id<>OLD.dataset_id OR NEW.committed_sequence<>OLD.committed_sequence+1
 OR NOT EXISTS (
    SELECT 1 FROM market_event_commits AS committed
    WHERE committed.dataset_id=NEW.dataset_id
      AND committed.commit_sequence=NEW.committed_sequence
      AND committed.content_digest=NEW.content_digest
      AND committed.previous_content_digest IS OLD.content_digest
 )
BEGIN SELECT RAISE(ABORT,'invalid active event head transition'); END;

CREATE TRIGGER market_event_commits_immutable_update BEFORE UPDATE ON market_event_commits
BEGIN SELECT RAISE(ABORT,'market event commits are immutable'); END;
CREATE TRIGGER market_event_commits_immutable_delete BEFORE DELETE ON market_event_commits
BEGIN SELECT RAISE(ABORT,'market event commits are immutable'); END;
CREATE TRIGGER market_event_heads_immutable_delete BEFORE DELETE ON market_event_storage_heads
BEGIN SELECT RAISE(ABORT,'market event heads cannot be deleted'); END;

CREATE TABLE ingest_run_provider_publication_bindings (
    run_id TEXT NOT NULL REFERENCES ingest_runs(run_id),
    input_ordinal INTEGER NOT NULL CHECK (input_ordinal BETWEEN 0 AND 4095),
    output_artifact_ordinal INTEGER CHECK (
        output_artifact_ordinal BETWEEN 0 AND 1023
    ),
    object_input_ordinal INTEGER CHECK (
        object_input_ordinal BETWEEN 0 AND 4095
    ),
    active_dataset_id TEXT,
    active_commit_sequence INTEGER,
    publication_digest BLOB NOT NULL UNIQUE CHECK (
        length(publication_digest) = 32 AND publication_digest <> zeroblob(32)
    ),
    publication_kind TEXT NOT NULL CHECK (
        publication_kind IN (
            'response_market_event',
            'event_microbatch',
            'composite_response_event',
            'option_snapshots',
            'option_expirations',
            'provider_logical'
        )
    ),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    response_binding_digest BLOB
        REFERENCES provider_response_market_event_bindings(response_event_binding_digest),
    event_binding_digest BLOB REFERENCES provider_event_bindings(event_binding_digest),
    composite_binding_digest BLOB
        REFERENCES provider_composite_response_event_bindings(composite_binding_digest),
    option_binding_digest BLOB
        REFERENCES provider_option_market_bindings(option_binding_digest),
    logical_binding_digest BLOB
        REFERENCES provider_logical_publication_bindings(binding_digest),
    FOREIGN KEY (active_dataset_id,active_commit_sequence,publication_digest)
        REFERENCES market_event_commits(dataset_id,commit_sequence,publication_digest),
    CHECK ((output_artifact_ordinal IS NOT NULL AND object_input_ordinal IS NOT NULL
            AND active_dataset_id IS NULL AND active_commit_sequence IS NULL
            AND publication_kind IN ('option_snapshots','option_expirations','provider_logical'))
        OR (output_artifact_ordinal IS NULL AND object_input_ordinal IS NULL
            AND active_dataset_id IS NOT NULL AND active_commit_sequence IS NOT NULL
            AND input_ordinal=0 AND publication_kind IN
                ('response_market_event','event_microbatch','composite_response_event'))),
    PRIMARY KEY (run_id, input_ordinal),
    UNIQUE (run_id, publication_digest),
    UNIQUE (run_id, output_artifact_ordinal, object_input_ordinal),
    FOREIGN KEY (run_id, output_artifact_ordinal)
        REFERENCES artifacts(run_id, publication_ordinal),
    CHECK (
        (publication_kind='response_market_event'
            AND response_binding_digest IS NOT NULL
            AND event_binding_digest IS NULL
            AND composite_binding_digest IS NULL
            AND option_binding_digest IS NULL
            AND logical_binding_digest IS NULL
            AND publication_digest=response_binding_digest)
        OR (publication_kind='event_microbatch'
            AND response_binding_digest IS NULL
            AND event_binding_digest IS NOT NULL
            AND composite_binding_digest IS NULL
            AND option_binding_digest IS NULL
            AND logical_binding_digest IS NULL
            AND publication_digest=event_binding_digest)
        OR (publication_kind='composite_response_event'
            AND response_binding_digest IS NOT NULL
            AND event_binding_digest IS NOT NULL
            AND composite_binding_digest IS NOT NULL
            AND option_binding_digest IS NULL
            AND logical_binding_digest IS NULL
            AND publication_digest=composite_binding_digest)
        OR (publication_kind IN ('option_snapshots', 'option_expirations')
            AND response_binding_digest IS NULL
            AND event_binding_digest IS NULL
            AND composite_binding_digest IS NULL
            AND option_binding_digest IS NOT NULL
            AND logical_binding_digest IS NULL
            AND publication_digest=option_binding_digest)
        OR (publication_kind='provider_logical'
            AND response_binding_digest IS NULL
            AND event_binding_digest IS NULL
            AND composite_binding_digest IS NULL
            AND option_binding_digest IS NULL
            AND logical_binding_digest IS NOT NULL
            AND publication_digest=logical_binding_digest)
    )
) STRICT, WITHOUT ROWID;

-- Placement of each canonical partition beneath one complete logical source binding.
-- These rows are output coordinates, never new provider publications or raw custody claims.
CREATE TABLE ingest_run_provider_logical_partition_artifacts (
    run_id TEXT NOT NULL,
    logical_binding_digest BLOB NOT NULL,
    partition_ordinal INTEGER NOT NULL CHECK (partition_ordinal BETWEEN 0 AND 1023),
    output_artifact_ordinal INTEGER NOT NULL CHECK (output_artifact_ordinal BETWEEN 0 AND 1023),
    object_input_ordinal INTEGER NOT NULL CHECK (object_input_ordinal BETWEEN 0 AND 1023),
    PRIMARY KEY (run_id, partition_ordinal),
    UNIQUE (run_id, output_artifact_ordinal, object_input_ordinal),
    FOREIGN KEY (run_id, logical_binding_digest)
        REFERENCES ingest_run_provider_publication_bindings(run_id, publication_digest),
    FOREIGN KEY (logical_binding_digest, partition_ordinal)
        REFERENCES provider_logical_publication_canonical_expectations(binding_digest, partition_ordinal),
    FOREIGN KEY (run_id, output_artifact_ordinal)
        REFERENCES artifacts(run_id, publication_ordinal)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER ingest_run_provider_logical_partition_artifacts_guarded_insert
BEFORE INSERT ON ingest_run_provider_logical_partition_artifacts
WHEN NOT EXISTS (
    SELECT 1 FROM ingest_runs AS run
    JOIN ingest_run_provider_publication_bindings AS input ON input.run_id=run.run_id
    WHERE run.run_id=NEW.run_id AND run.state='reserved' AND run.operation='persist'
      AND input.publication_kind='provider_logical'
      AND input.logical_binding_digest=NEW.logical_binding_digest
      AND input.input_ordinal=0 AND input.output_artifact_ordinal=0 AND input.object_input_ordinal=0
      AND (SELECT COUNT(*) FROM ingest_run_provider_publication_bindings WHERE run_id=NEW.run_id)=1
      AND NOT EXISTS (SELECT 1 FROM ingest_run_provider_capture_bindings WHERE run_id=NEW.run_id)
      AND NEW.partition_ordinal=(SELECT COUNT(*) FROM ingest_run_provider_logical_partition_artifacts WHERE run_id=NEW.run_id)
      AND ((NEW.partition_ordinal=0 AND NEW.output_artifact_ordinal=0 AND NEW.object_input_ordinal=0)
        OR EXISTS (
            SELECT 1 FROM ingest_run_provider_logical_partition_artifacts AS prior
            WHERE prior.run_id=NEW.run_id AND prior.partition_ordinal=NEW.partition_ordinal-1
              AND ((NEW.output_artifact_ordinal=prior.output_artifact_ordinal AND NEW.object_input_ordinal=prior.object_input_ordinal+1)
                OR (NEW.output_artifact_ordinal=prior.output_artifact_ordinal+1 AND NEW.object_input_ordinal=0))
        ))
)
BEGIN
    SELECT RAISE(ABORT, 'logical canonical partition placement is invalid');
END;

CREATE TRIGGER ingest_run_provider_logical_partition_artifacts_no_update
BEFORE UPDATE ON ingest_run_provider_logical_partition_artifacts
BEGIN
    SELECT RAISE(ABORT, 'logical canonical partition placement is immutable');
END;
CREATE TRIGGER ingest_run_provider_logical_partition_artifacts_no_delete
BEFORE DELETE ON ingest_run_provider_logical_partition_artifacts
BEGIN
    SELECT RAISE(ABORT, 'logical canonical partition placement is immutable');
END;

-- Root-owned addition to the active provider logical schema after the logical publication
-- tables and ingest_run_provider_publication_bindings exist. This is part of the existing
-- catalog migration closure, not another database or migration-version authority.
CREATE TABLE provider_logical_originals (
    coordinate_digest BLOB PRIMARY KEY CHECK (length(coordinate_digest)=32 AND coordinate_digest<>zeroblob(32)),
    dataset_id TEXT NOT NULL CHECK (length(CAST(dataset_id AS BLOB)) BETWEEN 1 AND 256),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    native_schema_digest BLOB NOT NULL CHECK (length(native_schema_digest)=32 AND native_schema_digest<>zeroblob(32)),
    source_revision_digest BLOB NOT NULL CHECK (length(source_revision_digest)=32 AND source_revision_digest<>zeroblob(32)),
    registered_source_revision_digest BLOB NOT NULL CHECK (length(registered_source_revision_digest)=32 AND registered_source_revision_digest<>zeroblob(32)),
    source_revision_kind TEXT NOT NULL CHECK (source_revision_kind IN ('metadata','contract_payload')),
    original_digest BLOB NOT NULL CHECK (length(original_digest)=32 AND original_digest<>zeroblob(32)),
    received_at_ns INTEGER NOT NULL,
    checkpoint_digest BLOB NOT NULL CHECK (length(checkpoint_digest)=32 AND checkpoint_digest<>zeroblob(32)),
    checkpoint_bytes BLOB NOT NULL CHECK (length(checkpoint_bytes) BETWEEN 1 AND 131072),
    object_count INTEGER NOT NULL CHECK (object_count BETWEEN 1 AND 64),
    object_set_digest BLOB NOT NULL CHECK (length(object_set_digest)=32 AND object_set_digest<>zeroblob(32)),
    rights_id BLOB NOT NULL REFERENCES source_rights(rights_id),
    custody_digest BLOB NOT NULL CHECK (length(custody_digest)=32 AND custody_digest<>zeroblob(32)),
    retained_at_ns INTEGER NOT NULL CHECK (received_at_ns<=retained_at_ns),
    publication_digest BLOB UNIQUE REFERENCES provider_logical_publication_bindings(binding_digest),
    published_at_ns INTEGER,
    UNIQUE(dataset_id, source_id, native_schema_digest, source_revision_digest, original_digest),
    FOREIGN KEY(source_id, registered_source_revision_digest) REFERENCES source_revisions(source_id, revision_digest),
    CHECK (source_revision_kind<>'metadata' OR source_revision_digest=registered_source_revision_digest),
    CHECK ((publication_digest IS NULL AND published_at_ns IS NULL)
        OR (publication_digest IS NOT NULL AND published_at_ns IS NOT NULL AND published_at_ns>=retained_at_ns))
) STRICT, WITHOUT ROWID;

CREATE INDEX provider_logical_original_pending
ON provider_logical_originals(dataset_id, source_id, native_schema_digest, source_revision_digest, retained_at_ns DESC, coordinate_digest DESC)
WHERE publication_digest IS NULL;

CREATE TABLE provider_logical_original_objects (
    coordinate_digest BLOB NOT NULL REFERENCES provider_logical_originals(coordinate_digest),
    object_ordinal INTEGER NOT NULL CHECK (object_ordinal BETWEEN 0 AND 63),
    object_role TEXT NOT NULL CHECK (object_role IN ('catalog','provider_payload','expanded_payload','provider_component')),
    semantic_identity BLOB NOT NULL CHECK (length(semantic_identity)=32 AND semantic_identity<>zeroblob(32)),
    raw_claim_digest BLOB NOT NULL,
    physical_receipt_digest BLOB NOT NULL,
    PRIMARY KEY(coordinate_digest, object_ordinal),
    UNIQUE(coordinate_digest, raw_claim_digest),
    FOREIGN KEY(raw_claim_digest, physical_receipt_digest)
        REFERENCES sealed_raw_objects(raw_claim_digest, physical_receipt_digest)
) STRICT, WITHOUT ROWID;

CREATE INDEX provider_logical_original_object_recovery
ON provider_logical_original_objects(raw_claim_digest, physical_receipt_digest);

CREATE TRIGGER provider_logical_originals_guarded_insert
BEFORE INSERT ON provider_logical_originals
WHEN NEW.publication_digest IS NOT NULL OR NEW.published_at_ns IS NOT NULL
    OR NOT EXISTS (
        SELECT 1 FROM source_rights AS rights
        JOIN source_revisions AS revision ON revision.source_id=rights.source_id
        WHERE rights.rights_id=NEW.rights_id AND rights.source_id=NEW.source_id
          AND rights.payload_algorithm=1 AND rights.payload_digest=NEW.original_digest
          AND (rights.operation_mask & 4)<>0 AND rights.admitted_at_ns<=NEW.retained_at_ns
          AND (rights.authorization_expires_at_ns IS NULL OR rights.authorization_expires_at_ns>NEW.retained_at_ns)
          AND revision.revision_digest=NEW.registered_source_revision_digest AND revision.registered_at_ns<=NEW.retained_at_ns
    )
BEGIN
    SELECT RAISE(ABORT, 'logical original custody requires exact admitted Persist evidence');
END;

CREATE TRIGGER provider_logical_original_objects_guarded_insert
BEFORE INSERT ON provider_logical_original_objects
WHEN NOT EXISTS (
    SELECT 1 FROM provider_logical_originals AS original
    JOIN sealed_raw_objects AS claim ON claim.raw_claim_digest=NEW.raw_claim_digest
      AND claim.physical_receipt_digest=NEW.physical_receipt_digest AND claim.raw_claim_kind='logical_object'
    WHERE original.coordinate_digest=NEW.coordinate_digest AND original.publication_digest IS NULL
      AND NEW.object_ordinal<original.object_count
      AND NEW.object_ordinal=(SELECT COUNT(*) FROM provider_logical_original_objects WHERE coordinate_digest=NEW.coordinate_digest)
)
BEGIN
    SELECT RAISE(ABORT, 'logical original object must extend the exact pending custody graph');
END;

CREATE TRIGGER provider_logical_originals_guarded_update
BEFORE UPDATE ON provider_logical_originals
WHEN OLD.publication_digest IS NOT NULL OR OLD.published_at_ns IS NOT NULL
    OR NEW.publication_digest IS NULL OR NEW.published_at_ns IS NULL
    OR NEW.coordinate_digest<>OLD.coordinate_digest OR NEW.dataset_id<>OLD.dataset_id
    OR NEW.source_id<>OLD.source_id OR NEW.native_schema_digest<>OLD.native_schema_digest
    OR NEW.registered_source_revision_digest<>OLD.registered_source_revision_digest
    OR NEW.source_revision_kind<>OLD.source_revision_kind
    OR NEW.source_revision_digest<>OLD.source_revision_digest OR NEW.original_digest<>OLD.original_digest
    OR NEW.received_at_ns<>OLD.received_at_ns OR NEW.checkpoint_digest<>OLD.checkpoint_digest
    OR NEW.checkpoint_bytes<>OLD.checkpoint_bytes OR NEW.object_count<>OLD.object_count
    OR NEW.object_set_digest<>OLD.object_set_digest OR NEW.rights_id<>OLD.rights_id
    OR NEW.custody_digest<>OLD.custody_digest OR NEW.retained_at_ns<>OLD.retained_at_ns
    OR NEW.published_at_ns<OLD.retained_at_ns
    OR NOT EXISTS (
        SELECT 1 FROM provider_logical_publication_bindings AS binding
        JOIN ingest_run_provider_publication_bindings AS input ON input.publication_digest=binding.binding_digest
          AND input.logical_binding_digest=binding.binding_digest AND input.publication_kind='provider_logical'
        JOIN ingest_runs AS run ON run.run_id=input.run_id AND run.source_id=input.source_id
        WHERE binding.binding_digest=NEW.publication_digest AND binding.source_id=NEW.source_id
          AND input.source_id=NEW.source_id AND run.state='reserved' AND run.operation='persist'
          AND run.payload_algorithm=1 AND run.payload_digest=NEW.publication_digest
          AND binding.object_count=NEW.object_count
          AND (SELECT COUNT(*) FROM provider_logical_original_objects WHERE coordinate_digest=NEW.coordinate_digest)=NEW.object_count
          AND (SELECT COUNT(*) FROM provider_logical_publication_objects WHERE binding_digest=NEW.publication_digest)=NEW.object_count
          AND NOT EXISTS (
              SELECT 1 FROM provider_logical_original_objects AS object
              WHERE object.coordinate_digest=NEW.coordinate_digest AND NOT EXISTS (
                  SELECT 1 FROM provider_logical_publication_objects AS published
                  WHERE published.binding_digest=NEW.publication_digest AND published.object_ordinal=object.object_ordinal
                    AND published.object_role=object.object_role AND published.semantic_identity=object.semantic_identity
                    AND published.raw_claim_digest=object.raw_claim_digest AND published.physical_receipt_digest=object.physical_receipt_digest
              )
          )
          AND EXISTS (SELECT 1 FROM provider_logical_publication_partitions
              WHERE binding_digest=NEW.publication_digest AND partition_family='provider_native')
          AND NOT EXISTS (SELECT 1 FROM provider_logical_publication_partitions
              WHERE binding_digest=NEW.publication_digest AND partition_family='provider_native' AND schema_identity<>NEW.native_schema_digest)
    )
BEGIN
    SELECT RAISE(ABORT, 'logical original publication requires its exact complete retained binding');
END;

CREATE TRIGGER provider_logical_originals_immutable_delete
BEFORE DELETE ON provider_logical_originals BEGIN
    SELECT RAISE(ABORT, 'logical original custody is retained');
END;
CREATE TRIGGER provider_logical_original_objects_immutable_update
BEFORE UPDATE ON provider_logical_original_objects BEGIN
    SELECT RAISE(ABORT, 'logical original objects are immutable');
END;
CREATE TRIGGER provider_logical_original_objects_immutable_delete
BEFORE DELETE ON provider_logical_original_objects BEGIN
    SELECT RAISE(ABORT, 'logical original objects are retained');
END;

-- Immutable row-level coordinates for bounded provider-neutral current-market selection. This
-- table retains no provider preference: later application policy compares these exact candidates.
CREATE TABLE provider_market_event_selection_index (
    dataset_id TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL,
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
    publication_row_ordinal INTEGER NOT NULL CHECK (
        publication_row_ordinal BETWEEN 0 AND 127
    ),
    component_kind TEXT NOT NULL CHECK (component_kind IN ('response', 'stream')),
    component_binding_digest BLOB NOT NULL CHECK (
        length(component_binding_digest) = 32
        AND component_binding_digest <> zeroblob(32)
    ),
    component_row_ordinal INTEGER NOT NULL CHECK (
        component_row_ordinal BETWEEN 0 AND 63
    ),
    canonical_event_digest BLOB NOT NULL CHECK (
        length(canonical_event_digest) = 32
        AND canonical_event_digest <> zeroblob(32)
    ),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    instrument_id BLOB CHECK (
        instrument_id IS NULL OR (length(instrument_id) = 16 AND instrument_id <> zeroblob(16))
    ),
    venue_id TEXT NOT NULL CHECK (
        length(CAST(venue_id AS BLOB)) BETWEEN 1 AND 128
    ),
    event_kind TEXT NOT NULL CHECK (
        event_kind IN (
            'trade', 'quote', 'book_snapshot', 'book_delta',
            'auction', 'trading_halt', 'instrument_status', 'corporate_action', 'chart', 'screener'
        )
    ),
    source_timestamp_ns INTEGER,
    received_at_ns INTEGER NOT NULL,
    available_at_ns INTEGER NOT NULL,
    ingested_at_ns INTEGER NOT NULL,
    connection_generation_be BLOB NOT NULL CHECK (
        length(connection_generation_be) = 8
        AND connection_generation_be <> zeroblob(8)
    ),
    source_sequence_be BLOB CHECK (
        source_sequence_be IS NULL OR length(source_sequence_be) = 8
    ),
    provider_event_id TEXT NOT NULL CHECK (
        length(CAST(provider_event_id AS BLOB)) BETWEEN 1 AND 512
    ),
    coordinate_digest BLOB NOT NULL CHECK (
        length(coordinate_digest) = 32 AND coordinate_digest <> zeroblob(32)
    ),
    cohort_key TEXT CHECK (cohort_key IS NULL OR length(CAST(cohort_key AS BLOB)) BETWEEN 1 AND 512),
    provider_product TEXT NOT NULL CHECK (length(CAST(provider_product AS BLOB)) BETWEEN 1 AND 512),
    provider_channel TEXT NOT NULL CHECK (length(CAST(provider_channel AS BLOB)) BETWEEN 1 AND 512),
    CHECK (
        (event_kind = 'screener' AND instrument_id IS NULL AND cohort_key IS NOT NULL
         AND cohort_key = provider_event_id)
        OR (event_kind <> 'screener' AND instrument_id IS NOT NULL AND cohort_key IS NULL)
    ),
    FOREIGN KEY (dataset_id,commit_sequence,publication_digest)
        REFERENCES market_event_commits(dataset_id,commit_sequence,publication_digest),
    PRIMARY KEY (publication_digest, publication_row_ordinal),
    UNIQUE (
        publication_digest,
        component_kind,
        component_row_ordinal
    ),
    UNIQUE (publication_digest, coordinate_digest),
    FOREIGN KEY (publication_digest)
        REFERENCES ingest_run_provider_publication_bindings(publication_digest),
    CHECK (received_at_ns <= available_at_ns AND available_at_ns <= ingested_at_ns)
) STRICT, WITHOUT ROWID;

CREATE INDEX provider_market_event_selection_source_time
ON provider_market_event_selection_index(
    instrument_id,
    venue_id,
    event_kind,
    source_timestamp_ns DESC,
    available_at_ns DESC,
    ingested_at_ns DESC,
    publication_digest,
    publication_row_ordinal
)
WHERE source_timestamp_ns IS NOT NULL;

CREATE INDEX provider_market_event_selection_received_time
ON provider_market_event_selection_index(
    instrument_id,
    venue_id,
    event_kind,
    received_at_ns DESC,
    available_at_ns DESC,
    ingested_at_ns DESC,
    publication_digest,
    publication_row_ordinal
);

CREATE INDEX provider_market_event_selection_cohort
ON provider_market_event_selection_index(
    source_id, provider_product, provider_channel, cohort_key, venue_id, event_kind,
    source_timestamp_ns DESC, received_at_ns DESC, publication_digest, publication_row_ordinal
)
WHERE instrument_id IS NULL;

CREATE INDEX provider_market_event_active_source_time
ON provider_market_event_selection_index(dataset_id,source_id,instrument_id,venue_id,event_kind,
    source_timestamp_ns DESC,received_at_ns DESC,available_at_ns,ingested_at_ns,
    commit_sequence,publication_digest,publication_row_ordinal)
WHERE dataset_id IS NOT NULL AND source_timestamp_ns IS NOT NULL;
CREATE INDEX provider_market_event_active_received_time
ON provider_market_event_selection_index(dataset_id,source_id,instrument_id,venue_id,event_kind,
    received_at_ns DESC,available_at_ns,ingested_at_ns,
    commit_sequence,publication_digest,publication_row_ordinal)
WHERE dataset_id IS NOT NULL;
CREATE INDEX provider_market_event_active_cohort
ON provider_market_event_selection_index(dataset_id,source_id,provider_product,provider_channel,
    cohort_key,venue_id,event_kind,source_timestamp_ns DESC,received_at_ns DESC,
    commit_sequence,publication_digest,publication_row_ordinal)
WHERE dataset_id IS NOT NULL AND instrument_id IS NULL;
CREATE INDEX provider_market_event_active_commit_order
ON provider_market_event_selection_index(dataset_id,commit_sequence,publication_row_ordinal)
WHERE dataset_id IS NOT NULL;

CREATE TABLE market_event_archive_objects (
    content_digest BLOB PRIMARY KEY CHECK (length(content_digest)=32),
    relative_reference TEXT NOT NULL UNIQUE CHECK (length(CAST(relative_reference AS BLOB)) BETWEEN 1 AND 1024),
    schema_name TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    schema_fingerprint BLOB NOT NULL CHECK (length(schema_fingerprint)=32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes>0),
    row_count INTEGER NOT NULL CHECK (row_count>0),
    created_at_ns INTEGER NOT NULL,
    published_at_ns INTEGER NOT NULL CHECK (published_at_ns>=created_at_ns),
    CHECK (schema_name='market_squawk.market_events' AND schema_version=1
        AND schema_fingerprint=X'e0bf8cc9a74c880cc772d3987907b13eb3d4d8fc2dc3ca1a239873d650a151f0')
) STRICT, WITHOUT ROWID;

CREATE TABLE market_event_archive_memberships (
    publication_digest BLOB PRIMARY KEY REFERENCES market_event_commits(publication_digest),
    dataset_id TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL CHECK (commit_sequence>0),
    object_content_digest BLOB NOT NULL REFERENCES market_event_archive_objects(content_digest),
    first_row INTEGER NOT NULL CHECK (first_row>=0),
    row_count INTEGER NOT NULL CHECK (row_count>0),
    FOREIGN KEY (dataset_id,commit_sequence,publication_digest)
        REFERENCES market_event_commits(dataset_id,commit_sequence,publication_digest),
    UNIQUE (object_content_digest,first_row)
) STRICT, WITHOUT ROWID;
CREATE INDEX market_event_archive_commit_order
ON market_event_archive_memberships(dataset_id,commit_sequence);

CREATE TABLE market_event_archive_progress (
    dataset_id TEXT PRIMARY KEY REFERENCES market_event_storage_heads(dataset_id),
    archived_sequence INTEGER NOT NULL CHECK (archived_sequence>=0)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER market_event_archive_memberships_guarded_insert
BEFORE INSERT ON market_event_archive_memberships
WHEN NOT EXISTS (
    SELECT 1 FROM market_event_commits AS committed
    JOIN ingest_runs AS run ON run.run_id=committed.run_id
    JOIN market_event_archive_objects AS object ON object.content_digest=NEW.object_content_digest
    WHERE committed.dataset_id=NEW.dataset_id AND committed.commit_sequence=NEW.commit_sequence
      AND committed.publication_digest=NEW.publication_digest AND committed.row_count=NEW.row_count
      AND object.schema_name=committed.schema_name AND object.schema_version=committed.schema_version
      AND object.schema_fingerprint=committed.schema_fingerprint
      AND NEW.first_row<=object.row_count AND NEW.row_count<=object.row_count-NEW.first_row
      AND object.published_at_ns>=committed.available_at_ns
      AND run.state='succeeded' AND run.completed_at_ns=committed.available_at_ns
) OR EXISTS (
    SELECT 1 FROM market_event_archive_memberships AS retained
    WHERE retained.object_content_digest=NEW.object_content_digest
      AND retained.first_row<NEW.first_row+NEW.row_count
      AND NEW.first_row<retained.first_row+retained.row_count
)
BEGIN SELECT RAISE(ABORT,'invalid market event archive membership'); END;

CREATE TRIGGER market_event_archive_progress_guarded_insert
BEFORE INSERT ON market_event_archive_progress WHEN NEW.archived_sequence<>0
BEGIN SELECT RAISE(ABORT,'invalid initial market event archive progress'); END;
CREATE TRIGGER market_event_archive_progress_guarded_update
BEFORE UPDATE ON market_event_archive_progress
WHEN NEW.dataset_id<>OLD.dataset_id OR NEW.archived_sequence<=OLD.archived_sequence
 OR NEW.archived_sequence>(SELECT committed_sequence FROM market_event_storage_heads WHERE dataset_id=NEW.dataset_id)
 OR (SELECT COUNT(*) FROM market_event_archive_memberships
     WHERE dataset_id=NEW.dataset_id AND commit_sequence>OLD.archived_sequence
       AND commit_sequence<=NEW.archived_sequence)<>NEW.archived_sequence-OLD.archived_sequence
BEGIN SELECT RAISE(ABORT,'incomplete market event archive progress'); END;

CREATE TRIGGER market_event_archive_objects_immutable_update BEFORE UPDATE ON market_event_archive_objects
BEGIN SELECT RAISE(ABORT,'market event archives are immutable'); END;
CREATE TRIGGER market_event_archive_objects_immutable_delete BEFORE DELETE ON market_event_archive_objects
BEGIN SELECT RAISE(ABORT,'market event archives are retained evidence'); END;
CREATE TRIGGER market_event_archive_memberships_immutable_update BEFORE UPDATE ON market_event_archive_memberships
BEGIN SELECT RAISE(ABORT,'market event archive memberships are immutable'); END;
CREATE TRIGGER market_event_archive_memberships_immutable_delete BEFORE DELETE ON market_event_archive_memberships
BEGIN SELECT RAISE(ABORT,'market event archive memberships are retained evidence'); END;
CREATE TRIGGER market_event_archive_progress_immutable_delete BEFORE DELETE ON market_event_archive_progress
BEGIN SELECT RAISE(ABORT,'market event archive progress cannot be deleted'); END;

CREATE TABLE market_event_active_rows (
    publication_digest BLOB NOT NULL,
    publication_row_ordinal INTEGER NOT NULL,
    event_json BLOB NOT NULL CHECK (length(event_json)>0),
    PRIMARY KEY (publication_digest,publication_row_ordinal),
    FOREIGN KEY (publication_digest,publication_row_ordinal)
        REFERENCES provider_market_event_selection_index(publication_digest,publication_row_ordinal)
) STRICT, WITHOUT ROWID;
CREATE TRIGGER market_event_active_rows_guarded_insert
BEFORE INSERT ON market_event_active_rows
WHEN NOT EXISTS (
    SELECT 1 FROM market_event_commits AS committed
    JOIN ingest_runs AS run ON run.run_id=committed.run_id
    WHERE committed.publication_digest=NEW.publication_digest
      AND NEW.publication_row_ordinal>=0 AND NEW.publication_row_ordinal<committed.row_count
      AND run.state='reserved'
) BEGIN SELECT RAISE(ABORT,'invalid canonical active event row'); END;
CREATE TRIGGER market_event_active_rows_immutable_update BEFORE UPDATE ON market_event_active_rows
BEGIN SELECT RAISE(ABORT,'canonical active event rows are immutable'); END;
CREATE TRIGGER market_event_active_rows_immutable_delete BEFORE DELETE ON market_event_active_rows
WHEN NOT EXISTS (
    SELECT 1 FROM market_event_archive_memberships AS archived
    WHERE archived.publication_digest=OLD.publication_digest
      AND OLD.publication_row_ordinal>=0 AND OLD.publication_row_ordinal<archived.row_count
)
BEGIN SELECT RAISE(ABORT,'canonical active event rows require archival before deletion'); END;

CREATE VIEW market_event_complete_commits AS
SELECT committed.* FROM market_event_commits AS committed
JOIN ingest_run_provider_publication_bindings AS publication
  ON publication.run_id=committed.run_id
 AND publication.publication_digest=committed.publication_digest
 AND publication.publication_kind=committed.publication_kind
 AND publication.active_dataset_id=committed.dataset_id
 AND publication.active_commit_sequence=committed.commit_sequence
JOIN market_event_storage_heads AS head ON head.dataset_id=committed.dataset_id
WHERE head.committed_sequence>=committed.commit_sequence
 AND publication.output_artifact_ordinal IS NULL AND publication.object_input_ordinal IS NULL
 AND ((SELECT COUNT(*) FROM market_event_active_rows AS active
       WHERE active.publication_digest=committed.publication_digest)=committed.row_count
      OR EXISTS (SELECT 1 FROM market_event_archive_memberships AS archived
          WHERE archived.publication_digest=committed.publication_digest
            AND archived.dataset_id=committed.dataset_id
            AND archived.commit_sequence=committed.commit_sequence
            AND archived.row_count=committed.row_count))
 AND (SELECT COUNT(*) FROM provider_market_event_selection_index AS indexed
      WHERE indexed.publication_digest=committed.publication_digest
        AND indexed.dataset_id=committed.dataset_id
        AND indexed.commit_sequence=committed.commit_sequence)=committed.row_count
 AND NOT EXISTS (SELECT 1 FROM artifacts WHERE run_id=committed.run_id);

CREATE TRIGGER provider_market_event_selection_index_guarded_insert
BEFORE INSERT ON provider_market_event_selection_index
WHEN NOT EXISTS (
    SELECT 1
    FROM ingest_run_provider_publication_bindings AS publication
    WHERE publication.publication_digest = NEW.publication_digest
      AND publication.publication_kind = NEW.publication_kind
      AND publication.source_id = NEW.source_id
      AND publication.active_dataset_id IS NEW.dataset_id
      AND publication.active_commit_sequence IS NEW.commit_sequence
      AND (
          (
              NEW.component_kind = 'response'
              AND publication.response_binding_digest = NEW.component_binding_digest
              AND EXISTS (
                  SELECT 1
                  FROM provider_response_market_event_binding_rows AS row
                  WHERE row.response_event_binding_digest = NEW.component_binding_digest
                    AND row.canonical_row_ordinal = NEW.component_row_ordinal
                    AND row.canonical_event_digest = NEW.canonical_event_digest
              )
          )
          OR (
              NEW.component_kind = 'stream'
              AND publication.event_binding_digest = NEW.component_binding_digest
              AND EXISTS (
                  SELECT 1
                  FROM provider_event_binding_rows AS row
                  WHERE row.event_binding_digest = NEW.component_binding_digest
                    AND row.canonical_row_ordinal = NEW.component_row_ordinal
                    AND row.canonical_event_digest = NEW.canonical_event_digest
              )
          )
      )
      AND (
          (
              publication.publication_kind IN (
                  'response_market_event', 'event_microbatch'
              )
              AND NEW.publication_row_ordinal = NEW.component_row_ordinal
          )
          OR (
              publication.publication_kind = 'composite_response_event'
              AND NEW.component_kind = 'response'
              AND NEW.publication_row_ordinal = NEW.component_row_ordinal
          )
          OR (
              publication.publication_kind = 'composite_response_event'
              AND NEW.component_kind = 'stream'
              AND NEW.publication_row_ordinal = (
                  SELECT composite.response_row_count + NEW.component_row_ordinal
                  FROM provider_composite_response_event_bindings AS composite
                  WHERE composite.composite_binding_digest = NEW.publication_digest
                    AND composite.response_binding_digest = publication.response_binding_digest
                    AND composite.event_binding_digest = publication.event_binding_digest
              )
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'provider market-event selection coordinate is invalid');
END;

CREATE TRIGGER ingest_run_provider_publication_bindings_guarded_insert
BEFORE INSERT ON ingest_run_provider_publication_bindings
WHEN NOT EXISTS (
    SELECT 1
    FROM ingest_runs AS run
    WHERE run.run_id=NEW.run_id
      AND run.state='reserved'
      AND run.operation='persist'
      AND run.source_id=NEW.source_id
      AND NEW.input_ordinal=(
          SELECT COUNT(*) FROM ingest_run_provider_publication_bindings AS retained
          WHERE retained.run_id=NEW.run_id
      )
      AND (
          (NEW.input_ordinal=0 AND NEW.active_dataset_id IS NOT NULL
           AND EXISTS (SELECT 1 FROM market_event_commits AS committed
               WHERE committed.dataset_id=NEW.active_dataset_id
                 AND committed.commit_sequence=NEW.active_commit_sequence
                 AND committed.run_id=NEW.run_id
                 AND committed.publication_digest=NEW.publication_digest))
          OR (NEW.input_ordinal=0 AND NEW.output_artifact_ordinal=0
           AND NEW.object_input_ordinal=0)
          OR EXISTS (
              SELECT 1 FROM ingest_run_provider_publication_bindings AS prior
              WHERE prior.run_id=NEW.run_id
                AND prior.input_ordinal=NEW.input_ordinal - 1
                AND (
                    (NEW.output_artifact_ordinal=prior.output_artifact_ordinal
                     AND NEW.object_input_ordinal=prior.object_input_ordinal + 1)
                    OR (NEW.output_artifact_ordinal=prior.output_artifact_ordinal + 1
                        AND NEW.object_input_ordinal=0)
                )
          )
      )
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
          OR (NEW.publication_kind IN ('option_snapshots', 'option_expirations') AND EXISTS (
              SELECT 1
              FROM provider_option_market_bindings AS binding
              JOIN provider_raw_observations AS capture
                ON capture.capture_observation_digest=binding.capture_observation_digest
              JOIN provider_option_market_binding_native_lineage AS native
                ON native.option_binding_digest=binding.option_binding_digest
              WHERE binding.option_binding_digest=NEW.option_binding_digest
                AND binding.publication_kind=NEW.publication_kind
                AND NEW.publication_digest=binding.option_binding_digest
                AND capture.source_id=NEW.source_id
                AND native.row_count=binding.canonical_row_count
                AND (SELECT COUNT(*) FROM provider_option_market_binding_rows AS row
                     WHERE row.option_binding_digest=binding.option_binding_digest)
                    = binding.canonical_row_count
          ))
          OR (NEW.publication_kind='provider_logical' AND EXISTS (
              SELECT 1
              FROM provider_logical_publication_bindings AS binding
              WHERE binding.binding_digest=NEW.logical_binding_digest
                AND NEW.publication_digest=binding.binding_digest
                AND binding.source_id=NEW.source_id
                AND binding.required_family_count=(
                    SELECT COUNT(*)
                    FROM provider_logical_publication_required_families AS family
                    WHERE family.binding_digest=binding.binding_digest
                )
                AND binding.object_count=(
                    SELECT COUNT(*)
                    FROM provider_logical_publication_objects AS object
                    WHERE object.binding_digest=binding.binding_digest
                )
                AND binding.partition_count=(
                    SELECT COUNT(*)
                    FROM provider_logical_publication_partitions AS partition
                    WHERE partition.binding_digest=binding.binding_digest
                )
                AND binding.canonical_partition_count=(
                    SELECT COUNT(*)
                    FROM provider_logical_publication_canonical_expectations AS expected
                    WHERE expected.binding_digest=binding.binding_digest
                )
                AND (SELECT MIN(family.family_ordinal)
                     FROM provider_logical_publication_required_families AS family
                     WHERE family.binding_digest=binding.binding_digest)=0
                AND (SELECT MAX(family.family_ordinal)
                     FROM provider_logical_publication_required_families AS family
                     WHERE family.binding_digest=binding.binding_digest)
                    = binding.required_family_count - 1
                AND (SELECT MIN(object.object_ordinal)
                     FROM provider_logical_publication_objects AS object
                     WHERE object.binding_digest=binding.binding_digest)=0
                AND (SELECT MAX(object.object_ordinal)
                     FROM provider_logical_publication_objects AS object
                     WHERE object.binding_digest=binding.binding_digest)
                    = binding.object_count - 1
                AND (
                    binding.canonical_partition_count=0
                    OR (
                        (SELECT MIN(expected.partition_ordinal)
                         FROM provider_logical_publication_canonical_expectations AS expected
                         WHERE expected.binding_digest=binding.binding_digest)=0
                        AND (SELECT MAX(expected.partition_ordinal)
                             FROM provider_logical_publication_canonical_expectations AS expected
                             WHERE expected.binding_digest=binding.binding_digest)
                            = binding.canonical_partition_count - 1
                    )
                )
          ))
      )
)
BEGIN
    SELECT RAISE(ABORT, 'ingest-run provider publication is invalid');
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
      AND NEW.input_ordinal = (
          SELECT COUNT(*) FROM ingest_run_provider_capture_bindings AS retained
          WHERE retained.run_id = NEW.run_id
      )
      AND (
          (NEW.input_ordinal = 0 AND NEW.output_artifact_ordinal = 0
           AND NEW.object_input_ordinal = 0)
          OR EXISTS (
              SELECT 1 FROM ingest_run_provider_capture_bindings AS prior
              WHERE prior.run_id = NEW.run_id
                AND prior.input_ordinal = NEW.input_ordinal - 1
                AND (
                    (NEW.output_artifact_ordinal = prior.output_artifact_ordinal
                     AND NEW.object_input_ordinal = prior.object_input_ordinal + 1)
                    OR (NEW.output_artifact_ordinal = prior.output_artifact_ordinal + 1
                        AND NEW.object_input_ordinal = 0)
                )
          )
      )
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
            'composite_response_event',
            'option_snapshots',
            'option_expirations',
            'provider_logical'
        )
    ),
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    PRIMARY KEY (generation_sequence, input_ordinal),
    UNIQUE (generation_sequence, publication_digest),
    FOREIGN KEY (run_id, publication_digest)
        REFERENCES ingest_run_provider_publication_bindings(run_id, publication_digest)
) STRICT, WITHOUT ROWID;

CREATE INDEX analytical_generation_provider_publication_by_digest
ON analytical_generation_provider_publication_bindings(publication_digest, publication_kind, generation_sequence);
CREATE INDEX analytical_generation_provider_capture_by_digest
ON analytical_generation_provider_capture_bindings(binding_digest, generation_sequence);
CREATE INDEX ingest_run_provider_publication_by_digest
ON ingest_run_provider_publication_bindings(publication_digest, publication_kind, run_id, source_id);
CREATE INDEX ingest_run_provider_capture_by_digest
ON ingest_run_provider_capture_bindings(binding_digest, run_id, source_id);

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

CREATE TRIGGER provider_option_market_bindings_immutable_update
BEFORE UPDATE ON provider_option_market_bindings BEGIN
    SELECT RAISE(ABORT, 'provider option-market bindings are immutable');
END;

CREATE TRIGGER provider_option_market_bindings_immutable_delete
BEFORE DELETE ON provider_option_market_bindings BEGIN
    SELECT RAISE(ABORT, 'provider option-market bindings are immutable');
END;

CREATE TRIGGER provider_option_market_binding_native_lineage_immutable_update
BEFORE UPDATE ON provider_option_market_binding_native_lineage BEGIN
    SELECT RAISE(ABORT, 'provider option-market native lineage is immutable');
END;

CREATE TRIGGER provider_option_market_binding_native_lineage_immutable_delete
BEFORE DELETE ON provider_option_market_binding_native_lineage BEGIN
    SELECT RAISE(ABORT, 'provider option-market native lineage is immutable');
END;

CREATE TRIGGER provider_option_market_binding_rows_immutable_update
BEFORE UPDATE ON provider_option_market_binding_rows BEGIN
    SELECT RAISE(ABORT, 'provider option-market rows are immutable');
END;

CREATE TRIGGER provider_option_market_binding_rows_immutable_delete
BEFORE DELETE ON provider_option_market_binding_rows BEGIN
    SELECT RAISE(ABORT, 'provider option-market rows are immutable');
END;

CREATE TRIGGER provider_logical_publication_bindings_immutable_update
BEFORE UPDATE ON provider_logical_publication_bindings BEGIN
    SELECT RAISE(ABORT, 'provider logical publication bindings are immutable');
END;

CREATE TRIGGER provider_logical_publication_bindings_immutable_delete
BEFORE DELETE ON provider_logical_publication_bindings BEGIN
    SELECT RAISE(ABORT, 'provider logical publication bindings are immutable');
END;

CREATE TRIGGER provider_logical_publication_required_families_immutable_update
BEFORE UPDATE ON provider_logical_publication_required_families BEGIN
    SELECT RAISE(ABORT, 'provider logical required families are immutable');
END;

CREATE TRIGGER provider_logical_publication_required_families_immutable_delete
BEFORE DELETE ON provider_logical_publication_required_families BEGIN
    SELECT RAISE(ABORT, 'provider logical required families are immutable');
END;

CREATE TRIGGER provider_logical_publication_objects_immutable_update
BEFORE UPDATE ON provider_logical_publication_objects BEGIN
    SELECT RAISE(ABORT, 'provider logical publication objects are immutable');
END;

CREATE TRIGGER provider_logical_publication_objects_immutable_delete
BEFORE DELETE ON provider_logical_publication_objects BEGIN
    SELECT RAISE(ABORT, 'provider logical publication objects are immutable');
END;

CREATE TRIGGER provider_logical_publication_partitions_immutable_update
BEFORE UPDATE ON provider_logical_publication_partitions BEGIN
    SELECT RAISE(ABORT, 'provider logical publication partitions are immutable');
END;

CREATE TRIGGER provider_logical_publication_partitions_immutable_delete
BEFORE DELETE ON provider_logical_publication_partitions BEGIN
    SELECT RAISE(ABORT, 'provider logical publication partitions are immutable');
END;

CREATE TRIGGER provider_logical_publication_canonical_expectations_immutable_update
BEFORE UPDATE ON provider_logical_publication_canonical_expectations BEGIN
    SELECT RAISE(ABORT, 'provider logical canonical expectations are immutable');
END;

CREATE TRIGGER provider_logical_publication_canonical_expectations_immutable_delete
BEFORE DELETE ON provider_logical_publication_canonical_expectations BEGIN
    SELECT RAISE(ABORT, 'provider logical canonical expectations are immutable');
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

CREATE TRIGGER provider_market_event_selection_index_immutable_update
BEFORE UPDATE ON provider_market_event_selection_index BEGIN
    SELECT RAISE(ABORT, 'provider market-event selection coordinates are immutable');
END;

CREATE TRIGGER provider_market_event_selection_index_immutable_delete
BEFORE DELETE ON provider_market_event_selection_index BEGIN
    SELECT RAISE(ABORT, 'provider market-event selection coordinates are immutable');
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
    binding_digest BLOB NOT NULL UNIQUE CHECK (
        length(binding_digest) = 32 AND binding_digest <> zeroblob(32)
    ),
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
    timestamp_basis TEXT CHECK (
        timestamp_basis IN ('period_start', 'period_end')
    ),
    session_kind TEXT CHECK (
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
    requested_start_ns INTEGER,
    requested_end_ns INTEGER CHECK (requested_end_ns > requested_start_ns),
    coverage_first_ns INTEGER CHECK (
        coverage_first_ns BETWEEN requested_start_ns AND requested_end_ns
    ),
    coverage_last_ns INTEGER CHECK (
        coverage_last_ns BETWEEN coverage_first_ns AND requested_end_ns
    ),
    coverage_last_complete_ns INTEGER CHECK (
        coverage_last_complete_ns >= coverage_last_ns
        -- Completion is exclusive; the unchanged provider request end is inclusive.
        AND coverage_last_complete_ns > -9223372036854775808
        AND coverage_last_complete_ns - 1 <= requested_end_ns
    ),
    requested_start_date INTEGER,
    requested_end_date INTEGER,
    origin_record_count INTEGER NOT NULL CHECK (origin_record_count BETWEEN 1 AND 4294967295),
    expected_bar_count INTEGER NOT NULL CHECK (
        expected_bar_count BETWEEN 1 AND 4294967295
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
    market_bar_component_ordinal INTEGER CHECK (
        market_bar_component_ordinal BETWEEN 0 AND 63
    ),
    market_bar_component_content_digest BLOB CHECK (
        length(market_bar_component_content_digest) = 32
        AND market_bar_component_content_digest <> zeroblob(32)
    ),
    market_bar_component_page_count INTEGER CHECK (
        market_bar_component_page_count BETWEEN 1 AND 64
    ),
    session_calendar_component_ordinal INTEGER CHECK (
        session_calendar_component_ordinal BETWEEN 0 AND 63
        AND session_calendar_component_ordinal <> market_bar_component_ordinal
    ),
    session_calendar_component_content_digest BLOB CHECK (
        length(session_calendar_component_content_digest) = 32
        AND session_calendar_component_content_digest <> zeroblob(32)
    ),
    session_calendar_component_page_count INTEGER CHECK (
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
    CHECK (origin_record_count >= returned_bar_count),
    CHECK (
      (requested_start_ns IS NOT NULL AND requested_end_ns IS NOT NULL
       AND coverage_first_ns IS NOT NULL AND coverage_last_ns IS NOT NULL AND coverage_last_complete_ns IS NOT NULL
       AND timestamp_basis IS NOT NULL AND session_kind IS NOT NULL
       AND requested_start_date IS NULL AND requested_end_date IS NULL
       AND session_calendar_component_ordinal IS NOT NULL AND session_calendar_component_content_digest IS NOT NULL AND session_calendar_component_page_count IS NOT NULL)
      OR
      (requested_start_ns IS NULL AND requested_end_ns IS NULL
       AND coverage_first_ns IS NULL AND coverage_last_ns IS NULL AND coverage_last_complete_ns IS NULL
       AND timestamp_basis IS NULL AND session_kind IS NULL
       AND requested_start_date IS NOT NULL AND requested_end_date IS NOT NULL AND requested_start_date <= requested_end_date
       AND session_calendar_component_ordinal IS NULL AND session_calendar_component_content_digest IS NULL AND session_calendar_component_page_count IS NULL)
    ),
    UNIQUE (origin_generation_sequence, binding_digest)
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
      AND object.row_count = NEW.origin_record_count
      AND (
        (NEW.requested_start_ns IS NOT NULL AND EXISTS (
          SELECT 1 FROM analytical_generation_provider_capture_bindings AS capture_input
          JOIN provider_capture_bindings AS binding USING (binding_digest)
          JOIN provider_raw_observations AS capture
            ON capture.capture_observation_digest = binding.capture_observation_digest
          WHERE capture_input.generation_sequence = generation.generation_sequence
            AND capture_input.run_id = run.run_id
            AND capture_input.binding_digest = NEW.binding_digest
            AND capture_input.source_id = NEW.source_id
            AND capture.source_id = NEW.source_id
            AND capture.provider_dataset = NEW.provider_dataset
            AND capture.terminal_disposition = 'complete_request_graph'
            AND capture.capture_content_digest = NEW.capture_content_digest
            AND capture.capture_observation_digest = NEW.capture_observation_digest
            AND capture.recorded_at_ns = NEW.capture_recorded_at_ns
        )) OR (NEW.requested_start_date IS NOT NULL AND EXISTS (
          SELECT 1 FROM analytical_generation_provider_publication_bindings AS logical_input
          JOIN provider_logical_publication_bindings AS logical
            ON logical.binding_digest = logical_input.publication_digest
          WHERE logical_input.generation_sequence = generation.generation_sequence
            AND logical_input.run_id = run.run_id
            AND logical_input.publication_kind = 'provider_logical'
            AND logical_input.publication_digest = NEW.binding_digest
            AND logical_input.source_id = NEW.source_id
            AND logical.source_id = NEW.source_id
            AND logical.terminal_receipt_digest = NEW.capture_receipt_digest
            AND logical.recorded_at_ns = NEW.capture_recorded_at_ns
            AND json_extract(logical.terminal_json, '$.total_canonical_rows') = NEW.origin_record_count
        ))
      )
      AND instrument_revision.published_at_ns <= run.requested_at_ns
      AND (
          (NEW.requested_start_ns IS NOT NULL
           AND json_type(NEW.receipt_json, '$.identity_selection') = 'object'
           AND json_type(NEW.receipt_json, '$.symbol_asof') = 'object'
           AND json_extract(NEW.receipt_json, '$.identity_selection.native.namespace') = 'alpaca-basic-asset-reference-v1'
           AND json_extract(NEW.receipt_json, '$.identity_selection.native.instrument') = NEW.instrument_id
           AND json_extract(NEW.receipt_json, '$.identity_selection.native.venue') = NEW.venue_id
           AND json_extract(NEW.receipt_json, '$.identity_selection.native.venue_symbol') = NEW.provider_instrument_id
           AND json_extract(NEW.receipt_json, '$.identity_selection.definition_published_at') = instrument_revision.published_at_ns
           AND instrument_revision.published_at_ns <= json_extract(NEW.receipt_json, '$.identity_selection.native.knowledge_at')
           AND json_extract(NEW.receipt_json, '$.identity_selection.native.knowledge_at') <= run.requested_at_ns
           AND json_extract(NEW.receipt_json, '$.identity_selection.native.effective_at') <= json_extract(NEW.receipt_json, '$.identity_selection.native.knowledge_at')
           AND instrument_revision.effective_start_ns <= json_extract(NEW.receipt_json, '$.identity_selection.native.effective_at')
           AND (instrument_revision.effective_end_ns IS NULL
                OR json_extract(NEW.receipt_json, '$.identity_selection.native.effective_at') < instrument_revision.effective_end_ns))
          OR
          (NEW.requested_start_date IS NOT NULL
           AND instrument_revision.effective_start_ns <= json_extract(NEW.receipt_json, '$.date_windows.normalization.resolved_at')
           AND (instrument_revision.effective_end_ns IS NULL
                OR json_extract(NEW.receipt_json, '$.date_windows.normalization.resolved_at') < instrument_revision.effective_end_ns))
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
    WHERE publication.publication_receipt_digest = NEW.publication_receipt_digest
      AND publication.origin_generation_sequence = NEW.generation_sequence
      AND (
        (publication.requested_start_ns IS NOT NULL AND EXISTS (
          SELECT 1 FROM analytical_generation_provider_capture_bindings AS input
          WHERE input.generation_sequence = NEW.generation_sequence
            AND input.binding_digest = publication.binding_digest
            AND input.source_id = publication.source_id
        )) OR (publication.requested_start_date IS NOT NULL AND EXISTS (
          SELECT 1 FROM analytical_generation_provider_publication_bindings AS input
          WHERE input.generation_sequence = NEW.generation_sequence
            AND input.publication_digest = publication.binding_digest
            AND input.publication_kind = 'provider_logical'
            AND input.source_id = publication.source_id
        ))
      )
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

-- Provider-neutral durable acquisition state for bounded multi-response macro publications.
-- Checkpoint bytes remain opaque to common code; only their exact SHA-256 identity and CAS
-- coordinate are interpreted here.
CREATE TABLE provider_macro_plan_values (
    value_id BLOB PRIMARY KEY CHECK (
        length(value_id) = 32 AND value_id <> zeroblob(32)
    ),
    session_id TEXT NOT NULL CHECK (length(CAST(session_id AS BLOB)) = 36),
    value_kind TEXT NOT NULL CHECK (value_kind IN ('checkpoint', 'semantics')),
    value_ordinal INTEGER NOT NULL CHECK (value_ordinal BETWEEN 0 AND 1024),
    value_digest BLOB NOT NULL CHECK (
        length(value_digest) = 32 AND value_digest <> zeroblob(32)
    ),
    byte_length INTEGER NOT NULL CHECK (byte_length BETWEEN 1 AND 16777216),
    chunk_count INTEGER NOT NULL CHECK (chunk_count BETWEEN 1 AND 4096),
    retained_at_ns INTEGER NOT NULL,
    UNIQUE (session_id, value_kind, value_ordinal),
    UNIQUE (value_id, value_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_macro_plan_value_chunks (
    value_id BLOB NOT NULL REFERENCES provider_macro_plan_values(value_id),
    chunk_ordinal INTEGER NOT NULL CHECK (chunk_ordinal BETWEEN 0 AND 4095),
    chunk_digest BLOB NOT NULL CHECK (
        length(chunk_digest) = 32 AND chunk_digest <> zeroblob(32)
    ),
    chunk_bytes BLOB NOT NULL CHECK (length(chunk_bytes) BETWEEN 1 AND 524288),
    PRIMARY KEY (value_id, chunk_ordinal)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_macro_plan_sessions (
    session_id TEXT PRIMARY KEY CHECK (length(CAST(session_id AS BLOB)) = 36),
    analytical_dataset TEXT NOT NULL CHECK (
        length(CAST(analytical_dataset AS BLOB)) BETWEEN 1 AND 256
    ),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    metadata_revision TEXT NOT NULL CHECK (
        length(CAST(metadata_revision AS BLOB)) BETWEEN 1 AND 512
    ),
    provider_dataset TEXT NOT NULL CHECK (
        length(CAST(provider_dataset AS BLOB)) BETWEEN 1 AND 512
    ),
    source_generation_digest BLOB NOT NULL CHECK (
        length(source_generation_digest) = 32
        AND source_generation_digest <> zeroblob(32)
    ),
    plan_identity BLOB NOT NULL CHECK (
        length(plan_identity) = 32 AND plan_identity <> zeroblob(32)
    ),
    initial_checkpoint_digest BLOB NOT NULL CHECK (
        length(initial_checkpoint_digest) = 32
        AND initial_checkpoint_digest <> zeroblob(32)
    ),
    state TEXT NOT NULL CHECK (state IN ('acquiring', 'complete')),
    state_version INTEGER NOT NULL CHECK (state_version BETWEEN 0 AND 1024),
    checkpoint_digest BLOB NOT NULL CHECK (
        length(checkpoint_digest) = 32 AND checkpoint_digest <> zeroblob(32)
    ),
    checkpoint_value_id BLOB NOT NULL REFERENCES provider_macro_plan_values(value_id),
    response_count INTEGER NOT NULL CHECK (response_count BETWEEN 0 AND 1024),
    data_page_count INTEGER NOT NULL CHECK (data_page_count BETWEEN 0 AND 1024),
    analytical_row_count INTEGER NOT NULL CHECK (
        analytical_row_count BETWEEN 0 AND 102400000
    ),
    semantics_bytes INTEGER NOT NULL CHECK (semantics_bytes BETWEEN 0 AND 67108864),
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL,
    CHECK (updated_at_ns >= created_at_ns),
    CHECK (
        (state = 'acquiring'
            AND response_count <= 1023
            AND response_count = data_page_count
            AND state_version = response_count)
        OR (state = 'complete'
            AND response_count BETWEEN 1 AND 1024
            AND data_page_count BETWEEN MAX(1, response_count - 1) AND response_count
            AND state_version = response_count)
    ),
    FOREIGN KEY (checkpoint_value_id, checkpoint_digest)
        REFERENCES provider_macro_plan_values(value_id, value_digest)
) STRICT;

CREATE UNIQUE INDEX provider_macro_plan_one_acquiring_session
ON provider_macro_plan_sessions(
    analytical_dataset,
    source_id,
    metadata_revision,
    provider_dataset,
    source_generation_digest,
    plan_identity,
    initial_checkpoint_digest
)
WHERE state = 'acquiring';

CREATE TABLE provider_macro_plan_staged_pages (
    session_id TEXT NOT NULL REFERENCES provider_macro_plan_sessions(session_id),
    page_ordinal INTEGER NOT NULL CHECK (page_ordinal BETWEEN 0 AND 1023),
    candidate_digest BLOB NOT NULL CHECK (
        length(candidate_digest) = 32 AND candidate_digest <> zeroblob(32)
    ),
    binding_digest BLOB NOT NULL UNIQUE
        REFERENCES provider_capture_bindings(binding_digest),
    capture_observation_digest BLOB NOT NULL UNIQUE
        REFERENCES provider_raw_observations(capture_observation_digest),
    canonical_record_count INTEGER NOT NULL CHECK (
        canonical_record_count BETWEEN 1 AND 100000
    ),
    semantics_schema TEXT NOT NULL CHECK (
        length(CAST(semantics_schema AS BLOB)) BETWEEN 1 AND 512
    ),
    semantics_schema_requirement_digest BLOB NOT NULL CHECK (
        length(semantics_schema_requirement_digest) = 32
        AND semantics_schema_requirement_digest <> zeroblob(32)
    ),
    semantics_digest BLOB NOT NULL CHECK (
        length(semantics_digest) = 32 AND semantics_digest <> zeroblob(32)
    ),
    semantics_value_id BLOB NOT NULL REFERENCES provider_macro_plan_values(value_id),
    semantics_payload_digest BLOB NOT NULL CHECK (
        length(semantics_payload_digest) = 32
        AND semantics_payload_digest <> zeroblob(32)
    ),
    object_relative_reference TEXT NOT NULL CHECK (
        length(CAST(object_relative_reference AS BLOB)) BETWEEN 1 AND 1024
    ),
    object_content_hash BLOB NOT NULL CHECK (
        length(object_content_hash) = 32 AND object_content_hash <> zeroblob(32)
    ),
    object_size_bytes INTEGER NOT NULL CHECK (
        object_size_bytes BETWEEN 1 AND 1073741824
    ),
    object_lineage_hash BLOB NOT NULL CHECK (
        length(object_lineage_hash) = 32 AND object_lineage_hash <> zeroblob(32)
    ),
    object_created_at_ns INTEGER NOT NULL,
    staged_at_ns INTEGER NOT NULL,
    PRIMARY KEY (session_id, page_ordinal),
    UNIQUE (session_id, candidate_digest),
    UNIQUE (session_id, binding_digest),
    UNIQUE (session_id, object_content_hash),
    FOREIGN KEY (semantics_value_id, semantics_payload_digest)
        REFERENCES provider_macro_plan_values(value_id, value_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_macro_plan_terminal_completions (
    session_id TEXT PRIMARY KEY REFERENCES provider_macro_plan_sessions(session_id),
    response_ordinal INTEGER NOT NULL CHECK (response_ordinal BETWEEN 0 AND 1023),
    completion_kind TEXT NOT NULL CHECK (completion_kind IN ('completion_only', 'data_page')),
    adapter_completion_digest BLOB NOT NULL CHECK (
        length(adapter_completion_digest) = 32
        AND adapter_completion_digest <> zeroblob(32)
    ),
    capture_observation_digest BLOB NOT NULL UNIQUE
        REFERENCES provider_raw_observations(capture_observation_digest),
    sealed_capture_receipt_digest BLOB NOT NULL UNIQUE CHECK (
        length(sealed_capture_receipt_digest) = 32
        AND sealed_capture_receipt_digest <> zeroblob(32)
    ),
    raw_claim_digest BLOB NOT NULL,
    physical_receipt_digest BLOB NOT NULL CHECK (
        length(physical_receipt_digest) = 32
        AND physical_receipt_digest <> zeroblob(32)
    ),
    completed_at_ns INTEGER NOT NULL,
    UNIQUE (session_id, response_ordinal),
    FOREIGN KEY (
        capture_observation_digest,
        raw_claim_digest,
        physical_receipt_digest
    ) REFERENCES provider_raw_observation_objects(
        capture_observation_digest,
        raw_claim_digest,
        physical_receipt_digest
    )
) STRICT, WITHOUT ROWID;

-- Exact restart state for bounded replay-group finalization. These rows retain only immutable
-- content-addressed group outputs; the existing analytical generation transaction remains the
-- sole authority that makes any output selector-visible.
CREATE TABLE provider_macro_plan_finalizations (
    session_id TEXT PRIMARY KEY
        REFERENCES provider_macro_plan_terminal_completions(session_id),
    run_id TEXT NOT NULL UNIQUE REFERENCES ingest_runs(run_id),
    publication_digest BLOB NOT NULL UNIQUE CHECK (
        length(publication_digest) = 32 AND publication_digest <> zeroblob(32)
    ),
    predecessor_publication_digest BLOB REFERENCES provider_macro_plan_publications(
        publication_digest
    ),
    predecessor_manifest_dataset_id TEXT,
    predecessor_manifest_version INTEGER,
    predecessor_checkpoint_version INTEGER,
    predecessor_checkpoint_digest BLOB,
    started_at_ns INTEGER NOT NULL,
    CHECK (
        (predecessor_publication_digest IS NULL
            AND predecessor_manifest_dataset_id IS NULL
            AND predecessor_manifest_version IS NULL
            AND predecessor_checkpoint_version IS NULL
            AND predecessor_checkpoint_digest IS NULL)
        OR (predecessor_publication_digest IS NOT NULL
            AND predecessor_manifest_dataset_id IS NOT NULL
            AND predecessor_manifest_version IS NOT NULL
            AND predecessor_manifest_version > 0
            AND predecessor_checkpoint_version BETWEEN 1 AND 1024
            AND length(predecessor_checkpoint_digest) = 32
            AND predecessor_checkpoint_digest <> zeroblob(32))
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_macro_plan_finalized_groups (
    session_id TEXT NOT NULL REFERENCES provider_macro_plan_finalizations(session_id),
    output_ordinal INTEGER NOT NULL CHECK (output_ordinal BETWEEN 0 AND 31),
    first_page_ordinal INTEGER NOT NULL CHECK (first_page_ordinal BETWEEN 0 AND 1023),
    page_count INTEGER NOT NULL CHECK (page_count BETWEEN 1 AND 32),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 3200000),
    object_relative_reference TEXT NOT NULL CHECK (
        length(CAST(object_relative_reference AS BLOB)) BETWEEN 1 AND 1024
    ),
    object_content_hash BLOB NOT NULL CHECK (
        length(object_content_hash) = 32 AND object_content_hash <> zeroblob(32)
    ),
    object_size_bytes INTEGER NOT NULL CHECK (
        object_size_bytes BETWEEN 1 AND 1073741824
    ),
    object_lineage_hash BLOB NOT NULL CHECK (
        length(object_lineage_hash) = 32 AND object_lineage_hash <> zeroblob(32)
    ),
    object_created_at_ns INTEGER NOT NULL,
    recorded_at_ns INTEGER NOT NULL CHECK (recorded_at_ns >= object_created_at_ns),
    PRIMARY KEY (session_id, output_ordinal),
    UNIQUE (session_id, first_page_ordinal)
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_macro_plan_publications (
    publication_digest BLOB PRIMARY KEY CHECK (
        length(publication_digest) = 32 AND publication_digest <> zeroblob(32)
    ),
    generation_sequence INTEGER NOT NULL UNIQUE
        REFERENCES analytical_generations(generation_sequence),
    session_id TEXT NOT NULL UNIQUE
        REFERENCES provider_macro_plan_terminal_completions(session_id),
    manifest_dataset_id TEXT NOT NULL CHECK (
        length(CAST(manifest_dataset_id AS BLOB)) BETWEEN 1 AND 256
    ),
    manifest_version INTEGER NOT NULL CHECK (manifest_version > 0),
    manifest_schema_name TEXT NOT NULL CHECK (
        length(CAST(manifest_schema_name AS BLOB)) BETWEEN 1 AND 128
    ),
    manifest_schema_version INTEGER NOT NULL CHECK (manifest_schema_version > 0),
    manifest_schema_fingerprint BLOB NOT NULL CHECK (
        length(manifest_schema_fingerprint) = 32
        AND manifest_schema_fingerprint <> zeroblob(32)
    ),
    manifest_content_hash BLOB NOT NULL CHECK (
        length(manifest_content_hash) = 32 AND manifest_content_hash <> zeroblob(32)
    ),
    anchor_manifest_id TEXT NOT NULL UNIQUE REFERENCES dataset_manifests(manifest_id),
    run_id TEXT NOT NULL UNIQUE REFERENCES ingest_runs(run_id),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    metadata_revision TEXT NOT NULL CHECK (
        length(CAST(metadata_revision AS BLOB)) BETWEEN 1 AND 512
    ),
    provider_dataset TEXT NOT NULL CHECK (
        length(CAST(provider_dataset AS BLOB)) BETWEEN 1 AND 512
    ),
    source_generation_digest BLOB NOT NULL CHECK (
        length(source_generation_digest) = 32
        AND source_generation_digest <> zeroblob(32)
    ),
    plan_identity BLOB NOT NULL CHECK (
        length(plan_identity) = 32 AND plan_identity <> zeroblob(32)
    ),
    request_set_identity BLOB NOT NULL CHECK (
        length(request_set_identity) = 32 AND request_set_identity <> zeroblob(32)
    ),
    adapter_completion_digest BLOB NOT NULL CHECK (
        length(adapter_completion_digest) = 32
        AND adapter_completion_digest <> zeroblob(32)
    ),
    terminal_seal_digest BLOB NOT NULL CHECK (
        length(terminal_seal_digest) = 32 AND terminal_seal_digest <> zeroblob(32)
    ),
    catalog_receipt_digest BLOB NOT NULL UNIQUE CHECK (
        length(catalog_receipt_digest) = 32
        AND catalog_receipt_digest <> zeroblob(32)
    ),
    response_count INTEGER NOT NULL CHECK (response_count BETWEEN 1 AND 1024),
    data_page_count INTEGER NOT NULL CHECK (data_page_count BETWEEN 1 AND 1024),
    analytical_row_count INTEGER NOT NULL CHECK (
        analytical_row_count BETWEEN 1 AND 102400000
    ),
    completed_checkpoint_version INTEGER NOT NULL CHECK (
        completed_checkpoint_version BETWEEN 1 AND 1024
    ),
    completed_checkpoint_digest BLOB NOT NULL CHECK (
        length(completed_checkpoint_digest) = 32
        AND completed_checkpoint_digest <> zeroblob(32)
    ),
    predecessor_publication_digest BLOB REFERENCES provider_macro_plan_publications(
        publication_digest
    ),
    predecessor_manifest_dataset_id TEXT,
    predecessor_manifest_version INTEGER,
    predecessor_checkpoint_version INTEGER,
    predecessor_checkpoint_digest BLOB,
    published_at_ns INTEGER NOT NULL,
    UNIQUE (manifest_dataset_id, manifest_version),
    CHECK (data_page_count BETWEEN MAX(1, response_count - 1) AND response_count),
    CHECK (completed_checkpoint_version = response_count),
    CHECK (
        (predecessor_publication_digest IS NULL
            AND predecessor_manifest_dataset_id IS NULL
            AND predecessor_manifest_version IS NULL
            AND predecessor_checkpoint_version IS NULL
            AND predecessor_checkpoint_digest IS NULL)
        OR (predecessor_publication_digest IS NOT NULL
            AND predecessor_manifest_dataset_id IS NOT NULL
            AND predecessor_manifest_version IS NOT NULL
            AND predecessor_manifest_version > 0
            AND predecessor_checkpoint_version BETWEEN 1 AND 1024
            AND length(predecessor_checkpoint_digest) = 32
            AND predecessor_checkpoint_digest <> zeroblob(32))
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE provider_macro_plan_published_heads (
    analytical_dataset TEXT NOT NULL,
    source_id TEXT NOT NULL,
    provider_dataset TEXT NOT NULL,
    publication_digest BLOB NOT NULL UNIQUE
        REFERENCES provider_macro_plan_publications(publication_digest),
    session_id TEXT NOT NULL UNIQUE REFERENCES provider_macro_plan_sessions(session_id),
    generation_sequence INTEGER NOT NULL UNIQUE
        REFERENCES analytical_generations(generation_sequence),
    manifest_version INTEGER NOT NULL CHECK (manifest_version > 0),
    completed_checkpoint_version INTEGER NOT NULL CHECK (
        completed_checkpoint_version BETWEEN 1 AND 1024
    ),
    completed_checkpoint_digest BLOB NOT NULL CHECK (
        length(completed_checkpoint_digest) = 32
        AND completed_checkpoint_digest <> zeroblob(32)
    ),
    advanced_at_ns INTEGER NOT NULL,
    PRIMARY KEY (analytical_dataset, source_id, provider_dataset)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER provider_macro_plan_sessions_guarded_insert
BEFORE INSERT ON provider_macro_plan_sessions
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_macro_plan_values AS value
    WHERE value.value_id = NEW.checkpoint_value_id
      AND value.session_id = NEW.session_id
      AND value.value_kind = 'checkpoint'
      AND value.value_ordinal = NEW.state_version
      AND value.value_digest = NEW.checkpoint_digest
      AND (SELECT COUNT(*) FROM provider_macro_plan_value_chunks AS chunk
           WHERE chunk.value_id = value.value_id) = value.chunk_count
      AND (SELECT COALESCE(SUM(length(chunk.chunk_bytes)), 0)
           FROM provider_macro_plan_value_chunks AS chunk
           WHERE chunk.value_id = value.value_id) = value.byte_length
      AND (SELECT MIN(chunk.chunk_ordinal)
           FROM provider_macro_plan_value_chunks AS chunk
           WHERE chunk.value_id = value.value_id) = 0
      AND (SELECT MAX(chunk.chunk_ordinal)
           FROM provider_macro_plan_value_chunks AS chunk
           WHERE chunk.value_id = value.value_id) = value.chunk_count - 1
)
BEGIN
    SELECT RAISE(ABORT, 'invalid provider macro-plan initial checkpoint');
END;

CREATE TRIGGER provider_macro_plan_sessions_guarded_update
BEFORE UPDATE ON provider_macro_plan_sessions
WHEN OLD.state <> 'acquiring'
    OR NEW.session_id <> OLD.session_id
    OR NEW.analytical_dataset <> OLD.analytical_dataset
    OR NEW.source_id <> OLD.source_id
    OR NEW.metadata_revision <> OLD.metadata_revision
    OR NEW.provider_dataset <> OLD.provider_dataset
    OR NEW.source_generation_digest <> OLD.source_generation_digest
    OR NEW.plan_identity <> OLD.plan_identity
    OR NEW.initial_checkpoint_digest <> OLD.initial_checkpoint_digest
    OR NEW.created_at_ns <> OLD.created_at_ns
    OR NEW.updated_at_ns < OLD.updated_at_ns
    OR NEW.checkpoint_digest = OLD.checkpoint_digest
    OR NEW.checkpoint_value_id = OLD.checkpoint_value_id
    OR NEW.state_version <> OLD.state_version + 1
    OR NEW.response_count <> OLD.response_count + 1
    OR NOT EXISTS (
        SELECT 1
        FROM provider_macro_plan_values AS value
        WHERE value.value_id = NEW.checkpoint_value_id
          AND value.session_id = NEW.session_id
          AND value.value_kind = 'checkpoint'
          AND value.value_ordinal = NEW.state_version
          AND value.value_digest = NEW.checkpoint_digest
          AND (SELECT COUNT(*) FROM provider_macro_plan_value_chunks AS chunk
               WHERE chunk.value_id = value.value_id) = value.chunk_count
          AND (SELECT COALESCE(SUM(length(chunk.chunk_bytes)), 0)
               FROM provider_macro_plan_value_chunks AS chunk
               WHERE chunk.value_id = value.value_id) = value.byte_length
          AND (SELECT MIN(chunk.chunk_ordinal)
               FROM provider_macro_plan_value_chunks AS chunk
               WHERE chunk.value_id = value.value_id) = 0
          AND (SELECT MAX(chunk.chunk_ordinal)
               FROM provider_macro_plan_value_chunks AS chunk
               WHERE chunk.value_id = value.value_id) = value.chunk_count - 1
    )
    OR (
        NEW.state = 'acquiring'
        AND (
            EXISTS (
                SELECT 1 FROM provider_macro_plan_terminal_completions AS terminal
                WHERE terminal.session_id = OLD.session_id
            )
            OR NEW.data_page_count <> OLD.data_page_count + 1
            OR NEW.analytical_row_count <= OLD.analytical_row_count
            OR NEW.semantics_bytes <= OLD.semantics_bytes
            OR NOT EXISTS (
                SELECT 1 FROM provider_macro_plan_staged_pages AS page
                WHERE page.session_id = OLD.session_id
                  AND page.page_ordinal = OLD.data_page_count
                  AND NEW.analytical_row_count =
                      OLD.analytical_row_count + page.canonical_record_count
                  AND NEW.semantics_bytes =
                      OLD.semantics_bytes + (
                          SELECT value.byte_length
                          FROM provider_macro_plan_values AS value
                          WHERE value.value_id = page.semantics_value_id
                      )
            )
        )
    )
    OR (
        NEW.state = 'complete'
        AND NOT EXISTS (
            SELECT 1 FROM provider_macro_plan_terminal_completions AS terminal
            WHERE terminal.session_id = OLD.session_id
              AND terminal.response_ordinal = OLD.response_count
              AND (
                  (terminal.completion_kind = 'completion_only'
                      AND NEW.data_page_count = OLD.data_page_count
                      AND NEW.analytical_row_count = OLD.analytical_row_count
                      AND NEW.semantics_bytes = OLD.semantics_bytes)
                  OR (terminal.completion_kind = 'data_page'
                      AND NEW.data_page_count = OLD.data_page_count + 1
                      AND EXISTS (
                          SELECT 1 FROM provider_macro_plan_staged_pages AS page
                          JOIN provider_macro_plan_values AS value
                            ON value.value_id = page.semantics_value_id
                          WHERE page.session_id = OLD.session_id
                            AND page.page_ordinal = OLD.data_page_count
                            AND page.capture_observation_digest = terminal.capture_observation_digest
                            AND NEW.analytical_row_count = OLD.analytical_row_count + page.canonical_record_count
                            AND NEW.semantics_bytes = OLD.semantics_bytes + value.byte_length
                      ))
              )
        )
    )
    OR NEW.state NOT IN ('acquiring', 'complete')
BEGIN
    SELECT RAISE(ABORT, 'invalid provider macro-plan session transition');
END;

CREATE TRIGGER provider_macro_plan_staged_pages_guarded_insert
BEFORE INSERT ON provider_macro_plan_staged_pages
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_macro_plan_sessions AS session
    JOIN provider_capture_bindings AS binding
      ON binding.binding_digest = NEW.binding_digest
    JOIN provider_raw_observations AS capture
      ON capture.capture_observation_digest = binding.capture_observation_digest
    JOIN provider_macro_plan_values AS value
      ON value.value_id = NEW.semantics_value_id
    WHERE session.session_id = NEW.session_id
      AND session.state = 'acquiring'
      AND session.data_page_count = NEW.page_ordinal
      AND session.response_count = NEW.page_ordinal
      AND binding.capture_observation_digest = NEW.capture_observation_digest
      AND binding.canonical_record_count = NEW.canonical_record_count
      AND NEW.object_size_bytes BETWEEN 1 AND 1073741824
      AND length(CAST(NEW.object_relative_reference AS BLOB)) BETWEEN 1 AND 1024
      AND length(NEW.object_content_hash) = 32
      AND NEW.object_content_hash <> zeroblob(32)
      AND length(NEW.object_lineage_hash) = 32
      AND NEW.object_lineage_hash <> zeroblob(32)
      AND NEW.object_created_at_ns <= NEW.staged_at_ns
      AND capture.source_id = session.source_id
      AND capture.metadata_revision = session.metadata_revision
      AND capture.provider_dataset = session.provider_dataset
      AND capture.terminal_disposition = 'standalone_response'
      AND value.session_id = NEW.session_id
      AND value.value_kind = 'semantics'
      AND value.value_ordinal = NEW.page_ordinal
      AND value.value_digest = NEW.semantics_payload_digest
      AND (SELECT COUNT(*) FROM provider_macro_plan_value_chunks AS chunk
           WHERE chunk.value_id = value.value_id) = value.chunk_count
      AND (SELECT COALESCE(SUM(length(chunk.chunk_bytes)), 0)
           FROM provider_macro_plan_value_chunks AS chunk
           WHERE chunk.value_id = value.value_id) = value.byte_length
      AND (SELECT MIN(chunk.chunk_ordinal)
           FROM provider_macro_plan_value_chunks AS chunk
           WHERE chunk.value_id = value.value_id) = 0
      AND (SELECT MAX(chunk.chunk_ordinal)
           FROM provider_macro_plan_value_chunks AS chunk
           WHERE chunk.value_id = value.value_id) = value.chunk_count - 1
)
BEGIN
    SELECT RAISE(ABORT, 'invalid provider macro-plan staged page');
END;

CREATE TRIGGER provider_macro_plan_terminal_completions_guarded_insert
BEFORE INSERT ON provider_macro_plan_terminal_completions
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_macro_plan_sessions AS session
    JOIN provider_raw_observations AS capture
      ON capture.capture_observation_digest = NEW.capture_observation_digest
    JOIN provider_raw_observation_objects AS object
      ON object.capture_observation_digest = capture.capture_observation_digest
     AND object.input_ordinal = 0
     AND object.raw_claim_digest = NEW.raw_claim_digest
     AND object.physical_receipt_digest = NEW.physical_receipt_digest
    WHERE session.session_id = NEW.session_id
      AND session.state = 'acquiring'
      AND session.response_count = NEW.response_ordinal
      AND capture.source_id = session.source_id
      AND capture.metadata_revision = session.metadata_revision
      AND capture.provider_dataset = session.provider_dataset
      AND capture.terminal_disposition = 'standalone_response'
      AND capture.page_count = 1
      AND object.capture_receipt_digest = NEW.sealed_capture_receipt_digest
      AND (
          (NEW.completion_kind = 'completion_only'
              AND session.data_page_count BETWEEN 1 AND 1023
              AND NOT EXISTS (
                  SELECT 1 FROM provider_capture_bindings AS binding
                  WHERE binding.capture_observation_digest = capture.capture_observation_digest
              ))
          OR (NEW.completion_kind = 'data_page'
              AND session.data_page_count BETWEEN 0 AND 1023
              AND EXISTS (
                  SELECT 1 FROM provider_macro_plan_staged_pages AS page
                  JOIN provider_capture_bindings AS binding
                    ON binding.binding_digest = page.binding_digest
                  WHERE page.session_id = session.session_id
                    AND page.page_ordinal = NEW.response_ordinal
                    AND page.capture_observation_digest = NEW.capture_observation_digest
                    AND binding.sealed_capture_receipt_digest = NEW.sealed_capture_receipt_digest
              ))
      )
)
BEGIN
    SELECT RAISE(ABORT, 'invalid provider macro-plan terminal completion');
END;

CREATE TRIGGER provider_macro_plan_finalizations_guarded_insert
BEFORE INSERT ON provider_macro_plan_finalizations
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_macro_plan_sessions AS session
    JOIN provider_macro_plan_terminal_completions AS terminal
      ON terminal.session_id = session.session_id
    JOIN ingest_runs AS run ON run.run_id = NEW.run_id
    WHERE session.session_id = NEW.session_id
      AND session.state = 'complete'
      AND run.state = 'reserved'
      AND run.operation = 'persist'
      AND run.source_id = session.source_id
      AND run.payload_digest = NEW.publication_digest
      AND (
          (NEW.predecessor_publication_digest IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM provider_macro_plan_published_heads AS head
                  WHERE head.analytical_dataset = session.analytical_dataset
                    AND head.source_id = session.source_id
                    AND head.provider_dataset = session.provider_dataset
              ))
          OR EXISTS (
              SELECT 1 FROM provider_macro_plan_published_heads AS head
              WHERE head.analytical_dataset = session.analytical_dataset
                AND head.source_id = session.source_id
                AND head.provider_dataset = session.provider_dataset
                AND head.publication_digest = NEW.predecessor_publication_digest
                AND head.analytical_dataset = NEW.predecessor_manifest_dataset_id
                AND head.manifest_version = NEW.predecessor_manifest_version
                AND head.completed_checkpoint_version = NEW.predecessor_checkpoint_version
                AND head.completed_checkpoint_digest = NEW.predecessor_checkpoint_digest
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'invalid provider macro-plan finalization');
END;

CREATE TRIGGER provider_macro_plan_finalized_groups_guarded_insert
BEFORE INSERT ON provider_macro_plan_finalized_groups
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_macro_plan_finalizations AS finalization
    JOIN provider_macro_plan_sessions AS session USING (session_id)
    JOIN ingest_runs AS run ON run.run_id = finalization.run_id
    WHERE finalization.session_id = NEW.session_id
      AND session.state = 'complete'
      AND run.state = 'reserved'
      AND NOT EXISTS (
          SELECT 1 FROM provider_macro_plan_publications AS publication
          WHERE publication.session_id = finalization.session_id
      )
      AND (
          (finalization.predecessor_publication_digest IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM provider_macro_plan_published_heads AS head
                  WHERE head.analytical_dataset = session.analytical_dataset
                    AND head.source_id = session.source_id
                    AND head.provider_dataset = session.provider_dataset
              ))
          OR EXISTS (
              SELECT 1 FROM provider_macro_plan_published_heads AS head
              WHERE head.analytical_dataset = session.analytical_dataset
                AND head.source_id = session.source_id
                AND head.provider_dataset = session.provider_dataset
                AND head.publication_digest = finalization.predecessor_publication_digest
                AND head.analytical_dataset = finalization.predecessor_manifest_dataset_id
                AND head.manifest_version = finalization.predecessor_manifest_version
                AND head.completed_checkpoint_version =
                    finalization.predecessor_checkpoint_version
                AND head.completed_checkpoint_digest =
                    finalization.predecessor_checkpoint_digest
          )
      )
      AND NEW.output_ordinal = (
          SELECT COUNT(*) FROM provider_macro_plan_finalized_groups AS retained
          WHERE retained.session_id = finalization.session_id
      )
      AND NEW.first_page_ordinal = COALESCE((
          SELECT SUM(retained.page_count)
          FROM provider_macro_plan_finalized_groups AS retained
          WHERE retained.session_id = finalization.session_id
      ), 0)
      AND NEW.first_page_ordinal + NEW.page_count <= session.data_page_count
      AND NEW.page_count = (
          SELECT COUNT(*) FROM provider_macro_plan_staged_pages AS page
          WHERE page.session_id = finalization.session_id
            AND page.page_ordinal >= NEW.first_page_ordinal
            AND page.page_ordinal < NEW.first_page_ordinal + NEW.page_count
      )
      AND NEW.row_count = (
          SELECT SUM(page.canonical_record_count)
          FROM provider_macro_plan_staged_pages AS page
          WHERE page.session_id = finalization.session_id
            AND page.page_ordinal >= NEW.first_page_ordinal
            AND page.page_ordinal < NEW.first_page_ordinal + NEW.page_count
      )
)
BEGIN
    SELECT RAISE(ABORT, 'invalid provider macro-plan finalized group');
END;

CREATE TRIGGER provider_macro_plan_publications_guarded_insert
BEFORE INSERT ON provider_macro_plan_publications
WHEN NOT EXISTS (
    SELECT 1
    FROM analytical_generations AS generation
    JOIN analytical_generation_source_inputs AS source_input
      ON source_input.generation_sequence = generation.generation_sequence
    JOIN ingest_runs AS run ON run.run_id = source_input.run_id
    JOIN provider_macro_plan_sessions AS session ON session.session_id = NEW.session_id
    JOIN provider_macro_plan_terminal_completions AS terminal
      ON terminal.session_id = session.session_id
    JOIN provider_macro_plan_finalizations AS finalization
      ON finalization.session_id = session.session_id
    WHERE generation.generation_sequence = NEW.generation_sequence
      AND generation.dataset_id = NEW.manifest_dataset_id
      AND generation.manifest_version = NEW.manifest_version
      AND generation.schema_name = NEW.manifest_schema_name
      AND generation.schema_version = NEW.manifest_schema_version
      AND generation.schema_fingerprint = NEW.manifest_schema_fingerprint
      AND generation.content_hash = NEW.manifest_content_hash
      AND generation.anchor_manifest_id = NEW.anchor_manifest_id
      AND generation.generation_kind = 'ingest'
      AND NEW.analytical_row_count = (
          SELECT SUM(object.row_count)
          FROM artifacts AS output
          JOIN analytical_generation_objects AS object
            ON object.dataset_id = generation.dataset_id
           AND object.manifest_version = generation.manifest_version
           AND object.artifact_id = output.artifact_id
          WHERE output.run_id = run.run_id
      )
      AND run.run_id = NEW.run_id
      AND run.state = 'reserved'
      AND run.operation = 'persist'
      AND run.source_id = NEW.source_id
      AND run.payload_digest = NEW.publication_digest
      AND finalization.run_id = run.run_id
      AND finalization.publication_digest = NEW.publication_digest
      AND finalization.predecessor_publication_digest IS NEW.predecessor_publication_digest
      AND finalization.predecessor_manifest_dataset_id IS NEW.predecessor_manifest_dataset_id
      AND finalization.predecessor_manifest_version IS NEW.predecessor_manifest_version
      AND finalization.predecessor_checkpoint_version IS NEW.predecessor_checkpoint_version
      AND finalization.predecessor_checkpoint_digest IS NEW.predecessor_checkpoint_digest
      AND session.state = 'complete'
      AND session.analytical_dataset = NEW.manifest_dataset_id
      AND session.source_id = NEW.source_id
      AND session.metadata_revision = NEW.metadata_revision
      AND session.provider_dataset = NEW.provider_dataset
      AND session.source_generation_digest = NEW.source_generation_digest
      AND session.plan_identity = NEW.plan_identity
      AND session.response_count = NEW.response_count
      AND session.data_page_count = NEW.data_page_count
      AND session.analytical_row_count = NEW.analytical_row_count
      AND session.state_version = NEW.completed_checkpoint_version
      AND session.checkpoint_digest = NEW.completed_checkpoint_digest
      AND terminal.adapter_completion_digest = NEW.adapter_completion_digest
      AND terminal.sealed_capture_receipt_digest = NEW.terminal_seal_digest
      AND (SELECT COUNT(*) FROM provider_macro_plan_staged_pages AS page
           WHERE page.session_id = session.session_id) = session.data_page_count
      AND (SELECT SUM(finalized.page_count)
           FROM provider_macro_plan_finalized_groups AS finalized
           WHERE finalized.session_id = session.session_id) = session.data_page_count
      AND (SELECT COUNT(*)
           FROM provider_macro_plan_finalized_groups AS finalized
           WHERE finalized.session_id = session.session_id) = (
               SELECT COUNT(*) FROM artifacts AS output WHERE output.run_id = run.run_id
           )
      AND NOT EXISTS (
          SELECT 1
          FROM provider_macro_plan_finalized_groups AS finalized
          WHERE finalized.session_id = session.session_id
            AND NOT EXISTS (
                SELECT 1
                FROM artifacts AS output
                JOIN analytical_generation_objects AS object
                  ON object.dataset_id = generation.dataset_id
                 AND object.manifest_version = generation.manifest_version
                 AND object.artifact_id = output.artifact_id
                WHERE output.run_id = run.run_id
                  AND output.publication_ordinal = finalized.output_ordinal
                  AND output.relative_reference = finalized.object_relative_reference
                  AND output.content_algorithm = 1
                  AND output.content_digest = finalized.object_content_hash
                  AND output.size_bytes = finalized.object_size_bytes
                  AND object.row_count = finalized.row_count
                  AND object.size_bytes = finalized.object_size_bytes
                  AND object.content_hash = finalized.object_content_hash
                  AND object.lineage_hash = finalized.object_lineage_hash
            )
      )
      AND (SELECT COUNT(*) FROM ingest_run_provider_capture_bindings AS input
           WHERE input.run_id = run.run_id) = session.data_page_count
      AND (SELECT MIN(input.input_ordinal)
           FROM ingest_run_provider_capture_bindings AS input
           WHERE input.run_id = run.run_id) = 0
      AND (SELECT MAX(input.input_ordinal)
           FROM ingest_run_provider_capture_bindings AS input
           WHERE input.run_id = run.run_id) = session.data_page_count - 1
      AND NOT EXISTS (
          SELECT 1
          FROM artifacts AS output
          WHERE output.run_id = run.run_id
            AND (
                NOT EXISTS (
                    SELECT 1
                    FROM ingest_run_provider_capture_bindings AS input
                    WHERE input.run_id = run.run_id
                      AND input.output_artifact_ordinal = output.publication_ordinal
                )
                OR (SELECT MIN(input.object_input_ordinal)
                    FROM ingest_run_provider_capture_bindings AS input
                    WHERE input.run_id = run.run_id
                      AND input.output_artifact_ordinal = output.publication_ordinal) <> 0
                OR (SELECT MAX(input.object_input_ordinal)
                    FROM ingest_run_provider_capture_bindings AS input
                    WHERE input.run_id = run.run_id
                      AND input.output_artifact_ordinal = output.publication_ordinal) <> (
                    SELECT COUNT(*) - 1
                    FROM ingest_run_provider_capture_bindings AS input
                    WHERE input.run_id = run.run_id
                      AND input.output_artifact_ordinal = output.publication_ordinal
                )
            )
      )
      AND (SELECT SUM(binding.canonical_record_count)
           FROM ingest_run_provider_capture_bindings AS input
           JOIN provider_capture_bindings AS binding
             ON binding.binding_digest = input.binding_digest
           WHERE input.run_id = run.run_id) = session.analytical_row_count
      AND (SELECT COUNT(*)
           FROM analytical_generation_provider_capture_bindings AS input
           WHERE input.generation_sequence = generation.generation_sequence
             AND input.run_id = run.run_id) = session.data_page_count
)
BEGIN
    SELECT RAISE(ABORT, 'invalid provider macro-plan publication');
END;

CREATE TRIGGER ingest_runs_provider_capture_mapping_guarded_success
BEFORE UPDATE ON ingest_runs
WHEN NEW.state = 'succeeded'
 AND EXISTS (
     SELECT 1 FROM ingest_run_provider_capture_bindings AS input
     WHERE input.run_id = NEW.run_id
 )
 AND (
     NOT EXISTS (
         SELECT 1 FROM analytical_generation_source_inputs AS source_input
         WHERE source_input.run_id = NEW.run_id
     )
     OR (SELECT MIN(input.input_ordinal)
         FROM ingest_run_provider_capture_bindings AS input
         WHERE input.run_id = NEW.run_id) <> 0
     OR (SELECT MAX(input.input_ordinal)
         FROM ingest_run_provider_capture_bindings AS input
         WHERE input.run_id = NEW.run_id) <> (
         SELECT COUNT(*) - 1
         FROM ingest_run_provider_capture_bindings AS input
         WHERE input.run_id = NEW.run_id
     )
     OR EXISTS (
         SELECT 1
         FROM artifacts AS output
         JOIN analytical_generation_source_inputs AS source_input
           ON source_input.run_id = output.run_id
         JOIN analytical_generations AS generation
           ON generation.generation_sequence = source_input.generation_sequence
         JOIN analytical_generation_objects AS object
           ON object.dataset_id = generation.dataset_id
          AND object.manifest_version = generation.manifest_version
          AND object.artifact_id = output.artifact_id
         WHERE output.run_id = NEW.run_id
           AND (
               NOT EXISTS (
                   SELECT 1
                   FROM ingest_run_provider_capture_bindings AS input
                   WHERE input.run_id = output.run_id
                     AND input.output_artifact_ordinal = output.publication_ordinal
               )
               OR (SELECT MIN(input.object_input_ordinal)
                   FROM ingest_run_provider_capture_bindings AS input
                   WHERE input.run_id = output.run_id
                     AND input.output_artifact_ordinal = output.publication_ordinal) <> 0
               OR (SELECT MAX(input.object_input_ordinal)
                   FROM ingest_run_provider_capture_bindings AS input
                   WHERE input.run_id = output.run_id
                     AND input.output_artifact_ordinal = output.publication_ordinal) <> (
                   SELECT COUNT(*) - 1
                   FROM ingest_run_provider_capture_bindings AS input
                   WHERE input.run_id = output.run_id
                     AND input.output_artifact_ordinal = output.publication_ordinal
               )
               OR object.row_count <> COALESCE((
                   SELECT SUM(binding.canonical_record_count)
                   FROM ingest_run_provider_capture_bindings AS input
                   JOIN provider_capture_bindings AS binding
                     ON binding.binding_digest = input.binding_digest
                   WHERE input.run_id = output.run_id
                     AND input.output_artifact_ordinal = output.publication_ordinal
               ), -1)
           )
     )
 )
BEGIN
    SELECT RAISE(ABORT, 'provider capture inputs do not exactly cover output artifacts');
END;

CREATE TRIGGER ingest_runs_provider_publication_mapping_guarded_success
BEFORE UPDATE ON ingest_runs
WHEN NEW.state = 'succeeded'
 AND EXISTS (
     SELECT 1 FROM ingest_run_provider_publication_bindings AS input
     WHERE input.run_id = NEW.run_id AND input.active_dataset_id IS NULL
 )
 AND NOT EXISTS (
     SELECT 1 FROM ingest_run_provider_publication_bindings
     WHERE run_id=NEW.run_id AND publication_kind='provider_logical'
 )
 AND (
     NOT EXISTS (
         SELECT 1 FROM analytical_generation_source_inputs AS source_input
         WHERE source_input.run_id = NEW.run_id
     )
     OR (SELECT MIN(input.input_ordinal)
         FROM ingest_run_provider_publication_bindings AS input
         WHERE input.run_id = NEW.run_id) <> 0
     OR (SELECT MAX(input.input_ordinal)
         FROM ingest_run_provider_publication_bindings AS input
         WHERE input.run_id = NEW.run_id) <> (
         SELECT COUNT(*) - 1
         FROM ingest_run_provider_publication_bindings AS input
         WHERE input.run_id = NEW.run_id
     )
     OR EXISTS (
         SELECT 1
         FROM artifacts AS output
         JOIN analytical_generation_source_inputs AS source_input
           ON source_input.run_id = output.run_id
         JOIN analytical_generations AS generation
           ON generation.generation_sequence = source_input.generation_sequence
         JOIN analytical_generation_objects AS object
           ON object.dataset_id = generation.dataset_id
          AND object.manifest_version = generation.manifest_version
          AND object.artifact_id = output.artifact_id
         WHERE output.run_id = NEW.run_id
           AND (
               NOT EXISTS (
                   SELECT 1
                   FROM ingest_run_provider_publication_bindings AS input
                   WHERE input.run_id = output.run_id
                     AND input.output_artifact_ordinal = output.publication_ordinal
               )
               OR (SELECT MIN(input.object_input_ordinal)
                   FROM ingest_run_provider_publication_bindings AS input
                   WHERE input.run_id = output.run_id
                     AND input.output_artifact_ordinal = output.publication_ordinal) <> 0
               OR (SELECT MAX(input.object_input_ordinal)
                   FROM ingest_run_provider_publication_bindings AS input
                   WHERE input.run_id = output.run_id
                     AND input.output_artifact_ordinal = output.publication_ordinal) <> (
                   SELECT COUNT(*) - 1
                   FROM ingest_run_provider_publication_bindings AS input
                   WHERE input.run_id = output.run_id
                     AND input.output_artifact_ordinal = output.publication_ordinal
               )
               OR object.row_count <> COALESCE((
                   SELECT SUM(
                       CASE input.publication_kind
                           WHEN 'option_snapshots' THEN 1 + (
                               SELECT binding.canonical_row_count
                               FROM provider_option_market_bindings AS binding
                               WHERE binding.option_binding_digest = input.option_binding_digest
                           )
                           WHEN 'option_expirations' THEN 1 + (
                               SELECT binding.canonical_row_count
                               FROM provider_option_market_bindings AS binding
                               WHERE binding.option_binding_digest = input.option_binding_digest
                           )
                       END
                   )
                   FROM ingest_run_provider_publication_bindings AS input
                   WHERE input.run_id = output.run_id
                     AND input.output_artifact_ordinal = output.publication_ordinal
               ), -1)
           )
     )
 )
BEGIN
    SELECT RAISE(ABORT, 'provider publications do not exactly cover output artifacts');
END;

CREATE TRIGGER ingest_runs_provider_logical_mapping_guarded_success
BEFORE UPDATE ON ingest_runs
WHEN NEW.state='succeeded'
 AND EXISTS (SELECT 1 FROM ingest_run_provider_publication_bindings
             WHERE run_id=NEW.run_id AND publication_kind='provider_logical')
 AND (
     (SELECT COUNT(*) FROM ingest_run_provider_publication_bindings WHERE run_id=NEW.run_id)<>1
     OR EXISTS (SELECT 1 FROM ingest_run_provider_capture_bindings WHERE run_id=NEW.run_id)
     OR NOT EXISTS (
         SELECT 1 FROM ingest_run_provider_publication_bindings AS input
         JOIN provider_logical_publication_bindings AS binding ON binding.binding_digest=input.logical_binding_digest
         WHERE input.run_id=NEW.run_id AND binding.canonical_partition_count>0
           AND binding.canonical_partition_count=(
               SELECT COUNT(*) FROM ingest_run_provider_logical_partition_artifacts AS placement
               WHERE placement.run_id=NEW.run_id AND placement.logical_binding_digest=binding.binding_digest)
     )
     OR NOT EXISTS (SELECT 1 FROM artifacts WHERE run_id=NEW.run_id)
     OR EXISTS (
         SELECT 1 FROM artifacts AS output WHERE output.run_id=NEW.run_id
           AND NOT EXISTS (
               SELECT 1 FROM dataset_manifests AS anchor
               JOIN analytical_generations AS generation ON generation.anchor_manifest_id=anchor.manifest_id
                 AND generation.generation_kind='ingest'
               JOIN analytical_generation_source_inputs AS source
                 ON source.generation_sequence=generation.generation_sequence AND source.run_id=output.run_id
               JOIN analytical_generation_objects AS object ON object.dataset_id=generation.dataset_id
                 AND object.manifest_version=generation.manifest_version AND object.artifact_id=output.artifact_id
               WHERE anchor.run_id=output.run_id
                 AND object.row_count=(
                     SELECT SUM(expected.row_count)
                     FROM ingest_run_provider_logical_partition_artifacts AS placement
                     JOIN provider_logical_publication_canonical_expectations AS expected
                       ON expected.binding_digest=placement.logical_binding_digest
                      AND expected.partition_ordinal=placement.partition_ordinal
                     WHERE placement.run_id=output.run_id
                       AND placement.output_artifact_ordinal=output.publication_ordinal)
           )
     )
 )
BEGIN
    SELECT RAISE(ABORT, 'logical canonical partitions do not exactly cover output artifacts');
END;

CREATE TRIGGER provider_macro_plan_published_heads_guarded_insert
BEFORE INSERT ON provider_macro_plan_published_heads
WHEN NOT EXISTS (
    SELECT 1 FROM provider_macro_plan_publications AS publication
    WHERE publication.publication_digest = NEW.publication_digest
      AND publication.predecessor_publication_digest IS NULL
      AND publication.session_id = NEW.session_id
      AND publication.generation_sequence = NEW.generation_sequence
      AND publication.manifest_dataset_id = NEW.analytical_dataset
      AND publication.source_id = NEW.source_id
      AND publication.provider_dataset = NEW.provider_dataset
      AND publication.manifest_version = NEW.manifest_version
      AND publication.completed_checkpoint_version = NEW.completed_checkpoint_version
      AND publication.completed_checkpoint_digest = NEW.completed_checkpoint_digest
)
BEGIN
    SELECT RAISE(ABORT, 'invalid initial provider macro-plan published head');
END;

CREATE TRIGGER provider_macro_plan_published_heads_guarded_update
BEFORE UPDATE ON provider_macro_plan_published_heads
WHEN NEW.analytical_dataset <> OLD.analytical_dataset
    OR NEW.source_id <> OLD.source_id
    OR NEW.provider_dataset <> OLD.provider_dataset
    OR NEW.publication_digest = OLD.publication_digest
    OR NEW.manifest_version <= OLD.manifest_version
    OR NEW.advanced_at_ns < OLD.advanced_at_ns
    OR NOT EXISTS (
        SELECT 1 FROM provider_macro_plan_publications AS publication
        WHERE publication.publication_digest = NEW.publication_digest
          AND publication.predecessor_publication_digest = OLD.publication_digest
          AND publication.predecessor_manifest_dataset_id = OLD.analytical_dataset
          AND publication.predecessor_manifest_version = OLD.manifest_version
          AND publication.predecessor_checkpoint_version = OLD.completed_checkpoint_version
          AND publication.predecessor_checkpoint_digest = OLD.completed_checkpoint_digest
          AND publication.session_id = NEW.session_id
          AND publication.generation_sequence = NEW.generation_sequence
          AND publication.manifest_dataset_id = NEW.analytical_dataset
          AND publication.source_id = NEW.source_id
          AND publication.provider_dataset = NEW.provider_dataset
          AND publication.manifest_version = NEW.manifest_version
          AND publication.completed_checkpoint_version = NEW.completed_checkpoint_version
          AND publication.completed_checkpoint_digest = NEW.completed_checkpoint_digest
    )
BEGIN
    SELECT RAISE(ABORT, 'invalid provider macro-plan published-head successor');
END;

CREATE TRIGGER provider_macro_plan_sessions_immutable_delete
BEFORE DELETE ON provider_macro_plan_sessions BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan sessions are immutable');
END;

CREATE TRIGGER provider_macro_plan_values_immutable_update
BEFORE UPDATE ON provider_macro_plan_values BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan values are immutable');
END;

CREATE TRIGGER provider_macro_plan_values_guarded_delete
BEFORE DELETE ON provider_macro_plan_values
WHEN OLD.value_kind <> 'checkpoint'
    OR EXISTS (
        SELECT 1 FROM provider_macro_plan_sessions AS session
        WHERE session.checkpoint_value_id = OLD.value_id
    )
    OR EXISTS (
        SELECT 1 FROM provider_macro_plan_value_chunks AS chunk
        WHERE chunk.value_id = OLD.value_id
    )
BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan value cannot be retired');
END;

CREATE TRIGGER provider_macro_plan_value_chunks_immutable_update
BEFORE UPDATE ON provider_macro_plan_value_chunks BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan value chunks are immutable');
END;

CREATE TRIGGER provider_macro_plan_value_chunks_guarded_delete
BEFORE DELETE ON provider_macro_plan_value_chunks
WHEN NOT EXISTS (
    SELECT 1
    FROM provider_macro_plan_values AS value
    WHERE value.value_id = OLD.value_id
      AND value.value_kind = 'checkpoint'
      AND NOT EXISTS (
          SELECT 1 FROM provider_macro_plan_sessions AS session
          WHERE session.checkpoint_value_id = value.value_id
      )
)
BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan value chunk cannot be retired');
END;

CREATE TRIGGER provider_macro_plan_staged_pages_immutable_update
BEFORE UPDATE ON provider_macro_plan_staged_pages BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan staged pages are immutable');
END;

CREATE TRIGGER provider_macro_plan_staged_pages_immutable_delete
BEFORE DELETE ON provider_macro_plan_staged_pages BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan staged pages are immutable');
END;

CREATE TRIGGER provider_macro_plan_terminal_completions_immutable_update
BEFORE UPDATE ON provider_macro_plan_terminal_completions BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan terminal completions are immutable');
END;

CREATE TRIGGER provider_macro_plan_terminal_completions_immutable_delete
BEFORE DELETE ON provider_macro_plan_terminal_completions BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan terminal completions are immutable');
END;

CREATE TRIGGER provider_macro_plan_finalizations_immutable_update
BEFORE UPDATE ON provider_macro_plan_finalizations BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan finalizations are immutable');
END;

CREATE TRIGGER provider_macro_plan_finalizations_immutable_delete
BEFORE DELETE ON provider_macro_plan_finalizations BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan finalizations are immutable');
END;

CREATE TRIGGER provider_macro_plan_finalized_groups_immutable_update
BEFORE UPDATE ON provider_macro_plan_finalized_groups BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan finalized groups are immutable');
END;

CREATE TRIGGER provider_macro_plan_finalized_groups_immutable_delete
BEFORE DELETE ON provider_macro_plan_finalized_groups BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan finalized groups are immutable');
END;

CREATE TRIGGER provider_macro_plan_publications_immutable_update
BEFORE UPDATE ON provider_macro_plan_publications BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan publications are immutable');
END;

CREATE TRIGGER provider_macro_plan_publications_immutable_delete
BEFORE DELETE ON provider_macro_plan_publications BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan publications are immutable');
END;

CREATE TRIGGER provider_macro_plan_published_heads_immutable_delete
BEFORE DELETE ON provider_macro_plan_published_heads BEGIN
    SELECT RAISE(ABORT, 'provider macro-plan published heads cannot be deleted');
END;

-- Provider-neutral Fund NAV publication authority. Provider coordinates are retained only as
-- immutable receipt evidence; request-time lookup authority is canonical instrument + schema +
-- family + versioned neutral policy under an internal knowledge cutoff.
CREATE TABLE fund_nav_publications (
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
    instrument_revision_digest BLOB NOT NULL
        REFERENCES market_data_instrument_revisions(revision_digest),
    provider_instrument_id TEXT NOT NULL CHECK (
        length(CAST(provider_instrument_id AS BLOB)) BETWEEN 1 AND 256
    ),
    provider_product TEXT NOT NULL CHECK (
        length(CAST(provider_product AS BLOB)) BETWEEN 1 AND 512
    ),
    provider_channel TEXT NOT NULL CHECK (
        length(CAST(provider_channel AS BLOB)) BETWEEN 1 AND 512
    ),
    valuation_basis TEXT NOT NULL CHECK (valuation_basis = 'per_share'),
    currency TEXT NOT NULL CHECK (
        length(CAST(currency AS BLOB)) BETWEEN 1 AND 16
    ),
    source_family_digest BLOB NOT NULL CHECK (
        length(source_family_digest) = 32
        AND source_family_digest <> zeroblob(32)
    ),
    row_set_digest BLOB NOT NULL CHECK (
        length(row_set_digest) = 32 AND row_set_digest <> zeroblob(32)
    ),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 100000),
    first_nav_date TEXT NOT NULL CHECK (
        length(first_nav_date) = 10 AND date(first_nav_date) = first_nav_date
    ),
    last_nav_date TEXT NOT NULL CHECK (
        length(last_nav_date) = 10
        AND date(last_nav_date) = last_nav_date
        AND last_nav_date >= first_nav_date
    ),
    max_available_at_ns INTEGER NOT NULL,
    max_received_at_ns INTEGER NOT NULL,
    max_ingested_at_ns INTEGER NOT NULL CHECK (
        max_ingested_at_ns >= max_available_at_ns
        AND max_ingested_at_ns >= max_received_at_ns
    ),
    max_canonical_published_at_ns INTEGER NOT NULL CHECK (
        max_canonical_published_at_ns >= max_available_at_ns
        AND max_canonical_published_at_ns >= max_received_at_ns
        AND max_canonical_published_at_ns >= max_ingested_at_ns
    ),
    published_at_ns INTEGER NOT NULL CHECK (
        published_at_ns >= max_canonical_published_at_ns
    ),
    has_preliminary INTEGER NOT NULL CHECK (has_preliminary IN (0, 1)),
    has_final INTEGER NOT NULL CHECK (has_final IN (0, 1)),
    has_correction INTEGER NOT NULL CHECK (has_correction IN (0, 1)),
    receipt_json TEXT NOT NULL CHECK (
        length(CAST(receipt_json AS BLOB)) BETWEEN 2 AND 4194304
        AND json_valid(receipt_json)
    ),
    FOREIGN KEY (origin_generation_sequence, binding_digest)
        REFERENCES analytical_generation_provider_capture_bindings(
            generation_sequence, binding_digest
        )
) STRICT, WITHOUT ROWID;

CREATE TABLE analytical_generation_fund_nav_inputs (
    generation_sequence INTEGER NOT NULL
        REFERENCES analytical_generations(generation_sequence),
    input_ordinal INTEGER NOT NULL CHECK (input_ordinal BETWEEN 0 AND 4095),
    publication_receipt_digest BLOB NOT NULL
        REFERENCES fund_nav_publications(publication_receipt_digest),
    PRIMARY KEY (generation_sequence, input_ordinal),
    UNIQUE (generation_sequence, publication_receipt_digest)
) STRICT, WITHOUT ROWID;

CREATE INDEX fund_nav_publications_neutral_latest
ON fund_nav_publications(
    instrument_id,
    published_at_ns DESC,
    origin_generation_sequence DESC,
    publication_receipt_digest
);

CREATE INDEX fund_nav_publications_origin
ON fund_nav_publications(origin_generation_sequence, publication_receipt_digest);

CREATE INDEX analytical_generation_fund_nav_receipt
ON analytical_generation_fund_nav_inputs(
    publication_receipt_digest,
    generation_sequence
);

CREATE TRIGGER fund_nav_publications_guarded_insert
BEFORE INSERT ON fund_nav_publications
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
      AND object.row_count = NEW.row_count
      AND capture_input.binding_digest = NEW.binding_digest
      AND capture_input.source_id = NEW.source_id
      AND capture.source_id = NEW.source_id
      AND capture.provider_dataset = NEW.provider_dataset
      AND capture.capture_content_digest = NEW.capture_content_digest
      AND capture.capture_observation_digest = NEW.capture_observation_digest
      AND capture.recorded_at_ns = NEW.capture_recorded_at_ns
      AND instrument_revision.published_at_ns <= run.requested_at_ns
      AND generation.created_at_ns = NEW.published_at_ns
      AND manifest.created_at_ns = NEW.published_at_ns
)
BEGIN
    SELECT RAISE(ABORT, 'Fund NAV publication lineage is invalid');
END;

CREATE TRIGGER analytical_generation_fund_nav_inputs_guarded_insert
BEFORE INSERT ON analytical_generation_fund_nav_inputs
WHEN NOT EXISTS (
    SELECT 1
    FROM fund_nav_publications AS publication
    JOIN analytical_generation_provider_capture_bindings AS capture_input
      ON capture_input.generation_sequence = NEW.generation_sequence
     AND capture_input.binding_digest = publication.binding_digest
    WHERE publication.publication_receipt_digest = NEW.publication_receipt_digest
      AND publication.origin_generation_sequence = NEW.generation_sequence
)
BEGIN
    SELECT RAISE(ABORT, 'analytical generation Fund NAV input is invalid');
END;

CREATE TRIGGER fund_nav_publications_immutable_update
BEFORE UPDATE ON fund_nav_publications BEGIN
    SELECT RAISE(ABORT, 'Fund NAV publications are immutable');
END;

CREATE TRIGGER fund_nav_publications_immutable_delete
BEFORE DELETE ON fund_nav_publications BEGIN
    SELECT RAISE(ABORT, 'Fund NAV publications are immutable');
END;

CREATE TRIGGER analytical_generation_fund_nav_inputs_immutable_update
BEFORE UPDATE ON analytical_generation_fund_nav_inputs BEGIN
    SELECT RAISE(ABORT, 'analytical generation Fund NAV inputs are immutable');
END;

CREATE TRIGGER analytical_generation_fund_nav_inputs_immutable_delete
BEFORE DELETE ON analytical_generation_fund_nav_inputs BEGIN
    SELECT RAISE(ABORT, 'analytical generation Fund NAV inputs are immutable');
END;

-- Root-owned addition to the existing analytical catalog migration; no new database or raw root.
CREATE TABLE provider_capture_originals (
 session_digest BLOB NOT NULL CHECK(length(session_digest)=32 AND session_digest<>zeroblob(32)),
 ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 65534),
 expected_count INTEGER NOT NULL CHECK(expected_count BETWEEN 1 AND 65535 AND ordinal<expected_count),
 dataset_id TEXT NOT NULL CHECK(length(CAST(dataset_id AS BLOB)) BETWEEN 1 AND 256),
 context_bytes BLOB NOT NULL CHECK(length(context_bytes)<=131072 AND ((ordinal=0 AND length(context_bytes)>0) OR (ordinal>0 AND length(context_bytes)=0))),
 decoded_at_ns INTEGER NOT NULL,
 capture_observation_digest BLOB NOT NULL REFERENCES provider_raw_observations(capture_observation_digest),
 raw_claim_digest BLOB NOT NULL,
 physical_receipt_digest BLOB NOT NULL,
 predecessor_digest BLOB,
 original_digest BLOB NOT NULL UNIQUE CHECK(length(original_digest)=32 AND original_digest<>zeroblob(32)),
 rights_id BLOB NOT NULL REFERENCES source_rights(rights_id),
 retained_at_ns INTEGER NOT NULL CHECK(decoded_at_ns<=retained_at_ns),
 published_binding BLOB REFERENCES provider_capture_bindings(binding_digest),
 published_option_binding BLOB REFERENCES provider_option_market_bindings(option_binding_digest),
 published_logical_binding BLOB REFERENCES provider_logical_publication_bindings(binding_digest),
 CHECK((published_binding IS NOT NULL)+(published_option_binding IS NOT NULL)+(published_logical_binding IS NOT NULL)<=1),
 PRIMARY KEY(session_digest,ordinal), UNIQUE(raw_claim_digest),
 FOREIGN KEY(raw_claim_digest,physical_receipt_digest) REFERENCES sealed_raw_objects(raw_claim_digest,physical_receipt_digest),
 CHECK((ordinal=0 AND predecessor_digest IS NULL) OR (ordinal>0 AND length(predecessor_digest)=32))
) STRICT, WITHOUT ROWID;
CREATE INDEX provider_capture_original_pending ON provider_capture_originals(session_digest,ordinal) WHERE published_binding IS NULL AND published_option_binding IS NULL AND published_logical_binding IS NULL;
CREATE TRIGGER provider_capture_original_insert BEFORE INSERT ON provider_capture_originals
WHEN NEW.published_binding IS NOT NULL OR NEW.published_option_binding IS NOT NULL OR NEW.published_logical_binding IS NOT NULL
 OR NOT EXISTS(SELECT 1 FROM provider_raw_observations AS observation
   JOIN source_rights AS rights ON rights.rights_id=NEW.rights_id AND rights.source_id=observation.source_id
   JOIN provider_raw_observation_objects AS object ON object.capture_observation_digest=observation.capture_observation_digest AND object.input_ordinal=0
   JOIN sealed_raw_objects AS raw ON raw.raw_claim_digest=object.raw_claim_digest AND raw.raw_claim_kind='journal_segment'
   WHERE observation.capture_observation_digest=NEW.capture_observation_digest AND observation.page_count=1
    AND object.raw_claim_digest=NEW.raw_claim_digest AND object.physical_receipt_digest=NEW.physical_receipt_digest
    AND rights.payload_algorithm=1 AND rights.payload_digest=NEW.capture_observation_digest AND (rights.operation_mask & 4)<>0
    AND rights.admitted_at_ns<=NEW.retained_at_ns AND (rights.authorization_expires_at_ns IS NULL OR rights.authorization_expires_at_ns>NEW.retained_at_ns))
 OR (NEW.ordinal=0 AND EXISTS(SELECT 1 FROM provider_capture_originals AS pending
   JOIN provider_raw_observations AS prior ON prior.capture_observation_digest=pending.capture_observation_digest
   JOIN provider_raw_observations AS incoming ON incoming.capture_observation_digest=NEW.capture_observation_digest
   WHERE pending.ordinal=0 AND pending.published_binding IS NULL AND pending.published_option_binding IS NULL AND pending.published_logical_binding IS NULL AND pending.session_digest<>NEW.session_digest AND prior.source_id=incoming.source_id))
 OR (NEW.ordinal>0 AND NOT EXISTS(SELECT 1 FROM provider_capture_originals AS previous
   WHERE previous.session_digest=NEW.session_digest AND previous.ordinal=NEW.ordinal-1
    AND previous.original_digest=NEW.predecessor_digest AND previous.expected_count=NEW.expected_count
    AND previous.dataset_id=NEW.dataset_id AND previous.published_binding IS NULL AND previous.published_option_binding IS NULL AND previous.published_logical_binding IS NULL AND previous.decoded_at_ns<=NEW.decoded_at_ns))
BEGIN SELECT RAISE(ABORT,'original capture requires exact bounded custody predecessor and raw Persist grant'); END;
CREATE TRIGGER provider_capture_original_update BEFORE UPDATE ON provider_capture_originals
WHEN OLD.published_binding IS NOT NULL OR OLD.published_option_binding IS NOT NULL OR OLD.published_logical_binding IS NOT NULL
 OR ((NEW.published_binding IS NOT NULL)+(NEW.published_option_binding IS NOT NULL)+(NEW.published_logical_binding IS NOT NULL)<>1)
 OR NEW.session_digest<>OLD.session_digest OR NEW.ordinal<>OLD.ordinal OR NEW.expected_count<>OLD.expected_count
 OR NEW.dataset_id<>OLD.dataset_id OR NEW.context_bytes<>OLD.context_bytes OR NEW.decoded_at_ns<>OLD.decoded_at_ns
 OR NEW.capture_observation_digest<>OLD.capture_observation_digest OR NEW.raw_claim_digest<>OLD.raw_claim_digest
 OR NEW.physical_receipt_digest<>OLD.physical_receipt_digest OR NEW.predecessor_digest IS NOT OLD.predecessor_digest
 OR NEW.original_digest<>OLD.original_digest OR NEW.rights_id<>OLD.rights_id OR NEW.retained_at_ns<>OLD.retained_at_ns
 OR NOT (EXISTS(SELECT 1 FROM ingest_run_provider_capture_bindings AS binding JOIN ingest_runs AS run ON run.run_id=binding.run_id
   JOIN provider_capture_binding_objects AS object ON object.binding_digest=binding.binding_digest
   WHERE binding.binding_digest=NEW.published_binding AND run.state='reserved'
    AND object.raw_claim_digest=NEW.raw_claim_digest AND object.physical_receipt_digest=NEW.physical_receipt_digest)
  OR EXISTS(SELECT 1 FROM ingest_run_provider_publication_bindings AS input
   JOIN ingest_runs AS run ON run.run_id=input.run_id AND run.state='reserved'
   JOIN provider_option_market_bindings AS binding ON binding.option_binding_digest=input.option_binding_digest
   JOIN json_each(binding.reference_dependencies_json) AS reference
   JOIN provider_capture_metadata_dependencies AS dependency
     ON lower(hex(dependency.dependency_digest))=json_extract(reference.value,'$.dependency_digest')
   JOIN provider_raw_observations AS original_capture ON original_capture.capture_observation_digest=NEW.capture_observation_digest
   JOIN provider_raw_observations AS option_capture ON option_capture.capture_observation_digest=binding.capture_observation_digest
   WHERE input.option_binding_digest=NEW.published_option_binding
    AND binding.publication_kind='option_snapshots'
    AND CAST(reference.key AS INTEGER)=NEW.ordinal
    AND json_array_length(binding.reference_dependencies_json)=NEW.expected_count
    AND dependency.capture_observation_digest=NEW.capture_observation_digest
    AND dependency.raw_claim_digest=NEW.raw_claim_digest
    AND dependency.physical_receipt_digest=NEW.physical_receipt_digest
    AND original_capture.source_id=option_capture.source_id
    AND original_capture.metadata_revision=option_capture.metadata_revision
    AND original_capture.source_revision_digest=option_capture.source_revision_digest
    AND NEW.retained_at_ns<=binding.recorded_at_ns)
  OR EXISTS(SELECT 1 FROM ingest_run_provider_publication_bindings AS input
   JOIN ingest_runs AS run ON run.run_id=input.run_id AND run.state='reserved' AND run.operation='persist'
   JOIN provider_logical_publication_bindings AS binding ON binding.binding_digest=input.logical_binding_digest
   JOIN provider_raw_observations AS original_capture ON original_capture.capture_observation_digest=NEW.capture_observation_digest
   WHERE input.publication_kind='provider_logical'
    AND input.logical_binding_digest=NEW.published_logical_binding
    AND input.source_id=original_capture.source_id
    AND run.source_id=original_capture.source_id
    AND binding.source_id=original_capture.source_id
    AND NEW.retained_at_ns<=binding.recorded_at_ns))
BEGIN SELECT RAISE(ABORT,'original capture publication requires its exact existing run transaction'); END;
CREATE TRIGGER provider_capture_original_delete BEFORE DELETE ON provider_capture_originals
BEGIN SELECT RAISE(ABORT,'original captures are retained for recovery'); END;

-- The manifest is inserted later in that same publication transaction. Bind the actual target
-- there, instead of guessing a dataset from an earlier raw capture or application-side label.
CREATE TRIGGER provider_capture_original_manifest_target BEFORE INSERT ON dataset_manifests
WHEN EXISTS(SELECT 1 FROM ingest_run_provider_capture_bindings AS binding
 JOIN provider_capture_originals AS original ON original.published_binding=binding.binding_digest
 WHERE binding.run_id=NEW.run_id AND original.dataset_id<>NEW.dataset_name)
 OR EXISTS(SELECT 1 FROM ingest_run_provider_publication_bindings AS binding
 JOIN provider_capture_originals AS original ON original.published_option_binding=binding.option_binding_digest
 WHERE binding.run_id=NEW.run_id AND original.dataset_id<>NEW.dataset_name)
 OR EXISTS(SELECT 1 FROM ingest_run_provider_publication_bindings AS binding
 JOIN provider_capture_originals AS original ON original.published_logical_binding=binding.logical_binding_digest
 WHERE binding.run_id=NEW.run_id AND original.dataset_id<>NEW.dataset_name)
BEGIN SELECT RAISE(ABORT,'original capture publication target differs from retained custody'); END;

-- Original issuer documents retained by canonical reference admission; never synthetic HTTP captures.
CREATE TABLE market_data_issuer_documents (
    document_digest BLOB PRIMARY KEY CHECK (length(document_digest) = 32 AND document_digest <> zeroblob(32)),
    source_reference TEXT NOT NULL CHECK (length(CAST(source_reference AS BLOB)) BETWEEN 1 AND 512),
    observed_at_ns INTEGER NOT NULL CHECK (observed_at_ns > 0),
    payload BLOB NOT NULL CHECK (length(payload) BETWEEN 1 AND 8388608),
    retained_at_ns INTEGER NOT NULL CHECK (retained_at_ns >= observed_at_ns)
) STRICT, WITHOUT ROWID;
CREATE TRIGGER market_data_issuer_documents_immutable_update
BEFORE UPDATE ON market_data_issuer_documents BEGIN
    SELECT RAISE(ABORT, 'issuer documents are immutable');
END;
CREATE TRIGGER market_data_issuer_documents_immutable_delete
BEFORE DELETE ON market_data_issuer_documents BEGIN
    SELECT RAISE(ABORT, 'issuer documents are immutable');
END;

CREATE TABLE market_data_issuer_reference_admissions (
    revision_digest BLOB NOT NULL REFERENCES market_data_instrument_revisions(revision_digest),
    listing_generation_digest BLOB NOT NULL REFERENCES listing_reference_generations(generation_digest),
    listing_record_revision TEXT NOT NULL CHECK (length(CAST(listing_record_revision AS BLOB)) BETWEEN 1 AND 512),
    identity_document_digest BLOB NOT NULL REFERENCES market_data_issuer_documents(document_digest),
    currency_document_digest BLOB NOT NULL REFERENCES market_data_issuer_documents(document_digest),
    admitted_at_ns INTEGER NOT NULL CHECK (admitted_at_ns > 0),
    PRIMARY KEY (revision_digest, listing_generation_digest),
    FOREIGN KEY (listing_generation_digest, listing_record_revision)
        REFERENCES listing_reference_memberships(generation_digest, record_revision)
) STRICT, WITHOUT ROWID;
CREATE TRIGGER market_data_issuer_reference_admissions_immutable_update
BEFORE UPDATE ON market_data_issuer_reference_admissions BEGIN
    SELECT RAISE(ABORT, 'issuer reference admissions are immutable');
END;
CREATE TRIGGER market_data_issuer_reference_admissions_immutable_delete
BEFORE DELETE ON market_data_issuer_reference_admissions BEGIN
    SELECT RAISE(ABORT, 'issuer reference admissions are immutable');
END;

-- Exact native identity references share the canonical raw store and its recovery capacity.
-- An unchanged ProviderIdentityRecord retains this edge across later definition revisions.
CREATE TABLE market_data_native_reference_captures (
    identity_digest BLOB PRIMARY KEY CHECK(length(identity_digest)=32 AND identity_digest<>zeroblob(32)),
    origin_revision_digest BLOB NOT NULL REFERENCES market_data_instrument_revisions(revision_digest),
    coordinate_json TEXT NOT NULL CHECK(length(CAST(coordinate_json AS BLOB)) BETWEEN 2 AND 32768 AND json_valid(coordinate_json)),
    raw_claim_digest BLOB NOT NULL,
    physical_receipt_digest BLOB NOT NULL,
    custody_digest BLOB NOT NULL CHECK(length(custody_digest)=32 AND custody_digest<>zeroblob(32)),
    retained_at_ns INTEGER NOT NULL CHECK(retained_at_ns>0),
    FOREIGN KEY(raw_claim_digest, physical_receipt_digest)
        REFERENCES sealed_raw_objects(raw_claim_digest, physical_receipt_digest)
) STRICT, WITHOUT ROWID;
CREATE INDEX market_data_native_reference_recovery
ON market_data_native_reference_captures(raw_claim_digest, physical_receipt_digest);
CREATE TRIGGER market_data_native_reference_guarded_insert
BEFORE INSERT ON market_data_native_reference_captures
WHEN NOT EXISTS (SELECT 1 FROM market_data_instrument_revisions AS revision
        WHERE revision.revision_digest=NEW.origin_revision_digest AND revision.published_at_ns<=NEW.retained_at_ns)
    OR NOT EXISTS (SELECT 1 FROM sealed_raw_objects AS raw
        WHERE raw.raw_claim_digest=NEW.raw_claim_digest AND raw.physical_receipt_digest=NEW.physical_receipt_digest
          AND raw.recorded_at_ns<=NEW.retained_at_ns)
BEGIN SELECT RAISE(ABORT,'native reference custody requires exact original evidence'); END;
CREATE TRIGGER market_data_native_reference_immutable_update
BEFORE UPDATE ON market_data_native_reference_captures BEGIN
    SELECT RAISE(ABORT,'native reference custody is immutable');
END;
CREATE TRIGGER market_data_native_reference_immutable_delete
BEFORE DELETE ON market_data_native_reference_captures BEGIN
    SELECT RAISE(ABORT,'native reference custody is retained for recovery');
END;

-- Install the success guard after both physical and active publication schemas exist.
CREATE TRIGGER ingest_runs_publication_guarded_success
BEFORE UPDATE ON ingest_runs
WHEN NEW.state = 'succeeded'
 AND NEW.operation IN ('persist', 'cache')
 AND NOT EXISTS (
    SELECT 1
    FROM dataset_manifests AS manifest
    JOIN artifacts AS anchor
      ON anchor.artifact_id = manifest.artifact_id
     AND anchor.run_id = manifest.run_id
    WHERE manifest.run_id = NEW.run_id
      AND (SELECT COUNT(*) FROM artifacts AS member
           WHERE member.run_id = NEW.run_id) BETWEEN 1 AND 1024
      AND anchor.publication_ordinal = (
          SELECT COUNT(*) - 1 FROM artifacts AS member
          WHERE member.run_id = NEW.run_id
      )
      AND (SELECT MIN(member.publication_ordinal) FROM artifacts AS member
           WHERE member.run_id = NEW.run_id) = 0
      AND (SELECT MAX(member.publication_ordinal) FROM artifacts AS member
           WHERE member.run_id = NEW.run_id) = (
          SELECT COUNT(*) - 1 FROM artifacts AS member
          WHERE member.run_id = NEW.run_id
      )
)
AND NOT EXISTS (
    SELECT 1 FROM market_event_complete_commits AS committed
    WHERE committed.run_id=NEW.run_id AND committed.available_at_ns=NEW.completed_at_ns
)
BEGIN
    SELECT RAISE(ABORT, 'successful ingest run lacks a closed publication');
END;
