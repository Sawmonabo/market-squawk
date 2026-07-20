CREATE TABLE analytical_artifact_root_authority_events (
    sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
    format_version INTEGER NOT NULL CHECK (format_version = 2),
    event_kind TEXT NOT NULL CHECK (event_kind IN ('legacy_required', 'prepared', 'bound')),
    previous_event_digest BLOB CHECK (
        previous_event_digest IS NULL OR length(previous_event_digest) = 32
    ),
    event_digest BLOB NOT NULL UNIQUE CHECK (length(event_digest) = 32),
    transition_id BLOB CHECK (transition_id IS NULL OR length(transition_id) = 16),
    transition_kind TEXT CHECK (
        transition_kind IS NULL
        OR transition_kind IN ('initialize', 'legacy_migration', 'backup_restore')
    ),
    authority_generation INTEGER CHECK (
        authority_generation IS NULL OR authority_generation > 0
    ),
    target_catalog_identity BLOB NOT NULL CHECK (length(target_catalog_identity) = 32),
    target_root_endpoint_identity BLOB CHECK (
        target_root_endpoint_identity IS NULL OR length(target_root_endpoint_identity) = 32
    ),
    root_instance_id BLOB CHECK (root_instance_id IS NULL OR length(root_instance_id) = 32),
    evidence_digest BLOB NOT NULL CHECK (length(evidence_digest) = 32),
    restore_source_catalog_identity BLOB CHECK (
        restore_source_catalog_identity IS NULL OR length(restore_source_catalog_identity) = 32
    ),
    restore_source_root_identity BLOB CHECK (
        restore_source_root_identity IS NULL OR length(restore_source_root_identity) = 32
    ),
    restore_source_authority_generation INTEGER CHECK (
        restore_source_authority_generation IS NULL OR restore_source_authority_generation > 0
    ),
    restore_source_bound_event BLOB CHECK (
        restore_source_bound_event IS NULL OR length(restore_source_bound_event) = 32
    ),
    restore_source_authority_evidence BLOB CHECK (
        restore_source_authority_evidence IS NULL
        OR length(restore_source_authority_evidence) = 32
    ),
    restore_source_catalog_content_evidence BLOB CHECK (
        restore_source_catalog_content_evidence IS NULL
        OR length(restore_source_catalog_content_evidence) = 32
    ),
    restore_artifact_inventory BLOB CHECK (
        restore_artifact_inventory IS NULL OR length(restore_artifact_inventory) = 32
    ),
    restore_backup_version INTEGER,
    restore_backup_byte_length INTEGER,
    restore_backup_sha256 BLOB CHECK (
        restore_backup_sha256 IS NULL OR length(restore_backup_sha256) = 32
    ),
    restore_snapshot_at_ns INTEGER,
    root_binding_generation INTEGER CHECK (
        root_binding_generation IS NULL OR root_binding_generation > 0
    ),
    root_marker_record_digest BLOB CHECK (
        root_marker_record_digest IS NULL OR length(root_marker_record_digest) = 32
    ),
    stable_root_identity BLOB CHECK (
        stable_root_identity IS NULL OR length(stable_root_identity) = 32
    ),
    root_binding_record_digest BLOB CHECK (
        root_binding_record_digest IS NULL OR length(root_binding_record_digest) = 32
    ),
    CHECK (
        (
            event_kind = 'legacy_required'
            AND transition_id IS NULL
            AND transition_kind IS NULL
            AND authority_generation IS NULL
            AND target_root_endpoint_identity IS NULL
            AND root_instance_id IS NULL
            AND restore_source_catalog_identity IS NULL
            AND restore_source_root_identity IS NULL
            AND restore_source_authority_generation IS NULL
            AND restore_source_bound_event IS NULL
            AND restore_source_authority_evidence IS NULL
            AND restore_source_catalog_content_evidence IS NULL
            AND restore_artifact_inventory IS NULL
            AND restore_backup_version IS NULL
            AND restore_backup_byte_length IS NULL
            AND restore_backup_sha256 IS NULL
            AND restore_snapshot_at_ns IS NULL
            AND root_binding_generation IS NULL
            AND root_marker_record_digest IS NULL
            AND stable_root_identity IS NULL
            AND root_binding_record_digest IS NULL
        )
        OR (
            event_kind IN ('prepared', 'bound')
            AND transition_id IS NOT NULL
            AND transition_kind IS NOT NULL
            AND authority_generation IS NOT NULL
            AND target_root_endpoint_identity IS NOT NULL
            AND root_instance_id IS NOT NULL
            AND root_binding_generation IS NOT NULL
            AND root_binding_generation = authority_generation
            AND (
                (
                    transition_kind = 'backup_restore'
                    AND restore_source_catalog_identity IS NOT NULL
                    AND restore_source_root_identity IS NOT NULL
                    AND restore_source_authority_generation IS NOT NULL
                    AND restore_source_bound_event IS NOT NULL
                    AND restore_source_authority_evidence IS NOT NULL
                    AND restore_source_catalog_content_evidence IS NOT NULL
                    AND restore_artifact_inventory IS NOT NULL
                    AND restore_backup_version IS NOT NULL
                    AND restore_backup_byte_length IS NOT NULL
                    AND restore_backup_sha256 IS NOT NULL
                    AND restore_snapshot_at_ns IS NOT NULL
                )
                OR (
                    transition_kind != 'backup_restore'
                    AND restore_source_catalog_identity IS NULL
                    AND restore_source_root_identity IS NULL
                    AND restore_source_authority_generation IS NULL
                    AND restore_source_bound_event IS NULL
                    AND restore_source_authority_evidence IS NULL
                    AND restore_source_catalog_content_evidence IS NULL
                    AND restore_artifact_inventory IS NULL
                    AND restore_backup_version IS NULL
                    AND restore_backup_byte_length IS NULL
                    AND restore_backup_sha256 IS NULL
                    AND restore_snapshot_at_ns IS NULL
                )
            )
            AND (
                (
                    event_kind = 'prepared'
                    AND root_marker_record_digest IS NULL
                    AND stable_root_identity IS NULL
                    AND root_binding_record_digest IS NULL
                )
                OR (
                    event_kind = 'bound'
                    AND root_marker_record_digest IS NOT NULL
                    AND stable_root_identity IS NOT NULL
                    AND root_binding_record_digest IS NOT NULL
                )
            )
        )
    )
) STRICT;

CREATE TRIGGER analytical_artifact_root_authority_events_immutable_update
BEFORE UPDATE ON analytical_artifact_root_authority_events BEGIN
    SELECT RAISE(ABORT, 'analytical artifact-root authority events are append-only');
END;

CREATE TRIGGER analytical_artifact_root_authority_events_immutable_delete
BEFORE DELETE ON analytical_artifact_root_authority_events BEGIN
    SELECT RAISE(ABORT, 'analytical artifact-root authority events are append-only');
END;

CREATE TRIGGER analytical_artifact_root_authority_events_append_guard
BEFORE INSERT ON analytical_artifact_root_authority_events BEGIN
    SELECT CASE WHEN NEW.sequence != COALESCE(
        (SELECT MAX(sequence) + 1 FROM analytical_artifact_root_authority_events), 1
    ) THEN RAISE(ABORT, 'authority event sequence is not contiguous') END;
    SELECT CASE WHEN NEW.sequence = 1 AND NEW.previous_event_digest IS NOT NULL
        THEN RAISE(ABORT, 'first authority event has a predecessor') END;
    SELECT CASE WHEN NEW.sequence > 1 AND NEW.previous_event_digest IS NOT (
        SELECT event_digest FROM analytical_artifact_root_authority_events
        WHERE sequence = NEW.sequence - 1
    ) THEN RAISE(ABORT, 'authority event predecessor does not match') END;
    SELECT CASE WHEN NEW.sequence = 1 AND NEW.event_kind NOT IN ('legacy_required', 'prepared')
        THEN RAISE(ABORT, 'first authority event is invalid') END;
    SELECT CASE WHEN NEW.event_kind = 'legacy_required' AND NEW.sequence != 1
        THEN RAISE(ABORT, 'legacy authority requirement is not the first event') END;
    SELECT CASE WHEN NEW.sequence = 1 AND NEW.event_kind = 'prepared' AND NOT (
        NEW.transition_kind = 'initialize' AND NEW.authority_generation = 1
    ) THEN RAISE(ABORT, 'first prepared authority event is not initialization') END;
    SELECT CASE WHEN NEW.sequence > 1 AND NOT (
        (
            (SELECT event_kind FROM analytical_artifact_root_authority_events
             WHERE sequence = NEW.sequence - 1) = 'legacy_required'
            AND NEW.event_kind = 'prepared'
        )
        OR (
            (SELECT event_kind FROM analytical_artifact_root_authority_events
             WHERE sequence = NEW.sequence - 1) = 'prepared'
            AND NEW.event_kind = 'bound'
        )
        OR (
            (SELECT event_kind FROM analytical_artifact_root_authority_events
             WHERE sequence = NEW.sequence - 1) = 'bound'
            AND NEW.event_kind = 'prepared'
            AND NEW.transition_kind = 'backup_restore'
            AND NEW.authority_generation = (
                SELECT authority_generation + 1
                FROM analytical_artifact_root_authority_events
                WHERE sequence = NEW.sequence - 1
            )
        )
    ) THEN RAISE(ABORT, 'authority event transition is invalid') END;
    SELECT CASE WHEN NEW.sequence > 1 AND (
        SELECT event_kind FROM analytical_artifact_root_authority_events
        WHERE sequence = NEW.sequence - 1
    ) = 'legacy_required' AND NOT (
        NEW.event_kind = 'prepared'
        AND NEW.transition_kind = 'legacy_migration'
        AND NEW.authority_generation = 1
        AND NEW.target_catalog_identity IS (
            SELECT target_catalog_identity FROM analytical_artifact_root_authority_events
            WHERE sequence = NEW.sequence - 1
        )
    ) THEN RAISE(ABORT, 'legacy authority preparation is invalid') END;
    SELECT CASE WHEN NEW.sequence > 1 AND (
        SELECT event_kind FROM analytical_artifact_root_authority_events
        WHERE sequence = NEW.sequence - 1
    ) = 'bound' AND NOT (
        NEW.restore_source_catalog_identity IS (
            SELECT target_catalog_identity FROM analytical_artifact_root_authority_events
            WHERE sequence = NEW.sequence - 1
        )
        AND NEW.restore_source_root_identity IS (
            SELECT stable_root_identity FROM analytical_artifact_root_authority_events
            WHERE sequence = NEW.sequence - 1
        )
        AND NEW.restore_source_authority_generation IS (
            SELECT authority_generation FROM analytical_artifact_root_authority_events
            WHERE sequence = NEW.sequence - 1
        )
        AND NEW.restore_source_bound_event IS (
            SELECT event_digest FROM analytical_artifact_root_authority_events
            WHERE sequence = NEW.sequence - 1
        )
        AND NEW.restore_source_authority_evidence IS (
            SELECT evidence_digest FROM analytical_artifact_root_authority_events
            WHERE sequence = NEW.sequence - 1
        )
    ) THEN RAISE(ABORT, 'restore source authority linkage is invalid') END;
    SELECT CASE WHEN NEW.event_kind = 'bound' AND EXISTS (
        SELECT 1 FROM analytical_artifact_root_authority_events AS prepared
        WHERE prepared.sequence = NEW.sequence - 1
          AND (
              prepared.event_kind != 'prepared'
              OR prepared.transition_id IS NOT NEW.transition_id
              OR prepared.transition_kind IS NOT NEW.transition_kind
              OR prepared.authority_generation IS NOT NEW.authority_generation
              OR prepared.target_catalog_identity IS NOT NEW.target_catalog_identity
              OR prepared.target_root_endpoint_identity IS NOT NEW.target_root_endpoint_identity
              OR prepared.root_instance_id IS NOT NEW.root_instance_id
              OR prepared.evidence_digest IS NOT NEW.evidence_digest
              OR prepared.restore_source_catalog_identity IS NOT NEW.restore_source_catalog_identity
              OR prepared.restore_source_root_identity IS NOT NEW.restore_source_root_identity
              OR prepared.restore_source_authority_generation IS NOT NEW.restore_source_authority_generation
              OR prepared.restore_source_bound_event IS NOT NEW.restore_source_bound_event
              OR prepared.restore_source_authority_evidence IS NOT NEW.restore_source_authority_evidence
              OR prepared.restore_source_catalog_content_evidence IS NOT NEW.restore_source_catalog_content_evidence
              OR prepared.restore_artifact_inventory IS NOT NEW.restore_artifact_inventory
              OR prepared.restore_backup_version IS NOT NEW.restore_backup_version
              OR prepared.restore_backup_byte_length IS NOT NEW.restore_backup_byte_length
              OR prepared.restore_backup_sha256 IS NOT NEW.restore_backup_sha256
              OR prepared.restore_snapshot_at_ns IS NOT NEW.restore_snapshot_at_ns
              OR prepared.root_binding_generation IS NOT NEW.root_binding_generation
          )
    ) THEN RAISE(ABORT, 'bound authority event differs from prepared intent') END;
END;
