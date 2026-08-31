DROP TRIGGER source_rights_immutable_update;
DROP TRIGGER source_rights_immutable_delete;

ALTER TABLE source_rights RENAME COLUMN terms_url TO basis_reference;
ALTER TABLE source_rights RENAME COLUMN terms_algorithm TO basis_algorithm;
ALTER TABLE source_rights RENAME COLUMN terms_digest TO basis_digest;
ALTER TABLE source_rights ADD COLUMN basis_kind TEXT NOT NULL DEFAULT 'reviewed_terms'
    CHECK (basis_kind IN ('reviewed_terms', 'user_owned_local'));
ALTER TABLE source_rights ADD COLUMN basis_root_algorithm INTEGER
    CHECK (basis_root_algorithm IS NULL OR basis_root_algorithm IN (1, 2));
ALTER TABLE source_rights ADD COLUMN basis_root_digest BLOB
    CHECK (basis_root_digest IS NULL OR length(basis_root_digest) = 32);
ALTER TABLE source_rights ADD COLUMN fingerprint_version INTEGER NOT NULL DEFAULT 1
    CHECK (fingerprint_version IN (1, 2));

CREATE TRIGGER source_rights_basis_guard_insert
BEFORE INSERT ON source_rights
WHEN NOT (
    (
        NEW.fingerprint_version = 2
        AND NEW.basis_kind = 'reviewed_terms'
        AND NEW.basis_root_algorithm IS NULL
        AND NEW.basis_root_digest IS NULL
    ) OR (
        NEW.fingerprint_version = 2
        AND NEW.basis_kind = 'user_owned_local'
        AND NEW.basis_root_algorithm IS NOT NULL
        AND NEW.basis_root_digest IS NOT NULL
        AND NEW.basis_root_digest <> zeroblob(32)
    )
)
BEGIN
    SELECT RAISE(ABORT, 'source-rights basis is invalid');
END;

CREATE TRIGGER source_rights_immutable_update
BEFORE UPDATE ON source_rights BEGIN
    SELECT RAISE(ABORT, 'source rights are immutable');
END;

CREATE TRIGGER source_rights_immutable_delete
BEFORE DELETE ON source_rights BEGIN
    SELECT RAISE(ABORT, 'source rights are immutable');
END;

DROP TRIGGER ingest_runs_rights_admitted_before_request;

CREATE TRIGGER ingest_runs_rights_guarded_insert
BEFORE INSERT ON ingest_runs
WHEN NOT EXISTS (
    SELECT 1 FROM source_rights
    WHERE rights_id = NEW.rights_id
      AND source_id = NEW.source_id
      AND payload_algorithm = NEW.payload_algorithm
      AND payload_digest = NEW.payload_digest
      AND admitted_at_ns <= NEW.requested_at_ns
      AND (
          authorization_expires_at_ns IS NULL
          OR NEW.requested_at_ns < authorization_expires_at_ns
      )
      AND (
          operation_mask & CASE NEW.operation
              WHEN 'retrieve' THEN 1
              WHEN 'display' THEN 2
              WHEN 'persist' THEN 4
              WHEN 'cache' THEN 8
              WHEN 'redistribute' THEN 16
              WHEN 'train' THEN 32
              ELSE 0
          END
      ) <> 0
)
BEGIN
    SELECT RAISE(ABORT, 'ingest run source rights are invalid');
END;

CREATE TABLE source_research_use_grants (
    research_grant_id BLOB PRIMARY KEY CHECK (length(research_grant_id) = 32),
    rights_id BLOB NOT NULL REFERENCES source_rights(rights_id),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    use_mask INTEGER NOT NULL CHECK (use_mask BETWEEN 1 AND 7),
    evidence_algorithm INTEGER NOT NULL CHECK (evidence_algorithm IN (1, 2)),
    evidence_digest BLOB NOT NULL CHECK (length(evidence_digest) = 32),
    authorization_expires_at_ns INTEGER,
    admitted_at_ns INTEGER NOT NULL,
    UNIQUE (rights_id, use_mask, evidence_algorithm, evidence_digest,
            authorization_expires_at_ns)
) STRICT;

CREATE TRIGGER source_research_use_grants_guarded_insert
BEFORE INSERT ON source_research_use_grants
WHEN NOT EXISTS (
    SELECT 1
    FROM source_rights
    WHERE rights_id = NEW.rights_id
      AND source_id = NEW.source_id
      AND admitted_at_ns <= NEW.admitted_at_ns
      AND (
          authorization_expires_at_ns IS NULL
          OR NEW.admitted_at_ns < authorization_expires_at_ns
      )
      AND (
          authorization_expires_at_ns IS NULL
          OR (
              NEW.authorization_expires_at_ns IS NOT NULL
              AND NEW.authorization_expires_at_ns <= authorization_expires_at_ns
          )
      )
      AND (
          NEW.authorization_expires_at_ns IS NULL
          OR NEW.admitted_at_ns < NEW.authorization_expires_at_ns
      )
      AND operation_mask & (
          (CASE WHEN NEW.use_mask & 1 <> 0 THEN 2 ELSE 0 END)
          | (CASE WHEN NEW.use_mask & 2 <> 0 THEN 4 ELSE 0 END)
          | (CASE WHEN NEW.use_mask & 4 <> 0 THEN 32 ELSE 0 END)
      ) = (
          (CASE WHEN NEW.use_mask & 1 <> 0 THEN 2 ELSE 0 END)
          | (CASE WHEN NEW.use_mask & 2 <> 0 THEN 4 ELSE 0 END)
          | (CASE WHEN NEW.use_mask & 4 <> 0 THEN 32 ELSE 0 END)
      )
)
BEGIN
    SELECT RAISE(ABORT, 'research-use grant is invalid');
END;

CREATE TRIGGER source_research_use_grants_immutable_update
BEFORE UPDATE ON source_research_use_grants BEGIN
    SELECT RAISE(ABORT, 'research-use grants are immutable');
END;

CREATE TRIGGER source_research_use_grants_immutable_delete
BEFORE DELETE ON source_research_use_grants BEGIN
    SELECT RAISE(ABORT, 'research-use grants are immutable');
END;

CREATE TABLE source_research_use_revocations (
    revocation_sequence INTEGER PRIMARY KEY CHECK (revocation_sequence > 0),
    revocation_id BLOB NOT NULL UNIQUE CHECK (length(revocation_id) = 32),
    research_grant_id BLOB NOT NULL REFERENCES source_research_use_grants(research_grant_id),
    use_mask INTEGER NOT NULL CHECK (use_mask BETWEEN 1 AND 7),
    reason_code TEXT NOT NULL CHECK (
        reason_code IN (
            'authorization_withdrawn',
            'authorization_superseded',
            'evidence_invalidated',
            'source_disabled'
        )
    ),
    evidence_algorithm INTEGER NOT NULL CHECK (evidence_algorithm IN (1, 2)),
    evidence_digest BLOB NOT NULL CHECK (length(evidence_digest) = 32),
    effective_at_ns INTEGER NOT NULL,
    recorded_at_ns INTEGER NOT NULL CHECK (effective_at_ns <= recorded_at_ns)
) STRICT;

CREATE TRIGGER source_research_use_revocations_guarded_insert
BEFORE INSERT ON source_research_use_revocations
WHEN NEW.revocation_sequence <> COALESCE(
        (SELECT MAX(revocation_sequence) + 1 FROM source_research_use_revocations),
        1
    )
    OR NEW.recorded_at_ns < COALESCE(
        (SELECT MAX(recorded_at_ns) FROM source_research_use_revocations),
        NEW.recorded_at_ns
    )
    OR NOT EXISTS (
        SELECT 1
        FROM source_research_use_grants
        WHERE research_grant_id = NEW.research_grant_id
          AND use_mask & NEW.use_mask = NEW.use_mask
          AND admitted_at_ns <= NEW.effective_at_ns
    )
BEGIN
    SELECT RAISE(ABORT, 'research-use revocation is invalid');
END;

CREATE TRIGGER source_research_use_revocations_immutable_update
BEFORE UPDATE ON source_research_use_revocations BEGIN
    SELECT RAISE(ABORT, 'research-use revocations are immutable');
END;

CREATE TRIGGER source_research_use_revocations_immutable_delete
BEFORE DELETE ON source_research_use_revocations BEGIN
    SELECT RAISE(ABORT, 'research-use revocations are immutable');
END;

CREATE TABLE analytical_generation_source_inputs (
    generation_sequence INTEGER PRIMARY KEY
        REFERENCES analytical_generations(generation_sequence),
    run_id TEXT NOT NULL UNIQUE REFERENCES ingest_runs(run_id),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    rights_id BLOB NOT NULL REFERENCES source_rights(rights_id)
) STRICT;

CREATE TRIGGER analytical_generation_source_inputs_guarded_insert
BEFORE INSERT ON analytical_generation_source_inputs
WHEN NOT EXISTS (
    SELECT 1
    FROM analytical_generations AS generation
    JOIN dataset_manifests AS manifest
      ON manifest.manifest_id = generation.anchor_manifest_id
    JOIN ingest_runs AS run
      ON run.run_id = manifest.run_id
    WHERE generation.generation_sequence = NEW.generation_sequence
      AND generation.generation_kind = 'ingest'
      AND run.run_id = NEW.run_id
      AND run.source_id = NEW.source_id
      AND run.rights_id = NEW.rights_id
      AND run.operation = 'persist'
      AND (SELECT COUNT(*) FROM artifacts AS member
           WHERE member.run_id = run.run_id) BETWEEN 1 AND 1024
      AND manifest.artifact_id = (
          SELECT member.artifact_id
          FROM artifacts AS member
          WHERE member.run_id = run.run_id
          ORDER BY member.publication_ordinal DESC
          LIMIT 1
      )
      AND (
          (
              generation.manifest_version = 1
              AND generation.parent_count = 0
              AND (SELECT COUNT(*)
                   FROM analytical_generation_objects AS object
                   WHERE object.dataset_id = generation.dataset_id
                     AND object.manifest_version = generation.manifest_version) = (
                  SELECT COUNT(*) FROM artifacts AS member
                  WHERE member.run_id = run.run_id
              )
          ) OR (
              generation.manifest_version > 1
              AND generation.parent_count = 1
              AND EXISTS (
                  SELECT 1
                  FROM analytical_generation_parents AS edge
                  JOIN analytical_generations AS parent
                    ON parent.generation_sequence = edge.parent_generation_sequence
                   AND parent.dataset_id = edge.parent_dataset_id
                   AND parent.manifest_version = edge.parent_manifest_version
                  WHERE edge.child_dataset_id = generation.dataset_id
                    AND edge.child_manifest_version = generation.manifest_version
                    AND edge.ordinal = 0
                    AND edge.relation = 'append_predecessor'
                    AND parent.dataset_id = generation.dataset_id
                    AND parent.manifest_version = generation.manifest_version - 1
                    AND (SELECT COUNT(*)
                         FROM analytical_generation_objects AS current_object
                         WHERE current_object.dataset_id = generation.dataset_id
                           AND current_object.manifest_version =
                               generation.manifest_version) = (
                        SELECT COUNT(*)
                        FROM analytical_generation_objects AS parent_object
                        WHERE parent_object.dataset_id = parent.dataset_id
                          AND parent_object.manifest_version = parent.manifest_version
                    ) + (
                        SELECT COUNT(*) FROM artifacts AS member
                        WHERE member.run_id = run.run_id
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM analytical_generation_objects AS parent_object
                        LEFT JOIN analytical_generation_objects AS current_object
                          ON current_object.dataset_id = generation.dataset_id
                         AND current_object.manifest_version = generation.manifest_version
                         AND current_object.ordinal = parent_object.ordinal
                        WHERE parent_object.dataset_id = parent.dataset_id
                          AND parent_object.manifest_version = parent.manifest_version
                          AND (
                              current_object.artifact_id IS NULL
                              OR current_object.artifact_id <> parent_object.artifact_id
                              OR current_object.content_hash <> parent_object.content_hash
                              OR current_object.row_count <> parent_object.row_count
                              OR current_object.size_bytes <> parent_object.size_bytes
                              OR current_object.lineage_hash <> parent_object.lineage_hash
                          )
                    )
              )
          )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM artifacts AS member
          LEFT JOIN analytical_generation_objects AS object
            ON object.dataset_id = generation.dataset_id
           AND object.manifest_version = generation.manifest_version
           AND object.ordinal = (
               SELECT COUNT(*)
               FROM analytical_generation_objects AS retained
               WHERE retained.dataset_id = generation.dataset_id
                 AND retained.manifest_version = generation.manifest_version
           ) - (
               SELECT COUNT(*) FROM artifacts AS current_member
               WHERE current_member.run_id = run.run_id
           ) + member.publication_ordinal
          WHERE member.run_id = run.run_id
            AND (
                object.artifact_id IS NULL
                OR object.artifact_id <> member.artifact_id
                OR member.content_algorithm <> 1
                OR object.content_hash <> member.content_digest
                OR object.size_bytes <> member.size_bytes
            )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'analytical generation source input is invalid');
END;

CREATE TRIGGER analytical_generation_source_inputs_immutable_update
BEFORE UPDATE ON analytical_generation_source_inputs BEGIN
    SELECT RAISE(ABORT, 'analytical generation source inputs are immutable');
END;

CREATE TRIGGER analytical_generation_source_inputs_immutable_delete
BEFORE DELETE ON analytical_generation_source_inputs BEGIN
    SELECT RAISE(ABORT, 'analytical generation source inputs are immutable');
END;

INSERT INTO analytical_generation_source_inputs (
    generation_sequence,
    run_id,
    source_id,
    rights_id
)
SELECT
    generation.generation_sequence,
    run.run_id,
    run.source_id,
    run.rights_id
FROM analytical_generations AS generation
JOIN dataset_manifests AS manifest
  ON manifest.manifest_id = generation.anchor_manifest_id
JOIN artifacts AS artifact
  ON artifact.artifact_id = manifest.artifact_id
JOIN ingest_runs AS run
  ON run.run_id = manifest.run_id
 AND artifact.run_id = run.run_id
WHERE generation.generation_kind = 'ingest'
ORDER BY generation.generation_sequence;

CREATE TABLE research_use_decisions (
    decision_id BLOB PRIMARY KEY CHECK (length(decision_id) = 32),
    policy_version INTEGER NOT NULL CHECK (policy_version = 1),
    requested_use TEXT NOT NULL CHECK (
        requested_use IN ('display', 'local_analysis', 'train')
    ),
    graph_digest BLOB NOT NULL CHECK (length(graph_digest) = 32),
    requested_at_ns INTEGER NOT NULL,
    decided_at_ns INTEGER NOT NULL CHECK (requested_at_ns <= decided_at_ns),
    expires_at_ns INTEGER,
    outcome TEXT NOT NULL CHECK (outcome IN ('allowed', 'denied')),
    reason_code TEXT CHECK (
        reason_code IS NULL OR reason_code IN (
            'missing_grant',
            'expired',
            'revoked',
            'corrupt_authority',
            'limit_exceeded',
            'cancelled',
            'deadline_exceeded'
        )
    ),
    max_root_count INTEGER NOT NULL CHECK (max_root_count BETWEEN 1 AND 256),
    max_node_count INTEGER NOT NULL CHECK (max_node_count BETWEEN 1 AND 100000),
    max_edge_count INTEGER NOT NULL CHECK (max_edge_count BETWEEN 1 AND 400000),
    max_source_count INTEGER NOT NULL CHECK (max_source_count BETWEEN 1 AND 100000),
    max_retained_bytes INTEGER NOT NULL CHECK (
        max_retained_bytes BETWEEN 1 AND 67108864
    ),
    traversal_deadline_ns INTEGER NOT NULL CHECK (
        traversal_deadline_ns BETWEEN 1 AND 30000000000
    ),
    permit_lifetime_ns INTEGER NOT NULL CHECK (
        permit_lifetime_ns BETWEEN 1 AND 300000000000
    ),
    root_count INTEGER NOT NULL CHECK (root_count BETWEEN 1 AND 256),
    node_count INTEGER NOT NULL CHECK (node_count BETWEEN 1 AND 100000),
    edge_count INTEGER NOT NULL CHECK (edge_count BETWEEN 0 AND 400000),
    source_count INTEGER NOT NULL CHECK (source_count BETWEEN 1 AND 100000),
    retained_bytes INTEGER NOT NULL CHECK (retained_bytes BETWEEN 1 AND 67108864),
    CHECK (root_count <= max_root_count),
    CHECK (node_count <= max_node_count),
    CHECK (edge_count <= max_edge_count),
    CHECK (source_count <= max_source_count),
    CHECK (retained_bytes <= max_retained_bytes),
    CHECK (
        (
            outcome = 'allowed'
            AND reason_code IS NULL
            AND expires_at_ns > decided_at_ns
            AND expires_at_ns - decided_at_ns <= permit_lifetime_ns
        )
        OR (outcome = 'denied' AND reason_code IS NOT NULL AND expires_at_ns IS NULL)
    )
) STRICT;

CREATE TABLE research_use_decision_nodes (
    decision_id BLOB NOT NULL
        REFERENCES research_use_decisions(decision_id) DEFERRABLE INITIALLY DEFERRED,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 99999),
    generation_sequence INTEGER NOT NULL REFERENCES analytical_generations(generation_sequence),
    PRIMARY KEY (decision_id, ordinal),
    UNIQUE (decision_id, generation_sequence)
) STRICT, WITHOUT ROWID;

CREATE TABLE research_use_decision_roots (
    decision_id BLOB NOT NULL
        REFERENCES research_use_decisions(decision_id) DEFERRABLE INITIALLY DEFERRED,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 255),
    generation_sequence INTEGER NOT NULL REFERENCES analytical_generations(generation_sequence),
    dataset_id TEXT NOT NULL,
    manifest_version INTEGER NOT NULL CHECK (manifest_version > 0),
    schema_name TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    schema_fingerprint BLOB NOT NULL CHECK (length(schema_fingerprint) = 32),
    content_hash BLOB NOT NULL CHECK (length(content_hash) = 32),
    PRIMARY KEY (decision_id, ordinal),
    UNIQUE (decision_id, generation_sequence),
    FOREIGN KEY (generation_sequence) REFERENCES analytical_generations(generation_sequence)
) STRICT, WITHOUT ROWID;

CREATE TABLE research_use_decision_sources (
    decision_id BLOB NOT NULL
        REFERENCES research_use_decisions(decision_id) DEFERRABLE INITIALLY DEFERRED,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 99999),
    source_generation_sequence INTEGER NOT NULL
        REFERENCES analytical_generation_source_inputs(generation_sequence),
    run_id TEXT NOT NULL REFERENCES ingest_runs(run_id),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    rights_id BLOB NOT NULL REFERENCES source_rights(rights_id),
    requested_use TEXT NOT NULL CHECK (
        requested_use IN ('display', 'local_analysis', 'train')
    ),
    selection_outcome TEXT NOT NULL CHECK (
        selection_outcome IN ('selected', 'missing', 'not_evaluated')
    ),
    selected_research_grant_id BLOB
        REFERENCES source_research_use_grants(research_grant_id),
    selected_expires_at_ns INTEGER,
    observed_revocation_sequence INTEGER NOT NULL CHECK (observed_revocation_sequence >= 0),
    PRIMARY KEY (decision_id, ordinal),
    UNIQUE (decision_id, source_generation_sequence, rights_id),
    CHECK (
        (selection_outcome = 'selected' AND selected_research_grant_id IS NOT NULL)
        OR (
            selection_outcome IN ('missing', 'not_evaluated')
            AND selected_research_grant_id IS NULL
            AND selected_expires_at_ns IS NULL
        )
    ),
    FOREIGN KEY (source_generation_sequence)
        REFERENCES analytical_generation_source_inputs(generation_sequence)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER research_use_decision_nodes_guarded_insert
BEFORE INSERT ON research_use_decision_nodes
WHEN NOT EXISTS (
        SELECT 1 FROM analytical_generations
        WHERE generation_sequence = NEW.generation_sequence
    ) OR (
        NEW.ordinal > 0 AND NOT EXISTS (
            SELECT 1 FROM research_use_decision_nodes AS prior
            WHERE prior.decision_id = NEW.decision_id
              AND prior.ordinal = NEW.ordinal - 1
              AND prior.generation_sequence < NEW.generation_sequence
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'research-use decision node is invalid');
END;

CREATE TRIGGER research_use_decision_roots_guarded_insert
BEFORE INSERT ON research_use_decision_roots
WHEN NOT EXISTS (
        SELECT 1 FROM analytical_generations
        WHERE generation_sequence = NEW.generation_sequence
          AND dataset_id = NEW.dataset_id
          AND manifest_version = NEW.manifest_version
          AND schema_name = NEW.schema_name
          AND schema_version = NEW.schema_version
          AND schema_fingerprint = NEW.schema_fingerprint
          AND content_hash = NEW.content_hash
    ) OR NOT EXISTS (
        SELECT 1 FROM research_use_decision_nodes
        WHERE decision_id = NEW.decision_id
          AND generation_sequence = NEW.generation_sequence
    ) OR (
        NEW.ordinal > 0 AND NOT EXISTS (
            SELECT 1 FROM research_use_decision_roots AS prior
            WHERE prior.decision_id = NEW.decision_id
              AND prior.ordinal = NEW.ordinal - 1
              AND (
                  prior.dataset_id,
                  prior.manifest_version,
                  prior.schema_name,
                  prior.schema_version,
                  prior.schema_fingerprint,
                  prior.content_hash
              ) < (
                  NEW.dataset_id,
                  NEW.manifest_version,
                  NEW.schema_name,
                  NEW.schema_version,
                  NEW.schema_fingerprint,
                  NEW.content_hash
              )
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'research-use decision root is invalid');
END;

CREATE TRIGGER research_use_decision_sources_guarded_insert
BEFORE INSERT ON research_use_decision_sources
WHEN NOT EXISTS (
        SELECT 1 FROM analytical_generation_source_inputs
        WHERE generation_sequence = NEW.source_generation_sequence
          AND run_id = NEW.run_id
          AND source_id = NEW.source_id
          AND rights_id = NEW.rights_id
    ) OR NOT EXISTS (
        SELECT 1
        FROM research_use_decision_nodes AS node
        JOIN analytical_generations AS generation USING (generation_sequence)
        WHERE node.decision_id = NEW.decision_id
          AND node.generation_sequence = NEW.source_generation_sequence
          AND generation.generation_kind = 'ingest'
    ) OR (
        NEW.selection_outcome = 'selected' AND NOT EXISTS (
            SELECT 1
            FROM source_research_use_grants AS grant
            WHERE grant.research_grant_id = NEW.selected_research_grant_id
              AND grant.rights_id = NEW.rights_id
              AND grant.source_id = NEW.source_id
              AND (grant.use_mask & CASE NEW.requested_use
                      WHEN 'display' THEN 1
                      WHEN 'local_analysis' THEN 2
                      WHEN 'train' THEN 4
                  END) <> 0
              AND grant.authorization_expires_at_ns IS NEW.selected_expires_at_ns
        )
    ) OR (
        NEW.ordinal > 0 AND NOT EXISTS (
            SELECT 1 FROM research_use_decision_sources AS prior
            WHERE prior.decision_id = NEW.decision_id
              AND prior.ordinal = NEW.ordinal - 1
              AND (
                  prior.source_generation_sequence,
                  prior.run_id,
                  prior.source_id,
                  prior.rights_id
              ) < (
                  NEW.source_generation_sequence,
                  NEW.run_id,
                  NEW.source_id,
                  NEW.rights_id
              )
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'research-use decision source selection is invalid');
END;

CREATE TRIGGER research_use_decision_nodes_immutable_update
BEFORE UPDATE ON research_use_decision_nodes BEGIN
    SELECT RAISE(ABORT, 'research-use decision nodes are immutable');
END;

CREATE TRIGGER research_use_decision_nodes_immutable_delete
BEFORE DELETE ON research_use_decision_nodes BEGIN
    SELECT RAISE(ABORT, 'research-use decision nodes are immutable');
END;

CREATE TRIGGER research_use_decision_roots_immutable_update
BEFORE UPDATE ON research_use_decision_roots BEGIN
    SELECT RAISE(ABORT, 'research-use decision roots are immutable');
END;

CREATE TRIGGER research_use_decision_roots_immutable_delete
BEFORE DELETE ON research_use_decision_roots BEGIN
    SELECT RAISE(ABORT, 'research-use decision roots are immutable');
END;

CREATE TRIGGER research_use_decision_sources_immutable_update
BEFORE UPDATE ON research_use_decision_sources BEGIN
    SELECT RAISE(ABORT, 'research-use decision sources are immutable');
END;

CREATE TRIGGER research_use_decision_sources_immutable_delete
BEFORE DELETE ON research_use_decision_sources BEGIN
    SELECT RAISE(ABORT, 'research-use decision sources are immutable');
END;

CREATE TRIGGER research_use_decisions_guarded_insert
BEFORE INSERT ON research_use_decisions
WHEN NOT (
    (SELECT COUNT(*) FROM research_use_decision_nodes
     WHERE decision_id = NEW.decision_id) = NEW.node_count
    AND (SELECT COALESCE(MIN(ordinal), 0) FROM research_use_decision_nodes
         WHERE decision_id = NEW.decision_id) = 0
    AND (SELECT COALESCE(MAX(ordinal), -1) FROM research_use_decision_nodes
         WHERE decision_id = NEW.decision_id) = NEW.node_count - 1
    AND (SELECT COUNT(*) FROM research_use_decision_roots
     WHERE decision_id = NEW.decision_id) = NEW.root_count
    AND (SELECT COALESCE(MIN(ordinal), 0) FROM research_use_decision_roots
         WHERE decision_id = NEW.decision_id) = 0
    AND (SELECT COALESCE(MAX(ordinal), -1) FROM research_use_decision_roots
         WHERE decision_id = NEW.decision_id) = NEW.root_count - 1
    AND (SELECT COUNT(*) FROM research_use_decision_sources
         WHERE decision_id = NEW.decision_id) = NEW.source_count
    AND (SELECT COALESCE(MIN(ordinal), 0) FROM research_use_decision_sources
         WHERE decision_id = NEW.decision_id) = 0
    AND (SELECT COALESCE(MAX(ordinal), -1) FROM research_use_decision_sources
         WHERE decision_id = NEW.decision_id) = NEW.source_count - 1
    AND NOT EXISTS (
        SELECT 1 FROM research_use_decision_sources
        WHERE decision_id = NEW.decision_id
          AND requested_use <> NEW.requested_use
    )
    AND NOT EXISTS (
        SELECT 1
        FROM research_use_decision_nodes AS node
        JOIN analytical_generation_parents AS parent
          ON parent.child_dataset_id = (
                SELECT dataset_id FROM analytical_generations
                WHERE generation_sequence = node.generation_sequence
             )
         AND parent.child_manifest_version = (
                SELECT manifest_version FROM analytical_generations
                WHERE generation_sequence = node.generation_sequence
             )
        WHERE node.decision_id = NEW.decision_id
          AND NOT EXISTS (
              SELECT 1 FROM research_use_decision_nodes AS retained_parent
              WHERE retained_parent.decision_id = NEW.decision_id
                AND retained_parent.generation_sequence = parent.parent_generation_sequence
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM research_use_decision_nodes AS node
        WHERE node.decision_id = NEW.decision_id
          AND NOT EXISTS (
              SELECT 1 FROM research_use_decision_roots AS root
              WHERE root.decision_id = NEW.decision_id
                AND root.generation_sequence = node.generation_sequence
          )
          AND NOT EXISTS (
              SELECT 1
              FROM analytical_generation_parents AS parent
              JOIN analytical_generations AS child
                ON child.dataset_id = parent.child_dataset_id
               AND child.manifest_version = parent.child_manifest_version
              JOIN research_use_decision_nodes AS retained_child
                ON retained_child.decision_id = NEW.decision_id
               AND retained_child.generation_sequence = child.generation_sequence
              WHERE parent.parent_generation_sequence = node.generation_sequence
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM research_use_decision_nodes AS node
        JOIN analytical_generations AS generation USING (generation_sequence)
        WHERE node.decision_id = NEW.decision_id
          AND generation.parent_count <> (
              SELECT COUNT(*) FROM analytical_generation_parents AS parent
              WHERE parent.child_dataset_id = generation.dataset_id
                AND parent.child_manifest_version = generation.manifest_version
          )
    )
    AND (
        SELECT COALESCE(SUM(generation.parent_count), 0)
        FROM research_use_decision_nodes AS node
        JOIN analytical_generations AS generation USING (generation_sequence)
        WHERE node.decision_id = NEW.decision_id
    ) = NEW.edge_count
    AND NOT EXISTS (
        SELECT 1
        FROM research_use_decision_nodes AS node
        JOIN analytical_generations AS generation USING (generation_sequence)
        WHERE node.decision_id = NEW.decision_id
          AND generation.generation_kind = 'ingest'
          AND NOT EXISTS (
              SELECT 1 FROM research_use_decision_sources AS source
              WHERE source.decision_id = NEW.decision_id
                AND source.source_generation_sequence = node.generation_sequence
          )
    )
    AND NOT EXISTS (
        SELECT 1 FROM research_use_decision_sources AS source
        WHERE source.decision_id = NEW.decision_id
          AND NOT EXISTS (
              SELECT 1
              FROM research_use_decision_nodes AS node
              JOIN analytical_generations AS generation USING (generation_sequence)
              WHERE node.decision_id = NEW.decision_id
                AND node.generation_sequence = source.source_generation_sequence
                AND generation.generation_kind = 'ingest'
          )
    )
    AND NOT EXISTS (
        SELECT 1 FROM research_use_decision_sources
        WHERE decision_id = NEW.decision_id
          AND observed_revocation_sequence <> COALESCE(
              (
                  SELECT MAX(revocation_sequence)
                  FROM source_research_use_revocations
                  WHERE recorded_at_ns <= NEW.decided_at_ns
              ),
              0
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM research_use_decision_sources AS source
        JOIN source_research_use_grants AS grant
          ON grant.research_grant_id = source.selected_research_grant_id
        JOIN source_rights AS rights ON rights.rights_id = source.rights_id
        WHERE source.decision_id = NEW.decision_id
          AND source.selection_outcome = 'selected'
          AND (
              grant.admitted_at_ns > NEW.decided_at_ns
              OR rights.admitted_at_ns > NEW.decided_at_ns
          )
    )
    AND (
        (
            NEW.outcome = 'allowed'
            AND NOT EXISTS (
                SELECT 1 FROM research_use_decision_sources
                WHERE decision_id = NEW.decision_id
                  AND selection_outcome <> 'selected'
            )
            AND NOT EXISTS (
                SELECT 1
                FROM research_use_decision_sources AS source
                JOIN source_research_use_grants AS grant
                  ON grant.research_grant_id = source.selected_research_grant_id
                JOIN source_rights AS rights ON rights.rights_id = source.rights_id
                WHERE source.decision_id = NEW.decision_id
                  AND (
                      grant.admitted_at_ns > NEW.decided_at_ns
                      OR rights.admitted_at_ns > NEW.decided_at_ns
                      OR (
                          grant.authorization_expires_at_ns IS NOT NULL
                          AND NEW.expires_at_ns > grant.authorization_expires_at_ns
                      )
                      OR (
                          rights.authorization_expires_at_ns IS NOT NULL
                          AND NEW.expires_at_ns > rights.authorization_expires_at_ns
                      )
                      OR EXISTS (
                          SELECT 1 FROM source_research_use_revocations AS revocation
                          WHERE revocation.research_grant_id = grant.research_grant_id
                            AND revocation.revocation_sequence
                                <= source.observed_revocation_sequence
                            AND revocation.effective_at_ns <= NEW.decided_at_ns
                            AND revocation.recorded_at_ns <= NEW.decided_at_ns
                            AND (
                                revocation.use_mask & CASE NEW.requested_use
                                    WHEN 'display' THEN 1
                                    WHEN 'local_analysis' THEN 2
                                    WHEN 'train' THEN 4
                                END
                            ) <> 0
                      )
                  )
            )
        ) OR (
            NEW.outcome = 'denied'
            AND (
                NEW.reason_code <> 'missing_grant'
                OR EXISTS (
                    SELECT 1 FROM research_use_decision_sources
                    WHERE decision_id = NEW.decision_id
                      AND selection_outcome = 'missing'
                )
            )
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'research-use decision is invalid');
END;

CREATE TRIGGER research_use_decisions_immutable_update
BEFORE UPDATE ON research_use_decisions BEGIN
    SELECT RAISE(ABORT, 'research-use decisions are immutable');
END;

CREATE TRIGGER research_use_decisions_immutable_delete
BEFORE DELETE ON research_use_decisions BEGIN
    SELECT RAISE(ABORT, 'research-use decisions are immutable');
END;

CREATE TABLE derived_output_groups (
    output_group_id BLOB PRIMARY KEY CHECK (length(output_group_id) = 32),
    decision_id BLOB NOT NULL UNIQUE REFERENCES research_use_decisions(decision_id),
    parent_graph_digest BLOB NOT NULL CHECK (length(parent_graph_digest) = 32),
    dataset_id TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    schema_fingerprint BLOB NOT NULL CHECK (length(schema_fingerprint) = 32),
    build_spec_digest BLOB NOT NULL CHECK (length(build_spec_digest) = 32),
    plan_content_hash BLOB NOT NULL CHECK (length(plan_content_hash) = 32),
    plan_lineage_hash BLOB NOT NULL CHECK (length(plan_lineage_hash) = 32),
    row_count INTEGER NOT NULL CHECK (row_count > 0),
    object_count INTEGER NOT NULL CHECK (object_count BETWEEN 1 AND 1024),
    total_bytes INTEGER NOT NULL CHECK (total_bytes > 0),
    retention_operation TEXT NOT NULL CHECK (retention_operation IN ('persist', 'cache')),
    anchor_artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
    anchor_manifest_id TEXT NOT NULL REFERENCES dataset_manifests(manifest_id),
    committed_at_ns INTEGER NOT NULL
) STRICT;

CREATE TABLE derived_output_group_members (
    output_group_id BLOB NOT NULL
        REFERENCES derived_output_groups(output_group_id) DEFERRABLE INITIALLY DEFERRED,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 1023),
    run_id TEXT NOT NULL UNIQUE REFERENCES ingest_runs(run_id),
    artifact_id TEXT NOT NULL UNIQUE REFERENCES artifacts(artifact_id),
    retention_operation TEXT NOT NULL CHECK (retention_operation IN ('persist', 'cache')),
    rights_id BLOB NOT NULL REFERENCES source_rights(rights_id),
    PRIMARY KEY (output_group_id, ordinal)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER derived_output_group_members_guarded_insert
BEFORE INSERT ON derived_output_group_members
WHEN NOT EXISTS (
    SELECT 1
    FROM ingest_runs AS run
    JOIN artifacts AS artifact ON artifact.run_id = run.run_id
    WHERE run.run_id = NEW.run_id
      AND artifact.artifact_id = NEW.artifact_id
      AND run.operation = NEW.retention_operation
      AND run.rights_id = NEW.rights_id
      AND run.state IN ('reserved', 'succeeded')
)
BEGIN
    SELECT RAISE(ABORT, 'derived output member is invalid');
END;

CREATE TRIGGER derived_output_groups_guarded_insert
BEFORE INSERT ON derived_output_groups
WHEN NOT (
    (SELECT COUNT(*) FROM derived_output_group_members
     WHERE output_group_id = NEW.output_group_id) = NEW.object_count
    AND (SELECT COALESCE(MIN(ordinal), 0) FROM derived_output_group_members
         WHERE output_group_id = NEW.output_group_id) = 0
    AND (SELECT COALESCE(MAX(ordinal), -1) FROM derived_output_group_members
         WHERE output_group_id = NEW.output_group_id) = NEW.object_count - 1
    AND NOT EXISTS (
        SELECT 1 FROM derived_output_group_members
        WHERE output_group_id = NEW.output_group_id
          AND retention_operation <> NEW.retention_operation
    )
    AND EXISTS (
        SELECT 1 FROM derived_output_group_members
        WHERE output_group_id = NEW.output_group_id
          AND artifact_id = NEW.anchor_artifact_id
    )
    AND EXISTS (
        SELECT 1 FROM dataset_manifests
        WHERE manifest_id = NEW.anchor_manifest_id
          AND artifact_id = NEW.anchor_artifact_id
    )
    AND EXISTS (
        SELECT 1 FROM research_use_decisions
        WHERE decision_id = NEW.decision_id
          AND outcome = 'allowed'
          AND requested_use IN ('local_analysis', 'train')
          AND decided_at_ns <= NEW.committed_at_ns
          AND NEW.committed_at_ns < expires_at_ns
          AND graph_digest = NEW.parent_graph_digest
    )
)
BEGIN
    SELECT RAISE(ABORT, 'derived output group is invalid');
END;

CREATE TRIGGER derived_output_groups_immutable_update
BEFORE UPDATE ON derived_output_groups BEGIN
    SELECT RAISE(ABORT, 'derived output groups are immutable');
END;

CREATE TRIGGER derived_output_groups_immutable_delete
BEFORE DELETE ON derived_output_groups BEGIN
    SELECT RAISE(ABORT, 'derived output groups are immutable');
END;

CREATE TRIGGER derived_output_group_members_immutable_update
BEFORE UPDATE ON derived_output_group_members BEGIN
    SELECT RAISE(ABORT, 'derived output group members are immutable');
END;

CREATE TRIGGER derived_output_group_members_immutable_delete
BEFORE DELETE ON derived_output_group_members BEGIN
    SELECT RAISE(ABORT, 'derived output group members are immutable');
END;

CREATE TABLE derived_generation_authorizations (
    generation_sequence INTEGER PRIMARY KEY
        REFERENCES analytical_generations(generation_sequence),
    decision_id BLOB NOT NULL UNIQUE REFERENCES research_use_decisions(decision_id),
    output_group_id BLOB NOT NULL UNIQUE REFERENCES derived_output_groups(output_group_id),
    requested_use TEXT NOT NULL CHECK (requested_use IN ('local_analysis', 'train')),
    graph_digest BLOB NOT NULL CHECK (length(graph_digest) = 32),
    build_spec_digest BLOB NOT NULL CHECK (length(build_spec_digest) = 32)
) STRICT;

CREATE TRIGGER derived_generation_authorizations_guarded_insert
BEFORE INSERT ON derived_generation_authorizations
WHEN NOT EXISTS (
    SELECT 1
    FROM analytical_generations AS generation
    JOIN derived_output_groups AS output
      ON output.output_group_id = NEW.output_group_id
    JOIN research_use_decisions AS decision
      ON decision.decision_id = NEW.decision_id
    WHERE generation.generation_sequence = NEW.generation_sequence
      AND generation.generation_kind = 'derived'
      AND generation.build_spec_digest = NEW.build_spec_digest
      AND generation.dataset_id = output.dataset_id
      AND generation.schema_name = output.schema_name
      AND generation.schema_version = output.schema_version
      AND generation.schema_fingerprint = output.schema_fingerprint
      AND generation.content_hash = output.plan_content_hash
      AND generation.lineage_hash = output.plan_lineage_hash
      AND generation.row_count = output.row_count
      AND generation.total_bytes = output.total_bytes
      AND generation.anchor_manifest_id = output.anchor_manifest_id
      AND output.build_spec_digest = NEW.build_spec_digest
      AND output.decision_id = decision.decision_id
      AND output.parent_graph_digest = NEW.graph_digest
      AND decision.graph_digest = NEW.graph_digest
      AND decision.requested_use = NEW.requested_use
      AND decision.outcome = 'allowed'
      AND decision.decided_at_ns <= output.committed_at_ns
      AND output.committed_at_ns < decision.expires_at_ns
      AND generation.parent_count = decision.root_count
      AND output.object_count = (
          SELECT COUNT(*) FROM analytical_generation_objects AS object
          WHERE object.dataset_id = generation.dataset_id
            AND object.manifest_version = generation.manifest_version
      )
      AND NOT EXISTS (
          SELECT 1 FROM derived_output_group_members AS member
          WHERE member.output_group_id = output.output_group_id
            AND NOT EXISTS (
                SELECT 1 FROM analytical_generation_objects AS object
                WHERE object.dataset_id = generation.dataset_id
                  AND object.manifest_version = generation.manifest_version
                  AND object.artifact_id = member.artifact_id
            )
      )
      AND NOT EXISTS (
          SELECT 1 FROM analytical_generation_objects AS object
          WHERE object.dataset_id = generation.dataset_id
            AND object.manifest_version = generation.manifest_version
            AND NOT EXISTS (
                SELECT 1 FROM derived_output_group_members AS member
                WHERE member.output_group_id = output.output_group_id
                  AND member.artifact_id = object.artifact_id
            )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM research_use_decision_roots AS root
          WHERE root.decision_id = decision.decision_id
            AND NOT EXISTS (
                SELECT 1 FROM analytical_generation_parents AS parent
                WHERE parent.child_dataset_id = generation.dataset_id
                  AND parent.child_manifest_version = generation.manifest_version
                  AND parent.relation = 'derived_input'
                  AND parent.parent_generation_sequence = root.generation_sequence
                  AND parent.parent_dataset_id = root.dataset_id
                  AND parent.parent_manifest_version = root.manifest_version
                  AND parent.parent_schema_name = root.schema_name
                  AND parent.parent_schema_version = root.schema_version
                  AND parent.parent_schema_fingerprint = root.schema_fingerprint
                  AND parent.parent_content_hash = root.content_hash
            )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'derived generation authorization is invalid');
END;

CREATE TRIGGER derived_generation_authorizations_immutable_update
BEFORE UPDATE ON derived_generation_authorizations BEGIN
    SELECT RAISE(ABORT, 'derived generation authorizations are immutable');
END;

CREATE TRIGGER derived_generation_authorizations_immutable_delete
BEFORE DELETE ON derived_generation_authorizations BEGIN
    SELECT RAISE(ABORT, 'derived generation authorizations are immutable');
END;

CREATE TABLE research_use_authority_migration_guard (
    must_be_zero INTEGER NOT NULL CHECK (must_be_zero = 0)
) STRICT;

INSERT INTO research_use_authority_migration_guard(must_be_zero)
SELECT 1
WHERE EXISTS (
    SELECT 1
    FROM analytical_generations AS generation
    LEFT JOIN analytical_generation_source_inputs AS source_input
      ON source_input.generation_sequence = generation.generation_sequence
    GROUP BY generation.generation_sequence, generation.generation_kind
    HAVING (
        generation.generation_kind = 'ingest'
        AND COUNT(source_input.generation_sequence) <> 1
    ) OR (
        generation.generation_kind <> 'ingest'
        AND COUNT(source_input.generation_sequence) <> 0
    )
) OR EXISTS (
    SELECT 1 FROM source_rights
    WHERE fingerprint_version <> 1
       OR basis_kind <> 'reviewed_terms'
       OR basis_root_algorithm IS NOT NULL
       OR basis_root_digest IS NOT NULL
);

DROP TABLE research_use_authority_migration_guard;
