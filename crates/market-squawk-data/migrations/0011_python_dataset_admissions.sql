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
    )
) BEGIN
    SELECT RAISE(ABORT, 'analytical generation schema identity is not registered');
END;

CREATE TABLE feature_dataset_production_admissions (
    production_identity_sha256 BLOB PRIMARY KEY CHECK (
        length(production_identity_sha256) = 32
        AND production_identity_sha256 <>
            X'0000000000000000000000000000000000000000000000000000000000000000'
    ),
    receipt_schema TEXT NOT NULL CHECK (
        receipt_schema = 'market-squawk.feature-dataset-production-receipt/v1'
    ),
    receipt_sha256 BLOB NOT NULL UNIQUE CHECK (
        length(receipt_sha256) = 32
        AND receipt_sha256 <>
            X'0000000000000000000000000000000000000000000000000000000000000000'
    ),
    receipt_json BLOB NOT NULL CHECK (
        length(receipt_json) BETWEEN 1 AND 1048576
        AND json_valid(CAST(receipt_json AS TEXT))
    ),
    product_contract TEXT NOT NULL CHECK (
        product_contract IN (
            'market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-forward-return.analysis/v1',
            'market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-forward-return.training/v1'
        )
    ),
    export_sha256 BLOB NOT NULL UNIQUE CHECK (
        length(export_sha256) = 32
        AND export_sha256 <>
            X'0000000000000000000000000000000000000000000000000000000000000000'
    ),
    catalog_identity BLOB NOT NULL CHECK (
        length(catalog_identity) = 32
        AND catalog_identity <>
            X'0000000000000000000000000000000000000000000000000000000000000000'
    ),
    dataset_id TEXT NOT NULL,
    manifest_version INTEGER NOT NULL CHECK (manifest_version > 0),
    descriptor_json BLOB NOT NULL CHECK (
        length(descriptor_json) BETWEEN 1 AND 1048576
        AND json_valid(CAST(descriptor_json AS TEXT))
    ),
    selection_digest_version INTEGER NOT NULL CHECK (selection_digest_version = 2),
    output_group_id BLOB NOT NULL UNIQUE REFERENCES derived_output_groups(output_group_id)
        CHECK (
            length(output_group_id) = 32
            AND output_group_id <>
                X'0000000000000000000000000000000000000000000000000000000000000000'
        ),
    final_output_rights_id BLOB NOT NULL REFERENCES source_rights(rights_id)
        CHECK (
            length(final_output_rights_id) = 32
            AND final_output_rights_id <>
                X'0000000000000000000000000000000000000000000000000000000000000000'
        ),
    research_decision_id BLOB NOT NULL UNIQUE REFERENCES research_use_decisions(decision_id)
        CHECK (
            length(research_decision_id) = 32
            AND research_decision_id <>
                X'0000000000000000000000000000000000000000000000000000000000000000'
        ),
    research_graph_digest BLOB NOT NULL CHECK (
        length(research_graph_digest) = 32
        AND research_graph_digest <>
            X'0000000000000000000000000000000000000000000000000000000000000000'
    ),
    research_use TEXT NOT NULL CHECK (research_use IN ('local_analysis', 'train')),
    research_use_expires_at_ns INTEGER NOT NULL,
    admitted_at_ns INTEGER NOT NULL CHECK (admitted_at_ns < research_use_expires_at_ns),
    CHECK (
        (
            product_contract =
                'market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-forward-return.analysis/v1'
            AND research_use = 'local_analysis'
        ) OR (
            product_contract =
                'market-squawk.feature-dataset.price-return-macro-context-fixed-horizon-forward-return.training/v1'
            AND research_use = 'train'
        )
    ),
    UNIQUE (dataset_id, manifest_version),
    FOREIGN KEY (dataset_id, manifest_version)
        REFERENCES analytical_generations(dataset_id, manifest_version)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER feature_dataset_production_admissions_guarded_insert
BEFORE INSERT ON feature_dataset_production_admissions
WHEN NOT EXISTS (
    SELECT 1
    FROM analytical_generations AS generation
    JOIN derived_generation_authorizations AS generation_authorization
      ON generation_authorization.generation_sequence = generation.generation_sequence
    JOIN derived_output_groups AS output_group
      ON output_group.output_group_id = generation_authorization.output_group_id
    JOIN derived_output_group_members AS output_member
      ON output_member.output_group_id = output_group.output_group_id
     AND output_member.ordinal = 0
    JOIN analytical_generation_objects AS output_object
      ON output_object.dataset_id = generation.dataset_id
     AND output_object.manifest_version = generation.manifest_version
     AND output_object.ordinal = 0
     AND output_object.artifact_id = output_member.artifact_id
    JOIN source_rights AS publication_output_rights
      ON publication_output_rights.rights_id = output_member.rights_id
    JOIN source_rights AS final_output_rights
      ON final_output_rights.rights_id = NEW.final_output_rights_id
    JOIN research_use_decisions AS admission_decision
      ON admission_decision.decision_id = NEW.research_decision_id
    WHERE generation.dataset_id = NEW.dataset_id
      AND generation.manifest_version = NEW.manifest_version
      AND generation.generation_kind = 'derived'
      AND generation_authorization.output_group_id = NEW.output_group_id
      AND output_group.object_count = 1
      AND output_member.retention_operation = 'persist'
      AND publication_output_rights.source_id = final_output_rights.source_id
      AND publication_output_rights.payload_algorithm = final_output_rights.payload_algorithm
      AND publication_output_rights.payload_digest = final_output_rights.payload_digest
      AND publication_output_rights.basis_reference = final_output_rights.basis_reference
      AND publication_output_rights.basis_algorithm = final_output_rights.basis_algorithm
      AND publication_output_rights.basis_digest = final_output_rights.basis_digest
      AND publication_output_rights.authorization_algorithm =
          final_output_rights.authorization_algorithm
      AND publication_output_rights.authorization_digest =
          final_output_rights.authorization_digest
      AND publication_output_rights.authorization_expires_at_ns IS
          final_output_rights.authorization_expires_at_ns
      AND publication_output_rights.operation_mask = final_output_rights.operation_mask
      AND publication_output_rights.basis_kind = final_output_rights.basis_kind
      AND publication_output_rights.basis_root_algorithm IS
          final_output_rights.basis_root_algorithm
      AND publication_output_rights.basis_root_digest IS final_output_rights.basis_root_digest
      AND publication_output_rights.fingerprint_version = final_output_rights.fingerprint_version
      AND final_output_rights.payload_algorithm = 1
      AND final_output_rights.payload_digest = output_object.content_hash
      AND final_output_rights.operation_mask = 4
      AND final_output_rights.admitted_at_ns <= NEW.admitted_at_ns
      AND (
          final_output_rights.authorization_expires_at_ns IS NULL
          OR NEW.admitted_at_ns < final_output_rights.authorization_expires_at_ns
      )
      AND generation_authorization.requested_use = NEW.research_use
      AND generation_authorization.graph_digest = NEW.research_graph_digest
      AND admission_decision.graph_digest = NEW.research_graph_digest
      AND admission_decision.requested_use = NEW.research_use
      AND admission_decision.outcome = 'allowed'
      AND admission_decision.expires_at_ns = NEW.research_use_expires_at_ns
      AND admission_decision.decided_at_ns <= NEW.admitted_at_ns
      AND NEW.admitted_at_ns < admission_decision.expires_at_ns
      AND generation.parent_count = admission_decision.root_count
      AND NOT EXISTS (
          SELECT 1
          FROM research_use_decision_roots AS root
          WHERE root.decision_id = admission_decision.decision_id
            AND NOT EXISTS (
                SELECT 1
                FROM analytical_generation_parents AS parent
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
) BEGIN
    SELECT RAISE(ABORT, 'feature-dataset production admission is invalid');
END;

CREATE TRIGGER feature_dataset_production_admissions_immutable_update
BEFORE UPDATE ON feature_dataset_production_admissions BEGIN
    SELECT RAISE(ABORT, 'feature-dataset production admissions are immutable');
END;

CREATE TRIGGER feature_dataset_production_admissions_immutable_delete
BEFORE DELETE ON feature_dataset_production_admissions BEGIN
    SELECT RAISE(ABORT, 'feature-dataset production admissions are immutable');
END;
