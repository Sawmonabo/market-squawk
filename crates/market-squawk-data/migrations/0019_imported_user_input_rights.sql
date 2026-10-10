CREATE TABLE imported_user_input_rights (
    rights_id BLOB PRIMARY KEY REFERENCES source_rights(rights_id),
    binding_schema_version INTEGER NOT NULL CHECK (binding_schema_version = 1),
    admitted_input_set_algorithm INTEGER NOT NULL
        CHECK (admitted_input_set_algorithm IN (1, 2)),
    admitted_input_set_digest BLOB NOT NULL
        CHECK (length(admitted_input_set_digest) = 32
               AND admitted_input_set_digest <> zeroblob(32)),
    generated_manifest_algorithm INTEGER NOT NULL
        CHECK (generated_manifest_algorithm IN (1, 2)),
    generated_manifest_digest BLOB NOT NULL
        CHECK (length(generated_manifest_digest) = 32
               AND generated_manifest_digest <> zeroblob(32)),
    local_admission_algorithm INTEGER NOT NULL
        CHECK (local_admission_algorithm IN (1, 2)),
    local_admission_digest BLOB NOT NULL
        CHECK (length(local_admission_digest) = 32
               AND local_admission_digest <> zeroblob(32)),
    workspace_receipt_algorithm INTEGER NOT NULL
        CHECK (workspace_receipt_algorithm IN (1, 2)),
    workspace_receipt_digest BLOB NOT NULL
        CHECK (length(workspace_receipt_digest) = 32
               AND workspace_receipt_digest <> zeroblob(32)),
    import_receipt_algorithm INTEGER NOT NULL
        CHECK (import_receipt_algorithm IN (1, 2)),
    import_receipt_digest BLOB NOT NULL
        CHECK (length(import_receipt_digest) = 32
               AND import_receipt_digest <> zeroblob(32)),
    binding_algorithm INTEGER NOT NULL CHECK (binding_algorithm = 1),
    binding_digest BLOB NOT NULL
        CHECK (length(binding_digest) = 32 AND binding_digest <> zeroblob(32))
) STRICT;

CREATE TRIGGER imported_user_input_rights_guarded_insert
BEFORE INSERT ON imported_user_input_rights
WHEN NOT EXISTS (
    SELECT 1
    FROM source_rights AS rights
    WHERE rights.rights_id = NEW.rights_id
      AND rights.fingerprint_version = 2
      AND rights.basis_kind = 'user_owned_local'
      AND rights.basis_algorithm = NEW.generated_manifest_algorithm
      AND rights.basis_digest = NEW.generated_manifest_digest
      AND rights.basis_root_algorithm = NEW.binding_algorithm
      AND rights.basis_root_digest = NEW.binding_digest
      AND rights.basis_reference =
          'imported-user-input-v1:sha256:' || lower(hex(NEW.binding_digest))
)
BEGIN
    SELECT RAISE(ABORT, 'imported user-input rights binding is invalid');
END;

CREATE TRIGGER imported_user_input_rights_immutable_update
BEFORE UPDATE ON imported_user_input_rights BEGIN
    SELECT RAISE(ABORT, 'imported user-input rights are immutable');
END;

CREATE TRIGGER imported_user_input_rights_immutable_delete
BEFORE DELETE ON imported_user_input_rights BEGIN
    SELECT RAISE(ABORT, 'imported user-input rights are immutable');
END;
