CREATE TABLE fair_value_record_identities (
    record_kind INTEGER NOT NULL CHECK (record_kind BETWEEN 1 AND 8),
    record_id BLOB NOT NULL CHECK (length(record_id) = 32 AND record_id <> zeroblob(32)),
    payload_sha256 BLOB NOT NULL CHECK (
        length(payload_sha256) = 32 AND payload_sha256 <> zeroblob(32)
    ),
    appended_at_ns INTEGER NOT NULL,
    PRIMARY KEY (record_kind, record_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE fair_value_evidence (
    record_kind INTEGER NOT NULL DEFAULT 1 CHECK (record_kind = 1),
    record_id BLOB NOT NULL CHECK (length(record_id) = 32),
    payload BLOB NOT NULL CHECK (length(payload) BETWEEN 1 AND 16777216),
    PRIMARY KEY (record_id),
    FOREIGN KEY (record_kind, record_id)
        REFERENCES fair_value_record_identities(record_kind, record_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE fair_value_inputs (
    record_kind INTEGER NOT NULL DEFAULT 2 CHECK (record_kind = 2),
    record_id BLOB NOT NULL CHECK (length(record_id) = 32),
    payload BLOB NOT NULL CHECK (length(payload) BETWEEN 1 AND 16777216),
    PRIMARY KEY (record_id),
    FOREIGN KEY (record_kind, record_id)
        REFERENCES fair_value_record_identities(record_kind, record_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE fair_value_measurements (
    record_kind INTEGER NOT NULL DEFAULT 3 CHECK (record_kind = 3),
    record_id BLOB NOT NULL CHECK (length(record_id) = 32),
    payload BLOB NOT NULL CHECK (length(payload) BETWEEN 1 AND 16777216),
    PRIMARY KEY (record_id),
    FOREIGN KEY (record_kind, record_id)
        REFERENCES fair_value_record_identities(record_kind, record_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE fair_value_decisions (
    record_kind INTEGER NOT NULL DEFAULT 4 CHECK (record_kind = 4),
    record_id BLOB NOT NULL CHECK (length(record_id) = 32),
    payload BLOB NOT NULL CHECK (length(payload) BETWEEN 1 AND 16777216),
    PRIMARY KEY (record_id),
    FOREIGN KEY (record_kind, record_id)
        REFERENCES fair_value_record_identities(record_kind, record_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE fair_value_overrides (
    record_kind INTEGER NOT NULL DEFAULT 5 CHECK (record_kind = 5),
    record_id BLOB NOT NULL CHECK (length(record_id) = 32),
    payload BLOB NOT NULL CHECK (length(payload) BETWEEN 1 AND 16777216),
    PRIMARY KEY (record_id),
    FOREIGN KEY (record_kind, record_id)
        REFERENCES fair_value_record_identities(record_kind, record_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE fair_value_approvals (
    record_kind INTEGER NOT NULL DEFAULT 6 CHECK (record_kind = 6),
    record_id BLOB NOT NULL CHECK (length(record_id) = 32),
    payload BLOB NOT NULL CHECK (length(payload) BETWEEN 1 AND 16777216),
    PRIMARY KEY (record_id),
    FOREIGN KEY (record_kind, record_id)
        REFERENCES fair_value_record_identities(record_kind, record_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE fair_value_revocations (
    record_kind INTEGER NOT NULL DEFAULT 7 CHECK (record_kind = 7),
    record_id BLOB NOT NULL CHECK (length(record_id) = 32),
    payload BLOB NOT NULL CHECK (length(payload) BETWEEN 1 AND 16777216),
    PRIMARY KEY (record_id),
    FOREIGN KEY (record_kind, record_id)
        REFERENCES fair_value_record_identities(record_kind, record_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE fair_value_market_access (
    record_kind INTEGER NOT NULL DEFAULT 8 CHECK (record_kind = 8),
    record_id BLOB NOT NULL CHECK (length(record_id) = 32),
    payload BLOB NOT NULL CHECK (length(payload) BETWEEN 1 AND 16777216),
    PRIMARY KEY (record_id),
    FOREIGN KEY (record_kind, record_id)
        REFERENCES fair_value_record_identities(record_kind, record_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE fair_value_operations (
    operation_id BLOB PRIMARY KEY CHECK (
        length(operation_id) = 32 AND operation_id <> zeroblob(32)
    ),
    operation_kind INTEGER NOT NULL CHECK (operation_kind BETWEEN 1 AND 5),
    actor TEXT NOT NULL CHECK (
        length(CAST(actor AS BLOB)) BETWEEN 1 AND 128
    ),
    business_at_ns INTEGER NOT NULL,
    operation_sha256 BLOB NOT NULL UNIQUE CHECK (
        length(operation_sha256) = 32 AND operation_sha256 <> zeroblob(32)
    ),
    appended_at_ns INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE fair_value_operation_records (
    operation_id BLOB NOT NULL REFERENCES fair_value_operations(operation_id),
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 16383),
    record_kind INTEGER NOT NULL CHECK (record_kind BETWEEN 1 AND 8),
    record_id BLOB NOT NULL CHECK (length(record_id) = 32),
    PRIMARY KEY (operation_id, ordinal),
    UNIQUE (operation_id, record_kind, record_id),
    FOREIGN KEY (record_kind, record_id)
        REFERENCES fair_value_record_identities(record_kind, record_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE fair_value_record_links (
    operation_id BLOB NOT NULL REFERENCES fair_value_operations(operation_id),
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 16383),
    source_kind INTEGER NOT NULL CHECK (source_kind BETWEEN 1 AND 8),
    source_id BLOB NOT NULL CHECK (length(source_id) = 32),
    relation_kind INTEGER NOT NULL CHECK (relation_kind BETWEEN 1 AND 8),
    target_kind INTEGER NOT NULL CHECK (target_kind BETWEEN 1 AND 8),
    target_id BLOB NOT NULL CHECK (length(target_id) = 32),
    PRIMARY KEY (operation_id, ordinal),
    UNIQUE (operation_id, source_kind, source_id, relation_kind, target_kind, target_id),
    FOREIGN KEY (source_kind, source_id)
        REFERENCES fair_value_record_identities(record_kind, record_id),
    FOREIGN KEY (target_kind, target_id)
        REFERENCES fair_value_record_identities(record_kind, record_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE fair_value_audit (
    sequence INTEGER PRIMARY KEY CHECK (sequence >= 1),
    audit_id BLOB NOT NULL UNIQUE CHECK (
        length(audit_id) = 32 AND audit_id <> zeroblob(32)
    ),
    previous_audit_id BLOB CHECK (
        previous_audit_id IS NULL OR (
            length(previous_audit_id) = 32 AND previous_audit_id <> zeroblob(32)
        )
    ),
    operation_id BLOB NOT NULL UNIQUE REFERENCES fair_value_operations(operation_id),
    operation_kind INTEGER NOT NULL CHECK (operation_kind BETWEEN 1 AND 5),
    actor TEXT NOT NULL CHECK (length(CAST(actor AS BLOB)) BETWEEN 1 AND 128),
    business_at_ns INTEGER NOT NULL,
    appended_at_ns INTEGER NOT NULL,
    audit_sha256 BLOB NOT NULL UNIQUE CHECK (
        length(audit_sha256) = 32 AND audit_sha256 <> zeroblob(32)
    ),
    CHECK (
        (sequence = 1 AND previous_audit_id IS NULL)
        OR (sequence > 1 AND previous_audit_id IS NOT NULL)
    )
) STRICT;

CREATE TRIGGER fair_value_audit_contiguous_insert
BEFORE INSERT ON fair_value_audit
WHEN NEW.sequence <> COALESCE((SELECT MAX(sequence) + 1 FROM fair_value_audit), 1)
  OR (
      NEW.sequence > 1
      AND NEW.previous_audit_id <> (
          SELECT audit_id FROM fair_value_audit WHERE sequence = NEW.sequence - 1
      )
  )
BEGIN
    SELECT RAISE(ABORT, 'fair-value audit chain is not contiguous');
END;

CREATE TRIGGER fair_value_records_immutable_update
BEFORE UPDATE ON fair_value_record_identities
BEGIN
    SELECT RAISE(ABORT, 'fair-value records are immutable');
END;

CREATE TRIGGER fair_value_records_immutable_delete
BEFORE DELETE ON fair_value_record_identities
BEGIN
    SELECT RAISE(ABORT, 'fair-value records are immutable');
END;

CREATE TRIGGER fair_value_evidence_immutable_update BEFORE UPDATE ON fair_value_evidence
BEGIN SELECT RAISE(ABORT, 'fair-value evidence is immutable'); END;
CREATE TRIGGER fair_value_evidence_immutable_delete BEFORE DELETE ON fair_value_evidence
BEGIN SELECT RAISE(ABORT, 'fair-value evidence is immutable'); END;

CREATE TRIGGER fair_value_inputs_immutable_update BEFORE UPDATE ON fair_value_inputs
BEGIN SELECT RAISE(ABORT, 'fair-value inputs are immutable'); END;
CREATE TRIGGER fair_value_inputs_immutable_delete BEFORE DELETE ON fair_value_inputs
BEGIN SELECT RAISE(ABORT, 'fair-value inputs are immutable'); END;

CREATE TRIGGER fair_value_measurements_immutable_update BEFORE UPDATE ON fair_value_measurements
BEGIN SELECT RAISE(ABORT, 'fair-value measurements are immutable'); END;
CREATE TRIGGER fair_value_measurements_immutable_delete BEFORE DELETE ON fair_value_measurements
BEGIN SELECT RAISE(ABORT, 'fair-value measurements are immutable'); END;

CREATE TRIGGER fair_value_decisions_immutable_update BEFORE UPDATE ON fair_value_decisions
BEGIN SELECT RAISE(ABORT, 'fair-value decisions are immutable'); END;
CREATE TRIGGER fair_value_decisions_immutable_delete BEFORE DELETE ON fair_value_decisions
BEGIN SELECT RAISE(ABORT, 'fair-value decisions are immutable'); END;

CREATE TRIGGER fair_value_overrides_immutable_update BEFORE UPDATE ON fair_value_overrides
BEGIN SELECT RAISE(ABORT, 'fair-value overrides are immutable'); END;
CREATE TRIGGER fair_value_overrides_immutable_delete BEFORE DELETE ON fair_value_overrides
BEGIN SELECT RAISE(ABORT, 'fair-value overrides are immutable'); END;

CREATE TRIGGER fair_value_approvals_immutable_update BEFORE UPDATE ON fair_value_approvals
BEGIN SELECT RAISE(ABORT, 'fair-value approvals are immutable'); END;
CREATE TRIGGER fair_value_approvals_immutable_delete BEFORE DELETE ON fair_value_approvals
BEGIN SELECT RAISE(ABORT, 'fair-value approvals are immutable'); END;

CREATE TRIGGER fair_value_revocations_immutable_update BEFORE UPDATE ON fair_value_revocations
BEGIN SELECT RAISE(ABORT, 'fair-value revocations are immutable'); END;
CREATE TRIGGER fair_value_revocations_immutable_delete BEFORE DELETE ON fair_value_revocations
BEGIN SELECT RAISE(ABORT, 'fair-value revocations are immutable'); END;

CREATE TRIGGER fair_value_market_access_immutable_update BEFORE UPDATE ON fair_value_market_access
BEGIN SELECT RAISE(ABORT, 'fair-value market access is immutable'); END;
CREATE TRIGGER fair_value_market_access_immutable_delete BEFORE DELETE ON fair_value_market_access
BEGIN SELECT RAISE(ABORT, 'fair-value market access is immutable'); END;

CREATE TRIGGER fair_value_operations_immutable_update BEFORE UPDATE ON fair_value_operations
BEGIN SELECT RAISE(ABORT, 'fair-value operations are immutable'); END;
CREATE TRIGGER fair_value_operations_immutable_delete BEFORE DELETE ON fair_value_operations
BEGIN SELECT RAISE(ABORT, 'fair-value operations are immutable'); END;

CREATE TRIGGER fair_value_operation_records_immutable_update
BEFORE UPDATE ON fair_value_operation_records
BEGIN SELECT RAISE(ABORT, 'fair-value operation membership is immutable'); END;
CREATE TRIGGER fair_value_operation_records_immutable_delete
BEFORE DELETE ON fair_value_operation_records
BEGIN SELECT RAISE(ABORT, 'fair-value operation membership is immutable'); END;

CREATE TRIGGER fair_value_record_links_immutable_update BEFORE UPDATE ON fair_value_record_links
BEGIN SELECT RAISE(ABORT, 'fair-value links are immutable'); END;
CREATE TRIGGER fair_value_record_links_immutable_delete BEFORE DELETE ON fair_value_record_links
BEGIN SELECT RAISE(ABORT, 'fair-value links are immutable'); END;

CREATE TRIGGER fair_value_audit_immutable_update BEFORE UPDATE ON fair_value_audit
BEGIN SELECT RAISE(ABORT, 'fair-value audit is immutable'); END;
CREATE TRIGGER fair_value_audit_immutable_delete BEFORE DELETE ON fair_value_audit
BEGIN SELECT RAISE(ABORT, 'fair-value audit is immutable'); END;
