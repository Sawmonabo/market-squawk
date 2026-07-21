CREATE TABLE observed_revision_families (
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    family_algorithm INTEGER NOT NULL CHECK (family_algorithm = 1),
    family_digest BLOB NOT NULL CHECK (length(family_digest) = 32),
    family_encoding_version INTEGER NOT NULL CHECK (family_encoding_version = 1),
    family_evidence BLOB NOT NULL CHECK (
        length(family_evidence) BETWEEN 1 AND 16384
    ),
    PRIMARY KEY (source_id, family_algorithm, family_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE observed_revision_versions (
    source_id TEXT NOT NULL,
    family_algorithm INTEGER NOT NULL CHECK (family_algorithm = 1),
    family_digest BLOB NOT NULL CHECK (length(family_digest) = 32),
    revision INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 4294967295),
    version_kind TEXT NOT NULL CHECK (
        version_kind IN ('provider_supplied', 'locally_observed_content')
    ),
    version_algorithm INTEGER NOT NULL CHECK (version_algorithm = 1),
    version_digest BLOB NOT NULL CHECK (length(version_digest) = 32),
    version_evidence_version INTEGER NOT NULL CHECK (version_evidence_version = 1),
    version_evidence BLOB NOT NULL CHECK (
        length(version_evidence) BETWEEN 1 AND 65536
        AND (
            version_kind = 'locally_observed_content'
            OR length(version_evidence) <= 8192
        )
    ),
    payload_algorithm INTEGER NOT NULL CHECK (payload_algorithm = 1),
    payload_digest BLOB NOT NULL CHECK (length(payload_digest) = 32),
    payload_evidence_version INTEGER NOT NULL CHECK (payload_evidence_version = 1),
    payload_evidence BLOB NOT NULL CHECK (
        length(payload_evidence) BETWEEN 1 AND 65536
    ),
    provider_order_evidence_version INTEGER CHECK (
        provider_order_evidence_version IS NULL
        OR provider_order_evidence_version = 1
    ),
    provider_coordinate_json TEXT CHECK (
        provider_coordinate_json IS NULL
        OR (
            length(CAST(provider_coordinate_json AS BLOB)) BETWEEN 1 AND 16384
            AND json_valid(provider_coordinate_json)
        )
    ),
    provider_tie_breaker BLOB CHECK (
        provider_tie_breaker IS NULL
        OR length(provider_tie_breaker) BETWEEN 1 AND 4096
    ),
    assigned_at_ns INTEGER NOT NULL,
    PRIMARY KEY (source_id, family_algorithm, family_digest, revision),
    UNIQUE (
        source_id,
        family_algorithm,
        family_digest,
        version_kind,
        version_algorithm,
        version_digest
    ),
    UNIQUE (
        source_id,
        family_algorithm,
        family_digest,
        revision,
        version_kind,
        version_algorithm,
        version_digest
    ),
    FOREIGN KEY (source_id, family_algorithm, family_digest)
        REFERENCES observed_revision_families(
            source_id,
            family_algorithm,
            family_digest
        ),
    CHECK (
        (version_kind <> 'locally_observed_content')
        OR version_evidence = payload_evidence
    ),
    CHECK (
        version_kind <> 'locally_observed_content'
        OR provider_order_evidence_version IS NULL
    ),
    CHECK (
        (
            provider_order_evidence_version IS NULL
            AND provider_coordinate_json IS NULL
            AND provider_tie_breaker IS NULL
        ) OR (
            provider_order_evidence_version = 1
            AND provider_coordinate_json IS NOT NULL
            AND provider_tie_breaker IS NOT NULL
        )
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE observed_revision_batches (
    batch_algorithm INTEGER NOT NULL CHECK (batch_algorithm = 1),
    batch_digest BLOB NOT NULL CHECK (length(batch_digest) = 32),
    canonical_version INTEGER NOT NULL CHECK (canonical_version = 1),
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    input_count INTEGER NOT NULL CHECK (input_count BETWEEN 0 AND 100000),
    unique_count INTEGER NOT NULL CHECK (
        unique_count BETWEEN 0 AND input_count
    ),
    assigned_at_ns INTEGER NOT NULL,
    PRIMARY KEY (batch_algorithm, batch_digest),
    UNIQUE (source_id, batch_algorithm, batch_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE observed_revision_batch_members (
    source_id TEXT NOT NULL,
    batch_algorithm INTEGER NOT NULL CHECK (batch_algorithm = 1),
    batch_digest BLOB NOT NULL CHECK (length(batch_digest) = 32),
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 99999),
    family_algorithm INTEGER NOT NULL CHECK (family_algorithm = 1),
    family_digest BLOB NOT NULL CHECK (length(family_digest) = 32),
    revision INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 4294967295),
    version_kind TEXT NOT NULL CHECK (
        version_kind IN ('provider_supplied', 'locally_observed_content')
    ),
    version_algorithm INTEGER NOT NULL CHECK (version_algorithm = 1),
    version_digest BLOB NOT NULL CHECK (length(version_digest) = 32),
    PRIMARY KEY (batch_algorithm, batch_digest, ordinal),
    UNIQUE (
        batch_algorithm,
        batch_digest,
        source_id,
        family_algorithm,
        family_digest,
        revision
    ),
    FOREIGN KEY (source_id, batch_algorithm, batch_digest)
        REFERENCES observed_revision_batches(
            source_id,
            batch_algorithm,
            batch_digest
        ) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (
        source_id,
        family_algorithm,
        family_digest,
        revision,
        version_kind,
        version_algorithm,
        version_digest
    ) REFERENCES observed_revision_versions(
        source_id,
        family_algorithm,
        family_digest,
        revision,
        version_kind,
        version_algorithm,
        version_digest
    )
) STRICT, WITHOUT ROWID;

CREATE TRIGGER observed_revision_batches_closed_insert
BEFORE INSERT ON observed_revision_batches
WHEN NOT (
    (SELECT COUNT(*) FROM observed_revision_batch_members
     WHERE batch_algorithm = NEW.batch_algorithm
       AND batch_digest = NEW.batch_digest) = NEW.unique_count
    AND (SELECT COALESCE(MIN(ordinal), 0) FROM observed_revision_batch_members
         WHERE batch_algorithm = NEW.batch_algorithm
           AND batch_digest = NEW.batch_digest) = 0
    AND (SELECT COALESCE(MAX(ordinal), -1) FROM observed_revision_batch_members
         WHERE batch_algorithm = NEW.batch_algorithm
           AND batch_digest = NEW.batch_digest) = NEW.unique_count - 1
    AND NOT EXISTS (
        SELECT 1 FROM observed_revision_batch_members
        WHERE batch_algorithm = NEW.batch_algorithm
          AND batch_digest = NEW.batch_digest
          AND source_id <> NEW.source_id
    )
) BEGIN
    SELECT RAISE(ABORT, 'observed revision batch is not closed');
END;

CREATE TRIGGER observed_revision_versions_contiguous_insert
BEFORE INSERT ON observed_revision_versions
WHEN NEW.revision <> 1 + COALESCE((
        SELECT MAX(revision)
        FROM observed_revision_versions
        WHERE source_id = NEW.source_id
          AND family_algorithm = NEW.family_algorithm
          AND family_digest = NEW.family_digest
    ), 0)
    OR NEW.assigned_at_ns < COALESCE((
        SELECT MAX(assigned_at_ns)
        FROM observed_revision_versions
        WHERE source_id = NEW.source_id
          AND family_algorithm = NEW.family_algorithm
          AND family_digest = NEW.family_digest
    ), NEW.assigned_at_ns)
BEGIN
    SELECT RAISE(ABORT, 'observed revision sequence is not contiguous');
END;

CREATE TRIGGER observed_revision_families_immutable_update
BEFORE UPDATE ON observed_revision_families BEGIN
    SELECT RAISE(ABORT, 'observed revision families are immutable');
END;

CREATE TRIGGER observed_revision_families_immutable_delete
BEFORE DELETE ON observed_revision_families BEGIN
    SELECT RAISE(ABORT, 'observed revision families are immutable');
END;

CREATE TRIGGER observed_revision_versions_immutable_update
BEFORE UPDATE ON observed_revision_versions BEGIN
    SELECT RAISE(ABORT, 'observed revision versions are immutable');
END;

CREATE TRIGGER observed_revision_versions_immutable_delete
BEFORE DELETE ON observed_revision_versions BEGIN
    SELECT RAISE(ABORT, 'observed revision versions are immutable');
END;

CREATE TRIGGER observed_revision_batches_immutable_update
BEFORE UPDATE ON observed_revision_batches BEGIN
    SELECT RAISE(ABORT, 'observed revision batches are immutable');
END;

CREATE TRIGGER observed_revision_batches_immutable_delete
BEFORE DELETE ON observed_revision_batches BEGIN
    SELECT RAISE(ABORT, 'observed revision batches are immutable');
END;

CREATE TRIGGER observed_revision_batch_members_immutable_update
BEFORE UPDATE ON observed_revision_batch_members BEGIN
    SELECT RAISE(ABORT, 'observed revision batch members are immutable');
END;

CREATE TRIGGER observed_revision_batch_members_immutable_delete
BEFORE DELETE ON observed_revision_batch_members BEGIN
    SELECT RAISE(ABORT, 'observed revision batch members are immutable');
END;
