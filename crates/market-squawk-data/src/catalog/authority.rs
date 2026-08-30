//! Append-only catalog-to-artifact-root authority persistence and exact endpoint identity.

use std::fs::File;
use std::num::NonZeroU64;
use std::path::Path;

use market_squawk_domain::Timestamp;
use rusqlite::{Connection, Row, Transaction, params};
use sha2::{Digest as _, Sha256};

use super::{Catalog, CatalogError};
use crate::BackupReceipt;
use crate::authority_transition::evidence::CatalogContentEvidenceDigest;
use crate::authority_transition::{
    ArtifactInventoryDigest, AuthorityEventDigest, AuthorityEvidenceDigest, AuthorityGeneration,
    AuthorityHead, AuthorityMutationToken, AuthoritySnapshot, AuthorityState,
    AuthorityTransitionKind, BoundAuthorityTransition, CatalogEndpointIdentity,
    ControlRecordDigest, LegacyAuthorityRequirement, PreparedAuthorityTransition,
    RestoreReceiptFields, RootEndpointIdentity, RootInstanceId, StableArtifactRootIdentity,
    TransitionId,
};

const AUTHORITY_EVENT_VERSION: i64 = 2;
const MAX_AUTHORITY_EVENTS: usize = 16_384;
const EVENT_COLUMNS: &str = "sequence, format_version, event_kind, previous_event_digest, \
    event_digest, transition_id, transition_kind, authority_generation, target_catalog_identity, \
    target_root_endpoint_identity, root_instance_id, evidence_digest, \
    restore_source_catalog_identity, restore_source_root_identity, \
    restore_source_authority_generation, restore_source_bound_event, \
    restore_source_authority_evidence, restore_source_catalog_content_evidence, \
    restore_artifact_inventory, restore_backup_version, restore_backup_byte_length, \
    restore_backup_sha256, restore_snapshot_at_ns, root_binding_generation, \
    root_marker_record_digest, stable_root_identity, root_binding_record_digest";

#[derive(Clone, Debug, Eq, PartialEq)]
enum AuthorityEvent {
    LegacyRequired {
        catalog_identity: CatalogEndpointIdentity,
        evidence_digest: AuthorityEvidenceDigest,
    },
    Prepared(PreparedAuthorityTransition),
    Bound(BoundAuthorityTransition),
}

impl AuthorityEvent {
    const fn kind_tag(&self) -> u8 {
        match self {
            Self::LegacyRequired { .. } => 1,
            Self::Prepared(_) => 2,
            Self::Bound(_) => 3,
        }
    }

    const fn database_kind(&self) -> &'static str {
        match self {
            Self::LegacyRequired { .. } => "legacy_required",
            Self::Prepared(_) => "prepared",
            Self::Bound(_) => "bound",
        }
    }

    const fn prepared(&self) -> Option<&PreparedAuthorityTransition> {
        match self {
            Self::Prepared(prepared) => Some(prepared),
            Self::Bound(bound) => Some(bound.prepared()),
            Self::LegacyRequired { .. } => None,
        }
    }

    const fn catalog_identity(&self) -> CatalogEndpointIdentity {
        match self {
            Self::LegacyRequired {
                catalog_identity, ..
            } => *catalog_identity,
            Self::Prepared(prepared) => prepared.target_catalog_identity(),
            Self::Bound(bound) => bound.prepared().target_catalog_identity(),
        }
    }

    const fn evidence_digest(&self) -> AuthorityEvidenceDigest {
        match self {
            Self::LegacyRequired {
                evidence_digest, ..
            } => *evidence_digest,
            Self::Prepared(prepared) => prepared.evidence_digest(),
            Self::Bound(bound) => bound.prepared().evidence_digest(),
        }
    }
}

#[derive(Debug)]
struct StoredAuthorityEvent {
    sequence: NonZeroU64,
    previous_event_digest: Option<AuthorityEventDigest>,
    event_digest: AuthorityEventDigest,
    event: AuthorityEvent,
}

#[derive(Debug)]
struct RawAuthorityEvent {
    sequence: i64,
    format_version: i64,
    event_kind: String,
    previous_event_digest: Option<Vec<u8>>,
    event_digest: Vec<u8>,
    transition_id: Option<Vec<u8>>,
    transition_kind: Option<String>,
    authority_generation: Option<i64>,
    target_catalog_identity: Vec<u8>,
    target_root_endpoint_identity: Option<Vec<u8>>,
    root_instance_id: Option<Vec<u8>>,
    evidence_digest: Vec<u8>,
    restore_source_catalog_identity: Option<Vec<u8>>,
    restore_source_root_identity: Option<Vec<u8>>,
    restore_source_authority_generation: Option<i64>,
    restore_source_bound_event: Option<Vec<u8>>,
    restore_source_authority_evidence: Option<Vec<u8>>,
    restore_source_catalog_content_evidence: Option<Vec<u8>>,
    restore_artifact_inventory: Option<Vec<u8>>,
    restore_backup_version: Option<i64>,
    restore_backup_byte_length: Option<i64>,
    restore_backup_sha256: Option<Vec<u8>>,
    restore_snapshot_at_ns: Option<i64>,
    root_binding_generation: Option<i64>,
    root_marker_record_digest: Option<Vec<u8>>,
    stable_root_identity: Option<Vec<u8>>,
    root_binding_record_digest: Option<Vec<u8>>,
}

impl Catalog {
    pub(super) fn catalog_endpoint_identity(
        &self,
    ) -> Result<CatalogEndpointIdentity, CatalogError> {
        catalog_endpoint_identity(self.artifact_root_binding)
    }

    pub(super) fn initialization_evidence_digest(
        &self,
        root_endpoint: RootEndpointIdentity,
    ) -> Result<AuthorityEvidenceDigest, CatalogError> {
        let analytical_records: i64 = self.connection.query_row(
            "SELECT
                 (SELECT COUNT(*) FROM artifacts)
                 + (SELECT COUNT(*) FROM dataset_manifests)
                 + (SELECT COUNT(*) FROM analytical_generations)
                 + (SELECT COUNT(*) FROM analytical_generation_objects)
                 + (SELECT COUNT(*) FROM query_artifact_results)
                 + (SELECT COUNT(*) FROM company_identity_observations)
                 + (SELECT COUNT(*) FROM listing_reference_generations)
                 + (SELECT COUNT(*) FROM listing_reference_files)
                 + (SELECT COUNT(*) FROM listing_reference_values)
                 + (SELECT COUNT(*) FROM listing_reference_memberships)
                 + (SELECT COUNT(*) FROM market_data_instrument_identities)
                 + (SELECT COUNT(*) FROM market_data_instrument_revisions)
                 + (SELECT COUNT(*) FROM market_data_instrument_current)
                 + (SELECT COUNT(*) FROM market_data_instrument_search_terms)
                 + (SELECT COUNT(*) FROM company_security_link_events)
                 + (SELECT COUNT(*) FROM company_security_link_current)
                 + (SELECT COUNT(*) FROM official_options_reference_generations)
                 + (SELECT COUNT(*) FROM official_options_reference_generation_sources)
                 + (SELECT COUNT(*) FROM official_options_reference_objects)
                 + (SELECT COUNT(*) FROM official_options_reference_values)
                 + (SELECT COUNT(*) FROM official_options_reference_memberships)
                 + (SELECT COUNT(*) FROM official_options_reference_alias_resolutions)
                 + (SELECT COUNT(*) FROM official_options_reference_conflicts)
                 + (SELECT COUNT(*) FROM provider_raw_observations)
                 + (SELECT COUNT(*) FROM provider_raw_observation_pages)
                 + (SELECT COUNT(*) FROM sealed_raw_objects)
                 + (SELECT COUNT(*) FROM provider_raw_observation_objects)
                 + (SELECT COUNT(*) FROM provider_raw_observation_frames)
                 + (SELECT COUNT(*) FROM provider_capture_bindings)
                 + (SELECT COUNT(*) FROM provider_capture_binding_native_lineage)
                 + (SELECT COUNT(*) FROM provider_capture_binding_objects)
                 + (SELECT COUNT(*) FROM provider_capture_binding_rows)
                 + (SELECT COUNT(*) FROM provider_response_market_event_bindings)
                 + (SELECT COUNT(*) FROM provider_response_market_event_binding_native_lineage)
                 + (SELECT COUNT(*) FROM provider_response_market_event_binding_rows)
                 + (SELECT COUNT(*) FROM provider_event_microbatches)
                 + (SELECT COUNT(*) FROM provider_event_microbatch_frames)
                 + (SELECT COUNT(*) FROM provider_event_microbatch_objects)
                 + (SELECT COUNT(*) FROM provider_event_bindings)
                 + (SELECT COUNT(*) FROM provider_event_binding_native_lineage)
                 + (SELECT COUNT(*) FROM provider_event_binding_rows)
                 + (SELECT COUNT(*) FROM provider_composite_response_event_bindings)
                 + (SELECT COUNT(*) FROM provider_option_market_bindings)
                 + (SELECT COUNT(*) FROM provider_option_market_binding_native_lineage)
                 + (SELECT COUNT(*) FROM provider_option_market_binding_rows)
                 + (SELECT COUNT(*) FROM provider_logical_publication_bindings)
                 + (SELECT COUNT(*) FROM provider_logical_publication_required_families)
                 + (SELECT COUNT(*) FROM provider_logical_publication_objects)
                 + (SELECT COUNT(*) FROM provider_logical_publication_partitions)
                 + (SELECT COUNT(*) FROM provider_logical_publication_canonical_expectations)
                 + (SELECT COUNT(*) FROM ingest_run_provider_capture_bindings)
                 + (SELECT COUNT(*) FROM ingest_run_provider_publication_bindings)
                 + (SELECT COUNT(*) FROM provider_market_event_selection_index)
                 + (SELECT COUNT(*) FROM analytical_generation_provider_capture_bindings)
                 + (SELECT COUNT(*) FROM analytical_generation_provider_publication_bindings)",
            [],
            |row| row.get(0),
        )?;
        if analytical_records != 0 {
            return Err(CatalogError::ArtifactRootAuthorityTransitionConflict);
        }
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/empty-analytical-authority-evidence/v2");
        digest.update(self.catalog_endpoint_identity()?.bytes());
        digest.update(root_endpoint.bytes());
        AuthorityEvidenceDigest::try_new(digest.finalize().into())
            .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)
    }

    pub(super) fn authority_snapshot(&self) -> Result<AuthoritySnapshot, CatalogError> {
        let expected_catalog_identity = catalog_endpoint_identity(self.artifact_root_binding)?;
        read_authority_snapshot(&self.connection, Some(expected_catalog_identity))
    }

    pub(super) fn authority_snapshot_without_endpoint(
        &self,
    ) -> Result<AuthoritySnapshot, CatalogError> {
        read_authority_snapshot(&self.connection, None)
    }

    pub(super) fn append_prepared_authority(
        &mut self,
        _token: &AuthorityMutationToken,
        prepared: PreparedAuthorityTransition,
    ) -> Result<AuthoritySnapshot, CatalogError> {
        let expected_catalog_identity = catalog_endpoint_identity(self.artifact_root_binding)?;
        if prepared.target_catalog_identity() != expected_catalog_identity {
            return Err(CatalogError::ArtifactRootAuthorityMismatch);
        }
        let transaction = self.connection.transaction()?;
        let snapshot = read_authority_snapshot(&transaction, None)?;
        match snapshot.state() {
            AuthorityState::InitializationRequired => {
                require_first_prepared(&prepared, AuthorityTransitionKind::Initialize)?;
            }
            AuthorityState::LegacyRequired { .. } => {
                require_first_prepared(&prepared, AuthorityTransitionKind::LegacyMigration)?;
            }
            AuthorityState::Prepared {
                transition: existing,
                ..
            } if existing == &prepared => return Ok(snapshot),
            AuthorityState::Bound {
                transition: existing,
                ..
            } if existing.prepared() == &prepared => return Ok(snapshot),
            AuthorityState::Bound {
                head,
                transition: previous,
            } => require_restore_successor(*head, previous, &prepared)?,
            AuthorityState::Prepared { .. } => {
                return Err(CatalogError::ArtifactRootAuthorityTransitionConflict);
            }
        }
        let sequence = next_sequence(snapshot.head())?;
        let previous = snapshot.head().map(AuthorityHead::event_digest);
        insert_event(
            &transaction,
            sequence,
            previous,
            AuthorityEvent::Prepared(prepared),
        )?;
        let snapshot = read_authority_snapshot(&transaction, Some(expected_catalog_identity))?;
        transaction.commit()?;
        Ok(snapshot)
    }

    pub(super) fn append_bound_authority(
        &mut self,
        _token: &AuthorityMutationToken,
        bound: BoundAuthorityTransition,
    ) -> Result<AuthoritySnapshot, CatalogError> {
        let expected_catalog_identity = catalog_endpoint_identity(self.artifact_root_binding)?;
        if bound.prepared().target_catalog_identity() != expected_catalog_identity {
            return Err(CatalogError::ArtifactRootAuthorityMismatch);
        }
        let transaction = self.connection.transaction()?;
        let snapshot = read_authority_snapshot(&transaction, None)?;
        match snapshot.state() {
            AuthorityState::Prepared { transition, .. } if transition == bound.prepared() => {}
            AuthorityState::Bound {
                transition: existing,
                ..
            } if existing == &bound => return Ok(snapshot),
            _ => return Err(CatalogError::ArtifactRootAuthorityTransitionConflict),
        }
        let sequence = next_sequence(snapshot.head())?;
        let previous = snapshot.head().map(AuthorityHead::event_digest);
        insert_event(
            &transaction,
            sequence,
            previous,
            AuthorityEvent::Bound(bound),
        )?;
        let snapshot = read_authority_snapshot(&transaction, Some(expected_catalog_identity))?;
        transaction.commit()?;
        Ok(snapshot)
    }
}

pub(super) fn append_legacy_authority_requirement(
    transaction: &Transaction<'_>,
    catalog_identity: [u8; 32],
    legacy_schema_version: u64,
) -> Result<(), CatalogError> {
    let catalog_identity = catalog_endpoint_identity(catalog_identity)?;
    let evidence_digest = legacy_requirement_digest(catalog_identity, legacy_schema_version)?;
    insert_event(
        transaction,
        NonZeroU64::new(1).ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)?,
        None,
        AuthorityEvent::LegacyRequired {
            catalog_identity,
            evidence_digest,
        },
    )
}

pub(super) fn read_authority_snapshot_without_endpoint(
    connection: &Connection,
) -> Result<AuthoritySnapshot, CatalogError> {
    read_authority_snapshot(connection, None)
}

fn read_authority_snapshot(
    connection: &Connection,
    expected_catalog_identity: Option<CatalogEndpointIdentity>,
) -> Result<AuthoritySnapshot, CatalogError> {
    let query = format!(
        "SELECT {EVENT_COLUMNS} FROM analytical_artifact_root_authority_events \
         ORDER BY sequence LIMIT ?1"
    );
    let limit = i64::try_from(MAX_AUTHORITY_EVENTS)
        .map_err(|_| CatalogError::ArtifactRootAuthorityChainInvalid)?
        .checked_add(1)
        .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)?;
    let mut statement = connection.prepare(&query)?;
    let mut rows = statement.query([limit])?;
    let mut state = AuthorityState::InitializationRequired;
    let mut legacy_requirement = None;
    let mut count = 0_usize;
    while let Some(row) = rows.next()? {
        count = count
            .checked_add(1)
            .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)?;
        if count > MAX_AUTHORITY_EVENTS {
            return Err(CatalogError::ArtifactRootAuthorityChainInvalid);
        }
        let stored = decode_stored_event(raw_authority_event(row)?)?;
        let expected_sequence = u64::try_from(count)
            .ok()
            .and_then(NonZeroU64::new)
            .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)?;
        if stored.sequence != expected_sequence
            || stored.previous_event_digest != state.head().map(AuthorityHead::event_digest)
            || event_digest(stored.sequence, stored.previous_event_digest, &stored.event)?
                != stored.event_digest
        {
            return Err(CatalogError::ArtifactRootAuthorityChainInvalid);
        }
        if count == 1
            && let AuthorityEvent::LegacyRequired {
                catalog_identity,
                evidence_digest,
            } = &stored.event
        {
            legacy_requirement = Some(LegacyAuthorityRequirement::new(
                AuthorityHead::new(stored.sequence, stored.event_digest),
                *catalog_identity,
                *evidence_digest,
            ));
        }
        state = apply_validated_event(state, stored)?;
    }
    if let Some(expected) = expected_catalog_identity {
        validate_active_catalog_endpoint(&state, expected)?;
        if legacy_requirement.is_some_and(|requirement| requirement.catalog_identity() != expected)
        {
            return Err(CatalogError::ArtifactRootAuthorityMismatch);
        }
    }
    Ok(AuthoritySnapshot::new(state, legacy_requirement))
}

fn raw_authority_event(row: &Row<'_>) -> rusqlite::Result<RawAuthorityEvent> {
    Ok(RawAuthorityEvent {
        sequence: row.get(0)?,
        format_version: row.get(1)?,
        event_kind: row.get(2)?,
        previous_event_digest: row.get(3)?,
        event_digest: row.get(4)?,
        transition_id: row.get(5)?,
        transition_kind: row.get(6)?,
        authority_generation: row.get(7)?,
        target_catalog_identity: row.get(8)?,
        target_root_endpoint_identity: row.get(9)?,
        root_instance_id: row.get(10)?,
        evidence_digest: row.get(11)?,
        restore_source_catalog_identity: row.get(12)?,
        restore_source_root_identity: row.get(13)?,
        restore_source_authority_generation: row.get(14)?,
        restore_source_bound_event: row.get(15)?,
        restore_source_authority_evidence: row.get(16)?,
        restore_source_catalog_content_evidence: row.get(17)?,
        restore_artifact_inventory: row.get(18)?,
        restore_backup_version: row.get(19)?,
        restore_backup_byte_length: row.get(20)?,
        restore_backup_sha256: row.get(21)?,
        restore_snapshot_at_ns: row.get(22)?,
        root_binding_generation: row.get(23)?,
        root_marker_record_digest: row.get(24)?,
        stable_root_identity: row.get(25)?,
        root_binding_record_digest: row.get(26)?,
    })
}

fn decode_stored_event(raw: RawAuthorityEvent) -> Result<StoredAuthorityEvent, CatalogError> {
    if raw.format_version != AUTHORITY_EVENT_VERSION {
        return Err(CatalogError::ArtifactRootAuthorityChainInvalid);
    }
    match raw.event_kind.as_str() {
        "legacy_required" => require_legacy_null_shape(&raw)?,
        "prepared" => require_bound_null_shape(&raw)?,
        "bound" => {}
        _ => return Err(CatalogError::ArtifactRootAuthorityChainInvalid),
    }
    let sequence = nonzero_i64(raw.sequence)?;
    let previous_event_digest = raw
        .previous_event_digest
        .clone()
        .map(|value| opaque_digest(value, AuthorityEventDigest::try_new))
        .transpose()?;
    let event_digest = opaque_digest(raw.event_digest.clone(), AuthorityEventDigest::try_new)?;
    let catalog_identity = opaque_digest(
        raw.target_catalog_identity.clone(),
        CatalogEndpointIdentity::try_new,
    )?;
    let evidence_digest = opaque_digest(
        raw.evidence_digest.clone(),
        AuthorityEvidenceDigest::try_new,
    )?;
    let event = match raw.event_kind.as_str() {
        "legacy_required" => AuthorityEvent::LegacyRequired {
            catalog_identity,
            evidence_digest,
        },
        "prepared" | "bound" => {
            let prepared = decode_prepared(&raw, catalog_identity, evidence_digest)?;
            if raw.event_kind == "prepared" {
                AuthorityEvent::Prepared(prepared)
            } else {
                let marker = required_opaque_digest(
                    raw.root_marker_record_digest,
                    ControlRecordDigest::try_new,
                )?;
                let stable_root = required_opaque_digest(
                    raw.stable_root_identity,
                    StableArtifactRootIdentity::try_new,
                )?;
                let binding = required_opaque_digest(
                    raw.root_binding_record_digest,
                    ControlRecordDigest::try_new,
                )?;
                AuthorityEvent::Bound(BoundAuthorityTransition::new(
                    prepared,
                    marker,
                    stable_root,
                    binding,
                ))
            }
        }
        _ => return Err(CatalogError::ArtifactRootAuthorityChainInvalid),
    };
    Ok(StoredAuthorityEvent {
        sequence,
        previous_event_digest,
        event_digest,
        event,
    })
}

fn decode_prepared(
    raw: &RawAuthorityEvent,
    catalog_identity: CatalogEndpointIdentity,
    evidence_digest: AuthorityEvidenceDigest,
) -> Result<PreparedAuthorityTransition, CatalogError> {
    let transition_id = required_array::<16>(raw.transition_id.clone()).and_then(|value| {
        TransitionId::try_from_bytes(value).ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)
    })?;
    let kind = match raw.transition_kind.as_deref() {
        Some("initialize") => AuthorityTransitionKind::Initialize,
        Some("legacy_migration") => AuthorityTransitionKind::LegacyMigration,
        Some("backup_restore") => AuthorityTransitionKind::BackupRestore,
        _ => return Err(CatalogError::ArtifactRootAuthorityChainInvalid),
    };
    let authority_generation = raw
        .authority_generation
        .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)
        .and_then(nonzero_i64)
        .and_then(|value| {
            AuthorityGeneration::try_new(value.get())
                .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)
        })?;
    let target_root = required_opaque_digest(
        raw.target_root_endpoint_identity.clone(),
        RootEndpointIdentity::try_new,
    )?;
    let root_instance =
        required_opaque_digest(raw.root_instance_id.clone(), RootInstanceId::try_new)?;
    let restore_receipt = decode_restore_receipt(raw)?;
    let prepared = PreparedAuthorityTransition::try_new(
        transition_id,
        kind,
        authority_generation,
        catalog_identity,
        target_root,
        root_instance,
        evidence_digest,
        restore_receipt,
    )
    .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)?;
    let stored_generation = raw
        .root_binding_generation
        .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)
        .and_then(|value| {
            u64::try_from(value).map_err(|_| CatalogError::ArtifactRootAuthorityChainInvalid)
        })?;
    if stored_generation != prepared.root_binding_generation().get() {
        return Err(CatalogError::ArtifactRootAuthorityChainInvalid);
    }
    Ok(prepared)
}

fn decode_restore_receipt(
    raw: &RawAuthorityEvent,
) -> Result<Option<RestoreReceiptFields>, CatalogError> {
    let any_present = raw.restore_source_catalog_identity.is_some()
        || raw.restore_source_root_identity.is_some()
        || raw.restore_source_authority_generation.is_some()
        || raw.restore_source_bound_event.is_some()
        || raw.restore_source_authority_evidence.is_some()
        || raw.restore_source_catalog_content_evidence.is_some()
        || raw.restore_artifact_inventory.is_some()
        || raw.restore_backup_version.is_some()
        || raw.restore_backup_byte_length.is_some()
        || raw.restore_backup_sha256.is_some()
        || raw.restore_snapshot_at_ns.is_some();
    if !any_present {
        return Ok(None);
    }
    let source_catalog = required_opaque_digest(
        raw.restore_source_catalog_identity.clone(),
        CatalogEndpointIdentity::try_new,
    )?;
    let source_root = required_opaque_digest(
        raw.restore_source_root_identity.clone(),
        StableArtifactRootIdentity::try_new,
    )?;
    let source_event = required_opaque_digest(
        raw.restore_source_bound_event.clone(),
        AuthorityEventDigest::try_new,
    )?;
    let source_generation = raw
        .restore_source_authority_generation
        .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)
        .and_then(nonzero_i64)
        .and_then(|value| {
            AuthorityGeneration::try_new(value.get())
                .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)
        })?;
    let source_evidence = required_opaque_digest(
        raw.restore_source_authority_evidence.clone(),
        AuthorityEvidenceDigest::try_new,
    )?;
    let source_catalog_content_evidence = required_opaque_digest(
        raw.restore_source_catalog_content_evidence.clone(),
        CatalogContentEvidenceDigest::try_new,
    )?;
    let inventory = required_opaque_digest(
        raw.restore_artifact_inventory.clone(),
        ArtifactInventoryDigest::try_new,
    )?;
    let backup_version = u16::try_from(
        raw.restore_backup_version
            .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)?,
    )
    .map_err(|_| CatalogError::ArtifactRootAuthorityChainInvalid)?;
    let backup_bytes = u64::try_from(
        raw.restore_backup_byte_length
            .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)?,
    )
    .map_err(|_| CatalogError::ArtifactRootAuthorityChainInvalid)?;
    let backup_digest = required_array::<32>(raw.restore_backup_sha256.clone())?;
    let backup = BackupReceipt::try_from_parts(backup_version, backup_bytes, backup_digest)?;
    let snapshot_at = Timestamp::from_unix_nanos(
        raw.restore_snapshot_at_ns
            .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)?,
    );
    Ok(Some(RestoreReceiptFields::new(
        source_catalog,
        source_root,
        source_generation,
        source_event,
        source_evidence,
        source_catalog_content_evidence,
        inventory,
        backup,
        snapshot_at,
    )))
}

fn require_legacy_null_shape(raw: &RawAuthorityEvent) -> Result<(), CatalogError> {
    if raw.transition_id.is_none()
        && raw.transition_kind.is_none()
        && raw.authority_generation.is_none()
        && raw.target_root_endpoint_identity.is_none()
        && raw.root_instance_id.is_none()
        && raw.restore_source_catalog_identity.is_none()
        && raw.restore_source_root_identity.is_none()
        && raw.restore_source_authority_generation.is_none()
        && raw.restore_source_bound_event.is_none()
        && raw.restore_source_authority_evidence.is_none()
        && raw.restore_source_catalog_content_evidence.is_none()
        && raw.restore_artifact_inventory.is_none()
        && raw.restore_backup_version.is_none()
        && raw.restore_backup_byte_length.is_none()
        && raw.restore_backup_sha256.is_none()
        && raw.restore_snapshot_at_ns.is_none()
        && raw.root_binding_generation.is_none()
        && raw.root_marker_record_digest.is_none()
        && raw.stable_root_identity.is_none()
        && raw.root_binding_record_digest.is_none()
    {
        Ok(())
    } else {
        Err(CatalogError::ArtifactRootAuthorityChainInvalid)
    }
}

fn require_bound_null_shape(raw: &RawAuthorityEvent) -> Result<(), CatalogError> {
    if raw.root_marker_record_digest.is_none()
        && raw.stable_root_identity.is_none()
        && raw.root_binding_record_digest.is_none()
    {
        Ok(())
    } else {
        Err(CatalogError::ArtifactRootAuthorityChainInvalid)
    }
}

fn apply_validated_event(
    state: AuthorityState,
    stored: StoredAuthorityEvent,
) -> Result<AuthorityState, CatalogError> {
    let head = AuthorityHead::new(stored.sequence, stored.event_digest);
    match (state, stored.event) {
        (
            AuthorityState::InitializationRequired,
            AuthorityEvent::LegacyRequired {
                evidence_digest, ..
            },
        ) => Ok(AuthorityState::LegacyRequired {
            head,
            evidence_digest,
        }),
        (AuthorityState::InitializationRequired, AuthorityEvent::Prepared(prepared)) => {
            require_first_prepared(&prepared, AuthorityTransitionKind::Initialize)?;
            Ok(AuthorityState::Prepared {
                head,
                transition: prepared,
            })
        }
        (AuthorityState::LegacyRequired { .. }, AuthorityEvent::Prepared(prepared)) => {
            require_first_prepared(&prepared, AuthorityTransitionKind::LegacyMigration)?;
            Ok(AuthorityState::Prepared {
                head,
                transition: prepared,
            })
        }
        (
            AuthorityState::Prepared {
                transition: prepared,
                ..
            },
            AuthorityEvent::Bound(bound),
        ) if bound.prepared() == &prepared => Ok(AuthorityState::Bound {
            head,
            transition: bound,
        }),
        (
            AuthorityState::Bound {
                head: previous_head,
                transition: previous,
            },
            AuthorityEvent::Prepared(prepared),
        ) => {
            require_restore_successor(previous_head, &previous, &prepared)?;
            Ok(AuthorityState::Prepared {
                head,
                transition: prepared,
            })
        }
        _ => Err(CatalogError::ArtifactRootAuthorityChainInvalid),
    }
}

fn require_first_prepared(
    prepared: &PreparedAuthorityTransition,
    expected_kind: AuthorityTransitionKind,
) -> Result<(), CatalogError> {
    if prepared.kind() == expected_kind && prepared.authority_generation().get() == 1 {
        Ok(())
    } else {
        Err(CatalogError::ArtifactRootAuthorityTransitionConflict)
    }
}

fn require_restore_successor(
    previous_head: AuthorityHead,
    previous: &BoundAuthorityTransition,
    prepared: &PreparedAuthorityTransition,
) -> Result<(), CatalogError> {
    let expected_generation = previous
        .prepared()
        .authority_generation()
        .get()
        .checked_add(1)
        .ok_or(CatalogError::ArtifactRootAuthorityTransitionConflict)?;
    let source_is_exact = prepared.restore_receipt().is_some_and(|receipt| {
        receipt.source_catalog_identity() == previous.prepared().target_catalog_identity()
            && receipt.source_root_identity() == previous.stable_root_identity()
            && receipt.source_authority_generation() == previous.prepared().authority_generation()
            && receipt.source_bound_event() == previous_head.event_digest()
            && receipt.source_authority_evidence() == previous.prepared().evidence_digest()
    });
    if prepared.kind() == AuthorityTransitionKind::BackupRestore
        && prepared.authority_generation().get() == expected_generation
        && source_is_exact
    {
        Ok(())
    } else {
        Err(CatalogError::ArtifactRootAuthorityTransitionConflict)
    }
}

fn validate_active_catalog_endpoint(
    state: &AuthorityState,
    expected: CatalogEndpointIdentity,
) -> Result<(), CatalogError> {
    let observed = match state {
        AuthorityState::InitializationRequired => return Ok(()),
        AuthorityState::LegacyRequired { .. } => None,
        AuthorityState::Prepared { transition, .. } => Some(transition.target_catalog_identity()),
        AuthorityState::Bound { transition, .. } => {
            Some(transition.prepared().target_catalog_identity())
        }
    };
    if observed.is_none_or(|identity| identity == expected) {
        Ok(())
    } else {
        Err(CatalogError::ArtifactRootAuthorityMismatch)
    }
}

fn insert_event(
    connection: &Connection,
    sequence: NonZeroU64,
    previous_event_digest: Option<AuthorityEventDigest>,
    event: AuthorityEvent,
) -> Result<(), CatalogError> {
    let event_digest = event_digest(sequence, previous_event_digest, &event)?;
    let prepared = event.prepared();
    let bound = match &event {
        AuthorityEvent::Bound(bound) => Some(bound),
        AuthorityEvent::LegacyRequired { .. } | AuthorityEvent::Prepared(_) => None,
    };
    let restore = prepared.and_then(PreparedAuthorityTransition::restore_receipt);
    let transition_id = prepared.map(|value| value.transition_id().as_uuid().as_bytes().to_vec());
    let transition_kind = prepared.map(|value| transition_kind_name(value.kind()));
    let authority_generation = prepared
        .map(|value| i64::try_from(value.authority_generation().get()))
        .transpose()
        .map_err(|_| CatalogError::ArtifactRootAuthorityChainInvalid)?;
    let target_root = prepared.map(|value| value.target_root_endpoint_identity().bytes());
    let root_instance = prepared.map(|value| value.root_instance_id().bytes());
    let root_binding_generation = prepared
        .map(|value| i64::try_from(value.root_binding_generation().get()))
        .transpose()
        .map_err(|_| CatalogError::ArtifactRootAuthorityChainInvalid)?;
    let sequence = i64::try_from(sequence.get())
        .map_err(|_| CatalogError::ArtifactRootAuthorityChainInvalid)?;
    connection.execute(
        "INSERT INTO analytical_artifact_root_authority_events(
             sequence, format_version, event_kind, previous_event_digest, event_digest,
             transition_id, transition_kind, authority_generation, target_catalog_identity,
             target_root_endpoint_identity, root_instance_id, evidence_digest,
             restore_source_catalog_identity, restore_source_root_identity,
             restore_source_authority_generation, restore_source_bound_event,
             restore_source_authority_evidence, restore_source_catalog_content_evidence,
             restore_artifact_inventory, restore_backup_version, restore_backup_byte_length,
             restore_backup_sha256, restore_snapshot_at_ns,
             root_binding_generation, root_marker_record_digest, stable_root_identity,
             root_binding_record_digest
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
             ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27
         )",
        params![
            sequence,
            AUTHORITY_EVENT_VERSION,
            event.database_kind(),
            previous_event_digest.map(AuthorityEventDigest::bytes),
            event_digest.bytes(),
            transition_id,
            transition_kind,
            authority_generation,
            event.catalog_identity().bytes(),
            target_root,
            root_instance,
            event.evidence_digest().bytes(),
            restore
                .map(RestoreReceiptFields::source_catalog_identity)
                .map(CatalogEndpointIdentity::bytes),
            restore
                .map(RestoreReceiptFields::source_root_identity)
                .map(StableArtifactRootIdentity::bytes),
            restore
                .map(|value| i64::try_from(value.source_authority_generation().get()))
                .transpose()
                .map_err(|_| CatalogError::ArtifactRootAuthorityChainInvalid)?,
            restore
                .map(RestoreReceiptFields::source_bound_event)
                .map(AuthorityEventDigest::bytes),
            restore
                .map(RestoreReceiptFields::source_authority_evidence)
                .map(AuthorityEvidenceDigest::bytes),
            restore
                .map(RestoreReceiptFields::source_catalog_content_evidence)
                .map(CatalogContentEvidenceDigest::bytes),
            restore
                .map(RestoreReceiptFields::artifact_inventory)
                .map(ArtifactInventoryDigest::bytes),
            restore.map(|value| i64::from(value.catalog_backup().version())),
            restore
                .map(|value| i64::try_from(value.catalog_backup().byte_length()))
                .transpose()
                .map_err(|_| CatalogError::ArtifactRootAuthorityChainInvalid)?,
            restore.map(|value| value.catalog_backup().sha256()),
            restore.map(|value| value.snapshot_at().unix_nanos()),
            root_binding_generation,
            bound.map(|value| value.root_marker_record_digest().bytes()),
            bound.map(|value| value.stable_root_identity().bytes()),
            bound.map(|value| value.root_binding_record_digest().bytes()),
        ],
    )?;
    Ok(())
}

fn event_digest(
    sequence: NonZeroU64,
    previous_event_digest: Option<AuthorityEventDigest>,
    event: &AuthorityEvent,
) -> Result<AuthorityEventDigest, CatalogError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/artifact-root-authority-event/v2");
    digest.update(sequence.get().to_be_bytes());
    match previous_event_digest {
        None => digest.update([0]),
        Some(previous) => {
            digest.update([1]);
            digest.update(previous.bytes());
        }
    }
    digest.update([event.kind_tag()]);
    match event {
        AuthorityEvent::LegacyRequired {
            catalog_identity,
            evidence_digest,
        } => {
            digest.update(catalog_identity.bytes());
            digest.update(evidence_digest.bytes());
        }
        AuthorityEvent::Prepared(prepared) => update_prepared_digest(&mut digest, prepared),
        AuthorityEvent::Bound(bound) => {
            update_prepared_digest(&mut digest, bound.prepared());
            digest.update(bound.root_marker_record_digest().bytes());
            digest.update(bound.stable_root_identity().bytes());
            digest.update(bound.root_binding_record_digest().bytes());
        }
    }
    AuthorityEventDigest::try_new(digest.finalize().into())
        .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)
}

fn update_prepared_digest(digest: &mut Sha256, prepared: &PreparedAuthorityTransition) {
    digest.update(prepared.transition_id().as_uuid().as_bytes());
    digest.update([transition_kind_tag(prepared.kind())]);
    digest.update(prepared.authority_generation().get().to_be_bytes());
    digest.update(prepared.target_catalog_identity().bytes());
    digest.update(prepared.target_root_endpoint_identity().bytes());
    digest.update(prepared.root_instance_id().bytes());
    digest.update(prepared.evidence_digest().bytes());
    match prepared.restore_receipt() {
        None => digest.update([0]),
        Some(restore) => {
            digest.update([1]);
            digest.update(restore.source_catalog_identity().bytes());
            digest.update(restore.source_root_identity().bytes());
            digest.update(restore.source_authority_generation().get().to_be_bytes());
            digest.update(restore.source_bound_event().bytes());
            digest.update(restore.source_authority_evidence().bytes());
            digest.update(restore.source_catalog_content_evidence().bytes());
            digest.update(restore.artifact_inventory().bytes());
            digest.update(restore.catalog_backup().version().to_be_bytes());
            digest.update(restore.catalog_backup().byte_length().to_be_bytes());
            digest.update(restore.catalog_backup().sha256());
            digest.update(restore.snapshot_at().unix_nanos().to_be_bytes());
        }
    }
    digest.update(prepared.root_binding_generation().get().to_be_bytes());
}

fn transition_kind_tag(kind: AuthorityTransitionKind) -> u8 {
    match kind {
        AuthorityTransitionKind::Initialize => 1,
        AuthorityTransitionKind::LegacyMigration => 2,
        AuthorityTransitionKind::BackupRestore => 3,
    }
}

fn transition_kind_name(kind: AuthorityTransitionKind) -> &'static str {
    match kind {
        AuthorityTransitionKind::Initialize => "initialize",
        AuthorityTransitionKind::LegacyMigration => "legacy_migration",
        AuthorityTransitionKind::BackupRestore => "backup_restore",
    }
}

fn legacy_requirement_digest(
    catalog_identity: CatalogEndpointIdentity,
    legacy_schema_version: u64,
) -> Result<AuthorityEvidenceDigest, CatalogError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/legacy-authority-requirement/v2");
    digest.update(catalog_identity.bytes());
    digest.update(legacy_schema_version.to_be_bytes());
    AuthorityEvidenceDigest::try_new(digest.finalize().into())
        .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)
}

fn next_sequence(head: Option<AuthorityHead>) -> Result<NonZeroU64, CatalogError> {
    let value = head.map_or(1, |head| head.sequence().get().saturating_add(1));
    NonZeroU64::new(value).ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)
}

fn nonzero_i64(value: i64) -> Result<NonZeroU64, CatalogError> {
    u64::try_from(value)
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)
}

fn catalog_endpoint_identity(bytes: [u8; 32]) -> Result<CatalogEndpointIdentity, CatalogError> {
    CatalogEndpointIdentity::try_new(bytes).ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)
}

fn opaque_digest<T>(
    value: Vec<u8>,
    constructor: impl FnOnce([u8; 32]) -> Option<T>,
) -> Result<T, CatalogError> {
    constructor(
        value
            .try_into()
            .map_err(|_| CatalogError::ArtifactRootAuthorityChainInvalid)?,
    )
    .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)
}

fn required_opaque_digest<T>(
    value: Option<Vec<u8>>,
    constructor: impl FnOnce([u8; 32]) -> Option<T>,
) -> Result<T, CatalogError> {
    opaque_digest(
        value.ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)?,
        constructor,
    )
}

fn required_array<const SIZE: usize>(value: Option<Vec<u8>>) -> Result<[u8; SIZE], CatalogError> {
    value
        .ok_or(CatalogError::ArtifactRootAuthorityChainInvalid)?
        .try_into()
        .map_err(|_| CatalogError::ArtifactRootAuthorityChainInvalid)
}

#[cfg(any(unix, windows))]
pub(crate) fn exact_catalog_file_binding(
    file: &File,
    path: &Path,
) -> Result<[u8; 32], CatalogError> {
    use cap_fs_ext::MetadataExt as _;

    let metadata = cap_std::fs::File::from_std(file.try_clone()?).metadata()?;
    Ok(hash_catalog_file_identity(
        path,
        metadata.dev(),
        metadata.ino(),
    ))
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn exact_catalog_file_binding(
    _file: &File,
    _path: &Path,
) -> Result<[u8; 32], CatalogError> {
    Err(CatalogError::UnsafePath)
}

fn hash_catalog_file_identity(path: &Path, device: u64, inode: u64) -> [u8; 32] {
    let path = path.as_os_str().as_encoded_bytes();
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/catalog-artifact-root-binding/v2");
    digest.update(u64::try_from(path.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(path);
    digest.update(device.to_be_bytes());
    digest.update(inode.to_be_bytes());
    digest.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::{AuthorityEvent, event_digest};
    use crate::authority_transition::{AuthorityEvidenceDigest, CatalogEndpointIdentity};
    use std::num::NonZeroU64;

    #[test]
    fn event_digest_commits_sequence_predecessor_and_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let event = AuthorityEvent::LegacyRequired {
            catalog_identity: CatalogEndpointIdentity::try_new([1; 32]).ok_or("zero identity")?,
            evidence_digest: AuthorityEvidenceDigest::try_new([2; 32]).ok_or("zero evidence")?,
        };
        let first = event_digest(NonZeroU64::MIN, None, &event)?;
        let repeated = event_digest(NonZeroU64::MIN, None, &event)?;
        let chained = event_digest(
            NonZeroU64::new(2).ok_or("zero sequence")?,
            Some(first),
            &event,
        )?;

        assert_eq!(first, repeated);
        assert_ne!(first, chained);
        Ok(())
    }
}
