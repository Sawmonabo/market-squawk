//! Rights-admitted ingest reservations, recovery reads, and terminal transitions.

use std::fmt;
use std::ops::Deref;

use market_squawk_domain::{CompanyIdentityObservation, SourceId, Timestamp};
use rusqlite::{OptionalExtension as _, Row, Transaction, params};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::RestoreCatalogBaseline;
use super::company_identity::persist_company_identity;
use super::publication::{PublishedIngest, publication_for_run};
use super::storage::{
    AppendOutcome, ResultBudget, append_audit, digest_columns, existing_reservation, parse_digest,
    persist_rights, require_admitted_rights, sha256, trusted_catalog_now,
};
use super::types::{
    Catalog, CatalogConfig, CatalogError, CatalogLimit, ContractCompletion, IngestReservation,
    IngestRunRecord, IngestRunState,
};
use super::{InstalledBackupCatalog, InstalledCatalogState};
use crate::authority_transition::{
    AuthorityEvidenceDigest, AuthorityMutationToken, AuthoritySnapshot, BoundAuthorityTransition,
    CatalogEndpointIdentity, PreparedAuthorityTransition, RootEndpointIdentity,
};
use crate::python_dataset::PythonDatasetCatalogError;
use crate::research_use::ResearchUseCatalogError;
use crate::rights::{RightsBasis, RightsBasisKind, RightsRegistrar, SourceRightsDecision};
use crate::{IngestIdentity, RegisteredRightsGrant, RightsDecisionInput, SourceOperation};

/// Sole composition-owned authority for one open catalog writer session.
pub struct CatalogAuthority {
    catalog: Catalog,
    rights_registrar: RightsRegistrar,
}

impl CatalogAuthority {
    /// Opens the exclusive durable catalog writer and its non-duplicable rights registrar.
    pub fn open(config: CatalogConfig) -> Result<Self, CatalogError> {
        let catalog = Catalog::open(config)?;
        Ok(Self::from_catalog(catalog))
    }

    pub(crate) fn open_installed(
        config: CatalogConfig,
        installed: InstalledBackupCatalog,
    ) -> Result<(Self, InstalledCatalogState), CatalogError> {
        let (catalog, state) = Catalog::open_installed(config, installed)?;
        Ok((Self::from_catalog(catalog), state))
    }

    fn from_catalog(catalog: Catalog) -> Self {
        let rights_registrar = RightsRegistrar {
            catalog_id: catalog.catalog_id,
        };
        Self {
            catalog,
            rights_registrar,
        }
    }

    /// Returns the catalog service edge while retaining the private registrar authority.
    pub const fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub(crate) const fn session_id(&self) -> Uuid {
        self.catalog.catalog_id
    }

    /// Runs one ResearchUse authority mutation against a single trusted-time transaction.
    ///
    /// This is deliberately narrower than exposing the catalog connection: ResearchUse owns the
    /// callback, the catalog owns transaction construction, the rollback-latched clock, and commit.
    pub(crate) fn with_research_use_transaction<T, F>(
        &self,
        operation: F,
    ) -> Result<T, ResearchUseCatalogError>
    where
        F: for<'transaction> FnOnce(
            &Transaction<'transaction>,
            Uuid,
            Timestamp,
        ) -> Result<T, ResearchUseCatalogError>,
    {
        let transaction = self.catalog.connection.unchecked_transaction()?;
        let now = trusted_catalog_now(&transaction)?;
        let result = operation(&transaction, self.catalog.catalog_id, now)?;
        transaction.commit()?;
        Ok(result)
    }

    /// Runs one Python-dataset admission mutation in a trusted-time transaction.
    pub(crate) fn with_python_dataset_transaction<T, F>(
        &self,
        operation: F,
    ) -> Result<T, PythonDatasetCatalogError>
    where
        F: for<'transaction> FnOnce(
            &Transaction<'transaction>,
            Timestamp,
        ) -> Result<T, PythonDatasetCatalogError>,
    {
        let transaction = self.catalog.connection.unchecked_transaction()?;
        let now = trusted_catalog_now(&transaction)?;
        let result = operation(&transaction, now)?;
        transaction.commit()?;
        Ok(result)
    }

    pub(crate) fn validate_ingest_reservation(
        &self,
        reservation: &IngestReservation,
    ) -> Result<(), CatalogError> {
        if reservation.catalog_id == self.catalog.catalog_id {
            Ok(())
        } else {
            Err(CatalogError::InvalidReservationCapability)
        }
    }

    pub(crate) const fn artifact_root_binding(&self) -> [u8; 32] {
        self.catalog.artifact_root_binding
    }

    pub(crate) fn catalog_endpoint_identity(
        &self,
    ) -> Result<CatalogEndpointIdentity, CatalogError> {
        self.catalog.catalog_endpoint_identity()
    }

    pub(crate) fn initialization_evidence_digest(
        &self,
        root_endpoint: RootEndpointIdentity,
    ) -> Result<AuthorityEvidenceDigest, CatalogError> {
        self.catalog.initialization_evidence_digest(root_endpoint)
    }

    pub(crate) fn authority_snapshot(&self) -> Result<AuthoritySnapshot, CatalogError> {
        self.catalog.authority_snapshot()
    }

    pub(crate) fn authority_snapshot_without_endpoint(
        &self,
    ) -> Result<AuthoritySnapshot, CatalogError> {
        self.catalog.authority_snapshot_without_endpoint()
    }

    pub(crate) fn append_prepared_authority(
        &mut self,
        token: &AuthorityMutationToken,
        transition: PreparedAuthorityTransition,
    ) -> Result<AuthoritySnapshot, CatalogError> {
        self.catalog.append_prepared_authority(token, transition)
    }

    pub(crate) fn append_bound_authority(
        &mut self,
        token: &AuthorityMutationToken,
        transition: BoundAuthorityTransition,
    ) -> Result<AuthoritySnapshot, CatalogError> {
        self.catalog.append_bound_authority(token, transition)
    }

    pub(crate) fn checkpoint_restore_state(&self) -> Result<super::BackupReceipt, CatalogError> {
        self.catalog.checkpoint_restore_state()
    }

    pub(crate) fn acquire_restore_exclusive_locking(&self) -> Result<(), CatalogError> {
        self.catalog.acquire_restore_exclusive_locking()
    }

    pub(crate) fn release_restore_exclusive_locking(&self) -> Result<(), CatalogError> {
        self.catalog.release_restore_exclusive_locking()
    }

    pub(crate) fn revalidate_restore_state(
        &self,
        expected: super::BackupReceipt,
    ) -> Result<(), CatalogError> {
        self.catalog.revalidate_restore_state(expected)
    }

    pub(crate) fn verify_restore_baseline(
        &self,
        expected: RestoreCatalogBaseline,
        cancellation: &CancellationToken,
    ) -> Result<(), CatalogError> {
        self.catalog.verify_restore_baseline(expected, cancellation)
    }

    /// Validates and durably admits rights evidence through the composition authority.
    pub fn admit_source_rights(
        &self,
        input: RightsDecisionInput,
    ) -> Result<RegisteredRightsGrant, CatalogError> {
        self.catalog
            .admit_source_rights(&self.rights_registrar, input)
    }
}

impl Deref for CatalogAuthority {
    type Target = Catalog;

    fn deref(&self) -> &Self::Target {
        &self.catalog
    }
}

impl fmt::Debug for CatalogAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogAuthority")
            .field("catalog", &self.catalog)
            .field("rights_registrar", &"[SEALED COMPOSITION AUTHORITY]")
            .finish()
    }
}

/// A restart-validated reservation sealed to the current writer session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResumedIngest {
    reservation: IngestReservation,
    publication: Option<PublishedIngest>,
}

impl ResumedIngest {
    /// Returns the freshly sealed reservation required for further transitions.
    pub const fn reservation(&self) -> &IngestReservation {
        &self.reservation
    }

    /// Returns an already committed publication when the prior process published before exit.
    pub const fn publication(&self) -> Option<&PublishedIngest> {
        self.publication.as_ref()
    }
}

impl Catalog {
    fn admit_source_rights(
        &self,
        registrar: &RightsRegistrar,
        input: RightsDecisionInput,
    ) -> Result<RegisteredRightsGrant, CatalogError> {
        if registrar.catalog_id != self.catalog_id {
            return Err(CatalogError::InvalidRightsCapability);
        }
        let rights = SourceRightsDecision::try_new(input)?;
        let transaction = self.connection.unchecked_transaction()?;
        let admitted_at = trusted_catalog_now(&transaction)?;
        rights.validate_at(admitted_at)?;
        let source_exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM sources WHERE source_id=?1)",
            [rights.source_id().as_str()],
            |row| row.get(0),
        )?;
        if !source_exists {
            return Err(CatalogError::UnknownSource);
        }
        if persist_rights(&transaction, &rights, admitted_at)? == AppendOutcome::Inserted {
            append_audit(
                &transaction,
                "source-rights.admitted",
                rights.source_id().as_str(),
                rights.fingerprint(),
                admitted_at,
            )?;
            transaction.commit()?;
        }
        Ok(RegisteredRightsGrant {
            catalog_id: self.catalog_id,
            rights_id: rights.fingerprint(),
        })
    }

    /// Reserves an ingest only after operation-specific rights admission.
    pub fn reserve_ingest(
        &self,
        request: &IngestIdentity,
        grant: &RegisteredRightsGrant,
    ) -> Result<IngestReservation, CatalogError> {
        if grant.catalog_id != self.catalog_id {
            return Err(CatalogError::InvalidRightsCapability);
        }
        let transaction = self.connection.unchecked_transaction()?;
        let requested_at = trusted_catalog_now(&transaction)?;
        let mut budget = ResultBudget::new(self.result_bytes);
        let rights = load_admitted_rights(&transaction, grant.rights_id, &mut budget)?;
        rights.authorize_at(request, requested_at)?;
        require_admitted_rights(&transaction, &rights)?;
        if let Some(existing) = existing_reservation(&transaction, request, self.catalog_id)? {
            if existing.matches(request) {
                return Ok(existing.reservation);
            }
            return Err(CatalogError::IdempotencyConflict);
        }
        let reservation = IngestReservation {
            run_id: Uuid::new_v4(),
            requested_at,
            catalog_id: self.catalog_id,
        };
        let (payload_algorithm, payload_digest) = digest_columns(request.payload_digest());
        transaction.execute(
            "INSERT INTO ingest_runs
             (run_id, idempotency_key, source_id, payload_algorithm, payload_digest,
              operation, rights_id, state, requested_at_ns, completed_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'reserved', ?8, NULL)",
            params![
                reservation.run_id.to_string(),
                request.idempotency_key(),
                request.source_id().as_str(),
                payload_algorithm,
                payload_digest,
                request.operation().database_name(),
                grant.rights_id,
                requested_at.unix_nanos()
            ],
        )?;
        append_audit(
            &transaction,
            "ingest.reserved",
            &reservation.run_id.to_string(),
            grant.rights_id,
            requested_at,
        )?;
        transaction.commit()?;
        Ok(reservation)
    }

    /// Records a terminal outcome; no adapter can rewrite a prior terminal state.
    pub fn complete_ingest(
        &self,
        reservation: &IngestReservation,
        completion: ContractCompletion,
    ) -> Result<(), CatalogError> {
        self.complete_ingest_with_company_identity(reservation, completion, None)
    }

    pub(crate) fn complete_ingest_with_company_identity(
        &self,
        reservation: &IngestReservation,
        completion: ContractCompletion,
        company_identity: Option<&CompanyIdentityObservation>,
    ) -> Result<(), CatalogError> {
        if reservation.catalog_id != self.catalog_id {
            return Err(CatalogError::InvalidReservationCapability);
        }
        if company_identity.is_some() && completion != ContractCompletion::Succeeded {
            return Err(CatalogError::RunStateConflict);
        }
        let transaction = self.connection.unchecked_transaction()?;
        let completed_at = trusted_catalog_now(&transaction)?;
        let operation: Option<String> = transaction
            .query_row(
                "SELECT operation FROM ingest_runs WHERE run_id=?1 AND state='reserved'",
                [reservation.run_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let operation = operation.ok_or(CatalogError::RunStateConflict)?;
        if completion == ContractCompletion::Succeeded
            && matches!(operation.as_str(), "persist" | "cache")
        {
            let has_manifest: bool = transaction.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM artifacts
                     JOIN dataset_manifests USING (artifact_id)
                     WHERE artifacts.run_id=?1
                 )",
                [reservation.run_id.to_string()],
                |row| row.get(0),
            )?;
            if !has_manifest {
                return Err(CatalogError::RunStateConflict);
            }
        }
        if let Some(company_identity) = company_identity {
            persist_company_identity(&transaction, reservation, company_identity, completed_at)?;
        }
        let changed = transaction.execute(
            "UPDATE ingest_runs SET state=?1, completed_at_ns=?2
             WHERE run_id=?3 AND state='reserved'",
            params![
                completion.database_name(),
                completed_at.unix_nanos(),
                reservation.run_id.to_string()
            ],
        )?;
        if changed != 1 {
            return Err(CatalogError::RunStateConflict);
        }
        append_audit(
            &transaction,
            "ingest.completed",
            &reservation.run_id.to_string(),
            sha256(completion.database_name().as_bytes()),
            completed_at,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn reconcile_company_identity(
        &self,
        reservation: &IngestReservation,
        company_identity: &CompanyIdentityObservation,
    ) -> Result<(), CatalogError> {
        if reservation.catalog_id != self.catalog_id {
            return Err(CatalogError::InvalidReservationCapability);
        }
        let transaction = self.connection.unchecked_transaction()?;
        let reconciled_at = trusted_catalog_now(&transaction)?;
        persist_company_identity(&transaction, reservation, company_identity, reconciled_at)?;
        transaction.commit()?;
        Ok(())
    }

    /// Revalidates a durable reservation and seals it to this restarted writer session.
    pub fn resume_ingest(&self, run_id: Uuid) -> Result<ResumedIngest, CatalogError> {
        let transaction = self.connection.unchecked_transaction()?;
        let mut budget = ResultBudget::new(self.result_bytes);
        let stored = transaction
            .query_row(
                "SELECT runs.idempotency_key, runs.source_id, runs.payload_algorithm,
                        runs.payload_digest, runs.operation, runs.rights_id, runs.state,
                        runs.requested_at_ns, runs.completed_at_ns,
                        rights.source_id, rights.payload_algorithm, rights.payload_digest,
                        rights.retrieved_at_ns, rights.basis_reference, rights.basis_algorithm,
                        rights.basis_digest, rights.authorization_algorithm,
                        rights.authorization_digest, rights.authorization_expires_at_ns,
                        rights.operation_mask, rights.admitted_at_ns, rights.basis_kind,
                        rights.basis_root_algorithm, rights.basis_root_digest,
                        rights.fingerprint_version
                 FROM ingest_runs AS runs
                 JOIN source_rights AS rights USING (rights_id)
                 WHERE runs.run_id=?1",
                [run_id.to_string()],
                |row| read_stored_run(row, 0),
            )
            .optional()?
            .ok_or(CatalogError::RunStateConflict)?;
        charge_stored_run(&stored, &mut budget)?;
        let run = decode_run(run_id, stored)?;
        if run.state != IngestRunState::Reserved {
            return Err(CatalogError::RunStateConflict);
        }
        let rights = load_admitted_rights(&transaction, run.rights_id, &mut budget)?;
        require_admitted_rights(&transaction, &rights)?;
        let publication = publication_for_run(&transaction, run_id, &mut budget)?;
        let reservation = IngestReservation {
            run_id,
            requested_at: run.requested_at,
            catalog_id: self.catalog_id,
        };
        transaction.commit()?;
        Ok(ResumedIngest {
            reservation,
            publication,
        })
    }

    /// Loads one durable run for reconciliation.
    pub fn ingest_run(&self, run_id: Uuid) -> Result<Option<IngestRunRecord>, CatalogError> {
        let mut budget = ResultBudget::new(self.result_bytes);
        let stored = self
            .connection
            .query_row(
                "SELECT runs.idempotency_key, runs.source_id, runs.payload_algorithm,
                        runs.payload_digest, runs.operation, runs.rights_id, runs.state,
                        runs.requested_at_ns, runs.completed_at_ns,
                        rights.source_id, rights.payload_algorithm, rights.payload_digest,
                        rights.retrieved_at_ns, rights.basis_reference, rights.basis_algorithm,
                        rights.basis_digest, rights.authorization_algorithm,
                        rights.authorization_digest, rights.authorization_expires_at_ns,
                        rights.operation_mask, rights.admitted_at_ns, rights.basis_kind,
                        rights.basis_root_algorithm, rights.basis_root_digest,
                        rights.fingerprint_version
                 FROM ingest_runs AS runs
                 JOIN source_rights AS rights USING (rights_id)
                 WHERE runs.run_id=?1",
                [run_id.to_string()],
                |row| read_stored_run(row, 0),
            )
            .optional()?;
        stored
            .map(|stored| {
                charge_stored_run(&stored, &mut budget)?;
                decode_run(run_id, stored)
            })
            .transpose()
    }

    /// Returns oldest-first reservations that require recovery, within one global result bound.
    pub fn active_ingest_runs(
        &self,
        limit: CatalogLimit,
    ) -> Result<Vec<IngestRunRecord>, CatalogError> {
        self.enforce_limit(limit)?;
        let mut budget = ResultBudget::new(self.result_bytes);
        let row_limit = i64::try_from(limit.get()).map_err(|_| CatalogError::InvalidLimit)?;
        let mut statement = self.connection.prepare(
            "SELECT runs.run_id, runs.idempotency_key, runs.source_id, runs.payload_algorithm,
                    runs.payload_digest, runs.operation, runs.rights_id, runs.state,
                    runs.requested_at_ns, runs.completed_at_ns,
                    rights.source_id, rights.payload_algorithm, rights.payload_digest,
                    rights.retrieved_at_ns, rights.basis_reference, rights.basis_algorithm,
                    rights.basis_digest, rights.authorization_algorithm,
                    rights.authorization_digest, rights.authorization_expires_at_ns,
                    rights.operation_mask, rights.admitted_at_ns, rights.basis_kind,
                    rights.basis_root_algorithm, rights.basis_root_digest,
                    rights.fingerprint_version
             FROM ingest_runs AS runs
             JOIN source_rights AS rights USING (rights_id)
             WHERE runs.state='reserved'
             ORDER BY runs.requested_at_ns, runs.run_id LIMIT ?1",
        )?;
        let rows = statement.query_map([row_limit], |row| {
            let run_id = row.get::<_, String>(0)?;
            Ok((run_id, read_stored_run(row, 1)?))
        })?;
        let mut runs = Vec::new();
        runs.try_reserve_exact(budget.bounded_row_capacity(limit.get()))
            .map_err(|_| CatalogError::Allocation)?;
        for row in rows {
            let (run_id, stored) = row?;
            charge_stored_run(&stored, &mut budget)?;
            let run_id = Uuid::parse_str(&run_id).map_err(|_| CatalogError::CorruptCatalog)?;
            runs.push(decode_run(run_id, stored)?);
        }
        Ok(runs)
    }
}

struct StoredRun {
    idempotency_key: String,
    source_id: String,
    payload_algorithm: i64,
    payload_digest: Vec<u8>,
    operation: String,
    rights_id: Vec<u8>,
    state: String,
    requested_at_ns: i64,
    completed_at_ns: Option<i64>,
    rights: StoredRights,
}

struct StoredRights {
    source_id: String,
    payload_algorithm: i64,
    payload_digest: Vec<u8>,
    retrieved_at_ns: i64,
    basis_reference: String,
    basis_algorithm: i64,
    basis_digest: Vec<u8>,
    authorization_algorithm: i64,
    authorization_digest: Vec<u8>,
    authorization_expires_at_ns: Option<i64>,
    operation_mask: i64,
    admitted_at_ns: i64,
    basis_kind: String,
    basis_root_algorithm: Option<i64>,
    basis_root_digest: Option<Vec<u8>>,
    fingerprint_version: i64,
}

fn read_stored_run(row: &Row<'_>, offset: usize) -> rusqlite::Result<StoredRun> {
    Ok(StoredRun {
        idempotency_key: row.get(offset)?,
        source_id: row.get(offset + 1)?,
        payload_algorithm: row.get(offset + 2)?,
        payload_digest: row.get(offset + 3)?,
        operation: row.get(offset + 4)?,
        rights_id: row.get(offset + 5)?,
        state: row.get(offset + 6)?,
        requested_at_ns: row.get(offset + 7)?,
        completed_at_ns: row.get(offset + 8)?,
        rights: read_stored_rights(row, offset + 9)?,
    })
}

fn read_stored_rights(row: &Row<'_>, offset: usize) -> rusqlite::Result<StoredRights> {
    Ok(StoredRights {
        source_id: row.get(offset)?,
        payload_algorithm: row.get(offset + 1)?,
        payload_digest: row.get(offset + 2)?,
        retrieved_at_ns: row.get(offset + 3)?,
        basis_reference: row.get(offset + 4)?,
        basis_algorithm: row.get(offset + 5)?,
        basis_digest: row.get(offset + 6)?,
        authorization_algorithm: row.get(offset + 7)?,
        authorization_digest: row.get(offset + 8)?,
        authorization_expires_at_ns: row.get(offset + 9)?,
        operation_mask: row.get(offset + 10)?,
        admitted_at_ns: row.get(offset + 11)?,
        basis_kind: row.get(offset + 12)?,
        basis_root_algorithm: row.get(offset + 13)?,
        basis_root_digest: row.get(offset + 14)?,
        fingerprint_version: row.get(offset + 15)?,
    })
}

fn load_admitted_rights(
    transaction: &Transaction<'_>,
    rights_id: [u8; 32],
    budget: &mut ResultBudget,
) -> Result<SourceRightsDecision, CatalogError> {
    let stored = transaction
        .query_row(
            "SELECT source_id, payload_algorithm, payload_digest, retrieved_at_ns,
                    basis_reference, basis_algorithm, basis_digest, authorization_algorithm,
                    authorization_digest, authorization_expires_at_ns, operation_mask,
                    admitted_at_ns, basis_kind, basis_root_algorithm, basis_root_digest,
                    fingerprint_version
             FROM source_rights WHERE rights_id=?1",
            [rights_id],
            |row| read_stored_rights(row, 0),
        )
        .optional()?
        .ok_or(CatalogError::RightsNotAdmitted)?;
    charge_stored_rights(&stored, budget)?;
    let rights = decode_rights(stored)?;
    if rights.fingerprint() != rights_id {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(rights)
}

fn charge_stored_run(stored: &StoredRun, budget: &mut ResultBudget) -> Result<(), CatalogError> {
    budget.charge([
        stored.idempotency_key.len(),
        stored.source_id.len(),
        stored.payload_digest.len(),
        stored.operation.len(),
        stored.rights_id.len(),
        stored.state.len(),
    ])?;
    charge_stored_rights(&stored.rights, budget)
}

fn charge_stored_rights(
    stored: &StoredRights,
    budget: &mut ResultBudget,
) -> Result<(), CatalogError> {
    budget.charge([
        stored.source_id.len(),
        stored.payload_digest.len(),
        stored.basis_reference.len(),
        stored.basis_digest.len(),
        stored.authorization_digest.len(),
        stored.basis_kind.len(),
        stored.basis_root_digest.as_ref().map_or(0, Vec::len),
    ])
}

fn decode_run(run_id: Uuid, stored: StoredRun) -> Result<IngestRunRecord, CatalogError> {
    let source_id =
        SourceId::try_from(stored.source_id.as_str()).map_err(|_| CatalogError::CorruptCatalog)?;
    let payload_digest = parse_digest(stored.payload_algorithm, &stored.payload_digest)?;
    let operation = SourceOperation::from_database_name(&stored.operation)
        .ok_or(CatalogError::CorruptCatalog)?;
    let rights_id: [u8; 32] = stored
        .rights_id
        .try_into()
        .map_err(|_| CatalogError::CorruptCatalog)?;
    let requested_at = Timestamp::from_unix_nanos(stored.requested_at_ns);
    let identity = IngestIdentity::try_new(
        source_id.clone(),
        payload_digest,
        operation,
        stored.idempotency_key.clone(),
    )
    .map_err(|_| CatalogError::CorruptCatalog)?;
    let admitted_at = Timestamp::from_unix_nanos(stored.rights.admitted_at_ns);
    let rights = decode_rights(stored.rights)?;
    if admitted_at > requested_at
        || rights.fingerprint() != rights_id
        || rights.authorize_at(&identity, requested_at).is_err()
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let completed_at = stored.completed_at_ns.map(Timestamp::from_unix_nanos);
    let state = match (stored.state.as_str(), completed_at) {
        ("reserved", None) => IngestRunState::Reserved,
        ("succeeded", Some(completed_at)) if completed_at >= requested_at => {
            IngestRunState::Succeeded
        }
        ("failed", Some(completed_at)) if completed_at >= requested_at => IngestRunState::Failed,
        _ => return Err(CatalogError::CorruptCatalog),
    };
    Ok(IngestRunRecord {
        run_id,
        idempotency_key: stored.idempotency_key,
        source_id,
        payload_digest,
        operation,
        rights_id,
        state,
        requested_at,
        completed_at,
    })
}

fn decode_rights(stored: StoredRights) -> Result<SourceRightsDecision, CatalogError> {
    let admitted_at = Timestamp::from_unix_nanos(stored.admitted_at_ns);
    let source_id =
        SourceId::try_from(stored.source_id.as_str()).map_err(|_| CatalogError::CorruptCatalog)?;
    let payload_digest = parse_digest(stored.payload_algorithm, &stored.payload_digest)?;
    let basis_digest = parse_digest(stored.basis_algorithm, &stored.basis_digest)?;
    let basis_root_digest = match (
        stored.basis_root_algorithm,
        stored.basis_root_digest.as_deref(),
    ) {
        (Some(algorithm), Some(digest)) => Some(parse_digest(algorithm, digest)?),
        (None, None) => None,
        _ => return Err(CatalogError::CorruptCatalog),
    };
    let authorization_evidence =
        parse_digest(stored.authorization_algorithm, &stored.authorization_digest)?;
    let operation_mask = u8::try_from(stored.operation_mask)
        .ok()
        .filter(|mask| (1..=63).contains(mask))
        .ok_or(CatalogError::CorruptCatalog)?;
    let operations = [
        SourceOperation::Retrieve,
        SourceOperation::Display,
        SourceOperation::Persist,
        SourceOperation::Cache,
        SourceOperation::Redistribute,
        SourceOperation::Train,
    ]
    .into_iter()
    .filter(|operation| operation_mask & operation.mask() != 0)
    .collect();
    let basis_kind = RightsBasisKind::from_database_name(&stored.basis_kind)
        .ok_or(CatalogError::CorruptCatalog)?;
    let basis = RightsBasis::from_stored(
        basis_kind,
        stored.basis_reference,
        basis_digest,
        basis_root_digest,
    )
    .map_err(|_| CatalogError::CorruptCatalog)?;
    let input = RightsDecisionInput {
        source_id,
        payload_digest,
        retrieved_at: Timestamp::from_unix_nanos(stored.retrieved_at_ns),
        basis,
        authorization_evidence,
        authorization_expires_at: stored
            .authorization_expires_at_ns
            .map(Timestamp::from_unix_nanos),
        permitted_operations: operations,
    };
    let rights = match stored.fingerprint_version {
        1 => SourceRightsDecision::try_new_legacy(input),
        2 => SourceRightsDecision::try_new(input),
        _ => return Err(CatalogError::CorruptCatalog),
    }
    .map_err(|_| CatalogError::CorruptCatalog)?;
    rights
        .validate_at(admitted_at)
        .map_err(|_| CatalogError::CorruptCatalog)?;
    Ok(rights)
}
