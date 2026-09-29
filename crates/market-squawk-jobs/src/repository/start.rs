//! Original start-request reservations in the sole job database and writer transaction.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use rusqlite::{Connection, OptionalExtension as _, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::codec::{StoredRequestId, decode_snapshot};
use super::engine::map_sql;
use crate::{
    AdmittedJobSpec, JobGeneration, JobId, JobOrigin, JobRepositoryError, JobStartAdmission,
    JobStartBinding, JobStartPermit, JobStartReconciliation, JobStartState,
};

pub(super) const MAXIMUM_START_REQUESTS: usize = 65_536;
const PENDING: i64 = 0;
const NOT_ADMITTED: i64 = 1;
const ADMITTED: i64 = 2;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct StoredStartRequest {
    workspace: SourceIdentifier,
    client: SourceIdentifier,
    request_id: StoredRequestId,
    operation: SourceIdentifier,
    arguments_digest: EvidenceDigest,
    job_id: Uuid,
    admitted_spec_sha256: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct StartRequestRecord {
    state: i64,
    request: StoredStartRequest,
}

impl StartRequestRecord {
    fn binding(&self) -> Result<JobStartBinding, JobRepositoryError> {
        if self.request.arguments_digest.algorithm() != DigestAlgorithm::Sha256
            || !matches!(self.state, PENDING | NOT_ADMITTED | ADMITTED)
            || (self.state == ADMITTED) != self.request.admitted_spec_sha256.is_some()
        {
            return Err(JobRepositoryError::InvalidState);
        }
        JobId::try_from_uuid(self.request.job_id).map_err(|_| JobRepositoryError::InvalidState)?;
        Ok(JobStartBinding::new(
            JobOrigin::new(self.request.workspace.clone(), self.request.client.clone()),
            self.request.request_id.clone().into_request_id()?,
            self.request.operation.clone(),
            self.request.arguments_digest,
        ))
    }

    pub(super) fn verify_spec(
        &self,
        spec: Option<&AdmittedJobSpec>,
    ) -> Result<(), JobRepositoryError> {
        let binding = self.binding()?;
        match (self.state, spec) {
            (ADMITTED, Some(spec))
                if spec.id().as_uuid() == self.request.job_id
                    && spec.generation().get() == 1
                    && spec.origin() == binding.origin()
                    && spec.request_id() == binding.request_id()
                    && Some(spec_digest(spec)?) == self.request.admitted_spec_sha256 =>
            {
                Ok(())
            }
            (PENDING | NOT_ADMITTED, None) => Ok(()),
            _ => Err(JobRepositoryError::InvalidState),
        }
    }

    pub(super) const fn job_id(&self) -> Uuid {
        self.request.job_id
    }

    pub(super) fn key(&self) -> Result<(String, String, Vec<u8>), JobRepositoryError> {
        let binding = self.binding()?;
        Ok((
            self.request.workspace.as_str().to_owned(),
            self.request.client.as_str().to_owned(),
            binding
                .request_id()
                .canonical_bytes()
                .map_err(|_| JobRepositoryError::InvalidState)?,
        ))
    }
}

fn spec_digest(spec: &AdmittedJobSpec) -> Result<[u8; 32], JobRepositoryError> {
    let bytes = serde_json::to_vec(spec).map_err(|_| JobRepositoryError::InvalidState)?;
    Ok(Sha256::digest(bytes).into())
}

fn new_record(binding: &JobStartBinding, state: i64) -> StartRequestRecord {
    StartRequestRecord {
        state,
        request: StoredStartRequest {
            workspace: binding.origin().workspace().clone(),
            client: binding.origin().client().clone(),
            request_id: StoredRequestId::from(binding.request_id()),
            operation: binding.operation().clone(),
            arguments_digest: binding.arguments_digest(),
            job_id: Uuid::new_v4(),
            admitted_spec_sha256: None,
        },
    }
}

fn decode_record(state: i64, bytes: &[u8]) -> Result<StartRequestRecord, JobRepositoryError> {
    let request = serde_json::from_slice(bytes).map_err(|_| JobRepositoryError::InvalidState)?;
    let record = StartRequestRecord { state, request };
    record.binding()?;
    Ok(record)
}

fn read_record(
    connection: &Connection,
    binding: &JobStartBinding,
) -> Result<Option<StartRequestRecord>, JobRepositoryError> {
    let key = binding
        .request_id()
        .canonical_bytes()
        .map_err(|_| JobRepositoryError::InvalidState)?;
    let row = connection.query_row(
        "SELECT state, request_json, job_id FROM job_start_requests WHERE workspace = ?1 AND client = ?2 AND request_id = ?3",
        params![binding.origin().workspace().as_str(), binding.origin().client().as_str(), key],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, Vec<u8>>(2)?)),
    ).optional().map_err(map_sql)?;
    let record = row
        .map(|(state, bytes, id)| {
            let record = decode_record(state, &bytes)?;
            if record.job_id().as_bytes().as_slice() != id {
                return Err(JobRepositoryError::InvalidState);
            }
            Ok(record)
        })
        .transpose()?;
    if let Some(record) = &record
        && record.binding()? != *binding
    {
        return Err(JobRepositoryError::Conflict);
    }
    Ok(record)
}

pub(super) fn insert_record(
    connection: &Connection,
    record: &StartRequestRecord,
) -> Result<(), JobRepositoryError> {
    let (workspace, client, request_id) = record.key()?;
    let bytes =
        serde_json::to_vec(&record.request).map_err(|_| JobRepositoryError::InvalidState)?;
    connection.execute(
        "INSERT INTO job_start_requests (workspace, client, request_id, job_id, state, request_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![workspace, client, request_id, record.request.job_id.as_bytes().as_slice(), record.state, bytes],
    ).map_err(map_sql)?;
    Ok(())
}

fn check_capacity(connection: &Connection) -> Result<(), JobRepositoryError> {
    let count: i64 = connection
        .query_row("SELECT COUNT(*) FROM job_start_requests", [], |row| {
            row.get(0)
        })
        .map_err(map_sql)?;
    if usize::try_from(count).map_err(|_| JobRepositoryError::InvalidState)?
        >= MAXIMUM_START_REQUESTS
    {
        return Err(JobRepositoryError::Unavailable);
    }
    Ok(())
}

pub(super) fn begin(
    connection: &Connection,
    binding: &JobStartBinding,
) -> Result<JobStartAdmission, JobRepositoryError> {
    let transaction = connection.unchecked_transaction().map_err(map_sql)?;
    if let Some(record) = read_record(&transaction, binding)? {
        return Ok(JobStartAdmission::Existing(reconcile_record(
            &transaction,
            &record,
        )?));
    }
    check_capacity(&transaction)?;
    let record = new_record(binding, PENDING);
    insert_record(&transaction, &record)?;
    transaction.commit().map_err(map_sql)?;
    Ok(JobStartAdmission::Execute(JobStartPermit {
        id: JobId::try_from_uuid(record.request.job_id)
            .map_err(|_| JobRepositoryError::InvalidState)?,
        binding: binding.clone(),
    }))
}

pub(super) fn reconcile(
    connection: &Connection,
    binding: &JobStartBinding,
) -> Result<JobStartReconciliation, JobRepositoryError> {
    let transaction = connection.unchecked_transaction().map_err(map_sql)?;
    read_record(&transaction, binding)?.map_or_else(
        || {
            Ok(JobStartReconciliation {
                state: JobStartState::Unknown,
                snapshot: None,
            })
        },
        |record| reconcile_record(&transaction, &record),
    )
}

pub(super) fn cancel(
    connection: &Connection,
    binding: &JobStartBinding,
) -> Result<JobStartReconciliation, JobRepositoryError> {
    let transaction = connection.unchecked_transaction().map_err(map_sql)?;
    let record = match read_record(&transaction, binding)? {
        Some(mut record) => {
            if record.state == PENDING {
                transaction
                    .execute(
                        "UPDATE job_start_requests SET state = ?1 WHERE job_id = ?2 AND state = ?3",
                        params![
                            NOT_ADMITTED,
                            record.request.job_id.as_bytes().as_slice(),
                            PENDING
                        ],
                    )
                    .map_err(map_sql)?;
                record.state = NOT_ADMITTED;
            }
            record
        }
        None => {
            check_capacity(&transaction)?;
            let record = new_record(binding, NOT_ADMITTED);
            insert_record(&transaction, &record)?;
            record
        }
    };
    let result = reconcile_record(&transaction, &record)?;
    transaction.commit().map_err(map_sql)?;
    Ok(result)
}

fn reconcile_record(
    connection: &Connection,
    record: &StartRequestRecord,
) -> Result<JobStartReconciliation, JobRepositoryError> {
    let state = match record.state {
        PENDING => JobStartState::Pending,
        NOT_ADMITTED => JobStartState::NotAdmitted,
        ADMITTED => JobStartState::Admitted,
        _ => return Err(JobRepositoryError::InvalidState),
    };
    let snapshot =
        if state == JobStartState::Admitted {
            let id = JobId::try_from_uuid(record.request.job_id)
                .map_err(|_| JobRepositoryError::InvalidState)?;
            let first = super::engine::read_snapshot(
                connection,
                id,
                JobGeneration::try_new(1).map_err(|_| JobRepositoryError::InvalidState)?,
            )?;
            record.verify_spec(Some(first.spec()))?;
            let bytes: Vec<u8> = connection.query_row(
            "SELECT snapshot_json FROM jobs WHERE job_id = ?1 ORDER BY generation DESC LIMIT 1",
            [id.as_uuid().as_bytes().as_slice()], |row| row.get(0),
        ).map_err(map_sql)?;
            let latest = decode_snapshot(&bytes)?;
            if latest.id() != first.id()
                || latest.spec().origin() != first.spec().origin()
                || latest.spec().request_id() != first.spec().request_id()
                || latest.spec().input() != first.spec().input()
                || latest.spec().kind() != first.spec().kind()
                || latest.spec().authority() != first.spec().authority()
                || latest.spec().attempt_limit() != first.spec().attempt_limit()
            {
                return Err(JobRepositoryError::InvalidState);
            }
            Some(latest)
        } else {
            record.verify_spec(None)?;
            let exists: bool = connection
                .query_row(
                    "SELECT EXISTS (SELECT 1 FROM jobs WHERE job_id = ?1)",
                    [record.job_id().as_bytes().as_slice()],
                    |row| row.get(0),
                )
                .map_err(map_sql)?;
            if exists {
                return Err(JobRepositoryError::InvalidState);
            }
            None
        };
    Ok(JobStartReconciliation { state, snapshot })
}

/// Called inside the same transaction that inserts the first immutable job snapshot.
pub(super) fn admit(
    connection: &Connection,
    spec: &AdmittedJobSpec,
) -> Result<(), JobRepositoryError> {
    let row = connection
        .query_row(
            "SELECT state, request_json FROM job_start_requests WHERE job_id = ?1",
            [spec.id().as_uuid().as_bytes().as_slice()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
        .map_err(map_sql)?;
    let Some((state, bytes)) = row else {
        return Ok(());
    };
    let mut record = decode_record(state, &bytes)?;
    let binding = record.binding()?;
    if record.request.job_id != spec.id().as_uuid()
        || !read_record(connection, &binding)?
            .is_some_and(|indexed| indexed.job_id() == record.job_id())
    {
        return Err(JobRepositoryError::InvalidState);
    }
    if record.state != PENDING
        || spec.origin() != binding.origin()
        || spec.request_id() != binding.request_id()
        || spec.generation().get() != 1
    {
        return Err(JobRepositoryError::Conflict);
    }
    record.request.admitted_spec_sha256 = Some(spec_digest(spec)?);
    let bytes =
        serde_json::to_vec(&record.request).map_err(|_| JobRepositoryError::InvalidState)?;
    let changed = connection.execute("UPDATE job_start_requests SET state = ?1, request_json = ?2 WHERE job_id = ?3 AND state = ?4",
        params![ADMITTED, bytes, record.request.job_id.as_bytes().as_slice(), PENDING]).map_err(map_sql)?;
    if changed != 1 {
        return Err(JobRepositoryError::Conflict);
    }
    Ok(())
}

pub(super) fn capture(
    connection: &Connection,
) -> Result<Vec<StartRequestRecord>, JobRepositoryError> {
    let mut statement = connection.prepare("SELECT workspace, client, request_id, job_id, state, request_json FROM job_start_requests ORDER BY workspace, client, request_id").map_err(map_sql)?;
    let mut rows = statement.query([]).map_err(map_sql)?;
    let mut records = Vec::new();
    while let Some(row) = rows.next().map_err(map_sql)? {
        if records.len() >= MAXIMUM_START_REQUESTS {
            return Err(JobRepositoryError::InvalidState);
        }
        let record = decode_record(
            row.get(4).map_err(map_sql)?,
            &row.get::<_, Vec<u8>>(5).map_err(map_sql)?,
        )?;
        let (workspace, client, request_id) = record.key()?;
        if workspace != row.get::<_, String>(0).map_err(map_sql)?
            || client != row.get::<_, String>(1).map_err(map_sql)?
            || request_id != row.get::<_, Vec<u8>>(2).map_err(map_sql)?
            || record.job_id().as_bytes().as_slice() != row.get::<_, Vec<u8>>(3).map_err(map_sql)?
        {
            return Err(JobRepositoryError::InvalidState);
        }
        records
            .try_reserve(1)
            .map_err(|_| JobRepositoryError::Unavailable)?;
        records.push(record);
    }
    Ok(records)
}
