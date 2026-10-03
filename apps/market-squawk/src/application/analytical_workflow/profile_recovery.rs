//! Preserve unsupported profiles verbatim before admitting this release's current default.

use std::collections::HashSet;

use market_squawk_platform::LocalAuthorityStateStore;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::{ControllerDocument, WorkflowError, WorkflowRunState, hex_digest, unix_nanos_now};

const MAXIMUM_REJECTED_DOCUMENTS: usize = 32;
pub(super) const RECOVERY_NOTICE: &str = "Earlier analysis profiles do not match the current settings and cannot be used. Their original records are preserved. Current recommended settings are available; copy them to create a new custom profile.";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct RejectedProfileDocument {
    payload_sha256: String,
    payload_bytes: usize,
    preserved_at: String,
}

pub(super) fn validate_rejections(
    records: &[RejectedProfileDocument],
) -> Result<(), WorkflowError> {
    let mut digests = HashSet::with_capacity(records.len());
    if records.len() > MAXIMUM_REJECTED_DOCUMENTS
        || records.iter().any(|record| {
            !super::valid_digest(&record.payload_sha256)
                || !digests.insert(&record.payload_sha256)
                || record.payload_bytes == 0
                || record.payload_bytes > LocalAuthorityStateStore::maximum_payload_bytes()
                || !super::valid_timestamp(&record.preserved_at)
        })
    {
        return Err(WorkflowError::internal());
    }
    Ok(())
}

pub(super) fn read_current_document(
    store: &LocalAuthorityStateStore,
    owner_workspace_id: Uuid,
    payload: &[u8],
) -> Result<ControllerDocument, WorkflowError> {
    let previous: ControllerDocument =
        serde_json::from_slice(payload).map_err(|_| WorkflowError::internal())?;
    if previous.validate(owner_workspace_id).is_ok() {
        return Ok(previous);
    }
    // An incompatible financial schema does not authorize discarding corrupt controller state.
    // Authenticate the same workspace, profile content digests, journal and exact job handles
    // without interpreting those rejected financial settings as current authority.
    previous.validate_for_retention(owner_workspace_id, false)?;
    if previous.rejected_profile_documents.len() >= MAXIMUM_REJECTED_DOCUMENTS {
        return Err(WorkflowError::new(
            "profile_recovery_capacity",
            "Earlier analysis records must be preserved before more profiles can be replaced.",
        ));
    }
    let preserved_at = unix_nanos_now()?;
    let payload_sha256 = hex_digest(Sha256::digest(payload));
    let archive = store
        .try_open_namespace(&payload_sha256)
        .map_err(|_| WorkflowError::internal())?;
    match archive.load().map_err(|_| WorkflowError::internal())? {
        Some(retained) if retained == payload => {}
        Some(_) => return Err(WorkflowError::internal()),
        None => archive
            .store(payload)
            .map_err(|_| WorkflowError::internal())?,
    }
    // Preserve in the retained directory first, then install an independently constructed current
    // default. The archive uses the same durable two-slot writer and never introduces an unsynced
    // child directory. A crash before the second commit repeats the exact archive check; no old
    // config or digest is rewritten.
    let mut current = ControllerDocument::initial(owner_workspace_id)?;
    current.rejected_profile_documents = previous.rejected_profile_documents;
    current
        .rejected_profile_documents
        .push(RejectedProfileDocument {
            payload_sha256,
            payload_bytes: payload.len(),
            preserved_at: preserved_at.clone(),
        });
    current.prepared_starts = previous.prepared_starts;
    current.workflow_runs = previous.workflow_runs;
    for run in &mut current.workflow_runs {
        if matches!(
            run.state,
            WorkflowRunState::Completed | WorkflowRunState::Cancelled
        ) {
            continue;
        }
        // Rejected methodology cannot resume analytical work. Its original child jobs and
        // pending request remain under the same existing cancellation owner until settled.
        run.state = if run.pending_invocation.is_some()
            || run
                .child_jobs
                .iter()
                .any(|child| child.terminal_sequence.is_none())
        {
            WorkflowRunState::Cancelling
        } else {
            WorkflowRunState::Stale
        };
        run.updated_at.clone_from(&preserved_at);
        run.last_error = Some("The captured profile is unavailable in this release. Start a new analysis with current settings after earlier work has stopped.".to_owned());
    }
    current.validate(owner_workspace_id)?;
    let encoded = serde_json::to_vec(&current).map_err(|_| WorkflowError::internal())?;
    store
        .store(&encoded)
        .map_err(|_| WorkflowError::internal())?;
    Ok(current)
}
