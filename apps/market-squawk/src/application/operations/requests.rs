//! Closed request decoding and domain-error projection for Operations.

use market_squawk_domain::Timestamp;
use market_squawk_runtime::WorkspaceId;
use market_squawk_services::ServiceError;
use serde::Deserialize;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::application::{
    logs::{LogDomain, LogSeverity, StructuredLogQuery},
    settings::SettingValue,
    setup::SetupPlanSelection,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct BackupListInput {
    pub(super) after_backup_id: Option<String>,
    pub(super) limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct BackupIdentityInput {
    pub(super) backup_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct RetentionInput {
    pub(super) keep_latest: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct WorkspaceListInput {
    pub(super) after_workspace_id: Option<Uuid>,
    pub(super) limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct WorkspaceTargetInput {
    pub(super) workspace_id: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct PreviewReferenceInput {
    pub(super) preview_id: String,
    pub(super) preview_digest: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct SettingsChangeInput {
    pub(super) expected_revision: u64,
    pub(super) changes: Vec<SettingValue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct SettingsRollbackInput {
    pub(super) expected_revision: u64,
    pub(super) target_revision: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct SetupPlanPreviewInput {
    pub(super) expected_revision: u64,
    pub(super) selection: SetupPlanSelection,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct SetupPlanConfirmationInput {
    pub(super) preview_id: Uuid,
    pub(super) preview_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct LogQueryInput {
    from: Option<Timestamp>,
    through: Option<Timestamp>,
    minimum_severity: Option<LogSeverity>,
    domain: Option<LogDomain>,
    source_id: Option<String>,
    job_id: Option<String>,
    correlation_id: Option<String>,
    search: Option<String>,
    after_sequence: Option<u64>,
    limit: usize,
}

impl LogQueryInput {
    pub(super) fn into_query(self) -> StructuredLogQuery {
        StructuredLogQuery {
            from: self.from,
            through: self.through,
            minimum_severity: self.minimum_severity,
            domain: self.domain,
            source_id: self.source_id,
            job_id: self.job_id,
            correlation_id: self.correlation_id,
            search: self.search,
            after_sequence: self.after_sequence,
            limit: self.limit,
        }
    }
}

pub(super) fn decode<T: for<'de> Deserialize<'de>>(
    arguments: &Map<String, Value>,
) -> Result<T, ServiceError> {
    let mut arguments = arguments.clone();
    arguments.remove("resultLimits");
    serde_json::from_value(Value::Object(arguments)).map_err(|_| ServiceError::InvalidRequest)
}

pub(super) fn decode_mutation<T: for<'de> Deserialize<'de>>(
    arguments: &Map<String, Value>,
) -> Result<T, ServiceError> {
    let mut arguments = arguments.clone();
    arguments.remove("confirm");
    decode(&arguments)
}

pub(super) fn require_confirmation(arguments: &Map<String, Value>) -> Result<(), ServiceError> {
    if arguments.get("confirm") == Some(&Value::Bool(true)) {
        Ok(())
    } else {
        Err(ServiceError::Unauthorized)
    }
}

pub(super) fn parse_workspace(value: Uuid) -> Result<WorkspaceId, ServiceError> {
    WorkspaceId::try_from_uuid(value).map_err(|_| ServiceError::InvalidRequest)
}

pub(super) fn parse_sha256(value: &str) -> Result<[u8; 32], ServiceError> {
    if value.len() != 64
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err(ServiceError::InvalidRequest);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair).map_err(|_| ServiceError::InvalidRequest)?;
        bytes[index] = u8::from_str_radix(pair, 16).map_err(|_| ServiceError::InvalidRequest)?;
    }
    Ok(bytes)
}

pub(super) fn result_item_count(value: &Value) -> usize {
    let Some(object) = value.as_object() else {
        return 1;
    };
    ["manifests", "workspaces", "records", "entries"]
        .into_iter()
        .filter_map(|field| object.get(field).and_then(Value::as_array).map(Vec::len))
        .max()
        .unwrap_or(1)
}

pub(super) fn map_backup_error(_error: super::super::backup::ProductBackupError) -> ServiceError {
    ServiceError::InvalidRequest
}

pub(super) fn map_lifecycle_error(error: super::super::lifecycle::LifecycleError) -> ServiceError {
    use super::super::lifecycle::LifecycleError;
    match error {
        LifecycleError::PreflightBlocked
        | LifecycleError::InvalidTarget
        | LifecycleError::StaleApproval
        | LifecycleError::WrongWorkspace
        | LifecycleError::StaleWorkspaceGeneration => ServiceError::InvalidRequest,
        LifecycleError::RequestsFenced | LifecycleError::AuthorityBusy => ServiceError::Unavailable,
        LifecycleError::InvalidGeneration
        | LifecycleError::GenerationExhausted
        | LifecycleError::AuthorityUnavailable
        | LifecycleError::InvalidTimeout
        | LifecycleError::Encoding
        | LifecycleError::InvalidRestartHandoff
        | LifecycleError::RuntimeIdentity(_) => ServiceError::Internal,
    }
}

pub(super) fn map_update_error(error: super::super::lifecycle::UpdateError) -> ServiceError {
    use super::super::lifecycle::UpdateError;
    match error {
        UpdateError::PreflightBlocked | UpdateError::StaleApproval => ServiceError::InvalidRequest,
        UpdateError::AuthorityBusy | UpdateError::AuthorityFenced => ServiceError::Unavailable,
        UpdateError::InvalidGeneration
        | UpdateError::GenerationExhausted
        | UpdateError::InvalidCandidate
        | UpdateError::InvalidTimeout
        | UpdateError::Encoding
        | UpdateError::ActivationFailed
        | UpdateError::HealthCheckFailed
        | UpdateError::RollbackFailed
        | UpdateError::JournalUnavailable => ServiceError::Internal,
    }
}

pub(super) fn map_setup_error(error: super::super::setup::SetupPlanError) -> ServiceError {
    use super::super::setup::SetupPlanError;
    match error {
        SetupPlanError::InvalidSelection
        | SetupPlanError::InvalidRevision
        | SetupPlanError::StaleRevision
        | SetupPlanError::PreviewUnavailable
        | SetupPlanError::PreviewExpired
        | SetupPlanError::InvalidConfirmation
        | SetupPlanError::CrossWorkspacePreview => ServiceError::InvalidRequest,
        SetupPlanError::CapacityExceeded => ServiceError::ResourceExhausted,
        SetupPlanError::Unavailable
        | SetupPlanError::RecoveryRequired
        | SetupPlanError::TimeUnavailable
        | SetupPlanError::Persistence(_) => ServiceError::Unavailable,
        SetupPlanError::RevisionExhausted
        | SetupPlanError::CorruptState
        | SetupPlanError::Encoding => ServiceError::Internal,
    }
}
