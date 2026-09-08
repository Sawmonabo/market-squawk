//! Bounded, origin-bound preview retention for operational mutations.

use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_domain::Timestamp;
use market_squawk_services::{RequestContext, ServiceError};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::{
    ManagedSettingsRollbackPreview, ProgramRollbackPreviewEvidence, RestorePreviewEvidence,
};
use crate::application::{
    backup::BackupRetentionPreview,
    lifecycle::{UpdatePreview, WorkspaceSwitchPreview},
    settings::SettingsChangePreview,
};

const MAXIMUM_PREVIEWS: usize = 256;
const PREVIEW_LIFETIME: Duration = Duration::from_secs(15 * 60);
const MAXIMUM_PREVIEW_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub(super) enum PreviewPayload {
    BackupRetention(BackupRetentionPreview),
    Restore(Box<RestorePreviewEvidence>),
    Workspace(WorkspaceSwitchPreview),
    Update(UpdatePreview),
    ProgramRollback(ProgramRollbackPreviewEvidence),
    SettingsChange(SettingsChangePreview),
    SettingsRollback(ManagedSettingsRollbackPreview),
}

#[derive(Debug)]
struct PreviewEntry {
    owner_workspace: Uuid,
    owner_client: Uuid,
    digest: [u8; 32],
    expires_at: Instant,
    payload: PreviewPayload,
}

/// Process-owned, bounded preview store. Restart invalidates every outstanding preview.
#[derive(Debug, Default)]
pub(super) struct PreviewRegistry {
    entries: Mutex<BTreeMap<Uuid, PreviewEntry>>,
}

impl PreviewRegistry {
    pub(super) fn insert(
        &self,
        context: &RequestContext,
        kind: &'static str,
        evidence: &impl Serialize,
        payload: PreviewPayload,
    ) -> Result<Value, ServiceError> {
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let encoded = serde_json::to_vec(&(
            "market-squawk-operations-preview-v1",
            kind,
            origin.workspace_id(),
            origin.client_id(),
            evidence,
        ))
        .map_err(|_| ServiceError::Internal)?;
        if encoded.len() > MAXIMUM_PREVIEW_BYTES {
            return Err(ServiceError::InvalidResult);
        }
        let digest: [u8; 32] = Sha256::digest(encoded).into();
        let preview_id = Uuid::new_v4();
        let expires_at = Instant::now()
            .checked_add(PREVIEW_LIFETIME)
            .ok_or(ServiceError::Unavailable)?;
        let expires_at_timestamp = current_timestamp()?
            .checked_add_nanos(
                i64::try_from(PREVIEW_LIFETIME.as_nanos())
                    .map_err(|_| ServiceError::Unavailable)?,
            )
            .map_err(|_| ServiceError::Unavailable)?;
        let mut entries = self.entries.lock().map_err(|_| ServiceError::Unavailable)?;
        let now = Instant::now();
        entries.retain(|_, entry| entry.expires_at > now);
        if entries.len() >= MAXIMUM_PREVIEWS {
            return Err(ServiceError::ResourceExhausted);
        }
        entries.insert(
            preview_id,
            PreviewEntry {
                owner_workspace: origin.workspace_id(),
                owner_client: origin.client_id(),
                digest,
                expires_at,
                payload,
            },
        );
        Ok(json!({
            "previewId": preview_id,
            "previewDigest": encode_hex(digest),
            "expiresAt": expires_at_timestamp,
            "evidence": project_digest_fields(
                serde_json::to_value(evidence).map_err(|_| ServiceError::Internal)?,
            )?,
        }))
    }

    pub(super) fn consume(
        &self,
        context: &RequestContext,
        preview_id: Uuid,
        expected_digest: [u8; 32],
    ) -> Result<(PreviewPayload, [u8; 32]), ServiceError> {
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let mut entries = self.entries.lock().map_err(|_| ServiceError::Unavailable)?;
        let entry = entries.get(&preview_id).ok_or(ServiceError::NotFound)?;
        if entry.expires_at <= Instant::now()
            || entry.owner_workspace != origin.workspace_id()
            || entry.owner_client != origin.client_id()
            || entry.digest != expected_digest
        {
            return Err(ServiceError::Unauthorized);
        }
        let entry = entries.remove(&preview_id).ok_or(ServiceError::NotFound)?;
        Ok((entry.payload, entry.digest))
    }
}

/// Converts explicitly digest-valued fields to lowercase SHA-256 hex for client DTOs.
pub(super) fn project_digest_fields(value: Value) -> Result<Value, ServiceError> {
    match value {
        Value::Array(values) => values
            .into_iter()
            .map(project_digest_fields)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(mut object) => {
            for (key, value) in &mut object {
                let digest_field = key == "backupId"
                    || key == "snapshotId"
                    || key.ends_with("BackupId")
                    || key.ends_with("BackupIds")
                    || key == "sha256"
                    || key == "digest"
                    || key.ends_with("Sha256")
                    || key.ends_with("Digest")
                    || key.ends_with("_sha256")
                    || key.ends_with("_digest");
                if digest_field {
                    *value = project_digest_value(value.take())?;
                } else {
                    *value = project_digest_fields(value.take())?;
                }
            }
            Ok(Value::Object(object))
        }
        scalar => Ok(scalar),
    }
}

fn project_digest_value(value: Value) -> Result<Value, ServiceError> {
    if value.is_null() {
        return Ok(value);
    }
    if value.as_str().is_some_and(|value| {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }) {
        return Ok(value);
    }
    let Value::Array(values) = value else {
        return Err(ServiceError::InvalidResult);
    };
    if values.len() == 32 && values.iter().all(Value::is_u64) {
        let bytes = values
            .into_iter()
            .map(|byte| {
                byte.as_u64()
                    .and_then(|byte| u8::try_from(byte).ok())
                    .ok_or(ServiceError::InvalidResult)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let bytes: [u8; 32] = bytes.try_into().map_err(|_| ServiceError::InvalidResult)?;
        return Ok(Value::String(encode_hex(bytes)));
    }
    values
        .into_iter()
        .map(project_digest_value)
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

pub(super) fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn current_timestamp() -> Result<Timestamp, ServiceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?
        .as_nanos();
    i64::try_from(nanos)
        .map(Timestamp::from_unix_nanos)
        .map_err(|_| ServiceError::Unavailable)
}
