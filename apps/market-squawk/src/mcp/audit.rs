//! Bounded local MCP audit ownership with durable mutation admission.

use std::{
    io::Write as _,
    path::Path,
    sync::{Arc, Mutex},
    time::UNIX_EPOCH,
};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::{ambient_authority, fs::OpenOptions};
use market_squawk_mcp::{
    AuditCompletion, AuditCompletionReservation, AuditError, AuditEvent, AuditOperation,
    AuditPhase, AuditResultClass, AuditSink, LocalProcessIdentityClass, MutationAuditBundle,
    MutationAuditReservation,
};
use serde::Serialize;
use thiserror::Error;

const MAXIMUM_AUDIT_RECORDS: usize = 16_384;
const MAXIMUM_ENCODED_RECORD_BYTES: usize = 8 * 1024;

/// One-session bounded audit sink retained by the production composition.
#[derive(Debug)]
pub(super) struct DurableAuditSink {
    state: Arc<Mutex<AuditState>>,
}

impl DurableAuditSink {
    pub(super) fn try_new(data_root: &Path) -> Result<Self, LocalAuditError> {
        let root = cap_std::fs::Dir::open_ambient_dir(data_root, ambient_authority())
            .map_err(LocalAuditError::Io)?;
        root.create_dir_all("control")
            .map_err(LocalAuditError::Io)?;
        let control = root.open_dir("control").map_err(LocalAuditError::Io)?;
        let mut options = OpenOptions::new();
        options.write(true).append(true).create(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let file = control
            .open_with("mcp-audit.jsonl", &options)
            .map_err(LocalAuditError::Io)?;
        if !file
            .metadata()
            .map_err(LocalAuditError::Io)?
            .file_type()
            .is_file()
        {
            return Err(LocalAuditError::NonRegularFile);
        }
        let mut records = Vec::new();
        records
            .try_reserve_exact(MAXIMUM_AUDIT_RECORDS)
            .map_err(|_| LocalAuditError::Capacity)?;
        records.resize_with(MAXIMUM_AUDIT_RECORDS, || None);
        Ok(Self {
            state: Arc::new(Mutex::new(AuditState {
                file,
                records: records.into_boxed_slice(),
                next: 0,
            })),
        })
    }

    pub(super) fn flush(&self) -> Result<(), LocalAuditError> {
        let mut state = self.state.lock().map_err(|_| LocalAuditError::State)?;
        for index in 0..state.next {
            let Some(event) = state.records[index].take() else {
                continue;
            };
            if let Err(error) = append_durable(&mut state.file, &event) {
                state.records[index] = Some(event);
                return Err(error);
            }
        }
        Ok(())
    }
}

impl AuditSink for DurableAuditSink {
    fn record(&self, event: AuditEvent) -> Result<(), AuditError> {
        let mut state = self.state.lock().map_err(|_| AuditError::Unavailable)?;
        let index = state.reserve(1).ok_or(AuditError::Unavailable)?;
        commit_reserved_durable(&mut state, index, event)
    }

    fn reserve_completion(
        &self,
        completion: AuditCompletion,
    ) -> Result<AuditCompletionReservation, AuditError> {
        let index = self.reserve_indices(1)?;
        let state = Arc::clone(&self.state);
        Ok(AuditCompletionReservation::new(completion, move |event| {
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            commit_reserved_durable(&mut state, index, event)
        }))
    }

    fn reserve_mutation(
        &self,
        bundle: MutationAuditBundle,
    ) -> Result<MutationAuditReservation, AuditError> {
        let first = self.reserve_indices(3)?;
        let admission_state = Arc::clone(&self.state);
        let service_state = Arc::clone(&self.state);
        let delivery_state = Arc::clone(&self.state);
        MutationAuditReservation::try_new(
            bundle,
            move |event| {
                let mut state = admission_state
                    .lock()
                    .map_err(|_| AuditError::Unavailable)?;
                append_durable(&mut state.file, &event).map_err(|_| AuditError::Unavailable)
            },
            move |event| {
                let mut state = service_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                commit_reserved_durable(&mut state, first.saturating_add(1), event)
            },
            move |event| {
                let mut state = delivery_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                commit_reserved_durable(&mut state, first.saturating_add(2), event)
            },
        )
    }
}

impl DurableAuditSink {
    fn reserve_indices(&self, count: usize) -> Result<usize, AuditError> {
        self.state
            .lock()
            .map_err(|_| AuditError::Unavailable)?
            .reserve(count)
            .ok_or(AuditError::Unavailable)
    }
}

#[derive(Debug)]
struct AuditState {
    file: cap_std::fs::File,
    records: Box<[Option<AuditEvent>]>,
    next: usize,
}

impl AuditState {
    fn reserve(&mut self, count: usize) -> Option<usize> {
        let end = self.next.checked_add(count)?;
        if end > self.records.len() {
            return None;
        }
        let start = self.next;
        self.next = end;
        Some(start)
    }
}

fn commit_reserved_durable(
    state: &mut AuditState,
    index: usize,
    event: AuditEvent,
) -> Result<(), AuditError> {
    if append_durable(&mut state.file, &event).is_ok() {
        return Ok(());
    }
    if let Some(slot) = state.records.get_mut(index) {
        *slot = Some(event);
    }
    Err(AuditError::Unavailable)
}

fn append_durable(file: &mut cap_std::fs::File, event: &AuditEvent) -> Result<(), LocalAuditError> {
    let encoded = serde_json::to_vec(&AuditRecord::try_from(event)?)
        .map_err(|_| LocalAuditError::Encoding)?;
    if encoded.len() > MAXIMUM_ENCODED_RECORD_BYTES {
        return Err(LocalAuditError::Capacity);
    }
    file.write_all(&encoded).map_err(LocalAuditError::Io)?;
    file.write_all(b"\n").map_err(LocalAuditError::Io)?;
    file.sync_all().map_err(LocalAuditError::Io)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuditRecord<'event> {
    schema_version: u16,
    phase: &'static str,
    request_id_sha256: &'event str,
    identity_class: &'static str,
    operation: AuditOperationRecord<'event>,
    occurred_at_unix_nanos: u64,
    content_sha256: &'event str,
    result_class: Option<&'static str>,
    limits: AuditLimitRecord,
}

impl<'event> AuditRecord<'event> {
    fn try_from(event: &'event AuditEvent) -> Result<Self, LocalAuditError> {
        let occurred_at_unix_nanos = event
            .occurred_at()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| LocalAuditError::Clock)?
            .as_nanos();
        let occurred_at_unix_nanos =
            u64::try_from(occurred_at_unix_nanos).map_err(|_| LocalAuditError::Clock)?;
        let limits = event.limits();
        Ok(Self {
            schema_version: 1,
            phase: phase_name(event.phase()),
            request_id_sha256: event.request_id_sha256(),
            identity_class: identity_name(event.identity_class()),
            operation: AuditOperationRecord::from(event.operation()),
            occurred_at_unix_nanos,
            content_sha256: event.content_sha256(),
            result_class: event.result_class().map(result_name),
            limits: AuditLimitRecord {
                maximum_inline_bytes: limits.maximum_inline_bytes(),
                maximum_inline_items: limits.maximum_inline_items(),
                maximum_result_bytes: limits.maximum_result_bytes(),
                maximum_result_items: limits.maximum_result_items(),
            },
        })
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum AuditOperationRecord<'event> {
    Initialize,
    Ping,
    ListTools,
    CallTool {
        name: &'event str,
        version: &'event str,
    },
    Other,
}

impl<'event> From<&'event AuditOperation> for AuditOperationRecord<'event> {
    fn from(operation: &'event AuditOperation) -> Self {
        match operation {
            AuditOperation::Initialize => Self::Initialize,
            AuditOperation::Ping => Self::Ping,
            AuditOperation::ListTools => Self::ListTools,
            AuditOperation::CallTool { name, version } => Self::CallTool { name, version },
            AuditOperation::Other => Self::Other,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuditLimitRecord {
    maximum_inline_bytes: usize,
    maximum_inline_items: usize,
    maximum_result_bytes: usize,
    maximum_result_items: usize,
}

const fn phase_name(phase: AuditPhase) -> &'static str {
    match phase {
        AuditPhase::Admitted => "admitted",
        AuditPhase::MutationAdmitted => "mutation_admitted",
        AuditPhase::MutationServiceCompleted => "mutation_service_completed",
        AuditPhase::Completed => "completed",
    }
}

const fn identity_name(identity: LocalProcessIdentityClass) -> &'static str {
    match identity {
        LocalProcessIdentityClass::InheritedStdioUnverified => "inherited_stdio_unverified",
        LocalProcessIdentityClass::CallerSuppliedIoUnverified => "caller_supplied_io_unverified",
    }
}

const fn result_name(result: AuditResultClass) -> &'static str {
    match result {
        AuditResultClass::Succeeded => "succeeded",
        AuditResultClass::ArtifactPublished => "artifact_published",
        AuditResultClass::ProtocolRejected => "protocol_rejected",
        AuditResultClass::ServiceRejected => "service_rejected",
        AuditResultClass::Cancelled => "cancelled",
        AuditResultClass::DeadlineExceeded => "deadline_exceeded",
        AuditResultClass::ResourceExhausted => "resource_exhausted",
        AuditResultClass::OutputUnavailable => "output_unavailable",
    }
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options
        .mode(0o600)
        .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}

/// Durable local audit construction or drain failure.
#[derive(Debug, Error)]
pub enum LocalAuditError {
    #[error("local MCP audit I/O failed")]
    Io(#[source] std::io::Error),
    #[error("local MCP audit path is not a regular file")]
    NonRegularFile,
    #[error("local MCP audit bounded capacity is unavailable")]
    Capacity,
    #[error("local MCP audit state is unavailable")]
    State,
    #[error("local MCP audit encoding failed")]
    Encoding,
    #[error("local MCP audit clock is invalid")]
    Clock,
}

#[cfg(all(test, unix))]
mod tests {
    use std::{error::Error, process::Command};

    use super::DurableAuditSink;

    #[test]
    fn audit_open_rejects_a_preexisting_fifo_without_blocking() -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        let control = temporary.path().join("control");
        std::fs::create_dir(&control)?;
        let fifo = control.join("mcp-audit.jsonl");
        let status = Command::new("mkfifo").arg(&fifo).status()?;
        if !status.success() {
            return Err(std::io::Error::other("mkfifo failed").into());
        }

        assert!(DurableAuditSink::try_new(temporary.path()).is_err());
        Ok(())
    }
}
