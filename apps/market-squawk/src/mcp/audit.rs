//! Bounded local MCP audit ownership with durable mutation admission.

use std::{
    fs::File,
    io::{Read as _, Seek as _, SeekFrom, Write as _},
    sync::{Arc, Mutex},
    time::UNIX_EPOCH,
};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use fs2::FileExt as _;
use market_squawk_mcp::{
    AuditCompletion, AuditCompletionReservation, AuditError, AuditEvent, AuditOperation,
    AuditPhase, AuditResultClass, AuditSink, LocalProcessIdentityClass, MutationAuditBundle,
    MutationAuditReservation,
};
use serde::Serialize;
use thiserror::Error;

const MAXIMUM_AUDIT_RECORDS: usize = 16_384;
const MAXIMUM_ENCODED_RECORD_BYTES: usize = 8 * 1024;
const MAXIMUM_AUDIT_FILE_BYTES: u64 =
    MAXIMUM_AUDIT_RECORDS as u64 * (MAXIMUM_ENCODED_RECORD_BYTES as u64 + 1);

/// One-session bounded audit sink retained by the production composition.
#[derive(Debug)]
pub(super) struct DurableAuditSink {
    state: Arc<Mutex<AuditState>>,
}

impl DurableAuditSink {
    pub(super) fn try_new(control: Dir) -> Result<Self, LocalAuditError> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).append(true).create(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let file = control
            .open_with("mcp-audit.jsonl", &options)
            .map_err(LocalAuditError::Io)?
            .into_std();
        validate_private_file_identity(&control, &file)?;
        file.try_lock_exclusive().map_err(|source| {
            if source.kind() == std::io::ErrorKind::WouldBlock {
                LocalAuditError::AlreadyLocked
            } else {
                LocalAuditError::Io(source)
            }
        })?;
        validate_private_file_identity(&control, &file)?;
        recover_audit_file(&file)?;
        validate_private_file_identity(&control, &file)?;
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
                poisoned: false,
            })),
        })
    }

    pub(super) fn flush(&self) -> Result<(), LocalAuditError> {
        let mut state = self.state.lock().map_err(|_| LocalAuditError::State)?;
        if state.poisoned {
            return Err(LocalAuditError::Poisoned);
        }
        for index in 0..state.next {
            let Some(event) = state.records[index].take() else {
                continue;
            };
            if let Err(error) = append_durable(&mut state, &event) {
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
                append_durable(&mut state, &event).map_err(|_| AuditError::Unavailable)
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
    file: File,
    records: Box<[Option<AuditEvent>]>,
    next: usize,
    poisoned: bool,
}

impl AuditState {
    fn reserve(&mut self, count: usize) -> Option<usize> {
        if self.poisoned {
            return None;
        }
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
    if append_durable(state, &event).is_ok() {
        return Ok(());
    }
    if let Some(slot) = state.records.get_mut(index) {
        *slot = Some(event);
    }
    Err(AuditError::Unavailable)
}

fn append_durable(state: &mut AuditState, event: &AuditEvent) -> Result<(), LocalAuditError> {
    if state.poisoned {
        return Err(LocalAuditError::Poisoned);
    }
    let encoded = serde_json::to_vec(&AuditRecord::try_from(event)?)
        .map_err(|_| LocalAuditError::Encoding)?;
    if encoded.len() > MAXIMUM_ENCODED_RECORD_BYTES {
        return Err(LocalAuditError::Capacity);
    }
    let frame_bytes = encoded
        .len()
        .checked_add(1)
        .ok_or(LocalAuditError::Capacity)?;
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(frame_bytes)
        .map_err(|_| LocalAuditError::Capacity)?;
    frame.extend_from_slice(&encoded);
    frame.push(b'\n');

    let offset = state
        .file
        .seek(SeekFrom::End(0))
        .map_err(LocalAuditError::Io)?;
    let observed_length = state.file.metadata().map_err(LocalAuditError::Io)?.len();
    if offset != observed_length {
        state.poisoned = true;
        return Err(LocalAuditError::Poisoned);
    }
    let final_length = offset
        .checked_add(u64::try_from(frame.len()).map_err(|_| LocalAuditError::Capacity)?)
        .ok_or(LocalAuditError::Capacity)?;
    if final_length > MAXIMUM_AUDIT_FILE_BYTES {
        return Err(LocalAuditError::Capacity);
    }

    let append_result = state.file.write_all(&frame).and_then(|()| {
        if state.file.metadata()?.len() != final_length {
            return Err(std::io::Error::other(
                "audit append length did not match the reserved frame",
            ));
        }
        state.file.sync_all()
    });
    if let Err(source) = append_result {
        if rollback_append(&mut state.file, offset).is_err() {
            state.poisoned = true;
            return Err(LocalAuditError::Poisoned);
        }
        return Err(LocalAuditError::Io(source));
    }
    Ok(())
}

fn rollback_append(file: &mut File, offset: u64) -> Result<(), LocalAuditError> {
    file.set_len(offset).map_err(LocalAuditError::Io)?;
    file.sync_all().map_err(LocalAuditError::Io)?;
    let recovered_length = file.seek(SeekFrom::End(0)).map_err(LocalAuditError::Io)?;
    if recovered_length != offset {
        return Err(LocalAuditError::Poisoned);
    }
    Ok(())
}

fn recover_audit_file(file: &File) -> Result<(), LocalAuditError> {
    let file_length = file.metadata().map_err(LocalAuditError::Io)?.len();
    if file_length > MAXIMUM_AUDIT_FILE_BYTES {
        return Err(LocalAuditError::Capacity);
    }

    let mut reader = file.try_clone().map_err(LocalAuditError::Io)?;
    reader
        .seek(SeekFrom::Start(0))
        .map_err(LocalAuditError::Io)?;
    let mut chunk = [0_u8; MAXIMUM_ENCODED_RECORD_BYTES];
    let mut record = Vec::new();
    record
        .try_reserve_exact(MAXIMUM_ENCODED_RECORD_BYTES)
        .map_err(|_| LocalAuditError::Capacity)?;
    let mut record_too_large = false;
    let mut consumed = 0_u64;
    let mut proven_boundary = 0_u64;

    loop {
        let read = reader.read(&mut chunk).map_err(LocalAuditError::Io)?;
        if read == 0 {
            break;
        }
        for byte in &chunk[..read] {
            consumed = consumed.checked_add(1).ok_or(LocalAuditError::Capacity)?;
            if *byte == b'\n' {
                if record_too_large || !is_valid_audit_record(&record) {
                    return Err(LocalAuditError::CorruptRecord);
                }
                record.clear();
                record_too_large = false;
                proven_boundary = consumed;
            } else if record.len() < MAXIMUM_ENCODED_RECORD_BYTES {
                record.push(*byte);
            } else {
                record_too_large = true;
            }
        }
    }

    if consumed != proven_boundary {
        file.set_len(proven_boundary).map_err(LocalAuditError::Io)?;
        file.sync_all().map_err(LocalAuditError::Io)?;
    }
    Ok(())
}

fn is_valid_audit_record(record: &[u8]) -> bool {
    let Ok(serde_json::Value::Object(record)) = serde_json::from_slice(record) else {
        return false;
    };
    let Some(serde_json::Value::Object(operation)) = record.get("operation") else {
        return false;
    };
    let Some(serde_json::Value::Object(limits)) = record.get("limits") else {
        return false;
    };
    record
        .get("schemaVersion")
        .and_then(serde_json::Value::as_u64)
        == Some(1)
        && record
            .get("phase")
            .and_then(serde_json::Value::as_str)
            .is_some()
        && record
            .get("requestIdSha256")
            .and_then(serde_json::Value::as_str)
            .is_some()
        && record
            .get("identityClass")
            .and_then(serde_json::Value::as_str)
            .is_some()
        && operation
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .is_some()
        && record
            .get("occurredAtUnixNanos")
            .and_then(serde_json::Value::as_u64)
            .is_some()
        && record
            .get("contentSha256")
            .and_then(serde_json::Value::as_str)
            .is_some()
        && record
            .get("resultClass")
            .is_some_and(|value| value.is_null() || value.is_string())
        && limits
            .get("maximumInlineBytes")
            .and_then(serde_json::Value::as_u64)
            .is_some()
        && limits
            .get("maximumInlineItems")
            .and_then(serde_json::Value::as_u64)
            .is_some()
        && limits
            .get("maximumResultBytes")
            .and_then(serde_json::Value::as_u64)
            .is_some()
        && limits
            .get("maximumResultItems")
            .and_then(serde_json::Value::as_u64)
            .is_some()
}

fn validate_private_file_identity(control: &Dir, file: &File) -> Result<(), LocalAuditError> {
    use cap_fs_ext::MetadataExt as _;

    let opened = file.metadata().map_err(LocalAuditError::Io)?;
    let named = control
        .symlink_metadata("mcp-audit.jsonl")
        .map_err(LocalAuditError::Io)?;
    if !opened.is_file()
        || !named.is_file()
        || opened.nlink() != 1
        || named.nlink() != 1
        || (opened.dev(), opened.ino()) != (named.dev(), named.ino())
    {
        return Err(LocalAuditError::UnsafeFileIdentity);
    }
    validate_private_permissions(&opened)
}

#[cfg(unix)]
fn validate_private_permissions(metadata: &std::fs::Metadata) -> Result<(), LocalAuditError> {
    use std::os::unix::fs::MetadataExt as _;

    if metadata.mode() & 0o077 == 0 {
        Ok(())
    } else {
        Err(LocalAuditError::InsecurePermissions)
    }
}

#[cfg(windows)]
fn validate_private_permissions(metadata: &std::fs::Metadata) -> Result<(), LocalAuditError> {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    if metadata.number_of_links() != Some(1)
        || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    {
        return Err(LocalAuditError::UnsafeFileIdentity);
    }
    Err(LocalAuditError::PermissionProofUnavailable)
}

#[cfg(not(any(unix, windows)))]
fn validate_private_permissions(_metadata: &std::fs::Metadata) -> Result<(), LocalAuditError> {
    Err(LocalAuditError::PermissionProofUnavailable)
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
    #[error("local MCP audit endpoint identity is unsafe or ambiguous")]
    UnsafeFileIdentity,
    #[error("local MCP audit endpoint permissions are not private")]
    InsecurePermissions,
    #[error("local MCP audit endpoint permissions cannot be proven private")]
    PermissionProofUnavailable,
    #[error("local MCP audit endpoint is already locked")]
    AlreadyLocked,
    #[error("local MCP audit contains a corrupt complete record")]
    CorruptRecord,
    #[error("local MCP audit durability is indeterminate and the sink is poisoned")]
    Poisoned,
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
    use std::{
        error::Error,
        fs::{self, OpenOptions},
        io::Write as _,
        os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _},
        process::Command,
    };

    use market_squawk_platform::LocalPaths;

    use super::{DurableAuditSink, LocalAuditError};

    #[test]
    fn audit_open_rejects_a_preexisting_fifo_without_blocking() -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path())?;
        let control = paths.control_root()?;
        let fifo = control.root().join("mcp-audit.jsonl");
        let status = Command::new("mkfifo").arg(&fifo).status()?;
        if !status.success() {
            return Err(std::io::Error::other("mkfifo failed").into());
        }

        assert!(DurableAuditSink::try_new(control.try_clone_directory()?).is_err());
        Ok(())
    }

    #[test]
    fn audit_open_rejects_a_hard_linked_endpoint() -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path())?;
        let control = paths.control_root()?;
        let audit = control.root().join("mcp-audit.jsonl");
        let alias = control.root().join("mcp-audit-alias.jsonl");
        let _file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&audit)?;
        fs::hard_link(&audit, alias)?;

        assert!(DurableAuditSink::try_new(control.try_clone_directory()?).is_err());
        Ok(())
    }

    #[test]
    fn audit_open_rejects_a_permissive_existing_endpoint() -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path())?;
        let control = paths.control_root()?;
        let audit = control.root().join("mcp-audit.jsonl");
        let _file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&audit)?;
        let mut permissions = fs::metadata(&audit)?.permissions();
        permissions.set_mode(0o640);
        fs::set_permissions(&audit, permissions)?;

        assert!(DurableAuditSink::try_new(control.try_clone_directory()?).is_err());
        Ok(())
    }

    #[test]
    fn audit_open_truncates_only_an_incomplete_final_record() -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path())?;
        let control = paths.control_root()?;
        let audit = control.root().join("mcp-audit.jsonl");
        let valid = br#"{"schemaVersion":1,"phase":"admitted","requestIdSha256":"request","identityClass":"caller_supplied_io_unverified","operation":{"kind":"ping"},"occurredAtUnixNanos":1,"contentSha256":"content","resultClass":null,"limits":{"maximumInlineBytes":1,"maximumInlineItems":1,"maximumResultBytes":1,"maximumResultItems":1}}"#;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&audit)?;
        file.write_all(valid)?;
        file.write_all(b"\n{\"schemaVersion\":")?;
        file.sync_all()?;
        drop(file);

        let sink = DurableAuditSink::try_new(control.try_clone_directory()?)?;
        drop(sink);

        let recovered = fs::read(audit)?;
        assert_eq!(recovered, [valid.as_slice(), b"\n"].concat());
        Ok(())
    }

    #[test]
    fn audit_open_rejects_corruption_at_a_complete_record_boundary() -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path())?;
        let control = paths.control_root()?;
        let audit = control.root().join("mcp-audit.jsonl");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&audit)?;
        file.write_all(b"not-json\n")?;
        file.sync_all()?;
        drop(file);

        assert!(DurableAuditSink::try_new(control.try_clone_directory()?).is_err());
        Ok(())
    }

    #[test]
    fn poisoned_audit_state_remains_terminal_during_shutdown_flush() -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path())?;
        let control = paths.control_root()?;
        let sink = DurableAuditSink::try_new(control.try_clone_directory()?)?;
        sink.state
            .lock()
            .map_err(|_| std::io::Error::other("audit mutex poisoned in test"))?
            .poisoned = true;

        assert!(matches!(sink.flush(), Err(LocalAuditError::Poisoned)));
        Ok(())
    }
}
