//! Owned durable consumers for mandatory execution and paper audit streams.

use std::{
    fs::File,
    io::Write as _,
    sync::mpsc,
    thread::{self, JoinHandle},
    time::Duration,
};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_adapter_paper::{
    PaperAuditKind, PaperAuditReadError, PaperAuditReader, PaperAuditRecord, PaperOrderState,
};
use market_squawk_execution::{
    ExecutionAuditError, ExecutionAuditEvent, ExecutionAuditKind, ExecutionAuditReader,
    ExecutionAuditReason,
};
use serde::Serialize;
use thiserror::Error;

const AUDIT_SCHEMA_VERSION: u16 = 1;
const MAXIMUM_ENCODED_RECORD_BYTES: usize = 64 * 1024;
const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(1);
const EXECUTION_AUDIT_FILE: &str = "paper-execution-audit-v1.jsonl";
const PAPER_AUDIT_FILE: &str = "paper-state-audit-v1.jsonl";

/// Sole owner of both mandatory production audit consumers and their durable files.
#[derive(Debug)]
pub(super) struct ProductionAuditService {
    control: mpsc::Sender<AuditControl>,
    worker: Option<JoinHandle<Result<ProductionAuditEvidence, ProductionAuditError>>>,
}

#[derive(Debug)]
enum AuditControl {
    Flush(mpsc::SyncSender<Option<ProductionAuditEvidence>>),
}

impl ProductionAuditService {
    pub(super) fn try_start(
        directory: Dir,
        execution: ExecutionAuditReader,
        paper: PaperAuditReader,
    ) -> Result<Self, ProductionAuditError> {
        let execution_file = open_audit_file(&directory, EXECUTION_AUDIT_FILE)?;
        let paper_file = open_audit_file(&directory, PAPER_AUDIT_FILE)?;
        let (control, commands) = mpsc::channel();
        let worker = thread::Builder::new()
            .name(String::from("market-squawk-paper-audit"))
            .spawn(move || {
                run_audit_service(execution, paper, execution_file, paper_file, commands)
            })
            .map_err(ProductionAuditError::Io)?;
        Ok(Self {
            control,
            worker: Some(worker),
        })
    }

    pub(super) fn flush(&self) -> Result<ProductionAuditEvidence, ProductionAuditBarrierError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.control
            .send(AuditControl::Flush(reply))
            .map_err(|_| ProductionAuditBarrierError::Unavailable)?;
        response
            .recv()
            .map_err(|_| ProductionAuditBarrierError::Unavailable)?
            .ok_or(ProductionAuditBarrierError::PersistenceFailed)
    }

    pub(super) fn shutdown(mut self) -> ProductionAuditShutdown {
        let Some(worker) = self.worker.take() else {
            return ProductionAuditShutdown::Panicked;
        };
        match worker.join() {
            Ok(Ok(evidence)) => ProductionAuditShutdown::Complete(evidence),
            Ok(Err(error)) => ProductionAuditShutdown::Failed(error),
            Err(_) => ProductionAuditShutdown::Panicked,
        }
    }
}

impl Drop for ProductionAuditService {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            drop(worker.join());
        }
    }
}

fn run_audit_service(
    execution: ExecutionAuditReader,
    paper: PaperAuditReader,
    execution_file: File,
    paper_file: File,
    commands: mpsc::Receiver<AuditControl>,
) -> Result<ProductionAuditEvidence, ProductionAuditError> {
    let mut worker = AuditWorker {
        execution,
        paper,
        execution_file,
        paper_file,
        execution_closed: false,
        paper_closed: false,
        execution_records: 0,
        paper_records: 0,
    };
    let result = run_audit_worker(&mut worker, &commands);
    if result.is_err() {
        worker.paper.report_persistence_failure();
    }
    result
}

#[derive(Debug)]
struct AuditWorker {
    execution: ExecutionAuditReader,
    paper: PaperAuditReader,
    execution_file: File,
    paper_file: File,
    execution_closed: bool,
    paper_closed: bool,
    execution_records: u64,
    paper_records: u64,
}

impl AuditWorker {
    fn drain_once(&mut self) -> Result<bool, ProductionAuditError> {
        let mut progressed = false;
        if !self.execution_closed {
            match self.execution.try_next() {
                Ok(Some(event)) => {
                    append_durable(
                        &mut self.execution_file,
                        &ExecutionAuditRecord::from(&event),
                    )?;
                    self.execution_records = self
                        .execution_records
                        .checked_add(1)
                        .ok_or(ProductionAuditError::RecordCountOverflow)?;
                    progressed = true;
                }
                Ok(None) => {}
                Err(ExecutionAuditError::Closed) => self.execution_closed = true,
                Err(error) => return Err(ProductionAuditError::ExecutionReader(error)),
            }
        }
        if !self.paper_closed {
            match self.paper.try_next() {
                Ok(Some(record)) => {
                    append_durable(&mut self.paper_file, &PaperAuditRecordWire::from(record))?;
                    self.paper_records = self
                        .paper_records
                        .checked_add(1)
                        .ok_or(ProductionAuditError::RecordCountOverflow)?;
                    progressed = true;
                }
                Ok(None) => {}
                Err(PaperAuditReadError::Closed) => self.paper_closed = true,
            }
        }
        Ok(progressed)
    }

    fn flush(&mut self) -> Result<ProductionAuditEvidence, ProductionAuditError> {
        while self.drain_once()? {}
        self.execution_file
            .sync_all()
            .map_err(ProductionAuditError::Io)?;
        self.paper_file
            .sync_all()
            .map_err(ProductionAuditError::Io)?;
        Ok(self.evidence())
    }

    const fn evidence(&self) -> ProductionAuditEvidence {
        ProductionAuditEvidence {
            execution_records: self.execution_records,
            paper_records: self.paper_records,
        }
    }

    const fn closed(&self) -> bool {
        self.execution_closed && self.paper_closed
    }
}

fn run_audit_worker(
    worker: &mut AuditWorker,
    commands: &mpsc::Receiver<AuditControl>,
) -> Result<ProductionAuditEvidence, ProductionAuditError> {
    loop {
        while let Ok(command) = commands.try_recv() {
            match command {
                AuditControl::Flush(reply) => match worker.flush() {
                    Ok(evidence) => {
                        let _ = reply.send(Some(evidence));
                    }
                    Err(error) => {
                        let _ = reply.send(None);
                        return Err(error);
                    }
                },
            }
        }
        if worker.closed() {
            return worker.flush();
        }
        if !worker.drain_once()? {
            thread::park_timeout(IDLE_POLL_INTERVAL);
        }
    }
}

fn open_audit_file(directory: &Dir, name: &str) -> Result<File, ProductionAuditError> {
    let mut options = OpenOptions::new();
    options.write(true).append(true).create(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    let file = directory
        .open_with(name, &options)
        .map_err(ProductionAuditError::Io)?;
    if !file
        .metadata()
        .map_err(ProductionAuditError::Io)?
        .file_type()
        .is_file()
    {
        return Err(ProductionAuditError::NonRegularFile);
    }
    let file = file.into_std();
    fs2::FileExt::try_lock_exclusive(&file).map_err(|_| ProductionAuditError::AlreadyOwned)?;
    Ok(file)
}

fn append_durable(file: &mut File, record: &impl Serialize) -> Result<(), ProductionAuditError> {
    let encoded = serde_json::to_vec(record).map_err(|_| ProductionAuditError::Encoding)?;
    if encoded.len() > MAXIMUM_ENCODED_RECORD_BYTES {
        return Err(ProductionAuditError::RecordTooLarge);
    }
    file.write_all(&encoded).map_err(ProductionAuditError::Io)?;
    file.write_all(b"\n").map_err(ProductionAuditError::Io)?;
    file.sync_all().map_err(ProductionAuditError::Io)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionAuditRecord {
    schema_version: u16,
    kind: ExecutionAuditKind,
    approval_id: String,
    order_id: String,
    intent_digest_sha256: String,
    strategy_id: String,
    model_id: Option<String>,
    account_id: String,
    instrument_id: String,
    assessment_digest_sha256: Option<String>,
    evidence_binding_digest_sha256: Option<String>,
    execution_identity_digest_sha256: Option<String>,
    risk_policy_digest_sha256: String,
    risk_policy_ruleset_version: u32,
    market_observed_at_unix_nanos: i64,
    valid_until_unix_nanos: i64,
    observed_at_unix_nanos: i64,
    reasons: Vec<ExecutionAuditReason>,
}

impl From<&ExecutionAuditEvent> for ExecutionAuditRecord {
    fn from(event: &ExecutionAuditEvent) -> Self {
        let policy = event.risk_policy();
        Self {
            schema_version: AUDIT_SCHEMA_VERSION,
            kind: event.kind(),
            approval_id: event.approval_id().to_string(),
            order_id: event.order_id().to_string(),
            intent_digest_sha256: hex(event.intent_digest().as_bytes()),
            strategy_id: event.strategy_id().to_string(),
            model_id: event.model_id().map(|model| model.to_string()),
            account_id: event.account_id().to_string(),
            instrument_id: event.instrument_id().to_string(),
            assessment_digest_sha256: event.assessment_digest().map(hex),
            evidence_binding_digest_sha256: event.evidence_binding_digest().map(hex),
            execution_identity_digest_sha256: event.execution_identity_digest().map(hex),
            risk_policy_digest_sha256: hex(policy.digest()),
            risk_policy_ruleset_version: policy.ruleset_version().get(),
            market_observed_at_unix_nanos: event.market_observed_at().unix_nanos(),
            valid_until_unix_nanos: event.valid_until().unix_nanos(),
            observed_at_unix_nanos: event.observed_at().unix_nanos(),
            reasons: event.reasons().collect(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PaperAuditRecordWire {
    schema_version: u16,
    sequence: u64,
    order_id: Option<String>,
    kind: PaperAuditKind,
    previous_state: Option<PaperOrderState>,
    new_state: Option<PaperOrderState>,
    event_at_unix_nanos: i64,
    fill_quantity_lots: Option<i64>,
    configuration_digest_sha256: String,
    input_digest_sha256: String,
}

impl From<PaperAuditRecord> for PaperAuditRecordWire {
    fn from(record: PaperAuditRecord) -> Self {
        Self {
            schema_version: AUDIT_SCHEMA_VERSION,
            sequence: record.sequence(),
            order_id: record.order_id().map(|order| order.to_string()),
            kind: record.kind(),
            previous_state: record.previous_state(),
            new_state: record.new_state(),
            event_at_unix_nanos: record.event_at().unix_nanos(),
            fill_quantity_lots: record.fill_quantity().map(|quantity| quantity.get()),
            configuration_digest_sha256: hex(record.configuration_digest()),
            input_digest_sha256: hex(record.input_digest()),
        }
    }
}

fn hex(bytes: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProductionAuditEvidence {
    execution_records: u64,
    paper_records: u64,
}

impl ProductionAuditEvidence {
    pub const fn execution_records(self) -> u64 {
        self.execution_records
    }

    pub const fn paper_records(self) -> u64 {
        self.paper_records
    }
}

#[derive(Debug)]
pub enum ProductionAuditShutdown {
    Complete(ProductionAuditEvidence),
    Failed(ProductionAuditError),
    Panicked,
}

impl ProductionAuditShutdown {
    pub const fn is_complete(&self) -> bool {
        matches!(self, Self::Complete(_))
    }

    pub const fn evidence(&self) -> Option<ProductionAuditEvidence> {
        match self {
            Self::Complete(evidence) => Some(*evidence),
            Self::Failed(_) | Self::Panicked => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProductionAuditBarrierError {
    #[error("production audit service is unavailable")]
    Unavailable,
    #[error("production audit service could not durably flush admitted records")]
    PersistenceFailed,
}

#[derive(Debug, Error)]
pub enum ProductionAuditError {
    #[error("production audit files are already owned by another service")]
    AlreadyOwned,
    #[error("production audit path is not a regular file")]
    NonRegularFile,
    #[error("production audit JSON encoding failed")]
    Encoding,
    #[error("production audit record exceeds its fixed encoded bound")]
    RecordTooLarge,
    #[error("production audit record count overflowed")]
    RecordCountOverflow,
    #[error("production execution audit reader failed: {0}")]
    ExecutionReader(ExecutionAuditError),
    #[error("production audit I/O failed: {0}")]
    Io(std::io::Error),
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;
    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}
