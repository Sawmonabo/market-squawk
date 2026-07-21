//! Parent-side helper launch, handshake, and synchronous capture-sink protocol.

use std::io::{BufReader, BufWriter, Write};
use std::process::{ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use thiserror::Error;
use uuid::Uuid;

#[cfg(all(feature = "capture-test", debug_assertions))]
use super::config::ProcessCaptureHelperTestBehavior;
use super::config::ProcessJournalCaptureConfig;
#[cfg(all(feature = "capture-test", debug_assertions))]
use super::helper::{test_mode_environment, test_stall_after_append};
use super::process::{ProcessOwner, ProcessSupervisionError, TerminalReaperReservation};
use super::protocol::{
    CountingDigestWriter, Header, MessageKind, ProtocolError, VerifyingForwardWriter,
    control_digest, startup_digest,
};
use crate::capture::CapturedRawRecord;
use crate::capture::writer::{
    CaptureDestination, CaptureIoContext, CaptureSink, CaptureSinkError, CaptureStorageErrorClass,
};

const STARTUP_REAP_POLL_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Debug)]
pub(super) struct StartedProcessJournalSink {
    pub(super) sink: ProcessJournalSink,
    pub(super) process: ProcessOwner,
    pub(super) reaper: TerminalReaperReservation,
}

#[derive(Debug)]
pub(super) struct ProcessJournalSink {
    destination: CaptureDestination,
    input: BufWriter<ChildStdin>,
    output: BufReader<ChildStdout>,
    next_sequence: u64,
    shutdown_sent: bool,
}

impl ProcessJournalSink {
    pub(super) fn try_start(
        config: ProcessJournalCaptureConfig,
    ) -> Result<StartedProcessJournalSink, ProcessJournalSinkStartError> {
        let reaper = TerminalReaperReservation::try_acquire()?;
        let nonce = Uuid::new_v4();
        let mut command = Command::new(config.executable());
        command
            .arg(config.root())
            .arg(config.source())
            .arg(nonce.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .env_clear();
        #[cfg(all(feature = "capture-test", debug_assertions))]
        if matches!(
            config.test_behavior(),
            Some(ProcessCaptureHelperTestBehavior::StallAfterAppend)
        ) {
            command.env(test_mode_environment(), test_stall_after_append());
        }
        let mut child = command
            .spawn()
            .map_err(ProcessJournalSinkStartError::HelperLaunch)?;
        let input = match child.stdin.take() {
            Some(input) => input,
            None => return Err(terminate_unowned_child(child)),
        };
        let output = match child.stdout.take() {
            Some(output) => output,
            None => return Err(terminate_unowned_child(child)),
        };
        let process = ProcessOwner::try_start(child)?;
        let (sender, receiver) = mpsc::sync_channel(1);
        let startup_thread = match std::thread::Builder::new()
            .name("msq-capture-ready".to_owned())
            .stack_size(128 * 1024)
            .spawn(move || {
                let mut output = BufReader::new(output);
                let result = Header::read_from(&mut output).map(|header| (header, output));
                let _sent = sender.send(result);
            }) {
            Ok(startup_thread) => startup_thread,
            Err(source) => {
                process.kill();
                wait_for_startup_cleanup(process, None, reaper, config.startup_deadline());
                return Err(ProcessJournalSinkStartError::StartupThread(source));
            }
        };
        let startup = receiver.recv_timeout(config.startup_deadline());
        let (header, output) = match startup {
            Ok(Ok(startup)) => startup,
            Ok(Err(_error)) => {
                process.kill();
                wait_for_startup_cleanup(
                    process,
                    Some(startup_thread),
                    reaper,
                    config.startup_deadline(),
                );
                return Err(ProcessJournalSinkStartError::StartupProtocol);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                process.kill();
                wait_for_startup_cleanup(
                    process,
                    Some(startup_thread),
                    reaper,
                    config.startup_deadline(),
                );
                return Err(ProcessJournalSinkStartError::StartupDeadline);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                process.kill();
                wait_for_startup_cleanup(
                    process,
                    Some(startup_thread),
                    reaper,
                    config.startup_deadline(),
                );
                return Err(ProcessJournalSinkStartError::StartupProtocol);
            }
        };
        if header.kind != MessageKind::Ready
            || header.sequence != 0
            || header.payload_bytes != 0
            || header.digest != startup_digest(nonce.as_bytes())
        {
            drop(output);
            process.kill();
            wait_for_startup_cleanup(
                process,
                Some(startup_thread),
                reaper,
                config.startup_deadline(),
            );
            return Err(ProcessJournalSinkStartError::StartupProtocol);
        }
        if startup_thread.join().is_err() {
            drop(output);
            process.kill();
            wait_for_startup_cleanup(process, None, reaper, config.startup_deadline());
            return Err(ProcessJournalSinkStartError::StartupThreadPanicked);
        }
        Ok(StartedProcessJournalSink {
            sink: Self {
                destination: config.destination().clone(),
                input: BufWriter::new(input),
                output,
                next_sequence: 1,
                shutdown_sent: false,
            },
            process,
            reaper,
        })
    }

    fn append_record(&mut self, record: &CapturedRawRecord) -> Result<(), CaptureSinkError> {
        let sequence = self.next_sequence;
        let mut measurement = CountingDigestWriter::new();
        serde_json::to_writer(&mut measurement, record.record())
            .map_err(|_error| storage_error(CaptureStorageErrorClass::Corruption))?;
        let (payload_bytes, digest) = measurement.finish();
        Header::try_new(MessageKind::Append, sequence, payload_bytes, digest)
            .and_then(|header| header.write_to(&mut self.input))
            .map_err(protocol_storage_error)?;
        let mut forwarding = VerifyingForwardWriter::new(&mut self.input, payload_bytes);
        serde_json::to_writer(&mut forwarding, record.record())
            .map_err(|_error| storage_error(CaptureStorageErrorClass::Corruption))?;
        let (observed_bytes, observed_digest) = forwarding.finish();
        if observed_bytes != payload_bytes || observed_digest != digest {
            return Err(storage_error(CaptureStorageErrorClass::Corruption));
        }
        self.input
            .flush()
            .map_err(|_error| storage_error(CaptureStorageErrorClass::Unavailable))?;
        self.read_acknowledgement(sequence, digest)?;
        self.advance_sequence()
    }

    fn control(&mut self, kind: MessageKind) -> Result<(), CaptureSinkError> {
        let sequence = self.next_sequence;
        let digest = control_digest(kind, sequence);
        Header::try_new(kind, sequence, 0, digest)
            .and_then(|header| header.write_to(&mut self.input))
            .map_err(protocol_storage_error)?;
        self.input
            .flush()
            .map_err(|_error| storage_error(CaptureStorageErrorClass::Unavailable))?;
        self.read_acknowledgement(sequence, digest)?;
        self.advance_sequence()
    }

    fn read_acknowledgement(
        &mut self,
        sequence: u64,
        digest: [u8; 32],
    ) -> Result<(), CaptureSinkError> {
        let acknowledgement =
            Header::read_from(&mut self.output).map_err(protocol_storage_error)?;
        if acknowledgement.kind != MessageKind::Acknowledged
            || acknowledgement.sequence != sequence
            || acknowledgement.payload_bytes != 0
            || acknowledgement.digest != digest
        {
            return Err(storage_error(CaptureStorageErrorClass::Corruption));
        }
        Ok(())
    }

    fn advance_sequence(&mut self) -> Result<(), CaptureSinkError> {
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| storage_error(CaptureStorageErrorClass::Capacity))?;
        Ok(())
    }
}

impl CaptureSink for ProcessJournalSink {
    fn destination(&self) -> CaptureDestination {
        self.destination.clone()
    }

    fn append(
        &mut self,
        record: &CapturedRawRecord,
        context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        context.checkpoint()?;
        self.append_record(record)
    }

    fn flush(&mut self, context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        context.checkpoint()?;
        self.control(MessageKind::Flush)
    }
}

impl Drop for ProcessJournalSink {
    fn drop(&mut self) {
        if !self.shutdown_sent {
            self.shutdown_sent = true;
            let _shutdown = self.control(MessageKind::Shutdown);
        }
    }
}

fn wait_for_startup_cleanup(
    mut process: ProcessOwner,
    bootstrap: Option<JoinHandle<()>>,
    reaper: TerminalReaperReservation,
    deadline: Duration,
) {
    let expires = Instant::now()
        .checked_add(deadline)
        .unwrap_or_else(Instant::now);
    while !process.is_reaped() && Instant::now() < expires {
        std::thread::sleep(STARTUP_REAP_POLL_INTERVAL);
    }
    if process.is_reaped() {
        let _joined = process.join_if_reaped();
        if let Some(bootstrap) = bootstrap {
            let _bootstrap = bootstrap.join();
        }
        return;
    }
    if let Some(supervisor) = process.take_supervisor() {
        reaper.retain(supervisor, bootstrap);
    }
}

fn terminate_unowned_child(mut child: std::process::Child) -> ProcessJournalSinkStartError {
    let _killed = child.kill();
    let _reaped = child.wait();
    ProcessJournalSinkStartError::MissingPipe
}

fn storage_error(class: CaptureStorageErrorClass) -> CaptureSinkError {
    CaptureSinkError::storage(class)
}

fn protocol_storage_error(error: ProtocolError) -> CaptureSinkError {
    match error {
        ProtocolError::Io(_error) => storage_error(CaptureStorageErrorClass::Unavailable),
        ProtocolError::InvalidHeader
        | ProtocolError::UnsupportedVersion
        | ProtocolError::UnknownKind
        | ProtocolError::PayloadTooLarge => storage_error(CaptureStorageErrorClass::Corruption),
    }
}

#[derive(Debug, Error)]
pub(super) enum ProcessJournalSinkStartError {
    #[error("capture helper launch failed")]
    HelperLaunch(#[source] std::io::Error),
    #[error("capture helper did not expose its configured standard pipes")]
    MissingPipe,
    #[error("capture helper startup reader could not be created")]
    StartupThread(#[source] std::io::Error),
    #[error("capture helper startup reader panicked")]
    StartupThreadPanicked,
    #[error("capture helper startup deadline elapsed")]
    StartupDeadline,
    #[error("capture helper startup protocol validation failed")]
    StartupProtocol,
    #[error(transparent)]
    Supervision(#[from] ProcessSupervisionError),
}
