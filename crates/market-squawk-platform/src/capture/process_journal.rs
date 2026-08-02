//! Process-isolated durable journal capture with bounded kill-and-reap shutdown.

mod config;
mod helper;
mod lifecycle;
mod process;
mod protocol;
mod sink;

#[cfg(all(feature = "capture-test", debug_assertions))]
pub use config::ProcessCaptureHelperTestBehavior;
pub use config::{ProcessJournalCaptureConfig, ProcessJournalCaptureConfigError};
#[doc(hidden)]
pub use helper::{CaptureHelperError, run_capture_helper};
pub use lifecycle::{
    ProcessCaptureShutdownDisposition, ProcessCaptureShutdownOutcome, ProcessCaptureShutdownPolicy,
    ProcessCaptureShutdownPolicyError, ProcessCaptureWriterSpawnError, ProcessJournalCaptureWriter,
    spawn_process_journal_capture_writer,
};
