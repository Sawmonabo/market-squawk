//! Local configuration, confined paths, compatible journals, and asynchronous raw capture.

mod capture;
mod config;
mod journal;
mod paths;
mod raw_record;

pub use capture::{
    CaptureGenerationError, CaptureHealthEvent, CaptureHealthReason, CaptureHealthSnapshot,
    CapturePublishError, CaptureShutdown, CaptureSink, CaptureSinkError, CaptureStorageErrorClass,
    CaptureWriterHandle, CaptureWriterOutcome, CaptureWriterPolicy, CaptureWriterPolicyError,
    CaptureWriterSpawnError, CapturedRawRecord, DiagnosticCaptureBundle, DiagnosticCaptureError,
    DiagnosticCaptureFrame, DiagnosticCaptureReceipt, MemoryCaptureSink, RawCaptureControl,
    RawCapturePublisher, RawCaptureWriter, raw_capture_channel, spawn_capture_writer,
};
pub use config::{
    AppConfig, ConfigError, ConfigOverrides, ConfigSources, SecretError, SecretProvider,
    SecretReference, SecretValue,
};
pub use journal::{JournalError, JournalReader, JournalReplayAuthority, JournalWriter};
pub use paths::{
    ArtifactPathError, ArtifactRoot, JournalFileFormat, JournalSelectionError, LocalPaths,
    PathError, ResolvedArtifactPath,
};
pub use raw_record::{RawCaptureRecord, RawCaptureRecordError};
