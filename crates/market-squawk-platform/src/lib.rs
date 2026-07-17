//! Local configuration, confined paths, compatible journals, and asynchronous raw capture.

mod authority_state;
mod capture;
mod config;
mod journal;
mod paths;
mod raw_record;

pub use authority_state::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};
pub use capture::{
    CaptureDestination, CaptureDestinationError, CaptureGenerationError, CaptureHealthEvent,
    CaptureHealthReason, CaptureHealthSnapshot, CaptureIoContext, CapturePublishError,
    CaptureShutdownStatus, CaptureSink, CaptureSinkError, CaptureStorageErrorClass,
    CaptureWorkerReapError, CaptureWorkerTermination, CaptureWriterHandle, CaptureWriterOutcome,
    CaptureWriterPolicy, CaptureWriterPolicyError, CaptureWriterSpawnError, CapturedRawRecord,
    DiagnosticCaptureBundle, DiagnosticCaptureError, DiagnosticCaptureFrame,
    DiagnosticCaptureReceipt, MemoryCaptureSink, PendingCaptureWriter, RawCaptureControl,
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
