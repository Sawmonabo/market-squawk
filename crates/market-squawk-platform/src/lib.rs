//! Local configuration, confined paths, compatible journals, and asynchronous raw capture.

mod authority_state;
mod capture;
mod config;
mod journal;
mod paths;
mod raw_record;
mod secrets;

pub use authority_state::{
    AuthorityCommitContext, AuthorityStateSnapshot, LocalAuthorityStateStore,
    LocalAuthorityStateStoreError,
};
#[cfg(all(feature = "capture-test", debug_assertions))]
pub use capture::CaptureReceiverTestCoordinationError;
#[cfg(feature = "capture-benchmark")]
pub use capture::benchmark_support as capture_benchmark_support;
pub use capture::{
    CaptureAccountingSnapshot, CaptureAccountingSnapshotError, CaptureChannelError,
    CaptureChannelLimits, CaptureDestination, CaptureDestinationError,
    CaptureDestinationFenceError, CaptureGenerationError, CaptureHealthEvent, CaptureHealthReason,
    CaptureHealthSnapshot, CaptureIoContext, CaptureProcessInfrastructure,
    CaptureProcessInfrastructureLimits, CapturePublishError, CapturePublisherCloneError,
    CaptureQueueKind, CaptureShutdownStatus, CaptureSink, CaptureSinkError,
    CaptureStorageErrorClass, CaptureWorkerReapError, CaptureWorkerTermination,
    CaptureWriterHandle, CaptureWriterOutcome, CaptureWriterPolicy, CaptureWriterPolicyError,
    CaptureWriterSpawnError, CapturedRawRecord, DestinationFenceRegistryInitializationError,
    DestinationFenceRegistryPermanentInitializationError, DiagnosticCaptureBundle,
    DiagnosticCaptureError, DiagnosticCaptureFrame, DiagnosticCaptureReceipt, MemoryCaptureSink,
    MemoryCaptureSinkConstructionError, PendingCaptureWriter, RawCaptureChannel, RawCaptureControl,
    RawCapturePublisher, RawCaptureWriter, WriterFixedStorageReceipt, WriterRuntimeProofError,
    initialize_capture_process_infrastructure, raw_capture_channel, spawn_capture_writer,
};
pub use config::{
    AppConfig, COINBASE_EXCHANGE_ENDPOINT, CoinbaseAuthorizationAttestation,
    CoinbaseConfigurationError, CoinbaseControlLimits, CoinbaseInstrumentMapping,
    CoinbaseSourceConfig, ConfigError, ConfigOverrides, ConfigSources, SecretError, SecretProvider,
    SecretReference, SecretValue,
};
pub use journal::{
    JournalError, JournalReader, JournalReplayAuthority, JournalSinkConstructionError,
    JournalSinkLimits, JournalWriter,
};
pub use paths::{
    ArtifactPathError, ArtifactRoot, CatalogFileGuard, CatalogLocation, CatalogRestoreScanGuard,
    CatalogRestoreStage, CatalogRestoreTarget, CatalogWriterGuard, InstalledCatalogFile,
    JournalFileFormat, JournalSelectionError, LocalPaths, PathError, ResolvedArtifactPath,
};
pub use raw_record::{RawCaptureRecord, RawCaptureRecordError};
pub use secrets::{
    EncryptedFileSecretStore, LocalSecretStoreError, OsKeyringSecretStore, RotationAuthority,
    RotationOutcome, SecretKey, SecretStore,
};
