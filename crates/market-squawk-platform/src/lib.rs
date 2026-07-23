//! Local configuration, confined paths, compatible journals, and asynchronous raw capture.

mod authority_state;
mod capture;
mod config;
mod input;
mod journal;
mod paths;
mod raw_record;
mod secrets;

pub use authority_state::{
    AuthorityCommitContext, AuthorityStateSnapshot, LocalAuthorityStateStore,
    LocalAuthorityStateStoreError,
};
#[cfg(feature = "capture-benchmark")]
pub use capture::benchmark_support as capture_benchmark_support;
pub use capture::{
    CaptureAccountingSnapshot, CaptureAccountingSnapshotError, CaptureChannelError,
    CaptureChannelLimits, CaptureDestination, CaptureDestinationError,
    CaptureDestinationFenceError, CaptureGenerationError, CaptureHealthEvent, CaptureHealthReason,
    CaptureHealthSnapshot, CaptureHelperError, CaptureIoContext, CaptureProcessInfrastructure,
    CaptureProcessInfrastructureLimits, CapturePublishError, CapturePublisherCloneError,
    CaptureQueueKind, CaptureShutdownStatus, CaptureSink, CaptureSinkError,
    CaptureStorageErrorClass, CaptureWorkerReapError, CaptureWorkerTermination,
    CaptureWriterHandle, CaptureWriterOutcome, CaptureWriterPolicy, CaptureWriterPolicyError,
    CaptureWriterSpawnError, CapturedRawRecord, DestinationFenceRegistryInitializationError,
    DestinationFenceRegistryPermanentInitializationError, DiagnosticCaptureBundle,
    DiagnosticCaptureError, DiagnosticCaptureFrame, DiagnosticCaptureReceipt, MemoryCaptureSink,
    MemoryCaptureSinkConstructionError, PendingCaptureWriter, ProcessCaptureShutdownDisposition,
    ProcessCaptureShutdownOutcome, ProcessCaptureShutdownPolicy, ProcessCaptureShutdownPolicyError,
    ProcessCaptureWriterSpawnError, ProcessJournalCaptureConfig, ProcessJournalCaptureConfigError,
    ProcessJournalCaptureWriter, RawCaptureChannel, RawCaptureControl, RawCapturePublisher,
    RawCaptureWriter, WriterFixedStorageReceipt, WriterRuntimeProofError,
    initialize_capture_process_infrastructure, raw_capture_channel, run_capture_helper,
    spawn_capture_writer, spawn_process_journal_capture_writer,
};
#[cfg(all(feature = "capture-test", debug_assertions))]
pub use capture::{CaptureReceiverTestCoordinationError, ProcessCaptureHelperTestBehavior};
pub use config::{
    AppConfig, COINBASE_EXCHANGE_ENDPOINT, CoinbaseAuthorizationAttestation,
    CoinbaseConfigurationError, CoinbaseControlLimits, CoinbaseInstrumentMapping,
    CoinbaseSourceConfig, ConfigError, ConfigOrigin, ConfigOverrides, ConfigProvenance,
    ConfigSetting, ConfigSources, EffectiveConfig, KRAKEN_WEBSOCKET_V2_ENDPOINT,
    KrakenAuthorizationAttestation, KrakenConfigurationError, KrakenInstrumentMapping,
    KrakenSourceConfig, SecretError, SecretProvider, SecretReference, SecretValue,
};
pub use input::{
    BoundedInput, ControlledInputFileError, InputFileCapability, InputFileError, InputFileIdentity,
    InputReadCheckpoint, InputReadControl, InputReadControlError, InputReadPass,
    UserAuthorizedInputRoot, UserOwnedInputAuthority, UserOwnedInputEvidence,
    UserOwnedInputRootIdentityDigest, VerifiedInputFile,
};
pub use journal::{
    JournalError, JournalReader, JournalReplayAuthority, JournalSinkConstructionError,
    JournalSinkLimits, JournalWriter,
};
pub use paths::{
    ArtifactPathError, ArtifactRoot, CatalogFileGuard, CatalogLocation, CatalogRestoreScanGuard,
    CatalogRestoreStage, CatalogRestoreTarget, CatalogWriterGuard, ConfiguredJournalRead,
    ConfiguredJournalReadTarget, ControlRoot, InstalledCatalogFile, JournalFileFormat,
    JournalOpenError, JournalSelectionError, LocalPaths, PathError, ResolvedArtifactPath,
};
pub use raw_record::{RawCaptureRecord, RawCaptureRecordError};
pub use secrets::{
    EncryptedFileSecretStore, LocalSecretStoreError, OsKeyringSecretStore, RotationAuthority,
    RotationOutcome, SecretBackend, SecretCancellation, SecretDeadlineCapability, SecretGeneration,
    SecretInteractionCapability, SecretInteractionPolicy, SecretKey, SecretOperationControl,
    SecretRef, SecretStore, SecretStoreCapabilities,
};
