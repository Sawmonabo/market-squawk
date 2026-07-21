//! Public catalog records, bounds, and typed failures.

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use market_squawk_domain::{
    ContractRollMapping, CorporateActionObservation, EvidenceDigest, InstrumentDefinition,
    LifecycleTransition, SchemaVersion, SourceId, SourceIdentifier, SymbolIdentityRecord,
    Timestamp,
};
use market_squawk_platform::{CatalogFileGuard, CatalogLocation, CatalogWriterGuard};
use market_squawk_sources::SourceMetadataError;
use rusqlite::Connection;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use uuid::Uuid;

use super::storage::{valid_artifact_reference, valid_text};
use crate::RightsError;

const MAX_QUERY_ROWS: usize = 10_000;
const MIN_SQLITE_RECORD_BYTES: usize = 8 * 1024;
pub(super) const MAX_SQLITE_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_QUERY_RESULT_BYTES: usize = 64 * 1024 * 1024;
const MAX_BUSY_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CURSOR_NAME_BYTES: usize = 128;
const MAX_CURSOR_VALUE_BYTES: usize = 4 * 1024;
const ARTIFACT_ID_NAMESPACE: Uuid = Uuid::from_u128(0x62ef3f5d_705e_4dd5_970f_55fbf8524d20);
const MANIFEST_ID_NAMESPACE: Uuid = Uuid::from_u128(0x92a5a273_c17b_4bbf_9d99_b99bf41b606b);

static OPEN_CATALOGS: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();

/// Explicit, bounded local catalog configuration.
#[derive(Clone)]
pub struct CatalogConfig {
    pub(super) location: CatalogLocation,
    pub(super) busy_timeout: Duration,
    pub(super) max_result_rows: CatalogLimit,
    pub(super) result_bytes: CatalogResultLimits,
}

impl CatalogConfig {
    /// Constructs a local path, busy timeout, and per-query maximum result bound.
    pub fn try_new(
        location: CatalogLocation,
        busy_timeout: Duration,
        max_result_rows: CatalogLimit,
        result_bytes: CatalogResultLimits,
    ) -> Result<Self, CatalogError> {
        if busy_timeout.is_zero() || busy_timeout > MAX_BUSY_TIMEOUT {
            return Err(CatalogError::InvalidConfiguration);
        }
        Ok(Self {
            location,
            busy_timeout,
            max_result_rows,
            result_bytes,
        })
    }

    pub(crate) const fn location(&self) -> &CatalogLocation {
        &self.location
    }
}

impl fmt::Debug for CatalogConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogConfig")
            .field("path", &"[LOCAL PATH REDACTED]")
            .field("busy_timeout", &self.busy_timeout)
            .field("max_result_rows", &self.max_result_rows)
            .field("result_bytes", &self.result_bytes)
            .finish()
    }
}

/// Independent per-record and cumulative decoded-result byte bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogResultLimits {
    max_record_bytes: NonZeroUsize,
    max_result_bytes: NonZeroUsize,
}

impl CatalogResultLimits {
    /// Constructs bounded SQLite record and decoded query-result limits.
    pub fn try_new(max_record_bytes: usize, max_result_bytes: usize) -> Result<Self, CatalogError> {
        let max_record_bytes = NonZeroUsize::new(max_record_bytes)
            .filter(|value| {
                (MIN_SQLITE_RECORD_BYTES..=MAX_SQLITE_RECORD_BYTES).contains(&value.get())
            })
            .ok_or(CatalogError::InvalidConfiguration)?;
        let max_result_bytes = NonZeroUsize::new(max_result_bytes)
            .filter(|value| value.get() <= MAX_QUERY_RESULT_BYTES)
            .ok_or(CatalogError::InvalidConfiguration)?;
        Ok(Self {
            max_record_bytes,
            max_result_bytes,
        })
    }

    pub(super) const fn max_record_bytes(self) -> usize {
        self.max_record_bytes.get()
    }

    pub(super) const fn max_result_bytes(self) -> usize {
        self.max_result_bytes.get()
    }
}

/// A nonzero, globally bounded catalog result limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogLimit(NonZeroUsize);

impl CatalogLimit {
    /// Constructs a result bound no greater than 10,000 records.
    pub fn new(value: usize) -> Result<Self, CatalogError> {
        NonZeroUsize::new(value)
            .filter(|value| value.get() <= MAX_QUERY_ROWS)
            .map(Self)
            .ok_or(CatalogError::InvalidLimit)
    }

    pub(super) const fn get(self) -> usize {
        self.0.get()
    }
}

/// Defensive SQLite state observed after catalog initialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogHealth {
    pub(super) journal_mode: String,
    pub(super) foreign_keys: bool,
    pub(super) trusted_schema: bool,
    pub(super) synchronous: i64,
    pub(super) busy_timeout: Duration,
    pub(super) applied_migrations: u32,
}

impl CatalogHealth {
    /// Returns the normalized journal mode.
    pub fn journal_mode(&self) -> &str {
        &self.journal_mode
    }

    /// Returns whether foreign-key enforcement is active.
    pub const fn foreign_keys(&self) -> bool {
        self.foreign_keys
    }

    /// Returns whether SQLite permits schema-controlled unsafe functions.
    pub const fn trusted_schema(&self) -> bool {
        self.trusted_schema
    }

    /// Returns SQLite's synchronous policy integer (`2` is `FULL`).
    pub const fn synchronous(&self) -> i64 {
        self.synchronous
    }

    /// Returns the configured bounded lock wait.
    pub const fn busy_timeout(&self) -> Duration {
        self.busy_timeout
    }

    /// Returns the number of digest-verified applied migrations.
    pub const fn applied_migrations(&self) -> u32 {
        self.applied_migrations
    }
}

/// Durable source cursor with exact UTC nanosecond update time.
#[derive(Clone, Eq, PartialEq)]
pub struct SourceCursor {
    pub(super) source_id: SourceId,
    pub(super) name: String,
    pub(super) value: String,
    pub(super) updated_at: Timestamp,
}

impl fmt::Debug for SourceCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceCursor")
            .field("source_id", &self.source_id)
            .field("name", &self.name)
            .field("value", &"[REDACTED OPAQUE CURSOR]")
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

impl SourceCursor {
    /// Constructs a bounded provider cursor.
    pub fn try_new(
        source_id: SourceId,
        name: impl Into<String>,
        value: impl Into<String>,
        updated_at: Timestamp,
    ) -> Result<Self, CatalogError> {
        let name = name.into();
        let value = value.into();
        if !valid_text(&name, MAX_CURSOR_NAME_BYTES) || !valid_text(&value, MAX_CURSOR_VALUE_BYTES)
        {
            return Err(CatalogError::InvalidRecord);
        }
        Ok(Self {
            source_id,
            name,
            value,
            updated_at,
        })
    }

    /// Returns the source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the cursor namespace.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the opaque provider cursor value.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Returns cursor update time.
    pub const fn updated_at(&self) -> Timestamp {
        self.updated_at
    }
}

/// Immutable metadata for one controlled artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactRecord {
    pub(super) artifact_id: Uuid,
    pub(super) relative_reference: String,
    pub(super) content_digest: EvidenceDigest,
    pub(super) size_bytes: u64,
    pub(super) created_at: Timestamp,
}

impl ArtifactRecord {
    /// Constructs a bounded portable relative reference and exact content identity.
    pub fn try_new(
        relative_reference: impl Into<String>,
        content_digest: EvidenceDigest,
        size_bytes: u64,
        created_at: Timestamp,
    ) -> Result<Self, CatalogError> {
        let relative_reference = relative_reference.into();
        if !valid_artifact_reference(&relative_reference) || i64::try_from(size_bytes).is_err() {
            return Err(CatalogError::InvalidRecord);
        }
        Ok(Self {
            artifact_id: deterministic_artifact_id(&relative_reference, content_digest, size_bytes),
            relative_reference,
            content_digest,
            size_bytes,
            created_at,
        })
    }

    /// Returns the opaque artifact identity.
    pub const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    /// Returns the portable path below the controlled artifact root.
    pub fn relative_reference(&self) -> &str {
        &self.relative_reference
    }

    /// Returns the exact artifact content digest.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns the exact artifact size in bytes.
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Returns artifact creation time.
    pub const fn created_at(&self) -> Timestamp {
        self.created_at
    }

    pub(super) fn try_from_stored(
        artifact_id: Uuid,
        relative_reference: String,
        content_digest: EvidenceDigest,
        size_bytes: u64,
        created_at: Timestamp,
    ) -> Result<Self, CatalogError> {
        let record = Self::try_new(relative_reference, content_digest, size_bytes, created_at)
            .map_err(|_| CatalogError::CorruptCatalog)?;
        if record.artifact_id == artifact_id {
            Ok(record)
        } else {
            Err(CatalogError::CorruptCatalog)
        }
    }
}

/// Immutable manifest metadata retained before analytical storage is introduced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetManifestRecord {
    pub(super) manifest_id: Uuid,
    pub(super) dataset_name: SourceIdentifier,
    pub(super) schema_version: SchemaVersion,
    pub(super) artifact_id: Uuid,
    pub(super) content_digest: EvidenceDigest,
    pub(super) created_at: Timestamp,
}

impl DatasetManifestRecord {
    /// Constructs manifest metadata bound to one already controlled artifact.
    pub fn try_new(
        dataset_name: SourceIdentifier,
        schema_version: SchemaVersion,
        artifact_id: Uuid,
        content_digest: EvidenceDigest,
        created_at: Timestamp,
    ) -> Self {
        Self {
            manifest_id: deterministic_manifest_id(
                &dataset_name,
                schema_version,
                artifact_id,
                content_digest,
            ),
            dataset_name,
            schema_version,
            artifact_id,
            content_digest,
            created_at,
        }
    }

    /// Returns the opaque manifest identity.
    pub const fn manifest_id(&self) -> Uuid {
        self.manifest_id
    }

    /// Returns the stable dataset name.
    pub const fn dataset_name(&self) -> &SourceIdentifier {
        &self.dataset_name
    }

    /// Returns the manifest's dataset schema version.
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns the controlled artifact referenced by this manifest.
    pub const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    /// Returns the exact manifest-content digest.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns manifest creation time.
    pub const fn created_at(&self) -> Timestamp {
        self.created_at
    }

    pub(super) fn try_from_stored(
        manifest_id: Uuid,
        dataset_name: SourceIdentifier,
        schema_version: SchemaVersion,
        artifact_id: Uuid,
        content_digest: EvidenceDigest,
        created_at: Timestamp,
    ) -> Result<Self, CatalogError> {
        let record = Self::try_new(
            dataset_name,
            schema_version,
            artifact_id,
            content_digest,
            created_at,
        );
        if record.manifest_id == manifest_id {
            Ok(record)
        } else {
            Err(CatalogError::CorruptCatalog)
        }
    }
}

fn deterministic_artifact_id(reference: &str, digest: EvidenceDigest, size_bytes: u64) -> Uuid {
    let mut identity = Sha256::new();
    update_canonical_text(&mut identity, reference);
    update_canonical_digest(&mut identity, digest);
    identity.update(size_bytes.to_be_bytes());
    Uuid::new_v5(&ARTIFACT_ID_NAMESPACE, &identity.finalize())
}

fn deterministic_manifest_id(
    dataset_name: &SourceIdentifier,
    schema_version: SchemaVersion,
    artifact_id: Uuid,
    digest: EvidenceDigest,
) -> Uuid {
    let mut identity = Sha256::new();
    update_canonical_text(&mut identity, dataset_name.as_str());
    identity.update(schema_version.get().to_be_bytes());
    identity.update(artifact_id.as_bytes());
    update_canonical_digest(&mut identity, digest);
    Uuid::new_v5(&MANIFEST_ID_NAMESPACE, &identity.finalize())
}

fn update_canonical_text(identity: &mut Sha256, value: &str) {
    identity.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    identity.update(value.as_bytes());
}

fn update_canonical_digest(identity: &mut Sha256, digest: EvidenceDigest) {
    identity.update([match digest.algorithm() {
        market_squawk_domain::DigestAlgorithm::Sha256 => 1,
        market_squawk_domain::DigestAlgorithm::Blake3 => 2,
    }]);
    identity.update(digest.bytes());
}

/// A successfully rights-admitted run reservation.
#[derive(Clone, Eq, PartialEq)]
pub struct IngestReservation {
    pub(super) run_id: Uuid,
    pub(super) requested_at: Timestamp,
    pub(super) catalog_id: Uuid,
}

impl IngestReservation {
    /// Returns the opaque run identity used by downstream publication.
    pub const fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Returns the trusted local reservation time.
    pub const fn requested_at(&self) -> Timestamp {
        self.requested_at
    }
}

impl fmt::Debug for IngestReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IngestReservation")
            .field("run_id", &self.run_id)
            .field("requested_at", &self.requested_at)
            .field("catalog_capability", &"[SEALED]")
            .finish()
    }
}

/// Durable ingest-run state used by crash recovery and reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestRunState {
    /// Rights were admitted and downstream work may need recovery.
    Reserved,
    /// Publication or the non-persisting operation completed successfully.
    Succeeded,
    /// The operation completed unsuccessfully.
    Failed,
}

/// Immutable run identity plus its single mutable terminal transition.
#[derive(Clone, Eq, PartialEq)]
pub struct IngestRunRecord {
    pub(super) run_id: Uuid,
    pub(super) idempotency_key: String,
    pub(super) source_id: SourceId,
    pub(super) payload_digest: EvidenceDigest,
    pub(super) operation: crate::SourceOperation,
    pub(super) rights_id: [u8; 32],
    pub(super) state: IngestRunState,
    pub(super) requested_at: Timestamp,
    pub(super) completed_at: Option<Timestamp>,
}

impl IngestRunRecord {
    /// Returns the opaque run identity.
    pub const fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Returns the opaque idempotency key for authorized recovery logic.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    /// Returns the source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact gated payload digest.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns the rights-gated operation.
    pub const fn operation(&self) -> crate::SourceOperation {
        self.operation
    }

    /// Returns the immutable rights-decision fingerprint.
    pub const fn rights_id(&self) -> [u8; 32] {
        self.rights_id
    }

    /// Returns the durable run state.
    pub const fn state(&self) -> IngestRunState {
        self.state
    }

    /// Returns request time.
    pub const fn requested_at(&self) -> Timestamp {
        self.requested_at
    }

    /// Returns terminal completion time, when present.
    pub const fn completed_at(&self) -> Option<Timestamp> {
        self.completed_at
    }
}

impl fmt::Debug for IngestRunRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IngestRunRecord")
            .field("run_id", &self.run_id)
            .field("idempotency_key", &"[REDACTED OPAQUE KEY]")
            .field("source_id", &self.source_id)
            .field("payload_digest", &self.payload_digest)
            .field("operation", &self.operation)
            .field("rights_id", &self.rights_id)
            .field("state", &self.state)
            .field("requested_at", &self.requested_at)
            .field("completed_at", &self.completed_at)
            .finish()
    }
}

/// Terminal ingest outcome recorded by the catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractCompletion {
    /// The reserved ingest and its required publication succeeded.
    Succeeded,
    /// The reserved ingest failed without claiming publication.
    Failed,
}

impl ContractCompletion {
    pub(super) const fn database_name(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

/// Bounded durable instrument and corporate-action reference history.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReferenceBundle {
    pub(super) instrument: Option<InstrumentDefinition>,
    pub(super) symbols: Vec<SymbolIdentityRecord>,
    pub(super) lifecycle: Vec<LifecycleTransition>,
    pub(super) contract_rolls: Vec<ContractRollMapping>,
    pub(super) corporate_actions: Vec<CorporateActionObservation>,
}

impl ReferenceBundle {
    /// Returns the current instrument definition.
    pub const fn instrument(&self) -> Option<&InstrumentDefinition> {
        self.instrument.as_ref()
    }

    /// Returns bounded venue-symbol history.
    pub fn symbols(&self) -> &[SymbolIdentityRecord] {
        &self.symbols
    }

    /// Returns bounded merger and delisting history.
    pub fn lifecycle(&self) -> &[LifecycleTransition] {
        &self.lifecycle
    }

    /// Returns bounded futures roll mappings.
    pub fn contract_rolls(&self) -> &[ContractRollMapping] {
        &self.contract_rolls
    }

    /// Returns bounded canonical corporate actions.
    pub fn corporate_actions(&self) -> &[CorporateActionObservation] {
        &self.corporate_actions
    }
}

/// Immutable application audit metadata without secret or payload bodies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditEvent {
    pub(super) sequence: u64,
    pub(super) event_type: String,
    pub(super) subject_id: String,
    pub(super) details_digest: EvidenceDigest,
    pub(super) occurred_at: Timestamp,
}

impl AuditEvent {
    /// Returns the monotonic catalog-local audit sequence.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the stable audit event class.
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    /// Returns the event subject's opaque identifier.
    pub fn subject_id(&self) -> &str {
        &self.subject_id
    }

    /// Returns the digest of non-secret event details.
    pub const fn details_digest(&self) -> EvidenceDigest {
        self.details_digest
    }

    /// Returns exact event time.
    pub const fn occurred_at(&self) -> Timestamp {
        self.occurred_at
    }
}

/// The sole process-local writer for one catalog path.
pub struct Catalog {
    pub(super) connection: Connection,
    pub(super) _catalog_file: CatalogFileGuard,
    pub(super) _cross_process_writer: CatalogWriterGuard,
    pub(super) _writer_permit: WriterPermit,
    pub(super) busy_timeout: Duration,
    pub(super) max_result_rows: CatalogLimit,
    pub(super) result_bytes: CatalogResultLimits,
    pub(super) catalog_id: Uuid,
    pub(super) artifact_root_binding: [u8; 32],
}

impl fmt::Debug for Catalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Catalog")
            .field("connection", &"[SQLITE CONNECTION]")
            .field("busy_timeout", &self.busy_timeout)
            .field("max_result_rows", &self.max_result_rows)
            .field("result_bytes", &self.result_bytes)
            .finish()
    }
}

impl Catalog {
    pub(super) fn enforce_limit(&self, limit: CatalogLimit) -> Result<(), CatalogError> {
        if limit.get() <= self.max_result_rows.get() {
            Ok(())
        } else {
            Err(CatalogError::InvalidLimit)
        }
    }
}

/// Catalog construction, integrity, admission, or durable-record failure.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// Catalog configuration is empty or outside explicit bounds.
    #[error("catalog configuration is invalid")]
    InvalidConfiguration,
    /// A query limit was zero or exceeded the global maximum.
    #[error("catalog result limit is invalid")]
    InvalidLimit,
    /// Decoded result bytes exceeded the configured per-record or cumulative bound.
    #[error("catalog result byte limit was exceeded")]
    ResultByteLimitExceeded,
    /// A durable record violated a bounded invariant.
    #[error("catalog record is invalid")]
    InvalidRecord,
    /// The catalog path or final file type was unsafe.
    #[error("catalog path is not a safe local regular file")]
    UnsafePath,
    /// One process-local writer already owns this catalog path.
    #[error("catalog already has a process-local writer")]
    WriterAlreadyOpen,
    /// The process-local writer registry was poisoned.
    #[error("catalog writer registry is unavailable")]
    WriterRegistryUnavailable,
    /// SQLite did not enable local WAL mode.
    #[error("catalog could not enable WAL mode")]
    UnsafeJournalMode,
    /// The path names a nonempty or explicitly identified non-Market-Squawk database.
    #[error("catalog path contains a foreign SQLite database")]
    ForeignCatalog,
    /// An applied migration's digest differs from the compiled registry.
    #[error("catalog migration {version} digest does not match this binary")]
    MigrationDigestMismatch {
        /// One-based migration version.
        version: i64,
    },
    /// The catalog contains an unrecognized migration version or invalid ordering.
    #[error("catalog migration registry is incompatible")]
    MigrationRegistryMismatch,
    /// SQLite integrity or foreign-key verification failed.
    #[error("catalog integrity verification failed")]
    CorruptCatalog,
    /// The immutable catalog/root authority pair differs from the retained live capabilities.
    #[error("catalog analytical artifact-root authority does not match")]
    ArtifactRootAuthorityMismatch,
    /// No authority lineage exists; explicit initialization is required before ordinary open.
    #[error("catalog analytical artifact-root authority requires explicit initialization")]
    ArtifactRootAuthorityInitializationRequired,
    /// The append-only authority event sequence, hash chain, or payload is invalid.
    #[error("catalog analytical artifact-root authority event chain is invalid")]
    ArtifactRootAuthorityChainInvalid,
    /// A repeated transition differs from the exact durable intent or result.
    #[error("catalog analytical artifact-root authority transition conflicts")]
    ArtifactRootAuthorityTransitionConflict,
    /// The catalog has no exact bound authority result eligible for ordinary activation.
    #[error("catalog analytical artifact-root authority is not bound")]
    ArtifactRootAuthorityNotBound,
    /// A legacy analytical catalog requires an explicit artifact-root migration.
    #[error("catalog analytical artifact-root migration is required")]
    ArtifactRootMigrationRequired,
    /// The requested operation was not admitted by exact rights evidence.
    #[error("catalog rights admission failed: {0}")]
    RightsDenied(#[from] RightsError),
    /// Rights evidence was not admitted and retained by this catalog authority.
    #[error("catalog rights evidence is not admitted")]
    RightsNotAdmitted,
    /// The supplied rights capability belongs to another catalog session.
    #[error("catalog rights capability is not valid for this session")]
    InvalidRightsCapability,
    /// The supplied reservation was not sealed by this open catalog session.
    #[error("catalog ingest reservation is not valid for this session")]
    InvalidReservationCapability,
    /// Shared composition authority could not be locked.
    #[error("catalog composition authority lock is unavailable")]
    AuthorityLockPoisoned,
    /// A query-artifact reservation expired before durable result binding.
    #[error("query artifact reservation is expired")]
    QueryArtifactExpired,
    /// A query-artifact receipt differs from its durable reservation.
    #[error("query artifact reservation does not match durable authority")]
    QueryArtifactReservationMismatch,
    /// Cancellation was observed while query-artifact binding could still roll back safely.
    #[error("query artifact binding was cancelled before durable commit")]
    QueryArtifactCancelled,
    /// The monotonic query deadline elapsed before artifact binding became durable.
    #[error("query artifact binding deadline elapsed before durable commit")]
    QueryArtifactDeadlineExceeded,
    /// The local wall clock moved behind the last committed authority decision.
    #[error("catalog authority clock rollback was detected")]
    AuthorityClockRollback,
    /// Rights evidence named a source that is not registered in this catalog.
    #[error("catalog rights evidence names an unknown source")]
    UnknownSource,
    /// A repeated idempotency key named different immutable input.
    #[error("catalog idempotency identity conflicts with a prior run")]
    IdempotencyConflict,
    /// A cursor update attempted to move durable progress backwards.
    #[error("catalog cursor update is older than durable progress")]
    StaleCursor,
    /// Equal cursor time carried a different opaque provider value.
    #[error("catalog cursor update conflicts at the same timestamp")]
    CursorConflict,
    /// A source revision attempted to replace newer durable metadata.
    #[error("catalog source revision is older than the current revision")]
    StaleSourceRevision,
    /// Distinct source revisions carried the same registration time.
    #[error("catalog source revisions conflict at the same timestamp")]
    SourceRevisionConflict,
    /// An instrument definition attempted to replace a newer durable definition.
    #[error("catalog instrument revision is older than the current revision")]
    StaleInstrumentRevision,
    /// Distinct instrument definitions carried the same observation time.
    #[error("catalog instrument revisions conflict at the same timestamp")]
    InstrumentRevisionConflict,
    /// An append identity already names different immutable evidence.
    #[error("catalog append identity conflicts with retained evidence")]
    EvidenceConflict,
    /// A referenced instrument was not registered.
    #[error("catalog reference names an unknown instrument")]
    UnknownInstrument,
    /// Artifact and manifest metadata named different controlled objects.
    #[error("catalog manifest does not reference the supplied artifact")]
    ManifestArtifactMismatch,
    /// Artifact or manifest creation time precedes its required durable predecessor.
    #[error("catalog publication timestamps are out of order")]
    PublicationTimeConflict,
    /// A publication did not name an active reserved run.
    #[error("catalog run is unknown or is not reserved")]
    RunStateConflict,
    /// Backups are never overwritten.
    #[error("catalog backup destination already exists")]
    BackupAlreadyExists,
    /// A bounded set of collision-resistant owned temporary names was unavailable.
    #[error("catalog backup temporary name is unavailable")]
    BackupTemporaryUnavailable,
    /// The platform cannot prove backup directory-entry durability.
    #[error("catalog backup durability is unsupported on this platform")]
    BackupDurabilityUnsupported,
    /// Publication occurred but final directory durability or verification was not proven.
    #[error("catalog backup publication outcome is indeterminate")]
    BackupPublicationIndeterminate {
        /// Exact receipt required to reconcile the surviving backup names.
        receipt: super::BackupReceipt,
    },
    /// The final backup was verified but cleanup of retained temporary evidence is pending.
    #[error("catalog backup is durable but publication cleanup is pending")]
    BackupPublishedWithCleanupPending {
        /// Exact receipt for the already durable final backup.
        receipt: super::BackupReceipt,
    },
    /// A backup no longer matches its exact byte-length and SHA-256 receipt.
    #[error("catalog backup does not match its receipt")]
    BackupReceiptMismatch,
    /// Another process retains a non-mutating lease over the backup file.
    #[error("catalog backup is already leased for another authority operation")]
    BackupLeaseUnavailable,
    /// A digest-addressed restore stage or final target contains different immutable bytes.
    #[error("catalog backup restore conflicts with retained destination state")]
    BackupRestoreConflict,
    /// Restore publication may have reached durable storage and requires exact receipt retry.
    #[error("catalog backup restore publication outcome is indeterminate")]
    BackupRestoreIndeterminate,
    /// Analytical evidence exceeded caller-selected or fixed process bounds.
    #[error("catalog analytical evidence resource limit was exceeded")]
    AnalyticalEvidenceLimitExceeded,
    /// Analytical evidence capture was cancelled before completion.
    #[error("catalog analytical evidence capture was cancelled")]
    AnalyticalEvidenceCancelled,
    /// Stored analytical relationships failed canonical semantic validation.
    #[error("catalog analytical evidence is invalid")]
    AnalyticalEvidenceInvalid,
    /// A bounded allocation failed.
    #[error("catalog bounded allocation failed")]
    Allocation,
    /// A filesystem operation failed.
    #[error("catalog filesystem operation failed")]
    Io(#[from] std::io::Error),
    /// SQLite rejected an operation or durable invariant.
    #[error("catalog SQLite operation failed")]
    Sqlite(#[from] rusqlite::Error),
    /// Canonical serialization or deserialization failed.
    #[error("catalog canonical serialization failed")]
    Serialization(#[from] serde_json::Error),
    /// Stored source metadata failed current validation.
    #[error("catalog source metadata is invalid")]
    SourceMetadata(#[from] SourceMetadataError),
}

impl CatalogError {
    /// Returns the exact receipt carried by an indeterminate backup publication.
    pub const fn backup_receipt(&self) -> Option<&super::BackupReceipt> {
        match self {
            Self::BackupPublicationIndeterminate { receipt }
            | Self::BackupPublishedWithCleanupPending { receipt } => Some(receipt),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub(super) struct WriterPermit {
    path: PathBuf,
}

impl WriterPermit {
    pub(super) fn acquire(path: PathBuf) -> Result<Self, CatalogError> {
        let registry = OPEN_CATALOGS.get_or_init(|| Mutex::new(BTreeSet::new()));
        let mut paths = registry
            .lock()
            .map_err(|_| CatalogError::WriterRegistryUnavailable)?;
        if !paths.insert(path.clone()) {
            return Err(CatalogError::WriterAlreadyOpen);
        }
        Ok(Self { path })
    }
}

impl Drop for WriterPermit {
    fn drop(&mut self) {
        let registry = OPEN_CATALOGS.get_or_init(|| Mutex::new(BTreeSet::new()));
        match registry.lock() {
            Ok(mut paths) => {
                paths.remove(&self.path);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(&self.path);
            }
        }
    }
}
