//! Capability-confined durable checkpoint publication and opaque persistence receipts.

use std::io::{Read as _, Write as _};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_platform::{ArtifactPathError, ArtifactRoot, PathError};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::snapshot::PaperCheckpointPersistenceEvidence;
use crate::{PaperCheckpointError, PaperExecutionCheckpoint, PaperExecutionConfig};

const CHECKPOINT_OBJECT_ROOT: &str = "paper-checkpoints/v1";

/// Single-writer durable publisher bound to one artifact root and paper configuration.
#[derive(Debug)]
pub struct PaperCheckpointRepository {
    root: ArtifactRoot,
    config: PaperExecutionConfig,
    maximum_bytes: NonZeroUsize,
    repository_id: [u8; 32],
    generation: u64,
}

impl PaperCheckpointRepository {
    /// Retains and validates one artifact capability and exact decode configuration.
    pub fn try_new(
        root: ArtifactRoot,
        config: PaperExecutionConfig,
        maximum_bytes: NonZeroUsize,
    ) -> Result<Self, PaperCheckpointRepositoryError> {
        drop(root.try_clone_directory()?);
        let mut nonce = [0_u8; 32];
        getrandom::fill(&mut nonce)
            .map_err(|_| PaperCheckpointRepositoryError::RandomUnavailable)?;
        let mut identity = Sha256::new();
        identity.update(b"market-squawk/paper-checkpoint-repository/v1\0");
        identity.update(config.digest());
        identity.update(nonce);
        let repository_id = identity.finalize().into();
        if repository_id == [0; 32] {
            return Err(PaperCheckpointRepositoryError::RandomUnavailable);
        }
        Ok(Self {
            root,
            config,
            maximum_bytes,
            repository_id,
            generation: 0,
        })
    }

    /// Publishes, synchronizes, reopens, and fully validates one immutable checkpoint.
    pub fn persist(
        &mut self,
        checkpoint: &PaperExecutionCheckpoint,
    ) -> Result<PaperCheckpointReceipt, PaperCheckpointRepositoryError> {
        self.persist_with_checkpoint(checkpoint, |_| Ok(()))
    }

    pub(crate) const fn binding_identity(&self) -> [u8; 32] {
        self.repository_id
    }

    pub(crate) fn binds_config(&self, config: &PaperExecutionConfig) -> bool {
        self.config.digest() == config.digest()
    }

    fn persist_with_checkpoint<F>(
        &mut self,
        checkpoint: &PaperExecutionCheckpoint,
        mut publication_checkpoint: F,
    ) -> Result<PaperCheckpointReceipt, PaperCheckpointRepositoryError>
    where
        F: FnMut(PaperCheckpointPublicationPoint) -> Result<(), PaperCheckpointRepositoryError>,
    {
        if checkpoint.schema_version() != PaperExecutionConfig::CHECKPOINT_SCHEMA_VERSION
            || checkpoint.configuration_digest() != self.config.digest()
            || !checkpoint.complete()
        {
            return Err(PaperCheckpointRepositoryError::ConfigurationMismatch);
        }
        let generation = self
            .generation
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(PaperCheckpointRepositoryError::GenerationExhausted)?;
        self.generation = generation.get();

        let maximum_bytes = self.maximum_bytes.get();
        let bytes = checkpoint.encode(maximum_bytes)?;
        let artifact_digest: [u8; 32] = Sha256::digest(&bytes).into();
        let recovery_digest = checkpoint.recovery_input_digest()?;
        let digest_hex = hex_bytes(&artifact_digest)?;
        let repository_hex = hex_bytes(&self.repository_id)?;
        let parent = format!("{CHECKPOINT_OBJECT_ROOT}/{}", &digest_hex[..2]);
        let artifact_reference = format!("{parent}/{digest_hex}.json");
        let staging_reference = format!("{parent}/stage-{repository_hex}-{}.tmp", generation.get());
        let artifact_path = Path::new(&artifact_reference);
        let staging_path = Path::new(&staging_reference);
        drop(self.root.resolve(artifact_path)?);
        drop(self.root.resolve(staging_path)?);

        let directory = self.root.try_clone_directory()?;
        directory
            .create_dir_all(&parent)
            .map_err(|source| io_error("create paper checkpoint object directory", source))?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let mut staging = directory
            .open_with(staging_path, &options)
            .map_err(|source| io_error("create paper checkpoint staging file", source))?;
        let mut staging_guard = StagingGuard::new(&directory, staging_path);
        staging
            .write_all(&bytes)
            .map_err(|source| io_error("write paper checkpoint staging file", source))?;
        staging
            .sync_all()
            .map_err(|source| io_error("synchronize paper checkpoint staging file", source))?;
        drop(staging);
        publication_checkpoint(PaperCheckpointPublicationPoint::AfterStagedFileSync)?;

        match directory.hard_link(staging_path, &directory, artifact_path) {
            Ok(()) => {}
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(source) => {
                return Err(io_error(
                    "publish immutable paper checkpoint object",
                    source,
                ));
            }
        }
        publication_checkpoint(PaperCheckpointPublicationPoint::AfterPublication)?;
        staging_guard.remove()?;
        synchronize_publication_directories(&directory, artifact_path)?;
        publication_checkpoint(PaperCheckpointPublicationPoint::AfterDirectorySync)?;
        publication_checkpoint(PaperCheckpointPublicationPoint::BeforeVerifiedReadback)?;

        let persisted = read_bounded_regular(&directory, artifact_path, maximum_bytes)?;
        if persisted != bytes {
            return Err(PaperCheckpointRepositoryError::ContentConflict);
        }
        let persisted_digest: [u8; 32] = Sha256::digest(&persisted).into();
        if persisted_digest != artifact_digest {
            return Err(PaperCheckpointRepositoryError::VerificationFailed);
        }
        let decoded =
            PaperExecutionCheckpoint::decode(self.config.clone(), &persisted, maximum_bytes)?;
        if decoded != *checkpoint {
            return Err(PaperCheckpointRepositoryError::VerificationFailed);
        }

        Ok(PaperCheckpointReceipt {
            repository_id: self.repository_id,
            generation,
            configuration_digest: self.config.digest(),
            sequence: checkpoint.sequence(),
            recovery_digest,
            artifact_digest,
            artifact_reference: artifact_reference.into_boxed_str(),
        })
    }
}

/// Non-cloneable proof that an exact checkpoint passed durable verified publication.
#[derive(Debug)]
pub struct PaperCheckpointReceipt {
    repository_id: [u8; 32],
    generation: NonZeroU64,
    configuration_digest: [u8; 32],
    sequence: u64,
    recovery_digest: [u8; 32],
    artifact_digest: [u8; 32],
    artifact_reference: Box<str>,
}

impl PaperCheckpointReceipt {
    pub const fn generation(&self) -> NonZeroU64 {
        self.generation
    }

    pub const fn configuration_digest(&self) -> [u8; 32] {
        self.configuration_digest
    }

    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    pub const fn artifact_digest(&self) -> [u8; 32] {
        self.artifact_digest
    }

    pub fn artifact_reference(&self) -> &Path {
        Path::new(self.artifact_reference.as_ref())
    }

    pub(crate) const fn persistence_evidence(&self) -> PaperCheckpointPersistenceEvidence {
        PaperCheckpointPersistenceEvidence {
            configuration_digest: self.configuration_digest,
            sequence: self.sequence,
            recovery_digest: self.recovery_digest,
        }
    }

    pub(crate) fn retained_heap_bytes(&self) -> usize {
        self.artifact_reference.len()
    }

    pub(crate) fn authority_is_valid(
        &self,
        expected_repository_id: [u8; 32],
        minimum_generation: u64,
    ) -> bool {
        self.repository_id == expected_repository_id
            && self.generation.get() > minimum_generation
            && self.artifact_digest != [0; 32]
            && !self.artifact_reference.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PaperCheckpointPublicationPoint {
    AfterStagedFileSync,
    AfterPublication,
    AfterDirectorySync,
    BeforeVerifiedReadback,
}

#[cfg(test)]
impl PaperCheckpointPublicationPoint {
    const ALL: [Self; 4] = [
        Self::AfterStagedFileSync,
        Self::AfterPublication,
        Self::AfterDirectorySync,
        Self::BeforeVerifiedReadback,
    ];
}

/// Durable publication or verification failure; every variant withholds a receipt.
#[derive(Debug, Error)]
pub enum PaperCheckpointRepositoryError {
    #[error("paper checkpoint repository randomness is unavailable")]
    RandomUnavailable,
    #[error("paper checkpoint repository generation is exhausted")]
    GenerationExhausted,
    #[error("paper checkpoint configuration does not match the bound repository")]
    ConfigurationMismatch,
    #[error("paper checkpoint repository bounded allocation failed")]
    Allocation,
    #[error("paper checkpoint artifact is not an unambiguous regular file")]
    UnsafeArtifact,
    #[error("paper checkpoint content-addressed object contains conflicting bytes")]
    ContentConflict,
    #[error("paper checkpoint durable read-back verification failed")]
    VerificationFailed,
    #[error("paper checkpoint directory durability is unsupported on this platform")]
    UnsupportedDurability,
    #[error("{context}: {source}")]
    Io {
        context: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Path(#[from] PathError),
    #[error(transparent)]
    ArtifactPath(#[from] ArtifactPathError),
    #[error(transparent)]
    Checkpoint(#[from] PaperCheckpointError),
    #[cfg(test)]
    #[error("injected paper checkpoint publication interruption")]
    TestInterruption,
}

#[derive(Debug)]
struct StagingGuard<'directory> {
    directory: &'directory Dir,
    path: &'directory Path,
    armed: bool,
}

impl<'directory> StagingGuard<'directory> {
    const fn new(directory: &'directory Dir, path: &'directory Path) -> Self {
        Self {
            directory,
            path,
            armed: true,
        }
    }

    fn remove(&mut self) -> Result<(), PaperCheckpointRepositoryError> {
        self.directory
            .remove_file(self.path)
            .map_err(|source| io_error("remove paper checkpoint staging file", source))?;
        self.armed = false;
        Ok(())
    }
}

impl Drop for StagingGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ignored = self.directory.remove_file(self.path);
            self.armed = false;
        }
    }
}

fn read_bounded_regular(
    directory: &Dir,
    path: &Path,
    maximum_bytes: usize,
) -> Result<Vec<u8>, PaperCheckpointRepositoryError> {
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    configure_nonblocking_read(&mut options);
    let mut file = directory
        .open_with(path, &options)
        .map_err(|source| io_error("open published paper checkpoint object", source))?;
    let metadata = file
        .metadata()
        .map_err(|source| io_error("inspect opened paper checkpoint object", source))?;
    if !metadata.is_file() {
        return Err(PaperCheckpointRepositoryError::UnsafeArtifact);
    }
    let size = usize::try_from(metadata.len())
        .map_err(|_| PaperCheckpointRepositoryError::VerificationFailed)?;
    if size == 0 || size > maximum_bytes {
        return Err(PaperCheckpointRepositoryError::VerificationFailed);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
    bytes.resize(size, 0);
    file.read_exact(&mut bytes)
        .map_err(|source| io_error("read published paper checkpoint object", source))?;
    let mut trailing = [0_u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|source| io_error("bound published paper checkpoint object", source))?
        != 0
    {
        return Err(PaperCheckpointRepositoryError::VerificationFailed);
    }
    Ok(bytes)
}

fn hex_bytes(bytes: &[u8]) -> Result<String, PaperCheckpointRepositoryError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let capacity = bytes
        .len()
        .checked_mul(2)
        .ok_or(PaperCheckpointRepositoryError::Allocation)?;
    let mut output = String::new();
    output
        .try_reserve_exact(capacity)
        .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(output)
}

fn io_error(context: &'static str, source: std::io::Error) -> PaperCheckpointRepositoryError {
    PaperCheckpointRepositoryError::Io { context, source }
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn configure_nonblocking_read(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC);
}

#[cfg(not(unix))]
fn configure_nonblocking_read(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn synchronize_publication_directories(
    directory: &Dir,
    artifact_path: &Path,
) -> Result<(), PaperCheckpointRepositoryError> {
    let parent = artifact_path
        .parent()
        .ok_or(PaperCheckpointRepositoryError::VerificationFailed)?;
    for path in [
        parent,
        Path::new("paper-checkpoints/v1"),
        Path::new("paper-checkpoints"),
        Path::new("."),
    ] {
        directory
            .open_dir(path)
            .map(Dir::into_std_file)
            .and_then(|file| file.sync_all())
            .map_err(|source| io_error("synchronize paper checkpoint object directory", source))?;
    }
    Ok(())
}

#[cfg(windows)]
fn synchronize_publication_directories(
    _directory: &Dir,
    _artifact_path: &Path,
) -> Result<(), PaperCheckpointRepositoryError> {
    Err(PaperCheckpointRepositoryError::UnsupportedDurability)
}

#[cfg(not(any(unix, windows)))]
fn synchronize_publication_directories(
    _directory: &Dir,
    _artifact_path: &Path,
) -> Result<(), PaperCheckpointRepositoryError> {
    Err(PaperCheckpointRepositoryError::UnsupportedDurability)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
    use std::str::FromStr;
    use std::time::Duration;

    use market_squawk_domain::{
        AccountId, Currency, Money, RuleVersion, SourceIdentifier, Timestamp, VenueId,
    };
    use market_squawk_platform::LocalPaths;
    use rust_decimal::Decimal;
    use static_assertions::assert_not_impl_any;

    use super::{
        PaperCheckpointPublicationPoint, PaperCheckpointReceipt, PaperCheckpointRepository,
        PaperCheckpointRepositoryError, read_bounded_regular,
    };
    use crate::{
        FeeSchedule, PaperAccountBootstrap, PaperExecutionCheckpoint, PaperExecutionConfig,
        PaperExecutionConfigInput, PaperExposureValuation, PaperLedger, PaperVenueSession,
        PaperVenueSessionCalendar,
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    assert_not_impl_any!(PaperCheckpointReceipt: Clone, Copy);

    #[cfg(windows)]
    #[test]
    fn windows_directory_durability_fails_closed() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("data"))?;
        let capability = paths.artifacts()?.try_clone_directory()?;
        assert!(matches!(
            super::synchronize_publication_directories(
                &capability,
                std::path::Path::new("paper-checkpoints/v1/00/object.json"),
            ),
            Err(PaperCheckpointRepositoryError::UnsupportedDurability)
        ));
        Ok(())
    }

    #[test]
    fn receipt_requires_every_durability_boundary_and_exact_existing_recovery() -> TestResult {
        for interrupted_at in PaperCheckpointPublicationPoint::ALL {
            let directory = tempfile::tempdir()?;
            let paths = LocalPaths::prepare(directory.path().join("data"))?;
            let (config, checkpoint) = checkpoint_fixture()?;
            let mut repository = PaperCheckpointRepository::try_new(
                paths.artifacts()?.clone(),
                config,
                NonZeroUsize::new(1024 * 1024).ok_or("zero checkpoint bound")?,
            )?;

            let interrupted = repository.persist_with_checkpoint(&checkpoint, |observed| {
                if observed == interrupted_at {
                    Err(PaperCheckpointRepositoryError::TestInterruption)
                } else {
                    Ok(())
                }
            });
            assert!(interrupted.is_err());

            let receipt = repository.persist(&checkpoint)?;
            assert_eq!(
                receipt.configuration_digest(),
                checkpoint.configuration_digest()
            );
            assert_eq!(receipt.sequence(), checkpoint.sequence());
            assert_ne!(receipt.artifact_digest(), [0; 32]);
            assert!(
                paths
                    .artifacts()?
                    .root()
                    .join(receipt.artifact_reference())
                    .is_file()
            );

            let recovered = repository.persist(&checkpoint)?;
            assert_eq!(recovered.artifact_digest(), receipt.artifact_digest());
            assert_eq!(recovered.artifact_reference(), receipt.artifact_reference());
        }
        Ok(())
    }

    #[test]
    fn conflicting_existing_object_never_mints_a_receipt() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("data"))?;
        let (config, checkpoint) = checkpoint_fixture()?;
        let maximum_bytes = NonZeroUsize::new(1024 * 1024).ok_or("zero checkpoint bound")?;
        let mut repository = PaperCheckpointRepository::try_new(
            paths.artifacts()?.clone(),
            config.clone(),
            maximum_bytes,
        )?;
        let receipt = repository.persist(&checkpoint)?;
        std::fs::write(
            paths.artifacts()?.root().join(receipt.artifact_reference()),
            b"conflicting checkpoint bytes",
        )?;

        let mut retry =
            PaperCheckpointRepository::try_new(paths.artifacts()?.clone(), config, maximum_bytes)?;
        assert!(matches!(
            retry.persist(&checkpoint),
            Err(PaperCheckpointRepositoryError::ContentConflict)
        ));
        Ok(())
    }

    #[test]
    fn recovered_receipt_authority_is_runtime_bound_and_strictly_monotonic() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("primary"))?;
        let alternate_paths = LocalPaths::prepare(directory.path().join("alternate"))?;
        let (config, mut checkpoint) = checkpoint_fixture()?;
        let maximum_bytes = NonZeroUsize::new(1024 * 1024).ok_or("zero checkpoint bound")?;
        let mut repository = PaperCheckpointRepository::try_new(
            paths.artifacts()?.clone(),
            config.clone(),
            maximum_bytes,
        )?;
        let first = repository.persist(&checkpoint)?;
        checkpoint.accepted_repository_id = repository.binding_identity();
        checkpoint.accepted_repository_generation = first.generation().get();
        let recovered = PaperExecutionCheckpoint::decode(
            config.clone(),
            &checkpoint.encode(maximum_bytes.get())?,
            maximum_bytes.get(),
        )?;
        let second = repository.persist(&recovered)?;
        let mut alternate = PaperCheckpointRepository::try_new(
            alternate_paths.artifacts()?.clone(),
            config,
            maximum_bytes,
        )?;
        let wrong_root = alternate.persist(&recovered)?;

        assert!(!crate::worker::receipt_authority_is_current(
            repository.binding_identity(),
            recovered.accepted_repository_id,
            recovered.accepted_repository_generation,
            &first,
        ));
        assert!(crate::worker::receipt_authority_is_current(
            repository.binding_identity(),
            recovered.accepted_repository_id,
            recovered.accepted_repository_generation,
            &second,
        ));
        assert!(!crate::worker::receipt_authority_is_current(
            repository.binding_identity(),
            recovered.accepted_repository_id,
            recovered.accepted_repository_generation,
            &wrong_root,
        ));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn opened_special_file_is_rejected_from_the_exact_handle() -> TestResult {
        use std::os::unix::net::UnixListener;

        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("data"))?;
        let capability = paths.artifacts()?.try_clone_directory()?;
        let relative = std::path::Path::new("special.sock");
        let _listener = UnixListener::bind(paths.artifacts()?.root().join(relative))?;

        assert!(matches!(
            read_bounded_regular(&capability, relative, 1024),
            Err(PaperCheckpointRepositoryError::UnsafeArtifact)
                | Err(PaperCheckpointRepositoryError::Io { .. })
        ));
        Ok(())
    }

    fn checkpoint_fixture() -> TestResultWith<(PaperExecutionConfig, PaperExecutionCheckpoint)> {
        let usd = Currency::try_from("USD")?;
        let venue = VenueId::try_from("paper")?;
        let calendar = PaperVenueSessionCalendar::try_new(
            SourceIdentifier::try_from("checkpoint-calendar")?,
            RuleVersion::new(1)?,
            venue,
            "UTC",
            vec![PaperVenueSession::try_new(
                SourceIdentifier::try_from("checkpoint-session")?,
                Timestamp::from_unix_nanos(i64::MIN),
                Timestamp::from_unix_nanos(i64::MAX),
            )?],
        )?;
        let config = PaperExecutionConfig::try_new(PaperExecutionConfigInput {
            configuration_version: NonZeroU64::MIN,
            deterministic_seed: [7; 32],
            command_capacity: NonZeroUsize::new(4).ok_or("zero command capacity")?,
            command_maximum_bytes: NonZeroU32::new(256 * 1024).ok_or("zero command bytes")?,
            market_capacity: NonZeroUsize::new(4).ok_or("zero market capacity")?,
            market_maximum_bytes: NonZeroU32::new(256 * 1024).ok_or("zero market bytes")?,
            audit_capacity: NonZeroUsize::new(4).ok_or("zero audit capacity")?,
            audit_maximum_bytes: NonZeroU32::new(256 * 1024).ok_or("zero audit bytes")?,
            maximum_orders: NonZeroUsize::new(4).ok_or("zero order capacity")?,
            maximum_fills: NonZeroUsize::new(4).ok_or("zero fill capacity")?,
            maximum_idempotency_keys: NonZeroUsize::new(4).ok_or("zero idempotency capacity")?,
            maximum_archived_orders: NonZeroUsize::new(4).ok_or("zero archive capacity")?,
            matching_work_quantum: NonZeroUsize::MIN,
            minimum_latency_nanos: 0,
            maximum_latency_nanos: 0,
            cancel_latency_nanos: 0,
            maximum_mark_age_nanos: 1_000_000_000,
            day_session_calendar: calendar,
            maximum_participation_basis_points: 10_000,
            impact_basis_points_per_level: 0,
            reporting_currency: usd,
            ledger_maximum_accounts: NonZeroUsize::MIN,
            ledger_maximum_balances: NonZeroUsize::MIN,
            ledger_maximum_positions: NonZeroUsize::MIN,
            allow_short: false,
            exposure_valuation: PaperExposureValuation::ExecutableExit,
            abort_join_deadline: Duration::from_secs(1),
            fee_schedule: FeeSchedule::try_new(0, 0, Money::new(Decimal::ZERO, usd), None, 2)?,
        })?;
        let account_id = AccountId::from_str("50000000-0000-0000-0000-000000000088")?;
        let ledger = PaperLedger::try_new(
            config.ledger_config(),
            [PaperAccountBootstrap {
                account_id,
                revision: NonZeroU64::MIN,
                eligible: true,
                cash: vec![Money::new(Decimal::new(1_000, 0), usd)],
                capital: Money::new(Decimal::new(1_000, 0), usd),
                peak_capital: Money::new(Decimal::new(1_000, 0), usd),
                gross_exposure: Money::new(Decimal::ZERO, usd),
                realized_loss: Money::new(Decimal::ZERO, usd),
                realized_pnl: Money::new(Decimal::ZERO, usd),
                positions: Vec::new(),
                position_cost_basis: Vec::new(),
            }],
        )?;
        let checkpoint = PaperExecutionCheckpoint {
            schema_version: PaperExecutionConfig::CHECKPOINT_SCHEMA_VERSION,
            configuration_digest: config.digest(),
            complete: true,
            sequence: 0,
            reconciliation_required: false,
            orders: BTreeMap::new(),
            fills: Vec::new(),
            archived_orders: BTreeMap::new(),
            archived_fills: Vec::new(),
            durable_sequence: 0,
            accepted_repository_id: [0; 32],
            accepted_repository_generation: 0,
            reconciled_orders: BTreeSet::new(),
            acknowledged_reconciliation_batches: Vec::new(),
            ledger,
            idempotency: BTreeMap::new(),
        };
        Ok((config, checkpoint))
    }

    type TestResultWith<T> = Result<T, Box<dyn std::error::Error>>;
}
