//! Capability-confined durable checkpoint publication and opaque persistence receipts.

use std::io::{Read as _, Write as _};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;

use cap_fs_ext::{FollowSymlinks, MetadataExt as _, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use fs2::FileExt as _;
use market_squawk_domain::{
    AccountId, ClientOrderId, Currency, InstrumentId, Money, OrderId, Timestamp,
};
use market_squawk_execution::{
    AccountIdempotencyBootstrap, AccountIdempotencyBootstrapError, AccountIdempotencyTombstone,
    OrderIntentDigest, ReconciledAccountState, ReconciledAccountStateError,
};
use market_squawk_platform::{ArtifactPathError, ArtifactRoot, PathError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::snapshot::PaperCheckpointPersistenceEvidence;
use crate::{PaperCheckpointError, PaperExecutionCheckpoint, PaperExecutionConfig};

const CHECKPOINT_OBJECT_ROOT: &str = "paper-checkpoints/v1";
const CURRENT_MANIFEST_PATH: &str = "paper-checkpoints/v1/current.json";
const RUN_DIRTY_PATH: &str = "paper-checkpoints/v1/run-dirty.json";
const REPOSITORY_LOCK_PATH: &str = ".market-squawk-paper-checkpoints.lock";
const MANIFEST_SCHEMA_VERSION: u32 = 1;
const RUN_DIRTY_SCHEMA_VERSION: u32 = 1;
const MAXIMUM_RUN_DIRTY_BYTES: usize = 4 * 1024;

/// Single-writer durable publisher bound to one artifact root and paper configuration.
#[derive(Debug)]
pub struct PaperCheckpointRepository {
    root: ArtifactRoot,
    config: PaperExecutionConfig,
    maximum_bytes: NonZeroUsize,
    repository_id: [u8; 32],
    generation: u64,
    recovery: Option<PaperCheckpointRecovery>,
    dirty_authority: Option<[u8; 32]>,
    _writer_lock: std::fs::File,
}

impl PaperCheckpointRepository {
    /// Retains and validates one artifact capability and exact decode configuration.
    pub fn try_new(
        root: ArtifactRoot,
        config: PaperExecutionConfig,
        maximum_bytes: NonZeroUsize,
    ) -> Result<Self, PaperCheckpointRepositoryError> {
        let directory = root.try_clone_directory()?;
        let writer_lock = acquire_repository_writer(&directory)?;
        cleanup_stale_staging(&directory)?;
        reject_unclean_run(&directory)?;
        if let Some(recovered) = read_current_manifest(&directory, &config, maximum_bytes.get())? {
            return Ok(Self {
                root,
                config,
                maximum_bytes,
                repository_id: recovered.repository_id,
                generation: recovered.generation.get(),
                recovery: Some(PaperCheckpointRecovery {
                    checkpoint: recovered.checkpoint,
                    accounts: recovered.accounts,
                }),
                dirty_authority: None,
                _writer_lock: writer_lock,
            });
        }
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
            recovery: None,
            dirty_authority: None,
            _writer_lock: writer_lock,
        })
    }

    /// Transfers the exact current checkpoint and replay fence discovered at repository open.
    pub fn take_recovery(&mut self) -> Option<PaperCheckpointRecovery> {
        self.recovery.take()
    }

    /// Atomically advances the fixed current manifest with one exact checkpoint and replay image.
    pub fn persist_with_replay(
        &mut self,
        checkpoint: &PaperExecutionCheckpoint,
        account_replay: &[PaperAccountReplaySnapshot],
    ) -> Result<PaperCheckpointReceipt, PaperCheckpointRepositoryError> {
        self.persist_with_checkpoint(checkpoint, account_replay, |_| Ok(()))
    }

    /// Durably marks this run unsafe to restore until one exact stabilized checkpoint is published.
    pub fn mark_run_dirty(&mut self) -> Result<(), PaperCheckpointRepositoryError> {
        if self.dirty_authority.is_some() {
            return Ok(());
        }
        let directory = self.root.try_clone_directory()?;
        self.validate_current_authority(&directory)?;
        directory
            .create_dir_all(CHECKPOINT_OBJECT_ROOT)
            .map_err(|source| io_error("create paper run-dirty namespace", source))?;
        let marker = RunDirtyWire {
            schema_version: RUN_DIRTY_SCHEMA_VERSION,
            repository_id: self.repository_id,
            generation: self.generation,
            nonce: random_bytes()?,
        };
        let mut output = BoundedRepositoryWriter::new(MAXIMUM_RUN_DIRTY_BYTES)?;
        serde_json::to_writer(&mut output, &marker)
            .map_err(PaperCheckpointRepositoryError::ManifestEncoding)?;
        let bytes = output.into_inner();
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let mut file = directory
            .open_with(RUN_DIRTY_PATH, &options)
            .map_err(|source| io_error("create paper run-dirty authority", source))?;
        file.write_all(&bytes)
            .map_err(|source| io_error("write paper run-dirty authority", source))?;
        file.sync_all()
            .map_err(|source| io_error("synchronize paper run-dirty authority", source))?;
        drop(file);
        synchronize_current_manifest_directories(&directory)?;
        let persisted = read_bounded_regular(
            &directory,
            Path::new(RUN_DIRTY_PATH),
            MAXIMUM_RUN_DIRTY_BYTES,
        )?;
        if persisted != bytes {
            return Err(PaperCheckpointRepositoryError::DirtyAuthorityChanged);
        }
        self.dirty_authority = Some(Sha256::digest(&persisted).into());
        Ok(())
    }

    /// Publishes the exact stabilized recovery image and only then clears run-dirty authority.
    pub fn persist_stabilized_with_replay(
        &mut self,
        checkpoint: &PaperExecutionCheckpoint,
        account_replay: &[PaperAccountReplaySnapshot],
    ) -> Result<PaperCheckpointReceipt, PaperCheckpointRepositoryError> {
        if self.dirty_authority.is_none() {
            return Err(PaperCheckpointRepositoryError::DirtyAuthorityRequired);
        }
        if checkpoint.has_nonterminal_orders()
            || checkpoint.reconciliation_required()
            || checkpoint.durable_sequence != checkpoint.sequence()
        {
            return Err(PaperCheckpointRepositoryError::UnstabilizedCheckpoint);
        }
        let receipt = self.persist_with_replay(checkpoint, account_replay)?;
        let recovery_digest = checkpoint.recovery_digest()?;
        if receipt.sequence() != checkpoint.sequence()
            || receipt.recovery_digest() != recovery_digest
            || receipt.artifact_digest() != recovery_digest
        {
            return Err(PaperCheckpointRepositoryError::VerificationFailed);
        }
        self.clear_run_dirty()?;
        Ok(receipt)
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
        account_replay: &[PaperAccountReplaySnapshot],
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
        if checkpoint.reconciliation_required() {
            return Err(PaperCheckpointRepositoryError::QuarantinedCheckpoint);
        }
        let directory = self.root.try_clone_directory()?;
        self.validate_current_authority(&directory)?;
        let generation = self
            .generation
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(PaperCheckpointRepositoryError::GenerationExhausted)?;
        let maximum_bytes = self.maximum_bytes.get();
        let bytes = checkpoint.encode(maximum_bytes)?;
        let artifact_digest: [u8; 32] = Sha256::digest(&bytes).into();
        let recovery_digest = checkpoint.recovery_input_digest()?;
        let digest_hex = hex_bytes(&artifact_digest)?;
        let repository_hex = hex_bytes(&self.repository_id)?;
        let stage_nonce = random_hex()?;
        let parent = format!("{CHECKPOINT_OBJECT_ROOT}/{}", &digest_hex[..2]);
        let artifact_reference = format!("{parent}/{digest_hex}.json");
        let staging_reference = format!(
            "{parent}/stage-{repository_hex}-{}-{stage_nonce}.tmp",
            generation.get()
        );
        let artifact_path = Path::new(&artifact_reference);
        let staging_path = Path::new(&staging_reference);
        drop(self.root.resolve(artifact_path)?);
        drop(self.root.resolve(staging_path)?);

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

        let manifest = CurrentManifestWire::try_new(
            self.repository_id,
            generation,
            self.config.digest(),
            checkpoint,
            recovery_digest,
            artifact_digest,
            artifact_reference.clone(),
            account_replay,
        )?;
        // Burn the generation before the atomic current-name replacement. If a later durability
        // or read-back barrier fails, this process can never reissue that generation for different
        // bytes; reopening recovers the manifest generation that actually became current.
        self.generation = generation.get();
        publish_current_manifest(
            &directory,
            &manifest,
            &repository_hex,
            generation,
            maximum_bytes,
        )?;
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

    fn validate_current_authority(
        &self,
        directory: &Dir,
    ) -> Result<(), PaperCheckpointRepositoryError> {
        if self.generation == 0 {
            match directory.symlink_metadata(CURRENT_MANIFEST_PATH) {
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(source) => {
                    return Err(io_error(
                        "inspect current paper checkpoint authority",
                        source,
                    ));
                }
                Ok(_) => {}
            }
        }
        match read_current_manifest(directory, &self.config, self.maximum_bytes.get())? {
            Some(current)
                if current.repository_id == self.repository_id
                    && current.generation.get() == self.generation =>
            {
                Ok(())
            }
            None => Err(PaperCheckpointRepositoryError::AuthorityChanged),
            _ => Err(PaperCheckpointRepositoryError::AuthorityChanged),
        }
    }

    fn clear_run_dirty(&mut self) -> Result<(), PaperCheckpointRepositoryError> {
        let expected = self
            .dirty_authority
            .ok_or(PaperCheckpointRepositoryError::DirtyAuthorityRequired)?;
        let directory = self.root.try_clone_directory()?;
        let bytes = read_bounded_regular(
            &directory,
            Path::new(RUN_DIRTY_PATH),
            MAXIMUM_RUN_DIRTY_BYTES,
        )?;
        if <[u8; 32]>::from(Sha256::digest(&bytes)) != expected {
            return Err(PaperCheckpointRepositoryError::DirtyAuthorityChanged);
        }
        directory
            .remove_file(RUN_DIRTY_PATH)
            .map_err(|source| io_error("clear paper run-dirty authority", source))?;
        synchronize_current_manifest_directories(&directory)?;
        match directory.symlink_metadata(RUN_DIRTY_PATH) {
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(io_error("verify cleared paper run-dirty authority", source));
            }
            Ok(_) => return Err(PaperCheckpointRepositoryError::DirtyAuthorityChanged),
        }
        self.dirty_authority = None;
        Ok(())
    }
}

/// Exact persisted replay fence for one paper account.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperAccountReplaySnapshot {
    account_id: AccountId,
    state: Option<ReconciledAccountState>,
    idempotency: AccountIdempotencyBootstrap,
}

impl PaperAccountReplaySnapshot {
    pub const fn new(account_id: AccountId, idempotency: AccountIdempotencyBootstrap) -> Self {
        Self {
            account_id,
            state: None,
            idempotency,
        }
    }

    /// Binds the replay fence to the risk coordinator's exact quiescent financial revision.
    pub fn from_reconciled_state(
        state: ReconciledAccountState,
        idempotency: AccountIdempotencyBootstrap,
    ) -> Self {
        Self {
            account_id: state.account_id(),
            state: Some(state),
            idempotency,
        }
    }

    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub const fn idempotency(&self) -> &AccountIdempotencyBootstrap {
        &self.idempotency
    }
}

/// One verified current recovery image. No directory scan participates in selection.
#[derive(Debug)]
pub struct PaperCheckpointRecovery {
    checkpoint: PaperExecutionCheckpoint,
    accounts: Box<[PaperAccountRecoverySnapshot]>,
}

impl PaperCheckpointRecovery {
    pub const fn checkpoint(&self) -> &PaperExecutionCheckpoint {
        &self.checkpoint
    }

    pub const fn accounts(&self) -> &[PaperAccountRecoverySnapshot] {
        &self.accounts
    }

    pub fn into_parts(
        self,
    ) -> (
        PaperExecutionCheckpoint,
        Box<[PaperAccountRecoverySnapshot]>,
    ) {
        (self.checkpoint, self.accounts)
    }
}

/// Exact marked account state and replay fence bound to the current checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperAccountRecoverySnapshot {
    state: ReconciledAccountState,
    idempotency: AccountIdempotencyBootstrap,
}

impl PaperAccountRecoverySnapshot {
    pub const fn state(&self) -> &ReconciledAccountState {
        &self.state
    }

    pub const fn idempotency(&self) -> &AccountIdempotencyBootstrap {
        &self.idempotency
    }

    pub fn into_parts(self) -> (ReconciledAccountState, AccountIdempotencyBootstrap) {
        (self.state, self.idempotency)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CurrentManifestWire {
    schema_version: u32,
    repository_id: [u8; 32],
    generation: NonZeroU64,
    configuration_digest: [u8; 32],
    sequence: u64,
    recovery_digest: [u8; 32],
    artifact_digest: [u8; 32],
    artifact_reference: String,
    account_replay: Vec<AccountReplayWire>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct RunDirtyWire {
    schema_version: u32,
    repository_id: [u8; 32],
    generation: u64,
    nonce: [u8; 32],
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AccountReplayWire {
    account_id: AccountId,
    state: AccountStateWire,
    revision: NonZeroU64,
    tombstones: Vec<ReplayTombstoneWire>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AccountStateWire {
    revision: NonZeroU64,
    eligible: bool,
    currency: Currency,
    cash: Money,
    settled_capital: Money,
    marked_equity: Money,
    peak_marked_equity: Money,
    marked_gross_exposure: Money,
    unrealized_pnl: Money,
    drawdown: Money,
    mark_digest: [u8; 32],
    realized_pnl: Money,
    realized_loss: Money,
    positions: Vec<(InstrumentId, i64)>,
    position_cost_basis: Vec<(InstrumentId, Money)>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplayTombstoneWire {
    order_id: OrderId,
    client_order_id: ClientOrderId,
    intent_digest: [u8; 32],
    intent_expires_at: Timestamp,
}

impl CurrentManifestWire {
    #[expect(
        clippy::too_many_arguments,
        reason = "manifest binds independent recovery authorities"
    )]
    fn try_new(
        repository_id: [u8; 32],
        generation: NonZeroU64,
        configuration_digest: [u8; 32],
        checkpoint: &PaperExecutionCheckpoint,
        recovery_digest: [u8; 32],
        artifact_digest: [u8; 32],
        artifact_reference: String,
        account_replay: &[PaperAccountReplaySnapshot],
    ) -> Result<Self, PaperCheckpointRepositoryError> {
        let states = checkpoint.reconciled_accounts_for_recovery()?;
        if account_replay.len() != states.len()
            || account_replay.iter().any(|snapshot| {
                !states
                    .iter()
                    .any(|state| state.account_id() == snapshot.account_id)
            })
        {
            return Err(PaperCheckpointRepositoryError::InvalidReplay);
        }
        let mut replay = Vec::new();
        replay
            .try_reserve_exact(states.len())
            .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
        for state in states {
            let snapshot = account_replay
                .iter()
                .find(|snapshot| snapshot.account_id == state.account_id())
                .ok_or(PaperCheckpointRepositoryError::InvalidReplay)?;
            let idempotency = snapshot.idempotency.clone();
            let recovery_state = snapshot.state.clone().unwrap_or_else(|| state.clone());
            if !same_financial_state(&recovery_state, &state) {
                return Err(PaperCheckpointRepositoryError::InvalidReplay);
            }
            let mut tombstones = Vec::new();
            tombstones
                .try_reserve_exact(idempotency.tombstones().len())
                .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
            tombstones.extend(idempotency.tombstones().iter().map(|tombstone| {
                ReplayTombstoneWire {
                    order_id: tombstone.order_id(),
                    client_order_id: tombstone.client_order_id().clone(),
                    intent_digest: tombstone.intent_digest().as_bytes(),
                    intent_expires_at: tombstone.intent_expires_at(),
                }
            }));
            replay.push(AccountReplayWire {
                account_id: state.account_id(),
                state: AccountStateWire::from_state(&recovery_state),
                revision: idempotency.revision(),
                tombstones,
            });
        }
        replay.sort_unstable_by_key(|entry| entry.account_id);
        Ok(Self {
            schema_version: MANIFEST_SCHEMA_VERSION,
            repository_id,
            generation,
            configuration_digest,
            sequence: checkpoint.sequence(),
            recovery_digest,
            artifact_digest,
            artifact_reference,
            account_replay: replay,
        })
    }
}

struct OpenedRecovery {
    repository_id: [u8; 32],
    generation: NonZeroU64,
    checkpoint: PaperExecutionCheckpoint,
    accounts: Box<[PaperAccountRecoverySnapshot]>,
}

impl AccountStateWire {
    fn from_state(state: &ReconciledAccountState) -> Self {
        Self {
            revision: state.revision(),
            eligible: state.eligible(),
            currency: state.currency(),
            cash: state.cash(),
            settled_capital: state.settled_capital(),
            marked_equity: state.marked_equity(),
            peak_marked_equity: state.peak_marked_equity(),
            marked_gross_exposure: state.marked_gross_exposure(),
            unrealized_pnl: state.unrealized_pnl(),
            drawdown: state.drawdown(),
            mark_digest: state.mark_digest(),
            realized_pnl: state.realized_pnl(),
            realized_loss: state.realized_loss(),
            positions: state.positions().to_vec(),
            position_cost_basis: state.position_cost_basis().to_vec(),
        }
    }

    fn into_state(
        self,
        account_id: AccountId,
    ) -> Result<ReconciledAccountState, ReconciledAccountStateError> {
        ReconciledAccountState::try_new(
            account_id,
            self.revision,
            self.eligible,
            self.currency,
            self.cash,
            self.settled_capital,
            self.marked_equity,
            self.peak_marked_equity,
            self.marked_gross_exposure,
            self.unrealized_pnl,
            self.drawdown,
            self.mark_digest,
            self.realized_pnl,
            self.realized_loss,
            self.positions,
            self.position_cost_basis,
        )
    }
}

fn read_current_manifest(
    directory: &Dir,
    config: &PaperExecutionConfig,
    maximum_bytes: usize,
) -> Result<Option<OpenedRecovery>, PaperCheckpointRepositoryError> {
    match directory.symlink_metadata(CURRENT_MANIFEST_PATH) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => return Err(PaperCheckpointRepositoryError::UnsafeArtifact),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return match directory.symlink_metadata("paper-checkpoints") {
                Err(missing) if missing.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Ok(_) => Err(PaperCheckpointRepositoryError::PartialState),
                Err(source) => Err(io_error("inspect paper checkpoint namespace", source)),
            };
        }
        Err(source) => {
            return Err(io_error(
                "inspect current paper checkpoint manifest",
                source,
            ));
        }
    }
    let bytes = read_bounded_regular(directory, Path::new(CURRENT_MANIFEST_PATH), maximum_bytes)?;
    let manifest: CurrentManifestWire =
        serde_json::from_slice(&bytes).map_err(PaperCheckpointRepositoryError::ManifestEncoding)?;
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION
        || manifest.repository_id == [0; 32]
        || manifest.configuration_digest != config.digest()
        || manifest.artifact_digest == [0; 32]
        || manifest.recovery_digest == [0; 32]
    {
        return Err(PaperCheckpointRepositoryError::InvalidManifest);
    }
    let digest_hex = hex_bytes(&manifest.artifact_digest)?;
    let expected_reference = format!(
        "{CHECKPOINT_OBJECT_ROOT}/{}/{}.json",
        &digest_hex[..2],
        digest_hex
    );
    if manifest.artifact_reference != expected_reference {
        return Err(PaperCheckpointRepositoryError::InvalidManifest);
    }
    let artifact_path = Path::new(&manifest.artifact_reference);
    let persisted = read_bounded_regular(directory, artifact_path, maximum_bytes)?;
    if <[u8; 32]>::from(Sha256::digest(&persisted)) != manifest.artifact_digest {
        return Err(PaperCheckpointRepositoryError::VerificationFailed);
    }
    let mut checkpoint =
        PaperExecutionCheckpoint::decode(config.clone(), &persisted, maximum_bytes)?;
    if checkpoint.sequence() != manifest.sequence
        || checkpoint.configuration_digest() != manifest.configuration_digest
        || checkpoint.recovery_input_digest()? != manifest.recovery_digest
    {
        return Err(PaperCheckpointRepositoryError::InvalidManifest);
    }
    checkpoint.bind_current_manifest(manifest.repository_id, manifest.generation);
    let mut accounts = Vec::new();
    accounts
        .try_reserve_exact(manifest.account_replay.len())
        .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
    for replay in manifest.account_replay {
        if accounts
            .iter()
            .any(|candidate: &PaperAccountRecoverySnapshot| {
                candidate.state.account_id() == replay.account_id
            })
        {
            return Err(PaperCheckpointRepositoryError::InvalidReplay);
        }
        let mut tombstones = Vec::new();
        tombstones
            .try_reserve_exact(replay.tombstones.len())
            .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
        tombstones.extend(replay.tombstones.into_iter().map(|tombstone| {
            AccountIdempotencyTombstone::new(
                tombstone.order_id,
                tombstone.client_order_id,
                OrderIntentDigest::from_bytes(tombstone.intent_digest),
                tombstone.intent_expires_at,
            )
        }));
        let idempotency = AccountIdempotencyBootstrap::try_new(replay.revision, tombstones)?;
        let state = replay.state.into_state(replay.account_id)?;
        accounts.push(PaperAccountRecoverySnapshot { state, idempotency });
    }
    accounts.sort_unstable_by_key(|account| account.state.account_id());
    let checkpoint_accounts = checkpoint.reconciled_accounts_for_recovery()?;
    if accounts.len() != checkpoint_accounts.len()
        || accounts.iter().any(|account| {
            !checkpoint_accounts.iter().any(|state| {
                state.account_id() == account.state.account_id()
                    && same_financial_state(state, &account.state)
            })
        })
    {
        return Err(PaperCheckpointRepositoryError::InvalidReplay);
    }
    Ok(Some(OpenedRecovery {
        repository_id: manifest.repository_id,
        generation: manifest.generation,
        checkpoint,
        accounts: accounts.into_boxed_slice(),
    }))
}

fn reject_unclean_run(directory: &Dir) -> Result<(), PaperCheckpointRepositoryError> {
    match directory.symlink_metadata(RUN_DIRTY_PATH) {
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(io_error("inspect paper run-dirty authority", source)),
        Ok(metadata) if metadata.file_type().is_file() => {
            Err(PaperCheckpointRepositoryError::UncleanShutdown)
        }
        Ok(_) => Err(PaperCheckpointRepositoryError::UnsafeArtifact),
    }
}

fn same_financial_state(left: &ReconciledAccountState, right: &ReconciledAccountState) -> bool {
    left.account_id() == right.account_id()
        && left.revision() == right.revision()
        && left.eligible() == right.eligible()
        && left.currency() == right.currency()
        && left.cash() == right.cash()
        && left.settled_capital() == right.settled_capital()
        && left.marked_equity() == right.marked_equity()
        && left.peak_marked_equity() == right.peak_marked_equity()
        && left.marked_gross_exposure() == right.marked_gross_exposure()
        && left.unrealized_pnl() == right.unrealized_pnl()
        && left.drawdown() == right.drawdown()
        && left.mark_digest() == right.mark_digest()
        && left.realized_pnl() == right.realized_pnl()
        && left.realized_loss() == right.realized_loss()
        && left.positions() == right.positions()
        && left.position_cost_basis() == right.position_cost_basis()
}

fn publish_current_manifest(
    directory: &Dir,
    manifest: &CurrentManifestWire,
    repository_hex: &str,
    generation: NonZeroU64,
    maximum_bytes: usize,
) -> Result<(), PaperCheckpointRepositoryError> {
    let mut output = BoundedRepositoryWriter::new(maximum_bytes)?;
    serde_json::to_writer(&mut output, manifest)
        .map_err(PaperCheckpointRepositoryError::ManifestEncoding)?;
    let bytes = output.into_inner();
    let stage_nonce = random_hex()?;
    let staging_reference = format!(
        "{CHECKPOINT_OBJECT_ROOT}/current-stage-{repository_hex}-{}-{stage_nonce}.tmp",
        generation.get(),
    );
    let staging_path = Path::new(&staging_reference);
    let current_path = Path::new(CURRENT_MANIFEST_PATH);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    let mut staging = directory
        .open_with(staging_path, &options)
        .map_err(|source| io_error("create current paper checkpoint manifest", source))?;
    let mut staging_guard = StagingGuard::new(directory, staging_path);
    staging
        .write_all(&bytes)
        .map_err(|source| io_error("write current paper checkpoint manifest", source))?;
    staging
        .sync_all()
        .map_err(|source| io_error("synchronize current paper checkpoint manifest", source))?;
    drop(staging);
    directory
        .rename(staging_path, directory, current_path)
        .map_err(|source| io_error("publish current paper checkpoint manifest", source))?;
    staging_guard.disarm();
    synchronize_current_manifest_directories(directory)?;
    let persisted = read_bounded_regular(directory, current_path, maximum_bytes)?;
    if persisted != bytes {
        return Err(PaperCheckpointRepositoryError::VerificationFailed);
    }
    let verified: CurrentManifestWire = serde_json::from_slice(&persisted)
        .map_err(PaperCheckpointRepositoryError::ManifestEncoding)?;
    if verified.repository_id != manifest.repository_id
        || verified.generation != manifest.generation
        || verified.artifact_digest != manifest.artifact_digest
        || verified.sequence != manifest.sequence
    {
        return Err(PaperCheckpointRepositoryError::VerificationFailed);
    }
    Ok(())
}

fn acquire_repository_writer(
    directory: &Dir,
) -> Result<std::fs::File, PaperCheckpointRepositoryError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    let lock = directory
        .open_with(REPOSITORY_LOCK_PATH, &options)
        .map_err(|source| io_error("open paper checkpoint repository lock", source))?;
    let metadata = lock
        .metadata()
        .map_err(|source| io_error("inspect paper checkpoint repository lock", source))?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(PaperCheckpointRepositoryError::UnsafeArtifact);
    }
    let lock = lock.into_std();
    lock.try_lock_exclusive().map_err(|source| {
        if is_lock_contended(&source) {
            PaperCheckpointRepositoryError::RepositoryAlreadyOwned
        } else {
            io_error("acquire paper checkpoint repository lock", source)
        }
    })?;
    Ok(lock)
}

fn cleanup_stale_staging(directory: &Dir) -> Result<(), PaperCheckpointRepositoryError> {
    match directory.symlink_metadata(CHECKPOINT_OBJECT_ROOT) {
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(io_error("inspect paper checkpoint namespace", source)),
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Err(PaperCheckpointRepositoryError::UnsafeArtifact),
    }
    let mut changed = false;
    for entry in directory
        .read_dir(CHECKPOINT_OBJECT_ROOT)
        .map_err(|source| io_error("scan paper checkpoint staging namespace", source))?
    {
        let entry =
            entry.map_err(|source| io_error("read paper checkpoint staging entry", source))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| PaperCheckpointRepositoryError::UnsafeArtifact)?;
        let file_type = entry
            .file_type()
            .map_err(|source| io_error("inspect paper checkpoint staging entry", source))?;
        let reference = format!("{CHECKPOINT_OBJECT_ROOT}/{name}");
        if is_current_stage_name(&name) {
            if !file_type.is_file() {
                return Err(PaperCheckpointRepositoryError::UnsafeArtifact);
            }
            directory
                .remove_file(&reference)
                .map_err(|source| io_error("remove stale current-manifest stage", source))?;
            changed = true;
            continue;
        }
        if !is_lower_hex(&name, 2) || !file_type.is_dir() {
            continue;
        }
        for object in directory
            .read_dir(&reference)
            .map_err(|source| io_error("scan paper checkpoint object stages", source))?
        {
            let object =
                object.map_err(|source| io_error("read paper checkpoint object stage", source))?;
            let object_name = object
                .file_name()
                .into_string()
                .map_err(|_| PaperCheckpointRepositoryError::UnsafeArtifact)?;
            if !is_object_stage_name(&object_name) {
                continue;
            }
            if !object
                .file_type()
                .map_err(|source| io_error("inspect paper checkpoint object stage", source))?
                .is_file()
            {
                return Err(PaperCheckpointRepositoryError::UnsafeArtifact);
            }
            directory
                .remove_file(format!("{reference}/{object_name}"))
                .map_err(|source| io_error("remove stale checkpoint object stage", source))?;
            changed = true;
        }
        if directory
            .read_dir(&reference)
            .map_err(|source| io_error("rescan paper checkpoint object shard", source))?
            .next()
            .transpose()
            .map_err(|source| io_error("inspect paper checkpoint object shard", source))?
            .is_none()
        {
            directory
                .remove_dir(&reference)
                .map_err(|source| io_error("remove empty paper checkpoint object shard", source))?;
            changed = true;
        }
    }
    if directory
        .read_dir(CHECKPOINT_OBJECT_ROOT)
        .map_err(|source| io_error("rescan paper checkpoint namespace", source))?
        .next()
        .transpose()
        .map_err(|source| io_error("inspect paper checkpoint namespace", source))?
        .is_none()
    {
        directory
            .remove_dir(CHECKPOINT_OBJECT_ROOT)
            .map_err(|source| io_error("remove empty paper checkpoint namespace", source))?;
        if directory
            .read_dir("paper-checkpoints")
            .map_err(|source| io_error("inspect paper checkpoint root", source))?
            .next()
            .transpose()
            .map_err(|source| io_error("read paper checkpoint root", source))?
            .is_none()
        {
            directory
                .remove_dir("paper-checkpoints")
                .map_err(|source| io_error("remove empty paper checkpoint root", source))?;
        }
        changed = true;
    }
    if changed {
        directory
            .try_clone()
            .map(Dir::into_std_file)
            .and_then(|root| root.sync_all())
            .map_err(|source| io_error("synchronize stale-stage cleanup", source))?;
    }
    Ok(())
}

fn is_current_stage_name(name: &str) -> bool {
    name.strip_prefix("current-stage-")
        .is_some_and(is_stage_suffix)
}

fn is_object_stage_name(name: &str) -> bool {
    name.strip_prefix("stage-").is_some_and(is_stage_suffix)
}

fn is_stage_suffix(suffix: &str) -> bool {
    let Some(suffix) = suffix.strip_suffix(".tmp") else {
        return false;
    };
    let mut parts = suffix.split('-');
    let Some(repository) = parts.next() else {
        return false;
    };
    let Some(generation) = parts.next() else {
        return false;
    };
    let nonce = parts.next();
    is_lower_hex(repository, 64)
        && generation.parse::<u64>().is_ok_and(|value| value != 0)
        && nonce.is_none_or(|value| is_lower_hex(value, 64))
        && parts.next().is_none()
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn random_hex() -> Result<String, PaperCheckpointRepositoryError> {
    hex_bytes(&random_bytes()?)
}

fn random_bytes() -> Result<[u8; 32], PaperCheckpointRepositoryError> {
    let mut nonce = [0_u8; 32];
    getrandom::fill(&mut nonce).map_err(|_| PaperCheckpointRepositoryError::RandomUnavailable)?;
    Ok(nonce)
}

fn is_lock_contended(source: &std::io::Error) -> bool {
    let expected = fs2::lock_contended_error();
    match (source.raw_os_error(), expected.raw_os_error()) {
        (Some(actual), Some(expected)) => actual == expected,
        _ => source.kind() == expected.kind(),
    }
}

struct BoundedRepositoryWriter {
    bytes: Vec<u8>,
    maximum_bytes: usize,
}

impl BoundedRepositoryWriter {
    fn new(maximum_bytes: usize) -> Result<Self, PaperCheckpointRepositoryError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve(maximum_bytes.min(8 * 1024))
            .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
        Ok(Self {
            bytes,
            maximum_bytes,
        })
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl std::io::Write for BoundedRepositoryWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let next_len = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .ok_or_else(|| std::io::Error::other("paper manifest byte bound overflowed"))?;
        if next_len > self.maximum_bytes {
            return Err(std::io::Error::other("paper manifest byte bound exceeded"));
        }
        self.bytes
            .try_reserve(buffer.len())
            .map_err(|_| std::io::Error::other("paper manifest allocation failed"))?;
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
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

    /// Returns the exact restart-content digest verified before this receipt was minted.
    pub const fn recovery_digest(&self) -> [u8; 32] {
        self.recovery_digest
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
    #[error("paper checkpoint repository is already owned by another writer")]
    RepositoryAlreadyOwned,
    #[error("paper checkpoint current authority changed while the writer was active")]
    AuthorityChanged,
    #[error("paper checkpoint repository records an unclean prior run")]
    UncleanShutdown,
    #[error("paper run-dirty authority changed while the writer was active")]
    DirtyAuthorityChanged,
    #[error("a stabilized paper checkpoint requires active run-dirty authority")]
    DirtyAuthorityRequired,
    #[error("paper run-dirty authority requires an exact durable terminal checkpoint")]
    UnstabilizedCheckpoint,
    #[error("paper checkpoint configuration does not match the bound repository")]
    ConfigurationMismatch,
    #[error("quarantined paper state cannot be published as a clean recovery checkpoint")]
    QuarantinedCheckpoint,
    #[error("paper checkpoint repository bounded allocation failed")]
    Allocation,
    #[error("paper checkpoint namespace exists without one valid current manifest")]
    PartialState,
    #[error("current paper checkpoint manifest is structurally invalid")]
    InvalidManifest,
    #[error("current paper account replay image is structurally invalid")]
    InvalidReplay,
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
    #[error("paper checkpoint replay snapshot is invalid")]
    Idempotency(#[from] AccountIdempotencyBootstrapError),
    #[error("paper recovered account state is invalid")]
    ReconciledAccount(#[from] ReconciledAccountStateError),
    #[error("paper current-manifest encoding failed")]
    ManifestEncoding(#[source] serde_json::Error),
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

    fn disarm(&mut self) {
        self.armed = false;
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

#[cfg(unix)]
fn synchronize_current_manifest_directories(
    directory: &Dir,
) -> Result<(), PaperCheckpointRepositoryError> {
    for path in [
        Path::new("paper-checkpoints/v1"),
        Path::new("paper-checkpoints"),
        Path::new("."),
    ] {
        directory
            .open_dir(path)
            .map(Dir::into_std_file)
            .and_then(|file| file.sync_all())
            .map_err(|source| io_error("synchronize current manifest directory", source))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn synchronize_current_manifest_directories(
    _directory: &Dir,
) -> Result<(), PaperCheckpointRepositoryError> {
    Err(PaperCheckpointRepositoryError::UnsupportedDurability)
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
        AccountId, ClientOrderId, Currency, Money, OrderId, RuleVersion, SourceIdentifier,
        Timestamp, VenueId,
    };
    use market_squawk_execution::{
        AccountIdempotencyBootstrap, AccountIdempotencyTombstone, OrderIntentDigest,
        ReconciledAccountState,
    };
    use market_squawk_platform::LocalPaths;
    use rust_decimal::Decimal;
    use sha2::{Digest, Sha256};
    use static_assertions::assert_not_impl_any;

    use super::{
        CHECKPOINT_OBJECT_ROOT, PaperAccountReplaySnapshot, PaperCheckpointPublicationPoint,
        PaperCheckpointReceipt, PaperCheckpointRepository, PaperCheckpointRepositoryError,
        hex_bytes, read_bounded_regular,
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
            let replay = exact_empty_replay(&checkpoint)?;

            let interrupted =
                repository.persist_with_checkpoint(&checkpoint, &replay, |observed| {
                    if observed == interrupted_at {
                        Err(PaperCheckpointRepositoryError::TestInterruption)
                    } else {
                        Ok(())
                    }
                });
            assert!(interrupted.is_err());

            let receipt = repository.persist_with_replay(&checkpoint, &replay)?;
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

            let recovered = repository.persist_with_replay(&checkpoint, &replay)?;
            assert_eq!(recovered.artifact_digest(), receipt.artifact_digest());
            assert_eq!(recovered.artifact_reference(), receipt.artifact_reference());
        }
        Ok(())
    }

    #[test]
    fn quarantined_checkpoint_cannot_be_published_as_clean_recovery() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("data"))?;
        let (config, mut checkpoint) = checkpoint_fixture()?;
        checkpoint.reconciliation_required = true;
        let mut repository = PaperCheckpointRepository::try_new(
            paths.artifacts()?.clone(),
            config,
            NonZeroUsize::new(1024 * 1024).ok_or("zero checkpoint bound")?,
        )?;
        let replay = exact_empty_replay(&checkpoint)?;

        assert!(matches!(
            repository.persist_with_replay(&checkpoint, &replay),
            Err(PaperCheckpointRepositoryError::QuarantinedCheckpoint)
        ));
        Ok(())
    }

    #[test]
    fn dirty_run_cannot_restore_an_older_clean_manifest_after_crash() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("data"))?;
        let (config, checkpoint) = checkpoint_fixture()?;
        let maximum_bytes = NonZeroUsize::new(1024 * 1024).ok_or("zero checkpoint bound")?;
        let mut repository = PaperCheckpointRepository::try_new(
            paths.artifacts()?.clone(),
            config.clone(),
            maximum_bytes,
        )?;
        let replay = exact_empty_replay(&checkpoint)?;
        repository.persist_with_replay(&checkpoint, &replay)?;
        repository.mark_run_dirty()?;
        let mut unstabilized = checkpoint.clone();
        unstabilized.sequence = 1;
        assert!(matches!(
            repository.persist_stabilized_with_replay(&unstabilized, &replay),
            Err(PaperCheckpointRepositoryError::UnstabilizedCheckpoint)
        ));
        repository.persist_with_replay(&checkpoint, &replay)?;
        drop(repository);

        assert!(matches!(
            PaperCheckpointRepository::try_new(paths.artifacts()?.clone(), config, maximum_bytes,),
            Err(PaperCheckpointRepositoryError::UncleanShutdown)
        ));
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
        let replay = exact_empty_replay(&checkpoint)?;
        let receipt = repository.persist_with_replay(&checkpoint, &replay)?;
        std::fs::write(
            paths.artifacts()?.root().join(receipt.artifact_reference()),
            b"conflicting checkpoint bytes",
        )?;
        drop(repository);

        assert!(matches!(
            PaperCheckpointRepository::try_new(paths.artifacts()?.clone(), config, maximum_bytes),
            Err(PaperCheckpointRepositoryError::VerificationFailed)
        ));
        Ok(())
    }

    #[test]
    fn repository_writer_is_exclusively_owned_for_its_full_lifetime() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("data"))?;
        let (config, _) = checkpoint_fixture()?;
        let maximum_bytes = NonZeroUsize::new(1024 * 1024).ok_or("zero checkpoint bound")?;
        let repository = PaperCheckpointRepository::try_new(
            paths.artifacts()?.clone(),
            config.clone(),
            maximum_bytes,
        )?;

        assert!(matches!(
            PaperCheckpointRepository::try_new(
                paths.artifacts()?.clone(),
                config.clone(),
                maximum_bytes,
            ),
            Err(PaperCheckpointRepositoryError::RepositoryAlreadyOwned)
        ));

        drop(repository);
        let _reopened =
            PaperCheckpointRepository::try_new(paths.artifacts()?.clone(), config, maximum_bytes)?;
        Ok(())
    }

    #[test]
    fn crash_residue_cannot_wedge_the_next_checkpoint_publication() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("data"))?;
        let (config, checkpoint) = checkpoint_fixture()?;
        let maximum_bytes = NonZeroUsize::new(1024 * 1024).ok_or("zero checkpoint bound")?;
        let repository = PaperCheckpointRepository::try_new(
            paths.artifacts()?.clone(),
            config.clone(),
            maximum_bytes,
        )?;
        let bytes = checkpoint.encode(maximum_bytes.get())?;
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let digest_hex = hex_bytes(&digest)?;
        let repository_hex = hex_bytes(&repository.binding_identity())?;
        let parent = format!("{CHECKPOINT_OBJECT_ROOT}/{}", &digest_hex[..2]);
        let stale = format!("{parent}/stage-{repository_hex}-1.tmp");
        let stale_current =
            format!("{CHECKPOINT_OBJECT_ROOT}/current-stage-{repository_hex}-1.tmp");
        std::fs::create_dir_all(paths.artifacts()?.root().join(&parent))?;
        std::fs::write(paths.artifacts()?.root().join(&stale), b"crash residue")?;
        std::fs::write(
            paths.artifacts()?.root().join(&stale_current),
            b"current crash residue",
        )?;
        drop(repository);

        let mut reopened =
            PaperCheckpointRepository::try_new(paths.artifacts()?.clone(), config, maximum_bytes)?;
        assert!(!paths.artifacts()?.root().join(&stale).exists());
        assert!(!paths.artifacts()?.root().join(&stale_current).exists());
        let replay = exact_empty_replay(&checkpoint)?;
        let receipt = reopened.persist_with_replay(&checkpoint, &replay)?;
        assert_eq!(receipt.sequence(), checkpoint.sequence());
        Ok(())
    }

    #[test]
    fn every_manifest_advance_preserves_the_explicit_replay_image() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("data"))?;
        let (config, checkpoint) = checkpoint_fixture()?;
        let maximum_bytes = NonZeroUsize::new(1024 * 1024).ok_or("zero checkpoint bound")?;
        let mut repository = PaperCheckpointRepository::try_new(
            paths.artifacts()?.clone(),
            config.clone(),
            maximum_bytes,
        )?;
        let state = checkpoint.reconciled_accounts_for_recovery()?[0].clone();
        let replay = [PaperAccountReplaySnapshot::from_reconciled_state(
            state,
            AccountIdempotencyBootstrap::try_new(
                NonZeroU64::new(7).ok_or("zero replay revision")?,
                vec![AccountIdempotencyTombstone::new(
                    OrderId::from_str("20000000-0000-0000-0000-000000000077")?,
                    ClientOrderId::try_from("persisted-client-77")?,
                    OrderIntentDigest::from_bytes([7; 32]),
                    Timestamp::from_unix_nanos(i64::MAX),
                )],
            )?,
        )];

        repository.persist_with_replay(&checkpoint, &replay)?;
        repository.persist_with_replay(&checkpoint, &replay)?;
        drop(repository);

        let mut reopened =
            PaperCheckpointRepository::try_new(paths.artifacts()?.clone(), config, maximum_bytes)?;
        let recovered = reopened.take_recovery().ok_or("missing recovery")?;
        assert_eq!(recovered.accounts().len(), replay.len());
        assert_eq!(
            recovered.accounts()[0].idempotency(),
            replay[0].idempotency()
        );
        Ok(())
    }

    #[test]
    fn account_replay_rejects_a_financial_revision_mismatch() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("data"))?;
        let (config, checkpoint) = checkpoint_fixture()?;
        let maximum_bytes = NonZeroUsize::new(1024 * 1024).ok_or("zero checkpoint bound")?;
        let mut repository =
            PaperCheckpointRepository::try_new(paths.artifacts()?.clone(), config, maximum_bytes)?;
        let paper = &checkpoint.reconciled_accounts_for_recovery()?[0];
        let replay_state = ReconciledAccountState::try_new(
            paper.account_id(),
            paper
                .revision()
                .checked_add(1)
                .ok_or("risk revision overflow")?,
            paper.eligible(),
            paper.currency(),
            paper.cash(),
            paper.settled_capital(),
            paper.marked_equity(),
            paper.peak_marked_equity(),
            paper.marked_gross_exposure(),
            paper.unrealized_pnl(),
            paper.drawdown(),
            paper.mark_digest(),
            paper.realized_pnl(),
            paper.realized_loss(),
            paper.positions().to_vec(),
            paper.position_cost_basis().to_vec(),
        )?;
        let replay = [PaperAccountReplaySnapshot::from_reconciled_state(
            replay_state,
            AccountIdempotencyBootstrap::empty(),
        )];

        assert!(matches!(
            repository.persist_with_replay(&checkpoint, &replay),
            Err(PaperCheckpointRepositoryError::InvalidReplay)
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
        let first_replay = exact_empty_replay(&checkpoint)?;
        let first = repository.persist_with_replay(&checkpoint, &first_replay)?;
        let repository_id = repository.binding_identity();
        checkpoint.accepted_repository_id = repository_id;
        checkpoint.accepted_repository_generation = first.generation().get();
        let recovered = PaperExecutionCheckpoint::decode(
            config.clone(),
            &checkpoint.encode(maximum_bytes.get())?,
            maximum_bytes.get(),
        )?;
        let paper_state = &recovered.reconciled_accounts_for_recovery()?[0];
        let risk_state = paper_state.clone();
        let replay = [PaperAccountReplaySnapshot::from_reconciled_state(
            risk_state.clone(),
            AccountIdempotencyBootstrap::empty(),
        )];
        let second = repository.persist_with_replay(&recovered, &replay)?;
        drop(repository);
        let mut reopened = PaperCheckpointRepository::try_new(
            paths.artifacts()?.clone(),
            config.clone(),
            maximum_bytes,
        )?;
        let current = reopened.take_recovery().ok_or("missing current recovery")?;
        assert_eq!(reopened.binding_identity(), repository_id);
        let mut expected_current = recovered.clone();
        expected_current.bind_current_manifest(repository_id, second.generation());
        assert_eq!(current.checkpoint(), &expected_current);
        assert_eq!(current.accounts().len(), 1);
        assert_eq!(current.accounts()[0].state(), &risk_state);
        assert_eq!(
            current.accounts()[0].idempotency(),
            &AccountIdempotencyBootstrap::empty()
        );
        let mut alternate = PaperCheckpointRepository::try_new(
            alternate_paths.artifacts()?.clone(),
            config,
            maximum_bytes,
        )?;
        let wrong_root = alternate.persist_with_replay(&recovered, &replay)?;

        assert!(!crate::worker::receipt_authority_is_current(
            repository_id,
            recovered.accepted_repository_id,
            recovered.accepted_repository_generation,
            &first,
        ));
        assert!(crate::worker::receipt_authority_is_current(
            repository_id,
            recovered.accepted_repository_id,
            recovered.accepted_repository_generation,
            &second,
        ));
        assert!(!crate::worker::receipt_authority_is_current(
            repository_id,
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

    fn exact_empty_replay(
        checkpoint: &PaperExecutionCheckpoint,
    ) -> TestResultWith<Vec<PaperAccountReplaySnapshot>> {
        Ok(checkpoint
            .reconciled_accounts_for_recovery()?
            .into_iter()
            .map(|state| {
                PaperAccountReplaySnapshot::from_reconciled_state(
                    state,
                    AccountIdempotencyBootstrap::empty(),
                )
            })
            .collect())
    }

    type TestResultWith<T> = Result<T, Box<dyn std::error::Error>>;
}
