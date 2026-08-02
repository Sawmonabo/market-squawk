//! Installed-product trusted update retrieval, staging, activation, and recovery.

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::StreamExt as _;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_installer::{
    MAXIMUM_MANIFEST_BYTES, PendingTrustedUpdate, RepairRequest, RollbackRequest, SuppliedMetadata,
    SuppliedTarget, TargetSource, TrustedRoot, TrustedUpdateReceipt, TrustedUpdateStore,
    UpdateRequest,
};
use market_squawk_platform::LocalAuthorityStateStore;
use market_squawk_services::ServiceError;
use market_squawk_sources::install_ring_tls_provider;
use reqwest::header::{ACCEPT, ACCEPT_ENCODING, CACHE_CONTROL, CONTENT_ENCODING};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::application::lifecycle::{
    ProgramGeneration, StagedUpdateCandidate, TrustedUpdateAuthority, UpdateActivation,
    UpdateActivitySnapshot, UpdateApproval, UpdateError, UpdateOutcome,
};
use crate::application::operations::{
    PreparedOperation, TrustedStagedUpdate, UpdateStatusEvidence,
};
use crate::jobs::{
    LifecycleJobExecutionError, LifecycleJobPublication, LifecycleJobPublicationError,
    UpdateJobCommand, UpdateJobRunner,
};

const STATE_SCHEMA_VERSION: u32 = 1;
const METADATA_LIMIT: usize = 1024 * 1024;
const TRUST_STATE_LIMIT: u64 = 2 * METADATA_LIMIT as u64;
const MAXIMUM_ROOT_CHAIN: usize = 32;
const MAXIMUM_STAGING_ENTRIES: usize = 128;
const TRUST_STATE_FILE: &str = "trusted-update-metadata.json";
const UPDATE_MEDIA_TYPE: &str = "application/json";
const TARGET_MEDIA_TYPE: &str = "application/octet-stream";
const DIAGNOSTIC_FAILURE: &str = "trusted-update-operation-failed";
const DIAGNOSTIC_PUBLICATION: &str = "trusted-update-publication-failed";

type ActivityReader =
    dyn Fn(u64) -> Result<UpdateActivitySnapshot, ServiceError> + Send + Sync + 'static;
type DrainFuture = Pin<Box<dyn Future<Output = Result<(), UpdateError>> + Send + 'static>>;
type DrainAuthority = dyn Fn(CancellationToken, Instant) -> DrainFuture + Send + Sync + 'static;

/// Exact HTTPS repository and signed-target selection for the installed update channel.
#[derive(Clone, Debug)]
pub(crate) struct TrustedUpdateRepository {
    base_url: Url,
    manifest_target_path: Box<str>,
    archive_target_path: Box<str>,
    pinned_root: TrustedRoot,
    pinned_root_version: u64,
}

impl TrustedUpdateRepository {
    /// Admits one pinned HTTPS origin, root, and two distinct closed target names.
    pub(crate) fn try_new(
        base_url: Url,
        pinned_root_bytes: &[u8],
        manifest_target_path: impl Into<Box<str>>,
        archive_target_path: impl Into<Box<str>>,
    ) -> Result<Self, ServiceError> {
        if base_url.scheme() != "https"
            || base_url.host_str().is_none()
            || !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
            || !base_url.path().ends_with('/')
        {
            return Err(ServiceError::InvalidRequest);
        }
        let manifest_target_path = manifest_target_path.into();
        let archive_target_path = archive_target_path.into();
        if manifest_target_path == archive_target_path
            || !valid_repository_path(&manifest_target_path)
            || !valid_repository_path(&archive_target_path)
        {
            return Err(ServiceError::InvalidRequest);
        }
        let pinned_root_version = metadata_version(pinned_root_bytes)?;
        let pinned_root = TrustedRoot::from_pinned(pinned_root_bytes)
            .map_err(|_| ServiceError::InvalidRequest)?;
        Ok(Self {
            base_url,
            manifest_target_path,
            archive_target_path,
            pinned_root,
            pinned_root_version,
        })
    }
}

/// Fixed network, disk, plan, and activation ceilings for installed updates.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ManagedUpdateLimits {
    maximum_bundle_bytes: u64,
    maximum_prepared_plans: usize,
    request_timeout: Duration,
    activation_timeout: Duration,
}

impl ManagedUpdateLimits {
    /// Creates finite bounds no broader than the installer and lifecycle authorities accept.
    pub(crate) fn try_new(
        maximum_bundle_bytes: u64,
        maximum_prepared_plans: usize,
        request_timeout: Duration,
        activation_timeout: Duration,
    ) -> Result<Self, ServiceError> {
        if maximum_bundle_bytes == 0
            || maximum_bundle_bytes > 2 * 1024 * 1024 * 1024
            || maximum_prepared_plans == 0
            || maximum_prepared_plans > 4096
            || request_timeout.is_zero()
            || request_timeout > Duration::from_secs(5 * 60)
            || activation_timeout.is_zero()
            || activation_timeout > Duration::from_secs(10 * 60)
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self {
            maximum_bundle_bytes,
            maximum_prepared_plans,
            request_timeout,
            activation_timeout,
        })
    }
}

/// Concrete installed-product implementation of the shared managed-update contract.
pub(crate) struct ManagedUpdateOperations {
    install_root: PathBuf,
    staging_root: PathBuf,
    state: LocalAuthorityStateStore,
    repository: TrustedUpdateRepository,
    lifecycle: Arc<TrustedUpdateAuthority>,
    limits: ManagedUpdateLimits,
    http: Client,
    activity: Arc<ActivityReader>,
    drain: Arc<DrainAuthority>,
    plans: Mutex<BTreeMap<SourceIdentifier, RetainedPlan>>,
    mutation: AsyncMutex<()>,
}

impl ManagedUpdateOperations {
    /// Binds the Task22 trust store and installer to application runtime preflight/drain authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "all update trust, persistence, lifecycle, and runtime authorities are explicit"
    )]
    pub(crate) fn try_new<A, D, F>(
        install_root: PathBuf,
        staging_root: PathBuf,
        state: LocalAuthorityStateStore,
        repository: TrustedUpdateRepository,
        lifecycle: Arc<TrustedUpdateAuthority>,
        limits: ManagedUpdateLimits,
        activity: A,
        drain: D,
    ) -> Result<Self, ServiceError>
    where
        A: Fn(u64) -> Result<UpdateActivitySnapshot, ServiceError> + Send + Sync + 'static,
        D: Fn(CancellationToken, Instant) -> F + Send + Sync + 'static,
        F: Future<Output = Result<(), UpdateError>> + Send + 'static,
    {
        if !install_root.is_absolute() || !staging_root.is_absolute() {
            return Err(ServiceError::InvalidRequest);
        }
        prepare_private_directory(&staging_root)?;
        install_ring_tls_provider().map_err(|_| ServiceError::Unavailable)?;
        let http = Client::builder()
            .https_only(true)
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(limits.request_timeout.min(Duration::from_secs(30)))
            .read_timeout(limits.request_timeout)
            .user_agent(concat!("market-squawk/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| ServiceError::Unavailable)?;
        let drain = Arc::new(move |cancellation, deadline| {
            Box::pin(drain(cancellation, deadline)) as DrainFuture
        });
        let operations = Self {
            install_root,
            staging_root,
            state,
            repository,
            lifecycle,
            limits,
            http,
            activity: Arc::new(activity),
            drain,
            plans: Mutex::new(BTreeMap::new()),
            mutation: AsyncMutex::new(()),
        };
        operations.load_state()?;
        operations.recover_interrupted();
        operations.cleanup_orphans()?;
        Ok(operations)
    }

    fn load_state(&self) -> Result<DurableUpdateState, ServiceError> {
        let Some(bytes) = self.state.load().map_err(|_| ServiceError::Unavailable)? else {
            return Ok(DurableUpdateState::empty());
        };
        let state: DurableUpdateState =
            serde_json::from_slice(&bytes).map_err(|_| ServiceError::Unavailable)?;
        state.validate()?;
        Ok(state)
    }

    fn store_state(&self, state: &DurableUpdateState) -> Result<(), ServiceError> {
        state.validate()?;
        let bytes = serde_json::to_vec(state).map_err(|_| ServiceError::Internal)?;
        self.state
            .store(&bytes)
            .map_err(|_| ServiceError::Unavailable)
    }

    fn installer_status(&self) -> Result<market_squawk_installer::InstallStatus, ServiceError> {
        market_squawk_installer::status(&self.install_root).map_err(|_| ServiceError::Unavailable)
    }

    fn recover_interrupted(&self) {
        let Ok(mut state) = self.load_state() else {
            return;
        };
        let Some(transition) = state.transition.clone() else {
            return;
        };
        let Ok(current) = self.lifecycle.current() else {
            return;
        };
        let Ok(status) = self.installer_status() else {
            return;
        };
        let current_value = current.get();
        let activated_generation = transition.previous_generation.checked_add(1);
        let rollback_generation = transition.previous_generation.checked_add(2);
        let candidate_active =
            status.active_version() == Some(transition.candidate_version.as_ref());

        let recovered = if Some(current_value) == activated_generation
            && candidate_active
            && status.is_healthy()
        {
            Some((DurableOutcome::Activated, current_value))
        } else if Some(current_value) == rollback_generation
            && !candidate_active
            && status.is_healthy()
        {
            Some((DurableOutcome::RolledBack, current_value))
        } else if current_value == transition.previous_generation {
            if candidate_active {
                if market_squawk_installer::rollback(RollbackRequest::new(
                    self.install_root.clone(),
                ))
                .is_err()
                {
                    return;
                }
            }
            let Ok(recovered_status) = self.installer_status() else {
                return;
            };
            if recovered_status.is_healthy()
                && recovered_status.active_version() != Some(transition.candidate_version.as_ref())
            {
                Some((DurableOutcome::RecoveredKnownGood, current_value))
            } else {
                None
            }
        } else {
            None
        };
        let Some((outcome, active_generation)) = recovered else {
            return;
        };
        state.last_receipt = Some(DurableUpdateReceipt {
            operation_identity: transition.operation_identity,
            candidate_version: transition.candidate_version,
            attempted_generation: transition
                .previous_generation
                .checked_add(1)
                .unwrap_or(transition.previous_generation),
            active_generation,
            outcome,
            completed_at: now_timestamp().unwrap_or(transition.started_at),
        });
        let retired = state.staged.take();
        state.transition = None;
        if self.store_state(&state).is_ok() {
            if let Some(retired) = retired {
                let _ignored = self.remove_stage(&retired.directory);
            }
        }
    }

    fn cleanup_orphans(&self) -> Result<(), ServiceError> {
        let state = self.load_state()?;
        let retained = state.staged.as_ref().map(|stage| stage.directory.as_ref());
        let mut observed = 0_usize;
        for entry in fs::read_dir(&self.staging_root).map_err(|_| ServiceError::Unavailable)? {
            let entry = entry.map_err(|_| ServiceError::Unavailable)?;
            observed = observed
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?;
            if observed > MAXIMUM_STAGING_ENTRIES {
                return Err(ServiceError::ResourceExhausted);
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| ServiceError::Unavailable)?;
            if !valid_stage_directory_name(&name) {
                return Err(ServiceError::Unavailable);
            }
            if retained != Some(name.as_str()) {
                self.remove_stage(&name)?;
            }
        }
        Ok(())
    }

    fn remove_stage(&self, directory: &str) -> Result<(), ServiceError> {
        if !valid_stage_directory_name(directory) {
            return Err(ServiceError::Unavailable);
        }
        let path = self.staging_root.join(directory);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                fs::remove_dir_all(path).map_err(|_| ServiceError::Unavailable)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            _ => Err(ServiceError::Unavailable),
        }
    }

    async fn stage_fresh(
        &self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<DurableStagedUpdate, ServiceError> {
        ensure_live(cancellation, deadline)?;
        let directory = format!("stage-{}", Uuid::new_v4().as_simple());
        let stage_root = self.staging_root.join(&directory);
        create_private_stage(&stage_root)?;
        let result = self
            .download_and_admit(&stage_root, directory.clone(), cancellation, deadline)
            .await;
        if result.is_err() {
            let _ignored = self.remove_stage(&directory);
        }
        result
    }

    async fn download_and_admit(
        &self,
        stage_root: &Path,
        directory: String,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<DurableStagedUpdate, ServiceError> {
        let trusted_root_version =
            trusted_root_version(&self.install_root, self.repository.pinned_root_version)?;
        let mut roots = Vec::new();
        for offset in 1..=MAXIMUM_ROOT_CHAIN.saturating_add(1) {
            let version = trusted_root_version
                .checked_add(u64::try_from(offset).map_err(|_| ServiceError::Internal)?)
                .ok_or(ServiceError::ResourceExhausted)?;
            let path = format!("{version}.root.json");
            let Some(bytes) = self
                .download_metadata(&path, true, cancellation, deadline)
                .await?
            else {
                break;
            };
            if offset > MAXIMUM_ROOT_CHAIN || metadata_version(&bytes)? != version {
                return Err(ServiceError::ResourceExhausted);
            }
            roots.push(bytes);
        }

        let timestamp = self
            .download_metadata("timestamp.json", false, cancellation, deadline)
            .await?
            .ok_or(ServiceError::Unavailable)?;
        let timestamp_route: TimestampRouting = signed_metadata(&timestamp)?;
        let snapshot_description = timestamp_route
            .meta
            .get("snapshot.json")
            .ok_or(ServiceError::Unavailable)?;
        let snapshot_path = format!("{}.snapshot.json", snapshot_description.version);
        let snapshot = self
            .download_metadata(&snapshot_path, false, cancellation, deadline)
            .await?
            .ok_or(ServiceError::Unavailable)?;
        verify_routing_identity(&snapshot, snapshot_description)?;

        let snapshot_route: SnapshotRouting = signed_metadata(&snapshot)?;
        let targets_description = snapshot_route
            .meta
            .get("targets.json")
            .ok_or(ServiceError::Unavailable)?;
        let targets_path = format!("{}.targets.json", targets_description.version);
        let targets = self
            .download_metadata(&targets_path, false, cancellation, deadline)
            .await?
            .ok_or(ServiceError::Unavailable)?;
        verify_routing_identity(&targets, targets_description)?;
        let target_route: TargetsRouting = signed_metadata(&targets)?;
        if target_route.targets.len() > 512 {
            return Err(ServiceError::ResourceExhausted);
        }
        let manifest_route = target_route
            .targets
            .get(self.repository.manifest_target_path.as_ref())
            .ok_or(ServiceError::Unavailable)?;
        let archive_route = target_route
            .targets
            .get(self.repository.archive_target_path.as_ref())
            .ok_or(ServiceError::Unavailable)?;
        let manifest_identity = target_identity(
            self.repository.manifest_target_path.as_ref(),
            manifest_route,
            MAXIMUM_MANIFEST_BYTES as u64,
        )?;
        let archive_identity = target_identity(
            self.repository.archive_target_path.as_ref(),
            archive_route,
            self.limits.maximum_bundle_bytes,
        )?;
        let compatibility = candidate_compatibility(manifest_route)?;

        let manifest_file = "manifest.json";
        let bundle_file = "bundle.bin";
        self.download_target(
            &manifest_identity.download_path,
            stage_root.join(manifest_file),
            manifest_identity.length,
            manifest_identity.sha256,
            cancellation,
            deadline,
        )
        .await?;
        self.download_target(
            &archive_identity.download_path,
            stage_root.join(bundle_file),
            archive_identity.length,
            archive_identity.sha256,
            cancellation,
            deadline,
        )
        .await?;

        persist_metadata(stage_root, &roots, &timestamp, &snapshot, &targets).await?;
        let manifest_path = stage_root.join(manifest_file);
        let bundle_path = stage_root.join(bundle_file);
        let manifest = read_verified_file(
            &manifest_path,
            manifest_identity.length,
            manifest_identity.sha256,
            MAXIMUM_MANIFEST_BYTES as u64,
        )?;
        let release = market_squawk_installer::ReleaseManifest::admit_current(&manifest)
            .map_err(|_| ServiceError::Unavailable)?;
        if release.version() != compatibility.release_version.as_ref()
            || decode_sha256(release.manifest_sha256())? != manifest_identity.sha256
        {
            return Err(ServiceError::Unavailable);
        }

        let trusted_time = Utc::now();
        let trusted_receipt = admit_and_persist_trust(
            self.install_root.clone(),
            self.repository.pinned_root.clone(),
            roots.clone(),
            timestamp.clone(),
            snapshot_path.clone(),
            snapshot.clone(),
            targets_path.clone(),
            targets.clone(),
            self.repository.manifest_target_path.clone(),
            manifest_identity.download_path.clone().into_boxed_str(),
            manifest_path.clone(),
            self.repository.archive_target_path.clone(),
            archive_identity.download_path.clone().into_boxed_str(),
            bundle_path.clone(),
            trusted_time,
            cancellation,
            deadline,
        )
        .await?;
        ensure_live(cancellation, deadline)?;

        let trusted_metadata_sha256 =
            metadata_chain_digest(&roots, &timestamp, &snapshot, &targets);
        let candidate = StagedUpdateCandidate::try_from_trusted_metadata(
            release.version(),
            trusted_metadata_sha256,
            manifest_identity.sha256,
            archive_identity.sha256,
            archive_identity.length,
            compatibility.minimum_schema_version,
            compatibility.maximum_schema_version,
        )
        .map_err(|_| ServiceError::Unavailable)?;
        let checked_at = now_timestamp()?;
        let mut staged = DurableStagedUpdate {
            directory: directory.into_boxed_str(),
            candidate,
            version: release.version().into(),
            checked_at,
            metadata: DurableMetadata {
                root_files: (0..roots.len())
                    .map(|index| format!("root-{index}.json").into_boxed_str())
                    .collect(),
                timestamp_file: "timestamp.json".into(),
                snapshot_file: "snapshot.json".into(),
                targets_file: "targets.json".into(),
                snapshot_path: snapshot_path.into_boxed_str(),
                targets_path: targets_path.into_boxed_str(),
                manifest_target_path: self.repository.manifest_target_path.clone(),
                manifest_download_path: manifest_identity.download_path.into_boxed_str(),
                archive_target_path: self.repository.archive_target_path.clone(),
                archive_download_path: archive_identity.download_path.into_boxed_str(),
                trusted_root_version: trusted_receipt.root_version(),
                trusted_timestamp_version: trusted_receipt.timestamp_version(),
                trusted_snapshot_version: trusted_receipt.snapshot_version(),
                trusted_targets_version: trusted_receipt.targets_version(),
            },
            manifest_file: manifest_file.into(),
            bundle_file: bundle_file.into(),
            bundle_bytes: archive_identity.length,
            stage_sha256: [0; 32],
        };
        staged.stage_sha256 = staged_digest(&staged)?;
        staged.validate()?;
        Ok(staged)
    }

    async fn download_metadata(
        &self,
        relative: &str,
        absent_is_end: bool,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<Option<Vec<u8>>, ServiceError> {
        if !valid_repository_path(relative) {
            return Err(ServiceError::Unavailable);
        }
        let response = self
            .send(relative, UPDATE_MEDIA_TYPE, cancellation, deadline)
            .await?;
        if absent_is_end && response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if response.status() != StatusCode::OK {
            return Err(ServiceError::Unavailable);
        }
        validate_response_encoding(&response)?;
        if response
            .content_length()
            .is_some_and(|length| length > METADATA_LIMIT as u64)
        {
            return Err(ServiceError::ResourceExhausted);
        }
        let mut stream = response.bytes_stream();
        let mut result = Vec::new();
        loop {
            let chunk = next_chunk(&mut stream, cancellation, deadline).await?;
            let Some(chunk) = chunk else {
                break;
            };
            if result.len().saturating_add(chunk.len()) > METADATA_LIMIT {
                return Err(ServiceError::ResourceExhausted);
            }
            result.extend_from_slice(&chunk);
        }
        if result.is_empty() {
            return Err(ServiceError::Unavailable);
        }
        Ok(Some(result))
    }

    async fn download_target(
        &self,
        relative: &str,
        destination: PathBuf,
        expected_length: u64,
        expected_sha256: [u8; 32],
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), ServiceError> {
        let response = self
            .send(relative, TARGET_MEDIA_TYPE, cancellation, deadline)
            .await?;
        if response.status() != StatusCode::OK {
            return Err(ServiceError::Unavailable);
        }
        validate_response_encoding(&response)?;
        if response
            .content_length()
            .is_some_and(|length| length != expected_length)
        {
            return Err(ServiceError::Unavailable);
        }
        let (sender, mut receiver) = mpsc::channel::<Bytes>(2);
        let writer =
            tokio::task::spawn_blocking(move || -> Result<(u64, [u8; 32]), ServiceError> {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&destination)
                    .map_err(|_| ServiceError::Unavailable)?;
                let mut total = 0_u64;
                let mut digest = Sha256::new();
                while let Some(chunk) = receiver.blocking_recv() {
                    total = total
                        .checked_add(
                            u64::try_from(chunk.len()).map_err(|_| ServiceError::Unavailable)?,
                        )
                        .ok_or(ServiceError::ResourceExhausted)?;
                    if total > expected_length {
                        return Err(ServiceError::Unavailable);
                    }
                    file.write_all(&chunk)
                        .map_err(|_| ServiceError::Unavailable)?;
                    digest.update(&chunk);
                }
                file.sync_all().map_err(|_| ServiceError::Unavailable)?;
                Ok((total, digest.finalize().into()))
            });
        let mut stream = response.bytes_stream();
        let transfer = async {
            loop {
                let Some(chunk) = next_chunk(&mut stream, cancellation, deadline).await? else {
                    break;
                };
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
                    () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                        return Err(ServiceError::DeadlineExceeded);
                    }
                    result = sender.send(chunk) => {
                        result.map_err(|_| ServiceError::Unavailable)?;
                    }
                }
            }
            Ok::<(), ServiceError>(())
        }
        .await;
        drop(sender);
        let written = writer.await.map_err(|_| ServiceError::Unavailable)?;
        transfer?;
        let (length, sha256) = written?;
        if length != expected_length || sha256 != expected_sha256 {
            return Err(ServiceError::Unavailable);
        }
        Ok(())
    }

    async fn send(
        &self,
        relative: &str,
        accept: &'static str,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<reqwest::Response, ServiceError> {
        ensure_live(cancellation, deadline)?;
        let url = self
            .repository
            .base_url
            .join(relative)
            .map_err(|_| ServiceError::Unavailable)?;
        if url.origin() != self.repository.base_url.origin()
            || !url.path().starts_with(self.repository.base_url.path())
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(ServiceError::Unavailable);
        }
        let timeout = remaining(deadline)?.min(self.limits.request_timeout);
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                Err(ServiceError::DeadlineExceeded)
            }
            response = self.http
                .get(url.clone())
                .version(reqwest::Version::HTTP_11)
                .header(ACCEPT, accept)
                .header(ACCEPT_ENCODING, "identity")
                .header(CACHE_CONTROL, "no-cache")
                .timeout(timeout)
                .send() => {
                    let response = response.map_err(|error| {
                        if error.is_timeout() {
                            ServiceError::DeadlineExceeded
                        } else {
                            ServiceError::Unavailable
                        }
                    })?;
                    if response.url() != &url {
                        return Err(ServiceError::Unavailable);
                    }
                    Ok(response)
                }
        }
    }

    fn load_stage_files(&self, stage: &DurableStagedUpdate) -> Result<LoadedStage, ServiceError> {
        stage.validate()?;
        let root = self.staging_root.join(stage.directory.as_ref());
        validate_stage_root(&root)?;
        let roots = stage
            .metadata
            .root_files
            .iter()
            .map(|name| read_bounded_regular(&root.join(name.as_ref()), METADATA_LIMIT as u64))
            .collect::<Result<Vec<_>, _>>()?;
        let timestamp = read_bounded_regular(
            &root.join(stage.metadata.timestamp_file.as_ref()),
            METADATA_LIMIT as u64,
        )?;
        let snapshot = read_bounded_regular(
            &root.join(stage.metadata.snapshot_file.as_ref()),
            METADATA_LIMIT as u64,
        )?;
        let targets = read_bounded_regular(
            &root.join(stage.metadata.targets_file.as_ref()),
            METADATA_LIMIT as u64,
        )?;
        let manifest = read_bounded_regular(
            &root.join(stage.manifest_file.as_ref()),
            MAXIMUM_MANIFEST_BYTES as u64,
        )?;
        Ok(LoadedStage {
            roots,
            timestamp,
            snapshot,
            targets,
            manifest,
            manifest_path: root.join(stage.manifest_file.as_ref()),
            bundle_path: root.join(stage.bundle_file.as_ref()),
        })
    }

    async fn pending_request(
        &self,
        stage: &DurableStagedUpdate,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<UpdateRequest, LifecycleJobExecutionError> {
        let loaded = self
            .load_stage_files(stage)
            .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?;
        let install_root = self.install_root.clone();
        let pinned_root = self.repository.pinned_root.clone();
        let metadata = stage.metadata.clone();
        let trusted_time = Utc::now();
        let mut worker = tokio::task::spawn_blocking(move || {
            let root_refs = loaded.roots.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let store =
                TrustedUpdateStore::open_or_bootstrap(&install_root, pinned_root, trusted_time)
                    .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?;
            let supplied = SuppliedMetadata {
                root_chain: &root_refs,
                timestamp: &loaded.timestamp,
                snapshot_path: &metadata.snapshot_path,
                snapshot: &loaded.snapshot,
                targets_path: &metadata.targets_path,
                targets: &loaded.targets,
            };
            let targets = [
                SuppliedTarget {
                    metadata_path: &metadata.manifest_target_path,
                    download_path: &metadata.manifest_download_path,
                    source: TargetSource::File(&loaded.manifest_path),
                },
                SuppliedTarget {
                    metadata_path: &metadata.archive_target_path,
                    download_path: &metadata.archive_download_path,
                    source: TargetSource::File(&loaded.bundle_path),
                },
            ];
            let pending = store
                .admit(supplied, &targets, trusted_time)
                .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?;
            UpdateRequest::from_trusted_local(
                install_root,
                &loaded.manifest,
                &loaded.bundle_path,
                pending,
                &metadata.manifest_target_path,
                &metadata.archive_target_path,
            )
            .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))
        });
        let mut interrupted = false;
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                interrupted = true;
                (&mut worker).await
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                interrupted = true;
                (&mut worker).await
            }
            result = &mut worker => result,
        }
        .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?;
        if interrupted {
            return Err(LifecycleJobExecutionError::Cancelled);
        }
        result
    }
}

#[async_trait]
impl crate::application::operations::ManagedUpdateOperations for ManagedUpdateOperations {
    fn status(&self, current: ProgramGeneration) -> Result<UpdateStatusEvidence, ServiceError> {
        self.recover_interrupted();
        let state = self.load_state()?;
        let installer = self.installer_status()?;
        let known_good = installer
            .previous_version()
            .or_else(|| installer.active_version())
            .ok_or(ServiceError::Unavailable)?;
        let staged_candidate = state.staged.as_ref().map(|stage| stage.candidate.clone());
        UpdateStatusEvidence::try_new(
            current,
            known_good,
            staged_candidate,
            state.last_checked_at,
            state.transition.is_some() || !installer.is_healthy(),
        )
    }

    async fn check_and_stage(
        &self,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<TrustedStagedUpdate, ServiceError> {
        let _mutation = self.mutation.lock().await;
        ensure_live(&cancellation, deadline)?;
        let mut state = self.load_state()?;
        if state.transition.is_some() {
            return Err(ServiceError::Unavailable);
        }
        let installer = self.installer_status()?;
        if !installer.is_installed() || !installer.is_healthy() {
            return Err(ServiceError::Unavailable);
        }
        if let Some(staged) = state.staged.as_ref() {
            if installer.active_version() != Some(staged.version.as_ref()) {
                let activity = (self.activity)(staged.candidate_bundle_bytes())?;
                return Ok(TrustedStagedUpdate::new(staged.candidate.clone(), activity));
            }
            let retired = state.staged.take().ok_or(ServiceError::Internal)?;
            self.store_state(&state)?;
            self.remove_stage(&retired.directory)?;
        }

        let staged = self.stage_fresh(&cancellation, deadline).await?;
        let active_version = installer
            .active_version()
            .ok_or(ServiceError::Unavailable)?;
        state.last_checked_at = Some(staged.checked_at);
        if !strictly_newer_semver(staged.version.as_ref(), active_version)? {
            self.store_state(&state)?;
            self.remove_stage(&staged.directory)?;
            return Err(ServiceError::NotFound);
        }
        let activity = (self.activity)(staged.candidate_bundle_bytes())?;
        let candidate = staged.candidate.clone();
        let retired = state.staged.replace(staged);
        self.store_state(&state)?;
        if let Some(retired) = retired {
            self.remove_stage(&retired.directory)?;
        }
        Ok(TrustedStagedUpdate::new(candidate, activity))
    }

    fn current_staged(&self) -> Result<TrustedStagedUpdate, ServiceError> {
        let state = self.load_state()?;
        if state.transition.is_some() {
            return Err(ServiceError::Unavailable);
        }
        let staged = state.staged.ok_or(ServiceError::NotFound)?;
        let activity = (self.activity)(staged.candidate_bundle_bytes())?;
        Ok(TrustedStagedUpdate::new(staged.candidate, activity))
    }

    fn prepare_update(&self, approval: UpdateApproval) -> Result<PreparedOperation, ServiceError> {
        let state = self.load_state()?;
        if state.transition.is_some() {
            return Err(ServiceError::Unavailable);
        }
        let staged = state.staged.ok_or(ServiceError::NotFound)?;
        let mut plans = self.plans.lock().map_err(|_| ServiceError::Unavailable)?;
        if plans.len() >= self.limits.maximum_prepared_plans {
            return Err(ServiceError::ResourceExhausted);
        }
        let operation_identity = SourceIdentifier::try_from(format!(
            "trusted-update-plan-{}",
            Uuid::new_v4().as_simple()
        ))
        .map_err(|_| ServiceError::Internal)?;
        let approval_binding = format!("{approval:?}");
        let encoded = serde_json::to_vec(&(
            "market-squawk-managed-update-plan-v1",
            &operation_identity,
            staged.stage_sha256,
            approval_binding,
        ))
        .map_err(|_| ServiceError::Internal)?;
        let evidence_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(encoded).into());
        let prepared = PreparedOperation::try_new(operation_identity.clone(), evidence_digest)?;
        match plans.entry(operation_identity.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(RetainedPlan {
                    evidence_digest,
                    stage_sha256: staged.stage_sha256,
                    approval,
                });
            }
            Entry::Occupied(_) => return Err(ServiceError::Unavailable),
        }
        Ok(prepared)
    }

    fn revoke(&self, operation: &PreparedOperation) {
        let Ok(mut plans) = self.plans.lock() else {
            return;
        };
        let should_remove = plans
            .get(operation.identity())
            .is_some_and(|plan| plan.evidence_digest == operation.evidence_digest());
        if should_remove {
            plans.remove(operation.identity());
        }
    }

    async fn execute(
        &self,
        command: UpdateJobCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        let plan = {
            let mut plans = self
                .plans
                .lock()
                .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?;
            let plan = plans
                .remove(command.identity())
                .ok_or_else(|| execution_failure(DIAGNOSTIC_FAILURE, false))?;
            if plan.evidence_digest != command.evidence_digest() {
                return Err(execution_failure(DIAGNOSTIC_FAILURE, false));
            }
            plan
        };
        if cancellation.is_cancelled() {
            return Err(LifecycleJobExecutionError::Cancelled);
        }
        if deadline <= Instant::now() {
            return Err(execution_failure(DIAGNOSTIC_FAILURE, false));
        }
        let _mutation = self.mutation.lock().await;
        let mut state = self
            .load_state()
            .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?;
        let staged = state
            .staged
            .clone()
            .ok_or_else(|| execution_failure(DIAGNOSTIC_FAILURE, false))?;
        if state.transition.is_some() || staged.stage_sha256 != plan.stage_sha256 {
            return Err(execution_failure(DIAGNOSTIC_FAILURE, false));
        }
        let request = self
            .pending_request(&staged, &cancellation, deadline)
            .await?;
        if cancellation.is_cancelled() {
            return Err(LifecycleJobExecutionError::Cancelled);
        }
        let result = UpdateJobRunner::try_result_reference(
            command.identity().clone(),
            command.evidence_digest(),
            Vec::new(),
        )
        .map_err(|_| execution_failure(DIAGNOSTIC_PUBLICATION, false))?;
        publication.prepare_and_claim(result)?;

        let current = self
            .lifecycle
            .current()
            .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?;
        let started_at =
            now_timestamp().map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?;
        state.transition = Some(DurableTransition {
            operation_identity: command.identity().clone(),
            evidence_sha256: command.evidence_digest().bytes(),
            candidate_version: staged.version.clone(),
            previous_generation: current.get(),
            started_at,
        });
        self.store_state(&state)
            .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?;

        let activation = InstallerUpdateActivation::new(
            self.install_root.clone(),
            staged.version.clone(),
            request,
            cancellation.clone(),
            deadline,
            Arc::clone(&self.drain),
        );
        let timeout = remaining(deadline)
            .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?
            .min(self.limits.activation_timeout);
        let outcome = self
            .lifecycle
            .activate(plan.approval, &activation, timeout)
            .await;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(_) if activation.selector_state() == SelectorState::NotStarted => {
                state.transition = None;
                self.store_state(&state)
                    .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?;
                if cancellation.is_cancelled() {
                    return Err(LifecycleJobExecutionError::Cancelled);
                }
                return Err(execution_failure(DIAGNOSTIC_FAILURE, false));
            }
            Err(_) => return Err(execution_failure(DIAGNOSTIC_FAILURE, false)),
        };
        let (outcome_kind, receipt) = match outcome {
            UpdateOutcome::Activated(receipt) => (DurableOutcome::Activated, receipt),
            UpdateOutcome::RolledBack(receipt) => (DurableOutcome::RolledBack, receipt),
        };
        let attempted_generation = current
            .get()
            .checked_add(1)
            .ok_or_else(|| execution_failure(DIAGNOSTIC_FAILURE, false))?;
        state.last_receipt = Some(DurableUpdateReceipt {
            operation_identity: command.identity().clone(),
            candidate_version: staged.version.clone(),
            attempted_generation,
            active_generation: receipt.active_generation().get(),
            outcome: outcome_kind,
            completed_at: now_timestamp()
                .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?,
        });
        state.transition = None;
        state.staged = None;
        self.store_state(&state)
            .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?;
        self.remove_stage(&staged.directory)
            .map_err(|_| execution_failure(DIAGNOSTIC_FAILURE, false))?;
        publication.commit_succeeded();
        Ok(())
    }
}

impl fmt::Debug for ManagedUpdateOperations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedUpdateOperations")
            .field("install_root", &"[controlled install root]")
            .field("staging_root", &"[controlled staging root]")
            .field("repository", &"[pinned HTTPS origin]")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct RetainedPlan {
    evidence_digest: EvidenceDigest,
    stage_sha256: [u8; 32],
    approval: UpdateApproval,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableUpdateState {
    schema_version: u32,
    staged: Option<DurableStagedUpdate>,
    last_checked_at: Option<Timestamp>,
    transition: Option<DurableTransition>,
    last_receipt: Option<DurableUpdateReceipt>,
}

impl DurableUpdateState {
    const fn empty() -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            staged: None,
            last_checked_at: None,
            transition: None,
            last_receipt: None,
        }
    }

    fn validate(&self) -> Result<(), ServiceError> {
        if self.schema_version != STATE_SCHEMA_VERSION
            || self.transition.is_some() && self.staged.is_none()
        {
            return Err(ServiceError::Unavailable);
        }
        if let Some(stage) = &self.staged {
            stage.validate()?;
        }
        if let Some(transition) = &self.transition {
            transition.validate()?;
        }
        if let Some(receipt) = &self.last_receipt {
            receipt.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableStagedUpdate {
    directory: Box<str>,
    candidate: StagedUpdateCandidate,
    version: Box<str>,
    checked_at: Timestamp,
    metadata: DurableMetadata,
    manifest_file: Box<str>,
    bundle_file: Box<str>,
    bundle_bytes: u64,
    stage_sha256: [u8; 32],
}

impl DurableStagedUpdate {
    fn validate(&self) -> Result<(), ServiceError> {
        if !valid_stage_directory_name(&self.directory)
            || !valid_stage_file_name(&self.manifest_file)
            || !valid_stage_file_name(&self.bundle_file)
            || self.manifest_file == self.bundle_file
            || self.version.is_empty()
            || self.version.len() > 128
            || self.bundle_bytes == 0
            || self.stage_sha256 == [0; 32]
            || staged_digest(self)? != self.stage_sha256
        {
            return Err(ServiceError::Unavailable);
        }
        self.metadata.validate()
    }

    fn candidate_bundle_bytes(&self) -> u64 {
        self.bundle_bytes
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableMetadata {
    root_files: Vec<Box<str>>,
    timestamp_file: Box<str>,
    snapshot_file: Box<str>,
    targets_file: Box<str>,
    snapshot_path: Box<str>,
    targets_path: Box<str>,
    manifest_target_path: Box<str>,
    manifest_download_path: Box<str>,
    archive_target_path: Box<str>,
    archive_download_path: Box<str>,
    trusted_root_version: u64,
    trusted_timestamp_version: u64,
    trusted_snapshot_version: u64,
    trusted_targets_version: u64,
}

impl DurableMetadata {
    fn validate(&self) -> Result<(), ServiceError> {
        if self.root_files.len() > MAXIMUM_ROOT_CHAIN
            || self
                .root_files
                .iter()
                .any(|name| !valid_stage_file_name(name))
            || !valid_stage_file_name(&self.timestamp_file)
            || !valid_stage_file_name(&self.snapshot_file)
            || !valid_stage_file_name(&self.targets_file)
            || !valid_repository_path(&self.snapshot_path)
            || !valid_repository_path(&self.targets_path)
            || !valid_repository_path(&self.manifest_target_path)
            || !valid_repository_path(&self.manifest_download_path)
            || !valid_repository_path(&self.archive_target_path)
            || !valid_repository_path(&self.archive_download_path)
            || self.trusted_root_version == 0
            || self.trusted_timestamp_version == 0
            || self.trusted_snapshot_version == 0
            || self.trusted_targets_version == 0
        {
            return Err(ServiceError::Unavailable);
        }
        let names = self
            .root_files
            .iter()
            .map(|name| name.as_ref())
            .chain([
                self.timestamp_file.as_ref(),
                self.snapshot_file.as_ref(),
                self.targets_file.as_ref(),
            ])
            .collect::<BTreeSet<_>>();
        if names.len() != self.root_files.len().saturating_add(3) {
            return Err(ServiceError::Unavailable);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableTransition {
    operation_identity: SourceIdentifier,
    evidence_sha256: [u8; 32],
    candidate_version: Box<str>,
    previous_generation: u64,
    started_at: Timestamp,
}

impl DurableTransition {
    fn validate(&self) -> Result<(), ServiceError> {
        if self.evidence_sha256 == [0; 32]
            || self.candidate_version.is_empty()
            || self.candidate_version.len() > 128
            || self.previous_generation == 0
        {
            return Err(ServiceError::Unavailable);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum DurableOutcome {
    Activated,
    RolledBack,
    RecoveredKnownGood,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableUpdateReceipt {
    operation_identity: SourceIdentifier,
    candidate_version: Box<str>,
    attempted_generation: u64,
    active_generation: u64,
    outcome: DurableOutcome,
    completed_at: Timestamp,
}

impl DurableUpdateReceipt {
    fn validate(&self) -> Result<(), ServiceError> {
        if self.candidate_version.is_empty()
            || self.candidate_version.len() > 128
            || self.attempted_generation == 0
            || self.active_generation == 0
        {
            return Err(ServiceError::Unavailable);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct LoadedStage {
    roots: Vec<Vec<u8>>,
    timestamp: Vec<u8>,
    snapshot: Vec<u8>,
    targets: Vec<u8>,
    manifest: Vec<u8>,
    manifest_path: PathBuf,
    bundle_path: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectorState {
    NotStarted,
    Unchanged,
    Changed,
    Indeterminate,
}

struct InstallerUpdateActivation {
    install_root: PathBuf,
    candidate_version: Box<str>,
    request: Mutex<Option<UpdateRequest>>,
    cancellation: CancellationToken,
    deadline: Instant,
    drain: Arc<DrainAuthority>,
    selector: Mutex<SelectorState>,
}

impl InstallerUpdateActivation {
    fn new(
        install_root: PathBuf,
        candidate_version: Box<str>,
        request: UpdateRequest,
        cancellation: CancellationToken,
        deadline: Instant,
        drain: Arc<DrainAuthority>,
    ) -> Self {
        Self {
            install_root,
            candidate_version,
            request: Mutex::new(Some(request)),
            cancellation,
            deadline,
            drain,
            selector: Mutex::new(SelectorState::NotStarted),
        }
    }

    fn selector_state(&self) -> SelectorState {
        self.selector
            .lock()
            .map_or(SelectorState::Indeterminate, |state| *state)
    }

    fn set_selector_state(&self, state: SelectorState) {
        if let Ok(mut selector) = self.selector.lock() {
            *selector = state;
        }
    }

    async fn run_installer<T, F>(&self, operation: F) -> Result<T, UpdateError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, market_squawk_installer::InstallError> + Send + 'static,
    {
        let mut worker = tokio::task::spawn_blocking(operation);
        let mut interrupted = false;
        let result = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => {
                interrupted = true;
                (&mut worker).await
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(self.deadline)) => {
                interrupted = true;
                (&mut worker).await
            }
            result = &mut worker => result,
        };
        let value = result
            .map_err(|_| UpdateError::ActivationFailed)?
            .map_err(|_| UpdateError::ActivationFailed)?;
        if interrupted {
            return Err(UpdateError::ActivationFailed);
        }
        Ok(value)
    }
}

impl fmt::Debug for InstallerUpdateActivation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstallerUpdateActivation")
            .field("install_root", &"[controlled install root]")
            .field("candidate_version", &self.candidate_version)
            .field("selector", &self.selector_state())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl UpdateActivation for InstallerUpdateActivation {
    async fn drain_and_reconcile(&self, deadline: Instant) -> Result<(), UpdateError> {
        if self.cancellation.is_cancelled() || deadline > self.deadline {
            return Err(UpdateError::ActivationFailed);
        }
        (self.drain)(self.cancellation.clone(), deadline).await
    }

    async fn activate(
        &self,
        _candidate: &StagedUpdateCandidate,
        _attempted_generation: ProgramGeneration,
    ) -> Result<(), UpdateError> {
        let request = self
            .request
            .lock()
            .map_err(|_| UpdateError::ActivationFailed)?
            .take()
            .ok_or(UpdateError::ActivationFailed)?;
        let root = self.install_root.clone();
        let candidate_version = self.candidate_version.clone();
        let result = self
            .run_installer(move || market_squawk_installer::update(request))
            .await;
        match result {
            Ok(receipt) if receipt.version() == candidate_version.as_ref() => {
                self.set_selector_state(SelectorState::Changed);
                Ok(())
            }
            Ok(_) => {
                self.set_selector_state(SelectorState::Indeterminate);
                Err(UpdateError::ActivationFailed)
            }
            Err(error) => {
                let status = market_squawk_installer::status(&root);
                match status {
                    Ok(status) if status.active_version() == Some(candidate_version.as_ref()) => {
                        self.set_selector_state(SelectorState::Changed);
                    }
                    Ok(status) if status.is_healthy() => {
                        self.set_selector_state(SelectorState::Unchanged);
                    }
                    _ => self.set_selector_state(SelectorState::Indeterminate),
                }
                Err(error)
            }
        }
    }

    async fn restart_and_health_check(
        &self,
        _generation: ProgramGeneration,
    ) -> Result<(), UpdateError> {
        let root = self.install_root.clone();
        let candidate = self.candidate_version.clone();
        let status = self
            .run_installer(move || market_squawk_installer::status(&root))
            .await
            .map_err(|_| UpdateError::HealthCheckFailed)?;
        if status.is_healthy() && status.active_version() == Some(candidate.as_ref()) {
            Ok(())
        } else {
            Err(UpdateError::HealthCheckFailed)
        }
    }

    async fn rollback_known_good(&self, _generation: ProgramGeneration) -> Result<(), UpdateError> {
        let selector = self.selector_state();
        let root = self.install_root.clone();
        let candidate = self.candidate_version.clone();
        match selector {
            SelectorState::Changed => {
                let rollback_root = root.clone();
                self.run_installer(move || {
                    market_squawk_installer::rollback(RollbackRequest::new(rollback_root))
                })
                .await
                .map_err(|_| UpdateError::RollbackFailed)?;
            }
            SelectorState::Unchanged | SelectorState::NotStarted => {
                let repair_root = root.clone();
                self.run_installer(move || {
                    market_squawk_installer::repair(RepairRequest::new(repair_root))
                })
                .await
                .map_err(|_| UpdateError::RollbackFailed)?;
            }
            SelectorState::Indeterminate => {
                let status = market_squawk_installer::status(&root)
                    .map_err(|_| UpdateError::RollbackFailed)?;
                if status.active_version() == Some(candidate.as_ref()) {
                    let rollback_root = root.clone();
                    self.run_installer(move || {
                        market_squawk_installer::rollback(RollbackRequest::new(rollback_root))
                    })
                    .await
                    .map_err(|_| UpdateError::RollbackFailed)?;
                } else if !status.is_healthy() {
                    let repair_root = root.clone();
                    self.run_installer(move || {
                        market_squawk_installer::repair(RepairRequest::new(repair_root))
                    })
                    .await
                    .map_err(|_| UpdateError::RollbackFailed)?;
                }
            }
        }
        let status =
            market_squawk_installer::status(&root).map_err(|_| UpdateError::RollbackFailed)?;
        if status.is_healthy() && status.active_version() != Some(candidate.as_ref()) {
            self.set_selector_state(SelectorState::Unchanged);
            Ok(())
        } else {
            Err(UpdateError::RollbackFailed)
        }
    }
}

#[derive(Debug, Deserialize)]
struct RoutingEnvelope<T> {
    signed: T,
}

#[derive(Debug, Deserialize)]
struct VersionRouting {
    version: u64,
}

#[derive(Debug, Deserialize)]
struct TimestampRouting {
    meta: BTreeMap<Box<str>, MetadataRouting>,
}

#[derive(Debug, Deserialize)]
struct SnapshotRouting {
    meta: BTreeMap<Box<str>, MetadataRouting>,
}

#[derive(Debug, Deserialize)]
struct TargetsRouting {
    targets: BTreeMap<Box<str>, TargetRouting>,
}

#[derive(Debug, Deserialize)]
struct MetadataRouting {
    version: u64,
    length: u64,
    hashes: BTreeMap<Box<str>, Box<str>>,
}

#[derive(Debug, Deserialize)]
struct TargetRouting {
    length: u64,
    hashes: BTreeMap<Box<str>, Box<str>>,
    custom: Option<Value>,
}

#[derive(Debug)]
struct RoutedTarget {
    download_path: String,
    length: u64,
    sha256: [u8; 32],
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CompatibilityRouting {
    schema_version: u32,
    release_version: Box<str>,
    minimum_schema_version: u32,
    maximum_schema_version: u32,
}

async fn next_chunk<S>(
    stream: &mut S,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<Option<Bytes>, ServiceError>
where
    S: futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ServiceError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(ServiceError::DeadlineExceeded)
        }
        chunk = stream.next() => chunk.transpose().map_err(|_| ServiceError::Unavailable),
    }
}

fn validate_response_encoding(response: &reqwest::Response) -> Result<(), ServiceError> {
    if response
        .headers()
        .get(CONTENT_ENCODING)
        .is_some_and(|value| value.as_bytes() != b"identity")
    {
        return Err(ServiceError::Unavailable);
    }
    Ok(())
}

fn signed_metadata<T>(bytes: &[u8]) -> Result<T, ServiceError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_slice::<RoutingEnvelope<T>>(bytes)
        .map(|envelope| envelope.signed)
        .map_err(|_| ServiceError::Unavailable)
}

fn metadata_version(bytes: &[u8]) -> Result<u64, ServiceError> {
    let routing: VersionRouting = signed_metadata(bytes)?;
    if routing.version == 0 {
        return Err(ServiceError::Unavailable);
    }
    Ok(routing.version)
}

fn verify_routing_identity(
    bytes: &[u8],
    description: &MetadataRouting,
) -> Result<(), ServiceError> {
    let length = u64::try_from(bytes.len()).map_err(|_| ServiceError::ResourceExhausted)?;
    if description.version == 0
        || description.length != length
        || exact_sha256(&description.hashes)? != <[u8; 32]>::from(Sha256::digest(bytes))
    {
        return Err(ServiceError::Unavailable);
    }
    Ok(())
}

fn target_identity(
    metadata_path: &str,
    route: &TargetRouting,
    maximum: u64,
) -> Result<RoutedTarget, ServiceError> {
    if route.length == 0 || route.length > maximum || !valid_repository_path(metadata_path) {
        return Err(ServiceError::ResourceExhausted);
    }
    let sha256 = exact_sha256(&route.hashes)?;
    let (parent, name) = metadata_path
        .rsplit_once('/')
        .map_or(("", metadata_path), |(parent, name)| (parent, name));
    let prefixed = format!("{}.{name}", encode_sha256(sha256));
    let download_path = if parent.is_empty() {
        prefixed
    } else {
        format!("{parent}/{prefixed}")
    };
    if !valid_repository_path(&download_path) {
        return Err(ServiceError::Unavailable);
    }
    Ok(RoutedTarget {
        download_path,
        length: route.length,
        sha256,
    })
}

fn candidate_compatibility(route: &TargetRouting) -> Result<CompatibilityRouting, ServiceError> {
    let custom = route.custom.as_ref().ok_or(ServiceError::Unavailable)?;
    let value = custom
        .get("marketSquawk")
        .cloned()
        .ok_or(ServiceError::Unavailable)?;
    let compatibility: CompatibilityRouting =
        serde_json::from_value(value).map_err(|_| ServiceError::Unavailable)?;
    if compatibility.schema_version != 1
        || compatibility.release_version.is_empty()
        || compatibility.release_version.len() > 128
        || compatibility.minimum_schema_version == 0
        || compatibility.minimum_schema_version > compatibility.maximum_schema_version
    {
        return Err(ServiceError::Unavailable);
    }
    Ok(compatibility)
}

fn exact_sha256(hashes: &BTreeMap<Box<str>, Box<str>>) -> Result<[u8; 32], ServiceError> {
    if hashes.len() != 1 {
        return Err(ServiceError::Unavailable);
    }
    hashes
        .get("sha256")
        .ok_or(ServiceError::Unavailable)
        .and_then(|value| decode_sha256(value))
}

fn decode_sha256(value: &str) -> Result<[u8; 32], ServiceError> {
    if value.len() != 64 {
        return Err(ServiceError::Unavailable);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = decode_nibble(pair[0])?
            .checked_mul(16)
            .and_then(|high| high.checked_add(decode_nibble(pair[1]).ok()?))
            .ok_or(ServiceError::Unavailable)?;
    }
    Ok(bytes)
}

fn decode_nibble(value: u8) -> Result<u8, ServiceError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(ServiceError::Unavailable),
    }
}

fn encode_sha256(bytes: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn metadata_chain_digest(
    roots: &[Vec<u8>],
    timestamp: &[u8],
    snapshot: &[u8],
    targets: &[u8],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk-trusted-metadata-chain-v1\0");
    for root in roots {
        digest.update(u64::try_from(root.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(root);
    }
    for metadata in [timestamp, snapshot, targets] {
        digest.update(
            u64::try_from(metadata.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        digest.update(metadata);
    }
    digest.finalize().into()
}

async fn persist_metadata(
    root: &Path,
    roots: &[Vec<u8>],
    timestamp: &[u8],
    snapshot: &[u8],
    targets: &[u8],
) -> Result<(), ServiceError> {
    let root = root.to_path_buf();
    let roots = roots.to_vec();
    let timestamp = timestamp.to_vec();
    let snapshot = snapshot.to_vec();
    let targets = targets.to_vec();
    tokio::task::spawn_blocking(move || {
        for (index, bytes) in roots.iter().enumerate() {
            write_new_synced(&root.join(format!("root-{index}.json")), bytes)?;
        }
        write_new_synced(&root.join("timestamp.json"), &timestamp)?;
        write_new_synced(&root.join("snapshot.json"), &snapshot)?;
        write_new_synced(&root.join("targets.json"), &targets)?;
        sync_directory(&root)
    })
    .await
    .map_err(|_| ServiceError::Unavailable)?
}

#[allow(
    clippy::too_many_arguments,
    reason = "every fetched metadata and target identity is supplied independently"
)]
async fn admit_and_persist_trust(
    install_root: PathBuf,
    pinned_root: TrustedRoot,
    roots: Vec<Vec<u8>>,
    timestamp: Vec<u8>,
    snapshot_path: String,
    snapshot: Vec<u8>,
    targets_path: String,
    targets: Vec<u8>,
    manifest_target_path: Box<str>,
    manifest_download_path: Box<str>,
    manifest_path: PathBuf,
    archive_target_path: Box<str>,
    archive_download_path: Box<str>,
    archive_path: PathBuf,
    trusted_time: DateTime<Utc>,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<TrustedUpdateReceipt, ServiceError> {
    let mut worker = tokio::task::spawn_blocking(move || {
        let root_refs = roots.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let store = TrustedUpdateStore::open_or_bootstrap(&install_root, pinned_root, trusted_time)
            .map_err(|_| ServiceError::Unavailable)?;
        let metadata = SuppliedMetadata {
            root_chain: &root_refs,
            timestamp: &timestamp,
            snapshot_path: &snapshot_path,
            snapshot: &snapshot,
            targets_path: &targets_path,
            targets: &targets,
        };
        let supplied_targets = [
            SuppliedTarget {
                metadata_path: &manifest_target_path,
                download_path: &manifest_download_path,
                source: TargetSource::File(&manifest_path),
            },
            SuppliedTarget {
                metadata_path: &archive_target_path,
                download_path: &archive_download_path,
                source: TargetSource::File(&archive_path),
            },
        ];
        store
            .admit(metadata, &supplied_targets, trusted_time)
            .and_then(PendingTrustedUpdate::persist)
            .map_err(|_| ServiceError::Unavailable)
    });
    let mut interrupted = None;
    let result = tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            interrupted = Some(ServiceError::Cancelled);
            (&mut worker).await
        }
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            interrupted = Some(ServiceError::DeadlineExceeded);
            (&mut worker).await
        }
        result = &mut worker => result,
    }
    .map_err(|_| ServiceError::Unavailable)?;
    if let Some(error) = interrupted {
        return Err(error);
    }
    result
}

fn trusted_root_version(install_root: &Path, pinned: u64) -> Result<u64, ServiceError> {
    let path = install_root.join(TRUST_STATE_FILE);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(pinned),
        Err(_) => return Err(ServiceError::Unavailable),
    };
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > TRUST_STATE_LIMIT
    {
        return Err(ServiceError::Unavailable);
    }
    #[derive(Deserialize)]
    struct RootVersion {
        root_version: u64,
    }
    let bytes = read_bounded_regular(&path, TRUST_STATE_LIMIT)?;
    let state: RootVersion =
        serde_json::from_slice(&bytes).map_err(|_| ServiceError::Unavailable)?;
    if state.root_version == 0 {
        return Err(ServiceError::Unavailable);
    }
    Ok(state.root_version)
}

fn staged_digest(stage: &DurableStagedUpdate) -> Result<[u8; 32], ServiceError> {
    let encoded = serde_json::to_vec(&(
        "market-squawk-durable-staged-update-v1",
        &stage.directory,
        &stage.candidate,
        &stage.version,
        stage.checked_at,
        &stage.metadata,
        &stage.manifest_file,
        &stage.bundle_file,
        stage.bundle_bytes,
    ))
    .map_err(|_| ServiceError::Internal)?;
    Ok(Sha256::digest(encoded).into())
}

fn valid_repository_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 512
        && !path.starts_with('/')
        && !path.ends_with('/')
        && !path.contains("//")
        && path.split('/').all(|component| {
            !component.is_empty()
                && component != "."
                && component != ".."
                && component.len() <= 255
                && component
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
}

fn valid_stage_directory_name(name: &str) -> bool {
    name.len() == "stage-".len() + 32
        && name.starts_with("stage-")
        && name["stage-".len()..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn valid_stage_file_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && !name.contains(['/', '\\'])
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn prepare_private_directory(path: &Path) -> Result<(), ServiceError> {
    fs::create_dir_all(path).map_err(|_| ServiceError::Unavailable)?;
    validate_stage_root(path)?;
    set_private_directory_permissions(path)
}

fn create_private_stage(path: &Path) -> Result<(), ServiceError> {
    fs::create_dir(path).map_err(|_| ServiceError::Unavailable)?;
    set_private_directory_permissions(path)?;
    validate_stage_root(path)
}

fn validate_stage_root(path: &Path) -> Result<(), ServiceError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| ServiceError::Unavailable)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(ServiceError::Unavailable);
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<(), ServiceError> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|_| ServiceError::Unavailable)
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<(), ServiceError> {
    Ok(())
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), ServiceError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| ServiceError::Unavailable)?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| ServiceError::Unavailable)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), ServiceError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| ServiceError::Unavailable)
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), ServiceError> {
    Ok(())
}

fn read_bounded_regular(path: &Path, maximum: u64) -> Result<Vec<u8>, ServiceError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| ServiceError::Unavailable)?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > maximum
    {
        return Err(ServiceError::Unavailable);
    }
    let capacity = usize::try_from(metadata.len()).map_err(|_| ServiceError::ResourceExhausted)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| ServiceError::ResourceExhausted)?;
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|_| ServiceError::Unavailable)?;
    if bytes.len() != capacity {
        return Err(ServiceError::Unavailable);
    }
    Ok(bytes)
}

fn read_verified_file(
    path: &Path,
    expected_length: u64,
    expected_sha256: [u8; 32],
    maximum: u64,
) -> Result<Vec<u8>, ServiceError> {
    let bytes = read_bounded_regular(path, maximum)?;
    if u64::try_from(bytes.len()).map_err(|_| ServiceError::ResourceExhausted)? != expected_length
        || <[u8; 32]>::from(Sha256::digest(&bytes)) != expected_sha256
    {
        return Err(ServiceError::Unavailable);
    }
    Ok(bytes)
}

fn now_timestamp() -> Result<Timestamp, ServiceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?
        .as_nanos();
    i64::try_from(nanos)
        .map(Timestamp::from_unix_nanos)
        .map_err(|_| ServiceError::Unavailable)
}

fn ensure_live(cancellation: &CancellationToken, deadline: Instant) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if deadline <= Instant::now() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn remaining(deadline: Instant) -> Result<Duration, ServiceError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or(ServiceError::DeadlineExceeded)
}

fn execution_failure(diagnostic: &str, retryable: bool) -> LifecycleJobExecutionError {
    match SourceIdentifier::try_from(diagnostic) {
        Ok(identifier) => LifecycleJobExecutionError::failed(identifier, retryable),
        Err(_) => LifecycleJobExecutionError::Publication(LifecycleJobPublicationError::Revoked),
    }
}

fn strictly_newer_semver(candidate: &str, current: &str) -> Result<bool, ServiceError> {
    let candidate = ParsedSemver::parse(candidate)?;
    let current = ParsedSemver::parse(current)?;
    Ok(candidate > current)
}

#[derive(Debug, Eq, PartialEq)]
struct ParsedSemver<'a> {
    core: [u64; 3],
    prerelease: Option<Vec<SemverIdentifier<'a>>>,
}

#[derive(Debug, Eq, PartialEq)]
enum SemverIdentifier<'a> {
    Numeric(u64),
    Text(&'a str),
}

impl<'a> ParsedSemver<'a> {
    fn parse(value: &'a str) -> Result<Self, ServiceError> {
        let without_build = value.split_once('+').map_or(value, |(head, _)| head);
        let (core, prerelease) = without_build
            .split_once('-')
            .map_or((without_build, None), |(core, pre)| (core, Some(pre)));
        let components = core.split('.').collect::<Vec<_>>();
        if components.len() != 3 {
            return Err(ServiceError::Unavailable);
        }
        let mut parsed_core = [0_u64; 3];
        for (slot, component) in parsed_core.iter_mut().zip(components) {
            if component.is_empty()
                || component.len() > 1 && component.starts_with('0')
                || !component.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(ServiceError::Unavailable);
            }
            *slot = component.parse().map_err(|_| ServiceError::Unavailable)?;
        }
        let prerelease = prerelease
            .map(|pre| {
                pre.split('.')
                    .map(|identifier| {
                        if identifier.is_empty()
                            || !identifier
                                .bytes()
                                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                        {
                            return Err(ServiceError::Unavailable);
                        }
                        if identifier.bytes().all(|byte| byte.is_ascii_digit()) {
                            if identifier.len() > 1 && identifier.starts_with('0') {
                                return Err(ServiceError::Unavailable);
                            }
                            identifier
                                .parse()
                                .map(SemverIdentifier::Numeric)
                                .map_err(|_| ServiceError::Unavailable)
                        } else {
                            Ok(SemverIdentifier::Text(identifier))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        Ok(Self {
            core: parsed_core,
            prerelease,
        })
    }
}

impl Ord for ParsedSemver<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.core
            .cmp(&other.core)
            .then_with(|| match (&self.prerelease, &other.prerelease) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some(_), None) => std::cmp::Ordering::Less,
                (Some(left), Some(right)) => left.cmp(right),
            })
    }
}

impl PartialOrd for ParsedSemver<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SemverIdentifier<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Numeric(left), Self::Numeric(right)) => left.cmp(right),
            (Self::Numeric(_), Self::Text(_)) => std::cmp::Ordering::Less,
            (Self::Text(_), Self::Numeric(_)) => std::cmp::Ordering::Greater,
            (Self::Text(left), Self::Text(right)) => left.cmp(right),
        }
    }
}

impl PartialOrd for SemverIdentifier<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
