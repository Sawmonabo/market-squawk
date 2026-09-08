//! Exact owner-issued model and forecast backup snapshots.

mod archive;

use std::{
    collections::BTreeSet,
    io::{Read, Write},
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk_modeling::{OnnxWorkerProgram, VerifiedTrainingEnvironment};
use market_squawk_platform::{ArtifactPathError, LocalPaths, PathError};
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactReadContext,
    ArtifactReadRequest, ArtifactReference, ArtifactRepository,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use self::archive::{
    DecodedArchive, ForecastArtifactManifestRecord, MemberManifestRecord, ModelManifestRecord,
    ModelMemberManifestRecord, SnapshotManifest, read_archive, write_archive,
};
use super::{
    ForecastApplicationError, ForecastApplicationLimits, ForecastApplicationService,
    forecast::ForecastBackupCaptureError,
    runtime::{
        ProductionModelRuntime, ProductionModelRuntimeError, ProductionModelRuntimeLimits,
        RuntimeBackupCoordinate,
    },
};

pub(crate) const MODEL_BACKUP_SCHEMA_VERSION: u16 = 1;
const RUNTIME_INDEX_PATH: &str = "runtime-index.json";
const FORECAST_INDEX_PATH: &str = "forecast-index.json";
const FORECAST_AUTHORITY_DIRECTORY: &str = "model/forecasts";
const SEMANTIC_REVISION_DOMAIN: &[u8] = b"market-squawk/model-backup-authority/v1\0";
const MAXIMUM_ARCHIVE_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MAXIMUM_MEMBER_BYTES: usize = 512 * 1024 * 1024;
const MAXIMUM_MEMBERS: usize = 131_072;
const MAXIMUM_READ_TIME: Duration = Duration::from_secs(5 * 60);

/// Fixed archive, member, count, and immutable-artifact read bounds.
#[derive(Clone, Copy, Debug)]
pub struct ModelBackupLimits {
    maximum_archive_bytes: NonZeroU64,
    maximum_member_bytes: NonZeroUsize,
    maximum_members: NonZeroUsize,
    read_time: Duration,
}

impl ModelBackupLimits {
    /// Constructs bounds no greater than the product component and model-owner ceilings.
    pub fn try_new(
        maximum_archive_bytes: NonZeroU64,
        maximum_member_bytes: NonZeroUsize,
        maximum_members: NonZeroUsize,
        read_time: Duration,
    ) -> Result<Self, ModelBackupError> {
        if maximum_archive_bytes.get() > MAXIMUM_ARCHIVE_BYTES
            || maximum_member_bytes.get() > MAXIMUM_MEMBER_BYTES
            || maximum_members.get() > MAXIMUM_MEMBERS
            || maximum_members.get() < 3
            || read_time.is_zero()
            || read_time > MAXIMUM_READ_TIME
        {
            return Err(ModelBackupError::InvalidLimits);
        }
        Ok(Self {
            maximum_archive_bytes,
            maximum_member_bytes,
            maximum_members,
            read_time,
        })
    }

    /// Returns bounded installed-product defaults.
    pub fn standard() -> Result<Self, ModelBackupError> {
        Self::try_new(
            NonZeroU64::new(2 * 1024 * 1024 * 1024).ok_or(ModelBackupError::InvalidLimits)?,
            NonZeroUsize::new(MAXIMUM_MEMBER_BYTES).ok_or(ModelBackupError::InvalidLimits)?,
            NonZeroUsize::new(32_768).ok_or(ModelBackupError::InvalidLimits)?,
            MAXIMUM_READ_TIME,
        )
    }

    pub(super) const fn maximum_archive_bytes(self) -> u64 {
        self.maximum_archive_bytes.get()
    }

    pub(super) const fn maximum_member_bytes(self) -> NonZeroUsize {
        self.maximum_member_bytes
    }

    pub(super) const fn maximum_members(self) -> NonZeroUsize {
        self.maximum_members
    }
}

/// Joint owner of the admitted model runtime and immutable forecast authority.
pub struct ModelBackupAuthority {
    runtime: Option<Arc<ProductionModelRuntime>>,
    runtime_limits: ProductionModelRuntimeLimits,
    forecasts: Arc<ForecastApplicationService>,
    limits: ModelBackupLimits,
}

impl ModelBackupAuthority {
    /// Binds the two real mutation owners used by normal application composition.
    #[must_use]
    pub fn new(
        runtime: Option<Arc<ProductionModelRuntime>>,
        runtime_limits: ProductionModelRuntimeLimits,
        forecasts: Arc<ForecastApplicationService>,
        limits: ModelBackupLimits,
    ) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            runtime_limits,
            forecasts,
            limits,
        })
    }

    /// Issues the exact installed runtime capabilities for one factory-created fresh workspace.
    pub(crate) fn fresh_workspace_target(
        &self,
        paths: LocalPaths,
        artifacts: Arc<dyn ArtifactRepository>,
    ) -> Result<FreshModelWorkspaceTarget, ModelBackupError> {
        let (runtime_capabilities, runtime_limits) = match &self.runtime {
            Some(runtime) => {
                let (training_environment, onnx_worker, runtime_limits) =
                    runtime.restore_capabilities()?;
                (Some((training_environment, onnx_worker)), runtime_limits)
            }
            None => (None, self.runtime_limits),
        };
        Ok(FreshModelWorkspaceTarget::new(
            paths,
            artifacts,
            runtime_capabilities,
            runtime_limits,
            self.forecasts.backup_limits(),
        ))
    }

    /// Restores and reopens the exact model/forecast archive in one fresh workspace.
    pub(crate) async fn restore_fresh_workspace(
        &self,
        reader: &mut (dyn Read + Send),
        paths: LocalPaths,
        artifacts: Arc<dyn ArtifactRepository>,
        cancellation: &CancellationToken,
    ) -> Result<RestoredModelAuthorities, ModelBackupError> {
        let target = self.fresh_workspace_target(paths, artifacts)?;
        restore_into_fresh_workspace(reader, target, self.limits, cancellation).await
    }

    /// Retains one immutable cross-authority image and verifies every referenced artifact.
    pub async fn retain(
        self: &Arc<Self>,
        cancellation: &CancellationToken,
    ) -> Result<ModelBackupSnapshot, ModelBackupError> {
        if cancellation.is_cancelled() {
            return Err(ModelBackupError::Cancelled);
        }
        let retained = self
            .forecasts
            .retain_backup_with_runtime(self.runtime.as_deref(), self.runtime_limits)
            .await
            .map_err(map_capture_error)?;
        let mut members = Vec::new();
        let mut models = Vec::new();
        admit_member(
            &mut members,
            RUNTIME_INDEX_PATH,
            Arc::from(retained.runtime.canonical_index),
            self.limits,
        )?;
        for (coordinate, bundle) in retained.runtime.models {
            let mut model_members = Vec::new();
            for (role, relative_path, bytes, digest) in bundle.retained_members() {
                if <[u8; 32]>::from(Sha256::digest(bytes)) != digest.bytes() {
                    return Err(ModelBackupError::ArtifactMismatch);
                }
                let archive_path = model_archive_path(&coordinate, relative_path)?;
                admit_member(&mut members, &archive_path, Arc::from(bytes), self.limits)?;
                model_members.push(ModelMemberManifestRecord {
                    role: role.to_owned(),
                    relative_path: relative_path.to_owned(),
                    archive_path,
                    byte_length: u64::try_from(bytes.len())
                        .map_err(|_| ModelBackupError::Capacity)?,
                    sha256: hex(digest.bytes()),
                });
            }
            if model_members
                .iter()
                .find(|member| member.role == "metadata")
                .is_none_or(|member| member.relative_path != coordinate.metadata_path.as_ref())
            {
                return Err(ModelBackupError::CoordinateMismatch);
            }
            models.push(model_manifest(coordinate, model_members));
        }
        admit_member(
            &mut members,
            FORECAST_INDEX_PATH,
            Arc::from(retained.canonical_index),
            self.limits,
        )?;
        let deadline = Instant::now()
            .checked_add(self.limits.read_time)
            .ok_or(ModelBackupError::Capacity)?;
        let mut forecast_artifacts = Vec::new();
        for reference in retained.artifact_references {
            if cancellation.is_cancelled() {
                return Err(ModelBackupError::Cancelled);
            }
            let maximum = NonZeroUsize::new(reference.byte_count())
                .ok_or(ModelBackupError::ArtifactMismatch)?;
            if maximum.get() > self.limits.maximum_member_bytes.get() {
                return Err(ModelBackupError::Capacity);
            }
            let read = self
                .forecasts
                .artifact_repository()
                .read(
                    ArtifactReadRequest::try_new(reference.clone(), maximum)?,
                    ArtifactReadContext::new(cancellation.clone(), deadline),
                )
                .await?;
            if read.reference() != &reference {
                return Err(ModelBackupError::ArtifactMismatch);
            }
            let archive_path = format!("forecast-artifacts/{}.json", reference.sha256());
            admit_member(
                &mut members,
                &archive_path,
                Arc::from(read.content()),
                self.limits,
            )?;
            forecast_artifacts.push(ForecastArtifactManifestRecord {
                artifact_id: reference.id().to_owned(),
                archive_path,
                byte_length: u64::try_from(reference.byte_count())
                    .map_err(|_| ModelBackupError::Capacity)?,
                sha256: reference.sha256().to_owned(),
                media_type: reference.media_type().to_owned(),
            });
        }
        validate_member_order_and_bounds(&members, self.limits)?;
        let revision = semantic_revision(&members)?;
        let manifest = SnapshotManifest {
            schema_version: MODEL_BACKUP_SCHEMA_VERSION,
            semantic_authority_revision: hex(revision),
            runtime_index_path: RUNTIME_INDEX_PATH.to_owned(),
            forecast_index_path: FORECAST_INDEX_PATH.to_owned(),
            models,
            forecast_artifacts,
            members: members
                .iter()
                .map(member_manifest)
                .collect::<Result<Vec<_>, _>>()?,
        };
        Ok(ModelBackupSnapshot {
            authority: Arc::clone(self),
            revision,
            manifest,
            members,
        })
    }
}

impl std::fmt::Debug for ModelBackupAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ModelBackupAuthority([SEALED MODEL AND FORECAST AUTHORITIES])")
    }
}

/// Immutable retained Models component image.
pub struct ModelBackupSnapshot {
    authority: Arc<ModelBackupAuthority>,
    revision: [u8; 32],
    manifest: SnapshotManifest,
    members: Vec<RetainedArchiveMember>,
}

impl ModelBackupSnapshot {
    /// Returns the one semantic revision spanning runtime, bundles, forecast index, and artifacts.
    #[must_use]
    pub const fn semantic_authority_revision(&self) -> [u8; 32] {
        self.revision
    }

    /// Streams the deterministic strict archive without constructing a second archive image.
    pub fn write_to(
        &self,
        writer: &mut (dyn Write + Send),
        cancellation: &CancellationToken,
    ) -> Result<ModelBackupReceipt, ModelBackupError> {
        if cancellation.is_cancelled() {
            return Err(ModelBackupError::Cancelled);
        }
        let (byte_length, sha256) = write_archive(
            writer,
            &self.manifest,
            &self.members,
            self.authority.limits,
            cancellation,
        )?;
        Ok(ModelBackupReceipt {
            semantic_authority_revision: self.revision,
            byte_length,
            sha256,
        })
    }

    /// Reacquires both owners and rejects any semantic mutation since retention.
    pub async fn revalidate(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), ModelBackupError> {
        let current = self.authority.retain(cancellation).await?;
        if current.revision != self.revision {
            return Err(ModelBackupError::AuthorityChanged);
        }
        Ok(())
    }
}

impl std::fmt::Debug for ModelBackupSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelBackupSnapshot")
            .field("semantic_authority_revision", &hex(self.revision))
            .field("member_count", &self.members.len())
            .finish()
    }
}

/// Exact streamed Models component receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelBackupReceipt {
    semantic_authority_revision: [u8; 32],
    byte_length: u64,
    sha256: [u8; 32],
}

impl ModelBackupReceipt {
    #[must_use]
    pub const fn semantic_authority_revision(self) -> [u8; 32] {
        self.semantic_authority_revision
    }

    #[must_use]
    pub const fn byte_length(self) -> u64 {
        self.byte_length
    }

    #[must_use]
    pub const fn sha256(self) -> [u8; 32] {
        self.sha256
    }
}

/// Capabilities needed to restore Models into one factory-created inactive workspace.
pub(crate) struct FreshModelWorkspaceTarget {
    paths: LocalPaths,
    artifacts: Arc<dyn ArtifactRepository>,
    runtime_capabilities: Option<(VerifiedTrainingEnvironment, Option<OnnxWorkerProgram>)>,
    runtime_limits: ProductionModelRuntimeLimits,
    forecast_limits: ForecastApplicationLimits,
}

impl FreshModelWorkspaceTarget {
    pub(crate) fn new(
        paths: LocalPaths,
        artifacts: Arc<dyn ArtifactRepository>,
        runtime_capabilities: Option<(VerifiedTrainingEnvironment, Option<OnnxWorkerProgram>)>,
        runtime_limits: ProductionModelRuntimeLimits,
        forecast_limits: ForecastApplicationLimits,
    ) -> Self {
        Self {
            paths,
            artifacts,
            runtime_capabilities,
            runtime_limits,
            forecast_limits,
        }
    }
}

impl std::fmt::Debug for FreshModelWorkspaceTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FreshModelWorkspaceTarget")
            .field("paths", &self.paths)
            .field("artifacts", &"[CONTROLLED ARTIFACT REPOSITORY]")
            .field(
                "runtime_capabilities",
                &self.runtime_capabilities.as_ref().map(|_| "[VERIFIED]"),
            )
            .field("runtime_limits", &self.runtime_limits)
            .field("forecast_limits", &self.forecast_limits)
            .finish()
    }
}

/// Reopened production model and forecast authorities for a fresh workspace.
pub(crate) struct RestoredModelAuthorities {
    _runtime: Option<Arc<ProductionModelRuntime>>,
    _forecasts: Arc<ForecastApplicationService>,
}

impl std::fmt::Debug for RestoredModelAuthorities {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RestoredModelAuthorities([REOPENED MODEL AUTHORITIES])")
    }
}

/// Validates and restores one strict Models archive through normal production constructors.
pub(crate) async fn restore_into_fresh_workspace(
    reader: &mut (dyn Read + Send),
    target: FreshModelWorkspaceTarget,
    limits: ModelBackupLimits,
    cancellation: &CancellationToken,
) -> Result<RestoredModelAuthorities, ModelBackupError> {
    if cancellation.is_cancelled() {
        return Err(ModelBackupError::Cancelled);
    }
    let archive = match read_archive(reader, limits, cancellation) {
        Err(ModelBackupError::Io(error))
            if cancellation.is_cancelled() && error.kind() == std::io::ErrorKind::Interrupted =>
        {
            return Err(ModelBackupError::Cancelled);
        }
        result => result?,
    };
    validate_decoded_archive(&archive, target.runtime_limits, target.forecast_limits)?;
    let runtime_index = archive_member(&archive, RUNTIME_INDEX_PATH)?;
    let coordinates =
        ProductionModelRuntime::backup_coordinates(runtime_index, target.runtime_limits)?;
    let artifact_root = target.paths.artifacts()?;
    for model in &archive.manifest.models {
        let coordinate = coordinate_for_manifest(&coordinates, model)?;
        for member in &model.members {
            let bytes = archive_member(&archive, &member.archive_path)?;
            let relative = format!(
                "{}/{}",
                coordinate.candidate_directory, member.relative_path
            );
            let resolved = artifact_root.resolve(relative)?;
            let mut file = resolved.create_new()?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
    }
    ProductionModelRuntime::stage_backup_index(
        &target.paths,
        runtime_index,
        target.runtime_limits,
    )?;
    let runtime = match (coordinates.is_empty(), target.runtime_capabilities) {
        (true, None) => None,
        (_, Some((training_environment, onnx_worker))) => {
            Some(Arc::new(ProductionModelRuntime::try_open(
                &target.paths,
                training_environment,
                onnx_worker,
                target.runtime_limits,
            )?))
        }
        _ => {
            return Err(ModelBackupError::Runtime(
                ProductionModelRuntimeError::RuntimeUnavailable,
            ));
        }
    };

    let deadline = Instant::now()
        .checked_add(limits.read_time)
        .ok_or(ModelBackupError::Capacity)?;
    let mut restored_references = Vec::new();
    for artifact in &archive.manifest.forecast_artifacts {
        let bytes = archive_member(&archive, &artifact.archive_path)?;
        let publication = match artifact.media_type.as_str() {
            "application/json" => ArtifactPublication::try_json(bytes.to_vec())?,
            _ => return Err(ModelBackupError::ArtifactMismatch),
        };
        let expected = ArtifactReference::try_new(
            artifact.artifact_id.clone(),
            artifact.sha256.clone(),
            usize::try_from(artifact.byte_length).map_err(|_| ModelBackupError::Capacity)?,
            artifact.media_type.clone(),
        )?;
        if !expected.matches(&publication) {
            return Err(ModelBackupError::ArtifactMismatch);
        }
        let restored = target
            .artifacts
            .publish(
                publication,
                ArtifactPublicationContext::new(cancellation.clone(), deadline),
            )
            .await?;
        if restored != expected {
            return Err(ModelBackupError::ArtifactMismatch);
        }
        let verified = target
            .artifacts
            .read(
                ArtifactReadRequest::try_new(
                    restored.clone(),
                    NonZeroUsize::new(restored.byte_count())
                        .ok_or(ModelBackupError::ArtifactMismatch)?,
                )?,
                ArtifactReadContext::new(cancellation.clone(), deadline),
            )
            .await?;
        if verified.reference() != &expected || verified.content() != bytes {
            return Err(ModelBackupError::ArtifactMismatch);
        }
        restored_references.push(restored);
    }
    let forecast_root = target
        .paths
        .control_root()?
        .root()
        .join(FORECAST_AUTHORITY_DIRECTORY);
    let forecast_index = archive_member(&archive, FORECAST_INDEX_PATH)?;
    ForecastApplicationService::stage_backup_index(
        &forecast_root,
        forecast_index,
        &restored_references,
        target.forecast_limits,
    )?;
    let forecasts = Arc::new(ForecastApplicationService::try_open(
        forecast_root,
        Arc::clone(&target.artifacts),
        target.forecast_limits,
    )?);
    Ok(RestoredModelAuthorities {
        _runtime: runtime,
        _forecasts: forecasts,
    })
}

#[derive(Clone)]
struct RetainedArchiveMember {
    path: String,
    bytes: Arc<[u8]>,
}

impl std::fmt::Debug for RetainedArchiveMember {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetainedArchiveMember")
            .field("path", &self.path)
            .field("byte_length", &self.bytes.len())
            .field("bytes", &"[RETAINED AUTHORITY BYTES]")
            .finish()
    }
}

fn member_manifest(
    member: &RetainedArchiveMember,
) -> Result<MemberManifestRecord, ModelBackupError> {
    Ok(MemberManifestRecord {
        path: member.path.clone(),
        byte_length: u64::try_from(member.bytes.len()).map_err(|_| ModelBackupError::Capacity)?,
        sha256: hex(Sha256::digest(&member.bytes).into()),
    })
}

fn admit_member(
    members: &mut Vec<RetainedArchiveMember>,
    path: &str,
    bytes: Arc<[u8]>,
    limits: ModelBackupLimits,
) -> Result<(), ModelBackupError> {
    if bytes.is_empty()
        || bytes.len() > limits.maximum_member_bytes.get()
        || members.len() >= limits.maximum_members.get().saturating_sub(1)
        || members.iter().any(|member| member.path == path)
    {
        return Err(ModelBackupError::Capacity);
    }
    members.push(RetainedArchiveMember {
        path: path.to_owned(),
        bytes,
    });
    Ok(())
}

fn validate_member_order_and_bounds(
    members: &[RetainedArchiveMember],
    limits: ModelBackupLimits,
) -> Result<(), ModelBackupError> {
    let unique = members
        .iter()
        .map(|member| member.path.as_str())
        .collect::<BTreeSet<_>>();
    let payload_bytes = members.iter().try_fold(0_u64, |total, member| {
        u64::try_from(member.bytes.len())
            .ok()
            .and_then(|length| total.checked_add(length))
    });
    if unique.len() != members.len()
        || payload_bytes.is_none_or(|bytes| bytes >= limits.maximum_archive_bytes.get())
    {
        return Err(ModelBackupError::Capacity);
    }
    Ok(())
}

fn semantic_revision(members: &[RetainedArchiveMember]) -> Result<[u8; 32], ModelBackupError> {
    let mut digest = Sha256::new();
    digest.update(SEMANTIC_REVISION_DOMAIN);
    for member in members {
        digest.update(
            u64::try_from(member.path.len())
                .map_err(|_| ModelBackupError::Capacity)?
                .to_be_bytes(),
        );
        digest.update(member.path.as_bytes());
        digest.update(
            u64::try_from(member.bytes.len())
                .map_err(|_| ModelBackupError::Capacity)?
                .to_be_bytes(),
        );
        digest.update(Sha256::digest(&member.bytes));
    }
    Ok(digest.finalize().into())
}

fn model_archive_path(
    coordinate: &RuntimeBackupCoordinate,
    relative_path: &str,
) -> Result<String, ModelBackupError> {
    let path = format!(
        "models/{}/{}/{}/{}",
        coordinate.model_id,
        coordinate.bundle_id.as_str(),
        coordinate.bundle_version,
        relative_path
    );
    if path.len() > 1_024 {
        return Err(ModelBackupError::Capacity);
    }
    Ok(path)
}

fn model_manifest(
    coordinate: RuntimeBackupCoordinate,
    members: Vec<ModelMemberManifestRecord>,
) -> ModelManifestRecord {
    ModelManifestRecord {
        model_id: coordinate.model_id.to_string(),
        bundle_id: coordinate.bundle_id.as_str().to_owned(),
        bundle_version: coordinate.bundle_version.get(),
        candidate_directory: coordinate.candidate_directory.into(),
        metadata_path: coordinate.metadata_path.into(),
        members,
    }
}

fn validate_decoded_archive(
    archive: &DecodedArchive,
    runtime_limits: ProductionModelRuntimeLimits,
    forecast_limits: ForecastApplicationLimits,
) -> Result<(), ModelBackupError> {
    if archive.manifest.schema_version != MODEL_BACKUP_SCHEMA_VERSION
        || archive.manifest.runtime_index_path != RUNTIME_INDEX_PATH
        || archive.manifest.forecast_index_path != FORECAST_INDEX_PATH
        || archive.manifest.semantic_authority_revision
            != hex(semantic_revision_from_archive(archive)?)
    {
        return Err(ModelBackupError::Archive);
    }
    let coordinates = ProductionModelRuntime::backup_coordinates(
        archive_member(archive, RUNTIME_INDEX_PATH)?,
        runtime_limits,
    )?;
    if coordinates.len() != archive.manifest.models.len() {
        return Err(ModelBackupError::CoordinateMismatch);
    }
    let mut mapped_paths = vec![RUNTIME_INDEX_PATH];
    for (coordinate, model) in coordinates.iter().zip(&archive.manifest.models) {
        if coordinate_for_manifest(std::slice::from_ref(coordinate), model).is_err()
            || !valid_model_members(model)
        {
            return Err(ModelBackupError::CoordinateMismatch);
        }
        for member in &model.members {
            if member.archive_path != model_archive_path(coordinate, &member.relative_path)?
                || !manifest_member_matches(
                    &archive.manifest.members,
                    &member.archive_path,
                    member.byte_length,
                    &member.sha256,
                )
            {
                return Err(ModelBackupError::Archive);
            }
            mapped_paths.push(&member.archive_path);
        }
    }
    mapped_paths.push(FORECAST_INDEX_PATH);
    let forecast_index = super::forecast::persistence::ForecastIndex::decode_canonical(
        archive_member(archive, FORECAST_INDEX_PATH)?,
        forecast_limits,
    )?;
    if forecast_index
        .model_coordinates()
        .any(|(model_id, bundle_id, version)| {
            !archive.manifest.models.iter().any(|model| {
                model.model_id == model_id
                    && model.bundle_id == bundle_id
                    && model.bundle_version == version
            })
        })
    {
        return Err(ModelBackupError::CoordinateMismatch);
    }
    let expected = forecast_index.artifact_references()?;
    let declared = archive
        .manifest
        .forecast_artifacts
        .iter()
        .map(|artifact| {
            ArtifactReference::try_new(
                artifact.artifact_id.clone(),
                artifact.sha256.clone(),
                usize::try_from(artifact.byte_length)
                    .map_err(|_| ArtifactError::InvalidReference)?,
                artifact.media_type.clone(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    if expected != declared {
        return Err(ModelBackupError::ArtifactMismatch);
    }
    for artifact in &archive.manifest.forecast_artifacts {
        if artifact.archive_path != format!("forecast-artifacts/{}.json", artifact.sha256)
            || artifact.media_type != "application/json"
            || !manifest_member_matches(
                &archive.manifest.members,
                &artifact.archive_path,
                artifact.byte_length,
                &artifact.sha256,
            )
        {
            return Err(ModelBackupError::ArtifactMismatch);
        }
        mapped_paths.push(&artifact.archive_path);
    }
    if !mapped_paths.into_iter().eq(archive
        .manifest
        .members
        .iter()
        .map(|member| member.path.as_str()))
    {
        return Err(ModelBackupError::Archive);
    }
    Ok(())
}

fn manifest_member_matches(
    members: &[MemberManifestRecord],
    path: &str,
    byte_length: u64,
    sha256: &str,
) -> bool {
    members.iter().any(|member| {
        member.path == path && member.byte_length == byte_length && member.sha256 == sha256
    })
}

fn valid_model_members(model: &ModelManifestRecord) -> bool {
    let roles = model
        .members
        .iter()
        .map(|member| member.role.as_str())
        .collect::<Vec<_>>();
    matches!(
        roles.as_slice(),
        ["metadata", "artifact", "training_run"]
            | [
                "metadata",
                "artifact",
                "training_run",
                "forecast_residuals",
                "forecast_policy"
            ]
    ) && model
        .members
        .iter()
        .find(|member| member.role == "metadata")
        .is_some_and(|member| member.relative_path == model.metadata_path)
}

fn coordinate_for_manifest<'coordinate>(
    coordinates: &'coordinate [RuntimeBackupCoordinate],
    model: &ModelManifestRecord,
) -> Result<&'coordinate RuntimeBackupCoordinate, ModelBackupError> {
    coordinates
        .iter()
        .find(|coordinate| {
            coordinate.model_id.to_string() == model.model_id
                && coordinate.bundle_id.as_str() == model.bundle_id
                && coordinate.bundle_version.get() == model.bundle_version
                && coordinate.candidate_directory.as_ref() == model.candidate_directory
                && coordinate.metadata_path.as_ref() == model.metadata_path
        })
        .ok_or(ModelBackupError::CoordinateMismatch)
}

fn archive_member<'archive>(
    archive: &'archive DecodedArchive,
    path: &str,
) -> Result<&'archive [u8], ModelBackupError> {
    archive
        .members
        .get(path)
        .map(AsRef::as_ref)
        .ok_or(ModelBackupError::Archive)
}

fn semantic_revision_from_archive(archive: &DecodedArchive) -> Result<[u8; 32], ModelBackupError> {
    let retained = archive
        .manifest
        .members
        .iter()
        .map(|member| {
            Ok(RetainedArchiveMember {
                path: member.path.clone(),
                bytes: Arc::from(archive_member(archive, &member.path)?),
            })
        })
        .collect::<Result<Vec<_>, ModelBackupError>>()?;
    semantic_revision(&retained)
}

fn map_capture_error(error: ForecastBackupCaptureError) -> ModelBackupError {
    match error {
        ForecastBackupCaptureError::Forecast(error) => ModelBackupError::Forecast(error),
        ForecastBackupCaptureError::Runtime(error) => ModelBackupError::Runtime(error),
        ForecastBackupCaptureError::ModelCoordinateMismatch => ModelBackupError::CoordinateMismatch,
    }
}

pub(super) fn hex(bytes: [u8; 32]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(ALPHABET[usize::from(byte >> 4)]));
        value.push(char::from(ALPHABET[usize::from(byte & 0x0f)]));
    }
    value
}

/// Models snapshot, archive, artifact, or fresh-workspace restore failure.
#[derive(Debug, Error)]
pub enum ModelBackupError {
    #[error("model backup limits are invalid")]
    InvalidLimits,
    #[error("model backup retained capacity was exceeded")]
    Capacity,
    #[error("model backup archive is malformed or noncanonical")]
    Archive,
    #[error("model backup runtime, bundle, or forecast coordinates disagree")]
    CoordinateMismatch,
    #[error("model backup forecast artifact evidence disagrees")]
    ArtifactMismatch,
    #[error("model backup authority changed after retention")]
    AuthorityChanged,
    #[error("model backup operation was cancelled")]
    Cancelled,
    #[error(transparent)]
    Runtime(#[from] ProductionModelRuntimeError),
    #[error(transparent)]
    Forecast(#[from] ForecastApplicationError),
    #[error(transparent)]
    Artifact(#[from] ArtifactError),
    #[error(transparent)]
    Path(#[from] PathError),
    #[error(transparent)]
    ArtifactPath(#[from] ArtifactPathError),
    #[error("model backup local I/O failed")]
    Io(#[from] std::io::Error),
}
