//! Exact owner-issued model and forecast backup snapshots.

mod archive;

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use market_squawk_data::{
    AnalyticalReadCapability, ChartProjectionCatalogCapability, ForecastInventoryCatalogCapability,
    ForecastInventoryHead, ModelInventoryCatalogCapability, ModelInventoryRecord,
};
use market_squawk_modeling::{OnnxWorkerProgram, VerifiedTrainingEnvironment};
use market_squawk_platform::{ArtifactPathError, LocalPaths, PathError};
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactReadContext,
    ArtifactReadRequest, ArtifactRepository,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use self::archive::{ArchiveReader, ArchiveWriter, ModelManifestRecord, ModelMemberManifestRecord};
use super::{
    ForecastApplicationError, ForecastApplicationService, ModelDomainService,
    ModelDomainServiceError,
    forecast::{ForecastBackupCaptureError, ForecastBackupRecord},
    runtime::{
        ProductionModelRuntime, ProductionModelRuntimeError, ProductionModelRuntimeLimits,
        RuntimeBackupCoordinate,
    },
};

pub(crate) const MODEL_BACKUP_SCHEMA_VERSION: u16 = 1;
const RUNTIME_INDEX_PATH: &str = "runtime-index.json";
const FORECAST_INDEX_PATH: &str = "forecast-index.json";
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
        analytical: AnalyticalReadCapability,
        catalog: ModelInventoryCatalogCapability,
        forecast_catalog: ForecastInventoryCatalogCapability,
        charts: ChartProjectionCatalogCapability,
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
            analytical,
            catalog,
            forecast_catalog,
            charts,
            runtime_capabilities,
            runtime_limits,
        ))
    }

    /// Restores and reopens the exact model/forecast archive in one fresh workspace.
    pub(crate) async fn restore_fresh_workspace(
        &self,
        reader: &mut (dyn Read + Send),
        paths: LocalPaths,
        artifacts: Arc<dyn ArtifactRepository>,
        analytical: AnalyticalReadCapability,
        catalog: ModelInventoryCatalogCapability,
        forecast_catalog: ForecastInventoryCatalogCapability,
        charts: ChartProjectionCatalogCapability,
        evaluation_records: NonZeroUsize,
        cancellation: &CancellationToken,
    ) -> Result<RestoredModelAuthorities, ModelBackupError> {
        let target = self.fresh_workspace_target(
            paths,
            artifacts,
            analytical,
            catalog,
            forecast_catalog,
            charts,
        )?;
        restore_into_fresh_workspace(
            reader,
            target,
            self.limits,
            evaluation_records,
            cancellation,
        )
        .await
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
            .retain_backup_with_runtime(self.runtime.as_deref())
            .await
            .map_err(map_capture_error)?;
        // The anonymous file is owned by the snapshot and removed on drop, including failure.
        // No model corpus or second archive image is retained in RAM.
        let mut file = tempfile::tempfile()?;
        let mut archive = ArchiveWriter::new(&mut file, self.limits, cancellation)?;
        archive.member(RUNTIME_INDEX_PATH, &retained.runtime.canonical_index)?;
        let head = ProductionModelRuntime::decode_backup_head(&retained.runtime.canonical_index)?;
        let mut after = 0_u64;
        loop {
            let page = retained.runtime.page(after)?;
            if page.is_empty() {
                break;
            }
            for entry in page {
                ensure_live(cancellation)?;
                if after.checked_add(1) != Some(entry.sequence) || entry.sequence > head.sequence {
                    return Err(ModelBackupError::CoordinateMismatch);
                }
                let bundle = retained.runtime.bundle(&entry.coordinate)?;
                let members = bundle.retained_members().collect::<Vec<_>>();
                let mut model_members = Vec::new();
                for (role, relative_path, bytes, digest) in &members {
                    if <[u8; 32]>::from(Sha256::digest(bytes)) != digest.bytes() {
                        return Err(ModelBackupError::ArtifactMismatch);
                    }
                    model_members.push(ModelMemberManifestRecord {
                        role: (*role).to_owned(),
                        relative_path: (*relative_path).to_owned(),
                        archive_path: model_archive_path(&entry.coordinate, relative_path)?,
                        byte_length: u64::try_from(bytes.len())
                            .map_err(|_| ModelBackupError::Capacity)?,
                        sha256: hex(digest.bytes()),
                    });
                }
                let model = model_manifest(entry.coordinate, model_members);
                if !valid_model_members(&model) {
                    return Err(ModelBackupError::CoordinateMismatch);
                }
                archive.member(
                    &admission_archive_path(entry.sequence),
                    &encode(&entry.record)?,
                )?;
                archive.member(&manifest_archive_path(entry.sequence), &encode(&model)?)?;
                for (expected, (_, _, bytes, _)) in model.members.iter().zip(members) {
                    archive.member(&expected.archive_path, bytes)?;
                }
                after = entry.sequence;
            }
        }
        if after != head.sequence {
            return Err(ModelBackupError::CoordinateMismatch);
        }
        archive.member(FORECAST_INDEX_PATH, &retained.canonical_index)?;
        let forecast_head: ForecastInventoryHead = decode(&retained.canonical_index)?;
        let deadline = Instant::now()
            .checked_add(self.limits.read_time)
            .ok_or(ModelBackupError::Capacity)?;
        for (kind, count) in [(1, forecast_head.vintages), (2, forecast_head.outcomes)] {
            let mut after = 0_u64;
            loop {
                ensure_live(cancellation)?;
                let page = retained.page(kind, after)?;
                if page.is_empty() {
                    break;
                }
                for row in page {
                    ensure_live(cancellation)?;
                    if row.kind != kind
                        || after.checked_add(1) != Some(row.sequence)
                        || row.sequence > count
                    {
                        return Err(ModelBackupError::CoordinateMismatch);
                    }
                    archive.member(&forecast_record_path(kind, row.sequence), &row.record)?;
                    let reference = &row.artifact;
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
                    if read.reference() != reference
                        || read.content().len() != reference.byte_count()
                        || hex(Sha256::digest(read.content()).into()) != reference.sha256()
                    {
                        return Err(ModelBackupError::ArtifactMismatch);
                    }
                    archive.member(&forecast_artifact_path(kind, row.sequence), read.content())?;
                    after = row.sequence;
                }
            }
            if after != count {
                return Err(ModelBackupError::CoordinateMismatch);
            }
        }
        let (revision, byte_length, sha256) = archive.finish()?;
        file.sync_all()?;
        Ok(ModelBackupSnapshot {
            authority: Arc::clone(self),
            revision,
            file: Mutex::new(file),
            byte_length,
            sha256,
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
    file: Mutex<File>,
    byte_length: u64,
    sha256: [u8; 32],
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
        let mut file = self.file.lock().map_err(|_| ModelBackupError::Archive)?;
        file.seek(SeekFrom::Start(0))?;
        let mut buffer = [0_u8; 64 * 1024];
        let mut digest = Sha256::new();
        let mut byte_length = 0_u64;
        loop {
            ensure_live(cancellation)?;
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            byte_length = byte_length
                .checked_add(u64::try_from(read).map_err(|_| ModelBackupError::Capacity)?)
                .ok_or(ModelBackupError::Capacity)?;
            if byte_length > self.byte_length {
                return Err(ModelBackupError::Archive);
            }
            writer.write_all(&buffer[..read])?;
            digest.update(&buffer[..read]);
        }
        let sha256: [u8; 32] = digest.finalize().into();
        if byte_length != self.byte_length || sha256 != self.sha256 {
            return Err(ModelBackupError::Archive);
        }
        writer.flush()?;
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
            .field("byte_length", &self.byte_length)
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
    analytical: AnalyticalReadCapability,
    catalog: ModelInventoryCatalogCapability,
    forecast_catalog: ForecastInventoryCatalogCapability,
    charts: ChartProjectionCatalogCapability,
    runtime_capabilities: Option<(VerifiedTrainingEnvironment, Option<OnnxWorkerProgram>)>,
    runtime_limits: ProductionModelRuntimeLimits,
}

impl FreshModelWorkspaceTarget {
    pub(crate) fn new(
        paths: LocalPaths,
        artifacts: Arc<dyn ArtifactRepository>,
        analytical: AnalyticalReadCapability,
        catalog: ModelInventoryCatalogCapability,
        forecast_catalog: ForecastInventoryCatalogCapability,
        charts: ChartProjectionCatalogCapability,
        runtime_capabilities: Option<(VerifiedTrainingEnvironment, Option<OnnxWorkerProgram>)>,
        runtime_limits: ProductionModelRuntimeLimits,
    ) -> Self {
        Self {
            paths,
            artifacts,
            analytical,
            catalog,
            forecast_catalog,
            charts,
            runtime_capabilities,
            runtime_limits,
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
            .finish()
    }
}

/// Reopened production model and forecast authorities for a fresh workspace.
pub(crate) struct RestoredModelAuthorities {
    _runtime: Option<Arc<ProductionModelRuntime>>,
    _forecasts: Arc<ForecastApplicationService>,
    model_domain: Arc<ModelDomainService>,
}

impl RestoredModelAuthorities {
    /// Binds saved cohort reconstruction to existing restored captures without provider runtime.
    pub(crate) fn bind_retained_forecast_calendar(
        &mut self,
        calendar: crate::application::market_calendar::RetainedMarketSessionReadCapability,
    ) -> Result<(), ModelBackupError> {
        let model =
            Arc::get_mut(&mut self.model_domain).ok_or(ModelBackupError::ArtifactMismatch)?;
        model.forecast_calendar = Some(
            crate::application::market_calendar::ForecastSessionReadCapability::Retained(calendar),
        );
        Ok(())
    }

    pub(crate) fn model_domain(&self) -> Arc<ModelDomainService> {
        Arc::clone(&self.model_domain)
    }
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
    evaluation_records: NonZeroUsize,
    cancellation: &CancellationToken,
) -> Result<RestoredModelAuthorities, ModelBackupError> {
    if cancellation.is_cancelled() {
        return Err(ModelBackupError::Cancelled);
    }
    let mut archive = ArchiveReader::new(reader, limits, cancellation)?;
    let runtime_index = archive.member_bounded(RUNTIME_INDEX_PATH, 1_024)?;
    let head = ProductionModelRuntime::decode_backup_head(&runtime_index)?;
    // The catalog component has already restored the exact immutable inventory. Models may
    // supply files only for those admissions; the archive cannot mint or replace catalog rows.
    if target
        .catalog
        .head()
        .map_err(ProductionModelRuntimeError::from)?
        != head
    {
        return Err(ModelBackupError::CoordinateMismatch);
    }
    target
        .catalog
        .verify(head)
        .map_err(ProductionModelRuntimeError::from)?;
    let artifact_root = target.paths.artifacts()?;
    for sequence in 1..=head.sequence {
        ensure_live(cancellation)?;
        let record: ModelInventoryRecord =
            decode(&archive.member_bounded(&admission_archive_path(sequence), 8 * 1024 * 1024)?)?;
        let coordinate = ProductionModelRuntime::validate_backup_record(&record)?;
        let existing = target
            .catalog
            .get(head, &record.bundle_id, record.bundle_version)
            .map_err(ProductionModelRuntimeError::from)?
            .ok_or(ModelBackupError::CoordinateMismatch)?;
        if existing.head.sequence != sequence || existing.admission != record {
            return Err(ModelBackupError::CoordinateMismatch);
        }
        let model: ModelManifestRecord =
            decode(&archive.member_bounded(&manifest_archive_path(sequence), 16 * 1024)?)?;
        coordinate_for_manifest(std::slice::from_ref(&coordinate), &model)?;
        if !valid_model_members(&model) {
            return Err(ModelBackupError::CoordinateMismatch);
        }
        for member in &model.members {
            if member.archive_path != model_archive_path(&coordinate, &member.relative_path)? {
                return Err(ModelBackupError::Archive);
            }
            let bytes = archive.member(&member.archive_path)?;
            if u64::try_from(bytes.len()) != Ok(member.byte_length)
                || hex(Sha256::digest(&bytes).into()) != member.sha256
            {
                return Err(ModelBackupError::ArtifactMismatch);
            }
            let resolved = artifact_root.resolve(format!(
                "{}/{}",
                coordinate.candidate_directory, member.relative_path
            ))?;
            let mut file = resolved.create_new()?;
            for chunk in bytes.chunks(64 * 1024) {
                ensure_live(cancellation)?;
                file.write_all(chunk)?;
            }
            file.sync_all()?;
        }
    }
    let forecast_bytes = archive.member_bounded(FORECAST_INDEX_PATH, 1_024)?;
    let forecast_head: ForecastInventoryHead = decode(&forecast_bytes)?;
    ForecastApplicationService::validate_backup_head(&target.forecast_catalog, &forecast_bytes)?;
    let forecasts = Arc::new(ForecastApplicationService::try_open(
        target.forecast_catalog.clone(),
        target.charts.clone(),
        Arc::clone(&target.artifacts),
    )?);
    let deadline = Instant::now()
        .checked_add(limits.read_time)
        .ok_or(ModelBackupError::Capacity)?;
    let read_context = ArtifactReadContext::new(cancellation.clone(), deadline);
    for (kind, count) in [(1, forecast_head.vintages), (2, forecast_head.outcomes)] {
        for sequence in 1..=count {
            ensure_live(cancellation)?;
            let maximum_record_bytes = if kind == 1 {
                4 * 1024 * 1024
            } else {
                64 * 1024
            };
            let bytes = archive
                .member_bounded(&forecast_record_path(kind, sequence), maximum_record_bytes)?;
            let existing = if kind == 1 {
                target
                    .forecast_catalog
                    .vintages(forecast_head, sequence - 1, 1, false, None)
            } else {
                target
                    .forecast_catalog
                    .outcomes(forecast_head, sequence - 1, 1, None)
            }
            .map_err(ForecastApplicationError::from)?;
            if existing.len() != 1
                || existing.first().is_none_or(|(position, record)| {
                    *position != sequence || record.as_ref() != bytes.as_ref()
                })
            {
                return Err(ModelBackupError::CoordinateMismatch);
            }
            drop(existing);
            if kind == 1 {
                let stored = super::forecast::persistence::StoredVintageRecord::decode(&bytes)?;
                let (model_id, bundle_id, version) = stored.model_coordinate();
                let version =
                    NonZeroU64::new(version).ok_or(ModelBackupError::CoordinateMismatch)?;
                let model = target
                    .catalog
                    .get(head, bundle_id, version)
                    .map_err(ProductionModelRuntimeError::from)?
                    .ok_or(ModelBackupError::CoordinateMismatch)?;
                if model.admission.model_id.to_string() != model_id {
                    return Err(ModelBackupError::CoordinateMismatch);
                }
            }
            let row = ForecastBackupRecord::decode(kind, sequence, bytes)?;
            let expected = &row.artifact;
            let bytes = archive.member_bounded(
                &forecast_artifact_path(kind, sequence),
                expected.byte_count(),
            )?;
            if expected.media_type() != "application/json" {
                return Err(ModelBackupError::ArtifactMismatch);
            }
            let publication = ArtifactPublication::try_json(bytes.into_vec())?;
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
            if &restored != expected {
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
                    read_context.clone(),
                )
                .await?;
            if verified.reference() != expected
                || verified.content().len() != expected.byte_count()
                || hex(Sha256::digest(verified.content()).into()) != expected.sha256()
            {
                return Err(ModelBackupError::ArtifactMismatch);
            }
            drop(verified);
            forecasts.stage_backup_record(&row, &read_context).await?;
        }
    }
    archive.finish()?;
    ensure_live(cancellation)?;
    ForecastApplicationService::validate_backup_head(&target.forecast_catalog, &forecast_bytes)?;
    if target
        .catalog
        .head()
        .map_err(ProductionModelRuntimeError::from)?
        != head
    {
        return Err(ModelBackupError::CoordinateMismatch);
    }
    target
        .catalog
        .verify(head)
        .map_err(ProductionModelRuntimeError::from)?;
    let runtime = match (head.sequence == 0, target.runtime_capabilities) {
        (true, None) => None,
        (_, Some((training_environment, onnx_worker))) => {
            let runtime = Arc::new(ProductionModelRuntime::try_open(
                &target.paths,
                target.catalog.clone(),
                training_environment,
                onnx_worker,
                target.runtime_limits,
            )?);
            // Reopen each saved bundle through the production authority validator without
            // compiling it or retaining other generations.
            let retained = runtime.retain_backup()?;
            let mut after = 0;
            loop {
                let page = retained.page(after)?;
                if page.is_empty() {
                    break;
                }
                for entry in page {
                    ensure_live(cancellation)?;
                    retained.bundle(&entry.coordinate)?;
                    after = entry.sequence;
                }
            }
            Some(runtime)
        }
        _ => {
            return Err(ModelBackupError::Runtime(
                ProductionModelRuntimeError::RuntimeUnavailable,
            ));
        }
    };
    if cancellation.is_cancelled() {
        return Err(ModelBackupError::Cancelled);
    }
    let snapshot = match runtime.as_ref().map(|runtime| runtime.snapshot()) {
        Some(Ok(snapshot)) => snapshot,
        None | Some(Err(ProductionModelRuntimeError::EmptyRuntime)) => {
            ProductionModelRuntime::empty_snapshot()?
        }
        Some(Err(error)) => return Err(error.into()),
    };
    let model_domain = Arc::new(
        ModelDomainService::try_from_runtime_snapshot_with_forecasts(
            snapshot,
            evaluation_records,
            Arc::clone(&forecasts),
            target.analytical,
        )?,
    );
    Ok(RestoredModelAuthorities {
        _runtime: runtime,
        _forecasts: forecasts,
        model_domain,
    })
}

fn ensure_live(cancellation: &CancellationToken) -> Result<(), ModelBackupError> {
    if cancellation.is_cancelled() {
        return Err(ModelBackupError::Cancelled);
    }
    Ok(())
}

fn encode(value: &impl serde::Serialize) -> Result<Vec<u8>, ModelBackupError> {
    serde_json::to_vec(value).map_err(|_| ModelBackupError::Archive)
}

fn decode<T: serde::de::DeserializeOwned + serde::Serialize>(
    bytes: &[u8],
) -> Result<T, ModelBackupError> {
    let value: T = serde_json::from_slice(bytes).map_err(|_| ModelBackupError::Archive)?;
    if encode(&value)? != bytes {
        return Err(ModelBackupError::Archive);
    }
    Ok(value)
}

fn forecast_record_path(kind: u8, sequence: u64) -> String {
    format!("forecasts/{kind}/{sequence}/record.json")
}

fn forecast_artifact_path(kind: u8, sequence: u64) -> String {
    format!("forecasts/{kind}/{sequence}/artifact.json")
}

fn admission_archive_path(sequence: u64) -> String {
    format!("inventory/{sequence}/admission.json")
}
fn manifest_archive_path(sequence: u64) -> String {
    format!("inventory/{sequence}/manifest.json")
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
                "probability_outcomes",
                "probability_policy"
            ]
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
    Domain(#[from] ModelDomainServiceError),
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
