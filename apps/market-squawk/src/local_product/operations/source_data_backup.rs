//! Original sealed source closure and stopped paper state in the workspace SourceData stream.

use std::{
    fmt,
    io::{Read, Write},
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_adapter_paper::PaperCheckpointRepository;
use market_squawk_data::SealedSourceBackupInventory;
use market_squawk_domain::{SchemaVersion, SourceIdentifier};
use market_squawk_platform::LocalPaths;
use market_squawk_services::{
    ArtifactPublication, ArtifactPublicationContext, ArtifactReadContext, ArtifactReadRequest,
    ArtifactReference, ArtifactRepository,
};
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::workspace_backup::{
    DigestingWriter, MAXIMUM_COMPONENT_BYTES, WorkspaceComponentDescriptor,
    WorkspaceComponentSnapshotAuthority, WorkspaceComponentSnapshotLease,
    WorkspaceComponentSnapshotReceipt,
};
use crate::{
    ResearchService,
    application::{
        PaperAuditBackupKind, PaperStoppedBackupAuthority, PaperStoppedBackupLease,
        SourceAppliedCorporateActionPlanReference, SourceAppliedCorporateActionReadCapability,
        decision::DecisionApplication,
        backup::{
            ProductBackupComponentKind, ProductBackupComponentSchema, ProductBackupError,
            ProductBackupSensitivity, ProductBackupSnapshot,
        },
    },
};

const SOURCE_DATA_SCHEMA: &str = "market-squawk-source-data-v1";
const MAGIC: &[u8; 16] = b"MSQSOURCEDATA1\0\0";
const CHUNK_BYTES: usize = 64 * 1024;
const MAXIMUM_SNAPSHOT_BYTES: usize = 1024;
// The outer backup operation supplies its own cancellation/deadline. This finite ceiling also
// bounds retained synchronous work; one deadline is kept through each complete source operation.
const SOURCE_DATA_IO_BUDGET: Duration = Duration::from_secs(30 * 60);
const AUDIT_KINDS: [PaperAuditBackupKind; 2] =
    [PaperAuditBackupKind::Execution, PaperAuditBackupKind::State];

pub(crate) struct SourceDataWorkspaceBackupAuthority {
    research: Arc<ResearchService>,
    decisions: Arc<DecisionApplication>,
    paper: Arc<PaperStoppedBackupAuthority>,
    artifacts: Arc<dyn ArtifactRepository>,
    maximum_checkpoint_bytes: NonZeroUsize,
    maximum_artifact_bytes: NonZeroUsize,
    descriptors: [WorkspaceComponentDescriptor; 1],
}

impl SourceDataWorkspaceBackupAuthority {
    pub(super) fn try_new(
        research: Arc<ResearchService>,
        decisions: Arc<DecisionApplication>,
        paper: Arc<PaperStoppedBackupAuthority>,
        artifacts: Arc<dyn ArtifactRepository>,
        maximum_checkpoint_bytes: NonZeroUsize,
        maximum_artifact_bytes: NonZeroUsize,
    ) -> Result<Self, ProductBackupError> {
        let producer = SourceIdentifier::try_from(SOURCE_DATA_SCHEMA)
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        let schema =
            ProductBackupComponentSchema::try_new(producer.clone(), SchemaVersion::CURRENT)?;
        Ok(Self {
            research,
            decisions,
            paper,
            artifacts,
            maximum_checkpoint_bytes,
            maximum_artifact_bytes,
            descriptors: [WorkspaceComponentDescriptor::try_new(
                ProductBackupComponentKind::SourceData,
                producer,
                schema,
                ProductBackupSensitivity::Protected,
            )?],
        })
    }
}

impl fmt::Debug for SourceDataWorkspaceBackupAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SourceDataWorkspaceBackupAuthority([SEALED SOURCES AND STOPPED PAPER])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotAuthority for SourceDataWorkspaceBackupAuthority {
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor] {
        &self.descriptors
    }

    async fn retain(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn WorkspaceComponentSnapshotLease>, ProductBackupError> {
        let deadline = operation_deadline()?;
        ensure_live(deadline, cancellation)?;
        let paper = self
            .paper
            .retain(deadline, cancellation)
            .await
            .map_err(|_| operation_error("retain-paper", "owner-rejected", cancellation))?;
        if let Some(checkpoint) = paper.checkpoint() {
            for bytes in [
                checkpoint.manifest_bytes(),
                checkpoint.checkpoint_bytes(),
                checkpoint.configuration_history_bytes(),
            ] {
                if bytes.is_empty() || bytes.len() > self.maximum_checkpoint_bytes.get() {
                    return Err(ProductBackupError::InvalidComponent);
                }
            }
        }
        let source = SourceAppliedCorporateActionReadCapability::for_paper_backup(
            Arc::clone(&self.research),
            Arc::clone(&self.artifacts),
        );
        let recipe = match paper
            .checkpoint()
            .and_then(|checkpoint| checkpoint.action_source_reference())
        {
            Some(reference) => source
                .validate_paper_backup_source(reference, deadline, cancellation)
                .await
                .map_err(|_| operation_error("retain-paper-source", "owner-rejected", cancellation))?,
            None => None,
        };
        if recipe
            .as_ref()
            .is_some_and(|recipe| recipe.byte_count() > self.maximum_artifact_bytes.get())
        {
            return Err(ProductBackupError::InvalidComponent);
        }
        let decision_recipes = self.decisions.source_recipe_artifacts()
            .map_err(|_| ProductBackupError::SnapshotMismatch)?;
        let mut recipes = decision_recipes.clone();
        if let Some(reference) = &recipe {
            match recipes.binary_search_by(|entry| entry.id().cmp(reference.id())) {
                Ok(index) if recipes[index] != *reference => return Err(ProductBackupError::ArtifactMismatch),
                Ok(_) => {},
                Err(index) => {
                    recipes.try_reserve(1).map_err(|_| ProductBackupError::InvalidComponent)?;
                    recipes.insert(index, reference.clone());
                }
            }
        }
        if recipes.iter().any(|reference| reference.byte_count() > self.maximum_artifact_bytes.get()) {
            return Err(ProductBackupError::InvalidComponent);
        }
        let inventory = source_inventory(&self.research, deadline, cancellation).await?;
        if inventory.raw_bytes() > MAXIMUM_COMPONENT_BYTES {
            return Err(ProductBackupError::InvalidComponent);
        }
        let mut revision = Sha256::new();
        revision.update(b"market-squawk/source-data-workspace-authority/v1\0");
        revision.update(inventory.digest());
        match paper.checkpoint() {
            None => revision.update([0]),
            Some(checkpoint) => {
                revision.update([1]);
                for bytes in [
                    checkpoint.manifest_bytes(),
                    checkpoint.checkpoint_bytes(),
                    checkpoint.configuration_history_bytes(),
                ] {
                    revision.update((bytes.len() as u64).to_be_bytes());
                    revision.update(Sha256::digest(bytes));
                }
            }
        }
        for kind in AUDIT_KINDS {
            match paper.audits().iter().find(|audit| audit.kind() == kind) {
                Some(audit) => {
                    revision.update([1]);
                    revision.update(audit.bytes().to_be_bytes());
                    revision.update(audit.digest());
                }
                None => revision.update([0]),
            }
        }
        revision.update((recipes.len() as u64).to_be_bytes());
        for reference in &recipes {
            let bytes = serde_json::to_vec(reference).map_err(|_| ProductBackupError::ArtifactMismatch)?;
            revision.update((bytes.len() as u64).to_be_bytes());
            revision.update(bytes);
        }
        Ok(Box::new(RetainedSourceData {
            descriptors: self.descriptors.clone(),
            research: Arc::clone(&self.research),
            artifacts: Arc::clone(&self.artifacts),
            source,
            decisions: Arc::clone(&self.decisions),
            decision_recipes,
            paper,
            recipe,
            recipes,
            inventory,
            deadline,
            maximum_artifact_bytes: self.maximum_artifact_bytes,
            revision: revision.finalize().into(),
            issued: None,
        }))
    }
}

struct RetainedSourceData {
    descriptors: [WorkspaceComponentDescriptor; 1],
    research: Arc<ResearchService>,
    artifacts: Arc<dyn ArtifactRepository>,
    source: SourceAppliedCorporateActionReadCapability,
    paper: PaperStoppedBackupLease,
    recipe: Option<ArtifactReference>,
    decisions: Arc<DecisionApplication>,
    decision_recipes: Vec<ArtifactReference>,
    recipes: Vec<ArtifactReference>,
    inventory: SealedSourceBackupInventory,
    deadline: Instant,
    maximum_artifact_bytes: NonZeroUsize,
    revision: [u8; 32],
    issued: Option<(ProductBackupSnapshot, WorkspaceComponentSnapshotReceipt)>,
}
impl fmt::Debug for RetainedSourceData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RetainedSourceData([ORIGINAL SOURCE AND PAPER CUSTODY])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotLease for RetainedSourceData {
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor] {
        &self.descriptors
    }

    async fn write_snapshot(
        &mut self,
        kind: ProductBackupComponentKind,
        snapshot: ProductBackupSnapshot,
        writer: &mut (dyn Write + Send),
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceComponentSnapshotReceipt, ProductBackupError> {
        require_source(kind, self.deadline, cancellation)?;
        if self.issued.is_some() {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        let snapshot_bytes = snapshot_bytes(snapshot)?;
        let custody = self.paper.stream_custody();
        let mut output = DigestingWriter::new(writer, MAXIMUM_COMPONENT_BYTES);
        write_bytes(&mut output, MAGIC)?;
        write_bytes(&mut output, &(snapshot_bytes.len() as u32).to_be_bytes())?;
        write_bytes(&mut output, &snapshot_bytes)?;
        match custody.checkpoint() {
            Some(checkpoint) => {
                write_bytes(&mut output, &[1])?;
                for bytes in [checkpoint.manifest_bytes(), checkpoint.checkpoint_bytes(), checkpoint.configuration_history_bytes()] {
                    write_field(&mut output, bytes)?;
                }
            }
            None => write_bytes(&mut output, &[0])?,
        }
        write_bytes(&mut output, &u32::try_from(self.recipes.len())
            .map_err(|_| ProductBackupError::InvalidComponent)?.to_be_bytes())?;
        for reference in &self.recipes {
            ensure_live(self.deadline, cancellation)?;
            let reference_bytes = serde_json::to_vec(reference).map_err(|_| ProductBackupError::ArtifactMismatch)?;
            let read = self.artifacts.read(
                ArtifactReadRequest::try_new(reference.clone(), self.maximum_artifact_bytes)
                    .map_err(|_| ProductBackupError::ArtifactMismatch)?,
                ArtifactReadContext::new(cancellation.clone(), self.deadline),
            ).await.map_err(|_| operation_error("read-recipe", "owner-rejected", cancellation))?;
            write_field(&mut output, &reference_bytes)?;
            write_field(&mut output, read.content())?;
        }
        let analytical = self.research.analytical_service();
        let store = self.research.provider_capture_store();
        let inventory = self.inventory;
        let deadline = self.deadline;
        let (sender, receiver) = mpsc::channel(2);
        let worker = self
            .research
            .run_owned_research_io(deadline, cancellation, move |cancel| {
                let mut output = StreamWriter {
                    sender,
                    deadline,
                    cancellation: cancel.clone(),
                };
                for kind in AUDIT_KINDS {
                    if let Some(audit) = custody.audits().iter().find(|audit| audit.kind() == kind)
                    {
                        write_bytes(&mut output, &[1])?;
                        write_bytes(&mut output, &audit.bytes().to_be_bytes())?;
                        write_bytes(&mut output, &audit.digest())?;
                        audit
                            .write_to(&mut output, deadline, &cancel)
                            .map_err(|_| operation_error("write-paper-audit", "owner-rejected", &cancel))?;
                    } else {
                        write_bytes(&mut output, &[0])?;
                    }
                }
                analytical
                    .write_source_backup(inventory, store.as_ref(), &mut output, deadline, &cancel)
                    .map_err(|error| ingest_operation_error("write-sealed-sources", &error, &cancel))?;
                ensure_live(deadline, &cancel)
            });
        let transfer = receive_stream(receiver, &mut output, deadline, cancellation);
        let (joined, transferred) = tokio::join!(worker, transfer);
        let joined = joined.map_err(|error| worker_operation_error("write-worker", &error, cancellation));
        transferred?;
        joined??;
        let observed = output.finish()?;
        self.revalidate_owners(cancellation).await?;
        let receipt = WorkspaceComponentSnapshotReceipt::try_new(
            self.revision,
            observed.byte_length,
            observed.sha256,
        )?;
        self.issued = Some((snapshot, receipt));
        Ok(receipt)
    }

    async fn revalidate(
        &mut self,
        kind: ProductBackupComponentKind,
        snapshot: ProductBackupSnapshot,
        receipt: WorkspaceComponentSnapshotReceipt,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        require_source(kind, self.deadline, cancellation)?;
        if self.issued != Some((snapshot, receipt)) {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        self.revalidate_owners(cancellation).await
    }
}
impl RetainedSourceData {
    async fn revalidate_owners(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        self.paper
            .revalidate(self.deadline, cancellation)
            .await
            .map_err(|_| operation_error("revalidate-paper", "owner-rejected", cancellation))?;
        if let Some(reference) = self
            .paper
            .checkpoint()
            .and_then(|checkpoint| checkpoint.action_source_reference())
        {
            if self
                .source
                .validate_paper_backup_source(reference, self.deadline, cancellation)
                .await
                .map_err(|_| operation_error("revalidate-paper-source", "owner-rejected", cancellation))?
                != self.recipe
            {
                tracing::warn!(stage = "revalidate-paper-recipe", "source backup recipe changed");
                return Err(ProductBackupError::SnapshotMismatch);
            }
        }
        if self.decisions.source_recipe_artifacts().map_err(|_| ProductBackupError::SnapshotMismatch)? != self.decision_recipes {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        let observed = source_inventory(&self.research, self.deadline, cancellation).await?;
        if observed != self.inventory {
            tracing::warn!(
                stage = "revalidate-source-inventory",
                retained_claims = self.inventory.claims(),
                observed_claims = observed.claims(),
                retained_bytes = self.inventory.raw_bytes(),
                observed_bytes = observed.raw_bytes(),
                "source backup inventory changed"
            );
            return Err(ProductBackupError::SnapshotMismatch);
        }
        ensure_live(self.deadline, cancellation)
    }
}

/// Inert wire coordinates; the existing validated constructor admits the artifact reference.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RecipeArtifactReferenceWire {
    id: String,
    sha256: String,
    byte_count: usize,
    media_type: String,
}
impl RecipeArtifactReferenceWire {
    fn decode(self) -> Result<ArtifactReference, ProductBackupError> {
        ArtifactReference::try_new(self.id, self.sha256, self.byte_count, self.media_type)
            .map_err(|_| ProductBackupError::ArtifactMismatch)
    }
}

struct RestoredCheckpoint {
    manifest: Vec<u8>,
    checkpoint: Vec<u8>,
    configuration_history: Vec<u8>,
}

/// Caller owns the fresh inactive target and has restored its exact analytical catalog first.
/// The same research owner/store remains alive until raw and original paper-source admission ends.
#[allow(
    clippy::too_many_arguments,
    reason = "fresh owners, snapshot and existing allocation bounds remain explicit"
)]
pub(super) async fn restore_source_data_fresh(
    reader: &mut (dyn Read + Send),
    snapshot: ProductBackupSnapshot,
    paths: &LocalPaths,
    research: Arc<ResearchService>,
    artifacts: Arc<dyn ArtifactRepository>,
    maximum_checkpoint_bytes: NonZeroUsize,
    maximum_artifact_bytes: NonZeroUsize,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    let deadline = operation_deadline()?;
    ensure_live(deadline, cancellation)?;
    let mut magic = [0; 16];
    read_exact(reader, &mut magic, deadline, cancellation)?;
    if &magic != MAGIC {
        return Err(ProductBackupError::InvalidComponentSchema);
    }
    let mut length = [0; 4];
    read_exact(reader, &mut length, deadline, cancellation)?;
    let length = u32::from_be_bytes(length) as usize;
    if length == 0 || length > MAXIMUM_SNAPSHOT_BYTES {
        return Err(ProductBackupError::InvalidSnapshot);
    }
    if read_bytes(reader, length, deadline, cancellation)?.as_slice()
        != snapshot_bytes(snapshot)?.as_slice()
    {
        return Err(ProductBackupError::SnapshotMismatch);
    }
    let checkpoint = if read_presence(reader, deadline, cancellation)? {
        Some(Arc::new(RestoredCheckpoint {
            manifest: read_field(
                reader,
                maximum_checkpoint_bytes.get(),
                false,
                deadline,
                cancellation,
            )?,
            checkpoint: read_field(
                reader,
                maximum_checkpoint_bytes.get(),
                false,
                deadline,
                cancellation,
            )?,
            configuration_history: read_field(
                reader,
                maximum_checkpoint_bytes.get(),
                false,
                deadline,
                cancellation,
            )?,
        }))
    } else {
        None
    };
    // Decode through the actual stopped repository before trusting its inert source locator.
    let source_reference = match checkpoint.as_ref().map(Arc::clone) {
        Some(checkpoint) => research
            .run_owned_research_io(deadline, cancellation, move |cancel| {
                ensure_live(deadline, &cancel)?;
                PaperCheckpointRepository::backup_source_reference(
                    maximum_checkpoint_bytes,
                    &checkpoint.manifest,
                    &checkpoint.checkpoint,
                    &checkpoint.configuration_history,
                )
                .map_err(|_| ProductBackupError::RestoreComponents)
            })
            .await
            .map_err(|error| worker_operation_error("restore-checkpoint-decode-worker", &error, cancellation))??,
        None => None,
    };
    let expected_recipe = source_reference
        .as_deref()
        .map(|bytes| {
            SourceAppliedCorporateActionPlanReference::from_paper_checkpoint(bytes)
                .and_then(|reference| reference.current_recipe_artifact())
                .map_err(|_| ProductBackupError::RestoreComponents)
        })
        .transpose()?
        .flatten();
    let checkpoint_bytes = checkpoint.as_ref().map_or(0_u64, |checkpoint| {
        24 + checkpoint.manifest.len() as u64
            + checkpoint.checkpoint.len() as u64
            + checkpoint.configuration_history.len() as u64
    });
    let mut prefix_bytes = 16_u64 + 4 + length as u64 + 1 + checkpoint_bytes + 4;
    let mut count = [0; 4];
    read_exact(reader, &mut count, deadline, cancellation)?;
    let count = u32::from_be_bytes(count);
    // Each entry requires two nonempty length-prefixed fields within the component ceiling.
    if u64::from(count) > MAXIMUM_COMPONENT_BYTES / 18 { return Err(ProductBackupError::InvalidComponent); }
    let mut previous: Option<ArtifactReference> = None;
    let mut paper_recipe_restored = expected_recipe.is_none();
    for _ in 0..count {
        let reference_bytes = read_field(reader, MAXIMUM_SNAPSHOT_BYTES, false, deadline, cancellation)?;
        let expected = serde_json::from_slice::<RecipeArtifactReferenceWire>(&reference_bytes)
            .map_err(|_| ProductBackupError::ArtifactMismatch)?.decode()?;
        if previous.as_ref().is_some_and(|value| value.id() >= expected.id())
            || expected.byte_count() > maximum_artifact_bytes.get() {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        let recipe = read_field(reader, maximum_artifact_bytes.get(), false, deadline, cancellation)?;
        prefix_bytes = prefix_bytes.checked_add(16 + reference_bytes.len() as u64 + recipe.len() as u64)
            .filter(|bytes| *bytes <= MAXIMUM_COMPONENT_BYTES).ok_or(ProductBackupError::InvalidComponent)?;
        let publication = ArtifactPublication::try_json(recipe).map_err(|_| ProductBackupError::ArtifactMismatch)?;
        if !expected.matches(&publication) { return Err(ProductBackupError::ArtifactMismatch); }
        let actual = artifacts.publish(publication, ArtifactPublicationContext::new(cancellation.clone(), deadline))
            .await.map_err(|_| operation_error("restore-recipe-publication", "owner-rejected", cancellation))?;
        if actual != expected { return Err(ProductBackupError::ArtifactMismatch); }
        if expected_recipe.as_ref() == Some(&expected) { paper_recipe_restored = true; }
        previous = Some(expected);
    }
    if !paper_recipe_restored { return Err(ProductBackupError::ArtifactMismatch); }
    let analytical = research.analytical_service();
    let store = research.provider_capture_store();
    let control = paths
        .control_root()
        .map_err(|_| ProductBackupError::InvalidRestoreTarget)?;
    let directory = control
        .try_clone_directory()
        .map_err(|_| ProductBackupError::InvalidRestoreTarget)?;
    let (sender, receiver) = mpsc::channel(2);
    let worker = research.run_owned_research_io(deadline, cancellation, move |cancel| {
        let mut input = StreamReader {
            receiver,
            chunk: Vec::new(),
            offset: 0,
        };
        let mut present = [false; 2];
        let mut audit_total = 0_u64;
        for (index, kind) in AUDIT_KINDS.into_iter().enumerate() {
            present[index] = read_presence(&mut input, deadline, &cancel)?;
            if !present[index] {
                match directory.symlink_metadata(kind.file_name()) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    _ => return Err(ProductBackupError::RestoreComponents),
                }
                continue;
            }
            let mut length = [0; 8];
            let mut digest = [0; 32];
            read_exact(&mut input, &mut length, deadline, &cancel)?;
            read_exact(&mut input, &mut digest, deadline, &cancel)?;
            let length = u64::from_be_bytes(length);
            audit_total = audit_total
                .checked_add(length)
                .filter(|bytes| *bytes <= MAXIMUM_COMPONENT_BYTES)
                .ok_or(ProductBackupError::InvalidComponent)?;
            restore_audit(
                &directory, kind, length, digest, &mut input, deadline, &cancel,
            )?;
        }
        sync_directory(&directory)?;
        analytical
            .restore_source_backup(store.as_ref(), &mut input, deadline, &cancel)
            .map_err(|error| ingest_operation_error("restore-sealed-sources", &error, &cancel))?;
        let mut trailing = [0];
        ensure_live(deadline, &cancel)?;
        if input
            .read(&mut trailing)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?
            != 0
        {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        ensure_live(deadline, &cancel)?;
        Ok::<_, ProductBackupError>(present)
    });
    let transfer = send_stream(sender, reader, prefix_bytes, deadline, cancellation);
    let (joined, transferred) = tokio::join!(worker, transfer);
    let joined = joined.map_err(|error| worker_operation_error("restore-worker", &error, cancellation));
    transferred?;
    let audits = joined??;
    // Reopen the original raw captures, immutable identities, calendar, recipe and financial
    // projection. No active provider runtime or fabricated currentness is introduced by restore.
    if let Some(reference) = source_reference.as_deref() {
        let source = SourceAppliedCorporateActionReadCapability::for_paper_backup(
            Arc::clone(&research),
            artifacts,
        );
        if source
            .validate_paper_backup_source(reference, deadline, cancellation)
            .await
            .map_err(|_| operation_error("restore-paper-source", "owner-rejected", cancellation))?
            != expected_recipe
        {
            return Err(ProductBackupError::RestoreComponents);
        }
    }
    let root = paths
        .artifacts()
        .map_err(|_| ProductBackupError::InvalidRestoreTarget)?
        .clone();
    research
        .run_owned_research_io(deadline, cancellation, move |cancel| {
            ensure_live(deadline, &cancel)?;
            if let Some(checkpoint) = checkpoint {
                let mut restored = PaperCheckpointRepository::restore_fresh(
                    root,
                    maximum_checkpoint_bytes,
                    &checkpoint.manifest,
                    &checkpoint.checkpoint,
                    &checkpoint.configuration_history,
                )
                .map_err(|_| ProductBackupError::RestoreComponents)?;
                let recovery = restored
                    .take_recovery()
                    .ok_or(ProductBackupError::RestoreComponents)?;
                if recovery.checkpoint().sequence() > 0 && audits.iter().any(|present| !present) {
                    return Err(ProductBackupError::RestoreComponents);
                }
            } else {
                let empty = PaperCheckpointRepository::retain_stopped_backup(
                    &root,
                    maximum_checkpoint_bytes,
                )
                .map_err(|_| ProductBackupError::RestoreComponents)?;
                if empty.checkpoint().is_some() {
                    return Err(ProductBackupError::RestoreComponents);
                }
            }
            ensure_live(deadline, &cancel)
        })
        .await
        .map_err(|error| worker_operation_error("restore-checkpoint-worker", &error, cancellation))??;
    ensure_live(deadline, cancellation)
}

async fn source_inventory(
    research: &ResearchService,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<SealedSourceBackupInventory, ProductBackupError> {
    let analytical = research.analytical_service();
    research
        .run_owned_research_io(deadline, cancellation, move |cancel| {
            analytical
                .source_backup_inventory(deadline, &cancel)
                .map_err(|error| ingest_operation_error("source-inventory", &error, &cancel))
        })
        .await
        .map_err(|error| worker_operation_error("source-inventory-worker", &error, cancellation))?
}

// Only the synchronous worker blocks on a bounded channel. Async peers own their endpoint, so
// cancellation, sink failure or caller Drop closes it and releases the retained worker's wait.
struct StreamWriter {
    sender: mpsc::Sender<Vec<u8>>,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl Write for StreamWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.cancellation.is_cancelled() || Instant::now() >= self.deadline {
            return Err(std::io::Error::other("source backup stopped"));
        }
        let count = bytes.len().min(CHUNK_BYTES);
        if count == 0 {
            return Ok(0);
        }
        let mut chunk = Vec::new();
        chunk
            .try_reserve_exact(count)
            .map_err(|_| std::io::Error::other("source backup allocation failed"))?;
        chunk.extend_from_slice(&bytes[..count]);
        self.sender
            .blocking_send(chunk)
            .map_err(|_| std::io::Error::other("source backup sink closed"))?;
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
struct StreamReader {
    receiver: mpsc::Receiver<Vec<u8>>,
    chunk: Vec<u8>,
    offset: usize,
}
impl Read for StreamReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.offset == self.chunk.len() {
            let Some(chunk) = self.receiver.blocking_recv() else {
                return Ok(0);
            };
            self.chunk = chunk;
            self.offset = 0;
        }
        let count = buffer.len().min(self.chunk.len() - self.offset);
        buffer[..count].copy_from_slice(&self.chunk[self.offset..self.offset + count]);
        self.offset += count;
        Ok(count)
    }
}
async fn receive_stream(
    mut receiver: mpsc::Receiver<Vec<u8>>,
    writer: &mut (dyn Write + Send),
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    loop {
        let chunk = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ProductBackupError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => return Err(ProductBackupError::ArtifactUnavailable),
            chunk = receiver.recv() => chunk,
        };
        let Some(chunk) = chunk else {
            break;
        };
        write_bytes(writer, &chunk)?;
    }
    ensure_live(deadline, cancellation)
}
async fn send_stream(
    sender: mpsc::Sender<Vec<u8>>,
    reader: &mut (dyn Read + Send),
    mut observed: u64,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    loop {
        ensure_live(deadline, cancellation)?;
        let mut chunk = Vec::new();
        chunk
            .try_reserve_exact(CHUNK_BYTES)
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        chunk.resize(CHUNK_BYTES, 0);
        let read = reader
            .read(&mut chunk)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        if read == 0 {
            break;
        }
        chunk.truncate(read);
        observed = observed
            .checked_add(read as u64)
            .filter(|bytes| *bytes <= MAXIMUM_COMPONENT_BYTES)
            .ok_or(ProductBackupError::InvalidComponent)?;
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ProductBackupError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => return Err(ProductBackupError::ArtifactUnavailable),
            sent = sender.send(chunk) => sent.map_err(|_| ProductBackupError::ArtifactMismatch)?,
        }
    }
    ensure_live(deadline, cancellation)
}
fn restore_audit(
    directory: &Dir,
    kind: PaperAuditBackupKind,
    length: u64,
    digest: [u8; 32],
    reader: &mut dyn Read,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .follow(FollowSymlinks::No);
    #[cfg(unix)]
    {
        use cap_std::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = directory
        .open_with(kind.file_name(), &options)
        .map_err(|_| ProductBackupError::RestoreComponents)?;
    let mut remaining = length;
    let mut hash = Sha256::new();
    let mut buffer = [0; CHUNK_BYTES];
    while remaining != 0 {
        let count = remaining.min(CHUNK_BYTES as u64) as usize;
        read_exact(reader, &mut buffer[..count], deadline, cancellation)?;
        file.write_all(&buffer[..count])
            .map_err(|_| ProductBackupError::RestoreComponents)?;
        hash.update(&buffer[..count]);
        remaining -= count as u64;
    }
    if <[u8; 32]>::from(hash.finalize()) != digest {
        return Err(ProductBackupError::ArtifactMismatch);
    }
    file.sync_all()
        .map_err(|_| ProductBackupError::RestoreComponents)?;
    ensure_live(deadline, cancellation)
}
#[cfg(unix)]
fn sync_directory(directory: &Dir) -> Result<(), ProductBackupError> {
    use cap_std::fs::OpenOptionsExt as _;
    let mut options = OpenOptions::new();
    options
        .read(true)
        .follow(FollowSymlinks::No)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    directory
        .open_with(".", &options)
        .and_then(|file| file.sync_all())
        .map_err(|_| ProductBackupError::RestoreComponents)
}
#[cfg(not(unix))]
fn sync_directory(_directory: &Dir) -> Result<(), ProductBackupError> {
    Ok(())
}
fn write_bytes(writer: &mut dyn Write, bytes: &[u8]) -> Result<(), ProductBackupError> {
    writer
        .write_all(bytes)
        .map_err(|_| ProductBackupError::ArtifactUnavailable)
}
fn write_field(writer: &mut dyn Write, bytes: &[u8]) -> Result<(), ProductBackupError> {
    write_bytes(writer, &(bytes.len() as u64).to_be_bytes())?;
    write_bytes(writer, bytes)
}
fn read_exact(
    reader: &mut dyn Read,
    bytes: &mut [u8],
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    for chunk in bytes.chunks_mut(CHUNK_BYTES) {
        ensure_live(deadline, cancellation)?;
        reader
            .read_exact(chunk)
            .map_err(|_| ProductBackupError::ArtifactMismatch)?;
    }
    ensure_live(deadline, cancellation)
}
fn read_presence(
    reader: &mut dyn Read,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<bool, ProductBackupError> {
    let mut present = [0];
    read_exact(reader, &mut present, deadline, cancellation)?;
    match present[0] {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ProductBackupError::InvalidComponent),
    }
}
fn read_bytes(
    reader: &mut dyn Read,
    length: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, ProductBackupError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| ProductBackupError::InvalidComponent)?;
    bytes.resize(length, 0);
    read_exact(reader, &mut bytes, deadline, cancellation)?;
    Ok(bytes)
}
fn read_field(
    reader: &mut dyn Read,
    maximum: usize,
    empty: bool,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, ProductBackupError> {
    let mut length = [0; 8];
    read_exact(reader, &mut length, deadline, cancellation)?;
    let length = usize::try_from(u64::from_be_bytes(length))
        .map_err(|_| ProductBackupError::InvalidComponent)?;
    if length > maximum || !empty && length == 0 {
        return Err(ProductBackupError::InvalidComponent);
    }
    read_bytes(reader, length, deadline, cancellation)
}
fn snapshot_bytes(snapshot: ProductBackupSnapshot) -> Result<Vec<u8>, ProductBackupError> {
    let bytes = serde_json::to_vec(&snapshot).map_err(|_| ProductBackupError::InvalidSnapshot)?;
    if bytes.is_empty() || bytes.len() > MAXIMUM_SNAPSHOT_BYTES {
        return Err(ProductBackupError::InvalidSnapshot);
    }
    Ok(bytes)
}
fn require_source(
    kind: ProductBackupComponentKind,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    ensure_live(deadline, cancellation)?;
    if kind != ProductBackupComponentKind::SourceData {
        return Err(ProductBackupError::InvalidComponent);
    }
    Ok(())
}
fn operation_deadline() -> Result<Instant, ProductBackupError> {
    Instant::now()
        .checked_add(SOURCE_DATA_IO_BUDGET)
        .ok_or(ProductBackupError::ArtifactUnavailable)
}
fn ensure_live(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    if cancellation.is_cancelled() {
        Err(ProductBackupError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ProductBackupError::ArtifactUnavailable)
    } else {
        Ok(())
    }
}
// Diagnostics expose only closed classes and fixed stages, never provider payloads, paths,
// identifiers, or formatted nested I/O errors. They do not change snapshot admission.
fn ingest_operation_error(
    stage: &'static str,
    error: &market_squawk_data::IngestError,
    cancellation: &CancellationToken,
) -> ProductBackupError {
    use market_squawk_data::IngestError;
    use market_squawk_platform::{ResearchObjectControlError, SealedResearchJournalStoreError};
    let class = match error {
        IngestError::AuthorityBusy => "catalog-authority-busy",
        IngestError::AuthorityLockPoisoned => "catalog-authority-poisoned",
        IngestError::Cancelled => "cancelled",
        IngestError::DeadlineExceeded => "deadline-exceeded",
        IngestError::ProviderCaptureRequired => "source-inventory-or-stream-mismatch",
        IngestError::Catalog(_) => "catalog-read-rejected",
        IngestError::SealedProviderCapture(error) => match error {
            SealedResearchJournalStoreError::ObjectControl(ResearchObjectControlError::Unavailable) => "raw-store-control-unavailable",
            SealedResearchJournalStoreError::ObjectControl(ResearchObjectControlError::Cancelled) => "raw-store-cancelled",
            SealedResearchJournalStoreError::ObjectControl(ResearchObjectControlError::DeadlineExceeded) => "raw-store-deadline-exceeded",
            SealedResearchJournalStoreError::OperationLockPoisoned => "raw-store-lock-poisoned",
            SealedResearchJournalStoreError::Io { .. } => "raw-store-io",
            SealedResearchJournalStoreError::ReceiptMismatch | SealedResearchJournalStoreError::ObjectReceiptMismatch => "raw-store-receipt-mismatch",
            SealedResearchJournalStoreError::StateConflict => "raw-store-state-conflict",
            _ => "raw-store-validation-rejected",
        },
        _ => "source-authority-rejected",
    };
    operation_error(stage, class, cancellation)
}

fn worker_operation_error(
    stage: &'static str,
    error: &crate::ResearchServiceError,
    cancellation: &CancellationToken,
) -> ProductBackupError {
    match error {
        crate::ResearchServiceError::Ingest(error) => ingest_operation_error(stage, error, cancellation),
        _ => operation_error(stage, "research-worker-unavailable", cancellation),
    }
}

fn operation_error(
    stage: &'static str,
    class: &'static str,
    cancellation: &CancellationToken,
) -> ProductBackupError {
    tracing::warn!(stage, class, "source backup owner operation failed");
    if cancellation.is_cancelled() {
        ProductBackupError::Cancelled
    } else {
        ProductBackupError::SnapshotMismatch
    }
}
