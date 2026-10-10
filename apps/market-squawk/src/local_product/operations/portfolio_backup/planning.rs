//! Streamed immutable planning artifacts bound to the restored catalog's exact inventory.

use std::{
    io::{Read, Write},
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk_data::{
    PortfolioPlanningCatalogCapability, PortfolioPlanningChainHead, PortfolioPlanningCompletion,
    PortfolioPlanningHead,
};
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactReadContext,
    ArtifactReadRequest, ArtifactReference, ArtifactRepository,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::super::workspace_backup::{
    DigestingWriter, MAXIMUM_COMPONENT_BYTES, WorkspaceComponentSnapshotReceipt,
};
use crate::{ResearchService, application::backup::ProductBackupError};

const MAGIC: &[u8; 16] = b"MSQPORTFOLIO1\0\0\0";
const ARTIFACT_DEADLINE: Duration = Duration::from_secs(60);
const CHUNK_BYTES: usize = 64 * 1024;

pub(super) struct RetainedPlanning {
    catalog: PortfolioPlanningCatalogCapability,
    research: Arc<ResearchService>,
    artifacts: Arc<dyn ArtifactRepository>,
    head: PortfolioPlanningHead,
    authority_revision: [u8; 32],
}

impl RetainedPlanning {
    pub(super) async fn retain(
        catalog: PortfolioPlanningCatalogCapability,
        research: Arc<ResearchService>,
        artifacts: Arc<dyn ArtifactRepository>,
        portfolio_revision: [u8; 32],
        cancellation: &CancellationToken,
    ) -> Result<Self, ProductBackupError> {
        ensure_live(cancellation)?;
        let head = current_head(&research, &catalog, cancellation).await?;
        let authority_revision = combined_revision(portfolio_revision, head);
        ensure_live(cancellation)?;
        Ok(Self {
            catalog,
            research,
            artifacts,
            head,
            authority_revision,
        })
    }

    pub(super) const fn authority_revision(&self) -> [u8; 32] {
        self.authority_revision
    }

    pub(super) async fn revalidate(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        require_head(&self.research, &self.catalog, self.head, cancellation).await
    }

    pub(super) async fn write(
        &self,
        portfolios: &[u8],
        writer: &mut (dyn Write + Send),
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceComponentSnapshotReceipt, ProductBackupError> {
        self.revalidate(cancellation).await?;
        let mut output = DigestingWriter::new(writer, MAXIMUM_COMPONENT_BYTES);
        append(&mut output, MAGIC, cancellation)?;
        for head in [self.head.completions, self.head.saves] {
            append(&mut output, &head.sequence.to_be_bytes(), cancellation)?;
            append(&mut output, &head.sha256, cancellation)?;
        }
        append(
            &mut output,
            &(portfolios.len() as u64).to_be_bytes(),
            cancellation,
        )?;
        append(&mut output, portfolios, cancellation)?;
        let mut after = PortfolioPlanningHead::empty().completions;
        loop {
            ensure_live(cancellation)?;
            let catalog = self.catalog.clone();
            let head = self.head;
            let page = catalog_io(&self.research, cancellation, move || {
                catalog
                    .completion_page(head, after.sequence)
                    .map_err(|_| ProductBackupError::SnapshotMismatch)
            })
            .await?;
            if page.is_empty() {
                break;
            }
            for entry in page {
                require_successor(after, entry.head)?;
                let reference = artifact_reference(&entry.completion)?;
                let read = self
                    .artifacts
                    .read(
                        ArtifactReadRequest::try_new(reference.clone(), artifact_limit()?)
                            .map_err(map_artifact)?,
                        ArtifactReadContext::new(cancellation.clone(), deadline()?),
                    )
                    .await
                    .map_err(map_artifact)?;
                if read.reference() != &reference {
                    return Err(ProductBackupError::ArtifactMismatch);
                }
                append(
                    &mut output,
                    entry.completion.calculation_token.as_bytes(),
                    cancellation,
                )?;
                append(
                    &mut output,
                    &entry.completion.artifact_byte_length.to_be_bytes(),
                    cancellation,
                )?;
                append(&mut output, read.content(), cancellation)?;
                after = entry.head;
            }
        }
        if after != self.head.completions {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        verify_saved_inventory(&self.research, &self.catalog, self.head, cancellation).await?;
        self.revalidate(cancellation).await?;
        let observed = output.finish()?;
        WorkspaceComponentSnapshotReceipt::try_new(
            self.authority_revision,
            observed.byte_length,
            observed.sha256,
        )
    }
}

/// Restores one artifact at a time; catalog rows already came from the analytical snapshot.
pub(super) async fn restore(
    reader: &mut (dyn Read + Send),
    catalog: &PortfolioPlanningCatalogCapability,
    research: &ResearchService,
    artifacts: &dyn ArtifactRepository,
    maximum_portfolio_bytes: usize,
    maximum_artifact_bytes: NonZeroUsize,
    cancellation: &CancellationToken,
) -> Result<(Vec<u8>, PortfolioPlanningHead), ProductBackupError> {
    let mut input = PlanningReader {
        reader,
        observed: 0,
    };
    let mut magic = [0; 16];
    input.exact(&mut magic, cancellation)?;
    if &magic != MAGIC {
        return Err(ProductBackupError::InvalidComponentSchema);
    }
    let head = PortfolioPlanningHead {
        completions: input.head(cancellation)?,
        saves: input.head(cancellation)?,
    };
    require_head(research, catalog, head, cancellation).await?;
    let portfolio_length = input.length(cancellation)?;
    if portfolio_length == 0 || portfolio_length > maximum_portfolio_bytes {
        return Err(ProductBackupError::RestoreComponents);
    }
    let portfolios = input.bytes(portfolio_length, cancellation)?;
    let mut after = PortfolioPlanningHead::empty().completions;
    loop {
        ensure_live(cancellation)?;
        let page_catalog = catalog.clone();
        let page = catalog_io(research, cancellation, move || {
            page_catalog
                .completion_page(head, after.sequence)
                .map_err(|_| ProductBackupError::SnapshotMismatch)
        })
        .await?;
        if page.is_empty() {
            break;
        }
        for entry in page {
            require_successor(after, entry.head)?;
            let expected = artifact_reference(&entry.completion)?;
            let mut token = [0; 16];
            input.exact(&mut token, cancellation)?;
            let length = input.length(cancellation)?;
            if token != *entry.completion.calculation_token.as_bytes()
                || length != expected.byte_count()
                || length > maximum_artifact_bytes.get()
                || portfolio_length
                    .checked_add(length)
                    .is_none_or(|total| total > maximum_portfolio_bytes)
            {
                return Err(ProductBackupError::ArtifactMismatch);
            }
            let bytes = input.bytes(length, cancellation)?;
            if <[u8; 32]>::from(Sha256::digest(&bytes)) != entry.completion.artifact_sha256 {
                return Err(ProductBackupError::ArtifactMismatch);
            }
            let publication = ArtifactPublication::try_json(bytes).map_err(map_artifact)?;
            let actual = artifacts
                .publish(
                    publication,
                    ArtifactPublicationContext::new(cancellation.clone(), deadline()?),
                )
                .await
                .map_err(map_artifact)?;
            if actual != expected {
                return Err(ProductBackupError::ArtifactMismatch);
            }
            after = entry.head;
        }
    }
    if after != head.completions {
        return Err(ProductBackupError::SnapshotMismatch);
    }
    verify_saved_inventory(research, catalog, head, cancellation).await?;
    input.finish(cancellation)?;
    require_head(research, catalog, head, cancellation).await?;
    Ok((portfolios, head))
}

pub(super) fn combined_revision(
    portfolio_revision: [u8; 32],
    head: PortfolioPlanningHead,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-workspace-backup/v1\0");
    digest.update(portfolio_revision);
    for head in [head.completions, head.saves] {
        digest.update(head.sequence.to_be_bytes());
        digest.update(head.sha256);
    }
    digest.finalize().into()
}

async fn verify_saved_inventory(
    research: &ResearchService,
    catalog: &PortfolioPlanningCatalogCapability,
    head: PortfolioPlanningHead,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    let mut after = PortfolioPlanningHead::empty().saves;
    loop {
        ensure_live(cancellation)?;
        let page_catalog = catalog.clone();
        let page = catalog_io(research, cancellation, move || {
            page_catalog
                .save_page(head, after.sequence)
                .map_err(|_| ProductBackupError::SnapshotMismatch)
        })
        .await?;
        if page.is_empty() {
            break;
        }
        for entry in page {
            require_successor(after, entry.head)?;
            after = entry.head;
        }
    }
    if after != head.saves {
        return Err(ProductBackupError::SnapshotMismatch);
    }
    Ok(())
}

fn require_successor(
    previous: PortfolioPlanningChainHead,
    current: PortfolioPlanningChainHead,
) -> Result<(), ProductBackupError> {
    if previous.sequence.checked_add(1) != Some(current.sequence) {
        return Err(ProductBackupError::SnapshotMismatch);
    }
    Ok(())
}

async fn require_head(
    research: &ResearchService,
    catalog: &PortfolioPlanningCatalogCapability,
    expected: PortfolioPlanningHead,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    if current_head(research, catalog, cancellation).await? != expected {
        return Err(ProductBackupError::SnapshotMismatch);
    }
    Ok(())
}

async fn current_head(
    research: &ResearchService,
    catalog: &PortfolioPlanningCatalogCapability,
    cancellation: &CancellationToken,
) -> Result<PortfolioPlanningHead, ProductBackupError> {
    let catalog = catalog.clone();
    catalog_io(research, cancellation, move || {
        catalog
            .head()
            .map_err(|_| ProductBackupError::SnapshotMismatch)
    })
    .await
}

// Each bounded page runs sequentially on the existing owned blocking lane. The runner joins
// the original handle before returning, so cancellation cannot release a retained lease early.
async fn catalog_io<T, F>(
    research: &ResearchService,
    cancellation: &CancellationToken,
    operation: F,
) -> Result<T, ProductBackupError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, ProductBackupError> + Send + 'static,
{
    ensure_live(cancellation)?;
    let deadline = deadline()?;
    let result = research
        .run_owned_research_io_joined(deadline, cancellation, move |worker_cancellation| {
            ensure_catalog_live(deadline, &worker_cancellation)?;
            let result = operation();
            ensure_catalog_live(deadline, &worker_cancellation)?;
            result
        })
        .await
        .map_err(|_| {
            if cancellation.is_cancelled() {
                ProductBackupError::Cancelled
            } else {
                ProductBackupError::ArtifactUnavailable
            }
        })?;
    ensure_catalog_live(deadline, cancellation)?;
    result
}

fn ensure_catalog_live(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    ensure_live(cancellation)?;
    if Instant::now() >= deadline {
        return Err(ProductBackupError::ArtifactUnavailable);
    }
    Ok(())
}

fn artifact_reference(
    completion: &PortfolioPlanningCompletion,
) -> Result<ArtifactReference, ProductBackupError> {
    if completion.artifact_media_type != "application/json" {
        return Err(ProductBackupError::ArtifactMismatch);
    }
    crate::portfolio_application::planning_artifact_reference(completion)
        .map_err(super::map_portfolio_backup_error)
}

fn artifact_limit() -> Result<NonZeroUsize, ProductBackupError> {
    NonZeroUsize::new(crate::local_product::LOCAL_MAXIMUM_ARTIFACT_BYTES)
        .ok_or(ProductBackupError::InvalidComponent)
}

fn deadline() -> Result<Instant, ProductBackupError> {
    Instant::now()
        .checked_add(ARTIFACT_DEADLINE)
        .ok_or(ProductBackupError::InvalidComponent)
}

fn ensure_live(cancellation: &CancellationToken) -> Result<(), ProductBackupError> {
    if cancellation.is_cancelled() {
        Err(ProductBackupError::Cancelled)
    } else {
        Ok(())
    }
}

fn map_artifact(error: ArtifactError) -> ProductBackupError {
    match error {
        ArtifactError::Cancelled => ProductBackupError::Cancelled,
        _ => ProductBackupError::ArtifactMismatch,
    }
}

fn append(
    writer: &mut impl Write,
    bytes: &[u8],
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    for chunk in bytes.chunks(CHUNK_BYTES) {
        ensure_live(cancellation)?;
        writer
            .write_all(chunk)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
    }
    Ok(())
}

struct PlanningReader<'a> {
    reader: &'a mut (dyn Read + Send),
    observed: u64,
}

impl PlanningReader<'_> {
    fn exact(
        &mut self,
        bytes: &mut [u8],
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        self.observed = self
            .observed
            .checked_add(bytes.len() as u64)
            .filter(|length| *length <= MAXIMUM_COMPONENT_BYTES)
            .ok_or(ProductBackupError::InvalidComponent)?;
        for chunk in bytes.chunks_mut(CHUNK_BYTES) {
            ensure_live(cancellation)?;
            self.reader
                .read_exact(chunk)
                .map_err(|_| ProductBackupError::ArtifactMismatch)?;
        }
        Ok(())
    }
    fn length(&mut self, cancellation: &CancellationToken) -> Result<usize, ProductBackupError> {
        let mut length = [0; 8];
        self.exact(&mut length, cancellation)?;
        usize::try_from(u64::from_be_bytes(length))
            .map_err(|_| ProductBackupError::InvalidComponent)
    }
    fn head(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<PortfolioPlanningChainHead, ProductBackupError> {
        let mut sequence = [0; 8];
        let mut sha256 = [0; 32];
        self.exact(&mut sequence, cancellation)?;
        self.exact(&mut sha256, cancellation)?;
        Ok(PortfolioPlanningChainHead {
            sequence: u64::from_be_bytes(sequence),
            sha256,
        })
    }
    fn bytes(
        &mut self,
        length: usize,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, ProductBackupError> {
        if self
            .observed
            .checked_add(length as u64)
            .is_none_or(|value| value > MAXIMUM_COMPONENT_BYTES)
        {
            return Err(ProductBackupError::InvalidComponent);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        bytes.resize(length, 0);
        self.exact(&mut bytes, cancellation)?;
        Ok(bytes)
    }
    fn finish(&mut self, cancellation: &CancellationToken) -> Result<(), ProductBackupError> {
        ensure_live(cancellation)?;
        if self
            .reader
            .read(&mut [0])
            .map_err(|_| ProductBackupError::ArtifactMismatch)?
            != 0
        {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_squawk_data::{
        CatalogConfig, CatalogLimit, CatalogResultLimits, ObjectStoreConfig, PortfolioPlanningKind,
    };
    use market_squawk_domain::{AccountId, Timestamp};
    use market_squawk_platform::LocalPaths;
    use uuid::Uuid;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn owners(
        paths: &LocalPaths,
        reopen: bool,
    ) -> TestResult<(Arc<ResearchService>, Arc<dyn ArtifactRepository>)> {
        let config = CatalogConfig::try_new(
            paths.catalog()?.clone(),
            Duration::from_millis(250),
            CatalogLimit::new(32)?,
            CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
        )?;
        let objects = ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?;
        let research = if reopen {
            ResearchService::open(paths, config, 8, objects)?
        } else {
            ResearchService::initialize(paths, config, 8, objects)?
        };
        let artifacts = crate::artifact_repository::controlled_artifact_repository(
            paths.artifacts()?.clone(),
            artifact_limit()?,
        )?;
        Ok((Arc::new(research), artifacts))
    }

    #[tokio::test]
    async fn planning_backup_restores_saved_and_unsaved_artifacts_and_rejects_damage() -> TestResult
    {
        let temporary = tempfile::tempdir()?;
        let source_paths = LocalPaths::prepare(temporary.path().join("source"))?;
        let target_paths = LocalPaths::prepare(temporary.path().join("target"))?;
        let (source, source_artifacts) = owners(&source_paths, false)?;
        let (target, target_artifacts) = owners(&target_paths, false)?;
        let source_catalog = source.analytical_service().portfolio_planning();
        let target_catalog = target.analytical_service().portfolio_planning();
        let cancellation = CancellationToken::new();
        let account = AccountId::try_from(Uuid::from_u128(1))?;
        let originals = [
            b"{\n  \"originalResult\": \"saved 12.3400\"\n}\n".to_vec(),
            b"{\n  \"originalResult\": \"unsaved -9.8700\"\n}\n".to_vec(),
        ];
        let mut completions = Vec::new();
        for (index, bytes) in originals.iter().enumerate() {
            let reference = source_artifacts
                .publish(
                    ArtifactPublication::try_json(bytes.clone())?,
                    ArtifactPublicationContext::new(cancellation.clone(), deadline()?),
                )
                .await?;
            let completion = PortfolioPlanningCompletion {
                calculation_token: Uuid::from_u128(10 + index as u128),
                account_id: account,
                kind: PortfolioPlanningKind::ScenarioBatch,
                snapshot_token: Uuid::from_u128(20),
                calculated_at: Timestamp::from_unix_nanos(300),
                portfolio_effective_at: Timestamp::from_unix_nanos(100),
                portfolio_available_at: Some(Timestamp::from_unix_nanos(200)),
                artifact_id: reference.id().to_owned(),
                artifact_sha256: Sha256::digest(bytes).into(),
                artifact_byte_length: bytes.len() as u64,
                artifact_media_type: reference.media_type().to_owned(),
            };
            assert!(source_catalog.complete(&completion)?.1);
            // The analytical restore supplies these exact rows before this component runs.
            // Deliberately do not populate the target artifact store from the source.
            assert!(target_catalog.complete(&completion)?.1);
            completions.push(completion);
        }
        let saved_at = Timestamp::from_unix_nanos(400);
        let saved = source_catalog.save(account, completions[0].calculation_token, saved_at)?;
        assert_eq!(
            target_catalog.save(account, completions[0].calculation_token, saved_at)?,
            saved
        );
        let head = source_catalog.head()?;
        assert_eq!(head.completions.sequence, 2);
        assert_eq!(head.saves.sequence, 1);
        let portfolios = b"{\"pairedPortfolio\":\"original snapshot\"}";

        // Catalog rows alone cannot produce a successful backup when either payload is lost.
        let incomplete = RetainedPlanning::retain(
            target_catalog.clone(),
            Arc::clone(&target),
            Arc::clone(&target_artifacts),
            [7; 32],
            &cancellation,
        )
        .await?;
        assert!(matches!(
            incomplete
                .write(portfolios, &mut Vec::new(), &cancellation)
                .await,
            Err(ProductBackupError::ArtifactMismatch)
        ));
        drop(incomplete);

        let retained = RetainedPlanning::retain(
            source_catalog.clone(),
            Arc::clone(&source),
            Arc::clone(&source_artifacts),
            [7; 32],
            &cancellation,
        )
        .await?;
        let mut stream = Vec::new();
        retained
            .write(portfolios, &mut stream, &cancellation)
            .await?;
        let (restored_portfolios, restored_head) = restore(
            &mut stream.as_slice(),
            &target_catalog,
            &target,
            target_artifacts.as_ref(),
            1024 * 1024,
            artifact_limit()?,
            &cancellation,
        )
        .await?;
        assert_eq!(restored_portfolios, portfolios);
        assert_eq!(restored_head, head);

        // Both classes must remain byte-exact after every target owner has been reopened.
        target.begin_owned_io_shutdown();
        target.finish_owned_io_shutdown(deadline()?).await?;
        drop((target_catalog, target_artifacts, target));
        let (target, target_artifacts) = owners(&target_paths, true)?;
        let target_catalog = target.analytical_service().portfolio_planning();
        assert_eq!(target_catalog.head()?, head);
        assert_eq!(
            target_catalog.saved(account, completions[0].calculation_token)?,
            Some(saved)
        );
        assert!(
            target_catalog
                .saved(account, completions[1].calculation_token)?
                .is_none()
        );
        for (completion, original) in completions.iter().zip(&originals) {
            let read = target_artifacts
                .read(
                    ArtifactReadRequest::try_new(
                        artifact_reference(completion)?,
                        artifact_limit()?,
                    )?,
                    ArtifactReadContext::new(cancellation.clone(), deadline()?),
                )
                .await?;
            assert_eq!(read.content(), original);
            let offset = stream
                .windows(original.len())
                .position(|bytes| bytes == original)
                .ok_or("original artifact missing from emitted stream")?;
            let mut corrupt = stream.clone();
            corrupt[offset] ^= 1;
            assert!(matches!(
                restore(
                    &mut corrupt.as_slice(),
                    &target_catalog,
                    &target,
                    target_artifacts.as_ref(),
                    1024 * 1024,
                    artifact_limit()?,
                    &cancellation,
                )
                .await,
                Err(ProductBackupError::ArtifactMismatch)
            ));
        }
        assert!(matches!(
            restore(
                &mut &stream[..stream.len() - 1],
                &target_catalog,
                &target,
                target_artifacts.as_ref(),
                1024 * 1024,
                artifact_limit()?,
                &cancellation,
            )
            .await,
            Err(ProductBackupError::ArtifactMismatch)
        ));
        target.begin_owned_io_shutdown();
        target.finish_owned_io_shutdown(deadline()?).await?;
        source.begin_owned_io_shutdown();
        source.finish_owned_io_shutdown(deadline()?).await?;
        Ok(())
    }
}
