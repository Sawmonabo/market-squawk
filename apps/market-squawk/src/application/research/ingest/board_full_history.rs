//! Exact installed Board complete-file acquisition on the existing provider and I/O owners.

use super::*;
use market_squawk_adapter_federal_reserve::{
    BoardDatasetProfile, BoardFullHistoryError, BoardFullHistoryOriginal,
};
use market_squawk_data::{BoardFullHistoryPublicationInput, BoardFullHistoryPublicationReference};
use market_squawk_domain::DigestAlgorithm;
use market_squawk_platform::{
    ResearchObjectControl, ResearchObjectControlError, ResearchObjectControlPoint,
};
use sha2::{Digest as _, Sha256};

const BOARD_PROFILE: &str = "federal-reserve-board.data-download-program";

/// Actual first source/acquisition failure is preserved through application preparation.
#[derive(Debug, Error)]
pub(crate) enum BoardFullHistoryApplicationError {
    #[error(transparent)]
    Service(#[from] ServiceError),
    #[error(transparent)]
    Composition(#[from] ResearchIngestCompositionError),
    #[error(transparent)]
    Source(#[from] BoardFullHistoryError),
    #[error(transparent)]
    Data(#[from] IngestError),
    #[error(transparent)]
    Research(#[from] ResearchServiceError),
    #[error("Board full-history original source differs from the admitted installed generation")]
    OriginalMismatch,
}

impl ProductionResearchIngestCoordinator {
    /// Acquires the genuine installed source, retaining its original file before bounded
    /// partition normalization. A pending exact original is physically reopened before any new
    /// network request. Actual publication time must precede any subsequent analytical cutoff.
    pub(crate) async fn prepare_default_h15_full_history(
        &self,
        context: &RequestContext,
    ) -> Result<BoardFullHistoryPublicationReference, BoardFullHistoryApplicationError> {
        let profile = BoardDatasetProfile::h15_treasury_constant_maturities_full_history()
            .map_err(BoardFullHistoryError::from)?;
        let profile_id =
            SourceIdentifier::try_from(BOARD_PROFILE).map_err(|_| ServiceError::InvalidRequest)?;
        // This is an actual installed-source lookup, not a placeholder unavailable result.
        let generation = self
            .provider_runtime_generation(&profile_id)?
            .ok_or(ServiceError::NotFound)?;
        let (common, capability) = self
            .acquire_provider_macro_operation_with_registered_capability(
                &generation,
                profile.dataset(),
                context,
                operation_deadline(context, Duration::from_secs(60 * 60))?,
            )
            .await?;
        let RegisteredTypedSourceCapability::BoardFullHistory(source) = capability else {
            return Err(ServiceError::Unavailable.into());
        };
        common.ensure_live()?;
        let dataset = DatasetId::try_from(profile.analytical_dataset().as_str())
            .map_err(|_| ServiceError::InvalidRequest)?;
        let metadata = generation.metadata().clone();
        let revision = board_metadata_digest(&metadata)?;
        let native_schema = BoardFullHistoryOriginal::native_partition_schema_digest();
        let deadline = common.operation_deadline();
        let cancellation = common.cancellation();
        let data = self.research.analytical_service();
        let source_id = metadata.source_id().clone();
        let requested_dataset = dataset.clone();
        let pending = self
            .research
            .run_owned_research_io(deadline, cancellation, move |worker| {
                data.provider_logical_original(
                    &requested_dataset,
                    &source_id,
                    native_schema,
                    revision,
                    None,
                    deadline,
                    &worker,
                )
            })
            .await??;
        common.ensure_live()?;
        let (staging, original) = match pending {
            Some(receipt) => {
                let staging = self
                    .research
                    .analytical_service()
                    .begin_board_full_history_staging(deadline, cancellation)
                    .await?;
                let store = self.research.provider_capture_store();
                let bytes = receipt.checkpoint_bytes().to_vec();
                let digest = receipt.original_digest();
                self.research
                    .run_owned_research_io(deadline, cancellation, move |worker| {
                        let control = BoardIoControl {
                            deadline,
                            cancellation: worker,
                        };
                        let original =
                            BoardFullHistoryOriginal::reopen_checkpoint(&bytes, &store, &control)?;
                        if original.original_digest() != digest {
                            return Err(BoardFullHistoryError::InvalidEvidence);
                        }
                        Ok::<_, BoardFullHistoryError>((staging, original))
                    })
                    .await??
            }
            None => {
                let pending = source
                    .retrieve_h15_full_history(
                        &common.extraction(),
                        common.provider_deadline()?,
                        cancellation,
                    )
                    .await?;
                common.ensure_live()?;
                let staging = self
                    .research
                    .analytical_service()
                    .begin_board_full_history_staging(deadline, cancellation)
                    .await?;
                let store = self.research.provider_capture_store();
                self.research
                    .run_owned_research_io(deadline, cancellation, move |worker| {
                        let original = pending.seal_original(
                            &store,
                            &BoardIoControl {
                                deadline,
                                cancellation: worker,
                            },
                        )?;
                        Ok::<_, BoardFullHistoryError>((staging, original))
                    })
                    .await??
            }
        };
        common.ensure_live()?;
        if original.metadata() != &metadata {
            return Err(BoardFullHistoryApplicationError::OriginalMismatch);
        }
        let rights = common.rights_decision(original.original_digest(), system_timestamp()?)?;
        let original_digest = original.original_digest();
        let data = self.research.analytical_service();
        let store = self.research.provider_capture_store();
        let requested_dataset = dataset.clone();
        let schema = market_squawk_data::DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| BoardFullHistoryApplicationError::OriginalMismatch)?;
        let canonical_schema = EvidenceDigest::new(DigestAlgorithm::Sha256, schema.fingerprint());
        let (staging, input) = self
            .research
            .run_owned_research_io(deadline, cancellation, move |worker| {
                let control = BoardIoControl {
                    deadline,
                    cancellation: worker.clone(),
                };
                let checkpoint = original.checkpoint_bytes()?;
                let receipt = data.retain_provider_logical_original(
                    original.metadata(),
                    &requested_dataset,
                    original.native_schema_digest(),
                    original_digest,
                    original.received_at(),
                    &checkpoint,
                    original.objects(),
                    &rights,
                    &store,
                    deadline,
                    &worker,
                )?;
                if receipt.original_digest() != original_digest
                    || receipt.checkpoint_bytes() != checkpoint.as_ref()
                {
                    return Err(BoardFullHistoryApplicationError::OriginalMismatch);
                }
                // Original raw+transport claims are now durable in the same existing catalog.
                let prepared = original.prepare(canonical_schema, &store, &control)?;
                let input = BoardFullHistoryPublicationInput::try_from_source(prepared)?;
                Ok::<_, BoardFullHistoryApplicationError>((staging, input))
            })
            .await??;
        common.ensure_live()?;
        if input.dataset() != &dataset || input.metadata() != &metadata {
            return Err(BoardFullHistoryApplicationError::OriginalMismatch);
        }
        let payload = input.publication_digest();
        let observed_at = system_timestamp()?;
        let rights = common.rights_decision(payload, observed_at)?;
        let reserved = self
            .research
            .run_owned_research_io(deadline, cancellation, move |worker| {
                staging.reserve_publication(input, rights, deadline, &worker)
            })
            .await??;
        common.ensure_live()?;
        let mut publication = reserved.begin(deadline, cancellation).await?;
        let precommit = common.publication_authority();
        let (returned, prior) = self
            .research
            .run_owned_research_io(deadline, cancellation, move |worker| {
                let prior = publication.validate(deadline, &worker, precommit.as_ref())?;
                Ok::<_, IngestError>((publication, prior))
            })
            .await??;
        publication = returned;
        common.ensure_live()?;
        if let Some(prior) = prior {
            return Ok(prior);
        }
        loop {
            let (returned, next) = self
                .research
                .run_owned_research_io(deadline, cancellation, move |worker| {
                    let next = publication.next_partition(deadline, &worker)?;
                    Ok::<_, IngestError>((publication, next))
                })
                .await??;
            publication = returned;
            common.ensure_live()?;
            let Some(next) = next else {
                break;
            };
            let assigned = publication
                .assign_partition(next, deadline, cancellation.clone())
                .await?;
            common.ensure_live()?;
            let arrow = self
                .research
                .run_owned_research_io(deadline, cancellation, move |worker| {
                    assigned.convert(deadline, &worker)
                })
                .await??;
            common.ensure_live()?;
            publication
                .stage_partition(arrow, deadline, cancellation)
                .await?;
            common.ensure_live()?;
        }
        let precommit = common.publication_authority();
        let publication = self
            .research
            .run_owned_research_io(deadline, cancellation, move |worker| {
                publication.commit(deadline, &worker, precommit.as_ref())
            })
            .await??;
        common.ensure_live()?;
        if publication.original_digest() != original_digest
            || publication.manifest().dataset_id() != &dataset
        {
            return Err(BoardFullHistoryApplicationError::OriginalMismatch);
        }
        Ok(publication)
    }
}

fn board_metadata_digest(metadata: &SourceMetadata) -> Result<EvidenceDigest, ServiceError> {
    let bytes = serde_json::to_vec(metadata).map_err(|_| ServiceError::InvalidResult)?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(&bytes).into(),
    ))
}
struct BoardIoControl {
    deadline: Instant,
    cancellation: CancellationToken,
}
impl ResearchObjectControl for BoardIoControl {
    fn checkpoint(&self, _: ResearchObjectControlPoint) -> Result<(), ResearchObjectControlError> {
        if self.cancellation.is_cancelled() {
            Err(ResearchObjectControlError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(ResearchObjectControlError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}
