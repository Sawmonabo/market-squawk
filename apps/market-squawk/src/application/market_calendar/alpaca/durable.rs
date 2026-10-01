//! Independent calendar capture, canonical publication, and exact retained completed-session read.

use std::{sync::Arc, time::Instant};

use market_squawk_adapter_alpaca::{AlpacaAuthenticatedCalendarRequest, AlpacaCalendarMarket};
use market_squawk_data::{DatasetId, DatasetManifestRef, extraction_provider_payload_digest};
use market_squawk_domain::{CalendarDate, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_sources::{ExtractionRevisionPlan, ProviderCaptureTerminalDisposition};
use tokio_util::sync::CancellationToken;

use super::{AlpacaCompletedSessionEvidence, publication};
use crate::application::market_calendar::{
    CompletedMarketSessionError, MarketCalendarClock, SystemMarketCalendarClock,
};
use crate::application::market_runtime::{
    AlpacaHistoricalCalendarError, AlpacaHistoricalCapabilityError,
    AlpacaHistoricalRuntimeCapability,
};
use crate::{ResearchIngestRequest, ResearchService, ResearchServiceError};

/// Secret-free coordinates of one actual committed calendar publication. Retained reads verify
/// these coordinates against the creating generation; this value is not itself use authority.
#[derive(Clone, Debug)]
pub(crate) struct PublishedAlpacaCalendar {
    manifest: DatasetManifestRef,
    request: AlpacaAuthenticatedCalendarRequest,
    binding_digest: EvidenceDigest,
}

impl PublishedAlpacaCalendar {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    pub(crate) const fn request(&self) -> &AlpacaAuthenticatedCalendarRequest {
        &self.request
    }
    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
}

/// Fetches and seals one exact range through the existing active account executor and budget,
/// then commits Coverage and all returned SessionDay siblings in one canonical publication.
/// Workflow callers invoke this once before choosing their analysis knowledge cutoff.
pub(crate) async fn publish_alpaca_market_calendar(
    service: &Arc<ResearchService>,
    runtime: &AlpacaHistoricalRuntimeCapability,
    market: AlpacaCalendarMarket,
    start_date: CalendarDate,
    end_date: CalendarDate,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<PublishedAlpacaCalendar, CompletedMarketSessionError> {
    publish_alpaca_market_calendar_with_job_context(
        service,
        runtime,
        market,
        start_date,
        end_date,
        deadline,
        cancellation,
        None,
    )
    .await
}

pub(crate) async fn publish_alpaca_market_calendar_with_job_context(
    service: &Arc<ResearchService>,
    runtime: &AlpacaHistoricalRuntimeCapability,
    market: AlpacaCalendarMarket,
    start_date: CalendarDate,
    end_date: CalendarDate,
    deadline: Instant,
    cancellation: &CancellationToken,
    job: Option<&market_squawk_jobs::JobRunContext>,
) -> Result<PublishedAlpacaCalendar, CompletedMarketSessionError> {
    ensure_before(deadline, cancellation)?;
    let response = runtime
        .fetch_market_calendar_range(market, start_date, end_date, deadline, cancellation)
        .await
        .inspect_err(|error| {
            tracing::warn!(%error, stage = "calendar_fetch", "calendar publication unavailable");
        })
        .map_err(map_calendar_error)?;
    let metadata = runtime.calendar_metadata();
    let dataset_name = format!(
        "alpaca-market-calendar-{}",
        market.request_code().to_ascii_lowercase()
    );
    let dataset = SourceIdentifier::try_from(dataset_name.as_str()).map_err(invalid)?;
    let analytical_dataset = DatasetId::try_from(dataset_name.as_str()).map_err(invalid)?;
    let material = response
        .provider_capture_material(
            metadata.source_id().clone(),
            metadata.revision().clone(),
            dataset,
        )
        .map_err(invalid)?;
    let (expectation, seal_request) = material.into_whole_seal_parts();
    let sealed = match job {
        Some(job) => {
            service
                .seal_provider_capture_for_job(job, seal_request, cancellation, deadline)
                .await
        }
        None => {
            service
                .seal_provider_capture(seal_request, cancellation, deadline)
                .await
        }
    }
    .inspect_err(|error| {
        tracing::warn!(kind = ?std::mem::discriminant(error), stage = "calendar_seal", "calendar publication unavailable");
    })
    .map_err(map_research_error)?;
    let token = expectation
        .try_rejoin(sealed)
        .map_err(invalid)?
        .try_into_whole()
        .map_err(invalid)?;
    let binding = publication::try_bind_alpaca_calendar_publication(
        &response,
        token,
        deadline,
        cancellation,
    )?;
    let binding_digest = binding.evidence_digest().evidence();
    let revisions =
        ExtractionRevisionPlan::locally_observed_with_native_lineage(binding.record_count())
            .map_err(invalid)?;
    let now = SystemMarketCalendarClock.now().map_err(invalid)?;
    let rights = runtime
        .calendar_rights()
        .decision(extraction_provider_payload_digest(binding.batch()), now)
        .map_err(|_| operation_error(deadline, cancellation))?;
    let request = ResearchIngestRequest::with_provider_publication(
        metadata.clone(),
        rights,
        analytical_dataset,
        binding,
        revisions,
    )
    .map_err(invalid)?;
    // No network or ordinary runtime callback runs while this existing account mutation guard
    // is retained. The borrowed catalog hook validates the same authority at durable commit.
    let guard = runtime
        .acquire_calendar_publication_authority(deadline, cancellation)
        .await
        .map_err(map_capability_error)?;
    let request = request.with_precommit_authority(Arc::clone(&guard));
    let committed = match job {
        Some(job) => {
            service
                .ingest_for_job(job, request, cancellation.clone(), deadline)
                .await
        }
        None => service.ingest(request, cancellation.clone()).await,
    };
    drop(guard);
    let committed = committed.map_err(map_research_error)?;
    runtime
        .require_current(deadline, cancellation)
        .await
        .map_err(map_capability_error)?;
    Ok(PublishedAlpacaCalendar {
        manifest: committed.manifest().clone(),
        request: response.request().clone(),
        binding_digest,
    })
}

/// Reopens the exact creating generation and complete source-native sibling set. The original
/// publication clock must precede the caller's frozen cutoff. IEX daily aggregation semantics
/// stay source-specific; another market's schedule cannot establish Schwab candle timestamps.
pub(crate) async fn read_alpaca_completed_calendar(
    service: &Arc<ResearchService>,
    runtime: AlpacaHistoricalRuntimeCapability,
    manifest: DatasetManifestRef,
    request: AlpacaAuthenticatedCalendarRequest,
    binding_digest: EvidenceDigest,
    observations: Box<[market_squawk_domain::MarketCalendarObservation]>,
    knowledge_cutoff: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Arc<AlpacaCompletedSessionEvidence>, CompletedMarketSessionError> {
    read_alpaca_completed_calendar_with_job_context(
        service,
        runtime,
        manifest,
        request,
        binding_digest,
        observations,
        knowledge_cutoff,
        deadline,
        cancellation,
        None,
    )
    .await
}

pub(crate) async fn read_alpaca_completed_calendar_with_job_context(
    service: &Arc<ResearchService>,
    runtime: AlpacaHistoricalRuntimeCapability,
    manifest: DatasetManifestRef,
    request: AlpacaAuthenticatedCalendarRequest,
    binding_digest: EvidenceDigest,
    observations: Box<[market_squawk_domain::MarketCalendarObservation]>,
    knowledge_cutoff: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
    job: Option<&market_squawk_jobs::JobRunContext>,
) -> Result<Arc<AlpacaCompletedSessionEvidence>, CompletedMarketSessionError> {
    ensure_before(deadline, cancellation)?;
    runtime
        .require_current(deadline, cancellation)
        .await
        .inspect_err(|error| {
            tracing::warn!(
                ?error,
                stage = "calendar-native-read-currentness-before",
                "completed market calendar replay unavailable"
            );
        })
        .map_err(map_capability_error)?;
    // Match publication's activation-before-worker ordering. The same account guard validates
    // replay without treating concurrent monitor reads as a stale lease. It stays local to this
    // operation and is released before the ordinary final currentness check.
    let replay_currentness = runtime
        .acquire_calendar_publication_authority(deadline, cancellation)
        .await
        .map_err(map_capability_error)?;
    let worker_currentness = Arc::clone(&replay_currentness);
    let operation_cancellation = cancellation.child_token();
    let _cancel_on_drop = operation_cancellation.clone().drop_guard();
    let read_runtime = runtime.clone();
    let expected_manifest = manifest.clone();
    let evidence = service
        .read_provider_capture_generation_with_job_context(
            job,
            manifest,
            deadline,
            &operation_cancellation,
            move |generation, store, control, analytical, read_cancellation| {
                if generation.pinned().manifest() != &expected_manifest
                    || generation.source_id() != read_runtime.calendar_metadata().source_id()
                    || generation.published_at() > knowledge_cutoff
                    || generation.objects().len() != 1
                {
                    return Err(ResearchServiceError::IngestAuthorityMismatch);
                }
                let inputs = generation.objects()[0].inputs();
                if inputs.len() != 1 {
                    return Err(ResearchServiceError::IngestAuthorityMismatch);
                }
                let binding = inputs[0].binding();
                let capture = binding.capture();
                if binding.binding_digest() != binding_digest
                    || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
                    || capture.pages().len() != 1
                    || binding.physical_claims().len() != 1
                    || capture.request_set_identity()
                        != request
                            .capture_request_identity()
                            .map_err(|_| ResearchServiceError::IngestAuthorityMismatch)?
                    || capture.pages()[0].received_at() > generation.published_at()
                {
                    return Err(ResearchServiceError::IngestAuthorityMismatch);
                }
                let metadata = analytical
                    .retained_source_metadata(
                        capture.source_id(),
                        capture.metadata_revision(),
                        knowledge_cutoff,
                        deadline,
                        read_cancellation,
                    )?
                    .ok_or(ResearchServiceError::IngestAuthorityMismatch)?;
                let segment = store.open_verified_claim_with_control(
                    binding.physical_claims()[0].claim(),
                    control,
                )?;
                if segment.records().len() != 1 {
                    return Err(ResearchServiceError::IngestAuthorityMismatch);
                }
                if let Err(error) = publication::validate_retained_calendar_native_rows(
                    &request,
                    segment.records()[0].payload(),
                    binding,
                    &observations,
                    deadline,
                    read_cancellation,
                ) {
                    return Ok(Err(error));
                }
                Ok(
                    AlpacaCompletedSessionEvidence::try_from_published_calendar_capture(
                        read_runtime,
                        &request,
                        capture.clone(),
                        &segment,
                        generation.published_at(),
                        metadata,
                        worker_currentness.as_ref(),
                        deadline,
                        read_cancellation,
                    ),
                )
            },
        )
        .await
        .inspect_err(|error| {
            use market_squawk_data::{CatalogError, IngestError};
            use market_squawk_platform::SealedResearchJournalStoreError as StoreError;
            let failure = match error {
                ResearchServiceError::Ingest(IngestError::AuthorityBusy)
                | ResearchServiceError::Ingest(IngestError::Catalog(CatalogError::AuthorityBusy))
                | ResearchServiceError::Catalog(CatalogError::AuthorityBusy) => "catalog-busy",
                ResearchServiceError::Ingest(IngestError::AuthorityLockPoisoned) => {
                    "catalog-lock-poisoned"
                }
                ResearchServiceError::Ingest(IngestError::Cancelled) => "cancelled",
                ResearchServiceError::Ingest(IngestError::DeadlineExceeded) => "deadline-exceeded",
                ResearchServiceError::Ingest(IngestError::ProviderCaptureRequired) => {
                    "capture-evidence-mismatch"
                }
                ResearchServiceError::Ingest(IngestError::Manifest(_))
                | ResearchServiceError::Manifest(_) => "manifest-error",
                ResearchServiceError::Ingest(IngestError::Catalog(_))
                | ResearchServiceError::Catalog(_) => "catalog-error",
                ResearchServiceError::Ingest(IngestError::SealedProviderCapture(
                    StoreError::Io { .. },
                ))
                | ResearchServiceError::ProviderCaptureStore(StoreError::Io { .. }) => {
                    "capture-store-io"
                }
                ResearchServiceError::Ingest(IngestError::SealedProviderCapture(_))
                | ResearchServiceError::ProviderCaptureStore(_) => "capture-store-error",
                ResearchServiceError::Ingest(_) => "ingest-error",
                ResearchServiceError::IngestAuthorityMismatch => "ingest-authority-mismatch",
                ResearchServiceError::ProviderCaptureSealWorkerUnavailable => "worker-unavailable",
                ResearchServiceError::Path(_) => "path-unavailable",
                _ => "research-error",
            };
            tracing::warn!(
                stage = "calendar-native-read-worker",
                failure,
                "completed market calendar replay unavailable"
            );
        })
        .map_err(map_research_error)??;
    drop(replay_currentness);
    runtime
        .require_current(deadline, cancellation)
        .await
        .inspect_err(|error| {
            tracing::warn!(
                ?error,
                stage = "calendar-native-read-currentness-after",
                "completed market calendar replay unavailable"
            );
        })
        .map_err(map_capability_error)?;
    Ok(Arc::new(evidence))
}

/// Replays an original independently published calendar without consulting today's account.
/// The result is immutable native evidence only, never CompletedSessionEvidence or currentness.
pub(crate) async fn read_alpaca_retained_calendar_with_job_context(
    service: &Arc<ResearchService>,
    manifest: DatasetManifestRef,
    request: AlpacaAuthenticatedCalendarRequest,
    binding_digest: EvidenceDigest,
    observations: Box<[market_squawk_domain::MarketCalendarObservation]>,
    knowledge_cutoff: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
    job: Option<&market_squawk_jobs::JobRunContext>,
) -> Result<
    Arc<market_squawk_adapter_alpaca::AlpacaRetainedCalendarSessions>,
    CompletedMarketSessionError,
> {
    use market_squawk_adapter_alpaca::{
        AlpacaRetainedCalendarSessions, AlpacaTradingApiEnvironment,
    };
    use market_squawk_sources::{NetworkAccessPolicy, SealedProviderCaptureSetReceipt};
    ensure_before(deadline, cancellation)?;
    let expected_manifest = manifest.clone();
    let operation_cancellation = cancellation.child_token();
    let _cancel_on_drop = operation_cancellation.clone().drop_guard();
    let replay = service.read_provider_capture_generation_with_job_context(
        job, manifest, deadline, &operation_cancellation,
        move |generation, store, control, analytical, read_cancellation| {
            let invalid = || ResearchServiceError::IngestAuthorityMismatch;
            if generation.pinned().manifest() != &expected_manifest
                || generation.published_at() > knowledge_cutoff
                || generation.objects().len() != 1
                || generation.objects()[0].inputs().len() != 1
            { return Err(invalid()); }
            let binding = generation.objects()[0].inputs()[0].binding();
            let capture = binding.capture();
            if binding.binding_digest() != binding_digest
                || binding.native_lineage().implementation() != "alpaca_calendar_v1"
                || binding.scope() != "whole"
                || binding.layout() != "whole_single_segment"
                || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
                || capture.pages().len() != 1
                || binding.physical_claims().len() != 1
                || generation.source_id() != capture.source_id()
                || capture.request_set_identity() != request.capture_request_identity().map_err(|_| invalid())?
                || capture.pages()[0].received_at() > generation.published_at()
                || capture.dataset().as_str() != format!("alpaca-market-calendar-{}", request.market().request_code().to_ascii_lowercase())
            { return Err(invalid()); }
            let metadata = analytical.retained_source_metadata(
                capture.source_id(), capture.metadata_revision(), knowledge_cutoff, deadline, read_cancellation,
            )?.ok_or_else(invalid)?;
            let NetworkAccessPolicy::Allowlisted(policy) = metadata.network_policy() else { return Err(invalid()); };
            let environment = [AlpacaTradingApiEnvironment::Live, AlpacaTradingApiEnvironment::Paper]
                .into_iter().find(|environment| environment.origin() == request.origin()).ok_or_else(invalid)?;
            if metadata.source_id() != capture.source_id()
                || metadata.revision() != capture.metadata_revision()
                || !metadata.is_effective_at(capture.pages()[0].received_at())
                || market_squawk_adapter_alpaca::validate_alpaca_calendar_metadata(
                    &metadata, policy.request_bounds(), environment,
                ).is_err()
            { return Err(invalid()); }
            let segment = store.open_verified_claim_with_control(binding.physical_claims()[0].claim(), control)?;
            if segment.records().len() != 1 { return Err(invalid()); }
            if let Err(error) = publication::validate_retained_calendar_native_rows(
                &request, segment.records()[0].payload(), binding, &observations, deadline, read_cancellation,
            ) { return Ok(Err(error)); }
            let sealed = SealedProviderCaptureSetReceipt::try_bind(capture.clone(), segment.receipt().clone())
                .map_err(|_| invalid())?;
            if sealed.receipt_digest() != binding.sealed_capture_receipt_digest() { return Err(invalid()); }
            Ok(AlpacaRetainedCalendarSessions::try_replay(&request, &sealed, &segment, control)
                .map(Arc::new).map_err(|error| match error {
                    market_squawk_adapter_alpaca::AlpacaCalendarDecodeError::Control(
                        market_squawk_platform::ResearchObjectControlError::Cancelled,
                    ) => CompletedMarketSessionError::Cancelled,
                    market_squawk_adapter_alpaca::AlpacaCalendarDecodeError::Control(
                        market_squawk_platform::ResearchObjectControlError::DeadlineExceeded,
                    ) => CompletedMarketSessionError::DeadlineExceeded,
                    market_squawk_adapter_alpaca::AlpacaCalendarDecodeError::ResourceBoundExceeded => CompletedMarketSessionError::ResourceBoundExceeded,
                    _ => CompletedMarketSessionError::InvalidEvidence,
                }))
        },
    ).await.map_err(map_research_error)??;
    ensure_before(deadline, cancellation)?;
    Ok(replay)
}

fn ensure_before(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CompletedMarketSessionError> {
    if cancellation.is_cancelled() {
        Err(CompletedMarketSessionError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(CompletedMarketSessionError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn operation_error(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> CompletedMarketSessionError {
    ensure_before(deadline, cancellation)
        .err()
        .unwrap_or(CompletedMarketSessionError::Unavailable)
}
fn invalid<T>(_: T) -> CompletedMarketSessionError {
    CompletedMarketSessionError::InvalidEvidence
}

// Keep local worker/integrity failures fatal: this contract has no separate Internal variant.
fn map_research_error(error: ResearchServiceError) -> CompletedMarketSessionError {
    use market_squawk_data::IngestError;
    match error {
        ResearchServiceError::Ingest(IngestError::SealedProviderCapture(error))
        | ResearchServiceError::ProviderCaptureStore(error) => map_store_error(error),
        ResearchServiceError::Ingest(IngestError::ProviderCapture(error)) => match error {
            market_squawk_sources::ProviderCaptureError::PageLimitExceeded { .. }
            | market_squawk_sources::ProviderCaptureError::ByteLimitExceeded { .. }
            | market_squawk_sources::ProviderCaptureError::AllocationFailed => {
                CompletedMarketSessionError::ResourceBoundExceeded
            }
            _ => CompletedMarketSessionError::InvalidEvidence,
        },
        ResearchServiceError::Catalog(error) => map_ingest_error(IngestError::Catalog(error)),
        ResearchServiceError::Manifest(error) => map_ingest_error(IngestError::Manifest(error)),
        ResearchServiceError::Ingest(error) => map_ingest_error(error),
        ResearchServiceError::Path(_) => CompletedMarketSessionError::Unavailable,
        _ => CompletedMarketSessionError::InvalidEvidence,
    }
}

fn map_ingest_error(error: market_squawk_data::IngestError) -> CompletedMarketSessionError {
    use market_squawk_data::IngestError;
    use market_squawk_services::ServiceError;
    // Reuse the typed shared cases only; its fallback is intentionally not absence evidence.
    let error = match error {
        IngestError::Manifest(market_squawk_data::ManifestCatalogError::LockPoisoned)
        | IngestError::Catalog(market_squawk_data::CatalogError::WriterRegistryUnavailable)
        | IngestError::Parquet(
            market_squawk_data::ParquetStoreError::BlockingTaskFailed
            | market_squawk_data::ParquetStoreError::RootAuthorityRegistryUnavailable
            | market_squawk_data::ParquetStoreError::InvalidConfiguration
            | market_squawk_data::ParquetStoreError::InvalidPublicationLease
            | market_squawk_data::ParquetStoreError::InvalidStagedObject
            | market_squawk_data::ParquetStoreError::CatalogRestoreConflict
            | market_squawk_data::ParquetStoreError::Arrow(_)
            | market_squawk_data::ParquetStoreError::Parquet(_)
            | market_squawk_data::ParquetStoreError::ArtifactPath(_),
        ) => ServiceError::InvalidResult,
        error @ (IngestError::Cancelled
        | IngestError::DeadlineExceeded
        | IngestError::Parquet(_)
        | IngestError::Catalog(_)
        | IngestError::Manifest(_)
        | IngestError::ResearchUse(_)
        | IngestError::ListingReference(_)
        | IngestError::MarketDataInstrumentReference(_)
        | IngestError::ProviderMarketEventSelection(_)) => {
            crate::application::research::map_durable_market_ingest_error(error)
        }
        IngestError::AuthorityBusy | IngestError::PublicationAuthorityRevoked => {
            ServiceError::Unavailable
        }
        _ => ServiceError::InvalidResult,
    };
    match error {
        ServiceError::Cancelled => CompletedMarketSessionError::Cancelled,
        ServiceError::DeadlineExceeded => CompletedMarketSessionError::DeadlineExceeded,
        ServiceError::ResourceExhausted => CompletedMarketSessionError::ResourceBoundExceeded,
        ServiceError::Unavailable => CompletedMarketSessionError::Unavailable,
        _ => CompletedMarketSessionError::InvalidEvidence,
    }
}

fn map_store_error(
    error: market_squawk_platform::SealedResearchJournalStoreError,
) -> CompletedMarketSessionError {
    use market_squawk_platform::{
        ResearchObjectControlError as C, SealedResearchJournalStoreError as E,
    };
    match error {
        E::ObjectControl(C::Cancelled) => CompletedMarketSessionError::Cancelled,
        E::ObjectControl(C::DeadlineExceeded) => CompletedMarketSessionError::DeadlineExceeded,
        E::FrameLimitExceeded { .. }
        | E::ByteLimitExceeded { .. }
        | E::ObjectByteLimitExceeded { .. }
        | E::ObjectChunkLimitExceeded { .. }
        | E::ObjectAllocationFailed => CompletedMarketSessionError::ResourceBoundExceeded,
        E::Io { .. } | E::AlreadyOwned | E::ObjectStageActive => {
            CompletedMarketSessionError::Unavailable
        }
        _ => CompletedMarketSessionError::InvalidEvidence,
    }
}

fn map_capability_error(error: AlpacaHistoricalCapabilityError) -> CompletedMarketSessionError {
    match error {
        AlpacaHistoricalCapabilityError::Cancelled => CompletedMarketSessionError::Cancelled,
        AlpacaHistoricalCapabilityError::DeadlineExceeded => {
            CompletedMarketSessionError::DeadlineExceeded
        }
        AlpacaHistoricalCapabilityError::Revoked | AlpacaHistoricalCapabilityError::Stale => {
            CompletedMarketSessionError::Unavailable
        }
    }
}

fn map_calendar_error(error: AlpacaHistoricalCalendarError) -> CompletedMarketSessionError {
    use AlpacaHistoricalCalendarError as E;
    use market_squawk_adapter_alpaca::AlpacaError;
    match error {
        E::Capability(error) => map_capability_error(error),
        E::Adapter(AlpacaError::Cancelled) => CompletedMarketSessionError::Cancelled,
        E::Adapter(AlpacaError::DeadlineExceeded) => CompletedMarketSessionError::DeadlineExceeded,
        E::Adapter(AlpacaError::BodyTooLarge | AlpacaError::Allocation)
        | E::ResponseBoundExceeded
        | E::Allocation => CompletedMarketSessionError::ResourceBoundExceeded,
        E::Adapter(AlpacaError::Network)
        | E::BudgetUnavailable
        | E::RetryLimitExceeded
        | E::RangeHttpStatus(401 | 403 | 408 | 429 | 500..=599) => {
            CompletedMarketSessionError::Unavailable
        }
        _ => CompletedMarketSessionError::InvalidEvidence,
    }
}
