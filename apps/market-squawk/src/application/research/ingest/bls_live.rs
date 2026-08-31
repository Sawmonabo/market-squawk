//! BLS acquisition, durable macro publication, and exact PIT handoff.
//!
//! This leaf pairs one concrete [`BlsSource`] with the generic source value published into the
//! application registry. Production acquisition therefore uses the registry-minted extraction
//! authority backed by the sole shared provider-rate authority while retaining the concrete BLS
//! methods needed for doctor activation and seal-first whole-plan extraction. The adapter owns
//! request-plan/cache identity and repeated-response extraction; this boundary adds no cache,
//! counter, raw store, or provider request.

use std::{
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Instant,
};

use futures_util::future::BoxFuture;
use market_squawk_adapter_bls::{BlsAccessTier, BlsCredentialRejoin, BlsSource, BlsSourceError};
use market_squawk_data::{DatasetId, IngestError, IngestIdentity, QueryLimits, SourceOperation};
use market_squawk_domain::{EvidenceDigest, ResearchPeriod, SourceIdentifier, Timestamp};
use market_squawk_services::{RequestContext, ServiceError};
use market_squawk_sources::{
    DiscoveryRequest, ExtractionAuthority, ExtractionBatch, ExtractionRevisionPlan,
    ExtractionSource, ExtractionSourceError, SourceMetadata, SourceMetadataProvider,
};
use sha2::Digest as _;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::bls::BlsSelectedRowEvidenceJoin;
#[cfg(all(test, feature = "bls-installed-fixture", debug_assertions))]
use super::bls::{
    BlsProviderPeriodObservationSemanticEvidence, BlsSelectedRowEvidenceBatch,
    BlsSelectedRowEvidenceJoinError, BlsSelectedRowEvidenceJoinReceipt,
    BlsSelectedRowEvidenceLimits, BlsSelectedRowNativeEvidence,
};
use super::{
    BlsMacroApplicationClosure, BlsMacroApplicationError, BlsMacroCapabilityState,
    BlsMacroPlanPublication, BlsMacroUnavailableReason, BlsPreparedMacroPlan,
    BlsProviderPeriodLatestKnownDto, BlsProviderPeriodLatestKnownRequest,
    BlsSealFirstExtractionLimits, ManagedResearchExtractionSource,
    ProductionResearchIngestCoordinator, ProviderMacroOperationAuthority,
    ResearchIngestCompositionError, ResearchProviderRuntimeGeneration, ResearchRevisionPlanError,
};

/// One exact BLS publication and fixed provider-period read request.
#[derive(Clone, Debug)]
pub(crate) struct BlsLiveRequest {
    doctor_deadline: Timestamp,
    acquisition_deadline: Timestamp,
    seal_deadline: Instant,
    maximum_records_per_chunk: NonZeroU32,
    maximum_canonical_bytes_per_chunk: NonZeroU64,
    series_allowlist: market_squawk_data::AnalyticalMacroSeriesAllowlist,
    knowledge_cutoff: Timestamp,
    effective_period_cutoff: ResearchPeriod,
    query_limits: QueryLimits,
    query_deadline: Instant,
}

impl BlsLiveRequest {
    /// Retains the independent provider, sealing, extraction, and typed-read bounds.
    #[allow(
        clippy::too_many_arguments,
        reason = "provider deadlines, physical sealing, extraction ceilings, and PIT cutoffs are independent"
    )]
    pub(crate) fn new(
        doctor_deadline: Timestamp,
        acquisition_deadline: Timestamp,
        seal_deadline: Instant,
        maximum_records_per_chunk: NonZeroU32,
        maximum_canonical_bytes_per_chunk: NonZeroU64,
        series_allowlist: market_squawk_data::AnalyticalMacroSeriesAllowlist,
        knowledge_cutoff: Timestamp,
        effective_period_cutoff: ResearchPeriod,
        query_limits: QueryLimits,
        query_deadline: Instant,
    ) -> Self {
        Self {
            doctor_deadline,
            acquisition_deadline,
            seal_deadline,
            maximum_records_per_chunk,
            maximum_canonical_bytes_per_chunk,
            series_allowlist,
            knowledge_cutoff,
            effective_period_cutoff,
            query_limits,
            query_deadline,
        }
    }
}

/// Paired generic-registration and concrete-production values for one exact BLS generation.
///
/// The pair shares one source instance. Activation registers [`Self::live_source`] through
/// the ordinary provider runtime mutation authority and retains [`Self::runtime`] in application
/// composition. Constructing another provider-rate authority or BLS cache is neither required nor
/// possible through this capability.
pub(crate) struct BlsLiveComposition {
    live_source: BlsLiveSource,
    runtime: BlsLiveRuntime,
}

impl BlsLiveComposition {
    /// Binds an exact public-v1 or registered-v2 source to its current runtime generation and
    /// application closure before either half is published.
    pub(crate) fn try_new(
        coordinator: Arc<ProductionResearchIngestCoordinator>,
        source: BlsSource,
        generation: ResearchProviderRuntimeGeneration,
    ) -> Result<Self, BlsLivePublicationError> {
        Self::try_new_inner(coordinator, source, generation, None)
    }

    /// Binds the root-owned selected-row evidence capability into this exact BLS runtime.
    pub(super) fn try_new_with_selected_row_evidence(
        coordinator: Arc<ProductionResearchIngestCoordinator>,
        source: BlsSource,
        generation: ResearchProviderRuntimeGeneration,
        selected_row_evidence: Arc<dyn BlsSelectedRowEvidenceJoin>,
    ) -> Result<Self, BlsLivePublicationError> {
        Self::try_new_inner(coordinator, source, generation, Some(selected_row_evidence))
    }

    fn try_new_inner(
        coordinator: Arc<ProductionResearchIngestCoordinator>,
        source: BlsSource,
        generation: ResearchProviderRuntimeGeneration,
        selected_row_evidence: Option<Arc<dyn BlsSelectedRowEvidenceJoin>>,
    ) -> Result<Self, BlsLivePublicationError> {
        let source = Arc::new(source);
        validate_source_generation(source.as_ref(), &generation)?;
        let closure = match selected_row_evidence {
            Some(selected_row_evidence) => BlsMacroApplicationClosure::with_selected_row_evidence(
                Arc::clone(&coordinator.research),
                selected_row_evidence,
            ),
            None => BlsMacroApplicationClosure::new(Arc::clone(&coordinator.research)),
        };
        Ok(Self {
            live_source: BlsLiveSource {
                source: Arc::clone(&source),
            },
            runtime: BlsLiveRuntime {
                coordinator,
                closure,
                source,
                generation,
            },
        })
    }

    /// Separates the generic registration value from its exact concrete production runtime.
    pub(crate) fn into_parts(self) -> (BlsLiveSource, BlsLiveRuntime) {
        (self.live_source, self.runtime)
    }
}

impl std::fmt::Debug for BlsLiveComposition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsLiveComposition")
            .field("source", &self.live_source)
            .field("runtime", &self.runtime)
            .finish()
    }
}

/// Registry-facing wrapper sharing the exact concrete source with the typed BLS runtime.
pub(crate) struct BlsLiveSource {
    source: Arc<BlsSource>,
}

impl std::fmt::Debug for BlsLiveSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsLiveSource")
            .field("source", self.source.as_ref())
            .finish()
    }
}

impl SourceMetadataProvider for BlsLiveSource {
    fn metadata(&self) -> &SourceMetadata {
        self.source.metadata()
    }
}

impl ExtractionSource for BlsLiveSource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<market_squawk_sources::DiscoveryBatch, ExtractionSourceError>> {
        self.source.discover(authority, request, cancellation)
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: market_squawk_sources::ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        self.source.extract(authority, request, cancellation)
    }
}

impl ManagedResearchExtractionSource for BlsLiveSource {
    fn discovery_dataset_identifier(&self) -> Option<&SourceIdentifier> {
        Some(self.source.dataset())
    }

    fn rights_subject(
        &self,
        dataset: &SourceIdentifier,
    ) -> Result<Option<SourceIdentifier>, ResearchRevisionPlanError> {
        if dataset != self.source.dataset() {
            return Err(ResearchRevisionPlanError);
        }
        self.source
            .activation_plan()
            .map(|plan| plan.rate().authorization_subject().cloned())
            .map_err(|_error| ResearchRevisionPlanError)
    }

    fn analytical_dataset(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<DatasetId, ResearchRevisionPlanError> {
        let identifier =
            BlsSource::analytical_dataset_identifier(batch.request().object().dataset())
                .map_err(|_error| ResearchRevisionPlanError)?;
        DatasetId::try_from(identifier.as_str()).map_err(|_error| ResearchRevisionPlanError)
    }

    fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        self.source
            .revision_plan(batch)
            .map(Some)
            .map_err(|_error| ResearchRevisionPlanError)
    }
}

/// Callable BLS source through immutable publication and exact restart/PIT read.
pub(crate) struct BlsLiveRuntime {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    closure: BlsMacroApplicationClosure,
    source: Arc<BlsSource>,
    generation: ResearchProviderRuntimeGeneration,
}

impl BlsLiveRuntime {
    /// Runs one complete bounded public-v1 or registered-v2 producer-to-consumer journey.
    ///
    /// Provider requests use only the registry-minted extraction authority. Doctor and discovery
    /// responses are physically sealed by the sole [`crate::ResearchService`] before canonical
    /// extraction, the whole plan commits atomically, and the returned data is read only after the
    /// exact manifest reopens. Adapter-authored provider/effective/availability clocks and native
    /// period semantics pass through unchanged.
    pub(crate) async fn publish_and_read(
        &self,
        request: BlsLiveRequest,
        context: &RequestContext,
    ) -> Result<BlsLiveOutcome, BlsLivePublicationError> {
        validate_source_generation(self.source.as_ref(), &self.generation)?;
        let operation = self
            .coordinator
            .acquire_provider_macro_operation(&self.generation, self.source.dataset(), context)
            .await?;
        let cancellation = operation.cancellation().clone();
        ensure_not_cancelled(&cancellation)?;
        let provider_deadline = operation.provider_deadline()?;
        let doctor_deadline = request.doctor_deadline.min(provider_deadline);
        let acquisition_deadline = request.acquisition_deadline.min(provider_deadline);
        let seal_deadline = request.seal_deadline.min(operation.operation_deadline());

        let discovery = DiscoveryRequest::try_new(
            self.source.dataset().clone(),
            None,
            maximum_discovery_chunks(self.source.as_ref())?,
            acquisition_deadline,
        )?;
        let handoff = self
            .closure
            .acquire_complete_plan(
                self.source.as_ref(),
                operation.extraction(),
                doctor_deadline,
                discovery,
                BlsSealFirstExtractionLimits::new(
                    request.maximum_records_per_chunk,
                    request.maximum_canonical_bytes_per_chunk,
                ),
                seal_deadline,
                cancellation.clone(),
            )
            .await?;
        operation.ensure_live()?;
        ensure_not_cancelled(&cancellation)?;

        let prepared = handoff.try_prepare()?;
        validate_prepared_plan(self.source.as_ref(), &self.generation, &prepared)?;
        let publication_digest = prepared.publication_digest();
        let observed_at = system_timestamp()?;
        let reservation = reserve_publication(
            self.coordinator.as_ref(),
            &self.generation,
            &operation,
            prepared.analytical_dataset(),
            publication_digest,
            observed_at,
        )
        .await?;
        let publication = self
            .closure
            .commit_prepared_plan(
                prepared,
                reservation,
                operation.publication_authority(),
                cancellation.clone(),
            )
            .await?;
        operation.ensure_live()?;
        ensure_not_cancelled(&cancellation)?;

        let read_request = BlsProviderPeriodLatestKnownRequest::try_new(
            publication.restart_selector(),
            request.series_allowlist,
            request.knowledge_cutoff,
            request.effective_period_cutoff,
        )?;
        let state = self
            .closure
            .read_provider_period_latest_known(
                read_request,
                request.query_limits,
                request.query_deadline.min(operation.operation_deadline()),
                cancellation,
            )
            .await?;
        operation.ensure_live()?;
        let read = match state {
            BlsMacroCapabilityState::Available(read) => {
                if read.restart_selector().manifest() != publication.receipt().manifest()
                    || read.source_id() != self.generation.metadata().source_id()
                {
                    return Err(BlsLivePublicationError::RestartMismatch);
                }
                BlsLiveReadOutcome::Available(read)
            }
            BlsMacroCapabilityState::Unavailable(
                BlsMacroUnavailableReason::IncompleteSeriesAtCutoff,
            ) => BlsLiveReadOutcome::IncompleteAtCutoff {
                restart_selector: publication.restart_selector(),
            },
            BlsMacroCapabilityState::Unavailable(
                BlsMacroUnavailableReason::ActivationRequired
                | BlsMacroUnavailableReason::ManifestRequired,
            ) => return Err(BlsLivePublicationError::ReadUnavailable),
        };
        Ok(BlsLiveOutcome {
            activation_plan_digest: self.source.activation_plan()?.plan_digest(),
            publication_digest,
            publication,
            read,
        })
    }
}

impl std::fmt::Debug for BlsLiveRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsLiveRuntime")
            .field("profile", self.generation.profile())
            .field("source_id", self.generation.metadata().source_id())
            .field("provider_dataset", self.source.dataset())
            .field("closure", &self.closure)
            .finish_non_exhaustive()
    }
}

/// Exact immutable generation and closed provider-period result from the live BLS journey.
#[derive(Debug)]
pub(crate) struct BlsLiveOutcome {
    activation_plan_digest: EvidenceDigest,
    publication_digest: EvidenceDigest,
    publication: BlsMacroPlanPublication,
    read: BlsLiveReadOutcome,
}

impl BlsLiveOutcome {
    /// Returns the adapter-owned identity of tier, credential generation, series, windows, and
    /// shared rate declaration used for acquisition.
    pub(crate) const fn activation_plan_digest(&self) -> EvidenceDigest {
        self.activation_plan_digest
    }

    /// Returns the exact atomic whole-plan payload bound by the ingest reservation.
    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    /// Returns the immutable generation proven reopenable immediately after commit.
    pub(crate) const fn publication(&self) -> &BlsMacroPlanPublication {
        &self.publication
    }

    /// Returns the complete typed read only for the available state.
    pub(crate) const fn available_read(&self) -> Option<&BlsProviderPeriodLatestKnownDto> {
        self.read.available()
    }

    /// Returns the exact restart selector for either closed live-read state.
    pub(crate) const fn restart_selector(
        &self,
    ) -> &market_squawk_data::ProviderMacroPlanRestartSelector {
        self.read.restart_selector()
    }

    /// Returns committed restart evidence only for exact cutoff incompleteness.
    pub(crate) const fn incomplete_restart_selector(
        &self,
    ) -> Option<&market_squawk_data::ProviderMacroPlanRestartSelector> {
        self.read.incomplete_restart_selector()
    }
}

/// Closed live read result after the new immutable generation has committed and reopened.
#[derive(Debug)]
enum BlsLiveReadOutcome {
    /// The exact cutoff yielded one selected observed-or-missing row for every requested series.
    Available(BlsProviderPeriodLatestKnownDto),
    /// The exact cutoff had no complete selected set; the retained selector identifies the
    /// committed and already-reopened generation that was queried.
    IncompleteAtCutoff {
        restart_selector: market_squawk_data::ProviderMacroPlanRestartSelector,
    },
}

impl BlsLiveReadOutcome {
    /// Returns the complete typed read only for the available state.
    const fn available(&self) -> Option<&BlsProviderPeriodLatestKnownDto> {
        match self {
            Self::Available(read) => Some(read),
            Self::IncompleteAtCutoff { .. } => None,
        }
    }

    /// Returns committed restart evidence only for exact cutoff incompleteness.
    const fn incomplete_restart_selector(
        &self,
    ) -> Option<&market_squawk_data::ProviderMacroPlanRestartSelector> {
        match self {
            Self::Available(_) => None,
            Self::IncompleteAtCutoff { restart_selector } => Some(restart_selector),
        }
    }

    const fn restart_selector(&self) -> &market_squawk_data::ProviderMacroPlanRestartSelector {
        match self {
            Self::Available(read) => read.restart_selector(),
            Self::IncompleteAtCutoff { restart_selector } => restart_selector,
        }
    }
}

async fn reserve_publication(
    coordinator: &ProductionResearchIngestCoordinator,
    generation: &ResearchProviderRuntimeGeneration,
    operation: &ProviderMacroOperationAuthority,
    analytical_dataset: &DatasetId,
    publication_digest: EvidenceDigest,
    observed_at: Timestamp,
) -> Result<market_squawk_data::IngestReservation, BlsLivePublicationError> {
    let identity = IngestIdentity::try_new(
        generation.metadata().source_id().clone(),
        publication_digest,
        SourceOperation::Persist,
        bls_plan_ingest_identity(generation, analytical_dataset, publication_digest)?,
    )?;
    let rights = operation.rights_decision(publication_digest, observed_at)?;
    coordinator
        .research
        .analytical()
        .reserve_source_ingest(
            generation.metadata(),
            generation.authority_effective_at(),
            rights,
            &identity,
            operation.cancellation(),
        )
        .await
        .map_err(Into::into)
}

fn validate_source_generation(
    source: &BlsSource,
    generation: &ResearchProviderRuntimeGeneration,
) -> Result<(), BlsLivePublicationError> {
    let plan = source.activation_plan()?;
    let credential_rejoin = match plan.rate().tier() {
        BlsAccessTier::PublicV1 => {
            if generation.credential_generation().is_some()
                || generation.secret_reference().is_some()
            {
                return Err(BlsLivePublicationError::SourceGenerationMismatch);
            }
            BlsCredentialRejoin::PublicNoCredential
        }
        BlsAccessTier::RegisteredV2 => BlsCredentialRejoin::for_registered_v2(
            generation
                .secret_reference()
                .ok_or(BlsLivePublicationError::SourceGenerationMismatch)?,
        )?,
    };
    if plan.credential_rejoin() != credential_rejoin
        || plan.source_id() != generation.metadata().source_id()
        || plan.metadata_revision() != generation.metadata().revision()
        || plan.provider_dataset() != source.dataset()
        || plan.rate().shared_rate_declaration().validate().is_err()
        || !plan.rate().persistent_shared_authority_required()
        || !plan.rate().counts_all_started_attempts()
    {
        return Err(BlsLivePublicationError::SourceGenerationMismatch);
    }
    Ok(())
}

fn validate_prepared_plan(
    source: &BlsSource,
    generation: &ResearchProviderRuntimeGeneration,
    prepared: &BlsPreparedMacroPlan,
) -> Result<(), BlsLivePublicationError> {
    let plan = source.activation_plan()?;
    let expected_dataset = DatasetId::try_from(plan.analytical_dataset().as_str())
        .map_err(|_error| BlsLivePublicationError::SourceGenerationMismatch)?;
    if prepared.source_id() != generation.metadata().source_id()
        || prepared.analytical_dataset() != &expected_dataset
        || prepared.total_rows() == 0
        || prepared.publication_digest().bytes() == [0; 32]
    {
        return Err(BlsLivePublicationError::PreparedPlanMismatch);
    }
    Ok(())
}

fn maximum_discovery_chunks(source: &BlsSource) -> Result<NonZeroU16, BlsLivePublicationError> {
    let maximum = source
        .activation_plan()?
        .rate()
        .application_requests_per_day()
        .checked_sub(1)
        .ok_or(BlsLivePublicationError::Capacity)?;
    NonZeroU16::new(maximum).ok_or(BlsLivePublicationError::Capacity)
}

fn bls_plan_ingest_identity(
    generation: &ResearchProviderRuntimeGeneration,
    analytical_dataset: &DatasetId,
    publication_digest: EvidenceDigest,
) -> Result<String, BlsLivePublicationError> {
    use sha2::Sha256;

    let mut digest = Sha256::new();
    digest.update(b"market-squawk/bls-complete-plan-ingest/v1\0");
    update_digest_field(&mut digest, generation.profile().as_str().as_bytes())?;
    update_digest_field(
        &mut digest,
        generation.metadata().source_id().as_str().as_bytes(),
    )?;
    update_digest_field(&mut digest, analytical_dataset.as_str().as_bytes())?;
    digest.update(generation.generation_digest()?.bytes());
    digest.update(publication_digest.bytes());
    Ok(format!("bls-plan-v1-{:x}", digest.finalize()))
}

fn update_digest_field(
    digest: &mut sha2::Sha256,
    value: &[u8],
) -> Result<(), BlsLivePublicationError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_error| BlsLivePublicationError::Capacity)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}

fn system_timestamp() -> Result<Timestamp, BlsLivePublicationError> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_error| BlsLivePublicationError::TrustedTimeUnavailable)?;
    let nanos = i64::try_from(elapsed.as_nanos())
        .map_err(|_error| BlsLivePublicationError::TrustedTimeUnavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<(), BlsLivePublicationError> {
    if cancellation.is_cancelled() {
        Err(BlsLivePublicationError::Cancelled)
    } else {
        Ok(())
    }
}

/// Failure in the exact current BLS producer-to-consumer path.
#[derive(Debug, Error)]
pub(crate) enum BlsLivePublicationError {
    /// The BLS adapter rejected source, credential-generation, or provider evidence.
    #[error("BLS source rejected live publication authority")]
    Adapter(#[from] BlsSourceError),
    /// Existing BLS sealing, canonical publication, restart, or PIT selection failed.
    #[error("BLS publication closure failed")]
    Application(#[from] BlsMacroApplicationError),
    /// The shared registry, provider generation, or publication lease is unavailable.
    #[error("BLS runtime authority is unavailable")]
    Composition(#[from] ResearchIngestCompositionError),
    /// The registry could not mint current extraction authority for the exact source.
    #[error("BLS extraction authority is unavailable")]
    Registry(#[from] market_squawk_sources::RegistryError),
    /// The fixed BLS request-plan discovery contract is invalid.
    #[error("BLS discovery request is invalid")]
    Discovery(#[from] market_squawk_sources::ExtractionError),
    /// The shared analytical ingest authority rejected reservation or precommit.
    #[error("BLS analytical publication failed")]
    Ingest(#[from] IngestError),
    /// Exact source/payload/operation/idempotency identity is not reservation-safe.
    #[error("BLS ingest identity is invalid")]
    IngestIdentity(#[from] market_squawk_data::RightsError),
    /// The retained source rights no longer admit exact payload persistence.
    #[error("BLS persistence rights are unavailable")]
    Rights(#[from] ServiceError),
    /// The concrete source does not match the exact current public-v1 or registered-v2 generation.
    #[error("BLS source generation does not match application authority")]
    SourceGenerationMismatch,
    /// The adapter handoff does not match its configured source/dataset or is empty.
    #[error("BLS prepared plan does not match source authority")]
    PreparedPlanMismatch,
    /// Exact whole-plan restart and typed-read evidence changed manifest or source.
    #[error("BLS restart or PIT read changed immutable identity")]
    RestartMismatch,
    /// The fixed producer-to-consumer journey did not return an available typed read.
    #[error("BLS provider-period read is unavailable")]
    ReadUnavailable,
    /// A request-plan count or identity field exceeds the bounded representation.
    #[error("BLS request exceeds application capacity")]
    Capacity,
    /// Caller or exact runtime-generation cancellation won the operation.
    #[error("BLS publication was cancelled")]
    Cancelled,
    /// The process wall clock cannot produce a trusted publication coordinate.
    #[error("BLS publication trusted time is unavailable")]
    TrustedTimeUnavailable,
}

#[cfg(all(test, feature = "bls-installed-fixture", debug_assertions))]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use market_squawk_adapter_bls::{
        BLS_TIMESERIES_NATIVE_LINEAGE_IMPLEMENTATION, BlsAccessTier, BlsAuthorization,
        BlsCanonicalObservationSemantics, BlsCanonicalProviderSemantics, BlsCredentialRejoin,
        BlsRegistrationKey, BlsScriptedResponse, BlsScriptedTransportFactory, BlsSeriesMetadata,
        BlsSource, BlsSourceConfig, BlsTimeseriesNativeLineageRowV1,
        bls_application_provider_budget,
    };
    use market_squawk_data::{
        AnalyticalMacroSeriesAllowlist, CatalogConfig, CatalogResultLimits, ObjectStoreConfig,
        QueryLimits, RightsBasis, SourceOperation, SqliteProviderRateStore,
    };
    use market_squawk_domain::{
        AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
        DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MacroObservation,
        MetadataRevision, ResearchPeriod, RevisionBoundPayloadEvidence, SchemaVersion,
        SequenceCapability, SourceId, SourceIdentifier, Timestamp,
    };
    use market_squawk_platform::{
        EncryptedFileSecretStore, LocalPaths, SecretCancellation, SecretGeneration,
        SecretInteractionPolicy, SecretKey, SecretOperationControl, SecretRef, SecretStore,
        SecretValue,
    };
    use market_squawk_services::{JsonStructureLimits, RequestContext, RequestId, ServiceLimits};
    use market_squawk_sources::{
        ApiEndpointRule, AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
        CoverageDomain, EndpointPolicy, FreshnessPolicy, HistoricalCapability, HttpRequestBounds,
        NetworkAccessPolicy, PathScope, ProviderCapabilityRevision, ProviderRateAuthority,
        ProviderRateDeclaration, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
        SourceMetadataInput, SourceProtocolProfile,
    };
    use sha2::{Digest as _, Sha256};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::*;
    use crate::ResearchService;
    use crate::application::{
        ResearchExtractionLimits, ResearchProviderRuntimeMutationAuthority, ResearchRightsAuthority,
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    /// Test-only proof adapter for the selected-row join contract.
    ///
    /// This adapter reopens the exact creating generation, which is deliberately tiny in this
    /// fixture, and emits only rows that uniquely match the analytical selection. Production does
    /// not install it and therefore fails closed until shared data supplies the indexed bounded
    /// method required by [`BlsSelectedRowEvidenceJoin`].
    struct FixtureBlsSelectedRowEvidenceJoin {
        research: Arc<ResearchService>,
    }

    impl BlsSelectedRowEvidenceJoin for FixtureBlsSelectedRowEvidenceJoin {
        fn reopen_selected_rows<'a>(
            &'a self,
            restart_selector: &'a market_squawk_data::ProviderMacroPlanRestartSelector,
            output: &'a market_squawk_data::AnalyticalMacroProviderPeriodLatestKnownOutput,
            limits: BlsSelectedRowEvidenceLimits,
            deadline: Instant,
            cancellation: CancellationToken,
        ) -> BoxFuture<'a, Result<BlsSelectedRowEvidenceJoinReceipt, BlsSelectedRowEvidenceJoinError>>
        {
            Box::pin(async move {
                ensure_fixture_evidence_join_live(deadline, &cancellation)?;
                let store = self.research.provider_capture_store();
                let owned = self
                    .research
                    .analytical()
                    .generation_owned_provider_capture_evidence(
                        restart_selector.manifest(),
                        store.as_ref(),
                    )
                    .map_err(|_| BlsSelectedRowEvidenceJoinError::Rejected)?;
                if owned.pinned().manifest() != restart_selector.manifest()
                    || owned.source_id() != restart_selector.source_id()
                {
                    return Err(BlsSelectedRowEvidenceJoinError::Rejected);
                }

                let mut batches = Vec::new();
                for object in owned.objects() {
                    for input in object.inputs() {
                        ensure_fixture_evidence_join_live(deadline, &cancellation)?;
                        let binding = input.binding();
                        let native_schema = binding.native_lineage();
                        let (Some(sidecar), Some(sidecar_digest)) = (
                            native_schema.batch_sidecar_semantic_payload(),
                            native_schema.batch_sidecar_semantic_payload_digest(),
                        ) else {
                            return Err(BlsSelectedRowEvidenceJoinError::Rejected);
                        };
                        if binding.capture().source_id() != restart_selector.source_id()
                            || native_schema.implementation()
                                != BLS_TIMESERIES_NATIVE_LINEAGE_IMPLEMENTATION
                            || native_schema.row_count() != binding.rows().len()
                        {
                            return Err(BlsSelectedRowEvidenceJoinError::Rejected);
                        }
                        let semantics =
                            BlsCanonicalProviderSemantics::try_decode_persisted_native_sidecar(
                                sidecar,
                            )
                            .map_err(|_| BlsSelectedRowEvidenceJoinError::Rejected)?;
                        let mut selected_rows = Vec::new();
                        for row in binding.rows() {
                            let native = BlsTimeseriesNativeLineageRowV1::try_decode_persisted(
                                native_schema.version(),
                                native_schema.implementation(),
                                row.native_semantic_payload(),
                            )
                            .map_err(|_| BlsSelectedRowEvidenceJoinError::Rejected)?;
                            let companion = semantics
                                .validate_persisted_native_row(row.canonical_row_ordinal(), &native)
                                .map_err(|_| BlsSelectedRowEvidenceJoinError::Rejected)?;
                            if output.observations().iter().any(|observation| {
                                fixture_semantics_match_selected(companion, &native, observation)
                            }) {
                                selected_rows.push(BlsSelectedRowNativeEvidence::new(
                                    row.canonical_row_ordinal(),
                                    row.canonical_row_digest(),
                                    row.native_semantic_payload().to_vec().into_boxed_slice(),
                                    row.native_semantic_digest(),
                                    row.received_at(),
                                ));
                            }
                        }
                        if !selected_rows.is_empty() {
                            batches.push(BlsSelectedRowEvidenceBatch::new(
                                binding.binding_digest(),
                                native_schema.version(),
                                native_schema.implementation().to_owned().into_boxed_str(),
                                native_schema.row_count(),
                                native_schema.batch_digest(),
                                sidecar.to_vec().into_boxed_slice(),
                                sidecar_digest,
                                selected_rows,
                            ));
                        }
                    }
                }
                ensure_fixture_evidence_join_live(deadline, &cancellation)?;
                BlsSelectedRowEvidenceJoinReceipt::try_new(
                    restart_selector.manifest().clone(),
                    restart_selector.source_id().clone(),
                    output.selection_digest(),
                    batches,
                    limits,
                )
            })
        }
    }

    fn ensure_fixture_evidence_join_live(
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), BlsSelectedRowEvidenceJoinError> {
        if cancellation.is_cancelled() {
            Err(BlsSelectedRowEvidenceJoinError::Cancelled)
        } else if Instant::now() >= deadline {
            Err(BlsSelectedRowEvidenceJoinError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }

    fn fixture_semantics_match_selected(
        companion: &BlsCanonicalObservationSemantics,
        native: &BlsTimeseriesNativeLineageRowV1,
        observation: &MacroObservation,
    ) -> bool {
        let provenance = observation.context().provenance();
        let value_matches = match (
            observation.value().observed_value(),
            observation.value().missing_value(),
            companion.value(),
        ) {
            (Some(selected), None, Some(provider)) => selected == provider,
            (None, Some(missing), None) => missing.marker().as_str() == companion.raw_value(),
            (Some(_), Some(_), _)
            | (None, None, _)
            | (Some(_), None, None)
            | (None, Some(_), Some(_)) => false,
        };
        companion.series_id() == observation.series()
            && companion.effective_time() == observation.context().time().effective()
            && companion.canonical_revision() == provenance.source_identifier()
            && companion.locally_available_at() == provenance.received_at()
            && companion.canonical_ingested_at() == provenance.ingested_at()
            && provenance.availability().conservative_available_at()
                == Some(companion.locally_available_at())
            && native.series().unit() == observation.unit()
            && value_matches
    }

    const SERIES_METADATA: &[u8] = br#"{
      "schema_version":1,
      "series_id":"LNS14000000",
      "title":"Unemployment Rate",
      "unit":"percent",
      "frequency":"monthly",
      "seasonal_adjustment":"seasonally-adjusted",
      "measure":"rate"
    }"#;
    const PROVIDER_DOCTOR_RESPONSE: &[u8] = br#"{
      "status":"REQUEST_SUCCEEDED",
      "responseTime":1,
      "message":[],
      "Results":{"series":[{"seriesID":"LNS14000000","data":[{
        "year":"2026","period":"M06","periodName":"June","latest":"true",
        "value":"4.2","footnotes":[]
      }]}]}
    }"#;
    const PROVIDER_RESPONSE_V1: &[u8] = br#"{
      "status":"REQUEST_SUCCEEDED",
      "responseTime":1,
      "message":[],
      "Results":{"series":[{"seriesID":"LNS14000000","data":[{
        "year":"2026","period":"M06","periodName":"June","latest":"true",
        "value":"4.2","footnotes":[{"code":"P","text":"Preliminary."}]
      },{
        "year":"2026","period":"M05","periodName":"May","latest":"false",
        "value":"-","footnotes":[{"code":null,"text":"Data not available."}]
      }]}]}
    }"#;
    const PROVIDER_RESPONSE_V2: &[u8] = br#"{
      "status":"REQUEST_SUCCEEDED",
      "responseTime":1,
      "message":[],
      "Results":{"series":[{"seriesID":"LNS14000000","data":[{
        "year":"2026","period":"M06","periodName":"June","latest":"true",
        "value":"4.1","footnotes":[]
      },{
        "year":"2026","period":"M05","periodName":"May","latest":"false",
        "value":"-","footnotes":[{"code":null,"text":"Data not available."}]
      }]}]}
    }"#;
    const LIVE_REGISTERED_ACCEPTANCE: &str = "MARKET_SQUAWK_BLS_REGISTERED_LIVE_ACCEPTANCE";

    #[tokio::test]
    async fn public_and_protected_sources_seal_publish_and_reopen_exact_macro_plans() -> TestResult
    {
        let temporary = tempfile::tempdir()?;
        let live_registered = live_registered_acceptance_enabled()?;
        let registration_key = if live_registered {
            std::env::var("BLS_REGISTRATION_KEY")
                .map_err(|_error| "live BLS registration key is unavailable")?
        } else {
            "fixture-registration-key".to_owned()
        };
        let secret_reference =
            protected_registration_key(temporary.path(), &registration_key, live_registered)?;
        prove_live_journey(
            &temporary.path().join("public-v1"),
            BlsAccessTier::PublicV1,
            None,
            None,
            false,
        )
        .await?;
        prove_live_journey(
            &temporary.path().join("registered-v2"),
            BlsAccessTier::RegisteredV2,
            Some(secret_reference),
            Some(registration_key),
            live_registered,
        )
        .await
    }

    async fn prove_live_journey(
        root: &std::path::Path,
        tier: BlsAccessTier,
        secret_reference: Option<SecretRef>,
        registration_key: Option<String>,
        live_http: bool,
    ) -> TestResult {
        if live_http && tier != BlsAccessTier::RegisteredV2 {
            return Err("live BLS acceptance is registered-v2 only".into());
        }
        let now = current_timestamp()?;
        let observed_at = now.checked_sub_nanos(1_000_000)?;
        let response_received_at = observed_at.checked_sub_nanos(1)?;
        let revised_observed_at = observed_at.checked_add_nanos(1)?;
        let revised_response_received_at = revised_observed_at.checked_sub_nanos(1)?;
        let incomplete_observed_at = revised_observed_at.checked_add_nanos(1)?;
        let incomplete_response_received_at = incomplete_observed_at.checked_sub_nanos(1)?;
        let (authorization, expected_rejoin, profile, evidence_byte, revision) =
            match (tier, secret_reference.as_ref(), registration_key) {
                (BlsAccessTier::PublicV1, None, None) => (
                    BlsAuthorization::public_v1(),
                    BlsCredentialRejoin::PublicNoCredential,
                    "bls.v1-unregistered",
                    1,
                    "bls-public-v1-fixture",
                ),
                (BlsAccessTier::RegisteredV2, Some(reference), Some(registration_key)) => (
                    BlsAuthorization::registered_v2(
                        BlsRegistrationKey::try_new(registration_key)?,
                        reference,
                    )?,
                    BlsCredentialRejoin::for_registered_v2(reference)?,
                    "bls.v2-registered",
                    2,
                    if live_http {
                        "bls-registered-v2-live-acceptance"
                    } else {
                        "bls-registered-v2-fixture"
                    },
                ),
                _ => return Err("credential does not match BLS access tier".into()),
            };
        let config = BlsSourceConfig::try_new(authorization, vec![series_metadata()?], 2026, 2026)?;
        let metadata = source_metadata(tier, now, digest(evidence_byte), revision)?;

        let fixture = (!live_http)
            .then(|| {
                BlsScriptedTransportFactory::try_new(vec![
                    BlsScriptedResponse::try_new(
                        Bytes::from_static(PROVIDER_DOCTOR_RESPONSE),
                        response_received_at,
                        observed_at,
                    )?,
                    BlsScriptedResponse::try_new(
                        Bytes::from_static(PROVIDER_RESPONSE_V1),
                        response_received_at,
                        observed_at,
                    )?,
                    BlsScriptedResponse::try_new(
                        Bytes::from_static(PROVIDER_DOCTOR_RESPONSE),
                        revised_response_received_at,
                        revised_observed_at,
                    )?,
                    BlsScriptedResponse::try_new(
                        Bytes::from_static(PROVIDER_RESPONSE_V2),
                        revised_response_received_at,
                        revised_observed_at,
                    )?,
                    BlsScriptedResponse::try_new(
                        Bytes::from_static(PROVIDER_DOCTOR_RESPONSE),
                        incomplete_response_received_at,
                        incomplete_observed_at,
                    )?,
                    BlsScriptedResponse::try_new(
                        Bytes::from_static(PROVIDER_RESPONSE_V2),
                        incomplete_response_received_at,
                        incomplete_observed_at,
                    )?,
                ])
            })
            .transpose()?;
        let source = match fixture.as_ref() {
            Some(fixture) => fixture.production_source(metadata.clone(), config)?,
            None => BlsSource::try_new(metadata.clone(), config)?,
        };
        assert_eq!(
            source.activation_plan()?.credential_rejoin(),
            expected_rejoin
        );

        let rights_subject = source
            .activation_plan()?
            .rate()
            .authorization_subject()
            .cloned();
        let rights_basis =
            RightsBasis::reviewed_terms("https://example.test/bls-terms", digest(3))?;
        let rights = match rights_subject.as_ref() {
            Some(subject) => ResearchRightsAuthority::try_new_scoped(
                metadata.source_id().clone(),
                rights_basis,
                digest(4),
                digest(5),
                now.checked_add_nanos(60_000_000_000)?,
                vec![subject.clone()],
                vec![SourceOperation::Persist],
            )?,
            None => ResearchRightsAuthority::try_new(
                metadata.source_id().clone(),
                rights_basis,
                digest(5),
                Some(now.checked_add_nanos(60_000_000_000)?),
            )?,
        };
        let generation = ResearchProviderRuntimeGeneration::try_new(
            SourceIdentifier::try_from(profile)?,
            Uuid::new_v4(),
            ProviderCapabilityRevision::new(1)?,
            digest(6),
            secret_reference.as_ref().map(SecretRef::generation),
            secret_reference,
            now,
            metadata.clone(),
            rights.clone(),
        )?;
        let paths = LocalPaths::prepare(root.join("research"))?;
        let research = Arc::new(open_research(&paths)?);
        let provider_rate = ProviderRateAuthority::try_new(Arc::new(
            SqliteProviderRateStore::try_open(root.join("provider-rate.sqlite3"))?,
        ))?;
        if let Some(subject) = rights_subject {
            provider_rate.bind_authorization_subject(
                metadata.authorization().mode(),
                metadata.authorization().evidence().content_digest(),
                &subject,
            )?;
        }
        let registry = AuthoritativeSourceRegistry::try_new_in_memory_for_bounded_extraction(
            Arc::new(provider_rate.clone()),
            provider_rate,
        )?;
        let (coordinator, mutation, alpaca) =
            ProductionResearchIngestCoordinator::try_new_with_runtime_authorities(
                registry,
                Arc::clone(&research),
                ResearchExtractionLimits::standard(),
                std::iter::empty(),
            )?;
        let selected_row_evidence = Arc::new(FixtureBlsSelectedRowEvidenceJoin {
            research: Arc::clone(&research),
        });
        let composition = BlsLiveComposition::try_new_with_selected_row_evidence(
            Arc::clone(&coordinator),
            source,
            generation.clone(),
            selected_row_evidence,
        )?;
        let (live_source, runtime) = composition.into_parts();
        register_source(&mutation, generation, live_source, rights)?;

        let cutoff = now.checked_add_nanos(30_000_000_000)?;
        let period = ResearchPeriod::try_new(
            SourceIdentifier::try_from("bls-monthly")?,
            2026,
            NonZeroU16::new(6).ok_or("invalid period")?,
            SourceIdentifier::try_from("M06")?,
        )?;
        let no_eligible_period = ResearchPeriod::try_new(
            SourceIdentifier::try_from("bls-monthly")?,
            2025,
            NonZeroU16::new(12).ok_or("invalid no-row period")?,
            SourceIdentifier::try_from("M12")?,
        )?;
        let allowlist = AnalyticalMacroSeriesAllowlist::try_from_code_owned(&["LNS14000000"])?;
        let query_limits = query_limits()?;
        let operation_deadline = Instant::now() + Duration::from_secs(30);
        let live_request = BlsLiveRequest::new(
            now.checked_add_nanos(20_000_000_000)?,
            now.checked_add_nanos(20_000_000_000)?,
            operation_deadline,
            NonZeroU32::new(16).ok_or("invalid record bound")?,
            NonZeroU64::new(1024 * 1024).ok_or("invalid byte bound")?,
            allowlist.clone(),
            cutoff,
            period.clone(),
            query_limits,
            operation_deadline,
        );
        let first_outcome = runtime
            .publish_and_read(live_request.clone(), &request_context(operation_deadline)?)
            .await?;
        let first_read = first_outcome
            .available_read()
            .ok_or("first live read was incomplete")?;
        assert_complete_observed_handoff(first_read, 1, None, None);
        if !live_http {
            assert_complete_observed_handoff(first_read, 1, Some("4.2"), Some(true));
        }
        let first_generation = (
            first_read.restart_selector().clone(),
            first_outcome.publication().receipt().manifest().clone(),
            first_read.output().selection_digest(),
        );
        let (outcome, prior_generation, incomplete_generation) = if live_http {
            (first_outcome, None, None)
        } else {
            let revised_outcome = runtime
                .publish_and_read(live_request.clone(), &request_context(operation_deadline)?)
                .await?;
            let revised_read = revised_outcome
                .available_read()
                .ok_or("revised live read was incomplete")?;
            assert_complete_observed_handoff(revised_read, 2, Some("4.1"), Some(false));
            assert_ne!(
                first_outcome.publication().receipt().manifest(),
                revised_outcome.publication().receipt().manifest()
            );
            assert_ne!(
                first_read.output().selection_digest(),
                revised_read.output().selection_digest()
            );
            let mut incomplete_request = live_request;
            incomplete_request.effective_period_cutoff = no_eligible_period.clone();
            let incomplete_outcome = runtime
                .publish_and_read(incomplete_request, &request_context(operation_deadline)?)
                .await?;
            let incomplete_selector = incomplete_outcome
                .incomplete_restart_selector()
                .ok_or("no-row live read did not retain exact incompleteness")?;
            assert_eq!(
                incomplete_selector.manifest(),
                incomplete_outcome.publication().receipt().manifest()
            );
            assert_eq!(
                incomplete_selector.manifest(),
                incomplete_outcome.publication().reopened().manifest()
            );
            assert_eq!(
                incomplete_selector.publication_digest(),
                incomplete_outcome.publication_digest()
            );
            assert_ne!(
                incomplete_selector.manifest(),
                revised_outcome.publication().receipt().manifest()
            );
            let incomplete_generation = (
                incomplete_selector.clone(),
                incomplete_outcome
                    .publication()
                    .receipt()
                    .manifest()
                    .clone(),
            );
            (
                revised_outcome,
                Some(first_generation),
                Some(incomplete_generation),
            )
        };
        if let Some(fixture) = fixture {
            assert_eq!(fixture.counters()?.attempts, 6);
            assert_eq!(fixture.counters()?.completed, 6);
            assert_eq!(fixture.counters()?.remaining, 0);
        }
        let outcome_read = outcome
            .available_read()
            .ok_or("retained live read was incomplete")?;
        let restart_selector = outcome_read.restart_selector().clone();
        let manifest = outcome.publication().receipt().manifest().clone();
        let selection_digest = outcome_read.output().selection_digest();

        drop(outcome);
        drop(runtime);
        drop(mutation);
        drop(alpaca);
        drop(coordinator);
        drop(research);

        let reopened = Arc::new(open_research(&paths)?);
        let uncomposed = BlsMacroApplicationClosure::new(Arc::clone(&reopened));
        let uncomposed_error = uncomposed
            .read_provider_period_latest_known(
                BlsProviderPeriodLatestKnownRequest::try_new(
                    restart_selector.clone(),
                    allowlist.clone(),
                    cutoff,
                    period.clone(),
                )?,
                query_limits,
                Instant::now() + Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await
            .expect_err("available BLS read must require the root selected-row evidence join");
        assert!(matches!(
            uncomposed_error,
            BlsMacroApplicationError::SelectedRowEvidenceJoinUnavailable
        ));
        let reopened_selected_row_evidence = Arc::new(FixtureBlsSelectedRowEvidenceJoin {
            research: Arc::clone(&reopened),
        });
        let closure = BlsMacroApplicationClosure::with_selected_row_evidence(
            reopened,
            reopened_selected_row_evidence,
        );
        let reopened_current = closure
            .read_provider_period_latest_known(
                BlsProviderPeriodLatestKnownRequest::try_new(
                    restart_selector.clone(),
                    allowlist.clone(),
                    cutoff,
                    period.clone(),
                )?,
                query_limits,
                Instant::now() + Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await?;
        let read = reopened_current
            .available()
            .ok_or("restart read unavailable")?;
        assert_eq!(read.reopened().manifest(), &manifest);
        assert_eq!(read.output().selection_digest(), selection_digest);
        assert_complete_observed_handoff(
            read,
            if live_http { 1 } else { 2 },
            None,
            (!live_http).then_some(false),
        );

        let (no_eligible_selector, no_eligible_manifest) = incomplete_generation
            .as_ref()
            .map_or((&restart_selector, &manifest), |(selector, manifest)| {
                (selector, manifest)
            });
        let no_eligible = closure
            .read_provider_period_latest_known(
                BlsProviderPeriodLatestKnownRequest::try_new(
                    no_eligible_selector.clone(),
                    allowlist.clone(),
                    cutoff,
                    no_eligible_period,
                )?,
                query_limits,
                Instant::now() + Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await?;
        assert!(no_eligible.available().is_none());
        assert_eq!(
            no_eligible.unavailable_reason(),
            Some(BlsMacroUnavailableReason::IncompleteSeriesAtCutoff)
        );
        assert_eq!(no_eligible_selector.manifest(), no_eligible_manifest);

        if let Some((prior_selector, prior_manifest, prior_selection_digest)) = prior_generation {
            let reopened_prior = closure
                .read_provider_period_latest_known(
                    BlsProviderPeriodLatestKnownRequest::try_new(
                        prior_selector,
                        allowlist.clone(),
                        cutoff,
                        period.clone(),
                    )?,
                    query_limits,
                    Instant::now() + Duration::from_secs(5),
                    CancellationToken::new(),
                )
                .await?;
            let prior = reopened_prior
                .available()
                .ok_or("prior restart read unavailable")?;
            assert_eq!(prior.reopened().manifest(), &prior_manifest);
            assert_eq!(prior.output().selection_digest(), prior_selection_digest);
            assert_complete_observed_handoff(prior, 1, Some("4.2"), Some(true));

            let missing_period = ResearchPeriod::try_new(
                SourceIdentifier::try_from("bls-monthly")?,
                2026,
                NonZeroU16::new(5).ok_or("invalid missing period")?,
                SourceIdentifier::try_from("M05")?,
            )?;
            let missing_state = closure
                .read_provider_period_latest_known(
                    BlsProviderPeriodLatestKnownRequest::try_new(
                        restart_selector,
                        allowlist,
                        cutoff,
                        missing_period.clone(),
                    )?,
                    query_limits,
                    Instant::now() + Duration::from_secs(5),
                    CancellationToken::new(),
                )
                .await?;
            let missing = missing_state
                .available()
                .ok_or("explicit missing restart read unavailable")?;
            assert_complete_missing_handoff(missing, &missing_period, 1);
        }
        Ok(())
    }

    fn assert_complete_observed_handoff(
        read: &BlsProviderPeriodLatestKnownDto,
        expected_revision: u32,
        expected_value: Option<&str>,
        expected_preliminary: Option<bool>,
    ) {
        let request = read.analytical_request();
        assert_eq!(request.manifest(), read.restart_selector().manifest());
        assert_eq!(request.manifest(), read.reopened().manifest());
        assert_eq!(request.source_series().source_id(), read.source_id());
        assert_eq!(
            read.output().period_scheme(),
            request.effective_period_cutoff().scheme()
        );
        let [observation] = read.output().observations() else {
            panic!("BLS consumer handoff did not retain exactly one requested series");
        };
        assert_eq!(
            observation
                .context()
                .time()
                .effective()
                .source_period_value(),
            Some(request.effective_period_cutoff())
        );
        assert_eq!(
            observation.context().time().revision().get(),
            expected_revision
        );
        assert!(observation.value().missing_value().is_none());
        assert!(
            observation
                .context()
                .provenance()
                .availability()
                .conservative_available_at()
                .is_some_and(|available_at| available_at <= request.knowledge_cutoff())
        );
        if let Some(expected_value) = expected_value {
            assert_eq!(
                observation
                    .value()
                    .observed_value()
                    .map(|value| value.to_string())
                    .as_deref(),
                Some(expected_value)
            );
        } else {
            assert!(observation.value().observed_value().is_some());
        }
        let [semantic] = read.semantic_observations() else {
            panic!("BLS consumer handoff did not retain exactly one native semantic row");
        };
        assert_common_semantic_evidence(semantic);
        let companion = semantic.companion();
        let native = semantic.native().observation();
        assert_eq!(companion.series_id(), observation.series());
        assert_eq!(
            companion.effective_time(),
            observation.context().time().effective()
        );
        assert_eq!(companion.value(), observation.value().observed_value());
        assert_eq!(companion.raw_value(), native.raw_value());
        if let Some(expected_value) = expected_value {
            assert_eq!(companion.raw_value(), expected_value);
        }
        assert_eq!(companion.is_latest(), true);
        if let Some(expected_preliminary) = expected_preliminary {
            assert_eq!(companion.is_preliminary(), expected_preliminary);
            assert_eq!(native.is_preliminary(), expected_preliminary);
            if expected_preliminary {
                let [footnote] = companion.footnotes() else {
                    panic!("preliminary BLS row did not retain its exact footnote");
                };
                assert_eq!(footnote.code(), Some("P"));
                assert_eq!(footnote.text(), Some("Preliminary."));
            } else {
                assert!(companion.footnotes().is_empty());
            }
        }
        assert!(companion.missing_explanations().is_empty());
    }

    fn assert_complete_missing_handoff(
        read: &BlsProviderPeriodLatestKnownDto,
        expected_period: &ResearchPeriod,
        expected_revision: u32,
    ) {
        let request = read.analytical_request();
        assert_eq!(request.effective_period_cutoff(), expected_period);
        let [observation] = read.output().observations() else {
            panic!("BLS missing-value handoff did not retain exactly one requested series");
        };
        assert_eq!(
            observation
                .context()
                .time()
                .effective()
                .source_period_value(),
            Some(expected_period)
        );
        assert_eq!(
            observation.context().time().revision().get(),
            expected_revision
        );
        assert!(observation.value().observed_value().is_none());
        assert!(observation.value().missing_value().is_some());
        let [semantic] = read.semantic_observations() else {
            panic!("BLS missing-value handoff did not retain exactly one native semantic row");
        };
        assert_common_semantic_evidence(semantic);
        let companion = semantic.companion();
        let native = semantic.native().observation();
        assert_eq!(companion.raw_value(), "-");
        assert_eq!(native.raw_value(), "-");
        assert_eq!(companion.value(), None);
        assert!(!companion.is_latest());
        assert!(!companion.is_preliminary());
        let [footnote] = companion.footnotes() else {
            panic!("missing BLS row did not retain its exact explanatory footnote");
        };
        assert_eq!(footnote.code(), None);
        assert_eq!(footnote.text(), Some("Data not available."));
        assert_eq!(
            companion
                .missing_explanations()
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>(),
            vec!["Data not available."]
        );
    }

    fn assert_common_semantic_evidence(semantic: &BlsProviderPeriodObservationSemanticEvidence) {
        let companion = semantic.companion();
        assert!(companion.response_received_at() < companion.locally_available_at());
        assert!(companion.locally_available_at() < companion.canonical_ingested_at());
        assert_eq!(
            companion.canonical_payload_digest(),
            semantic.canonical_row_digest()
        );
        for digest in [
            semantic.binding_digest(),
            semantic.canonical_row_digest(),
            semantic.native_semantic_digest(),
            semantic.native_batch_digest(),
            semantic.native_sidecar_digest(),
            semantic.provider_semantics_digest(),
        ] {
            assert_eq!(digest.algorithm(), DigestAlgorithm::Sha256);
            assert_ne!(digest.bytes(), [0; 32]);
        }
    }

    fn register_source(
        mutation: &ResearchProviderRuntimeMutationAuthority,
        generation: ResearchProviderRuntimeGeneration,
        source: super::BlsLiveSource,
        rights: ResearchRightsAuthority,
    ) -> Result<(), ResearchIngestCompositionError> {
        mutation.register_provider_source(generation, source, rights)
    }

    fn open_research(paths: &LocalPaths) -> TestResult<ResearchService> {
        Ok(ResearchService::open_or_initialize(
            paths,
            CatalogConfig::try_new(
                paths.catalog()?.clone(),
                Duration::from_millis(750),
                market_squawk_data::CatalogLimit::new(64)?,
                CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
            )?,
            8,
            ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
        )?)
    }

    fn protected_registration_key(
        root: &std::path::Path,
        registration_key: &str,
        live: bool,
    ) -> TestResult<SecretRef> {
        let store = EncryptedFileSecretStore::try_open(
            root.join("secrets"),
            SecretValue::new("bls-live-fixture-unlock".to_owned())?,
        )?;
        let control = SecretOperationControl::try_new(
            "bls-live-fixture",
            Instant::now() + Duration::from_secs(30),
            0,
            SecretInteractionPolicy::Forbid,
            SecretCancellation::new(),
        )?;
        Ok(store.create(
            &SecretKey::try_new(
                "market-squawk.bls",
                if live {
                    "registered-v2-live-acceptance"
                } else {
                    "registered-v2-fixture"
                },
            )?,
            SecretGeneration::new(1)?,
            SecretValue::new(registration_key.to_owned())?,
            &control,
        )?)
    }

    fn live_registered_acceptance_enabled() -> TestResult<bool> {
        match std::env::var(LIVE_REGISTERED_ACCEPTANCE) {
            Err(std::env::VarError::NotPresent) => Ok(false),
            Ok(value) if value == "1" => Ok(true),
            Ok(_) | Err(std::env::VarError::NotUnicode(_)) => {
                Err("live BLS acceptance gate must be unset or exactly 1".into())
            }
        }
    }

    fn series_metadata() -> TestResult<BlsSeriesMetadata> {
        Ok(BlsSeriesMetadata::parse_exact(
            Bytes::from_static(SERIES_METADATA),
            ExactPayloadEvidence::from_content_digest(digest_bytes(SERIES_METADATA)),
            SourceIdentifier::try_from("user-approved-bls-series-metadata")?,
        )?)
    }

    fn source_metadata(
        tier: BlsAccessTier,
        now: Timestamp,
        evidence_digest: EvidenceDigest,
        revision: &str,
    ) -> TestResult<SourceMetadata> {
        let effective = EffectiveInterval::new(now.checked_sub_nanos(1_000_000_000)?, None)?;
        let provider = SourceIdentifier::try_from("us-bls")?;
        let authorization_mode = match tier {
            BlsAccessTier::PublicV1 => AuthorizationMode::PublicInterface,
            BlsAccessTier::RegisteredV2 => AuthorizationMode::UserAuthorized,
        };
        let authorization_basis = match tier {
            BlsAccessTier::PublicV1 => SourceIdentifier::try_from("official-public-interface")?,
            BlsAccessTier::RegisteredV2 => {
                ProviderRateDeclaration::governed_provider_subject(&provider)?
            }
        };
        let authorization = AuthorizationGrant::new(
            authorization_mode,
            AuthorizationBasis::new(authorization_basis),
            ExactPayloadEvidence::from_content_digest(evidence_digest),
            effective,
        );
        let endpoint = match tier {
            BlsAccessTier::PublicV1 => "https://api.bls.gov/publicAPI/v1/timeseries/data/",
            BlsAccessTier::RegisteredV2 => "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        };
        let network = EndpointPolicy::try_from_api_rules(
            vec![ApiEndpointRule::try_new(
                endpoint,
                PathScope::Exact,
                Vec::new(),
                1,
                1,
            )?],
            HttpRequestBounds::default(),
        )?;
        Ok(SourceMetadata::try_new(SourceMetadataInput::new(
            SchemaVersion::CURRENT,
            SourceId::try_from("bls-timeseries")?,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from(revision)?),
                ExactPayloadEvidence::from_content_digest(evidence_digest),
            ),
            SourceClass::OfficialAgency,
            provider,
            authorization,
            SourceCoverage::try_non_instrument(
                ExactPayloadEvidence::from_content_digest(evidence_digest),
                effective,
                CoverageDomain::Macroeconomic,
                CoverageDelay::Delayed(1),
                DeliveryEvidence::Unknown,
            )?,
            DataQuality::OfficialDelayed,
            NetworkAccessPolicy::Allowlisted(network),
            FreshnessPolicy::try_new(60, 60, 60, 60, 1)?,
            Some(bls_application_provider_budget(tier)?),
            SourceCapabilities::new(
                false,
                true,
                SequenceCapability::Unsupported,
                ChecksumCapability::Unsupported,
                HistoricalCapability::Historical,
                false,
            ),
            SourceProtocolProfile::NotLive,
        ))?)
    }

    fn query_limits() -> TestResult<QueryLimits> {
        Ok(QueryLimits::try_new(
            32,
            1024 * 1024,
            64 * 1024 * 1024,
            8,
            1024,
            1024,
            Duration::from_secs(5),
        )?)
    }

    fn request_context(deadline: Instant) -> TestResult<RequestContext> {
        let structure = JsonStructureLimits::try_new(16, 4096, 64, 64)?;
        let limits = ServiceLimits::try_new(4096, 8, 4096, 8, structure)?;
        Ok(RequestContext::new(
            RequestId::String(Arc::from("test.bls-live-publication")),
            CancellationToken::new(),
            deadline,
            limits,
        ))
    }

    fn current_timestamp() -> TestResult<Timestamp> {
        let nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
        Ok(Timestamp::from_unix_nanos(nanos))
    }

    fn digest(byte: u8) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
    }

    fn digest_bytes(bytes: &[u8]) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
    }
}
