//! Registered BLS v2 acquisition, durable macro publication, and exact PIT handoff.
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

use super::{
    BlsMacroApplicationClosure, BlsMacroApplicationError, BlsMacroCapabilityState,
    BlsMacroPlanPublication, BlsPreparedMacroPlan, BlsProviderPeriodLatestKnownDto,
    BlsProviderPeriodLatestKnownRequest, BlsSealFirstExtractionLimits,
    ManagedResearchExtractionSource, ProductionResearchIngestCoordinator,
    ProviderMacroOperationAuthority, ResearchIngestCompositionError,
    ResearchProviderRuntimeGeneration, ResearchRevisionPlanError,
};

/// One exact BLS registered-v2 publication and fixed provider-period read request.
#[derive(Clone, Debug)]
pub(crate) struct BlsRegisteredV2LiveRequest {
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

impl BlsRegisteredV2LiveRequest {
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
/// The pair shares one source instance. Activation registers [`Self::registered_source`] through
/// the ordinary provider runtime mutation authority and retains [`Self::runtime`] in application
/// composition. Constructing another provider-rate authority or BLS cache is neither required nor
/// possible through this capability.
pub(crate) struct BlsRegisteredV2LiveComposition {
    registered_source: BlsRegisteredSource,
    runtime: BlsRegisteredV2LiveRuntime,
}

impl BlsRegisteredV2LiveComposition {
    /// Binds an exact registered-v2 source to its current runtime generation and application
    /// closure before either half is published.
    pub(crate) fn try_new(
        coordinator: Arc<ProductionResearchIngestCoordinator>,
        source: BlsSource,
        generation: ResearchProviderRuntimeGeneration,
    ) -> Result<Self, BlsLivePublicationError> {
        let source = Arc::new(source);
        validate_source_generation(source.as_ref(), &generation)?;
        let closure = BlsMacroApplicationClosure::new(Arc::clone(&coordinator.research));
        Ok(Self {
            registered_source: BlsRegisteredSource {
                source: Arc::clone(&source),
            },
            runtime: BlsRegisteredV2LiveRuntime {
                coordinator,
                closure,
                source,
                generation,
            },
        })
    }

    /// Separates the generic registration value from its exact concrete production runtime.
    pub(crate) fn into_parts(self) -> (BlsRegisteredSource, BlsRegisteredV2LiveRuntime) {
        (self.registered_source, self.runtime)
    }
}

impl std::fmt::Debug for BlsRegisteredV2LiveComposition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsRegisteredV2LiveComposition")
            .field("source", &self.registered_source)
            .field("runtime", &self.runtime)
            .finish()
    }
}

/// Registry-facing wrapper sharing the exact concrete source with the typed BLS runtime.
pub(crate) struct BlsRegisteredSource {
    source: Arc<BlsSource>,
}

impl std::fmt::Debug for BlsRegisteredSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsRegisteredSource")
            .field("source", self.source.as_ref())
            .finish()
    }
}

impl SourceMetadataProvider for BlsRegisteredSource {
    fn metadata(&self) -> &SourceMetadata {
        self.source.metadata()
    }
}

impl ExtractionSource for BlsRegisteredSource {
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

impl ManagedResearchExtractionSource for BlsRegisteredSource {
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

/// Callable registered-v2 BLS source through immutable publication and exact restart/PIT read.
pub(crate) struct BlsRegisteredV2LiveRuntime {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    closure: BlsMacroApplicationClosure,
    source: Arc<BlsSource>,
    generation: ResearchProviderRuntimeGeneration,
}

impl BlsRegisteredV2LiveRuntime {
    /// Runs one complete bounded registered-v2 producer-to-consumer journey.
    ///
    /// Provider requests use only the registry-minted extraction authority. Doctor and discovery
    /// responses are physically sealed by the sole [`crate::ResearchService`] before canonical
    /// extraction, the whole plan commits atomically, and the returned data is read only after the
    /// exact manifest reopens. Adapter-authored provider/effective/availability clocks and native
    /// period semantics pass through unchanged.
    pub(crate) async fn publish_and_read(
        &self,
        request: BlsRegisteredV2LiveRequest,
        context: &RequestContext,
    ) -> Result<BlsRegisteredV2LiveOutcome, BlsLivePublicationError> {
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
        let BlsMacroCapabilityState::Available(read) = state else {
            return Err(BlsLivePublicationError::ReadUnavailable);
        };
        if read.restart_selector().manifest() != publication.receipt().manifest()
            || read.source_id() != self.generation.metadata().source_id()
        {
            return Err(BlsLivePublicationError::RestartMismatch);
        }
        Ok(BlsRegisteredV2LiveOutcome {
            activation_plan_digest: self.source.activation_plan()?.plan_digest(),
            publication_digest,
            publication,
            read,
        })
    }
}

impl std::fmt::Debug for BlsRegisteredV2LiveRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsRegisteredV2LiveRuntime")
            .field("profile", self.generation.profile())
            .field("source_id", self.generation.metadata().source_id())
            .field("provider_dataset", self.source.dataset())
            .field("closure", &self.closure)
            .finish_non_exhaustive()
    }
}

/// Exact immutable generation and provider-period rows returned by the live BLS journey.
#[derive(Debug)]
pub(crate) struct BlsRegisteredV2LiveOutcome {
    activation_plan_digest: EvidenceDigest,
    publication_digest: EvidenceDigest,
    publication: BlsMacroPlanPublication,
    read: BlsProviderPeriodLatestKnownDto,
}

impl BlsRegisteredV2LiveOutcome {
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

    /// Returns the exact manifest-bound provider-period PIT output.
    pub(crate) const fn read(&self) -> &BlsProviderPeriodLatestKnownDto {
        &self.read
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
    let generation_digest = generation.generation_digest()?;
    if plan.rate().tier() != BlsAccessTier::RegisteredV2
        || plan.credential_rejoin() != BlsCredentialRejoin::RegisteredGeneration(generation_digest)
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

/// Failure in the exact registered BLS producer-to-consumer path.
#[derive(Debug, Error)]
pub(crate) enum BlsLivePublicationError {
    /// The BLS adapter rejected source, credential-generation, or provider evidence.
    #[error("registered BLS source rejected live publication authority")]
    Adapter(#[from] BlsSourceError),
    /// Existing BLS sealing, canonical publication, restart, or PIT selection failed.
    #[error("registered BLS publication closure failed")]
    Application(#[from] BlsMacroApplicationError),
    /// The shared registry, provider generation, or publication lease is unavailable.
    #[error("registered BLS runtime authority is unavailable")]
    Composition(#[from] ResearchIngestCompositionError),
    /// The registry could not mint current extraction authority for the exact source.
    #[error("registered BLS extraction authority is unavailable")]
    Registry(#[from] market_squawk_sources::RegistryError),
    /// The fixed BLS request-plan discovery contract is invalid.
    #[error("registered BLS discovery request is invalid")]
    Discovery(#[from] market_squawk_sources::ExtractionError),
    /// The shared analytical ingest authority rejected reservation or precommit.
    #[error("registered BLS analytical publication failed")]
    Ingest(#[from] IngestError),
    /// Exact source/payload/operation/idempotency identity is not reservation-safe.
    #[error("registered BLS ingest identity is invalid")]
    IngestIdentity(#[from] market_squawk_data::RightsError),
    /// The retained source rights no longer admit exact payload persistence.
    #[error("registered BLS persistence rights are unavailable")]
    Rights(#[from] ServiceError),
    /// The concrete source does not match the exact current registered-v2 generation.
    #[error("registered BLS source generation does not match application authority")]
    SourceGenerationMismatch,
    /// The adapter handoff does not match its configured source/dataset or is empty.
    #[error("registered BLS prepared plan does not match source authority")]
    PreparedPlanMismatch,
    /// Exact whole-plan restart and typed-read evidence changed manifest or source.
    #[error("registered BLS restart or PIT read changed immutable identity")]
    RestartMismatch,
    /// The fixed producer-to-consumer journey did not return an available typed read.
    #[error("registered BLS provider-period read is unavailable")]
    ReadUnavailable,
    /// A request-plan count or identity field exceeds the bounded representation.
    #[error("registered BLS request exceeds application capacity")]
    Capacity,
    /// Caller or exact runtime-generation cancellation won the operation.
    #[error("registered BLS publication was cancelled")]
    Cancelled,
    /// The process wall clock cannot produce a trusted publication coordinate.
    #[error("registered BLS publication trusted time is unavailable")]
    TrustedTimeUnavailable,
}
