//! Application-owned BEA activation, atomic macro publication, and provider-period reads.
//!
//! BEA acquisition runs only through a registry-minted extraction authority. The adapter reserves
//! the shared durable response-byte and provider-error claims before every send and consumes the
//! exact in-flight permit when each response terminates. This boundary therefore never performs a
//! second settlement or creates a local counter, credential path, raw store, or publication
//! authority.

use std::{
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Instant,
};

use futures_util::future::BoxFuture;
use market_squawk_adapter_bea::{
    BeaDoctorAdmissionEvidence, BeaDoctorRefreshDisposition, BeaProviderQuotaDeclaration,
    BeaPublicationCandidate, BeaPublicationError, BeaRequiredSharedSettlement,
    BeaSealedDiscoveryAdmission, BeaSource, BeaSourceError,
};
use market_squawk_data::{
    AnalyticalMacroProviderPeriodLatestKnownOutput,
    AnalyticalMacroProviderPeriodLatestKnownRequest, AnalyticalMacroSeriesAllowlist,
    AnalyticalMacroSourceQualifiedSeries, AnalyticalReadError, DatasetId, IngestError,
    IngestIdentity, IngestPrecommitAuthority, IngestReservation, PinnedDataset,
    ProviderMacroPlanChunkInput, ProviderMacroPlanPublicationInput,
    ProviderMacroPlanPublicationReceipt, ProviderMacroPlanRestartSelector,
    ProviderMacroPlanSemantics, QueryLimits, SourceOperation,
};
use market_squawk_domain::{EvidenceDigest, ResearchPeriod, SourceId, SourceIdentifier, Timestamp};
use market_squawk_services::{RequestContext, ServiceError};
use market_squawk_sources::{
    DiscoveryBatch, DiscoveryRequest, ExtractionAuthority, ExtractionBatch, ExtractionRequest,
    ExtractionRevisionPlan, ExtractionSource, ExtractionSourceError,
    ProviderNativeLineageImplementation, SourceMetadata, SourceMetadataProvider,
};
use sha2::Digest as _;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    ManagedResearchExtractionSource, ProductionResearchIngestCoordinator,
    ProviderMacroOperationAuthority, ResearchIngestCompositionError,
    ResearchProviderRuntimeGeneration, ResearchRevisionPlanError,
};
use crate::{ResearchService, ResearchServiceError};

/// Fixed typed operation that later Desktop, CLI, and MCP composition must register.
pub(crate) const BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION: &str =
    "Macro.GetBeaProviderPeriodLatestKnown";

const BEA_PROVIDER_SEMANTICS_SCHEMA: &str = "bea-regional-provider-semantics-v1";

/// One exact bounded Regional publication and fixed provider-period PIT read.
#[derive(Clone, Debug)]
pub(crate) struct BeaRegionalLiveRequest {
    provider_dataset: SourceIdentifier,
    doctor_deadline: Timestamp,
    acquisition_deadline: Timestamp,
    seal_deadline: Instant,
    maximum_records: NonZeroU32,
    maximum_canonical_bytes: NonZeroU64,
    knowledge_cutoff: Timestamp,
    effective_period_cutoff: ResearchPeriod,
    query_limits: QueryLimits,
    query_deadline: Instant,
}

impl BeaRegionalLiveRequest {
    /// Retains independent provider, physical-seal, canonical, and point-in-time bounds.
    #[allow(
        clippy::too_many_arguments,
        reason = "provider deadlines, physical sealing, extraction ceilings, and PIT cutoffs remain independent"
    )]
    pub(crate) fn new(
        provider_dataset: SourceIdentifier,
        doctor_deadline: Timestamp,
        acquisition_deadline: Timestamp,
        seal_deadline: Instant,
        maximum_records: NonZeroU32,
        maximum_canonical_bytes: NonZeroU64,
        knowledge_cutoff: Timestamp,
        effective_period_cutoff: ResearchPeriod,
        query_limits: QueryLimits,
        query_deadline: Instant,
    ) -> Self {
        Self {
            provider_dataset,
            doctor_deadline,
            acquisition_deadline,
            seal_deadline,
            maximum_records,
            maximum_canonical_bytes,
            knowledge_cutoff,
            effective_period_cutoff,
            query_limits,
            query_deadline,
        }
    }
}

/// Same-instance generic registry value and concrete BEA Regional production runtime.
pub(crate) struct BeaRegionalLiveComposition {
    registered_source: BeaRegisteredSource,
    runtime: BeaRegionalLiveRuntime,
}

impl BeaRegionalLiveComposition {
    /// Binds the exact credential/configuration generation before either half becomes callable.
    pub(crate) fn try_new(
        coordinator: Arc<ProductionResearchIngestCoordinator>,
        source: BeaSource,
        generation: ResearchProviderRuntimeGeneration,
    ) -> Result<Self, BeaLivePublicationError> {
        validate_source_generation(&source, &generation)?;
        let source = Arc::new(source);
        let closure = BeaMacroApplicationClosure::new(Arc::clone(&coordinator.research));
        Ok(Self {
            registered_source: BeaRegisteredSource {
                source: Arc::clone(&source),
            },
            runtime: BeaRegionalLiveRuntime {
                coordinator,
                closure,
                source,
                generation,
            },
        })
    }

    /// Separates the registry wrapper from the paired concrete runtime without cloning authority.
    pub(crate) fn into_parts(self) -> (BeaRegisteredSource, BeaRegionalLiveRuntime) {
        (self.registered_source, self.runtime)
    }
}

impl std::fmt::Debug for BeaRegionalLiveComposition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BeaRegionalLiveComposition")
            .field("registered_source", &self.registered_source)
            .field("runtime", &self.runtime)
            .finish()
    }
}

/// Registry-facing wrapper sharing the exact concrete source with the rich BEA runtime.
pub(crate) struct BeaRegisteredSource {
    source: Arc<BeaSource>,
}

impl std::fmt::Debug for BeaRegisteredSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BeaRegisteredSource")
            .field("source", self.source.as_ref())
            .finish()
    }
}

impl SourceMetadataProvider for BeaRegisteredSource {
    fn metadata(&self) -> &SourceMetadata {
        self.source.metadata()
    }
}

impl ExtractionSource for BeaRegisteredSource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        self.source.discover(authority, request, cancellation)
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        self.source.extract(authority, request, cancellation)
    }
}

impl ManagedResearchExtractionSource for BeaRegisteredSource {
    fn rights_subject(
        &self,
        dataset: &SourceIdentifier,
    ) -> Result<Option<SourceIdentifier>, ResearchRevisionPlanError> {
        self.source
            .analytical_dataset_identifier(dataset)
            .map_err(|_error| ResearchRevisionPlanError)?;
        Ok(self
            .source
            .metadata()
            .budget_policy()
            .and_then(|policy| policy.scope().authorization_account())
            .cloned())
    }

    fn analytical_dataset(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<DatasetId, ResearchRevisionPlanError> {
        let identifier = self
            .source
            .analytical_dataset_identifier(batch.request().object().dataset())
            .map_err(|_error| ResearchRevisionPlanError)?;
        DatasetId::try_from(identifier.as_str()).map_err(|_error| ResearchRevisionPlanError)
    }

    fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        let object = batch.request().object();
        if object.source_id() != self.source.metadata().source_id()
            || object.metadata_revision() != self.source.metadata().revision()
            || self
                .source
                .analytical_dataset_identifier(object.dataset())
                .is_err()
        {
            return Err(ResearchRevisionPlanError);
        }
        ExtractionRevisionPlan::locally_observed_with_native_lineage(batch.records().len())
            .map(Some)
            .map_err(|_error| ResearchRevisionPlanError)
    }
}

/// Application-owned BEA physical sealing, publication, and typed-read coordinator.
#[derive(Clone)]
pub(crate) struct BeaMacroApplicationClosure {
    research: Arc<ResearchService>,
}

impl std::fmt::Debug for BeaMacroApplicationClosure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BeaMacroApplicationClosure")
            .field("research", &"[APPLICATION-OWNED RESEARCH SERVICE]")
            .finish()
    }
}

impl BeaMacroApplicationClosure {
    /// Binds BEA to the sole application-owned physical sealer and analytical authority.
    pub(crate) fn new(research: Arc<ResearchService>) -> Self {
        Self { research }
    }

    /// Reports protected setup or an invalid code-owned quota contract without network I/O.
    pub(crate) fn acquisition_state(source: Option<&BeaSource>) -> BeaMacroCapabilityState {
        let Some(source) = source else {
            return BeaMacroCapabilityState::SetupRequired(BeaSetupRequiredDto {
                kind: BeaSetupRequiredKind::ProtectedCredential,
            });
        };
        if !complete_shared_quota_declaration(source.quota_declaration()) {
            return BeaMacroCapabilityState::Unavailable(BeaUnavailableDto::invalid_quota(
                source.quota_declaration(),
            ));
        }
        BeaMacroCapabilityState::SetupRequired(BeaSetupRequiredDto {
            kind: BeaSetupRequiredKind::DoctorActivation,
        })
    }

    /// Acquires, physically seals, rejoins, and activates one protected BEA doctor run.
    ///
    /// The registry-minted extraction authority is the only request path. The BEA adapter uses it
    /// to reserve the worst-case weighted claim before every transport send and consuming-settles
    /// every complete response with exact bytes, error class, and Retry-After evidence. Transport
    /// abandonment charges the reserved maximum through the common authority. No credential
    /// material or independent quota state is retained in the result.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn acquire_seal_and_activate_doctor(
        &self,
        source: &BeaSource,
        authority: &ExtractionAuthority,
        provider_dataset: &SourceIdentifier,
        doctor_deadline: Timestamp,
        publication_deadline: Timestamp,
        seal_deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<BeaDoctorActivationState, BeaMacroApplicationError> {
        let quota = source.quota_declaration();
        if !complete_shared_quota_declaration(quota) {
            return Ok(BeaDoctorActivationState::Unavailable(
                BeaUnavailableDto::invalid_quota(quota),
            ));
        }
        let doctor = source
            .doctor(
                authority,
                provider_dataset,
                doctor_deadline,
                cancellation.clone(),
            )
            .await?;
        let doctor_receipt_digest = doctor.receipt().receipt_digest();

        let (pending, seal_request) = doctor.into_sealing_parts()?;
        // A completed provider response is already evidence. Bound its physical seal by the
        // operation deadline, but do not discard it merely because outer cancellation raced the
        // completed response.
        let raw_seal = CancellationToken::new();
        let sealed = self
            .research
            .seal_provider_capture(seal_request, &raw_seal, seal_deadline)
            .await?;
        let sealed = pending.try_rejoin_for_publication(source.source_binding(), sealed)?;
        let discovery = DiscoveryRequest::try_new(
            provider_dataset.clone(),
            None,
            NonZeroU16::MIN,
            publication_deadline,
        )?;
        let (admission, discovery, refresh) = source.activate_sealed_doctor_for_publication(
            authority,
            discovery,
            sealed,
            &cancellation,
        )?;
        if admission.quota_declaration_digest() != quota.declaration_digest()
            || admission.doctor_receipt_digest() != doctor_receipt_digest
        {
            return Err(BeaMacroApplicationError::DoctorAuthorityMismatch);
        }
        Ok(BeaDoctorActivationState::Available(
            BeaDoctorActivationDto {
                admission,
                discovery,
                refresh,
            },
        ))
    }

    /// Commits one reservation-bound BEA Regional plan and proves exact restart readability.
    async fn commit_prepared_candidate(
        &self,
        prepared: BeaPreparedRegionalMacroPlan,
        persist_reservation: IngestReservation,
        application_precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<BeaMacroPlanPublication, BeaMacroApplicationError> {
        application_precommit_authority.validate_precommit()?;
        let BeaPreparedRegionalMacroPlan {
            publication_input, ..
        } = prepared;
        let pending = self
            .research
            .analytical()
            .prepare_provider_macro_plan_publication(persist_reservation, publication_input)?;
        let receipt = pending
            .commit(
                self.research.analytical(),
                cancellation,
                application_precommit_authority,
            )
            .await?;
        let restart_selector = receipt.restart_selector();
        let reopened = self
            .research
            .analytical()
            .verify_provider_macro_plan_restart(&restart_selector)?;
        if reopened.manifest() != receipt.manifest() {
            return Err(BeaMacroApplicationError::RestartVerificationMismatch);
        }
        Ok(BeaMacroPlanPublication { receipt, reopened })
    }

    /// Reopens one exact generation and performs the fixed provider-period PIT read.
    ///
    /// This boundary accepts only [`ResearchPeriod`]. It never converts a provider period to a
    /// calendar date and never substitutes a latest manifest.
    pub(crate) async fn read_provider_period_latest_known(
        &self,
        request: BeaProviderPeriodLatestKnownRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<BeaMacroCapabilityState, BeaMacroApplicationError> {
        let BeaProviderPeriodLatestKnownRequest {
            restart_selector,
            analytical,
        } = request;
        let reopened = self
            .research
            .analytical()
            .verify_provider_macro_plan_restart(&restart_selector)?;
        if reopened.manifest() != restart_selector.manifest()
            || analytical.manifest() != restart_selector.manifest()
            || analytical.source_series().source_id() != restart_selector.source_id()
        {
            return Err(BeaMacroApplicationError::RestartVerificationMismatch);
        }
        let output = self
            .research
            .analytical_reader()
            .read_macro_provider_period_latest_known_snapshot(
                analytical,
                limits,
                deadline,
                cancellation,
            )
            .await?;
        if output.source_id() != restart_selector.source_id()
            || output.output().manifest() != restart_selector.manifest()
        {
            return Err(BeaMacroApplicationError::InvalidReadResult);
        }
        Ok(BeaMacroCapabilityState::Available(
            BeaProviderPeriodLatestKnownDto {
                restart_selector,
                reopened,
                output,
            },
        ))
    }
}

impl ProductionResearchIngestCoordinator {
    /// Reopens one exact BEA generation through the application-owned research service.
    ///
    /// This is deliberately credential-free: restart consumers supply only the immutable
    /// selector and fixed typed point-in-time request assembled by the provider activation lane.
    pub(crate) async fn read_bea_provider_period_latest_known(
        &self,
        request: BeaProviderPeriodLatestKnownRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<BeaMacroCapabilityState, BeaMacroApplicationError> {
        BeaMacroApplicationClosure::new(Arc::clone(&self.research))
            .read_provider_period_latest_known(request, limits, deadline, cancellation)
            .await
    }
}

/// Callable BEA Regional source through exact raw sealing, immutable publication, and PIT read.
pub(crate) struct BeaRegionalLiveRuntime {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    closure: BeaMacroApplicationClosure,
    source: Arc<BeaSource>,
    generation: ResearchProviderRuntimeGeneration,
}

impl BeaRegionalLiveRuntime {
    /// Runs one complete bounded Regional producer-to-consumer journey.
    ///
    /// Metadata and data requests use only the registry-minted extraction authority. The sealed
    /// doctor graph is consumed directly as the publication graph, so one bounded provider
    /// acquisition authorizes exactly one publication. Canonicalization remains adapter-owned, so
    /// provider publication, effective, availability, and receipt clocks pass through unchanged.
    /// Success is returned
    /// only after the exact immutable manifest reopens and its provider-period PIT read completes.
    pub(crate) async fn publish_and_read(
        &self,
        request: BeaRegionalLiveRequest,
        context: &RequestContext,
    ) -> Result<BeaRegionalLiveOutcome, BeaLivePublicationError> {
        validate_source_generation(self.source.as_ref(), &self.generation)?;
        validate_regional_dataset(self.source.as_ref(), &request.provider_dataset)?;
        let operation = self
            .coordinator
            .acquire_provider_macro_operation(&self.generation, &request.provider_dataset, context)
            .await?;
        validate_operation_generation(
            self.source.as_ref(),
            &self.generation,
            &request.provider_dataset,
            &operation,
        )?;
        let cancellation = operation.cancellation().clone();
        ensure_not_cancelled(&cancellation)?;
        let provider_deadline = operation.provider_deadline()?;
        let doctor_deadline = request.doctor_deadline.min(provider_deadline);
        let acquisition_deadline = request.acquisition_deadline.min(provider_deadline);
        let seal_deadline = request.seal_deadline.min(operation.operation_deadline());

        let doctor = self
            .closure
            .acquire_seal_and_activate_doctor(
                self.source.as_ref(),
                &operation.extraction(),
                &request.provider_dataset,
                doctor_deadline,
                acquisition_deadline,
                seal_deadline,
                cancellation.clone(),
            )
            .await?;
        let BeaDoctorActivationState::Available(doctor) = doctor else {
            return Err(BeaLivePublicationError::DoctorUnavailable);
        };
        if doctor.quota_declaration_digest() != self.source.quota_declaration().declaration_digest()
        {
            return Err(BeaLivePublicationError::SourceGenerationMismatch);
        }
        let (admission, refresh) = doctor.into_publication_parts();
        operation.ensure_live()?;
        ensure_not_cancelled(&cancellation)?;
        operation.ensure_live()?;
        ensure_not_cancelled(&cancellation)?;
        let object = match admission.batch().objects() {
            [object] => object.clone(),
            _ => return Err(BeaLivePublicationError::CandidateMismatch),
        };
        let extraction = ExtractionRequest::try_new(
            object,
            request.maximum_records,
            request.maximum_canonical_bytes,
            acquisition_deadline,
        )?;
        let candidate = self.source.extract_sealed_discovery(
            operation.extraction(),
            extraction,
            admission,
            cancellation.clone(),
        )?;
        validate_candidate_authority(
            self.source.as_ref(),
            &self.generation,
            &request.provider_dataset,
            &candidate,
        )?;
        let prepared = BeaPreparedRegionalMacroPlan::try_from_candidate(candidate)?;
        if prepared.source_id() != self.generation.metadata().source_id()
            || prepared.provider_dataset() != &request.provider_dataset
            || prepared.source_binding_digest() != self.source.source_binding().binding_digest()
        {
            return Err(BeaLivePublicationError::CandidateMismatch);
        }
        operation.ensure_live()?;
        let publication_digest = prepared.publication_digest();
        let published_series = prepared.published_series();
        let series_allowlist = prepared.series_allowlist().clone();
        let observed_at = system_timestamp()?;
        let reservation = reserve_publication(
            self.coordinator.as_ref(),
            &self.generation,
            &operation,
            prepared.analytical_dataset(),
            &request.provider_dataset,
            publication_digest,
            observed_at,
        )
        .await?;
        let source_binding_digest = prepared.source_binding_digest();
        let publication = self
            .closure
            .commit_prepared_candidate(
                prepared,
                reservation,
                operation.publication_authority(),
                cancellation.clone(),
            )
            .await?;
        operation.ensure_live()?;
        ensure_not_cancelled(&cancellation)?;

        let read_request = BeaProviderPeriodLatestKnownRequest::try_new(
            publication.restart_selector(),
            series_allowlist.clone(),
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
        let BeaMacroCapabilityState::Available(read) = state else {
            return Err(BeaLivePublicationError::ReadUnavailable);
        };
        if read.restart_selector().manifest() != publication.receipt().manifest()
            || read.source_id() != self.generation.metadata().source_id()
        {
            return Err(BeaLivePublicationError::RestartMismatch);
        }
        Ok(BeaRegionalLiveOutcome {
            source_binding_digest,
            doctor_refresh: refresh,
            published_series,
            series_allowlist,
            publication_digest,
            publication,
            read,
        })
    }
}

impl std::fmt::Debug for BeaRegionalLiveRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BeaRegionalLiveRuntime")
            .field("profile", self.generation.profile())
            .field("source_id", self.generation.metadata().source_id())
            .field("metadata_revision", self.generation.metadata().revision())
            .field(
                "configured_regional_contracts",
                &self.source.config().contracts().len(),
            )
            .finish_non_exhaustive()
    }
}

/// Reservation-ready, non-cloneable BEA Regional publication plan.
#[derive(Debug)]
struct BeaPreparedRegionalMacroPlan {
    analytical_dataset: DatasetId,
    provider_dataset: SourceIdentifier,
    source_binding_digest: EvidenceDigest,
    published_series: usize,
    series_allowlist: AnalyticalMacroSeriesAllowlist,
    publication_input: ProviderMacroPlanPublicationInput,
}

impl BeaPreparedRegionalMacroPlan {
    fn try_from_candidate(
        candidate: BeaPublicationCandidate,
    ) -> Result<Self, BeaMacroApplicationError> {
        candidate.validate()?;
        let mut series = candidate
            .observations()
            .iter()
            .map(|observation| observation.observation().series().clone())
            .collect::<Vec<_>>();
        series.sort_unstable();
        series.dedup();
        let published_series = series.len();
        // The provider-neutral snapshot contract intentionally bounds one focused read to 32
        // series. Publication still retains every admitted row from the selected BEA contract.
        series.truncate(32);
        let series_allowlist =
            AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(series)?;
        let coordinates = candidate.rejoin_coordinates().clone();
        let analytical_dataset = DatasetId::try_from(coordinates.analytical_dataset_id().as_str())
            .map_err(|_error| IngestError::InvalidProviderMacroPlan)?;
        let provider_dataset = coordinates.dataset_id().clone();
        let source_binding_digest = coordinates.source_binding_digest();
        let publication_input = try_into_provider_macro_plan_publication_input(candidate)?;
        if publication_input.source_id() != coordinates.source_id()
            || publication_input.metadata_revision() != coordinates.metadata_revision()
            || publication_input.provider_dataset() != &provider_dataset
            || publication_input.source_generation_digest() != source_binding_digest
            || publication_input.total_rows() != coordinates.row_count()
        {
            return Err(IngestError::InvalidProviderMacroPlan.into());
        }
        Ok(Self {
            analytical_dataset,
            provider_dataset,
            source_binding_digest,
            published_series,
            series_allowlist,
            publication_input,
        })
    }

    const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_input.publication_digest()
    }

    const fn source_id(&self) -> &SourceId {
        self.publication_input.source_id()
    }

    const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    const fn source_binding_digest(&self) -> EvidenceDigest {
        self.source_binding_digest
    }

    const fn analytical_dataset(&self) -> &DatasetId {
        &self.analytical_dataset
    }

    const fn series_allowlist(&self) -> &AnalyticalMacroSeriesAllowlist {
        &self.series_allowlist
    }

    const fn published_series(&self) -> usize {
        self.published_series
    }
}

/// Exact immutable BEA Regional generation and provider-period rows from the live journey.
#[derive(Debug)]
pub(crate) struct BeaRegionalLiveOutcome {
    source_binding_digest: EvidenceDigest,
    doctor_refresh: BeaDoctorRefreshDisposition,
    published_series: usize,
    series_allowlist: AnalyticalMacroSeriesAllowlist,
    publication_digest: EvidenceDigest,
    publication: BeaMacroPlanPublication,
    read: BeaProviderPeriodLatestKnownDto,
}

impl BeaRegionalLiveOutcome {
    /// Returns the exact non-secret source/configuration/credential/quota binding.
    pub(crate) const fn source_binding_digest(&self) -> EvidenceDigest {
        self.source_binding_digest
    }

    /// Returns whether metadata admission was reused, activated, expired, or drift-refreshed.
    pub(crate) const fn doctor_refresh(&self) -> BeaDoctorRefreshDisposition {
        self.doctor_refresh
    }

    /// Returns the exact bounded series selection used by the typed restart-safe read.
    pub(crate) const fn series_allowlist(&self) -> &AnalyticalMacroSeriesAllowlist {
        &self.series_allowlist
    }

    /// Returns distinct series retained in the immutable publication before focused read bounds.
    pub(crate) const fn published_series(&self) -> usize {
        self.published_series
    }

    /// Returns the exact payload identity bound into the persist reservation.
    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    /// Returns the immutable generation proven reopenable immediately after commit.
    pub(crate) const fn publication(&self) -> &BeaMacroPlanPublication {
        &self.publication
    }

    /// Returns exact provider-period rows selected from the reopened immutable generation.
    pub(crate) const fn read(&self) -> &BeaProviderPeriodLatestKnownDto {
        &self.read
    }
}

fn validate_source_generation(
    source: &BeaSource,
    generation: &ResearchProviderRuntimeGeneration,
) -> Result<(), BeaLivePublicationError> {
    let binding = source.source_binding();
    if source.metadata() != generation.metadata()
        || binding.source_id() != generation.metadata().source_id()
        || binding.metadata_revision() != generation.metadata().revision()
        || binding.credential_generation_digest() != generation.generation_digest()?
        || binding.quota_declaration_digest() != source.quota_declaration().declaration_digest()
        || !generation
            .metadata()
            .is_effective_at(generation.authority_effective_at())
        || !complete_shared_quota_declaration(source.quota_declaration())
        || source.config().contracts().iter().any(|contract| {
            !contract
                .provider_dataset()
                .as_str()
                .eq_ignore_ascii_case("Regional")
        })
    {
        return Err(BeaLivePublicationError::SourceGenerationMismatch);
    }
    Ok(())
}

fn validate_regional_dataset(
    source: &BeaSource,
    provider_dataset: &SourceIdentifier,
) -> Result<(), BeaLivePublicationError> {
    let contract = source
        .config()
        .contracts()
        .iter()
        .find(|contract| contract.dataset_id() == provider_dataset)
        .ok_or(BeaLivePublicationError::RegionalContractRequired)?;
    if !contract
        .provider_dataset()
        .as_str()
        .eq_ignore_ascii_case("Regional")
    {
        return Err(BeaLivePublicationError::RegionalContractRequired);
    }
    Ok(())
}

fn validate_operation_generation(
    source: &BeaSource,
    generation: &ResearchProviderRuntimeGeneration,
    provider_dataset: &SourceIdentifier,
    operation: &ProviderMacroOperationAuthority,
) -> Result<(), BeaLivePublicationError> {
    operation.ensure_live()?;
    validate_regional_dataset(source, provider_dataset)?;
    if operation.generation() != generation
        || source.metadata() != operation.generation().metadata()
        || source.source_binding().source_id() != operation.generation().metadata().source_id()
        || source.source_binding().metadata_revision()
            != operation.generation().metadata().revision()
    {
        return Err(BeaLivePublicationError::SourceGenerationMismatch);
    }
    Ok(())
}

fn validate_candidate_authority(
    source: &BeaSource,
    generation: &ResearchProviderRuntimeGeneration,
    provider_dataset: &SourceIdentifier,
    candidate: &BeaPublicationCandidate,
) -> Result<(), BeaLivePublicationError> {
    candidate.validate()?;
    let coordinates = candidate.rejoin_coordinates();
    let analytical_dataset = source.analytical_dataset_identifier(provider_dataset)?;
    if coordinates.source_id() != generation.metadata().source_id()
        || coordinates.metadata_revision() != generation.metadata().revision()
        || coordinates.dataset_id() != provider_dataset
        || !coordinates
            .provider_dataset()
            .as_str()
            .eq_ignore_ascii_case("Regional")
        || coordinates.analytical_dataset_id() != &analytical_dataset
        || coordinates.source_binding_digest() != source.source_binding().binding_digest()
        || coordinates.row_count() == 0
    {
        return Err(BeaLivePublicationError::CandidateMismatch);
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "generation, operation, immutable target, provider contract, and payload identity remain explicit"
)]
async fn reserve_publication(
    coordinator: &ProductionResearchIngestCoordinator,
    generation: &ResearchProviderRuntimeGeneration,
    operation: &ProviderMacroOperationAuthority,
    analytical_dataset: &DatasetId,
    provider_dataset: &SourceIdentifier,
    publication_digest: EvidenceDigest,
    observed_at: Timestamp,
) -> Result<IngestReservation, BeaLivePublicationError> {
    let identity = IngestIdentity::try_new(
        generation.metadata().source_id().clone(),
        publication_digest,
        SourceOperation::Persist,
        regional_ingest_identity(
            generation,
            analytical_dataset,
            provider_dataset,
            publication_digest,
        )?,
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

fn regional_ingest_identity(
    generation: &ResearchProviderRuntimeGeneration,
    analytical_dataset: &DatasetId,
    provider_dataset: &SourceIdentifier,
    publication_digest: EvidenceDigest,
) -> Result<String, BeaLivePublicationError> {
    use sha2::Sha256;

    let mut digest = Sha256::new();
    digest.update(b"market-squawk/bea-regional-plan-ingest/v1\0");
    update_digest_field(&mut digest, generation.profile().as_str().as_bytes())?;
    update_digest_field(
        &mut digest,
        generation.metadata().source_id().as_str().as_bytes(),
    )?;
    update_digest_field(&mut digest, provider_dataset.as_str().as_bytes())?;
    update_digest_field(&mut digest, analytical_dataset.as_str().as_bytes())?;
    digest.update(generation.generation_digest()?.bytes());
    digest.update(publication_digest.bytes());
    Ok(format!("bea-regional-v1-{:x}", digest.finalize()))
}

fn update_digest_field(
    digest: &mut sha2::Sha256,
    value: &[u8],
) -> Result<(), BeaLivePublicationError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_error| BeaLivePublicationError::Capacity)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}

fn system_timestamp() -> Result<Timestamp, BeaLivePublicationError> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_error| BeaLivePublicationError::TrustedTimeUnavailable)?;
    let nanos = i64::try_from(elapsed.as_nanos())
        .map_err(|_error| BeaLivePublicationError::TrustedTimeUnavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<(), BeaLivePublicationError> {
    if cancellation.is_cancelled() {
        Err(BeaLivePublicationError::Cancelled)
    } else {
        Ok(())
    }
}

fn complete_shared_quota_declaration(declaration: &BeaProviderQuotaDeclaration) -> bool {
    let requirements = declaration.required_shared_settlements();
    requirements.as_slice()
        == [
            BeaRequiredSharedSettlement::ResponseBytes,
            BeaRequiredSharedSettlement::ProviderErrors,
        ]
        && declaration.shared_declaration().validate().is_ok()
}

/// Consumes one BEA canonical/native/raw handoff into the shared atomic macro-plan input.
fn try_into_provider_macro_plan_publication_input(
    candidate: BeaPublicationCandidate,
) -> Result<ProviderMacroPlanPublicationInput, IngestError> {
    candidate
        .validate()
        .map_err(|_error| IngestError::InvalidProviderMacroPlan)?;
    let coordinates = candidate.rejoin_coordinates().clone();
    let completion_digest = candidate.candidate_digest();
    let expected_total_rows = coordinates.row_count();
    let analytical_dataset = DatasetId::try_from(coordinates.analytical_dataset_id().as_str())
        .map_err(|_error| IngestError::InvalidProviderMacroPlan)?;
    let (_, revisions, sealed_capture) = candidate.into_shared_publication_parts().into_parts();
    let native_lineage = sealed_capture.native_lineage();
    if native_lineage.schema().implementation()
        != ProviderNativeLineageImplementation::BeaRegionalV1
    {
        return Err(IngestError::InvalidProviderMacroPlan);
    }
    let sidecar = native_lineage
        .batch_sidecar()
        .ok_or(IngestError::InvalidProviderMacroPlan)?;
    let semantics_schema = SourceIdentifier::try_from(BEA_PROVIDER_SEMANTICS_SCHEMA)
        .map_err(|_error| IngestError::InvalidProviderMacroPlan)?;
    let semantics = ProviderMacroPlanSemantics::try_new(
        semantics_schema,
        native_lineage.schema().fingerprint(),
        sidecar.semantic_payload_digest(),
        sidecar.semantic_payload().to_vec().into_boxed_slice(),
    )?;
    let chunk = ProviderMacroPlanChunkInput::try_new(
        0,
        1,
        coordinates.candidate_digest(),
        coordinates.source_binding_digest(),
        semantics,
        sealed_capture,
        revisions,
    )?;
    ProviderMacroPlanPublicationInput::try_new(
        analytical_dataset,
        completion_digest,
        expected_total_rows,
        vec![chunk],
    )
}

/// Protected doctor result after shared settlement, physical sealing, and process activation.
#[derive(Debug)]
pub(crate) enum BeaDoctorActivationState {
    /// The exact doctor admission is active without retaining its protected credential.
    Available(BeaDoctorActivationDto),
    /// The code-owned shared weighted declaration is invalid.
    Unavailable(BeaUnavailableDto),
}

/// Bounded non-secret doctor activation evidence.
#[derive(Debug)]
pub(crate) struct BeaDoctorActivationDto {
    admission: Arc<BeaDoctorAdmissionEvidence>,
    discovery: BeaSealedDiscoveryAdmission,
    refresh: BeaDoctorRefreshDisposition,
}

impl BeaDoctorActivationDto {
    /// Returns the exact sealed process activation used by subsequent adapter operations.
    pub(crate) fn admission(&self) -> &Arc<BeaDoctorAdmissionEvidence> {
        &self.admission
    }

    /// Returns the complete request/byte/error policy identity that was settled.
    pub(crate) fn quota_declaration_digest(&self) -> EvidenceDigest {
        self.admission.quota_declaration_digest()
    }

    /// Returns the exact successful page/byte receipt bound to consuming shared settlements.
    pub(crate) fn doctor_receipt_digest(&self) -> EvidenceDigest {
        self.admission.doctor_receipt_digest()
    }

    /// Consumes the linear sealed observations into their sole publication continuation.
    pub(crate) fn into_publication_parts(
        self,
    ) -> (BeaSealedDiscoveryAdmission, BeaDoctorRefreshDisposition) {
        (self.discovery, self.refresh)
    }
}

/// Exact immutable BEA macro-plan generation proven readable after commit.
#[derive(Debug)]
pub(crate) struct BeaMacroPlanPublication {
    receipt: ProviderMacroPlanPublicationReceipt,
    reopened: PinnedDataset,
}

impl BeaMacroPlanPublication {
    /// Returns the atomic publication receipt.
    pub(crate) const fn receipt(&self) -> &ProviderMacroPlanPublicationReceipt {
        &self.receipt
    }

    /// Returns the exact immutable generation reopened immediately after commit.
    pub(crate) const fn reopened(&self) -> &PinnedDataset {
        &self.reopened
    }

    /// Reconstructs the only selector accepted by restart and typed PIT reads.
    pub(crate) fn restart_selector(&self) -> ProviderMacroPlanRestartSelector {
        self.receipt.restart_selector()
    }
}

/// Exact generation-bound request for the fixed BEA provider-period operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BeaProviderPeriodLatestKnownRequest {
    restart_selector: ProviderMacroPlanRestartSelector,
    analytical: AnalyticalMacroProviderPeriodLatestKnownRequest,
}

impl BeaProviderPeriodLatestKnownRequest {
    /// Pins a source-qualified allowlist and exact provider-period cutoffs to one generation.
    pub(crate) fn try_new(
        restart_selector: ProviderMacroPlanRestartSelector,
        series_allowlist: AnalyticalMacroSeriesAllowlist,
        knowledge_cutoff: Timestamp,
        effective_period_cutoff: ResearchPeriod,
    ) -> Result<Self, BeaMacroApplicationError> {
        let source_series = AnalyticalMacroSourceQualifiedSeries::new(
            restart_selector.source_id().clone(),
            series_allowlist,
        );
        let analytical = AnalyticalMacroProviderPeriodLatestKnownRequest::try_new(
            restart_selector.manifest().clone(),
            source_series,
            knowledge_cutoff,
            effective_period_cutoff,
        )?;
        Ok(Self {
            restart_selector,
            analytical,
        })
    }

    /// Returns the fixed application operation identity.
    pub(crate) const fn operation_identity(&self) -> &'static str {
        BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION
    }

    /// Returns the exact immutable selector retained by this request.
    pub(crate) const fn restart_selector(&self) -> &ProviderMacroPlanRestartSelector {
        &self.restart_selector
    }

    /// Returns the fixed typed analytical request; no SQL or physical path is exposed.
    pub(crate) const fn analytical_request(
        &self,
    ) -> &AnalyticalMacroProviderPeriodLatestKnownRequest {
        &self.analytical
    }

    /// Returns the minimum bounded row envelope needed to retain tied revisions.
    pub(crate) fn required_query_rows(&self) -> u64 {
        self.analytical.required_query_rows()
    }
}

/// Fixed BEA operation state suitable for later Desktop, CLI, and MCP registration.
#[derive(Debug)]
pub(crate) enum BeaMacroCapabilityState {
    /// An exact generation-bound provider-period read completed.
    Available(BeaProviderPeriodLatestKnownDto),
    /// Protected source construction or doctor activation is still required.
    SetupRequired(BeaSetupRequiredDto),
    /// A shared authority or exact immutable generation is unavailable.
    Unavailable(BeaUnavailableDto),
}

impl BeaMacroCapabilityState {
    /// Returns the successful typed output, when available.
    pub(crate) const fn available(&self) -> Option<&BeaProviderPeriodLatestKnownDto> {
        match self {
            Self::Available(value) => Some(value),
            Self::SetupRequired(_) | Self::Unavailable(_) => None,
        }
    }

    /// Returns the bounded setup requirement, when user/setup action is needed.
    pub(crate) const fn setup_required(&self) -> Option<&BeaSetupRequiredDto> {
        match self {
            Self::SetupRequired(value) => Some(value),
            Self::Available(_) | Self::Unavailable(_) => None,
        }
    }

    /// Returns the explicit infrastructure/data blocker, when unavailable.
    pub(crate) const fn unavailable(&self) -> Option<&BeaUnavailableDto> {
        match self {
            Self::Unavailable(value) => Some(value),
            Self::Available(_) | Self::SetupRequired(_) => None,
        }
    }
}

/// Closed user-actionable setup requirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BeaSetupRequiredDto {
    kind: BeaSetupRequiredKind,
}

impl BeaSetupRequiredDto {
    /// Returns the exact missing setup step without exposing credential values.
    pub(crate) const fn kind(&self) -> BeaSetupRequiredKind {
        self.kind
    }
}

/// Closed BEA setup states.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BeaSetupRequiredKind {
    /// A protected BEA credential/source instance has not been activated.
    ProtectedCredential,
    /// The configured source has not completed a current sealed doctor activation.
    DoctorActivation,
}

/// Bounded non-user-actionable unavailable result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BeaUnavailableDto {
    reason: BeaUnavailableReason,
    quota_declaration_digest: Option<EvidenceDigest>,
}

impl BeaUnavailableDto {
    fn invalid_quota(declaration: &BeaProviderQuotaDeclaration) -> Self {
        Self {
            reason: BeaUnavailableReason::InvalidQuotaDeclaration,
            quota_declaration_digest: Some(declaration.declaration_digest()),
        }
    }

    /// Returns the exact closed blocker.
    pub(crate) const fn reason(&self) -> BeaUnavailableReason {
        self.reason
    }

    /// Returns the BEA declaration identity when its shared weighted contract is invalid.
    pub(crate) const fn quota_declaration_digest(&self) -> Option<EvidenceDigest> {
        self.quota_declaration_digest
    }
}

/// Closed reasons the fixed BEA operation cannot currently run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BeaUnavailableReason {
    /// The adapter declaration does not retain both mandatory BEA settlement dimensions.
    InvalidQuotaDeclaration,
    /// No exact restart selector is available for the fixed read.
    ManifestRequired,
}

/// Typed successful result for the fixed BEA provider-period operation.
#[derive(Debug)]
pub(crate) struct BeaProviderPeriodLatestKnownDto {
    restart_selector: ProviderMacroPlanRestartSelector,
    reopened: PinnedDataset,
    output: AnalyticalMacroProviderPeriodLatestKnownOutput,
}

impl BeaProviderPeriodLatestKnownDto {
    /// Returns the fixed application operation that produced this DTO.
    pub(crate) const fn operation_identity(&self) -> &'static str {
        BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION
    }

    /// Returns the exact selector revalidated immediately before the read.
    pub(crate) const fn restart_selector(&self) -> &ProviderMacroPlanRestartSelector {
        &self.restart_selector
    }

    /// Returns the exact immutable generation reopened immediately before the read.
    pub(crate) const fn reopened(&self) -> &PinnedDataset {
        &self.reopened
    }

    /// Returns exact provider-period Macro rows with revision and clock evidence.
    pub(crate) const fn output(&self) -> &AnalyticalMacroProviderPeriodLatestKnownOutput {
        &self.output
    }

    /// Returns the sole source-rights owner for the fixed selection.
    pub(crate) const fn source_id(&self) -> &SourceId {
        self.output.source_id()
    }
}

/// Failure before or during BEA physical sealing, atomic publication, or exact PIT selection.
#[derive(Debug, Error)]
pub(crate) enum BeaMacroApplicationError {
    /// The BEA adapter rejected source, activation, or canonical handoff evidence.
    #[error("BEA adapter rejected application composition")]
    Adapter(#[from] BeaSourceError),
    /// The BEA doctor could not rejoin its exact physical seal.
    #[error("BEA doctor physical seal could not be rejoined")]
    Doctor(#[from] market_squawk_adapter_bea::BeaDoctorError),
    /// The BEA canonical publication candidate is invalid.
    #[error("BEA canonical publication candidate is invalid")]
    Publication(#[from] BeaPublicationError),
    /// Registry-authorized BEA acquisition or shared weighted response settlement failed.
    #[error("BEA registry-authorized acquisition failed")]
    Extraction(#[from] ExtractionSourceError),
    /// The application-owned research service rejected physical capture sealing.
    #[error("BEA application-owned physical capture sealing failed")]
    ResearchService(#[from] ResearchServiceError),
    /// Shared atomic publication, reservation, commit, or restart verification failed.
    #[error("BEA atomic macro publication failed")]
    Ingest(#[from] IngestError),
    /// The exact provider-period analytical capability rejected the bounded read.
    #[error("BEA provider-period analytical read failed")]
    AnalyticalRead(#[from] AnalyticalReadError),
    /// Exact publication evidence did not reopen the same immutable generation.
    #[error("BEA exact restart verification changed generation identity")]
    RestartVerificationMismatch,
    /// Doctor sealing changed quota or successful-response receipt identity.
    #[error("BEA doctor activation changed shared authority evidence")]
    DoctorAuthorityMismatch,
    /// The typed read did not retain the exact source and immutable generation.
    #[error("BEA provider-period read returned invalid binding evidence")]
    InvalidReadResult,
}

/// Failure in the exact registered BEA Regional producer-to-consumer path.
#[derive(Debug, Error)]
pub(crate) enum BeaLivePublicationError {
    /// The BEA adapter rejected source, configuration-generation, or provider evidence.
    #[error("BEA Regional source rejected live publication authority")]
    Adapter(#[from] BeaSourceError),
    /// Existing BEA sealing, publication, restart, or PIT selection failed.
    #[error("BEA Regional publication closure failed")]
    Application(#[from] BeaMacroApplicationError),
    /// The shared provider runtime generation is absent, stale, or revoked.
    #[error("BEA Regional runtime authority is unavailable")]
    Composition(#[from] ResearchIngestCompositionError),
    /// Registry-authorized provider acquisition or shared weighted settlement failed.
    #[error("BEA Regional bounded acquisition failed")]
    Extraction(#[from] ExtractionSourceError),
    /// Discovery or extraction bounds are invalid.
    #[error("BEA Regional extraction contract is invalid")]
    ExtractionContract(#[from] market_squawk_sources::ExtractionError),
    /// A completed provider graph could not rejoin its exact shared physical seal.
    #[error("BEA Regional physical response graph could not be rejoined")]
    Seal(#[from] market_squawk_adapter_bea::BeaSealedAcquisitionError),
    /// The application-owned physical journal rejected the exact response graph.
    #[error("BEA Regional physical capture sealing failed")]
    Research(#[from] ResearchServiceError),
    /// Shared immutable publication or exact restart reconstruction failed.
    #[error("BEA Regional immutable publication failed")]
    Ingest(#[from] IngestError),
    /// Exact source/payload/operation/idempotency identity is not reservation-safe.
    #[error("BEA Regional ingest identity is invalid")]
    IngestIdentity(#[from] market_squawk_data::RightsError),
    /// Current rights, cancellation, deadline, or operation authority is unavailable.
    #[error("BEA Regional operation authority is unavailable")]
    Service(#[from] ServiceError),
    /// Canonical/native candidate validation failed before shared publication.
    #[error("BEA Regional publication candidate is invalid")]
    Publication(#[from] BeaPublicationError),
    /// The concrete source does not match the exact registered credential/configuration generation.
    #[error("BEA Regional source generation does not match application authority")]
    SourceGenerationMismatch,
    /// The requested source contract is absent or is not a BEA Regional contract.
    #[error("an exact code-owned BEA Regional dataset contract is required")]
    RegionalContractRequired,
    /// The sealed provider candidate changed source, contract, generation, or row identity.
    #[error("BEA Regional publication candidate does not match source authority")]
    CandidateMismatch,
    /// The sealed doctor could not establish current process activation.
    #[error("BEA Regional sealed doctor activation is unavailable")]
    DoctorUnavailable,
    /// Exact whole-plan restart and typed-read evidence changed manifest or source.
    #[error("BEA Regional restart or PIT read changed immutable identity")]
    RestartMismatch,
    /// The fixed producer-to-consumer journey did not return an available typed read.
    #[error("BEA Regional provider-period read is unavailable")]
    ReadUnavailable,
    /// A bounded count or identity field exceeds its application representation.
    #[error("BEA Regional request exceeds application capacity")]
    Capacity,
    /// Caller or exact runtime-generation cancellation won the operation.
    #[error("BEA Regional publication was cancelled")]
    Cancelled,
    /// The process wall clock cannot produce a trusted persistence coordinate.
    #[error("BEA Regional publication trusted time is unavailable")]
    TrustedTimeUnavailable,
}
