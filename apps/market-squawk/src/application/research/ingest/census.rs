//! Census metadata-first acquisition, raw sealing, publication, and quarterly PIT reads.

use std::{
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Instant,
};

use futures_util::future::BoxFuture;
use market_squawk_adapter_census::{
    CensusPublicationCandidate, CensusSource, CensusSourceError, CensusSourceTelemetry,
};
use market_squawk_data::{
    AnalyticalMacroProviderPeriodLatestKnownOutput,
    AnalyticalMacroProviderPeriodLatestKnownRequest, AnalyticalMacroSeriesAllowlist,
    AnalyticalMacroSourceQualifiedSeries, AnalyticalReadError, CommittedDataset, DatasetId,
    DatasetManifestRef, PersistedProviderCaptureBindingEvidence, QueryLimits,
};
use market_squawk_domain::{ResearchPeriod, SourceId, SourceIdentifier, Timestamp};
use market_squawk_services::{RequestContext, ServiceError};
use market_squawk_sources::{
    DiscoveryBatch, DiscoveryRequest, ExtractionAuthority, ExtractionBatch, ExtractionError,
    ExtractionRequest, ExtractionRevisionPlan, ExtractionSource, ExtractionSourceError,
    ProviderNativeLineageImplementation, SourceMetadata, SourceMetadataProvider,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    ManagedResearchExtractionSource, ProductionResearchIngestCoordinator,
    ProviderMacroOperationAuthority, ProviderMacroPublicationError, ProviderMacroRestartBinding,
    ResearchProviderRuntimeGeneration, ResearchRevisionPlanError,
};
use crate::{ResearchService, ResearchServiceError};

pub(crate) const CENSUS_QUARTERLY_POINT_IN_TIME_OPERATION: &str =
    "Macro.GetCensusQuarterlyPointInTime";

const CENSUS_QUARTER_SCHEME: &str = "census-quarter";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CensusSealFirstExtractionLimits {
    max_records: NonZeroU32,
    max_bytes: NonZeroU64,
}

impl CensusSealFirstExtractionLimits {
    pub(crate) const fn new(max_records: NonZeroU32, max_bytes: NonZeroU64) -> Self {
        Self {
            max_records,
            max_bytes,
        }
    }
}

/// Census producer composition bound to one coordinator-owned runtime generation.
pub(crate) struct CensusMacroApplicationClosure {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    source: Arc<CensusSource>,
    generation: ResearchProviderRuntimeGeneration,
}

/// Same-instance generic registration and typed Census runtime composition.
pub(crate) struct CensusLiveComposition {
    registered_source: CensusRegisteredSource,
    closure: CensusMacroApplicationClosure,
}

impl CensusLiveComposition {
    pub(crate) fn try_new(
        coordinator: Arc<ProductionResearchIngestCoordinator>,
        source: CensusSource,
        generation: ResearchProviderRuntimeGeneration,
    ) -> Result<Self, CensusMacroApplicationError> {
        let plan = source.activation_plan()?;
        if source.metadata() != generation.metadata()
            || plan.source_id() != generation.metadata().source_id()
            || plan.metadata_revision() != generation.metadata().revision().as_source_identifier()
            || !generation
                .metadata()
                .is_effective_at(generation.authority_effective_at())
        {
            return Err(CensusMacroApplicationError::AuthorityInvalid);
        }
        let source = Arc::new(source);
        Ok(Self {
            registered_source: CensusRegisteredSource {
                source: Arc::clone(&source),
            },
            closure: CensusMacroApplicationClosure {
                coordinator,
                source,
                generation,
            },
        })
    }

    pub(crate) fn into_parts(self) -> (CensusRegisteredSource, CensusMacroApplicationClosure) {
        (self.registered_source, self.closure)
    }
}

/// Registry-facing wrapper retaining the exact concrete Census source used by the typed closure.
pub(crate) struct CensusRegisteredSource {
    source: Arc<CensusSource>,
}

impl SourceMetadataProvider for CensusRegisteredSource {
    fn metadata(&self) -> &SourceMetadata {
        self.source.metadata()
    }
}

impl ExtractionSource for CensusRegisteredSource {
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

impl ManagedResearchExtractionSource for CensusRegisteredSource {
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

impl std::fmt::Debug for CensusMacroApplicationClosure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CensusMacroApplicationClosure")
            .field("profile", self.generation.profile())
            .field("source_id", self.generation.metadata().source_id())
            .field("metadata_revision", self.generation.metadata().revision())
            .finish_non_exhaustive()
    }
}

impl CensusMacroApplicationClosure {
    /// Acquires registry extraction, exact rights, cancellation, and publication lease atomically,
    /// then seals every completed Census response before canonical publication.
    pub(crate) async fn acquire_seal_and_publish(
        &self,
        provider_dataset: SourceIdentifier,
        limits: CensusSealFirstExtractionLimits,
        context: &RequestContext,
    ) -> Result<CensusPublicationReceipt, CensusMacroApplicationError> {
        let operation = self
            .coordinator
            .acquire_provider_macro_operation(&self.generation, &provider_dataset, context)
            .await?;
        let census = self.source.as_ref();
        self.validate_source(census, &provider_dataset, &operation)?;
        let authority = operation.extraction();
        let provider_deadline = operation.provider_deadline()?;
        let cancellation = operation.cancellation().clone();

        let doctor = census
            .doctor(&authority, provider_deadline, cancellation.clone())
            .await?;
        let (pending_doctor, doctor_seal) = doctor.into_sealing_parts();
        let raw_seal = CancellationToken::new();
        let sealed_doctor = self
            .coordinator
            .research
            .seal_provider_capture(doctor_seal, &raw_seal, operation.operation_deadline())
            .await?;
        operation.ensure_live()?;
        let activation = census.activation_candidate(pending_doctor, sealed_doctor)?;

        let discovery =
            DiscoveryRequest::try_new(provider_dataset, None, NonZeroU16::MIN, provider_deadline)?;
        let discovered = census
            .discover_with_activation(
                authority.clone(),
                discovery,
                activation,
                cancellation.clone(),
            )
            .await?;
        let (pending_discovery, graph_seal) = discovered.into_sealing_parts();
        let raw_seal = CancellationToken::new();
        let sealed_graph = self
            .coordinator
            .research
            .seal_provider_capture(graph_seal, &raw_seal, operation.operation_deadline())
            .await?;
        operation.ensure_live()?;
        let admission = pending_discovery.try_bind_sealed(sealed_graph)?;
        let extraction = ExtractionRequest::try_new(
            admission.object()?.clone(),
            limits.max_records,
            limits.max_bytes,
            provider_deadline,
        )?;
        let extracted = census
            .extract_sealed_discovery(authority, extraction, admission, cancellation)
            .await?;
        let (candidate, telemetry) = extracted.into_parts();
        self.publish_candidate(candidate, telemetry, &operation)
            .await
    }

    async fn publish_candidate(
        &self,
        candidate: CensusPublicationCandidate,
        telemetry: CensusSourceTelemetry,
        operation: &ProviderMacroOperationAuthority,
    ) -> Result<CensusPublicationReceipt, CensusMacroApplicationError> {
        operation.ensure_live()?;
        candidate.plan().validate()?;
        self.validate_candidate(&candidate, operation)?;
        let analytical_dataset = DatasetId::try_from(candidate.analytical_dataset().as_str())
            .map_err(|_error| CensusMacroApplicationError::CandidateInvalid)?;
        let observed_at = candidate.plan().prepared_at();
        let (binding, revisions, _activation) = candidate.try_into_root_publication_parts()?;
        let publication = operation
            .publish_single_binding(
                analytical_dataset,
                binding,
                revisions,
                ProviderNativeLineageImplementation::CensusTabularV1,
                observed_at,
            )
            .await?;
        let (committed, binding) = publication.into_parts();
        Ok(CensusPublicationReceipt {
            committed,
            restart: CensusRestartSelector { binding },
            telemetry,
        })
    }

    fn validate_source(
        &self,
        census: &CensusSource,
        provider_dataset: &SourceIdentifier,
        operation: &ProviderMacroOperationAuthority,
    ) -> Result<(), CensusMacroApplicationError> {
        operation.ensure_live()?;
        census.analytical_dataset_identifier(provider_dataset)?;
        let plan = census.activation_plan()?;
        if census.metadata() != self.generation.metadata()
            || operation.generation() != &self.generation
            || plan.source_id() != self.generation.metadata().source_id()
            || plan.metadata_revision()
                != self.generation.metadata().revision().as_source_identifier()
            || !plan
                .datasets()
                .iter()
                .any(|dataset| dataset.provider_dataset() == provider_dataset)
        {
            return Err(CensusMacroApplicationError::AuthorityInvalid);
        }
        Ok(())
    }

    fn validate_candidate(
        &self,
        candidate: &CensusPublicationCandidate,
        operation: &ProviderMacroOperationAuthority,
    ) -> Result<(), CensusMacroApplicationError> {
        let observed_at = candidate.plan().prepared_at();
        if operation.generation() != &self.generation
            || candidate.source_id() != self.generation.metadata().source_id()
            || candidate.metadata_revision()
                != self.generation.metadata().revision().as_source_identifier()
            || observed_at < self.generation.authority_effective_at()
            || !self.generation.metadata().is_effective_at(observed_at)
        {
            return Err(CensusMacroApplicationError::AuthorityInvalid);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct CensusPublicationReceipt {
    committed: CommittedDataset,
    restart: CensusRestartSelector,
    telemetry: CensusSourceTelemetry,
}

impl CensusPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &CensusRestartSelector {
        &self.restart
    }

    pub(crate) const fn telemetry(&self) -> CensusSourceTelemetry {
        self.telemetry
    }
}

/// Exact single-binding Census generation reconstructed entirely from the installed catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CensusRestartSelector {
    binding: ProviderMacroRestartBinding,
}

impl CensusRestartSelector {
    pub(crate) fn try_reopen(
        research: &ResearchService,
        manifest: DatasetManifestRef,
        expected_source: &SourceId,
    ) -> Result<Self, CensusMacroApplicationError> {
        Ok(Self {
            binding: ProviderMacroRestartBinding::try_reopen(
                research,
                manifest,
                expected_source,
                ProviderNativeLineageImplementation::CensusTabularV1,
            )?,
        })
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        self.binding.manifest()
    }

    pub(crate) const fn binding_digest(&self) -> market_squawk_domain::EvidenceDigest {
        self.binding.binding_digest()
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        self.binding.provider_dataset()
    }

    fn validate_quarterly_request(
        &self,
        request: &AnalyticalMacroProviderPeriodLatestKnownRequest,
    ) -> Result<(), CensusMacroApplicationError> {
        let cutoff = request.effective_period_cutoff();
        if request.manifest() != self.binding.manifest()
            || request.source_series().source_id() != self.binding.source_id()
            || cutoff.scheme().as_str() != CENSUS_QUARTER_SCHEME
            || cutoff.ordinal().get() > 4
            || cutoff.code().as_str() != format!("{:04}-Q{}", cutoff.year(), cutoff.ordinal())
        {
            return Err(CensusMacroApplicationError::QuarterlySelectionInvalid);
        }
        Ok(())
    }

    pub(crate) async fn reopen_quarterly(
        &self,
        research: &ResearchService,
        request: CensusQuarterlyPointInTimeRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CensusQuarterlyRestartReceipt, CensusMacroApplicationError> {
        self.validate_quarterly_request(&request.analytical)?;
        let evidence = self.binding.evidence(research)?;
        let output = research
            .analytical_reader()
            .read_macro_provider_period_latest_known_snapshot(
                request.analytical,
                limits,
                deadline,
                cancellation,
            )
            .await?;
        if output.source_id() != self.binding.source_id()
            || output.output().manifest() != self.binding.manifest()
            || output.period_scheme().as_str() != CENSUS_QUARTER_SCHEME
            || output.observations().iter().any(|observation| {
                observation
                    .context()
                    .time()
                    .effective()
                    .source_period_value()
                    .is_none_or(|period| period.scheme().as_str() != CENSUS_QUARTER_SCHEME)
            })
        {
            return Err(CensusMacroApplicationError::RestartInvalid);
        }
        Ok(CensusQuarterlyRestartReceipt { evidence, output })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CensusQuarterlyPointInTimeRequest {
    analytical: AnalyticalMacroProviderPeriodLatestKnownRequest,
}

impl CensusQuarterlyPointInTimeRequest {
    pub(crate) fn try_new(
        selector: &CensusRestartSelector,
        series_allowlist: AnalyticalMacroSeriesAllowlist,
        knowledge_cutoff: Timestamp,
        effective_period_cutoff: ResearchPeriod,
    ) -> Result<Self, CensusMacroApplicationError> {
        let source_series = AnalyticalMacroSourceQualifiedSeries::new(
            selector.binding.source_id().clone(),
            series_allowlist,
        );
        let analytical = AnalyticalMacroProviderPeriodLatestKnownRequest::try_new(
            selector.binding.manifest().clone(),
            source_series,
            knowledge_cutoff,
            effective_period_cutoff,
        )?;
        selector.validate_quarterly_request(&analytical)?;
        Ok(Self { analytical })
    }

    pub(crate) const fn operation_identity(&self) -> &'static str {
        CENSUS_QUARTERLY_POINT_IN_TIME_OPERATION
    }

    pub(crate) const fn analytical_request(
        &self,
    ) -> &AnalyticalMacroProviderPeriodLatestKnownRequest {
        &self.analytical
    }

    pub(crate) fn required_query_rows(&self) -> u64 {
        self.analytical.required_query_rows()
    }
}

#[derive(Debug)]
pub(crate) struct CensusQuarterlyRestartReceipt {
    evidence: PersistedProviderCaptureBindingEvidence,
    output: AnalyticalMacroProviderPeriodLatestKnownOutput,
}

impl CensusQuarterlyRestartReceipt {
    pub(crate) const fn evidence(&self) -> &PersistedProviderCaptureBindingEvidence {
        &self.evidence
    }

    pub(crate) const fn output(&self) -> &AnalyticalMacroProviderPeriodLatestKnownOutput {
        &self.output
    }
}

#[derive(Debug, Error)]
pub(crate) enum CensusMacroApplicationError {
    #[error("Census source or application authority does not match")]
    AuthorityInvalid,
    #[error("Census canonical publication candidate is incomplete or ambiguous")]
    CandidateInvalid,
    #[error("Census quarterly point-in-time selection is invalid")]
    QuarterlySelectionInvalid,
    #[error("Census exact restart evidence is invalid")]
    RestartInvalid,
    #[error("Census adapter rejected application composition")]
    Adapter(#[from] CensusSourceError),
    #[error("Census bounded acquisition failed")]
    Extraction(#[from] ExtractionSourceError),
    #[error("Census extraction request is invalid")]
    ExtractionContract(#[from] ExtractionError),
    #[error("Census provider macro publication failed")]
    Publication(#[from] ProviderMacroPublicationError),
    #[error("Census application authority is unavailable")]
    Service(#[from] ServiceError),
    #[error("Census application research composition failed")]
    Research(#[from] ResearchServiceError),
    #[error("Census quarterly analytical read failed")]
    AnalyticalRead(#[from] AnalyticalReadError),
}
