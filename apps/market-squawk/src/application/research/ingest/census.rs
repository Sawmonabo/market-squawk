//! Census metadata-first acquisition, raw sealing, publication, and quarterly PIT reads.

use std::{
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Instant,
};

use futures_util::future::BoxFuture;
use market_squawk_adapter_census::{
    CENSUS_PROVIDER_SEMANTICS_SCHEMA, CensusPublicationCandidate, CensusSource, CensusSourceError,
    CensusSourceTelemetry,
};
use market_squawk_data::{
    AnalyticalMacroProviderPeriodLatestKnownOutput,
    AnalyticalMacroProviderPeriodLatestKnownRequest, AnalyticalMacroSeriesAllowlist,
    AnalyticalMacroSourceQualifiedSeries, AnalyticalReadError, DatasetId, IngestError,
    IngestIdentity, PinnedDataset, ProviderMacroPlanChunkInput, ProviderMacroPlanPublicationInput,
    ProviderMacroPlanPublicationReceipt, ProviderMacroPlanRestartSelector,
    ProviderMacroPlanSemantics, QueryLimits, SourceOperation,
};
use market_squawk_domain::{EvidenceDigest, ResearchPeriod, SourceId, SourceIdentifier, Timestamp};
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
    ProviderMacroOperationAuthority, ResearchIngestCompositionError,
    ResearchProviderRuntimeGeneration, ResearchRevisionPlanError, encode_hex,
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
        let observed_at = candidate.plan().prepared_at();
        let source_id = candidate.source_id().clone();
        let series = candidate
            .plan()
            .observations()
            .iter()
            .map(|observation| observation.canonical_series().clone())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let analytical_dataset = DatasetId::try_from(candidate.analytical_dataset().as_str())
            .map_err(|_error| CensusMacroApplicationError::CandidateInvalid)?;
        let publication_input = census_macro_plan_input(
            candidate,
            analytical_dataset.clone(),
            self.generation.generation_digest()?,
        )?;
        let publication_digest = publication_input.publication_digest();
        let identity = IngestIdentity::try_new(
            source_id.clone(),
            publication_digest,
            SourceOperation::Persist,
            format!(
                "census-macro-plan-v1-{}",
                encode_hex(publication_digest.bytes())
            ),
        )
        .map_err(|_error| CensusMacroApplicationError::CandidateInvalid)?;
        let rights = operation.rights_decision(publication_digest, observed_at)?;
        let reservation = self
            .coordinator
            .research
            .analytical()
            .reserve_source_ingest(
                self.generation.metadata(),
                self.generation.authority_effective_at(),
                rights,
                &identity,
                operation.cancellation(),
            )
            .await?;
        let pending = self
            .coordinator
            .research
            .analytical()
            .prepare_provider_macro_plan_publication(reservation, publication_input)?;
        let receipt = pending
            .commit(
                self.coordinator.research.analytical(),
                operation.cancellation().clone(),
                operation.publication_authority(),
            )
            .await?;
        let restart = CensusRestartSelector::try_reopen(
            self.coordinator.research.as_ref(),
            receipt.restart_selector(),
            &source_id,
        )?;
        if restart.manifest() != receipt.manifest() {
            return Err(CensusMacroApplicationError::RestartInvalid);
        }
        Ok(CensusPublicationReceipt {
            receipt,
            restart,
            series,
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

fn census_macro_plan_input(
    candidate: CensusPublicationCandidate,
    analytical_dataset: DatasetId,
    source_generation_digest: EvidenceDigest,
) -> Result<ProviderMacroPlanPublicationInput, CensusMacroApplicationError> {
    let source_id = candidate.source_id().clone();
    let metadata_revision = candidate.metadata_revision().clone();
    let provider_dataset = candidate.provider_dataset().clone();
    let completion_digest = candidate.candidate_digest();
    let expected_rows = u64::try_from(candidate.canonical_record_count())
        .map_err(|_error| CensusMacroApplicationError::CandidateInvalid)?;
    let candidate_digest = candidate.candidate_digest();
    let native_lineage = candidate.native_lineage();
    if native_lineage.schema().implementation()
        != ProviderNativeLineageImplementation::CensusTabularV1
    {
        return Err(CensusMacroApplicationError::CandidateInvalid);
    }
    let sidecar = native_lineage
        .batch_sidecar()
        .ok_or(CensusMacroApplicationError::CandidateInvalid)?;
    let semantics = ProviderMacroPlanSemantics::try_new(
        SourceIdentifier::try_from(CENSUS_PROVIDER_SEMANTICS_SCHEMA)
            .map_err(|_error| CensusMacroApplicationError::CandidateInvalid)?,
        native_lineage.schema().fingerprint(),
        sidecar.semantic_payload_digest(),
        sidecar.semantic_payload().to_vec().into_boxed_slice(),
    )?;
    let (sealed_capture, revisions, _activation) = candidate.try_into_root_publication_parts()?;
    let chunk = ProviderMacroPlanChunkInput::try_new(
        0,
        1,
        candidate_digest,
        source_generation_digest,
        semantics,
        sealed_capture,
        revisions,
    )?;
    let input = ProviderMacroPlanPublicationInput::try_new(
        analytical_dataset,
        completion_digest,
        expected_rows,
        vec![chunk],
    )?;
    if input.source_id() != &source_id
        || input.metadata_revision().as_source_identifier() != &metadata_revision
        || input.provider_dataset() != &provider_dataset
        || input.source_generation_digest() != source_generation_digest
        || input.total_chunks() != 1
        || input.total_rows() != expected_rows
    {
        return Err(CensusMacroApplicationError::CandidateInvalid);
    }
    Ok(input)
}

#[derive(Debug)]
pub(crate) struct CensusPublicationReceipt {
    receipt: ProviderMacroPlanPublicationReceipt,
    restart: CensusRestartSelector,
    series: Box<[SourceIdentifier]>,
    telemetry: CensusSourceTelemetry,
}

impl CensusPublicationReceipt {
    pub(crate) const fn receipt(&self) -> &ProviderMacroPlanPublicationReceipt {
        &self.receipt
    }

    pub(crate) const fn restart_selector(&self) -> &CensusRestartSelector {
        &self.restart
    }

    pub(crate) fn series(&self) -> &[SourceIdentifier] {
        &self.series
    }

    pub(crate) const fn telemetry(&self) -> CensusSourceTelemetry {
        self.telemetry
    }
}

/// Exact atomic Census macro plan reconstructed entirely from the installed catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CensusRestartSelector {
    selector: ProviderMacroPlanRestartSelector,
}

impl CensusRestartSelector {
    pub(crate) fn try_reopen(
        research: &ResearchService,
        selector: ProviderMacroPlanRestartSelector,
        expected_source: &SourceId,
    ) -> Result<Self, CensusMacroApplicationError> {
        if selector.source_id() != expected_source {
            return Err(CensusMacroApplicationError::RestartInvalid);
        }
        let reopened = research
            .analytical()
            .verify_provider_macro_plan_restart(&selector)?;
        if reopened.manifest() != selector.manifest() {
            return Err(CensusMacroApplicationError::RestartInvalid);
        }
        Ok(Self { selector })
    }

    pub(crate) const fn manifest(&self) -> &market_squawk_data::DatasetManifestRef {
        self.selector.manifest()
    }

    fn validate_quarterly_request(
        &self,
        request: &AnalyticalMacroProviderPeriodLatestKnownRequest,
    ) -> Result<(), CensusMacroApplicationError> {
        let cutoff = request.effective_period_cutoff();
        if request.manifest() != self.selector.manifest()
            || request.source_series().source_id() != self.selector.source_id()
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
        let reopened = research
            .analytical()
            .verify_provider_macro_plan_restart(&self.selector)?;
        let output = research
            .analytical_reader()
            .read_macro_provider_period_latest_known_snapshot(
                request.analytical,
                limits,
                deadline,
                cancellation,
            )
            .await?;
        if output.source_id() != self.selector.source_id()
            || output.output().manifest() != self.selector.manifest()
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
        Ok(CensusQuarterlyRestartReceipt { reopened, output })
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
            selector.selector.source_id().clone(),
            series_allowlist,
        );
        let analytical = AnalyticalMacroProviderPeriodLatestKnownRequest::try_new(
            selector.selector.manifest().clone(),
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
    reopened: PinnedDataset,
    output: AnalyticalMacroProviderPeriodLatestKnownOutput,
}

impl CensusQuarterlyRestartReceipt {
    pub(crate) const fn reopened(&self) -> &PinnedDataset {
        &self.reopened
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
    #[error("Census atomic macro publication failed")]
    Ingest(#[from] IngestError),
    #[error("Census runtime generation is invalid")]
    Composition(#[from] ResearchIngestCompositionError),
    #[error("Census application authority is unavailable")]
    Service(#[from] ServiceError),
    #[error("Census application research composition failed")]
    Research(#[from] ResearchServiceError),
    #[error("Census quarterly analytical read failed")]
    AnalyticalRead(#[from] AnalyticalReadError),
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU16, NonZeroU32, NonZeroU64},
        sync::Arc,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    use market_squawk_adapter_census::{
        CensusApiKey, CensusDataQuery, CensusDataset, CensusDatasetContract,
        CensusEffectiveTimePolicy, CensusGeography, CensusGeographyClause, CensusGeographyCode,
        CensusParseLimits, CensusPredicate, CensusPredicateType, CensusSelection,
        CensusSourceConfig, CensusTimePoint, CensusTimePredicate, CensusVariableMapping,
        census_api_endpoint_rules, census_provider_rate_declaration,
    };
    use market_squawk_data::{
        AnalyticalMacroSeriesAllowlist, CatalogConfig, CatalogResultLimits, ObjectStoreConfig,
        QueryLimits, RightsBasis, SqliteProviderRateStore,
    };
    use market_squawk_domain::{
        AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
        DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
        RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceIdentifier,
    };
    use market_squawk_platform::LocalPaths;
    use market_squawk_services::{JsonStructureLimits, RequestContext, RequestId, ServiceLimits};
    use market_squawk_sources::{
        AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, CoverageDomain,
        EndpointPolicy, FreshnessPolicy, HistoricalCapability, HttpRequestBounds,
        NetworkAccessPolicy, ProviderCapabilityRevision, ProviderRateAuthority, SourceCapabilities,
        SourceClass, SourceCoverage, SourceMetadataInput, SourceProtocolProfile,
    };
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::*;
    use crate::application::{ResearchExtractionLimits, ResearchRightsAuthority};

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    #[tokio::test]
    #[ignore = "requires a configured Census API key and performs one bounded live journey"]
    async fn live_qwi_quarter_seals_publishes_reads_and_reopens() -> TestResult {
        let api_key = std::env::var("CENSUS_API_KEY")?;
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path().join("research"))?;
        let now = current_timestamp()?;
        let subject = SourceIdentifier::try_from("census-live-api-key")?;
        let contract = live_qwi_contract()?;
        let provider_dataset = contract.dataset_id().clone();
        let config = CensusSourceConfig::try_new([contract], CensusParseLimits::default())?;
        let metadata = source_metadata(now, subject.clone(), &config)?;
        let source =
            CensusSource::try_new(metadata.clone(), CensusApiKey::try_new(api_key)?, config)?;
        let rights = ResearchRightsAuthority::try_new_scoped(
            metadata.source_id().clone(),
            RightsBasis::reviewed_terms(
                "https://www.census.gov/data/developers/about/terms-of-service.html",
                digest(1),
            )?,
            digest(2),
            digest(3),
            now.checked_add_nanos(300_000_000_000)?,
            vec![subject.clone()],
            vec![SourceOperation::Persist],
        )?;
        let generation = ResearchProviderRuntimeGeneration::try_new(
            SourceIdentifier::try_from("census.data-api.live-qwi")?,
            Uuid::new_v4(),
            ProviderCapabilityRevision::new(1)?,
            digest(4),
            None,
            None,
            now,
            metadata.clone(),
            rights.clone(),
        )?;
        let research = Arc::new(open_research(&paths)?);
        let provider_rate = ProviderRateAuthority::try_new(Arc::new(
            SqliteProviderRateStore::try_open(temporary.path().join("provider-rate.sqlite3"))?,
        ))?;
        provider_rate.bind_authorization_subject(
            metadata.authorization().mode(),
            metadata.authorization().evidence().content_digest(),
            &subject,
        )?;
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
        let composition =
            CensusLiveComposition::try_new(Arc::clone(&coordinator), source, generation.clone())?;
        let (registered, closure) = composition.into_parts();
        mutation.register_provider_source(generation, registered, rights)?;

        let service_deadline = Instant::now() + Duration::from_secs(90);
        let publication = closure
            .acquire_seal_and_publish(
                provider_dataset,
                CensusSealFirstExtractionLimits::new(
                    NonZeroU32::new(8).ok_or("invalid record bound")?,
                    NonZeroU64::new(1024 * 1024).ok_or("invalid byte bound")?,
                ),
                &request_context(service_deadline)?,
            )
            .await?;
        assert_eq!(publication.receipt().total_chunks(), 1);
        assert_eq!(publication.receipt().total_rows(), 1);
        assert_eq!(publication.telemetry().requests(), 5);
        assert_eq!(publication.telemetry().returned_rows(), 1);
        assert_eq!(publication.series().len(), 1);

        let period = ResearchPeriod::try_new(
            SourceIdentifier::try_from(CENSUS_QUARTER_SCHEME)?,
            2023,
            NonZeroU16::new(4).ok_or("invalid quarter")?,
            SourceIdentifier::try_from("2023-Q4")?,
        )?;
        let cutoff = current_timestamp()?.checked_add_nanos(30_000_000_000)?;
        let allowlist = AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(
            publication.series().to_vec(),
        )?;
        let selector = publication.restart_selector().clone();
        let request = CensusQuarterlyPointInTimeRequest::try_new(
            &selector,
            allowlist.clone(),
            cutoff,
            period.clone(),
        )?;
        let first = selector
            .reopen_quarterly(
                research.as_ref(),
                request,
                query_limits()?,
                Instant::now() + Duration::from_secs(15),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(first.output().observations().len(), 1);
        assert_eq!(
            first.reopened().manifest(),
            publication.receipt().manifest()
        );
        let manifest = publication.receipt().manifest().clone();
        let plan_selector = selector.selector.clone();
        let expected_source = metadata.source_id().clone();

        drop(first);
        drop(publication);
        drop(selector);
        drop(closure);
        drop(mutation);
        drop(alpaca);
        drop(coordinator);
        drop(research);

        let reopened = open_research(&paths)?;
        let selector =
            CensusRestartSelector::try_reopen(&reopened, plan_selector, &expected_source)?;
        let request =
            CensusQuarterlyPointInTimeRequest::try_new(&selector, allowlist, cutoff, period)?;
        let restarted = selector
            .reopen_quarterly(
                &reopened,
                request,
                query_limits()?,
                Instant::now() + Duration::from_secs(15),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(restarted.reopened().manifest(), &manifest);
        assert_eq!(restarted.output().observations().len(), 1);
        eprintln!(
            "CENSUS_LIVE_EVIDENCE manifest_version={} rows={} acquisition_requests=5 restart_rows=1",
            manifest.manifest_version(),
            restarted.output().observations().len()
        );
        Ok(())
    }

    fn live_qwi_contract() -> TestResult<CensusDatasetContract> {
        let query = CensusDataQuery::try_new(
            CensusDataset::try_time_series("qwi/sa")?,
            CensusSelection::variables(["Emp"])?,
            vec![
                CensusPredicate::try_new("agegrp", CensusPredicateType::String, ["A00"])?,
                CensusPredicate::try_new("sex", CensusPredicateType::String, ["0"])?,
            ],
            CensusGeography::standard(
                CensusGeographyClause::try_new("state", [CensusGeographyCode::try_new("06")?])?,
                Vec::new(),
            )?,
            Some(CensusTimePredicate::At {
                point: CensusTimePoint::quarter(2023, 4)?,
            }),
        )?;
        Ok(CensusDatasetContract::try_new(
            query,
            [CensusVariableMapping::try_new(
                SourceIdentifier::try_from("Emp")?,
                SourceIdentifier::try_from("macro.employment.beginning-quarter")?,
                SourceIdentifier::try_from("persons")?,
            )?],
            CensusEffectiveTimePolicy::RequireReportedTime,
        )?)
    }

    fn source_metadata(
        now: Timestamp,
        subject: SourceIdentifier,
        config: &CensusSourceConfig,
    ) -> TestResult<market_squawk_sources::SourceMetadata> {
        let evidence = ExactPayloadEvidence::from_content_digest(digest(5));
        let effective = EffectiveInterval::new(now.checked_sub_nanos(1_000_000_000)?, None)?;
        let authorization = AuthorizationGrant::new(
            AuthorizationMode::UserAuthorized,
            AuthorizationBasis::new(subject.clone()),
            evidence.clone(),
            effective,
        );
        let network = EndpointPolicy::try_from_api_rules(
            census_api_endpoint_rules(config)?,
            HttpRequestBounds::default(),
        )?;
        Ok(market_squawk_sources::SourceMetadata::try_new(
            SourceMetadataInput::new(
                SchemaVersion::CURRENT,
                SourceId::try_from("census-qwi-live")?,
                RevisionBoundPayloadEvidence::new(
                    MetadataRevision::new(SourceIdentifier::try_from("census-qwi-live-v1")?),
                    evidence.clone(),
                ),
                SourceClass::OfficialAgency,
                SourceIdentifier::try_from("us-census")?,
                authorization,
                SourceCoverage::try_non_instrument(
                    evidence,
                    effective,
                    CoverageDomain::Macroeconomic,
                    CoverageDelay::Delayed(1),
                    DeliveryEvidence::Unknown,
                )?,
                DataQuality::OfficialDelayed,
                NetworkAccessPolicy::Allowlisted(network),
                FreshnessPolicy::try_new(60, 60, 60, 60, 1)?,
                Some(census_provider_rate_declaration(&subject)?.policy().clone()),
                SourceCapabilities::new(
                    false,
                    true,
                    SequenceCapability::Unsupported,
                    ChecksumCapability::Unsupported,
                    HistoricalCapability::Historical,
                    false,
                ),
                SourceProtocolProfile::NotLive,
            ),
        )?)
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

    fn query_limits() -> TestResult<QueryLimits> {
        Ok(QueryLimits::try_new(
            16,
            1024 * 1024,
            8 * 1024 * 1024,
            8,
            1024,
            1024,
            Duration::from_secs(10),
        )?)
    }

    fn request_context(deadline: Instant) -> TestResult<RequestContext> {
        Ok(RequestContext::new(
            RequestId::String(Arc::from("test.census-live-publication")),
            CancellationToken::new(),
            deadline,
            ServiceLimits::try_new(
                4096,
                8,
                4096,
                8,
                JsonStructureLimits::try_new(16, 4096, 64, 64)?,
            )?,
        ))
    }

    fn current_timestamp() -> TestResult<Timestamp> {
        let nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
        Ok(Timestamp::from_unix_nanos(nanos))
    }

    const fn digest(byte: u8) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
    }
}
