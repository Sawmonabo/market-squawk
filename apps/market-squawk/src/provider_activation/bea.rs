//! Protected BEA Regional activation and fixed durable research operations.
//!
//! The only admitted provider product is the code-owned `Regional` contract below. Credential
//! bytes remain in the onboarding secret lease and the BEA adapter's zeroizing `UserID`; status,
//! restart, and typed output retain only non-secret generation and publication evidence.

use std::{
    collections::BTreeMap,
    fmt,
    num::{NonZeroU32, NonZeroU64},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use market_squawk_adapter_bea::{
    BeaDatasetContract, BeaDatasetIdentity, BeaError, BeaParameterIdentity, BeaParseLimits,
    BeaSource, BeaSourceConfig, BeaSourceError, BeaUserId,
};
use market_squawk_data::{
    AnalyticalMacroSeriesAllowlist, ProviderMacroPlanRestartSelector, QueryError, QueryLimits,
};
use market_squawk_domain::{EvidenceDigest, ResearchPeriod, SourceId, SourceIdentifier, Timestamp};
use market_squawk_services::RequestContext;
use market_squawk_sources::{AuthorizationMode, SourceMetadata};
use tokio_util::sync::CancellationToken;

use super::{
    BeaAdapterActivation, ProviderAdapterActivation, provider_research_rights, runtime_generation,
};
use crate::application::{
    BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION, BeaLivePublicationError, BeaMacroApplicationError,
    BeaMacroCapabilityState, BeaProviderPeriodLatestKnownDto, BeaProviderPeriodLatestKnownRequest,
    BeaRegionalLiveComposition, BeaRegionalLiveOutcome, BeaRegionalLiveRequest,
    BeaRegionalLiveRuntime, ManagedResearchExtractionSource, ResearchIngestCompositionError,
    ResearchProviderRuntimeGeneration, ResearchProviderRuntimeReplacement, ResearchRightsAuthority,
};
use crate::{ProviderActivationLease, ProviderOnboardingError};

pub(crate) const BEA_SURFACE: &str = "bea.api-data";

const REGIONAL_DATASET: &str = "Regional";
pub(crate) const BEA_SOURCE_ID: &str = "us-bea";
const REGIONAL_TABLE: &str = "SAINC1";
const REGIONAL_LINE_CODE: &str = "3";
const REGIONAL_GEO_FIPS: &str = "DE";
const REGIONAL_YEAR_SCOPE: &str = "LAST5";
const BEA_ANNUAL_PERIOD_SCHEME: &str = "bea-annual";

// `GeoFips=DE` returns Delaware state and its regional descendants. The immutable publication
// retains that complete bounded provider response. This identifier selects only the state row
// (`GeoFips=10000`) for the fixed product read. It is the adapter's period-independent canonical
// identity for Regional / SAINC1 / CAINC1-3 / Delaware under canonical-series schema v1.
const DELAWARE_PER_CAPITA_INCOME_SERIES: &str =
    "bea-series:2de222290053ee1046f475d708a1b0d617d7d6011f880b47ac5cb7bdeb455790";

const MAXIMUM_PROVIDER_ROWS: u32 = 4_096;
const MAXIMUM_CANONICAL_BYTES: u64 = 16 * 1_024 * 1_024;
const MAXIMUM_METADATA_RECORDS: usize = 8_192;
const MAXIMUM_STRING_BYTES: usize = 32 * 1_024;
const MAXIMUM_DIMENSIONS: usize = 64;
const MAXIMUM_NOTES: usize = 2_048;
const MAXIMUM_QUERY_ROWS: u64 = 64;
const MAXIMUM_QUERY_BYTES: u64 = 1_024 * 1_024;
const MAXIMUM_QUERY_MEMORY_BYTES: u64 = 8 * 1_024 * 1_024;
const MAXIMUM_QUERY_AST_NODES: usize = 128;
const MAXIMUM_QUERY_PLAN_NODES: usize = 128;
const QUERY_DURATION: Duration = Duration::from_secs(10);

/// Closed lifecycle state for the fixed BEA Regional research product.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BeaProductAvailability {
    /// The code-owned product is selected but no protected runtime is retained.
    Desired,
    /// The exact protected runtime is current and can perform its first bounded publication.
    Configured,
    /// The retained runtime no longer matches onboarding or shared runtime authority.
    Unavailable,
    /// At least one exact immutable generation completed restart verification and a typed PIT read.
    Ready,
}

/// Closed reason a previously configured BEA product is unavailable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BeaProductUnavailableReason {
    /// The onboarding lease was revoked, rotated, or otherwise replaced.
    ActivationLeaseChanged,
    /// The shared research runtime no longer owns the exact configured generation.
    RuntimeGenerationChanged,
    /// Application-retained state could not be read safely.
    StateUnavailable,
}

/// Non-secret identity of one exact configured BEA source generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BeaConfiguredEvidence {
    source_id: SourceId,
    provider_dataset: SourceIdentifier,
    runtime_generation_digest: EvidenceDigest,
    source_binding_digest: EvidenceDigest,
    quota_declaration_digest: EvidenceDigest,
}

impl BeaConfiguredEvidence {
    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    pub(crate) const fn runtime_generation_digest(&self) -> EvidenceDigest {
        self.runtime_generation_digest
    }

    pub(crate) const fn source_binding_digest(&self) -> EvidenceDigest {
        self.source_binding_digest
    }

    pub(crate) const fn quota_declaration_digest(&self) -> EvidenceDigest {
        self.quota_declaration_digest
    }
}

/// Exact durable evidence from the latest successful fixed BEA product read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BeaReadyEvidence {
    configured: BeaConfiguredEvidence,
    restart_selector: ProviderMacroPlanRestartSelector,
    selection_digest: EvidenceDigest,
    selected_observations: usize,
}

impl BeaReadyEvidence {
    pub(crate) const fn configured(&self) -> &BeaConfiguredEvidence {
        &self.configured
    }

    pub(crate) const fn restart_selector(&self) -> &ProviderMacroPlanRestartSelector {
        &self.restart_selector
    }

    pub(crate) const fn selection_digest(&self) -> EvidenceDigest {
        self.selection_digest
    }

    pub(crate) const fn selected_observations(&self) -> usize {
        self.selected_observations
    }
}

/// Sanitized status. Secret bytes, opaque secret references, provider clients, and paths are
/// structurally absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BeaProductStatus {
    availability: BeaProductAvailability,
    operation: &'static str,
    configured: Option<BeaConfiguredEvidence>,
    ready: Option<BeaReadyEvidence>,
    unavailable_reason: Option<BeaProductUnavailableReason>,
}

impl BeaProductStatus {
    fn desired() -> Self {
        Self {
            availability: BeaProductAvailability::Desired,
            operation: BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION,
            configured: None,
            ready: None,
            unavailable_reason: None,
        }
    }

    fn unavailable(
        configured: Option<BeaConfiguredEvidence>,
        reason: BeaProductUnavailableReason,
    ) -> Self {
        Self {
            availability: BeaProductAvailability::Unavailable,
            operation: BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION,
            configured,
            ready: None,
            unavailable_reason: Some(reason),
        }
    }

    pub(crate) const fn availability(&self) -> BeaProductAvailability {
        self.availability
    }

    pub(crate) const fn operation(&self) -> &'static str {
        self.operation
    }

    pub(crate) const fn configured(&self) -> Option<&BeaConfiguredEvidence> {
        self.configured.as_ref()
    }

    pub(crate) const fn ready(&self) -> Option<&BeaReadyEvidence> {
        self.ready.as_ref()
    }

    pub(crate) const fn unavailable_reason(&self) -> Option<BeaProductUnavailableReason> {
        self.unavailable_reason
    }
}

/// User-supplied clocks for one fixed, code-bounded Regional publication and PIT read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BeaRegionalProductRequest {
    doctor_deadline: Timestamp,
    acquisition_deadline: Timestamp,
    seal_deadline: Instant,
    knowledge_cutoff: Timestamp,
    effective_period_cutoff: ResearchPeriod,
    query_deadline: Instant,
}

impl BeaRegionalProductRequest {
    #[allow(
        clippy::too_many_arguments,
        reason = "provider, seal, knowledge, provider-period, and query clocks remain independent"
    )]
    pub(crate) fn try_new(
        doctor_deadline: Timestamp,
        acquisition_deadline: Timestamp,
        seal_deadline: Instant,
        knowledge_cutoff: Timestamp,
        effective_period_cutoff: ResearchPeriod,
        query_deadline: Instant,
    ) -> Result<Self, BeaProductError> {
        if effective_period_cutoff.scheme().as_str() != BEA_ANNUAL_PERIOD_SCHEME {
            return Err(BeaProductError::InvalidOperation);
        }
        Ok(Self {
            doctor_deadline,
            acquisition_deadline,
            seal_deadline,
            knowledge_cutoff,
            effective_period_cutoff,
            query_deadline,
        })
    }
}

/// Typed successful producer-to-consumer result and its copyable durable coordinates.
#[derive(Debug)]
pub(crate) struct BeaRegionalProductOutput {
    ready: BeaReadyEvidence,
    outcome: BeaRegionalLiveOutcome,
}

impl BeaRegionalProductOutput {
    pub(crate) const fn ready(&self) -> &BeaReadyEvidence {
        &self.ready
    }

    /// Returns exact canonical rows with provider effective/availability/receipt clocks.
    pub(crate) const fn outcome(&self) -> &BeaRegionalLiveOutcome {
        &self.outcome
    }
}

/// Prepared credential-free restart/PIT read against one caller-supplied exact manifest selector.
#[derive(Debug)]
pub(crate) struct BeaRegionalRestartRead {
    request: BeaProviderPeriodLatestKnownRequest,
    limits: QueryLimits,
    deadline: Instant,
    completion: BeaRegionalRestartCompletion,
}

impl BeaRegionalRestartRead {
    /// Builds a fixed PIT read from exact durable catalog coordinates without opening a secret or
    /// contacting BEA. The shared reader revalidates the selector against the immutable catalog.
    pub(crate) fn try_new(
        restart_selector: ProviderMacroPlanRestartSelector,
        knowledge_cutoff: Timestamp,
        effective_period_cutoff: ResearchPeriod,
        deadline: Instant,
    ) -> Result<Self, BeaProductError> {
        if effective_period_cutoff.scheme().as_str() != BEA_ANNUAL_PERIOD_SCHEME
            || restart_selector.source_id().as_str() != BEA_SOURCE_ID
            || restart_selector.total_rows() == 0
        {
            return Err(BeaProductError::InvalidOperation);
        }
        let completion = BeaRegionalRestartCompletion {
            restart_selector: restart_selector.clone(),
        };
        Ok(Self {
            request: BeaProviderPeriodLatestKnownRequest::try_new(
                restart_selector,
                fixed_series_allowlist()?,
                knowledge_cutoff,
                effective_period_cutoff,
            )?,
            limits: fixed_query_limits()?,
            deadline,
            completion,
        })
    }

    /// Decomposes into the existing fixed BEA application read; no physical path or SQL is exposed.
    pub(crate) fn into_parts(
        self,
    ) -> (
        BeaProviderPeriodLatestKnownRequest,
        QueryLimits,
        Instant,
        BeaRegionalRestartCompletion,
    ) {
        (self.request, self.limits, self.deadline, self.completion)
    }
}

/// Linear validator pairing a fixed restart request with its returned typed rows.
#[derive(Debug)]
pub(crate) struct BeaRegionalRestartCompletion {
    restart_selector: ProviderMacroPlanRestartSelector,
}

impl BeaRegionalRestartCompletion {
    pub(crate) fn complete(
        self,
        read: BeaProviderPeriodLatestKnownDto,
    ) -> Result<BeaRegionalRestartOutput, BeaProductError> {
        let output = read.output();
        if read.restart_selector() != &self.restart_selector
            || read.source_id().as_str() != BEA_SOURCE_ID
            || output.period_scheme().as_str() != BEA_ANNUAL_PERIOD_SCHEME
            || output.observations().is_empty()
        {
            return Err(BeaProductError::InvalidReadResult);
        }
        let evidence = BeaRegionalRestartEvidence {
            restart_selector: self.restart_selector,
            selection_digest: output.selection_digest(),
            selected_observations: output.observations().len(),
        };
        Ok(BeaRegionalRestartOutput { evidence, read })
    }
}

/// Exact immutable selector and typed selection identity reproduced after restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BeaRegionalRestartEvidence {
    restart_selector: ProviderMacroPlanRestartSelector,
    selection_digest: EvidenceDigest,
    selected_observations: usize,
}

impl BeaRegionalRestartEvidence {
    pub(crate) const fn restart_selector(&self) -> &ProviderMacroPlanRestartSelector {
        &self.restart_selector
    }

    pub(crate) const fn selection_digest(&self) -> EvidenceDigest {
        self.selection_digest
    }

    pub(crate) const fn selected_observations(&self) -> usize {
        self.selected_observations
    }
}

/// Successful credential-free fixed PIT read against one exact immutable BEA generation.
#[derive(Debug)]
pub(crate) struct BeaRegionalRestartOutput {
    evidence: BeaRegionalRestartEvidence,
    read: BeaProviderPeriodLatestKnownDto,
}

impl BeaRegionalRestartOutput {
    pub(crate) const fn evidence(&self) -> &BeaRegionalRestartEvidence {
        &self.evidence
    }

    /// Returns canonical observations with exact provider clocks and immutable query evidence.
    pub(crate) const fn read(&self) -> &BeaProviderPeriodLatestKnownDto {
        &self.read
    }
}

/// Application-retained protected BEA source and its sole Regional product runtime.
pub(super) struct BeaProductActivation {
    lease: ProviderActivationLease,
    metadata: SourceMetadata,
    generation: ResearchProviderRuntimeGeneration,
    runtime: BeaRegionalLiveRuntime,
    configured: BeaConfiguredEvidence,
    ready: RwLock<Option<BeaReadyEvidence>>,
}

impl fmt::Debug for BeaProductActivation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BeaProductActivation")
            .field("surface_id", self.lease.surface_id())
            .field("source_id", self.metadata.source_id())
            .field("metadata_revision", self.metadata.revision())
            .field("provider_dataset", self.configured.provider_dataset())
            .field("credential", &"[SECRET-STORE ONLY]")
            .field("runtime", &"[APPLICATION-OWNED]")
            .finish()
    }
}

impl BeaProductActivation {
    pub(super) const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        &self.generation
    }

    pub(super) fn matches(
        &self,
        lease: &ProviderActivationLease,
        metadata: &SourceMetadata,
    ) -> bool {
        self.lease.same_authority_as(lease)
            && &self.metadata == metadata
            && self.generation.metadata() == metadata
    }

    fn status(&self, availability: BeaProductAvailability) -> BeaProductStatus {
        let ready = match self.ready.read() {
            Ok(ready) => ready.clone(),
            Err(_) => {
                return BeaProductStatus::unavailable(
                    Some(self.configured.clone()),
                    BeaProductUnavailableReason::StateUnavailable,
                );
            }
        };
        BeaProductStatus {
            availability: if ready.is_some() {
                BeaProductAvailability::Ready
            } else {
                availability
            },
            operation: BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION,
            configured: Some(self.configured.clone()),
            ready,
            unavailable_reason: None,
        }
    }

    async fn publish_and_read(
        &self,
        request: BeaRegionalProductRequest,
        context: &RequestContext,
    ) -> Result<BeaRegionalProductOutput, BeaProductError> {
        let BeaRegionalProductRequest {
            doctor_deadline,
            acquisition_deadline,
            seal_deadline,
            knowledge_cutoff,
            effective_period_cutoff,
            query_deadline,
        } = request;
        let live_request = BeaRegionalLiveRequest::new(
            self.configured.provider_dataset.clone(),
            doctor_deadline,
            acquisition_deadline,
            seal_deadline,
            NonZeroU32::new(MAXIMUM_PROVIDER_ROWS).ok_or(BeaProductError::InvalidOperation)?,
            NonZeroU64::new(MAXIMUM_CANONICAL_BYTES).ok_or(BeaProductError::InvalidOperation)?,
            fixed_series_allowlist()?,
            knowledge_cutoff,
            effective_period_cutoff,
            fixed_query_limits()?,
            query_deadline,
        );
        let outcome = self.runtime.publish_and_read(live_request, context).await?;
        let restart_selector = outcome.read().restart_selector().clone();
        let output = outcome.read().output();
        if outcome.source_binding_digest() != self.configured.source_binding_digest
            || outcome.publication_digest() != restart_selector.publication_digest()
            || restart_selector.source_id() != &self.configured.source_id
            || restart_selector.source_generation_digest() != self.configured.source_binding_digest
            || output.source_id() != &self.configured.source_id
            || output.period_scheme().as_str() != BEA_ANNUAL_PERIOD_SCHEME
            || output.observations().is_empty()
        {
            return Err(BeaProductError::InvalidReadResult);
        }
        let ready = BeaReadyEvidence {
            configured: self.configured.clone(),
            restart_selector,
            selection_digest: output.selection_digest(),
            selected_observations: output.observations().len(),
        };
        *self
            .ready
            .write()
            .map_err(|_| BeaProductError::Unavailable)? = Some(ready.clone());
        Ok(BeaRegionalProductOutput { ready, outcome })
    }
}

impl ProviderAdapterActivation {
    /// Constructs and registers one exact protected BEA Regional source generation.
    pub(crate) async fn prepare_bea_regional(
        &self,
        lease: ProviderActivationLease,
        spec: BeaAdapterActivation,
        cancellation: CancellationToken,
    ) -> Result<Arc<BeaProductActivation>, BeaProductError> {
        let (rights, generation) = self.bea_regional_authority(&lease, &spec, &cancellation)?;
        if let Some(current) = self
            .bea
            .read()
            .map_err(|_| BeaProductError::Unavailable)?
            .as_ref()
            .filter(|current| current.matches(&lease, &spec.metadata))
            .cloned()
            && matches!(
                self.research
                    .provider_runtime_generation(current.generation().profile()),
                Ok(Some(active)) if active == *current.generation()
            )
        {
            return Ok(current);
        }
        let (activation, registered_source) = self
            .build_bea_regional_candidate(lease.clone(), spec, generation.clone(), cancellation)
            .await?;

        let onboarding = self.onboarding.try_acquire_runtime_mutation_authority()?;
        onboarding.require_active(&lease)?;
        let mut retained = self.bea.write().map_err(|_| BeaProductError::Unavailable)?;
        if let Some(current) = retained.as_ref() {
            if current.matches(&lease, &activation.metadata)
                && matches!(
                    self.research
                        .provider_runtime_generation(current.generation().profile()),
                    Ok(Some(active)) if active == *current.generation()
                )
            {
                return Ok(Arc::clone(current));
            }
            return Err(BeaProductError::Unavailable);
        }
        self.research_mutation
            .register_provider_source(generation, registered_source, rights)?;
        *retained = Some(Arc::clone(&activation));
        Ok(activation)
    }

    /// Constructs a non-callable replacement candidate while the exact predecessor remains owned
    /// by the serialized research runtime transaction.
    pub(super) async fn prepare_bea_regional_replacement(
        &self,
        lease: ProviderActivationLease,
        spec: BeaAdapterActivation,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        cancellation: CancellationToken,
    ) -> Result<
        (
            ResearchProviderRuntimeReplacement,
            Arc<BeaProductActivation>,
        ),
        BeaProductError,
    > {
        let (rights, derived_candidate) =
            self.bea_regional_authority(&lease, &spec, &cancellation)?;
        if candidate != derived_candidate || expected.profile() != candidate.profile() {
            return Err(BeaProductError::AuthorityUnavailable);
        }
        if !self
            .bea
            .read()
            .map_err(|_| BeaProductError::Unavailable)?
            .as_ref()
            .is_some_and(|current| current.generation() == &expected)
        {
            return Err(BeaProductError::Unavailable);
        }
        let (activation, registered_source) = self
            .build_bea_regional_candidate(lease.clone(), spec, candidate.clone(), cancellation)
            .await?;
        self.require_runtime_replacement_authority(&lease, &expected)
            .await
            .map_err(|_| BeaProductError::AuthorityUnavailable)?;
        let replacement = self.research_mutation.prepare_provider_replacement(
            expected,
            candidate,
            registered_source,
            rights,
        )?;
        Ok((replacement, activation))
    }

    fn bea_regional_authority(
        &self,
        lease: &ProviderActivationLease,
        spec: &BeaAdapterActivation,
        cancellation: &CancellationToken,
    ) -> Result<(ResearchRightsAuthority, ResearchProviderRuntimeGeneration), BeaProductError> {
        if cancellation.is_cancelled()
            || lease.surface_id().as_str() != BEA_SURFACE
            || lease.generation().is_none()
            || lease.secret_reference().is_none()
            || spec.metadata.source_id().as_str() != BEA_SOURCE_ID
            || spec.metadata.authorization().mode() != AuthorizationMode::UserAuthorized
        {
            return Err(BeaProductError::InvalidOperation);
        }
        let config = fixed_regional_source_config()?;
        if config.contracts().len() != 1
            || config.contracts()[0].dataset_id() != spec.provider_dataset_identifier()
        {
            return Err(BeaProductError::InvalidOperation);
        }
        let rights = provider_research_rights(lease, spec.metadata.source_id())
            .map_err(|_| BeaProductError::AuthorityUnavailable)?;
        let generation = runtime_generation(lease, spec.metadata.clone(), rights.clone())
            .map_err(|_| BeaProductError::AuthorityUnavailable)?;
        self.bind_authorization_subject(&spec.metadata)
            .map_err(|_| BeaProductError::AuthorityUnavailable)?;
        Ok((rights, generation))
    }

    async fn build_bea_regional_candidate(
        &self,
        lease: ProviderActivationLease,
        spec: BeaAdapterActivation,
        generation: ResearchProviderRuntimeGeneration,
        cancellation: CancellationToken,
    ) -> Result<
        (
            Arc<BeaProductActivation>,
            impl ManagedResearchExtractionSource,
        ),
        BeaProductError,
    > {
        let provider_dataset = spec.provider_dataset_identifier().clone();
        let secret = self
            .onboarding
            .read_secret_for_activation_request(&lease, cancellation)
            .await?;
        let user_id = BeaUserId::try_new(secret.expose_secret().to_owned())?;
        let generation_digest = generation.generation_digest()?;
        let source = BeaSource::try_new(
            spec.metadata.clone(),
            user_id,
            fixed_regional_source_config()?,
            generation_digest,
        )?;
        let configured = BeaConfiguredEvidence {
            source_id: spec.metadata.source_id().clone(),
            provider_dataset,
            runtime_generation_digest: generation_digest,
            source_binding_digest: source.source_binding().binding_digest(),
            quota_declaration_digest: source.quota_declaration().declaration_digest(),
        };
        let composition = BeaRegionalLiveComposition::try_new(
            Arc::clone(&self.research),
            source,
            generation.clone(),
        )?;
        let (registered_source, runtime) = composition.into_parts();
        Ok((
            Arc::new(BeaProductActivation {
                lease,
                metadata: spec.metadata,
                generation,
                runtime,
                configured,
                ready: RwLock::new(None),
            }),
            registered_source,
        ))
    }

    /// Returns only exact current BEA product state and non-secret durable evidence.
    pub(crate) fn bea_status(&self) -> BeaProductStatus {
        let activation = match self.bea.read() {
            Ok(slot) => slot.as_ref().cloned(),
            Err(_) => {
                return BeaProductStatus::unavailable(
                    None,
                    BeaProductUnavailableReason::StateUnavailable,
                );
            }
        };
        let Some(activation) = activation else {
            return BeaProductStatus::desired();
        };
        match self
            .onboarding
            .activation_lease(activation.lease.session_id())
        {
            Ok(current) if current.same_authority_as(&activation.lease) => {}
            Ok(_) | Err(_) => {
                return BeaProductStatus::unavailable(
                    Some(activation.configured.clone()),
                    BeaProductUnavailableReason::ActivationLeaseChanged,
                );
            }
        }
        match self
            .research
            .provider_runtime_generation(activation.generation().profile())
        {
            Ok(Some(current)) if current == *activation.generation() => {
                activation.status(BeaProductAvailability::Configured)
            }
            Ok(_) | Err(_) => BeaProductStatus::unavailable(
                Some(activation.configured.clone()),
                BeaProductUnavailableReason::RuntimeGenerationChanged,
            ),
        }
    }

    /// Executes the only live BEA product operation after revalidating its protected lease.
    pub(crate) async fn execute_bea_regional(
        &self,
        request: BeaRegionalProductRequest,
        context: &RequestContext,
    ) -> Result<BeaRegionalProductOutput, BeaProductError> {
        let activation = self
            .bea
            .read()
            .map_err(|_| BeaProductError::Unavailable)?
            .as_ref()
            .cloned()
            .ok_or(BeaProductError::SetupRequired)?;
        let onboarding = self.onboarding.acquire_runtime_mutation_authority().await;
        onboarding.require_active(&activation.lease)?;
        if !matches!(
            self.research
                .provider_runtime_generation(activation.generation().profile()),
            Ok(Some(current)) if current == *activation.generation()
        ) {
            return Err(BeaProductError::Unavailable);
        }
        drop(onboarding);
        activation.publish_and_read(request, context).await
    }

    /// Reopens one caller-pinned immutable BEA generation without loading a credential or making
    /// a provider request. This is the process-restart read path for Desktop, CLI, and MCP
    /// composition once those shared surfaces register the fixed operation.
    pub(crate) async fn read_bea_regional_restart(
        &self,
        request: BeaRegionalRestartRead,
        cancellation: CancellationToken,
    ) -> Result<BeaRegionalRestartOutput, BeaProductError> {
        let (request, limits, deadline, completion) = request.into_parts();
        let state = self
            .research
            .read_bea_provider_period_latest_known(request, limits, deadline, cancellation)
            .await?;
        let read = match state {
            BeaMacroCapabilityState::Available(read) => read,
            BeaMacroCapabilityState::SetupRequired(_) | BeaMacroCapabilityState::Unavailable(_) => {
                return Err(BeaProductError::InvalidReadResult);
            }
        };
        completion.complete(read)
    }
}

fn fixed_regional_contract() -> Result<BeaDatasetContract, BeaProductError> {
    let mut parameters = BTreeMap::new();
    for (name, value) in [
        ("GeoFips", REGIONAL_GEO_FIPS),
        ("LineCode", REGIONAL_LINE_CODE),
        ("TableName", REGIONAL_TABLE),
        ("Year", REGIONAL_YEAR_SCOPE),
    ] {
        parameters.insert(BeaParameterIdentity::try_new(name)?, value.to_owned());
    }
    Ok(BeaDatasetContract::try_new(
        BeaDatasetIdentity::try_new(REGIONAL_DATASET)?,
        parameters,
        None,
    )?)
}

/// Returns the sole code-owned BEA configuration used by activation metadata and runtime
/// construction. Callers cannot add datasets, selectors, or parser capacity.
pub(crate) fn fixed_regional_source_config() -> Result<BeaSourceConfig, BeaProductError> {
    Ok(BeaSourceConfig::try_new(
        vec![fixed_regional_contract()?],
        fixed_parse_limits()?,
    )?)
}

fn fixed_parse_limits() -> Result<BeaParseLimits, BeaProductError> {
    Ok(BeaParseLimits::try_new(
        usize::try_from(MAXIMUM_PROVIDER_ROWS).map_err(|_| BeaProductError::InvalidOperation)?,
        MAXIMUM_METADATA_RECORDS,
        usize::try_from(MAXIMUM_CANONICAL_BYTES).map_err(|_| BeaProductError::InvalidOperation)?,
        MAXIMUM_STRING_BYTES,
        MAXIMUM_DIMENSIONS,
        MAXIMUM_NOTES,
    )?)
}

fn fixed_series_allowlist() -> Result<AnalyticalMacroSeriesAllowlist, BeaProductError> {
    Ok(AnalyticalMacroSeriesAllowlist::try_from_code_owned(&[
        DELAWARE_PER_CAPITA_INCOME_SERIES,
    ])?)
}

fn fixed_query_limits() -> Result<QueryLimits, BeaProductError> {
    Ok(QueryLimits::try_new(
        MAXIMUM_QUERY_ROWS,
        MAXIMUM_QUERY_BYTES,
        MAXIMUM_QUERY_MEMORY_BYTES,
        1,
        MAXIMUM_QUERY_AST_NODES,
        MAXIMUM_QUERY_PLAN_NODES,
        QUERY_DURATION,
    )?)
}

/// Closed BEA product failure. Provider credential material is absent from every variant.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BeaProductError {
    #[error("BEA Regional setup is required")]
    SetupRequired,
    #[error("the exact BEA Regional runtime is unavailable")]
    Unavailable,
    #[error("the BEA Regional product operation is invalid")]
    InvalidOperation,
    #[error("BEA onboarding or shared runtime authority is unavailable")]
    AuthorityUnavailable,
    #[error("the BEA Regional typed read returned invalid exact-generation evidence")]
    InvalidReadResult,
    #[error("BEA rejected the code-owned Regional request")]
    Adapter(#[from] BeaError),
    #[error("BEA rejected source construction or Regional publication authority")]
    Source(#[from] BeaSourceError),
    #[error("the shared research runtime rejected BEA generation composition")]
    Composition(#[from] ResearchIngestCompositionError),
    #[error("BEA live sealing, publication, restart, or PIT selection failed")]
    Live(#[from] BeaLivePublicationError),
    #[error("the fixed BEA restart/PIT read is invalid")]
    Application(#[from] BeaMacroApplicationError),
    #[error("the fixed BEA analytical query limits are invalid")]
    Query(#[from] QueryError),
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        num::NonZeroU16,
        sync::Arc,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    use chrono::{Datelike as _, Utc};
    use market_squawk_adapter_bea::{bea_api_endpoint_rule, bea_provider_rate_declaration};
    use market_squawk_data::{
        CatalogConfig, CatalogResultLimits, ObjectStoreConfig, SqliteProviderRateStore,
    };
    use market_squawk_domain::{
        AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
        EffectiveInterval, ExactPayloadEvidence, MetadataRevision, RevisionBoundPayloadEvidence,
        SchemaVersion, SequenceCapability,
    };
    use market_squawk_platform::{
        AppConfig, ConfigOverrides, ConfigSources, EncryptedFileSecretStore,
        LocalAuthorityStateStore, LocalPaths, SecretValue,
    };
    use market_squawk_services::{JsonStructureLimits, RequestId, ServiceLimits};
    use market_squawk_sources::{
        AuthoritativeSourceRegistry, AuthorizationGrant, CoverageDomain, EndpointPolicy,
        FreshnessPolicy, HistoricalCapability, HttpRequestBounds, NetworkAccessPolicy,
        ProviderRateAuthority, ProviderRateDeclaration, SourceCapabilities, SourceClass,
        SourceCoverage, SourceMetadataInput, SourceProtocolProfile,
    };

    use super::*;
    use crate::ResearchService;
    use crate::application::ResearchExtractionLimits;
    use crate::provider_onboarding::StartOnboardingRequest;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    /// One intentionally ignored live proof covers the only code-owned BEA response family and the
    /// complete protected setup -> sealed raw -> immutable macro -> PIT -> restart journey. It is
    /// ignored in normal test runs because it requires an operator credential and network access.
    #[tokio::test]
    #[ignore = "requires BEA_USER_ID and bounded live BEA access"]
    async fn protected_regional_live_publication_and_restart_are_exact() -> TestResult {
        let user_id = std::env::var("BEA_USER_ID")?;
        BeaUserId::try_new(user_id.clone())?;

        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path().join("product"))?;
        let provider_rate =
            ProviderRateAuthority::try_new(Arc::new(SqliteProviderRateStore::try_open(
                paths.control_root()?.root().join("provider-rate.sqlite3"),
            )?))?;
        let secrets = Arc::new(EncryptedFileSecretStore::try_open(
            paths.control_root()?.root().join("provider-secrets"),
            SecretValue::new("bea-live-proof-local-unlock".to_owned())?,
        )?);
        let (research, onboarding, _publisher) =
            ResearchService::open_or_initialize_with_provider_onboarding_service(
                &paths,
                catalog_config(&paths)?,
                8,
                object_store_config()?,
                secrets,
                provider_rate.clone(),
            )?;
        let research = Arc::new(research);
        let onboarding = Arc::new(onboarding);
        let registry = AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
            LocalAuthorityStateStore::try_open(
                paths.control_root()?.root().join("source-authority"),
            )?,
            Arc::new(provider_rate.clone()),
            provider_rate.clone(),
        )?;
        let (coordinator, mutation, alpaca) =
            crate::application::ProductionResearchIngestCoordinator::try_new_with_runtime_authorities(
                registry,
                Arc::clone(&research),
                ResearchExtractionLimits::standard(),
                std::iter::empty(),
            )?;
        let coordinator = Arc::new(coordinator);
        let app_config = AppConfig::load(ConfigSources::new(
            None,
            &BTreeMap::<OsString, OsString>::new(),
            ConfigOverrides {
                data_dir: Some(temporary.path().join("app-data")),
                ..ConfigOverrides::default()
            },
        ))?;
        let activation = ProviderAdapterActivation::new(
            Arc::clone(&onboarding),
            Arc::clone(&coordinator),
            mutation,
            app_config,
            provider_rate,
            paths.control_root()?.root().to_path_buf(),
        );

        let started = onboarding
            .start(
                StartOnboardingRequest::try_new(BEA_SURFACE, None, None)?,
                CancellationToken::new(),
            )
            .await?;
        let imported = onboarding
            .submit_secret(
                started.session_id(),
                SecretValue::new(user_id)?,
                CancellationToken::new(),
            )
            .await?;
        let prepared = onboarding
            .prepare_runtime_activation_target(imported.session_id(), CancellationToken::new())
            .await?;
        let lease = onboarding.commit_prepared_activation(&prepared).await?;
        let metadata = source_metadata(&lease)?;
        let config = fixed_regional_source_config()?;
        let provider_dataset = config
            .contracts()
            .first()
            .filter(|_| config.contracts().len() == 1)
            .ok_or("fixed BEA Regional contract is unavailable")?
            .dataset_id()
            .clone();
        activation
            .activate_ready_profile(
                lease.session_id(),
                crate::ProviderAdapterActivationRequest::Bea(BeaAdapterActivation::new(
                    metadata,
                    provider_dataset,
                )),
                CancellationToken::new(),
            )
            .await?;

        let now = current_timestamp()?;
        let provider_deadline = now.checked_add_nanos(120_000_000_000)?;
        let knowledge_cutoff = now.checked_add_nanos(120_000_000_000)?;
        let period = current_provider_annual_period()?;
        let operation_deadline = Instant::now() + Duration::from_secs(120);
        let outcome = activation
            .execute_bea_regional(
                BeaRegionalProductRequest::try_new(
                    provider_deadline,
                    provider_deadline,
                    operation_deadline,
                    knowledge_cutoff,
                    period.clone(),
                    operation_deadline,
                )?,
                &request_context(operation_deadline)?,
            )
            .await?;
        let restart_selector = outcome.ready().restart_selector().clone();
        let selection_digest = outcome.ready().selection_digest();
        let selected_observations = outcome.ready().selected_observations();
        let manifest = restart_selector.manifest().clone();
        assert!(selected_observations > 0);
        assert_eq!(restart_selector.source_id().as_str(), BEA_SOURCE_ID);

        drop(outcome);
        drop(activation);
        drop(onboarding);
        drop(alpaca);
        drop(coordinator);
        drop(research);

        let reopened = Arc::new(ResearchService::open_or_initialize(
            &paths,
            catalog_config(&paths)?,
            8,
            object_store_config()?,
        )?);
        reopened
            .recover_provider_capture_store(&CancellationToken::new())
            .await?;
        let read = crate::application::BeaMacroApplicationClosure::new(reopened)
            .read_provider_period_latest_known(
                BeaProviderPeriodLatestKnownRequest::try_new(
                    restart_selector.clone(),
                    fixed_series_allowlist()?,
                    knowledge_cutoff,
                    period,
                )?,
                fixed_query_limits()?,
                Instant::now() + Duration::from_secs(10),
                CancellationToken::new(),
            )
            .await?;
        let read = read
            .available()
            .ok_or("restarted BEA read is unavailable")?;
        assert_eq!(read.reopened().manifest(), &manifest);
        assert_eq!(read.output().selection_digest(), selection_digest);
        assert_eq!(read.output().observations().len(), selected_observations);
        eprintln!(
            "BEA_LIVE_EVIDENCE source={} dataset={} table={} line={} geography={} period_scope={} canonical_rows={} selected_rows={} manifest_dataset={} manifest_version={} selection={selection_digest:?}",
            BEA_SOURCE_ID,
            REGIONAL_DATASET,
            REGIONAL_TABLE,
            REGIONAL_LINE_CODE,
            REGIONAL_GEO_FIPS,
            REGIONAL_YEAR_SCOPE,
            restart_selector.total_rows(),
            selected_observations,
            manifest.dataset_id().as_str(),
            manifest.manifest_version(),
        );
        Ok(())
    }

    fn source_metadata(lease: &ProviderActivationLease) -> TestResult<SourceMetadata> {
        let config = fixed_regional_source_config()?;
        let effective = EffectiveInterval::new(
            lease.authority_effective_at(),
            lease.verification_expires_at(),
        )?;
        let provider = SourceIdentifier::try_from("bea")?;
        let evidence = ExactPayloadEvidence::from_content_digest(lease.capability_digest());
        let authorization = AuthorizationGrant::new(
            AuthorizationMode::UserAuthorized,
            AuthorizationBasis::new(ProviderRateDeclaration::governed_provider_subject(
                &provider,
            )?),
            evidence.clone(),
            effective,
        );
        let network = EndpointPolicy::try_from_api_rules(
            vec![bea_api_endpoint_rule(&config)?],
            HttpRequestBounds::default(),
        )?;
        Ok(SourceMetadata::try_new(SourceMetadataInput::new(
            SchemaVersion::CURRENT,
            SourceId::try_from(BEA_SOURCE_ID)?,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from("bea-regional-v1")?),
                evidence.clone(),
            ),
            SourceClass::OfficialAgency,
            provider,
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
            Some(bea_provider_rate_declaration()?.policy().clone()),
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

    fn catalog_config(paths: &LocalPaths) -> TestResult<CatalogConfig> {
        Ok(CatalogConfig::try_new(
            paths.catalog()?.clone(),
            Duration::from_millis(750),
            market_squawk_data::CatalogLimit::new(64)?,
            CatalogResultLimits::try_new(1_024 * 1_024, 8 * 1_024 * 1_024)?,
        )?)
    }

    fn object_store_config() -> TestResult<ObjectStoreConfig> {
        Ok(ObjectStoreConfig::try_new(
            16 * 1_024 * 1_024,
            1_024,
            Duration::from_secs(60),
        )?)
    }

    fn request_context(deadline: Instant) -> TestResult<RequestContext> {
        Ok(RequestContext::new(
            RequestId::String(Arc::from("test.bea-live-publication")),
            CancellationToken::new(),
            deadline,
            ServiceLimits::try_new(
                4_096,
                8,
                4_096,
                8,
                JsonStructureLimits::try_new(32, 8_192, 128, 128)?,
            )?,
        ))
    }

    fn current_timestamp() -> TestResult<Timestamp> {
        let nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
        Ok(Timestamp::from_unix_nanos(nanos))
    }

    fn current_provider_annual_period() -> TestResult<ResearchPeriod> {
        let year = u16::try_from(Utc::now().year())?;
        Ok(ResearchPeriod::try_new(
            SourceIdentifier::try_from(BEA_ANNUAL_PERIOD_SCHEME)?,
            year,
            NonZeroU16::MIN,
            SourceIdentifier::try_from(year.to_string())?,
        )?)
    }
}
