//! Protected Census setup and generation-bound durable macro publication.
//!
//! Setup retains one configured transport in the existing research registry. Its paired
//! application runtime performs the doctor, raw sealing, and publication under that registry's
//! operation authority. Durable reads use the existing manifest-pinned Census selector and do not
//! require a credential or a live provider runtime.

use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Instant,
};

use market_squawk_adapter_census::{
    CensusApiKey, CensusDatasetContract, CensusParseLimits, CensusSource, CensusSourceConfig,
};
use market_squawk_data::{CatalogAuthority, IngestError, IngestPrecommitAuthority};
use market_squawk_domain::SourceIdentifier;
use market_squawk_services::RequestContext;
use market_squawk_sources::{AuthorizationMode, SourceMetadata};
use tokio_util::sync::CancellationToken;

use super::{
    ActivatedResearchProvider, ProviderAdapterActivation, ProviderAdapterActivationError,
    provider_research_rights, require_surface, runtime_generation,
};
use crate::ProviderActivationLease;
use crate::application::{
    CensusLiveComposition, CensusMacroApplicationClosure, CensusMacroApplicationError,
    CensusPublicationReceipt, CensusSealFirstExtractionLimits, ResearchProviderRuntimeGeneration,
    ResearchProviderRuntimeReplacement,
};
use crate::provider_onboarding::ProviderOnboardingOwnedMutationAuthority;

pub(super) const CENSUS_SURFACE: &str = "census.data-api";

/// Code-owned route selection and finite acquisition bounds; contains no credential.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CensusAdapterActivation {
    metadata: SourceMetadata,
    config: CensusSourceConfig,
    provider_dataset: SourceIdentifier,
    analytical_dataset: SourceIdentifier,
    acquisition_limits: CensusSealFirstExtractionLimits,
}

impl CensusAdapterActivation {
    /// Binds the existing metadata-driven query to finite application-owned limits.
    pub fn try_new(
        metadata: SourceMetadata,
        contract: CensusDatasetContract,
        parse_limits: CensusParseLimits,
        max_records: NonZeroU32,
        max_bytes: NonZeroU64,
    ) -> Result<Self, ProviderAdapterActivationError> {
        if metadata.authorization().mode() != AuthorizationMode::UserAuthorized {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        let provider_dataset = contract.dataset_id().clone();
        let analytical_dataset = contract.analytical_dataset_id().clone();
        let config = CensusSourceConfig::try_new([contract], parse_limits)
            .map_err(|_| ProviderAdapterActivationError::SourceBinding)?;
        Ok(Self {
            metadata,
            config,
            provider_dataset,
            analytical_dataset,
            acquisition_limits: CensusSealFirstExtractionLimits::new(max_records, max_bytes),
        })
    }
    pub(crate) const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
    pub(crate) const fn provider_dataset_identifier(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }
    pub(crate) const fn analytical_dataset_identifier(&self) -> &SourceIdentifier {
        &self.analytical_dataset
    }
}

/// One protected credential generation and its paired, shared-registry macro runtime.
pub(super) struct CensusProductActivation {
    lease: ProviderActivationLease,
    generation: ResearchProviderRuntimeGeneration,
    specification: CensusAdapterActivation,
    runtime: CensusMacroApplicationClosure,
}

impl CensusProductActivation {
    pub(super) const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        &self.generation
    }

    fn matches(
        &self,
        lease: &ProviderActivationLease,
        generation: &ResearchProviderRuntimeGeneration,
        specification: &CensusAdapterActivation,
    ) -> bool {
        self.lease.same_authority_as(lease)
            && &self.generation == generation
            && &self.specification == specification
    }
}

impl std::fmt::Debug for CensusProductActivation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CensusProductActivation")
            .field("surface_id", self.lease.surface_id())
            .field("source_id", self.generation.metadata().source_id())
            .field("provider_dataset", &self.specification.provider_dataset)
            .finish_non_exhaustive()
    }
}

impl ProviderAdapterActivation {
    /// Registers a configured Census source without making a provider request or claiming readiness.
    pub(super) async fn activate_census(
        &self,
        lease: ProviderActivationLease,
        specification: CensusAdapterActivation,
        cancellation: CancellationToken,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError> {
        require_surface(&lease, CENSUS_SURFACE)?;
        let rights = provider_research_rights(&lease, specification.metadata.source_id())?;
        let generation =
            self.runtime_registration_generation(&lease, &specification.metadata, &rights)?;
        {
            let retained = self
                .census
                .read()
                .map_err(|_| ProviderAdapterActivationError::SourceBinding)?;
            if let Some(current) = retained.as_ref() {
                if !current.matches(&lease, &generation, &specification)
                    || self
                        .research
                        .provider_runtime_generation(generation.profile())?
                        .as_ref()
                        != Some(&generation)
                {
                    return Err(ProviderAdapterActivationError::SourceBinding);
                }
                return Ok(ActivatedResearchProvider {
                    lease,
                    profile: generation.profile().clone(),
                    generation,
                });
            }
        }
        let composition = self
            .configured_census_composition(
                &lease,
                &specification,
                &generation,
                cancellation.clone(),
            )
            .await?;
        let (registered_source, runtime) = composition.into_parts();
        if cancellation.is_cancelled() {
            return Err(ProviderAdapterActivationError::Cancelled);
        }
        let onboarding = self.onboarding.try_acquire_runtime_mutation_authority()?;
        onboarding.require_active(&lease)?;
        let mut retained = self
            .census
            .write()
            .map_err(|_| ProviderAdapterActivationError::SourceBinding)?;
        if let Some(current) = retained.as_ref() {
            if !current.matches(&lease, &generation, &specification)
                || self
                    .research
                    .provider_runtime_generation(generation.profile())?
                    .as_ref()
                    != Some(&generation)
            {
                return Err(ProviderAdapterActivationError::SourceBinding);
            }
        } else {
            self.research_mutation.register_provider_source(
                generation.clone(),
                registered_source,
                rights,
            )?;
            *retained = Some(Arc::new(CensusProductActivation {
                lease: lease.clone(),
                generation: generation.clone(),
                specification,
                runtime,
            }));
        }
        Ok(ActivatedResearchProvider {
            lease,
            profile: generation.profile().clone(),
            generation,
        })
    }

    /// Prepares a replacement through the existing serialized runtime transaction.
    /// The candidate cannot run a doctor or publish until the shared parent commits it.
    pub(super) async fn prepare_census_runtime_replacement(
        &self,
        lease: &ProviderActivationLease,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        specification: CensusAdapterActivation,
        cancellation: CancellationToken,
    ) -> Result<
        (
            ResearchProviderRuntimeReplacement,
            Arc<CensusProductActivation>,
        ),
        ProviderAdapterActivationError,
    > {
        require_surface(lease, CENSUS_SURFACE)?;
        let rights = provider_research_rights(lease, specification.metadata.source_id())?;
        if candidate != runtime_generation(lease, specification.metadata.clone(), rights.clone())?
            || self
                .census
                .read()
                .map_err(|_| ProviderAdapterActivationError::SourceBinding)?
                .as_ref()
                .is_some_and(|current| current.generation() != &expected)
        {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        self.bind_authorization_subject(&specification.metadata)?;
        let composition = self
            .configured_census_composition(lease, &specification, &candidate, cancellation.clone())
            .await?;
        if cancellation.is_cancelled() {
            return Err(ProviderAdapterActivationError::Cancelled);
        }
        let (registered_source, runtime) = composition.into_parts();
        let activation = Arc::new(CensusProductActivation {
            lease: lease.clone(),
            generation: candidate.clone(),
            specification,
            runtime,
        });
        let replacement = self
            .prepare_runtime_replacement(lease, expected, candidate, registered_source, rights)
            .await?;
        Ok((replacement, activation))
    }

    async fn configured_census_composition(
        &self,
        lease: &ProviderActivationLease,
        specification: &CensusAdapterActivation,
        generation: &ResearchProviderRuntimeGeneration,
        cancellation: CancellationToken,
    ) -> Result<CensusLiveComposition, ProviderAdapterActivationError> {
        if cancellation.is_cancelled() {
            return Err(ProviderAdapterActivationError::Cancelled);
        }
        if lease.generation().is_none() || lease.secret_reference().is_none() {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        let secret = self
            .onboarding
            .read_secret_for_activation_request(lease, cancellation)
            .await?;
        let key = CensusApiKey::try_new(secret.expose_secret().to_owned())
            .map_err(|_| ProviderAdapterActivationError::SourceBinding)?;
        let source = CensusSource::try_new(
            specification.metadata.clone(),
            key,
            specification.config.clone(),
        )
        .map_err(|_| ProviderAdapterActivationError::SourceBinding)?;
        CensusLiveComposition::try_new(Arc::clone(&self.research), source, generation.clone())
            .map_err(|_| ProviderAdapterActivationError::SourceBinding)
    }

    /// Publishes through the exact current Census generation and returns immutable read coordinates.
    /// Ordinary macro operations consume the receipt's neutral observations through the shared
    /// selector; this provider-specific entry point remains inside setup and source orchestration.
    pub(crate) async fn publish_census_macro(
        &self,
        context: &RequestContext,
    ) -> Result<CensusPublicationReceipt, CensusProductError> {
        let activation = self
            .census
            .read()
            .map_err(|_| CensusProductError::Unavailable)?
            .as_ref()
            .cloned()
            .ok_or(CensusProductError::SetupRequired)?;
        let onboarding = self.onboarding.try_acquire_runtime_mutation_authority()?;
        onboarding.require_active(&activation.lease)?;
        if self
            .research
            .provider_runtime_generation(activation.generation.profile())?
            .as_ref()
            != Some(&activation.generation)
        {
            return Err(CensusProductError::Unavailable);
        }
        drop(onboarding);
        Ok(activation
            .runtime
            .acquire_seal_and_publish(
                activation.specification.provider_dataset.clone(),
                activation.specification.acquisition_limits,
                context,
                |deadline, cancellation| {
                    let onboarding = self
                        .onboarding
                        .try_acquire_owned_runtime_mutation_authority()
                        .map_err(|_| CensusMacroApplicationError::AuthorityInvalid)?;
                    let authority = CensusPublicationAuthority {
                        onboarding,
                        lease: activation.lease.clone(),
                        deadline,
                        cancellation,
                    };
                    authority
                        .validate_precommit()
                        .map_err(|_| CensusMacroApplicationError::AuthorityInvalid)?;
                    Ok(Some(Arc::new(authority)))
                },
            )
            .await
            .inspect_err(trace_census_publication_failure)?)
    }
}

// Log only code-owned categories and enum discriminants, never error formatting or payloads.
fn trace_census_publication_failure(error: &CensusMacroApplicationError) {
    macro_rules! nested {
        ($stage:literal, $cause:expr) => {
            tracing::warn!(
                stage = $stage,
                failure = ?std::mem::discriminant(error),
                cause = ?std::mem::discriminant($cause),
                "Census acquisition or publication failed"
            )
        };
    }
    match error {
        CensusMacroApplicationError::AuthorityInvalid => nested!("publication_authority", error),
        CensusMacroApplicationError::CandidateInvalid => nested!("publication_candidate", error),
        CensusMacroApplicationError::QuarterlySelectionInvalid => {
            nested!("quarterly_selection", error)
        }
        CensusMacroApplicationError::RestartInvalid => nested!("restart_verification", error),
        CensusMacroApplicationError::Adapter(cause) => nested!("adapter_acquisition", cause),
        CensusMacroApplicationError::Extraction(cause) => nested!("source_extraction", cause),
        CensusMacroApplicationError::ExtractionContract(cause) => {
            nested!("extraction_request", cause)
        }
        CensusMacroApplicationError::Publication(..) => tracing::warn!(
            stage = "generation_publication",
            failure = ?std::mem::discriminant(error),
            "Census acquisition or publication failed"
        ),
        CensusMacroApplicationError::Service(cause) => nested!("operation_authority", cause),
        CensusMacroApplicationError::Research(cause) => nested!("research_storage", cause),
        CensusMacroApplicationError::AnalyticalRead(cause) => nested!("analytical_read", cause),
    }
}

/// Retains the sole onboarding mutation guard from completed acquisition through catalog commit.
#[derive(Debug)]
struct CensusPublicationAuthority {
    onboarding: ProviderOnboardingOwnedMutationAuthority,
    lease: ProviderActivationLease,
    deadline: Instant,
    cancellation: CancellationToken,
}

impl CensusPublicationAuthority {
    fn ensure_live(&self) -> Result<(), IngestError> {
        if self.cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(IngestError::DeadlineExceeded);
        }
        Ok(())
    }
}

impl IngestPrecommitAuthority for CensusPublicationAuthority {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.ensure_live()?;
        self.onboarding
            .require_active(&self.lease)
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        self.ensure_live()
    }

    fn validate_catalog_precommit(&self, catalog: &CatalogAuthority) -> Result<(), IngestError> {
        self.ensure_live()?;
        self.onboarding
            .require_active_in_catalog(catalog, &self.lease)
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        self.ensure_live()
    }
}

/// Technical source failure; every nested Census diagnostic is closed and payload-free.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CensusProductError {
    #[error("economic data setup is required")]
    SetupRequired,
    #[error("the configured economic source is unavailable")]
    Unavailable,
    #[error(transparent)]
    Onboarding(#[from] crate::ProviderOnboardingError),
    #[error(transparent)]
    Composition(#[from] crate::application::ResearchIngestCompositionError),
    #[error("economic data acquisition or durable publication failed")]
    Application(#[from] CensusMacroApplicationError),
}
