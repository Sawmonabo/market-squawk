//! Protected EIA setup and generation-bound durable macro publication.
//!
//! Setup retains one configured transport in the existing research registry. Its paired
//! application runtime performs the doctor, raw sealing, and publication under that registry's
//! operation authority. Durable reads use the existing manifest-pinned EIA selector and do not
//! require a credential or a live provider runtime.

use std::{
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Instant,
};

use market_squawk_adapter_eia::{
    EiaApiKey, EiaDatasetProfile, EiaPublicationMode, EiaSourceTransport, EiaTransportLimits,
    eia_data_dataset_identifier,
};
use market_squawk_data::{CatalogAuthority, DatasetId, IngestError, IngestPrecommitAuthority};
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
    EiaApplicationAcquisitionLimits, EiaLiveComposition, EiaMacroApplicationClosure,
    EiaMacroApplicationError, EiaMacroPublicationReceipt, ResearchProviderRuntimeGeneration,
    ResearchProviderRuntimeReplacement,
};
use crate::provider_onboarding::ProviderOnboardingOwnedMutationAuthority;

pub(super) const EIA_SURFACE: &str = "eia.api-v2";
const ANALYTICAL_DATASET: &str = crate::application::RESIDENTIAL_ELECTRICITY_PRICE_DATASET;

/// Code-owned route selection and finite acquisition bounds; contains no credential.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaAdapterActivation {
    metadata: SourceMetadata,
    profile: EiaDatasetProfile,
    provider_dataset: SourceIdentifier,
    transport_limits: EiaTransportLimits,
    acquisition_limits: EiaApplicationAcquisitionLimits,
}

impl EiaAdapterActivation {
    /// Binds an exact metadata-driven route profile to explicit page, row, and byte ceilings.
    pub fn try_new(
        metadata: SourceMetadata,
        profile: EiaDatasetProfile,
        transport_limits: EiaTransportLimits,
        max_pages: NonZeroU16,
        max_records: NonZeroU32,
        max_bytes: NonZeroU64,
    ) -> Result<Self, ProviderAdapterActivationError> {
        if profile.publication_mode() != EiaPublicationMode::CanonicalMacro
            || metadata.authorization().mode() != AuthorizationMode::UserAuthorized
            || max_pages.get() > transport_limits.max_pages()
            || transport_limits.max_acquisition_bytes() > max_bytes.get()
            || u32::from(profile.query().length()) > max_records.get()
        {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        let provider_dataset = eia_data_dataset_identifier(profile.query())
            .map_err(|_| ProviderAdapterActivationError::SourceBinding)?;
        let acquisition_limits =
            EiaApplicationAcquisitionLimits::try_new(max_pages, max_records, max_bytes)
                .map_err(|_| ProviderAdapterActivationError::SourceBinding)?;
        Ok(Self {
            metadata,
            profile,
            provider_dataset,
            transport_limits,
            acquisition_limits,
        })
    }

    pub(crate) const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    pub(crate) const fn provider_dataset_identifier(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }
}

/// One protected credential generation and its paired, shared-registry macro runtime.
pub(super) struct EiaProductActivation {
    lease: ProviderActivationLease,
    generation: ResearchProviderRuntimeGeneration,
    specification: EiaAdapterActivation,
    runtime: EiaMacroApplicationClosure,
}

impl EiaProductActivation {
    pub(super) const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        &self.generation
    }

    fn matches(
        &self,
        lease: &ProviderActivationLease,
        generation: &ResearchProviderRuntimeGeneration,
        specification: &EiaAdapterActivation,
    ) -> bool {
        self.lease.same_authority_as(lease)
            && &self.generation == generation
            && &self.specification == specification
    }
}

impl std::fmt::Debug for EiaProductActivation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EiaProductActivation")
            .field("surface_id", self.lease.surface_id())
            .field("source_id", self.generation.metadata().source_id())
            .field("provider_dataset", &self.specification.provider_dataset)
            .finish_non_exhaustive()
    }
}

impl ProviderAdapterActivation {
    /// Registers a configured EIA source without making a provider request or claiming readiness.
    pub(super) async fn activate_eia(
        &self,
        lease: ProviderActivationLease,
        specification: EiaAdapterActivation,
        cancellation: CancellationToken,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError> {
        require_surface(&lease, EIA_SURFACE)?;
        let rights = provider_research_rights(&lease, specification.metadata.source_id())?;
        let generation =
            self.runtime_registration_generation(&lease, &specification.metadata, &rights)?;
        {
            let retained = self
                .eia
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
            .configured_eia_composition(&lease, &specification, &generation, cancellation.clone())
            .await?;
        let (registered_source, runtime) = composition.into_parts();
        if cancellation.is_cancelled() {
            return Err(ProviderAdapterActivationError::Cancelled);
        }
        let onboarding = self.onboarding.try_acquire_runtime_mutation_authority()?;
        onboarding.require_active(&lease)?;
        let mut retained = self
            .eia
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
            *retained = Some(Arc::new(EiaProductActivation {
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
    pub(super) async fn prepare_eia_replacement(
        &self,
        lease: &ProviderActivationLease,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        specification: EiaAdapterActivation,
        cancellation: CancellationToken,
    ) -> Result<
        (
            ResearchProviderRuntimeReplacement,
            Arc<EiaProductActivation>,
        ),
        ProviderAdapterActivationError,
    > {
        require_surface(lease, EIA_SURFACE)?;
        let rights = provider_research_rights(lease, specification.metadata.source_id())?;
        if candidate != runtime_generation(lease, specification.metadata.clone(), rights.clone())?
            || self
                .eia
                .read()
                .map_err(|_| ProviderAdapterActivationError::SourceBinding)?
                .as_ref()
                .is_some_and(|current| current.generation() != &expected)
        {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        self.bind_authorization_subject(&specification.metadata)?;
        let composition = self
            .configured_eia_composition(lease, &specification, &candidate, cancellation.clone())
            .await?;
        if cancellation.is_cancelled() {
            return Err(ProviderAdapterActivationError::Cancelled);
        }
        let (registered_source, runtime) = composition.into_parts();
        let activation = Arc::new(EiaProductActivation {
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

    async fn configured_eia_composition(
        &self,
        lease: &ProviderActivationLease,
        specification: &EiaAdapterActivation,
        generation: &ResearchProviderRuntimeGeneration,
        cancellation: CancellationToken,
    ) -> Result<EiaLiveComposition, ProviderAdapterActivationError> {
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
        let key = EiaApiKey::try_new(secret.expose_secret().to_owned())
            .map_err(|_| ProviderAdapterActivationError::SourceBinding)?;
        let transport = EiaSourceTransport::try_new(
            specification.metadata.clone(),
            key,
            specification.transport_limits,
        )
        .map_err(|_| ProviderAdapterActivationError::SourceBinding)?;
        EiaLiveComposition::try_new(
            Arc::clone(&self.research),
            transport,
            specification.profile.clone(),
            generation.clone(),
        )
        .map_err(|_| ProviderAdapterActivationError::SourceBinding)
    }

    /// Publishes through the exact current EIA generation and returns immutable read coordinates.
    /// Ordinary macro operations consume the receipt's neutral observations through the shared
    /// selector; this provider-specific entry point remains inside setup and source orchestration.
    pub(crate) async fn publish_eia_macro(
        &self,
        context: &RequestContext,
    ) -> Result<EiaMacroPublicationReceipt, EiaProductError> {
        let activation = self
            .eia
            .read()
            .map_err(|_| EiaProductError::Unavailable)?
            .as_ref()
            .cloned()
            .ok_or(EiaProductError::SetupRequired)?;
        let onboarding = self.onboarding.try_acquire_runtime_mutation_authority()?;
        onboarding.require_active(&activation.lease)?;
        if self
            .research
            .provider_runtime_generation(activation.generation.profile())?
            .as_ref()
            != Some(&activation.generation)
        {
            return Err(EiaProductError::Unavailable);
        }
        drop(onboarding);
        let dataset =
            DatasetId::try_from(ANALYTICAL_DATASET).map_err(|_| EiaProductError::Unavailable)?;
        Ok(activation
            .runtime
            .acquire_seal_publish(
                dataset,
                activation.specification.acquisition_limits,
                context,
                |deadline, cancellation| {
                    let onboarding = self
                        .onboarding
                        .try_acquire_owned_runtime_mutation_authority()
                        .map_err(|_| EiaMacroApplicationError::AuthorityInvalid)?;
                    let authority = EiaPublicationAuthority {
                        onboarding,
                        lease: activation.lease.clone(),
                        deadline,
                        cancellation,
                    };
                    authority
                        .validate_precommit()
                        .map_err(|_| EiaMacroApplicationError::AuthorityInvalid)?;
                    Ok(Some(Arc::new(authority)))
                },
            )
            .await?)
    }
}

/// Retains the sole onboarding mutation guard from completed acquisition through catalog commit.
#[derive(Debug)]
struct EiaPublicationAuthority {
    onboarding: ProviderOnboardingOwnedMutationAuthority,
    lease: ProviderActivationLease,
    deadline: Instant,
    cancellation: CancellationToken,
}

impl EiaPublicationAuthority {
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

impl IngestPrecommitAuthority for EiaPublicationAuthority {
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

/// Technical source failure; every nested EIA diagnostic is closed and payload-free.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EiaProductError {
    #[error("energy data setup is required")]
    SetupRequired,
    #[error("the configured energy source is unavailable")]
    Unavailable,
    #[error(transparent)]
    Onboarding(#[from] crate::ProviderOnboardingError),
    #[error(transparent)]
    Composition(#[from] crate::application::ResearchIngestCompositionError),
    #[error("energy data acquisition or durable publication failed")]
    Application(#[from] EiaMacroApplicationError),
}
