//! Current BLS activation and provider-neutral canonical macro operation.
//!
//! Public v1 and registered v2 occupy one application slot. Only the registered source retains
//! the existing live producer runtime; the neutral operation never exposes provider transport,
//! credentials, raw paths, or provider-specific frontend state.

use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Instant,
};

use market_squawk_adapter_bls::{BlsAccessTier, BlsSource};
use market_squawk_data::{
    AnalyticalMacroProviderPeriodLatestKnownOutput, AnalyticalMacroSeriesAllowlist,
    ProviderMacroPlanRestartSelector, QueryLimits,
};
use market_squawk_domain::{EvidenceDigest, ResearchPeriod, Timestamp};
use market_squawk_services::RequestContext;
use market_squawk_sources::SourceMetadataProvider;

use super::{ActivatedResearchProvider, ProviderAdapterActivation, ProviderAdapterActivationError};
use crate::application::{
    BlsLivePublicationError, BlsRegisteredV2LiveComposition, BlsRegisteredV2LiveOutcome,
    BlsRegisteredV2LiveRequest, BlsRegisteredV2LiveRuntime, ResearchProviderRuntimeGeneration,
    ResearchProviderRuntimeReplacement, ResearchRightsAuthority,
};
use crate::{ProviderActivationLease, ProviderOnboardingError};

/// Provider-neutral bounded request for the current canonical provider-period macro source.
#[derive(Clone, Debug)]
pub(crate) struct MacroProviderPeriodLatestKnownRequest {
    live: BlsRegisteredV2LiveRequest,
}

impl MacroProviderPeriodLatestKnownRequest {
    /// Retains independent provider, physical-seal, extraction, point-in-time, and query bounds.
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
        series_allowlist: AnalyticalMacroSeriesAllowlist,
        knowledge_cutoff: Timestamp,
        effective_period_cutoff: ResearchPeriod,
        query_limits: QueryLimits,
        query_deadline: Instant,
    ) -> Self {
        Self {
            live: BlsRegisteredV2LiveRequest::new(
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
            ),
        }
    }
}

/// Canonical macro observations and immutable restart/PIT evidence from the current source.
#[derive(Debug)]
pub(crate) struct MacroProviderPeriodLatestKnownOutput {
    live: BlsRegisteredV2LiveOutcome,
}

impl MacroProviderPeriodLatestKnownOutput {
    /// Returns the exact manifest-only selector proven reopenable before the typed read.
    pub(crate) const fn restart_selector(&self) -> &ProviderMacroPlanRestartSelector {
        self.live.read().restart_selector()
    }

    /// Returns canonical macro rows with provider period, missingness, and exact clocks preserved.
    pub(crate) const fn canonical(&self) -> &AnalyticalMacroProviderPeriodLatestKnownOutput {
        self.live.read().output()
    }

    /// Returns the exact whole-plan publication identity retained by the neutral operation.
    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.live.publication_digest()
    }

    /// Returns the adapter-owned source-plan identity without exposing provider request details.
    pub(crate) const fn source_plan_digest(&self) -> EvidenceDigest {
        self.live.activation_plan_digest()
    }
}

enum BlsCurrentRuntime {
    PublicV1,
    RegisteredV2(BlsRegisteredV2LiveRuntime),
}

/// One current BLS generation across the alternative public and registered access tiers.
pub(super) struct BlsProductActivation {
    lease: ProviderActivationLease,
    generation: ResearchProviderRuntimeGeneration,
    tier: BlsAccessTier,
    source_plan_digest: EvidenceDigest,
    runtime: BlsCurrentRuntime,
}

impl BlsProductActivation {
    fn public_v1(
        lease: ProviderActivationLease,
        generation: ResearchProviderRuntimeGeneration,
        source_plan_digest: EvidenceDigest,
    ) -> Self {
        Self {
            lease,
            generation,
            tier: BlsAccessTier::PublicV1,
            source_plan_digest,
            runtime: BlsCurrentRuntime::PublicV1,
        }
    }

    fn registered_v2(
        lease: ProviderActivationLease,
        generation: ResearchProviderRuntimeGeneration,
        source_plan_digest: EvidenceDigest,
        runtime: BlsRegisteredV2LiveRuntime,
    ) -> Self {
        Self {
            lease,
            generation,
            tier: BlsAccessTier::RegisteredV2,
            source_plan_digest,
            runtime: BlsCurrentRuntime::RegisteredV2(runtime),
        }
    }

    pub(super) const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        &self.generation
    }

    fn matches(
        &self,
        lease: &ProviderActivationLease,
        generation: &ResearchProviderRuntimeGeneration,
        tier: BlsAccessTier,
        source_plan_digest: EvidenceDigest,
    ) -> bool {
        self.lease.same_authority_as(lease)
            && &self.generation == generation
            && self.tier == tier
            && self.source_plan_digest == source_plan_digest
    }

    fn registered_runtime(&self) -> Option<&BlsRegisteredV2LiveRuntime> {
        match &self.runtime {
            BlsCurrentRuntime::PublicV1 => None,
            BlsCurrentRuntime::RegisteredV2(runtime) => Some(runtime),
        }
    }
}

impl std::fmt::Debug for BlsProductActivation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsProductActivation")
            .field("surface_id", self.lease.surface_id())
            .field("source_id", self.generation.metadata().source_id())
            .field("tier", &self.tier)
            .field("source_plan_digest", &self.source_plan_digest)
            .field("credential", &"[SECRET-STORE ONLY]")
            .finish()
    }
}

impl ProviderAdapterActivation {
    /// Registers one exact public or registered BLS generation into the sole current slot.
    pub(super) fn register_bls_source(
        &self,
        lease: ProviderActivationLease,
        source: BlsSource,
        rights: ResearchRightsAuthority,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError> {
        let generation =
            self.runtime_registration_generation(&lease, source.metadata(), &rights)?;
        let plan = source.activation_plan()?;
        let tier = plan.rate().tier();
        let source_plan_digest = plan.plan_digest();
        let mut retained = self
            .bls
            .write()
            .map_err(|_| ProviderAdapterActivationError::SourceBinding)?;
        if let Some(current) = retained.as_ref() {
            if current.matches(&lease, &generation, tier, source_plan_digest)
                && self
                    .research
                    .provider_runtime_generation(generation.profile())?
                    .as_ref()
                    == Some(&generation)
            {
                return Ok(ActivatedResearchProvider {
                    lease,
                    profile: generation.profile().clone(),
                    generation,
                });
            }
            return Err(ProviderAdapterActivationError::SourceBinding);
        }

        let activation = match tier {
            BlsAccessTier::PublicV1 => {
                self.research_mutation.register_provider_source(
                    generation.clone(),
                    source,
                    rights,
                )?;
                BlsProductActivation::public_v1(
                    lease.clone(),
                    generation.clone(),
                    source_plan_digest,
                )
            }
            BlsAccessTier::RegisteredV2 => {
                let composition = BlsRegisteredV2LiveComposition::try_new(
                    Arc::clone(&self.research),
                    source,
                    generation.clone(),
                )
                .map_err(map_live_composition_error)?;
                let (registered_source, runtime) = composition.into_parts();
                self.research_mutation.register_provider_source(
                    generation.clone(),
                    registered_source,
                    rights,
                )?;
                BlsProductActivation::registered_v2(
                    lease.clone(),
                    generation.clone(),
                    source_plan_digest,
                    runtime,
                )
            }
        };
        *retained = Some(Arc::new(activation));
        Ok(ActivatedResearchProvider {
            lease,
            profile: generation.profile().clone(),
            generation,
        })
    }

    /// Prepares a registered-v2 successor together with its still-non-callable typed runtime.
    pub(super) async fn prepare_bls_registered_replacement(
        &self,
        lease: &ProviderActivationLease,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        source: BlsSource,
        rights: ResearchRightsAuthority,
    ) -> Result<
        (
            ResearchProviderRuntimeReplacement,
            Arc<BlsProductActivation>,
        ),
        ProviderAdapterActivationError,
    > {
        let plan = source.activation_plan()?;
        if plan.rate().tier() != BlsAccessTier::RegisteredV2 {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        let source_plan_digest = plan.plan_digest();
        if let Some(current) = self
            .bls
            .read()
            .map_err(|_| ProviderAdapterActivationError::SourceBinding)?
            .as_ref()
            && current.generation() != &expected
        {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        let composition = BlsRegisteredV2LiveComposition::try_new(
            Arc::clone(&self.research),
            source,
            candidate.clone(),
        )
        .map_err(map_live_composition_error)?;
        let (registered_source, runtime) = composition.into_parts();
        let activation = Arc::new(BlsProductActivation::registered_v2(
            lease.clone(),
            candidate.clone(),
            source_plan_digest,
            runtime,
        ));
        let replacement = self
            .prepare_runtime_replacement(lease, expected, candidate, registered_source, rights)
            .await?;
        Ok((replacement, activation))
    }

    /// Executes the neutral canonical macro operation against the exact current registered source.
    pub(crate) async fn execute_macro_provider_period_latest_known(
        &self,
        request: MacroProviderPeriodLatestKnownRequest,
        context: &RequestContext,
    ) -> Result<MacroProviderPeriodLatestKnownOutput, MacroProviderPeriodOperationError> {
        let activation = self
            .bls
            .read()
            .map_err(|_| MacroProviderPeriodOperationError::Unavailable)?
            .as_ref()
            .cloned()
            .ok_or(MacroProviderPeriodOperationError::SetupRequired)?;
        let runtime = activation
            .registered_runtime()
            .ok_or(MacroProviderPeriodOperationError::RegisteredAccessRequired)?;
        let onboarding = self.onboarding.acquire_runtime_mutation_authority().await;
        onboarding.require_active(&activation.lease)?;
        if self
            .research
            .provider_runtime_generation(activation.generation().profile())?
            .as_ref()
            != Some(activation.generation())
        {
            return Err(MacroProviderPeriodOperationError::Unavailable);
        }
        drop(onboarding);
        let live = runtime.publish_and_read(request.live, context).await?;
        Ok(MacroProviderPeriodLatestKnownOutput { live })
    }
}

fn map_live_composition_error(error: BlsLivePublicationError) -> ProviderAdapterActivationError {
    tracing::error!(%error, "BLS live application composition failed");
    ProviderAdapterActivationError::SourceBinding
}

/// Closed neutral macro-operation failure; provider credentials and transport are absent.
#[derive(Debug, thiserror::Error)]
pub(crate) enum MacroProviderPeriodOperationError {
    #[error("macro data setup is required")]
    SetupRequired,
    #[error("the configured macro source does not admit the durable provider-period operation")]
    RegisteredAccessRequired,
    #[error("the configured macro source is unavailable")]
    Unavailable,
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    #[error(transparent)]
    Composition(#[from] crate::application::ResearchIngestCompositionError),
    #[error("canonical macro publication or point-in-time selection failed")]
    Live(#[from] BlsLivePublicationError),
}
