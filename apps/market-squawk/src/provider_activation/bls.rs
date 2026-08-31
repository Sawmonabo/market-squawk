//! Current BLS activation and provider-neutral canonical macro operation.
//!
//! Public v1 and registered v2 occupy one application slot and retain the same live producer
//! runtime. The neutral operation never exposes provider transport, credentials, raw paths, or
//! provider-specific frontend state.

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
    BlsLiveComposition, BlsLiveOutcome, BlsLivePublicationError, BlsLiveRequest, BlsLiveRuntime,
    ResearchProviderRuntimeGeneration, ResearchProviderRuntimeReplacement, ResearchRightsAuthority,
};
use crate::{ProviderActivationLease, ProviderOnboardingError};

/// Provider-neutral bounded request for the current canonical provider-period macro source.
#[derive(Clone, Debug)]
pub(crate) struct MacroProviderPeriodLatestKnownRequest {
    live: BlsLiveRequest,
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
            live: BlsLiveRequest::new(
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
    live: BlsLiveOutcome,
}

impl MacroProviderPeriodLatestKnownOutput {
    /// Returns the exact manifest-only selector proven reopenable before the typed read.
    pub(crate) const fn restart_selector(&self) -> &ProviderMacroPlanRestartSelector {
        self.live.restart_selector()
    }

    /// Returns canonical macro rows only when the exact cutoff produced a complete selected set.
    pub(crate) const fn canonical(
        &self,
    ) -> Option<&AnalyticalMacroProviderPeriodLatestKnownOutput> {
        match self.live.available_read() {
            Some(read) => Some(read.output()),
            None => None,
        }
    }

    /// Reports exact cutoff incompleteness while retaining committed restart evidence.
    pub(crate) const fn incomplete_at_cutoff(&self) -> bool {
        self.live.incomplete_restart_selector().is_some()
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

/// One current BLS generation across the alternative public and registered access tiers.
pub(super) struct BlsProductActivation {
    lease: ProviderActivationLease,
    generation: ResearchProviderRuntimeGeneration,
    tier: BlsAccessTier,
    source_plan_digest: EvidenceDigest,
    runtime: BlsLiveRuntime,
}

impl BlsProductActivation {
    fn new(
        lease: ProviderActivationLease,
        generation: ResearchProviderRuntimeGeneration,
        tier: BlsAccessTier,
        source_plan_digest: EvidenceDigest,
        runtime: BlsLiveRuntime,
    ) -> Self {
        Self {
            lease,
            generation,
            tier,
            source_plan_digest,
            runtime,
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

    const fn runtime(&self) -> &BlsLiveRuntime {
        &self.runtime
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

        let composition =
            BlsLiveComposition::try_new(Arc::clone(&self.research), source, generation.clone())
                .map_err(map_live_composition_error)?;
        let (live_source, runtime) = composition.into_parts();
        self.research_mutation
            .register_provider_source(generation.clone(), live_source, rights)?;
        let activation = BlsProductActivation::new(
            lease.clone(),
            generation.clone(),
            tier,
            source_plan_digest,
            runtime,
        );
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
        let composition =
            BlsLiveComposition::try_new(Arc::clone(&self.research), source, candidate.clone())
                .map_err(map_live_composition_error)?;
        let (live_source, runtime) = composition.into_parts();
        let activation = Arc::new(BlsProductActivation::new(
            lease.clone(),
            candidate.clone(),
            BlsAccessTier::RegisteredV2,
            source_plan_digest,
            runtime,
        ));
        let replacement = self
            .prepare_runtime_replacement(lease, expected, candidate, live_source, rights)
            .await?;
        Ok((replacement, activation))
    }

    /// Executes the neutral canonical macro operation against the exact current BLS source.
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
        let live = activation
            .runtime()
            .publish_and_read(request.live, context)
            .await?;
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
    #[error("the configured macro source is unavailable")]
    Unavailable,
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    #[error(transparent)]
    Composition(#[from] crate::application::ResearchIngestCompositionError),
    #[error("canonical macro publication or point-in-time selection failed")]
    Live(#[from] BlsLivePublicationError),
}
